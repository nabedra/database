//! Conditional v3 initialization and bounded lookup in one captured history.
use super::{
    Catalog, CatalogFence, ClusterCatalog, Decision, MAX_DECISION_BYTES, Object, ObjectKey,
    ObjectStore, Storage, StorageError, WriteCondition,
};
use crate::protocol::MAX_CATALOG_BYTES;
use serde::Deserialize;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

/// A complete validated v3 representation and its nonempty opaque version.
/// Private fields prevent callers from changing relationships after validation.
#[derive(Clone, PartialEq, Eq)]
pub struct LoadedClusterCatalog {
    catalog: ClusterCatalog,
    etag: String,
}
impl std::fmt::Debug for LoadedClusterCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedClusterCatalog")
            .field("revision", &self.catalog.revision)
            .field("commit_id", &self.catalog.commit_id)
            .finish_non_exhaustive()
    }
}
impl LoadedClusterCatalog {
    pub fn catalog(&self) -> &ClusterCatalog {
        &self.catalog
    }
    pub fn etag(&self) -> &str {
        &self.etag
    }
    pub fn new(catalog: ClusterCatalog, etag: String) -> Result<Self, StorageError> {
        catalog.validate()?;
        if etag.is_empty() {
            return Err(StorageError::Unavailable);
        }
        if catalog
            .decision_head
            .is_some_and(|head| head != catalog.commit_id)
        {
            return Err(StorageError::InvalidCatalog);
        }
        Ok(Self { catalog, etag })
    }
}

/// Caller-selected work limits for one lookup. No runtime defaults or TTL.
/// The deadline is absolute and monotonic, including backend GET and body reads.
#[derive(Debug, Clone, Copy)]
pub struct HistoryBudget {
    max_records: usize,
    max_bytes: usize,
    deadline: Instant,
}
impl HistoryBudget {
    pub fn new(
        max_records: usize,
        max_bytes: usize,
        deadline: Instant,
    ) -> Result<Self, StorageError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(StorageError::LimitExceeded);
        }
        let budget = Self {
            max_records,
            max_bytes,
            deadline,
        };
        budget.check_deadline()?;
        Ok(budget)
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    fn check_deadline(&self) -> Result<(), StorageError> {
        if Instant::now() >= self.deadline {
            Err(StorageError::Unavailable)
        } else {
            Ok(())
        }
    }
}

/// IdentityConflict is separate from a backend conditional-write Conflict.
/// Neither the conflicting stored body nor the supplied body is returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryLookup {
    Found(Decision),
    IdentityConflict,
    Absent,
}

enum Root {
    Cluster(LoadedClusterCatalog),
    Legacy(Catalog, String),
}
fn decode_root(object: Object) -> Result<Root, StorageError> {
    // Only select a codec here. Both codecs still consume the ORIGINAL bytes,
    // preserving duplicate fields and all nested strict validation evidence.
    #[derive(Deserialize)]
    struct Version {
        format_version: u32,
    }
    let version: Version =
        serde_json::from_slice(&object.bytes).map_err(|_| StorageError::InvalidCatalog)?;
    if matches!(version.format_version, 3 | 4) {
        Ok(Root::Cluster(LoadedClusterCatalog::new(
            ClusterCatalog::decode(&object.bytes)?,
            object.etag,
        )?))
    } else {
        let catalog = Catalog::decode(&object.bytes)?;
        if object.etag.is_empty() {
            return Err(StorageError::Unavailable);
        }
        Ok(Root::Legacy(catalog, object.etag))
    }
}
/// Migration requires compatible old writers stopped; ColdStart requires explicit
/// operator evidence that all old queue holders stopped. Heartbeats are not proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceTransition {
    Migrate,
    Takeover,
    ColdStart,
}
impl<B: ObjectStore> Storage<B> {
    /// Load v3/v4 with bounded complete reads and root/head validation.
    pub async fn load_cluster_catalog(&self) -> Result<LoadedClusterCatalog, StorageError> {
        let object = self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await?;
        LoadedClusterCatalog::new(ClusterCatalog::decode(&object.bytes)?, object.etag)
    }

    /// Attach, create, or conditionally migrate once. The old standalone writer
    /// MUST be stopped before migration; remote process absence is not checked.
    /// Conflict/ambiguity triggers one fresh read, never a second PUT.
    pub async fn initialize_cluster_catalog(&self) -> Result<LoadedClusterCatalog, StorageError> {
        let (catalog, token) = match self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await {
            Ok(object) => match decode_root(object)? {
                Root::Cluster(loaded) => return Ok(loaded),
                Root::Legacy(catalog, token) => {
                    (ClusterCatalog::from_catalog(&catalog)?, Some(token))
                }
            },
            Err(StorageError::NotFound) => (ClusterCatalog::from_catalog(&Catalog::empty())?, None),
            Err(error) => return Err(error),
        };
        let condition = token
            .as_deref()
            .map_or(WriteCondition::Create, WriteCondition::Match);
        match self
            .put(&ObjectKey::catalog(), catalog.encode()?, condition)
            .await
        {
            Ok(etag) if !etag.is_empty() => LoadedClusterCatalog::new(catalog, etag),
            result => {
                let error = match result {
                    Ok(_) => StorageError::Ambiguous,
                    Err(error @ (StorageError::Conflict | StorageError::Ambiguous)) => error,
                    Err(error) => return Err(error),
                };
                match self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await {
                    Ok(object) => match decode_root(object)? {
                        Root::Cluster(loaded) => Ok(loaded),
                        Root::Legacy(_, _) => Err(error),
                    },
                    Err(StorageError::NotFound) => Err(error),
                    Err(error) => Err(error),
                }
            }
        }
    }

    /// Production bootstrap creates v4 directly when the root is absent.
    /// Existing legacy/v3 writers require an explicit compatible migration
    /// acknowledgement; even an empty existing v3 root is a migration. Existing
    /// v4 attaches without changing authority unless cold start was authorized.
    /// Cold start requires evidence that all previous queue holders stopped.
    /// Every transition submits at most one conditional root PUT.
    pub async fn initialize_fenced_cluster_catalog(
        &self,
        fence: CatalogFence,
        migration_allowed: bool,
        cold: bool,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        fence.validate()?;
        let (mut catalog, token) = match self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await {
            Ok(object) => match decode_root(object)? {
                Root::Cluster(loaded) if loaded.catalog.format_version == 4 => {
                    if cold {
                        return self
                            .fence_cluster_catalog(&loaded, fence, FenceTransition::ColdStart)
                            .await;
                    }
                    return Ok(loaded);
                }
                Root::Cluster(loaded) => {
                    if !migration_allowed {
                        return Err(StorageError::UnsupportedCatalog);
                    }
                    (loaded.catalog, Some(loaded.etag))
                }
                Root::Legacy(catalog, token) => {
                    if !migration_allowed {
                        return Err(StorageError::UnsupportedCatalog);
                    }
                    (ClusterCatalog::from_catalog(&catalog)?, Some(token))
                }
            },
            Err(StorageError::NotFound) => (ClusterCatalog::from_catalog(&Catalog::empty())?, None),
            Err(error) => return Err(error),
        };
        catalog.format_version = 4;
        catalog.fence = Some(fence);
        let condition = token
            .as_deref()
            .map_or(WriteCondition::Create, WriteCondition::Match);
        match self
            .put(&ObjectKey::catalog(), catalog.encode()?, condition)
            .await
        {
            Ok(etag) if !etag.is_empty() => LoadedClusterCatalog::new(catalog, etag),
            result => {
                let error = match result {
                    Ok(_) => StorageError::Ambiguous,
                    Err(error @ (StorageError::Conflict | StorageError::Ambiguous)) => error,
                    Err(error) => return Err(error),
                };
                // A fresh complete read can confirm only this exact intended
                // fence. A competing root never proves our transition applied.
                match self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await {
                    Ok(object) => match decode_root(object)? {
                        Root::Cluster(loaded) if loaded.catalog.fence == Some(fence) => Ok(loaded),
                        _ => Err(error),
                    },
                    Err(StorageError::NotFound) => Err(error),
                    Err(error) => Err(error),
                }
            }
        }
    }

    /// Fence first, before issuing jobs. This changes the SAME ETag used by
    /// mutation publication and preserves tables, commit identity and history.
    /// One conditional PUT only: ambiguous replies reconcile this exact fence.
    pub async fn fence_cluster_catalog(
        &self,
        base: &LoadedClusterCatalog,
        fence: CatalogFence,
        transition: FenceTransition,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        fence.validate()?;
        match (base.catalog.fence, transition) {
            (None, FenceTransition::Migrate) if base.catalog.format_version == 3 => {}
            (Some(old), FenceTransition::Takeover)
                if old.run_id == fence.run_id && fence.scheduler_term > old.scheduler_term => {}
            (Some(old), FenceTransition::ColdStart) if old.run_id != fence.run_id => {}
            _ => return Err(StorageError::Conflict),
        }
        let mut catalog = base.catalog.clone();
        catalog.format_version = 4;
        catalog.fence = Some(fence);
        let result = self
            .put(
                &ObjectKey::catalog(),
                catalog.encode()?,
                WriteCondition::Match(base.etag()),
            )
            .await;
        match result {
            Ok(etag) if !etag.is_empty() => LoadedClusterCatalog::new(catalog, etag),
            Ok(_) | Err(StorageError::Ambiguous) => {
                let current = self.load_cluster_catalog().await?;
                if current.catalog.fence == Some(fence) {
                    Ok(current)
                } else {
                    Err(StorageError::Ambiguous)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Search only this captured root's reachable chain. Authorize the caller
    /// BEFORE lookup. Identity is normalized UUID plus exact original body bytes.
    /// Only a validated null terminus proves absence; a matching prefix suffices
    /// for Found. Cost is linear with retained history; auxiliary space is one
    /// bounded record, with no chain/body collection, caching, GC or retries.
    pub async fn lookup_decision(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        request_body: &[u8],
        budget: HistoryBudget,
    ) -> Result<HistoryLookup, StorageError> {
        self.lookup_decision_inner(loaded, request_id, Some(request_body), budget)
            .await
    }

    /// Receipt reconciliation is authorized by database scope. This returns only
    /// an authoritative reachable mutation decision, never a pending request.
    pub async fn lookup_decision_by_id(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        budget: HistoryBudget,
    ) -> Result<Option<Decision>, StorageError> {
        match self
            .lookup_decision_inner(loaded, request_id, None, budget)
            .await?
        {
            HistoryLookup::Found(decision) => Ok(Some(decision)),
            HistoryLookup::Absent => Ok(None),
            HistoryLookup::IdentityConflict => Err(StorageError::InvalidCatalog),
        }
    }
    async fn lookup_decision_inner(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        request_body: Option<&[u8]>,
        budget: HistoryBudget,
    ) -> Result<HistoryLookup, StorageError> {
        budget.check_deadline()?;
        let mut next = loaded.catalog.decision_head;
        let mut revision = loaded.catalog.revision;
        let mut records = 0usize;
        let mut bytes = 0usize;
        while let Some(id) = next {
            budget.check_deadline()?;
            records = records.checked_add(1).ok_or(StorageError::LimitExceeded)?;
            if records > budget.max_records {
                return Err(StorageError::LimitExceeded);
            }
            let remaining = budget
                .max_bytes
                .checked_sub(bytes)
                .ok_or(StorageError::LimitExceeded)?;
            let object = timeout_at(
                budget.deadline,
                self.read(&ObjectKey::decision(id), MAX_DECISION_BYTES.min(remaining)),
            )
            .await
            .map_err(|_| StorageError::Unavailable)?
            .map_err(|error| {
                if error == StorageError::NotFound {
                    StorageError::InvalidCatalog
                } else {
                    error
                }
            })?;
            budget.check_deadline()?;
            bytes = bytes
                .checked_add(object.bytes.len())
                .ok_or(StorageError::LimitExceeded)?;
            let decision = Decision::decode(&object.bytes)?;
            if decision.id != id
                || decision.revision != revision
                || revision.checked_sub(1) != Some(decision.base_revision)
            {
                return Err(StorageError::InvalidCatalog);
            }
            budget.check_deadline()?;
            if decision.request_id == request_id {
                return Ok(
                    if request_body.is_none_or(|body| decision.request_body.as_bytes() == body) {
                        HistoryLookup::Found(decision)
                    } else {
                        HistoryLookup::IdentityConflict
                    },
                );
            }
            // Strict decrease detects cycles without a visited-ID allocation.
            revision = decision.base_revision;
            next = decision.parent;
        }
        budget.check_deadline()?;
        Ok(HistoryLookup::Absent)
    }
}
