//! Catalog root initialization, fencing and indexed decision lookup.
use super::{
    CatalogFence, ClusterCatalog, Decision, MAX_DECISION_BYTES, ObjectAccessError, ObjectKey,
    ObjectStore, Storage, StorageError, WriteCondition,
    decision_index::{PageHash, Reads},
};
use crate::protocol::MAX_CATALOG_BYTES;
use futures_util::{StreamExt, TryStreamExt};
use nabedra_rt::time::{Instant, timeout_at};
use uuid::Uuid;

/// A complete validated root and its nonempty opaque version.
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

/// Object permission probes in flight at once for one catalog (OLAP-5).
const AUTHORIZE_CONCURRENCY: usize = 16;

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
    fn caps(&self) -> (usize, usize) {
        (self.max_records, self.max_bytes)
    }
    fn check_deadline(&self) -> Result<(), StorageError> {
        if nabedra_rt::time::now() >= self.deadline {
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

/// Takeover raises the scheduler term within a run; ColdStart requires
/// explicit operator evidence that all old queue holders stopped.
/// Heartbeats are not proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceTransition {
    Takeover,
    ColdStart,
}
impl<B: ObjectStore> Storage<B> {
    /// Check GET permission and drop the transfer before reading its body.
    pub(crate) async fn authorize_object(&self, key: &ObjectKey) -> Result<(), ObjectAccessError> {
        self.backend
            .authorize_get(&format!("{}{}", self.namespace.as_str(), key.as_str()))
            .await
    }

    /// Reauthorize all backing objects before exposing a retained result through
    /// a fresh grant. A successful probe establishes object permission; no body
    /// is downloaded into a shared cache.
    pub async fn authorize_catalog_objects(
        &self,
        loaded: &LoadedClusterCatalog,
        deadline: Instant,
    ) -> Result<(), StorageError> {
        self.authorize_catalog_access(loaded, deadline)
            .await
            .map_err(ObjectAccessError::into_storage_error)
    }

    /// As `authorize_catalog_objects`, keeping an explicit denial apart from a
    /// storage failure or the deadline (OLAP-5), so that callers can answer
    /// `unauthorized` only for a denial. Probes run concurrently, at most
    /// `AUTHORIZE_CONCURRENCY` at a time, under the one deadline.
    pub async fn authorize_catalog_access(
        &self,
        loaded: &LoadedClusterCatalog,
        deadline: Instant,
    ) -> Result<(), ObjectAccessError> {
        loaded.catalog.validate()?;
        let mut keys = Vec::new();
        for entry in loaded.catalog.tables.values() {
            match &entry.data {
                super::TableData::Legacy { snapshot } => {
                    keys.push(ObjectKey::parse_snapshot(snapshot, entry.id)?);
                }
                super::TableData::Partitioned { files } => {
                    for file in files {
                        keys.push(ObjectKey::parse_partition(&file.object, entry.id)?);
                    }
                }
            }
        }
        futures_util::stream::iter(keys)
            .map(|key| async move {
                timeout_at(deadline, self.authorize_object(&key))
                    .await
                    .map_err(|_| ObjectAccessError::Storage(StorageError::Unavailable))?
            })
            .buffer_unordered(AUTHORIZE_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
            .map(drop)
    }

    /// Create the root on first use, attach to it, or fence it. Ownership, run
    /// and term are published in the same conditional root write. Attaching
    /// with fresh credentials never changes the run or cluster owner. A root of
    /// any other format is refused (`UnsupportedCatalog`).
    pub async fn initialize_owned_cluster_catalog(
        &self,
        cluster_id: Uuid,
        fence: CatalogFence,
        cold: bool,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        fence.validate()?;
        if cluster_id.is_nil() {
            return Err(StorageError::InvalidCatalog);
        }
        match self.load_cluster_catalog().await {
            Ok(loaded) => {
                if loaded.catalog.owner_cluster != Some(cluster_id) {
                    return Err(StorageError::Conflict);
                }
                self.attach_or_fence(loaded, fence, cold).await
            }
            Err(StorageError::NotFound) => {
                self.create_root(ClusterCatalog::empty(fence, Some(cluster_id))?)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    /// Load with a bounded complete read and root/head validation.
    pub async fn load_cluster_catalog(&self) -> Result<LoadedClusterCatalog, StorageError> {
        let object = self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await?;
        LoadedClusterCatalog::new(ClusterCatalog::decode(&object.bytes)?, object.etag)
    }

    /// Fixed-cluster bootstrap: create the unowned root when it is absent,
    /// attach to it, or fence it for a cold start. Cold start requires evidence
    /// that all previous queue holders stopped. Every transition submits at most
    /// one conditional root PUT.
    pub async fn initialize_fenced_cluster_catalog(
        &self,
        fence: CatalogFence,
        cold: bool,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        fence.validate()?;
        match self.load_cluster_catalog().await {
            Ok(loaded) if loaded.catalog.owner_cluster.is_some() => Err(StorageError::Conflict),
            Ok(loaded) if cold => {
                self.fence_cluster_catalog(&loaded, fence, FenceTransition::ColdStart)
                    .await
            }
            Ok(loaded) => Ok(loaded),
            Err(StorageError::NotFound) => {
                self.create_root(ClusterCatalog::empty(fence, None)?).await
            }
            Err(error) => Err(error),
        }
    }

    async fn attach_or_fence(
        &self,
        loaded: LoadedClusterCatalog,
        fence: CatalogFence,
        cold: bool,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        if loaded.catalog.fence == Some(fence) {
            return Ok(loaded);
        }
        let old = loaded.catalog.fence.ok_or(StorageError::InvalidCatalog)?;
        let transition = if old.run_id == fence.run_id {
            FenceTransition::Takeover
        } else if cold {
            FenceTransition::ColdStart
        } else {
            return Err(StorageError::Conflict);
        };
        self.fence_cluster_catalog(&loaded, fence, transition).await
    }

    /// One `Create` of a new root. A conflict or ambiguous reply triggers one
    /// fresh read, never a second PUT; only this exact root confirms it.
    async fn create_root(
        &self,
        catalog: ClusterCatalog,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        match self
            .put(
                &ObjectKey::catalog(),
                catalog.encode()?,
                WriteCondition::Create,
            )
            .await
        {
            Ok(etag) if !etag.is_empty() => LoadedClusterCatalog::new(catalog, etag),
            result => {
                let error = match result {
                    Ok(_) => StorageError::Ambiguous,
                    Err(error @ (StorageError::Conflict | StorageError::Ambiguous)) => error,
                    Err(error) => return Err(error),
                };
                match self.load_cluster_catalog().await {
                    Ok(current)
                        if current.catalog.fence == catalog.fence
                            && current.catalog.owner_cluster == catalog.owner_cluster =>
                    {
                        Ok(current)
                    }
                    Ok(_) | Err(StorageError::NotFound) => Err(error),
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
            (Some(old), FenceTransition::Takeover)
                if old.run_id == fence.run_id && fence.scheduler_term > old.scheduler_term => {}
            (Some(old), FenceTransition::ColdStart) if old.run_id != fence.run_id => {}
            _ => return Err(StorageError::Conflict),
        }
        let mut catalog = base.catalog.clone();
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

    /// Search only this captured root's index. Authorize the caller BEFORE
    /// lookup. Identity is the UUID plus the exact original body bytes. Only a
    /// validated index path proves absence. At most 16 page reads (none for
    /// cached pages) and one decision read, within the supplied caps.
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

    pub(crate) async fn lookup_decision_inner(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        request_body: Option<&[u8]>,
        budget: HistoryBudget,
    ) -> Result<HistoryLookup, StorageError> {
        budget.check_deadline()?;
        let Some(root) = loaded.catalog.index_root()? else {
            return Ok(HistoryLookup::Absent);
        };
        let mut reads = Reads {
            records: 0,
            bytes: 0,
        };
        let caps = budget.caps();
        let Some(id) = self
            .index_find(&root, request_id, budget.deadline, &mut reads, caps)
            .await?
        else {
            budget.check_deadline()?;
            return Ok(HistoryLookup::Absent);
        };
        reads.records += 1;
        if reads.records > caps.0 {
            return Err(StorageError::LimitExceeded);
        }
        let remaining = caps
            .1
            .checked_sub(reads.bytes)
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
        let decision = Decision::decode(&object.bytes)?;
        if decision.id != id
            || decision.request_id != request_id
            || decision.revision > loaded.catalog.revision
        {
            return Err(StorageError::InvalidCatalog);
        }
        budget.check_deadline()?;
        Ok(
            if request_body.is_none_or(|body| decision.request_body.as_bytes() == body) {
                HistoryLookup::Found(decision)
            } else {
                HistoryLookup::IdentityConflict
            },
        )
    }
}

/// The index root of a loaded catalog, for publication.
pub(crate) fn index_of(loaded: &LoadedClusterCatalog) -> Result<Option<PageHash>, StorageError> {
    loaded.catalog.index_root()
}
