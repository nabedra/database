//! Conditional v3 initialization and bounded lookup in one captured history.
use super::{
    Catalog, CatalogFence, ClusterCatalog, Decision, MAX_DECISION_BYTES, Object, ObjectAccessError,
    ObjectKey, ObjectStore, Storage, StorageError, WriteCondition,
    decision_index::{IndexBase, PageHash, Reads, hex},
};
use crate::protocol::MAX_CATALOG_BYTES;
use nabedra_rt::time::{Instant, timeout_at};
use serde::Deserialize;
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
    if matches!(version.format_version, 3..=6) {
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
/// The fenced format of a root: indexed roots stay indexed.
fn fenced_format(catalog: &ClusterCatalog) -> u32 {
    if catalog.indexed() {
        6
    } else if catalog.owner_cluster.is_some() {
        5
    } else {
        4
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
    /// Check GET permission and drop the transfer before reading its body.
    pub(crate) async fn authorize_object(&self, key: &ObjectKey) -> Result<(), ObjectAccessError> {
        self.backend
            .authorize_get(&format!("{}{}", self.namespace.as_str(), key.as_str()))
            .await
    }

    /// Reauthorize all backing objects before exposing a retained result through
    /// a fresh grant. A successful GET establishes object permission; the body
    /// is immediately dropped rather than downloaded into a shared cache.
    pub async fn authorize_catalog_objects(
        &self,
        loaded: &LoadedClusterCatalog,
        deadline: Instant,
    ) -> Result<(), StorageError> {
        loaded.catalog.validate()?;
        for entry in loaded.catalog.tables.values() {
            match &entry.data {
                super::TableData::Legacy { snapshot } => {
                    let key = ObjectKey::parse_snapshot(snapshot, entry.id)?;
                    timeout_at(deadline, self.authorize_object(&key))
                        .await
                        .map_err(|_| StorageError::Unavailable)?
                        .map_err(ObjectAccessError::into_storage_error)?;
                }
                super::TableData::Partitioned { files } => {
                    for file in files {
                        let key = ObjectKey::parse_partition(&file.object, entry.id)?;
                        timeout_at(deadline, self.authorize_object(&key))
                            .await
                            .map_err(|_| StorageError::Unavailable)?
                            .map_err(ObjectAccessError::into_storage_error)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Initialize a customer catalog or explicitly migrate after old writers stop.
    /// Ownership, run and term are published in the same conditional root write.
    /// Attaching with fresh credentials never changes the run or cluster owner.
    pub async fn initialize_owned_cluster_catalog(
        &self,
        cluster_id: Uuid,
        fence: CatalogFence,
        migration_allowed: bool,
        cold: bool,
    ) -> Result<LoadedClusterCatalog, StorageError> {
        fence.validate()?;
        if cluster_id.is_nil() {
            return Err(StorageError::InvalidCatalog);
        }
        let (mut catalog, token) = match self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await {
            Ok(object) => match decode_root(object)? {
                Root::Cluster(loaded) if loaded.catalog.owner_cluster.is_some() => {
                    if loaded.catalog.owner_cluster != Some(cluster_id) {
                        return Err(StorageError::Conflict);
                    }
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
                    return self.fence_cluster_catalog(&loaded, fence, transition).await;
                }
                Root::Cluster(loaded) => {
                    if loaded.catalog.owner_cluster.is_some() {
                        return Err(StorageError::Conflict);
                    }
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
        if !catalog.indexed() {
            catalog.format_version = 5;
        }
        catalog.owner_cluster = Some(cluster_id);
        catalog.fence = Some(fence);
        let condition = token
            .as_deref()
            .map_or(WriteCondition::Create, WriteCondition::Match);
        match self
            .put(&ObjectKey::catalog(), catalog.encode()?, condition)
            .await
        {
            Ok(etag) if !etag.is_empty() => LoadedClusterCatalog::new(catalog, etag),
            Ok(_) | Err(StorageError::Ambiguous) => {
                let current = self.load_cluster_catalog().await?;
                if current.catalog.owner_cluster == Some(cluster_id)
                    && current.catalog.fence == Some(fence)
                {
                    Ok(current)
                } else {
                    Err(StorageError::Ambiguous)
                }
            }
            Err(error) => Err(error),
        }
    }

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
                Root::Cluster(loaded) => {
                    if loaded.catalog.owner_cluster.is_some() {
                        return Err(StorageError::Conflict);
                    }
                    return Ok(loaded);
                }
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
                Root::Cluster(loaded)
                    if loaded.catalog.fence.is_some() && loaded.catalog.owner_cluster.is_none() =>
                {
                    if cold {
                        return self
                            .fence_cluster_catalog(&loaded, fence, FenceTransition::ColdStart)
                            .await;
                    }
                    return Ok(loaded);
                }
                Root::Cluster(loaded) => {
                    if loaded.catalog.owner_cluster.is_some() {
                        return Err(StorageError::Conflict);
                    }
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
        catalog.format_version = fenced_format(&catalog);
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
        catalog.format_version = fenced_format(&catalog);
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
        self.lookup_planned(loaded, request_id, request_body, budget, false)
            .await
            .map(|(lookup, _)| lookup)
    }

    /// Lookup that also returns what a commit on `loaded` needs to publish
    /// the next index. `collect` keeps the legacy chain's pairs in memory.
    pub(crate) async fn lookup_planned(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        request_body: Option<&[u8]>,
        budget: HistoryBudget,
        collect: bool,
    ) -> Result<(HistoryLookup, IndexBase), StorageError> {
        budget.check_deadline()?;
        if loaded.catalog.indexed() {
            let root = loaded.catalog.index_root()?;
            let lookup = self
                .lookup_indexed(loaded, root, request_id, request_body, budget)
                .await?;
            return Ok((lookup, IndexBase::Index(root)));
        }
        let mut walked = Vec::new();
        let walk = self
            .walk_chain(loaded, request_id, request_body, budget, &mut walked)
            .await;
        let fenced = loaded.catalog.fence.is_some();
        match walk {
            Ok(lookup) if fenced && collect => Ok((
                lookup,
                IndexBase::Chain(walked.into_iter().map(|(r, d, _)| (r, d)).collect()),
            )),
            Ok(lookup) => Ok((
                lookup,
                if fenced {
                    IndexBase::Chain(Vec::new())
                } else {
                    IndexBase::Unindexed
                },
            )),
            // OLAP-1: a fenced chain longer than any request budget is indexed
            // with resumable work, and the lookup is answered from that index.
            Err((StorageError::LimitExceeded, cursor)) if fenced => {
                let root = self
                    .migrate_long_chain(loaded, walked, cursor, budget)
                    .await?;
                let lookup = self
                    .lookup_indexed(loaded, root, request_id, request_body, budget)
                    .await?;
                Ok((lookup, IndexBase::Index(root)))
            }
            Err((error, _)) => Err(error),
        }
    }

    /// Index a legacy chain that no single lookup budget can walk, then try to
    /// publish the migrated root with one conditional PUT on the captured
    /// ETag. The index is exact for `loaded` whichever root wins, so the
    /// caller may answer from it either way.
    async fn migrate_long_chain(
        &self,
        loaded: &LoadedClusterCatalog,
        walked: Vec<(Uuid, Uuid, u64)>,
        cursor: Option<(Uuid, u64)>,
        budget: HistoryBudget,
    ) -> Result<Option<PageHash>, StorageError> {
        let Some(head) = loaded.catalog.decision_head else {
            return Err(StorageError::LimitExceeded);
        };
        let deadline = budget.deadline();
        // Another process may have finished this exact migration already.
        if let Ok(Ok(current)) = timeout_at(deadline, self.load_cluster_catalog()).await
            && current.catalog.indexed()
            && current.catalog.decision_head == Some(head)
            && current.catalog.revision == loaded.catalog.revision
        {
            return current.catalog.index_root();
        }
        let built = self
            .build_chain_index(head, loaded.catalog.revision, (walked, cursor), deadline)
            .await?;
        let Some((root, pages)) = built else {
            return Err(StorageError::InvalidCatalog);
        };
        self.put_index_pages(pages, deadline).await?;
        let mut migrated = loaded.catalog.clone();
        migrated.format_version = 6;
        migrated.decision_index = Some(hex(&root));
        // The private index is exact for `loaded`, so the outcome of this PUT
        // changes only which root later lookups start from.
        let _ = timeout_at(
            deadline,
            self.put(
                &ObjectKey::catalog(),
                migrated.encode()?,
                WriteCondition::Match(loaded.etag()),
            ),
        )
        .await;
        Ok(Some(root))
    }

    /// Indexed lookup: at most 16 validated pages and one decision.
    async fn lookup_indexed(
        &self,
        loaded: &LoadedClusterCatalog,
        root: Option<PageHash>,
        request_id: Uuid,
        request_body: Option<&[u8]>,
        budget: HistoryBudget,
    ) -> Result<HistoryLookup, StorageError> {
        let Some(root) = root else {
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

    /// The legacy walk. Records every visited `(request, decision, revision)`
    /// into `walked`; on failure also returns the next unread position.
    async fn walk_chain(
        &self,
        loaded: &LoadedClusterCatalog,
        request_id: Uuid,
        request_body: Option<&[u8]>,
        budget: HistoryBudget,
        walked: &mut Vec<(Uuid, Uuid, u64)>,
    ) -> Result<HistoryLookup, (StorageError, Option<(Uuid, u64)>)> {
        budget.check_deadline().map_err(|e| (e, None))?;
        let mut next = loaded.catalog.decision_head;
        let mut revision = loaded.catalog.revision;
        let mut records = 0usize;
        let mut bytes = 0usize;
        while let Some(id) = next {
            let cursor = Some((id, revision));
            let fail = |error| (error, cursor);
            budget.check_deadline().map_err(fail)?;
            records = records
                .checked_add(1)
                .ok_or(StorageError::LimitExceeded)
                .map_err(fail)?;
            if records > budget.max_records {
                return Err(fail(StorageError::LimitExceeded));
            }
            let remaining = budget
                .max_bytes
                .checked_sub(bytes)
                .ok_or(StorageError::LimitExceeded)
                .map_err(fail)?;
            let object = timeout_at(
                budget.deadline,
                self.read(&ObjectKey::decision(id), MAX_DECISION_BYTES.min(remaining)),
            )
            .await
            .map_err(|_| fail(StorageError::Unavailable))?
            .map_err(|error| {
                fail(if error == StorageError::NotFound {
                    StorageError::InvalidCatalog
                } else {
                    error
                })
            })?;
            budget.check_deadline().map_err(fail)?;
            bytes = bytes
                .checked_add(object.bytes.len())
                .ok_or(StorageError::LimitExceeded)
                .map_err(fail)?;
            let decision = Decision::decode(&object.bytes).map_err(|e| (e, None))?;
            if decision.id != id
                || decision.revision != revision
                || revision.checked_sub(1) != Some(decision.base_revision)
            {
                return Err((StorageError::InvalidCatalog, None));
            }
            budget.check_deadline().map_err(fail)?;
            if decision.request_id == request_id {
                return Ok(
                    if request_body.is_none_or(|body| decision.request_body.as_bytes() == body) {
                        HistoryLookup::Found(decision)
                    } else {
                        HistoryLookup::IdentityConflict
                    },
                );
            }
            walked.push((decision.request_id, decision.id, decision.revision));
            // Strict decrease detects cycles without a visited-ID allocation.
            revision = decision.base_revision;
            next = decision.parent;
        }
        budget.check_deadline().map_err(|e| (e, None))?;
        Ok(HistoryLookup::Absent)
    }
}
