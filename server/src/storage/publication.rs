//! Metadata-only publication against one captured v3 root. No execution or table
//! uploads happen here. See PreparedPublication for the caller's sequencing duty.
use super::{
    CatalogEntry, CatalogFence, ClusterCatalog, Decision, HistoryBudget, HistoryLookup,
    LoadedClusterCatalog, MAX_DECISION_BYTES, ObjectKey, ObjectStore, Storage, StorageError,
    TableData, WriteCondition,
};
use std::{collections::BTreeMap, future::Future};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

/// A result for this attempt, not an HTTP outcome for the logical request.
/// NeedsRecompute never permits reuse of this candidate against a newer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationOutcome {
    Terminal(Decision),
    IdentityConflict,
    NeedsRecompute,
    Unknown,
}

/// Immutable, fully encoded metadata. Preparation performs no I/O and does not
/// establish that a mutation was executed or its table objects were uploaded.
///
/// Caller sequence: authorize and complete replay lookup; compute and validate
/// rows/schema/final response; prepare; verify/upload newly referenced immutable
/// table objects; consume this candidate to publish metadata in the SAME namespace.
/// The caller owns table-object ownership, retained-object facts and upload checks.
/// Once root submission may have started, cancellation/drop cannot prove rollback.
/// Retain admitted work and reconcile it across disconnects, as in the executor.
#[must_use]
pub struct PreparedPublication {
    base: LoadedClusterCatalog,
    decision: Decision,
    root_bytes: Vec<u8>,
    decision_bytes: Vec<u8>,
}
impl std::fmt::Debug for PreparedPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedPublication")
            .field("commit_id", &self.decision.id)
            .field("revision", &self.decision.revision)
            .finish_non_exhaustive()
    }
}
impl PreparedPublication {
    pub fn prepare(
        base: LoadedClusterCatalog,
        tables: BTreeMap<String, CatalogEntry>,
        decision: Decision,
    ) -> Result<Self, StorageError> {
        if base.catalog().fence.is_some() {
            return Err(StorageError::Conflict);
        }
        Self::prepare_inner(base, tables, decision)
    }
    /// Capture the expected owner from admitted work, never from a fresh root.
    pub fn prepare_fenced(
        base: LoadedClusterCatalog,
        tables: BTreeMap<String, CatalogEntry>,
        decision: Decision,
        expected: CatalogFence,
    ) -> Result<Self, StorageError> {
        expected.validate()?;
        if base.catalog().fence != Some(expected) || base.catalog().format_version != 4 {
            return Err(StorageError::Conflict);
        }
        Self::prepare_inner(base, tables, decision)
    }
    fn prepare_inner(
        base: LoadedClusterCatalog,
        tables: BTreeMap<String, CatalogEntry>,
        decision: Decision,
    ) -> Result<Self, StorageError> {
        let old = base.catalog();
        if decision.id == old.commit_id
            || decision.parent != old.decision_head
            || decision.base_revision != old.revision
            || old.revision.checked_add(1) != Some(decision.revision)
        {
            return Err(StorageError::InvalidCatalog);
        }
        let mut changed = old
            .tables
            .keys()
            .filter(|name| old.tables.get(*name) != tables.get(*name))
            .chain(tables.keys().filter(|name| !old.tables.contains_key(*name)));
        if let Some(name) = changed.next() {
            if changed.next().is_some() || decision.response_status != 200 {
                return Err(StorageError::InvalidCatalog);
            }
            if let (Some(previous), Some(entry)) = (old.tables.get(name), tables.get(name))
                && (previous.descriptor(name) != entry.descriptor(name)
                    || matches!(
                        (&previous.data, &entry.data),
                        (TableData::Partitioned { .. }, TableData::Legacy { .. })
                    ))
            {
                return Err(StorageError::InvalidCatalog);
            }
        }
        let root = ClusterCatalog {
            format_version: old.format_version,
            revision: decision.revision,
            commit_id: decision.id,
            tables,
            decision_head: Some(decision.id),
            fence: old.fence,
        };
        let decision_bytes = decision.encode()?;
        let root_bytes = root.encode()?;
        Ok(Self {
            base,
            decision,
            root_bytes,
            decision_bytes,
        })
    }

    pub fn commit_id(&self) -> Uuid {
        self.decision.id
    }

    /// At most one Create and one Match. Each history scan gets the supplied
    /// count/byte caps; at most two scans share the ONE absolute deadline. Err is
    /// a pre-root failure of THIS attempt, never proof about other same-ID work.
    pub async fn publish<B: ObjectStore>(
        self,
        storage: &Storage<B>,
        budget: HistoryBudget,
    ) -> Result<PublicationOutcome, StorageError> {
        match storage
            .lookup_decision(
                &self.base,
                self.decision.request_id,
                self.decision.request_body.as_bytes(),
                budget,
            )
            .await?
        {
            HistoryLookup::Found(d) => return Ok(PublicationOutcome::Terminal(d)),
            HistoryLookup::IdentityConflict => return Ok(PublicationOutcome::IdentityConflict),
            HistoryLookup::Absent => {}
        }
        let key = ObjectKey::decision(self.decision.id);
        match bounded(
            budget,
            storage.put(&key, self.decision_bytes.clone(), WriteCondition::Create),
        )
        .await
        {
            Ok(token) if !token.is_empty() => {}
            Ok(_) | Err(StorageError::Conflict | StorageError::Ambiguous) => {
                let object = bounded(budget, storage.read(&key, MAX_DECISION_BYTES)).await?;
                if object.bytes != self.decision_bytes {
                    return Err(StorageError::InvalidCatalog);
                }
            }
            Err(error) => return Err(error),
        }
        // Check before constructing/polling the PUT. A timeout after this point
        // can represent a pending server-side write even when the future drops.
        check_deadline(budget)?;
        let result = timeout_at(
            budget.deadline(),
            storage.put(
                &ObjectKey::catalog(),
                self.root_bytes,
                WriteCondition::Match(self.base.etag()),
            ),
        )
        .await;
        let conflict = match result {
            Ok(Ok(token)) if !token.is_empty() => {
                return Ok(PublicationOutcome::Terminal(self.decision));
            }
            Ok(Err(StorageError::Conflict)) => true,
            _ => false,
        };
        // A fresh root may have superseded our commit. Search its entire reachable
        // chain, never the orphan alone and never just the latest commit UUID.
        let recovered = async {
            let root = bounded(budget, storage.load_cluster_catalog()).await?;
            storage
                .lookup_decision(
                    &root,
                    self.decision.request_id,
                    self.decision.request_body.as_bytes(),
                    budget,
                )
                .await
        }
        .await;
        Ok(match recovered {
            Ok(HistoryLookup::Found(d)) => PublicationOutcome::Terminal(d),
            Ok(HistoryLookup::IdentityConflict) => PublicationOutcome::IdentityConflict,
            Ok(HistoryLookup::Absent) if conflict => PublicationOutcome::NeedsRecompute,
            _ => PublicationOutcome::Unknown,
        })
    }
}
fn check_deadline(budget: HistoryBudget) -> Result<(), StorageError> {
    if Instant::now() >= budget.deadline() {
        Err(StorageError::Unavailable)
    } else {
        Ok(())
    }
}
async fn bounded<T>(
    budget: HistoryBudget,
    work: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    check_deadline(budget)?;
    let result = timeout_at(budget.deadline(), work)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    check_deadline(budget)?;
    result
}
