//! Serialized one-table publication. Share one `Arc<Coordinator<_>>` per engine.
//! Callers must retain the session while loading, validating and encoding rows.
//! Do not abort admitted mutation work, and await all blocking work before releasing
//! the session. The executor must keep it alive across client disconnects.
use crate::{
    protocol::{ErrorCode, Outcome, ProtocolError},
    storage::{
        Catalog, LoadedCatalog, ObjectKey, ObjectStore, Storage, StorageError, WriteCondition,
    },
};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// Same 80 MiB encoded limit as R03. Kept here to avoid coupling publication to
/// codec internals. R05/R06 must also bound downloads before collecting bytes.
pub const MAX_ENCODED_SNAPSHOT_BYTES: usize = 80 * 1024 * 1024;

/// Owns the process-wide mutation lock and its storage. Intentionally not Clone;
/// clone an Arc to share it, never construct a coordinator per operation.
pub struct Coordinator<B> {
    storage: Storage<B>,
    mutation: Mutex<()>,
}
impl<B: ObjectStore> Coordinator<B> {
    pub fn new(storage: Storage<B>) -> Self {
        Self {
            storage,
            mutation: Mutex::new(()),
        }
    }
    /// Ordinary reads need no mutation lock. Mutations must use `begin` and
    /// `MutationSession::publish`; do not bypass them with direct Storage PUTs.
    pub fn storage(&self) -> &Storage<B> {
        &self.storage
    }

    /// Acquire the lock first, then load fresh validated metadata and opaque ETag.
    pub async fn begin(&self) -> Result<MutationSession<'_, B>, ProtocolError> {
        let guard = self.mutation.lock().await;
        let loaded = self.storage.load_catalog().await.map_err(storage_failure)?;
        let revision = loaded
            .catalog
            .revision
            .checked_add(1)
            .ok_or_else(|| failure(ErrorCode::LimitExceeded, "catalog revision exhausted"))?;
        Ok(MutationSession {
            coordinator: self,
            _guard: guard,
            loaded,
            revision,
            commit_id: Uuid::new_v4(),
        })
    }
}

/// A single operation's exclusive session. Dropping before publication makes no
/// database change. Cancellation during publication is unsafe; await completion.
#[must_use = "retain the mutation session until the operation completes"]
pub struct MutationSession<'a, B> {
    coordinator: &'a Coordinator<B>,
    _guard: MutexGuard<'a, ()>,
    loaded: LoadedCatalog,
    revision: u64,
    commit_id: Uuid,
}
impl<B: ObjectStore> MutationSession<'_, B> {
    pub fn catalog(&self) -> &Catalog {
        &self.loaded.catalog
    }
    pub fn commit_id(&self) -> Uuid {
        self.commit_id
    }
    /// Detached candidate with server-assigned publication metadata. Change only
    /// one table. A no-op should simply drop this session without publication.
    pub fn candidate(&self) -> Catalog {
        let mut candidate = self.loaded.catalog.clone();
        candidate.revision = self.revision;
        candidate.commit_id = self.commit_id;
        candidate
    }
    /// Consumes the session, retaining its lock through reconciliation.
    ///
    /// Preconditions: snapshot bytes are fully validated, closed encoded Parquet;
    /// their schema and row count agree with the candidate. An empty table must
    /// supply valid empty Parquet. This layer does not decode or validate rows.
    /// Supply exactly one snapshot for a create/data change, or None for a drop.
    /// Existing table identity, schema, key and format are immutable in v0.
    pub async fn publish(
        self,
        candidate: Catalog,
        snapshot: Option<Vec<u8>>,
    ) -> Result<LoadedCatalog, ProtocolError> {
        let invalid = || failure(ErrorCode::ValidationError, "invalid publication candidate");
        if candidate.format_version != self.loaded.catalog.format_version
            || candidate.revision != self.revision
            || candidate.commit_id != self.commit_id
        {
            return Err(invalid());
        }
        let old = &self.loaded.catalog.tables;
        let new = &candidate.tables;
        let mut changed = old
            .keys()
            .filter(|name| old.get(*name) != new.get(*name))
            .chain(new.keys().filter(|name| !old.contains_key(*name)));
        let name = changed.next().ok_or_else(invalid)?;
        if changed.next().is_some() {
            return Err(invalid());
        }
        let key = match (old.get(name), new.get(name), snapshot.as_ref()) {
            (Some(_), None, None) => None,
            (previous, Some(entry), Some(bytes)) => {
                if bytes.is_empty() {
                    return Err(invalid());
                }
                if bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES {
                    return Err(failure(
                        ErrorCode::LimitExceeded,
                        "encoded snapshot exceeds 80 MiB",
                    ));
                }
                if previous.is_some_and(|p| p.descriptor(name) != entry.descriptor(name)) {
                    return Err(invalid());
                }
                let key = ObjectKey::snapshot(entry.id, self.commit_id);
                if entry.snapshot != key.as_str() {
                    return Err(invalid());
                }
                Some(key)
            }
            _ => return Err(invalid()),
        };
        // Authoritative catalog validation and the 1 MiB serialization bound
        // precede every PUT, including the immutable snapshot upload.
        let bytes = candidate.encode().map_err(|e| match e {
            StorageError::LimitExceeded => {
                failure(ErrorCode::LimitExceeded, "catalog exceeds 1 MiB")
            }
            _ => invalid(),
        })?;
        let storage = &self.coordinator.storage;
        if let (Some(key), Some(snapshot)) = (key, snapshot) {
            storage
                .put(&key, snapshot, WriteCondition::Create)
                .await
                .map_err(|error| {
                    if error == StorageError::Conflict {
                        storage_failure(error)
                    } else {
                        failure(ErrorCode::StorageUnavailable, "snapshot upload failed")
                    }
                })?;
        }
        match storage
            .put(
                &ObjectKey::catalog(),
                bytes,
                WriteCondition::Match(&self.loaded.etag),
            )
            .await
        {
            Ok(etag) => Ok(LoadedCatalog {
                catalog: candidate,
                etag,
            }),
            Err(StorageError::Ambiguous) => {
                // Exactly one fresh validated read. Even an older catalog cannot
                // prove rollback. Never replay publication or clean up objects.
                match storage.load_catalog().await {
                    Ok(loaded) if loaded.catalog.commit_id == self.commit_id => Ok(loaded),
                    _ => Err(ProtocolError::new(
                        ErrorCode::OutcomeUnknown,
                        "catalog publication outcome is unknown",
                        Outcome::Unknown,
                    )),
                }
            }
            Err(error) => Err(storage_failure(error)),
        }
    }
}
fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn storage_failure(error: StorageError) -> ProtocolError {
    match error {
        StorageError::Conflict => failure(ErrorCode::Conflict, "conditional write conflict"),
        StorageError::LimitExceeded => failure(
            ErrorCode::LimitExceeded,
            "storage object exceeds byte limit",
        ),
        _ => failure(ErrorCode::StorageUnavailable, "storage is unavailable"),
    }
}
