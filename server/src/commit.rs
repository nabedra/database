//! Serialized one-table publication. Share one `Arc<Coordinator<_>>` per engine.
//! Callers must retain the session while loading, validating and encoding rows.
//! Do not abort admitted mutation work, and await all blocking work before releasing
//! the session. The executor must keep it alive across client disconnects.
use crate::{
    protocol::{ErrorCode, Outcome, ProtocolError},
    storage::{
        Catalog, LoadedCatalog, ObjectKey, ObjectStore, Storage, StorageError, TableData,
        WriteCondition,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// Temporary Legacy engine allowance only. Partition files use the separate
/// 64 MiB cap and must match their manifest byte_size.
pub const MAX_ENCODED_SNAPSHOT_BYTES: usize = 80 * 1024 * 1024;

/// A closed, validated object ready for immutable publication. The caller owns
/// codec/schema/row validation; publication checks ownership and byte length.
pub struct PendingObject {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
}

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
            commit_id: nabedra_rt::ids::uuid_v4(),
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
        candidate.format_version = 2;
        candidate.revision = self.revision;
        candidate.commit_id = self.commit_id;
        candidate
    }
    /// Compatibility path for the temporary single-snapshot Legacy engine.
    /// Uses the same validation, upload and reconciliation core as partitions,
    /// with the original 80 MiB snapshot allowance. None is valid only for drop.
    pub async fn publish(
        self,
        candidate: Catalog,
        snapshot: Option<Vec<u8>>,
    ) -> Result<LoadedCatalog, ProtocolError> {
        let objects = match snapshot {
            Some(bytes) => {
                let key = candidate
                    .tables
                    .values()
                    .find_map(|entry| {
                        let key = ObjectKey::snapshot(entry.id, self.commit_id);
                        (entry.legacy_snapshot().ok() == Some(key.as_str())).then_some(key)
                    })
                    .ok_or_else(|| {
                        failure(ErrorCode::ValidationError, "invalid legacy publication")
                    })?;
                vec![PendingObject { key, bytes }]
            }
            None => vec![],
        };
        self.publish_objects(candidate, objects, true).await
    }

    /// Publish exactly the newly introduced partition references, retaining the
    /// session lock through all uploads and catalog reconciliation. Empty creates,
    /// deletion to empty, retained-file changes and drops may need no objects.
    /// Caller supplies closed Parquet with validated rows/schema and manifest
    /// accounting. This layer does not decode objects. All validation and bounded
    /// catalog encoding finish before any PUT. Uploads are sequential and awaited.
    pub async fn publish_partitions(
        self,
        candidate: Catalog,
        objects: Vec<PendingObject>,
    ) -> Result<LoadedCatalog, ProtocolError> {
        self.publish_objects(candidate, objects, false).await
    }

    async fn publish_objects(
        self,
        candidate: Catalog,
        objects: Vec<PendingObject>,
        legacy: bool,
    ) -> Result<LoadedCatalog, ProtocolError> {
        let invalid = || failure(ErrorCode::ValidationError, "invalid publication candidate");
        if candidate.format_version != 2
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
        let previous = old.get(name);
        let entry = new.get(name);
        if let (Some(previous), Some(entry)) = (previous, entry)
            && (previous.descriptor(name) != entry.descriptor(name)
                || matches!(
                    (&previous.data, &entry.data),
                    (TableData::Partitioned { .. }, TableData::Legacy { .. })
                ))
        {
            return Err(invalid());
        }
        // New objects must be owned by this session. Retained objects can only
        // come from the affected table's admitted manifest. Routing bounds may
        // change when an adjacent partition becomes empty; physical facts cannot.
        let mut expected = BTreeMap::new();
        let commit_path = self.commit_id.to_string();
        match entry.map(|entry| &entry.data) {
            None => {}
            Some(TableData::Legacy { snapshot }) if legacy => {
                let entry = entry.unwrap();
                if *snapshot != ObjectKey::snapshot(entry.id, self.commit_id).as_str() {
                    return Err(invalid());
                }
                expected.insert(snapshot.as_str(), None);
            }
            Some(TableData::Partitioned { files }) if !legacy => {
                let retained: BTreeMap<_, _> = match previous.map(|entry| &entry.data) {
                    Some(TableData::Partitioned { files }) => files
                        .iter()
                        .map(|file| (file.object.as_str(), file))
                        .collect(),
                    _ => BTreeMap::new(),
                };
                for file in files {
                    if let Some(old) = retained.get(file.object.as_str()) {
                        if (file.row_count, file.byte_size, file.payload_bytes)
                            != (old.row_count, old.byte_size, old.payload_bytes)
                        {
                            return Err(invalid());
                        }
                    } else {
                        ObjectKey::parse_partition(&file.object, entry.unwrap().id)
                            .map_err(|_| invalid())?;
                        if file.object.split('/').nth(2) != Some(commit_path.as_str()) {
                            return Err(invalid());
                        }
                        if expected
                            .insert(file.object.as_str(), Some(file.byte_size))
                            .is_some()
                        {
                            return Err(invalid());
                        }
                    }
                }
            }
            _ => return Err(invalid()),
        }
        let mut supplied = BTreeSet::new();
        for object in &objects {
            if !supplied.insert(object.key.as_str()) {
                return Err(invalid());
            }
            let size = expected.get(object.key.as_str()).ok_or_else(invalid)?;
            if object.bytes.is_empty() {
                return Err(invalid());
            }
            match size {
                Some(size) => {
                    if object.bytes.len() > crate::partition_size::MAX_PARTITION_BYTES {
                        return Err(failure(
                            ErrorCode::LimitExceeded,
                            "partition exceeds 64 MiB",
                        ));
                    }
                    if object.bytes.len() as u64 != *size {
                        return Err(invalid());
                    }
                }
                None if object.bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES => {
                    return Err(failure(
                        ErrorCode::LimitExceeded,
                        "encoded snapshot exceeds 80 MiB",
                    ));
                }
                None => {}
            }
        }
        if supplied.len() != expected.len() {
            return Err(invalid());
        }
        // Authoritative catalog validation and the 1 MiB serialization bound
        // precede every PUT, including all immutable object uploads.
        let bytes = candidate.encode().map_err(|e| match e {
            StorageError::LimitExceeded => {
                failure(ErrorCode::LimitExceeded, "catalog exceeds 1 MiB")
            }
            _ => invalid(),
        })?;
        let storage = &self.coordinator.storage;
        for object in objects {
            storage
                .put(&object.key, object.bytes, WriteCondition::Create)
                .await
                .map_err(|error| {
                    if error == StorageError::Conflict {
                        storage_failure(error)
                    } else {
                        failure(ErrorCode::StorageUnavailable, "object upload failed")
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
