//! Bounded state export and staged generation activation. Consensus log keys
//! are outside the state namespace and are never overwritten by installation.
use super::{DATABASE_KEY, OltpEngine, active_namespace, codec, failure, storage_error};
use crate::protocol::{ErrorCode, MAX_BODY_BYTES, ProtocolError};
use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const MAGIC: &[u8; 16] = b"NABEDRA-SNAP-001";
const HEADER_LIMIT: usize = 64 * 1024;
const CHUNK_BYTES: usize = 1024 * 1024;
pub(super) const CURRENT_SNAPSHOT: &[u8] = b"\x00s";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotHeader {
    pub engine: String,
    pub format_version: u32,
    pub cluster_id: Uuid,
    pub group_id: Option<Uuid>,
    pub database: String,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct StoreSnapshot {
    pub file: Arc<File>,
    pub path: PathBuf,
    pub bytes: u64,
    pub header: SnapshotHeader,
}

impl OltpEngine {
    /// Capture schema, rows, dedup and applied state from one native snapshot.
    /// The caller owns retention of the resulting immutable file.
    pub async fn export_snapshot(
        &self,
        path: PathBuf,
        metadata: Value,
    ) -> Result<StoreSnapshot, ProtocolError> {
        let engine = self.clone();
        self.blocking(move || engine.export_snapshot_sync(&path, metadata))
            .await
    }

    pub(super) fn export_snapshot_sync(
        &self,
        path: &Path,
        metadata: Value,
    ) -> Result<StoreSnapshot, ProtocolError> {
        if self.requires_recovery() {
            return Err(super::recovery_unknown());
        }
        let snapshot = self.inner.db.snapshot();
        let namespace = active_namespace(&snapshot)?;
        let database = database_identity(self, &snapshot)?;
        let applied = snapshot
            .get(namespace.category(b'p'))
            .map_err(storage_error)?
            .map(|bytes| {
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid applied state"))
            })
            .transpose()?;
        let membership = snapshot
            .get(namespace.category(b'v'))
            .map_err(storage_error)?
            .map(|bytes| {
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid membership state"))
            })
            .transpose()?;
        let header = SnapshotHeader {
            engine: "oltp".into(),
            format_version: self.manifest().format_version,
            cluster_id: self.manifest().cluster_id,
            group_id: self.manifest().group_id,
            database,
            metadata: serde_json::json!({"applied":applied,"membership":membership,"extra":metadata}),
        };
        let bytes = super::encode(&header)?;
        if bytes.len() > HEADER_LIMIT {
            return Err(failure(
                ErrorCode::LimitExceeded,
                "snapshot metadata exceeds limit",
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io_error)?;
        let mut hash = Sha256::new();
        write_hash(&mut file, &mut hash, MAGIC)?;
        write_hash(&mut file, &mut hash, &(bytes.len() as u32).to_be_bytes())?;
        write_hash(&mut file, &mut hash, &bytes)?;
        let prefix = namespace.prefix();
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(codec::end(&prefix));
        for entry in snapshot.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), options)
        {
            let (key, value) = entry.map_err(storage_error)?;
            let suffix = &key[prefix.len()..];
            validate_lengths(
                suffix.len(),
                value.len(),
                self.resources().query_memory_bytes,
            )?;
            write_hash(&mut file, &mut hash, &(suffix.len() as u32).to_be_bytes())?;
            write_hash(&mut file, &mut hash, &(value.len() as u32).to_be_bytes())?;
            write_hash(&mut file, &mut hash, suffix)?;
            write_hash(&mut file, &mut hash, &value)?;
        }
        write_hash(&mut file, &mut hash, &[0_u8; 8])?;
        file.write_all(&hash.finalize()).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        if let Some(parent) = path.parent() {
            File::open(parent)
                .map_err(io_error)?
                .sync_all()
                .map_err(io_error)?;
        }
        let bytes = file.metadata().map_err(io_error)?.len();
        Ok(StoreSnapshot {
            file: Arc::new(file),
            path: path.to_owned(),
            bytes,
            header,
        })
    }

    /// The active generation changes only after every record and checksum have
    /// been persisted. Any earlier error leaves the previous state readable.
    pub async fn install_store_snapshot(
        &self,
        file: Arc<File>,
    ) -> Result<SnapshotHeader, ProtocolError> {
        if self.manifest().group_id.is_some() {
            return Err(failure(
                ErrorCode::UnsupportedOperation,
                "replica snapshot installation requires the consensus state machine",
            ));
        }
        let owner = self.writer_ownership().await;
        let engine = self.clone();
        self.blocking(move || {
            let _owner = owner;
            engine.install_snapshot_sync(file, None)
        })
        .await
    }

    pub(super) fn install_snapshot_sync(
        &self,
        file: Arc<File>,
        snapshot_reference: Option<Vec<u8>>,
    ) -> Result<SnapshotHeader, ProtocolError> {
        let _guard = self.writer();
        if self.requires_recovery() {
            return Err(super::recovery_unknown());
        }
        let current = self.inner.db.snapshot();
        let old = active_namespace(&current)?;
        let next = codec::Namespace(
            old.0
                .checked_add(1)
                .ok_or_else(|| failure(ErrorCode::LimitExceeded, "state generation exhausted"))?,
        );
        let mut reader = file.try_clone().map_err(io_error)?;
        reader.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let (mut hash, header) = read_header(&mut reader)?;
        if header.engine != "oltp"
            || header.format_version != self.manifest().format_version
            || header.cluster_id != self.manifest().cluster_id
            || header.group_id != self.manifest().group_id
        {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "snapshot engine, format, cluster or group mismatch",
            ));
        }
        let database = database_identity(self, &current)?;
        if database != header.database {
            return Err(failure(ErrorCode::Conflict, "snapshot database mismatch"));
        }
        // Recheck under the same writer lock as activation. Another cloned
        // adapter cannot advance acknowledged state between validation and flip.
        if self.manifest().group_id.is_some() {
            type LogId = openraft::type_config::alias::LogIdOf<crate::oltp_replication::TypeConfig>;
            let incoming: Option<LogId> =
                serde_json::from_value(header.metadata["applied"].clone()).map_err(|_| {
                    failure(
                        ErrorCode::StorageUnavailable,
                        "invalid snapshot applied position",
                    )
                })?;
            let previous = current
                .get(old.category(b'p'))
                .map_err(storage_error)?
                .map(|bytes| {
                    serde_json::from_slice::<LogId>(&bytes).map_err(|_| {
                        failure(
                            ErrorCode::StorageUnavailable,
                            "invalid current applied position",
                        )
                    })
                })
                .transpose()?;
            if previous.as_ref().is_some_and(|last| {
                incoming.as_ref().is_none_or(|next| {
                    next.index < last.index || (next.index == last.index && next != last)
                })
            }) {
                return Err(failure(
                    ErrorCode::Conflict,
                    "snapshot would replace newer or conflicting applied state",
                ));
            }
        }
        // Erase a previously interrupted staging generation. This never selects
        // the active range, even after a process crash between staged batches.
        let prefix = next.prefix();
        let mut batch = WriteBatch::default();
        batch.delete_range(&prefix, &codec::end(&prefix));
        self.durable_write(batch)?;
        let mut batch = WriteBatch::default();
        let mut buffered = 0_usize;
        let mut previous = Vec::new();
        let mut staged_applied = Value::Null;
        let mut staged_membership = Value::Null;
        loop {
            let mut lengths = [0_u8; 8];
            read_hash(&mut reader, &mut hash, &mut lengths)?;
            let key_len = u32::from_be_bytes(lengths[..4].try_into().unwrap()) as usize;
            let value_len = u32::from_be_bytes(lengths[4..].try_into().unwrap()) as usize;
            if key_len == 0 && value_len == 0 {
                break;
            }
            validate_lengths(key_len, value_len, self.resources().query_memory_bytes)?;
            let mut key = vec![0; key_len];
            let mut value = vec![0; value_len];
            read_hash(&mut reader, &mut hash, &mut key)?;
            read_hash(&mut reader, &mut hash, &mut value)?;
            if !matches!(
                key.first(),
                Some(b't' | b'n' | b'r' | b'd' | b'p' | b'v' | b'i' | b'a')
            ) || (!previous.is_empty() && previous >= key)
            {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "invalid or unordered snapshot key",
                ));
            }
            previous = key.clone();
            if matches!(key.first(), Some(b'p' | b'v')) {
                if key.len() != 1 {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "invalid reserved snapshot metadata key",
                    ));
                }
                let metadata: Value = serde_json::from_slice(&value).map_err(|_| {
                    failure(
                        ErrorCode::StorageUnavailable,
                        "invalid snapshot state metadata",
                    )
                })?;
                if key[0] == b'p' {
                    staged_applied = metadata;
                } else {
                    staged_membership = metadata;
                }
            }
            let mut physical = prefix.clone();
            physical.extend_from_slice(&key);
            buffered = buffered
                .checked_add(physical.len() + value.len())
                .ok_or_else(|| failure(ErrorCode::LimitExceeded, "snapshot buffer overflow"))?;
            batch.put(physical, value);
            if buffered >= CHUNK_BYTES {
                self.durable_write(batch)?;
                batch = WriteBatch::default();
                buffered = 0;
            }
        }
        let mut checksum = [0_u8; 32];
        reader.read_exact(&mut checksum).map_err(io_error)?;
        if hash.finalize().as_slice() != checksum {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "snapshot checksum mismatch",
            ));
        }
        let mut extra = [0_u8; 1];
        if reader.read(&mut extra).map_err(io_error)? != 0 {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "trailing snapshot bytes",
            ));
        }
        if staged_applied != header.metadata["applied"]
            || staged_membership != header.metadata["membership"]
        {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "snapshot header disagrees with its applied or membership records",
            ));
        }
        // The final staged records and activation pointer share one synchronous
        // batch, so recovering that pointer proves all earlier chunks are stable.
        batch.put(codec::ACTIVE, next.0.to_be_bytes());
        if let Some(reference) = snapshot_reference {
            batch.put(CURRENT_SNAPSHOT, reference);
        }
        self.durable_write(batch)?;
        // Old native snapshots still see the old range despite this tombstone.
        let mut retire = WriteBatch::default();
        retire.delete_range(old.prefix(), codec::end(&old.prefix()));
        // Failure of optional garbage collection must not undo activation.
        let _ = self.durable_write(retire);
        Ok(header)
    }
}

fn validate_lengths(key: usize, value: usize, budget: usize) -> Result<(), ProtocolError> {
    if key == 0
        || key > MAX_BODY_BYTES * 2 + 128
        || value > MAX_BODY_BYTES + 4096
        || key
            .checked_add(value)
            .and_then(|n| n.checked_mul(3))
            .is_none_or(|n| n > budget)
    {
        return Err(failure(
            ErrorCode::LimitExceeded,
            "snapshot record exceeds bounded buffer",
        ));
    }
    Ok(())
}
fn write_hash(file: &mut File, hash: &mut Sha256, bytes: &[u8]) -> Result<(), ProtocolError> {
    file.write_all(bytes).map_err(io_error)?;
    hash.update(bytes);
    Ok(())
}
fn read_hash(file: &mut File, hash: &mut Sha256, bytes: &mut [u8]) -> Result<(), ProtocolError> {
    file.read_exact(bytes).map_err(io_error)?;
    hash.update(bytes);
    Ok(())
}
pub(super) fn read_header(file: &mut File) -> Result<(Sha256, SnapshotHeader), ProtocolError> {
    let mut hash = Sha256::new();
    let mut magic = [0_u8; 16];
    read_hash(file, &mut hash, &mut magic)?;
    if &magic != MAGIC {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "foreign snapshot format",
        ));
    }
    let mut length = [0_u8; 4];
    read_hash(file, &mut hash, &mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > HEADER_LIMIT {
        return Err(failure(
            ErrorCode::LimitExceeded,
            "snapshot metadata exceeds limit",
        ));
    }
    let mut bytes = vec![0; length];
    read_hash(file, &mut hash, &mut bytes)?;
    let header = serde_json::from_slice(&bytes)
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid snapshot metadata"))?;
    Ok((hash, header))
}
fn io_error(_: std::io::Error) -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "snapshot file unavailable or truncated",
    )
}

fn database_identity(
    engine: &OltpEngine,
    snapshot: &rocksdb::Snapshot<'_>,
) -> Result<String, ProtocolError> {
    if let Some(database) = &engine.manifest().database {
        return Ok(database.clone());
    }
    let database = snapshot
        .get(DATABASE_KEY)
        .map_err(storage_error)?
        .ok_or_else(|| {
            failure(
                ErrorCode::ValidationError,
                "bind database before snapshot operations",
            )
        })?;
    String::from_utf8(database)
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid database binding"))
}
