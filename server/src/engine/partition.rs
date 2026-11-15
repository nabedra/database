//! Shared synchronous file validation and bounded indexed worker dispatch.
use crate::{
    partition_size::MAX_PARTITION_BYTES,
    protocol::{ErrorCode, Key, Outcome, ProtocolError, Row, Scalar, TableDescriptor},
    snapshot,
    storage::{
        CatalogEntry, KeyRange, ObjectKey, ObjectStore, PartitionFile, Storage, StorageError,
        TableData,
    },
    worker_pool::WorkerPool,
};
use bytes::Bytes;
use std::{cmp::Ordering, sync::Arc};
use tokio::runtime::Runtime;

#[derive(Clone)]
pub(super) enum ReadFile {
    Legacy { object: String, row_count: u64 },
    Partition(PartitionFile),
}

pub(super) fn files(entry: &CatalogEntry) -> Vec<(usize, ReadFile)> {
    match &entry.data {
        TableData::Legacy { snapshot } => vec![(
            0,
            ReadFile::Legacy {
                object: snapshot.clone(),
                row_count: entry.row_count,
            },
        )],
        TableData::Partitioned { files } => files
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, f)| (i, ReadFile::Partition(f)))
            .collect(),
    }
}

pub(super) struct LoadedRows {
    pub rows: Vec<Row>,
    pub payload_bytes: usize,
}

/// Call on a worker, outside block_on. Only the bounded storage download uses IO.
/// Metadata must belong to the operation's captured, validated catalog.
pub(super) fn load_file<B: ObjectStore>(
    runtime: &Runtime,
    storage: &Storage<B>,
    table: &TableDescriptor,
    file: &ReadFile,
) -> Result<LoadedRows, ProtocolError> {
    let (object, cap, count) = match file {
        ReadFile::Legacy { object, row_count } => (
            ObjectKey::parse_snapshot(object, table.id),
            snapshot::MAX_ENCODED_SNAPSHOT_BYTES,
            *row_count,
        ),
        ReadFile::Partition(f) => (
            ObjectKey::parse_partition(&f.object, table.id),
            MAX_PARTITION_BYTES,
            f.row_count,
        ),
    };
    let object = runtime
        .block_on(storage.read(&object.map_err(storage_failure)?, cap))
        .map_err(storage_failure)?;
    if let ReadFile::Partition(f) = file
        && object.bytes.len() as u64 != f.byte_size
    {
        return Err(corrupt());
    }
    let rows = snapshot::decode(table, Bytes::from(object.bytes), Some(count))?;
    let partition = match file {
        ReadFile::Partition(f) => Some((&f.range, f.payload_bytes)),
        ReadFile::Legacy { .. } => None,
    };
    let payload_bytes = validate_partition_rows(table, &rows, partition)?;
    Ok(LoadedRows {
        rows,
        payload_bytes,
    })
}

/// Validate decoded rows with optional partition payload/range/order metadata.
/// Legacy snapshots preserve their existing unrestricted key ordering.
pub(crate) fn validate_partition_rows(
    table: &TableDescriptor,
    rows: &[Row],
    partition: Option<(&KeyRange, u64)>,
) -> Result<usize, ProtocolError> {
    let payload_bytes = snapshot::validate_rows(table, rows)?;
    if let Some((range, expected_payload)) = partition {
        if payload_bytes as u64 != expected_payload {
            return Err(corrupt());
        }
        let mut previous: Option<Key> = None;
        for row in rows {
            let key = row_key(table, row);
            if !range
                .contains(&key, table.schema[&table.key])
                .map_err(storage_failure)?
            {
                return Err(corrupt());
            }
            if let Some(prev) = &previous {
                let order = match (prev, &key) {
                    (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
                    (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
                    _ => return Err(corrupt()),
                };
                if order != Ordering::Less {
                    return Err(corrupt());
                }
            }
            previous = Some(key);
        }
    }
    Ok(payload_bytes)
}

pub(crate) fn row_key(table: &TableDescriptor, row: &Row) -> Key {
    match &row[&table.key] {
        Scalar::Int64(n) => Key::Int64(*n),
        Scalar::Utf8(s) => Key::Utf8(s.clone()),
        _ => unreachable!("validated primary key"),
    }
}

/// At most N outstanding jobs. Drain every accepted job even after submission or
/// receive failure; return the earliest original ordinal error and stop windows.
pub(super) async fn map_files<T, F>(
    pool: &WorkerPool,
    files: Vec<(usize, ReadFile)>,
    work: F,
) -> Result<Vec<T>, ProtocolError>
where
    T: Send + 'static,
    F: Fn(ReadFile, &Runtime) -> Result<T, ProtocolError> + Send + Sync + 'static,
{
    map_indexed(pool, files, work).await
}

/// Generic form for mutation jobs carrying both captured file metadata and an
/// operation. Input indices retain original partition ordinals for worker
/// assignment and deterministic errors; callers supply ascending indices.
pub(super) async fn map_indexed<I, T, F>(
    pool: &WorkerPool,
    inputs: Vec<(usize, I)>,
    work: F,
) -> Result<Vec<T>, ProtocolError>
where
    I: Clone + Send + 'static,
    T: Send + 'static,
    F: Fn(I, &Runtime) -> Result<T, ProtocolError> + Send + Sync + 'static,
{
    let work = Arc::new(work);
    let mut output = Vec::new();
    for window in inputs.chunks(pool.worker_count().get()) {
        let mut jobs = Vec::new();
        let mut error = None;
        for (index, file) in window {
            let work = work.clone();
            let file = file.clone();
            match pool
                .submit(*index, move |_, runtime| work(file, runtime))
                .await
            {
                Ok(job) => jobs.push((*index, job)),
                Err(e) => {
                    error = Some((*index, e));
                    break;
                }
            }
        }
        for (index, job) in jobs {
            match job.receive().await {
                Ok(value) => output.push(value),
                Err(e) => {
                    if error.as_ref().is_none_or(|(ordinal, _)| index < *ordinal) {
                        error = Some((index, e));
                    }
                }
            }
        }
        if let Some((_, e)) = error {
            return Err(e);
        }
    }
    Ok(output)
}

pub(super) fn storage_failure(error: StorageError) -> ProtocolError {
    let (code, message) = match error {
        StorageError::LimitExceeded => {
            (ErrorCode::LimitExceeded, "stored object exceeds byte limit")
        }
        _ => (ErrorCode::StorageUnavailable, "stored data is unavailable"),
    };
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn corrupt() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "invalid stored partition",
        Outcome::NotApplied,
    )
}
