//! Shared synchronous file validation and bounded indexed worker dispatch.
use crate::worker_pool::WorkerRuntime;
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
    runtime: &WorkerRuntime,
    storage: &Storage<B>,
    table: &TableDescriptor,
    file: &ReadFile,
) -> Result<LoadedRows, ProtocolError> {
    let (object, mut cap, count) = match file {
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
    let count = usize::try_from(count).map_err(|_| corrupt())?;
    match file {
        ReadFile::Partition(f) => {
            cap = usize::try_from(f.byte_size).map_err(|_| corrupt())?;
            snapshot::check_memory_shape(
                table,
                count,
                usize::try_from(f.payload_bytes).map_err(|_| corrupt())?,
                cap,
            )?;
        }
        ReadFile::Legacy { .. } => {
            cap = legacy_input_cap(table, count, cap)?;
        }
    }
    let object = runtime
        .block_on(storage.read(&object.map_err(storage_failure)?, cap))
        .map_err(storage_failure)?;
    if let ReadFile::Partition(f) = file
        && object.bytes.len() as u64 != f.byte_size
    {
        return Err(corrupt());
    }
    let rows = snapshot::decode(table, Bytes::from(object.bytes), Some(count as u64))?;
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

fn legacy_input_cap(
    table: &TableDescriptor,
    count: usize,
    maximum: usize,
) -> Result<usize, ProtocolError> {
    let base = snapshot::memory_shape(table, count, 0, 0)?;
    crate::scheduling::check_task_memory(base)?;
    // Legacy catalogs have no encoded/payload sizes. Bound the download itself,
    // then the codec validates actual metadata and decoded payload before growth.
    Ok(
        crate::scheduling::task_memory_limit().map_or(maximum, |budget| {
            maximum.min((budget.saturating_sub(base) / 4) as usize)
        }),
    )
}

pub(crate) struct DownloadedFile {
    file: ReadFile,
    bytes: Vec<u8>,
    charge: crate::scheduling::MemoryCharge,
}
impl DownloadedFile {
    pub(crate) fn into_parts(self) -> (ReadFile, Vec<u8>, crate::scheduling::MemoryCharge) {
        (self.file, self.bytes, self.charge)
    }
}
pub(crate) async fn download_file<B: ObjectStore>(
    storage: &Storage<B>,
    pool: &WorkerPool,
    request: u64,
    table: &TableDescriptor,
    file: ReadFile,
) -> Result<DownloadedFile, ProtocolError> {
    let budget = pool.resources().task_memory_limit_bytes;
    download_file_reserving(storage, pool, request, table, file, budget).await
}

/// Download one file while holding `reserve` bytes of the worker's memory,
/// at most its task budget. The shape check still uses the whole budget.
pub(crate) async fn download_file_reserving<B: ObjectStore>(
    storage: &Storage<B>,
    pool: &WorkerPool,
    request: u64,
    table: &TableDescriptor,
    file: ReadFile,
    reserve: u64,
) -> Result<DownloadedFile, ProtocolError> {
    let budget = pool.resources().task_memory_limit_bytes;
    let reserve = reserve.min(budget);
    let (object, cap) = match &file {
        ReadFile::Legacy { object, row_count } => {
            let cap = crate::scheduling::with_task_limit(budget, || {
                legacy_input_cap(
                    table,
                    usize::try_from(*row_count).map_err(|_| corrupt())?,
                    snapshot::MAX_ENCODED_SNAPSHOT_BYTES,
                )
            })?;
            (ObjectKey::parse_snapshot(object, table.id), cap)
        }
        ReadFile::Partition(f) => {
            crate::scheduling::with_task_limit(budget, || {
                snapshot::check_memory_shape(
                    table,
                    usize::try_from(f.row_count).map_err(|_| corrupt())?,
                    usize::try_from(f.payload_bytes).map_err(|_| corrupt())?,
                    usize::try_from(f.byte_size).map_err(|_| corrupt())?,
                )
            })?;
            (
                ObjectKey::parse_partition(&f.object, table.id),
                usize::try_from(f.byte_size).map_err(|_| corrupt())?,
            )
        }
    };
    let object = object.map_err(storage_failure)?;
    let charge = pool.scheduler().reserve_memory(request, 0, reserve).await?;
    let object = storage.read(&object, cap).await.map_err(storage_failure)?;
    if let ReadFile::Partition(f) = &file
        && object.bytes.len() as u64 != f.byte_size
    {
        return Err(corrupt());
    }
    Ok(DownloadedFile {
        file,
        bytes: object.bytes,
        charge,
    })
}
pub(crate) fn decode_downloaded(
    table: &TableDescriptor,
    file: &ReadFile,
    bytes: Vec<u8>,
) -> Result<LoadedRows, ProtocolError> {
    let count = match file {
        ReadFile::Legacy { row_count, .. } => *row_count,
        ReadFile::Partition(f) => f.row_count,
    };
    let rows = snapshot::decode(table, Bytes::from(bytes), Some(count))?;
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
pub(super) struct Buffered<T> {
    pub value: T,
    pub charge: crate::scheduling::MemoryCharge,
}
/// Download under memory-only reservations, then acquire CPU reusing that memory.
/// Each completion immediately starts another independent download/compute pipeline.
pub(super) async fn map_prepared<B, I, T, S, F>(
    pool: &WorkerPool,
    storage: &Storage<B>,
    table: TableDescriptor,
    inputs: Vec<(usize, I)>,
    select: S,
    work: F,
) -> Result<Vec<Buffered<T>>, ProtocolError>
where
    B: ObjectStore,
    I: Send + 'static,
    T: Send + 'static,
    S: Fn(&I) -> Option<ReadFile>,
    F: Fn(I, LoadedRows) -> Result<(T, u64), ProtocolError> + Send + Sync + 'static,
{
    use futures_util::{StreamExt, stream::FuturesUnordered};
    let request = pool.request_id();
    let work = Arc::new(work);
    let mut pending = inputs.into_iter();
    let mut running = FuturesUnordered::new();
    let mut output = Vec::new();
    let mut error = None;
    loop {
        while error.is_none() && running.len() < pool.worker_count().get() {
            let Some((index, input)) = pending.next() else {
                break;
            };
            let selected = select(&input);
            let work = work.clone();
            let table = table.clone();
            running.push(async move {
                let prepared = match selected {
                    Some(file) => Some(download_file(storage, pool, request, &table, file).await?),
                    None => None,
                };
                let (file, bytes, charge) = match prepared {
                    Some(file) => {
                        let (f, b, c) = file.into_parts();
                        (Some(f), b, c)
                    }
                    None => (
                        None,
                        Vec::new(),
                        pool.scheduler()
                            .reserve_memory(request, 0, pool.resources().task_memory_limit_bytes)
                            .await?,
                    ),
                };
                let job = pool
                    .submit_buffered(
                        request,
                        1,
                        vec![charge],
                        pool.resources().task_memory_limit_bytes,
                        move |_, _| {
                            let loaded = match file {
                                Some(file) => decode_downloaded(&table, &file, bytes)?,
                                None => LoadedRows {
                                    rows: Vec::new(),
                                    payload_bytes: 0,
                                },
                            };
                            work(input, loaded)
                        },
                    )
                    .await?;
                let ((value, retained), mut charge) = job.receive().await?;
                charge.shrink_to(retained)?;
                Ok::<_, ProtocolError>((index, Buffered { value, charge }))
            });
        }
        let Some(result) = running.next().await else {
            break;
        };
        match result {
            Ok(value) => output.push(value),
            Err(e) => {
                if error.is_none() {
                    error = Some(e)
                }
            }
        }
    }
    if let Some(error) = error {
        return Err(error);
    }
    output.sort_by_key(|(index, _)| *index);
    Ok(output.into_iter().map(|(_, value)| value).collect())
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
