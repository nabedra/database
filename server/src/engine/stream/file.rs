//! Read one capped immutable object under an accounted I/O reservation. A
//! compute worker validates and projects it, then returns only the local cache.
use super::{Cursor, PinnedScan, corrupt, encoding::BatchEncoder, error, resource};
use crate::{
    engine::{compare_rows, matches_filter, partition::storage_failure},
    protocol::{ErrorCode, ProtocolError, Row},
    scheduling::{MemoryCharge, with_task_limit},
    snapshot::{self, stream::ProjectedReader},
    storage::{ObjectStore, Storage, StorageError, TableData},
    worker_pool::WorkerPool,
};
use bytes::Bytes;

const RAW_ROWS_PER_JOB: usize = 1024;

pub(super) struct OpenFile {
    catalog_commit: uuid::Uuid,
    table_id: uuid::Uuid,
    file: usize,
    next_offset: u64,
    raw_end: u64,
    reader: Reader,
    charge: MemoryCharge,
}

enum Reader {
    Unchecked(Bytes),
    Projected(Box<ProjectedReader>),
    Legacy(Vec<Row>),
}

impl OpenFile {
    pub(super) async fn load<B: ObjectStore>(
        storage: &Storage<B>,
        pool: &WorkerPool,
        scan: &PinnedScan,
        cursor: Cursor,
        file_budget: u64,
    ) -> Result<Self, ProtocolError> {
        let file_rows = scan.file_rows(cursor.file)?;
        let read_rows = file_rows
            .checked_sub(cursor.raw_offset)
            .filter(|rows| *rows > 0)
            .ok_or_else(corrupt)?
            .min(cursor.raw_remaining);
        let (max_bytes, charge_bytes) = match &scan.data {
            TableData::Partitioned { files } => {
                let file = files.get(cursor.file).ok_or_else(corrupt)?;
                let metadata = 128 * 1024 + (scan.table.schema.len() as u64) * 4096;
                // Encoded input, bounded codec copies and projected Arrow
                // buffers, plus schema, row maps and per-column decoder state.
                let charged = file
                    .byte_size
                    .checked_mul(5)
                    .and_then(|bytes| bytes.checked_add(metadata))
                    .filter(|bytes| *bytes <= file_budget)
                    .ok_or_else(resource)?;
                (
                    usize::try_from(file.byte_size).map_err(|_| corrupt())?,
                    charged,
                )
            }
            TableData::Legacy { .. } => {
                let count = usize::try_from(file_rows).map_err(|_| corrupt())?;
                let base = snapshot::memory_shape(&scan.table, count, 0, 0)?;
                let max_bytes = file_budget
                    .checked_sub(base)
                    .and_then(|bytes| usize::try_from(bytes / 8).ok())
                    .filter(|bytes| *bytes > 0)
                    .ok_or_else(legacy_unsupported)?
                    .min(snapshot::MAX_ENCODED_SNAPSHOT_BYTES);
                (max_bytes, file_budget)
            }
        };
        let key = scan.file_key(cursor.file)?;
        // Do not wait with a partially filled output accumulator for another
        // stream to release a cached file. The worker can retry this batch
        // from its durable-in-RAM cursor when capacity next becomes available.
        let limits = pool.resources();
        let headroom = (limits.working_bytes() - limits.task_memory_limit_bytes)
            .min(limits.task_memory_limit_bytes);
        let mut charge = pool
            .scheduler()
            .charge_memory(0, charge_bytes.checked_add(headroom).ok_or_else(resource)?)
            .map_err(|_| error(ErrorCode::Overloaded, "stream file memory is busy"))?;
        charge.shrink_to(charge_bytes)?;
        let object = storage.read(&key, max_bytes).await.map_err(|failure| {
            if matches!(scan.data, TableData::Legacy { .. })
                && matches!(failure, StorageError::LimitExceeded)
            {
                legacy_unsupported()
            } else {
                storage_failure(failure)
            }
        })?;
        if matches!(scan.data, TableData::Partitioned { .. }) && object.bytes.len() != max_bytes {
            return Err(corrupt());
        }

        Ok(Self {
            catalog_commit: scan.catalog_commit,
            table_id: scan.table.id,
            file: cursor.file,
            next_offset: cursor.raw_offset,
            raw_end: cursor.raw_offset + read_rows,
            reader: Reader::Unchecked(Bytes::from(object.bytes)),
            charge,
        })
    }

    pub(super) fn at(&self, scan: &PinnedScan, cursor: Cursor) -> bool {
        self.catalog_commit == scan.catalog_commit
            && self.table_id == scan.table.id
            && self.file == cursor.file
            && self.next_offset == cursor.raw_offset
            && scan.file_rows(cursor.file).is_ok_and(|rows| {
                self.raw_end == rows.min(cursor.raw_offset.saturating_add(cursor.raw_remaining))
            })
    }

    pub(super) fn read(
        mut self,
        scan: &PinnedScan,
        mut cursor: Cursor,
        mut encoder: BatchEncoder,
    ) -> Result<(Option<Self>, Cursor, BatchEncoder), ProtocolError> {
        if let Reader::Unchecked(bytes) = &self.reader {
            self.reader = with_task_limit(self.charge.bytes(), || {
                self.open_reader(scan, bytes.clone())
            })?;
            if let Reader::Legacy(rows) = &self.reader {
                let payload = rows.iter().map(snapshot::row_payload).sum::<usize>();
                let retained = snapshot::memory_shape(&scan.table, rows.len(), payload, 0)?;
                self.charge.shrink_to(retained)?;
            }
        }
        let stop = (cursor.raw_offset + RAW_ROWS_PER_JOB as u64).min(self.raw_end);
        while cursor.raw_offset < stop && !cursor.exhausted() && !encoder.full() {
            let projected;
            let row = match &mut self.reader {
                Reader::Unchecked(_) => unreachable!("validated stream file"),
                Reader::Projected(reader) => {
                    projected = reader.next_row(&scan.table)?;
                    &projected
                }
                Reader::Legacy(rows) => rows.get(cursor.raw_offset as usize).ok_or_else(corrupt)?,
            };
            let matched = matches_filter(&scan.query, row);
            if matched {
                encoder.push(scan, row)?;
            }
            cursor.visit(matched);
        }

        self.next_offset = cursor.raw_offset;
        if cursor.raw_offset == self.raw_end || cursor.exhausted() {
            if cursor.raw_offset == scan.file_rows(cursor.file)? {
                cursor.file += 1;
                cursor.raw_offset = 0;
            }
            return Ok((None, cursor, encoder));
        }
        Ok((Some(self), cursor, encoder))
    }

    fn open_reader(&self, scan: &PinnedScan, bytes: Bytes) -> Result<Reader, ProtocolError> {
        match &scan.data {
            TableData::Partitioned { files } => {
                let reader = ProjectedReader::open(
                    &scan.table,
                    bytes,
                    files.get(self.file).ok_or_else(corrupt)?,
                    scan.read_columns(),
                    self.next_offset,
                    self.raw_end - self.next_offset,
                )?;
                Ok(Reader::Projected(Box::new(reader)))
            }
            TableData::Legacy { .. } => {
                let mut rows =
                    snapshot::decode(&scan.table, bytes, Some(scan.file_rows(self.file)?))
                        .map_err(|failure| {
                            if failure.code == ErrorCode::LimitExceeded {
                                legacy_unsupported()
                            } else {
                                failure
                            }
                        })?;
                rows.sort_unstable_by(|a, b| compare_rows(&scan.table, &scan.query, a, b));
                Ok(Reader::Legacy(rows))
            }
        }
    }
}

fn legacy_unsupported() -> ProtocolError {
    error(
        ErrorCode::UnsupportedOperation,
        "legacy stream snapshot cannot be sorted within the task memory limit",
    )
}
