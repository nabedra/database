//! Bounded batch scans of immutable OLAP table manifests.
mod encoding;
mod file;
#[cfg(test)]
mod tests;

use crate::{
    protocol::{
        ErrorCode, LogicalPredicate, MAX_BATCH_ROWS, MAX_BODY_BYTES, Outcome, Predicate,
        ProtocolError, Query, TableDescriptor,
    },
    scheduling::MemoryCharge,
    storage::{Catalog, CatalogEntry, ClusterCatalog, ObjectStore, Storage, TableData},
    worker_pool::WorkerPool,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

use encoding::BatchEncoder;
use file::OpenFile;

/// The worker can transfer this descriptor through its replicated RAM queue.
/// The manifest and query come from a validated catalog and stream request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinnedScan {
    catalog_revision: u64,
    catalog_commit: Uuid,
    table: TableDescriptor,
    query: Query,
    data: TableData,
    /// One prefix per file, followed by the full raw row count.
    file_starts: Vec<u64>,
}
impl PinnedScan {
    pub(crate) fn capture(
        catalog: &Catalog,
        table_id: Uuid,
        query: Query,
    ) -> Result<Self, ProtocolError> {
        Self::from_entries(
            catalog.revision,
            catalog.commit_id,
            &catalog.tables,
            table_id,
            query,
        )
    }

    pub(crate) fn capture_cluster(
        catalog: &ClusterCatalog,
        table_id: Uuid,
        query: Query,
    ) -> Result<Self, ProtocolError> {
        Self::from_entries(
            catalog.revision,
            catalog.commit_id,
            &catalog.tables,
            table_id,
            query,
        )
    }

    fn from_entries(
        catalog_revision: u64,
        catalog_commit: Uuid,
        tables: &BTreeMap<String, CatalogEntry>,
        table_id: Uuid,
        query: Query,
    ) -> Result<Self, ProtocolError> {
        let (name, entry) = tables
            .iter()
            .find(|(_, entry)| entry.id == table_id)
            .ok_or_else(|| error(ErrorCode::NotFound, "table not found"))?;
        let table = entry.descriptor(name);
        let query = normalize_query(&table, query)?;
        let mut file_starts = vec![0_u64];
        match &entry.data {
            TableData::Legacy { .. } => file_starts.push(entry.row_count),
            TableData::Partitioned { files } => {
                file_starts
                    .try_reserve_exact(files.len())
                    .map_err(|_| resource())?;
                for file in files {
                    let next = file_starts
                        .last()
                        .unwrap()
                        .checked_add(file.row_count)
                        .ok_or_else(corrupt)?;
                    file_starts.push(next);
                }
            }
        }
        if file_starts.last() != Some(&entry.row_count) {
            return Err(corrupt());
        }
        Ok(Self {
            catalog_revision,
            catalog_commit,
            table,
            query,
            data: entry.data.clone(),
            file_starts,
        })
    }

    pub(crate) fn check_query(&self, table_id: Uuid, query: &Query) -> Result<(), ProtocolError> {
        if table_id != self.table.id || normalize_query(&self.table, query.clone())? != self.query {
            return Err(error(
                ErrorCode::Conflict,
                "stream snapshot query does not match",
            ));
        }
        Ok(())
    }

    pub(crate) fn cursor(&self, shard: u32, shards: u32) -> Result<Cursor, ProtocolError> {
        if shard >= shards {
            return Err(error(ErrorCode::ValidationError, "invalid stream shard"));
        }
        if shards > 1 && self.query.limit.is_some() {
            return Err(error(
                ErrorCode::UnsupportedOperation,
                "scan limits require a single stream shard",
            ));
        }
        let total = *self.file_starts.last().ok_or_else(corrupt)?;
        let boundary = |rank| {
            u64::try_from(u128::from(total) * u128::from(rank) / u128::from(shards))
                .map_err(|_| corrupt())
        };
        let start = boundary(shard)?;
        let end = boundary(shard + 1)?;
        let file = self
            .file_starts
            .partition_point(|offset| *offset <= start)
            .checked_sub(1)
            .ok_or_else(corrupt)?;
        Ok(Cursor {
            file,
            raw_offset: start - self.file_starts[file],
            raw_remaining: end - start,
            matches_remaining: self.query.limit,
        })
    }

    fn file_rows(&self, file: usize) -> Result<u64, ProtocolError> {
        let start = self.file_starts.get(file).ok_or_else(corrupt)?;
        let end = self
            .file_starts
            .get(file.checked_add(1).ok_or_else(corrupt)?)
            .ok_or_else(corrupt)?;
        end.checked_sub(*start).ok_or_else(corrupt)
    }

    fn output_columns(&self) -> &[String] {
        self.query.select.as_ref().expect("normalized projection")
    }

    fn read_columns(&self) -> Vec<usize> {
        let mut names: BTreeSet<&str> = self.output_columns().iter().map(String::as_str).collect();
        names.insert(&self.table.key);
        if let Some(predicate) = &self.query.filter {
            filter_columns(predicate, &mut names);
        }
        self.table
            .schema
            .keys()
            .enumerate()
            .filter_map(|(index, name)| names.contains(name.as_str()).then_some(index))
            .collect()
    }
}

fn normalize_query(table: &TableDescriptor, mut query: Query) -> Result<Query, ProtocolError> {
    query.validate(&table.schema)?;
    if query
        .order_by
        .take()
        .is_some_and(|order| order.desc || order.column != table.key)
    {
        return Err(error(
            ErrorCode::UnsupportedOperation,
            "streams require ascending primary-key order",
        ));
    }
    if query.select.is_none() {
        query.select = Some(table.schema.keys().cloned().collect());
    }
    Ok(query)
}

fn filter_columns<'a>(predicate: &'a Predicate, names: &mut BTreeSet<&'a str>) {
    match predicate {
        Predicate::Comparison(comparison) => {
            names.insert(&comparison.column);
        }
        Predicate::Logical(LogicalPredicate::And { args } | LogicalPredicate::Or { args }) => {
            for predicate in args {
                filter_columns(predicate, names);
            }
        }
        Predicate::Logical(LogicalPredicate::Not { arg }) => filter_columns(arg, names),
    }
}

/// The offset is relative to a file in the pinned manifest. On replay the
/// reader locates its row group by prefix count before decoding selected rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    file: usize,
    raw_offset: u64,
    raw_remaining: u64,
    matches_remaining: Option<u64>,
}
impl Cursor {
    fn exhausted(self) -> bool {
        self.raw_remaining == 0 || self.matches_remaining == Some(0)
    }

    fn visit(&mut self, matched: bool) {
        self.raw_offset += 1;
        self.raw_remaining -= 1;
        if let (true, Some(remaining)) = (matched, &mut self.matches_remaining) {
            *remaining -= 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BatchFormat {
    Rows,
    Arrow,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchOptions {
    pub(crate) batch_rows: usize,
    pub(crate) max_batch_bytes: usize,
    pub(crate) format: BatchFormat,
    pub(crate) drop_last: bool,
}
impl BatchOptions {
    fn validate(self) -> Result<(), ProtocolError> {
        if !(1..=MAX_BATCH_ROWS).contains(&self.batch_rows)
            || !(1..=MAX_BODY_BYTES).contains(&self.max_batch_bytes)
        {
            return Err(error(
                ErrorCode::ValidationError,
                "invalid stream batch budget",
            ));
        }
        Ok(())
    }
}

pub(crate) struct ScanBatch {
    pub(crate) cursor: Cursor,
    pub(crate) data: Option<BatchBytes>,
}

/// Every clone of the returned Bytes holds the original reservation, including
/// clones moved into an HTTP body that outlives the stream cache.
#[derive(Debug, Clone)]
pub(crate) struct BatchBytes(Bytes);
impl BatchBytes {
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.0
    }

    fn new(bytes: Vec<u8>, mut charge: MemoryCharge) -> Result<Self, ProtocolError> {
        charge.shrink_to(
            (bytes.capacity() as u64)
                .checked_add(128)
                .ok_or_else(resource)?,
        )?;
        Ok(Self(Bytes::from_owner(ChargedBytes {
            bytes,
            _charge: charge,
        })))
    }
}
struct ChargedBytes {
    bytes: Vec<u8>,
    _charge: MemoryCharge,
}
impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// The current file and its projected decoder are local, disposable caches.
/// The caller's serializable cursor is the source of truth after takeover.
#[derive(Default)]
pub(crate) struct ScanReader {
    file: Option<OpenFile>,
}
impl ScanReader {
    pub(crate) async fn next_batch<B: ObjectStore>(
        &mut self,
        storage: &Storage<B>,
        pool: &WorkerPool,
        scan: Arc<PinnedScan>,
        mut cursor: Cursor,
        options: BatchOptions,
    ) -> Result<ScanBatch, ProtocolError> {
        options.validate()?;
        if cursor.exhausted() {
            self.file = None;
            return Ok(ScanBatch { cursor, data: None });
        }

        let job_id = pool.request_id();
        let budget = BatchEncoder::budget(&scan, options);
        let file_budget = pool
            .resources()
            .task_memory_limit_bytes
            .checked_sub(budget)
            .filter(|budget| *budget > 0)
            .ok_or_else(resource)?;
        let mut batch_charge = pool.scheduler().reserve_memory(job_id, 0, budget).await?;
        let mut encoder = BatchEncoder::new(&scan, options)?;
        let mut file = self.file.take().filter(|file| file.at(&scan, cursor));
        while !cursor.exhausted() && !encoder.full() {
            let current = match file.take() {
                Some(file) => file,
                None => OpenFile::load(storage, pool, &scan, cursor, file_budget).await?,
            };
            let scan = scan.clone();
            let result = pool
                .submit_buffered(job_id, 1, vec![batch_charge], budget, move |_, _| {
                    current.read(&scan, cursor, encoder)
                })
                .await?
                .receive()
                .await?;
            ((file, cursor, encoder), batch_charge) = result;
        }

        let job = pool
            .submit_buffered(job_id, 1, vec![batch_charge], budget, move |_, _| {
                encoder.finish()
            })
            .await?;
        let (encoded, charge) = job.receive().await?;
        let data = encoded
            .map(|bytes| BatchBytes::new(bytes, charge))
            .transpose()?;
        if !cursor.exhausted() {
            self.file = file;
        }
        Ok(ScanBatch { cursor, data })
    }
}

fn error(code: ErrorCode, message: &'static str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn corrupt() -> ProtocolError {
    error(
        ErrorCode::StorageUnavailable,
        "invalid stored stream partition",
    )
}
fn resource() -> ProtocolError {
    error(
        ErrorCode::LimitExceeded,
        "stream exceeds the worker task memory limit",
    )
}
