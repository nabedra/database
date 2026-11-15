//! Partition-local private preparation followed by one atomic publication.
use super::{Engine, mutation, partition, read};
use crate::{
    commit::PendingObject,
    protocol::{
        BatchPayload, ErrorCode, Key, MAX_DECODED_PAYLOAD_BYTES, MAX_TABLE_ROWS, Operation,
        Outcome, ProtocolError,
    },
    snapshot::{self, EncodedPartition},
    storage::{KeyRange, ObjectKey, ObjectStore, PartitionFile, TableData},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
struct Work {
    index: usize,
    file: Option<partition::ReadFile>,
    range: KeyRange,
    operation: Operation,
}
struct Prepared {
    index: usize,
    result: mutation::MutationResult,
    encoded: Option<Vec<EncodedPartition>>,
}

impl<B: ObjectStore + 'static> Engine<B> {
    /// The global mutation session stays held through preparation, draining and
    /// publication. No data is uploaded until every private candidate succeeds.
    pub async fn mutate(
        &self,
        operation: Operation,
    ) -> Result<mutation::MutationResult, ProtocolError> {
        let table_id = match &operation {
            Operation::Insert(p) | Operation::Upsert(p) => p.table_id,
            Operation::InsertMany(p) => p.table_id,
            Operation::Update(p) => p.table_id,
            Operation::Delete(p) => p.table_id,
            _ => {
                return Err(failure(
                    ErrorCode::UnsupportedOperation,
                    "expected a row mutation",
                ));
            }
        };
        let session = self.coordinator().begin().await?;
        let (table, entry) = read::resolve_table(session.catalog(), table_id)?;
        let validation_table = table.clone();
        let operation = self
            .worker_pool()
            .submit(0, move |_, _| {
                operation.validate_for_table(&validation_table)?;
                if let Operation::InsertMany(p) = &operation {
                    let mut keys = BTreeSet::new();
                    for row in &p.rows {
                        let key = match partition::row_key(&validation_table, row) {
                            Key::Int64(n) => n.to_string(),
                            Key::Utf8(s) => s,
                        };
                        if !keys.insert(key) {
                            return Err(failure(ErrorCode::Conflict, "duplicate primary key"));
                        }
                    }
                }
                Ok(operation)
            })
            .await?
            .receive()
            .await?;
        let batch_len = match &operation {
            Operation::InsertMany(p) => Some(p.rows.len()),
            _ => None,
        };
        if batch_len == Some(0) {
            return Ok(mutation::MutationResult::Count(0));
        }
        let full_range = KeyRange {
            lower: None,
            upper: None,
        };
        let mut groups: BTreeMap<usize, Work> = BTreeMap::new();
        match &entry.data {
            TableData::Legacy { .. } => {
                groups.insert(
                    0,
                    Work {
                        index: 0,
                        file: Some(partition::files(&entry).remove(0).1),
                        range: full_range,
                        operation,
                    },
                );
            }
            TableData::Partitioned { files } if files.is_empty() => {
                groups.insert(
                    0,
                    Work {
                        index: 0,
                        file: None,
                        range: full_range,
                        operation,
                    },
                );
            }
            TableData::Partitioned { files } => {
                let owner = |key: &Key| -> Result<usize, ProtocolError> {
                    let file = entry
                        .route(key)
                        .map_err(partition::storage_failure)?
                        .ok_or_else(|| {
                            failure(ErrorCode::StorageUnavailable, "missing partition range")
                        })?;
                    Ok(files
                        .iter()
                        .position(|f| f.object == file.object)
                        .expect("routed file"))
                };
                let make_work = |index, operation| Work {
                    index,
                    file: Some(partition::ReadFile::Partition(files[index].clone())),
                    range: files[index].range.clone(),
                    operation,
                };
                if let Operation::InsertMany(p) = operation {
                    for row in p.rows {
                        let index = owner(&partition::row_key(&table, &row))?;
                        let job = groups.entry(index).or_insert_with(|| {
                            make_work(
                                index,
                                Operation::InsertMany(BatchPayload {
                                    table_id,
                                    rows: Vec::new(),
                                }),
                            )
                        });
                        let Operation::InsertMany(batch) = &mut job.operation else {
                            unreachable!()
                        };
                        batch.rows.push(row);
                    }
                } else {
                    let key = match &operation {
                        Operation::Insert(p) | Operation::Upsert(p) => {
                            partition::row_key(&table, &p.row)
                        }
                        Operation::Update(p) => p.key.clone(),
                        Operation::Delete(p) => p.key.clone(),
                        _ => unreachable!(),
                    };
                    let index = owner(&key)?;
                    groups.insert(index, make_work(index, operation));
                }
            }
        }
        let coordinator = self.coordinator().clone();
        let worker_table = table.clone();
        let target = self.partition_size();
        let prepared = partition::map_indexed(
            self.worker_pool(),
            groups.into_iter().collect(),
            move |job, runtime| {
                let rows = match &job.file {
                    Some(file) => {
                        partition::load_file(runtime, coordinator.storage(), &worker_table, file)?
                            .rows
                    }
                    None => Vec::new(),
                };
                let applied = mutation::apply(&worker_table, &job.operation, rows)?;
                let encoded = if applied.changed {
                    Some(snapshot::encode_partition_range(
                        &worker_table,
                        applied.rows,
                        job.range,
                        target,
                    )?)
                } else {
                    None
                };
                Ok(Prepared {
                    index: job.index,
                    result: applied.result,
                    encoded,
                })
            },
        )
        .await?;
        let changed = prepared.iter().any(|p| p.encoded.is_some());
        let result = if let Some(len) = batch_len {
            mutation::MutationResult::Count(len)
        } else {
            // A keyed operation has exactly one owning job.
            match &prepared[0].result {
                mutation::MutationResult::Null => mutation::MutationResult::Null,
                mutation::MutationResult::Found(found) => mutation::MutationResult::Found(*found),
                mutation::MutationResult::Count(_) => unreachable!(),
            }
        };
        if !changed {
            return Ok(result);
        }
        let mut replacements: BTreeMap<_, _> = prepared
            .into_iter()
            .filter_map(|p| p.encoded.map(|encoded| (p.index, encoded)))
            .collect();
        let old_files = match entry.data {
            TableData::Partitioned { files } => files,
            TableData::Legacy { .. } => Vec::new(),
        };
        let mut files: Vec<PartitionFile> = Vec::new();
        let mut uploads = Vec::new();
        for index in 0..old_files.len().max(1) {
            if let Some(encoded) = replacements.remove(&index) {
                for part in encoded {
                    let ordinal = u32::try_from(uploads.len())
                        .map_err(|_| failure(ErrorCode::LimitExceeded, "too many partitions"))?;
                    let key = ObjectKey::partition(table_id, session.commit_id(), ordinal);
                    files.push(PartitionFile {
                        object: key.as_str().into(),
                        range: part.range,
                        row_count: part.row_count,
                        byte_size: part.bytes.len() as u64,
                        payload_bytes: part.payload_bytes,
                    });
                    uploads.push(PendingObject {
                        key,
                        bytes: part.bytes,
                    });
                }
            } else if let Some(file) = old_files.get(index) {
                files.push(file.clone());
            }
        }
        // Empty files disappear. Extend a neighbor's routing range without
        // altering its physical object or metadata.
        let mut previous_upper = None;
        for file in &mut files {
            file.range.lower = previous_upper;
            previous_upper = file.range.upper.clone();
        }
        if let Some(last) = files.last_mut() {
            last.range.upper = None;
        }
        let (mut rows, mut payload) = (0u64, 0u64);
        for file in &files {
            rows = rows.checked_add(file.row_count).ok_or_else(limit)?;
            payload = payload.checked_add(file.payload_bytes).ok_or_else(limit)?;
        }
        if rows > MAX_TABLE_ROWS as u64 || payload > MAX_DECODED_PAYLOAD_BYTES as u64 {
            return Err(limit());
        }
        let mut candidate = session.candidate();
        let entry = candidate
            .tables
            .get_mut(&table.name)
            .expect("resolved table");
        entry.row_count = rows;
        entry.data = TableData::Partitioned { files };
        session.publish_partitions(candidate, uploads).await?;
        Ok(result)
    }
}
fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn limit() -> ProtocolError {
    failure(ErrorCode::LimitExceeded, "table exceeds decoded limits")
}
