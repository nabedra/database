//! Execute exactly one coordinator-captured input on an admitted partition worker.
use super::{
    mutation,
    partition::{self, ReadFile},
    query,
};
use crate::{
    node::{
        LocalOutput,
        job::{
            Effect, FileInput, JobEnvelope, JobError, JobResult, ResultEnvelope, RowMutation, Task,
            encode_job,
        },
    },
    partition_size::PartitionSize,
    protocol::{ErrorCode, Key, Outcome, ProtocolError},
    snapshot,
    storage::{ObjectStore, PartitionFile, Storage},
};
use tokio::runtime::Runtime;

pub(crate) fn run_captured<B: ObjectStore>(
    runtime: &Runtime,
    storage: &Storage<B>,
    job: &JobEnvelope,
    output: &mut LocalOutput<'_>,
) -> Result<ResultEnvelope, JobError> {
    encode_job(job)?;
    output.check(job)?;
    let file = match &job.task {
        Task::Get { file, .. } | Task::Collect { file, .. } | Task::Count { file, .. } => {
            Some(file)
        }
        Task::Prepare {
            file,
            range,
            mutation,
            ..
        } => {
            let contains = |key: &Key| {
                range
                    .contains(key, job.table.schema[&job.table.key])
                    .map_err(|_| failure(ErrorCode::ValidationError))
                    .and_then(|yes| {
                        if yes {
                            Ok(())
                        } else {
                            Err(failure(ErrorCode::ValidationError))
                        }
                    })
            };
            match mutation {
                RowMutation::Insert(p) | RowMutation::Upsert(p) => {
                    contains(&partition::row_key(&job.table, &p.row))?
                }
                RowMutation::InsertMany(p) => {
                    for row in &p.rows {
                        contains(&partition::row_key(&job.table, row))?;
                    }
                }
                RowMutation::Update(p) => contains(&p.key)?,
                RowMutation::Delete(p) => contains(&p.key)?,
            }
            file.as_ref()
        }
    };
    let loaded = file
        .map(|file| {
            let file = match file {
                FileInput::Legacy { object, row_count } => ReadFile::Legacy {
                    object: object.clone(),
                    row_count: *row_count,
                },
                FileInput::Partition {
                    object,
                    range,
                    row_count,
                    byte_size,
                    payload_bytes,
                } => ReadFile::Partition(PartitionFile {
                    object: object.clone(),
                    range: range.clone(),
                    row_count: *row_count,
                    byte_size: *byte_size,
                    payload_bytes: *payload_bytes,
                }),
            };
            partition::load_file(runtime, storage, &job.table, &file).map_err(|e| failure(e.code))
        })
        .transpose()?;
    let (rows, source_payload_bytes) =
        loaded.map_or((Vec::new(), 0), |v| (v.rows, v.payload_bytes as u64));
    let source_rows = rows.len() as u64;
    output.check(job)?;
    let (result, rows) = match &job.task {
        Task::Get { key, .. } => {
            let rows: Vec<_> = rows
                .into_iter()
                .find(|r| partition::row_key(&job.table, r) == *key)
                .into_iter()
                .collect();
            (
                JobResult::Get {
                    source_rows,
                    source_payload_bytes,
                    found: !rows.is_empty(),
                },
                rows,
            )
        }
        Task::Collect { query: q, .. } => {
            let rows = query::local_collect(&job.table, q, rows);
            (
                JobResult::Collect {
                    source_rows,
                    source_payload_bytes,
                    matched_rows: rows.len() as u64,
                },
                rows,
            )
        }
        Task::Count { query: q, .. } => (
            JobResult::Count {
                source_rows,
                source_payload_bytes,
                matched_rows: query::local_count(q, &rows),
            },
            Vec::new(),
        ),
        Task::Prepare {
            mutation: m,
            range,
            target_bytes,
            ..
        } => {
            let changed =
                mutation::apply(&job.table, &m.operation(), rows).map_err(|e| failure(e.code))?;
            let effect = match changed.result {
                mutation::MutationResult::Null => Effect::Null {},
                mutation::MutationResult::Count(value) => Effect::Count {
                    value: value as u64,
                },
                mutation::MutationResult::Found(value) => Effect::Found { value },
            };
            let mut producer = output.prepared(job, effect, changed.changed)?;
            if changed.changed {
                let size = usize::try_from(*target_bytes)
                    .map_err(|_| failure(ErrorCode::ValidationError))?;
                let target: PartitionSize = format!("{size}B")
                    .parse()
                    .map_err(|_| failure(ErrorCode::ValidationError))?;
                snapshot::encode_partition_range_each(
                    &job.table,
                    changed.rows,
                    range.clone(),
                    target,
                    |part| {
                        output
                            .check(job)
                            .and_then(|()| producer.push(part))
                            .map_err(|e| {
                                ProtocolError::new(
                                    e.code,
                                    "local output rejected",
                                    Outcome::NotApplied,
                                )
                            })
                    },
                )
                .map_err(|e| failure(e.code))?;
            }
            return output.accept_prepared(job, producer.finish()?);
        }
    };
    output.accept(job, result, rows)
}
fn failure(code: ErrorCode) -> JobError {
    JobError {
        code,
        message: "captured job failed".into(),
    }
}
