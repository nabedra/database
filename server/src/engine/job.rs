//! Execute exactly one coordinator-captured input on an admitted partition worker.
use super::{
    mutation,
    partition::{self, ReadFile},
    query,
};
use crate::worker_pool::WorkerRuntime;
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

pub(crate) fn run_captured<B: ObjectStore>(
    runtime: &WorkerRuntime,
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
    run_loaded(job, loaded, output)
}

pub(crate) struct CapturedInput {
    file: Option<ReadFile>,
    bytes: Vec<u8>,
}
fn captured_file(job: &JobEnvelope) -> Option<ReadFile> {
    let file = match &job.task {
        Task::Get { file, .. } | Task::Collect { file, .. } | Task::Count { file, .. } => {
            Some(file)
        }
        Task::Prepare { file, .. } => file.as_ref(),
    };
    file.map(|file| match file {
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
    })
}
/// Memory one captured job needs: its decoded input partition plus the rows a
/// mutation adds. The coordinator charges this estimate to the worker in its
/// remote ledger before it sends the job, and the worker reserves the same.
pub(crate) fn job_memory_bytes(job: &JobEnvelope) -> Result<u64, ProtocolError> {
    let limit = || {
        ProtocolError::new(
            ErrorCode::LimitExceeded,
            "job memory estimate overflow",
            Outcome::NotApplied,
        )
    };
    let file = match &job.task {
        Task::Get { file, .. } | Task::Collect { file, .. } | Task::Count { file, .. } => {
            Some(file)
        }
        Task::Prepare { file, .. } => file.as_ref(),
    };
    let (mut count, mut payload, encoded) = match file {
        Some(FileInput::Partition {
            row_count,
            payload_bytes,
            byte_size,
            ..
        }) => (*row_count, *payload_bytes, *byte_size),
        // A legacy manifest contains no encoded or decoded byte counts. Use
        // only its known lower bound; the worker reserves its whole task budget.
        Some(FileInput::Legacy { row_count, .. }) => (*row_count, 0, 0),
        None => (0, 0, 0),
    };
    if let Task::Prepare { mutation, .. } = &job.task {
        let rows = match mutation {
            RowMutation::Insert(p) | RowMutation::Upsert(p) => std::slice::from_ref(&p.row),
            RowMutation::InsertMany(p) => p.rows.as_slice(),
            RowMutation::Update(p) => std::slice::from_ref(&p.values),
            _ => &[],
        };
        count = count.checked_add(rows.len() as u64).ok_or_else(limit)?;
        for row in rows {
            payload = payload
                .checked_add(snapshot::row_payload(row) as u64)
                .ok_or_else(limit)?;
        }
    }
    snapshot::memory_shape(
        &job.table,
        usize::try_from(count).map_err(|_| limit())?,
        usize::try_from(payload).map_err(|_| limit())?,
        usize::try_from(encoded).map_err(|_| limit())?,
    )
    .map(|n| n.max(4096))
}

/// What a captured job reserves on its worker. A read (get, collect, count)
/// reserves its estimate, at most the task budget: its output never exceeds
/// its decoded input. Reserving the whole budget for every partition read let
/// two small reads refuse each other on a small worker, although the
/// coordinator's ledger, which charges the estimate, had placed both there.
/// A mutation also encodes replacement partitions up to their target size,
/// and a legacy file has no byte counts: both reserve the whole budget.
fn captured_reservation(job: &JobEnvelope, task_limit: u64) -> u64 {
    if matches!(job.task, Task::Prepare { .. })
        || matches!(captured_file(job), Some(ReadFile::Legacy { .. }))
    {
        return task_limit;
    }
    job_memory_bytes(job).map_or(task_limit, |bytes| bytes.min(task_limit))
}

pub(crate) async fn prepare_captured<B: ObjectStore>(
    storage: &Storage<B>,
    pool: &crate::worker_pool::WorkerPool,
    job: &JobEnvelope,
) -> Result<(CapturedInput, crate::scheduling::MemoryCharge), JobError> {
    encode_job(job)?;
    let request = job.scheduler_request_id();
    let reserve = captured_reservation(job, pool.resources().task_memory_limit_bytes);
    if let Some(file) = captured_file(job) {
        let prepared =
            partition::download_file_reserving(storage, pool, request, &job.table, file, reserve)
                .await
                .map_err(|e| failure(e.code))?;
        let (file, bytes, charge) = prepared.into_parts();
        Ok((
            CapturedInput {
                file: Some(file),
                bytes,
            },
            charge,
        ))
    } else {
        let charge = pool
            .scheduler()
            .reserve_memory(request, 0, reserve)
            .await
            .map_err(|e| failure(e.code))?;
        Ok((
            CapturedInput {
                file: None,
                bytes: Vec::new(),
            },
            charge,
        ))
    }
}
pub(crate) fn run_prepared(
    job: &JobEnvelope,
    input: CapturedInput,
    output: &mut LocalOutput<'_>,
) -> Result<ResultEnvelope, JobError> {
    encode_job(job)?;
    output.check(job)?;
    if captured_file(job).is_some() != input.file.is_some() {
        return Err(failure(ErrorCode::ValidationError));
    }
    if let Task::Prepare {
        range, mutation, ..
    } = &job.task
    {
        let contains =
            |key: &Key| range.contains(key, job.table.schema[&job.table.key]).ok() == Some(true);
        let valid = match mutation {
            RowMutation::Insert(p) | RowMutation::Upsert(p) => {
                contains(&partition::row_key(&job.table, &p.row))
            }
            RowMutation::InsertMany(p) => p
                .rows
                .iter()
                .all(|r| contains(&partition::row_key(&job.table, r))),
            RowMutation::Update(p) => contains(&p.key),
            RowMutation::Delete(p) => contains(&p.key),
        };
        if !valid {
            return Err(failure(ErrorCode::ValidationError));
        }
    }
    let loaded = input
        .file
        .map(|file| {
            partition::decode_downloaded(&job.table, &file, input.bytes)
                .map_err(|e| failure(e.code))
        })
        .transpose()?;
    run_loaded(job, loaded, output)
}
fn run_loaded(
    job: &JobEnvelope,
    loaded: Option<partition::LoadedRows>,
    output: &mut LocalOutput<'_>,
) -> Result<ResultEnvelope, JobError> {
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
