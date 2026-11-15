//! Synchronous prepared-file production and reception. Received sequences still
//! require the frame adapter to verify STREAM_END and real EOF.
use crate::{
    engine::validate_partition_rows,
    node::{
        frame::MAX_DATA_BYTES,
        job::{
            Effect, JobEnvelope, JobError, JobResult, MAX_PREPARATION_ROWS, PartMeta,
            PreparationValidator, ResultEnvelope,
        },
        spool::{SpoolOperation, SpoolPayload, SpoolReader, SpoolWriter},
    },
    protocol::{ErrorCode, TableDescriptor},
    snapshot,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{fmt, io::Read, sync::Arc};

fn invalid() -> JobError {
    JobError {
        code: ErrorCode::ValidationError,
        message: "invalid prepared payload".into(),
    }
}
fn check(ok: bool) -> Result<(), JobError> {
    if ok { Ok(()) } else { Err(invalid()) }
}

struct ActivePart {
    meta: PartMeta,
    writer: SpoolWriter,
    length: u64,
    digest: Sha256,
}

/// Owns one sequence without borrowing the caller's stack. Share the same
/// operation across every file and attempt. Every command and final drop must
/// run on the caller's bounded coordinator CPU/I/O executor, including errors.
/// Any rejected command permanently poisons this receiver until drop.
pub struct PreparedReceiver {
    operation: Arc<SpoolOperation>,
    table: TableDescriptor,
    envelope: ResultEnvelope,
    validator: PreparationValidator,
    active: Option<ActivePart>,
    parts: Vec<ValidatedPart>,
    failed: bool,
}
impl PreparedReceiver {
    pub fn new(
        operation: Arc<SpoolOperation>,
        result: &ResultEnvelope,
        job: &JobEnvelope,
    ) -> Result<Self, JobError> {
        let validator = PreparationValidator::new(result, job).map_err(|_| invalid())?;
        Ok(Self {
            operation,
            table: job.table.clone(),
            envelope: result.clone(),
            validator,
            active: None,
            parts: Vec::new(),
            failed: false,
        })
    }

    /// Validate and push metadata before creating a capped writer.
    pub fn begin_part(&mut self, meta: PartMeta) -> Result<(), JobError> {
        check(!self.failed)?;
        self.failed = true;
        check(self.active.is_none())?;
        self.validator.push(&meta).map_err(|_| invalid())?;
        let writer = self
            .operation
            .writer(meta.byte_size)
            .map_err(|_| invalid())?;
        self.active = Some(ActivePart {
            meta,
            writer,
            length: 0,
            digest: Sha256::new(),
        });
        self.failed = false;
        Ok(())
    }

    /// Append exactly the DATA bytes, excluding all frame headers and metadata.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), JobError> {
        check(!self.failed)?;
        self.failed = true;
        check(!bytes.is_empty() && bytes.len() <= MAX_DATA_BYTES)?;
        let active = self.active.as_mut().ok_or_else(invalid)?;
        let next = active
            .length
            .checked_add(bytes.len() as u64)
            .ok_or_else(invalid)?;
        check(next <= active.meta.byte_size)?;
        active.writer.append(bytes).map_err(|_| invalid())?;
        active.digest.update(bytes);
        active.length = next;
        self.failed = false;
        Ok(())
    }

    /// Close and validate one file, then discard all decoded working memory.
    /// Completed files retain no open descriptor and remain private until finish.
    pub fn end_part(&mut self) -> Result<(), JobError> {
        check(!self.failed)?;
        self.failed = true;
        let active = self.active.take().ok_or_else(invalid)?;
        check(active.length == active.meta.byte_size)?;
        check(digest_hex(active.digest) == active.meta.content_digest)?;
        let payload = active.writer.finish().map_err(|_| invalid())?;
        verify_closed_file(&self.table, &active.meta, &payload)?;
        self.parts.push(ValidatedPart {
            meta: active.meta,
            payload,
        });
        self.failed = false;
        Ok(())
    }

    /// Semantic completion only. The adapter must retain this output privately
    /// until frame termination and real EOF, then perform global finalization.
    pub fn finish(self) -> Result<PreparedResult, JobError> {
        check(!self.failed && self.active.is_none())?;
        self.validator.finish().map_err(|_| invalid())?;
        let JobResult::Prepared {
            replacement_count, ..
        } = &self.envelope.result
        else {
            return Err(invalid());
        };
        let parts = replacement_count.map(|_| self.parts);
        Ok(PreparedResult {
            envelope: self.envelope,
            parts,
        })
    }
}

fn digest_hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn content_digest(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest_hex(digest)
}

// Both local and received metadata are untrusted. Verify closed bytes before
// invoking Parquet, and release all decoding memory before the next file.
fn verify_closed_file(
    table: &TableDescriptor,
    meta: &PartMeta,
    payload: &SpoolPayload,
) -> Result<(), JobError> {
    check(payload.len() == meta.byte_size)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(meta.byte_size).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    payload
        .open()
        .map_err(|_| invalid())?
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    check(bytes.len() as u64 == meta.byte_size)?;
    check(content_digest(&bytes) == meta.content_digest)?;
    let rows =
        snapshot::decode(table, Bytes::from(bytes), Some(meta.row_count)).map_err(|_| invalid())?;
    validate_partition_rows(table, &rows, Some((&meta.range, meta.payload_bytes)))
        .map_err(|_| invalid())?;
    Ok(())
}

/// Worker-local incremental production. Construction, commands, error unwinds
/// and final Drop must execute synchronously on the bounded worker. Share the
/// operation across all files and attempts; deposit success before leaving it.
pub(crate) struct LocalPrepared<'a> {
    operation: Arc<SpoolOperation>,
    job: &'a JobEnvelope,
    envelope: ResultEnvelope,
    changed: bool,
    parts: Vec<ValidatedPart>,
    rows: u64,
    payload_bytes: u64,
    encoded_bytes: u64,
    failed: bool,
}
impl<'a> LocalPrepared<'a> {
    pub(crate) fn new(
        operation: Arc<SpoolOperation>,
        job: &'a JobEnvelope,
        effect: Effect,
        changed: bool,
    ) -> Result<Self, JobError> {
        let envelope = ResultEnvelope {
            version: job.version,
            cluster_id: job.cluster_id,
            operation_id: job.operation_id,
            job_id: job.job_id,
            attempt_id: job.attempt_id,
            request_digest: job.request_digest.clone(),
            snapshot: job.snapshot.clone(),
            ordinal: job.ordinal,
            result: JobResult::Prepared {
                effect,
                replacement_count: None,
            },
        };
        envelope.validate_for(job).map_err(|_| invalid())?;
        Ok(Self {
            operation,
            job,
            envelope,
            changed,
            parts: Vec::new(),
            rows: 0,
            payload_bytes: 0,
            encoded_bytes: 0,
            failed: false,
        })
    }

    pub(crate) fn push(&mut self, part: snapshot::EncodedPartition) -> Result<(), JobError> {
        check(!self.failed)?;
        self.failed = true;
        check(self.changed)?;
        let meta = PartMeta {
            range: part.range,
            row_count: part.row_count,
            payload_bytes: part.payload_bytes,
            byte_size: u64::try_from(part.bytes.len()).map_err(|_| invalid())?,
            content_digest: content_digest(&part.bytes),
        };
        meta.validate(&self.job.table).map_err(|_| invalid())?;
        let count = self.parts.len().checked_add(1).ok_or_else(invalid)?;
        let rows = self.rows.checked_add(meta.row_count).ok_or_else(invalid)?;
        let payload_bytes = self
            .payload_bytes
            .checked_add(meta.payload_bytes)
            .ok_or_else(invalid)?;
        let encoded_bytes = self
            .encoded_bytes
            .checked_add(meta.byte_size)
            .ok_or_else(invalid)?;
        check(count <= MAX_PREPARATION_ROWS && rows <= MAX_PREPARATION_ROWS as u64)?;
        let mut writer = self
            .operation
            .writer(meta.byte_size)
            .map_err(|_| invalid())?;
        for chunk in part.bytes.chunks(MAX_DATA_BYTES) {
            writer.append(chunk).map_err(|_| invalid())?;
        }
        drop(part.bytes);
        let payload = writer.finish().map_err(|_| invalid())?;
        verify_closed_file(&self.job.table, &meta, &payload)?;
        self.parts.push(ValidatedPart { meta, payload });
        self.rows = rows;
        self.payload_bytes = payload_bytes;
        self.encoded_bytes = encoded_bytes;
        self.failed = false;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<PreparedResult, JobError> {
        check(!self.failed)?;
        let JobResult::Prepared {
            replacement_count, ..
        } = &mut self.envelope.result
        else {
            return Err(invalid());
        };
        *replacement_count = if self.changed {
            Some(u32::try_from(self.parts.len()).map_err(|_| invalid())?)
        } else {
            None
        };
        self.envelope
            .validate_for(self.job)
            .map_err(|_| invalid())?;
        let mut validator =
            PreparationValidator::new(&self.envelope, self.job).map_err(|_| invalid())?;
        for part in &self.parts {
            validator.push(&part.meta).map_err(|_| invalid())?;
        }
        validator.finish().map_err(|_| invalid())?;
        Ok(PreparedResult {
            envelope: self.envelope,
            parts: self.changed.then_some(self.parts),
        })
    }
}

/// Verified semantic output. Private construction preserves identity, effect and
/// unchanged None versus changed Some(empty). This is not framed acceptance.
pub struct PreparedResult {
    envelope: ResultEnvelope,
    parts: Option<Vec<ValidatedPart>>,
}
impl PreparedResult {
    pub(crate) fn parts(&self) -> Option<&[ValidatedPart]> {
        self.parts.as_deref()
    }
    pub fn envelope(&self) -> &ResultEnvelope {
        &self.envelope
    }
    pub fn into_parts(self) -> (ResultEnvelope, Option<Vec<ValidatedPart>>) {
        (self.envelope, self.parts)
    }
}

/// Closed immutable spool verified against its metadata. No unchecked cloning,
/// path, File or raw descriptor is exposed. Drop and read on the bounded executor.
pub struct ValidatedPart {
    meta: PartMeta,
    payload: SpoolPayload,
}
impl ValidatedPart {
    pub fn metadata(&self) -> &PartMeta {
        &self.meta
    }

    /// Consume the handle for later bounded reading/upload. The reader retains
    /// quota until its final synchronous drop. Upload authorization is external.
    pub fn into_reader(self) -> Result<(PartMeta, SpoolReader), JobError> {
        let reader = self.payload.open().map_err(|_| invalid())?;
        Ok((self.meta, reader))
    }
}
macro_rules! opaque_debug {
    ($($name:ty),+) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(stringify!($name)) }
    })+ };
}
opaque_debug!(PreparedReceiver, PreparedResult, ValidatedPart);

#[cfg(test)]
mod tests;
