//! Typed CONTROL records only. Parsing does not resolve catalogs or publish data.
//! Use the bounded parse/encode entry points at the future framing boundary.
use crate::{
    partition_size::MAX_PARTITION_BYTES,
    protocol::{
        self, BatchPayload, ErrorCode, Key, KeyPayload, MAX_BATCH_ROWS, MAX_BODY_BYTES,
        MAX_DECODED_PAYLOAD_BYTES, MAX_TABLE_ROWS, Operation, Query, QueryPayload, Request, Row,
        RowPayload, TableDescriptor, UpdatePayload,
    },
    snapshot::MAX_ENCODED_SNAPSHOT_BYTES,
    storage::{KeyRange, ObjectKey, PartitionFile},
};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{collections::BTreeSet, fmt};
use uuid::Uuid;

pub const MAX_CONTROL_BYTES: usize = 16 * 1024 * 1024;
/// Escaped JSON row-array bound; the public response cap belongs to the reducer.
pub const MAX_INTERNAL_ROW_BYTES: usize = 6 * MAX_DECODED_PAYLOAD_BYTES + 2;
pub const MAX_PREPARATION_ROWS: usize = MAX_TABLE_ROWS + MAX_BATCH_ROWS;
pub const MAX_ERROR_BYTES: usize = 1024;
pub const MAX_CATALOG_ETAG_BYTES: usize = 1024;

/// A job failure makes no assertion about the database outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobError {
    #[serde(deserialize_with = "error_code")]
    pub code: ErrorCode,
    pub message: String,
}
impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for JobError {}
fn invalid() -> JobError {
    JobError {
        code: ErrorCode::ValidationError,
        message: "invalid job control record".into(),
    }
}
fn check(ok: bool) -> Result<(), JobError> {
    if ok { Ok(()) } else { Err(invalid()) }
}
fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn safe(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control)
}
fn error_code<'de, D: Deserializer<'de>>(d: D) -> Result<ErrorCode, D::Error> {
    let name = String::deserialize(d)?;
    serde_json::from_value(serde_json::Value::String(name)).map_err(serde::de::Error::custom)
}
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}

// Each record uses raw JSON so derived structs cannot accept positional arrays.
// The preflight rejects duplicates in maps too (schemas and rows included).
macro_rules! record {
    ($name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Serialize)]
        pub struct $name { $(pub $field: $ty),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d:D)->Result<Self,D::Error>{
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let raw=Box::<RawValue>::deserialize(d)?;
                strict(raw.get()).map_err(serde::de::Error::custom)?;
                let w:Wire=object(&raw).map_err(serde::de::Error::custom)?;
                Ok(Self { $($field:w.$field),* })
            }
        }
    }
}
macro_rules! tagged {
    ($name:ident { $($variant:ident => $tag:literal { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Serialize)]
        #[serde(tag="kind",content="payload")]
        pub enum $name { $(#[serde(rename=$tag)] $variant { $($field:$ty),* }),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D:Deserializer<'de>>(d:D)->Result<Self,D::Error>{
                #[derive(Deserialize)] #[serde(deny_unknown_fields)]
                struct Tag { kind:String, payload:Box<RawValue> }
                let raw=Box::<RawValue>::deserialize(d)?;
                strict(raw.get()).map_err(serde::de::Error::custom)?;
                let tag:Tag=object(&raw).map_err(serde::de::Error::custom)?;
                match tag.kind.as_str() { $($tag => {
                    #[derive(Deserialize)] #[serde(deny_unknown_fields)]
                    struct Payload { $($(#[$attr])* $field:$ty),* }
                    let _p:Payload=object(&tag.payload).map_err(serde::de::Error::custom)?;
                    Ok(Self::$variant { $($field:_p.$field),* })
                }),*, _=>Err(serde::de::Error::custom("unknown control kind")) }
            }
        }
    }
}
fn object<T: DeserializeOwned>(raw: &RawValue) -> Result<T, serde_json::Error> {
    protocol::deserialize_object(&mut serde_json::Deserializer::from_str(raw.get()))
}
// Raw subtrees retain integer syntax and signed zero. Depth is bounded even
// though each subtree starts a new deserializer.
fn strict(text: &str) -> Result<(), JobError> {
    strict_at(text, 0)
}
fn strict_at(text: &str, depth: usize) -> Result<(), JobError> {
    check(text.len() <= MAX_CONTROL_BYTES && depth <= 96)?;
    match text.trim_start().as_bytes().first() {
        Some(b'{') => {
            struct Unique;
            impl<'de> serde::de::Visitor<'de> for Unique {
                type Value = Vec<Box<RawValue>>;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("unique object fields")
                }
                fn visit_map<M: serde::de::MapAccess<'de>>(
                    self,
                    mut m: M,
                ) -> Result<Self::Value, M::Error> {
                    let mut names = BTreeSet::new();
                    let mut values = Vec::new();
                    while let Some(k) = m.next_key::<String>()? {
                        if !names.insert(k) {
                            return Err(serde::de::Error::custom("duplicate field"));
                        }
                        values.push(m.next_value()?);
                    }
                    Ok(values)
                }
            }
            let mut d = serde_json::Deserializer::from_str(text);
            let values =
                serde::Deserializer::deserialize_map(&mut d, Unique).map_err(|_| invalid())?;
            d.end().map_err(|_| invalid())?;
            for v in values {
                strict_at(v.get(), depth + 1)?;
            }
        }
        Some(b'[') => {
            let values: Vec<Box<RawValue>> = serde_json::from_str(text).map_err(|_| invalid())?;
            for v in values {
                strict_at(v.get(), depth + 1)?;
            }
        }
        _ => {
            serde_json::from_str::<Box<RawValue>>(text).map_err(|_| invalid())?;
        }
    }
    Ok(())
}
fn range<'de, D: Deserializer<'de>>(d: D) -> Result<KeyRange, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire {
        #[serde(deserialize_with = "nullable")]
        lower: Option<Key>,
        #[serde(deserialize_with = "nullable")]
        upper: Option<Key>,
    }
    let w: Wire = protocol::deserialize_object(d)?;
    Ok(KeyRange {
        lower: w.lower,
        upper: w.upper,
    })
}
record!(Snapshot {
    catalog_revision: u64,
    catalog_commit_id: Uuid,
    catalog_etag: String
});
tagged!(FileInput {
    Legacy => "legacy" { object:String, row_count:u64 },
    Partition => "partition" { object:String, #[serde(deserialize_with="range")] range:KeyRange, row_count:u64, byte_size:u64, payload_bytes:u64 }
});
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum RowMutation {
    Insert(RowPayload),
    InsertMany(BatchPayload),
    Update(UpdatePayload),
    Upsert(RowPayload),
    Delete(KeyPayload),
}
impl RowMutation {
    pub(crate) fn operation(&self) -> Operation {
        match self {
            Self::Insert(p) => Operation::Insert(p.clone()),
            Self::InsertMany(p) => Operation::InsertMany(p.clone()),
            Self::Update(p) => Operation::Update(p.clone()),
            Self::Upsert(p) => Operation::Upsert(p.clone()),
            Self::Delete(p) => Operation::Delete(p.clone()),
        }
    }
}
impl<'de> Deserialize<'de> for RowMutation {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Tag {
            kind: String,
            payload: Box<RawValue>,
        }
        let raw = Box::<RawValue>::deserialize(d)?;
        strict(raw.get()).map_err(serde::de::Error::custom)?;
        let t: Tag = object(&raw).map_err(serde::de::Error::custom)?;
        macro_rules! decode {
            ($variant:ident) => {
                object(&t.payload)
                    .map(Self::$variant)
                    .map_err(serde::de::Error::custom)
            };
        }
        match t.kind.as_str() {
            "insert" => decode!(Insert),
            "insert_many" => decode!(InsertMany),
            "update" => decode!(Update),
            "upsert" => decode!(Upsert),
            "delete" => decode!(Delete),
            _ => Err(serde::de::Error::custom("unsupported mutation")),
        }
    }
}
tagged!(Task {
    Get => "get" { file:FileInput, key:Key },
    Collect => "collect" { file:FileInput, #[serde(deserialize_with="protocol::deserialize_object")] query:Query },
    Count => "count" { file:FileInput, #[serde(deserialize_with="protocol::deserialize_object")] query:Query },
    Prepare => "prepare" { #[serde(deserialize_with="nullable")] file:Option<FileInput>, #[serde(deserialize_with="range")] range:KeyRange, mutation:RowMutation, target_bytes:u64 }
});
record!(JobEnvelope {
    version: u32,
    cluster_id: Uuid,
    operation_id: Uuid,
    job_id: Uuid,
    attempt_id: Uuid,
    request_digest: String,
    snapshot: Snapshot,
    ordinal: u32,
    #[serde(deserialize_with = "protocol::deserialize_object")]
    table: TableDescriptor,
    task: Task
});
tagged!(Effect { Null => "null" {}, Count => "count" {value:u64}, Found => "found" {value:bool} });
record!(PartMeta {
    #[serde(deserialize_with = "range")]
    range: KeyRange,
    row_count: u64,
    payload_bytes: u64,
    byte_size: u64,
    content_digest: String
});
tagged!(JobResult {
    Get => "get" {source_rows:u64, source_payload_bytes:u64, found:bool},
    Collect => "collect" {source_rows:u64, source_payload_bytes:u64, matched_rows:u64},
    Count => "count" {source_rows:u64, source_payload_bytes:u64, matched_rows:u64},
    Prepared => "prepared" {effect:Effect, #[serde(deserialize_with="nullable")] replacement_count:Option<u32>},
    Error => "error" {#[serde(deserialize_with="error_code")] code:ErrorCode, message:String}
});
record!(ResultEnvelope {
    version: u32,
    cluster_id: Uuid,
    operation_id: Uuid,
    job_id: Uuid,
    attempt_id: Uuid,
    request_digest: String,
    snapshot: Snapshot,
    ordinal: u32,
    result: JobResult
});
impl FileInput {
    pub fn row_count(&self) -> u64 {
        match self {
            Self::Legacy { row_count, .. } | Self::Partition { row_count, .. } => *row_count,
        }
    }
    pub fn read_limit(&self) -> usize {
        match self {
            Self::Legacy { .. } => MAX_ENCODED_SNAPSHOT_BYTES,
            Self::Partition { .. } => MAX_PARTITION_BYTES,
        }
    }
    pub fn validate(&self, table: &TableDescriptor) -> Result<(), JobError> {
        table.validate().map_err(|_| invalid())?;
        check(self.row_count() <= MAX_TABLE_ROWS as u64)?;
        match self {
            Self::Legacy { object, .. } => {
                ObjectKey::parse_snapshot(object, table.id).map_err(|_| invalid())?;
            }
            Self::Partition {
                object,
                range,
                row_count,
                byte_size,
                payload_bytes,
            } => {
                ObjectKey::parse_partition(object, table.id).map_err(|_| invalid())?;
                range
                    .validate(table.schema[&table.key])
                    .map_err(|_| invalid())?;
                check(
                    *row_count > 0
                        && *byte_size > 0
                        && *byte_size <= MAX_PARTITION_BYTES as u64
                        && *payload_bytes > 0
                        && *payload_bytes <= MAX_DECODED_PAYLOAD_BYTES as u64,
                )?;
            }
        }
        Ok(())
    }
}
impl From<PartitionFile> for FileInput {
    fn from(p: PartitionFile) -> Self {
        Self::Partition {
            object: p.object,
            range: p.range,
            row_count: p.row_count,
            byte_size: p.byte_size,
            payload_bytes: p.payload_bytes,
        }
    }
}
impl JobEnvelope {
    pub fn validate(&self) -> Result<(), JobError> {
        check(
            self.version == 1
                && digest(&self.request_digest)
                && safe(&self.snapshot.catalog_etag, MAX_CATALOG_ETAG_BYTES),
        )?;
        self.table.validate().map_err(|_| invalid())?;
        let ty = self.table.schema[&self.table.key];
        match &self.task {
            Task::Get { file, key } => {
                file.validate(&self.table)?;
                key.validate(ty).map_err(|_| invalid())?;
                validate_body(
                    Operation::Get(KeyPayload {
                        table_id: self.table.id,
                        key: key.clone(),
                    }),
                    self.operation_id,
                )?;
            }
            Task::Collect { file, query } | Task::Count { file, query } => {
                file.validate(&self.table)?;
                query.validate(&self.table.schema).map_err(|_| invalid())?;
                let p = QueryPayload {
                    table_id: self.table.id,
                    query: query.clone(),
                };
                let op = if matches!(self.task, Task::Collect { .. }) {
                    Operation::Collect(p)
                } else {
                    Operation::Count(p)
                };
                validate_body(op, self.operation_id)?;
            }
            Task::Prepare {
                file,
                range,
                mutation,
                target_bytes,
            } => {
                check(*target_bytes > 0 && *target_bytes <= MAX_PARTITION_BYTES as u64)?;
                range.validate(ty).map_err(|_| invalid())?;
                if let Some(file) = file {
                    file.validate(&self.table)?;
                    match file {
                        FileInput::Partition { range: r, .. } => check(r == range)?,
                        FileInput::Legacy { .. } => {
                            check(range.lower.is_none() && range.upper.is_none())?
                        }
                    }
                }
                let op = mutation.operation();
                op.validate_for_table(&self.table).map_err(|_| invalid())?;
                // Preserve the public mutation body limit, independently of the
                // larger control envelope and the descriptor carried alongside it.
                validate_body(op, self.operation_id)?;
            }
        }
        Ok(())
    }
    fn file(&self) -> Option<&FileInput> {
        match &self.task {
            Task::Get { file, .. } | Task::Collect { file, .. } | Task::Count { file, .. } => {
                Some(file)
            }
            Task::Prepare { file, .. } => file.as_ref(),
        }
    }
}
impl ResultEnvelope {
    /// Validates CONTROL only. Get rows and Prepared metadata/Parquet remain unverified.
    pub fn validate_for(&self, j: &JobEnvelope) -> Result<(), JobError> {
        j.validate()?;
        check(
            self.version == j.version
                && self.cluster_id == j.cluster_id
                && self.operation_id == j.operation_id
                && self.job_id == j.job_id
                && self.attempt_id == j.attempt_id
                && self.request_digest == j.request_digest
                && self.snapshot == j.snapshot
                && self.ordinal == j.ordinal,
        )?;
        let source = |rows: u64, bytes: u64| -> Result<(), JobError> {
            check(if rows == 0 {
                bytes == 0
            } else {
                bytes >= rows.checked_mul(8).ok_or_else(invalid)?
            })?;
            check(
                rows <= MAX_TABLE_ROWS as u64
                    && bytes <= MAX_DECODED_PAYLOAD_BYTES as u64
                    && Some(rows) == j.file().map(FileInput::row_count),
            )?;
            if let Some(FileInput::Partition { payload_bytes, .. }) = j.file() {
                check(bytes == *payload_bytes)?;
            }
            Ok(())
        };
        match (&j.task, &self.result) {
            (_, JobResult::Error { code, message }) => {
                check(
                    safe(message, MAX_ERROR_BYTES) && !matches!(code, ErrorCode::OutcomeUnknown),
                )?;
                check(
                    serde_json::to_vec(&JobError {
                        code: *code,
                        message: message.clone(),
                    })
                    .map_err(|_| invalid())?
                    .len()
                        <= MAX_ERROR_BYTES,
                )?;
            }
            (
                Task::Get { .. },
                JobResult::Get {
                    source_rows,
                    source_payload_bytes,
                    found,
                },
            ) => {
                source(*source_rows, *source_payload_bytes)?;
                check(!found || *source_rows > 0)?;
            }
            (
                Task::Collect { .. },
                JobResult::Collect {
                    source_rows,
                    source_payload_bytes,
                    matched_rows,
                },
            )
            | (
                Task::Count { .. },
                JobResult::Count {
                    source_rows,
                    source_payload_bytes,
                    matched_rows,
                },
            ) => {
                source(*source_rows, *source_payload_bytes)?;
                check(*matched_rows <= *source_rows)?;
            }
            (
                Task::Prepare { mutation, .. },
                JobResult::Prepared {
                    effect,
                    replacement_count,
                },
            ) => {
                match (mutation, effect) {
                    (RowMutation::InsertMany(p), Effect::Count { value }) => {
                        check(*value == p.rows.len() as u64)?
                    }
                    (RowMutation::Update(_) | RowMutation::Delete(_), Effect::Found { .. }) => {}
                    (RowMutation::Insert(_) | RowMutation::Upsert(_), Effect::Null {}) => {}
                    _ => return Err(invalid()),
                }
                if let Some(count) = replacement_count {
                    check(*count <= MAX_PREPARATION_ROWS as u32)?;
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
}
fn validate_body(operation: Operation, request_id: Uuid) -> Result<(), JobError> {
    check(
        encode(&Request {
            request_id,
            operation,
        })?
        .len()
            <= MAX_BODY_BYTES,
    )
}
fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, JobError> {
    check(bytes.len() <= MAX_CONTROL_BYTES)?;
    serde_json::from_slice(bytes).map_err(|_| invalid())
}
pub fn parse_job(bytes: &[u8]) -> Result<JobEnvelope, JobError> {
    let j: JobEnvelope = parse(bytes)?;
    j.validate()?;
    Ok(j)
}
pub fn parse_result(bytes: &[u8], expected: &JobEnvelope) -> Result<ResultEnvelope, JobError> {
    let r: ResultEnvelope = parse(bytes)?;
    r.validate_for(expected)?;
    Ok(r)
}
pub fn encode_job(job: &JobEnvelope) -> Result<Vec<u8>, JobError> {
    job.validate()?;
    encode(job)
}
pub fn encode_result(result: &ResultEnvelope, expected: &JobEnvelope) -> Result<Vec<u8>, JobError> {
    result.validate_for(expected)?;
    encode(result)
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, JobError> {
    struct Sink(Vec<u8>);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_CONTROL_BYTES - self.0.len() {
                return Err(std::io::Error::other("control limit"));
            }
            self.0
                .try_reserve_exact(bytes.len())
                .map_err(std::io::Error::other)?;
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Sink(Vec::new());
    serde_json::to_writer(&mut sink, value).map_err(|_| invalid())?;
    Ok(sink.0)
}

impl PartMeta {
    pub fn validate(&self, table: &TableDescriptor) -> Result<(), JobError> {
        table.validate().map_err(|_| invalid())?;
        self.range
            .validate(table.schema[&table.key])
            .map_err(|_| invalid())?;
        check(
            self.row_count > 0
                && self.row_count <= MAX_TABLE_ROWS as u64
                && self.payload_bytes > 0
                && self.payload_bytes <= MAX_DECODED_PAYLOAD_BYTES as u64
                && self.byte_size > 0
                && self.byte_size <= MAX_PARTITION_BYTES as u64
                && digest(&self.content_digest),
        )?;
        counted_size(self, MAX_CONTROL_BYTES)?;
        Ok(())
    }
}
/// Each metadata CONTROL is independently bounded. Actual Parquet bytes are not verified here.
pub fn parse_part_meta(bytes: &[u8], table: &TableDescriptor) -> Result<PartMeta, JobError> {
    let part: PartMeta = parse(bytes)?;
    part.validate(table)?;
    Ok(part)
}
pub fn encode_part_meta(part: &PartMeta, table: &TableDescriptor) -> Result<Vec<u8>, JobError> {
    part.validate(table)?;
    encode(part)
}

/// Constant-space metadata checks. Finish is mandatory even for null/zero counts.
/// This does not verify Parquet lengths/digests, spool limits or final global/catalog limits.
#[must_use = "metadata sequences must be finished before accepting a result"]
pub struct PreparationValidator {
    table: TableDescriptor,
    range: KeyRange,
    lower: Option<Key>,
    declared: u32,
    seen: u32,
    rows: u64,
    payload_bytes: u64,
    encoded_bytes: u64,
    failed: bool,
}
impl PreparationValidator {
    pub fn new(result: &ResultEnvelope, job: &JobEnvelope) -> Result<Self, JobError> {
        result.validate_for(job)?;
        let (
            Task::Prepare { range, .. },
            JobResult::Prepared {
                replacement_count, ..
            },
        ) = (&job.task, &result.result)
        else {
            return Err(invalid());
        };
        Ok(Self {
            table: job.table.clone(),
            range: range.clone(),
            lower: range.lower.clone(),
            declared: replacement_count.unwrap_or(0),
            seen: 0,
            rows: 0,
            payload_bytes: 0,
            encoded_bytes: 0,
            failed: false,
        })
    }
    /// A rejected record permanently invalidates this sequence.
    pub fn push(&mut self, part: &PartMeta) -> Result<(), JobError> {
        check(!self.failed)?;
        self.failed = true;
        check(self.seen < self.declared)?;
        part.validate(&self.table)?;
        check(part.range.lower == self.lower)?;
        let seen = self.seen.checked_add(1).ok_or_else(invalid)?;
        check(if seen == self.declared {
            part.range.upper == self.range.upper
        } else {
            part.range.upper.is_some()
        })?;
        // Reject crossing a finite input bound before accepting further records.
        if let (Some(upper), Some(end)) = (&part.range.upper, &self.range.upper) {
            let remaining = KeyRange {
                lower: Some(upper.clone()),
                upper: Some(end.clone()),
            };
            if seen < self.declared {
                remaining
                    .validate(self.table.schema[&self.table.key])
                    .map_err(|_| invalid())?;
            }
        }
        let rows = self.rows.checked_add(part.row_count).ok_or_else(invalid)?;
        let payload = self
            .payload_bytes
            .checked_add(part.payload_bytes)
            .ok_or_else(invalid)?;
        let encoded = self
            .encoded_bytes
            .checked_add(part.byte_size)
            .ok_or_else(invalid)?;
        check(rows <= MAX_PREPARATION_ROWS as u64)?;
        self.seen = seen;
        self.rows = rows;
        self.payload_bytes = payload;
        self.encoded_bytes = encoded;
        self.lower = part.range.upper.clone();
        self.failed = false;
        Ok(())
    }
    pub fn finish(self) -> Result<(), JobError> {
        check(
            !self.failed
                && self.seen == self.declared
                && (self.declared == 0 || self.lower == self.range.upper),
        )
    }
}

/// Checks an already decoded Get array, including its serialized escaped size.
/// The framing layer must still enforce actual stream lengths and a terminal record.
/// Returns JSON-array bytes without allocating a second serialized row buffer.
pub fn validate_get_rows(
    result: &ResultEnvelope,
    job: &JobEnvelope,
    rows: &[Row],
) -> Result<usize, JobError> {
    result.validate_for(job)?;
    let (
        Task::Get { key, .. },
        JobResult::Get {
            source_payload_bytes,
            found,
            ..
        },
    ) = (&job.task, &result.result)
    else {
        return Err(invalid());
    };
    check(rows.len() == usize::from(*found))?;
    let payload = crate::snapshot::validate_rows(&job.table, rows).map_err(|_| invalid())?;
    check(payload as u64 <= *source_payload_bytes)?;
    if let Some(row) = rows.first() {
        check(match (&row[&job.table.key], key) {
            (protocol::Scalar::Int64(a), Key::Int64(b)) => a == b,
            (protocol::Scalar::Utf8(a), Key::Utf8(b)) => a == b,
            _ => false,
        })?;
    }
    counted_size(rows, MAX_INTERNAL_ROW_BYTES)
}

/// DTO decoding for a bounded in-memory Get array, not a transport/frame reader.
/// A future incremental reader must preserve these scalar and duplicate-field rules.
pub fn parse_get_rows(
    bytes: &[u8],
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<Vec<Row>, JobError> {
    check(bytes.len() <= MAX_INTERNAL_ROW_BYTES)?;
    struct StrictRow(Row);
    impl<'de> Deserialize<'de> for StrictRow {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Fields;
            impl<'de> serde::de::Visitor<'de> for Fields {
                type Value = StrictRow;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("row object")
                }
                fn visit_map<M: serde::de::MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> Result<StrictRow, M::Error> {
                    let mut row = Row::new();
                    while let Some((key, value)) = map.next_entry()? {
                        if row.insert(key, value).is_some() {
                            return Err(serde::de::Error::custom("duplicate row field"));
                        }
                    }
                    Ok(StrictRow(row))
                }
            }
            d.deserialize_map(Fields)
        }
    }
    struct Rows;
    impl<'de> serde::de::Visitor<'de> for Rows {
        type Value = Vec<Row>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("zero or one row")
        }
        fn visit_seq<S: serde::de::SeqAccess<'de>>(self, mut seq: S) -> Result<Vec<Row>, S::Error> {
            let row = seq.next_element::<StrictRow>()?;
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("too many Get rows"));
            }
            Ok(row.into_iter().map(|r| r.0).collect())
        }
    }
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let rows = serde::Deserializer::deserialize_seq(&mut d, Rows).map_err(|_| invalid())?;
    d.end().map_err(|_| invalid())?;
    validate_get_rows(result, job, &rows)?;
    Ok(rows)
}

fn counted_size<T: Serialize + ?Sized>(value: &T, limit: usize) -> Result<usize, JobError> {
    struct Counter {
        size: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit - self.size {
                return Err(std::io::Error::other("JSON limit"));
            }
            self.size += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { size: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| invalid())?;
    Ok(counter.size)
}
