//! Public stream objects and the signed worker response frame.
use crate::protocol::{ErrorCode, Outcome, ProtocolError, Query};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_REQUEST_BYTES: usize = 128 * 1024;
pub const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_BATCH_ROWS: usize = crate::protocol::MAX_BATCH_ROWS;
pub const MAX_PREFETCH: u8 = 2;
pub(super) const MAX_SHARDS: u32 = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Format {
    Rows,
    Arrow,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Open {
    pub request_id: Uuid,
    pub expected_run_id: Uuid,
    pub table_id: Uuid,
    #[serde(deserialize_with = "object_query")]
    pub query: Query,
    pub batch_rows: usize,
    pub prefetch_batches: u8,
    pub format: Format,
    pub drop_last: bool,
    #[serde(deserialize_with = "required_snapshot_id")]
    pub snapshot_id: Option<String>,
    pub shard: u32,
}
fn object_query<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Query, D::Error> {
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if !raw.get().starts_with('{') {
        return Err(serde::de::Error::custom("query must be an object"));
    }
    serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)
}

fn required_snapshot_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::deserialize(deserializer)
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenSnapshot {
    pub request_id: Uuid,
    pub expected_run_id: Uuid,
    pub table_id: Uuid,
    #[serde(deserialize_with = "object_query")]
    pub query: Query,
    pub shards: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Request {
    Capabilities,
    Open(Open),
    OpenSnapshot(OpenSnapshot),
    Batch {
        run_id: Uuid,
        stream_id: Uuid,
        sequence: u64,
    },
    Close {
        run_id: Uuid,
        stream_id: Uuid,
    },
    CloseSnapshot {
        run_id: Uuid,
        snapshot_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Capabilities {
    pub version: u8,
    pub run_id: Uuid,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
    pub max_prefetch_batches: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Opened {
    pub stream_id: Uuid,
    pub run_id: Uuid,
    pub prefetch_batches: u8,
    pub expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotOpened {
    pub snapshot_id: String,
    pub run_id: Uuid,
    pub expires_at: u64,
}

#[derive(Debug)]
pub(crate) struct Batch {
    pub bytes: Bytes,
    pub format: Format,
    pub release: Option<Uuid>,
}

#[derive(Debug)]
pub(crate) enum Reply {
    Capabilities(Capabilities),
    Opened(Opened),
    SnapshotOpened(SnapshotOpened),
    Batch(Batch),
    Eof,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Error {
    Protocol(ProtocolError),
    Stream(StreamError),
}
impl Error {
    pub(crate) fn status_code(&self) -> u16 {
        match self {
            Self::Protocol(error) => error.status_code(),
            Self::Stream(_) => 410,
        }
    }

    pub(super) fn expired() -> Self {
        Self::Stream(StreamError {
            code: StreamCode::StreamExpired,
            message: "stream lease expired or unavailable".into(),
            outcome: Outcome::NotApplied,
        })
    }
    pub(super) fn abandoned() -> Self {
        Self::Stream(StreamError {
            code: StreamCode::StreamAbandoned,
            message: "the stream belongs to an abandoned worker run".into(),
            outcome: Outcome::NotApplied,
        })
    }
}
impl From<ProtocolError> for Error {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(out),
            Self::Stream(error) => out.write_str(&error.message),
        }
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamError {
    pub code: StreamCode,
    pub message: String,
    pub outcome: Outcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamCode {
    StreamExpired,
    StreamAbandoned,
}

pub(super) fn fail(code: ErrorCode, message: &'static str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

pub(super) fn conflict() -> ProtocolError {
    fail(
        ErrorCode::Conflict,
        "stream identity or batch sequence conflicts",
    )
}

pub(super) fn overloaded() -> ProtocolError {
    fail(
        ErrorCode::Overloaded,
        "stream capacity is temporarily unavailable",
    )
}

pub(super) fn unauthorized() -> ProtocolError {
    fail(ErrorCode::Unauthorized, "unauthorized")
}

pub(super) fn unavailable() -> ProtocolError {
    fail(
        ErrorCode::StorageUnavailable,
        "stream authority or storage is unavailable",
    )
}

pub(super) fn invalid() -> ProtocolError {
    fail(ErrorCode::ValidationError, "invalid stream request")
}

pub(super) fn snapshot_id(run: Uuid, id: Uuid) -> String {
    format!("{run}.{id}")
}

pub(super) fn parse_snapshot_id(s: &str) -> Result<(Uuid, Uuid), ProtocolError> {
    if s.len() != 73 {
        return Err(invalid());
    }
    let (run, id) = s.split_once('.').ok_or_else(invalid)?;
    let run = Uuid::parse_str(run).map_err(|_| invalid())?;
    let id = Uuid::parse_str(id).map_err(|_| invalid())?;
    if run.is_nil() || id.is_nil() || snapshot_id(run, id) != s {
        return Err(invalid());
    }
    Ok((run, id))
}
