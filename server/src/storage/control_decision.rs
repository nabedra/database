//! Pure A05 administration history record codec. Decoding proves local shape and
//! correlation only; publication, authority and reachable history require callers.
use super::StorageError;
use crate::{
    cluster::state::{Policy, PolicyMode},
    protocol::deserialize_object,
};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::io::{self, Write};
use uuid::Uuid;

/// Public administration envelopes retain their independent 1 MiB byte limit.
pub const MAX_CONTROL_BODY_BYTES: usize = 1024 * 1024;
/// Quoting both valid JSON envelopes can double their bytes, plus metadata room.
pub const MAX_CONTROL_DECISION_BYTES: usize = 4 * MAX_CONTROL_BODY_BYTES + 64 * 1024;

/// Exact request/response strings are durable replay identities. No body content
/// is included in Debug or errors. Use decode/encode to enforce the byte bounds.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlDecision {
    pub version: u32,
    #[serde(deserialize_with = "canonical_uuid")]
    pub id: Uuid,
    #[serde(deserialize_with = "nullable_uuid")]
    pub parent: Option<Uuid>,
    pub admin_seq: u64,
    #[serde(deserialize_with = "canonical_uuid")]
    pub cluster_id: Uuid,
    pub base_root_revision: u64,
    pub published_root_revision: u64,
    #[serde(deserialize_with = "canonical_uuid")]
    pub request_id: Uuid,
    pub request_body: String,
    pub response_status: u16,
    pub response_body: String,
}
impl std::fmt::Debug for ControlDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlDecision")
            .field("id", &self.id)
            .field("admin_seq", &self.admin_seq)
            .finish_non_exhaustive()
    }
}
impl ControlDecision {
    /// Caps metadata input before parsing; original body bytes are parsed directly.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: Self = object(bytes, MAX_CONTROL_DECISION_BYTES)?;
        value.validate()?;
        Ok(value)
    }
    /// Revalidates constructed candidates and bounds serialization while writing.
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        bounded(self)
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        let invalid = StorageError::InvalidCatalog;
        if self.request_body.len() > MAX_CONTROL_BODY_BYTES
            || self.response_body.len() > MAX_CONTROL_BODY_BYTES
        {
            return Err(StorageError::LimitExceeded);
        }
        if self.version != 1
            || self.admin_seq == 0
            || (self.admin_seq == 1) != self.parent.is_none()
            || self.parent == Some(self.id)
            || self.base_root_revision == 0
            || self.base_root_revision.checked_add(1) != Some(self.published_root_revision)
        {
            return Err(invalid);
        }
        let request: Request = object(self.request_body.as_bytes(), MAX_CONTROL_BODY_BYTES)?;
        if request.request_id != self.request_id {
            return Err(invalid);
        }
        let target = match request.operation.as_str() {
            "cluster.scale" => {
                let p: Scale = object(request.payload.get().as_bytes(), MAX_CONTROL_BODY_BYTES)?;
                Target::Fixed(p.nodes)
            }
            "cluster.autoscale" => {
                let p: Autoscale =
                    object(request.payload.get().as_bytes(), MAX_CONTROL_BODY_BYTES)?;
                Target::Autoscale(p.min_nodes, p.max_nodes)
            }
            _ => return Err(invalid),
        };
        if self.response_status == 200 {
            let response: Success = object(self.response_body.as_bytes(), MAX_CONTROL_BODY_BYTES)?;
            // Parse the original five-field policy with its strict object/enum
            // deserializer, without Policy::decode's small raw-input cap.
            response.data.validate().map_err(|_| invalid)?;
            if response.request_id != self.request_id || !target.matches(&response.data) {
                return Err(invalid);
            }
        } else {
            let response: Failure = object(self.response_body.as_bytes(), MAX_CONTROL_BODY_BYTES)?;
            let expected_status = match response.error.code.as_str() {
                "validation_error" | "unsupported_capability" => 400,
                "conflict" => 409,
                "limit_exceeded" => 413,
                _ => return Err(invalid),
            };
            if response.request_id != self.request_id
                || response.error.outcome != "not_applied"
                || self.response_status != expected_status
                || (!target.in_bounds() && response.error.code != "validation_error")
            {
                return Err(invalid);
            }
        }
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(deserialize_with = "canonical_uuid")]
    request_id: Uuid,
    operation: String,
    payload: Box<RawValue>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scale {
    nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Autoscale {
    min_nodes: u64,
    max_nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Success {
    #[serde(deserialize_with = "canonical_uuid")]
    request_id: Uuid,
    data: Policy,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Failure {
    #[serde(deserialize_with = "canonical_uuid")]
    request_id: Uuid,
    #[serde(deserialize_with = "deserialize_object")]
    error: TerminalError,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalError {
    code: String,
    // Deliberately opaque. Producing a sanitized deterministic message belongs
    // to the caller; the codec never emits this string in diagnostics.
    #[serde(rename = "message")]
    _message: String,
    outcome: String,
}
enum Target {
    Fixed(u64),
    Autoscale(u64, u64),
}
impl Target {
    fn in_bounds(&self) -> bool {
        match *self {
            Self::Fixed(n) => n <= 1024,
            Self::Autoscale(low, high) => low >= 1 && low <= high && high <= 1024,
        }
    }
    fn matches(&self, policy: &Policy) -> bool {
        self.in_bounds()
            && match *self {
                Self::Fixed(n) => policy.mode == PolicyMode::Fixed && policy.desired == n,
                Self::Autoscale(low, high) => {
                    policy.mode == PolicyMode::Autoscale
                        && policy.min_nodes == low
                        && policy.max_nodes == high
                }
            }
    }
}
fn canonical_uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
    let text = String::deserialize(d)?;
    let id = Uuid::parse_str(&text).map_err(|_| serde::de::Error::custom("invalid identity"))?;
    if id.hyphenated().to_string() != text {
        return Err(serde::de::Error::custom("invalid identity"));
    }
    Ok(id)
}
// deserialize_with prevents Option's implicit missing-field default.
fn nullable_uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
    #[derive(Deserialize)]
    struct Identity(#[serde(deserialize_with = "canonical_uuid")] Uuid);
    Option::<Identity>::deserialize(d).map(|value| value.map(|id| id.0))
}
fn object<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, StorageError> {
    if bytes.len() > limit {
        return Err(StorageError::LimitExceeded);
    }
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let value = deserialize_object(&mut d).map_err(|_| StorageError::InvalidCatalog)?;
    d.end().map_err(|_| StorageError::InvalidCatalog)?;
    Ok(value)
}
fn bounded(value: &impl Serialize) -> Result<Vec<u8>, StorageError> {
    struct Writer {
        bytes: Vec<u8>,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > MAX_CONTROL_DECISION_BYTES - self.bytes.len() {
                return Err(io::Error::other("metadata byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer { bytes: Vec::new() };
    serde_json::to_writer(&mut writer, value).map_err(|_| StorageError::LimitExceeded)?;
    Ok(writer.bytes)
}
