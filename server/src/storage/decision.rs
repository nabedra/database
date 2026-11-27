//! Pure bounded cluster metadata codecs. Reachability and publication belong to
//! the coordinator; decoding an immutable object does not establish authority.
use super::{Catalog, CatalogEntry, StorageError};
use crate::protocol::{
    ErrorCode, MAX_BODY_BYTES, MAX_CATALOG_BYTES, Outcome, ProtocolError, parse_request,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{
    collections::BTreeMap,
    io::{self, Write},
};
use uuid::Uuid;

/// Both valid JSON bodies may double in size when quoted in the outer record.
pub const MAX_DECISION_BYTES: usize = 4 * MAX_BODY_BYTES + 64 * 1024;

/// Included in the conditional catalog root, so stale owners cannot publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogFence {
    pub run_id: Uuid,
    pub scheduler_term: u64,
}
impl CatalogFence {
    pub fn validate(self) -> Result<(), StorageError> {
        if self.run_id.is_nil() || self.scheduler_term == 0 {
            return Err(StorageError::InvalidCatalog);
        }
        Ok(())
    }
}
/// The only catalog root format. Older roots (formats 1 to 5) are refused.
pub const CATALOG_FORMAT: u32 = 6;

/// The catalog root `catalog.json`: tables, the run and term fence, the head
/// of the decision history and the root page of its immutable UUID index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterCatalog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_cluster: Option<Uuid>,
    pub format_version: u32,
    pub revision: u64,
    pub commit_id: Uuid,
    pub tables: BTreeMap<String, CatalogEntry>,
    pub decision_head: Option<Uuid>,
    /// Always present; `validate` refuses a root without it.
    pub fence: Option<CatalogFence>,
    /// The SHA-256 (lowercase hex) of the root page of the immutable UUID
    /// index over every decision reachable from `decision_head`.
    pub decision_index: Option<String>,
}
impl ClusterCatalog {
    /// An empty root fenced for `fence`. Performs no I/O.
    pub fn empty(fence: CatalogFence, owner_cluster: Option<Uuid>) -> Result<Self, StorageError> {
        let empty = Catalog::empty();
        let root = Self {
            owner_cluster,
            format_version: CATALOG_FORMAT,
            revision: empty.revision,
            commit_id: empty.commit_id,
            tables: empty.tables,
            decision_head: None,
            fence: Some(fence),
            decision_index: None,
        };
        root.validate()?;
        Ok(root)
    }
    /// The validated index root, or None when the history is empty.
    pub(crate) fn index_root(&self) -> Result<Option<[u8; 32]>, StorageError> {
        self.decision_index
            .as_deref()
            .map(|hex| super::decision_index::parse_hex(hex).ok_or(StorageError::InvalidCatalog))
            .transpose()
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.format_version != CATALOG_FORMAT {
            return Err(StorageError::UnsupportedCatalog);
        }
        self.fence.ok_or(StorageError::InvalidCatalog)?.validate()?;
        if self.owner_cluster.is_some_and(|id| id.is_nil())
            || self.index_root()?.is_some() != self.decision_head.is_some()
        {
            return Err(StorageError::InvalidCatalog);
        }
        Catalog {
            format_version: 2,
            revision: self.revision,
            commit_id: self.commit_id,
            tables: self.tables.clone(),
        }
        .validate()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        // Refuse any other format before reading its fields.
        #[derive(Deserialize)]
        struct Version {
            format_version: u32,
        }
        let version: Version = object(bytes, MAX_CATALOG_BYTES)?;
        if version.format_version != CATALOG_FORMAT {
            return Err(StorageError::UnsupportedCatalog);
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(default)]
            owner_cluster: Option<Uuid>,
            #[allow(dead_code)]
            format_version: u32,
            revision: u64,
            commit_id: Uuid,
            tables: Box<RawValue>,
            #[serde(deserialize_with = "required_nullable")]
            decision_head: Option<Uuid>,
            fence: CatalogFence,
            #[serde(deserialize_with = "required_nullable_string")]
            decision_index: Option<String>,
        }
        let wire: Wire = object(bytes, MAX_CATALOG_BYTES)?;
        // Keep raw tables through the legacy decoder, which owns object shape,
        // duplicate/schema/key/path/range and partition aggregate validation.
        #[derive(Serialize)]
        struct LegacyView<'a> {
            format_version: u32,
            revision: u64,
            commit_id: Uuid,
            tables: &'a RawValue,
        }
        let bytes = bounded(
            &LegacyView {
                format_version: 2,
                revision: wire.revision,
                commit_id: wire.commit_id,
                tables: &wire.tables,
            },
            MAX_CATALOG_BYTES,
        )?;
        let catalog = Catalog::decode(&bytes)?;
        let decoded = Self {
            owner_cluster: wire.owner_cluster,
            format_version: CATALOG_FORMAT,
            revision: catalog.revision,
            commit_id: catalog.commit_id,
            tables: catalog.tables,
            decision_head: wire.decision_head,
            fence: Some(wire.fence),
            decision_index: wire.decision_index,
        };
        decoded.validate()?;
        Ok(decoded)
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        bounded(self, MAX_CATALOG_BYTES)
    }
}

/// Stored body strings remain byte-for-byte identities. Fields are available to
/// future root relationship checks; encoding revalidates constructed candidates.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub version: u32,
    pub id: Uuid,
    #[serde(deserialize_with = "required_nullable")]
    pub parent: Option<Uuid>,
    pub base_revision: u64,
    pub revision: u64,
    pub request_id: Uuid,
    pub request_body: String,
    pub response_status: u16,
    pub response_body: String,
}
impl std::fmt::Debug for Decision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decision")
            .field("id", &self.id)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}
impl Decision {
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let decision: Self = object(bytes, MAX_DECISION_BYTES)?;
        decision.validate()?;
        Ok(decision)
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        bounded(self, MAX_DECISION_BYTES)
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        let invalid = StorageError::InvalidCatalog;
        if self.request_body.len() > MAX_BODY_BYTES || self.response_body.len() > MAX_BODY_BYTES {
            return Err(StorageError::LimitExceeded);
        }
        if self.version != 1
            || self.parent == Some(self.id)
            || self.base_revision.checked_add(1) != Some(self.revision)
        {
            return Err(invalid);
        }
        let request = parse_request(self.request_body.as_bytes()).map_err(|_| invalid)?;
        if request.request_id != self.request_id || !request.operation.is_mutation() {
            return Err(invalid);
        }
        if self.response_status == 200 {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Success {
                request_id: Uuid,
                data: Box<RawValue>,
            }
            let response: Success = object(self.response_body.as_bytes(), MAX_BODY_BYTES)?;
            let _ = response.data; // Operation-specific data is the encoder's responsibility.
            if response.request_id != self.request_id {
                return Err(invalid);
            }
        } else {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Error {
                request_id: Uuid,
                error: Box<RawValue>,
            }
            let response: Error = object(self.response_body.as_bytes(), MAX_BODY_BYTES)?;
            // Serde also accepts map-shaped unit enums; the public wire uses strings.
            let shape: serde_json::Value = object(response.error.get().as_bytes(), MAX_BODY_BYTES)?;
            if !shape["code"].is_string() || !shape["outcome"].is_string() {
                return Err(invalid);
            }
            let error: ProtocolError = object(response.error.get().as_bytes(), MAX_BODY_BYTES)?;
            if response.request_id != self.request_id
                || error.outcome != Outcome::NotApplied
                || error.code.status_code() != self.response_status
                || !matches!(
                    error.code,
                    ErrorCode::ValidationError
                        | ErrorCode::NotFound
                        | ErrorCode::Conflict
                        | ErrorCode::LimitExceeded
                )
            {
                return Err(invalid);
            }
        }
        Ok(())
    }
}

// A deserialize_with Option has no implicit missing-field default.
fn required_nullable<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
    Option::<Uuid>::deserialize(d)
}
fn required_nullable_string<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}
fn object<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, StorageError> {
    if bytes.len() > limit {
        return Err(StorageError::LimitExceeded);
    }
    // Serde's derived structs also accept sequences. Require an object first.
    if bytes.iter().copied().find(|b| !b.is_ascii_whitespace()) != Some(b'{') {
        return Err(StorageError::InvalidCatalog);
    }
    serde_json::from_slice(bytes).map_err(|_| StorageError::InvalidCatalog)
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, StorageError> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::other("metadata byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| StorageError::LimitExceeded)?;
    Ok(writer.bytes)
}
