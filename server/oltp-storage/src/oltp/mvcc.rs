//! Common fixed changes for Catalog and Data groups. This module does not
//! evaluate application operations during recovery or resolve a transaction by
//! guessing its status. The coordinator/status protocol supplies those proofs.
#[doc(hidden)]
pub mod catalog;
pub mod index;
#[doc(hidden)]
pub mod planner;
#[doc(hidden)]
pub mod store;
use super::{
    failure,
    pool::{ByteClass, ChargedBytes, ScratchLease, WorkerSharedResources},
};
use crate::protocol::{ErrorCode, Key, ProtocolError};
use nabedra_oltp_types::types::{CommitTs, Text};
/// Fixed commands may duplicate bounded frontend keys/rows. This is an
/// internal message ceiling, independent of the unchanged8MiB application body.
pub use nabedra_resources::decode_budget::{
    MAX_FIXED_COMMAND_BYTES, json_decode_budget, request_decode_budget,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::{
    io,
    sync::{Arc, OnceLock},
};
use uuid::Uuid;

/// Compact JSON with sorted object keys. The number token spelling is retained,
/// so int64 and float64 values keep their existing protocol distinction.
/// An internal codec may deserialize Unattached only under a precharged decode
/// lease. It must attach the whole command and fields before allowing escape.
#[derive(Clone)]
pub struct CanonicalJson(Arc<JsonStorage>);
enum JsonStorage {
    Owned(ChargedBytes),
    Unattached {
        raw: Arc<RawValue>,
        owner: OnceLock<Arc<ScratchLease>>,
    },
}
impl std::fmt::Debug for CanonicalJson {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalJson")
            .field("bytes", &self.as_str().len())
            .field("charged", &self.is_charged())
            .finish()
    }
}
impl PartialEq for CanonicalJson {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for CanonicalJson {}

impl AsRef<str> for CanonicalJson {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl CanonicalJson {
    /// Only the MVCC store calls this on values installed from validated
    /// canonical fixed changes. The page builder precharges aggregate retained
    /// bytes before copying. Parsing RawValue checks syntax without a row tree.
    pub(crate) fn from_stored_json(
        bytes: &[u8],
        owner: Arc<ScratchLease>,
    ) -> Result<Self, ProtocolError> {
        if bytes
            .len()
            .checked_add(128)
            .is_none_or(|n| n > owner.bytes())
        {
            return Err(invalid_json());
        }
        crate::protocol::json_input(bytes, MAX_FIXED_COMMAND_BYTES).map_err(|_| invalid_json())?;
        let raw = serde_json::from_slice::<Box<RawValue>>(bytes).map_err(|_| invalid_json())?;
        let cell = OnceLock::new();
        let _ = cell.set(owner);
        Ok(Self(Arc::new(JsonStorage::Unattached {
            raw: Arc::from(raw),
            owner: cell,
        })))
    }
    pub fn from_serializable<T: Serialize>(
        shared: &WorkerSharedResources,
        value: &T,
        max_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        let input = shared.encode(value, max_bytes)?;
        let _scratch =
            shared.reserve_scratch(ByteClass::Data, json_decode_budget(input.as_slice())?)?;
        let value: serde_json::Value =
            serde_json::from_slice(input.as_slice()).map_err(|_| invalid_json())?;
        drop(input);
        let bytes = shared.encode(&value, max_bytes)?;
        Ok(Self(Arc::new(JsonStorage::Owned(bytes))))
    }
    pub fn from_canonical_bytes(
        shared: &WorkerSharedResources,
        bytes: &[u8],
        max_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        if bytes.len() > max_bytes {
            return Err(invalid_json());
        }
        let _scratch = shared.reserve_scratch(ByteClass::Data, json_decode_budget(bytes)?)?;
        validate_canonical(bytes)?;
        drop(_scratch);
        Ok(Self(Arc::new(JsonStorage::Owned(
            shared.copy_bytes(bytes)?,
        ))))
    }
    pub fn as_str(&self) -> &str {
        match &*self.0 {
            JsonStorage::Owned(bytes) => {
                std::str::from_utf8(bytes.as_slice()).expect("constructor validates JSON UTF8")
            }
            JsonStorage::Unattached { raw, .. } => raw.get(),
        }
    }
    pub fn is_charged(&self) -> bool {
        match &*self.0 {
            JsonStorage::Owned(_) => true,
            JsonStorage::Unattached { owner, .. } => owner.get().is_some(),
        }
    }
    /// Uncharged metadata/raw allocations only. The envelope checks the SUM
    /// across all fields and metadata before attaching any individual value.
    pub fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        if self.is_charged() {
            return Ok(0);
        }
        self.as_str()
            .len()
            .checked_add(128)
            .ok_or_else(invalid_json)
    }
    pub fn attach_charge(&self, charge: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let JsonStorage::Unattached { raw, owner } = &*self.0 {
            if owner.get().is_some() {
                return Ok(());
            }
            // Includes temporary Value verification and metadata overlap. The
            // outer decoder must additionally cover the aggregate command.
            if charge.bytes() < json_decode_budget(raw.get().as_bytes())? {
                return Err(invalid_json());
            }
            validate_canonical(raw.get().as_bytes())?;
            let _ = owner.set(charge);
        }
        Ok(())
    }
    pub fn ensure_charged(&self) -> Result<(), ProtocolError> {
        if self.is_charged() {
            Ok(())
        } else {
            Err(failure(
                ErrorCode::StorageUnavailable,
                "unattached fixed JSON payload",
            ))
        }
    }
}
impl Serialize for CanonicalJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.ensure_charged().map_err(serde::ser::Error::custom)?;
        let raw: &RawValue =
            serde_json::from_str(self.as_str()).map_err(serde::ser::Error::custom)?;
        raw.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for CanonicalJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Allocation is covered by the codec's already-owned decode lease.
        // This value cannot serialize or enter application until attached.
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Ok(Self(Arc::new(JsonStorage::Unattached {
            raw: Arc::from(raw),
            owner: OnceLock::new(),
        })))
    }
}

fn validate_canonical(bytes: &[u8]) -> Result<(), ProtocolError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid_json())?;
    let mut comparison = ComparisonWriter {
        expected: bytes,
        offset: 0,
    };
    serde_json::to_writer(&mut comparison, &value).map_err(|_| invalid_json())?;
    if comparison.offset != bytes.len() {
        return Err(invalid_json());
    }
    Ok(())
}
struct ComparisonWriter<'a> {
    expected: &'a [u8],
    offset: usize,
}
impl io::Write for ComparisonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("invalid JSON length"))?;
        if self.expected.get(self.offset..end) != Some(bytes) {
            return Err(io::Error::other("noncanonical JSON"));
        }
        self.offset = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn invalid_json() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "invalid or unbudgeted fixed JSON",
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MvccKey {
    Row {
        table_id: Uuid,
        primary_key: Key,
    },
    CatalogName {
        name: Text<128>,
    },
    CatalogTable {
        table_id: Uuid,
    },
    CatalogIndex {
        table_id: Uuid,
        index_id: Uuid,
    },
    CatalogIndexName {
        table_id: Uuid,
        name: Text<128>,
    },
    CatalogRoutingRoot {
        table_id: Uuid,
    },
    CatalogRoutingPage {
        table_id: Uuid,
        page_id: Uuid,
    },
    CatalogIndexesRoot {
        table_id: Uuid,
    },
    CatalogIndexesPage {
        table_id: Uuid,
        page_id: Uuid,
    },
    CatalogIndexProgress {
        table_id: Uuid,
        progress_id: Uuid,
    },
    CatalogIndexClosed {
        table_id: Uuid,
        closure_id: Uuid,
    },
    CatalogSplitClosed {
        table_id: Uuid,
        split_id: Uuid,
    },
    IndexEntry {
        table_id: Uuid,
        index_id: Uuid,
        values: nabedra_oltp_types::types::Page<index::IndexValue>,
        primary_key: Key,
    },
    /// A direct-commit dedup record. It is stored in its own `'D'` namespace,
    /// never as a versioned head. Images carry it so split children, backups
    /// and restores keep exact retry results. It sorts after every other key
    /// and, within a table, by anchor primary key then request id.
    DirectDedup {
        table_id: Uuid,
        anchor: Key,
        request_id: Uuid,
    },
}
impl MvccKey {
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::IndexEntry {
                values,
                primary_key,
                ..
            } => {
                values.capacity() * std::mem::size_of::<index::IndexValue>()
                    + values
                        .iter()
                        .map(index::IndexValue::heap_bytes)
                        .sum::<usize>()
                    + match primary_key {
                        Key::Utf8(v) => v.capacity(),
                        _ => 0,
                    }
            }
            Self::Row {
                primary_key: Key::Utf8(value),
                ..
            }
            | Self::DirectDedup {
                anchor: Key::Utf8(value),
                ..
            } => value.capacity(),
            Self::CatalogName { name } | Self::CatalogIndexName { name, .. } => {
                name.capacity_bytes()
            }
            _ => 0,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    Keep,
    Put(CanonicalJson),
    Delete,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedWrite {
    pub key: MvccKey,
    /// None is never-present only. A tombstone has its exact commit timestamp.
    pub expected_version: Option<CommitTs>,
    pub change: Change,
}
impl PreparedWrite {
    pub fn attach_charge(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Change::Put(value) = &self.change {
            value.attach_charge(owner)?;
        }
        Ok(())
    }
    pub fn fields_attached(&self) -> bool {
        match &self.change {
            Change::Put(value) => value.is_charged(),
            _ => true,
        }
    }
}
impl nabedra_oltp_types::owned::OwnedPayload for PreparedWrite {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let value = match &self.change {
            Change::Put(value) => value.heap_bytes()?,
            _ => 0,
        };
        self.key
            .heap_bytes()
            .checked_add(value)
            .ok_or_else(invalid_json)
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.attach_charge(owner)
    }
    fn fields_attached(&self) -> bool {
        PreparedWrite::fields_attached(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::pool::SharedResourceConfig;
    use nabedra_resources::config::WorkerResources;
    fn resources() -> Arc<WorkerSharedResources> {
        WorkerSharedResources::new(
            0,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap()
    }
    #[test]
    fn m01_f03_canonical_json_needs_charge_and_extracted_clones_keep_decode_owner_after_frame_drop()
    {
        let resources = resources();
        let baseline = resources.scheduler().usage()[0].2;
        let wire = br#"{"a":1,"b":"large text"}"#;
        let owner = Arc::new(
            resources
                .reserve_scratch(ByteClass::Data, json_decode_budget(wire).unwrap())
                .unwrap(),
        );
        let decoded: CanonicalJson = serde_json::from_slice(wire).unwrap();
        assert!(!decoded.is_charged());
        assert!(serde_json::to_vec(&decoded).is_err());
        decoded.attach_charge(owner.clone()).unwrap();
        let clone = decoded.clone();
        drop(owner);
        drop(decoded);
        assert!(resources.scheduler().usage()[0].2 > baseline);
        assert_eq!(serde_json::to_vec(&clone).unwrap(), wire);
        drop(clone);
        assert_eq!(resources.scheduler().usage()[0].2, baseline);
    }
    #[test]
    fn m01_fixed_json_is_sorted_compact_and_rejects_insufficient_decode_charge() {
        let resources = resources();
        let canonical = CanonicalJson::from_serializable(
            &resources,
            &serde_json::json!({"z":true,"a":1.0}),
            1024,
        )
        .unwrap();
        assert_eq!(canonical.as_str(), r#"{"a":1.0,"z":true}"#);
        assert!(CanonicalJson::from_canonical_bytes(&resources, br#"{ "a":1 }"#, 1024).is_err());
        let raw: CanonicalJson = serde_json::from_str(r#"{"a":1}"#).unwrap();
        let small = Arc::new(resources.reserve_scratch(ByteClass::Data, 1).unwrap());
        assert!(raw.attach_charge(small).is_err());
        assert!(!raw.is_charged());
        drop(canonical);
        assert_eq!(resources.control_bytes_used(), 0);
    }

    #[test]
    fn m01_wide_fixed_value_releases_source_copy_before_encoding_canonical_output() {
        let resources = resources();
        let text = "x".repeat(7 << 20);
        let canonical =
            CanonicalJson::from_serializable(&resources, &text, crate::protocol::MAX_BODY_BYTES)
                .unwrap();
        assert_eq!(canonical.as_str().len(), text.len() + 2);
        assert_eq!(resources.control_bytes_used(), canonical.as_str().len());
        drop(canonical);
        assert_eq!(resources.control_bytes_used(), 0);
    }
}
