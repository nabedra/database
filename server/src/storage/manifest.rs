//! Versioned catalog wire compatibility and validated partition metadata.
use super::{ObjectKey, StorageError};
use crate::{
    partition_size::MAX_PARTITION_BYTES,
    protocol::{
        ColumnType, Key, MAX_CATALOG_BYTES, MAX_DECODED_PAYLOAD_BYTES, MAX_TABLE_ROWS, Schema,
        TableDescriptor, TableFormat,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Catalog {
    pub format_version: u32,
    pub revision: u64,
    pub commit_id: Uuid,
    pub tables: BTreeMap<String, CatalogEntry>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    pub id: Uuid,
    #[serde(deserialize_with = "unique_map")]
    pub schema: Schema,
    pub key: String,
    pub format: TableFormat,
    pub data: TableData,
    pub row_count: u64,
}
impl CatalogEntry {
    pub fn legacy_snapshot(&self) -> Result<&str, StorageError> {
        match &self.data {
            TableData::Legacy { snapshot } => Ok(snapshot),
            TableData::Partitioned { .. } => Err(StorageError::InvalidCatalog),
        }
    }
    /// Metadata-only routing. Legacy entries have no partition manifest.
    pub fn route(&self, key: &Key) -> Result<Option<&PartitionFile>, StorageError> {
        let ty = *self
            .schema
            .get(&self.key)
            .ok_or(StorageError::InvalidCatalog)?;
        key.validate(ty).map_err(|_| StorageError::InvalidCatalog)?;
        match &self.data {
            TableData::Legacy { .. } => Ok(None),
            TableData::Partitioned { files } => {
                for file in files {
                    if file.range.contains(key, ty)? {
                        return Ok(Some(file));
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn descriptor(&self, name: &str) -> TableDescriptor {
        TableDescriptor {
            id: self.id,
            name: name.into(),
            schema: self.schema.clone(),
            key: self.key.clone(),
            format: self.format,
        }
    }
    pub fn snapshot_key(&self) -> Result<ObjectKey, StorageError> {
        ObjectKey::parse_snapshot(self.legacy_snapshot()?, self.id)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedCatalog {
    pub catalog: Catalog,
    pub etag: String,
}
impl Catalog {
    pub fn empty() -> Self {
        Self {
            format_version: 2,
            revision: 0,
            commit_id: Uuid::from_u128(0x6e894cb9_9b3b_47e5_a964_581df72d7e14),
            tables: BTreeMap::new(),
        }
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        if !matches!(self.format_version, 1 | 2) {
            return Err(StorageError::UnsupportedCatalog);
        }
        let mut ids = BTreeSet::new();
        for (name, table) in &self.tables {
            table
                .descriptor(name)
                .validate()
                .map_err(|_| StorageError::InvalidCatalog)?;
            if !ids.insert(table.id)
                || table.row_count > MAX_TABLE_ROWS as u64
                || (self.format_version == 1 && !matches!(table.data, TableData::Legacy { .. }))
            {
                return Err(StorageError::InvalidCatalog);
            }
            table.validate_data()?;
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(StorageError::LimitExceeded);
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| StorageError::InvalidCatalog)?;
        if !value.is_object() || !value.get("tables").is_some_and(|v| v.is_object()) {
            return Err(StorageError::InvalidCatalog);
        }
        for entry in value["tables"]
            .as_object()
            .ok_or(StorageError::InvalidCatalog)?
            .values()
        {
            if !entry.is_object() || !entry.get("schema").is_some_and(|v| v.is_object()) {
                return Err(StorageError::InvalidCatalog);
            }
            if let Some(data) = entry.get("data") {
                if !data.is_object() {
                    return Err(StorageError::InvalidCatalog);
                }
                if let Some(files) = data.get("files") {
                    let files = files.as_array().ok_or(StorageError::InvalidCatalog)?;
                    if files.iter().any(|file| {
                        !file.is_object()
                            || !file.get("range").is_some_and(|range| range.is_object())
                    }) {
                        return Err(StorageError::InvalidCatalog);
                    }
                }
            }
        }
        let catalog: Self =
            serde_json::from_slice(bytes).map_err(|_| StorageError::InvalidCatalog)?;
        catalog.validate()?;
        Ok(catalog)
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        let mut output = BoundedCatalogWriter(Vec::new());
        serde_json::to_writer(
            &mut output,
            &Self {
                format_version: 2,
                ..self.clone()
            },
        )
        .map_err(|_| StorageError::LimitExceeded)?;
        Ok(output.0)
    }
}

// Reject duplicate names rather than silently replacing metadata during decoding.
fn unique_map<'de, D, T>(deserializer: D) -> Result<BTreeMap<String, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object with unique names")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, T>()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate name"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(Visitor(std::marker::PhantomData))
}
struct BoundedCatalogWriter(Vec<u8>);
impl std::io::Write for BoundedCatalogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_CATALOG_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("catalog byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "layout", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableData {
    Legacy { snapshot: String },
    Partitioned { files: Vec<PartitionFile> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionFile {
    pub object: String,
    pub range: KeyRange,
    pub row_count: u64,
    /// Actual length of the closed Parquet object, including its footer.
    pub byte_size: u64,
    /// Decoded payload bytes, used for the table-wide aggregate limit.
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRange {
    pub lower: Option<Key>,
    pub upper: Option<Key>,
}

fn compare(a: &Key, b: &Key) -> Result<Ordering, StorageError> {
    match (a, b) {
        (Key::Int64(a), Key::Int64(b)) => Ok(a.cmp(b)),
        (Key::Utf8(a), Key::Utf8(b)) => Ok(a.cmp(b)),
        _ => Err(StorageError::InvalidCatalog),
    }
}
impl KeyRange {
    pub fn validate(&self, ty: ColumnType) -> Result<(), StorageError> {
        if !matches!(ty, ColumnType::Int64 | ColumnType::Utf8) {
            return Err(StorageError::InvalidCatalog);
        }
        for bound in [&self.lower, &self.upper].into_iter().flatten() {
            bound
                .validate(ty)
                .map_err(|_| StorageError::InvalidCatalog)?;
        }
        if let (Some(lower), Some(upper)) = (&self.lower, &self.upper)
            && compare(lower, upper)? != Ordering::Less
        {
            return Err(StorageError::InvalidCatalog);
        }
        Ok(())
    }
    /// Inclusive lower bound and exclusive upper bound, with schema type checks.
    pub fn contains(&self, key: &Key, ty: ColumnType) -> Result<bool, StorageError> {
        self.validate(ty)?;
        key.validate(ty).map_err(|_| StorageError::InvalidCatalog)?;
        if let Some(lower) = &self.lower
            && compare(key, lower)? == Ordering::Less
        {
            return Ok(false);
        }
        if let Some(upper) = &self.upper
            && compare(key, upper)? != Ordering::Less
        {
            return Ok(false);
        }
        Ok(true)
    }
}
impl CatalogEntry {
    fn validate_data(&self) -> Result<(), StorageError> {
        let invalid = StorageError::InvalidCatalog;
        let TableData::Partitioned { files } = &self.data else {
            self.snapshot_key().map_err(|_| invalid)?;
            return Ok(());
        };
        let ty = *self.schema.get(&self.key).ok_or(invalid)?;
        let mut objects = BTreeSet::new();
        let mut rows = 0u64;
        let mut payload = 0u64;
        let mut previous_upper = None;
        for (index, file) in files.iter().enumerate() {
            ObjectKey::parse_partition(&file.object, self.id).map_err(|_| invalid)?;
            file.range.validate(ty)?;
            if !objects.insert(&file.object)
                || file.row_count == 0
                || file.byte_size == 0
                || file.byte_size > MAX_PARTITION_BYTES as u64
                || file.range.lower.as_ref() != previous_upper
                || (index + 1 < files.len() && file.range.upper.is_none())
            {
                return Err(invalid);
            }
            rows = rows.checked_add(file.row_count).ok_or(invalid)?;
            payload = payload.checked_add(file.payload_bytes).ok_or(invalid)?;
            if rows > MAX_TABLE_ROWS as u64 || payload > MAX_DECODED_PAYLOAD_BYTES as u64 {
                return Err(invalid);
            }
            previous_upper = file.range.upper.as_ref();
        }
        if previous_upper.is_some() || rows != self.row_count {
            return Err(invalid);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCatalog<T> {
    format_version: u32,
    revision: u64,
    commit_id: Uuid,
    #[serde(
        deserialize_with = "unique_map",
        bound(deserialize = "T: Deserialize<'de>")
    )]
    tables: BTreeMap<String, T>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyEntry {
    id: Uuid,
    #[serde(deserialize_with = "unique_map")]
    schema: Schema,
    key: String,
    format: TableFormat,
    snapshot: String,
    row_count: u64,
}
impl<'de> Deserialize<'de> for Catalog {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(d)?;
        let value: serde_json::Value =
            serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        if value.get("format_version").and_then(|v| v.as_u64()) == Some(1) {
            let wire: WireCatalog<LegacyEntry> =
                serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
            Ok(Self {
                format_version: wire.format_version,
                revision: wire.revision,
                commit_id: wire.commit_id,
                tables: wire
                    .tables
                    .into_iter()
                    .map(|(name, e)| {
                        (
                            name,
                            CatalogEntry {
                                id: e.id,
                                schema: e.schema,
                                key: e.key,
                                format: e.format,
                                data: TableData::Legacy {
                                    snapshot: e.snapshot,
                                },
                                row_count: e.row_count,
                            },
                        )
                    })
                    .collect(),
            })
        } else {
            let wire: WireCatalog<CatalogEntry> =
                serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
            Ok(Self {
                format_version: wire.format_version,
                revision: wire.revision,
                commit_id: wire.commit_id,
                tables: wire.tables,
            })
        }
    }
}
