//! Bounded object access and validated catalogs. The backend is a trusted adapter;
//! callers use Storage so every object key stays within its namespace.
use crate::protocol::{MAX_CATALOG_BYTES, MAX_TABLE_ROWS, Schema, TableDescriptor, TableFormat};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};
use uuid::Uuid;

/// Sanitized failures. Never contains SDK diagnostics or configuration values.
/// Ambiguous means a PUT may have succeeded; callers must reconcile, not retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    Configuration,
    InvalidPath,
    NotFound,
    Conflict,
    Unavailable,
    Ambiguous,
    LimitExceeded,
    InvalidCatalog,
    UnsupportedCatalog,
}
impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Configuration => "Storage configuration is missing or invalid",
            Self::InvalidPath => "Invalid storage path",
            Self::NotFound => "Object not found",
            Self::Conflict => "Conditional write conflict",
            Self::Unavailable => "Storage is unavailable",
            Self::Ambiguous => "Storage write outcome is unknown",
            Self::LimitExceeded => "Storage object exceeds byte limit",
            Self::InvalidCatalog => "Invalid catalog",
            Self::UnsupportedCatalog => "Unsupported catalog version",
        })
    }
}
impl std::error::Error for StorageError {}

#[derive(Debug, Clone)]
pub struct Namespace(String);
impl Namespace {
    pub fn new(prefix: &str) -> Result<Self, StorageError> {
        let prefix = prefix.trim_end_matches('/');
        safe_path(prefix)?;
        Ok(Self(format!("{prefix}/")))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
fn safe_path(path: &str) -> Result<(), StorageError> {
    if path.is_empty()
        || path.contains(['\\', ':', '%', '?', '#'])
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(StorageError::InvalidPath);
    }
    Ok(())
}
/// Only the catalog and canonical immutable snapshot keys can be addressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKey(String);
impl ObjectKey {
    pub fn catalog() -> Self {
        Self("catalog.json".into())
    }
    pub fn snapshot(table_id: Uuid, commit_id: Uuid) -> Self {
        Self(format!("tables/{table_id}/{commit_id}.parquet"))
    }
    pub fn parse_snapshot(path: &str, table_id: Uuid) -> Result<Self, StorageError> {
        safe_path(path)?;
        let parts: Vec<_> = path.split('/').collect();
        if parts.len() != 3 || parts[0] != "tables" || parts[1] != table_id.to_string() {
            return Err(StorageError::InvalidPath);
        }
        let commit = parts[2]
            .strip_suffix(".parquet")
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or(StorageError::InvalidPath)?;
        let key = Self::snapshot(table_id, commit);
        if key.0 != path {
            return Err(StorageError::InvalidPath);
        }
        Ok(key)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, Copy)]
pub enum WriteCondition<'a> {
    Create,
    Match(&'a str),
}
pub struct Download {
    pub body: ByteStream,
    pub etag: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub bytes: Vec<u8>,
    pub etag: String,
}
/// Adapter contract: get returns NotFound only for a definite missing object.
/// Put must apply its condition atomically and never automatically retry.
/// Keys are already namespace-qualified by Storage; there is no per-call bucket.
pub trait ObjectStore: Send + Sync {
    fn get(&self, key: &str) -> impl Future<Output = Result<Download, StorageError>> + Send;
    fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> impl Future<Output = Result<String, StorageError>> + Send;
}

pub struct S3Store {
    namespace: Namespace,
    bucket: String,
    reads: Client,
    writes: Client,
}
impl S3Store {
    /// Uses the standard server-side credential and region provider chains.
    /// The caller supplies configuration explicitly; this never reads .env files.
    async fn connect(bucket: &str, namespace: Namespace) -> Result<Self, StorageError> {
        // General purpose bucket names only; reject ARNs, URLs and access points.
        if !(3..=63).contains(&bucket.len())
            || !bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            || !bucket.as_bytes()[0].is_ascii_alphanumeric()
            || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
            || bucket.contains("..")
            || bucket.parse::<std::net::Ipv4Addr>().is_ok()
        {
            return Err(StorageError::Configuration);
        }
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        if config.region().is_none() {
            return Err(StorageError::Configuration);
        }
        let writes = Client::from_conf(
            aws_sdk_s3::config::Builder::from(&config)
                .retry_config(RetryConfig::disabled())
                .build(),
        );
        Ok(Self {
            namespace,
            bucket: bucket.into(),
            reads: Client::new(&config),
            writes,
        })
    }
}
impl S3Store {
    fn check_key(&self, key: &str) -> Result<(), StorageError> {
        let relative = key
            .strip_prefix(self.namespace.as_str())
            .ok_or(StorageError::InvalidPath)?;
        if relative == "catalog.json" {
            return Ok(());
        }
        let id = relative
            .split('/')
            .nth(1)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or(StorageError::InvalidPath)?;
        ObjectKey::parse_snapshot(relative, id).map(|_| ())
    }
}
impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.check_key(key)?;
        let response = self
            .reads
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                if error.as_service_error().and_then(|e| e.code()) != Some("NoSuchBucket")
                    && error
                        .raw_response()
                        .is_some_and(|r| r.status().as_u16() == 404)
                {
                    StorageError::NotFound
                } else {
                    StorageError::Unavailable
                }
            })?;
        let etag = response
            .e_tag
            .filter(|s| !s.is_empty())
            .ok_or(StorageError::Unavailable)?;
        Ok(Download {
            body: response.body,
            etag,
        })
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.check_key(key)?;
        if key != format!("{}catalog.json", self.namespace.as_str())
            && matches!(condition, WriteCondition::Match(_))
        {
            return Err(StorageError::InvalidPath);
        }
        let builder = self
            .writes
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes));
        let builder = match condition {
            WriteCondition::Create => builder.if_none_match("*"),
            WriteCondition::Match(etag) => builder.if_match(etag),
        };
        let response = builder.send().await.map_err(|error| {
            match error.raw_response().map(|r| r.status().as_u16()) {
                Some(409 | 412) => StorageError::Conflict,
                Some(400..=499) => StorageError::Unavailable,
                _ => StorageError::Ambiguous,
            }
        })?;
        response
            .e_tag
            .filter(|s| !s.is_empty())
            .ok_or(StorageError::Ambiguous)
    }
}

impl Storage<S3Store> {
    /// Validate the namespace before loading the standard AWS provider chains.
    pub async fn connect(bucket: &str, namespace: Namespace) -> Result<Self, StorageError> {
        let backend = S3Store::connect(bucket, namespace.clone()).await?;
        Ok(Self::new(backend, namespace))
    }
}
pub struct Storage<B> {
    backend: B,
    namespace: Namespace,
}
impl<B: ObjectStore> Storage<B> {
    pub fn new(backend: B, namespace: Namespace) -> Self {
        Self { backend, namespace }
    }
    pub fn backend(&self) -> &B {
        &self.backend
    }
    pub async fn read(&self, key: &ObjectKey, max_bytes: usize) -> Result<Object, StorageError> {
        let mut download = self
            .backend
            .get(&format!("{}{}", self.namespace.0, key.0))
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = download.body.next().await {
            let chunk = chunk.map_err(|_| StorageError::Unavailable)?;
            if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
                return Err(StorageError::LimitExceeded);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Object {
            bytes,
            etag: download.etag,
        })
    }
    pub async fn put(
        &self,
        key: &ObjectKey,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        if matches!(condition, WriteCondition::Match(etag) if etag.is_empty()) {
            return Err(StorageError::Configuration);
        }
        // Snapshots are immutable even if a caller accidentally requests replacement.
        if key != &ObjectKey::catalog() && matches!(condition, WriteCondition::Match(_)) {
            return Err(StorageError::InvalidPath);
        }
        self.backend
            .put(&format!("{}{}", self.namespace.0, key.0), bytes, condition)
            .await
    }
    pub async fn load_catalog(&self) -> Result<LoadedCatalog, StorageError> {
        let object = self.read(&ObjectKey::catalog(), MAX_CATALOG_BYTES).await?;
        Ok(LoadedCatalog {
            catalog: Catalog::decode(&object.bytes)?,
            etag: object.etag,
        })
    }
    pub async fn initialize_catalog(&self) -> Result<LoadedCatalog, StorageError> {
        match self.load_catalog().await {
            Ok(catalog) => return Ok(catalog),
            Err(StorageError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let catalog = Catalog::empty();
        match self
            .put(
                &ObjectKey::catalog(),
                catalog.encode()?,
                WriteCondition::Create,
            )
            .await
        {
            Ok(etag) => Ok(LoadedCatalog { catalog, etag }),
            Err(StorageError::Conflict | StorageError::Ambiguous) => self.load_catalog().await,
            Err(error) => Err(error),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub format_version: u32,
    pub revision: u64,
    pub commit_id: Uuid,
    #[serde(deserialize_with = "unique_map")]
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
    pub snapshot: String,
    pub row_count: u64,
}
impl CatalogEntry {
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
        ObjectKey::parse_snapshot(&self.snapshot, self.id)
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
            format_version: 1,
            revision: 0,
            commit_id: Uuid::from_u128(0x6e894cb9_9b3b_47e5_a964_581df72d7e14),
            tables: BTreeMap::new(),
        }
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.format_version != 1 {
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
                || table.snapshot_key().is_err()
            {
                return Err(StorageError::InvalidCatalog);
            }
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
        }
        let catalog: Self =
            serde_json::from_slice(bytes).map_err(|_| StorageError::InvalidCatalog)?;
        catalog.validate()?;
        Ok(catalog)
    }
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        let mut output = BoundedCatalogWriter(Vec::new());
        serde_json::to_writer(&mut output, self).map_err(|_| StorageError::LimitExceeded)?;
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
