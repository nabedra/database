//! Bounded object access and validated catalogs. The backend is a trusted adapter;
//! callers use Storage so every object key stays within its namespace.
use crate::protocol::MAX_CATALOG_BYTES;
mod control_decision;
mod decision;
mod history;
mod publication;
pub use control_decision::{ControlDecision, MAX_CONTROL_BODY_BYTES, MAX_CONTROL_DECISION_BYTES};
pub use decision::{CatalogFence, ClusterCatalog, Decision, MAX_DECISION_BYTES};
pub use history::{FenceTransition, HistoryBudget, HistoryLookup, LoadedClusterCatalog};
pub use publication::{PreparedPublication, PublicationOutcome};
pub mod http;
mod manifest;
pub use http::HttpStore;
#[cfg(feature = "s3")]
pub mod s3;
pub use manifest::*;
#[cfg(feature = "s3")]
pub use s3::S3Store;
use std::{future::Future, pin::Pin};
use tokio::io::{AsyncRead, AsyncReadExt};
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
/// Only canonical table objects and explicitly typed metadata can be addressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKey(String);
impl ObjectKey {
    pub fn catalog() -> Self {
        Self("catalog.json".into())
    }
    pub fn decision(id: Uuid) -> Self {
        Self(format!("decisions/{id}.json"))
    }
    pub fn control_decision(id: Uuid) -> Self {
        Self(format!("control-decisions/{id}.json"))
    }
    pub fn control() -> Self {
        Self("cluster/control.json".into())
    }
    pub fn heartbeat(incarnation: Uuid) -> Self {
        Self(format!("cluster/heartbeats/{incarnation}.json"))
    }
    pub fn permits_match(&self) -> bool {
        self.0 == "catalog.json"
            || self.0 == "cluster/control.json"
            || self.0.starts_with("cluster/heartbeats/")
    }
    pub fn snapshot(table_id: Uuid, commit_id: Uuid) -> Self {
        Self(format!("tables/{table_id}/{commit_id}.parquet"))
    }
    pub fn partition(table_id: Uuid, commit_id: Uuid, ordinal: u32) -> Self {
        Self(format!("tables/{table_id}/{commit_id}/{ordinal}.parquet"))
    }
    pub fn parse_partition(path: &str, table_id: Uuid) -> Result<Self, StorageError> {
        safe_path(path)?;
        let parts: Vec<_> = path.split('/').collect();
        if parts.len() != 4 || parts[0] != "tables" || parts[1] != table_id.to_string() {
            return Err(StorageError::InvalidPath);
        }
        let commit = Uuid::parse_str(parts[2]).map_err(|_| StorageError::InvalidPath)?;
        let ordinal = parts[3]
            .strip_suffix(".parquet")
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or(StorageError::InvalidPath)?;
        let key = Self::partition(table_id, commit, ordinal);
        if key.0 != path {
            return Err(StorageError::InvalidPath);
        }
        Ok(key)
    }
    /// The shared relative-key guard used by Storage and the S3 adapter.
    pub fn parse_relative(path: &str) -> Result<Self, StorageError> {
        if path == "catalog.json" {
            return Ok(Self::catalog());
        }
        if path == "cluster/control.json" {
            return Ok(Self::control());
        }
        for (prefix, constructor) in [
            ("decisions/", Self::decision as fn(Uuid) -> Self),
            ("control-decisions/", Self::control_decision),
            ("cluster/heartbeats/", Self::heartbeat),
        ] {
            if let Some(id) = path
                .strip_prefix(prefix)
                .and_then(|s| s.strip_suffix(".json"))
            {
                let id = Uuid::parse_str(id).map_err(|_| StorageError::InvalidPath)?;
                let key = constructor(id);
                return if key.0 == path {
                    Ok(key)
                } else {
                    Err(StorageError::InvalidPath)
                };
            }
        }
        let id = path
            .split('/')
            .nth(1)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or(StorageError::InvalidPath)?;
        Self::parse_snapshot(path, id).or_else(|_| Self::parse_partition(path, id))
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
/// One complete object representation and its opaque conditional-write version.
/// Readers must report truncated/failed transfers as I/O errors, never clean EOF.
pub struct Download {
    pub body: Pin<Box<dyn AsyncRead + Send>>,
    pub etag: String,
}
impl Download {
    pub fn new(reader: impl AsyncRead + Send + 'static, etag: String) -> Self {
        Self {
            body: Box::pin(reader),
            etag,
        }
    }
    /// Construct a complete in-memory download, useful for embedded backends and fakes.
    pub fn from_bytes(bytes: Vec<u8>, etag: String) -> Self {
        Self::new(std::io::Cursor::new(bytes), etag)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub bytes: Vec<u8>,
    pub etag: String,
}
/// Trusted backend contract. GET is linearizable and returns a complete object
/// with a nonempty version for that same representation. NotFound means definite
/// absence; incomplete reads must fail rather than return a successful prefix.
///
/// Create atomically succeeds only for an absent key; Match atomically succeeds
/// only for the supplied current version. Success means durable bytes and a new,
/// nonempty, non-ABA version, even when replacing a key with identical bytes.
/// A failed condition returns Conflict without applying the write. Any failure
/// that may have applied a write must conservatively return Ambiguous. Adapters
/// must never automatically retry PUT; callers reconcile possibly applied writes.
///
/// Keys are already namespace-qualified by Storage; there is no per-call bucket.
/// Storage additionally enforces immutable snapshots and namespace validation.
pub trait ObjectStore: Send + Sync {
    fn get(&self, key: &str) -> impl Future<Output = Result<Download, StorageError>> + Send;
    fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> impl Future<Output = Result<String, StorageError>> + Send;
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
        ObjectKey::parse_relative(key.as_str())?;
        let mut download = self
            .backend
            .get(&format!("{}{}", self.namespace.0, key.0))
            .await?;
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            // Probe at most one byte beyond the remaining budget to detect excess
            // without buffering an unbounded body, including when max_bytes is zero.
            let capacity = chunk
                .len()
                .min(max_bytes.saturating_sub(bytes.len()).saturating_add(1));
            let read = download
                .body
                .read(&mut chunk[..capacity])
                .await
                .map_err(|_| StorageError::Unavailable)?;
            if read == 0 {
                break;
            }
            if read > max_bytes.saturating_sub(bytes.len()) {
                return Err(StorageError::LimitExceeded);
            }
            bytes.extend_from_slice(&chunk[..read]);
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
        ObjectKey::parse_relative(key.as_str())?;
        if matches!(condition, WriteCondition::Match(etag) if etag.is_empty()) {
            return Err(StorageError::Configuration);
        }
        // Snapshots are immutable even if a caller accidentally requests replacement.
        if !key.permits_match() && matches!(condition, WriteCondition::Match(_)) {
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
