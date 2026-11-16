//! Storage identity and path validation shared with request records.
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
pub(super) fn safe_path(path: &str) -> Result<(), StorageError> {
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
