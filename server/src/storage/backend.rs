//! A single distributed runtime supports either conditional object adapter.
use super::{Download, HttpStore, ObjectStore, StorageError, WriteCondition};
#[derive(Clone)]
pub enum RuntimeStore {
    Http(HttpStore),
    #[cfg(test)]
    Fixture {
        backend: HttpStore,
        _reservation: std::sync::Arc<dyn Send + Sync>,
    },
    #[cfg(feature = "s3")]
    S3(super::S3Store),
}
impl ObjectStore for RuntimeStore {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        match self {
            Self::Http(s) => s.get(key).await,
            #[cfg(test)]
            Self::Fixture { backend, .. } => backend.get(key).await,
            #[cfg(feature = "s3")]
            Self::S3(s) => s.get(key).await,
        }
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        match self {
            Self::Http(s) => s.put(key, bytes, condition).await,
            #[cfg(test)]
            Self::Fixture { backend, .. } => backend.put(key, bytes, condition).await,
            #[cfg(feature = "s3")]
            Self::S3(s) => s.put(key, bytes, condition).await,
        }
    }
}
