//! Legacy S3 adapter. Provider setup and SDK types stay behind the `s3` feature.
use super::{Download, Namespace, ObjectKey, ObjectStore, Storage, StorageError, WriteCondition};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};

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
        ObjectKey::parse_relative(relative).map(|_| ())
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
        Ok(Download::new(response.body.into_async_read(), etag))
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
