//! Legacy S3 adapter. Provider setup and SDK types stay behind the `s3` feature.
use super::{
    Download, Namespace, ObjectAccessError, ObjectKey, ObjectStore, Storage, StorageError,
    WriteCondition,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream};

#[derive(Clone)]
pub struct S3Store {
    namespace: Namespace,
    bucket: String,
    reads: Client,
    writes: Client,
    #[cfg(test)]
    snapshot: Option<aws_sdk_s3::config::Credentials>,
    reservation: Option<std::sync::Arc<dyn Send + Sync>>,
}
impl S3Store {
    /// Uses the standard server-side credential and region provider chains.
    /// The caller supplies configuration explicitly; this never reads .env files.
    pub async fn connect(bucket: &str, namespace: Namespace) -> Result<Self, StorageError> {
        Self::connect_region(bucket, namespace, None).await
    }
    pub async fn connect_region(
        bucket: &str,
        namespace: Namespace,
        region: Option<&str>,
    ) -> Result<Self, StorageError> {
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
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(crate::aws_http::https_client());
        if let Some(region) = region {
            if !super::context::valid_region(region) {
                return Err(StorageError::Configuration);
            }
            loader = loader.region(aws_sdk_s3::config::Region::new(region.to_owned()));
        }
        let config = loader.load().await;
        if config.region().is_none() {
            return Err(StorageError::Configuration);
        }
        let mut builder = aws_sdk_s3::config::Builder::from(&config)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(5))
                    .read_timeout(std::time::Duration::from_secs(30))
                    .operation_timeout(std::time::Duration::from_secs(30))
                    .build(),
            )
            .retry_config(RetryConfig::disabled());
        builder.set_endpoint_url(None);
        let writes = Client::from_conf(builder.build());
        Ok(Self {
            #[cfg(test)]
            snapshot: None,
            reservation: None,
            namespace,
            bucket: bucket.into(),
            reads: writes.clone(),
            writes,
        })
    }
}
impl S3Store {
    pub async fn from_context(
        context: &super::context::RequestContext,
    ) -> Result<Self, StorageError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| StorageError::Configuration)?
            .as_secs();
        context.validate(now)?;
        let c = context.credentials();
        let credentials = aws_sdk_s3::config::Credentials::new(
            &c.access_key_id,
            &c.secret_access_key,
            c.session_token.clone(),
            None,
            "nabedra-gateway-snapshot",
        );
        // Starting from an empty SDK builder deliberately excludes operator
        // identity, profile/process resolution and endpoint environment settings.
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .http_client(crate::aws_http::https_client())
            .region(aws_sdk_s3::config::Region::new(context.region().to_owned()))
            .credentials_provider(credentials.clone())
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(5))
                    .read_timeout(std::time::Duration::from_secs(30))
                    .operation_timeout(std::time::Duration::from_secs(30))
                    .build(),
            )
            .retry_config(RetryConfig::disabled())
            .build();
        let writes = Client::from_conf(config);
        Ok(Self {
            #[cfg(test)]
            snapshot: Some(credentials),
            reservation: None,
            namespace: context.namespace()?,
            bucket: context.bucket().into(),
            reads: writes.clone(),
            writes,
        })
    }
    pub(crate) fn retain_reservation(&mut self, reservation: std::sync::Arc<dyn Send + Sync>) {
        self.reservation = Some(reservation);
    }
    pub fn region(&self) -> &str {
        self.writes
            .config()
            .region()
            .expect("validated region")
            .as_ref()
    }
    fn check_key(&self, key: &str) -> Result<(), StorageError> {
        let relative = key
            .strip_prefix(self.namespace.as_str())
            .ok_or(StorageError::InvalidPath)?;
        ObjectKey::parse_relative(relative).map(|_| ())
    }
    async fn get_with_access_error(&self, key: &str) -> Result<Download, ObjectAccessError> {
        self.check_key(key)?;
        let response = self
            .reads
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(
                |error| match error.raw_response().map(|r| r.status().as_u16()) {
                    Some(401 | 403) => ObjectAccessError::Denied,
                    Some(404)
                        if error.as_service_error().and_then(|e| e.code())
                            != Some("NoSuchBucket") =>
                    {
                        StorageError::NotFound.into()
                    }
                    _ => StorageError::Unavailable.into(),
                },
            )?;
        let etag = response
            .e_tag
            .filter(|s| !s.is_empty())
            .ok_or(StorageError::Unavailable)?;
        Ok(Download::new(
            DeadlineReader {
                reader: response.body.into_async_read(),
                deadline: Box::pin(tokio::time::sleep(std::time::Duration::from_secs(30))),
            },
            etag,
        ))
    }
}
impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.get_with_access_error(key)
            .await
            .map_err(ObjectAccessError::into_storage_error)
    }
    async fn authorize_get(&self, key: &str) -> Result<(), ObjectAccessError> {
        self.get_with_access_error(key).await.map(drop)
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.check_key(key)?;
        if !ObjectKey::parse_relative(
            key.strip_prefix(self.namespace.as_str())
                .ok_or(StorageError::InvalidPath)?,
        )?
        .permits_match()
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

#[cfg(test)]
impl S3Store {
    /// Contract-suite helper: an unconditional DELETE of a namespaced key.
    pub(crate) async fn test_delete(&self, key: &str) -> Result<(), StorageError> {
        self.check_key(key)?;
        self.writes
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(drop)
            .map_err(|_| StorageError::Unavailable)
    }
    /// Contract-suite helper: one LIST page (at most 1,000 keys) under `prefix`.
    pub(crate) async fn test_list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        if !prefix.starts_with(self.namespace.as_str()) {
            return Err(StorageError::InvalidPath);
        }
        let response = self
            .reads
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|_| StorageError::Unavailable)?;
        let mut keys: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_owned))
            .collect();
        keys.sort();
        Ok(keys)
    }
    pub(crate) async fn test_credentials(&self) -> aws_sdk_s3::config::Credentials {
        self.snapshot
            .as_ref()
            .expect("explicit customer snapshot")
            .clone()
    }
}

struct DeadlineReader<R> {
    reader: R,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for DeadlineReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::future::Future;
        if self.deadline.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "storage deadline",
            )));
        }
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod live_context_probe {
    use super::*;
    /// Private opt-in probe. Launcher supplies a newly allocated prefix and
    /// explicit credentials only in child memory/environment, never a file.
    #[tokio::test]
    #[ignore = "requires explicitly authorized fresh live S3 fixture"]
    async fn explicit_context_first_use() {
        assert_eq!(
            std::env::var("NABEDRA_LIVE_CONTEXT_PROBE").ok().as_deref(),
            Some("1")
        );
        let context: super::super::context::RequestContext = serde_json::from_value(serde_json::json!({
            "version":1,"bucket":std::env::var("NABEDRA_S3_BUCKET").unwrap(),
            "region":std::env::var("AWS_REGION").or_else(|_|std::env::var("AWS_DEFAULT_REGION")).unwrap(),
            "prefix":std::env::var("NABEDRA_S3_PREFIX").unwrap(),
            "credentials":{"access_key_id":std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
            "secret_access_key":std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
            "session_token":std::env::var("AWS_SESSION_TOKEN").ok(),"expires_at":null}
        })).unwrap();
        eprintln!("live-context-probe phase=construct");
        let client = S3Store::from_context(&context).await.unwrap();
        eprintln!("live-context-probe phase=absent-get");
        let response = client
            .reads
            .get_object()
            .bucket(context.bucket())
            .key(format!(
                "{}catalog.json",
                context.namespace().unwrap().as_str()
            ))
            .send()
            .await;
        let error = match response {
            Err(error) => error,
            Ok(_) => panic!("fresh probe target already exists"),
        };
        let kind = match error.as_service_error().and_then(|e| e.code()) {
            Some("NoSuchKey") => "NoSuchKey",
            Some("AccessDenied") => "AccessDenied",
            Some("InvalidAccessKeyId") => "InvalidAccessKeyId",
            Some("SignatureDoesNotMatch") => "SignatureDoesNotMatch",
            Some("ExpiredToken") => "ExpiredToken",
            Some(_) => "OtherServiceError",
            None => {
                use aws_sdk_s3::error::SdkError;
                match &error {
                    SdkError::ConstructionFailure(_) => "ConstructionFailure",
                    SdkError::TimeoutError(_) => "TimeoutError",
                    SdkError::DispatchFailure(_) => "DispatchFailure",
                    SdkError::ResponseError(_) => "ResponseError",
                    _ => "OtherSdkError",
                }
            }
        };
        eprintln!("live-context-probe absent-get-kind={kind}");
        assert_eq!(kind, "NoSuchKey");
        let storage = Storage::new(client, context.namespace().unwrap());
        let owner = uuid::Uuid::new_v4();
        let fence = super::super::CatalogFence {
            run_id: uuid::Uuid::new_v4(),
            scheduler_term: 1,
        };
        eprintln!("live-context-probe phase=owned-init");
        let loaded = storage
            .initialize_owned_cluster_catalog(owner, fence, false, false)
            .await
            .unwrap();
        assert_eq!(loaded.catalog().owner_cluster, Some(owner));
        assert!(loaded.catalog().tables.is_empty());
        eprintln!("live-context-probe phase=read-list");
        let loaded = storage.load_cluster_catalog().await.unwrap();
        assert_eq!(loaded.catalog().fence, Some(fence));
        assert!(loaded.catalog().tables.is_empty());
        eprintln!("live-context-probe phase=catalog-compare-and-swap");
        let takeover = super::super::CatalogFence {
            scheduler_term: 2,
            ..fence
        };
        storage
            .fence_cluster_catalog(&loaded, takeover, super::super::FenceTransition::Takeover)
            .await
            .unwrap();
        let stale = super::super::CatalogFence {
            scheduler_term: 3,
            ..fence
        };
        assert!(matches!(
            storage
                .fence_cluster_catalog(&loaded, stale, super::super::FenceTransition::Takeover)
                .await,
            Err(StorageError::Conflict)
        ));
        let current = storage.load_cluster_catalog().await.unwrap();
        assert_eq!(current.catalog().fence, Some(takeover));
        assert_eq!(current.catalog().owner_cluster, Some(owner));
        eprintln!("live-context-probe phase=complete");
    }
}
