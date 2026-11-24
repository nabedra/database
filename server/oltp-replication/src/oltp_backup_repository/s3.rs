use super::*;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    primitives::ByteStream,
};
use nabedra_rt::time::{Instant, timeout_at};
use std::time::Duration;
use tokio::{
    io::AsyncReadExt,
    sync::{OwnedSemaphorePermit, Semaphore},
};

pub(crate) struct S3RepositoryConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    /// Explicit operator identity. None uses the normal AWS credential chain.
    pub credentials: Option<Credentials>,
    /// Endpoint overrides are deliberately limited to local SDK contract tests.
    #[cfg(test)]
    pub fixture_endpoint: Option<String>,
}
#[derive(Clone)]
pub(crate) struct S3Repository {
    client: Client,
    bucket: String,
    prefix: String,
    shared: Arc<WorkerSharedResources>,
    slots: Arc<Semaphore>,
}
struct BodyOwner {
    bytes: ChargedBytes,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for BodyOwner {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}
impl S3Repository {
    pub(crate) async fn connect(
        config: S3RepositoryConfig,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !(3..=63).contains(&config.bucket.len())
            || config.bucket.contains("..")
            || !config
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
            || !config.bucket.as_bytes()[0].is_ascii_alphanumeric()
            || !config.bucket.as_bytes()[config.bucket.len() - 1].is_ascii_alphanumeric()
            || config.bucket.parse::<std::net::Ipv4Addr>().is_ok()
            || config.prefix.len() > 512
            || config.prefix.split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || !part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            })
            || config.region.is_empty()
            || config.region.len() > 64
            || !config
                .region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid());
        }
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(crate::aws_http::https_client())
            .region(Region::new(config.region));
        if let Some(credentials) = config.credentials {
            loader = loader.credentials_provider(credentials);
        }
        let loaded = nabedra_rt::time::timeout(Duration::from_secs(15), loader.load())
            .await
            .map_err(|_| unavailable())?;
        let mut builder = aws_sdk_s3::config::Builder::from(&loaded)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(Duration::from_secs(15))
                    .operation_timeout(Duration::from_secs(30))
                    .build(),
            )
            .retry_config(RetryConfig::disabled())
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .response_checksum_validation(
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
            );
        builder.set_endpoint_url(None);
        #[cfg(test)]
        if let Some(endpoint) = config.fixture_endpoint {
            let uri: axum::http::Uri = endpoint.parse().map_err(|_| invalid())?;
            if uri.scheme_str() != Some("http")
                || !matches!(uri.host(), Some("127.0.0.1" | "localhost" | "[::1]"))
                || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
            {
                return Err(invalid());
            }
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }
        Ok(Arc::new(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket,
            prefix: config.prefix,
            shared,
            slots: Arc::new(Semaphore::new(1)),
        }))
    }
    fn object_key(&self, key: ArchiveKey, name: &str) -> String {
        format!("{}/{}{}", self.prefix, key.prefix(), name)
    }
    async fn get_inner(
        &self,
        key: String,
        limit: usize,
        deadline: Instant,
    ) -> Result<ChargedBytes, ProtocolError> {
        timeout_at(deadline, async {
            let response = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|_| unavailable())?;
            let length = response
                .content_length
                .and_then(|v| usize::try_from(v).ok())
                .filter(|n| *n > 0 && *n <= limit)
                .ok_or_else(corrupt)?;
            let expected = response
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("nabedra-sha256"))
                .and_then(|digest| Digest::parse(digest).ok())
                .ok_or_else(corrupt)?;
            let mut bytes = self.shared.allocate_bytes(ByteClass::Data, length)?;
            let mut body = response.body.into_async_read();
            body.read_exact(bytes.as_mut_slice())
                .await
                .map_err(|_| unavailable())?;
            let mut extra = [0];
            if body.read(&mut extra).await.map_err(|_| unavailable())? != 0 {
                return Err(corrupt());
            }
            let bytes = bytes.freeze();
            if hash(bytes.as_ref()) != expected {
                return Err(corrupt());
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| unavailable())?
    }
}
impl private::Sealed for S3Repository {}
impl Repository for S3Repository {
    fn shared(&self) -> &Arc<WorkerSharedResources> {
        &self.shared
    }
    fn put<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        body: ChargedBytes,
    ) -> RepositoryFuture<'a, Digest> {
        Box::pin(async move {
            let limit = stored_limit(&name);
            if !key.valid() || body.is_empty() || body.len() > limit {
                return Err(invalid());
            }
            let slot = Arc::new(self.slots.clone().try_acquire_owned().map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "archive repository IO slot is busy",
                    Outcome::NotApplied,
                )
            })?);
            let repository = self.clone();
            let name = self.object_key(key, &name);
            let expected = hash(body.as_ref());
            let length = body.len();
            // A detached owner survives cancellation of the caller. Its body
            // retains both the byte charge and IO slot in HTTP transport clones.
            nabedra_rt::task::spawn(async move {
                let _slot = slot.clone();
                let deadline = nabedra_rt::time::now() + Duration::from_secs(30);
                let bytes = bytes::Bytes::from_owner(BodyOwner {
                    bytes: body,
                    _slot: slot,
                });
                let sent = timeout_at(
                    deadline,
                    repository
                        .client
                        .put_object()
                        .bucket(&repository.bucket)
                        .key(&name)
                        .if_none_match("*")
                        .metadata("nabedra-sha256", expected.to_string())
                        .content_length(length as i64)
                        .body(ByteStream::from(bytes))
                        .send(),
                )
                .await;
                match sent {
                    Ok(Ok(_)) => Ok(expected),
                    // Includes conditional conflict and ambiguous/lost replies.
                    // Read and hash the actual exact key, never infer success
                    // from HEAD metadata, an ETag or HTTP status alone.
                    _ => {
                        // A timed-out PUT can still settle at the provider. Give
                        // the exact GET its own bounded deadline. Missing or
                        // unreadable objects leave this attempt uncertain.
                        let existing = repository
                            .get_inner(name, limit, nabedra_rt::time::now() + Duration::from_secs(30))
                            .await?;
                        if existing.len() != length || hash(existing.as_ref()) != expected {
                            return Err(conflict());
                        }
                        Ok(expected)
                    }
                }
            })
            .await
            .map_err(|_| unavailable())?
        })
    }
    fn get<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        limit: usize,
    ) -> RepositoryFuture<'a, ChargedBytes> {
        Box::pin(async move {
            if !key.valid() || limit == 0 || limit > stored_limit(&name) {
                return Err(invalid());
            }
            let slot = self.slots.clone().try_acquire_owned().map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "archive repository IO slot is busy",
                    Outcome::NotApplied,
                )
            })?;
            let repository = self.clone();
            let name = self.object_key(key, &name);
            nabedra_rt::task::spawn(async move {
                let _slot = slot;
                repository
                    .get_inner(name, limit, nabedra_rt::time::now() + Duration::from_secs(30))
                    .await
            })
            .await
            .map_err(|_| unavailable())?
        })
    }
}
