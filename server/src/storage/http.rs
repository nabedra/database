//! Authenticated conditional-object HTTP transport. No redirects or PUT retries.
use super::{
    Download, Namespace, ObjectAccessError, ObjectKey, ObjectStore, StorageError, WriteCondition,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Uri,
    header::{self, HeaderMap, HeaderValue},
};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    time::{Instant, Sleep},
};
use tokio_util::io::StreamReader;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OBJECT: usize = 80 * 1024 * 1024;

/// Construct with `new(base_url, namespace, bearer_token, optional_ca_pem)`.
/// Construction performs only local validation. Connect timeout is 5s and the
/// complete request deadline is 30s, including streaming GET consumption.
/// TLS uses the supplied CA bundle or a conventional system PEM bundle. No
/// proxy, credential, or certificate environment variables are consulted.
/// Neither this type nor its errors expose the token or transport diagnostics.
#[derive(Clone)]
pub struct HttpStore {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base: String,
    namespace: Namespace,
    authorization: HeaderValue,
    #[cfg(test)]
    startup_ca_argument: Option<Vec<u8>>,
    /// Test-only: each queued value is sent once, on the next request, as
    /// `X-Nabedra-Test-Fault`. Only `local_store.py --test-faults` honours it.
    #[cfg(test)]
    test_faults: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<String>>>,
}

fn component_path(path: &str) -> bool {
    !path.is_empty()
        && path.split('/').all(|s| {
            !s.is_empty()
                && s != "."
                && s != ".."
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
        })
}
// String tokens use a reversible Latin-1 mapping for RFC obs-text bytes.
fn tag_bytes(tag: &str) -> Option<Vec<u8>> {
    let bytes: Vec<u8> = tag
        .chars()
        .map(u8::try_from)
        .collect::<Result<_, _>>()
        .ok()?;
    ((2..=1024).contains(&bytes.len())
        && bytes[0] == b'"'
        && bytes[bytes.len() - 1] == b'"'
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|b| *b == 0x21 || *b >= 0x23 && *b != 0x7f))
    .then_some(bytes)
}
fn etag(headers: &HeaderMap) -> Option<String> {
    let mut values = headers.get_all(header::ETAG).iter();
    let tag: String = values
        .next()?
        .as_bytes()
        .iter()
        .map(|b| char::from(*b))
        .collect();
    (values.next().is_none() && tag_bytes(&tag).is_some()).then_some(tag)
}
impl HttpStore {
    pub fn new(
        base_url: &str,
        namespace: Namespace,
        bearer_token: &str,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self, StorageError> {
        let invalid = StorageError::Configuration;
        let uri: Uri = base_url.parse().map_err(|_| invalid)?;
        let authority = uri.authority().ok_or(invalid)?;
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || authority.as_str().contains('@')
            || uri.host().is_none()
            || base_url.contains(['?', '#', '\\', '%'])
            || !base_url.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(invalid);
        }
        // Uri::port() conflates an absent port with an invalid explicit port.
        // host() retains IPv6 brackets, so only the suffix may specify a port.
        let host = authority.host();
        if host.is_empty() {
            return Err(invalid);
        }
        let suffix = authority.as_str().strip_prefix(host).ok_or(invalid)?;
        if !suffix.is_empty() {
            let port = suffix.strip_prefix(':').ok_or(invalid)?;
            if port.is_empty()
                || !port.bytes().all(|b| b.is_ascii_digit())
                || port.parse::<u16>().is_err()
            {
                return Err(invalid);
            }
        }
        let path = uri
            .path()
            .strip_prefix('/')
            .ok_or(invalid)?
            .strip_suffix('/')
            .unwrap_or(uri.path().strip_prefix('/').ok_or(invalid)?);
        if (path.is_empty() && uri.path() != "/") || (!path.is_empty() && !component_path(path)) {
            return Err(invalid);
        }
        let ns = namespace.as_str().strip_suffix('/').ok_or(invalid)?;
        if !component_path(ns)
            || namespace.as_str().len() + "catalog.json".len() > 4096
            || bearer_token.is_empty()
            || !bearer_token.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(invalid);
        }
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {bearer_token}")).map_err(|_| invalid)?;
        authorization.set_sensitive(true);
        let mut roots = rustls::RootCertStore::empty();
        let system_pem;
        let pem = if let Some(pem) = ca_pem {
            Some(pem)
        } else if uri.scheme_str() == Some("https") {
            system_pem = [
                "/etc/ssl/cert.pem",
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/certs/ca-bundle.crt",
            ]
            .iter()
            .find_map(|path| std::fs::read(path).ok())
            .ok_or(invalid)?;
            Some(system_pem.as_slice())
        } else {
            None
        };
        if let Some(pem) = pem {
            let mut count = 0;
            for cert in CertificateDer::pem_slice_iter(pem) {
                roots.add(cert.map_err(|_| invalid)?).map_err(|_| invalid)?;
                count += 1;
            }
            if count == 0 {
                return Err(invalid);
            }
        }
        let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(CONNECT_TIMEOUT));
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new())
            .retry_canceled_requests(false)
            .pool_max_idle_per_host(0)
            .build(connector);
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').into(),
            namespace,
            authorization,
            #[cfg(test)]
            startup_ca_argument: ca_pem.map(<[u8]>::to_vec),
            #[cfg(test)]
            test_faults: Default::default(),
        })
    }
    fn key(&self, key: &str) -> Result<ObjectKey, StorageError> {
        if key.len() > 4096 || !component_path(key) {
            return Err(StorageError::InvalidPath);
        }
        ObjectKey::parse_relative(
            key.strip_prefix(self.namespace.as_str())
                .ok_or(StorageError::InvalidPath)?,
        )
    }
    /// Test-only: send `fault` as `X-Nabedra-Test-Fault` on the next request.
    #[cfg(test)]
    pub(crate) fn push_test_fault(&self, fault: &str) {
        self.test_faults.lock().unwrap().push_back(fault.into());
    }
    fn request(
        &self,
        method: &str,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<Request<Full<Bytes>>, StorageError> {
        let builder = Request::builder();
        #[cfg(test)]
        let builder = match self.test_faults.lock().unwrap().pop_front() {
            Some(fault) => builder.header("X-Nabedra-Test-Fault", fault),
            None => builder,
        };
        builder
            .method(method)
            .uri(format!("{}/v1/objects/{key}", self.base))
            .header(header::AUTHORIZATION, self.authorization.clone())
            .header(header::CONTENT_LENGTH, bytes.len())
            .header(header::CONNECTION, "close")
            .body(Full::new(Bytes::from(bytes)))
            .map_err(|_| StorageError::Configuration)
    }
    async fn get_with_access_error(&self, key: &str) -> Result<Download, ObjectAccessError> {
        self.key(key)?;
        let request = self.request("GET", key, vec![])?;
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let response = tokio::time::timeout_at(deadline, self.client.request(request))
            .await
            .map_err(|_| StorageError::Unavailable)?
            .map_err(|_| StorageError::Unavailable)?;
        match response.status().as_u16() {
            401 | 403 => return Err(ObjectAccessError::Denied),
            404 => return Err(StorageError::NotFound.into()),
            200 => (),
            _ => return Err(StorageError::Unavailable.into()),
        }
        let etag = etag(response.headers()).ok_or(StorageError::Unavailable)?;
        let stream = response
            .into_body()
            .into_data_stream()
            .map_err(|_| io::Error::other("Storage read failed"));
        Ok(Download::new(
            DeadlineReader {
                inner: Box::pin(StreamReader::new(stream)),
                deadline: Box::pin(tokio::time::sleep_until(deadline)),
            },
            etag,
        ))
    }
}
impl ObjectStore for HttpStore {
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
        let relative = self.key(key)?;
        if !relative.permits_match() && matches!(condition, WriteCondition::Match(_)) {
            return Err(StorageError::InvalidPath);
        }
        if bytes.len() > MAX_OBJECT {
            return Err(StorageError::LimitExceeded);
        }
        let (name, value) = match condition {
            WriteCondition::Create => (header::IF_NONE_MATCH, vec![b'*']),
            WriteCondition::Match(tag) => (
                header::IF_MATCH,
                tag_bytes(tag).ok_or(StorageError::Configuration)?,
            ),
        };
        let mut request = self.request("PUT", key, bytes)?;
        request.headers_mut().insert(
            name,
            HeaderValue::from_bytes(&value).map_err(|_| StorageError::Configuration)?,
        );
        let response = tokio::time::timeout(REQUEST_TIMEOUT, self.client.request(request))
            .await
            .map_err(|_| StorageError::Ambiguous)?
            .map_err(|_| StorageError::Ambiguous)?;
        match response.status().as_u16() {
            412 => Err(StorageError::Conflict),
            400 | 401 | 403 | 411 | 413 | 414 | 417 => Err(StorageError::Unavailable),
            204 => {
                // The helper sends exactly Content-Length: 0. Accept absent CL
                // too, as HTTP defines 204 as bodyless. Never accept TE or a
                // positive/duplicate CL even though Hyper suppresses 204 bodies.
                let headers = response.headers();
                let lengths: Vec<_> = headers.get_all(header::CONTENT_LENGTH).iter().collect();
                if headers.contains_key(header::TRANSFER_ENCODING)
                    || lengths.len() > 1
                    || lengths
                        .first()
                        .is_some_and(|value| value.as_bytes() != b"0")
                {
                    return Err(StorageError::Ambiguous);
                }
                etag(headers).ok_or(StorageError::Ambiguous)
            }
            _ => Err(StorageError::Ambiguous),
        }
    }
}
struct DeadlineReader<R> {
    inner: Pin<Box<R>>,
    deadline: Pin<Box<Sleep>>,
}
impl<R: AsyncRead> AsyncRead for DeadlineReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Storage read timed out",
            )));
        }
        self.inner.as_mut().poll_read(cx, buf)
    }
}

#[cfg(test)]
pub(crate) mod startup_tests;
