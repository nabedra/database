//! One-exchange internal HTTP. The caller retains the operation through settlement.
use super::auth::{
    self, ApiSigningKey, Auth, Receiver, RequestContext, RequestIds, Sender, SenderSequence,
    WorkerSigningKey,
};
use crate::{
    cluster::{
        controller::OwnershipObservation, member::Node, startup_material::files::TlsMaterial,
        state::Control,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    storage::{RuntimeStore, Storage},
    worker_pool::WorkerPool,
};
use axum::{
    body::Body,
    http::{HeaderMap, Request, Response, header},
};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

pub(crate) type Result<T> = std::result::Result<T, ProtocolError>;
pub(crate) fn failure(code: ErrorCode) -> ProtocolError {
    ProtocolError::new(code, "cluster exchange failed", Outcome::NotApplied)
}
pub(crate) fn unavailable() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable)
}
pub(crate) fn invalid() -> ProtocolError {
    failure(ErrorCode::ValidationError)
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
pub(crate) fn one<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .ok_or_else(invalid)?
        .to_str()
        .map_err(|_| invalid())?;
    if values.next().is_some() {
        return Err(invalid());
    }
    Ok(value)
}
pub(crate) fn bearer(headers: &HeaderMap, expected: &str) -> bool {
    let Ok(value) = one(headers, "authorization") else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer")
        && token.len() == expected.len()
        && token
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |n, (a, b)| n | (a ^ b))
            == 0
}
pub(crate) fn framing(headers: &HeaderMap, cap: usize, chunked: bool) -> Result<Option<usize>> {
    if headers.len() > 32
        || headers
            .iter()
            .map(|(k, v)| k.as_str().len() + v.len() + 4)
            .sum::<usize>()
            > 16384
        || headers.contains_key(header::CONTENT_ENCODING)
        || headers.contains_key(header::TRAILER)
        || headers.contains_key(header::UPGRADE)
    {
        return Err(invalid());
    }
    if chunked {
        if headers.contains_key(header::CONTENT_LENGTH)
            || one(headers, "transfer-encoding")? != "chunked"
        {
            return Err(invalid());
        }
        Ok(None)
    } else {
        if headers.contains_key(header::TRANSFER_ENCODING) {
            return Err(invalid());
        }
        let length = one(headers, "content-length")?;
        if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let length: usize = length.parse().map_err(|_| invalid())?;
        if length > cap {
            return Err(failure(ErrorCode::LimitExceeded));
        }
        Ok(Some(length))
    }
}
pub(crate) async fn read_small(
    mut body: Body,
    length: usize,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(length);
    while let Some(frame) = timeout_at(deadline, body.frame())
        .await
        .map_err(|_| unavailable())?
    {
        let data = frame
            .map_err(|_| unavailable())?
            .into_data()
            .map_err(|_| invalid())?;
        if data.len() > length.saturating_sub(bytes.len()) {
            return Err(invalid());
        }
        bytes.extend_from_slice(&data);
    }
    if bytes.len() != length {
        return Err(invalid());
    }
    Ok(bytes)
}
pub(super) fn framed_reader(body: Body) -> super::payload_stream::Reader {
    let stream = futures_util::stream::unfold(body, |mut body| async move {
        body.frame().await.map(|frame| {
            let bytes = frame
                .map_err(|_| std::io::Error::other("body failed"))
                .and_then(|frame| {
                    frame
                        .into_data()
                        .map_err(|_| std::io::Error::other("unexpected trailers"))
                });
            (bytes, body)
        })
    });
    super::frame::FrameReader::new(Box::new(tokio_util::io::StreamReader::new(Box::pin(
        stream,
    ))))
}
pub(crate) async fn cpu<T, F>(pool: &WorkerPool, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&tokio::runtime::Runtime) -> Result<T> + Send + 'static,
{
    pool.submit(0, move |_, runtime| work(runtime))
        .await?
        .receive()
        .await
}
pub(crate) async fn load_root(
    storage: &Arc<Storage<RuntimeStore>>,
    pool: &WorkerPool,
) -> Result<Control> {
    let storage = storage.clone();
    cpu(pool, move |runtime| {
        runtime
            .block_on(storage.load_control(Instant::now() + Duration::from_secs(10)))
            .map(|loaded| loaded.root().clone())
            .map_err(|_| unavailable())
    })
    .await
}

#[derive(Clone)]
pub(crate) struct HttpClient {
    client: Client<HttpsConnector<HttpConnector>, Body>,
}
impl HttpClient {
    pub(crate) fn new(material: Option<&TlsMaterial>) -> Result<Self> {
        let roots = material
            .map(|m| m.ca.roots.clone())
            .unwrap_or_else(rustls::RootCertStore::empty);
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let client = Client::builder(TokioExecutor::new())
            .retry_canceled_requests(false)
            .pool_max_idle_per_host(0)
            .build(connector);
        Ok(Self { client })
    }
    pub(crate) async fn request(
        &self,
        request: Request<Body>,
        deadline: Instant,
    ) -> Result<Response<Body>> {
        let response = timeout_at(deadline, self.client.request(request))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        Ok(response.map(Body::new))
    }
}
enum Key {
    Api(ApiSigningKey),
    Worker(Arc<WorkerSigningKey>),
}
pub(crate) struct Signer {
    cluster: Uuid,
    sender: Sender,
    key: Key,
    sequence: Mutex<SenderSequence>,
}
impl Signer {
    pub(crate) fn api(cluster: Uuid, process: Uuid, token: &str) -> Result<Self> {
        let key = ApiSigningKey::derive(cluster, token).map_err(|_| invalid())?;
        let sender = Sender::api(process, hex(&key.public_key())).map_err(|_| invalid())?;
        let sequence = SenderSequence::for_api(cluster, &sender, &key).map_err(|_| invalid())?;
        Ok(Self {
            cluster,
            sender,
            key: Key::Api(key),
            sequence: Mutex::new(sequence),
        })
    }
    pub(crate) fn worker(
        cluster: Uuid,
        sender: Sender,
        key: Arc<WorkerSigningKey>,
    ) -> Result<Self> {
        let sequence = SenderSequence::for_worker(cluster, &sender, &key).map_err(|_| invalid())?;
        Ok(Self {
            cluster,
            sender,
            key: Key::Worker(key),
            sequence: Mutex::new(sequence),
        })
    }
    pub(crate) fn sign_queue(&self, bytes: &[u8]) -> Result<[u8; 64]> {
        match &self.key {
            Key::Api(key) => key.sign_queue(bytes),
            Key::Worker(key) => key.sign_queue(bytes),
        }
        .map_err(|_| invalid())
    }
    pub(crate) fn sender(&self) -> &Sender {
        &self.sender
    }
    pub(crate) fn public_key(&self) -> [u8; 32] {
        match &self.key {
            Key::Api(k) => k.public_key(),
            Key::Worker(k) => k.public_key(),
        }
    }
    pub(crate) fn context(
        &self,
        root: &Control,
        node: &Node,
        route: auth::Route,
        ids: RequestIds,
        bytes: Vec<u8>,
    ) -> Result<RequestContext> {
        let receiver = Receiver::new(
            node.node_id,
            node.incarnation.ok_or_else(unavailable)?,
            node.session.as_ref().ok_or_else(unavailable)?.id,
        );
        let auth = Auth::request(
            self.cluster,
            self.sender.clone(),
            receiver,
            route,
            ids,
            digest(&bytes),
        )
        .map_err(|_| invalid())?;
        let auth = self
            .sequence
            .lock()
            .unwrap()
            .stamp(auth)
            .map_err(|_| failure(ErrorCode::Overloaded))?;
        RequestContext::for_send(auth, bytes, root).map_err(|_| invalid())
    }
    pub(crate) fn request(
        &self,
        endpoint: &str,
        context: &RequestContext,
        body: Body,
        chunked: bool,
    ) -> Result<Request<Body>> {
        let a = context.auth();
        let signature = match &self.key {
            Key::Api(k) => k.sign_request(a),
            Key::Worker(k) => k.sign_request(a),
        }
        .map_err(|_| invalid())?;
        let path = serde_json::to_value(a.route()).map_err(|_| invalid())?;
        let mut request = Request::builder()
            .method("POST")
            .uri(format!(
                "{}{}",
                endpoint.trim_end_matches('/'),
                path.as_str().ok_or_else(invalid)?
            ))
            .header(header::CONNECTION, "close")
            .header(
                "x-nabedra-auth",
                hex(&a.canonical_bytes().map_err(|_| invalid())?),
            )
            .header("x-nabedra-signature", hex(&signature));
        if chunked {
            request = request
                .header(header::TRANSFER_ENCODING, "chunked")
                .header(header::CONTENT_TYPE, "application/octet-stream");
        } else {
            request = request
                .header(header::CONTENT_LENGTH, context.initial_bytes().len())
                .header(header::CONTENT_TYPE, "application/json");
        }
        if let Some(dependency) = a.dependency_id() {
            request = request.header("x-nabedra-dependency", dependency.to_string());
        }
        request.body(body).map_err(|_| invalid())
    }
    pub(crate) fn response(
        &self,
        request: &Auth,
        status: u16,
        initial: &[u8],
        body: Body,
        chunked: bool,
    ) -> Result<Response<Body>> {
        let Key::Worker(key) = &self.key else {
            return Err(invalid());
        };
        let auth = auth::ReplyAuth::response(request, self.sender.clone(), status, digest(initial))
            .map_err(|_| invalid())?;
        let signature = key.sign_response(&auth).map_err(|_| invalid())?;
        let mut response = Response::builder()
            .status(status)
            .header(header::CONNECTION, "close")
            .header(
                "x-nabedra-auth",
                hex(&auth.canonical_bytes().map_err(|_| invalid())?),
            )
            .header("x-nabedra-signature", hex(&signature));
        if chunked {
            response = response
                .header(header::TRANSFER_ENCODING, "chunked")
                .header(header::CONTENT_TYPE, "application/octet-stream");
        } else {
            response = response
                .header(header::CONTENT_LENGTH, initial.len())
                .header(header::CONTENT_TYPE, "application/json");
        }
        response.body(body).map_err(|_| invalid())
    }
}
/// An unsigned HTTP error is a backpressure/transport observation only. It
/// cannot prove that the peer did not apply an earlier or current invocation.
fn checked_status(status: u16) -> Result<()> {
    if status == 200 {
        return Ok(());
    }
    let code = match status {
        400 => ErrorCode::ValidationError,
        401 | 403 => ErrorCode::Unauthorized,
        404 => ErrorCode::NotFound,
        409 => ErrorCode::Conflict,
        413 => ErrorCode::LimitExceeded,
        429 => ErrorCode::Overloaded,
        _ => ErrorCode::StorageUnavailable,
    };
    Err(ProtocolError::new(
        code,
        "peer control HTTP exchange unavailable",
        Outcome::Unknown,
    ))
}
/// Retrying a control invocation retains its original logical permit. Each
/// invocation must construct a fresh signed sender sequence inside `exchange`.
pub(super) async fn control_backpressure<T, F, Fut>(mut exchange: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut delay = Duration::from_millis(10);
    for attempt in 0..32 {
        let result = timeout_at(deadline, exchange()).await.map_err(|_| {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "peer control retry deadline exhausted",
                Outcome::Unknown,
            )
        })?;
        match result {
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::Overloaded | ErrorCode::StorageUnavailable
                ) && attempt < 31 =>
            {
                let wake = Instant::now() + delay;
                if wake >= deadline {
                    return Err(error);
                }
                tokio::time::sleep_until(wake).await;
                delay = delay.saturating_mul(2).min(Duration::from_millis(250));
            }
            result => return result,
        }
    }
    unreachable!("bounded control attempts always return")
}
fn small_response_headers(response: &Response<Body>) -> Result<(u16, String, String, usize)> {
    let status = response.status().as_u16();
    checked_status(status)?;
    let a = one(response.headers(), "x-nabedra-auth")?.to_owned();
    let signature = one(response.headers(), "x-nabedra-signature")?.to_owned();
    let length =
        framing(response.headers(), crate::protocol::MAX_BODY_BYTES, false)?.ok_or_else(invalid)?;
    Ok((status, a, signature, length))
}
fn checked_small_reply(
    a: &str,
    signature: &str,
    status: u16,
    bytes: Vec<u8>,
    context: RequestContext,
    root: &Control,
) -> Result<auth::ReplyHandoff> {
    let signed = auth::verify_reply(a, signature, status, &context).map_err(|_| invalid())?;
    let content = auth::check_reply_content(&signed, bytes).map_err(|_| invalid())?;
    let eligible = auth::check_reply_authority(&signed, root, &context).map_err(|_| invalid())?;
    let observation = OwnershipObservation::new(root).map_err(|_| unavailable())?;
    context
        .into_reply_handoff(eligible, content, &observation)
        .map_err(|_| invalid())
}
pub(crate) async fn small_exchange(
    client: &HttpClient,
    signer: &Signer,
    context: RequestContext,
    endpoint: &str,
    storage: &Arc<Storage<RuntimeStore>>,
    pool: &WorkerPool,
) -> Result<auth::ReplyHandoff> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let request = signer.request(
        endpoint,
        &context,
        Body::from(context.initial_bytes().to_vec()),
        false,
    )?;
    let response = client.request(request, deadline).await?;
    let (status, a, signature, length) = small_response_headers(&response)?;
    let bytes = read_small(response.into_body(), length, deadline).await?;
    let root = load_root(storage, pool).await?;
    cpu(pool, move |_| {
        checked_small_reply(&a, &signature, status, bytes, context, &root)
    })
    .await
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
