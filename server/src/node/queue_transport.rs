//! Signed, bounded queue control traffic uses its own cryptographic domain.
use super::{
    auth,
    http::{self, HttpClient, Result, Signer, invalid, unavailable},
};
use crate::{
    cluster::{member::Node, state::Control},
    protocol::ErrorCode,
};
use axum::{
    body::Body,
    http::{HeaderMap, Request, Response},
};
use bytes::Bytes;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};
use uuid::Uuid;

pub(super) const PATH: &str = "/internal/v1/queue";
pub(super) const MAX_BYTES: usize = 128 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    version: u8,
    command: String,
    class: String,
    cluster: Uuid,
    scheduler_term: u64,
    membership_revision: u64,
    receiver: Uuid,
    receiver_incarnation: Uuid,
    receiver_session: Uuid,
    sender_node: Option<Uuid>,
    sender_process: Uuid,
    sender_incarnation: Option<Uuid>,
    sender_session: Option<Uuid>,
    public_key: String,
    nonce: Uuid,
    response: bool,
    expires_at_ms: u64,
    digest: String,
}
fn unhex<const N: usize>(s: &str) -> Result<[u8; N]> {
    if s.len() != N * 2
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut out = [0; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(out)
}
pub(crate) fn command_class(command: &str) -> Result<&'static str> {
    Ok(match command {
        "submit" => "ingress",
        "lookup" => "receipt",
        "install" => "replica",
        "snapshot" | "bootstrap" => "bootstrap",
        "stats" | "seal" | "unseal" | "join" | "stream_release" => "progress",
        "stream_request" => "ingress",
        "stream_install" => "replica",
        "stream_reset" | "stream_page" => "bootstrap",
        _ => return Err(invalid()),
    })
}
fn wire_command(bytes: &[u8]) -> Result<&'static str> {
    for command in [
        "submit",
        "lookup",
        "install",
        "snapshot",
        "bootstrap",
        "stats",
        "seal",
        "unseal",
        "join",
        "stream_request",
        "stream_install",
        "stream_reset",
        "stream_page",
        "stream_release",
    ] {
        let prefix = format!("{{\"command\":\"{command}\"");
        if bytes.starts_with(prefix.as_bytes()) {
            return Ok(command);
        }
    }
    Err(invalid())
}
fn claims(
    signer: &Signer,
    root: &Control,
    receiver: &Node,
    nonce: Uuid,
    response: bool,
    bytes: &[u8],
) -> Result<Claims> {
    let sender = signer.sender();
    Ok(Claims {
        version: 1,
        command: wire_command(bytes)?.into(),
        class: command_class(wire_command(bytes)?)?.into(),
        cluster: root.cluster_id,
        scheduler_term: root.scheduler.scheduler_term,
        membership_revision: root.revision,
        receiver: receiver.node_id,
        receiver_incarnation: receiver.incarnation.ok_or_else(unavailable)?,
        receiver_session: receiver.session.as_ref().ok_or_else(unavailable)?.id,
        sender_node: sender.node_id(),
        sender_process: sender.process_id(),
        sender_incarnation: sender.incarnation(),
        sender_session: sender.session_id(),
        public_key: http::hex(&signer.public_key()),
        nonce,
        response,
        expires_at_ms: super::queue_runtime::now_ms().saturating_add(30000),
        digest: http::digest(bytes),
    })
}
fn signed(signer: &Signer, claims: &Claims) -> Result<(String, String)> {
    let bytes = serde_json::to_vec(claims).map_err(|_| invalid())?;
    Ok((http::hex(&bytes), http::hex(&signer.sign_queue(&bytes)?)))
}
fn verify(
    headers: &HeaderMap,
    root: &Control,
    local: &auth::Sender,
    pinned: &[u8; 32],
    stream: bool,
) -> Result<Claims> {
    let raw = http::one(headers, "x-nabedra-queue-auth")?;
    if raw.len() > 8192 || raw.len() % 2 != 0 {
        return Err(invalid());
    }
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    for index in (0..raw.len()).step_by(2) {
        bytes.push(u8::from_str_radix(&raw[index..index + 2], 16).map_err(|_| invalid())?);
    }
    let c: Claims = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if http::one(headers, "x-nabedra-queue-command")? != c.command
        || http::one(headers, "x-nabedra-queue-class")? != c.class
        || (command_class(&c.command)? != c.class
            && !(c.command == "install" && c.class == "bootstrap"))
    {
        return Err(invalid());
    }
    let stale_stream =
        stream && c.scheduler_term > 0 && c.scheduler_term < root.scheduler.scheduler_term;
    if c.version != 1
        || c.cluster != root.cluster_id
        || (stream && !c.command.starts_with("stream_"))
        || (c.scheduler_term != root.scheduler.scheduler_term && !stale_stream)
        || c.membership_revision > root.revision
        || c.response
    {
        return Err(invalid());
    }
    if Some(c.receiver) != local.node_id()
        || Some(c.receiver_incarnation) != local.incarnation()
        || Some(c.receiver_session) != local.session_id()
    {
        return Err(invalid());
    }
    if !stale_stream {
        auth::checked_worker(root, local).map_err(|_| http::failure(ErrorCode::Unauthorized))?;
    }
    let key = unhex::<32>(&c.public_key)?;
    let signature = unhex::<64>(http::one(headers, "x-nabedra-queue-signature")?)?;
    let mut input = b"nabedra.queue-control.v1\0".to_vec();
    input.extend_from_slice(&bytes);
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&input, &signature)
        .map_err(|_| http::failure(ErrorCode::Unauthorized))?;
    if let Some(node) = c.sender_node {
        let sender = auth::Sender::worker(
            node,
            c.sender_incarnation.ok_or_else(invalid)?,
            c.sender_session.ok_or_else(invalid)?,
            c.public_key.clone(),
        )
        .map_err(|_| invalid())?;
        if sender.process_id() != c.sender_process {
            return Err(invalid());
        }
        auth::checked_worker(root, &sender).map_err(|_| http::failure(ErrorCode::Unauthorized))?;
    } else if c.sender_incarnation.is_some() || c.sender_session.is_some() || &key != pinned {
        return Err(http::failure(ErrorCode::Unauthorized));
    }
    Ok(c)
}
pub(super) fn preauthenticate(
    headers: &HeaderMap,
    root: &Control,
    local: &auth::Sender,
    pinned: &[u8; 32],
    seen: &mut BTreeMap<Uuid, u64>,
) -> Result<(Uuid, Option<Uuid>, String, String, String)> {
    let c = verify(headers, root, local, pinned, false)?;
    admit_nonce(&c, seen)?;
    Ok((c.nonce, c.sender_node, c.digest, c.command, c.class))
}

pub(super) struct StreamAuthentication {
    pub nonce: Uuid,
    pub sender: Option<Uuid>,
    pub digest: String,
    pub command: String,
    pub class: String,
    pub stale_term: bool,
}

pub(super) fn preauthenticate_stream(
    headers: &HeaderMap,
    root: &Control,
    local: &auth::Sender,
    pinned: &[u8; 32],
    seen: &mut BTreeMap<Uuid, u64>,
) -> Result<StreamAuthentication> {
    let c = verify(headers, root, local, pinned, true)?;
    admit_nonce(&c, seen)?;
    Ok(StreamAuthentication {
        nonce: c.nonce,
        sender: c.sender_node,
        digest: c.digest,
        command: c.command,
        class: c.class,
        stale_term: c.scheduler_term != root.scheduler.scheduler_term,
    })
}

fn admit_nonce(c: &Claims, seen: &mut BTreeMap<Uuid, u64>) -> Result<()> {
    let now = super::queue_runtime::now_ms();
    if c.expires_at_ms < now || c.expires_at_ms > now.saturating_add(30000) {
        return Err(invalid());
    }
    seen.retain(|_, expiry| *expiry >= now);
    if seen.len() >= 8192 {
        return Err(http::failure(ErrorCode::Overloaded));
    }
    if seen.insert(c.nonce, c.expires_at_ms).is_some() {
        return Err(http::failure(ErrorCode::Conflict));
    }
    Ok(())
}
// API replies target the API's ephemeral process, so response authenticity is
// checked against the exact worker and request nonce instead of a member receiver.
pub(crate) async fn exchange(
    client: &HttpClient,
    signer: &Signer,
    root: &Control,
    node: &Node,
    body: Vec<u8>,
) -> Result<Vec<u8>> {
    exchange_bounded(client, signer, root, node, body, MAX_BYTES).await
}
pub(super) async fn exchange_bounded(
    client: &HttpClient,
    signer: &Signer,
    root: &Control,
    node: &Node,
    body: Vec<u8>,
    response_cap: usize,
) -> Result<Vec<u8>> {
    exchange_class(client, signer, root, node, body, response_cap, None).await
}
pub(super) async fn exchange_class(
    client: &HttpClient,
    signer: &Signer,
    root: &Control,
    node: &Node,
    body: Vec<u8>,
    response_cap: usize,
    class: Option<&str>,
) -> Result<Vec<u8>> {
    let (bytes, ()) = exchange_accounted(
        client,
        signer,
        root,
        node,
        body,
        response_cap,
        class,
        |_| Ok(()),
    )
    .await?;
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn exchange_accounted<T>(
    client: &HttpClient,
    signer: &Signer,
    root: &Control,
    node: &Node,
    body: Vec<u8>,
    response_cap: usize,
    class: Option<&str>,
    reserve_reply: impl FnOnce(usize) -> Result<T>,
) -> Result<(Vec<u8>, T)> {
    if body.len() > MAX_BYTES {
        return Err(http::failure(ErrorCode::LimitExceeded));
    }
    let nonce = nabedra_rt::ids::uuid_v4();
    let mut c = claims(signer, root, node, nonce, false, &body)?;
    if let Some(class) = class {
        c.class = class.into();
    }
    if command_class(&c.command)? != c.class && !(c.command == "install" && c.class == "bootstrap")
    {
        return Err(invalid());
    }
    let (a, s) = signed(signer, &c)?;
    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "{}{}",
            node.endpoint
                .as_ref()
                .ok_or_else(unavailable)?
                .trim_end_matches('/'),
            if c.command.starts_with("stream_") {
                super::stream::PATH
            } else {
                PATH
            }
        ))
        .header("connection", "close")
        .header("content-type", "application/json")
        .header("content-length", body.len())
        .header("x-nabedra-queue-command", c.command.as_str())
        .header("x-nabedra-queue-class", c.class.as_str())
        .header("x-nabedra-queue-auth", a)
        .header("x-nabedra-queue-signature", s)
        .body(Body::from(body))
        .map_err(|_| invalid())?;
    let end = nabedra_rt::time::now() + Duration::from_secs(30);
    let response = client.request(request, end).await?;
    if response.status() != 200 {
        // Do not turn a denial or malformed peer request into a retryable
        // observation failure. Unsigned errors carry no success authority.
        let code = match response.status().as_u16() {
            401 | 403 => ErrorCode::Unauthorized,
            404 => ErrorCode::NotFound,
            409 => ErrorCode::Conflict,
            413 => ErrorCode::LimitExceeded,
            429 => ErrorCode::Overloaded,
            500 => ErrorCode::InternalError,
            502 | 503 | 504 => ErrorCode::StorageUnavailable,
            _ => ErrorCode::ValidationError,
        };
        return Err(http::failure(code));
    }
    let headers = response.headers().clone();
    let len = http::framing(&headers, response_cap.min(MAX_BYTES), false)?.ok_or_else(invalid)?;
    let reservation = reserve_reply(len)?;
    let bytes = http::read_small(response.into_body(), len, end).await?;
    let raw = http::one(&headers, "x-nabedra-queue-auth")?;
    if raw.len() > 8192 || raw.len() % 2 != 0 {
        return Err(invalid());
    }
    let decoded = (0..raw.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&raw[i..i + 2], 16).map_err(|_| invalid()))
        .collect::<Result<Vec<_>>>()?;
    let expected_command = c.command;
    let expected_class = c.class;
    let c: Claims = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    let session = node.session.as_ref().ok_or_else(unavailable)?;
    if c.command != expected_command || c.class != expected_class {
        return Err(invalid());
    }
    if c.version != 1
        || c.cluster != root.cluster_id
        || c.scheduler_term != root.scheduler.scheduler_term
        || c.membership_revision > root.revision
        || !c.response
        || c.nonce != nonce
        || c.sender_node != Some(node.node_id)
        || c.sender_incarnation != node.incarnation
        || c.sender_session != Some(session.id)
        || c.digest != http::digest(&bytes)
    {
        return Err(invalid());
    }
    let local = signer.sender();
    if c.receiver != local.node_id().unwrap_or(local.process_id())
        || c.receiver_incarnation != local.incarnation().unwrap_or(local.process_id())
        || c.receiver_session != local.session_id().unwrap_or(local.process_id())
        || c.sender_process != node.incarnation.ok_or_else(unavailable)?
    {
        return Err(invalid());
    }
    let key = unhex::<32>(&c.public_key)?;
    let signature = unhex::<64>(http::one(&headers, "x-nabedra-queue-signature")?)?;
    let sender = auth::Sender::worker(
        node.node_id,
        node.incarnation.ok_or_else(unavailable)?,
        session.id,
        c.public_key,
    )
    .map_err(|_| invalid())?;
    auth::checked_worker(root, &sender).map_err(|_| invalid())?;
    let mut input = b"nabedra.queue-control.v1\0".to_vec();
    input.extend_from_slice(&decoded);
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&input, &signature)
        .map_err(|_| invalid())?;
    Ok((bytes, reservation))
}
/// Echo receiver identity for API replies without exposing a reusable credential.
pub(super) fn reply(
    signer: &Signer,
    root: &Control,
    headers: &HeaderMap,
    nonce: Uuid,
    bytes: Vec<u8>,
) -> Result<Response<Body>> {
    let len = bytes.len();
    let digest = http::digest(&bytes);
    signed_reply(
        signer,
        root,
        headers,
        nonce,
        len,
        digest,
        "application/json",
        Body::from(bytes),
    )
}

pub(super) fn stream_reply(
    signer: &Signer,
    root: &Control,
    headers: &HeaderMap,
    nonce: Uuid,
    marker: u8,
    bytes: Bytes,
) -> Result<Response<Body>> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update([marker]);
    digest.update(&bytes);
    let len = bytes.len().checked_add(1).ok_or_else(invalid)?;
    let prefix = match marker {
        0 => Bytes::from_static(&[0]),
        1 => Bytes::from_static(&[1]),
        2 => Bytes::from_static(&[2]),
        _ => return Err(invalid()),
    };
    let body = http_body_util::StreamBody::new(futures_util::stream::iter([
        Ok::<_, std::convert::Infallible>(hyper::body::Frame::data(prefix)),
        Ok(hyper::body::Frame::data(bytes)),
    ]));
    signed_reply(
        signer,
        root,
        headers,
        nonce,
        len,
        http::hex(&digest.finalize()),
        "application/octet-stream",
        Body::new(body),
    )
}

#[allow(clippy::too_many_arguments)]
fn signed_reply(
    signer: &Signer,
    root: &Control,
    headers: &HeaderMap,
    nonce: Uuid,
    len: usize,
    digest: String,
    media_type: &'static str,
    body: Body,
) -> Result<Response<Body>> {
    let raw = http::one(headers, "x-nabedra-queue-auth")?;
    let decoded = (0..raw.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&raw[i..i + 2], 16).map_err(|_| invalid()))
        .collect::<Result<Vec<_>>>()?;
    let request: Claims = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    let sender = signer.sender();
    let c = Claims {
        version: 1,
        command: request.command,
        class: request.class,
        cluster: root.cluster_id,
        scheduler_term: root.scheduler.scheduler_term,
        membership_revision: request.membership_revision,
        receiver: request.sender_node.unwrap_or(request.sender_process),
        receiver_incarnation: request.sender_incarnation.unwrap_or(request.sender_process),
        receiver_session: request.sender_session.unwrap_or(request.sender_process),
        sender_node: sender.node_id(),
        sender_process: sender.process_id(),
        sender_incarnation: sender.incarnation(),
        sender_session: sender.session_id(),
        public_key: http::hex(&signer.public_key()),
        nonce,
        response: true,
        expires_at_ms: super::queue_runtime::now_ms().saturating_add(30000),
        digest,
    };
    let (a, s) = signed(signer, &c)?;
    Response::builder()
        .status(200)
        .header("content-type", media_type)
        .header("content-length", len)
        .header("x-nabedra-queue-auth", a)
        .header("x-nabedra-queue-signature", s)
        .body(body)
        .map_err(|_| invalid())
}

/// Count without allocating a wire buffer; reserve the exact encoded output next.
pub(super) fn encoded_len(value: &impl Serialize, cap: usize) -> Result<usize> {
    struct Counter {
        len: usize,
        cap: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.len = self
                .len
                .checked_add(bytes.len())
                .filter(|len| *len <= self.cap)
                .ok_or_else(|| std::io::Error::other("queue wire limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { len: 0, cap };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| http::failure(ErrorCode::LimitExceeded))?;
    Ok(counter.len)
}
pub(super) fn encode_exact(value: &impl Serialize, len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| http::failure(ErrorCode::LimitExceeded))?;
    serde_json::to_writer(&mut bytes, value).map_err(|_| invalid())?;
    if bytes.len() != len {
        return Err(invalid());
    }
    Ok(bytes)
}

#[cfg(test)]
mod allocation_tests {
    use super::*;
    #[test]
    fn u02_exact_wire_charge_counts_empty_and_non_ascii_payloads_before_allocation() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"data":[0,255,17],"text":"é\nquoted\""}),
        ] {
            let bytes = serde_json::to_vec(&value).unwrap();
            assert_eq!(encoded_len(&value, bytes.len()).unwrap(), bytes.len());
            assert_eq!(encode_exact(&value, bytes.len()).unwrap(), bytes);
            assert_eq!(
                encoded_len(&value, bytes.len() - 1).unwrap_err().code,
                ErrorCode::LimitExceeded
            );
        }
    }
    #[test]
    fn c07_tiny_install_and_lookup_share_control_memory_concurrently() {
        use crate::{config::WorkerResources, scheduling::ResourceScheduler};
        let scheduler = ResourceScheduler::new(vec![(
            0,
            WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 256 << 20,
                task_memory_limit_bytes: 255 << 20,
                control_memory_reserve_bytes: 1 << 20,
            },
        )])
        .unwrap();
        let actor = scheduler.charge_memory(0, 16 << 20).unwrap();
        let install = serde_json::json!({"command":"install","body":vec![255u8;1024]});
        let install_bytes = encoded_len(&install, MAX_BYTES).unwrap();
        let lookup =
            serde_json::json!({"kind":"receipt","state":"succeeded","result":{"name":"items"}});
        let lookup_bytes = encoded_len(&lookup, MAX_BYTES).unwrap();
        let entered = std::sync::Barrier::new(3);
        let release = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let _charge = scheduler
                    .charge_memory(0, (2 * install_bytes + 2 * 1024 + 128 * 1024) as u64)
                    .unwrap();
                entered.wait();
                release.wait();
            });
            scope.spawn(|| {
                let _charge = scheduler
                    .charge_memory(0, (64 * 1024 + lookup_bytes + 4096) as u64)
                    .unwrap();
                entered.wait();
                release.wait();
            });
            entered.wait();
            let (_, threads, bytes) = scheduler.usage()[0];
            assert_eq!(threads, 0);
            assert!(bytes > 16 << 20 && bytes < 17 << 20);
            release.wait();
        });
        assert_eq!(scheduler.usage()[0].2, 16 << 20);
        drop(actor);
        assert_eq!(scheduler.usage()[0].2, 0);
    }
}

#[cfg(test)]
mod observation_http_tests {
    use super::*;
    use axum::Router;
    use serde_json::json;
    #[tokio::test]
    async fn u02_observation_http_errors_preserve_denial_and_malformed_classification() {
        for (status, expected) in [
            (400, ErrorCode::ValidationError),
            (401, ErrorCode::Unauthorized),
            (403, ErrorCode::Unauthorized),
            (404, ErrorCode::NotFound),
            (409, ErrorCode::Conflict),
            (413, ErrorCode::LimitExceeded),
            (429, ErrorCode::Overloaded),
            (500, ErrorCode::InternalError),
            (502, ErrorCode::StorageUnavailable),
            (503, ErrorCode::StorageUnavailable),
            (504, ErrorCode::StorageUnavailable),
            (200, ErrorCode::ValidationError),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let id = |n| Uuid::from_u128(n);
            let template = json!({"api_urls":["http://127.0.0.1:1"],"listen":"127.0.0.1:0","storage":{"driver":"http","options":{"base_url":"http://127.0.0.1:1","namespace":"fixture/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":1,"partition_slots":1,"queue_size":4,"partition_size":"4 KiB"});
            let typed: crate::cluster::configuration::Template =
                serde_json::from_value(template.clone()).unwrap();
            let root:Control=serde_json::from_value(json!({"version":1,"cluster_id":id(1),"name":"Fixture","config":{"generation":2,"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":typed.fingerprint().unwrap(),"lifecycle_fingerprint":"c".repeat(64),"worker_semantic_fingerprint":"d".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":crate::cluster::configuration::Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"coordinator_node":id(3),"scheduler_term":1,"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":endpoint,"operation_slots":1,"partition_slots":1,"queue_size":4,"registration":"verified","session":{"id":id(5),"verifier":"a".repeat(64)},"drain":null}],"intents":[]})).unwrap();
            let app = Router::new().fallback(move || async move {
                Response::builder()
                    .status(status)
                    .header("content-length", "2")
                    .body(Body::from("{}"))
                    .unwrap()
            });
            let server =
                nabedra_rt::task::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = HttpClient::new(None).unwrap();
            let signer = Signer::api(id(1), id(2), &"A".repeat(43)).unwrap();
            let error = exchange(
                &client,
                &signer,
                &root,
                &root.nodes[0],
                br#"{"command":"stats"}"#.to_vec(),
            )
            .await
            .unwrap_err();
            server.abort();
            let _ = server.await;
            assert_eq!(error.code, expected, "HTTP {status}");
        }
    }
}
