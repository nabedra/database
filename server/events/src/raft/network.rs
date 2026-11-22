use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{future::Future, io};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path as RequestPath, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::TryStreamExt;
use openraft::{
    OptionalSend, RaftNetwork, RaftNetworkFactory, Snapshot, SnapshotMeta, Vote,
    error::{
        Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
    },
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
};
use reqwest::{Certificate, Client, Identity, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
#[cfg_attr(feature = "sim", allow(unused_imports))]
use tokio_util::io::{ReaderStream, StreamReader};
use uuid::Uuid;

#[cfg_attr(feature = "sim", allow(unused_imports))]
use super::snapshot::{CHUNK_BYTES, MAX_SNAPSHOT_BYTES};
use super::{Broker, Group, Node, TypeConfig};

const CLUSTER_HEADER: &str = "x-nabedra-events-cluster";
const TARGET_HEADER: &str = "x-nabedra-events-target";
const RPC_PREFIX: &str = "/internal/raft/v1";
const PEER_REQUEST_BYTES: usize = 5 * 1024 * 1024;
const PEER_RESPONSE_BYTES: usize = 5 * 1024 * 1024;
const SNAPSHOT_FRAME_BYTES: usize = 64 * 1024;
const SNAPSHOT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct PeerTransport {
    client: Client,
    authorization: HeaderValue,
    cluster: HeaderValue,
    allow_plaintext: bool,
    lanes: Arc<Lanes>,
}

#[cfg_attr(feature = "sim", allow(dead_code))]
struct Lanes {
    outgoing_control: Semaphore,
    outgoing_data: Semaphore,
    outgoing_bulk: Semaphore,
    outgoing_application: Semaphore,
    incoming_control: Semaphore,
    incoming_data: Semaphore,
    incoming_bulk: Semaphore,
}

impl PeerTransport {
    pub fn new(
        cluster: Uuid,
        secret: &str,
        certificate_authority: Option<&Path>,
        identity: Option<&Path>,
        allow_plaintext: bool,
    ) -> Result<Self> {
        ensure!(
            secret.len() >= 32
                && secret.len() <= 256
                && secret.bytes().all(|byte| byte.is_ascii_graphic()),
            "peer authentication token must be 32 to 256 printable ASCII characters"
        );
        let mut authorization = HeaderValue::from_str(&format!("Bearer {secret}"))
            .context("invalid peer authentication token")?;
        authorization.set_sensitive(true);
        // reqwest is built without a provider of its own (`rustls-no-provider`): it
        // reads the process default, so make sure that is `ring`. An error means a
        // provider is already installed, which is then the one in use.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .pool_max_idle_per_host(2)
            .connect_timeout(Duration::from_secs(2))
            .https_only(!allow_plaintext);
        if let Some(ca) = certificate_authority {
            let certificates = Certificate::from_pem_bundle(&std::fs::read(ca)?)?;
            ensure!(!certificates.is_empty(), "peer CA has no certificates");
            builder = builder.tls_certs_only(certificates);
        }
        if let Some(identity) = identity {
            builder = builder.identity(Identity::from_pem(&std::fs::read(identity)?)?);
        }
        Ok(Self {
            client: builder.build()?,
            authorization,
            cluster: HeaderValue::from_str(&cluster.to_string())?,
            allow_plaintext,
            lanes: Arc::new(Lanes {
                outgoing_control: Semaphore::new(32),
                outgoing_data: Semaphore::new(8),
                outgoing_bulk: Semaphore::new(2),
                outgoing_application: Semaphore::new(16),
                incoming_control: Semaphore::new(32),
                incoming_data: Semaphore::new(8),
                incoming_bulk: Semaphore::new(2),
            }),
        })
    }

    pub fn authenticated(&self, headers: &HeaderMap) -> bool {
        headers.get(CLUSTER_HEADER) == Some(&self.cluster)
            && headers.get(AUTHORIZATION).is_some_and(|provided| {
                bool::from(provided.as_bytes().ct_eq(self.authorization.as_bytes()))
            })
    }

    pub fn validate_address(&self, address: &str) -> Result<()> {
        self.origin(address).map(|_| ())
    }

    fn origin(&self, address: &str) -> Result<Url> {
        ensure!(address.len() <= 512, "peer origin exceeds its length limit");
        let url = Url::parse(address).context("invalid peer origin")?;
        ensure!(
            (url.scheme() == "https" || self.allow_plaintext && url.scheme() == "http")
                && url.has_host()
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "peer address must be an allowed HTTP(S) origin"
        );
        Ok(url)
    }

    /// Send to a private application route with cluster peer authentication.
    pub async fn post<T: Serialize, R: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        body: &T,
    ) -> Result<R> {
        self.post_with_timeout(base_url, path, body, PEER_RPC_TIMEOUT)
            .await
    }

    pub async fn post_with_timeout<T: Serialize, R: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        body: &T,
        deadline: Duration,
    ) -> Result<R> {
        ensure!(
            path.starts_with("/internal/"),
            "peer protected route required"
        );
        tokio::time::timeout(deadline, async {
            let _permit = self.lanes.outgoing_application.acquire().await?;
            self.exchange(base_url, path, body, None).await
        })
        .await
        .context("peer application RPC timed out")?
    }

    async fn exchange<T: Serialize, R: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        body: &T,
        target: Option<u64>,
    ) -> Result<R> {
        let data = serde_json::to_vec(body)?;
        ensure!(
            data.len() <= PEER_REQUEST_BYTES,
            "peer request exceeds the byte limit"
        );
        let request = self
            .request(base_url, path, target)?
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(data);
        self.read_response(request).await
    }

    fn request(
        &self,
        base_url: &str,
        path: &str,
        target: Option<u64>,
    ) -> Result<reqwest::RequestBuilder> {
        ensure!(
            path.starts_with('/') && !path.starts_with("//"),
            "invalid private route"
        );
        let mut request = self
            .client
            .post(self.origin(base_url)?.join(path)?)
            .header(AUTHORIZATION, self.authorization.clone())
            .header(CLUSTER_HEADER, self.cluster.clone());
        if let Some(target) = target {
            request = request.header(TARGET_HEADER, target.to_string());
        }
        Ok(request)
    }

    async fn read_response<R: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<R> {
        let mut response = request.send().await?.error_for_status()?;
        ensure!(
            response
                .content_length()
                .is_none_or(|size| size <= PEER_RESPONSE_BYTES as u64),
            "peer response exceeds the byte limit"
        );
        let mut output = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                output.len() + chunk.len() <= PEER_RESPONSE_BYTES,
                "peer response exceeds the byte limit"
            );
            output.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&output).context("invalid private response")
    }
}

pub(super) fn router(broker: Arc<Broker>) -> Router {
    Router::new()
        .route("/internal/raft/v1/{group}/{rpc}", post(handle))
        .with_state(broker)
}

async fn handle(
    State(broker): State<Arc<Broker>>,
    RequestPath((group_id, rpc)): RequestPath<(String, String)>,
    request: Request,
) -> Response {
    if !broker.peers().authenticated(request.headers()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let target = request
        .headers()
        .get(TARGET_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if target != Some(broker.id()) {
        return StatusCode::MISDIRECTED_REQUEST.into_response();
    }
    let Some(group) = broker.group(&group_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if rpc == "vote" && !group.accepts_votes() {
        return StatusCode::CONFLICT.into_response();
    }
    let lanes = &broker.peers().lanes;
    if rpc == "snapshot" {
        let Ok(_permit) = lanes.incoming_bulk.try_acquire() else {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        };
        return receive_snapshot(&group, request).await;
    }
    let (semaphore, body_limit) = match rpc.as_str() {
        "vote" | "heartbeat" => (&lanes.incoming_control, 16 * 1024),
        "append" => (&lanes.incoming_data, PEER_REQUEST_BYTES),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let Ok(_permit) = semaphore.try_acquire() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let body = match tokio::time::timeout(
        Duration::from_secs(2),
        to_bytes(request.into_body(), body_limit),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    match rpc.as_str() {
        "vote" => match serde_json::from_slice::<VoteRequest<u64>>(&body) {
            Ok(vote) => Json(group.raft.vote(vote).await).into_response(),
            Err(_) => StatusCode::BAD_REQUEST.into_response(),
        },
        "heartbeat" | "append" => {
            match serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(&body) {
                Ok(append) if rpc == "append" || append.entries.is_empty() => {
                    Json(group.raft.append_entries(append).await).into_response()
                }
                _ => StatusCode::BAD_REQUEST.into_response(),
            }
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize, Serialize)]
struct SnapshotFrame {
    vote: Vote<u64>,
    meta: SnapshotMeta<u64, Node>,
}

async fn receive_snapshot(group: &Group, request: Request) -> Response {
    let receive = async {
        let chunks = request
            .into_body()
            .into_data_stream()
            .map_err(io::Error::other);
        let mut input = StreamReader::new(chunks);
        let header_bytes = input.read_u32().await? as usize;
        ensure!(
            (1..=SNAPSHOT_FRAME_BYTES).contains(&header_bytes),
            "invalid snapshot frame size"
        );
        let mut header = vec![0; header_bytes];
        input.read_exact(&mut header).await?;
        let frame = serde_json::from_slice::<SnapshotFrame>(&header)?;
        let mut input = input.take(MAX_SNAPSHOT_BYTES + 1);
        let mut file = group.snapshots.receive().await?;
        let size = tokio::io::copy(&mut input, &mut file).await?;
        ensure!(
            (1..=MAX_SNAPSHOT_BYTES).contains(&size),
            "invalid snapshot file size"
        );
        file.flush().await?;
        Ok::<_, anyhow::Error>((frame, file))
    };
    let (frame, file) = match tokio::time::timeout(SNAPSHOT_TRANSFER_TIMEOUT, receive).await {
        Ok(Ok(received)) => received,
        Ok(Err(error)) => {
            tracing::warn!(group = group.id(), %error, "snapshot transfer failed");
            return StatusCode::BAD_REQUEST.into_response();
        }
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    Json(
        group
            .raft
            .install_full_snapshot(
                frame.vote,
                Snapshot {
                    meta: frame.meta,
                    snapshot: Box::new(file),
                },
            )
            .await,
    )
    .into_response()
}

pub(super) struct NetworkFactory {
    pub peers: PeerTransport,
    pub group: Arc<str>,
}

#[cfg_attr(feature = "sim", allow(dead_code))]
pub(super) struct Network {
    peers: PeerTransport,
    group: Arc<str>,
    node: Node,
    target: u64,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: u64, node: &Node) -> Self::Network {
        Network {
            peers: self.peers.clone(),
            group: self.group.clone(),
            node: node.clone(),
            target,
        }
    }
}

#[cfg_attr(feature = "sim", allow(dead_code))]
impl Network {
    async fn call<T, R, E>(
        &self,
        rpc: &str,
        body: T,
        lane: &Semaphore,
        option: RPCOption,
    ) -> Result<R, RPCError<u64, Node, RaftError<u64, E>>>
    where
        T: Serialize,
        R: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let path = format!("{RPC_PREFIX}/{}/{rpc}", self.group);
        let call = async {
            let _permit = lane.acquire().await?;
            self.peers
                .exchange::<_, Result<R, RaftError<u64, E>>>(
                    &self.node.addr,
                    &path,
                    &body,
                    Some(self.target),
                )
                .await
        };
        let result = tokio::time::timeout(option.hard_ttl(), call)
            .await
            .map_err(|error| Unreachable::new(&error))?
            .map_err(|error| Unreachable::new(&io::Error::other(error.to_string())))?;
        result.map_err(|error| RemoteError::new(self.target, error).into())
    }
}

#[cfg(not(feature = "sim"))]
impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        append: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
        let lanes = &self.peers.lanes;
        let (rpc, lane) = if append.entries.is_empty() {
            ("heartbeat", &lanes.outgoing_control)
        } else {
            ("append", &lanes.outgoing_data)
        };
        self.call(rpc, append, lane, option).await
    }

    async fn vote(
        &mut self,
        vote: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
        self.call("vote", vote, &self.peers.lanes.outgoing_control, option)
            .await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        let path = format!("{RPC_PREFIX}/{}/snapshot", self.group);
        let send = async {
            let _permit = self.peers.lanes.outgoing_bulk.try_acquire()?;
            let header = serde_json::to_vec(&SnapshotFrame {
                vote,
                meta: snapshot.meta,
            })?;
            ensure!(
                (1..=SNAPSHOT_FRAME_BYTES).contains(&header.len()),
                "invalid snapshot frame size"
            );
            let mut prefix = Vec::with_capacity(4 + header.len());
            prefix.extend_from_slice(&(header.len() as u32).to_be_bytes());
            prefix.extend_from_slice(&header);
            let data = io::Cursor::new(prefix).chain(*snapshot.snapshot);
            let stream = ReaderStream::with_capacity(data, CHUNK_BYTES);
            let request = self
                .peers
                .request(&self.node.addr, &path, Some(self.target))?
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(reqwest::Body::wrap_stream(stream));
            self.peers
                .read_response::<Result<SnapshotResponse<u64>, Fatal<u64>>>(request)
                .await
        };
        let result = tokio::select! {
            closed = cancel => return Err(closed.into()),
            result = tokio::time::timeout(option.hard_ttl(), send) => result,
        }
        .map_err(|error| Unreachable::new(&error))?
        .map_err(|error| Unreachable::new(&io::Error::other(error.to_string())))?;
        result.map_err(|error| RemoteError::new(self.target, error).into())
    }
}

/// The simulated peer network: the same receiving checks as [`handle`], carried by the
/// in-memory router in [`crate::sim::net`] instead of reqwest and axum.
#[cfg(feature = "sim")]
impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        append: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
        let rpc = if append.entries.is_empty() {
            "heartbeat"
        } else {
            "append"
        };
        let group = self.group.clone();
        let delivered = crate::sim::net::call(self.target, rpc, option.hard_ttl(), move |broker| {
            let append = append.clone();
            let group = broker.group(&group);
            async move { Some(group?.raft.append_entries(append).await) }
        })
        .await;
        match delivered {
            Some(reply) => reply.map_err(|error| RemoteError::new(self.target, error).into()),
            // Planted bug (simulation only): an unreachable follower counts as having
            // acknowledged the append, so the leader can commit without a majority.
            None if crate::sim::hooks::ack_unreachable_appends() => {
                Ok(AppendEntriesResponse::Success)
            }
            None => Err(sim_unreachable()),
        }
    }

    async fn vote(
        &mut self,
        vote: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
        let group = self.group.clone();
        let delivered =
            crate::sim::net::call(self.target, "vote", option.hard_ttl(), move |broker| {
                let (group, vote) = (broker.group(&group), vote.clone());
                async move {
                    let group = group?;
                    if !group.accepts_votes() {
                        return None;
                    }
                    Some(group.raft.vote(vote).await)
                }
            })
            .await;
        match delivered {
            Some(reply) => reply.map_err(|error| RemoteError::new(self.target, error).into()),
            None => Err(sim_unreachable()),
        }
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<u64>,
        _snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        // Snapshot transfer is not simulated yet; runs stay below the snapshot threshold.
        Err(Unreachable::new(&io::Error::other(
            "simulated snapshot transfer is not supported",
        ))
        .into())
    }
}

#[cfg(feature = "sim")]
fn sim_unreachable<E: std::error::Error>() -> RPCError<u64, Node, E> {
    Unreachable::new(&io::Error::other("simulated peer unreachable")).into()
}
