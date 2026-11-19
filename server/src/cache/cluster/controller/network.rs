use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::sync::Arc;

use openraft::{
    BasicNode,
    error::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendResult, SnapshotResponse, StreamAppendError, VoteRequest,
        VoteResponse,
    },
    type_config::alias::{LogIdOf, VoteOf},
    vote::RaftLeaderId,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use uuid::Uuid;

use super::super::protocol::{CONTROL_FRAME, VERSION};
use super::super::transport::{Endpoint, Fingerprint, PeerConnection, PeerTls, bounded_json};
use super::disk::invalid;
use super::machine::{Saved, Snapshot};
use super::state::State;
use super::{Raft, RaftConfig};

#[derive(Clone)]
pub(super) struct Network {
    pub cluster: Uuid,
    pub node: u64,
    pub voters: Arc<BTreeMap<u64, Endpoint>>,
    pub tls: PeerTls,
}

pub(super) struct Peer {
    network: Network,
    target: u64,
    connection: Option<PeerConnection>,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    cluster: Uuid,
    version: u16,
    from: u64,
    to: u64,
    body: Request,
}

#[derive(Serialize, Deserialize)]
enum Request {
    Append(AppendEntriesRequest<RaftConfig>),
    Vote(VoteRequest<RaftConfig>),
    Snapshot {
        vote: VoteOf<RaftConfig>,
        saved: Saved,
    },
}

#[derive(Serialize, Deserialize)]
enum Response {
    Append(Result<Option<LogIdOf<RaftConfig>>, StreamAppendError<RaftConfig>>),
    Vote(VoteResponse<RaftConfig>),
    Snapshot(SnapshotResponse<RaftConfig>),
}

impl Network {
    pub async fn handle(
        &self,
        raft: Raft,
        fingerprint: Fingerprint,
        bytes: Vec<u8>,
    ) -> io::Result<Vec<u8>> {
        // Authorize the role before allocating any decoded Raft payload.
        let authenticated = self
            .voters
            .iter()
            .find(|(id, voter)| **id != self.node && voter.fingerprints.contains(&fingerprint))
            .map(|(id, _)| *id)
            .ok_or_else(|| invalid("certificate is not a controller voter"))?;
        let message: Envelope = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        drop(bytes);
        if message.version != VERSION
            || message.cluster != self.cluster
            || message.to != self.node
            || message.from != authenticated
        {
            return Err(invalid("Raft envelope identity mismatch"));
        }
        let response = match message.body {
            Request::Append(request) if *request.vote.leader_id().node_id() == authenticated => {
                let result = raft
                    .append_entries(request)
                    .await
                    .map_err(io::Error::other)?;
                Response::Append(result.map_err(Into::into))
            }
            Request::Vote(request) if *request.vote.leader_id().node_id() == authenticated => {
                Response::Vote(raft.vote(request).await.map_err(io::Error::other)?)
            }
            Request::Snapshot { vote, saved } if *vote.leader_id().node_id() == authenticated => {
                Response::Snapshot(
                    raft.install_full_snapshot(vote, saved.into())
                        .await
                        .map_err(io::Error::other)?,
                )
            }
            _ => return Err(invalid("Raft vote identity mismatch")),
        };
        bounded_json(&response, CONTROL_FRAME)
    }
}

impl RaftNetworkFactory<RaftConfig> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Peer {
        Peer {
            network: self.clone(),
            target,
            connection: None,
        }
    }
}

impl Peer {
    async fn send(&mut self, body: Request, option: RPCOption) -> io::Result<Response> {
        let peer = self
            .network
            .voters
            .get(&self.target)
            .ok_or_else(|| invalid("Raft destination is outside configured voters"))?;
        let envelope = Envelope {
            cluster: self.network.cluster,
            version: VERSION,
            from: self.network.node,
            to: self.target,
            body,
        };
        let mut connection = self.connection.take();
        let response = timeout(option.soft_ttl(), async {
            if connection.is_none() {
                connection = Some(self.network.tls.connect(peer).await?);
            }
            connection
                .as_mut()
                .expect("connected peer")
                .json(&envelope, CONTROL_FRAME)
                .await
        })
        .await??;
        self.connection = connection;
        Ok(response)
    }
}

impl RaftNetworkV2<RaftConfig> for Peer {
    type SnapshotData = State;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RaftConfig>,
        option: RPCOption,
    ) -> Result<AppendResult<RaftConfig>, RPCError<RaftConfig>> {
        match self.send(Request::Append(rpc), option).await {
            Ok(Response::Append(result)) => Ok(result.map_err(Into::into)),
            _ => Err(RPCError::Unreachable(Unreachable::from_string(
                "cache Raft append unavailable",
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftConfig>, RPCError<RaftConfig>> {
        match self.send(Request::Vote(rpc), option).await {
            Ok(Response::Vote(result)) => Ok(result),
            _ => Err(RPCError::Unreachable(Unreachable::from_string(
                "cache Raft vote unavailable",
            ))),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<RaftConfig>,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<RaftConfig>, StreamingError<RaftConfig>> {
        let saved = Saved {
            meta: snapshot.meta,
            state: snapshot.snapshot,
        };
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            response = self.send(Request::Snapshot { vote, saved }, option) => match response {
                Ok(Response::Snapshot(result)) => Ok(result),
                _ => Err(StreamingError::Unreachable(Unreachable::from_string("cache Raft snapshot unavailable"))),
            }
        }
    }
}
