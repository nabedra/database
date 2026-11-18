//! Typed consensus RPCs share the worker's authenticated listener. The bounded
//! header routes a permanent local replica node ID inside the logical group.
//! Current process enrollment remains a transport concern, not a Raft node ID.
use super::{
    authority::consensus::Applied,
    entry::{LogId, OwnedRaftEntry, Vote},
    hosted::{HostedState, TypedMachine, TypedSnapshot},
    multiplex::{
        AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture, Purpose,
        RAFT_PROGRESS_DECODE_BYTES, RAFT_PROGRESS_HEADER_BYTES, ReceivePlan, ReplicaLease,
        RequestError, TrafficClass, WorkerClient,
    },
    typed_log::TypedConfig,
    types::{GroupKind, ReplicaMember},
};
use crate::{
    oltp::pool::{ChargedBytes, ExecutionClass, WorkerSharedResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use openraft::{
    Raft,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse, VoteRequest,
        VoteResponse,
    },
    type_config::{alias::SnapshotOf, async_runtime::watch::WatchReceiver},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
mod snapshot;
#[cfg(test)]
#[path = "typed_network/tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Method {
    Vote,
    Append,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    from_node: u64,
    to_node: u64,
    method: Method,
    payload_bytes: u64,
    leadership_transfer_from: Option<Vote>,
    vote: Vote,
    prev_log_id: Option<LogId>,
    leader_commit: Option<LogId>,
}
impl Header {
    fn route(self, kind: &GroupKind) -> (Purpose, TrafficClass) {
        let traffic = class(kind);
        if traffic == TrafficClass::DataConsensus && self.payload_bytes == 0 {
            (Purpose::RaftProgress, TrafficClass::RaftProgress)
        } else {
            (Purpose::Raft, traffic)
        }
    }
}
fn header(bytes: &[u8]) -> Result<Header, ProtocolError> {
    crate::protocol::json_input(bytes, 1024)
        .map_err(|_| invalid("invalid typed Raft header bounds"))?;
    serde_json::from_slice(bytes).map_err(|_| invalid("invalid typed Raft header"))
}

#[derive(Clone)]
pub(crate) struct TypedNetworkFactory<C: TypedConfig> {
    client: WorkerClient,
    local: ReplicaMember,
    resident: ReplicaLease,
    shared: Arc<WorkerSharedResources>,
    _config: std::marker::PhantomData<C>,
}
impl<C: TypedConfig> TypedNetworkFactory<C> {
    pub(crate) fn new(
        client: WorkerClient,
        local: ReplicaMember,
        resident: ReplicaLease,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        if local.node_id != resident.node_id()
            || Some(local.worker_id) != shared.worker_id()
            || !resident.same_resources(&shared)
        {
            return Err(invalid(
                "typed Raft factory identity differs from physical worker",
            ));
        }
        Ok(Self {
            client,
            local,
            resident,
            shared,
            _config: std::marker::PhantomData,
        })
    }
    fn class(&self) -> TrafficClass {
        class(&self.resident.identity().kind)
    }
}
pub(crate) struct TypedNetwork<C: TypedConfig> {
    factory: TypedNetworkFactory<C>,
    target: u64,
    node: ReplicaMember,
}
impl<C: TypedConfig> RaftNetworkFactory<C> for TypedNetworkFactory<C> {
    type Network = TypedNetwork<C>;
    async fn new_client(&mut self, target: u64, node: &ReplicaMember) -> Self::Network {
        TypedNetwork {
            factory: self.clone(),
            target,
            node: node.clone(),
        }
    }
}
impl<C: TypedConfig> TypedNetwork<C> {
    async fn rpc<R: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        header: Header,
        payload: Option<ChargedBytes>,
        ttl: Duration,
    ) -> Result<R, ProtocolError> {
        if self.node.node_id != self.target {
            return Err(invalid("target node disagrees with membership"));
        }
        let group = self.factory.resident.identity();
        let (purpose, traffic) = header.route(&group.kind);
        let shared = self.factory.shared.clone();
        let byte_class = traffic.byte_class();
        let report = |phase: &'static str, failure: &'static str, code: Option<ErrorCode>| {
            tracing::debug!(group=%group.group_id, from_node=header.from_node, to_node=header.to_node,
                method=?header.method, ?purpose, phase, failure, ?code,
                prev_index=header.prev_log_id.map(|id| id.index),
                commit_index=header.leader_commit.map(|id| id.index), "typed Raft RPC failed");
        };
        let encoded = shared
            .encode_in(byte_class, &header, RAFT_PROGRESS_HEADER_BYTES)
            .inspect_err(|error| report("encode_header", "local_protocol", Some(error.code)))?;
        let (response_decode, response_header) = if traffic == TrafficClass::RaftProgress {
            (RAFT_PROGRESS_DECODE_BYTES, RAFT_PROGRESS_HEADER_BYTES)
        } else {
            (128 << 10, 16 << 10)
        };
        let reply = tokio::time::timeout(
            ttl,
            self.factory.client.request_classified(
                group,
                self.node.worker_id,
                purpose,
                encoded,
                [payload, None, None],
                response_decode,
            ),
        )
        .await
        .map_err(|_| {
            report("exchange", "rpc_deadline", None);
            unavailable()
        })?
        .map_err(|failure| {
            let (reason, code) = match &failure {
                RequestError::OutgoingLaneFull => ("outgoing_lane_full", None),
                RequestError::Unreachable => ("unreachable", None),
                RequestError::PeerRejected { error, .. } => ("peer_rejected", Some(error.code)),
                RequestError::Protocol(error) => ("local_protocol", Some(error.code)),
            };
            report("exchange", reason, code);
            ProtocolError::from(failure)
        })?;
        // The two private return types contain only fixed vote/log metadata.
        // Move the entire transport owner into the native codec until it settles.
        let decoded = shared
            .execute(execution(traffic), self.target, 0, move || {
                crate::protocol::json_input(reply.header.as_slice(), response_header)
                    .map_err(|_| invalid("typed Raft reply bounds"))?;
                serde_json::from_slice(reply.header.as_slice())
                    .map_err(|_| invalid("invalid typed Raft reply"))
            })
            .await
            .inspect_err(|error| report("decode_reply", "local_protocol", Some(error.code)))?;
        if traffic == TrafficClass::RaftProgress {
            tracing::debug!(group=%group.group_id, from_node=header.from_node, to_node=header.to_node,
                method=?header.method, ?purpose, prev_index=header.prev_log_id.map(|id| id.index),
                commit_index=header.leader_commit.map(|id| id.index), "typed Raft reply decoded");
        }
        Ok(decoded)
    }
}
impl<C: TypedConfig> RaftNetworkV2<C> for TypedNetwork<C> {
    type SnapshotData = TypedSnapshot;
    async fn append_entries(
        &mut self,
        mut rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendResult<C>, RPCError<C>> {
        // Every producer path uses one entry per RPC. Translate success against
        // the sent prefix, never the unsent original request tail.
        rpc.entries.truncate(1);
        let previous = rpc.prev_log_id;
        let last = rpc.last_log_id();
        let payload = if let Some(entry) = rpc.entries.pop() {
            let shared = self.factory.shared.clone();
            let class = self.factory.class();
            let entry = entry
                .charge_resident(&self.factory.resident)
                .map_err(|_| unreachable())?;
            Some(
                shared
                    .execute(execution(class), self.target, 0, {
                        let shared = shared.clone();
                        move || {
                            shared.encode_in(
                                class.byte_class(),
                                &entry,
                                crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
                            )
                        }
                    })
                    .await
                    .map_err(|_| unreachable())?,
            )
        } else {
            None
        };
        let header = Header {
            from_node: self.factory.local.node_id,
            to_node: self.target,
            method: Method::Append,
            leadership_transfer_from: None,
            payload_bytes: payload.as_ref().map_or(0, |p| p.len() as u64),
            vote: rpc.vote,
            prev_log_id: previous,
            leader_commit: rpc.leader_commit,
        };
        let reply: AppendEntriesResponse<C> = self
            .rpc(header, payload, option.soft_ttl())
            .await
            .map_err(|_| unreachable())?;
        if (previous.is_none() && matches!(reply, AppendEntriesResponse::Conflict))
            || (header.payload_bytes == 0
                && matches!(reply, AppendEntriesResponse::PartialSuccess(_)))
        {
            tracing::debug!(group=%self.factory.resident.identity().group_id, to_node=self.target,
                prev_index=previous.map(|id| id.index), "typed Raft reply disagrees with append prefix");
            return Err(unreachable());
        }
        Ok(reply.into_append_result(previous, last))
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        self.rpc(
            Header {
                from_node: self.factory.local.node_id,
                to_node: self.target,
                method: Method::Vote,
                payload_bytes: 0,
                leadership_transfer_from: rpc.leadership_transfer_from,
                vote: rpc.vote,
                prev_log_id: rpc.last_log_id,
                leader_commit: None,
            },
            None,
            option.soft_ttl(),
        )
        .await
        .map_err(|_| unreachable())
    }
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: SnapshotOf<C, TypedSnapshot>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        self.stream_snapshot(vote, snapshot, cancel, option).await
    }
}

/// A group endpoint dispatches permanent node IDs, allowing an old voter and a
/// replacement learner on different pools of one worker to coexist safely.
pub(crate) struct ReplicaDispatcher {
    nodes: Mutex<BTreeMap<u64, Arc<DispatchRoute>>>,
}
struct DispatchRoute {
    endpoint: Arc<dyn Endpoint>,
    closed: std::sync::atomic::AtomicBool,
    admitted: tokio::sync::RwLock<()>,
}
impl Endpoint for DispatchRoute {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let _admitted = self.admitted.read().await;
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(unavailable());
            }
            self.endpoint.preflight(session, bytes).await
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let _admitted = self.admitted.read().await;
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(unavailable());
            }
            self.endpoint.handle(session, bytes, sections).await
        })
    }
}
impl ReplicaDispatcher {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Mutex::new(BTreeMap::new()),
        }
    }
    pub(crate) fn add(&self, node: u64, endpoint: Arc<dyn Endpoint>) -> Result<(), ProtocolError> {
        let mut nodes = self.nodes.lock().map_err(|_| unavailable())?;
        if node == 0 || nodes.contains_key(&node) || nodes.len() >= 128 {
            return Err(invalid("invalid or duplicate hosted replica route"));
        }
        nodes.insert(
            node,
            Arc::new(DispatchRoute {
                endpoint,
                closed: std::sync::atomic::AtomicBool::new(false),
                admitted: tokio::sync::RwLock::new(()),
            }),
        );
        Ok(())
    }
    fn get(&self, node: u64) -> Result<Arc<DispatchRoute>, ProtocolError> {
        self.nodes
            .lock()
            .map_err(|_| unavailable())?
            .get(&node)
            .cloned()
            .ok_or_else(|| invalid("worker does not host target replica node"))
    }
    /// Remove this permanent node route and join requests already admitted by
    /// it. A retained preflight handle cannot reopen it after removal.
    pub(in crate::oltp_replication) async fn retire(&self, node: u64) -> Result<(), ProtocolError> {
        let route = self.nodes.lock().map_err(|_| unavailable())?.remove(&node);
        if let Some(route) = route {
            route
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            let _joined = route.admitted.write().await;
        }
        Ok(())
    }
}
impl Endpoint for ReplicaDispatcher {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.get(snapshot::target_node(bytes)?)?
                .preflight(session, bytes)
                .await
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            self.get(snapshot::target_node(bytes.as_slice())?)?
                .handle(session, bytes, sections)
                .await
        })
    }
}

pub(crate) struct ReplicaEndpoint<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    raft: Raft<C, HostedState<C, M>>,
    state: HostedState<C, M>,
    incoming: Arc<tokio::sync::Mutex<Option<snapshot::Incoming>>>,
    resident: ReplicaLease,
    shared: Arc<WorkerSharedResources>,
    bootstrap: Arc<BTreeMap<u64, ReplicaMember>>,
}
impl<C, M> ReplicaEndpoint<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(crate) fn new(
        raft: Raft<C, HostedState<C, M>>,
        state: HostedState<C, M>,
        resident: ReplicaLease,
        shared: Arc<WorkerSharedResources>,
        bootstrap: BTreeMap<u64, ReplicaMember>,
    ) -> Result<Self, ProtocolError> {
        if bootstrap.len() > 128 || !resident.same_resources(&shared) {
            return Err(invalid("invalid typed bootstrap route"));
        }
        Ok(Self {
            raft,
            state,
            incoming: Arc::new(tokio::sync::Mutex::new(None)),
            resident,
            shared,
            bootstrap: Arc::new(bootstrap),
        })
    }
    fn verify(
        &self,
        session: &AuthenticatedSession,
        head: Header,
    ) -> Result<TrafficClass, ProtocolError> {
        let (purpose, traffic) = head.route(&self.resident.identity().kind);
        if session.purpose() != purpose {
            return Err(invalid("typed RPC purpose mismatch"));
        }
        if head.vote.leader_id().node_id != head.from_node {
            return Err(invalid(
                "Raft vote or append sender differs from TLS replica",
            ));
        }
        match head.method {
            Method::Vote if head.payload_bytes != 0 || head.leader_commit.is_some() => {
                return Err(invalid("invalid vote frame fields"));
            }
            Method::Append if head.leadership_transfer_from.is_some() => {
                return Err(invalid("invalid append frame fields"));
            }
            _ => {}
        }
        self.verify_sender(session, head.from_node, head.to_node)?;
        Ok(traffic)
    }
    fn verify_sender(
        &self,
        session: &AuthenticatedSession,
        from_node: u64,
        to_node: u64,
    ) -> Result<(), ProtocolError> {
        if session.group() != self.resident.identity() || to_node != self.resident.node_id() {
            return Err(invalid("typed RPC target mismatch"));
        }
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let membership = metrics.membership_config.membership();
        let member = membership
            .nodes()
            .find(|(node, _)| **node == from_node)
            .map(|(_, member)| member)
            .or_else(|| {
                if membership.nodes().next().is_none() {
                    self.bootstrap.get(&from_node)
                } else {
                    None
                }
            })
            .ok_or_else(|| invalid("sender is outside enrolled replica membership"))?;
        if member.worker_id != session.remote().worker_id {
            return Err(invalid("sender node is bound to another physical worker"));
        }
        Ok(())
    }
}
impl<C, M> Endpoint for ReplicaEndpoint<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            if session.purpose() == Purpose::Snapshot {
                return self.snapshot_preflight(session, bytes);
            }
            let head = header(bytes)?;
            let traffic = self.verify(session, head)?;
            Ok(ReceivePlan {
                class: traffic,
                section_lengths: [head.payload_bytes, 0, 0],
                decode_budget_bytes: if traffic == TrafficClass::RaftProgress {
                    RAFT_PROGRESS_DECODE_BYTES
                } else {
                    0
                },
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            if session.purpose() == Purpose::Snapshot {
                return self.snapshot_handle(session, bytes, sections).await;
            }
            let head = header(bytes.as_slice())?;
            let traffic = self.verify(&session, head)?;
            let response_limit = if traffic == TrafficClass::RaftProgress {
                RAFT_PROGRESS_HEADER_BYTES
            } else {
                16 << 10
            };
            let observe = |phase: &'static str, decision: &'static str| {
                tracing::debug!(group=%self.resident.identity().group_id, from_node=head.from_node,
                    to_node=head.to_node, method=?head.method, purpose=?session.purpose(), phase,
                    decision, prev_index=head.prev_log_id.map(|id| id.index),
                    commit_index=head.leader_commit.map(|id| id.index), "typed Raft RPC");
            };
            let shared = self.shared.clone();
            observe("received", "pending");
            let header = match head.method {
                Method::Append => {
                    let entry = if head.payload_bytes == 0 {
                        None
                    } else {
                        Some(
                            shared
                                .execute(execution(traffic), head.from_node, 0, {
                                    let shared = shared.clone();
                                    move || {
                                        let input = sections
                                            .section(0)
                                            .ok_or_else(|| invalid("missing append payload"))?;
                                        OwnedRaftEntry::<C::Command>::decode(
                                            input,
                                            &shared,
                                            traffic.byte_class(),
                                            usize::MAX,
                                        )
                                    }
                                })
                                .await?,
                        )
                    };
                    let rpc = AppendEntriesRequest::<C> {
                        vote: head.vote,
                        prev_log_id: head.prev_log_id,
                        entries: entry.into_iter().collect(),
                        leader_commit: head.leader_commit,
                    };
                    let reply = self.raft.append_entries(rpc).await.map_err(|_| {
                        observe("raft_handled", "raft_unavailable");
                        unavailable()
                    })?;
                    let reply = AppendEntriesResponse::<C>::from(reply);
                    let decision = match reply {
                        AppendEntriesResponse::Success => "accepted",
                        AppendEntriesResponse::PartialSuccess(_) => "partial",
                        AppendEntriesResponse::Conflict => "conflict",
                        AppendEntriesResponse::HigherVote(_) => "higher_vote",
                    };
                    observe("raft_handled", decision);
                    shared.encode_in(traffic.byte_class(), &reply, response_limit)?
                }
                Method::Vote => {
                    let reply = self
                        .raft
                        .vote(VoteRequest::<C> {
                            vote: head.vote,
                            last_log_id: head.prev_log_id,
                            leadership_transfer_from: head.leadership_transfer_from,
                        })
                        .await
                        .map_err(|_| {
                            observe("raft_handled", "raft_unavailable");
                            unavailable()
                        })?;
                    observe(
                        "raft_handled",
                        if reply.vote_granted {
                            "granted"
                        } else {
                            "declined"
                        },
                    );
                    shared.encode_in(traffic.byte_class(), &reply, response_limit)?
                }
            };
            Ok(ChargedReply {
                header,
                sections: [None, None, None],
            })
        })
    }
}
fn class(kind: &GroupKind) -> TrafficClass {
    match kind {
        GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
            TrafficClass::ControlConsensus
        }
        _ => TrafficClass::DataConsensus,
    }
}
fn execution(class: TrafficClass) -> ExecutionClass {
    match class {
        TrafficClass::ControlConsensus | TrafficClass::RaftProgress => ExecutionClass::Control,
        _ => ExecutionClass::Query,
    }
}
fn unreachable<C: TypedConfig>() -> RPCError<C> {
    RPCError::Unreachable(Unreachable::from_string(
        "typed authenticated Raft RPC unavailable",
    ))
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "typed consensus unavailable",
        Outcome::Unknown,
    )
}
