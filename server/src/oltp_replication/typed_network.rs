//! Typed consensus RPCs share the worker's authenticated listener. The bounded
//! header routes a permanent local replica node ID inside the logical group.
//! Current process enrollment remains a transport concern, not a Raft node ID.
use super::{
    authority::consensus::Applied,
    entry::{CommittedLeaderId, LogId, Membership, OwnedRaftEntry, Vote},
    hosted::{HostedState, RaftCores, TypedMachine, TypedSnapshot},
    multiplex::{
        AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture, PeerFeature,
        Purpose, RAFT_PROGRESS_DECODE_BYTES, RAFT_PROGRESS_HEADER_BYTES, ReceivePlan, ReplicaLease,
        RequestError, RequestTarget, TrafficClass, WorkerClient,
    },
    typed_log::{MAX_APPEND_BYTES, MAX_APPEND_ENTRIES, TypedConfig},
    types::{GroupKind, ReplicaMember},
};
use crate::{
    oltp::pool::{ChargedBytes, ExecutionClass, WorkerSharedResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use openraft::{
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
    /// Entry count of a multi-entry append (see [`MultiEntry`]). Absent for
    /// zero or one entry, whose frames stay byte-identical to the one-entry
    /// wire that older peers decode. Sent only to peers whose session
    /// negotiated [`PeerFeature::MultiEntryAppend`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entries: Option<u32>,
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
/// Multi-entry append payload: each entry's one-entry encoding, prefixed by
/// its length as a big-endian `u32`, in log order.
struct MultiEntry;
impl MultiEntry {
    /// Framed bytes per append, so one append stays near the log's synced
    /// batch bound. A single larger entry still travels alone.
    const BUDGET: usize = MAX_APPEND_BYTES + 4 * MAX_APPEND_ENTRIES;
    fn encode(
        shared: &WorkerSharedResources,
        class: TrafficClass,
        entries: &[ChargedBytes],
    ) -> Result<ChargedBytes, ProtocolError> {
        let total = entries.iter().map(|entry| 4 + entry.len()).sum();
        let mut out = shared.allocate_bytes(class.byte_class(), total)?;
        let mut at = 0;
        let buffer = out.as_mut_slice();
        for entry in entries {
            let len = u32::try_from(entry.len()).map_err(|_| invalid("append entry too large"))?;
            buffer[at..at + 4].copy_from_slice(&len.to_be_bytes());
            buffer[at + 4..at + 4 + entry.len()].copy_from_slice(entry.as_slice());
            at += 4 + entry.len();
        }
        Ok(out.freeze())
    }
    fn decode<D: crate::oltp_replication::entry::CommandData>(
        shared: &WorkerSharedResources,
        class: TrafficClass,
        input: &ChargedBytes,
        count: usize,
    ) -> Result<Vec<OwnedRaftEntry<D>>, ProtocolError> {
        let bytes = input.as_slice();
        let mut entries = Vec::with_capacity(count);
        let mut at = 0usize;
        for _ in 0..count {
            let len = bytes
                .get(at..at + 4)
                .map(|len| u32::from_be_bytes(len.try_into().expect("four bytes")) as usize)
                .ok_or_else(|| invalid("truncated multi-entry append"))?;
            let body = bytes
                .get(at + 4..at + 4 + len)
                .ok_or_else(|| invalid("truncated multi-entry append"))?;
            let part = shared.copy_bytes_in(class.byte_class(), body)?;
            entries.push(OwnedRaftEntry::decode(
                &part,
                shared,
                class.byte_class(),
                usize::MAX,
            )?);
            at += 4 + len;
        }
        if at != bytes.len() {
            return Err(invalid("multi-entry append has trailing bytes"));
        }
        Ok(entries)
    }
}

/// Test evidence: appends a receiver decoded per (group, node), as
/// (RPCs, entries, multi-entry RPCs).
#[cfg(test)]
pub(crate) static RECEIVED_APPENDS: Mutex<BTreeMap<(uuid::Uuid, u64), ReceivedAppends>> =
    Mutex::new(BTreeMap::new());
#[cfg(test)]
pub(crate) type ReceivedAppends = (u64, u64, u64);

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
        target: RequestTarget,
    ) -> Result<R, ProtocolError> {
        if self.node.node_id != self.target {
            return Err(invalid("target node disagrees with membership"));
        }
        let group = self.factory.resident.identity();
        #[cfg(test)]
        if crate::oltp_runtime::recovery_tests::block_raft_for_test(group.group_id)
            || crate::oltp_growth_acceptance::data_raft_isolated(group.group_id)
        {
            return Err(unavailable());
        }
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
                target,
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
                RequestError::OutgoingLaneFull { .. } => ("outgoing_lane_full", None),
                RequestError::Unreachable => ("unreachable", None),
                RequestError::PeerRejected { error, .. } => ("peer_rejected", Some(error.code)),
                RequestError::Protocol(error) => ("local_protocol", Some(error.code)),
            };
            report("exchange", reason, code);
            ProtocolError::from(failure)
        })?;
        // The two private return types contain only fixed vote/log metadata.
        // Move the entire transport owner into the codec until it settles; a
        // small reply decodes on this task (RD3, PERF2b's bound).
        let reply_bytes = reply.header.len();
        let decoded = super::hosted::execute_small(
            &shared,
            execution(traffic),
            self.target,
            reply_bytes,
            move || {
                crate::protocol::json_input(reply.header.as_slice(), response_header)
                    .map_err(|_| invalid("typed Raft reply bounds"))?;
                serde_json::from_slice(reply.header.as_slice())
                    .map_err(|_| invalid("invalid typed Raft reply"))
            },
        )
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
        // A peer whose session negotiated multi-entry appends gets up to
        // MAX_APPEND_ENTRIES entries and about 1 MiB per RPC; any other peer
        // gets one entry per RPC. Success is translated against the sent
        // prefix, never the unsent original request tail.
        let class = self.factory.class();
        let multi = if rpc.entries.len() > 1 {
            self.factory.client.peer_supports(
                self.node.worker_id,
                class,
                PeerFeature::MultiEntryAppend,
            )
        } else {
            None
        };
        rpc.entries.truncate(if multi.is_some() {
            MAX_APPEND_ENTRIES
        } else {
            1
        });
        let budget = MultiEntry::BUDGET.min(self.factory.client.payload_limit(class));
        let mut encoded = if rpc.entries.is_empty() {
            Vec::new()
        } else {
            let shared = self.factory.shared.clone();
            let resident = self.factory.resident.clone();
            let entries = rpc.entries.clone();
            let estimate = entries
                .iter()
                .map(|entry| {
                    use crate::oltp_replication::owned::OwnedPayload;
                    entry.heap_bytes().unwrap_or(usize::MAX)
                })
                .fold(0usize, usize::saturating_add);
            super::hosted::execute_small(&shared, execution(class), self.target, estimate, {
                let shared = shared.clone();
                move || {
                    let mut encoded = Vec::with_capacity(entries.len());
                    let mut framed = 0usize;
                    for entry in entries {
                        let bytes = entry.charge_resident(&resident).and_then(|entry| {
                            shared.encode_in(
                                class.byte_class(),
                                &entry,
                                crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
                            )
                        });
                        let bytes = match bytes {
                            Ok(bytes) => bytes,
                            Err(error) if encoded.is_empty() => return Err(error),
                            Err(_) => break,
                        };
                        framed += 4 + bytes.len();
                        if !encoded.is_empty() && framed > budget {
                            break;
                        }
                        encoded.push(bytes);
                    }
                    Ok(encoded)
                }
            })
            .await
            .map_err(|_| unreachable())?
        };
        rpc.entries.truncate(encoded.len());
        let previous = rpc.prev_log_id;
        let last = rpc.last_log_id();
        // CAP1: entries Control's cluster features gate (`ReclaimIndex`) need
        // no per-connection check here: a leader proposes one only once every
        // replica host's build decodes it.
        let (payload, entries, target) = match encoded.len() {
            0 => (None, None, RequestTarget::from(self.node.worker_id)),
            1 => (
                encoded.pop(),
                None,
                RequestTarget::from(self.node.worker_id),
            ),
            count => {
                let identity = multi.expect("several entries only for a multi-entry peer");
                let payload = MultiEntry::encode(&self.factory.shared, class, &encoded)
                    .map_err(|_| unreachable())?;
                (
                    Some(payload),
                    Some(count as u32),
                    RequestTarget::from(identity),
                )
            }
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
            entries,
        };
        let names = if header.payload_bytes == 0 {
            [
                "raft.data.net.heartbeat",
                "raft.ts.net.heartbeat",
                "raft.meta.net.heartbeat",
            ]
        } else {
            [
                "raft.data.net.append",
                "raft.ts.net.append",
                "raft.meta.net.append",
            ]
        };
        let data_append = header.payload_bytes != 0
            && matches!(
                self.factory.resident.identity().kind,
                crate::oltp_replication::types::GroupKind::Data { .. }
            );
        if data_append {
            super::hosted::proposal_clock::mark(crate::oltp_runtime::stage_timing::perf_stage!(
                "raft.data.propose_to_send"
            ));
        }
        let reply: AppendEntriesResponse<C> = crate::oltp_runtime::stage_timing::timed(
            crate::oltp_runtime::stage_timing::by_kind(
                &self.factory.resident.identity().kind,
                names,
            ),
            self.rpc(header, payload, option.soft_ttl(), target),
        )
        .await
        .map_err(|_| unreachable())?;
        if data_append {
            super::hosted::proposal_clock::mark(crate::oltp_runtime::stage_timing::perf_stage!(
                "raft.data.propose_to_ack"
            ));
            super::hosted::proposal_clock::mark_first(
                crate::oltp_runtime::stage_timing::perf_stage!("raft.data.propose_to_first_ack"),
            );
        }
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
                entries: None,
            },
            None,
            option.soft_ttl(),
            RequestTarget::from(self.node.worker_id),
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

    fn route(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<Arc<DispatchRoute>, ProtocolError> {
        self.get(snapshot::target_node(bytes, session.purpose())?)
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
        Box::pin(async move { self.route(session, bytes)?.preflight(session, bytes).await })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            self.route(&session, bytes.as_slice())?
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
    /// Follows a supervisor restart of the host's core (RV-APPLY F4).
    raft: Arc<RaftCores<C, M>>,
    state: HostedState<C, M>,
    incoming: Arc<tokio::sync::Mutex<Option<snapshot::Incoming>>>,
    resident: ReplicaLease,
    shared: Arc<WorkerSharedResources>,
    bootstrap: Arc<BTreeMap<u64, ReplicaMember>>,
    catch_up_before: Option<u64>,
}
impl<C, M> ReplicaEndpoint<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(crate) fn new(
        raft: Arc<RaftCores<C, M>>,
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
            catch_up_before: None,
        })
    }
    pub(crate) fn with_learner_catch_up(mut self, catch_up_before: Option<u64>) -> Self {
        self.catch_up_before = catch_up_before;
        self
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
            Method::Vote
                if head.payload_bytes != 0
                    || head.leader_commit.is_some()
                    || head.entries.is_some() =>
            {
                return Err(invalid("invalid vote frame fields"));
            }
            Method::Append
                if head.leadership_transfer_from.is_some()
                    || head.entries.is_some_and(|count| {
                        !(2..=MAX_APPEND_ENTRIES as u32).contains(&count) || head.payload_bytes == 0
                    }) =>
            {
                return Err(invalid("invalid append frame fields"));
            }
            _ => {}
        }
        self.verify_sender(
            session,
            head.from_node,
            head.to_node,
            matches!(head.method, Method::Append),
        )?;
        Ok(traffic)
    }
    fn verify_sender(
        &self,
        session: &AuthenticatedSession,
        from_node: u64,
        to_node: u64,
        catch_up: bool,
    ) -> Result<(), ProtocolError> {
        if session.group() != self.resident.identity() || to_node != self.resident.node_id() {
            return Err(invalid("typed RPC target mismatch"));
        }
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let member = raft_sender(
            &metrics.membership_config,
            &self.bootstrap,
            self.catch_up_before.filter(|_| catch_up),
            from_node,
        )
        .ok_or_else(|| invalid("sender is outside enrolled replica membership"))?;
        if member.worker_id != session.remote().worker_id {
            return Err(invalid("sender node is bound to another physical worker"));
        }
        Ok(())
    }
}

fn raft_sender<'a>(
    stored: &'a openraft::StoredMembership<CommittedLeaderId, Membership>,
    bootstrap: &'a BTreeMap<u64, ReplicaMember>,
    catch_up_before: Option<u64>,
    sender: u64,
) -> Option<&'a ReplicaMember> {
    let membership = stored.membership();
    membership
        .nodes()
        .find(|(node, _)| **node == sender)
        .map(|(_, member)| member)
        .or_else(|| {
            // A newly hosted learner can receive historical membership
            // entries before it sees the previously published placement.
            // Until then, its authorized bootstrap voters can send catch-up
            // traffic even if a historical membership does not list them.
            let replaying_history = catch_up_before
                .is_some_and(|index| stored.log_id().is_none_or(|log| log.index < index));
            if replaying_history || membership.nodes().next().is_none() {
                bootstrap.get(&sender)
            } else {
                None
            }
        })
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
            #[cfg(test)]
            if crate::oltp_growth_acceptance::data_raft_isolated(self.resident.identity().group_id)
            {
                return Err(unavailable());
            }
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
                    let entries = if head.payload_bytes == 0 {
                        Vec::new()
                    } else {
                        let input_bytes = sections.section(0).map_or(0, |input| input.len());
                        super::hosted::execute_small(
                            &shared,
                            execution(traffic),
                            head.from_node,
                            input_bytes,
                            {
                                let shared = shared.clone();
                                move || {
                                    let input = sections
                                        .section(0)
                                        .ok_or_else(|| invalid("missing append payload"))?;
                                    match head.entries {
                                        Some(count) => MultiEntry::decode::<C::Command>(
                                            &shared,
                                            traffic,
                                            input,
                                            count as usize,
                                        ),
                                        None => OwnedRaftEntry::<C::Command>::decode(
                                            input,
                                            &shared,
                                            traffic.byte_class(),
                                            usize::MAX,
                                        )
                                        .map(|entry| vec![entry]),
                                    }
                                }
                            },
                        )
                        .await?
                    };
                    #[cfg(test)]
                    if !entries.is_empty() {
                        let mut received = RECEIVED_APPENDS.lock().unwrap();
                        let group = received
                            .entry((self.resident.identity().group_id, self.resident.node_id()))
                            .or_default();
                        group.0 += 1;
                        group.1 += entries.len() as u64;
                        group.2 += u64::from(entries.len() > 1);
                    }
                    let rpc = AppendEntriesRequest::<C> {
                        vote: head.vote,
                        prev_log_id: head.prev_log_id,
                        entries,
                        leader_commit: head.leader_commit,
                    };
                    let names = if rpc.entries.is_empty() {
                        [
                            "raft.data.net.heartbeat_handle",
                            "raft.ts.net.heartbeat_handle",
                            "raft.meta.net.heartbeat_handle",
                        ]
                    } else {
                        [
                            "raft.data.net.append_handle",
                            "raft.ts.net.append_handle",
                            "raft.meta.net.append_handle",
                        ]
                    };
                    let reply = crate::oltp_runtime::stage_timing::timed(
                        crate::oltp_runtime::stage_timing::by_kind(
                            &self.resident.identity().kind,
                            names,
                        ),
                        self.raft.append_entries(rpc),
                    )
                    .await
                    .map_err(|_| {
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
