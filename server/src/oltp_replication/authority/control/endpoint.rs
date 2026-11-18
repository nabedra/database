//! Strong Control reads share the authenticated worker listener. This endpoint
//! grants no raw Raft proposal or caller-supplied proof capability.
use super::{
    ControlHandle, DataWorkerGrowthProof, GroupRecord, ScopedQueuePlacement, ServiceAction,
    ServiceAuthorization, stale_service_route, unsupported_service_route,
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    oltp_deployment::live::InventoryPin,
    oltp_replication::{multiplex::*, types::*},
    oltp_runtime::{
        replica::{QueueAuthority, QueueScope},
        worker_endpoint::AuthorityReader,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{
    mem::size_of,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

const HEADER_LIMIT: usize = 4096;

#[cfg(test)]
mod route_tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ReadRequest {
    Current,
    ColdPin {
        authority: Authority,
        prior_run: Uuid,
    },
    QueueRemoval {
        owner: WorkerIdentity,
        ticket: JoinTicket,
    },
    EnrollmentRevision,
    GroupInventoryRevision,
    RestartPreparation {
        revision: u64,
    },
    PreparedDataWorker {
        revision: u64,
    },
    WorkerExcluded {
        worker: WorkerIdentity,
    },
    OwnWorkerDrain,
    OwnGrowthSample {
        pin: InventoryPin,
        claim_id: Uuid,
    },
    PoolReplicaRetired { stamp: Box<super::ReplicaRetiredStamp> },
    RemoteReplicaMove {
        id: Uuid,
    },
    RemoteReplicaVoter {
        id: Uuid,
        node: u64,
    },
    Group {
        group_id: Uuid,
    },
    ScopedQueue {
        group_id: Uuid,
    },
    SplitSource {
        expected: Box<GroupRecord>,
        authority: Authority,
    },
    ServiceRole {
        remote: WorkerIdentity,
        action: ServiceAction,
        target: Box<GroupIdentity>,
    },
    ServiceRoleAtReplica {
        remote: WorkerIdentity,
        target: Box<GroupIdentity>,
        node_id: u64,
        generation: Uuid,
    },
    TerminalReader {
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    },
    Apis {
        revision: u64,
        after: Option<Uuid>,
    },
    Workers {
        revision: u64,
        after: Option<Uuid>,
    },
    Ready {
        expected: ReadyMembership,
        after: Option<Uuid>,
    },
    Groups {
        revision: u64,
        after: Option<Uuid>,
    },
    GroupMembers {
        expected: Box<GroupRecord>,
        after: Option<u64>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply {
    nonce: Uuid,
    bytes: u64,
}

pub(crate) struct ControlEndpoint {
    handle: Arc<ControlHandle>,
    shared: Arc<WorkerSharedResources>,
}
impl ControlEndpoint {
    pub(super) async fn read_eks_replacement(&self, worker: WorkerIdentity, token_digest: Digest) -> Result<ChargedBytes, ProtocolError> {
        self.handle.own_eks_replacement(worker, token_digest).await
    }
    pub(super) async fn authorize_data_worker_join(&self, worker: WorkerIdentity, token: Option<Digest>) -> Result<(), ProtocolError> {
        self.handle.authorize_data_worker_join(worker, token).await
    }
    pub(crate) fn new(
        handle: Arc<ControlHandle>,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !Arc::ptr_eq(handle.resources(), &shared) {
            return Err(invalid("Control endpoint has a foreign worker budget"));
        }
        Ok(Arc::new(Self { handle, shared }))
    }
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<ReadRequest, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.handle.identity()
            || header.len() > HEADER_LIMIT
        {
            return Err(invalid("invalid Control authority session"));
        }
        crate::protocol::json_input(header, HEADER_LIMIT)
            .map_err(|_| invalid("invalid Control authority header"))?;
        serde_json::from_slice(header).map_err(|_| invalid("invalid Control authority request"))
    }
}
impl Endpoint for ControlEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            // No payload or fresh recursive Control lookup is needed to read
            // Control itself. TLS authenticates locally applied enrollment.
            Ok(ReceivePlan {
                class: TrafficClass::Authority,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request = self.parse(&session, header.as_ref())?;
            if (0..3).any(|i| sections.section(i).is_some()) {
                return Err(invalid("Control read has unexpected payload"));
            }
            let scope = QueueScope {
                cluster_id: session.group().cluster_id,
                database: session.group().database.clone(),
            };
            let bytes = match request {
                ReadRequest::Group { group_id } => {
                    let group = self.handle.group(group_id).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &group, HEADER_LIMIT)?
                }
                ReadRequest::ScopedQueue { group_id } => self.handle.read_scoped_queue(group_id).await?,
                ReadRequest::SplitSource { expected, authority } => {
                    self.handle.split_source(*expected, authority).await?
                }
                ReadRequest::ServiceRole {
                    remote,
                    action,
                    target,
                } => {
                    let grant = self
                        .handle
                        .service_role(&scope, remote, action, *target)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &grant, HEADER_LIMIT)?
                }
                ReadRequest::ServiceRoleAtReplica {
                    remote,
                    target,
                    node_id,
                    generation,
                } => {
                    let grant = self
                        .handle
                        .service_role_at_replica(
                            &scope,
                            remote,
                            *target,
                            session.remote(),
                            node_id,
                            generation,
                        )
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &grant, HEADER_LIMIT)?
                }
                ReadRequest::EnrollmentRevision => {
                    let revision = self
                        .handle
                        .inspect()
                        .await?
                        .ok_or_else(unavailable)?
                        .enrollment_revision;
                    self.shared
                        .encode_in(ByteClass::Progress, &revision, HEADER_LIMIT)?
                }
                ReadRequest::GroupInventoryRevision => {
                    let revision = self
                        .handle
                        .inspect()
                        .await?
                        .ok_or_else(unavailable)?
                        .group_inventory_revision;
                    self.shared.encode_in(ByteClass::Progress, &revision, HEADER_LIMIT)?
                }
                ReadRequest::RestartPreparation { revision } => {
                    let preparation = self.handle.restart_preparation(revision).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &preparation, HEADER_LIMIT)?
                }
                ReadRequest::PreparedDataWorker { revision } => {
                    let enrollment = self.handle.prepared_data_worker(revision).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &enrollment, HEADER_LIMIT)?
                }
                ReadRequest::WorkerExcluded { worker } => {
                    let excluded = self.handle.worker_replaced(worker).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &excluded, HEADER_LIMIT)?
                }
                ReadRequest::OwnWorkerDrain => self.handle.own_worker_drain(session.remote()).await?,
                ReadRequest::OwnGrowthSample { pin, claim_id } => {
                    self.handle
                        .data_worker_growth_proof(session.remote(), claim_id, pin)
                        .await?
                }
                ReadRequest::PoolReplicaRetired { stamp } => {
                    if stamp.worker != session.remote() {
                        return Err(invalid("replica retirement is for a different worker"));
                    }
                    let retired = self.handle.verify_pool_replica_retired(*stamp).await?;
                    self.shared.encode_in(ByteClass::Progress, &retired, HEADER_LIMIT)?
                }
                ReadRequest::RemoteReplicaMove { id } => {
                    self.handle.remote_move_view(id, session.remote()).await?
                }
                ReadRequest::RemoteReplicaVoter { id, node } => {
                    self.handle.replica_move_voter_read(id, node, session.remote()).await?
                }
                ReadRequest::Current => {
                    let current = self.handle.current(&scope).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::ColdPin {
                    authority,
                    prior_run,
                } => {
                    let published = self
                        .handle
                        .published_cold_fence(authority, Some(prior_run))
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &published, HEADER_LIMIT)?
                }
                ReadRequest::QueueRemoval { owner, ticket } => {
                    let current = self.handle.removal_view(&scope, owner, ticket).await?;
                    self.shared.encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::TerminalReader {
                    remote,
                    timestamps_group_id,
                } => {
                    let current = self
                        .handle
                        .terminal_reader_view(&scope, remote, timestamps_group_id)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::Apis { revision, after } => self.handle.apis(revision, after).await?,
                ReadRequest::Workers { revision, after } => {
                    self.handle.workers(revision, after).await?
                }
                ReadRequest::Ready { expected, after } => {
                    self.handle.ready_page_read(expected, after).await?
                }
                ReadRequest::Groups { revision, after } => {
                    self.handle.groups(revision, after).await?
                }
                ReadRequest::GroupMembers { expected, after } => {
                    self.handle.group_members(*expected, after).await?
                }
            };
            let reply = self.shared.encode_in(
                ByteClass::Progress,
                &ReadReply {
                    nonce: session.nonce(),
                    bytes: bytes.len() as u64,
                },
                HEADER_LIMIT,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [Some(bytes), None, None],
            })
        })
    }
}

/// The manager supplies its current locally applied permanent Control voters.
/// Candidates are paged/cursor based; no total-cluster-size cap is introduced.
pub(crate) trait ControlRoutes: Send + Sync + 'static {
    fn preferred(&self) -> Option<Uuid> {
        None
    }
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError>;
}
pub(crate) struct ControlClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
    routes: Arc<dyn ControlRoutes>,
    replica_routes: Option<Box<Mutex<ReplicaRouteCache>>>,
}
impl ControlClient {
    pub(crate) async fn scoped_queue(
        &self,
        group: &GroupIdentity,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        if group.cluster_id != self.group.cluster_id
            || group.database != self.group.database
            || group.validate().is_err()
            || !matches!(group.kind, GroupKind::Data { .. })
        {
            return Err(invalid("foreign scoped queue group"));
        }
        let placement = self.scoped_queue_by_id(group.group_id).await?;
        if let Some(placement) = &placement {
            placement.validate(group)?;
        }
        Ok(placement)
    }

    pub(crate) async fn scoped_queue_by_id(
        &self,
        group_id: Uuid,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        if group_id.is_nil() {
            return Err(invalid("invalid scoped queue group ID"));
        }
        let reply = self
            .read(&ReadRequest::ScopedQueue { group_id })
            .await?;
        let reply = self.reserve_section_decode(reply).await?;
        let placement = reply.decode_section::<Option<ScopedQueuePlacement>>(0)?;
        if let Some(placement) = &*placement {
            let group = &placement.group;
            if group.group_id != group_id
                || group.cluster_id != self.group.cluster_id
                || group.database != self.group.database
            {
                return Err(invalid("foreign scoped queue placement"));
            }
            placement.validate(group)?;
        }
        Ok((*placement).clone())
    }

    pub(crate) async fn published_cold_fence(
        &self,
        authority: Authority,
        prior_run: Uuid,
    ) -> Result<Option<Uuid>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ColdPin {
                authority,
                prior_run,
            })
            .await?;
        Ok(*reply.decode_section::<Option<Uuid>>(0)?)
    }

    pub(crate) async fn own_growth_sample(
        &self,
        pin: InventoryPin,
        claim_id: Uuid,
    ) -> Result<Decoded<DataWorkerGrowthProof>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::OwnGrowthSample { pin, claim_id })
            .await?;
        let proof = reply.decode_section::<DataWorkerGrowthProof>(0)?;
        let local = self.client.local_identity();
        if proof.pin != pin
            || proof.authority.ready != pin.ready
            || proof.primary.claim_id != claim_id
            || proof.primary.mount.worker != local
            || proof
                .added
                .as_ref()
                .is_some_and(|added| added.mount.worker != local)
        {
            return Err(invalid("Control growth sample scope differs"));
        }
        Ok(proof)
    }

    pub(crate) async fn split_source(
        &self,
        expected: &GroupRecord,
        authority: Authority,
    ) -> Result<crate::oltp_replication::authority::timestamp::split::SplitSourcePlacement, ProtocolError> {
        let reply = self.read(&ReadRequest::SplitSource {
            expected: Box::new(expected.clone()),
            authority,
        }).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let decoded = reply.decode_section::<crate::oltp_replication::authority::timestamp::split::SplitSourcePlacement>(0)?;
        if decoded.record != *expected {
            return Err(invalid("automatic split Control source generation changed"));
        }
        decoded.validate(&expected.identity)?;
        Ok((*decoded).clone())
    }

    pub(crate) async fn own_worker_drain(&self) -> Result<Option<super::WorkerDrainRecord>, ProtocolError> {
        let reply = self.read(&ReadRequest::OwnWorkerDrain).await?;
        let record = reply.decode_section::<Option<super::WorkerDrainRecord>>(0)?;
        if record.as_ref().is_some_and(|record| record.intent.worker != self.client.local_identity()) {
            return Err(invalid("Control worker drain identity differs"));
        }
        Ok((*record).clone())
    }

    pub(crate) async fn verify_pool_replica_retired(&self, stamp: super::ReplicaRetiredStamp) -> Result<bool, ProtocolError> {
        let reply = self.read(&ReadRequest::PoolReplicaRetired { stamp: Box::new(stamp) }).await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }

    pub(crate) async fn remote_replica_move(
        &self,
        id: Uuid,
    ) -> Result<Decoded<super::RemoteMoveView>, ProtocolError> {
        let reply = self.read(&ReadRequest::RemoteReplicaMove { id }).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let record = reply.decode_section::<super::RemoteMoveView>(0)?;
        let local = self.client.local_identity();
        if record.plan.id != id
            || (record.plan.request.worker != local
                && !record.plan.remote_source.as_ref().is_some_and(|source| source.worker == local))
        {
            return Err(invalid("Control remote placement identity differs"));
        }
        Ok(record)
    }

    pub(crate) async fn remote_replica_voter(
        &self,
        id: Uuid,
        node: u64,
    ) -> Result<Decoded<(super::ReplicaMove, WorkerIdentity)>, ProtocolError> {
        use super::ReplicaMovePhase;
        let reply = self.read(&ReadRequest::RemoteReplicaVoter { id, node }).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let record = reply.decode_section::<(super::ReplicaMove, WorkerIdentity)>(0)?;
        let (plan, voter) = &*record;
        let local = self.client.local_identity();
        let members = match plan.phase {
            ReplicaMovePhase::Moving => &plan.old,
            ReplicaMovePhase::Published { .. } => &plan.next,
            _ => return Err(invalid("Control remote voter phase differs")),
        };
        let learner_check = plan.phase == ReplicaMovePhase::Moving
            && plan.remote_target()
            && plan.request.worker == local;
        if plan.id != id
            || (local != *voter && !learner_check)
            || !members.iter().any(|member| member.node_id == node && member.worker_id == voter.worker_id)
        {
            return Err(invalid("Control remote voter identity differs"));
        }
        Ok(record)
    }

    pub(crate) fn new(
        client: WorkerClient,
        shared: Arc<WorkerSharedResources>,
        group: GroupIdentity,
        routes: Arc<dyn ControlRoutes>,
    ) -> Result<Arc<Self>, ProtocolError> {
        group
            .validate()
            .map_err(|_| invalid("invalid Control RPC group"))?;
        if group.kind != GroupKind::Control {
            return Err(invalid("wrong Control RPC group kind"));
        }
        let replica_routes = ReplicaRouteCache::new(&shared);
        Ok(Arc::new(Self {
            client,
            shared,
            group,
            routes,
            replica_routes,
        }))
    }
    async fn read(&self, request: &ReadRequest) -> Result<ReceivedReply, ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER_LIMIT)?;
        // The ServiceRole endpoint finishes using Control consensus and its
        // local state. It does not make another outgoing Authority read.
        let placement_role = matches!(request, ReadRequest::ServiceRoleAtReplica { .. });
        let role_dependency = placement_role || matches!(request, ReadRequest::ServiceRole { .. });
        let exact_sample = matches!(request, ReadRequest::OwnGrowthSample { .. });
        let preferred = self.routes.preferred();
        let mut first = preferred;
        let mut after = None;
        let mut error = unavailable();
        let mut legacy_placement_role = false;
        loop {
            let worker = if let Some(worker) = first.take() {
                worker
            } else {
                let Some(worker) = self.routes.next(after)? else {
                    break;
                };
                if after.is_some_and(|previous| worker <= previous) {
                    return Err(invalid("Control routes are not strictly ordered"));
                }
                after = Some(worker);
                if Some(worker) == preferred {
                    continue;
                }
                worker
            };
            if worker.is_nil() {
                return Err(invalid("nil Control route"));
            }
            let reply = if role_dependency {
                match self
                    .client
                    .request_authority_dependency(&self.group, worker, header.clone(), 64 << 10)
                    .await
                {
                    // Every route shares this local lane. Exhaustion cannot
                    // improve by selecting another peer.
                    Err(RequestError::OutgoingLaneFull) => {
                        return Err(RequestError::OutgoingLaneFull.into());
                    }
                    Err(RequestError::PeerRejected {
                        worker: peer,
                        error,
                    }) if placement_role && peer.worker_id == worker => {
                        if error == invalid("invalid Control authority request") {
                            // Older Control voters reject this new, closed request
                            // in their parser. The application has not been run.
                            legacy_placement_role = true;
                            continue;
                        }
                        if error == stale_service_route() || error.code == ErrorCode::Unauthorized {
                            return Err(error);
                        }
                        Err(error)
                    }
                    result => result.map_err(ProtocolError::from),
                }
            } else if exact_sample {
                match self
                    .client
                    .request_classified(
                        &self.group,
                        worker,
                        Purpose::Authority,
                        header.clone(),
                        [None, None, None],
                        64 << 10,
                    )
                    .await
                {
                    Ok(reply) => Ok(reply),
                    Err(RequestError::Unreachable) => Err(RequestError::Unreachable.into()),
                    Err(RequestError::PeerRejected { worker: peer, error }) => {
                        if peer.worker_id != worker {
                            return Err(invalid("Control growth sample error binding differs"));
                        }
                        tracing::debug!(%worker, code=?error.code, outcome=?error.outcome,
                            message=error.message, "Control own growth sample rejected");
                        if !growth_sample_peer_unavailable(&error) {
                            return Err(error);
                        }
                        Err(error)
                    }
                    // Local exhaustion applies to every route. An invalid
                    // exchange cannot be replaced with a different peer reply.
                    Err(failure) => return Err(failure.into()),
                }
            } else {
                self.client
                    .request(
                        &self.group,
                        worker,
                        Purpose::Authority,
                        header.clone(),
                        [None, None, None],
                        64 << 10,
                    )
                    .await
            };
            match reply {
                Ok(reply) => {
                    if reply.remote_identity().worker_id != worker
                        || reply.group() != &self.group
                        || reply.purpose() != Purpose::Authority
                        || reply.sections[1].is_some()
                        || reply.sections[2].is_some()
                    {
                        return Err(invalid("Control response transport binding differs"));
                    }
                    {
                        let decoded = reply.decode_header::<ReadReply>()?;
                        if decoded.nonce != reply.nonce()
                            || reply.sections[0].as_ref().map(|bytes| bytes.len() as u64)
                                != Some(decoded.bytes)
                        {
                            return Err(invalid("Control response payload differs"));
                        }
                    }
                    return Ok(reply);
                }
                Err(failure) => error = failure,
            }
        }
        if legacy_placement_role {
            return Err(unsupported_service_route());
        }
        Err(error)
    }
    async fn authority(
        &self,
        scope: &QueueScope,
        request: ReadRequest,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign Control query scope"));
        }
        let reply = self.read(&request).await?;
        let current = reply.decode_section::<QueueAuthority>(0)?;
        current
            .ready
            .view
            .validate()
            .map_err(|_| invalid("invalid Control ready view"))?;
        current
            .owner
            .validate()
            .map_err(|_| invalid("invalid Control owner"))?;
        if current.ready.ready_count == 0 {
            return Err(unavailable());
        }
        Ok(*current)
    }
    pub(crate) async fn worker_replaced(
        &self,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        let reply = self.read(&ReadRequest::WorkerExcluded { worker }).await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }
    pub(crate) async fn group(&self, group_id: Uuid) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Group { group_id }).await
    }
    pub(crate) async fn group_members(
        &self,
        expected: GroupRecord,
        after: Option<u64>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::GroupMembers {
            expected: Box::new(expected),
            after,
        })
        .await
    }
    pub(crate) async fn service_role(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        action: ServiceAction,
        target: GroupIdentity,
    ) -> Result<ServiceAuthorization, ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign service authorization scope"));
        }
        let reply = self
            .read(&ReadRequest::ServiceRole {
                remote,
                action,
                target: Box::new(target),
            })
            .await?;
        Ok(*reply.decode_section::<ServiceAuthorization>(0)?)
    }
    pub(crate) async fn service_role_at_replica(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        target: GroupIdentity,
        node_id: u64,
        generation: Uuid,
    ) -> Result<(), ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign service authorization scope"));
        }
        let reply = self
            .read(&ReadRequest::ServiceRoleAtReplica {
                remote,
                target: Box::new(target),
                node_id,
                generation,
            })
            .await?;
        let grant = reply.decode_section::<ServiceAuthorization>(0)?;
        if grant.owner != Some(remote) {
            return Err(invalid(
                "receiver placement grant differs from request owner",
            ));
        }
        Ok(())
    }
    pub(crate) async fn ready_page(
        &self,
        expected: ReadyMembership,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Ready { expected, after }).await
    }
}

fn growth_sample_peer_unavailable(error: &ProtocolError) -> bool {
    // A stale pin and a failed ReadIndex both use StorageUnavailable. Only
    // recognized quorum/transport failures allow another authenticated read.
    match (error.code, error.outcome) {
        (ErrorCode::StorageUnavailable, Outcome::Unknown) => matches!(
            error.message.as_str(),
            "Control leader quorum or local application unavailable"
                | "typed Raft quorum, application or leader authority unavailable"
                | "worker transport unavailable"
        ),
        (ErrorCode::Overloaded, Outcome::NotApplied) => matches!(
            error.message.as_str(),
            "worker handler lane full" | "worker preflight lane full"
        ),
        _ => false,
    }
}
impl AuthorityReader for ControlClient {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, ReadRequest::Current))
    }
    fn removal_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        owner: WorkerIdentity,
        ticket: JoinTicket,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, ReadRequest::QueueRemoval { owner, ticket }))
    }
    fn excludes_process(&self, worker: WorkerIdentity) -> EndpointFuture<'_, bool> {
        Box::pin(self.worker_replaced(worker))
    }
    fn terminal_reader_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(
            scope,
            ReadRequest::TerminalReader {
                remote,
                timestamps_group_id,
            },
        ))
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "Control quorum is unavailable",
        Outcome::NotApplied,
    )
}

pub(crate) type ReplicaRoutes = Arc<VerifiedReplicaRoutes>;

/// One verified membership cut. The route vector and record remain charged to
/// worker working memory through eviction and the last outstanding caller.
pub(crate) struct VerifiedReplicaRoutes {
    pub record: GroupRecord,
    pub members: Vec<ReplicaMember>,
    hint: Mutex<ReplicaRouteHint>,
    _owner: ScratchLease,
}

#[derive(Default)]
struct ReplicaRouteHint {
    last_successful_node: Option<u64>,
    receiver: Option<(u64, WorkerIdentity)>,
    probe_after: Option<Instant>,
}

impl VerifiedReplicaRoutes {
    pub(crate) fn last_successful_node(&self) -> Option<u64> {
        self.hint.lock().ok()?.last_successful_node
    }

    pub(crate) fn record_successful_node(&self, node_id: u64) {
        if let Ok(mut hint) = self.hint.lock() {
            hint.last_successful_node = Some(node_id);
        }
    }

    pub(crate) fn hinted_receiver(&self, node_id: u64) -> Option<WorkerIdentity> {
        let (node, receiver) = self.hint.lock().ok()?.receiver?;
        (node == node_id).then_some(receiver)
    }

    pub(crate) fn begin_receiver_probe(&self) -> bool {
        let Ok(mut hint) = self.hint.lock() else {
            return false;
        };
        let now = Instant::now();
        if hint.probe_after.is_some_and(|after| after > now) {
            return false;
        }
        hint.probe_after = Some(now + Duration::from_secs(1));
        true
    }

    pub(crate) fn confirm_hinted_receiver(&self, node_id: u64, receiver: WorkerIdentity) {
        if let Ok(mut hint) = self.hint.lock() {
            hint.receiver = Some((node_id, receiver));
        }
    }
}

const ROUTE_CACHE_ENTRIES: usize = 64;
const ROUTE_CACHE_ENTRY_BYTES: usize = 4 << 10;

struct ReplicaRouteCache {
    entries: [Option<ReplicaRoutes>; ROUTE_CACHE_ENTRIES],
    next: usize,
    _metadata: ScratchLease,
}
impl ReplicaRouteCache {
    fn new(shared: &WorkerSharedResources) -> Option<Box<Mutex<Self>>> {
        let metadata = shared
            .reserve_scratch(ByteClass::Data, size_of::<Mutex<Self>>() + 64)
            .ok()?;
        Some(Box::new(Mutex::new(Self {
            entries: [const { None }; ROUTE_CACHE_ENTRIES],
            next: 0,
            _metadata: metadata,
        })))
    }

    fn slot(&mut self, group: &GroupIdentity) -> Option<&mut Option<ReplicaRoutes>> {
        self.entries.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|route| route.record.identity == *group)
        })
    }

    fn store(&mut self, route: ReplicaRoutes) -> ReplicaRoutes {
        if let Some(existing) = self
            .slot(&route.record.identity)
            .and_then(|slot| slot.as_ref())
        {
            // A concurrent verification may already have filled this slot.
            // Each caller uses only the record from its own strong read.
            return if existing.record == route.record {
                existing.clone()
            } else {
                route
            };
        }
        if let Some(slot) = self.entries.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(route.clone());
        } else {
            self.entries[self.next] = Some(route.clone());
            self.next = (self.next + 1) % ROUTE_CACHE_ENTRIES;
        }
        route
    }
}
impl ControlClient {
    pub(crate) fn cached_replica_routes(&self, group: &GroupIdentity) -> Option<ReplicaRoutes> {
        self.route_cache()?.slot(group)?.clone()
    }
    pub(crate) async fn enrollment_revision(&self) -> Result<u64, ProtocolError> {
        let reply = self.read(&ReadRequest::EnrollmentRevision).await?;
        Ok(*reply.decode_section::<u64>(0)?)
    }
    pub(crate) async fn group_inventory_revision(&self) -> Result<u64, ProtocolError> {
        let reply = self.read(&ReadRequest::GroupInventoryRevision).await?;
        Ok(*reply.decode_section::<u64>(0)?)
    }
    pub(crate) async fn groups(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Groups { revision, after }).await
    }
    pub(crate) async fn restart_preparation(
        &self,
        revision: u64,
    ) -> Result<Option<super::RestartPreparation>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::RestartPreparation { revision })
            .await?;
        Ok((*reply.decode_section::<Option<super::RestartPreparation>>(0)?).clone())
    }
    pub(crate) async fn prepared_data_worker(
        &self,
        revision: u64,
    ) -> Result<Option<WorkerEnrollment>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::PreparedDataWorker { revision })
            .await?;
        Ok((*reply.decode_section::<Option<WorkerEnrollment>>(0)?).clone())
    }
    pub(crate) async fn workers(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Workers { revision, after }).await
    }
    pub(crate) async fn apis(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Apis { revision, after }).await
    }
    pub(crate) async fn replica_routes(
        &self,
        expected: &GroupIdentity,
    ) -> Result<ReplicaRoutes, ProtocolError> {
        if expected.cluster_id != self.group.cluster_id || expected.database != self.group.database
        {
            return Err(invalid("foreign replica routing scope"));
        }
        let start = Instant::now();
        let reply = self.group(expected.group_id).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let decoded = reply.decode_section::<Option<GroupRecord>>(0)?;
        let record = decoded
            .as_ref()
            .filter(|record| record.identity == *expected && record.replica_count != 0);
        let (cached, changed) = {
            let mut cache = self.route_cache();
            if let Some(slot) = cache.as_mut().and_then(|cache| cache.slot(expected)) {
                if slot
                    .as_ref()
                    .is_some_and(|route| record == Some(&route.record))
                {
                    (slot.clone(), false)
                } else {
                    *slot = None;
                    (None, true)
                }
            } else {
                (None, false)
            }
        };
        if let Some(route) = cached {
            tracing::debug!(
                target: "nabedra_database_server::oltp_replication::service_rpc",
                cache = "hit",
                pages = 0,
                ms = start.elapsed().as_millis(),
                "verified replica routes"
            );
            return Ok(route);
        }
        let record = record.ok_or_else(|| {
            if decoded.is_some() {
                invalid("replica routing identity differs")
            } else {
                unavailable()
            }
        })?;
        let count = usize::try_from(record.replica_count).map_err(|_| unavailable())?;
        let bytes = count
            .checked_mul(size_of::<ReplicaMember>() + 128)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(unavailable)?;
        let (owner, retain) = match self.shared.reserve_scratch(ByteClass::Data, bytes) {
            Ok(owner) => (owner, true),
            Err(error) => {
                let Some(mut cache) = self.route_cache() else {
                    return Err(error);
                };
                if cache.entries.iter().all(Option::is_none) {
                    return Err(error);
                }
                cache.entries.fill(None);
                drop(cache);
                (self.shared.reserve_scratch(ByteClass::Data, bytes)?, false)
            }
        };
        let record = record.clone();
        // Release the record's independent reply decoder before requesting pages.
        drop(decoded);
        drop(reply);

        let record_ms = start.elapsed().as_millis();
        let (members, pages) = self.load_replica_members(&record, count).await?;
        let retained = members
            .capacity()
            .checked_mul(size_of::<ReplicaMember>())
            .and_then(|bytes| {
                bytes.checked_add(
                    record.identity.database.capacity_bytes()
                        + size_of::<VerifiedReplicaRoutes>()
                        + 2 * size_of::<usize>()
                        + 128,
                )
            })
            .ok_or_else(unavailable)?;
        owner.shrink_unused(retained)?;
        let route = Arc::new(VerifiedReplicaRoutes {
            record,
            members,
            hint: Mutex::new(ReplicaRouteHint::default()),
            _owner: owner,
        });
        let cacheable = retain && retained <= ROUTE_CACHE_ENTRY_BYTES;
        tracing::debug!(
            target: "nabedra_database_server::oltp_replication::service_rpc",
            cache = if changed { "changed" } else { "miss" },
            cacheable,
            pages,
            record_ms,
            ms = start.elapsed().as_millis(),
            "verified replica routes"
        );
        Ok(match self.route_cache() {
            Some(mut cache) if cacheable => cache.store(route),
            _ => route,
        })
    }

    pub(crate) fn invalidate_replica_routes(&self, route: &ReplicaRoutes) {
        let invalidated = {
            let mut cache = self.route_cache();
            cache
                .as_mut()
                .and_then(|cache| cache.slot(&route.record.identity))
                .is_some_and(|slot| {
                    if slot
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, route))
                    {
                        *slot = None;
                        true
                    } else {
                        false
                    }
                })
        };
        if invalidated {
            tracing::debug!(
                target: "nabedra_database_server::oltp_replication::service_rpc",
                cache = "invalidated",
                "service exhausted verified replica routes"
            );
        }
    }

    fn route_cache(&self) -> Option<std::sync::MutexGuard<'_, ReplicaRouteCache>> {
        self.replica_routes.as_ref()?.lock().ok()
    }

    async fn reserve_section_decode(
        &self,
        reply: ReceivedReply,
    ) -> Result<ReceivedReply, ProtocolError> {
        let raw = reply.sections[0].as_ref().ok_or_else(unavailable)?.clone();
        let decode = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                crate::oltp::mvcc::json_decode_budget(raw.as_ref())
            })
            .await?;
        reply.reserve_reply_decode(&self.shared, decode).await
    }

    async fn load_replica_members(
        &self,
        record: &GroupRecord,
        count: usize,
    ) -> Result<(Vec<ReplicaMember>, usize), ProtocolError> {
        let mut members = Vec::new();
        members
            .try_reserve_exact(count)
            .map_err(|_| unavailable())?;
        let mut after = None;
        let mut root = super::machine::group_seed();
        let mut pages = 0;
        loop {
            let reply = self.group_members(record.clone(), after).await?;
            let reply = self.reserve_section_decode(reply).await?;
            let page = self
                .shared
                .execute(ExecutionClass::Control, 0, 0, move || {
                    reply.decode_section::<super::service::ReplicaInventoryPage>(0)
                })
                .await?;
            if page.group != *record || (!page.done && page.items.is_empty()) {
                return Err(invalid("replica routing page differs"));
            }
            pages += 1;
            for member in page.items.iter() {
                member
                    .validate()
                    .map_err(|_| invalid("invalid replica routing member"))?;
                if after.is_some_and(|previous| member.node_id <= previous)
                    || members.len() >= count
                {
                    return Err(invalid("replica routing order or count differs"));
                }
                root = super::machine::group_step(root, member);
                after = Some(member.node_id);
                members.push(member.clone());
            }
            if page.after != after {
                return Err(invalid("replica routing cursor differs"));
            }
            if page.done {
                break;
            }
        }
        if members.len() != count
            || super::machine::group_root(record.replica_count, root) != record.replica_root
        {
            return Err(invalid("replica routing root differs"));
        }
        Ok((members, pages))
    }
}
