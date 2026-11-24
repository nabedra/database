//! Checks a surviving RF3 quorum on the actual group leader. Raft heartbeats
//! identify the voter that acknowledged them, unlike a successful ReadIndex.
use super::*;
use crate::{
    oltp::pool::{ByteClass, ChargedBytes},
    oltp_replication::{
        authority::control::{
            ControlHandle, LocalOperatorPermit, ReplacementGroup, ReplacementMountProof,
            ReplacementRecord, SYSTEM_REPLACEMENT_CAPABILITY, SYSTEM_REPLACEMENT_EXECUTION,
            SYSTEM_REPLACEMENT_HARDENING,
        },
        multiplex::{
            AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture,
            ReceivePlan, TrafficClass,
        },
    },
};
use nabedra_oltp_types::queue::{PeerReachability, QueueScope};
use openraft::{Instant as _, type_config::TypeConfigExt as _};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

const HEADER: usize = 4096;
// Fixed reply fields fit in this decode allowance, including the JSON codec's
// base allocation and structural token charges.
const REPLY_DECODE_BYTES: usize = 16 << 10;
const QUORUM_TIMEOUT: Duration = Duration::from_secs(3);
const LOST_HEARTBEAT_MAX_AGE: Duration = Duration::from_secs(3);

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct QuorumRequest {
    pub group: GroupIdentity,
    pub voters: [ReplicaMember; 3],
    pub lost: Uuid,
    pub leader: u64,
    pub minimum: AppliedPosition,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct SurvivingQuorum {
    pub leader: u64,
    pub follower: u64,
    pub applied_at: AppliedPosition,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Quorum(Box<QuorumRequest>),
    Advertise,
    AdvertiseExecution,
    AdvertiseHardening,
    CheckMount {
        proof: ReplacementMountProof,
        retained: bool,
    },
    Mount(ReplacementMountProof),
    Reconcile { id: Uuid, group: Uuid },
}

pub(super) type PositionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AppliedPosition, ProtocolError>> + Send + 'a>>;

pub(super) type QuorumFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SurvivingQuorum, ProtocolError>> + Send + 'a>>;

impl QuorumRequest {
    fn validate(&self, scope: &GroupIdentity) -> Result<(), ProtocolError> {
        self.group.validate().map_err(|_| invalid())?;
        validate_voters(&self.voters).map_err(|_| invalid())?;
        if self.group.cluster_id != scope.cluster_id
            || self.group.database != scope.database
            || !self
                .voters
                .iter()
                .any(|member| member.worker_id == self.lost)
            || !self
                .voters
                .iter()
                .any(|member| member.node_id == self.leader && member.worker_id != self.lost)
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn matches(&self, membership: &openraft::Membership<u64, ReplicaMember>) -> bool {
        let [voters] = membership.get_joint_config().as_slice() else {
            return false;
        };
        voters.len() == self.voters.len()
            && self.voters.iter().all(|member| {
                voters.contains(&member.node_id)
                    && membership.get_node(&member.node_id) == Some(member)
            })
    }

    fn follower(&self) -> Result<u64, ProtocolError> {
        self.voters
            .iter()
            .find(|member| member.node_id != self.leader && member.worker_id != self.lost)
            .map(|member| member.node_id)
            .ok_or_else(invalid)
    }
}

impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(super) async fn check_surviving_quorum(
        &self,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let local = self.state.resident().node_id();
        if self.state.store().identity() != &request.group || local != request.leader {
            return Err(invalid());
        }
        let lost = request
            .voters
            .iter()
            .find(|member| member.worker_id == request.lost)
            .ok_or_else(invalid)?
            .node_id;
        let follower = request.follower()?;
        let start = C::now();
        let (generation, mut metrics) = self.raft.current_metrics();
        let matches_leader = |state: &openraft::RaftMetrics<C>| {
            state.running_state.is_ok()
                && state.current_leader == Some(local)
                && request.matches(state.membership_config.membership())
                && state.membership_config == state.committed_membership_config
        };
        nabedra_rt::time::timeout(QUORUM_TIMEOUT, async {
            let term = {
                let state = metrics.borrow_watched();
                if !matches_leader(&state) {
                    return Err(unavailable());
                }
                state.current_term
            };

            // The read provides the applied barrier. The heartbeat supplies a
            // separate, fresh acknowledgment specifically from the other survivor.
            let applied_at = self.ensure_linearizable().await?.applied;
            if applied_at.index < request.minimum.index {
                return Err(unavailable());
            }
            self.raft
                .trigger()
                .heartbeat()
                .await
                .map_err(|_| unavailable())?;
            loop {
                {
                    let state = metrics.borrow_watched();
                    if !matches_leader(&state)
                        || state.current_term != term
                        || self.raft.current_metrics().0 != generation
                    {
                        return Err(unavailable());
                    }
                    let heartbeat = state.heartbeat.as_ref().ok_or_else(unavailable)?;
                    if heartbeat
                        .get(&lost)
                        .and_then(Option::as_ref)
                        .is_some_and(|last| last.elapsed() < LOST_HEARTBEAT_MAX_AGE)
                    {
                        return Err(reachable());
                    }
                    if heartbeat
                        .get(&follower)
                        .and_then(Option::as_ref)
                        .is_some_and(|last| last.into_inner() >= start)
                    {
                        return Ok(SurvivingQuorum {
                            leader: local,
                            follower,
                            applied_at,
                        });
                    }
                }
                metrics.changed().await.map_err(|_| unavailable())?;
            }
        })
        .await
        .map_err(|_| unavailable())?
    }
}

impl HostedGroupManager {
    pub(crate) fn local_control(
        &self,
        group: &GroupIdentity,
    ) -> Result<ManagedControl, ProtocolError> {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        let control = routes
            .get(&group.group_id)
            .and_then(|route| route.replicas.values().find(|replica| !replica.retiring))
            .ok_or_else(unavailable)?
            .typed
            .clone()
            .downcast::<RaftConsensus<ControlConfig, ControlMachine>>()
            .map_err(|_| invalid())?;
        if control.state.store().identity() != group {
            return Err(invalid());
        }
        Ok(control)
    }

    pub(crate) async fn verify_recovery_hardening(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        self.verify_recovery_advert(
            control,
            target,
            Request::AdvertiseHardening,
            SYSTEM_REPLACEMENT_HARDENING,
        )
        .await
    }

    pub(crate) async fn verify_recovery_execution(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        self.verify_recovery_advert(
            control,
            target,
            Request::AdvertiseExecution,
            SYSTEM_REPLACEMENT_EXECUTION,
        )
        .await
    }

    pub(crate) async fn verify_recovery_capability(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        self.verify_recovery_advert(
            control,
            target,
            Request::Advertise,
            SYSTEM_REPLACEMENT_CAPABILITY,
        )
        .await
    }

    async fn verify_recovery_advert(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
        request: Request,
        capability: &str,
    ) -> Result<(), ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, &request, HEADER)?;
        let reply = self
            .client
            .request_classified(
                control,
                target,
                Purpose::SystemRecovery,
                header,
                [None, None, None],
                REPLY_DECODE_BYTES,
            )
            .await
            .map_err(|_| unsupported())?;
        if reply.remote_identity() != target
            || reply.group() != control
            || reply.purpose() != Purpose::SystemRecovery
            || reply.sections.iter().any(Option::is_some)
            || reply.decode_header::<String>()?.as_str() != capability
        {
            return Err(unsupported());
        }
        Ok(())
    }

    fn replacement_replica(
        &self,
        plan: &ReplacementGroup,
        local: &ReplicaMember,
    ) -> Result<Arc<dyn Running>, ProtocolError> {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        routes
            .get(&plan.expected.identity.group_id)
            .and_then(|routes| routes.replicas.get(&local.node_id))
            .filter(|replica| !replica.retiring && replica.member == *local)
            .map(|replica| replica.running.clone())
            .ok_or_else(unavailable)
    }

    #[doc(hidden)]
    pub async fn replacement_applied(
        &self,
        record: &ReplacementRecord,
    ) -> Result<(), ProtocolError> {
        for plan in &record.groups {
            self.replacement_replica(plan, &plan.learner)?
                .verify_replacement_applied(plan, record.intent.previous.worker_id)
                .await?;
        }
        Ok(())
    }

    async fn reconcile_replacement_local(
        &self,
        plan: &ReplacementGroup,
        lost: Uuid,
    ) -> Result<AppliedPosition, ProtocolError> {
        let local = self.client.local_identity().worker_id;
        let members = plan.next(lost)?;
        let member = members
            .iter()
            .find(|member| member.worker_id == local)
            .ok_or_else(invalid)?;
        self.replacement_replica(plan, member)?
            .reconcile_system_replacement(plan, lost)
            .await
    }

    #[doc(hidden)]
    pub async fn check_replacement_mount(
        &self,
        scope: &GroupIdentity,
        target: WorkerIdentity,
        proof: &ReplacementMountProof,
        retained: bool,
    ) -> Result<bool, ProtocolError> {
        self.recovery_call(
            scope,
            target,
            &Request::CheckMount {
                proof: proof.clone(),
                retained,
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn send_mount_proof(
        &self,
        scope: &GroupIdentity,
        target: WorkerIdentity,
        proof: &ReplacementMountProof,
    ) -> Result<(), ProtocolError> {
        self.recovery_call::<()>(scope, target, &Request::Mount(proof.clone()))
            .await
    }

    async fn recovery_call<T: serde::de::DeserializeOwned + Clone + Send + 'static>(
        &self,
        scope: &GroupIdentity,
        target: WorkerIdentity,
        request: &Request,
    ) -> Result<T, ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER)?;
        let reply = self
            .client
            .request_classified(
                scope,
                target,
                Purpose::SystemRecovery,
                header,
                [None, None, None],
                REPLY_DECODE_BYTES,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != scope
            || reply.purpose() != Purpose::SystemRecovery
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(invalid());
        }
        let decoded = reply.decode_header::<Result<T, ProtocolError>>()?;
        (*decoded).clone()
    }

    #[doc(hidden)]
    pub async fn advance_system_replacement(
        &self,
        control: &ControlHandle,
        running: &ManagedControl,
        permit: &LocalOperatorPermit,
    ) -> Result<(), ProtocolError> {
        if running.raft.current_leader().await != Some(running.state.resident().node_id()) {
            return Ok(());
        }
        let Some((record, remaining)) = control.active_system_replacement().await? else {
            return control.queue_lost_data_repairs(running, permit).await;
        };
        // The candidate stays outside Control elections until the other groups
        // have their stable memberships recorded.
        let plan = record
            .groups
            .iter()
            .filter(|plan| remaining.contains(&plan.expected.identity.group_id))
            .min_by_key(|plan| match plan.expected.identity.kind {
                GroupKind::Data { .. } => 0,
                GroupKind::Control => 2,
                _ => 1,
            })
            .ok_or_else(invalid)?;
        let group = plan.expected.identity.group_id;
        let lost = record.intent.previous.worker_id;
        let local = self.client.local_identity();
        let request = Request::Reconcile {
            id: record.intent.id,
            group,
        };
        let next = plan.next(lost)?;
        let mut last = unavailable();
        for member in next {
            let result = if member.worker_id == local.worker_id {
                self.reconcile_replacement_local(plan, lost).await
            } else {
                let target = control
                    .worker(member.worker_id)
                    .await?
                    .ok_or_else(unavailable)?;
                self.recovery_call(control.identity(), WorkerIdentity::from(&target), &request)
                    .await
            };
            match result {
                Ok(applied) => {
                    return control
                        .publish_system_replacement_group(running, permit, &record, group, applied)
                        .await;
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    #[doc(hidden)]
    pub async fn surviving_quorum(
        &self,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let running = {
            let routes = self.routes.lock().map_err(|_| unavailable())?;
            routes
                .get(&request.group.group_id)
                .and_then(|group| group.replicas.get(&request.leader))
                .filter(|route| !route.retiring && request.voters.contains(&route.member))
                .ok_or_else(unavailable)?
                .running
                .clone()
        };
        running.surviving_quorum(request).await
    }

    #[doc(hidden)]
    pub async fn remote_surviving_quorum(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &Request::Quorum(Box::new(request.clone())),
            HEADER,
        )?;
        let reply = self
            .client
            .request_classified(
                control,
                target,
                Purpose::SystemRecovery,
                header,
                [None, None, None],
                REPLY_DECODE_BYTES,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != control
            || reply.purpose() != Purpose::SystemRecovery
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(invalid());
        }
        let proof = reply.decode_header::<SurvivingQuorum>()?;
        if proof.leader != request.leader
            || proof.follower != request.follower()?
            || proof.applied_at.index < request.minimum.index
        {
            return Err(invalid());
        }
        Ok(*proof)
    }
}

pub(crate) type ProbeQueuePeer = fn(
    &HostedGroupManager,
    &GroupIdentity,
    WorkerIdentity,
    QueueScope,
    ReadyMembership,
) -> EndpointFuture<'static, PeerReachability>;

#[doc(hidden)]
pub struct QuorumEndpoint {
    pub manager: Arc<HostedGroupManager>,
    pub control: Arc<ControlHandle>,
    pub probe_queue_peer: ProbeQueuePeer,
}
impl QuorumEndpoint {
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<Request, ProtocolError> {
        if session.group() != self.control.identity()
            || session.purpose() != Purpose::SystemRecovery
            || session.recovery_only()
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "system recovery requires a registered operator peer",
                Outcome::NotApplied,
            ));
        }
        crate::protocol::json_input(header, HEADER).map_err(|_| invalid())?;
        let request: Request = serde_json::from_slice(header).map_err(|_| invalid())?;
        match &request {
            Request::Mount(proof) | Request::CheckMount { proof, .. }
                if proof.worker == session.remote() => {}
            Request::Mount(_) | Request::CheckMount { .. } => return Err(invalid()),
            Request::Advertise | Request::AdvertiseExecution | Request::AdvertiseHardening => {}
            Request::Quorum(request) if self.control.is_operator(&session.remote()) => {
                request.validate(self.control.identity())?;
            }
            Request::Reconcile { .. } if self.control.is_operator(&session.remote()) => {}
            _ => return Err(invalid()),
        }
        Ok(request)
    }
}
impl QuorumEndpoint {
    async fn mount(&self, proof: ReplacementMountProof) -> Result<(), ProtocolError> {
        let control = &self.control;
        let running = self.manager.local_control(control.identity())?;
        if running.raft.current_leader().await != Some(running.state.resident().node_id()) {
            return Err(unavailable());
        }
        let (record, mounted, _, _) = control
            .replacement_record_local(proof.id)
            .await?
            .ok_or_else(unavailable)?;
        if proof.worker != WorkerIdentity::from(&record.intent.next) {
            return Err(invalid());
        }
        if control.replacement_hardening_active().await? {
            if mounted {
                return Err(mounted_conflict());
            }
            self.manager
                .verify_recovery_hardening(control.identity(), proof.worker)
                .await?;
        } else {
            self.manager
                .verify_recovery_execution(control.identity(), proof.worker)
                .await?;
        }
        let state = control.inspect().await?.ok_or_else(unavailable)?;
        if !mounted {
            // Check liveness again just before the transaction that revokes
            // the original identity. Never treat a failed authentication as loss.
            let scope = QueueScope {
                cluster_id: control.identity().cluster_id,
                database: control.identity().database.clone(),
            };
            let lost = record.intent.previous;
            match (self.probe_queue_peer)(&self.manager, control.identity(), lost, scope, state.ready)
                .await?
            {
                PeerReachability::Unreachable => {}
                PeerReachability::Reachable => return Err(reachable()),
                PeerReachability::Indeterminate => return Err(unavailable()),
            }
            for plan in &record.groups {
                let mut checked = false;
                for survivor in plan
                    .old
                    .iter()
                    .filter(|member| member.worker_id != lost.worker_id)
                {
                    let request = QuorumRequest {
                        group: plan.expected.identity.clone(),
                        voters: plan.old.clone(),
                        lost: lost.worker_id,
                        leader: survivor.node_id,
                        minimum: plan.expected.membership_position,
                    };
                    let result =
                        if survivor.worker_id == self.manager.client.local_identity().worker_id {
                            self.manager.surviving_quorum(&request).await
                        } else {
                            let enrollment = control
                                .worker(survivor.worker_id)
                                .await?
                                .ok_or_else(unavailable)?;
                            self.manager
                                .remote_surviving_quorum(
                                    control.identity(),
                                    WorkerIdentity::from(&enrollment),
                                    &request,
                                )
                                .await
                        };
                    match result {
                        Ok(_) => {
                            checked = true;
                            break;
                        }
                        Err(error) if error.code == ErrorCode::Conflict => return Err(error),
                        Err(_) => {}
                    }
                }
                if !checked {
                    return Err(unavailable());
                }
            }
        }
        control
            .mount_system_replacement(&running, proof, &state)
            .await
    }

    async fn check_mount(
        &self,
        proof: ReplacementMountProof,
        retained: bool,
    ) -> Result<bool, ProtocolError> {
        let control = &self.control;
        let mounted = control.check_replacement_mount(&proof).await?;
        if mounted && !retained {
            return Err(mounted_conflict());
        }
        Ok(mounted)
    }

    async fn reconcile(&self, id: Uuid, group: Uuid) -> Result<AppliedPosition, ProtocolError> {
        let (record, mounted, _, _) = self
            .control
            .replacement_record_local(id)
            .await?
            .ok_or_else(unavailable)?;
        if !mounted {
            return Err(unavailable());
        }
        let plan = record
            .groups
            .iter()
            .find(|plan| plan.expected.identity.group_id == group)
            .ok_or_else(invalid)?;
        self.manager
            .reconcile_replacement_local(plan, record.intent.previous.worker_id)
            .await
    }
}

impl Endpoint for QuorumEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            Ok(ReceivePlan {
                class: TrafficClass::Settlement,
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
            let request = self.parse(&session, header.as_slice())?;
            if (0..3).any(|index| sections.section(index).is_some()) {
                return Err(invalid());
            }
            let header = match request {
                Request::Quorum(request) => {
                    let proof = self.manager.surviving_quorum(&request).await?;
                    self.manager
                        .shared
                        .encode_in(ByteClass::Progress, &proof, HEADER)?
                }
                Request::Mount(proof) => self.manager.shared.encode_in(
                    ByteClass::Progress,
                    &self.mount(proof).await,
                    HEADER,
                )?,
                Request::Reconcile { id, group } => self.manager.shared.encode_in(
                    ByteClass::Progress,
                    &self.reconcile(id, group).await,
                    HEADER,
                )?,
                Request::CheckMount { proof, retained } => self.manager.shared.encode_in(
                    ByteClass::Progress,
                    &self.check_mount(proof, retained).await,
                    HEADER,
                )?,
                Request::Advertise | Request::AdvertiseExecution | Request::AdvertiseHardening => {
                    capability_reply(&self.manager.shared, &request)?
                }
            };
            Ok(ChargedReply {
                header,
                sections: [None, None, None],
            })
        })
    }
}

#[doc(hidden)]
pub struct ReplacementCapabilityEndpoint {
    pub group: GroupIdentity,
    pub shared: Arc<WorkerSharedResources>,
}

impl ReplacementCapabilityEndpoint {
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<Request, ProtocolError> {
        if session.group() != &self.group || session.purpose() != Purpose::SystemRecovery {
            return Err(invalid());
        }
        match serde_json::from_slice::<Request>(header).map_err(|_| invalid())? {
            request @ (Request::Advertise
            | Request::AdvertiseExecution
            | Request::AdvertiseHardening) => Ok(request),
            _ => Err(invalid()),
        }
    }
}

impl Endpoint for ReplacementCapabilityEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            Ok(ReceivePlan {
                class: TrafficClass::Settlement,
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
            let request = self.parse(&session, header.as_slice())?;
            if (0..3).any(|index| sections.section(index).is_some()) {
                return Err(invalid());
            }
            Ok(ChargedReply {
                header: capability_reply(&self.shared, &request)?,
                sections: [None, None, None],
            })
        })
    }
}

fn capability_reply(
    shared: &WorkerSharedResources,
    request: &Request,
) -> Result<ChargedBytes, ProtocolError> {
    #[cfg(any(test, feature = "test-hooks"))]
    if std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|path| {
        std::path::Path::new(&path)
            .join("hold-system-replacement-advert")
            .exists()
    }) {
        return Err(unsupported());
    }
    let capability = match request {
        Request::Advertise => SYSTEM_REPLACEMENT_CAPABILITY,
        Request::AdvertiseExecution => SYSTEM_REPLACEMENT_EXECUTION,
        Request::AdvertiseHardening => SYSTEM_REPLACEMENT_HARDENING,
        _ => return Err(invalid()),
    };
    shared.encode_in(ByteClass::Progress, &capability, HEADER)
}

fn mounted_conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "replacement mount already committed",
        Outcome::NotApplied,
    )
}

#[doc(hidden)]
pub fn reachable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "lost worker is still reachable",
        Outcome::NotApplied,
    )
}

fn unsupported() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        "every current Control voter must advertise control.system-replacement.v1",
        Outcome::NotApplied,
    )
}
