//! The native mount owner accepts bounded placement requests for the exact
//! transition recorded by Control.
use super::*;
use crate::{
    oltp::pool::{
        PoolCloseOutcome, VerifiedPoolClosed, VerifiedReplicaActivation,
        mount::{MountedPoolObservation, PoolMountProfile},
    },
    oltp_replication::{
        authority::{
            control::{
                DataWorkerIntent, RemoteActivationAttestation, RemotePlacementReply,
                RemoteRetirementAttestation, RestartMountKey, VerifiedRemoteActivation,
                VerifiedRemoteRetirement, WorkerCloseAttestation, WorkerDrainPhase,
                WorkerDrainRecord, decode_remote_placement_reply,
            },
            status::StatusTopology,
            timestamp::split::SplitVoter,
        },
        manager::ReplicaRetired,
        owned::serialized_digest,
    },
};
use tokio::{
    sync::{Mutex, watch},
    time::MissedTickBehavior,
};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Action {
    CheckUnused,
    HostLearner,
    LearnerApplied { minimum: AppliedPosition },
    StableMembership,
    Activate { minimum: AppliedPosition },
    RetireSource,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    id: Uuid,
    binding: Digest,
    pub(super) action: Action,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum PlacementRequest {
    Move(Request),
    Vote(VoteRequest),
}

pub(super) fn parse(
    control: &GroupIdentity,
    session: &AuthenticatedSession,
    bytes: &[u8],
) -> Result<PlacementRequest, ProtocolError> {
    if session.group() != control
        || session.purpose() != Purpose::ReplicaPlacement
        || session.recovery_only()
    {
        return Err(unauthorized());
    }
    crate::protocol::json_input(bytes, HEADER).map_err(|_| invalid())?;
    let request = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if match &request {
        PlacementRequest::Move(request) => {
            request.id.is_nil() || request.binding.as_bytes() == &[0; 32]
        }
        PlacementRequest::Vote(request) => request.id.is_nil() || request.node == 0,
    } {
        return Err(invalid());
    }
    Ok(request)
}

impl ReplicaPlacement {
    pub(super) async fn remote_action<T: serde::de::DeserializeOwned + Clone>(
        &self,
        plan: &ReplicaMove,
        action: Action,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let (reply, request) = self.remote_request(plan, plan.request.worker, action).await?;
        let value = decode_remote_placement_reply(
            &reply,
            self.factory.control.identity(),
            plan.request.worker,
            request,
        )?;
        Ok((value, reply.scratch_lease()))
    }

    pub(super) async fn remote_activation(
        &self,
        plan: &ReplicaMove,
        minimum: AppliedPosition,
    ) -> Result<VerifiedRemoteActivation, ProtocolError> {
        let (domains, key) = self
            .factory
            .control
            .remote_placement_binding(
                plan.request.clone(),
                plan.authority,
                plan.old.clone(),
                plan.next.clone(),
            )
            .await?;
        if Some(domains) != plan.failure_domains {
            return Err(unauthorized());
        }
        let (reply, request) = self
            .remote_request(plan, plan.request.worker, Action::Activate { minimum })
            .await?;
        VerifiedRemoteActivation::from_authenticated(
            &reply,
            self.factory.control.identity(),
            plan,
            minimum,
            request,
            key,
        )
    }

    pub(super) async fn remote_retirement(
        &self,
        plan: &ReplicaMove,
    ) -> Result<VerifiedRemoteRetirement, ProtocolError> {
        let (reply, request) = self.remote_request(plan, plan.source_worker(), Action::RetireSource).await?;
        VerifiedRemoteRetirement::from_authenticated(
            &reply,
            self.factory.control.identity(),
            plan,
            request,
        )
    }

    pub(super) async fn retire_bootstrap(
        &self,
        session: &AuthenticatedSession,
        request: &Request,
    ) -> Result<ChargedReply, ProtocolError> {
        let (generation, key) = self.restart_mount.as_ref().ok_or_else(unauthorized)?;
        let view = self.factory.rpc.remote_replica_move(request.id).await?;
        let plan = &view.plan;
        let source = plan.remote_source.as_ref().ok_or_else(unauthorized)?;
        let removed = plan.removed().ok_or_else(unauthorized)?;
        let manager = &self.factory.manager;
        let pool = &self.factory.pool;
        if !view.eligible
            || plan.owner != session.remote()
            || !plan.remote_target()
            || plan.binding()? != request.binding
            || source.worker != manager.client().local_identity()
            || source.mount_public_key != key.public_key()
            || source.pool_generation != *generation
            || removed.pool_id != pool.identity().pool_id
            || !matches!(plan.phase, ReplicaMovePhase::Published { .. })
        {
            return Err(unauthorized());
        }
        let result = async {
            let _settlement = self.settle.try_lock().map_err(|_| busy())?;
            let permit =
                ReplicaRetirementPermit::from_remote(self.factory.rpc.clone(), plan).await?;
            let observed = manager
                .resources()
                .execute(ExecutionClass::Control, 0, 0, {
                    let pool = pool.clone();
                    move || pool.observe_registered_mount()
                })
                .await?;
            if !matches!(observed.expected.profile, PoolMountProfile::Docker { .. })
                || &observed.expected.identity != pool.identity()
                || observed.expected.worker != source.worker
                || observed.expected.generation != source.pool_generation
            {
                return Err(unauthorized());
            }
            let retired =
                retire_native_source(manager, pool, plan, permit, &self.factory.config).await?;
            self.factory
                .wired
                .lock()
                .map_err(|_| unavailable())?
                .remove(&(plan.request.group.group_id, removed.node_id));
            retirement_attestation(self.factory.control.identity(), plan, &retired, key)
        }
        .await;
        reply(
            manager,
            session.nonce(),
            serialized_digest(request)?,
            result,
        )
    }

    async fn remote_request(
        &self,
        plan: &ReplicaMove,
        worker: WorkerIdentity,
        action: Action,
    ) -> Result<(ReceivedReply, Digest), ProtocolError> {
        remote_request(&self.factory.manager, self.factory.control.identity(), plan, worker, action).await
    }
}

async fn remote_request(
    manager: &HostedGroupManager,
    control: &GroupIdentity,
    plan: &ReplicaMove,
    worker: WorkerIdentity,
    action: Action,
) -> Result<(ReceivedReply, Digest), ProtocolError> {
    let request = Request { id: plan.id, binding: plan.binding()?, action };
    let digest = serialized_digest(&request)?;
    let header = manager.resources().encode_in(ByteClass::Progress, &request, HEADER)?;
    let reply = manager.client().request_classified(
        control, worker, Purpose::ReplicaPlacement, header, [None, None, None], DECODE,
    ).await.map_err(ProtocolError::from)?;
    Ok((reply, digest))
}

struct HeldActivation {
    id: Uuid,
    binding: Digest,
    native: VerifiedReplicaActivation,
    attestation: RemoteActivationAttestation,
}

#[derive(Default)]
struct TargetState {
    closing: bool,
    activation: Option<HeldActivation>,
    closed: Option<VerifiedPoolClosed>,
    closed_added: Option<VerifiedPoolClosed>,
    split_services: std::collections::BTreeSet<(Uuid, u64)>,
}

#[doc(hidden)]
pub struct RemotePlacementTarget {
    manager: Arc<HostedGroupManager>,
    control: Arc<ControlClient>,
    control_group: GroupIdentity,
    pool: std::sync::Mutex<Option<OltpPool>>,
    added: Option<(std::sync::Mutex<Option<OltpPool>>, MountedPoolObservation)>,
    intent: DataWorkerIntent,
    mounted_bytes: u64,
    key: RestartMountKey,
    topology: StatusTopology,
    config: openraft::Config,
    state: Mutex<TargetState>,
}

impl RemotePlacementTarget {
    #[doc(hidden)]
    pub fn new(
        manager: Arc<HostedGroupManager>,
        control: Arc<ControlClient>,
        control_group: GroupIdentity,
        pool: OltpPool,
        intent: DataWorkerIntent,
        mounted_bytes: u64,
        added: Option<(OltpPool, MountedPoolObservation)>,
        key: RestartMountKey,
        topology: StatusTopology,
        config: openraft::Config,
    ) -> Result<Arc<Self>, ProtocolError> {
        intent.validate(&control_group)?;
        if control_group.kind != GroupKind::Control
            || key.public_key() != intent.mount_public_key
            || *pool.identity() != intent.mount.identity
            || manager.client().local_identity() != WorkerIdentity::from(&intent.enrollment)
            || !Arc::ptr_eq(manager.resources(), pool.resources())
            || added.as_ref().is_some_and(|(pool, mount)| {
                *pool.identity() != mount.expected.identity
                    || pool.identity().pool_id == intent.mount.identity.pool_id
                    || mount.expected.worker != WorkerIdentity::from(&intent.enrollment)
                    || !Arc::ptr_eq(manager.resources(), pool.resources())
            })
        {
            return Err(unauthorized());
        }
        Ok(Arc::new(Self {
            manager,
            control,
            control_group,
            pool: std::sync::Mutex::new(Some(pool)),
            added: added.map(|(pool, mount)| (std::sync::Mutex::new(Some(pool)), mount)),
            intent,
            mounted_bytes,
            key,
            topology,
            config,
            state: Mutex::new(TargetState::default()),
        }))
    }

    // This task belongs to the process. Requests can disappear while it holds
    // the generation lease, including after proof bytes reach the proposer.
    #[doc(hidden)]
    pub async fn settle(&self, mut stop: watch::Receiver<bool>) {
        let mut interval = nabedra_rt::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let activation = {
                let mut state = self.state.lock().await;
                state.closing |= *stop.borrow_and_update();
                if state.closing && state.activation.is_none() {
                    return;
                }
                state
                    .activation
                    .as_ref()
                    .map(|held| (held.id, held.binding))
            };
            if let Some((id, binding)) = activation {
                if let Ok(Ok(view)) = nabedra_rt::time::timeout(
                    Duration::from_secs(3),
                    self.control.remote_replica_move(id),
                )
                .await
                {
                    let mut state = self.state.lock().await;
                    if state.activation.as_ref().is_some_and(|held| {
                        held.id == id && held.binding == binding && settles(held, &view)
                    }) {
                        if matches!(
                            view.plan.phase,
                            ReplicaMovePhase::Published { .. } | ReplicaMovePhase::Retired { .. }
                        ) {
                            let group = &view.plan.request.group;
                            let node = view.plan.request.target.node_id;
                            if let Err(error) =
                                self.manager.hosted_data(group, node).and_then(|running| {
                                    wire_participant(
                                        &self.manager,
                                        &self.control,
                                        &self.topology,
                                        group,
                                        node,
                                        running,
                                    )
                                })
                            {
                                tracing::warn!(target: "nabedra_database_server::oltp_replication::manager::data_factory::placement::remote", move_id=%id, ?error.code, "could not enable the published Data replica");
                                drop(state);
                                interval.tick().await;
                                continue;
                            }
                        }
                        state.activation = None;
                        #[cfg(any(test, feature = "test-hooks"))]
                        test_settled(id, &view.plan.phase);
                    }
                }
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = stop.changed(), if !*stop.borrow() => {}
            }
        }
    }

    #[doc(hidden)]
    pub async fn shutdown(&self) -> Result<(), ProtocolError> {
        self.manager.shutdown().await
    }

    async fn plan(
        &self,
        session: &AuthenticatedSession,
        request: &Request,
    ) -> Result<Decoded<crate::oltp_replication::authority::control::RemoteMoveView>, ProtocolError>
    {
        let view = self.control.remote_replica_move(request.id).await?;
        let plan = &view.plan;
        let local = WorkerIdentity::from(&self.intent.enrollment);
        let correct_mount = match request.action {
            Action::RetireSource => plan.remote_source.as_ref().is_some_and(|source| {
                source.worker == local
                    && source.mount_public_key == self.key.public_key()
                    && plan.removed().is_some_and(|member| self.generation(member.pool_id) == Some(source.pool_generation))
                    && matches!(plan.phase, ReplicaMovePhase::Published { .. })
            }),
            _ => plan.request.worker == local
                && self.generation(plan.request.target.pool_id) == Some(plan.request.pool_generation),
        };
        if !view.eligible
            || plan.binding()? != request.binding
            || !correct_mount
            || plan.authority.run_id != self.intent.run_id
        {
            return Err(unauthorized());
        }
        if matches!(request.action, Action::LearnerApplied { .. }) {
            let peer = session.remote();
            let node = plan.old.iter().find(|member| member.worker_id == peer.worker_id)
                .ok_or_else(unauthorized)?.node_id;
            let voter = self.control.remote_replica_voter(plan.id, node).await?;
            if voter.1 != peer || voter.0.binding()? != request.binding
                || voter.0.phase != ReplicaMovePhase::Moving
            {
                return Err(unauthorized());
            }
        } else if plan.owner != session.remote() {
            return Err(unauthorized());
        }
        Ok(view)
    }

    async fn vote(
        &self,
        session: &AuthenticatedSession,
        request: &VoteRequest,
    ) -> Result<Membership, ProtocolError> {
        let local = self.manager.client().local_identity();
        let current = self.control.remote_replica_voter(request.id, request.node).await?;
        let (plan, voter) = &*current;
        if *voter != local || plan.owner != session.remote() || plan.authority.run_id != self.intent.run_id {
            return Err(unauthorized());
        }
        let target_applied = |minimum| -> Pin<
            Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>,
        > {
            Box::pin(async move {
                if plan.request.worker == local {
                    self.learner_applied(plan, minimum).await
                } else {
                    let (reply, request) = remote_request(
                        &self.manager, &self.control_group, plan, plan.request.worker,
                        Action::LearnerApplied { minimum },
                    ).await?;
                    decode_remote_placement_reply::<()>(&reply, &self.control_group, plan.request.worker, request)
                }
            })
        };
        let target_applied: &PlacementCatchUp<'_> = &target_applied;
        let observed = native_vote(&self.manager, plan, request.node, plan.remote_target().then_some(target_applied)).await?;
        Ok(Membership {
            group: observed.group,
            replicas: observed.replicas,
            applied_at: observed.applied_at,
        })
    }

    fn live_pool(&self) -> Result<OltpPool, ProtocolError> {
        Self::read_pool(&self.pool)
    }

    fn read_pool(slot: &std::sync::Mutex<Option<OltpPool>>) -> Result<OltpPool, ProtocolError> {
        slot.lock().map_err(|_| unavailable())?.clone().ok_or_else(unavailable)
    }

    #[doc(hidden)]
    pub async fn readiness_mount(&self) -> Result<MountedPoolObservation, ProtocolError> {
        let pool = self.live_pool()?;
        self.observe(pool, &self.intent.mount, self.mounted_bytes).await
    }

    #[doc(hidden)]
    pub async fn readiness_added_mount(&self) -> Result<Option<MountedPoolObservation>, ProtocolError> {
        match &self.added {
            Some((slot, mount)) => Ok(Some(self.observe(Self::read_pool(slot)?, &mount.expected, mount.total_bytes).await?)),
            None => Ok(None),
        }
    }

    fn generation(&self, pool: Uuid) -> Option<u64> {
        if pool == self.intent.mount.identity.pool_id {
            Some(self.intent.mount.generation)
        } else {
            self.added.as_ref().filter(|(_, mount)| mount.expected.identity.pool_id == pool)
                .map(|(_, mount)| mount.expected.generation)
        }
    }

    async fn observe(&self, pool: OltpPool, expected: &crate::oltp::pool::mount::PoolMountExpectation, total: u64)
        -> Result<MountedPoolObservation, ProtocolError>
    {
        let mount = self
            .manager
            .resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.observe_registered_mount()
            })
            .await?;
        if &mount.expected != expected || mount.total_bytes < total {
            return Err(unavailable());
        }
        Ok(mount)
    }

    async fn writable_pool(&self, id: Uuid) -> Result<OltpPool, ProtocolError> {
        Ok(self.writable_mount(id).await?.0)
    }

    async fn writable_mount(&self, id: Uuid) -> Result<(OltpPool, MountedPoolObservation), ProtocolError> {
        let (pool, mount) = if id == self.intent.mount.identity.pool_id {
            let pool = self.live_pool()?;
            let mount = self.observe(pool.clone(), &self.intent.mount, self.mounted_bytes).await?;
            (pool, mount)
        } else {
            let (slot, mount) = self.added.as_ref().filter(|(_, mount)| mount.expected.identity.pool_id == id)
                .ok_or_else(unauthorized)?;
            let pool = Self::read_pool(slot)?;
            (pool.clone(), self.observe(pool.clone(), &mount.expected, mount.total_bytes).await?)
        };
        if mount.available_bytes == 0 { return Err(unavailable()); }
        Ok((pool, mount))
    }

    pub(in crate::oltp_replication::manager::data_factory) async fn split_pool(
        &self,
        voter: &SplitVoter,
    ) -> Result<(OltpPool, MountedPoolObservation), ProtocolError> {
        let generation = self.generation(voter.member.pool_id).ok_or_else(unauthorized)?;
        if voter.worker != WorkerIdentity::from(&self.intent.enrollment)
            || voter.registered_pool_generation != Some(generation)
        {
            return Err(unauthorized());
        }
        self.writable_mount(voter.member.pool_id).await
    }

    pub(in crate::oltp_replication::manager::data_factory) async fn host_split(
        &self,
        group: &GroupIdentity,
        voter: &SplitVoter,
        voters: &[ReplicaMember],
    ) -> Result<(), ProtocolError> {
        let mut state = self.state.lock().await;
        if state.closing {
            return Err(unavailable());
        }
        let (pool, _) = self.split_pool(voter).await?;
        let local = &voter.member;
        let running = start_data_group(
            &self.manager, &self.config, &pool, group, local, voters, true,
        ).await?;
        if !state.split_services.contains(&(group.group_id, local.node_id)) {
            wire_participant(&self.manager, &self.control, &self.topology, group, local.node_id, running)?;
            state.split_services.insert((group.group_id, local.node_id));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn close_drained_worker(
        &self, record: &WorkerDrainRecord,
    ) -> Result<WorkerCloseAttestation, ProtocolError> {
        if !matches!(record.phase, WorkerDrainPhase::QueueRemoved { .. })
            || record.intent.worker != WorkerIdentity::from(&self.intent.enrollment)
            || record.intent.run_id != self.intent.run_id
            || record.growth_claim != self.intent.claim_id
            || record.retained_eks_pool.as_ref().map(|retained| &retained.pool.mount)
                != self.added.as_ref().map(|(_, mount)| &mount.expected)
        {
            return Err(unauthorized());
        }
        let mut state = self.state.lock().await;
        if state.activation.is_some() { return Err(busy()); }
        state.closing = true;

        // Before transferring either handle, check every remaining native
        // pool's retired groups against the exact Control acknowledgements.
        let added = self.added.as_ref().map(|(slot, _)| slot);
        for slot in [
            state.closed.is_none().then_some(&self.pool),
            added.filter(|_| state.closed_added.is_none()),
        ].into_iter().flatten() {
            Self::read_pool(slot)?
                .check_retired_groups(|proof| self.control.verify_pool_replica_retired(proof))
                .await?;
        }
        if state.closed.is_none() {
            state.closed = Some(self.close_pool(&self.pool, record.id).await?);
        }
        if let Some(slot) = added.filter(|_| state.closed_added.is_none()) {
            state.closed_added = Some(self.close_pool(slot, record.id).await?);
        }
        WorkerCloseAttestation::from_native(
            &self.control_group,
            record,
            state.closed.as_ref().ok_or_else(unavailable)?,
            state.closed_added.as_ref(),
            &self.key,
        )
    }

    async fn close_pool(
        &self, slot: &std::sync::Mutex<Option<OltpPool>>, id: Uuid,
    ) -> Result<VerifiedPoolClosed, ProtocolError> {
        let prepared = self.manager.resources().prepare(ExecutionClass::Control).await?;
        let compute = prepared.reserve_compute(0, 0).await?;
        let pool = slot.lock().map_err(|_| unavailable())?.take().ok_or_else(unavailable)?;
        match pool.close_with_native(id, prepared, compute).await? {
            PoolCloseOutcome::Closed(verified) => Ok(verified),
            PoolCloseOutcome::StillOwned(pool) => {
                *slot.lock().map_err(|_| unavailable())? = Some(pool);
                Err(conflict())
            }
            PoolCloseOutcome::Failed { pool, error } => {
                *slot.lock().map_err(|_| unavailable())? = Some(pool);
                Err(error)
            }
        }
    }

    async fn host(&self, plan: &ReplicaMove) -> Result<(), ProtocolError> {
        let state = self.state.lock().await;
        if state.closing || plan.phase != ReplicaMovePhase::Moving {
            return Err(conflict());
        }
        let store = self
            .writable_pool(plan.request.target.pool_id).await?
            .placement_learner(plan.request.group.clone(), plan.binding()?)
            .await?;
        self.manager
            .start_placement_learner(store, plan, self.config.clone())
            .await?;
        Ok(())
    }

    async fn learner_applied(
        &self,
        plan: &ReplicaMove,
        minimum: AppliedPosition,
    ) -> Result<(), ProtocolError> {
        if plan.phase != ReplicaMovePhase::Moving
            || minimum.index <= plan.expected.membership_position.index
        {
            return Err(conflict());
        }
        self.writable_pool(plan.request.target.pool_id).await?;
        let running = self
            .manager
            .hosted_data(&plan.request.group, plan.request.target.node_id)?;
        let deadline = nabedra_rt::time::now() + Duration::from_secs(5);
        loop {
            match running
                .applied_placement(
                    &plan.request.group,
                    &plan.old,
                    &plan.request.target,
                    minimum,
                    false,
                )
                .await
            {
                Ok(_) => {
                    #[cfg(any(test, feature = "test-hooks"))]
                    test_record("learner", plan.id, &minimum);
                    return Ok(());
                }
                Err(error) if retryable(&error) && nabedra_rt::time::now() < deadline => {
                    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn activate(
        &self,
        plan: &ReplicaMove,
        minimum: AppliedPosition,
    ) -> Result<RemoteActivationAttestation, ProtocolError> {
        if plan.phase != ReplicaMovePhase::Moving
            || minimum.index <= plan.expected.membership_position.index
        {
            return Err(conflict());
        }
        self.writable_pool(plan.request.target.pool_id).await?;
        let binding = plan.binding()?;
        let mut state = self.state.lock().await;
        if let Some(held) = &state.activation {
            if held.id != plan.id
                || held.binding != binding
                || held.native.statement().minimum != minimum
            {
                return Err(busy());
            }
            return Ok(held.attestation.clone());
        }
        if state.closing {
            return Err(unavailable());
        }
        let current = self.control.remote_replica_move(plan.id).await?;
        if !current.eligible
            || current.plan.phase != ReplicaMovePhase::Moving
            || current.plan.binding()? != binding
        {
            return Err(conflict());
        }
        let group = &plan.request.group;
        let target = &plan.request.target;
        let running = self.manager.hosted_data(group, target.node_id)?;
        let applied = running
            .applied_placement(group, &plan.next, target, minimum, true)
            .await?;
        let native = running
            .state
            .store()
            .replica_activation_applied(&applied)
            .await?;
        let attestation = RemoteActivationAttestation::from_native(
            &self.control_group,
            plan,
            &native,
            &self.key,
        )?;
        #[cfg(any(test, feature = "test-hooks"))]
        test_record("activation", plan.id, native.statement());
        state.activation = Some(HeldActivation {
            id: plan.id,
            binding,
            native,
            attestation: attestation.clone(),
        });
        #[cfg(any(test, feature = "test-hooks"))]
        if test_dir().is_some_and(|directory| {
            std::fs::remove_file(directory.join(format!("move-lose-target-activation-{}", plan.id)))
                .is_ok()
        }) {
            return Err(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "target activation reply was lost",
                Outcome::Unknown,
            ));
        }
        Ok(attestation)
    }

    async fn stable(&self, plan: &ReplicaMove) -> Result<Membership, ProtocolError> {
        if !matches!(
            plan.phase,
            ReplicaMovePhase::Moving | ReplicaMovePhase::Published { .. }
        ) {
            return Err(conflict());
        }
        let running = self
            .manager
            .hosted_data(&plan.request.group, plan.request.target.node_id)?;
        let observed = running.observe_stable_membership().await?;
        if observed.replicas() != plan.next {
            return Err(unavailable());
        }
        Ok(Membership {
            group: observed.group,
            replicas: observed.replicas,
            applied_at: observed.applied_at,
        })
    }

    async fn retire_source(&self, plan: &ReplicaMove) -> Result<RemoteRetirementAttestation, ProtocolError> {
        let mut state = self.state.lock().await;
        if state.activation.as_ref().is_some_and(|held| held.id == plan.id) {
            return Err(unavailable());
        }
        if state.closing || state.activation.is_some() {
            return Err(busy());
        }
        let permit = ReplicaRetirementPermit::from_remote(self.control.clone(), plan).await?;
        let removed = plan.removed().ok_or_else(invalid)?;
        let pool = if removed.pool_id == self.intent.mount.identity.pool_id {
            self.live_pool()?
        } else {
            self.added.as_ref().filter(|(_, mount)| mount.expected.identity.pool_id == removed.pool_id)
                .ok_or_else(unauthorized).and_then(|(slot, _)| Self::read_pool(slot))?
        };
        let retired = retire_native_source(&self.manager, &pool, plan, permit, &self.config).await?;
        state.split_services.remove(&(plan.request.group.group_id, removed.node_id));
        retirement_attestation(&self.control_group, plan, &retired, &self.key)
    }
}

async fn retire_native_source(
    manager: &HostedGroupManager,
    pool: &OltpPool,
    plan: &ReplicaMove,
    permit: ReplicaRetirementPermit,
    config: &openraft::Config,
) -> Result<ReplicaRetired, ProtocolError> {
    let removed = plan.removed().ok_or_else(invalid)?;
    let store = pool.retained_group(plan.request.group.clone()).await?;
    if store.replica_retirement(plan.id).await?.is_none()
        && manager
            .hosted_data(&plan.request.group, removed.node_id)
            .is_err()
    {
        // Reopen only the retained group to recover an interrupted closure.
        manager
            .start_data(
                store.clone(),
                removed.clone(),
                plan.old
                    .iter()
                    .map(|member| (member.node_id, member.clone()))
                    .collect(),
                config.clone(),
            )
            .await?;
    }
    manager.retire_removed_replica(permit, &store).await
}

fn retirement_attestation(
    control: &GroupIdentity,
    plan: &ReplicaMove,
    retired: &ReplicaRetired,
    key: &RestartMountKey,
) -> Result<RemoteRetirementAttestation, ProtocolError> {
    let attestation = RemoteRetirementAttestation::from_native(control, plan, retired, key)?;
    #[cfg(any(test, feature = "test-hooks"))]
    {
        test_record("retirement", plan.id, &attestation);
        if test_dir().is_some_and(|directory| {
            std::fs::remove_file(directory.join(format!("move-lose-retirement-{}", plan.id)))
                .is_ok()
        }) {
            return Err(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "placement retirement acknowledgement was lost",
                Outcome::Unknown,
            ));
        }
    }
    Ok(attestation)
}

fn reply<T: Serialize>(
    manager: &HostedGroupManager,
    nonce: Uuid,
    request: Digest,
    result: Result<T, ProtocolError>,
) -> Result<ChargedReply, ProtocolError> {
    let header = manager.resources().encode_in(
        ByteClass::Progress,
        &RemotePlacementReply {
            nonce,
            request,
            result,
        },
        HEADER,
    )?;
    Ok(ChargedReply {
        header,
        sections: [None, None, None],
    })
}

fn settles(
    held: &HeldActivation,
    view: &crate::oltp_replication::authority::control::RemoteMoveView,
) -> bool {
    if view.plan.binding().ok() != Some(held.binding) {
        return false;
    }
    match &view.plan.phase {
        ReplicaMovePhase::Published {
            activated_generation,
            activated_at,
            activation_lineage,
            ..
        }
        | ReplicaMovePhase::Retired {
            activated_generation,
            activated_at,
            activation_lineage,
            ..
        } => {
            let native = held.native.statement();
            *activated_generation == native.active_generation
                && *activated_at == native.applied
                && *activation_lineage == native.lineage_digest
        }
        ReplicaMovePhase::Cancelled => true,
        ReplicaMovePhase::Claimed | ReplicaMovePhase::Moving => view.fenced,
    }
}

impl Endpoint for RemotePlacementTarget {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            parse(&self.control_group, session, bytes)?;
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
            if (0..3).any(|index| sections.section(index).is_some()) {
                return Err(invalid());
            }
            let request = match parse(&self.control_group, &session, header.as_slice())? {
                PlacementRequest::Move(request) => request,
                PlacementRequest::Vote(request) => {
                    let result = self.vote(&session, &request).await;
                    let header = self.manager.resources().encode_in(
                        ByteClass::Progress,
                        &VoteReply { id: request.id, node: request.node, nonce: session.nonce(), result },
                        HEADER,
                    )?;
                    return Ok(ChargedReply { header, sections: [None, None, None] });
                }
            };
            let view = self.plan(&session, &request).await?;
            let plan = &view.plan;
            let digest = serialized_digest(&request)?;
            let nonce = session.nonce();
            match request.action {
                Action::CheckUnused if plan.phase == ReplicaMovePhase::Claimed => {
                    reply(
                        &self.manager,
                        nonce,
                        digest,
                        self.writable_pool(plan.request.target.pool_id).await?
                            .unused_placement_target(plan.request.group.clone())
                            .await,
                    )
                }
                Action::CheckUnused => Err(conflict()),
                Action::HostLearner => reply(&self.manager, nonce, digest, self.host(plan).await),
                Action::LearnerApplied { minimum } => {
                    reply(&self.manager, nonce, digest, self.learner_applied(plan, minimum).await)
                }
                Action::StableMembership => reply(&self.manager, nonce, digest, self.stable(plan).await),
                Action::Activate { minimum } => {
                    reply(&self.manager, nonce, digest, self.activate(plan, minimum).await)
                }
                Action::RetireSource => reply(&self.manager, nonce, digest, self.retire_source(plan).await),
            }
        })
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn test_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").map(Into::into)
}

#[cfg(any(test, feature = "test-hooks"))]
fn test_record(name: &str, id: Uuid, proof: &impl Serialize) {
    if let Some(directory) = test_dir() {
        let path = directory.join(format!("remote-replica-{name}-{id}.json"));
        let pending = path.with_extension("json.pending");
        if let Ok(bytes) = serde_json::to_vec(proof) {
            if std::fs::write(&pending, bytes).is_ok() {
                let _ = std::fs::rename(pending, path);
            }
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn test_settled(id: Uuid, phase: &ReplicaMovePhase) {
    test_record("settled", id, phase);
}
