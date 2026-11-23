//! Guarded RF3 Data moves. The target owns the native generation lease, the
//! Data leader edits voters, and the queue owner publishes through Control.
use super::super::lifecycle::PlacementCatchUp;
use super::*;
use crate::{
    oltp_deployment::inventory::WorkerPools,
    oltp_replication::authority::control::{
        ControlReply, Fault, LocalOperatorPermit, ReplicaActivationEvidence, ReplicaMove,
        ReplicaMovePhase, ReplicaMoveRequest, ReplicaRoutes, RestartMountKey, VerifiedMembership,
    },
};
use std::time::Duration;
mod catalog;
mod remote;
pub(crate) use catalog::PlacementCatalog;
pub(crate) use remote::RemotePlacementTarget;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoteRequest {
    id: Uuid,
    node: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoteReply {
    id: Uuid,
    node: u64,
    nonce: Uuid,
    result: Result<Membership, ProtocolError>,
}

pub(crate) struct ReplicaPlacement {
    factory: Arc<DataGroupFactory>,
    pools: Arc<WorkerPools>,
    restart_mount: Option<(u64, RestartMountKey)>,
    settle: tokio::sync::Mutex<()>,
}

impl ReplicaPlacement {
    pub(crate) fn new(
        factory: Arc<DataGroupFactory>,
        pools: Arc<WorkerPools>,
        restart_mount: Option<(u64, RestartMountKey)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            factory,
            pools,
            restart_mount,
            settle: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) async fn route(
        &self,
        group: &GroupIdentity,
    ) -> Result<ReplicaRoutes, ProtocolError> {
        if !matches!(group.kind, GroupKind::Data { .. }) {
            return Err(invalid());
        }
        self.factory.rpc.replica_routes(group).await
    }

    pub(crate) async fn advance(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        id: Uuid,
        request: ReplicaMoveRequest,
    ) -> Result<ReplicaMove, ProtocolError> {
        let _settlement = self.settle.try_lock().map_err(|_| busy())?;
        request.validate()?;
        let local = self.factory.manager.client().local_identity();
        let mut plan = match self.factory.control.replica_move(id).await? {
            Some(plan) if plan.request == request && plan.owner == local => plan,
            Some(_) => return Err(conflict()),
            None => self.start(permit, catalog, id, request).await?,
        };
        if plan.phase == ReplicaMovePhase::Claimed {
            plan = self.arm(permit, catalog, plan).await?;
        }
        if matches!(
            plan.phase,
            ReplicaMovePhase::Retired { .. } | ReplicaMovePhase::Cancelled
        ) {
            return Ok(plan);
        }
        let mut quorum = None;
        if plan.phase == ReplicaMovePhase::Moving {
            catalog.verify(&plan).await?;
            let local = if plan.remote_target() {
                self.remote_action::<()>(&plan, remote::Action::HostLearner)
                    .await?;
                None
            } else {
                #[cfg(any(test, feature = "test-hooks"))]
                if crate::oltp::test_hooks::elect_move_source(plan.id) {
                    let group = &plan.request.group;
                    let node = plan.request.remove_node;
                    let source = self.factory.manager.hosted_data(group, node)?;
                    nabedra_rt::time::timeout(Duration::from_secs(30), async {
                        while source.raft.current_leader().await != Some(node)
                            || source.ensure_linearizable().await.is_err()
                        {
                            source.raft.trigger().elect(false).await.unwrap();
                            nabedra_rt::time::sleep(Duration::from_millis(250)).await;
                        }
                    })
                    .await
                    .expect("pool-drain source is the Data leader");
                }
                let pool = self
                    .pools
                    .eligible_for_claimed_move(&plan, &self.factory.control)
                    .await?;
                let store = pool
                    .placement_learner(plan.request.group.clone(), plan.binding()?)
                    .await?;
                let running = self
                    .factory
                    .manager
                    .start_placement_learner(store.clone(), &plan, self.factory.config.clone())
                    .await?;
                Some((store, running))
            };
            let observation = self.quorum(&plan).await?;
            plan = match local {
                Some((store, running)) => {
                    let native = wait_activation(|| store.replica_activation(&observation)).await?;
                    self.factory.wire_participant(
                        &plan.request.group,
                        plan.request.target.node_id,
                        running,
                    )?;
                    self.publish(
                        permit,
                        catalog,
                        &plan,
                        &observation,
                        ReplicaActivationEvidence::Local(&native),
                    )
                    .await?
                }
                None => {
                    let native =
                        wait_activation(|| self.remote_activation(&plan, observation.applied_at()))
                            .await?;
                    self.publish(
                        permit,
                        catalog,
                        &plan,
                        &observation,
                        ReplicaActivationEvidence::Remote(&native),
                    )
                    .await?
                }
            };
            quorum = Some(observation);
        }
        if matches!(plan.phase, ReplicaMovePhase::Retired { .. }) {
            return Ok(plan);
        }
        self.retire(permit, catalog, plan, quorum).await
    }

    async fn start(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        id: Uuid,
        request: ReplicaMoveRequest,
    ) -> Result<ReplicaMove, ProtocolError> {
        if self.factory.control.active_replica_move().await?.is_some() {
            return Err(busy());
        }
        let routes = self.route(&request.group).await?;
        let old: &[ReplicaMember; 3] = routes
            .members
            .as_slice()
            .try_into()
            .map_err(|_| invalid())?;
        let old = old.clone();
        let mut next = old.clone();
        let source = next
            .iter_mut()
            .find(|member| member.node_id == request.remove_node)
            .ok_or_else(conflict)?;
        let local = self.factory.manager.client().local_identity();
        let remote_source = source.worker_id != local.worker_id;
        if routes.record.generation != request.expected_generation
            || source.pool_id == request.target.pool_id
        {
            return Err(conflict());
        }
        if !remote_source {
            let source_pool = self
                .pools
                .retained_for_group(source.pool_id, &self.factory.control)
                .await?;
            source_pool.retained_group(request.group.clone()).await?;
        }
        *source = request.target.clone();
        next.sort_by_key(|member| member.node_id);
        validate_voters(&next).map_err(|_| invalid())?;
        let current = self
            .factory
            .control
            .current(&QueueScope {
                cluster_id: request.group.cluster_id,
                database: request.group.database.clone(),
            })
            .await?;
        if current.owner != local {
            return Err(unauthorized());
        }
        let authority = current.ready.view.authority();
        let failure_domains = if request.worker.worker_id == local.worker_id {
            let target = self
                .pools
                .eligible(
                    request.target.pool_id,
                    request.worker,
                    request.pool_generation,
                    &self.factory.control,
                )
                .await?;
            target
                .unused_placement_target(request.group.clone())
                .await?;
            None
        } else {
            let (domains, _) = self
                .factory
                .control
                .remote_placement_binding(request.clone(), authority, old.clone(), next.clone())
                .await?;
            Some(domains)
        };
        let lineage = catalog.observe(&request.group, authority).await?;
        let mut plan = ReplicaMove {
            id,
            request,
            owner: current.owner,
            authority,
            expected: routes.record.clone(),
            old,
            next,
            lineage,
            failure_domains,
            remote_source: None,
            guard: None,
            phase: ReplicaMovePhase::Claimed,
        };
        if remote_source {
            let (source, domains) = self.factory.control.replica_source_binding(plan.clone()).await?;
            plan.remote_source = Some(source);
            plan.failure_domains = Some(domains);
        }
        let applied = self
            .factory
            .control
            .begin_replica_move(permit, plan.clone())
            .await?;
        let (_, reply, _owner) = applied.into_parts()?;
        match reply {
            ControlReply::ReplicaMove(bound) if *bound == plan => Ok(*bound),
            ControlReply::Rejected(Fault::Busy) => Err(busy()),
            ControlReply::Rejected(Fault::Stale | Fault::Conflict) => Err(conflict()),
            ControlReply::Rejected(Fault::Unauthorized) => Err(unauthorized()),
            _ => Err(unavailable()),
        }
    }

    async fn arm(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        mut plan: ReplicaMove,
    ) -> Result<ReplicaMove, ProtocolError> {
        let mut changed = match catalog.observe(&plan.request.group, plan.authority).await {
            Ok(lineage) => lineage != plan.lineage,
            Err(error)
                if error.code == ErrorCode::Conflict && error.outcome == Outcome::NotApplied =>
            {
                true
            }
            Err(error) => return Err(error),
        };
        if !changed && plan.remote_target() {
            match self
                .remote_action::<()>(&plan, remote::Action::CheckUnused)
                .await
            {
                Ok(_) => {}
                Err(error)
                    if error.code == ErrorCode::Conflict
                        && error.outcome == Outcome::NotApplied =>
                {
                    changed = true
                }
                Err(error) => return Err(error),
            }
        }
        let applied = if changed {
            catalog.release(&plan).await?;
            self.factory
                .control
                .cancel_replica_move(permit, plan.id)
                .await?
        } else {
            let guard = catalog.acquire(&plan).await?;
            plan.guard = Some(guard.into_sealed());
            let guard = catalog.verify(&plan).await?;
            self.factory
                .control
                .arm_replica_move(permit, plan.id, guard)
                .await?
        };
        let (_, reply, _owner) = applied.into_parts()?;
        match reply {
            ControlReply::ReplicaMove(next) if next.id == plan.id => Ok(*next),
            _ => Err(unavailable()),
        }
    }

    async fn host(
        &self,
        store: GroupStore,
        local: &ReplicaMember,
        bootstrap: &[ReplicaMember],
    ) -> Result<ManagedData, ProtocolError> {
        self.factory
            .manager
            .start_data(
                store,
                local.clone(),
                bootstrap
                    .iter()
                    .map(|member| (member.node_id, member.clone()))
                    .collect(),
                self.factory.config.clone(),
            )
            .await
    }

    async fn publish(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        plan: &ReplicaMove,
        observation: &GroupMembershipObservation,
        native: ReplicaActivationEvidence<'_>,
    ) -> Result<ReplicaMove, ProtocolError> {
        let observation = GroupMembershipObservation {
            group: observation.group.clone(),
            replicas: observation.replicas.clone(),
            applied_at: observation.applied_at,
            _owner: observation._owner.clone(),
        };
        let membership = VerifiedMembership::from_consensus(observation, plan.id, plan.authority)?;
        #[cfg(any(test, feature = "test-hooks"))]
        crate::oltp::test_hooks::pause("replica_move_activated", plan.id).await;
        let operation = nabedra_rt::ids::uuid_v4();
        loop {
            // The admin executor owns this future independently of the socket.
            // On an uncertain result keep the exact native lease and retry the
            // same proposal until a strong Control read observes settlement.
            let result = match catalog.verify(plan).await {
                Ok(guard) => {
                    self.factory
                        .control
                        .publish_replica_move(permit, operation, &membership, native, guard)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Ok((_, reply, _owner)) = result.and_then(|applied| applied.into_parts()) {
                if let ControlReply::ReplicaMove(published) = reply {
                    if published.id == plan.id && published.phase != ReplicaMovePhase::Moving {
                        return Ok(*published);
                    }
                }
            }
            if let Ok(Some(published)) = self.factory.control.replica_move(plan.id).await {
                if published.phase != ReplicaMovePhase::Moving {
                    return Ok(published);
                }
            }
            nabedra_rt::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn retire(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        plan: ReplicaMove,
        observation: Option<GroupMembershipObservation>,
    ) -> Result<ReplicaMove, ProtocolError> {
        let operation = nabedra_rt::ids::uuid_v4();
        let applied = if plan.remote_source.is_some() {
            let retired = if plan.source_worker() == plan.request.worker {
                wait_activation(|| self.remote_retirement(&plan)).await?
            } else {
                self.remote_retirement(&plan).await?
            };
            catalog.release(&plan).await?;
            self.factory.control
                .acknowledge_replica_retired_remote(permit, operation, &retired)
                .await?
        } else {
            let observation = match observation {
                Some(observation) => observation,
                None => self.quorum(&plan).await?,
            };
            self.retire_local(permit, catalog, &plan, observation, operation).await?
        };
        let (_, reply, _owner) = applied.into_parts()?;
        if !matches!(reply, ControlReply::GroupRetirement { generation, remaining: 0 } if generation == plan.id)
        {
            return Err(unavailable());
        }
        self.factory
            .control
            .replica_move(plan.id)
            .await?
            .ok_or_else(unavailable)
    }

    async fn retire_local(
        &self,
        permit: &LocalOperatorPermit,
        catalog: &PlacementCatalog<'_>,
        plan: &ReplicaMove,
        observation: GroupMembershipObservation,
        operation: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let removed = plan.removed().ok_or_else(invalid)?;
        let pool = self
            .pools
            .retained_for_group(removed.pool_id, &self.factory.control)
            .await?;
        let store = pool.retained_group(plan.request.group.clone()).await?;
        if store.replica_retirement(plan.id).await?.is_none()
            && self
                .factory
                .manager
                .hosted_data(&plan.request.group, removed.node_id)
                .is_err()
        {
            // Reopening exact retained bytes can recover a stop before its
            // closure marker. An absent local dispatcher proves nothing.
            self.host(store.clone(), removed, &plan.old).await?;
        }
        let retirement = ReplicaRetirementPermit::from_published(
            self.factory.control.clone(),
            plan.owner,
            removed.clone(),
            observation,
        )
        .await?;
        let retired = self
            .factory
            .manager
            .retire_removed_replica(retirement, &store)
            .await?;
        catalog.release(plan).await?;
        self.factory
            .wired
            .lock()
            .map_err(|_| unavailable())?
            .remove(&(plan.request.group.group_id, removed.node_id));
        #[cfg(test)]
        if std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|directory| {
            std::fs::remove_file(
                std::path::PathBuf::from(directory)
                    .join(format!("move-lose-retirement-{}", plan.id)),
            )
            .is_ok()
        }) {
            return Err(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "placement retirement acknowledgement was lost",
                Outcome::Unknown,
            ));
        }
        self.factory
            .control
            .acknowledge_replica_retired_local(permit, operation, &retired)
            .await
    }

    async fn quorum(
        &self,
        plan: &ReplicaMove,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let deadline = nabedra_rt::time::now() + Duration::from_secs(45);
        let mut candidates = plan.next.to_vec();
        candidates.push(plan.removed().ok_or_else(invalid)?.clone());
        loop {
            for member in &candidates {
                if let Ok(Ok(observation)) =
                    nabedra_rt::time::timeout_at(deadline, self.observe(plan, member)).await
                {
                    if observation.group() == &plan.request.group
                        && observation.replicas() == plan.next
                        && observation.applied_at().index > plan.expected.membership_position.index
                    {
                        return Ok(observation);
                    }
                }
            }
            if nabedra_rt::time::now() >= deadline {
                return Err(unavailable());
            }
            nabedra_rt::time::sleep(Duration::from_millis(75)).await;
        }
    }

    async fn observe(
        &self,
        plan: &ReplicaMove,
        member: &ReplicaMember,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let client = self.factory.manager.client();
        if member.worker_id == self.factory.manager.client().local_identity().worker_id {
            return self.vote(plan, member.node_id).await;
        }
        if plan.remote_target() && member == &plan.request.target {
            let (observed, owner) = self
                .remote_action::<Membership>(plan, remote::Action::StableMembership)
                .await?;
            return Ok(GroupMembershipObservation {
                group: observed.group,
                replicas: observed.replicas,
                applied_at: observed.applied_at,
                _owner: owner,
            });
        }
        let control = self.factory.control.identity();
        let (current, voter) = self
            .factory
            .control
            .replica_move_voter(plan.id, member.node_id, client.local_identity())
            .await?;
        if current.binding()? != plan.binding()? || voter.worker_id != member.worker_id {
            return Err(unauthorized());
        }
        let header = self.factory.manager.resources().encode_in(
            ByteClass::Progress,
            &VoteRequest {
                id: plan.id,
                node: member.node_id,
            },
            HEADER,
        )?;
        let reply = client
            .request_classified(
                control,
                voter,
                Purpose::ReplicaPlacement,
                header,
                [None, None, None],
                DECODE,
            )
            .await
            .map_err(ProtocolError::from)?;
        let decoded = reply.decode_header::<VoteReply>()?;
        if reply.group() != control
            || reply.purpose() != Purpose::ReplicaPlacement
            || reply.remote_identity() != voter
            || reply.sections.iter().any(Option::is_some)
            || decoded.id != plan.id
            || decoded.node != member.node_id
            || decoded.nonce != reply.nonce()
        {
            return Err(invalid());
        }
        let observed = decoded.result.as_ref().map_err(Clone::clone)?;
        // The peer can only produce membership after the routed Data leader's
        // actual ReadIndex and applied-state check for this exact move.
        Ok(GroupMembershipObservation {
            group: observed.group.clone(),
            replicas: observed.replicas.clone(),
            applied_at: observed.applied_at,
            _owner: reply.scratch_lease(),
        })
    }

    async fn vote(
        &self,
        plan: &ReplicaMove,
        node: u64,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let target_applied = |minimum| -> Pin<
            Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>,
        > {
            Box::pin(async move {
                self.remote_action::<()>(plan, remote::Action::LearnerApplied { minimum })
                    .await?;
                Ok(())
            })
        };
        let target_applied: &PlacementCatchUp<'_> = &target_applied;
        native_vote(
            &self.factory.manager,
            plan,
            node,
            plan.remote_target().then_some(target_applied),
        ).await
    }

    fn parse(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<remote::PlacementRequest, ProtocolError> {
        let request = remote::parse(self.factory.control.identity(), session, bytes)?;
        if let remote::PlacementRequest::Move(request) = &request {
            if self.restart_mount.is_none()
                || !matches!(request.action, remote::Action::RetireSource)
            {
                return Err(unauthorized());
            }
        }
        Ok(request)
    }
}

impl Endpoint for ReplicaPlacement {
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
            let request = match request {
                remote::PlacementRequest::Move(request) => {
                    return self.retire_bootstrap(&session, &request).await;
                }
                remote::PlacementRequest::Vote(request) => request,
            };
            let result = async {
                let local = self.factory.manager.client().local_identity();
                let (plan, voter) = self
                    .factory
                    .control
                    .replica_move_voter(request.id, request.node, local)
                    .await?;
                if session.remote() != plan.owner || voter != local {
                    return Err(unauthorized());
                }
                let observed = self.vote(&plan, request.node).await?;
                Ok(Membership {
                    group: observed.group,
                    replicas: observed.replicas,
                    applied_at: observed.applied_at,
                })
            }
            .await;
            let reply = self.factory.manager.resources().encode_in(
                ByteClass::Progress,
                &VoteReply {
                    id: request.id,
                    node: request.node,
                    nonce: session.nonce(),
                    result,
                },
                HEADER,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [None, None, None],
            })
        })
    }
}

async fn native_vote(
    manager: &HostedGroupManager,
    plan: &ReplicaMove,
    node: u64,
    target_applied: Option<&PlacementCatchUp<'_>>,
) -> Result<GroupMembershipObservation, ProtocolError> {
    let local = manager.client().local_identity();
    let member = plan.old.iter().chain(&plan.next)
        .find(|member| member.node_id == node && member.worker_id == local.worker_id)
        .ok_or_else(unauthorized)?;
    let running = manager.hosted_data(&plan.request.group, node)?;
    if running.state.store().pool().identity().pool_id != member.pool_id {
        return Err(unauthorized());
    }
    match plan.phase {
        ReplicaMovePhase::Moving if member != &plan.request.target => {
            running.reconcile_voter_replacement(
                &plan.old, &plan.next, &plan.request.target, plan.request.remove_node, target_applied,
                // The source replica retires after applying the final membership.
                true,
            ).await
        }
        ReplicaMovePhase::Moving | ReplicaMovePhase::Published { .. } => {
            running.observe_stable_membership().await
        }
        ReplicaMovePhase::Claimed | ReplicaMovePhase::Retired { .. } | ReplicaMovePhase::Cancelled => {
            Err(conflict())
        }
    }
}

async fn wait_activation<T, F, R>(mut acquire: F) -> Result<T, ProtocolError>
where
    F: FnMut() -> R,
    R: Future<Output = Result<T, ProtocolError>>,
{
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
        loop {
            match acquire().await {
                Ok(native) => return Ok(native),
                Err(error) if retryable(&error) => {
                    nabedra_rt::time::sleep(Duration::from_millis(50)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| unavailable())?
}

fn retryable(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}

fn busy() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "a replica move is still active",
        Outcome::Unknown,
    )
}

fn conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "replica move ID or expected placement differs",
        Outcome::NotApplied,
    )
}
