use super::*;
use crate::{
    oltp::mvcc::store::lifecycle::VerifiedLifecycleGuard,
    oltp_deployment::controller::{GrowthPolicy, GrowthRecord},
    oltp_replication::{
        authority::control::{
            RemoteMoveView, RemoteSourceBinding, ReplicaActivationEvidence, ReplicaMove, ReplicaMovePhase,
            ReplicaMoveRequest,
            placement::{RemoteTargetBinding, ReplicaMovePublication, source_binding, target_binding},
        },
        owned::serialized_digest,
    },
};
use crate::oltp_replication::authority::timestamp::split::{
    MAX_SPLIT_VOTERS, SplitSourcePlacement, SplitVoter,
};

impl ControlHandle {
    pub(in crate::oltp_replication::authority::control) async fn split_source(
        &self,
        expected: GroupRecord,
        authority: Authority,
    ) -> Result<ChargedBytes, ProtocolError> {
        let scope = self.identity().clone();
        self.read(move |view| {
            let stale = || unavailable("automatic split source placement or ready membership changed");
            let current = state(view)?;
            if current.ready.view.authority() != authority
                || expected.identity.cluster_id != scope.cluster_id
                || expected.identity.database != scope.database
                || !matches!(expected.identity.kind, GroupKind::Data { .. })
                || !matches!(expected.replica_count, 1 | 3 | 5)
                || load::<GroupRecord>(view, &machine::key(GROUPS, expected.identity.group_id))?
                    .as_ref() != Some(&expected)
            {
                return Err(stale());
            }
            let bootstrap = load::<GroupRecord>(view, &machine::key(GROUPS, scope.group_id))?
                .ok_or_else(stale)?;
            let mut members = Vec::with_capacity(MAX_SPLIT_VOTERS);
            let progress = view.scan_page(
                &machine::member_prefix(expected.generation),
                None,
                MAX_SPLIT_VOTERS + 1,
                |key, value| {
                    let member: ReplicaMember = machine::decode(value)?;
                    if members.len() == MAX_SPLIT_VOTERS
                        || key != machine::member_key(expected.generation, member.node_id)
                    {
                        return Err(stale());
                    }
                    members.push(member);
                    Ok(())
                },
            )?;
            if !progress.exhausted || members.len() as u64 != expected.replica_count {
                return Err(stale());
            }
            let mut voters = Vec::with_capacity(members.len());
            let mut fixed = bootstrap.replica_count == expected.replica_count;
            for member in &members {
                let worker = load::<WorkerIdentity>(
                    view, &machine::ready_key(current.ready_generation, member.worker_id),
                )?.ok_or_else(stale)?;
                if !target_ready(view, &current, worker)?
                    || view.get(&machine::pool_drain::barrier_key(member.pool_id))?.is_some()
                    || view.get(&machine::worker_drain::barrier_key(member.worker_id))?.is_some()
                {
                    return Err(stale());
                }
                let registered_pool_generation = if let Some((registered, growth)) =
                    registration(view, worker.worker_id)?
                {
                    if registered.intent.worker() != worker
                        || registered.intent.run_id != authority.run_id
                        || !registered.completed(&growth)
                    {
                        return Err(stale());
                    }
                    fixed = false;
                    Some(registered.mounted_pool(member.pool_id).ok_or_else(stale)?.pool_generation)
                } else {
                    let original = load::<ReplicaMember>(
                        view, &machine::member_worker_key(bootstrap.generation, member.worker_id),
                    )?;
                    // Moving between mounts on one fixed worker preserves its
                    // failure domain. The mounted worker verifies that pool.
                    fixed &= original.is_some();
                    None
                };
                voters.push(SplitVoter { member: member.clone(), worker, registered_pool_generation });
            }
            let policy = load::<GrowthPolicy>(view, super::super::growth::POLICY)?;
            let explicit_zones = policy.as_ref().is_some_and(|policy| !policy.initial_replica_zones.is_empty());
            let domains = if let (Ok(rf3), Some(policy)) = (
                <&[ReplicaMember; 3]>::try_from(members.as_slice()), policy.as_ref(),
            ) {
                super::super::placement::failure_domains(rf3, rf3, &policy, |worker| registration(view, worker))?
            } else {
                None
            };
            // Fixed deployments already enforce their configured topology.
            // An altered placement must prove its current three failure domains.
            if (!fixed || (expected.replica_count == 3 && explicit_zones)) && domains.is_none() {
                return Err(unavailable("automatic split needs three verified replica failure domains"));
            }
            let source = SplitSourcePlacement {
                record: expected,
                voters: Page::new(voters).map_err(|_| stale())?,
                domains,
            };
            source.validate(&source.record.identity)?;
            view.encode(&source)
        }).await
    }

    pub(crate) async fn acknowledge_replica_retired_remote(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        retired: &super::super::VerifiedRemoteRetirement,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let (proof, signature) = retired.parts();
        let authorization = self.local_operator(
            permit,
            operation_id,
            serialized_digest(&(proof, signature))?,
        )?;
        self.submit(ControlCommand::ReplicaRetired {
            operation_id,
            authorization,
            proof: proof.clone(),
            remote: Some(signature.clone()),
        }).await
    }

    pub(crate) async fn replica_source_binding(
        &self,
        plan: ReplicaMove,
    ) -> Result<(RemoteSourceBinding, Digest), ProtocolError> {
        let scope = self.identity().clone();
        let raw = self.read(move |view| {
            let current = state(view)?;
            if current.owner != Some(plan.owner)
                || current.ready.view.authority() != plan.authority
                || !target_ready(view, &current, plan.owner)?
                || !target_ready(view, &current, plan.request.worker)?
                || !placement_current(view, &plan.expected)?
            {
                return Err(unauthorized());
            }
            let (source, domains) = read_source_binding(view, &scope, &plan)?
                .ok_or_else(unauthorized)?;
            if !target_ready(view, &current, source.worker)? {
                return Err(unauthorized());
            }
            view.encode(&(source, domains))
        }).await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn remote_placement_binding(
        &self,
        request: ReplicaMoveRequest,
        authority: Authority,
        old: [ReplicaMember; 3],
        next: [ReplicaMember; 3],
    ) -> Result<(Digest, Digest), ProtocolError> {
        let raw = self
            .read(move |view| {
                let current = state(view)?;
                if current.ready.view.authority() != authority
                    || !target_ready(view, &current, request.worker)?
                {
                    return Err(unauthorized());
                }
                let target = read_target_binding(view, &request, authority, &old, &next)?
                    .ok_or_else(unauthorized)?;
                view.encode(&(target.domains, target.mount_public_key))
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(in crate::oltp_replication::authority::control) async fn remote_move_view(
        &self,
        id: Uuid,
        remote: WorkerIdentity,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let plan = load::<ReplicaMove>(view, &machine::placement::move_key(id))?
                .ok_or_else(unauthorized)?;
            let source = plan.remote_source.as_ref()
                .is_some_and(|source| source.worker == remote);
            let target = plan.remote_target()
                && plan.request.worker == remote
                && plan.failure_domains.is_some();
            if !source && !target {
                return Err(unauthorized());
            }
            let current = state(view)?;
            let fenced = current.ready.view.authority() != plan.authority;
            let active = move_active(view, &current, &plan)? && target_ready(view, &current, remote)?;
            let eligible = match &plan.phase {
                ReplicaMovePhase::Published { placement, .. } if source => {
                    // Exact source retries use the published proof even after a policy change.
                    active && placement_current(view, placement)?
                }
                ReplicaMovePhase::Claimed | ReplicaMovePhase::Moving if target => {
                    active && placement_current(view, &plan.expected)?
                        && read_target_binding(view, &plan.request, plan.authority, &plan.old, &plan.next)?
                            .is_some_and(|binding| Some(binding.domains) == plan.failure_domains)
                }
                ReplicaMovePhase::Published { placement, .. } if target => {
                    active && placement_current(view, placement)?
                }
                _ => false,
            };
            view.encode(&RemoteMoveView {
                plan,
                eligible,
                fenced,
            })
        })
        .await
    }

    pub(in crate::oltp_replication::authority::control) async fn replica_move_voter_read(
        &self,
        id: Uuid,
        node: u64,
        caller: WorkerIdentity,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let plan = load::<ReplicaMove>(view, &machine::placement::move_key(id))?
                .ok_or_else(unauthorized)?;
            let (members, expected) = match &plan.phase {
                ReplicaMovePhase::Moving => (&plan.old, &plan.expected),
                ReplicaMovePhase::Published { placement, .. } => (&plan.next, placement),
                _ => return Err(unauthorized()),
            };
            let member = members.iter()
                .find(|member| member.node_id == node)
                .ok_or_else(unauthorized)?;
            let current = state(view)?;
            let voter = load::<WorkerIdentity>(
                view, &machine::ready_key(current.ready_generation, member.worker_id),
            )?.ok_or_else(unauthorized)?;
            let target_learner = plan.phase == ReplicaMovePhase::Moving
                && plan.remote_target()
                && plan.request.worker == caller;
            if (caller != plan.owner && caller != voter && !target_learner)
                || !move_active(view, &current, &plan)?
                || !placement_current(view, expected)?
                || !target_ready(view, &current, caller)?
                || !target_ready(view, &current, voter)?
            {
                return Err(unauthorized());
            }
            view.encode(&(plan, voter))
        }).await
    }

    pub(crate) async fn replica_move_voter(
        &self,
        id: Uuid,
        node: u64,
        caller: WorkerIdentity,
    ) -> Result<(ReplicaMove, WorkerIdentity), ProtocolError> {
        let raw = self.replica_move_voter_read(id, node, caller).await?;
        machine::decode(raw.as_ref())
    }

    #[doc(hidden)]
    pub async fn replica_move(
        &self,
        id: Uuid,
    ) -> Result<Option<ReplicaMove>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let value: Option<ReplicaMove> = load(view, &machine::placement::move_key(id))?;
                view.encode(&value)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    /// CONTROL-1: the run owner that retires a published move after an owner
    /// fence released its slot (`machine::placement::resolve_on_owner_fence`).
    #[doc(hidden)]
    pub async fn replica_move_steward(&self, id: Uuid) -> Result<Option<WorkerIdentity>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let value: Option<(WorkerIdentity, Authority)> =
                    load(view, &machine::placement::steward_key(id))?;
                view.encode(&value.map(|(owner, _)| owner))
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    #[doc(hidden)]
    pub async fn active_replica_move(&self) -> Result<Option<ReplicaMove>, ProtocolError> {
        let raw = self
            .read(|view| {
                let value = match load::<Uuid>(view, machine::placement::ACTIVE)? {
                    Some(id) => Some(
                        load::<ReplicaMove>(view, &machine::placement::move_key(id))?
                            .ok_or_else(|| unavailable("active replica move is missing"))?,
                    ),
                    None => None,
                };
                view.encode(&value)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn begin_replica_move(
        &self,
        permit: &LocalOperatorPermit,
        plan: ReplicaMove,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        if permit.local != plan.owner {
            return Err(unauthorized());
        }
        let authorization = self.local_operator(permit, plan.id, plan.binding()?)?;
        self.submit(ControlCommand::BeginReplicaMove {
            operation_id: plan.id,
            authorization,
            plan: Box::new(plan),
        })
        .await
    }

    pub(crate) async fn publish_replica_move(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        quorum: &VerifiedMembership,
        native: ReplicaActivationEvidence<'_>,
        guard: VerifiedLifecycleGuard,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let (activation, remote) = match native {
            ReplicaActivationEvidence::Local(native) => (native.statement(), None),
            ReplicaActivationEvidence::Remote(remote) => {
                let (activation, signature) = remote.parts();
                (activation, Some(signature.clone()))
            }
        };
        let proof = ReplicaMovePublication {
            worker: permit.local,
            membership: quorum.stamp.clone(),
            activation: activation.clone(),
            guard: guard.into_sealed(),
            remote,
        };
        let authorization =
            self.local_operator(permit, operation_id, serialized_digest(&proof)?)?;
        self.submit(ControlCommand::PublishReplicaMove {
            operation_id,
            authorization,
            proof: Box::new(proof),
        })
        .await
    }

    pub(crate) async fn arm_replica_move(
        &self,
        permit: &LocalOperatorPermit,
        transition_id: Uuid,
        guard: VerifiedLifecycleGuard,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let operation_id = nabedra_rt::ids::uuid_v4();
        let guard = guard.into_sealed();
        let authorization = self.local_operator(
            permit,
            operation_id,
            serialized_digest(&(transition_id, &guard))?,
        )?;
        self.submit(ControlCommand::ArmReplicaMove {
            operation_id,
            transition_id,
            worker: permit.local,
            authorization,
            guard: Box::new(guard),
        })
        .await
    }

    pub(crate) async fn cancel_replica_move(
        &self,
        permit: &LocalOperatorPermit,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let operation_id = nabedra_rt::ids::uuid_v4();
        let authorization = self.local_operator(
            permit,
            operation_id,
            serialized_digest(&(b"cancel", transition_id))?,
        )?;
        self.submit(ControlCommand::CancelReplicaMove {
            operation_id,
            transition_id,
            worker: permit.local,
            authorization,
        })
        .await
    }
}

fn move_active(
    view: &mut GroupRead<'_>,
    state: &State,
    plan: &ReplicaMove,
) -> Result<bool, ProtocolError> {
    Ok(state.owner == Some(plan.owner)
        && state.ready.view.authority() == plan.authority
        && target_ready(view, state, plan.owner)?
        && load::<Uuid>(view, machine::placement::ACTIVE)? == Some(plan.id))
}

fn placement_current(view: &mut GroupRead<'_>, record: &GroupRecord) -> Result<bool, ProtocolError> {
    Ok(load::<GroupRecord>(view, &machine::key(GROUPS, record.identity.group_id))?
        .as_ref() == Some(record))
}

fn target_ready(
    view: &mut GroupRead<'_>,
    state: &State,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(state.pending.is_none()
        && state.invalid_ready == 0
        && load::<WorkerIdentity>(
            view,
            &machine::ready_key(state.ready_generation, worker.worker_id),
        )? == Some(worker)
        && load::<WorkerRecord>(view, &machine::key(WORKERS, worker.worker_id))?
            .is_some_and(|entry| WorkerIdentity::from(&entry.enrollment) == worker))
}

fn read_target_binding(
    view: &mut GroupRead<'_>,
    request: &ReplicaMoveRequest,
    authority: Authority,
    old: &[ReplicaMember; 3],
    next: &[ReplicaMember; 3],
) -> Result<Option<RemoteTargetBinding>, ProtocolError> {
    use super::super::growth;
    let Some(policy) = load::<GrowthPolicy>(view, growth::POLICY)? else {
        return Ok(None);
    };
    target_binding(request, authority, old, next, &policy, |worker| registration(view, worker))
}

fn read_source_binding(
    view: &mut GroupRead<'_>,
    scope: &GroupIdentity,
    plan: &ReplicaMove,
) -> Result<Option<(RemoteSourceBinding, Digest)>, ProtocolError> {
    use super::super::growth;
    let Some(policy) = load::<GrowthPolicy>(view, growth::POLICY)? else {
        return Ok(None);
    };
    let Some(claim) = load::<Uuid>(view, &growth::pool_claim_key(plan.request.target.pool_id))? else {
        return Ok(None);
    };
    let target = load::<GrowthRecord>(view, &growth::key(claim))?.ok_or_else(unauthorized)?;
    let bootstrap_mount = match plan.removed() {
        Some(member) if policy.initial_replica_zones.contains_key(&member.worker_id) => {
            machine::restart::bootstrap_primary(scope, member, |key| view.get(key))?
        }
        _ => None,
    };
    source_binding(
        &plan.request,
        plan.owner,
        plan.authority,
        (&plan.old, &plan.next),
        (&policy, &target),
        bootstrap_mount,
        |worker| registration(view, worker),
    )
}

fn registration(
    view: &mut GroupRead<'_>,
    worker: Uuid,
) -> Result<Option<(super::super::data_worker::DataWorkerRegistration, GrowthRecord)>, ProtocolError> {
    use super::super::{data_worker::DataWorkerRegistration, growth};
    let Some(registration) = load::<DataWorkerRegistration>(
        view,
        &machine::key(machine::data_worker::REGISTERED, worker),
    )? else {
        return Ok(None);
    };
    let claim = load::<GrowthRecord>(view, &growth::key(registration.intent.claim_id))?
        .ok_or_else(unauthorized)?;
    Ok(Some((registration, claim)))
}
