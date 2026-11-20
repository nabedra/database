use super::*;
use crate::oltp::mvcc::store::lifecycle::{GuardMode, SealedLifecycleGuard};
use crate::oltp_replication::{
    authority::control::{ReplicaMove, ReplicaMovePhase, placement::{
        ReplicaMovePublication, source_binding, target_binding, verify_remote_activation, verify_remote_retirement,
    }},
    owned::serialized_digest,
};
use crate::oltp_deployment::controller::{GrowthPolicy, GrowthRecord};
use super::super::{data_worker::DataWorkerRegistration, growth, restart::RestartMountProof};

pub(in crate::oltp_replication::authority::control) const ACTIVE: &[u8] = b"ct/replica-move-active";

pub(in crate::oltp_replication::authority::control) fn move_key(id: Uuid) -> Vec<u8> {
    key(b"ct/replica-move/", id)
}

fn claim_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/group-replacement/", group_id)
}

fn occupied(group_id: Uuid, node_id: u64) -> Vec<u8> {
    member_key_nested(b"ct/permanent-replica/", group_id, node_id)
}

fn root(replicas: &[ReplicaMember]) -> Digest {
    group_root(
        replicas.len() as u64,
        replicas.iter().fold(group_seed(), group_step),
    )
}

pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    operation_id: Uuid,
    plan: &ReplicaMove,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    let request = &plan.request;
    if !stamp(authorization, state.policy)
        || authorization.subject != plan.owner.certificate_sha256
        || authorization.evidence != plan.binding()?
    {
        return fault(Fault::Unauthorized);
    }
    if state.pending.is_some() || tx.get(ACTIVE)?.is_some() {
        return fault(Fault::Busy);
    }
    let Some(removed) = plan.removed() else {
        return fault(Fault::Invalid);
    };
    // The added replica is a learner first: once `data.direct.v1` is active
    // its worker must decode direct entries.
    if !super::direct::admits_worker_id(tx, request.target.worker_id)? {
        return fault(Fault::DirectCapabilityRequired);
    }
    let mut next = plan.old.clone();
    for member in &mut next {
        if member == removed {
            *member = request.target.clone();
        }
    }
    next.sort_by_key(|member| member.node_id);
    if operation_id != plan.id
        || request.validate().is_err()
        || !same_scope(scope, &request.group)
        || plan.expected.identity != request.group
        || plan.expected.generation != request.expected_generation
        || plan.id == request.expected_generation
        || plan.phase != ReplicaMovePhase::Claimed
        || plan.guard.is_some()
        || removed.worker_id != plan.source_worker().worker_id
        || removed.pool_id == request.target.pool_id
        || validate_voters(&plan.old).is_err()
        || validate_voters(&next).is_err()
        || plan
            .old
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || plan.next != next
        || plan.expected.replica_count != 3
        || plan.expected.replica_root != root(&plan.old)
    {
        return fault(Fault::Invalid);
    }
    let GroupKind::Data { table_id, .. } = request.group.kind else {
        return fault(Fault::Invalid);
    };
    if plan.lineage.table != table_id
        || plan.lineage.schema_epoch == 0
        || !nonzero(plan.lineage.route_digest)
        || load::<GroupRecord>(tx, &key(GROUPS, plan.lineage.catalog))?
            .is_none_or(|record| record.identity.kind != GroupKind::Catalog)
    {
        return fault(Fault::Invalid);
    }
    let group_id = request.group.group_id;
    if state.owner != Some(plan.owner)
        || state.ready.view.authority() != plan.authority
        || state.invalid_ready != 0
        || !current_ready(tx, state, plan.owner)?
        || !current_ready(tx, state, request.worker)?
        || !current_ready(tx, state, plan.source_worker())?
        || load::<GroupRecord>(tx, &key(GROUPS, group_id))?.as_ref() != Some(&plan.expected)
    {
        return fault(Fault::Stale);
    }
    if pool_drain::blocks_pool(tx, request.target.pool_id)? || worker_drain::blocks_worker(tx, request.target.worker_id)? {
        return fault(Fault::Busy);
    }
    if !target_matches(tx, plan, scope, None)? {
        return fault(Fault::Unauthorized);
    }
    for member in &plan.old {
        if load::<ReplicaMember>(tx, &member_key(plan.expected.generation, member.node_id))?
            .as_ref()
            != Some(member)
            || load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, member.worker_id))?
                .is_none()
        {
            return fault(Fault::NotReady);
        }
    }
    if tx.get(&claim_key(group_id))?.is_some()
        || tx.get(&key(b"ct/group-build/", plan.id))?.is_some()
        || tx.get(&move_key(plan.id))?.is_some()
        || tx
            .get(&occupied(group_id, request.target.node_id))?
            .is_some()
    {
        return fault(Fault::Conflict);
    }
    save(tx, ACTIVE, &plan.id)?;
    save(tx, &claim_key(group_id), &plan.id)?;
    for member in [&plan.request.target, removed] {
        save(tx, &occupied(group_id, member.node_id), member)?;
    }
    save(tx, &move_key(plan.id), plan)?;
    Ok(ControlReply::ReplicaMove(Box::new(plan.clone())))
}

pub(super) fn arm(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    guard: &SealedLifecycleGuard,
) -> Result<ControlReply, ProtocolError> {
    let Some(mut plan) = load::<ReplicaMove>(tx, &move_key(id))? else {
        return fault(Fault::NotFound);
    };
    if worker != plan.owner
        || !stamp(authorization, state.policy)
        || authorization.subject != plan.owner.certificate_sha256
        || authorization.evidence != serialized_digest(&(id, guard))?
    {
        return fault(Fault::Unauthorized);
    }
    if guard.mode() != GuardMode::Shared || guard.binding() != &plan.lifecycle()? {
        return fault(Fault::Invalid);
    }
    if plan.phase == ReplicaMovePhase::Moving && plan.guard.as_ref() == Some(guard) {
        return Ok(ControlReply::ReplicaMove(Box::new(plan)));
    }
    if plan.phase != ReplicaMovePhase::Claimed
        || load::<Uuid>(tx, ACTIVE)? != Some(id)
        || state.owner != Some(plan.owner)
        || state.ready.view.authority() != plan.authority
    {
        return fault(Fault::Stale);
    }
    plan.phase = ReplicaMovePhase::Moving;
    plan.guard = Some(guard.clone());
    save(tx, &move_key(id), &plan)?;
    Ok(ControlReply::ReplicaMove(Box::new(plan)))
}

pub(super) fn cancel(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    let Some(mut plan) = load::<ReplicaMove>(tx, &move_key(id))? else {
        return fault(Fault::NotFound);
    };
    if worker != plan.owner
        || !stamp(authorization, state.policy)
        || authorization.subject != plan.owner.certificate_sha256
        || authorization.evidence != serialized_digest(&(b"cancel", id))?
    {
        return fault(Fault::Unauthorized);
    }
    if plan.phase == ReplicaMovePhase::Cancelled {
        return Ok(ControlReply::ReplicaMove(Box::new(plan)));
    }
    let claim = claim_key(plan.request.group.group_id);
    if plan.phase != ReplicaMovePhase::Claimed
        || load::<Uuid>(tx, ACTIVE)? != Some(id)
        || load::<Uuid>(tx, &claim)? != Some(id)
        || state.owner != Some(plan.owner)
        || state.ready.view.authority() != plan.authority
    {
        return fault(Fault::Stale);
    }
    plan.phase = ReplicaMovePhase::Cancelled;
    save(tx, &move_key(id), &plan)?;
    tx.delete(&claim)?;
    tx.delete(ACTIVE)?;
    // A claimed-only attempt never added this exact replica ID to Raft.
    tx.delete(&occupied(
        plan.request.group.group_id,
        plan.request.target.node_id,
    ))?;
    Ok(ControlReply::ReplicaMove(Box::new(plan)))
}

pub(super) fn publish(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    proof: &ReplicaMovePublication,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    let id = proof.membership.generation;
    let Some(mut plan) = load::<ReplicaMove>(tx, &move_key(id))? else {
        return fault(Fault::NotFound);
    };
    let request = &plan.request;
    if !stamp(authorization, state.policy)
        || authorization.subject != plan.owner.certificate_sha256
        || authorization.evidence != serialized_digest(proof)?
        || proof.worker != plan.owner
    {
        return fault(Fault::Unauthorized);
    }
    if matches!(
        plan.phase,
        ReplicaMovePhase::Published { .. } | ReplicaMovePhase::Retired { .. }
    ) {
        return Ok(ControlReply::ReplicaMove(Box::new(plan)));
    }
    if plan.phase != ReplicaMovePhase::Moving {
        return fault(Fault::Stale);
    }
    let group = &request.group;
    let quorum = &proof.membership;
    let target = &proof.activation;
    let minimum_root = serialized_digest(&(
        b"nabedra/activation-voters/v1",
        group,
        quorum.applied_at,
        &plan.next,
    ))?;
    let lineage = serialized_digest(&(
        b"nabedra/activation-lineage/v1",
        group,
        &target.pool,
        target.registered_owner,
        target.minimum,
        target.minimum_voters_root,
        target.active_generation,
        target.applied,
        target.command_digest,
        target.membership_digest,
    ))?;
    if state.pending.is_some()
        || state.owner != Some(plan.owner)
        || state.ready.view.authority() != plan.authority
        || state.invalid_ready != 0
        || !current_ready(tx, state, plan.owner)?
        || !current_ready(tx, state, request.worker)?
        || !current_ready(tx, state, plan.source_worker())?
        || load::<Uuid>(tx, ACTIVE)? != Some(id)
        || load::<Uuid>(tx, &claim_key(group.group_id))? != Some(id)
        || load::<GroupRecord>(tx, &key(GROUPS, group.group_id))?.as_ref() != Some(&plan.expected)
    {
        return fault(Fault::Stale);
    }
    if !target_matches(tx, &plan, scope, Some(proof))? {
        return fault(Fault::Unauthorized);
    }
    if quorum.group != *group
        || quorum.replica_count != 3
        || quorum.replica_root != root(&plan.next)
        || quorum.authority != plan.authority
        || plan.guard.as_ref() != Some(&proof.guard)
        || !position_valid(quorum.applied_at)
        || quorum.applied_at.index <= plan.expected.membership_position.index
        || target.group != *group
        || target.pool.cluster_id != group.cluster_id
        || target.pool.worker_id != request.worker.worker_id
        || target.pool.pool_id != request.target.pool_id
        || target.registered_owner != Some((request.worker, request.pool_generation))
        || target.minimum != quorum.applied_at
        || target.minimum_voters_root != minimum_root
        || target.lineage_digest != lineage
        || !position_valid(target.applied)
        || target.applied.index < quorum.applied_at.index
        || (target.applied.index == quorum.applied_at.index && target.applied != quorum.applied_at)
    {
        return fault(Fault::Invalid);
    }
    let placement = GroupRecord {
        identity: group.clone(),
        generation: id,
        replica_count: 3,
        replica_root: quorum.replica_root,
        membership_position: quorum.applied_at,
    };
    for member in &plan.next {
        save(tx, &member_key(id, member.node_id), member)?;
        save(tx, &member_worker_key(id, member.worker_id), member)?;
    }
    save(tx, &key(GROUPS, group.group_id), &placement)?;
    state.group_inventory_revision = state
        .group_inventory_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    plan.phase = ReplicaMovePhase::Published {
        placement,
        activated_generation: target.active_generation,
        activated_at: target.applied,
        activation_lineage: target.lineage_digest,
    };
    save(tx, &move_key(id), &plan)?;
    Ok(ControlReply::ReplicaMove(Box::new(plan)))
}

pub(super) fn retire(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    mut plan: ReplicaMove,
    proof: &ReplicaRetiredStamp,
    remote: Option<&RestartMountProof>,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    let evidence = match remote {
        Some(signature) => serialized_digest(&(proof, signature))?,
        None => serialized_digest(proof)?,
    };
    let source_worker = plan.source_worker();
    if !stamp(authorization, state.policy)
        || authorization.subject != plan.owner.certificate_sha256
        || authorization.evidence != evidence
        || proof.worker != source_worker
        || match (plan.remote_source.as_ref(), remote) {
            (None, None) => false,
            (Some(_), Some(signature)) => verify_remote_retirement(scope, &plan, proof, signature).is_err(),
            _ => true,
        }
    {
        return fault(Fault::Unauthorized);
    }
    if proof.group != plan.request.group
        || proof.generation != plan.id
        || plan.removed() != Some(&proof.replica)
        || !position_valid(proof.applied_at)
    {
        return fault(Fault::Invalid);
    }
    let (placement, activated_generation, activated_at, activation_lineage) = match plan.phase {
        ReplicaMovePhase::Published {
            placement,
            activated_generation,
            activated_at,
            activation_lineage,
        } => (
            placement,
            activated_generation,
            activated_at,
            activation_lineage,
        ),
        ReplicaMovePhase::Retired { retired_at, .. } if retired_at == proof.applied_at => {
            return Ok(ControlReply::GroupRetirement {
                generation: plan.id,
                remaining: 0,
            });
        }
        _ => return fault(Fault::Conflict),
    };
    let claim = claim_key(placement.identity.group_id);
    if load::<Uuid>(tx, ACTIVE)? != Some(plan.id)
        || load::<Uuid>(tx, &claim)? != Some(plan.id)
        || load::<GroupRecord>(tx, &key(GROUPS, placement.identity.group_id))?.as_ref()
            != Some(&placement)
        || state.owner != Some(plan.owner)
        || state.ready.view.authority() != plan.authority
        || !current_ready(tx, state, plan.owner)?
        || !current_ready(tx, state, source_worker)?
        || proof.applied_at.index < placement.membership_position.index
    {
        return fault(Fault::Stale);
    }
    plan.phase = ReplicaMovePhase::Retired {
        placement,
        activated_generation,
        activated_at,
        activation_lineage,
        retired_at: proof.applied_at,
    };
    save(tx, &move_key(plan.id), &plan)?;
    tx.delete(&claim)?;
    tx.delete(ACTIVE)?;
    Ok(ControlReply::GroupRetirement {
        generation: plan.id,
        remaining: 0,
    })
}

fn target_matches(
    tx: &mut GroupTransaction<'_>,
    plan: &ReplicaMove,
    scope: &GroupIdentity,
    proof: Option<&ReplicaMovePublication>,
) -> Result<bool, ProtocolError> {
    if let Some(source) = &plan.remote_source {
        let Some(policy) = load::<GrowthPolicy>(tx, growth::POLICY)? else {
            return Ok(false);
        };
        let Some(claim) = load::<Uuid>(tx, &growth::pool_claim_key(plan.request.target.pool_id))? else {
            return Ok(false);
        };
        if claim != source.target_claim {
            return Ok(false);
        }
        let target = load::<GrowthRecord>(tx, &growth::key(claim))?.ok_or_else(unknown)?;
        let bootstrap_mount = match plan.removed() {
            Some(member) if policy.initial_replica_zones.contains_key(&member.worker_id) => {
                restart::bootstrap_primary(scope, member, |key| tx.get(key))?
            }
            _ => None,
        };
        let bound = source_binding(
            &plan.request,
            plan.owner,
            plan.authority,
            (&plan.old, &plan.next),
            (&policy, &target),
            bootstrap_mount,
            |worker| registration(tx, worker),
        )?;
        if !bound.is_some_and(|(bound, domains)| {
            bound == *source && plan.failure_domains == Some(domains)
        }) {
            return Ok(false);
        }
    }
    if !plan.remote_target() {
        return Ok(plan.request.worker == plan.owner
            && proof.is_none_or(|proof| proof.remote.is_none())
            && (plan.remote_source.is_some() || plan.failure_domains.is_none()));
    }
    let Some(policy) = load::<GrowthPolicy>(tx, growth::POLICY)? else {
        return Ok(false);
    };
    let Some(target) = target_binding(
        &plan.request,
        plan.authority,
        &plan.old,
        &plan.next,
        &policy,
        |worker| registration(tx, worker),
    )? else {
        return Ok(false);
    };
    Ok(plan.failure_domains == Some(target.domains)
        && proof.is_none_or(|proof| {
            proof.remote.as_ref().is_some_and(|signature| {
                verify_remote_activation(scope, plan, &proof.activation, signature, target.mount_public_key).is_ok()
            })
        }))
}

fn registration(
    tx: &mut GroupTransaction<'_>,
    worker: Uuid,
) -> Result<Option<(DataWorkerRegistration, GrowthRecord)>, ProtocolError> {
    let Some(registration) = load::<DataWorkerRegistration>(tx, &key(data_worker::REGISTERED, worker))? else {
        return Ok(None);
    };
    let claim = load::<GrowthRecord>(tx, &growth::key(registration.intent.claim_id))?
        .ok_or_else(unknown)?;
    Ok(Some((registration, claim)))
}
