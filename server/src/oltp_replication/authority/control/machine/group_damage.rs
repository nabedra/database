//! NS14 and NS14b: applying `GroupDamaged`, `GroupDamageReplacementBegin` and
//! `GroupDamageReplaced` (see `control/group_damage.rs`).
use super::super::group_damage::{
    ACTIVE_KEY, ActiveDamageReplacement, GroupDamagePhase, GroupDamageRecord,
    GroupDamageReplacement, GroupDamageReplacementPlan, GroupDamageReport, MAX_ACTIVE_PER_POOL,
    MAX_ACTIVE_REPLACEMENTS, record_key,
};
use super::*;

/// Rule 2: the first report of a replica is recorded; any later report of the
/// same replica returns that record unchanged, whatever its phase.
pub(super) fn report(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    worker: WorkerIdentity,
    report: &GroupDamageReport,
) -> Result<ControlReply, ProtocolError> {
    if report.validate().is_err()
        || !same_scope(scope, &report.group)
        || report.replica.worker_id != worker.worker_id
    {
        return fault(Fault::Invalid);
    }
    let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))?
        .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == worker);
    if !enrolled {
        return fault(Fault::Unauthorized);
    }
    let key = record_key(report.group.group_id, report.replica.node_id);
    if let Some(existing) = load::<GroupDamageRecord>(tx, &key)? {
        if existing.report.replica != report.replica || existing.report.group != report.group {
            return fault(Fault::Conflict);
        }
        return Ok(ControlReply::GroupDamage(Box::new(existing)));
    }
    let record = GroupDamageRecord {
        report: report.clone(),
        worker,
        reported_at: position,
        phase: GroupDamagePhase::Reported,
    };
    save(tx, &key, &record)?;
    Ok(ControlReply::GroupDamage(Box::new(record)))
}

/// NS14b rule 3, first step: Control's scheduler claims the group for one
/// fresh identity and placement. The record moves to `Replacing { plan }`;
/// repeating the same plan returns it, any other plan conflicts. The group's
/// replica-move claim and the fresh node ID are taken in the same apply, so
/// neither a replica move nor a second replacement can start for the group,
/// and the node ID is never reused. Every check runs before the first write.
pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    position: AppliedPosition,
    authorization: &AuthorizationStamp,
    plan: &GroupDamageReplacementPlan,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.evidence != crate::oltp_replication::owned::serialized_digest(plan)?
    {
        return fault(Fault::Unauthorized);
    }
    if plan.validate().is_err() || !same_scope(scope, &plan.group) {
        return fault(Fault::Invalid);
    }
    let record_key = record_key(plan.group.group_id, plan.damaged.node_id);
    let Some(mut record) = load::<GroupDamageRecord>(tx, &record_key)? else {
        return fault(Fault::NotFound);
    };
    if record.report.group != plan.group || record.report.replica != plan.damaged {
        return fault(Fault::Invalid);
    }
    match &record.phase {
        GroupDamagePhase::Replacing { plan: existing, .. } if **existing == *plan => {
            return Ok(ControlReply::GroupDamage(Box::new(record)));
        }
        GroupDamagePhase::Replacing { .. } => return fault(Fault::Conflict),
        GroupDamagePhase::Replaced { .. } => {
            return Ok(ControlReply::GroupDamage(Box::new(record)));
        }
        GroupDamagePhase::Reported => {}
    }
    let group_id = plan.group.group_id;
    let expected = &plan.expected;
    if load::<GroupRecord>(tx, &key(GROUPS, group_id))?.as_ref() != Some(expected)
        || expected.replica_root != root(&plan.members)
    {
        return fault(Fault::Stale);
    }
    for member in &plan.members {
        if load::<ReplicaMember>(tx, &member_key(expected.generation, member.node_id))?.as_ref()
            != Some(member)
        {
            return fault(Fault::Stale);
        }
    }
    if load::<WorkerRecord>(tx, &key(WORKERS, plan.replacement.worker_id))?.is_none() {
        return fault(Fault::Invalid);
    }
    // The fresh replica is a learner first: once `data.direct.v1` is active
    // its worker must decode direct entries.
    if !direct::admits_worker_id(tx, plan.replacement.worker_id)? {
        return fault(Fault::DirectCapabilityRequired);
    }
    if pool_drain::blocks_pool(tx, plan.replacement.pool_id)?
        || worker_drain::blocks_worker(tx, plan.replacement.worker_id)?
        || tx.get(&placement::claim_key(group_id))?.is_some()
    {
        return fault(Fault::Busy);
    }
    if tx
        .get(&placement::occupied(group_id, plan.replacement.node_id))?
        .is_some()
    {
        return fault(Fault::Conflict);
    }
    let mut active = load::<Vec<ActiveDamageReplacement>>(tx, ACTIVE_KEY)?.unwrap_or_default();
    let pools = [plan.damaged.pool_id, plan.replacement.pool_id];
    let busy_pool = pools.iter().any(|pool| {
        active
            .iter()
            .filter(|running| running.pools.contains(pool))
            .count()
            >= MAX_ACTIVE_PER_POOL
    });
    if active.len() >= MAX_ACTIVE_REPLACEMENTS || busy_pool {
        return fault(Fault::Busy);
    }
    save(tx, &placement::claim_key(group_id), &plan.generation()?)?;
    for member in [&plan.replacement, &plan.damaged] {
        save(tx, &placement::occupied(group_id, member.node_id), member)?;
    }
    active.push(ActiveDamageReplacement {
        group_id,
        node_id: plan.damaged.node_id,
        pools,
    });
    save(tx, ACTIVE_KEY, &active)?;
    record.phase = GroupDamagePhase::Replacing {
        plan: Box::new(plan.clone()),
        claimed_at: position,
    };
    save(tx, &record_key, &record)?;
    Ok(ControlReply::GroupDamage(Box::new(record)))
}

/// Rule 3: records that the change removing the damaged replica committed,
/// and publishes the plan's placement in the same apply: a new generation
/// whose members exclude the damaged replica. Only the record's own plan can
/// complete it, and only after the change committed past the published
/// placement. A replay returns the record unchanged.
pub(super) fn replaced(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    authorization: &AuthorizationStamp,
    proof: &GroupDamageReplacement,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.evidence != crate::oltp_replication::owned::serialized_digest(proof)?
    {
        return fault(Fault::Unauthorized);
    }
    if !same_scope(scope, &proof.group)
        || proof.replacement.validate().is_err()
        || proof.replacement.node_id == proof.removed.node_id
        || !position_valid(proof.committed_at)
    {
        return fault(Fault::Invalid);
    }
    let record_key = record_key(proof.group.group_id, proof.removed.node_id);
    let Some(mut record) = load::<GroupDamageRecord>(tx, &record_key)? else {
        return fault(Fault::NotFound);
    };
    if record.report.group != proof.group || record.report.replica != proof.removed {
        return fault(Fault::Invalid);
    }
    let plan = match &record.phase {
        GroupDamagePhase::Replaced {
            replacement,
            committed_at,
            ..
        } if *replacement == proof.replacement && *committed_at == proof.committed_at => {
            return Ok(ControlReply::GroupDamage(Box::new(record)));
        }
        GroupDamagePhase::Replaced { .. } => return fault(Fault::Conflict),
        // Without a plan nothing would publish the routes.
        GroupDamagePhase::Reported => return fault(Fault::NotReady),
        GroupDamagePhase::Replacing { plan, .. } => plan.clone(),
    };
    if plan.replacement != proof.replacement {
        return fault(Fault::Conflict);
    }
    if proof.committed_at.index <= plan.expected.membership_position.index {
        return fault(Fault::Invalid);
    }
    let group_id = plan.group.group_id;
    let generation = plan.generation()?;
    let claim = placement::claim_key(group_id);
    if load::<GroupRecord>(tx, &key(GROUPS, group_id))?.as_ref() != Some(&plan.expected)
        || load::<Uuid>(tx, &claim)? != Some(generation)
    {
        return fault(Fault::Stale);
    }
    let next = plan.next_members();
    let placement = GroupRecord {
        identity: plan.group.clone(),
        generation,
        replica_count: next.len() as u64,
        replica_root: root(&next),
        membership_position: proof.committed_at,
    };
    let mut active = load::<Vec<ActiveDamageReplacement>>(tx, ACTIVE_KEY)?.unwrap_or_default();
    active
        .retain(|running| running.group_id != group_id || running.node_id != plan.damaged.node_id);
    for member in &next {
        save(tx, &member_key(generation, member.node_id), member)?;
        save(tx, &member_worker_key(generation, member.worker_id), member)?;
    }
    save(tx, &key(GROUPS, group_id), &placement)?;
    state.group_inventory_revision = state
        .group_inventory_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    tx.delete(&claim)?;
    save(tx, ACTIVE_KEY, &active)?;
    record.phase = GroupDamagePhase::Replaced {
        replacement: proof.replacement.clone(),
        committed_at: proof.committed_at,
        published: Some(Box::new(placement)),
    };
    save(tx, &record_key, &record)?;
    Ok(ControlReply::GroupDamage(Box::new(record)))
}

fn root(members: &[ReplicaMember]) -> Digest {
    group_root(
        members.len() as u64,
        members.iter().fold(group_seed(), group_step),
    )
}
