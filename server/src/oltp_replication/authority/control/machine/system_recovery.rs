use super::*;
use crate::oltp_replication::authority::control::{
    MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup, ReplacementIntent, ReplacementRecord,
    SystemRecoveryCommand,
};
use crate::oltp_replication::owned::serialized_digest;

pub(in crate::oltp_replication::authority::control) const ACTIVATION: &[u8] =
    b"ct/system-replacement-activation";

pub(in crate::oltp_replication::authority::control) fn by_worker(worker: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-worker/", worker)
}

pub(in crate::oltp_replication::authority::control) fn by_id(id: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-id/", id)
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    position: AppliedPosition,
    operation_id: Uuid,
    command: &SystemRecoveryCommand,
) -> Result<ControlReply, ProtocolError> {
    let (authorization, evidence) = match command {
        SystemRecoveryCommand::Enable {
            group,
            voters,
            enrollment_revision,
            authorization,
        } => (
            authorization,
            serialized_digest(&(group, voters, enrollment_revision))?,
        ),
        SystemRecoveryCommand::Prepare {
            intent,
            ready,
            enrollment_revision,
            group_inventory_revision,
            groups,
            authorization,
        } => (
            authorization,
            serialized_digest(&(
                intent,
                ready,
                enrollment_revision,
                group_inventory_revision,
                groups,
            ))?,
        ),
    };
    if !stamp(authorization, state.policy)
        || authorization.authorization_id != operation_id
        || authorization.evidence != evidence
    {
        return fault(Fault::Unauthorized);
    }
    match command {
        SystemRecoveryCommand::Enable {
            group,
            voters,
            enrollment_revision,
            ..
        } => {
            if let Some(existing) = load(tx, ACTIVATION)? {
                return Ok(ControlReply::SystemReplacementEnabled(existing));
            }
            if *enrollment_revision != state.enrollment_revision
                || group.identity != *scope
                || group.replica_count != 3
                || load::<GroupRecord>(tx, &key(GROUPS, scope.group_id))?.as_ref() != Some(group)
            {
                return fault(Fault::Stale);
            }
            let mut workers = BTreeSet::new();
            for voter in voters {
                if !workers.insert(voter.worker_id)
                    || load::<WorkerRecord>(tx, &key(WORKERS, voter.worker_id))?
                        .is_none_or(|record| WorkerIdentity::from(&record.enrollment) != *voter)
                    || tx
                        .get(&member_worker_key(group.generation, voter.worker_id))?
                        .is_none()
                {
                    return fault(Fault::Unauthorized);
                }
            }
            save(tx, ACTIVATION, &position)?;
            Ok(ControlReply::SystemReplacementEnabled(position))
        }
        SystemRecoveryCommand::Prepare {
            intent,
            ready,
            enrollment_revision,
            group_inventory_revision,
            groups,
            ..
        } => {
            if tx.get(ACTIVATION)?.is_none() {
                return fault(Fault::NotReady);
            }
            let lookup = by_worker(intent.previous.worker_id);
            if let Some(id) = load::<Uuid>(tx, &lookup)? {
                let record: ReplacementRecord = load(tx, &by_id(id))?.ok_or_else(unknown)?;
                return if record.intent == *intent {
                    Ok(ControlReply::SystemReplacementPrepared(Box::new(record)))
                } else {
                    fault(Fault::Conflict)
                };
            }
            if tx.get(&by_id(intent.id))?.is_some() {
                return fault(Fault::Conflict);
            }
            if *ready != state.ready
                || *enrollment_revision != state.enrollment_revision
                || *group_inventory_revision != state.group_inventory_revision
                || intent.run_id != state.ready.view.run_id
                || state.pending.is_some()
                || state.restart.is_some()
                || state.invalid_ready != 0
                || state
                    .owner
                    .is_none_or(|owner| owner.worker_id == intent.previous.worker_id)
                || tx
                    .get(&ready_key(
                        state.ready_generation,
                        intent.previous.worker_id,
                    ))?
                    .is_some()
                || load::<WorkerRecord>(tx, &key(WORKERS, intent.previous.worker_id))?.is_none_or(
                    |record| WorkerIdentity::from(&record.enrollment) != intent.previous,
                )
            {
                return fault(Fault::Stale);
            }
            if intent.validate().is_err()
                || intent.mount.identity.cluster_id != scope.cluster_id
                || groups.is_empty()
                || groups.len() > MAX_SYSTEM_REPLACEMENT_GROUPS
                || tx.get(&key(WORKERS, intent.next.worker_id))?.is_some()
                || tx.get(&key(APIS, intent.next.worker_id))?.is_some()
                || tx.get(&by_worker(intent.next.worker_id))?.is_some()
            {
                return fault(Fault::Invalid);
            }
            if !validate_groups(tx, scope, intent, groups)? {
                return fault(Fault::Stale);
            }
            let record = ReplacementRecord {
                intent: intent.clone(),
                groups: groups.to_vec(),
                applied_at: position,
            };
            save(tx, &lookup, &intent.id)?;
            save(tx, &by_id(intent.id), &record)?;
            Ok(ControlReply::SystemReplacementPrepared(Box::new(record)))
        }
    }
}

fn validate_groups(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    intent: &ReplacementIntent,
    groups: &[ReplacementGroup],
) -> Result<bool, ProtocolError> {
    let mut previous_group = None;
    for group in groups {
        let id = group.expected.identity.group_id;
        if previous_group.is_some_and(|previous| id <= previous)
            || !same_scope(scope, &group.expected.identity)
            || validate_voters(&group.old).is_err()
            || group
                .old
                .iter()
                .all(|member| member.worker_id != intent.previous.worker_id)
            || group
                .old
                .iter()
                .any(|member| member.pool_id == intent.mount.identity.pool_id)
            || group.expected.replica_count != 3
            || group.expected.replica_root
                != group_root(3, group.old.iter().fold(group_seed(), group_step))
            || load::<GroupRecord>(tx, &key(GROUPS, id))?.as_ref() != Some(&group.expected)
            || tx.get(&placement::claim_key(id))?.is_some()
            || tx
                .get(&placement::occupied(id, group.learner.node_id))?
                .is_some()
            || ReplacementGroup::new(group.expected.clone(), group.old.clone(), intent)?.learner
                != group.learner
        {
            return Ok(false);
        }
        previous_group = Some(id);
    }
    Ok(groups.iter().any(|group| group.expected.identity == *scope))
}
