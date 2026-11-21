use super::*;
use crate::oltp_replication::authority::control::{
    MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup, ReplacementIntent, ReplacementRecord,
    ReplacementMountProof, SystemRecoveryCommand,
};
use crate::oltp_replication::owned::serialized_digest;

pub(in crate::oltp_replication::authority::control) const ACTIVATION: &[u8] =
    b"ct/system-replacement-activation";

pub(in crate::oltp_replication::authority::control) const EXECUTION: &[u8] =
    b"ct/system-replacement-execution";
pub(super) const PREPARING: &[u8] = b"ct/system-replacement-preparing";
pub(super) const REVOKED_WORKERS: &[u8] = b"ct/system-replacement-revoked-worker/";
pub(in crate::oltp_replication::authority::control) const ACTIVE: &[u8] =
    b"ct/system-replacement-active";

pub(in crate::oltp_replication::authority::control) fn revoked_id(worker: Uuid) -> Vec<u8> {
    key(REVOKED_WORKERS, worker)
}

pub(in crate::oltp_replication::authority::control) fn revoked_certificate(certificate: Digest) -> Vec<u8> {
    let mut key = b"ct/system-replacement-revoked-certificate/".to_vec();
    key.extend_from_slice(certificate.as_bytes());
    key
}

pub(super) fn blocked_enrollment(
    tx: &mut GroupTransaction<'_>,
    enrollment: &WorkerEnrollment,
) -> Result<bool, ProtocolError> {
    let reserved = load::<Uuid>(tx, &by_certificate(enrollment.certificate_sha256))?;
    let target = load::<Uuid>(tx, &by_target(enrollment.worker_id))?;
    Ok(tx.get(&revoked_id(enrollment.worker_id))?.is_some()
        || tx.get(&revoked_certificate(enrollment.certificate_sha256))?.is_some()
        || (reserved.is_some() && reserved != target)
        || (target.is_some() && tx.get(&key(WORKERS, enrollment.worker_id))?.is_none()))
}

pub(in crate::oltp_replication::authority::control) fn by_worker(worker: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-worker/", worker)
}

pub(in crate::oltp_replication::authority::control) fn by_id(id: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-id/", id)
}

pub(in crate::oltp_replication::authority::control) fn by_target(worker: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-target/", worker)
}

fn by_certificate(certificate: Digest) -> Vec<u8> {
    let mut key = b"ct/system-replacement-certificate/".to_vec();
    key.extend_from_slice(certificate.as_bytes());
    key
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    position: AppliedPosition,
    operation_id: Uuid,
    command: &SystemRecoveryCommand,
) -> Result<ControlReply, ProtocolError> {
    let (authorization, evidence) = match command {
        SystemRecoveryCommand::Mount { proof, ready, enrollment_revision, group_inventory_revision } =>
            return mount(tx, scope, state, proof, *ready, *enrollment_revision, *group_inventory_revision),
        SystemRecoveryCommand::Publish { id, group, applied, authorization } =>
            (authorization, serialized_digest(&(id, group, applied))?),
        SystemRecoveryCommand::Enable {
            group,
            execution,
            voters,
            enrollment_revision,
            authorization,
        } => (
            authorization,
            if *execution {
                serialized_digest(&(group, voters, enrollment_revision, execution))?
            } else {
                serialized_digest(&(group, voters, enrollment_revision))?
            },
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
        SystemRecoveryCommand::Mount { .. } => unreachable!("mount applied above"),
        SystemRecoveryCommand::Publish { id, group, applied, .. } =>
            publish(tx, state, *id, *group, *applied),
        SystemRecoveryCommand::Enable {
            group,
            execution,
            voters,
            enrollment_revision,
            ..
        } => {
            if let Some(existing) = load(tx, if *execution { EXECUTION } else { ACTIVATION })? {
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
            if tx.get(ACTIVATION)?.is_none() {
                save(tx, ACTIVATION, &position)?;
            }
            if *execution {
                save(tx, EXECUTION, &position)?;
            }
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
            if tx.get(&by_id(intent.id))?.is_some()
                || (intent.mount_public_key.is_some() && tx.get(PREPARING)?.is_some())
            {
                return fault(Fault::Conflict);
            }
            let target = by_target(intent.next.worker_id);
            let certificate = by_certificate(intent.next.certificate_sha256);
            if tx.get(&target)?.is_some() || tx.get(&certificate)?.is_some() {
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
                || blocked_enrollment(tx, &intent.next)?
                || !certificate_unused(tx, intent.next.certificate_sha256)?
                || intent.mount_public_key.is_some_and(|digest| digest.as_bytes() == &[0; 32])
                || (intent.mount_public_key.is_some() && tx.get(EXECUTION)?.is_none())
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
                inventory_revision: intent.mount_public_key.map(|_| *group_inventory_revision),
            };
            save(tx, &lookup, &intent.id)?;
            save(tx, &by_id(intent.id), &record)?;
            save(tx, &target, &intent.id)?;
            save(tx, &certificate, &intent.id)?;
            if intent.mount_public_key.is_some() {
                save(tx, PREPARING, &intent.id)?;
            }
            Ok(ControlReply::SystemReplacementPrepared(Box::new(record)))
        }
    }
}

fn certificate_unused(
    tx: &mut GroupTransaction<'_>,
    certificate: Digest,
) -> Result<bool, ProtocolError> {
    for prefix in [WORKERS, APIS] {
        let mut after = None;
        loop {
            let mut found = false;
            let mut cursor = None;
            let progress = tx.scan_page(prefix, after.as_deref(), 128, |key, raw| {
                let candidate = if prefix == WORKERS {
                    decode::<WorkerRecord>(raw)?.enrollment.certificate_sha256
                } else {
                    decode::<ApiEnrollment>(raw)?.transport().certificate_sha256
                };
                found |= candidate == certificate;
                cursor = Some(key.to_vec());
                Ok(())
            })?;
            if found {
                return Ok(false);
            }
            if progress.exhausted {
                break;
            }
            after = cursor;
        }
    }
    Ok(true)
}

fn validate_groups(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    intent: &ReplacementIntent,
    groups: &[ReplacementGroup],
) -> Result<bool, ProtocolError> {
    let mut previous_group = None;
    for group in groups {
        let Ok(expected) = ReplacementGroup::new(group.expected.clone(), group.old.clone(), intent)
        else {
            return Ok(false);
        };
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
            || expected.learner != group.learner
        {
            return Ok(false);
        }
        previous_group = Some(id);
    }
    Ok(groups.iter().any(|group| group.expected.identity == *scope))
}

fn mount(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    proof: &ReplacementMountProof,
    ready: ReadyMembership,
    enrollment_revision: u64,
    group_inventory_revision: u64,
) -> Result<ControlReply, ProtocolError> {
    if tx.get(EXECUTION)?.is_none() {
        return fault(Fault::NotReady);
    }
    let Some(record) = load::<ReplacementRecord>(tx, &by_id(proof.id))? else {
        return fault(Fault::NotFound);
    };
    if proof.verify(scope, &record).is_err() || state.ready.view.run_id != record.intent.run_id {
        return fault(Fault::Unauthorized);
    }
    let old = record.intent.previous;
    if load::<WorkerIdentity>(tx, &revoked_id(old.worker_id))? == Some(old) {
        return if load::<WorkerRecord>(tx, &key(WORKERS, proof.worker.worker_id))?
            .is_some_and(|worker| WorkerIdentity::from(&worker.enrollment) == proof.worker)
        {
            Ok(ControlReply::SystemReplacementMounted)
        } else {
            fault(Fault::Stale)
        };
    }
    if state.ready != ready
        || state.enrollment_revision != enrollment_revision
        || state.group_inventory_revision != group_inventory_revision
        || record.inventory_revision != Some(group_inventory_revision)
        || state.pending.is_some()
        || state.restart.is_some()
        || state.invalid_ready != 0
        || state.owner.is_none_or(|owner| owner.worker_id == old.worker_id)
        || tx.get(ACTIVE)?.is_some()
        || tx.get(placement::ACTIVE)?.is_some()
        || tx.get(data_worker::PREPARED)?.is_some()
        || tx.get(&ready_key(state.ready_generation, old.worker_id))?.is_some()
        || load::<WorkerRecord>(tx, &key(WORKERS, old.worker_id))?
            .is_none_or(|worker| WorkerIdentity::from(&worker.enrollment) != old)
        || tx.get(&key(WORKERS, proof.worker.worker_id))?.is_some()
        || tx.get(&key(APIS, proof.worker.worker_id))?.is_some()
        || !certificate_unused(tx, proof.worker.certificate_sha256)?
        || !validate_groups(tx, scope, &record.intent, &record.groups)?
    {
        return fault(Fault::Stale);
    }
    for plan in &record.groups {
        save(tx, &placement::claim_key(plan.expected.identity.group_id), &proof.id)?;
    }
    save(tx, ACTIVE, &proof.id)?;
    save(tx, &revoked_id(old.worker_id), &old)?;
    save(tx, &revoked_certificate(old.certificate_sha256), &old)?;
    tx.delete(&key(WORKERS, old.worker_id))?;
    save(tx, &key(WORKERS, proof.worker.worker_id), &WorkerRecord {
        enrollment: record.intent.next,
        previous_process: None,
    })?;
    state.enrollment_revision = state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
    Ok(ControlReply::SystemReplacementMounted)
}

pub(in crate::oltp_replication::authority::control) fn published(id: Uuid, group: Uuid) -> Vec<u8> {
    key(&key(b"ct/system-replacement-published/", id), group)
}

fn publish(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    group: Uuid,
    applied: AppliedPosition,
) -> Result<ControlReply, ProtocolError> {
    if let Some(existing) = load(tx, &published(id, group))? {
        return Ok(ControlReply::SystemReplacementPublished(existing));
    }
    let Some(record) = load::<ReplacementRecord>(tx, &by_id(id))? else {
        return fault(Fault::NotFound);
    };
    let Some(plan) = record.groups.iter().find(|plan| plan.expected.identity.group_id == group) else {
        return fault(Fault::Invalid);
    };
    let claim = placement::claim_key(group);
    if load::<Uuid>(tx, ACTIVE)? != Some(id)
        || state.ready.view.run_id != record.intent.run_id
        || load::<Uuid>(tx, &claim)? != Some(id)
        || load::<GroupRecord>(tx, &key(GROUPS, group))?.as_ref() != Some(&plan.expected)
        || !position_valid(applied)
        || applied.index <= plan.expected.membership_position.index
    {
        return fault(Fault::Stale);
    }
    let remaining = record.groups.iter().filter(|item| item.expected.identity.group_id != group)
        .try_fold(0, |count, item| {
            Ok::<_, ProtocolError>(count + usize::from(tx.get(&published(id, item.expected.identity.group_id))?.is_none()))
        })?;
    let next = plan.next(record.intent.previous.worker_id)?;
    let digest = serialized_digest(&(b"nabedra/system-replacement-placement/v1".as_slice(), id, group))?;
    let generation = Uuid::from_bytes(digest.as_bytes()[..16].try_into().expect("digest prefix"));
    let placement = GroupRecord {
        identity: plan.expected.identity.clone(),
        generation,
        replica_count: 3,
        replica_root: group_root(3, next.iter().fold(group_seed(), group_step)),
        membership_position: applied,
    };
    for member in &next {
        save(tx, &member_key(generation, member.node_id), member)?;
        save(tx, &member_worker_key(generation, member.worker_id), member)?;
        save(tx, &placement::occupied(group, member.node_id), member)?;
    }
    save(tx, &key(GROUPS, group), &placement)?;
    save(tx, &published(id, group), &placement)?;
    state.group_inventory_revision = state.group_inventory_revision.checked_add(1).ok_or_else(unknown)?;
    tx.delete(&claim)?;
    if remaining == 0 {
        tx.delete(ACTIVE)?;
        tx.delete(PREPARING)?;
    }
    Ok(ControlReply::SystemReplacementPublished(placement))
}

pub(super) fn waits_for_groups(
    tx: &mut GroupTransaction<'_>,
    worker: Uuid,
) -> Result<bool, ProtocolError> {
    let Some(active) = load::<Uuid>(tx, ACTIVE)? else { return Ok(false) };
    Ok(load::<Uuid>(tx, &by_target(worker))? == Some(active))
}
