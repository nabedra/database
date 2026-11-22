use super::*;
use crate::oltp_replication::authority::control::{
    MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup, ReplacementIntent, ReplacementMountProof,
    ReplacementRecord, SystemRecoveryCommand,
};
use crate::oltp_replication::owned::serialized_digest;

pub(in crate::oltp_replication::authority::control) const ACTIVATION: &[u8] =
    b"ct/system-replacement-activation";

pub(in crate::oltp_replication::authority::control) const EXECUTION: &[u8] =
    b"ct/system-replacement-execution";
pub(in crate::oltp_replication::authority::control) const HARDENING: &[u8] =
    b"ct/system-replacement-hardening";
pub(in crate::oltp_replication::authority::control) const DEFERRED_DATA: &[u8] =
    b"ct/system-replacement-deferred-data";
pub(super) const MAX_REVOCATIONS: usize = 1024;
pub(super) const PREPARING: &[u8] = b"ct/system-replacement-preparing";
pub(in crate::oltp_replication::authority::control) const REVOKED_WORKERS: &[u8] =
    b"ct/system-replacement-revoked-worker/";
pub(in crate::oltp_replication::authority::control) const ACTIVE: &[u8] =
    b"ct/system-replacement-active";

pub(in crate::oltp_replication::authority::control) fn revoked_id(worker: Uuid) -> Vec<u8> {
    key(REVOKED_WORKERS, worker)
}

pub(in crate::oltp_replication::authority::control) fn revoked_certificate(
    certificate: Digest,
) -> Vec<u8> {
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
        || tx
            .get(&revoked_certificate(enrollment.certificate_sha256))?
            .is_some()
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

pub(in crate::oltp_replication::authority::control) fn abandoned(id: Uuid) -> Vec<u8> {
    key(b"ct/system-replacement-abandoned/", id)
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
        SystemRecoveryCommand::Mount {
            proof,
            ready,
            enrollment_revision,
            group_inventory_revision,
        } => {
            return mount(
                tx,
                scope,
                state,
                proof,
                *ready,
                *enrollment_revision,
                *group_inventory_revision,
            );
        }
        SystemRecoveryCommand::Abandon { id, authorization } => (
            authorization,
            serialized_digest(&(b"abandon-system-replacement".as_slice(), id))?,
        ),
        SystemRecoveryCommand::QueueLostData {
            after,
            authorization,
        } => (
            authorization,
            serialized_digest(&(b"queue-lost-data".as_slice(), after))?,
        ),
        SystemRecoveryCommand::Publish {
            id,
            group,
            applied,
            authorization,
        } => (authorization, serialized_digest(&(id, group, applied))?),
        SystemRecoveryCommand::Enable {
            group,
            execution,
            hardening,
            voters,
            enrollment_revision,
            authorization,
        } => (
            authorization,
            if *hardening {
                serialized_digest(&(group, voters, enrollment_revision, execution, hardening))?
            } else if *execution {
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
        SystemRecoveryCommand::Abandon { id, .. } => {
            if tx.get(HARDENING)?.is_none() {
                return fault(Fault::NotReady);
            }
            if tx.get(&abandoned(*id))?.is_some() {
                return Ok(ControlReply::SystemReplacementAbandoned);
            }
            let Some(record) = load::<ReplacementRecord>(tx, &by_id(*id))? else {
                return fault(Fault::NotFound);
            };
            if load::<Uuid>(tx, &by_worker(record.intent.previous.worker_id))? != Some(*id)
                || tx
                    .get(&revoked_id(record.intent.previous.worker_id))?
                    .is_some()
            {
                return fault(Fault::Conflict);
            }
            retire(tx, &record)?;
            Ok(ControlReply::SystemReplacementAbandoned)
        }
        SystemRecoveryCommand::QueueLostData { after, .. } => queue_lost_data(tx, position, *after),
        SystemRecoveryCommand::Publish {
            id, group, applied, ..
        } => publish(tx, state, *id, *group, *applied),
        SystemRecoveryCommand::Enable {
            group,
            execution,
            hardening,
            voters,
            enrollment_revision,
            ..
        } => {
            let capability = if *hardening {
                HARDENING
            } else if *execution {
                EXECUTION
            } else {
                ACTIVATION
            };
            if let Some(existing) = load(tx, capability)? {
                return Ok(ControlReply::SystemReplacementEnabled(existing));
            }
            if *enrollment_revision != state.enrollment_revision
                || group.identity != *scope
                || group.replica_count != 3
                || load::<GroupRecord>(tx, &key(GROUPS, scope.group_id))?.as_ref()
                    != Some(group.as_ref())
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
            if *execution && tx.get(EXECUTION)?.is_none() {
                save(tx, EXECUTION, &position)?;
            }
            if *hardening {
                save(tx, HARDENING, &position)?;
                if let Some(record) = unmounted_execution(tx)?
                    && record.intent.run_id != state.ready.view.run_id
                {
                    retire(tx, &record)?;
                }
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
            let obsolete = if let Some(id) = load::<Uuid>(tx, &lookup)? {
                let record: ReplacementRecord = load(tx, &by_id(id))?.ok_or_else(unknown)?;
                if record.intent == **intent && record.intent.run_id == state.ready.view.run_id {
                    return Ok(ControlReply::SystemReplacementPrepared(Box::new(record)));
                }
                if tx.get(HARDENING)?.is_none()
                    || record.intent.run_id == state.ready.view.run_id
                    || tx
                        .get(&revoked_id(record.intent.previous.worker_id))?
                        .is_some()
                {
                    return fault(Fault::Conflict);
                }
                Some(record)
            } else {
                None
            };
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
                || intent
                    .mount_public_key
                    .is_some_and(|digest| digest.as_bytes() == &[0; 32])
                || (intent.mount_public_key.is_some() && tx.get(EXECUTION)?.is_none())
            {
                return fault(Fault::Invalid);
            }
            if !validate_groups(tx, scope, intent, groups)? {
                return fault(Fault::Stale);
            }
            let record = ReplacementRecord {
                intent: intent.as_ref().clone(),
                groups: groups.to_vec(),
                applied_at: position,
            };
            if let Some(obsolete) = obsolete {
                retire(tx, &obsolete)?;
            }
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

fn retire(tx: &mut GroupTransaction<'_>, record: &ReplacementRecord) -> Result<(), ProtocolError> {
    let id = record.intent.id;
    save(tx, &abandoned(id), &true)?;
    tx.delete(&by_worker(record.intent.previous.worker_id))?;
    if load::<Uuid>(tx, PREPARING)? == Some(id) {
        tx.delete(PREPARING)?;
    }
    // The target and certificate reservations permanently fence this candidate.
    Ok(())
}

fn unmounted_execution(
    tx: &mut GroupTransaction<'_>,
) -> Result<Option<ReplacementRecord>, ProtocolError> {
    if tx.get(ACTIVE)?.is_none()
        && let Some(id) = load::<Uuid>(tx, PREPARING)?
    {
        return load::<ReplacementRecord>(tx, &by_id(id))?
            .ok_or_else(unknown)
            .map(Some);
    }
    Ok(None)
}

pub(super) fn retire_on_cold_fence(tx: &mut GroupTransaction<'_>) -> Result<(), ProtocolError> {
    if tx.get(HARDENING)?.is_some()
        && let Some(record) = unmounted_execution(tx)?
    {
        retire(tx, &record)?;
    }
    Ok(())
}

fn revocation_capacity_available(tx: &mut GroupTransaction<'_>) -> Result<bool, ProtocolError> {
    let mut count = 0;
    let mut after = None;
    loop {
        let mut next = None;
        let page = tx.scan_page(REVOKED_WORKERS, after.as_deref(), 128, |key, _| {
            count += 1;
            next = Some(key.to_vec());
            Ok(())
        })?;
        if count >= MAX_REVOCATIONS {
            return Ok(false);
        }
        if page.exhausted {
            return Ok(true);
        }
        after = next;
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
    if proof.verify(scope, &record).is_err()
        || state.ready.view.run_id != record.intent.run_id
        || tx.get(&abandoned(proof.id))?.is_some()
    {
        return fault(Fault::Unauthorized);
    }
    let old = record.intent.previous;
    let hardened = tx.get(HARDENING)?.is_some();
    if load::<WorkerIdentity>(tx, &revoked_id(old.worker_id))? == Some(old) {
        if hardened {
            return fault(Fault::Conflict);
        }
        return if load::<WorkerRecord>(tx, &key(WORKERS, proof.worker.worker_id))?
            .is_some_and(|worker| WorkerIdentity::from(&worker.enrollment) == proof.worker)
        {
            Ok(ControlReply::SystemReplacementMounted)
        } else {
            fault(Fault::Stale)
        };
    }
    let defer_data = hardened && state.group_count > MAX_SYSTEM_REPLACEMENT_GROUPS as u64;
    if state.ready != ready
        || state.enrollment_revision != enrollment_revision
        || state.group_inventory_revision != group_inventory_revision
        || state.pending.is_some()
        || state.restart.is_some()
        || state.invalid_ready != 0
        || state
            .owner
            .is_none_or(|owner| owner.worker_id == old.worker_id)
        || tx.get(ACTIVE)?.is_some()
        || tx.get(placement::ACTIVE)?.is_some()
        || tx.get(data_worker::PREPARED)?.is_some()
        || tx
            .get(&ready_key(state.ready_generation, old.worker_id))?
            .is_some()
        || load::<WorkerRecord>(tx, &key(WORKERS, old.worker_id))?
            .is_none_or(|worker| WorkerIdentity::from(&worker.enrollment) != old)
        || tx.get(&key(WORKERS, proof.worker.worker_id))?.is_some()
        || tx.get(&key(APIS, proof.worker.worker_id))?.is_some()
        || !certificate_unused(tx, proof.worker.certificate_sha256)?
        || !validate_groups(tx, scope, &record.intent, &record.groups)?
        || !covers_every_group(tx, &record, defer_data)?
    {
        return fault(Fault::Stale);
    }
    if hardened && !revocation_capacity_available(tx)? {
        return fault(Fault::Busy);
    }
    let next_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    for plan in &record.groups {
        save(
            tx,
            &placement::claim_key(plan.expected.identity.group_id),
            &proof.id,
        )?;
    }
    save(tx, ACTIVE, &proof.id)?;
    if defer_data {
        save(tx, DEFERRED_DATA, &None::<Uuid>)?;
    }
    save(tx, &revoked_id(old.worker_id), &old)?;
    save(tx, &revoked_certificate(old.certificate_sha256), &old)?;
    tx.delete(&key(WORKERS, old.worker_id))?;
    save(
        tx,
        &key(WORKERS, proof.worker.worker_id),
        &WorkerRecord {
            enrollment: record.intent.next,
            previous_process: None,
        },
    )?;
    state.enrollment_revision = next_revision;
    Ok(ControlReply::SystemReplacementMounted)
}

fn covers_every_group(
    tx: &mut GroupTransaction<'_>,
    record: &ReplacementRecord,
    defer_data: bool,
) -> Result<bool, ProtocolError> {
    let lost = record.intent.previous.worker_id;
    let mut after = None;
    loop {
        let mut next = None;
        let mut candidates = Vec::new();
        let page = tx.scan_page(GROUPS, after.as_deref(), 128, |key, bytes| {
            let group = decode::<GroupRecord>(bytes)?;
            if !defer_data || !matches!(group.identity.kind, GroupKind::Data { .. }) {
                candidates.push((group.generation, group.identity.group_id));
            }
            next = Some(key.to_vec());
            Ok(())
        })?;
        for (generation, group) in candidates {
            if tx.get(&member_worker_key(generation, lost))?.is_some()
                && !record
                    .groups
                    .iter()
                    .any(|plan| plan.expected.identity.group_id == group)
            {
                return Ok(false);
            }
        }
        if page.exhausted {
            return Ok(true);
        }
        after = next;
    }
}

// After the system voters are restored, hand Data replicas to NS14 through
// a durable cursor. Leadership changes resume the next bounded apply page.
fn queue_lost_data(
    tx: &mut GroupTransaction<'_>,
    position: AppliedPosition,
    after: Option<Uuid>,
) -> Result<ControlReply, ProtocolError> {
    use super::super::group_damage::{
        GroupDamagePhase, GroupDamageReason, GroupDamageRecord, GroupDamageReport, record_key,
    };
    if load::<Option<Uuid>>(tx, DEFERRED_DATA)? != Some(after) || tx.get(ACTIVE)?.is_some() {
        return fault(Fault::Stale);
    }
    let mut groups = Vec::new();
    let mut next = after;
    let cursor = after.map(|id| key(GROUPS, id));
    let page = tx.scan_page(GROUPS, cursor.as_deref(), 32, |_, bytes| {
        let group = decode::<GroupRecord>(bytes)?;
        next = Some(group.identity.group_id);
        if matches!(group.identity.kind, GroupKind::Data { .. }) {
            groups.push(group);
        }
        Ok(())
    })?;
    for group in groups {
        let mut members = Vec::new();
        tx.scan_page(&member_prefix(group.generation), None, 128, |_, bytes| {
            members.push(decode::<ReplicaMember>(bytes)?);
            Ok(())
        })?;
        for replica in members {
            let Some(worker) = load::<WorkerIdentity>(tx, &revoked_id(replica.worker_id))? else {
                continue;
            };
            let record = record_key(group.identity.group_id, replica.node_id);
            if tx.get(&record)?.is_none() {
                save(
                    tx,
                    &record,
                    &GroupDamageRecord {
                        report: GroupDamageReport {
                            group: group.identity.clone(),
                            replica,
                            reason: GroupDamageReason::PoolRefused,
                            first_bad_seq: None,
                            refusal: None,
                        },
                        worker,
                        reported_at: position,
                        phase: GroupDamagePhase::Reported,
                    },
                )?;
            }
        }
    }
    if page.exhausted {
        tx.delete(DEFERRED_DATA)?;
    } else {
        save(tx, DEFERRED_DATA, &next)?;
    }
    Ok(ControlReply::SystemReplacementDataQueued)
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
    let Some(plan) = record
        .groups
        .iter()
        .find(|plan| plan.expected.identity.group_id == group)
    else {
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
    let remaining = record
        .groups
        .iter()
        .filter(|item| item.expected.identity.group_id != group)
        .try_fold(0, |count, item| {
            Ok::<_, ProtocolError>(
                count
                    + usize::from(
                        tx.get(&published(id, item.expected.identity.group_id))?
                            .is_none(),
                    ),
            )
        })?;
    let next = plan.next(record.intent.previous.worker_id)?;
    let digest = serialized_digest(&(
        b"nabedra/system-replacement-placement/v1".as_slice(),
        id,
        group,
    ))?;
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
    state.group_inventory_revision = state
        .group_inventory_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
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
    let Some(active) = load::<Uuid>(tx, ACTIVE)? else {
        return Ok(false);
    };
    Ok(load::<Uuid>(tx, &by_target(worker))? == Some(active))
}
