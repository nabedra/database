use super::*;
use crate::oltp_replication::authority::control::scoped_queue::*;
use crate::oltp_replication::owned::serialized_digest;

pub(in crate::oltp_replication::authority::control) mod capability;
mod transfer;

pub(in crate::oltp_replication::authority::control) fn placement_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/scoped-queue/", group_id)
}

pub(in crate::oltp_replication::authority::control) fn replacement_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/group-replacement/", group_id)
}

pub(in crate::oltp_replication::authority::control) fn transfer_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/scoped-queue-transfer/", group_id)
}

pub(in crate::oltp_replication::authority::control) fn completed_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/scoped-queue-completed/", group_id)
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    operation_id: Uuid,
    actor: WorkerIdentity,
    action: &ScopedQueueOperation,
    authorization: Option<&AuthorizationStamp>,
) -> Result<ControlReply, ProtocolError> {
    if actor.validate().is_err() {
        return fault(Fault::Unauthorized);
    }
    if action.requires_operator() {
        let evidence = serialized_digest(&(scope, actor, action))?;
        if !authorization.is_some_and(|stamp_value| {
            stamp_value.authorization_id == operation_id
                && stamp_value.subject == actor.certificate_sha256
                && stamp_value.evidence == evidence
                && stamp(stamp_value, state.policy)
        }) || !load::<WorkerRecord>(tx, &key(WORKERS, actor.worker_id))?
            .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == actor)
            || tx
                .get(&key(data_worker::REGISTERED, actor.worker_id))?
                .is_some()
        {
            return fault(Fault::Unauthorized);
        }
    } else if authorization.is_some() {
        return fault(Fault::Unauthorized);
    }

    match action {
        ScopedQueueOperation::Advertise { version } => capability::advert(tx, actor, *version),
        ScopedQueueOperation::SetAdmission { expected, enabled } => {
            capability::set(tx, state, operation_id, *expected, *enabled)
        }
        ScopedQueueOperation::Ensure { group } => {
            use capability::Admission;
            if !same_scope(scope, group)
                || !capability::supports(tx, actor)?
                || !capability::is_api(tx, actor)? && !current_ready(tx, state, actor)?
            {
                return fault(Fault::Unauthorized);
            }
            let gate = Admission::capability(load(tx, Admission::key())?, state);
            if gate.activation.is_none() {
                return fault(Fault::NotReady);
            }
            let Some(record) = load::<GroupRecord>(tx, &key(GROUPS, group.group_id))?
                .filter(|record| record.identity == *group)
            else {
                return fault(Fault::Stale);
            };
            create(tx, state, operation_id, &record, gate.run_id, true)
        }
        ScopedQueueOperation::BeginTransfer {
            expected,
            next_owner,
            replaces,
        } => {
            if !same_scope(scope, &expected.group) || expected.validate(&expected.group).is_err() {
                return fault(Fault::Invalid);
            }
            transfer::begin(tx, state, operation_id, expected, *next_owner, *replaces)
        }
        ScopedQueueOperation::RebaseTransfer {
            group_id,
            transfer_id,
            removed,
            next_owner,
        } => {
            if !current_ready(tx, state, actor)? {
                return fault(Fault::Unauthorized);
            }
            transfer::rebase(
                tx,
                state,
                operation_id,
                *group_id,
                *transfer_id,
                *removed,
                *next_owner,
            )
        }
        ScopedQueueOperation::ReportTransfer {
            group_id,
            transfer_id,
            report,
        } => transfer::report(tx, state, actor, *group_id, *transfer_id, *report),
        ScopedQueueOperation::PublishTransfer {
            group_id,
            transfer_id,
        } => transfer::publish(tx, state, actor, *group_id, *transfer_id),
    }
}

fn holder_key(run_id: Uuid, worker: WorkerIdentity) -> Vec<u8> {
    let mut key = key(b"ct/scoped-queue-held/", run_id);
    key.extend_from_slice(worker.worker_id.as_bytes());
    key.extend_from_slice(worker.incarnation.as_bytes());
    key.extend_from_slice(&worker.enrollment_revision.to_be_bytes());
    key
}

pub(super) fn holds_scoped_queues(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(load::<u64>(tx, &holder_key(state.ready.view.run_id, worker))?.unwrap_or(0) > 0)
}

fn change_holders(
    tx: &mut GroupTransaction<'_>,
    previous: Option<&ScopedQueuePlacement>,
    next: &ScopedQueuePlacement,
) -> Result<(), ProtocolError> {
    let run_id = next.view.run_id;
    if let Some(previous) = previous {
        for worker in previous.members {
            if previous.has_holder(worker) && !next.has_holder(worker) {
                let key = holder_key(run_id, worker);
                let count = load::<u64>(tx, &key)?.ok_or_else(unknown)?;
                if count == 1 {
                    tx.delete(&key)?;
                } else {
                    save(tx, &key, &count.checked_sub(1).ok_or_else(unknown)?)?;
                }
            }
        }
    }
    for worker in next.members {
        if next.has_holder(worker) && previous.is_none_or(|previous| !previous.has_holder(worker)) {
            let key = holder_key(run_id, worker);
            let count = load::<u64>(tx, &key)?.unwrap_or(0);
            save(tx, &key, &count.checked_add(1).ok_or_else(unknown)?)?;
        }
    }
    Ok(())
}

/// A published replacement remains claimed until retirement. Transfer may
/// target its new generation once Control has published the exact voters.
fn changing_data(
    tx: &mut GroupTransaction<'_>,
    record: &GroupRecord,
) -> Result<bool, ProtocolError> {
    Ok(
        load::<Uuid>(tx, &replacement_key(record.identity.group_id))?
            .is_some_and(|next| next != record.generation),
    )
}

pub(in crate::oltp_replication::authority::control) fn creation_digest(
    expected: &GroupRecord,
    run_id: Uuid,
) -> Result<Digest, ProtocolError> {
    serialized_digest(&("nabedra/scoped-queue/placement-v2", expected, run_id))
}

pub(super) fn ensure(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    operation_id: Uuid,
    expected: &GroupRecord,
    run_id: Uuid,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.authorization_id != operation_id
        || authorization.evidence != creation_digest(expected, run_id)?
        || !same_scope(scope, &expected.identity)
    {
        return fault(Fault::Unauthorized);
    }
    create(tx, state, operation_id, expected, run_id, false)
}

fn create(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    operation_id: Uuid,
    expected: &GroupRecord,
    run_id: Uuid,
    require_capability: bool,
) -> Result<ControlReply, ProtocolError> {
    if state.pending.is_some() || state.restart.is_some() {
        return fault(Fault::Busy);
    }
    let group = &expected.identity;
    if group.validate().is_err()
        || !matches!(group.kind, GroupKind::Data { .. })
        || expected.generation.is_nil()
        || expected.replica_count != 3
    {
        return fault(Fault::Invalid);
    }
    if state.ready.view.run_id != run_id
        || load::<GroupRecord>(tx, &key(GROUPS, group.group_id))?.as_ref() != Some(expected)
    {
        return fault(Fault::Stale);
    }
    if changing_data(tx, expected)? {
        return fault(Fault::Busy);
    }

    let Some(members) = exact_members(tx, state, expected, require_capability)? else {
        return fault(Fault::NotReady);
    };
    let key = placement_key(group.group_id);
    if let Some(previous) = load::<ScopedQueuePlacement>(tx, &key)? {
        previous.validate(group)?;
        if previous.view.run_id == run_id {
            if previous.data_generation != expected.generation
                || previous.members != members
                || tx.get(&transfer_key(group.group_id))?.is_some()
            {
                // A fresh Control membership cannot recreate accepted RAM.
                return fault(Fault::Stale);
            }
            return Ok(ControlReply::ScopedQueue(Box::new(previous)));
        }
        if !permits_new_run(
            previous.view.run_id,
            state.ready.view,
            load(tx, COLD_PUBLICATION)?,
            load::<bool>(tx, &super::key(RUNS, previous.view.run_id))? == Some(true),
        ) {
            return fault(Fault::Stale);
        }
    }

    // Placement uses the Data group's stable identity to spread initial queue
    // ownership across the current voters. Locality hints cannot change it.
    let rank = hash(group.group_id.as_bytes()).as_bytes()[0] as usize % members.len();
    let placement = ScopedQueuePlacement {
        group: group.clone(),
        data_generation: expected.generation,
        queue_generation: operation_id,
        view: AuthorityView {
            run_id,
            owner_term: 1,
            membership_revision: 1,
        },
        owner: members[rank],
        members,
        unavailable_holder: None,
    };
    placement.validate(group)?;
    change_holders(tx, None, &placement)?;
    save(tx, &key, &placement)?;
    tx.delete(&transfer_key(group.group_id))?;
    tx.delete(&completed_key(group.group_id))?;
    Ok(ControlReply::ScopedQueue(Box::new(placement)))
}

fn exact_members(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    expected: &GroupRecord,
    require_capability: bool,
) -> Result<Option<[WorkerIdentity; 3]>, ProtocolError> {
    let Some(replicas) = exact_replicas(tx, expected)? else {
        return Ok(None);
    };
    let mut members = Vec::with_capacity(3);
    for replica in replicas {
        let Some(worker) =
            load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, replica.worker_id))?
        else {
            return Ok(None);
        };
        if !eligible_destination(tx, state, &replica, worker, require_capability)? {
            return Ok(None);
        }
        members.push(worker);
    }
    members.sort_unstable_by_key(|worker| worker.worker_id);
    Ok(Some(members.try_into().map_err(|_| unknown())?))
}

fn eligible_destination(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    replica: &ReplicaMember,
    worker: WorkerIdentity,
    require_capability: bool,
) -> Result<bool, ProtocolError> {
    Ok(worker.worker_id == replica.worker_id
        && current_ready(tx, state, worker)?
        && !pool_drain::blocks_pool(tx, replica.pool_id)?
        && !worker_drain::blocks_worker(tx, replica.worker_id)?
        && (!require_capability || capability::supports(tx, worker)?))
}

fn exact_replicas(
    tx: &mut GroupTransaction<'_>,
    expected: &GroupRecord,
) -> Result<Option<Vec<ReplicaMember>>, ProtocolError> {
    if expected.replica_count != 3 || expected.generation.is_nil() {
        return Ok(None);
    }
    let mut replicas = Vec::with_capacity(3);
    let progress = tx.scan_page(
        &member_prefix(expected.generation),
        None,
        4,
        |key, value| {
            let member: ReplicaMember = decode(value)?;
            if key != member_key(expected.generation, member.node_id) || replicas.len() == 3 {
                return Err(unknown());
            }
            replicas.push(member);
            Ok(())
        },
    )?;
    if !progress.exhausted
        || replicas.len() != 3
        || validate_voters(&replicas).is_err()
        || group_root(3, replicas.iter().fold(group_seed(), group_step)) != expected.replica_root
    {
        return Err(unknown());
    }
    Ok(Some(replicas))
}
