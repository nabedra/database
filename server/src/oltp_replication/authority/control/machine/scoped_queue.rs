use super::*;
use crate::oltp_replication::{authority::control::ScopedQueuePlacement, owned::serialized_digest};

pub(in crate::oltp_replication::authority::control) fn placement_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/scoped-queue/", group_id)
}

pub(in crate::oltp_replication::authority::control) fn replacement_key(group_id: Uuid) -> Vec<u8> {
    key(b"ct/group-replacement/", group_id)
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
    if tx.get(&replacement_key(group.group_id))?.is_some() {
        return fault(Fault::Busy);
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

    let mut members = Vec::with_capacity(3);
    for replica in replicas {
        let Some(worker) =
            load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, replica.worker_id))?
        else {
            return fault(Fault::NotReady);
        };
        if !current_ready(tx, state, worker)?
            || pool_drain::blocks_pool(tx, replica.pool_id)?
            || worker_drain::blocks_worker(tx, replica.worker_id)?
        {
            return fault(Fault::NotReady);
        }
        members.push(worker);
    }
    members.sort_unstable_by_key(|worker| worker.worker_id);
    let members: [WorkerIdentity; 3] = members.try_into().map_err(|_| unknown())?;
    let key = placement_key(group.group_id);
    let previous = load::<ScopedQueuePlacement>(tx, &key)?;
    let owner_term = if let Some(previous) = previous {
        previous.validate(group)?;
        if previous.view.run_id == run_id {
            if previous.data_generation != expected.generation || previous.members != members {
                // A fresh Control membership cannot recreate accepted RAM.
                return fault(Fault::Stale);
            }
            return Ok(ControlReply::ScopedQueue(Box::new(previous)));
        }
        let published = load::<(Uuid, Authority, Authority)>(tx, COLD_PUBLICATION)?;
        if !published.is_some_and(|(id, prior, next)| {
            !id.is_nil()
                && prior.run_id != next.run_id
                && prior.owner_term.checked_add(1) == Some(next.owner_term)
                && next.run_id == run_id
                && next.owner_term <= state.ready.view.owner_term
        }) || load::<bool>(tx, &super::key(RUNS, previous.view.run_id))? != Some(true)
        {
            return fault(Fault::Stale);
        }
        previous
            .view
            .owner_term
            .checked_add(1)
            .ok_or_else(unknown)?
    } else {
        1
    };

    // Placement uses the Data group's stable identity to spread initial queue
    // ownership across the current voters. Locality hints cannot change it.
    let rank = hash(group.group_id.as_bytes()).as_bytes()[0] as usize % members.len();
    let placement = ScopedQueuePlacement {
        group: group.clone(),
        data_generation: expected.generation,
        queue_generation: operation_id,
        view: AuthorityView {
            run_id,
            owner_term,
            membership_revision: 1,
        },
        owner: members[rank],
        members,
    };
    placement.validate(group)?;
    save(tx, &key, &placement)?;
    Ok(ControlReply::ScopedQueue(Box::new(placement)))
}
