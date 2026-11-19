use super::*;

pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    expected: &ScopedQueuePlacement,
    owner: WorkerIdentity,
    replaces: Option<Uuid>,
) -> Result<ControlReply, ProtocolError> {
    let group = &expected.group;
    let group_id = group.group_id;
    let key = transfer_key(group_id);
    let pending = load::<ScopedQueueTransition>(tx, &key)?
        .filter(|transfer| transfer.previous.view.run_id == state.ready.view.run_id);
    if load::<ScopedQueuePlacement>(tx, &placement_key(group_id))?.as_ref() != Some(expected)
        || pending.as_ref().map(|transfer| transfer.id) != replaces
        || pending
            .as_ref()
            .is_some_and(|transfer| &transfer.previous != expected)
        || expected.view.run_id != state.ready.view.run_id
    {
        return fault(Fault::Stale);
    }
    let Some(record) = load::<GroupRecord>(tx, &super::key(GROUPS, group_id))?
        .filter(|record| record.identity == *group)
    else {
        return fault(Fault::Stale);
    };
    if state.pending.is_some() || state.restart.is_some() || changing_data(tx, &record)? {
        return fault(Fault::Busy);
    }
    let Some((members, unavailable_holder)) = destinations(tx, state, &record, expected)? else {
        return fault(Fault::NotReady);
    };
    if !members.contains(&owner) || unavailable_holder == Some(owner) {
        return fault(Fault::Unauthorized);
    }
    let membership_changed = expected.data_generation != record.generation
        || expected.members != members
        || expected.unavailable_holder != unavailable_holder;
    let term = pending.map_or(expected.view.owner_term, |transfer| {
        transfer.next.view.owner_term
    });
    let next = ScopedQueuePlacement {
        group: group.clone(),
        data_generation: record.generation,
        queue_generation: expected.queue_generation,
        view: AuthorityView {
            run_id: expected.view.run_id,
            owner_term: term.checked_add(1).ok_or_else(unknown)?,
            membership_revision: expected
                .view
                .membership_revision
                .checked_add(u64::from(membership_changed))
                .ok_or_else(unknown)?,
        },
        owner,
        members,
        unavailable_holder,
    };
    next.validate(group)?;
    let transition = ScopedQueueTransition {
        id,
        previous: expected.clone(),
        next,
        frozen: [None; 3],
        installed: [None; 3],
    };
    save(tx, &key, &transition)?;
    Ok(ControlReply::ScopedQueueTransfer(Box::new(transition)))
}

pub(super) fn rebase(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    group_id: Uuid,
    replaces: Uuid,
    removed: WorkerIdentity,
    owner: WorkerIdentity,
) -> Result<ControlReply, ProtocolError> {
    let Some(transfer) = active(tx, state, group_id, replaces)? else {
        return fault(Fault::Stale);
    };
    let next = &transfer.next;
    let Some(record) = load::<GroupRecord>(tx, &super::key(GROUPS, group_id))?.filter(|record| {
        record.identity == next.group
            && record.generation == next.data_generation
            && record.generation == transfer.previous.data_generation
    }) else {
        return fault(Fault::Stale);
    };
    if next.unavailable_holder.is_some()
        || destinations(tx, state, &record, &transfer.previous)?
            != Some((next.members, Some(removed)))
    {
        return fault(Fault::NotReady);
    }

    // Ready removal changed the exact destinations. Restart the transfer from
    // the same published source under the next term, with fresh copy proofs.
    begin(tx, state, id, &transfer.previous, owner, Some(replaces))
}

fn destinations(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    record: &GroupRecord,
    previous: &ScopedQueuePlacement,
) -> Result<Option<([WorkerIdentity; 3], Option<WorkerIdentity>)>, ProtocolError> {
    if let Some(members) = exact_members(tx, state, record, true)? {
        return Ok(Some((members, None)));
    }
    if record.generation != previous.data_generation {
        return Ok(None);
    }
    let Some(replicas) = exact_replicas(tx, record)? else {
        return Ok(None);
    };
    // A legacy logical removal has already stopped admitting this process
    // through the ready set. Keep the same three Data voters and require both
    // other exact old members to be eligible destinations.
    let mut removed = None;
    for replica in replicas {
        let Some(worker) = previous
            .members
            .into_iter()
            .find(|worker| worker.worker_id == replica.worker_id)
        else {
            return Ok(None);
        };
        match load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, replica.worker_id))? {
            None if removed.is_none() => removed = Some(worker),
            Some(ready)
                if ready == worker
                    && previous.has_holder(worker)
                    && eligible_destination(tx, state, &replica, worker, true)? => {}
            _ => return Ok(None),
        }
    }
    Ok(removed.map(|worker| (previous.members, Some(worker))))
}

fn active(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    group_id: Uuid,
    id: Uuid,
) -> Result<Option<ScopedQueueTransition>, ProtocolError> {
    let Some(transition) = load::<ScopedQueueTransition>(tx, &transfer_key(group_id))? else {
        return Ok(None);
    };
    if id != transition.id
        || state.pending.is_some()
        || state.restart.is_some()
        || transition.previous.view.run_id != state.ready.view.run_id
        || load::<ScopedQueuePlacement>(tx, &placement_key(group_id))?.as_ref()
            != Some(&transition.previous)
    {
        return Ok(None);
    }
    Ok(Some(transition))
}

pub(super) fn report(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    actor: WorkerIdentity,
    group_id: Uuid,
    id: Uuid,
    report: ScopedQueueTransferReport,
) -> Result<ControlReply, ProtocolError> {
    let Some(mut transition) = active(tx, state, group_id, id)? else {
        return fault(Fault::Stale);
    };
    if !current_ready(tx, state, actor)? || !capability::supports(tx, actor)? {
        return fault(Fault::Unauthorized);
    }
    let (slot, inventory) = match report {
        ScopedQueueTransferReport::Frozen(inventory) => {
            if !transition.previous.has_holder(actor) {
                return fault(Fault::Unauthorized);
            }
            let Some(index) = transition
                .previous
                .members
                .iter()
                .position(|worker| *worker == actor)
            else {
                return fault(Fault::Unauthorized);
            };
            if transition.frozen[index].is_none() && transition.basis()?.is_some() {
                return fault(Fault::Conflict);
            }
            (&mut transition.frozen[index], inventory)
        }
        ScopedQueueTransferReport::Installed { basis, inventory } => {
            if !transition.next.has_holder(actor) {
                return fault(Fault::Unauthorized);
            }
            let Some(index) = transition
                .next
                .members
                .iter()
                .position(|worker| *worker == actor)
            else {
                return fault(Fault::Unauthorized);
            };
            if transition.basis()? != Some(basis) {
                return fault(Fault::Incomplete);
            }
            (&mut transition.installed[index], inventory)
        }
    };
    if !nonzero(inventory.root) || slot.is_some_and(|old| old != inventory) {
        return fault(Fault::Conflict);
    }
    *slot = Some(inventory);
    save(tx, &transfer_key(group_id), &transition)?;
    Ok(ControlReply::ScopedQueueUpdated)
}

pub(super) fn publish(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    actor: WorkerIdentity,
    group_id: Uuid,
    id: Uuid,
) -> Result<ControlReply, ProtocolError> {
    let Some(transition) = active(tx, state, group_id, id)? else {
        return fault(Fault::Stale);
    };
    let next = &transition.next;
    if next.owner != actor {
        return fault(Fault::Unauthorized);
    }
    let Some(record) = load::<GroupRecord>(tx, &key(GROUPS, group_id))?.filter(|record| {
        record.identity == next.group && record.generation == next.data_generation
    }) else {
        return fault(Fault::Stale);
    };
    if changing_data(tx, &record)?
        || destinations(tx, state, &record, &transition.previous)?
            != Some((next.members, next.unavailable_holder))
    {
        return fault(Fault::NotReady);
    }
    if transition.basis()?.is_none() || !transition.has_installed_quorum() {
        return fault(Fault::Incomplete);
    }
    change_holders(tx, Some(&transition.previous), next)?;
    save(tx, &placement_key(group_id), next)?;
    save(tx, &completed_key(group_id), &(id, next.view))?;
    tx.delete(&transfer_key(group_id))?;
    Ok(ControlReply::ScopedQueue(Box::new(transition.next)))
}
