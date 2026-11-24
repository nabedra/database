use super::*;
use crate::oltp_replication::authority::control::restart::{
    RestartIntent, RestartMountProof, RestartPreparation,
};

const MOUNTS: &[u8] = b"ct/restart-mount/";

#[derive(Serialize, serde::Deserialize)]
pub(in crate::oltp_replication::authority::control) struct MountRegistration {
    pub worker: WorkerIdentity,
    pub pool_id: Uuid,
    pub generation: u64,
    pub public_key: Digest,
}

pub(in crate::oltp_replication::authority::control) fn bootstrap_primary(
    scope: &GroupIdentity,
    removed: &ReplicaMember,
    mut get: impl FnMut(&[u8]) -> Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<Option<MountRegistration>, ProtocolError> {
    let Some(bytes) = get(&key(MOUNTS, removed.worker_id))? else {
        return Ok(None);
    };
    let mount: MountRegistration = decode(bytes.as_ref())?;
    if mount.worker.worker_id != removed.worker_id
        || mount.worker.validate().is_err()
        || mount.pool_id != removed.pool_id
        || mount.generation == 0
        || !nonzero(mount.public_key)
    {
        return Ok(None);
    }
    let Some(bytes) = get(&key(GROUPS, scope.group_id))? else {
        return Ok(None);
    };
    let control: GroupRecord = decode(bytes.as_ref())?;
    if scope.kind != GroupKind::Control || control.identity != *scope {
        return Ok(None);
    }
    let Some(bytes) = get(&member_worker_key(control.generation, removed.worker_id))? else {
        return Ok(None);
    };
    let member: ReplicaMember = decode(bytes.as_ref())?;
    Ok((member.worker_id == removed.worker_id && member.pool_id == mount.pool_id).then_some(mount))
}

pub(super) fn register_mount(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    worker: WorkerIdentity,
    pool_id: Uuid,
    generation: u64,
    public_key: Digest,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.subject != worker.certificate_sha256
        || pool_id.is_nil()
        || generation == 0
        || !nonzero(public_key)
    {
        return fault(Fault::Unauthorized);
    }
    let Some(record) = load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))? else {
        return fault(Fault::NotFound);
    };
    if WorkerIdentity::from(&record.enrollment) != worker {
        return fault(Fault::Stale);
    }
    let mount = key(MOUNTS, worker.worker_id);
    if let Some(old) = load::<MountRegistration>(tx, &mount)? {
        if old.pool_id != pool_id || old.public_key != public_key {
            return fault(Fault::Conflict);
        }
        if old.worker == worker && old.generation == generation {
            return Ok(ControlReply::RestartMountRegistered(worker));
        }
        if generation <= old.generation || old.worker == worker {
            return fault(Fault::Stale);
        }
    }
    save(
        tx,
        &mount,
        &MountRegistration {
            worker,
            pool_id,
            generation,
            public_key,
        },
    )?;
    Ok(ControlReply::RestartMountRegistered(worker))
}

pub(super) fn prepare(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    operation_id: Uuid,
    intent: &RestartIntent,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || operation_id != intent.id
        || intent.validate().is_err()
        || system_recovery::blocked_enrollment(tx, &intent.next)?
    {
        return fault(Fault::Unauthorized);
    }
    if state.pending.is_some()
        || state.restart.as_ref().is_some_and(|prep| {
            prep.enrolled
                || prep.intent.previous != intent.previous
                || prep.intent.next.enrollment_revision >= intent.next.enrollment_revision
                || prep.intent.mount_generation >= intent.mount_generation
        })
    {
        return fault(Fault::Busy);
    }
    if state.ready.view.run_id != intent.run_id
        || state.ready.ready_count == 0
        || state.invalid_ready != 0
        || state
            .owner
            .is_none_or(|owner| owner.worker_id == intent.previous.worker_id)
        || load::<WorkerIdentity>(
            tx,
            &ready_key(state.ready_generation, intent.previous.worker_id),
        )?
        .is_some()
    {
        return fault(Fault::NotReady);
    }
    let Some(old) = load::<WorkerRecord>(tx, &key(WORKERS, intent.previous.worker_id))? else {
        return fault(Fault::NotFound);
    };
    let Some(mount) = load::<MountRegistration>(tx, &key(MOUNTS, intent.previous.worker_id))?
    else {
        return fault(Fault::NotFound);
    };
    let Some(group) = load::<GroupRecord>(tx, &key(GROUPS, scope.group_id))? else {
        return fault(Fault::NotFound);
    };
    let Some(replica) = load::<ReplicaMember>(
        tx,
        &member_worker_key(group.generation, intent.previous.worker_id),
    )?
    else {
        return fault(Fault::NotFound);
    };
    if WorkerIdentity::from(&old.enrollment) != intent.previous
        || mount.worker != intent.previous
        || mount.pool_id != intent.pool_id
        || replica.pool_id != intent.pool_id
        || mount.generation >= intent.mount_generation
    {
        return fault(Fault::Stale);
    }
    state.restart = Some(RestartPreparation {
        intent: intent.clone(),
        enrolled: false,
    });
    state.enrollment_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(ControlReply::RestartPrepared(WorkerIdentity::from(
        &intent.next,
    )))
}

pub(super) fn complete(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    operation_id: Uuid,
    intent: &RestartIntent,
    proof: &RestartMountProof,
    previous: &ExclusionStamp,
) -> Result<ControlReply, ProtocolError> {
    let Some(prepared) = state.restart.as_ref() else {
        return fault(Fault::NotFound);
    };
    if system_recovery::blocked_enrollment(tx, &intent.next)? {
        return fault(Fault::Unauthorized);
    }
    let worker = WorkerIdentity::from(&intent.next);
    if &prepared.intent != intent
        || prepared.enrolled
        || previous.worker != intent.previous
        || previous.authorization.authorization_id != operation_id
        || previous.authorization.subject != worker.certificate_sha256
        || !exclusion(previous, state.policy)
    {
        return fault(Fault::Unauthorized);
    }
    if state.pending.is_some()
        || state.ready.view.run_id != intent.run_id
        || state.ready.ready_count == 0
        || state.invalid_ready != 0
        || state
            .owner
            .is_none_or(|owner| owner.worker_id == worker.worker_id)
        || load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, worker.worker_id))?
            .is_some()
    {
        return fault(Fault::NotReady);
    }
    let worker_key = key(WORKERS, worker.worker_id);
    let mount_key = key(MOUNTS, worker.worker_id);
    let Some(old) = load::<WorkerRecord>(tx, &worker_key)? else {
        return fault(Fault::NotFound);
    };
    let Some(mut mount) = load::<MountRegistration>(tx, &mount_key)? else {
        return fault(Fault::NotFound);
    };
    if WorkerIdentity::from(&old.enrollment) != intent.previous
        || mount.worker != intent.previous
        || mount.pool_id != intent.pool_id
        || mount.generation >= intent.mount_generation
        || proof.verify(scope, intent, mount.public_key).is_err()
    {
        return fault(Fault::Unauthorized);
    }
    mount.worker = worker;
    mount.generation = intent.mount_generation;
    record_process_exclusion(tx, previous)?;
    save(tx, &mount_key, &mount)?;
    save(
        tx,
        &worker_key,
        &WorkerRecord {
            enrollment: intent.next.clone(),
            previous_process: Some(previous.clone()),
        },
    )?;
    state.restart = Some(RestartPreparation {
        intent: intent.clone(),
        enrolled: true,
    });
    state.enrollment_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(ControlReply::Enrolled(worker))
}
