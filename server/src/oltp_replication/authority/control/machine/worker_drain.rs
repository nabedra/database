use super::super::{
    data_worker::DataWorkerRegistration,
    growth,
    worker_drain::{
        EksReplacementRecord, RetainedEksPool, WorkerCloseAttestation, WorkerDrainIntent,
        WorkerDrainPhase, WorkerDrainRecord,
    },
};
use super::*;
use crate::{
    oltp_deployment::{controller::{GrowthPolicy, GrowthRecord}, planner::Action},
    oltp_replication::owned::serialized_digest,
};

pub(in crate::oltp_replication::authority::control) const ACTIVE: &[u8] = b"ct/worker-drain-active";

pub(in crate::oltp_replication::authority::control) fn record_key(id: Uuid) -> Vec<u8> {
    key(b"ct/worker-drain/", id)
}
pub(in crate::oltp_replication::authority::control) fn barrier_key(worker: Uuid) -> Vec<u8> {
    key(b"ct/worker-drain-barrier/", worker)
}
pub(in crate::oltp_replication::authority::control) fn blocks_worker(
    tx: &mut GroupTransaction<'_>,
    worker: Uuid,
) -> Result<bool, ProtocolError> {
    Ok(tx.get(&barrier_key(worker))?.is_some())
}

fn reply(record: WorkerDrainRecord) -> Result<ControlReply, ProtocolError> {
    Ok(ControlReply::WorkerDrain(Box::new(record)))
}

pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    intent: &WorkerDrainIntent,
) -> Result<ControlReply, ProtocolError> {
    if !operator(state, id, worker, authorization, intent)? || intent.validate().is_err() {
        return fault(Fault::Unauthorized);
    }
    if let Some(old) = load::<WorkerDrainRecord>(tx, &record_key(id))? {
        return if old.intent == *intent {
            reply(old)
        } else {
            fault(Fault::Conflict)
        };
    }
    if state.pending.is_some()
        || state.restart.is_some()
        || tx.get(ACTIVE)?.is_some()
        || tx.get(data_worker::PREPARED)?.is_some()
        || blocks_worker(tx, intent.worker.worker_id)?
    {
        return fault(Fault::Busy);
    }
    if state.invalid_ready != 0
        || state.ready.ready_count < 4
        || state.owner == Some(intent.worker)
        || state.ready.view.run_id != intent.run_id
        || !current_ready(tx, state, worker)?
        || !current_ready(tx, state, intent.worker)?
    {
        return fault(Fault::NotReady);
    }
    let Some(registration) = registration(tx, intent.worker)? else {
        return fault(Fault::Unauthorized);
    };
    let Some(claim) = load::<GrowthRecord>(tx, &growth::key(registration.intent.claim_id))? else {
        return fault(Fault::NotFound);
    };
    if !registration.completed(&claim) || !intent.matches_registration(&registration.intent)
    {
        return fault(Fault::Unauthorized);
    }
    let retained_eks_pool = match (registration.replacement.as_ref(), intent.retained_replacement_drain) {
        (None, None) => None,
        (Some(_), Some(previous_id)) if previous_id != id => {
            let Some(published) = load::<EksReplacementRecord>(tx, &replacement::record_key(previous_id))? else {
                return fault(Fault::Unauthorized);
            };
            let Some(previous) = load::<WorkerDrainRecord>(tx, &record_key(previous_id))? else {
                return fault(Fault::Unauthorized);
            };
            let Some(growth) = load::<GrowthRecord>(tx, &growth::key(published.add_pool_claim))? else {
                return fault(Fault::Unauthorized);
            };
            let Action::AddWorker { zone, .. } = &claim.intent.action else {
                return fault(Fault::Unauthorized);
            };
            let Some(retained) = RetainedEksPool::from_published(
                &previous, &published, &registration, &growth, zone,
            ) else {
                return fault(Fault::Unauthorized);
            };
            if let Some(active) = load::<Uuid>(tx, growth::ACTIVE)? {
                let Some(pending) = load::<GrowthRecord>(tx, &growth::key(active))? else {
                    return fault(Fault::Conflict);
                };
                if pending.intent.worker.worker_id == intent.worker.worker_id {
                    return fault(Fault::Busy);
                }
            }
            Some(Box::new(retained))
        }
        _ => return fault(Fault::Unauthorized),
    };
    if let Some(id) = intent.replacement_growth_claim {
        let replacement = load::<GrowthRecord>(tx, &growth::key(id))?;
        if registration.intent.eks_replacement_key.is_none()
            || load::<Uuid>(tx, growth::ACTIVE)? != Some(id)
            || !replacement.as_ref().is_some_and(|pending| {
                super::super::worker_drain::eligible_add_pool(intent, &claim, pending)
            })
        {
            return fault(Fault::Unauthorized);
        }
    }
    let record = WorkerDrainRecord {
        id,
        intent: intent.clone(),
        growth_claim: claim.intent.claim_id,
        retained_eks_pool,
        phase: WorkerDrainPhase::Draining,
    };
    save(tx, ACTIVE, &id)?;
    save(tx, &barrier_key(intent.worker.worker_id), &id)?;
    save(tx, &record_key(id), &record)?;
    reply(record)
}

// The owner obtains this pin by reading every bounded placement page against
// the same strong Control snapshot revision. The CAS must recheck each pin.
pub(super) fn begin_removal(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    transition: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    id: Uuid,
    expected: ReadyMembership,
    placement_revision: u64,
    policy: Uuid,
    growth_revision: u64,
) -> Result<ControlReply, ProtocolError> {
    if !operator(
        state,
        transition,
        worker,
        authorization,
        &(id, expected, placement_revision, policy, growth_revision),
    )? || state.owner != Some(worker)
        || !current_ready(tx, state, worker)?
    {
        return fault(Fault::Unauthorized);
    }
    let Some(mut record) = load::<WorkerDrainRecord>(tx, &record_key(id))? else {
        return fault(Fault::NotFound);
    };
    if record.phase != WorkerDrainPhase::Draining
        || state.ready != expected
        || state.ready.view.run_id != record.intent.run_id
        || state.invalid_ready != 0
        || state.ready.ready_count < 4
        || state.restart.is_some()
        || !current_ready(tx, state, record.intent.worker)?
        || load::<Uuid>(tx, ACTIVE)? != Some(id)
        || load::<Uuid>(tx, &barrier_key(record.intent.worker.worker_id))? != Some(id)
        || tx.get(placement::ACTIVE)?.is_some()
        || load::<u64>(tx, pool_drain::REVISION)?.unwrap_or_default() != placement_revision
        || load::<u64>(tx, growth::REVISION)?.unwrap_or_default() != growth_revision
        || load::<GrowthPolicy>(tx, growth::POLICY)?.is_none_or(|value| value.policy_id != policy)
    {
        return fault(Fault::Stale);
    }
    let begun = begin_ready(
        tx,
        state,
        transition,
        expected.view,
        record.intent.worker,
        ReadyChange::Remove,
        None,
        true,
    )?;
    if matches!(begun, ControlReply::JoinBegun { .. }) {
        record.phase = WorkerDrainPhase::Removing { transition };
        save(tx, &record_key(id), &record)?;
    }
    Ok(begun)
}

pub(super) fn publish_queue_removed(
    tx: &mut GroupTransaction<'_>,
    pending: &ReadyPending,
    ready: ReadyMembership,
) -> Result<(), ProtocolError> {
    let Some(id) = load::<Uuid>(tx, ACTIVE)? else {
        return Ok(());
    };
    let mut record = load::<WorkerDrainRecord>(tx, &record_key(id))?.ok_or_else(unknown)?;
    if pending.change == ReadyChange::Remove
        && pending.target == record.intent.worker
        && record.phase
            == (WorkerDrainPhase::Removing {
                transition: pending.transition_id,
            })
    {
        record.phase = WorkerDrainPhase::QueueRemoved { ready };
        save(tx, &record_key(id), &record)?;
    }
    Ok(())
}

pub(super) fn close(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    proof: &WorkerCloseAttestation,
) -> Result<ControlReply, ProtocolError> {
    let Some(mut record) = load::<WorkerDrainRecord>(tx, &record_key(proof.id))? else {
        return fault(Fault::NotFound);
    };
    let Some(registration) = registration(tx, proof.worker)? else {
        return fault(Fault::Unauthorized);
    };
    if record.intent.worker != proof.worker
        || !record.intent.matches_registration(&registration.intent)
        || record.growth_claim != registration.intent.claim_id
        || record.retained_eks_pool.is_some() != registration.replacement.is_some()
        || !record.matches_closure(&registration.intent.mount, &proof.mount, proof.native_mounts.as_deref())
        || proof
            .verify(scope, &record, registration.intent.mount_public_key)
            .is_err()
    {
        return fault(Fault::Unauthorized);
    }
    if record.phase
        == (WorkerDrainPhase::Closed {
            ready: proof.ready,
            mount: proof.mount.clone(),
            native_mounts: proof.native_mounts.clone(),
        })
    {
        return reply(record);
    }
    if record.phase != (WorkerDrainPhase::QueueRemoved { ready: proof.ready })
        || state.ready != proof.ready
        || state.ready.ready_count < 3
        || state.invalid_ready != 0
        || state.pending.is_some()
        || state.restart.is_some()
        || load::<WorkerIdentity>(
            tx,
            &ready_key(state.ready_generation, proof.worker.worker_id),
        )?
        .is_some()
        || load::<Uuid>(tx, ACTIVE)? != Some(proof.id)
        || load::<Uuid>(tx, &barrier_key(proof.worker.worker_id))? != Some(proof.id)
    {
        return fault(Fault::Stale);
    }
    record.phase = WorkerDrainPhase::Closed {
        ready: proof.ready,
        mount: proof.mount.clone(),
        native_mounts: proof.native_mounts.clone(),
    };
    save(tx, &record_key(proof.id), &record)?;
    tx.delete(ACTIVE)?;
    reply(record)
}

fn registration(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<Option<DataWorkerRegistration>, ProtocolError> {
    if load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))?
        .is_none_or(|entry| WorkerIdentity::from(&entry.enrollment) != worker)
    {
        return Ok(None);
    }
    Ok(
        load::<DataWorkerRegistration>(tx, &key(data_worker::REGISTERED, worker.worker_id))?
            .filter(|entry| entry.intent.worker() == worker),
    )
}

fn operator<T: Serialize>(
    state: &State,
    id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    command: &T,
) -> Result<bool, ProtocolError> {
    Ok(stamp(authorization, state.policy)
        && authorization.authorization_id == id
        && authorization.subject == worker.certificate_sha256
        && authorization.evidence == serialized_digest(&(worker, command))?)
}
