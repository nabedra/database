use super::*;
use crate::{
    oltp_deployment::controller::{GrowthCommand, GrowthRecord, MountedGrowthStamp},
    oltp_replication::authority::control::{
        DataWorkerIntent, data_worker::DataWorkerRegistration, restart::RestartMountProof,
    },
};

pub(in crate::oltp_replication::authority::control) const PREPARED: &[u8] =
    b"ct/prepared-data-worker";
pub(in crate::oltp_replication::authority::control) const REGISTERED: &[u8] = b"ct/data-worker/";

pub(super) fn prepare(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    intent: &DataWorkerIntent,
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.subject == intent.worker().certificate_sha256
        || intent.validate(scope).is_err()
        || system_recovery::blocked_enrollment(tx, &intent.enrollment)?
    {
        return fault(Fault::Unauthorized);
    }
    if let Some(prepared) = load::<DataWorkerIntent>(tx, PREPARED)? {
        return if &prepared == intent {
            Ok(ControlReply::DataWorkerPrepared(intent.worker()))
        } else {
            fault(Fault::Busy)
        };
    }
    if !ready_for_join(state, intent.run_id) {
        return fault(Fault::NotReady);
    }
    let worker = intent.worker().worker_id;
    if tx.get(&key(WORKERS, worker))?.is_some() || tx.get(&key(APIS, worker))?.is_some() {
        return fault(Fault::Conflict);
    }
    let Some(claim) = load::<GrowthRecord>(tx, &super::super::growth::key(intent.claim_id))? else {
        return fault(Fault::NotFound);
    };
    if !intent.matches_claim(&claim) {
        return fault(Fault::Unauthorized);
    }
    save(tx, PREPARED, intent)?;
    advance_enrollment(state)?;
    Ok(ControlReply::DataWorkerPrepared(intent.worker()))
}

pub(super) fn complete(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    intent: &DataWorkerIntent,
    mount: &MountedGrowthStamp,
    proof: &RestartMountProof,
) -> Result<ControlReply, ProtocolError> {
    let worker = intent.worker();
    let worker_key = key(WORKERS, worker.worker_id);
    let registration_key = key(REGISTERED, worker.worker_id);
    if !ready_for_join(state, intent.run_id) {
        return fault(Fault::NotReady);
    }
    if system_recovery::blocked_enrollment(tx, &intent.enrollment)?
        || load::<DataWorkerIntent>(tx, PREPARED)?.as_ref() != Some(intent)
        || tx.get(&worker_key)?.is_some()
        || tx.get(&key(APIS, worker.worker_id))?.is_some()
    {
        return fault(Fault::Unauthorized);
    }
    let Some(claim) = load::<GrowthRecord>(tx, &super::super::growth::key(intent.claim_id))? else {
        return fault(Fault::NotFound);
    };
    if intent.verify(scope, mount, proof, &claim).is_err() {
        return fault(Fault::Unauthorized);
    }
    save(
        tx,
        &worker_key,
        &WorkerRecord {
            enrollment: intent.enrollment.clone(),
            previous_process: None,
        },
    )?;
    save(
        tx,
        &registration_key,
        &DataWorkerRegistration {
            intent: intent.clone(),
            mount: mount.clone(),
            replacement: None,
        },
    )?;
    tx.delete(PREPARED)?;
    advance_enrollment(state)?;
    Ok(ControlReply::Enrolled(worker))
}

pub(super) fn begin_join(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    operation_id: Uuid,
    worker: WorkerIdentity,
    claim_id: Uuid,
    expected: ReadyMembership,
) -> Result<ControlReply, ProtocolError> {
    let Some(registration) =
        load::<DataWorkerRegistration>(tx, &key(REGISTERED, worker.worker_id))?
    else {
        return fault(Fault::Unauthorized);
    };
    let Some(claim) = load::<GrowthRecord>(tx, &super::super::growth::key(claim_id))? else {
        return fault(Fault::NotFound);
    };
    if registration.intent.worker() != worker
        || if registration.replacement.is_some() {
            claim.intent.claim_id != registration.intent.claim_id
                || !super::replacement::can_join(tx, worker)?
        } else {
            !registration.intent.matches_claim(&claim)
        }
    {
        return fault(Fault::Unauthorized);
    }
    if !ready_for_join(state, registration.intent.run_id) || state.ready != expected {
        return fault(Fault::Stale);
    }
    begin_ready(
        tx,
        state,
        operation_id,
        expected.view,
        worker,
        ReadyChange::Join,
        None,
        false,
    )
}

pub(super) fn publish_mount(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    pending: &ReadyPending,
) -> Result<Option<Fault>, ProtocolError> {
    if pending.change != ReadyChange::Join {
        return Ok(None);
    }
    let Some(registration) =
        load::<DataWorkerRegistration>(tx, &key(REGISTERED, pending.target.worker_id))?
    else {
        return Ok(None);
    };
    if registration.intent.worker() != pending.target
        || registration.intent.run_id != state.ready.view.run_id
    {
        return Ok(Some(Fault::Unauthorized));
    }
    if let Some(added) = &registration.replacement {
        let claim = load::<GrowthRecord>(tx, &super::super::growth::key(registration.intent.claim_id))?
            .ok_or_else(unknown)?;
        return super::replacement::publish(tx, state, pending, &claim, added);
    }
    match super::super::growth::apply(
        tx,
        &GrowthCommand::Mounted {
            claim_id: registration.intent.claim_id,
            proof: registration.mount,
        },
        state,
    )? {
        ControlReply::Growth(_) => Ok(None),
        ControlReply::Rejected(fault) => Ok(Some(fault)),
        _ => Err(unknown()),
    }
}

pub(super) fn ready_for_join(state: &State, run: Uuid) -> bool {
    state.ready.view.run_id == run
        && state.ready.ready_count > 0
        && state.owner.is_some()
        && state.invalid_ready == 0
        && state.pending.is_none()
        && state.restart.is_none()
}

fn advance_enrollment(state: &mut State) -> Result<(), ProtocolError> {
    state.enrollment_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(())
}
