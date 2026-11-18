use super::super::{
    growth,
    worker_drain::{
        EksAuthorization, EksMountedReplacement, EksReplacementAction, EksReplacementRecord,
        SignedEksReplacement, WorkerDrainPhase, WorkerDrainRecord, eks_statement,
    },
};
use super::*;
use crate::oltp_deployment::{controller::GrowthRecord, planner::Action};

pub(in crate::oltp_replication::authority::control) fn record_key(drain_id: Uuid) -> Vec<u8> {
    key(b"ct/eks-worker-replacement/", drain_id)
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    operation_id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    request: &SignedEksReplacement,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.authorization_id != operation_id
        || authorization.subject != worker.certificate_sha256
        || authorization.evidence
            != crate::oltp_replication::owned::serialized_digest(&(worker, request))?
        || !current_ready(tx, state, worker)?
    {
        return fault(Fault::Unauthorized);
    }
    let id = request.action.drain_id();
    let Some(drain) = load::<WorkerDrainRecord>(tx, &worker_drain::record_key(id))? else {
        return fault(Fault::NotFound);
    };
    let WorkerDrainPhase::Closed { ready, .. } = drain.phase else {
        return fault(Fault::NotReady);
    };
    let target = drain.intent.worker;
    let barrier = worker_drain::barrier_key(target.worker_id);
    let worker_key = key(WORKERS, target.worker_id);
    let registration_key = key(data_worker::REGISTERED, target.worker_id);
    let record_key = record_key(id);
    let previous = load::<EksReplacementRecord>(tx, &record_key)?;
    if let Some(previous) = &previous {
        let exact = match &request.action {
            EksReplacementAction::Exclude(evidence) => {
                previous.exclusion_operation == operation_id
                    && &previous.exclusion == evidence
                    && previous.exclusion_signature == request.signature
            }
            EksReplacementAction::Authorize(candidate) => {
                previous.authorization.as_ref().is_some_and(|old| {
                    old.operation == operation_id
                        && &old.candidate == candidate
                        && old.signature == request.signature
                })
            }
        };
        if exact {
            return Ok(ControlReply::EksReplacement(Box::new(previous.clone())));
        }
    }
    if state.ready != ready
        || state.ready.ready_count < 3
        || state.invalid_ready != 0
        || state.pending.is_some()
        || state.restart.is_some()
        || tx.get(placement::ACTIVE)?.is_some()
        || tx.get(worker_drain::ACTIVE)?.is_some()
        || tx.get(data_worker::PREPARED)?.is_some()
        || load::<Uuid>(tx, &barrier)? != Some(id)
        || tx
            .get(&ready_key(state.ready_generation, target.worker_id))?
            .is_some()
        || tx.get(&key(APIS, target.worker_id))?.is_some()
    {
        return fault(Fault::Stale);
    }
    let Some(registration) =
        load::<super::super::data_worker::DataWorkerRegistration>(tx, &registration_key)?
    else {
        return fault(Fault::Unauthorized);
    };
    let Some(original) = load::<GrowthRecord>(tx, &growth::key(drain.growth_claim))? else {
        return fault(Fault::NotFound);
    };
    let Some(add_pool_claim) = drain.intent.replacement_growth_claim else {
        return fault(Fault::Invalid);
    };
    let Some(growth) = load::<GrowthRecord>(tx, &growth::key(add_pool_claim))? else {
        return fault(Fault::NotFound);
    };
    if !registration.completed(&original)
        || load::<Uuid>(tx, growth::ACTIVE)? != Some(add_pool_claim)
    {
        return fault(Fault::Conflict);
    }
    let Action::AddWorker { zone, .. } = &original.intent.action else {
        return fault(Fault::Invalid);
    };
    let Some(verifier) = registration.intent.eks_replacement_key else {
        return fault(Fault::Unauthorized);
    };
    let Ok(message) = eks_statement(
        scope,
        operation_id,
        &drain,
        &registration.intent,
        &original,
        &growth,
        &request.action,
        previous.as_ref(),
    ) else {
        return fault(Fault::Unauthorized);
    };
    if request.signature.verify_message(message, verifier).is_err() {
        return fault(Fault::Unauthorized);
    }
    let record = match (&request.action, previous) {
        (EksReplacementAction::Exclude(evidence), None) => {
            if load::<WorkerRecord>(tx, &worker_key)?
                .is_none_or(|entry| WorkerIdentity::from(&entry.enrollment) != target)
            {
                return fault(Fault::Stale);
            }
            tx.delete(&worker_key)?;
            EksReplacementRecord {
                drain_id: id,
                add_pool_claim,
                exclusion_operation: operation_id,
                exclusion: evidence.clone(),
                exclusion_signature: request.signature.clone(),
                authorization: None,
                published: false,
                zone: zone.clone(),
            }
        }
        (EksReplacementAction::Authorize(candidate), Some(mut old)) => {
            if tx.get(&worker_key)?.is_some()
                || worker.certificate_sha256 == candidate.next.worker().certificate_sha256
            {
                return fault(Fault::Unauthorized);
            }
            save(tx, data_worker::PREPARED, &candidate.next)?;
            old.authorization = Some(EksAuthorization {
                operation: operation_id,
                candidate: candidate.clone(),
                signature: request.signature.clone(),
            });
            old
        }
        _ => return fault(Fault::Conflict),
    };
    save(tx, &record_key, &record)?;
    state.enrollment_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(ControlReply::EksReplacement(Box::new(record)))
}

pub(super) fn complete(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    worker: WorkerIdentity,
    mounted: &EksMountedReplacement,
) -> Result<ControlReply, ProtocolError> {
    let Some((record, drain)) = replacement(tx, worker, mounted.drain_id)? else {
        return fault(Fault::Unauthorized);
    };
    let permit = record.permit(&drain)?;
    let old_claim =
        load::<GrowthRecord>(tx, &growth::key(permit.next.claim_id))?.ok_or_else(unknown)?;
    let new_claim =
        load::<GrowthRecord>(tx, &growth::key(permit.add_pool_claim))?.ok_or_else(unknown)?;
    if mounted
        .verify(scope, &permit, &old_claim, &new_claim)
        .is_err()
    {
        return fault(Fault::Unauthorized);
    }
    let current = key(WORKERS, worker.worker_id);
    let registered = key(data_worker::REGISTERED, worker.worker_id);
    if let Some(entry) = load::<WorkerRecord>(tx, &current)? {
        let registration =
            load::<super::super::data_worker::DataWorkerRegistration>(tx, &registered)?;
        return if WorkerIdentity::from(&entry.enrollment) == worker
            && registration.as_ref().is_some_and(|entry| {
                entry.intent == permit.next
                    && entry.mount == mounted.primary
                    && entry
                        .replacement
                        .as_ref()
                        .is_some_and(|old| old == &mounted.added)
            }) {
            Ok(ControlReply::Enrolled(worker))
        } else {
            fault(Fault::Conflict)
        };
    }
    if !data_worker::ready_for_join(state, permit.next.run_id)
        || load::<super::super::DataWorkerIntent>(tx, data_worker::PREPARED)?.as_ref()
            != Some(&permit.next)
        || !super::super::worker_drain::eligible_add_pool(&drain.intent, &old_claim, &new_claim)
        || load::<Uuid>(tx, growth::ACTIVE)? != Some(permit.add_pool_claim)
    {
        return fault(Fault::NotReady);
    }
    save(
        tx,
        &current,
        &WorkerRecord {
            enrollment: permit.next.enrollment.clone(),
            previous_process: None,
        },
    )?;
    save(
        tx,
        &registered,
        &super::super::data_worker::DataWorkerRegistration {
            intent: permit.next,
            mount: mounted.primary.clone(),
            replacement: Some(mounted.added.clone()),
        },
    )?;
    tx.delete(data_worker::PREPARED)?;
    state.enrollment_revision = state
        .enrollment_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(ControlReply::Enrolled(worker))
}

fn replacement(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
    id: Uuid,
) -> Result<Option<(EksReplacementRecord, WorkerDrainRecord)>, ProtocolError> {
    if load::<Uuid>(tx, &worker_drain::barrier_key(worker.worker_id))? != Some(id) {
        return Ok(None);
    }
    let Some(record) = load::<EksReplacementRecord>(tx, &record_key(id))? else {
        return Ok(None);
    };
    if !record
        .authorization
        .as_ref()
        .is_some_and(|old| old.candidate.next.worker() == worker)
    {
        return Ok(None);
    }
    let drain =
        load::<WorkerDrainRecord>(tx, &worker_drain::record_key(id))?.ok_or_else(unknown)?;
    Ok(Some((record, drain)))
}

pub(super) fn can_join(
    tx: &mut GroupTransaction<'_>,
    target: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    let Some(id) = load::<Uuid>(tx, &worker_drain::barrier_key(target.worker_id))? else {
        return Ok(false);
    };
    Ok(replacement(tx, target, id)?.is_some_and(|(record, _)| !record.published))
}

pub(super) fn publish(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    pending: &ReadyPending,
    claim: &GrowthRecord,
    proof: &crate::oltp_deployment::controller::MountedGrowthStamp,
) -> Result<Option<Fault>, ProtocolError> {
    let target = pending.target;
    let barrier = worker_drain::barrier_key(target.worker_id);
    let Some(id) = load::<Uuid>(tx, &barrier)? else {
        return Ok(Some(Fault::Unauthorized));
    };
    let Some((mut record, drain)) = replacement(tx, target, id)? else {
        return Ok(Some(Fault::Unauthorized));
    };
    let permit = record.permit(&drain)?;
    if permit.next.claim_id != claim.intent.claim_id || record.published || proof.worker != target {
        return Ok(Some(Fault::Unauthorized));
    }
    match growth::publish_replacement_pool(tx, state, &drain.intent, claim, &permit, proof)? {
        ControlReply::Growth(_) => {
            record.published = true;
            save(tx, &record_key(id), &record)?;
            tx.delete(&barrier)?;
            Ok(None)
        }
        ControlReply::Rejected(fault) => Ok(Some(fault)),
        _ => Err(unknown()),
    }
}
