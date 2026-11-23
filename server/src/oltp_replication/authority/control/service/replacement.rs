use super::super::{
    DataWorkerIntent, EksReplacementAction, SignedEksReplacement,
    data_worker::DataWorkerRegistration,
    growth,
    worker_drain::{EksReplacementRecord, WorkerDrainPhase, WorkerDrainRecord, eks_statement},
};
use super::*;
use crate::{
    oltp_deployment::controller::GrowthRecord, oltp_replication::owned::serialized_digest,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksReplacementChallenge {
    digest: Digest,
    verifier_key: Digest,
}

impl ControlHandle {
    pub(crate) async fn eks_replacement_record(
        &self,
        operator: &LocalOperatorPermit,
        id: Uuid,
    ) -> Result<Option<EksReplacementRecord>, ProtocolError> {
        self.local_operator(operator, id, serialized_digest(&id)?)?;
        let bytes = self
            .read(move |view| {
                let record: Option<EksReplacementRecord> =
                    load(view, &machine::replacement::record_key(id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(bytes.as_ref())
    }

    pub(crate) async fn eks_replacement_challenge(
        &self,
        operator: &LocalOperatorPermit,
        operation: Uuid,
        action: EksReplacementAction,
    ) -> Result<EksReplacementChallenge, ProtocolError> {
        self.local_operator(operator, operation, serialized_digest(&action)?)?;
        if matches!(&action, EksReplacementAction::Exclude(_)) {
            self.worker_stop_permit(operator, action.drain_id()).await?;
        }
        if matches!(&action, EksReplacementAction::Authorize(value)
            if self.policy.certificate_pins.contains(&value.next.worker().certificate_sha256))
        {
            return Err(unauthorized());
        }
        let scope = self.identity().clone();
        let bytes = self
            .read(move |view| {
                let id = action.drain_id();
                let drain: WorkerDrainRecord =
                    load(view, &machine::worker_drain::record_key(id))?.ok_or_else(unauthorized)?;
                let WorkerDrainPhase::Closed { ready, .. } = drain.phase else {
                    return Err(unauthorized());
                };
                let current = state(view)?;
                let target = drain.intent.worker;
                if current.ready != ready
                    || current.invalid_ready != 0
                    || current.pending.is_some()
                    || current.restart.is_some()
                    || view.get(machine::placement::ACTIVE)?.is_some()
                    || view.get(machine::worker_drain::ACTIVE)?.is_some()
                    || view.get(machine::data_worker::PREPARED)?.is_some()
                    || load::<Uuid>(view, &machine::worker_drain::barrier_key(target.worker_id))?
                        != Some(id)
                {
                    return Err(stale());
                }
                let registration: DataWorkerRegistration = load(
                    view,
                    &machine::key(machine::data_worker::REGISTERED, target.worker_id),
                )?
                .ok_or_else(unauthorized)?;
                let primary: GrowthRecord =
                    load(view, &growth::key(drain.growth_claim))?.ok_or_else(unauthorized)?;
                let add = drain
                    .intent
                    .replacement_growth_claim
                    .ok_or_else(unauthorized)?;
                let growth: GrowthRecord =
                    load(view, &growth::key(add))?.ok_or_else(unauthorized)?;
                let exclusion: Option<EksReplacementRecord> =
                    load(view, &machine::replacement::record_key(id))?;
                if !registration.completed(&primary)
                    || load::<Uuid>(view, growth::ACTIVE)? != Some(add)
                {
                    return Err(stale());
                }
                let digest = eks_statement(
                    &scope,
                    operation,
                    &drain,
                    &registration.intent,
                    &primary,
                    &growth,
                    &action,
                    exclusion.as_ref(),
                )?;
                view.encode(&EksReplacementChallenge {
                    digest,
                    verifier_key: registration
                        .intent
                        .eks_replacement_key
                        .ok_or_else(unauthorized)?,
                })
            })
            .await?;
        machine::decode(bytes.as_ref())
    }

    pub(crate) async fn apply_eks_replacement(
        &self,
        operator: &LocalOperatorPermit,
        operation: Uuid,
        request: SignedEksReplacement,
    ) -> Result<EksReplacementRecord, ProtocolError> {
        if matches!(&request.action, EksReplacementAction::Authorize(value)
            if self.policy.certificate_pins.contains(&value.next.worker().certificate_sha256))
        {
            return Err(unauthorized());
        }
        let worker = operator.local;
        let authorization =
            self.local_operator(operator, operation, serialized_digest(&(worker, &request))?)?;
        match self
            .submit(ControlCommand::EksWorkerReplacement {
                operation_id: operation,
                worker,
                authorization,
                request: Box::new(request),
            })
            .await?
            .into_parts()?
            .1
        {
            ControlReply::EksReplacement(record) => Ok(*record),
            ControlReply::Rejected(Fault::Unauthorized) => Err(unauthorized()),
            ControlReply::Rejected(_) => Err(stale()),
            _ => Err(unavailable("EKS replacement awaits reconciliation")),
        }
    }

    pub(in crate::oltp_replication::authority::control) async fn own_eks_replacement(
        &self,
        worker: WorkerIdentity,
        token_digest: Digest,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let now = nabedra_rt::time::wall_now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| unauthorized())?
                .as_secs();
            let id: Uuid = load(view, &machine::worker_drain::barrier_key(worker.worker_id))?
                .ok_or_else(unauthorized)?;
            let record: EksReplacementRecord =
                load(view, &machine::replacement::record_key(id))?.ok_or_else(unauthorized)?;
            let drain: WorkerDrainRecord =
                load(view, &machine::worker_drain::record_key(id))?.ok_or_else(unauthorized)?;
            let permit = record.permit(&drain)?;
            let prepared: Option<DataWorkerIntent> = load(view, machine::data_worker::PREPARED)?;
            let current: Option<WorkerRecord> =
                load(view, &machine::key(WORKERS, worker.worker_id))?;
            if permit.next.worker() != worker
                || state(view)?.ready.view.run_id != permit.next.run_id
                || record.published
                || record.authorization.as_ref().is_none_or(|authorization| {
                    authorization.candidate.projected_token_sha256 != token_digest
                        || authorization.candidate.projected_token_expires_at <= now
                })
                || (prepared.as_ref() != Some(&permit.next)
                    && current.is_none_or(|entry| entry.enrollment != permit.next.enrollment))
            {
                return Err(unauthorized());
            }
            view.encode(&permit)
        })
        .await
    }

    pub(in crate::oltp_replication::authority::control) async fn authorize_data_worker_join(
        &self,
        worker: WorkerIdentity,
        token: Option<Digest>,
    ) -> Result<(), ProtocolError> {
        let raw = self
            .read(move |view| {
                let registered: DataWorkerRegistration = load(
                    view,
                    &machine::key(machine::data_worker::REGISTERED, worker.worker_id),
                )?
                .ok_or_else(unauthorized)?;
                if registered.intent.worker() != worker
                    || registered.replacement.is_some() != token.is_some()
                {
                    return Err(unauthorized());
                }
                view.encode(&())
            })
            .await?;
        machine::decode::<()>(raw.as_ref())?;
        if let Some(token) = token {
            self.own_eks_replacement(worker, token).await?;
        }
        Ok(())
    }
}
