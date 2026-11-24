use super::*;
use crate::oltp_replication::authority::control::DataWorkerIntent;

impl ControlHandle {
    #[doc(hidden)]
    pub async fn prepare_data_worker(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        intent: DataWorkerIntent,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        if self
            .policy
            .certificate_pins
            .contains(&intent.worker().certificate_sha256)
        {
            return Err(unauthorized());
        }
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&intent)?,
        )?;
        self.submit(ControlCommand::PrepareDataWorker {
            operation_id,
            intent: Box::new(intent),
            authorization,
        })
        .await
    }

    pub(crate) async fn prepared_data_worker(
        &self,
        revision: u64,
    ) -> Result<Option<WorkerEnrollment>, ProtocolError> {
        let raw = self
            .read(move |view| {
                if state(view)?.enrollment_revision != revision {
                    return Err(stale());
                }
                let candidate: Option<DataWorkerIntent> =
                    load(view, machine::data_worker::PREPARED)?;
                view.encode(&candidate.map(|intent| intent.enrollment))
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    #[doc(hidden)]
    pub async fn data_worker(
        &self,
        worker: WorkerIdentity,
    ) -> Result<Option<Uuid>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let registration: Option<super::super::data_worker::DataWorkerRegistration> = load(
                    view,
                    &machine::key(machine::data_worker::REGISTERED, worker.worker_id),
                )?;
                view.encode(
                    &registration
                        .filter(|registration| registration.intent.worker() == worker)
                        .map(|registration| registration.intent.claim_id),
                )
            })
            .await?;
        machine::decode(raw.as_ref())
    }
}
