use super::super::restart::{RestartIntent, RestartMountKey, RestartPreparation};
use super::*;

impl ControlHandle {
    #[doc(hidden)]
    pub async fn register_restart_mount(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        pool_id: Uuid,
        generation: u64,
        key: &RestartMountKey,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let public_key = key.public_key();
        let worker = permit.local;
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&(
                worker, pool_id, generation, public_key,
            ))?,
        )?;
        self.submit(ControlCommand::RegisterRestartMount {
            operation_id,
            worker,
            pool_id,
            generation,
            public_key,
            authorization,
        })
        .await
    }

    #[doc(hidden)]
    pub async fn prepare_restart(
        &self,
        permit: &LocalOperatorPermit,
        intent: RestartIntent,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        intent.validate()?;
        if !self
            .policy
            .certificate_pins
            .contains(&intent.next.certificate_sha256)
        {
            return Err(unauthorized());
        }
        let operation_id = intent.id;
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&intent)?,
        )?;
        self.submit(ControlCommand::PrepareRestart {
            operation_id,
            intent,
            authorization,
        })
        .await
    }

    #[doc(hidden)]
    pub async fn complete_restart(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        intent: RestartIntent,
        key: &RestartMountKey,
        exclusion: VerifiedExclusion,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        if permit.local != WorkerIdentity::from(&intent.next)
            || !Arc::ptr_eq(&permit.shared, self.resources())
        {
            return Err(unauthorized());
        }
        let proof = key.sign(self.identity(), &intent)?;
        self.submit(ControlCommand::CompleteRestart {
            operation_id,
            intent,
            proof,
            exclusion: exclusion.stamp,
        })
        .await
    }

    pub(crate) async fn restart_preparation(
        &self,
        revision: u64,
    ) -> Result<Option<RestartPreparation>, ProtocolError> {
        let state = self.inspect().await?.ok_or_else(stale)?;
        if state.enrollment_revision != revision {
            return Err(stale());
        }
        Ok(state.restart)
    }

    #[doc(hidden)]
    pub async fn admission_authority(
        &self,
        scope: &QueueScope,
    ) -> Result<QueueAuthority, ProtocolError> {
        self.authority(scope, None, true).await
    }
}
