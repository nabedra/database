use super::*;
use crate::oltp_replication::authority::control::{DataWorkerIntent, EksReplacementAction, SignedEksReplacement, WorkerDrainIntent};

impl WorkerRuntime {
    pub(in crate::oltp_runtime) async fn read_eks_replacement_admin(
        &self, id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        let record = self.services.control.eks_replacement_record(&self.services.operator, id).await?;
        Ok(response::encode_success(id, &serde_json::json!({"replacement": record}), false))
    }

    pub(in crate::oltp_runtime) async fn challenge_eks_replacement_admin(
        &self, id: Uuid, action: EksReplacementAction,
    ) -> Result<EncodedResponse, ProtocolError> {
        let challenge = self.services.control.eks_replacement_challenge(&self.services.operator, id, action).await?;
        Ok(response::encode_success(id, &challenge, false))
    }

    pub(in crate::oltp_runtime) async fn apply_eks_replacement_admin(
        &self, id: Uuid, request: SignedEksReplacement,
    ) -> Result<EncodedResponse, ProtocolError> {
        let record = self.services.control.apply_eks_replacement(&self.services.operator, id, request).await?;
        Ok(response::encode_success(id, &record, true))
    }
    pub(in crate::oltp_runtime) async fn begin_worker_drain_admin(
        &self,
        id: Uuid,
        intent: WorkerDrainIntent,
    ) -> Result<EncodedResponse, ProtocolError> {
        let record = self
            .services
            .control
            .begin_worker_drain(&self.services.operator, id, intent)
            .await?;
        Ok(response::encode_success(id, &record, true))
    }

    pub(in crate::oltp_runtime) async fn worker_drain_status_admin(
        &self,
        id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        let record = self
            .services
            .control
            .worker_drain(id)
            .await?
            .ok_or_else(|| {
                ProtocolError::new(
                    ErrorCode::NotFound,
                    "worker drain is unknown",
                    Outcome::NotApplied,
                )
            })?;
        Ok(response::encode_success(id, &record, false))
    }

    pub(in crate::oltp_runtime) async fn worker_stop_permit_admin(
        &self,
        id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        let permit = self
            .services
            .control
            .worker_stop_permit(&self.services.operator, id)
            .await?;
        Ok(response::encode_success(id, &permit, false))
    }

    pub(in crate::oltp_runtime) async fn check_worker_replacement_admin(
        &self,
        id: Uuid,
        next: DataWorkerIntent,
    ) -> Result<EncodedResponse, ProtocolError> {
        let check = self
            .services
            .control
            .check_worker_replacement(&self.services.operator, id, &next)
            .await?;
        Ok(response::encode_success(id, &check, false))
    }
}
