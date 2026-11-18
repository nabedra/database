use super::*;
use crate::oltp_deployment::inventory::PoolDrainRequest;
use crate::oltp_replication::authority::control::PoolDrainPhase;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancel {
    drain_id: Uuid,
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_pool_drain_admin(
        operation: &str,
        payload: &str,
    ) -> Result<bool, ProtocolError> {
        match operation {
            "storage.pool.drain" => {
                let request: PoolDrainRequest =
                    serde_json::from_str(payload).map_err(|_| invalid())?;
                if request.pool_id.is_nil() || request.generation == 0 {
                    return Err(invalid());
                }
            }
            "storage.pool.drain.cancel" => {
                let request: Cancel = serde_json::from_str(payload).map_err(|_| invalid())?;
                if request.drain_id.is_nil() {
                    return Err(invalid());
                }
            }
            "storage.pool.drain.status" | "storage.pool.detach_permit" => {
                if payload.trim() != "{}" {
                    return Err(invalid());
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub(in crate::oltp_runtime) async fn pool_drain_admin(
        &self,
        id: Uuid,
        operation: &str,
        payload: &str,
    ) -> Result<EncodedResponse, ProtocolError> {
        let control = &self.services.control;
        let operator = &self.services.operator;
        match operation {
            "storage.pool.drain" => {
                let request = serde_json::from_str(payload).map_err(|_| invalid())?;
                let record = self
                    .services
                    .pools
                    .drain_secondary(control, operator, id, request)
                    .await?;
                Ok(response::encode_success(
                    id,
                    &record,
                    record.phase == PoolDrainPhase::Closed,
                ))
            }
            "storage.pool.drain.status" => {
                let record = control
                    .pool_drain(id)
                    .await?
                    .filter(|record| {
                        record.mount.expected.worker == self.services.pools.worker_identity()
                    })
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ErrorCode::NotFound,
                            "pool drain is unknown for this worker",
                            Outcome::NotApplied,
                        )
                    })?;
                Ok(response::encode_success(id, &record, false))
            }
            "storage.pool.drain.cancel" => {
                let request: Cancel = serde_json::from_str(payload).map_err(|_| invalid())?;
                let record = control
                    .cancel_pool_drain(operator, id, request.drain_id)
                    .await?;
                Ok(response::encode_success(id, &record, true))
            }
            "storage.pool.detach_permit" => {
                let permit = control.pool_detach_permit(operator, id).await?;
                Ok(response::encode_success(id, permit.record(), false))
            }
            _ => Err(invalid()),
        }
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid pool drain administration request",
        Outcome::NotApplied,
    )
}
