//! Protected HTTP administration for the worker that owns a database.
//! The HTTP boundary owns its admin credential; application tokens cannot
//! create this operator capability or submit raw Control commands.
use super::worker_runtime::WorkerRuntime;
use crate::{
    oltp::pool::ExecutionClass,
    oltp_replication::{
        authority::control::{ApiEnrollment, ControlReply, DataWorkerIntent, EksReplacementAction, ReplacementIntent, RestartIntent, ScopedQueueCapability, SignedEksReplacement, WorkerDrainIntent},
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    response,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    request_id: Uuid,
    operation: String,
    payload: Box<serde_json::value::RawValue>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Enrollment {
    expected: Option<WorkerIdentity>,
    next: ApiEnrollment,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedAdmissionChange {
    expected: ScopedQueueCapability,
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedTransfer {
    group_id: Uuid,
    next_owner: WorkerIdentity,
    replaces: Option<Uuid>,
}

impl WorkerRuntime {
    pub(super) async fn enroll_api_admin(
        self: Arc<Self>,
        body: Vec<u8>,
    ) -> Result<response::EncodedResponse, ProtocolError> {
        let shared = self.resources().clone();
        let request = shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                crate::protocol::json_input(&body, 64 << 10).map_err(|_| invalid())?;
                let request: Request = serde_json::from_slice(&body).map_err(|_| invalid())?;
                if request.request_id.is_nil() {
                    return Err(invalid());
                }
                if request.operation == "api.enroll" {
                    let enrollment: Enrollment =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    enrollment.next.validate()?;
                } else if request.operation == "worker.restart.prepare" {
                    let intent: RestartIntent =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    if intent.id != request.request_id {
                        return Err(invalid());
                    }
                    intent.validate()?;
                } else if matches!(request.operation.as_str(), "worker.system_replacement.check" | "worker.system_replacement.status") {
                    let check: super::worker_runtime::system_recovery::ReplacementCheck =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    check.validate()?;
                } else if request.operation == "worker.system_replacement.abandon" {
                    let abandon: super::worker_runtime::system_recovery::ReplacementAbandon =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    if abandon.id.is_nil() {
                        return Err(invalid());
                    }
                } else if request.operation == "worker.system_replacement.prepare" {
                    let intent: ReplacementIntent =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    if intent.id != request.request_id {
                        return Err(invalid());
                    }
                    intent.validate()?;
                } else if request.operation == "worker.drain" {
                    let intent: WorkerDrainIntent = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    intent.validate()?;
                } else if request.operation == "worker.replacement.check" {
                    serde_json::from_str::<DataWorkerIntent>(request.payload.get()).map_err(|_| invalid())?;
                } else if request.operation == "worker.replacement.challenge" {
                    serde_json::from_str::<EksReplacementAction>(request.payload.get()).map_err(|_| invalid())?;
                } else if request.operation == "worker.replacement.apply" {
                    serde_json::from_str::<SignedEksReplacement>(request.payload.get()).map_err(|_| invalid())?;
                } else if request.operation == "queue.scoped.admission" {
                    serde_json::from_str::<ScopedAdmissionChange>(request.payload.get()).map_err(|_| invalid())?;
                } else if request.operation == "queue.scoped.transfer" {
                    let transfer: ScopedTransfer = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    if transfer.group_id.is_nil() || transfer.next_owner.validate().is_err() {
                        return Err(invalid());
                    }
                } else if Self::validate_growth_admin(&request.operation, request.payload.get())? {
                } else if Self::validate_archive_admin(&request.operation, request.payload.get())? {
                } else if Self::validate_split_admin(&request.operation, request.payload.get())? {
                } else if Self::validate_index_admin(&request.operation, request.payload.get())? {
                } else if Self::validate_pool_drain_admin(&request.operation, request.payload.get())? {
                } else if Self::validate_placement_admin(&request.operation, request.payload.get())? {
                } else if !matches!(request.operation.as_str(), "cluster.connection" | "query.statistics" | "worker.drain.status" | "worker.stop_permit" | "worker.replacement.status" | "queue.scoped.status" | "worker.system_replacement.enable")
                    || request.payload.get().trim() != "{}"
                {
                    return Err(invalid());
                }
                Ok(request)
            })
            .await?;
        if request.operation == "cluster.connection" {
            return Ok(response::encode_success(
                request.request_id,
                &self.operator_connection().await?,
                false,
            ));
        }
        if request.operation == "query.statistics" {
            return self.query_statistics_admin(request.request_id).await;
        }
        if request.operation.starts_with("queue.scoped.") {
            let control = self.scoped_operator_client()?;
            return match request.operation.as_str() {
                "queue.scoped.status" => Ok(response::encode_success(
                    request.request_id, &control.scoped_queue_capability().await?, false,
                )),
                "queue.scoped.admission" => {
                    let payload: ScopedAdmissionChange =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    control.set_scoped_queue_admission(
                        request.request_id, payload.expected, payload.enabled,
                    ).await?;
                    Ok(response::encode_success(
                        request.request_id, &control.scoped_queue_capability().await?, true,
                    ))
                }
                "queue.scoped.transfer" => {
                    let payload: ScopedTransfer =
                        serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
                    let previous = control.scoped_queue_predecessor(payload.group_id).await?
                        .ok_or_else(|| ProtocolError::new(
                            ErrorCode::NotFound, "scoped queue placement unavailable", Outcome::NotApplied,
                        ))?;
                    let transfer = control.begin_scoped_queue_transfer(
                        request.request_id, previous, payload.next_owner, payload.replaces,
                    ).await?;
                    Ok(response::encode_success(request.request_id, &transfer, true))
                }
                _ => Err(invalid()),
            };
        }
        if request.operation == "worker.restart.prepare" {
            let intent = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.prepare_worker_restart(intent).await;
        }
        if request.operation == "worker.system_replacement.status" {
            let check = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.system_replacement_status(request.request_id, check).await;
        }
        if request.operation == "worker.system_replacement.check" {
            let check = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.check_system_replacement(request.request_id, check).await;
        }
        if request.operation == "worker.system_replacement.enable" {
            return self.enable_system_replacement(request.request_id).await;
        }
        if request.operation == "worker.system_replacement.abandon" {
            let abandon: super::worker_runtime::system_recovery::ReplacementAbandon =
                serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.abandon_system_replacement(request.request_id, abandon.id).await;
        }
        if request.operation == "worker.system_replacement.prepare" {
            let intent = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.prepare_system_replacement(intent).await;
        }
        if request.operation == "worker.drain" {
            let intent = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.begin_worker_drain_admin(request.request_id, intent).await;
        }
        if request.operation == "worker.drain.status" {
            return self.worker_drain_status_admin(request.request_id).await;
        }
        if request.operation == "worker.stop_permit" {
            return self.worker_stop_permit_admin(request.request_id).await;
        }
        if request.operation == "worker.replacement.check" {
            let next = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.check_worker_replacement_admin(request.request_id, next).await;
        }
        if request.operation == "worker.replacement.status" {
            return self.read_eks_replacement_admin(request.request_id).await;
        }
        if request.operation == "worker.replacement.challenge" {
            let action = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.challenge_eks_replacement_admin(request.request_id, action).await;
        }
        if request.operation == "worker.replacement.apply" {
            let signed = serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
            return self.apply_eks_replacement_admin(request.request_id, signed).await;
        }
        if request.operation == "storage.shard.split" {
            return self.split_admin(request.request_id, request.payload.get()).await;
        }
        if request.operation == "storage.shard.split.status" {
            return self.automatic_split_status(request.request_id);
        }
        if request.operation.starts_with("storage.index.") {
            return self
                .index_admin(request.request_id, &request.operation, request.payload.get())
                .await;
        }
        if request.operation.starts_with("storage.pool.") {
            return self.pool_drain_admin(request.request_id, &request.operation, request.payload.get()).await;
        }
        if request.operation.starts_with("storage.replica.") {
            return self.placement_admin(request.request_id, &request.operation, request.payload.get()).await;
        }
        if request.operation.starts_with("archive.") {
            return self
                .archive_admin(
                    request.request_id,
                    &request.operation,
                    request.payload.get(),
                )
                .await;
        }
        if request.operation.starts_with("storage.growth.") {
            return self
                .growth_admin(
                    request.request_id,
                    &request.operation,
                    request.payload.get(),
                )
                .await;
        }
        let payload: Enrollment =
            serde_json::from_str(request.payload.get()).map_err(|_| invalid())?;
        let applied = self
            .enroll_api_operator(request.request_id, payload.expected, payload.next)
            .await?;
        let (position, reply, _owner) = applied.into_parts()?;
        match reply {
            ControlReply::Enrolled(identity) => Ok(response::encode_success(
                request.request_id,
                &serde_json::json!({"identity":identity,"applied_at":position,"service_role":"api_ingress"}),
                true,
            )),
            ControlReply::Rejected(_) => Err(ProtocolError::new(
                ErrorCode::Conflict,
                "API enrollment authority or previous incarnation differs",
                Outcome::NotApplied,
            )),
            _ => Err(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "API enrollment requires reconciliation",
                Outcome::Unknown,
            )),
        }
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid Control administration request",
        Outcome::NotApplied,
    )
}
