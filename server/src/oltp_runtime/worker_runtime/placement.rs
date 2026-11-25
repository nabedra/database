use super::*;
use crate::oltp_replication::authority::control::{ReplicaMovePhase, ReplicaMoveRequest};
use crate::oltp_replication::manager::PlacementCatalog;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteRequest {
    group: GroupIdentity,
}

#[derive(Serialize)]
struct RouteReply<'a> {
    record: &'a crate::oltp_replication::authority::control::GroupRecord,
    members: &'a [ReplicaMember],
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_placement_admin(
        operation: &str,
        payload: &str,
    ) -> Result<bool, ProtocolError> {
        match operation {
            "storage.replica.move" => {
                let request: ReplicaMoveRequest =
                    serde_json::from_str(payload).map_err(|_| invalid())?;
                request.validate()?;
            }
            "storage.replica.placement" => {
                let request: RouteRequest = serde_json::from_str(payload).map_err(|_| invalid())?;
                if request.group.validate().is_err()
                    || !matches!(request.group.kind, GroupKind::Data { .. })
                {
                    return Err(invalid());
                }
            }
            "storage.replica.move.status" if payload.trim() == "{}" => {}
            "storage.replica.move.status" => return Err(invalid()),
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub(in crate::oltp_runtime) async fn placement_admin(
        &self,
        id: Uuid,
        operation: &str,
        payload: &str,
    ) -> Result<EncodedResponse, ProtocolError> {
        let placement = self.services.placement.as_ref().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "replica moves require mounted RF3 workers",
                Outcome::NotApplied,
            )
        })?;
        match operation {
            "storage.replica.move" => {
                let request = serde_json::from_str(payload).map_err(|_| invalid())?;
                let catalog = PlacementCatalog {
                    catalog: &self.services.catalog,
                    status: self.services.status.as_ref(),
                };
                let record = placement
                    .advance(&self.services.operator, &catalog, id, request)
                    .await?;
                let retired = matches!(record.phase, ReplicaMovePhase::Retired { .. });
                Ok(response::encode_success(id, &record, retired))
            }
            "storage.replica.placement" => {
                let request: RouteRequest = serde_json::from_str(payload).map_err(|_| invalid())?;
                let route = placement.route(&request.group).await?;
                Ok(response::encode_success(
                    id,
                    &RouteReply {
                        record: &route.record,
                        members: &route.members,
                    },
                    false,
                ))
            }
            "storage.replica.move.status" => {
                let record = self
                    .services
                    .control
                    .replica_move(id)
                    .await?
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ErrorCode::NotFound,
                            "replica move is unknown",
                            Outcome::NotApplied,
                        )
                    })?;
                Ok(response::encode_success(id, &record, false))
            }
            _ => Err(invalid()),
        }
    }
}

#[track_caller]
fn invalid() -> ProtocolError {
    nabedra_rt::diag_at("invalid", std::panic::Location::caller());
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid replica move administration request",
        Outcome::NotApplied,
    )
}
