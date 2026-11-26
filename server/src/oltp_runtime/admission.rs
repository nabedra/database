//! Durable identity reservation precedes every executable RAM admission. This
//! index cannot reconstruct work after the last holder stops: only the original
//! scope/UUID/digest/run/class and non-executable state survive.
use super::{
    coordinator::QueueCoordinator,
    replica::{AcceptedContext, OriginalAuthorization, QueueReadToken, QueueScope},
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, WorkerSharedResources},
    oltp_replication::{
        authority::{
            control::ControlHandle,
            status::{
                IdentityLookup, IdentityReservation, RequestKind, StatusFault, StatusReply,
                StatusService, TerminalIdentity,
            },
            timestamp::TimestampService,
        },
        types::{Authority, AuthorityView, Digest, PinIdentity, RequestBinding, StoragePin},
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use uuid::Uuid;

/// No Deserialize implementation. The original durable reservation, exact raw
/// bytes and authorized ingress context travel together until RAM installation.
pub(crate) struct ReservedSubmission {
    pub(super) reservation: IdentityReservation,
    pub(super) body: ChargedBytes,
    pub(super) authorization: OriginalAuthorization,
}
impl ReservedSubmission {
    pub(crate) fn request_binding(
        &self,
        read: &QueueReadToken,
    ) -> Result<RequestBinding, ProtocolError> {
        let context = AcceptedContext {
            authorization: self.authorization.clone(),
            read: Some(read.clone()),
        };
        Ok(RequestBinding {
            run_id: self.reservation.original_run,
            request_id: self.reservation.request_id,
            digest: self.reservation.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(&context)?,
        })
    }
    pub(crate) fn pin_identity(&self) -> PinIdentity {
        PinIdentity {
            run_id: self.reservation.original_run,
            request_id: self.reservation.request_id,
            digest: self.reservation.digest,
            nonce: self.reservation.reservation_id,
        }
    }
}
pub(super) struct PreparedSubmission {
    pub request_id: Uuid,
    pub digest: Digest,
    kind: RequestKind,
    body: ChargedBytes,
    authorization: OriginalAuthorization,
}
impl PreparedSubmission {
    pub(super) fn body(&self) -> &[u8] {
        self.body.as_ref()
    }
}
pub(crate) enum ReservationResult {
    Reserved(ReservedSubmission),
    Abandoned(IdentityReservation),
    Terminal(TerminalIdentity),
}

pub(crate) struct DurableAdmission {
    shared: Arc<WorkerSharedResources>,
    control: Arc<ControlHandle>,
    status: Arc<dyn StatusService>,
    timestamps: Arc<dyn TimestampService>,
    scope: QueueScope,
    queue: Arc<QueueCoordinator>,
}
impl DurableAdmission {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        control: Arc<ControlHandle>,
        status: Arc<dyn StatusService>,
        timestamps: Arc<dyn TimestampService>,
        scope: QueueScope,
        queue: Arc<QueueCoordinator>,
    ) -> Result<Self, ProtocolError> {
        if control.identity().cluster_id != scope.cluster_id
            || control.identity().database != scope.database
            || status.identity().cluster_id != scope.cluster_id
            || status.identity().database != scope.database
            || timestamps.identity().cluster_id != scope.cluster_id
            || timestamps.identity().database != scope.database
        {
            return Err(invalid("durable admission has a foreign Control scope"));
        }
        Ok(Self {
            shared,
            control,
            status,
            timestamps,
            scope,
            queue,
        })
    }
    pub(super) async fn prepare(
        &self,
        body: Vec<u8>,
        authorization: OriginalAuthorization,
        now_ms: u64,
    ) -> Result<PreparedSubmission, ProtocolError> {
        if body.is_empty()
            || body.len() > crate::protocol::MAX_BODY_BYTES
            || authorization.database != self.scope.database
            || authorization.policy_revision == 0
            || authorization.ingress_id.is_nil()
            || authorization.ingress_incarnation.is_nil()
            || authorization
                .credential_expires_at_ms
                .is_some_and(|expiry| expiry <= now_ms)
        {
            return Err(invalid("invalid original admission context"));
        }
        let raw = self.shared.copy_bytes(&body)?;
        drop(body);
        let held = raw.clone();
        let shared = self.shared.clone();
        // The fixed physical executor owns scanning/decoding. Working memory is
        // sized from actual structure; no second full-task memory grant exists.
        let (request_id, digest, kind) = self
            .shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let bound = crate::oltp::mvcc::request_decode_budget(held.as_ref())?
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(64 << 10))
                    .and_then(|bytes| {
                        super::worker_endpoint::request_raw_clone_bytes(held.as_ref())
                            .ok()
                            .and_then(|extra| bytes.checked_add(extra))
                    })
                    .ok_or_else(|| invalid("request decode budget overflow"))?;
                let _working = shared.reserve_scratch(ByteClass::Data, bound)?;
                let request = crate::protocol::parse_oltp_request(held.as_ref())?;
                let kind = if request.operation.is_mutation() {
                    RequestKind::Mutation
                } else {
                    RequestKind::Read
                };
                Ok((
                    request.request_id,
                    Digest::from_bytes(Sha256::digest(held.as_ref()).into()),
                    kind,
                ))
            })
            .await?;
        Ok(PreparedSubmission {
            request_id,
            digest,
            kind,
            body: raw,
            authorization,
        })
    }
    pub(super) async fn reserve(
        &self,
        prepared: PreparedSubmission,
        expected: Option<Authority>,
    ) -> Result<ReservationResult, ProtocolError> {
        let current = self.control.admission_authority(&self.scope).await?;
        if expected.is_some_and(|expected| current.ready.view.authority() != expected) {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "index build owner changed before refresh reservation",
                Outcome::NotApplied,
            ));
        }
        let PreparedSubmission {
            request_id,
            digest,
            kind,
            body,
            authorization,
        } = prepared;
        // A lost response never establishes certain absence. The native/consensus
        // error propagates, and the same UUID+digest retry reconciles this entry.
        let applied = self
            .status
            .reserve_identity(current.ready.view.authority(), request_id, digest, kind)
            .await?;
        let (_, reply, _decode_owner) = applied.into_parts()?;
        #[cfg(test)]
        if super::recovery_tests::lose_reservation_reply(request_id, &reply).await {
            return Err(unknown("reservation reply lost after durable apply"));
        }
        match reply {
            StatusReply::Identity(IdentityLookup::Reserved(reservation)) => {
                if reservation.request_id != request_id
                    || reservation.digest != digest
                    || reservation.kind != kind
                    || reservation.original_run != current.ready.view.run_id
                    || reservation.scoped.is_some()
                {
                    return Err(unknown(
                        "identity reservation differs from its original request",
                    ));
                }
                Ok(ReservationResult::Reserved(ReservedSubmission {
                    reservation,
                    body,
                    authorization,
                }))
            }
            StatusReply::Identity(IdentityLookup::Abandoned(reservation)) => {
                Ok(ReservationResult::Abandoned(reservation))
            }
            StatusReply::Identity(IdentityLookup::Terminal(terminal)) => {
                Ok(ReservationResult::Terminal(terminal))
            }
            StatusReply::Identity(IdentityLookup::Rejected(rejection)) => Err(rejection
                .failure
                .result()
                .err()
                .ok_or_else(|| unknown("invalid rejected identity outcome"))?),
            StatusReply::Rejected(StatusFault::DigestConflict | StatusFault::TerminalConflict) => {
                Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "request UUID conflicts with its durable identity",
                    Outcome::NotApplied,
                ))
            }
            StatusReply::Rejected(StatusFault::StaleAuthority) => Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "request authority changed before reservation",
                Outcome::NotApplied,
            )),
            StatusReply::Rejected(_) => Err(invalid("identity reservation was rejected")),
            _ => Err(unknown("unexpected identity reservation response")),
        }
    }
    pub(crate) async fn begin_snapshot(
        &self,
        reserved: &ReservedSubmission,
    ) -> Result<StoragePin, ProtocolError> {
        use super::worker_endpoint::AuthorityReader;
        let current = self.control.current(&self.scope).await?;
        if current.ready.view.run_id != reserved.reservation.original_run {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "original request run was abandoned",
                Outcome::NotApplied,
            ));
        }
        self.timestamps
            .begin_snapshot(
                current.ready.view,
                reserved.pin_identity(),
                reserved.reservation,
            )
            .await
    }
    /// Catalog resolves the immutable lineage at this token's read timestamp.
    /// The TSO then binds its final hash before either RAM acceptance phase.
    pub(crate) fn context(
        &self,
        reserved: &ReservedSubmission,
        read: QueueReadToken,
    ) -> Result<AcceptedContext, ProtocolError> {
        if read.identity != reserved.pin_identity() {
            return Err(invalid("snapshot identity differs"));
        }
        Ok(AcceptedContext {
            authorization: reserved.authorization.clone(),
            read: Some(read),
        })
    }
    pub(crate) async fn bind_staged(
        &self,
        view: AuthorityView,
        context: &AcceptedContext,
    ) -> Result<RequestBinding, ProtocolError> {
        let read = context
            .read
            .as_ref()
            .ok_or_else(|| invalid("missing original snapshot token"))?;
        if view.run_id != read.identity.run_id {
            return Err(invalid("original request run was abandoned"));
        }
        let binding = RequestBinding {
            run_id: read.identity.run_id,
            request_id: read.identity.request_id,
            digest: read.identity.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(context)?,
        };
        self.timestamps
            .bind_snapshot_context(
                view,
                read.identity.clone(),
                read.storage_pin,
                binding,
            )
            .await?;
        Ok(binding)
    }
    pub(super) fn queue(&self) -> &Arc<QueueCoordinator> {
        &self.queue
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unknown(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::OutcomeUnknown, message, Outcome::Unknown)
}
