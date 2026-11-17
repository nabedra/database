//! Durable identity reservation precedes every executable RAM admission. This
//! index cannot reconstruct work after the last holder stops: only the original
//! scope/UUID/digest/run/class and non-executable state survive.
use super::{
    coordinator::QueueCoordinator,
    replica::{AcceptedContext, OriginalAuthorization, QueueReadToken, QueueScope, RecordVersion},
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, WorkerSharedResources},
    oltp_replication::{
        authority::{
            control::ControlHandle,
            status::{
                IdentityLookup, IdentityReservation, RequestKind, StatusFault, StatusHandle,
                StatusReply, TerminalIdentity,
            },
            timestamp::TimestampHandle,
        },
        types::{Digest, PinIdentity, RequestBinding, StoragePin},
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

/// No Deserialize implementation. The original durable reservation, exact raw
/// bytes and authorized ingress context travel together until RAM installation.
pub(crate) struct ReservedSubmission {
    pub(super) reservation: IdentityReservation,
    pub(super) body: ChargedBytes,
    pub(super) authorization: OriginalAuthorization,
}
impl ReservedSubmission {
    pub(crate) fn pin_identity(&self) -> PinIdentity {
        PinIdentity {
            run_id: self.reservation.original_run,
            request_id: self.reservation.request_id,
            digest: self.reservation.digest,
            nonce: self.reservation.reservation_id,
        }
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
    status: Arc<StatusHandle>,
    timestamps: Arc<TimestampHandle>,
    scope: QueueScope,
    queue: Arc<QueueCoordinator>,
}
impl DurableAdmission {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        control: Arc<ControlHandle>,
        status: Arc<StatusHandle>,
        timestamps: Arc<TimestampHandle>,
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
    pub(crate) async fn reserve(
        &self,
        body: Vec<u8>,
        authorization: OriginalAuthorization,
        now_ms: u64,
    ) -> Result<ReservationResult, ProtocolError> {
        use super::worker_endpoint::AuthorityReader;
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
        let current = self.control.current(&self.scope).await?;
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
                let request = crate::protocol::parse_request(held.as_ref())?;
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
        // A lost response never establishes certain absence. The native/consensus
        // error propagates, and the same UUID+digest retry reconciles this entry.
        let applied = self
            .status
            .reserve_identity(current.ready.view.authority(), request_id, digest, kind)
            .await?;
        let (_, reply, _decode_owner) = applied.into_parts()?;
        match reply {
            StatusReply::Identity(IdentityLookup::Reserved(reservation)) => {
                if reservation.request_id != request_id
                    || reservation.digest != digest
                    || reservation.kind != kind
                    || reservation.original_run != current.ready.view.run_id
                {
                    return Err(unknown(
                        "identity reservation differs from its original request",
                    ));
                }
                Ok(ReservationResult::Reserved(ReservedSubmission {
                    reservation,
                    body: raw,
                    authorization,
                }))
            }
            StatusReply::Identity(IdentityLookup::Abandoned(reservation)) => {
                Ok(ReservationResult::Abandoned(reservation))
            }
            StatusReply::Identity(IdentityLookup::Terminal(terminal)) => {
                Ok(ReservationResult::Terminal(terminal))
            }
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
    pub(crate) async fn accept(
        &self,
        reserved: ReservedSubmission,
        read: QueueReadToken,
        now_ms: u64,
    ) -> Result<RecordVersion, ProtocolError> {
        use super::worker_endpoint::AuthorityReader;
        let current = self.control.current(&self.scope).await?;
        if current.ready.view.run_id != reserved.reservation.original_run
            || read.identity != reserved.pin_identity()
        {
            return Err(invalid("snapshot token differs from original reservation"));
        }
        let context = AcceptedContext {
            authorization: reserved.authorization,
            read: Some(read),
        };
        let request = RequestBinding {
            run_id: reserved.reservation.original_run,
            request_id: reserved.reservation.request_id,
            digest: reserved.reservation.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(&context)?,
        };
        let read = context.read.as_ref().expect("constructed read token");
        self.timestamps
            .bind_snapshot_context(
                current.ready.view,
                read.identity.clone(),
                read.storage_pin,
                request,
            )
            .await?;
        self.queue
            .admit(reserved.body.as_ref().to_vec(), context, now_ms)
            .await
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
