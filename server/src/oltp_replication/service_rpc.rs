//! Authenticated application service RPCs. Ordinary worker TLS enrollment is
//! not permission to submit an arbitrary Raft command or manufacture a proof.
use super::{
    multiplex::{Purpose, ReceivedReply},
    owned::serialized_digest,
    types::*,
};
use crate::{
    oltp::pool::ScratchLease,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::Serialize;
use std::sync::Arc;
mod metadata;
mod participant;
mod participant_full;
#[cfg(test)]
pub(crate) use participant_full::continuation_fixture;
mod read_attempt;
pub(crate) use metadata::{
    AdmissionTicketIssuer, AuthenticatedMetadataReply, BackupPublicationReader,
    OwnerAdmissionObservation, OwnerAdmissionReader, OwnerAdmissionTicket, StatusClient,
    StatusEndpoint, TimestampClient, TimestampEndpoint,
};
#[cfg(test)]
pub(crate) use read_attempt::fixture as read_fixture;
pub(crate) use read_attempt::{
    ReadAttempts, ReadGrant, ReadScope, ScheduledReadError, UnsettledRead,
};
mod status;
pub(crate) use participant::{ParticipantProofClient, ParticipantProofEndpoint};
pub(crate) use participant_full::{ParticipantClient, ParticipantEndpoint};
#[cfg(test)]
mod tests;
pub(crate) use status::{StatusProofClient, StatusProofEndpoint};

use super::{
    authority::control::{
        ControlClient, ServiceAction, stale_service_route, unsupported_service_route,
    },
    multiplex::{
        AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture, ReceivePlan,
        RequestError, RequestTarget, TrafficClass, WorkerClient,
    },
};
use crate::oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, WorkerSharedResources};
use serde::Deserialize;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::{Instant, sleep_until, timeout_at};

// Registered election timeouts reach three seconds. Leave time to refresh
// routes and reach the new leader without using the full public wait budget.
const SERVICE_CONVERGENCE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Method {
    StatusCall {
        operation: u8,
        mutation: bool,
    },
    TimestampCall {
        operation: u8,
        mutation: bool,
    },
    StatusDecision {
        txn: TxnId,
        manifest: ManifestRef,
    },
    StatusSettlement {
        txn: TxnId,
        manifest: ManifestRef,
    },
    ParticipantSeal {
        txn: TxnId,
        manifest: ManifestRef,
    },
    ParticipantResolution {
        txn: TxnId,
        manifest: ManifestRef,
    },
    ParticipantGuard,
    ParticipantIndexClosed {
        owner: MaintenanceOwnerRef,
    },
    StatusParticipant {
        txn: TxnId,
        manifest: ManifestRef,
        group_id: uuid::Uuid,
    },
    ParticipantRead {
        read_ts: ReadTs,
    },
    ParticipantScan,
    ParticipantScanOpen {
        scope: ReadScope,
        input: Digest,
    },
    ParticipantScanRun {
        ticket: uuid::Uuid,
    },
    ParticipantReadPage,
    ParticipantImages,
    ParticipantArchiveEffects {
        txn: TxnId,
        manifest: ManifestRef,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    },
    ParticipantActivation,
    ParticipantAcquire,
    ParticipantHolder {
        table_id: uuid::Uuid,
    },
    ParticipantFence {
        transition_id: uuid::Uuid,
        expected: Authority,
        next: Authority,
    },
    ParticipantPropose {
        progress: bool,
    },
}
impl Method {
    fn purpose(self) -> Purpose {
        match self {
            Self::StatusCall { mutation: true, .. }
            | Self::TimestampCall { mutation: true, .. } => Purpose::ServiceMutation,
            Self::ParticipantRead { .. }
            | Self::ParticipantReadPage
            | Self::ParticipantImages
            | Self::ParticipantArchiveEffects { .. }
            | Self::ParticipantScan
            | Self::ParticipantScanRun { .. }
            | Self::ParticipantPropose { progress: false } => Purpose::Storage,
            Self::ParticipantAcquire
            | Self::ParticipantScanOpen { .. }
            | Self::ParticipantFence { .. }
            | Self::ParticipantPropose { progress: true } => Purpose::Settlement,
            _ => Purpose::Authority,
        }
    }
    fn class(self) -> TrafficClass {
        match self.purpose() {
            Purpose::Storage => TrafficClass::Storage,
            Purpose::Settlement => TrafficClass::Settlement,
            Purpose::ServiceMutation => TrafficClass::ServiceMutation,
            _ => TrafficClass::ServiceAuthority,
        }
    }
    fn can_repeat_read(self) -> bool {
        matches!(self.purpose(), Purpose::Authority | Purpose::Storage)
            && !matches!(
                self,
                Self::ParticipantPropose { .. } | Self::ParticipantScanRun { .. }
            )
    }
    fn input_limit(self) -> usize {
        match self {
            Self::StatusCall { .. } | Self::TimestampCall { .. } => 128 << 10,
            Self::ParticipantGuard | Self::ParticipantAcquire => 4096,
            Self::ParticipantReadPage => 1 << 20,
            Self::ParticipantRead { .. }
            | Self::ParticipantImages
            | Self::ParticipantScan
            | Self::ParticipantScanRun { .. }
            | Self::ParticipantPropose { progress: false } => {
                crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES
            }
            Self::ParticipantPropose { progress: true } => 128 << 10,
            _ => 0,
        }
    }
}
async fn registered_target(
    control: &ControlClient,
    group: uuid::Uuid,
) -> Result<GroupIdentity, ProtocolError> {
    let reply = control.group(group).await?;
    let record = reply.decode_section::<Option<super::authority::control::GroupRecord>>(0)?;
    record
        .as_ref()
        .map(|record| record.identity.clone())
        .ok_or_else(unavailable)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    to_node: u64,
    action: ServiceAction,
    method: Method,
    payload_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement_generation: Option<uuid::Uuid>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHeader {
    node: u64,
    nonce: uuid::Uuid,
    bytes: u64,
    digest: Digest,
    error: Option<ProtocolError>,
}

enum CallError {
    Transport(RequestError),
    Local(ProtocolError),
}
impl From<ProtocolError> for CallError {
    fn from(error: ProtocolError) -> Self {
        Self::Local(error)
    }
}
impl From<CallError> for ProtocolError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Transport(error) => error.into(),
            CallError::Local(error) => error,
        }
    }
}

struct Client {
    group: GroupIdentity,
    shared: Arc<WorkerSharedResources>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
    action: ServiceAction,
    leader: AtomicU64,
    _metadata: Arc<ScratchLease>,
}
impl Client {
    fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Self, ProtocolError> {
        group.validate().map_err(|_| invalid())?;
        let metadata = Arc::new(shared.reserve_scratch(
            ByteClass::Progress,
            group.database.capacity_bytes() + std::mem::size_of::<Self>(),
        )?);
        Ok(Self {
            group,
            shared,
            transport,
            control,
            action,
            leader: AtomicU64::new(0),
            _metadata: metadata,
        })
    }
    async fn read(&self, method: Method) -> Result<ServiceResponse, ProtocolError> {
        self.call(method, None).await
    }
    async fn call(
        &self,
        method: Method,
        payload: Option<ChargedBytes>,
    ) -> Result<ServiceResponse, ProtocolError> {
        self.call_as(self.action, method, payload).await
    }
    async fn call_as(
        &self,
        action: ServiceAction,
        method: Method,
        payload: Option<ChargedBytes>,
    ) -> Result<ServiceResponse, ProtocolError> {
        let mut possibly_applied = false;
        self.call_round(action, method, &payload, &mut possibly_applied)
            .await
            .map_err(Into::into)
    }
    async fn converge(
        &self,
        method: Method,
        payload: ChargedBytes,
    ) -> Result<ServiceResponse, ProtocolError> {
        let payload = Some(payload);
        let deadline = Instant::now() + SERVICE_CONVERGENCE;
        let mut delay = Duration::from_millis(50);
        let mut possibly_applied = false;
        loop {
            let attempt = timeout_at(
                deadline,
                self.call_round(self.action, method, &payload, &mut possibly_applied),
            )
            .await
            .map_err(|_| unavailable())?;
            let refused_before_send = match attempt {
                Err(CallError::Transport(RequestError::OutgoingLaneFull)) => {
                    tracing::debug!(group=%self.group.group_id, method=?method, retry_ms=delay.as_millis(), "local service outgoing lane full");
                    true
                }
                Err(CallError::Transport(
                    RequestError::Protocol(error) | RequestError::PeerRejected { error, .. },
                )) if error.code == ErrorCode::Overloaded => {
                    // An outer transport refusal can outlive an applied command.
                    // Retry the same idempotent metadata operation with fresh TLS context.
                    possibly_applied = true;
                    tracing::debug!(group=%self.group.group_id, method=?method, retry_ms=delay.as_millis(), "service transport temporarily overloaded");
                    false
                }
                Err(CallError::Local(error)) if route_unavailable(&error) => {
                    possibly_applied = true;
                    tracing::debug!(group=%self.group.group_id, method=?method, retry_ms=delay.as_millis(), "service routes temporarily unavailable");
                    false
                }
                result => {
                    return result.map_err(|error| {
                        let mut error = ProtocolError::from(error);
                        if possibly_applied && method.purpose() == Purpose::ServiceMutation {
                            error.outcome = Outcome::Unknown;
                        }
                        error
                    });
                }
            };
            // Only the original charged request crosses the wait.
            sleep_until((Instant::now() + delay).min(deadline)).await;
            if Instant::now() >= deadline {
                return Err(if refused_before_send && !possibly_applied {
                    RequestError::OutgoingLaneFull.into()
                } else {
                    unavailable()
                });
            }
            delay = (delay * 2).min(Duration::from_millis(250));
        }
    }
    async fn call_round(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        possibly_applied: &mut bool,
    ) -> Result<ServiceResponse, CallError> {
        if action == ServiceAction::Owner
            && let Some(result) = self.call_hinted(method, payload, possibly_applied).await
        {
            return result;
        }
        tracing::debug!(group=%self.group.group_id, method=?method, "service replica routes start");
        let routes = self.control.replica_routes(&self.group).await.map_err(|error| {
            tracing::debug!(group=%self.group.group_id, method=?method, code=?error.code, outcome=?error.outcome, "service replica routes failed");
            error
        })?;
        tracing::debug!(group=%self.group.group_id, method=?method, replicas=routes.members.len(), "service replica routes ready");
        let preferred = self.leader.load(Ordering::Relaxed);
        // Try the last observed leader, then every currently registered member.
        // The hint never grants authority and every response checks actual TLS.
        for pass in 0..2 {
            for member in routes
                .members
                .iter()
                .filter(|member| (member.node_id == preferred) == (pass == 0))
            {
                match self
                    .call_member(
                        action,
                        method,
                        payload,
                        member.node_id,
                        member.worker_id.into(),
                    )
                    .await
                {
                    Ok(Ok(response)) => {
                        if action == ServiceAction::Owner {
                            routes.record_successful_node(member.node_id);
                        }
                        return Ok(response);
                    }
                    Ok(Err(error)) if route_unavailable(&error) => {
                        *possibly_applied = true;
                        continue;
                    }
                    Ok(Err(error)) => return Err(error.into()),
                    Err(CallError::Transport(error)) => {
                        match &error {
                            RequestError::Protocol(detail)
                            | RequestError::PeerRejected { error: detail, .. } => {
                                tracing::debug!(group=%self.group.group_id, method=?method, node=member.node_id, code=?detail.code, outcome=?detail.outcome, "service peer transport failed");
                                if route_unavailable(detail) {
                                    *possibly_applied = true;
                                    continue;
                                }
                            }
                            RequestError::Unreachable => {
                                *possibly_applied = true;
                                continue;
                            }
                            RequestError::OutgoingLaneFull => {}
                        }
                        return Err(CallError::Transport(error));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        // Local saturation and definitive errors already returned above. On
        // exhaustion the next call must verify the membership pages again.
        self.control.invalidate_replica_routes(&routes);
        Err(unavailable().into())
    }
    async fn call_hinted(
        &self,
        method: Method,
        payload: &Option<ChargedBytes>,
        possibly_applied: &mut bool,
    ) -> Option<Result<ServiceResponse, CallError>> {
        if *possibly_applied
            || matches!(
                method,
                Method::ParticipantScanOpen { .. } | Method::ParticipantScanRun { .. }
            )
        {
            return None;
        }
        let routes = self.control.cached_replica_routes(&self.group)?;
        let preferred = routes
            .last_successful_node()
            .unwrap_or_else(|| self.leader.load(Ordering::Relaxed));
        let member = routes
            .members
            .iter()
            .find(|member| member.node_id == preferred)
            .or_else(|| routes.members.first())?;
        let generation = Some(routes.record.generation);
        let receiver = match routes.hinted_receiver(member.node_id) {
            Some(receiver) => receiver,
            None if routes.begin_receiver_probe() => {
                // Probe using existing read-only methods. An old replica can
                // reject the marker without seeing the application request.
                let (probe, input) = match self.group.kind {
                    GroupKind::Status { .. } => (
                        Method::StatusCall {
                            operation: 0,
                            mutation: false,
                        },
                        Some("Current"),
                    ),
                    GroupKind::Timestamps => (
                        Method::TimestampCall {
                            operation: 0,
                            mutation: false,
                        },
                        Some("Current"),
                    ),
                    GroupKind::Catalog | GroupKind::Data { .. } => {
                        (Method::ParticipantActivation, None)
                    }
                    GroupKind::Control => return None,
                };
                let input = input
                    .map(|input| self.shared.encode_in(ByteClass::Progress, &input, 4096))
                    .transpose()
                    .ok()?;
                let response = self
                    .call_member_at(
                        ServiceAction::Owner,
                        probe,
                        &input,
                        member.node_id,
                        member.worker_id.into(),
                        generation,
                    )
                    .await
                    .ok()?
                    .ok()?;
                let receiver = response.reply.remote_identity();
                routes.confirm_hinted_receiver(member.node_id, receiver);
                receiver
            }
            None => return None,
        };
        tracing::debug!(group=%self.group.group_id, method=?method, node=member.node_id, "service replica route hint");
        let attempt = self
            .call_member_at(
                ServiceAction::Owner,
                method,
                payload,
                member.node_id,
                receiver.into(),
                generation,
            )
            .await;
        let route_failure = match attempt {
            Ok(Ok(response)) => {
                routes.record_successful_node(member.node_id);
                return Some(Ok(response));
            }
            Err(CallError::Transport(RequestError::PeerRejected { worker, error }))
                if worker == receiver && placement_rejected_before_execution(&error) =>
            {
                self.control.invalidate_replica_routes(&routes);
                return None;
            }
            Err(CallError::Local(error)) if error == unsupported_service_route() => return None,
            Err(CallError::Transport(RequestError::Protocol(error)))
                if error
                    == ProtocolError::new(
                        ErrorCode::ValidationError,
                        "target incarnation differs from cached enrollment",
                        Outcome::NotApplied,
                    ) =>
            {
                // The transport rejects an exact incarnation mismatch before dialing.
                self.control.invalidate_replica_routes(&routes);
                return None;
            }
            Err(CallError::Transport(RequestError::Unreachable)) => unavailable(),
            Err(CallError::Transport(RequestError::PeerRejected { worker, error }))
                if worker == receiver =>
            {
                if route_unavailable(&error) {
                    error
                } else {
                    if error == invalid() {
                        self.control.invalidate_replica_routes(&routes);
                    }
                    return Some(Err(CallError::Transport(RequestError::PeerRejected {
                        worker,
                        error,
                    })));
                }
            }
            Ok(Err(error)) | Err(CallError::Local(error)) if route_unavailable(&error) => error,
            Ok(Err(error)) => return Some(Err(error.into())),
            Err(error) => return Some(Err(error)),
        };
        self.control.invalidate_replica_routes(&routes);
        if method.can_repeat_read() {
            // A healthy leader can still serve the read within this route round.
            return None;
        }
        *possibly_applied = true;
        Some(Err(route_failure.into()))
    }
    async fn call_member(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        node: u64,
        worker: RequestTarget,
    ) -> Result<Result<ServiceResponse, ProtocolError>, CallError> {
        self.call_member_at(action, method, payload, node, worker, None)
            .await
    }
    async fn call_member_at(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        node: u64,
        worker: RequestTarget,
        placement_generation: Option<uuid::Uuid>,
    ) -> Result<Result<ServiceResponse, ProtocolError>, CallError> {
        let header = self.shared.encode_in(
            method.class().byte_class(),
            &Header {
                to_node: node,
                action,
                method,
                payload_bytes: payload.as_ref().map_or(0, |bytes| bytes.len() as u64),
                placement_generation,
            },
            4096,
        )?;
        // The hosted replica dispatcher parses this header at a 1024-byte bound.
        if placement_generation.is_some() && header.len() > 1024 {
            return Err(unsupported_service_route().into());
        }
        tracing::debug!(group=%self.group.group_id, method=?method, node, "service peer transport start");
        let reply = self
            .transport
            .request_classified(
                &self.group,
                worker,
                method.purpose(),
                header,
                [payload.clone(), None, None],
                64 << 10,
            )
            .await
            .map_err(CallError::Transport)?;
        tracing::debug!(group=%self.group.group_id, method=?method, node, "service peer transport completed");
        if reply.remote_identity().worker_id != worker.worker_id()
            || reply.group() != &self.group
            || reply.purpose() != method.purpose()
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid().into());
        }
        let raw = reply.header.clone();
        let shared = self.shared.clone();
        let decoded: ReplyHeader = shared
            .execute(
                if method.purpose() == Purpose::Storage {
                    ExecutionClass::Query
                } else {
                    ExecutionClass::Control
                },
                node,
                0,
                move || {
                    crate::protocol::json_input(raw.as_slice(), 4096).map_err(|_| invalid())?;
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())
                },
            )
            .await?;
        if decoded.node != node || decoded.nonce != reply.nonce() {
            return Err(invalid().into());
        }
        if let Some(error) = decoded.error {
            tracing::debug!(group=%self.group.group_id, method=?method, node=node, code=?error.code, outcome=?error.outcome, "service peer returned error");
            return Ok(Err(error));
        }
        let input = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        if input.len() as u64 != decoded.bytes {
            return Err(invalid().into());
        }
        let (digest, bytes) = self
            .shared
            .execute(
                if method.purpose() == Purpose::Storage {
                    ExecutionClass::Query
                } else {
                    ExecutionClass::Control
                },
                node,
                0,
                move || {
                    use sha2::{Digest as _, Sha256};
                    Ok((
                        Digest::from_bytes(Sha256::digest(input.as_slice()).into()),
                        crate::oltp::mvcc::json_decode_budget(input.as_slice())?
                            .checked_add(4096)
                            .ok_or_else(invalid)?,
                    ))
                },
            )
            .await?;
        if digest != decoded.digest {
            return Err(invalid().into());
        }
        tracing::debug!(group=%self.group.group_id, method=?method, node=node, decode_bytes=bytes, "service reply decode reserve start");
        let reply = if method.purpose() == Purpose::Storage {
            reply.reserve_decode(&self.shared, bytes)?
        } else {
            reply.reserve_reply_decode(&self.shared, bytes).await?
        };
        tracing::debug!(group=%self.group.group_id, method=?method, node=node, "service reply decode reserved");
        if !matches!(method, Method::ParticipantScanOpen { .. }) {
            self.leader.store(node, Ordering::Relaxed);
        }
        Ok(Ok(ServiceResponse {
            reply,
            node,
            digest,
            method,
        }))
    }
}
struct ServiceResponse {
    reply: ReceivedReply,
    node: u64,
    digest: Digest,
    method: Method,
}
impl ServiceResponse {
    fn participant_context(
        &self,
        kind: ParticipantProofKind,
    ) -> Result<AuthenticatedParticipantReplyContext, ProtocolError> {
        let matches = matches!(
            (self.method, kind),
            (Method::ParticipantSeal { .. }, ParticipantProofKind::Seal)
                | (
                    Method::ParticipantResolution { .. },
                    ParticipantProofKind::Resolution
                )
                | (Method::ParticipantGuard, ParticipantProofKind::Lifecycle)
                | (Method::ParticipantAcquire, ParticipantProofKind::Lifecycle)
                | (
                    Method::ParticipantIndexClosed { .. },
                    ParticipantProofKind::IndexClosed
                )
        );
        if !matches {
            return Err(invalid());
        }
        AuthenticatedParticipantReplyContext::verified(
            &self.reply,
            self.node,
            kind,
            self.digest,
            self.reply.scratch.clone(),
        )
    }
    fn status_context(&self) -> AuthenticatedStatusReplyContext {
        AuthenticatedStatusReplyContext {
            group: self.reply.group().clone(),
            node: self.node,
            worker: self.reply.remote_identity(),
            nonce: self.reply.nonce(),
            digest: self.digest,
            method: self.method,
            owner: self.reply.scratch.clone(),
        }
    }
}
/// A strong-read response bound to the actual enrolled Status replica and exact
/// bytes. No public worker DTO can construct this proof context.
pub(crate) struct AuthenticatedStatusReplyContext {
    group: GroupIdentity,
    node: u64,
    worker: WorkerIdentity,
    nonce: uuid::Uuid,
    digest: Digest,
    method: Method,
    owner: Arc<ScratchLease>,
}
impl AuthenticatedStatusReplyContext {
    pub(crate) fn verify(
        &self,
        group: uuid::Uuid,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if self.group.group_id != group
            || !matches!(self.group.kind, GroupKind::Status { .. })
            || self.node == 0
            || self.worker.validate().is_err()
            || self.nonce.is_nil()
            || serialized_digest(value)? != self.digest
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn decision(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if self.method != (Method::StatusDecision { txn, manifest }) {
            return Err(invalid());
        }
        self.verify(manifest.status_group_id, value)
    }
    pub(crate) fn settlement(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if self.method != (Method::StatusSettlement { txn, manifest }) {
            return Err(invalid());
        }
        self.verify(manifest.status_group_id, value)
    }
    pub(crate) fn owner(&self) -> Arc<ScratchLease> {
        self.owner.clone()
    }
}
fn parse(
    session: &AuthenticatedSession,
    group: &GroupIdentity,
    node: u64,
    bytes: &[u8],
) -> Result<Header, ProtocolError> {
    if session.recovery_only()
        || session.group() != group
        || !matches!(
            session.purpose(),
            Purpose::Authority | Purpose::Storage | Purpose::Settlement | Purpose::ServiceMutation
        )
    {
        return Err(invalid());
    }
    crate::protocol::json_input(bytes, 4096).map_err(|_| invalid())?;
    let header: Header = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if header.to_node != node
        || session.purpose() != header.method.purpose()
        || (if header.method.input_limit() > 0 {
            header.payload_bytes == 0 || header.payload_bytes > header.method.input_limit() as u64
        } else {
            header.payload_bytes != 0
        })
        || (header.placement_generation.is_some()
            && (header.action != ServiceAction::Owner
                || matches!(
                    header.method,
                    Method::ParticipantScanOpen { .. } | Method::ParticipantScanRun { .. }
                )))
    {
        return Err(invalid());
    }
    Ok(header)
}
async fn authorize(
    control: &ControlClient,
    session: &AuthenticatedSession,
    header: &Header,
) -> Result<(), ProtocolError> {
    let scope = crate::oltp_runtime::replica::QueueScope {
        cluster_id: session.group().cluster_id,
        database: session.group().database.clone(),
    };
    if let Some(generation) = header.placement_generation {
        return control
            .service_role_at_replica(
                &scope,
                session.remote(),
                session.group().clone(),
                header.to_node,
                generation,
            )
            .await;
    }
    control
        .service_role(
            &scope,
            session.remote(),
            header.action,
            session.group().clone(),
        )
        .await?;
    Ok(())
}
async fn reply(
    shared: &Arc<WorkerSharedResources>,
    session: &AuthenticatedSession,
    node: u64,
    result: Result<ChargedBytes, ProtocolError>,
) -> Result<ChargedReply, ProtocolError> {
    let nonce = session.nonce();
    let data = session.purpose() == Purpose::Storage;
    let resources = shared.clone();
    shared
        .execute(
            if data {
                ExecutionClass::Query
            } else {
                ExecutionClass::Control
            },
            node,
            0,
            move || {
                use sha2::{Digest as _, Sha256};
                let (payload, error) = match result {
                    Ok(bytes) => (Some(bytes), None),
                    Err(error) => (None, Some(error)),
                };
                let bytes = payload.as_ref().map_or(0, |bytes| bytes.len() as u64);
                let digest = Digest::from_bytes(
                    Sha256::digest(payload.as_ref().map_or(&[][..], |bytes| bytes.as_slice()))
                        .into(),
                );
                let header = resources.encode_in(
                    if data {
                        ByteClass::Data
                    } else {
                        ByteClass::Progress
                    },
                    &ReplyHeader {
                        node,
                        nonce,
                        bytes,
                        digest,
                        error,
                    },
                    4096,
                )?;
                Ok(ChargedReply {
                    header,
                    sections: [payload, None, None],
                })
            },
        )
        .await
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "no registered group leader completed the strong service call",
        Outcome::Unknown,
    )
}
fn route_unavailable(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown
    ) && error.outcome == Outcome::Unknown
}

fn placement_rejected_before_execution(error: &ProtocolError) -> bool {
    *error == stale_service_route()
        || *error == unsupported_service_route()
        // The permanent node dispatcher has no endpoint to execute the request.
        || (error.code == ErrorCode::ValidationError
            && error.outcome == Outcome::NotApplied
            && error.message == "worker does not host target replica node")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParticipantProofKind {
    Seal,
    Resolution,
    Lifecycle,
    Activation,
    IndexClosed,
}
/// Only the verified strong-read RPC decoder can create this object. Its
/// statement hash prevents reusing an authenticated response with changed DTOs.
pub(crate) struct AuthenticatedParticipantReplyContext {
    group: GroupIdentity,
    node_id: u64,
    worker: WorkerIdentity,
    nonce: uuid::Uuid,
    kind: ParticipantProofKind,
    payload_digest: Digest,
    owner: Arc<ScratchLease>,
}
impl AuthenticatedParticipantReplyContext {
    pub(crate) fn group(&self) -> &GroupIdentity {
        &self.group
    }
    pub(crate) fn owner(&self) -> Arc<ScratchLease> {
        self.owner.clone()
    }
    pub(crate) fn verify_payload(
        &self,
        kind: ParticipantProofKind,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if self.kind != kind
            || self.node_id == 0
            || self.nonce.is_nil()
            || self.worker.validate().is_err()
            || serialized_digest(value)? != self.payload_digest
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn verified(
        reply: &ReceivedReply,
        node_id: u64,
        kind: ParticipantProofKind,
        payload_digest: Digest,
        owner: Arc<ScratchLease>,
    ) -> Result<Self, ProtocolError> {
        if !(reply.purpose() == Purpose::Authority
            || (reply.purpose() == Purpose::Settlement && kind == ParticipantProofKind::Lifecycle))
            || node_id == 0
            || reply.nonce().is_nil()
        {
            return Err(invalid());
        }
        Ok(Self {
            group: reply.group().clone(),
            node_id,
            worker: reply.remote_identity(),
            nonce: reply.nonce(),
            kind,
            payload_digest,
            owner,
        })
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "authenticated participant proof context differs",
        Outcome::NotApplied,
    )
}
