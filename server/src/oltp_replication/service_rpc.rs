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
pub(crate) mod direct;
mod grant_cache;
pub(crate) use grant_cache::GrantCache;
use grant_cache::GrantKey;
mod metadata;
mod participant;
mod participant_full;
#[cfg(test)]
pub(crate) use participant_full::continuation_fixture;
mod read_attempt;
pub(crate) use metadata::{
    AdmissionTicketIssuer, AuthenticatedMetadataReply, BackupPublicationReader,
    OwnerAdmissionObservation, OwnerAdmissionReader, OwnerAdmissionTicket, StatusClient,
    StatusEndpoint, TimestampClient, TimestampEndpoint, VerifiedTimestampPointDecision,
};
#[cfg(test)]
pub(crate) use read_attempt::fixture as read_fixture;
pub(crate) use read_attempt::{
    ReadAttempts, ReadGrant, ReadScope, ScheduledReadError, UnsettledRead,
};
mod status;
pub(crate) use participant::{
    ParticipantProofClient, ParticipantProofEndpoint, VerifiedTimestampPointRejection,
    VerifiedTimestampPointResolution, VerifiedTimestampPointSeal,
};
pub(crate) use participant_full::{ParticipantClient, ParticipantEndpoint};
#[cfg(test)]
mod tests;
pub(crate) use status::{StatusProofClient, StatusProofEndpoint, VerifiedTimestampPointLocator};

use super::{
    authority::control::{
        ControlClient, ReplicaRoutes, ServiceAction, ServiceAuthorization, stale_service_route,
        unsupported_service_route,
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
/// How long a routing-independent call reuses a member list without reading
/// the group record again (plan section 1 bounds cached grants the same way).
const ROUTE_REUSE: Duration = Duration::from_secs(5);
/// PERF2a: while a client served `AllocateDirect` within [`ROUTE_WARM_IDLE`],
/// a background task re-confirms its member list once it is older than half
/// of [`ROUTE_REUSE`], checking every [`ROUTE_REFRESH`]. A serial caller then
/// never pays the Control group read; the staleness bound stays
/// [`ROUTE_REUSE`], and a failed call still drops the list.
const ROUTE_REFRESH: Duration = Duration::from_secs(1);
const ROUTE_WARM_IDLE: Duration = Duration::from_secs(30);
/// PERF2a: an `AllocateDirect` reply header or body at most this long is
/// checked and decoded on the calling task, not through a native-executor
/// hop. Both are a few hundred bytes of JSON; larger replies (a long error
/// message) keep the hop.
pub(super) const INLINE_REPLY_BYTES: usize = 1024;
const RECEIVER_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
const HINTED_READ_TIMEOUT: Duration = Duration::from_secs(1);
const SCOPED_MUTATION_TIMEOUT: Duration = Duration::from_secs(2);
const DIRECT_CATALOG_PEER_TIMEOUT: Duration = Duration::from_millis(500);
const ALLOCATE_DIRECT_PEER_TIMEOUT: Duration = Duration::from_millis(750);
const ALLOCATE_DIRECT_STALLED_PROBE: Duration = Duration::from_millis(200);
pub(super) const SERVICE_HEADER_BYTES: usize = 4096;

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
    TimestampPointDecisionRead {
        route: super::authority::control::TimestampPointRoute,
        source_digest: Digest,
        locator_digest: Digest,
    },
    StatusDecision {
        txn: TxnId,
        manifest: ManifestRef,
    },
    StatusSettlement {
        txn: TxnId,
        manifest: ManifestRef,
    },
    StatusPointLocator {
        route: super::authority::control::TimestampPointRoute,
        txn: TxnId,
        request_digest: Digest,
        manifest: ManifestRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<super::authority::control::ScopedCatalogRead>,
    },
    ParticipantSeal {
        txn: TxnId,
        manifest: ManifestRef,
    },
    ParticipantResolution {
        txn: TxnId,
        manifest: ManifestRef,
    },
    ParticipantTimestampPointSeal {
        route: super::authority::control::TimestampPointRoute,
        locator_digest: Digest,
    },
    ParticipantTimestampPointRejection {
        route: super::authority::control::TimestampPointRoute,
        locator_digest: Digest,
    },
    ParticipantTimestampPointResolution {
        route: super::authority::control::TimestampPointRoute,
        source_digest: Digest,
        locator_digest: Digest,
    },
    ParticipantResolveTimestampPoint,
    ParticipantGuard,
    ParticipantScopedGuard {
        txn: TxnId,
    },
    ParticipantIndexClosed {
        owner: MaintenanceOwnerRef,
    },
    StatusParticipant {
        txn: TxnId,
        manifest: ManifestRef,
        group_id: uuid::Uuid,
    },
    ScopedStatusParticipant {
        scope: super::authority::control::ScopedCatalogRead,
        txn: TxnId,
        manifest: ManifestRef,
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
    ParticipantIndexReclaim,
    ParticipantStoredIndex {
        after: Option<uuid::Uuid>,
    },
    /// A point operation for the range leader (plan section 6). The body is
    /// the first payload section.
    Direct(direct::DirectHeader),
    /// Settle a request id at its anchor before the queued path (addendum
    /// 2026-11-19). The body is the first payload section.
    DirectForward(direct::DirectHeader),
    /// The table's exclusive lifecycle guard on the Catalog, read by a
    /// direct range leader before it activates a Data group (T7b). Sent
    /// only while `data.direct.v1` is active, when every peer decodes it.
    ParticipantExclusive {
        table_id: uuid::Uuid,
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
            | Self::ParticipantResolveTimestampPoint
            | Self::ParticipantIndexReclaim
            | Self::ParticipantPropose { progress: true } => Purpose::Settlement,
            Self::Direct(_) | Self::DirectForward(_) => Purpose::ApiIngress,
            _ => Purpose::Authority,
        }
    }
    fn class(self) -> TrafficClass {
        match self.purpose() {
            Purpose::Storage => TrafficClass::Storage,
            Purpose::Settlement => TrafficClass::Settlement,
            Purpose::ServiceMutation => TrafficClass::ServiceMutation,
            Purpose::ApiIngress => TrafficClass::ApiIngress,
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
    fn uses_bounded_read_failover(self, action: ServiceAction) -> bool {
        self.can_repeat_read()
            && (matches!(
                action,
                ServiceAction::ScopedCatalogRead(_)
                    | ServiceAction::ScopedQueueExecute(_)
                    | ServiceAction::ScopedQueueRead { .. }
                    | ServiceAction::ScopedQueueFenceReceipt { .. }
            ) || matches!(self, Self::ScopedStatusParticipant { .. })
                || self.is_direct_catalog_read(action))
    }
    fn is_direct_catalog_read(self, action: ServiceAction) -> bool {
        action == ServiceAction::DirectLeader
            && matches!(self, Self::ParticipantRead { .. } | Self::ParticipantGuard)
    }
    /// `AllocateDirect` has a fixed-shape reply decoded inline (PERF2a).
    fn is_allocate_direct(self) -> bool {
        matches!(
            self,
            Self::TimestampCall {
                operation: 41,
                mutation: true
            }
        )
    }
    fn peer_timeout(self, action: ServiceAction, stalled: bool) -> Duration {
        if self.is_allocate_direct() {
            // Timestamp replicas can immediately refuse when they are not
            // the leader. Probe a silent old leader briefly between rounds
            // so a freshly elected survivor can answer in the same request.
            if stalled {
                ALLOCATE_DIRECT_STALLED_PROBE
            } else {
                ALLOCATE_DIRECT_PEER_TIMEOUT
            }
        } else if self.is_direct_catalog_read(action) {
            DIRECT_CATALOG_PEER_TIMEOUT
        } else if self.can_repeat_read() {
            HINTED_READ_TIMEOUT
        } else {
            SCOPED_MUTATION_TIMEOUT
        }
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
            Self::ParticipantIndexReclaim => 4096,
            Self::TimestampPointDecisionRead { .. }
            | Self::ParticipantTimestampPointSeal { .. }
            | Self::ParticipantTimestampPointRejection { .. }
            | Self::ParticipantTimestampPointResolution { .. }
            | Self::ParticipantResolveTimestampPoint => 8 << 10,
            Self::Direct(_) | Self::DirectForward(_) => crate::protocol::MAX_BODY_BYTES,
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

#[derive(Clone, Copy, Default)]
struct CallDeadlines {
    peer_failover: Option<Instant>,
    outgoing: Option<Instant>,
}

struct Client {
    group: GroupIdentity,
    shared: Arc<WorkerSharedResources>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
    action: ServiceAction,
    leader: AtomicU64,
    /// The last replica whose bounded attempt timed out (0: none). It is
    /// tried last until it answers again, so a paused leader costs one
    /// attempt deadline, not one per call (D05/LIVE1).
    stalled: AtomicU64,
    /// `AllocateDirect` counters and warm-route state (PERF2a).
    direct: DirectRouting,
    _metadata: Arc<ScratchLease>,
}

/// PERF2a state of the thin `AllocateDirect` path on one [`Client`]. The
/// three counters are read by tests; production sees the `ts.rpc.thin`,
/// `ts.rpc.converge_fallback` and `ts.routes.refresh` stages.
struct DirectRouting {
    /// Calls answered by the single attempt to the cached leader.
    #[cfg_attr(not(test), allow(dead_code))]
    thin: AtomicU64,
    /// Calls that went through [`Client::converge_with`] instead.
    #[cfg_attr(not(test), allow(dead_code))]
    converged: AtomicU64,
    /// Member-list refreshes done by the warm-route task.
    #[cfg_attr(not(test), allow(dead_code))]
    refreshes: AtomicU64,
    /// Milliseconds since `epoch` of the last `AllocateDirect` call.
    used_ms: AtomicU64,
    epoch: std::time::Instant,
    /// Whether the warm-route task runs.
    warming: std::sync::atomic::AtomicBool,
}
impl DirectRouting {
    fn new() -> Self {
        Self {
            thin: AtomicU64::new(0),
            converged: AtomicU64::new(0),
            refreshes: AtomicU64::new(0),
            used_ms: AtomicU64::new(0),
            epoch: std::time::Instant::now(),
            warming: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn now_ms(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
    fn idle(&self) -> bool {
        self.now_ms()
            .saturating_sub(self.used_ms.load(Ordering::Relaxed))
            > ROUTE_WARM_IDLE.as_millis() as u64
    }
}

/// `AllocateDirect` routing counters of one client (tests).
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectRoutingStats {
    pub thin: u64,
    pub converged: u64,
    pub refreshes: u64,
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
            stalled: AtomicU64::new(0),
            direct: DirectRouting::new(),
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
        if method.uses_bounded_read_failover(action) {
            return self.repeat_as(action, method, payload).await;
        }
        self.call_once(action, method, payload, None).await
    }

    async fn call_once(
        &self,
        action: ServiceAction,
        method: Method,
        payload: Option<ChargedBytes>,
        outgoing_deadline: Option<Instant>,
    ) -> Result<ServiceResponse, ProtocolError> {
        let mut possibly_applied = false;
        self.call_round(
            action,
            method,
            &payload,
            &mut possibly_applied,
            CallDeadlines {
                outgoing: outgoing_deadline,
                ..Default::default()
            },
            false,
        )
        .await
        .map_err(|error| {
            let error = ProtocolError::from(error);
            // An earlier replica may have executed the call before this
            // round met a local refusal on the next one (G1).
            if possibly_applied && !method.can_repeat_read() {
                return mutation_uncertain(error);
            }
            error
        })
    }
    async fn converge(
        &self,
        method: Method,
        payload: ChargedBytes,
    ) -> Result<ServiceResponse, ProtocolError> {
        self.converge_as(self.action, method, Some(payload), false)
            .await
    }

    /// Call only for a repeatable read or an exact, idempotent command.
    async fn repeat_as(
        &self,
        action: ServiceAction,
        method: Method,
        payload: Option<ChargedBytes>,
    ) -> Result<ServiceResponse, ProtocolError> {
        self.converge_as(action, method, payload, true).await
    }

    /// `AllocateDirect` in one transport round trip (PERF2a). Its result
    /// does not depend on which replica answers, only on the receiver's own
    /// checks: only the Timestamp leader answers, after its lease barrier.
    /// So it makes a single bounded attempt to the cached leader on a member
    /// list a strong Control read confirmed within [`ROUTE_REUSE`] (PERF1),
    /// with no route read and no native hop on this side; a stale route can
    /// make the call fail but never answer it wrongly. A route failure
    /// forgets the leader and drops the member list. Any failure then
    /// converges as [`Self::repeat_as`] does, reusing confirmed routes and
    /// within the same deadline, trying a leader that timed out last
    /// (D05/LIVE1).
    pub(super) async fn allocate_direct_as(
        self: &Arc<Self>,
        action: ServiceAction,
        method: Method,
        payload: ChargedBytes,
    ) -> Result<ServiceResponse, ProtocolError> {
        use crate::oltp_runtime::stage_timing::{perf_stage, timed};
        let deadline = Instant::now() + SERVICE_CONVERGENCE;
        self.direct
            .used_ms
            .store(self.direct.now_ms(), Ordering::Relaxed);
        self.keep_routes_warm();
        let payload = Some(payload);
        if let Some(response) = timed(
            perf_stage!("ts.rpc.thin"),
            self.cached_leader_attempt(action, method, &payload, deadline),
        )
        .await
        {
            self.direct.thin.fetch_add(1, Ordering::Relaxed);
            return Ok(response);
        }
        self.direct.converged.fetch_add(1, Ordering::Relaxed);
        timed(
            perf_stage!("ts.rpc.converge_fallback"),
            self.converge_with(action, method, payload, true, true, deadline),
        )
        .await
    }

    async fn cached_leader_attempt(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        deadline: Instant,
    ) -> Option<ServiceResponse> {
        let routes = self
            .control
            .recent_replica_routes(&self.group, ROUTE_REUSE)?;
        let node = match self.leader.load(Ordering::Relaxed) {
            0 => routes.last_successful_node()?,
            node => node,
        };
        // A leader whose last attempt timed out is tried last, by the
        // converging path, until it answers again (D05/LIVE1).
        if node == self.stalled.load(Ordering::Relaxed) {
            return None;
        }
        let member = routes
            .members
            .iter()
            .find(|member| member.node_id == node)?;
        let attempt = timeout_at(
            deadline.min(Instant::now() + method.peer_timeout(action, false)),
            self.call_member(action, method, payload, node, member.worker_id.into()),
        )
        .await;
        let route_failed = match attempt {
            Ok(Ok(Ok(response))) => {
                routes.record_successful_node(node);
                return Some(response);
            }
            Err(_) => {
                self.stalled.store(node, Ordering::Relaxed);
                true
            }
            Ok(Ok(Err(error))) => route_unavailable(&error),
            Ok(Err(CallError::Transport(
                RequestError::Protocol(error) | RequestError::PeerRejected { error, .. },
            ))) => route_unavailable(&error) || error == invalid(),
            Ok(Err(CallError::Transport(RequestError::Unreachable))) => true,
            Ok(Err(CallError::Transport(RequestError::OutgoingLaneFull { .. }))) => false,
            // The reply failed this side's checks.
            Ok(Err(CallError::Local(_))) => true,
        };
        crate::oltp_runtime::stage_timing::perf_stage!("ts.rpc.thin_failed").record(Duration::ZERO);
        if route_failed {
            let _ = self
                .leader
                .compare_exchange(node, 0, Ordering::Relaxed, Ordering::Relaxed);
            self.control.invalidate_replica_routes(&routes);
        }
        None
    }

    /// Starts the warm-route task unless it runs (PERF2a). It holds the
    /// client weakly and stops after [`ROUTE_WARM_IDLE`] without a call.
    fn keep_routes_warm(self: &Arc<Self>) {
        if self.direct.warming.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.direct.warming.store(false, Ordering::Release);
            return;
        };
        let client = Arc::downgrade(self);
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(ROUTE_REFRESH).await;
                let Some(client) = client.upgrade() else {
                    return;
                };
                if client.direct.idle() {
                    client.direct.warming.store(false, Ordering::Release);
                    // A call that saw `warming` set just before the store
                    // started no task: take over if it is recent.
                    if client.direct.idle()
                        || client.direct.warming.swap(true, Ordering::AcqRel)
                    {
                        return;
                    }
                }
                if client
                    .control
                    .recent_replica_routes(&client.group, ROUTE_REUSE / 2)
                    .is_some()
                {
                    continue;
                }
                let refreshed = crate::oltp_runtime::stage_timing::timed(
                    crate::oltp_runtime::stage_timing::perf_stage!("ts.routes.refresh"),
                    tokio::time::timeout(
                        SERVICE_CONVERGENCE,
                        client.control.replica_routes(&client.group),
                    ),
                )
                .await;
                if let Ok(Ok(_)) = refreshed {
                    client.direct.refreshes.fetch_add(1, Ordering::Relaxed);
                } else {
                    tracing::debug!(group=%client.group.group_id, "AllocateDirect route refresh failed");
                }
            }
        });
    }

    #[cfg(test)]
    fn direct_routing_stats(&self) -> DirectRoutingStats {
        DirectRoutingStats {
            thin: self.direct.thin.load(Ordering::Relaxed),
            converged: self.direct.converged.load(Ordering::Relaxed),
            refreshes: self.direct.refreshes.load(Ordering::Relaxed),
        }
    }

    async fn converge_as(
        &self,
        action: ServiceAction,
        method: Method,
        payload: Option<ChargedBytes>,
        bound_mutation_attempt: bool,
    ) -> Result<ServiceResponse, ProtocolError> {
        let deadline = Instant::now() + SERVICE_CONVERGENCE;
        self.converge_with(
            action,
            method,
            payload,
            bound_mutation_attempt,
            false,
            deadline,
        )
        .await
    }

    async fn converge_with(
        &self,
        action: ServiceAction,
        method: Method,
        payload: Option<ChargedBytes>,
        bound_mutation_attempt: bool,
        reuse_routes: bool,
        deadline: Instant,
    ) -> Result<ServiceResponse, ProtocolError> {
        let peer_deadline =
            (bound_mutation_attempt || method.can_repeat_read()).then_some(deadline);
        let mut delay = Duration::from_millis(50);
        let mut possibly_applied = false;
        loop {
            let attempt = timeout_at(
                deadline,
                self.call_round(
                    action,
                    method,
                    &payload,
                    &mut possibly_applied,
                    CallDeadlines {
                        peer_failover: peer_deadline,
                        ..Default::default()
                    },
                    reuse_routes,
                ),
            )
            .await
            .map_err(|_| unavailable())?;
            let refused_before_send = match attempt {
                Err(CallError::Transport(error @ RequestError::OutgoingLaneFull { .. })) => {
                    tracing::debug!(group=%self.group.group_id, method=?method, ?error, retry_ms=delay.as_millis(), "local service outgoing lane full");
                    Some(error)
                }
                Err(CallError::Transport(
                    RequestError::Protocol(error) | RequestError::PeerRejected { error, .. },
                )) if error.code == ErrorCode::Overloaded => {
                    // An outer transport refusal can outlive an applied command.
                    // Retry the same idempotent operation with fresh TLS context.
                    possibly_applied = true;
                    tracing::debug!(group=%self.group.group_id, method=?method, retry_ms=delay.as_millis(), "service transport temporarily overloaded");
                    None
                }
                Err(CallError::Local(error)) if route_unavailable(&error) => {
                    possibly_applied = true;
                    tracing::debug!(group=%self.group.group_id, method=?method, retry_ms=delay.as_millis(), "service routes temporarily unavailable");
                    None
                }
                result => {
                    return result.map_err(|error| {
                        let error = ProtocolError::from(error);
                        if possibly_applied
                            && (method.purpose() == Purpose::ServiceMutation
                                || bound_mutation_attempt && !method.can_repeat_read())
                        {
                            return mutation_uncertain(error);
                        }
                        error
                    });
                }
            };
            // Only the original charged request crosses the wait.
            sleep_until((Instant::now() + delay).min(deadline)).await;
            if Instant::now() >= deadline {
                return Err(match refused_before_send {
                    Some(error) if !possibly_applied => error.into(),
                    _ => unavailable(),
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
        deadlines: CallDeadlines,
        reuse_routes: bool,
    ) -> Result<ServiceResponse, CallError> {
        let mut failed_read_hint = None;
        if action == ServiceAction::Owner
            && let Some(result) = self
                .call_hinted(
                    method,
                    payload,
                    possibly_applied,
                    &mut failed_read_hint,
                    deadlines.outgoing,
                )
                .await
        {
            return result;
        }
        if reuse_routes
            && let Some(routes) = self.control.recent_replica_routes(&self.group, ROUTE_REUSE)
        {
            let result = self
                .call_routed(
                    action,
                    method,
                    payload,
                    possibly_applied,
                    &routes,
                    None,
                    deadlines,
                )
                .await;
            if result.is_ok() {
                return result;
            }
            // The members may have changed: read them again below.
            self.control.invalidate_replica_routes(&routes);
        }
        tracing::debug!(group=%self.group.group_id, method=?method, "service replica routes start");
        let routes = self.control.replica_routes(&self.group).await.map_err(|error| {
            tracing::debug!(group=%self.group.group_id, method=?method, code=?error.code, outcome=?error.outcome, "service replica routes failed");
            error
        })?;
        tracing::debug!(group=%self.group.group_id, method=?method, replicas=routes.members.len(), "service replica routes ready");
        let retry_last = failed_read_hint
            .filter(|(generation, _)| *generation == routes.record.generation)
            .map(|(_, node)| node);
        self.call_routed(
            action,
            method,
            payload,
            possibly_applied,
            &routes,
            retry_last,
            deadlines,
        )
        .await
    }

    async fn call_routed(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        possibly_applied: &mut bool,
        routes: &ReplicaRoutes,
        retry_last: Option<u64>,
        deadlines: CallDeadlines,
    ) -> Result<ServiceResponse, CallError> {
        let stalled = self.stalled.load(Ordering::Relaxed);
        let preferred = match self.leader.load(Ordering::Relaxed) {
            0 if deadlines.peer_failover.is_some() => routes.last_successful_node().unwrap_or(0),
            node => node,
        };
        let mut forwarded = None;
        // Prefer the observed leader, but let other replicas answer before
        // spending another transport deadline on a failed cached receiver.
        for pass in 0..3 {
            for member in routes.members.iter().filter(|member| {
                let priority = if Some(member.node_id) == retry_last
                    || (stalled != 0 && member.node_id == stalled)
                {
                    2
                } else {
                    u8::from(member.node_id != preferred)
                };
                priority == pass
            }) {
                let call = self.call_member_at(
                    action,
                    method,
                    payload,
                    member.node_id,
                    member.worker_id.into(),
                    None,
                    deadlines.outgoing,
                );
                let reply = if let Some(deadline) = deadlines.peer_failover {
                    // Leave time to reach the next leader when one peer stalls.
                    let attempt = method.peer_timeout(action, member.node_id == stalled);
                    let peer_deadline = deadline.min(Instant::now() + attempt);
                    timeout_at(peer_deadline, call).await.unwrap_or_else(|_| {
                        self.stalled.store(member.node_id, Ordering::Relaxed);
                        Err(CallError::Transport(RequestError::Unreachable))
                    })
                } else {
                    call.await
                };
                match reply {
                    Ok(Ok(response)) => {
                        let _ = self.stalled.compare_exchange(
                            member.node_id,
                            0,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                        if action == ServiceAction::Owner || deadlines.peer_failover.is_some() {
                            routes.record_successful_node(member.node_id);
                        }
                        return Ok(response);
                    }
                    Ok(Err(error)) if error == super::hosted::proposal_forwarded() => {
                        forwarded = Some(error);
                    }
                    Ok(Err(error)) if route_unavailable(&error) => {
                        *possibly_applied = true;
                        continue;
                    }
                    Ok(Err(error)) => return Err(error.into()),
                    Err(CallError::Transport(RequestError::PeerRejected { worker, error }))
                        if matches!(method, Method::ParticipantFence { .. })
                            && worker.worker_id == member.worker_id
                            && placement_rejected_before_execution(&error) =>
                    {
                        // Control retains the placement while replicas reopen.
                        // This peer had no route for the fence, so try the
                        // other verified voters before refreshing membership.
                        continue;
                    }
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
                            RequestError::OutgoingLaneFull { .. } => {}
                        }
                        return Err(CallError::Transport(error));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        // Local saturation and definitive errors already returned above. On
        // exhaustion the next call must verify the membership pages again.
        self.control.invalidate_replica_routes(routes);
        if !*possibly_applied && let Some(error) = forwarded {
            return Err(error.into());
        }
        Err(unavailable().into())
    }

    async fn call_timestamp_point_proof(
        &self,
        method: Method,
        payload: Option<ChargedBytes>,
        source: &ReplicaRoutes,
        target: &ReplicaRoutes,
    ) -> Result<ServiceResponse, ProtocolError> {
        let mut completed = false;
        let deadline = Instant::now() + SERVICE_CONVERGENCE;
        timeout_at(
            deadline,
            self.call_routed(
                self.action,
                method,
                &payload,
                &mut completed,
                target,
                None,
                CallDeadlines {
                    peer_failover: Some(deadline),
                    ..Default::default()
                },
            ),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|error| {
            let error = ProtocolError::from(error);
            if placement_rejected_before_execution(&error) {
                self.control.invalidate_replica_routes(source);
                self.control.invalidate_replica_routes(target);
                stale_service_route()
            } else {
                error
            }
        })
    }
    async fn call_hinted(
        &self,
        method: Method,
        payload: &Option<ChargedBytes>,
        possibly_applied: &mut bool,
        failed_read_hint: &mut Option<(uuid::Uuid, u64)>,
        outgoing_deadline: Option<Instant>,
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
                let response = tokio::time::timeout(
                    RECEIVER_PROBE_TIMEOUT,
                    self.call_member_at(
                        ServiceAction::Owner,
                        probe,
                        &input,
                        member.node_id,
                        member.worker_id.into(),
                        generation,
                        None,
                    ),
                )
                .await
                .ok()?
                .ok()?
                .ok()?;
                let receiver = response.reply.remote_identity();
                routes.confirm_hinted_receiver(member.node_id, receiver);
                receiver
            }
            None => return None,
        };
        tracing::debug!(group=%self.group.group_id, method=?method, node=member.node_id, "service replica route hint");
        let call = self.call_member_at(
            ServiceAction::Owner,
            method,
            payload,
            member.node_id,
            receiver.into(),
            generation,
            outgoing_deadline,
        );
        let attempt = if method.can_repeat_read() {
            match tokio::time::timeout(HINTED_READ_TIMEOUT, call).await {
                Ok(result) => result,
                Err(_) => Err(CallError::Transport(RequestError::Unreachable)),
            }
        } else {
            call.await
        };
        let route_failure = match attempt {
            Ok(Err(error)) if error == super::hosted::proposal_forwarded() => {
                self.control.invalidate_replica_routes(&routes);
                return None;
            }
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
            *failed_read_hint = Some((routes.record.generation, member.node_id));
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
        self.call_member_at(action, method, payload, node, worker, None, None)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn call_member_at(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        node: u64,
        worker: RequestTarget,
        placement_generation: Option<uuid::Uuid>,
        outgoing_deadline: Option<Instant>,
    ) -> Result<Result<ServiceResponse, ProtocolError>, CallError> {
        let result = self
            .call_member_reserved(
                action,
                method,
                payload,
                node,
                worker,
                placement_generation,
                None,
            )
            .await;
        let (until, refusal) = match (outgoing_deadline, result) {
            (
                Some(until),
                Err(error @ CallError::Transport(RequestError::OutgoingLaneFull { .. })),
            ) => (until, error),
            (_, result) => return result,
        };
        // Only a local pre-send refusal can wait and send the same command.
        // Keep the original charged payload and the round's G1 tracking.
        let reservation = self
            .transport
            .reserve_outgoing(&self.group, worker.worker_id(), method.purpose(), until)
            .await
            .map_err(CallError::Transport)?;
        if Instant::now() >= until {
            return Err(refusal);
        }
        self.call_member_reserved(
            action,
            method,
            payload,
            node,
            worker,
            placement_generation,
            Some(reservation),
        )
        .await
    }

    /// `call_member_at` on an outgoing slot the caller already holds
    /// ([`WorkerClient::reserve_outgoing`]); `None` takes one or refuses.
    #[allow(clippy::too_many_arguments)]
    async fn call_member_reserved(
        &self,
        action: ServiceAction,
        method: Method,
        payload: &Option<ChargedBytes>,
        node: u64,
        worker: RequestTarget,
        placement_generation: Option<uuid::Uuid>,
        reservation: Option<crate::oltp_replication::multiplex::OutgoingReservation>,
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
            SERVICE_HEADER_BYTES,
        )?;
        // Older replicas use a 1024-byte routing header for owner hints.
        if placement_generation.is_some() && header.len() > 1024 {
            return Err(unsupported_service_route().into());
        }
        tracing::debug!(group=%self.group.group_id, method=?method, node, "service peer transport start");
        let _timer = crate::oltp_runtime::stage_timing::variant("rpc.call.", &method).timer();
        let sections = [payload.clone(), None, None];
        // One boxed transport future, whichever lane path: every service
        // call nests this future, and holding both variants inline grew
        // the callers' futures until debug builds overflowed a Tokio
        // worker's stack (run 44895705548, D05 and quorum workers).
        let transport: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ReceivedReply, RequestError>> + Send + '_>,
        > = match reservation {
            None => Box::pin(self.transport.request_classified(
                &self.group,
                worker,
                method.purpose(),
                header,
                sections,
                64 << 10,
            )),
            Some(reservation) => Box::pin(self.transport.request_reserved(
                reservation,
                &self.group,
                worker,
                method.purpose(),
                header,
                sections,
                64 << 10,
            )),
        };
        let reply = transport.await.map_err(|error| {
                tracing::debug!(group=%self.group.group_id, method=?method, node, error=?error, "service peer transport refused or lost");
                CallError::Transport(error)
            })?;
        tracing::debug!(group=%self.group.group_id, method=?method, node, "service peer transport completed");
        self.member_reply(method, node, worker, reply)
            .await
            .map_err(|error| {
                // The peer answered, so it may have executed the call before
                // this side failed to check or charge the reply (G1).
                CallError::Local(if method.can_repeat_read() {
                    error
                } else {
                    mutation_uncertain(error)
                })
            })
    }

    async fn member_reply(
        &self,
        method: Method,
        node: u64,
        worker: RequestTarget,
        reply: ReceivedReply,
    ) -> Result<Result<ServiceResponse, ProtocolError>, ProtocolError> {
        if reply.remote_identity().worker_id != worker.worker_id()
            || reply.group() != &self.group
            || reply.purpose() != method.purpose()
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid());
        }
        let raw = reply.header.clone();
        let class = if method.purpose() == Purpose::Storage {
            ExecutionClass::Query
        } else {
            ExecutionClass::Control
        };
        // PERF2b: a small direct reply (a point operation's answer) is
        // checked and decoded on this task, not in two native jobs.
        // PERF2a: so is a small fixed-shape reply (`AllocateDirect`).
        let inline = (matches!(method, Method::Direct(_) | Method::DirectForward(_))
            && reply.sections[0]
                .as_ref()
                .is_none_or(|section| section.len() <= DIRECT_INLINE_REPLY_BYTES))
            || (method.is_allocate_direct()
                && raw.len() <= INLINE_REPLY_BYTES
                && reply.sections[0]
                    .as_ref()
                    .is_none_or(|section| section.len() <= INLINE_REPLY_BYTES));
        let decoded: ReplyHeader = if inline {
            decode_reply_header(raw.as_slice())?
        } else {
            self.shared
                .execute(class, node, 0, move || decode_reply_header(raw.as_slice()))
                .await?
        };
        if decoded.node != node || decoded.nonce != reply.nonce() {
            return Err(invalid());
        }
        if let Some(error) = decoded.error {
            tracing::debug!(group=%self.group.group_id, method=?method, node=node, code=?error.code, outcome=?error.outcome, "service peer returned error");
            return Ok(Err(error));
        }
        let input = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        if input.len() as u64 != decoded.bytes {
            return Err(invalid());
        }
        let (digest, bytes) = if inline {
            reply_digest_and_budget(input.as_slice())?
        } else {
            self.shared
                .execute(class, node, 0, move || {
                    reply_digest_and_budget(input.as_slice())
                })
                .await?
        };
        if digest != decoded.digest {
            return Err(invalid());
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
/// A direct reply section up to this size is framed inline at the leader
/// and checked and decoded inline at the gateway (PERF2b); larger ones keep
/// their native jobs.
const DIRECT_INLINE_REPLY_BYTES: usize = 4 << 10;
fn decode_reply_header(raw: &[u8]) -> Result<ReplyHeader, ProtocolError> {
    crate::protocol::json_input(raw, 4096).map_err(|_| invalid())?;
    serde_json::from_slice(raw).map_err(|_| invalid())
}
fn reply_digest_and_budget(input: &[u8]) -> Result<(Digest, usize), ProtocolError> {
    use sha2::{Digest as _, Sha256};
    Ok((
        Digest::from_bytes(Sha256::digest(input).into()),
        crate::oltp::mvcc::json_decode_budget(input)?
            .checked_add(4096)
            .ok_or_else(invalid)?,
    ))
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
                | (
                    Method::ParticipantGuard | Method::ParticipantScopedGuard { .. },
                    ParticipantProofKind::Lifecycle
                )
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
            Purpose::Authority
                | Purpose::Storage
                | Purpose::Settlement
                | Purpose::ServiceMutation
                | Purpose::ApiIngress
        )
    {
        return Err(invalid());
    }
    crate::protocol::json_input(bytes, SERVICE_HEADER_BYTES).map_err(|_| invalid())?;
    let header: Header = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    // PERF1: service calls this process serves, by method.
    crate::oltp_runtime::stage_timing::variant("rpc.serve.", &header.method)
        .record(std::time::Duration::ZERO);
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
fn scoped_admission_read(scope: super::authority::control::ScopedCatalogRead) -> bool {
    // A fresh scoped admission captures Catalog lineage before there is an
    // accepted context to hash. Only the queue owner gets this read privilege.
    scope.request.accepted_context == Digest::from_bytes([0; 32])
}
/// The caller's full grant, always read fresh from Control: callers compare
/// its owner and authority with the request's fences. The passed role check
/// warms the receiver's cache for [`authorize`]. See `grant_cache`.
async fn service_grant(
    control: &ControlClient,
    session: &AuthenticatedSession,
    header: &Header,
) -> Result<ServiceAuthorization, ProtocolError> {
    let key = GrantKey::new(session.remote(), header.action, session.group(), None);
    let scope = crate::oltp_runtime::replica::QueueScope {
        cluster_id: session.group().cluster_id,
        database: session.group().database.clone(),
    };
    control
        .service_grants()
        .grant(key, control.applied_revision(), || {
            control.service_role(
                &scope,
                session.remote(),
                header.action,
                session.group().clone(),
            )
        })
        .await
}

/// The caller's role only. A passed check is cached per caller incarnation
/// and role while the locally applied Control revision is unchanged and the
/// Control read started under 5 s ago.
async fn authorize(
    control: &Arc<ControlClient>,
    session: &AuthenticatedSession,
    header: &Header,
) -> Result<(), ProtocolError> {
    let scope = crate::oltp_runtime::replica::QueueScope {
        cluster_id: session.group().cluster_id,
        database: session.group().database.clone(),
    };
    let revision = control.applied_revision();
    if let Some(generation) = header.placement_generation {
        let key = GrantKey::new(
            session.remote(),
            header.action,
            session.group(),
            Some((header.to_node, generation)),
        );
        return control
            .service_grants()
            .role(key, revision, || async {
                control
                    .service_role_at_replica(
                        &scope,
                        session.remote(),
                        session.group().clone(),
                        header.to_node,
                        generation,
                    )
                    .await
                    .map(|_| None)
            })
            .await;
    }
    let key = GrantKey::new(session.remote(), header.action, session.group(), None);
    let read = || async {
        control
            .service_role(
                &scope,
                session.remote(),
                header.action,
                session.group().clone(),
            )
            .await
            .map(Some)
    };
    // RD2: a range leader's calls on the direct path (`AllocateDirect` at the
    // Timestamp leader, Catalog reads) keep their role warm, so an idle
    // leader's next commit does not wait for a Control read.
    if header.action == ServiceAction::DirectLeader {
        let refresh = || {
            warm_role(
                control,
                scope.clone(),
                session.remote(),
                header.action,
                session.group().clone(),
            )
        };
        return control
            .service_grants()
            .role_warm(key, revision, read, refresh)
            .await;
    }
    control.service_grants().role(key, revision, read).await
}

/// The background refresh of one warm role check (RD2, `grant_cache`).
pub(super) fn warm_role(
    control: &Arc<ControlClient>,
    scope: crate::oltp_runtime::replica::QueueScope,
    remote: WorkerIdentity,
    action: ServiceAction,
    target: GroupIdentity,
) -> grant_cache::Refresh {
    let control = Arc::downgrade(control);
    Arc::new(move || {
        let control = control.upgrade()?;
        let revision = control.applied_revision();
        let (scope, target) = (scope.clone(), target.clone());
        Some((
            revision,
            Box::pin(async move {
                control
                    .service_role(&scope, remote, action, target)
                    .await
                    .map(Some)
            }),
        ))
    })
}
/// `warm_role` for a role whose target moved (AZ2): asks for `target` and, when
/// a previous Control build refuses it as `Unauthorized`, for `fallback`, as
/// the request path does.
pub(super) fn warm_role_or(
    control: &Arc<ControlClient>,
    scope: crate::oltp_runtime::replica::QueueScope,
    remote: WorkerIdentity,
    action: ServiceAction,
    target: GroupIdentity,
    fallback: GroupIdentity,
) -> grant_cache::Refresh {
    let control = Arc::downgrade(control);
    Arc::new(move || {
        let control = control.upgrade()?;
        let revision = control.applied_revision();
        let (scope, target, fallback) = (scope.clone(), target.clone(), fallback.clone());
        Some((
            revision,
            Box::pin(async move {
                match control.service_role(&scope, remote, action, target).await {
                    Err(error) if error.code == ErrorCode::Unauthorized => control
                        .service_role(&scope, remote, action, fallback)
                        .await
                        .map(Some),
                    grant => grant.map(Some),
                }
            }),
        ))
    })
}
async fn reply(
    shared: &Arc<WorkerSharedResources>,
    session: &AuthenticatedSession,
    node: u64,
    result: Result<ChargedBytes, ProtocolError>,
) -> Result<ChargedReply, ProtocolError> {
    let nonce = session.nonce();
    let data = session.purpose() == Purpose::Storage;
    // PERF2b: a small direct answer (only `Method::Direct` and
    // `DirectForward` use the ingress purpose) is framed on this task.
    let inline = session.purpose() == Purpose::ApiIngress
        && !matches!(&result, Ok(bytes) if bytes.len() > DIRECT_INLINE_REPLY_BYTES);
    if inline {
        return build_reply(shared, data, nonce, node, result);
    }
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
            move || build_reply(&resources, data, nonce, node, result),
        )
        .await
}
/// [`reply`] on the calling task, for a small fixed-shape `AllocateDirect`
/// answer or its error (PERF2a): one SHA-256 of at most
/// [`INLINE_REPLY_BYTES`] and a header of a few hundred bytes.
fn reply_inline(
    shared: &Arc<WorkerSharedResources>,
    session: &AuthenticatedSession,
    node: u64,
    result: Result<ChargedBytes, ProtocolError>,
) -> Result<ChargedReply, ProtocolError> {
    let data = session.purpose() == Purpose::Storage;
    build_reply(shared, data, session.nonce(), node, result)
}
fn build_reply(
    resources: &Arc<WorkerSharedResources>,
    data: bool,
    nonce: uuid::Uuid,
    node: u64,
    result: Result<ChargedBytes, ProtocolError>,
) -> Result<ChargedReply, ProtocolError> {
    use sha2::{Digest as _, Sha256};
    let (payload, error) = match result {
        Ok(bytes) => (Some(bytes), None),
        Err(error) => (None, Some(error)),
    };
    let bytes = payload.as_ref().map_or(0, |bytes| bytes.len() as u64);
    let digest = Digest::from_bytes(
        Sha256::digest(payload.as_ref().map_or(&[][..], |bytes| bytes.as_slice())).into(),
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

fn mutation_uncertain(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
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
