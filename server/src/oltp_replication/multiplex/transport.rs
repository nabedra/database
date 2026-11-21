use super::*;
use crate::oltp::pool::{GroupCapacityLease, RAFT_REPLICA_BYTES, WorkerSharedResources};
use crate::oltp_replication::{
    TlsMaterial,
    types::{Digest, GroupKind},
};
use rustls::pki_types::ServerName;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit},
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

mod resolve;
mod session;
use resolve::{PeerResolver, ResolveError};
pub(crate) use session::PeerFeature;
use session::{INDEX_RECLAIM_ALPN, PeerSessions, RawReply, SESSION_ALPN, session_alpn};
#[cfg(test)]
mod tests;

// Engine capability is fixed in the authenticated protocol, before variable
// metadata. An OLAP worker does not implement this listener or enrollment.
const ALPN: &[u8] = b"nabedra-oltp-worker/1";
const MAGIC: &[u8; 8] = b"NABOTP01";
const PROLOGUE_BYTES: usize = 173;
const CLASSES: usize = 15;

/// Applied enrollment authenticates only transport identity. Application
/// endpoints still require fresh authority in preflight. Control Raft trust
/// never performs a quorum-dependent lookup through its own transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTrust {
    AppliedEnrollment,
    RecoveryControlRaft,
    BootstrapControl,
    PreparedControl,
}
impl TransportTrust {
    fn recovery_only(self) -> bool {
        matches!(self, Self::RecoveryControlRaft | Self::PreparedControl)
    }
    fn validate_prepared(
        self,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<(), ProtocolError> {
        if self == Self::PreparedControl
            && (group.kind != GroupKind::Control || purpose != Purpose::ControlDelegation)
        {
            return Err(malformed("prepared identity permits only Control recovery"));
        }
        Ok(())
    }
}
/// Trust checks shared by the one-shot listener, sessions and callers.
fn admit_trust(
    trust: TransportTrust,
    group: &GroupIdentity,
    purpose: Purpose,
) -> Result<(), ProtocolError> {
    trust.validate_prepared(group, purpose)?;
    if trust == TransportTrust::BootstrapControl
        && (group.kind != GroupKind::Control
            || !matches!(
                purpose,
                Purpose::Raft | Purpose::Authority | Purpose::ControlDelegation | Purpose::SystemRecovery
            ))
    {
        return Err(malformed(
            "bootstrap trust permits only Control consensus and enrollment reads",
        ));
    }
    if trust == TransportTrust::RecoveryControlRaft
        && !(group.kind == GroupKind::Control && purpose == Purpose::Raft)
    {
        return Err(malformed("recovery trust permits only Control Raft"));
    }
    Ok(())
}
pub trait PeerDirectory: Send + Sync + 'static {
    /// Read local applied enrollment or explicit recovery policy only. This
    /// synchronous interface must not perform network I/O or await Control.
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError>;
    fn resolve(&self, worker: Uuid)
    -> Result<super::super::types::WorkerEnrollment, ProtocolError>;
    /// The locally applied Control enrollment revision, when this directory
    /// follows Control. Receivers drop cached service grants when it changes.
    fn applied_revision(&self) -> Option<u64> {
        None
    }
    /// Revocations are checked again after asynchronous endpoint preflight.
    fn revoked(&self, _peer: WorkerIdentity) -> bool {
        false
    }
    /// Whether `peer` is an enrolled API gateway (not a worker).
    fn is_api(&self, _peer: WorkerIdentity) -> bool {
        false
    }
}

fn revoked_peer() -> ProtocolError {
    ProtocolError::new(ErrorCode::Unauthorized, "worker certificate or identity was revoked", Outcome::NotApplied)
}

/// Whether this worker is the hello's audience. Every audience names this
/// incarnation exactly, except one: an enrolled API gateway that has not yet
/// learned this worker's newer incarnation may still reach Control authority
/// here. After every worker cold-starts onto a new run with its certificate,
/// the gateway can learn the new enrollments only from Control (G6). TLS has
/// proved the pinned certificate and the sender's API enrollment; Control's
/// reads and commands do not depend on the incarnation the sender assumed.
fn audience(
    directory: &dyn PeerDirectory,
    hello: &Hello,
    group: &GroupIdentity,
    local: &WorkerIdentity,
) -> bool {
    hello.to == *local
        || (hello.to.worker_id == local.worker_id
            && hello.to.certificate_sha256 == local.certificate_sha256
            && hello.to.enrollment_revision < local.enrollment_revision
            && hello.purpose == Purpose::Authority
            && group.kind == GroupKind::Control
            && directory.is_api(hello.from))
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub handshake_slots: usize,
    pub preflight_slots: [usize; CLASSES],
    pub handler_slots: [usize; CLASSES],
    pub payload_bytes: [usize; CLASSES],
    pub decode_bytes: [usize; CLASSES],
    pub deadline: Duration,
    /// Accepted sessions per traffic class. Each lane has its own pool, so
    /// sessions on one class cannot take Control's or Raft progress's (C07).
    pub session_slots: [usize; CLASSES],
    /// TLS sessions accepted before their first verified request names a
    /// lane. When none is free the listener offers only the one-shot ALPN.
    pub pending_sessions: usize,
    /// A caller pings an idle session after this long without receiving a
    /// byte. A session that never names a lane is closed after this long.
    pub ping_idle: Duration,
    /// A caller with a stream waiting pings after this long without
    /// receiving a byte, and closes the session when nothing arrives within
    /// this long of the ping reaching the socket.
    pub ping_timeout: Duration,
    /// Bound on one TCP connect plus TLS handshake for a lane.
    pub connect_timeout: Duration,
}
impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            handshake_slots: 16,
            preflight_slots: [4, 4, 4, 8, 4, 2, 4, 2, 4, 4, 4, 4, 4, 4, 8],
            handler_slots: [2, 2, 4, 8, 4, 1, 4, 1, 4, 4, 8, 4, 4, 4, 8],
            payload_bytes: [
                128 << 10,
                32 << 20,
                64 << 10,
                16 << 10,
                2 * crate::protocol::MAX_BODY_BYTES + 4096,
                64 << 10,
                32 << 20,
                64 << 10,
                128 << 10,
                2 * crate::protocol::MAX_BODY_BYTES + (16 << 10),
                64 << 10,
                128 << 10,
                128 << 10,
                128 << 10,
                0,
            ],
            decode_bytes: [
                512 << 10,
                16 << 20,
                128 << 10,
                32 << 10,
                16 << 20,
                64 << 10,
                32 << 20,
                64 << 10,
                1 << 20,
                128 << 20,
                256 << 10,
                1 << 20,
                1 << 20,
                2 << 20,
                RAFT_PROGRESS_DECODE_BYTES,
            ],
            deadline: Duration::from_secs(30),
            session_slots: [256; CLASSES],
            pending_sessions: 64,
            ping_idle: Duration::from_secs(15),
            ping_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
        }
    }
}
impl TransportConfig {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.handshake_slots == 0
            || self.handshake_slots > Semaphore::MAX_PERMITS
            || self
                .preflight_slots
                .iter()
                .chain(&self.handler_slots)
                .chain(&self.session_slots)
                .chain([&self.pending_sessions])
                .any(|&n| n == 0 || n > Semaphore::MAX_PERMITS)
            || self
                .payload_bytes
                .iter()
                .enumerate()
                .any(|(i, &n)| (n == 0) != (i == TrafficClass::RaftProgress as usize))
            || self.decode_bytes.contains(&0)
            || self.deadline.is_zero()
            || self.ping_idle.is_zero()
            || self.ping_timeout.is_zero()
            || self.connect_timeout.is_zero()
        {
            return Err(malformed("invalid worker transport limits"));
        }
        Ok(())
    }
}

struct Residency {
    identity: GroupIdentity,
    _slot: GroupCapacityLease,
    shared: Arc<WorkerSharedResources>,
}
/// Clone this into the Raft network factory and state machine BEFORE spawning
/// their tasks. The last task owner retains the resident permit on cancellation.
#[derive(Clone)]
pub struct ResidentLease(Arc<Residency>);
impl ResidentLease {
    pub fn identity(&self) -> &GroupIdentity {
        &self.0.identity
    }
}
struct Binding {
    lease: ResidentLease,
    endpoints: Mutex<BTreeMap<Purpose, Arc<dyn Endpoint>>>,
    replicas: Mutex<BTreeMap<u64, std::sync::Weak<ReplicaResidency>>>,
    closed: AtomicBool,
}
struct ReplicaResidency {
    group: ResidentLease,
    node_id: u64,
    _slot: GroupCapacityLease,
    metadata_used: std::sync::atomic::AtomicUsize,
    log_metadata_used: std::sync::atomic::AtomicUsize,
    /// Woken whenever a metadata charge is released (RV-APPLY F3, F5).
    metadata_released: tokio::sync::Notify,
    ownership: std::sync::OnceLock<Arc<dyn Send + Sync>>,
}
/// One live Raft instance, keyed by permanent node ID inside its group. A
/// colocated replacement learner consumes a second real slot and byte budget.
#[derive(Clone)]
pub(crate) struct ReplicaLease(Arc<ReplicaResidency>);
impl ReplicaLease {
    pub(crate) fn retain_owner(&self, owner: Arc<dyn Send + Sync>) -> Result<(), ProtocolError> {
        self.0
            .ownership
            .set(owner)
            .map_err(|_| malformed("replica ownership already bound"))
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.0.group.identity()
    }
    pub(crate) fn node_id(&self) -> u64 {
        self.0.node_id
    }
    pub(crate) fn same_resources(&self, shared: &Arc<WorkerSharedResources>) -> bool {
        Arc::ptr_eq(shared, &self.0.group.0.shared)
    }
    pub(crate) fn metadata(&self, bytes: usize) -> Result<ReplicaMetadata, ProtocolError> {
        self.reserve_metadata(bytes, false)
    }
    /// Two MiB of the existing eight MiB resident charge belongs solely to
    /// live entry/container metadata. Wire and snapshot pressure cannot use it.
    pub(crate) fn log_metadata(&self, bytes: usize) -> Result<ReplicaMetadata, ProtocolError> {
        self.reserve_metadata(bytes, true)
    }
    pub(crate) fn proposal_metadata(&self, bytes: usize) -> Result<ReplicaMetadata, ProtocolError> {
        self.reserve_metadata_limit(bytes, true, (2 << 20) - (128 << 10))
    }
    /// [`Self::metadata`] that waits for room instead of failing. Every
    /// holder releases on drop (snapshot objects, transfer descriptions), so
    /// the wait ends. Used where a refusal would stop the Raft core (RV-APPLY
    /// F3). A request larger than the whole budget fails at once.
    pub(crate) async fn metadata_wait(
        &self,
        bytes: usize,
    ) -> Result<ReplicaMetadata, ProtocolError> {
        self.wait_metadata(bytes, false, RAFT_REPLICA_BYTES / 2)
            .await
    }
    /// [`Self::log_metadata`] that waits for room (RV-APPLY F5: the leader's
    /// own Blank and Membership entries in `append`).
    pub(crate) async fn log_metadata_wait(
        &self,
        bytes: usize,
    ) -> Result<ReplicaMetadata, ProtocolError> {
        self.wait_metadata(bytes, true, 2 << 20).await
    }
    async fn wait_metadata(
        &self,
        bytes: usize,
        log: bool,
        limit: usize,
    ) -> Result<ReplicaMetadata, ProtocolError> {
        if bytes > limit {
            return Err(malformed("replica metadata request exceeds its budget"));
        }
        loop {
            let released = self.0.metadata_released.notified();
            if let Ok(owner) = self.reserve_metadata_limit(bytes, log, limit) {
                return Ok(owner);
            }
            released.await;
        }
    }
    pub(crate) fn log_metadata_used(&self) -> usize {
        self.0
            .log_metadata_used
            .load(std::sync::atomic::Ordering::Acquire)
    }
    fn reserve_metadata(&self, bytes: usize, log: bool) -> Result<ReplicaMetadata, ProtocolError> {
        self.reserve_metadata_limit(
            bytes,
            log,
            if log { 2 << 20 } else { RAFT_REPLICA_BYTES / 2 },
        )
    }
    fn reserve_metadata_limit(
        &self,
        bytes: usize,
        log: bool,
        limit: usize,
    ) -> Result<ReplicaMetadata, ProtocolError> {
        use std::sync::atomic::Ordering;
        let counter = if log {
            &self.0.log_metadata_used
        } else {
            &self.0.metadata_used
        };
        let mut used = counter.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|n| *n <= limit)
                .ok_or_else(|| overloaded("replica metadata capacity exhausted"))?;
            match counter.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    return Ok(ReplicaMetadata {
                        owner: self.clone(),
                        bytes,
                        log,
                    });
                }
                Err(current) => used = current,
            }
        }
    }
}
pub(crate) struct ReplicaMetadata {
    owner: ReplicaLease,
    bytes: usize,
    log: bool,
}
impl ReplicaMetadata {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn same_replica(&self, resident: &ReplicaLease) -> bool {
        Arc::ptr_eq(&self.owner.0, &resident.0)
    }
}
impl Drop for ReplicaMetadata {
    fn drop(&mut self) {
        (if self.log {
            &self.owner.0.log_metadata_used
        } else {
            &self.owner.0.metadata_used
        })
        .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
        self.owner.0.metadata_released.notify_waiters();
    }
}
#[derive(Clone)]
pub struct ResidentGroup(Arc<Binding>);
impl ResidentGroup {
    pub fn lease(&self) -> ResidentLease {
        self.0.lease.clone()
    }
    pub(crate) fn activate_replica(
        &self,
        node_id: u64,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<ReplicaLease, ProtocolError> {
        if node_id == 0 || !Arc::ptr_eq(shared, &self.0.lease.0.shared) {
            return Err(malformed(
                "invalid hosted replica node or physical worker resources",
            ));
        }
        let mut replicas = self.0.replicas.lock().map_err(|_| unavailable())?;
        if self.0.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        replicas.retain(|_, replica| replica.strong_count() != 0);
        if replicas
            .get(&node_id)
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            return Err(malformed("replica node is already live"));
        }
        let slot = shared.lease_replica_residency()?;
        let replica = Arc::new(ReplicaResidency {
            group: self.lease(),
            node_id,
            _slot: slot,
            metadata_used: std::sync::atomic::AtomicUsize::new(0),
            log_metadata_used: std::sync::atomic::AtomicUsize::new(0),
            metadata_released: tokio::sync::Notify::new(),
            ownership: std::sync::OnceLock::new(),
        });
        replicas.insert(node_id, Arc::downgrade(&replica));
        Ok(ReplicaLease(replica))
    }
    pub fn bind(&self, purpose: Purpose, endpoint: Arc<dyn Endpoint>) -> Result<(), ProtocolError> {
        traffic(self.lease().identity(), purpose)?;
        let mut endpoints = self.0.endpoints.lock().map_err(|_| unavailable())?;
        if self.0.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        if endpoints.contains_key(&purpose) {
            return Err(malformed("group purpose already registered"));
        }
        endpoints.insert(purpose, endpoint);
        Ok(())
    }
}
/// Strong resident registrations are never evicted as an LRU. Retirement will
/// require committed placement proof and joined Raft tasks; it has no raw remove
/// operation. Closing the whole worker is a separate lifecycle operation.
pub struct EndpointRegistry {
    shared: Arc<WorkerSharedResources>,
    groups: Mutex<BTreeMap<Uuid, Arc<Binding>>>,
    closed: AtomicBool,
}
impl EndpointRegistry {
    pub fn new(shared: Arc<WorkerSharedResources>) -> Self {
        Self {
            shared,
            groups: Mutex::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
        }
    }
    pub fn reserve(&self, identity: GroupIdentity) -> Result<ResidentGroup, ProtocolError> {
        self.reserve_with(identity, |group| Ok(group.clone()))
    }
    pub(crate) fn reserve_replica(
        &self,
        identity: GroupIdentity,
        node_id: u64,
    ) -> Result<(ResidentGroup, ReplicaLease), ProtocolError> {
        self.reserve_with(identity, |group| {
            let replica = group.activate_replica(node_id, &self.shared)?;
            Ok((group.clone(), replica))
        })
    }
    fn reserve_with<T>(
        &self,
        identity: GroupIdentity,
        activate: impl FnOnce(&ResidentGroup) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        identity
            .validate()
            .map_err(|_| malformed("invalid resident group identity"))?;
        let mut groups = self.groups.lock().map_err(|_| unavailable())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let id = identity.group_id;
        if let Some(existing) = groups.get(&id) {
            if existing.lease.identity() != &identity {
                return Err(malformed("resident group identity changed"));
            }
            return activate(&ResidentGroup(existing.clone()));
        }
        let slot = self.shared.lease_group_residency()?;
        let group = ResidentGroup(Arc::new(Binding {
            lease: ResidentLease(Arc::new(Residency {
                identity,
                _slot: slot,
                shared: self.shared.clone(),
            })),
            endpoints: Mutex::new(BTreeMap::new()),
            replicas: Mutex::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
        }));
        let result = activate(&group)?;
        groups.insert(id, group.0);
        Ok(result)
    }
    fn lookup(
        &self,
        id: Uuid,
        purpose: Purpose,
    ) -> Result<(ResidentLease, Arc<dyn Endpoint>), ProtocolError> {
        let groups = self.groups.lock().map_err(|_| unavailable())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let group = groups
            .get(&id)
            .ok_or_else(|| malformed("worker does not host this group"))?;
        let endpoint = group
            .endpoints
            .lock()
            .map_err(|_| unavailable())?
            .get(&purpose)
            .cloned()
            .ok_or_else(|| malformed("group purpose is unavailable"))?;
        Ok((group.lease.clone(), endpoint))
    }
    /// Close the entire worker's registrations, never an individual replica.
    /// Existing RPC/native/Raft owners retain their leases until joined; this
    /// only prevents new work and breaks service-routing ownership cycles.
    pub(crate) fn close_worker(&self) -> Result<(), ProtocolError> {
        self.closed.store(true, Ordering::Release);
        let groups = {
            let mut groups = self.groups.lock().map_err(|_| unavailable())?;
            std::mem::take(&mut *groups)
        };
        for group in groups.into_values() {
            // Bind/activate check the flag while holding their own lock, so a
            // retained ResidentGroup cannot resurrect a closed registration.
            group.closed.store(true, Ordering::Release);
            group.endpoints.lock().map_err(|_| unavailable())?.clear();
            group.replicas.lock().map_err(|_| unavailable())?.clear();
        }
        Ok(())
    }
}

struct Common {
    local: WorkerIdentity,
    shared: Arc<WorkerSharedResources>,
    directory: Arc<dyn PeerDirectory>,
    connector: TlsConnector,
    acceptor: TlsAcceptor,
    /// Offers only the one-shot ALPN, for handshakes without a pending
    /// session slot: the caller falls back instead of losing an agreed session.
    one_shot_acceptor: TlsAcceptor,
    config: TransportConfig,
    handshakes: Arc<Semaphore>,
    preflight: [Arc<Semaphore>; CLASSES],
    handlers: [Arc<Semaphore>; CLASSES],
    /// Caller slots per peer lane (peer worker, traffic class). A peer that
    /// stops reading (SIGSTOP, a black-holed route) holds only its own lane's
    /// slots until their deadlines; requests to other peers keep theirs. With
    /// one class-wide pool, its stalled requests took every slot, and Raft
    /// votes and heartbeats to the healthy peers were refused locally (LIVE1).
    outgoing: OutgoingLanes,
    /// Caller slots per class across every peer lane (RV-FINAL L1).
    outgoing_class: [Arc<Semaphore>; CLASSES],
    /// Bounds on the callers that wait for a lane (`reserve_outgoing`): one
    /// slot short of each peer lane, and one lane's worth short of each
    /// class, so a caller that refuses at once (a queued submission next to
    /// waiting direct requests) always finds headroom (PERF1).
    waiting: OutgoingLanes,
    waiting_class: [Arc<Semaphore>; CLASSES],
    authority_roles: Semaphore,
    authority_waiters: Semaphore,
    resolver: PeerResolver,
    /// False only for a test peer that speaks the one-shot protocol alone.
    sessions_enabled: bool,
    sessions: PeerSessions,
    session_slots: [Arc<Semaphore>; CLASSES],
    pending_sessions: Arc<Semaphore>,
    /// Local lane refusals, reported in `/v1/runtime/stats` (NS11 finding 8).
    refusals: LaneRefusals,
    #[cfg(test)]
    outgoing_lane_full: [tokio::sync::Notify; CLASSES],
    /// Tests close a whole class with `hold_outgoing_for_test`.
    #[cfg(test)]
    outgoing_hold: [Arc<Semaphore>; CLASSES],
    /// Tests hold caller session writers before they dequeue a frame.
    #[cfg(test)]
    hold_writes: tokio::sync::watch::Sender<bool>,
    /// Replies a receiver skipped because the caller cancelled the stream.
    #[cfg(test)]
    cancelled_replies: std::sync::atomic::AtomicU64,
}
/// Requests this worker refused locally because a lane was full: `outgoing`
/// before sending (caller slots), `handler` on receipt (handler slots).
#[derive(Default)]
struct LaneRefusals {
    outgoing: [AtomicU64; CLASSES],
    handler: [AtomicU64; CLASSES],
}
impl LaneRefusals {
    fn outgoing(&self, class: TrafficClass) {
        self.outgoing[class as usize].fetch_add(1, Ordering::Relaxed);
    }
    fn handler(&self, class: TrafficClass) {
        self.handler[class as usize].fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(&self) -> serde_json::Value {
        let side = |counters: &[AtomicU64; CLASSES]| {
            let mut total = 0;
            let mut by_class = serde_json::Map::new();
            for class in TrafficClass::ALL {
                let count = counters[class as usize].load(Ordering::Relaxed);
                if count != 0 {
                    total += count;
                    by_class.insert(format!("{class:?}"), count.into());
                }
            }
            serde_json::json!({"total": total, "by_class": by_class})
        };
        serde_json::json!({
            "outgoing_lane_full": side(&self.outgoing),
            "handler_lane_full": side(&self.handler),
        })
    }
}
pub struct WorkerTransport {
    common: Arc<Common>,
    registry: Arc<EndpointRegistry>,
}
#[derive(Clone)]
pub struct WorkerClient {
    common: Arc<Common>,
}

#[derive(Debug)]
enum RaftTransportPhase {
    ResolvePeer,
    ResolveAddress,
    Connect,
    Tls,
    SendRequest,
    ReceiveReply,
    Route,
    AuthorizePeer,
    Preflight,
    ReadSections,
    Handle,
    SendReply,
    Complete,
}

struct RaftTransportTrace {
    group: Uuid,
    purpose: Purpose,
    phase: RaftTransportPhase,
}

impl Drop for RaftTransportTrace {
    fn drop(&mut self) {
        if matches!(self.purpose, Purpose::Raft | Purpose::RaftProgress)
            && !matches!(self.phase, RaftTransportPhase::Complete)
        {
            tracing::debug!(group=%self.group, purpose=?self.purpose, phase=?self.phase,
                "Raft transport ended before completion");
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RequestTarget {
    WorkerId(Uuid),
    Exact(WorkerIdentity),
    Feature(Uuid, PeerFeature),
}
impl RequestTarget {
    pub(crate) fn worker_id(self) -> Uuid {
        match self {
            Self::WorkerId(id) | Self::Feature(id, _) => id,
            Self::Exact(worker) => worker.worker_id,
        }
    }
}
impl From<Uuid> for RequestTarget {
    fn from(worker: Uuid) -> Self {
        Self::WorkerId(worker)
    }
}
impl From<WorkerIdentity> for RequestTarget {
    fn from(worker: WorkerIdentity) -> Self {
        Self::Exact(worker)
    }
}

#[derive(Debug)]
pub(crate) enum RequestError {
    OutgoingLaneFull,
    Unreachable,
    PeerRejected {
        worker: WorkerIdentity,
        error: ProtocolError,
    },
    Protocol(ProtocolError),
}
impl From<ProtocolError> for RequestError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
impl From<RequestError> for ProtocolError {
    fn from(error: RequestError) -> Self {
        match error {
            RequestError::OutgoingLaneFull => overloaded("worker outgoing lane full"),
            RequestError::Unreachable => unavailable(),
            RequestError::Protocol(error) | RequestError::PeerRejected { error, .. } => error,
        }
    }
}
impl WorkerTransport {
    pub fn new(
        local: &super::super::types::WorkerEnrollment,
        tls: &TlsMaterial,
        shared: Arc<WorkerSharedResources>,
        directory: Arc<dyn PeerDirectory>,
        registry: Arc<EndpointRegistry>,
        config: TransportConfig,
    ) -> Result<Self, ProtocolError> {
        Self::build(local, tls, shared, directory, registry, config, true)
    }
    /// A peer as released before persistent sessions: it offers and accepts
    /// only the one-shot `NABOTP01` exchange.
    #[cfg(test)]
    pub(crate) fn one_shot_for_test(
        local: &super::super::types::WorkerEnrollment,
        tls: &TlsMaterial,
        shared: Arc<WorkerSharedResources>,
        directory: Arc<dyn PeerDirectory>,
        registry: Arc<EndpointRegistry>,
        config: TransportConfig,
    ) -> Result<Self, ProtocolError> {
        Self::build(local, tls, shared, directory, registry, config, false)
    }
    fn build(
        local: &super::super::types::WorkerEnrollment,
        tls: &TlsMaterial,
        shared: Arc<WorkerSharedResources>,
        directory: Arc<dyn PeerDirectory>,
        registry: Arc<EndpointRegistry>,
        config: TransportConfig,
        sessions_enabled: bool,
    ) -> Result<Self, ProtocolError> {
        local
            .validate()
            .map_err(|_| malformed("invalid local worker enrollment"))?;
        shared.bind_worker(local.worker_id)?;
        if !Arc::ptr_eq(&shared, &registry.shared) {
            return Err(malformed(
                "listener and registry have different worker resources",
            ));
        }
        config.validate()?;
        if local.certificate_sha256.to_string() != tls.certificate_sha256() {
            return Err(malformed("local certificate differs from enrollment"));
        }
        let (connector, acceptor) = if sessions_enabled {
            tls.transports(&[INDEX_RECLAIM_ALPN, SESSION_ALPN, ALPN])
        } else {
            tls.transports(&[ALPN])
        };
        let (_, one_shot_acceptor) = tls.transports(&[ALPN]);
        Ok(Self {
            common: Arc::new(Common {
                local: WorkerIdentity::from(local),
                shared,
                directory,
                connector,
                acceptor,
                one_shot_acceptor,
                handshakes: Arc::new(Semaphore::new(config.handshake_slots)),
                preflight: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.preflight_slots[i]))
                }),
                handlers: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.handler_slots[i]))
                }),
                outgoing: OutgoingLanes::default(),
                outgoing_class: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(outgoing_class_slots(
                        config.handler_slots[i],
                    )))
                }),
                waiting: OutgoingLanes {
                    headroom: 1,
                    ..OutgoingLanes::default()
                },
                waiting_class: std::array::from_fn(|i| {
                    let class = outgoing_class_slots(config.handler_slots[i]);
                    Arc::new(Semaphore::new(
                        class.saturating_sub(config.handler_slots[i]).max(1),
                    ))
                }),
                authority_roles: Semaphore::new(
                    config.handler_slots[TrafficClass::Authority as usize] - 1,
                ),
                authority_waiters: Semaphore::new(
                    config.preflight_slots[TrafficClass::Authority as usize],
                ),
                resolver: PeerResolver::default(),
                sessions_enabled,
                sessions: PeerSessions::default(),
                session_slots: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.session_slots[i]))
                }),
                pending_sessions: Arc::new(Semaphore::new(config.pending_sessions)),
                refusals: LaneRefusals::default(),
                #[cfg(test)]
                outgoing_lane_full: std::array::from_fn(|_| tokio::sync::Notify::new()),
                #[cfg(test)]
                outgoing_hold: std::array::from_fn(|_| {
                    Arc::new(Semaphore::new(TEST_HOLD_PERMITS as usize))
                }),
                #[cfg(test)]
                hold_writes: tokio::sync::watch::channel(false).0,
                #[cfg(test)]
                cancelled_replies: std::sync::atomic::AtomicU64::new(0),
                config,
            }),
            registry,
        })
    }
    pub fn client(&self) -> WorkerClient {
        WorkerClient {
            common: self.common.clone(),
        }
    }
    pub async fn run(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ProtocolError> {
        tokio::pin!(shutdown);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _=&mut shutdown=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                accepted=listener.accept()=>{
                    let (socket,_)=accepted.map_err(|_| unavailable())?;
                    let Ok(permit)=self.common.handshakes.clone().try_acquire_owned() else {drop(socket);continue};
                    let owner=self.clone();
                    let stopped=stopped.clone();
                    tasks.spawn(async move {owner.accept(socket,permit,stopped).await;});
                }
            }
        }
        // Sessions stop reading new frames and finish their in-flight streams.
        let _ = stop.send(true);
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
    /// The TLS handshake runs under the handshake slot. A session releases it
    /// once established; a one-shot exchange keeps it, as before sessions.
    /// Sessions are offered only while a pending session slot is free, so a
    /// full listener falls back to one-shot during the handshake and never
    /// drops a session it agreed to.
    async fn accept(
        self: Arc<Self>,
        socket: TcpStream,
        handshake: OwnedSemaphorePermit,
        stopped: tokio::sync::watch::Receiver<bool>,
    ) {
        let deadline = Instant::now() + self.common.config.deadline;
        tune_socket(&socket);
        let pending = if self.common.sessions_enabled {
            self.common
                .pending_sessions
                .clone()
                .try_acquire_owned()
                .ok()
        } else {
            None
        };
        let acceptor = if pending.is_some() {
            &self.common.acceptor
        } else {
            &self.common.one_shot_acceptor
        };
        let Ok(Ok(stream)) = timeout_at(deadline, acceptor.accept(socket)).await else {
            return;
        };
        if let Some(pending) = pending
            && session_alpn(stream.get_ref().1.alpn_protocol())
        {
            let Ok(certificate) = peer_digest(stream.get_ref().1.peer_certificates()) else {
                return;
            };
            drop(handshake);
            session::serve(self, stream, certificate, stopped, pending).await;
            return;
        }
        let _ = timeout_at(deadline, self.serve(stream, handshake)).await;
    }
    async fn serve(
        &self,
        mut stream: tokio_rustls::server::TlsStream<TcpStream>,
        classification: OwnedSemaphorePermit,
    ) -> Result<(), ProtocolError> {
        if stream.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(malformed("wrong worker ALPN"));
        }
        let certificate = peer_digest(stream.get_ref().1.peer_certificates())?;
        let mut bytes = [0; PROLOGUE_BYTES];
        stream
            .read_exact(&mut bytes)
            .await
            .map_err(|_| unavailable())?;
        let hello = Hello::decode(&bytes, certificate, self.common.local.certificate_sha256)?;
        let mut trace = RaftTransportTrace {
            group: hello.group_id,
            purpose: hello.purpose,
            phase: RaftTransportPhase::Route,
        };
        let (lease, endpoint) = self.registry.lookup(hello.group_id, hello.purpose)?;
        let group = lease.identity();
        if hello.cluster_id != group.cluster_id
            || hello.group_digest != identity_digest(group)?
            || !audience(&*self.common.directory, &hello, group, &self.common.local)
        {
            return Err(malformed("foreign worker/group audience"));
        }
        trace.phase = RaftTransportPhase::AuthorizePeer;
        let trust = self
            .common
            .directory
            .verify(hello.from, group, hello.purpose)?;
        admit_trust(trust, group, hello.purpose)?;
        let class = traffic(group, hello.purpose)?;
        trace.phase = RaftTransportPhase::Preflight;
        let preflight = self.common.preflight[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded("worker preflight lane full"))?;
        // Classified queue waits cannot occupy the TLS/prologue pool needed by
        // independent Control consensus or authority connections.
        drop(classification);
        let mut header = self
            .common
            .shared
            .allocate_bytes(class.byte_class(), hello.header_len)?;
        stream
            .read_exact(header.as_mut_slice())
            .await
            .map_err(|_| unavailable())?;
        let header = header.freeze();
        let session = AuthenticatedSession {
            local: self.common.local,
            remote: hello.from,
            group: group.clone(),
            purpose: hello.purpose,
            nonce: hello.nonce,
            recovery_only: trust.recovery_only(),
        };
        let result = async {
            let header_scratch = self
                .common
                .shared
                .reserve_scratch(class.byte_class(), header_decode_bytes(class, header.len()))?;
            let plan = endpoint.preflight(&session, header.as_slice()).await?;
            drop(header_scratch);
            validate_plan(&self.common.config, class, plan)?;
            let handler = self.common.handlers[class as usize]
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    self.common.refusals.handler(class);
                    overloaded("worker handler lane full")
                })?;
            trace.phase = RaftTransportPhase::ReadSections;
            let sections = read_sections(&mut stream, &self.common, class, plan).await?;
            drop(preflight);
            trace.phase = RaftTransportPhase::Handle;
            if self.common.directory.revoked(hello.from) {
                return Err(revoked_peer());
            }
            let reply = endpoint.handle(session, header, sections).await?;
            validate_reply(&self.common.config, class, &reply)?;
            // Free the lane before the reply can reach the peer, as on a
            // session: its next request must not find this call's slot held.
            drop(handler);
            trace.phase = RaftTransportPhase::SendReply;
            write_reply(&mut stream, hello.nonce, 0, &reply).await
        }
        .await;
        if let Err(error) = result {
            if matches!(hello.purpose, Purpose::Raft | Purpose::RaftProgress) {
                tracing::debug!(group=%hello.group_id, purpose=?hello.purpose, phase=?trace.phase,
                    code=?error.code, "Raft transport rejected request");
            }
            trace.phase = RaftTransportPhase::SendReply;
            let encoded = self.common.shared.encode_in(
                ByteClass::Progress,
                &error,
                header_limit(class).min(2048),
            )?;
            write_reply(
                &mut stream,
                hello.nonce,
                1,
                &ChargedReply {
                    header: encoded,
                    sections: std::array::from_fn(|_| None),
                },
            )
            .await?;
        }
        trace.phase = RaftTransportPhase::Complete;
        Ok(())
    }
}

impl WorkerClient {
    pub(crate) fn local_identity(&self) -> WorkerIdentity {
        self.common.local
    }
    pub async fn request(
        &self,
        group: &GroupIdentity,
        target: Uuid,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.request_classified(
            group,
            target,
            purpose,
            header,
            sections,
            response_decode_bytes,
        )
        .await
        .map_err(Into::into)
    }
    pub(crate) async fn request_classified(
        &self,
        group: &GroupIdentity,
        target: impl Into<RequestTarget>,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, RequestError> {
        self.request_with_timeout(
            group,
            target.into(),
            (purpose, None),
            header,
            sections,
            response_decode_bytes,
            None,
        )
        .await
    }
    /// Waits until `until` for one caller slot on the lane to `target` for
    /// the class `group` and `purpose` select, instead of refusing at once
    /// when the lane is full (PERF1). Nothing is written while waiting, so a
    /// timeout is still a not-applied `OutgoingLaneFull`. The slot keeps the
    /// lane's bound: no more streams in flight to `target` than its handler
    /// admits. Pass it to [`Self::request_reserved`].
    pub(crate) async fn reserve_outgoing(
        &self,
        group: &GroupIdentity,
        target: Uuid,
        purpose: Purpose,
        until: Instant,
    ) -> Result<OutgoingReservation, RequestError> {
        group
            .validate()
            .map_err(|_| malformed("invalid target group"))?;
        let class = traffic(group, purpose)?;
        let index = class as usize;
        let waiting_lane = self.common.waiting.lane(target, class, &self.common.config);
        let waiting_class = self.common.waiting_class[index].clone();
        let lane = self
            .common
            .outgoing
            .lane(target, class, &self.common.config);
        let class_slots = self.common.outgoing_class[index].clone();
        let acquire = timeout_at(until, async {
            let waiting = (
                waiting_lane.acquire_owned().await?,
                waiting_class.acquire_owned().await?,
            );
            #[cfg(test)]
            let held = self.common.outgoing_hold[index]
                .clone()
                .acquire_owned()
                .await?;
            let slot = lane.acquire_owned().await?;
            let class_slot = class_slots.acquire_owned().await?;
            Ok::<_, tokio::sync::AcquireError>((
                waiting,
                OutgoingSlot {
                    _slot: slot,
                    _class: class_slot,
                    #[cfg(test)]
                    _held: held,
                },
            ))
        });
        // Charge the pending acquisition and timer, as `authority_slots` does.
        let _memory = self
            .common
            .shared
            .reserve_scratch(ByteClass::Progress, std::mem::size_of_val(&acquire))?;
        match acquire.await {
            Ok(Ok((waiting, slot))) => Ok(OutgoingReservation {
                target,
                class,
                slot,
                _waiting: waiting,
            }),
            _ => {
                self.common.refusals.outgoing(class);
                Err(RequestError::OutgoingLaneFull)
            }
        }
    }
    /// [`Self::request_classified`] on a slot from [`Self::reserve_outgoing`]
    /// for the same target and class.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_reserved(
        &self,
        reservation: OutgoingReservation,
        group: &GroupIdentity,
        target: impl Into<RequestTarget>,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, RequestError> {
        let target = target.into();
        if reservation.target != target.worker_id()
            || traffic(group, purpose).ok() != Some(reservation.class)
        {
            return Err(malformed("outgoing reservation is for another lane").into());
        }
        self.request_with_timeout(
            group,
            target,
            (purpose, None),
            header,
            sections,
            response_decode_bytes,
            Some(reservation.slot),
        )
        .await
    }
    /// A pending local DNS lookup is indeterminate for a liveness probe.
    pub(crate) async fn request_queue_probe(
        &self,
        group: &GroupIdentity,
        target: WorkerIdentity,
        header: ChargedBytes,
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, RequestError> {
        self.request_with_timeout(
            group,
            target.into(),
            (Purpose::QueueControl, Some(Duration::from_millis(750))),
            header,
            [None, None, None],
            response_decode_bytes,
            None,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn request_with_timeout(
        &self,
        group: &GroupIdentity,
        target: RequestTarget,
        (purpose, probe_timeout): (Purpose, Option<Duration>),
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
        reserved: Option<OutgoingSlot>,
    ) -> Result<ReceivedReply, RequestError> {
        group
            .validate()
            .map_err(|_| malformed("invalid target group"))?;
        let class = traffic(group, purpose)?;
        let _slot = match reserved {
            Some(slot) => slot,
            None => self.try_outgoing(target.worker_id(), class)?,
        };
        let deadline = self.common.config.deadline;
        let mut resolving = false;
        timeout(
            probe_timeout.map_or(deadline, |probe| probe.min(deadline)),
            self.exchange(
                group,
                target,
                (purpose, class),
                header,
                sections,
                response_decode_bytes,
                probe_timeout.map(|_| &mut resolving),
            ),
        )
        .await
        .map_err(|_| {
            if resolving {
                unavailable().into()
            } else {
                RequestError::Unreachable
            }
        })?
    }
    /// A service preflight depends on a fresh Control role read. It can wait
    /// for the existing outgoing slot, then send one exchange in the same
    /// transport deadline.
    pub(crate) async fn request_authority_dependency(
        &self,
        group: &GroupIdentity,
        target: Uuid,
        header: ChargedBytes,
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, RequestError> {
        group
            .validate()
            .map_err(|_| malformed("invalid target group"))?;
        let purpose = Purpose::Authority;
        let class = traffic(group, purpose)?;
        if class != TrafficClass::Authority {
            return Err(malformed("Control authority dependency requires a Control group").into());
        }
        let deadline = Instant::now()
            .checked_add(self.common.config.deadline)
            .ok_or_else(|| malformed("worker transport deadline is too large"))?;
        let refused = |error: RequestError| {
            if matches!(error, RequestError::OutgoingLaneFull) {
                self.common.refusals.outgoing(class);
            }
            error
        };
        let _slots = self
            .authority_slots(target, deadline)
            .await
            .map_err(refused)?;
        if Instant::now() >= deadline {
            return Err(refused(RequestError::OutgoingLaneFull));
        }
        timeout_at(
            deadline,
            self.exchange(
                group,
                target.into(),
                (purpose, class),
                header,
                [None, None, None],
                response_decode_bytes,
                None,
            ),
        )
        .await
        .map_err(|_| RequestError::Unreachable)?
    }
    async fn authority_slots(
        &self,
        target: Uuid,
        deadline: Instant,
    ) -> Result<(Option<SemaphorePermit<'_>>, OutgoingSlot), RequestError> {
        let class = TrafficClass::Authority;
        let index = class as usize;
        let roles = &self.common.authority_roles;
        // Keep an existing slot available to ordinary Authority traffic.
        // A one-slot lane has no capacity to reserve and keeps immediate admission.
        if self.common.config.handler_slots[index] == 1 {
            return self
                .try_outgoing_slot(target, class)
                .map(|slot| (None, slot));
        }
        if let Ok(role) = roles.try_acquire()
            && let Ok(slot) = self.try_outgoing_slot(target, class)
        {
            return Ok((Some(role), slot));
        }
        let outgoing = self
            .common
            .outgoing
            .lane(target, class, &self.common.config);
        let class_slots = self.common.outgoing_class[index].clone();
        let _waiter = self
            .common
            .authority_waiters
            .try_acquire()
            .map_err(|_| RequestError::OutgoingLaneFull)?;
        let acquire = timeout_at(deadline, async {
            let role = roles.acquire().await?;
            #[cfg(test)]
            let held = self.common.outgoing_hold[index]
                .clone()
                .acquire_owned()
                .await?;
            let slot = outgoing.acquire_owned().await?;
            let class_slot = class_slots.acquire_owned().await?;
            Ok::<_, tokio::sync::AcquireError>((
                Some(role),
                OutgoingSlot {
                    _slot: slot,
                    _class: class_slot,
                    #[cfg(test)]
                    _held: held,
                },
            ))
        });
        // The caller already owns the charged request header. Charge the
        // pending acquisition and timer until a permit becomes available.
        let _memory = self
            .common
            .shared
            .reserve_scratch(ByteClass::Progress, std::mem::size_of_val(&acquire))?;
        acquire
            .await
            .map_err(|_| RequestError::OutgoingLaneFull)?
            .map_err(|_| RequestError::OutgoingLaneFull)
    }
    async fn exchange(
        &self,
        group: &GroupIdentity,
        target: RequestTarget,
        (purpose, class): (Purpose, TrafficClass),
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
        mut probe_resolving: Option<&mut bool>,
    ) -> Result<ReceivedReply, RequestError> {
        let mut trace = RaftTransportTrace {
            group: group.group_id,
            purpose,
            phase: RaftTransportPhase::ResolvePeer,
        };
        let (worker, expected, required) = match target {
            RequestTarget::WorkerId(worker) => (worker, None, None),
            RequestTarget::Exact(identity) => (identity.worker_id, Some(identity), None),
            RequestTarget::Feature(worker, feature) => (worker, None, Some(feature)),
        };
        let peer = self.common.directory.resolve(worker)?;
        peer.validate()
            .map_err(|_| malformed("invalid target enrollment"))?;
        let identity = WorkerIdentity::from(&peer);
        if expected.is_some_and(|expected| expected != identity) {
            return Err(malformed("target incarnation differs from cached enrollment").into());
        }
        let trust = self.common.directory.verify(identity, group, purpose)?;
        admit_trust(trust, group, purpose)?;
        validate_reply(
            &self.common.config,
            class,
            &ChargedReply {
                header: header.clone(),
                sections: sections.clone(),
            },
        )?;
        let nonce = Uuid::new_v4();
        let hello = Hello {
            cluster_id: group.cluster_id,
            group_id: group.group_id,
            group_digest: identity_digest(group)?,
            purpose,
            from: self.common.local,
            to: identity,
            nonce,
            header_len: header.len(),
        };
        let deadline = Instant::now() + self.common.config.deadline;
        let raw = self
            .common
            .sessions
            .call(
                &self.common,
                &peer,
                class,
                &hello.encode(),
                header,
                sections,
                deadline,
                required,
                probe_resolving.take(),
                &mut trace.phase,
            )
            .await
            .map_err(|error| match trace.phase {
                RaftTransportPhase::ReceiveReply => sent_request_failed(error),
                _ => error,
            })?;
        let reply = self
            .finish(raw, nonce, class, response_decode_bytes)
            .map_err(sent_request_failed)?;
        trace.phase = RaftTransportPhase::Complete;
        match reply {
            Err(error) => Err(RequestError::PeerRejected {
                worker: identity,
                error,
            }),
            Ok((reply, scratch)) => Ok(ReceivedReply {
                reply,
                scratch,
                session: AuthenticatedSession {
                    local: self.common.local,
                    remote: identity,
                    group: group.clone(),
                    purpose,
                    nonce,
                    recovery_only: trust.recovery_only(),
                },
                resources: self.common.shared.clone(),
            }),
        }
    }
    /// Check a framed reply against this request. The outer error is local;
    /// the inner error is the peer's rejection.
    #[allow(clippy::type_complexity)]
    fn finish(
        &self,
        raw: RawReply,
        nonce: Uuid,
        class: TrafficClass,
        response_decode_bytes: usize,
    ) -> Result<Result<(ChargedReply, Arc<ScratchLease>), ProtocolError>, RequestError> {
        let RawReply {
            prefix,
            header,
            sections,
        } = raw;
        if prefix.nonce != *nonce.as_bytes() {
            return Err(malformed("invalid worker response context").into());
        }
        validate_plan(
            &self.common.config,
            class,
            ReceivePlan {
                class,
                section_lengths: prefix.lengths,
                decode_budget_bytes: response_decode_bytes,
            },
        )?;
        if prefix.status == 1 {
            if prefix.lengths.iter().any(|&length| length != 0) {
                return Err(malformed("worker error has unexpected sections").into());
            }
            let _scratch = self
                .common
                .shared
                .reserve_scratch(class.byte_class(), header_decode_bytes(class, header.len()))?;
            let error = serde_json::from_slice(header.as_slice())
                .map_err(|_| malformed("invalid worker error"))?;
            return Ok(Err(error));
        }
        let scratch = Arc::new(
            self.common
                .shared
                .reserve_scratch(class.byte_class(), response_decode_bytes)?,
        );
        Ok(Ok((ChargedReply { header, sections }, scratch)))
    }
    /// TLS connections this caller has opened, sessions and one-shot exchanges.
    #[cfg(test)]
    pub(crate) fn connections_for_test(&self) -> u64 {
        self.common.sessions.connections()
    }
    #[cfg(test)]
    pub(crate) fn live_sessions_for_test(&self) -> usize {
        self.common.sessions.live_for_test()
    }
    #[cfg(test)]
    pub(crate) fn waiting_streams_for_test(&self) -> usize {
        self.common.sessions.waiting_for_test()
    }
    /// Close every caller session, as if each connection failed at once.
    #[cfg(test)]
    pub(crate) fn close_sessions_for_test(&self) {
        self.common.sessions.close_all();
    }
    /// The locally applied Control enrollment revision, if the directory has one.
    pub(crate) fn applied_revision(&self) -> Option<u64> {
        self.common.directory.applied_revision()
    }
    /// The peer's current incarnation when this caller's live session to it on
    /// `class` negotiated a protocol with `feature`. `None` when there is no
    /// such session yet, or the peer speaks only the one-shot protocol.
    /// Send the feature's frames to the returned identity only, so a restarted
    /// (possibly older) incarnation never receives them.
    pub(crate) fn peer_supports(
        &self,
        worker: Uuid,
        class: TrafficClass,
        feature: PeerFeature,
    ) -> Option<WorkerIdentity> {
        let peer = self.common.directory.resolve(worker).ok()?;
        self.common
            .sessions
            .supports(&peer, class, feature)
            .then(|| WorkerIdentity::from(&peer))
    }
    /// Local lane refusals since start, for `/v1/runtime/stats`.
    pub(crate) fn lane_refusals(&self) -> serde_json::Value {
        self.common.refusals.snapshot()
    }
    /// The largest request payload this caller sends on `class`.
    pub(crate) fn payload_limit(&self, class: TrafficClass) -> usize {
        self.common.config.payload_bytes[class as usize]
    }

    /// One caller slot on the lane to `target` for `class`, or a local
    /// `OutgoingLaneFull` refusal before anything is written.
    fn try_outgoing(
        &self,
        target: Uuid,
        class: TrafficClass,
    ) -> Result<OutgoingSlot, RequestError> {
        self.try_outgoing_slot(target, class).inspect_err(|_| {
            self.common.refusals.outgoing(class);
        })
    }
    /// `try_outgoing` for a caller that waits instead when the lane is full.
    fn try_outgoing_slot(
        &self,
        target: Uuid,
        class: TrafficClass,
    ) -> Result<OutgoingSlot, RequestError> {
        let full = || {
            #[cfg(test)]
            self.common.outgoing_lane_full[class as usize].notify_one();
            RequestError::OutgoingLaneFull
        };
        #[cfg(test)]
        let held = self.common.outgoing_hold[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| full())?;
        let slot = self
            .common
            .outgoing
            .lane(target, class, &self.common.config)
            .try_acquire_owned()
            .map_err(|_| full())?;
        let class_slot = self.common.outgoing_class[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| full())?;
        Ok(OutgoingSlot {
            _slot: slot,
            _class: class_slot,
            #[cfg(test)]
            _held: held,
        })
    }

    /// Closes `class` to every peer until the permit drops. The class must be idle.
    #[cfg(test)]
    pub(crate) fn hold_outgoing_for_test(&self, class: TrafficClass) -> OwnedSemaphorePermit {
        self.common.outgoing_hold[class as usize]
            .clone()
            .try_acquire_many_owned(TEST_HOLD_PERMITS)
            .expect("outgoing lane is idle")
    }

    /// Takes `slots` caller slots of the lane to `target` for `class`.
    #[cfg(test)]
    pub(crate) fn take_outgoing_for_test(
        &self,
        target: Uuid,
        class: TrafficClass,
        slots: u32,
    ) -> OwnedSemaphorePermit {
        self.common
            .outgoing
            .lane(target, class, &self.common.config)
            .try_acquire_many_owned(slots)
            .expect("outgoing lane has the slots")
    }

    #[cfg(test)]
    pub(crate) async fn outgoing_lane_full_for_test(&self, class: TrafficClass) {
        self.common.outgoing_lane_full[class as usize]
            .notified()
            .await;
    }
}

#[cfg(test)]
const TEST_HOLD_PERMITS: u32 = 1 << 20;

/// Peer lanes' worth of caller slots one class may use in total. A paused
/// peer holds at most its own lane (one handler capacity), so the others keep
/// the rest (LIVE1), while the total in flight per class stays bounded by a
/// constant instead of growing with the number of peers (RV-FINAL L1, M02).
pub(crate) const OUTGOING_CLASS_LANES: usize = 4;
fn outgoing_class_slots(handler_slots: usize) -> usize {
    handler_slots
        .saturating_mul(OUTGOING_CLASS_LANES)
        .min(Semaphore::MAX_PERMITS)
}

/// A caller slot taken ahead of its request by
/// [`WorkerClient::reserve_outgoing`]. Dropping it frees the slot.
pub(crate) struct OutgoingReservation {
    target: Uuid,
    class: TrafficClass,
    slot: OutgoingSlot,
    /// The waiting callers' share of the lane and the class, held with the
    /// slot. A request moves `slot` out; dropping the rest frees the share.
    _waiting: (OwnedSemaphorePermit, OwnedSemaphorePermit),
}

/// A caller slot on one peer lane and in its class. Dropping it frees both.
struct OutgoingSlot {
    _slot: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
    #[cfg(test)]
    _held: OwnedSemaphorePermit,
}

/// Caller slots per peer lane, created on first use. Each lane has the
/// receiver's handler capacity for its class, so a caller never has more
/// streams in flight to one receiver than that receiver's handler admits.
#[derive(Default)]
struct OutgoingLanes {
    lanes: Mutex<std::collections::HashMap<Uuid, Arc<[Arc<Semaphore>; CLASSES]>>>,
    /// Slots each lane keeps back from its handler capacity (at least one
    /// slot stays).
    headroom: usize,
}
impl OutgoingLanes {
    /// Lanes are kept per worker ID, not per incarnation. Idle lanes of
    /// departed workers are dropped once the map exceeds this many peers.
    const PRUNE_AT: usize = 1024;

    fn lane(&self, worker: Uuid, class: TrafficClass, config: &TransportConfig) -> Arc<Semaphore> {
        let mut lanes = self
            .lanes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let capacity = |slots: usize| slots.saturating_sub(self.headroom).max(1);
        if !lanes.contains_key(&worker) && lanes.len() >= Self::PRUNE_AT {
            lanes.retain(|_, lane| {
                lane.iter()
                    .zip(&config.handler_slots)
                    .any(|(slots, &handlers)| slots.available_permits() != capacity(handlers))
            });
        }
        lanes.entry(worker).or_insert_with(|| {
            Arc::new(std::array::from_fn(|i| {
                Arc::new(Semaphore::new(capacity(config.handler_slots[i])))
            }))
        })[class as usize]
            .clone()
    }
}

fn traffic(group: &GroupIdentity, purpose: Purpose) -> Result<TrafficClass, ProtocolError> {
    let small = matches!(
        group.kind,
        GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. }
    );
    match purpose {
        Purpose::Raft => Ok(if small {
            TrafficClass::ControlConsensus
        } else {
            TrafficClass::DataConsensus
        }),
        Purpose::RaftProgress
            if matches!(group.kind, GroupKind::Data { .. } | GroupKind::Catalog) =>
        {
            Ok(TrafficClass::RaftProgress)
        }
        Purpose::Snapshot => Ok(if small {
            TrafficClass::ControlSnapshot
        } else {
            TrafficClass::Snapshot
        }),
        Purpose::Authority => Ok(if group.kind == GroupKind::Control {
            TrafficClass::Authority
        } else {
            TrafficClass::ServiceAuthority
        }),
        Purpose::QueueControl if group.kind == GroupKind::Control => Ok(TrafficClass::QueueControl),
        Purpose::QueueData if group.kind == GroupKind::Control => Ok(TrafficClass::QueueData),
        Purpose::Storage if !small => Ok(TrafficClass::Storage),
        // Data groups: `Method::Direct` from a gateway to the range leader.
        Purpose::ApiIngress
            if matches!(group.kind, GroupKind::Control | GroupKind::Data { .. }) =>
        {
            Ok(TrafficClass::ApiIngress)
        }
        Purpose::ApiObservation if group.kind == GroupKind::Control => {
            Ok(TrafficClass::ApiObservation)
        }
        Purpose::Settlement
            if matches!(group.kind, GroupKind::Data { .. } | GroupKind::Catalog) =>
        {
            Ok(TrafficClass::Settlement)
        }
        Purpose::ServiceMutation
            if matches!(group.kind, GroupKind::Timestamps | GroupKind::Status { .. }) =>
        {
            Ok(TrafficClass::ServiceMutation)
        }
        Purpose::GroupLifecycle if group.kind == GroupKind::Control => Ok(TrafficClass::Settlement),
        Purpose::ReplicaPlacement | Purpose::GroupDamage | Purpose::SystemRecovery
            if group.kind == GroupKind::Control => {
            Ok(TrafficClass::Settlement)
        }
        Purpose::ShardSplit if group.kind == GroupKind::Control => Ok(TrafficClass::Settlement),
        Purpose::ShardSplitProof if group.kind == GroupKind::Control => {
            Ok(TrafficClass::ServiceAuthority)
        }
        Purpose::ShardSplitImage if group.kind == GroupKind::Control => Ok(TrafficClass::Storage),
        Purpose::ControlDelegation if group.kind == GroupKind::Control => {
            Ok(TrafficClass::ControlDelegation)
        }
        _ => Err(malformed("RPC purpose does not match group kind")),
    }
}
fn header_limit(class: TrafficClass) -> usize {
    if class == TrafficClass::RaftProgress {
        RAFT_PROGRESS_HEADER_BYTES
    } else {
        MAX_HEADER_BYTES
    }
}
fn header_decode_bytes(class: TrafficClass, header_len: usize) -> usize {
    if class == TrafficClass::RaftProgress {
        RAFT_PROGRESS_DECODE_BYTES
    } else {
        // The fixed transport prefix caps header_len before this calculation.
        header_len * 32 + 4096
    }
}
fn validate_plan(
    config: &TransportConfig,
    class: TrafficClass,
    plan: ReceivePlan,
) -> Result<(), ProtocolError> {
    let length = plan
        .section_lengths
        .iter()
        .try_fold(0u64, |n, &length| n.checked_add(length))
        .ok_or_else(|| malformed("worker section lengths overflow"))?;
    if plan.class != class
        || length > config.payload_bytes[class as usize] as u64
        || plan.decode_budget_bytes > config.decode_bytes[class as usize]
        || class == TrafficClass::RaftProgress
            && plan.decode_budget_bytes > RAFT_PROGRESS_DECODE_BYTES
    {
        return Err(malformed("worker receive plan exceeds class limits"));
    }
    Ok(())
}
fn validate_reply(
    config: &TransportConfig,
    class: TrafficClass,
    reply: &ChargedReply,
) -> Result<(), ProtocolError> {
    if reply.header.len() > header_limit(class) {
        return Err(malformed("worker header exceeds limit"));
    }
    validate_plan(
        config,
        class,
        ReceivePlan {
            class,
            section_lengths: std::array::from_fn(|i| {
                reply.sections[i]
                    .as_ref()
                    .map_or(0, |bytes| bytes.len() as u64)
            }),
            decode_budget_bytes: 0,
        },
    )
}
async fn read_sections(
    stream: &mut (impl AsyncRead + Unpin),
    common: &Common,
    class: TrafficClass,
    plan: ReceivePlan,
) -> Result<ChargedSections, ProtocolError> {
    let scratch = Arc::new(
        common
            .shared
            .reserve_scratch(class.byte_class(), plan.decode_budget_bytes)?,
    );
    let mut sections = std::array::from_fn(|_| None);
    for (slot, length) in sections.iter_mut().zip(plan.section_lengths) {
        let transmitted = stream.read_u64().await.map_err(|_| unavailable())?;
        if transmitted != length {
            return Err(malformed("worker section length differs from preflight"));
        }
        if length != 0 {
            let mut bytes = common
                .shared
                .allocate_bytes(class.byte_class(), length as usize)?;
            stream
                .read_exact(bytes.as_mut_slice())
                .await
                .map_err(|_| unavailable())?;
            *slot = Some(bytes.freeze());
        }
    }
    Ok(ChargedSections {
        sections,
        scratch,
        resources: common.shared.clone(),
    })
}
async fn write_reply(
    stream: &mut (impl AsyncWrite + Unpin),
    nonce: Uuid,
    status: u8,
    reply: &ChargedReply,
) -> Result<(), ProtocolError> {
    stream.write_all(MAGIC).await.map_err(|_| unavailable())?;
    stream
        .write_all(nonce.as_bytes())
        .await
        .map_err(|_| unavailable())?;
    stream.write_u8(status).await.map_err(|_| unavailable())?;
    stream
        .write_u32(reply.header.len() as u32)
        .await
        .map_err(|_| unavailable())?;
    for section in &reply.sections {
        stream
            .write_u64(section.as_ref().map_or(0, |b| b.len() as u64))
            .await
            .map_err(|_| unavailable())?;
    }
    stream
        .write_all(reply.header.as_slice())
        .await
        .map_err(|_| unavailable())?;
    for section in reply.sections.iter().flatten() {
        stream
            .write_all(section.as_slice())
            .await
            .map_err(|_| unavailable())?;
    }
    stream.flush().await.map_err(|_| unavailable())
}

struct Hello {
    cluster_id: Uuid,
    group_id: Uuid,
    group_digest: Digest,
    purpose: Purpose,
    from: WorkerIdentity,
    to: WorkerIdentity,
    nonce: Uuid,
    header_len: usize,
}
impl Hello {
    fn encode(&self) -> [u8; PROLOGUE_BYTES] {
        let mut out = [0; PROLOGUE_BYTES];
        let mut offset = 0;
        let mut write = |bytes: &[u8]| {
            out[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
        };
        write(MAGIC);
        write(self.cluster_id.as_bytes());
        write(self.group_id.as_bytes());
        write(self.group_digest.as_bytes());
        write(&[self.purpose as u8]);
        for member in [self.from, self.to] {
            write(member.worker_id.as_bytes());
            write(member.incarnation.as_bytes());
            write(&member.enrollment_revision.to_be_bytes());
        }
        write(self.nonce.as_bytes());
        write(&(self.header_len as u32).to_be_bytes());
        out
    }
    fn decode(
        bytes: &[u8; PROLOGUE_BYTES],
        certificate: Digest,
        local_certificate: Digest,
    ) -> Result<Self, ProtocolError> {
        if &bytes[..8] != MAGIC {
            return Err(malformed("invalid worker prologue"));
        }
        let uuid = |start| Uuid::from_slice(&bytes[start..start + 16]).expect("fixed UUID bytes");
        let purpose = match bytes[72] {
            1 => Purpose::Raft,
            2 => Purpose::Snapshot,
            3 => Purpose::Authority,
            4 => Purpose::QueueControl,
            5 => Purpose::QueueData,
            6 => Purpose::Storage,
            7 => Purpose::ApiIngress,
            8 => Purpose::ApiObservation,
            9 => Purpose::Settlement,
            10 => Purpose::ServiceMutation,
            11 => Purpose::GroupLifecycle,
            12 => Purpose::ControlDelegation,
            13 => Purpose::ShardSplit,
            14 => Purpose::ShardSplitProof,
            15 => Purpose::ShardSplitImage,
            16 => Purpose::ReplicaPlacement,
            17 => Purpose::RaftProgress,
            18 => Purpose::GroupDamage,
            19 => Purpose::SystemRecovery,
            _ => return Err(malformed("unknown worker RPC purpose")),
        };
        let member = |start| WorkerIdentity {
            worker_id: uuid(start),
            incarnation: uuid(start + 16),
            enrollment_revision: u64::from_be_bytes(
                bytes[start + 32..start + 40]
                    .try_into()
                    .expect("fixed enrollment revision"),
            ),
            certificate_sha256: certificate,
        };
        let mut from = member(73);
        let mut to = member(113);
        // The target certificate is this listener's own configured identity;
        // it is not a caller-controlled certificate claim on the wire.
        from.certificate_sha256 = certificate;
        to.certificate_sha256 = local_certificate;
        let header_len =
            u32::from_be_bytes(bytes[169..173].try_into().expect("fixed header size")) as usize;
        let nonce = uuid(153);
        let max_header = if purpose == Purpose::RaftProgress {
            RAFT_PROGRESS_HEADER_BYTES
        } else {
            MAX_HEADER_BYTES
        };
        if header_len > max_header || nonce.is_nil() {
            return Err(malformed("invalid worker header size or nonce"));
        }
        from.validate()
            .map_err(|_| malformed("invalid sender identity"))?;
        to.validate()
            .map_err(|_| malformed("invalid audience identity"))?;
        Ok(Self {
            cluster_id: uuid(8),
            group_id: uuid(24),
            group_digest: Digest::from_bytes(bytes[40..72].try_into().expect("fixed group digest")),
            purpose,
            from,
            to,
            nonce,
            header_len,
        })
    }
}
fn identity_digest(group: &GroupIdentity) -> Result<Digest, ProtocolError> {
    struct HashWriter(Sha256);
    impl io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    writer.0.update(b"nabedra/oltp/group-wire/v1\0");
    serde_json::to_writer(&mut writer, group)
        .map_err(|_| malformed("invalid group fingerprint"))?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}
fn peer_digest(
    certificates: Option<&[rustls::pki_types::CertificateDer<'_>]>,
) -> Result<Digest, ProtocolError> {
    certificates
        .and_then(|certs| certs.first())
        .map(|certificate| Digest::from_bytes(Sha256::digest(certificate.as_ref()).into()))
        .ok_or_else(|| malformed("missing worker certificate"))
}
/// `TCP_NODELAY`, and TCP keepalive so the kernel also notices a dead peer
/// on a connection that carries no traffic.
fn tune_socket(socket: &TcpStream) {
    let _ = socket.set_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_interval(Duration::from_secs(5));
    let _ = socket2::SockRef::from(socket).set_tcp_keepalive(&keepalive);
}
fn overloaded(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Overloaded, message, Outcome::NotApplied)
}
/// A local failure after the request reached the peer's socket (reading,
/// charging or checking its reply) says nothing about whether the peer
/// executed it (G1). The peer's own typed refusal is still definitive.
fn sent_request_failed(error: RequestError) -> RequestError {
    match error {
        RequestError::Protocol(mut error) => {
            error.outcome = Outcome::Unknown;
            RequestError::Protocol(error)
        }
        error => error,
    }
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker transport unavailable",
        Outcome::Unknown,
    )
}
