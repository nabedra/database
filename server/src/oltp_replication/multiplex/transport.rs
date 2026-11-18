use super::*;
use crate::oltp_replication::{
    TlsMaterial,
    types::{Digest, GroupKind},
};
use crate::oltp::pool::{GroupCapacityLease, RAFT_REPLICA_BYTES, WorkerSharedResources};
use rustls::pki_types::ServerName;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
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
use resolve::{PeerResolver, ResolveError};
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
    fn validate_prepared(self, group: &GroupIdentity, purpose: Purpose) -> Result<(), ProtocolError> {
        if self == Self::PreparedControl && (
            group.kind != GroupKind::Control
            || purpose != Purpose::ControlDelegation
        ) {
            return Err(malformed("prepared identity permits only Control recovery"));
        }
        Ok(())
    }
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
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub handshake_slots: usize,
    pub preflight_slots: [usize; CLASSES],
    pub handler_slots: [usize; CLASSES],
    pub payload_bytes: [usize; CLASSES],
    pub decode_bytes: [usize; CLASSES],
    pub deadline: Duration,
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
                .any(|&n| n == 0 || n > Semaphore::MAX_PERMITS)
            || self
                .payload_bytes
                .iter()
                .enumerate()
                .any(|(i, &n)| (n == 0) != (i == TrafficClass::RaftProgress as usize))
            || self.decode_bytes.contains(&0)
            || self.deadline.is_zero()
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
    config: TransportConfig,
    handshakes: Arc<Semaphore>,
    preflight: [Arc<Semaphore>; CLASSES],
    handlers: [Arc<Semaphore>; CLASSES],
    outgoing: [Arc<Semaphore>; CLASSES],
    authority_roles: Semaphore,
    authority_waiters: Semaphore,
    resolver: PeerResolver,
    #[cfg(test)]
    outgoing_lane_full: [tokio::sync::Notify; CLASSES],
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
}
impl RequestTarget {
    pub(crate) fn worker_id(self) -> Uuid {
        match self {
            Self::WorkerId(id) => id,
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
        let (connector, acceptor) = tls.transports(ALPN);
        Ok(Self {
            common: Arc::new(Common {
                local: WorkerIdentity::from(local),
                shared,
                directory,
                connector,
                acceptor,
                handshakes: Arc::new(Semaphore::new(config.handshake_slots)),
                preflight: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.preflight_slots[i]))
                }),
                handlers: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.handler_slots[i]))
                }),
                outgoing: std::array::from_fn(|i| {
                    Arc::new(Semaphore::new(config.handler_slots[i]))
                }),
                authority_roles: Semaphore::new(
                    config.handler_slots[TrafficClass::Authority as usize] - 1,
                ),
                authority_waiters: Semaphore::new(
                    config.preflight_slots[TrafficClass::Authority as usize],
                ),
                resolver: PeerResolver::default(),
                #[cfg(test)]
                outgoing_lane_full: std::array::from_fn(|_| tokio::sync::Notify::new()),
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
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _=&mut shutdown=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                accepted=listener.accept()=>{
                    let (socket,_)=accepted.map_err(|_| unavailable())?;
                    let Ok(permit)=self.common.handshakes.clone().try_acquire_owned() else {drop(socket);continue};
                    let owner=self.clone();
                    tasks.spawn(async move {let _=timeout(owner.common.config.deadline,owner.serve(socket,permit)).await;});
                }
            }
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
    async fn serve(
        &self,
        socket: TcpStream,
        classification: OwnedSemaphorePermit,
    ) -> Result<(), ProtocolError> {
        let mut stream = self
            .common
            .acceptor
            .accept(socket)
            .await
            .map_err(|_| unavailable())?;
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
            || hello.to != self.common.local
        {
            return Err(malformed("foreign worker/group audience"));
        }
        trace.phase = RaftTransportPhase::AuthorizePeer;
        let trust = self
            .common
            .directory
            .verify(hello.from, group, hello.purpose)?;
        trust.validate_prepared(group, hello.purpose)?;
        if trust == TransportTrust::BootstrapControl
            && (group.kind != GroupKind::Control
                || !matches!(
                    hello.purpose,
                    Purpose::Raft | Purpose::Authority | Purpose::ControlDelegation
                ))
        {
            return Err(malformed(
                "bootstrap trust permits only Control consensus and enrollment reads",
            ));
        }
        if trust == TransportTrust::RecoveryControlRaft
            && !(group.kind == GroupKind::Control && hello.purpose == Purpose::Raft)
        {
            return Err(malformed("recovery trust permits only Control Raft"));
        }
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
                .map_err(|_| overloaded("worker handler lane full"))?;
            trace.phase = RaftTransportPhase::ReadSections;
            let sections = read_sections(&mut stream, &self.common, class, plan).await?;
            drop(preflight);
            trace.phase = RaftTransportPhase::Handle;
            let reply = endpoint.handle(session, header, sections).await?;
            validate_reply(&self.common.config, class, &reply)?;
            let _handler = handler;
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
        )
        .await
    }
    async fn request_with_timeout(
        &self,
        group: &GroupIdentity,
        target: RequestTarget,
        (purpose, probe_timeout): (Purpose, Option<Duration>),
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, RequestError> {
        group
            .validate()
            .map_err(|_| malformed("invalid target group"))?;
        let class = traffic(group, purpose)?;
        let _slot = self.common.outgoing[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                #[cfg(test)]
                self.common.outgoing_lane_full[class as usize].notify_one();
                RequestError::OutgoingLaneFull
            })?;
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
        let _slots = self.authority_slots(deadline).await?;
        if Instant::now() >= deadline {
            return Err(RequestError::OutgoingLaneFull);
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
        deadline: Instant,
    ) -> Result<(Option<SemaphorePermit<'_>>, OwnedSemaphorePermit), RequestError> {
        let index = TrafficClass::Authority as usize;
        let outgoing = &self.common.outgoing[index];
        let roles = &self.common.authority_roles;
        // Keep an existing slot available to ordinary Authority traffic.
        // A one-slot lane has no capacity to reserve and keeps immediate admission.
        if self.common.config.handler_slots[index] == 1 {
            return outgoing
                .clone()
                .try_acquire_owned()
                .map(|slot| (None, slot))
                .map_err(|_| RequestError::OutgoingLaneFull);
        }
        if let Ok(slots) = roles.try_acquire().and_then(|role| {
            outgoing
                .clone()
                .try_acquire_owned()
                .map(|slot| (Some(role), slot))
        }) {
            return Ok(slots);
        }
        let _waiter = self
            .common
            .authority_waiters
            .try_acquire()
            .map_err(|_| RequestError::OutgoingLaneFull)?;
        let acquire = timeout_at(deadline, async {
            let role = roles.acquire().await?;
            let slot = outgoing.clone().acquire_owned().await?;
            Ok::<_, tokio::sync::AcquireError>((Some(role), slot))
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
        let (worker, expected) = match target {
            RequestTarget::WorkerId(worker) => (worker, None),
            RequestTarget::Exact(identity) => (identity.worker_id, Some(identity)),
        };
        let peer = self.common.directory.resolve(worker)?;
        peer.validate()
            .map_err(|_| malformed("invalid target enrollment"))?;
        let identity = WorkerIdentity::from(&peer);
        if expected.is_some_and(|expected| expected != identity) {
            return Err(malformed("target incarnation differs from cached enrollment").into());
        }
        let trust = self.common.directory.verify(identity, group, purpose)?;
        trust.validate_prepared(group, purpose)?;
        if trust == TransportTrust::BootstrapControl
            && (group.kind != GroupKind::Control
                || !matches!(
                    purpose,
                    Purpose::Raft | Purpose::Authority | Purpose::ControlDelegation
                ))
        {
            return Err(malformed(
                "bootstrap trust permits only Control consensus and enrollment reads",
            )
            .into());
        }
        if trust == TransportTrust::RecoveryControlRaft
            && !(group.kind == GroupKind::Control && purpose == Purpose::Raft)
        {
            return Err(malformed("recovery trust permits only Control Raft").into());
        }
        let name = ServerName::try_from(peer.server_name.as_str().to_owned())
            .map_err(|_| malformed("invalid target TLS name"))?;
        let socket = if let Ok(address) = peer.endpoint.parse::<SocketAddr>() {
            trace.phase = RaftTransportPhase::Connect;
            TcpStream::connect(address).await
        } else {
            trace.phase = RaftTransportPhase::ResolveAddress;
            if let Some(resolving) = probe_resolving.as_deref_mut() {
                *resolving = true;
            }
            let addresses = self
                .common
                .resolver
                .resolve(identity, &peer.endpoint, class)
                .await
                .map_err(|error| match error {
                    ResolveError::LocallyUnavailable if probe_resolving.is_some() => {
                        unavailable().into()
                    }
                    _ => RequestError::Unreachable,
                })?;
            if let Some(resolving) = probe_resolving {
                *resolving = false;
            }
            trace.phase = RaftTransportPhase::Connect;
            TcpStream::connect(addresses.as_ref()).await
        }
        .map_err(|_| RequestError::Unreachable)?;
        trace.phase = RaftTransportPhase::Tls;
        let mut stream = self
            .common
            .connector
            .connect(name, socket)
            .await
            .map_err(|_| RequestError::Unreachable)?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN)
            || peer_digest(stream.get_ref().1.peer_certificates())? != peer.certificate_sha256
        {
            return Err(malformed("target certificate or ALPN differs").into());
        }
        trace.phase = RaftTransportPhase::SendRequest;
        let nonce = Uuid::new_v4();
        validate_reply(
            &self.common.config,
            class,
            &ChargedReply {
                header: header.clone(),
                sections: sections.clone(),
            },
        )?;
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
        stream
            .write_all(&hello.encode())
            .await
            .map_err(|_| RequestError::Unreachable)?;
        stream
            .write_all(header.as_slice())
            .await
            .map_err(|_| RequestError::Unreachable)?;
        for section in &sections {
            let bytes = section.as_ref().map_or(&[][..], ChargedBytes::as_slice);
            stream
                .write_u64(bytes.len() as u64)
                .await
                .map_err(|_| RequestError::Unreachable)?;
            stream
                .write_all(bytes)
                .await
                .map_err(|_| RequestError::Unreachable)?;
        }
        stream
            .flush()
            .await
            .map_err(|_| RequestError::Unreachable)?;
        trace.phase = RaftTransportPhase::ReceiveReply;
        let mut prefix = [0; 53];
        stream
            .read_exact(&mut prefix)
            .await
            .map_err(|_| RequestError::Unreachable)?;
        if &prefix[..8] != MAGIC || &prefix[8..24] != nonce.as_bytes() || prefix[24] > 1 {
            return Err(malformed("invalid worker response context").into());
        }
        let header_len =
            u32::from_be_bytes(prefix[25..29].try_into().expect("fixed response header")) as usize;
        if header_len > header_limit(class) {
            return Err(malformed("worker response header exceeds limit").into());
        }
        let lengths = std::array::from_fn(|i| {
            u64::from_be_bytes(
                prefix[29 + i * 8..37 + i * 8]
                    .try_into()
                    .expect("fixed response length"),
            )
        });
        let plan = ReceivePlan {
            class,
            section_lengths: lengths,
            decode_budget_bytes: response_decode_bytes,
        };
        validate_plan(&self.common.config, class, plan)?;
        let mut response_header = self
            .common
            .shared
            .allocate_bytes(class.byte_class(), header_len)?;
        stream
            .read_exact(response_header.as_mut_slice())
            .await
            .map_err(|_| RequestError::Unreachable)?;
        let response_header = response_header.freeze();
        if prefix[24] == 1 {
            if lengths.iter().any(|&length| length != 0) {
                return Err(malformed("worker error has unexpected sections").into());
            }
            let _scratch = self.common.shared.reserve_scratch(
                class.byte_class(),
                header_decode_bytes(class, response_header.len()),
            )?;
            let error = serde_json::from_slice(response_header.as_slice())
                .map_err(|_| malformed("invalid worker error"))?;
            trace.phase = RaftTransportPhase::Complete;
            return Err(RequestError::PeerRejected {
                worker: identity,
                error,
            });
        }
        let scratch = Arc::new(
            self.common
                .shared
                .reserve_scratch(class.byte_class(), response_decode_bytes)?,
        );
        let mut response_sections = std::array::from_fn(|_| None);
        for (slot, length) in response_sections.iter_mut().zip(lengths) {
            if length != 0 {
                let mut bytes = self
                    .common
                    .shared
                    .allocate_bytes(class.byte_class(), length as usize)?;
                stream
                    .read_exact(bytes.as_mut_slice())
                    .await
                    .map_err(|_| RequestError::Unreachable)?;
                *slot = Some(bytes.freeze());
            }
        }
        trace.phase = RaftTransportPhase::Complete;
        Ok(ReceivedReply {
            reply: ChargedReply {
                header: response_header,
                sections: response_sections,
            },
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
        })
    }

    #[cfg(test)]
    pub(crate) fn hold_outgoing_for_test(&self, class: TrafficClass) -> OwnedSemaphorePermit {
        self.common.outgoing[class as usize]
            .clone()
            .try_acquire_many_owned(self.common.config.handler_slots[class as usize] as u32)
            .expect("outgoing lane is idle")
    }

    #[cfg(test)]
    pub(crate) async fn outgoing_lane_full_for_test(&self, class: TrafficClass) {
        self.common.outgoing_lane_full[class as usize]
            .notified()
            .await;
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
        Purpose::ApiIngress if group.kind == GroupKind::Control => Ok(TrafficClass::ApiIngress),
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
        Purpose::ReplicaPlacement if group.kind == GroupKind::Control => Ok(TrafficClass::Settlement),
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
fn overloaded(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Overloaded, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker transport unavailable",
        Outcome::Unknown,
    )
}
