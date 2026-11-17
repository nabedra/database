use super::*;
use crate::oltp_replication::{
    TlsMaterial,
    types::{Digest, GroupKind},
};
use crate::{oltp::pool::WorkerSharedResources, scheduling::MemoryCharge};
use rustls::pki_types::ServerName;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, io, sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

// Engine capability is fixed in the authenticated protocol, before variable
// metadata. An OLAP worker does not implement this listener or enrollment.
const ALPN: &[u8] = b"nabedra-oltp-worker/1";
const MAGIC: &[u8; 8] = b"NABOTP01";
const PROLOGUE_BYTES: usize = 173;
const CLASSES: usize = 7;

/// Applied enrollment authenticates only transport identity. Application
/// endpoints still require fresh authority in preflight. Control Raft trust
/// never performs a quorum-dependent lookup through its own transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTrust {
    AppliedEnrollment,
    RecoveryControlRaft,
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
            preflight_slots: [4, 4, 4, 8, 4, 2, 4],
            handler_slots: [2, 2, 4, 8, 4, 1, 4],
            payload_bytes: [
                128 << 10,
                8 << 20,
                64 << 10,
                16 << 10,
                2 * crate::protocol::MAX_BODY_BYTES + 4096,
                64 << 10,
                16 << 20,
            ],
            decode_bytes: [
                512 << 10,
                16 << 20,
                128 << 10,
                32 << 10,
                16 << 20,
                64 << 10,
                32 << 20,
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
                .chain(&self.decode_bytes)
                .any(|&n| n == 0)
            || self.deadline.is_zero()
        {
            return Err(malformed("invalid worker transport limits"));
        }
        Ok(())
    }
}

struct Residency {
    identity: GroupIdentity,
    _slot: OwnedSemaphorePermit,
    _bytes: MemoryCharge,
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
}
const REPLICA_METADATA_BYTES: usize = 8 << 20;
struct ReplicaResidency {
    group: ResidentLease,
    node_id: u64,
    _slot: OwnedSemaphorePermit,
    _bytes: MemoryCharge,
    metadata_used: std::sync::atomic::AtomicUsize,
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
        use std::sync::atomic::Ordering;
        let mut used = self.0.metadata_used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|n| *n <= REPLICA_METADATA_BYTES / 2)
                .ok_or_else(|| overloaded("replica snapshot metadata capacity exhausted"))?;
            match self.0.metadata_used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ReplicaMetadata {
                        owner: self.clone(),
                        bytes,
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
}
impl Drop for ReplicaMetadata {
    fn drop(&mut self) {
        self.owner
            .0
            .metadata_used
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
        replicas.retain(|_, replica| replica.strong_count() != 0);
        if replicas
            .get(&node_id)
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            return Err(malformed("replica node is already live"));
        }
        let slot = shared
            .reserve_replica_residency()
            .map_err(|_| overloaded("worker active Raft replica capacity exhausted"))?;
        let bytes = shared
            .scheduler()
            .charge_memory(shared.scheduler_node_id(), REPLICA_METADATA_BYTES as u64)?;
        let replica = Arc::new(ReplicaResidency {
            group: self.lease(),
            node_id,
            _slot: slot,
            _bytes: bytes,
            metadata_used: std::sync::atomic::AtomicUsize::new(0),
            ownership: std::sync::OnceLock::new(),
        });
        replicas.insert(node_id, Arc::downgrade(&replica));
        Ok(ReplicaLease(replica))
    }
    pub fn bind(&self, purpose: Purpose, endpoint: Arc<dyn Endpoint>) -> Result<(), ProtocolError> {
        traffic(self.lease().identity(), purpose)?;
        let mut endpoints = self.0.endpoints.lock().map_err(|_| unavailable())?;
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
}
impl EndpointRegistry {
    pub fn new(shared: Arc<WorkerSharedResources>) -> Self {
        Self {
            shared,
            groups: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn reserve(&self, identity: GroupIdentity) -> Result<ResidentGroup, ProtocolError> {
        identity
            .validate()
            .map_err(|_| malformed("invalid resident group identity"))?;
        let mut groups = self.groups.lock().map_err(|_| unavailable())?;
        if let Some(existing) = groups.get(&identity.group_id) {
            if existing.lease.identity() != &identity {
                return Err(malformed("resident group identity changed"));
            }
            return Ok(ResidentGroup(existing.clone()));
        }
        let slot = self
            .shared
            .reserve_group_residency()
            .map_err(|_| overloaded("worker resident-group placement capacity exhausted"))?;
        let bytes = self
            .shared
            .scheduler()
            .charge_memory(self.shared.scheduler_node_id(), 64 << 10)?;
        let binding = Arc::new(Binding {
            lease: ResidentLease(Arc::new(Residency {
                identity,
                _slot: slot,
                _bytes: bytes,
                shared: self.shared.clone(),
            })),
            endpoints: Mutex::new(BTreeMap::new()),
            replicas: Mutex::new(BTreeMap::new()),
        });
        groups.insert(binding.lease.identity().group_id, binding.clone());
        Ok(ResidentGroup(binding))
    }
    fn lookup(
        &self,
        id: Uuid,
        purpose: Purpose,
    ) -> Result<(ResidentLease, Arc<dyn Endpoint>), ProtocolError> {
        let groups = self.groups.lock().map_err(|_| unavailable())?;
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
}
pub struct WorkerTransport {
    common: Arc<Common>,
    registry: Arc<EndpointRegistry>,
}
#[derive(Clone)]
pub struct WorkerClient {
    common: Arc<Common>,
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
        let (lease, endpoint) = self.registry.lookup(hello.group_id, hello.purpose)?;
        let group = lease.identity();
        if hello.cluster_id != group.cluster_id
            || hello.group_digest != identity_digest(group)?
            || hello.to != self.common.local
        {
            return Err(malformed("foreign worker/group audience"));
        }
        let trust = self
            .common
            .directory
            .verify(hello.from, group, hello.purpose)?;
        if trust == TransportTrust::RecoveryControlRaft
            && !(group.kind == GroupKind::Control && hello.purpose == Purpose::Raft)
        {
            return Err(malformed("recovery trust permits only Control Raft"));
        }
        let class = traffic(group, hello.purpose)?;
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
            recovery_only: trust == TransportTrust::RecoveryControlRaft,
        };
        let result = async {
            let header_decode_bytes = header
                .len()
                .checked_mul(32)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| malformed("header decode budget overflow"))?;
            let header_scratch = self
                .common
                .shared
                .reserve_scratch(class.byte_class(), header_decode_bytes)?;
            let plan = endpoint.preflight(&session, header.as_slice()).await?;
            drop(header_scratch);
            validate_plan(&self.common.config, class, plan)?;
            let handler = self.common.handlers[class as usize]
                .clone()
                .try_acquire_owned()
                .map_err(|_| overloaded("worker handler lane full"))?;
            let sections = read_sections(&mut stream, &self.common, class, plan).await?;
            drop(preflight);
            let reply = endpoint.handle(session, header, sections).await?;
            validate_reply(&self.common.config, class, &reply)?;
            let _handler = handler;
            write_reply(&mut stream, hello.nonce, 0, &reply).await
        }
        .await;
        if let Err(error) = result {
            let encoded = self
                .common
                .shared
                .encode_in(ByteClass::Progress, &error, 2048)?;
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
        Ok(())
    }
}

impl WorkerClient {
    pub async fn request(
        &self,
        group: &GroupIdentity,
        target: Uuid,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, ProtocolError> {
        timeout(
            self.common.config.deadline,
            self.exchange(
                group,
                target,
                purpose,
                header,
                sections,
                response_decode_bytes,
            ),
        )
        .await
        .map_err(|_| unavailable())?
    }
    async fn exchange(
        &self,
        group: &GroupIdentity,
        target: Uuid,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        response_decode_bytes: usize,
    ) -> Result<ReceivedReply, ProtocolError> {
        group
            .validate()
            .map_err(|_| malformed("invalid target group"))?;
        let class = traffic(group, purpose)?;
        let _slot = self.common.outgoing[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded("worker outgoing lane full"))?;
        let peer = self.common.directory.resolve(target)?;
        peer.validate()
            .map_err(|_| malformed("invalid target enrollment"))?;
        let trust = self
            .common
            .directory
            .verify(WorkerIdentity::from(&peer), group, purpose)?;
        if trust == TransportTrust::RecoveryControlRaft
            && !(group.kind == GroupKind::Control && purpose == Purpose::Raft)
        {
            return Err(malformed("recovery trust permits only Control Raft"));
        }
        let name = ServerName::try_from(peer.server_name.as_str().to_owned())
            .map_err(|_| malformed("invalid target TLS name"))?;
        let socket = TcpStream::connect(peer.endpoint.as_str())
            .await
            .map_err(|_| unavailable())?;
        let mut stream = self
            .common
            .connector
            .connect(name, socket)
            .await
            .map_err(|_| unavailable())?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN)
            || peer_digest(stream.get_ref().1.peer_certificates())? != peer.certificate_sha256
        {
            return Err(malformed("target certificate or ALPN differs"));
        }
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
            to: WorkerIdentity::from(&peer),
            nonce,
            header_len: header.len(),
        };
        stream
            .write_all(&hello.encode())
            .await
            .map_err(|_| unavailable())?;
        stream
            .write_all(header.as_slice())
            .await
            .map_err(|_| unavailable())?;
        for section in &sections {
            let bytes = section.as_ref().map_or(&[][..], ChargedBytes::as_slice);
            stream
                .write_u64(bytes.len() as u64)
                .await
                .map_err(|_| unavailable())?;
            stream.write_all(bytes).await.map_err(|_| unavailable())?;
        }
        stream.flush().await.map_err(|_| unavailable())?;
        let mut prefix = [0; 53];
        stream
            .read_exact(&mut prefix)
            .await
            .map_err(|_| unavailable())?;
        if &prefix[..8] != MAGIC || &prefix[8..24] != nonce.as_bytes() || prefix[24] > 1 {
            return Err(malformed("invalid worker response context"));
        }
        let header_len =
            u32::from_be_bytes(prefix[25..29].try_into().expect("fixed response header")) as usize;
        if header_len > MAX_HEADER_BYTES {
            return Err(malformed("worker response header exceeds limit"));
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
            .map_err(|_| unavailable())?;
        let response_header = response_header.freeze();
        if prefix[24] == 1 {
            if lengths.iter().any(|&length| length != 0) {
                return Err(malformed("worker error has unexpected sections"));
            }
            let _scratch = self.common.shared.reserve_scratch(
                class.byte_class(),
                response_header
                    .len()
                    .checked_mul(32)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(|| malformed("error decode budget overflow"))?,
            )?;
            return Err(serde_json::from_slice(response_header.as_slice())
                .map_err(|_| malformed("invalid worker error"))?);
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
                    .map_err(|_| unavailable())?;
                *slot = Some(bytes.freeze());
            }
        }
        Ok(ReceivedReply {
            reply: ChargedReply {
                header: response_header,
                sections: response_sections,
            },
            scratch,
            session: AuthenticatedSession {
                local: self.common.local,
                remote: WorkerIdentity::from(&peer),
                group: group.clone(),
                purpose,
                nonce,
                recovery_only: trust == TransportTrust::RecoveryControlRaft,
            },
            resources: self.common.shared.clone(),
        })
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
        Purpose::Snapshot => Ok(TrafficClass::Snapshot),
        Purpose::Authority if small => Ok(TrafficClass::Authority),
        Purpose::QueueControl if group.kind == GroupKind::Control => Ok(TrafficClass::QueueControl),
        Purpose::QueueData if group.kind == GroupKind::Control => Ok(TrafficClass::QueueData),
        Purpose::Storage if !small => Ok(TrafficClass::Storage),
        _ => Err(malformed("RPC purpose does not match group kind")),
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
    if reply.header.len() > MAX_HEADER_BYTES {
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
        if header_len > MAX_HEADER_BYTES || nonce.is_nil() {
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
