//! One authenticated worker listener routes many groups and RPC purposes.
//!
//! Endpoint preflight receives a bounded header before any payload/native/data
//! permits. Control consensus and authority traffic have independent capacity.
use nabedra_oltp_types::types::{GroupIdentity, WorkerIdentity};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError, json_input};
use nabedra_resources::{
    decode_budget::json_decode_budget,
    memory::{ByteClass, ChargedBytes, ScratchLease, WorkerMemory},
};
use serde::de::DeserializeOwned;
use std::{future::Future, ops::Deref, pin::Pin, sync::Arc};
use uuid::Uuid;

pub mod backend;
pub mod tls;
mod transport;

pub use transport::{
    EndpointRegistry, ONE_SHOT_ALPN, OutgoingReservation, PeerDirectory, PeerFeature, ReplicaLease,
    ReplicaMetadata, RequestError, RequestTarget, ResidentGroup, ResidentLease, TransportConfig,
    TransportTrust, WorkerClient, WorkerTransport,
};
#[cfg(feature = "sim")]
pub use transport::{FrameKind, StreamFrame, session_alpn};

/// The worker that owns the physical accounting used by this transport.
/// The owner retains native resources until the last transport handle is dropped.
pub trait ResourceOwner: Send + Sync + 'static {
    fn worker_memory(&self) -> &Arc<WorkerMemory>;

    fn bind_worker(self: &Arc<Self>, worker_id: Uuid) -> Result<(), ProtocolError>;
}

struct MemoryHandle {
    memory: Arc<WorkerMemory>,
    _owner: Arc<dyn Send + Sync>,
}

impl MemoryHandle {
    fn new(owner: Arc<impl ResourceOwner>) -> Arc<Self> {
        Arc::new(Self {
            memory: owner.worker_memory().clone(),
            _owner: owner,
        })
    }

    #[inline]
    fn belongs_to(&self, owner: &impl ResourceOwner) -> bool {
        Arc::ptr_eq(&self.memory, owner.worker_memory())
    }
}

impl Deref for MemoryHandle {
    type Target = WorkerMemory;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.memory
    }
}

pub const MAX_SECTIONS: usize = 3;
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
pub const RAFT_PROGRESS_HEADER_BYTES: usize = 1024;
pub const RAFT_PROGRESS_DECODE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Purpose {
    Raft = 1,
    Snapshot = 2,
    Authority = 3,
    QueueControl = 4,
    QueueData = 5,
    Storage = 6,
    ApiIngress = 7,
    ApiObservation = 8,
    Settlement = 9,
    ServiceMutation = 10,
    GroupLifecycle = 11,
    ControlDelegation = 12,
    ShardSplit = 13,
    ShardSplitProof = 14,
    ShardSplitImage = 15,
    ReplicaPlacement = 16,
    RaftProgress = 17,
    /// NS14b: Control's damage-replacement scheduler to a worker (Control scope).
    GroupDamage = 18,
    SystemRecovery = 19,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrafficClass {
    ControlConsensus,
    DataConsensus,
    Authority,
    QueueControl,
    QueueData,
    Snapshot,
    Storage,
    ControlSnapshot,
    ServiceAuthority,
    ApiIngress,
    ApiObservation,
    Settlement,
    ServiceMutation,
    ControlDelegation,
    RaftProgress,
}
impl TrafficClass {
    pub(crate) const ALL: [Self; 15] = [
        Self::ControlConsensus,
        Self::DataConsensus,
        Self::Authority,
        Self::QueueControl,
        Self::QueueData,
        Self::Snapshot,
        Self::Storage,
        Self::ControlSnapshot,
        Self::ServiceAuthority,
        Self::ApiIngress,
        Self::ApiObservation,
        Self::Settlement,
        Self::ServiceMutation,
        Self::ControlDelegation,
        Self::RaftProgress,
    ];
    #[inline]
    pub fn byte_class(self) -> ByteClass {
        match self {
            Self::ControlConsensus
            | Self::Authority
            | Self::QueueControl
            | Self::ControlSnapshot
            | Self::ServiceAuthority
            | Self::ApiObservation
            | Self::Settlement
            | Self::ServiceMutation
            | Self::ControlDelegation
            | Self::RaftProgress => ByteClass::Progress,
            Self::DataConsensus
            | Self::QueueData
            | Self::Snapshot
            | Self::Storage
            | Self::ApiIngress => ByteClass::Data,
        }
    }
}

/// Only the TLS/prologue verifier constructs this context. Recovery-only
/// Control Raft authentication never grants queue or application authority.
pub struct AuthenticatedSession {
    pub(crate) local: WorkerIdentity,
    pub(crate) remote: WorkerIdentity,
    pub(crate) group: GroupIdentity,
    pub(crate) purpose: Purpose,
    pub(crate) nonce: Uuid,
    pub(crate) recovery_only: bool,
}
impl AuthenticatedSession {
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn for_test(
        local: WorkerIdentity,
        remote: WorkerIdentity,
        group: GroupIdentity,
        purpose: Purpose,
        nonce: Uuid,
    ) -> Self {
        Self {
            local,
            remote,
            group,
            purpose,
            nonce,
            recovery_only: false,
        }
    }
    #[inline]
    pub fn local(&self) -> WorkerIdentity {
        self.local
    }
    #[inline]
    pub fn remote(&self) -> WorkerIdentity {
        self.remote
    }
    #[inline]
    pub fn group(&self) -> &GroupIdentity {
        &self.group
    }
    #[inline]
    pub fn purpose(&self) -> Purpose {
        self.purpose
    }
    #[inline]
    pub fn nonce(&self) -> Uuid {
        self.nonce
    }
    #[inline]
    pub fn recovery_only(&self) -> bool {
        self.recovery_only
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReceivePlan {
    pub class: TrafficClass,
    pub section_lengths: [u64; MAX_SECTIONS],
    pub decode_budget_bytes: usize,
}

/// Input bytes remain charged until the endpoint and any native continuation
/// release them. Decode scratch has the same ownership rule.
pub struct ChargedSections {
    pub(crate) sections: [Option<ChargedBytes>; MAX_SECTIONS],
    pub(crate) scratch: Arc<ScratchLease>,
    pub(crate) resources: Arc<MemoryHandle>,
}
impl ChargedSections {
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn for_test(
        shared: &Arc<impl ResourceOwner>,
        class: ByteClass,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        decode_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            sections,
            scratch: Arc::new(
                shared
                    .worker_memory()
                    .reserve_scratch(class, decode_bytes)?,
            ),
            resources: MemoryHandle::new(shared.clone()),
        })
    }
    /// Lexically inspect raw sections first, then acquire a working decode
    /// lease. Wire buffering and protected Progress capacity stay independent.
    pub fn reserve_decode(
        mut self,
        shared: &Arc<impl ResourceOwner>,
        bytes: usize,
    ) -> Result<Self, ProtocolError> {
        if !self.resources.belongs_to(shared.as_ref()) {
            return Err(malformed("decode reservation belongs to another worker"));
        }
        self.scratch = Arc::new(self.resources.reserve_scratch(ByteClass::Data, bytes)?);
        Ok(self)
    }
    #[inline]
    pub fn section(&self, index: usize) -> Option<&ChargedBytes> {
        self.sections.get(index).and_then(Option::as_ref)
    }
    pub fn retain_decoded<T>(&self, value: T) -> Decoded<T> {
        Decoded {
            value,
            _sections: self.sections.clone(),
            _scratch: self.scratch.clone(),
        }
    }
    pub fn decode<T: DeserializeOwned>(&self, index: usize) -> Result<Decoded<T>, ProtocolError> {
        let bytes = self
            .section(index)
            .ok_or_else(|| malformed("missing RPC section"))?;
        json_input(bytes.as_slice(), bytes.len())
            .map_err(|_| malformed("RPC section exceeds JSON bounds"))?;
        if json_decode_budget(bytes.as_slice())? > self.scratch.bytes() {
            return Err(malformed("RPC decode reservation is too small"));
        }
        let value = serde_json::from_slice(bytes.as_slice())
            .map_err(|_| malformed("invalid RPC section"))?;
        Ok(self.retain_decoded(value))
    }
}

/// Move this wrapper into native work. Extracting a bare decoded value is not
/// exposed, so cancellation cannot drop its scratch owner while work continues.
pub struct Decoded<T> {
    value: T,
    _sections: [Option<ChargedBytes>; MAX_SECTIONS],
    _scratch: Arc<ScratchLease>,
}
impl<T> Deref for Decoded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

pub struct ChargedReply {
    pub header: ChargedBytes,
    pub sections: [Option<ChargedBytes>; MAX_SECTIONS],
}
/// A received reply retains decode overlap capacity through the caller's use.
pub struct ReceivedReply {
    pub(crate) reply: ChargedReply,
    pub(crate) scratch: Arc<ScratchLease>,
    pub(crate) session: AuthenticatedSession,
    pub(crate) resources: Arc<MemoryHandle>,
}
impl Deref for ReceivedReply {
    type Target = ChargedReply;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.reply
    }
}
impl ReceivedReply {
    /// Keep the decode charge while retaining an owned value from the reply.
    #[inline]
    pub fn scratch_lease(&self) -> Arc<ScratchLease> {
        self.scratch.clone()
    }

    /// Bytes covered by the reply's current decode reservation.
    #[inline]
    pub fn decode_reserved_bytes(&self) -> usize {
        self.scratch.bytes()
    }

    /// Metadata readers use the worker's independent bounded reply decoder,
    /// never the query working budget or an arena holding their raw frames.
    pub async fn reserve_reply_decode(
        mut self,
        shared: &Arc<impl ResourceOwner>,
        bytes: usize,
    ) -> Result<Self, ProtocolError> {
        if !self.resources.belongs_to(shared.as_ref()) {
            return Err(malformed("reply decoder belongs to another worker"));
        }
        self.scratch = Arc::new(self.resources.reserve_reply_decode(bytes).await?);
        Ok(self)
    }
    pub fn reserve_decode(
        mut self,
        shared: &Arc<impl ResourceOwner>,
        bytes: usize,
    ) -> Result<Self, ProtocolError> {
        if !self.resources.belongs_to(shared.as_ref()) {
            return Err(malformed(
                "reply decode reservation belongs to another worker",
            ));
        }
        self.scratch = Arc::new(self.resources.reserve_scratch(ByteClass::Data, bytes)?);
        Ok(self)
    }
    #[inline]
    pub fn remote_identity(&self) -> WorkerIdentity {
        self.session.remote()
    }
    #[inline]
    pub fn nonce(&self) -> Uuid {
        self.session.nonce()
    }
    #[inline]
    pub fn purpose(&self) -> Purpose {
        self.session.purpose()
    }
    #[inline]
    pub fn group(&self) -> &GroupIdentity {
        self.session.group()
    }
    pub fn decode_header<T: DeserializeOwned>(&self) -> Result<Decoded<T>, ProtocolError> {
        json_input(self.reply.header.as_slice(), MAX_HEADER_BYTES)
            .map_err(|_| malformed("RPC reply header exceeds JSON bounds"))?;
        if json_decode_budget(self.reply.header.as_slice())? > self.scratch.bytes() {
            return Err(malformed("RPC reply decode reservation is too small"));
        }
        let value = serde_json::from_slice(self.reply.header.as_slice())
            .map_err(|_| malformed("invalid RPC reply header"))?;
        Ok(Decoded {
            value,
            _sections: self.reply.sections.clone(),
            _scratch: self.scratch.clone(),
        })
    }
    pub fn decode_section<T: DeserializeOwned>(
        &self,
        index: usize,
    ) -> Result<Decoded<T>, ProtocolError> {
        ChargedSections {
            sections: self.reply.sections.clone(),
            scratch: self.scratch.clone(),
            resources: self.resources.clone(),
        }
        .decode(index)
    }
}
pub type EndpointFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;

pub trait Endpoint: Send + Sync + 'static {
    /// Perform fresh authority and method/class checks here. No payload or
    /// native permits are held. Reject data methods on QueueControl.
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan>;
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply>;
}

fn malformed(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
