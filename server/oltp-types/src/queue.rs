//! Identity and authority records for the replicated RAM queue.
pub use crate::authority::{MAX_READ_GRANTS, QueueReadToken, ScopedTimestampEpoch};
use crate::{
    authority::{IdentityReservation, ReadGrant, RejectedIdentity, ScopedQueuePlacement},
    owned::serialized_digest,
    types::*,
};
use nabedra_protocol::{
    QueueError, RequestState,
    protocol::{self, ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueScope {
    pub cluster_id: Uuid,
    pub database: Text<128>,
}
impl QueueScope {
    pub fn validate(&self) -> Result<(), QueueError> {
        if self.cluster_id.is_nil() || !crate::valid_database(&self.database) {
            return Err(QueueError::Invalid);
        }
        Ok(())
    }
}

/// Immutable authorization evidence, never a bearer credential. Expiry is
/// checked at first admission, not while recovering already accepted work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalAuthorization {
    pub database: Text<128>,
    pub principal: Digest,
    pub policy_revision: u64,
    pub ingress_id: Uuid,
    pub ingress_incarnation: Uuid,
    pub credential_expires_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedContext {
    pub authorization: OriginalAuthorization,
    pub read: Option<QueueReadToken>,
}
impl AcceptedContext {
    pub fn validate_binding(
        &self,
        scope: &QueueScope,
        run_id: Uuid,
        request_id: Uuid,
        digest: Digest,
        mutation: bool,
    ) -> Result<(), QueueError> {
        let auth = &self.authorization;
        if auth.database != scope.database
            || auth.policy_revision == 0
            || auth.ingress_id.is_nil()
            || auth.ingress_incarnation.is_nil()
        {
            return Err(QueueError::Invalid);
        }
        if let Some(read) = &self.read
            && (read.identity.run_id != run_id
                || read.identity.request_id != request_id
                || read.identity.digest != digest
                || read.identity.nonce.is_nil()
                || read.storage_pin.pin_id.is_nil()
                || read.storage_pin.allocator_epoch == 0
                || read.schema_epoch == 0
                || read.routing_epoch == 0)
        {
            return Err(QueueError::Invalid);
        }
        // Pure reads cannot be accepted with an owner-private or absent pin.
        if !mutation && self.read.is_none() {
            return Err(QueueError::Invalid);
        }
        Ok(())
    }
}

/// The caller obtained this view through the Control authority lane. The owner
/// identity is current enrollment, independent of permanent Raft replica IDs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAuthority {
    pub ready: ReadyMembership,
    pub owner: WorkerIdentity,
}

impl QueueAuthority {
    pub fn view_is_behind(self, expected: AuthorityView) -> bool {
        let local = self.ready.view;
        local.run_id == expected.run_id && local.membership_revision < expected.membership_revision
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordVersion {
    pub request_id: Uuid,
    pub version: u64,
    pub digest: Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerReachability {
    Reachable,
    Unreachable,
    Indeterminate,
}

pub const QUEUE_FORMAT_VERSION: u32 = 1;
pub const MAX_HEADER_BYTES: usize = 4096;
pub const MAX_RECORD_HEADER_BYTES: usize = 8192;
pub const MAX_CONTEXT_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordHeader {
    pub queue_format_version: u32,
    pub scope: QueueScope,
    pub ready: ReadyMembership,
    pub owner: WorkerIdentity,
    pub request_id: Uuid,
    pub digest: Digest,
    pub sequence: u64,
    pub version: u64,
    pub accepted: bool,
    pub state: RequestState,
    pub response_status: Option<u16>,
    pub completed_at_ms: Option<u64>,
    /// Fixed lengths refer to exact raw body/context/response sections. There
    /// is no serde byte-array expansion or worker list in this header.
    pub section_lengths: [u64; 3],
    pub all_copy_evidence: bool,
    #[serde(
        default = "ReadGrantSet::initial",
        skip_serializing_if = "ReadGrantSet::is_initial"
    )]
    pub read_grants: ReadGrantSet,
}
impl RecordHeader {
    pub fn preflight(&self, scope: &QueueScope, current: QueueAuthority) -> Result<(), QueueError> {
        if self.queue_format_version != QUEUE_FORMAT_VERSION
            || self.scope != *scope
            || self.request_id.is_nil()
            || self.sequence == 0
            || self.version == 0
            || self.version == u64::MAX
            || self.section_lengths[0] == 0
            || self.section_lengths[0] > protocol::MAX_BODY_BYTES as u64
            || self.section_lengths[1] == 0
            || self.section_lengths[1] > MAX_CONTEXT_BYTES as u64
            || self.section_lengths[2] > protocol::MAX_BODY_BYTES as u64
            || self.state.terminal() != self.completed_at_ms.is_some()
            || self.state.terminal() != self.response_status.is_some()
            || self.state.terminal() != (self.section_lengths[2] != 0)
            || !self.accepted && self.state != RequestState::Queued
            || !self.accepted && self.all_copy_evidence
        {
            return Err(QueueError::Invalid);
        }
        self.scope.validate()?;
        self.ready.view.validate().map_err(invalid)?;
        self.owner.validate().map_err(invalid)?;
        self.read_grants
            .validate(self.ready.view, self.request_id, self.digest, self.accepted)?;
        // Control can authorize receipt before this replica applies the new ready set.
        if self.ready != current.ready || self.owner != current.owner {
            return Err(QueueError::Stale);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrantSet {
    pub revision: u64,
    pub grants: Vec<ReadGrant>,
}

impl ReadGrantSet {
    pub fn initial() -> Self {
        Self {
            revision: 1,
            grants: Vec::new(),
        }
    }

    pub fn is_initial(&self) -> bool {
        self.revision == 1 && self.grants.is_empty()
    }

    pub fn validate(
        &self,
        view: AuthorityView,
        id: Uuid,
        digest: Digest,
        accepted: bool,
    ) -> Result<(), QueueError> {
        if self.revision == 0
            || self.grants.len() > MAX_READ_GRANTS
            || !accepted && !self.grants.is_empty()
            || self
                .grants
                .windows(2)
                .any(|pair| pair[0].ticket >= pair[1].ticket)
        {
            return Err(QueueError::Invalid);
        }
        for grant in &self.grants {
            if grant.ticket.is_nil()
                || grant.group.is_nil()
                || grant.node == 0
                || grant.owner.validate().is_err()
                || grant.worker.validate().is_err()
                || grant.scope.request.run_id != view.run_id
                || grant.scope.request.request_id != id
                || grant.scope.request.digest != digest
                || grant.scope.authority.run_id != view.run_id
                || grant.scope.authority.owner_term == 0
                || grant.scope.authority.owner_term > view.owner_term
            {
                return Err(QueueError::Invalid);
            }
        }
        Ok(())
    }

    pub fn validate_context(
        &self,
        context: &AcceptedContext,
        bytes: &[u8],
    ) -> Result<(), QueueError> {
        if !self.grants.is_empty() {
            let pin = context.read.as_ref().ok_or(QueueError::Invalid)?;
            let digest = Digest::from_bytes(Sha256::digest(bytes).into());
            if self.grants.iter().any(|grant| {
                grant.scope.request.accepted_context != digest
                    || grant.scope.read_ts != pin.storage_pin.read_ts
            }) {
                return Err(QueueError::Conflict);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPhase {
    Snapshot,
    Delta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSnapshot {
    pub ticket: JoinTicket,
    pub source: WorkerIdentity,
    pub snapshot_id: Uuid,
    pub snapshot_cut: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinPageHeader {
    pub snapshot: JoinSnapshot,
    pub phase: JoinPhase,
    pub final_cut: Option<u64>,
    pub after_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub done: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyPhase {
    Payload,
    Marker,
    Terminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedCopyStamp {
    pub placement: Digest,
    pub reservation: Digest,
    pub request: RequestBinding,
    pub payload: RecordVersion,
    pub copied: RecordVersion,
    pub phase: CopyPhase,
}

impl ScopedCopyStamp {
    pub fn for_payload(
        current: &ScopedQueuePlacement,
        reservation: IdentityReservation,
        request: RequestBinding,
        version: RecordVersion,
    ) -> Result<Self, QueueError> {
        if request.run_id != current.view.run_id
            || !reservation.binds(request)
            || request.request_id.is_nil()
            || version.request_id != request.request_id
            || version.version == 0
            || version.version == u64::MAX
        {
            return Err(QueueError::Invalid);
        }
        Ok(Self {
            placement: placement_digest(current)?,
            reservation: serialized_digest(&reservation).map_err(invalid)?,
            request,
            payload: version,
            copied: version,
            phase: CopyPhase::Payload,
        })
    }

    pub fn payload_claim(self) -> Result<(RequestBinding, RecordVersion), QueueError> {
        if self.phase != CopyPhase::Payload {
            return Err(QueueError::Invalid);
        }
        Ok((self.request, self.payload))
    }

    pub fn marker(self) -> Self {
        Self {
            phase: CopyPhase::Marker,
            copied: self.payload,
            ..self
        }
    }

    pub fn terminal(self, version: RecordVersion) -> Self {
        Self {
            phase: CopyPhase::Terminal,
            copied: version,
            ..self
        }
    }

    pub fn binding(self) -> RequestBinding {
        self.request
    }

    pub fn version(self) -> RecordVersion {
        self.copied
    }
}

pub fn placement_digest(current: &ScopedQueuePlacement) -> Result<Digest, QueueError> {
    current.validate(&current.group).map_err(invalid)?;
    serialized_digest(&("nabedra/scoped-ram-queue/v2", current)).map_err(invalid)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedInventoryEntry {
    pub reservation: IdentityReservation,
    pub payload: ScopedCopyStamp,
    pub marked: bool,
    pub terminal: Option<ScopedCopyStamp>,
}

impl ScopedInventoryEntry {
    pub fn request_id(self) -> Uuid {
        self.payload.request.request_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTransferRecord {
    pub entry: ScopedInventoryEntry,
    pub payload: RecordHeader,
    pub terminal: Option<RecordHeader>,
}

impl ScopedTransferRecord {
    pub fn request_id(&self) -> Uuid {
        self.entry.payload.request.request_id
    }

    pub fn payload_header(&self) -> &RecordHeader {
        &self.payload
    }

    pub fn terminal_claim(&self) -> Result<(ScopedCopyStamp, RecordVersion, [u64; 3]), QueueError> {
        let terminal = self.terminal.as_ref().ok_or(QueueError::Invalid)?;
        let stamp = self.entry.terminal.ok_or(QueueError::Invalid)?;
        Ok((self.entry.payload, stamp.copied, terminal.section_lengths))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTerminalStatement {
    pub epoch: ScopedTimestampEpoch,
    pub request: RequestBinding,
    pub pin_identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub payload: RecordVersion,
    pub terminal: RecordVersion,
    pub response_status: u16,
    pub response_hash: Digest,
    pub completed_at_ms: u64,
}

impl ScopedTerminalStatement {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !self.epoch.valid()
            || self.request.run_id != self.epoch.view.run_id
            || self.request.request_id.is_nil()
            || self.payload.request_id != self.request.request_id
            || self.payload.version == 0
            || self.terminal.version <= self.payload.version
            || self.terminal.request_id != self.request.request_id
            || self.terminal.version == u64::MAX
            || self.pin_identity.run_id != self.request.run_id
            || self.pin_identity.request_id != self.request.request_id
            || self.pin_identity.digest != self.request.digest
            || self.pin_identity.nonce.is_nil()
            || self.storage_pin.pin_id.is_nil()
            || self.storage_pin.allocator_epoch == 0
            || !(self.response_status == 200 || (400..600).contains(&self.response_status))
        {
            return Err(invalid_terminal_statement());
        }
        Ok(())
    }

    pub fn for_placement(&self, placement: &ScopedQueuePlacement) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.epoch != ScopedTimestampEpoch::from_placement(placement)? {
            return Err(terminal_unavailable());
        }
        Ok(())
    }

    pub fn rebases(&self, old: &Self) -> bool {
        self.epoch.advances(old.epoch)
            && self.request == old.request
            && self.pin_identity == old.pin_identity
            && self.storage_pin == old.storage_pin
            && self.payload.version == old.payload.version
            && self.terminal.version == old.terminal.version
            && self.completed_at_ms == old.completed_at_ms
            && self.response_hash == old.response_hash
            && self.response_status == old.response_status
    }

    pub fn matches_reservation(&self, reservation: IdentityReservation) -> bool {
        reservation.binds(self.request)
            && reservation.reservation_id == self.pin_identity.nonce
            && self.epoch.admits(reservation.scoped)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerAdmissionTicket {
    pub owner: WorkerIdentity,
    pub nonce: Uuid,
    pub status_group: GroupIdentity,
    pub expected: RejectedIdentity,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRequest {
    Ping {
        scope: QueueScope,
        view: AuthorityView,
    },
    RemovalStatement {
        scope: QueueScope,
        view: AuthorityView,
        ticket: JoinTicket,
    },
    RemovalRecord {
        scope: QueueScope,
        ticket: JoinTicket,
        after: Option<Uuid>,
    },
    Install {
        record: RecordHeader,
    },
    ScopedPayloadV2 {
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        record: Box<RecordHeader>,
    },
    ScopedPayloadWitnessV2 {
        scope: QueueScope,
        view: AuthorityView,
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        request: RequestBinding,
        version: RecordVersion,
    },
    ScopedMarkerV2 {
        scope: QueueScope,
        view: AuthorityView,
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        request: RequestBinding,
        version: RecordVersion,
        witness: bool,
    },
    ScopedTerminalV2 {
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        record: Box<RecordHeader>,
        version: RecordVersion,
    },
    ScopedTerminalRepairV2 {
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        record: Box<ScopedTransferRecord>,
    },
    ScopedTerminalWitnessV2 {
        scope: QueueScope,
        view: AuthorityView,
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        request: RequestBinding,
        payload: RecordVersion,
        terminal: RecordVersion,
    },
    ScopedTerminalStateV2 {
        scope: QueueScope,
        source_group: Uuid,
        statement: Box<ScopedTerminalStatement>,
    },
    ScopedSurvivorV2 {
        scope: QueueScope,
        view: AuthorityView,
        queue_group_id: Uuid,
        transfer_id: Uuid,
        after: Option<Uuid>,
    },
    JoinPage {
        scope: QueueScope,
        view: AuthorityView,
        page: Box<JoinPageHeader>,
        record: Option<RecordHeader>,
    },
    State {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
    },
    TerminalState {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        timestamps_group_id: Uuid,
    },
    JoinStatement {
        scope: QueueScope,
        view: AuthorityView,
        ticket_id: Uuid,
    },
    AdmissionState {
        scope: QueueScope,
        view: AuthorityView,
        ticket: Box<OwnerAdmissionTicket>,
    },
    ReadGrantCopy {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        set: ReadGrantSet,
    },
    ReadGrantSettle {
        scope: QueueScope,
        view: AuthorityView,
        grant: Box<ReadGrant>,
    },
    Result {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        version: u64,
    },
}
impl QueueRequest {
    pub fn scope(&self) -> &QueueScope {
        match self {
            Self::Install { record } => &record.scope,
            Self::ScopedPayloadV2 { record, .. } | Self::ScopedTerminalV2 { record, .. } => {
                &record.scope
            }
            Self::ScopedTerminalRepairV2 { record, .. } => &record.payload_header().scope,
            Self::State { scope, .. }
            | Self::Ping { scope, .. }
            | Self::RemovalRecord { scope, .. }
            | Self::RemovalStatement { scope, .. }
            | Self::TerminalState { scope, .. }
            | Self::JoinStatement { scope, .. }
            | Self::AdmissionState { scope, .. }
            | Self::ReadGrantCopy { scope, .. }
            | Self::ReadGrantSettle { scope, .. }
            | Self::ScopedPayloadWitnessV2 { scope, .. }
            | Self::ScopedMarkerV2 { scope, .. }
            | Self::ScopedTerminalWitnessV2 { scope, .. }
            | Self::ScopedTerminalStateV2 { scope, .. }
            | Self::ScopedSurvivorV2 { scope, .. }
            | Self::Result { scope, .. }
            | Self::JoinPage { scope, .. } => scope,
        }
    }
    pub fn view(&self) -> AuthorityView {
        match self {
            Self::Install { record } => record.ready.view,
            Self::ScopedPayloadV2 { record, .. } | Self::ScopedTerminalV2 { record, .. } => {
                record.ready.view
            }
            Self::ScopedTerminalRepairV2 { record, .. } => record.payload_header().ready.view,
            Self::RemovalRecord { ticket, .. } => ticket.source_ready.view,
            Self::ScopedTerminalStateV2 { statement, .. } => statement.epoch.view,
            Self::State { view, .. }
            | Self::Ping { view, .. }
            | Self::RemovalStatement { view, .. }
            | Self::TerminalState { view, .. }
            | Self::JoinStatement { view, .. }
            | Self::AdmissionState { view, .. }
            | Self::ReadGrantCopy { view, .. }
            | Self::ReadGrantSettle { view, .. }
            | Self::ScopedPayloadWitnessV2 { view, .. }
            | Self::ScopedMarkerV2 { view, .. }
            | Self::ScopedTerminalWitnessV2 { view, .. }
            | Self::ScopedSurvivorV2 { view, .. }
            | Self::Result { view, .. }
            | Self::JoinPage { view, .. } => *view,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueState {
    pub request_id: Uuid,
    pub run_id: Uuid,
    pub version: u64,
    pub state: RequestState,
    pub copies_complete: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueReply {
    Ping {
        worker: WorkerIdentity,
        ready: ReadyMembership,
        nonce: Uuid,
    },
    RemovalStatement {
        worker: WorkerIdentity,
        statement: Box<QueueCopyStatement>,
        nonce: Uuid,
    },
    RemovalRecord {
        worker: WorkerIdentity,
        ticket: JoinTicket,
        after: Option<Uuid>,
        record: Option<Box<RecordHeader>>,
        nonce: Uuid,
    },
    Installed {
        worker: WorkerIdentity,
        record: RecordVersion,
        nonce: Uuid,
    },
    ReadGrantConfirmed {
        digest: Digest,
        nonce: Uuid,
    },
    Joined {
        worker: WorkerIdentity,
        page: Box<JoinPageHeader>,
        nonce: Uuid,
    },
    State(QueueState),
    TerminalState {
        worker: WorkerIdentity,
        statement: Box<TerminalCopyStatement>,
        nonce: Uuid,
    },
    JoinStatement {
        worker: WorkerIdentity,
        statement: Box<QueueCopyStatement>,
        nonce: Uuid,
    },
    Result {
        request_id: Uuid,
        version: u64,
        status: u16,
        response_bytes: u64,
    },
}

fn invalid(_: impl Sized) -> QueueError {
    QueueError::Invalid
}

fn invalid_terminal_statement() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "timestamp adapter requires a Timestamps group",
        Outcome::NotApplied,
    )
}
fn terminal_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "timestamp durable state is unavailable",
        Outcome::Unknown,
    )
}

/// The worker-local view used to verify and route scheduled participant reads.
pub trait ReplicaReadSource: Send + Sync + 'static {
    fn scan_context(
        &self,
        scope: crate::authority::ReadScope,
        owner: WorkerIdentity,
        target: WorkerIdentity,
    ) -> Result<nabedra_resources::memory::ChargedBytes, ProtocolError>;

    fn knows_read_grant(&self, grant: ReadGrant) -> Result<bool, ProtocolError>;

    fn read_member(
        &self,
        authority: Authority,
        worker: Uuid,
    ) -> Result<Option<WorkerIdentity>, ProtocolError>;

    fn local_identity(&self) -> Result<WorkerIdentity, ProtocolError>;
}

impl<T: ReplicaReadSource> ReplicaReadSource for std::sync::Mutex<T> {
    fn scan_context(
        &self,
        scope: crate::authority::ReadScope,
        owner: WorkerIdentity,
        target: WorkerIdentity,
    ) -> Result<nabedra_resources::memory::ChargedBytes, ProtocolError> {
        self.lock()
            .map_err(|_| read_source_unavailable())?
            .scan_context(scope, owner, target)
    }

    fn knows_read_grant(&self, grant: ReadGrant) -> Result<bool, ProtocolError> {
        self.lock()
            .map_err(|_| read_source_unavailable())?
            .knows_read_grant(grant)
    }

    fn read_member(
        &self,
        authority: Authority,
        worker: Uuid,
    ) -> Result<Option<WorkerIdentity>, ProtocolError> {
        self.lock()
            .map_err(|_| read_source_unavailable())?
            .read_member(authority, worker)
    }

    fn local_identity(&self) -> Result<WorkerIdentity, ProtocolError> {
        self.lock()
            .map_err(|_| read_source_unavailable())?
            .local_identity()
    }
}

fn read_source_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "no registered group leader completed the strong service call",
        Outcome::Unknown,
    )
}

pub type QueueReadFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ProtocolError>> + Send + 'a>>;

/// The owner retains the read grant across retries until all copies settle.
pub trait QueueReadSource: Send + Sync + 'static {
    fn resume_read_continuation(&self, scope: crate::authority::ReadScope) -> QueueReadFuture<'_>;
    fn open_read_grant(&self, grant: ReadGrant) -> QueueReadFuture<'_>;
    fn finish_read_grant(&self, grant: ReadGrant) -> QueueReadFuture<'_>;
    fn settle_read_grant(&self, grant: ReadGrant) -> QueueReadFuture<'_>;
}
