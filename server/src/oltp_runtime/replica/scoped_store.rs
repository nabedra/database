//! Worker-local RAM for Data-colocated RF3 queues. A worker builds one store
//! across its scopes and supplies the scoped share of its configured queue
//! budgets. The normal worker scheduler also charges the retained bytes.
//!
//! Frame preparation runs with owned codec capacity outside the queue lock.
//! Placement and reservation inputs come from authenticated Control and Status
//! reads. Only an authenticated owner can install or settle a record. This
//! module has no authority to change a placement or authorize a new owner.

use super::*;
use crate::{
    oltp::{mvcc::catalog::ShardRoute, pool::ScratchLease},
    oltp_replication::{
        authority::{
            control::{ScopedQueueInventory, ScopedQueuePlacement, ScopedQueueTransition},
            status::{
                IdentityReservation, RequestKind, ScopedAdmission, VerifiedSettlement,
                VerifiedStatusDecision,
            },
            timestamp::{ScopedPinRelease, ScopedTerminalStatement, ScopedTimestampEpoch},
        },
        multiplex::{Purpose, ReceivedReply},
        owned::serialized_digest,
    },
    protocol::Operation,
};
use std::{
    ops::Bound::{Excluded, Included},
    sync::{Mutex, Weak},
};

// Includes fixed state, ordered-map nodes and ownership metadata.
const ENTRY_BYTES: usize = 2048;
const TERMINAL_BYTES: usize = 512;
const STORE_BYTES: usize = 1024;
// Covers the shared registration and its ordered-map entry.
const INSERT_REGISTRATION_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CopyPhase {
    Payload,
    Marker,
    Terminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedCopyStamp {
    placement: Digest,
    reservation: Digest,
    request: RequestBinding,
    payload: RecordVersion,
    copied: RecordVersion,
    phase: CopyPhase,
}

impl ScopedCopyStamp {
    fn for_payload(
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

    pub(crate) fn payload_claim(self) -> Result<(RequestBinding, RecordVersion), QueueError> {
        if self.phase != CopyPhase::Payload {
            return Err(QueueError::Invalid);
        }
        Ok((self.request, self.payload))
    }

    pub(crate) fn marker(self) -> Self {
        Self {
            phase: CopyPhase::Marker,
            copied: self.payload,
            ..self
        }
    }

    pub(crate) fn terminal(self, version: RecordVersion) -> Self {
        Self {
            phase: CopyPhase::Terminal,
            copied: version,
            ..self
        }
    }

    pub(crate) fn binding(self) -> RequestBinding {
        self.request
    }

    pub(crate) fn version(self) -> RecordVersion {
        self.copied
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedCopyReply {
    worker: WorkerIdentity,
    stamp: ScopedCopyStamp,
    nonce: Uuid,
}

/// No deserializer. A store install or an authenticated RPC can attest to a
/// local copy. A worker list supplied in the owner's header cannot do so.
#[derive(Clone, Copy)]
pub(crate) struct ScopedCopyAck {
    worker: WorkerIdentity,
    stamp: ScopedCopyStamp,
}

impl ScopedCopyAck {
    pub(crate) fn reply(self, nonce: Uuid) -> ScopedCopyReply {
        ScopedCopyReply {
            worker: self.worker,
            stamp: self.stamp,
            nonce,
        }
    }

    pub(crate) fn authenticate(
        reply: &ReceivedReply,
        rpc_group: &GroupIdentity,
        target: WorkerIdentity,
        expected: ScopedCopyStamp,
        purpose: Purpose,
    ) -> Result<Self, QueueError> {
        if reply.group() != rpc_group
            || reply.remote_identity() != target
            || reply.sections.iter().any(Option::is_some)
            || reply.purpose() != purpose
        {
            return Err(QueueError::Stale);
        }
        let decoded = reply.decode_header::<ScopedCopyReply>().map_err(invalid)?;
        if decoded.worker != target || decoded.stamp != expected || decoded.nonce != reply.nonce() {
            return Err(QueueError::Stale);
        }
        Ok(Self {
            worker: target,
            stamp: expected,
        })
    }
}

/// A majority of exact copies in a single placement. This capability remains
/// process-local until the transport can verify both holders' attestations.
#[derive(Clone, Copy)]
pub(crate) struct ScopedCopyQuorum {
    stamp: ScopedCopyStamp,
    holders: [WorkerIdentity; 2],
}

impl ScopedCopyQuorum {
    pub(crate) fn confirm(
        current: &ScopedQueuePlacement,
        expected: ScopedCopyStamp,
        a: ScopedCopyAck,
        b: ScopedCopyAck,
    ) -> Result<Self, QueueError> {
        let quorum = Self {
            stamp: expected,
            holders: [a.worker, b.worker],
        };
        if a.stamp != expected || b.stamp != expected {
            return Err(QueueError::Conflict);
        }
        quorum.validate(current)?;
        Ok(quorum)
    }

    fn validate(self, current: &ScopedQueuePlacement) -> Result<(), QueueError> {
        if self.stamp.placement != placement_digest(current)?
            || self.holders[0].worker_id == self.holders[1].worker_id
            || !self
                .holders
                .iter()
                .all(|worker| current.has_holder(*worker))
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    pub(crate) fn stamp(self) -> ScopedCopyStamp {
        self.stamp
    }

    #[cfg(test)]
    pub(crate) fn holders(self) -> [WorkerIdentity; 2] {
        self.holders
    }
}

pub(crate) fn placement_digest(current: &ScopedQueuePlacement) -> Result<Digest, QueueError> {
    current.validate(&current.group).map_err(invalid)?;
    serialized_digest(&("nabedra/scoped-ram-queue/v2", current)).map_err(invalid)
}

pub(crate) fn scoped_authority(
    current: &ScopedQueuePlacement,
) -> Result<QueueAuthority, QueueError> {
    let mut ready = ReadySetHasher::default();
    for member in current
        .members
        .into_iter()
        .filter(|member| current.has_holder(*member))
    {
        ready.push(member).map_err(invalid)?;
    }
    Ok(QueueAuthority {
        ready: ReadyMembership {
            view: current.view,
            ready_root: ready.finish(),
            ready_count: ready.count(),
        },
        owner: current.owner,
    })
}

fn validate_scoped_frame_header(
    current: &ScopedQueuePlacement,
    header: &RecordHeader,
) -> Result<(), QueueError> {
    let scope = QueueScope {
        cluster_id: current.group.cluster_id,
        database: current.group.database.clone(),
    };
    // The outer scoped copy carries the version-2 placement. Its bounded
    // body/context/result envelope keeps the existing frame codec.
    header.preflight(&scope, scoped_authority(current)?)?;
    if header.all_copy_evidence || !header.read_grants.is_initial() {
        return Err(QueueError::Invalid);
    }
    Ok(())
}

pub(crate) fn validate_scoped_terminal_header(
    current: &ScopedQueuePlacement,
    header: &RecordHeader,
) -> Result<(), QueueError> {
    validate_scoped_frame_header(current, header)?;
    if !header.accepted || !matches!(header.state, RequestState::Succeeded | RequestState::Failed) {
        return Err(QueueError::Invalid);
    }
    Ok(())
}

pub(crate) fn validate_scoped_payload_header(
    current: &ScopedQueuePlacement,
    header: &RecordHeader,
) -> Result<(), QueueError> {
    validate_scoped_frame_header(current, header)?;
    if header.accepted || header.state != RequestState::Queued || header.section_lengths[2] != 0 {
        return Err(QueueError::Invalid);
    }
    Ok(())
}

fn frame_version(
    current: &ScopedQueuePlacement,
    frame: &RecordFrame,
) -> Result<(RecordHeader, RecordVersion), QueueError> {
    let header = frame.decode_header()?;
    validate_scoped_frame_header(current, &header)?;
    let version = frame.version(&header)?;
    Ok((header, version))
}

fn section(frame: &RecordFrame, index: usize) -> Result<&[u8], QueueError> {
    frame.sections[index]
        .as_ref()
        .map(AsRef::as_ref)
        .ok_or(QueueError::Invalid)
}

pub(crate) struct PreparedScopedPayload {
    reservation: IdentityReservation,
    stamp: ScopedCopyStamp,
    sequence: u64,
    payload: PayloadBytes,
    _working: ScratchLease,
}

impl PreparedScopedPayload {
    pub(crate) fn prepare(
        current: &ScopedQueuePlacement,
        route: &ShardRoute,
        reservation: IdentityReservation,
        frame: RecordFrame,
        shared: &WorkerSharedResources,
    ) -> Result<Self, QueueError> {
        let policy = reservation.scoped.ok_or(QueueError::Stale)?;
        if policy.protocol_version != ScopedAdmission::VERSION
            || policy.queue_group_id != current.group.group_id
            || policy.queue_generation != current.queue_generation
            || policy.status_group_id.is_nil()
            || policy.status_group_id == policy.queue_group_id
            || reservation.original_run != current.view.run_id
            || reservation.reservation_id.is_nil()
            || route.group != current.group
        {
            return Err(QueueError::Stale);
        }
        let (header, version) = frame_version(current, &frame)?;
        validate_scoped_payload_header(current, &header)?;
        let body = section(&frame, 0)?;
        let context_bytes = section(&frame, 1)?;
        let binding = RequestBinding {
            run_id: current.view.run_id,
            request_id: header.request_id,
            digest: header.digest,
            accepted_context: hash(context_bytes),
        };
        if !reservation.binds(binding)
            || header.accepted
            || header.state != RequestState::Queued
            || frame.sections[2].is_some()
            || hash(body) != reservation.digest
        {
            return Err(QueueError::Conflict);
        }

        let decode_bytes = crate::oltp::mvcc::request_decode_budget(body)
            .map_err(invalid)?
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(64 << 10))
            .ok_or(QueueError::Overloaded)?;
        let working_bytes = decode_bytes
            .checked_add(frame.header.len())
            .and_then(|bytes| bytes.checked_add(body.len()))
            .and_then(|bytes| bytes.checked_add(context_bytes.len()))
            .ok_or(QueueError::Overloaded)?;
        let working = shared
            .reserve_scratch(ByteClass::Data, working_bytes)
            .map_err(|_| QueueError::Overloaded)?;
        let request = protocol::parse_request(body).map_err(invalid)?;
        let (kind, table, key) = match &request.operation {
            Operation::Get(payload) => (RequestKind::Read, payload.table_id, &payload.key),
            Operation::Update(payload) => (RequestKind::Mutation, payload.table_id, &payload.key),
            _ => return Err(QueueError::Invalid),
        };
        if request.request_id != header.request_id
            || reservation.kind != kind
            || !matches!(current.group.kind, GroupKind::Data { table_id, .. } if table_id == table)
            || !route.contains_key(key)
        {
            return Err(QueueError::Conflict);
        }
        let context: AcceptedContext = serde_json::from_slice(context_bytes).map_err(invalid)?;
        context.validate_binding(
            &header.scope,
            binding.run_id,
            binding.request_id,
            binding.digest,
            kind == RequestKind::Mutation,
        )?;
        let read = context.read.ok_or(QueueError::Invalid)?;
        if read.identity.nonce != reservation.reservation_id
            || read.schema_epoch != route.schema_epoch
            || read.routing_epoch < route.routing_epoch
        {
            return Err(QueueError::Stale);
        }
        Ok(Self {
            reservation,
            stamp: ScopedCopyStamp::for_payload(current, reservation, binding, version)?,
            sequence: header.sequence,
            payload: PayloadBytes {
                pin_identity: read.identity,
                storage_pin: read.storage_pin,
                header: frame.header.as_ref().into(),
                body: body.into(),
                context: context_bytes.into(),
            },
            _working: working,
        })
    }

    pub(crate) fn stamp(&self) -> ScopedCopyStamp {
        self.stamp
    }
}

pub(crate) enum ScopedTerminalProof<'a> {
    Read,
    Mutation {
        decision: &'a VerifiedStatusDecision,
        settled: &'a VerifiedSettlement,
    },
    Replica(ScopedCopyAck),
}

pub(crate) struct PreparedScopedTerminal {
    stamp: ScopedCopyStamp,
    reservation: IdentityReservation,
    sequence: u64,
    terminal: TerminalBytes,
    _working: ScratchLease,
}

impl PreparedScopedTerminal {
    pub(crate) fn stamp(&self) -> ScopedCopyStamp {
        self.stamp
    }

    pub(crate) fn prepare(
        current: &ScopedQueuePlacement,
        accepted: ScopedCopyQuorum,
        reservation: IdentityReservation,
        proof: ScopedTerminalProof<'_>,
        frame: RecordFrame,
        shared: &WorkerSharedResources,
    ) -> Result<Self, QueueError> {
        accepted.validate(current)?;
        let (header, version) = frame_version(current, &frame)?;
        validate_scoped_terminal_header(current, &header)?;
        let binding = accepted.stamp.request;
        if accepted.stamp.phase != CopyPhase::Marker
            || !reservation.binds(binding)
            || header.request_id != binding.request_id
            || header.digest != binding.digest
            || hash(section(&frame, 0)?) != binding.digest
            || hash(section(&frame, 1)?) != binding.accepted_context
            || version.version <= accepted.stamp.payload.version
        {
            return Err(QueueError::Conflict);
        }
        let result = section(&frame, 2)?;
        let status = header.response_status.ok_or(QueueError::Invalid)?;
        let working_bytes = crate::oltp::mvcc::json_decode_budget(result)
            .map_err(invalid)?
            .checked_add(frame.header.len())
            .and_then(|bytes| bytes.checked_add(result.len()))
            .ok_or(QueueError::Overloaded)?;
        let working = shared
            .reserve_scratch(ByteClass::Data, working_bytes)
            .map_err(|_| QueueError::Overloaded)?;
        if (status == 200) != (header.state == RequestState::Succeeded) {
            return Err(QueueError::Conflict);
        }
        match proof {
            ScopedTerminalProof::Read if reservation.kind == RequestKind::Read => {
                validate_read_result(binding.request_id, status, result)?;
            }
            ScopedTerminalProof::Mutation { decision, settled }
                if reservation.kind == RequestKind::Mutation =>
            {
                let policy = reservation.scoped.ok_or(QueueError::Stale)?;
                let statement = decision.statement();
                let outcome = decision.outcome().ok_or(QueueError::Unavailable)?;
                let settled = settled.clone().into_sealed();
                let settlement = settled
                    .validate_for(statement.txn, &binding, &statement.manifest)
                    .map_err(invalid)?;
                let expected_status = outcome
                    .result()
                    .err()
                    .map_or(200, |error| error.status_code());
                if statement.request != binding
                    || statement.manifest.status_group_id != policy.status_group_id
                    || statement.manifest.participant_count > 1
                    || &settlement.decision != statement
                    || expected_status != status
                    || outcome.response_hash(binding.request_id).map_err(invalid)? != hash(result)
                {
                    return Err(QueueError::Conflict);
                }
            }
            ScopedTerminalProof::Replica(owner)
                if owner.worker == current.owner
                    && owner.stamp == accepted.stamp.terminal(version) => {}
            _ => return Err(QueueError::Invalid),
        }
        Ok(Self {
            stamp: accepted.stamp.terminal(version),
            reservation,
            sequence: header.sequence,
            terminal: TerminalBytes::new(&header, frame.header.as_ref(), result)?,
            _working: working,
        })
    }
}

fn validate_read_result(id: Uuid, status: u16, bytes: &[u8]) -> Result<(), QueueError> {
    if status == 200 {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ResultFrame<'a> {
            request_id: Uuid,
            #[serde(borrow, rename = "data")]
            _data: &'a serde_json::value::RawValue,
        }
        let result: ResultFrame<'_> = serde_json::from_slice(bytes).map_err(invalid)?;
        if result.request_id != id {
            return Err(QueueError::Conflict);
        }
    } else {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ErrorFrame {
            request_id: Uuid,
            error: protocol::ProtocolError,
        }
        let result: ErrorFrame = serde_json::from_slice(bytes).map_err(invalid)?;
        if result.request_id != id || result.error.status_code() != status {
            return Err(QueueError::Conflict);
        }
    }
    Ok(())
}

struct Usage {
    queue: usize,
    results: usize,
    admissions: usize,
}

/// The stored copy or an in-flight transfer owns its charge until the last
/// reference is released. Export only clones an Arc while holding the lock.
struct RetainedCharge {
    usage: Arc<Mutex<Usage>>,
    queue: usize,
    result: usize,
    _memory: MemoryCharge,
}
impl Drop for RetainedCharge {
    fn drop(&mut self) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.queue -= self.queue;
        usage.results -= self.result;
    }
}

impl RetainedCharge {
    fn shrink_queue(&mut self, bytes: usize) -> Result<(), QueueError> {
        if bytes > self.queue || self.result != 0 {
            return Err(QueueError::Invalid);
        }
        self._memory.shrink_to(bytes as u64).map_err(invalid)?;
        let mut usage = self.usage.lock().map_err(|_| QueueError::Unavailable)?;
        usage.queue -= self.queue - bytes;
        self.queue = bytes;
        Ok(())
    }
}

pub(crate) struct ScopedAdmissionPermit {
    usage: Arc<Mutex<Usage>>,
    payload: Option<(RetainedCharge, RetainedCharge)>,
}

impl Drop for ScopedAdmissionPermit {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admissions -= 1;
    }
}

struct PayloadBytes {
    header: Box<[u8]>,
    body: Box<[u8]>,
    context: Box<[u8]>,
    pin_identity: PinIdentity,
    storage_pin: StoragePin,
}

struct RetainedPayload {
    header: Box<[u8]>,
    data: Arc<RetainedRequest>,
    _charge: RetainedCharge,
}

struct RetainedRequest {
    body: Box<[u8]>,
    context: Box<[u8]>,
    pin_identity: PinIdentity,
    storage_pin: StoragePin,
    _charge: RetainedCharge,
}

struct TerminalBytes {
    header: Box<[u8]>,
    result: Box<[u8]>,
    response_hash: Digest,
    response_status: u16,
    completed_at_ms: u64,
}

impl TerminalBytes {
    fn new(header: &RecordHeader, encoded: &[u8], result: &[u8]) -> Result<Self, QueueError> {
        Ok(Self {
            header: encoded.into(),
            result: result.into(),
            response_hash: hash(result),
            response_status: header.response_status.ok_or(QueueError::Invalid)?,
            completed_at_ms: header.completed_at_ms.ok_or(QueueError::Invalid)?,
        })
    }
}

struct RetainedTerminal {
    stamp: ScopedCopyStamp,
    header: Box<[u8]>,
    data: Arc<RetainedResult>,
}

struct RetainedResult {
    result: Box<[u8]>,
    response_hash: Digest,
    response_status: u16,
    completed_at_ms: u64,
    _charge: RetainedCharge,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScopedLocalEpoch {
    pub placement: Digest,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
    pub candidate: Option<(Uuid, Digest)>,
}

impl ScopedLocalEpoch {
    pub(crate) fn published(placement: &ScopedQueuePlacement) -> Result<Self, QueueError> {
        Ok(Self {
            placement: placement_digest(placement)?,
            queue_generation: placement.queue_generation,
            view: placement.view,
            candidate: None,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ScopedSurvivor {
    pub reservation: IdentityReservation,
    pub stamp: ScopedCopyStamp,
    pub marker: Option<ScopedCopyQuorum>,
    pub acceptance: Option<ScopedCopyQuorum>,
    pub terminal_quorum: Option<ScopedCopyQuorum>,
    pub recovered_marker: bool,
    terminal_verified: bool,
    pin_released: bool,
    epoch: ScopedLocalEpoch,
    sequence: u64,
    payload: Arc<RetainedPayload>,
    terminal: Option<Arc<RetainedTerminal>>,
}

impl ScopedSurvivor {
    pub(crate) fn pin_released(&self) -> bool {
        self.pin_released
    }
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn terminal_stamp(&self) -> Option<ScopedCopyStamp> {
        self.terminal.as_ref().map(|terminal| terminal.stamp)
    }

    pub(crate) fn terminal_statement(
        &self,
        placement: &ScopedQueuePlacement,
    ) -> Result<Option<ScopedTerminalStatement>, QueueError> {
        if self.stamp.placement != placement_digest(placement)? {
            return Err(QueueError::Stale);
        }
        let Some(terminal) = &self.terminal else {
            return Ok(None);
        };
        let statement = ScopedTerminalStatement {
            epoch: ScopedTimestampEpoch::from_placement(placement).map_err(invalid)?,
            request: self.stamp.request,
            pin_identity: self.payload.data.pin_identity.clone(),
            storage_pin: self.payload.data.storage_pin,
            payload: self.stamp.payload,
            terminal: terminal.stamp.copied,
            response_hash: terminal.data.response_hash,
            response_status: terminal.data.response_status,
            completed_at_ms: terminal.data.completed_at_ms,
        };
        statement.validate().map_err(invalid)?;
        Ok(Some(statement))
    }

    pub(crate) fn payload_frame(
        &self,
        shared: &WorkerSharedResources,
    ) -> Result<RecordFrame, QueueError> {
        let charged = |bytes| shared.copy_bytes(bytes).map_err(|_| QueueError::Overloaded);
        Ok(RecordFrame {
            header: charged(&self.payload.header)?,
            sections: [
                Some(charged(&self.payload.data.body)?),
                Some(charged(&self.payload.data.context)?),
                None,
            ],
        })
    }

    /// Deliberately export the original epoch. A receiver in a later epoch has
    /// to verify transfer and reconcile the decision before re-stamping it.
    pub(crate) fn frames(
        &self,
        shared: &WorkerSharedResources,
    ) -> Result<(RecordFrame, Option<RecordFrame>), QueueError> {
        let charged = |bytes| shared.copy_bytes(bytes).map_err(|_| QueueError::Overloaded);
        let payload = self.payload_frame(shared)?;
        let terminal = self
            .terminal
            .as_ref()
            .map(|terminal| {
                Ok::<_, QueueError>(RecordFrame {
                    header: charged(&terminal.header)?,
                    sections: [
                        payload.sections[0].clone(),
                        payload.sections[1].clone(),
                        Some(charged(&terminal.data.result)?),
                    ],
                })
            })
            .transpose()?;
        Ok((payload, terminal))
    }

    pub(crate) fn inventory_entry(&self) -> ScopedInventoryEntry {
        ScopedInventoryEntry {
            reservation: self.reservation,
            payload: self.stamp,
            marked: self.marker.is_some() || self.recovered_marker,
            terminal: self.terminal_stamp(),
        }
    }

    pub(crate) fn transfer_frame(
        &self,
        shared: &WorkerSharedResources,
    ) -> Result<(ScopedTransferRecord, [Option<ChargedBytes>; 3]), QueueError> {
        let (payload, terminal) = self.frames(shared)?;
        let record = ScopedTransferRecord {
            entry: self.inventory_entry(),
            payload: payload.decode_header()?,
            terminal: terminal
                .as_ref()
                .map(RecordFrame::decode_header)
                .transpose()?,
        };
        let [body, context, _] = payload.sections;
        let result = terminal.and_then(|frame| frame.sections.into_iter().nth(2).flatten());
        Ok((record, [body, context, result]))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedInventoryEntry {
    reservation: IdentityReservation,
    payload: ScopedCopyStamp,
    marked: bool,
    terminal: Option<ScopedCopyStamp>,
}

impl ScopedInventoryEntry {
    pub(crate) fn request_id(self) -> Uuid {
        self.payload.request.request_id
    }
}

pub(crate) fn scoped_inventory(
    transition: &ScopedQueueTransition,
    placement: &ScopedQueuePlacement,
) -> Result<ScopedQueueInventory, QueueError> {
    let root = serialized_digest(&(
        "nabedra/scoped-queue/inventory-v1",
        transition.id,
        placement_digest(placement)?,
    ))
    .map_err(invalid)?;
    Ok(ScopedQueueInventory { count: 0, root })
}

pub(crate) fn append_scoped_inventory(
    inventory: &mut ScopedQueueInventory,
    entry: ScopedInventoryEntry,
) -> Result<(), QueueError> {
    let count = inventory
        .count
        .checked_add(1)
        .ok_or(QueueError::Overloaded)?;
    let root = serialized_digest(&(inventory.root, count, entry)).map_err(invalid)?;
    *inventory = ScopedQueueInventory { count, root };
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTransferRecord {
    pub(crate) entry: ScopedInventoryEntry,
    payload: RecordHeader,
    terminal: Option<RecordHeader>,
}

impl ScopedTransferRecord {
    pub(crate) fn request_id(&self) -> Uuid {
        self.entry.payload.request.request_id
    }

    pub(crate) fn payload_header(&self) -> &RecordHeader {
        &self.payload
    }

    pub(crate) fn terminal_claim(
        &self,
    ) -> Result<(ScopedCopyStamp, RecordVersion, [u64; 3]), QueueError> {
        let terminal = self.terminal.as_ref().ok_or(QueueError::Invalid)?;
        let stamp = self.entry.terminal.ok_or(QueueError::Invalid)?;
        Ok((self.entry.payload, stamp.copied, terminal.section_lengths))
    }

    pub(crate) fn validate_terminal_repair(
        &self,
        placement: &ScopedQueuePlacement,
    ) -> Result<(), QueueError> {
        validate_scoped_payload_header(placement, &self.payload)?;
        validate_scoped_terminal_header(
            placement,
            self.terminal.as_ref().ok_or(QueueError::Invalid)?,
        )?;
        Ok(())
    }
}

pub(crate) struct PreparedScopedTransfer {
    epoch: ScopedLocalEpoch,
    source: ScopedInventoryEntry,
    entry: ScopedInventoryEntry,
    sequence: u64,
    payload: PayloadBytes,
    terminal: Option<(ScopedCopyStamp, TerminalBytes)>,
    _working: ScratchLease,
}

impl PreparedScopedTransfer {
    pub(crate) fn prepare(
        source: &ScopedQueuePlacement,
        destination: &ScopedQueuePlacement,
        frame: ScopedTransferRecord,
        sections: [Option<ChargedBytes>; 3],
        shared: &WorkerSharedResources,
        task_limit: u64,
    ) -> Result<Self, QueueError> {
        let ScopedTransferRecord {
            entry,
            mut payload,
            mut terminal,
        } = frame;
        let [body, context, result] = sections
            .each_ref()
            .map(|bytes| bytes.as_ref().map(AsRef::as_ref));
        let body = body.ok_or(QueueError::Invalid)?;
        let context = context.ok_or(QueueError::Invalid)?;
        let context_decode = crate::oltp::mvcc::json_decode_budget(context).map_err(invalid)?;
        let working = body
            .len()
            .checked_add(context.len())
            .and_then(|n| n.checked_add(context_decode))
            .and_then(|n| n.checked_add(result.map_or(0, <[u8]>::len)))
            .and_then(|n| n.checked_add(2 * MAX_RECORD_HEADER_BYTES))
            .filter(|bytes| *bytes as u64 <= task_limit)
            .ok_or(QueueError::Overloaded)?;
        let scratch = shared
            .reserve_scratch(ByteClass::Data, working)
            .map_err(|_| QueueError::Overloaded)?;
        validate_scoped_payload_header(source, &payload)?;
        let binding = entry.payload.request;
        let version = RecordFrame::canonical_version(&payload, [body, context, &[]])?;
        if entry.payload
            != ScopedCopyStamp::for_payload(source, entry.reservation, binding, version)?
            || !entry.reservation.binds(binding)
            || entry.reservation.scoped.is_none_or(|policy| {
                policy.protocol_version != ScopedAdmission::VERSION
                    || policy.queue_group_id != source.group.group_id
                    || policy.queue_generation != source.queue_generation
            })
            || hash(body) != binding.digest
            || hash(context) != binding.accepted_context
        {
            return Err(QueueError::Conflict);
        }
        let accepted: AcceptedContext = serde_json::from_slice(context).map_err(invalid)?;
        accepted.validate_binding(
            &payload.scope,
            binding.run_id,
            binding.request_id,
            binding.digest,
            entry.reservation.kind == RequestKind::Mutation,
        )?;
        let read = accepted.read.ok_or(QueueError::Invalid)?;
        if read.identity.nonce != entry.reservation.reservation_id {
            return Err(QueueError::Conflict);
        }
        let authority = scoped_authority(destination)?;
        payload.ready = authority.ready;
        payload.owner = authority.owner;
        let next_payload = ScopedCopyStamp::for_payload(
            destination,
            entry.reservation,
            binding,
            RecordFrame::canonical_version(&payload, [body, context, &[]])?,
        )?;
        let terminal = match (terminal.as_mut(), result, entry.terminal) {
            (None, None, None) => None,
            (Some(header), Some(result), Some(stamp)) if entry.marked => {
                validate_scoped_terminal_header(source, header)?;
                let old_version = RecordFrame::canonical_version(header, [body, context, result])?;
                if header.request_id != binding.request_id
                    || header.digest != binding.digest
                    || header.sequence != payload.sequence
                    || old_version.version <= version.version
                    || stamp != entry.payload.terminal(old_version)
                {
                    return Err(QueueError::Conflict);
                }
                header.ready = authority.ready;
                header.owner = authority.owner;
                let new_version = RecordFrame::canonical_version(header, [body, context, result])?;
                let encoded = shared
                    .encode(header, MAX_RECORD_HEADER_BYTES)
                    .map_err(invalid)?;
                Some((
                    next_payload.terminal(new_version),
                    TerminalBytes::new(header, encoded.as_ref(), result)?,
                ))
            }
            _ => return Err(QueueError::Conflict),
        };
        let header = shared
            .encode(&payload, MAX_RECORD_HEADER_BYTES)
            .map_err(invalid)?;
        Ok(Self {
            epoch: ScopedLocalEpoch::published(destination)?,
            source: entry,
            entry: ScopedInventoryEntry {
                payload: next_payload,
                terminal: terminal.as_ref().map(|(stamp, _)| *stamp),
                ..entry
            },
            sequence: payload.sequence,
            payload: PayloadBytes {
                pin_identity: read.identity,
                storage_pin: read.storage_pin,
                header: header.as_ref().into(),
                body: body.into(),
                context: context.into(),
            },
            terminal,
            _working: scratch,
        })
    }
}

#[derive(Default)]
pub(crate) struct ScopedTransferBatch {
    records: BTreeMap<Uuid, ScopedSurvivor>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScopedTransferCursor {
    pub(crate) source: usize,
    pub(crate) after: Option<Uuid>,
    inventory: ScopedQueueInventory,
}

struct ScopedTransferProgress {
    epoch: ScopedLocalEpoch,
    basis: Digest,
    cursor: ScopedTransferCursor,
    batch: ScopedTransferBatch,
    _charge: ScratchLease,
}

impl ScopedTransferBatch {
    pub(crate) fn inventory(
        &self,
        transition: &ScopedQueueTransition,
    ) -> Result<ScopedQueueInventory, QueueError> {
        let mut inventory = scoped_inventory(transition, &transition.next)?;
        for record in self.records.values() {
            append_scoped_inventory(&mut inventory, record.inventory_entry())?;
        }
        Ok(inventory)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScopedWriteRevision {
    epoch: u64,
    expiry: u64,
}

type ScopedInsertKey = (Uuid, Uuid, Uuid);
type ScopedInsertMap = Mutex<BTreeMap<ScopedInsertKey, Weak<ScopedInsertRegistration>>>;

// The last copy task drops its registration, including on cancellation.
pub(crate) struct ScopedQueueInsert {
    registration: Arc<ScopedInsertRegistration>,
    epoch_revision: u64,
}

struct ScopedInsertRegistration {
    key: ScopedInsertKey,
    active: Weak<ScopedInsertMap>,
    _charge: ScratchLease,
}

impl Drop for ScopedInsertRegistration {
    fn drop(&mut self) {
        // Expiry can remove this entry and a new copy can register the same key.
        if let Some(active) = self.active.upgrade()
            && let Ok(mut active) = active.lock()
            && active
                .get(&self.key)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
        {
            active.remove(&self.key);
        }
    }
}

pub(crate) struct ScopedQueueStore {
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    local: WorkerIdentity,
    limits: QueueLimits,
    usage: Arc<Mutex<Usage>>,
    pending: usize,
    write_revisions: [ScopedWriteRevision; 32],
    active_inserts: Arc<ScopedInsertMap>,
    records: BTreeMap<(Uuid, Digest, Uuid), ScopedSurvivor>,
    frozen: BTreeMap<Uuid, (ScopedLocalEpoch, ScratchLease)>,
    transfers: BTreeMap<Uuid, ScopedTransferProgress>,
    _metadata: MemoryCharge,
}

impl ScopedQueueStore {
    pub(super) fn new(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        limits: QueueLimits,
    ) -> Result<Self, QueueError> {
        local.validate().map_err(invalid)?;
        scope.validate()?;
        if limits.max_pending_requests == 0
            || limits.max_queue_bytes_per_replica <= STORE_BYTES
            || limits.max_retained_result_bytes_per_worker == 0
            || limits.result_retention_ms == 0
        {
            return Err(QueueError::Invalid);
        }
        shared.bind_worker(local.worker_id).map_err(invalid)?;
        let metadata = shared
            .scheduler()
            .charge_memory(shared.scheduler_node_id(), STORE_BYTES as u64)
            .map_err(|_| QueueError::Overloaded)?;
        Ok(Self {
            shared,
            scope,
            local,
            limits,
            usage: Arc::new(Mutex::new(Usage {
                queue: STORE_BYTES,
                results: 0,
                admissions: 0,
            })),
            pending: 0,
            write_revisions: [ScopedWriteRevision::default(); 32],
            active_inserts: Arc::default(),
            records: BTreeMap::new(),
            frozen: BTreeMap::new(),
            transfers: BTreeMap::new(),
            _metadata: metadata,
        })
    }

    pub(crate) fn reserve_admission(
        &self,
        body_bytes: usize,
    ) -> Result<ScopedAdmissionPermit, QueueError> {
        self.check_pending_room()?;
        let metadata = self.reserve(
            ENTRY_BYTES + TERMINAL_BYTES + 2 * MAX_RECORD_HEADER_BYTES,
            0,
        )?;
        let body = self.reserve(
            body_bytes
                .checked_add(MAX_RECORD_HEADER_BYTES)
                .ok_or(QueueError::Overloaded)?,
            0,
        )?;
        self.usage
            .lock()
            .map_err(|_| QueueError::Unavailable)?
            .admissions += 1;
        Ok(ScopedAdmissionPermit {
            usage: self.usage.clone(),
            payload: Some((metadata, body)),
        })
    }

    fn check_pending_room(&self) -> Result<(), QueueError> {
        let usage = self.usage.lock().map_err(|_| QueueError::Unavailable)?;
        if self.pending.saturating_add(usage.admissions) >= self.limits.max_pending_requests {
            return Err(QueueError::Overloaded);
        }
        Ok(())
    }

    fn reserve(&self, queue: usize, result: usize) -> Result<RetainedCharge, QueueError> {
        let bytes = queue.checked_add(result).ok_or(QueueError::Overloaded)?;
        let memory = self
            .shared
            .scheduler()
            .charge_memory(self.shared.scheduler_node_id(), bytes as u64)
            .map_err(|_| QueueError::Overloaded)?;
        let mut usage = self.usage.lock().map_err(|_| QueueError::Unavailable)?;
        let next_queue = usage
            .queue
            .checked_add(queue)
            .filter(|used| *used <= self.limits.max_queue_bytes_per_replica)
            .ok_or(QueueError::Overloaded)?;
        let next_results = usage
            .results
            .checked_add(result)
            .filter(|used| *used <= self.limits.max_retained_result_bytes_per_worker)
            .ok_or(QueueError::Overloaded)?;
        usage.queue = next_queue;
        usage.results = next_results;
        Ok(RetainedCharge {
            usage: self.usage.clone(),
            queue,
            result,
            _memory: memory,
        })
    }

    fn authorize(
        &self,
        current: &ScopedQueuePlacement,
        sender: WorkerIdentity,
        stamp: ScopedCopyStamp,
    ) -> Result<(), QueueError> {
        if sender != current.owner
            || !self.local_holder(current)
            || stamp.placement != placement_digest(current)?
            || self.is_frozen(current)
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    fn local_holder(&self, placement: &ScopedQueuePlacement) -> bool {
        placement.group.cluster_id == self.scope.cluster_id
            && placement.group.database == self.scope.database
            && placement.has_holder(self.local)
    }

    fn is_frozen(&self, placement: &ScopedQueuePlacement) -> bool {
        self.frozen
            .get(&placement.group.group_id)
            .is_some_and(|(fence, _)| {
                fence.queue_generation == placement.queue_generation
                    && fence.view.run_id == placement.view.run_id
                    && fence.view.owner_term >= placement.view.owner_term
            })
    }

    fn ack(&self, stamp: ScopedCopyStamp) -> ScopedCopyAck {
        ScopedCopyAck {
            worker: self.local,
            stamp,
        }
    }

    pub(crate) fn payload_witness(
        &self,
        current: &ScopedQueuePlacement,
        request: RequestBinding,
        version: RecordVersion,
    ) -> Result<ScopedCopyAck, QueueError> {
        Ok(self.ack(self.claim_payload(current, request, version)?))
    }

    pub(crate) fn claim_payload(
        &self,
        current: &ScopedQueuePlacement,
        request: RequestBinding,
        version: RecordVersion,
    ) -> Result<ScopedCopyStamp, QueueError> {
        let record = self.record(current, request.request_id)?;
        if record.stamp.payload_claim()? != (request, version) {
            return Err(QueueError::Conflict);
        }
        Ok(record.stamp)
    }

    pub(crate) fn copy_witness(
        &self,
        current: &ScopedQueuePlacement,
        stamp: ScopedCopyStamp,
    ) -> Result<ScopedCopyAck, QueueError> {
        let record = self.record(current, stamp.request.request_id)?;
        let retained = match stamp.phase {
            CopyPhase::Payload => Some(record.stamp),
            CopyPhase::Marker => record.marker.map(|_| record.stamp.marker()),
            CopyPhase::Terminal if record.terminal_verified => record.terminal_stamp(),
            CopyPhase::Terminal => None,
        };
        if retained != Some(stamp) {
            return Err(QueueError::Conflict);
        }
        Ok(self.ack(stamp))
    }

    pub(crate) fn record(
        &self,
        placement: &ScopedQueuePlacement,
        id: Uuid,
    ) -> Result<&ScopedSurvivor, QueueError> {
        let digest = placement_digest(placement)?;
        if !self.local_holder(placement) || self.is_frozen(placement) {
            return Err(QueueError::Stale);
        }
        self.records
            .get(&(placement.group.group_id, digest, id))
            .ok_or(QueueError::NotFound)
    }

    pub(crate) fn install_payload(
        &mut self,
        current: &ScopedQueuePlacement,
        sender: WorkerIdentity,
        payload: PreparedScopedPayload,
        mut admission: Option<ScopedAdmissionPermit>,
        insert: ScopedQueueInsert,
    ) -> Result<ScopedCopyAck, QueueError> {
        let stamp = payload.stamp;
        self.authorize(current, sender, stamp)?;
        self.require_insert(
            &insert,
            current.group.group_id,
            stamp.request.run_id,
            stamp.request.request_id,
        )?;
        let key = (
            current.group.group_id,
            stamp.placement,
            stamp.request.request_id,
        );
        if let Some(old) = self.records.get(&key) {
            if old.stamp != stamp || old.reservation != payload.reservation {
                return Err(QueueError::Conflict);
            }
            return Ok(self.ack(stamp));
        }
        if admission.is_none() {
            self.check_pending_room()?;
        }
        let retained = self.retain_payload(
            payload.payload,
            None,
            admission.as_mut().and_then(|permit| permit.payload.take()),
        )?;
        self.records.insert(
            key,
            ScopedSurvivor {
                reservation: payload.reservation,
                stamp,
                sequence: payload.sequence,
                marker: None,
                acceptance: None,
                terminal_quorum: None,
                recovered_marker: false,
                terminal_verified: false,
                pin_released: false,
                epoch: ScopedLocalEpoch::published(current)?,
                payload: retained,
                terminal: None,
            },
        );
        self.pending += 1;
        Ok(self.ack(stamp))
    }

    /// `bound` comes from the authoritative snapshot-binding receipt.
    pub(crate) fn install_marker(
        &mut self,
        current: &ScopedQueuePlacement,
        sender: WorkerIdentity,
        installation: ScopedCopyQuorum,
        bound: RequestBinding,
    ) -> Result<ScopedCopyAck, QueueError> {
        installation.validate(current)?;
        self.authorize(current, sender, installation.stamp)?;
        if installation.stamp.phase != CopyPhase::Payload || installation.stamp.request != bound {
            return Err(QueueError::Conflict);
        }
        let record = self
            .records
            .get_mut(&(
                current.group.group_id,
                installation.stamp.placement,
                bound.request_id,
            ))
            .ok_or(QueueError::Unavailable)?;
        if record.stamp != installation.stamp {
            return Err(QueueError::Conflict);
        }
        // Identical retries may show a different valid quorum. Retain the
        // original proof and acknowledge the same immutable marker.
        record.marker.get_or_insert(installation);
        Ok(self.ack(installation.stamp.marker()))
    }

    pub(crate) fn install_terminal(
        &mut self,
        current: &ScopedQueuePlacement,
        sender: WorkerIdentity,
        terminal: PreparedScopedTerminal,
    ) -> Result<ScopedCopyAck, QueueError> {
        let stamp = terminal.stamp;
        self.authorize(current, sender, stamp)?;
        let key = (
            current.group.group_id,
            stamp.placement,
            stamp.request.request_id,
        );
        let record = self.records.get(&key).ok_or(QueueError::Unavailable)?;
        if record.marker.is_none()
            || record.reservation != terminal.reservation
            || record.sequence != terminal.sequence
            || record.stamp.terminal(stamp.copied) != stamp
        {
            return Err(QueueError::Conflict);
        }
        if let Some(old) = &record.terminal {
            if old.stamp != stamp {
                return Err(QueueError::Conflict);
            }
            self.records
                .get_mut(&key)
                .ok_or(QueueError::Unavailable)?
                .terminal_verified = true;
            return Ok(self.ack(stamp));
        }
        let terminal = self.retain_terminal((stamp, terminal.terminal), None)?;
        let record = self.records.get_mut(&key).ok_or(QueueError::Unavailable)?;
        record.terminal = Some(terminal);
        record.terminal_verified = true;
        self.pending -= 1;
        Ok(self.ack(stamp))
    }

    /// The owner's local install does not authorize dispatch or a terminal
    /// reply. Record the exact majority after each round before exposing it.
    pub(crate) fn confirm_copies(
        &mut self,
        current: &ScopedQueuePlacement,
        quorum: ScopedCopyQuorum,
    ) -> Result<(), QueueError> {
        quorum.validate(current)?;
        self.authorize(current, self.local, quorum.stamp)?;
        let record = self
            .records
            .get_mut(&(
                current.group.group_id,
                quorum.stamp.placement,
                quorum.stamp.request.request_id,
            ))
            .ok_or(QueueError::Unavailable)?;
        let confirmation = match quorum.stamp.phase {
            CopyPhase::Marker
                if record.marker.is_some() && record.stamp.marker() == quorum.stamp =>
            {
                &mut record.acceptance
            }
            CopyPhase::Terminal
                if record.acceptance.is_some()
                    && record.terminal_verified
                    && record.terminal_stamp() == Some(quorum.stamp) =>
            {
                &mut record.terminal_quorum
            }
            _ => return Err(QueueError::Conflict),
        };
        confirmation.get_or_insert(quorum);
        Ok(())
    }

    /// One cursor step over the source placement. This only returns physically
    /// surviving RAM; it never replays an operation or changes its authority.
    pub(crate) fn export_next(
        &self,
        source: &ScopedQueuePlacement,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedSurvivor>, QueueError> {
        if !self.local_holder(source) {
            return Err(QueueError::Stale);
        }
        let digest = placement_digest(source)?;
        let group = source.group.group_id;
        let first = Excluded((group, digest, after.unwrap_or(Uuid::nil())));
        let last = Included((group, digest, Uuid::from_u128(u128::MAX)));
        Ok(self
            .records
            .range((first, last))
            .next()
            .map(|(_, record)| record.clone()))
    }

    /// Install the local fence under the same lock used for all store writes.
    /// An RPC that read Control before the transition cannot write after this cut.
    pub(crate) fn freeze(&mut self, source: &ScopedQueuePlacement) -> Result<(), QueueError> {
        let epoch = ScopedLocalEpoch::published(source)?;
        if !self.local_holder(source) {
            return Err(QueueError::Stale);
        }
        let group = source.group.group_id;
        if self.is_frozen(source) {
            return Ok(());
        }
        if let Some((fence, _)) = self.frozen.get_mut(&group) {
            *fence = epoch;
        } else {
            let charge = self
                .shared
                .reserve_scratch(ByteClass::Progress, 256)
                .map_err(|_| QueueError::Overloaded)?;
            self.frozen.insert(group, (epoch, charge));
        }
        Ok(())
    }

    pub(crate) fn export_frozen_next(
        &self,
        source: &ScopedQueuePlacement,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedSurvivor>, QueueError> {
        let epoch = ScopedLocalEpoch::published(source)?;
        if self
            .frozen
            .get(&source.group.group_id)
            .map(|(fence, _)| *fence)
            != Some(epoch)
        {
            return Err(QueueError::Stale);
        }
        self.export_next(source, after)
    }

    pub(crate) fn candidate_next(
        &self,
        transition: &ScopedQueueTransition,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedInventoryEntry>, QueueError> {
        let source = placement_digest(&transition.previous)?;
        self.export_next(&transition.next, after)?
            .map(|record| {
                if record.epoch.candidate != Some((transition.id, source)) {
                    return Err(QueueError::Conflict);
                }
                Ok(record.inventory_entry())
            })
            .transpose()
    }

    pub(crate) fn owner_next(
        &self,
        current: &ScopedQueuePlacement,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedSurvivor>, QueueError> {
        if current.owner != self.local || self.is_frozen(current) {
            return Err(QueueError::Stale);
        }
        self.export_next(current, after)
    }

    fn retain_payload(
        &self,
        payload: PayloadBytes,
        previous: Option<&ScopedSurvivor>,
        reserved: Option<(RetainedCharge, RetainedCharge)>,
    ) -> Result<Arc<RetainedPayload>, QueueError> {
        // Reserve the terminal header with its payload so result settlement
        // needs only the independently limited response bytes.
        let metadata = (ENTRY_BYTES + TERMINAL_BYTES + MAX_RECORD_HEADER_BYTES)
            .checked_add(payload.header.len())
            .ok_or(QueueError::Overloaded)?;
        let (charge, reserved_body) = if let Some((mut header, body)) = reserved {
            header.shrink_queue(metadata)?;
            (header, Some(body))
        } else {
            (self.reserve(metadata, 0)?, None)
        };
        let data = if let Some(previous) = previous {
            previous.payload.data.clone()
        } else {
            let bytes = payload
                .body
                .len()
                .checked_add(payload.context.len())
                .ok_or(QueueError::Overloaded)?;
            let charge = if let Some(mut charge) = reserved_body {
                charge.shrink_queue(bytes)?;
                charge
            } else {
                self.reserve(bytes, 0)?
            };
            Arc::new(RetainedRequest {
                body: payload.body,
                context: payload.context,
                pin_identity: payload.pin_identity,
                storage_pin: payload.storage_pin,
                _charge: charge,
            })
        };
        Ok(Arc::new(RetainedPayload {
            header: payload.header,
            data,
            _charge: charge,
        }))
    }

    fn retain_terminal(
        &self,
        (stamp, bytes): (ScopedCopyStamp, TerminalBytes),
        previous: Option<&RetainedTerminal>,
    ) -> Result<Arc<RetainedTerminal>, QueueError> {
        let data = if let Some(previous) = previous {
            previous.data.clone()
        } else {
            let charge = self.reserve(0, bytes.result.len())?;
            Arc::new(RetainedResult {
                result: bytes.result,
                response_hash: bytes.response_hash,
                response_status: bytes.response_status,
                completed_at_ms: bytes.completed_at_ms,
                _charge: charge,
            })
        };
        Ok(Arc::new(RetainedTerminal {
            stamp,
            header: bytes.header,
            data,
        }))
    }

    pub(crate) fn terminal_statement(
        &self,
        placement: &ScopedQueuePlacement,
        id: Uuid,
    ) -> Result<ScopedTerminalStatement, QueueError> {
        let record = self.record(placement, id)?;
        if !record.terminal_verified {
            return Err(QueueError::Unavailable);
        }
        record
            .terminal_statement(placement)?
            .ok_or(QueueError::Unavailable)
    }

    pub(crate) fn terminal_retention_elapsed(&self, completed_at_ms: u64, now_ms: u64) -> bool {
        now_ms.saturating_sub(completed_at_ms) >= self.limits.result_retention_ms
    }

    pub(crate) fn can_expire_terminal(
        &self,
        placement: &ScopedQueuePlacement,
        expected: &ScopedTerminalStatement,
        now_ms: u64,
    ) -> Result<bool, QueueError> {
        if !self.terminal_retention_elapsed(expected.completed_at_ms, now_ms) {
            return Ok(false);
        }
        match self.record(placement, expected.request.request_id) {
            Ok(record) => Ok(record.terminal_statement(placement)?.as_ref() == Some(expected)),
            Err(QueueError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn note_pin_release(
        &mut self,
        placement: &ScopedQueuePlacement,
        expected: &ScopedTerminalStatement,
        release: &ScopedPinRelease,
    ) -> Result<(), QueueError> {
        release.validate(expected).map_err(invalid)?;
        if self.terminal_statement(placement, expected.request.request_id)? != *expected {
            return Err(QueueError::Conflict);
        }
        let key = (
            placement.group.group_id,
            placement_digest(placement)?,
            expected.request.request_id,
        );
        let record = self.records.get_mut(&key).ok_or(QueueError::NotFound)?;
        record.pin_released = true;
        Ok(())
    }

    pub(crate) fn expire_terminal(
        &mut self,
        placement: &ScopedQueuePlacement,
        expected: &ScopedTerminalStatement,
        release: &ScopedPinRelease,
        now_ms: u64,
    ) -> Result<bool, QueueError> {
        release.validate(expected).map_err(invalid)?;
        if !self.can_expire_terminal(placement, expected, now_ms)? {
            return Ok(false);
        }
        let key = (
            placement.group.group_id,
            placement_digest(placement)?,
            expected.request.request_id,
        );
        self.fence_expired_request(
            placement.group.group_id,
            expected.request.run_id,
            expected.request.request_id,
        )?;
        self.records.remove(&key);
        Ok(true)
    }

    pub(crate) fn transfer_cursor(
        &mut self,
        transition: &ScopedQueueTransition,
        basis: Digest,
    ) -> Result<ScopedTransferCursor, QueueError> {
        let group = transition.next.group.group_id;
        let mut epoch = ScopedLocalEpoch::published(&transition.next)?;
        epoch.candidate = Some((transition.id, placement_digest(&transition.previous)?));
        if let Some(progress) = self.transfers.get(&group) {
            return if progress.basis == basis && progress.epoch == epoch {
                Ok(progress.cursor)
            } else {
                Err(QueueError::Stale)
            };
        }
        let cursor = Self::next_source(transition, 0)?;
        let charge = self
            .shared
            .reserve_scratch(ByteClass::Data, 1024)
            .map_err(|_| QueueError::Overloaded)?;
        self.transfers.insert(
            group,
            ScopedTransferProgress {
                epoch,
                basis,
                cursor,
                batch: ScopedTransferBatch::default(),
                _charge: charge,
            },
        );
        Ok(cursor)
    }

    fn next_source(
        transition: &ScopedQueueTransition,
        start: usize,
    ) -> Result<ScopedTransferCursor, QueueError> {
        let source = transition
            .frozen
            .iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, cut)| cut.map(|_| index))
            .unwrap_or(transition.frozen.len());
        Ok(ScopedTransferCursor {
            source,
            after: None,
            inventory: scoped_inventory(transition, &transition.previous)?,
        })
    }

    pub(crate) fn advance_transfer(
        &mut self,
        transition: &ScopedQueueTransition,
        basis: Digest,
        cursor: ScopedTransferCursor,
        record: Option<PreparedScopedTransfer>,
    ) -> Result<(), QueueError> {
        let group = transition.next.group.group_id;
        let mut progress = self.transfers.remove(&group).ok_or(QueueError::Stale)?;
        let result = (|| {
            if progress.basis != basis || progress.cursor != cursor {
                return Err(QueueError::Stale);
            }
            let expected = transition
                .frozen
                .get(cursor.source)
                .and_then(|cut| *cut)
                .ok_or(QueueError::Stale)?;
            if let Some(record) = record {
                if cursor.inventory.count >= expected.count {
                    return Err(QueueError::Conflict);
                }
                let mut inventory = cursor.inventory;
                append_scoped_inventory(&mut inventory, record.source)?;
                let after = Some(record.source.request_id());
                self.stage_transfer(group, &mut progress.batch, record)?;
                progress.cursor = ScopedTransferCursor {
                    inventory,
                    after,
                    ..cursor
                };
            } else {
                if cursor.inventory != expected {
                    return Err(QueueError::Conflict);
                }
                progress.cursor = Self::next_source(transition, cursor.source + 1)?;
            }
            Ok(())
        })();
        self.transfers.insert(group, progress);
        result
    }

    pub(crate) fn transfer_inventory(
        &self,
        transition: &ScopedQueueTransition,
        basis: Digest,
    ) -> Result<ScopedQueueInventory, QueueError> {
        let progress = self
            .transfers
            .get(&transition.next.group.group_id)
            .ok_or(QueueError::Stale)?;
        if progress.basis != basis || progress.cursor.source != transition.frozen.len() {
            return Err(QueueError::Stale);
        }
        progress.batch.inventory(transition)
    }

    pub(crate) fn finish_transfer(
        &mut self,
        transition: &ScopedQueueTransition,
        basis: Digest,
    ) -> Result<(), QueueError> {
        self.transfer_inventory(transition, basis)?;
        let group = transition.next.group.group_id;
        let mut progress = self.transfers.remove(&group).ok_or(QueueError::Stale)?;
        let result = self.install_transfer(transition, &mut progress.batch);
        if result.is_err() {
            self.transfers.insert(group, progress);
        }
        result
    }

    fn stage_transfer(
        &self,
        group: Uuid,
        batch: &mut ScopedTransferBatch,
        prepared: PreparedScopedTransfer,
    ) -> Result<(), QueueError> {
        let entry = prepared.entry;
        let id = entry.payload.request.request_id;
        let source = prepared.source;
        // Both canonical stamps were verified against the complete incoming
        // sections before staging. An exact local source can share its
        // immutable raw backing while each epoch owns its own header.
        let previous = self
            .records
            .get(&(group, source.payload.placement, id))
            .filter(|record| {
                record.stamp == source.payload && record.reservation == source.reservation
            });
        let previous_terminal = previous
            .and_then(|record| record.terminal.as_deref())
            .filter(|terminal| Some(terminal.stamp) == source.terminal);
        if let Some(retained) = batch.records.get_mut(&id) {
            if retained.stamp != entry.payload || retained.reservation != entry.reservation {
                return Err(QueueError::Conflict);
            }
            if let Some(prepared_terminal) = prepared.terminal {
                match retained.terminal_stamp() {
                    Some(stamp) if stamp != prepared_terminal.0 => {
                        return Err(QueueError::Conflict);
                    }
                    None => {
                        retained.terminal =
                            Some(self.retain_terminal(prepared_terminal, previous_terminal)?)
                    }
                    Some(_) => {}
                }
            }
            retained.recovered_marker |= entry.marked;
            return Ok(());
        }
        let payload = self.retain_payload(prepared.payload, previous, None)?;
        let terminal = prepared
            .terminal
            .map(|terminal| self.retain_terminal(terminal, previous_terminal))
            .transpose()?;
        batch.records.insert(
            id,
            ScopedSurvivor {
                reservation: entry.reservation,
                stamp: entry.payload,
                marker: None,
                acceptance: None,
                terminal_quorum: None,
                recovered_marker: entry.marked,
                terminal_verified: false,
                pin_released: false,
                epoch: prepared.epoch,
                sequence: prepared.sequence,
                payload,
                terminal,
            },
        );
        Ok(())
    }

    fn install_transfer(
        &mut self,
        transition: &ScopedQueueTransition,
        batch: &mut ScopedTransferBatch,
    ) -> Result<(), QueueError> {
        let destination = &transition.next;
        let mut epoch = ScopedLocalEpoch::published(destination)?;
        let source = placement_digest(&transition.previous)?;
        epoch.candidate = Some((transition.id, source));
        if !self.local_holder(destination) || self.is_frozen(destination) {
            return Err(QueueError::Stale);
        }
        let group = destination.group.group_id;
        let mut retained = self
            .records
            .range(scoped_range(group, epoch.placement))
            .peekable();
        if retained.peek().is_some() {
            for (id, incoming) in &batch.records {
                let Some(((_, _, retained_id), old)) = retained.next() else {
                    return Err(QueueError::Conflict);
                };
                if retained_id != id
                    || old.epoch != epoch
                    || old.inventory_entry() != incoming.inventory_entry()
                {
                    return Err(QueueError::Conflict);
                }
            }
            return if retained.next().is_none() {
                Ok(())
            } else {
                Err(QueueError::Conflict)
            };
        }
        let added = batch
            .records
            .values()
            .filter(|record| record.terminal.is_none())
            .count();
        let previous = self
            .records
            .range(scoped_range(group, source))
            .filter(|(_, record)| record.terminal.is_none())
            .count();
        let pending = self
            .pending
            .checked_add(added)
            .ok_or(QueueError::Overloaded)?;
        let admissions = self
            .usage
            .lock()
            .map_err(|_| QueueError::Unavailable)?
            .admissions;
        if (pending - previous).saturating_add(admissions) > self.limits.max_pending_requests {
            return Err(QueueError::Overloaded);
        }
        for (id, mut record) in std::mem::take(&mut batch.records) {
            debug_assert_eq!(record.stamp.placement, epoch.placement);
            record.epoch = epoch;
            self.records.insert((group, epoch.placement, id), record);
        }
        self.pending = pending;
        Ok(())
    }

    pub(crate) fn install_terminal_repair(
        &mut self,
        current: &ScopedQueuePlacement,
        witness: ScopedCopyAck,
        prepared: PreparedScopedTransfer,
        insert: ScopedQueueInsert,
    ) -> Result<ScopedCopyAck, QueueError> {
        let payload = prepared.entry.payload;
        let stamp = prepared.entry.terminal.ok_or(QueueError::Invalid)?;
        self.authorize(current, witness.worker, stamp)?;
        self.require_insert(
            &insert,
            current.group.group_id,
            stamp.request.run_id,
            stamp.request.request_id,
        )?;
        if witness.stamp != stamp || self.local == witness.worker {
            return Err(QueueError::Conflict);
        }
        let key = (
            current.group.group_id,
            stamp.placement,
            stamp.request.request_id,
        );
        if let Some(retained) = self.records.get(&key) {
            if retained.stamp != payload
                || retained.reservation != prepared.entry.reservation
                || retained.terminal_stamp().is_some_and(|old| old != stamp)
            {
                return Err(QueueError::Conflict);
            }
            let terminal = if retained.terminal.is_none() {
                Some(self.retain_terminal(prepared.terminal.ok_or(QueueError::Invalid)?, None)?)
            } else {
                None
            };
            let retained = self.records.get_mut(&key).ok_or(QueueError::Unavailable)?;
            if terminal.is_some() {
                retained.terminal = terminal;
                self.pending -= 1;
            }
            retained.recovered_marker = true;
            retained.terminal_verified = true;
        } else {
            let mut batch = ScopedTransferBatch::default();
            self.stage_transfer(current.group.group_id, &mut batch, prepared)?;
            let mut repaired = batch
                .records
                .remove(&key.2)
                .ok_or(QueueError::Unavailable)?;
            repaired.terminal_verified = true;
            self.records.insert(key, repaired);
        }
        Ok(self.ack(stamp))
    }

    pub(crate) fn begin_insert(
        &self,
        group: Uuid,
        run: Uuid,
        request: Uuid,
    ) -> Result<ScopedQueueInsert, QueueError> {
        let key = (group, run, request);
        let mut active = self
            .active_inserts
            .lock()
            .map_err(|_| QueueError::Unavailable)?;
        let registration = if let Some(registration) = active.get(&key).and_then(Weak::upgrade) {
            registration
        } else {
            let charge = self
                .shared
                .reserve_scratch(ByteClass::Data, INSERT_REGISTRATION_BYTES)
                .map_err(|_| QueueError::Overloaded)?;
            let registration = Arc::new(ScopedInsertRegistration {
                key,
                active: Arc::downgrade(&self.active_inserts),
                _charge: charge,
            });
            active.insert(key, Arc::downgrade(&registration));
            registration
        };
        Ok(ScopedQueueInsert {
            registration,
            epoch_revision: self.write_revision(group).epoch,
        })
    }

    fn require_insert(
        &self,
        insert: &ScopedQueueInsert,
        group: Uuid,
        run: Uuid,
        request: Uuid,
    ) -> Result<(), QueueError> {
        let key = (group, run, request);
        if insert.registration.key != key
            || insert.epoch_revision != self.write_revision(group).epoch
        {
            return Err(QueueError::Stale);
        }
        let active = self
            .active_inserts
            .lock()
            .map_err(|_| QueueError::Unavailable)?;
        if active
            .get(&key)
            .is_none_or(|entry| entry.as_ptr() != Arc::as_ptr(&insert.registration))
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    fn fence_expired_request(
        &mut self,
        group: Uuid,
        run: Uuid,
        request: Uuid,
    ) -> Result<(), QueueError> {
        // Transfer inventory checks still observe every removal in the group.
        let revision = &mut self.write_revisions[Self::write_slot(group)].expiry;
        let next = revision.checked_add(1).ok_or(QueueError::Unavailable)?;
        self.active_inserts
            .lock()
            .map_err(|_| QueueError::Unavailable)?
            .remove(&(group, run, request));
        *revision = next;
        Ok(())
    }

    pub(crate) fn write_revision(&self, group: Uuid) -> ScopedWriteRevision {
        self.write_revisions[Self::write_slot(group)]
    }

    pub(crate) fn require_write_revision(
        &self,
        group: Uuid,
        revision: ScopedWriteRevision,
    ) -> Result<(), QueueError> {
        if self.write_revision(group) != revision {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    fn fence_epoch_writes(&mut self, group: Uuid) -> Result<(), QueueError> {
        let slot = &mut self.write_revisions[Self::write_slot(group)].epoch;
        *slot = slot.checked_add(1).ok_or(QueueError::Unavailable)?;
        Ok(())
    }

    fn write_slot(group: Uuid) -> usize {
        group.as_bytes()[15] as usize % 32
    }

    pub(crate) fn cleanup_epoch(
        &self,
        group: Uuid,
        after: Option<Digest>,
    ) -> Option<ScopedLocalEpoch> {
        let min = Digest::from_bytes([0; 32]);
        let max = Digest::from_bytes([u8::MAX; 32]);
        let start = after.map_or(Included((group, min, Uuid::nil())), |digest| {
            Excluded((group, digest, Uuid::from_u128(u128::MAX)))
        });
        let record = self
            .records
            .range((start, Included((group, max, Uuid::from_u128(u128::MAX)))))
            .next()
            .map(|(_, record)| record.epoch);
        // Frozen cuts may be empty and still reserve space.
        let fence = self
            .frozen
            .get(&group)
            .map(|(epoch, _)| *epoch)
            .filter(|epoch| after.is_none_or(|after| epoch.placement > after));
        let transfer = self
            .transfers
            .get(&group)
            .map(|entry| entry.epoch)
            .filter(|epoch| after.is_none_or(|after| epoch.placement > after));
        [record, fence, transfer]
            .into_iter()
            .flatten()
            .min_by_key(|epoch| epoch.placement)
    }

    /// Only called after the endpoint authenticates Control's retirement of
    /// this exact epoch. Leave charged frames borrowed by in-flight readers
    /// owned by their original Arc until the last reader drops them.
    pub(crate) fn release_epoch(
        &mut self,
        group: Uuid,
        epoch: ScopedLocalEpoch,
    ) -> Result<(), QueueError> {
        const RECORDS_PER_PASS: usize = 8;
        if self
            .transfers
            .get(&group)
            .is_some_and(|progress| progress.epoch == epoch)
        {
            self.fence_epoch_writes(group)?;
            self.transfers.remove(&group);
        }
        if self
            .records
            .range(scoped_range(group, epoch.placement))
            .next()
            .is_none()
            && self
                .frozen
                .get(&group)
                .is_none_or(|(fence, _)| fence.placement != epoch.placement)
        {
            return Ok(());
        }
        self.fence_epoch_writes(group)?;
        for _ in 0..RECORDS_PER_PASS {
            let Some(key) = self
                .records
                .range(scoped_range(group, epoch.placement))
                .next()
                .map(|(key, _)| *key)
            else {
                if self
                    .frozen
                    .get(&group)
                    .is_some_and(|(fence, _)| fence.placement == epoch.placement)
                {
                    self.frozen.remove(&group);
                }
                break;
            };
            if self
                .records
                .remove(&key)
                .is_some_and(|record| record.terminal.is_none())
            {
                self.pending -= 1;
            }
        }
        Ok(())
    }
}

fn scoped_range(
    group: Uuid,
    digest: Digest,
) -> (
    std::ops::Bound<(Uuid, Digest, Uuid)>,
    std::ops::Bound<(Uuid, Digest, Uuid)>,
) {
    (
        Excluded((group, digest, Uuid::nil())),
        Included((group, digest, Uuid::from_u128(u128::MAX))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::WorkerResources, oltp::pool::SharedResourceConfig};

    #[test]
    fn q03_q06_scoped_expiry_only_fences_inserts_for_its_request() {
        let shared = WorkerSharedResources::new(
            81,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let mut store = ScopedQueueStore::new(
            shared.clone(),
            QueueScope {
                cluster_id: nabedra_rt::ids::uuid_v4(),
                database: Text::try_from("default").unwrap(),
            },
            WorkerIdentity {
                worker_id: nabedra_rt::ids::uuid_v4(),
                incarnation: nabedra_rt::ids::uuid_v4(),
                certificate_sha256: Digest::from_bytes([1; 32]),
                enrollment_revision: 1,
            },
            QueueLimits {
                max_pending_requests: 20,
                max_queue_bytes_per_replica: 4 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 100,
            },
        )
        .unwrap();
        let group = nabedra_rt::ids::uuid_v4();
        let run = nabedra_rt::ids::uuid_v4();
        let expired = nabedra_rt::ids::uuid_v4();
        let unrelated = nabedra_rt::ids::uuid_v4();
        let old_payload = store.begin_insert(group, run, expired).unwrap();
        let old_repair = store.begin_insert(group, run, expired).unwrap();
        let new_payload = store.begin_insert(group, run, unrelated).unwrap();
        let transfer = store.write_revision(group);
        assert_eq!(shared.working_decode_bytes(), 2 * INSERT_REGISTRATION_BYTES);

        store.fence_expired_request(group, run, expired).unwrap();
        assert_eq!(
            store.require_insert(&old_payload, group, run, expired),
            Err(QueueError::Stale)
        );
        assert_eq!(
            store.require_insert(&old_repair, group, run, expired),
            Err(QueueError::Stale)
        );
        assert_eq!(
            store.require_insert(&new_payload, group, run, unrelated),
            Ok(())
        );
        assert_eq!(
            store.require_write_revision(group, transfer),
            Err(QueueError::Stale)
        );

        // Later copies independently recheck Timestamp or the owner's witness.
        // Dropping earlier copies cannot remove the later registration.
        let later_copy = store.begin_insert(group, run, expired).unwrap();
        drop((old_payload, old_repair));
        assert_eq!(
            store.require_insert(&later_copy, group, run, expired),
            Ok(())
        );
        assert_eq!(shared.working_decode_bytes(), 2 * INSERT_REGISTRATION_BYTES);

        store.fence_epoch_writes(group).unwrap();
        assert_eq!(
            store.require_insert(&new_payload, group, run, unrelated),
            Err(QueueError::Stale)
        );
        assert_eq!(
            store.require_insert(&later_copy, group, run, expired),
            Err(QueueError::Stale)
        );
        drop((new_payload, later_copy));
        assert!(store.active_inserts.lock().unwrap().is_empty());
        assert_eq!(shared.working_decode_bytes(), 0);
    }
}
