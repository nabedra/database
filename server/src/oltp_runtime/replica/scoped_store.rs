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
            control::ScopedQueuePlacement,
            status::{
                IdentityReservation, RequestKind, ScopedAdmission, VerifiedSettlement,
                VerifiedStatusDecision,
            },
        },
        multiplex::{Purpose, ReceivedReply},
        owned::serialized_digest,
    },
    protocol::Operation,
};
use std::{
    ops::Bound::{Excluded, Included},
    sync::Mutex,
};

// Includes fixed state, ordered-map nodes and ownership metadata.
const ENTRY_BYTES: usize = 2048;
const TERMINAL_BYTES: usize = 512;
const STORE_BYTES: usize = 1024;

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
    request: RequestBinding,
    payload: RecordVersion,
    copied: RecordVersion,
    phase: CopyPhase,
}

impl ScopedCopyStamp {
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
    ) -> Result<Self, QueueError> {
        let purpose = match expected.phase {
            CopyPhase::Marker => Purpose::QueueControl,
            _ => Purpose::QueueData,
        };
        Self::authenticate_on(reply, rpc_group, target, expected, purpose)
    }

    pub(crate) fn authenticate_payload_witness(
        reply: &ReceivedReply,
        rpc_group: &GroupIdentity,
        target: WorkerIdentity,
        expected: ScopedCopyStamp,
    ) -> Result<Self, QueueError> {
        expected.payload_claim()?;
        Self::authenticate_on(reply, rpc_group, target, expected, Purpose::QueueControl)
    }

    fn authenticate_on(
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
                .all(|worker| current.members.contains(worker))
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }
}

fn placement_digest(current: &ScopedQueuePlacement) -> Result<Digest, QueueError> {
    current.validate(&current.group).map_err(invalid)?;
    serialized_digest(&("nabedra/scoped-ram-queue/v2", current)).map_err(invalid)
}

fn validate_scoped_frame_header(
    current: &ScopedQueuePlacement,
    header: &RecordHeader,
) -> Result<(), QueueError> {
    let mut ready = ReadySetHasher::default();
    for member in current.members {
        ready.push(member).map_err(invalid)?;
    }
    let scope = QueueScope {
        cluster_id: current.group.cluster_id,
        database: current.group.database.clone(),
    };
    let authority = QueueAuthority {
        ready: ReadyMembership {
            view: current.view,
            ready_root: ready.finish(),
            ready_count: ready.count(),
        },
        owner: current.owner,
    };
    // The outer scoped copy carries the version-2 placement. Its bounded
    // body/context/result envelope keeps the existing frame codec.
    header.preflight(&scope, authority)?;
    if header.all_copy_evidence || !header.read_grants.is_initial() {
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
    header: Box<[u8]>,
    body: Box<[u8]>,
    context: Box<[u8]>,
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
        let placement = placement_digest(current)?;
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
            stamp: ScopedCopyStamp {
                placement,
                request: binding,
                payload: version,
                copied: version,
                phase: CopyPhase::Payload,
            },
            sequence: header.sequence,
            header: frame.header.as_ref().into(),
            body: body.into(),
            context: context_bytes.into(),
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
}

pub(crate) struct PreparedScopedTerminal {
    stamp: ScopedCopyStamp,
    reservation: IdentityReservation,
    sequence: u64,
    header: Box<[u8]>,
    result: Box<[u8]>,
    _working: ScratchLease,
}

impl PreparedScopedTerminal {
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
        let binding = accepted.stamp.request;
        if accepted.stamp.phase != CopyPhase::Marker
            || !reservation.binds(binding)
            || header.request_id != binding.request_id
            || header.digest != binding.digest
            || hash(section(&frame, 0)?) != binding.digest
            || hash(section(&frame, 1)?) != binding.accepted_context
            || version.version <= accepted.stamp.payload.version
            || !header.accepted
            || !matches!(header.state, RequestState::Succeeded | RequestState::Failed)
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
            _ => return Err(QueueError::Invalid),
        }
        Ok(Self {
            stamp: accepted.stamp.terminal(version),
            reservation,
            sequence: header.sequence,
            header: frame.header.as_ref().into(),
            result: result.into(),
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

struct RetainedPayload {
    header: Box<[u8]>,
    body: Box<[u8]>,
    context: Box<[u8]>,
    _charge: RetainedCharge,
}
struct RetainedTerminal {
    stamp: ScopedCopyStamp,
    header: Box<[u8]>,
    result: Box<[u8]>,
    _charge: RetainedCharge,
}

#[derive(Clone)]
pub(crate) struct ScopedSurvivor {
    pub reservation: IdentityReservation,
    pub stamp: ScopedCopyStamp,
    pub marker: Option<ScopedCopyQuorum>,
    pub acceptance: Option<ScopedCopyQuorum>,
    pub terminal_quorum: Option<ScopedCopyQuorum>,
    sequence: u64,
    payload: Arc<RetainedPayload>,
    terminal: Option<Arc<RetainedTerminal>>,
}

impl ScopedSurvivor {
    pub(crate) fn terminal_stamp(&self) -> Option<ScopedCopyStamp> {
        self.terminal.as_ref().map(|terminal| terminal.stamp)
    }

    /// Deliberately export the original epoch. A receiver in a later epoch has
    /// to verify transfer and reconcile the decision before re-stamping it.
    pub(crate) fn frames(
        &self,
        shared: &WorkerSharedResources,
    ) -> Result<(RecordFrame, Option<RecordFrame>), QueueError> {
        let charged = |bytes| shared.copy_bytes(bytes).map_err(|_| QueueError::Overloaded);
        let body = charged(&self.payload.body)?;
        let context = charged(&self.payload.context)?;
        let terminal = self
            .terminal
            .as_ref()
            .map(|terminal| {
                Ok::<_, QueueError>(RecordFrame {
                    header: charged(&terminal.header)?,
                    sections: [
                        Some(body.clone()),
                        Some(context.clone()),
                        Some(charged(&terminal.result)?),
                    ],
                })
            })
            .transpose()?;
        Ok((
            RecordFrame {
                header: charged(&self.payload.header)?,
                sections: [Some(body), Some(context), None],
            },
            terminal,
        ))
    }
}

pub(crate) struct ScopedQueueStore {
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    local: WorkerIdentity,
    limits: QueueLimits,
    usage: Arc<Mutex<Usage>>,
    pending: usize,
    records: BTreeMap<(Uuid, Uuid), ScopedSurvivor>,
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
            })),
            pending: 0,
            records: BTreeMap::new(),
            _metadata: metadata,
        })
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
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    fn local_holder(&self, placement: &ScopedQueuePlacement) -> bool {
        placement.group.cluster_id == self.scope.cluster_id
            && placement.group.database == self.scope.database
            && placement.members.contains(&self.local)
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
        if !self.local_holder(current) || request.run_id != current.view.run_id {
            return Err(QueueError::Stale);
        }
        let placement = placement_digest(current)?;
        let record = self
            .records
            .get(&(current.group.group_id, request.request_id))
            .ok_or(QueueError::Unavailable)?;
        if record.stamp.placement != placement
            || record.stamp.payload_claim()? != (request, version)
        {
            return Err(QueueError::Conflict);
        }
        Ok(self.ack(record.stamp))
    }

    pub(crate) fn install_payload(
        &mut self,
        current: &ScopedQueuePlacement,
        sender: WorkerIdentity,
        payload: PreparedScopedPayload,
    ) -> Result<ScopedCopyAck, QueueError> {
        let stamp = payload.stamp;
        self.authorize(current, sender, stamp)?;
        let key = (current.group.group_id, stamp.request.request_id);
        if let Some(old) = self.records.get(&key) {
            if old.stamp != stamp || old.reservation != payload.reservation {
                return Err(QueueError::Conflict);
            }
            return Ok(self.ack(stamp));
        }
        if self.pending >= self.limits.max_pending_requests {
            return Err(QueueError::Overloaded);
        }
        // Keep room for terminal metadata even after subsequent admissions
        // have filled the queue arena. Result payloads have their own budget.
        let bytes = (ENTRY_BYTES + TERMINAL_BYTES + MAX_RECORD_HEADER_BYTES)
            .checked_add(payload.header.len())
            .and_then(|n| n.checked_add(payload.body.len()))
            .and_then(|n| n.checked_add(payload.context.len()))
            .ok_or(QueueError::Overloaded)?;
        let charge = self.reserve(bytes, 0)?;
        self.records.insert(
            key,
            ScopedSurvivor {
                reservation: payload.reservation,
                stamp,
                sequence: payload.sequence,
                marker: None,
                acceptance: None,
                terminal_quorum: None,
                payload: Arc::new(RetainedPayload {
                    header: payload.header,
                    body: payload.body,
                    context: payload.context,
                    _charge: charge,
                }),
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
            .get_mut(&(current.group.group_id, bound.request_id))
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
        let key = (current.group.group_id, stamp.request.request_id);
        let record = self.records.get(&key).ok_or(QueueError::Unavailable)?;
        if record.marker.is_none()
            || record.reservation != terminal.reservation
            || record.sequence != terminal.sequence
            || record.stamp.terminal(stamp.copied) != stamp
        {
            return Err(QueueError::Conflict);
        }
        if let Some(old) = &record.terminal {
            return if old.stamp == stamp {
                Ok(self.ack(stamp))
            } else {
                Err(QueueError::Conflict)
            };
        }
        let charge = self.reserve(0, terminal.result.len())?;
        let record = self.records.get_mut(&key).ok_or(QueueError::Unavailable)?;
        record.terminal = Some(Arc::new(RetainedTerminal {
            stamp,
            header: terminal.header,
            result: terminal.result,
            _charge: charge,
        }));
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
            .get_mut(&(current.group.group_id, quorum.stamp.request.request_id))
            .ok_or(QueueError::Unavailable)?;
        let confirmation = match quorum.stamp.phase {
            CopyPhase::Marker
                if record.marker.is_some() && record.stamp.marker() == quorum.stamp =>
            {
                &mut record.acceptance
            }
            CopyPhase::Terminal
                if record.acceptance.is_some() && record.terminal_stamp() == Some(quorum.stamp) =>
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
        let first = Excluded((group, after.unwrap_or(Uuid::nil())));
        let last = Included((group, Uuid::from_u128(u128::MAX)));
        Ok(self
            .records
            .range((first, last))
            .filter_map(|(_, record)| (record.stamp.placement == digest).then_some(record))
            .next()
            .cloned())
    }
}
