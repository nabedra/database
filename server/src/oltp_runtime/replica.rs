//! Worker-memory queue records for the typed distributed path.
//!
//! This module has no disk interface and cannot grant run authority. Its caller
//! supplies a fresh Control view and authenticated peer identity. The installed
//! RF1 server composes this same queue with durable typed data/status groups.
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, QueueLease, WorkerSharedResources},
    oltp_replication::{ReadySetHasher, service_rpc::ReadGrant, types::*},
    protocol,
    request_queue::{
        QueueError, QueueLimits, RequestQueue, RequestRecord, RequestState, ValidatedRecord,
        ValidatedResponse,
    },
    scheduling::MemoryCharge,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};
use uuid::Uuid;

pub const QUEUE_FORMAT_VERSION: u32 = 1;
pub const MAX_HEADER_BYTES: usize = 4096;
pub(crate) const MAX_RECORD_HEADER_BYTES: usize = 8192;
pub const MAX_CONTEXT_BYTES: usize = 4096;

fn hash(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

mod queue_latency;
use queue_latency::{AcceptanceTiming, QueueLatency, QueueLatencySnapshot};
#[cfg(test)]
pub(super) mod scoped;
pub(super) mod scoped_store;
mod removal;
mod read_grants;
pub use read_grants::ReadGrantSet;
pub(crate) use read_grants::MAX_READ_GRANTS;
#[cfg(test)]
mod tests;
mod transfer;
pub use transfer::{JoinPage, JoinPageHeader, JoinPhase, JoinSnapshot};
fn invalid<T>(_: T) -> QueueError {
    QueueError::Invalid
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueScope {
    pub cluster_id: Uuid,
    pub database: Text<128>,
}
impl QueueScope {
    fn validate(&self) -> Result<(), QueueError> {
        if self.cluster_id.is_nil() || !crate::oltp::valid_database(&self.database) {
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

/// Catalog MVCC resolves this lineage at storage_pin.read_ts. A TSO allocation
/// alone cannot manufacture schema/routing lineage supplied by a caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReadToken {
    pub identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub catalog_root: Digest,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedContext {
    pub authorization: OriginalAuthorization,
    pub read: Option<QueueReadToken>,
}
impl AcceptedContext {
    fn validate(&self, scope: &QueueScope, record: &RequestRecord) -> Result<(), QueueError> {
        self.validate_binding(
            scope,
            record.run_id,
            record.request_id,
            Digest::parse(&record.digest).map_err(invalid)?,
            protocol::parse_request(&record.body)
                .map_err(invalid)?
                .operation
                .is_mutation(),
        )
    }
    fn validate_binding(
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
    #[serde(default = "ReadGrantSet::initial", skip_serializing_if = "ReadGrantSet::is_initial")]
    pub(crate) read_grants: ReadGrantSet,
}
impl RecordHeader {
    pub fn preflight(&self, scope: &QueueScope, current: QueueAuthority) -> Result<(), QueueError> {
        if self.queue_format_version != QUEUE_FORMAT_VERSION
            || self.scope != *scope
            || self.ready != current.ready
            || self.owner != current.owner
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
        self.read_grants.validate(self.ready.view, self.request_id, self.digest, self.accepted)?;
        Ok(())
    }
}

/// The transport retains each section's charge through sending/cancellation.
/// Receive must allocate charged sections only after header preflight succeeds.
#[derive(Clone, Debug)]
pub struct RecordFrame {
    pub header: ChargedBytes,
    pub sections: [Option<ChargedBytes>; 3],
}
impl RecordFrame {
    pub fn record_version(&self) -> Result<RecordVersion, QueueError> {
        self.version(&self.decode_header()?)
    }
    pub fn decode_header(&self) -> Result<RecordHeader, QueueError> {
        if self.header.len() > MAX_RECORD_HEADER_BYTES {
            return Err(QueueError::Invalid);
        }
        serde_json::from_slice(self.header.as_ref()).map_err(invalid)
    }
    fn version(&self, header: &RecordHeader) -> Result<RecordVersion, QueueError> {
        Self::canonical_version(
            header,
            self.sections
                .each_ref()
                .map(|section| section.as_ref().map_or(&[][..], AsRef::as_ref)),
        )
    }
    fn canonical_version(
        header: &RecordHeader,
        sections: [&[u8]; 3],
    ) -> Result<RecordVersion, QueueError> {
        let mut canonical = header.clone();
        // Copy evidence grows without changing the immutable record version.
        canonical.all_copy_evidence = false;
        struct Writer(Sha256);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Writer(Sha256::new());
        writer.0.update(b"nabedra/ram-queue-record/v1\0");
        serde_json::to_writer(&mut writer, &canonical).map_err(invalid)?;
        for (index, bytes) in sections.into_iter().enumerate() {
            if bytes.len() as u64 != header.section_lengths[index] {
                return Err(QueueError::Invalid);
            }
            writer.0.update(bytes);
        }
        Ok(RecordVersion {
            request_id: header.request_id,
            version: header.version,
            digest: Digest::from_bytes(writer.0.finalize().into()),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordVersion {
    pub request_id: Uuid,
    pub version: u64,
    pub digest: Digest,
}

/// Constructed by an authenticated transport callback, not deserializable as a
/// capability. The callback must check the response nonce and enrolled sender.
#[derive(Debug)]
pub struct CopyAcknowledgement {
    worker: WorkerIdentity,
    record: RecordVersion,
}

#[derive(Clone)]
pub struct InstallEnvironment {
    scope: QueueScope,
    authority: QueueAuthority,
    members: Arc<BTreeMap<Uuid, WorkerIdentity>>,
}
// Includes the BTreeMap node and fixed metadata. The queue reserves this slot
// from initial admission through eventual durable identity retirement.
const PROOF_METADATA_BYTES: usize = 2560;
#[derive(Clone)]
struct TerminalMetadata {
    version: u64,
    request: RequestBinding,
    response_hash: Digest,
    pin: Option<(PinIdentity, StoragePin)>,
}
const _: () = assert!(
    std::mem::size_of::<TerminalMetadata>() + std::mem::size_of::<CopyBarrier>() + 256
        <= PROOF_METADATA_BYTES
);
impl TerminalMetadata {
    fn prepare(
        record: &RequestRecord,
        context: &AcceptedContext,
        context_bytes: &[u8],
        response: &[u8],
    ) -> Result<Self, QueueError> {
        Ok(Self {
            version: record.version,
            request: RequestBinding {
                run_id: record.run_id,
                request_id: record.request_id,
                digest: Digest::parse(&record.digest).map_err(invalid)?,
                accepted_context: hash(context_bytes),
            },
            response_hash: hash(response),
            pin: context
                .read
                .as_ref()
                .map(|read| (read.identity.clone(), read.storage_pin)),
        })
    }
}
/// A bounded immutable descriptor copied under the queue lock, then decoded and
/// hashed on the owned native executor before the completion transition.
pub struct TerminalInput {
    version: u64,
    request: RequestBinding,
    context: Vec<u8>,
}
pub struct PreparedTerminal {
    input: TerminalInput,
    metadata: TerminalMetadata,
    response: ValidatedResponse,
    now_ms: u64,
}
impl PreparedTerminal {
    pub fn prepare(
        input: TerminalInput,
        status: u16,
        body: Vec<u8>,
        now_ms: u64,
    ) -> Result<Self, QueueError> {
        if !(200..600).contains(&status) || body.is_empty() || body.len() > protocol::MAX_BODY_BYTES
        {
            return Err(QueueError::Invalid);
        }
        let context: AcceptedContext = serde_json::from_slice(&input.context).map_err(invalid)?;
        let metadata = TerminalMetadata {
            version: input.version,
            request: RequestBinding {
                accepted_context: hash(&input.context),
                ..input.request
            },
            response_hash: hash(&body),
            pin: context.read.map(|read| (read.identity, read.storage_pin)),
        };
        let response = ValidatedResponse::prepare(input.request.request_id, status, body)?;
        Ok(Self {
            input,
            metadata,
            response,
            now_ms,
        })
    }
}
pub struct PreparedInstall {
    authority: QueueAuthority,
    version: RecordVersion,
    record: ValidatedRecord,
    terminal: Option<TerminalMetadata>,
    read_grants: ReadGrantSet,
}
impl PreparedInstall {
    pub(crate) fn record_id(&self) -> Uuid {
        self.version.request_id
    }

    /// Run under a native/task/scratch owner outside the queue lock.
    pub fn prepare(
        environment: InstallEnvironment,
        frame: &RecordFrame,
    ) -> Result<Self, QueueError> {
        let header = frame.decode_header()?;
        header.preflight(&environment.scope, environment.authority)?;
        let version = frame.version(&header)?;
        let section = |index: usize| {
            frame.sections[index]
                .as_ref()
                .map(AsRef::as_ref)
                .ok_or(QueueError::Invalid)
        };
        let body = section(0)?;
        let context_bytes = section(1)?;
        let context: AcceptedContext = serde_json::from_slice(context_bytes).map_err(invalid)?;
        header.read_grants.validate_context(&context, context_bytes)?;
        let record = RequestRecord {
            context: None,
            run_id: header.ready.view.run_id,
            scheduler_term: header.ready.view.owner_term,
            membership_revision: header.ready.view.membership_revision,
            request_id: header.request_id,
            digest: header.digest.to_string(),
            sequence: header.sequence,
            version: header.version,
            body: body.to_vec(),
            accepted: header.accepted,
            state: header.state,
            pinned_catalog: Some(context_bytes.to_vec()),
            response_status: header.response_status,
            response_body: frame.sections[2].as_ref().map(|v| v.as_ref().to_vec()),
            completed_at_ms: header.completed_at_ms,
            result_expired: false,
            replicas: if header.all_copy_evidence {
                environment.members.keys().copied().collect()
            } else {
                Default::default()
            },
        };
        context.validate(&environment.scope, &record)?;
        let terminal = record
            .state
            .terminal()
            .then(|| {
                TerminalMetadata::prepare(
                    &record,
                    &context,
                    context_bytes,
                    record.response_body.as_deref().ok_or(QueueError::Invalid)?,
                )
            })
            .transpose()?;
        Ok(Self {
            authority: environment.authority,
            version,
            record: ValidatedRecord::prepare(record)?,
            terminal,
            read_grants: header.read_grants,
        })
    }
}
impl CopyAcknowledgement {
    pub(crate) fn authenticated(worker: WorkerIdentity, record: RecordVersion) -> Self {
        Self { worker, record }
    }
    pub fn record(&self) -> RecordVersion {
        self.record
    }
}

struct CopyBarrier {
    version: RecordVersion,
    ready: ReadySetHasher,
    complete: bool,
    terminal_confirmed: bool,
    acceptance_timing: AcceptanceTiming,
}

/// One physical worker's queue, constructed once and shared by all hosted pools.
/// It is intentionally not Clone. Queue/result maxima are reserved once against
/// WorkerSharedResources; all frame copies use its separately bounded arena.
pub struct WorkerReplica {
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    local: WorkerIdentity,
    authority: QueueAuthority,
    members: Arc<BTreeMap<Uuid, WorkerIdentity>>,
    queue: RequestQueue,
    scoped: Option<Box<scoped_store::ScopedQueueStore>>,
    barriers: BTreeMap<Uuid, CopyBarrier>,
    queue_latency: QueueLatency,
    terminal_metadata: BTreeMap<Uuid, TerminalMetadata>,
    read_grants: BTreeMap<Uuid, read_grants::GrantEntry>,
    retired_through_sequence: u64,
    retirement_generation: u64,
    change_cut: u64,
    changes: BTreeMap<Uuid, u64>,
    export: Option<transfer::JoinExport>,
    import: Option<transfer::JoinImport>,
    bootstrap: Option<JoinTicket>,
    removal: Option<JoinTicket>,
    removal_root: Option<QueueCopyStatement>,
    removal_worker_after: Option<Uuid>,
    removal_record_after: Option<Uuid>,
    _memory: MemoryCharge,
    _membership_memory: MemoryCharge,
    _queue_owner: QueueLease,
    retention_ms: u64,
}
#[derive(serde::Serialize)]
pub(crate) struct QueueStats {
    pub accepted_records: u64,
    pub queued_records: u64,
    pub running_records: u64,
    pub reconciling_records: u64,
    pub terminal_records: u64,
    pub charged_queue_bytes: usize,
    pub charged_result_bytes: usize,
}
impl WorkerReplica {
    /// One physical queue lease owns both formats. An enabled worker must
    /// carve scoped capacity out of the existing queue envelope.
    pub(crate) fn new_with_scoped_queues(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        authority: QueueAuthority,
        members: impl IntoIterator<Item = WorkerIdentity>,
        total: QueueLimits,
        scoped: QueueLimits,
    ) -> Result<Self, QueueError> {
        if scoped.result_retention_ms != total.result_retention_ms {
            return Err(QueueError::Invalid);
        }
        let legacy = QueueLimits {
            max_pending_requests: total
                .max_pending_requests
                .checked_sub(scoped.max_pending_requests)
                .ok_or(QueueError::Invalid)?,
            max_queue_bytes_per_replica: total
                .max_queue_bytes_per_replica
                .checked_sub(scoped.max_queue_bytes_per_replica)
                .ok_or(QueueError::Invalid)?,
            max_retained_result_bytes_per_worker: total
                .max_retained_result_bytes_per_worker
                .checked_sub(scoped.max_retained_result_bytes_per_worker)
                .ok_or(QueueError::Invalid)?,
            result_retention_ms: total.result_retention_ms,
        };
        let mut replica = Self::new(
            shared.clone(),
            scope.clone(),
            local,
            authority,
            members,
            legacy,
        )?;
        replica.scoped = Some(Box::new(scoped_store::ScopedQueueStore::new(
            shared, scope, local, scoped,
        )?));
        Ok(replica)
    }

    pub(crate) fn scoped_queues(&mut self) -> Result<&mut scoped_store::ScopedQueueStore, QueueError> {
        self.scoped.as_deref_mut().ok_or(QueueError::Unavailable)
    }

    pub(crate) fn growth_queue_observation(&self) -> Option<(usize, Option<(Uuid, u64)>)> {
        let mut queued = 0;
        let mut oldest: Option<&RequestRecord> = None;
        for (index, record) in self.queue.records().enumerate() {
            if index == 4096 {
                return None;
            }
            if record.accepted && record.state == RequestState::Queued {
                queued += 1;
                if oldest.is_none_or(|previous| record.sequence < previous.sequence) {
                    oldest = Some(record);
                }
            }
        }
        Some((queued, oldest.map(|record| (record.request_id, record.version))))
    }
    pub(crate) fn stats(&self) -> QueueStats {
        let (charged_queue_bytes, charged_result_bytes) = self.queue.charged_bytes();
        let mut stats = QueueStats {
            accepted_records: 0,
            queued_records: 0,
            running_records: 0,
            reconciling_records: 0,
            terminal_records: 0,
            charged_queue_bytes,
            charged_result_bytes,
        };
        for record in self.queue.records() {
            stats.accepted_records += u64::from(record.accepted);
            match record.state {
                RequestState::Queued => stats.queued_records += 1,
                RequestState::Running | RequestState::Committing => stats.running_records += 1,
                RequestState::Reconciling => stats.reconciling_records += 1,
                _ => stats.terminal_records += 1,
            }
        }
        stats
    }
    pub(crate) fn queue_latency_stats(&self) -> QueueLatencySnapshot {
        self.queue_latency
            .snapshot(self.local, self.authority.owner, self.authority.ready.view)
    }
    pub fn new(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        authority: QueueAuthority,
        members: impl IntoIterator<Item = WorkerIdentity>,
        limits: QueueLimits,
    ) -> Result<Self, QueueError> {
        Self::construct(shared, scope, local, authority, members, limits, None)
    }
    /// Initial/cold bootstrap owns a real empty physical RAM queue, but grants
    /// no admission until Control's TSO-fenced ready transition is published.
    pub(crate) fn new_empty_bootstrap(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        ticket: JoinTicket,
        limits: QueueLimits,
    ) -> Result<Self, QueueError> {
        if ticket.ticket_id.is_nil()
            || ticket.target != local
            || ticket.source_ready.ready_count != 0
            || ticket.source_ready.ready_root != ReadySetHasher::default().finish()
        {
            return Err(QueueError::Invalid);
        }
        let mut hash = ReadySetHasher::default();
        hash.push(local).map_err(invalid)?;
        let mut view = ticket.source_ready.view;
        view.membership_revision = view
            .membership_revision
            .checked_add(1)
            .ok_or(QueueError::Invalid)?;
        let authority = QueueAuthority {
            ready: ReadyMembership {
                view,
                ready_root: hash.finish(),
                ready_count: 1,
            },
            owner: local,
        };
        let mut replica = Self::construct(shared, scope, local, authority, [local], limits, None)?;
        replica.bootstrap = Some(ticket);
        Ok(replica)
    }
    pub(crate) fn empty_bootstrap_statement(
        &self,
        ticket: JoinTicket,
    ) -> Result<QueueCopyStatement, QueueError> {
        if self.bootstrap != Some(ticket)
            || self.queue.records().next().is_some()
            || self.change_cut != 0
        {
            return Err(QueueError::Conflict);
        }
        Ok(QueueCopyStatement {
            ticket,
            source: self.local,
            snapshot_id: Uuid::new_v4(),
            snapshot_cut: 0,
            final_cut: 0,
            record_version_hash: hash(b"nabedra/ram-queue-cut/v1\0"),
        })
    }
    pub(crate) fn activate_empty_bootstrap(
        &mut self,
        current: QueueAuthority,
    ) -> Result<(), QueueError> {
        let ticket = self.bootstrap.ok_or(QueueError::Conflict)?;
        self.empty_bootstrap_statement(ticket)?;
        if current != self.authority {
            return Err(QueueError::Stale);
        }
        self.bootstrap = None;
        Ok(())
    }
    /// A joining process starts unready. Only the revision-bound ticket grants
    /// snapshot import; it grants no admission, execution or ready membership.
    pub fn new_joining(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        authority: QueueAuthority,
        members: impl IntoIterator<Item = WorkerIdentity>,
        limits: QueueLimits,
        ticket: JoinTicket,
    ) -> Result<Self, QueueError> {
        if ticket.ticket_id.is_nil()
            || ticket.target != local
            || ticket.source_ready != authority.ready
        {
            return Err(QueueError::Invalid);
        }
        Self::construct(
            shared,
            scope,
            local,
            authority,
            members,
            limits,
            Some(ticket),
        )
    }
    fn construct(
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        local: WorkerIdentity,
        authority: QueueAuthority,
        members: impl IntoIterator<Item = WorkerIdentity>,
        limits: QueueLimits,
        joining: Option<JoinTicket>,
    ) -> Result<Self, QueueError> {
        scope.validate()?;
        local.validate().map_err(invalid)?;
        shared
            .bind_worker(local.worker_id)
            .map_err(|_| QueueError::Invalid)?;
        authority.ready.view.validate().map_err(invalid)?;
        authority.owner.validate().map_err(invalid)?;
        let mut ordered = BTreeMap::new();
        let mut hasher = ReadySetHasher::default();
        for member in members {
            hasher.push(member).map_err(invalid)?;
            ordered.insert(member.worker_id, member);
        }
        if hasher.count() != authority.ready.ready_count
            || hasher.finish() != authority.ready.ready_root
            || joining.is_none() && ordered.get(&local.worker_id) != Some(&local)
            || joining.is_some() && ordered.contains_key(&local.worker_id)
            || ordered.get(&authority.owner.worker_id) != Some(&authority.owner)
        {
            return Err(QueueError::Invalid);
        }
        let bytes = limits
            .max_queue_bytes_per_replica
            .checked_add(limits.max_retained_result_bytes_per_worker)
            .and_then(|n| n.checked_add(8 << 10))
            .ok_or(QueueError::Overloaded)?;
        let memory = shared
            .scheduler()
            .charge_memory(shared.scheduler_node_id(), bytes as u64)
            .map_err(|_| QueueError::Overloaded)?;
        let membership_memory = shared
            .scheduler()
            .charge_memory(
                shared.scheduler_node_id(),
                ordered
                    .len()
                    .checked_mul(256)
                    .ok_or(QueueError::Overloaded)? as u64,
            )
            .map_err(|_| QueueError::Overloaded)?;
        let queue = RequestQueue::new_with_metadata_overhead(
            authority.ready.view.run_id,
            authority.ready.view.owner_term,
            authority.ready.view.membership_revision,
            limits,
            ordered.keys().copied().collect(),
            PROOF_METADATA_BYTES,
        )?;
        let queue_owner = shared.claim_queue().map_err(|_| QueueError::Overloaded)?;
        Ok(Self {
            shared,
            scope,
            local,
            authority,
            members: Arc::new(ordered),
            queue,
            scoped: None,
            barriers: BTreeMap::new(),
            queue_latency: QueueLatency::default(),
            terminal_metadata: BTreeMap::new(),
            read_grants: BTreeMap::new(),
            retired_through_sequence: 0,
            retirement_generation: 0,
            change_cut: 0,
            changes: BTreeMap::new(),
            export: None,
            import: joining.map(transfer::JoinImport::new),
            bootstrap: None,
            removal: None,
            removal_root: None,
            removal_worker_after: None,
            removal_record_after: None,
            _memory: memory,
            _membership_memory: membership_memory,
            _queue_owner: queue_owner,
            retention_ms: limits.result_retention_ms,
        })
    }
    pub fn authority(&self) -> QueueAuthority {
        self.authority
    }
    pub fn record(&self, id: Uuid) -> Option<&RequestRecord> {
        self.queue.record(id)
    }
    pub(crate) fn observation(&self, id: Uuid) -> Result<RequestRecord, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        Ok(RequestRecord {
            context: None,
            run_id: record.run_id,
            scheduler_term: record.scheduler_term,
            membership_revision: record.membership_revision,
            request_id: record.request_id,
            digest: record.digest.clone(),
            sequence: record.sequence,
            version: record.version,
            body: Vec::new(),
            accepted: record.accepted,
            state: record.state,
            pinned_catalog: None,
            response_status: record.response_status,
            response_body: None,
            completed_at_ms: record.completed_at_ms,
            result_expired: record.result_expired,
            replicas: Default::default(),
        })
    }
    pub(crate) fn read_member(&self, authority: Authority, worker: Uuid) -> Option<WorkerIdentity> {
        (self.authority.ready.view.authority() == authority)
            .then(|| self.members.get(&worker).copied())
            .flatten()
    }
    pub(crate) fn scan_context(
        &self,
        scope: crate::oltp_replication::service_rpc::ReadScope,
        owner: WorkerIdentity,
        target: WorkerIdentity,
    ) -> Result<ChargedBytes, QueueError> {
        if self.authority.owner != owner
            || self.local != target
            || self.read_member(scope.authority, target.worker_id) != Some(target)
            || scope.request.run_id != scope.authority.run_id
        {
            return Err(QueueError::Invalid);
        }
        let record = self
            .queue
            .record(scope.request.request_id)
            .ok_or(QueueError::NotFound)?;
        if !record.accepted
            || record.state != RequestState::Running
            || record.run_id != scope.request.run_id
            || Digest::parse(&record.digest).map_err(invalid)? != scope.request.digest
        {
            return Err(QueueError::Invalid);
        }
        self.shared
            .copy_bytes_in(
                ByteClass::Progress,
                record
                    .pinned_catalog
                    .as_deref()
                    .ok_or(QueueError::Invalid)?,
            )
            .map_err(|_| QueueError::Overloaded)
    }
    pub(crate) fn input(
        &self,
        id: Uuid,
    ) -> Result<(ChargedBytes, ChargedBytes, RequestRecord), QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        let body = self
            .shared
            .copy_bytes(&record.body)
            .map_err(|_| QueueError::Overloaded)?;
        let context = self
            .shared
            .copy_bytes(
                record
                    .pinned_catalog
                    .as_deref()
                    .ok_or(QueueError::Invalid)?,
            )
            .map_err(|_| QueueError::Overloaded)?;
        Ok((body, context, self.observation(id)?))
    }
    pub(crate) fn staged_context(&self, id: Uuid) -> Result<Option<ChargedBytes>, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        if record.accepted {
            return Ok(None);
        }
        self.shared
            .copy_bytes(record.pinned_catalog.as_deref().ok_or(QueueError::Invalid)?)
            .map(Some)
            .map_err(|_| QueueError::Overloaded)
    }
    pub(crate) fn response(
        &self,
        run: Uuid,
        id: Uuid,
        now: u64,
    ) -> Result<(RequestRecord, Option<ChargedBytes>), QueueError> {
        if run != self.queue.run_id() {
            return Err(QueueError::Abandoned);
        }
        let record = self.retained_record(id, now)?;
        let bytes = self.copy_response_bytes(record, usize::MAX, ByteClass::Data)?;
        Ok((self.observation(id)?, bytes))
    }
    pub(crate) fn copy_response_bytes(
        &self,
        record: &RequestRecord,
        limit: usize,
        class: ByteClass,
    ) -> Result<Option<ChargedBytes>, QueueError> {
        record
            .response_body
            .as_ref()
            .filter(|body| body.len() <= limit)
            .map(|body| {
                self.shared
                    .copy_bytes_in(class, body)
                    .map_err(|_| QueueError::Overloaded)
            })
            .transpose()
    }
    pub(crate) fn reconciling_ids(&self, maximum: usize) -> Vec<Uuid> {
        self.queue
            .records()
            .filter(|record| record.state == RequestState::Reconciling)
            .take(maximum.min(128))
            .map(|record| record.request_id)
            .collect()
    }
    fn needs_recovery(&self, record: &RequestRecord, now: u64) -> bool {
        self.has_read_grants(record.request_id)
            || !record.state.terminal()
            || (!self.terminal_expired(record, now)
                && self.terminal_copy_confirmation(self.authority, record).is_none())
    }
    pub(crate) fn has_recovery_work(&self, now: u64, active: &BTreeSet<Uuid>) -> bool {
        self.queue
            .records()
            .any(|record| self.needs_recovery(record, now) && !active.contains(&record.request_id))
    }
    fn terminal_expired(&self, record: &RequestRecord, now: u64) -> bool {
        record.accepted && record.state.terminal() && !self.has_read_grants(record.request_id)
            && record
                .completed_at_ms
                .is_some_and(|done| now.saturating_sub(done) >= self.retention_ms)
    }
    pub(crate) fn has_retirement_work(&self, now: u64, active: &BTreeSet<Uuid>) -> bool {
        self.queue.records().any(|record| {
            !active.contains(&record.request_id)
                && (self.terminal_expired(record, now)
                    || (self.local == self.authority.owner
                        && record.state == RequestState::Reconciling))
        })
    }
    /// Bounded cursor page so a blocked old request cannot starve another
    /// retained record. Tentative copies must finish admission before execution.
    pub(crate) fn recovery_ids(&self, now: u64, after: Option<Uuid>, maximum: usize) -> Vec<Uuid> {
        self.queue
            .records()
            .filter(|record| {
                after.is_none_or(|after| record.request_id > after) && self.needs_recovery(record, now)
            })
            .take(maximum.min(128))
            .map(|record| record.request_id)
            .collect()
    }
    pub(crate) fn expired_terminals(
        &self,
        now: u64,
        after: Option<u64>,
        maximum: usize,
    ) -> Vec<(Uuid, u64)> {
        let maximum = maximum.min(128);
        let mut page = BTreeMap::new();
        if maximum == 0 {
            return Vec::new();
        }
        for record in self.queue.records().filter(|record| {
            after.is_none_or(|after| record.sequence > after) && self.terminal_expired(record, now)
        }) {
            if page.len() < maximum
                || page
                    .last_key_value()
                    .is_some_and(|(last, _)| record.sequence < *last)
            {
                page.insert(record.sequence, record.request_id);
                if page.len() > maximum {
                    page.pop_last();
                }
            }
        }
        page.into_iter()
            .map(|(sequence, id)| (id, sequence))
            .collect()
    }
    pub(crate) fn retire(
        &mut self,
        current: QueueAuthority,
        proof: crate::oltp_replication::authority::status::VerifiedTerminalRetirement,
        now: u64,
    ) -> Result<(), QueueError> {
        if self.authority != current || !self.is_ready_member() {
            return Err(QueueError::Stale);
        }
        if self.export.is_some() {
            return Err(QueueError::Unavailable);
        }
        let id = proof.request_id();
        let Some(record) = self.queue.record(id) else {
            return Ok(());
        };
        if !self.terminal_expired(record, now) || !proof.matches(&self.terminal_copy_statement(id)?) {
            return Err(QueueError::Stale);
        }
        let sequence = record.sequence;
        let generation = self.retirement_generation.checked_add(1).ok_or(QueueError::Overloaded)?;
        self.queue.retire_terminal(id, record.version)?;
        self.retired_through_sequence = self.retired_through_sequence.max(sequence);
        self.retirement_generation = generation;
        self.terminal_metadata.remove(&id);
        self.read_grants.remove(&id);
        if let Some(mut barrier) = self.barriers.remove(&id) {
            self.queue_latency.discard(&mut barrier.acceptance_timing);
        }
        self.changes.remove(&id);
        // A frozen removal can retry against a smaller certified inventory.
        // Keep the admission freeze until the ready transition finishes.
        self.removal_root = None;
        self.removal_worker_after = None;
        self.removal_record_after = None;
        Ok(())
    }
    pub(crate) fn can_certify_retirement(&self, current: QueueAuthority) -> bool {
        self.export.is_none() && self.mutating_owner(current).is_ok()
    }
    pub(crate) fn install_retirement_check(&self, header: &RecordHeader) -> Option<u64> {
        self.missing_record_retirement_generation(header.request_id, header.sequence)
    }
    fn missing_record_retirement_generation(&self, id: Uuid, sequence: u64) -> Option<u64> {
        // A delayed retransmission keeps its original sequence. New sequences
        // above the highest local deletion need no Status lookup.
        (sequence <= self.retired_through_sequence && self.queue.record(id).is_none())
            .then_some(self.retirement_generation)
    }
    pub(crate) fn reconcile(
        &mut self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<(), QueueError> {
        self.mutating_owner(current)?;
        if self
            .queue
            .record(id)
            .is_some_and(|record| record.state.terminal())
        {
            return Ok(());
        }
        self.queue.transition(id, RequestState::Reconciling)?;
        self.changed(id)
    }
    pub fn pending(&self) -> usize {
        self.queue.pending()
    }
    pub fn local_identity(&self) -> WorkerIdentity {
        self.local
    }
    pub fn is_ready_member(&self) -> bool {
        self.bootstrap.is_none()
            && self.import.is_none()
            && self.members.get(&self.local.worker_id) == Some(&self.local)
    }
    pub fn scope(&self) -> &QueueScope {
        &self.scope
    }
    pub fn next_copy_member(&self, id: Uuid) -> Result<WorkerIdentity, QueueError> {
        let barrier = self.barriers.get(&id).ok_or(QueueError::NotFound)?;
        if barrier.complete {
            return Err(QueueError::Conflict);
        }
        self.members
            .values()
            .nth(barrier.ready.count() as usize)
            .copied()
            .ok_or(QueueError::Stale)
    }
    pub(crate) fn next_copy_batch(
        &self,
        id: Uuid,
        limit: usize,
    ) -> Result<Vec<WorkerIdentity>, QueueError> {
        let first = self.next_copy_member(id)?;
        Ok(self
            .members
            .range(first.worker_id..)
            .take(limit)
            .map(|(_, worker)| *worker)
            .collect())
    }
    pub fn retained_record(&self, id: Uuid, now_ms: u64) -> Result<&RequestRecord, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        if !record.accepted {
            return Err(QueueError::Unavailable);
        }
        if record.result_expired
            || record
                .completed_at_ms
                .is_some_and(|t| now_ms.saturating_sub(t) >= self.retention_ms)
        {
            return Err(QueueError::NotFound);
        }
        Ok(record)
    }

    fn check_owner(
        &self,
        current: QueueAuthority,
        sender: WorkerIdentity,
    ) -> Result<(), QueueError> {
        if current != self.authority || sender != current.owner {
            return Err(QueueError::Stale);
        }
        Ok(())
    }
    fn mutating_owner(&self, current: QueueAuthority) -> Result<(), QueueError> {
        self.check_owner(current, self.local)?;
        if self.bootstrap.is_some()
            || self.removal.is_some()
            || self.export.as_ref().is_some_and(|e| e.final_cut.is_some())
            || self.import.is_some()
        {
            return Err(QueueError::Unavailable);
        }
        Ok(())
    }
    fn changed(&mut self, id: Uuid) -> Result<(), QueueError> {
        self.change_cut = self
            .change_cut
            .checked_add(1)
            .ok_or(QueueError::Overloaded)?;
        self.changes.insert(id, self.change_cut);
        Ok(())
    }

    pub fn propose(
        &mut self,
        current: QueueAuthority,
        body: Vec<u8>,
        context: AcceptedContext,
        now_ms: u64,
    ) -> Result<RecordFrame, QueueError> {
        self.mutating_owner(current)?;
        let request = protocol::parse_request(&body).map_err(invalid)?;
        if context.authorization.database != self.scope.database
            || context.authorization.policy_revision == 0
            || context.authorization.ingress_id.is_nil()
            || context.authorization.ingress_incarnation.is_nil()
            || context
                .authorization
                .credential_expires_at_ms
                .is_some_and(|t| t <= now_ms)
        {
            return Err(QueueError::Invalid);
        }
        if let Some(old) = self.queue.record(request.request_id) {
            // Authorized retries attach to the original accepted context.
            if old.digest != hash(&body).to_string() || old.body != body {
                return Err(QueueError::Conflict);
            }
            return self.begin_barrier(request.request_id);
        }
        let context_bytes = self
            .shared
            .encode(&context, MAX_CONTEXT_BYTES)
            .map_err(|_| QueueError::Overloaded)?;
        context.validate_binding(
            &self.scope,
            self.queue.run_id(),
            request.request_id,
            hash(&body),
            request.operation.is_mutation(),
        )?;
        drop(request);
        // Shared queue chooses the global sequence; proposal is unaccepted.
        let record = self
            .queue
            .propose_pinned(body, context_bytes.as_ref().to_vec(), 0)?;
        self.changed(record.request_id)?;
        self.begin_barrier(record.request_id)
    }

    /// Both payload and acceptance-evidence phases call this same bounded
    /// installation path. Only the currently enrolled owner may send a record.
    pub fn install(
        &mut self,
        current: QueueAuthority,
        authenticated_sender: WorkerIdentity,
        frame: &RecordFrame,
    ) -> Result<CopyAcknowledgement, QueueError> {
        let environment = self.install_environment(current, authenticated_sender)?;
        self.install_prepared(PreparedInstall::prepare(environment, frame)?)
    }
    pub fn install_environment(
        &self,
        current: QueueAuthority,
        authenticated_sender: WorkerIdentity,
    ) -> Result<InstallEnvironment, QueueError> {
        self.check_owner(current, authenticated_sender)?;
        if self.bootstrap.is_some() {
            return Err(QueueError::Unavailable);
        }
        if self.export.as_ref().is_some_and(|e| e.final_cut.is_some()) {
            return Err(QueueError::Unavailable);
        }
        Ok(InstallEnvironment {
            scope: self.scope.clone(),
            authority: current,
            members: self.members.clone(),
        })
    }
    pub fn install_prepared(
        &mut self,
        prepared: PreparedInstall,
    ) -> Result<CopyAcknowledgement, QueueError> {
        self.install_environment(prepared.authority, prepared.authority.owner)?;
        let incoming = prepared.record.record();
        self.check_read_grant_revision(incoming.request_id, &prepared.read_grants)?;
        let changed = self
            .queue
            .record(incoming.request_id)
            .is_none_or(|old| old.version != incoming.version);
        self.queue.install_validated(prepared.record)?;
        let grant_changed = self.record_read_grants(prepared.version.request_id, prepared.read_grants);
        if let Some(metadata) = prepared.terminal {
            self.terminal_metadata
                .insert(prepared.version.request_id, metadata);
        }
        if changed || grant_changed {
            self.changed(prepared.version.request_id)?;
        }
        Ok(CopyAcknowledgement::authenticated(
            self.local,
            prepared.version,
        ))
    }
    pub(crate) fn install_prepared_checked(
        &mut self,
        prepared: PreparedInstall,
        observed: Option<u64>,
    ) -> Result<CopyAcknowledgement, QueueError> {
        let record = prepared.record.record();
        if self
            .missing_record_retirement_generation(record.request_id, record.sequence)
            .is_some_and(|generation| observed != Some(generation))
        {
            return Err(QueueError::Stale);
        }
        self.install_prepared(prepared)
    }

    fn begin_barrier(&mut self, id: Uuid) -> Result<RecordFrame, QueueError> {
        let frame = self.frame(id)?;
        let version = frame.version(&frame.decode_header()?)?;
        self.barriers
            .entry(id)
            .and_modify(|barrier| {
                if barrier.version != version {
                    barrier.version = version;
                    barrier.ready = ReadySetHasher::default();
                    barrier.complete = false;
                    barrier.terminal_confirmed = false;
                }
            })
            .or_insert_with(|| CopyBarrier {
                version,
                ready: ReadySetHasher::default(),
                complete: false,
                terminal_confirmed: false,
                acceptance_timing: AcceptanceTiming::Untracked,
            });
        Ok(frame)
    }

    /// ACKs are stream-merged in authoritative worker-ID order. The barrier is
    /// fixed-size even for ready sets larger than a wire page. No compute grant
    /// is held while the coordinator waits for these network observations.
    pub fn acknowledge(
        &mut self,
        current: QueueAuthority,
        ack: CopyAcknowledgement,
    ) -> Result<Option<RecordFrame>, QueueError> {
        self.acknowledge_phase(current, ack, true)
    }
    pub(crate) fn acknowledge_phase(
        &mut self,
        current: QueueAuthority,
        ack: CopyAcknowledgement,
        promote: bool,
    ) -> Result<Option<RecordFrame>, QueueError> {
        self.mutating_owner(current)?;
        if self.members.get(&ack.worker.worker_id) != Some(&ack.worker) {
            return Err(QueueError::Stale);
        }
        let barrier = self
            .barriers
            .get_mut(&ack.record.request_id)
            .ok_or(QueueError::NotFound)?;
        if barrier.version != ack.record || barrier.complete {
            return Err(QueueError::Stale);
        }
        let required = self
            .members
            .values()
            .nth(barrier.ready.count() as usize)
            .ok_or(QueueError::Stale)?;
        if required != &ack.worker {
            return Err(QueueError::Stale);
        }
        let record = self.queue.acknowledge_copy(
            ack.record.request_id,
            ack.record.version,
            ack.worker.worker_id,
            promote,
        )?;
        barrier.ready.push(ack.worker).map_err(invalid)?;
        if barrier.ready.count() != current.ready.ready_count {
            return Ok(None);
        }
        if barrier.ready.finish() != current.ready.ready_root {
            return Err(QueueError::Stale);
        }
        barrier.complete = true;
        if record.version != ack.record.version {
            barrier.acceptance_timing = AcceptanceTiming::Copying;
            self.changed(ack.record.request_id)?;
            // The final payload ACK creates the acceptance-evidence version.
            return self.begin_barrier(ack.record.request_id).map(Some);
        }
        if record.accepted
            && record.state == RequestState::Queued
            && matches!(barrier.acceptance_timing, AcceptanceTiming::Copying)
        {
            self.queue_latency
                .accepted(&mut barrier.acceptance_timing, Instant::now());
        }
        Ok(None)
    }

    pub(crate) fn payloads_complete(&self, id: Uuid) -> bool {
        self.queue.record(id).is_some_and(|record| {
            self.barriers.get(&id).is_some_and(|barrier| {
                barrier.complete && barrier.version.version == record.version
            })
        })
    }
    pub(crate) fn accept_staged(
        &mut self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<RecordFrame, QueueError> {
        self.mutating_owner(current)?;
        if !self.payloads_complete(id) {
            return Err(QueueError::Unavailable);
        }
        let old = self.queue.record(id).ok_or(QueueError::NotFound)?.version;
        self.queue.accept_payload(id)?;
        if self.queue.record(id).ok_or(QueueError::NotFound)?.version != old {
            if let Some(barrier) = self.barriers.get_mut(&id) {
                barrier.acceptance_timing = AcceptanceTiming::Copying;
            }
            self.changed(id)?;
        }
        self.begin_barrier(id)
    }
    pub fn copies_complete(&self, id: Uuid) -> bool {
        self.queue.record(id).is_some_and(|record| {
            record.accepted
                && self.barriers.get(&id).is_some_and(|barrier| {
                    barrier.complete && barrier.version.version == record.version
                })
        })
    }

    /// Record the coordinator's final Control check on the exact copied version.
    pub(super) fn confirm_terminal_copy(
        &mut self,
        current: QueueAuthority,
        copied: RecordVersion,
    ) -> Result<(), QueueError> {
        self.check_owner(current, self.local)?;
        let record = self
            .queue
            .record(copied.request_id)
            .ok_or(QueueError::NotFound)?;
        if !record.accepted || !record.state.terminal() || record.version != copied.version {
            return Err(QueueError::Stale);
        }
        let barrier = self
            .barriers
            .get_mut(&copied.request_id)
            .ok_or(QueueError::Stale)?;
        if !barrier.complete || barrier.version != copied {
            return Err(QueueError::Stale);
        }
        barrier.terminal_confirmed = true;
        Ok(())
    }

    pub(super) fn terminal_copy_confirmation(
        &self,
        current: QueueAuthority,
        record: &RequestRecord,
    ) -> Option<RecordVersion> {
        if current != self.authority
            || current.owner != self.local
            || !record.accepted
            || !record.state.terminal()
        {
            return None;
        }
        let barrier = self.barriers.get(&record.request_id)?;
        (barrier.complete && barrier.terminal_confirmed && barrier.version.version == record.version)
            .then_some(barrier.version)
    }

    pub fn start(&mut self, current: QueueAuthority, id: Uuid) -> Result<RecordFrame, QueueError> {
        self.mutating_owner(current)?;
        if self.has_read_grants(id) {
            return Err(QueueError::Unavailable);
        }
        if self
            .queue
            .record(id)
            .is_some_and(|r| r.state == RequestState::Running)
        {
            return self.begin_barrier(id);
        }
        if !self.copies_complete(id) {
            return Err(QueueError::Unavailable);
        }
        self.queue.start(id, None)?;
        self.changed(id)?;
        self.begin_barrier(id)
    }

    pub(crate) fn observe_dispatch(&mut self, current: QueueAuthority, version: RecordVersion) {
        if self.authority != current || current.owner != self.local {
            return;
        }
        if let Some(barrier) = self.barriers.get_mut(&version.request_id)
            && barrier.complete
            && barrier.version == version
            && self.queue.record(version.request_id).is_some_and(|record| {
                record.version == version.version && record.state == RequestState::Running
            })
        {
            self.queue_latency
                .dispatch(&mut barrier.acceptance_timing, Instant::now());
        }
    }

    pub fn terminal_input(&self, id: Uuid) -> Result<TerminalInput, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        Ok(TerminalInput {
            version: record.version,
            request: RequestBinding {
                run_id: record.run_id,
                request_id: id,
                digest: Digest::parse(&record.digest).map_err(invalid)?,
                accepted_context: Digest::from_bytes([0; 32]),
            },
            context: record
                .pinned_catalog
                .as_ref()
                .ok_or(QueueError::Invalid)?
                .clone(),
        })
    }
    pub fn shared_resources(&self) -> Arc<WorkerSharedResources> {
        self.shared.clone()
    }

    /// Receipt races reuse an immutable terminal version. Hashing and decoding
    /// happened outside this mutex; settlement checks its exact captured input.
    pub fn finish_prepared(
        &mut self,
        current: QueueAuthority,
        prepared: PreparedTerminal,
    ) -> Result<RecordFrame, QueueError> {
        self.mutating_owner(current)?;
        let id = prepared.input.request.request_id;
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        if record.run_id != prepared.input.request.run_id
            || record.digest != prepared.input.request.digest.to_string()
            || record.pinned_catalog.as_ref() != Some(&prepared.input.context)
        {
            return Err(QueueError::Stale);
        }
        if record.state.terminal() {
            let cache = self.terminal_metadata.get(&id).ok_or(QueueError::Invalid)?;
            if cache.version != record.version
                || cache.request != prepared.metadata.request
                || cache.response_hash != prepared.metadata.response_hash
                || record.response_status != Some(prepared.response.status())
                || record.response_body.as_ref().map(Vec::len)
                    != Some(prepared.response.body().len())
            {
                return Err(QueueError::Conflict);
            }
            return self.begin_barrier(id);
        }
        if record.version != prepared.input.version {
            return Err(QueueError::Stale);
        }
        self.queue
            .finish_retained_validated(id, prepared.response, prepared.now_ms)?;
        if let Some(barrier) = self.barriers.get_mut(&id) {
            self.queue_latency.discard(&mut barrier.acceptance_timing);
        }
        let mut metadata = prepared.metadata;
        metadata.version = self.queue.record(id).ok_or(QueueError::NotFound)?.version;
        self.terminal_metadata.insert(id, metadata);
        self.changed(id)?;
        self.begin_barrier(id)
    }
    #[cfg(test)]
    pub fn finish(
        &mut self,
        current: QueueAuthority,
        id: Uuid,
        status: u16,
        body: Vec<u8>,
        now_ms: u64,
    ) -> Result<RecordFrame, QueueError> {
        let prepared = PreparedTerminal::prepare(self.terminal_input(id)?, status, body, now_ms)?;
        self.finish_prepared(current, prepared)
    }
    /// Protected proof reads perform fixed-size copies only. Terminal bytes
    /// and the original storage pin stay retained until durable TSO release.
    pub fn terminal_statement(&self, id: Uuid) -> Result<TerminalCopyStatement, QueueError> {
        if !self.copies_complete(id) {
            return Err(QueueError::Unavailable);
        }
        self.terminal_copy_statement(id)
    }
    /// Describes only this holder. The TSO leader verifies the complete current
    /// ready inventory itself; followers need no lost owner's private ACK set.
    pub fn terminal_copy_statement(&self, id: Uuid) -> Result<TerminalCopyStatement, QueueError> {
        if self.has_read_grants(id) {
            return Err(QueueError::Unavailable);
        }
        let (record, metadata) = self.terminal_record_metadata(id)?;
        let (pin_identity, storage_pin) = metadata.pin.clone().ok_or(QueueError::Invalid)?;
        let mut digest = Sha256::new();
        digest.update(b"nabedra/terminal-record/v1");
        digest.update(metadata.response_hash.as_bytes());
        digest.update(record.response_status.ok_or(QueueError::Invalid)?.to_be_bytes());
        digest.update(record.completed_at_ms.ok_or(QueueError::Invalid)?.to_be_bytes());
        digest.update([record.state as u8]);
        Ok(TerminalCopyStatement {
            ready: self.authority.ready,
            request: metadata.request,
            terminal_version: record.version,
            response_hash: metadata.response_hash,
            terminal_digest: Some(Digest::from_bytes(digest.finalize().into())),
            pin_identity,
            storage_pin,
        })
    }

    pub(super) fn terminal_response_hash(&self, id: Uuid) -> Result<Digest, QueueError> {
        Ok(self.terminal_record_metadata(id)?.1.response_hash)
    }

    fn terminal_record_metadata(
        &self,
        id: Uuid,
    ) -> Result<(&RequestRecord, &TerminalMetadata), QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        if !record.accepted || !record.state.terminal() {
            return Err(QueueError::Conflict);
        }
        let metadata = self.terminal_metadata.get(&id).ok_or(QueueError::Invalid)?;
        if metadata.version != record.version
            || metadata.request.request_id != id
            || metadata.request.run_id != record.run_id
        {
            return Err(QueueError::Stale);
        }
        Ok((record, metadata))
    }

    /// Called only after Control and dependent groups commit their fences.
    /// Surviving complete records keep the run, input, authorization and read
    /// token. A new process with empty RAM cannot use this to recreate them.
    pub fn fenced_takeover(
        &mut self,
        previous: QueueAuthority,
        next: QueueAuthority,
        members: impl IntoIterator<Item = WorkerIdentity>,
    ) -> Result<(), QueueError> {
        if previous != self.authority || next.ready.view.run_id != previous.ready.view.run_id {
            return Err(QueueError::Stale);
        }
        if self.import.is_some() {
            self.join_statement(previous)?;
        }
        next.ready.view.validate().map_err(invalid)?;
        let mut ordered = BTreeMap::new();
        let mut hasher = ReadySetHasher::default();
        for member in members {
            hasher.push(member).map_err(invalid)?;
            ordered.insert(member.worker_id, member);
        }
        if hasher.finish() != next.ready.ready_root
            || hasher.count() != next.ready.ready_count
            || ordered.get(&self.local.worker_id) != Some(&self.local)
            || ordered.get(&next.owner.worker_id) != Some(&next.owner)
        {
            return Err(QueueError::Invalid);
        }
        let memory = self
            .shared
            .scheduler()
            .charge_memory(
                self.shared.scheduler_node_id(),
                ordered
                    .len()
                    .checked_mul(256)
                    .ok_or(QueueError::Overloaded)? as u64,
            )
            .map_err(|_| QueueError::Overloaded)?;
        self.queue.takeover_in_place(
            next.ready.view.owner_term,
            next.ready.view.membership_revision,
            ordered.keys().copied().collect(),
        )?;
        self.members = Arc::new(ordered);
        self.authority = next;
        for entry in self.read_grants.values_mut() {
            entry.copying = false;
        }
        self.queue_latency.change_authority();
        self.barriers.clear();
        self.export = None;
        self.import = None;
        self.removal = None;
        self.removal_root = None;
        self.removal_worker_after = None;
        self.removal_record_after = None;
        let cut = self
            .change_cut
            .checked_add(1)
            .ok_or(QueueError::Overloaded)?;
        self.change_cut = cut;
        for record in self.queue.records() {
            if let Some(metadata) = self.terminal_metadata.get_mut(&record.request_id) {
                metadata.version = record.version;
            }
            self.changes.insert(record.request_id, cut);
        }
        self._membership_memory = memory;
        Ok(())
    }

    /// Resume copying an existing version after a lost ACK/owner change.
    pub fn resume(&mut self, current: QueueAuthority, id: Uuid) -> Result<RecordFrame, QueueError> {
        self.mutating_owner(current)?;
        self.begin_barrier(id)
    }

    pub fn frame(&self, id: Uuid) -> Result<RecordFrame, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        let context = record.pinned_catalog.as_ref().ok_or(QueueError::Invalid)?;
        let sections = [
            Some(
                self.shared
                    .copy_bytes(&record.body)
                    .map_err(|_| QueueError::Overloaded)?,
            ),
            Some(
                self.shared
                    .copy_bytes(context)
                    .map_err(|_| QueueError::Overloaded)?,
            ),
            record
                .response_body
                .as_ref()
                .map(|b| self.shared.copy_bytes(b))
                .transpose()
                .map_err(|_| QueueError::Overloaded)?,
        ];
        let header = self.record_header(record)?;
        Ok(RecordFrame {
            header: self
                .shared
                .encode(&header, MAX_RECORD_HEADER_BYTES)
                .map_err(|_| QueueError::Overloaded)?,
            sections,
        })
    }
    fn record_header(&self, record: &RequestRecord) -> Result<RecordHeader, QueueError> {
        Ok(RecordHeader {
            queue_format_version: QUEUE_FORMAT_VERSION,
            scope: self.scope.clone(),
            ready: self.authority.ready,
            owner: self.authority.owner,
            request_id: record.request_id,
            digest: Digest::parse(&record.digest).map_err(invalid)?,
            sequence: record.sequence,
            version: record.version,
            accepted: record.accepted,
            state: record.state,
            response_status: record.response_status,
            completed_at_ms: record.completed_at_ms,
            section_lengths: [
                record.body.len() as u64,
                record.pinned_catalog.as_ref().map_or(0, Vec::len) as u64,
                record.response_body.as_ref().map_or(0, Vec::len) as u64,
            ],
            all_copy_evidence: record.accepted && record.replicas == *self.queue.members(),
            read_grants: self.read_grant_set(record.request_id),
        })
    }
}
