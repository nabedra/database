//! Bounded local payload ownership. Keep the injected coordinator pool alive until drain.
//! Disk cells deliberately suppress implicit destruction when lifecycle ordering fails.
use super::{
    frame::MAX_DATA_BYTES,
    job::{
        JobEnvelope, JobResult, MAX_INTERNAL_ROW_BYTES, PreparationValidator, ResultEnvelope,
        encode_job, encode_part_meta, encode_result, parse_job, parse_part_meta, parse_result,
    },
    payload::{PreparedReceiver, PreparedResult, ValidatedPart, decode_rows, encode_rows},
    spool::{Spool, SpoolLimits, SpoolOperation, SpoolReader, SpoolWriter},
};
use crate::{
    protocol::Row,
    worker_pool::{WorkerPool, WorkerRuntime},
};
use std::{
    fmt,
    future::{Future, poll_fn},
    io::Read,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerError {
    Refused,
    Invalid,
    Cancelled,
    FailedDrain,
}
impl fmt::Display for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Refused => "payload admission refused",
            Self::Invalid => "invalid payload command",
            Self::Cancelled => "payload cancelled",
            Self::FailedDrain => "payload drain failed",
        })
    }
}
impl std::error::Error for OwnerError {}
type Result<T> = std::result::Result<T, OwnerError>;
#[derive(Clone, Copy, Debug)]
pub(crate) enum Role {
    Operation,
    Dependency,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Process,
    Control,
    Operation,
    Stream,
}

// No raw anchors occur in commands, replies, guards or leases.
pub(crate) enum Command {
    BeginRequest(Vec<u8>),
    BeginCheckedRequest(CheckedRequestJob),
    BeginCheckedResult {
        result: Box<ResultEnvelope>,
        expected: Box<JobEnvelope>,
    },
    ExecuteCaptured(Arc<crate::storage::Storage<crate::storage::RuntimeStore>>),
    ExecutePrepared(crate::engine::CapturedInput),
    StagePrepared {
        table: uuid::Uuid,
        commit: uuid::Uuid,
        ordinal: u32,
    },
    UploadStaged(Arc<crate::storage::Storage<crate::storage::RuntimeStore>>),
    BeginResult {
        bytes: Vec<u8>,
        expected: Box<JobEnvelope>,
    },
    BeginPart(Vec<u8>),
    ClosePart,
    CompletePrepared,
    BeginRows {
        job: Box<JobEnvelope>,
        result: Box<ResultEnvelope>,
    },
    Append(Vec<u8>),
    CloseRows,
    CommitEof,
    TakeRows,
    ClearReceived,
    ClearStaged,
    StartSend {
        job: Box<JobEnvelope>,
        request: bool,
    },
    NextPart,
    ReadNext,
}
pub(crate) enum Reply {
    Parts(Option<Vec<crate::storage::PartitionFile>>),
    Ready,
    Sending(Vec<u8>, super::payload_stream::ReceiveKind),
    Bytes(Vec<u8>),
    Receiving(super::payload_stream::ReceiveKind),
    RequestAccepted(Box<JobEnvelope>),
    Accepted(Box<ResultEnvelope>),
    Rows(Vec<Row>),
}
enum Initial {
    Process(PathBuf, SpoolLimits),
    Operation(SpoolLimits),
    Stream,
    Control,
}
struct Sending {
    reader: Option<SpoolReader>,
    parts: std::vec::IntoIter<ValidatedPart>,
    job: Box<JobEnvelope>,
}
enum Processor {
    Sending(Box<Sending>),
    Empty,
    PendingRequest(Box<JobEnvelope>),
    AcceptedRequest(Box<JobEnvelope>),
    PendingControl(Box<ResultEnvelope>),
    AcceptedControl(Box<ResultEnvelope>),
    Prepared {
        job: Box<JobEnvelope>,
        receiver: Box<PreparedReceiver>,
    },
    PendingPrepared(PreparedResult),
    AcceptedPrepared(PreparedResult),
    Rows {
        job: Box<JobEnvelope>,
        result: Box<ResultEnvelope>,
        writer: SpoolWriter,
    },
    Pending(Vec<Row>, Box<ResultEnvelope>),
    Accepted(Vec<Row>, Box<ResultEnvelope>),
}
struct Disk {
    spool: Option<Spool>,
    operation: Option<Arc<SpoolOperation>>,
    processor: Processor,
    uploads: Vec<(uuid::Uuid, uuid::Uuid, u32, PreparedResult)>,
}
impl Disk {
    fn empty() -> Self {
        Self {
            spool: None,
            operation: None,
            processor: Processor::Empty,
            uploads: Vec::new(),
        }
    }
}
struct Handoff {
    cancelled: bool,
}
pub(super) struct Slot {
    evidence: Arc<super::admission::WorkEvidence>,
    id: u64,
    operation_id: Mutex<Option<uuid::Uuid>>,
    receiver_limits: std::sync::OnceLock<SpoolLimits>,
    kind: Kind,
    deadline: Option<Instant>,
    disk: Mutex<Option<ManuallyDrop<Disk>>>,
    retained_memory: Mutex<Option<crate::scheduling::MemoryCharge>>,
    handoff: Mutex<Handoff>,
    wake: watch::Sender<bool>,
    children: Mutex<usize>,
    child_done: Notify,
    producing: AtomicBool,
    cleanup: AtomicBool,
    failed: AtomicBool,
    initialized: AtomicBool,
    initialization_done: watch::Sender<Option<bool>>,
    delivery_holds: Mutex<usize>,
    // Only the original preclaimed response guard sets/clears this, under delivery_holds.
    response_live: AtomicBool,
    delivery_done: Notify,
    #[cfg(test)]
    hooks: Mutex<tests::Hooks>,
}
impl Slot {
    fn cancel(&self) {
        let mut handoff = self.handoff.lock().unwrap();
        if !handoff.cancelled {
            let _change = self.evidence.begin(false).expect("cancellation");
            handoff.cancelled = true;
        }
        drop(handoff);
        self.wake.send_replace(true);
    }
    pub(super) fn cancelled(&self) -> bool {
        let mut h = self.handoff.lock().unwrap();
        if !h.cancelled && self.deadline.is_some_and(|d| nabedra_rt::time::now() >= d) {
            let _change = self.evidence.begin(false).expect("deadline cancellation");
            h.cancelled = true;
        }
        h.cancelled
    }
    pub(super) async fn next_frame(
        &self,
        reader: &mut super::payload_stream::Reader,
    ) -> Result<Option<super::frame::Frame>> {
        let mut wake = self.wake.subscribe();
        if self.cancelled() {
            return Err(OwnerError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = wake.changed() => Err(OwnerError::Cancelled),
            _ = async { if let Some(d) = self.deadline { nabedra_rt::time::sleep_until(d).await } else { std::future::pending::<()>().await } } => { self.cancel(); Err(OwnerError::Cancelled) },
            frame = reader.next_frame() => frame.map_err(|_| OwnerError::Invalid),
        }
    }
    pub(super) async fn output<T>(
        &self,
        future: impl Future<Output = std::result::Result<T, super::frame::FrameError>>,
    ) -> Result<T> {
        let mut wake = self.wake.subscribe();
        if self.cancelled() {
            return Err(OwnerError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = wake.changed() => Err(OwnerError::Cancelled),
            _ = async { if let Some(d) = self.deadline { nabedra_rt::time::sleep_until(d).await } else { std::future::pending::<()>().await } } => { self.cancel(); Err(OwnerError::Cancelled) },
            result = future => result.map_err(|_| OwnerError::Invalid),
        }
    }
    // Called only while executing a worker command, including quarantine teardown in tests.
    fn discard(&self) {
        let value = self.disk.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(value.map(ManuallyDrop::into_inner));
        self.retained_memory.lock().unwrap().take();
        #[cfg(test)]
        self.hooks
            .lock()
            .unwrap()
            .disposed
            .push(std::thread::current().id());
    }
}
struct ParentHold(Arc<Slot>);
impl Drop for ParentHold {
    fn drop(&mut self) {
        let _change = self.0.evidence.begin(false).expect("settlement");
        *self.0.children.lock().unwrap() -= 1;
        self.0.child_done.notify_one();
    }
}
enum Admission {
    Legacy {
        permit: Option<OwnedSemaphorePermit>,
        lifetime: Option<Arc<OwnedSemaphorePermit>>,
        parent: Option<ParentHold>,
    },
    Distributed {
        owner: DistributedOwner,
        dependency: Option<super::admission::DependencyUse>,
        transport: Option<Arc<OwnedSemaphorePermit>>,
        parent: Option<ParentHold>,
    },
}
#[derive(Clone, Copy)]
enum DistributedOwner {
    Prepared,
    Boot(uuid::Uuid),
    Work(super::admission::WorkKey),
    Dependency(super::admission::DependencyKey),
    Stream,
    Control,
}
impl Admission {
    fn legacy(
        permit: Option<OwnedSemaphorePermit>,
        lifetime: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Self {
        Self::Legacy {
            permit,
            lifetime,
            parent: None,
        }
    }
    fn distributed(owner: DistributedOwner, transport: Option<Arc<OwnedSemaphorePermit>>) -> Self {
        Self::Distributed {
            owner,
            dependency: None,
            transport,
            parent: None,
        }
    }
    fn set_parent(&mut self, hold: Option<ParentHold>) {
        match self {
            Self::Legacy { parent, .. } | Self::Distributed { parent, .. } => *parent = hold,
        }
    }
    fn owner(&self) -> Option<DistributedOwner> {
        match self {
            Self::Legacy { .. } => None,
            Self::Distributed { owner, .. } => Some(*owner),
        }
    }
    fn set_owner(&mut self, value: DistributedOwner) {
        if let Self::Distributed { owner, .. } = self {
            *owner = value;
        } else {
            unreachable!("distributed registered guards")
        }
    }
}
struct Entry {
    slot: Arc<Slot>,
    handle: JoinHandle<()>,
    guards: ManuallyDrop<Admission>,
    terminated: bool,
    position: Option<usize>,
    record: Option<usize>,
    account_tx: Option<mpsc::Sender<Request>>,
    account_ready: Option<oneshot::Receiver<Result<Reply>>>,
    request: RequestStorage,
}
// Positions remain occupied until the original handle has successfully terminated.
struct Preparation {
    id: u64,
    cancelled: bool,
    guards: Option<ManuallyDrop<Admission>>,
    entry: Option<Entry>,
}
struct Activation {
    parent: Option<Arc<Slot>>,
    initial: Initial,
    production: Option<LocalProduction>,
}
// Cancellation/activation ticket only. The registry owns every handle and guard.
pub(super) struct PreparedEntry {
    _shared: Arc<Shared>,
    slot: Arc<Slot>,
    activate: Option<oneshot::Sender<Activation>>,
    tx: mpsc::Sender<Request>,
    initial: Option<oneshot::Receiver<Result<Reply>>>,
}
impl Drop for PreparedEntry {
    fn drop(&mut self) {
        if self.activate.is_some() {
            self.slot.cancel();
        }
    }
}
pub(super) struct PayloadRegistry {
    proof: super::drain_observation::ProofSlot,
    evidence: Arc<super::admission::WorkEvidence>,
    closed: bool,
    failed: bool,
    process_used: bool,
    next: u64,
    entries: Vec<Entry>,
    accepted_limit: usize,
    operation_bound: usize,
    operation_stream_bound: usize,
    dependency_stream_bound: usize,
    outgoing_bound: usize,
    positions: Vec<Option<u64>>,
    records: Vec<Option<AcceptedOwner>>,
    controls: Vec<Entry>,
    boot_reserved: Option<u64>,
    pending_releases: usize,
    rollback_notifications: Vec<Arc<Slot>>,
    preparations: [Option<Preparation>; 32],
}
/// Counts include failed, cleanup-complete and release-in-progress owners.
/// This is retention evidence, never a positive idle or termination receipt.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RetentionSnapshot {
    pub(super) proof_cycles: usize,
    pub(super) proof_results: usize,
    pub(super) accepted: usize,
    pub(super) preparations: usize,
    pub(super) pending_releases: usize,
    pub(super) failed: bool,
    pub(super) records: usize,
    pub(super) payload_positions: usize,
    pub(super) controls: usize,
    pub(super) supervisors: usize,
    pub(super) cleanup_unreaped: usize,
    pub(super) deliveries: usize,
}
impl PayloadRegistry {
    pub(super) fn retention_locked(&self) -> RetentionSnapshot {
        RetentionSnapshot {
            proof_cycles: self.proof.retained(),
            proof_results: self.proof.results(),
            accepted: self.entries.len() + self.controls.len(),
            preparations: self.preparations.iter().flatten().count(),
            pending_releases: self.pending_releases + self.rollback_notifications.len(),
            failed: self.failed || self.proof.failed(),
            records: self.records.iter().flatten().count(),
            payload_positions: self.positions.iter().flatten().count(),
            controls: self.controls.len(),
            supervisors: self.proof.handles()
                + self.entries.len()
                + self.controls.len()
                + self
                    .preparations
                    .iter()
                    .flatten()
                    .filter(|p| p.entry.is_some())
                    .count(),
            cleanup_unreaped: self
                .entries
                .iter()
                .chain(&self.controls)
                .chain(
                    self.preparations
                        .iter()
                        .flatten()
                        .filter_map(|p| p.entry.as_ref()),
                )
                .filter(|e| e.slot.cleanup.load(Ordering::Acquire))
                .count(),
            deliveries: self
                .entries
                .iter()
                .chain(&self.controls)
                .map(|e| *e.slot.delivery_holds.lock().unwrap())
                .sum(),
        }
    }
}
// The mutable borrow pins this reservation to one still-locked transaction.
// Tentative parent ownership rolls back without acquiring ReceiverState again.
pub(super) struct EntryReservation<'a, 'p> {
    prepared: &'p mut PreparedEntry,
    registry: &'a mut PayloadRegistry,
    preparation: usize,
    parent: Option<Arc<Slot>>,
    record: Option<usize>,
}
impl Drop for EntryReservation<'_, '_> {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.take() {
            self.registry.rollback_parent_locked(parent);
        }
    }
}
impl PayloadRegistry {
    fn reserve_entry_locked<'a, 'p>(
        &'a mut self,
        prepared: &'p mut PreparedEntry,
        parent: Option<&PayloadLease>,
        kind: Kind,
        shared: &Arc<Shared>,
    ) -> Result<EntryReservation<'a, 'p>> {
        self.evidence.begin(true).map_err(|_| OwnerError::Refused)?;
        if self.closed
            || self.failed
            || self.rollback_notifications.len() == 32
            || (kind != Kind::Control && self.entries.len() >= self.accepted_limit)
            || (kind == Kind::Control && self.controls.len() >= 64)
            || (kind == Kind::Process && self.process_used)
            || !Arc::ptr_eq(&prepared._shared, shared)
            || prepared.slot.kind != kind
        {
            return Err(OwnerError::Refused);
        }
        let preparation = self
            .preparations
            .iter()
            .position(|p| {
                p.as_ref().is_some_and(|p| {
                    p.id == prepared.slot.id
                        && !p.cancelled
                        && p.entry
                            .as_ref()
                            .is_some_and(|e| Arc::ptr_eq(&e.slot, &prepared.slot))
                })
            })
            .ok_or(OwnerError::Refused)?;
        if kind == Kind::Operation || kind == Kind::Stream {
            let guards = &self.preparations[preparation]
                .as_ref()
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .guards;
            if let Admission::Legacy {
                permit, lifetime, ..
            } = &**guards
                && (lifetime.is_none() || permit.as_ref().is_some_and(|p| p.num_permits() != 1))
            {
                return Err(OwnerError::Refused);
            }
        }
        let record = if kind == Kind::Control {
            Some(
                self.records
                    .iter()
                    .position(Option::is_none)
                    .ok_or(OwnerError::Refused)?,
            )
        } else {
            None
        };
        if kind != Kind::Process && kind != Kind::Control && parent.is_none() {
            return Err(OwnerError::Refused);
        }
        if let Some(p) = parent {
            if !Arc::ptr_eq(&p.shared, shared)
                || p.slot.cancelled()
                || p.slot.kind
                    != if kind == Kind::Operation {
                        Kind::Process
                    } else {
                        Kind::Operation
                    }
                || !self.entries.iter().any(|e| Arc::ptr_eq(&e.slot, &p.slot))
            {
                return Err(OwnerError::Refused);
            }
            self.claim_parent_locked(&p.slot)?;
        }
        Ok(EntryReservation {
            prepared,
            registry: self,
            preparation,
            parent: parent.map(|p| p.slot.clone()),
            record,
        })
    }
}
impl EntryReservation<'_, '_> {
    fn install_entry_locked(
        mut self,
        initial: Initial,
        production: Option<LocalProduction>,
    ) -> PayloadLease {
        self.registry.evidence.record();
        let prepared = &mut *self.prepared;
        let parent = self.parent.clone();
        prepared
            .slot
            .producing
            .store(production.is_some(), Ordering::Release);
        let mut entry = self.registry.preparations[self.preparation]
            .take()
            .unwrap()
            .entry
            .unwrap();
        entry.guards.set_parent(self.parent.take().map(ParentHold));
        if entry.slot.kind == Kind::Process {
            self.registry.process_used = true;
        }
        if let Some(record) = self.record {
            entry.record = Some(record);
            self.registry.records[record] = Some(AcceptedOwner::Control(entry.slot.id));
            self.registry.controls.push(entry);
        } else {
            self.registry.entries.push(entry);
        }
        if prepared
            .activate
            .take()
            .unwrap()
            .send(Activation {
                parent,
                initial,
                production,
            })
            .is_err()
        {
            {
                let _change = prepared.slot.evidence.begin(false).expect("settlement");
                prepared.slot.failed.store(true, Ordering::Release);
            }
            self.registry.failed = true;
        }
        PayloadLease {
            slot: prepared.slot.clone(),
            shared: prepared._shared.clone(),
            tx: prepared.tx.clone(),
            initial: prepared.initial.take(),
        }
    }
}
// JoinHandle keeps one waker. Give it a stable broadcast waker so cancelling
// whichever disposal waiter polled last cannot strand the remaining waiters.
struct ReapWake(Arc<Notify>);
impl Wake for ReapWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_waiters();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_waiters();
    }
}
struct Shared {
    state: Arc<Mutex<super::admission::ReceiverState>>,
    #[cfg(test)]
    pair_preparing: Mutex<Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    #[cfg(test)]
    releasing: Mutex<Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    reap_wake: Arc<ReapWake>,
    pool: WorkerPool,
    operations: Arc<Semaphore>,
    operation_transport: Arc<Semaphore>,
    dependency_transport: Arc<Semaphore>,
    outgoing: Arc<Semaphore>,
    outgoing_limit: usize,
}
pub(crate) struct PayloadOwners {
    shared: Arc<Shared>,
}
enum Request {
    Send {
        writer: super::payload_stream::Writer,
        job: Box<JobEnvelope>,
        request: bool,
        reply: oneshot::Sender<Result<super::payload_stream::Transport>>,
    },
    Command {
        command: Command,
        reply: oneshot::Sender<Result<Reply>>,
    },
    Receive {
        reader: super::payload_stream::Reader,
        expected: Option<Box<JobEnvelope>>,
        checked: Option<OwnedRequestPayload>,
        checked_reply: Option<Box<(super::auth::ReplyHandoff, Box<JobEnvelope>)>>,
        reply: oneshot::Sender<Result<Reply>>,
    },
}
pub(crate) struct PayloadLease {
    slot: Arc<Slot>,
    shared: Arc<Shared>,
    tx: mpsc::Sender<Request>,
    initial: Option<oneshot::Receiver<Result<Reply>>>,
}
struct Abandon(Option<Arc<Slot>>);
impl Drop for Abandon {
    fn drop(&mut self) {
        if let Some(s) = &self.0 {
            s.cancel();
        }
    }
}
impl Drop for PayloadLease {
    fn drop(&mut self) {
        self.slot.cancel();
    }
}

impl PayloadOwners {
    pub(super) fn reap(&self) {
        reap_shared(&self.shared, None);
    }
    /// All capacities and lifetime permits are injected. This type never uses partition permits.
    pub(crate) fn new(
        pool: WorkerPool,
        operations: Arc<Semaphore>,
        operation_transport: Arc<Semaphore>,
        dependency_transport: Arc<Semaphore>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Arc::new(Mutex::new(super::admission::ReceiverState::legacy(
                    PayloadRegistry {
                        evidence: Arc::new(super::admission::WorkEvidence::default()),
                        proof: Default::default(),
                        closed: false,
                        failed: false,
                        process_used: false,
                        next: 0,
                        entries: Vec::with_capacity(8481),
                        accepted_limit: 8481,
                        operation_bound: 4352,
                        operation_stream_bound: 16,
                        dependency_stream_bound: 16,
                        outgoing_bound: 16,
                        positions: vec![None; 8481],
                        records: (0..8768).map(|_| None).collect(),
                        controls: Vec::with_capacity(64),
                        boot_reserved: None,
                        pending_releases: 0,
                        rollback_notifications: Vec::with_capacity(32),
                        preparations: std::array::from_fn(|_| None),
                    },
                ))),
                #[cfg(test)]
                releasing: Mutex::new(None),
                #[cfg(test)]
                pair_preparing: Mutex::new(None),
                reap_wake: Arc::new(ReapWake(Arc::new(Notify::new()))),
                pool,
                operations,
                operation_transport,
                dependency_transport,
                outgoing: Arc::new(Semaphore::new(16)),
                outgoing_limit: 16,
            }),
        }
    }
    pub(crate) fn try_process(&self, parent: PathBuf, limits: SpoolLimits) -> Result<PayloadLease> {
        self.register(
            None,
            Kind::Process,
            Initial::Process(parent, limits),
            None,
            None,
            None,
        )
    }
    pub(crate) fn try_operation(
        &self,
        process: &PayloadLease,
        limits: SpoolLimits,
        lifetime: Arc<OwnedSemaphorePermit>,
    ) -> Result<PayloadLease> {
        self.register(
            Some(process),
            Kind::Operation,
            Initial::Operation(limits),
            Some(&self.shared.operations),
            Some(lifetime),
            None,
        )
    }
    pub(crate) fn try_admit(
        &self,
        operation: &PayloadLease,
        role: Role,
        lifetime: Arc<OwnedSemaphorePermit>,
    ) -> Result<PayloadLease> {
        self.try_admit_until(operation, role, lifetime, None)
    }
    pub(crate) fn try_admit_until(
        &self,
        operation: &PayloadLease,
        role: Role,
        lifetime: Arc<OwnedSemaphorePermit>,
        deadline: Option<Instant>,
    ) -> Result<PayloadLease> {
        let capacity = match role {
            Role::Operation => &self.shared.operation_transport,
            Role::Dependency => &self.shared.dependency_transport,
        };
        self.register(
            Some(operation),
            Kind::Stream,
            Initial::Stream,
            Some(capacity),
            Some(lifetime),
            deadline,
        )
    }
    fn register(
        &self,
        parent: Option<&PayloadLease>,
        kind: Kind,
        initial: Initial,
        capacity: Option<&Arc<Semaphore>>,
        lifetime: Option<Arc<OwnedSemaphorePermit>>,
        deadline: Option<Instant>,
    ) -> Result<PayloadLease> {
        self.register_with(
            parent,
            kind,
            initial,
            capacity,
            lifetime,
            deadline,
            |_, _| None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn register_with(
        &self,
        parent: Option<&PayloadLease>,
        kind: Kind,
        initial: Initial,
        capacity: Option<&Arc<Semaphore>>,
        lifetime: Option<Arc<OwnedSemaphorePermit>>,
        deadline: Option<Instant>,
        production: impl FnOnce(Arc<Slot>, Option<Arc<Slot>>) -> Option<LocalProduction>,
    ) -> Result<PayloadLease> {
        if !self.shared.state.lock().unwrap().legacy_mode() {
            return Err(OwnerError::Refused);
        }
        let id = self.reserve_preparation(Admission::legacy(None, lifetime), false, capacity)?;
        let prepared = self.spawn_preparation(id, kind, deadline)?;
        self.activate_legacy(prepared, parent, kind, initial, production)
    }
    fn activate_legacy(
        &self,
        mut prepared: PreparedEntry,
        parent: Option<&PayloadLease>,
        kind: Kind,
        initial: Initial,
        production: impl FnOnce(Arc<Slot>, Option<Arc<Slot>>) -> Option<LocalProduction>,
    ) -> Result<PayloadLease> {
        // Future construction can allocate; finish it before taking the receiver lock.
        let parent_slot = parent.map(|p| p.slot.clone());
        let production = production(prepared.slot.clone(), parent_slot);
        let mut state = self.shared.state.lock().unwrap();
        if !state.legacy_mode() {
            return Err(OwnerError::Refused);
        }
        let reservation =
            state
                .payload
                .reserve_entry_locked(&mut prepared, parent, kind, &self.shared)?;
        Ok(reservation.install_entry_locked(initial, production))
    }
    fn reserve_preparation(
        &self,
        guards: Admission,
        control: bool,
        capacity: Option<&Arc<Semaphore>>,
    ) -> Result<u64> {
        reap_shared(&self.shared, None);
        let mut guards = Some(guards);
        let mut state = self.shared.state.lock().unwrap();
        let registry = &mut state.payload;
        registry.reserve_preparation_locked(&mut guards, control, capacity)
    }
    fn prepare_entry(
        &self,
        kind: Kind,
        deadline: Option<Instant>,
        guards: Admission,
        control: bool,
    ) -> Result<PreparedEntry> {
        let id = self.reserve_preparation(guards, control, None)?;
        self.spawn_preparation(id, kind, deadline)
    }
    fn spawn_preparation(
        &self,
        id: u64,
        kind: Kind,
        deadline: Option<Instant>,
    ) -> Result<PreparedEntry> {
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                let mut state = self.shared.state.lock().unwrap();
                let registry = &mut state.payload;
                let position = registry
                    .preparations
                    .iter()
                    .position(|p| p.as_ref().is_some_and(|p| p.id == id))
                    .unwrap();
                let placeholder = registry.preparations[position].take().unwrap();
                if registry.boot_reserved == Some(id) {
                    registry.failed = true;
                }
                registry.evidence.record();
                registry.pending_releases += 1;
                drop(state);
                drop(ManuallyDrop::into_inner(placeholder.guards.unwrap()));
                {
                    let mut state = self.shared.state.lock().unwrap();
                    state.payload.evidence.record();
                    state.payload.pending_releases -= 1;
                }
                self.shared.reap_wake.0.notify_waiters();
                return Err(OwnerError::Refused);
            }
        };
        let (wake, _) = watch::channel(false);
        let (initialization_done, _) = watch::channel(None);
        let slot = Arc::new(Slot {
            evidence: self.shared.state.lock().unwrap().payload.evidence.clone(),
            id,
            operation_id: Mutex::new(None),
            receiver_limits: std::sync::OnceLock::new(),
            kind,
            deadline,
            disk: Mutex::new(Some(ManuallyDrop::new(Disk::empty()))),
            retained_memory: Mutex::new(None),
            handoff: Mutex::new(Handoff { cancelled: false }),
            wake,
            children: Mutex::new(0),
            child_done: Notify::new(),
            producing: AtomicBool::new(false),
            cleanup: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            initialization_done,
            delivery_holds: Mutex::new(0),
            response_live: AtomicBool::new(false),
            delivery_done: Notify::new(),
            #[cfg(test)]
            hooks: Mutex::default(),
        });
        let (tx, rx) = mpsc::channel(1);
        let (ready, initial) = oneshot::channel();
        let (activate, activation) = oneshot::channel::<Activation>();
        let pool = self.shared.pool.clone();
        let supervised = slot.clone();
        let mut wake = slot.wake.subscribe();
        let request = RequestStorage::new();
        let handle = nabedra_rt::task::spawn_on(&runtime, async move {
            #[cfg(test)]
            if let Some(polled) = supervised.hooks.lock().unwrap().dormant_pending.take() {
                let _ = polled.send(());
            }
            let activation = tokio::select! {
                biased;
                activation = activation => activation.ok(),
                _ = async { if !*wake.borrow() { let _ = wake.changed().await; } } => None,
            };
            if let Some(activation) = activation {
                supervise(
                    pool,
                    supervised,
                    activation.parent,
                    activation.initial,
                    ready,
                    rx,
                    activation.production,
                )
                .await;
            } else {
                // Memory-only abandonment: no WorkerPool command or filesystem work.
                {
                    let _change = supervised.evidence.begin(false).expect("settlement");
                    supervised.cleanup.store(true, Ordering::Release);
                }
                #[cfg(test)]
                tests::before_return(&supervised).await;
            }
        });
        // Publication immediately follows spawn. No await, callback or caller return.
        let mut state = self.shared.state.lock().unwrap();
        let registry = &mut state.payload;
        let position = registry.positions.iter().position(|p| *p == Some(id));
        let placeholder = registry
            .preparations
            .iter_mut()
            .flatten()
            .find(|p| p.id == id)
            .unwrap();
        if placeholder.cancelled {
            slot.cancel();
        }
        registry.evidence.record();
        placeholder.entry = Some(Entry {
            slot: slot.clone(),
            handle,
            guards: placeholder.guards.take().unwrap(),
            terminated: false,
            position,
            record: None,
            account_tx: None,
            account_ready: None,
            request,
        });
        self.shared.reap_wake.0.notify_waiters();
        Ok(PreparedEntry {
            _shared: self.shared.clone(),
            slot,
            activate: Some(activate),
            tx,
            initial: Some(initial),
        })
    }
    // Transfers the already-held control exchange into the reserved preparation share.
    #[allow(dead_code)]
    pub(super) fn prepare_control(&self, transport: OwnedSemaphorePermit) -> Result<PreparedEntry> {
        let distributed = !self.shared.state.lock().unwrap().legacy_mode();
        let guards = if distributed {
            Admission::distributed(DistributedOwner::Control, Some(Arc::new(transport)))
        } else {
            Admission::legacy(Some(transport), None)
        };
        self.prepare_entry(
            if distributed {
                Kind::Control
            } else {
                Kind::Stream
            },
            None,
            guards,
            true,
        )
    }
    pub(crate) async fn close_and_drain(&self) -> Result<()> {
        {
            let mut state = self.shared.state.lock().unwrap();
            let (r, local) = state.parts();
            if let Some(local) = local {
                local.close_payload_dependencies();
            }
            if !r.closed {
                r.evidence.record();
            }
            r.closed = true;
            r.proof.cancel();
            for e in r.entries.iter().chain(&r.controls) {
                e.slot.cancel();
            }
            for p in r.preparations.iter_mut().flatten() {
                p.cancelled = true;
                if let Some(e) = &p.entry {
                    e.slot.cancel();
                }
            }
        }
        wait_reap(&self.shared, None).await
    }
}
// Poll handles in registry-owned storage. Dropping this waiter never takes a handle.
fn reap_locked(
    registry: &mut PayloadRegistry,
    releases: &mut Vec<Admission>,
    mut cx: Option<&mut Context<'_>>,
    mut local: Option<&mut super::admission::LocalAdmission>,
) -> bool {
    let mut changed = false;
    for preparation in &mut registry.preparations {
        if let Some(p) = preparation
            && let Some(entry) = &mut p.entry
        {
            if reap_entry(entry, cx.as_deref_mut())
                && finish_entry_locked(
                    entry,
                    registry.positions.as_mut_slice(),
                    registry.records.as_mut_slice(),
                    local.as_deref_mut(),
                )
            {
                let p = preparation.take().unwrap();
                releases.push(ManuallyDrop::into_inner(p.entry.unwrap().guards));
                changed = true;
            } else if entry.slot.failed.load(Ordering::Acquire) {
                changed |= !registry.failed;
                registry.failed = true;
            }
        }
    }
    let mut i = 0;
    while i < registry.entries.len() {
        let entry = &mut registry.entries[i];
        if reap_entry(entry, cx.as_deref_mut())
            && finish_entry_locked(
                entry,
                registry.positions.as_mut_slice(),
                registry.records.as_mut_slice(),
                local.as_deref_mut(),
            )
        {
            let entry = registry.entries.swap_remove(i);
            releases.push(ManuallyDrop::into_inner(entry.guards));
            changed = true;
        } else {
            if entry.slot.failed.load(Ordering::Acquire) {
                changed |= !registry.failed;
                registry.failed = true;
            }
            i += 1;
        }
    }
    let mut i = 0;
    while i < registry.controls.len() {
        let entry = &mut registry.controls[i];
        if reap_entry(entry, cx.as_deref_mut())
            && finish_entry_locked(
                entry,
                &mut registry.positions,
                &mut registry.records,
                local.as_deref_mut(),
            )
        {
            let entry = registry.controls.swap_remove(i);
            releases.push(ManuallyDrop::into_inner(entry.guards));
            changed = true;
        } else {
            if entry.slot.failed.load(Ordering::Acquire) {
                registry.failed = true;
            }
            i += 1;
        }
    }
    changed
}
fn finish_entry_locked(
    entry: &mut Entry,
    positions: &mut [Option<u64>],
    records: &mut [Option<AcceptedOwner>],
    local: Option<&mut super::admission::LocalAdmission>,
) -> bool {
    if let Some(p) = entry.record {
        let valid = match (records[p], entry.guards.owner()) {
            (Some(AcceptedOwner::Work(a)), Some(DistributedOwner::Work(b))) => a == b,
            (Some(AcceptedOwner::Control(id)), Some(DistributedOwner::Control)) => {
                id == entry.slot.id
            }
            _ => false,
        };
        if !valid {
            {
                let _change = entry.slot.evidence.begin(false).expect("settlement");
                entry.slot.failed.store(true, Ordering::Release);
            }
            return false;
        }
    }
    if entry
        .position
        .is_some_and(|p| positions[p] != Some(entry.slot.id))
    {
        {
            let _change = entry.slot.evidence.begin(false).expect("settlement");
            entry.slot.failed.store(true, Ordering::Release);
        }
        return false;
    }
    let result = match entry.guards.owner() {
        Some(DistributedOwner::Work(key)) => local.ok_or(OwnerError::FailedDrain).and_then(|l| {
            let Admission::Distributed {
                dependency: Some(dependency),
                ..
            } = &*entry.guards
            else {
                return Err(OwnerError::FailedDrain);
            };
            l.reap_payload_work(key, dependency)
                .map_err(|_| OwnerError::FailedDrain)
        }),
        Some(DistributedOwner::Dependency(key)) => local
            .ok_or(OwnerError::FailedDrain)
            .and_then(|l| l.release_account(key).map_err(|_| OwnerError::FailedDrain)),
        _ => Ok(()),
    };
    if result.is_err() {
        {
            let _change = entry.slot.evidence.begin(false).expect("settlement");
            entry.slot.failed.store(true, Ordering::Release);
        }
        return false;
    }
    if let Some(p) = entry.position {
        if positions[p] != Some(entry.slot.id) {
            {
                let _change = entry.slot.evidence.begin(false).expect("settlement");
                entry.slot.failed.store(true, Ordering::Release);
            }
            return false;
        }
        positions[p] = None;
    }
    if let Some(p) = entry.record {
        records[p] = None;
    }
    true
}
fn reap_entry(entry: &mut Entry, cx: Option<&mut Context<'_>>) -> bool {
    if !entry.terminated {
        let outcome = if let Some(cx) = cx {
            Pin::new(&mut entry.handle).poll(cx)
        } else if entry.handle.is_finished() {
            Pin::new(&mut entry.handle).poll(&mut Context::from_waker(Waker::noop()))
        } else {
            Poll::Pending
        };
        if let Poll::Ready(result) = outcome {
            entry.slot.evidence.record();
            entry.terminated = true;
            if result.is_err() || !entry.slot.cleanup.load(Ordering::Acquire) {
                {
                    let _change = entry.slot.evidence.begin(false).expect("settlement");
                    entry.slot.failed.store(true, Ordering::Release);
                }
            }
        }
    }
    entry.terminated
        && entry.slot.cleanup.load(Ordering::Acquire)
        && !entry.slot.failed.load(Ordering::Acquire)
}

// Release memory guards only after unlocking. Keep a bounded marker until every
// release-triggered parent transition has completed, including its notification.
fn reap_shared(shared: &Shared, cx: Option<&mut Context<'_>>) -> bool {
    let mut releases = Vec::with_capacity(8481 + 64 + 32);
    let mut notifications = Vec::with_capacity(32);
    let proof_release;
    let changed = {
        let mut state = shared.state.lock().unwrap();
        let (registry, mut local) = state.parts();
        let waker = Waker::from(shared.reap_wake.clone());
        proof_release = registry.proof.poll(&mut Context::from_waker(&waker));
        let changed = reap_locked(registry, &mut releases, cx, local.as_deref_mut());
        if let Some(local) = local {
            for entry in &registry.entries {
                if let Some(DistributedOwner::Dependency(key)) = entry.guards.owner()
                    && local.dependency_closing(key)
                {
                    entry.slot.cancel();
                }
            }
        }
        if changed {
            registry.evidence.record();
        }
        notifications.append(&mut state.payload.rollback_notifications);
        state.payload.pending_releases +=
            releases.len() + notifications.len() + usize::from(proof_release.is_some());
        if state.payload.pending_releases == 0 {
            state.payload.evidence.record_locked();
        }
        changed
    };
    let proof_count = usize::from(proof_release.is_some());
    let count = releases.len() + notifications.len() + proof_count;
    #[cfg(test)]
    if count != 0
        && let Some((at_release, gate)) = shared.releasing.lock().unwrap().take()
    {
        let _ = at_release.send(());
        let _ = gate.recv_timeout(std::time::Duration::from_secs(10));
    }
    if let Some(cycle) = &proof_release {
        cycle.release();
    }
    drop(proof_release);
    drop(releases);
    for parent in notifications {
        parent.child_done.notify_one();
    }
    if count != 0 {
        let mut state = shared.state.lock().unwrap();
        if count != proof_count {
            state.payload.evidence.record();
        }
        state.payload.pending_releases -= count;
        state.payload.evidence.record_locked();
    }
    if changed || count != 0 {
        shared.reap_wake.0.notify_waiters();
    }
    changed
}

async fn wait_reap(shared: &Shared, id: Option<u64>) -> Result<()> {
    let waker = Waker::from(shared.reap_wake.clone());
    loop {
        let notified = shared.reap_wake.0.notified();
        tokio::pin!(notified);
        // Register before inspecting handles/state; removal or termination
        // between inspection and await must wake every surviving waiter.
        notified.as_mut().enable();
        reap_shared(shared, Some(&mut Context::from_waker(&waker)));
        let result = {
            let mut state = shared.state.lock().unwrap();
            let r = &mut state.payload;
            if r.failed || r.proof.failed() {
                Some(Err(OwnerError::FailedDrain))
            } else if (id.is_some() || r.proof.retained() == 0)
                && r.retention_locked().pending_releases == 0
                && r.preparations
                    .iter()
                    .flatten()
                    .all(|p| id.is_some_and(|id| p.id != id))
                && r.controls
                    .iter()
                    .all(|e| id.is_some_and(|id| e.slot.id != id))
                && r.entries
                    .iter()
                    .all(|e| id.is_some_and(|id| e.slot.id != id))
            {
                Some(Ok(()))
            } else {
                None
            }
        };
        if let Some(result) = result {
            return result;
        }
        notified.await;
    }
}
impl PayloadLease {
    /// Typed local completion; early errors settle disposal before returning.
    pub(crate) async fn local_complete(mut self) -> Result<(Box<ResultEnvelope>, Self)> {
        match self.ready().await {
            Ok(Reply::Accepted(envelope)) if !self.slot.cancelled() => Ok((envelope, self)),
            other => {
                self.dispose().await?;
                Err(other.err().unwrap_or(OwnerError::Invalid))
            }
        }
    }
    // Guard is constructed before the future, so even an unpolled future cancels.
    pub(crate) fn ready(&mut self) -> impl Future<Output = Result<Reply>> + '_ {
        let mut guard = Abandon(Some(self.slot.clone()));
        async move {
            let rx = self.initial.take().ok_or(OwnerError::Invalid)?;
            let result = rx.await.map_err(|_| OwnerError::FailedDrain)?;
            if result.is_ok() {
                guard.0.take();
            }
            result
        }
    }
    pub(crate) fn command(&mut self, command: Command) -> impl Future<Output = Result<Reply>> + '_ {
        let mut guard = Abandon(Some(self.slot.clone()));
        async move {
            if self.slot.cancelled() {
                return Err(OwnerError::Cancelled);
            }
            let change = self
                .slot
                .evidence
                .begin(true)
                .map_err(|_| OwnerError::Refused)?;
            let (reply, rx) = oneshot::channel();
            self.tx
                .try_send(Request::Command { command, reply })
                .map_err(|_| OwnerError::Invalid)?;
            drop(change);
            let result = rx.await.map_err(|_| OwnerError::FailedDrain)?;
            if result.is_ok() {
                guard.0.take();
            }
            result
        }
    }
    pub(super) async fn receive(
        mut self,
        reader: super::payload_stream::Reader,
        expected: Option<Box<JobEnvelope>>,
    ) -> Result<(Reply, Self)> {
        let result = async {
            if self.initial.is_some() {
                self.ready().await?;
            }
            let change = self
                .slot
                .evidence
                .begin(true)
                .map_err(|_| OwnerError::Refused)?;
            let (reply, rx) = oneshot::channel();
            self.tx
                .try_send(Request::Receive {
                    reader,
                    expected,
                    checked: None,
                    checked_reply: None,
                    reply,
                })
                .map_err(|_| OwnerError::Invalid)?;
            drop(change);
            rx.await.unwrap_or(Err(OwnerError::FailedDrain))
        }
        .await;
        match result {
            Ok(reply) => Ok((reply, self)),
            Err(error) => {
                self.dispose().await?;
                Err(error)
            }
        }
    }
    pub(super) async fn receive_checked(
        mut self,
        reader: super::payload_stream::Reader,
    ) -> Result<(Reply, Self)> {
        let result = async {
            if self.initial.is_some() {
                self.ready().await?;
            }
            let checked = self.take_request()?;
            let change = self
                .slot
                .evidence
                .begin(true)
                .map_err(|_| OwnerError::Refused)?;
            let (reply, rx) = oneshot::channel();
            self.tx
                .try_send(Request::Receive {
                    reader,
                    expected: None,
                    checked: Some(checked),
                    checked_reply: None,
                    reply,
                })
                .map_err(|_| OwnerError::Invalid)?;
            drop(change);
            rx.await.unwrap_or(Err(OwnerError::FailedDrain))
        }
        .await;
        match result {
            Ok(reply) => Ok((reply, self)),
            Err(error) => {
                self.dispose().await?;
                Err(error)
            }
        }
    }
    pub(super) async fn receive_checked_reply(
        &mut self,
        reader: super::payload_stream::Reader,
        handoff: super::auth::ReplyHandoff,
        expected: Box<JobEnvelope>,
    ) -> Result<Reply> {
        let mut guard = Abandon(Some(self.slot.clone()));
        let result = async {
            if self.initial.is_some() {
                self.ready().await?;
            }
            let change = self
                .slot
                .evidence
                .begin(true)
                .map_err(|_| OwnerError::Refused)?;
            let (reply, rx) = oneshot::channel();
            self.tx
                .try_send(Request::Receive {
                    reader,
                    expected: None,
                    checked: None,
                    checked_reply: Some(Box::new((handoff, expected))),
                    reply,
                })
                .map_err(|_| OwnerError::Invalid)?;
            drop(change);
            rx.await.unwrap_or(Err(OwnerError::FailedDrain))
        }
        .await;
        if result.is_ok() {
            guard.0.take();
        }
        result
    }
    pub(super) async fn send(
        mut self,
        writer: super::payload_stream::Writer,
        job: Box<JobEnvelope>,
        request: bool,
    ) -> Result<super::payload_stream::Transport> {
        let result = async {
            if self.initial.is_some() {
                self.ready().await?;
            }
            let change = self
                .slot
                .evidence
                .begin(true)
                .map_err(|_| OwnerError::Refused)?;
            let (reply, rx) = oneshot::channel();
            self.tx
                .try_send(Request::Send {
                    writer,
                    job,
                    request,
                    reply,
                })
                .map_err(|_| OwnerError::Invalid)?;
            drop(change);
            rx.await.unwrap_or(Err(OwnerError::FailedDrain))
        }
        .await;
        self.dispose().await?;
        result
    }
    pub(crate) fn dispose(self) -> impl Future<Output = Result<()>> {
        self.slot.cancel();
        async move { wait_reap(&self.shared, Some(self.slot.id)).await }
    }
}
pub(super) async fn run_command(
    pool: &WorkerPool,
    slot: &Arc<Slot>,
    command: Command,
) -> Result<Reply> {
    if slot.producing.load(Ordering::Acquire) {
        return Err(OwnerError::Invalid);
    }
    run(pool, slot, Work::Command(command)).await
}
async fn run(pool: &WorkerPool, slot: &Arc<Slot>, work: Work) -> Result<Reply> {
    let work = match work {
        Work::Command(Command::ExecuteCaptured(storage)) => {
            let job = {
                let disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
                let Processor::AcceptedRequest(job) =
                    &disk.as_ref().ok_or(OwnerError::Invalid)?.processor
                else {
                    return Err(OwnerError::Invalid);
                };
                job.clone()
            };
            let (input, charge) = crate::engine::prepare_captured(&storage, pool, &job)
                .await
                .map_err(|_| OwnerError::Invalid)?;
            let request = job.scheduler_request_id();
            let cell = slot.clone();
            let (reply, mut charge) = pool
                .submit_buffered(
                    request,
                    1,
                    vec![charge],
                    pool.resources().task_memory_limit_bytes,
                    move |_, runtime| {
                        Ok(execute(
                            &cell,
                            Work::Command(Command::ExecutePrepared(input)),
                            runtime,
                        ))
                    },
                )
                .await
                .map_err(|_| OwnerError::Invalid)?
                .receive()
                .await
                .map_err(|_| OwnerError::Invalid)?;
            let retained = {
                let disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
                match &disk.as_ref().ok_or(OwnerError::Invalid)?.processor {
                    Processor::Accepted(rows, _) => {
                        let payload = rows.iter().map(crate::snapshot::row_payload).sum::<usize>();
                        crate::snapshot::memory_shape(&job.table, rows.len(), payload, 0)
                            .map_err(|_| OwnerError::Invalid)?
                    }
                    _ => 4096,
                }
            };
            charge
                .shrink_to(retained.min(charge.bytes()))
                .map_err(|_| OwnerError::Invalid)?;
            *slot.retained_memory.lock().unwrap() = Some(charge);
            return reply;
        }
        Work::Command(Command::UploadStaged(storage)) => {
            return upload_staged(pool, slot, storage).await;
        }
        other => other,
    };
    let cell = slot.clone();
    let submit = pool.submit(
        (slot.id % pool.worker_count().get() as u64) as usize,
        move |_, runtime| Ok(execute(&cell, work, runtime)),
    );
    tokio::pin!(submit);
    let job = poll_fn(|cx| {
        let polled = submit.as_mut().poll(cx);
        #[cfg(test)]
        if polled.is_pending()
            && let Some(tx) = slot.hooks.lock().unwrap().submit_pending.take()
        {
            let _ = tx.send(());
        }
        polled
    })
    .await
    .map_err(|_| OwnerError::FailedDrain)?;
    job.receive().await.map_err(|_| OwnerError::FailedDrain)?
}
async fn upload_staged(
    pool: &WorkerPool,
    slot: &Arc<Slot>,
    storage: Arc<crate::storage::Storage<crate::storage::RuntimeStore>>,
) -> Result<Reply> {
    let uploads = {
        let mut disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
        std::mem::take(&mut disk.as_mut().ok_or(OwnerError::Invalid)?.uploads)
    };
    let request = pool.request_id();
    for (table, commit, ordinal, result) in uploads {
        let (_, parts) = result.into_parts();
        for (i, part) in parts.unwrap_or_default().into_iter().enumerate() {
            if slot.cancelled() {
                return Err(OwnerError::Cancelled);
            }
            let n = ordinal
                .checked_add(u32::try_from(i).map_err(|_| OwnerError::Invalid)?)
                .ok_or(OwnerError::Invalid)?;
            let key = crate::storage::ObjectKey::partition(table, commit, n);
            let charge = pool
                .scheduler()
                .reserve_memory(request, 0, pool.resources().task_memory_limit_bytes)
                .await
                .map_err(|_| OwnerError::Refused)?;
            let ((meta, bytes), _charge) = pool
                .submit_buffered(
                    request,
                    1,
                    vec![charge],
                    pool.resources().task_memory_limit_bytes,
                    move |_, _| {
                        let (meta, mut reader) = part.into_reader().map_err(|_| {
                            crate::protocol::ProtocolError::new(
                                crate::protocol::ErrorCode::StorageUnavailable,
                                "staged partition unavailable",
                                crate::protocol::Outcome::NotApplied,
                            )
                        })?;
                        crate::scheduling::check_task_memory(
                            meta.byte_size.checked_mul(3).ok_or_else(|| {
                                crate::protocol::ProtocolError::new(
                                    crate::protocol::ErrorCode::LimitExceeded,
                                    "staged bytes exceed limit",
                                    crate::protocol::Outcome::NotApplied,
                                )
                            })?,
                        )?;
                        let mut bytes = Vec::new();
                        bytes
                            .try_reserve_exact(meta.byte_size as usize)
                            .map_err(|_| {
                                crate::protocol::ProtocolError::new(
                                    crate::protocol::ErrorCode::LimitExceeded,
                                    "staged allocation limit",
                                    crate::protocol::Outcome::NotApplied,
                                )
                            })?;
                        reader.read_to_end(&mut bytes).map_err(|_| {
                            crate::protocol::ProtocolError::new(
                                crate::protocol::ErrorCode::StorageUnavailable,
                                "staged partition unavailable",
                                crate::protocol::Outcome::NotApplied,
                            )
                        })?;
                        Ok((meta, bytes))
                    },
                )
                .await
                .map_err(|_| OwnerError::Invalid)?
                .receive()
                .await
                .map_err(|_| OwnerError::Invalid)?;
            let deadline = nabedra_rt::time::now() + std::time::Duration::from_secs(30);
            let wrote = nabedra_rt::time::timeout_at(
                deadline,
                storage.put(&key, bytes.clone(), crate::storage::WriteCondition::Create),
            )
            .await;
            if !matches!(wrote,Ok(Ok(ref token)) if !token.is_empty()) {
                let confirmed = nabedra_rt::time::timeout_at(
                    deadline,
                    storage.read(&key, meta.byte_size as usize),
                )
                .await;
                if !matches!(confirmed,Ok(Ok(object)) if object.bytes==bytes && !object.etag.is_empty())
                {
                    return Err(OwnerError::Invalid);
                }
            }
        }
    }
    Ok(Reply::Ready)
}
enum Work {
    Initialize(Initial, Option<Arc<Slot>>),
    Command(Command),
    Discard,
}
fn execute(slot: &Slot, work: Work, runtime: &WorkerRuntime) -> Result<Reply> {
    let _change = slot
        .evidence
        .begin(false)
        .expect("accepted worker transition");
    if matches!(work, Work::Discard) {
        #[cfg(test)]
        tests::installation_gate(slot, true);
        slot.discard();
        return Ok(Reply::Ready);
    }
    let mut disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
    let result = catch_unwind(AssertUnwindSafe(|| {
        #[cfg(test)]
        {
            tests::installation_gate(slot, false);
            tests::before_command(slot);
            if matches!(&work, Work::Command(Command::Append(_))) {
                tests::before_append(slot);
            }
        }
        let d = disk.as_mut().ok_or(OwnerError::Invalid)?;
        match work {
            Work::Initialize(initial, parent) => {
                match initial {
                    Initial::Process(path, limits) => {
                        let spool = Spool::new(path, limits).map_err(|_| OwnerError::Invalid)?;
                        #[cfg(test)]
                        tests::after_create(slot);
                        d.spool = Some(spool);
                    }
                    Initial::Operation(limits) => {
                        let p = parent.ok_or(OwnerError::Invalid)?;
                        let pd = p.disk.lock().map_err(|_| OwnerError::Invalid)?;
                        d.operation = Some(Arc::new(
                            pd.as_ref()
                                .and_then(|v| v.spool.as_ref())
                                .ok_or(OwnerError::Invalid)?
                                .operation(limits),
                        ));
                    }
                    Initial::Control => return Err(OwnerError::Invalid),
                    Initial::Stream => {
                        let p = parent.ok_or(OwnerError::Invalid)?;
                        let pd = p.disk.lock().map_err(|_| OwnerError::Invalid)?;
                        d.operation = Some(
                            pd.as_ref()
                                .and_then(|v| v.operation.as_ref())
                                .ok_or(OwnerError::Invalid)?
                                .clone(),
                        );
                    }
                }
                Ok(Reply::Ready)
            }
            Work::Command(c) => {
                if slot.kind != Kind::Stream {
                    return Err(OwnerError::Invalid);
                }
                if slot.cancelled() {
                    return Err(OwnerError::Cancelled);
                }
                match c {
                    Command::ExecuteCaptured(storage) => {
                        let Processor::AcceptedRequest(job) =
                            std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            return Err(OwnerError::Invalid);
                        };
                        let mut output = LocalOutput {
                            slot,
                            disk: d,
                            expected: &job,
                        };
                        let result =
                            match crate::engine::run_captured(runtime, &storage, &job, &mut output)
                            {
                                Ok(result) => result,
                                Err(error) => output
                                    .accept(
                                        &job,
                                        JobResult::Error {
                                            code: error.code,
                                            message: error.message,
                                        },
                                        Vec::new(),
                                    )
                                    .map_err(|_| OwnerError::Invalid)?,
                            };
                        Ok(Reply::Accepted(Box::new(result)))
                    }
                    Command::ExecutePrepared(input) => {
                        let Processor::AcceptedRequest(job) =
                            std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            return Err(OwnerError::Invalid);
                        };
                        let mut output = LocalOutput {
                            slot,
                            disk: d,
                            expected: &job,
                        };
                        let result = match crate::engine::run_prepared(&job, input, &mut output) {
                            Ok(result) => result,
                            Err(error) => output
                                .accept(
                                    &job,
                                    JobResult::Error {
                                        code: error.code,
                                        message: error.message,
                                    },
                                    Vec::new(),
                                )
                                .map_err(|_| OwnerError::Invalid)?,
                        };
                        Ok(Reply::Accepted(Box::new(result)))
                    }
                    Command::StagePrepared {
                        table,
                        commit,
                        ordinal,
                    } => {
                        let Processor::AcceptedPrepared(result) = &d.processor else {
                            return Err(OwnerError::Invalid);
                        };
                        let parts = result
                            .parts()
                            .map(|parts| {
                                parts
                                    .iter()
                                    .enumerate()
                                    .map(|(i, part)| {
                                        let n = ordinal
                                            .checked_add(
                                                u32::try_from(i)
                                                    .map_err(|_| OwnerError::Invalid)?,
                                            )
                                            .ok_or(OwnerError::Invalid)?;
                                        let m = part.metadata();
                                        Ok(crate::storage::PartitionFile {
                                            object: crate::storage::ObjectKey::partition(
                                                table, commit, n,
                                            )
                                            .as_str()
                                            .into(),
                                            range: m.range.clone(),
                                            row_count: m.row_count,
                                            byte_size: m.byte_size,
                                            payload_bytes: m.payload_bytes,
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()
                            })
                            .transpose()?;
                        if d.uploads.len() >= super::job::MAX_PREPARATION_ROWS {
                            return Err(OwnerError::Refused);
                        }
                        let Processor::AcceptedPrepared(result) =
                            std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            unreachable!()
                        };
                        d.uploads.push((table, commit, ordinal, result));
                        Ok(Reply::Parts(parts))
                    }
                    Command::UploadStaged(storage) => {
                        for (table, commit, ordinal, result) in std::mem::take(&mut d.uploads) {
                            let (_, parts) = result.into_parts();
                            for (i, part) in parts.unwrap_or_default().into_iter().enumerate() {
                                let n = ordinal
                                    .checked_add(u32::try_from(i).map_err(|_| OwnerError::Invalid)?)
                                    .ok_or(OwnerError::Invalid)?;
                                let key = crate::storage::ObjectKey::partition(table, commit, n);
                                let (meta, mut reader) =
                                    part.into_reader().map_err(|_| OwnerError::Invalid)?;
                                let mut bytes = Vec::with_capacity(meta.byte_size as usize);
                                reader
                                    .read_to_end(&mut bytes)
                                    .map_err(|_| OwnerError::Invalid)?;
                                let deadline =
                                    nabedra_rt::time::now() + std::time::Duration::from_secs(30);
                                let wrote = runtime.block_on(async {
                                    nabedra_rt::time::timeout_at(
                                        deadline,
                                        storage.put(
                                            &key,
                                            bytes.clone(),
                                            crate::storage::WriteCondition::Create,
                                        ),
                                    )
                                    .await
                                });
                                if !matches!(wrote, Ok(Ok(ref token)) if !token.is_empty()) {
                                    let confirmed = runtime.block_on(async {
                                        nabedra_rt::time::timeout_at(
                                            deadline,
                                            storage.read(&key, meta.byte_size as usize),
                                        )
                                        .await
                                    });
                                    if !matches!(confirmed, Ok(Ok(object)) if object.bytes == bytes && !object.etag.is_empty())
                                    {
                                        return Err(OwnerError::Invalid);
                                    }
                                }
                            }
                        }
                        Ok(Reply::Ready)
                    }
                    Command::StartSend { job, request } => {
                        let mut sending = Sending {
                            reader: None,
                            parts: Vec::new().into_iter(),
                            job,
                        };
                        let (bytes, kind) = if request {
                            if !matches!(
                                d.processor,
                                Processor::Empty | Processor::AcceptedRequest(_)
                            ) {
                                return Err(OwnerError::Invalid);
                            }
                            (
                                encode_job(&sending.job).map_err(|_| OwnerError::Invalid)?,
                                super::payload_stream::ReceiveKind::Control,
                            )
                        } else {
                            let old = std::mem::replace(&mut d.processor, Processor::Empty);
                            match old {
                                Processor::Accepted(rows, result) => {
                                    let bytes = encode_result(&result, &sending.job)
                                        .map_err(|_| OwnerError::Invalid)?;
                                    let payload = encode_rows(
                                        d.operation.as_ref().ok_or(OwnerError::Invalid)?,
                                        &rows,
                                        &result,
                                        &sending.job,
                                    )
                                    .map_err(|_| OwnerError::Invalid)?;
                                    sending.reader =
                                        Some(payload.open().map_err(|_| OwnerError::Invalid)?);
                                    (bytes, super::payload_stream::ReceiveKind::Rows)
                                }
                                Processor::AcceptedControl(result) => (
                                    encode_result(&result, &sending.job)
                                        .map_err(|_| OwnerError::Invalid)?,
                                    super::payload_stream::ReceiveKind::Control,
                                ),
                                Processor::AcceptedPrepared(result) => {
                                    let (envelope, parts) = result.into_parts();
                                    let bytes = encode_result(&envelope, &sending.job)
                                        .map_err(|_| OwnerError::Invalid)?;
                                    let mut validator =
                                        PreparationValidator::new(&envelope, &sending.job)
                                            .map_err(|_| OwnerError::Invalid)?;
                                    for part in parts.iter().flatten() {
                                        validator
                                            .push(part.metadata())
                                            .map_err(|_| OwnerError::Invalid)?;
                                    }
                                    validator.finish().map_err(|_| OwnerError::Invalid)?;
                                    let count = parts.as_ref().map_or(0, |p| p.len() as u32);
                                    sending.parts = parts.unwrap_or_default().into_iter();
                                    (bytes, super::payload_stream::ReceiveKind::Prepared(count))
                                }
                                _ => return Err(OwnerError::Invalid),
                            }
                        };
                        d.processor = Processor::Sending(Box::new(sending));
                        Ok(Reply::Sending(bytes, kind))
                    }
                    Command::NextPart => {
                        #[cfg(test)]
                        let operation = d.operation.as_ref().unwrap().clone();
                        let Processor::Sending(sending) = &mut d.processor else {
                            return Err(OwnerError::Invalid);
                        };
                        let part = sending.parts.next().ok_or(OwnerError::Invalid)?;
                        drop(sending.reader.take());
                        #[cfg(test)]
                        tests::before_part_open(slot, &operation);
                        let (meta, reader) = part.into_reader().map_err(|_| OwnerError::Invalid)?;
                        sending.reader = Some(reader);
                        Ok(Reply::Bytes(
                            encode_part_meta(&meta, &sending.job.table)
                                .map_err(|_| OwnerError::Invalid)?,
                        ))
                    }
                    Command::ReadNext => {
                        #[cfg(test)]
                        tests::before_read(slot);
                        let Processor::Sending(sending) = &mut d.processor else {
                            return Err(OwnerError::Invalid);
                        };
                        let reader = sending.reader.as_mut().ok_or(OwnerError::Invalid)?;
                        let mut bytes = vec![0; MAX_DATA_BYTES];
                        let n = reader.read(&mut bytes).map_err(|_| OwnerError::Invalid)?;
                        bytes.truncate(n);
                        Ok(Reply::Bytes(bytes))
                    }
                    Command::BeginCheckedRequest(job) => {
                        if !matches!(d.processor, Processor::Empty) {
                            return Err(OwnerError::Invalid);
                        }
                        d.processor = Processor::PendingRequest(job.0);
                        Ok(Reply::Ready)
                    }
                    Command::BeginRequest(bytes) => {
                        if !matches!(d.processor, Processor::Empty) {
                            return Err(OwnerError::Invalid);
                        }
                        let job = parse_job(&bytes).map_err(|_| OwnerError::Invalid)?;
                        d.processor = Processor::PendingRequest(Box::new(job));
                        Ok(Reply::Ready)
                    }
                    Command::BeginResult { bytes, expected } => {
                        if !matches!(d.processor, Processor::Empty) {
                            return Err(OwnerError::Invalid);
                        }
                        let result = Box::new(
                            parse_result(&bytes, &expected).map_err(|_| OwnerError::Invalid)?,
                        );
                        begin_result(d, result, expected)
                    }
                    Command::BeginCheckedResult { result, expected } => {
                        if !matches!(d.processor, Processor::Empty) {
                            return Err(OwnerError::Invalid);
                        }
                        begin_result(d, result, expected)
                    }
                    Command::BeginPart(bytes) => {
                        let Processor::Prepared { job, receiver } = &mut d.processor else {
                            return Err(OwnerError::Invalid);
                        };
                        let meta =
                            parse_part_meta(&bytes, &job.table).map_err(|_| OwnerError::Invalid)?;
                        receiver.begin_part(meta).map_err(|_| OwnerError::Invalid)?;
                        Ok(Reply::Ready)
                    }
                    Command::ClosePart => {
                        let Processor::Prepared { receiver, .. } = &mut d.processor else {
                            return Err(OwnerError::Invalid);
                        };
                        receiver.end_part().map_err(|_| OwnerError::Invalid)?;
                        Ok(Reply::Ready)
                    }
                    Command::CompletePrepared => {
                        let Processor::Prepared { receiver, .. } =
                            std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            return Err(OwnerError::Invalid);
                        };
                        d.processor = Processor::PendingPrepared(
                            receiver.finish().map_err(|_| OwnerError::Invalid)?,
                        );
                        Ok(Reply::Ready)
                    }
                    Command::BeginRows { job, result } => {
                        if !matches!(d.processor, Processor::Empty) {
                            return Err(OwnerError::Invalid);
                        }
                        result.validate_for(&job).map_err(|_| OwnerError::Invalid)?;
                        let writer = d
                            .operation
                            .as_ref()
                            .ok_or(OwnerError::Invalid)?
                            .writer(MAX_INTERNAL_ROW_BYTES as u64)
                            .map_err(|_| OwnerError::Invalid)?;
                        d.processor = Processor::Rows {
                            job,
                            result,
                            writer,
                        };
                        Ok(Reply::Ready)
                    }
                    Command::Append(bytes) => {
                        if bytes.is_empty() || bytes.len() > MAX_DATA_BYTES {
                            return Err(OwnerError::Invalid);
                        }
                        if let Processor::Rows { writer, .. } = &mut d.processor {
                            writer.append(&bytes).map_err(|_| OwnerError::Invalid)?;
                            Ok(Reply::Ready)
                        } else if let Processor::Prepared { receiver, .. } = &mut d.processor {
                            receiver.append(&bytes).map_err(|_| OwnerError::Invalid)?;
                            Ok(Reply::Ready)
                        } else {
                            Err(OwnerError::Invalid)
                        }
                    }
                    Command::CloseRows => {
                        let Processor::Rows {
                            job,
                            result,
                            writer,
                        } = std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            return Err(OwnerError::Invalid);
                        };
                        let payload = writer.finish().map_err(|_| OwnerError::Invalid)?;
                        let rows = decode_rows(&payload, &result, &job)
                            .map_err(|_| OwnerError::Invalid)?;
                        d.processor = Processor::Pending(rows, result);
                        Ok(Reply::Ready)
                    }
                    Command::CommitEof => {
                        let handoff = slot.handoff.lock().unwrap();
                        if handoff.cancelled
                            || slot.deadline.is_some_and(|d| nabedra_rt::time::now() >= d)
                        {
                            return Err(OwnerError::Cancelled);
                        }
                        match std::mem::replace(&mut d.processor, Processor::Empty) {
                            Processor::Pending(rows, result) => {
                                let reply = result.clone();
                                d.processor = Processor::Accepted(rows, result);
                                Ok(Reply::Accepted(reply))
                            }
                            Processor::PendingRequest(job) => {
                                let reply = job.clone();
                                d.processor = Processor::AcceptedRequest(job);
                                Ok(Reply::RequestAccepted(reply))
                            }
                            Processor::PendingControl(result) => {
                                let reply = result.clone();
                                d.processor = Processor::AcceptedControl(result);
                                Ok(Reply::Accepted(reply))
                            }
                            Processor::PendingPrepared(result) => {
                                let reply = Box::new(result.envelope().clone());
                                d.processor = Processor::AcceptedPrepared(result);
                                Ok(Reply::Accepted(reply))
                            }
                            _ => Err(OwnerError::Invalid),
                        }
                    }
                    Command::ClearReceived => {
                        if !matches!(d.processor, Processor::AcceptedControl(_)) {
                            return Err(OwnerError::Invalid);
                        }
                        d.processor = Processor::Empty;
                        Ok(Reply::Ready)
                    }
                    Command::ClearStaged => {
                        d.uploads.clear();
                        Ok(Reply::Ready)
                    }
                    Command::TakeRows => {
                        let Processor::Accepted(rows, _) =
                            std::mem::replace(&mut d.processor, Processor::Empty)
                        else {
                            return Err(OwnerError::Invalid);
                        };
                        Ok(Reply::Rows(rows))
                    }
                }
            }
            Work::Discard => unreachable!(),
        }
    }));
    match result {
        Ok(result) => result,
        Err(_) => {
            drop(disk);
            slot.discard();
            Err(OwnerError::Invalid)
        }
    }
}
async fn supervise(
    pool: WorkerPool,
    slot: Arc<Slot>,
    parent: Option<Arc<Slot>>,
    initial: Initial,
    ready: oneshot::Sender<Result<Reply>>,
    mut rx: mpsc::Receiver<Request>,
    production: Option<LocalProduction>,
) {
    let deadline = slot.deadline;
    let local = production.is_some();
    let control = matches!(initial, Initial::Control);
    // Installed stream/account pairs share a process Spool. The stream waits for
    // the original account initialization command, never races its disk cell.
    let parent_ready = if slot.kind == Kind::Stream {
        if let Some(parent) = &parent {
            let mut done = parent.initialization_done.subscribe();
            loop {
                if let Some(ok) = *done.borrow() {
                    break ok;
                }
                if done.changed().await.is_err() {
                    break false;
                }
            }
        } else {
            false
        }
    } else {
        true
    };
    let initial = if !parent_ready {
        Err(OwnerError::Invalid)
    } else if let Some(production) = production {
        let result = production.0.await;
        {
            let _change = slot.evidence.begin(false).expect("settlement");
            slot.producing.store(false, Ordering::Release);
        }
        result
    } else if control {
        Ok(Reply::Ready)
    } else {
        run(&pool, &slot, Work::Initialize(initial, parent)).await
    };
    let initial = if local && slot.cancelled() && initial.is_ok() {
        Err(OwnerError::Cancelled)
    } else {
        initial
    };
    if initial.is_ok() {
        {
            let _change = slot.evidence.begin(false).expect("settlement");
            slot.initialized.store(true, Ordering::Release);
        }
    }
    slot.initialization_done.send_replace(Some(initial.is_ok()));
    let failed = initial.is_err();
    if ready.send(initial).is_err() || failed {
        slot.cancel();
    }
    #[cfg(test)]
    if let Some(tx) = slot.hooks.lock().unwrap().local_ready.take() {
        let _ = tx.send(());
    }
    let mut wake = slot.wake.subscribe();
    while !slot.cancelled() {
        let request = tokio::select! {
            _ = wake.changed() => None,
            _ = async { if let Some(d) = deadline { nabedra_rt::time::sleep_until(d).await } else { std::future::pending::<()>().await } } => { slot.cancel(); None },
            request = rx.recv() => request,
        };
        let Some(request) = request else {
            break;
        };
        let request = match request {
            Request::Send {
                writer,
                job,
                request,
                reply,
            } => {
                let result =
                    super::payload_stream::drive_send(writer, job, request, &pool, &slot).await;
                let result = if slot.cancelled() && result.is_ok() {
                    Err(OwnerError::Cancelled)
                } else {
                    result
                };
                let _ = reply.send(result);
                slot.cancel();
                break;
            }
            other => other,
        };
        let (reply, result) = match request {
            Request::Send { .. } => unreachable!(),
            Request::Command { command, reply } => (
                reply,
                if control {
                    Err(OwnerError::Invalid)
                } else {
                    run(&pool, &slot, Work::Command(command)).await
                },
            ),
            Request::Receive {
                reader,
                expected,
                checked,
                checked_reply,
                reply,
            } => (
                reply,
                super::payload_stream::drive_checked(
                    reader,
                    expected,
                    checked,
                    checked_reply.map(|reply| *reply),
                    &pool,
                    &slot,
                )
                .await,
            ),
        };
        let result = if slot.cancelled() && result.is_ok() {
            Err(OwnerError::Cancelled)
        } else {
            result
        };
        let failed = result.is_err();
        if reply.send(result).is_err() || failed {
            slot.cancel();
        }
    }
    // Parents cannot dispose before dependent entries have actually been reaped.
    loop {
        let changed = slot.child_done.notified();
        if *slot.children.lock().unwrap() == 0 {
            break;
        }
        changed.await;
    }
    loop {
        let changed = slot.delivery_done.notified();
        if *slot.delivery_holds.lock().unwrap() == 0 {
            break;
        }
        changed.await;
    }
    if !control && run(&pool, &slot, Work::Discard).await.is_err() {
        {
            let _change = slot.evidence.begin(false).expect("settlement");
            slot.failed.store(true, Ordering::Release);
        }
        return;
    }
    {
        let _change = slot.evidence.begin(false).expect("settlement");
        slot.cleanup.store(true, Ordering::Release);
    }
    #[cfg(test)]
    tests::before_return(&slot).await;
}
macro_rules! opaque { ($($t:ty),*) => { $(impl fmt::Debug for $t { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(stringify!($t)) } })* }; }
opaque!(PayloadOwners, PayloadLease, Command, Reply);
#[cfg(test)]
pub(super) mod tests;

#[cfg(test)]
mod local_tests;

// Only the typed partition prelude is erased here; commands remain the closed enum above.
struct LocalProduction(Pin<Box<dyn Future<Output = Result<Reply>> + Send>>);
impl PayloadOwners {
    pub(crate) fn try_bound_operation(
        &self,
        process: &PayloadLease,
        limits: SpoolLimits,
        lifetime: Arc<OwnedSemaphorePermit>,
        operation_id: uuid::Uuid,
    ) -> Result<PayloadLease> {
        let lease = self.try_operation(process, limits, lifetime)?;
        *lease.slot.operation_id.lock().unwrap() = Some(operation_id);
        Ok(lease)
    }
    /// Caller supplies configured context and already admitted execution/queue ownership.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_local<B: crate::storage::ObjectStore + 'static>(
        &self,
        operation: &PayloadLease,
        role: Role,
        lifetime: Arc<OwnedSemaphorePermit>,
        deadline: Option<Instant>,
        cluster_id: uuid::Uuid,
        job: Box<JobEnvelope>,
        storage: Arc<crate::storage::Storage<B>>,
        partition: WorkerPool,
        execution: OwnedSemaphorePermit,
        queue: OwnedSemaphorePermit,
    ) -> Result<PayloadLease> {
        if !self.shared.state.lock().unwrap().legacy_mode() {
            return Err(OwnerError::Refused);
        }
        if job.cluster_id != cluster_id
            || *operation.slot.operation_id.lock().unwrap() != Some(job.operation_id)
        {
            return Err(OwnerError::Invalid);
        }
        encode_job(&job).map_err(|_| OwnerError::Invalid)?;
        let capacity = match role {
            Role::Operation => &self.shared.operation_transport,
            Role::Dependency => &self.shared.dependency_transport,
        };
        self.register_with(
            Some(operation),
            Kind::Stream,
            Initial::Stream,
            Some(capacity),
            Some(lifetime),
            deadline,
            move |slot, parent| {
                Some(LocalProduction(Box::pin(async move {
                    let parent = parent.ok_or(OwnerError::Invalid)?;
                    let (input, charge) =
                        crate::engine::prepare_captured(&storage, &partition, &job)
                            .await
                            .map_err(|_| OwnerError::Invalid)?;
                    let request = job.scheduler_request_id();
                    let worker_slot = slot.clone();
                    let worker_job = job.clone();
                    // Keep this future and every accepted Job until settlement, even after cancellation.
                    let submitted = partition
                        .submit_buffered(
                            request,
                            1,
                            vec![charge],
                            partition.resources().task_memory_limit_bytes,
                            move |_, _| {
                                let _execution = execution;
                                Ok(produce(&worker_slot, &parent, &worker_job, input))
                            },
                        )
                        .await
                        .map_err(|_| OwnerError::Refused)?;
                    drop(queue);
                    let (reply, mut charge) =
                        submitted.receive().await.map_err(|_| OwnerError::Invalid)?;
                    let reply = reply?;
                    let retained = {
                        let disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
                        match &disk.as_ref().ok_or(OwnerError::Invalid)?.processor {
                            Processor::Accepted(rows, _) => {
                                let payload = rows.iter().map(crate::snapshot::row_payload).sum();
                                crate::snapshot::memory_shape(&job.table, rows.len(), payload, 0)
                                    .map_err(|_| OwnerError::Invalid)?
                            }
                            _ => 4096,
                        }
                    };
                    charge
                        .shrink_to(retained.min(charge.bytes()))
                        .map_err(|_| OwnerError::Invalid)?;
                    *slot.retained_memory.lock().unwrap() = Some(charge);
                    Ok(reply)
                })))
            },
        )
    }
}

/// A worker-only borrow. Its fields and construction are confined to this owner.
pub(crate) struct LocalOutput<'a> {
    slot: &'a Slot,
    disk: &'a mut Disk,
    expected: &'a JobEnvelope,
}
impl LocalOutput<'_> {
    pub(crate) fn check(&self, job: &JobEnvelope) -> std::result::Result<(), super::job::JobError> {
        if self.slot.cancelled() || job != self.expected {
            return Err(local_error());
        }
        Ok(())
    }
    pub(crate) fn prepared<'a>(
        &self,
        job: &'a JobEnvelope,
        effect: super::job::Effect,
        changed: bool,
    ) -> std::result::Result<super::payload::LocalPrepared<'a>, super::job::JobError> {
        self.check(job)?;
        super::payload::LocalPrepared::new(
            self.disk
                .operation
                .as_ref()
                .ok_or_else(local_error)?
                .clone(),
            job,
            effect,
            changed,
        )
    }
    pub(crate) fn accept(
        &mut self,
        job: &JobEnvelope,
        result: JobResult,
        rows: Vec<Row>,
    ) -> std::result::Result<ResultEnvelope, super::job::JobError> {
        self.check(job)?;
        let envelope = ResultEnvelope {
            version: job.version,
            cluster_id: job.cluster_id,
            operation_id: job.operation_id,
            job_id: job.job_id,
            attempt_id: job.attempt_id,
            request_digest: job.request_digest.clone(),
            snapshot: job.snapshot.clone(),
            ordinal: job.ordinal,
            result,
        };
        encode_result(&envelope, self.expected)?;
        if matches!(
            envelope.result,
            JobResult::Get { .. } | JobResult::Collect { .. }
        ) {
            super::payload::validate_rows(&rows, &envelope, job)?;
        }
        let handoff = self.slot.handoff.lock().unwrap();
        if handoff.cancelled {
            return Err(local_error());
        }
        self.disk.processor = if matches!(
            envelope.result,
            JobResult::Get { .. } | JobResult::Collect { .. }
        ) {
            Processor::Accepted(rows, Box::new(envelope.clone()))
        } else {
            Processor::AcceptedControl(Box::new(envelope.clone()))
        };
        Ok(envelope)
    }
    pub(crate) fn accept_prepared(
        &mut self,
        job: &JobEnvelope,
        prepared: PreparedResult,
    ) -> std::result::Result<ResultEnvelope, super::job::JobError> {
        self.check(job)?;
        let envelope = prepared.envelope().clone();
        encode_result(&envelope, self.expected)?;
        #[cfg(test)]
        local_tests::before_deposit(self.slot);
        let handoff = self.slot.handoff.lock().unwrap();
        if handoff.cancelled {
            return Err(local_error());
        }
        self.disk.processor = Processor::AcceptedPrepared(prepared);
        Ok(envelope)
    }
}
fn local_error() -> super::job::JobError {
    super::job::JobError {
        code: crate::protocol::ErrorCode::ValidationError,
        message: "local output rejected".into(),
    }
}
fn produce(
    slot: &Slot,
    parent: &Slot,
    job: &JobEnvelope,
    input: crate::engine::CapturedInput,
) -> Result<Reply> {
    let _change = slot.evidence.begin(false).expect("accepted production");
    let mut disk = slot.disk.lock().map_err(|_| OwnerError::Invalid)?;
    let result = catch_unwind(AssertUnwindSafe(|| {
        #[cfg(test)]
        tests::before_command(slot);
        if slot.cancelled() {
            return Err(OwnerError::Cancelled);
        }
        let d = disk.as_mut().ok_or(OwnerError::Invalid)?;
        let parent_disk = parent.disk.lock().map_err(|_| OwnerError::Invalid)?;
        d.operation = Some(
            parent_disk
                .as_ref()
                .and_then(|p| p.operation.as_ref())
                .ok_or(OwnerError::Invalid)?
                .clone(),
        );
        drop(parent_disk);
        let mut output = LocalOutput {
            slot,
            disk: d,
            expected: job,
        };
        let envelope = crate::engine::run_prepared(job, input, &mut output)
            .map_err(|_| OwnerError::Invalid)?;
        Ok(Reply::Accepted(Box::new(envelope)))
    }));
    match result {
        Ok(value) => value,
        Err(_) => {
            drop(disk);
            slot.discard();
            Err(OwnerError::Invalid)
        }
    }
}

#[cfg(test)]
mod preparation_tests;

#[cfg(test)]
mod installation_tests;

impl PayloadRegistry {
    fn reserve_preparation_locked(
        &mut self,
        guards: &mut Option<Admission>,
        control: bool,
        capacity: Option<&Arc<Semaphore>>,
    ) -> Result<u64> {
        self.evidence.begin(true).map_err(|_| OwnerError::Refused)?;
        if self.closed || self.failed {
            return Err(OwnerError::Refused);
        }
        let mut range = if control { 24..32 } else { 0..24 };
        let position = range
            .find(|i| self.preparations[*i].is_none())
            .ok_or(OwnerError::Refused)?;
        let id = self.next;
        let next = id.checked_add(1).ok_or(OwnerError::Refused)?;
        if let Some(capacity) = capacity {
            if !matches!(guards, Some(Admission::Legacy { .. })) {
                return Err(OwnerError::Refused);
            }
            let permit = capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| OwnerError::Refused)?;
            match guards.as_mut().unwrap() {
                Admission::Legacy { permit: held, .. } => *held = Some(permit),
                Admission::Distributed { .. } => return Err(OwnerError::Refused),
            }
        }
        self.next = next;
        self.preparations[position] = Some(Preparation {
            id,
            cancelled: false,
            guards: Some(ManuallyDrop::new(guards.take().unwrap())),
            entry: None,
        });
        Ok(id)
    }
}

impl PayloadRegistry {
    pub(super) fn bounded(local: &super::admission::LocalAdmission) -> Self {
        let (o, p) = local.owner_bounds();
        let bound = 1 + o + 4096 + 48;
        Self {
            evidence: local.evidence(),
            proof: Default::default(),
            closed: false,
            failed: false,
            process_used: false,
            next: 0,
            entries: Vec::with_capacity(bound),
            accepted_limit: bound,
            operation_bound: o,
            operation_stream_bound: 16,
            dependency_stream_bound: 16,
            outgoing_bound: 16,
            positions: vec![None; bound],
            records: (0..o + p + 64).map(|_| None).collect(),
            controls: Vec::with_capacity(64),
            boot_reserved: None,
            pending_releases: 0,
            rollback_notifications: Vec::with_capacity(32),
            preparations: std::array::from_fn(|_| None),
        }
    }
    pub(super) fn bounds(&self) -> (usize, usize) {
        (self.positions.len(), self.records.len())
    }
}
#[derive(Clone, Copy)]
enum AcceptedOwner {
    Work(super::admission::WorkKey),
    Control(u64),
}

/// Only a settled worker initialization of the retained boot entry issues this.
pub(super) struct ProcessReady {
    boot: Arc<super::auth::ReceiverIdentity>,
    initialization: uuid::Uuid,
    entry: u64,
}
impl ProcessReady {
    pub(super) fn matches_initialization(
        &self,
        configured: &super::initialization::InitializedWorker,
    ) -> bool {
        configured.matches(&self.boot, self.initialization)
    }
    pub(super) fn check_locked(&self, state: &super::admission::ReceiverState) -> bool {
        state
            .local()
            .is_some_and(|local| local.initialization_matches(&self.boot, self.initialization))
            && state.payload.boot_reserved == Some(self.entry)
            && state.payload.entries.iter().any(|e| {
                e.slot.id == self.entry
                    && e.slot.kind == Kind::Process
                    && e.slot.initialized.load(Ordering::Acquire)
                    && !e.slot.cancelled()
            })
    }
}
#[allow(dead_code)] // R07 boot construction and R04c typed control transactions.
impl PayloadOwners {
    pub(super) fn from_receiver(
        pool: WorkerPool,
        state: Arc<Mutex<super::admission::ReceiverState>>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                state,
                pool,
                reap_wake: Arc::new(ReapWake(Arc::new(Notify::new()))),
                operations: Arc::new(Semaphore::new(0)),
                operation_transport: Arc::new(Semaphore::new(0)),
                dependency_transport: Arc::new(Semaphore::new(0)),
                outgoing: Arc::new(Semaphore::new(16)),
                outgoing_limit: 16,
                #[cfg(test)]
                releasing: Mutex::new(None),
                #[cfg(test)]
                pair_preparing: Mutex::new(None),
            }),
        }
    }
    pub(super) fn from_receiver_with_streams(
        pool: WorkerPool,
        state: Arc<Mutex<super::admission::ReceiverState>>,
        outgoing: usize,
    ) -> Self {
        let mut owners = Self::from_receiver(pool, state);
        let shared = Arc::get_mut(&mut owners.shared).expect("new owner registry");
        shared.outgoing = Arc::new(Semaphore::new(outgoing));
        shared.outgoing_limit = outgoing;
        {
            let mut state = shared.state.lock().unwrap();
            let (o, p) = state.local().expect("distributed receiver").owner_bounds();
            let registry = &mut state.payload;
            registry.operation_stream_bound = o.min(256);
            registry.dependency_stream_bound = p.min(1024);
            registry.outgoing_bound = outgoing;
            let bound = 1
                + registry.operation_bound
                + 4096
                + registry.operation_stream_bound
                + registry.dependency_stream_bound
                + outgoing;
            registry.positions.resize(bound, None);
            registry.accepted_limit = bound;
            registry
                .entries
                .reserve(bound.saturating_sub(registry.entries.capacity()));
        }
        owners
    }
    pub(crate) fn outgoing_capacity(&self) -> usize {
        self.shared.outgoing_limit
    }
    pub(super) fn start_boot(
        &self,
        boot: &super::auth::ReceiverIdentity,
        initialization: uuid::Uuid,
        path: PathBuf,
        limits: SpoolLimits,
    ) -> Result<PayloadLease> {
        let mut guards = Some(Admission::distributed(
            DistributedOwner::Boot(initialization),
            None,
        ));
        let id = {
            let mut state = self.shared.state.lock().unwrap();
            if !state
                .local()
                .is_some_and(|l| l.initialization_matches(boot, initialization))
            {
                return Err(OwnerError::Refused);
            }
            let r = &mut state.payload;
            if r.boot_reserved.is_some() || r.positions[0].is_some() {
                return Err(OwnerError::Refused);
            }
            let id = r.reserve_preparation_locked(&mut guards, false, None)?;
            r.boot_reserved = Some(id);
            r.positions[0] = Some(id);
            id
        };
        let mut prepared = self.spawn_preparation(id, Kind::Process, None)?;
        prepared
            .slot
            .receiver_limits
            .set(limits)
            .expect("new process limits");
        let mut state = self.shared.state.lock().unwrap();
        if !state
            .local()
            .is_some_and(|l| l.initialization_matches(boot, initialization))
        {
            return Err(OwnerError::Refused);
        }
        let reservation =
            state
                .payload
                .reserve_entry_locked(&mut prepared, None, Kind::Process, &self.shared)?;
        Ok(reservation.install_entry_locked(Initial::Process(path, limits), None))
    }
}
impl PayloadLease {
    #[allow(dead_code)] // R07 consumes this along with configured storage/catalog completion.
    pub(super) async fn process_ready(&mut self) -> Result<ProcessReady> {
        if !matches!(self.ready().await?, Reply::Ready) {
            return Err(OwnerError::Invalid);
        }
        let state = self.shared.state.lock().unwrap();
        let local = state.local().ok_or(OwnerError::Refused)?;
        let entry = state
            .payload
            .entries
            .iter()
            .find(|e| e.slot.id == self.slot.id)
            .ok_or(OwnerError::Refused)?;
        let Some(DistributedOwner::Boot(initialization)) = entry.guards.owner() else {
            return Err(OwnerError::Refused);
        };
        let receipt = ProcessReady {
            boot: local.boot(),
            initialization,
            entry: self.slot.id,
        };
        if !receipt.check_locked(&state) {
            return Err(OwnerError::Refused);
        }
        Ok(receipt)
    }
}

#[allow(dead_code)] // R04c consumes this reservation only after its typed local/replay checks.
impl PayloadRegistry {
    pub(super) fn reserve_control_locked<'a, 'p>(
        &'a mut self,
        prepared: &'p mut PreparedEntry,
    ) -> Result<EntryReservation<'a, 'p>> {
        if !matches!(prepared.slot.kind, Kind::Control) {
            return Err(OwnerError::Refused);
        }
        let position = self.prepared_position(prepared)?;
        if !matches!(
            self.preparations[position]
                .as_ref()
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .guards
                .owner(),
            Some(DistributedOwner::Control)
        ) {
            return Err(OwnerError::Refused);
        }
        let shared = prepared._shared.clone();
        self.reserve_entry_locked(prepared, None, Kind::Control, &shared)
    }
}
impl EntryReservation<'_, '_> {
    #[allow(dead_code)] // Typed memory-only commit used by R04c.
    pub(super) fn install_control_locked(self) -> ControlLease {
        ControlLease(self.install_entry_locked(Initial::Control, None))
    }
}
pub(super) struct ControlLease(PayloadLease);
#[allow(dead_code)] // Response delivery lifetime belongs to R04c's typed route owner.
impl ControlLease {
    pub(super) async fn ready(&mut self) -> Result<()> {
        self.0.ready().await.map(|_| ())
    }
    pub(super) async fn finish(self) -> Result<()> {
        self.0.dispose().await
    }
    pub(super) fn retain_response(&self) -> Result<DeliveryHold> {
        self.0.retain_delivery()
    }
}
/// A concrete outstanding delivery/publication obligation. It owns no authority.
// The second field identifies the preclaimed response, never an extra delivery.
pub(super) struct DeliveryHold(Arc<Slot>, bool);
impl Drop for DeliveryHold {
    fn drop(&mut self) {
        let _change = self.0.evidence.begin(false).expect("settlement");
        let mut holds = self.0.delivery_holds.lock().unwrap();
        *holds -= 1;
        if self.1 {
            self.0.response_live.store(false, Ordering::Release);
        }
        drop(holds);
        self.0.delivery_done.notify_one();
    }
}
impl PayloadLease {
    #[allow(dead_code)] // Retained coordinator/publication completion for R04c.
    pub(super) fn retain_delivery(&self) -> Result<DeliveryHold> {
        let _change = self
            .slot
            .evidence
            .begin(true)
            .map_err(|_| OwnerError::Refused)?;
        let handoff = self.slot.handoff.lock().unwrap();
        if handoff.cancelled {
            return Err(OwnerError::Cancelled);
        }
        let mut held = self.slot.delivery_holds.lock().unwrap();
        *held = held.checked_add(1).ok_or(OwnerError::Refused)?;
        Ok(DeliveryHold(self.slot.clone(), false))
    }
}

pub(super) struct PreparedPair {
    account: PreparedEntry,
    stream: PreparedEntry,
    role: Role,
}
pub(super) struct InstalledPair {
    pub(super) account: Option<PayloadLease>,
    pub(super) stream: PayloadLease,
}
enum AccountPlan {
    New { preparation: usize, position: usize },
    Existing(usize),
}
pub(super) struct PairReservation<'a, 'p> {
    registry: &'a mut PayloadRegistry,
    local: &'a mut super::admission::LocalAdmission,
    pair: &'p mut PreparedPair,
    account: AccountPlan,
    stream_preparation: usize,
    stream_position: usize,
    record: usize,
    key: super::admission::WorkKey,
    ticket: Option<super::admission::WorkTicket>,
    dependency: super::admission::DependencyKey,
    retained_use: Option<super::admission::DependencyUse>,
    operation: uuid::Uuid,
    generation: u64,
    process_hold: Option<Arc<Slot>>,
    account_hold: Option<Arc<Slot>>,
    limits: SpoolLimits,
    installed: bool,
}
impl Drop for PairReservation<'_, '_> {
    fn drop(&mut self) {
        if self.installed {
            return;
        }
        if let AccountPlan::New { position, .. } = self.account {
            self.registry.positions[position] = None;
        }
        self.registry.positions[self.stream_position] = None;
        self.registry.records[self.record] = None;
        self.local.rollback_work(
            self.ticket.take().unwrap(),
            self.generation,
            matches!(self.account, AccountPlan::New { .. })
                && matches!(self.pair.role, Role::Dependency),
        );
        for parent in [&mut self.process_hold, &mut self.account_hold] {
            if let Some(parent) = parent.take() {
                self.registry.rollback_parent_locked(parent);
            }
        }
    }
}
#[allow(dead_code)] // R04c prepares before locking/consuming replay.
impl PayloadOwners {
    pub(super) fn prepare_pair(
        &self,
        role: Role,
        transport: OwnedSemaphorePermit,
        deadline: Option<Instant>,
    ) -> Result<PreparedPair> {
        tokio::runtime::Handle::try_current().map_err(|_| OwnerError::Refused)?;
        let transport = Arc::new(transport);
        let guards = [
            Admission::distributed(DistributedOwner::Prepared, Some(transport.clone())),
            Admission::distributed(DistributedOwner::Prepared, Some(transport)),
        ];
        reap_shared(&self.shared, None);
        let ids = {
            let mut state = self.shared.state.lock().unwrap();
            if state.legacy_mode() {
                return Err(OwnerError::Refused);
            }
            let r = &mut state.payload;
            if r.closed || r.failed {
                return Err(OwnerError::Refused);
            }
            let mut free = (0..24).filter(|i| r.preparations[*i].is_none());
            let a = free.next().ok_or(OwnerError::Refused)?;
            let b = free.next().ok_or(OwnerError::Refused)?;
            let next = r.next.checked_add(2).ok_or(OwnerError::Refused)?;
            let _change = r.evidence.begin(true).map_err(|_| OwnerError::Refused)?;
            let ids = [r.next, r.next + 1];
            r.next = next;
            for ((position, id), guards) in [a, b].into_iter().zip(ids).zip(guards) {
                r.preparations[position] = Some(Preparation {
                    id,
                    cancelled: false,
                    guards: Some(ManuallyDrop::new(guards)),
                    entry: None,
                });
            }
            ids
        };
        #[cfg(test)]
        if let Some((reserved, gate)) = self.shared.pair_preparing.lock().unwrap().take() {
            let _ = reserved.send(());
            let _ = gate.recv_timeout(std::time::Duration::from_secs(10));
        }
        let account = self.spawn_preparation(ids[0], Kind::Operation, None)?;
        let stream = self.spawn_preparation(ids[1], Kind::Stream, deadline)?;
        Ok(PreparedPair {
            account,
            stream,
            role,
        })
    }
}
impl PayloadRegistry {
    fn prepared_position(&self, prepared: &PreparedEntry) -> Result<usize> {
        if prepared.slot.cancelled() {
            return Err(OwnerError::Refused);
        }
        self.preparations
            .iter()
            .position(|p| {
                p.as_ref().is_some_and(|p| {
                    !p.cancelled
                        && p.id == prepared.slot.id
                        && p.entry.as_ref().is_some_and(|e| {
                            Arc::ptr_eq(&e.slot, &prepared.slot) && e.guards.has_transport()
                        })
                })
            })
            .ok_or(OwnerError::Refused)
    }
    fn free_position(&self, range: std::ops::Range<usize>) -> Result<usize> {
        range
            .into_iter()
            .find(|p| self.positions[*p].is_none())
            .ok_or(OwnerError::Refused)
    }
    #[allow(clippy::too_many_arguments, dead_code)] // R04c's closed typed operation/job transaction.
    pub(super) fn reserve_pair_locked<'a, 'p>(
        &'a mut self,
        local: &'a mut super::admission::LocalAdmission,
        pair: &'p mut PreparedPair,
        process: &PayloadLease,
        binding: super::admission::Binding,
        permit: uuid::Uuid,
        job: Option<(uuid::Uuid, uuid::Uuid)>,
        limits: SpoolLimits,
    ) -> Result<PairReservation<'a, 'p>> {
        self.evidence.begin(true).map_err(|_| OwnerError::Refused)?;
        use super::admission::LocalAdmission;
        if self.closed
            || self.failed
            || pair.account.slot.kind != Kind::Operation
            || pair.stream.slot.kind != Kind::Stream
            || self.rollback_notifications.len() > 30
            || process.slot.kind != Kind::Process
            || process.slot.cancelled()
            || !process.slot.initialized.load(Ordering::Acquire)
            || !Arc::ptr_eq(&process.shared, &pair.stream._shared)
            || !Arc::ptr_eq(&pair.account._shared, &pair.stream._shared)
            || !self
                .entries
                .iter()
                .any(|e| Arc::ptr_eq(&e.slot, &process.slot))
        {
            return Err(OwnerError::Refused);
        }
        let stream_preparation = self.prepared_position(&pair.stream)?;
        let account_preparation = self.prepared_position(&pair.account)?;
        let same_transport = {
            let a = &self.preparations[account_preparation]
                .as_ref()
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .guards;
            let b = &self.preparations[stream_preparation]
                .as_ref()
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .guards;
            a.same_transport(b)
        };
        if !same_transport {
            return Err(OwnerError::Refused);
        }
        let record = self
            .records
            .iter()
            .position(Option::is_none)
            .ok_or(OwnerError::Refused)?;
        let end_accounts = 1 + self.operation_bound;
        let end_dependencies = end_accounts + 4096;
        let stream_range = match pair.role {
            Role::Operation => end_dependencies..end_dependencies + self.operation_stream_bound,
            Role::Dependency => {
                end_dependencies + self.operation_stream_bound
                    ..end_dependencies + self.operation_stream_bound + self.dependency_stream_bound
            }
        };
        let stream_position = self.free_position(stream_range)?;
        let existing = if matches!(pair.role, Role::Dependency) {
            let dep = local
                .dependency_key(&binding, permit)
                .map_err(|_| OwnerError::Refused)?;
            self.entries.iter().position(
                |e| matches!(e.guards.owner(),Some(DistributedOwner::Dependency(key)) if key==dep),
            )
        } else {
            None
        };
        let account = if let Some(i) = existing {
            if self.entries[i].slot.cancelled() {
                return Err(OwnerError::Refused);
            }
            AccountPlan::Existing(i)
        } else {
            let range = if matches!(pair.role, Role::Operation) {
                1..end_accounts
            } else {
                end_accounts..end_dependencies
            };
            AccountPlan::New {
                preparation: account_preparation,
                position: self.free_position(range)?,
            }
        };
        if !matches!(
            (pair.role, job),
            (Role::Operation, None) | (Role::Dependency, Some(_))
        ) {
            return Err(OwnerError::Refused);
        }
        let parent = match account {
            AccountPlan::New { .. } => pair.account.slot.clone(),
            AccountPlan::Existing(i) => self.entries[i].slot.clone(),
        };
        let process_hold = if matches!(account, AccountPlan::New { .. }) {
            Some(self.claim_parent_locked(&process.slot)?)
        } else {
            None
        };
        let account_hold = match self.claim_parent_locked(&parent) {
            Ok(hold) => Some(hold),
            Err(error) => {
                if let Some(hold) = process_hold {
                    self.rollback_parent_locked(hold);
                }
                return Err(error);
            }
        };
        let generation = local.generation();
        let operation = binding.operation_id();
        let ticket = match (pair.role, job) {
            (Role::Operation, None) => {
                local.reserve_operation(binding.clone(), binding.admission_id(), permit)
            }
            (Role::Dependency, Some((job, attempt))) => {
                local.reserve_partition(binding, permit, job, attempt)
            }
            _ => return Err(OwnerError::Refused),
        };
        let ticket = match ticket {
            Ok(ticket) => ticket,
            Err(_) => {
                for hold in [process_hold, account_hold].into_iter().flatten() {
                    self.rollback_parent_locked(hold);
                }
                return Err(OwnerError::Refused);
            }
        };
        let key = LocalAdmission::work_key(&ticket);
        let dependency = local.work_dependency(key).expect("newly reserved work");
        let retained_use = Some(
            local
                .retain_payload_use(key)
                .expect("newly reserved dependency use"),
        );
        if matches!(pair.role, Role::Dependency)
            && matches!(account, AccountPlan::New { .. })
            && local.hold_account(dependency).is_err()
        {
            local.rollback_work(ticket, generation, false);
            for hold in [process_hold, account_hold].into_iter().flatten() {
                self.rollback_parent_locked(hold);
            }
            return Err(OwnerError::Refused);
        }
        if let AccountPlan::New { position, .. } = account {
            self.positions[position] = Some(pair.account.slot.id);
        }
        self.positions[stream_position] = Some(pair.stream.slot.id);
        self.records[record] = Some(AcceptedOwner::Work(key));
        Ok(PairReservation {
            registry: self,
            local,
            pair,
            account,
            stream_preparation,
            stream_position,
            record,
            key,
            ticket: Some(ticket),
            dependency,
            retained_use,
            operation,
            generation,
            process_hold,
            account_hold,
            limits,
            installed: false,
        })
    }
}
impl PairReservation<'_, '_> {
    #[allow(dead_code)] // Infallible commit after R04c consumes replay in this same transaction.
    pub(super) fn install_entry_locked(mut self) -> InstalledPair {
        self.registry.evidence.record();
        let mut account_lease = None;
        if let AccountPlan::New {
            preparation,
            position,
        } = self.account
        {
            let mut entry = self.registry.preparations[preparation]
                .take()
                .unwrap()
                .entry
                .unwrap();
            entry.position = Some(position);
            *entry.slot.operation_id.lock().unwrap() = Some(self.operation);
            let operation = matches!(self.pair.role, Role::Operation);
            entry.guards.set_owner(if operation {
                DistributedOwner::Work(self.key)
            } else {
                DistributedOwner::Dependency(self.dependency)
            });
            entry
                .guards
                .set_parent(self.process_hold.take().map(ParentHold));
            if operation {
                entry
                    .guards
                    .retain_work_use(self.retained_use.take().unwrap());
                entry.record = Some(self.record);
            } else {
                entry.account_tx = Some(self.pair.account.tx.clone());
                entry.account_ready = self.pair.account.initial.take();
            }
            self.registry.entries.push(entry);
            let parent = self.registry.entries.last().unwrap().guards.parent_slot();
            activate_registered(
                self.registry,
                &mut self.pair.account,
                Initial::Operation(self.limits),
                parent,
            );
            if operation {
                account_lease = Some(lease_from_prepared(&mut self.pair.account));
            }
        }
        let mut stream = self.registry.preparations[self.stream_preparation]
            .take()
            .unwrap()
            .entry
            .unwrap();
        stream.position = Some(self.stream_position);
        *stream.slot.operation_id.lock().unwrap() = Some(self.operation);
        if matches!(self.pair.role, Role::Dependency) {
            stream.record = Some(self.record);
            stream.guards.set_owner(DistributedOwner::Work(self.key));
            stream
                .guards
                .retain_work_use(self.retained_use.take().unwrap());
        } else {
            stream.guards.set_owner(DistributedOwner::Stream);
        }
        let parent = self.account_hold.take().unwrap();
        stream.guards.set_parent(Some(ParentHold(parent.clone())));
        self.registry.entries.push(stream);
        activate_registered(
            self.registry,
            &mut self.pair.stream,
            Initial::Stream,
            Some(parent),
        );
        self.installed = true;
        InstalledPair {
            account: account_lease,
            stream: lease_from_prepared(&mut self.pair.stream),
        }
    }
}
fn activate_registered(
    registry: &mut PayloadRegistry,
    prepared: &mut PreparedEntry,
    initial: Initial,
    parent: Option<Arc<Slot>>,
) {
    if prepared
        .activate
        .take()
        .unwrap()
        .send(Activation {
            parent,
            initial,
            production: None,
        })
        .is_err()
    {
        {
            let _change = prepared.slot.evidence.begin(false).expect("settlement");
            prepared.slot.failed.store(true, Ordering::Release);
        }
        registry.failed = true;
    }
}
fn lease_from_prepared(prepared: &mut PreparedEntry) -> PayloadLease {
    PayloadLease {
        slot: prepared.slot.clone(),
        shared: prepared._shared.clone(),
        tx: prepared.tx.clone(),
        initial: prepared.initial.take(),
    }
}
impl Admission {
    fn parent_slot(&self) -> Option<Arc<Slot>> {
        match self {
            Self::Legacy { parent, .. } | Self::Distributed { parent, .. } => {
                parent.as_ref().map(|p| p.0.clone())
            }
        }
    }
    fn has_transport(&self) -> bool {
        match self {
            Self::Distributed {
                transport: Some(t), ..
            } => t.num_permits() == 1,
            _ => false,
        }
    }
    fn same_transport(&self, other: &Self) -> bool {
        matches!((self,other),(Self::Distributed{transport:Some(a),..},Self::Distributed{transport:Some(b),..}) if Arc::ptr_eq(a,b))
    }
}
impl PayloadRegistry {
    pub(super) fn cancel_dependency_locked(&mut self, key: super::admission::DependencyKey) {
        for e in &self.entries {
            if matches!(e.guards.owner(),Some(DistributedOwner::Dependency(k)) if k==key) {
                e.slot.cancel();
            }
        }
    }
}
impl PayloadRegistry {
    fn claim_parent_locked(&mut self, parent: &Arc<Slot>) -> Result<Arc<Slot>> {
        let _change = self.evidence.begin(true).map_err(|_| OwnerError::Refused)?;
        {
            let mut children = parent.children.lock().unwrap();
            *children = children.checked_add(1).ok_or(OwnerError::Refused)?;
        }
        // Once claimed, cancellation/disposal must observe the child hold.
        if parent.cancelled() {
            self.rollback_parent_locked(parent.clone());
            return Err(OwnerError::Refused);
        }
        Ok(parent.clone())
    }
    fn rollback_parent_locked(&mut self, parent: Arc<Slot>) {
        self.evidence.record();
        *parent.children.lock().unwrap() -= 1;
        self.rollback_notifications.push(parent);
    }
}
impl PayloadOwners {
    #[allow(dead_code)] // R04c calls after unlocking on a refused transaction.
    pub(super) fn release_pending(&self) {
        reap_shared(&self.shared, None);
    }
}

impl Admission {
    fn retain_work_use(&mut self, retained: super::admission::DependencyUse) {
        if let Self::Distributed { dependency, .. } = self {
            *dependency = Some(retained);
        }
    }
}

/// Opaque retained identity for R04c's later exact memory-only drain observer.
pub(super) struct ControlKey {
    entry: u64,
    // Preserve exact allocation identity without keeping the registered owner alive.
    slot: std::sync::Weak<Slot>,
}
impl PartialEq for ControlKey {
    fn eq(&self, other: &Self) -> bool {
        self.entry == other.entry && self.slot.ptr_eq(&other.slot)
    }
}
impl Eq for ControlKey {}
impl ControlKey {
    fn matches(&self, slot: &Arc<Slot>) -> bool {
        self.entry == slot.id && self.slot.ptr_eq(&Arc::downgrade(slot))
    }
}
#[allow(dead_code)] // Observation only, no positive drain/publication receipt.
pub(super) struct ControlRetention {
    pub(super) response_holds: usize,
    pub(super) cleanup_complete: bool,
    pub(super) terminated: bool,
}
impl ControlLease {
    #[allow(dead_code)] // R04c binds its barrier sample to this exact registered control.
    pub(super) fn key(&self) -> ControlKey {
        ControlKey {
            entry: self.0.slot.id,
            slot: Arc::downgrade(&self.0.slot),
        }
    }
}
impl PayloadRegistry {
    #[allow(dead_code)] // R04c still checks all other owners and proof-heartbeat settlement.
    pub(super) fn control_retention_locked(&self, key: &ControlKey) -> Option<ControlRetention> {
        self.controls
            .iter()
            .find(|e| {
                key.matches(&e.slot) && matches!(e.guards.owner(), Some(DistributedOwner::Control))
            })
            .map(|e| ControlRetention {
                response_holds: *e.slot.delivery_holds.lock().unwrap(),
                cleanup_complete: e.slot.cleanup.load(Ordering::Acquire),
                terminated: e.terminated,
            })
    }
}

struct RequestStorage {
    authority: Arc<std::sync::OnceLock<super::admission::RetainedRequestAuthority>>,
    payload: Option<(Box<[u8]>, super::auth::RequestBody)>,
    transferred: bool,
}
impl RequestStorage {
    fn new() -> Self {
        Self {
            authority: Arc::new(std::sync::OnceLock::new()),
            payload: None,
            transferred: false,
        }
    }
    fn empty(&self) -> bool {
        self.authority.get().is_none() && self.payload.is_none() && !self.transferred
    }
    fn install(
        &mut self,
        handoff: super::auth::RequestHandoff,
        boot: Arc<super::auth::ReceiverIdentity>,
        work: Option<(
            super::admission::WorkKey,
            super::admission::Binding,
            super::admission::DependencyKey,
        )>,
    ) {
        assert!(self.empty());
        let (authority, bytes, body) =
            super::admission::RetainedRequestAuthority::from_handoff(handoff, boot, work);
        assert!(self.authority.set(authority).is_ok());
        self.payload = Some((bytes, body));
    }
}
impl PayloadLease {
    pub(super) fn receiver_limits(&self) -> Result<SpoolLimits> {
        self.slot
            .receiver_limits
            .get()
            .copied()
            .ok_or(OwnerError::Refused)
    }
    /// Takes only this registered owner's original body. Caller drops the receiver
    /// lock before submitting any command, and retains delivery through settlement.
    #[allow(dead_code)] // The prechecked stream adapter is a later owner.
    pub(super) fn take_request_locked(
        &mut self,
        state: &mut super::admission::ReceiverState,
    ) -> Result<OwnedRequestPayload> {
        let registry = &mut state.payload;
        registry.evidence.record();
        let entry = registry
            .entries
            .iter_mut()
            .chain(registry.controls.iter_mut())
            .find(|e| Arc::ptr_eq(&e.slot, &self.slot))
            .ok_or(OwnerError::Refused)?;
        if entry.request.transferred || entry.request.authority.get().is_none() {
            return Err(OwnerError::Refused);
        }
        let delivery = self.retain_delivery()?;
        let (bytes, body) = entry.request.payload.take().ok_or(OwnerError::Refused)?;
        entry.request.transferred = true;
        Ok(OwnedRequestPayload {
            bytes,
            body,
            delivery,
        })
    }
}
#[allow(dead_code)] // Concrete stream continuation must retain delivery through accepted command settlement.
pub(super) struct OwnedRequestPayload {
    pub(super) bytes: Box<[u8]>,
    pub(super) body: super::auth::RequestBody,
    pub(super) delivery: DeliveryHold,
}
// Only consuming an admitted original body can construct this command input.
pub(crate) struct CheckedRequestJob(Box<JobEnvelope>);
pub(super) struct CheckedRequestRetention {
    _bytes: Box<[u8]>,
    _delivery: DeliveryHold,
}
impl OwnedRequestPayload {
    pub(super) fn into_job(self) -> Result<(CheckedRequestJob, CheckedRequestRetention)> {
        let Self {
            bytes,
            body,
            delivery,
        } = self;
        let super::auth::RequestBody::Job(job) = body else {
            return Err(OwnerError::Invalid);
        };
        Ok((
            CheckedRequestJob(job),
            CheckedRequestRetention {
                _bytes: bytes,
                _delivery: delivery,
            },
        ))
    }
}
impl PairReservation<'_, '_> {
    fn request_storage(&self) -> &RequestStorage {
        let position = if matches!(self.pair.role, Role::Operation) {
            match self.account {
                AccountPlan::New { preparation, .. } => preparation,
                AccountPlan::Existing(_) => unreachable!(),
            }
        } else {
            self.stream_preparation
        };
        &self.registry.preparations[position]
            .as_ref()
            .unwrap()
            .entry
            .as_ref()
            .unwrap()
            .request
    }
    pub(super) fn check_request_positions(&self) -> Result<()> {
        if self.request_storage().empty() {
            Ok(())
        } else {
            Err(OwnerError::Refused)
        }
    }
    pub(super) fn install_request_locked(
        self,
        handoff: super::auth::RequestHandoff,
    ) -> (InstalledPair, super::admission::StartSubject) {
        let work = self.local.retained_work(self.key);
        let boot = self.local.boot();
        let position = if matches!(self.pair.role, Role::Operation) {
            match self.account {
                AccountPlan::New { preparation, .. } => preparation,
                AccountPlan::Existing(_) => unreachable!(),
            }
        } else {
            self.stream_preparation
        };
        let storage = &mut self.registry.preparations[position]
            .as_mut()
            .unwrap()
            .entry
            .as_mut()
            .unwrap()
            .request;
        self.registry.evidence.record();
        storage.install(handoff, boot, Some(work));
        let subject = super::admission::StartSubject::new(self.key, storage.authority.clone());
        (self.install_entry_locked(), subject)
    }
}
impl EntryReservation<'_, '_> {
    pub(super) fn check_request_positions(&self) -> Result<()> {
        if self.registry.preparations[self.preparation]
            .as_ref()
            .unwrap()
            .entry
            .as_ref()
            .unwrap()
            .request
            .empty()
        {
            Ok(())
        } else {
            Err(OwnerError::Refused)
        }
    }
    pub(super) fn reserve_response(&mut self) -> Result<DeliveryHold> {
        #[cfg(test)]
        if let Some(generation) = self
            .prepared
            .slot
            .hooks
            .lock()
            .unwrap()
            .response_generation
            .take()
        {
            self.registry.evidence.fixture_generation(generation);
        }
        let _change = self
            .registry
            .evidence
            .begin(true)
            .map_err(|_| OwnerError::Refused)?;
        let handoff = self.prepared.slot.handoff.lock().unwrap();
        if handoff.cancelled {
            return Err(OwnerError::Cancelled);
        }
        let mut holds = self.prepared.slot.delivery_holds.lock().unwrap();
        *holds = holds.checked_add(1).ok_or(OwnerError::Refused)?;
        self.prepared
            .slot
            .response_live
            .store(true, Ordering::Release);
        Ok(DeliveryHold(self.prepared.slot.clone(), true))
    }
    pub(super) fn install_request_locked(
        self,
        handoff: super::auth::RequestHandoff,
        boot: Arc<super::auth::ReceiverIdentity>,
    ) -> ControlLease {
        self.registry.preparations[self.preparation]
            .as_mut()
            .unwrap()
            .entry
            .as_mut()
            .unwrap()
            .request
            .install(handoff, boot, None);
        self.install_control_locked()
    }
}
impl PayloadRegistry {
    pub(super) fn retains_request(
        &self,
        key: super::admission::WorkKey,
        authority: &Arc<std::sync::OnceLock<super::admission::RetainedRequestAuthority>>,
    ) -> bool {
        self.entries.iter().any(|e| {
            matches!(e.guards.owner(), Some(DistributedOwner::Work(k)) if k == key)
                && Arc::ptr_eq(&e.request.authority, authority)
                && !e.slot.cancelled()
                && !e.slot.failed.load(Ordering::Acquire)
        })
    }
}

impl PayloadRegistry {
    #[allow(dead_code)] // c3 binds its observer to this exact authenticated registered control.
    pub(super) fn control_request_locked(
        &self,
        key: &ControlKey,
    ) -> Option<&super::admission::RetainedRequestAuthority> {
        self.controls
            .iter()
            .find(|e| {
                key.matches(&e.slot) && matches!(e.guards.owner(), Some(DistributedOwner::Control))
            })?
            .request
            .authority
            .get()
    }
}

impl PayloadOwners {
    /// The concrete caller boundary flushes rollback notifications after unlocking.
    #[allow(dead_code)] // Future prelude supplies checked inputs and its existing role.
    pub(super) fn accept(
        &self,
        view: super::admission::DurableRequestView,
        content: super::auth::CheckedContent,
        prepared: &mut super::admission::PreparedRoute,
        process: &PayloadLease,
    ) -> std::result::Result<super::admission::AcceptedRoute, super::admission::AcceptError> {
        let result = self
            .shared
            .state
            .lock()
            .unwrap()
            .accept_locked(view, content, prepared, process);
        self.release_pending();
        result
    }
}
impl PayloadLease {
    #[allow(dead_code)] // Future prechecked stream continuation takes its original body once.
    pub(super) fn take_request(&mut self) -> Result<OwnedRequestPayload> {
        let shared = self.shared.clone();
        let mut state = shared.state.lock().unwrap();
        self.take_request_locked(&mut state)
    }
}

// Temporary item-level allowance until c3b consumes this private owner evidence.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DrainWork {
    InvalidOwner,
    WorkPresent,
    ExemptOnly,
}
impl PayloadRegistry {
    #[allow(dead_code)] // c3b will consume owner evidence, not a drain receipt.
    pub(super) fn drain_work_locked(
        &self,
        process_identity: uuid::Uuid,
        observer: &ControlKey,
        local: &super::admission::LocalAdmission,
    ) -> DrainWork {
        self.drain_work_for_completion(process_identity, observer, local, None)
    }
    pub(super) fn drain_work_for_reaped(
        &self,
        process_identity: uuid::Uuid,
        observer: &ControlKey,
        local: &super::admission::LocalAdmission,
        proof: &super::drain_observation::ReapedProof,
    ) -> DrainWork {
        if !self.proof.matches_reaped(proof) || self.failed || self.proof.failed() {
            return DrainWork::InvalidOwner;
        }
        // The original response obligation must still exist, independently of extras.
        if !self
            .controls
            .iter()
            .any(|e| observer.matches(&e.slot) && e.slot.response_live.load(Ordering::Acquire))
        {
            return DrainWork::InvalidOwner;
        }
        self.drain_work_for_completion(process_identity, observer, local, Some(proof))
    }
    fn drain_work_for_completion(
        &self,
        process_identity: uuid::Uuid,
        observer: &ControlKey,
        local: &super::admission::LocalAdmission,
        proof: Option<&super::drain_observation::ReapedProof>,
    ) -> DrainWork {
        // ReceiverState is held by the caller. Never inspect Disk or reacquire it.
        let snapshot = local.snapshot();
        let Some(process) = self.entries.iter().find(|e| {
            self.boot_reserved == Some(e.slot.id)
                && e.slot.kind == Kind::Process
                && matches!(e.guards.owner(), Some(DistributedOwner::Boot(id)) if id == process_identity)
        }) else {
            return DrainWork::InvalidOwner;
        };
        let Some(control) = self.controls.iter().find(|e| {
            observer.matches(&e.slot)
                && e.slot.kind == Kind::Control
                && matches!(e.guards.owner(), Some(DistributedOwner::Control))
        }) else {
            return DrainWork::InvalidOwner;
        };
        let live = |e: &Entry| {
            !e.terminated
                && !e.handle.is_finished()
                && e.slot.initialized.load(Ordering::Acquire)
                && !e.slot.cleanup.load(Ordering::Acquire)
                && !e.slot.failed.load(Ordering::Acquire)
                && !e.slot.cancelled()
        };
        let Some(request) = control.request.authority.get() else {
            return DrainWork::InvalidOwner;
        };
        use super::auth::PendingLocalChecks;
        let PendingLocalChecks::Drain { drain_id } = request.pending else {
            return DrainWork::InvalidOwner;
        };
        if !snapshot.initialized()
            || !live(process)
            || !live(control)
            || !request.local.same_boot(&local.boot())
            || request.binding.auth().scope() != super::auth::Scope::Drain
            || (!control.request.transferred
                && !matches!(control.request.payload.as_ref(),
                    Some((_, super::auth::RequestBody::Drain(body))) if body.drain_id() == drain_id))
        {
            return DrainWork::InvalidOwner;
        }
        let ordinary_guards = |e: &Entry, transport_required: bool| {
            matches!(&*e.guards, Admission::Distributed {
                dependency: None, parent: None, transport, ..
            } if match transport { None => !transport_required, Some(p) => transport_required && Arc::strong_count(p) == 1 })
        };
        if self.closed
            || self.failed
            || (self.proof.retained() != 0 && !proof.is_some_and(|p| self.proof.matches_reaped(p)))
            || self.pending_releases != 0
            || !self.rollback_notifications.is_empty()
            || self.preparations.iter().any(Option::is_some)
            || self.entries.len() != 1
            || self.controls.len() != 1
            || !self.process_used
            || self.positions.first().copied().flatten() != Some(process.slot.id)
            || self.positions.iter().skip(1).any(Option::is_some)
            || process.position != Some(0)
            || process.record.is_some()
            || !process.request.empty()
            || control.position.is_some()
            || control.record.is_none()
            || self
                .records
                .iter()
                .enumerate()
                .any(|(i, record)| match record {
                    None => control.record == Some(i),
                    Some(AcceptedOwner::Control(id)) => {
                        control.record != Some(i) || *id != control.slot.id
                    }
                    Some(AcceptedOwner::Work(_)) => true,
                })
            || !ordinary_guards(process, false)
            || !ordinary_guards(control, true)
            || [process, control].iter().any(|e| {
                e.account_tx.is_some()
                    || e.account_ready.is_some()
                    || e.slot.producing.load(Ordering::Acquire)
                    || *e.slot.children.lock().unwrap() != 0
            })
            || *process.slot.delivery_holds.lock().unwrap() != 0
            || {
                let holds = control.slot.delivery_holds.lock().unwrap();
                *holds > usize::from(control.slot.response_live.load(Ordering::Acquire))
            }
            || snapshot.occupancy() != (0, 0, 0, 0)
            || snapshot.dependencies() != (0, 0)
        {
            DrainWork::WorkPresent
        } else {
            DrainWork::ExemptOnly
        }
    }
}

impl PayloadRegistry {
    pub(super) fn control_authority(
        &self,
        key: &ControlKey,
    ) -> Option<Arc<std::sync::OnceLock<super::admission::RetainedRequestAuthority>>> {
        self.controls
            .iter()
            .find(|e| key.matches(&e.slot))
            .map(|e| e.request.authority.clone())
    }
}

impl PayloadRegistry {
    pub(super) fn sample_handoff_settled(&self) -> bool {
        self.pending_releases == 0 && self.rollback_notifications.is_empty()
    }
}

impl PayloadOwners {
    pub(super) fn publish_proof<B: crate::storage::ObjectStore + 'static>(
        &self,
        sample: super::admission::BarrierSample,
        storage: Arc<crate::storage::Storage<B>>,
        deadline: Instant,
    ) -> std::result::Result<
        super::drain_observation::ProofTicket,
        super::drain_observation::ProofError,
    > {
        use super::drain_observation::ProofError;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| ProofError::Refused)?;
        let (activate, activated) = oneshot::channel();
        let cycle = super::drain_observation::ProofSlot::prepare(
            sample,
            storage.clone(),
            self.shared.reap_wake.0.clone(),
        );
        let mut state = self.shared.state.lock().unwrap();
        if state.payload.proof.retained() != 0 {
            return Err(ProofError::Busy);
        }
        if state.payload.closed
            || state.payload.failed
            || !super::drain_observation::ProofSlot::sample_matches(&cycle, &state)
        {
            return Err(ProofError::Refused);
        }
        let ticket = state.payload.proof.reserve(cycle)?;
        let invocation = state.payload.proof.invocation(&ticket);
        #[cfg(test)]
        let preparing = state.payload.proof.preparing.take();
        #[cfg(test)]
        let spawning = state.payload.proof.spawning.take();
        drop(state);
        #[cfg(test)]
        if let Some((entered, gate)) = preparing {
            let _ = entered.send(());
            let _ = gate.recv_timeout(std::time::Duration::from_secs(10));
        }
        #[cfg(test)]
        if let Some(probe) = spawning {
            let _ = probe.send(self.shared.state.try_lock().is_ok());
        }
        let handle =
            nabedra_rt::task::spawn_on(&runtime, invocation.run(storage, deadline, activated));
        self.shared
            .state
            .lock()
            .unwrap()
            .payload
            .proof
            .publish_handle(&ticket, handle);
        // A waiter that inspected the empty placeholder has no handle waker yet.
        self.shared.reap_wake.0.notify_waiters();
        let _ = activate.send(());
        Ok(ticket)
    }
    /// The ordinary publisher and registered drain cycles share one writer for
    /// this boot, preserving heartbeat sequence and conditional-write ownership.
    pub(super) fn heartbeat_writer(
        &self,
    ) -> Arc<tokio::sync::Mutex<Option<crate::cluster::heartbeat::HeartbeatWriter>>> {
        self.shared
            .state
            .lock()
            .unwrap()
            .payload
            .proof
            .heartbeat_writer()
    }
    pub(super) async fn wait_proof(
        &self,
        ticket: &super::drain_observation::ProofTicket,
    ) -> std::result::Result<(), super::drain_observation::ProofError> {
        loop {
            let notified = self.shared.reap_wake.0.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            reap_shared(&self.shared, None);
            if self
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .proof
                .status(ticket)?
            {
                return Ok(());
            }
            notified.await;
        }
    }
    pub(super) fn take_proof(
        &self,
        ticket: &super::drain_observation::ProofTicket,
        observer: &ControlKey,
    ) -> Option<super::drain_observation::ReapedProof> {
        let state = self.shared.state.lock().unwrap();
        if state.payload.pending_releases != 0 {
            return None;
        }
        state.payload.proof.take(ticket, observer)
    }
}

impl PayloadOwners {
    #[allow(dead_code)] // Local producer; HTTP integration is a later owner.
    pub(super) fn finish_drain(
        &self,
        proof: super::drain_observation::ReapedProof,
    ) -> std::result::Result<super::admission::DrainObservation, super::admission::DrainRefusal>
    {
        super::admission::DrainObservation::finish(&self.shared.state, proof)
    }
}

fn begin_result(
    d: &mut Disk,
    result: Box<ResultEnvelope>,
    expected: Box<JobEnvelope>,
) -> Result<Reply> {
    use super::payload_stream::ReceiveKind;
    let kind = match &result.result {
        JobResult::Get { .. } | JobResult::Collect { .. } => {
            let writer = d
                .operation
                .as_ref()
                .ok_or(OwnerError::Invalid)?
                .writer(MAX_INTERNAL_ROW_BYTES as u64)
                .map_err(|_| OwnerError::Invalid)?;
            d.processor = Processor::Rows {
                job: expected,
                result,
                writer,
            };
            ReceiveKind::Rows
        }
        JobResult::Prepared {
            replacement_count, ..
        } => {
            let count = replacement_count.unwrap_or(0);
            let receiver = PreparedReceiver::new(
                d.operation.as_ref().ok_or(OwnerError::Invalid)?.clone(),
                &result,
                &expected,
            )
            .map_err(|_| OwnerError::Invalid)?;
            d.processor = Processor::Prepared {
                job: expected,
                receiver: Box::new(receiver),
            };
            ReceiveKind::Prepared(count)
        }
        _ => {
            d.processor = Processor::PendingControl(result);
            ReceiveKind::Control
        }
    };
    Ok(Reply::Receiving(kind))
}

mod parallel;
pub(super) use parallel::ForkSource;
