//! Worker-owned physical execution and native-memory resources.
use crate::{
    config::WorkerResources,
    protocol::{ErrorCode, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use rocksdb::{Cache, Env, WriteBufferManager};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fmt, io,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
};
use uuid::Uuid;
static WORKERS: Mutex<BTreeMap<Uuid, Weak<WorkerSharedResources>>> = Mutex::new(BTreeMap::new());
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

#[derive(Clone, Copy, Debug)]
pub struct SharedResourceConfig {
    pub query_threads: usize,
    pub control_threads: usize,
    pub background_threads: i32,
    pub cache_bytes: usize,
    pub write_buffer_bytes: usize,
    pub control_bytes: usize,
    pub progress_bytes: usize,
    pub apply_bytes_per_slot: usize,
    pub query_reply_bytes: usize,
    pub control_reply_bytes: usize,
    pub max_open_pools: usize,
    pub max_group_handles: usize,
    pub max_resident_groups: usize,
}
impl Default for SharedResourceConfig {
    fn default() -> Self {
        Self {
            query_threads: 1,
            control_threads: 1,
            background_threads: 2,
            cache_bytes: 16 << 20,
            write_buffer_bytes: 16 << 20,
            control_bytes: 32 << 20,
            progress_bytes: 4 << 20,
            apply_bytes_per_slot: 64 << 20,
            query_reply_bytes: 8 << 20,
            control_reply_bytes: 1 << 20,
            max_open_pools: 8,
            max_group_handles: 128,
            max_resident_groups: 64,
        }
    }
}

/// One owner per worker, shared by all volumes, groups and RAM queue accounting.
/// The scheduler ID is not a permanent Raft node ID or worker enrollment UUID.
pub struct WorkerSharedResources {
    pub(super) cache: Cache,
    pub(super) write_buffers: WriteBufferManager,
    pub(super) env: Env,
    pub(super) config: SharedResourceConfig,
    pub(super) pool_slots: Arc<Semaphore>,
    pub(super) handle_slots: Arc<Semaphore>,
    resident_slots: Arc<Semaphore>,
    scheduler: ResourceScheduler,
    scheduler_node_id: u64,
    task_memory_limit_bytes: usize,
    decode_bytes: Arc<AtomicUsize>,
    thread_budget: super::super::NativeThreadBudget,
    worker_id: Mutex<Option<Uuid>>,
    weak_self: OnceLock<Weak<Self>>,
    query: NativeExecutor,
    control: NativeExecutor,
    arena: Arc<ByteArena>,
    progress: Arc<ByteArena>,
    apply_query: Arc<ByteArena>,
    apply_control: Arc<ByteArena>,
    reply_query: Arc<ByteArena>,
    reply_control: Arc<ByteArena>,
    queue_claim: Arc<AtomicBool>,
    _native_charge: MemoryCharge,
    leases: Mutex<LeaseState>,
    settled: Notify,
}

impl WorkerSharedResources {
    pub fn new(
        scheduler_node_id: u64,
        limits: WorkerResources,
        config: SharedResourceConfig,
    ) -> Result<Arc<Self>, ProtocolError> {
        limits.validate().map_err(|_| capacity())?;
        let foreground = config
            .query_threads
            .checked_add(config.control_threads)
            .ok_or_else(capacity)?;
        let total_threads = foreground
            .checked_add(
                config
                    .background_threads
                    .try_into()
                    .map_err(|_| capacity())?,
            )
            .ok_or_else(capacity)?;
        if config.query_threads == 0
            || config.control_threads == 0
            || !(2..=128).contains(&config.background_threads)
            || total_threads as u64 > limits.max_threads
            || config.cache_bytes < 1 << 20
            || config.write_buffer_bytes < 1 << 20
            || config.control_bytes == 0
            || config.apply_bytes_per_slot == 0
            || config.query_reply_bytes == 0
            || config.control_reply_bytes == 0
            || config.progress_bytes == 0
            || config.progress_bytes >= config.control_bytes
            || config.control_bytes as u64 > limits.task_memory_limit_bytes
            || config.max_open_pools == 0
            || config.max_group_handles == 0
            || config.max_resident_groups == 0
            || [
                config.max_open_pools,
                config.max_group_handles,
                config.max_resident_groups,
            ]
            .iter()
            .any(|&n| n > Semaphore::MAX_PERMITS)
        {
            return Err(capacity());
        }
        let scheduler = ResourceScheduler::new(vec![(
            scheduler_node_id,
            WorkerResources {
                max_threads: foreground as u64,
                ..limits
            },
        )])?;
        // Stack reservation and native overhead are conservative logical charges.
        // The OS page cache and allocator RSS still require measured deployment limits.
        let thread_budget = super::super::NativeThreadBudget::new(
            config.query_threads,
            config.control_threads,
            config.background_threads,
        )?;
        let native_bytes = config
            .cache_bytes
            .checked_add(config.write_buffer_bytes)
            .and_then(|n| n.checked_add(thread_budget.total_stack_reservation_bytes))
            .and_then(|n| n.checked_add(16 << 20))
            .ok_or_else(capacity)?;
        let native_charge = scheduler.charge_memory(scheduler_node_id, native_bytes as u64)?;
        let make_arena = |bytes: usize| {
            ByteArena::new(
                bytes,
                scheduler.charge_memory(scheduler_node_id, bytes as u64)?,
            )
        };
        let arena = make_arena(config.control_bytes - config.progress_bytes)?;
        let progress = make_arena(config.progress_bytes)?;
        let apply_query = make_arena(
            config
                .apply_bytes_per_slot
                .checked_mul(config.query_threads)
                .ok_or_else(capacity)?,
        )?;
        let apply_control = make_arena(
            config
                .apply_bytes_per_slot
                .checked_mul(config.control_threads)
                .ok_or_else(capacity)?,
        )?;
        let reply_query = make_arena(config.query_reply_bytes)?;
        let reply_control = make_arena(config.control_reply_bytes)?;
        let env = super::super::native_environment(config.background_threads)?;
        let owner = Arc::new(Self {
            cache: Cache::new_lru_cache(config.cache_bytes),
            write_buffers: WriteBufferManager::new_write_buffer_manager(
                config.write_buffer_bytes,
                true,
            ),
            env,
            config,
            pool_slots: Arc::new(Semaphore::new(config.max_open_pools)),
            handle_slots: Arc::new(Semaphore::new(config.max_group_handles)),
            resident_slots: Arc::new(Semaphore::new(config.max_resident_groups)),
            scheduler,
            scheduler_node_id,
            task_memory_limit_bytes: limits.task_memory_limit_bytes as usize,
            decode_bytes: Arc::new(AtomicUsize::new(0)),
            thread_budget,
            worker_id: Mutex::new(None),
            weak_self: OnceLock::new(),
            query: NativeExecutor::new(config.query_threads, "oltp-query")?,
            control: NativeExecutor::new(config.control_threads, "oltp-control")?,
            arena,
            progress,
            apply_query,
            apply_control,
            reply_query,
            reply_control,
            queue_claim: Arc::new(AtomicBool::new(false)),
            _native_charge: native_charge,
            leases: Mutex::new(LeaseState {
                closed: false,
                count: 0,
            }),
            settled: Notify::new(),
        });
        let weak = Arc::downgrade(&owner);
        let _ = owner.weak_self.set(weak.clone());
        for arena in [
            &owner.arena,
            &owner.progress,
            &owner.apply_query,
            &owner.apply_control,
            &owner.reply_query,
            &owner.reply_control,
        ] {
            let _ = arena.worker.set(weak.clone());
        }
        Ok(owner)
    }
    fn owner(&self) -> Result<Arc<Self>, ProtocolError> {
        self.weak_self
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(capacity)
    }

    /// Bind the one physical worker accounting owner before pools or queue
    /// enrollment. A second live owner cannot reset that worker's limits.
    pub fn bind_worker(self: &Arc<Self>, worker_id: Uuid) -> Result<(), ProtocolError> {
        if worker_id.is_nil() {
            return Err(capacity());
        }
        let mut workers = WORKERS.lock().map_err(|_| capacity())?;
        workers.retain(|_, owner| owner.strong_count() > 0);
        if let Some(other) = workers.get(&worker_id).and_then(Weak::upgrade)
            && !Arc::ptr_eq(&other, self)
        {
            return Err(capacity());
        }
        let mut current = self.worker_id.lock().map_err(|_| capacity())?;
        if current.is_some_and(|id| id != worker_id) {
            return Err(capacity());
        }
        *current = Some(worker_id);
        workers.insert(worker_id, Arc::downgrade(self));
        Ok(())
    }
    pub fn worker_id(&self) -> Option<Uuid> {
        *self.worker_id.lock().expect("worker enrollment state")
    }

    /// The one physical queue owner contains every database scope under shared
    /// count/byte/result ceilings. A scoped wrapper must not create another owner.
    pub fn claim_queue(self: &Arc<Self>) -> Result<QueueLease, ProtocolError> {
        if self.worker_id().is_none() || self.leases.lock().map_err(|_| capacity())?.closed {
            return Err(capacity());
        }
        self.queue_claim
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| capacity())?;
        Ok(QueueLease {
            owner: self.clone(),
        })
    }
    pub fn reply_waiters(&self, class: ExecutionClass) -> usize {
        match class {
            ExecutionClass::Query => self.reply_query.waiting.load(Ordering::Acquire),
            ExecutionClass::Control => self.reply_control.waiting.load(Ordering::Acquire),
        }
    }
    pub fn apply_bytes_used(&self) -> usize {
        self.apply_query.used.load(Ordering::Acquire)
            + self.apply_control.used.load(Ordering::Acquire)
    }

    pub fn scheduler(&self) -> ResourceScheduler {
        self.scheduler.clone()
    }
    pub fn scheduler_node_id(&self) -> u64 {
        self.scheduler_node_id
    }
    pub fn control_bytes_used(&self) -> usize {
        [
            &self.arena,
            &self.progress,
            &self.apply_query,
            &self.apply_control,
            &self.reply_query,
            &self.reply_control,
        ]
        .iter()
        .map(|arena| arena.used.load(Ordering::Acquire))
        .sum()
    }

    /// The multi-Raft owner must hold this through task termination. A storage
    /// handle going inactive never releases an advertised voter's residency.
    pub fn reserve_group_residency(&self) -> Result<OwnedSemaphorePermit, ProtocolError> {
        self.resident_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())
    }

    pub fn buffer_bytes_used(&self, class: ByteClass) -> usize {
        self.arena_for(class).used.load(Ordering::Acquire)
    }
    fn arena_for(&self, class: ByteClass) -> &Arc<ByteArena> {
        match class {
            ByteClass::Progress => &self.progress,
            ByteClass::Data => &self.arena,
        }
    }
    pub fn allocate_bytes(
        &self,
        class: ByteClass,
        len: usize,
    ) -> Result<ChargedBuffer, ProtocolError> {
        let charge = self.reserve_bytes_in(class, len)?;
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|_| capacity())?;
        data.resize(len, 0);
        Ok(ChargedBuffer { data, charge })
    }
    pub fn reserve_scratch(
        &self,
        class: ByteClass,
        bytes: usize,
    ) -> Result<ScratchLease, ProtocolError> {
        let backing = match class {
            ByteClass::Progress => ScratchBacking::Arena(self.reserve_bytes_in(class, bytes)?),
            ByteClass::Data => {
                if bytes > self.task_memory_limit_bytes {
                    return Err(capacity());
                }
                let charge = self
                    .scheduler
                    .charge_memory(self.scheduler_node_id, bytes as u64)?;
                self.decode_bytes.fetch_add(bytes, Ordering::AcqRel);
                ScratchBacking::Working {
                    charge,
                    counter: self.decode_bytes.clone(),
                }
            }
        };
        Ok(ScratchLease {
            backing,
            _owner: self.owner()?,
        })
    }
    pub fn working_decode_bytes(&self) -> usize {
        self.decode_bytes.load(Ordering::Acquire)
    }
    pub fn copy_bytes(&self, bytes: &[u8]) -> Result<ChargedBytes, ProtocolError> {
        self.copy_bytes_in(ByteClass::Data, bytes)
    }
    pub fn copy_bytes_in(
        &self,
        class: ByteClass,
        bytes: &[u8],
    ) -> Result<ChargedBytes, ProtocolError> {
        self.reserve_bytes_in(class, bytes.len())?.copy(bytes)
    }
    pub fn encode<T: Serialize>(
        &self,
        value: &T,
        max_bytes: usize,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.encode_in(ByteClass::Data, value, max_bytes)
    }
    pub fn encode_in<T: Serialize>(
        &self,
        class: ByteClass,
        value: &T,
        max_bytes: usize,
    ) -> Result<ChargedBytes, ProtocolError> {
        let mut count = CountingWriter {
            bytes: 0,
            limit: max_bytes,
        };
        serde_json::to_writer(&mut count, value).map_err(|_| capacity())?;
        self.reserve_bytes_in(class, count.bytes)?.encode(value)
    }
    pub(super) fn reserve_bytes_in(
        &self,
        class: ByteClass,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        self.arena_for(class).reserve(bytes)
    }

    pub(super) async fn reply_budget(
        &self,
        class: ExecutionClass,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        match class {
            ExecutionClass::Query => self.reply_query.reserve_wait(bytes).await,
            ExecutionClass::Control => self.reply_control.reserve_wait(bytes).await,
        }
    }
    pub async fn reserve_scratch_wait(
        &self,
        class: ByteClass,
        bytes: usize,
        request: u64,
    ) -> Result<ScratchLease, ProtocolError> {
        match class {
            ByteClass::Progress => Ok(ScratchLease {
                backing: ScratchBacking::Arena(self.progress.reserve_wait(bytes).await?),
                _owner: self.owner()?,
            }),
            ByteClass::Data => {
                if bytes > self.task_memory_limit_bytes {
                    return Err(capacity());
                }
                let charge = self
                    .scheduler
                    .reserve_memory(request, self.scheduler_node_id, bytes as u64)
                    .await?;
                self.decode_bytes.fetch_add(bytes, Ordering::AcqRel);
                Ok(ScratchLease {
                    backing: ScratchBacking::Working {
                        charge,
                        counter: self.decode_bytes.clone(),
                    },
                    _owner: self.owner()?,
                })
            }
        }
    }

    pub(super) fn pool_overhead(&self) -> Result<MemoryCharge, ProtocolError> {
        self.scheduler
            .charge_memory(self.scheduler_node_id, 64 << 20)
    }

    pub fn config(&self) -> SharedResourceConfig {
        self.config
    }
    pub fn thread_budget(&self) -> super::super::NativeThreadBudget {
        self.thread_budget
    }
    pub fn native_baseline_bytes(&self) -> u64 {
        self._native_charge.bytes()
            + [
                &self.arena,
                &self.progress,
                &self.apply_query,
                &self.apply_control,
                &self.reply_query,
                &self.reply_control,
            ]
            .iter()
            .map(|arena| arena._charge.bytes())
            .sum::<u64>()
    }
    pub async fn prepare(
        self: &Arc<Self>,
        class: ExecutionClass,
    ) -> Result<PreparedNative, ProtocolError> {
        {
            let mut state = self.leases.lock().map_err(|_| capacity())?;
            if state.closed {
                return Err(capacity());
            }
            state.count += 1;
        }
        let mut lease = NativeLease {
            owner: self.clone(),
            _permit: None,
        };
        let executor = match class {
            ExecutionClass::Query => &self.query,
            ExecutionClass::Control => &self.control,
        };
        let permit = executor
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| capacity())?;
        lease._permit = Some(permit);
        Ok(PreparedNative { class, lease })
    }
    /// Close admission and join every prepared or dispatched native lease.
    /// Callers must release their own prepared work before awaiting this drain.
    pub async fn drain(&self) {
        {
            let mut state = self.leases.lock().expect("native lease state");
            state.closed = true;
            self.query.slots.close();
            self.control.slots.close();
            self.reply_query.close();
            self.reply_control.close();
        }
        loop {
            let notified = self.settled.notified();
            if self.leases.lock().expect("native lease state").count == 0 {
                return;
            }
            notified.await;
        }
    }
    /// Physical capacity precedes CPU admission, even with a caller Tokio
    /// runtime whose blocking pool has only one thread.
    pub async fn execute<T: Send + 'static>(
        self: &Arc<Self>,
        class: ExecutionClass,
        request: u64,
        task_bytes: u64,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let prepared = self.prepare(class).await?;
        let grant = self
            .scheduler
            .reserve_on(request, 1, self.scheduler_node_id, task_bytes)
            .await?;
        prepared.run_with_guard(grant, work).await?.0
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ExecutionClass {
    Query,
    Control,
}

/// Choose this from the authenticated endpoint role. Queue/data payloads must
/// never consume Progress, which keeps small authority/settlement exchanges live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteClass {
    Progress,
    Data,
}
/// A mutable received frame owns its budget before its first byte is allocated.
pub struct ChargedBuffer {
    data: Vec<u8>,
    charge: ByteReservation,
}
impl ChargedBuffer {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    pub fn freeze(self) -> ChargedBytes {
        ChargedBytes(Arc::new(ChargedData {
            data: self.data,
            _charge: self.charge,
        }))
    }
}
pub struct QueueLease {
    owner: Arc<WorkerSharedResources>,
}
impl Drop for QueueLease {
    fn drop(&mut self) {
        self.owner.queue_claim.store(false, Ordering::Release);
    }
}

pub struct ScratchLease {
    _owner: Arc<WorkerSharedResources>,
    backing: ScratchBacking,
}
enum ScratchBacking {
    Arena(ByteReservation),
    Working {
        charge: MemoryCharge,
        counter: Arc<AtomicUsize>,
    },
}
impl ScratchLease {
    pub fn bytes(&self) -> usize {
        match &self.backing {
            ScratchBacking::Arena(charge) => charge.remaining,
            ScratchBacking::Working { charge, .. } => charge.bytes() as usize,
        }
    }
}
impl Drop for ScratchLease {
    fn drop(&mut self) {
        if let ScratchBacking::Working { charge, counter } = &self.backing {
            counter.fetch_sub(charge.bytes() as usize, Ordering::AcqRel);
        }
    }
}

struct LeaseState {
    closed: bool,
    count: usize,
}
struct NativeLease {
    owner: Arc<WorkerSharedResources>,
    _permit: Option<OwnedSemaphorePermit>,
}
impl Drop for NativeLease {
    fn drop(&mut self) {
        // Drop physical capacity before notifying an awaiting drain.
        self._permit.take();
        self.owner.leases.lock().expect("native lease state").count -= 1;
        self.owner.settled.notify_waiters();
    }
}
pub struct PreparedNative {
    class: ExecutionClass,
    lease: NativeLease,
}
impl PreparedNative {
    pub(super) fn buffers(
        &self,
        budget: super::TransactionBudget,
        output: ByteReservation,
    ) -> Result<(ByteReservation, ByteReservation, ByteReservation), ProtocolError> {
        let owner = &self.lease.owner;
        let working = budget
            .scratch_bytes
            .checked_add(budget.batch_bytes)
            .ok_or_else(capacity)?;
        if working > owner.config.apply_bytes_per_slot {
            return Err(capacity());
        }
        let apply = match self.class {
            ExecutionClass::Query => &owner.apply_query,
            ExecutionClass::Control => &owner.apply_control,
        };
        // Reply capacity was acquired without a physical or CPU permit. Every
        // active native slot now owns this bounded working maximum already.
        let mut all = apply.reserve(working)?;
        let scratch = all.split(budget.scratch_bytes)?;
        let batch = all.split(budget.batch_bytes)?;
        Ok((scratch, batch, output))
    }
    pub async fn execute<T: Send + 'static>(
        self,
        request: u64,
        task_bytes: u64,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let owner = self.lease.owner.clone();
        let grant = owner
            .scheduler
            .reserve_on(request, 1, owner.scheduler_node_id, task_bytes)
            .await?;
        self.run_with_guard(grant, work).await?.0
    }

    /// The caller acquires its scheduler grant after prepare and passes that
    /// owner here. It stays inside native work through future cancellation.
    pub async fn run_with_guard<T: Send + 'static, G: Send + 'static>(
        self,
        guard: G,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<(Result<T, ProtocolError>, G), ProtocolError> {
        let owner = self.lease.owner.clone();
        let executor = match self.class {
            ExecutionClass::Query => &owner.query,
            ExecutionClass::Control => &owner.control,
        };
        let (tx, rx) = oneshot::channel();
        executor.send(Box::new(move || {
            let _lease = self.lease;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
                .unwrap_or_else(|_| Err(super::super::recovery_unknown()));
            let _ = tx.send((result, guard));
        }))?;
        rx.await.map_err(|_| super::super::recovery_unknown())
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;
struct NativeExecutor {
    sender: Option<mpsc::Sender<Job>>,
    threads: Vec<JoinHandle<()>>,
    slots: Arc<Semaphore>,
}
impl NativeExecutor {
    fn new(count: usize, name: &str) -> Result<Self, ProtocolError> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut executor = Self {
            sender: Some(sender),
            threads: Vec::new(),
            slots: Arc::new(Semaphore::new(count)),
        };
        for index in 0..count {
            let receiver = receiver.clone();
            let thread = thread::Builder::new()
                .name(format!("{name}-{index}"))
                .stack_size(2 << 20)
                .spawn(move || {
                    loop {
                        let job = receiver.lock().unwrap().recv();
                        match job {
                            Ok(job) => {
                                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                            }
                            Err(_) => break,
                        }
                    }
                })
                .map_err(|_| capacity())?;
            executor.threads.push(thread);
        }
        Ok(executor)
    }
    fn send(&self, job: Job) -> Result<(), ProtocolError> {
        self.sender
            .as_ref()
            .ok_or_else(capacity)?
            .send(job)
            .map_err(|_| capacity())
    }
}
impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.slots.close();
        self.sender.take();
        for thread in self.threads.drain(..) {
            if thread.thread().id() != thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

struct ByteArena {
    settled: Notify,
    waiting: AtomicUsize,
    closed: AtomicBool,
    capacity: usize,
    used: AtomicUsize,
    _charge: MemoryCharge,
    worker: OnceLock<Weak<WorkerSharedResources>>,
}
impl ByteArena {
    fn new(capacity: usize, charge: MemoryCharge) -> Result<Arc<Self>, ProtocolError> {
        Ok(Arc::new(Self {
            capacity,
            used: AtomicUsize::new(0),
            _charge: charge,
            worker: OnceLock::new(),
            settled: Notify::new(),
            waiting: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }))
    }
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.settled.notify_waiters();
    }
    async fn reserve_wait(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        if bytes > self.capacity {
            return Err(capacity());
        }
        let mut waiting = ByteWaiter {
            arena: self.clone(),
            active: false,
        };
        loop {
            let notified = self.settled.notified();
            if self.closed.load(Ordering::Acquire) {
                return Err(capacity());
            }
            if let Ok(reservation) = self.reserve(bytes) {
                return Ok(reservation);
            }
            if !waiting.active {
                self.waiting.fetch_add(1, Ordering::AcqRel);
                waiting.active = true;
            }
            notified.await;
        }
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<ByteReservation, ProtocolError> {
        let owner = self
            .worker
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(capacity)?;
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|&n| n <= self.capacity)
                .ok_or_else(capacity)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => used = current,
            }
        }
        Ok(ByteReservation {
            arena: self.clone(),
            _owner: owner,
            remaining: bytes,
        })
    }
}
struct ByteWaiter {
    arena: Arc<ByteArena>,
    active: bool,
}
impl Drop for ByteWaiter {
    fn drop(&mut self) {
        if self.active {
            self.arena.waiting.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
pub(super) struct ByteReservation {
    _owner: Arc<WorkerSharedResources>,
    arena: Arc<ByteArena>,
    remaining: usize,
}
impl ByteReservation {
    pub(super) fn rehome(&mut self, value: ChargedBytes) -> Result<ChargedBytes, ProtocolError> {
        if Arc::ptr_eq(&self.arena, &value.0._charge.arena) {
            Ok(value)
        } else {
            self.copy(value.as_slice())
        }
    }
    pub(super) fn encode_value<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<ChargedBytes, ProtocolError> {
        let mut count = CountingWriter {
            bytes: 0,
            limit: self.remaining,
        };
        serde_json::to_writer(&mut count, value).map_err(|_| capacity())?;
        self.split(count.bytes)?.encode(value)
    }
    pub(super) fn buffer(&mut self, len: usize) -> Result<ScratchBuffer<'_>, ProtocolError> {
        if len > self.remaining {
            return Err(capacity());
        }
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|_| capacity())?;
        data.resize(len, 0);
        Ok(ScratchBuffer {
            data,
            _reservation: self,
        })
    }
    pub(super) fn remaining(&self) -> usize {
        self.remaining
    }
    pub(super) fn split(&mut self, bytes: usize) -> Result<Self, ProtocolError> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(capacity)?;
        Ok(Self {
            arena: self.arena.clone(),
            _owner: self._owner.clone(),
            remaining: bytes,
        })
    }
    pub(super) fn join(&mut self, parts: &[&[u8]]) -> Result<ChargedBytes, ProtocolError> {
        let length = parts
            .iter()
            .try_fold(0usize, |n, p| n.checked_add(p.len()))
            .ok_or_else(capacity)?;
        let charge = self.split(length)?;
        let mut data = Vec::new();
        data.try_reserve_exact(length).map_err(|_| capacity())?;
        for part in parts {
            data.extend_from_slice(part);
        }
        Ok(ChargedBytes(Arc::new(ChargedData {
            data,
            _charge: charge,
        })))
    }
    pub(super) fn copy(&mut self, bytes: &[u8]) -> Result<ChargedBytes, ProtocolError> {
        let charge = self.split(bytes.len())?;
        let mut data = Vec::new();
        data.try_reserve_exact(bytes.len())
            .map_err(|_| capacity())?;
        data.extend_from_slice(bytes);
        Ok(ChargedBytes(Arc::new(ChargedData {
            data,
            _charge: charge,
        })))
    }
    pub(super) fn encode<T: Serialize>(mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        let mut writer = BoundedWriter {
            data: Vec::new(),
            limit: self.remaining,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| capacity())?;
        let charge = self.split(writer.data.capacity())?;
        Ok(ChargedBytes(Arc::new(ChargedData {
            data: writer.data,
            _charge: charge,
        })))
    }
}
impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.arena.used.fetch_sub(self.remaining, Ordering::AcqRel);
        self.arena.settled.notify_waiters();
    }
}
pub(super) struct ScratchBuffer<'a> {
    data: Vec<u8>,
    _reservation: &'a mut ByteReservation,
}
impl ScratchBuffer<'_> {
    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.data
    }
}
struct ChargedData {
    data: Vec<u8>,
    _charge: ByteReservation,
}
/// Immutable bytes and their capacity charge share the same lifetime across
/// log readers, application results, transport frames and cancellation.
#[derive(Clone)]
pub struct ChargedBytes(Arc<ChargedData>);
impl ChargedBytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0.data
    }
    pub fn len(&self) -> usize {
        self.0.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.data.is_empty()
    }
}
impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl fmt::Debug for ChargedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChargedBytes")
            .field("len", &self.len())
            .finish()
    }
}

struct BoundedWriter {
    data: Vec<u8>,
    limit: usize,
}
impl io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .data
            .len()
            .checked_add(bytes.len())
            .filter(|&n| n <= self.limit)
            .ok_or_else(|| io::Error::other("encoded value exceeds reserved bytes"))?;
        if end > self.data.capacity() {
            let grow = end
                .max(self.data.capacity().saturating_mul(2))
                .min(self.limit);
            self.data
                .try_reserve_exact(grow - self.data.len())
                .map_err(io::Error::other)?;
        }
        self.data.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn capacity() -> ProtocolError {
    super::super::failure(
        ErrorCode::LimitExceeded,
        "worker storage capacity unavailable",
    )
}

struct CountingWriter {
    bytes: usize,
    limit: usize,
}
impl io::Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|&n| n <= self.limit)
            .ok_or_else(|| io::Error::other("encoded value exceeds limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
