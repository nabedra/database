//! Worker-owned physical execution and native-memory resources.
use crate::{
    config::{ExecutionConfig, WorkerResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    fmt, io,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
};
use uuid::Uuid;
static WORKERS: Mutex<BTreeMap<Uuid, Weak<WorkerSharedResources>>> = Mutex::new(BTreeMap::new());
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

const POOL_OVERHEAD_BYTES: u64 = 64 << 20;
const GROUP_HANDLE_BYTES: usize = 64 << 10;
const GROUP_ENDPOINT_BYTES: usize = 64 << 10;
pub(crate) const RAFT_REPLICA_BYTES: usize = 8 << 20;

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
    pub reply_decode_bytes: usize,
    pub data_apply_reply_bytes: usize,
    pub max_open_pools: usize,
    pub max_group_handles: usize,
    pub max_resident_groups: usize,
    pub precharge_group_capacity: bool,
}
impl Default for SharedResourceConfig {
    fn default() -> Self {
        Self {
            query_threads: 1,
            control_threads: 1,
            background_threads: 2,
            cache_bytes: 16 << 20,
            write_buffer_bytes: 16 << 20,
            // One32MiB fixed payload may coexist with its encoded frame.
            control_bytes: 69 << 20,
            progress_bytes: 4 << 20,
            //32MiB encoding +32MiB native copy, plus bounded key/marker framing.
            apply_bytes_per_slot: 65 << 20,
            query_reply_bytes: 8 << 20,
            control_reply_bytes: 1 << 20,
            reply_decode_bytes: 1 << 20,
            data_apply_reply_bytes: 1 << 20,
            max_open_pools: 8,
            max_group_handles: 128,
            max_resident_groups: 64,
            precharge_group_capacity: false,
        }
    }
}

/// One owner per worker, shared by all volumes, groups and RAM queue accounting.
/// The scheduler ID is not a permanent Raft node ID or worker enrollment UUID.
pub struct WorkerSharedResources {
    /// Block cache, memtable budget and flush/compaction threads of every pool.
    pub(super) storage: super::storage::NativeStorage,
    pub(super) config: SharedResourceConfig,
    pub(super) pool_slots: Arc<Semaphore>,
    handle_slots: Arc<Semaphore>,
    resident_slots: Arc<Semaphore>,
    replica_slots: Arc<Semaphore>,
    group_capacity: Option<Arc<ByteArena>>,
    scheduler: ResourceScheduler,
    scheduler_node_id: u64,
    next_native_job: AtomicU64,
    task_memory_limit_bytes: usize,
    decode_bytes: Arc<AtomicUsize>,
    thread_budget: super::super::NativeThreadBudget,
    pub(crate) read_attempts: crate::oltp_replication::service_rpc::ReadAttempts,
    read_replica: OnceLock<Weak<Mutex<crate::oltp_runtime::replica::WorkerReplica>>>,
    read_queue: OnceLock<Weak<crate::oltp_runtime::coordinator::QueueCoordinator>>,
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
    reply_decode: Arc<ByteArena>,
    reply_apply: Arc<ByteArena>,
    queue_claim: Arc<AtomicBool>,
    _native_charge: MemoryCharge,
    leases: Mutex<LeaseState>,
    settled: Notify,
}

pub(crate) struct GroupCapacityLease {
    _reserved: Option<ByteReservation>,
    _charged: Option<MemoryCharge>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Serialize)]
pub(crate) struct BlockCacheUsage {
    configured_bytes: usize,
    usage_bytes: usize,
    pinned_bytes: usize,
}

impl WorkerSharedResources {
    pub fn new(
        scheduler_node_id: u64,
        limits: WorkerResources,
        config: SharedResourceConfig,
    ) -> Result<Arc<Self>, ProtocolError> {
        limits.validate().map_err(|_| capacity())?;
        if config.query_threads == 0
            || config.control_threads == 0
            || !(2..=128).contains(&config.background_threads)
            || config.query_threads as u64 > limits.max_threads
            || config.control_threads > 16
            || config.cache_bytes < 1 << 20
            || config.write_buffer_bytes < 1 << 20
            || config.control_bytes == 0
            || config.apply_bytes_per_slot == 0
            || config.query_reply_bytes == 0
            || config.control_reply_bytes == 0
            || config.reply_decode_bytes == 0
            || config.data_apply_reply_bytes == 0
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
                max_threads: config.query_threads as u64,
                ..limits
            },
        )])?;
        // Stack reservation and native overhead are conservative logical charges.
        // The OS page cache and allocator RSS still require measured deployment limits.
        let thread_budget = super::super::NativeThreadBudget::with_pools(
            config.query_threads,
            config.control_threads,
            config.background_threads,
            config.max_open_pools,
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
        let reply_decode = make_arena(config.reply_decode_bytes)?;
        let reply_apply = make_arena(config.data_apply_reply_bytes)?;
        let group_capacity = if config.precharge_group_capacity {
            let bytes = config
                .max_resident_groups
                .checked_mul(GROUP_ENDPOINT_BYTES + RAFT_REPLICA_BYTES)
                .and_then(|n| {
                    n.checked_add(config.max_group_handles.checked_mul(GROUP_HANDLE_BYTES)?)
                })
                .ok_or_else(group_profile_error)?;
            Some(make_arena(bytes).map_err(|_| group_profile_error())?)
        } else {
            None
        };
        let owner = Arc::new(Self {
            storage: super::storage::NativeStorage::new(
                config.cache_bytes,
                config.write_buffer_bytes,
                config.background_threads as usize,
            )?,
            config,
            pool_slots: Arc::new(Semaphore::new(config.max_open_pools)),
            handle_slots: Arc::new(Semaphore::new(config.max_group_handles)),
            resident_slots: Arc::new(Semaphore::new(config.max_resident_groups)),
            replica_slots: Arc::new(Semaphore::new(config.max_resident_groups)),
            group_capacity,
            scheduler,
            scheduler_node_id,
            next_native_job: AtomicU64::new(1),
            task_memory_limit_bytes: limits.task_memory_limit_bytes as usize,
            decode_bytes: Arc::new(AtomicUsize::new(0)),
            thread_budget,
            read_attempts: Default::default(),
            read_replica: OnceLock::new(),
            read_queue: OnceLock::new(),
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
            reply_decode,
            reply_apply,
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
            &owner.reply_decode,
            &owner.reply_apply,
        ] {
            let _ = arena.worker.set(weak.clone());
        }
        if let Some(arena) = &owner.group_capacity {
            let _ = arena.worker.set(weak);
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
    pub(crate) fn bind_read_replica(
        &self,
        replica: &Arc<Mutex<crate::oltp_runtime::replica::WorkerReplica>>,
    ) -> Result<(), ProtocolError> {
        self.read_replica
            .set(Arc::downgrade(replica))
            .map_err(|_| capacity())
    }

    pub(crate) fn read_replica(
        &self,
    ) -> Result<Arc<Mutex<crate::oltp_runtime::replica::WorkerReplica>>, ProtocolError> {
        self.read_replica
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(capacity)
    }

    pub(crate) fn bind_read_queue(
        &self,
        queue: &Arc<crate::oltp_runtime::coordinator::QueueCoordinator>,
    ) -> Result<(), ProtocolError> {
        self.read_queue.set(Arc::downgrade(queue)).map_err(|_| capacity())
    }

    pub(crate) fn read_queue(
        &self,
    ) -> Result<Arc<crate::oltp_runtime::coordinator::QueueCoordinator>, ProtocolError> {
        self.read_queue.get().and_then(Weak::upgrade).ok_or_else(capacity)
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
    /// Zero denotes an independent internal phase, not one shared logical
    /// request. Give those phases distinct caps so free native lanes can run.
    pub(crate) fn next_native_request_id(&self) -> Result<u64, ProtocolError> {
        let mut current = self.next_native_job.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .filter(|n| *n < (1u64 << 63))
                .ok_or_else(capacity)?;
            match self.next_native_job.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(current | (1u64 << 63)),
                Err(observed) => current = observed,
            }
        }
    }
    pub fn task_memory_limit_bytes(&self) -> usize {
        self.task_memory_limit_bytes
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
            &self.reply_decode,
            &self.reply_apply,
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

    /// Actual live Raft task owners are bounded separately from registered
    /// endpoint groups. A relocation learner may share the logical group ID,
    /// but it still consumes its own task capacity until joined shutdown.
    pub fn reserve_replica_residency(&self) -> Result<OwnedSemaphorePermit, ProtocolError> {
        self.replica_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())
    }

    pub(crate) fn lease_group_residency(&self) -> Result<GroupCapacityLease, ProtocolError> {
        let permit = self.reserve_group_residency().map_err(|_| {
            group_slots_exhausted("worker resident-group placement capacity exhausted")
        })?;
        self.charge_group_capacity(permit, GROUP_ENDPOINT_BYTES, true)
    }

    pub(crate) fn lease_replica_residency(&self) -> Result<GroupCapacityLease, ProtocolError> {
        let permit = self
            .reserve_replica_residency()
            .map_err(|_| group_slots_exhausted("worker active Raft replica capacity exhausted"))?;
        self.charge_group_capacity(permit, RAFT_REPLICA_BYTES, true)
    }

    pub(super) fn reserve_group_handle(&self) -> Result<GroupCapacityLease, ProtocolError> {
        let permit = self.handle_slots.clone().try_acquire_owned().map_err(|_| {
            if self.group_capacity.is_some() {
                group_slots_exhausted("worker storage group-handle capacity exhausted")
            } else {
                capacity()
            }
        })?;
        // The stock handle allowance is covered by the fixed native overhead.
        self.charge_group_capacity(permit, GROUP_HANDLE_BYTES, false)
    }

    fn charge_group_capacity(
        &self,
        permit: OwnedSemaphorePermit,
        bytes: usize,
        charge_default: bool,
    ) -> Result<GroupCapacityLease, ProtocolError> {
        let (reserved, charged) = match &self.group_capacity {
            Some(arena) => (Some(arena.reserve(bytes)?), None),
            None if charge_default => (
                None,
                Some(
                    self.scheduler
                        .charge_memory(self.scheduler_node_id, bytes as u64)?,
                ),
            ),
            None => (None, None),
        };
        Ok(GroupCapacityLease {
            _reserved: reserved,
            _charged: charged,
            _permit: permit,
        })
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
                    charge: Mutex::new(charge),
                    counter: self.decode_bytes.clone(),
                }
            }
        };
        Ok(ScratchLease {
            backing,
            _owner: self.owner()?,
        })
    }
    /// Planning may retain bounded derived keys/maps beyond the decoded body.
    /// Add only the missing working charge before those copies are allocated.
    pub(crate) fn ensure_scratch_budget(
        &self,
        current: Arc<ScratchLease>,
        required: usize,
    ) -> Result<Arc<ScratchLease>, ProtocolError> {
        if !Arc::ptr_eq(&current._owner, &self.owner()?) || required > self.task_memory_limit_bytes
        {
            return Err(capacity());
        }
        if current.bytes() >= required {
            return Ok(current);
        }
        let extra = Arc::new(self.reserve_scratch(ByteClass::Data, required - current.bytes())?);
        Ok(Arc::new(ScratchLease {
            _owner: self.owner()?,
            backing: ScratchBacking::Combined([current, extra]),
        }))
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
        serde_json::to_writer(&mut count, value).map_err(|_| {
            capacity_context(format!(
                "encode count class={class:?} counted={} limit={max_bytes}",
                count.bytes
            ))
        })?;
        let context = || {
            format!(
                "encode class={class:?} requested={} limit={max_bytes}",
                count.bytes
            )
        };
        let reservation = self
            .reserve_bytes_in(class, count.bytes)
            .map_err(|mut error| {
                error.message = format!("{}; {} stage=reserve", error.message, context());
                error
            })?;
        reservation.encode(value).map_err(|mut error| {
            error.message = format!("{}; {} stage=write", error.message, context());
            error
        })
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
    /// Reply decoding cannot borrow the raw-reply arena: fully occupied raw
    /// replies would otherwise wait on their own release. This separately
    /// reserved ceiling retains decoded DTOs through responders and clones.
    /// Acquire it before native codec execution, and bound the lexical/DTO
    /// allocation estimate before deserializing.
    pub(super) async fn data_apply_reply_budget(
        &self,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        self.reply_apply.reserve_wait(bytes).await
    }
    pub fn data_apply_reply_bytes_used(&self) -> usize {
        self.reply_apply.used.load(Ordering::Acquire)
    }
    pub async fn reserve_reply_decode(&self, bytes: usize) -> Result<ScratchLease, ProtocolError> {
        Ok(ScratchLease {
            backing: ScratchBacking::Arena(self.reply_decode.reserve_wait(bytes).await?),
            _owner: self.owner()?,
        })
    }
    pub fn reply_decode_bytes_used(&self) -> usize {
        self.reply_decode.used.load(Ordering::Acquire)
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
                        charge: Mutex::new(charge),
                        counter: self.decode_bytes.clone(),
                    },
                    _owner: self.owner()?,
                })
            }
        }
    }

    pub(super) fn pool_overhead(&self) -> Result<MemoryCharge, ProtocolError> {
        self.scheduler
            .charge_memory(self.scheduler_node_id, POOL_OVERHEAD_BYTES)
    }

    pub(crate) fn request_actor_bytes(execution: &ExecutionConfig) -> Option<u64> {
        execution
            .max_pending_requests
            .checked_add(1)?
            .checked_mul(64 << 10)
    }

    pub(crate) fn validate_registered_group_capacity(
        &self,
        execution: &ExecutionConfig,
        configured_pools: usize,
        system_groups: usize,
    ) -> Result<(), ProtocolError> {
        if self.group_capacity.is_none() {
            return Ok(());
        }
        if system_groups > self.config.max_resident_groups
            || system_groups > self.config.max_group_handles
            || configured_pools > self.config.max_open_pools
        {
            return Err(group_profile_error());
        }
        let headroom = (configured_pools as u64)
            .checked_mul(POOL_OVERHEAD_BYTES)
            .and_then(|n| n.checked_add(self.task_memory_limit_bytes as u64))
            .and_then(|n| n.checked_add(execution.max_queue_bytes_per_replica))
            .and_then(|n| n.checked_add(execution.max_retained_result_bytes_per_worker))
            .and_then(|n| n.checked_add(Self::request_actor_bytes(execution)?))
            // Queue replica bookkeeping and enrolled-member metadata.
            .and_then(|n| n.checked_add(1 << 20))
            .ok_or_else(group_profile_error)?;
        self.scheduler
            .charge_memory(self.scheduler_node_id, headroom)
            .map(drop)
            .map_err(|_| group_profile_error())
    }

    pub fn config(&self) -> SharedResourceConfig {
        self.config
    }
    pub(crate) fn block_cache_usage(&self) -> BlockCacheUsage {
        BlockCacheUsage {
            configured_bytes: self.config.cache_bytes,
            usage_bytes: self.storage.cache.usage_bytes(),
            pinned_bytes: self.storage.cache.pinned_bytes(),
        }
    }
    /// Store counters of every open pool on this worker (`/v1/runtime/stats`).
    pub(crate) fn storage_stats(&self) -> super::StorageStats {
        self.storage.stats()
    }
    pub(crate) fn resident_group_usage(&self) -> (usize, usize) {
        let limit = self.config.max_resident_groups;
        let free = self
            .resident_slots
            .available_permits()
            .min(self.replica_slots.available_permits());
        (limit.saturating_sub(free), limit)
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
                &self.reply_decode,
                &self.reply_apply,
            ]
            .iter()
            .map(|arena| arena._charge.bytes())
            .sum::<u64>()
            + self
                .group_capacity
                .as_ref()
                .map_or(0, |arena| arena._charge.bytes())
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
            self.reply_decode.close();
            self.reply_apply.close();
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
        let grant = prepared.reserve_compute(request, task_bytes).await?;
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
    Combined([Arc<ScratchLease>; 2]),
    Working {
        charge: Mutex<MemoryCharge>,
        counter: Arc<AtomicUsize>,
    },
}
impl ScratchLease {
    /// Only a codec with a checked aggregate retained-heap bound may release
    /// unused decode scratch. Every clone observes the same smaller charge.
    pub(crate) fn shrink_unused(&self, bytes: usize) -> Result<(), ProtocolError> {
        let ScratchBacking::Working { charge, counter } = &self.backing else {
            return Err(capacity());
        };
        let mut charge = charge.lock().map_err(|_| capacity())?;
        let old = charge.bytes() as usize;
        charge.shrink_to(bytes as u64)?;
        counter.fetch_sub(old - bytes, Ordering::AcqRel);
        Ok(())
    }

    pub fn bytes(&self) -> usize {
        match &self.backing {
            ScratchBacking::Arena(charge) => charge.remaining,
            ScratchBacking::Combined(owners) => owners.iter().map(|o| o.bytes()).sum(),
            ScratchBacking::Working { charge, .. } => {
                charge.lock().expect("scratch charge").bytes() as usize
            }
        }
    }
}
impl Drop for ScratchLease {
    fn drop(&mut self) {
        if let ScratchBacking::Working { charge, counter } = &mut self.backing {
            counter.fetch_sub(
                charge.get_mut().expect("scratch charge").bytes() as usize,
                Ordering::AcqRel,
            );
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
pub struct NativeComputeGuard {
    _reservation: Option<crate::scheduling::Reservation>,
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
        let grant = self.reserve_compute(request, task_bytes).await?;
        self.run_with_guard(grant, work).await?.0
    }

    /// Query compute follows max_threads. Control has its separately bounded
    /// physical executor and precharged memory, so a busy query cannot consume
    /// its progress capacity or be counted as an extra query worker.
    pub async fn reserve_compute(
        &self,
        request: u64,
        task_bytes: u64,
    ) -> Result<NativeComputeGuard, ProtocolError> {
        let owner = &self.lease.owner;
        let reservation = match self.class {
            ExecutionClass::Control => {
                if task_bytes != 0 {
                    return Err(capacity());
                }
                None
            }
            ExecutionClass::Query => {
                let request = if request == 0 {
                    owner.next_native_request_id()?
                } else {
                    request
                };
                Some(
                    owner
                        .scheduler
                        .reserve_on(request, 1, owner.scheduler_node_id, task_bytes)
                        .await?,
                )
            }
        };
        Ok(NativeComputeGuard {
            _reservation: reservation,
        })
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
    allocation: Mutex<ArenaAllocation>,
}
struct ArenaAllocation {
    next: u64,
    pending: VecDeque<ArenaWaiter>,
}
struct ArenaWaiter {
    ticket: u64,
    bytes: usize,
    bypassed: u32,
}
const BYTE_BYPASS_LIMIT: u32 = 32;
impl ByteArena {
    fn new(capacity: usize, charge: MemoryCharge) -> Result<Arc<Self>, ProtocolError> {
        Ok(Arc::new(Self {
            capacity,
            used: AtomicUsize::new(0),
            _charge: charge,
            worker: OnceLock::new(),
            allocation: Mutex::new(ArenaAllocation {
                next: 0,
                pending: VecDeque::new(),
            }),
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
        if bytes == 0 {
            return self.reserve(0);
        }
        let ticket = {
            let mut allocation = self.allocation.lock().map_err(|_| capacity())?;
            if self.closed.load(Ordering::Acquire) {
                return Err(capacity());
            }
            let next = allocation.next.checked_add(1).ok_or_else(capacity)?;
            allocation.pending.try_reserve(1).map_err(|_| capacity())?;
            allocation.next = next;
            allocation.pending.push_back(ArenaWaiter {
                ticket: next,
                bytes,
                bypassed: 0,
            });
            self.waiting.fetch_add(1, Ordering::AcqRel);
            next
        };
        let mut waiting = ByteWaiter {
            arena: self.clone(),
            ticket,
            active: true,
        };
        loop {
            let notified = self.settled.notified();
            if self.closed.load(Ordering::Acquire) {
                return Err(capacity());
            }
            if let Ok(reservation) = self.reserve_ticket(bytes, Some(ticket)) {
                waiting.active = false;
                return Ok(reservation);
            }
            notified.await;
        }
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<ByteReservation, ProtocolError> {
        self.reserve_ticket(bytes, None)
    }
    fn reserve_ticket(
        self: &Arc<Self>,
        bytes: usize,
        ticket: Option<u64>,
    ) -> Result<ByteReservation, ProtocolError> {
        let unavailable = |reason: &str| {
            capacity_context(format!(
                "byte arena {reason} requested={bytes} used={} capacity={} waiting={}",
                self.used.load(Ordering::Acquire),
                self.capacity,
                self.waiting.load(Ordering::Acquire)
            ))
        };
        let waiting = |reason: &str| {
            let mut error = unavailable(reason);
            error.code = ErrorCode::Overloaded;
            error
        };
        let owner = self
            .worker
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| unavailable("owner unavailable"))?;
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| unavailable("allocation poisoned"))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable("closed"));
        }
        let free = self.capacity - self.used.load(Ordering::Acquire);
        if bytes > free {
            return Err(unavailable("insufficient bytes"));
        }
        let index = match ticket {
            Some(ticket) => Some(
                allocation
                    .pending
                    .iter()
                    .position(|waiter| waiter.ticket == ticket && waiter.bytes == bytes)
                    .ok_or_else(|| unavailable("waiter unavailable"))?,
            ),
            None => None,
        };
        if bytes != 0 {
            let free_after = free - bytes;
            // Queued grants follow first fit. Immediate grants can use the
            // bytes left over after the first fitting waiter.
            if let Some((first_fit, waiter)) = allocation
                .pending
                .iter()
                .enumerate()
                .find(|(_, waiter)| waiter.bytes <= free)
                && index.map_or(free_after < waiter.bytes, |index| index > first_fit)
            {
                return Err(waiting("older fitting waiter"));
            }
            // After 32 bypasses, freed bytes accumulate for the oldest aged waiter.
            if let Some(protected) = allocation
                .pending
                .iter()
                .position(|waiter| waiter.bypassed >= BYTE_BYPASS_LIMIT)
                && index.is_none_or(|index| index > protected)
                && free_after < allocation.pending[protected].bytes.min(free)
            {
                return Err(waiting("older protected waiter"));
            }
            let older = index.unwrap_or(allocation.pending.len());
            for waiter in allocation
                .pending
                .iter_mut()
                .take(older)
                .filter(|waiter| waiter.bytes > free)
            {
                waiter.bypassed = waiter.bypassed.saturating_add(1).min(BYTE_BYPASS_LIMIT);
            }
        }
        if let Some(index) = index {
            allocation.pending.remove(index);
            self.waiting.fetch_sub(1, Ordering::AcqRel);
        }
        // Grants serialize under allocation; concurrent drops can only free
        // more bytes after the availability observation above.
        self.used.fetch_add(bytes, Ordering::AcqRel);
        drop(allocation);
        self.settled.notify_waiters();
        Ok(ByteReservation {
            arena: self.clone(),
            _owner: owner,
            remaining: bytes,
        })
    }
}
struct ByteWaiter {
    arena: Arc<ByteArena>,
    ticket: u64,
    active: bool,
}
impl Drop for ByteWaiter {
    fn drop(&mut self) {
        if self.active {
            let mut allocation = self.arena.allocation.lock().expect("byte allocation state");
            if let Some(index) = allocation
                .pending
                .iter()
                .position(|waiter| waiter.ticket == self.ticket)
            {
                allocation.pending.remove(index);
                self.arena.waiting.fetch_sub(1, Ordering::AcqRel);
            }
            drop(allocation);
            self.arena.settled.notify_waiters();
        }
    }
}
pub(super) struct ByteReservation {
    _owner: Arc<WorkerSharedResources>,
    arena: Arc<ByteArena>,
    remaining: usize,
}
impl ByteReservation {
    pub(super) fn allocate(&mut self, len: usize) -> Result<ChargedBuffer, ProtocolError> {
        let charge = self.split(len)?;
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|_| capacity())?;
        data.resize(len, 0);
        Ok(ChargedBuffer { data, charge })
    }
    pub(super) fn reclaim_buffer(&mut self, buffer: ChargedBuffer) -> Result<(), ProtocolError> {
        let ChargedBuffer { data, mut charge } = buffer;
        if !Arc::ptr_eq(&self.arena, &charge.arena) {
            return Err(capacity());
        }
        drop(data);
        self.remaining = self
            .remaining
            .checked_add(charge.remaining)
            .ok_or_else(capacity)?;
        charge.remaining = 0;
        Ok(())
    }
    pub(super) fn reclaim_value(&mut self, value: ChargedBytes) -> Result<(), ProtocolError> {
        let owned = Arc::try_unwrap(value.0).map_err(|_| capacity())?;
        self.reclaim_buffer(ChargedBuffer {
            data: owned.data,
            charge: owned._charge,
        })
    }
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

fn group_profile_error() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "registered native group capacity does not fit worker resources",
        Outcome::NotApplied,
    )
}

fn group_slots_exhausted(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Overloaded, message, Outcome::NotApplied)
}

fn capacity_context(context: String) -> ProtocolError {
    let mut error = capacity();
    error.message.push_str("; ");
    error.message.push_str(&context);
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    #[test]
    fn o05_c02_m02_registered_group_capacity_precharges_and_recovers_each_lease() {
        let config = SharedResourceConfig {
            max_resident_groups: 2,
            max_group_handles: 3,
            precharge_group_capacity: true,
            ..Default::default()
        };
        let resources = WorkerSharedResources::new(1, WorkerResources::default(), config).unwrap();
        let arena = resources.group_capacity.as_ref().unwrap();
        let budget = 2 * (GROUP_ENDPOINT_BYTES + RAFT_REPLICA_BYTES) + 3 * GROUP_HANDLE_BYTES;
        let scheduler = resources.scheduler();
        let baseline = scheduler.usage()[0].2;
        assert_eq!(arena.capacity, budget);
        assert_eq!(baseline, resources.native_baseline_bytes());
        let groups = [(); 2].map(|()| resources.lease_group_residency().unwrap());
        let [replica, other_replica] =
            [(); 2].map(|()| resources.lease_replica_residency().unwrap());
        let handles = [(); 3].map(|()| resources.reserve_group_handle().unwrap());
        assert_eq!(arena.used.load(Ordering::Acquire), budget);
        assert_eq!(scheduler.usage()[0].2, baseline);
        for (error, message) in [
            (
                resources.lease_group_residency().err().unwrap(),
                "worker resident-group placement capacity exhausted",
            ),
            (
                resources.lease_replica_residency().err().unwrap(),
                "worker active Raft replica capacity exhausted",
            ),
            (
                resources.reserve_group_handle().err().unwrap(),
                "worker storage group-handle capacity exhausted",
            ),
        ] {
            assert_eq!(error.code, ErrorCode::Overloaded);
            assert_eq!(error.outcome, Outcome::NotApplied);
            assert_eq!(error.message, message);
        }
        drop(replica);
        assert_eq!(
            arena.used.load(Ordering::Acquire),
            budget - RAFT_REPLICA_BYTES
        );
        let replica = resources.lease_replica_residency().unwrap();
        assert_eq!(arena.used.load(Ordering::Acquire), budget);
        drop((groups, replica, other_replica, handles));
        assert_eq!(arena.used.load(Ordering::Acquire), 0);
        assert_eq!(scheduler.usage()[0].2, baseline);
        let execution = ExecutionConfig::default();
        resources
            .validate_registered_group_capacity(&execution, 1, 2)
            .unwrap();
        let error = resources
            .validate_registered_group_capacity(&execution, 1, 3)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ValidationError);
        let stock_limits = WorkerResources {
            memory_limit_bytes: 1536 << 20,
            task_memory_limit_bytes: 256 << 20,
            control_memory_reserve_bytes: 128 << 20,
            ..WorkerResources::default()
        };
        let larger = SharedResourceConfig {
            max_resident_groups: 272,
            max_group_handles: 320,
            ..config
        };
        let error = WorkerSharedResources::new(2, stock_limits, larger)
            .err()
            .unwrap();
        assert_eq!(error.code, ErrorCode::ValidationError);
        let declared = WorkerResources {
            memory_limit_bytes: 3 << 30,
            ..stock_limits
        };
        let fixture_execution = ExecutionConfig {
            max_pending_requests: 16,
            max_queue_bytes_per_replica: 8 << 20,
            max_retained_result_bytes_per_worker: 8 << 20,
            ..execution
        };
        let profile = WorkerSharedResources::new(2, declared, larger).unwrap();
        profile
            .validate_registered_group_capacity(&fixture_execution, 1, 4)
            .unwrap();
        let error = profile
            .validate_registered_group_capacity(&fixture_execution, 8, 4)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ValidationError);
        let impossible = SharedResourceConfig {
            max_resident_groups: Semaphore::MAX_PERMITS,
            ..config
        };
        let error = WorkerSharedResources::new(2, WorkerResources::default(), impossible)
            .err()
            .unwrap();
        assert_eq!(error.code, ErrorCode::ValidationError);
        drop(resources);
        assert_eq!(scheduler.usage()[0].2, 0);
    }

    #[tokio::test]
    async fn m03_c07_f03_immediate_progress_preserves_fitting_waiter_headroom() {
        let resources = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let arena = &resources.progress;
        let used = || resources.buffer_bytes_used(ByteClass::Progress);
        let waiters = || arena.waiting.load(Ordering::Acquire);
        let scheduler = resources.scheduler();
        let baseline = scheduler.usage();
        let retained = resources
            .reserve_scratch(ByteClass::Progress, 176_925)
            .unwrap();
        let blocker = arena.reserve(arena.capacity - retained.bytes()).unwrap();
        let mut older = Box::pin(resources.reserve_scratch_wait(ByteClass::Progress, 16 << 10, 0));
        std::future::poll_fn(|cx| {
            assert!(older.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(waiters(), 1);
        drop(blocker);
        assert_eq!(used(), retained.bytes());

        let progress = resources.reserve_scratch(ByteClass::Progress, 383).unwrap();
        assert_eq!(progress.bytes(), 383);
        assert_eq!(used(), retained.bytes() + 383);
        assert_eq!(arena.allocation.lock().unwrap().pending[0].bypassed, 0);

        let mut younger = Box::pin(resources.reserve_scratch_wait(ByteClass::Progress, 1, 0));
        let granted = std::future::poll_fn(|cx| {
            assert!(younger.as_mut().poll(cx).is_pending());
            assert_eq!(waiters(), 2);
            let Poll::Ready(granted) = older.as_mut().poll(cx) else {
                panic!("older waiter must still fit while progress holds its bytes");
            };
            Poll::Ready(granted.unwrap())
        })
        .await;
        assert_eq!(granted.bytes(), 16 << 10);
        assert_eq!(waiters(), 1);
        drop(younger);
        assert_eq!(waiters(), 0);
        assert_eq!(
            used(),
            retained.bytes() + progress.bytes() + granted.bytes()
        );
        drop((retained, progress, granted));
        assert_eq!(used(), 0);
        assert_eq!(scheduler.usage(), baseline);
    }

    #[tokio::test]
    async fn m03_o19_byte_arena_distinguishes_waiting_from_impossible_work() {
        let resources = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let arena = &resources.progress;
        let held = arena.reserve(arena.capacity).unwrap();
        let mut earlier = std::pin::pin!(arena.reserve_wait(arena.capacity - 382));
        std::future::poll_fn(|cx| {
            assert!(earlier.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(held);
        let blocked = resources
            .reserve_scratch(ByteClass::Progress, 383)
            .err()
            .expect("older waiter gets this space");
        assert_eq!(blocked.code, ErrorCode::Overloaded);
        assert_eq!(blocked.outcome, Outcome::NotApplied);
        assert_eq!(
            blocked.message,
            format!(
                "worker storage capacity unavailable; byte arena older fitting waiter requested=383 used=0 capacity={} waiting=1",
                arena.capacity
            )
        );
        assert_eq!(resources.buffer_bytes_used(ByteClass::Progress), 0);
        let impossible = arena
            .reserve_wait(arena.capacity + 1)
            .await
            .err()
            .expect("request exceeds arena capacity");
        assert_eq!(impossible.code, ErrorCode::LimitExceeded);
        assert_eq!(impossible.outcome, Outcome::NotApplied);
        let remaining = resources.reserve_scratch(ByteClass::Progress, 382).unwrap();
        let earlier = std::future::poll_fn(|cx| {
            let Poll::Ready(granted) = earlier.as_mut().poll(cx) else {
                panic!("older waiter must fit when it has exactly enough bytes");
            };
            Poll::Ready(granted.unwrap())
        })
        .await;
        assert_eq!(
            resources.buffer_bytes_used(ByteClass::Progress),
            arena.capacity
        );
        drop((remaining, earlier));
        assert_eq!(resources.buffer_bytes_used(ByteClass::Progress), 0);
        assert_eq!(arena.waiting.load(Ordering::Acquire), 0);
    }
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
