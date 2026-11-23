//! Worker memory accounting independent of storage and executors.
use super::{
    ByteArena, ByteClass, ByteReservation, ChargedBuffer, ChargedBytes, CountingWriter,
    RAFT_REPLICA_BYTES, ScratchLease, SharedResourceConfig, capacity, capacity_context,
};
use crate::{
    config::ExecutionConfig,
    protocol::{ErrorCode, Outcome, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use serde::Serialize;
use std::{
    any::Any,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const POOL_OVERHEAD_BYTES: u64 = 64 << 20;
pub const GROUP_HANDLE_BYTES: usize = 64 << 10;
pub const GROUP_ENDPOINT_BYTES: usize = 64 << 10;

/// Memory and capacity accounting that reservations share with the full worker.
pub struct WorkerMemory {
    config: SharedResourceConfig,
    pool_slots: Arc<Semaphore>,
    handle_slots: Arc<Semaphore>,
    resident_slots: Arc<Semaphore>,
    replica_slots: Arc<Semaphore>,
    group_capacity: Option<Arc<ByteArena>>,
    scheduler: ResourceScheduler,
    scheduler_node_id: u64,
    task_memory_limit_bytes: usize,
    decode_bytes: Arc<AtomicUsize>,
    #[cfg(any(test, feature = "test-hooks"))]
    peak_decode_bytes: AtomicUsize,
    arena: Arc<ByteArena>,
    progress: Arc<ByteArena>,
    apply_query: Arc<ByteArena>,
    apply_control: Arc<ByteArena>,
    reply_query: Arc<ByteArena>,
    reply_control: Arc<ByteArena>,
    reply_decode: Arc<ByteArena>,
    reply_apply: Arc<ByteArena>,
    owner: OnceLock<Weak<dyn Any + Send + Sync>>,
    _native_charge: MemoryCharge,
}

impl WorkerMemory {
    pub fn new(
        config: SharedResourceConfig,
        scheduler: ResourceScheduler,
        scheduler_node_id: u64,
        task_memory_limit_bytes: usize,
        native_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        if config.progress_bytes >= config.control_bytes
            || [
                config.max_open_pools,
                config.max_group_handles,
                config.max_resident_groups,
            ]
            .into_iter()
            .any(|slots| slots == 0 || slots > Semaphore::MAX_PERMITS)
        {
            return Err(capacity());
        }
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
        Ok(Self {
            config,
            pool_slots: Arc::new(Semaphore::new(config.max_open_pools)),
            handle_slots: Arc::new(Semaphore::new(config.max_group_handles)),
            resident_slots: Arc::new(Semaphore::new(config.max_resident_groups)),
            replica_slots: Arc::new(Semaphore::new(config.max_resident_groups)),
            group_capacity,
            scheduler,
            scheduler_node_id,
            task_memory_limit_bytes: task_memory_limit_bytes,
            decode_bytes: Arc::new(AtomicUsize::new(0)),
            #[cfg(any(test, feature = "test-hooks"))]
            peak_decode_bytes: AtomicUsize::new(0),
            arena,
            progress,
            apply_query,
            apply_control,
            reply_query,
            reply_control,
            reply_decode,
            reply_apply,
            owner: OnceLock::new(),
            _native_charge: native_charge,
        })
    }

    pub fn bind_owner(&self, owner: Weak<dyn Any + Send + Sync>) {
        assert!(
            self.owner.set(owner.clone()).is_ok(),
            "worker owner rebound"
        );
        for arena in [
            &self.arena,
            &self.progress,
            &self.apply_query,
            &self.apply_control,
            &self.reply_query,
            &self.reply_control,
            &self.reply_decode,
            &self.reply_apply,
        ] {
            arena.bind_owner(owner.clone());
        }
        if let Some(arena) = &self.group_capacity {
            arena.bind_owner(owner);
        }
    }

    fn owner(&self) -> Result<Arc<dyn Any + Send + Sync>, ProtocolError> {
        self.owner
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(capacity)
    }

    pub fn reserve_pool_slot(&self) -> Result<OwnedSemaphorePermit, ProtocolError> {
        self.pool_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())
    }

    pub fn close_reply_arenas(&self) {
        self.reply_query.close();
        self.reply_control.close();
        self.reply_decode.close();
        self.reply_apply.close();
    }

    #[inline]
    pub async fn reserve_apply(
        &self,
        class: ExecutionClass,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        if bytes > self.config.apply_bytes_per_slot {
            return Err(capacity());
        }
        let arena = match class {
            ExecutionClass::Query => &self.apply_query,
            ExecutionClass::Control => &self.apply_control,
        };
        arena.reserve_wait(bytes).await
    }

    #[inline]
    pub async fn reserve_compute(
        &self,
        request: u64,
        bytes: u64,
    ) -> Result<crate::scheduling::Reservation, ProtocolError> {
        self.scheduler
            .reserve_on(request, 1, self.scheduler_node_id, bytes)
            .await
    }

    #[cfg(feature = "test-hooks")]
    pub fn progress_arena(&self) -> &Arc<ByteArena> {
        &self.progress
    }

    #[cfg(feature = "test-hooks")]
    pub fn group_capacity_arena(&self) -> Option<&ByteArena> {
        self.group_capacity.as_deref()
    }
    pub fn reply_waiters(&self, class: ExecutionClass) -> usize {
        match class {
            ExecutionClass::Query => self.reply_query.waiters(),
            ExecutionClass::Control => self.reply_control.waiters(),
        }
    }
    pub fn apply_bytes_used(&self) -> usize {
        self.apply_query.used() + self.apply_control.used()
    }

    pub fn scheduler(&self) -> ResourceScheduler {
        self.scheduler.clone()
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
        .map(|arena| arena.used())
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

    pub fn lease_group_residency(&self) -> Result<GroupCapacityLease, ProtocolError> {
        let permit = self.reserve_group_residency().map_err(|_| {
            group_slots_exhausted("worker resident-group placement capacity exhausted")
        })?;
        self.charge_group_capacity(permit, GROUP_ENDPOINT_BYTES, true)
    }

    pub fn lease_replica_residency(&self) -> Result<GroupCapacityLease, ProtocolError> {
        let permit = self
            .reserve_replica_residency()
            .map_err(|_| group_slots_exhausted("worker active Raft replica capacity exhausted"))?;
        self.charge_group_capacity(permit, RAFT_REPLICA_BYTES, true)
    }

    pub fn reserve_group_handle(&self) -> Result<GroupCapacityLease, ProtocolError> {
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
        self.arena_for(class).used()
    }
    #[inline]
    fn arena_for(&self, class: ByteClass) -> &Arc<ByteArena> {
        match class {
            ByteClass::Progress => &self.progress,
            ByteClass::Data => &self.arena,
        }
    }
    #[inline]
    pub fn allocate_bytes(
        &self,
        class: ByteClass,
        len: usize,
    ) -> Result<ChargedBuffer, ProtocolError> {
        self.reserve_bytes_in(class, len)?.into_buffer()
    }
    pub fn reserve_scratch(
        &self,
        class: ByteClass,
        bytes: usize,
    ) -> Result<ScratchLease, ProtocolError> {
        match class {
            ByteClass::Progress => Ok(ScratchLease::arena(self.reserve_bytes_in(class, bytes)?)),
            ByteClass::Data => {
                let owner = self.owner()?;
                if bytes > self.task_memory_limit_bytes {
                    return Err(capacity());
                }
                let charge = self
                    .scheduler
                    .charge_memory(self.scheduler_node_id, bytes as u64)?;
                self.account_decode(bytes);
                Ok(ScratchLease::working(
                    owner,
                    charge,
                    self.decode_bytes.clone(),
                ))
            }
        }
    }
    /// Planning may retain bounded derived keys/maps beyond the decoded body.
    /// Add only the missing working charge before those copies are allocated.
    pub fn ensure_scratch_budget(
        &self,
        current: Arc<ScratchLease>,
        required: usize,
    ) -> Result<Arc<ScratchLease>, ProtocolError> {
        let owner = self.owner()?;
        if !current.belongs_to(&owner) || required > self.task_memory_limit_bytes {
            return Err(capacity());
        }
        if current.bytes() >= required {
            return Ok(current);
        }
        let extra = Arc::new(self.reserve_scratch(ByteClass::Data, required - current.bytes())?);
        Ok(Arc::new(ScratchLease::combined(owner, [current, extra])))
    }
    fn account_decode(&self, bytes: usize) {
        let previous = self.decode_bytes.fetch_add(bytes, Ordering::AcqRel);
        #[cfg(any(test, feature = "test-hooks"))]
        self.peak_decode_bytes
            .fetch_max(previous + bytes, Ordering::Relaxed);
        #[cfg(not(any(test, feature = "test-hooks")))]
        let _ = previous;
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn reset_working_decode_peak(&self) {
        self.peak_decode_bytes
            .swap(self.working_decode_bytes(), Ordering::Relaxed);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn peak_working_decode_bytes(&self) -> usize {
        self.peak_decode_bytes.load(Ordering::Relaxed)
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
    #[inline]
    pub fn reserve_bytes_in(
        &self,
        class: ByteClass,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        self.arena_for(class).reserve(bytes)
    }

    #[inline]
    pub async fn reply_budget(
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
    #[inline]
    pub async fn data_apply_reply_budget(
        &self,
        bytes: usize,
    ) -> Result<ByteReservation, ProtocolError> {
        self.reply_apply.reserve_wait(bytes).await
    }
    pub fn data_apply_reply_bytes_used(&self) -> usize {
        self.reply_apply.used()
    }
    #[inline]
    pub async fn reserve_reply_decode(&self, bytes: usize) -> Result<ScratchLease, ProtocolError> {
        Ok(ScratchLease::arena(
            self.reply_decode.reserve_wait(bytes).await?,
        ))
    }
    pub fn reply_decode_bytes_used(&self) -> usize {
        self.reply_decode.used()
    }
    pub async fn reserve_scratch_wait(
        &self,
        class: ByteClass,
        bytes: usize,
        request: u64,
    ) -> Result<ScratchLease, ProtocolError> {
        match class {
            ByteClass::Progress => Ok(ScratchLease::arena(
                self.progress.reserve_wait(bytes).await?,
            )),
            ByteClass::Data => {
                let owner = self.owner()?;
                if bytes > self.task_memory_limit_bytes {
                    return Err(capacity());
                }
                let charge = self
                    .scheduler
                    .reserve_memory(request, self.scheduler_node_id, bytes as u64)
                    .await?;
                self.account_decode(bytes);
                Ok(ScratchLease::working(
                    owner,
                    charge,
                    self.decode_bytes.clone(),
                ))
            }
        }
    }

    pub fn pool_overhead(&self) -> Result<MemoryCharge, ProtocolError> {
        self.scheduler
            .charge_memory(self.scheduler_node_id, POOL_OVERHEAD_BYTES)
    }

    pub fn request_actor_bytes(execution: &ExecutionConfig) -> Option<u64> {
        execution
            .max_pending_requests
            .checked_add(1)?
            .checked_mul(64 << 10)
    }

    pub fn validate_registered_group_capacity(
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
    pub fn resident_group_usage(&self) -> (usize, usize) {
        let limit = self.config.max_resident_groups;
        let free = self
            .resident_slots
            .available_permits()
            .min(self.replica_slots.available_permits());
        (limit.saturating_sub(free), limit)
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
            .map(|arena| arena.charged_bytes())
            .sum::<u64>()
            + self
                .group_capacity
                .as_ref()
                .map_or(0, |arena| arena.charged_bytes())
    }
}

pub struct GroupCapacityLease {
    _reserved: Option<ByteReservation>,
    _charged: Option<MemoryCharge>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug)]
pub enum ExecutionClass {
    Query,
    Control,
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
