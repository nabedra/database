//! Bounded memory reservations and byte buffers shared by OLTP components.

use std::{
    any::Any,
    collections::VecDeque,
    fmt, io,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use serde::Serialize;
use tokio::sync::Notify;

use crate::{
    protocol::{ErrorCode, Outcome, ProtocolError},
    scheduling::MemoryCharge,
};

pub const RAFT_REPLICA_BYTES: usize = 8 << 20;

pub const DEFAULT_REPLY_DECODE_BYTES: usize = 1 << 20;

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
            reply_decode_bytes: DEFAULT_REPLY_DECODE_BYTES,
            data_apply_reply_bytes: 1 << 20,
            max_open_pools: 8,
            max_group_handles: 128,
            max_resident_groups: 64,
            precharge_group_capacity: false,
        }
    }
}

pub struct ChargedBuffer {
    data: Vec<u8>,
    charge: ByteReservation,
}
impl ChargedBuffer {
    pub fn from_reserved(data: Vec<u8>, charge: ByteReservation) -> Self {
        Self { data, charge }
    }

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
pub struct ByteArena {
    settled: Notify,
    waiting: AtomicUsize,
    closed: AtomicBool,
    capacity: usize,
    used: AtomicUsize,
    _charge: MemoryCharge,
    worker: OnceLock<Weak<dyn Any + Send + Sync>>,
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
    /// Reservations hold this owner while they release charges and wake waiters.
    pub fn bind_owner(&self, worker: Weak<dyn Any + Send + Sync>) {
        let _ = self.worker.set(worker);
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    #[inline]
    pub fn waiters(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }

    #[inline]
    pub fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }

    pub fn new(capacity: usize, charge: MemoryCharge) -> Result<Arc<Self>, ProtocolError> {
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
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.settled.notify_waiters();
    }
    pub async fn reserve_wait(
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
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<ByteReservation, ProtocolError> {
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
pub struct ByteReservation {
    _owner: Arc<dyn Any + Send + Sync>,
    arena: Arc<ByteArena>,
    remaining: usize,
}
impl ByteReservation {
    pub fn allocate(&mut self, len: usize) -> Result<ChargedBuffer, ProtocolError> {
        let charge = self.split(len)?;
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|_| capacity())?;
        data.resize(len, 0);
        Ok(ChargedBuffer { data, charge })
    }
    pub fn reclaim_buffer(&mut self, buffer: ChargedBuffer) -> Result<(), ProtocolError> {
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
    pub fn reclaim_value(&mut self, value: ChargedBytes) -> Result<(), ProtocolError> {
        let owned = Arc::try_unwrap(value.0).map_err(|_| capacity())?;
        self.reclaim_buffer(ChargedBuffer {
            data: owned.data,
            charge: owned._charge,
        })
    }
    pub fn rehome(&mut self, value: ChargedBytes) -> Result<ChargedBytes, ProtocolError> {
        if Arc::ptr_eq(&self.arena, &value.0._charge.arena) {
            Ok(value)
        } else {
            self.copy(value.as_slice())
        }
    }
    pub fn encode_value<T: Serialize>(&mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        let mut count = CountingWriter {
            bytes: 0,
            limit: self.remaining,
        };
        serde_json::to_writer(&mut count, value).map_err(|_| capacity())?;
        self.split(count.bytes)?.encode(value)
    }
    pub fn buffer(&mut self, len: usize) -> Result<ScratchBuffer<'_>, ProtocolError> {
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
    pub fn remaining(&self) -> usize {
        self.remaining
    }
    pub fn split(&mut self, bytes: usize) -> Result<Self, ProtocolError> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(capacity)?;
        Ok(Self {
            arena: self.arena.clone(),
            _owner: self._owner.clone(),
            remaining: bytes,
        })
    }
    pub fn join(&mut self, parts: &[&[u8]]) -> Result<ChargedBytes, ProtocolError> {
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
    pub fn copy(&mut self, bytes: &[u8]) -> Result<ChargedBytes, ProtocolError> {
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
    pub fn encode<T: Serialize>(mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
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
pub struct ScratchBuffer<'a> {
    data: Vec<u8>,
    _reservation: &'a mut ByteReservation,
}
impl ScratchBuffer<'_> {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    pub fn as_slice(&self) -> &[u8] {
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
pub fn capacity() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::LimitExceeded,
        "worker storage capacity unavailable",
        Outcome::NotApplied,
    )
}

pub fn capacity_context(context: String) -> ProtocolError {
    let mut error = capacity();
    error.message.push_str("; ");
    error.message.push_str(&context);
    error
}

pub struct CountingWriter {
    pub bytes: usize,
    pub limit: usize,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::WorkerResources, scheduling::ResourceScheduler};

    #[test]
    fn f03_last_buffer_releases_its_bytes_before_releasing_its_owner() {
        struct Owner {
            arena: Arc<ByteArena>,
            dropped: Arc<AtomicBool>,
        }

        impl Drop for Owner {
            fn drop(&mut self) {
                assert_eq!(self.arena.used(), 0);
                self.dropped.store(true, Ordering::Release);
            }
        }

        let scheduler = ResourceScheduler::new(vec![(1, WorkerResources::default())]).unwrap();
        let arena = ByteArena::new(64, scheduler.charge_memory(1, 64).unwrap()).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let owner: Arc<dyn Any + Send + Sync> = Arc::new(Owner {
            arena: arena.clone(),
            dropped: dropped.clone(),
        });
        arena.bind_owner(Arc::downgrade(&owner));

        let bytes = arena.reserve(23).unwrap().allocate(23).unwrap().freeze();
        let clone = bytes.clone();
        drop(owner);
        drop(bytes);
        assert_eq!(arena.used(), 23);
        assert!(!dropped.load(Ordering::Acquire));
        drop(clone);
        assert!(dropped.load(Ordering::Acquire));
        drop(arena);
        assert_eq!(scheduler.usage(), vec![(1, 0, 0)]);
    }
}
