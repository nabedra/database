//! Direct-path commit timestamps (`plans/oltp-direct-path.md`, section 5).
//!
//! The Timestamp leader reserves a durable window with one `ReserveWindow`
//! entry and hands values out of it from memory through `AllocateDirect`.
//!
//! Why issued values stay monotonic in real time across both paths:
//! * A window `[first, last]` is installed only after its `ReserveWindow`
//!   applied and raised `high_watermark` to `last`, and only while that is
//!   still the newest local state. No other allocation returns a value inside
//!   it.
//! * Every local apply that moves `high_watermark` (or changes the authority)
//!   discards the window under the mutex that hands values out, before that
//!   command's reply is released. A queued allocation that has completed is
//!   therefore followed only by direct values from a later window.
//! * Values leave the window only after a read barrier whose applied term
//!   equals the term in which the window was reserved. A node that lost and
//!   regained leadership, or that caught up through a snapshot, reserves a new
//!   window above the durable watermark instead of reusing an old one.
// Consumers arrive with T4, T5 and T7; until then only tests call this API.
#![cfg_attr(not(test), allow(dead_code))]
use super::{TimestampCommand, TimestampHandle, TimestampReply};
use crate::{
    oltp::pool::GroupStore,
    oltp_replication::{authority::consensus::ConsensusFuture, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

/// Default number of timestamps one `ReserveWindow` entry reserves.
pub(crate) const DEFAULT_DIRECT_WINDOW: u64 = 4096;
/// Upper bound for a configured window and for one `ReserveWindow` entry.
pub(crate) const MAX_DIRECT_WINDOW: u64 = 1 << 20;
/// Upper bound for one `AllocateDirect` call, local or remote.
pub(crate) const MAX_DIRECT_COUNT: u64 = 4096;
/// Reservations retried when a concurrent queued allocation wins the race
/// between `ReserveWindow` applying and the window being installed.
const REFILL_ATTEMPTS: usize = 8;

/// A contiguous run of commit timestamps from `AllocateDirect`. Every value is
/// above every timestamp returned by an allocation that completed before the
/// `AllocateDirect` call started.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectTimestampRange {
    first: CommitTs,
    last: CommitTs,
}
impl DirectTimestampRange {
    pub(crate) fn new(first: CommitTs, last: CommitTs) -> Result<Self, ProtocolError> {
        if first > last {
            return Err(invalid_range());
        }
        Ok(Self { first, last })
    }
    fn from_bounds(first: u64, last: u64) -> Result<Self, ProtocolError> {
        Self::new(
            Timestamp::new(first).map_err(|_| invalid_range())?,
            Timestamp::new(last).map_err(|_| invalid_range())?,
        )
    }
    pub(crate) fn first(self) -> CommitTs {
        self.first
    }
    pub(crate) fn last(self) -> CommitTs {
        self.last
    }
    pub(crate) fn len(self) -> u64 {
        self.last.get() - self.first.get() + 1
    }
    pub(crate) fn iter(self) -> impl Iterator<Item = CommitTs> {
        (self.first.get()..=self.last.get()).filter_map(|value| Timestamp::new(value).ok())
    }
    /// Reject a decoded range that does not hold exactly `count` values.
    pub(crate) fn expect_len(self, count: u64) -> Result<Self, ProtocolError> {
        if self.first > self.last || self.len() != count {
            return Err(invalid_range());
        }
        Ok(self)
    }
    /// Split off the first `count` values. Returns the head and the remainder.
    pub(crate) fn split_first(self, count: u64) -> Result<(Self, Option<Self>), ProtocolError> {
        if count == 0 || count > self.len() {
            return Err(invalid_range());
        }
        let head_last = self.first.get() + count - 1;
        let head = Self::from_bounds(self.first.get(), head_last)?;
        let rest = (head_last < self.last.get())
            .then(|| Self::from_bounds(head_last + 1, self.last.get()))
            .transpose()?;
        Ok((head, rest))
    }
}

/// Mixed-version gate for `TimestampCommand::ReserveWindow`.
///
/// The Timestamp leader proposes `ReserveWindow` only when the installed gate
/// resolves true. The implementation must return true only when every voter
/// and learner of `timestamps` runs a binary that decodes `ReserveWindow`. The
/// `data.direct.v1` Control capability satisfies this when its advertisement
/// covers every Timestamp-group member, because every binary that advertises
/// it carries this variant. Without an installed gate, `AllocateDirect` fails
/// with `not_applied` and proposes nothing.
pub(crate) trait DirectTimestampGate: Send + Sync {
    fn reserve_window_supported<'a>(
        &'a self,
        timestamps: &'a GroupIdentity,
    ) -> ConsensusFuture<'a, bool>;
}

/// Something that answers `AllocateDirect { count }`: the in-process
/// Timestamp handle (only succeeds on the leader) or the remote client.
pub(crate) trait DirectTimestampSource: Send + Sync {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange>;
}

/// The part of the durable Timestamp state whose change invalidates a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StateMark {
    pub(super) high_watermark: u64,
    pub(super) authority: Authority,
    pub(super) allocator_epoch: u64,
}

#[derive(Debug)]
struct Window {
    term: u64,
    next: u64,
    last: u64,
}
#[derive(Default)]
struct WindowState {
    observed: Option<StateMark>,
    window: Option<Window>,
}

/// In-memory window state shared by every `TimestampMachine` and
/// `TimestampHandle` opened on the same group store.
pub(crate) struct DirectWindows {
    state: Mutex<WindowState>,
    refill: tokio::sync::Mutex<()>,
    gate: RwLock<Option<Arc<dyn DirectTimestampGate>>>,
    window_size: AtomicU64,
    reservations: AtomicU64,
    discards: AtomicU64,
}
impl DirectWindows {
    fn new() -> Self {
        Self {
            state: Mutex::new(WindowState::default()),
            refill: tokio::sync::Mutex::new(()),
            gate: RwLock::new(None),
            window_size: AtomicU64::new(DEFAULT_DIRECT_WINDOW),
            reservations: AtomicU64::new(0),
            discards: AtomicU64::new(0),
        }
    }
    /// The hosted state machine and the service handle are separate
    /// `TimestampMachine` values over one store. Both must see the same
    /// window, so it is keyed by the store's shared group allocation. A live
    /// entry keeps a machine, hence that allocation, alive, so the key cannot
    /// be reused while the entry can still be upgraded.
    pub(super) fn for_store(store: &GroupStore) -> Arc<Self> {
        static REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<DirectWindows>>>> = OnceLock::new();
        let key = std::ptr::from_ref(store.identity()) as usize;
        let mut registry = REGISTRY
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.retain(|_, windows| windows.strong_count() > 0);
        if let Some(windows) = registry.get(&key).and_then(Weak::upgrade) {
            return windows;
        }
        let windows = Arc::new(Self::new());
        registry.insert(key, Arc::downgrade(&windows));
        windows
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, WindowState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    /// Called while applying a committed command that wrote the Timestamp
    /// state. A changed watermark or authority discards the window.
    pub(super) fn observe(&self, mark: StateMark) {
        let mut state = self.lock();
        if state.observed == Some(mark) {
            return;
        }
        state.observed = Some(mark);
        if state.window.take().is_some() {
            self.discards.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn take_locked(
        &self,
        state: &mut WindowState,
        term: u64,
        count: u64,
    ) -> Option<DirectTimestampRange> {
        let window = state.window.as_mut()?;
        let remaining = window.last - window.next + 1;
        if window.term != term || remaining < count {
            state.window = None;
            self.discards.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let first = window.next;
        let last = first + count - 1;
        if last == window.last {
            state.window = None;
        } else {
            window.next = last + 1;
        }
        DirectTimestampRange::from_bounds(first, last).ok()
    }
    /// Hand out `count` values when the window was reserved in `term`.
    pub(super) fn take(&self, term: u64, count: u64) -> Option<DirectTimestampRange> {
        let mut state = self.lock();
        self.take_locked(&mut state, term, count)
    }
    /// Install `[first, last]` if its reservation is still the newest applied
    /// state, then hand out the first `count` values.
    pub(super) fn install_and_take(
        &self,
        term: u64,
        authority: Authority,
        first: u64,
        last: u64,
        count: u64,
    ) -> Option<DirectTimestampRange> {
        let mut state = self.lock();
        if !state
            .observed
            .is_some_and(|mark| mark.high_watermark == last && mark.authority == authority)
            || first == 0
            || first > last
        {
            return None;
        }
        state.window = Some(Window {
            term,
            next: first,
            last,
        });
        self.take_locked(&mut state, term, count)
    }
    fn gate(&self) -> Option<Arc<dyn DirectTimestampGate>> {
        self.gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
    /// Number of `ReserveWindow` entries this node proposed and applied.
    pub(crate) fn reservations(&self) -> u64 {
        self.reservations.load(Ordering::Relaxed)
    }
    /// Number of windows dropped before they were exhausted.
    pub(crate) fn discards(&self) -> u64 {
        self.discards.load(Ordering::Relaxed)
    }
}

thread_local! {
    // Outer `Some` while a Timestamp apply is capturing state writes.
    static CAPTURE: Cell<Option<Option<StateMark>>> = const { Cell::new(None) };
}
/// Records the last Timestamp state written by one synchronous apply closure.
pub(super) struct StateCapture {
    previous: Option<Option<StateMark>>,
}
impl StateCapture {
    pub(super) fn begin() -> Self {
        Self {
            previous: CAPTURE.with(|capture| capture.replace(Some(None))),
        }
    }
    pub(super) fn finish(self) -> Option<StateMark> {
        CAPTURE.with(|capture| capture.get()).flatten()
    }
}
impl Drop for StateCapture {
    fn drop(&mut self) {
        CAPTURE.with(|capture| capture.set(self.previous));
    }
}
pub(super) fn record_state_write(mark: StateMark) {
    CAPTURE.with(|capture| {
        if capture.get().is_some() {
            capture.set(Some(Some(mark)));
        }
    });
}

impl TimestampHandle {
    /// Install the mixed-version predicate that allows `ReserveWindow`.
    pub(crate) fn install_direct_gate(&self, gate: Arc<dyn DirectTimestampGate>) {
        *self
            .machine
            .direct
            .gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(gate);
    }
    /// Configure how many timestamps one `ReserveWindow` entry reserves.
    pub(crate) fn set_direct_window_size(&self, size: u64) -> Result<(), ProtocolError> {
        if size == 0 || size > MAX_DIRECT_WINDOW {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                format!("direct timestamp window must hold 1 to {MAX_DIRECT_WINDOW} values"),
                Outcome::NotApplied,
            ));
        }
        self.machine
            .direct
            .window_size
            .store(size, Ordering::Relaxed);
        Ok(())
    }
    pub(crate) fn direct_windows(&self) -> &Arc<DirectWindows> {
        &self.machine.direct
    }
    /// `AllocateDirect { count }`. Succeeds only on the Timestamp leader.
    pub(crate) async fn allocate_direct(
        &self,
        count: u64,
    ) -> Result<DirectTimestampRange, ProtocolError> {
        if count == 0 || count > MAX_DIRECT_COUNT {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                format!("AllocateDirect count must be 1 to {MAX_DIRECT_COUNT}"),
                Outcome::NotApplied,
            ));
        }
        // The plan asks for ReadPolicy::LeaseRead. The Consensus contract only
        // exposes the stricter ReadIndex barrier today.
        let barrier = self.consensus.ensure_linearizable().await?;
        let direct = &self.machine.direct;
        if let Some(range) = direct.take(barrier.applied.term, count) {
            return Ok(range);
        }
        let _refill = direct.refill.lock().await;
        if let Some(range) = direct.take(barrier.applied.term, count) {
            return Ok(range);
        }
        let gate = direct.gate().ok_or_else(direct_disabled)?;
        if !gate.reserve_window_supported(self.identity()).await? {
            return Err(direct_disabled());
        }
        let size = direct.window_size.load(Ordering::Relaxed).max(count);
        for _ in 0..REFILL_ATTEMPTS {
            let authority = self.local_authority().await?.ok_or_else(not_initialized)?;
            let applied = self
                .submit(TimestampCommand::ReserveWindow {
                    authority,
                    count: size,
                })
                .await?;
            match applied.reply {
                TimestampReply::Window {
                    authority: reserved,
                    first,
                    last,
                } if reserved == authority
                    && last.get().checked_sub(first.get()) == Some(size - 1) =>
                {
                    direct.reservations.fetch_add(1, Ordering::Relaxed);
                    if let Some(range) = direct.install_and_take(
                        applied.position.term,
                        authority,
                        first.get(),
                        last.get(),
                        count,
                    ) {
                        return Ok(range);
                    }
                }
                TimestampReply::Rejected(super::Fault::StaleAuthority) => {}
                reply => return Err(super::handle::rejected(reply)),
            }
        }
        Err(ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "direct timestamp window kept moving under concurrent allocations",
            Outcome::NotApplied,
        ))
    }
}
impl DirectTimestampSource for TimestampHandle {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        Box::pin(TimestampHandle::allocate_direct(self, count))
    }
}

fn direct_disabled() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        "direct timestamp windows are not enabled on every Timestamp replica",
        Outcome::NotApplied,
    )
}
fn not_initialized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "timestamp authority is not initialized",
        Outcome::NotApplied,
    )
}
fn invalid_range() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid direct timestamp range",
        Outcome::NotApplied,
    )
}

#[cfg(test)]
mod tests;
