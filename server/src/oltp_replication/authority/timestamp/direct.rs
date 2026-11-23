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
//! * Values leave the window only after a read barrier whose applied leader ID
//!   `(term, node_id)` equals the leader ID of the `ReserveWindow` entry. The
//!   cluster uses openraft's advanced leader IDs, so one term can have several
//!   leaders; comparing the term alone would depend on proposals never being
//!   forwarded. A node that lost and regained leadership, or that caught up
//!   through a snapshot (which skips apply), reserves a new window above the
//!   durable watermark instead of reusing an old one.
//! * If a queued allocation applies between `ReserveWindow` and installing its
//!   window, the caller still gets the first values of that reservation: its
//!   call started before the reservation was proposed, so every allocation that
//!   completed before the call is below them. Only the rest is not installed.
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
/// Reservations retried when `ReserveWindow` meets a stale authority.
const REFILL_ATTEMPTS: usize = 8;
/// A discarded window is reserved again in the background only while this
/// leader served `AllocateDirect` within this long (PERF1).
const PREFILL_IDLE: std::time::Duration = std::time::Duration::from_secs(30);
/// Bound on one background reservation.
const PREFILL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);
/// The background task checks this often that its handle still exists.
const PREFILL_POLL: std::time::Duration = std::time::Duration::from_secs(1);

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
    /// False when this source knows it cannot answer now: an in-process
    /// replica whose Raft names another node as leader (PERF2a). A routing
    /// hint only; a true answer still needs the leader's lease barrier.
    fn may_answer(&self) -> bool {
        true
    }
}

/// The part of the durable Timestamp state whose change invalidates a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StateMark {
    pub(super) high_watermark: u64,
    pub(super) authority: Authority,
    pub(super) allocator_epoch: u64,
}

/// A committed leader ID, `(term, node_id)`, ordered as openraft orders them.
type LeaderKey = (u64, u64);
fn leader_of(position: AppliedPosition) -> LeaderKey {
    (position.term, position.node_id)
}

#[derive(Debug)]
struct Window {
    leader: LeaderKey,
    next: u64,
    last: u64,
}
/// Window counters for tests and stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectWindowStats {
    /// `ReserveWindow` entries this node proposed and applied.
    pub reservations: u64,
    /// Windows dropped before they were exhausted.
    pub discards: u64,
    /// Reservations that lost the race to a queued allocation. The caller got
    /// the first values; the rest were skipped.
    pub raced: u64,
}
#[derive(Default)]
struct WindowState {
    observed: Option<StateMark>,
    window: Option<Window>,
}

/// In-memory window state shared by every `TimestampMachine` and
/// `TimestampHandle` opened on the same group store.
/// Never handed out beyond the machines that own the store: the registry key
/// is only unique while one of them keeps the store alive.
pub(super) struct DirectWindows {
    state: Mutex<WindowState>,
    refill: tokio::sync::Mutex<()>,
    /// Fired when an apply discards a window (a queued allocation moved the
    /// watermark) or a reservation lost that race. Its own `Arc`, so the
    /// background task that waits on it never keeps these windows alive.
    prefill: Arc<tokio::sync::Notify>,
    /// Milliseconds since `served_epoch` of the last `AllocateDirect` that
    /// passed this node's read barrier; 0 before the first.
    served_ms: AtomicU64,
    served_epoch: std::time::Instant,
    gate: RwLock<Option<Arc<dyn DirectTimestampGate>>>,
    window_size: AtomicU64,
    reservations: AtomicU64,
    discards: AtomicU64,
    raced: AtomicU64,
}
impl DirectWindows {
    fn new() -> Self {
        Self {
            state: Mutex::new(WindowState::default()),
            refill: tokio::sync::Mutex::new(()),
            prefill: Arc::default(),
            served_ms: AtomicU64::new(0),
            served_epoch: nabedra_rt::time::now_std(),
            gate: RwLock::new(None),
            window_size: AtomicU64::new(DEFAULT_DIRECT_WINDOW),
            reservations: AtomicU64::new(0),
            discards: AtomicU64::new(0),
            raced: AtomicU64::new(0),
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
            self.prefill.notify_one();
        }
    }
    fn served(&self) {
        let ms = u64::try_from(self.served_epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.served_ms.store(ms.max(1), Ordering::Relaxed);
    }
    fn recently_served(&self) -> bool {
        let served = self.served_ms.load(Ordering::Relaxed);
        let now = u64::try_from(self.served_epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        served != 0 && now.saturating_sub(served) <= PREFILL_IDLE.as_millis() as u64
    }
    /// A window reserved by `leader` is installed.
    fn holds(&self, leader: LeaderKey) -> bool {
        self.lock()
            .window
            .as_ref()
            .is_some_and(|window| window.leader == leader)
    }
    fn discard(&self, state: &mut WindowState) {
        if state.window.take().is_some() {
            self.discards.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn take_locked(
        &self,
        state: &mut WindowState,
        leader: LeaderKey,
        count: u64,
    ) -> Option<DirectTimestampRange> {
        let window = state.window.as_mut()?;
        if window.leader != leader {
            // A window from an older leader is stale. A newer one was reserved
            // after this caller's barrier; it stays for current callers.
            if window.leader < leader {
                self.discard(state);
            }
            return None;
        }
        if window.last - window.next + 1 < count {
            self.discard(state);
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
    /// Hand out `count` values when the window was reserved by `leader`, the
    /// leader ID applied at the caller's read barrier.
    pub(super) fn take(&self, leader: LeaderKey, count: u64) -> Option<DirectTimestampRange> {
        let mut state = self.lock();
        self.take_locked(&mut state, leader, count)
    }
    /// Hand the first `count` values of the reservation `[first, last]`, made
    /// by `leader`, to the caller. Install the rest as the window only if the
    /// reservation is still the newest applied state; otherwise skip it.
    pub(super) fn install_and_take(
        &self,
        leader: LeaderKey,
        authority: Authority,
        first: u64,
        last: u64,
        count: u64,
    ) -> Result<DirectTimestampRange, ProtocolError> {
        let (head, rest) = DirectTimestampRange::from_bounds(first, last)?.split_first(count)?;
        let mut state = self.lock();
        let current = state
            .observed
            .is_some_and(|mark| mark.high_watermark == last && mark.authority == authority);
        if !current {
            self.raced.fetch_add(1, Ordering::Relaxed);
            self.prefill.notify_one();
            return Ok(head);
        }
        self.discard(&mut state);
        state.window = rest.map(|rest| Window {
            leader,
            next: rest.first().get(),
            last,
        });
        Ok(head)
    }
    /// Install the whole reservation `[first, last]` made by `leader` as the
    /// window, under the same rule as [`Self::install_and_take`]: only if it is
    /// still the newest applied state. Nothing is handed out here.
    fn install(&self, leader: LeaderKey, authority: Authority, first: u64, last: u64) -> bool {
        let mut state = self.lock();
        let current = state
            .observed
            .is_some_and(|mark| mark.high_watermark == last && mark.authority == authority);
        if !current {
            self.raced.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.discard(&mut state);
        state.window = Some(Window {
            leader,
            next: first,
            last,
        });
        true
    }
    fn gate(&self) -> Option<Arc<dyn DirectTimestampGate>> {
        self.gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
    pub(super) fn stats(&self) -> DirectWindowStats {
        DirectWindowStats {
            reservations: self.reservations.load(Ordering::Relaxed),
            discards: self.discards.load(Ordering::Relaxed),
            raced: self.raced.load(Ordering::Relaxed),
        }
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
    /// A copy of the window counters. The shared window itself stays inside
    /// the machines that own the store.
    pub(crate) fn direct_window_stats(&self) -> DirectWindowStats {
        self.machine.direct.stats()
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
        let _total = crate::oltp_runtime::stage_timing::perf_stage!("ts.allocate.total").timer();
        // Plan section 5: a valid leader lease, else ReadIndex (PERF1). The
        // window rule below still compares the applied leader ID. RD1: the
        // lease is checked on this task and the applied position comes from
        // consensus memory: the window is in-memory state, which the machine
        // updates before an entry counts as applied, so a store read of the
        // applied position adds nothing to it.
        let applied = crate::oltp_runtime::stage_timing::timed(
            crate::oltp_runtime::stage_timing::perf_stage!("ts.allocate.barrier"),
            self.consensus.lease_read_position(),
        )
        .await?;
        let leader = leader_of(applied);
        let direct = &self.machine.direct;
        direct.served();
        if let Some(range) = direct.take(leader, count) {
            return Ok(range);
        }
        let _refill_timer =
            crate::oltp_runtime::stage_timing::perf_stage!("ts.allocate.refill").timer();
        let _refill = direct.refill.lock().await;
        if let Some(range) = direct.take(leader, count) {
            return Ok(range);
        }
        let (leader, authority, first, last) = self.reserve_window(count).await?;
        direct.install_and_take(leader, authority, first, last, count)
    }
    /// One `ReserveWindow` of at least `count` values, retried on a stale
    /// authority. The caller holds `refill`.
    async fn reserve_window(
        &self,
        count: u64,
    ) -> Result<(LeaderKey, Authority, u64, u64), ProtocolError> {
        let direct = &self.machine.direct;
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
                    return Ok((
                        leader_of(applied.position),
                        authority,
                        first.get(),
                        last.get(),
                    ));
                }
                TimestampReply::Rejected(super::Fault::StaleAuthority) => {}
                reply => return Err(super::handle::rejected(reply)),
            }
        }
        Err(ProtocolError::new(
            ErrorCode::StorageUnavailable,
            format!(
                "no direct timestamp window after {REFILL_ATTEMPTS} reservation attempts; \
                 any reserved values were skipped and none were issued"
            ),
            Outcome::NotApplied,
        ))
    }
    /// Reserve a new window ahead of the next caller after an apply discarded
    /// the last one (PERF1). The same reservation and install rules as a
    /// caller's refill apply; it hands nothing out, so a window installed here
    /// is used only by callers whose own barrier names its leader. Values in a
    /// window that a later queued allocation discards are skipped, never issued.
    pub(crate) async fn prefill_direct_window(&self) -> Result<bool, ProtocolError> {
        let direct = &self.machine.direct;
        if !direct.recently_served() {
            return Ok(false);
        }
        let barrier = self.consensus.ensure_linearizable_lease().await?;
        let leader = leader_of(barrier.applied);
        let _refill = direct.refill.lock().await;
        if direct.holds(leader) {
            return Ok(false);
        }
        let (leader, authority, first, last) = self.reserve_window(1).await?;
        Ok(direct.install(leader, authority, first, last))
    }
    /// Starts the task that keeps a window reserved while this node serves
    /// `AllocateDirect`: after an apply discards the window, it reserves the
    /// next one in the background, so a direct commit after a queued
    /// allocation does not wait for a `ReserveWindow` entry (PERF1). The task
    /// holds no strong reference while it waits and ends with the handle.
    pub(crate) fn spawn_direct_prefill(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let handle = Arc::downgrade(self);
        let wake = self.machine.direct.prefill.clone();
        nabedra_rt::task::spawn(async move {
            loop {
                if nabedra_rt::time::timeout(PREFILL_POLL, wake.notified())
                    .await
                    .is_err()
                {
                    if handle.strong_count() == 0 {
                        return;
                    }
                    continue;
                }
                let Some(handle) = handle.upgrade() else {
                    return;
                };
                let started = nabedra_rt::time::now_std();
                match nabedra_rt::time::timeout(PREFILL_DEADLINE, handle.prefill_direct_window()).await {
                    Ok(Ok(true)) => {
                        crate::oltp_runtime::stage_timing::perf_stage!("ts.window.prefilled")
                            .since(started);
                    }
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(code = ?error.code, message = %error.message,
                            "direct timestamp window prefill failed");
                    }
                    Err(_) => tracing::debug!("direct timestamp window prefill timed out"),
                }
            }
        })
    }
}
impl DirectTimestampSource for TimestampHandle {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        Box::pin(TimestampHandle::allocate_direct(self, count))
    }
    /// A follower would fail the lease read and then a ReadIndex: two Raft
    /// core round trips per call. Skip it while it names another leader.
    fn may_answer(&self) -> bool {
        match (self.consensus.current_leader(), self.consensus.local_node()) {
            (Some(leader), Some(node)) => leader == node,
            _ => true,
        }
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
