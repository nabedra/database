//! In-memory read/commit ordering for one range on its leader (plan section 5).
//!
//! Rule 1: a direct commit registers its timestamp before proposing, and the
//! registration fails when the timestamp is at or below a snapshot that was
//! already served. Rule 2: every snapshot read at `s` raises `max_read_ts` to
//! `s` and waits until no registered commit is at or below `s`. Both happen
//! under one mutex, so a commit either registers before the read (and the read
//! waits for it) or after it (and must take a newer timestamp).
//!
//! Rule 3: a new leader serves nothing until its first entry of the new term
//! applies. The executor calls `lead(term)` when it becomes leader and feeds
//! `observe_applied` from the Raft metrics, because Raft's blank and
//! membership entries never reach `ParticipantMachine::apply`. Until an entry
//! of `term` applies, registrations fail with `NotServing` and the leader's
//! snapshot reads wait. A clock that never saw `lead` refuses every
//! registration, so a direct commit cannot start on a node that has not
//! proven it caught up.
//!
//! Why rule 1 survives a restart, although `max_read_ts` lives in memory and
//! starts empty: a read at `s` is served only after every commit registered
//! at or below `s` applied, so every commit the old process could still land
//! at or below a served snapshot was already applied, and is in the log
//! before any entry of a later term. A restarted node leads only in a new
//! term and serves nothing until an entry of that term applies, which first
//! applies every earlier entry (the replay that RV-T2 C2 notes may lag after a
//! crash). Timestamps it allocates afterwards come from the Timestamp
//! authority, which issues values above every value issued before in real
//! time, so they are above every snapshot already served.
use crate::{
    oltp::{failure, mvcc::MvccKey, pool::GroupStore},
    oltp_replication::types::{AppliedPosition, CommitTs, ReadTs},
    protocol::{ErrorCode, ProtocolError},
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::Duration,
};

/// A read never waits longer than this for an in-flight commit. A pending
/// entry that outlives it was leaked by its executor, so the read fails
/// explicitly instead of hanging.
pub(crate) const RANGE_CLOCK_WAIT: Duration = Duration::from_secs(10);

/// Why a commit could not register.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClockRejection {
    /// Rule 1: `commit_ts <= max_read_ts`. Take a new timestamp and retry.
    StaleTimestamp { max_read_ts: ReadTs },
    /// The same timestamp is already pending on this range.
    Duplicate,
    /// Rule 3: this node does not lead, or no entry of its term applied yet.
    NotServing,
    /// A split (or other range change) paused new direct commits.
    Paused,
}

#[derive(Default)]
struct ClockState {
    max_read_ts: u64,
    pending: BTreeMap<CommitTs, Vec<MvccKey>>,
    /// The term this node leads, if any.
    leader_term: Option<u64>,
    /// The highest term of any entry applied on this node.
    applied_term: u64,
    paused: usize,
}
impl ClockState {
    fn held(&self) -> bool {
        self.leader_term
            .is_some_and(|term| self.applied_term < term)
    }
    fn serving(&self) -> bool {
        self.leader_term
            .is_some_and(|term| self.applied_term >= term)
    }
}

pub(crate) struct RangeClock {
    state: Mutex<ClockState>,
    changed: tokio::sync::Notify,
    /// Keeps the registry key (the store's shared allocation) alive while
    /// any handle to this clock exists, so no other group can reuse it.
    _store: GroupStore,
}

/// While alive, `register_pending` refuses new commits with `Paused`.
#[must_use]
pub(crate) struct CommitPause(Arc<RangeClock>);
impl Drop for CommitPause {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.paused -= 1;
    }
}

#[allow(dead_code)] // The leader executor (T5) is the main caller.
impl RangeClock {
    /// Every `ParticipantMachine` over one group store shares a clock, so the
    /// executor that registers commits and the readers see one state.
    pub(crate) fn for_store(store: &GroupStore) -> Arc<Self> {
        static REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<RangeClock>>>> = OnceLock::new();
        let key = std::ptr::from_ref(store.identity()) as usize;
        let mut registry = REGISTRY
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.retain(|_, clock| clock.strong_count() > 0);
        if let Some(clock) = registry.get(&key).and_then(Weak::upgrade) {
            return clock;
        }
        let clock = Arc::new(Self {
            state: Mutex::default(),
            changed: tokio::sync::Notify::new(),
            _store: store.clone(),
        });
        registry.insert(key, Arc::downgrade(&clock));
        clock
    }

    fn lock(&self) -> MutexGuard<'_, ClockState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The highest snapshot timestamp served on this range, if any.
    pub(crate) fn max_read_ts(&self) -> Option<ReadTs> {
        ReadTs::new(self.lock().max_read_ts).ok()
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.lock().pending.len()
    }

    /// Rule 1 as a pure check: true when `commit_ts` is at or below an
    /// already-served snapshot, so the caller must fetch a new timestamp.
    /// `register_pending` repeats this check atomically.
    pub(crate) fn needs_new_timestamp(&self, commit_ts: CommitTs) -> bool {
        commit_ts.get() <= self.lock().max_read_ts
    }

    /// Register a commit before proposing it. It leaves `pending` when its
    /// entry applies (the participant machine settles it), or when the
    /// caller calls `settle` after a definite failure (a proposal error, an
    /// apply error, or a dropped request), or through `step_down`.
    pub(crate) fn register_pending(
        &self,
        commit_ts: CommitTs,
        keys: Vec<MvccKey>,
    ) -> Result<(), ClockRejection> {
        let mut state = self.lock();
        if !state.serving() {
            return Err(ClockRejection::NotServing);
        }
        if state.paused != 0 {
            return Err(ClockRejection::Paused);
        }
        if commit_ts.get() <= state.max_read_ts {
            return Err(ClockRejection::StaleTimestamp {
                max_read_ts: ReadTs::new(state.max_read_ts).expect("positive after a read"),
            });
        }
        if state.pending.contains_key(&commit_ts) {
            return Err(ClockRejection::Duplicate);
        }
        state.pending.insert(commit_ts, keys);
        Ok(())
    }

    /// Remove a pending commit. Idempotent.
    pub(crate) fn settle(&self, commit_ts: CommitTs) {
        let removed = self.lock().pending.remove(&commit_ts).is_some();
        if removed {
            self.changed.notify_waiters();
        }
    }

    /// Rule 3: this node now leads `term`. Registrations and snapshot reads
    /// wait until an entry of `term` applies here. Entries this node
    /// registered in an older term are dropped: they are in the log before
    /// any entry of `term`, so they apply before this node serves again.
    pub(crate) fn lead(&self, term: u64) {
        let mut state = self.lock();
        if state.leader_term.is_some_and(|current| current >= term) {
            return;
        }
        state.leader_term = Some(term);
        state.pending.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    /// Leadership was lost. Entries this node proposed may still commit under
    /// the next leader, which reads them from its applied state; this node no
    /// longer serves direct commits or leader reads, so waiters are released.
    pub(crate) fn step_down(&self) {
        let mut state = self.lock();
        state.leader_term = None;
        state.pending.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    /// True once this node leads and an entry of its term applied.
    pub(crate) fn serving(&self) -> bool {
        self.lock().serving()
    }

    /// Called for every applied entry of this group: by the participant
    /// machine for normal entries and by the executor from the Raft metrics
    /// (`last_applied`) for blank and membership entries.
    pub(crate) fn observe_applied(&self, position: AppliedPosition) {
        let mut state = self.lock();
        if position.term <= state.applied_term {
            return;
        }
        let was_held = state.held();
        state.applied_term = position.term;
        if was_held && !state.held() {
            drop(state);
            self.changed.notify_waiters();
        }
    }

    /// Refuse new registrations until the guard drops. A split freeze is
    /// proposed after `drained` returns, so no direct commit above its cut
    /// can apply first and get the freeze refused.
    pub(crate) fn pause_commits(self: &Arc<Self>) -> CommitPause {
        self.lock().paused += 1;
        CommitPause(self.clone())
    }

    /// Wait until no commit is pending, bounded like a read.
    pub(crate) async fn drained(&self) -> Result<(), ProtocolError> {
        self.wait(|state| state.pending.is_empty()).await
    }

    /// Rule 2 for a snapshot read at `read_ts`. `u64::MAX` is the "latest"
    /// sentinel some callers use. It is not a timestamp, so it waits for every
    /// pending commit without raising `max_read_ts`.
    pub(crate) async fn observe_snapshot(&self, read_ts: ReadTs) -> Result<(), ProtocolError> {
        let bound = read_ts.get();
        if bound != u64::MAX {
            // Raise before waiting, so a commit cannot register under a read
            // that is still waiting for an earlier one or for rule 3.
            let mut state = self.lock();
            state.max_read_ts = state.max_read_ts.max(bound);
        }
        self.wait(|state| {
            state
                .pending
                .first_key_value()
                .is_none_or(|(ts, _)| ts.get() > bound)
        })
        .await
    }

    /// A latest-version read of `key` (plan section 6, `table.get`) waits for
    /// every pending commit that writes it. It raises nothing.
    pub(crate) async fn wait_latest(&self, key: &MvccKey) -> Result<(), ProtocolError> {
        self.wait(|state| state.pending.values().all(|keys| !keys.contains(key)))
            .await
    }

    async fn wait(&self, ready: impl Fn(&ClockState) -> bool) -> Result<(), ProtocolError> {
        let deadline = tokio::time::Instant::now() + RANGE_CLOCK_WAIT;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.lock();
                if !state.held() && ready(&state) {
                    return Ok(());
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "range read waited too long for an in-flight direct commit",
                ));
            }
        }
    }
}
