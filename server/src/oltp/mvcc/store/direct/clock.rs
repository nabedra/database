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
//!
//! Leadership generation (RV-T4 F1, F4): `lead` and `step_down` bump a
//! generation counter. A timestamp is bound to the generation in which it was
//! fetched: the executor takes a `LeaderToken` before asking the Timestamp
//! authority and passes it to `register_pending`, which refuses the token
//! once leadership changed, because another leader may have served a snapshot
//! above the timestamp meanwhile. A read waiting on a pending commit (or on
//! rule 3) fails when the generation changes, because `lead` and `step_down`
//! drop pending entries that may still commit under the next leader.
use super::super::{encode_key, key_length};
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
    /// Rule 3: this node does not lead, no entry of its term applied yet, or
    /// leadership changed since the caller's `LeaderToken` was taken.
    NotServing,
    /// A key cannot be encoded; the command is malformed.
    InvalidKey,
}

/// Proof that this node served as leader when the token was taken. Take it
/// before fetching a commit timestamp and pass it to `register_pending`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LeaderToken {
    term: u64,
    generation: u64,
}
impl LeaderToken {
    pub(crate) fn term(&self) -> u64 {
        self.term
    }
}

#[derive(Default)]
struct ClockState {
    max_read_ts: u64,
    /// Pending commits and the encoded keys each writes.
    pending: BTreeMap<CommitTs, Vec<Vec<u8>>>,
    /// How many pending commits write each encoded key, so a latest read
    /// checks its key in O(1) when woken.
    pending_keys: HashMap<Vec<u8>, usize>,
    /// The term this node leads, if any.
    leader_term: Option<u64>,
    /// The highest term of any entry applied on this node.
    applied_term: u64,
    /// Bumped by every `lead` of a new term and every `step_down`.
    generation: u64,
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
    fn remove(&mut self, commit_ts: CommitTs) -> bool {
        let Some(keys) = self.pending.remove(&commit_ts) else {
            return false;
        };
        for key in keys {
            if let Some(count) = self.pending_keys.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    self.pending_keys.remove(&key);
                }
            }
        }
        true
    }
    fn clear(&mut self) {
        self.pending.clear();
        self.pending_keys.clear();
    }
}

pub(crate) struct RangeClock {
    state: Mutex<ClockState>,
    changed: tokio::sync::Notify,
    /// Keeps the registry key (the store's shared allocation) alive while
    /// any handle to this clock exists, so no other group can reuse it.
    _store: GroupStore,
}

fn encoded(key: &MvccKey) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = vec![0; key_length(key)?];
    encode_key(key, &mut bytes);
    Ok(bytes)
}

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

    /// Distinct keys with a pending writer (RV-T4 F10 bookkeeping).
    #[cfg(test)]
    pub(crate) fn pending_key_count(&self) -> usize {
        self.lock().pending_keys.len()
    }

    /// Rule 1 as a pure check: true when `commit_ts` is at or below an
    /// already-served snapshot, so the caller must fetch a new timestamp.
    /// `register_pending` repeats this check atomically.
    pub(crate) fn needs_new_timestamp(&self, commit_ts: CommitTs) -> bool {
        commit_ts.get() <= self.lock().max_read_ts
    }

    /// The current leadership, or `None` while this node does not serve.
    /// Take it before fetching the commit timestamp.
    pub(crate) fn leader_token(&self) -> Option<LeaderToken> {
        let state = self.lock();
        state.serving().then(|| LeaderToken {
            term: state.leader_term.expect("serving implies a term"),
            generation: state.generation,
        })
    }

    /// Register a commit before proposing it. `token` must be the one taken
    /// before its timestamp was fetched: after a leadership change it is
    /// refused (`NotServing`), even if this node leads again, because a
    /// snapshot above the timestamp may have been served elsewhere meanwhile.
    /// The entry leaves `pending` when its entry applies (the participant
    /// machine settles it), when the caller calls `settle` after a definite
    /// failure (a proposal error, an apply error, or a dropped request), or
    /// through `lead` or `step_down`.
    pub(crate) fn register_pending(
        &self,
        token: LeaderToken,
        commit_ts: CommitTs,
        keys: Vec<MvccKey>,
    ) -> Result<(), ClockRejection> {
        let keys = keys
            .iter()
            .map(encoded)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ClockRejection::InvalidKey)?;
        let mut state = self.lock();
        if !state.serving()
            || state.generation != token.generation
            || state.leader_term != Some(token.term)
        {
            return Err(ClockRejection::NotServing);
        }
        if commit_ts.get() <= state.max_read_ts {
            return Err(ClockRejection::StaleTimestamp {
                max_read_ts: ReadTs::new(state.max_read_ts).expect("positive after a read"),
            });
        }
        if state.pending.contains_key(&commit_ts) {
            return Err(ClockRejection::Duplicate);
        }
        for key in &keys {
            *state.pending_keys.entry(key.clone()).or_default() += 1;
        }
        state.pending.insert(commit_ts, keys);
        Ok(())
    }

    /// Remove a pending commit. Idempotent.
    pub(crate) fn settle(&self, commit_ts: CommitTs) {
        let removed = self.lock().remove(commit_ts);
        if removed {
            self.changed.notify_waiters();
        }
    }

    /// Rule 3: this node now leads `term`. Registrations and snapshot reads
    /// wait until an entry of `term` applies here. Entries this node
    /// registered in an older term are dropped: they are in the log before
    /// any entry of `term`, so they apply before this node serves again.
    /// Reads that waited on them fail, and older tokens are refused.
    pub(crate) fn lead(&self, term: u64) {
        let mut state = self.lock();
        if state.leader_term.is_some_and(|current| current >= term) {
            return;
        }
        state.leader_term = Some(term);
        state.generation += 1;
        state.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    /// Leadership was lost. Entries this node proposed may still commit under
    /// the next leader, which reads them from its applied state. Reads that
    /// waited here fail instead of reading without them, and every token
    /// taken so far is refused.
    pub(crate) fn step_down(&self) {
        let mut state = self.lock();
        state.leader_term = None;
        state.generation += 1;
        state.clear();
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

    /// Rule 2 for a snapshot read at `read_ts`. `u64::MAX` is the "latest"
    /// sentinel some callers use. It is not a timestamp, so it waits for every
    /// pending commit without raising `max_read_ts`.
    pub(crate) async fn observe_snapshot(&self, read_ts: ReadTs) -> Result<(), ProtocolError> {
        let bound = read_ts.get();
        let generation = {
            // Raise before waiting, so a commit cannot register under a read
            // that is still waiting for an earlier one or for rule 3.
            let mut state = self.lock();
            if bound != u64::MAX {
                state.max_read_ts = state.max_read_ts.max(bound);
            }
            state.generation
        };
        self.wait_since(generation, |state| {
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
        let key = encoded(key)?;
        let generation = self.lock().generation;
        self.wait_since(generation, |state| !state.pending_keys.contains_key(&key))
            .await
    }

    async fn wait_since(
        &self,
        generation: u64,
        ready: impl Fn(&ClockState) -> bool,
    ) -> Result<(), ProtocolError> {
        let deadline = tokio::time::Instant::now() + RANGE_CLOCK_WAIT;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.lock();
                // Checked first: `lead` and `step_down` empty `pending`, so
                // `ready` would hold although the awaited commit may still
                // commit under another leader.
                if state.generation != generation {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "range leadership changed while a read waited for an in-flight direct commit",
                    ));
                }
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
