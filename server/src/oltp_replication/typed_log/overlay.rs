//! PERF2d: entries an `append` returned for before their synced write.
//!
//! `append` publishes its batch here and returns, so replication can read and
//! send the entries while the local sync runs. `IOFlushed` still fires only
//! after that sync, so this replica counts toward a quorum only once the
//! entries are on disk. An entry leaves the overlay only after its write is
//! durable and visible in the store, or when the write failed. Then the group
//! stays failed until a new core reads the log state again.
//!
//! Readers check the overlay before the store: an entry absent from the
//! overlay at that point is either already durable or not appended yet.
//! Applying an entry waits until it is durable here, so the state machine
//! never runs ahead of this replica's own synced log.
//!
//! The commit marker is a recovery hint. `save_committed` queues it for a
//! background writer, so the Raft core hands a commit to the apply worker
//! without waiting for a store write; reads of the marker and the next vote
//! wait for that writer.
use super::super::entry::{CommandData, LogId, OwnedRaftEntry};
use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

pub(super) struct Overlay<D: CommandData> {
    state: Mutex<State<D>>,
    /// Woken whenever entries leave the overlay.
    pub(super) settled: tokio::sync::Notify,
    /// Woken when the commit-marker writer goes idle.
    marker_idle: tokio::sync::Notify,
}
struct State<D: CommandData> {
    entries: BTreeMap<u64, OwnedRaftEntry<D>>,
    /// A commit marker for a pending entry, written after that entry syncs.
    committed: Option<LogId>,
    failed: bool,
    /// The newest commit marker of a durable entry not written yet.
    unsaved: Option<LogId>,
    /// A background marker writer runs.
    writing: bool,
}
impl<D: CommandData> Default for Overlay<D> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                committed: None,
                failed: false,
                unsaved: None,
                writing: false,
            }),
            settled: tokio::sync::Notify::new(),
            marker_idle: tokio::sync::Notify::new(),
        }
    }
}
impl<D: CommandData> Overlay<D> {
    fn lock(&self) -> MutexGuard<'_, State<D>> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
    pub(super) fn failed(&self) -> bool {
        self.lock().failed
    }
    /// Makes a submitted batch readable before its sync.
    pub(super) fn publish(&self, batch: &[OwnedRaftEntry<D>]) {
        let mut state = self.lock();
        for entry in batch {
            state.entries.insert(entry.log_id.index, entry.clone());
        }
    }
    /// Removes `first..=last` once their write is durable (`ok`) or failed.
    /// Returns a deferred commit marker the caller may now write.
    pub(super) fn settle(&self, first: u64, last: u64, ok: bool) -> Option<LogId> {
        let committed = {
            let mut state = self.lock();
            state
                .entries
                .retain(|index, _| *index < first || *index > last);
            if ok {
                state.committed.take_if(|committed| committed.index <= last)
            } else {
                // A failed write leaves no later entry durable; the core stops.
                state.failed = true;
                state.entries.clear();
                state.committed = None;
                None
            }
        };
        self.settled.notify_waiters();
        committed
    }
    pub(super) fn contains(&self, index: u64) -> bool {
        self.lock().entries.contains_key(&index)
    }
    /// The first pending entry in `start..end`.
    pub(super) fn first_from(&self, start: u64, end: Option<u64>) -> Option<OwnedRaftEntry<D>> {
        self.lock()
            .entries
            .range(start..)
            .next()
            .filter(|(index, _)| end.is_none_or(|end| **index < end))
            .map(|(_, entry)| entry.clone())
    }
    /// Defers the commit marker for `id` while its entry is pending. The
    /// marker is a recovery hint; it must not name an entry that a crash
    /// could still lose, so the writer records it after the entry's sync.
    pub(super) fn defer_commit(&self, id: LogId) -> bool {
        let mut state = self.lock();
        if state
            .entries
            .get(&id.index)
            .is_none_or(|entry| entry.log_id != id)
        {
            return false;
        }
        if state.committed.is_none_or(|old| old.index < id.index) {
            state.committed = Some(id);
        }
        true
    }
    /// Queues the commit marker `id` of a durable entry. True when the
    /// caller must start the writer; a running writer picks up the newest.
    pub(super) fn queue_marker(&self, id: LogId) -> bool {
        let mut state = self.lock();
        if state.unsaved.is_none_or(|old| old.index < id.index) {
            state.unsaved = Some(id);
        }
        !std::mem::replace(&mut state.writing, true)
    }
    /// The writer's next marker, or `None` when it goes idle.
    pub(super) fn next_marker(&self) -> Option<LogId> {
        let next = {
            let mut state = self.lock();
            let next = state.unsaved.take();
            state.writing = next.is_some();
            next
        };
        if next.is_none() {
            self.marker_idle.notify_waiters();
        }
        next
    }
    /// Waits until every queued commit marker is written.
    pub(super) async fn markers_written(&self) {
        loop {
            let idle = self.marker_idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if !self.lock().writing {
                return;
            }
            idle.await;
        }
    }
    /// A new core starts on the durable log: forget an earlier failure.
    pub(super) fn reset(&self) {
        let mut state = self.lock();
        state.failed = false;
        state.entries.clear();
        state.committed = None;
    }
}
