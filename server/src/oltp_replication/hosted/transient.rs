//! RV-APPLY: transient storage and capacity errors never stop a Raft core.
//!
//! OpenRaft 0.10 stops its core (`Fatal::StorageError`) on every error a log
//! or state-machine write returns. Some of ours are temporary and leave the
//! replica's state exact:
//!
//! - `NoSpace` (NS11): the device is full and the write was voided. It is
//!   retried with capped backoff for as long as it lasts. Meanwhile the core
//!   behaves like a partitioned node, and the leader refuses new proposals
//!   with an explicit `NotApplied` (`RaftConsensus::propose`).
//! - capacity (`Overloaded`, `LimitExceeded`, both `NotApplied`): a try-only
//!   reservation found no room. It is retried for at most
//!   [`CAPACITY_RETRY_LIMIT`]; a reservation that cannot succeed in that time
//!   is returned, the core stops, and the supervisor (`manager/supervision.rs`)
//!   restarts or gives the replica up.
//!
//! Everything else (`RecoveryRequired`, a latched pool, quarantine,
//! corruption) is returned at once. Every retry loop ends when the host shuts
//! down: `RaftConsensus::shutdown` calls [`HostRetry::stop`] before
//! `Raft::shutdown`, so a core blocked in storage never holds shutdown up.
use crate::{
    oltp::pool::is_no_space,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::Serialize;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

const FIRST_DELAY: Duration = Duration::from_millis(50);
const MAX_DELAY: Duration = Duration::from_secs(2);
/// How long a capacity error is retried before it stops the core.
pub(crate) const CAPACITY_RETRY_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Transient {
    NoSpace,
    Capacity,
}
pub(crate) fn transient(error: &ProtocolError) -> Option<Transient> {
    if is_no_space(error) {
        Some(Transient::NoSpace)
    } else if matches!(error.code, ErrorCode::Overloaded | ErrorCode::LimitExceeded)
        && error.outcome == Outcome::NotApplied
    {
        Some(Transient::Capacity)
    } else {
        None
    }
}

/// The Raft write a retry belongs to (`/v1/runtime/stats` `raft.retries`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RaftWrite {
    Apply,
    Append,
    Vote,
    Committed,
    Truncate,
    Purge,
    InstallSnapshot,
    BuildSnapshot,
}
impl RaftWrite {
    fn name(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::Append => "append",
            Self::Vote => "vote",
            Self::Committed => "committed",
            Self::Truncate => "truncate",
            Self::Purge => "purge",
            Self::InstallSnapshot => "install_snapshot",
            Self::BuildSnapshot => "build_snapshot",
        }
    }
}

/// Worker-wide counters of the hosted Raft cores (`/v1/runtime/stats`
/// section `raft`). One instance per `HostedGroupManager`, shared by all its
/// hosts; a host started outside a manager has its own.
#[derive(Default)]
pub(crate) struct RaftHealthStats {
    no_space_retries: AtomicU64,
    capacity_retries: AtomicU64,
    /// Retries per write kind, both causes.
    by_write: [AtomicU64; 8],
    pub(crate) snapshot_builds_deferred: AtomicU64,
    pub(crate) proposals_refused_no_space: AtomicU64,
    pub(crate) replies_dropped: AtomicU64,
    pub(crate) fatal: AtomicU64,
    pub(crate) fatal_transient: AtomicU64,
    pub(crate) fatal_recovery_required: AtomicU64,
    pub(crate) fatal_damaged: AtomicU64,
    pub(crate) restarts: AtomicU64,
    pub(crate) given_up: AtomicU64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct RaftHealthSnapshot {
    pub no_space_retries: u64,
    pub capacity_retries: u64,
    pub retries: std::collections::BTreeMap<&'static str, u64>,
    pub snapshot_builds_deferred: u64,
    pub proposals_refused_no_space: u64,
    pub replies_dropped: u64,
    /// Cores OpenRaft stopped (`Fatal`), by class.
    pub fatal: u64,
    pub fatal_transient: u64,
    pub fatal_recovery_required: u64,
    pub fatal_damaged: u64,
    /// Cores the supervisor restarted on the same log and state.
    pub restarts: u64,
    /// Replicas the supervisor stopped and reported after repeated failures.
    pub given_up: u64,
}
const WRITES: [RaftWrite; 8] = [
    RaftWrite::Apply,
    RaftWrite::Append,
    RaftWrite::Vote,
    RaftWrite::Committed,
    RaftWrite::Truncate,
    RaftWrite::Purge,
    RaftWrite::InstallSnapshot,
    RaftWrite::BuildSnapshot,
];
impl RaftHealthStats {
    fn retried(&self, write: RaftWrite, kind: Transient) {
        match kind {
            Transient::NoSpace => &self.no_space_retries,
            Transient::Capacity => &self.capacity_retries,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.by_write[write as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> RaftHealthSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        RaftHealthSnapshot {
            no_space_retries: load(&self.no_space_retries),
            capacity_retries: load(&self.capacity_retries),
            retries: WRITES
                .iter()
                .map(|write| (write.name(), load(&self.by_write[*write as usize])))
                .collect(),
            snapshot_builds_deferred: load(&self.snapshot_builds_deferred),
            proposals_refused_no_space: load(&self.proposals_refused_no_space),
            replies_dropped: load(&self.replies_dropped),
            fatal: load(&self.fatal),
            fatal_transient: load(&self.fatal_transient),
            fatal_recovery_required: load(&self.fatal_recovery_required),
            fatal_damaged: load(&self.fatal_damaged),
            restarts: load(&self.restarts),
            given_up: load(&self.given_up),
        }
    }
}

/// One host's retry state, shared by its log store and state machine.
pub(crate) struct HostRetry {
    stopping: AtomicBool,
    /// Bumped by every stop, so a loop of a stopped core never outlives it
    /// into the restarted one.
    epoch: AtomicU64,
    /// No policy snapshot build before this instant (F2 deferral).
    defer_snapshots_until: std::sync::Mutex<Option<tokio::time::Instant>>,
    #[cfg(test)]
    pub(crate) fail_exports: std::sync::atomic::AtomicU32,
    /// Test hook: the next applies fail with a non-transient error.
    #[cfg(test)]
    pub(crate) fail_applies: std::sync::atomic::AtomicU32,
    /// Test hook: the next vote saves fail with `NoSpace`.
    #[cfg(test)]
    pub(crate) fail_votes: std::sync::atomic::AtomicU32,
    #[cfg(test)]
    pub(crate) force_no_space: AtomicBool,
    stopped: tokio::sync::Notify,
    /// The last write of this host hit `NoSpace` and no write succeeded since.
    no_space: AtomicBool,
    stats: Arc<RaftHealthStats>,
}
impl Default for HostRetry {
    fn default() -> Self {
        Self::new(Arc::default())
    }
}
impl HostRetry {
    pub(crate) fn new(stats: Arc<RaftHealthStats>) -> Self {
        Self {
            stopping: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            defer_snapshots_until: std::sync::Mutex::new(None),
            #[cfg(test)]
            fail_exports: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            fail_applies: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            fail_votes: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            force_no_space: AtomicBool::new(false),
            stopped: tokio::sync::Notify::new(),
            no_space: AtomicBool::new(false),
            stats,
        }
    }
    pub(crate) fn stats(&self) -> &Arc<RaftHealthStats> {
        &self.stats
    }
    /// Ends every retry and wait of this host; set before `Raft::shutdown`.
    pub(crate) fn stop(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.stopping.store(true, Ordering::Release);
        self.stopped.notify_waiters();
    }
    /// A supervisor restart runs the same log and state again.
    pub(crate) fn resume(&self) {
        self.stopping.store(false, Ordering::Release);
    }
    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }
    fn stopped_since(&self, epoch: u64) -> bool {
        self.is_stopping() || self.epoch.load(Ordering::Acquire) != epoch
    }
    /// A policy snapshot build failed transiently: skip builds for a while
    /// instead of exporting again at every applied entry.
    pub(crate) fn defer_snapshots(&self, delay: Duration) {
        if let Ok(mut until) = self.defer_snapshots_until.lock() {
            *until = Some(tokio::time::Instant::now() + delay);
        }
    }
    pub(crate) fn snapshots_deferred(&self) -> bool {
        self.defer_snapshots_until
            .lock()
            .ok()
            .and_then(|until| *until)
            .is_some_and(|until| tokio::time::Instant::now() < until)
    }
    #[cfg(test)]
    pub(crate) fn take_vote_fault(&self) -> bool {
        take(&self.fail_votes)
    }
    #[cfg(test)]
    pub(crate) fn take_apply_fault(&self) -> bool {
        take(&self.fail_applies)
    }
    #[cfg(test)]
    pub(crate) fn take_export_fault(&self) -> bool {
        take(&self.fail_exports)
    }
    /// True while this host's writes find the device full.
    pub(crate) fn out_of_space(&self) -> bool {
        #[cfg(test)]
        if self.force_no_space.load(Ordering::Acquire) {
            return true;
        }
        self.no_space.load(Ordering::Acquire)
    }
    /// Runs `future` unless the host stops first.
    pub(crate) async fn or_stopped<T>(
        &self,
        future: impl Future<Output = Result<T, ProtocolError>>,
    ) -> Result<T, ProtocolError> {
        let stopped = self.stopped.notified();
        if self.is_stopping() {
            return Err(stopping());
        }
        tokio::select! {
            result = future => result,
            () = stopped => Err(stopping()),
        }
    }
    /// Runs `write` until it succeeds, fails with a non-transient error, a
    /// capacity error outlasts [`CAPACITY_RETRY_LIMIT`], or the host stops.
    /// `write` must be idempotent after a transient failure: a `NoSpace`
    /// write was voided, and a capacity error is raised before writing.
    pub(crate) async fn retry<T, F, Fut>(
        &self,
        what: RaftWrite,
        mut write: F,
    ) -> Result<T, ProtocolError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ProtocolError>>,
    {
        let epoch = self.epoch.load(Ordering::Acquire);
        let started = tokio::time::Instant::now();
        let mut delay = FIRST_DELAY;
        let mut attempts = 0u64;
        loop {
            let error = match write().await {
                Ok(value) => {
                    if attempts > 0 || self.out_of_space() {
                        self.no_space.store(false, Ordering::Release);
                    }
                    if attempts > 0 {
                        tracing::warn!(
                            write = what.name(),
                            attempts,
                            "transient Raft write error cleared"
                        );
                    }
                    return Ok(value);
                }
                Err(error) => error,
            };
            let Some(kind) = transient(&error) else {
                return Err(error);
            };
            if kind == Transient::NoSpace {
                self.no_space.store(true, Ordering::Release);
            }
            if self.stopped_since(epoch)
                || (kind == Transient::Capacity && started.elapsed() >= CAPACITY_RETRY_LIMIT)
            {
                return Err(error);
            }
            self.stats.retried(what, kind);
            attempts += 1;
            if attempts == 1 || attempts.is_power_of_two() {
                tracing::warn!(write = what.name(), ?kind, attempts, %error,
                    "transient Raft write error; retrying without stopping the core");
            }
            let stopped = self.stopped.notified();
            if self.stopped_since(epoch) {
                return Err(error);
            }
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = stopped => return Err(error),
            }
            delay = (delay * 2).min(MAX_DELAY);
        }
    }
}
fn stopping() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "typed Raft host is stopping",
        Outcome::Unknown,
    )
}

/// Takes one from a test fault counter.
#[cfg(test)]
fn take(counter: &std::sync::atomic::AtomicU32) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    while current > 0 {
        match counter.compare_exchange_weak(
            current,
            current - 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(now) => current = now,
        }
    }
    false
}
