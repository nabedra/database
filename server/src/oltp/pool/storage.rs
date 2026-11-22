//! Glue between the pool and `nabedra-kv`: worker-shared engine resources, store
//! options, key scopes, error mapping and a raw iterator with the shape the pool's
//! readers use. It replaces the storage helpers the pool used to import from the
//! deleted standalone `oltp` engine (plan section 5).
use super::{PoolInner, disk::DiskReservationStats, failure, recovery_unknown};
use crate::protocol::{ErrorCode, ProtocolError};
use bytes::Bytes;
use nabedra_kv::{
    Background, BlockCache, Commit, MemtableBudget, Options, Seq, StoreStats, fs::Fs,
};
use serde::Serialize;
use std::{
    future::Future,
    ops::Bound,
    path::Path,
    pin::pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Wake, Waker},
    thread::Thread,
};
use uuid::Uuid;

/// Directory of the pool's `nabedra-kv` store, under the pool directory.
pub(super) const STORE_DIR: &str = "kv";
/// Length of a pool group's scope: `0x40 ‖ group id`.
pub(super) const GROUP_SCOPE_BYTES: usize = 17;

/// Engine resources one worker shares across all of its pools, each charged once
/// (`cache_bytes`, `write_buffer_bytes`, `background_threads`).
pub(super) struct NativeStorage {
    pub(super) cache: Arc<BlockCache>,
    pub(super) memtables: Arc<MemtableBudget>,
    pub(super) background: Arc<Background>,
    /// Open pools, for `/v1/runtime/stats`. Pruned lazily. A pool close takes this
    /// lock around its last-owner check so stats never keep a closing store alive.
    pub(super) pools: Mutex<Vec<Weak<PoolInner>>>,
    /// Test hook: runs inside `stats()` once it holds its pool handles.
    #[cfg(test)]
    pub(super) stats_probe: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl NativeStorage {
    pub(super) fn new(
        cache_bytes: usize,
        write_buffer_bytes: usize,
        background: usize,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            cache: Arc::new(BlockCache::new(cache_bytes)),
            memtables: Arc::new(MemtableBudget::new(write_buffer_bytes)),
            // F9: the OS refusing a thread fails the worker's setup, never panics.
            background: Background::new(background).map_err(|error| {
                tracing::error!(%error, "could not start the OLTP storage background threads");
                failure(
                    ErrorCode::StorageUnavailable,
                    "could not start the OLTP storage background threads",
                )
            })?,
            pools: Mutex::new(Vec::new()),
            #[cfg(test)]
            stats_probe: Mutex::new(None),
        })
    }
    pub(super) fn register(&self, pool: &Arc<PoolInner>) {
        let mut pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
        pools.retain(|pool| pool.strong_count() > 0);
        pools.push(Arc::downgrade(pool));
    }
    pub(super) fn disk_stats(&self) -> Vec<DiskReservationStats> {
        // A close checks for outstanding pool handles under this lock.
        let mut stats = std::collections::BTreeMap::new();
        let mut last_owned = Vec::new();
        {
            let pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
            for pool in pools.iter().filter_map(Weak::upgrade) {
                let device = pool.disk.device();
                stats.entry(device).or_insert_with(|| pool.disk.stats());
                last_owned.extend(Arc::into_inner(pool));
            }
        }
        drop(last_owned);
        stats.into_values().collect()
    }
    /// Sums the open pools' store counters. Every handle `stats()` takes is also
    /// released under the registry lock, so `close_with_native`'s last-owner check
    /// under that lock stays exact. When the other owners dropped meanwhile and
    /// `stats()` holds the last handle, the store closes only after the lock is
    /// released: a close waits for flushes and compactions and joins the journal
    /// threads, and must not block `register` or other stats calls (RV-NS6 F11).
    pub(super) fn stats(&self) -> StorageStats {
        let mut total = StorageStats::default();
        let mut last_owned = Vec::new();
        {
            let pools = self.pools.lock().unwrap_or_else(|p| p.into_inner());
            let open: Vec<Arc<PoolInner>> = pools.iter().filter_map(Weak::upgrade).collect();
            #[cfg(test)]
            if let Some(probe) = self
                .stats_probe
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
            {
                probe();
            }
            for pool in open {
                total.add(&pool.store.stats(), pool.store.quarantined().len());
                last_owned.extend(Arc::into_inner(pool));
            }
        }
        drop(last_owned);
        total
    }
}

/// Store counters summed over a worker's open pools (`/v1/runtime/stats`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StorageStats {
    pub pools: u32,
    /// Real journal fsyncs (`fdatasync`, or `F_FULLFSYNC` on macOS).
    pub journal_syncs: u64,
    pub file_syncs: u64,
    pub commits: u64,
    pub synced_commits: u64,
    pub group_commits: u64,
    pub user_bytes: u64,
    pub journal_bytes: u64,
    pub flush_bytes: u64,
    pub compaction_bytes: u64,
    pub relocated_bytes: u64,
    pub stall_ms: u64,
    pub memtable_bytes: u64,
    pub l0_runs: u32,
    /// The largest per-store L0 peak since open (PERF1).
    pub l0_runs_peak: u32,
    /// `Store::write_delay` pacing at the L0 slowdown point (PERF1).
    pub write_delays: u64,
    pub write_delay_us: u64,
    pub recovery_ms: u64,
    pub corruption_detected: u64,
    pub tail_truncations: u64,
    pub quarantined_groups: u32,
    /// Journal records the manifest expected but the last open did not find.
    pub journal_behind_manifest: u64,
    /// Reads that met a locator into a recycled journal segment (each latches).
    pub recycled_value_reads: u64,
    /// Store memory charged to the pool overhead.
    pub overhead_bytes: u64,
}
impl StorageStats {
    fn add(&mut self, stats: &StoreStats, quarantined: usize) {
        self.pools += 1;
        self.journal_syncs += stats.journal_syncs;
        self.file_syncs += stats.file_syncs;
        self.commits += stats.commits;
        self.synced_commits += stats.synced_commits;
        self.group_commits += stats.group_commits;
        self.user_bytes += stats.user_bytes;
        self.journal_bytes += stats.journal_bytes;
        self.flush_bytes += stats.flush_bytes;
        self.compaction_bytes += stats.compaction_bytes;
        self.relocated_bytes += stats.relocated_bytes;
        self.stall_ms += stats.stall_ms;
        self.memtable_bytes += stats.memtable_bytes;
        self.l0_runs += stats.l0_runs;
        self.l0_runs_peak = self.l0_runs_peak.max(stats.l0_runs_peak);
        self.write_delays += stats.write_delays;
        self.write_delay_us += stats.write_delay_us;
        self.recovery_ms = self.recovery_ms.max(stats.recovery_ms);
        self.corruption_detected += stats.corruption_detected;
        self.tail_truncations += stats.tail_truncations;
        self.quarantined_groups += quarantined as u32;
        self.journal_behind_manifest += stats.journal_behind_manifest;
        self.recycled_value_reads += stats.recycled_value_reads;
        self.overhead_bytes += stats.overhead_bytes;
    }
}

/// A key's scope: its group (`0x40 ‖ id`) or store-wide (the pool identity key).
pub(super) fn scope_of(key: &[u8]) -> usize {
    if key.len() >= GROUP_SCOPE_BYTES && key[0] == 0x40 {
        GROUP_SCOPE_BYTES
    } else {
        0
    }
}
pub(super) fn group_scope(id: Uuid) -> [u8; GROUP_SCOPE_BYTES] {
    let mut scope = [0u8; GROUP_SCOPE_BYTES];
    scope[0] = 0x40;
    scope[1..].copy_from_slice(id.as_bytes());
    scope
}
/// The first key after every key that starts with `prefix`.
pub(super) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    nabedra_kv::batch::prefix_successor(prefix).expect("pool prefixes have a finite successor")
}

/// Store options around the worker-shared resources. The memtable size and replay
/// window keep the previous engine's per-pool settings: half of `wb` per memtable
/// and a replay window of `wb`.
pub(super) fn store_options(
    storage: &NativeStorage,
    fs: Arc<dyn Fs>,
    write_buffer_bytes: usize,
) -> Options {
    let mut options = Options::new(
        fs,
        storage.cache.clone(),
        storage.memtables.clone(),
        storage.background.clone(),
    );
    options.scope_of = scope_of;
    options.memtable_bytes = (write_buffer_bytes / 2).max(1 << 20);
    options.max_replay_bytes = (write_buffer_bytes as u64).max(4 << 20);
    if hold_compaction() {
        hold_compaction_levels(&mut options.levels);
    }
    options
}

/// Benchmark only (PERF1): `NABEDRA_BENCH_HOLD_COMPACTION=1` holds compaction off,
/// so a soak can measure what compaction costs foreground writes. L0 then grows
/// without bound and point reads slow down; never set it in production.
const HOLD_COMPACTION_ENV: &str = "NABEDRA_BENCH_HOLD_COMPACTION";
fn hold_compaction() -> bool {
    let hold = std::env::var_os(HOLD_COMPACTION_ENV).is_some_and(|value| value == "1");
    if hold {
        tracing::warn!("{HOLD_COMPACTION_ENV}=1: compaction is held off (benchmark only)");
    }
    hold
}
/// Raises every L0 trigger out of reach: no L0 compaction (so no deeper level
/// fills either), no write delay and no L0 stop. Flushes, the journal and its
/// fsyncs are unchanged.
fn hold_compaction_levels(levels: &mut nabedra_kv::LevelOptions) {
    levels.l0_compaction_trigger = 1 << 20;
    levels.l0_slowdown_writes = 1 << 20;
    levels.l0_stop_writes = 1 << 20;
}

/// Maps an engine error onto the pool's protocol errors.
pub(super) fn storage_error(error: nabedra_kv::Error) -> ProtocolError {
    match error {
        nabedra_kv::Error::UnsupportedFormat { found } => unsupported_format(&found),
        nabedra_kv::Error::NoSpace => no_space(),
        _ => failure(
            ErrorCode::StorageUnavailable,
            "OLTP storage unavailable or corrupt",
        ),
    }
}
pub(super) fn unsupported_format(found: &str) -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        &format!(
            "unsupported store format: {found}. For RocksDB-era OLTP pools, use the archive export and restore procedure in docs/oltp-rocksdb-upgrade.md"
        ),
    )
}
const NO_SPACE: &str = "OLTP storage device is full; the write was not applied";
pub(crate) fn no_space() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable, NO_SPACE)
}
/// A write refused because the device is full (NS11). Nothing was written, so
/// the caller may retry it unchanged once space frees up. The protocol has no
/// code of its own for it (a new code changes every ORM and old peer), so the
/// message identifies it.
pub(crate) fn is_no_space(error: &ProtocolError) -> bool {
    error.code == ErrorCode::StorageUnavailable && error.message.starts_with(NO_SPACE)
}
/// A failed snapshot file operation: a full device (`ENOSPC`, `EDQUOT`) is the
/// recoverable [`no_space`], anything else stays the generic file error.
pub(super) fn file_error(error: std::io::Error) -> ProtocolError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    ) {
        no_space()
    } else {
        failure(
            ErrorCode::StorageUnavailable,
            "typed physical pool unavailable or identity mismatch",
        )
    }
}

/// The outcome of a failed commit: `NoSpace` wrote nothing; anything else is
/// ambiguous and latches the pool (O29).
pub(super) fn commit_error(error: nabedra_kv::Error) -> (bool, ProtocolError) {
    match error {
        nabedra_kv::Error::NoSpace => (false, no_space()),
        _ => (true, recovery_unknown()),
    }
}

/// Waits for a commit on any thread, including one that drives an async runtime,
/// where `Commit::wait` would panic. Only the synchronous pool paths use it: open,
/// close, snapshot install and export, group creation and retirement stamps, all
/// of which run on an executor thread except open. `GroupStore::mutate` never blocks; it awaits `Commit::done` instead.
///
/// The future is polled outside tokio's cooperative budget (RV-NS6 F1). On a
/// runtime worker whose task has spent its budget, a budgeted poll returns
/// `Pending` and defers its waker to the scheduler until the task yields. This
/// loop never yields, so that waker would never fire and the thread would park
/// forever.
pub(super) fn wait_commit(commit: Commit) -> nabedra_kv::Result<Seq> {
    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(tokio::task::coop::unconstrained(commit.done()));
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        std::thread::park();
    }
}

/// A raw cursor (seek, next, key, value, status) over a `nabedra-kv` iterator,
/// with the shape the pool's readers already use. `key()` is `None` when the cursor is not on a record; damage and
/// quarantine surface through `status()`.
pub(super) struct RawIter(nabedra_kv::Iter);
impl RawIter {
    pub(super) fn new(snapshot: &nabedra_kv::Snapshot, lower: &[u8], fill_cache: bool) -> Self {
        Self(snapshot.iter(Bound::Included(lower), Bound::Unbounded, fill_cache))
    }
    /// A cursor over `[lower, upper)`.
    pub(super) fn bounded(
        snapshot: &nabedra_kv::Snapshot,
        lower: &[u8],
        upper: &[u8],
        fill_cache: bool,
    ) -> Self {
        Self(snapshot.iter(Bound::Included(lower), Bound::Excluded(upper), fill_cache))
    }
    pub(super) fn seek(&mut self, target: &[u8]) {
        self.0.seek(target);
    }
    pub(super) fn next(&mut self) {
        if self.0.valid() {
            self.0.next();
        }
    }
    pub(super) fn valid(&self) -> bool {
        self.0.valid()
    }
    pub(super) fn key(&self) -> Option<&[u8]> {
        self.0.valid().then(|| self.0.key())
    }
    pub(super) fn value(&self) -> Result<Bytes, ProtocolError> {
        if !self.0.valid() {
            return Err(recovery_unknown());
        }
        self.0.value().map_err(storage_error)
    }
    pub(super) fn status(&self) -> Result<(), ProtocolError> {
        self.0.status().map_err(storage_error)
    }
    pub(super) fn into_inner(self) -> nabedra_kv::Iter {
        self.0
    }
}

/// R4 (Q1, NS22): files a RocksDB store or the legacy engine leaves in its
/// directory. A pool directory without a format-2 manifest that holds any of them
/// is refused as an unsupported store format, before anything is touched.
pub(super) fn legacy_store_marker(path: &Path) -> Option<String> {
    let entries = std::fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let legacy = matches!(
            name.as_ref(),
            "CURRENT" | "IDENTITY" | "LOCK" | "LOG" | "NABEDRA_OLTP.json" | "rocksdb"
        ) || name.starts_with("MANIFEST-")
            || name.starts_with("OPTIONS-")
            || name.ends_with(".sst")
            || name.ends_with(".log");
        if legacy {
            return Some(format!(
                "{}: RocksDB or legacy OLTP files ({name})",
                path.display()
            ));
        }
    }
    None
}
