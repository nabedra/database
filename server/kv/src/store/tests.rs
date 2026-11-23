//! Store unit tests that look at internals.

use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::fs::{FaultFs, Fs, FsFile, FsLock, OpenMode};
use crate::{
    Background, BlockCache, Durability, MemtableBudget, MemtableOptions, Options, Store, WriteBatch,
};

/// Counts live handles of run files.
struct CountingFs {
    inner: FaultFs,
    live_runs: Arc<AtomicUsize>,
}

struct CountedFile {
    inner: Arc<dyn FsFile>,
    live: Option<Arc<AtomicUsize>>,
}

impl Drop for CountedFile {
    fn drop(&mut self) {
        if let Some(live) = &self.live {
            live.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl FsFile for CountedFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
}

impl Fs for CountingFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        let inner = self.inner.open(path, mode)?;
        let live =
            (mode == OpenMode::Read && path.extension().is_some_and(|e| e == "run")).then(|| {
                self.live_runs.fetch_add(1, Ordering::SeqCst);
                self.live_runs.clone()
            });
        Ok(Arc::new(CountedFile { inner, live }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.list(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        self.inner.lock(path)
    }
}

fn small(fs: Arc<dyn Fs>, bg: &Arc<Background>, max_open_runs: usize) -> Options {
    let mut o = Options::new(
        fs,
        Arc::new(BlockCache::new(1 << 20)),
        Arc::new(MemtableBudget::new(8 << 20)),
        bg.clone(),
    );
    o.journal.segment_bytes = 64 << 10;
    o.journal.max_group_bytes = 64 << 10;
    o.memtable_bytes = 16 << 10;
    o.max_replay_bytes = 1 << 30;
    o.levels.l0_compaction_trigger = 2;
    o.levels.target_file_size = 4 << 10;
    o.levels.max_bytes_for_level_base = 16 << 10;
    o.levels.level_size_multiplier = 4;
    o.run_writer.block_size = 512;
    o.memtable = MemtableOptions {
        shard_prefix_len: 0,
        shards: 1,
    };
    o.max_open_runs = max_open_runs;
    o
}

/// RV-KV2 finding 8: a scan over many deeper-level runs keeps at most one run per level
/// open, so the table cache's limit bounds live iterators too.
#[test]
fn ns10_full_scan_respects_the_open_run_limit() {
    let fault = FaultFs::new();
    fault.create_dir_all_durable(Path::new("/db"));
    let live = Arc::new(AtomicUsize::new(0));
    let fs = Arc::new(CountingFs {
        inner: fault,
        live_runs: live.clone(),
    });
    let bg = Background::new(2).unwrap();
    let limit = 8;
    let store = Store::open(Path::new("/db/kv"), small(fs, &bg, limit))
        .unwrap()
        .store;
    let n = 3000u32;
    for i in 0..n {
        let mut b = WriteBatch::new();
        b.put(format!("k{i:06}").as_bytes(), &[b'v'; 64]);
        store
            .submit(b, Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    store.flush().unwrap();
    store.compact().unwrap();
    let deep: usize = store.level_runs()[1..].iter().sum();
    assert!(deep > 4 * limit, "levels {:?}", store.level_runs());
    let snap = store.snapshot();
    let mut it = snap.iter(Bound::Unbounded, Bound::Unbounded, false);
    it.seek_to_first();
    let mut count = 0u32;
    let mut most_open = 0;
    while it.valid() {
        assert_eq!(it.key(), format!("k{count:06}").as_bytes());
        count += 1;
        most_open = most_open.max(live.load(Ordering::SeqCst));
        it.next();
    }
    it.status().unwrap();
    assert_eq!(count, n);
    // The table cache's runs, one more per level the cursor holds past an eviction,
    // and the pinned L0 runs.
    let bound = limit + crate::version::NUM_LEVELS + store.level_runs()[0];
    assert!(
        most_open <= bound,
        "{most_open} run files open, bound {bound}; levels {:?}",
        store.level_runs()
    );
    // Backwards and seeks across run boundaries.
    it.seek_to_last();
    let mut back = 0u32;
    while it.valid() {
        back += 1;
        it.prev();
    }
    assert_eq!(back, n);
    for probe in [0u32, 1, 777, 1500, 2999] {
        let key = format!("k{probe:06}");
        it.seek(key.as_bytes());
        assert_eq!(it.key(), key.as_bytes());
        it.seek_for_prev(key.as_bytes());
        assert_eq!(it.key(), key.as_bytes());
    }
}

/// NS17 (KVfix3): `close` and drop free the memtable charges before they return. A
/// background thread runs a job on an `Arc<Inner>` it upgraded from its queue entry and
/// releases it only after `end_job`, so `shutdown` can return while that thread still
/// holds the store; the last reference then drops on the background thread, later. The
/// clone below stands in for that thread.
#[test]
fn ns17_close_frees_memtable_charges_while_a_background_thread_holds_the_store() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let bg = Background::new(2).unwrap();
    let budget = Arc::new(MemtableBudget::new(8 << 20));
    for close in [true, false] {
        let mut options = small(Arc::new(fs.clone()), &bg, 16);
        options.memtables = budget.clone();
        // Large enough that nothing flushes on its own: an active and an immutable
        // memtable.
        options.memtable_bytes = 1 << 20;
        let store = Store::open(Path::new("/db/kv"), options).unwrap().store;
        for i in 0..64u32 {
            let mut b = WriteBatch::new();
            b.put(format!("k{i:04}").as_bytes(), &[b'v'; 100]);
            store
                .submit(b, Durability::NoSync, Box::new(()))
                .unwrap()
                .wait()
                .unwrap();
            if i == 31 {
                store.inner.freeze_active();
            }
        }
        assert!(budget.used() > 0);
        let background_thread = store.inner.clone();
        if close {
            store.close().unwrap();
        } else {
            drop(store);
        }
        assert_eq!(
            budget.used(),
            0,
            "charged after the store was closed (close: {close})"
        );
        drop(background_thread);
    }
}

/// Occupies a one-thread [`Background`] until released, so a test drives flushes and
/// compactions on its own thread in a fixed order.
struct Blocker {
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
    entered: std::sync::Mutex<bool>,
    entered_wake: std::sync::Condvar,
}

impl Blocker {
    fn occupy(bg: &Arc<Background>) -> Arc<Blocker> {
        let blocker = Arc::new(Blocker {
            released: std::sync::Mutex::new(false),
            wake: std::sync::Condvar::new(),
            entered: std::sync::Mutex::new(false),
            entered_wake: std::sync::Condvar::new(),
        });
        let job: Arc<dyn super::background::Job> = blocker.clone();
        bg.schedule_flush(Arc::downgrade(&job), std::time::Duration::ZERO);
        let mut entered = blocker.entered.lock().unwrap();
        while !*entered {
            entered = blocker.entered_wake.wait(entered).unwrap();
        }
        drop(entered);
        blocker
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

/// Releases the [`Blocker`] when dropped, also while a failed assertion unwinds: the
/// `Background` joins its thread on drop.
struct Release(Arc<Blocker>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl super::background::Job for Blocker {
    fn run_flush(&self) -> Option<std::time::Duration> {
        *self.entered.lock().unwrap() = true;
        self.entered_wake.notify_all();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
        None
    }
    fn run_compaction(&self) -> Option<std::time::Duration> {
        None
    }
}

/// NS03 (KVfix3): a compaction deletes its own inputs before it returns when nothing
/// else pins them. `compact_once` purged obsolete runs while its `pick` still held the
/// inputs, so their files stayed until the next compaction's purge: a snapshot taken in
/// that window holds the new version while `runs/` still lists the old inputs, which
/// made `ns03_snapshot_survives_flush_and_compaction_and_pins_runs` flaky on CI.
#[test]
fn ns03_compaction_deletes_its_unpinned_inputs_before_it_returns() {
    let bg = Background::new(1).unwrap();
    let _release = Release(Blocker::occupy(&bg));
    check_compaction_deletes_unpinned_inputs(&bg);
}

#[cfg(feature = "sim")]
#[test]
fn phase9_sim_compaction_deletes_unpinned_inputs() {
    use nabedra_rt::sim::{self, NodeClock};
    sim::run(3, |world| async move {
        world.add_node(1, NodeClock::default());
        sim::in_node(1, async {
            let bg = Background::new(1).unwrap();
            check_compaction_deletes_unpinned_inputs(&bg);
        })
        .await;
    });
}

fn check_compaction_deletes_unpinned_inputs(bg: &Arc<Background>) {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let store = Store::open(Path::new("/db/kv"), small(Arc::new(fs.clone()), bg, 64))
        .unwrap()
        .store;
    // Two L0 runs reach the L0 trigger (2).
    for round in 0..2u32 {
        for i in 0..50u32 {
            let mut b = WriteBatch::new();
            b.put(
                format!("k{i:04}").as_bytes(),
                format!("v{round}").as_bytes(),
            );
            store
                .submit(b, Durability::NoSync, Box::new(()))
                .unwrap()
                .wait()
                .unwrap();
        }
        store.flush().unwrap();
    }
    assert_eq!(store.level_runs()[0], 2, "levels {:?}", store.level_runs());
    let files = || -> Vec<String> {
        let mut v: Vec<String> = fs
            .list(Path::new("/db/kv/runs"))
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    let inputs = files();
    assert!(store.inner.compact_once().unwrap());
    let mut live: Vec<String> = store
        .inner
        .current_sv()
        .version
        .runs()
        .map(|r| format!("{}.run", r.id()))
        .collect();
    live.sort();
    assert_eq!(
        files(),
        live,
        "after one compaction of {inputs:?}: runs/ must hold exactly the current runs"
    );
    store.close().unwrap();
}

/// NS13, NS11 (KVfix3): a relocation the journal refuses moves nothing, so its segments
/// do not retire. GC marked segments cleared when it submitted their relocation records,
/// without awaiting them; a `NoSpace` roll voids a record's sequences, which still become
/// visible, so the segments retired with their live values unmoved (NS10 long campaign,
/// seed 510: "journal locator names a recycled segment"). The background thread is
/// occupied so this thread runs every flush and GC pass.
#[test]
fn ns13_gc_retires_nothing_for_a_relocation_the_journal_refused() {
    use crate::fs::{Fault, FaultError, FsOp};
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let bg = Background::new(1).unwrap();
    let _release = Release(Blocker::occupy(&bg));
    let mut o = small(Arc::new(fs.clone()), &bg, 64);
    o.journal.segment_bytes = 32 << 10;
    o.journal.max_group_bytes = 16 << 10;
    o.max_journal_bytes = 3 * (32 << 10);
    o.memtable_bytes = 1 << 20;
    let store = Store::open(Path::new("/db/kv"), o).unwrap().store;
    let put = |group: u32, idx: u32, len: usize, d: Durability| {
        let mut b = WriteBatch::new();
        let prefix = format!("g{group:02}/");
        b.set_scope(prefix.as_bytes());
        let mut value = format!("<{group}:{idx}>").into_bytes();
        value.resize(len, b'a');
        b.put_journal(format!("{prefix}e{idx:06}").as_bytes(), &value);
        store.submit(b, d, Box::new(())).unwrap().wait()
    };
    // g00's live values sit in the oldest segment; g01 fills the journal past its budget
    // and deletes what it wrote.
    for idx in 0..6 {
        put(0, idx, 1500, Durability::Sync).unwrap();
    }
    for idx in 0..300 {
        put(1, idx, 900, Durability::NoSync).unwrap();
    }
    let mut b = WriteBatch::new();
    b.set_scope(b"g01/");
    b.delete_range(b"g01/", b"g010");
    store
        .submit(b, Durability::Sync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    store.flush().unwrap();
    // No segment can be prepared any more: small writes fill the active segment until
    // one needs a roll and is refused.
    fs.inject(
        Fault::on(FsOp::Preallocate)
            .error(FaultError::Enospc)
            .repeat(),
    );
    let mut refused = false;
    for idx in 0..5000 {
        match put(2, idx, 300, Durability::NoSync) {
            Ok(_) => {}
            Err(crate::Error::NoSpace) => {
                refused = true;
                break;
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert!(refused, "the journal never ran out of segments");
    // GC relocates g00's values into the full journal and is refused; then later
    // writes are refused too, and their voided sequences become visible.
    for _ in 0..3 {
        let _ = store.inner.journal_gc();
        let _ = put(2, 9999, 300, Durability::NoSync);
    }
    for _ in 0..3 {
        let _ = store.inner.journal_gc();
    }
    assert!(!store.requires_recovery());
    let snap = store.snapshot();
    for idx in 0..6 {
        let key = format!("g00/e{idx:06}");
        let value = snap.get(key.as_bytes());
        assert!(
            matches!(&value, Ok(Some(v)) if v.starts_with(format!("<0:{idx}>").as_bytes())),
            "{key}: {value:?}"
        );
    }
    drop(snap);
    fs.clear_faults();
    store.close().unwrap();
}
