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
