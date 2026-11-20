//! NS02 (atomic batches under concurrent readers and writers with flush and compaction
//! forced) and NS17 (random sleeps in every I/O path, cancelled callers) at store level.

mod common;

use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use common::*;
use nabedra_kv::fs::{FaultFs, Fs, FsFile, FsLock, OpenMode};
use nabedra_kv::{Background, Durability, MemtableBudget, Store, WriteBatch};

const KEYS: usize = 12;

fn batch_keys() -> Vec<Vec<u8>> {
    (0..KEYS)
        .map(|i| match i % 6 {
            5 => format!("s/atomic{i:02}").into_bytes(),
            g => group_key(g as u64, 500 + i as u64),
        })
        .collect()
}

/// Aborts the process if a test hangs (a deadlock must fail, not wait forever).
fn watchdog(limit: Duration) -> Arc<AtomicBool> {
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    std::thread::spawn(move || {
        let start = Instant::now();
        while !flag.load(Ordering::Acquire) {
            if start.elapsed() > limit {
                eprintln!("watchdog: no progress after {limit:?}, probable deadlock");
                std::process::abort();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    });
    done
}

fn read_ids(snap: &nabedra_kv::Snapshot, keys: &[Vec<u8>]) -> Vec<Option<u64>> {
    keys.iter()
        .map(|k| {
            snap.get(k).unwrap().map(|v| {
                std::str::from_utf8(&v[..v.iter().position(|&b| b == b':').unwrap()])
                    .unwrap()
                    .parse()
                    .unwrap()
            })
        })
        .collect()
}

/// NS02: 8 writers, 16 readers, flush and compaction forced continuously. No read sees
/// part of a batch, and a snapshot taken after a commit completes sees it.
#[test]
fn ns02_no_reader_sees_part_of_a_batch_through_flush_and_compaction() {
    let done = watchdog(Duration::from_secs(240));
    let (fs, dir) = fault_fs();
    let store = Arc::new(open(
        &dir,
        small_options(Arc::new(fs), Background::new(3).unwrap()),
    ));
    let keys = Arc::new(batch_keys());
    let next_id = Arc::new(AtomicU64::new(1));
    let completed_seq = Arc::new(AtomicU64::new(0));
    let writers_done = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for w in 0..8u64 {
        let (store, keys, next_id, completed_seq, writers_done) = (
            store.clone(),
            keys.clone(),
            next_id.clone(),
            completed_seq.clone(),
            writers_done.clone(),
        );
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(w + 100);
            for n in 0..250 {
                // Flush and compaction forced every N operations, on top of the
                // maintenance thread and the automatic ones.
                if w == 0 && n % 25 == 24 {
                    store.flush().unwrap();
                }
                if w == 1 && n % 50 == 49 {
                    store.compact().unwrap();
                }
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let mut b = WriteBatch::new();
                if rng.chance(20) {
                    // Covers every batch key, group and store-wide.
                    b.delete_range(b"g", b"t");
                }
                if !rng.chance(5) {
                    for k in keys.iter() {
                        let mut v = format!("{id}:").into_bytes();
                        v.resize(10 + rng.below(300) as usize, b'x');
                        if rng.chance(40) {
                            b.put_journal(k, &v);
                        } else {
                            b.put(k, &v);
                        }
                    }
                }
                let d = if rng.chance(25) {
                    Durability::Sync
                } else {
                    Durability::NoSync
                };
                let seq = store.submit(b, d, Box::new(())).unwrap().wait().unwrap();
                completed_seq.fetch_max(seq, Ordering::AcqRel);
            }
            writers_done.fetch_add(1, Ordering::AcqRel);
        }));
    }
    for r in 0..16u64 {
        let (store, keys, completed_seq, writers_done, reads) = (
            store.clone(),
            keys.clone(),
            completed_seq.clone(),
            writers_done.clone(),
            reads.clone(),
        );
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(r);
            while writers_done.load(Ordering::Acquire) < 8 {
                let floor = completed_seq.load(Ordering::Acquire);
                let snap = store.snapshot();
                assert!(snap.seq() >= floor, "a completed commit is not visible");
                let ids = if rng.chance(50) {
                    read_ids(&snap, &keys)
                } else {
                    // A scan in either direction over the batch keys.
                    let mut it = snap.iter(Bound::Unbounded, Bound::Unbounded, false);
                    let mut found = std::collections::BTreeMap::new();
                    let forward = rng.chance(50);
                    if forward {
                        it.seek_to_first()
                    } else {
                        it.seek_to_last()
                    }
                    while it.valid() {
                        if keys.iter().any(|k| k.as_slice() == it.key()) {
                            let v = it.value().unwrap();
                            let id: u64 = std::str::from_utf8(
                                &v[..v.iter().position(|&b| b == b':').unwrap()],
                            )
                            .unwrap()
                            .parse()
                            .unwrap();
                            found.insert(it.key().to_vec(), id);
                        }
                        if forward { it.next() } else { it.prev() }
                    }
                    it.status().unwrap();
                    keys.iter().map(|k| found.get(k).copied()).collect()
                };
                assert!(
                    ids.iter().all(|i| *i == ids[0]),
                    "reader saw part of a batch: {ids:?}"
                );
                reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let maint = {
        let (store, writers_done) = (store.clone(), writers_done.clone());
        std::thread::spawn(move || {
            let mut rounds = 0;
            while writers_done.load(Ordering::Acquire) < 8 {
                store.flush().unwrap();
                store.compact().unwrap();
                rounds += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
            rounds
        })
    };
    for t in threads {
        t.join().unwrap();
    }
    let rounds = maint.join().unwrap();
    let stats = store.stats();
    eprintln!(
        "NS02: 2000 batches, {} reads, {rounds} forced flush+compaction rounds, flush {} B, compaction {} B",
        reads.load(Ordering::Relaxed),
        stats.flush_bytes,
        stats.compaction_bytes
    );
    assert!(rounds >= 1 && stats.compaction_bytes > 0);
    done.store(true, Ordering::Release);
}

/// An `Fs` that sleeps at random in its calls, so flush, compaction, retirement and the
/// journal thread interleave differently on every run.
struct SleepyFs {
    inner: FaultFs,
    rng: Mutex<Rng>,
}

impl SleepyFs {
    fn nap(&self) {
        let ms = {
            let mut rng = self.rng.lock().unwrap();
            if rng.chance(8) { rng.below(3) } else { 0 }
        };
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

struct SleepyFile {
    inner: Arc<dyn FsFile>,
    fs: Arc<SleepyFs>,
}

struct SleepyHandle(Arc<SleepyFs>);

impl Fs for SleepyHandle {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        self.0.nap();
        let inner = self.0.inner.open(path, mode)?;
        Ok(Arc::new(SleepyFile {
            inner,
            fs: self.0.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.0.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.0.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.0.inner.list(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        self.0.inner.lock(path)
    }
}

impl FsFile for SleepyFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.fs.nap();
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.fs.nap();
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.fs.nap();
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.fs.nap();
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

/// A batch of 1 to 5 operations confined to group `g`.
fn group_batch(rng: &mut Rng, g: u64, tag: u64) -> ModelBatch {
    let mut ops = Vec::new();
    for i in 0..1 + rng.below(5) {
        let key = group_key(g, rng.below(40));
        let mut value = format!("v{tag}.{i}.").into_bytes();
        value.resize(value.len() + rng.below(400) as usize, b'v');
        ops.push(match rng.below(10) {
            0..=4 => Op::Put(key, value),
            5..=6 => Op::PutJournal(key, value),
            7..=8 => Op::Delete(key),
            _ => {
                let a = rng.below(40);
                let end = if rng.chance(20) {
                    let mut p = group_prefix(g);
                    *p.last_mut().unwrap() += 1;
                    p
                } else {
                    group_key(g, a + rng.below(10))
                };
                Op::DeleteRange(group_key(g, a), end)
            }
        });
    }
    ModelBatch {
        scope: group_prefix(g),
        ops,
    }
}

/// Dropped by the journal thread once the commit is done; checks visibility then.
struct OrderGuard {
    store: Arc<Store>,
    seq: Arc<OnceLock<u64>>,
    checked: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    ok: Arc<AtomicBool>,
}

impl Drop for OrderGuard {
    fn drop(&mut self) {
        if std::thread::current().name() != Some("kv-journal") {
            self.ok.store(false, Ordering::Release);
        }
        if let Some(&seq) = self.seq.get() {
            if self.store.visible_seq() < seq {
                self.ok.store(false, Ordering::Release);
            }
            self.checked.fetch_add(1, Ordering::Relaxed);
        }
        self.dropped.fetch_add(1, Ordering::Release);
    }
}

/// NS17: random sleeps in flush, compaction, retirement and the journal thread, with a
/// tiny memtable budget (stalls) and callers that cancel their `done()` futures. No
/// deadlock, no order guard dropped before its write is visible or off the journal
/// thread, no leaked memtable charge, and the data is right.
#[test]
fn ns17_random_sleeps_and_cancelled_callers() {
    let done = watchdog(Duration::from_secs(240));
    let (fs, dir) = fault_fs();
    let sleepy = Arc::new(SleepyFs {
        inner: fs.clone(),
        rng: Mutex::new(Rng(17)),
    });
    // The budget, not the memtable size, freezes memtables, so writers stall.
    let budget = Arc::new(MemtableBudget::new(48 << 10));
    let mut options = small_options(
        Arc::new(SleepyHandle(sleepy.clone())),
        Background::new(2).unwrap(),
    );
    options.memtables = budget.clone();
    options.memtable_bytes = 1 << 20;
    options.max_replay_bytes = 1 << 20;
    let store = Arc::new(open(&dir, options));
    let checked = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicBool::new(true));
    let submitted = Arc::new(AtomicU64::new(0));
    let models: Arc<Mutex<Vec<(u64, ModelBatch)>>> = Arc::default();
    let mut threads = Vec::new();
    for w in 0..6u64 {
        let (store, checked, dropped, ok, submitted, models) = (
            store.clone(),
            checked.clone(),
            dropped.clone(),
            ok.clone(),
            submitted.clone(),
            models.clone(),
        );
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(w * 31 + 7);
            for i in 0..150u64 {
                // Each writer owns its group, so its batches commute with the others'.
                let b = group_batch(&mut rng, w, w * 1000 + i);
                let seq_cell = Arc::new(OnceLock::new());
                let guard = OrderGuard {
                    store: store.clone(),
                    seq: seq_cell.clone(),
                    checked: checked.clone(),
                    dropped: dropped.clone(),
                    ok: ok.clone(),
                };
                let d = if rng.chance(30) {
                    Durability::Sync
                } else {
                    Durability::NoSync
                };
                let commit = store.submit(b.to_batch(), d, Box::new(guard)).unwrap();
                let _ = seq_cell.set(commit.seq());
                models.lock().unwrap().push((commit.seq(), b));
                submitted.fetch_add(1, Ordering::Relaxed);
                if rng.chance(50) {
                    // A cancelled async caller: poll once, then drop the future.
                    let mut fut = Box::pin(commit.done());
                    let mut cx = Context::from_waker(Waker::noop());
                    let _ = Pin::as_mut(&mut fut).poll(&mut cx);
                    drop(fut);
                } else {
                    commit.wait().unwrap();
                }
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    store.sync().unwrap().wait().unwrap();
    let total = submitted.load(Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(30);
    while dropped.load(Ordering::Acquire) < total {
        assert!(Instant::now() < deadline, "order guards never dropped");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        ok.load(Ordering::Acquire),
        "an order guard dropped before visibility or off the journal thread"
    );
    // Commit order is sequence order; replay the model in it.
    let mut list = models.lock().unwrap().clone();
    list.sort_by_key(|(seq, _)| *seq);
    let mut model = std::collections::BTreeMap::new();
    for (_, b) in &list {
        b.apply(&mut model);
    }
    assert_eq!(dump(&store.snapshot()), model);
    let stats = store.stats();
    eprintln!(
        "NS17: {total} commits, {} guards checked, stall {} ms, l0 {}, flush {} B, compaction {} B",
        checked.load(Ordering::Relaxed),
        stats.stall_ms,
        stats.l0_runs,
        stats.flush_bytes,
        stats.compaction_bytes
    );
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store still shared"));
    store.close().unwrap();
    assert_eq!(budget.used(), 0, "leaked memtable charge");
    done.store(true, Ordering::Release);
}
