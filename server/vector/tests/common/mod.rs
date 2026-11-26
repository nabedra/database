//! Shared helpers of the adversarial tests of the NVMe store and the runtime. Written by someone
//! who did not write the code under test.

#![allow(dead_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use nabedra_check::Gen;
use nabedra_rt::fs::os;
use nabedra_rt::fs::uring::Ring;
use nabedra_vector::index::{Config, ExactCodec, Hit, Index, Query, ReadAt};
use nabedra_vector::store::{Backend, Disk, PAGE, Pages, Req, VecKey, Vectors, open};
use nabedra_vector::types::{
    Change, ColumnType, Error, Key, Op, Page, RangeId, Record, Result, Row, Schema, Value,
};

pub const EIO: i32 = 5;
pub const ENOSPC: i32 = 28;

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A scratch directory on the filesystem of the build, which takes `O_DIRECT` (tmpfs does not).
pub struct Tmp(pub PathBuf);

impl Tmp {
    pub fn new(name: &str) -> Tmp {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/vector-adv-tests");
        let d = Path::new(base).join(format!("{name}-{}-{n}", std::process::id()));
        let _ = os::remove_dir_all(&d);
        os::create_dir_all(&d).unwrap();
        Tmp(d)
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = os::remove_dir_all(&self.0);
    }
}

/// The backends to test. A host that blocks io_uring has to say so with `NABEDRA_VECTOR_NO_URING=1`,
/// so a missing ring never passes silently.
pub fn backends() -> Vec<Backend> {
    if Ring::new(8).is_ok() {
        return vec![Backend::Pread, Backend::Uring];
    }
    if std::env::var_os("NABEDRA_VECTOR_NO_URING").is_some() {
        eprintln!("io_uring is unavailable here: its half is skipped");
        return vec![Backend::Pread];
    }
    panic!("io_uring is unavailable; set NABEDRA_VECTOR_NO_URING=1 to skip its half");
}

pub fn kind(b: Backend) -> &'static str {
    if b == Backend::Uring { "io_uring" } else { "pread" }
}

/// A vector that differs by seed and in every dimension.
pub fn vector(dim: usize, seed: u64) -> Vec<f32> {
    (0..dim)
        .map(|i| ((seed.wrapping_mul(31).wrapping_add(i as u64 * 7) % 997) as f32 - 498.0) / 7.0)
        .collect()
}

pub fn bits(v: &[f32]) -> Vec<u32> {
    v.iter().map(|x| x.to_bits()).collect()
}

pub fn key(slot: u32, at: u64) -> VecKey {
    VecKey { slot, at }
}

/// Flips one bit of a file through another descriptor.
pub fn flip(path: &Path, offset: u64) {
    let f = os::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut b = [0u8];
    f.read_exact_at(&mut b, offset).unwrap();
    b[0] ^= 0x10;
    f.write_all_at(&b, offset).unwrap();
    f.sync_data().unwrap();
}

pub fn raw_read(path: &Path, offset: u64, len: usize) -> Vec<u8> {
    let f = os::OpenOptions::new().read(true).open(path).unwrap();
    let mut b = vec![0u8; len];
    f.read_exact_at(&mut b, offset).unwrap();
    b
}

pub fn raw_write(path: &Path, offset: u64, data: &[u8]) {
    let f = os::OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.write_all_at(data, offset).unwrap();
    f.sync_data().unwrap();
}

pub fn truncate(path: &Path, len: u64) {
    let f = os::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len).unwrap();
}

// ----- A disk that misbehaves on demand.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Hurt {
    /// The call fails with this errno.
    Errno(i32),
    /// A write moves only its first `n` pages and then fails (a partial write).
    Partial(usize),
    /// A write reports success and keeps the old second half of every page (a torn write).
    Torn,
    /// A write reports success and writes nothing (a lost write).
    Lost,
    /// A read reports success with the second half of its pages zeroed.
    Zeroed,
    /// A read reports success with one bit of its first page flipped.
    Flipped,
}

#[derive(Clone, Copy, Debug)]
pub struct Fault {
    /// Index of the call (reads and writes counted apart) at which it fires.
    pub at: u64,
    pub hurt: Hurt,
    /// Every call from `at` on.
    pub sticky: bool,
}

#[derive(Default, Debug)]
pub struct Chaos {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub read_fault: Mutex<Option<Fault>>,
    pub write_fault: Mutex<Option<Fault>>,
    /// Every request that broke the rules of `O_DIRECT`.
    pub misaligned: Mutex<Vec<String>>,
    /// (offset, length) of every write.
    pub write_log: Mutex<Vec<(u64, usize)>>,
}

impl Chaos {
    pub fn clear(&self) {
        *lock(&self.read_fault) = None;
        *lock(&self.write_fault) = None;
    }

    fn due(slot: &Mutex<Option<Fault>>, n: u64) -> Option<Hurt> {
        let mut f = lock(slot);
        let fault = (*f)?;
        if n == fault.at || (fault.sticky && n > fault.at) {
            if !fault.sticky {
                *f = None;
            }
            return Some(fault.hurt);
        }
        None
    }
}

#[derive(Debug)]
pub struct ChaosDisk {
    pub inner: Arc<dyn Disk>,
    pub chaos: Arc<Chaos>,
}

fn aligned(ptr: *const u8, offset: u64, len: usize) -> Option<String> {
    let bad = offset % PAGE as u64 != 0 || len % PAGE != 0 || (ptr as usize) % PAGE != 0;
    bad.then(|| format!("offset {offset}, length {len}, buffer at {:#x}", ptr as usize))
}

impl Disk for ChaosDisk {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn read(&self, reqs: &mut [Req]) {
        for r in reqs.iter() {
            if let Some(w) = aligned(r.buf.bytes().as_ptr(), r.offset, r.buf.len()) {
                lock(&self.chaos.misaligned).push(format!("read {w}"));
            }
        }
        let n = self.chaos.reads.fetch_add(1, Ordering::SeqCst);
        match Chaos::due(&self.chaos.read_fault, n) {
            Some(Hurt::Errno(e)) => {
                for r in reqs {
                    r.result = Err(io::Error::from_raw_os_error(e));
                }
            }
            Some(Hurt::Zeroed) => {
                self.inner.read(reqs);
                for r in reqs.iter_mut().filter(|r| r.result.is_ok()) {
                    let len = r.buf.len();
                    for p in 0..len / PAGE {
                        r.buf.bytes_mut()[p * PAGE + PAGE / 2..(p + 1) * PAGE].fill(0);
                    }
                }
            }
            Some(Hurt::Flipped) => {
                self.inner.read(reqs);
                for r in reqs.iter_mut().filter(|r| r.result.is_ok() && !r.buf.is_empty()) {
                    r.buf.bytes_mut()[17] ^= 0x04;
                }
            }
            _ => self.inner.read(reqs),
        }
    }

    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        if let Some(w) = aligned(buf.as_ptr(), offset, buf.len()) {
            lock(&self.chaos.misaligned).push(format!("write {w}"));
        }
        lock(&self.chaos.write_log).push((offset, buf.len()));
        let n = self.chaos.writes.fetch_add(1, Ordering::SeqCst);
        match Chaos::due(&self.chaos.write_fault, n) {
            Some(Hurt::Errno(e)) => Err(io::Error::from_raw_os_error(e)),
            Some(Hurt::Partial(k)) => {
                let k = k.min(buf.len() / PAGE);
                if k > 0 {
                    self.inner.write(offset, &buf[..k * PAGE])?;
                }
                Err(io::Error::from_raw_os_error(EIO))
            }
            Some(Hurt::Lost) => Ok(()),
            Some(Hurt::Torn) => {
                let pages = buf.len() / PAGE;
                let mut old = [Req::new(offset, pages)];
                self.inner.read(&mut old);
                let [old] = old;
                let mut torn = Pages::new(pages);
                for p in 0..pages {
                    let (a, mid, b) = (p * PAGE, p * PAGE + PAGE / 2, (p + 1) * PAGE);
                    torn.bytes_mut()[a..mid].copy_from_slice(&buf[a..mid]);
                    if old.result.is_ok() {
                        torn.bytes_mut()[mid..b].copy_from_slice(&old.buf.bytes()[mid..b]);
                    }
                }
                self.inner.write(offset, torn.bytes())
            }
            _ => self.inner.write(offset, buf),
        }
    }
}

/// `cores` files, each behind a [`ChaosDisk`].
pub struct Files {
    pub tmp: Tmp,
    pub disks: Vec<Arc<dyn Disk>>,
    pub chaos: Vec<Arc<Chaos>>,
}

pub fn files(name: &str, backend: Backend, cores: usize) -> Files {
    let tmp = Tmp::new(name);
    let mut f = Files {
        disks: Vec::new(),
        chaos: Vec::new(),
        tmp,
    };
    for c in 0..cores {
        let inner = open(&f.tmp.join(&format!("raw-{c}")), backend, 2).unwrap();
        assert_eq!(inner.kind(), kind(backend));
        let chaos = Arc::new(Chaos::default());
        f.disks.push(Arc::new(ChaosDisk {
            inner,
            chaos: chaos.clone(),
        }));
        f.chaos.push(chaos);
    }
    f
}

impl Files {
    pub fn path(&self, core: usize) -> PathBuf {
        self.tmp.join(&format!("raw-{core}"))
    }

    pub fn assert_aligned(&self) {
        for (c, ch) in self.chaos.iter().enumerate() {
            let bad = lock(&ch.misaligned);
            assert!(bad.is_empty(), "core {c} broke the rules of O_DIRECT: {:?}", &bad[..bad.len().min(3)]);
        }
    }
}

// ----- An audit of the Vectors contract under an index.

/// What the index did to its store, checked against the slot rule: a version is read only while
/// it is stored, and is stored once at a time.
#[derive(Debug)]
pub struct Audit {
    pub inner: Arc<dyn Vectors>,
    /// What each live key was stored with.
    pub live: Mutex<std::collections::HashMap<VecKey, Vec<u32>>>,
    pub violations: Mutex<Vec<String>>,
    pub puts: AtomicU64,
    pub gets: AtomicU64,
}

impl Audit {
    pub fn new(inner: Arc<dyn Vectors>) -> Arc<Audit> {
        Arc::new(Audit {
            inner,
            live: Mutex::default(),
            violations: Mutex::default(),
            puts: AtomicU64::new(0),
            gets: AtomicU64::new(0),
        })
    }

    pub fn live_keys(&self) -> usize {
        lock(&self.live).len()
    }

    pub fn violations(&self) -> Vec<String> {
        lock(&self.violations).clone()
    }

    fn violate(&self, what: String) {
        lock(&self.violations).push(what);
    }
}

impl Vectors for Audit {
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        if lock(&self.live).contains_key(&key) {
            self.violate(format!("put of {key:?} while it is stored"));
        }
        self.inner.put(key, vec)?;
        lock(&self.live).insert(key, bits(vec));
        Ok(())
    }

    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>> {
        self.gets.fetch_add(keys.len() as u64, Ordering::Relaxed);
        let got = self.inner.get_many(keys);
        let live = lock(&self.live);
        for (k, g) in keys.iter().zip(&got) {
            match (live.get(k), g) {
                (None, Ok(_)) => self.violate(format!("{k:?} was read after it was released")),
                (None, Err(_)) => self.violate(format!("{k:?} was asked for after it was released")),
                (Some(want), Ok(v)) if bits(v) != *want => {
                    self.violate(format!("{k:?} read another vector than it was stored with"));
                }
                _ => {}
            }
        }
        got
    }

    fn release(&self, key: VecKey) {
        if lock(&self.live).remove(&key).is_none() {
            self.violate(format!("release of {key:?}, which is not stored"));
        }
        self.inner.release(key);
    }

    fn on_disk(&self) -> bool {
        self.inner.on_disk()
    }

    fn stats(&self) -> nabedra_vector::store::Stats {
        self.inner.stats()
    }
}

/// Counts the vectors that the index reads.
#[derive(Debug)]
pub struct Counting {
    pub inner: Arc<dyn Vectors>,
    pub reads: AtomicU64,
}

impl Counting {
    pub fn new(inner: Arc<dyn Vectors>) -> Arc<Counting> {
        Arc::new(Counting {
            inner,
            reads: AtomicU64::new(0),
        })
    }

    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Vectors for Counting {
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()> {
        self.inner.put(key, vec)
    }

    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>> {
        self.reads.fetch_add(keys.len() as u64, Ordering::SeqCst);
        self.inner.get_many(keys)
    }

    fn release(&self, key: VecKey) {
        self.inner.release(key);
    }

    fn on_disk(&self) -> bool {
        self.inner.on_disk()
    }

    fn stats(&self) -> nabedra_vector::store::Stats {
        self.inner.stats()
    }
}

// ----- An index over a store.

pub const RANGE: RangeId = RangeId(9);

pub fn ix_config(dim: usize, vectors: Option<Arc<dyn Vectors>>) -> Config {
    let schema = Schema {
        dim,
        columns: vec![ColumnType::Int],
        partition_by: None,
    };
    Config {
        flat: 0,
        part_min: 2,
        part_max: 8,
        vectors,
        ..Config::new(schema, 3)
    }
}

pub fn serving(cfg: Config) -> Index<ExactCodec> {
    let mut ix = Index::new(cfg, ExactCodec, None).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    ix
}

pub fn rkey(n: u64) -> Key {
    Arc::new(n.to_be_bytes())
}

pub fn row_of(k: u64, v: &[f32]) -> Row {
    Row {
        vec: Some(v.into()),
        cols: vec![Value::Int(k as i64 % 3)],
        image: Arc::new(k.to_be_bytes()),
    }
}

/// Feeds changes after position `after`, running the index to quiet. Returns the new position.
pub fn feed(ix: &mut Index<ExactCodec>, changes: &[(u64, Option<Vec<f32>>)], after: u64) -> Result<u64> {
    let records = changes.iter().enumerate().map(|(i, (k, v))| {
        let at = after + 1 + i as u64;
        Record {
            at,
            change: Change::Row {
                key: rkey(*k),
                commit_ts: at * 10,
                row: v.as_ref().map(|v| row_of(*k, v)),
            },
        }
    });
    let mut page = Page {
        after,
        through: after + changes.len() as u64,
        records: records.collect(),
    };
    while page.after < page.through {
        match ix.apply(RANGE, &page) {
            Ok(at) => page.after = at,
            Err(Error::Busy) => {}
            Err(e) => return Err(e),
        }
        page.records.retain(|r| r.at > page.after);
        ix.run();
    }
    Ok(page.through)
}

pub fn latest(ix: &mut Index<ExactCodec>) -> nabedra_vector::index::Pin {
    ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap()
}

pub fn search(ix: &mut Index<ExactCodec>, q: &Query) -> Result<Vec<Hit>> {
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None)?;
    ix.search(&pin, q)
}

pub fn exact(op: Op, v: &[f32], limit: usize) -> Query {
    Query {
        op,
        vec: v.to_vec(),
        limit,
        filter: None,
        exact: true,
        recall: 1.0,
    }
}

pub fn grid(g: &mut Gen, dim: usize) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 0.5, 9.0, 1e-3, 0.0]);
    (0..dim).map(|_| g.range_i64(-8, 8) as f32 / 8.0 * scale).collect()
}

/// Runs `f` on a thread and fails the test if it does not finish in time, instead of hanging.
pub fn within<T: Send + 'static>(secs: u64, what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(v) => v,
        Err(_) => panic!("{what}: no answer in {secs} s"),
    }
}
