//! Shared helpers for the store-level tests (task NS5).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nabedra_kv::fs::{FaultFs, Fs};
use nabedra_kv::{
    Background, BlockCache, Durability, MemtableBudget, MemtableOptions, Options, Snapshot, Store,
    WriteBatch,
};

/// SplitMix64: a seeded generator, so failures replay from their seed.
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Keys `gNN/...` belong to scope `gNN/`; everything else is store-wide.
pub fn scope4(key: &[u8]) -> usize {
    if key.len() >= 4 && key[0] == b'g' && key[3] == b'/' {
        4
    } else {
        0
    }
}

pub fn group_prefix(group: u64) -> Vec<u8> {
    format!("g{group:02}/").into_bytes()
}

pub fn group_key(group: u64, k: u64) -> Vec<u8> {
    format!("g{group:02}/k{k:04}").into_bytes()
}

/// Small sizes so flushes, compactions, segment rolls and retirement happen in tests.
pub fn small_options(fs: Arc<dyn Fs>, background: Arc<Background>) -> Options {
    let mut o = Options::new(
        fs,
        Arc::new(BlockCache::new(1 << 20)),
        Arc::new(MemtableBudget::new(8 << 20)),
        background,
    );
    o.journal.segment_bytes = 64 << 10;
    o.journal.max_group_bytes = 64 << 10;
    o.memtable_bytes = 24 << 10;
    o.max_replay_bytes = 48 << 10;
    o.levels.l0_compaction_trigger = 2;
    o.levels.target_file_size = 12 << 10;
    o.levels.max_bytes_for_level_base = 32 << 10;
    o.levels.level_size_multiplier = 4;
    o.run_writer.block_size = 512;
    o.memtable = MemtableOptions {
        shard_prefix_len: 4,
        shards: 4,
    };
    o.scope_of = scope4;
    if test_auth() {
        o.journal_integrity = nabedra_kv::JournalIntegrity::Authenticated;
    }
    o
}

/// `NABEDRA_KV_TEST_AUTH=1` runs the store tests with format 2 stores (authenticated
/// journal frames, LSM-1) instead of format 1.
pub fn test_auth() -> bool {
    std::env::var_os("NABEDRA_KV_TEST_AUTH").is_some_and(|v| v == "1")
}

pub fn fault_fs() -> (FaultFs, PathBuf) {
    let fs = FaultFs::new();
    let dir = PathBuf::from("/db/kv");
    fs.create_dir_all_durable(Path::new("/db"));
    (fs, dir)
}

/// A unique temp dir, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        let dir = std::env::temp_dir().join(format!(
            "nbkv-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One operation of a model batch.
#[derive(Clone, Debug)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    PutJournal(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct ModelBatch {
    pub scope: Vec<u8>,
    pub ops: Vec<Op>,
}

impl ModelBatch {
    pub fn to_batch(&self) -> WriteBatch {
        let mut b = WriteBatch::new();
        b.set_scope(&self.scope);
        for op in &self.ops {
            match op {
                Op::Put(k, v) => b.put(k, v),
                Op::PutJournal(k, v) => b.put_journal(k, v),
                Op::Delete(k) => b.delete(k),
                Op::DeleteRange(s, e) => b.delete_range(s, e),
            }
        }
        b
    }

    pub fn apply(&self, model: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
        for op in &self.ops {
            match op {
                Op::Put(k, v) | Op::PutJournal(k, v) => {
                    model.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    model.remove(k);
                }
                Op::DeleteRange(s, e) => {
                    if s < e {
                        let doomed: Vec<_> = model
                            .range::<Vec<u8>, _>((Bound::Included(s), Bound::Excluded(e)))
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in doomed {
                            model.remove(&k);
                        }
                    }
                }
            }
        }
    }
}

/// Random batches over `groups` groups of `keys` keys each, mostly scoped to one group.
pub fn random_batch(rng: &mut Rng, groups: u64, keys: u64, tag: u64) -> ModelBatch {
    let store_wide = rng.chance(10);
    let group = rng.below(groups);
    let n = 1 + rng.below(6);
    let mut ops = Vec::new();
    for i in 0..n {
        let g = if store_wide { rng.below(groups) } else { group };
        let key = if store_wide && rng.chance(20) {
            format!("s/k{:03}", rng.below(keys)).into_bytes()
        } else {
            group_key(g, rng.below(keys))
        };
        let len = if rng.chance(3) {
            1000 + rng.below(2000)
        } else {
            1 + rng.below(120)
        };
        let mut value = format!("v{tag}.{i}.").into_bytes();
        value.resize(value.len() + len as usize, b'a' + (tag % 26) as u8);
        let op = match rng.below(100) {
            0..=44 => Op::Put(key, value),
            45..=69 => Op::PutJournal(key, value),
            70..=89 => Op::Delete(key),
            _ => {
                let a = rng.below(keys);
                let b = a + rng.below(keys / 2 + 1);
                let end = if rng.chance(15) {
                    let mut p = group_prefix(g);
                    *p.last_mut().unwrap() += 1;
                    p
                } else {
                    group_key(g, b)
                };
                Op::DeleteRange(group_key(g, a), end)
            }
        };
        ops.push(op);
    }
    ModelBatch {
        scope: if store_wide {
            Vec::new()
        } else {
            group_prefix(group)
        },
        ops,
    }
}

/// Every key and value visible at `snap`, by a forward scan.
pub fn dump(snap: &Snapshot) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut it = snap.iter(Bound::Unbounded, Bound::Unbounded, true);
    let mut out = BTreeMap::new();
    it.seek_to_first();
    while it.valid() {
        out.insert(it.key().to_vec(), it.value().unwrap().to_vec());
        it.next();
    }
    it.status().unwrap();
    out
}

pub fn open(dir: &Path, options: Options) -> Store {
    Store::open(dir, options).unwrap().store
}

pub fn write(store: &Store, batch: &ModelBatch, durability: Durability) -> nabedra_kv::Result<u64> {
    store
        .submit(batch.to_batch(), durability, Box::new(()))?
        .wait()
}

pub fn put(store: &Store, key: &[u8], value: &[u8]) {
    let mut b = WriteBatch::new();
    b.put(key, value);
    store
        .submit(b, Durability::Sync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
}

/// An `Fs` that can hold the engine's compaction threads: while the gate is closed, every
/// call a `kv-compact` thread makes (through the `Fs` or through any file it reads or
/// writes) waits. Other threads, the caller's included, never wait. A test closes the
/// gate to arrange a fault before background compaction can run, then opens it.
pub struct Gate {
    pub inner: FaultFs,
    closed: std::sync::Mutex<bool>,
    opened: std::sync::Condvar,
}

impl Gate {
    pub fn new(inner: FaultFs) -> Arc<Gate> {
        Arc::new(Gate {
            inner,
            closed: std::sync::Mutex::new(false),
            opened: std::sync::Condvar::new(),
        })
    }

    pub fn close(&self) {
        *self.closed.lock().unwrap() = true;
    }

    pub fn open(&self) {
        *self.closed.lock().unwrap() = false;
        self.opened.notify_all();
    }

    fn pass(&self) {
        if std::thread::current().name() != Some("kv-compact") {
            return;
        }
        let mut closed = self.closed.lock().unwrap();
        while *closed {
            closed = self.opened.wait(closed).unwrap();
        }
    }
}

/// Opens the gate when dropped, also while a failed assertion unwinds.
pub struct OpenOnDrop(pub Arc<Gate>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

pub struct GateFs(pub Arc<Gate>);

struct GateFile {
    inner: Arc<dyn nabedra_kv::fs::FsFile>,
    gate: Arc<Gate>,
}

impl Fs for GateFs {
    fn open(
        &self,
        path: &Path,
        mode: nabedra_kv::fs::OpenMode,
    ) -> std::io::Result<Arc<dyn nabedra_kv::fs::FsFile>> {
        self.0.pass();
        let inner = self.0.inner.open(path, mode)?;
        Ok(Arc::new(GateFile {
            inner,
            gate: self.0.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.0.pass();
        self.0.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> std::io::Result<()> {
        self.0.pass();
        self.0.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> std::io::Result<bool> {
        self.0.pass();
        self.0.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.0.pass();
        self.0.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.0.pass();
        self.0.inner.list(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        self.0.pass();
        self.0.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> std::io::Result<nabedra_kv::fs::FsLock> {
        self.0.inner.lock(path)
    }
}

impl nabedra_kv::fs::FsFile for GateFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.sync_all()
    }
    fn preallocate(&self, len: u64) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.gate.pass();
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.gate.pass();
        self.inner.len()
    }
    fn advise_dont_need(&self, offset: u64, len: u64) {
        self.inner.advise_dont_need(offset, len)
    }
}
