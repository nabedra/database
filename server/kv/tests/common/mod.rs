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
    o
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
