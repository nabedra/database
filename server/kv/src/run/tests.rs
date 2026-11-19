//! Run-level scenario tests: NS01 (differential against a sorted `Vec` oracle) and NS07
//! (bit flips in every part of a run), plus cache and descriptor accounting.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

use super::format::{FOOTER_LEN, Footer};
use super::*;
use crate::cache::{BlockCache, ENTRY_OVERHEAD};
use crate::error::{Error, FileId};
use crate::fs::{Fs, FsFile, FsLock, OpenMode, RealFs};
use crate::source::{InternalIterator, RangeTombstone};
use crate::types::{
    Kind, Locator, MAX_SEQ, SEEK_KIND_BYTE, Seq, compare_internal_keys, internal_key,
    parse_internal_key, user_key,
};

// ---------------------------------------------------------------------------------------
// Test filesystem and helpers
// ---------------------------------------------------------------------------------------

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

#[derive(Default)]
struct MemState {
    files: Mutex<HashMap<PathBuf, Arc<RwLock<Vec<u8>>>>>,
    opens: AtomicU64,
    live_handles: AtomicUsize,
    sync_alls: AtomicU64,
    advises: AtomicU64,
    fail_writes: AtomicBool,
}

/// A minimal in-memory [`Fs`] that counts opens, live handles and syncs.
#[derive(Clone, Default)]
struct MemFs(Arc<MemState>);

struct MemFile {
    data: Arc<RwLock<Vec<u8>>>,
    state: Arc<MemState>,
}

impl Drop for MemFile {
    fn drop(&mut self) {
        self.state.live_handles.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

impl MemFs {
    fn with_file(path: &Path, bytes: Vec<u8>) -> MemFs {
        let fs = MemFs::default();
        fs.0.files
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), Arc::new(RwLock::new(bytes)));
        fs
    }

    fn contents(&self, path: &Path) -> Vec<u8> {
        self.0.files.lock().unwrap()[path].read().unwrap().clone()
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
}

impl Fs for MemFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        let mut files = self.0.files.lock().unwrap();
        let data = match (mode, files.get(path)) {
            (OpenMode::CreateNew, Some(_)) => return Err(io::ErrorKind::AlreadyExists.into()),
            (OpenMode::CreateNew, None) => {
                let data = Arc::new(RwLock::new(Vec::new()));
                files.insert(path.to_path_buf(), Arc::clone(&data));
                data
            }
            (_, Some(data)) => Arc::clone(data),
            (_, None) => return Err(io::ErrorKind::NotFound.into()),
        };
        self.0.opens.fetch_add(1, AtomicOrdering::SeqCst);
        self.0.live_handles.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(Arc::new(MemFile {
            data,
            state: Arc::clone(&self.0),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut files = self.0.files.lock().unwrap();
        let data = files.remove(from).ok_or(io::ErrorKind::NotFound)?;
        files.insert(to.to_path_buf(), data);
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.0.files.lock().unwrap().remove(path);
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        Ok(self.0.files.lock().unwrap().contains_key(path))
    }

    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        Ok(self
            .0
            .files
            .lock()
            .unwrap()
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn lock(&self, _path: &Path) -> io::Result<FsLock> {
        Ok(FsLock::new(Box::new(())))
    }
}

impl FsFile for MemFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.state.fail_writes.load(AtomicOrdering::SeqCst) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let mut data = self.data.write().unwrap();
        let end = offset as usize + buf.len();
        if data.len() < end {
            data.resize(end, 0);
        }
        data[offset as usize..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let data = self.data.read().unwrap();
        let end = (offset as usize)
            .checked_add(buf.len())
            .filter(|&end| end <= data.len())
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        buf.copy_from_slice(&data[offset as usize..end]);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.state.sync_alls.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(())
    }

    fn preallocate(&self, _len: u64) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.data.write().unwrap().resize(len as usize, 0);
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.data.read().unwrap().len() as u64)
    }

    fn advise_dont_need(&self, _offset: u64, _len: u64) {
        self.state.advises.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

fn dir() -> PathBuf {
    PathBuf::from("/mem/runs")
}

/// A sorted run's contents, the oracle every read is checked against.
#[derive(Clone, Debug, Default)]
struct Model {
    /// Sorted by internal key.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    tombstones: Vec<RangeTombstone>,
    universe: Vec<Vec<u8>>,
    max_seq: Seq,
}

impl Model {
    fn write(&self, fs: &dyn Fs, id: u64, options: RunWriterOptions) -> crate::Result<RunMeta> {
        let mut writer = RunWriter::create(fs, &dir().join(run_file_name(id)), id, options)?;
        for (k, v) in &self.entries {
            writer.add(k, v)?;
        }
        for t in &self.tombstones {
            writer.add_range_tombstone(t.clone())?;
        }
        writer.finish()
    }

    fn get(&self, user: &[u8], snapshot: Seq) -> Lookup {
        let start = self.entries.partition_point(|(k, _)| user_key(k) < user);
        for (k, v) in &self.entries[start..] {
            let p = parse_internal_key(k).unwrap();
            if p.user_key != user {
                break;
            }
            if p.seq <= snapshot {
                return match p.kind {
                    Kind::Delete => Lookup::Deleted { seq: p.seq },
                    kind => Lookup::Found {
                        kind,
                        value: Bytes::copy_from_slice(v),
                        seq: p.seq,
                    },
                };
            }
        }
        Lookup::NotFound
    }

    fn covering(&self, user: &[u8], snapshot: Seq) -> Option<Seq> {
        self.tombstones
            .iter()
            .filter(|t| t.start.as_ref() <= user && user < t.end.as_ref() && t.seq <= snapshot)
            .map(|t| t.seq)
            .max()
    }

    fn sorted_tombstones(&self) -> Vec<RangeTombstone> {
        let mut out: Vec<_> = self
            .tombstones
            .iter()
            .filter(|t| t.start < t.end)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.start
                .cmp(&b.start)
                .then_with(|| b.seq.cmp(&a.seq))
                .then_with(|| a.end.cmp(&b.end))
        });
        out
    }
}

fn random_user_key(rng: &mut Rng) -> Vec<u8> {
    const PREFIXES: [&[u8]; 5] = [b"", b"a", b"ab", b"g\x00\x01\x02", b"zz"];
    let mut key = PREFIXES[rng.below(PREFIXES.len() as u64) as usize].to_vec();
    for _ in 0..rng.below(7) {
        key.push(b"abc\x00\xff"[rng.below(5) as usize]);
    }
    key
}

fn random_model(rng: &mut Rng, max_entries: usize) -> Model {
    let universe: Vec<Vec<u8>> = (0..rng.below(max_entries as u64 / 2) + 1)
        .map(|_| random_user_key(rng))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let versions: Vec<usize> = universe.iter().map(|_| 1 + rng.below(4) as usize).collect();
    let total: usize = versions.iter().sum();
    // Distinct sequences in random order.
    let mut seqs: Vec<Seq> = (1..=total as u64 + rng.below(50)).collect();
    for i in (1..seqs.len()).rev() {
        seqs.swap(i, rng.below(i as u64 + 1) as usize);
    }
    seqs.truncate(total);
    let max_seq = seqs.iter().copied().max().unwrap_or(0) + 2;
    let mut entries = Vec::with_capacity(total);
    let mut next = seqs.into_iter();
    for (key, count) in universe.iter().zip(&versions) {
        for _ in 0..*count {
            let seq = next.next().unwrap();
            let roll = rng.below(100);
            let (kind, value) = if roll < 15 {
                (Kind::Delete, Vec::new())
            } else if roll < 30 {
                let locator = Locator {
                    segment: 3 + rng.below(20),
                    epoch: rng.below(4),
                    offset: rng.next() >> 20,
                    len: rng.next() as u32,
                    crc32c: rng.next() as u32,
                };
                (Kind::JournalPut, locator.encode().to_vec())
            } else {
                let len = if rng.chance(3) {
                    4096 + rng.below(9000) as usize
                } else {
                    rng.below(40) as usize
                };
                (Kind::Put, rng.bytes(len))
            };
            entries.push((internal_key(key, seq, kind), value));
        }
    }
    entries.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
    let tombstones = (0..rng.below(8))
        .map(|_| {
            let a = random_user_key(rng);
            let b = random_user_key(rng);
            // Some ranges are empty or inverted on purpose: they must be dropped.
            RangeTombstone {
                start: a.into(),
                end: b.into(),
                seq: 1 + rng.below(max_seq),
            }
        })
        .collect();
    Model {
        entries,
        tombstones,
        universe,
        max_seq,
    }
}

fn open_run(fs: &MemFs, id: u64, ctx: RunReadContext) -> crate::Result<Arc<Run>> {
    let file = fs.open(&dir().join(run_file_name(id)), OpenMode::Read)?;
    Run::open(file, id, ctx)
}

fn collect_forward(iter: &mut RunIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    iter.seek_to_first();
    while iter.valid() {
        out.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next();
    }
    out
}

fn collect_backward(iter: &mut RunIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    iter.seek_to_last();
    while iter.valid() {
        out.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.prev();
    }
    out.reverse();
    out
}

fn random_target(rng: &mut Rng, model: &Model) -> Vec<u8> {
    let mut target = if !model.universe.is_empty() && rng.chance(80) {
        model.universe[rng.below(model.universe.len() as u64) as usize].clone()
    } else {
        random_user_key(rng)
    };
    let seq = rng.below(model.max_seq + 1);
    let kind = [0u8, 1, 2, 3, SEEK_KIND_BYTE][rng.below(5) as usize];
    target.extend_from_slice(&((seq << 8) | u64::from(kind)).to_be_bytes());
    target
}

fn assert_corruption(result: crate::Result<impl std::fmt::Debug>, id: u64) {
    match result {
        Err(Error::Corruption {
            file: FileId::Run { id: got },
            ..
        }) => assert_eq!(got, id),
        other => panic!("expected corruption of run {id}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------
// NS01: differential test against a sorted Vec
// ---------------------------------------------------------------------------------------

fn check_model(seed: u64, model: &Model, rng: &mut Rng) {
    let fs = MemFs::default();
    let options = RunWriterOptions {
        // Tiny blocks only for small models: a 64-byte block holds one or two entries,
        // which makes the index (re-read by every get through a zero-capacity cache)
        // as large as the data.
        block_size: if model.entries.len() < 400 {
            [64, 256, 4096][rng.below(3) as usize]
        } else {
            [256, 4096][rng.below(2) as usize]
        },
        restart_interval: [1, 3, 16][rng.below(3) as usize],
        ..RunWriterOptions::default()
    };
    let id = 1 + rng.below(1000);
    let meta = match model.write(&fs, id, options) {
        Ok(meta) => meta,
        Err(Error::InvalidArgument(_)) if model.entries.is_empty() => return,
        Err(e) => panic!("seed {seed}: {e:?}"),
    };
    let cache = Arc::new(BlockCache::new(
        [0, 16 << 10, 64 << 20][rng.below(3) as usize],
    ));
    let ctx = RunReadContext::new(Arc::clone(&cache));
    let run = open_run(&fs, id, ctx.clone()).unwrap();
    let fill = rng.chance(50);

    // Properties.
    let tombstones = model.sorted_tombstones();
    assert_eq!(run.meta(), &meta, "seed {seed}");
    assert_eq!(meta.num_entries, model.entries.len() as u64);
    let count = |kind| {
        model
            .entries
            .iter()
            .filter(|(k, _)| parse_internal_key(k).unwrap().kind == kind)
            .count() as u64
    };
    assert_eq!(meta.num_deletions, count(Kind::Delete));
    assert_eq!(meta.num_journal_values, count(Kind::JournalPut));
    assert_eq!(meta.num_range_tombstones, tombstones.len() as u64);
    let oldest = model
        .entries
        .iter()
        .filter(|(k, _)| parse_internal_key(k).unwrap().kind == Kind::JournalPut)
        .map(|(_, v)| Locator::decode(v).unwrap().segment)
        .min();
    assert_eq!(meta.oldest_journal_segment, oldest);
    let seqs = model
        .entries
        .iter()
        .map(|(k, _)| parse_internal_key(k).unwrap().seq)
        .chain(tombstones.iter().map(|t| t.seq));
    assert_eq!(meta.smallest_seq, seqs.clone().min().unwrap());
    assert_eq!(meta.largest_seq, seqs.max().unwrap());
    // Every entry and tombstone lies within [smallest, largest].
    for (k, _) in &model.entries {
        assert_ne!(compare_internal_keys(k, &meta.smallest), Ordering::Less);
        assert_ne!(compare_internal_keys(k, &meta.largest), Ordering::Greater);
    }
    for t in &tombstones {
        assert!(user_key(&meta.smallest) <= t.start.as_ref());
        assert!(user_key(&meta.largest) >= t.end.as_ref());
    }
    assert_eq!(run.range_tombstones(), &tombstones[..], "seed {seed}");

    // Point lookups and covering tombstones, for present and absent keys.
    let mut keys = model.universe.clone();
    keys.extend((0..20).map(|_| random_user_key(rng)));
    for key in &keys {
        for snapshot in [0, rng.below(model.max_seq + 1), MAX_SEQ] {
            let got = run.get(key, snapshot, fill).unwrap();
            assert_eq!(
                got,
                model.get(key, snapshot),
                "seed {seed} key {key:?} at {snapshot}"
            );
            assert_eq!(
                run.max_covering_tombstone_seq(key, snapshot),
                model.covering(key, snapshot),
                "seed {seed} key {key:?} at {snapshot}"
            );
        }
    }

    // Full scans both ways.
    let mut iter = run.iter(fill);
    assert_eq!(collect_forward(&mut iter), model.entries, "seed {seed}");
    assert_eq!(collect_backward(&mut iter), model.entries, "seed {seed}");
    iter.status().unwrap();

    // Random seeks and walks with direction switches.
    let n = model.entries.len();
    for _ in 0..200 {
        let target = random_target(rng, model);
        let mut pos: Option<usize>;
        if rng.chance(50) {
            iter.seek(&target);
            let p = model
                .entries
                .partition_point(|(k, _)| compare_internal_keys(k, &target) == Ordering::Less);
            pos = (p < n).then_some(p);
        } else {
            iter.seek_for_prev(&target);
            let p = model
                .entries
                .partition_point(|(k, _)| compare_internal_keys(k, &target) != Ordering::Greater);
            pos = p.checked_sub(1);
        }
        for _ in 0..rng.below(20) {
            match pos {
                Some(p) => {
                    assert!(iter.valid(), "seed {seed}");
                    assert_eq!(iter.key(), &model.entries[p].0[..], "seed {seed}");
                    assert_eq!(iter.value(), &model.entries[p].1[..], "seed {seed}");
                    if rng.chance(50) {
                        iter.next();
                        pos = (p + 1 < n).then_some(p + 1);
                    } else {
                        iter.prev();
                        pos = p.checked_sub(1);
                    }
                }
                None => {
                    assert!(!iter.valid(), "seed {seed}");
                    break;
                }
            }
        }
        if pos.is_none() {
            assert!(!iter.valid(), "seed {seed}");
        }
        iter.status().unwrap();
    }
    assert_eq!(ctx.stats.corruption_detected(), 0);
    drop(iter);
    drop(run);
    assert_eq!(
        cache.pinned_bytes(),
        0,
        "seed {seed}: nothing is held any more"
    );
}

#[test]
fn ns01_run_reads_match_a_sorted_vec_oracle() {
    for seed in 0..32 {
        let mut rng = Rng(seed);
        // Sizes stay small here: until NS1's fast CRC32C lands, a debug build checksums
        // bit by bit, and a zero-capacity cache re-reads the index on every get. The
        // ignored long variant covers large runs.
        let max_entries = [4, 60, 300, 1000][(seed % 4) as usize];
        let model = random_model(&mut rng, max_entries);
        check_model(seed, &model, &mut rng);
    }
}

#[test]
#[ignore = "long NS01 run variant; run with --ignored"]
fn ns01_run_reads_match_a_sorted_vec_oracle_long() {
    for seed in 1000..1400 {
        let mut rng = Rng(seed);
        let model = random_model(&mut rng, 6000);
        check_model(seed, &model, &mut rng);
    }
}

#[test]
fn ns01_tombstone_only_and_single_entry_runs() {
    let fs = MemFs::default();
    let model = Model {
        entries: Vec::new(),
        tombstones: vec![
            RangeTombstone {
                start: Bytes::from_static(b"b"),
                end: Bytes::from_static(b"d"),
                seq: 7,
            },
            RangeTombstone {
                start: Bytes::from_static(b"a"),
                end: Bytes::from_static(b"c"),
                seq: 9,
            },
        ],
        universe: Vec::new(),
        max_seq: 10,
    };
    let meta = model.write(&fs, 5, RunWriterOptions::default()).unwrap();
    assert_eq!(meta.smallest, internal_key(b"a", 9, Kind::RangeDelete));
    assert_eq!(meta.largest, internal_key(b"d", MAX_SEQ, Kind::RangeDelete));
    assert_eq!((meta.smallest_seq, meta.largest_seq), (7, 9));
    let run = open_run(
        &fs,
        5,
        RunReadContext::new(Arc::new(BlockCache::new(1 << 20))),
    )
    .unwrap();
    assert_eq!(run.get(b"b", MAX_SEQ, true).unwrap(), Lookup::NotFound);
    assert_eq!(run.max_covering_tombstone_seq(b"b", MAX_SEQ), Some(9));
    assert_eq!(run.max_covering_tombstone_seq(b"b", 8), Some(7));
    assert_eq!(run.max_covering_tombstone_seq(b"c", 8), Some(7));
    assert_eq!(run.max_covering_tombstone_seq(b"d", 99), None);
    let mut iter = run.iter(true);
    assert!(collect_forward(&mut iter).is_empty());
    iter.seek_for_prev(&internal_key(b"z", 1, Kind::Put));
    assert!(!iter.valid());
    iter.status().unwrap();

    let model = Model {
        entries: vec![(internal_key(b"", 1, Kind::Put), b"empty key".to_vec())],
        ..Model::default()
    };
    model.write(&fs, 6, RunWriterOptions::default()).unwrap();
    let run = open_run(&fs, 6, RunReadContext::new(Arc::new(BlockCache::new(0)))).unwrap();
    assert_eq!(
        run.get(b"", 1, true).unwrap(),
        Lookup::Found {
            kind: Kind::Put,
            value: Bytes::from_static(b"empty key"),
            seq: 1
        }
    );
    assert_eq!(run.get(b"", 0, true).unwrap(), Lookup::NotFound);
    assert_eq!(run.get(b"a", 9, true).unwrap(), Lookup::NotFound);
}

#[test]
fn ns01_writer_rejects_bad_input_and_isolates_oversized_values() {
    let fs = MemFs::default();
    let path = dir().join("1.run");
    let mut writer = RunWriter::create(&fs, &path, 1, RunWriterOptions::default()).unwrap();
    assert!(RunWriter::create(&fs, &path, 1, RunWriterOptions::default()).is_err());
    writer.add(&internal_key(b"b", 5, Kind::Put), b"x").unwrap();
    let bad = [
        writer.add(&internal_key(b"b", 5, Kind::Put), b"dup"),
        writer.add(&internal_key(b"b", 6, Kind::Put), b"older seq sorts first"),
        writer.add(&internal_key(b"a", 1, Kind::Put), b"smaller key"),
        writer.add(&internal_key(b"c", 1, Kind::RangeDelete), b"d"),
        writer.add(&internal_key(b"c", 1, Kind::JournalPut), b"not a locator"),
        writer.add(b"short", b""),
    ];
    for result in bad {
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{result:?}"
        );
    }
    let big = vec![7u8; 20_000];
    writer.add(&internal_key(b"c", 1, Kind::Put), &big).unwrap();
    writer
        .add(&internal_key(b"d", 1, Kind::Put), b"after")
        .unwrap();
    assert!(writer.estimated_size() > 20_000);
    let meta = writer.finish().unwrap();
    assert_eq!(
        meta.num_data_blocks, 3,
        "the big value has a block of its own"
    );
    assert_eq!(fs.0.sync_alls.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(fs.0.advises.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(meta.file_size, fs.contents(&path).len() as u64);

    let run = open_run(
        &fs,
        1,
        RunReadContext::new(Arc::new(BlockCache::new(1 << 20))),
    )
    .unwrap();
    match run.get(b"c", 9, true).unwrap() {
        Lookup::Found { value, .. } => assert_eq!(value, big),
        other => panic!("{other:?}"),
    }

    let empty =
        RunWriter::create(&fs, &dir().join("2.run"), 2, RunWriterOptions::default()).unwrap();
    assert!(matches!(empty.finish(), Err(Error::InvalidArgument(_))));
}

#[test]
fn ns01_writer_latches_its_first_io_error() {
    let fs = MemFs::default();
    let options = RunWriterOptions {
        write_buffer_bytes: 1,
        ..RunWriterOptions::default()
    };
    let mut writer = RunWriter::create(&fs, &dir().join("3.run"), 3, options).unwrap();
    fs.0.fail_writes.store(true, AtomicOrdering::SeqCst);
    let mut first = None;
    for i in 0..100u32 {
        if let Err(e) = writer.add(&internal_key(&i.to_be_bytes(), 1, Kind::Put), &[0; 100]) {
            first = Some(e);
            break;
        }
    }
    assert_eq!(first, Some(Error::NoSpace));
    fs.0.fail_writes.store(false, AtomicOrdering::SeqCst);
    assert_eq!(
        writer.add(&internal_key(b"\xff", 1, Kind::Put), b""),
        Err(Error::NoSpace)
    );
    assert_eq!(writer.finish(), Err(Error::NoSpace));
}

// ---------------------------------------------------------------------------------------
// NS07: bit flips
// ---------------------------------------------------------------------------------------

/// Reads everything a run holds and checks that each result is either exactly the
/// oracle's or a `Corruption` of this run. Returns how many operations hit corruption.
fn read_all_checked(fs: &MemFs, id: u64, model: &Model) -> usize {
    let ctx = RunReadContext::new(Arc::new(BlockCache::new(1 << 20)));
    let run = match open_run(fs, id, ctx.clone()) {
        Ok(run) => run,
        Err(e) => {
            assert_corruption(Err::<(), _>(e), id);
            assert_eq!(ctx.stats.corruption_detected(), 1);
            return 1;
        }
    };
    let mut corrupt = 0;
    assert_eq!(run.range_tombstones(), &model.sorted_tombstones()[..]);
    for key in &model.universe {
        for snapshot in [MAX_SEQ, model.max_seq / 2] {
            match run.get(key, snapshot, true) {
                Ok(got) => assert_eq!(got, model.get(key, snapshot), "wrong value for {key:?}"),
                Err(e) => {
                    assert_corruption(Err::<(), _>(e), id);
                    corrupt += 1;
                }
            }
        }
    }
    let mut iter = run.iter(false);
    let forward = collect_forward(&mut iter);
    match iter.status() {
        Ok(()) => assert_eq!(forward, model.entries),
        Err(e) => {
            assert_corruption(Err::<(), _>(e), id);
            assert_eq!(forward[..], model.entries[..forward.len()], "wrong prefix");
            corrupt += 1;
        }
    }
    let backward = collect_backward(&mut iter);
    match iter.status() {
        Ok(()) => assert_eq!(backward, model.entries),
        Err(e) => {
            assert_corruption(Err::<(), _>(e), id);
            assert_eq!(
                backward[..],
                model.entries[model.entries.len() - backward.len()..],
                "wrong suffix"
            );
            corrupt += 1;
        }
    }
    if corrupt > 0 {
        assert!(ctx.stats.corruption_detected() > 0);
    }
    corrupt
}

fn small_model() -> Model {
    let mut entries = Vec::new();
    let mut universe = Vec::new();
    for i in 0..40u32 {
        let key = format!("k{i:03}").into_bytes();
        entries.push((
            internal_key(&key, 100 + u64::from(i), Kind::Put),
            format!("v{i}").into_bytes(),
        ));
        if i % 3 == 0 {
            entries.push((internal_key(&key, u64::from(i), Kind::Delete), Vec::new()));
        }
        universe.push(key);
    }
    Model {
        entries,
        tombstones: vec![RangeTombstone {
            start: Bytes::from_static(b"k010"),
            end: Bytes::from_static(b"k020"),
            seq: 50,
        }],
        universe,
        max_seq: 200,
    }
}

#[test]
fn ns07_bit_flip_in_each_run_part_returns_corruption_never_a_wrong_value() {
    let model = small_model();
    let fs = MemFs::default();
    let options = RunWriterOptions {
        block_size: 128,
        ..RunWriterOptions::default()
    };
    model.write(&fs, 9, options).unwrap();
    let path = dir().join(run_file_name(9));
    let bytes = fs.contents(&path);
    let footer_at = bytes.len() - FOOTER_LEN;
    let footer = Footer::decode(bytes[footer_at..].try_into().unwrap()).unwrap();
    let flip = |offset: usize| {
        let mut bad = bytes.clone();
        bad[offset] ^= 0x10;
        MemFs::with_file(&path, bad)
    };
    let ctx = || RunReadContext::new(Arc::new(BlockCache::new(1 << 20)));

    // Data block (the first one starts at offset 0): its keys and every scan fail.
    let fs = flip(3);
    let run = open_run(&fs, 9, ctx()).unwrap();
    assert_corruption(run.get(b"k000", MAX_SEQ, true), 9);
    let mut iter = run.iter(true);
    iter.seek_to_first();
    assert!(!iter.valid());
    assert_corruption(iter.status(), 9);
    assert_eq!(run.stats().corruption_detected(), 2);
    assert!(read_all_checked(&fs, 9, &model) > 0);

    // Index block: every lookup and scan fails.
    let fs = flip(footer.index.offset as usize + 2);
    let run = open_run(&fs, 9, ctx()).unwrap();
    assert_corruption(run.get(b"k020", MAX_SEQ, true), 9);
    let mut iter = run.iter(true);
    iter.seek(&internal_key(b"k005", MAX_SEQ, Kind::Put));
    assert_corruption(iter.status(), 9);
    assert!(read_all_checked(&fs, 9, &model) > 0);

    // Bloom block: lookups fail (scans do not use it).
    let fs = flip(footer.bloom.offset as usize + 1);
    let run = open_run(&fs, 9, ctx()).unwrap();
    assert_corruption(run.get(b"k020", MAX_SEQ, true), 9);
    assert_eq!(run.stats().corruption_detected(), 1);
    assert!(read_all_checked(&fs, 9, &model) > 0);

    // Footer, properties and range tombstones are checked at open.
    for offset in [
        footer_at + 13,
        bytes.len() - 1,
        footer.props.offset as usize + 4,
        footer.range_del.offset as usize + 1,
    ] {
        let ctx = ctx();
        assert_corruption(open_run(&flip(offset), 9, ctx.clone()), 9);
        assert_eq!(ctx.stats.corruption_detected(), 1);
    }

    // Truncation is corruption too, not an I/O error.
    for len in [0, 10, bytes.len() - 1] {
        let fs = MemFs::with_file(&path, bytes[..len].to_vec());
        assert_corruption(open_run(&fs, 9, ctx()), 9);
    }
    // A run renamed to another id is refused.
    let fs = MemFs::with_file(&dir().join(run_file_name(10)), bytes.clone());
    assert_corruption(open_run(&fs, 10, ctx()), 10);
}

#[test]
fn ns07_every_single_bit_flip_is_detected_and_never_returns_a_wrong_value() {
    let model = small_model();
    let fs = MemFs::default();
    let options = RunWriterOptions {
        block_size: 128,
        restart_interval: 4,
        ..RunWriterOptions::default()
    };
    model.write(&fs, 4, options).unwrap();
    let path = dir().join(run_file_name(4));
    let bytes = fs.contents(&path);
    assert_eq!(
        read_all_checked(&MemFs::with_file(&path, bytes.clone()), 4, &model),
        0
    );
    let mut rng = Rng(7);
    for offset in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[offset] ^= 1 << rng.below(8);
        let corrupt = read_all_checked(&MemFs::with_file(&path, bad), 4, &model);
        // Every byte is covered by some checksum and the reads touch every block.
        assert!(
            corrupt > 0,
            "flip at byte {offset} of {} went unnoticed",
            bytes.len()
        );
    }
}

#[test]
fn ns07_bit_flip_on_the_real_filesystem() {
    let dir = std::env::temp_dir().join(format!(
        "nabedra-kv-run-ns07-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let model = small_model();
    let mut writer = RunWriter::create(
        &*fs,
        &dir.join(run_file_name(1)),
        1,
        RunWriterOptions::default(),
    )
    .unwrap();
    for (k, v) in &model.entries {
        writer.add(k, v).unwrap();
    }
    writer.finish().unwrap();
    let tables = TableCache::new(
        Arc::clone(&fs),
        &dir,
        RunReadContext::new(Arc::new(BlockCache::new(1 << 20))),
        DEFAULT_MAX_OPEN_RUNS,
    );
    let run = tables.get(1).unwrap();
    assert_eq!(
        run.get(b"k001", MAX_SEQ, false).unwrap(),
        model.get(b"k001", MAX_SEQ)
    );
    drop(run);
    tables.evict(1);

    let path = dir.join(run_file_name(1));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[20] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    let run = tables.get(1).unwrap();
    assert_corruption(run.get(b"k001", MAX_SEQ, false), 1);
    assert_eq!(tables.context().stats.corruption_detected(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

// ---------------------------------------------------------------------------------------
// Block cache and descriptor cache
// ---------------------------------------------------------------------------------------

#[test]
fn run_reads_fill_the_cache_only_when_asked_and_pin_what_callers_hold() {
    let model = small_model();
    let fs = MemFs::default();
    model.write(&fs, 2, RunWriterOptions::default()).unwrap();
    let cache = Arc::new(BlockCache::new(1 << 20));
    let ctx = RunReadContext::new(Arc::clone(&cache));
    let run = open_run(&fs, 2, ctx.clone()).unwrap();

    // fill_cache = false reads through without inserting.
    for key in &model.universe {
        run.get(key, MAX_SEQ, false).unwrap();
    }
    let mut iter = run.iter(false);
    collect_forward(&mut iter);
    drop(iter);
    assert_eq!(cache.usage_bytes(), 0);
    let cold_reads = ctx.stats.block_reads();
    assert!(cold_reads > 0);

    // fill_cache = true caches index, filter and data blocks; a second pass reads nothing.
    for key in &model.universe {
        run.get(key, MAX_SEQ, true).unwrap();
    }
    let usage = cache.usage_bytes();
    assert!(usage > 0);
    let warm = ctx.stats.block_reads();
    for key in &model.universe {
        run.get(key, MAX_SEQ, true).unwrap();
    }
    let mut iter = run.iter(true);
    collect_forward(&mut iter);
    assert_eq!(ctx.stats.block_reads(), warm, "hot reads hit the cache");
    // A positioned iterator pins its index and data blocks.
    iter.seek_to_first();
    assert!(cache.pinned_bytes() > 0);
    drop(iter);
    assert_eq!(cache.pinned_bytes(), 0);

    // A value handed to a caller pins its block until the caller drops it.
    let Lookup::Found { value, .. } = run.get(b"k001", MAX_SEQ, true).unwrap() else {
        panic!("k001 is present");
    };
    let pinned = cache.pinned_bytes();
    assert!(pinned >= value.len() + ENTRY_OVERHEAD);
    drop(value);
    assert_eq!(cache.pinned_bytes(), 0);
    assert!(ctx.stats.bloom_checks() > 0);
    assert_eq!(run.get(b"k0005", MAX_SEQ, true).unwrap(), Lookup::NotFound);
}

#[test]
fn descriptor_cache_keeps_at_most_max_open_runs_and_reopens_transparently() {
    let fs = MemFs::default();
    let total = 200u64;
    for id in 0..total {
        let model = Model {
            entries: vec![(
                internal_key(&id.to_be_bytes(), 1, Kind::Put),
                id.to_le_bytes().to_vec(),
            )],
            ..Model::default()
        };
        model.write(&fs, id, RunWriterOptions::default()).unwrap();
    }
    assert_eq!(fs.0.live_handles.load(AtomicOrdering::SeqCst), 0);
    let base_opens = fs.0.opens.load(AtomicOrdering::SeqCst);
    let ctx = RunReadContext::new(Arc::new(BlockCache::new(1 << 20)));
    let tables = TableCache::new(fs.arc(), &dir(), ctx, DEFAULT_MAX_OPEN_RUNS);
    let read = |id: u64| {
        let run = tables.get(id).unwrap();
        assert_eq!(
            run.get(&id.to_be_bytes(), 1, true).unwrap(),
            Lookup::Found {
                kind: Kind::Put,
                value: Bytes::copy_from_slice(&id.to_le_bytes()),
                seq: 1
            }
        );
    };
    for id in 0..total {
        read(id);
        assert!(tables.open_runs() <= DEFAULT_MAX_OPEN_RUNS);
        assert!(fs.0.live_handles.load(AtomicOrdering::SeqCst) <= DEFAULT_MAX_OPEN_RUNS);
    }
    assert_eq!(tables.open_runs(), DEFAULT_MAX_OPEN_RUNS);
    assert_eq!(tables.opens(), total);
    // Runs 72..200 are open, 72 the least recently used.
    read(199);
    read(72);
    assert_eq!(tables.opens(), total);
    read(0); // evicts 73, the least recently used now
    assert_eq!(tables.opens(), total + 1);
    read(72);
    assert_eq!(tables.opens(), total + 1);
    read(73);
    assert_eq!(tables.opens(), total + 2);

    // An iterator keeps an evicted run usable.
    let held = tables.get(150).unwrap();
    let mut iter = held.iter(true);
    drop(held);
    tables.evict(150);
    iter.seek_to_first();
    assert!(iter.valid());
    assert_eq!(iter.value(), &150u64.to_le_bytes()[..]);
    drop(iter);

    assert_eq!(
        fs.0.live_handles.load(AtomicOrdering::SeqCst),
        tables.open_runs()
    );
    assert_eq!(
        fs.0.opens.load(AtomicOrdering::SeqCst) - base_opens,
        tables.opens()
    );
    assert!(
        tables.get(total + 5).is_err(),
        "a missing run is an I/O error"
    );
    assert_eq!(parse_run_file_name("17.run"), Some(17));
    for bad in ["017.run", ".run", "17.runx", "a.run", "17"] {
        assert_eq!(parse_run_file_name(bad), None, "{bad}");
    }
}

#[test]
fn runs_serve_concurrent_readers_through_a_small_shared_cache() {
    let mut rng = Rng(99);
    let model = random_model(&mut rng, 2000);
    let fs = MemFs::default();
    model
        .write(
            &fs,
            8,
            RunWriterOptions {
                block_size: 512,
                ..RunWriterOptions::default()
            },
        )
        .unwrap();
    let cache = Arc::new(BlockCache::new(32 << 10));
    let run = open_run(&fs, 8, RunReadContext::new(Arc::clone(&cache))).unwrap();
    let model = Arc::new(model);
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let run = Arc::clone(&run);
            let model = Arc::clone(&model);
            std::thread::spawn(move || {
                let mut rng = Rng(t);
                for _ in 0..3000 {
                    let key = &model.universe[rng.below(model.universe.len() as u64) as usize];
                    let snapshot = rng.below(model.max_seq + 1);
                    assert_eq!(
                        run.get(key, snapshot, true).unwrap(),
                        model.get(key, snapshot)
                    );
                }
                let mut iter = run.iter(t % 2 == 0);
                assert_eq!(collect_forward(&mut iter), model.entries);
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(cache.stats().evictions > 0);
    assert_eq!(cache.pinned_bytes(), 0);
}

// ---------------------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------------------

/// Point-get and scan cost on the real filesystem. Run with
/// `cargo test -p nabedra-kv --release -- --ignored bench_run --nocapture`.
/// "Cold" means a zero-capacity block cache: every get reads the bloom, index and data
/// blocks with `pread` (the OS page cache is warm).
#[test]
#[ignore = "benchmark; run with --ignored --nocapture in release"]
fn bench_run_point_get_and_scan() {
    let dir = std::env::temp_dir().join(format!("nabedra-kv-run-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    for (id, n) in [(1u64, 50_000u64), (2, 500_000)] {
        bench_one_run(&fs, &dir, id, n);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

fn bench_one_run(fs: &Arc<dyn Fs>, dir: &Path, id: u64, n: u64) {
    use std::time::Instant;
    let key_of = |i: u64| {
        // 17-byte group prefix plus a row key, like the pool layout.
        let mut key = vec![0x40];
        key.extend_from_slice(&(i % 64).to_be_bytes());
        key.extend_from_slice(&[0u8; 8]);
        key.extend_from_slice(&i.to_be_bytes());
        key
    };
    let mut keys: Vec<Vec<u8>> = (0..n).map(key_of).collect();
    keys.sort();
    let start = Instant::now();
    let mut writer = RunWriter::create(
        &**fs,
        &dir.join(run_file_name(id)),
        id,
        RunWriterOptions::default(),
    )
    .unwrap();
    let value = [0xABu8; 100];
    for key in &keys {
        writer
            .add(&internal_key(key, 1, Kind::Put), &value)
            .unwrap();
    }
    let meta = writer.finish().unwrap();
    let write_secs = start.elapsed().as_secs_f64();
    println!(
        "write: {n} entries x 100 B, {} MiB in {:.3} s ({:.0} ns/entry), {} data blocks",
        meta.file_size >> 20,
        write_secs,
        write_secs * 1e9 / n as f64,
        meta.num_data_blocks
    );

    // Cold reads checksum every block they read, so they run fewer operations.
    for (label, capacity, gets, rounds) in [
        ("hot", 256usize << 20, 200_000u32, 20_000u32),
        ("cold", 0, 2_000, 500),
    ] {
        let cache = Arc::new(BlockCache::new(capacity));
        let ctx = RunReadContext::new(Arc::clone(&cache));
        let tables = TableCache::new(Arc::clone(fs), dir, ctx.clone(), DEFAULT_MAX_OPEN_RUNS);
        let run = tables.get(id).unwrap();
        let mut rng = Rng(1);
        if capacity > 0 {
            let mut iter = run.iter(true);
            collect_forward(&mut iter);
            for key in keys.iter().step_by(1000) {
                run.get(key, MAX_SEQ, true).unwrap();
            }
        }
        let read_bytes = |ctx: &RunReadContext| ctx.stats.block_read_bytes();
        let before = read_bytes(&ctx);
        let t = Instant::now();
        for _ in 0..gets {
            let key = &keys[rng.below(n) as usize];
            let found = run.get(key, MAX_SEQ, true).unwrap();
            assert!(matches!(found, Lookup::Found { .. }));
        }
        let hit_ns = t.elapsed().as_nanos() as f64 / f64::from(gets);
        let hit_bytes = (read_bytes(&ctx) - before) / u64::from(gets);
        let before = read_bytes(&ctx);
        let t = Instant::now();
        for i in 0..gets {
            let mut key = key_of(n + u64::from(i));
            key.push(1);
            assert_eq!(run.get(&key, MAX_SEQ, true).unwrap(), Lookup::NotFound);
        }
        let miss_ns = t.elapsed().as_nanos() as f64 / f64::from(gets);
        let miss_bytes = (read_bytes(&ctx) - before) / u64::from(gets);
        println!(
            "{label}: point get present {hit_ns:.0} ns ({hit_bytes} B read), \
             absent {miss_ns:.0} ns ({miss_bytes} B read)"
        );
        for scan in [10usize, 128] {
            let mut iter = run.iter(capacity > 0);
            let before = read_bytes(&ctx);
            let t = Instant::now();
            for _ in 0..rounds {
                let key = &keys[rng.below(n - scan as u64) as usize];
                iter.seek(&internal_key(key, MAX_SEQ, Kind::Put));
                for _ in 0..scan {
                    assert!(iter.valid());
                    std::hint::black_box(iter.value());
                    iter.next();
                }
            }
            let ns = t.elapsed().as_nanos() as f64 / f64::from(rounds);
            let bytes = (read_bytes(&ctx) - before) / u64::from(rounds);
            println!(
                "{label}: seek + scan {scan}: {ns:.0} ns ({:.0} ns/entry, {bytes} B read)",
                ns / scan as f64
            );
        }
        println!("{label}: cache {:?}", cache.stats());
    }
}
