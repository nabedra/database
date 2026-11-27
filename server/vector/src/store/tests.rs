//! The store's rules, each over io_uring and over positioned reads: damage is an error and never a
//! vector, places are reused only after release, restart empties the files, both backends agree
//! with an in-RAM oracle, and an index scans alike whether its vectors live in RAM or on NVMe.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::fs::uring::Ring;
use nabedra_rt::fs::{Fault, FaultError, FaultFs, Fs, FsOp, OpenMode, os};

use super::*;
use crate::index::{Config, ExactCodec, Hit, Index, Query, ReadAt};
use crate::runtime::{Budget, Memory, Tier};
use crate::types::{Change, Error, Key, Op, Page, RangeId, Record, Row, Schema, Value};

const EIO: i32 = 5;
const ENOSPC: i32 = 28;

/// A fresh directory on the filesystem of the build, which takes `O_DIRECT` (tmpfs does not).
fn dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/vector-store-tests");
    let d = Path::new(base).join(format!("{name}-{}-{n}", std::process::id()));
    let _ = os::remove_dir_all(&d);
    os::create_dir_all(&d).unwrap();
    d
}

/// Whether to run the io_uring half of a test. A host that blocks io_uring has to say so with
/// `NABEDRA_VECTOR_NO_URING=1`, so a missing ring never passes silently.
fn have(backend: Backend) -> bool {
    if backend != Backend::Uring || Ring::new(8).is_ok() {
        return true;
    }
    if std::env::var_os("NABEDRA_VECTOR_NO_URING").is_some() {
        eprintln!("io_uring is unavailable here: skipped");
        return false;
    }
    panic!("io_uring is unavailable; set NABEDRA_VECTOR_NO_URING=1 to skip its half");
}

/// Faults below the store, on either backend.
#[derive(Debug, Default)]
struct Faults {
    /// The next write keeps the old second half of every page and reports success: a torn write
    /// that a power cut or a lying device leaves.
    tear: AtomicBool,
    write: Mutex<Option<i32>>,
    read: Mutex<Option<i32>>,
}

#[derive(Debug)]
struct FaultDisk {
    inner: Arc<dyn Disk>,
    faults: Arc<Faults>,
}

impl Disk for FaultDisk {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn read(&self, reqs: &mut [Req]) {
        if let Some(e) = *self.faults.read.lock().unwrap() {
            for r in reqs {
                r.result = Err(io::Error::from_raw_os_error(e));
            }
            return;
        }
        self.inner.read(reqs);
    }

    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        if let Some(e) = *self.faults.write.lock().unwrap() {
            return Err(io::Error::from_raw_os_error(e));
        }
        if !self.faults.tear.swap(false, Ordering::SeqCst) {
            return self.inner.write(offset, buf);
        }
        let n = buf.len() / PAGE;
        let mut old = [Req::new(offset, n)];
        self.inner.read(&mut old);
        let [old] = old;
        let mut torn = Pages::new(n);
        for p in 0..n {
            let (a, mid, b) = (p * PAGE, p * PAGE + PAGE / 2, (p + 1) * PAGE);
            torn.bytes_mut()[a..mid].copy_from_slice(&buf[a..mid]);
            if old.result.is_ok() {
                torn.bytes_mut()[mid..b].copy_from_slice(&old.buf.bytes()[mid..b]);
            }
        }
        self.inner.write(offset, torn.bytes())
    }
}

struct Files {
    dir: PathBuf,
    disks: Vec<Arc<dyn Disk>>,
    faults: Vec<Arc<Faults>>,
}

fn files(name: &str, backend: Backend, n: usize) -> Files {
    let dir = dir(name);
    let mut f = Files {
        dir: dir.clone(),
        disks: Vec::new(),
        faults: Vec::new(),
    };
    for c in 0..n {
        let inner = open(&dir.join(format!("raw-{c}")), backend, 2).unwrap();
        assert_eq!(inner.kind(), if backend == Backend::Uring { "io_uring" } else { "pread" });
        let faults = Arc::new(Faults::default());
        f.disks.push(Arc::new(FaultDisk {
            inner,
            faults: faults.clone(),
        }));
        f.faults.push(faults);
    }
    f
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = os::remove_dir_all(&self.dir);
    }
}

/// A vector that differs by key and in every dimension.
fn vector(dim: usize, seed: u64) -> Vec<f32> {
    (0..dim)
        .map(|i| ((seed.wrapping_mul(31).wrapping_add(i as u64) % 997) as f32 - 498.0) / 7.0)
        .collect()
}

fn key(slot: u32, at: u64) -> VecKey {
    VecKey { slot, at }
}

/// Flips one bit of a file through another descriptor.
fn flip(path: &Path, offset: u64) {
    let f = os::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut b = [0u8];
    f.read_exact_at(&mut b, offset).unwrap();
    b[0] ^= 0x10;
    f.write_all_at(&b, offset).unwrap();
    f.sync_data().unwrap();
}

/// Generates `uring` and `pread` tests from one body.
macro_rules! both {
    ($name:ident, $body:expr) => {
        mod $name {
            use super::*;
            #[test]
            fn uring() {
                if have(Backend::Uring) {
                    ($body)(Backend::Uring);
                }
            }
            #[test]
            fn pread() {
                ($body)(Backend::Pread);
            }
        }
    };
}

// ----- The files.

both!(vs16_vectors_read_back_bit_for_bit, |b| {
    // 3 dimensions pack records across pages; 1024 fill one each.
    for dim in [3, 768, 1024, 1500] {
        let f = files("roundtrip", b, 2);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        for i in 0..200u32 {
            s.put(key(i, u64::from(i) + 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let keys: Vec<VecKey> = (0..200u32).map(|i| key(i, u64::from(i) + 1)).collect();
        for (i, got) in s.get_many(&keys).into_iter().enumerate() {
            let want = vector(dim, i as u64);
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(bits(&got.unwrap()), bits(&want), "dim {dim} key {i}");
        }
        assert_eq!(s.stats().vectors, 200);
        assert!(s.stats().disk_bytes >= 200 * dim as u64 * 4);
    }
});

both!(vs16_files_are_opened_with_o_direct, |b| {
    let d = dir("direct");
    let disk = open(&d.join("raw-0"), b, 0).unwrap();
    // O_DIRECT refuses a write that is not page-aligned.
    assert!(disk.write(1, &[7u8; PAGE]).is_err());
    assert!(disk.write(0, Pages::new(1).bytes()).is_ok());
    let _ = os::remove_dir_all(&d);
});

both!(vs16_a_stored_version_is_not_stored_again, |b| {
    let f = files("twice", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), 8).unwrap();
    s.put(key(1, 1), &vector(8, 1)).unwrap();
    assert!(matches!(s.put(key(1, 1), &vector(8, 2)), Err(Error::Internal(_))));
    assert_eq!(&*s.get(key(1, 1)).unwrap(), &vector(8, 1)[..]);
});

both!(vs16_places_are_reused_only_after_release, |b| {
    let f = files("reuse", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), 16).unwrap();
    for i in 0..10 {
        s.put(key(i, 1), &vector(16, u64::from(i))).unwrap();
    }
    let file = &s.files()[0];
    let place = file.record_of(&key(3, 1)).unwrap();
    // Nothing released: a new version takes a new place.
    s.put(key(3, 2), &vector(16, 99)).unwrap();
    assert_ne!(file.record_of(&key(3, 2)), Some(place));
    assert_eq!(&*s.get(key(3, 1)).unwrap(), &vector(16, 3)[..]);
    // Released: the next version takes its place, and the old key reads nothing.
    s.release(key(3, 1));
    s.put(key(11, 1), &vector(16, 11)).unwrap();
    assert_eq!(file.record_of(&key(11, 1)), Some(place));
    assert!(s.get(key(3, 1)).is_err());
    s.flush().unwrap();
    assert_eq!(&*s.get(key(11, 1)).unwrap(), &vector(16, 11)[..]);
    for i in [0, 1, 2, 4, 9] {
        assert_eq!(&*s.get(key(i, 1)).unwrap(), &vector(16, u64::from(i))[..]);
    }
});

both!(vs10_restart_empties_the_files, |b| {
    let d = dir("restart");
    let s = NvmeVectors::open(&d, 2, 64, b, 2).unwrap();
    for i in 0..100 {
        s.put(key(i, 1), &vector(64, u64::from(i))).unwrap();
    }
    s.flush().unwrap();
    assert!(os::metadata(d.join("raw-0")).unwrap().len() > 0);
    drop(s);
    let s = NvmeVectors::open(&d, 2, 64, b, 2).unwrap();
    assert_eq!(os::metadata(d.join("raw-0")).unwrap().len(), 0);
    assert!(s.get(key(1, 1)).is_err(), "nothing of the last run is read");
    s.put(key(1, 1), &vector(64, 5)).unwrap();
    assert_eq!(&*s.get(key(1, 1)).unwrap(), &vector(64, 5)[..]);
    let _ = os::remove_dir_all(&d);
});

// ----- Damage.

// Review rv-dense (2026-11-27). A released record on a damaged page goes back to the top of the
// free list when its put fails, so every later put of the file takes it again and fails, although
// appending a new record would work. In an index each failed put fails the range (Error::Store),
// so a resync of any range of that core fails as soon as its puts reach that record.
#[test]
fn vs52_a_damaged_free_record_does_not_refuse_every_later_put_uring() {
    if have(Backend::Uring) {
        damaged_free_record(Backend::Uring);
    }
}

#[test]
fn vs52_a_damaged_free_record_does_not_refuse_every_later_put_pread() {
    damaged_free_record(Backend::Pread);
}

fn damaged_free_record(b: Backend) {
    let dim = 16; // 64 bytes: 64 records per page.
    let f = files("poisoned-free", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    for i in 0..256u32 {
        s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
    }
    s.flush().unwrap();
    // Damage page 0 (records 0 to 63) on disk, and release record 0.
    flip(&f.dir.join("raw-0"), 100);
    s.release(key(0, 1));
    // Taking the released record has to read page 0: an error, once, is fair.
    assert!(matches!(s.put(key(1000, 1), &vector(dim, 1000)), Err(Error::Store(_))));
    // Later puts of other versions must not all fail on the same damaged record.
    let failed = (1001..1011u32)
        .filter(|&i| s.put(key(i, 1), &vector(dim, u64::from(i))).is_err())
        .count();
    assert_eq!(failed, 0, "{failed} of 10 later puts failed on the one damaged free record");
}

// The same for every record of a damaged page: one put learns that the page is damaged, and no
// later put takes a record of it, neither one released before nor after, nor a new record that
// would share the page with the tail on disk. Every later put reads back exactly.
both!(vs52_a_damaged_page_costs_one_failed_put_whatever_its_records, |b| {
    let dim = 16; // 64 bytes: 64 records per page.
    let later = |s: &NvmeVectors, from: u32| {
        let failed = (from..from + 10)
            .filter(|&i| s.put(key(i, 1), &vector(dim, u64::from(i))).is_err())
            .count();
        s.flush().unwrap();
        for i in from..from + 10 {
            if let Ok(v) = s.get(key(i, 1)) {
                assert_eq!(&*v, &vector(dim, u64::from(i))[..], "{i}");
            }
        }
        failed
    };
    // Records released on a damaged page, before and after the put that finds it damaged.
    let f = files("bad-page-free", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    for i in 0..256u32 {
        s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
    }
    s.flush().unwrap();
    flip(&f.dir.join("raw-0"), 100);
    for i in 0..5 {
        s.release(key(i, 1));
    }
    assert_eq!(later(&s, 1000), 1, "records released on one damaged page");
    for i in 5..10 {
        s.release(key(i, 1));
    }
    assert_eq!(later(&s, 2000), 0, "records released on a page known to be damaged");
    // A new record whose page is the damaged tail on disk.
    let f = files("bad-page-tail", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    for i in 0..70u32 {
        s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
    }
    s.flush().unwrap();
    flip(&f.dir.join("raw-0"), 4096 + 100);
    assert_eq!(later(&s, 1000), 1, "new records on a damaged tail page");
    assert_eq!(later(&s, 2000), 0);
    assert_eq!(&*s.get(key(3, 1)).unwrap(), &vector(dim, 3)[..], "page 0 is whole");
});

/// Every key reads back exactly what was put, or an error.
fn exact_or_error(s: &NvmeVectors, dim: usize, keys: &[(VecKey, u64)]) -> usize {
    let mut errors = 0;
    for &(k, seed) in keys {
        match s.get(k) {
            Ok(v) => assert_eq!(&*v, &vector(dim, seed)[..], "{k:?} read another vector"),
            Err(Error::Store(_)) => errors += 1,
            Err(e) => panic!("{k:?}: {e}"),
        }
    }
    errors
}

both!(vs10_a_torn_page_reads_as_an_error, |b| {
    let dim = 300;
    let f = files("torn", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    let mut keys = Vec::new();
    // 40 records of 1,200 bytes stay in RAM until the flush: 12 pages, below a flush's 16.
    for i in 0..40 {
        s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        keys.push((key(i, 1), u64::from(i)));
    }
    assert_eq!(exact_or_error(&s, dim, &keys), 0);
    // The write reports success, but the second half of every page keeps what was on disk.
    f.faults[0].tear.store(true, Ordering::SeqCst);
    s.flush().unwrap();
    // Every page is torn, and its checksum covers the whole page, so no record of it reads.
    assert_eq!(exact_or_error(&s, dim, &keys), 40);
    // Reusing a place on a torn page has to read that page, so the put is refused.
    s.release(key(0, 1));
    s.put(key(50, 2), &vector(dim, 50)).unwrap_err();
});

both!(vs10_a_wrong_checksum_reads_as_an_error, |b| {
    let dim = 100;
    let f = files("crc", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    let keys: Vec<_> = (0..100).map(|i| (key(i, 1), u64::from(i))).collect();
    for &(k, seed) in &keys {
        s.put(k, &vector(dim, seed)).unwrap();
    }
    s.flush().unwrap();
    // Key 30 is bytes 12,000..12,400: page 2.
    flip(&f.dir.join("raw-0"), 12_100);
    let errors = exact_or_error(&s, dim, &keys);
    assert!(matches!(s.get(key(30, 1)), Err(Error::Store(_))));
    // Only the records on that page.
    assert!((1..=12).contains(&errors), "{errors}");
});

both!(vs10_a_short_read_is_an_error, |b| {
    let dim = 256;
    let f = files("short", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    let keys: Vec<_> = (0..64).map(|i| (key(i, 1), u64::from(i))).collect();
    for &(k, seed) in &keys {
        s.put(k, &vector(dim, seed)).unwrap();
    }
    s.flush().unwrap();
    // The file loses its last pages.
    os::OpenOptions::new().write(true).open(f.dir.join("raw-0")).unwrap().set_len(10 * PAGE as u64).unwrap();
    let errors = exact_or_error(&s, dim, &keys);
    assert_eq!(errors, 64 - 40, "records beyond page 10 fail, the others read");
});

both!(vs10_enospc_stores_nothing_and_loses_nothing, |b| {
    let dim = 1024;
    let f = files("enospc", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    *f.faults[0].write.lock().unwrap() = Some(ENOSPC);
    let mut stored = Vec::new();
    let mut refused = 0;
    for i in 0..40 {
        match s.put(key(i, 1), &vector(dim, u64::from(i))) {
            Ok(()) => stored.push((key(i, 1), u64::from(i))),
            Err(Error::Store(why)) => {
                assert!(why.contains("No space") || why.contains("28"), "{why}");
                refused += 1;
                assert!(s.get(key(i, 1)).is_err(), "a refused put stored something");
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert!(refused > 0 && !stored.is_empty());
    assert_eq!(exact_or_error(&s, dim, &stored), 0, "stored vectors stay readable");
    *f.faults[0].write.lock().unwrap() = None;
    s.put(key(100, 1), &vector(dim, 100)).unwrap();
    s.flush().unwrap();
    stored.push((key(100, 1), 100));
    assert_eq!(exact_or_error(&s, dim, &stored), 0);
});

both!(vs10_eio_reads_as_an_error, |b| {
    let dim = 512;
    let f = files("eio", b, 1);
    let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
    let keys: Vec<_> = (0..50).map(|i| (key(i, 1), u64::from(i))).collect();
    for &(k, seed) in &keys {
        s.put(k, &vector(dim, seed)).unwrap();
    }
    s.flush().unwrap();
    *f.faults[0].read.lock().unwrap() = Some(EIO);
    assert_eq!(exact_or_error(&s, dim, &keys), 50);
    *f.faults[0].write.lock().unwrap() = Some(EIO);
    assert!(s.put(key(60, 1), &vector(dim, 60)).is_err() || s.flush().is_err());
    *f.faults[0].read.lock().unwrap() = None;
    *f.faults[0].write.lock().unwrap() = None;
    assert_eq!(exact_or_error(&s, dim, &keys), 0);
});

/// The repository's fault layer, under positioned reads: an I/O error and a full disk on a
/// FaultFs file, and a bit flip in its contents.
#[test]
fn vs10_faultfs_errors_and_flips_read_as_errors() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/v"));
    let file = fs.open(Path::new("/v/raw-0"), OpenMode::CreateNew).unwrap();
    let disk: Arc<dyn Disk> = Arc::new(PreadDisk::new(file, 2).unwrap());
    let dim = 200;
    let s = NvmeVectors::on(vec![disk], dim).unwrap();
    let keys: Vec<_> = (0..60).map(|i| (key(i, 1), u64::from(i))).collect();
    for &(k, seed) in &keys {
        s.put(k, &vector(dim, seed)).unwrap();
    }
    s.flush().unwrap();
    fs.inject(Fault::on(FsOp::Read).range(0, PAGE as u64).repeat());
    let errors = exact_or_error(&s, dim, &keys);
    assert!((1..=6).contains(&errors), "{errors}");
    fs.clear_faults();
    fs.flip_bit(Path::new("/v/raw-0"), 5 * PAGE as u64 + 9, 3).unwrap();
    assert!(exact_or_error(&s, dim, &keys) > 0);
    fs.inject(Fault::on(FsOp::Write).error(FaultError::Enospc).repeat());
    let mut refused = false;
    // 200 records of 800 bytes fill more than the 16 pages that start a flush.
    for i in 100..300 {
        refused |= s.put(key(i, 1), &vector(dim, u64::from(i))).is_err();
    }
    assert!(refused, "a full disk refuses puts");
}

#[test]
fn vs10_page_checksums_are_crc32c() {
    assert_eq!(file::crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(file::crc32c(&[]), 0);
}

// ----- io_uring against positioned reads against RAM.

#[derive(Clone, Debug)]
enum Step {
    Put(VecKey, u64),
    Release(VecKey),
    Get(Vec<VecKey>),
    Flush,
}

fn steps(g: &mut Gen) -> Vec<Step> {
    let mut out = Vec::new();
    let mut at = 1;
    for _ in 0..g.below(300) + 1 {
        let k = key(g.below(40) as u32, at);
        out.push(match g.below(8) {
            0..=2 => {
                at += 1;
                Step::Put(k, g.u64())
            }
            3 => Step::Release(key(k.slot, g.below(at))),
            4 => Step::Flush,
            _ => Step::Get((0..g.below(8) + 1).map(|_| key(g.below(40) as u32, g.below(at + 1))).collect()),
        });
    }
    out
}

type Outcome = std::result::Result<Vec<u32>, bool>;

fn outcome(r: crate::types::Result<Arc<[f32]>>) -> Outcome {
    r.map(|v| v.iter().map(|x| x.to_bits()).collect())
        .map_err(|e| matches!(e, Error::Store(_)))
}

fn replay(s: &dyn Vectors, dim: usize, steps: &[Step]) -> Vec<Vec<Outcome>> {
    let mut out = Vec::new();
    for step in steps {
        out.push(match step {
            Step::Put(k, seed) => vec![s.put(*k, &vector(dim, *seed)).map(|()| Vec::new()).map_err(|_| false)],
            Step::Release(k) => {
                s.release(*k);
                Vec::new()
            }
            Step::Get(keys) => s.get_many(keys).into_iter().map(outcome).collect(),
            // The caller flushes a store with pages.
            Step::Flush => Vec::new(),
        });
    }
    out
}

#[test]
fn vs16_io_uring_positioned_reads_and_ram_agree() {
    let runs = Runs { cases: Runs::default().cases / 8 + 1, ..Runs::default() };
    let uring = have(Backend::Uring);
    check_with(runs, "vs16_io_uring_positioned_reads_and_ram_agree", |g| {
        let dim = *g.pick(&[1usize, 5, 300, 1024, 1100]);
        let steps = steps(g);
        let want = replay(&RamVectors::new(), dim, &steps);
        let mut backends = vec![Backend::Pread];
        if uring {
            backends.push(Backend::Uring);
        }
        for b in backends {
            let f = files("diff", b, 2);
            let s = NvmeVectors::on(f.disks.clone(), dim).map_err(|e| e.to_string())?;
            // Flushes write pages out, so later reads go to the disk.
            let mut got = Vec::new();
            for step in &steps {
                if let Step::Flush = step {
                    s.flush().map_err(|e| e.to_string())?;
                }
                got.extend(replay(&s, dim, std::slice::from_ref(step)));
            }
            if got != want {
                return Err(format!("{b:?} differs from RAM"));
            }
        }
        Ok(())
    });
}

// ----- Code extensions.

#[test]
fn vs17_extensions_stay_in_ram_while_the_budget_lasts() {
    let f = files("ext", Backend::Pread, 2);
    let memory = Arc::new(Memory::new(10 * 96));
    let ext = Extensions::new(96, memory.clone(), f.disks.clone()).unwrap();
    let bytes = |i: u64| -> Vec<u8> { (0..96).map(|j| (i * 7 + j) as u8).collect() };
    let k = |i: u64| ExtKey { part: (i % 3) as u32, id: i };
    let tiers: Vec<Tier> = (0..16).map(|i| ext.put(k(i), &bytes(i)).unwrap()).collect();
    assert_eq!(tiers.iter().filter(|t| **t == Tier::Ram).count(), 10);
    assert!(tiers[10..].iter().all(|t| *t == Tier::Nvme), "{tiers:?}");
    ext.flush().unwrap();
    for i in 0..16 {
        assert_eq!(&*ext.get(&k(i)).unwrap(), &bytes(i)[..]);
    }
    assert_eq!(memory.used(), (0, 10 * 96));
    // Releasing one in RAM gives its memory back to the next.
    ext.release(&k(0));
    assert_eq!(ext.put(k(100), &bytes(100)).unwrap(), Tier::Ram);
    ext.release(&k(12));
    assert_eq!(ext.tier(&k(12)), None);
    assert!(ext.get(&k(12)).is_err());
}

#[test]
fn vs17_one_bit_codes_always_stay_in_ram() {
    let f = files("codes", Backend::Pread, 1);
    let memory = Arc::new(Memory::new(1000));
    let ext = Extensions::new(10, memory.clone(), f.disks.clone()).unwrap();
    assert_eq!(ext.put(ExtKey { part: 0, id: 0 }, &[1; 10]).unwrap(), Tier::Ram);
    // Codes beyond the limit are still charged, and extensions go to NVMe from then on.
    memory.charge_codes(5000);
    assert_eq!(memory.used(), (5000, 10));
    assert_eq!(ext.put(ExtKey { part: 0, id: 1 }, &[2; 10]).unwrap(), Tier::Nvme);
    memory.release_codes(5000);
    assert_eq!(ext.put(ExtKey { part: 0, id: 2 }, &[3; 10]).unwrap(), Tier::Ram);
    assert_eq!(&*ext.get(&ExtKey { part: 0, id: 1 }).unwrap(), &[2; 10]);
}

#[test]
fn vs10_a_damaged_extension_on_nvme_reads_as_an_error() {
    let f = files("extdmg", Backend::Pread, 1);
    let ext = Extensions::new(64, Arc::new(Memory::new(0)), f.disks.clone()).unwrap();
    for i in 0..100 {
        assert_eq!(ext.put(ExtKey { part: 0, id: i }, &[i as u8; 64]).unwrap(), Tier::Nvme);
    }
    ext.flush().unwrap();
    flip(&f.dir.join("raw-0"), 70);
    assert!(matches!(ext.get(&ExtKey { part: 0, id: 1 }), Err(Error::Store(_))));
    assert_eq!(&*ext.get(&ExtKey { part: 0, id: 99 }).unwrap(), &[99u8; 64]);
}

// ----- The index over the store.

const RANGE: RangeId = RangeId(9);
const DIM: usize = 6;

fn ix_config(vectors: Option<Arc<dyn Vectors>>) -> Config {
    let schema = Schema {
        dim: DIM,
        columns: vec![crate::types::ColumnType::Int],
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

fn serving(cfg: Config) -> Index<ExactCodec> {
    let mut ix = Index::new(cfg, ExactCodec, None).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    ix
}

fn rkey(n: u64) -> Key {
    Arc::new(n.to_be_bytes())
}

fn row_vec(g: &mut Gen) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 0.5, 9.0, 1e-3, 0.0]);
    (0..DIM).map(|_| g.range_i64(-8, 8) as f32 / 8.0 * scale).collect()
}

fn feed(ix: &mut Index<ExactCodec>, changes: &[(u64, Option<Vec<f32>>)], after: u64) -> crate::types::Result<u64> {
    let records = changes.iter().enumerate().map(|(i, (k, v))| {
        let at = after + 1 + i as u64;
        let row = v.as_ref().map(|v| Row {
            vec: Some(v.as_slice().into()),
            cols: vec![Value::Int(*k as i64 % 3)],
            image: Arc::new(k.to_be_bytes()),
        });
        Record {
            at,
            change: Change::Row {
                key: rkey(*k),
                commit_ts: at * 10,
                row,
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

fn search(ix: &mut Index<ExactCodec>, q: &Query) -> crate::types::Result<Vec<Hit>> {
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None)?;
    ix.search(&pin, q)
}

struct Indexes {
    _files: Vec<Files>,
    ixs: Vec<(String, Index<ExactCodec>)>,
    nvme: Vec<Arc<NvmeVectors>>,
}

fn indexes() -> Indexes {
    let mut out = Indexes {
        _files: Vec::new(),
        ixs: vec![("rows".into(), serving(ix_config(None)))],
        nvme: Vec::new(),
    };
    out.ixs.push(("ram".into(), serving(ix_config(Some(Arc::new(RamVectors::new()))))));
    let mut backends = vec![Backend::Pread];
    if have(Backend::Uring) {
        backends.push(Backend::Uring);
    }
    for b in backends {
        let f = files("index", b, 3);
        let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
        out.nvme.push(s.clone());
        out.ixs.push((format!("{b:?}"), serving(ix_config(Some(s)))));
        out._files.push(f);
    }
    out
}

/// Exact and approximate scans under every operation, while rows change and partitions split and
/// merge, return the same hits, rows and scores whether vectors are in rows, in a RAM store or
/// on NVMe through either backend.
#[test]
fn vs07_scans_are_identical_from_ram_and_from_nvme() {
    let runs = Runs {
        cases: Runs::default().cases / 8 + 2,
        max_shrink_steps: 200,
        ..Runs::default()
    };
    check_with(runs, "vs07_scans_are_identical_from_ram_and_from_nvme", |g| {
        let mut set = indexes();
        let mut at = 0;
        for round in 0..g.below(6) + 1 {
            let changes: Vec<(u64, Option<Vec<f32>>)> = (0..g.below(30) + 1)
                .map(|_| (g.below(50), (!g.chance(1, 6)).then(|| row_vec(g))))
                .collect();
            let mut next = at;
            // Every index gets the same maintenance, so they hold the same partitions.
            let maintain = g.below(4);
            for (_, ix) in &mut set.ixs {
                next = feed(ix, &changes, at).map_err(|e| e.to_string())?;
                for _ in 0..maintain {
                    ix.maintain(8);
                    ix.run();
                }
                ix.advance_low(RANGE, next.saturating_sub(5)).map_err(|e| e.to_string())?;
            }
            at = next;
            if round % 2 == 0 {
                set.nvme.iter().try_for_each(|s| s.flush()).map_err(|e| e.to_string())?;
            }
            for _ in 0..4 {
                let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
                let mut vec = row_vec(g);
                if op == Op::Cosine && vec.iter().all(|x| *x == 0.0) {
                    vec[0] = 1.0;
                }
                let q = Query {
                    op,
                    vec,
                    limit: g.below(12) as usize + 1,
                    filter: None,
                    exact: g.bool(),
                    recall: 0.9,
                };
                let mut results = set.ixs.iter_mut().map(|(name, ix)| (name.clone(), search(ix, &q)));
                let (_, first) = results.next().ok_or("no index")?;
                let first = first.map_err(|e| e.to_string())?;
                for (name, r) in results {
                    if r.as_ref() != Ok(&first) {
                        return Err(format!("{name} differs: {r:?} against {first:?}"));
                    }
                }
            }
        }
        Ok(())
    });
}

fn exact(v: &[f32], limit: usize) -> Query {
    Query {
        op: Op::L2,
        vec: v.to_vec(),
        limit,
        filter: None,
        exact: true,
        recall: 1.0,
    }
}

/// A version's vector stays readable while a pin can see it, even after its row was updated,
/// deleted and its cut retired, and its place is reused only after the pin ended.
#[test]
fn vs05_a_pinned_version_keeps_its_vector_until_the_pin_ends() {
    let f = files("pinned", Backend::Pread, 3);
    let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
    let mut ix = serving(ix_config(Some(s.clone())));
    let old = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let at = feed(&mut ix, &[(1, Some(old.clone())), (2, Some(vec![0.5; DIM]))], 0).unwrap();
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let at = feed(&mut ix, &[(1, Some(vec![9.0; DIM])), (1, None)], at).unwrap();
    ix.advance_low(RANGE, at).unwrap();
    for _ in 0..20 {
        ix.maintain(64);
        ix.run();
    }
    s.flush().unwrap();
    // New rows would take any released place.
    let fill: Vec<_> = (10..60).map(|k| (k, Some(vec![k as f32; DIM]))).collect();
    let at = feed(&mut ix, &fill, at).unwrap();
    s.flush().unwrap();
    let hits = ix.search(&pin, &exact(&old, 1)).unwrap();
    assert_eq!(hits[0].row.vec.as_deref(), Some(&old[..]), "the pinned version was overwritten");
    assert_eq!(hits[0].score, 0.0);
    let free_before: usize = s.file_stats().iter().map(|f| f.free).sum();
    drop(pin);
    for _ in 0..20 {
        ix.maintain(64);
        ix.run();
    }
    let free_after: usize = s.file_stats().iter().map(|f| f.free).sum();
    assert!(free_after > free_before, "the versions were not released: {free_before} {free_after}");
    // The places are taken again, and every row still reads its own vector.
    feed(&mut ix, &[(100, Some(vec![7.0; DIM]))], at).unwrap();
    let hits = search(&mut ix, &exact(&[7.0; DIM], 1)).unwrap();
    assert_eq!(hits[0].key, rkey(100));
    for k in 10..60u64 {
        let hits = search(&mut ix, &exact(&[k as f32; DIM], 1)).unwrap();
        assert_eq!(hits[0].row.vec.as_deref(), Some(&vec![k as f32; DIM][..]));
    }
    assert!(search(&mut ix, &exact(&old, 60)).unwrap().iter().all(|h| h.key != rkey(1)));
}

/// A damaged page fails the query that reads it with an explicit error, and the range goes out
/// of service until it resyncs; no query answers from a damaged vector.
#[test]
fn vs10_damage_under_an_index_fails_queries_and_the_range() {
    let f = files("ixdamage", Backend::Pread, 3);
    let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
    let mut ix = serving(ix_config(Some(s.clone())));
    let rows: Vec<_> = (0..300u64).map(|k| (k, Some(vec![k as f32 / 10.0 + 0.1; DIM]))).collect();
    feed(&mut ix, &rows, 0).unwrap();
    s.flush().unwrap();
    for c in 0..3 {
        flip(&f.dir.join(format!("raw-{c}")), 100);
    }
    let r = search(&mut ix, &exact(&[0.1; DIM], 300));
    assert!(matches!(r, Err(Error::Store(_))), "{r:?}");
    ix.run();
    let status = ix.status(RANGE).unwrap();
    assert_eq!(status.damage.as_deref(), Some("the vector store failed"));
    assert!(search(&mut ix, &exact(&[0.1; DIM], 300)).is_err());
}

/// A store that cannot take a vector fails the range instead of losing the row.
#[test]
fn vs10_a_full_store_fails_the_range() {
    let f = files("ixfull", Backend::Pread, 3);
    let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
    let mut ix = serving(ix_config(Some(s)));
    for fault in &f.faults {
        *fault.write.lock().unwrap() = Some(ENOSPC);
    }
    // Vectors of 24 bytes: 16 pages of one core's file, which start its flush, hold 2,730.
    let rows: Vec<_> = (0..9000u64).map(|k| (k, Some(vec![1.0; DIM]))).collect();
    let r = feed(&mut ix, &rows, 0);
    assert!(matches!(r, Err(Error::Store(_))), "{r:?}");
    let status = ix.status(RANGE).unwrap();
    assert_eq!(status.damage.as_deref(), Some("the vector store failed"));
}

/// A checkpoint of an index on NVMe recovers into one on NVMe with the same answers.
#[test]
fn vs10_checkpoint_and_recovery_over_nvme() {
    let fs = FaultFs::new();
    let store = crate::index::Store::new(Arc::new(fs.clone()), "/ck");
    let f = files("ckpt", Backend::Pread, 3);
    let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
    let cfg = ix_config(Some(s.clone()));
    let mut ix = Index::new(cfg.clone(), ExactCodec, Some(store.clone())).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    let rows: Vec<_> = (0..200u64).map(|k| (k, Some(vec![(k % 17) as f32 - 8.0, 1.0, 2.0, (k % 5) as f32, 0.5, -1.0]))).collect();
    let at = feed(&mut ix, &rows, 0).unwrap();
    ix.checkpoint().unwrap();
    let q = exact(&[1.0, 1.0, 2.0, 3.0, 0.5, -1.0], 20);
    let want = search(&mut ix, &q).unwrap();
    drop(ix);
    let f2 = files("ckpt2", Backend::Pread, 3);
    let s2 = Arc::new(NvmeVectors::on(f2.disks.clone(), DIM).unwrap());
    let (mut back, damage) = Index::recover(ix_config(Some(s2.clone())), ExactCodec, store).unwrap();
    assert!(damage.is_empty());
    assert_eq!(s2.stats().vectors, 200, "recovery wrote every vector again");
    back.caught_up(RANGE, at).unwrap();
    assert_eq!(search(&mut back, &q).unwrap(), want);
}

// ----- Budgets on the scan.

fn budget_index(vectors: Option<Arc<dyn Vectors>>) -> Index<ExactCodec> {
    let mut ix = serving(ix_config(vectors));
    let rows: Vec<_> = (0..200u64)
        .map(|k| (k, Some((0..DIM).map(|i| ((k * 7 + i as u64) % 13) as f32 - 6.0).collect())))
        .collect();
    feed(&mut ix, &rows, 0).unwrap();
    ix
}

#[test]
fn vs17_a_query_over_budget_ends_with_an_error_and_never_in_part() {
    let f = files("budget", Backend::Pread, 3);
    let s = Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap());
    let mut ix = budget_index(Some(s));
    let q = exact(&[1.0, -2.0, 3.0, 0.0, 1.0, 2.0], 10);
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let full = ix.search(&pin, &q).unwrap();
    let within = |b: Budget| ix.search_within(&pin, &q, b);
    let unlimited = Budget::UNLIMITED;
    assert_eq!(within(unlimited).unwrap(), full);
    // An exact scan scores 200 rows of 6 dimensions and reads 200 vectors of 24 bytes.
    assert_eq!(within(Budget { cpu: 1199, ..unlimited }), Err(Error::OverBudget("cpu")));
    assert_eq!(within(Budget { cpu: 1200, ..unlimited }).unwrap(), full);
    assert_eq!(within(Budget { io: 200 * 24 - 1, ..unlimited }), Err(Error::OverBudget("io")));
    assert_eq!(within(Budget { io: 200 * 24, ..unlimited }).unwrap(), full);
    assert_eq!(within(Budget { memory: 100, ..unlimited }), Err(Error::OverBudget("memory")));
    // Driving the scan by hand: after the error, finishing it returns nothing either.
    let mut scan = ix
        .scan_within(&pin, &q, Box::new(crate::index::RecallEstimate), Budget { cpu: 600, ..unlimited })
        .unwrap();
    let mut ended = None;
    for _ in 0..1000 {
        match ix.visit(&mut scan) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                ended = Some(e);
                break;
            }
        }
    }
    assert_eq!(ended, Some(Error::OverBudget("cpu")));
    assert_eq!(ix.finish(scan), Err(Error::OverBudget("cpu")));
}

#[test]
fn vs17_any_budget_gives_the_whole_result_or_an_error() {
    let mut ix = budget_index(None);
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap();
    check_with(Runs::default(), "vs17_any_budget_gives_the_whole_result_or_an_error", |g| {
        let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
        let q = Query {
            op,
            vec: (0..DIM).map(|_| g.range_i64(1, 9) as f32).collect(),
            limit: g.below(20) as usize + 1,
            filter: None,
            exact: g.bool(),
            recall: 0.9,
        };
        let full = ix.search(&pin, &q).map_err(|e| e.to_string())?;
        let b = Budget {
            cpu: g.below(3000),
            io: u64::MAX,
            memory: g.below(4000),
        };
        match ix.search_within(&pin, &q, b) {
            Ok(hits) if hits == full => Ok(()),
            Err(Error::OverBudget(_)) => Ok(()),
            other => Err(format!("{b:?}: {other:?} against {full:?}")),
        }
    });
}

/// Batched random reads of 768-d vectors through each backend, under a second each. Run with
/// `--ignored --release`; the numbers depend on the host's disk.
#[test]
#[ignore]
fn vs19_micro_store_reads() {
    let (dim, n, batch) = (768usize, 20_000u32, 64usize);
    let mut backends = vec![Backend::Pread];
    if have(Backend::Uring) {
        backends.push(Backend::Uring);
    }
    for b in backends {
        let f = files("micro", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        let start = nabedra_rt::time::now_std();
        for i in 0..n {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let write = nabedra_rt::time::elapsed(start);
        let mut g = Gen::from_seed(7);
        let (start, mut reads) = (nabedra_rt::time::now_std(), 0u64);
        while nabedra_rt::time::elapsed(start).as_millis() < 1000 {
            let keys: Vec<VecKey> = (0..batch).map(|_| key(g.below(u64::from(n)) as u32, 1)).collect();
            for r in s.get_many(&keys) {
                r.unwrap();
            }
            reads += batch as u64;
        }
        let secs = nabedra_rt::time::elapsed(start).as_secs_f64();
        eprintln!(
            "{b:?}: wrote {n} x {dim}-d in {:.0} ms, {:.0} reads/s in batches of {batch}, {:.1} us per batch",
            write.as_secs_f64() * 1e3,
            reads as f64 / secs,
            secs * 1e6 * batch as f64 / reads as f64
        );
    }
}

// ----- Reads under rewrites.

/// Rewrites the page of record 1 on another thread whenever the reader thread reads, and waits up
/// to 300 ms for that rewrite, so that every read of a page meets a change.
#[derive(Debug)]
struct ChurnDisk {
    inner: Arc<dyn Disk>,
    file: std::sync::OnceLock<std::sync::Weak<RecordFile<u32>>>,
    reader: Mutex<Option<std::thread::ThreadId>>,
    next: AtomicU64,
    writers: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Disk for ChurnDisk {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn read(&self, reqs: &mut [Req]) {
        let me = Some(std::thread::current().id());
        let file = self.file.get().and_then(std::sync::Weak::upgrade);
        if let Some(file) = file.filter(|_| *self.reader.lock().unwrap() == me) {
            let n = self.next.fetch_add(1, Ordering::SeqCst) as u32;
            let (done, wait) = std::sync::mpsc::channel();
            let rewrite = move || {
                let old = if n == 0 { 1 } else { 1000 + n - 1 };
                file.release(&old);
                file.put(1000 + n, &[n as u8 + 1; 2048]).unwrap();
                file.flush().unwrap();
                let _ = done.send(());
            };
            let writer = nabedra_rt::task::spawn_service(format!("churn-{n}"), rewrite).unwrap();
            let _ = wait.recv_timeout(std::time::Duration::from_millis(300));
            self.writers.lock().unwrap().push(writer);
        }
        self.inner.read(reqs);
    }

    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write(offset, buf)
    }
}

/// A record that never changes reads back exactly however often the page it shares is rewritten
/// while it is read: retries end in a read that no writer can change, never in an error. An error
/// here fails the whole range in the index (task B2b found `a page kept changing` under load).
#[test]
fn vs10_a_stable_record_reads_back_while_its_page_changes_under_every_read() {
    let f = files("churn", Backend::Pread, 1);
    let disk = Arc::new(ChurnDisk {
        inner: f.disks[0].clone(),
        file: std::sync::OnceLock::new(),
        reader: Mutex::new(None),
        next: AtomicU64::new(0),
        writers: Mutex::new(Vec::new()),
    });
    // Two records of 2 KiB to a page: 0 and 1 share one.
    let file = Arc::new(RecordFile::new("churn", disk.clone() as Arc<dyn Disk>, 2048).unwrap());
    file.put(0, &[7; 2048]).unwrap();
    file.put(1, &[9; 2048]).unwrap();
    file.flush().unwrap();
    disk.file.set(Arc::downgrade(&file)).unwrap();
    *disk.reader.lock().unwrap() = Some(std::thread::current().id());
    let got = file.get(&0);
    *disk.reader.lock().unwrap() = None;
    for w in std::mem::take(&mut *disk.writers.lock().unwrap()) {
        w.join().unwrap();
    }
    assert_eq!(got.map(|v| v == [7; 2048]).map_err(|e| e.to_string()), Ok(true));
    assert!(disk.next.load(Ordering::SeqCst) >= 2, "the page changed under the reads");
}

// ----- Review rv-dense suspicions (task fix-dense, 2026-11-27).

/// Puts of one extension key that race take one place and one charge: exactly one succeeds, the
/// key is held once, the code memory holds what RAM holds, and releasing the key gives every byte
/// back. With room for many in RAM the racers meet there; with room for one more, they split
/// between RAM and NVMe.
#[test]
fn vs17_racing_puts_of_one_extension_take_one_place_and_one_charge() {
    for room in [1u64, 1000] {
        let f = files("ext-race", Backend::Pread, 1);
        let memory = Arc::new(Memory::new(40 * 64));
        let ext = Extensions::new(64, memory.clone(), f.disks.clone()).unwrap();
        // Others hold all of RAM but `room` extensions.
        let others = 40u64.saturating_sub(room);
        for id in 0..others {
            ext.put(ExtKey { part: 1, id }, &[0; 64]).unwrap();
        }
        let start = std::sync::Barrier::new(4);
        for id in 0..300u64 {
            let k = ExtKey { part: 0, id };
            let ok = std::thread::scope(|s| {
                let put = || {
                    start.wait();
                    ext.put(k, &[id as u8; 64]).is_ok()
                };
                let all: Vec<_> = (0..4).map(|_| s.spawn(put)).collect();
                all.into_iter().filter_map(|t| t.join().ok()).filter(|ok| *ok).count()
            });
            let (ram, nvme) = ext.counts();
            let held = ram + nvme - others as usize;
            assert_eq!((ok, held), (1, 1), "room {room}, key {id}: {ok} puts succeeded, held {held} times");
            assert_eq!(memory.used().1, ram as u64 * 64, "room {room}, key {id}: memory against RAM");
            assert_eq!(&*ext.get(&k).unwrap(), &[id as u8; 64]);
            ext.release(&k);
            assert_eq!(memory.used().1, others * 64, "room {room}, key {id}: released, still charged");
            assert_eq!(ext.tier(&k), None);
        }
    }
}

/// A file whose reads panic, as a buggy filesystem layer might.
struct PanickyFile;

impl nabedra_rt::fs::FsFile for PanickyFile {
    fn write_all_at(&self, _: &[u8], _: u64) -> io::Result<()> {
        Ok(())
    }
    fn read_exact_at(&self, _: &mut [u8], offset: u64) -> io::Result<()> {
        std::panic::panic_any(format!("a read at {offset} panics"))
    }
    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
    fn preallocate(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(0)
    }
}

/// A read whose pool threads all panicked answers, with an error for every request, and so does
/// every later read: a panicking read must not take its pool thread with it, or a batch queued
/// behind dead threads waits for ever.
#[test]
fn vs10_a_pool_whose_reads_all_panic_still_answers_every_read() {
    let disk = Arc::new(PreadDisk::new(Arc::new(PanickyFile), 2).unwrap());
    let (tell, told) = std::sync::mpsc::channel();
    let reader = disk.clone();
    let reads = move || {
        for _ in 0..4 {
            let mut reqs: Vec<Req> = (0..3).map(|i| Req::new(i * PAGE as u64, 1)).collect();
            reader.read(&mut reqs);
            let _ = tell.send(reqs.iter().all(|r| r.result.is_err()));
        }
    };
    let thread = nabedra_rt::task::spawn_service("vs10-panicky-reads", reads).unwrap();
    for round in 0..4 {
        let answer = told.recv_timeout(std::time::Duration::from_secs(10));
        assert_eq!(answer, Ok(true), "read {round} of a pool whose reads panicked");
    }
    thread.join().unwrap();
}
