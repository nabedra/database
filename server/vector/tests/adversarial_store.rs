//! Adversarial tests of the NVMe store (task N): damage, faults at every point, io_uring against
//! positioned reads, the ring itself, `O_DIRECT` alignment, slot reuse under an index, and hostile
//! input. Written by someone who did not write the code under test. Each test names the rule it
//! guards, and fails if that rule is removed.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::fs::os;
use nabedra_rt::fs::uring::{Op as RingOp, Ring};
use nabedra_rt::fs::{FaultFs, Fs};
use nabedra_vector::index::{ExactCodec, Index, ReadAt, Store};
use nabedra_vector::runtime::{Memory, Tier};
use nabedra_vector::store::{
    Backend, ExtKey, Extensions, NvmeVectors, PAGE, Pages, RamVectors, Req, VecKey, Vectors, open,
};
use nabedra_vector::types::{Error, Op};

type Res<T> = nabedra_vector::types::Result<T>;

fn exact_or_store_error(r: Res<Arc<[f32]>>, want: &[f32], what: &str) -> bool {
    match r {
        Ok(v) => {
            assert_eq!(bits(&v), bits(want), "{what}: read another vector");
            true
        }
        Err(Error::Store(_)) => false,
        Err(e) => panic!("{what}: {e}"),
    }
}

// ===== Damage reads as an error.

/// A page's checksum is checked on every read, not only the first one, whatever byte of the page
/// is damaged. Removing the check, or caching "verified" pages, fails it.
#[test]
fn nt01_a_checksum_is_checked_on_every_read_whatever_byte_is_damaged() {
    for b in backends() {
        let (dim, n) = (100usize, 200u32);
        let f = files("crc", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        for i in 0..n {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let keys: Vec<VecKey> = (0..n).map(|i| key(i, 1)).collect();
        let read_all = |tag: &str, bad: &dyn Fn(u32) -> bool| {
            let batch = s.get_many(&keys);
            for (i, got) in batch.into_iter().enumerate() {
                let single = s.get(keys[i]);
                let want = vector(dim, i as u64);
                let ok = exact_or_store_error(got, &want, tag);
                let ok1 = exact_or_store_error(single, &want, tag);
                assert_eq!(ok, ok1, "{tag}: batch and single reads disagree on key {i}");
                assert_eq!(ok, !bad(i as u32), "{tag} {b:?}: key {i}");
            }
        };
        // Twice undamaged: nothing is remembered as verified.
        read_all("clean 1", &|_| false);
        read_all("clean 2", &|_| false);
        let pages = (u64::from(n) * 400).div_ceil(PAGE as u64);
        for p in 0..pages {
            for off in [0u64, 17, 2048, 4095] {
                let at = p * PAGE as u64 + off;
                flip(&f.path(0), at);
                let touching = |i: u32| {
                    let (a, z) = (u64::from(i) * 400, (u64::from(i) + 1) * 400);
                    a < (p + 1) * PAGE as u64 && z > p * PAGE as u64
                };
                read_all(&format!("page {p} byte {off}"), &touching);
                flip(&f.path(0), at);
                read_all("repaired", &|_| false);
            }
        }
    }
}

/// A page that is valid but not the page that belongs there (misdirected write), and an old copy
/// of a page that was rewritten (stale), read as errors.
#[test]
fn nt02_misdirected_and_stale_pages_read_as_errors() {
    for b in backends() {
        let dim = 16usize; // 64-byte records, 64 to a page
        let f = files("stale", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        for i in 0..192u32 {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let (p0, p1) = (raw_read(&f.path(0), 0, PAGE), raw_read(&f.path(0), PAGE as u64, PAGE));
        // Page 1 now holds page 0's bytes: valid, and the wrong page.
        raw_write(&f.path(0), PAGE as u64, &p0);
        for i in 0..192u32 {
            let r = s.get(key(i, 1));
            let want = vector(dim, u64::from(i));
            let ok = exact_or_store_error(r, &want, "misdirected");
            assert_eq!(ok, i / 64 != 1, "{b:?} key {i}");
        }
        raw_write(&f.path(0), PAGE as u64, &p1);
        assert_eq!(&*s.get(key(70, 1)).unwrap(), &vector(dim, 70)[..]);
        // Rewrite page 0 through a reused place, then bring back its old bytes.
        s.release(key(3, 1));
        s.put(key(500, 1), &vector(dim, 500)).unwrap();
        s.flush().unwrap();
        assert_eq!(&*s.get(key(500, 1)).unwrap(), &vector(dim, 500)[..]);
        raw_write(&f.path(0), 0, &p0);
        let r = s.get(key(500, 1));
        assert!(matches!(r, Err(Error::Store(_))), "{b:?}: an old copy of a page read as a vector: {r:?}");
    }
}

/// The file loses its tail at every length, mid-page and on boundaries: every record on a page
/// that is not whole reads as an error, the others read exactly.
#[test]
fn nt03_a_file_cut_at_any_length_reads_exact_or_error() {
    for b in backends() {
        let (dim, n) = (100usize, 200u32);
        let f = files("cut", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        for i in 0..n {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let full = (u64::from(n) * 400).div_ceil(PAGE as u64) * PAGE as u64;
        let mut lens: Vec<u64> = (0..=full / 1024).map(|i| i * 1024).collect();
        for p in 0..=full / PAGE as u64 {
            lens.extend([p * PAGE as u64, (p * PAGE as u64).saturating_sub(1), p * PAGE as u64 + 1]);
        }
        lens.retain(|l| *l <= full);
        lens.sort_unstable_by(|a, b| b.cmp(a));
        lens.dedup();
        for len in lens {
            truncate(&f.path(0), len);
            for i in 0..n {
                let last_page = ((u64::from(i) + 1) * 400 - 1) / PAGE as u64;
                let whole = (last_page + 1) * PAGE as u64 <= len;
                let r = s.get(key(i, 1));
                let ok = exact_or_store_error(r, &vector(dim, u64::from(i)), "cut");
                assert_eq!(ok, whole, "{b:?}: file cut at {len}, key {i}");
            }
        }
    }
}

// ===== Faults at every point.

#[derive(Clone, Copy, Debug)]
struct Script {
    dim: usize,
    n: u32,
}

#[derive(Default)]
struct Ran {
    /// seed of the vector each key was stored with, for puts that returned Ok and were not
    /// released.
    live: BTreeMap<u32, u64>,
    /// puts that returned an error.
    refused: Vec<(u32, u64)>,
}

/// Puts, releases (so that places are reused), flushes and reads. A read that returns a vector
/// must return the right one, here and at every point.
fn script(s: &NvmeVectors, sc: Script) -> Ran {
    let mut ran = Ran::default();
    let check = |ran: &Ran, i: u32, s: &NvmeVectors| {
        if let Some(seed) = ran.live.get(&i) {
            exact_or_store_error(s.get(key(i, 1)), &vector(sc.dim, *seed), "during the script");
        }
    };
    for i in 0..sc.n {
        let seed = u64::from(i) + 1000 * u64::from(i % 7 == 0);
        match s.put(key(i, 1), &vector(sc.dim, seed)) {
            Ok(()) => {
                ran.live.insert(i, seed);
            }
            Err(e) => {
                assert!(matches!(e, Error::Store(_)), "a put failed with {e}");
                assert!(s.get(key(i, 1)).is_err(), "a refused put stored something");
                ran.refused.push((i, seed));
            }
        }
        if i == 20 {
            for r in [5, 6, 7] {
                s.release(key(r, 1));
                ran.live.remove(&r);
            }
        }
        if i == 30 {
            let _ = s.flush();
        }
        if i % 7 == 3 {
            check(&ran, i / 2, s);
        }
    }
    if s.flush().is_err() {
        // The pages stay dirty; reads must still be right.
    }
    for (&i, &seed) in &ran.live {
        exact_or_store_error(s.get(key(i, 1)), &vector(sc.dim, seed), "after the script");
    }
    ran
}

/// A fault at every call of the disk, for every kind of fault, on both backends. Whatever
/// happens, no read returns a vector it was not given, a refused put stores nothing, a failed
/// write loses nothing that was stored, and the store goes on after the fault.
#[test]
fn nt04_a_fault_at_every_point_reads_as_an_error_and_never_as_a_vector() {
    let sc = Script { dim: 300, n: 60 };
    for b in backends() {
        let (mut writes, mut reads) = (0, 0);
        {
            let f = files("dry", b, 1);
            let s = NvmeVectors::on(f.disks.clone(), sc.dim).unwrap();
            let ran = script(&s, sc);
            assert!(ran.refused.is_empty());
            writes = f.chaos[0].writes.load(Ordering::SeqCst).max(writes);
            reads = f.chaos[0].reads.load(Ordering::SeqCst).max(reads);
        }
        assert!(writes >= 2 && reads >= 1, "the script does not reach the disk: {writes} {reads}");
        let hurts: Vec<(Hurt, bool)> = vec![
            (Hurt::Errno(EIO), true),
            (Hurt::Errno(ENOSPC), true),
            (Hurt::Partial(1), true),
            (Hurt::Partial(0), true),
            (Hurt::Torn, true),
            (Hurt::Lost, true),
            (Hurt::Errno(EIO), false),
            (Hurt::Zeroed, false),
            (Hurt::Flipped, false),
        ];
        for (hurt, on_write) in hurts {
            let calls = if on_write { writes } else { reads };
            for at in 0..=calls + 1 {
                for sticky in [false, true] {
                    let f = files("fault", b, 1);
                    let s = NvmeVectors::on(f.disks.clone(), sc.dim).unwrap();
                    let fault = Fault { at, hurt, sticky };
                    *lock(if on_write { &f.chaos[0].write_fault } else { &f.chaos[0].read_fault }) = Some(fault);
                    let ran = script(&s, sc);
                    let what = format!("{b:?} {hurt:?} write={on_write} at {at} sticky {sticky}");
                    f.chaos[0].clear();
                    // Refused puts stored nothing and can be put again once the fault is gone.
                    let silent = matches!(hurt, Hurt::Torn | Hurt::Lost | Hurt::Zeroed | Hurt::Flipped);
                    if !silent {
                        s.flush().unwrap_or_else(|e| panic!("{what}: a flush after the fault failed: {e}"));
                        for (&i, &seed) in &ran.live {
                            let r = s.get(key(i, 1));
                            assert!(
                                matches!(&r, Ok(v) if bits(v) == bits(&vector(sc.dim, seed))),
                                "{what}: key {i} stored before the fault is lost: {r:?}"
                            );
                        }
                        for &(i, seed) in &ran.refused {
                            s.put(key(i, 1), &vector(sc.dim, seed))
                                .unwrap_or_else(|e| panic!("{what}: a put after the fault failed: {e}"));
                        }
                        s.flush().unwrap();
                        for (&i, &seed) in &ran.live {
                            let r = s.get(key(i, 1));
                            assert!(matches!(&r, Ok(v) if bits(v) == bits(&vector(sc.dim, seed))), "{what}: key {i}");
                        }
                        for &(i, seed) in &ran.refused {
                            let r = s.get(key(i, 1));
                            assert!(matches!(&r, Ok(v) if bits(v) == bits(&vector(sc.dim, seed))), "{what}: retried key {i}");
                        }
                    } else {
                        for (&i, &seed) in &ran.live {
                            exact_or_store_error(s.get(key(i, 1)), &vector(sc.dim, seed), &what);
                        }
                    }
                }
            }
        }
    }
}

/// A silent fault is never taken for data, also when the damaged page is reused for new records.
#[test]
fn nt05_reusing_a_place_on_a_damaged_page_is_refused_not_overwritten_blindly() {
    for b in backends() {
        let dim = 16usize;
        let f = files("reuse-damaged", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
        for i in 0..64u32 {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        flip(&f.path(0), 9);
        s.release(key(0, 1));
        let r = s.put(key(100, 1), &vector(dim, 100));
        assert!(matches!(r, Err(Error::Store(_))), "{b:?}: a put reused a place on a damaged page: {r:?}");
        // The page is damaged for every neighbour as well, so none of them reads.
        for i in 1..64u32 {
            assert!(s.get(key(i, 1)).is_err());
        }
    }
}

// ===== io_uring against positioned reads, over the same bytes.

type Norm<T> = std::result::Result<T, (io::ErrorKind, Option<i32>)>;

fn norm<T>(r: io::Result<T>) -> Norm<T> {
    r.map_err(|e| (e.kind(), e.raw_os_error()))
}

#[derive(Clone, Debug)]
enum DOp {
    Write { off: u64, pages: usize, seed: u8 },
    Skewed { off: u64, len: usize, skew: usize, seed: u8 },
    Read(Vec<(u64, usize)>),
    Truncate(u64),
}

fn offset(g: &mut Gen) -> u64 {
    match g.below(12) {
        0..=6 => g.below(40) * PAGE as u64,
        7 => g.below(40 * PAGE as u64),
        8 => (i64::MAX as u64 / PAGE as u64 - g.below(2)) * PAGE as u64,
        9 => (i64::MAX as u64 / PAGE as u64 + 1) * PAGE as u64,
        10 => u64::MAX - 4095,
        _ => 1 << 40,
    }
}

fn dops(g: &mut Gen) -> Vec<DOp> {
    (0..g.below(40) + 1)
        .map(|_| match g.below(10) {
            0..=3 => DOp::Write { off: offset(g), pages: g.below(4) as usize + 1, seed: g.below(250) as u8 },
            4 => DOp::Skewed {
                off: offset(g),
                len: g.below(3 * PAGE as u64) as usize + 1,
                skew: g.below(PAGE as u64) as usize,
                seed: g.below(250) as u8,
            },
            5 => DOp::Truncate(g.below(45) * PAGE as u64 + g.below(2) * g.below(PAGE as u64)),
            _ => {
                let n = *g.pick(&[1usize, 2, 3, 8, 300]);
                DOp::Read((0..n).map(|_| (offset(g), g.below(3) as usize + 1)).collect())
            }
        })
        .collect()
}

type Outcomes = Vec<Norm<Vec<Norm<Vec<u8>>>>>;

fn run_dops(path: &std::path::Path, b: Backend, threads: usize, ops: &[DOp]) -> Outcomes {
    let disk = open(path, b, threads).unwrap();
    let mut out = Vec::new();
    for op in ops {
        out.push(match op {
            DOp::Write { off, pages, seed } => {
                let mut p = Pages::new(*pages);
                p.bytes_mut().iter_mut().enumerate().for_each(|(i, x)| *x = seed.wrapping_add(i as u8));
                norm(disk.write(*off, p.bytes())).map(|()| Vec::new())
            }
            DOp::Skewed { off, len, skew, seed } => {
                let mut p = Pages::new(len.div_ceil(PAGE) + 1);
                p.bytes_mut().iter_mut().enumerate().for_each(|(i, x)| *x = seed.wrapping_add(i as u8));
                let end = (skew + len).min(p.len());
                norm(disk.write(*off, &p.bytes()[*skew..end])).map(|()| Vec::new())
            }
            DOp::Truncate(len) => {
                truncate(path, *len);
                Ok(Vec::new())
            }
            DOp::Read(reqs) => {
                let mut rs: Vec<Req> = reqs.iter().map(|(o, p)| Req::new(*o, *p)).collect();
                disk.read(&mut rs);
                Ok(rs
                    .into_iter()
                    .map(|r| norm(r.result).map(|()| r.buf.bytes().to_vec()))
                    .collect())
            }
        });
    }
    out
}

/// The same generated operations on three files, one per backend (and a pool of threads or none),
/// give the same bytes and the same errors, including EOF, misaligned and absurd offsets.
#[test]
fn nt06_io_uring_and_positioned_reads_agree_on_bytes_and_errors() {
    let bs = backends();
    let runs = Runs { cases: Runs::default().cases / 2 + 1, ..Runs::default() };
    check_with(runs, "nt06", |g| {
        let ops = dops(g);
        let t = Tmp::new("dops");
        let want = run_dops(&t.join("pread0"), Backend::Pread, 0, &ops);
        let mut variants = vec![(Backend::Pread, 3usize)];
        if bs.contains(&Backend::Uring) {
            variants.push((Backend::Uring, 0));
        }
        for (i, (b, th)) in variants.into_iter().enumerate() {
            let got = run_dops(&t.join(&format!("v{i}")), b, th, &ops);
            for (j, (a, c)) in want.iter().zip(&got).enumerate() {
                if a != c {
                    return Err(format!("{b:?}/{th} differs from pread/0 at op {j} {:?}: {c:?} against {a:?}", ops[j]));
                }
            }
        }
        Ok(())
    });
}

/// The extreme requests, once, so that a failure names itself.
#[test]
fn nt07_extreme_offsets_and_lengths_fail_alike_on_both_backends() {
    let t = Tmp::new("extreme");
    let mut outcomes = Vec::new();
    for (i, b) in backends().into_iter().enumerate() {
        let d = open(&t.join(&format!("f{i}")), b, 2).unwrap();
        let mut pg = Pages::new(2);
        pg.bytes_mut().fill(7);
        d.write(0, pg.bytes()).unwrap();
        let mut o = Vec::new();
        for off in [0u64, PAGE as u64, 2 * PAGE as u64, 1 << 62, i64::MAX as u64 - 4095, i64::MAX as u64 + 1, u64::MAX - 4095, 1] {
            let mut rs = [Req::new(off, 1), Req::new(off, 2)];
            d.read(&mut rs);
            let [a, z] = rs;
            o.push((norm(a.result).map(|()| a.buf.bytes().to_vec()), norm(z.result)));
            o.push((norm(d.write(off, pg.bytes())).map(|()| Vec::new()), Ok(())));
            // A write of 100 bytes, of half a page, of a page and a byte.
            o.push((norm(d.write(off, &pg.bytes()[..100])).map(|()| Vec::new()), Ok(())));
            o.push((norm(d.write(off, &pg.bytes()[..PAGE / 2])).map(|()| Vec::new()), Ok(())));
            o.push((norm(d.write(off, &pg.bytes()[..PAGE + 1])).map(|()| Vec::new()), Ok(())));
        }
        outcomes.push(o);
    }
    let offs = [0u64, PAGE as u64, 2 * PAGE as u64, 1 << 62, i64::MAX as u64 - 4095, i64::MAX as u64 + 1, u64::MAX - 4095, 1];
    let what = ["read a page and two", "write a page", "write 100 bytes", "write half a page", "write a page and a byte"];
    for o in &outcomes[1..] {
        for (j, (a, b)) in outcomes[0].iter().zip(o).enumerate() {
            let short = |x: &(Norm<Vec<u8>>, Norm<()>)| (x.0.as_ref().map(|v| v.len()).map_err(|e| *e), x.1);
            assert_eq!(short(a), short(b), "offset {}: {}", offs[j / 5], what[j % 5]);
        }
    }
}

/// Files are opened with `O_DIRECT` where the filesystem takes it: a write that is not aligned is
/// refused by the kernel, on both backends.
#[test]
fn nt23_files_are_opened_with_o_direct() {
    for b in backends() {
        let t = Tmp::new("direct");
        let disk = open(&t.join("f"), b, 0).unwrap();
        let mut p = Pages::new(2);
        p.bytes_mut().fill(3);
        assert!(disk.write(1, &p.bytes()[..PAGE]).is_err(), "{b:?}: a write at an odd offset was taken");
        assert!(disk.write(0, &p.bytes()[1..PAGE + 1]).is_err(), "{b:?}: a write from an odd address was taken");
        assert!(disk.write(0, &p.bytes()[..100]).is_err(), "{b:?}: a write of 100 bytes was taken");
        assert!(disk.write(0, p.bytes()).is_ok());
    }
}

/// A record that starts on a page already written and ends on the page that still fills up (it is
/// only in RAM) reads exactly, with no flush in between. Each batch of dirty pages leaves such a
/// record at its boundary, for every dimension that does not divide a page.
#[test]
fn nt24_a_record_across_a_written_page_and_an_unwritten_one_reads_exactly() {
    for b in backends() {
        for (dim, churn) in [(3usize, false), (6, false), (768, false), (1025, false), (1500, false), (768, true), (1536, true), (1024, true), (4096, false), (100, true)] {
            let f = files("straddle", b, 1);
            let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
            let n = (60 * PAGE / (dim * 4)) as u32 + 7;
            let mut gone = std::collections::BTreeSet::new();
            for i in 0..n {
                s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
                if churn && i >= 8 && i % 2 == 0 {
                    // Released places are reused by the next puts, and dirty old pages start flushes
                    // at moments that do not line up with the tail.
                    s.release(key(i - 8, 1));
                    gone.insert(i - 8);
                }
                for j in i.saturating_sub(2)..=i {
                    let r = s.get(key(j, 1));
                    let ok = matches!(&r, Ok(v) if bits(v) == bits(&vector(dim, u64::from(j))));
                    assert!(ok, "{b:?} dim {dim}: record {j} of {i} (disk writes so far: {}) read as {:?}",
                        f.chaos[0].writes.load(Ordering::SeqCst), r.err());
                }
            }
            assert!(f.chaos[0].writes.load(Ordering::SeqCst) > 0, "the test never wrote pages");
            let keys: Vec<VecKey> = (0..n).filter(|i| !gone.contains(i)).map(|i| key(i, 1)).collect();
            for (k, r) in keys.iter().zip(s.get_many(&keys)) {
                let i = k.slot as usize;
                let ok = matches!(&r, Ok(v) if bits(v) == bits(&vector(dim, i as u64)));
                assert!(ok, "{b:?} dim {dim}: record {i} of {n} without a flush: {:?}", r.err());
            }
        }
    }
}

/// Readers of records that never change get exact vectors while a writer rewrites the pages they
/// share (released places are reused, pages are flushed): a page that changed under a read is
/// read again, and no reader sees a mix of two states or an error.
#[test]
fn nt27_readers_of_stable_records_are_exact_while_neighbours_are_rewritten() {
    for b in backends() {
        // 512 floats are 2 KiB: two records to a page, none across a boundary.
        let dim = 512usize;
        let f = files("concurrent", b, 1);
        let s = Arc::new(NvmeVectors::on(f.disks.clone(), dim).unwrap());
        // Even slots are stable, odd slots churn, and they share pages.
        for i in 0..64u32 {
            s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
        }
        s.flush().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..3u32)
            .map(|r| {
                let (s, stop) = (s.clone(), stop.clone());
                spawn_thread(move || {
                    let mut reads = 0u64;
                    while !stop.load(Ordering::SeqCst) {
                        let keys: Vec<VecKey> = (0..32u32).map(|i| key(2 * ((i + r) % 32), 1)).collect();
                        for (k, got) in keys.iter().zip(s.get_many(&keys)) {
                            let want = vector(dim, u64::from(k.slot));
                            match got {
                                Ok(v) => assert_eq!(bits(&v), bits(&want), "a stable record read as another vector"),
                                Err(e) => panic!("a stable record {} read as an error: {e}", k.slot),
                            }
                            reads += 1;
                        }
                    }
                    reads
                })
            })
            .collect();
        let writer = {
            let s = s.clone();
            spawn_thread(move || {
                let mut next = 1000u32;
                for round in 0..3000u32 {
                    let odd = 2 * (round % 32) + 1;
                    let prev = if round >= 32 { 2 + u64::from(round - 32) } else { 1 };
                    s.release(key(odd, prev));
                    // Reuses the place that was released last.
                    let k = key(next, 1);
                    next += 1;
                    s.put(k, &vector(dim, u64::from(next))).unwrap();
                    s.release(k);
                    s.put(key(odd, 2 + u64::from(round)), &vector(dim, u64::from(round))).unwrap();
                    if round % 3 == 0 {
                        s.flush().unwrap();
                    }
                }
            })
        };
        writer.join().unwrap();
        stop.store(true, Ordering::SeqCst);
        let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(total > 1000, "the readers hardly ran: {total}");
        f.assert_aligned();
    }
}

/// A healthy range over NVMe is never taken out of service: whatever the dimension, feeding rows
/// and searching them give answers and no damage.
#[test]
fn nt26_a_healthy_index_over_nvme_is_never_taken_out_of_service() {
    for b in backends() {
        for dim in [6usize, 768, 1500] {
            let f = files("healthy", b, 3);
            let s = Arc::new(NvmeVectors::on(f.disks.clone(), dim).unwrap());
            let mut ix = serving(ix_config(dim, Some(s)));
            let rows: Vec<_> = (0..400u64).map(|k| (k, Some(vector(dim, k)))).collect();
            let fed = feed(&mut ix, &rows, 0);
            let status = ix.status(RANGE).unwrap();
            assert!(fed.is_ok() && status.damage.is_none(), "{b:?} dim {dim}: feed {fed:?}, damage {:?}", status.damage);
            let hits = search(&mut ix, &exact(Op::L2, &vector(dim, 7), 10));
            assert!(matches!(&hits, Ok(h) if h.len() == 10 && h[0].key == rkey(7)), "{b:?} dim {dim}: {:?}", hits.as_ref().err());
            assert!(ix.status(RANGE).unwrap().damage.is_none(), "{b:?} dim {dim}: a search took the range out of service");
        }
    }
}

/// The same for code extensions on NVMe, whose sizes (96 bytes and the like) rarely divide a page.
#[test]
fn nt25_an_extension_across_a_written_page_and_an_unwritten_one_reads_exactly() {
    for b in backends() {
        for size in [96usize, 100, 4100] {
            let f = files("extstraddle", b, 1);
            let ext = Extensions::new(size, Arc::new(Memory::new(0)), f.disks.clone()).unwrap();
            let bytes = |i: u64| -> Vec<u8> { (0..size).map(|j| (i * 7 + j as u64) as u8).collect() };
            let n = 70 * PAGE as u64 / size as u64;
            for i in 0..n {
                assert_eq!(ext.put(ExtKey { part: 0, id: i }, &bytes(i)).unwrap(), Tier::Nvme);
                if i >= 8 && i % 2 == 0 {
                    ext.release(&ExtKey { part: 0, id: i - 8 });
                }
                for j in i.saturating_sub(2)..=i {
                    let r = ext.get(&ExtKey { part: 0, id: j });
                    assert!(matches!(&r, Ok(v) if **v == bytes(j)[..]), "{b:?} size {size}: extension {j} of {i} read as {:?}", r.err());
                }
            }
        }
    }
}

// ===== The ring.

fn plain(path: &std::path::Path, len: usize) -> os::File {
    let f = os::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path).unwrap();
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    f.write_all_at(&data, 0).unwrap();
    f
}

fn pattern(off: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| ((off as usize + i) % 251) as u8).collect()
}

/// More operations than the queue holds, on queues from one slot up: every one completes with its
/// own bytes, in order.
#[test]
fn nt08_a_ring_serves_more_operations_than_its_queue_holds() {
    if backends().len() < 2 {
        return;
    }
    let t = Tmp::new("ringq");
    let f = plain(&t.join("f"), 1 << 20);
    for entries in [0u32, 1, 2, 3, 8, 256, 5000, u32::MAX] {
        let ring = Ring::new(entries).unwrap();
        let mut bufs: Vec<Vec<u8>> = (0..1000).map(|i| vec![0xAA; 100 + i % 7]).collect();
        let mut ops: Vec<RingOp> = bufs
            .iter_mut()
            .enumerate()
            .map(|(i, b)| RingOp::Read { buf: b, offset: (i * 997) as u64 })
            .collect();
        let res = ring.run(&f, &mut ops);
        drop(ops);
        assert_eq!(res.len(), 1000);
        for (i, (r, b)) in res.into_iter().zip(&bufs).enumerate() {
            assert_eq!(r.unwrap(), b.len(), "entries {entries} op {i}");
            assert_eq!(*b, pattern((i * 997) as u64, b.len()), "entries {entries} op {i}");
        }
    }
}

/// Failing operations among good ones fail alone: the others get their bytes, the results keep
/// their order, and the ring serves the next call.
#[test]
fn nt09_a_failing_operation_does_not_disturb_the_others_or_the_ring() {
    if backends().len() < 2 {
        return;
    }
    let t = Tmp::new("ringerr");
    let direct = {
        let mut o = os::OpenOptions::new();
        o.read(true).write(true).create(true).truncate(true).custom_flags(os::O_DIRECT);
        o.open(t.join("d")).unwrap()
    };
    let mut page = Pages::new(4);
    page.bytes_mut().iter_mut().enumerate().for_each(|(i, x)| *x = (i % 251) as u8);
    direct.write_all_at(page.bytes(), 0).unwrap();
    let ring = Ring::new(4).unwrap();
    for round in 0..20 {
        let (mut a, mut b, mut c, mut d, mut e, mut z) =
            (Pages::new(1), Pages::new(1), Pages::new(1), Pages::new(1), Pages::new(2), Pages::new(0));
        // A buffer that is not aligned, for O_DIRECT.
        let mut skew = Pages::new(2);
        let mut ops = vec![
            RingOp::Read { buf: a.bytes_mut(), offset: 0 },
            RingOp::Read { buf: &mut skew.bytes_mut()[1..PAGE + 1], offset: 0 },
            RingOp::Read { buf: b.bytes_mut(), offset: PAGE as u64 },
            RingOp::Read { buf: c.bytes_mut(), offset: 100 * PAGE as u64 },
            RingOp::Read { buf: d.bytes_mut(), offset: u64::MAX - 4095 },
            RingOp::Read { buf: e.bytes_mut(), offset: 3 * PAGE as u64 },
            RingOp::Read { buf: z.bytes_mut(), offset: 0 },
        ];
        let res = ring.run(&direct, &mut ops);
        drop(ops);
        assert_eq!(res.len(), 7, "round {round}");
        assert_eq!(*res[0].as_ref().unwrap(), PAGE);
        assert_eq!(res[1].as_ref().unwrap_err().raw_os_error(), Some(22), "an unaligned buffer: EINVAL");
        assert_eq!(*res[2].as_ref().unwrap(), PAGE);
        assert_eq!(*res[3].as_ref().unwrap(), 0, "beyond the end of the file");
        assert!(res[4].is_err(), "an offset beyond i64::MAX");
        assert_eq!(*res[5].as_ref().unwrap(), PAGE, "a short read says how much it moved");
        assert_eq!(*res[6].as_ref().unwrap(), 0);
        assert_eq!(a.bytes(), &page.bytes()[..PAGE]);
        assert_eq!(b.bytes(), &page.bytes()[PAGE..2 * PAGE]);
        assert_eq!(&e.bytes()[..PAGE], &page.bytes()[3 * PAGE..]);
    }
    // Files the call may not use: each operation fails, none hangs.
    let wo = os::OpenOptions::new().write(true).open(t.join("d")).unwrap();
    let ro = os::OpenOptions::new().read(true).open(t.join("d")).unwrap();
    let dirf = os::OpenOptions::new().read(true).open(&t.0).unwrap();
    let mut buf = vec![0u8; 64];
    let ones = vec![1u8; 64];
    for _ in 0..5 {
        let r = ring.run(&wo, &mut [RingOp::Read { buf: &mut buf, offset: 0 }, RingOp::Read { buf: &mut [0u8; 8], offset: 0 }]);
        assert!(r.iter().all(|r| r.as_ref().is_err_and(|e| e.raw_os_error() == Some(9))), "{r:?}");
        let r = ring.run(&ro, &mut [RingOp::Write { buf: &ones, offset: 0 }]);
        assert!(r[0].as_ref().is_err_and(|e| e.raw_os_error() == Some(9)), "{r:?}");
        let r = ring.run(&dirf, &mut [RingOp::Read { buf: &mut buf, offset: 0 }]);
        assert!(r[0].is_err(), "a directory reads as an error: {r:?}");
    }
    // The ring serves a good call afterwards.
    let r = ring.run(&direct, &mut [RingOp::Read { buf: a_page().bytes_mut(), offset: 0 }]);
    assert_eq!(*r[0].as_ref().unwrap(), PAGE);
}

fn a_page() -> Pages {
    Pages::new(1)
}

/// A buffer longer than u32::MAX cannot be one operation: it fails, and nothing else does.
#[test]
fn nt10_an_operation_longer_than_a_queue_entry_fails_alone() {
    if backends().len() < 2 {
        return;
    }
    let t = Tmp::new("ringbig");
    let f = plain(&t.join("f"), 4096);
    let ring = Ring::new(8).unwrap();
    let mut big = vec![0u8; (1usize << 32) + 1];
    let mut small = vec![0u8; 100];
    let r = ring.run(&f, &mut [RingOp::Read { buf: &mut big, offset: 0 }, RingOp::Read { buf: &mut small, offset: 0 }]);
    assert_eq!(r[0].as_ref().unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(*r[1].as_ref().unwrap(), 100);
    assert_eq!(small, pattern(0, 100));
    assert!(big[..4096].iter().all(|b| *b == 0), "a refused operation wrote into its buffer");
}

/// A call returns only after the kernel is done with its buffers: a read of a pipe that gets its
/// data later blocks the call, and the buffer holds the data on return.
#[test]
fn nt11_a_call_waits_for_a_completion_that_comes_later() {
    if backends().len() < 2 {
        return;
    }
    let t = Tmp::new("ringfifo");
    let path = t.join("fifo");
    let st = std::process::Command::new("mkfifo").arg(&path).status().unwrap();
    assert!(st.success());
    let fifo = os::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let ring = Arc::new(Ring::new(4).unwrap());
    let returned = Arc::new(AtomicBool::new(false));
    let (r2, ret2) = (ring.clone(), returned.clone());
    let reader = spawn_thread(move || {
        let mut buf = vec![0u8; 8];
        let res = r2.run(&fifo, &mut [RingOp::Read { buf: &mut buf, offset: 0 }]);
        ret2.store(true, Ordering::SeqCst);
        (res.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(), buf, fifo)
    });
    nabedra_rt::time::thread_sleep(std::time::Duration::from_millis(300));
    assert!(!returned.load(Ordering::SeqCst), "the call returned with its read still in flight");
    let mut w = os::OpenOptions::new().write(true).open(&path).unwrap();
    std::io::Write::write_all(&mut w, b"abcdefgh").unwrap();
    let (n, buf, _fifo) = reader.join().unwrap();
    assert_eq!(n, vec![8]);
    assert_eq!(&buf, b"abcdefgh");
}

/// Many threads share one ring: each gets its own results, and the ring lasts until the last
/// call has returned.
#[test]
fn nt12_threads_sharing_a_ring_get_their_own_bytes() {
    if backends().len() < 2 {
        return;
    }
    let t = Tmp::new("ringshare");
    let f = Arc::new(plain(&t.join("f"), 1 << 18));
    let ring = Arc::new(Ring::new(16).unwrap());
    let hs: Vec<_> = (0..8u64)
        .map(|th| {
            let (f, ring) = (f.clone(), ring.clone());
            spawn_thread(move || {
                for round in 0..200u64 {
                    let n = 1 + (round + th) as usize % 40;
                    let mut bufs: Vec<Vec<u8>> = (0..n).map(|i| vec![0u8; 50 + i]).collect();
                    let offs: Vec<u64> = (0..n as u64).map(|i| (th * 7919 + round * 131 + i * 17) % 200_000).collect();
                    let mut ops: Vec<RingOp> = bufs.iter_mut().zip(&offs).map(|(b, o)| RingOp::Read { buf: b, offset: *o }).collect();
                    let res = ring.run(&f, &mut ops);
                    drop(ops);
                    for ((r, b), o) in res.into_iter().zip(&bufs).zip(&offs) {
                        assert_eq!(r.unwrap(), b.len());
                        assert_eq!(*b, pattern(*o, b.len()));
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    drop(ring);
}

/// Writes through the ring land exactly, also across the queue size, and short writes finish.
#[test]
fn nt13_writes_through_the_ring_land_exactly() {
    for b in backends() {
        let t = Tmp::new("ringwrite");
        let d = open(&t.join("f"), b, 2).unwrap();
        let pages = 700;
        let mut data = Pages::new(pages);
        data.bytes_mut().iter_mut().enumerate().for_each(|(i, x)| *x = (i * 31 % 253) as u8);
        d.write(0, data.bytes()).unwrap();
        let mut reqs: Vec<Req> = (0..pages as u64).rev().map(|p| Req::new(p * PAGE as u64, 1)).collect();
        d.read(&mut reqs);
        for (i, r) in reqs.into_iter().enumerate() {
            let p = pages - 1 - i;
            r.result.unwrap();
            assert_eq!(r.buf.bytes(), &data.bytes()[p * PAGE..(p + 1) * PAGE], "{b:?} page {p}");
        }
    }
}

// ===== O_DIRECT alignment, odd dimensions and records across pages.

/// Whatever the dimension, every request the store makes is page-aligned in offset, length and
/// buffer, the file is whole pages, and records that straddle pages read back exactly.
#[test]
fn nt14_every_request_is_aligned_for_odd_dimensions() {
    for b in backends() {
        for dim in [1usize, 3, 4, 5, 1023, 1024, 1025, 2049, 4095, 4096] {
            eprintln!("nt14 {b:?} dim {dim}");
            let f = files("align", b, 3);
            let s = NvmeVectors::on(f.disks.clone(), dim).unwrap();
            let n = (200_000 / dim).clamp(40, 700) as u32;
            let mut live = BTreeMap::new();
            for i in 0..n {
                s.put(key(i, 1), &vector(dim, u64::from(i))).unwrap();
                live.insert(i, u64::from(i));
                if i % 11 == 5 {
                    s.release(key(i - 3, 1));
                    live.remove(&(i - 3));
                }
                if i % 97 == 96 {
                    s.flush().unwrap();
                }
            }
            s.flush().unwrap();
            let keys: Vec<VecKey> = live.keys().map(|i| key(*i, 1)).collect();
            for (got, (&i, &seed)) in s.get_many(&keys).into_iter().zip(&live) {
                let got = got.unwrap_or_else(|e| panic!("{b:?} dim {dim} key {i}: {e}"));
                assert_eq!(bits(&got), bits(&vector(dim, seed)), "{b:?} dim {dim} key {i}");
            }
            f.assert_aligned();
            for c in 0..3 {
                let len = os::metadata(f.path(c)).unwrap().len();
                assert_eq!(len % PAGE as u64, 0, "dim {dim} core {c}: the file is not whole pages");
                let writes = lock(&f.chaos[c].write_log);
                assert!(!writes.is_empty() || n < 4);
            }
        }
    }
}

/// The same operations on RAM and NVMe, over the dimensions that straddle pages: every outcome
/// equal, including reads of released, unknown and repeated keys.
#[test]
fn nt15_ram_and_nvme_agree_for_edge_dimensions_and_odd_batches() {
    let bs = backends();
    let runs = Runs { cases: Runs::default().cases / 8 + 1, ..Runs::default() };
    check_with(runs, "nt15", |g| {
        let dim = *g.pick(&[1usize, 3, 4, 1025, 4096]);
        let ram = RamVectors::new();
        let tmps: Vec<(Backend, Files)> = bs.iter().map(|b| (*b, files("diff2", *b, 2))).collect();
        let nvmes: Vec<NvmeVectors> = tmps.iter().map(|(_, f)| NvmeVectors::on(f.disks.clone(), dim).unwrap()).collect();
        let mut at = 1u64;
        for _ in 0..g.below(150) + 1 {
            let k = key(g.below(30) as u32, at);
            match g.below(8) {
                0..=2 => {
                    at += 1;
                    let seed = g.u64();
                    let want = ram.put(k, &vector(dim, seed)).is_ok();
                    for n in &nvmes {
                        if n.put(k, &vector(dim, seed)).is_ok() != want {
                            return Err("put".into());
                        }
                    }
                }
                3 => {
                    let r = key(k.slot, g.below(at));
                    ram.release(r);
                    nvmes.iter().for_each(|n| n.release(r));
                }
                4 => nvmes.iter().try_for_each(|n| n.flush()).map_err(|e| e.to_string())?,
                _ => {
                    let ks: Vec<VecKey> = (0..g.below(9)).map(|_| key(g.below(32) as u32, g.below(at + 2))).collect();
                    let norm = |v: Vec<Res<Arc<[f32]>>>| v.into_iter().map(|r| r.map(|v| bits(&v)).map_err(|e| matches!(e, Error::Store(_)))).collect::<Vec<_>>();
                    let want = norm(ram.get_many(&ks));
                    for n in &nvmes {
                        let got = norm(n.get_many(&ks));
                        if got.len() != want.len() {
                            return Err("a batch lost an answer".into());
                        }
                        for (a, b) in want.iter().zip(&got) {
                            // RAM says "no such version" as Internal; NVMe too. Both are errors,
                            // never different vectors.
                            if a.is_ok() != b.is_ok() || (a.is_ok() && a != b) {
                                return Err(format!("dim {dim}: {a:?} against {b:?}"));
                            }
                        }
                    }
                }
            }
        }
        tmps.iter().for_each(|(_, f)| f.assert_aligned());
        Ok(())
    });
}

// ===== Hostile input.

/// A vector the store cannot give back must not be taken: whatever RAM would return, NVMe returns
/// too, or refuses the put. NaN and infinities are not legal rows, but the store is the last line.
#[test]
fn nt16_a_put_that_can_never_be_read_back_is_refused() {
    for b in backends() {
        let f = files("nonfinite", b, 1);
        let nvme = NvmeVectors::on(f.disks.clone(), 4).unwrap();
        let ram = RamVectors::new();
        for (i, v) in [[f32::NAN, 1.0, 2.0, 3.0], [f32::INFINITY, 0.0, 0.0, 0.0], [f32::NEG_INFINITY, 1.0, 1.0, 1.0]].iter().enumerate() {
            let k = key(i as u32, 1);
            ram.put(k, v).unwrap();
            let stored = nvme.put(k, v).is_ok();
            if stored {
                let got = nvme.get(k).map(|v| bits(&v));
                assert_eq!(got.ok(), Some(bits(v)), "{b:?}: a put was accepted and cannot be read back");
            }
        }
    }
}

/// Signed zero and subnormals come back bit for bit.
#[test]
fn nt17_signed_zero_and_subnormals_round_trip_bit_for_bit() {
    for b in backends() {
        let f = files("bits", b, 1);
        let s = NvmeVectors::on(f.disks.clone(), 6).unwrap();
        let v = [-0.0f32, 0.0, f32::MIN_POSITIVE / 2.0, -1e-45, f32::MAX, f32::MIN];
        s.put(key(1, 1), &v).unwrap();
        assert_eq!(bits(&s.get(key(1, 1)).unwrap()), bits(&v));
        s.flush().unwrap();
        assert_eq!(bits(&s.get(key(1, 1)).unwrap()), bits(&v));
    }
}

/// Misuse is an error and never a panic or a big allocation.
#[test]
fn nt18_misuse_is_an_error_and_allocates_nothing_big() {
    for b in backends() {
        let f = files("misuse", b, 1);
        assert!(NvmeVectors::on(Vec::new(), 4).is_err());
        assert!(NvmeVectors::on(f.disks.clone(), 0).is_err());
        assert!(NvmeVectors::on(f.disks.clone(), usize::MAX).is_err());
        // A dimension of 2^58 floats is a record of 2^60 bytes: unused records cost nothing.
        let huge = NvmeVectors::on(f.disks.clone(), 1 << 58).unwrap();
        assert!(huge.get(key(1, 1)).is_err());
        assert!(huge.put(key(1, 1), &[1.0]).is_err());
        assert!(huge.get_many(&vec![key(1, 1); 100_000]).iter().all(|r| r.is_err()));
        let s = NvmeVectors::on(f.disks.clone(), 4).unwrap();
        assert!(s.put(key(1, 1), &[1.0; 3]).is_err());
        assert!(s.put(key(1, 1), &[1.0; 5]).is_err());
        assert!(s.put(key(1, 1), &[]).is_err());
        s.release(key(9, 9));
        assert!(s.get_many(&[]).is_empty());
        assert!(s.get(key(u32::MAX, u64::MAX)).is_err());
        s.put(key(u32::MAX, u64::MAX), &[1.0; 4]).unwrap();
        assert_eq!(&*s.get(key(u32::MAX, u64::MAX)).unwrap(), &[1.0; 4]);
        assert_eq!(s.stats().vectors, 1);
    }
}

// ===== Extensions.

/// RAM charged to the code memory returns to it exactly, through puts that fail, repeats,
/// releases and a full memory, and the tiers are what the budget says.
#[test]
fn nt19_extension_memory_is_charged_once_and_returned_once() {
    let runs = Runs { cases: Runs::default().cases / 4 + 1, ..Runs::default() };
    check_with(runs, "nt19", |g| {
        let bytes = *g.pick(&[1usize, 7, 96, 4096, 5000]);
        let limit = g.below(12) * bytes as u64 + g.below(bytes as u64);
        let mem = Arc::new(Memory::new(limit));
        let f = files("extmem", Backend::Pread, 2);
        let ext = Extensions::new(bytes, mem.clone(), f.disks.clone()).map_err(|e| e.to_string())?;
        let mut model: HashMap<ExtKey, Tier> = HashMap::new();
        let mut codes = 0u64;
        let payload = |id: u64| -> Vec<u8> { (0..bytes).map(|j| (id as usize * 13 + j) as u8).collect() };
        for _ in 0..g.below(200) + 1 {
            let k = ExtKey { part: g.below(5) as u32, id: g.below(25) };
            match g.below(7) {
                0..=2 => {
                    let ram_before = mem.used().1;
                    let fail = g.chance(1, 6);
                    *lock(&f.chaos[k.part as usize % 2].write_fault) = fail.then_some(Fault { at: 0, hurt: Hurt::Errno(ENOSPC), sticky: true });
                    let r = ext.put(k, &payload(k.id));
                    f.chaos.iter().for_each(|c| c.clear());
                    match (model.contains_key(&k), r) {
                        (true, Ok(_)) => return Err("a stored extension was stored again".into()),
                        (true, Err(_)) => {}
                        (false, Ok(t)) => {
                            let fits = codes + ram_before + bytes as u64 <= limit;
                            if (t == Tier::Ram) != fits {
                                return Err(format!("tier {t:?} with codes {codes}, ram {ram_before}, limit {limit}"));
                            }
                            model.insert(k, t);
                        }
                        (false, Err(_)) => {
                            if mem.used().1 != ram_before {
                                return Err("a refused put changed the memory".into());
                            }
                            if ext.tier(&k).is_some() {
                                return Err("a refused put stored something".into());
                            }
                        }
                    }
                }
                3 => {
                    ext.release(&k);
                    model.remove(&k);
                }
                4 => {
                    let c = g.below(3 * bytes as u64);
                    mem.charge_codes(c);
                    codes += c;
                }
                5 => {
                    let c = codes.min(g.below(2 * bytes as u64));
                    mem.release_codes(c);
                    codes -= c;
                }
                _ => {
                    let _ = ext.flush();
                    match (model.get(&k), ext.get(&k)) {
                        (Some(_), Ok(v)) if *v == payload(k.id)[..] => {}
                        (Some(Tier::Nvme), Err(Error::Store(_))) => {}
                        (None, Err(_)) => {}
                        (m, r) => return Err(format!("{k:?}: {m:?} against {r:?}")),
                    }
                }
            }
            let ram: u64 = model.values().filter(|t| **t == Tier::Ram).count() as u64 * bytes as u64;
            if mem.used() != (codes, ram) {
                return Err(format!("memory {:?} against {:?}", mem.used(), (codes, ram)));
            }
        }
        Ok(())
    });
}

// ===== Slot reuse under an index, with pins and checkpoints.

#[derive(Clone)]
struct Truth {
    /// key -> vector of the latest version, for rows that are not deleted.
    live: BTreeMap<u64, Vec<f32>>,
}

fn key_n(k: &nabedra_vector::types::Key) -> u64 {
    u64::from_be_bytes((**k).try_into().unwrap())
}

fn expect_rows(hits: &[nabedra_vector::index::Hit], truth: &Truth, what: &str) -> Result<(), String> {
    let got: BTreeMap<u64, Vec<u32>> = hits
        .iter()
        .map(|h| (key_n(&h.key), bits(h.row.vec.as_deref().unwrap_or(&[]))))
        .collect();
    let want: BTreeMap<u64, Vec<u32>> = truth.live.iter().map(|(k, v)| (*k, bits(v))).collect();
    if got != want {
        return Err(format!("{what}: the rows differ: got {} rows, want {}", got.len(), want.len()));
    }
    for h in hits {
        if *h.row.image != key_n(&h.key).to_be_bytes() {
            return Err(format!("{what}: a hit carries another row's image"));
        }
    }
    Ok(())
}

fn workload(g: &mut Gen, backend: Option<Backend>, damage: bool) -> Result<(), String> {
    let dim = *g.pick(&[1usize, 3, 6]);
    let tmp = backend.map(|b| files("workload", b, 3));
    let inner: Arc<dyn Vectors> = match &tmp {
        None => Arc::new(RamVectors::new()),
        Some(f) => Arc::new(NvmeVectors::on(f.disks.clone(), dim).map_err(|e| e.to_string())?),
    };
    let audit = Audit::new(inner);
    let fs = FaultFs::new();
    let store = Store::new(Arc::new(fs.clone()) as Arc<dyn Fs>, "/ck");
    let cfg = ix_config(dim, Some(audit.clone() as Arc<dyn Vectors>));
    let mut ix = Index::new(cfg.clone(), ExactCodec, Some(store.clone())).map_err(|e| e.to_string())?;
    ix.begin_load(RANGE, 0, 0).map_err(|e| e.to_string())?;
    ix.finish_load(RANGE).map_err(|e| e.to_string())?;
    ix.caught_up(RANGE, 0).map_err(|e| e.to_string())?;
    let mut at = 0u64;
    let mut truth = Truth { live: BTreeMap::new() };
    let mut pins: Vec<(nabedra_vector::index::Pin, Truth)> = Vec::new();
    let mut damaged = false;
    let q = |dimv: usize| exact(Op::L2, &vec![0.25; dimv], 1000);
    let check = |audit: &Audit, what: &str| -> Result<(), String> {
        let v = audit.violations();
        if !v.is_empty() {
            return Err(format!("{what}: the slot rule broke: {:?}", &v[..v.len().min(3)]));
        }
        Ok(())
    };
    for step in 0..g.below(60) + 5 {
        match g.below(14) {
            0..=4 => {
                let changes: Vec<(u64, Option<Vec<f32>>)> = (0..g.below(8) + 1)
                    .map(|_| (g.below(25), (!g.chance(1, 5)).then(|| grid(g, dim))))
                    .collect();
                match feed(&mut ix, &changes, at) {
                    Ok(next) => {
                        at = next;
                        for (k, v) in &changes {
                            match v {
                                Some(v) => truth.live.insert(*k, v.clone()),
                                None => truth.live.remove(k),
                            };
                        }
                    }
                    Err(e) if damaged => {
                        let _ = e;
                        break;
                    }
                    Err(e) => return Err(format!("step {step}: feed failed: {e}")),
                }
            }
            5 | 6 if pins.len() < 4 => {
                if let Ok(p) = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None) {
                    pins.push((p, truth.clone()));
                }
            }
            7 if !pins.is_empty() => {
                let i = g.below(pins.len() as u64) as usize;
                pins.remove(i);
            }
            8 | 9 if !pins.is_empty() => {
                let i = g.below(pins.len() as u64) as usize;
                match ix.search(&pins[i].0, &q(dim)) {
                    Ok(hits) => expect_rows(&hits, &pins[i].1, &format!("step {step}: a pin"))?,
                    Err(Error::Store(_) | Error::PinFailed) if damaged => {}
                    Err(e) => return Err(format!("step {step}: a pin failed with {e}")),
                }
            }
            10 => {
                let low = at.saturating_sub(g.below(4));
                let _ = ix.advance_low(RANGE, low);
                for _ in 0..g.below(4) {
                    ix.maintain(8);
                    ix.run();
                }
            }
            11 => match ix.checkpoint() {
                Ok(()) => {
                    // A checkpoint that went out must recover into the same rows, or fail.
                    let rec = Audit::new(Arc::new(RamVectors::new()));
                    let cfg2 = ix_config(dim, Some(rec.clone() as Arc<dyn Vectors>));
                    if let Ok((mut back, _)) = Index::recover(cfg2, ExactCodec, store.clone())
                        && back.caught_up(RANGE, at).is_ok()
                    {
                        let hits = search(&mut back, &q(dim));
                        match hits {
                            Ok(hits) => expect_rows(&hits, &truth, &format!("step {step}: recovered"))?,
                            Err(e) if damaged => {
                                let _ = e;
                            }
                            Err(e) => return Err(format!("step {step}: recovered index failed: {e}")),
                        }
                    }
                    check(&rec, "recovered index")?;
                }
                Err(_) if damaged => {}
                Err(e) => return Err(format!("step {step}: checkpoint failed: {e}")),
            },
            13 => {
                let rows: Vec<_> = truth.live.iter().map(|(k, v)| (rkey(*k), at * 10, row_of(*k, v))).collect();
                if ix.begin_load(RANGE, at, at * 10).is_err() || ix.load_rows(RANGE, &rows).is_err() {
                    if damaged {
                        break;
                    }
                    return Err(format!("step {step}: a resync was refused"));
                }
                let mut done = false;
                for _ in 0..200 {
                    ix.run();
                    if ix.finish_load(RANGE).is_ok() {
                        done = true;
                        break;
                    }
                }
                if !done || ix.caught_up(RANGE, at).is_err() {
                    if damaged {
                        break;
                    }
                    return Err(format!("step {step}: a resync did not finish"));
                }
                pins.clear();
            }
            12 if damage && backend.is_some() && !damaged => {
                let core = g.below(3) as usize;
                let path = tmp.as_ref().map(|f| f.path(core)).unwrap();
                let len = os::metadata(&path).map_or(0, |m| m.len());
                if len > 0 {
                    flip(&path, g.below(len));
                    damaged = true;
                }
            }
            _ => match search(&mut ix, &q(dim)) {
                Ok(hits) => expect_rows(&hits, &truth, &format!("step {step}: latest"))?,
                Err(Error::Store(_) | Error::PinFailed | Error::NotHeld(_) | Error::Behind { .. }) if damaged => {}
                Err(e) => return Err(format!("step {step}: a search failed with {e}")),
            },
        }
        check(&audit, &format!("step {step}"))?;
    }
    if damaged {
        return Ok(());
    }
    if !truth.live.is_empty() && audit.puts.load(Ordering::Relaxed) == 0 {
        return Err("rows were fed and none of their vectors went to the store".into());
    }
    // Everything let go: the places go back, and none leaks.
    pins.clear();
    ix.drop_range(RANGE).map_err(|e| e.to_string())?;
    for _ in 0..200 {
        ix.maintain(64);
        ix.run();
    }
    check(&audit, "at the end")?;
    if audit.live_keys() != 0 {
        return Err(format!("{} vectors stay stored after the range was dropped", audit.live_keys()));
    }
    Ok(())
}

fn workload_runs() -> Runs {
    Runs { cases: Runs::default().cases / 4 + 2, max_shrink_steps: 100, ..Runs::default() }
}

/// A place is reused only after its version is released: pins and checkpoints hold versions, no
/// read touches a released one, a stored version is never stored again, every hit carries the
/// vector of its own version, and when everything is let go nothing stays stored.
#[test]
fn nt20_the_slot_rule_holds_under_pins_checkpoints_and_resyncs_in_ram() {
    check_with(workload_runs(), "nt20", |g| workload(g, None, false));
}

#[test]
fn nt21_the_slot_rule_holds_under_pins_checkpoints_and_resyncs_on_nvme() {
    for b in backends() {
        check_with(workload_runs(), "nt21", |g| workload(g, Some(b), false));
    }
}

/// The same, with one bit flipped somewhere in the files at a random moment: an answer is right
/// or an error, never a wrong row.
#[test]
fn nt22_damage_under_a_workload_gives_errors_and_never_wrong_rows() {
    for b in backends() {
        check_with(workload_runs(), "nt22", |g| workload(g, Some(b), true));
    }
}

/// An empty request is the same on both backends, also at an offset the kernel refuses.
#[test]
fn nt28_empty_requests_fail_alike_on_both_backends() {
    let t = Tmp::new("empty");
    let mut outcomes = Vec::new();
    for (i, b) in backends().into_iter().enumerate() {
        let d = open(&t.join(&format!("f{i}")), b, 2).unwrap();
        let mut o = Vec::new();
        for off in [0u64, PAGE as u64, 1 << 62, i64::MAX as u64 + 1, u64::MAX - 4095, 1] {
            let mut rs = [Req::new(off, 0)];
            d.read(&mut rs);
            o.push(norm(std::mem::replace(&mut rs[0].result, Ok(()))));
            o.push(norm(d.write(off, &[])));
        }
        outcomes.push(o);
    }
    for o in &outcomes[1..] {
        assert_eq!(o, &outcomes[0]);
    }
}
