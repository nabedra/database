//! Crash, power-loss and damage regressions for the journal on NS1's [`FaultFs`] and on
//! the real filesystem. They started as the RV-KV1 review's reproductions (findings F1 to
//! F4, F11 and F13); scenario IDs follow `plans/native-storage-engine.md` (Tests).
//!
//! The NS04 and NS05 campaigns here are journal-level: the oracle is the sequence of
//! submitted records, and recovery must return an exact prefix of it that holds every
//! write acknowledged as synced, and a second open must be identical. `FaultFs` evidence
//! is a fault model, not physical power-loss evidence.

use std::path::Path;
use std::sync::{Arc, Mutex};

use super::*;
use crate::fs::{Fault, FaultError, FaultFs, Fs, FsOp, PowerLoss, RealFs};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Rec {
    first_seq: Seq,
    data: Vec<u8>,
    locators: Vec<Locator>,
    /// Segment and offset of the record, from its first journal value.
    at: Option<(u64, u64)>,
}

#[derive(Default)]
struct Sink(Mutex<Vec<Rec>>);

impl Sink {
    fn records(&self) -> Vec<Rec> {
        self.0.lock().unwrap().clone()
    }

    fn data(&self) -> Vec<Vec<u8>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.data.clone())
            .collect()
    }
}

impl JournalSink for Sink {
    fn apply(
        &self,
        _: RecordKind,
        first_seq: Seq,
        batch: &WriteBatch,
        loc: &[Locator],
    ) -> Result<()> {
        let at = batch.journal_values().first().map(|jv| {
            (
                loc[0].segment,
                loc[0].offset
                    - u64::from(jv.offset)
                    - batch.scope().len() as u64
                    - RECORD_HEADER_LEN as u64,
            )
        });
        self.0.lock().unwrap().push(Rec {
            first_seq,
            data: batch.data().to_vec(),
            locators: loc.to_vec(),
            at,
        });
        Ok(())
    }
}

fn opts() -> JournalOptions {
    JournalOptions {
        segment_bytes: 64 << 10,
        max_group_bytes: 4 << 20,
        zero_fill: true,
    }
}

fn open(fs: Arc<dyn Fs>, dir: &Path) -> Result<(Journal, RecoveryReport, Arc<Sink>)> {
    open_after(fs, dir, 0)
}

fn open_after(
    fs: Arc<dyn Fs>,
    dir: &Path,
    replay_after: Seq,
) -> Result<(Journal, RecoveryReport, Arc<Sink>)> {
    let sink = Arc::new(Sink::default());
    let (j, r) = Journal::open(fs, dir, opts(), replay_after, sink.clone())?;
    Ok((j, r, sink))
}

/// A batch whose record is exactly `record_len` bytes (scope "g/", one journal value).
fn sized(tag: &[u8], record_len: usize) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.set_scope(b"g/");
    let mut key = b"g/".to_vec();
    key.extend_from_slice(tag);
    // record = 48 + 2 (scope) + 1 (kind) + 1 (key len) + key + 2 (value len) + value
    let value_len = record_len - (RECORD_HEADER_LEN + 2 + 1 + 1 + key.len() + 2);
    assert!((128..16384).contains(&value_len));
    b.put_journal(&key, &vec![tag[0]; value_len]);
    assert_eq!(RECORD_HEADER_LEN + 2 + b.encoded_bytes(), record_len);
    b
}

/// A store-wide batch (empty scope) of one put.
fn plain(tag: &[u8], value_len: usize) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.put(tag, &vec![7u8; value_len]);
    b
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nabedra-kv-crash-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(bytes, offset).unwrap();
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------------------
// F1: appending after a clean close

/// RV-KV1 F1. Run 2 appends R1 and R2 (unsynced) after run 1's close pad; a power loss
/// drops R1's sector and keeps R2's. Run 3 recovers the prefix before R1 and writes W1,
/// which takes R1's sequence number and length. Run 4 must not replay R2 after W1.
#[test]
fn ns05_append_after_clean_close_never_resurrects_a_lost_record() {
    let dir = TempDir::new("append");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);

    let (j, _, _) = open(fs.clone(), dir).unwrap();
    for i in 0..3u8 {
        j.submit(sized(&[b'a', i], 512), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    j.close().unwrap();
    drop(j);

    let (j, _, sink) = open(fs.clone(), dir).unwrap();
    let base = sink.records().len();
    for tag in [b"r1", b"r2"] {
        j.submit(sized(tag, 512), Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    j.crash();
    drop(j);

    // Learn where R1 sits by replaying a copy of the directory.
    let copy = TempDir::new("append-probe");
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), copy.path().join(e.file_name())).unwrap();
    }
    let (pj, _, psink) = open(fs.clone(), copy.path()).unwrap();
    let recs = psink.records();
    pj.crash();
    drop(pj);
    drop(copy);
    assert_eq!(recs.len(), base + 2, "run 2's records replay after a kill");
    let r1 = recs[base].clone();
    let r2 = recs[base + 1].clone();
    let (seg, at) = r1.at.unwrap();

    // Power loss: R1's 512-byte sector reads as zeros, R2's survives.
    overwrite(
        &segment::path_of(dir, seg, segment::LIVE_EXT),
        at,
        &[0u8; 512],
    );

    let (j, report, sink) = open(fs.clone(), dir).unwrap();
    assert_eq!(
        sink.records().len(),
        base,
        "run 3 recovers the prefix before R1: {report:?}"
    );
    let w1 = j
        .submit(sized(b"w1", 512), Durability::NoSync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(w1, r1.first_seq, "W1 reuses R1's sequence number");
    j.crash();
    drop(j);

    let (j, _, sink) = open(fs.clone(), dir).unwrap();
    let after = sink.records();
    j.crash();
    drop(j);
    assert!(
        !after.iter().any(|r| r.data == r2.data),
        "a write lost in run 2 came back in run 4"
    );
    assert_eq!(after.len(), base + 1, "run 3's prefix plus W1 only");
    assert_eq!(after[base].data, sized(b"w1", 512).data());
}

// ---------------------------------------------------------------------------------------
// F2: NS05 power-loss campaign and NS04 crash at every boundary, on NS1's FaultFs.
// Only `/db` is durable at the start: the journal must make its own directory durable.

/// One seed: six open/write/stop cycles (power loss or clean close plus kill), then two
/// opens that must agree.
fn campaign(seed: u64, mode: PowerLoss) -> std::result::Result<(), String> {
    let mut rng = Rng(seed * 7919 + 1);
    let mut fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let dir = Path::new("/db/journal");
    // Data of each record in sequence order, and how many a synced ack covers.
    let mut log: Vec<Vec<u8>> = Vec::new();
    let mut synced = 0usize;
    let mut counter = 0u64;
    let check = |got: &[Vec<u8>], log: &[Vec<u8>], synced: usize| {
        got.len() >= synced && got.len() <= log.len() && got[..] == log[..got.len()]
    };
    for cycle in 0..6 {
        let shared: Arc<dyn Fs> = Arc::new(fs.clone());
        let (j, _, sink) = open(shared, dir).map_err(|e| format!("cycle {cycle} open: {e:?}"))?;
        let got = sink.data();
        if !check(&got, &log, synced) {
            j.crash();
            return Err(format!(
                "cycle {cycle}: recovered {} records, oracle {} (synced {synced})",
                got.len(),
                log.len()
            ));
        }
        log.truncate(got.len());
        synced = synced.min(log.len());
        for _ in 0..1 + rng.below(12) {
            let mut pending = Vec::new();
            for _ in 0..1 + rng.below(4) {
                counter += 1;
                let len = [200usize, 512, 700, 1500, 5000][rng.below(5) as usize];
                let b = sized(format!("k{counter:06}").as_bytes(), len);
                let data = b.data().to_vec();
                let sync = rng.below(3) == 0;
                let durability = if sync {
                    Durability::Sync
                } else {
                    Durability::NoSync
                };
                let c = j
                    .submit(b, durability, Box::new(()))
                    .map_err(|e| format!("submit: {e:?}"))?;
                log.push(data);
                pending.push((c, sync, log.len()));
            }
            for (c, sync, n) in pending {
                c.wait().map_err(|e| format!("wait: {e:?}"))?;
                if sync {
                    synced = synced.max(n);
                }
            }
        }
        if rng.below(3) == 0 {
            j.close().map_err(|e| format!("close: {e:?}"))?;
            synced = log.len();
            drop(j);
            fs = fs.kill();
        } else {
            j.crash();
            drop(j);
            fs = fs.power_loss_with(seed * 1000 + cycle, mode);
        }
    }
    let shared: Arc<dyn Fs> = Arc::new(fs.clone());
    let (j, _, sink) = open(shared, dir).map_err(|e| format!("final open: {e:?}"))?;
    let first = sink.data();
    j.crash();
    drop(j);
    if !check(&first, &log, synced) {
        return Err(format!(
            "final: recovered {}, oracle {}, synced {synced}",
            first.len(),
            log.len()
        ));
    }
    let shared: Arc<dyn Fs> = Arc::new(fs.kill());
    let (j, _, sink) = open(shared, dir).map_err(|e| format!("second open: {e:?}"))?;
    let second = sink.data();
    j.crash();
    if second != first {
        return Err(format!(
            "second open differs: {} vs {}",
            second.len(),
            first.len()
        ));
    }
    Ok(())
}

fn run_campaign(seeds: u64, mode: PowerLoss) {
    let failures: Vec<String> = (0..seeds)
        .filter_map(|seed| {
            campaign(seed, mode)
                .err()
                .map(|e| format!("seed {seed}: {e}"))
        })
        .collect();
    for f in failures.iter().take(12) {
        eprintln!("{f}");
    }
    assert!(
        failures.is_empty(),
        "{} of {seeds} seeds failed",
        failures.len()
    );
}

/// RV-KV1 F2: NS05-style power loss, 150 seeds (debug default, `NS05_SEEDS` overrides).
#[test]
fn ns05_power_loss_campaign_on_fault_fs() {
    run_campaign(env_u64("NS05_SEEDS", 150), PowerLoss::default());
}

/// The full campaign: 2,000 seeds. Run with `--release --ignored`.
#[test]
#[ignore = "full NS05 campaign; run with --release --ignored"]
fn ns05_power_loss_campaign_on_fault_fs_full() {
    run_campaign(env_u64("NS05_SEEDS", 2000), PowerLoss::default());
}

/// The campaign on a device whose atomic unit is a 4 KiB page that a power loss can also
/// zero or fill with garbage as a whole (RV-KV1 F7.2), 100 seeds by default.
#[test]
fn ns05_power_loss_campaign_with_4k_units_zero_and_garbage() {
    run_campaign(env_u64("NS05_SEEDS", 100), PowerLoss::destroy_4k());
}

#[test]
#[ignore = "full NS05 campaign (4 KiB units); run with --release --ignored"]
fn ns05_power_loss_campaign_with_4k_units_zero_and_garbage_full() {
    run_campaign(env_u64("NS05_SEEDS", 2000), PowerLoss::destroy_4k());
}

/// Runs a workload of two opens (40 mixed writes each, a clean close in between) with
/// `crash_after(limit)`, returning the oracle log and how much of it a synced ack covers.
fn boundary_workload(limit: Option<u64>, fs: &FaultFs) -> (Vec<Vec<u8>>, usize) {
    if let Some(n) = limit {
        fs.crash_after(n);
    }
    let dir = Path::new("/db/journal");
    let mut log: Vec<Vec<u8>> = Vec::new();
    let mut synced = 0usize;
    for phase in 0..2 {
        let shared: Arc<dyn Fs> = Arc::new(fs.clone());
        let Ok((j, _, sink)) = open(shared, dir) else {
            break;
        };
        log.truncate(sink.records().len());
        let mut ok = true;
        for i in 0..40u32 {
            let b = sized(
                format!("p{phase}i{i:03}").as_bytes(),
                [512, 3000, 1500][i as usize % 3],
            );
            let data = b.data().to_vec();
            let sync = i % 4 == 3;
            let durability = if sync {
                Durability::Sync
            } else {
                Durability::NoSync
            };
            let Ok(c) = j.submit(b, durability, Box::new(())) else {
                ok = false;
                break;
            };
            log.push(data);
            if c.wait().is_err() {
                ok = false;
                break;
            }
            if sync {
                synced = log.len();
            }
        }
        if !ok {
            j.crash();
            break;
        }
        if phase == 0 {
            if j.close().is_ok() {
                synced = log.len();
            }
        } else {
            j.crash();
        }
        drop(j);
    }
    (log, synced)
}

/// Crash after every mutating call, then `crash()`, `power_loss(seed)`, `kill()` or a
/// power loss with 4 KiB units that may be zeroed or turned to garbage.
/// `stride` > 1 checks every stride-th boundary (plus the last).
fn run_boundaries(stride: u64) {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    boundary_workload(None, &fs);
    let total = fs.mutating_ops();
    let mut failures = Vec::new();
    let mut checked = 0;
    let boundaries = (0..=total).filter(|n| n % stride == 0 || *n == total);
    for n in boundaries {
        for mode in 0..4u64 {
            let fs = FaultFs::new();
            fs.create_dir_all_durable(Path::new("/db"));
            let (log, synced) = boundary_workload(Some(n), &fs);
            let after = match mode {
                0 => fs.crash(),
                1 => fs.power_loss(n * 3 + 1),
                2 => fs.kill(),
                _ => fs.power_loss_with(n * 3 + 2, PowerLoss::destroy_4k()),
            };
            checked += 1;
            match open(Arc::new(after), Path::new("/db/journal")) {
                Ok((j, _, sink)) => {
                    let got = sink.data();
                    j.crash();
                    let prefix = got.len() <= log.len() && got[..] == log[..got.len()];
                    if !prefix || got.len() < synced {
                        failures.push(format!(
                            "n={n} mode={mode}: got {} of {} (synced {synced}), prefix {prefix}",
                            got.len(),
                            log.len()
                        ));
                    }
                }
                Err(e) => failures.push(format!("n={n} mode={mode}: open failed {e:?}")),
            }
        }
    }
    eprintln!(
        "{total} boundaries, {checked} crash cases checked; {} failures",
        failures.len()
    );
    for f in failures.iter().take(12) {
        eprintln!("{f}");
    }
    assert!(failures.is_empty(), "{} failures", failures.len());
}

/// RV-KV1 F2: NS04-style crash at every boundary (debug default: every 3rd boundary,
/// `NS04_STRIDE` overrides).
#[test]
fn ns04_crash_at_every_boundary() {
    run_boundaries(env_u64("NS04_STRIDE", 3).max(1));
}

/// Every boundary, four crash modes each. Run with `--release --ignored`.
#[test]
#[ignore = "full NS04 enumeration; run with --release --ignored"]
fn ns04_crash_at_every_boundary_full() {
    run_boundaries(1);
}

// ---------------------------------------------------------------------------------------
// F3: segment header blocks

/// RV-KV1 F3: a 4 KiB unit holding header copy A is destroyed whole (a torn rewrite on a
/// device with 4 KiB atomic units). Copy B lives in its own block and survives.
#[test]
fn ns25_destroyed_header_block_of_an_empty_newest_segment_opens() {
    let dir = TempDir::new("hdr");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let (j, _, _) = open(fs.clone(), dir).unwrap();
    j.submit(sized(b"a1", 512), Durability::Sync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    j.crash();
    drop(j);
    // This open fences the crash end and activates a new, empty segment.
    let (j, _, _) = open(fs.clone(), dir).unwrap();
    let newest = j.segments().last().unwrap().segment;
    j.crash();
    drop(j);
    let path = segment::path_of(dir, newest, segment::LIVE_EXT);
    overwrite(&path, 0, &[0xA5u8; 4096]);
    let (j, report, sink) = open(fs.clone(), dir).expect("copy B keeps the segment");
    assert_eq!(sink.records().len(), 1);
    assert_eq!(report.last_seq, 1);
    j.crash();
    drop(j);

    // Both header blocks destroyed (media damage) on the empty newest segment: nothing
    // chains into it and it holds no record, so it is an unused segment (L4).
    let (j, _, _) = open(fs.clone(), dir).unwrap();
    let newest = j.segments().last().unwrap().segment;
    j.crash();
    drop(j);
    let path = segment::path_of(dir, newest, segment::LIVE_EXT);
    overwrite(&path, 0, &[0xA5u8; 8192]);
    let (j, report, sink) = open(fs.clone(), dir).expect("an empty header-less segment is free");
    assert_eq!(sink.records().len(), 1);
    assert_eq!(report.last_seq, 1);
    assert!(!j.segments().iter().any(|s| s.segment == newest));
    assert_eq!(
        j.submit(sized(b"a2", 512), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap(),
        2
    );
    j.crash();
}

/// Both header blocks of a newest segment that holds synced records are destroyed: the
/// records are still recognisable, so the segment is not free and the open refuses (R3).
#[test]
fn ns07_destroyed_headers_of_a_segment_with_records_refuse() {
    let dir = TempDir::new("hdr-records");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let (j, _, _) = open(fs.clone(), dir).unwrap();
    for i in 0..3u8 {
        j.submit(sized(&[b'h', i], 700), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    let newest = j.segments().last().unwrap().segment;
    j.crash();
    drop(j);
    overwrite(
        &segment::path_of(dir, newest, segment::LIVE_EXT),
        0,
        &[0u8; 8192],
    );
    let err = open(fs, dir).err().expect("refused");
    assert!(
        matches!(err, Error::Corruption { file: FileId::Journal { segment }, .. } if segment == newest),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------------------
// F4: sync after NoSpace

/// RV-KV1 F4: after a `NoSpace` while preparing the next segment, a `sync()` with no
/// later write succeeds once space is back, and `visible_seq` catches up.
#[test]
fn ns11_sync_after_no_space_without_a_later_write_succeeds() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let shared: Arc<dyn Fs> = Arc::new(fs.clone());
    let (j, _, _) = open(shared, Path::new("/db/journal")).unwrap();
    fs.inject(
        Fault::on(FsOp::Preallocate)
            .error(FaultError::Enospc)
            .repeat(),
    );
    let mut failed = None;
    for i in 0..400u32 {
        let c = j
            .submit(
                sized(format!("x{i:05}").as_bytes(), 1500),
                Durability::Sync,
                Box::new(()),
            )
            .unwrap();
        let seq = c.seq();
        match c.wait() {
            Ok(_) => {}
            Err(Error::NoSpace) => {
                failed = Some(seq);
                break;
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    let failed = failed.expect("a roll hit ENOSPC");
    assert!(!j.requires_recovery());
    // Still no space: the sync cannot void the failed number either.
    fs.clear_faults();
    let s1 = j.sync().unwrap().wait();
    assert_eq!(s1, Ok(failed), "sync once space is back");
    assert_eq!(j.durable_seq(), failed);
    assert_eq!(j.visible_seq(), failed);
    let s2 = j.sync().unwrap().wait();
    assert_eq!(s2, Ok(failed));
    let w = j
        .submit(sized(b"after", 1500), Durability::Sync, Box::new(()))
        .unwrap()
        .wait();
    assert_eq!(w, Ok(failed + 1));
    j.close().unwrap();
    drop(j);
    let (j, report, sink) = open(Arc::new(fs.kill()), Path::new("/db/journal")).unwrap();
    assert_eq!(report.last_seq, failed + 1);
    assert_eq!(sink.records().last().unwrap().first_seq, failed + 1);
    assert!(sink.records().iter().all(|r| r.first_seq != failed));
    j.crash();
}

// ---------------------------------------------------------------------------------------
// F11: a damaged ROLL pad with evidence

/// RV-KV1 F11. Segment A holds synced records S0..S2 and unsynced U0..U2; a power loss
/// zeroes U0's header, so the next open fences A at U0 (a ROLL pad there) and continues
/// in segment B, where synced records name everything durable. Leftovers of A's epoch sit
/// after the fence at a 4 KiB boundary. Then the fence pad is damaged. Recovery must
/// continue in segment B (whose first sequence is exactly the fence's), never in the
/// leftovers after the pad.
#[test]
fn ns07_damaged_fence_pad_with_evidence_never_resumes_in_leftovers() {
    let dir = TempDir::new("fence-pad");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let (j, _, sink) = open(fs.clone(), dir).unwrap();
    for i in 0..3u8 {
        j.submit(sized(&[b's', i], 600), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    for i in 0..3u8 {
        j.submit(sized(&[b'u', i], 600), Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    j.crash();
    drop(j);
    let run1 = sink.records();
    let (seg_a, u0_at) = run1[3].at.unwrap();
    let (_, u1_at) = run1[4].at.unwrap();
    let path = segment::path_of(dir, seg_a, segment::LIVE_EXT);
    let leftovers = std::fs::read(&path).unwrap()[u1_at as usize..u1_at as usize + 1200].to_vec();
    overwrite(&path, u0_at, &[0u8; RECORD_HEADER_LEN]);

    // Run 2: L2 at U0, fenced there; three synced records in segment B.
    let (j, report, sink) = open(fs.clone(), dir).unwrap();
    assert_eq!(
        sink.data(),
        run1[..3].iter().map(|r| r.data.clone()).collect::<Vec<_>>()
    );
    // U1 starts mid-block, so the scan sees nothing after U0's zero header: L1.
    assert_eq!(report.tail, TailOutcome::Clean);
    for i in 0..3u8 {
        j.submit(sized(&[b'b', i], 600), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    let run2 = sink.records();
    let seg_b = j.segments().last().unwrap().segment;
    j.crash();
    drop(j);
    assert_ne!(seg_b, seg_a);
    assert_eq!(run2[3].at.unwrap().0, seg_b);

    let bytes = std::fs::read(&path).unwrap();
    let format::HeaderCheck::Valid(pad) = format::check_header(&bytes[u0_at as usize..]) else {
        panic!("expected the fence pad at {u0_at}")
    };
    assert!(pad.is_pad() && pad.flags & format::FLAG_ROLL != 0);
    // Leftovers of A's epoch (copies of U1 and U2) at the next 4 KiB boundary.
    let leftover_at = format::align_up(u0_at + 1);
    overwrite(&path, leftover_at, &leftovers);
    let format::HeaderCheck::Valid(l) =
        format::check_header(&std::fs::read(&path).unwrap()[leftover_at as usize..])
    else {
        panic!("planted leftover must parse")
    };
    assert_eq!(l.first_seq, run1[4].first_seq);
    // Damage the fence pad's header.
    overwrite(&path, u0_at + 20, &[0xFF]);

    for replay_after in [0, run1[4].first_seq] {
        let (j, _, sink) = open_after(fs.clone(), dir, replay_after)
            .expect("the fence's sequence is where segment B starts");
        let got = sink.data();
        j.crash();
        let expect: Vec<Vec<u8>> = run2
            .iter()
            .filter(|r| r.first_seq > replay_after)
            .map(|r| r.data.clone())
            .collect();
        assert_eq!(
            got, expect,
            "replay_after {replay_after}: continue in segment B"
        );
    }
}

// ---------------------------------------------------------------------------------------
// NS24 (journal level): an unreadable region is never a torn tail

#[test]
fn ns24_eio_region_in_recovery_refuses_and_truncates_nothing() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let dir = Path::new("/db/journal");
    let (j, _, sink) = open(Arc::new(fs.clone()), dir).unwrap();
    for i in 0..6u8 {
        j.submit(sized(&[b'e', i], 700), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    let written = sink.data();
    let (segment, at) = sink.records()[3].at.unwrap();
    j.crash();
    drop(j);
    let fs = fs.crash();
    // The 512 bytes holding the fourth record's header read back as EIO.
    let path = segment::path_of(dir, segment, segment::LIVE_EXT);
    fs.inject(
        Fault::on(FsOp::Read)
            .path_prefix(path.clone())
            .range(at, 512)
            .repeat(),
    );
    fs.clear_calls();
    let err = open(Arc::new(fs.clone()), dir).err().expect("R2 refuses");
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    let writes: Vec<_> = fs
        .calls()
        .into_iter()
        .filter(|c| {
            matches!(
                c.op,
                FsOp::Write | FsOp::Rename | FsOp::Remove | FsOp::SetLen | FsOp::Preallocate
            )
        })
        .collect();
    assert!(
        writes.is_empty(),
        "a refused open writes nothing: {writes:?}"
    );
    // Once the region reads again, every record is there: nothing was truncated.
    fs.clear_faults();
    let (j, report, sink) = open(Arc::new(fs.clone()), dir).unwrap();
    assert_eq!(sink.data(), written);
    assert_eq!(report.tail, TailOutcome::Clean);
    j.crash();
}

// ---------------------------------------------------------------------------------------
// F13: R3 sub-cases (structural damage refuses the open)

/// Writes `n` synced store-wide records and returns their segment, the offsets of every
/// record (scanned from the file) and the segment path.
fn synced_records(dir: &Path, n: usize) -> (u64, Vec<u64>, std::path::PathBuf) {
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let (j, _, _) = open(fs, dir).unwrap();
    for i in 0..n {
        j.submit(
            plain(format!("k{i}").as_bytes(), 300),
            Durability::Sync,
            Box::new(()),
        )
        .unwrap()
        .wait()
        .unwrap();
    }
    let seg = j.segments().last().unwrap().segment;
    j.crash();
    drop(j);
    let path = segment::path_of(dir, seg, segment::LIVE_EXT);
    let bytes = std::fs::read(&path).unwrap();
    let mut offsets = Vec::new();
    let mut pos = format::RECORDS_START;
    while let format::HeaderCheck::Valid(h) = format::check_header(&bytes[pos as usize..]) {
        if !h.is_pad() {
            offsets.push(pos);
        }
        pos += h.record_len();
    }
    assert_eq!(offsets.len(), n);
    (seg, offsets, path)
}

/// Rewrites the header of the record at `at` through `edit`, keeping its CRCs valid.
fn rewrite_header(path: &Path, at: u64, edit: impl FnOnce(&mut [u8])) {
    let mut bytes = std::fs::read(path).unwrap();
    let at = at as usize;
    let scope_len = usize::from(bytes[at + 14]);
    edit(&mut bytes[at..at + RECORD_HEADER_LEN]);
    let crc = crc::mask(crc::crc32c(
        &bytes[at + 4..at + RECORD_HEADER_LEN + scope_len],
    ));
    bytes[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    overwrite(path, at as u64, &bytes[at..at + RECORD_HEADER_LEN]);
}

fn assert_r3(dir: &Path, segment: u64, what: &str) {
    let err = open(Arc::new(RealFs), dir)
        .err()
        .unwrap_or_else(|| panic!("{what}: the open must refuse"));
    match err {
        Error::Corruption {
            file: FileId::Journal { segment: s },
            what: w,
            ..
        } => {
            assert_eq!(s, segment, "{what}");
            assert!(w.contains(what), "expected {what:?}, got {w:?}");
        }
        other => panic!("{what}: expected Corruption, got {other:?}"),
    }
    // Refusing changes nothing: a second open refuses the same way.
    assert!(open(Arc::new(RealFs), dir).is_err());
}

#[test]
fn ns07_r3_sequence_gap_between_valid_records_refuses() {
    let dir = TempDir::new("r3-gap");
    let (seg, offsets, path) = synced_records(dir.path(), 3);
    rewrite_header(&path, offsets[1], |h| {
        h[24..32].copy_from_slice(&7u64.to_le_bytes())
    });
    assert_r3(dir.path(), seg, "discontinuity");
}

#[test]
fn ns07_r3_unknown_record_kind_refuses() {
    let dir = TempDir::new("r3-kind");
    let (seg, offsets, path) = synced_records(dir.path(), 3);
    rewrite_header(&path, offsets[1], |h| h[12] = 9);
    assert_r3(dir.path(), seg, "unknown journal record kind");
}

#[test]
fn ns07_r3_record_from_a_future_epoch_refuses() {
    let dir = TempDir::new("r3-epoch");
    let (seg, offsets, path) = synced_records(dir.path(), 3);
    rewrite_header(&path, offsets[1], |h| {
        let epoch = u64::from_le_bytes(h[16..24].try_into().unwrap());
        h[16..24].copy_from_slice(&(epoch + 1).to_le_bytes());
    });
    assert_r3(dir.path(), seg, "future epoch");
}

#[test]
fn ns07_r3_segments_that_do_not_chain_refuse() {
    let dir = TempDir::new("r3-chain");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..opts()
    };
    let sink = Arc::new(Sink::default());
    let (j, _) = Journal::open(fs.clone(), dir, options.clone(), 0, sink).unwrap();
    for i in 0..12 {
        j.submit(
            plain(format!("k{i}").as_bytes(), 2000),
            Durability::Sync,
            Box::new(()),
        )
        .unwrap()
        .wait()
        .unwrap();
    }
    let segs = j.segments();
    j.crash();
    drop(j);
    assert!(segs.len() >= 3, "{segs:?}");
    // Rewrite the second segment's header (both copies) with a wrong first sequence.
    let second = &segs[1];
    let header = format::SegmentHeader {
        segment: second.segment,
        epoch: second.epoch,
        first_seq: second.first_seq + 1,
    };
    let path = segment::path_of(dir, second.segment, segment::LIVE_EXT);
    overwrite(&path, 0, &header.encode_blocks());
    let err = Journal::open(fs, dir, options, 0, Arc::new(Sink::default()))
        .err()
        .expect("refused");
    assert!(
        matches!(
            err,
            Error::Corruption {
                file: FileId::Journal { .. },
                what,
                ..
            } if what.contains("chain")
        ),
        "{err:?}"
    );
}

#[test]
fn ns07_r3_damaged_header_of_a_live_segment_refuses() {
    let dir = TempDir::new("r3-header");
    let dir = dir.path();
    let fs: Arc<dyn Fs> = Arc::new(RealFs);
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..opts()
    };
    let sink = Arc::new(Sink::default());
    let (j, _) = Journal::open(fs.clone(), dir, options.clone(), 0, sink).unwrap();
    for i in 0..12 {
        j.submit(
            plain(format!("k{i}").as_bytes(), 2000),
            Durability::Sync,
            Box::new(()),
        )
        .unwrap()
        .wait()
        .unwrap();
    }
    let segs = j.segments();
    j.crash();
    drop(j);
    // Both copies of an older (not newest) segment's header are damaged.
    let first = &segs[0];
    let path = segment::path_of(dir, first.segment, segment::LIVE_EXT);
    overwrite(&path, 0, &[0x5Au8; 8192]);
    let err = Journal::open(fs, dir, options, 0, Arc::new(Sink::default()))
        .err()
        .expect("refused");
    assert!(
        matches!(
            err,
            Error::Corruption {
                file: FileId::Journal { segment },
                what,
                ..
            } if segment == first.segment && what.contains("header")
        ),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------------------
// F6: read_value

#[test]
fn ns07_read_value_rejects_overflowing_locators_and_reports_recycled_segments() {
    use crate::cache::BlockCache;
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let shared: Arc<dyn Fs> = Arc::new(fs.clone());
    let sink = Arc::new(Sink::default());
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..opts()
    };
    let (j, _) = Journal::open(shared, Path::new("/db/journal"), options, 0, sink.clone()).unwrap();
    let cache = Arc::new(BlockCache::new(1 << 20));
    j.set_value_cache(cache.clone(), cache.new_namespace());
    for i in 0..12u8 {
        j.submit(sized(&[b'v', i], 2000), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    let locators: Vec<Locator> = sink.records().iter().map(|r| r.locators[0]).collect();
    // Reads go through the cache: the second pass is all hits.
    for l in &locators {
        assert_eq!(j.read_value_with(l, false).unwrap().len(), l.len as usize);
    }
    assert_eq!(
        cache.stats().inserts,
        0,
        "fill_cache = false inserts nothing"
    );
    for l in &locators {
        j.read_value(l).unwrap();
    }
    let hits = cache.stats().hits;
    for l in &locators {
        j.read_value(l).unwrap();
    }
    assert_eq!(cache.stats().hits, hits + locators.len() as u64);

    let what = |r: Result<Bytes>| match r {
        Err(Error::Corruption { what, .. }) => what,
        other => panic!("expected Corruption, got {other:?}"),
    };
    // An offset near u64::MAX must neither overflow nor allocate.
    let mut bad = locators[0];
    bad.offset = u64::MAX - 3;
    assert_eq!(what(j.read_value(&bad)), VALUE_OUT_OF_RANGE);
    bad = locators[0];
    bad.len = u32::MAX;
    assert_eq!(what(j.read_value(&bad)), VALUE_OUT_OF_RANGE);
    bad = locators[0];
    bad.offset = 100;
    assert_eq!(what(j.read_value(&bad)), VALUE_OUT_OF_RANGE);
    // A retired segment's locator reads as recycled, not as damage.
    let first = j.segments()[0].segment;
    j.retire(first).unwrap();
    let recycled = locators.iter().find(|l| l.segment == first).unwrap();
    assert_eq!(what(j.read_value(recycled)), VALUE_RECYCLED);
    // A flipped value byte is a checksum failure (read without the cache's copy).
    let l = *locators.last().unwrap();
    let path = segment::path_of(Path::new("/db/journal"), l.segment, segment::LIVE_EXT);
    fs.flip_bit(&path, l.offset + 3, 0).unwrap();
    cache.erase(&CacheKey::Journal {
        store: j.shared.value_cache.get().unwrap().1,
        segment: l.segment,
        epoch: l.epoch,
        offset: l.offset,
    });
    assert_eq!(what(j.read_value(&l)), VALUE_CHECKSUM);
    j.crash();
}

// ---------------------------------------------------------------------------------------
// F8: spare segments are prepared in the background

/// Once the active segment is half used, the `kv-journal-prep` thread prepares one free
/// segment without any commit asking for it, the roll takes that spare, and the number of
/// prepared files stays bounded.
#[test]
fn ns08_spare_segments_are_prepared_off_the_commit_path() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..opts()
    };
    let dir = Path::new("/db/journal");
    let sink = Arc::new(Sink::default());
    let (j, _) = Journal::open(Arc::new(fs.clone()), dir, options, 0, sink).unwrap();
    let free = |j: &Journal| lock(&j.shared.table).free.len();
    let wait_for_spare = |j: &Journal| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while free(j) == 0 {
            assert!(std::time::Instant::now() < deadline, "no spare prepared");
            std::thread::yield_now();
        }
    };
    assert_eq!(free(&j), 0, "a fresh journal has no spare yet");
    // Past the half of the first segment: the preparer makes one spare.
    for i in 0..3u8 {
        j.submit(sized(&[b'h', i], 1500), Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    wait_for_spare(&j);
    let segments = j.segments().len();
    for round in 0..10u8 {
        // Fill until the journal rolls, then the spare comes back.
        while j.segments().len() == segments + usize::from(round) {
            wait_for_spare(&j);
            j.submit(sized(&[b'r', round], 1500), Durability::Sync, Box::new(()))
                .unwrap()
                .wait()
                .unwrap();
        }
        assert!(free(&j) <= 1, "bounded: {} spares", free(&j));
    }
    // Every roll took a prepared spare: the commit thread never created a file, so the
    // files are the live segments plus at most one spare.
    let files = fs.list(dir).unwrap().len();
    assert!(files <= j.segments().len() + 1, "{files} files");
    j.crash();
}
