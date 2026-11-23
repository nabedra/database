//! Journal tests. Scenario IDs follow `plans/native-storage-engine.md` (Tests). Fault
//! injection uses the small [`FaultFs`] below until NS1's `FaultFs` lands.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use super::*;
use crate::fs::{Fs, FsFile, FsLock, OpenMode, RealFs};

// ---------------------------------------------------------------------------------------
// Helpers

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nabedra-kv-journal-{name}-{}-{}",
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

/// Seeded xorshift64*.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Applied {
    kind: RecordKind,
    first_seq: Seq,
    count: u32,
    scope: Vec<u8>,
    data: Vec<u8>,
    locators: Vec<Locator>,
    /// File offset of the record (computed from the first journal value, when there is one).
    record_at: Option<(u64, u64)>,
}

#[derive(Default)]
struct RecordingSink {
    applied: Mutex<Vec<Applied>>,
    fail: AtomicBool,
}

impl RecordingSink {
    fn records(&self) -> Vec<Applied> {
        self.applied.lock().unwrap().clone()
    }

    fn seqs(&self) -> Vec<Seq> {
        self.records().iter().map(|a| a.first_seq).collect()
    }
}

impl JournalSink for RecordingSink {
    fn apply(
        &self,
        kind: RecordKind,
        first_seq: Seq,
        batch: &WriteBatch,
        locators: &[Locator],
    ) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::Io(io::ErrorKind::Other));
        }
        let mut applied = self.applied.lock().unwrap();
        if let Some(last) = applied.last() {
            assert!(
                first_seq >= last.first_seq + u64::from(last.count),
                "sink order"
            );
        }
        let record_at = batch.journal_values().first().map(|jv| {
            let l = locators[0];
            (
                l.segment,
                l.offset - u64::from(jv.offset) - batch.scope().len() as u64 - 48,
            )
        });
        applied.push(Applied {
            kind,
            first_seq,
            count: batch.count(),
            scope: batch.scope().to_vec(),
            data: batch.data().to_vec(),
            locators: locators.to_vec(),
            record_at,
        });
        Ok(())
    }
}

fn small() -> JournalOptions {
    JournalOptions {
        segment_bytes: 64 << 10,
        max_group_bytes: 4 << 20,
        zero_fill: true,
    }
}

fn open(
    fs: Arc<dyn Fs>,
    dir: &Path,
    options: JournalOptions,
    replay_after: Seq,
) -> Result<(Journal, RecoveryReport, Arc<RecordingSink>)> {
    let sink = Arc::new(RecordingSink::default());
    let (journal, report) = Journal::open(fs, dir, options, replay_after, sink.clone())?;
    Ok((journal, report, sink))
}

fn real() -> Arc<dyn Fs> {
    Arc::new(RealFs)
}

/// One `put_journal` (so tests can locate the record) under `scope`.
fn batch(scope: &[u8], key: &[u8], value: &[u8]) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.set_scope(scope);
    let mut k = scope.to_vec();
    k.extend_from_slice(key);
    b.put_journal(&k, value);
    b
}

fn put(journal: &Journal, b: WriteBatch, durability: Durability) -> Seq {
    journal
        .submit(b, durability, Box::new(()))
        .unwrap()
        .wait()
        .unwrap()
}

fn seg_path(dir: &Path, segment: u64) -> PathBuf {
    segment::path_of(dir, segment, segment::LIVE_EXT)
}

fn flip(path: &Path, offset: u64) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0u8; 1];
    file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 0x5A;
    file.write_all_at(&byte, offset).unwrap();
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        thread::park();
    }
}

// ---------------------------------------------------------------------------------------
// A minimal fault-injecting Fs (journal-local; NS1's FaultFs replaces it).

#[derive(Default)]
struct Faults {
    fail_sync: AtomicBool,
    fail_write: AtomicBool,
    enospc_preallocate: AtomicBool,
    syncs: AtomicU64,
    writes: AtomicU64,
    /// While set, the sync with index `syncs_allowed` (0-based, counting every
    /// `sync_data`) and later ones wait until it clears or the bound rises.
    hold_syncs: AtomicBool,
    syncs_allowed: AtomicU64,
}

struct FaultFs {
    inner: RealFs,
    faults: Arc<Faults>,
}

struct FaultFile {
    inner: Arc<dyn FsFile>,
    faults: Arc<Faults>,
}

impl Fs for FaultFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        Ok(Arc::new(FaultFile {
            inner: self.inner.open(path, mode)?,
            faults: self.faults.clone(),
        }))
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

impl FsFile for FaultFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.faults.writes.fetch_add(1, Ordering::SeqCst);
        if self.faults.fail_write.load(Ordering::SeqCst) {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        let index = self.faults.syncs.fetch_add(1, Ordering::SeqCst);
        while self.faults.hold_syncs.load(Ordering::SeqCst)
            && index >= self.faults.syncs_allowed.load(Ordering::SeqCst)
        {
            nabedra_rt::time::thread_sleep(std::time::Duration::from_millis(1));
        }
        if self.faults.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.sync_data()
    }
    fn preallocate(&self, len: u64) -> io::Result<()> {
        if self.faults.enospc_preallocate.load(Ordering::SeqCst) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
}

fn fault_fs() -> (Arc<dyn Fs>, Arc<Faults>) {
    let faults = Arc::new(Faults::default());
    (
        Arc::new(FaultFs {
            inner: RealFs,
            faults: faults.clone(),
        }),
        faults,
    )
}

// ---------------------------------------------------------------------------------------
// Basic behavior

#[test]
fn ns_unit_submit_replay_round_trip_with_scopes_and_locators() {
    let dir = TempDir::new("roundtrip");
    let (journal, report, sink) = open(real(), dir.path(), small(), 0).unwrap();
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!((journal.visible_seq(), journal.durable_seq()), (0, 0));

    let mut multi = WriteBatch::new();
    multi.set_scope(b"g1/");
    multi.put(b"g1/a", b"1");
    multi.put_journal(b"g1/log/1", b"entry-one");
    multi.delete_range(b"g1/b", b"g10");
    let seq = put(&journal, multi.clone(), Durability::Sync);
    assert_eq!(seq, 3);
    let seq2 = put(&journal, batch(b"", b"store", b"wide"), Durability::NoSync);
    assert_eq!(seq2, 4);
    assert_eq!(journal.visible_seq(), 4);
    assert!(journal.durable_seq() >= 3);

    let records = sink.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].scope, b"g1/");
    assert_eq!(
        journal.read_value(&records[0].locators[0]).unwrap(),
        &b"entry-one"[..]
    );
    assert_eq!(
        journal.read_value(&records[1].locators[0]).unwrap(),
        &b"wide"[..]
    );

    let mut outside = WriteBatch::new();
    outside.set_scope(b"g1/");
    outside.put(b"g2/x", b"1");
    assert_eq!(
        journal
            .submit(outside, Durability::Sync, Box::new(()))
            .err(),
        Some(Error::InvalidArgument("batch leaves its scope"))
    );
    journal.close().unwrap();
    assert_eq!(
        journal
            .submit(WriteBatch::new(), Durability::Sync, Box::new(()))
            .err(),
        Some(Error::Closed)
    );
    drop(journal);

    let (journal, report, sink2) = open(real(), dir.path(), small(), 0).unwrap();
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!(report.replayed_records, 2);
    assert_eq!(report.last_seq, 4);
    assert_eq!(
        sink2.records(),
        records,
        "replay hands the sink the same records"
    );
    assert_eq!(
        journal.read_value(&records[0].locators[0]).unwrap(),
        &b"entry-one"[..]
    );
    assert_eq!(put(&journal, batch(b"", b"k", b"v"), Durability::Sync), 5);
}

#[test]
fn ns_unit_empty_batches_sync_and_async_done() {
    let dir = TempDir::new("async");
    let (journal, _, _) = open(real(), dir.path(), small(), 0).unwrap();
    let commit = journal
        .submit(WriteBatch::new(), Durability::NoSync, Box::new(()))
        .unwrap();
    assert_eq!(commit.seq(), 0);
    assert_eq!(block_on(commit.done()).unwrap(), 0);
    let a = journal
        .submit(batch(b"", b"a", b"1"), Durability::NoSync, Box::new(()))
        .unwrap();
    let sync = journal.sync().unwrap();
    assert_eq!(sync.seq(), 1);
    assert_eq!(block_on(a.done()).unwrap(), 1);
    assert_eq!(sync.wait().unwrap(), 1);
    assert!(journal.durable_seq() >= 1);
}

// ---------------------------------------------------------------------------------------
// NS08: group commit

struct DropProbe(Arc<Mutex<Vec<String>>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        let name = thread::current().name().unwrap_or("?").to_string();
        self.0.lock().unwrap().push(name);
    }
}

#[test]
#[allow(clippy::disallowed_methods)] // short-lived test threads, joined before the test ends
fn ns08_group_commit_shares_syncs_across_concurrent_writers() {
    let dir = TempDir::new("ns08");
    let options = JournalOptions {
        segment_bytes: 1 << 20,
        ..JournalOptions::default()
    };
    let (journal, _, sink) = open(real(), dir.path(), options.clone(), 0).unwrap();
    let journal = Arc::new(journal);
    let drops = Arc::new(Mutex::new(Vec::new()));
    const THREADS: u64 = 16;
    const PER_THREAD: u64 = 100;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let journal = journal.clone();
        let drops = drops.clone();
        handles.push(thread::spawn(move || {
            let mut rng = Rng(0x9E37_79B9 + t);
            let scope = format!("g{t:02}/").into_bytes();
            for i in 0..PER_THREAD {
                let value = vec![t as u8; 16 + rng.below(200) as usize];
                let b = batch(&scope, format!("{i}").as_bytes(), &value);
                if rng.below(4) == 0 {
                    // An unsynced write followed by a synced one: the sync covers both.
                    let a = journal
                        .submit(b, Durability::NoSync, Box::new(DropProbe(drops.clone())))
                        .unwrap();
                    let a_seq = a.seq();
                    let s = journal
                        .submit(
                            batch(&scope, format!("{i}s").as_bytes(), b"x"),
                            Durability::Sync,
                            Box::new(DropProbe(drops.clone())),
                        )
                        .unwrap();
                    s.wait().unwrap();
                    assert!(journal.durable_seq() >= a_seq);
                    a.wait().unwrap();
                } else {
                    let durability = if rng.below(2) == 0 {
                        Durability::Sync
                    } else {
                        Durability::NoSync
                    };
                    let c = journal
                        .submit(b, durability, Box::new(DropProbe(drops.clone())))
                        .unwrap();
                    let seq = c.wait().unwrap();
                    if durability == Durability::Sync {
                        assert!(journal.durable_seq() >= seq);
                    }
                    assert!(journal.visible_seq() >= seq);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let unsynced_tail = journal
        .submit(batch(b"", b"tail", b"t"), Durability::NoSync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    let sync = journal.sync().unwrap();
    assert_eq!(sync.wait().unwrap(), unsynced_tail);
    assert!(journal.durable_seq() >= unsynced_tail);

    let stats = journal.stats();
    println!(
        "ns08: commits {} synced {} journal_syncs {} group_commits {} ratio {:.3} syncs/synced commit",
        stats.commits,
        stats.synced_commits,
        stats.journal_syncs,
        stats.group_commits,
        stats.journal_syncs as f64 / stats.synced_commits as f64
    );
    assert!(stats.journal_syncs < stats.synced_commits, "{stats:?}");
    assert!(stats.group_commits < stats.commits, "{stats:?}");
    let drops = drops.lock().unwrap().clone();
    assert!(!drops.is_empty());
    assert!(
        drops.iter().all(|name| name == "kv-journal"),
        "on_done payloads are dropped on the journal thread"
    );
    let written = sink.records();
    drop(journal);

    let (_journal, report, replayed) = open(real(), dir.path(), options, 0).unwrap();
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!(replayed.records(), written);
    let mut next = 1;
    for r in &written {
        assert_eq!(r.first_seq, next, "sequence numbers are contiguous");
        next += u64::from(r.count);
    }
}

// ---------------------------------------------------------------------------------------
// NS06 / NS25: tails without evidence

/// Writes `synced` synced records then `unsynced` unsynced ones and "crashes".
fn crash_after(dir: &Path, synced: u64, unsynced: u64) -> Vec<Applied> {
    let (journal, _, sink) = open(real(), dir, small(), 0).unwrap();
    for i in 0..synced {
        put(
            &journal,
            batch(b"g/", format!("s{i}").as_bytes(), &[1; 100]),
            Durability::Sync,
        );
    }
    for i in 0..unsynced {
        put(
            &journal,
            batch(b"g/", format!("u{i}").as_bytes(), &[2; 100]),
            Durability::NoSync,
        );
    }
    journal.crash();
    sink.records()
}

fn reopen_twice(dir: &Path) -> (RecoveryReport, Vec<Applied>, RecoveryReport) {
    let (journal, first, sink) = open(real(), dir, small(), 0).unwrap();
    let records = sink.records();
    journal.crash();
    drop(journal);
    let (_journal, second, sink2) = open(real(), dir, small(), 0).unwrap();
    assert_eq!(
        sink2.records(),
        records,
        "a second open replays the same prefix"
    );
    assert_eq!(second.tail, TailOutcome::Clean);
    (first, records, second)
}

#[test]
fn ns06_torn_final_record_payload_truncates_to_exact_prefix() {
    let dir = TempDir::new("ns06");
    let written = crash_after(dir.path(), 10, 3);
    let last = written.last().unwrap();
    let (segment, at) = last.record_at.unwrap();
    // Tear the payload of the last record (written after the last completed sync).
    flip(&seg_path(dir.path(), segment), at + 60);
    let (report, records, _) = reopen_twice(dir.path());
    println!("ns06: recovery_ms {} report {report:?}", report.recovery_ms);
    assert_eq!(records, written[..written.len() - 1]);
    match report.tail {
        TailOutcome::TornTail {
            truncated_at_seq,
            bytes_discarded,
            ..
        } => {
            assert_eq!(truncated_at_seq, last.first_seq);
            assert!(bytes_discarded > 0);
        }
        other => panic!("expected a torn tail, got {other:?}"),
    }
    assert_eq!(report.tail_truncations, 1);
    assert_eq!(report.last_seq, last.first_seq - 1);
    assert!(report.damage.is_empty());
}

#[test]
fn ns25_torn_header_without_evidence_is_l2() {
    let dir = TempDir::new("ns25-header");
    let written = crash_after(dir.path(), 5, 4);
    // Tear the header of the second-to-last record: the last one stays valid but has no
    // evidence, so it is discarded too (exact prefix).
    let victim = &written[written.len() - 2];
    let (segment, at) = victim.record_at.unwrap();
    flip(&seg_path(dir.path(), segment), at + 20);
    let (report, records, _) = reopen_twice(dir.path());
    assert_eq!(records, written[..written.len() - 2]);
    assert!(matches!(
        report.tail,
        TailOutcome::TornTail { truncated_at_seq, .. } if truncated_at_seq == victim.first_seq
    ));
    assert_eq!(report.tail_truncations, 1);
    assert!(report.tail_bytes > 0);
}

#[test]
fn ns25_all_zero_header_is_a_clean_end() {
    let dir = TempDir::new("ns25-zero");
    let written = crash_after(dir.path(), 3, 3);
    let (report, records, _) = reopen_twice(dir.path());
    assert_eq!(records, written);
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!((report.tail_truncations, report.tail_bytes), (0, 0));
}

#[test]
fn ns25_lost_write_before_unsynced_records_ends_the_log() {
    // A lost (zeroed) header with valid unsynced records chained after it. An all-zero
    // header is an end marker; the boundary scan cannot see the chained records (they
    // never start a block, since only groups after a sync do, and those carry evidence),
    // so this is L1, and the records after the hole are dropped: an exact prefix.
    let dir = TempDir::new("ns25-hole");
    let written = crash_after(dir.path(), 2, 4);
    let victim = &written[3];
    let (segment, at) = victim.record_at.unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(seg_path(dir.path(), segment))
        .unwrap();
    std::os::unix::fs::FileExt::write_all_at(&file, &[0u8; 48], at).unwrap();
    let (report, records, _) = reopen_twice(dir.path());
    assert_eq!(records, written[..3]);
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!(report.last_seq, victim.first_seq - 1);
}

#[test]
fn ns25_short_segment_end_and_older_epoch_records_read_as_ends() {
    let dir = TempDir::new("ns25-epoch");
    let options = small();
    let (journal, _, sink) = open(real(), dir.path(), options.clone(), 0).unwrap();
    // One record leaving fewer than 48 bytes in the first segment: the next one rolls
    // without a roll pad.
    let room = options.segment_bytes - format::RECORDS_START;
    let mut big = WriteBatch::new();
    big.put_journal(b"k", &vec![7u8; (room - 20 - 48 - 6) as usize]);
    assert_eq!(48 + big.encoded_bytes() as u64, room - 20);
    put(&journal, big, Durability::NoSync);
    for i in 0..40 {
        put(
            &journal,
            batch(b"", format!("r{i}").as_bytes(), &[3; 3000]),
            Durability::Sync,
        );
    }
    let segments = journal.segments();
    assert!(segments.len() >= 3, "{segments:?}");
    assert_eq!(
        segments[1].first_seq, 2,
        "the second segment starts after the big record"
    );
    // Retire every inactive segment, then write into recycled files (new epochs).
    for s in segments.iter().filter(|s| !s.active) {
        journal.retire(s.segment).unwrap();
    }
    assert_eq!(
        journal
            .retire(segments.iter().find(|s| s.active).unwrap().segment)
            .err(),
        Some(Error::InvalidArgument("cannot retire the active segment"))
    );
    let before = sink.records().len();
    for i in 0..150 {
        put(
            &journal,
            batch(b"", format!("n{i}").as_bytes(), &[4; 1000]),
            Durability::NoSync,
        );
    }
    let reused: Vec<_> = journal.segments();
    assert!(
        reused.iter().any(|s| s.epoch >= 2),
        "a recycled file has a new epoch: {reused:?}"
    );
    let written = sink.records();
    let replay_after = written[before].first_seq - 1;
    journal.crash();
    drop(journal);
    // The recycled active segment ends mid-file, followed by records of an older epoch.
    let (journal, report, replayed) =
        open(real(), dir.path(), options.clone(), replay_after).unwrap();
    // The end reads as an older-epoch record (L1) when an old record starts exactly
    // there, and as a damaged header without evidence (L2) when the end falls inside an
    // old record: both keep exactly the written records.
    let last = written.last().unwrap();
    let end = last.first_seq + u64::from(last.count);
    assert!(
        matches!(report.tail, TailOutcome::Clean)
            || matches!(report.tail, TailOutcome::TornTail { truncated_at_seq, .. } if truncated_at_seq == end),
        "{:?}",
        report.tail
    );
    assert_eq!(replayed.records(), written[before..]);
    journal.crash();
    drop(journal);
    let (_j, report2, replayed2) = open(real(), dir.path(), options, replay_after).unwrap();
    assert_eq!(report2.tail, TailOutcome::Clean);
    assert_eq!(replayed2.records(), written[before..]);
}

// ---------------------------------------------------------------------------------------
// NS07 (journal level): damage with evidence

fn write_scoped(dir: &Path, close: bool) -> Vec<Applied> {
    let (journal, _, sink) = open(real(), dir, small(), 0).unwrap();
    put(&journal, batch(b"g1/", b"a", &[1; 64]), Durability::Sync);
    put(&journal, batch(b"g2/", b"b", &[2; 64]), Durability::Sync);
    put(&journal, batch(b"g1/", b"c", &[3; 64]), Durability::Sync);
    put(&journal, batch(b"", b"d", &[4; 64]), Durability::Sync);
    put(&journal, batch(b"g3/", b"e", &[5; 64]), Durability::Sync);
    if close {
        journal.close().unwrap();
    } else {
        journal.crash();
    }
    sink.records()
}

#[test]
fn ns07_group_scope_payload_damage_with_evidence_quarantines_the_group() {
    let dir = TempDir::new("ns07-g1");
    let written = write_scoped(dir.path(), false);
    let (segment, at) = written[0].record_at.unwrap();
    flip(&seg_path(dir.path(), segment), at + 70);
    let (_journal, report, sink) = open(real(), dir.path(), small(), 0).unwrap();
    assert_eq!(
        report.damage,
        vec![Damage {
            scope: Bytes::from_static(b"g1/"),
            reason: crate::DamageReason::JournalRecord,
            file: FileId::Journal { segment },
            first_bad_seq: Some(written[0].first_seq),
        }]
    );
    assert_eq!(report.skipped_records, 1, "the later g1 record is skipped");
    assert_eq!(
        sink.records(),
        vec![written[1].clone(), written[3].clone(), written[4].clone()]
    );
    assert_eq!(report.tail, TailOutcome::Clean);
}

#[test]
fn ns07_store_wide_or_unknown_scope_damage_with_evidence_refuses() {
    for (name, record, offset) in [("payload", 3usize, 70u64), ("header", 1, 20)] {
        let dir = TempDir::new(&format!("ns07-r1-{name}"));
        let written = write_scoped(dir.path(), false);
        let (segment, at) = written[record].record_at.unwrap();
        flip(&seg_path(dir.path(), segment), at + offset);
        let err = open(real(), dir.path(), small(), 0).err().unwrap();
        assert!(
            matches!(err, Error::Corruption { file: FileId::Journal { segment: s }, offset: o, .. } if s == segment && o == at),
            "{name}: {err:?}"
        );
        // Nothing was truncated: the refusal repeats.
        assert!(open(real(), dir.path(), small(), 0).is_err());
    }
}

#[test]
fn ns07_clean_close_gives_the_last_record_evidence() {
    let dir = TempDir::new("ns07-close");
    let written = write_scoped(dir.path(), true);
    let last = written.last().unwrap();
    let (segment, at) = last.record_at.unwrap();
    flip(&seg_path(dir.path(), segment), at + 70);
    let (_j, report, sink) = open(real(), dir.path(), small(), 0).unwrap();
    assert_eq!(report.damage.len(), 1);
    assert_eq!(report.damage[0].scope, &b"g3/"[..]);
    assert_eq!(report.tail, TailOutcome::Clean);
    assert_eq!(sink.records(), written[..written.len() - 1]);
}

#[test]
fn ns07_damage_below_replay_after_is_skipped() {
    let dir = TempDir::new("ns07-flushed");
    let written = write_scoped(dir.path(), false);
    let (segment, at) = written[3].record_at.unwrap();
    flip(&seg_path(dir.path(), segment), at + 70);
    let replay_after = written[3].first_seq;
    let (_j, report, sink) = open(real(), dir.path(), small(), replay_after).unwrap();
    assert_eq!(report.flushed_damage, 1);
    assert!(report.damage.is_empty());
    assert_eq!(sink.records(), written[4..]);
}

// ---------------------------------------------------------------------------------------
// NS09: EIO latches

#[test]
#[allow(clippy::disallowed_methods)] // short-lived test threads, joined before the test ends
fn ns09_eio_on_sync_latches_and_never_retries() {
    let dir = TempDir::new("ns09-sync");
    let (fs, faults) = fault_fs();
    let (journal, _, _) = open(fs.clone(), dir.path(), small(), 0).unwrap();
    put(&journal, batch(b"", b"ok", b"1"), Durability::Sync);
    faults.fail_sync.store(true, Ordering::SeqCst);
    let journal = Arc::new(journal);
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let journal = journal.clone();
            thread::spawn(move || {
                match journal.submit(
                    batch(b"", format!("k{i}").as_bytes(), b"v"),
                    Durability::Sync,
                    Box::new(()),
                ) {
                    Ok(commit) => commit.wait(),
                    Err(e) => Err(e),
                }
            })
        })
        .collect();
    for h in handles {
        let err = h.join().unwrap().unwrap_err();
        assert_eq!(err, Error::RecoveryRequired);
        assert!(err.is_ambiguous());
    }
    assert!(journal.requires_recovery());
    let syncs = faults.syncs.load(Ordering::SeqCst);
    faults.fail_sync.store(false, Ordering::SeqCst);
    assert_eq!(
        journal
            .submit(batch(b"", b"later", b"v"), Durability::Sync, Box::new(()))
            .err(),
        Some(Error::RecoveryRequired)
    );
    assert_eq!(journal.sync().err(), Some(Error::RecoveryRequired));
    assert_eq!(journal.close(), Err(Error::RecoveryRequired));
    assert_eq!(
        faults.syncs.load(Ordering::SeqCst),
        syncs,
        "no sync is retried"
    );
    drop(journal);
    // Reopening recovers from disk alone.
    let (journal, report, sink) = open(fs, dir.path(), small(), 0).unwrap();
    assert!(!journal.requires_recovery());
    assert!(!sink.records().is_empty());
    assert!(report.last_seq >= 1);
    assert!(put(&journal, batch(b"", b"new", b"v"), Durability::Sync) > report.last_seq);
}

#[test]
fn ns09_eio_on_write_latches_pending_commits() {
    let dir = TempDir::new("ns09-write");
    let (fs, faults) = fault_fs();
    let (journal, _, _) = open(fs.clone(), dir.path(), small(), 0).unwrap();
    put(&journal, batch(b"", b"ok", b"1"), Durability::NoSync);
    faults.fail_write.store(true, Ordering::SeqCst);
    let commits: Vec<_> = (0..5)
        .filter_map(|i| {
            journal
                .submit(
                    batch(b"", format!("k{i}").as_bytes(), b"v"),
                    Durability::NoSync,
                    Box::new(()),
                )
                .ok()
        })
        .collect();
    assert!(!commits.is_empty());
    for c in commits {
        assert_eq!(c.wait(), Err(Error::RecoveryRequired));
    }
    assert!(journal.requires_recovery());
    let writes = faults.writes.load(Ordering::SeqCst);
    faults.fail_write.store(false, Ordering::SeqCst);
    assert!(
        journal
            .submit(batch(b"", b"x", b"v"), Durability::NoSync, Box::new(()))
            .is_err()
    );
    drop(journal);
    assert_eq!(
        faults.writes.load(Ordering::SeqCst),
        writes,
        "no write is retried, no close pad"
    );
    let (_j, report, sink) = open(fs, dir.path(), small(), 0).unwrap();
    assert_eq!(sink.seqs(), vec![1]);
    assert_eq!(report.last_seq, 1);
}

#[test]
fn ns09_sink_failure_latches() {
    let dir = TempDir::new("ns09-sink");
    let (journal, _, sink) = open(real(), dir.path(), small(), 0).unwrap();
    sink.fail.store(true, Ordering::SeqCst);
    let c = journal
        .submit(batch(b"", b"k", b"v"), Durability::NoSync, Box::new(()))
        .unwrap();
    assert_eq!(c.wait(), Err(Error::RecoveryRequired));
    assert!(journal.requires_recovery());
    assert_eq!(journal.visible_seq(), 0);
}

// ---------------------------------------------------------------------------------------
// NS11 (journal level): no space while preparing the next segment

#[test]
fn ns11_no_space_for_the_next_segment_fails_without_latching() {
    let dir = TempDir::new("ns11");
    let (fs, faults) = fault_fs();
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..small()
    };
    let (journal, _, _) = open(fs.clone(), dir.path(), options.clone(), 0).unwrap();
    faults.enospc_preallocate.store(true, Ordering::SeqCst);
    let mut ok = Vec::new();
    let mut failed = None;
    for i in 0..20 {
        let commit = journal
            .submit(
                batch(b"", format!("k{i}").as_bytes(), &[9; 2000]),
                Durability::Sync,
                Box::new(()),
            )
            .unwrap();
        let seq = commit.seq();
        match commit.wait() {
            Ok(s) => ok.push(s),
            Err(e) => {
                assert_eq!(e, Error::NoSpace);
                failed = Some(seq);
                break;
            }
        }
    }
    let failed = failed.expect("the segment filled up");
    assert!(!journal.requires_recovery());
    faults.enospc_preallocate.store(false, Ordering::SeqCst);
    let after = put(&journal, batch(b"", b"after", &[1; 2000]), Durability::Sync);
    assert!(after > failed);
    ok.push(after);
    drop(journal);
    let (_j, report, sink) = open(fs, dir.path(), options, 0).unwrap();
    assert_eq!(
        sink.seqs(),
        ok,
        "the failed write is gone and its number voided"
    );
    assert_eq!(report.last_seq, after);
}

// ---------------------------------------------------------------------------------------
// Segments: roll, recycle with epochs, locators, replay window, gap fill

#[test]
fn ns_unit_segments_roll_recycle_and_locators() {
    let dir = TempDir::new("segments");
    let options = JournalOptions {
        segment_bytes: 16 << 10,
        ..small()
    };
    let (journal, _, sink) = open(real(), dir.path(), options.clone(), 0).unwrap();
    let mut rng = Rng(42);
    let mut values = Vec::new();
    for i in 0..120u64 {
        let value: Vec<u8> = (0..(100 + rng.below(1500)))
            .map(|_| rng.next() as u8)
            .collect();
        let durability = if i % 7 == 0 {
            Durability::Sync
        } else {
            Durability::NoSync
        };
        put(
            &journal,
            batch(b"log/", format!("{i}").as_bytes(), &value),
            durability,
        );
        values.push(value);
    }
    let segments = journal.segments();
    assert!(segments.len() > 5);
    for pair in segments.windows(2) {
        assert_eq!(pair[1].first_seq, pair[0].last_seq + 1, "{segments:?}");
    }
    let records = sink.records();
    for (r, v) in records.iter().zip(&values) {
        assert_eq!(journal.read_value(&r.locators[0]).unwrap(), &v[..]);
    }
    journal.sync().unwrap().wait().unwrap();
    // Retire the two oldest segments; their locators now fail.
    let retired: Vec<u64> = segments.iter().take(2).map(|s| s.segment).collect();
    for &s in &retired {
        journal.retire(s).unwrap();
    }
    let stale = records
        .iter()
        .find(|r| r.locators[0].segment == retired[0])
        .unwrap();
    assert!(matches!(
        journal.read_value(&stale.locators[0]),
        Err(Error::Corruption {
            file: FileId::JournalValue { .. },
            ..
        })
    ));
    // A flipped value byte is detected.
    let live = records.last().unwrap();
    flip(
        &seg_path(dir.path(), live.locators[0].segment),
        live.locators[0].offset + 3,
    );
    assert!(journal.read_value(&live.locators[0]).is_err());
    assert_eq!(journal.stats().value_corruptions, 2);
    flip(
        &seg_path(dir.path(), live.locators[0].segment),
        live.locators[0].offset + 3,
    );
    // Write on: the retired files come back with a new epoch.
    for i in 0..60u64 {
        put(
            &journal,
            batch(b"log/", format!("m{i}").as_bytes(), &[5; 900]),
            Durability::NoSync,
        );
    }
    let now = journal.segments();
    assert!(now.iter().any(|s| s.epoch == 2), "{now:?}");
    let all = sink.records();
    drop(journal);

    // Reopen with a replay window starting inside the retained segments.
    let first_kept = now[0].first_seq;
    let replay_after = first_kept + 10;
    let (journal, report, replayed) =
        open(real(), dir.path(), options.clone(), replay_after).unwrap();
    assert_eq!(report.tail, TailOutcome::Clean);
    let expected: Vec<_> = all
        .iter()
        .filter(|r| r.first_seq + u64::from(r.count) - 1 > replay_after)
        .cloned()
        .collect();
    assert_eq!(replayed.records(), expected);
    for r in &expected {
        assert!(journal.read_value(&r.locators[0]).is_ok());
    }
    // The same segments (a clean close may have rolled once more), still chained.
    let reopened = journal.segments();
    for (a, b) in now.iter().zip(&reopened) {
        assert_eq!(
            (a.segment, a.epoch, a.first_seq),
            (b.segment, b.epoch, b.first_seq)
        );
    }
    for pair in reopened.windows(2) {
        assert_eq!(pair[1].first_seq, pair[0].last_seq + 1, "{reopened:?}");
    }
    drop(journal);

    // Gap fill: the store says more is flushed than the journal holds.
    let flushed = report.last_seq + 50;
    let (journal, _, _) = open(real(), dir.path(), options.clone(), flushed).unwrap();
    assert_eq!(journal.visible_seq(), flushed);
    let next = put(
        &journal,
        batch(b"log/", b"after-gap", b"v"),
        Durability::Sync,
    );
    assert_eq!(next, flushed + 1);
    drop(journal);
    let (_j, report, replayed) = open(real(), dir.path(), options, report.last_seq).unwrap();
    assert_eq!(
        replayed.seqs(),
        vec![next],
        "the void pad keeps the sequence continuous"
    );
    assert_eq!(report.last_seq, next);
}

#[test]
fn ns_unit_random_crash_points_replay_an_exact_prefix() {
    // Truncates the active segment's written bytes at random points (a crash model where
    // the tail of the last writes never reached the file) and checks that recovery
    // returns an exact prefix that contains every synced record.
    for seed in 1..=12u64 {
        let dir = TempDir::new("crashpoints");
        let mut rng = Rng(seed * 7919);
        let options = JournalOptions {
            segment_bytes: 32 << 10,
            ..small()
        };
        let (journal, _, sink) = open(real(), dir.path(), options.clone(), 0).unwrap();
        let mut last_synced = 0;
        for i in 0..(20 + rng.below(40)) {
            let d = if rng.below(3) == 0 {
                Durability::Sync
            } else {
                Durability::NoSync
            };
            let seq = put(
                &journal,
                batch(
                    b"s/",
                    format!("{i}").as_bytes(),
                    &vec![i as u8; 50 + rng.below(800) as usize],
                ),
                d,
            );
            if d == Durability::Sync {
                last_synced = seq;
            }
        }
        let written = sink.records();
        let active = journal.segments().into_iter().find(|s| s.active).unwrap();
        journal.crash();
        drop(journal);
        // Zero a random suffix of the unsynced bytes in the active segment.
        let unsynced: Vec<&Applied> = written
            .iter()
            .filter(|r| r.first_seq > last_synced)
            .collect();
        let in_active: Vec<&&Applied> = unsynced
            .iter()
            .filter(|r| r.record_at.unwrap().0 == active.segment)
            .collect();
        if let Some(victim) = in_active.get(rng.below(in_active.len().max(1) as u64) as usize) {
            let (seg, at) = victim.record_at.unwrap();
            let cut = at + rng.below(60);
            let path = seg_path(dir.path(), seg);
            let len = std::fs::metadata(&path).unwrap().len();
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            std::os::unix::fs::FileExt::write_all_at(&file, &vec![0u8; (len - cut) as usize], cut)
                .unwrap();
        }
        let (journal, report, replayed) = open(real(), dir.path(), options.clone(), 0).unwrap();
        let got = replayed.records();
        assert_eq!(got, written[..got.len()], "seed {seed}: an exact prefix");
        assert!(
            report.last_seq >= last_synced,
            "seed {seed}: synced writes survive"
        );
        let more = put(&journal, batch(b"s/", b"more", b"m"), Durability::Sync);
        assert_eq!(more, report.last_seq + 1);
        journal.crash();
        drop(journal);
        let (_j, again, replayed) = open(real(), dir.path(), options, 0).unwrap();
        assert_eq!(again.tail, TailOutcome::Clean, "seed {seed}");
        assert_eq!(replayed.records()[..got.len()], got[..]);
        assert_eq!(replayed.seqs().last(), Some(&more));
    }
}

// ---------------------------------------------------------------------------------------
// NS12: replay throughput

#[test]
#[ignore = "throughput measurement; run with --release --ignored --nocapture"]
fn ns12_replay_of_64_mib_journal() {
    let dir = TempDir::new("ns12");
    let options = JournalOptions::default();
    let (journal, _, _) = open(real(), dir.path(), options.clone(), 0).unwrap();
    let value = vec![0xAB; 1024];
    let mut bytes = 0u64;
    let mut i = 0u64;
    let write_start = nabedra_rt::time::now_std();
    let mut pending = Vec::new();
    while bytes < 64 << 20 {
        let mut b = WriteBatch::new();
        b.set_scope(b"g/");
        for j in 0..8 {
            let key = format!("g/{i}/{j}");
            if j % 2 == 0 {
                b.put_journal(key.as_bytes(), &value);
            } else {
                b.put(key.as_bytes(), &value[..100]);
            }
        }
        bytes += b.encoded_bytes() as u64;
        pending.push(journal.submit(b, Durability::NoSync, Box::new(())).unwrap());
        if pending.len() == 256 {
            for c in pending.drain(..) {
                c.wait().unwrap();
            }
        }
        i += 1;
    }
    for c in pending.drain(..) {
        c.wait().unwrap();
    }
    let write_ms = nabedra_rt::time::elapsed(write_start).as_millis();
    drop(journal);

    #[derive(Default)]
    struct Counting(AtomicU64);
    impl JournalSink for Counting {
        fn apply(&self, _: RecordKind, _: Seq, b: &WriteBatch, l: &[Locator]) -> Result<()> {
            assert_eq!(l.len(), b.journal_values().len());
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    let sink = Arc::new(Counting::default());
    let started = nabedra_rt::time::now_std();
    let (_j, report) = Journal::open(real(), dir.path(), options, 0, sink.clone()).unwrap();
    let open_ms = nabedra_rt::time::elapsed(started).as_millis();
    println!(
        "ns12: wrote {} MiB in {write_ms} ms; replayed {} records ({} MiB) in recovery_ms {} (open {open_ms} ms)",
        bytes >> 20,
        report.replayed_records,
        report.replayed_bytes >> 20,
        report.recovery_ms
    );
    assert_eq!(report.replayed_records, i);
    assert_eq!(sink.0.load(Ordering::Relaxed), i);
}

/// NS08: a commit is counted in the stats before its waiter wakes, so a caller that
/// reads the stats right after its commit returns sees it (the pool-level ns08 test
/// counted a commit acknowledged before its `before` read: 385 vs 384 on CI).
#[test]
#[allow(clippy::disallowed_methods)] // short-lived test threads, joined before the test ends
fn ns08_commit_is_counted_before_its_waiter_wakes() {
    let fs = crate::fs::FaultFs::new();
    fs.create_dir_all_durable(Path::new("/db"));
    let (journal, _, _) = open(Arc::new(fs), Path::new("/db/journal"), small(), 0).unwrap();
    let journal = Arc::new(journal);
    let writers: Vec<_> = (0..4u8)
        .map(|t| {
            let journal = journal.clone();
            thread::spawn(move || {
                for i in 0..1500u32 {
                    let before = journal.stats();
                    let durability = if i % 2 == 0 {
                        Durability::Sync
                    } else {
                        Durability::NoSync
                    };
                    let mut b = WriteBatch::new();
                    b.put(&[b'k', t], &i.to_be_bytes());
                    journal
                        .submit(b, durability, Box::new(()))
                        .unwrap()
                        .wait()
                        .unwrap();
                    let after = journal.stats();
                    assert!(after.commits > before.commits, "{before:?} -> {after:?}");
                    if durability == Durability::Sync {
                        assert!(
                            after.synced_commits > before.synced_commits,
                            "{before:?} -> {after:?}"
                        );
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
}

/// Polls `commit` until it completes or `limit` passes.
fn result_within(commit: &mut Commit, limit: std::time::Duration) -> Option<Result<Seq>> {
    let deadline = nabedra_rt::time::now_std() + limit;
    loop {
        if let Some(result) = commit.try_result() {
            return Some(result);
        }
        if nabedra_rt::time::now_std() >= deadline {
            return None;
        }
        nabedra_rt::time::thread_sleep(std::time::Duration::from_millis(1));
    }
}

/// RD3 (O06, O11, NS08): an unsynced write queued ahead of a synced write in the same
/// group becomes visible and completes once the group's bytes are written, before the
/// sync the later write asked for; the synced write completes only after its sync, and
/// `durable_seq` never covers it earlier. The sink still sees every record in sequence
/// order, and the bytes on disk are those of one synced group. Before RD3 the unsynced
/// write waited for the whole sync (a Data apply behind the next Raft append's fsync).
#[test]
fn rd3_unsynced_head_of_a_group_is_visible_before_its_sync() {
    let dir = TempDir::new("rd3-head");
    let (fs, faults) = fault_fs();
    let (journal, _, sink) = open(fs, dir.path(), small(), 0).unwrap();
    let base = faults.syncs.load(Ordering::SeqCst);
    faults.syncs_allowed.store(base, Ordering::SeqCst);
    faults.hold_syncs.store(true, Ordering::SeqCst);
    // A failed assertion must not leave the journal thread in a held sync.
    struct ReleaseSyncs(Arc<Faults>);
    impl Drop for ReleaseSyncs {
        fn drop(&mut self) {
            self.0.hold_syncs.store(false, Ordering::SeqCst);
        }
    }
    let _release = ReleaseSyncs(faults.clone());
    // Group 1: a synced write whose sync is held.
    let mut s0 = journal
        .submit(batch(b"g/", b"s0", b"v0"), Durability::Sync, Box::new(()))
        .unwrap();
    let wait = nabedra_rt::time::now_std() + std::time::Duration::from_secs(10);
    while faults.syncs.load(Ordering::SeqCst) == base {
        assert!(
            nabedra_rt::time::now_std() < wait,
            "the journal reaches the held sync"
        );
        nabedra_rt::time::thread_sleep(std::time::Duration::from_millis(1));
    }
    // Group 2, queued meanwhile: an unsynced write, then a synced one.
    let mut u1 = journal
        .submit(batch(b"g/", b"u1", b"v1"), Durability::NoSync, Box::new(()))
        .unwrap();
    let mut s2 = journal
        .submit(batch(b"g/", b"s2", b"v2"), Durability::Sync, Box::new(()))
        .unwrap();
    let (s0_seq, u1_seq, s2_seq) = (s0.seq(), u1.seq(), s2.seq());
    assert!(s0_seq < u1_seq && u1_seq < s2_seq);
    // Release group 1's sync only.
    faults.syncs_allowed.store(base + 1, Ordering::SeqCst);
    let limit = std::time::Duration::from_secs(5);
    assert_eq!(
        result_within(&mut s0, limit)
            .expect("s0 completes")
            .unwrap(),
        s0_seq
    );
    let u1_result = result_within(&mut u1, limit)
        .expect("the unsynced write completes while the later sync is held");
    assert_eq!(u1_result.unwrap(), u1_seq);
    assert!(journal.visible_seq() >= u1_seq);
    assert!(
        journal.visible_seq() < s2_seq,
        "the synced write is not visible before its sync"
    );
    assert!(
        journal.durable_seq() < s2_seq,
        "durability is not published early"
    );
    assert!(
        s2.try_result().is_none(),
        "the synced write waits for its sync"
    );
    // Release every sync: s2 completes, durable covers it, the sink saw all three in order.
    faults.hold_syncs.store(false, Ordering::SeqCst);
    assert_eq!(
        result_within(&mut s2, limit)
            .expect("s2 completes")
            .unwrap(),
        s2_seq
    );
    assert!(journal.durable_seq() >= s2_seq);
    let applied: Vec<Seq> = sink
        .applied
        .lock()
        .unwrap()
        .iter()
        .map(|a| a.first_seq)
        .collect();
    assert_eq!(applied, vec![s0_seq, u1_seq, s2_seq]);
    // The same bytes as before: one record each, readable after a reopen.
    journal.close().unwrap();
    drop(journal);
    let (_journal, _, replayed) = open(real(), dir.path(), small(), 0).unwrap();
    let replayed: Vec<Seq> = replayed
        .applied
        .lock()
        .unwrap()
        .iter()
        .map(|a| a.first_seq)
        .collect();
    assert_eq!(replayed, vec![s0_seq, u1_seq, s2_seq]);
}
