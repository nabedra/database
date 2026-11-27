//! The journal park (risk R1 of the phase 9 design): a group that parks before a sink
//! call that would stall resumes at that record, so the sink sees every record once and
//! in order, and waiters see the same visible and durable sequences as when the journal
//! thread blocks inside the sink. One script runs twice: parked, as stepped services in a
//! simulated world, and blocking, on the product threads (the `sim` build outside a world
//! behaves as product). Simulation evidence is a fault model.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use nabedra_rt::sim::{self, NodeClock, SimEvent, StepOutcome};
use nabedra_rt::time;

use super::*;
use crate::fs::{FaultFs, Fs};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Gates {
    held: BTreeSet<Seq>,
    reached: BTreeSet<Seq>,
    parked_on: Vec<Seq>,
}

/// The controller holds a record until it explicitly releases it. A product thread
/// waits inside apply; the simulated journal parks before that call.
#[derive(Default)]
struct StallSink {
    gates: Mutex<Gates>,
    changed: Condvar,
    applied: Mutex<Vec<Seq>>,
}

impl StallSink {
    fn hold(&self, seq: Seq) {
        assert!(lock(&self.gates).held.insert(seq));
    }

    fn release(&self, seq: Seq) {
        assert!(lock(&self.gates).held.remove(&seq));
        self.changed.notify_all();
    }

    fn wait_reached(&self, seq: Seq) {
        let reached = || lock(&self.gates).reached.contains(&seq).then_some(());
        if task::drive_until("test: the journal reaches a stall", reached).is_none() {
            let (gates, _) = self
                .changed
                .wait_timeout_while(lock(&self.gates), TEST_TIMEOUT, |gates| {
                    !gates.reached.contains(&seq)
                })
                .unwrap();
            assert!(gates.reached.contains(&seq), "journal never reached {seq}");
        }
    }
}

impl JournalSink for StallSink {
    fn apply(&self, _: RecordKind, first_seq: Seq, _: &WriteBatch, _: &[Locator]) -> Result<()> {
        let mut gates = lock(&self.gates);
        if gates.held.contains(&first_seq) {
            assert!(sim::world().is_none(), "a stepped apply bypassed its park");
            gates.reached.insert(first_seq);
            self.changed.notify_all();
            let (released, _) = self
                .changed
                .wait_timeout_while(gates, TEST_TIMEOUT, |gates| gates.held.contains(&first_seq))
                .unwrap();
            assert!(
                !released.held.contains(&first_seq),
                "test did not release {first_seq}"
            );
        }
        lock(&self.applied).push(first_seq);
        Ok(())
    }

    fn stall_deadline(&self, _: RecordKind, first_seq: Seq, _: &WriteBatch) -> Option<Instant> {
        let mut gates = lock(&self.gates);
        if !gates.held.contains(&first_seq) {
            return None;
        }
        if gates.reached.insert(first_seq) {
            gates.parked_on.push(first_seq);
        }
        Some(time::now_std() + Duration::from_millis(1))
    }
}

/// What the script saw: the sink's apply order, each commit's result, and the visible
/// and durable sequences after each wait.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    applied: Vec<Seq>,
    results: Vec<Seq>,
    after_waits: Vec<(Seq, Seq)>,
}

/// Sequences: w0 = 1, w1 = 2, w2 = 3, w3 = 4, w4 = 5, w5 = 6, w6 = 7.
///
/// Round 1: w0 stalls alone in its group. Meanwhile {w1, w2, w3} queue, w1 and w2
/// unsynced and w3 synced, and w2 stalls while that group's unsynced head is published,
/// before its sync (the `Head` stage). Round 2: w4 stalls alone, {w5, w6} queue
/// unsynced, and w6 stalls while that group is published (the `Publish` stage). Each
/// later group is submitted only once the journal reached the stall before it, so the
/// blocking threads take the same groups as the stepped run.
fn script() -> (Seen, Vec<Seq>) {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/j"));
    let fs: Arc<dyn Fs> = Arc::new(fs);
    let sink = Arc::new(StallSink::default());
    let (journal, _) = Journal::open(
        fs,
        Path::new("/j/journal"),
        journal_options(),
        0,
        sink.clone(),
    )
    .unwrap();
    let submit = |key: &[u8], durability| {
        let mut batch = WriteBatch::new();
        batch.put(key, &[7; 300]);
        journal.submit(batch, durability, Box::new(())).unwrap()
    };
    let mut results = Vec::new();
    let mut after_waits = Vec::new();
    let mut wait = |commit: Commit| {
        results.push(commit.wait().unwrap());
        after_waits.push((journal.visible_seq(), journal.durable_seq()));
    };

    sink.hold(1);
    sink.hold(3);
    let w0 = submit(b"w0", Durability::NoSync);
    sink.wait_reached(1);
    let w1 = submit(b"w1", Durability::NoSync);
    let w2 = submit(b"w2", Durability::NoSync);
    let w3 = submit(b"w3", Durability::Sync);
    sink.release(1);
    wait(w0);
    sink.wait_reached(3);
    sink.release(3);
    wait(w3);
    wait(w1);
    wait(w2);

    sink.hold(5);
    sink.hold(7);
    let w4 = submit(b"w4", Durability::NoSync);
    sink.wait_reached(5);
    let w5 = submit(b"w5", Durability::NoSync);
    let w6 = submit(b"w6", Durability::NoSync);
    sink.release(5);
    sink.wait_reached(7);
    sink.release(7);
    wait(w6);
    wait(w4);
    wait(w5);

    journal.close().unwrap();
    drop(journal);
    let seen = Seen {
        applied: lock(&sink.applied).clone(),
        results,
        after_waits,
    };
    let parked_on = lock(&sink.gates).parked_on.clone();
    (seen, parked_on)
}

fn expected() -> Seen {
    Seen {
        applied: vec![1, 2, 3, 4, 5, 6, 7],
        results: vec![1, 4, 2, 3, 7, 5, 6],
        // After w0 only its own group is visible, and nothing is durable. After w3 its
        // group synced. Round 2 never syncs.
        after_waits: vec![(1, 0), (4, 4), (4, 4), (4, 4), (7, 4), (7, 4), (7, 4)],
    }
}

/// R1: the parked run (stepped services in a simulated world) applies every record once
/// and in order, completes each commit with its sequence, and shows the same visible and
/// durable sequences after each wait as the blocking run on product threads. It parks on
/// w0 and w4 (alone in their groups), on w2 (the `Head` stage) and on w6 (the `Publish`
/// stage). A resume one record early or late changes `applied`.
#[test]
fn r1_parked_and_blocking_journal_runs_publish_the_same_sequences() {
    let parks = Arc::new(AtomicU64::new(0));
    let counted = parks.clone();
    let (parked, parked_on) = sim::run(9, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            if let SimEvent::Step {
                service: "kv-journal",
                outcome: StepOutcome::Idle(Some(_)),
                ..
            } = event
            {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        }));
        sim::in_node(1, async { script() }).await
    });
    assert_eq!(parked_on, vec![1, 3, 5, 7], "where the parked run parked");
    assert!(
        parks.load(Ordering::SeqCst) >= 4,
        "kv-journal parks as timed idles: {}",
        parks.load(Ordering::SeqCst)
    );
    assert_eq!(parked, expected(), "parked run");

    let (blocking, parked_on) = script();
    assert!(parked_on.is_empty(), "the product thread never parks");
    assert_eq!(blocking, parked, "blocking run against parked run");
}

fn journal_options() -> JournalOptions {
    JournalOptions {
        segment_bytes: 64 << 10,
        max_group_bytes: 1 << 20,
        zero_fill: true,
        key: JournalOptions::default().key,
    }
}

fn batch_for(key: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.put(key, b"value");
    batch
}

struct CountDrop(Arc<AtomicU64>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn ns04_ns05_ns17_node_cut_resolves_parked_and_queued_journal_items() {
    let dir = Path::new("/j/journal");
    for power_loss in [false, true] {
        sim::run(23, |world| async move {
            world.add_node(1, NodeClock::default());
            let image = sim::in_node(1, async {
                let fs = FaultFs::new();
                fs.create_dir_all_durable(Path::new("/j"));
                let sink = Arc::new(StallSink::default());
                let (journal, _) = Journal::open(
                    Arc::new(fs.clone()),
                    dir,
                    journal_options(),
                    0,
                    sink.clone(),
                )
                .unwrap();
                assert_eq!(
                    journal
                        .submit(batch_for(b"acknowledged"), Durability::Sync, Box::new(()))
                        .unwrap()
                        .wait()
                        .unwrap(),
                    1
                );

                let dropped = Arc::new(AtomicU64::new(0));
                let submit = |key| {
                    journal
                        .submit(
                            batch_for(key),
                            Durability::NoSync,
                            Box::new(CountDrop(dropped.clone())),
                        )
                        .unwrap()
                };
                sink.hold(2);
                let mut parked = submit(b"parked");
                sink.wait_reached(2);
                let mut queued = submit(b"queued");
                let mut sync = journal.sync().unwrap();
                assert_eq!(journal.visible_seq(), 1);
                assert_eq!(journal.durable_seq(), 1);
                assert_eq!(dropped.load(Ordering::SeqCst), 0);

                let image = if power_loss {
                    fs.power_loss(23)
                } else {
                    fs.crash()
                };
                fs.halt();
                fs.clear_calls();
                world.crash(1);

                for commit in [&mut parked, &mut queued, &mut sync] {
                    let result = commit.try_result();
                    assert!(
                        matches!(result, Some(Err(Error::RecoveryRequired))),
                        "node cut left commit {} with {result:?}",
                        commit.seq()
                    );
                }
                assert_eq!(dropped.load(Ordering::SeqCst), 2);
                assert_eq!(journal.queued(), (0, 0));
                assert_eq!(journal.visible_seq(), 1);
                assert_eq!(journal.durable_seq(), 1);
                assert!(matches!(journal.close(), Err(Error::RecoveryRequired)));
                drop(journal);
                assert!(fs.calls().is_empty(), "journal attempted I/O after the cut");

                image
            })
            .await;

            world.restart(1);
            sim::in_node(1, async {
                let mut recovered = None;
                for _ in 0..2 {
                    let sink = Arc::new(StallSink::default());
                    let (journal, report) = Journal::open(
                        Arc::new(image.clone()),
                        dir,
                        journal_options(),
                        0,
                        sink.clone(),
                    )
                    .unwrap();
                    let applied = lock(&sink.applied).clone();
                    let max = if power_loss { 2 } else { 1 };
                    assert!((1..=max).contains(&report.last_seq));
                    assert_eq!(applied, (1..=report.last_seq).collect::<Vec<_>>());
                    if let Some(previous) = recovered.replace(applied.clone()) {
                        assert_eq!(applied, previous, "reopening changed the prefix");
                    }
                    journal.close().unwrap();
                }
            })
            .await;
        });
    }
}

#[test]
fn ns17_node_cut_releases_the_last_store_guard_in_an_unstarted_journal() {
    use crate::{Background, BlockCache, MemtableBudget, Options, Store};

    sim::run(17, |world| async move {
        world.add_node(1, NodeClock::default());
        sim::in_node(1, async {
            let fs = FaultFs::new();
            fs.create_dir_all_durable(Path::new("/db"));
            let mut options = Options::new(
                Arc::new(fs.clone()),
                Arc::new(BlockCache::new(1 << 20)),
                Arc::new(MemtableBudget::new(1 << 20)),
                Background::new(2).unwrap(),
            );
            options.journal = journal_options();
            let store = Arc::new(Store::open(Path::new("/db/store"), options).unwrap().store);
            let weak = Arc::downgrade(&store);
            let mut pending = store
                .submit(
                    batch_for(b"queued"),
                    Durability::Sync,
                    Box::new(store.clone()),
                )
                .unwrap();
            assert_eq!(world.service_steps(), 0);
            drop(store);
            assert_eq!(weak.strong_count(), 1);

            fs.halt();
            fs.clear_calls();
            world.crash(1);
            let result = pending.try_result();
            assert!(
                matches!(result, Some(Err(Error::RecoveryRequired))),
                "node cut left the queued commit with {result:?}"
            );
            assert!(
                weak.upgrade().is_none(),
                "the queued guard retained its store"
            );
            assert!(
                fs.calls().is_empty(),
                "store teardown attempted I/O after the cut"
            );
        })
        .await;
    });
}
