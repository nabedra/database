use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::{Cut, NodeClock, SimEvent, SplitMix64, World, in_node, run};
use super::{ClockRead, Side, StoragePause};
use crate::fs::{self, FsFile, FsOp, OpenMode};
use crate::sync::Condvar;
use crate::task::{self, Step, Stepped};
use crate::time;

const DATA: &str = "/sim/n1/data";
const LOG: &str = "/sim/n1/data/log";

fn us(value: u64) -> Duration {
    Duration::from_micros(value)
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Node 1 with a durable, empty log file.
async fn node_with_log(world: &Arc<World>) -> Arc<dyn FsFile> {
    world.add_node(1, NodeClock::default());
    world.mount(1, DATA);
    in_node(1, async {
        let file = fs::real()
            .open(Path::new(LOG), OpenMode::CreateNew)
            .unwrap();
        file.sync_all().unwrap();
        fs::real().sync_dir(Path::new(DATA)).unwrap();
        file
    })
    .await
}

/// The records a crash of node 1 leaves durable.
fn durable_after_crash(world: &Arc<World>) -> u64 {
    world.crash(1);
    world.restart_with(1, Cut::Crash);
    let volume = world.volume(1, Path::new(DATA)).unwrap();
    volume.contents(Path::new(LOG)).unwrap().len() as u64
}

/// A commit log of one-byte records. `failed` is set, as an engine poisons its log, when a
/// write or a sync fails.
#[derive(Default)]
struct Log {
    written: AtomicU64,
    synced: AtomicU64,
    failed: AtomicBool,
}

impl Log {
    /// Writes the next record and syncs it.
    fn commit(&self, file: &dyn FsFile) {
        let next = self.written.load(SeqCst) + 1;
        if file.write_all_at(&[1], next - 1).is_err() {
            return self.failed.store(true, SeqCst);
        }
        self.written.store(next, SeqCst);
        match file.sync_data() {
            Ok(()) => self.synced.store(next, SeqCst),
            Err(_) => self.failed.store(true, SeqCst),
        }
    }
}

/// Commits a record per doorbell, in one step, as nabedra-kv's journal does.
struct Journal {
    asked: Arc<(Condvar, AtomicU64)>,
    bell: Condvar,
    log: Arc<Log>,
    file: Arc<dyn FsFile>,
}

impl Stepped for Journal {
    fn step(&mut self) -> Step {
        let asked = self.asked.1.load(SeqCst);
        if asked <= self.log.written.load(SeqCst) || self.log.failed.load(SeqCst) {
            return Step::Idle(Some(time::now_std() + us(200)));
        }
        self.log.commit(&*self.file);
        Step::Worked
    }
    fn doorbell(&self) -> &Condvar {
        &self.bell
    }
    fn run(self) {}
}

/// Who makes the write and its sync.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Writer {
    Job,
    Service,
}

/// How a run looks for trouble.
#[derive(Clone, Copy, Debug)]
enum Search {
    /// Crashes at random times.
    Crashes,
    /// The same, with the volume halted at the n-th mutating call.
    Halt(u64),
    /// Crashes at random times with seeded storage pauses.
    Pauses,
}

/// One run; whether a record a client saw acknowledged is gone after the crash. Another
/// task acknowledges: the planted one what is written, its clean twin what is synced.
fn loses_an_ack(seed: u64, writer: Writer, planted: bool, search: Search) -> bool {
    run(seed, move |world| async move {
        let file = node_with_log(&world).await;
        let volume = world.volume(1, Path::new(DATA)).unwrap();
        match search {
            Search::Crashes => {}
            Search::Halt(calls) => {
                world.crash_volume_after(1, Path::new(DATA), volume.mutating_ops() + calls)
            }
            Search::Pauses => world.set_storage_pauses(500, ms(2)),
        }
        let log = Arc::new(Log::default());
        let asked = Arc::new((Condvar::new(), AtomicU64::new(0)));
        match writer {
            Writer::Job => {
                let (log, file) = (log.clone(), file.clone());
                drop(world.spawn_node(1, async move {
                    loop {
                        time::sleep(ms(1)).await;
                        let (log, file) = (log.clone(), file.clone());
                        task::spawn_blocking(move || log.commit(&*file))
                            .await
                            .unwrap();
                    }
                }));
            }
            Writer::Service => {
                let service = Journal {
                    asked: asked.clone(),
                    bell: Condvar::new(),
                    log: log.clone(),
                    file: file.clone(),
                };
                let asked = asked.clone();
                drop(world.spawn_node(1, async move {
                    task::spawn_stepped("journal", 64 * 1024, service).unwrap();
                    loop {
                        time::sleep(ms(1)).await;
                        asked.1.fetch_add(1, SeqCst);
                        asked.0.notify_all();
                    }
                }));
            }
        }
        let client = Arc::new(AtomicU64::new(0));
        let acked = client.clone();
        world.spawn_node(1, async move {
            loop {
                time::sleep(us(100)).await;
                let from = if planted { &log.written } else { &log.synced };
                if !log.failed.load(SeqCst) {
                    acked.fetch_max(from.load(SeqCst), SeqCst);
                }
            }
        });
        let mut rng = SplitMix64::new(seed);
        tokio::time::sleep(us(rng.below(8_000))).await;
        let seen = client.load(SeqCst);
        seen > durable_after_crash(&world)
    })
}

/// Finding H-01: an acknowledgement another task sends while the fsync is in flight. A job
/// was atomic, so by the time that task ran, the fsync had succeeded or the log was
/// poisoned, at every crash time and at every halt inside the job.
#[test]
fn storage_pauses_expose_an_ack_before_its_fsync() {
    for writer in [Writer::Job, Writer::Service] {
        let count = |planted, search: &dyn Fn(u64) -> Search, seeds: u64| {
            (0..seeds)
                .filter(|&seed| loses_an_ack(seed, writer, planted, search(seed)))
                .count()
        };
        assert_eq!(count(true, &|_| Search::Crashes, 200), 0, "{writer:?}");
        assert_eq!(
            count(true, &|seed| Search::Halt(seed % 12), 240),
            0,
            "{writer:?}"
        );
        let caught = count(true, &|_| Search::Pauses, 100);
        println!("{writer:?}: the planted ack is caught on {caught} of 100 seeds");
        assert!(caught > 5, "{writer:?}");
        assert_eq!(
            count(false, &|_| Search::Pauses, 400),
            0,
            "{writer:?}: the clean twin"
        );
    }
}

/// One run of a log whose records one job writes and another syncs. The planted syncer
/// publishes what is written once the sync returned; its clean twin what was written when
/// the sync began.
fn publishes_too_much(seed: u64, planted: bool, pauses: bool) -> bool {
    run(seed, move |world| async move {
        let file = node_with_log(&world).await;
        if pauses {
            world.set_storage_pauses(500, ms(3));
        }
        let log = Arc::new(Log::default());
        let (writes, output) = (log.clone(), file.clone());
        world.spawn_node(1, async move {
            loop {
                time::sleep(us(700)).await;
                let (writes, output) = (writes.clone(), output.clone());
                task::spawn_blocking(move || {
                    let next = writes.written.load(SeqCst) + 1;
                    output.write_all_at(&[1], next - 1).unwrap();
                    writes.written.store(next, SeqCst);
                })
                .await
                .unwrap();
            }
        });
        let syncs = log.clone();
        world.spawn_node(1, async move {
            loop {
                time::sleep(ms(1)).await;
                let (syncs, file) = (syncs.clone(), file.clone());
                task::spawn_blocking(move || {
                    let began = syncs.written.load(SeqCst);
                    file.sync_data().unwrap();
                    let ended = syncs.written.load(SeqCst);
                    syncs
                        .synced
                        .store(if planted { ended } else { began }, SeqCst);
                })
                .await
                .unwrap();
            }
        });
        let mut rng = SplitMix64::new(seed);
        tokio::time::sleep(us(rng.below(20_000))).await;
        let seen = log.synced.load(SeqCst);
        seen > durable_after_crash(&world)
    })
}

/// Finding H-01, the other side of the sync: work that slips in between the fsync and the
/// acknowledgement it allows.
#[test]
fn storage_pauses_expose_an_ack_wider_than_its_fsync() {
    let count = |planted, pauses, seeds: u64| {
        (0..seeds)
            .filter(|&seed| publishes_too_much(seed, planted, pauses))
            .count()
    };
    assert_eq!(count(true, false, 200), 0, "unreachable without pauses");
    let caught = count(true, true, 100);
    println!("the planted publication is caught on {caught} of 100 seeds");
    assert!(caught > 5);
    assert_eq!(count(false, true, 400), 0, "the clean twin");
}

/// Sets its flag when it drops.
struct OnDrop(Arc<AtomicBool>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

/// A crash inside a job, at a chosen side of its sync: the disk keeps exactly what the
/// sync covered, no later line of the job runs, and its destructors do.
#[test]
fn a_crash_lands_at_a_chosen_call() {
    for (pause, survives) in [
        (StoragePause::before(FsOp::SyncData), false),
        (StoragePause::after(FsOp::SyncData), true),
        (StoragePause::after(FsOp::Write).path_prefix(LOG), false),
        (StoragePause::before(FsOp::SyncData).nth(2), true),
    ] {
        run(3, move |world| async move {
            let file = node_with_log(&world).await;
            world.pause_storage(1, pause.clone());
            let (acked, dropped) = (Arc::new(AtomicBool::new(false)), OnDrop(Arc::default()));
            let (ack, unwound) = (acked.clone(), dropped.0.clone());
            let job = world.spawn_node(1, async move {
                task::spawn_blocking(move || {
                    let _dropped = dropped;
                    for at in 0..2 {
                        file.write_all_at(&[1], at).unwrap();
                        file.sync_data().unwrap();
                    }
                    ack.store(true, SeqCst);
                })
                .await
            });
            tokio::time::timeout(ms(50), world.storage_paused(1))
                .await
                .unwrap();
            let paused = world.paused_storage(1);
            assert_eq!(paused.len(), 1);
            assert_eq!(paused[0].path, Path::new(LOG));
            // The rest of the cluster and of the node runs on while the call is stopped.
            world
                .spawn_node(1, async { time::sleep(ms(5)).await })
                .await
                .unwrap();
            assert!(!unwound.load(SeqCst));
            world.crash(1);
            assert!(unwound.load(SeqCst), "the crash unwinds the job at once");
            assert!(world.paused_storage(1).is_empty());
            assert!(job.await.unwrap_err().is_cancelled());
            world.restart_with(1, Cut::Crash);
            let volume = world.volume(1, Path::new(DATA)).unwrap();
            let survived = !volume.contents(Path::new(LOG)).unwrap().is_empty();
            assert_eq!(survived, survives, "{pause:?}");
            tokio::time::sleep(ms(50)).await;
            assert!(!acked.load(SeqCst), "the job went on after the crash");
        });
    }
}

#[test]
fn a_paused_job_goes_on_after_its_hold_or_when_resumed() {
    run(4, |world| async move {
        let file = node_with_log(&world).await;
        let sync = |file: &Arc<dyn FsFile>| {
            let file = file.clone();
            world.spawn_node(1, async move {
                let started = time::now();
                task::spawn_blocking(move || file.sync_data().unwrap())
                    .await
                    .unwrap();
                time::now() - started
            })
        };
        world.pause_storage(1, StoragePause::before(FsOp::SyncData).hold(ms(7)));
        assert_eq!(sync(&file).await.unwrap(), ms(7));
        world.pause_storage(1, StoragePause::after(FsOp::SyncData));
        let job = sync(&file);
        tokio::time::sleep(ms(30)).await;
        assert_eq!(
            world.paused_storage(1)[0].side,
            Side::After,
            "no hold: it waits to be resumed"
        );
        world.resume_storage(1);
        assert!(job.await.unwrap() >= ms(30));
        // The rules were used up.
        assert_eq!(sync(&file).await.unwrap(), Duration::ZERO);
    });
}

/// Code that waits for a service inline finds it mid-step in a suspended job, and lets
/// that job finish, as a real waiter would.
#[test]
fn an_inline_wait_resumes_the_job_it_needs() {
    run(5, |world| async move {
        let file = node_with_log(&world).await;
        world.pause_storage(1, StoragePause::before(FsOp::SyncData));
        let log = Arc::new(Log::default());
        let asked = Arc::new((Condvar::new(), AtomicU64::new(1)));
        let service = Journal {
            asked,
            bell: Condvar::new(),
            log: log.clone(),
            file,
        };
        let node = world.clone();
        world
            .spawn_node(1, async move {
                let handle = task::spawn_stepped("journal", 64 * 1024, service).unwrap();
                node.storage_paused(1).await;
                assert_eq!(node.stepped_services(1).len(), 1);
                assert!(!handle.is_current(), "the suspended step is on no stack");
                let waiter = task::spawn_blocking(move || {
                    task::drive_until("test", || (log.synced.load(SeqCst) == 1).then_some(()))
                        .unwrap();
                });
                waiter.await.unwrap();
            })
            .await
            .unwrap();
    });
}

#[test]
fn an_aborted_join_handle_does_not_stop_the_job() {
    run(6, |world| async move {
        let file = node_with_log(&world).await;
        world.pause_storage(1, StoragePause::before(FsOp::SyncData));
        let done = Arc::new(AtomicBool::new(false));
        let flag = done.clone();
        let node = world.clone();
        world
            .spawn_node(1, async move {
                let job = task::spawn_blocking(move || {
                    file.sync_data().unwrap();
                    flag.store(true, SeqCst);
                });
                node.storage_paused(1).await;
                job.abort();
                assert!(job.await.unwrap_err().is_cancelled());
            })
            .await
            .unwrap();
        assert!(done.load(SeqCst));
    });
}

/// Events of one run with seeded pauses, jitter and crashes.
fn trace(seed: u64) -> Vec<String> {
    run(seed, |world| async move {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            sink.lock().unwrap().push(format!("{event:?}"));
        }));
        let file = node_with_log(&world).await;
        world.set_storage_pauses(400, ms(2));
        world.set_jitter(200, ms(1));
        let log = Arc::new(Log::default());
        let asked = Arc::new((Condvar::new(), AtomicU64::new(0)));
        let service = Journal {
            asked: asked.clone(),
            bell: Condvar::new(),
            log: log.clone(),
            file: file.clone(),
        };
        world.spawn_node(1, async move {
            task::spawn_stepped("journal", 64 * 1024, service).unwrap();
            loop {
                time::sleep(us(900)).await;
                asked.1.fetch_add(1, SeqCst);
                let (file, log) = (file.clone(), log.clone());
                drop(task::spawn_blocking(move || {
                    file.sync_data().map(|()| log.synced.load(SeqCst))
                }));
            }
        });
        tokio::time::sleep(ms(20)).await;
        world.crash(1);
        tokio::time::sleep(ms(5)).await;
        events.lock().unwrap().clone()
    })
}

#[test]
fn same_seed_same_trace_with_pauses() {
    for seed in 0..20 {
        let first = trace(seed);
        assert!(first.iter().any(|line| line.starts_with("StoragePaused")));
        assert_eq!(first, trace(seed));
    }
    assert_ne!(trace(1), trace(2));
}

/// A run that ends with jobs still suspended ends, and unwinds them.
#[test]
fn a_run_ends_with_a_job_suspended() {
    let unwound = Arc::new(AtomicBool::new(false));
    let flag = unwound.clone();
    run(7, |world| async move {
        let file = node_with_log(&world).await;
        world.pause_storage(1, StoragePause::before(FsOp::SyncData));
        drop(world.spawn_node(1, async move {
            task::spawn_blocking(move || {
                let _dropped = OnDrop(flag);
                file.sync_data().unwrap();
            })
            .await
        }));
        world.storage_paused(1).await;
    });
    assert!(unwound.load(SeqCst));
}

/// A suspended job that holds a lock another one needs fails the run, not the CI job.
#[test]
fn a_lock_held_across_a_pause_fails_the_run() {
    let result = std::panic::catch_unwind(|| {
        run(8, |world| async move {
            let file = node_with_log(&world).await;
            world.pause_storage(1, StoragePause::before(FsOp::SyncData));
            let lock = Arc::new(Mutex::new(()));
            let other = lock.clone();
            let node = world.clone();
            world
                .spawn_node(1, async move {
                    let first = task::spawn_blocking(move || {
                        let _held = lock.lock().unwrap();
                        file.sync_data().unwrap();
                    });
                    node.storage_paused(1).await;
                    let second = task::spawn_blocking(move || drop(other.lock()));
                    let _ = (first.await, second.await);
                })
                .await
                .unwrap();
        });
    });
    let panic = result.unwrap_err();
    let text = panic.downcast_ref::<String>().unwrap();
    assert!(text.contains("holds a lock it needs"), "{text}");
}

/// A store that two nodes write. It serves one lease holder at a time, by epoch, and the
/// clean one refuses a write from an earlier holder.
#[derive(Default)]
struct Store {
    epoch: AtomicU64,
    fenced: bool,
    stale_writes: AtomicU64,
}

impl Store {
    fn write(&self, epoch: u64) {
        if epoch < self.epoch.load(SeqCst) && !self.fenced {
            self.stale_writes.fetch_add(1, SeqCst);
        }
    }
}

/// How a run stops node 1.
#[derive(Clone, Copy, Debug)]
enum Stops {
    /// At random times, between polls.
    Freezes,
    /// Between a clock read and its use.
    ClockFreezes,
}

/// One run; whether the first holder's write took effect after the second took over. The
/// holder reads its exact clock, finds its lease valid, and writes, with no await between.
/// The planted store trusts that; its clean twin checks the holder's epoch.
fn writes_after_its_lease(seed: u64, planted: bool, stops: Stops) -> bool {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        let lease = ms(50);
        let end = time::now() + lease;
        let store = Arc::new(Store {
            fenced: !planted,
            epoch: AtomicU64::new(1),
            ..Store::default()
        });
        if let Stops::ClockFreezes = stops {
            world.set_clock_freezes(50, ms(40));
        }
        let holder = store.clone();
        world.spawn_node(1, async move {
            loop {
                time::sleep(us(900)).await;
                if time::now() < end {
                    holder.write(1);
                }
            }
        });
        let successor = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(lease + ms(1)).await;
            successor.epoch.store(2, SeqCst);
        });
        let mut rng = SplitMix64::new(seed);
        for _ in 0..4 {
            let before = us(rng.below(25_000));
            let frozen = us(rng.below(25_000));
            tokio::time::sleep(before).await;
            if let Stops::Freezes = stops {
                world.freeze(1);
            }
            tokio::time::sleep(frozen).await;
            if let Stops::Freezes = stops {
                world.thaw(1);
            }
        }
        tokio::time::sleep(ms(50)).await;
        store.stale_writes.load(SeqCst) > 0
    })
}

/// Finding H-04: a freeze acted between polls only, so a node that read its clock and
/// found its lease valid always used that at once.
#[test]
fn clock_freezes_expose_a_stale_lease_check() {
    let count = |planted, stops, seeds: u64| {
        (0..seeds)
            .filter(|&seed| writes_after_its_lease(seed, planted, stops))
            .count()
    };
    assert_eq!(count(true, Stops::Freezes, 300), 0, "unreachable without");
    let caught = count(true, Stops::ClockFreezes, 100);
    println!("the planted lease check is caught on {caught} of 100 seeds");
    assert!(caught > 5);
    assert_eq!(count(false, Stops::ClockFreezes, 300), 0, "the clean twin");
}

/// The freeze lands right after the chosen read: the code goes on at the thaw with the
/// value it read, and nothing of its node runs meanwhile.
#[test]
fn a_freeze_lands_after_a_chosen_clock_read() {
    run(9, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        let (stage, ticks) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let other = ticks.clone();
        world.spawn_node(1, async move {
            loop {
                time::sleep(ms(1)).await;
                other.fetch_add(1, SeqCst);
            }
        });
        world.freeze_after_clock_read(1, ClockRead::nth(2).in_file("sim/suspend/tests.rs"));
        let at = stage.clone();
        let reads = world.spawn_node(1, async move {
            time::sleep(ms(5)).await;
            let first = time::now();
            at.store(1, SeqCst);
            let second = time::unix_ms();
            at.store(2, SeqCst);
            (first, second, time::now())
        });
        tokio::time::timeout(ms(20), world.frozen_at_read(1))
            .await
            .unwrap();
        assert!(world.is_frozen(1));
        assert_eq!(
            stage.load(SeqCst),
            1,
            "frozen between the second read and its use"
        );
        let before = ticks.load(SeqCst);
        world
            .spawn_node(2, async { time::sleep(ms(30)).await })
            .await
            .unwrap();
        assert_eq!((stage.load(SeqCst), ticks.load(SeqCst)), (1, before));
        world.thaw(1);
        let (first, second, third) = reads.await.unwrap();
        let first = first.duration_since(world.start());
        assert_eq!(second, super::super::EPOCH_MS + first.as_millis() as u64);
        assert_eq!(third.duration_since(world.start()), first + ms(30));
        assert!(!world.is_frozen_at_read(1));
        tokio::time::sleep(ms(5)).await;
        assert!(ticks.load(SeqCst) > before);
    });
}

/// A job reads its clock too, and a clock of another crate reports its reads.
#[test]
fn a_freeze_lands_in_a_job_and_after_a_reported_read() {
    run(10, |world| async move {
        world.add_node(1, NodeClock::default());
        world.insert_ext(7_u64);
        world.freeze_after_clock_read(1, ClockRead::nth(1));
        let node = world.clone();
        let job = world.spawn_node(1, async move {
            task::spawn_blocking(move || {
                // Inside an extension slot a freeze would leave it empty.
                let value = node.with_ext::<u64, _>(|value| {
                    super::super::clock_read();
                    *value
                });
                assert!(!node.is_frozen(1));
                super::super::clock_read();
                value
            })
            .await
        });
        world.frozen_at_read(1).await;
        assert_eq!(world.with_ext::<u64, _>(|value| *value), Some(7));
        tokio::time::sleep(ms(3)).await;
        assert!(!job.is_finished());
        world.thaw(1);
        assert_eq!(job.await.unwrap().unwrap(), Some(7));
    });
}

/// A crash during the freeze is the end of the poll: no later line runs, and destructors
/// do. The next incarnation starts clean.
#[test]
fn a_crash_lands_between_a_clock_read_and_its_use() {
    run(11, |world| async move {
        world.add_node(1, NodeClock::default());
        world.freeze_after_clock_read(1, ClockRead::nth(1));
        let (used, dropped) = (Arc::new(AtomicBool::new(false)), OnDrop(Arc::default()));
        let (flag, unwound) = (used.clone(), dropped.0.clone());
        let task = world.spawn_node(1, async move {
            let _dropped = dropped;
            time::now();
            flag.store(true, SeqCst);
        });
        world.frozen_at_read(1).await;
        world.crash(1);
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(unwound.load(SeqCst) && !used.load(SeqCst));
        world.restart(1);
        assert!(!world.is_frozen(1));
        let reads = world.spawn_node(1, async { time::now() });
        assert_eq!(reads.await.unwrap(), world.start());
        assert!(!used.load(SeqCst));
    });
}

/// A run that ends with a poll suspended ends, and drops the task.
#[test]
fn a_run_ends_with_a_poll_suspended() {
    let dropped = OnDrop(Arc::default());
    let flag = dropped.0.clone();
    run(12, |world| async move {
        world.add_node(1, NodeClock::default());
        world.freeze_after_clock_read(1, ClockRead::nth(1));
        drop(world.spawn_node(1, async move {
            let _dropped = dropped;
            time::now();
        }));
        world.frozen_at_read(1).await;
    });
    assert!(flag.load(SeqCst));
}

/// Polls on a helper thread behave as polls: they yield, spawn, sleep, block, race
/// branches, panic and cancel. The trace lists what each step saw.
fn polls(seed: u64) -> Vec<String> {
    run(seed, |world| async move {
        world.add_node(1, NodeClock::default());
        world.set_clock_freezes(300, ms(3));
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for task in 0..4_u64 {
            let log = log.clone();
            tasks.push(world.spawn_node(1, async move {
                for round in 0..10 {
                    time::sleep(us(300 + 100 * task)).await;
                    let read = time::now();
                    tokio::task::yield_now().await;
                    let spawned = task::spawn(async { time::now() }).await.unwrap();
                    let blocked = task::spawn_blocking(time::unix_ms).await.unwrap();
                    let branch = tokio::select! {
                        () = std::future::ready(()) => 0,
                        () = std::future::ready(()) => 1,
                        () = std::future::ready(()) => 2,
                    };
                    let timed = time::timeout(us(50), std::future::pending::<()>()).await;
                    assert!(timed.is_err());
                    // Without `--cfg tokio_unstable` the branch is not seeded.
                    let branch = if super::super::TOKIO_RNG_SEEDED {
                        branch
                    } else {
                        0
                    };
                    let seen = (spawned - read, time::elapsed(read), blocked, branch);
                    log.lock().unwrap().push(format!("{task} {round} {seen:?}"));
                }
                assert!(task != 3, "a panic in a poll reaches its join handle");
            }));
        }
        let mut ends = Vec::new();
        for task in tasks {
            ends.push(task.await.map_err(|error| error.is_panic()));
        }
        assert_eq!(ends, [Ok(()), Ok(()), Ok(()), Err(true)]);
        log.lock().unwrap().clone()
    })
}

#[test]
fn polls_on_a_helper_thread_replay() {
    for seed in 0..10 {
        assert_eq!(polls(seed), polls(seed), "seed {seed}");
    }
}

/// With `--cfg tokio_unstable` the branches a poll races replay on a helper thread too.
#[test]
fn a_poll_on_a_helper_thread_races_branches_by_the_seed() {
    let branches = |seed| {
        run(seed, |world| async move {
            world.add_node(1, NodeClock::default());
            // A rule that never fires keeps every poll on the helper.
            world.freeze_after_clock_read(1, ClockRead::nth(u64::MAX));
            world
                .spawn_node(1, async {
                    let mut taken = Vec::new();
                    for _ in 0..40 {
                        taken.push(tokio::select! {
                            () = std::future::ready(()) => 0,
                            () = std::future::ready(()) => 1,
                        });
                        tokio::task::yield_now().await;
                    }
                    taken
                })
                .await
                .unwrap()
        })
    };
    if super::super::TOKIO_RNG_SEEDED {
        assert_eq!(branches(1), branches(1));
        assert_ne!(branches(1), branches(2));
    } else {
        assert_eq!(branches(1).len(), 40);
    }
}
