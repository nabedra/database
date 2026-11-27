//! One planted bug per fault class, with its clean twin (SIM-4 to SIM-7, SIM-13, SIM-16 to
//! SIM-20 of the simulator review).
//!
//! Each test runs a small piece of toy product code twice, planted and clean, with and
//! without one fault class of the World, and asserts three things: the planted bug is not
//! caught without the fault (so a sweep without it is blind to it), it is caught with the
//! fault, and the clean twin passes either way (so the fault itself is not what fails). A
//! cluster harness that turns the fault on can copy the shape. The storage-pause class and
//! the per-node timer floor have theirs in `suspend/tests.rs` and `tests.rs`.
//!
//! | Fault class | World API | Planted bug |
//! | --- | --- | --- |
//! | torn unsynced writes | `restart_with(Cut::PowerLoss)` | recovery trusts a record without a checksum |
//! | disk loss under the same identity | `wipe` | a voter trusts an empty disk |
//! | stale disk under the same identity | `snapshot_volume`, `restore_volume` | a voter trusts a rolled-back disk |
//! | pause between a sample and its use | `Fidelity::freeze_after_ext_reads` | a lease served on a stale ClockBound sample |
//! | dials to a powered-off host hang | `Fidelity::power_off_dials_hang` | a failover dial without a timeout |
//! | RST after a reboot | `Fidelity::reset_after_reboot` | a client that gives up on a reset |
//! | a killed process's frames still arrive | `Fidelity::kill_flush_per_mille` | a follower that ignores terms |
//! | full disk, write EIO | `set_volume_capacity`, `Fault::on(FsOp::Write)` | an ack that ignores a failed write |
//! | listing order | `Fidelity::seeded_listing` | recovery that trusts a sorted listing |
//! | one slow node | `Fidelity::sleep_suspends_job` | a lease served after a pacing sleep |
//! | declared drift bound | `set_drift_bound_ppm` | (none: a timer that fires early) |

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::net::{Envelope, Fate, Kind, Planner, SimStream, Target, dial, node_ip};
use super::{ClockRead, Cut, Fidelity, NodeClock, SimEvent, Wipe, World, in_node, run};
use crate::fs::{self, Fault, FaultError, FsFile, FsOp, OpenMode};
use crate::task;
use crate::time::{self, Instant};

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn fidelity(set: impl FnOnce(&mut Fidelity)) -> Fidelity {
    let mut fidelity = Fidelity::default();
    set(&mut fidelity);
    fidelity
}

/// How many of `seeds` catch the bug.
fn caught(seeds: std::ops::Range<u64>, run: impl Fn(u64) -> bool) -> usize {
    seeds.filter(|&seed| run(seed)).count()
}

const DATA: &str = "/sim/n1/data";
const KEYS: &str = "/sim/n1/keys";

fn path(dir: &str, name: &str) -> PathBuf {
    Path::new(dir).join(name)
}

/// Creates `name` in `dir`, durable with its directory entry.
fn create(dir: &str, name: &str) -> Arc<dyn FsFile> {
    let file = fs::real()
        .open(&path(dir, name), OpenMode::CreateNew)
        .unwrap();
    file.sync_all().unwrap();
    fs::real().sync_dir(Path::new(dir)).unwrap();
    file
}

// Torn unsynced writes (SIM-6).

const PAYLOAD: usize = 300;

fn fnv(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811C_9DC5, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
    })
}

/// Record `i`: its length, its index and the checksum of its payload, then the payload.
fn record(i: u32) -> Vec<u8> {
    let payload = vec![i as u8 + 1; PAYLOAD];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(PAYLOAD as u32).to_le_bytes());
    bytes.extend_from_slice(&i.to_le_bytes());
    bytes.extend_from_slice(&fnv(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes
}

/// The records recovery accepts from `bytes`. The planted recovery checks lengths only.
fn recover(bytes: &[u8], planted: bool) -> Vec<(u32, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 12 <= bytes.len() {
        let word = |i: usize| u32::from_le_bytes(bytes[at + i..at + i + 4].try_into().unwrap());
        let (len, index, sum) = (word(0) as usize, word(4), word(8));
        if len == 0 || at + 12 + len > bytes.len() {
            break;
        }
        let payload = bytes[at + 12..at + 12 + len].to_vec();
        if !planted && fnv(&payload) != sum {
            break;
        }
        out.push((index, payload));
        at += 12 + len;
    }
    out
}

/// Whether recovery after `cut` accepts a record that was never written.
fn accepts_a_torn_record(cut: Cut, planted: bool) -> bool {
    run(1, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        let torn = Arc::new(AtomicBool::new(false));
        let seen = torn.clone();
        world.observe(Arc::new(move |event| {
            if let SimEvent::TornWrite { report, .. } = event {
                seen.fetch_or(report.lost_anything(), SeqCst);
            }
        }));
        in_node(1, async {
            let file = create(DATA, "log");
            let mut at = 0;
            for i in 0..12_u32 {
                let bytes = record(i);
                file.write_all_at(&bytes, at).unwrap();
                at += bytes.len() as u64;
                if i % 4 == 3 && i < 8 {
                    file.sync_data().unwrap();
                }
            }
        })
        .await;
        world.restart_with(1, cut);
        let image = world.volume(1, Path::new(DATA)).unwrap();
        let bytes = image.contents(&path(DATA, "log")).unwrap();
        let recovered = recover(&bytes, planted);
        let bad = recovered.iter().enumerate().any(|(at, (index, payload))| {
            *index != at as u32 || record(*index)[12..] != payload[..]
        });
        assert!(
            !bad || torn.load(SeqCst),
            "a record was torn, so the cut must say it lost something"
        );
        bad
    })
}

#[test]
fn power_loss_tears_a_record_that_recovery_does_not_check() {
    for planted in [true, false] {
        let count = |cut: &dyn Fn(u64) -> Cut| {
            caught(0..100, |seed| accepts_a_torn_record(cut(seed), planted))
        };
        assert_eq!(
            count(&|_| Cut::Kill),
            0,
            "planted {planted}: a kill keeps every write"
        );
        assert_eq!(
            count(&|_| Cut::Crash),
            0,
            "planted {planted}: a crash keeps the synced prefix"
        );
        let torn = count(&|seed| Cut::PowerLoss { seed });
        let torn4k = count(&|seed| Cut::PowerLoss4k { seed });
        if planted {
            assert!(
                torn > 10 && torn4k > 0,
                "caught on {torn} and {torn4k} of 100"
            );
        } else {
            assert_eq!(
                (torn, torn4k),
                (0, 0),
                "the checksum stops at the torn record"
            );
        }
    }
}

// Disk loss and stale disks under the same identity (SIM-5).

/// What can happen to node 1's data volume after it voted.
#[derive(Clone, Copy, Debug)]
enum DiskFault {
    Crash,
    PowerLoss(u64),
    Wipe,
    Restore,
}

/// The vote record: term, candidate and the number of votes cast, 24 bytes.
fn read_vote() -> (u64, u64, u64) {
    let fs = fs::real();
    let Ok(file) = fs.open(&path(DATA, "vote"), OpenMode::Read) else {
        return (0, 0, 0);
    };
    let mut bytes = [0; 24];
    if file.read_exact_at(&mut bytes, 0).is_err() {
        return (0, 0, 0);
    }
    let word = |i: usize| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap());
    (word(0), word(1), word(2))
}

fn write_word(dir: &str, name: &str, words: &[u64]) {
    let fs = fs::real();
    let file = match fs.open(&path(dir, name), OpenMode::ReadWrite) {
        Ok(file) => file,
        Err(_) => create(dir, name),
    };
    let bytes: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
    file.write_all_at(&bytes, 0).unwrap();
    file.sync_data().unwrap();
}

fn votes_cast() -> u64 {
    let Ok(file) = fs::real().open(&path(KEYS, "votes"), OpenMode::Read) else {
        return 0;
    };
    let mut bytes = [0; 8];
    file.read_exact_at(&mut bytes, 0)
        .map_or(0, |()| u64::from_le_bytes(bytes))
}

/// One vote request. The clean voter keeps a count of the votes it cast on its key volume,
/// which no data-volume fault touches, and refuses to vote from a data volume that is behind
/// it (empty or rolled back), as a node must rejoin instead.
fn vote(term: u64, candidate: u64, planted: bool) -> bool {
    let (voted_term, voted_for, count) = read_vote();
    if !planted && count != votes_cast() {
        return false;
    }
    if voted_term > term || (voted_term == term && voted_for != candidate) {
        return false;
    }
    write_word(KEYS, "votes", &[count + 1]);
    write_word(DATA, "vote", &[term, candidate, count + 1]);
    true
}

/// Whether node 1 grants two candidates a vote in one term across `fault`.
fn votes_twice(fault: DiskFault, planted: bool) -> bool {
    run(2, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        world.mount(1, KEYS);
        in_node(1, async {
            write_word(KEYS, "votes", &[0]);
            write_word(DATA, "vote", &[0, 0, 0]);
        })
        .await;
        let before = world.snapshot_volume(1, Path::new(DATA));
        assert!(in_node(1, async { vote(5, 7, planted) }).await);
        match fault {
            DiskFault::Crash => world.restart_with(1, Cut::Crash),
            DiskFault::PowerLoss(seed) => world.restart_with(1, Cut::PowerLoss { seed }),
            DiskFault::Wipe => {
                world.crash(1);
                world.wipe(1, Path::new(DATA), Wipe::Reformat);
                world.restart(1);
            }
            DiskFault::Restore => {
                world.power_off(1);
                world.restore_volume(&before);
                world.restart(1);
            }
        }
        in_node(1, async { vote(5, 8, planted) }).await
    })
}

#[test]
fn a_wiped_or_rolled_back_disk_lets_a_trusting_voter_vote_twice() {
    for planted in [true, false] {
        assert!(!votes_twice(DiskFault::Crash, planted));
        assert_eq!(
            caught(0..50, |seed| votes_twice(
                DiskFault::PowerLoss(seed),
                planted
            )),
            0,
            "a synced vote survives a power loss"
        );
        assert_eq!(
            votes_twice(DiskFault::Wipe, planted),
            planted,
            "wipe, planted {planted}"
        );
        assert_eq!(
            votes_twice(DiskFault::Restore, planted),
            planted,
            "restore, planted {planted}"
        );
    }
}

// A pause between a ClockBound sample and its use (SIM-7).

/// The extension slot a product samples its clock bound through, as the Cache's hook does.
struct Sampler;

impl Sampler {
    fn sample(&mut self) -> Instant {
        time::now()
    }
}

/// Serves accepted after the lease ended, on true time. The clean store fences a serve whose
/// sample is older than a bound, the planted one takes any.
fn serves_after_the_lease(ext_reads: bool, planted: bool) -> u64 {
    run(3, move |world| async move {
        const LEASE: Duration = Duration::from_millis(1_000);
        world.add_node(1, NodeClock::default());
        world.insert_ext(Sampler);
        world.set_fidelity(fidelity(|f| f.freeze_after_ext_reads = ext_reads));
        // The 95th sample, near the end of the lease.
        world.freeze_after_clock_read(1, ClockRead::nth(95).in_file("sim/fault_classes.rs"));
        let late = Arc::new(AtomicU64::new(0));
        let counted = late.clone();
        let owner = world.clone();
        world.spawn_node(1, async move {
            let start = owner.start();
            loop {
                let sampled = owner.with_ext::<Sampler, _>(Sampler::sample).unwrap();
                if sampled.duration_since(start) >= LEASE {
                    return;
                }
                // The store's own fence: the sample must be fresh when the serve arrives.
                let age = owner
                    .elapsed()
                    .saturating_sub(sampled.duration_since(start));
                let accepted = planted || age <= ms(100);
                if accepted && owner.elapsed() >= LEASE {
                    counted.fetch_add(1, SeqCst);
                }
                time::sleep(ms(10)).await;
            }
        });
        if time::timeout(ms(1_500), world.frozen_at_read(1))
            .await
            .is_ok()
        {
            tokio::time::sleep(ms(2_000)).await;
            world.thaw(1);
        }
        tokio::time::sleep(ms(3_000)).await;
        late.load(SeqCst)
    })
}

#[test]
fn a_freeze_after_a_sample_in_an_extension_serves_a_lease_late() {
    assert_eq!(
        serves_after_the_lease(false, true),
        0,
        "reads in with_ext never stopped"
    );
    assert_eq!(serves_after_the_lease(true, true), 1, "caught");
    assert_eq!(serves_after_the_lease(false, false), 0);
    assert_eq!(
        serves_after_the_lease(true, false),
        0,
        "the clean twin fences stale samples"
    );
}

// Dials to a powered-off host (SIM-16).

fn addr(node: u64) -> SocketAddr {
    SocketAddr::new(node_ip(node), 7000)
}

fn any(port: u16) -> SocketAddr {
    SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port)
}

/// Whether node 1 fails to reach the backup within two seconds of its primary's power-off.
/// The planted client dials the primary without a timeout.
fn failover_stalls(hang: bool, planted: bool) -> bool {
    run(4, move |world| async move {
        for node in [1, 2, 3] {
            world.add_node(node, NodeClock::default());
        }
        world.set_fidelity(fidelity(|f| f.power_off_dials_hang = hang));
        let primary = world.listen(2, any(7000)).unwrap();
        let _backup = world.listen(3, any(7000)).unwrap();
        world.power_off(2);
        drop(primary);
        let client = in_node(1, async move {
            let first = if planted {
                dial(Target::Addr(addr(2))).await
            } else {
                time::timeout(ms(200), dial(Target::Addr(addr(2))))
                    .await
                    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
            };
            match first {
                Ok(_) => Ok(()),
                Err(_) => dial(Target::Addr(addr(3))).await.map(drop),
            }
        });
        time::timeout(ms(2_000), client).await.is_err()
    })
}

#[test]
fn a_failover_dial_without_a_timeout_stalls_on_a_powered_off_primary() {
    assert!(
        !failover_stalls(false, true),
        "a refusal at once hides the missing timeout"
    );
    assert!(failover_stalls(true, true), "caught");
    assert!(!failover_stalls(false, false));
    assert!(!failover_stalls(true, false));
}

// The RST a rebooted host sends (SIM-16).

/// Answers every message with the same bytes, on each connection it accepts.
fn echo(world: &Arc<World>, node: u64) {
    let listener = world.listen(node, any(7000)).unwrap();
    world.spawn_node(node, async move {
        while let Ok((pending, _)) = listener.accept().await {
            let mut stream = pending.plain();
            task::spawn(async move {
                let mut buf = [0; 64];
                while let Ok(n @ 1..) = stream.read(&mut buf).await {
                    if stream.write_all(&buf[..n]).await.is_err() || stream.flush().await.is_err() {
                        return;
                    }
                }
            });
        }
    });
}

async fn ask(stream: &mut SimStream) -> io::Result<()> {
    stream.write_all(b"ping").await?;
    stream.flush().await?;
    let mut buf = [0; 4];
    match time::timeout(ms(200), stream.read_exact(&mut buf)).await {
        Ok(read) => read.map(drop),
        Err(_) => Err(io::ErrorKind::TimedOut.into()),
    }
}

/// Whether the client gets no answer after the server's reboot. The planted client takes a
/// reset for a server that refuses it for good, and stops.
fn client_gives_up(rst: bool, planted: bool) -> bool {
    run(5, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        world.set_fidelity(fidelity(|f| f.reset_after_reboot = rst));
        echo(&world, 2);
        let answered = Arc::new(AtomicU64::new(0));
        let count = answered.clone();
        world.spawn_node(1, async move {
            let mut session = None;
            for _ in 0..30 {
                if session.is_none() {
                    session = dial(Target::Addr(addr(2))).await.ok().map(|p| p.plain());
                }
                if let Some(stream) = session.as_mut() {
                    match ask(stream).await {
                        Ok(()) => {
                            count.fetch_add(1, SeqCst);
                        }
                        Err(error) if planted && error.kind() == io::ErrorKind::ConnectionReset => {
                            return;
                        }
                        Err(_) => session = None,
                    }
                }
                time::sleep(ms(100)).await;
            }
        });
        tokio::time::sleep(ms(150)).await;
        let before = answered.load(SeqCst);
        assert!(before > 0);
        world.power_off(2);
        world.restart(2);
        echo(&world, 2);
        tokio::time::sleep(ms(3_000)).await;
        answered.load(SeqCst) == before
    })
}

#[test]
fn a_client_that_gives_up_on_a_reset_never_reaches_a_rebooted_server() {
    assert!(
        !client_gives_up(false, true),
        "a black-holed session only times out"
    );
    assert!(client_gives_up(true, true), "caught");
    assert!(!client_gives_up(false, false));
    assert!(!client_gives_up(true, false));
}

// Frames a killed process already sent (SIM-20).

/// Delays every data frame node 1 sends.
struct SlowOne;

impl Planner for SlowOne {
    fn message(&mut self, envelope: &Envelope<'_>) -> Fate {
        if envelope.from == Some(1) && envelope.kind == Kind::Data {
            Fate::Deliver { delay: ms(30) }
        } else {
            Fate::NOW
        }
    }
}

/// The value follower 3 holds after the old leader 1, killed, and the new leader 2 each
/// sent one append. The planted follower ignores terms.
fn follower_value(flush: u32, planted: bool) -> &'static str {
    run(6, move |world| async move {
        for node in [1, 2, 3] {
            world.add_node(node, NodeClock::default());
        }
        world.set_fidelity(fidelity(|f| f.kill_flush_per_mille = flush));
        let held = Arc::new(Mutex::new((0_u64, "none")));
        let listener = world.listen(3, any(7000)).unwrap();
        let state = held.clone();
        world.spawn_node(3, async move {
            while let Ok((pending, _)) = listener.accept().await {
                let mut stream = pending.plain();
                let state = state.clone();
                task::spawn(async move {
                    let mut buf = [0; 16];
                    while let Ok(n @ 1..) = stream.read(&mut buf).await {
                        let (term, value) = match &buf[..n] {
                            b"5:old" => (5, "old"),
                            b"6:new" => (6, "new"),
                            other => panic!("unexpected {other:?}"),
                        };
                        let mut held = state.lock().unwrap();
                        if planted || term >= held.0 {
                            *held = (term, value);
                        }
                    }
                });
            }
        });
        let mut old = in_node(1, dial(Target::Addr(addr(3))))
            .await
            .unwrap()
            .plain();
        let mut new = in_node(2, dial(Target::Addr(addr(3))))
            .await
            .unwrap()
            .plain();
        world.set_net_planner(SlowOne);
        old.write_all(b"5:old").await.unwrap();
        old.flush().await.unwrap();
        world.crash(1);
        new.write_all(b"6:new").await.unwrap();
        new.flush().await.unwrap();
        tokio::time::sleep(ms(100)).await;
        held.lock().unwrap().1
    })
}

#[test]
fn a_follower_that_ignores_terms_takes_a_killed_leaders_last_frame() {
    assert_eq!(
        follower_value(0, true),
        "new",
        "a reset discards what the leader sent"
    );
    assert_eq!(follower_value(1_000, true), "old", "caught");
    assert_eq!(follower_value(0, false), "new");
    assert_eq!(follower_value(1_000, false), "new");
}

// A full disk and write errors (SIM-17).

#[derive(Clone, Copy, Debug)]
enum WriteFault {
    None,
    Full,
    Eio,
}

/// Whether node 1 acknowledged a record that a crash then lost. The planted writer ignores
/// a failed write and acknowledges once the sync succeeds.
fn acks_a_lost_record(fault: WriteFault, planted: bool) -> bool {
    run(7, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        let file = in_node(1, async { create(DATA, "log") }).await;
        match fault {
            WriteFault::None => {}
            WriteFault::Full => world.set_volume_capacity(1, Path::new(DATA), Some(20 * 64)),
            WriteFault::Eio => world
                .volume(1, Path::new(DATA))
                .unwrap()
                .inject(Fault::on(FsOp::Write).nth(10)),
        }
        let acked = Arc::new(AtomicU64::new(0));
        let count = acked.clone();
        world
            .spawn_node(1, async move {
                for i in 0..40_u64 {
                    let (file, count) = (file.clone(), count.clone());
                    let stop = task::spawn_blocking(move || {
                        let wrote = file.write_all_at(&[1; 64], i * 64).is_ok();
                        let synced = file.sync_data().is_ok();
                        if synced && (wrote || planted) {
                            count.fetch_add(1, SeqCst);
                            false
                        } else {
                            true
                        }
                    })
                    .await
                    .unwrap();
                    if stop {
                        return;
                    }
                }
            })
            .await
            .unwrap();
        world.restart_with(1, Cut::Crash);
        let image = world.volume(1, Path::new(DATA)).unwrap();
        let bytes = image.contents(&path(DATA, "log")).unwrap();
        let durable = bytes
            .chunks(64)
            .take_while(|c| c.iter().all(|b| *b == 1))
            .count();
        acked.load(SeqCst) > durable as u64
    })
}

#[test]
fn an_ack_that_ignores_a_failed_write_loses_data_on_a_full_or_failing_disk() {
    for planted in [true, false] {
        assert!(!acks_a_lost_record(WriteFault::None, planted));
        assert_eq!(
            acks_a_lost_record(WriteFault::Full, planted),
            planted,
            "full"
        );
        assert_eq!(acks_a_lost_record(WriteFault::Eio, planted), planted, "EIO");
    }
}

#[test]
fn statvfs_reports_a_volume_capacity_and_enospc_names_it() {
    run(8, |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        world.set_volume_capacity(1, Path::new(DATA), Some(8 << 20));
        let space = in_node(1, async {
            crate::fs::os::File::open(DATA).unwrap().statvfs()
        })
        .await
        .unwrap();
        assert_eq!(space.blocks * space.block_size, 8 << 20);
        let faults = Arc::new(Mutex::new(Vec::new()));
        let seen = faults.clone();
        world.observe(Arc::new(move |event| {
            if let SimEvent::Fs { call, .. } = event {
                seen.lock().unwrap().extend(call.fault);
            }
        }));
        in_node(1, async {
            let file = create(DATA, "big");
            assert!(file.set_len(9 << 20).is_err());
            file.set_len(4 << 20).unwrap();
        })
        .await;
        world.restart_with(1, Cut::Kill);
        let image = world.volume(1, Path::new(DATA)).unwrap();
        assert_eq!(
            image.capacity(),
            Some(8 << 20),
            "a restart keeps the device's size"
        );
        assert_eq!(*faults.lock().unwrap(), [FaultError::Enospc]);
    });
}

// Listing order (SIM-19).

/// The value recovery ends with after replaying twelve segments. The planted recovery
/// takes the listing's order for the write order.
fn recovered_latest(seeded: bool, planted: bool, seed: u64) -> u64 {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        world.set_fidelity(fidelity(|f| f.seeded_listing = seeded));
        in_node(1, async move {
            for i in 1..=12_u64 {
                let file = create(DATA, &format!("seg-{i:03}"));
                file.write_all_at(&i.to_le_bytes(), 0).unwrap();
                file.sync_data().unwrap();
            }
            let mut names = fs::real().list(Path::new(DATA)).unwrap();
            if !planted {
                names.sort();
            }
            let mut latest = 0;
            for name in names {
                let file = fs::real().open(&name, OpenMode::Read).unwrap();
                let mut bytes = [0; 8];
                file.read_exact_at(&mut bytes, 0).unwrap();
                latest = u64::from_le_bytes(bytes);
            }
            latest
        })
        .await
    })
}

#[test]
fn recovery_that_trusts_a_sorted_listing_replays_out_of_order() {
    let wrong =
        |seeded, planted| caught(0..20, |seed| recovered_latest(seeded, planted, seed) != 12);
    assert_eq!(wrong(false, true), 0, "a sorted listing hides the bug");
    assert!(wrong(true, true) > 10, "caught");
    assert_eq!(wrong(false, false), 0);
    assert_eq!(wrong(true, false), 0);
}

// One slow node (SIM-18).

/// Whether node 1 serves under its lease after node 2 took over. Node 1 paces with a
/// synchronous sleep between its lease check and its serve; the planted node does not check
/// again after the sleep.
fn serves_after_takeover(suspend: bool, planted: bool) -> bool {
    run(9, move |world| async move {
        const LEASE: Duration = Duration::from_millis(1_000);
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        world.set_fidelity(fidelity(|f| f.sleep_suspends_job = suspend));
        let taken = Arc::new(AtomicBool::new(false));
        let overlap = Arc::new(AtomicBool::new(false));
        let start = world.start();
        let flag = taken.clone();
        world.spawn_node(2, async move {
            time::sleep_until(start + LEASE + ms(50)).await;
            flag.store(true, SeqCst);
        });
        let (seen, found) = (taken.clone(), overlap.clone());
        world.spawn_node(1, async move {
            while time::now().duration_since(start) < LEASE {
                let (seen, found) = (seen.clone(), found.clone());
                task::spawn_blocking(move || {
                    time::thread_sleep(ms(300));
                    if planted || time::now().duration_since(start) < LEASE {
                        found.fetch_or(seen.load(SeqCst), SeqCst);
                    }
                })
                .await
                .unwrap();
                time::sleep(ms(10)).await;
            }
        });
        tokio::time::sleep(ms(3_000)).await;
        if suspend {
            assert!(world.global_pauses() == 0, "no sleep paused the world");
        }
        overlap.load(SeqCst)
    })
}

#[test]
fn a_pacing_sleep_that_stops_the_world_hides_a_lease_served_late() {
    assert!(
        !serves_after_takeover(false, true),
        "a stop-the-world sleep hides it"
    );
    assert!(serves_after_takeover(true, true), "caught");
    assert!(!serves_after_takeover(false, false));
    assert!(!serves_after_takeover(true, false));
}

// A declared drift bound (SIM-13).

/// How early, on its node clock, a three-second timer armed before a slowdown fires.
fn earliness(bound: Option<i64>, add_before: bool) -> Duration {
    run(10, move |world| async move {
        if add_before {
            world.add_node(1, NodeClock::default());
            world.set_drift_bound_ppm(bound);
        } else {
            world.set_drift_bound_ppm(bound);
            world.add_node(1, NodeClock::default());
        }
        let timer = world.spawn_node(1, async {
            let deadline = time::now() + ms(3_000);
            time::sleep_until(deadline).await;
            deadline.saturating_duration_since(time::now())
        });
        tokio::time::sleep(ms(1)).await;
        world.set_drift_ppm(1, -200);
        timer.await.unwrap()
    })
}

#[test]
fn a_world_drift_bound_keeps_every_nodes_timers_from_firing_early() {
    assert!(
        earliness(None, true) > Duration::ZERO,
        "a slowdown fires timers early"
    );
    assert_eq!(
        earliness(Some(200), true),
        Duration::ZERO,
        "nodes added before"
    );
    assert_eq!(
        earliness(Some(200), false),
        Duration::ZERO,
        "nodes added after"
    );
}

#[test]
#[should_panic(expected = "beyond the world's declared bound of 200 ppm")]
fn a_drift_beyond_the_world_bound_panics() {
    run(11, |world| async move {
        world.add_node(1, NodeClock::default());
        world.set_drift_bound_ppm(Some(200));
        world.set_drift_ppm(1, 201);
    });
}

// Seeded storage pauses at writes (SIM-4).

#[test]
fn seeded_storage_pauses_stop_at_the_calls_asked_for() {
    let paused_ops = |ops: Option<&'static [FsOp]>| {
        run(12, move |world| async move {
            world.add_node(1, NodeClock::default());
            world.mount(1, DATA);
            if let Some(ops) = ops {
                world.set_storage_pause_ops(ops);
            }
            world.set_storage_pauses(1_000, ms(1));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let log = seen.clone();
            world.observe(Arc::new(move |event| {
                if let SimEvent::StoragePaused { call, .. } = event {
                    log.lock().unwrap().push((call.op, call.side));
                }
            }));
            world
                .spawn_node(1, async {
                    task::spawn_blocking(|| {
                        let file = create(DATA, "log");
                        file.write_all_at(b"x", 0).unwrap();
                        file.sync_data().unwrap();
                    })
                    .await
                    .unwrap();
                })
                .await
                .unwrap();
            let ops: Vec<FsOp> = seen.lock().unwrap().iter().map(|(op, _)| *op).collect();
            ops
        })
    };
    let default = paused_ops(None);
    assert!(!default.is_empty() && !default.contains(&FsOp::Write));
    let writes = paused_ops(Some(&[FsOp::Write]));
    assert!(!writes.is_empty() && writes.iter().all(|op| *op == FsOp::Write));
}
