//! Scratch, review of #123: my own plants with the faults off and on.
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{ClockRead, Cut, NodeClock, SimEvent, SplitMix64, Wipe, World, in_node, run};
use crate::fs::{self, OpenMode, os};
use crate::{task, time};

const BASE: u64 = 7_300_000;
const DATA: &str = "/sim/n1/data";

fn us(v: u64) -> Duration {
    Duration::from_micros(v)
}
fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

/// A file published by rename. The planted reader trusts the name as soon as it is there;
/// the clean one waits for the directory sync.
fn loses_a_publish(seed: u64, planted: bool, pauses: bool) -> bool {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.mount(1, DATA);
        if pauses {
            world.set_storage_pauses(300, ms(3));
        }
        let named = Arc::new(AtomicU64::new(0));
        let synced = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        {
            let (named, synced, failed) = (named.clone(), synced.clone(), failed.clone());
            world.spawn_node(1, async move {
                for n in 1u64.. {
                    time::sleep(us(650)).await;
                    let (named, synced, failed) = (named.clone(), synced.clone(), failed.clone());
                    task::spawn_blocking(move || {
                        let go = || -> std::io::Result<()> {
                            let tmp = format!("{DATA}/tmp{n}");
                            let f = fs::real().open(Path::new(&tmp), OpenMode::CreateNew)?;
                            f.write_all_at(b"x", 0)?;
                            f.sync_all()?;
                            fs::real().rename(Path::new(&tmp), Path::new(&format!("{DATA}/v{n}")))?;
                            named.store(n, SeqCst);
                            fs::real().sync_dir(Path::new(DATA))?;
                            synced.store(n, SeqCst);
                            Ok(())
                        };
                        if go().is_err() {
                            failed.store(true, SeqCst);
                        }
                    })
                    .await
                    .unwrap();
                }
            });
        }
        let seen = Arc::new(AtomicU64::new(0));
        let (s, n, y, f) = (seen.clone(), named, synced, failed);
        world.spawn_node(1, async move {
            loop {
                time::sleep(us(110)).await;
                if !f.load(SeqCst) {
                    s.fetch_max(if planted { n.load(SeqCst) } else { y.load(SeqCst) }, SeqCst);
                }
            }
        });
        let mut rng = SplitMix64::new(seed ^ 0x99);
        tokio::time::sleep(us(500 + rng.below(9_000))).await;
        let acked = seen.load(SeqCst);
        world.crash(1);
        world.restart_with(1, Cut::Crash);
        acked > 0 && {
            let volume = world.volume(1, Path::new(DATA)).unwrap();
            volume.contents(Path::new(&format!("{DATA}/v{acked}"))).is_err()
        }
    })
}

#[test]
fn own_a_rename() {
    let n = 400;
    let count = |planted, pauses| (0..n).filter(|i| loses_a_publish(BASE + i, planted, pauses)).count();
    println!(
        "OWN a rename before dir sync: planted off {} on {}, clean off {} on {} of {n}",
        count(true, false),
        count(true, true),
        count(false, false),
        count(false, true)
    );
}

/// The holder checks its lease in a blocking job, and the write goes to a store that keeps
/// true time. The clean store refuses a write whose holder's lease is over.
fn writes_late(seed: u64, stops: u8, fenced: bool) -> bool {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        let end = time::now() + ms(40);
        let start = world.start();
        match stops {
            0 => {}
            1 => world.set_clock_freezes(60, ms(30)),
            _ => world.freeze_after_clock_read(1, ClockRead::nth(30 + seed % 12).in_file("review123.rs")),
        }
        let late = Arc::new(AtomicU64::new(0));
        let l = late.clone();
        world.spawn_node(1, async move {
            loop {
                time::sleep(us(390)).await;
                let l = l.clone();
                task::spawn_blocking(move || {
                    if time::now() < end {
                        let over = tokio::time::Instant::now() >= start + ms(40);
                        if over && !fenced {
                            l.fetch_add(1, SeqCst);
                        }
                    }
                })
                .await
                .unwrap();
            }
        });
        let w: Arc<World> = world.clone();
        tokio::time::sleep(ms(70)).await;
        w.thaw(1);
        tokio::time::sleep(ms(80)).await;
        late.load(SeqCst) > 0
    })
}

#[test]
fn own_b_job() {
    let n = 400;
    let count = |stops, fenced| (0..n).filter(|i| writes_late(BASE + i, stops, fenced)).count();
    println!(
        "OWN b lease check in a job: off {} seeded {} chosen read {}, clean seeded {} chosen {} of {n}",
        count(0, false),
        count(1, false),
        count(2, false),
        count(1, true),
        count(2, true)
    );
}

/// The earliest firing of chained timers under many drift changes, with and without a floor.
fn earliest(seed: u64, changes: u64, floor: Option<i64>) -> (Duration, Duration) {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.set_timer_floor_ppm(1, floor);
        let worst = Arc::new(Mutex::new((Duration::ZERO, Duration::ZERO)));
        let mut timers = Vec::new();
        for kind in 0..3u64 {
            let worst = worst.clone();
            timers.push(world.spawn_node(1, async move {
                let note = |due: time::Instant| {
                    let now = time::now();
                    let mut w = worst.lock().unwrap();
                    w.0 = w.0.max(due.saturating_duration_since(now));
                    w.1 = w.1.max(now.saturating_duration_since(due));
                };
                match kind {
                    0 => {
                        let due = time::now() + ms(20_000);
                        time::sleep_until(due).await;
                        note(due);
                    }
                    1 => {
                        for _ in 0..20 {
                            let due = time::now() + ms(1_000);
                            time::sleep(ms(1_000)).await;
                            note(due);
                        }
                    }
                    _ => {
                        let start = time::now();
                        let mut t = time::interval(ms(200));
                        for i in 0..100u32 {
                            t.tick().await;
                            note(start + ms(200) * i);
                        }
                    }
                }
            }));
        }
        let mut rng = SplitMix64::new(seed ^ 0x77);
        for _ in 0..changes {
            tokio::time::sleep(us(1 + rng.below(2 * 20_000_000 / changes))).await;
            world.set_drift_ppm(1, rng.below(401) as i64 - 200);
        }
        for t in timers {
            t.await.unwrap();
        }
        let w = *worst.lock().unwrap();
        w
    })
}

#[test]
fn own_c_floor() {
    for changes in [0, 20, 900, 20_000, 200_000] {
        let seeds = if changes > 1_000 { 3 } else { 100 };
        let run = |floor| {
            (0..seeds)
                .map(|i| earliest(BASE + i, changes, floor))
                .fold((0, Duration::ZERO, Duration::ZERO), |a, b| {
                    (a.0 + usize::from(!b.0.is_zero()), a.1.max(b.0), a.2.max(b.1))
                })
        };
        println!(
            "OWN c {changes} drift changes, {seeds} seeds (early seeds, earliest, latest): no floor {:?}, floor {:?}",
            run(None),
            run(Some(-200))
        );
    }
}

#[test]
fn own_e_mkfs() {
    let n = 1_000;
    let mut same = (0, 0, 0);
    for i in 0..n {
        let (a, b) = run(BASE + i, |world| async move {
            world.set_mkfs_inodes(true);
            world.add_node(1, NodeClock::default());
            world.mount(1, DATA);
            let ino = async || {
                let uuid = world.volume_info(1, Path::new(DATA)).unwrap().filesystem_uuid;
                in_node(1, async move {
                    os::create_dir("/sim/n1/data/pool").unwrap();
                    use std::os::unix::fs::MetadataExt as _;
                    let r = os::metadata("/sim/n1/data").unwrap();
                    let p = os::metadata("/sim/n1/data/pool").unwrap();
                    (r.ino(), p.ino(), uuid)
                })
                .await
            };
            let a = ino().await;
            world.crash(1);
            world.wipe(1, Path::new(DATA), Wipe::Reformat);
            world.restart(1);
            (a, ino().await)
        });
        same.0 += usize::from(a.0 == b.0);
        same.1 += usize::from(a.1 == b.1);
        same.2 += usize::from(a.2 == b.2);
    }
    println!("OWN e mkfs inodes: root repeats {}, child repeats {}, uuid repeats {} of {n}", same.0, same.1, same.2);
}

/// Everything a run of a small node does, with no rule, with rules set to zero, or with the
/// setters called and taken back.
fn trace(seed: u64, mode: u8) -> (Vec<String>, u64) {
    run(seed, move |world| async move {
        let events = Arc::new(Mutex::new(Vec::new()));
        let faults = Arc::new(AtomicU64::new(0));
        let (sink, count) = (events.clone(), faults.clone());
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            if matches!(event, SimEvent::StoragePaused { .. } | SimEvent::ClockFrozen { .. } | SimEvent::StorageResumed { .. }) {
                count.fetch_add(1, SeqCst);
            }
            sink.lock().unwrap().push(format!("{event:?}"));
        }));
        world.set_jitter(200, ms(1));
        world.add_node(1, NodeClock { drift_ppm: 150, ..NodeClock::default() });
        world.mount(1, DATA);
        match mode {
            0 => {}
            1 => {
                world.set_storage_pauses(0, ms(5));
                world.set_clock_freezes(0, ms(5));
                world.set_timer_floor_ppm(1, None);
                world.set_mkfs_inodes(false);
            }
            _ => {
                world.set_storage_pauses(700, ms(5));
                world.set_clock_freezes(700, ms(5));
                world.set_timer_floor_ppm(1, Some(-300));
                world.set_storage_pauses(0, Duration::ZERO);
                world.set_clock_freezes(0, Duration::ZERO);
                world.set_timer_floor_ppm(1, None);
                world.set_mkfs_inodes(true);
                world.set_mkfs_inodes(false);
            }
        }
        let log = events.clone();
        for t in 0..3u64 {
            let log = log.clone();
            world.spawn_node(1, async move {
                for i in 0..30u64 {
                    time::sleep(us(200 + 70 * t)).await;
                    let r = time::unix_micros();
                    let b = task::spawn_blocking(move || {
                        os::write(format!("{DATA}/f{t}"), i.to_le_bytes()).unwrap();
                        fs::real().sync_dir(Path::new(DATA)).unwrap();
                        time::unix_micros()
                    })
                    .await
                    .unwrap();
                    let c = tokio::select! {
                        () = std::future::ready(()) => 0,
                        () = std::future::ready(()) => 1,
                    };
                    log.lock().unwrap().push(format!("{t} {i} {r} {b} {c} {}", crate::ids::uuid_v4()));
                }
            });
        }
        tokio::time::sleep(ms(6)).await;
        world.set_drift_ppm(1, -100);
        tokio::time::sleep(ms(12)).await;
        world.restart_with(1, Cut::PowerLoss { seed });
        tokio::time::sleep(ms(3)).await;
        let lines = events.lock().unwrap().clone();
        (lines, faults.load(SeqCst))
    })
}

#[test]
fn own_off_by_default() {
    let mut lines = 0;
    for i in 0..100 {
        let a = trace(BASE + i, 0);
        assert_eq!(a.1, 0);
        assert_eq!(a, trace(BASE + i, 1), "zero rates");
        assert_eq!(a, trace(BASE + i, 2), "set and taken back");
        lines += a.0.len();
    }
    println!("OWN off: 100 seeds, {lines} trace lines, no fault event, zero rates and taken-back rates identical");
}
