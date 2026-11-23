//! NS10: the whole-engine crash, power-loss and fault campaign with a differential model.
//!
//! One harness composes what the store-level tests check one at a time:
//! - a seeded multi-scope workload: scoped and store-wide batches of puts, journal puts,
//!   deletes and range deletes, synced, unsynced and cancelled; held snapshots; point
//!   reads and iterator walks forward and reverse with bounds and `seek_for_prev`;
//!   `release_scope`; forced flush, compaction and journal GC; clean reopens;
//! - machine stops: a crash at the n-th mutating filesystem operation (enumerated for
//!   small runs, sampled for large ones), process kill, and power loss that keeps any
//!   subset of the unsynced 512 B sectors (each new, old or torn) or destroys unsynced
//!   4 KiB units (zeros or garbage), with unsynced directory operations kept or lost;
//! - EIO on writes, syncs, renames and run reads, ENOSPC on runs, segment preallocation
//!   and all writes (NS09, NS11);
//! - bit flips in synced bytes of a cleanly closed store (NS07 classes);
//! - random sleeps in the engine's background threads with cancelled callers (NS17).
//!
//! The oracle is a `BTreeMap`. After every cut the store must reopen to the oracle state
//! after some prefix `p` of the submitted batches with `durable <= p <= submitted`, where
//! `durable` counts batches acknowledged durable (NS04, NS05). A crash right after that
//! recovery reopens to the same state with no tail truncated again. The run then goes on
//! from the recovered state, so later generations exercise recycled segments, voided
//! sequences and epochs. Damage outcomes follow plan section 3 (see `damage.rs`).
//!
//! `FaultFs` power loss is a fault model, not physical power-loss evidence (O29 wording).
//!
//! Scale comes from environment variables (defaults keep the debug suite under a minute):
//! `NS10_SEED_BASE`, `NS10_SEEDS`, `NS10_STEPS`, `NS10_ENUM_SEEDS`, `NS10_ENUM_STEPS`,
//! `NS10_DAMAGE_SEEDS`, `NS10_DAMAGE_TRIALS`, `NS10_SLEEPY_SEEDS`, `NS10_LONG_SEEDS`,
//! `NS10_LONG_OPS`, `NS10_SHRINK` (0 disables shrinking), `NS10_SHRINK_SECS`.
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

#[path = "../common/mod.rs"]
mod common;
mod damage;
mod harness;
mod oracle;
mod regressions;
mod shrink;
mod sleepy;
mod workload;

use std::time::{Duration, Instant};

use harness::{Config, Exec, Report, Watchdog, run_steps};
use workload::{CutKind, Mix, Step, generate};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Runs `steps`; on failure, shrinks the list (unless `NS10_SHRINK=0`) and panics with a
/// `regressions::Case` to keep.
fn run_or_shrink(
    cfg: &Config,
    steps: &[Step],
    len: usize,
    prefix: &[Step],
    wd: &Watchdog,
) -> Report {
    let all: Vec<Step> = prefix.iter().chain(steps).cloned().collect();
    let err = match run_steps(cfg, &all, Some(wd)) {
        Ok(r) => return r,
        Err(e) => e,
    };
    if env_u64("NS10_SHRINK", 1) == 0 {
        panic!("NS10 failure (seed {}): {err}", cfg.seed);
    }
    eprintln!("NS10 failure (seed {}): {err}\nshrinking...", cfg.seed);
    let budget = Duration::from_secs(env_u64("NS10_SHRINK_SECS", 120));
    let mut last = err.clone();
    let keep = shrink::ddmin(steps.len(), budget, &mut |idx: &[usize]| {
        let sub: Vec<Step> = prefix
            .iter()
            .cloned()
            .chain(idx.iter().map(|&i| steps[i].clone()))
            .collect();
        for _ in 0..2 {
            if let Err(e) = run_steps(cfg, &sub, Some(wd)) {
                last = e;
                return true;
            }
        }
        false
    });
    panic!(
        "NS10 failure (seed {}): {err}\nshrunk to {} of {} steps; last failure: {last}\nkeep as: Case {{ seed: {}, steps: {len}, mix: {:?}, damage_trials: {}, keep: &{keep:?}, note: \"...\" }}\nprefix: {prefix:?}\nsteps: {:?}",
        cfg.seed,
        keep.len(),
        steps.len(),
        cfg.seed,
        cfg.mix,
        cfg.damage_trials,
        keep.iter().map(|&i| &steps[i]).collect::<Vec<_>>()
    );
}

fn summary(name: &str, seeds: u64, started: Instant, r: &Report) {
    eprintln!(
        "{name}: {seeds} seeds, {} runs, {} steps in {:.1} s: {} writes ({} cancelled, {} NoSpace), {} syncs, {} read checks, {} flushes, {} compactions, {} GCs, {} releases, {} reopens, {} faults; {} cuts ({} crash, {} kill, {} power loss; {} after a fault), {} unsynced batches dropped; {} mutating FS ops; {} order guards checked; {} naps; {} repeated tails after a crash right after recovery",
        r.runs,
        r.steps,
        started.elapsed().as_secs_f64(),
        r.writes,
        r.cancelled,
        r.no_space_writes,
        r.syncs,
        r.reads,
        r.flushes,
        r.compactions,
        r.gcs,
        r.releases,
        r.reopens,
        r.faults,
        r.cuts,
        r.crash_cuts,
        r.kill_cuts,
        r.power_loss_cuts,
        r.fault_cuts,
        r.unsynced_dropped,
        r.mutating_ops,
        r.guards_checked,
        r.naps,
        r.repeat_tails
    );
    if r.damage.trials > 0 {
        eprintln!("{name}: damage {:?}", r.damage);
    }
}

fn sampled(
    name: &str,
    seeds_var: &str,
    default_seeds: u64,
    steps_var: &str,
    default_steps: u64,
    mix: Mix,
    tune: impl Fn(&mut Config),
) -> Report {
    let wd = Watchdog::start(Duration::from_secs(120));
    let base = env_u64("NS10_SEED_BASE", 0);
    let seeds = env_u64(seeds_var, default_seeds);
    let len = env_u64(steps_var, default_steps) as usize;
    let started = Instant::now();
    let mut total = Report::default();
    for seed in base..base + seeds {
        let mut cfg = Config::new(seed, mix);
        tune(&mut cfg);
        let steps = generate(seed, len, &mix);
        total.add(&run_or_shrink(&cfg, &steps, len, &[], &wd));
    }
    summary(name, seeds, started, &total);
    total
}

/// NS04: a crash at every mutating filesystem operation of a small run, each turned into a
/// crash, kill, power-loss or 4 KiB power-loss image in turn.
#[test]
fn ns04_ns10_crash_at_every_mutating_operation() {
    let wd = Watchdog::start(Duration::from_secs(120));
    let base = env_u64("NS10_SEED_BASE", 0);
    let seeds = env_u64("NS10_ENUM_SEEDS", 2);
    let len = env_u64("NS10_ENUM_STEPS", 60) as usize;
    let mix = Mix {
        cuts: false,
        faults: false,
        ..Mix::full()
    };
    let started = Instant::now();
    let mut total = Report::default();
    for seed in base + 100..base + 100 + seeds {
        let cfg = Config::new(seed, mix);
        let steps = generate(seed, len, &mix);
        let reference = run_or_shrink(&cfg, &steps, len, &[], &wd);
        let ops = reference.mutating_ops;
        eprintln!("NS04 seed {seed}: {ops} mutating operations in the reference run");
        let only = std::env::var("NS10_ENUM_ONLY")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        for n in 0..=ops {
            if only.is_some_and(|o| o != n) {
                continue;
            }
            let kind = match n % 4 {
                0 => CutKind::Crash,
                1 => CutKind::Kill,
                2 => CutKind::PowerLoss(n),
                _ => CutKind::PowerLoss4k(n),
            };
            let arm = [Step::ArmCrash { after: n, kind }];
            total.add(&run_or_shrink(&cfg, &steps, len, &arm, &wd));
        }
    }
    assert!(total.cuts > 0);
    summary("NS04 enumeration", seeds, started, &total);
}

/// NS05, NS09, NS11: sampled multi-generation runs with in-run power loss, kills, armed
/// crashes, EIO and ENOSPC, cancelled callers, releases and reopens.
#[test]
fn ns05_ns10_power_loss_faults_and_recoveries_sampled() {
    let r = sampled(
        "NS05 sampled",
        "NS10_SEEDS",
        12,
        "NS10_STEPS",
        400,
        Mix::full(),
        |_| {},
    );
    assert!(r.cuts > 0 && r.faults > 0 && r.releases > 0);
}

/// NS07: bit flips in synced records, runs, manifests and `CURRENT` of a cleanly closed
/// store.
#[test]
fn ns07_ns10_bit_flips_in_synced_bytes() {
    let trials = env_u64("NS10_DAMAGE_TRIALS", 30) as usize;
    let r = sampled(
        "NS07 damage",
        "NS10_DAMAGE_SEEDS",
        4,
        "NS10_STEPS",
        300,
        Mix {
            cuts: false,
            faults: false,
            ..Mix::full()
        },
        |c| c.damage_trials = trials,
    );
    assert!(r.damage.trials > 0);
}

/// NS17: random sleeps in the background threads with cancelled callers, plus the full
/// mix of cuts and faults.
#[test]
fn ns17_ns10_sleepy_background_threads_and_cancelled_callers() {
    let r = sampled(
        "NS17 sleepy",
        "NS10_SLEEPY_SEEDS",
        3,
        "NS10_STEPS",
        300,
        Mix::full(),
        |c| c.sleepy = Some(10),
    );
    assert!(r.naps > 0 && r.cancelled > 0);
}

/// NS01 at 10⁶ operations with every kind of cut and fault mixed in, generated in chunks.
#[test]
#[ignore = "10^6 operations; run in release"]
fn ns01_ns10_differential_1e6_with_cuts_and_faults() {
    let wd = Watchdog::start(Duration::from_secs(300));
    let seed = env_u64("NS10_SEED_BASE", 0) + 4242;
    let ops = env_u64("NS10_LONG_OPS", 1_000_000);
    let mix = Mix::full();
    let cfg = Config::new(seed, mix);
    let started = Instant::now();
    let mut exec = Exec::new(&cfg, Some(&wd));
    exec.start().unwrap();
    let chunk = 10_000;
    let mut done = 0;
    while done < ops {
        let n = chunk.min(ops - done);
        let steps = generate(seed.wrapping_add(done * 7919), n as usize, &mix);
        if let Err(e) = exec.steps(&steps) {
            panic!("NS01 1e6: chunk at op {done}: {e}");
        }
        done += n;
    }
    exec.finish().unwrap();
    summary("NS01 1e6", 1, started, &exec.report);
}

/// NS04/NS05/NS07/NS17 at scale: thousands of seeds.
#[test]
#[ignore = "thousands of seeds; run in release"]
fn ns05_ns10_thousands_of_seeds() {
    sampled(
        "NS10 long",
        "NS10_LONG_SEEDS",
        2000,
        "NS10_STEPS",
        400,
        Mix::full(),
        |c| {
            if c.seed % 4 == 0 {
                c.sleepy = Some(10);
            }
            if c.seed % 3 == 0 {
                c.damage_trials = 8;
            }
        },
    );
}

#[test]
fn ns10_shrinker_reduces_to_the_failing_steps() {
    let mut runs = 0;
    let keep = shrink::ddmin(200, Duration::from_secs(10), &mut |idx| {
        runs += 1;
        idx.contains(&17)
            && idx.contains(&150)
            && idx.iter().position(|&i| i == 17) < idx.iter().position(|&i| i == 150)
    });
    assert_eq!(keep, vec![17, 150], "after {runs} replays");
}

#[test]
fn ns10_regressions_replay_shrunk_cases() {
    let wd = Watchdog::start(Duration::from_secs(120));
    for case in regressions::FIXED {
        let mut cfg = Config::new(case.seed, case.mix);
        cfg.damage_trials = case.damage_trials;
        let all = generate(case.seed, case.steps, &case.mix);
        let steps: Vec<Step> = case.keep.iter().map(|&i| all[i].clone()).collect();
        if let Err(e) = run_steps(&cfg, &steps, Some(&wd)) {
            panic!(
                "regression {} (seed {}) fails again: {e}",
                case.note, case.seed
            );
        }
    }
    eprintln!("{} kept regressions replayed", regressions::FIXED.len());
}

/// Unfixed findings, kept shrunk until their fix lands (see `regressions.rs`).
#[test]
#[ignore = "NS10 open findings: replays regressions::OPEN_FINDINGS, expected to fail until fixed"]
fn ns10_open_findings_replay() {
    let wd = Watchdog::start(Duration::from_secs(120));
    for case in regressions::OPEN_FINDINGS {
        let mut cfg = Config::new(case.seed, case.mix);
        cfg.damage_trials = case.damage_trials;
        let all = generate(case.seed, case.steps, &case.mix);
        let steps: Vec<Step> = case.keep.iter().map(|&i| all[i].clone()).collect();
        if let Err(e) = run_steps(&cfg, &steps, Some(&wd)) {
            panic!("open finding {} (seed {}): {e}", case.note, case.seed);
        }
    }
}
