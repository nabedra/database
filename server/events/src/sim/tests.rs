//! Simulation tests. Run with
//! `RUSTFLAGS="--cfg tokio_unstable" cargo test --features sim --lib sim::`.
//!
//! - `NABEDRA_SEED`: the first seed (default 1).
//! - `NABEDRA_SEEDS`: how many consecutive seeds the sweep runs (default 20).
//! - `NABEDRA_SIM_HISTORY_DIR`: also write every run's history there.

use std::path::PathBuf;

use super::cluster::{Knobs, Report, SimConfig, run};
use super::history::History;

const REPLAY: &str = "RUSTFLAGS=\"--cfg tokio_unstable\" cargo test --features sim --lib sim::tests::seed_sweep -- --nocapture";

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer, got {value:?}")),
        Err(_) => default,
    }
}

fn history_path(dir: PathBuf, report: &Report, label: &str) -> PathBuf {
    dir.join(format!("events-{label}-seed-{}.jsonl", report.seed))
}

fn write_history(report: &Report, label: &str, dir: PathBuf) -> Option<PathBuf> {
    let path = history_path(dir, report, label);
    let history = History {
        events: report.history.clone(),
    };
    history.write_jsonl(&path).ok().map(|_| path)
}

fn describe(report: &Report, label: &str) -> String {
    let path = write_history(report, label, std::env::temp_dir().join("nabedra-sim"));
    format!(
        "seed {seed} failed ({label})\n  violations:\n    {violations}\n  stats: {stats:?}\n  trace tail:\n    {tail}\n  history: {history}\n  replay: NABEDRA_SEED={seed} NABEDRA_SEEDS=1 {REPLAY}",
        seed = report.seed,
        violations = report.violations.join("\n    "),
        stats = report.stats,
        tail = report.trace_tail.join("\n    "),
        history = path.map_or_else(|| "not written".into(), |path| path.display().to_string()),
    )
}

/// Every seed in the configured range keeps every invariant.
#[test]
fn seed_sweep() {
    let first = env_u64("NABEDRA_SEED", 1);
    let count = env_u64("NABEDRA_SEEDS", 20);
    let keep = std::env::var_os("NABEDRA_SIM_HISTORY_DIR").map(PathBuf::from);
    let mut failures = Vec::new();
    let mut totals = (0usize, 0usize, 0u64);
    for seed in first..first + count {
        let report = run(SimConfig::new(seed));
        if let Some(dir) = &keep {
            write_history(&report, "sweep", dir.clone());
        }
        totals.0 += report.stats.publishes_ok;
        totals.1 += report.stats.final_events;
        totals.2 += report.stats.crashes;
        if !report.passed() {
            eprintln!("{}", describe(&report, "sweep"));
            failures.push(seed);
        }
    }
    eprintln!(
        "events sim: {count} seeds from {first}: {} failed; {} publishes acknowledged, {} events retained, {} crashes",
        failures.len(),
        totals.0,
        totals.1,
        totals.2
    );
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

/// The same seed gives the same trace and history, run after run.
#[test]
fn same_seed_same_trace() {
    let seed = env_u64("NABEDRA_SEED", 7);
    let a = run(SimConfig::new(seed));
    let b = run(SimConfig::new(seed));
    assert!(
        a.trace_events > 1_000,
        "the run did too little: {:?}",
        a.stats
    );
    assert!(
        a.stats.publishes_ok > 10 && a.stats.polls_ok > 5,
        "the workload made no progress: {:?}",
        a.stats
    );
    assert_eq!(
        (a.trace_hash, a.trace_events),
        (b.trace_hash, b.trace_events),
        "seed {seed} is not deterministic.\nfirst tail:\n{}\nsecond tail:\n{}\nreplay: NABEDRA_SEED={seed} {REPLAY}",
        a.trace_tail.join("\n"),
        b.trace_tail.join("\n"),
    );
    assert_eq!(a.history, b.history, "seed {seed}: histories differ");
}

/// Different seeds explore different schedules.
#[test]
fn different_seeds_differ() {
    let a = run(SimConfig::new(11));
    let b = run(SimConfig::new(12));
    assert_ne!(a.trace_hash, b.trace_hash);
}

/// Runs seeds from a fixed start until one violation mentions `expect`.
fn find_planted(knobs: Knobs, expect: &[&str], label: &str) {
    let first = env_u64("NABEDRA_PLANTED_SEED", 1_000);
    let budget = env_u64("NABEDRA_PLANTED_SEEDS", 200);
    for seed in first..first + budget {
        let mut config = SimConfig::new(seed);
        config.knobs = knobs;
        let report = run(config);
        if report
            .violations
            .iter()
            .any(|violation| expect.iter().any(|name| violation.contains(name)))
        {
            eprintln!(
                "planted bug {label} caught at seed {seed} after {} seeds:\n  {}",
                seed - first + 1,
                report.violations.join("\n  ")
            );
            return;
        }
        if !report.passed() {
            eprintln!(
                "planted bug {label}: seed {seed} failed with other violations:\n  {}",
                report.violations.join("\n  ")
            );
        }
    }
    panic!("planted bug {label} was not caught in {budget} seeds from {first}");
}

#[test]
fn planted_commit_without_majority_is_caught() {
    find_planted(
        Knobs {
            ack_unreachable_appends: true,
            ..Knobs::default()
        },
        &[
            "MajorityReachedHighWatermark",
            "AppliedPrefixesAgree",
            "lost-write",
        ],
        "commit-without-majority",
    );
}

#[test]
fn planted_offset_reuse_after_dedup_is_caught() {
    find_planted(
        Knobs {
            reuse_offset_after_dedup: true,
            ..Knobs::default()
        },
        &["duplicate-offset", "offset-reassigned"],
        "offset-reuse-after-dedup",
    );
}

#[test]
fn planted_fsynced_data_loss_is_caught() {
    find_planted(
        Knobs {
            lose_fsynced_on_restart: true,
            ..Knobs::default()
        },
        &[
            "MonotonicEpoch",
            "MajorityReachedHighWatermark",
            "AppliedPrefixesAgree",
            "lost-write",
            "SingleLeader",
        ],
        "fsynced-data-loss",
    );
}
