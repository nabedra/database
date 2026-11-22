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
    assert!(count > 0, "NABEDRA_SEEDS must be positive");
    let last = first
        .checked_add(count - 1)
        .expect("seed range overflows u64");
    let replay = std::env::var_os("NABEDRA_SIM_VERIFY_REPLAY").is_some();
    let keep = std::env::var_os("NABEDRA_SIM_HISTORY_DIR").map(PathBuf::from);
    let mut failures = Vec::new();
    let mut totals = (0usize, 0usize, 0u64);
    for seed in first..=last {
        let report = run(SimConfig::new(seed));
        if count == 1 {
            eprintln!(
                "seed {seed} trace hash {:016x} ({} events)",
                report.trace_hash, report.trace_events
            );
        }
        if let Some(dir) = &keep {
            write_history(&report, "sweep", dir.clone());
        }
        totals.0 += report.stats.publishes_ok;
        totals.1 += report.stats.final_events;
        totals.2 += report.stats.crashes;
        if !report.passed() {
            eprintln!("{}", describe(&report, "sweep"));
            failures.push(seed);
        } else if replay {
            let repeated = run(SimConfig::new(seed));
            assert_same_trace(&report, &repeated);
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
    let seed = 7;
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
    assert_same_trace(&a, &b);
}

fn assert_same_trace(a: &Report, b: &Report) {
    let seed = a.seed;
    assert!(a.passed(), "{}", describe(a, "determinism-first"));
    assert!(b.passed(), "{}", describe(b, "determinism-replay"));
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

#[test]
fn same_term_leader_handoff_preserves_safety() {
    // Seed 188 elects n1 and n2 with different leader IDs in term 5.
    let report = run(SimConfig::new(188));
    assert!(report.passed(), "{}", describe(&report, "leader-handoff"));
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

mod checkers {
    use openraft::{CommittedLeaderId, LogId, Vote};
    use serde_json::json;
    use uuid::Uuid;

    use super::super::cluster::Observer;
    use super::super::history::{self, FinalLog, History};

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn history(ops: &[(&'static str, &'static str, serde_json::Value)]) -> History {
        let mut history = History::default();
        for (kind, f, value) in ops {
            history.push(kind, 0, f, value.clone(), "r", 1, 0);
        }
        history
    }

    fn publish(events: &[(u128, u64)]) -> serde_json::Value {
        history::publish_value(&events.iter().map(|(n, o)| (id(*n), *o)).collect::<Vec<_>>())
    }

    fn log(events: &[(u64, u128)]) -> FinalLog {
        events.iter().map(|(o, n)| (*o, id(*n))).collect()
    }

    fn poll(after: u64, events: &[(u64, u128)]) -> serde_json::Value {
        let events: Vec<_> = events
            .iter()
            .map(|(o, n)| json!([o, id(*n).to_string()]))
            .collect();
        json!({ "after": after, "events": events })
    }

    #[test]
    fn a_clean_history_passes() {
        let h = history(&[
            ("invoke", "publish", publish(&[(1, 1), (2, 2)])),
            ("ok", "publish", publish(&[(1, 1), (2, 2)])),
            ("invoke", "publish", publish(&[(1, 1), (2, 2)])),
            ("ok", "publish", publish(&[(1, 1), (2, 2)])),
            ("ok", "poll", poll(0, &[(1, 1), (2, 2)])),
        ]);
        assert_eq!(
            history::check(&h, &log(&[(1, 1), (2, 2)]), 2),
            Vec::<String>::new()
        );
    }

    #[test]
    fn known_bad_histories_are_flagged() {
        let cases: &[(&str, History, FinalLog, u64)] = &[
            (
                "lost-write",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0)])),
                    ("ok", "publish", publish(&[(1, 1)])),
                ]),
                log(&[]),
                0,
            ),
            (
                "duplicate-offset",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0), (2, 0)])),
                    ("ok", "publish", publish(&[(1, 1)])),
                    ("ok", "publish", publish(&[(2, 1)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "offset-reassigned",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0)])),
                    ("ok", "publish", publish(&[(1, 1)])),
                    ("ok", "publish", publish(&[(1, 2)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "gap",
                history(&[("invoke", "publish", publish(&[(1, 0), (2, 0)]))]),
                log(&[(1, 1), (3, 2)]),
                3,
            ),
            ("invented-event", history(&[]), log(&[(1, 9)]), 1),
            (
                "poll-order",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0), (2, 0)])),
                    ("ok", "poll", poll(0, &[(2, 2)])),
                ]),
                log(&[(1, 1), (2, 2)]),
                2,
            ),
            (
                "split-batch",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0), (2, 0)])),
                    ("ok", "publish", publish(&[(1, 1), (2, 3)])),
                ]),
                log(&[(1, 1), (2, 2), (3, 2)]),
                3,
            ),
            (
                "incomplete-receipt",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0), (2, 0)])),
                    ("ok", "publish", publish(&[(1, 1)])),
                    ("ok", "poll", poll(0, &[(1, 1), (2, 2)])),
                ]),
                log(&[(1, 1), (2, 2)]),
                2,
            ),
            (
                "partial-batch",
                history(&[
                    ("invoke", "publish", publish(&[(1, 0), (2, 0)])),
                    ("info", "publish", serde_json::Value::Null),
                    ("ok", "poll", poll(0, &[(1, 1)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "drain-incomplete",
                history(&[("invoke", "publish", publish(&[(1, 0)]))]),
                log(&[(1, 1)]),
                0,
            ),
        ];
        for (expected, h, final_log, checkpoint) in cases {
            let anomalies = history::check(h, final_log, *checkpoint);
            assert!(
                anomalies.iter().any(|a| a.contains(expected)),
                "{expected} not flagged: {anomalies:?}"
            );
        }
    }

    #[test]
    fn kafka_invariants_flag_known_bad_observations() {
        let same_term = Vote::new_committed(3, 1);
        let mut o = Observer::default();
        o.observe_for_test(1, same_term, true, Some(1));
        o.observe_for_test(2, same_term, true, Some(1));
        assert!(
            o.violations.iter().any(|v| v.contains("SingleLeader")),
            "{:?}",
            o.violations
        );

        let mut o = Observer::default();
        o.observe_for_test(1, same_term, true, Some(1));
        o.observe_for_test(2, Vote::new_committed(3, 2), true, Some(1));
        assert!(o.violations.is_empty(), "{:?}", o.violations);

        let mut o = Observer::default();
        o.observe_for_test(1, Vote::new(5, 2), false, None);
        o.observe_for_test(1, Vote::new(5, 1), false, None);
        assert!(
            o.violations.iter().any(|v| v.contains("MonotonicEpoch")),
            "{:?}",
            o.violations
        );

        let mut o = Observer::default();
        o.observe_for_test(1, same_term, false, Some(9));
        o.observe_for_test(1, same_term, false, Some(8));
        assert!(
            o.violations
                .iter()
                .any(|v| v.contains("MonotonicHighWatermark")),
            "{:?}",
            o.violations
        );

        let first = LogId::new(CommittedLeaderId::new(1, 1), 1);
        let next = LogId::new(CommittedLeaderId::new(1, 1), 2);
        let conflict = LogId::new(CommittedLeaderId::new(1, 2), 1);
        let mut o = Observer::new(3);
        o.appended(1, first);
        o.appended(2, first);
        o.applied(1, first, 0xaa, 0);
        o.applied(2, first, 0xbb, 0);
        assert!(
            o.violations
                .iter()
                .any(|v| v.contains("AppliedPrefixesAgree")),
            "{:?}",
            o.violations
        );

        let mut o = Observer::new(3);
        o.appended(1, first);
        o.appended(2, conflict);
        o.applied(1, first, 0xaa, 0);
        // The next entry reaches a majority before the next metrics sample.
        o.appended(1, next);
        o.appended(2, next);
        o.applied(1, next, 0xaa, 0);
        assert!(
            o.violations
                .iter()
                .any(|v| v.contains("MajorityReachedHighWatermark")),
            "{:?}",
            o.violations
        );
        let mut o = Observer::new(3);
        o.appended(1, first);
        o.appended(2, first);
        o.applied(1, first, 0xaa, 0);
        assert_eq!(o.violations, Vec::<String>::new());
    }
}
