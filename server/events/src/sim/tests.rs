//! Simulation tests. Run with
//! `RUSTFLAGS="--cfg tokio_unstable" cargo test --features sim --lib sim::`.
//!
//! - `NABEDRA_SEED`: the first seed (default 1).
//! - `NABEDRA_SEEDS`: how many consecutive seeds the sweep runs (default 20).
//! - `NABEDRA_SIM_HISTORY_DIR`: also write every run's history there.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::cluster::{Knobs, Report, SimConfig, run};
use super::history::History;

/// The default window of every planted-bug search. Each bug must be caught well inside
/// 10,000 seeds (testing plan phase 6); the README records the catching seeds.
const PLANTED_FIRST: u64 = 1_000;
const PLANTED_BUDGET: u64 = 200;

/// A seed in which a second node leads within a term another node already led (the first
/// of seeds 1 to 399 that does, after the move onto `nabedra_rt` and `nabedra_sim`).
const HANDOFF_SEED: u64 = 397;

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
    assert_client_history(&a);
}

fn assert_client_history(report: &Report) {
    let mut requests = BTreeMap::new();
    let mut receipts = BTreeMap::new();
    let mut retired = BTreeSet::new();
    let mut dedup_hits = 0;
    for event in &report.history {
        if event.kind == "info" {
            retired.insert(event.process);
        }
        if event.kind == "invoke" {
            assert!(
                !retired.contains(&event.process),
                "retired process at {event:?}"
            );
        }
        match (event.f, event.kind) {
            ("publish", "invoke") => {
                let first_id = event.value[0].as_str().unwrap();
                let request = event.request_id.as_str();
                assert_eq!(*requests.entry(first_id).or_insert(request), request);
            }
            ("publish", "ok") => {
                let dedup = event.value["dedup"].as_bool().unwrap();
                let offsets = &event.value["offsets"];
                if let Some(previous) = receipts.insert(&event.request_id, offsets) {
                    assert!(dedup, "replayed receipt at {event:?}");
                    assert_eq!(previous, offsets);
                    dedup_hits += 1;
                }
            }
            ("poll" | "ack", _) => {
                assert_eq!(event.value["subscription"], "s");
            }
            _ => {}
        }
    }
    assert!(dedup_hits > 0 && !retired.is_empty());
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
    assert!(a.passed(), "{}", describe(&a, "different-seeds"));
    assert!(b.passed(), "{}", describe(&b, "different-seeds"));
    assert_ne!(a.trace_hash, b.trace_hash);
}

#[test]
fn same_term_leader_handoff_preserves_safety() {
    let seed = env_u64("NABEDRA_HANDOFF_SEED", HANDOFF_SEED);
    let report = run(SimConfig::new(seed));
    assert!(report.passed(), "{}", describe(&report, "leader-handoff"));
    assert!(
        report.stats.same_term_handoffs > 0,
        "seed {seed} no longer hands leadership over within a term: {:?}",
        report.stats
    );
}

/// Runs seeds from a fixed start until one violation mentions `expect`, then runs the
/// catching seed again and requires the same trace, history and violations.
fn find_planted(knobs: Knobs, expect: &[&str], label: &str) {
    find_planted_with(|config| config.knobs = knobs, expect, label);
}

fn find_planted_with(configure: impl Fn(&mut SimConfig), expect: &[&str], label: &str) {
    let first = env_u64("NABEDRA_PLANTED_SEED", PLANTED_FIRST);
    let budget = env_u64("NABEDRA_PLANTED_SEEDS", PLANTED_BUDGET);
    let planted = |seed| {
        let mut config = SimConfig::new(seed);
        configure(&mut config);
        run(config)
    };
    for seed in first..first + budget {
        let report = planted(seed);
        if report
            .violations
            .iter()
            .any(|violation| expect.iter().any(|name| violation.contains(name)))
        {
            eprintln!(
                "planted bug {label} caught at seed {seed} after {} seeds, trace hash {:016x} ({} events):\n  {}",
                seed - first + 1,
                report.trace_hash,
                report.trace_events,
                report.violations.join("\n  ")
            );
            let replay = planted(seed);
            assert_eq!(
                (replay.trace_hash, replay.trace_events),
                (report.trace_hash, report.trace_events),
                "planted bug {label}: seed {seed} is not deterministic.\nfirst tail:\n{}\nsecond tail:\n{}",
                report.trace_tail.join("\n"),
                replay.trace_tail.join("\n"),
            );
            assert_eq!(
                replay.history, report.history,
                "planted bug {label}: seed {seed}: histories differ"
            );
            assert_eq!(
                replay.violations, report.violations,
                "planted bug {label}: seed {seed}: violations differ"
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
        &[
            "queue checker duplicate-offset",
            "queue checker duplicate-event",
        ],
        "offset-reuse-after-dedup",
    );
}

#[test]
fn planted_missing_redelivery_after_heal_is_caught() {
    find_planted(
        Knobs {
            delay_redelivery_after_heal: true,
            ..Knobs::default()
        },
        &["queue checker redelivery-missing"],
        "missing-redelivery-after-heal",
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

/// EVENTS-1: the seed sweep with volume wipes. A crashed node restarts on an empty
/// volume now and then; the empty-volume rule keeps it out of the group, so every safety
/// invariant holds. Before the fix (the planted reopen below, which is what the harness
/// and `Control::reopen` did), seeds 1 to 40 broke a safety invariant in 8 seeds.
#[test]
fn wipe_sweep() {
    let first = env_u64("NABEDRA_SEED", 1);
    let count = env_u64("NABEDRA_SEEDS", 20);
    let mut failures = Vec::new();
    let mut lost = 0;
    for seed in first..first + count {
        let mut config = SimConfig::new(seed);
        config.wipes = true;
        let report = run(config);
        lost += report.stats.lost_replicas;
        if !report.passed() {
            eprintln!("{}", describe(&report, "wipes"));
            failures.push(seed);
        }
    }
    eprintln!(
        "events sim wipes: {count} seeds from {first}: {} failed; {lost} replicas lost to wipes",
        failures.len()
    );
    assert!(lost > 0, "no seed wiped a volume");
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

#[test]
fn planted_rejoin_of_a_wiped_voter_is_caught() {
    find_planted_with(
        |config| {
            config.wipes = true;
            config.knobs.rejoin_wiped_voter = true;
        },
        &[
            "MajorityReachedHighWatermark",
            "AppliedPrefixesAgree",
            "ConsistentCommittedData",
            "lost-write",
        ],
        "rejoin-wiped-voter",
    );
}

/// EVENTS-2, EVENTS-3, EVENTS-8: the seed sweep with the retention loop (3 s), guarded
/// time stamps and clock jumps within and beyond the stated bound for one clock at a time.
/// Every removal is checked against EVT-G08 in true time.
#[test]
fn retention_sweep() {
    let first = env_u64("NABEDRA_SEED", 1);
    let count = env_u64("NABEDRA_SEEDS", 20);
    let mut failures = Vec::new();
    let (mut removed, mut jumps, mut steps) = (0, 0, 0);
    for seed in first..first + count {
        let mut config = SimConfig::new(seed);
        config.retention_ms = Some(3_000);
        let report = run(config);
        removed += report.stats.retained_removed;
        jumps += report.stats.clock_jumps;
        steps += report.stats.clock_steps;
        if !report.passed() {
            eprintln!("{}", describe(&report, "retention"));
            failures.push(seed);
        }
    }
    eprintln!(
        "events sim retention: {count} seeds from {first}: {} failed; {removed} events removed, {jumps} clock jumps, {steps} clock steps",
        failures.len()
    );
    assert!(removed > 0, "retention removed nothing");
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}

#[test]
fn planted_unguarded_clock_is_caught() {
    find_planted_with(
        |config| {
            config.retention_ms = Some(3_000);
            config.knobs.unguarded_clock = true;
        },
        &["EVT-G08 early-removal"],
        "unguarded-clock",
    );
}

mod checkers {
    use nabedra_sim::RaftObserver;
    use openraft::{CommittedLeaderId, LogId, Vote};
    use serde_json::json;
    use uuid::Uuid;

    use super::super::history::{self, FinalLog, History};
    use super::super::{log_id, vote};

    const G: &str = "p0";

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

    fn publish(ids: &[u128]) -> serde_json::Value {
        json!(ids.iter().copied().map(id).collect::<Vec<_>>())
    }

    fn receipt(events: &[(u128, u64)]) -> serde_json::Value {
        let (ids, offsets): (Vec<_>, Vec<_>) = events.iter().copied().unzip();
        json!({ "event_ids": publish(&ids), "offsets": offsets })
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
            ("invoke", "publish", publish(&[1, 2])),
            ("ok", "publish", receipt(&[(1, 1), (2, 2)])),
            ("invoke", "publish", publish(&[1, 2])),
            ("ok", "publish", receipt(&[(1, 1), (2, 2)])),
            ("ok", "poll", poll(0, &[(1, 1), (2, 2)])),
        ]);
        assert_eq!(
            history::check(&h, &log(&[(1, 1), (2, 2)]), 2, 1),
            Vec::<String>::new()
        );
    }

    #[test]
    fn known_bad_histories_are_flagged() {
        let cases: &[(&str, History, FinalLog, u64)] = &[
            (
                "lost-write",
                history(&[
                    ("invoke", "publish", publish(&[1])),
                    ("ok", "publish", receipt(&[(1, 1)])),
                ]),
                log(&[]),
                0,
            ),
            (
                "duplicate-offset",
                history(&[
                    ("invoke", "publish", publish(&[1, 2])),
                    ("ok", "publish", receipt(&[(1, 1)])),
                    ("ok", "publish", receipt(&[(2, 1)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "offset-reassigned",
                history(&[
                    ("invoke", "publish", publish(&[1])),
                    ("ok", "publish", receipt(&[(1, 1)])),
                    ("ok", "publish", receipt(&[(1, 2)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "gap",
                history(&[("invoke", "publish", publish(&[1, 2]))]),
                log(&[(1, 1), (3, 2)]),
                3,
            ),
            ("invented-event", history(&[]), log(&[(1, 9)]), 1),
            (
                "poll-order",
                history(&[
                    ("invoke", "publish", publish(&[1, 2])),
                    ("ok", "poll", poll(0, &[(2, 2)])),
                ]),
                log(&[(1, 1), (2, 2)]),
                2,
            ),
            (
                "split-batch",
                history(&[
                    ("invoke", "publish", publish(&[1, 2])),
                    ("ok", "publish", receipt(&[(1, 1), (2, 3)])),
                ]),
                log(&[(1, 1), (2, 2), (3, 2)]),
                3,
            ),
            (
                "incomplete-receipt",
                history(&[
                    ("invoke", "publish", publish(&[1, 2])),
                    ("ok", "publish", receipt(&[(1, 1)])),
                    ("ok", "poll", poll(0, &[(1, 1), (2, 2)])),
                ]),
                log(&[(1, 1), (2, 2)]),
                2,
            ),
            (
                "partial-batch",
                history(&[
                    ("invoke", "publish", publish(&[1, 2])),
                    ("info", "publish", serde_json::Value::Null),
                    ("ok", "poll", poll(0, &[(1, 1)])),
                ]),
                log(&[(1, 1)]),
                1,
            ),
            (
                "drain-incomplete",
                history(&[("invoke", "publish", publish(&[1]))]),
                log(&[(1, 1)]),
                0,
            ),
        ];
        for (expected, h, final_log, checkpoint) in cases {
            let anomalies = history::check(h, final_log, *checkpoint, 1);
            assert!(
                anomalies.iter().any(|a| a.contains(expected)),
                "{expected} not flagged: {anomalies:?}"
            );
        }
    }

    /// The Kafka invariants now live in `nabedra_sim::RaftObserver`; these cases feed it
    /// openraft 0.9 ids through the harness's conversions, as the hooks do.
    #[test]
    fn kafka_invariants_flag_known_bad_observations() {
        let same_term = vote(Vote::new_committed(3, 1));
        let violations = |o: &RaftObserver| o.violations().to_vec();
        let mut o = RaftObserver::default();
        o.observe(G, 1, same_term, true, Some(1), 0);
        o.observe(G, 2, same_term, true, Some(1), 0);
        assert!(
            violations(&o).iter().any(|v| v.contains("SingleLeader")),
            "{:?}",
            violations(&o)
        );

        let mut o = RaftObserver::default();
        o.observe(G, 1, same_term, true, Some(1), 0);
        o.observe(G, 2, vote(Vote::new_committed(3, 2)), true, Some(1), 0);
        assert!(violations(&o).is_empty(), "{:?}", violations(&o));

        let mut o = RaftObserver::default();
        o.observe(G, 1, vote(Vote::new(5, 2)), false, None, 0);
        o.observe(G, 1, vote(Vote::new(5, 1)), false, None, 0);
        assert!(
            violations(&o).iter().any(|v| v.contains("MonotonicEpoch")),
            "{:?}",
            violations(&o)
        );

        let mut o = RaftObserver::default();
        o.observe(G, 1, same_term, false, Some(9), 0);
        o.observe(G, 1, same_term, false, Some(8), 0);
        assert!(
            violations(&o)
                .iter()
                .any(|v| v.contains("MonotonicHighWatermark")),
            "{:?}",
            violations(&o)
        );

        let first = log_id(LogId::new(CommittedLeaderId::new(1, 1), 1));
        let next = log_id(LogId::new(CommittedLeaderId::new(1, 1), 2));
        let conflict = log_id(LogId::new(CommittedLeaderId::new(1, 2), 1));
        let mut o = RaftObserver::new(3);
        o.appended(G, 1, first);
        o.appended(G, 2, first);
        o.applied(G, 1, first, 0xaa, 0);
        o.applied(G, 2, first, 0xbb, 0);
        assert!(
            violations(&o)
                .iter()
                .any(|v| v.contains("AppliedPrefixesAgree")),
            "{:?}",
            violations(&o)
        );

        let mut o = RaftObserver::new(3);
        o.appended(G, 1, first);
        o.appended(G, 2, conflict);
        o.applied(G, 1, first, 0xaa, 0);
        // The next entry reaches a majority before the next metrics sample.
        o.appended(G, 1, next);
        o.appended(G, 2, next);
        o.applied(G, 1, next, 0xaa, 0);
        assert!(
            violations(&o)
                .iter()
                .any(|v| v.contains("MajorityReachedHighWatermark")),
            "{:?}",
            violations(&o)
        );
        let mut o = RaftObserver::new(3);
        o.appended(G, 1, first);
        o.appended(G, 2, first);
        o.applied(G, 1, first, 0xaa, 0);
        assert_eq!(violations(&o), Vec::<String>::new());
    }

    /// openraft 0.9's ids map onto the observer's with the same display form and order.
    #[test]
    fn openraft_ids_map_onto_the_observer() {
        let id = LogId::new(CommittedLeaderId::new(4, 2), 17);
        assert_eq!(log_id(id).to_string(), id.to_string());
        let votes = [
            Vote::new(3, 2),
            Vote::new_committed(3, 2),
            Vote::new(4, 1),
            Vote::new_committed(4, 1),
            Vote::new(4, 3),
        ];
        for a in votes {
            assert_eq!(vote(a).to_string(), a.to_string());
            for b in votes {
                assert_eq!(
                    vote(a).partial_cmp(&vote(b)),
                    a.partial_cmp(&b),
                    "{a} vs {b}"
                );
            }
        }
    }
}
