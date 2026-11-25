use std::path::PathBuf;

use nabedra_check::Gen;

use super::toy::{self, Knobs};
use crate::sweep::{PlantedSearch, SeedRun, SweepConfig, parallel_map, sweep};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nabedra-sim-unit-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn config(name: &str, first: u64, count: u64) -> SweepConfig {
    let mut config = SweepConfig::new(name, first, count);
    config.artifacts = scratch(name);
    config.replay = "cargo test -p nabedra-sim --lib".into();
    config
}

#[test]
fn parallel_map_keeps_item_order() {
    let items: Vec<u64> = (0..97).collect();
    for jobs in [1, 3, 200] {
        assert_eq!(
            parallel_map(&items, jobs, |x| x * x),
            items.iter().map(|x| x * x).collect::<Vec<_>>()
        );
    }
    assert!(parallel_map(&[] as &[u64], 4, |x| *x).is_empty());
}

/// The same range gives the same report on one thread and on three, and the double run
/// finds every seed deterministic.
#[test]
fn a_clean_sweep_is_deterministic_and_independent_of_the_thread_count() {
    let mut one = config("clean-1", 1, 8);
    one.verify_replay = 1.0;
    let mut three = one.clone();
    three.jobs = 3;
    three.label = "clean-3".into();
    let a = sweep(&one, |seed, g| toy::run(seed, g, Knobs::default()));
    let b = sweep(&three, |seed, g| toy::run(seed, g, Knobs::default()));
    a.assert_passed();
    assert_eq!(a.replayed(), 8);
    assert_eq!(a.seeds, b.seeds);
    assert!(a.virtual_ms() >= 8 * 2_000);
    let seeds = a.evidence_seeds();
    assert_eq!((seeds.range, seeds.count), (Some([1, 8]), 8));
    assert!(seeds.failing.is_empty());
    assert!(
        a.summary()
            .contains("8 seeds from 1: 0 failed, 0 nondeterministic, 8 replayed")
    );
}

#[test]
fn only_the_requested_fraction_runs_twice() {
    let mut config = config("fraction", 10, 20);
    config.verify_replay = 0.1;
    assert_eq!(config.replayed(), 2);
    let report = sweep(&config, |seed, _| SeedRun {
        seed,
        trace_hash: seed,
        ..SeedRun::default()
    });
    let replayed: Vec<u64> = report
        .seeds
        .iter()
        .filter(|seed| seed.replayed)
        .map(|seed| seed.seed)
        .collect();
    assert_eq!(replayed, [10, 11]);
    report.assert_passed();
}

#[test]
fn a_leak_between_runs_is_a_determinism_failure_with_artifacts() {
    let mut config = config("leak", 3, 2);
    config.verify_replay = 0.5;
    let report = sweep(&config, |seed, g| {
        toy::run(
            seed,
            g,
            Knobs {
                leak_global_state: true,
                ..Knobs::default()
            },
        )
    });
    assert!(!report.passed());
    assert_eq!(report.nondeterministic(), [3]);
    assert!(report.failing().is_empty());
    assert_eq!(report.evidence_seeds().failing, [3]);
    let text = &report.failures[0];
    assert!(text.contains("seed 3 is not deterministic"), "{text}");
    assert!(
        text.contains("replay: NABEDRA_SEED=3 NABEDRA_SEEDS=1 cargo test"),
        "{text}"
    );
    let dir = config.artifacts.join("leak-seed-3");
    for file in [
        "summary.json",
        "history.jsonl",
        "trace-tail.txt",
        "trace-tail-replay.txt",
    ] {
        assert!(dir.join(file).is_file(), "{file} missing");
    }
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("summary.json")).unwrap()).unwrap();
    assert_eq!(summary["deterministic"], false);
    assert_ne!(summary["trace_hash"], summary["replay_trace_hash"]);
    let _ = std::fs::remove_dir_all(&config.artifacts);
}

#[test]
fn a_failing_seed_is_replayed_reported_and_shrunk() {
    let mut config = config("planted", 1, 30);
    config.jobs = 2;
    config.shrink = true;
    config.shrink_budget = 40;
    let knobs = Knobs {
        flag_duplicates: true,
        ..Knobs::default()
    };
    let report = sweep(&config, |seed, g| toy::run(seed, g, knobs));
    let failing = report.failing();
    assert!(
        !failing.is_empty(),
        "no seed in 1..=30 delivered a duplicate"
    );
    assert!(
        report.nondeterministic().is_empty(),
        "{:?}",
        report.failures
    );
    let seed = failing[0];
    let dir = config.artifacts.join(format!("planted-seed-{seed}"));
    let tape = crate::shrink::read_tape(&dir.join(format!("{seed}.tape"))).unwrap();
    // The shrunk tape still reproduces the violation kind.
    let replay = toy::run(seed, &mut Gen::from_tape(tape.clone()), knobs);
    assert!(
        replay
            .violations
            .iter()
            .any(|v| v.contains("DuplicateDelivered")),
        "{:?}",
        replay.violations
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("summary.json")).unwrap()).unwrap();
    assert!(summary["shrunk_tape_len"].as_u64().unwrap() <= summary["tape_len"].as_u64().unwrap());
    assert_eq!(summary["shrunk_tape_len"].as_u64(), Some(tape.len() as u64));
    assert!(
        report.failures[0].contains("shrunk tape"),
        "{}",
        report.failures[0]
    );
    let _ = std::fs::remove_dir_all(&config.artifacts);
}

#[test]
fn a_panicking_run_is_a_violation_not_a_crash() {
    let config = config("panic", 1, 2);
    let report = sweep(&config, |seed, g| {
        toy::run(
            seed,
            g,
            Knobs {
                panic: true,
                ..Knobs::default()
            },
        )
    });
    assert_eq!(report.failing(), [1, 2]);
    assert!(report.nondeterministic().is_empty());
    assert!(report.failures[0].contains("harness: panic: the toy harness was told to panic"));
    let _ = std::fs::remove_dir_all(&config.artifacts);
}

#[test]
fn a_tape_replaces_the_fresh_draws() {
    let mut config = config("tape", 4, 1);
    config.tape = Some(vec![3, 1, 4, 1, 5]);
    let report = sweep(&config, |seed, g| {
        let first = g.below(100);
        SeedRun {
            seed,
            trace_hash: first,
            ..SeedRun::default()
        }
    });
    assert_eq!(report.seeds[0].trace_hash, 3);
}

#[test]
#[cfg(not(tokio_unstable))]
#[should_panic(expected = "needs a seeded runtime")]
fn an_unseeded_build_refuses_a_required_seeded_sweep() {
    let mut config = config("unseeded", 1, 1);
    config.require_seeded = true;
    // This panic is the test's own, not that of a trace another test has open.
    crate::tla_trace::attributed(|| {
        sweep(&config, |seed, _| SeedRun {
            seed,
            ..SeedRun::default()
        })
    });
}

fn every_seventh(seed: u64) -> Vec<String> {
    if seed % 7 == 3 {
        vec![format!("t=1ms LostWrite: seed {seed}")]
    } else if seed.is_multiple_of(5) {
        vec!["t=1ms Other: noise".into()]
    } else {
        Vec::new()
    }
}

#[test]
fn the_planted_search_finds_the_lowest_seed_on_any_thread_count() {
    for jobs in [1, 4, 16] {
        let mut search = PlantedSearch::new("seventh", 100, 50, &["LostWrite"]);
        search.jobs = jobs;
        let found = search.run(every_seventh).unwrap();
        assert_eq!((found.seed, found.tried), (101, 2), "jobs {jobs}");
    }
    // Seeds 104 to 107 hold no multiple of 7 plus 3.
    let err = PlantedSearch::new("seventh", 104, 4, &["LostWrite"])
        .run(every_seventh)
        .unwrap_err();
    assert!(err.contains("was not caught in 4 seeds from 104"), "{err}");
    let panics = PlantedSearch::new("panics", 1, 3, &["harness"])
        .run(|_| panic!("boom"))
        .unwrap();
    assert_eq!(panics.seed, 1);
}

#[test]
fn the_planted_search_catches_a_planted_bug_in_the_toy_cluster() {
    let mut search = PlantedSearch::new("duplicate", 1, 40, &["DuplicateDelivered"]);
    search.jobs = 2;
    let found = search
        .run(|seed| {
            let knobs = Knobs {
                flag_duplicates: true,
                ..Knobs::default()
            };
            toy::run(seed, &mut crate::sweep::workload_gen(seed), knobs).violations
        })
        .unwrap();
    assert!(found.tried <= 40);
    // The clean toy never reports it on the same seed.
    let clean = toy::run(
        found.seed,
        &mut crate::sweep::workload_gen(found.seed),
        Knobs::default(),
    );
    assert!(clean.passed(), "{:?}", clean.violations);
}

#[test]
fn planted_search_matches_the_invariant_name() {
    let search = PlantedSearch::new("exact", 1, 3, &["LostWrite"]);
    let found = search
        .run(|seed| match seed {
            1 => vec!["t=1ms Other: mentions LostWrite".into()],
            2 => vec!["t=1ms PotentialLostWrite: different invariant".into()],
            _ => vec!["t=2ms LostWrite: confirmed".into()],
        })
        .unwrap();
    assert_eq!((found.seed, found.tried), (3, 3));
}

#[test]
fn the_maximum_seed_is_included_in_the_sweep_and_planted_search() {
    let config = config("max-seed", u64::MAX, 1);
    let report = sweep(&config, |_, _| SeedRun::default());
    assert_eq!(report.evidence_seeds().range, Some([u64::MAX, u64::MAX]));

    let mut search = PlantedSearch::new("max", u64::MAX - 1, 5, &["LostWrite"]);
    search.jobs = 4;
    let found = search
        .run(|seed| {
            if seed == u64::MAX {
                vec!["LostWrite: last seed".into()]
            } else {
                Vec::new()
            }
        })
        .unwrap();
    assert_eq!((found.seed, found.tried), (u64::MAX, 2));
}

#[test]
fn a_replay_with_only_different_history_bytes_fails() {
    let mut config = config("history-diff", 1, 1);
    config.verify_replay = 1.0;
    let calls = std::sync::atomic::AtomicU8::new(0);
    let report = sweep(&config, |_, _| SeedRun {
        history: vec![calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed)],
        ..SeedRun::default()
    });
    assert_eq!(report.nondeterministic(), [1]);
    let _ = std::fs::remove_dir_all(&config.artifacts);
}
