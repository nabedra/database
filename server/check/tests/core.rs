//! Tests for the generators, the shrinker, the property runner, the config randomizer and
//! evidence records.

use std::panic::{self, AssertUnwindSafe};

use nabedra_check::config::{self, Knob};
use nabedra_check::evidence::{self, Artifact, Outcome, Record, Seeds};
use nabedra_check::prop::{self, Config};
use nabedra_check::rng::SplitMix64;
use nabedra_check::{Gen, generator, r#gen};

fn cfg(cases: u64, seed: u64) -> Config {
    Config { cases, seed: Some(seed), max_shrink_steps: prop::DEFAULT_MAX_SHRINK_STEPS }
}

/// Runs `check_with` and returns its panic message, or `None` if every case passed.
fn failure(c: Config, name: &str, f: impl Fn(&mut Gen) -> Result<(), String>) -> Option<String> {
    let out = panic::catch_unwind(AssertUnwindSafe(|| prop::check_with(c, name, f)));
    out.err().map(|p| {
        p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap()
    })
}

fn line<'a>(msg: &'a str, key: &str) -> &'a str {
    msg.lines().find_map(|l| l.trim().strip_prefix(key)).unwrap_or_else(|| panic!("no `{key}` in:\n{msg}"))
}

fn draws(g: &mut Gen) -> (u64, u64, i64, bool, bool, u8, Vec<u8>, String, Vec<u64>) {
    (
        g.u64(),
        g.below(17),
        g.range_i64(-5, 5),
        g.bool(),
        g.chance(1, 3),
        *g.pick(&[1u8, 2, 3]),
        g.bytes(12),
        g.ascii(12),
        g.vec(6, |g| g.below(1000)),
    )
}

#[test]
fn splitmix_matches_reference_values() {
    // Reference outputs of SplitMix64 seeded with 0 (same stream as kv's test Rng).
    let mut r = SplitMix64::new(0);
    assert_eq!(r.next_u64(), 0xe220_a839_7b1d_cdaf);
    assert_eq!(r.next_u64(), 0x6e78_9e6a_a1b9_65f4);
}

#[test]
fn same_seed_gives_same_draws_and_tape_replays_them() {
    for seed in [0, 1, 42, u64::MAX] {
        let mut a = Gen::from_seed(seed);
        let mut b = generator::Gen::from_seed(seed);
        let da = draws(&mut a);
        assert_eq!(da, draws(&mut b), "seed {seed}");
        assert_eq!(a.seed(), seed);
        let mut r = r#gen::Gen::from_tape(a.tape().to_vec());
        assert_eq!(da, draws(&mut r), "replay of seed {seed}");
        assert_eq!(r.tape(), a.tape());
    }
    assert_ne!(draws(&mut Gen::from_seed(1)), draws(&mut Gen::from_seed(2)));
}

#[test]
fn exhausted_tape_gives_minimal_values() {
    let mut g = Gen::from_tape(Vec::new());
    assert_eq!(draws(&mut g), (0, 0, -5, false, false, 1, vec![], String::new(), vec![]));
    assert!(!Gen::from_tape(vec![]).chance(0, 5));
    assert!(Gen::from_tape(vec![]).chance(5, 5), "chance(den, den) is always true");
}

#[test]
fn vector_accepts_the_largest_length_bound() {
    let mut g = Gen::from_tape(vec![2]);
    assert_eq!(g.vec(usize::MAX, Gen::bool), vec![false, false]);
    assert!(Gen::from_tape(vec![]).bytes(usize::MAX).is_empty());
}

#[test]
fn draws_stay_in_bounds() {
    let mut g = Gen::from_seed(7);
    for _ in 0..10_000 {
        assert!(g.below(3) < 3);
        let v = g.range_i64(-2, 2);
        assert!((-2..=2).contains(&v));
        assert!(g.ascii(4).bytes().all(|b| (0x20..=0x7e).contains(&b)));
        assert!(g.vec(3, |g| g.bool()).len() <= 3);
    }
    let full = g.range_i64(i64::MIN, i64::MAX);
    let _ = full;
    // Out-of-range tape values are reduced into range on replay.
    assert_eq!(Gen::from_tape(vec![10]).below(3), 1);
}

#[test]
fn sum_at_least_100_shrinks_to_one_element_near_100() {
    for seed in 0..20 {
        let msg = failure(cfg(256, seed), "sum", |g| {
            let v = g.vec(20, |g| g.below(1000));
            let sum: u64 = v.iter().sum();
            if sum >= 100 { Err(format!("{v:?}")) } else { Ok(()) }
        })
        .expect("a random vec of up to 20 values below 1000 sums to 100 or more");
        let failing = line(&msg, "failure: ");
        assert_eq!(failing, "[100]", "seed {seed}:\n{msg}");
        assert_eq!(line(&msg, "shrunk tape length: ").split(' ').next(), Some("2"), "{msg}");
    }
}

#[test]
fn seven_then_three_shrinks_to_exactly_7_3() {
    for seed in 0..20 {
        let msg = failure(cfg(1000, seed), "seven-then-three", |g| {
            let v = g.vec(30, |g| g.below(10));
            let after_seven = v.iter().position(|&x| x == 7).map(|i| &v[i + 1..]);
            if after_seven.is_some_and(|rest| rest.contains(&3)) { Err(format!("{v:?}")) } else { Ok(()) }
        })
        .expect("the property fails for some case");
        assert_eq!(line(&msg, "failure: "), "[7, 3]", "seed {seed}:\n{msg}");
    }
}

#[test]
fn shrinking_is_bounded_by_max_shrink_steps() {
    let prop = |g: &mut Gen| {
        let v = g.vec(20, |g| g.below(1000));
        if v.iter().sum::<u64>() >= 100 { Err(format!("{v:?}")) } else { Ok(()) }
    };
    let none = failure(Config { max_shrink_steps: 0, ..cfg(256, 3) }, "unshrunk", prop).unwrap();
    assert!(line(&none, "shrunk tape length: ").contains("0 shrink runs"), "{none}");
    let some = failure(Config { max_shrink_steps: 5, ..cfg(256, 3) }, "five", prop).unwrap();
    assert!(line(&some, "shrunk tape length: ").contains("5 shrink runs"), "{some}");
    assert!(prop::simpler(&[u64::MAX], &[0, 0]));
    assert!(prop::simpler(&[1, u64::MAX], &[2, 0]));
    assert!(!prop::simpler(&[0, 0], &[u64::MAX]));
    assert!(!prop::simpler(&[2, 0], &[1, u64::MAX]));
    assert!(!prop::simpler(&[0], &[0]));
}

#[test]
fn shrinking_preserves_the_seed_of_the_failing_case() {
    let seed = 42;
    let msg = failure(cfg(1, seed), "seeded-property", |g| {
        let value = g.u64();
        if g.seed() == seed {
            Err(format!("seed {seed} with value {value}"))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert_eq!(line(&msg, "failure: "), "seed 42 with value 0");
}

#[test]
fn report_names_property_seed_case_and_replay_line() {
    let msg = failure(cfg(256, 99), "big-value", |g| {
        let v = g.below(1000);
        if v >= 900 { Err(format!("{v} >= 900")) } else { Ok(()) }
    })
    .unwrap();
    assert!(msg.starts_with("property `big-value` failed"), "{msg}");
    assert_eq!(line(&msg, "failure: "), "900 >= 900", "{msg}");
    let seed = line(&msg, "seed: ").split(' ').next().unwrap();
    assert_eq!(line(&msg, "replay: "), format!("NABEDRA_SEED={seed} NABEDRA_CASES=1"));
    assert!(line(&msg, "case: ").ends_with("of 256"), "{msg}");

    // The printed seed alone reproduces the failure as case 0.
    let seed: u64 = seed.parse().unwrap();
    let again = failure(cfg(1, seed), "big-value", |g| {
        let v = g.below(1000);
        if v >= 900 { Err(format!("{v} >= 900")) } else { Ok(()) }
    })
    .expect("replaying the printed seed fails again");
    assert!(line(&again, "case: ").starts_with("0 of 1"), "{again}");
    assert_eq!(line(&again, "seed: "), line(&msg, "seed: "));
}

#[test]
fn panicking_property_is_a_failure_reported_with_its_seed() {
    let msg = failure(cfg(256, 5), "panics", |g| {
        let v = g.vec(10, |g| g.below(100));
        if v.len() >= 3 {
            let idx = v.len() + 1;
            let _ = v[idx]; // index out of bounds
        }
        Ok(())
    })
    .unwrap();
    assert!(msg.starts_with("property `panics` failed"), "{msg}");
    let fail = line(&msg, "failure: ");
    assert!(fail.starts_with("panic: panicked at") && fail.contains("core.rs"), "{msg}");
    assert!(msg.contains("index out of bounds"), "{msg}");
    assert!(line(&msg, "replay: ").starts_with("NABEDRA_SEED="), "{msg}");
    // Shrunk to the shortest vec that panics: length 3, all zeros.
    assert!(line(&msg, "shrunk tape length: ").ends_with("[3, 0, 0, 0]"), "{msg}");
}

#[test]
fn passing_property_does_not_panic() {
    assert!(failure(cfg(500, 1), "fine", |g| if g.below(10) < 10 { Ok(()) } else { Err("x".into()) }).is_none());
}

#[test]
fn parse_u64_accepts_decimal_and_hex() {
    assert_eq!(prop::parse_u64("42"), Some(42));
    assert_eq!(prop::parse_u64(" 0x2A "), Some(42));
    assert_eq!(prop::parse_u64("1_000"), Some(1000));
    assert_eq!(prop::parse_u64("seven"), None);
    assert_eq!(prop::case_seed(123, 0), 123);
    assert_ne!(prop::case_seed(123, 1), prop::case_seed(123, 2));
}

const KNOBS: &[Knob] = &[
    Knob::Bool("direct_oltp"),
    Knob::Choice("eviction", &["lru", "lfu", "none"]),
    Knob::IntRange("workers", 1, 8),
    Knob::IntRange("batch", -3, 3),
];

#[test]
fn config_sample_is_deterministic_per_seed_and_in_range() {
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..200 {
        let a = config::sample(&mut Gen::from_seed(seed), KNOBS);
        assert_eq!(a, config::sample(&mut Gen::from_seed(seed), KNOBS), "seed {seed}");
        assert_eq!(a.len(), 4);
        assert!(a["direct_oltp"].is_boolean());
        assert!(["lru", "lfu", "none"].contains(&a["eviction"].as_str().unwrap()));
        assert!((1..=8).contains(&a["workers"].as_i64().unwrap()));
        assert!((-3..=3).contains(&a["batch"].as_i64().unwrap()));
        seen.insert(config::describe(&a));
    }
    assert!(seen.len() > 50, "200 seeds give varied configurations, got {}", seen.len());
    let min = config::sample(&mut Gen::from_tape(vec![]), KNOBS);
    assert_eq!(config::describe(&min), "batch=-3 direct_oltp=false eviction=lru workers=1");
}

#[test]
#[should_panic(expected = "duplicate knob")]
fn config_rejects_duplicate_knobs() {
    config::sample(&mut Gen::from_seed(0), &[Knob::Bool("a"), Knob::Bool("a")]);
}

#[test]
fn evidence_record_round_trips_and_writes_its_file() {
    let mut r = Record::new("kv-store-model", "U S");
    assert_eq!(r.result, Outcome::Error);
    assert_eq!(r.run.request_id, "local");
    assert!(r.validate().unwrap_err().contains("sha"), "a record without a SHA is invalid");
    r.guarantees = vec!["KV-G02".into(), "OLAP-STR-G01".into()];
    r.scenarios = vec!["K01".into()];
    r.sha = "a".repeat(40);
    r.tree_clean = true;
    r.build.profile = Some("dev".into());
    r.build.features = vec!["cache".into()];
    r.build.binary_sha256 = Some("b".repeat(64));
    r.mode = Some(config::sample(&mut Gen::from_seed(1), KNOBS));
    r.run.request_id = "th-check-core-abc1234-170500".into();
    r.run.attempt = Some(2);
    r.run.url = Some("https://github.com/o/r/actions/runs/1".into());
    r.run.date = "2026-11-22T17:05:09Z".into();
    r.duration_s = 1.5;
    r.assertion(true).assertion(true).not_run("s3-roundtrip", "s3-unconfigured");
    assert_eq!(r.conclude(), Outcome::Pass);
    r.seeds = Some(Seeds { range: Some([0, 99]), count: 100, failing: vec![u64::MAX] });
    r.sim_core_hours = Some(0.25);
    r.artifacts.push(Artifact { name: "history.jsonl".into(), sha256: "c".repeat(64) });
    r.validate().unwrap();

    let json = r.to_json();
    assert_eq!(Record::from_json(&json).unwrap(), r);
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["schema"], "nabedra.evidence/1");
    assert_eq!(v["result"], "pass");
    assert_eq!(v["kind"], "U S");
    assert_eq!(v["assertions"]["not_run"][0]["id"], "s3-roundtrip");
    assert_eq!(v["seeds"]["failing"][0].as_u64(), Some(u64::MAX));
    assert_eq!(v["virtual_hours"], serde_json::Value::Null);
    assert_eq!(v["artifacts"][0]["name"], "history.jsonl");
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
    keys.sort();
    let mut want = vec![
        "schema", "check", "kind", "guarantees", "scenarios", "sha", "tree_clean", "build", "mode", "run",
        "duration_s", "result", "assertions", "seeds", "sim_core_hours", "virtual_hours", "artifacts",
    ];
    want.sort();
    assert_eq!(keys, want);
    let sub = |o: &str| {
        let mut k: Vec<String> = v[o].as_object().unwrap().keys().cloned().collect();
        k.sort();
        k.join(",")
    };
    assert_eq!(sub("build"), "binary_sha256,features,profile");
    assert_eq!(sub("run"), "attempt,date,request_id,runner,url");
    assert_eq!(sub("assertions"), "executed,failed,not_run");
    assert_eq!(sub("seeds"), "count,failing,range");
    assert!(Record::from_json(&json.replace("\"tree_clean\"", "\"extra\": 1, \"tree_clean\"")).is_err());

    let dir = std::env::temp_dir().join(format!("nabedra-check-evidence-{}", std::process::id()));
    let path = r.write_to(&dir).unwrap();
    assert_eq!(path, dir.join("2026/11/th-check-core-abc1234-170500--kv-store-model.json"));
    assert_eq!(Record::from_json(&std::fs::read_to_string(&path).unwrap()).unwrap(), r);
    for i in 0..100 {
        r.not_run(&format!("assertion-{i}"), &"r".repeat(250));
    }
    assert!(r.validate().is_ok());
    assert_eq!(r.write_to(&dir).unwrap_err().kind(), std::io::ErrorKind::InvalidData, "over 20 KiB");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn evidence_validate_rejects_schema_violations() {
    let mut ok = Record::new("check-core", "U");
    ok.sha = "0123456789abcdef0123456789abcdef01234567".into();
    ok.validate().unwrap();
    assert!(ok.run.date.len() == 20 && ok.run.date.ends_with('Z'), "{}", ok.run.date);
    type Mutation = Box<dyn Fn(&mut Record)>;
    let bad: Vec<(&str, Mutation)> = vec![
        ("check", Box::new(|r| r.check = "Check_Core".into())),
        ("kind", Box::new(|r| r.kind = "U,S".into())),
        ("kind", Box::new(|r| r.kind = "X".into())),
        ("guarantee", Box::new(|r| r.guarantees = vec!["KV-G2".into()])),
        ("sha", Box::new(|r| r.sha = "A".repeat(40))),
        ("binary_sha256", Box::new(|r| r.build.binary_sha256 = Some("z".repeat(64)))),
        ("mode", Box::new(|r| r.mode = Some([("k".to_string(), serde_json::json!([1]))].into()))),
        ("run.url", Box::new(|r| r.run.url = Some("http://x".into()))),
        ("run.attempt", Box::new(|r| r.run.attempt = Some(0))),
        ("run.request_id", Box::new(|r| r.run.request_id = "a/b".into())),
        ("run.date", Box::new(|r| r.run.date = "yesterday".into())),
        ("duration_s", Box::new(|r| r.duration_s = -1.0)),
        ("artifact", Box::new(|r| r.artifacts = vec![Artifact { name: "a".into(), sha256: "0".into() }])),
    ];
    for (what, f) in bad {
        let mut r = ok.clone();
        f(&mut r);
        let e = r.validate().expect_err(what);
        assert!(e.contains(what), "{what}: {e}");
        assert_eq!(r.write_to(&std::env::temp_dir()).unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }
}

#[test]
fn evidence_dates_match_the_schema_format() {
    let mut record = Record::new("check-core", "U");
    record.sha = "a".repeat(40);
    for date in [
        "2026-13-01T00:00:00Z",
        "2026-02-29T00:00:00Z",
        "2026-11-22T24:00:00Z",
        "2026-11-22T00:00:00",
        "2026-11-22T00:00:00+02:60",
        "2026-11-22T00:00:00.Z",
    ] {
        record.run.date = date.into();
        assert!(record.validate().unwrap_err().contains("run.date"), "{date}");
    }
    for date in ["2024-02-29T00:00:00Z", "2026-11-22t17:05:09.123+01:00"] {
        record.run.date = date.into();
        record.validate().unwrap();
    }
}

#[test]
fn evidence_requires_explicit_null_fields() {
    let mut record = Record::new("check-core", "U");
    record.seeds = Some(Seeds::default());
    let json = serde_json::to_value(record).unwrap();
    for (parent, key) in [
        ("", "mode"),
        ("", "seeds"),
        ("", "sim_core_hours"),
        ("", "virtual_hours"),
        ("build", "profile"),
        ("build", "binary_sha256"),
        ("run", "url"),
        ("run", "runner"),
        ("run", "attempt"),
        ("run", "date"),
        ("seeds", "range"),
    ] {
        let mut broken = json.clone();
        let object = if parent.is_empty() { &mut broken } else { &mut broken[parent] };
        object.as_object_mut().unwrap().remove(key).unwrap();
        assert!(Record::from_json(&broken.to_string()).is_err(), "missing {parent}.{key}");
    }
}

#[test]
fn rfc3339_dates_are_correct() {
    use std::time::{Duration, UNIX_EPOCH};
    let at = |s: u64| evidence::rfc3339_utc(UNIX_EPOCH + Duration::from_secs(s));
    assert_eq!(at(0), "1970-01-01T00:00:00Z");
    assert_eq!(at(951_782_400), "2000-02-29T00:00:00Z");
    assert_eq!(at(1_795_367_109), "2026-11-22T17:05:09Z");
}

#[test]
fn evidence_conclude_reports_fail_and_not_run() {
    let mut r = Record::new("x", "U");
    assert_eq!(r.conclude(), Outcome::NotRun);
    r.assertion(true).assertion(false);
    assert_eq!(r.conclude(), Outcome::Fail);
    assert_eq!(serde_json::to_value(Outcome::NotRun).unwrap(), "not-run");
    assert_eq!(serde_json::to_value(Outcome::Quarantined).unwrap(), "quarantined");
}

#[test]
fn pair_sum_collects_into_one_choice() {
    // Both values stay below 100, so no single zeroing step can keep the sum at 100: zeroing,
    // halving and decrementing alone stop at a balanced pair such as `50 + 50`. The
    // redistribution pass moves the total into the second choice, reaching the shortlex minimum.
    for seed in 0..10 {
        let msg = failure(cfg(256, seed), "pair-sum", |g| {
            let (a, b) = (g.below(100), g.below(100));
            if a + b >= 100 { Err(format!("{a} + {b}")) } else { Ok(()) }
        })
        .unwrap();
        assert_eq!(line(&msg, "failure: "), "1 + 99", "seed {seed}:\n{msg}");
    }
}
