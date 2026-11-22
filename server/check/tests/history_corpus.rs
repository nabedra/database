//! The history checkers' corpus: every anomaly class has at least one history that must be
//! flagged with the right kind, and valid histories (including `info` ops that did and did not
//! take effect) must pass. Files live in `tests/corpus/`.

use nabedra_check::checker::{Verdict, edn, linearizable, outcome, queue, relaxed_kv, txn};
use nabedra_check::history::{Event, read_jsonl, write_jsonl};
use std::path::PathBuf;

type Checker = fn(&[Event]) -> Verdict;

/// (file, checker name, checker, expected kinds; empty means the history must pass).
const CASES: &[(&str, &str, Checker, &[&str])] = &[
    // Per-key linearizability.
    ("lin_valid_basic", "linearizable", linearizable::check_kv, &[]),
    ("lin_valid_info_took_effect", "linearizable", linearizable::check_kv, &[]),
    ("lin_valid_info_never", "linearizable", linearizable::check_kv, &[]),
    ("lin_stale_read", "linearizable", linearizable::check_kv, &["non-linearizable"]),
    ("lin_lost_write", "linearizable", linearizable::check_kv, &["non-linearizable"]),
    ("lin_cas_double", "linearizable", linearizable::check_kv, &["non-linearizable"]),
    // Elle-style transactions.
    ("txn_g0", "txn", txn::check, &["G0"]),
    ("txn_g1a", "txn", txn::check, &["G1a"]),
    ("txn_g1b", "txn", txn::check, &["G1b"]),
    ("txn_g1c", "txn", txn::check, &["G1c"]),
    ("txn_g_single", "txn", txn::check, &["G-single"]),
    ("txn_g2", "txn", txn::check, &["G2"]),
    ("txn_g_single_realtime", "txn", txn::check, &["G-single-realtime"]),
    ("txn_register_g1a", "txn", txn::check, &["G1a"]),
    ("txn_register_g_single", "txn", txn::check, &["G-single"]),
    ("txn_valid_append", "txn", txn::check, &[]),
    ("txn_register_valid", "txn", txn::check, &[]),
    // Outcome truth.
    ("outcome_failed_write_observed", "outcome", outcome::check, &["failed-write-observed"]),
    ("outcome_not_applied_observed", "outcome", outcome::check, &["failed-write-observed"]),
    ("outcome_dedup_after_fail", "outcome", outcome::check, &["failed-write-observed"]),
    ("outcome_valid", "outcome", outcome::check, &[]),
    ("txn_valid_append", "outcome", outcome::check, &[]),
    ("queue_valid", "outcome", outcome::check, &[]),
    // Queue.
    ("queue_valid", "queue", queue::check, &[]),
    ("queue_duplicate_offset", "queue", queue::check, &["duplicate-offset"]),
    ("queue_offset_gap", "queue", queue::check, &["offset-gap"]),
    ("queue_lost_publish", "queue", queue::check, &["lost-publish"]),
    ("queue_out_of_order", "queue", queue::check, &["out-of-order"]),
    ("queue_redelivery_missing", "queue", queue::check, &["redelivery-missing"]),
    // Relaxed KV (asynchronous replica).
    ("kv_invented_value", "relaxed_kv", relaxed_kv::check, &["invented-value"]),
    ("kv_resurrection_delete", "relaxed_kv", relaxed_kv::check, &["resurrection"]),
    ("kv_resurrection_expiry", "relaxed_kv", relaxed_kv::check, &["resurrection"]),
    ("kv_stale_read", "relaxed_kv", relaxed_kv::check, &["stale-read"]),
    ("kv_lost_write", "relaxed_kv", relaxed_kv::check, &["lost-write"]),
    ("kv_valid_failover", "relaxed_kv", relaxed_kv::check, &[]),
    ("kv_counter_invented", "relaxed_kv", relaxed_kv::check, &["invented-value"]),
    ("kv_counter_lost", "relaxed_kv", relaxed_kv::check, &["lost-write"]),
    ("kv_counter_valid", "relaxed_kv", relaxed_kv::check, &[]),
];

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn load(name: &str) -> Vec<Event> {
    let path = corpus_dir().join(format!("{name}.jsonl"));
    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    read_jsonl(std::io::BufReader::new(file)).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[test]
fn corpus_verdicts() {
    let mut failures = Vec::new();
    for (name, checker_name, checker, expected) in CASES {
        let events = load(name);
        let verdict = checker(&events);
        let kinds = verdict.kinds();
        let ok = if expected.is_empty() {
            verdict.valid && kinds.is_empty()
        } else {
            !verdict.valid && expected.iter().all(|k| verdict.has(k))
        };
        println!("{name} [{checker_name}]: valid={} kinds={kinds:?}", verdict.valid);
        if !ok {
            failures.push(format!(
                "{name} [{checker_name}]: expected {expected:?}, got valid={} {:#?}",
                verdict.valid, verdict.anomalies
            ));
        }
    }
    assert!(failures.is_empty(), "corpus mismatches:\n{}", failures.join("\n"));
}

/// Every anomaly class the plan names has a corpus history that flags it.
#[test]
fn corpus_covers_every_anomaly_class() {
    let required = [
        "G0", "G1a", "G1b", "G1c", "G-single", "G2", "non-linearizable", "lost-write", "stale-read",
        "failed-write-observed", "duplicate-offset", "offset-gap", "lost-publish", "invented-value",
        "resurrection", "out-of-order", "redelivery-missing",
    ];
    for kind in required {
        assert!(
            CASES.iter().any(|c| c.3.contains(&kind)),
            "no corpus history is expected to flag {kind}"
        );
    }
    // Every corpus file is used.
    for entry in std::fs::read_dir(corpus_dir()).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        let stem = name.trim_end_matches(".jsonl");
        assert!(CASES.iter().any(|c| c.0 == stem), "corpus file {name} has no expectation");
    }
}

/// Mutating one value in a valid history turns it invalid: the checkers really look at values.
#[test]
fn mutated_valid_histories_fail() {
    // A read of a value nobody wrote.
    let mut events = load("lin_valid_basic");
    let last = events.last_mut().unwrap();
    last.value = serde_json::json!({"value": 1234});
    assert!(linearizable::check_kv(&events).has("non-linearizable"));
    assert!(relaxed_kv::check(&events).has("invented-value"));

    // The list read loses an element in the middle.
    let mut events = load("txn_valid_append");
    let n = events.len();
    events[n - 1].value = serde_json::json!([["r", "x", [2]]]);
    assert!(!txn::check(&events).valid);

    // The failed publish ("e") shows up in a poll.
    let mut events = load("queue_valid");
    let pos = events.iter().rposition(|e| e.f == "poll").unwrap();
    events[pos].value = serde_json::json!({"events": [{"offset": 3, "value": "c", "redelivered": true}, {"offset": 4, "value": "e"}]});
    assert!(outcome::check(&events).has("failed-write-observed"));
}

#[test]
fn jsonl_round_trip_preserves_every_corpus_history() {
    for (name, ..) in CASES {
        let events = load(name);
        let mut buf = Vec::new();
        write_jsonl(&mut buf, &events).unwrap();
        let back = read_jsonl(std::io::BufReader::new(&buf[..])).unwrap();
        assert_eq!(events, back, "{name}");
        let text = String::from_utf8(buf).unwrap();
        assert!(!text.contains("\"unknown\""), "{name}: info must be written as info");
    }
}

#[test]
fn edn_export_is_jepsen_shaped() {
    let events = load("txn_g0");
    let edn = edn::export(&events);
    let first = edn.lines().next().unwrap();
    assert_eq!(
        first,
        r#"{:index 0, :type :invoke, :process 0, :f :txn, :value [[:append "x" 1] [:append "y" 1]]}"#
    );
    assert_eq!(edn.lines().count(), events.len());

    let events = load("queue_valid");
    let edn = edn::export_vector(&events);
    assert!(edn.starts_with('[') && edn.trim_end().ends_with(']'));
    assert!(edn.contains(":process :nemesis, :f :heal"));
    assert!(edn.contains(r#":f :publish, :value ["t/0" ["a" "b"]]"#), "{edn}");
    assert!(edn.contains(r#":value ["t/0" {:offsets [1 2]}]"#), "{edn}");
}
