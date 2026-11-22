//! The history checkers' corpus: every anomaly class has at least one history that must be
//! flagged with the right kind, and valid histories (including `info` ops that did and did not
//! take effect) must pass. Files live in `tests/corpus/`.

use nabedra_check::checker::{Verdict, edn, linearizable, outcome, queue, relaxed_kv, txn};
use nabedra_check::history::{Event, Outcome, pair, read_jsonl, write_jsonl};
use std::path::PathBuf;

type Checker = fn(&[Event]) -> Verdict;

/// (file, checker name, checker, expected kinds; empty means the history must pass).
const CASES: &[(&str, &str, Checker, &[&str])] = &[
    // Per-key linearizability.
    (
        "lin_valid_basic",
        "linearizable",
        linearizable::check_kv,
        &[],
    ),
    (
        "lin_valid_info_took_effect",
        "linearizable",
        linearizable::check_kv,
        &[],
    ),
    (
        "lin_valid_info_never",
        "linearizable",
        linearizable::check_kv,
        &[],
    ),
    (
        "lin_stale_read",
        "linearizable",
        linearizable::check_kv,
        &["non-linearizable"],
    ),
    (
        "lin_lost_write",
        "linearizable",
        linearizable::check_kv,
        &["non-linearizable"],
    ),
    (
        "lin_cas_double",
        "linearizable",
        linearizable::check_kv,
        &["non-linearizable"],
    ),
    // Elle-style transactions.
    ("txn_g0", "txn", txn::check, &["G0"]),
    ("txn_g1a", "txn", txn::check, &["G1a"]),
    ("txn_g1b", "txn", txn::check, &["G1b"]),
    ("txn_g1c", "txn", txn::check, &["G1c"]),
    ("txn_g_single", "txn", txn::check, &["G-single"]),
    ("txn_g2", "txn", txn::check, &["G2"]),
    (
        "txn_g_single_realtime",
        "txn",
        txn::check,
        &["G-single-realtime"],
    ),
    ("txn_register_g1a", "txn", txn::check, &["G1a"]),
    ("txn_register_g_single", "txn", txn::check, &["G-single"]),
    ("txn_valid_append", "txn", txn::check, &[]),
    ("txn_register_valid", "txn", txn::check, &[]),
    // Outcome truth.
    (
        "outcome_failed_write_observed",
        "outcome",
        outcome::check,
        &["failed-write-observed"],
    ),
    (
        "outcome_not_applied_observed",
        "outcome",
        outcome::check,
        &["failed-write-observed"],
    ),
    (
        "outcome_dedup_after_fail",
        "outcome",
        outcome::check,
        &["failed-write-observed"],
    ),
    ("outcome_valid", "outcome", outcome::check, &[]),
    ("txn_valid_append", "outcome", outcome::check, &[]),
    ("queue_valid", "outcome", outcome::check, &[]),
    // Queue.
    ("queue_valid", "queue", queue::check, &[]),
    (
        "queue_duplicate_offset",
        "queue",
        queue::check,
        &["duplicate-offset"],
    ),
    ("queue_offset_gap", "queue", queue::check, &["offset-gap"]),
    (
        "queue_lost_publish",
        "queue",
        queue::check,
        &["lost-publish"],
    ),
    (
        "queue_out_of_order",
        "queue",
        queue::check,
        &["out-of-order"],
    ),
    (
        "queue_redelivery_missing",
        "queue",
        queue::check,
        &["redelivery-missing"],
    ),
    // Relaxed KV (asynchronous replica).
    (
        "kv_invented_value",
        "relaxed_kv",
        relaxed_kv::check,
        &["invented-value"],
    ),
    (
        "kv_resurrection_delete",
        "relaxed_kv",
        relaxed_kv::check,
        &["resurrection"],
    ),
    (
        "kv_resurrection_expiry",
        "relaxed_kv",
        relaxed_kv::check,
        &["resurrection"],
    ),
    (
        "kv_stale_read",
        "relaxed_kv",
        relaxed_kv::check,
        &["stale-read"],
    ),
    (
        "kv_lost_write",
        "relaxed_kv",
        relaxed_kv::check,
        &["lost-write"],
    ),
    ("kv_valid_failover", "relaxed_kv", relaxed_kv::check, &[]),
    (
        "kv_counter_invented",
        "relaxed_kv",
        relaxed_kv::check,
        &["invented-value"],
    ),
    (
        "kv_counter_lost",
        "relaxed_kv",
        relaxed_kv::check,
        &["lost-write"],
    ),
    ("kv_counter_valid", "relaxed_kv", relaxed_kv::check, &[]),
    // Written by scripts/testing/realenv/history.py: null fields, f="nemesis" markers, a
    // fresh process after an info op whose write took effect.
    (
        "realenv_python_writer",
        "linearizable",
        linearizable::check_kv,
        &[],
    ),
    (
        "realenv_python_writer",
        "relaxed_kv",
        relaxed_kv::check,
        &[],
    ),
    ("realenv_python_writer", "outcome", outcome::check, &[]),
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
        let expected: &[&str] = expected;
        let events = load(name);
        let verdict = checker(&events);
        let kinds = verdict.kinds();
        let ok = if expected.is_empty() {
            verdict.valid && kinds.is_empty()
        } else {
            !verdict.valid && expected.iter().all(|k| verdict.has(k))
        };
        println!(
            "{name} [{checker_name}]: valid={} kinds={kinds:?}",
            verdict.valid
        );
        if !ok {
            failures.push(format!(
                "{name} [{checker_name}]: expected {expected:?}, got valid={} {:#?}",
                verdict.valid, verdict.anomalies
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "corpus mismatches:\n{}",
        failures.join("\n")
    );
}

/// Every anomaly class the plan names has a corpus history that flags it.
#[test]
fn corpus_covers_every_anomaly_class() {
    let required = [
        "G0",
        "G1a",
        "G1b",
        "G1c",
        "G-single",
        "G2",
        "non-linearizable",
        "lost-write",
        "stale-read",
        "failed-write-observed",
        "duplicate-offset",
        "offset-gap",
        "lost-publish",
        "invented-value",
        "resurrection",
        "out-of-order",
        "redelivery-missing",
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
        assert!(
            CASES.iter().any(|c| c.0 == stem),
            "corpus file {name} has no expectation"
        );
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
        assert!(
            !text.contains("\"unknown\""),
            "{name}: info must be written as info"
        );
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
    assert!(
        edn.contains(r#":f :publish, :value ["t/0" ["a" "b"]]"#),
        "{edn}"
    );
    assert!(edn.contains(r#":value ["t/0" {:offsets [1 2]}]"#), "{edn}");
}

/// The real-environment Python writer's style: `null` for unknown fields, nemesis events as
/// `f: "nemesis"` with `value.action`, on an integer process.
#[test]
fn reads_the_python_writer_format() {
    let events = load("realenv_python_writer");
    assert_eq!(events.len(), 14);
    assert!(events.iter().all(|e| e.time_ns.is_some()));
    assert_eq!(events[2].key, None);
    assert_eq!(events[2].request_id, None);
    let paired = pair(&events).unwrap();
    assert_eq!(paired.ops.len(), 5);
    let names: Vec<&str> = paired.markers.iter().map(|m| m.f.as_str()).collect();
    assert_eq!(names, ["kill", "heal_all"]);
    assert!(paired.markers.iter().all(|m| m.applied()));
    assert_eq!(
        paired
            .ops
            .iter()
            .filter(|o| o.outcome == Outcome::Info)
            .count(),
        1
    );

    // A literal line in that style, with every optional field null.
    let line = r#"{"index":null,"type":"info","process":0,"f":"nemesis","key":null,"value":{"action":"heal_all","failed":1},"request_id":null,"node":null,"time_ns":null}"#;
    let parsed = read_jsonl(std::io::BufReader::new(line.as_bytes())).unwrap();
    assert_eq!(parsed[0].index, 0);
    assert!(parsed[0].is_nemesis());
    assert_eq!(parsed[0].marker_name(), "heal_all");
    assert_eq!(parsed[0].time_ns, None);
}

#[test]
fn linearizability_review_cases() {
    let stale = history(
        r#"
{"type":"invoke","process":0,"f":"write","key":"k","value":1}
{"type":"invoke","process":1,"f":"write","key":"k","value":2}
{"type":"ok","process":1,"f":"write","key":"k"}
{"type":"ok","process":0,"f":"write","key":"k"}
{"type":"invoke","process":2,"f":"read","key":"k"}
{"type":"ok","process":2,"f":"read","key":"k","value":2}
{"type":"invoke","process":3,"f":"read","key":"k"}
{"type":"ok","process":3,"f":"read","key":"k","value":1}
"#,
    );
    // The first read forces 1 before 2, then the second read illegally returns 1.
    assert!(linearizable::check_kv(&stale).has("non-linearizable"));
    let mut reordered = stale[..6].to_vec();
    reordered.swap(0, 1);
    assert!(
        linearizable::check_kv(&reordered).valid,
        "the cache must distinguish the two terminal values of the same set of writes"
    );

    let cas = history(
        r#"
{"type":"invoke","process":0,"f":"cas","key":"k","value":[null,1]}
{"type":"info","process":0,"f":"cas","key":"k"}
{"type":"invoke","process":1,"f":"cas","key":"k","value":[2,3]}
{"type":"invoke","process":2,"f":"cas","key":"k","value":[1,2]}
{"type":"ok","process":1,"f":"cas","key":"k"}
{"type":"ok","process":2,"f":"cas","key":"k"}
{"type":"invoke","process":3,"f":"read","key":"k"}
{"type":"ok","process":3,"f":"read","key":"k","value":3}
"#,
    );
    assert!(linearizable::check_kv(&cas).valid);
    assert!(linearizable::check_kv_with(&cas, 1).has("check-limit"));

    let malformed = history(
        r#"
{"type":"invoke","process":0,"f":"cas","key":"k","value":[1]}
{"type":"ok","process":0,"f":"cas","key":"k"}
"#,
    );
    assert!(linearizable::check_kv(&malformed).has("malformed-history"));
}

fn history(jsonl: &str) -> Vec<Event> {
    read_jsonl(jsonl.as_bytes()).unwrap()
}

#[test]
fn txn_version_order_review_cases() {
    let stale = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["w","x",1]]}
{"type":"ok","process":0,"f":"txn","value":[["w","x",1]]}
{"type":"invoke","process":1,"f":"txn","value":[["w","x",2]]}
{"type":"ok","process":1,"f":"txn","value":[["w","x",2]]}
{"type":"invoke","process":2,"f":"txn","value":[["r","z",null]]}
{"type":"ok","process":2,"f":"txn","value":[["r","z",null]]}
{"type":"invoke","process":3,"f":"txn","value":[["r","x",null]]}
{"type":"ok","process":3,"f":"txn","value":[["r","x",1]]}
"#,
    );
    assert_eq!(txn::check(&stale).kinds(), ["G-single-realtime"]);

    let overlap = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["w","x",1]]}
{"type":"invoke","process":1,"f":"txn","value":[["w","x",2]]}
{"type":"ok","process":0,"f":"txn","value":[["w","x",1]]}
{"type":"ok","process":1,"f":"txn","value":[["w","x",2]]}
{"type":"invoke","process":2,"f":"txn","value":[["r","x",null]]}
{"type":"ok","process":2,"f":"txn","value":[["r","x",1]]}
"#,
    );
    assert!(txn::check(&overlap).valid);

    let fractured = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["w","x",1],["w","y",1]]}
{"type":"invoke","process":1,"f":"txn","value":[["w","x",2],["w","y",2]]}
{"type":"ok","process":0,"f":"txn","value":[["w","x",1],["w","y",1]]}
{"type":"ok","process":1,"f":"txn","value":[["w","x",2],["w","y",2]]}
{"type":"invoke","process":2,"f":"txn","value":[["r","x",null],["r","y",null]]}
{"type":"ok","process":2,"f":"txn","value":[["r","x",1],["r","y",2]]}
"#,
    );
    assert_eq!(txn::check(&fractured).kinds(), ["G0-realtime"]);

    let hidden_append = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["append","x",1]]}
{"type":"ok","process":0,"f":"txn","value":[["append","x",1]]}
{"type":"invoke","process":1,"f":"txn","value":[["r","x",null]]}
{"type":"ok","process":1,"f":"txn","value":[["r","x",[]]]}
"#,
    );
    assert_eq!(txn::check(&hidden_append).kinds(), ["G-single-realtime"]);

    let array_register = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["w","x",[1,2]]]}
{"type":"ok","process":0,"f":"txn","value":[["w","x",[1,2]]]}
{"type":"invoke","process":1,"f":"txn","value":[["r","x",null]]}
{"type":"ok","process":1,"f":"txn","value":[["r","x",[1,2]]]}
"#,
    );
    assert!(txn::check(&array_register).valid);

    let inconsistent = history(
        r#"
{"type":"invoke","process":0,"f":"txn","value":[["w","x",1],["r","x",null]]}
{"type":"ok","process":0,"f":"txn","value":[["w","x",1],["r","x",null]]}
"#,
    );
    assert!(txn::check(&inconsistent).has("internal"));
}

#[test]
fn events_simulator_receipts_are_checked() {
    let events = history(
        r#"
{"type":"invoke","process":0,"f":"publish","key":"p0","value":[["00000000-0000-0000-0000-000000000001",42]]}
{"type":"ok","process":0,"f":"publish","key":"p0","value":[["00000000-0000-0000-0000-000000000001",1]]}
{"type":"invoke","process":1,"f":"poll","key":"p0","value":{"generation":1}}
{"type":"ok","process":1,"f":"poll","key":"p0","value":{"after":0,"events":[[1,"00000000-0000-0000-0000-000000000001"]]}}
{"type":"invoke","process":1,"f":"ack","key":"p0","value":{"after":0,"through":1}}
{"type":"info","process":1,"f":"ack","key":"p0","value":"client timeout"}
{"type":"invoke","process":1,"f":"poll","key":"p0","value":{"generation":1}}
{"type":"ok","process":1,"f":"poll","key":"p0","value":{"after":0,"events":[[1,"00000000-0000-0000-0000-000000000001"]]}}
{"type":"invoke","process":1,"f":"ack","key":"p0","value":{"after":0,"through":1}}
{"type":"ok","process":1,"f":"ack","key":"p0","value":{"checkpoint":1}}
{"type":"invoke","process":1,"f":"poll","key":"p0","value":{"generation":1}}
{"type":"ok","process":1,"f":"poll","key":"p0","value":{"after":1,"events":[]}}
"#,
    );
    assert!(queue::check(&events).valid, "{:?}", queue::check(&events));
    let mut wrong_offset = events.clone();
    wrong_offset[1].value[0][1] = serde_json::json!(2);
    assert!(queue::check(&wrong_offset).has("duplicate-event"));
    let mut missing = events[..4].to_vec();
    missing[3].value["events"] = serde_json::json!([]);
    assert!(queue::check(&missing).has("lost-publish"));
    let mut wrong_ack = events.clone();
    wrong_ack[9].value["checkpoint"] = serde_json::json!(2);
    assert!(queue::check(&wrong_ack).has("malformed-history"));

    let mut timeout_before_gap = history(
        r#"
{"type":"invoke","process":0,"f":"publish","key":"p0","value":"a"}
{"type":"info","process":0,"f":"publish","key":"p0"}
{"type":"invoke","process":1,"f":"publish","key":"p0","value":"b"}
{"type":"ok","process":1,"f":"publish","key":"p0","value":2}
"#,
    );
    assert!(queue::check(&timeout_before_gap).valid);
    timeout_before_gap.rotate_left(2);
    assert!(queue::check(&timeout_before_gap).has("offset-gap"));

    let mut before_heal = load("queue_redelivery_missing");
    before_heal.extend(history(
        r#"
{"type":"invoke","process":2,"f":"ack","key":"t/0","value":3}
{"type":"ok","process":2,"f":"ack","key":"t/0"}
"#,
    ));
    assert!(queue::check(&before_heal).has("redelivery-missing"));
}

#[test]
fn relaxed_kv_review_cases() {
    let failed = load("outcome_failed_write_observed");
    assert!(relaxed_kv::check(&failed).has("failed-write-observed"));

    let future_delete = history(
        r#"
{"type":"invoke","process":0,"f":"set","key":"k","value":"first"}
{"type":"ok","process":0,"f":"set","key":"k","value":{"epoch":1}}
{"type":"invoke","process":1,"f":"get","key":"k"}
{"type":"ok","process":1,"f":"get","key":"k","value":{"value":null,"epoch":1}}
{"type":"invoke","process":0,"f":"del","key":"k"}
{"type":"ok","process":0,"f":"del","key":"k","value":{"epoch":1}}
"#,
    );
    assert!(relaxed_kv::check(&future_delete).has("stale-read"));

    let stale = history(
        r#"
{"type":"invoke","process":0,"f":"set","key":"k","value":"first","time_ns":1}
{"type":"ok","process":0,"f":"set","key":"k","value":{"epoch":1},"time_ns":2}
{"type":"info","process":"nemesis","f":"failover","key":"k","time_ns":100}
{"type":"invoke","process":1,"f":"get","key":"k","time_ns":101}
{"type":"ok","process":1,"f":"get","key":"k","value":{"value":null,"epoch":2},"time_ns":102}
"#,
    );
    let bound = relaxed_kv::Options {
        lag_window_ns: Some(10),
        ..Default::default()
    };
    assert!(relaxed_kv::check_with(&stale, &bound).has("lost-write"));

    let mut reset = stale;
    reset[2].f = "cold-reset".into();
    assert!(relaxed_kv::check_with(&reset, &bound).valid);
    reset[4].value["value"] = serde_json::json!("first");
    assert!(relaxed_kv::check_with(&reset, &bound).has("resurrection"));

    let mut mget = load("kv_resurrection_expiry");
    mget[6].f = "mget".into();
    mget[6].value = serde_json::json!(["a", "b"]);
    mget[7].f = "mget".into();
    mget[7].value = serde_json::json!([1, null]);
    assert!(relaxed_kv::check(&mget).has("resurrection"));
}

#[test]
fn outcome_requires_a_possible_source_for_a_value_or_dedup_receipt() {
    let mut events = load("outcome_failed_write_observed");
    let mut successful_retry = events[..2].to_vec();
    successful_retry[1].kind = nabedra_check::history::EventType::Ok;
    events.extend(successful_retry);
    assert!(outcome::check(&events).has("failed-write-observed"));

    let mut dedup = load("outcome_dedup_after_fail");
    dedup[2].key = Some("other".into());
    dedup[3].key = Some("other".into());
    assert!(outcome::check(&dedup).valid);

    let mut dedup = load("outcome_dedup_after_fail");
    let mut committed = dedup[2..].to_vec();
    committed[1].value = serde_json::json!({"dedup": false});
    dedup.splice(2..2, committed);
    assert!(outcome::check(&dedup).valid);
}
