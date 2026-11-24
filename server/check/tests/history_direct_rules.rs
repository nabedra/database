//! The opt-in direct-path rules (`nabedra_check::checker::direct::request_rules`) on captured,
//! existing and written histories.
//!
//! `tests/fixtures/unreviewed_dr1_dr2/` holds histories captured by PR #118 at `9b810d96`: one
//! seed, one local run, a simulated fault model. They are **unreviewed findings**. DR-1 and DR-2
//! are open, and whether they break a guarantee is for a later review. These tests say
//! what the rules report on them. They do not say that a guarantee passes or fails, and this
//! file is not part of the `check-history` evidence.
//!
//! The 14th captured history, `dr2_admission_full_control_not_sent`, is left out. It was written
//! as a control, but its own first retry went on the wire and became the delayed attempt, so it
//! is neither a valid control nor a planned reproduction (`dr-repro` report).

use nabedra_check::checker::direct::{
    self, InsertConflict, Options, ROW_CONFLICT, insert_conflict,
};
use nabedra_check::checker::linearizable::DEFAULT_MAX_CONFIGS;
use nabedra_check::checker::{Anomaly, Verdict};
use nabedra_check::history::{Event, EventType, Outcome, pair, read_jsonl, reindex};
use nabedra_check::rng::SplitMix64;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const UNSTABLE: &str = "unstable-result";
const EFFECT: &str = "not-applied-took-effect";

const STABLE_RESULTS: Options = Options {
    stable_results: true,
    request_not_applied: false,
    max_configs: DEFAULT_MAX_CONFIGS,
};
const NOT_APPLIED: Options = Options {
    stable_results: false,
    request_not_applied: true,
    max_configs: DEFAULT_MAX_CONFIGS,
};
const BOTH: Options = Options {
    stable_results: true,
    request_not_applied: true,
    max_configs: DEFAULT_MAX_CONFIGS,
};

fn tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")
}

fn read(path: &Path) -> Vec<Event> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    read_jsonl(std::io::BufReader::new(file)).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The JSONL files of a directory by name, sorted.
fn jsonl_files(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|x| x == "jsonl"))
        .map(|path| {
            let name = path.file_stem().unwrap().to_str().unwrap().to_string();
            (name, path)
        })
        .collect();
    files.sort();
    files
}

/// What `rules` adds to the default verdict, which must stay a prefix of the new one.
fn added(events: &[Event], rules: &Options) -> Vec<Anomaly> {
    let default = direct::check(events);
    let with = direct::check_with(events, rules);
    let (kept, new) = with.anomalies.split_at(default.anomalies.len());
    assert_eq!(kept, default.anomalies, "a rule changed a default anomaly");
    new.to_vec()
}

fn kinds(anomalies: &[Anomaly]) -> Vec<&str> {
    anomalies.iter().map(|a| a.kind.as_str()).collect()
}

enum Captured {
    /// The finding was reproduced. The two rules' kinds it must raise.
    Reproduced(&'static [&'static str]),
    /// A valid control, or a case whose answers stayed unknown.
    Clean,
}

use Captured::{Clean, Reproduced};

const FINDINGS: &[(&str, Captured)] = &[
    ("dr1_insert_refused-seed-1", Reproduced(&[UNSTABLE, EFFECT])),
    ("dr1_insert_refused_control_lost-seed-1", Clean),
    ("dr1_insert_refused_control_not_sent-seed-1", Clean),
    ("dr1_sequential_retry-seed-1", Reproduced(&[UNSTABLE])),
    ("dr1_update-seed-1", Reproduced(&[UNSTABLE])),
    ("dr1_update_control_lost-seed-1", Clean),
    ("dr1_update_control_not_sent-seed-1", Clean),
    ("dr2_admission_full-seed-1", Reproduced(&[EFFECT])),
    ("dr2_admission_full_control_lost-seed-1", Clean),
    // DR-2 was not reproduced here: every retry was answered unknown, and the row applied.
    ("dr2_all_links-seed-1", Clean),
    ("dr2_leader_and_first_link-seed-1", Reproduced(&[EFFECT])),
    ("dr2_leader_and_second_link-seed-1", Reproduced(&[EFFECT])),
    ("dr2_leader_link-seed-1", Reproduced(&[EFFECT])),
];

#[test]
fn unreviewed_findings_are_flagged_by_their_rule_and_controls_are_not() {
    let files = jsonl_files(&tests_dir().join("fixtures/unreviewed_dr1_dr2"));
    assert_eq!(
        files.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        FINDINGS.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "every captured history has an expectation"
    );
    for ((name, path), (_, captured)) in files.iter().zip(FINDINGS) {
        let events = read(path);
        // The default verdict accepts all of them, and these tests must not change that.
        let default = direct::check(&events);
        assert!(default.valid, "{name}: {:#?}", default.anomalies);
        let expected: &[&str] = match captured {
            Reproduced(kinds) => kinds,
            Clean => &[],
        };
        for (rules, kind) in [(&STABLE_RESULTS, UNSTABLE), (&NOT_APPLIED, EFFECT)] {
            let hits = added(&events, rules);
            let expected = expected.iter().filter(|k| **k == kind).count();
            println!("{name} [{kind}]: {} hit(s)", hits.len());
            assert_eq!(hits.len(), expected, "{name}: {hits:#?}");
            assert!(hits.iter().all(|a| a.kind == kind), "{name}: {hits:#?}");
        }
        assert_eq!(kinds(&added(&events, &BOTH)), expected, "{name}");
    }
}

/// The report of each rule names the events that the finding is about.
#[test]
fn findings_name_the_answers_that_disagree() {
    let load = |name: &str| {
        let dir = tests_dir().join("fixtures/unreviewed_dr1_dr2");
        read(&dir.join(format!("{name}-seed-1.jsonl")))
    };
    let update = added(&load("dr1_update"), &STABLE_RESULTS);
    assert_eq!(update[0].events, [9, 10, 18, 19]);
    assert!(
        update[0].detail.contains("Existed(false)") && update[0].detail.contains("Existed(true)")
    );
    let insert = added(&load("dr1_insert_refused"), &BOTH);
    assert_eq!(insert[0].events, [11, 12, 20, 21]);
    assert!(insert[0].detail.contains("RowConflict"));
    for (events, about) in [
        (insert[1].events.clone(), vec![11, 12, 9, 10, 18, 19]),
        (
            added(&load("dr2_leader_link"), &NOT_APPLIED)[0]
                .events
                .clone(),
            vec![12, 13, 7, 8, 9, 10, 17, 18],
        ),
    ] {
        for index in about {
            assert!(events.contains(&index), "{index} not in {events:?}");
        }
    }
}

/// Every hit of the opt-in rules on the histories that were here before them, reported as it
/// is. The existing expectations (`history_corpus`) use the default and are unchanged.
#[test]
fn hits_on_the_existing_corpus_and_fixtures() {
    let mut hits = Vec::new();
    for dir in ["corpus", "fixtures"] {
        for (name, path) in jsonl_files(&tests_dir().join(dir)) {
            let events = read(&path);
            for anomaly in added(&events, &BOTH) {
                println!("{dir}/{name}: {}: {}", anomaly.kind, anomaly.detail);
                hits.push((name.clone(), anomaly.kind));
            }
        }
    }
    let hits: Vec<(&str, &str)> = hits.iter().map(|(n, k)| (n.as_str(), k.as_str())).collect();
    assert_eq!(
        hits,
        [
            // Expected invalid already, by `dedup-miss`: `true` and then `false`.
            ("direct_dedup_miss", UNSTABLE),
            // Expected valid by the default, which follows the shipped executor. `update` r2
            // answers `false`, another client inserts the row, and the retry of r2 answers `true`.
            ("direct_valid_retries", UNSTABLE),
        ]
    );
}

/// With the message of a row conflict in place of every other `conflict`, the history must be
/// flagged: it is the message that decides.
fn over_a_row(events: &[Event]) -> Vec<Event> {
    let mut row = events.to_vec();
    for event in row.iter_mut().filter(|e| e.kind == EventType::Fail) {
        if event.value["code"] == "conflict" {
            event.value["message"] = json!(ROW_CONFLICT);
        }
    }
    row
}

/// Legal histories in which nothing took effect. Another client's insert gets a `conflict` that is
/// not about a row: a reason of the queued path, or no recorded reason. The rule of `e4f44961`
/// flagged every one.
#[test]
fn legal_conflicts_that_are_not_about_a_row_are_not_flagged() {
    let files = jsonl_files(&tests_dir().join("fixtures/legal_conflicts"));
    assert_eq!(files.len(), 12);
    for (name, path) in files {
        let events = read(&path);
        let default = direct::check(&events);
        assert!(default.valid, "{name}: {:#?}", default.anomalies);
        assert!(added(&events, &BOTH).is_empty(), "{name}");
        assert_eq!(
            direct::check_with(&events, &BOTH).unclassified_conflicts,
            usize::from(name.ends_with("no_message")),
            "{name}"
        );
        let row = added(&over_a_row(&events), &NOT_APPLIED);
        assert_eq!(kinds(&row), [EFFECT], "{name}");
    }
}

/// Not legal, or not certainly: an effect of an earlier attempt appears after a refusal that says
/// `not_applied`. The refusal is not a row conflict, so rule 2 leaves these out (its module doc
/// says why). A stricter rule may flag them, and then they move out of here.
#[test]
fn accepted_misses() {
    let files = jsonl_files(&tests_dir().join("fixtures/accepted_misses"));
    assert_eq!(files.len(), 10);
    for (name, path) in files {
        let events = read(&path);
        assert!(direct::check(&events).valid, "{name}");
        assert!(added(&events, &BOTH).is_empty(), "{name}");
        let row = added(&over_a_row(&events), &NOT_APPLIED);
        assert_eq!(kinds(&row), [EFFECT], "{name}");
    }
}

/// The message is read wherever the code is. For each spelling: an "abandoned" refusal is no
/// result (rule 1) and no row (rule 2), and a row conflict is both a result and a refusal.
#[test]
fn every_spelling_of_a_conflict_is_classified() {
    let files = jsonl_files(&tests_dir().join("fixtures/conflict_spellings"));
    assert_eq!(files.len(), 15);
    for (name, path) in files {
        let events = read(&path);
        assert!(direct::check(&events).valid, "{name}");
        let expected: &[&str] = if name.starts_with("row_") {
            &[UNSTABLE, EFFECT]
        } else {
            &[]
        };
        assert_eq!(kinds(&added(&events, &BOTH)), expected, "{name}");
        let verdict = direct::check_with(&events, &BOTH);
        assert_eq!(verdict.unclassified_conflicts, 0, "{name}");
    }
}

/// If the product rewords the row conflict, or a writer drops it, the rules go quiet on it. The
/// verdict must say how many answers they left out, once, with either rule on and never by
/// default.
#[test]
fn a_reworded_row_conflict_is_counted() {
    let dir = tests_dir().join("fixtures/unreviewed_dr1_dr2");
    let captured = read(&dir.join("dr1_insert_refused-seed-1.jsonl"));
    let verdict = direct::check_with(&captured, &BOTH);
    assert_eq!(verdict.unclassified_conflicts, 0);
    for message in [json!("a row with this key already exists"), Value::Null] {
        let mut reworded = captured.clone();
        let refused = reworded
            .iter_mut()
            .find(|e| e.value["message"] == ROW_CONFLICT)
            .unwrap();
        refused.value["message"] = message;
        for (rules, count) in [
            (&STABLE_RESULTS, 1),
            (&NOT_APPLIED, 1),
            (&BOTH, 1),
            (&Options::default(), 0),
        ] {
            let verdict = direct::check_with(&reworded, rules);
            assert!(verdict.valid, "{:#?}", verdict.anomalies);
            assert_eq!(verdict.unclassified_conflicts, count);
            let json = serde_json::to_value(&verdict).unwrap();
            let shown = json.get("unclassified_conflicts").cloned();
            assert_eq!(
                shown,
                (count > 0).then(|| json!(count)),
                "the default is as before"
            );
        }
        // Also when the history is flagged for something else.
        reworded.extend(
            read(&dir.join("dr2_leader_link-seed-1.jsonl"))
                .into_iter()
                .map(|mut e| {
                    if !e.is_nemesis() {
                        e.process += 1000;
                    }
                    e.key = e.key.map(|k| format!("other-{k}"));
                    e
                }),
        );
        reindex(&mut reworded);
        let verdict = direct::check_with(&reworded, &NOT_APPLIED);
        assert_eq!(
            (verdict.kinds(), verdict.unclassified_conflicts),
            (vec![EFFECT.into()], 1)
        );
        let merged = Verdict::merge([Verdict::new(0, Vec::new()), verdict]);
        assert_eq!(merged.unclassified_conflicts, 1);
    }
}

#[test]
fn only_the_refusals_of_inserts_are_classified() {
    let conflict = |message: &str| json!({"code": "conflict", "message": message});
    assert_eq!(
        insert_conflict(&conflict(ROW_CONFLICT)),
        Some(InsertConflict::Row)
    );
    for other in [
        "index name already exists",
        "Primary key already exists",
        "",
    ] {
        let found = insert_conflict(&conflict(other));
        assert_eq!(found, Some(InsertConflict::Unclassified), "{other:?}");
    }
    assert_eq!(insert_conflict(&json!({"code": "overloaded"})), None);
    assert_eq!(insert_conflict(&json!(null)), None);
    // An upsert is never refused over a row, whatever the message says.
    let mut events = history(
        "1> upsert k 1 seed
         1< ok
         0> upsert k 2 r
         0< info
         2> upsert k 2 r
         2< exists
         1> get k
         1< ok 2",
    );
    let verdict = direct::check_with(&events, &NOT_APPLIED);
    assert!(!verdict.has(EFFECT), "{:#?}", verdict.anomalies);
    events[5].value["message"] = json!("unknown");
    let verdict = direct::check_with(&events, &NOT_APPLIED);
    assert_eq!(verdict.unclassified_conflicts, 0);
}

/// Every search of the rule reports its limit: whatever the bound, a history that the rule flags
/// is flagged or reported as `check-limit`, and never passes.
#[test]
fn request_not_applied_reports_its_search_limit() {
    let written = "4> upsert k 11 a
         4< info
         5> upsert k 12 b
         5< info
         6> upsert k 13 c
         6< info
         0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1";
    // Without r, only `delete m` and then a batch explain the first read, and the search tries the
    // six upserts before them. The refusal is judged only after that search.
    let hidden = r#"1> insert m 5 seed
         1< ok
         11> upsert k 21 a
         11< info
         12> upsert k 22 b
         12< info
         13> upsert k 23 c
         13< info
         14> upsert k 24 d
         14< info
         15> upsert k 25 e
         15< info
         16> upsert k 26 f
         16< info
         17> insert_many k [{"id":"k","value":1},{"id":"m","value":9}] y
         17< info
         18> insert_many k [{"id":"k","value":1},{"id":"m","value":9}] z
         18< info
         19> delete m null gone
         19< info
         0> upsert k 1 r
         0< info
         1> get k
         1< ok 1
         2> upsert k 1 r
         2< refused overloaded
         1> get m
         1< ok 5"#;
    let mut histories = vec![history(written), history(hidden)];
    histories.extend((0..100).map(|seed| generated(seed, 60, Reading::Plan, false)));
    let (mut flagged, mut limited) = (0, 0);
    for events in histories {
        if added(&events, &NOT_APPLIED).is_empty() {
            continue;
        }
        flagged += 1;
        let mut max_configs = 0;
        loop {
            let rules = Options {
                max_configs,
                ..NOT_APPLIED
            };
            let hits = kinds(&added(&events, &rules)).join(" ");
            assert!(!hits.is_empty(), "silent with {max_configs} configurations");
            if hits.contains(EFFECT) {
                break;
            }
            limited += 1;
            max_configs += 1 + max_configs / 16;
        }
    }
    println!("{flagged} flagged histories, {limited} lower bounds reported as check-limit");
    assert!(flagged > 5 && limited > 50);
}

/// A history from a compact text, one event per line:
///
/// - `0> update k 1 r1`: process 0 invokes `update` on `k` with value 1 and request ID `r1`;
/// - `0< ok false`, `0< info`, `0< fail` (a bare `fail`);
/// - `0< refused overloaded`: a `not_applied` refusal with that code;
/// - `0< exists`: the `conflict` refusal of an existing row, with its message.
fn history(text: &str) -> Vec<Event> {
    let mut open = BTreeMap::new();
    let mut events = Vec::new();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let words: Vec<&str> = line.split_whitespace().collect();
        let (process, direction) = words[0].split_at(words[0].len() - 1);
        let process: u64 = process.parse().unwrap();
        let word = |i: usize| words.get(i).copied();
        let parse = |i: usize| word(i).map_or(Value::Null, |w| serde_json::from_str(w).unwrap());
        if direction == ">" {
            let mut event = Event::new(EventType::Invoke, process, words[1])
                .with_key(words[2])
                .with_value(parse(3));
            event.request_id = word(4).map(String::from);
            open.insert(process, event.clone());
            events.push(event);
            continue;
        }
        let mut event = open.remove(&process).unwrap();
        (event.kind, event.value) = match words[1] {
            "ok" => (EventType::Ok, parse(2)),
            "info" => (EventType::Info, Value::Null),
            "fail" => (EventType::Fail, Value::Null),
            "refused" => (
                EventType::Fail,
                json!({"not_applied": true, "code": words[2]}),
            ),
            "exists" => (
                EventType::Fail,
                json!({"not_applied": true, "code": "conflict", "message": "primary key already exists"}),
            ),
            other => panic!("unknown completion {other}"),
        };
        events.push(event);
    }
    reindex(&mut events);
    events
}

/// Checks one case: the default verdict must accept it, and `rules` adds `kind` or nothing.
fn case(rules: &Options, name: &str, text: &str, kind: Option<&str>) {
    let events = history(text);
    let default = direct::check(&events);
    assert!(default.valid, "{name}: {:#?}", default.anomalies);
    let hits = added(&events, rules);
    assert_eq!(
        kinds(&hits),
        kind.into_iter().collect::<Vec<_>>(),
        "{name}: {hits:#?}"
    );
}

fn verdict(text: &str, rules: &Options) -> Verdict {
    direct::check_with(&history(text), rules)
}

#[test]
fn stable_results_cases() {
    let flagged = |name, text| case(&STABLE_RESULTS, name, text, Some(UNSTABLE));
    let legal = |name, text| case(&STABLE_RESULTS, name, text, None);

    flagged(
        "a no-op delete that a retry applies",
        "0> delete k null r
         0< ok false
         1> insert k 1 other
         1< ok
         0> delete k null r
         0< ok true",
    );
    flagged(
        "an insert refused over a row that a retry applies",
        "1> insert k 1 other
         1< ok
         0> insert k 2 r
         0< exists
         1> delete k null gone
         1< ok true
         0> insert k 2 r
         0< ok",
    );
    flagged(
        "a batch refused over a row that a retry applies",
        r#"1> insert b 1 other
           1< ok
           0> insert_many a [{"id":"a","value":2},{"id":"b","value":3}] r
           0< exists
           1> delete b null gone
           1< ok true
           0> insert_many a [{"id":"a","value":2},{"id":"b","value":3}] r
           0< ok 2"#,
    );
    // The second retry disagrees with the first answer, although the first retry overlapped it.
    flagged(
        "a later retry after an overlapping pair",
        "0> update k 2 r
         2> insert k 1 other
         1> update k 2 r
         0< ok false
         2< ok
         1< ok true
         1> update k 2 r
         1< ok true",
    );
    // The first attempt sent overlaps all others, so the two retries are the ones to compare.
    flagged(
        "two later answers under an attempt that is still open",
        "0> update k 2 r
         1> update k 2 r
         1< ok false
         2> insert k 1 other
         2< ok
         3> update k 2 r
         3< ok true
         0< ok true",
    );
    // Two commits with different answers are also a default anomaly. The kind is still added.
    let recommitted = "1> insert k 1 other
         1< ok
         0> update k 2 r
         0< ok true
         1> delete k null gone
         1< ok true
         0> update k 2 r
         0< ok false";
    let both = verdict(recommitted, &STABLE_RESULTS);
    assert!(both.has("dedup-miss") && both.has(UNSTABLE), "{both:#?}");

    legal(
        "overlapping attempts that disagree",
        "0> update k 2 r
         2> insert k 1 other
         1> update k 2 r
         0< ok false
         2< ok
         1< ok true",
    );
    // The retry is invoked one event before the first answer returns.
    legal(
        "a retry invoked before the first answer returned",
        "0> update k 2 r
         1> update k 2 r
         0< ok false
         2> insert k 1 other
         2< ok
         1< ok true",
    );
    legal(
        "an unknown attempt and one definite answer",
        "0> update k 2 r
         0< info
         3> update k 2 r
         3< ok false",
    );
    legal(
        "refusals before execution, before and after the result",
        "0> upsert k 1 r
         0< refused overloaded
         0> upsert k 1 r
         0< ok
         0> upsert k 1 r
         0< refused storage_unavailable
         0> upsert k 1 r
         0< fail",
    );
    legal(
        "another body, another key and another request ID",
        "0> update k 2 r
         0< ok false
         1> insert k 1 other
         1< ok
         0> update k 3 r2
         0< ok true
         0> update j 2 r
         0< ok false
         0> delete k null r
         0< ok true",
    );
    // The first refusal may be reuse of r by the upsert, so it is not r's insert result.
    legal(
        "a conflict that another body could explain",
        "1> insert k 1 other
         1< ok
         2> upsert k 9 r
         0> insert k 2 r
         0< exists
         1> delete k null gone
         1< ok true
         0> insert k 2 r
         0< ok",
    );
    legal(
        "existence not recorded",
        "0> update k 2 r
         0< ok false
         0> update k 2 r
         0< refused overloaded",
    );
    legal(
        "the same refusal twice",
        "1> insert k 1 other
         1< ok
         0> insert k 2 r
         0< exists
         0> insert k 2 r
         0< exists",
    );

    // The queued path's other conflicts are not the row's, so their message rules them out.
    let mut moved = history(
        "1> insert k 1 other
         1< ok
         0> insert k 2 r
         0< exists
         1> delete k null gone
         1< ok true
         0> insert k 2 r
         0< ok",
    );
    moved[3].value["message"] = json!("pinned Catalog route moved before queue admission");
    assert!(added(&moved, &STABLE_RESULTS).is_empty());
    // Without a message it may be any of them.
    moved[3].value.as_object_mut().unwrap().remove("message");
    assert!(added(&moved, &STABLE_RESULTS).is_empty());
    moved[3].value["message"] = json!(ROW_CONFLICT);
    assert_eq!(kinds(&added(&moved, &STABLE_RESULTS)), [UNSTABLE]);

    // Batch counts: a different count disagrees, a missing one does not.
    let batch = |count: &str| {
        format!(
            r#"0> insert_many a [{{"id":"a","value":2}}] r
               0< ok 1
               0> insert_many a [{{"id":"a","value":2}}] r
               0< ok {count}"#
        )
    };
    assert!(verdict(&batch("2"), &STABLE_RESULTS).has(UNSTABLE));
    assert!(verdict(&batch(""), &STABLE_RESULTS).valid);
    assert!(verdict(&batch("1"), &STABLE_RESULTS).valid);
}

#[test]
fn request_not_applied_cases() {
    let flagged = |name, text| case(&NOT_APPLIED, name, text, Some(EFFECT));
    let legal = |name, text| case(&NOT_APPLIED, name, text, None);

    flagged(
        "a read of the delayed attempt's value",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    flagged(
        "an attempt that never completed, refused through another gateway",
        "0> insert k 1 r
         2> insert k 1 r
         2< refused storage_unavailable
         1> get k
         1< ok 1",
    );
    flagged(
        "a delete seen by another client's existence flag",
        "1> insert k 1 seed
         1< ok
         0> delete k null r
         0< info
         2> delete k null r
         2< refused overloaded
         1> update k 5 other
         1< ok false",
    );
    flagged(
        "an insert seen by another client's conflict",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> insert k 5 other
         1< exists",
    );
    flagged(
        "one row of a batch",
        r#"0> insert_many a [{"id":"a","value":1},{"id":"b","value":2}] r
           0< info
           2> insert_many a [{"id":"a","value":1},{"id":"b","value":2}] r
           2< refused overloaded
           1> get b
           1< ok 2"#,
    );
    flagged(
        "a replay of the commit on a row that is gone",
        "1> insert k 1 seed
         1< ok
         0> update k 2 r
         0< info
         2> update k 2 r
         2< refused overloaded
         1> delete k null gone
         1< ok true
         3> update k 2 r
         3< ok true",
    );
    // The overlapping read does not count, and the one after the refusal does.
    flagged(
        "a read during the refusal and one after it",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         1> get k
         2< refused overloaded
         1< ok 1
         1> get k
         1< ok 1",
    );
    // A later retry cannot explain an earlier observation.
    flagged(
        "a retry after the observation",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1
         2> insert k 1 r
         2< ok",
    );
    // The first refusal covers no attempt, and the second covers the one that applied.
    flagged(
        "two refusals, and the second is wrong",
        "0> delete k null r
         0< refused overloaded
         1> insert k 1 seed
         1< ok
         2> delete k null r
         2< info
         3> delete k null r
         3< refused overloaded
         1> get k
         1< ok null",
    );

    // The links are found last to first, so one pass over the batches misses c. The read of c rules
    // out y, and then only r explains the refusal of x.
    flagged(
        "a key two batch links away",
        r#"3> insert_many b [{"id":"b","value":7},{"id":"c","value":8}] y
           3< info
           0> insert a 1 r
           0< info
           2> insert a 1 r
           2< refused overloaded
           1> insert_many a [{"id":"a","value":5},{"id":"b","value":6}] x
           1< exists
           1> get c
           1< ok null"#,
    );

    legal(
        "the unknown attempt never took effect",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok null",
    );
    legal(
        "a refusal alone, and a retry that then commits",
        "0> insert k 1 r
         0< refused overloaded
         0> insert k 1 r
         0< ok
         1> get k
         1< ok 1",
    );
    legal(
        "a retry after the refusal can explain the effect",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         2> insert k 1 r
         2< info
         1> get k
         1< ok 1",
    );
    legal(
        "an attempt invoked while the refused one was in flight can explain it",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         3> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    legal(
        "a refusal after an answered commit",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< ok
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    legal(
        "a commit answered while the refused attempt was in flight",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         3> insert k 1 r
         2< ok
         3< refused overloaded
         1> get k
         1< ok 1",
    );
    // The success replays a commit that the upsert overwrote. Only the first attempt explains it.
    legal(
        "a refusal after the answered replay of an overwritten commit",
        "0> upsert k 1 r
         0< info
         1> upsert k 5 other
         1< ok
         2> upsert k 1 r
         2< ok
         2> upsert k 1 r
         2< refused overloaded
         1> get k
         1< ok 5",
    );
    legal(
        "an effect shown before the refusal was invoked",
        "0> insert k 1 r
         0< info
         1> get k
         1< ok 1
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    legal(
        "an effect shown while the refused attempt was in flight",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         1> get k
         1< ok 1
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    legal(
        "a read invoked before the refusal returned",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         1> get k
         2< refused overloaded
         1< ok 1",
    );
    legal(
        "a value that another unknown request could have written",
        "0> upsert k 1 r
         0< info
         3> upsert k 1 other
         3< info
         2> upsert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    legal(
        "a bare fail",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< fail
         1> get k
         1< ok 1",
    );
    legal(
        "a refusal of another body, another key or another request ID",
        "0> insert k 1 r
         0< info
         2> insert k 2 r
         2< refused overloaded
         2> insert j 1 r
         2< refused overloaded
         2> insert k 1 other
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    // A fence that records the refusal would refuse the upsert too, with no effect of r.
    legal(
        "a conflict under the same identity",
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         3> upsert k 9 r
         3< refused conflict
         1> upsert k 5 other
         1< ok
         1> get k
         1< ok 5",
    );
    legal(
        "a no-op answered false is not a refusal",
        "0> update k 2 r
         0< info
         2> update k 2 r
         2< ok false
         1> insert k 1 other
         1< ok
         1> get k
         1< ok 2",
    );

    // Where the default already finds the key not linearizable, the rule adds nothing to it.
    // On other keys it still applies.
    let broken = "4> insert z 1 zz
         4< ok
         4> get z
         4< ok 7
         0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1";
    let both = verdict(broken, &NOT_APPLIED);
    assert_eq!(both.kinds(), ["non-linearizable", EFFECT]);
    let before = broken.replace(" z ", " k ").replace("get z", "get k");
    assert_eq!(verdict(&before, &NOT_APPLIED).kinds(), ["non-linearizable"]);
    let after = "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 7";
    assert_eq!(verdict(after, &NOT_APPLIED).kinds(), ["non-linearizable"]);
}

/// The default follows the shipped executor, also against the decided retry rule: the replay of a
/// recorded no-op or row conflict is `non-linearizable` there once the row changed. The rules
/// accept it and keep judging the rest of the key.
#[test]
fn the_default_rejects_the_replay_of_a_recorded_no_op() {
    let replayed = "0> update k 2 r
         0< ok false
         1> insert k 1 other
         1< ok
         0> update k 2 r
         0< ok false";
    assert_eq!(verdict(replayed, &BOTH).kinds(), ["non-linearizable"]);
    let delayed = format!(
        "{replayed}
         4> upsert k 3 late
         4< info
         5> upsert k 3 late
         5< refused overloaded
         1> get k
         1< ok 3"
    );
    assert_eq!(
        verdict(&delayed, &BOTH).kinds(),
        ["non-linearizable", EFFECT]
    );
}

/// Both spellings of `not_applied` in the history writers count.
#[test]
fn both_spellings_of_not_applied_count() {
    let mut events = history(
        "0> insert k 1 r
         0< info
         2> insert k 1 r
         2< refused overloaded
         1> get k
         1< ok 1",
    );
    events[3].value = json!({"code": "overloaded", "outcome": "not_applied"});
    // The outcome field decides, whatever the event's type.
    events[3].kind = EventType::Ok;
    assert_eq!(pair(&events).unwrap().ops[1].outcome, Outcome::Fail);
    assert_eq!(kinds(&added(&events, &NOT_APPLIED)), [EFFECT]);
    events[3].value = json!({"code": "overloaded", "not_applied": false});
    events[3].kind = EventType::Fail;
    assert!(added(&events, &NOT_APPLIED).is_empty());
}

/// The rules are off unless asked for, and one does not turn on the other.
#[test]
fn rules_are_off_by_default() {
    assert_eq!(
        Options::default(),
        Options {
            stable_results: false,
            request_not_applied: false,
            max_configs: DEFAULT_MAX_CONFIGS,
        }
    );
    for (_, path) in jsonl_files(&tests_dir().join("fixtures/unreviewed_dr1_dr2")) {
        let events = read(&path);
        let default = direct::check(&events);
        assert_eq!(default, direct::check_with(&events, &Options::default()));
        let off = direct::check_with(&events, &STABLE_RESULTS);
        assert!(!off.has(EFFECT));
        let off = direct::check_with(&events, &NOT_APPLIED);
        assert!(!off.has(UNSTABLE));
    }
}

/// Which text the generated store follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reading {
    /// Both rules hold: every result is recorded, also a no-op and a row conflict, and a refusal
    /// stops the attempts of its request that are still on their way.
    Identity,
    /// The shipped executor, and `plans/oltp-direct-path.md` before 2026-11-24: only commits are
    /// recorded, and a refusal speaks for its attempt alone.
    Plan,
}

#[derive(Clone, Debug)]
struct Request {
    f: &'static str,
    anchor: String,
    value: Value,
    id: String,
}

impl Request {
    fn body(&self) -> String {
        format!("{} {}", self.f, self.value)
    }

    fn same(&self, other: &Request) -> bool {
        self.id == other.id && self.anchor == other.anchor && self.body() == other.body()
    }

    fn rows(&self) -> Vec<(String, Value)> {
        let Some(rows) = self.value.as_array() else {
            return vec![(self.anchor.clone(), self.value.clone())];
        };
        let key = |row: &Value| row["id"].as_str().unwrap().to_string();
        rows.iter()
            .map(|row| (key(row), row["value"].clone()))
            .collect()
    }

    fn event(&self, kind: EventType, process: u64, value: Value) -> Event {
        let mut event = Event::new(kind, process, self.f)
            .with_key(&self.anchor)
            .with_value(value);
        event.request_id = (self.f != "get").then(|| self.id.clone());
        event
    }
}

type Reply = (EventType, Value);

fn refusal(code: &str, message: &str) -> Reply {
    let value = json!({"not_applied": true, "code": code, "message": message});
    (EventType::Fail, value)
}

#[derive(Default)]
struct Store {
    rows: BTreeMap<String, Value>,
    /// By anchor and request ID, the body and its result.
    records: BTreeMap<(String, String), (String, Reply)>,
}

impl Store {
    fn record(&self, request: &Request) -> Option<&(String, Reply)> {
        let id = (request.anchor.clone(), request.id.clone());
        self.records.get(&id)
    }

    fn run(&mut self, request: &Request, reading: Reading) -> Reply {
        if request.f == "get" {
            let row = self.rows.get(&request.anchor).cloned();
            return (EventType::Ok, row.unwrap_or(Value::Null));
        }
        if let Some((body, reply)) = self.record(request) {
            return if *body == request.body() {
                reply.clone()
            } else {
                refusal(
                    "conflict",
                    "request_id was already used with a different request body",
                )
            };
        }
        let rows = request.rows();
        let exists = rows.iter().any(|(key, _)| self.rows.contains_key(key));
        let (reply, commits) = match request.f {
            "insert" | "insert_many" if exists => {
                (refusal("conflict", "primary key already exists"), false)
            }
            "update" | "delete" => ((EventType::Ok, json!(exists)), exists),
            "insert_many" => ((EventType::Ok, json!(rows.len())), true),
            _ => ((EventType::Ok, Value::Null), true),
        };
        for (key, value) in rows.into_iter().filter(|_| commits) {
            if request.f == "delete" {
                self.rows.remove(&key);
            } else {
                self.rows.insert(key, value);
            }
        }
        if commits || reading == Reading::Identity {
            let id = (request.anchor.clone(), request.id.clone());
            self.records.insert(id, (request.body(), reply.clone()));
        }
        reply
    }
}

struct Attempt {
    request: Request,
    process: u64,
    reply: Option<Reply>,
    /// Stopped by a refusal of the same request.
    stopped: bool,
}

/// Concurrent clients over few keys. Replies get lost, attempts arrive late, requests are retried
/// after every kind of answer (also by another client at the same time) and refused before they
/// run, and some IDs are used again with another body.
/// With `queued`, some inserts are refused with a `conflict` of the queued path, which is recorded
/// nowhere and stops nothing.
fn generated(seed: u64, operations: usize, reading: Reading, queued: bool) -> Vec<Event> {
    let mut rng = SplitMix64::new(seed);
    let mut below = move |n: u64| rng.next_u64() % n;
    let mut store = Store::default();
    let mut events = Vec::new();
    let mut open: Vec<Option<Attempt>> = (0..5).map(|_| None).collect();
    let mut late: Vec<Request> = Vec::new();
    let mut sent: Vec<Request> = Vec::new();
    // Requests that were answered with a commit. A refusal does not take that back.
    let mut answered = BTreeSet::new();
    let (mut invoked, mut processes, mut values) = (0, 0, 0);
    while invoked < operations || open.iter().any(Option::is_some) {
        let client = below(5) as usize;
        match open[client].take() {
            None if invoked < operations => {
                invoked += 1;
                let request = if !sent.is_empty() && below(2) == 0 {
                    // Send one of the last requests again, whatever its answers so far.
                    let recent = below(sent.len().min(6) as u64) as usize;
                    let mut request = sent[sent.len() - 1 - recent].clone();
                    if below(12) == 0 && request.f == "upsert" && store.record(&request).is_some() {
                        values += 2;
                        request.value = json!(values);
                    }
                    request
                } else {
                    values += 2;
                    let key = below(3);
                    let anchor = format!("k{key}");
                    let (f, value) = match below(9) {
                        0 | 1 => ("get", Value::Null),
                        2 | 3 => ("insert", json!(values)),
                        4 => ("update", json!(values)),
                        5 | 6 => ("delete", Value::Null),
                        7 => ("upsert", json!(values)),
                        _ => {
                            let next = format!("k{}", key + 1);
                            let rows = json!([
                                {"id": anchor, "value": values},
                                {"id": next, "value": values + 1},
                            ]);
                            ("insert_many", rows)
                        }
                    };
                    let id = format!("r{values}");
                    Request {
                        f,
                        anchor,
                        value,
                        id,
                    }
                };
                let invoke = request.event(EventType::Invoke, processes, request.value.clone());
                events.push(invoke);
                if request.f != "get" {
                    sent.push(request.clone());
                }
                open[client] = Some(Attempt {
                    request,
                    process: processes,
                    reply: None,
                    stopped: false,
                });
                processes += 1;
            }
            None => {}
            Some(attempt) if attempt.reply.is_some() => {
                let (kind, value) = attempt.reply.unwrap();
                let request = attempt.request;
                if request.f != "get" && below(4) == 0 {
                    events.push(request.event(EventType::Info, attempt.process, Value::Null));
                    continue;
                }
                if request.f != "get" && kind == EventType::Ok && value != json!(false) {
                    answered.insert((request.id.clone(), request.body()));
                }
                events.push(request.event(kind, attempt.process, value));
            }
            Some(attempt) if attempt.request.f != "get" && below(5) == 0 => {
                // The client stops waiting. The attempt may still arrive.
                let request = attempt.request;
                events.push(request.event(EventType::Info, attempt.process, Value::Null));
                if !attempt.stopped && below(4) != 0 {
                    late.push(request);
                }
            }
            Some(mut attempt) => {
                let request = &attempt.request;
                let may_refuse = reading == Reading::Plan
                    || store.record(request).is_none()
                    || answered.contains(&(request.id.clone(), request.body()));
                // A stopped attempt stops no others: a later attempt may have committed by now.
                let refuse = request.f != "get" && !attempt.stopped && may_refuse && below(3) == 0;
                attempt.reply = Some(if attempt.stopped || refuse {
                    if reading == Reading::Identity && refuse {
                        late.retain(|other| !other.same(request));
                        for other in open.iter_mut().flatten() {
                            other.stopped |= other.reply.is_none() && other.request.same(request);
                        }
                    }
                    refusal("overloaded", "direct in-flight budget is full")
                } else if queued
                    && request.f.starts_with("insert")
                    && store.record(request).is_none()
                    && below(5) == 0
                {
                    refusal("conflict", "original request run was abandoned")
                } else {
                    store.run(request, reading)
                });
                open[client] = Some(attempt);
            }
        }
        if !late.is_empty() && below(12) == 0 {
            let request = late.swap_remove(below(late.len() as u64) as usize);
            store.run(&request, reading);
        }
    }
    for key in 0..4 {
        let request = Request {
            f: "get",
            anchor: format!("k{key}"),
            value: Value::Null,
            id: String::new(),
        };
        events.push(request.event(EventType::Invoke, processes, Value::Null));
        let (kind, value) = store.run(&request, reading);
        events.push(request.event(kind, processes, value));
    }
    reindex(&mut events);
    events
}

/// A store that keeps both promises is never flagged. The same clients on a store that follows
/// the shipped executor are, often, which shows that the first half is not empty.
#[test]
fn a_store_that_keeps_both_promises_is_never_flagged() {
    let (mut unstable, mut effects) = (0, 0);
    let (mut identity_accepted, mut plan_accepted) = (0, 0);
    for seed in 0..200 {
        for queued in [false, true] {
            let events = generated(seed, 60, Reading::Identity, queued);
            let hits = added(&events, &BOTH);
            assert!(hits.is_empty(), "seed {seed}, queued {queued}: {hits:#?}");
            identity_accepted += usize::from(direct::check(&events).valid && !queued);
        }
        let events = generated(seed, 60, Reading::Plan, false);
        unstable += usize::from(!added(&events, &STABLE_RESULTS).is_empty());
        effects += usize::from(!added(&events, &NOT_APPLIED).is_empty());
        plan_accepted += usize::from(direct::check(&events).valid);
    }
    println!(
        "200 seeds. Identity store: 0 flagged, {identity_accepted} accepted by the default. Plan store: {unstable} {UNSTABLE}, {effects} {EFFECT}, {plan_accepted} accepted by the default"
    );
    assert!(unstable >= 30 && effects >= 10);
    assert_eq!(plan_accepted, 200);
}
