//! Direct-path histories (P10-CHECK): a correct model of the direct path's dedup records must
//! pass [`direct::check`], and the same model with a planted dedup bug must be caught. Also
//! mutated corpus histories, and the performance bound with request identities on.
//!
//! The model follows `plans/oltp-direct-path.md`, section 4: the dedup record is keyed by
//! anchor key and request ID and holds the body and the result; the same body replays the
//! result, another body is refused with `conflict`; `update` and `delete` of a missing row and
//! refusals commit nothing. Clients retry the same ID and body after an unknown outcome,
//! sometimes through another gateway, and some requests reuse an earlier ID with another body
//! on the same or another key (D02, D03, D04).

use nabedra_check::checker::{direct, linearizable};
use nabedra_check::history::{Event, EventType, read_jsonl, reindex};
use nabedra_check::rng::SplitMix64;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Instant;

fn below(rng: &mut SplitMix64, n: u64) -> u64 {
    rng.next_u64() % n
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bug {
    None,
    /// The leader never finds a dedup record, so a retry runs again (the D02 and D04 class).
    ForgetDedup,
    /// The dedup lookup ignores the digest, so another body under the ID replays (D03).
    IgnoreDigest,
}

#[derive(Clone, Debug)]
struct Req {
    f: &'static str,
    key: String,
    value: Value,
    rid: String,
    node: u64,
}

impl Req {
    fn body(&self) -> String {
        format!("{}{}", self.f, self.value)
    }

    fn rows(&self) -> Vec<(String, Value)> {
        match self.f {
            "insert_many" => self
                .value
                .as_array()
                .unwrap()
                .iter()
                .map(|r| (r["id"].as_str().unwrap().to_string(), r["value"].clone()))
                .collect(),
            _ => vec![(self.key.clone(), self.value["value"].clone())],
        }
    }
}

#[derive(Default)]
struct Store {
    rows: BTreeMap<String, Value>,
    dedup: HashMap<(String, String), (String, Value)>,
    /// Times the planted bug changed an answer or an effect.
    bug_hits: usize,
}

fn conflict() -> Value {
    json!({"not_applied": true, "code": "conflict"})
}

impl Store {
    fn apply(&mut self, req: &Req, bug: Bug) -> (EventType, Value) {
        if req.f == "get" {
            let row = match self.rows.get(&req.key) {
                Some(v) => json!({"id": req.key, "value": v}),
                None => Value::Null,
            };
            return (EventType::Ok, row);
        }
        let id = (req.key.clone(), req.rid.clone());
        if let Some((body, result)) = self.dedup.get(&id).cloned() {
            match bug {
                Bug::ForgetDedup => self.bug_hits += 1,
                Bug::IgnoreDigest if body != req.body() => {
                    self.bug_hits += 1;
                    return (EventType::Ok, result);
                }
                _ if body == req.body() => return (EventType::Ok, result),
                _ => return (EventType::Fail, conflict()),
            }
        }
        let rows = req.rows();
        let present = |s: &Store, k: &str| s.rows.contains_key(k);
        let result = match req.f {
            "insert" | "insert_many" => {
                if rows.iter().any(|(k, _)| present(self, k)) {
                    return (EventType::Fail, conflict());
                }
                for (k, v) in &rows {
                    self.rows.insert(k.clone(), v.clone());
                }
                if req.f == "insert" {
                    Value::Null
                } else {
                    json!(rows.len())
                }
            }
            "upsert" => {
                self.rows.insert(rows[0].0.clone(), rows[0].1.clone());
                Value::Null
            }
            "update" => {
                if !present(self, &req.key) {
                    return (EventType::Ok, json!(false));
                }
                self.rows.insert(req.key.clone(), rows[0].1.clone());
                json!(true)
            }
            "delete" => {
                if self.rows.remove(&req.key).is_none() {
                    return (EventType::Ok, json!(false));
                }
                json!(true)
            }
            other => unreachable!("{other}"),
        };
        self.dedup.insert(id, (req.body(), result.clone()));
        (EventType::Ok, result)
    }
}

struct Pending {
    req: Req,
    process: u64,
    result: Option<(EventType, Value)>,
}

fn event(kind: EventType, process: u64, req: &Req, value: Value) -> Event {
    let mut e = Event::new(kind, process, req.f)
        .with_key(&req.key)
        .with_value(value);
    if req.f != "get" {
        e.request_id = Some(req.rid.clone());
    }
    e.node = Some(format!("n{}", req.node));
    e
}

/// A direct-path history: `clients` concurrent clients over `keys` keys. Each op takes effect
/// atomically between its invoke and its completion, or, after a lost reply or a timeout,
/// later or never.
fn direct_history(
    seed: u64,
    ops: usize,
    keys: u64,
    clients: usize,
    bug: Bug,
) -> (Vec<Event>, usize) {
    let mut rng = SplitMix64::new(seed);
    let mut store = Store::default();
    let mut events = Vec::new();
    let mut pending: Vec<Option<Pending>> = (0..clients).map(|_| None).collect();
    let mut retry: Vec<Option<Req>> = vec![None; clients];
    let mut process: Vec<u64> = (0..clients as u64).collect();
    let mut next_process = clients as u64;
    // Attempts whose reply was lost before they took effect, and that may still apply.
    let mut late: Vec<Req> = Vec::new();
    let mut used: Vec<(String, String)> = Vec::new(); // (anchor, rid)
    let mut next_value = 0u64;
    let mut next_rid = 0u64;
    let mut invoked = 0;
    let key = |i: u64| format!("k{:02}", i % keys);
    while invoked < ops || pending.iter().any(Option::is_some) {
        let c = below(&mut rng, clients as u64) as usize;
        match &mut pending[c] {
            None if invoked < ops => {
                invoked += 1;
                let mut fresh = |rng: &mut SplitMix64| {
                    next_value += 1;
                    let k = key(below(rng, keys));
                    let (f, value) = match below(rng, 9) {
                        0..=2 => ("get", Value::Null),
                        3 => ("insert", json!({"id": k, "value": next_value})),
                        4 => ("upsert", json!({"id": k, "value": next_value})),
                        5 | 6 => ("update", json!({"value": next_value})),
                        7 => ("delete", Value::Null),
                        _ => {
                            let i = below(rng, keys);
                            let (a, b) = (key(i), key(i + 1));
                            next_value += 1;
                            let rows = json!([
                                {"id": a, "value": next_value - 1},
                                {"id": b, "value": next_value},
                            ]);
                            ("insert_many", rows)
                        }
                    };
                    let anchor = match f {
                        "insert_many" => value[0]["id"]
                            .as_str()
                            .unwrap()
                            .min(value[1]["id"].as_str().unwrap())
                            .to_string(),
                        _ => k,
                    };
                    next_rid += 1;
                    Req {
                        f,
                        key: anchor,
                        value,
                        rid: format!("r{next_rid}"),
                        node: below(rng, 3),
                    }
                };
                let req = match retry[c].take() {
                    Some(mut r) => {
                        r.node = below(&mut rng, 3); // maybe through another gateway
                        r
                    }
                    None => {
                        let mut r = fresh(&mut rng);
                        // D03: reuse an earlier ID with another body, on its key or another.
                        if r.f != "get" && !used.is_empty() && below(&mut rng, 8) == 0 {
                            let (anchor, rid) =
                                used[below(&mut rng, used.len() as u64) as usize].clone();
                            r.rid = rid;
                            if below(&mut rng, 2) == 0 && r.f != "insert_many" {
                                r.key = anchor.clone();
                                if r.value.get("id").is_some() {
                                    r.value["id"] = json!(anchor);
                                }
                            }
                        }
                        if r.f != "get" {
                            used.push((r.key.clone(), r.rid.clone()));
                        }
                        r
                    }
                };
                events.push(event(
                    EventType::Invoke,
                    process[c],
                    &req,
                    req.value.clone(),
                ));
                pending[c] = Some(Pending {
                    req,
                    process: process[c],
                    result: None,
                });
            }
            None => {}
            Some(op) if op.result.is_none() => {
                // Admission refusal before the dedup lookup: nothing ran.
                if op.req.f != "get" && below(&mut rng, 25) == 0 {
                    op.result = Some((
                        EventType::Fail,
                        json!({"not_applied": true, "code": "overloaded"}),
                    ));
                } else {
                    op.result = Some(store.apply(&op.req, bug));
                }
                // The reply is lost after the effect.
                if op.req.f != "get" && below(&mut rng, 12) == 0 {
                    let op = pending[c].take().unwrap();
                    events.push(event(EventType::Info, op.process, &op.req, Value::Null));
                    next_process += 1;
                    process[c] = next_process;
                    if below(&mut rng, 4) != 0 {
                        retry[c] = Some(op.req);
                    }
                }
            }
            Some(_) => {
                let op = pending[c].take().unwrap();
                let (kind, value) = op.result.unwrap();
                events.push(event(kind, op.process, &op.req, value));
            }
        }
        // A timeout before the effect: the attempt may still apply later, or never.
        if below(&mut rng, 60) == 0 {
            let q = below(&mut rng, clients as u64) as usize;
            if let Some(op) = &pending[q]
                && op.result.is_none()
                && op.req.f != "get"
            {
                let op = pending[q].take().unwrap();
                events.push(event(
                    EventType::Info,
                    op.process,
                    &op.req,
                    json!({"outcome": "outcome_unknown"}),
                ));
                next_process += 1;
                process[q] = next_process;
                if below(&mut rng, 2) == 0 {
                    late.push(op.req.clone());
                }
                if below(&mut rng, 4) != 0 {
                    retry[q] = Some(op.req);
                }
            }
        }
        if !late.is_empty() && below(&mut rng, 10) == 0 {
            let r = late.swap_remove(below(&mut rng, late.len() as u64) as usize);
            store.apply(&r, bug);
        }
    }
    // Final reads of every key.
    let reader = next_process + 1;
    for i in 0..keys {
        let req = Req {
            f: "get",
            key: key(i),
            value: Value::Null,
            rid: String::new(),
            node: 0,
        };
        events.push(event(EventType::Invoke, reader, &req, Value::Null));
        let (kind, value) = store.apply(&req, bug);
        events.push(event(kind, reader, &req, value));
    }
    reindex(&mut events);
    (events, store.bug_hits)
}

/// Without request identities, a replayed retry reads as a second write of an old value, so the
/// plain per-key check rejects some correct direct-path histories. That is why
/// `direct::check` turns `request_dedup` on.
#[test]
fn generated_direct_histories_pass() {
    let mut plain_rejects = 0;
    for seed in 0..40u64 {
        let (events, _) = direct_history(seed, 400, 6, 5, Bug::None);
        let v = direct::check(&events);
        assert!(v.valid, "seed {seed}: {:#?}", v.anomalies);
        if !linearizable::check_kv(&events).valid {
            plain_rejects += 1;
        }
    }
    println!("the plain check rejects {plain_rejects} of 40 correct histories");
    assert!(plain_rejects > 0);
}

/// Planted bugs from the D02, D03 and D04 class. Not every hit is observable (a retried upsert
/// that writes the value already there changes nothing), so the test asserts that most seeds
/// with a hit are caught, and prints the counts.
#[test]
fn planted_dedup_bugs_are_caught() {
    for bug in [Bug::ForgetDedup, Bug::IgnoreDigest] {
        let (mut hit, mut caught) = (0, 0);
        for seed in 0..40u64 {
            let (events, hits) = direct_history(seed, 400, 6, 5, bug);
            if hits == 0 {
                continue;
            }
            hit += 1;
            let v = direct::check(&events);
            if !v.valid {
                caught += 1;
                assert!(
                    !v.has("malformed-history"),
                    "{bug:?} seed {seed}: {:#?}",
                    v.anomalies
                );
            }
        }
        println!("{bug:?}: {caught} of {hit} seeds with a hit caught");
        assert!(hit >= 30, "{bug:?}: only {hit} seeds hit the bug");
        assert!(caught * 10 >= hit * 9, "{bug:?}: {caught} of {hit} caught");
    }
}

fn load(name: &str) -> Vec<Event> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus")
        .join(format!("{name}.jsonl"));
    let file = std::fs::File::open(&path).unwrap();
    read_jsonl(std::io::BufReader::new(file)).unwrap()
}

/// Each answer in the valid corpus history matters: flipping it makes the history fail.
#[test]
fn mutated_direct_histories_fail() {
    let base = load("lin_valid_direct_ops");
    let pos = |f: &str, rid: &str, kind: EventType| {
        base.iter()
            .position(|e| e.f == f && e.request_id.as_deref() == Some(rid) && e.kind == kind)
            .unwrap()
    };
    // update r3 found the row; claim it did not.
    let mut e = base.clone();
    e[pos("update", "r3", EventType::Ok)].value = json!(false);
    assert!(linearizable::check_kv(&e).has("non-linearizable"));
    // update r4 found no row; claim it did.
    let mut e = base.clone();
    e[pos("update", "r4", EventType::Ok)].value = json!(true);
    assert!(linearizable::check_kv(&e).has("non-linearizable"));
    // delete r7 found no row; claim it did.
    let mut e = base.clone();
    e[pos("delete", "r7", EventType::Ok)].value = json!({"data": true});
    assert!(linearizable::check_kv(&e).has("non-linearizable"));
    // insert r2 was refused on a present key; claim it succeeded.
    let mut e = base.clone();
    let i = pos("insert", "r2", EventType::Fail);
    e[i].kind = EventType::Ok;
    e[i].value = Value::Null;
    assert!(linearizable::check_kv(&e).has("non-linearizable"));
    // The same refusal with another error code constrains nothing.
    let mut e = base.clone();
    e[pos("insert", "r2", EventType::Fail)].value =
        json!({"not_applied": true, "code": "overloaded"});
    assert!(linearizable::check_kv(&e).valid);
    // An update without the existence flag writes only when the row is present.
    let mut e = base.clone();
    e[pos("update", "r4", EventType::Ok)].value = Value::Null;
    assert!(linearizable::check_kv(&e).valid);
    let reads_k2 = e
        .iter()
        .rposition(|x| x.f == "get" && x.key.as_deref() == Some("k2") && x.kind == EventType::Ok)
        .unwrap();
    // k2's update ran on a missing row, so its value 4 can never be read.
    e[reads_k2].value = json!({"id": "k2", "value": 4});
    assert!(linearizable::check_kv(&e).has("non-linearizable"));

    // The D03 refusal is explained by the other body under r1 on k1. Without it, the refused
    // upsert on k1 is a misdetected reuse.
    let retries = load("direct_valid_retries");
    let mut e = retries.clone();
    for ev in e.iter_mut() {
        if ev.f == "upsert" {
            ev.request_id = Some("r-other".into());
        }
    }
    assert!(direct::check(&e).has("request-id-reuse-misdetected"));
    // Answering the replayed update from fresh state after the commit is a dedup miss.
    let mut e = retries.clone();
    let replay = e
        .iter()
        .enumerate()
        .filter(|(_, x)| x.f == "update" && x.kind == EventType::Ok)
        .map(|(i, _)| i)
        .nth(2)
        .unwrap();
    e[replay].value = json!(false);
    assert!(direct::check(&e).has("dedup-miss"));
    // A conflict refusal of the same body after the commit is also a dedup miss.
    let mut e = retries;
    let overloaded = e
        .iter()
        .position(|x| x.f == "update" && x.kind == EventType::Fail)
        .unwrap();
    e[overloaded].value = json!({"not_applied": true, "code": "conflict"});
    assert!(direct::check(&e).has("dedup-miss"));
}

#[test]
fn insert_many_without_anchor_key_uses_its_smallest_row_key() {
    let mut events = load("direct_valid_retries");
    for e in events.iter_mut().filter(|e| e.f == "insert_many") {
        e.key = None;
    }
    assert!(
        direct::check(&events).valid,
        "{:#?}",
        direct::check(&events)
    );
    let mut bad = events.clone();
    let i = bad.iter().position(|e| e.f == "insert_many").unwrap();
    bad[i].value = json!([{"value": 1}]);
    assert!(direct::check(&bad).has("malformed-history"));
}

#[test]
// A wall-clock bound on the checker itself; nabedra-check does not depend on nabedra-rt.
#[allow(clippy::disallowed_methods)]
fn performance_2000_direct_ops_under_2s() {
    let (events, _) = direct_history(7, 2000, 20, 10, Bug::None);
    let start = Instant::now();
    let v = direct::check(&events);
    let elapsed = start.elapsed();
    println!("checked {} ops in {elapsed:?}", v.checked_ops);
    assert!(v.valid, "{:#?}", v.anomalies);
    assert!(v.checked_ops >= 1500, "only {} ops checked", v.checked_ops);
    assert!(elapsed.as_secs_f64() < 2.0, "took {elapsed:?}");
}
