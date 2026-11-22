//! Generated histories from a correct simulated store must pass every applicable checker, and
//! planted bugs in them must be caught. Also the performance sanity bound: 2,000 register ops
//! over 20 keys check in under 2 s in a debug build.

use nabedra_check::checker::{linearizable, outcome, relaxed_kv, txn};
use nabedra_check::history::{Event, EventType, reindex};
use nabedra_check::rng::SplitMix64;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Instant;

fn below(rng: &mut SplitMix64, n: u64) -> u64 {
    rng.next_u64() % n
}

struct Pending {
    f: &'static str,
    key: String,
    invoke: Value,
    /// The result once the op has taken effect at its linearization point.
    result: Option<(EventType, Value)>,
}

/// A correct linearizable register store driven by `procs` concurrent clients. Each op takes
/// effect atomically at a random point between its invoke and completion. A few ops end as
/// `info`, whether or not they took effect.
fn register_history(seed: u64, ops: usize, keys: usize, procs: usize) -> Vec<Event> {
    let mut rng = SplitMix64::new(seed);
    let mut state: HashMap<String, Value> = HashMap::new();
    let mut pending: Vec<Option<Pending>> = (0..procs).map(|_| None).collect();
    let mut events = Vec::new();
    let mut next_value = 1u64;
    let mut invoked = 0;
    while invoked < ops || pending.iter().any(Option::is_some) {
        let p = below(&mut rng, procs as u64) as usize;
        match &mut pending[p] {
            None if invoked < ops => {
                invoked += 1;
                let key = format!("k{}", below(&mut rng, keys as u64));
                let (f, invoke) = match below(&mut rng, 10) {
                    0..=4 => ("read", Value::Null),
                    5..=7 => {
                        next_value += 1;
                        ("write", json!(next_value))
                    }
                    8 => {
                        next_value += 1;
                        let cur = state.get(&key).cloned().unwrap_or(Value::Null);
                        let expected = if below(&mut rng, 2) == 0 {
                            cur
                        } else {
                            json!(next_value * 1000)
                        };
                        ("cas", json!([expected, next_value]))
                    }
                    _ => ("delete", Value::Null),
                };
                events.push(
                    Event::new(EventType::Invoke, p as u64, f)
                        .with_key(&key)
                        .with_value(invoke.clone()),
                );
                pending[p] = Some(Pending {
                    f,
                    key,
                    invoke,
                    result: None,
                });
            }
            None => {}
            Some(op) if op.result.is_none() => {
                let cur = state.get(&op.key).cloned().unwrap_or(Value::Null);
                op.result = Some(match op.f {
                    "read" => (EventType::Ok, cur),
                    "write" => {
                        state.insert(op.key.clone(), op.invoke.clone());
                        (EventType::Ok, op.invoke.clone())
                    }
                    "cas" => {
                        if op.invoke[0] == cur {
                            state.insert(op.key.clone(), op.invoke[1].clone());
                            (EventType::Ok, op.invoke.clone())
                        } else {
                            (EventType::Fail, op.invoke.clone())
                        }
                    }
                    _ => {
                        state.remove(&op.key);
                        (EventType::Ok, Value::Null)
                    }
                });
                // Sometimes the client loses the reply before the op took effect.
                if below(&mut rng, 40) == 0 && op.f != "read" {
                    // Undo is not possible, so lose it after the effect instead.
                    let op = pending[p].take().unwrap();
                    events.push(
                        Event::new(EventType::Info, p as u64, op.f)
                            .with_key(&op.key)
                            .with_value(op.invoke),
                    );
                }
            }
            Some(_) => {
                let op = pending[p].take().unwrap();
                let (kind, value) = op.result.unwrap();
                events.push(
                    Event::new(kind, p as u64, op.f)
                        .with_key(&op.key)
                        .with_value(value),
                );
            }
        }
        // Occasionally an op times out before taking effect and never takes effect.
        if below(&mut rng, 200) == 0 {
            let q = below(&mut rng, procs as u64) as usize;
            if let Some(op) = &pending[q]
                && op.result.is_none()
                && op.f != "read"
            {
                let op = pending[q].take().unwrap();
                events.push(
                    Event::new(EventType::Info, q as u64, op.f)
                        .with_key(&op.key)
                        .with_value(op.invoke),
                );
            }
        }
    }
    reindex(&mut events);
    events
}

#[test]
fn generated_register_histories_pass() {
    for seed in 0..20u64 {
        let events = register_history(seed, 300, 4, 6);
        for (name, v) in [
            ("linearizable", linearizable::check_kv(&events)),
            ("relaxed_kv", relaxed_kv::check(&events)),
            ("outcome", outcome::check(&events)),
        ] {
            assert!(v.valid, "seed {seed} {name}: {:#?}", v.anomalies);
        }
    }
}

#[test]
fn performance_2000_ops_over_20_keys_under_2s() {
    let events = register_history(7, 2000, 20, 10);
    let start = Instant::now();
    let v = linearizable::check_kv(&events);
    let elapsed = start.elapsed();
    println!("checked {} ops in {elapsed:?}", v.checked_ops);
    assert!(v.valid, "{:#?}", v.anomalies);
    assert!(v.checked_ops >= 1500, "only {} ops checked", v.checked_ops);
    assert!(elapsed.as_secs_f64() < 2.0, "took {elapsed:?}");
}

/// Planted bug: a read returns a value whose write was invoked only after the read completed.
#[test]
fn planted_future_read_is_caught() {
    let mut caught = 0;
    for seed in 0..10u64 {
        let mut events = register_history(seed, 300, 4, 6);
        // Find an ok read and a write to the same key invoked after the read completed.
        let mut target = None;
        'outer: for (i, e) in events.iter().enumerate() {
            if e.kind == EventType::Ok && e.f == "read" {
                for w in &events[i + 1..] {
                    if w.kind == EventType::Invoke && w.f == "write" && w.key == e.key {
                        target = Some((i, w.value.clone()));
                        break 'outer;
                    }
                }
            }
        }
        let (i, v) = target.expect("generated history has a read followed by a write");
        events[i].value = v;
        let verdict = linearizable::check_kv(&events);
        assert!(
            verdict.has("non-linearizable"),
            "seed {seed}: future read not caught"
        );
        assert!(
            relaxed_kv::check(&events).has("invented-value"),
            "seed {seed}: relaxed missed it"
        );
        caught += 1;
    }
    assert_eq!(caught, 10);
}

/// A correct list-append store with atomic transactions.
fn append_history(seed: u64, txns: usize, keys: usize, procs: usize) -> Vec<Event> {
    let mut rng = SplitMix64::new(seed);
    let mut state: HashMap<String, Vec<Value>> = HashMap::new();
    let mut pending: Vec<Option<(Value, Option<Value>)>> = (0..procs).map(|_| None).collect();
    let mut events = Vec::new();
    let mut next = 0u64;
    let mut invoked = 0;
    while invoked < txns || pending.iter().any(Option::is_some) {
        let p = below(&mut rng, procs as u64) as usize;
        match &mut pending[p] {
            None if invoked < txns => {
                invoked += 1;
                let n = 1 + below(&mut rng, 4);
                let mops: Vec<Value> = (0..n)
                    .map(|_| {
                        let k = format!("x{}", below(&mut rng, keys as u64));
                        if below(&mut rng, 2) == 0 {
                            json!(["r", k, null])
                        } else {
                            next += 1;
                            json!(["append", k, next])
                        }
                    })
                    .collect();
                let v = Value::Array(mops);
                events.push(Event::new(EventType::Invoke, p as u64, "txn").with_value(v.clone()));
                pending[p] = Some((v, None));
            }
            None => {}
            Some((v, done @ None)) => {
                let mut out = Vec::new();
                for m in v.as_array().unwrap() {
                    let k = m[1].as_str().unwrap().to_string();
                    if m[0] == "r" {
                        out.push(json!(["r", k, state.get(&k).cloned().unwrap_or_default()]));
                    } else {
                        state.entry(k).or_default().push(m[2].clone());
                        out.push(m.clone());
                    }
                }
                *done = Some(Value::Array(out));
            }
            Some(_) => {
                let (_, done) = pending[p].take().unwrap();
                events.push(Event::new(EventType::Ok, p as u64, "txn").with_value(done.unwrap()));
            }
        }
    }
    reindex(&mut events);
    events
}

#[test]
fn generated_append_histories_pass_and_a_stale_read_is_caught() {
    for seed in 0..10u64 {
        let mut events = append_history(seed, 200, 4, 5);
        let v = txn::check(&events);
        assert!(v.valid, "seed {seed}: {:#?}", v.anomalies);
        assert!(outcome::check(&events).valid);

        // Planted bug: a read misses an append whose transaction completed before it began.
        let completed_before = |events: &[Event], pos: usize, val: &Value| -> bool {
            let invoke_pos = events[..pos]
                .iter()
                .rposition(|e| e.process == events[pos].process && e.kind == EventType::Invoke)
                .unwrap();
            events[..invoke_pos].iter().any(|e| {
                e.kind == EventType::Ok
                    && e.value
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|m| m[0] == "append" && &m[2] == val)
            })
        };
        let mut planted = false;
        'outer: for pos in 0..events.len() {
            if events[pos].kind != EventType::Ok {
                continue;
            }
            let mops = events[pos].value.as_array().unwrap().clone();
            for (mi, m) in mops.iter().enumerate() {
                if m[0] != "r" || mops[..mi].iter().any(|x| x[0] == "append" && x[1] == m[1]) {
                    continue;
                }
                let list = m[2].as_array().unwrap();
                if let Some(cut) = list
                    .iter()
                    .rposition(|val| completed_before(&events, pos, val))
                {
                    let mut mops = mops.clone();
                    mops[mi] = json!(["r", m[1], list[..cut].to_vec()]);
                    events[pos].value = Value::Array(mops);
                    planted = true;
                    break 'outer;
                }
            }
        }
        assert!(planted, "seed {seed}: no read to corrupt");
        let v = txn::check(&events);
        assert!(!v.valid, "seed {seed}: stale read not caught");
        assert!(
            v.anomalies.iter().any(|a| a.kind.starts_with('G')),
            "seed {seed}: expected a dependency cycle, got {:?}",
            v.kinds()
        );
    }
}

#[test]
fn append_checker_scales_to_1000_transactions() {
    let events = append_history(3, 1000, 10, 8);
    let start = Instant::now();
    let v = txn::check(&events);
    println!("checked {} txns in {:?}", v.checked_ops, start.elapsed());
    assert!(v.valid, "{:#?}", v.anomalies);
    assert!(start.elapsed().as_secs_f64() < 5.0);
}
