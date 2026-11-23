//! K03 fault-model history sensitivity for CACHE-G05.

use nabedra_check::checker::relaxed_kv;
use nabedra_check::history::{Event, Op, Outcome, pair, payload, read_jsonl};

fn recorded_cache_sim() -> Vec<Event> {
    read_jsonl(include_bytes!("fixtures/cache_sim_clean.jsonl").as_slice()).unwrap()
}

fn is_ok(op: &Op, name: &str) -> bool {
    op.f == name && op.outcome == Outcome::Ok
}

fn preceding_sets<'a>(ops: &'a [Op], later: &'a Op) -> impl Iterator<Item = &'a Op> {
    ops.iter()
        .filter(move |op| is_ok(op, "set") && op.key == later.key && op.precedes(later))
}

fn assert_rejects(events: &[Event], completion: usize, expected: &str) {
    let verdict = relaxed_kv::check(events);
    assert!(!verdict.valid);
    assert!(
        verdict.anomalies.iter().any(|anomaly| {
            anomaly.kind == expected && anomaly.events.contains(&events[completion].index)
        }),
        "expected {expected} at the changed reply, got {:#?}",
        verdict.anomalies,
    );
}

#[test]
fn cache_sim_clean_history_passes() {
    let events = recorded_cache_sim();
    let verdict = relaxed_kv::check(&events);
    assert!(verdict.valid, "{:#?}", verdict.anomalies);
    assert_eq!(verdict.checked_ops, 5);
    assert!(events.iter().any(|event| event.f == "failover"));
}

#[test]
fn cache_sim_rejects_resurrection_after_delete() {
    let mut events = recorded_cache_sim();
    let ops = pair(&events).unwrap().ops;
    let (read, deleted_value) = ops
        .iter()
        .filter(|op| is_ok(op, "get") && payload(&op.complete_value).is_null())
        .find_map(|read| {
            let delete = ops
                .iter()
                .filter(|op| {
                    is_ok(op, "del")
                        && op.key == read.key
                        && op.precedes(read)
                        && payload(&op.complete_value).as_i64() == Some(1)
                })
                .max_by_key(|op| op.ret_pos())?;
            let write = preceding_sets(&ops, delete).max_by_key(|op| op.ret_pos())?;
            Some((read, payload(&write.invoke_value).clone()))
        })
        .expect("recorded acknowledged SET and DEL followed by an absent GET");
    let completion = read.ret_pos();
    events[completion].value["value"] = deleted_value;
    assert_rejects(&events, completion, "resurrection");
}

#[test]
fn cache_sim_rejects_invented_value() {
    let mut events = recorded_cache_sim();
    let ops = pair(&events).unwrap().ops;
    let completion = ops
        .iter()
        .find(|op| is_ok(op, "get") && !payload(&op.complete_value).is_null())
        .expect("recorded successful GET of a written value")
        .ret_pos();
    events[completion].value["value"] = "cache-sim fixture: value never written".into();
    assert_rejects(&events, completion, "invented-value");
}

#[test]
fn cache_sim_rejects_stale_read_within_epoch() {
    let mut events = recorded_cache_sim();
    let ops = pair(&events).unwrap().ops;
    let (read, overwritten_value) = ops
        .iter()
        .filter(|op| is_ok(op, "get") && !payload(&op.complete_value).is_null())
        .find_map(|read| {
            let epoch = read.complete_value["epoch"].as_u64()?;
            let latest = preceding_sets(&ops, read).max_by_key(|op| op.ret_pos())?;
            let latest_value = payload(&latest.invoke_value);
            if latest.complete_value["epoch"].as_u64() != Some(epoch)
                || latest_value != payload(&read.complete_value)
            {
                return None;
            }
            let previous = preceding_sets(&ops, latest)
                .filter(|op| payload(&op.invoke_value) != latest_value)
                .max_by_key(|op| op.ret_pos())?;
            Some((read, payload(&previous.invoke_value).clone()))
        })
        .expect("recorded overwrite and GET in the same epoch");
    let completion = read.ret_pos();
    events[completion].value["value"] = overwritten_value;
    assert_rejects(&events, completion, "stale-read");
}
