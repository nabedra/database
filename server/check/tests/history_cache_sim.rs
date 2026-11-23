//! K03 fault-model history sensitivity for CACHE-G05.

use nabedra_check::checker::relaxed_kv;
use nabedra_check::history::{Event, EventType, NEMESIS, Op, Outcome, pair, payload, read_jsonl};

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

#[test]
fn cache_sim_seed_46_expired_overwrite() {
    let mut events =
        read_jsonl(include_bytes!("fixtures/cache_sim_seed_46_expired_overwrite.jsonl").as_slice())
            .unwrap();
    let verdict = relaxed_kv::check(&events);
    assert!(verdict.valid, "{:#?}", verdict.anomalies);
    assert_eq!(verdict.checked_ops, 3);

    let ops = pair(&events).unwrap().ops;
    let read = ops.iter().find(|op| is_ok(op, "get")).unwrap();
    let replacement = &ops[1];
    let expiry = replacement.invoke_value["at_ns"].as_u64().unwrap();
    let mut at_deadline = events.clone();
    at_deadline[read.call].time_ns = Some(expiry);
    at_deadline[read.ret_pos()].time_ns = Some(expiry + 1);
    assert!(relaxed_kv::check(&at_deadline).valid);

    let writes: Vec<_> = preceding_sets(&ops, read).collect();
    for write in writes {
        // The earlier value still has time left on its own TTL. The overwrite
        // consumed that version, even when the replacement itself expired.
        events[read.ret_pos()].value["value"] = payload(&write.invoke_value).clone();
        assert_rejects(&events, read.ret_pos(), "resurrection");
    }

    let first = &ops[0];
    let mut failover =
        Event::new(EventType::Info, NEMESIS, "failover").with_key(read.key.as_deref().unwrap());
    failover.time_ns = Some(read.call_time.unwrap() - 1);
    events[read.ret_pos()].value["epoch"] = 2.into();
    events[read.ret_pos()].value["value"] = payload(&first.invoke_value).clone();
    events.insert(read.call, failover);
    assert_rejects(&events, read.ret_pos() + 1, "resurrection");

    // A failover before the replacement's deadline can lose that write.
    // The older version may survive if its own deadline has not passed.
    events[read.call].time_ns = Some(replacement.ret_time.unwrap() + 1);
    let verdict = relaxed_kv::check(&events);
    assert!(verdict.valid, "{:#?}", verdict.anomalies);
}
