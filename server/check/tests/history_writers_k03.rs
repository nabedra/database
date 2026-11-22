use nabedra_check::checker::Verdict;
use nabedra_check::checker::relaxed_kv::{Options, check_with};
use nabedra_check::history::{Event, EventType, read_jsonl};
use serde_json::Value;

// Recorded by scripts.testing.realenv.test_cache_cluster.record_k03_fixture.
fn recorded_k03() -> Vec<Event> {
    read_jsonl(include_bytes!("fixtures/k03_history.jsonl").as_slice()).unwrap()
}

fn check(events: &[Event]) -> Verdict {
    // Require a scoped marker even when the serving epoch increases.
    check_with(
        events,
        &Options {
            lag_window_ns: Some(10_000_000),
            ..Options::default()
        },
    )
}

#[test]
fn recorded_cold_reset_passes() {
    let mut events = recorded_k03();
    let verdict = check(&events);
    assert!(verdict.valid, "{:?}", verdict.anomalies);
    assert_eq!(verdict.checked_ops, 8);

    events.retain(|event| event.marker_name() != "cold-reset");
    let verdict = check(&events);
    assert!(verdict.has("stale-read"), "{:?}", verdict.anomalies);
}

#[test]
fn recorded_lost_write_requires_a_failover_marker_for_its_key() {
    let mut events = recorded_k03();
    let after_promotion = events
        .iter_mut()
        .find(|event| event.f == "get" && event.kind == EventType::Ok && event.value["epoch"] == 2)
        .unwrap();
    after_promotion.value["value"] = Value::Null;
    assert!(!check(&events).has("lost-write"));

    events.retain(|event| !matches!(event.marker_name().as_str(), "failover" | "promote"));
    let verdict = check(&events);
    assert!(verdict.has("lost-write"), "{:?}", verdict.anomalies);
}
