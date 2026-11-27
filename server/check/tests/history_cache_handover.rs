//! CACHE-G05 loss bounds of the relaxed Cache checker (SIM-2): no acknowledged write may be lost
//! across a planned handover, and a failover loses only writes acknowledged within the lag
//! window before the old primary's authority ended.

use nabedra_check::checker::relaxed_kv::{Options, check, check_with};
use nabedra_check::history::{Event, EventType, NEMESIS, reindex};
use serde_json::{Value, json};

const SECOND: u64 = 1_000_000_000;

fn event(kind: EventType, process: u64, f: &str, value: Value, time_ns: u64) -> Event {
    let mut event = Event::new(kind, process, f).with_key("k").with_value(value);
    event.time_ns = Some(time_ns);
    event
}

/// A SET acknowledged at `write_ns` in epoch 1, an owner change marker at `marker_ns`, and a
/// read in epoch 2 at `read_ns` that misses the SET.
fn lost_set(
    marker: &str,
    marker_value: Value,
    write_ns: u64,
    marker_ns: u64,
    read_ns: u64,
) -> Vec<Event> {
    let mut events = vec![
        event(EventType::Invoke, 1, "set", json!("a"), write_ns),
        event(
            EventType::Ok,
            1,
            "set",
            json!({"value": null, "epoch": 1, "sequence": 1}),
            write_ns,
        ),
        event(EventType::Info, NEMESIS, marker, marker_value, marker_ns),
        event(EventType::Invoke, 1, "get", Value::Null, read_ns),
        event(
            EventType::Ok,
            1,
            "get",
            json!({"value": null, "epoch": 2}),
            read_ns,
        ),
    ];
    reindex(&mut events);
    events
}

fn window(ns: u64) -> Options {
    Options {
        lag_window_ns: Some(ns),
        ..Options::default()
    }
}

#[test]
fn k03_loss_across_a_planned_handover_is_flagged_with_or_without_a_window() {
    let events = lost_set(
        "handover",
        json!({"epoch": 2}),
        SECOND,
        2 * SECOND,
        3 * SECOND,
    );
    assert!(
        check(&events).has("lost-write"),
        "{:?}",
        check(&events).anomalies
    );
    assert!(check_with(&events, &window(10 * SECOND)).has("lost-write"));

    // The same history across a failover is the asynchronous loss CACHE-G05 allows.
    let failover = lost_set(
        "failover",
        json!({"epoch": 2}),
        SECOND,
        2 * SECOND,
        3 * SECOND,
    );
    assert!(check(&failover).valid, "{:?}", check(&failover).anomalies);
    assert!(check_with(&failover, &window(SECOND)).valid);
}

#[test]
fn k03_a_retained_write_is_not_lost_across_a_handover_then_a_failover() {
    // The handover kept the write and a later failover's copy has the prefix of epoch 2, so
    // only the window can allow losing an epoch 1 write. It was acknowledged long before.
    let mut events = lost_set(
        "handover",
        json!({"epoch": 2}),
        SECOND,
        2 * SECOND,
        30 * SECOND,
    );
    let failover = event(
        EventType::Info,
        NEMESIS,
        "failover",
        json!({"epoch": 3, "prefix_epoch": 2, "prefix_sequence": 0, "authority_end_ns": 20 * SECOND}),
        21 * SECOND,
    );
    events.insert(3, failover);
    let read = events.len() - 1;
    events[read].value["epoch"] = 3.into();
    reindex(&mut events);
    assert!(check_with(&events, &window(5 * SECOND)).has("lost-write"));
}

#[test]
fn k03_failover_loss_is_bounded_by_the_window_before_the_authority_end() {
    // The commit came long after the old grant expired. Measured from the marker the write is
    // 10 s old. Measured from the grant's end it is within the 2 s window, and allowed.
    let late_commit = lost_set(
        "failover",
        json!({"epoch": 2, "authority_end_ns": 2 * SECOND}),
        SECOND,
        11 * SECOND,
        12 * SECOND,
    );
    assert!(check_with(&late_commit, &window(2 * SECOND)).valid);
    let without_end = lost_set(
        "failover",
        json!({"epoch": 2}),
        SECOND,
        11 * SECOND,
        12 * SECOND,
    );
    assert!(check_with(&without_end, &window(2 * SECOND)).has("lost-write"));

    // A write acknowledged 3 s before the grant ended is outside a 2 s window.
    let old_write = lost_set(
        "failover",
        json!({"epoch": 2, "authority_end_ns": 4 * SECOND}),
        SECOND,
        5 * SECOND,
        6 * SECOND,
    );
    assert!(check_with(&old_write, &window(2 * SECOND)).has("lost-write"));
    assert!(
        check(&old_write).valid,
        "no window: any loss above the prefix is allowed"
    );

    // Inside the retained prefix nothing may be lost, window or not.
    let retained = lost_set(
        "failover",
        json!({"epoch": 2, "prefix_epoch": 1, "prefix_sequence": 1, "authority_end_ns": 2 * SECOND}),
        SECOND,
        3 * SECOND,
        4 * SECOND,
    );
    assert!(check(&retained).has("lost-write"));
}
