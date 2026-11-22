//! A queue checker for the Events log (EVT-G01 to EVT-G03, EVT-G05), modelled on
//! `jepsen.tests.kafka`. The op `key` is the partition (for example `"orders/0"`).
//!
//! | f | invoke value | ok completion value |
//! | --- | --- | --- |
//! | `publish` | one event payload, or an array of payloads for a batch | `{"offsets": [o, ..]}`, `{"offset": o}`, `o` or `[o, ..]` |
//! | `poll` | optional `{"subscription": s}` | `{"events": [d, ..], "after": o, "redelivery": bool}` or `[d, ..]` |
//! | `ack` | `{"offset": o, "subscription": s}`, `{"after": prior, "through": o}` or `o` | optional `{"checkpoint": o}` |
//!
//! A delivery `d` is `[offset, payload]` or `{"offset": o, "value": payload, "redelivered":
//! bool}`. The Events simulator records publish batches as `[[uuid, payload_number], ..]`
//! and receipts as `[[uuid, offset], ..]`. Polls from an explicit `after` checkpoint may
//! repeat unacknowledged deliveries. For implicit cursors, a replay must carry the poll's
//! redelivery flag or the first delivery's flag. Histories start with a fresh log and earliest
//! subscriptions. Payloads are unique. `info` ops (timeouts, "not
//! leader") are unknown outcomes. A `heal` (or `heal_all`) nemesis marker starts the
//! redelivery check.
//!
//! Per partition the checker reports:
//! - `duplicate-offset`: one offset seen with two payloads; `duplicate-event`: one payload at
//!   two offsets;
//! - `offset-gap`: offsets that do not start at 1 or skip a value, either in the delivery stream
//!   or as more unexplained holes below the highest known offset than unknown publishes;
//! - `out-of-order`: a delivery at or below an already delivered offset without a redelivery
//!   mark;
//! - `invented-event`: a poll delivered an event with no prior possible publish;
//! - `lost-publish`: an acknowledged publish was skipped or missing when a poll reached the end;
//! - `redelivery-missing`: after the last `heal`, an unacknowledged event not delivered again
//!   (within `redelivery_bound_ns` of the heal, when set and times are recorded).

use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Op, Outcome, pair};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug)]
pub struct Options {
    pub start_offset: i64,
    pub redelivery_bound_ns: Option<u64>,
    /// Marker names that end a fault (`heal_all` is the real-environment nemesis's name).
    pub heal_markers: Vec<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            start_offset: 1,
            redelivery_bound_ns: None,
            heal_markers: vec!["heal".into(), "heal_all".into()],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    pub offset: Option<i64>,
    pub value: Value,
    pub redelivered: bool,
}

/// The payloads a publish op writes.
pub(crate) fn published(op: &Op) -> Vec<Value> {
    if let Some(ids) = simulator_batch(&op.invoke_value) {
        return ids.into_iter().map(|row| row[0].clone()).collect();
    }
    match &op.invoke_value {
        Value::Array(items) => items.clone(),
        Value::Null => Vec::new(),
        v => vec![v.clone()],
    }
}

// The Events simulator records [UUID, payload number] on invoke and [UUID, offset]
// on completion. A UUID distinguishes this shape from ordinary array payloads.
fn simulator_batch(value: &Value) -> Option<Vec<&[Value]>> {
    let rows = value
        .as_array()?
        .iter()
        .map(|row| {
            let pair = row.as_array().filter(|row| row.len() == 2)?;
            let id = pair[0].as_str()?.as_bytes();
            (id.len() == 36
                && id.iter().enumerate().all(|(i, byte)| {
                    if matches!(i, 8 | 13 | 18 | 23) {
                        *byte == b'-'
                    } else {
                        byte.is_ascii_hexdigit()
                    }
                })
                && pair[1].is_u64())
            .then_some(pair.as_slice())
        })
        .collect::<Option<Vec<_>>>()?;
    (!rows.is_empty()).then_some(rows)
}

fn publish_offsets(op: &Op, payloads: &[Value]) -> Option<Vec<i64>> {
    let v = &op.complete_value;
    let offsets = if simulator_batch(&op.invoke_value).is_some() {
        simulator_batch(v)?
            .into_iter()
            .zip(payloads)
            .map(|(row, id)| (row[0] == *id).then(|| row[1].as_i64()).flatten())
            .collect::<Option<Vec<_>>>()?
    } else if let Some(items) = v
        .as_array()
        .or_else(|| v.get("offsets").and_then(Value::as_array))
    {
        items
            .iter()
            .map(Value::as_i64)
            .collect::<Option<Vec<_>>>()?
    } else {
        vec![v.get("offset").unwrap_or(v).as_i64()?]
    };
    let count = if simulator_batch(&op.invoke_value).is_some() {
        v.as_array()?.len()
    } else {
        offsets.len()
    };
    (count == payloads.len()).then_some(offsets)
}

/// The deliveries of an ok poll.
pub(crate) fn delivered(op: &Op) -> Vec<Delivery> {
    let v = &op.complete_value;
    let poll_flag = v
        .get("redelivery")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let items = match v {
        Value::Array(a) => a.as_slice(),
        Value::Object(m) => m
            .get("events")
            .and_then(Value::as_array)
            .map_or(&[][..], |a| a.as_slice()),
        _ => &[],
    };
    items
        .iter()
        .map(|d| match d {
            Value::Array(p) if p.len() == 2 => Delivery {
                offset: p[0].as_i64(),
                value: p[1].clone(),
                redelivered: poll_flag,
            },
            Value::Object(m) => Delivery {
                offset: m.get("offset").and_then(Value::as_i64),
                value: m.get("value").cloned().unwrap_or(Value::Null),
                redelivered: poll_flag
                    || m.get("redelivered")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
            },
            other => Delivery {
                offset: None,
                value: other.clone(),
                redelivered: poll_flag,
            },
        })
        .collect()
}

fn subscription(op: &Op) -> String {
    for v in [&op.invoke_value, &op.complete_value] {
        if let Some(s) = v.get("subscription") {
            return match s {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
        }
    }
    String::new()
}

fn ack_offset(op: &Op) -> Option<i64> {
    let v = &op.invoke_value;
    let through = v
        .get("offset")
        .or_else(|| v.get("through"))
        .unwrap_or(v)
        .as_i64()?;
    if v.get("after")
        .is_some_and(|after| after.as_i64().is_none_or(|after| after >= through))
    {
        return None;
    }
    Some(through)
}

fn poll_cursor(op: &Op) -> Option<i64> {
    op.complete_value.get("after").and_then(Value::as_i64)
}

fn malformed(op: &Op, message: &str, anomalies: &mut Vec<Anomaly>) {
    anomalies.push(Anomaly::new(
        "malformed-history",
        format!("{} at event {}: {message}", op.f, op.invoke_index),
        op.indices(),
    ));
}

pub fn check(events: &[Event]) -> Verdict {
    check_with(events, &Options::default())
}

#[derive(Default)]
struct Partition<'a> {
    publishes: Vec<&'a Op>,
    polls: Vec<&'a Op>,
    acks: Vec<&'a Op>,
}

pub fn check_with(events: &[Event], opts: &Options) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let mut parts: BTreeMap<String, Partition> = BTreeMap::new();
    let mut checked = 0;
    for op in &paired.ops {
        if !matches!(op.f.as_str(), "publish" | "poll" | "ack") {
            continue;
        }
        let Some(key) = &op.key else {
            return Verdict::malformed(format!(
                "{} at event {} has no partition",
                op.f, op.invoke_index
            ));
        };
        let p = parts.entry(key.clone()).or_default();
        match op.f.as_str() {
            "publish" => p.publishes.push(op),
            "poll" => p.polls.push(op),
            "ack" => p.acks.push(op),
            _ => continue,
        }
        checked += 1;
    }
    let heal = paired
        .markers
        .iter()
        .filter(|m| {
            m.applied()
                && opts.heal_markers.contains(&m.f)
                && m.value.get("failed").and_then(Value::as_u64).unwrap_or(0) == 0
        })
        .max_by_key(|m| m.pos);
    let mut anomalies = Vec::new();
    for (name, p) in &parts {
        check_partition(name, p, opts, heal, &mut anomalies);
    }
    Verdict::new(checked, anomalies)
}

fn checkpoints(p: &Partition, opts: &Options, sub: &str, op: &Op) -> (i64, i64) {
    let mut confirmed = opts.start_offset.saturating_sub(1);
    let mut possible = confirmed;
    for ack in p
        .acks
        .iter()
        .filter(|a| a.outcome != Outcome::Fail && a.call < op.ret_pos() && subscription(a) == sub)
    {
        if let Some(through) = ack_offset(ack) {
            possible = possible.max(through);
            if ack.outcome == Outcome::Ok && ack.precedes(op) {
                confirmed = confirmed.max(through);
            }
        }
    }
    (confirmed, possible)
}

fn check_partition<'a>(
    name: &str,
    p: &Partition<'a>,
    opts: &Options,
    heal: Option<&crate::history::Marker>,
    anomalies: &mut Vec<Anomaly>,
) {
    // offset -> (canonical payload, payload, earliest completed receipt or delivery)
    let mut at: BTreeMap<i64, (String, Value, &Op)> = BTreeMap::new();
    let mut offsets_of: HashMap<String, BTreeSet<i64>> = HashMap::new();
    let mut note = |anomalies: &mut Vec<Anomaly>, o: i64, v: &Value, src: &'a Op| {
        let c = canon(v);
        if o < opts.start_offset {
            anomalies.push(Anomaly::new(
                "offset-gap",
                format!(
                    "partition {name:?}: offset {o} is below the first offset {}",
                    opts.start_offset
                ),
                src.indices(),
            ));
        }
        offsets_of.entry(c.clone()).or_default().insert(o);
        match at.get(&o) {
            None => {
                at.insert(o, (c, v.clone(), src));
            }
            Some((prev, pv, first)) if *prev != c => anomalies.push(Anomaly::new(
                "duplicate-offset",
                format!(
                    "partition {name:?}: offset {o} holds both {pv} (event {}) and {v} (event {})",
                    first.invoke_index, src.invoke_index
                ),
                [first.indices(), src.indices()].concat(),
            )),
            Some((_, _, first)) if src.ret_pos() < first.ret_pos() => {
                at.insert(o, (c, v.clone(), src));
            }
            Some(_) => {}
        }
    };
    let mut unknown: HashMap<String, usize> = HashMap::new();
    let mut possible: HashMap<String, usize> = HashMap::new();
    let mut acked_publishes: Vec<(i64, Value, &Op)> = Vec::new();
    for op in &p.publishes {
        let payloads = published(op);
        if op.outcome != Outcome::Fail {
            for value in &payloads {
                let event = canon(value);
                possible.entry(event.clone()).or_insert(op.call);
                if op.outcome == Outcome::Info {
                    unknown.entry(event).or_insert(op.call);
                }
            }
        }
        if op.outcome == Outcome::Ok {
            if let Some(offsets) =
                publish_offsets(op, &payloads).filter(|offsets| !offsets.is_empty())
            {
                for (value, offset) in payloads.into_iter().zip(offsets) {
                    note(anomalies, offset, &value, op);
                    acked_publishes.push((offset, value, op));
                }
            } else {
                malformed(
                    op,
                    "expected one matching offset per published event",
                    anomalies,
                );
            }
        }
    }
    let mut deliveries = BTreeSet::new();
    let mut max_delivered: Option<i64> = None;
    for op in p.polls.iter().filter(|o| o.outcome == Outcome::Ok) {
        let value = &op.complete_value;
        if !(value.is_array() || value.get("events").is_some_and(Value::is_array))
            || (value.get("after").is_some() && poll_cursor(op).is_none())
        {
            malformed(
                op,
                "expected an events list and a numeric cursor when present",
                anomalies,
            );
        }
        for d in delivered(op) {
            if let Some(o) = d.offset {
                note(anomalies, o, &d.value, op);
                max_delivered = max_delivered.max(Some(o));
                deliveries.insert((o, canon(&d.value)));
            } else {
                malformed(op, "delivery has no valid offset", anomalies);
            }
            if !possible
                .get(&canon(&d.value))
                .is_some_and(|&call| call < op.ret_pos())
            {
                anomalies.push(Anomaly::new(
                    "invented-event",
                    format!(
                        "partition {name:?}: poll returned {} without a possible publish",
                        d.value
                    ),
                    op.indices(),
                ));
            }
        }
    }
    for op in p.acks.iter().filter(|op| op.outcome == Outcome::Ok) {
        let Some(through) = ack_offset(op) else {
            malformed(op, "invalid acknowledgment offset", anomalies);
            continue;
        };
        if let Some(reply) = op.complete_value.get("checkpoint") {
            let sub = subscription(op);
            let (known, possible) = checkpoints(p, opts, &sub, op);
            if reply
                .as_i64()
                .is_none_or(|checkpoint| checkpoint < known.max(through) || checkpoint > possible)
            {
                malformed(
                    op,
                    "acknowledgment checkpoint is outside the possible acknowledged prefix",
                    anomalies,
                );
            }
        }
    }
    for (c, offs) in &offsets_of {
        if offs.len() > 1 {
            anomalies.push(Anomaly::new(
                "duplicate-event",
                format!("partition {name:?}: event {c} appears at offsets {offs:?}"),
                Vec::new(),
            ));
        }
    }

    // Each missing offset requires a distinct unknown publish invoked before the receipt
    // or delivery of the higher offset. Count without walking the numeric offset range.
    let mut unresolved_calls: Vec<_> = unknown
        .iter()
        .filter_map(|(event, call)| (!offsets_of.contains_key(event)).then_some(*call))
        .collect();
    unresolved_calls.sort_unstable();
    let mut latest_lower_invoke = 0;
    for (known, (&offset, (value, _, source))) in at.range(opts.start_offset..).enumerate() {
        let holes = i128::from(offset) - i128::from(opts.start_offset) - known as i128;
        let unresolved = unresolved_calls.partition_point(|&call| call < source.ret_pos());
        if holes > unresolved as i128 {
            anomalies.push(Anomaly::new(
                "offset-gap",
                format!("partition {name:?}: offset {offset} has {holes} preceding holes and only {unresolved} possible unknown publishes"),
                source.indices(),
            ));
        }
        if latest_lower_invoke > source.ret_pos() {
            anomalies.push(Anomaly::new(
                "out-of-order",
                format!("partition {name:?}: offset {offset} was observed before an event at a lower offset was invoked"),
                source.indices(),
            ));
        }
        if let Some(&call) = possible.get(value) {
            latest_lower_invoke = latest_lower_invoke.max(call);
        }
    }

    // Delivery streams, one per subscription, in completion order.
    let mut streams: BTreeMap<String, Vec<&Op>> = BTreeMap::new();
    for op in &p.polls {
        streams.entry(subscription(op)).or_default().push(op);
    }
    for (sub, polls) in &mut streams {
        polls.sort_by_key(|o| o.done.unwrap_or(usize::MAX));
        let mut expected = opts.start_offset;
        let mut uncertain = false;
        for op in polls.iter() {
            match op.outcome {
                Outcome::Info => {
                    uncertain = true;
                    continue;
                }
                Outcome::Fail => continue,
                Outcome::Ok => {}
            }
            let ds = delivered(op);
            let cursor = poll_cursor(op);
            let redelivery = ds.first().is_some_and(|d| d.redelivered);
            if let Some(after) = cursor {
                let (confirmed, possible) = checkpoints(p, opts, sub, op);
                if after < confirmed || after > possible {
                    anomalies.push(Anomaly::new(
                        "out-of-order",
                        format!("partition {name:?} subscription {sub:?}: checkpoint {after} outside {confirmed}..={possible}"),
                        op.indices(),
                    ));
                }
            }
            let at_event = op.complete_index.unwrap_or(op.invoke_index);
            let mut prev: Option<i64> = None;
            for d in &ds {
                let Some(o) = d.offset else { continue };
                let problem = match (prev, cursor) {
                    (Some(p), _) if o <= p => {
                        Some(("out-of-order", format!("offset {o} after {p} in one poll")))
                    }
                    (Some(p), _) if o > p.saturating_add(1) => {
                        Some(("offset-gap", format!("offset {o} after {p} in one poll")))
                    }
                    (Some(_), _) => None,
                    (None, Some(after)) => (after.checked_add(1) != Some(o)).then(|| {
                        (
                            if o <= after {
                                "out-of-order"
                            } else {
                                "offset-gap"
                            },
                            format!("poll after {after} returned offset {o} first"),
                        )
                    }),
                    (None, None) if redelivery => (o > expected && !uncertain).then(|| {
                        (
                            "offset-gap",
                            format!("redelivery starts at {o}, beyond the next offset {expected}"),
                        )
                    }),
                    (None, None) if o < expected => Some((
                        "out-of-order",
                        format!(
                            "offset {o} delivered again without a redelivery mark (next was {expected})"
                        ),
                    )),
                    (None, None) if o > expected && !uncertain => Some((
                        "offset-gap",
                        format!("delivered offset {o}, expected {expected}"),
                    )),
                    (None, None) => None,
                };
                if let Some((kind, what)) = problem {
                    anomalies.push(Anomaly::new(
                        kind,
                        format!(
                            "partition {name:?} subscription {sub:?}: {what} (event {at_event})"
                        ),
                        op.indices(),
                    ));
                }
                prev = Some(o);
            }
            if let Some(last) = prev {
                expected = if redelivery {
                    last.saturating_add(1)
                } else {
                    expected.max(last.saturating_add(1))
                };
                uncertain = false;
            }
        }
    }

    // Acknowledged publishes must be delivered once the stream has passed them.
    if let Some(maxd) = max_delivered {
        for (o, v, op) in &acked_publishes {
            if *o < maxd && !deliveries.contains(&(*o, canon(v))) {
                anomalies.push(Anomaly::new(
                    "lost-publish",
                    format!("partition {name:?}: publish of {v} acknowledged at offset {o} (event {}) was never delivered, though offset {maxd} was",
                        op.complete_index.unwrap_or(op.invoke_index)),
                    op.indices(),
                ));
            }
        }
    }

    // An empty poll from a known checkpoint observes the end of this fresh log.
    for op in p
        .polls
        .iter()
        .filter(|op| op.outcome == Outcome::Ok && delivered(op).is_empty())
    {
        if let Some(after) = poll_cursor(op) {
            if let Some((offset, _, published)) = acked_publishes
                .iter()
                .find(|(o, _, w)| *o > after && w.precedes(op))
            {
                anomalies.push(Anomaly::new(
                    "lost-publish",
                    format!("partition {name:?}: empty poll after checkpoint {after} omitted acknowledged offset {offset}"),
                    [published.indices(), op.indices()].concat(),
                ));
            }
        }
    }

    // Redelivery after the last heal.
    let Some(heal) = heal else { return };
    for (sub, polls) in &streams {
        let deadline = match (opts.redelivery_bound_ns, heal.time_ns) {
            (Some(b), Some(t)) => Some(t.saturating_add(b)),
            (Some(_), None) => {
                anomalies.push(Anomaly::new(
                    "check-limit",
                    "redelivery bound needs a timestamp on the heal".into(),
                    vec![heal.index],
                ));
                continue;
            }
            _ => None,
        };
        let acked = p
            .acks
            .iter()
            .filter(|a| a.outcome != Outcome::Fail && a.call < heal.pos && subscription(a) == *sub)
            .filter_map(|a| ack_offset(a))
            .max()
            .unwrap_or(opts.start_offset.saturating_sub(1));
        let mut required = BTreeSet::new();
        for (o, _, publish) in &acked_publishes {
            if *o > acked && publish.ret_pos() < heal.pos {
                required.insert(*o);
            }
        }
        for op in polls
            .iter()
            .filter(|o| o.outcome == Outcome::Ok && o.ret_pos() < heal.pos)
        {
            for d in delivered(op) {
                if let Some(o) = d.offset.filter(|&o| o > acked) {
                    required.insert(o);
                }
            }
        }
        let mut after: BTreeSet<i64> = BTreeSet::new();
        for op in polls
            .iter()
            .filter(|o| o.outcome == Outcome::Ok && o.call > heal.pos)
        {
            if deadline.is_some_and(|d| op.ret_time.is_none_or(|t| t > d)) {
                continue;
            }
            for d in delivered(op) {
                if let Some(o) = d.offset {
                    after.insert(o);
                }
            }
        }
        let missing: Vec<_> = required.difference(&after).copied().take(16).collect();
        if !missing.is_empty() {
            anomalies.push(Anomaly::new(
                "redelivery-missing",
                format!(
                    "partition {name:?} subscription {sub:?}: unacknowledged offsets {:?} (acked through {acked}) were not delivered after the heal at event {}{}",
                    missing,
                    heal.index,
                    if deadline.is_some() { " within the bound" } else { "" }
                ),
                vec![heal.index],
            ));
        }
    }
}
