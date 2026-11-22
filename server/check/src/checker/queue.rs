//! A queue checker for the Events log (EVT-G01 to EVT-G03, EVT-G05), modelled on
//! `jepsen.tests.kafka`. The op `key` is the partition (for example `"orders/0"`).
//!
//! | f | invoke value | ok completion value |
//! | --- | --- | --- |
//! | `publish` | one event payload, or an array of payloads for a batch | `{"offsets": [o, ..]}`, `{"offset": o}`, `o` or `[o, ..]` |
//! | `poll` | optional `{"subscription": s}` | `{"events": [d, ..], "redelivery": bool}` or `[d, ..]` |
//! | `ack` | `{"offset": o, "subscription": s}` or `o` (a prefix ack up to `o`) | ignored |
//!
//! A delivery `d` is `[offset, payload]` or `{"offset": o, "value": payload, "redelivered":
//! bool}`. A poll is a redelivery when the poll or any of its deliveries is marked so; the
//! consumer marks it after a lease loss. Payloads are unique. `info` ops (timeouts, "not
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
//! - `lost-publish`: an acknowledged publish never delivered although a later offset was;
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
    match &op.invoke_value {
        Value::Array(items) => items.clone(),
        Value::Null => Vec::new(),
        v => vec![v.clone()],
    }
}

fn publish_offsets(op: &Op) -> Vec<Option<i64>> {
    let v = &op.complete_value;
    let list = match v {
        Value::Number(n) => vec![n.as_i64()],
        Value::Array(a) => a.iter().map(Value::as_i64).collect(),
        Value::Object(m) => match (m.get("offsets"), m.get("offset")) {
            (Some(Value::Array(a)), _) => a.iter().map(Value::as_i64).collect(),
            (_, Some(o)) => vec![o.as_i64()],
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    list
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
    match &op.invoke_value {
        Value::Number(n) => n.as_i64(),
        v => v.get("offset").and_then(Value::as_i64),
    }
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
        let p = parts.entry(op.key.clone().unwrap_or_default()).or_default();
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
        .filter(|m| m.applied() && opts.heal_markers.contains(&m.f))
        .max_by_key(|m| m.pos);
    let mut anomalies = Vec::new();
    for (name, p) in &parts {
        check_partition(name, p, opts, heal, &mut anomalies);
    }
    Verdict::new(checked, anomalies)
}

fn check_partition(
    name: &str,
    p: &Partition,
    opts: &Options,
    heal: Option<&crate::history::Marker>,
    anomalies: &mut Vec<Anomaly>,
) {
    // offset -> (canonical payload, payload, first source op's invoke index and event indices)
    let mut at: BTreeMap<i64, (String, Value, usize, Vec<usize>)> = BTreeMap::new();
    let mut offsets_of: HashMap<String, BTreeSet<i64>> = HashMap::new();
    let mut note = |anomalies: &mut Vec<Anomaly>, o: i64, v: &Value, src: &Op| {
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
                at.insert(o, (c, v.clone(), src.invoke_index, src.indices()));
            }
            Some((prev, pv, pinv, pidx)) if *prev != c => anomalies.push(Anomaly::new(
                "duplicate-offset",
                format!("partition {name:?}: offset {o} holds both {pv} (event {pinv}) and {v} (event {})",
                    src.invoke_index),
                [pidx.clone(), src.indices()].concat(),
            )),
            Some(_) => {}
        }
    };
    let mut unknown_events = 0usize;
    let mut acked_publishes: Vec<(i64, Value, &Op)> = Vec::new();
    for op in &p.publishes {
        let payloads = published(op);
        match op.outcome {
            Outcome::Info => unknown_events += payloads.len(),
            Outcome::Fail => {}
            Outcome::Ok => {
                let offs = publish_offsets(op);
                for (i, v) in payloads.iter().enumerate() {
                    if let Some(Some(o)) = offs.get(i) {
                        note(anomalies, *o, v, op);
                        acked_publishes.push((*o, v.clone(), op));
                    }
                }
            }
        }
    }
    let mut delivered_values: HashMap<String, usize> = HashMap::new();
    let mut max_delivered: Option<i64> = None;
    for op in p.polls.iter().filter(|o| o.outcome == Outcome::Ok) {
        for d in delivered(op) {
            if let Some(o) = d.offset {
                note(anomalies, o, &d.value, op);
                max_delivered = max_delivered.max(Some(o));
            }
            delivered_values.entry(canon(&d.value)).or_insert(op.call);
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

    // Holes below the highest known offset that no unknown publish can explain.
    if let Some((&max, _)) = at.iter().next_back() {
        let holes: Vec<i64> = (opts.start_offset..=max)
            .filter(|o| !at.contains_key(o))
            .collect();
        if holes.len() > unknown_events {
            anomalies.push(Anomaly::new(
                "offset-gap",
                format!(
                    "partition {name:?}: offsets {:?} below {max} were never assigned, and only {unknown_events} publish outcomes are unknown",
                    &holes[..holes.len().min(16)]
                ),
                Vec::new(),
            ));
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
            let redelivery = ds.iter().any(|d| d.redelivered);
            let at_event = op.complete_index.unwrap_or(op.invoke_index);
            let mut prev: Option<i64> = None;
            for d in &ds {
                let Some(o) = d.offset else { continue };
                let problem = match prev {
                    Some(p) if o <= p => {
                        Some(("out-of-order", format!("offset {o} after {p} in one poll")))
                    }
                    Some(p) if o > p + 1 => {
                        Some(("offset-gap", format!("offset {o} after {p} in one poll")))
                    }
                    Some(_) => None,
                    None if redelivery => (o > expected && !uncertain).then(|| {
                        (
                            "offset-gap",
                            format!("redelivery starts at {o}, beyond the next offset {expected}"),
                        )
                    }),
                    None if o < expected => Some((
                        "out-of-order",
                        format!(
                            "offset {o} delivered again without a redelivery mark (next was {expected})"
                        ),
                    )),
                    None if o > expected && !uncertain => Some((
                        "offset-gap",
                        format!("delivered offset {o}, expected {expected}"),
                    )),
                    None => None,
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
                    last + 1
                } else {
                    expected.max(last + 1)
                };
                uncertain = false;
            }
        }
    }

    // Acknowledged publishes must be delivered once the stream has passed them.
    if let Some(maxd) = max_delivered {
        for (o, v, op) in &acked_publishes {
            if *o < maxd && !delivered_values.contains_key(&canon(v)) {
                anomalies.push(Anomaly::new(
                    "lost-publish",
                    format!("partition {name:?}: publish of {v} acknowledged at offset {o} (event {}) was never delivered, though offset {maxd} was",
                        op.complete_index.unwrap_or(op.invoke_index)),
                    op.indices(),
                ));
            }
        }
    }

    // Redelivery after the last heal.
    let Some(heal) = heal else { return };
    for (sub, polls) in &streams {
        if !polls.iter().any(|o| o.outcome == Outcome::Ok) {
            continue;
        }
        let acked = p
            .acks
            .iter()
            .filter(|a| a.outcome != Outcome::Fail && subscription(a) == *sub)
            .filter_map(|a| ack_offset(a))
            .max()
            .unwrap_or(opts.start_offset - 1);
        let mut required: BTreeMap<i64, String> = BTreeMap::new();
        for (o, v, _) in &acked_publishes {
            if *o > acked {
                required.insert(*o, canon(v));
            }
        }
        for op in polls.iter().filter(|o| o.outcome == Outcome::Ok) {
            for d in delivered(op) {
                if let Some(o) = d.offset.filter(|&o| o > acked) {
                    required.entry(o).or_insert_with(|| canon(&d.value));
                }
            }
        }
        let deadline = match (opts.redelivery_bound_ns, heal.time_ns) {
            (Some(b), Some(t)) => Some(t.saturating_add(b)),
            _ => None,
        };
        let mut after: BTreeSet<i64> = BTreeSet::new();
        for op in polls
            .iter()
            .filter(|o| o.outcome == Outcome::Ok && o.call > heal.pos)
        {
            if let (Some(d), Some(t)) = (deadline, op.ret_time)
                && t > d
            {
                continue;
            }
            for d in delivered(op) {
                if let Some(o) = d.offset {
                    after.insert(o);
                }
            }
        }
        let missing: Vec<i64> = required
            .keys()
            .copied()
            .filter(|o| !after.contains(o))
            .collect();
        if !missing.is_empty() {
            anomalies.push(Anomaly::new(
                "redelivery-missing",
                format!(
                    "partition {name:?} subscription {sub:?}: unacknowledged offsets {:?} (acked through {acked}) were not delivered after the heal at event {}{}",
                    &missing[..missing.len().min(16)],
                    heal.index,
                    if deadline.is_some() { " within the bound" } else { "" }
                ),
                vec![heal.index],
            ));
        }
    }
}
