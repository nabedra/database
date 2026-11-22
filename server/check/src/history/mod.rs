//! Client histories in the pinned JSON-lines format, one event per line.
//!
//! ```text
//! {"index":0,"type":"invoke","process":3,"f":"write","key":"k1","value":5,
//!  "request_id":"r-1","node":"n1","time_ns":123}
//! ```
//!
//! `type` is `invoke`, `ok`, `fail` or `info`. `info` is an unknown outcome, as in Jepsen; the
//! spelling `unknown` is accepted on input and written back as `info`. Nemesis events use the
//! process `"nemesis"` (stored as [`NEMESIS`]) and serve as markers such as `heal` or `failover`.
//!
//! A completion whose value is an object with `"outcome": "not_applied"` counts as a definite
//! failure, and one with `"outcome": "outcome_unknown"` counts as unknown, whatever its `type`.
//! [`pair`] applies these rules, so checkers work on [`Op`]s instead of raw events.

use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, Write};

/// The process number that stands for the nemesis (`"process":"nemesis"` in JSON).
pub const NEMESIS: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    Invoke,
    Ok,
    Fail,
    #[serde(alias = "unknown")]
    Info,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Position in the history. `read_jsonl` fills it with the line position when it is absent.
    #[serde(default = "missing_index")]
    pub index: usize,
    #[serde(rename = "type")]
    pub kind: EventType,
    #[serde(serialize_with = "ser_process", deserialize_with = "de_process")]
    pub process: u64,
    pub f: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_key"
    )]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_ns: Option<u64>,
}

fn missing_index() -> usize {
    usize::MAX
}

fn ser_process<S: Serializer>(p: &u64, s: S) -> Result<S::Ok, S::Error> {
    if *p == NEMESIS {
        s.serialize_str("nemesis")
    } else {
        s.serialize_u64(*p)
    }
}

fn de_process<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    match Value::deserialize(d)? {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| de::Error::custom("process must be a non-negative integer")),
        Value::String(s) if s == "nemesis" => Ok(NEMESIS),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| de::Error::custom(format!("unknown process {s:?}"))),
        other => Err(de::Error::custom(format!("bad process {other}"))),
    }
}

fn de_key<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    match Value::deserialize(d)? {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s)),
        Value::Number(n) => Ok(Some(n.to_string())),
        other => Err(de::Error::custom(format!("bad key {other}"))),
    }
}

impl Event {
    pub fn new(kind: EventType, process: u64, f: &str) -> Self {
        Event {
            index: missing_index(),
            kind,
            process,
            f: f.to_string(),
            key: None,
            value: Value::Null,
            request_id: None,
            node: None,
            time_ns: None,
        }
    }

    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    pub fn with_value(mut self, value: Value) -> Self {
        self.value = value;
        self
    }

    pub fn is_nemesis(&self) -> bool {
        self.process == NEMESIS
    }
}

/// Reads one event per non-empty line. Missing `index` fields get the event's position.
pub fn read_jsonl(r: impl BufRead) -> std::io::Result<Vec<Event>> {
    let mut out = Vec::new();
    for (lineno, line) in r.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut ev: Event = serde_json::from_str(trimmed).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("history line {}: {e}", lineno + 1),
            )
        })?;
        if ev.index == missing_index() {
            ev.index = out.len();
        }
        out.push(ev);
    }
    Ok(out)
}

pub fn write_jsonl(mut w: impl Write, events: &[Event]) -> std::io::Result<()> {
    for ev in events {
        serde_json::to_writer(&mut w, ev)?;
        w.write_all(b"\n")?;
    }
    w.flush()
}

/// Assigns `index = position` to every event, for histories built in code.
pub fn reindex(events: &mut [Event]) {
    for (i, ev) in events.iter_mut().enumerate() {
        ev.index = i;
    }
}

/// The effective outcome of an operation after applying the `outcome` field rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Ok,
    Fail,
    /// Unknown: an `info` completion, an `outcome_unknown` reply, or no completion at all.
    Info,
}

/// An invocation paired with its completion.
#[derive(Clone, Debug, PartialEq)]
pub struct Op {
    pub process: u64,
    pub f: String,
    pub key: Option<String>,
    pub request_id: Option<String>,
    pub outcome: Outcome,
    /// The invoke event's value (for writes, what was written).
    pub invoke_value: Value,
    /// The completion event's value, or `Null` without a completion.
    pub complete_value: Value,
    /// Positions in the event slice. `ret` is `None` without a completion.
    pub call: usize,
    pub ret: Option<usize>,
    /// Position of the completion event even when the outcome is unknown; `None` without one.
    pub done: Option<usize>,
    /// The `index` fields of the invoke and completion events, for reports.
    pub invoke_index: usize,
    pub complete_index: Option<usize>,
    pub call_time: Option<u64>,
    pub ret_time: Option<u64>,
}

impl Op {
    /// The value that carries the operation's result: the completion value for an `ok` op,
    /// otherwise the invoke value.
    pub fn value(&self) -> &Value {
        if self.outcome == Outcome::Ok && !self.complete_value.is_null() {
            &self.complete_value
        } else {
            &self.invoke_value
        }
    }

    /// Event indices for a report: invoke, and completion when there is one.
    pub fn indices(&self) -> Vec<usize> {
        let mut v = vec![self.invoke_index];
        v.extend(self.complete_index);
        v
    }

    /// Completion position, or `usize::MAX` for an op without one.
    pub fn ret_pos(&self) -> usize {
        self.ret.unwrap_or(usize::MAX)
    }

    /// True when this op finished before `other` was invoked.
    pub fn precedes(&self, other: &Op) -> bool {
        matches!(self.ret, Some(r) if r < other.call)
    }
}

/// A nemesis marker such as `heal` or `failover`, at the position of its last event.
#[derive(Clone, Debug, PartialEq)]
pub struct Marker {
    pub f: String,
    pub pos: usize,
    pub index: usize,
    pub time_ns: Option<u64>,
    pub value: Value,
}

/// A history split into client operations and nemesis markers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Paired {
    pub ops: Vec<Op>,
    pub markers: Vec<Marker>,
}

/// If `v` is an object with a `value` field, that field; otherwise `v` itself.
pub fn payload(v: &Value) -> &Value {
    match v {
        Value::Object(m) if m.contains_key("value") => &m["value"],
        _ => v,
    }
}

/// The string field `name` of an object value.
pub fn str_field<'a>(v: &'a Value, name: &str) -> Option<&'a str> {
    v.as_object()?.get(name)?.as_str()
}

/// Pairs each invoke with the next completion from the same process. An invoke without a
/// completion becomes an `Info` op. Nemesis events become markers. Errors name the first
/// malformed event: a completion without an invoke, or a second invoke on a busy process.
pub fn pair(events: &[Event]) -> Result<Paired, String> {
    use std::collections::HashMap;
    let mut out = Paired::default();
    let mut open: HashMap<u64, usize> = HashMap::new(); // process -> op slot
    let mut nemesis_open: HashMap<String, usize> = HashMap::new(); // f -> marker slot
    for (pos, ev) in events.iter().enumerate() {
        if ev.is_nemesis() {
            // A nemesis invoke and its completion collapse into one marker at the later event.
            if ev.kind == EventType::Invoke {
                nemesis_open.insert(ev.f.clone(), out.markers.len());
                out.markers.push(marker(ev, pos));
            } else if let Some(slot) = nemesis_open.remove(&ev.f) {
                out.markers[slot] = marker(ev, pos);
            } else {
                out.markers.push(marker(ev, pos));
            }
            continue;
        }
        match ev.kind {
            EventType::Invoke => {
                if let Some(&slot) = open.get(&ev.process) {
                    return Err(format!(
                        "event {}: process {} invoked {} while its op at event {} is still open",
                        ev.index, ev.process, ev.f, out.ops[slot].invoke_index
                    ));
                }
                open.insert(ev.process, out.ops.len());
                out.ops.push(Op {
                    process: ev.process,
                    f: ev.f.clone(),
                    key: ev.key.clone(),
                    request_id: ev.request_id.clone(),
                    outcome: Outcome::Info,
                    invoke_value: ev.value.clone(),
                    complete_value: Value::Null,
                    call: pos,
                    ret: None,
                    done: None,
                    invoke_index: ev.index,
                    complete_index: None,
                    call_time: ev.time_ns,
                    ret_time: None,
                });
            }
            kind => {
                let Some(slot) = open.remove(&ev.process) else {
                    return Err(format!(
                        "event {}: completion for process {} without an open invoke",
                        ev.index, ev.process
                    ));
                };
                let op = &mut out.ops[slot];
                if op.f != ev.f {
                    return Err(format!(
                        "event {}: completion f {:?} does not match invoke f {:?} at event {}",
                        ev.index, ev.f, op.f, op.invoke_index
                    ));
                }
                op.outcome = match (kind, str_field(&ev.value, "outcome")) {
                    (_, Some("not_applied")) => Outcome::Fail,
                    (_, Some("outcome_unknown")) | (_, Some("unknown")) => Outcome::Info,
                    (EventType::Ok, _) => Outcome::Ok,
                    (EventType::Fail, _) => Outcome::Fail,
                    _ => Outcome::Info,
                };
                op.complete_value = ev.value.clone();
                op.complete_index = Some(ev.index);
                op.ret_time = ev.time_ns;
                op.done = Some(pos);
                // An unknown outcome has no known end: it may take effect at any later point.
                if op.outcome != Outcome::Info {
                    op.ret = Some(pos);
                }
            }
        }
    }
    Ok(out)
}

fn marker(ev: &Event, pos: usize) -> Marker {
    Marker {
        f: ev.f.clone(),
        pos,
        index: ev.index,
        time_ns: ev.time_ns,
        value: ev.value.clone(),
    }
}
