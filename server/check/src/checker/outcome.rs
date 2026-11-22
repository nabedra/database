//! Outcome truth (OLTP-G04): an operation answered `fail` or `not_applied` must not take
//! effect. If its unique written value is later observed, or a retry with the same request ID
//! reports a dedup hit on it, the checker reports `failed-write-observed`.
//!
//! Writes considered, from the invoke value: `write`, `set`, `put`, `insert`, `upsert` and
//! `append` (the value, unwrapped from `{"value": v}`), `cas` (the new value), `txn` (`w` and
//! `append` micro-ops) and `publish` (each event). Observations, from `ok` completions: `read`
//! and `get` (the value, or each element of a list), `txn` reads, and `poll` deliveries.
//!
//! A value that another possible source wrote or could have written by the observation
//! is ambiguous. A later retry cannot explain an earlier observation.
//!
//! A dedup hit is an `ok` completion whose value has `"dedup": true`, for an op whose
//! `request_id` and key match the failed op's, with no other possible source for the record.

use super::{Anomaly, Verdict, canon, queue};
use crate::history::{Event, Op, Outcome, pair, payload};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn written(op: &Op) -> Vec<(String, Value)> {
    let key = op.key.clone().unwrap_or_default();
    match op.f.as_str() {
        "write" | "set" | "put" | "insert" | "upsert" | "append" => {
            vec![(key, payload(&op.invoke_value).clone())]
        }
        "cas" => payload(&op.invoke_value)
            .as_array()
            .filter(|a| a.len() == 2)
            .map(|a| vec![(key, a[1].clone())])
            .unwrap_or_default(),
        "txn" => op
            .invoke_value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                let m = m.as_array().filter(|m| m.len() == 3)?;
                let f = m[0].as_str()?;
                (f == "w" || f == "append").then(|| (key_of(&m[1]), m[2].clone()))
            })
            .collect(),
        "publish" => queue::published(op)
            .into_iter()
            .map(|v| (key.clone(), v))
            .collect(),
        _ => Vec::new(),
    }
}

fn key_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn observed(op: &Op, lists: &HashSet<String>) -> Vec<(String, Value)> {
    let key = op.key.clone().unwrap_or_default();
    let spread = |k: String, v: &Value| -> Vec<(String, Value)> {
        match v {
            Value::Null => vec![],
            Value::Array(items) if lists.contains(&k) => {
                let mut out: Vec<(String, Value)> =
                    items.iter().map(|i| (k.clone(), i.clone())).collect();
                out.push((k, v.clone()));
                out
            }
            other => vec![(k, other.clone())],
        }
    };
    match op.f.as_str() {
        "read" | "get" => spread(key, payload(&op.complete_value)),
        "txn" => op
            .complete_value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                let m = m.as_array().filter(|m| m.len() == 3)?;
                (m[0].as_str()? == "r").then(|| spread(key_of(&m[1]), &m[2]))
            })
            .flatten()
            .collect(),
        "poll" => queue::delivered(op)
            .into_iter()
            .map(|d| (key.clone(), d.value))
            .collect(),
        _ => Vec::new(),
    }
}

pub fn check(events: &[Event]) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let ops = &paired.ops;
    let list_keys: HashSet<_> = ops
        .iter()
        .flat_map(|op| {
            if op.f == "append" {
                return vec![op.key.clone().unwrap_or_default()];
            }
            if op.f != "txn" {
                return Vec::new();
            }
            op.invoke_value
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| {
                    let mop = m.as_array().filter(|m| m.len() == 3 && m[0] == "append")?;
                    Some(key_of(&mop[1]))
                })
                .collect()
        })
        .collect();
    // (key, canonical value) -> observing ops, in history order.
    let mut seen: HashMap<(String, String), Vec<usize>> = HashMap::new();
    let mut dedup_hits = HashMap::<_, Vec<usize>>::new();
    for (i, op) in ops.iter().enumerate() {
        if op.outcome != Outcome::Ok {
            continue;
        }
        for (k, v) in observed(op, &list_keys) {
            seen.entry((k, canon(&v))).or_default().push(i);
        }
        if let (Some(rid), Some(true)) = (
            op.request_id.as_deref(),
            op.complete_value.get("dedup").and_then(Value::as_bool),
        ) {
            dedup_hits.entry((&op.key, rid)).or_default().push(i);
        }
    }
    let mut live_writes = HashMap::<_, Vec<&Op>>::new();
    for op in ops.iter().filter(|o| o.outcome != Outcome::Fail) {
        for (k, v) in written(op) {
            live_writes.entry((k, canon(&v))).or_default().push(op);
        }
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for op in ops.iter().filter(|o| o.outcome == Outcome::Fail) {
        let values = written(op);
        if values.is_empty() && !matches!(op.f.as_str(), "delete" | "del") {
            continue;
        }
        checked += 1;
        let mut reported = false;
        for (k, v) in values {
            if v.is_null() || reported {
                continue;
            }
            let value = (k.clone(), canon(&v));
            let hit = seen
                .get(&value)
                .into_iter()
                .flatten()
                .map(|&i| &ops[i])
                .find(|o| {
                    o.ret_pos() > op.call
                        && !live_writes
                            .get(&value)
                            .into_iter()
                            .flatten()
                            .any(|w| w.call < o.ret_pos())
                });
            if let Some(reader) = hit {
                anomalies.push(Anomaly::new(
                    "failed-write-observed",
                    format!(
                        "p{} {} of {v} to key {k:?} completed {} (event {}), yet p{} {} observed it (event {})",
                        op.process,
                        op.f,
                        failure_text(op),
                        op.complete_index.unwrap_or(op.invoke_index),
                        reader.process,
                        reader.f,
                        reader.complete_index.unwrap_or(reader.invoke_index),
                    ),
                    [op.indices(), reader.indices()].concat(),
                ));
                reported = true;
            }
        }
        if reported {
            continue;
        }
        if let Some(rid) = op.request_id.as_deref()
            && let Some(retry) = dedup_hits
                .get(&(&op.key, rid))
                .into_iter()
                .flatten()
                .map(|&i| &ops[i])
                .find(|retry| {
                    retry.call > op.call
                        && retry.f == op.f
                        && !ops.iter().any(|w| {
                            w.request_id.as_deref() == Some(rid)
                                && w.key == op.key
                                && w.f == op.f
                                && w.outcome != Outcome::Fail
                                && w.call < retry.ret_pos()
                                && w.complete_value.get("dedup").and_then(Value::as_bool)
                                    != Some(true)
                        })
                })
        {
            anomalies.push(Anomaly::new(
                "failed-write-observed",
                format!(
                    "request {rid:?} completed {} (event {}), yet a retry reported a dedup hit (event {})",
                    failure_text(op),
                    op.complete_index.unwrap_or(op.invoke_index),
                    retry.complete_index.unwrap_or(retry.invoke_index),
                ),
                [op.indices(), retry.indices()].concat(),
            ));
        }
    }
    Verdict::new(checked, anomalies)
}

fn failure_text(op: &Op) -> &'static str {
    if crate::history::str_field(&op.complete_value, "outcome") == Some("not_applied") {
        "not_applied"
    } else {
        "fail"
    }
}
