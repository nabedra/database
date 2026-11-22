//! A relaxed checker for KV keys served by an asynchronous replica (KV-G05). It checks only
//! what the replica promises, not linearizability.
//!
//! | f | invoke value | ok completion value |
//! | --- | --- | --- |
//! | `write`, `set`, `put` | the unique value | optional `{"epoch": e}` |
//! | `read`, `get` | ignored | the value (`null` when absent), or `{"value": v, "epoch": e}` |
//! | `cas` | `[expected, new]`, a write of `new` when it succeeds | optional epoch |
//! | `delete`, `del` | ignored | optional epoch |
//! | `expire` | `{"at_ns": t}`, the absolute deadline in `time_ns` terms | ignored |
//! | `incr`, `incrby` | the delta (default 1, must be positive) | the new value, optional epoch |
//!
//! `epoch` is the primary epoch that served the op. Nemesis markers named in
//! `Options::failover_markers` (default `failover`, `promote`) mark failovers.
//!
//! Register keys (no `incr`):
//! - `invented-value`: a value read that no op wrote, or read before its write was invoked;
//! - `resurrection`: a value read after an acknowledged delete, or after an expiry deadline
//!   (plus `expiry_tolerance_ns`), when its write was acknowledged before the delete or expire;
//! - `stale-read`: within one epoch (both epochs known and the read's not newer), a read that
//!   returns a value older than a write acknowledged before the read began, or that goes back
//!   in time after another read in the same epoch;
//! - `lost-write`: a read older than an acknowledged write with no failover between them (or
//!   outside `lag_window_ns` of it when set). A read in a newer epoch than the write counts as
//!   across a failover.
//!
//! Counter keys (any `incr`), the `consistency-test.rb` workload: a value above the sum of
//! attempted increments is `invented-value`; below the last acknowledged increment's value it
//! is a loss, allowed only across a failover (`lost-write` or `stale-read` otherwise); between
//! the two it is an unknown outcome and passes. Deletes on counter keys are not modelled.

use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Marker, Op, Outcome, pair, payload};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug)]
pub struct Options {
    pub failover_markers: Vec<String>,
    /// When set, a loss is allowed only for writes acknowledged within this long before the
    /// failover marker (needs `time_ns`).
    pub lag_window_ns: Option<u64>,
    pub expiry_tolerance_ns: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            failover_markers: vec!["failover".into(), "promote".into()],
            lag_window_ns: None,
            expiry_tolerance_ns: 10_000_000,
        }
    }
}

pub fn check(events: &[Event]) -> Verdict {
    check_with(events, &Options::default())
}

fn kind(f: &str) -> Option<&'static str> {
    Some(match f {
        "write" | "set" | "put" | "cas" => "write",
        "read" | "get" => "read",
        "delete" | "del" => "delete",
        "expire" => "expire",
        "incr" | "incrby" => "incr",
        _ => return None,
    })
}

/// The value a write op writes (the new value for `cas`).
fn written(op: &Op) -> &Value {
    let v = payload(&op.invoke_value);
    match (op.f.as_str(), v.as_array()) {
        ("cas", Some(a)) if a.len() == 2 => &a[1],
        _ => v,
    }
}

fn epoch(op: &Op) -> Option<u64> {
    op.complete_value.get("epoch").and_then(Value::as_u64)
}

fn as_int(v: &Value) -> Option<i64> {
    match payload(v) {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

struct Ctx<'a> {
    failovers: Vec<&'a Marker>,
    opts: &'a Options,
}

impl Ctx<'_> {
    /// Whether losing the effect of `w` is allowed for an op invoked at `read`.
    fn loss_allowed(&self, w: &Op, read: &Op) -> bool {
        if let (Some(we), Some(re)) = (epoch(w), epoch(read))
            && re > we
        {
            return true;
        }
        let wret = w.ret_pos();
        self.failovers.iter().any(|m| {
            m.pos > wret
                && m.pos < read.call
                && match (self.opts.lag_window_ns, w.ret_time, m.time_ns) {
                    (Some(lag), Some(wt), Some(mt)) => wt.saturating_add(lag) >= mt,
                    _ => true,
                }
        })
    }

    fn loss_kind(&self, w: &Op, read: &Op) -> &'static str {
        match (epoch(w), epoch(read)) {
            (Some(we), Some(re)) if re <= we => "stale-read",
            _ => "lost-write",
        }
    }
}

pub fn check_with(events: &[Event], opts: &Options) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let ctx = Ctx {
        failovers: paired
            .markers
            .iter()
            .filter(|m| m.applied() && opts.failover_markers.contains(&m.f))
            .collect(),
        opts,
    };
    let mut keys: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
    for op in &paired.ops {
        if let (Some(k), Some(_)) = (op.key.as_deref(), kind(&op.f)) {
            keys.entry(k).or_default().push(op);
        }
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for (key, ops) in &keys {
        checked += ops.len();
        if ops.iter().any(|o| kind(&o.f) == Some("incr")) {
            check_counter(key, ops, &ctx, &mut anomalies);
        } else {
            check_register(key, ops, &ctx, &mut anomalies);
        }
    }
    Verdict::new(checked, anomalies)
}

fn check_register(key: &str, ops: &[&Op], ctx: &Ctx, anomalies: &mut Vec<Anomaly>) {
    let mut writer: HashMap<String, &Op> = HashMap::new();
    for op in ops.iter().copied().filter(|o| kind(&o.f) == Some("write")) {
        writer.entry(canon(written(op))).or_insert(op);
    }
    let acked_writes: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|o| kind(&o.f) == Some("write") && o.outcome == Outcome::Ok)
        .collect();
    let removals: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|o| matches!(kind(&o.f), Some("delete" | "expire")) && o.outcome != Outcome::Fail)
        .collect();
    let reads: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|o| kind(&o.f) == Some("read") && o.outcome == Outcome::Ok)
        .collect();
    let at = |o: &Op| o.complete_index.unwrap_or(o.invoke_index);
    // (read, writer of the value it saw), for the monotonic-read check.
    let mut seen: Vec<(&Op, Option<&Op>)> = Vec::new();
    for r in &reads {
        let v = payload(&r.complete_value);
        let w = if v.is_null() {
            None
        } else {
            match writer.get(&canon(v)) {
                None => {
                    anomalies.push(Anomaly::new(
                        "invented-value",
                        format!("key {key:?}: read {v} (event {}), which no op wrote", at(r)),
                        r.indices(),
                    ));
                    continue;
                }
                Some(w) if w.call > r.ret_pos() => {
                    anomalies.push(Anomaly::new(
                        "invented-value",
                        format!("key {key:?}: read {v} (event {}) before its write was invoked (event {})", at(r), w.invoke_index),
                        [r.indices(), w.indices()].concat(),
                    ));
                    continue;
                }
                Some(w) => Some(*w),
            }
        };
        seen.push((r, w));

        // Resurrection after an acknowledged delete or an expiry.
        if let Some(w) = w {
            let resurrected = removals.iter().find(|d| {
                d.outcome == Outcome::Ok
                    && w.precedes(d)
                    && match kind(&d.f) {
                        Some("delete") => d.precedes(r),
                        _ => match (
                            d.invoke_value.get("at_ns").and_then(Value::as_u64),
                            r.call_time,
                        ) {
                            (Some(deadline), Some(t)) => {
                                t > deadline.saturating_add(ctx.opts.expiry_tolerance_ns)
                                    && d.precedes(r)
                            }
                            _ => false,
                        },
                    }
            });
            if let Some(d) = resurrected {
                anomalies.push(Anomaly::new(
                    "resurrection",
                    format!(
                        "key {key:?}: read {v} (event {}) after the acknowledged {} at event {}",
                        at(r),
                        d.f,
                        at(d)
                    ),
                    [w.indices(), d.indices(), r.indices()].concat(),
                ));
                continue;
            }
        }

        // A newer acknowledged write finished before this read began.
        let newer = acked_writes.iter().filter(|w2| {
            w2.precedes(r)
                && match w {
                    Some(w) => w.precedes(w2),
                    // An absent value is older only if no removal could explain it.
                    None => !removals.iter().any(|d| !d.precedes(w2)),
                }
        });
        if let Some(w2) = newer
            .filter(|w2| !ctx.loss_allowed(w2, r))
            .max_by_key(|w2| w2.ret_pos())
        {
            anomalies.push(Anomaly::new(
                ctx.loss_kind(w2, r),
                format!(
                    "key {key:?}: read {v} (event {}) after the write of {} was acknowledged (event {}){}",
                    at(r),
                    written(w2),
                    at(w2),
                    epochs(w2, r)
                ),
                [w2.indices(), r.indices()].concat(),
            ));
        }
    }

    // Monotonic reads within one epoch.
    for (i, (r2, w_old)) in seen.iter().enumerate() {
        let Some(w_old) = w_old else { continue };
        let back = seen[..i]
            .iter()
            .chain(seen[i + 1..].iter())
            .find(|(r1, w_new)| {
                r1.precedes(r2)
                    && epoch(r1).is_some()
                    && epoch(r1) == epoch(r2)
                    && w_new.is_some_and(|wn| w_old.precedes(wn))
            });
        if let Some((r1, _)) = back {
            anomalies.push(Anomaly::new(
                "stale-read",
                format!(
                    "key {key:?}: in epoch {}, read {} (event {}) went back from {} (event {})",
                    epoch(r2).unwrap_or(0),
                    payload(&r2.complete_value),
                    at(r2),
                    payload(&r1.complete_value),
                    at(r1)
                ),
                [r1.indices(), r2.indices()].concat(),
            ));
        }
    }
}

fn epochs(w: &Op, r: &Op) -> String {
    match (epoch(w), epoch(r)) {
        (Some(a), Some(b)) => format!(", write epoch {a}, read epoch {b}"),
        _ => ", with no failover between them".to_string(),
    }
}

fn check_counter(key: &str, ops: &[&Op], ctx: &Ctx, anomalies: &mut Vec<Anomaly>) {
    let incrs: Vec<(&Op, i64)> = ops
        .iter()
        .copied()
        .filter(|o| kind(&o.f) == Some("incr") && o.outcome != Outcome::Fail)
        .map(|o| (o, as_int(&o.invoke_value).unwrap_or(1)))
        .collect();
    let acked: Vec<(&Op, i64)> = incrs
        .iter()
        .filter(|(o, _)| o.outcome == Outcome::Ok)
        .filter_map(|(o, _)| as_int(&o.complete_value).map(|v| (*o, v)))
        .collect();
    let at = |o: &Op| o.complete_index.unwrap_or(o.invoke_index);
    for x in ops
        .iter()
        .copied()
        .filter(|o| o.outcome == Outcome::Ok && matches!(kind(&o.f), Some("read" | "incr")))
    {
        let Some(value) = (match payload(&x.complete_value) {
            Value::Null => Some(0),
            _ => as_int(&x.complete_value),
        }) else {
            continue;
        };
        let attempted: i64 = incrs
            .iter()
            .filter(|(o, _)| o.call < x.ret_pos())
            .map(|(_, d)| d)
            .sum();
        if value > attempted {
            anomalies.push(Anomaly::new(
                "invented-value",
                format!("counter {key:?}: {} returned {value} (event {}), above the {attempted} attempted", x.f, at(x)),
                x.indices(),
            ));
            continue;
        }
        let floor = acked
            .iter()
            .filter(|(o, v)| o.precedes(x) && *v > value && !ctx.loss_allowed(o, x))
            .max_by_key(|(_, v)| *v);
        if let Some((w, v)) = floor {
            anomalies.push(Anomaly::new(
                ctx.loss_kind(w, x),
                format!(
                    "counter {key:?}: {} returned {value} (event {}) after an increment was acknowledged at {v} (event {}){}",
                    x.f,
                    at(x),
                    at(w),
                    epochs(w, x)
                ),
                [w.indices(), x.indices()].concat(),
            ));
        }
    }
}
