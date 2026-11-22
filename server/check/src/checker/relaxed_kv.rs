//! A relaxed checker for KV keys served by an asynchronous replica (KV-G05). It checks only
//! what the replica promises, not linearizability.
//!
//! | f | invoke value | ok completion value |
//! | --- | --- | --- |
//! | `write`, `set`, `put` | the unique value, or `{"value": v, "at_ns": t}` for SET with expiry | optional `{"epoch": e}` |
//! | `read`, `get` | ignored | the value (`null` when absent), or `{"value": v, "epoch": e}` |
//! | `mget` | ordered key array | ordered value array, optionally wrapped with its epoch |
//! | `cas` | `[expected, new]`, a write of `new` when it succeeds | optional epoch |
//! | `delete`, `del` | ignored | optional epoch |
//! | `expire` | `{"at_ns": t}`, the absolute deadline in `time_ns` terms | ignored |
//! | `incr`, `incrby` | the delta (default 1, must be positive) | the new value, optional epoch |
//!
//! `epoch` is the primary epoch that served the op. Nemesis markers named in
//! `Options::failover_markers` (default `failover`, `promote`) mark failovers. A marker
//! can carry a top-level key to scope it to that key. A `cold-reset` marker records explicit
//! data loss and starts an empty store.
//!
//! Register keys (no `incr`):
//! - `invented-value`: a value read that no op wrote, or read before its write was invoked;
//! - `resurrection`: a value read after an acknowledged delete, or after an expiry deadline
//!   (plus `expiry_tolerance_ns`), when its write was acknowledged before the delete or expire;
//! - `stale-read`: within one epoch (both epochs known and the read's not newer), a read that
//!   returns a value older than a write acknowledged before the read began, or that goes back
//!   in time after another read in the same epoch;
//! - `lost-write`: a read older than an acknowledged write with no failover between them (or
//!   outside `lag_window_ns` of it when set). A newer read epoch can establish a failover
//!   without a marker when no time window is configured.
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
    resets: Vec<&'a Marker>,
    events: &'a [Event],
    opts: &'a Options,
}

impl Ctx<'_> {
    /// Whether losing the effect of `w` is allowed for an op invoked at `read`.
    fn loss_allowed(&self, w: &Op, read: &Op) -> bool {
        let applies = |m: &Marker| {
            m.pos > w.ret_pos()
                && m.pos < read.call
                && self.events[m.pos]
                    .key
                    .as_deref()
                    .is_none_or(|key| Some(key) == read.key.as_deref())
        };
        if self.resets.iter().any(|m| applies(m)) {
            return true;
        }
        let epoch_advanced = match (epoch(w), epoch(read)) {
            (Some(we), Some(re)) if re <= we => return false,
            (Some(_), Some(_)) => true,
            _ => false,
        };
        (epoch_advanced && self.opts.lag_window_ns.is_none())
            || self.failovers.iter().any(|m| {
                applies(m)
                    && match (self.opts.lag_window_ns, w.ret_time, m.time_ns) {
                        (Some(lag), Some(wt), Some(mt)) => wt <= mt && mt - wt <= lag,
                        (Some(_), _, _) => false,
                        (None, _, _) => true,
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
        resets: paired
            .markers
            .iter()
            .filter(|m| m.applied() && m.f == "cold-reset")
            .collect(),
        events,
        opts,
    };
    let mut expanded = Vec::new();
    for op in paired
        .ops
        .iter()
        .filter(|op| op.f == "mget" && op.outcome == Outcome::Ok)
    {
        let Some(keys) = op.invoke_value.as_array() else {
            return Verdict::malformed(format!(
                "mget at event {} needs its list of keys",
                op.invoke_index
            ));
        };
        let Some(values) = payload(&op.complete_value)
            .as_array()
            .filter(|vs| vs.len() == keys.len())
        else {
            return Verdict::malformed(format!(
                "mget at event {} needs one result per key",
                op.invoke_index
            ));
        };
        for (key, value) in keys.iter().zip(values) {
            let Some(key) = key.as_str() else {
                return Verdict::malformed(format!(
                    "mget at event {} has a non-string key",
                    op.invoke_index
                ));
            };
            let mut read = op.clone();
            read.f = "get".into();
            read.key = Some(key.into());
            read.complete_value = match epoch(op) {
                Some(e) => serde_json::json!({"value": value, "epoch": e}),
                None => serde_json::json!({"value": value}),
            };
            expanded.push(read);
        }
    }
    let mut keys: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
    for op in paired.ops.iter().chain(&expanded) {
        if kind(&op.f).is_none() {
            continue;
        }
        let Some(k) = op.key.as_deref() else {
            return Verdict::malformed(format!("{} at event {} has no key", op.f, op.invoke_index));
        };
        if op.outcome != Outcome::Fail
            && op.f == "expire"
            && op
                .invoke_value
                .get("at_ns")
                .and_then(Value::as_u64)
                .is_none()
        {
            return Verdict::malformed(format!(
                "expire at event {} needs an absolute at_ns deadline",
                op.invoke_index
            ));
        }
        keys.entry(k).or_default().push(op);
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for (key, ops) in &mut keys {
        ops.sort_by_key(|op| op.call);
        checked += ops.len();
        if ops
            .iter()
            .any(|o| kind(&o.f) == Some("incr") && o.outcome != Outcome::Fail)
        {
            check_counter(key, ops, &ctx, &mut anomalies);
        } else {
            check_register(key, ops, &ctx, &mut anomalies);
        }
    }
    Verdict::new(checked, anomalies)
}

fn check_register(key: &str, ops: &[&Op], ctx: &Ctx, anomalies: &mut Vec<Anomaly>) {
    let mut writer = HashMap::<String, Vec<&Op>>::new();
    for op in ops.iter().copied().filter(|o| kind(&o.f) == Some("write")) {
        writer.entry(canon(written(op))).or_default().push(op);
    }
    let acked_writes: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|o| kind(&o.f) == Some("write") && o.outcome == Outcome::Ok)
        .collect();
    let removals: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|o| {
            matches!(kind(&o.f), Some("delete" | "expire"))
                && o.outcome != Outcome::Fail
                && !matches!(payload(&o.complete_value), Value::Bool(false))
                && payload(&o.complete_value).as_i64() != Some(0)
        })
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
            let sources = writer.get(&canon(v));
            match sources
                .into_iter()
                .flatten()
                .copied()
                .find(|w| w.call < r.ret_pos() && w.outcome != Outcome::Fail)
            {
                Some(w) => Some(w),
                None => {
                    let failed = sources
                        .into_iter()
                        .flatten()
                        .copied()
                        .find(|w| w.call < r.ret_pos());
                    let (kind, detail, indices) = match failed {
                        Some(w) => (
                            "failed-write-observed",
                            format!("written only by the failed op at event {}", w.invoke_index),
                            [w.indices(), r.indices()].concat(),
                        ),
                        None => (
                            "invented-value",
                            "with no possible preceding write".into(),
                            r.indices(),
                        ),
                    };
                    anomalies.push(Anomaly::new(
                        kind,
                        format!("key {key:?}: read {v} (event {}) {detail}", at(r)),
                        indices,
                    ));
                    continue;
                }
            }
        };
        seen.push((r, w));

        // Resurrection after an acknowledged delete or an expiry.
        if let Some(w) = w {
            let version_was_visible = |d: &Op| {
                w.precedes(d)
                    || seen.iter().any(|(read, prior)| {
                        read.precedes(d) && prior.is_some_and(|prior| written(prior) == v)
                    })
            };
            let version_can_be_rewritten = |after: usize| {
                writer[&canon(v)].iter().any(|candidate| {
                    candidate.outcome != Outcome::Fail
                        && candidate.call < r.ret_pos()
                        && (candidate.call > after
                            || (!std::ptr::eq(*candidate, w) && candidate.ret_pos() > after))
                })
            };
            let expired = |d: &Op| {
                let Some(deadline) = d.invoke_value.get("at_ns").and_then(Value::as_u64) else {
                    return false;
                };
                let Some(t) = r.call_time else { return false };
                t >= deadline.saturating_add(ctx.opts.expiry_tolerance_ns)
                    && !removals.iter().any(|later| {
                        later.f == "expire"
                            && d.precedes(later)
                            && later.call < r.ret_pos()
                            && later.call_time.is_none_or(|time| time < deadline)
                    })
            };
            let resurrected = removals.iter().find(|d| {
                d.outcome == Outcome::Ok
                    && d.precedes(r)
                    && version_was_visible(d)
                    && !version_can_be_rewritten(d.call)
                    && (kind(&d.f) == Some("delete") || expired(d))
            });
            let reset = ctx.resets.iter().find(|m| {
                m.pos < r.call
                    && w.call < m.pos
                    && !version_can_be_rewritten(m.pos)
                    && ctx.events[m.pos]
                        .key
                        .as_deref()
                        .is_none_or(|key| Some(key) == r.key.as_deref())
            });
            if let Some(m) = reset {
                anomalies.push(Anomaly::new(
                    "resurrection",
                    format!(
                        "key {key:?}: read {v} after the cold reset at event {}",
                        m.index
                    ),
                    [w.indices(), vec![m.index], r.indices()].concat(),
                ));
            }
            if w.precedes(r) && expired(w) && !version_can_be_rewritten(w.ret_pos()) {
                anomalies.push(Anomaly::new(
                    "resurrection",
                    format!("key {key:?}: read {v} after its SET expiry deadline"),
                    [w.indices(), r.indices()].concat(),
                ));
            }
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
                    // A later operation can explain absence only if it could take effect
                    // between the write and this read.
                    None => {
                        !removals.iter().any(|d| can_remove_between(d, w2, r))
                            && !deadline_passed(w2, r)
                    }
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
        let back = seen[..i]
            .iter()
            .chain(seen[i + 1..].iter())
            .find(|(r1, w_new)| {
                r1.precedes(r2)
                    && !ctx.loss_allowed(r1, r2)
                    && epoch(r1).is_some()
                    && epoch(r1) == epoch(r2)
                    && w_new.is_some_and(|wn| match w_old {
                        Some(w_old) => w_old.precedes(wn),
                        None => {
                            !removals.iter().any(|d| can_remove_between(d, r1, r2))
                                && !deadline_passed(wn, r2)
                        }
                    })
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

fn deadline_passed(expiring: &Op, read: &Op) -> bool {
    expiring
        .invoke_value
        .get("at_ns")
        .and_then(Value::as_u64)
        .zip(read.ret_time)
        .is_some_and(|(deadline, time)| time >= deadline)
}

fn can_remove_between(removal: &Op, prior: &Op, read: &Op) -> bool {
    !removal.precedes(prior)
        && removal.call < read.ret_pos()
        && (kind(&removal.f) == Some("delete") || deadline_passed(removal, read))
}

fn epochs(w: &Op, r: &Op) -> String {
    match (epoch(w), epoch(r)) {
        (Some(a), Some(b)) => format!(", write epoch {a}, read epoch {b}"),
        _ => ", with no failover between them".to_string(),
    }
}

fn check_counter(key: &str, ops: &[&Op], ctx: &Ctx, anomalies: &mut Vec<Anomaly>) {
    let mut incrs = Vec::new();
    for op in ops.iter().copied().filter(|o| o.outcome != Outcome::Fail) {
        match kind(&op.f) {
            Some("incr") => match delta(op) {
                Some(d) if d > 0 => incrs.push((op, d)),
                _ => anomalies.push(Anomaly::new(
                    "malformed-history",
                    format!("counter {key:?}: missing or invalid positive increment"),
                    op.indices(),
                )),
            },
            Some("read") => {}
            _ => anomalies.push(Anomaly::new(
                "malformed-history",
                format!("counter {key:?}: unsupported {}", op.f),
                op.indices(),
            )),
        }
    }
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
        let is_increment = kind(&x.f) == Some("incr");
        let Some(value) = (match payload(&x.complete_value) {
            Value::Null if !is_increment => Some(0),
            _ => as_int(&x.complete_value),
        }) else {
            anomalies.push(Anomaly::new(
                "malformed-history",
                format!("counter {key:?}: {} returned a non-counter value", x.f),
                x.indices(),
            ));
            continue;
        };
        let attempted: i128 = incrs
            .iter()
            .filter(|(o, _)| o.call < x.ret_pos())
            .map(|(_, d)| i128::from(*d))
            .sum();
        let own_delta = if is_increment {
            delta(x).unwrap_or(0)
        } else {
            0
        };
        if value < own_delta || i128::from(value) > attempted {
            anomalies.push(Anomaly::new(
                "invented-value",
                format!(
                    "counter {key:?}: {} returned {value} (event {}), outside {own_delta}..={attempted}",
                    x.f,
                    at(x)
                ),
                x.indices(),
            ));
            continue;
        }
        let floor = acked
            .iter()
            .filter(|(o, v)| {
                o.precedes(x)
                    && i128::from(*v) + i128::from(own_delta) > i128::from(value)
                    && !ctx.loss_allowed(o, x)
            })
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

fn delta(op: &Op) -> Option<i64> {
    if op.f == "incr" && op.invoke_value.is_null() {
        Some(1)
    } else {
        as_int(&op.invoke_value)
    }
}
