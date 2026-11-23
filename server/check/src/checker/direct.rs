//! OLTP direct-path histories: the phase 10 `direct`, `cold` and `move` workloads (OLTP-G02,
//! OLTP-G03 and OLTP-G04 for single-key operations).
//!
//! [`check`] runs three checks over one history and merges their verdicts:
//!
//! 1. [`linearizable::check_kv_opts`] with [`Options::request_dedup`]: linearizability
//!    across the rows in each single-range batch, including request-ID replay and reuse.
//! 2. [`outcome::check`] (`failed-write-observed`) over possible writes and row observations.
//! 3. [`check_identity`]: specific diagnostics for replay and reuse violations.
//!
//! The request identity on the direct path is the table, the anchor key (the smallest key of
//! the request) and the request ID. Its dedup record stores the body's digest and the result.
//! A retry with the same body returns the stored result. Another body under the same identity
//! is refused with `conflict`. The same request ID on another key is another identity and runs
//! (D03). The body here is `f` plus the invoke value. [`check_identity`] reports:
//!
//! | Kind | Rule | Scenarios |
//! | --- | --- | --- |
//! | `dedup-miss` | an attempt invoked after an attempt of the same identity and body answered with a commit was answered from fresh state: an `update` or `delete` of a missing row, or a `conflict` refusal | D02, D04, O15 |
//! | `request-id-reuse-undetected` | two bodies under one identity both committed, or one was answered `ok` although it was invoked after the other committed | D03 |
//! | `request-id-reuse-misdetected` | an `upsert`, `update` or `delete` was refused with `conflict`, which on the direct path only reuse causes, but no other body under its identity could have held a dedup record by then | D03 |
//!
//! An answer "with a commit" is an `ok` `insert`, `insert_many` or `upsert`, or an `ok`
//! `update` or `delete` whose existence flag is `true` (see [`linearizable`]). Refusals other
//! than `conflict`, such as `overloaded` or a deadline before the dedup lookup, can come after a
//! commit and are not dedup misses. Dedup records expire after a retention of 24 hours, far
//! longer than a simulated run, so the rules assume they never expire.
//!
//! The completion values come from the simulator's history recorder: a `fail` carries the
//! error code (`code`, `error`, or `error.code`), and an `ok` `update` or `delete` carries the
//! existence flag (`true`, `false`, or `{"data": b}`). An `insert_many` success count,
//! when recorded, must equal the number of input rows. These histories use primary-key
//! conflicts. A workload with other uniqueness constraints needs to identify those refusals.

use super::linearizable::{
    self, Identities, Options, anchor, body, classify, committed_answer, error_code, rows_of,
};
use super::{Anomaly, Verdict, outcome};
use crate::history::{Event, Op, Outcome, pair};

/// Every direct-path check over one history.
pub fn check(events: &[Event]) -> Verdict {
    let lin = linearizable::check_kv_opts(
        events,
        &Options {
            request_dedup: true,
            ..Options::default()
        },
    );
    let paired = match pair(events) {
        Ok(paired) => paired,
        Err(e) => return Verdict::malformed(e),
    };
    Verdict::merge([
        lin,
        outcome::check(&for_outcome(events, &paired.ops)),
        identity_verdict(&paired.ops),
    ])
}

/// The request ID rules in the table above.
pub fn check_identity(events: &[Event]) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    identity_verdict(&paired.ops)
}

fn identity_verdict(ops: &[Op]) -> Verdict {
    let ids = Identities::new(ops);
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for ((anchor, rid), members) in &ids.by_id {
        checked += members.len();
        let mut bodies: Vec<(String, Vec<usize>)> = Vec::new();
        for &i in members {
            let b = body(&ops[i]);
            match bodies.iter_mut().find(|(x, _)| *x == b) {
                Some((_, attempts)) => attempts.push(i),
                None => bodies.push((b, vec![i])),
            }
        }
        // The first answer with a commit, per body.
        let committed: Vec<Option<usize>> = bodies
            .iter()
            .map(|(_, attempts)| {
                attempts
                    .iter()
                    .copied()
                    .filter(|&i| committed_answer(&ops[i]) == Some(true))
                    .min_by_key(|&i| ops[i].ret_pos())
            })
            .collect();
        let name = |i: usize| describe(&ops[i]);
        for (b, (_, attempts)) in bodies.iter().enumerate() {
            let Some(c) = committed[b] else { continue };
            for &a in attempts {
                let op = &ops[a];
                let fresh = committed_answer(op) == Some(false)
                    || (op.outcome == Outcome::Fail && error_code(op) == Some("conflict"));
                if op.call > ops[c].ret_pos() && fresh {
                    anomalies.push(Anomaly::new(
                        "dedup-miss",
                        format!(
                            "request {rid:?} at anchor {anchor:?}: {} committed (event {}), yet the retry {} invoked after it was answered from fresh state",
                            name(c),
                            ops[c].complete_index.unwrap_or(ops[c].invoke_index),
                            name(a),
                        ),
                        [ops[c].indices(), op.indices()].concat(),
                    ));
                }
            }
        }
        for (a, _) in bodies.iter().enumerate() {
            let Some(c) = committed[a] else { continue };
            for (b, (_, other)) in bodies.iter().enumerate() {
                if b == a {
                    continue;
                }
                let culprit = match committed[b] {
                    // Both committed: report the pair once.
                    Some(d) if a < b => Some(d),
                    Some(_) => None,
                    None => other
                        .iter()
                        .copied()
                        .find(|&i| ops[i].outcome == Outcome::Ok && ops[i].call > ops[c].ret_pos()),
                };
                if let Some(d) = culprit {
                    anomalies.push(Anomaly::new(
                        "request-id-reuse-undetected",
                        format!(
                            "request {rid:?} at anchor {anchor:?}: {} committed, and {} with another body was answered ok instead of conflict",
                            name(c),
                            name(d),
                        ),
                        [ops[c].indices(), ops[d].indices()].concat(),
                    ));
                }
            }
        }
        for &i in members {
            let op = &ops[i];
            if op.outcome == Outcome::Fail
                && error_code(op) == Some("conflict")
                && matches!(classify(&op.f), Some("write" | "update" | "delete"))
                && !ids.reuse_explains(i)
            {
                anomalies.push(Anomaly::new(
                    "request-id-reuse-misdetected",
                    format!(
                        "request {rid:?} at anchor {anchor:?}: {} was refused with conflict, but no other body under this request ID and key could have held a dedup record",
                        name(i),
                    ),
                    op.indices(),
                ));
            }
        }
    }
    Verdict::new(checked, anomalies)
}

fn describe(op: &Op) -> String {
    let status = match op.outcome {
        Outcome::Ok => "ok",
        Outcome::Fail => "fail",
        Outcome::Info => "info",
    };
    format!(
        "p{} {} {} ({status}, event {})",
        op.process, op.f, op.invoke_value, op.invoke_index
    )
}

/// Give each projected write its own process, retaining the original event order and indices.
/// A successful update of a missing row is neither a write nor a possible source for a value.
///
/// The generic outcome checker matches dedup receipts by key and request ID. Include the original
/// anchor and body so that projecting a batch row cannot alias a different request's receipt.
fn for_outcome(events: &[Event], ops: &[Op]) -> Vec<Event> {
    let mut at_event = vec![Vec::new(); events.len()];
    let mut process = 0;
    for op in ops {
        if committed_answer(op) == Some(false) {
            continue;
        }
        let request_id = op
            .request_id
            .as_ref()
            .map(|rid| serde_json::json!([anchor(op), rid, body(op)]).to_string());
        let parts = if op.f == "insert_many" {
            let Some(rows) = rows_of(&op.invoke_value) else {
                continue;
            };
            rows.into_iter()
                .map(|(key, row)| (Some(key), "insert", row))
                .collect()
        } else {
            let f = if op.f == "update" {
                "put"
            } else {
                op.f.as_str()
            };
            vec![(op.key.clone(), f, &op.invoke_value)]
        };
        for (key, f, value) in parts {
            for pos in std::iter::once(op.call).chain(op.done) {
                let mut event = events[pos].clone();
                event.process = process;
                event.key = key.clone();
                event.request_id = request_id.clone();
                event.f = f.to_string();
                if pos == op.call {
                    event.value = value.clone();
                }
                at_event[pos].push(event);
            }
            process += 1;
        }
    }
    at_event.into_iter().flatten().collect()
}
