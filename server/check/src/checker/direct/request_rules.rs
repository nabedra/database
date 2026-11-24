//! Opt-in rules about what one answer says for the whole request identity.
//!
//! Both are off by default ([`Options`]), and [`super::check`] does not depend on them. The first
//! is close to the retry rule that `plans/oltp-direct-path.md`, section 4, states since
//! 2026-11-24 ("Retry results"), but it is not that rule: see "Not flagged" below.
//! [`super::check`] still follows the shipped executor, which recomputes no-ops and row conflicts
//! on retry ("a known gap" in the same text). The second tests a reading of `not_applied` that
//! nobody has decided: section 6, step 1, of the plan says "When full, return `429 overloaded`
//! with `not_applied`", and does not say whether that speaks for the request or for one attempt.
//! A hit of either rule is a finding to review. It is not a failed guarantee, and a history
//! without hits is not evidence for one.
//!
//! The rules only add anomalies. They do not make the default accept the decided retry rule: a
//! retry that replays `false` after the row was inserted is `non-linearizable` there.
//!
//! The identity and the body are those of [`super`]: the table-qualified anchor key and the
//! request ID, and `f` plus the invoke value. Reads have no identity (plan addendum, RV-FINAL
//! M3). Like the other direct-path rules, these assume that no dedup record expires during the
//! history.
//!
//! # Stable results (`unstable-result`)
//!
//! From OLTP-G03 in `docs/testing/guarantees/oltp.md`: "On the direct path, a retry of a mutation
//! with the same request ID and body returns the first definite result of that identity,
//! including a no-op or row conflict, while its dedup record is retained"; and O08 in
//! `docs/testing/oltp-contract.md`: "No-op and conflict results remain stable by identity". D02
//! also requires "the original result".
//!
//! Two attempts of one identity and body that both got a definite result must agree, if the
//! second was invoked after the first was answered. Definite results are:
//!
//! - an `ok`: the existence flag of an `update` or `delete` (an absent flag is not compared), the
//!   count of an `insert_many` when recorded, and plain success for an `insert` or `upsert`;
//! - a `conflict` refusal of an `insert` or `insert_many` over an existing row.
//!
//! Not flagged:
//!
//! - Attempts that overlap. Neither is a retry of the other, and the text does not say which
//!   result is the original one.
//! - Refusals other than that conflict, such as `overloaded`, `storage_unavailable`, a deadline
//!   or an abandoned request. They answer an attempt that did not execute (D10, D12), so they can
//!   come before or after a result.
//! - A `conflict` that another body under the identity could explain (that body was invoked
//!   before the answer), a `conflict` of an `upsert`, `update` or `delete` (only reuse causes it),
//!   and a `conflict` whose recorded message does not say that a key "already exists". The queued
//!   path uses the same code for a moved route and an abandoned run. Without a recorded message,
//!   every other `conflict` of an insert counts as a row conflict.
//! - A single answer, however wrong. A lone no-op `false` whose effect then appears hits neither
//!   rule: this one needs the request to be answered again, and the second one needs a
//!   `not_applied` answer.
//!
//! # Request-level `not_applied` (`not-applied-took-effect`)
//!
//! From OLTP-G04: "Return `not_applied` only with proof of non-application. Otherwise, report
//! uncertainty as `outcome_unknown`"; and the D05 note in `docs/testing/oltp-contract.md`: 'D05
//! "row unchanged" applies to a `not_applied` answer'. The rule reads both as statements about
//! the request. [`super::super::outcome`] already checks the attempt itself.
//!
//! A refusal that says `not_applied` speaks for every attempt of its identity and body that was
//! invoked before it and whose outcome is unknown. If the history is linearizable, but not
//! without those attempts, then one of them took effect, and the rule flags the refusal. Every
//! kind of observation counts: reads, existence flags, conflicts and the rows of a batch. The
//! rule does not say whether the effect landed before or after the refusal. The answer is wrong
//! in both cases, and a history rarely shows which.
//!
//! Which attempts and answers count:
//!
//! - The refusal covers attempts invoked before it was *invoked*, whether or not the client had
//!   given up on them yet. An attempt invoked while the refused one was in flight may have reached
//!   the server after the refusal, so it may take effect, as may any later retry.
//! - Attempts with a definite answer are never covered. If an attempt of the request invoked
//!   before the refusal returned is answered with a commit, the refusal is not flagged: a `429`
//!   after a success, or one that overlaps it, does not take the success back.
//! - An observation counts only if it was *invoked after* the refusal returned. The answer of an
//!   operation invoked before that and returning later is taken as unknown.
//! - An effect that some answer had already shown when the refusal returned is not flagged. The
//!   rule is about effects that appear afterwards.
//! - An effect that another operation could have caused is not flagged, because the history stays
//!   linearizable without the covered attempts.
//! - A no-op or `conflict` answer is not an observation if its identity had been used before, with
//!   the same body or another. A protocol that records no-ops and refusals could give it without
//!   any effect, so this rule does not depend on the decision about the first.
//! - Of the `conflict` answers, only the row conflict counts: the one to an `insert` or
//!   `insert_many` whose recorded message says that a key "already exists". Any other, and any
//!   without a message, is neither an observation nor a refusal that covers earlier attempts. The
//!   queued path uses the code when a run was abandoned or fenced, a preparation conflicted, or
//!   the schema or a route moved, and both paths use it when the identity is taken. None of these
//!   says that a row exists, and they can mean that another execution of the request holds its
//!   identity. `sim-oltp` always records the message.
//! - A completion of type `fail` that does not say `not_applied` is not a refusal here.
//! - If the history is not linearizable to begin with on the keys involved, apart from those
//!   answers, [`super::check`] reports that, and this rule says nothing about them.
//!
//! A change of a row that the checker does not model, such as a dropped table, can still produce a
//! hit on a legal history.
//!
//! A search that reaches its limit is reported as `check-limit`, so it cannot pass silently.
//!
//! # Known limits against the decided retry rule
//!
//! The retry rule is lead decision 11 of 2026-11-24: "Retry results" in section 4 of
//! `plans/oltp-direct-path.md`, OLTP-G03 and O08. Neither rule here is that rule. The differences
//! that the independent review of PR #126 listed:
//!
//! 1. Overlapping attempts that disagree are not flagged. The retry rule gives an identity one
//!    result, so one of the two answers is wrong.
//! 2. A lone no-op `false` whose effect then appears is flagged by neither rule, and a lone row
//!    conflict whose effect appears only by the second.
//! 3. A row conflict is not a result if another body was invoked before its answer, even if the
//!    message says "already exists" and that other body was refused with a `429`.
//! 4. Another body that commits after a recorded no-op or row conflict is not flagged, also not by
//!    default. The retry rule does not say whether those results hold the identity against
//!    another body. This needs a decision.
//! 5. The retry rule holds while the dedup record is retained. These rules assume that no record
//!    expires and do not use `time_ns`.
//! 6. Row conflicts are recognised by their message. Without one, the first rule counts every
//!    `conflict` of an insert and the second none. Both include `insert_many`, and the retry rule
//!    names only `insert`.
//! 7. Refusals that the retry rule does not list are not results here: a deadline, an abandoned
//!    request, a preparation conflict, a changed schema, a missing table, a limit and reuse.
//! 8. The first rule compares pairs of answers by their order, not with "the first" result. The
//!    recorded result of an attempt that was never answered is invisible to it.
//! 9. Only the existence flag and the batch count are compared, and an absent one is not.
//!
//! The default check is further from the retry rule. It rejects the replay of a recorded no-op or
//! row conflict, and a refused other body after a no-op. The corpus case `direct_valid_retries`
//! predates decision 11: it records the shipped recomputing (`false`, then `true`) as valid, which
//! the plan now calls a known gap. The case and the default are unchanged, so the first rule flags
//! it.

use super::super::linearizable::{
    self, DEFAULT_MAX_CONFIGS, Identities, anchor, body, classify, committed_answer, error_code,
    existed, rows_of,
};
use super::super::{Anomaly, Verdict};
use super::describe;
use crate::history::{Event, EventType, Op, Outcome, str_field};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The opt-in rules that [`super::check_with`] adds. The default adds none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// Definite results of one identity and body agree (`unstable-result`).
    pub stable_results: bool,
    /// `not_applied` speaks for the earlier attempts of the request
    /// (`not-applied-took-effect`).
    pub request_not_applied: bool,
    /// The bound on each search that `request_not_applied` makes, as in
    /// [`linearizable::Options`].
    pub max_configs: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            stable_results: false,
            request_not_applied: false,
            max_configs: DEFAULT_MAX_CONFIGS,
        }
    }
}

/// The attempts of each identity and body, in invocation order.
fn requests<'a>(
    ids: &Identities<'a>,
    ops: &[Op],
) -> BTreeMap<(String, &'a str, String), Vec<usize>> {
    let mut out = BTreeMap::<_, Vec<usize>>::new();
    for ((anchor, rid), members) in &ids.by_id {
        for &i in members {
            let request = (anchor.clone(), *rid, body(&ops[i]));
            out.entry(request).or_default().push(i);
        }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Success,
    Existed(bool),
    Count(u64),
    RowConflict,
}

impl Answer {
    fn agrees(self, other: Answer) -> bool {
        match (self, other) {
            // An insert_many may record its count in only one of the answers.
            (Answer::Success, Answer::Count(_)) | (Answer::Count(_), Answer::Success) => true,
            _ => self == other,
        }
    }
}

/// The definite result of attempt `i`, or `None` when its answer is not one.
fn definite_answer(ops: &[Op], ids: &Identities, i: usize) -> Option<Answer> {
    let op = &ops[i];
    let class = classify(&op.f)?;
    match op.outcome {
        Outcome::Info => None,
        Outcome::Ok => match class {
            "update" | "delete" => existed(op).map(Answer::Existed),
            "insert_many" => {
                let count = op.complete_value.get("data").unwrap_or(&op.complete_value);
                Some(count.as_u64().map_or(Answer::Success, Answer::Count))
            }
            _ => Some(Answer::Success),
        },
        Outcome::Fail => {
            let row_conflict = matches!(class, "insert" | "insert_many")
                && error_code(op) == Some("conflict")
                && str_field(&op.complete_value, "message")
                    .is_none_or(|m| m.contains("already exists"))
                && !another_body_before(ops, ids, i);
            row_conflict.then_some(Answer::RowConflict)
        }
    }
}

/// Another body under the identity was invoked before attempt `i` was answered. This is wider
/// than [`Identities::reuse_explains`]: here a refusal or a no-op may hold a record too.
fn another_body_before(ops: &[Op], ids: &Identities, i: usize) -> bool {
    let op = &ops[i];
    let (Some(rid), Some(anchor)) = (op.request_id.as_deref(), anchor(op)) else {
        return false;
    };
    let mine = body(op);
    ids.by_id[&(anchor, rid)]
        .iter()
        .map(|&j| &ops[j])
        .any(|other| other.call < op.ret_pos() && body(other) != mine)
}

/// `unstable-result`, at most once per identity and body.
pub(super) fn stable_results(ops: &[Op]) -> Verdict {
    let ids = Identities::new(ops);
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for ((anchor, rid, _), attempts) in requests(&ids, ops) {
        let answers: Vec<(usize, Answer)> = attempts
            .iter()
            .filter_map(|&i| Some((i, definite_answer(ops, &ids, i)?)))
            .collect();
        checked += answers.len();
        let unstable = answers.iter().find_map(|&(retry, again)| {
            answers
                .iter()
                .find(|&&(first, original)| {
                    ops[first].precedes(&ops[retry]) && !original.agrees(again)
                })
                .map(|&(first, original)| (first, original, retry, again))
        });
        if let Some((first, original, retry, again)) = unstable {
            anomalies.push(Anomaly::new(
                "unstable-result",
                format!(
                    "request {rid:?} at anchor {anchor:?}: {} answered {original:?} (event {}), and the retry {} invoked after that answered {again:?}",
                    describe(&ops[first]),
                    ops[first].complete_index.unwrap_or(ops[first].invoke_index),
                    describe(&ops[retry]),
                ),
                [ops[first].indices(), ops[retry].indices()].concat(),
            ));
        }
    }
    Verdict::new(checked, anomalies)
}

/// A refusal that says `not_applied`, in either spelling the history writers use.
fn says_not_applied(op: &Op) -> bool {
    op.outcome == Outcome::Fail
        && (str_field(&op.complete_value, "outcome") == Some("not_applied")
            || op.complete_value.get("not_applied") == Some(&Value::Bool(true)))
}

fn is_conflict(op: &Op) -> bool {
    op.outcome == Outcome::Fail && error_code(op) == Some("conflict")
}

/// A `conflict` whose message says that a row was in the way. Others may be about the identity,
/// a run, the schema or a route.
fn is_row_conflict(op: &Op) -> bool {
    matches!(classify(&op.f), Some("insert" | "insert_many"))
        && is_conflict(op)
        && str_field(&op.complete_value, "message").is_some_and(|m| m.contains("already exists"))
}

/// `not-applied-took-effect`, at most once per identity and body.
pub(super) fn request_not_applied(events: &[Event], ops: &[Op], max_configs: usize) -> Verdict {
    let ids = Identities::new(ops);
    let requests = requests(&ids, ops);
    let ignored = not_observations(&ids, ops);
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for ((anchor, rid, _), attempts) in requests {
        let mut scope = None;
        let refuses = |op: &Op| says_not_applied(op) && (!is_conflict(op) || is_row_conflict(op));
        for &refused in attempts.iter().filter(|&&i| refuses(&ops[i])) {
            let returned = ops[refused].ret_pos();
            let committed = |op: &Op| committed_answer(op) == Some(true) && op.call < returned;
            if attempts.iter().any(|&i| committed(&ops[i])) {
                continue;
            }
            let covered: BTreeSet<usize> = attempts
                .iter()
                .copied()
                .filter(|&i| ops[i].outcome == Outcome::Info && ops[i].call < ops[refused].call)
                .collect();
            if covered.is_empty() {
                continue;
            }
            checked += 1;
            let scope = scope
                .get_or_insert_with(|| Scope::new(events, ops, &ignored, &anchor, max_configs));
            if !scope.base.valid {
                let limit = scope
                    .base
                    .anomalies
                    .iter()
                    .find(|a| a.kind == "check-limit");
                anomalies.extend(limit.cloned());
                break;
            }
            let mut verdict = scope.without(&covered, returned, true);
            if verdict.valid {
                verdict = scope.without(&covered, returned, false);
            } else if verdict.has("non-linearizable") {
                continue; // The effect had already appeared.
            }
            let Some(cause) = verdict.anomalies.first() else {
                continue;
            };
            if cause.kind != "non-linearizable" {
                anomalies.push(cause.clone());
                continue;
            }
            let sources: Vec<String> = covered.iter().map(|&i| describe(&ops[i])).collect();
            let mut involved = ops[refused].indices();
            involved.extend(covered.iter().flat_map(|&i| ops[i].indices()));
            involved.extend(&cause.events);
            anomalies.push(Anomaly::new(
                "not-applied-took-effect",
                format!(
                    "request {rid:?} at anchor {anchor:?}: {} was answered not_applied (event {}), yet an earlier attempt took effect ({}). Without it, {}",
                    describe(&ops[refused]),
                    ops[refused].complete_index.unwrap_or(ops[refused].invoke_index),
                    sources.join("; "),
                    cause.detail,
                ),
                involved,
            ));
            break;
        }
    }
    Verdict::new(checked, anomalies)
}

/// Answers that may say nothing about the rows: no-ops and `conflict` refusals under an identity
/// that had been used before, and every `conflict` that is not a row conflict.
fn not_observations(ids: &Identities, ops: &[Op]) -> BTreeSet<usize> {
    let mut out: BTreeSet<usize> = (0..ops.len())
        .filter(|&i| is_conflict(&ops[i]) && !is_row_conflict(&ops[i]))
        .collect();
    for members in ids.by_id.values() {
        for &i in members {
            let op = &ops[i];
            let used = |&j: &usize| j != i && ops[j].call < op.ret_pos();
            if (committed_answer(op) == Some(false) || is_conflict(op)) && members.iter().any(used)
            {
                out.insert(i);
            }
        }
    }
    out
}

/// The part of a history that shares keys with one request.
struct Scope<'a> {
    events: &'a [Event],
    ops: &'a [Op],
    /// Operations on the keys that batches connect to the request's anchor.
    members: Vec<usize>,
    max_configs: usize,
    /// The verdict with every attempt in place.
    base: Verdict,
}

impl<'a> Scope<'a> {
    fn new(
        events: &'a [Event],
        ops: &'a [Op],
        ignored: &BTreeSet<usize>,
        request_anchor: &str,
        max_configs: usize,
    ) -> Self {
        let mut keys = BTreeSet::from([request_anchor.to_string()]);
        loop {
            let known = keys.len();
            for op in ops.iter().filter(|op| op.f == "insert_many") {
                let batch: Vec<String> = rows_of(&op.invoke_value)
                    .into_iter()
                    .flatten()
                    .map(|(key, _)| key)
                    .chain(anchor(op))
                    .collect();
                if batch.iter().any(|key| keys.contains(key)) {
                    keys.extend(batch);
                }
            }
            if keys.len() == known {
                break;
            }
        }
        let members: Vec<usize> = (0..ops.len())
            .filter(|i| classify(&ops[*i].f).is_some() && !ignored.contains(i))
            .filter(|&i| anchor(&ops[i]).is_some_and(|key| keys.contains(&key)))
            .collect();
        let mut scope = Scope {
            events,
            ops,
            members,
            max_configs,
            base: Verdict::new(0, Vec::new()),
        };
        scope.base = scope.check(scope.history(|_| true, None, None));
        scope
    }

    /// Checks the history without the covered attempts. With `until`, only what had been
    /// answered at position `returned`. Otherwise all of it, except the answers to operations in
    /// flight at that position.
    fn without(&self, covered: &BTreeSet<usize>, returned: usize, until: bool) -> Verdict {
        let keep = |i: usize| !covered.contains(&i);
        self.check(if until {
            self.history(keep, Some(returned), None)
        } else {
            self.history(keep, None, Some(returned))
        })
    }

    fn history(
        &self,
        keep: impl Fn(usize) -> bool,
        until: Option<usize>,
        in_flight_at: Option<usize>,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        for &i in self.members.iter().filter(|&&i| keep(i)) {
            let op = &self.ops[i];
            if until.is_some_and(|end| op.call > end) {
                continue;
            }
            let forget = in_flight_at
                .is_some_and(|at| op.call < at && op.done.is_some_and(|done| done > at));
            if forget && op.outcome == Outcome::Fail {
                continue; // It has no effect, and its answer is not to count.
            }
            events.push((op.call, self.events[op.call].clone()));
            if let Some(done) = op.done.filter(|&done| until.is_none_or(|end| done <= end)) {
                let mut event = self.events[done].clone();
                if forget {
                    event.kind = EventType::Info;
                    event.value = Value::Null;
                }
                events.push((done, event));
            }
        }
        events.sort_by_key(|&(position, _)| position);
        events.into_iter().map(|(_, event)| event).collect()
    }

    fn check(&self, history: Vec<Event>) -> Verdict {
        linearizable::check_kv_opts(
            &history,
            &linearizable::Options {
                request_dedup: true,
                max_configs: self.max_configs,
            },
        )
    }
}
