//! Opt-in rules about what one answer says for the whole request identity.
//!
//! Both are off by default ([`Options`]). They test one reading of an open question, and
//! [`super::check`] follows the other one. `plans/oltp-direct-path.md`, section 4: "Read-only
//! outcomes such as `update` on a missing row commit nothing and are recomputed on retry"; and
//! section 6, step 1: "When full, return `429 overloaded` with `not_applied`". Until the user
//! decides which text is the contract, a hit is a finding to review. It is not a failed
//! guarantee, and a history without hits is not evidence for one.
//!
//! The rules only add anomalies. They do not make the default accept the other reading: a retry
//! that replays `false` after the row was inserted is `non-linearizable` there.
//!
//! The identity and the body are those of [`super`]: the table-qualified anchor key and the
//! request ID, and `f` plus the invoke value. Reads have no identity (plan addendum, RV-FINAL
//! M3). Like the other direct-path rules, these assume that no dedup record expires during the
//! history.
//!
//! # Stable results (`unstable-result`)
//!
//! From `AGENTS.md`, "OLTP direct path": "a retry with the same request ID and body returns the
//! original result"; and O08 in `docs/testing/oltp-contract.md`: "No-op and conflict results
//! remain stable by identity". D02 also requires "the original result".
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
//!   path uses the same code for a moved route and an abandoned run.
//! - A single answer, however wrong. A no-op answered `false` whose effect then appears is
//!   flagged only if the request is answered again.
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
//! - A completion of type `fail` that does not say `not_applied` is not a refusal here.
//! - If the history is not linearizable to begin with on the keys involved, apart from those
//!   answers, [`super::check`] reports that, and this rule says nothing about them.
//!
//! A search that reaches its limit is reported as `check-limit`, so it cannot pass silently.

use super::super::linearizable::{
    self, Identities, anchor, body, classify, committed_answer, error_code, existed, rows_of,
};
use super::super::{Anomaly, Verdict};
use super::describe;
use crate::history::{Event, EventType, Op, Outcome, str_field};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The opt-in rules that [`super::check_with`] adds. The default adds none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Options {
    /// Definite results of one identity and body agree (`unstable-result`).
    pub stable_results: bool,
    /// `not_applied` speaks for the earlier attempts of the request
    /// (`not-applied-took-effect`).
    pub request_not_applied: bool,
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

/// `not-applied-took-effect`, at most once per identity and body.
pub(super) fn request_not_applied(events: &[Event], ops: &[Op]) -> Verdict {
    let ids = Identities::new(ops);
    let requests = requests(&ids, ops);
    let replays = possible_replays(&ids, ops);
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for ((anchor, rid, _), attempts) in requests {
        let mut scope = None;
        for &refused in attempts.iter().filter(|&&i| says_not_applied(&ops[i])) {
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
            let scope = scope.get_or_insert_with(|| Scope::new(events, ops, &replays, &anchor));
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

/// No-op and `conflict` answers under an identity that had been used before.
fn possible_replays(ids: &Identities, ops: &[Op]) -> BTreeSet<usize> {
    let conflict = |op: &Op| op.outcome == Outcome::Fail && error_code(op) == Some("conflict");
    let mut out = BTreeSet::new();
    for members in ids.by_id.values() {
        for &i in members {
            let op = &ops[i];
            let used = |&j: &usize| j != i && ops[j].call < op.ret_pos();
            if (committed_answer(op) == Some(false) || conflict(op)) && members.iter().any(used) {
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
    /// The verdict with every attempt in place.
    base: Verdict,
}

impl<'a> Scope<'a> {
    fn new(
        events: &'a [Event],
        ops: &'a [Op],
        replays: &BTreeSet<usize>,
        request_anchor: &str,
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
            .filter(|i| classify(&ops[*i].f).is_some() && !replays.contains(i))
            .filter(|&i| anchor(&ops[i]).is_some_and(|key| keys.contains(&key)))
            .collect();
        let mut scope = Scope {
            events,
            ops,
            members,
            base: Verdict::new(0, Vec::new()),
        };
        scope.base = check(scope.history(|_| true, None, None));
        scope
    }

    /// Checks the history without the covered attempts. With `until`, only what had been
    /// answered at position `returned`. Otherwise all of it, except the answers to operations in
    /// flight at that position.
    fn without(&self, covered: &BTreeSet<usize>, returned: usize, until: bool) -> Verdict {
        let keep = |i: usize| !covered.contains(&i);
        check(if until {
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
}

fn check(history: Vec<Event>) -> Verdict {
    linearizable::check_kv_opts(
        &history,
        &linearizable::Options {
            request_dedup: true,
            ..linearizable::Options::default()
        },
    )
}
