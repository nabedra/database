//! An opt-in rule about what a refusal says for the whole request.
//!
//! It is off by default ([`Options`]). It tests a reading of `not_applied` that nobody has decided:
//! section 6, step 1, of `plans/oltp-direct-path.md` says "When full, return `429 overloaded` with
//! `not_applied`", and does not say whether that speaks for the request or for one attempt. A hit
//! is a finding to review. It is not a failed guarantee, and a history without hits is not evidence
//! for one. The rule only adds anomalies.
//!
//! The identity and the body are those of [`super`]. Reads have no identity. The retry rule that
//! was decided is in [`super::results`] and always on.
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
//!   the same body or another. It may be the replay of a recorded result or a refusal of reuse,
//!   which say nothing about the row at that time.
//! - Of the `conflict` answers, only the row conflict that [`super::insert_conflict`] recognises
//!   counts.
//!   Any other is neither an observation nor a refusal that covers earlier attempts. As an
//!   observation it says nothing about a row. As a refusal, a taken identity may be held by
//!   another execution of the request, and a conflicted preparation or a moved schema or route may
//!   be its doing. An abandoned run and a fenced authority are different: they claim that the
//!   request did not apply at least as firmly as a `429` does, and leaving them out is a choice, to
//!   rely on one recognised kind of `conflict` only. The consequence is the same for all of them:
//!   **an effect of an earlier attempt that appears after such a refusal is not flagged**.
//!   `tests/fixtures/accepted_misses/` holds histories of that shape.
//! - A completion of type `fail` that does not say `not_applied` is not a refusal here.
//! - If the history is not linearizable to begin with on the keys involved, apart from those
//!   answers, [`super::check`] reports that, and this rule says nothing about them.
//!
//! A change of a row that the checker does not model, such as a dropped table, can still produce a
//! hit on a legal history.
//!
//! A search that reaches its limit is reported as `check-limit`, so it cannot pass silently.
//!
//! Row conflicts are recognised by their message, as in [`super::results`], which says how the
//! answers that cannot be placed are counted.

use super::super::linearizable::{
    self, DEFAULT_MAX_CONFIGS, Identities, anchor, classify, code_of, committed_answer, rows_of,
};
use super::super::{Anomaly, Verdict};
use super::describe;
use super::results::{InsertConflict, refused_insert};
use crate::history::{Event, EventType, Op, Outcome, str_field};
use serde_json::Value;
use std::collections::BTreeSet;

/// The opt-in rule that [`super::check_with`] adds. The default does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
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
            request_not_applied: false,
            max_configs: DEFAULT_MAX_CONFIGS,
        }
    }
}

/// A refusal that says `not_applied`, in either spelling the history writers use.
fn says_not_applied(op: &Op) -> bool {
    op.outcome == Outcome::Fail
        && (str_field(&op.complete_value, "outcome") == Some("not_applied")
            || op.complete_value.get("not_applied") == Some(&Value::Bool(true)))
}

fn is_conflict(op: &Op) -> bool {
    op.outcome == Outcome::Fail && code_of(&op.complete_value) == Some("conflict")
}

fn is_row_conflict(op: &Op) -> bool {
    refused_insert(op) == Some(InsertConflict::Row)
}

/// `not-applied-took-effect`, at most once per identity and body.
pub(super) fn request_not_applied(events: &[Event], ops: &[Op], max_configs: usize) -> Verdict {
    let ids = Identities::new(ops);
    let requests = ids.requests();
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
