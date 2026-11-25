//! The retry rule of the direct path: which answers are results of a request, and which results
//! one request may have.
//!
//! The rule is lead decision 11 of 2026-11-24, "Retry results" in section 4 of
//! `plans/oltp-direct-path.md`. OLTP-G03 (`docs/testing/guarantees/oltp.md`) and "Direct-path retry
//! results" in `docs/testing/oltp-contract.md` (O08) say the same: "a retry of a mutation with the
//! same request ID and body returns the first definite result of that identity while its dedup
//! record is retained. That includes a no-op (`update` or `delete` of a missing row) and a row
//! conflict (`insert` of a present row). Unknown outcomes, unavailable or overloaded answers and
//! validation errors are not results and are not recorded. Reads have no identity."
//!
//! The shipped executor recomputes no-ops and row conflicts on retry, which the documents call a
//! known gap. The checker follows the documents, so it can flag a history of the shipped executor.
//! Such a hit is a finding.
//!
//! # What is enforced
//!
//! A *request* is the attempts that share the table-qualified anchor key, the request ID and the
//! body (`f` plus the invoke value). Its *definite results* are
//!
//! - a commit: an `ok` `insert`, `insert_many` or `upsert`, or an `ok` `update` or `delete` whose
//!   existence flag is `true`;
//! - a no-op: an `ok` `update` or `delete` whose flag is `false`;
//! - a row conflict: the `conflict` refusal of an `insert` that [`insert_conflict`] recognises as
//!   one over an existing row.
//!
//! The results of one request are equal (`unstable-result`, or `dedup-miss` for a retry sent after
//! a commit was answered). Overlapping attempts count too: whichever executed first, the others
//! are answered from its record. The answers are compared whatever other bodies did.
//!
//! A request whose result is a no-op or a row conflict never took effect, and it read the row once.
//! [`super::super::linearizable`] gives all its attempts one point in time, between the first
//! invoke and the first definite answer. So the replay of that result is legal whatever the row is
//! by then, and the attempt that read the row may be one whose answer was lost. An effect of any
//! attempt of such a request is `non-linearizable`, also when only one answer was ever given.
//!
//! # Where the decision is silent
//!
//! There the verdict is the one this checker gave before decision 11:
//!
//! | Case | Verdict |
//! | --- | --- |
//! | Another body under the identity commits after a no-op or a row conflict | accepted: these results do not hold the identity |
//! | Another body is refused as reuse, and only answered no-ops or row conflicts came before | flagged: `request-id-reuse-misdetected` for an `upsert`, `update` or `delete`, `non-linearizable` for an insert of an absent row |
//! | The same, but one of those requests also has an unknown attempt | accepted: that attempt may hold the identity, without an effect. Before, it had to be able to take effect on the rows as they were, which the rule now excludes |
//! | After such a commit, the first body answers its no-op again | flagged: `request-id-reuse-undetected` |
//! | After such a commit, the first body is refused as reuse | accepted |
//! | `insert_many` over a present row | not a result: each attempt is judged on the rows at its time |
//! | `not_found`, a deadline, an abandoned or fenced run, a conflicted preparation, a changed schema, a moved route, a limit | not results. As before, every `conflict` of an insert has to be explained by a row or another body, whatever its message |
//! | Expired records | a request whose attempts span [`RETENTION_NS`] or more, by `time_ns`, is judged as before: its commits hold, its no-ops and row conflicts may be recomputed. Without `time_ns` nothing expires |
//!
//! # Message texts
//!
//! The product has no code for a row conflict, so its message tells it from other `conflict`
//! refusals. If the text changes, or a writer records none, the answer is not a result and the rule
//! goes quiet on it. So every `conflict` of an insert that [`insert_conflict`] cannot place is
//! counted in [`Verdict::unclassified_conflicts`](super::super::Verdict::unclassified_conflicts). It
//! is not an anomaly. A run with a count above zero has checked less than it seems, and its caller
//! has to print the count.
//!
//! `d03_o08_insert_conflict_texts_are_the_ones_the_history_checker_knows` in
//! `server/src/oltp_runtime/worker_runtime/direct_gateway/tests.rs` (check
//! `oltp-conflict-text-pin`) pins [`ROW_CONFLICT`] against the direct and the queued path. It also
//! reaches "duplicate primary key in batch" and "request_id was already used with a different
//! request body", but not every place in the product that constructs them, and no test pins the
//! other texts. Rewording one of those changes nothing that is flagged, and the answers are then
//! counted as unclassified.
//!
//! # Limits
//!
//! - Only the kind of result is compared. The count of a batch and the presence of the existence
//!   flag are checked on each answer (`non-linearizable`).
//! - The recorded result of an attempt that was never answered shows only in later answers.
//! - An attempt is assumed not to stay in flight for longer than the retention.

use super::super::linearizable::{Identities, classify, code_of, committed_answer, message_of};
use crate::history::{Op, Outcome};
use serde_json::Value;
use std::collections::HashMap;

/// The default of `direct_dedup_retention_ms`: 24 hours.
pub const RETENTION_NS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// The message of a refusal over an existing row.
pub const ROW_CONFLICT: &str = "primary key already exists";

/// The messages of the other `conflict` refusals that an insert can get: of the request itself, of
/// its identity, and of the queued path.
const OTHER_CONFLICTS: &[&str] = &[
    "duplicate primary key in batch",
    "request_id was already used with a different request body",
    "request identity has a different body",
    "request UUID conflicts with its durable identity",
    "original request run was abandoned",
    "original execution authority was fenced",
    "transaction preparation conflicted with an existing version",
    "transaction preparation conflicted after exhausting snapshot retries",
    "captured schema or lifecycle changed",
    "pinned Catalog route moved before queue admission",
    "participant routing lineage changed",
];

/// What a `conflict` refusal of an `insert` or `insert_many` is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertConflict {
    Row,
    Other,
    /// No recorded message, or one that is not known here.
    Unclassified,
}

/// Classifies an answer to an insert by its message, in every spelling in which its code is read.
/// `None` if its code is not `conflict`.
pub fn insert_conflict(answer: &Value) -> Option<InsertConflict> {
    if code_of(answer) != Some("conflict") {
        return None;
    }
    Some(match message_of(answer) {
        Some(ROW_CONFLICT) => InsertConflict::Row,
        Some(message) if OTHER_CONFLICTS.contains(&message) => InsertConflict::Other,
        _ => InsertConflict::Unclassified,
    })
}

/// [`insert_conflict`] of a refused insert.
pub(super) fn refused_insert(op: &Op) -> Option<InsertConflict> {
    let insert = matches!(classify(&op.f), Some("insert" | "insert_many"));
    if !insert || op.outcome != Outcome::Fail {
        return None;
    }
    insert_conflict(&op.complete_value)
}

pub(super) fn unclassified_conflicts(ops: &[Op]) -> usize {
    ops.iter()
        .filter(|op| refused_insert(op) == Some(InsertConflict::Unclassified))
        .count()
}

/// A definite result of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Definite {
    Commit,
    NoOp,
    RowConflict,
}

/// The definite result that an attempt was answered with.
pub(crate) fn definite_result(op: &Op) -> Option<Definite> {
    match committed_answer(op) {
        Some(true) => Some(Definite::Commit),
        Some(false) => Some(Definite::NoOp),
        None => (op.f == "insert" && refused_insert(op) == Some(InsertConflict::Row))
            .then_some(Definite::RowConflict),
    }
}

/// The record that the first of these attempts made may have expired before the last one.
fn may_expire(ops: &[Op], attempts: &[usize]) -> bool {
    let times = || {
        attempts
            .iter()
            .flat_map(|&i| [ops[i].call_time, ops[i].ret_time])
            .flatten()
    };
    match (times().min(), times().max()) {
        (Some(first), Some(last)) => last - first >= RETENTION_NS,
        _ => false,
    }
}

/// The attempts of the requests that never took effect, with the one result of their request.
pub(crate) fn without_effect(ids: &Identities, ops: &[Op]) -> HashMap<usize, Definite> {
    let mut out = HashMap::new();
    for attempts in ids.requests().into_values() {
        let mut results = attempts.iter().filter_map(|&i| definite_result(&ops[i]));
        let Some(first) = results.next() else {
            continue;
        };
        let agree = results.all(|other| other == first);
        if first != Definite::Commit && agree && !may_expire(ops, &attempts) {
            out.extend(attempts.into_iter().map(|i| (i, first)));
        }
    }
    out
}

/// Two attempts of one request that were answered with different results, in the order of their
/// answers. A fresh answer to a retry sent after a commit was answered is left to `dedup-miss`.
pub(super) fn disagreement(
    ops: &[Op],
    attempts: &[usize],
) -> Option<((usize, Definite), (usize, Definite))> {
    if may_expire(ops, attempts) {
        return None;
    }
    let mut answers: Vec<(usize, Definite)> = attempts
        .iter()
        .filter_map(|&i| Some((i, definite_result(&ops[i])?)))
        .collect();
    answers.sort_by_key(|&(i, _)| ops[i].ret_pos());
    let &(first, result) = answers.first()?;
    let missed = |i: usize| result == Definite::Commit && ops[first].precedes(&ops[i]);
    let other = answers
        .into_iter()
        .find(|&(i, other)| other != result && !missed(i))?;
    Some(((first, result), other))
}
