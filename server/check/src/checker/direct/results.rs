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
//! A request that answered a no-op or a row conflict and was also refused as reuse is
//! `unstable-result`: whichever answer is right, the other is wrong.
//!
//! The record of an identity has no body in its key, and "A different digest returns the existing
//! `conflict` error" (plan, section 4; D03). So a no-op or a row conflict, answered or not, explains
//! the refusal of another body. And "No-op and conflict results remain stable by identity" (O08), so
//! the replay of the row conflict of an `insert_many` is legal too.
//!
//! # What is not judged
//!
//! No text decides these. They are accepted, and [`not_judged`] counts them by name in
//! [`Verdict::not_judged`](super::super::Verdict::not_judged):
//!
//! | Name | Case |
//! | --- | --- |
//! | `other-body-committed` | another body under the identity commits, not clearly before the first one answered a no-op or a row conflict. By the text above it should be refused; a question for the user |
//! | `other-body-without-effect` | the same, but it answers a no-op or a row conflict of its own |
//! | `other-body-unknown` | another body under the identity of an answered no-op or row conflict has an unknown attempt and no answered commit. It may have committed, and it may have been refused |
//! | `insert-many-recomputed` | an `insert_many` refused over a row commits on retry. Decision 11 names only `insert` |
//! | `request-spans-retention` | the attempts of a request with a no-op or a row conflict span [`RETENTION_NS`] or more, by `time_ns`. Its answers may be replayed or recomputed, because its record may have been deleted. Its commits are assumed to hold. Without `time_ns` nothing expires |
//!
//! Not results, and so neither stable nor held: `not_found`, a deadline, an abandoned or fenced
//! run, a conflicted preparation, a changed schema, a moved route, a limit. A `conflict` of an
//! insert must be explained by a row or another body whatever its message, so a `conflict` of the
//! queued path over an absent row is flagged, as before decision 11. After an answered commit of
//! another body, a no-op that the first body answers is `request-id-reuse-undetected` (D03), also
//! if it repeats an earlier one; if the two overlap it may have come first, and is accepted.
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
use std::collections::{BTreeMap, HashMap, HashSet};

/// The default of `direct_dedup_retention_ms`: 24 hours.
pub const RETENTION_NS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// The names in [`Verdict::not_judged`](super::super::Verdict::not_judged).
pub const OTHER_BODY_COMMITTED: &str = "other-body-committed";
pub const OTHER_BODY_UNCHANGED: &str = "other-body-without-effect";
pub const OTHER_BODY_UNKNOWN: &str = "other-body-unknown";
pub const INSERT_MANY_RECOMPUTED: &str = "insert-many-recomputed";
pub const EXPIRED: &str = "request-spans-retention";

/// The message of a refusal over an existing row.
pub const ROW_CONFLICT: &str = "primary key already exists";

/// The messages of a refusal because of another body.
const REUSE: &[&str] = &[
    "request_id was already used with a different request body",
    "request identity has a different body",
    "request UUID conflicts with its durable identity",
];

/// The messages of the other `conflict` refusals that an insert can get: of the request itself and
/// of the queued path.
const OTHER_CONFLICTS: &[&str] = &[
    "duplicate primary key in batch",
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
        Some(message) if REUSE.contains(&message) || OTHER_CONFLICTS.contains(&message) => {
            InsertConflict::Other
        }
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
        None => (op.f == "insert" && is_row_conflict(op)).then_some(Definite::RowConflict),
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

/// What the answers of each request say about its attempts.
pub(crate) struct Requests {
    without_effect: HashMap<usize, Definite>,
    spanning: HashSet<usize>,
}

impl Requests {
    pub(crate) fn new(ids: &Identities, ops: &[Op]) -> Self {
        let mut requests = Requests {
            without_effect: HashMap::new(),
            spanning: HashSet::new(),
        };
        for attempts in ids.requests().into_values() {
            let mut results = attempts.iter().filter_map(|&i| definite_result(&ops[i]));
            let first = results.next();
            let agree = results.all(|other| Some(other) == first);
            if may_expire(ops, &attempts) {
                requests.spanning.extend(&attempts);
            } else if let Some(first) = first
                && first != Definite::Commit
                && agree
            {
                requests
                    .without_effect
                    .extend(attempts.iter().map(|&i| (i, first)));
            }
        }
        requests
    }

    /// The one result of this attempt's request, if the request never took effect.
    pub(crate) fn without_effect(&self, attempt: usize) -> Option<Definite> {
        self.without_effect.get(&attempt).copied()
    }

    /// Its request spans the retention.
    pub(crate) fn spans(&self, attempt: usize) -> bool {
        self.spanning.contains(&attempt)
    }
}

/// A refused insert, also of a batch, that is known to be about a present row.
pub(crate) fn is_row_conflict(op: &Op) -> bool {
    refused_insert(op) == Some(InsertConflict::Row)
}

/// [`is_row_conflict`], or a refusal that cannot be placed.
pub(crate) fn may_be_row_conflict(op: &Op) -> bool {
    is_row_conflict(op) || refused_insert(op) == Some(InsertConflict::Unclassified)
}

/// A refusal because of another body: only that refuses an `upsert`, `update` or `delete` with
/// `conflict`, unless its message says otherwise.
fn is_reuse(op: &Op) -> bool {
    if op.outcome != Outcome::Fail || code_of(&op.complete_value) != Some("conflict") {
        return false;
    }
    let message = message_of(&op.complete_value);
    match classify(&op.f) {
        Some("insert" | "insert_many") => message.is_some_and(|m| REUSE.contains(&m)),
        Some("write" | "update" | "delete") => {
            let other = |m| m == ROW_CONFLICT || OTHER_CONFLICTS.contains(&m);
            message.is_none_or(|m| REUSE.contains(&m) || !other(m))
        }
        _ => false,
    }
}

/// Another body under an attempt's identity may have held it before that attempt was answered.
pub(super) fn may_hold(op: &Op) -> bool {
    op.outcome == Outcome::Info || committed_answer(op).is_some() || may_be_row_conflict(op)
}

/// A request that answered a no-op or a row conflict and was also refused as reuse: if its result
/// is recorded, the refusal is wrong, and otherwise the result is.
pub(super) fn result_and_refusal(ops: &[Op], attempts: &[usize]) -> Option<(usize, usize)> {
    if may_expire(ops, attempts) {
        return None;
    }
    let mut results = attempts
        .iter()
        .copied()
        .filter(|&i| definite_result(&ops[i]).is_some_and(|r| r != Definite::Commit));
    let refused = attempts.iter().copied().find(|&i| is_reuse(&ops[i]));
    Some((results.next()?, refused?))
}

/// What the checker accepts because no text decides it, by name.
pub(super) fn not_judged(ids: &Identities, ops: &[Op]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    let mut count = |name: &str, times: usize| {
        if times > 0 {
            *out.entry(name.to_string()).or_default() += times;
        }
    };
    let mut identities = BTreeMap::<(String, &str), Vec<Vec<usize>>>::new();
    for ((anchor, rid, _), attempts) in ids.requests() {
        identities.entry((anchor, rid)).or_default().push(attempts);
    }
    let is = |i: usize, commit: bool| {
        definite_result(&ops[i]).is_some_and(|r| (r == Definite::Commit) == commit)
    };
    let unchanged = |i: usize| is(i, false);
    let committed = |i: usize| is(i, true);
    for bodies in identities.values() {
        let answered = |body: &[usize]| body.iter().copied().find(|&i| unchanged(i));
        // It did not clearly commit before the other body asked.
        let commits_after = |body: &[usize], first: usize| {
            let mut commits = body.iter().copied().filter(|&i| committed(i));
            let precedes = |i: usize| ops[i].precedes(&ops[first]);
            commits.next().is_some_and(|i| !precedes(i)) && commits.all(|i| !precedes(i))
        };
        let others = |b: usize| bodies.iter().enumerate().filter(move |(o, _)| *o != b);
        for (b, attempts) in bodies.iter().enumerate() {
            let after = others(b)
                .filter_map(|(_, other)| answered(other))
                .any(|first| commits_after(attempts, first));
            count(OTHER_BODY_COMMITTED, usize::from(after));
            let unknown = attempts.iter().any(|&i| ops[i].outcome == Outcome::Info);
            let open = unknown && !attempts.iter().any(|&i| definite_result(&ops[i]).is_some());
            let beside = others(b).any(|(_, other)| answered(other).is_some());
            count(OTHER_BODY_UNKNOWN, usize::from(open && beside));
            if may_expire(ops, attempts) && answered(attempts).is_some() {
                count(EXPIRED, 1);
            }
            let batch = |i: &usize| ops[*i].f == "insert_many" && is_row_conflict(&ops[*i]);
            let recomputed = attempts.iter().any(batch) && attempts.iter().any(|&i| committed(i));
            count(INSERT_MANY_RECOMPUTED, usize::from(recomputed));
        }
        let without = bodies.iter().filter(|b| answered(b).is_some()).count();
        count(OTHER_BODY_UNCHANGED, without.saturating_sub(1));
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
