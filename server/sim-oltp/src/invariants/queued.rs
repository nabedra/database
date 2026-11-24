//! Rules for the queued path, judged from what clients and public stats observed.
//!
//! Each rule quotes the text it enforces. The text is in `docs/testing/scheduler-contract.md`
//! ("contract"), `docs/testing/guarantees/oltp.md` ("guarantees") or
//! `plans/resource-aware-scheduling.md` ("plan"). A rule reads no product symbol: its input
//! is the [`Log`] a workload writes from public HTTP replies, public stats and the process
//! lifecycle it drove itself.
//!
//! The stricter forms need probes that record who installed which copy under which authority
//! (`QueueCopy` and `Receipt` are reserved but never emitted). Those forms stay `not_run`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use uuid::Uuid;

/// Contract: "Retain accepted work through individual failures while a request copy
/// survives." Plan: "No accepted request may disappear on ordinary saturation, a transport
/// timeout, or an API restart."
///
/// An accepted request never answers as missing or abandoned in the run that accepted it,
/// unless its retained result has expired. The text gives no time bound for settling, so an
/// accepted request that is merely late is the workload's `Liveness`, not this rule.
pub(crate) const ACCEPTED_SETTLES: &str = "AcceptedSettlesInRun";

/// Guarantees, OLTP-G07: "Accepted queued work stays recoverable while a complete copy
/// survives. A full worker stop abandons outstanding work." Contract, Q03: "No acceptance
/// acknowledgment precedes required replication". Contract, R02: "Outstanding requests remain
/// on that holder". Contract, R04: "New run has no pending old work; old receipts report
/// abandonment unless a durable commit decision proves success".
pub(crate) const RETENTION: &str = "QueueRetention";

/// Contract, Q05: "One logical execution/receipt for identical identities, explicit conflict
/// for a different body; one mutation decision." Contract: "no logical mutation may publish
/// twice". Contract, Q06: "never silently execute a new logical mutation". Guarantees,
/// OLTP-G03: "Each request identity executes once, cluster-wide on the queued path and per
/// table and key on the direct path."
pub(crate) const ONE_EXECUTION: &str = "OneExecutionPerIdentity";

/// Contract: "No request may have conflicting terminal decisions". Plan: "an unknown or
/// expired receipt must never be fabricated as successful".
pub(crate) const RECEIPTS: &str = "ReceiptCheck";

/// Every rule this module judges from public observations.
pub(crate) const RULES: [&str; 4] = [ACCEPTED_SETTLES, RETENTION, ONE_EXECUTION, RECEIPTS];

/// What cannot run yet, and why. The sweep records each as `not_run`.
pub(crate) const NOT_RUN: [(&str, &str); 4] = [
    (
        "QueueRetention over QueueCopy",
        "needs a probe with the holder, version and completeness of each installed copy; QueueCopy is reserved and never emitted",
    ),
    (
        "OneExecutionPerIdentity over Applied",
        "needs a probe that names the queued request a Data commit belongs to; DedupApplied covers direct commits only",
    ),
    (
        "ReceiptCheck over Receipt",
        "needs a probe with the state and version of each terminal receipt copy; Receipt is reserved and never emitted",
    ),
    (
        "QueueCopyStaleNotInvalid",
        "left out: no text in the scheduler contract, the guarantees or the scheduling plan says how a copy from a newer authority is refused, and no probe reports a refused installation",
    ),
];

/// The route a request took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// Certainly the queue: `POST /v1/requests`, a cross-range batch, or `direct_oltp: false`.
    Queued,
    /// `POST /v1/operations` that the direct path may have served.
    MaybeDirect,
}

/// One body sent under a request identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Body {
    pub id: Uuid,
    /// The operation and payload, as sent.
    pub text: String,
    /// The smallest key, which is the direct path's identity key. `None` for a query.
    pub anchor: Option<i64>,
    /// The unique value this body writes, per key. Empty for reads and deletes.
    pub writes: Vec<(i64, i64)>,
    pub mutation: bool,
}

/// A definite result: the success data, or the error code.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Terminal {
    Succeeded(Value),
    Failed(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Answer {
    /// `202`, with the run that accepted it.
    Accepted(Uuid),
    Terminal(Terminal),
    /// A `404` that reports the request abandoned.
    Abandoned,
    /// A `404` that does not.
    Missing,
    /// No reply, `outcome_unknown`, or a refusal that says nothing about the identity.
    Unknown,
}

/// One submission attempt, in virtual milliseconds.
#[derive(Clone, Debug)]
pub(crate) struct Submission {
    pub body: Body,
    pub route: Route,
    /// The first attempt under this identity.
    pub fresh: bool,
    pub invoked: u64,
    pub completed: u64,
    pub answer: Answer,
}

/// One receipt lookup of `(run, id)`.
#[derive(Clone, Debug)]
pub(crate) struct Lookup {
    pub run: Uuid,
    pub id: Uuid,
    pub invoked: u64,
    pub completed: u64,
    pub seen: Seen,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Seen {
    /// `queued`, `running`, `committing` or `reconciling`.
    Pending,
    Terminal(Terminal),
    Abandoned,
    Missing,
    /// No reply, or an error that is not about the receipt.
    Unknown,
}

/// A linearizable observation of one key: its value, or `None` when absent.
#[derive(Clone, Debug)]
pub(crate) struct Read {
    pub key: i64,
    pub value: Option<i64>,
    pub invoked: u64,
    pub completed: u64,
}

/// A count of retained request records on one worker, from its public stats.
#[derive(Clone, Debug)]
pub(crate) struct Retained {
    pub node: u64,
    /// The accepted identities that the worker must hold then.
    pub required: usize,
    pub accepted_records: u64,
    pub at: u64,
}

/// The queue counters of one worker right after a cold start, before any new submission.
#[derive(Clone, Debug)]
pub(crate) struct Fresh {
    pub node: u64,
    pub run: Uuid,
    pub pending_records: u64,
    pub at: u64,
}

/// A run and when its complete stop began, if it was stopped.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Run {
    pub id: Uuid,
    pub stopped: Option<u64>,
}

/// Everything the rules read.
#[derive(Clone, Debug, Default)]
pub(crate) struct Log {
    pub submissions: Vec<Submission>,
    pub lookups: Vec<Lookup>,
    pub reads: Vec<Read>,
    pub retained: Vec<Retained>,
    pub fresh_runs: Vec<Fresh>,
    pub runs: Vec<Run>,
    /// Identities looked up that no client ever submitted.
    pub never_submitted: BTreeSet<Uuid>,
    /// When two of three workers were frozen, in a cluster that had reported three ready
    /// workers just before. No acceptance can finish its replication inside.
    pub two_frozen: Vec<(u64, u64)>,
    /// Intervals in which two or more worker processes were stopped, per run. A request copy
    /// may be gone, so retention is not judged from their start on.
    pub two_down: Vec<u64>,
    pub retention_ms: u64,
    /// How far the node clocks that measure retention may have jumped.
    pub clock_slack_ms: u64,
}

/// A rule's verdict over a log.
#[derive(Clone, Debug, Default)]
pub(crate) struct Outcome {
    pub violations: Vec<(&'static str, String)>,
    /// How often each rule judged something. A rule that is absent did not run.
    pub judged: BTreeMap<&'static str, u64>,
}

impl Outcome {
    fn judged(&mut self, rule: &'static str) {
        *self.judged.entry(rule).or_default() += 1;
    }

    fn violate(&mut self, rule: &'static str, detail: String) {
        self.violations.push((rule, detail));
    }

    /// The kinds reported, for tests.
    #[cfg(test)]
    pub(crate) fn kinds(&self) -> BTreeSet<&'static str> {
        self.violations.iter().map(|(kind, _)| *kind).collect()
    }
}

impl Log {
    pub(crate) fn judge(&self) -> Outcome {
        let mut out = Outcome::default();
        self.accepted_settles(&mut out);
        self.retention(&mut out);
        self.one_execution(&mut out);
        self.receipts(&mut out);
        out
    }

    /// Accepted submissions, first acceptance per `(run, id)`.
    pub(crate) fn accepted(&self) -> BTreeMap<(Uuid, Uuid), &Submission> {
        let mut accepted: BTreeMap<(Uuid, Uuid), &Submission> = BTreeMap::new();
        for submission in &self.submissions {
            let Answer::Accepted(run) = submission.answer else {
                continue;
            };
            let first = accepted
                .entry((run, submission.body.id))
                .or_insert(submission);
            if submission.completed < first.completed {
                *first = submission;
            }
        }
        accepted
    }

    fn stopped(&self, run: Uuid) -> u64 {
        self.runs
            .iter()
            .find(|known| known.id == run)
            .and_then(|known| known.stopped)
            .unwrap_or(u64::MAX)
    }

    /// Whether a lookup is judged: it ended before its run's full stop, and before two
    /// workers were down after the acceptance.
    fn in_run(&self, accepted: u64, lookup: &Lookup) -> bool {
        lookup.completed < self.stopped(lookup.run)
            && !self
                .two_down
                .iter()
                .any(|&at| at >= accepted && at <= lookup.completed)
    }

    fn accepted_settles(&self, out: &mut Outcome) {
        for ((run, id), acceptance) in self.accepted() {
            for lookup in self.lookups.iter().filter(|l| l.run == run && l.id == id) {
                if lookup.invoked < acceptance.completed
                    || !self.in_run(acceptance.completed, lookup)
                {
                    continue;
                }
                match lookup.seen {
                    Seen::Unknown => continue,
                    Seen::Pending | Seen::Terminal(_) => {}
                    Seen::Abandoned => out.violate(
                        ACCEPTED_SETTLES,
                        format!(
                            "{id} accepted at {}ms was reported abandoned at {}ms in its own run {run}, with no full stop",
                            acceptance.completed, lookup.completed
                        ),
                    ),
                    Seen::Missing => {
                        // The result cannot have expired before the request was last seen
                        // pending, or was accepted, plus the retention.
                        let pending = self
                            .lookups
                            .iter()
                            .filter(|l| {
                                l.run == run && l.id == id && l.seen == Seen::Pending
                            })
                            .map(|l| l.invoked)
                            .max()
                            .unwrap_or(0)
                            .max(acceptance.invoked);
                        let expires = (pending + self.retention_ms)
                            .saturating_sub(self.clock_slack_ms);
                        if lookup.completed < expires {
                            out.violate(
                                ACCEPTED_SETTLES,
                                format!(
                                    "{id} accepted at {}ms in run {run} was missing at {}ms, before its result could expire at {expires}ms",
                                    acceptance.completed, lookup.completed
                                ),
                            );
                        }
                    }
                }
                out.judged(ACCEPTED_SETTLES);
            }
        }
    }

    fn retention(&self, out: &mut Outcome) {
        // Q03: no acknowledgment before the required replication.
        for submission in &self.submissions {
            let inside = self
                .two_frozen
                .iter()
                .any(|&(from, to)| submission.invoked >= from && submission.completed <= to);
            if !submission.fresh || submission.route != Route::Queued || !inside {
                continue;
            }
            out.judged(RETENTION);
            let acknowledged = match &submission.answer {
                Answer::Accepted(_) => true,
                Answer::Terminal(Terminal::Succeeded(_)) => submission.body.mutation,
                _ => false,
            };
            if acknowledged {
                out.violate(
                    RETENTION,
                    format!(
                        "{} sent at {}ms was acknowledged at {}ms with two of three workers frozen",
                        submission.body.id, submission.invoked, submission.completed
                    ),
                );
            }
        }
        // R02: outstanding requests remain on the holders.
        for held in &self.retained {
            out.judged(RETENTION);
            if held.accepted_records < held.required as u64 {
                out.violate(
                    RETENTION,
                    format!(
                        "n{} holds {} accepted records at {}ms, {} accepted requests are outstanding",
                        held.node, held.accepted_records, held.at, held.required
                    ),
                );
            }
        }
        // R04: the new run has no pending old work.
        for fresh in &self.fresh_runs {
            out.judged(RETENTION);
            if fresh.pending_records > 0 {
                out.violate(
                    RETENTION,
                    format!(
                        "n{} starts run {} with {} pending queue records at {}ms",
                        fresh.node, fresh.run, fresh.pending_records, fresh.at
                    ),
                );
            }
        }
        // R04: an abandoned receipt means that no durable decision proves success.
        let accepted = self.accepted();
        for lookup in &self.lookups {
            if lookup.seen != Seen::Abandoned {
                continue;
            }
            let Some(acceptance) = accepted.get(&(lookup.run, lookup.id)) else {
                continue;
            };
            let body = &acceptance.body;
            if !body.mutation {
                continue;
            }
            out.judged(RETENTION);
            let visible = self.reads.iter().find(|read| {
                body.writes
                    .iter()
                    .any(|&(key, value)| read.key == key && read.value == Some(value))
            });
            if let Some(read) = visible {
                out.violate(
                    RETENTION,
                    format!(
                        "{} was reported abandoned at {}ms, yet key {} held its value at {}ms",
                        lookup.id, lookup.completed, read.key, read.completed
                    ),
                );
            }
            let succeeded = self.submissions.iter().find(|s| {
                s.body == *body && matches!(s.answer, Answer::Terminal(Terminal::Succeeded(_)))
            });
            if let Some(other) = succeeded {
                out.violate(
                    RETENTION,
                    format!(
                        "{} was reported abandoned at {}ms and succeeded at {}ms",
                        lookup.id, lookup.completed, other.completed
                    ),
                );
            }
        }
    }

    /// Every definite success of a mutation body: the result and where it was seen.
    fn successes(&self) -> Vec<(&Body, Route, &Value, u64)> {
        let accepted = self.accepted();
        let mut found: Vec<(&Body, Route, &Value, u64)> = Vec::new();
        for submission in &self.submissions {
            if let Answer::Terminal(Terminal::Succeeded(result)) = &submission.answer {
                found.push((
                    &submission.body,
                    submission.route,
                    result,
                    submission.completed,
                ));
            }
        }
        for lookup in &self.lookups {
            if let Seen::Terminal(Terminal::Succeeded(result)) = &lookup.seen
                && let Some(acceptance) = accepted.get(&(lookup.run, lookup.id))
            {
                found.push((&acceptance.body, Route::Queued, result, lookup.completed));
            }
        }
        found.retain(|(body, ..)| body.mutation);
        found
    }

    fn one_execution(&self, out: &mut Outcome) {
        let successes = self.successes();
        let mut by_id: BTreeMap<Uuid, Vec<(&Body, Route, &Value, u64)>> = BTreeMap::new();
        for success in successes {
            by_id.entry(success.0.id).or_default().push(success);
        }
        for (id, members) in &by_id {
            out.judged(ONE_EXECUTION);
            for (index, (body, route, result, at)) in members.iter().enumerate() {
                for (other, other_route, other_result, other_at) in &members[..index] {
                    if body == other {
                        // A result that changed no row is not recorded on the direct path, so
                        // only a committed result has to stay the same across paths.
                        let fixed = *route == Route::Queued && *other_route == Route::Queued
                            || (changed(result) && changed(other_result));
                        if fixed && result != other_result {
                            out.violate(
                                ONE_EXECUTION,
                                format!("{id} returned {other_result} at {other_at}ms and {result} at {at}ms for one body"),
                            );
                        }
                        continue;
                    }
                    // Cluster-wide on the queued path, per table and key otherwise. A result
                    // that changed no row leaves no record on the direct path.
                    let cluster_wide = *route == Route::Queued && *other_route == Route::Queued;
                    let same_key = body.anchor.is_some() && body.anchor == other.anchor;
                    if cluster_wide || (same_key && changed(result) && changed(other_result)) {
                        out.violate(
                            ONE_EXECUTION,
                            format!("{id} succeeded with two bodies, at {other_at}ms and {at}ms"),
                        );
                    }
                }
            }
        }
        // A body that succeeded is never answered as a conflict with another body.
        for submission in &self.submissions {
            let body = &submission.body;
            let refused =
                submission.answer == Answer::Terminal(Terminal::Failed("conflict".into()));
            if !refused || !body.mutation {
                continue;
            }
            let succeeded = by_id.get(&body.id).and_then(|members| {
                members.iter().find(|(other, route, result, _)| {
                    *other == body
                        && (changed(result)
                            || (*route == Route::Queued && submission.route == Route::Queued))
                })
            });
            if let Some((_, _, result, at)) = succeeded {
                out.violate(
                    ONE_EXECUTION,
                    format!(
                        "{} returned {result} at {at}ms and a conflict at {}ms for one body",
                        body.id, submission.completed
                    ),
                );
            }
        }
        self.reappearance(out);
    }

    /// Each value is written by one body under one identity, so a value that a later read
    /// found replaced can only come back if that identity executed again.
    fn reappearance(&self, out: &mut Outcome) {
        let mut writers: BTreeMap<(i64, i64), Uuid> = BTreeMap::new();
        for submission in &self.submissions {
            for &write in &submission.body.writes {
                writers.insert(write, submission.body.id);
            }
        }
        let mut by_key: BTreeMap<i64, Vec<&Read>> = BTreeMap::new();
        for read in &self.reads {
            by_key.entry(read.key).or_default().push(read);
        }
        for (key, mut reads) in by_key {
            reads.sort_by_key(|read| (read.completed, read.invoked));
            for (position, first) in reads.iter().enumerate() {
                let Some(value) = first.value else {
                    continue;
                };
                let Some(id) = writers.get(&(key, value)) else {
                    continue;
                };
                out.judged(ONE_EXECUTION);
                let replaced = reads[position + 1..]
                    .iter()
                    .find(|read| read.invoked > first.completed && read.value != first.value);
                let Some(replaced) = replaced else {
                    continue;
                };
                let back = reads
                    .iter()
                    .find(|read| read.invoked > replaced.completed && read.value == first.value);
                if let Some(back) = back {
                    out.violate(
                        ONE_EXECUTION,
                        format!(
                            "{id} wrote {value} to key {key}: read at {}ms, replaced by {:?} at {}ms, and back at {}ms",
                            first.completed, replaced.value, replaced.completed, back.completed
                        ),
                    );
                    break;
                }
            }
        }
    }

    fn receipts(&self, out: &mut Outcome) {
        let mut terminal: BTreeMap<(Uuid, Uuid), (&Terminal, u64)> = BTreeMap::new();
        for lookup in &self.lookups {
            if self.never_submitted.contains(&lookup.id) {
                if lookup.seen == Seen::Unknown {
                    continue;
                }
                out.judged(RECEIPTS);
                if matches!(lookup.seen, Seen::Pending | Seen::Terminal(_)) {
                    out.violate(
                        RECEIPTS,
                        format!(
                            "{} was never submitted, and its receipt answered {:?} at {}ms",
                            lookup.id, lookup.seen, lookup.completed
                        ),
                    );
                }
                continue;
            }
            let Seen::Terminal(seen) = &lookup.seen else {
                continue;
            };
            out.judged(RECEIPTS);
            let (first, at) = *terminal
                .entry((lookup.run, lookup.id))
                .or_insert((seen, lookup.completed));
            if first != seen {
                out.violate(
                    RECEIPTS,
                    format!(
                        "{} in run {} was {first:?} at {at}ms and {seen:?} at {}ms",
                        lookup.id, lookup.run, lookup.completed
                    ),
                );
            }
        }
    }

    /// Accepted `(run, id)` pairs of `run` that no lookup ever found terminal, expired or
    /// abandoned.
    pub(crate) fn unsettled(&self, run: Uuid) -> Vec<Uuid> {
        self.accepted()
            .into_keys()
            .filter(|(accepted, _)| *accepted == run)
            .filter(|(run, id)| {
                !self.lookups.iter().any(|lookup| {
                    lookup.run == *run
                        && lookup.id == *id
                        && matches!(
                            lookup.seen,
                            Seen::Terminal(_) | Seen::Missing | Seen::Abandoned
                        )
                })
            })
            .map(|(_, id)| id)
            .collect()
    }

    /// Whether an accepted request was sent before `at`, was not seen terminal by then, and was
    /// seen terminal afterwards.
    pub(crate) fn open_across(&self, at: u64) -> bool {
        self.accepted().iter().any(|((run, id), submission)| {
            let first = self
                .lookups
                .iter()
                .filter(|lookup| {
                    lookup.run == *run
                        && lookup.id == *id
                        && matches!(lookup.seen, Seen::Terminal(_))
                })
                .map(|lookup| lookup.completed)
                .min();
            submission.invoked <= at && first.is_some_and(|first| first > at)
        })
    }

    /// Lookups that found a request pending after a terminal answer. The contract does not
    /// forbid it at different observation points, so it is reported, not judged.
    pub(crate) fn regressions(&self) -> usize {
        self.lookups
            .iter()
            .filter(|late| {
                late.seen == Seen::Pending
                    && self.lookups.iter().any(|early| {
                        early.run == late.run
                            && early.id == late.id
                            && early.completed < late.invoked
                            && matches!(early.seen, Seen::Terminal(_))
                    })
            })
            .count()
    }
}

/// Whether a success changed rows. `update` and `delete` answer the existence flag.
fn changed(result: &Value) -> bool {
    *result != Value::Bool(false)
}

#[cfg(test)]
#[path = "queued_tests.rs"]
mod tests;
