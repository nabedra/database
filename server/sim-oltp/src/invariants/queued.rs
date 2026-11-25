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
///
/// Contract, Q01: "Every accepted ID remains tracked and eventually terminates after barriers
/// release; zero missing or duplicate logical outcomes." A receipt that failed with an unknown
/// outcome is terminal and names no outcome. For an accepted mutation that is a missing one,
/// unless the same identity is answered definitely later.
pub(crate) const ACCEPTED_SETTLES: &str = "AcceptedSettlesInRun";

/// Guarantees, OLTP-G07: "Accepted queued work stays recoverable while a complete copy
/// survives. A full worker stop abandons outstanding work." Contract, Q03: "No acceptance
/// acknowledgment precedes required replication". Contract, R02: "Outstanding requests remain
/// on that holder". Contract, R04: "New run has no pending old work; old receipts report
/// abandonment unless a durable commit decision proves success". Plan: "replicate complete
/// request bytes, identity/digest, acceptance order, and recoverable state to all ready workers
/// before acknowledging acceptance or starting execution."
///
/// The legacy protocol copies to all ready workers. The scoped one needs two copies, and no
/// public counter shows them, so its copies are judged only by what a full stop abandons.
pub(crate) const RETENTION: &str = "QueueRetention";

/// Contract, Q05: "One logical execution/receipt for identical identities, explicit conflict
/// for a different body; one mutation decision." Contract: "no logical mutation may publish
/// twice". Contract, Q06: "never silently execute a new logical mutation". Guarantees,
/// OLTP-G03: "Each request identity executes once, cluster-wide on the queued path and per
/// table and key on the direct path." The sentence after it is [`FIRST_RESULT`].
pub(crate) const ONE_EXECUTION: &str = "OneExecutionPerIdentity";

/// Guarantees, OLTP-G03: "On the direct path, a retry of a mutation with the same request ID and
/// body returns the first definite result of that identity, including a no-op or row conflict,
/// while its dedup record is retained." A record is kept 24 h, longer than any run.
///
/// Both answers must be ones that the direct path may have given. An answer that the queue
/// certainly gave is outside the sentence. `plans/oltp-direct-path.md` says that the product does
/// not do this today (DR-1), so a red here is that finding until something else is shown.
pub(crate) const FIRST_RESULT: &str = "DirectRetryResult";

/// Contract: "No request may have conflicting terminal decisions". Plan: "an unknown or
/// expired receipt must never be fabricated as successful".
pub(crate) const RECEIPTS: &str = "ReceiptCheck";

/// Every rule this module judges from public observations.
pub(crate) const RULES: [&str; 5] = [
    ACCEPTED_SETTLES,
    RETENTION,
    ONE_EXECUTION,
    FIRST_RESULT,
    RECEIPTS,
];

/// What cannot run yet, and why. The sweep records each as `not_run`.
pub(crate) const NOT_RUN: [(&str, &str); 6] = [
    (
        "QueueRetention over scoped copies",
        "/v1/runtime/stats counts the legacy queue only, so no public counter shows where a scoped record is held",
    ),
    (
        "DirectRetryResult across paths",
        "left out: OLTP-G03 gives the first definite result to a retry on the direct path, and no sentence covers a no-op or row conflict on one path that the other path answers again",
    ),
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

/// A terminal state: the success data, or the error code and whether the error leaves the
/// outcome unknown.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Terminal {
    Succeeded(Value),
    Failed { code: String, unknown: bool },
}

impl Terminal {
    pub(crate) fn failed(code: &str) -> Self {
        Self::Failed {
            code: code.into(),
            unknown: false,
        }
    }

    /// Whether it says what happened to the request.
    pub(crate) fn decided(&self) -> bool {
        !matches!(self, Self::Failed { unknown: true, .. })
    }

    pub(crate) fn conflict(&self) -> bool {
        *self == Self::failed("conflict")
    }
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
    /// Any other definite refusal (`not_applied`). It says nothing about the identity.
    Refused(String),
    /// No reply, or `outcome_unknown`.
    Unknown,
}

impl Answer {
    /// The request was accepted, or it ran.
    pub(crate) fn executed(&self) -> bool {
        matches!(
            self,
            Self::Accepted(_) | Self::Terminal(Terminal::Succeeded(_))
        )
    }

    pub(crate) fn conflict(&self) -> bool {
        matches!(self, Self::Terminal(terminal) if terminal.conflict())
    }
}

/// One submission attempt, in virtual milliseconds.
#[derive(Clone, Debug)]
pub(crate) struct Submission {
    pub body: Body,
    pub route: Route,
    /// Sent to `POST /v1/requests`.
    pub asynchronous: bool,
    pub gateway: u64,
    /// The client closed its socket on purpose before a reply could come.
    pub closed: bool,
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
    pub gateway: u64,
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

/// Who a record count was taken from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Holder {
    /// A ready worker of a cluster that nothing disturbs.
    Ready,
    /// The only worker that still runs.
    Last,
}

/// The request records of one worker before and after some requests were accepted, from its
/// public stats. Nothing may leave the queue between the two counts, so the difference counts
/// the new requests alone: either the first count is zero, or no result can have expired yet.
///
/// What it cannot see: while another worker still holds an old record, this one can be given a
/// copy of it between the counts, and each such record hides one lost request. So the clauses
/// need a baseline of all three workers.
#[derive(Clone, Debug)]
pub(crate) struct Retained {
    pub node: u64,
    pub holder: Holder,
    /// How many workers the baseline could compare.
    pub compared: usize,
    /// The identities accepted between the counts, which the worker must hold.
    pub required: usize,
    pub before: u64,
    pub after: u64,
    /// Records of `after` that are not terminal.
    pub pending: u64,
    pub at: u64,
}

/// A window in which one or two of three workers were frozen.
#[derive(Clone, Debug)]
pub(crate) struct Frozen {
    pub nodes: Vec<u64>,
    pub from: u64,
    pub to: u64,
    /// The ready workers that a running worker reported from Control, asked at `to`. A frozen
    /// worker cannot join, so three then means that it was ready throughout.
    pub ready: Option<u64>,
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
    /// Windows with frozen workers, in a cluster that had reported three ready workers just
    /// before.
    pub frozen: Vec<Frozen>,
    /// The scoped protocol: two of three copies are enough.
    pub scoped: bool,
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
        self.first_result(&mut out);
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
                        // pending, or was first sent, plus the retention. An earlier attempt
                        // with an unknown answer may have been the one that was accepted.
                        let sent = self
                            .submissions
                            .iter()
                            .filter(|submission| submission.body.id == id)
                            .map(|submission| submission.invoked)
                            .min()
                            .unwrap_or(acceptance.invoked);
                        let pending = self
                            .lookups
                            .iter()
                            .filter(|l| {
                                l.run == run && l.id == id && l.seen == Seen::Pending
                            })
                            .map(|l| l.invoked)
                            .max()
                            .unwrap_or(0)
                            .max(sent);
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
            self.missing_outcome(run, id, acceptance, out);
        }
    }

    /// An accepted mutation whose receipt is terminal without an outcome, and which nothing
    /// answers definitely later.
    fn missing_outcome(&self, run: Uuid, id: Uuid, acceptance: &Submission, out: &mut Outcome) {
        if !acceptance.body.mutation {
            return;
        }
        let Some(failed) = self.lookups.iter().find(|lookup| {
            lookup.run == run
                && lookup.id == id
                && matches!(&lookup.seen, Seen::Terminal(terminal) if !terminal.decided())
                && self.in_run(acceptance.completed, lookup)
        }) else {
            return;
        };
        let decided = |terminal: &Terminal, at: u64| at > failed.completed && terminal.decided();
        let later = self.lookups.iter().any(|lookup| {
            lookup.run == run
                && lookup.id == id
                && matches!(&lookup.seen, Seen::Terminal(terminal) if decided(terminal, lookup.completed))
        }) || self.submissions.iter().any(|submission| {
            submission.body == acceptance.body
                && matches!(&submission.answer, Answer::Terminal(terminal) if decided(terminal, submission.completed))
        });
        if !later {
            out.violate(
                ACCEPTED_SETTLES,
                format!(
                    "{id} accepted at {}ms in run {run} has the receipt {:?} at {}ms, terminal with an unknown outcome",
                    acceptance.completed, failed.seen, failed.completed
                ),
            );
        }
    }

    /// The frozen window that judges a submission, if any.
    pub(crate) fn frozen_window(&self, submission: &Submission) -> Option<&Frozen> {
        if !submission.fresh || submission.route != Route::Queued {
            return None;
        }
        self.frozen.iter().find(|window| {
            let judged = match window.nodes.len() {
                // Every ready worker holds a copy before the acknowledgment, and this one
                // could not install any while it was ready.
                1 => !self.scoped && window.ready == Some(3),
                // Neither protocol can copy to one worker alone.
                _ => true,
            };
            judged && submission.invoked >= window.from && submission.invoked < window.to
        })
    }

    fn retention(&self, out: &mut Outcome) {
        // Q03: no acknowledgment before the required replication.
        for submission in &self.submissions {
            let Some(window) = self.frozen_window(submission) else {
                continue;
            };
            out.judged(RETENTION);
            if submission.completed > window.to {
                continue;
            }
            let acknowledged = match &submission.answer {
                Answer::Accepted(_) => true,
                Answer::Terminal(Terminal::Succeeded(_)) => submission.body.mutation,
                _ => false,
            };
            if acknowledged {
                out.violate(
                    RETENTION,
                    format!(
                        "{} sent at {}ms was acknowledged at {}ms, while {:?} of three ready workers could not install a copy",
                        submission.body.id, submission.invoked, submission.completed, window.nodes
                    ),
                );
            }
        }
        // R02: outstanding requests remain on the holders.
        for held in &self.retained {
            if held.required == 0 {
                continue;
            }
            out.judged(RETENTION);
            let new = held.after.saturating_sub(held.before);
            if new < held.required as u64 {
                out.violate(
                    RETENTION,
                    format!(
                        "n{} went from {} to {} accepted records by {}ms, and {} requests were accepted meanwhile",
                        held.node, held.before, held.after, held.at, held.required
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
            // A success that changed rows was a durable commit decision, wherever it was seen.
            let answered = self.submissions.iter().find_map(|s| match &s.answer {
                Answer::Terminal(Terminal::Succeeded(result))
                    if s.body == *body && changed(result) =>
                {
                    Some(s.completed)
                }
                _ => None,
            });
            let receipt = self.lookups.iter().find_map(|other| match &other.seen {
                Seen::Terminal(Terminal::Succeeded(result))
                    if other.run == lookup.run && other.id == lookup.id && changed(result) =>
                {
                    Some(other.completed)
                }
                _ => None,
            });
            if let Some(at) = answered.or(receipt) {
                out.violate(
                    RETENTION,
                    format!(
                        "{} was reported abandoned at {}ms and succeeded at {at}ms",
                        lookup.id, lookup.completed
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
            if members.len() > 1 {
                out.judged(ONE_EXECUTION);
            }
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
            if !submission.answer.conflict() || !body.mutation {
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
        self.two_bodies(out);
        self.reappearance(out);
    }

    /// The bodies of one identity that were accepted or took effect, beyond the pairs of
    /// definite successes: a `202` is the acceptance of that body, and a value that only one
    /// body writes is its effect.
    fn two_bodies(&self, out: &mut Outcome) {
        // Whether every attempt was queued, and the first acceptance.
        type Sent<'a> = BTreeMap<&'a Body, (bool, Option<(Uuid, u64)>)>;
        let mut bodies: BTreeMap<Uuid, Sent<'_>> = BTreeMap::new();
        for submission in self.submissions.iter().filter(|s| s.body.mutation) {
            let known = bodies
                .entry(submission.body.id)
                .or_default()
                .entry(&submission.body)
                .or_insert((true, None));
            known.0 &= submission.route == Route::Queued;
            if let Answer::Accepted(run) = submission.answer {
                known.1.get_or_insert((run, submission.completed));
            }
        }
        let visible = |body: &Body| {
            self.reads.iter().find_map(|read| {
                let write = (read.key, read.value?);
                body.writes.contains(&write).then_some(read.completed)
            })
        };
        for (id, bodies) in bodies.iter().filter(|(_, bodies)| bodies.len() > 1) {
            let known: Vec<_> = bodies
                .iter()
                .map(|(body, (queued, accepted))| (*body, *queued, *accepted, visible(body)))
                .collect();
            if known
                .iter()
                .any(|(.., accepted, seen)| accepted.is_some() || seen.is_some())
            {
                out.judged(ONE_EXECUTION);
            }
            for (index, (body, queued, accepted, seen)) in known.iter().enumerate() {
                for (other, other_queued, other_accepted, other_seen) in &known[..index] {
                    // Two acceptances of one run are on the queued path, which is cluster-wide.
                    if let (Some((run, at)), Some((other_run, other_at))) =
                        (accepted, other_accepted)
                        && run == other_run
                    {
                        out.violate(
                            ONE_EXECUTION,
                            format!("{id} was accepted with two bodies in run {run}, at {other_at}ms and {at}ms"),
                        );
                        continue;
                    }
                    let same_key = body.anchor.is_some() && body.anchor == other.anchor;
                    let cluster_wide = *queued && *other_queued;
                    if let (Some(at), Some(other_at)) = (seen, other_seen)
                        && (cluster_wide || same_key)
                    {
                        out.violate(
                            ONE_EXECUTION,
                            format!(
                                "{id} took effect with two bodies, read at {other_at}ms and {at}ms"
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Definite results that the direct path may have given one body.
    fn first_result(&self, out: &mut Outcome) {
        let mut results: BTreeMap<&Body, Vec<(&Terminal, u64)>> = BTreeMap::new();
        for submission in &self.submissions {
            if !submission.body.mutation || submission.route != Route::MaybeDirect {
                continue;
            }
            if let Answer::Terminal(terminal) = &submission.answer {
                results
                    .entry(&submission.body)
                    .or_default()
                    .push((terminal, submission.completed));
            }
        }
        for (body, mut results) in results.into_iter().filter(|(_, r)| r.len() > 1) {
            out.judged(FIRST_RESULT);
            results.sort_by_key(|(_, at)| *at);
            let (first, at) = results[0];
            // A committed result and another answer is `OneExecutionPerIdentity`.
            let committed = |terminal: &Terminal| matches!(terminal, Terminal::Succeeded(result) if changed(result));
            let reported = |other: &Terminal| {
                committed(first) && (committed(other) || other.conflict())
                    || committed(other) && first.conflict()
            };
            if let Some((other, other_at)) = results
                .iter()
                .find(|(other, _)| *other != first && !reported(other))
            {
                out.violate(
                    FIRST_RESULT,
                    format!(
                        "{} answered {first:?} at {at}ms and {other:?} at {other_at}ms for one body",
                        body.id
                    ),
                );
            }
        }
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
                let replaced = reads[position + 1..]
                    .iter()
                    .find(|read| read.invoked > first.completed && read.value != first.value);
                let Some(replaced) = replaced else {
                    continue;
                };
                out.judged(ONE_EXECUTION);
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
    /// abandoned. A mutation that failed with an unknown outcome has not settled.
    pub(crate) fn unsettled(&self, run: Uuid) -> Vec<Uuid> {
        self.accepted()
            .into_iter()
            .filter(|((accepted, _), _)| *accepted == run)
            .filter(|((run, id), submission)| {
                !self.lookups.iter().any(|lookup| {
                    lookup.run == *run
                        && lookup.id == *id
                        && match &lookup.seen {
                            Seen::Terminal(terminal) => {
                                terminal.decided() || !submission.body.mutation
                            }
                            Seen::Missing | Seen::Abandoned => true,
                            Seen::Pending | Seen::Unknown => false,
                        }
                })
            })
            .map(|((_, id), _)| id)
            .collect()
    }

    /// Accepted requests of `run` with a receipt that is terminal and names no outcome.
    pub(crate) fn without_outcome(&self, run: Uuid) -> BTreeSet<Uuid> {
        self.lookups
            .iter()
            .filter(|lookup| lookup.run == run)
            .filter(|lookup| matches!(&lookup.seen, Seen::Terminal(t) if !t.decided()))
            .map(|lookup| lookup.id)
            .collect()
    }

    /// An accepted request that was open across `at`: accepted before it, reported pending by a
    /// lookup sent at or after it, and terminal after that, in one run. A request that nobody
    /// looked up meanwhile may have settled long before, and does not count.
    pub(crate) fn open_across(&self, at: u64) -> Option<(Uuid, Uuid)> {
        self.accepted()
            .into_iter()
            .find_map(|((run, id), accepted)| {
                if accepted.completed > at {
                    return None;
                }
                let mine = || self.lookups.iter().filter(|l| l.run == run && l.id == id);
                let pending = mine()
                    .filter(|l| l.invoked >= at && l.seen == Seen::Pending)
                    .map(|l| l.completed)
                    .min()?;
                mine()
                    .any(|l| {
                        l.completed > pending
                            && matches!(&l.seen, Seen::Terminal(terminal) if terminal.decided())
                    })
                    .then_some((run, id))
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

/// How far `jumps` wall-clock jumps may have moved a node clock: each by at most 2 s. Drift adds
/// well under a second.
pub(crate) fn clock_slack(jumps: u64) -> u64 {
    2_000 * (jumps + 1) + 1_000
}

/// Whether a success changed rows. `update` and `delete` answer the existence flag.
pub(crate) fn changed(result: &Value) -> bool {
    *result != Value::Bool(false)
}

#[cfg(test)]
#[path = "queued_tests.rs"]
mod tests;
