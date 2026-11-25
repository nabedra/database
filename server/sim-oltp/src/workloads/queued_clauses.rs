//! Which scenario clauses a `queued` run executed, and how much work it did.
//!
//! A clause is named only when its assertion ran: the fault met a request that was really open,
//! or the rule that judges the clause had something to compare. A fault that met nothing names
//! nothing.

use std::collections::{BTreeMap, BTreeSet};

use uuid::Uuid;

use crate::{
    bootstrap::WORKERS,
    invariants::queued::{
        Answer, Body, Holder, Log, Lookup, Route, Seen, Submission, Terminal, changed,
    },
};

/// A submission into a frozen window counts when it waited at least this long in it.
const HELD_MS: u64 = 100;

/// A fault of the queued workload that happened, and when it took effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The first API stopped, and both were restarted.
    ApiRestart,
    WorkerKilled,
    /// A gateway, or a worker, stopped with submissions in flight.
    CoordinatorKilled {
        gateway: Option<u64>,
    },
    /// Every worker stopped once, and the last one was ready at `end`.
    Rolling {
        end: u64,
    },
    /// A link fault of the shared nemesis.
    Partition,
    /// A lost reply of the shared nemesis, open until `to`.
    LostReply {
        to: u64,
    },
}

/// A complete stop of the workload's own finale, after which every step of R04 was taken.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stop {
    pub run: Uuid,
    pub at: u64,
    /// The rows were read again in the new run.
    pub reread: bool,
}

/// What [`clauses`] reads besides the log.
pub(crate) struct Facts<'a> {
    pub log: &'a Log,
    /// The run's `direct_oltp`.
    pub direct: bool,
    pub marks: &'a [(Mark, u64)],
    pub stop: Option<Stop>,
}

fn decided(seen: &Seen) -> bool {
    matches!(seen, Seen::Terminal(terminal) if terminal.decided())
}

impl Facts<'_> {
    fn lookups(&self, run: Uuid, id: Uuid) -> impl Iterator<Item = &Lookup> {
        self.log
            .lookups
            .iter()
            .filter(move |lookup| lookup.run == run && lookup.id == id)
    }

    /// When a body first got a definite result after `at`: an answer, or a receipt of an
    /// acceptance.
    fn resolved(&self, body: &Body, at: u64) -> Option<u64> {
        let answered = self
            .log
            .submissions
            .iter()
            .filter(|s| s.body == *body && s.completed > at)
            .filter(|s| matches!(&s.answer, Answer::Terminal(terminal) if terminal.decided()))
            .map(|s| s.completed)
            .min();
        let received = self
            .log
            .submissions
            .iter()
            .filter(|s| s.body == *body)
            .filter_map(|s| match s.answer {
                Answer::Accepted(run) => Some(run),
                _ => None,
            })
            .flat_map(|run| self.lookups(run, body.id))
            .filter(|lookup| lookup.completed > at && decided(&lookup.seen))
            .map(|lookup| lookup.completed)
            .min();
        answered.into_iter().chain(received).min()
    }

    pub(crate) fn clauses(&self) -> BTreeSet<&'static str> {
        let mut out = BTreeSet::new();
        for &(mark, at) in self.marks {
            self.mark(mark, at, &mut out);
        }
        self.frozen(&mut out);
        self.holders(&mut out);
        self.identities(&mut out);
        if self.expired() {
            out.insert("Q06:expired-receipt");
        }
        self.stopped(&mut out);
        out
    }

    fn mark(&self, mark: Mark, at: u64, out: &mut BTreeSet<&'static str>) {
        let open = self.log.open_across(at).is_some();
        match mark {
            Mark::ApiRestart if open => {
                out.insert("Q04:api-restart");
            }
            Mark::WorkerKilled if open => {
                out.insert("R01:worker-killed");
            }
            Mark::CoordinatorKilled { gateway } => {
                // An accepted request, or a submission that the killed gateway never answered
                // and that was resolved afterwards.
                let unanswered = || {
                    self.log.submissions.iter().any(|s| {
                        Some(s.gateway) == gateway
                            && s.route == Route::Queued
                            && s.invoked < at
                            && s.completed > at
                            && s.answer == Answer::Unknown
                            && self.resolved(&s.body, s.completed).is_some()
                    })
                };
                if open || unanswered() {
                    out.insert("Q03:coordinator-killed");
                }
            }
            // R03 and O14: the run and the receipt outlived every worker that held it first.
            Mark::Rolling { end } if open => {
                let kept = self.log.accepted().into_iter().any(|((run, id), s)| {
                    s.completed <= at
                        && self.lookups(run, id).any(|lookup| {
                            lookup.invoked >= end
                                && (lookup.seen == Seen::Pending || decided(&lookup.seen))
                        })
                });
                if kept {
                    out.extend(["R03:rolling-restart", "O14:rolling-restart"]);
                }
            }
            Mark::Partition if open => {
                out.insert("R05:partition");
            }
            Mark::LostReply { to } => {
                let cut = |cut: bool, invoked: u64, completed: u64| {
                    cut && invoked <= to && completed >= at
                };
                // The lost answer was certainly terminal, or the mutation had to be reconciled.
                let receipt = self.log.lookups.iter().any(|lost| {
                    cut(lost.cut, lost.invoked, lost.completed)
                        && self
                            .lookups(lost.run, lost.id)
                            .any(|l| l.completed <= lost.invoked && decided(&l.seen))
                        && self
                            .lookups(lost.run, lost.id)
                            .any(|l| l.invoked >= lost.completed && decided(&l.seen))
                });
                let mutation = || {
                    self.log.submissions.iter().any(|lost| {
                        lost.body.mutation
                            && cut(lost.cut, lost.invoked, lost.completed)
                            && self.resolved(&lost.body, lost.completed).is_some()
                    })
                };
                if receipt || mutation() {
                    out.insert("Q06:lost-terminal");
                }
            }
            _ => {}
        }
    }

    /// A window is executed when a submission waited in it and was resolved after it.
    fn frozen(&self, out: &mut BTreeSet<&'static str>) {
        for submission in &self.log.submissions {
            if let Some(window) = self.log.frozen_window(submission)
                && submission.invoked + HELD_MS <= window.to
                && submission.completed >= submission.invoked + HELD_MS
                && self.resolved(&submission.body, window.to).is_some()
            {
                out.insert(match (window.nodes.len(), self.log.scoped) {
                    (1, _) => "Q03:one-frozen",
                    (_, false) => "Q03:two-frozen",
                    (_, true) => "O30:two-holders-delayed",
                });
            }
            // O30: two copies suffice, so a request sent with a third holder frozen is
            // acknowledged before it thaws, and its result is visible.
            let delayed = self.log.frozen.iter().any(|window| {
                window.nodes.len() == 1
                    && submission.invoked >= window.from
                    && submission.completed <= window.to
            });
            if self.log.scoped
                && delayed
                && submission.fresh
                && submission.route == Route::Queued
                && matches!(submission.answer, Answer::Accepted(_))
                && self
                    .resolved(&submission.body, submission.completed)
                    .is_some()
            {
                out.insert("O30:third-holder-delayed");
            }
        }
    }

    fn holders(&self, out: &mut BTreeSet<&'static str>) {
        let judged = |holder| {
            self.log
                .retained
                .iter()
                .filter(move |held| held.holder == holder && held.required > 0)
        };
        let ready: BTreeSet<u64> = judged(Holder::Ready).map(|held| held.node).collect();
        if ready.len() == WORKERS.len() {
            out.insert("Q03:copies-on-every-ready-worker");
        }
        // R02 asks for queued and running requests on the holder.
        if judged(Holder::Last).any(|held| held.pending > 0) {
            out.extend(["R02:last-holder", "O14:last-copy"]);
        }
    }

    /// The Q05 clauses and the closed socket of Q04.
    fn identities(&self, out: &mut BTreeSet<&'static str>) {
        let mut by_body: BTreeMap<&Body, Vec<&Submission>> = BTreeMap::new();
        let mut by_id: BTreeMap<Uuid, Vec<&Submission>> = BTreeMap::new();
        for submission in &self.log.submissions {
            by_body
                .entry(&submission.body)
                .or_default()
                .push(submission);
            by_id
                .entry(submission.body.id)
                .or_default()
                .push(submission);
        }
        for (body, sent) in by_body {
            for (index, one) in sent.iter().enumerate() {
                if one.closed
                    && one.answer == Answer::Unknown
                    && sent.iter().any(|s| {
                        s.gateway != one.gateway
                            && s.invoked >= one.completed
                            && matches!(s.answer, Answer::Accepted(run)
                                if self.lookups(run, body.id).any(|l| decided(&l.seen)))
                    })
                {
                    out.insert("Q04:closed-socket");
                }
                for two in &sent[..index] {
                    if !one.answer.executed() || !two.answer.executed() || !body.mutation {
                        continue;
                    }
                    let later = one.completed.max(two.completed);
                    let known = self.resolved(body, later).is_some()
                        || [one, two]
                            .iter()
                            .any(|s| matches!(s.answer, Answer::Terminal(_)));
                    if !known {
                        continue;
                    }
                    let overlap = one.invoked < two.completed && two.invoked < one.completed;
                    if one.gateway != two.gateway && overlap {
                        out.insert("Q05:same-id-two-apis");
                    }
                    // One answer from each API, with the direct path on. `200` from a
                    // submission means that the direct path had committed.
                    let synchronous =
                        |s: &Submission| !s.asynchronous && s.route == Route::MaybeDirect;
                    if self.direct
                        && (synchronous(one) && two.asynchronous
                            || synchronous(two) && one.asynchronous)
                    {
                        out.insert("Q05:path-boundary");
                    }
                }
            }
        }
        for sent in by_id.values() {
            let refused = sent.iter().any(|refused| {
                refused.answer.conflict()
                    && sent.iter().any(|first| {
                        first.body != refused.body
                            && first.body.mutation
                            && first.completed <= refused.invoked
                            && first.answer.executed()
                    })
            });
            if refused {
                out.insert("Q05:other-body");
            }
        }
    }

    /// Q06: the result of a mutation expired, its identity was sent again, and its value, which
    /// a newer one had replaced, was looked for after that.
    fn expired(&self) -> bool {
        self.log.accepted().into_iter().any(|((run, id), first)| {
            let Some(missing) = self
                .lookups(run, id)
                .filter(|lookup| lookup.seen == Seen::Missing)
                .map(|lookup| lookup.completed)
                .min()
            else {
                return false;
            };
            let Some(&(key, value)) = first.body.writes.first() else {
                return false;
            };
            let reads = || self.log.reads.iter().filter(move |read| read.key == key);
            let Some(seen) = reads()
                .filter(|read| read.value == Some(value))
                .map(|read| read.completed)
                .min()
            else {
                return false;
            };
            self.log
                .submissions
                .iter()
                .filter(|again| {
                    again.body == first.body
                        && again.invoked >= missing
                        && again.answer != Answer::Unknown
                })
                .any(|again| {
                    let replaced = reads().any(|read| {
                        read.invoked > seen
                            && read.completed < again.invoked
                            && read.value != Some(value)
                    });
                    let done = match again.answer {
                        Answer::Accepted(run) => self
                            .lookups(run, id)
                            .filter(|l| !matches!(l.seen, Seen::Pending | Seen::Unknown))
                            .map(|l| l.completed)
                            .filter(|at| *at > again.completed)
                            .min(),
                        _ => Some(again.completed),
                    };
                    replaced && done.is_some_and(|at| reads().any(|read| read.invoked > at))
                })
        })
    }

    /// R04: a request that was open at the stop is reported abandoned in the next run. The
    /// counters of the new run exist for the legacy queue only.
    fn stopped(&self, out: &mut BTreeSet<&'static str>) {
        let Some(stop) = self.stop else { return };
        let accepted = self.log.accepted();
        let abandoned: Vec<_> = self
            .log
            .lookups
            .iter()
            .filter(|l| l.run == stop.run && l.invoked >= stop.at && l.seen == Seen::Abandoned)
            .filter_map(|l| accepted.get(&(l.run, l.id)))
            .collect();
        if abandoned.is_empty() || !stop.reread {
            return;
        }
        if abandoned.iter().any(|s| s.body.mutation) {
            out.insert("R04:abandoned");
        }
        if self.log.scoped {
            out.insert("O32:full-stop");
            return;
        }
        let counted: BTreeSet<u64> = self
            .log
            .fresh_runs
            .iter()
            .filter(|fresh| fresh.at >= stop.at)
            .map(|fresh| fresh.node)
            .collect();
        if counted.len() == WORKERS.len() {
            out.insert("R04:full-stop");
        }
    }
}

/// The work that clients completed in a window, all of it sent in that window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Work {
    pub attempts: u64,
    /// Mutations that changed rows.
    pub writes: u64,
    pub reads: u64,
    /// Accepted requests whose receipt became definite.
    pub settled: u64,
    /// The longest time without any of them, or an acceptance.
    pub gap_ms: u64,
}

impl Work {
    pub(crate) fn of(log: &Log, from: u64, to: u64) -> Self {
        let mut work = Self::default();
        let mut sent: BTreeMap<Uuid, u64> = BTreeMap::new();
        for submission in &log.submissions {
            let first = sent.entry(submission.body.id).or_insert(u64::MAX);
            *first = submission.invoked.min(*first);
        }
        let accepted = log.accepted();
        let (mut writes, mut reads, mut settled) =
            (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        let mut times = vec![from, to];
        let mut done = |body: &Body, result: &Terminal, at: u64| {
            if sent.get(&body.id).is_none_or(|first| *first < from) || at > to {
                return false;
            }
            if let Terminal::Succeeded(value) = result {
                if !body.mutation {
                    reads.insert(body.id);
                } else if changed(value) {
                    writes.insert(body.id);
                }
                times.push(at);
            }
            result.decided()
        };
        for submission in &log.submissions {
            let inside = submission.invoked >= from && submission.completed <= to;
            work.attempts += u64::from(submission.invoked >= from && submission.invoked < to);
            if let Answer::Terminal(result) = &submission.answer
                && inside
            {
                done(&submission.body, result, submission.completed);
            }
        }
        for lookup in &log.lookups {
            if let Seen::Terminal(result) = &lookup.seen
                && let Some(acceptance) = accepted.get(&(lookup.run, lookup.id))
                && done(&acceptance.body, result, lookup.completed)
            {
                settled.insert(lookup.id);
            }
        }
        times.extend(
            log.submissions
                .iter()
                .filter(|s| matches!(s.answer, Answer::Accepted(_)))
                .map(|s| s.completed)
                .filter(|at| (from..=to).contains(at)),
        );
        times.sort_unstable();
        work.gap_ms = times
            .windows(2)
            .map(|pair| pair[1].saturating_sub(pair[0]))
            .max()
            .unwrap_or(0);
        work.writes = writes.len() as u64;
        work.reads = reads.len() as u64;
        work.settled = settled.len() as u64;
        work
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::queued::{Fresh, Frozen, Read, Retained, Run};
    use serde_json::json;

    const RUN: Uuid = Uuid::from_u128(100);

    fn body(id: u128, key: i64, value: i64) -> Body {
        Body {
            id: Uuid::from_u128(id),
            text: format!("upsert {key} {value}"),
            anchor: Some(key),
            writes: vec![(key, value)],
            mutation: true,
        }
    }

    fn submit(body: Body, at: u64, answer: Answer) -> Submission {
        Submission {
            body,
            route: Route::Queued,
            asynchronous: true,
            gateway: 101,
            cut: false,
            closed: false,
            fresh: true,
            invoked: at,
            completed: at + 10,
            answer,
        }
    }

    fn accepted(id: u128, at: u64) -> Submission {
        submit(body(id, 1, id as i64), at, Answer::Accepted(RUN))
    }

    fn success() -> Answer {
        Answer::Terminal(Terminal::Succeeded(json!(null)))
    }

    fn lookup(id: u128, at: u64, seen: Seen) -> Lookup {
        Lookup {
            run: RUN,
            id: Uuid::from_u128(id),
            cut: false,
            invoked: at,
            completed: at + 10,
            seen,
        }
    }

    fn ok() -> Seen {
        Seen::Terminal(Terminal::Succeeded(json!(null)))
    }

    fn read(key: i64, value: i64, at: u64) -> Read {
        Read {
            key,
            value: Some(value),
            invoked: at,
            completed: at + 10,
        }
    }

    fn log() -> Log {
        Log {
            runs: vec![Run {
                id: RUN,
                stopped: None,
            }],
            retention_ms: 5_000,
            ..Log::default()
        }
    }

    fn names(log: &Log, marks: &[(Mark, u64)]) -> Vec<&'static str> {
        let facts = Facts {
            log,
            direct: true,
            marks,
            stop: None,
        };
        facts.clauses().into_iter().collect()
    }

    /// Review 132, M3: seed 7 claimed `Q04:api-restart` for requests that had settled 80 s
    /// before the restart and were looked up after it.
    #[test]
    fn a_fault_needs_a_request_that_was_seen_open_after_it() {
        let restart = [(Mark::ApiRestart, 300)];
        let killed = [(Mark::WorkerKilled, 300)];
        let mut log = log();
        log.submissions = vec![accepted(1, 100)];
        log.lookups = vec![lookup(1, 5_000, ok())];
        assert!(names(&log, &restart).is_empty(), "never seen open");
        log.lookups.insert(0, lookup(1, 250, Seen::Pending));
        assert!(names(&log, &restart).is_empty(), "open before the fault");
        log.lookups.insert(1, lookup(1, 300, Seen::Pending));
        assert_eq!(names(&log, &restart), ["Q04:api-restart"]);
        assert_eq!(names(&log, &killed), ["R01:worker-killed"]);
        assert_eq!(names(&log, &[(Mark::Partition, 300)]), ["R05:partition"]);
        assert!(
            names(&log, &[(Mark::ApiRestart, 50)]).is_empty(),
            "accepted after it"
        );
        log.lookups.pop();
        assert!(names(&log, &restart).is_empty(), "never terminal");
        // Terminal without an outcome is not settled.
        log.lookups.push(lookup(
            1,
            5_000,
            Seen::Terminal(Terminal::Failed {
                code: "storage_unavailable".into(),
                unknown: true,
            }),
        ));
        assert!(names(&log, &restart).is_empty());
    }

    #[test]
    fn a_rolling_restart_needs_a_receipt_after_the_last_worker() {
        let mut log = log();
        log.submissions = vec![accepted(1, 100)];
        log.lookups = vec![lookup(1, 300, Seen::Pending), lookup(1, 400, ok())];
        let marks = [(Mark::Rolling { end: 9_000 }, 300)];
        assert!(
            names(&log, &marks).is_empty(),
            "settled before the last restart"
        );
        log.lookups.push(lookup(1, 9_000, ok()));
        assert_eq!(
            names(&log, &marks),
            ["O14:rolling-restart", "R03:rolling-restart"]
        );
    }

    #[test]
    fn a_killed_coordinator_needs_open_work_or_an_unanswered_submission() {
        let mark = |gateway| [(Mark::CoordinatorKilled { gateway }, 105)];
        let mut log = log();
        log.submissions = vec![submit(body(1, 1, 1), 100, Answer::Unknown)];
        assert!(names(&log, &mark(Some(101))).is_empty(), "never resolved");
        log.submissions.push(submit(body(1, 1, 1), 900, success()));
        assert_eq!(names(&log, &mark(Some(101))), ["Q03:coordinator-killed"]);
        assert!(names(&log, &mark(Some(102))).is_empty(), "another gateway");
        assert!(names(&log, &mark(None)).is_empty());
        log.submissions[0].answer = Answer::Refused("overloaded".into());
        assert!(names(&log, &mark(Some(101))).is_empty(), "answered");
    }

    #[test]
    fn a_frozen_window_needs_a_submission_that_waited_in_it() {
        let window = |nodes: &[u64], ready| Frozen {
            nodes: nodes.to_vec(),
            from: 1_000,
            to: 2_000,
            ready,
        };
        let waiting = Submission {
            completed: 4_000,
            answer: Answer::Unknown,
            ..accepted(1, 1_100)
        };
        let mut log = log();
        log.frozen = vec![window(&[1], Some(3))];
        assert!(names(&log, &[]).is_empty(), "nothing was sent");
        log.submissions = vec![waiting.clone()];
        assert!(names(&log, &[]).is_empty(), "never resolved");
        log.submissions
            .push(submit(body(1, 1, 1), 5_000, success()));
        assert_eq!(names(&log, &[]), ["Q03:one-frozen"]);
        log.frozen = vec![window(&[1], Some(2))];
        assert!(names(&log, &[]).is_empty(), "the frozen worker had left");
        log.frozen = vec![window(&[1, 2], None)];
        assert_eq!(names(&log, &[]), ["Q03:two-frozen"]);
        log.scoped = true;
        assert_eq!(names(&log, &[]), ["O30:two-holders-delayed"]);
        // One frozen holder must not stop a scoped acceptance.
        log.frozen = vec![window(&[1], Some(3))];
        assert!(
            names(&log, &[]).is_empty(),
            "not acknowledged in the window"
        );
        log.submissions[0] = accepted(1, 1_100);
        assert_eq!(names(&log, &[]), ["O30:third-holder-delayed"]);
    }

    #[test]
    fn holder_clauses_need_three_counts_and_pending_work() {
        let held = |node, holder, required, pending| Retained {
            node,
            holder,
            required,
            before: 0,
            after: 9,
            pending,
            at: 900,
        };
        let mut log = log();
        log.retained = vec![
            held(1, Holder::Ready, 3, 0),
            held(2, Holder::Ready, 3, 0),
            held(3, Holder::Ready, 0, 0),
            held(3, Holder::Last, 3, 0),
        ];
        assert!(names(&log, &[]).is_empty());
        log.retained[2].required = 3;
        log.retained[3].pending = 1;
        assert_eq!(
            names(&log, &[]),
            [
                "O14:last-copy",
                "Q03:copies-on-every-ready-worker",
                "R02:last-holder"
            ]
        );
    }

    #[test]
    fn a_lost_terminal_needs_a_cut_answer_that_was_reconciled() {
        let marks = [(Mark::LostReply { to: 2_000 }, 1_000)];
        let mut log = log();
        log.submissions = vec![accepted(1, 100)];
        log.lookups = vec![
            lookup(1, 200, ok()),
            lookup(1, 1_500, Seen::Unknown),
            lookup(1, 3_000, ok()),
        ];
        assert!(names(&log, &marks).is_empty(), "a deadline is not a cut");
        log.lookups[1].cut = true;
        assert_eq!(names(&log, &marks), ["Q06:lost-terminal"]);
        log.lookups[0].seen = Seen::Pending;
        assert!(names(&log, &marks).is_empty(), "not known to be terminal");
        let mut lost = submit(body(2, 1, 2), 1_500, Answer::Unknown);
        lost.cut = true;
        log.submissions.push(lost);
        assert!(names(&log, &marks).is_empty(), "never reconciled");
        log.submissions
            .push(submit(body(2, 1, 2), 1_800, success()));
        assert_eq!(names(&log, &marks), ["Q06:lost-terminal"]);
    }

    #[test]
    fn identity_clauses_need_two_executed_answers() {
        let other = |answer, asynchronous| Submission {
            gateway: 102,
            asynchronous,
            route: if asynchronous {
                Route::Queued
            } else {
                Route::MaybeDirect
            },
            ..submit(body(1, 1, 1), 105, answer)
        };
        let mut log = log();
        for answer in [
            Answer::Refused("overloaded".into()),
            Answer::Refused("validation_error".into()),
        ] {
            log.submissions = vec![accepted(1, 100), other(answer, true)];
            log.lookups = vec![lookup(1, 300, ok())];
            assert!(names(&log, &[]).is_empty(), "a refusal executes nothing");
        }
        log.submissions = vec![accepted(1, 100), other(Answer::Accepted(RUN), true)];
        assert_eq!(names(&log, &[]), ["Q05:same-id-two-apis"]);
        log.lookups.clear();
        assert!(names(&log, &[]).is_empty(), "no result was compared");
        log.submissions[1] = other(success(), false);
        assert_eq!(
            names(&log, &[]),
            ["Q05:path-boundary", "Q05:same-id-two-apis"]
        );
        // Without the direct path there is no boundary to cross.
        let facts = Facts {
            log: &log,
            direct: false,
            marks: &[],
            stop: None,
        };
        assert!(!facts.clauses().contains("Q05:path-boundary"));
    }

    #[test]
    fn another_body_is_refused_only_after_the_first_one_ran() {
        let refused = submit(
            body(1, 2, 20),
            300,
            Answer::Terminal(Terminal::failed("conflict")),
        );
        let mut log = log();
        log.submissions = vec![submit(body(1, 1, 1), 100, Answer::Unknown), refused.clone()];
        assert!(names(&log, &[]).is_empty());
        log.submissions[0].answer = success();
        assert_eq!(names(&log, &[]), ["Q05:other-body"]);
    }

    #[test]
    fn a_closed_socket_needs_the_early_close() {
        let mut log = log();
        let mut closed = submit(body(1, 1, 1), 100, Answer::Unknown);
        let retry = Submission {
            gateway: 102,
            ..accepted(1, 300)
        };
        log.submissions = vec![closed.clone(), retry.clone()];
        log.lookups = vec![lookup(1, 400, ok())];
        assert!(names(&log, &[]).is_empty(), "not closed early");
        closed.closed = true;
        log.submissions = vec![closed.clone(), retry.clone()];
        assert_eq!(names(&log, &[]), ["Q04:closed-socket"]);
        closed.answer = Answer::Accepted(RUN);
        log.submissions = vec![closed, retry];
        assert!(
            !names(&log, &[]).contains(&"Q04:closed-socket"),
            "answered before the close"
        );
    }

    #[test]
    fn an_expired_receipt_needs_a_replaced_value_and_a_read_after_the_resend() {
        let mut log = log();
        let again = Submission {
            fresh: false,
            answer: Answer::Missing,
            ..accepted(1, 9_100)
        };
        log.submissions = vec![accepted(1, 100), again];
        log.lookups = vec![lookup(1, 200, ok()), lookup(1, 9_000, Seen::Missing)];
        log.reads = vec![read(1, 1, 300), read(1, 9, 9_500)];
        assert!(names(&log, &[]).is_empty(), "the last write of its key");
        log.reads.insert(1, read(1, 9, 400));
        assert_eq!(names(&log, &[]), ["Q06:expired-receipt"]);
        log.reads.pop();
        assert!(names(&log, &[]).is_empty(), "nothing read after the resend");
        log.reads.push(read(1, 9, 9_500));
        log.submissions[1].answer = Answer::Unknown;
        assert!(names(&log, &[]).is_empty(), "the resend was not answered");
        log.submissions[1].answer = Answer::Accepted(RUN);
        assert!(names(&log, &[]).is_empty(), "it may still run");
        log.lookups.push(lookup(1, 9_200, ok()));
        assert_eq!(names(&log, &[]), ["Q06:expired-receipt"]);
    }

    #[test]
    fn a_full_stop_needs_an_abandoned_receipt_and_every_counter() {
        let mut log = log();
        let names = |log: &Log, reread| {
            let facts = Facts {
                log,
                direct: false,
                marks: &[],
                stop: Some(Stop {
                    run: RUN,
                    at: 500,
                    reread,
                }),
            };
            facts.clauses().into_iter().collect::<Vec<_>>()
        };
        log.submissions = vec![accepted(1, 100)];
        let fresh = |node| Fresh {
            node,
            run: RUN,
            pending_records: 0,
            at: 600,
        };
        log.fresh_runs = WORKERS.map(fresh).to_vec();
        log.lookups = vec![lookup(1, 700, ok())];
        assert!(names(&log, true).is_empty(), "nothing was open at the stop");
        log.lookups = vec![lookup(1, 700, Seen::Abandoned)];
        assert_eq!(names(&log, true), ["R04:abandoned", "R04:full-stop"]);
        assert!(
            names(&log, false).is_empty(),
            "the rows were not read again"
        );
        log.fresh_runs.pop();
        assert_eq!(
            names(&log, true),
            ["R04:abandoned"],
            "one worker not counted"
        );
        log.scoped = true;
        assert_eq!(names(&log, true), ["O32:full-stop", "R04:abandoned"]);
    }

    /// Decision 17.
    #[test]
    fn only_work_sent_and_finished_in_the_window_counts() {
        let mut log = log();
        let mut get = body(2, 1, 0);
        get.mutation = false;
        get.writes.clear();
        log.submissions = vec![
            submit(body(1, 1, 1), 900, success()),
            submit(body(3, 1, 3), 1_100, Answer::Refused("overloaded".into())),
            submit(body(4, 1, 4), 1_200, Answer::Unknown),
            submit(
                body(5, 1, 5),
                1_300,
                Answer::Terminal(Terminal::Succeeded(json!(false))),
            ),
            submit(body(6, 1, 6), 1_995, success()),
            accepted(7, 1_400),
        ];
        log.lookups = vec![lookup(7, 2_500, ok())];
        let work = Work::of(&log, 1_000, 2_000);
        assert_eq!(
            (work.writes, work.reads, work.settled),
            (0, 0, 0),
            "{work:?}"
        );
        assert_eq!(work.attempts, 5);
        assert_eq!(work.gap_ms, 590, "from the acceptance to the end");
        log.submissions.push(submit(get, 1_500, success()));
        log.lookups.push(lookup(7, 1_600, ok()));
        let work = Work::of(&log, 1_000, 2_000);
        assert_eq!(
            (work.writes, work.reads, work.settled),
            (1, 1, 1),
            "{work:?}"
        );
    }
}
