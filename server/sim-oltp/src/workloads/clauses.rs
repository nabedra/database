//! Scenario clauses. A clause runs in a seed only when the fault and the client requests its
//! assertion is about happened together, judged from the attempt log after the run. A
//! scenario counts as executed in a sweep only when every one of its clauses ran.

use std::collections::{BTreeMap, BTreeSet};

use nabedra_protocol::protocol::ErrorCode;
use uuid::Uuid;

use crate::nemesis::Action;

/// Each scenario's clauses. Clauses that no phase 10 workload can reach yet stay listed, so
/// the sweep reports the scenario as incomplete instead of passed.
pub(crate) const SCENARIOS: &[(&str, &[&str])] = &[
    ("D02", &["D02:gateway-killed", "D02:after-propose"]),
    ("D03", &["D03:same-key", "D03:other-key"]),
    ("D04", &["D04:before-quorum", "D04:after-quorum"]),
    ("D05", &["D05:one-paused", "D05:rejoin", "D05:two-paused"]),
    ("D07", &["D07:split", "D07:leader-move"]),
    (
        "D10",
        &["D10:session-drop", "D10:lane", "D10:one-shot-peer"],
    ),
    (
        "O06",
        &[
            "O06:quorum-loss",
            "O06:sync-failure",
            "O06:re-election",
            "O06:corrupt-log",
            "O06:foreign-replica",
        ],
    ),
    ("O10", &["O10:lost-reply"]),
    ("O15", &["O15:cold-start"]),
    (
        "O26",
        &[
            "O26:growth",
            "O26:budgets",
            "O26:mount-identity",
            "O26:pool-move",
            "O26:exact-count",
            "O26:drain",
            "O26:pinned-snapshot",
            "O26:split-across-pools",
            "O26:failed-mount",
            "O26:worker-4",
        ],
    ),
    ("O29", &["O29:ambiguous-write"]),
];

/// Why a clause cannot run yet, for the sweep record.
pub(crate) fn unreachable(clause: &str) -> Option<&'static str> {
    Some(match clause {
        "D04:after-quorum" => "only the P10-PROBES Applied probe can prove the quorum came first",
        "D02:after-propose" => "needs the P10-PROBES proposal trigger",
        "O26:pinned-snapshot" => "a pinned query needs the queued path's barriers (phase 11)",
        "O26:split-across-pools" => "a split never publishes after a direct commit (open finding)",
        "O26:failed-mount" => "no phase 10 schedule loses a mount",
        "O26:worker-4" => "the data worker process binds a real TCP listener",
        _ => return None,
    })
}

/// How one attempt was answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    /// A mutation that changed rows.
    Committed,
    /// A read, or a mutation that changed no row.
    Ok,
    Refused(ErrorCode),
    Unknown,
}

/// One client attempt, in virtual milliseconds.
#[derive(Clone, Debug)]
pub(crate) struct Attempt {
    pub id: Uuid,
    pub mutation: bool,
    /// The first attempt of a request under a new identity.
    pub fresh: bool,
    pub gateway: u64,
    pub invoked: u64,
    pub completed: u64,
    pub answer: Answer,
}

impl Attempt {
    fn spans(&self, at: u64) -> bool {
        self.invoked <= at && at <= self.completed
    }

    fn overlaps(&self, from: u64, to: u64) -> bool {
        self.invoked <= to && from <= self.completed
    }
}

/// A request whose retries ended in a definite success, and whose identity was then replayed
/// through the other gateway.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Resolved {
    pub attempts: u32,
    pub at: u64,
    pub gateway: u64,
    /// The replay returned the original result.
    pub replayed: bool,
}

/// One applied fault window.
#[derive(Clone, Debug)]
pub(crate) struct Fault {
    pub action: Action,
    pub nodes: Vec<u64>,
    pub from: u64,
    pub to: Option<u64>,
    /// The cluster was converged and nothing else was open when it started.
    pub quiet: bool,
    /// `crash-leader` placement: `before-quorum`, `lost-replies`, or empty.
    pub variant: &'static str,
    /// The fault demonstrably happened: an EIO was returned, another leader served, a split
    /// published.
    pub observed: bool,
}

/// What the faults and attempts of a run amount to.
pub(crate) struct Evidence<'a> {
    pub faults: &'a [Fault],
    pub attempts: &'a [Attempt],
    pub resolved: &'a BTreeMap<Uuid, Resolved>,
    pub reuse_same_key: u64,
    pub reuse_other_key: u64,
    /// When `session-drop` reset a session.
    pub session_drops: &'a [u64],
    /// Intervals in which at least two of three workers were frozen or down.
    pub no_quorum: &'a [(u64, u64)],
    /// When a replica move delayed the old voters' frames.
    pub delayed: Option<(u64, u64)>,
    /// The request deadline.
    pub deadline_ms: u64,
}

pub(crate) struct Outcome {
    pub clauses: BTreeSet<&'static str>,
    pub violations: Vec<(&'static str, String)>,
    /// Observation: fresh writes not answered within a short deadline with only one follower
    /// paused.
    pub one_paused_late: u64,
}

impl Evidence<'_> {
    fn window(fault: &Fault) -> (u64, u64) {
        (fault.from, fault.to.unwrap_or(u64::MAX))
    }

    /// The success that a mutation attempt with an unknown outcome reached by a retry.
    fn retried(&self, attempt: &Attempt) -> Option<&Resolved> {
        (attempt.mutation && attempt.answer == Answer::Unknown)
            .then(|| self.resolved.get(&attempt.id))
            .flatten()
            .filter(|resolved| resolved.attempts > 1 && resolved.replayed)
    }

    /// When a fault other than `except` next starts after `at`.
    fn next_fault(&self, except: usize, at: u64) -> u64 {
        self.faults
            .iter()
            .enumerate()
            .filter(|(index, fault)| *index != except && fault.from >= at)
            .map(|(_, fault)| fault.from)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn loaded(&self, from: u64, to: u64) -> bool {
        self.attempts
            .iter()
            .any(|attempt| attempt.mutation && attempt.overlaps(from, to))
    }

    pub(crate) fn evaluate(&self) -> Outcome {
        let mut out = Outcome {
            clauses: BTreeSet::new(),
            violations: Vec::new(),
            one_paused_late: 0,
        };
        if self.reuse_same_key > 0 {
            out.clauses.insert("D03:same-key");
        }
        if self.reuse_other_key > 0 {
            out.clauses.insert("D03:other-key");
        }
        self.no_quorum(&mut out);
        for index in 0..self.faults.len() {
            self.fault(index, &mut out);
        }
        self.session_drops(&mut out);
        out
    }

    /// O06: no fresh write may succeed without a quorum. D05's own schedule runs in
    /// [`super::paused`].
    fn no_quorum(&self, out: &mut Outcome) {
        for &(from, to) in self.no_quorum {
            for attempt in self.attempts.iter().filter(|a| a.mutation && a.fresh) {
                if attempt.invoked < from || attempt.invoked >= to {
                    continue;
                }
                out.clauses.insert("O06:quorum-loss");
                if attempt.answer == Answer::Committed && attempt.completed <= to {
                    out.violations.push((
                        "NoQuorumNoSuccess",
                        format!(
                            "fresh write {} invoked at {}ms committed at {}ms without a quorum ({from}ms to {to}ms)",
                            attempt.id, attempt.invoked, attempt.completed
                        ),
                    ));
                }
            }
        }
    }

    fn fault(&self, index: usize, out: &mut Outcome) {
        let fault = &self.faults[index];
        let (from, to) = Self::window(fault);
        match fault.action {
            // Not an assertion: D05 is asserted only at its own deadline (`super::paused`).
            Action::FreezeFollower if fault.quiet && fault.nodes.len() == 1 => {
                let clean = to.min(self.next_fault(index, from));
                let late = self.attempts.iter().filter(|a| {
                    a.mutation
                        && a.fresh
                        && a.invoked >= from
                        && a.invoked + self.deadline_ms <= clean
                        && !matches!(
                            a.answer,
                            Answer::Committed | Answer::Ok | Answer::Refused(ErrorCode::Conflict)
                        )
                });
                out.one_paused_late += late.count() as u64;
            }
            Action::KillGateway => {
                let retried = self.attempts.iter().any(|attempt| {
                    attempt.gateway == fault.nodes[0]
                        && attempt.spans(from)
                        && self
                            .retried(attempt)
                            .is_some_and(|resolved| resolved.gateway != fault.nodes[0])
                });
                if retried {
                    out.clauses.insert("D02:gateway-killed");
                }
            }
            Action::CrashLeader | Action::PowerLeader | Action::DrainLeader => {
                // The retry completed while the old leader was still down.
                let on_new_leader = self.attempts.iter().any(|attempt| {
                    attempt.spans(from)
                        && self
                            .retried(attempt)
                            .is_some_and(|resolved| resolved.at < to)
                });
                if on_new_leader {
                    if fault.variant == "before-quorum" {
                        out.clauses.insert("D04:before-quorum");
                    }
                    if fault.action == Action::PowerLeader {
                        out.clauses.insert("O06:re-election");
                    }
                }
            }
            Action::LeaderMove if fault.observed && self.loaded(from, to) => {
                out.clauses.insert("D07:leader-move");
            }
            // It runs whether or not the split publishes: one that does not is a Liveness
            // violation of the same seed.
            Action::Split if self.loaded(from, to) => {
                out.clauses.insert("D07:split");
            }
            Action::LaneExhaust => self.lane(index, out),
            Action::DiskError if fault.observed => {
                let hit = self
                    .attempts
                    .iter()
                    .filter(|attempt| attempt.mutation && attempt.overlaps(from, to));
                for attempt in hit {
                    out.clauses.insert("O06:sync-failure");
                    if self.retried(attempt).is_some() {
                        out.clauses.insert("O29:ambiguous-write");
                    }
                }
            }
            Action::LostReply | Action::LostHttpReply => {
                let recovered = self
                    .attempts
                    .iter()
                    .any(|attempt| attempt.overlaps(from, to) && self.retried(attempt).is_some());
                if recovered {
                    out.clauses.insert("O10:lost-reply");
                }
            }
            _ => {}
        }
    }

    /// D10, lane exhaustion: some caller saw overload, and Raft progress continued.
    fn lane(&self, index: usize, out: &mut Outcome) {
        let fault = &self.faults[index];
        let (from, to) = Self::window(fault);
        let refused = self.attempts.iter().any(|attempt| {
            attempt.overlaps(from, to) && attempt.answer == Answer::Refused(ErrorCode::Overloaded)
        });
        if !refused {
            return;
        }
        let until = to.saturating_add(self.deadline_ms);
        let offered = self
            .attempts
            .iter()
            .any(|a| a.mutation && a.fresh && a.invoked >= from && a.completed <= until);
        if !offered {
            return;
        }
        out.clauses.insert("D10:lane");
        let progressed = self
            .attempts
            .iter()
            .any(|a| a.answer == Answer::Committed && a.completed >= from && a.completed <= until);
        if fault.quiet && self.next_fault(index, from) > until && !progressed {
            out.violations.push((
                "RaftProgress",
                format!("no write committed from {from}ms to {until}ms around a lane burst"),
            ));
        }
    }

    /// D10: callers across a dropped session see a success, an unknown outcome or overload.
    /// An attempt that also met another fault proves nothing about the drop.
    fn session_drops(&self, out: &mut Outcome) {
        for &at in self.session_drops {
            for attempt in self.attempts.iter().filter(|attempt| attempt.spans(at)) {
                let alone = self.faults.iter().all(|fault| {
                    let (from, to) = Self::window(fault);
                    fault.action == Action::SessionDrop || !attempt.overlaps(from, to)
                }) && self
                    .delayed
                    .is_none_or(|(from, to)| !attempt.overlaps(from, to));
                if !alone {
                    continue;
                }
                out.clauses.insert("D10:session-drop");
                let allowed = match attempt.answer {
                    Answer::Committed | Answer::Ok | Answer::Unknown => true,
                    Answer::Refused(code) => {
                        matches!(code, ErrorCode::Conflict | ErrorCode::Overloaded)
                    }
                };
                if !allowed {
                    out.violations.push((
                        "SessionDropAnswer",
                        format!(
                            "attempt {} across the session drop at {at}ms was answered {:?}",
                            attempt.id, attempt.answer
                        ),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(id: u128, invoked: u64, completed: u64, answer: Answer) -> Attempt {
        Attempt {
            id: Uuid::from_u128(id),
            mutation: true,
            fresh: true,
            gateway: 101,
            invoked,
            completed,
            answer,
        }
    }

    fn fault(action: Action, node: u64, from: u64, to: u64) -> Fault {
        Fault {
            action,
            nodes: vec![node],
            from,
            to: Some(to),
            quiet: true,
            variant: "",
            observed: true,
        }
    }

    fn evaluate(
        faults: &[Fault],
        attempts: &[Attempt],
        resolved: &BTreeMap<Uuid, Resolved>,
        no_quorum: &[(u64, u64)],
    ) -> Outcome {
        Evidence {
            faults,
            attempts,
            resolved,
            reuse_same_key: 0,
            reuse_other_key: 0,
            session_drops: &[],
            no_quorum,
            delayed: None,
            deadline_ms: 300,
        }
        .evaluate()
    }

    /// Lead decision 10.3: a short deadline is an observation, not a D05 assertion.
    #[test]
    fn one_paused_at_a_short_deadline_is_counted_and_not_asserted() {
        let none = BTreeMap::new();
        let freeze = fault(Action::FreezeFollower, 2, 1_000, 2_000);
        let late = attempt(1, 1_800, 2_500, Answer::Unknown);
        let out = evaluate(std::slice::from_ref(&freeze), &[late], &none, &[]);
        assert_eq!(out.one_paused_late, 0);
        let stuck = attempt(2, 1_100, 1_400, Answer::Unknown);
        let out = evaluate(
            std::slice::from_ref(&freeze),
            std::slice::from_ref(&stuck),
            &none,
            &[],
        );
        assert_eq!(out.one_paused_late, 1);
        assert!(out.clauses.is_empty() && out.violations.is_empty());
        // Another fault inside the window ends the clean part.
        let other = fault(Action::Partition, 1, 1_200, 1_500);
        let out = evaluate(&[freeze, other], &[stuck], &none, &[]);
        assert_eq!(out.one_paused_late, 0);
    }

    #[test]
    fn no_quorum_rejects_a_fresh_commit_and_needs_an_attempt() {
        let none = BTreeMap::new();
        let out = evaluate(&[], &[], &none, &[(1_000, 2_000)]);
        assert!(out.clauses.is_empty());
        let commit = attempt(1, 1_100, 1_200, Answer::Committed);
        let out = evaluate(&[], &[commit], &none, &[(1_000, 2_000)]);
        assert!(out.clauses.contains("O06:quorum-loss"));
        assert_eq!(out.violations[0].0, "NoQuorumNoSuccess");
        // Committed only after the thaw.
        let thawed = attempt(2, 1_100, 2_100, Answer::Committed);
        let out = evaluate(&[], &[thawed], &none, &[(1_000, 2_000)]);
        assert!(out.violations.is_empty());
    }

    #[test]
    fn leader_loss_needs_a_retry_that_completed_on_the_new_leader() {
        let mut crash = fault(Action::CrashLeader, 1, 1_000, 5_000);
        crash.variant = "before-quorum";
        let lost = attempt(1, 900, 1_300, Answer::Unknown);
        let resolved = |at, replayed| {
            let resolved = Resolved {
                attempts: 2,
                at,
                gateway: 102,
                replayed,
            };
            BTreeMap::from([(Uuid::from_u128(1), resolved)])
        };
        let run = |map: &BTreeMap<Uuid, Resolved>| {
            evaluate(
                std::slice::from_ref(&crash),
                std::slice::from_ref(&lost),
                map,
                &[],
            )
            .clauses
            .contains("D04:before-quorum")
        };
        assert!(run(&resolved(2_000, true)));
        assert!(!run(&resolved(6_000, true)), "the old leader was back");
        assert!(!run(&resolved(2_000, false)), "no replay was compared");
        assert!(!run(&BTreeMap::new()));
    }

    /// Direct seed 2: the refusal came from the partition that was open, not from the drop.
    #[test]
    fn a_session_drop_is_judged_only_without_another_fault() {
        let refused = attempt(
            1,
            1_000,
            1_500,
            Answer::Refused(ErrorCode::StorageUnavailable),
        );
        let run = |faults: &[Fault], delayed| {
            let evidence = Evidence {
                faults,
                attempts: std::slice::from_ref(&refused),
                resolved: &BTreeMap::new(),
                reuse_same_key: 0,
                reuse_other_key: 0,
                session_drops: &[1_400],
                no_quorum: &[],
                delayed,
                deadline_ms: 300,
            };
            let out = evidence.evaluate();
            (out.clauses.len(), out.violations.len())
        };
        assert_eq!(run(&[], None), (1, 1));
        let partition = fault(Action::Partition, 1, 500, 2_000);
        assert_eq!(run(&[partition], None), (0, 0));
        // Move seed 7: the move delayed every frame of the old voters by 177 ms.
        assert_eq!(run(&[], Some((500, 2_000))), (0, 0));
        assert_eq!(run(&[], Some((2_000, 3_000))), (1, 1));
    }

    #[test]
    fn scenario_clauses_are_unique() {
        let all: Vec<_> = SCENARIOS.iter().flat_map(|(_, clauses)| *clauses).collect();
        assert_eq!(all.iter().collect::<BTreeSet<_>>().len(), all.len());
        for (scenario, clauses) in SCENARIOS {
            assert!(clauses.iter().all(|clause| clause.starts_with(scenario)));
        }
    }
}
