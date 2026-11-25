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
    ("D07", &["D07:split", "D07:leader-move", "D07:stale-routes"]),
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
    (
        "F03",
        &[
            "F03:reservation",
            "F03:dispatch",
            "F03:receipt",
            "F03:reduction",
            "F03:cleanup",
        ],
    ),
    (
        "Q03",
        &[
            "Q03:one-frozen",
            "Q03:two-frozen",
            "Q03:coordinator-killed",
            "Q03:copies-on-every-ready-worker",
        ],
    ),
    ("Q04", &["Q04:closed-socket", "Q04:api-restart"]),
    (
        "Q05",
        &[
            "Q05:same-id-two-apis",
            "Q05:other-body",
            "Q05:path-boundary",
        ],
    ),
    ("Q06", &["Q06:expired-receipt", "Q06:lost-terminal"]),
    (
        "O14",
        &[
            "O14:rolling-restart",
            "O14:last-copy",
            "O14:restored-from-last-copy",
        ],
    ),
    ("R01", &["R01:worker-killed"]),
    ("R02", &["R02:last-holder"]),
    ("R04", &["R04:full-stop", "R04:abandoned"]),
    ("R05", &["R05:partition", "R05:paused-owner"]),
    (
        "O30",
        &[
            "O30:third-holder-delayed",
            "O30:two-holders-delayed",
            "O30:each-copy-phase",
        ],
    ),
    ("O31", &["O31:payload-quorum", "O31:marker-quorum"]),
    ("O32", &["O32:full-stop", "O32:isolated-holder"]),
    ("D11", &["D11:drop", "D11:index"]),
    (
        "O12",
        &[
            "O12:build-under-load",
            "O12:build-paused",
            "O12:worker-lost",
            "O12:incomplete-not-chosen",
            "O12:present-once",
            "O12:retired",
            "O12:refused-declaration",
            "O12:refused-under-guard",
            "O12:unique-validation",
            "O12:each-phase",
            "O12:restarted-build",
            "O12:bounded-space",
        ],
    ),
    (
        "O16",
        &[
            "O16:split-during-scans",
            "O16:split-during-commits",
            "O16:move-during-commits",
            "O16:restart-during-scans",
            "O16:coverage",
            "O16:queue-takeover",
            "O16:snapshot-interrupted",
        ],
    ),
    (
        "O21",
        &[
            "O21:split-keeps-table",
            "O21:rows-past-100000",
            "O21:automatic-split",
        ],
    ),
    (
        "O28",
        &[
            "O28:routes-after-split",
            "O28:paged-metadata",
            "O28:interrupted-pagination",
        ],
    ),
    (
        "O17",
        &[
            "O17:replacement",
            "O17:replacement-fault",
            "O17:abandoned-intent",
            "O17:removed-worker",
            "O17:wiped-voter-refused",
            "O17:wiped-voter-control",
            "O17:wiped-voter-ddl",
            "O17:wiped-voter-data",
            "O17:move-sequence",
            "O17:data-worker",
            "O17:low-disk",
            "O17:revocation-storm",
        ],
    ),
    (
        "R03",
        &[
            "R03:rolling-restart",
            "R03:replacement-joiner",
            "R03:last-holder",
        ],
    ),
];

/// Why a clause cannot run yet, for the sweep record.
pub(crate) fn unreachable(clause: &str) -> Option<&'static str> {
    Some(match clause {
        "D04:after-quorum" => "only the P10-PROBES Applied probe can prove the quorum came first",
        "D02:after-propose" => "needs the P10-PROBES proposal trigger",
        "O26:pinned-snapshot" => "a pinned query needs the queued path's barriers (phase 11)",
        "O26:split-across-pools" => "no phase 10 schedule splits a range across pools",
        "O26:failed-mount" => "no phase 10 schedule loses a mount",
        "F03:reservation" | "F03:dispatch" => {
            "only the resources overlay cuts such a request, and no seed of this range did"
        }
        "F03:receipt" | "F03:cleanup" => "no probe says that a caller was cancelled there",
        "F03:reduction" => "the resources overlay sends direct-path requests only",
        "O26:worker-4" => "the data worker process binds a real TCP listener",
        "O14:restored-from-last-copy" => {
            "two lost voters leave Control without a quorum, so only a cold start follows"
        }
        "R05:paused-owner" => "no public observation names the queue owner",
        "O30:each-copy-phase" | "O31:payload-quorum" | "O31:marker-quorum" => {
            "needs a probe at each copy phase to place the fault"
        }
        "O32:isolated-holder" => "needs a probe that names the holders of a request",
        "D11:index" => {
            "its assertion is SchemaEpochAtCommit, which needs the schema epoch of each commit and of each Catalog publication (probes)"
        }
        "O12:worker-lost" => "no public observation names the worker that runs a build",
        "O12:unique-validation" => {
            "the product has no unique index: IndexDefinition has no such field"
        }
        "O12:refused-under-guard" => {
            "a rival that reads the Catalog before the winner commits loses the guard, and a later one is refused before it"
        }
        "O12:each-phase" => {
            "placing a fault in backfill, replay, validation or publication needs a probe at each"
        }
        "O12:restarted-build" => "no operator request starts a newer attempt of a build",
        "O12:bounded-space" => "no rule reads the LedgerSnapshot probe of a build",
        "O16:queue-takeover" => "no public observation names the queue owner",
        "O16:snapshot-interrupted" => "needs a probe at the start of a snapshot installation",
        "O21:rows-past-100000" => {
            "100,001 rows on three replicas and their disk images do not fit this run's memory"
        }
        "O21:automatic-split" => "the shard target is 64 MiB; the runs stay far below it",
        "O28:paged-metadata" | "O28:interrupted-pagination" => {
            "needs more ranges than one routing page holds"
        }
        "O17:data-worker" => "the data worker process binds a real TCP listener",
        "O17:low-disk" => "a simulated volume has no capacity limit",
        "R03:last-holder" => "the script settles every receipt before the next worker stops",
        "O17:revocation-storm" => {
            "1,025 replacements, each with its own worker and certificate, need a separate large range"
        }
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

/// One planned fault, and what happened.
#[derive(Clone, Debug)]
pub(crate) struct Fault {
    pub action: Action,
    pub nodes: Vec<u64>,
    pub from: u64,
    pub to: Option<u64>,
    /// When it was planned to end.
    pub until: Option<u64>,
    /// The cluster was converged and nothing else was open when it started.
    pub quiet: bool,
    /// When the fault took effect: a process stopped, a session was reset, an EIO was returned,
    /// another leader was elected, a split published. A fault without an effect did not happen.
    pub effects: Vec<Effect>,
    /// Whether it hit the elected Data leader, for a fault aimed at one.
    pub leader: Option<bool>,
    /// `crash-leader` before quorum: from when every other voter was stopped.
    pub isolated: Option<u64>,
}

/// One effect of a fault.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Effect {
    pub at: u64,
    /// A reset connection's ends.
    pub ends: [Option<u64>; 2],
    /// Requests that a reset connection left unanswered: who sent them, and when.
    pub unanswered: Vec<(u64, u64)>,
}

impl Effect {
    pub(crate) fn at(at: u64) -> Self {
        Self {
            at,
            ..Self::default()
        }
    }
}

impl Fault {
    pub(crate) fn happened(&self) -> bool {
        !self.effects.is_empty()
    }
}

/// What the faults and attempts of a run amount to.
pub(crate) struct Evidence<'a> {
    pub faults: &'a [Fault],
    pub attempts: &'a [Attempt],
    pub resolved: &'a BTreeMap<Uuid, Resolved>,
    /// Reused IDs that D03's assertion judged.
    pub reuse_same_key: u64,
    pub reuse_other_key: u64,
    /// Elections of the table's range: when, and which worker.
    pub elections: &'a [(u64, u64)],
    /// Intervals in which at least two of three workers were frozen or down.
    pub no_quorum: &'a [(u64, u64)],
    /// When a replica move ran.
    pub moving: Option<(u64, u64)>,
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

    /// A mutation that got one outcome: it was answered, or its retry was and the result was
    /// compared with a replay.
    fn settled(&self, attempt: &Attempt) -> bool {
        attempt.mutation
            && (matches!(attempt.answer, Answer::Committed | Answer::Ok)
                || self.retried(attempt).is_some())
    }

    /// The first election of another worker than `node` in a window.
    fn replaced(&self, node: u64, from: u64, to: u64) -> Option<u64> {
        let mut elections = self.elections.iter();
        elections
            .find(|(at, leader)| *leader != node && (from..=to).contains(at))
            .map(|(at, _)| *at)
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
        out
    }

    /// O06: no fresh write may succeed without a quorum. D05's own schedule runs in
    /// [`super::paused`].
    fn no_quorum(&self, out: &mut Outcome) {
        for &(from, to) in self.no_quorum {
            for attempt in self.attempts.iter().filter(|a| a.mutation && a.fresh) {
                // One that outlives the window is answered with a quorum and proves nothing.
                if attempt.invoked < from || attempt.completed > to {
                    continue;
                }
                out.clauses.insert("O06:quorum-loss");
                if attempt.answer == Answer::Committed {
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
            Action::CrashLeader | Action::PowerLeader | Action::DrainLeader
                if fault.happened() && fault.leader == Some(true) =>
            {
                // The retry completed on another leader while the old one was down.
                let retried = |attempt: &Attempt| {
                    self.retried(attempt).is_some_and(|resolved| {
                        resolved.at < to
                            && self.replaced(fault.nodes[0], from, resolved.at).is_some()
                    })
                };
                for attempt in self.attempts.iter().filter(|a| a.spans(from) && retried(a)) {
                    // Sent while nothing could have replicated it.
                    if fault.isolated.is_some_and(|since| attempt.invoked >= since) {
                        out.clauses.insert("D04:before-quorum");
                    }
                    if fault.action == Action::PowerLeader {
                        out.clauses.insert("O06:re-election");
                    }
                }
            }
            Action::LeaderMove => {
                for effect in &fault.effects {
                    let mut across = self.attempts.iter().filter(|a| a.spans(effect.at));
                    if across.any(|attempt| self.settled(attempt)) {
                        out.clauses.insert("D07:leader-move");
                    }
                }
            }
            Action::Split => {
                // A request in flight at the publication, and one sent after it.
                for effect in &fault.effects {
                    let settled: Vec<_> =
                        self.attempts.iter().filter(|a| self.settled(a)).collect();
                    if settled.iter().any(|a| a.spans(effect.at))
                        && settled.iter().any(|a| a.invoked > effect.at)
                    {
                        out.clauses.insert("D07:split");
                    }
                }
            }
            Action::LaneExhaust => self.lane(index, out),
            Action::DiskError => {
                for effect in &fault.effects {
                    let hit = self
                        .attempts
                        .iter()
                        .filter(|a| a.mutation && a.spans(effect.at));
                    for attempt in hit {
                        out.clauses.insert("O06:sync-failure");
                        if fault.leader == Some(true) && self.retried(attempt).is_some() {
                            out.clauses.insert("O29:ambiguous-write");
                        }
                    }
                }
            }
            Action::LostReply | Action::LostHttpReply => {
                for effect in &fault.effects {
                    let recovered = self.attempts.iter().any(|attempt| {
                        attempt.spans(effect.at)
                            && effect.ends.contains(&Some(attempt.gateway))
                            && self.retried(attempt).is_some()
                    });
                    if recovered {
                        out.clauses.insert("O10:lost-reply");
                    }
                }
            }
            Action::SessionDrop => self.session_drop(index, out),
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
        // Judged only when nothing else could have stopped the range.
        if !fault.quiet || self.next_fault(index, from) <= until {
            return;
        }
        out.clauses.insert("D10:lane");
        let progressed = self
            .attempts
            .iter()
            .any(|a| a.answer == Answer::Committed && a.completed >= from && a.completed <= until);
        if !progressed {
            out.violations.push((
                "RaftProgress",
                format!("no write committed from {from}ms to {until}ms around a lane burst"),
            ));
        }
    }

    /// D10: callers whose session was dropped see a success, an unknown outcome or overload.
    /// An attempt that also met another fault proves nothing about the drop.
    fn session_drop(&self, index: usize, out: &mut Outcome) {
        let Some(effect) = self.faults[index].effects.first() else {
            return;
        };
        for attempt in self.attempts.iter().filter(|a| a.spans(effect.at)) {
            // Its gateway had a request of this attempt's time unanswered on that session.
            let carried = effect
                .unanswered
                .iter()
                .any(|&(from, sent)| from == attempt.gateway && sent >= attempt.invoked);
            let alone = self.faults.iter().all(|fault| {
                let (from, to) = Self::window(fault);
                fault.action == Action::SessionDrop || !attempt.overlaps(from, to)
            }) && self
                .moving
                .is_none_or(|(from, to)| !attempt.overlaps(from, to));
            if !carried || !alone {
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
                        "attempt {} across the session drop at {}ms was answered {:?}",
                        attempt.id, effect.at, attempt.answer
                    ),
                ));
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
            until: None,
            quiet: true,
            effects: vec![Effect::at(from)],
            leader: Some(true),
            isolated: None,
        }
    }

    fn evidence<'a>(
        faults: &'a [Fault],
        attempts: &'a [Attempt],
        resolved: &'a BTreeMap<Uuid, Resolved>,
    ) -> Evidence<'a> {
        Evidence {
            faults,
            attempts,
            resolved,
            reuse_same_key: 0,
            reuse_other_key: 0,
            elections: &[],
            no_quorum: &[],
            moving: None,
            deadline_ms: 300,
        }
    }

    fn evaluate(
        faults: &[Fault],
        attempts: &[Attempt],
        resolved: &BTreeMap<Uuid, Resolved>,
        no_quorum: &[(u64, u64)],
    ) -> Outcome {
        Evidence {
            no_quorum,
            ..evidence(faults, attempts, resolved)
        }
        .evaluate()
    }

    fn resolved(id: u128, at: u64, replayed: bool) -> BTreeMap<Uuid, Resolved> {
        let resolved = Resolved {
            attempts: 2,
            at,
            gateway: 102,
            replayed,
        };
        BTreeMap::from([(Uuid::from_u128(id), resolved)])
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
        // Committed only after the thaw: neither a violation nor an assertion that ran.
        let thawed = attempt(2, 1_100, 2_100, Answer::Committed);
        let out = evaluate(&[], &[thawed], &none, &[(1_000, 2_000)]);
        assert!(out.violations.is_empty() && out.clauses.is_empty());
    }

    /// Review 115, M3: a write sent before the freeze, a follower that was killed, and a retry
    /// without a new leader are none of them "before quorum".
    #[test]
    fn before_quorum_needs_the_leader_a_write_after_the_freeze_and_a_new_leader() {
        let mut crash = fault(Action::CrashLeader, 1, 1_000, 5_000);
        crash.isolated = Some(800);
        let run = |crash: &Fault, invoked, map: &BTreeMap<Uuid, Resolved>| {
            let lost = attempt(1, invoked, 1_300, Answer::Unknown);
            let evidence = Evidence {
                elections: &[(500, 1), (1_500, 2)],
                ..evidence(
                    std::slice::from_ref(crash),
                    std::slice::from_ref(&lost),
                    map,
                )
            };
            let out = evidence.evaluate();
            out.clauses.contains("D04:before-quorum")
        };
        assert!(run(&crash, 900, &resolved(1, 2_000, true)));
        assert!(
            !run(&crash, 700, &resolved(1, 2_000, true)),
            "sent too early"
        );
        assert!(!run(&crash, 900, &resolved(1, 6_000, true)), "it was back");
        assert!(!run(&crash, 900, &resolved(1, 2_000, false)), "no replay");
        assert!(
            !run(&crash, 900, &resolved(1, 1_400, true)),
            "no new leader"
        );
        assert!(!run(&crash, 900, &BTreeMap::new()));
        let mut follower = crash.clone();
        follower.leader = Some(false);
        assert!(!run(&follower, 900, &resolved(1, 2_000, true)));
        crash.isolated = None;
        assert!(!run(&crash, 900, &resolved(1, 2_000, true)));
    }

    /// Review 115, M1: the tag was set before the condition of its assertion.
    #[test]
    fn a_lane_tag_means_that_progress_was_judged() {
        let none = BTreeMap::new();
        let burst = fault(Action::LaneExhaust, 101, 1_000, 1_200);
        let attempts = [
            attempt(1, 1_050, 1_100, Answer::Refused(ErrorCode::Overloaded)),
            attempt(2, 1_050, 1_350, Answer::Unknown),
        ];
        let out = evaluate(std::slice::from_ref(&burst), &attempts, &none, &[]);
        assert!(out.clauses.contains("D10:lane"));
        assert_eq!(out.violations[0].0, "RaftProgress");
        let mut busy = burst.clone();
        busy.quiet = false;
        let out = evaluate(&[busy], &attempts, &none, &[]);
        assert!(out.clauses.is_empty() && out.violations.is_empty());
        let other = fault(Action::Partition, 1, 1_300, 1_500);
        let out = evaluate(&[burst, other], &attempts, &none, &[]);
        assert!(out.clauses.is_empty() && out.violations.is_empty());
    }

    /// Review 115, M2: a split that never published is not D07's split.
    #[test]
    fn a_split_tag_needs_a_publication_and_requests_on_both_sides_of_it() {
        let none = BTreeMap::new();
        let mut split = fault(Action::Split, 1, 1_000, 4_000);
        split.effects = vec![Effect::at(2_000)];
        let across = attempt(1, 1_900, 2_100, Answer::Committed);
        let after = attempt(2, 2_200, 2_300, Answer::Committed);
        let lost = attempt(3, 2_200, 2_300, Answer::Unknown);
        let run = |split: &Fault, attempts: &[Attempt]| {
            let out = evaluate(std::slice::from_ref(split), attempts, &none, &[]);
            out.clauses.contains("D07:split")
        };
        assert!(run(&split, &[across.clone(), after.clone()]));
        assert!(!run(&split, &[across.clone(), lost]));
        assert!(!run(&split, std::slice::from_ref(&after)));
        split.effects.clear();
        assert!(!run(&split, &[across, after]));
    }

    /// Review 115, M4 and M7: a window in which no reply was lost, and an armed EIO that never
    /// fired, tag nothing.
    #[test]
    fn a_fault_that_did_not_happen_tags_nothing() {
        let write = attempt(1, 1_000, 1_500, Answer::Unknown);
        let map = resolved(1, 2_000, true);
        let run = |fault: &Fault| {
            let write = std::slice::from_ref(&write);
            evaluate(std::slice::from_ref(fault), write, &map, &[]).clauses
        };
        let mut reply = fault(Action::LostReply, 2, 900, 1_600);
        reply.effects = vec![Effect {
            at: 1_200,
            ends: [Some(2), Some(101)],
            unanswered: Vec::new(),
        }];
        assert!(run(&reply).contains("O10:lost-reply"));
        reply.effects[0].ends = [Some(2), Some(102)];
        assert!(run(&reply).is_empty(), "another gateway's session");
        reply.effects.clear();
        assert!(run(&reply).is_empty());
        let mut disk = fault(Action::DiskError, 2, 900, 1_600);
        disk.effects = vec![Effect::at(1_200)];
        let both = ["O06:sync-failure", "O29:ambiguous-write"];
        assert_eq!(run(&disk), BTreeSet::from(both));
        disk.leader = Some(false);
        assert_eq!(run(&disk), BTreeSet::from([both[0]]));
        disk.effects.clear();
        assert!(run(&disk).is_empty());
    }

    /// Direct seed 2: the refusal came from the partition that was open, not from the drop.
    /// Review 115, M12: an attempt on another session was judged too.
    #[test]
    fn a_session_drop_is_judged_only_on_its_session_and_without_another_fault() {
        let refused = attempt(
            1,
            1_000,
            1_500,
            Answer::Refused(ErrorCode::StorageUnavailable),
        );
        let drop = |from, sent| {
            let mut drop = fault(Action::SessionDrop, 0, 1_400, 1_400);
            drop.effects[0].unanswered = vec![(from, sent)];
            drop
        };
        let run = |faults: &[Fault], moving| {
            let none = BTreeMap::new();
            let evidence = Evidence {
                moving,
                ..evidence(faults, std::slice::from_ref(&refused), &none)
            };
            let out = evidence.evaluate();
            (out.clauses.len(), out.violations.len())
        };
        assert_eq!(run(&[drop(101, 1_010)], None), (1, 1));
        assert_eq!(run(&[drop(102, 1_010)], None), (0, 0), "another gateway");
        assert_eq!(run(&[drop(101, 900)], None), (0, 0), "an earlier request");
        let partition = fault(Action::Partition, 1, 500, 2_000);
        assert_eq!(run(&[drop(101, 1_010), partition], None), (0, 0));
        // Move seed 7: the same refusals came with the drop and the delay switched off.
        assert_eq!(run(&[drop(101, 1_010)], Some((500, 2_000))), (0, 0));
        assert_eq!(run(&[drop(101, 1_010)], Some((2_000, 3_000))), (1, 1));
    }

    #[test]
    fn a_leader_move_needs_an_election_and_a_request_across_it() {
        let none = BTreeMap::new();
        let mut moved = fault(Action::LeaderMove, 1, 1_000, 3_000);
        moved.effects = vec![Effect::at(2_000)];
        let run = |fault: &Fault, attempt: Attempt| {
            let out = evaluate(std::slice::from_ref(fault), &[attempt], &none, &[]);
            out.clauses.contains("D07:leader-move")
        };
        assert!(run(&moved, attempt(1, 1_900, 2_100, Answer::Committed)));
        assert!(!run(&moved, attempt(1, 1_100, 1_900, Answer::Committed)));
        assert!(!run(&moved, attempt(1, 1_900, 2_100, Answer::Unknown)));
        moved.effects.clear();
        assert!(!run(&moved, attempt(1, 1_900, 2_100, Answer::Committed)));
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
