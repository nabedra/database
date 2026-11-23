//! Raft safety invariants over observations of `(group, node, term, index, payload hash)`,
//! after Kafka's `RaftEventSimulationTest`. Ported from the Events simulator's `Observer`
//! (`server/events/src/sim/cluster.rs`), generalised to many groups and freed of openraft
//! types, so any harness (openraft 0.9 or 0.10) maps its ids onto [`LogId`] and [`Vote`].
//!
//! | Name | Rule | Fed by |
//! | --- | --- | --- |
//! | `AppliedPrefixesAgree` | every replica applies the same log id and payload hash at a `(group, index)` (log matching) | [`RaftObserver::applied`] |
//! | `MonotonicCommit` | within one incarnation a replica applies strictly increasing indexes | [`RaftObserver::applied`] |
//! | `MajorityReachedHighWatermark` | an applied entry is durable on a majority of the group's voters | [`RaftObserver::appended`], [`RaftObserver::truncated`], [`RaftObserver::restarted`] |
//! | `MonotonicEpoch` | a replica's vote never decreases, across restarts (the design's `MonotonicVote`) | [`RaftObserver::observe`] |
//! | `SingleLeader` | a leader holds its own committed vote | [`RaftObserver::observe`] |
//! | `ElectionSafety` | at most one node leads under one `(group, vote)` | [`RaftObserver::leader_seen`] |
//! | `MonotonicHighWatermark` | a replica's reported applied index never falls within one incarnation | [`RaftObserver::observe`] |
//!
//! The names stay those of the Events simulator, so its planted-bug searches keep matching.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Violations kept per run; later ones are counted only by their first report.
pub const MAX_VIOLATIONS: usize = 32;

/// A leader: openraft's advanced leader id `(term, node)`, ordered by term, then node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaderId {
    pub term: u64,
    pub node: u64,
}

impl LeaderId {
    pub fn new(term: u64, node: u64) -> Self {
        Self { term, node }
    }
}

impl fmt::Display for LeaderId {
    /// openraft's form, `T<term>-N<node>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}-N{}", self.term, self.node)
    }
}

/// A log entry's id: the leader that proposed it and its index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId {
    pub leader: LeaderId,
    pub index: u64,
}

impl LogId {
    pub fn new(term: u64, leader: u64, index: u64) -> Self {
        Self {
            leader: LeaderId::new(term, leader),
            index,
        }
    }
}

impl fmt::Display for LogId {
    /// openraft's form, `T<term>-N<node>-<index>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.leader, self.index)
    }
}

/// A vote: the leader it grants and whether a quorum granted it. Ordered as openraft 0.9
/// orders votes with the advanced leader id: by leader id, then uncommitted before
/// committed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Vote {
    pub leader: LeaderId,
    pub committed: bool,
}

impl Vote {
    pub fn new(term: u64, node: u64) -> Self {
        Self {
            leader: LeaderId::new(term, node),
            committed: false,
        }
    }

    pub fn new_committed(term: u64, node: u64) -> Self {
        Self {
            leader: LeaderId::new(term, node),
            committed: true,
        }
    }
}

impl fmt::Display for Vote {
    /// openraft 0.9's form, `T<term>-N<node>:committed` or `:uncommitted`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.committed {
            "committed"
        } else {
            "uncommitted"
        };
        write!(f, "{}:{state}", self.leader)
    }
}

#[derive(Debug, Default)]
struct Group {
    voters: u64,
    /// index -> (leader id, payload hash) of every entry any replica applied.
    committed: BTreeMap<u64, (LeaderId, u64)>,
    /// Each replica's durable log, from the append and truncate observations; rebuilt from
    /// its volume on every start.
    logs: BTreeMap<u64, BTreeMap<u64, LeaderId>>,
    /// The highest vote each replica has reported, across restarts.
    max_vote: BTreeMap<u64, Vote>,
    /// The applied index each replica reported in its current incarnation.
    applied_hook: BTreeMap<u64, u64>,
    applied_metric: BTreeMap<u64, u64>,
    /// The node seen leading under each vote.
    leaders: BTreeMap<Vote, u64>,
}

/// The Raft invariants of every group in one run.
#[derive(Debug, Default)]
pub struct RaftObserver {
    default_voters: u64,
    groups: BTreeMap<String, Group>,
    violations: Vec<String>,
    /// Messages already reported, so a violation that persists is reported once.
    reported: BTreeSet<String>,
}

impl RaftObserver {
    /// An observer in which every group has `default_voters` voters until
    /// [`RaftObserver::set_voters`] says otherwise.
    pub fn new(default_voters: u64) -> Self {
        Self {
            default_voters,
            ..Self::default()
        }
    }

    fn group(&mut self, group: &str) -> &mut Group {
        if !self.groups.contains_key(group) {
            let voters = self.default_voters;
            self.groups.insert(
                group.to_owned(),
                Group {
                    voters,
                    ..Group::default()
                },
            );
        }
        self.groups.get_mut(group).expect("inserted above")
    }

    /// Sets the voter count a majority of `group` is computed from.
    pub fn set_voters(&mut self, group: &str, voters: u64) {
        self.group(group).voters = voters;
    }

    /// Records a violation at virtual time `now_ms`. A message already reported is not
    /// repeated, and at most [`MAX_VIOLATIONS`] are kept.
    pub fn violate(&mut self, now_ms: u64, message: String) {
        if self.violations.len() < MAX_VIOLATIONS && self.reported.insert(message.clone()) {
            self.violations.push(format!("t={now_ms}ms {message}"));
        }
    }

    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    pub fn take_violations(&mut self) -> Vec<String> {
        std::mem::take(&mut self.violations)
    }

    /// Distinct indexes any replica of `group` applied.
    pub fn committed_len(&self, group: &str) -> usize {
        self.groups
            .get(group)
            .map_or(0, |group| group.committed.len())
    }

    /// Groups observed so far, in name order.
    pub fn groups(&self) -> impl Iterator<Item = &str> {
        self.groups.keys().map(String::as_str)
    }

    /// `node` applied `log_id` with this payload hash.
    pub fn applied(&mut self, group: &str, node: u64, log_id: LogId, hash: u64, now_ms: u64) {
        let LogId { leader, index } = log_id;
        let state = self.group(group);
        let prior = state.committed.get(&index).copied();
        let mut found = Vec::new();
        match prior {
            Some((prior, prior_hash)) if (prior, prior_hash) != (leader, hash) => found.push(format!(
                "AppliedPrefixesAgree: {group} n{node} applied {log_id} ({hash:x}), another replica applied {} ({prior_hash:x})",
                LogId { leader: prior, index },
            )),
            Some(_) => {}
            None => {
                state.committed.insert(index, (leader, hash));
                found.extend(entry_majority(group, state, log_id));
            }
        }
        if let Some(previous) = state
            .applied_hook
            .insert(node, index)
            .filter(|prior| index <= *prior)
        {
            found.push(format!(
                "MonotonicCommit: {group} n{node} applied index {index} after {previous} in one incarnation"
            ));
        }
        for message in found {
            self.violate(now_ms, message);
        }
    }

    /// `node` made `log_id` durable.
    pub fn appended(&mut self, group: &str, node: u64, log_id: LogId) {
        self.group(group)
            .logs
            .entry(node)
            .or_default()
            .insert(log_id.index, log_id.leader);
    }

    /// `node` deleted its log from index `from` on.
    pub fn truncated(&mut self, group: &str, node: u64, from: u64) {
        self.group(group)
            .logs
            .entry(node)
            .or_default()
            .split_off(&from);
    }

    /// `node` started a new incarnation with this durable log, read from its volume.
    pub fn restarted(&mut self, group: &str, node: u64, log: impl IntoIterator<Item = LogId>) {
        let state = self.group(group);
        state.logs.insert(
            node,
            log.into_iter().map(|id| (id.index, id.leader)).collect(),
        );
        state.applied_hook.remove(&node);
        state.applied_metric.remove(&node);
    }

    /// A metrics sample of `node`: its vote, whether it claims leadership, and its applied
    /// index.
    pub fn observe(
        &mut self,
        group: &str,
        node: u64,
        vote: Vote,
        leader: bool,
        applied: Option<u64>,
        now_ms: u64,
    ) {
        let state = self.group(group);
        let mut found = Vec::new();
        let max = state.max_vote.entry(node).or_default();
        if vote < *max {
            found.push(format!(
                "MonotonicEpoch: {group} n{node} reports vote {vote} after {max}"
            ));
        } else {
            *max = vote;
        }
        // openraft's advanced election epoch is (term, node). A new leader can supersede
        // another leader in the same numeric term with a higher vote.
        if leader && (!vote.committed || vote.leader.node != node) {
            found.push(format!(
                "SingleLeader: {group} n{node} reports leadership under vote {vote}"
            ));
        }
        if let Some(applied) = applied
            && let Some(previous) = state
                .applied_metric
                .insert(node, applied)
                .filter(|prior| applied < *prior)
        {
            found.push(format!(
                "MonotonicHighWatermark: {group} n{node} applied index fell from {previous} to {applied}"
            ));
        }
        for message in found {
            self.violate(now_ms, message);
        }
        if leader {
            self.leader_seen(group, node, vote, now_ms);
        }
    }

    /// `node` acted as leader of `group` under `vote` (a probe, or a metrics sample).
    pub fn leader_seen(&mut self, group: &str, node: u64, vote: Vote, now_ms: u64) {
        let prior = *self.group(group).leaders.entry(vote).or_insert(node);
        if prior != node {
            self.violate(
                now_ms,
                format!("ElectionSafety: {group} n{node} and n{prior} both lead under vote {vote}"),
            );
        }
    }

    /// Checks that the highest applied entry of `group` is still held by a majority.
    pub fn check_majority(&mut self, group: &str, now_ms: u64) {
        let state = self.group(group);
        let Some((&index, &(leader, _))) = state.committed.last_key_value() else {
            return;
        };
        if let Some(message) = entry_majority(group, state, LogId { leader, index }) {
            self.violate(now_ms, message);
        }
    }

    /// [`RaftObserver::check_majority`] for every group.
    pub fn check_majorities(&mut self, now_ms: u64) {
        let groups: Vec<String> = self.groups.keys().cloned().collect();
        for group in groups {
            self.check_majority(&group, now_ms);
        }
    }
}

fn entry_majority(name: &str, group: &Group, log_id: LogId) -> Option<String> {
    let holders = group
        .logs
        .values()
        .filter(|log| log.get(&log_id.index) == Some(&log_id.leader))
        .count() as u64;
    (holders <= group.voters / 2).then(|| {
        format!(
            "MajorityReachedHighWatermark: {name} committed entry {log_id} is held by {holders} of {} replicas",
            group.voters
        )
    })
}
