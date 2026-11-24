//! Probe-driven protocol invariants (design section 3.7).
//!
//! [`Protocol`] is a pure state machine over the probe stream: the hub feeds it each probe
//! with its node, incarnation and virtual time, and it returns the violations that probe
//! alone decides. It never calls product code. Quiescence and history checks read its
//! state afterwards.
//!
//! Replicas are keyed by `(group, replica)`, the group ID and the Raft node ID inside it.
//! Votes and synced log entries are durable, so they survive crashes and are forgotten
//! only when the node's volume is wiped. Applied positions and resource ledgers belong
//! to one process incarnation.

use std::collections::{BTreeMap, BTreeSet};

use nabedra_oltp_types::{
    sim_probe::{Ledger, Probe, RouteRecord, Span},
    types::{AppliedPosition, Digest},
};
use nabedra_protocol::protocol::Key;
use uuid::Uuid;

/// Names of the invariants this module reports.
pub const ELECTION_SAFETY: &str = "ElectionSafety";
pub const MONOTONIC_VOTE: &str = "MonotonicVote";
pub const MONOTONIC_COMMIT: &str = "MonotonicCommit";
pub const LOG_MATCHING: &str = "LogMatching";
pub const MAJORITY_REACHED: &str = "MajorityReached";
pub const TRUNCATION: &str = "MajorityReached/truncation";
pub const DEDUP_UNIQUE: &str = "DedupUnique";
pub const NOT_APPLIED_NEVER_COMMITTED: &str = "NotAppliedNeverCommitted";
pub const RANGE_CLOCK_RULE1: &str = "RangeClockRule1";
pub const KEYSPACE_COVERAGE: &str = "KeyspaceCoverage";
pub const EXACTLY_ONCE_RELEASE: &str = "ExactlyOnceRelease";
pub const RESOURCE_BASELINE: &str = "ResourceBaseline";

/// Every invariant the probe stream checks, for the run's assertion map.
pub const PROBE_INVARIANTS: [&str; 11] = [
    ELECTION_SAFETY,
    MONOTONIC_VOTE,
    MONOTONIC_COMMIT,
    LOG_MATCHING,
    MAJORITY_REACHED,
    DEDUP_UNIQUE,
    NOT_APPLIED_NEVER_COMMITTED,
    RANGE_CLOCK_RULE1,
    KEYSPACE_COVERAGE,
    EXACTLY_ONCE_RELEASE,
    RESOURCE_BASELINE,
];

pub type Violation = (&'static str, String);

/// Where and when a probe was emitted. `live` is false for a probe a destructor emits
/// after its node was cut: it belongs to a dead incarnation and changes no state.
#[derive(Clone, Copy, Debug)]
pub struct Origin {
    pub node: Option<u64>,
    pub incarnation: u64,
    pub t_ns: u64,
    pub live: bool,
}

/// A key in route spans and dedup anchors, ordered like the product's primary key order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrdKey {
    Int(i64),
    Text(String),
}

impl OrdKey {
    pub fn new(key: &Key) -> Self {
        match key {
            Key::Int64(value) => Self::Int(*value),
            Key::Utf8(value) => Self::Text(value.clone()),
        }
    }

    /// The key as the history records it (`table/key`).
    pub fn history(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Text(value) => value.clone(),
        }
    }
}

type Replica = (Uuid, u64);
type LogPoint = (u64, u64);

fn point(position: &AppliedPosition) -> LogPoint {
    (position.term, position.node_id)
}

/// True when `holders` include a majority of every configuration in `joint`.
fn quorum(joint: &[Vec<u64>], holders: &BTreeSet<u64>) -> bool {
    !joint.is_empty()
        && joint.iter().all(|voters| {
            voters.iter().filter(|id| holders.contains(id)).count() > voters.len() / 2
        })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RouteSpan {
    start: Option<OrdKey>,
    end: Option<OrdKey>,
}

impl RouteSpan {
    fn new(span: &Span) -> Self {
        Self {
            start: span.start.as_ref().map(OrdKey::new),
            end: span.end.as_ref().map(OrdKey::new),
        }
    }
}

/// True when `spans` cover the whole key space once: sorted by start, the first is
/// unbounded below, the last unbounded above, each is nonempty and each ends where
/// the next starts.
pub fn covers_keyspace<K: Ord>(spans: &[(Option<K>, Option<K>)]) -> bool {
    let mut ordered: Vec<_> = spans.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let full = ordered.first().is_some_and(|span| span.0.is_none())
        && ordered.last().is_some_and(|span| span.1.is_none());
    let nonempty = ordered.iter().all(|span| match (&span.0, &span.1) {
        (Some(start), Some(end)) => start < end,
        _ => true,
    });
    let adjacent = ordered
        .windows(2)
        .all(|pair| pair[0].1.is_some() && pair[0].1 == pair[1].0);
    full && nonempty && adjacent
}

/// A committed dedup record: the request's identity, as the history names it, and when
/// the first replica wrote it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DedupIdentity {
    pub table: Uuid,
    pub key: OrdKey,
    pub request_id: Uuid,
}

type Root = (Option<Uuid>, u64);
type Page = (Option<Uuid>, Vec<RouteSpan>);

#[derive(Default)]
pub struct Protocol {
    /// Per replica: its newest durable vote `(term, leader, committed)`.
    votes: BTreeMap<Replica, (u64, u64, bool)>,
    /// Per group and committed vote: the simulated node that claimed it as leader.
    leaders: BTreeMap<(Uuid, LogPoint), u64>,
    /// Per group: voter configurations by log index and log id.
    voters: BTreeMap<Uuid, BTreeMap<(u64, LogPoint), Vec<Vec<u64>>>>,
    /// Per replica: its durable log, index to log id.
    durable: BTreeMap<Replica, BTreeMap<u64, LogPoint>>,
    /// Per group and index: the first applied log id and digest.
    applied: BTreeMap<(Uuid, u64), (LogPoint, Digest)>,
    /// Per node incarnation and replica: the last applied index.
    last_applied: BTreeMap<(u64, u64, Replica), u64>,
    /// Where each replica runs.
    replica_node: BTreeMap<Replica, u64>,
    dedup: BTreeMap<DedupIdentity, (u64, Digest, u64)>,
    /// Per group: the highest snapshot timestamp served on any replica.
    served: BTreeMap<Uuid, u64>,
    /// Per table: committed routing roots and pages by commit timestamp. `None` is a delete.
    roots: BTreeMap<Uuid, BTreeMap<u64, Option<Root>>>,
    pages: BTreeMap<(Uuid, Uuid), BTreeMap<u64, Option<Page>>>,
    /// How often each rule ran on an event of its own.
    exercised: BTreeMap<&'static str, u64>,
    /// Per live node: native leases acquired and released by this incarnation.
    native: BTreeMap<u64, (u64, u64)>,
    /// Per live node and group: the RangeClock pending ledger.
    pending: BTreeMap<(u64, Uuid), u64>,
}

impl Protocol {
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluations per rule. A rule absent here never ran on this stream.
    pub fn exercised(&self) -> &BTreeMap<&'static str, u64> {
        &self.exercised
    }

    fn ran(&mut self, rule: &'static str) {
        *self.exercised.entry(rule).or_default() += 1;
    }

    /// Checks `probe` and updates the state. Returns what it violates.
    pub fn observe(&mut self, origin: Origin, probe: &Probe) -> Vec<Violation> {
        let mut out = Vec::new();
        if !origin.live {
            return out;
        }
        let node = origin.node;
        match probe {
            Probe::VoteSaved {
                group,
                replica,
                term,
                leader,
                committed,
                ..
            } => {
                self.place(*group, *replica, node);
                self.vote(
                    node,
                    *group,
                    *replica,
                    (*term, *leader, *committed),
                    &mut out,
                );
            }
            Probe::Appended {
                group,
                replica,
                entries,
                ..
            } => {
                self.place(*group, *replica, node);
                let log = self.durable.entry((*group, *replica)).or_default();
                for entry in entries {
                    log.insert(entry.at.index, point(&entry.at));
                    if let Some(voters) = &entry.voters {
                        self.voters
                            .entry(*group)
                            .or_default()
                            .insert((entry.at.index, point(&entry.at)), voters.clone());
                    }
                }
            }
            Probe::Truncated {
                group,
                replica,
                after,
                ..
            } => {
                self.place(*group, *replica, node);
                let from = after.map_or(0, |after| after.index.saturating_add(1));
                let log = self.durable.entry((*group, *replica)).or_default();
                let removed = log.split_off(&from);
                *self.exercised.entry(TRUNCATION).or_default() += 1;
                for (index, id) in removed {
                    if let Some((applied, _)) = self.applied.get(&(*group, index))
                        && *applied == id
                    {
                        out.push((
                            MAJORITY_REACHED,
                            format!(
                                "group {group} replica {replica} truncated applied entry {index} (term {}, leader {})",
                                id.0, id.1
                            ),
                        ));
                    }
                }
            }
            Probe::Applied {
                group,
                replica,
                entry,
                digest,
                ..
            } => {
                self.place(*group, *replica, node);
                self.apply(origin, *group, *replica, entry, *digest, &mut out);
            }
            Probe::DedupApplied {
                table,
                anchor,
                request_id,
                commit_ts,
                digest,
                ..
            } => {
                self.ran(DEDUP_UNIQUE);
                let identity = DedupIdentity {
                    table: *table,
                    key: OrdKey::new(anchor),
                    request_id: *request_id,
                };
                let record =
                    self.dedup
                        .entry(identity)
                        .or_insert((*commit_ts, *digest, origin.t_ns));
                if (record.0, record.1) != (*commit_ts, *digest) {
                    out.push((
                        DEDUP_UNIQUE,
                        format!(
                            "table {table} key {anchor:?} request {request_id}: dedup record at {commit_ts} differs from the one at {}",
                            record.0
                        ),
                    ));
                }
            }
            Probe::SnapshotServed { group, read_ts } => {
                let served = self.served.entry(*group).or_default();
                *served = (*served).max(*read_ts);
            }
            Probe::DirectProposed {
                group, commit_ts, ..
            } => {
                let served = self.served.get(group).copied();
                if served.is_some() {
                    self.ran(RANGE_CLOCK_RULE1);
                }
                if let Some(served) = served
                    && *commit_ts <= served
                {
                    out.push((
                        RANGE_CLOCK_RULE1,
                        format!(
                            "group {group}: direct commit at {commit_ts} registered after a snapshot at {served} was served"
                        ),
                    ));
                }
            }
            Probe::RouteEpochPublished {
                table,
                commit_ts,
                record,
            } => match record {
                RouteRecord::Root {
                    first_page,
                    routing_epoch,
                } => {
                    let root = Some((*first_page, *routing_epoch));
                    self.roots
                        .entry(*table)
                        .or_default()
                        .insert(*commit_ts, root);
                }
                RouteRecord::RootDeleted => {
                    self.roots
                        .entry(*table)
                        .or_default()
                        .insert(*commit_ts, None);
                }
                RouteRecord::Page {
                    page_id,
                    next_page,
                    spans,
                } => {
                    let page = Some((*next_page, spans.iter().map(RouteSpan::new).collect()));
                    self.pages
                        .entry((*table, *page_id))
                        .or_default()
                        .insert(*commit_ts, page);
                }
                RouteRecord::PageDeleted { page_id } => {
                    self.pages
                        .entry((*table, *page_id))
                        .or_default()
                        .insert(*commit_ts, None);
                }
            },
            Probe::LedgerSnapshot {
                ledger: Ledger::RangeClockPending,
                group: Some(group),
                outstanding,
            } => {
                if let Some(node) = node {
                    self.pending.insert((node, *group), *outstanding);
                }
            }
            Probe::GuardAcquired { .. } => {
                if let Some(node) = node {
                    self.native.entry(node).or_default().0 += 1;
                }
            }
            Probe::GuardReleased { .. } => {
                if let Some(node) = node {
                    self.ran(EXACTLY_ONCE_RELEASE);
                    let (acquired, released) = self.native.entry(node).or_default();
                    *released += 1;
                    if *released > *acquired {
                        out.push((
                            EXACTLY_ONCE_RELEASE,
                            format!(
                                "n{node} released {released} native leases but acquired {acquired}"
                            ),
                        ));
                    }
                }
            }
            Probe::LedgerSnapshot { .. }
            | Probe::LeaderSeen { .. }
            | Probe::GatewayAttempt { .. }
            | Probe::QueueCopy { .. }
            | Probe::Receipt { .. }
            | Probe::WorkDone { .. } => {}
        }
        out
    }

    fn place(&mut self, group: Uuid, replica: u64, node: Option<u64>) {
        if let Some(node) = node {
            self.replica_node.insert((group, replica), node);
        }
    }

    fn vote(
        &mut self,
        node: Option<u64>,
        group: Uuid,
        replica: u64,
        vote: (u64, u64, bool),
        out: &mut Vec<Violation>,
    ) {
        let (term, leader, committed) = vote;
        if let Some(old) = self.votes.insert((group, replica), vote) {
            self.ran(MONOTONIC_VOTE);
            if vote < old {
                out.push((
                    MONOTONIC_VOTE,
                    format!("group {group} replica {replica} saved vote {vote:?} after {old:?}"),
                ));
            }
        }
        // At most one leader per (group, vote). OpenRaft's advanced leader IDs put the
        // Raft node in the vote, so a second claimant is another process with that ID.
        if let (true, true, Some(node)) = (committed, replica == leader, node) {
            self.ran(ELECTION_SAFETY);
            let first = *self.leaders.entry((group, (term, leader))).or_insert(node);
            if first != node {
                out.push((
                    ELECTION_SAFETY,
                    format!("group {group} vote ({term}, {leader}) led by n{first} and n{node}"),
                ));
            }
        }
    }

    fn apply(
        &mut self,
        origin: Origin,
        group: Uuid,
        replica: u64,
        entry: &AppliedPosition,
        digest: Digest,
        out: &mut Vec<Violation>,
    ) {
        let id = point(entry);
        self.ran(MONOTONIC_COMMIT);
        self.ran(LOG_MATCHING);
        if let Some(node) = origin.node {
            let last = self
                .last_applied
                .entry((node, origin.incarnation, (group, replica)));
            match last {
                std::collections::btree_map::Entry::Occupied(mut last) => {
                    if entry.index <= *last.get() {
                        out.push((
                            MONOTONIC_COMMIT,
                            format!(
                                "group {group} replica {replica} on n{node} applied index {} after {} in one incarnation",
                                entry.index,
                                last.get()
                            ),
                        ));
                    }
                    last.insert(entry.index.max(*last.get()));
                }
                std::collections::btree_map::Entry::Vacant(last) => {
                    last.insert(entry.index);
                }
            }
        }
        match self.applied.get(&(group, entry.index)) {
            Some((first, first_digest)) => {
                if (*first, *first_digest) != (id, digest) {
                    out.push((
                        LOG_MATCHING,
                        format!(
                            "group {group} index {}: replica {replica} applied term {} leader {} digest {digest:?}, first apply was term {} leader {} digest {first_digest:?}",
                            entry.index, id.0, id.1, first.0, first.1
                        ),
                    ));
                }
            }
            None => {
                self.applied.insert((group, entry.index), (id, digest));
                let holders: BTreeSet<u64> = self
                    .durable
                    .iter()
                    .filter(|((of, _), log)| *of == group && log.get(&entry.index) == Some(&id))
                    .map(|((_, replica), _)| *replica)
                    .collect();
                // The configuration in force: the newest at or below this index that a
                // holder of the entry has in its own log.
                let voters = self
                    .voters
                    .get(&group)
                    .and_then(|configs| {
                        configs.iter().rev().find(|((index, config), _)| {
                            *index <= entry.index
                                && holders.iter().any(|replica| {
                                    self.durable
                                        .get(&(group, *replica))
                                        .and_then(|log| log.get(index))
                                        == Some(config)
                                })
                        })
                    })
                    .map(|(_, voters)| voters.clone())
                    .unwrap_or_default();
                self.ran(MAJORITY_REACHED);
                if !quorum(&voters, &holders) {
                    out.push((
                        MAJORITY_REACHED,
                        format!(
                            "group {group} index {} (term {}, leader {}) applied while durable on {holders:?} of voters {voters:?}",
                            entry.index, id.0, id.1
                        ),
                    ));
                }
            }
        }
    }

    /// The node's process stopped: its applied positions and ledgers end with it.
    pub fn crashed(&mut self, node: u64) {
        self.native.remove(&node);
        self.pending.retain(|(owner, _), _| *owner != node);
    }

    /// The node's volume was replaced: its replicas lost their votes and logs.
    pub fn wiped(&mut self, node: u64) {
        let lost: Vec<_> = self
            .replica_node
            .iter()
            .filter(|(_, owner)| **owner == node)
            .map(|(replica, _)| *replica)
            .collect();
        for replica in lost {
            self.votes.remove(&replica);
            self.durable.remove(&replica);
        }
    }

    /// Native leases outstanding on `node` in its current incarnation.
    pub fn native_outstanding(&self, node: u64) -> u64 {
        self.native
            .get(&node)
            .map_or(0, |(acquired, released)| acquired.saturating_sub(*released))
    }

    /// RangeClock commits pending on `node`, over all its groups.
    pub fn clock_pending(&self, node: u64) -> u64 {
        self.pending
            .iter()
            .filter(|((owner, _), _)| *owner == node)
            .map(|(_, pending)| *pending)
            .sum()
    }

    /// KeyspaceCoverage: every committed root version must reach pages, as of its own
    /// commit timestamp, that cover the key space once.
    pub fn check_routes(&mut self) -> Vec<Violation> {
        let mut out = Vec::new();
        for (table, versions) in &self.roots {
            for (ts, root) in versions {
                let Some((first, epoch)) = root else {
                    continue;
                };
                *self.exercised.entry(KEYSPACE_COVERAGE).or_default() += 1;
                let (mut spans, mut seen, mut next) = (Vec::new(), BTreeSet::new(), *first);
                let mut broken = None;
                while let Some(page_id) = next {
                    let page = self
                        .pages
                        .get(&(*table, page_id))
                        .and_then(|page| page.range(..=*ts).next_back());
                    match page {
                        Some((_, Some((following, routes)))) if seen.insert(page_id) => {
                            spans.extend(routes.iter().map(|r| (r.start.clone(), r.end.clone())));
                            next = *following;
                        }
                        _ => {
                            broken = Some(page_id);
                            break;
                        }
                    }
                }
                if broken.is_some() || !covers_keyspace(&spans) {
                    out.push((
                        KEYSPACE_COVERAGE,
                        format!("table {table} epoch {epoch} at {ts}: routes {spans:?}, unreadable page {broken:?}"),
                    ));
                }
            }
        }
        out
    }

    /// Committed dedup records, with the virtual time of their first write.
    pub fn dedup_records(&self) -> impl Iterator<Item = (&DedupIdentity, u64)> {
        self.dedup
            .iter()
            .map(|(identity, (_, _, t_ns))| (identity, *t_ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_oltp_types::{sim_probe::LogEntry, types::GroupKind};

    const GROUP: Uuid = Uuid::from_u128(7);

    fn origin(node: u64) -> Origin {
        Origin {
            node: Some(node),
            incarnation: 0,
            t_ns: 0,
            live: true,
        }
    }

    fn at(index: u64, term: u64) -> AppliedPosition {
        AppliedPosition {
            term,
            node_id: 1,
            index,
        }
    }

    fn appended(replica: u64, entries: &[AppliedPosition]) -> Probe {
        Probe::Appended {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica,
            entries: entries
                .iter()
                .map(|at| LogEntry {
                    at: *at,
                    voters: (at.index == 1).then(|| vec![vec![1, 2, 3]]),
                })
                .collect(),
        }
    }

    fn applied(replica: u64, entry: AppliedPosition, digest: u8) -> Probe {
        Probe::Applied {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica,
            entry,
            digest: Digest::from_bytes([digest; 32]),
        }
    }

    fn kinds(violations: &[Violation]) -> Vec<&'static str> {
        violations.iter().map(|(kind, _)| *kind).collect()
    }

    #[test]
    fn a_clean_replicated_apply_passes() {
        let mut protocol = Protocol::new();
        for replica in 1..=3 {
            assert!(
                protocol
                    .observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]))
                    .is_empty()
            );
        }
        for replica in 1..=3 {
            for index in 1..=2 {
                assert!(
                    protocol
                        .observe(origin(replica), &applied(replica, at(index, 1), 9))
                        .is_empty()
                );
            }
        }
    }

    #[test]
    fn applying_one_index_twice_in_an_incarnation_violates_monotonic_commit() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &appended(1, &[at(1, 1)]));
        protocol.observe(origin(2), &appended(2, &[at(1, 1)]));
        assert!(
            protocol
                .observe(origin(1), &applied(1, at(1, 1), 9))
                .is_empty()
        );
        let twice = protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        assert_eq!(kinds(&twice), [MONOTONIC_COMMIT]);
        // A later incarnation may re-apply what its unsynced state lost.
        let restarted = Origin {
            incarnation: 1,
            ..origin(1)
        };
        assert!(
            protocol
                .observe(restarted, &applied(1, at(1, 1), 9))
                .is_empty()
        );
    }

    #[test]
    fn diverging_applies_violate_log_matching() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &appended(1, &[at(1, 1)]));
        protocol.observe(origin(2), &appended(2, &[at(1, 1)]));
        protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        let other = protocol.observe(origin(2), &applied(2, at(1, 1), 8));
        assert_eq!(kinds(&other), [LOG_MATCHING]);
    }

    #[test]
    fn an_apply_without_a_durable_majority_violates_majority_reached() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &appended(1, &[at(1, 1)]));
        protocol.observe(origin(2), &appended(2, &[at(1, 2)]));
        protocol.observe(origin(3), &appended(3, &[at(1, 2)]));
        let early = protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        assert_eq!(kinds(&early), [MAJORITY_REACHED]);
    }

    #[test]
    fn truncating_an_applied_entry_violates_majority_reached() {
        let mut protocol = Protocol::new();
        for replica in 1..=2 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]));
        }
        protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        let truncate = |after| Probe::Truncated {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica: 2,
            after,
        };
        // Index 2 was never applied: removing it is a normal conflict repair.
        assert!(
            protocol
                .observe(origin(2), &truncate(Some(at(1, 1))))
                .is_empty()
        );
        assert_eq!(
            kinds(&protocol.observe(origin(2), &truncate(None))),
            [MAJORITY_REACHED]
        );
    }

    #[test]
    fn an_apply_on_one_of_three_voters_fails_when_no_other_replica_was_seen() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &appended(1, &[at(1, 1)]));
        let alone = protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        assert_eq!(kinds(&alone), [MAJORITY_REACHED]);
    }

    #[test]
    fn votes_never_move_back_and_one_node_leads_a_vote() {
        let mut protocol = Protocol::new();
        let vote = |term, committed| Probe::VoteSaved {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica: 1,
            term,
            leader: 1,
            committed,
        };
        assert!(protocol.observe(origin(1), &vote(2, true)).is_empty());
        let back = protocol.observe(origin(1), &vote(1, false));
        assert_eq!(kinds(&back), [MONOTONIC_VOTE]);
        let twin = protocol.observe(origin(4), &vote(2, true));
        assert_eq!(kinds(&twin), [ELECTION_SAFETY]);
    }

    #[test]
    fn dedup_records_agree_and_reads_fence_commits() {
        let mut protocol = Protocol::new();
        let dedup = |commit_ts, digest| Probe::DedupApplied {
            group: GROUP,
            table: Uuid::from_u128(1),
            anchor: Key::Int64(5),
            request_id: Uuid::from_u128(2),
            commit_ts,
            digest: Digest::from_bytes([digest; 32]),
        };
        assert!(protocol.observe(origin(1), &dedup(10, 1)).is_empty());
        assert!(protocol.observe(origin(2), &dedup(10, 1)).is_empty());
        assert_eq!(
            kinds(&protocol.observe(origin(3), &dedup(11, 1))),
            [DEDUP_UNIQUE]
        );

        protocol.observe(
            origin(1),
            &Probe::SnapshotServed {
                group: GROUP,
                read_ts: 20,
            },
        );
        let proposed = |commit_ts| Probe::DirectProposed {
            group: GROUP,
            commit_ts,
            term: 1,
        };
        assert!(protocol.observe(origin(2), &proposed(21)).is_empty());
        assert_eq!(
            kinds(&protocol.observe(origin(2), &proposed(20))),
            [RANGE_CLOCK_RULE1]
        );
    }

    #[test]
    fn committed_routes_must_cover_the_key_space() {
        let (table, page_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let span = |start: Option<i64>, end: Option<i64>| Span {
            start: start.map(Key::Int64),
            end: end.map(Key::Int64),
        };
        let publish = |commit_ts, record| Probe::RouteEpochPublished {
            table,
            commit_ts,
            record,
        };
        let root = || RouteRecord::Root {
            first_page: Some(page_id),
            routing_epoch: 1,
        };
        let page = |spans| RouteRecord::Page {
            page_id,
            next_page: None,
            spans,
        };
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &publish(5, page(vec![span(None, None)])));
        protocol.observe(origin(1), &publish(5, root()));
        assert!(protocol.check_routes().is_empty());
        let gap = vec![span(None, Some(5)), span(Some(6), None)];
        protocol.observe(origin(1), &publish(9, page(gap)));
        protocol.observe(origin(1), &publish(9, root()));
        assert_eq!(kinds(&protocol.check_routes()), [KEYSPACE_COVERAGE]);
        // A root whose page never committed is a hole, not a pass.
        let mut missing = Protocol::new();
        missing.observe(origin(1), &publish(5, root()));
        assert_eq!(kinds(&missing.check_routes()), [KEYSPACE_COVERAGE]);
    }

    #[test]
    fn native_leases_are_released_once_and_reset_by_a_crash() {
        let mut protocol = Protocol::new();
        let acquire = Probe::GuardAcquired {
            class: "query",
            outstanding: 1,
        };
        let release = Probe::GuardReleased { outstanding: 0 };
        protocol.observe(origin(1), &acquire);
        assert_eq!(protocol.native_outstanding(1), 1);
        assert!(protocol.observe(origin(1), &release).is_empty());
        assert_eq!(
            kinds(&protocol.observe(origin(1), &release)),
            [EXACTLY_ONCE_RELEASE]
        );
        protocol.crashed(1);
        protocol.observe(origin(1), &acquire);
        let dead = Origin {
            live: false,
            ..origin(1)
        };
        assert!(protocol.observe(dead, &release).is_empty());
        assert_eq!(protocol.native_outstanding(1), 1);
    }
}
