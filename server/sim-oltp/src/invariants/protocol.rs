//! Probe-driven protocol invariants (design section 3.7).
//!
//! [`Protocol`] is a pure state machine over the probe stream: the hub feeds it each probe
//! with its node, incarnation and virtual time, and it returns the violations that probe
//! alone decides. It never calls product code. Quiescence and history checks read its
//! state afterwards.
//!
//! Replicas are keyed by `(group, replica)`, the group ID and the Raft node ID inside it.
//! Votes are durable and are never forgotten, not even by a wipe: design 3.7 exempts
//! nothing. Synced log entries survive a crash only if the `Recovered` probe finds them on
//! the volume at the next start, and a wipe drops them. Applied positions belong to one
//! process incarnation.

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
pub const APPLIED_PREFIXES_AGREE: &str = "AppliedPrefixesAgree";
pub const MAJORITY_REACHED: &str = "MajorityReached";
pub const TRUNCATION: &str = "MajorityReached/truncation";
pub const REBUILD: &str = "MajorityReached/rebuild";
pub const DEDUP_UNIQUE: &str = "DedupUnique";
pub const NOT_APPLIED_NEVER_COMMITTED: &str = "NotAppliedNeverCommitted";
pub const RANGE_CLOCK_RULE1: &str = "RangeClockRule1";
pub const KEYSPACE_COVERAGE: &str = "KeyspaceCoverage";
pub const EXACTLY_ONCE_RELEASE: &str = "ExactlyOnceRelease";
pub const RESOURCE_BASELINE: &str = "ResourceBaseline";

/// Every invariant the probe stream checks, for the run's assertion map.
pub const PROBE_INVARIANTS: [&str; 12] = [
    ELECTION_SAFETY,
    MONOTONIC_VOTE,
    MONOTONIC_COMMIT,
    LOG_MATCHING,
    APPLIED_PREFIXES_AGREE,
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

/// A committed request's identity, as the history names it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DedupIdentity {
    pub table: Uuid,
    pub key: OrdKey,
    pub request_id: Uuid,
}

/// One worker's ledgers, by ledger and group, as its last snapshot reported them.
pub type Ledgers = BTreeMap<(Ledger, Option<Uuid>), u64>;

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
    /// Per replica: the index up to which its log was purged when its core last started.
    purged: BTreeMap<Replica, u64>,
    /// Per group and index: the first applied log id and digest.
    applied: BTreeMap<(Uuid, u64), (LogPoint, Digest)>,
    /// Per node incarnation and replica: the last applied index.
    last_applied: BTreeMap<(u64, u64, Replica), u64>,
    /// Where each replica runs.
    replica_node: BTreeMap<Replica, u64>,
    /// Identities whose committed entry some replica applied, and when the first did.
    committed: BTreeMap<DedupIdentity, u64>,
    /// Per node and group: dedup records written since its last `Applied`.
    staged: BTreeMap<(u64, Uuid), Vec<(DedupIdentity, u64, Digest)>>,
    /// The first visible dedup record per identity.
    dedup: BTreeMap<DedupIdentity, (u64, Digest)>,
    /// Dedup records made visible, per node incarnation.
    dedup_seen: BTreeSet<(u64, u64, DedupIdentity)>,
    /// Per group: the highest snapshot timestamp served on any replica.
    served: BTreeMap<Uuid, u64>,
    /// Per table: committed routing roots and pages by commit timestamp. `None` is a delete.
    roots: BTreeMap<Uuid, BTreeMap<u64, Option<Root>>>,
    pages: BTreeMap<(Uuid, Uuid), BTreeMap<u64, Option<Page>>>,
    /// How often each rule ran on an event of its own.
    exercised: BTreeMap<&'static str, u64>,
    /// Per live node: native leases acquired and released by this incarnation.
    native: BTreeMap<u64, (u64, u64)>,
    /// Per node: its last ledger snapshot.
    ledgers: BTreeMap<u64, Ledgers>,
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
            Probe::Recovered {
                group,
                replica,
                purged,
                entries,
                ..
            } => {
                self.place(*group, *replica, node);
                self.recovered(
                    *group,
                    *replica,
                    purged.map(|at| at.index),
                    entries.iter().map(|at| (at.index, point(at))).collect(),
                    &mut out,
                );
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
                self.ran(TRUNCATION);
                for (index, id) in removed {
                    if self.is_applied(*group, index, id) {
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
                self.publish_dedup(origin, *group, &mut out);
            }
            Probe::LeaderSeen {
                group,
                term,
                leader,
                ..
            } => self.leads(node, *group, (*term, *leader), &mut out),
            Probe::DedupApplied {
                group,
                table,
                anchor,
                request_id,
                commit_ts,
                digest,
            } => {
                let identity = DedupIdentity {
                    table: *table,
                    key: OrdKey::new(anchor),
                    request_id: *request_id,
                };
                self.committed
                    .entry(identity.clone())
                    .or_insert(origin.t_ns);
                if let Some(node) = node {
                    // A retried apply transaction writes the same record again before its
                    // entry's Applied, so one staged record stands for both.
                    let record = (identity, *commit_ts, *digest);
                    let staged = self.staged.entry((node, *group)).or_default();
                    if !staged.contains(&record) {
                        staged.push(record);
                    }
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
            Probe::WritesResolved { commit_ts } => {
                let tables: Vec<Uuid> = self
                    .roots
                    .iter()
                    .filter(|(_, versions)| versions.contains_key(commit_ts))
                    .map(|(table, _)| *table)
                    .collect();
                for table in tables {
                    out.extend(self.check_route(table, *commit_ts));
                }
            }
            Probe::LedgerSnapshot {
                ledger,
                group,
                outstanding,
            } => {
                if let Some(node) = node {
                    *self
                        .ledgers
                        .entry(node)
                        .or_default()
                        .entry((*ledger, *group))
                        .or_default() += outstanding;
                }
            }
            Probe::GuardAcquired { .. } => {
                if let Some(node) = node {
                    self.native.entry(node).or_default().0 += 1;
                }
            }
            Probe::GuardReleased => {
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
            Probe::GatewayAttempt { .. }
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
        if committed && replica == leader {
            self.leads(node, group, (term, leader), out);
        }
    }

    /// ElectionSafety: `node` claims to lead `vote`. OpenRaft's advanced leader IDs put the
    /// Raft node in the vote, so a second claimant is another process with that ID.
    fn leads(&mut self, node: Option<u64>, group: Uuid, vote: LogPoint, out: &mut Vec<Violation>) {
        let Some(node) = node else { return };
        self.ran(ELECTION_SAFETY);
        let first = *self.leaders.entry((group, vote)).or_insert(node);
        if first != node {
            out.push((
                ELECTION_SAFETY,
                format!("group {group} vote {vote:?} led by n{first} and n{node}"),
            ));
        }
    }

    fn is_applied(&self, group: Uuid, index: u64, id: LogPoint) -> bool {
        self.applied
            .get(&(group, index))
            .is_some_and(|(applied, _)| *applied == id)
    }

    /// True when `replica` holds the entry: in its log, or purged below its state machine.
    fn holds(&self, replica: &Replica, index: u64, id: LogPoint) -> bool {
        self.durable
            .get(replica)
            .is_some_and(|log| log.get(&index) == Some(&id))
            || self
                .purged
                .get(replica)
                .is_some_and(|purged| index <= *purged)
    }

    /// MajorityReached for an applied entry. The configurations that may have committed it
    /// are the one in force at its index and any later one: a leader counts with its
    /// newest appended configuration. Each must itself be applied, or be in a holder's log.
    fn majority(&self, group: Uuid, index: u64, id: LogPoint, out: &mut Vec<Violation>) {
        let replicas = || self.durable.keys().filter(|(of, _)| *of == group);
        let holders: BTreeSet<u64> = replicas()
            .filter(|replica| self.holds(replica, index, id))
            .map(|(_, replica)| *replica)
            .collect();
        let known: Vec<(u64, &Vec<Vec<u64>>)> = self
            .voters
            .get(&group)
            .into_iter()
            .flatten()
            .filter(|((at, config), _)| {
                self.is_applied(group, *at, *config)
                    || holders
                        .iter()
                        .any(|holder| self.holds(&(group, *holder), *at, *config))
            })
            .map(|((at, _), voters)| (*at, voters))
            .collect();
        let in_force = known.iter().rev().find(|(at, _)| *at <= index);
        let later = known.iter().filter(|(at, _)| *at > index);
        if !in_force
            .into_iter()
            .chain(later)
            .any(|(_, voters)| quorum(voters, &holders))
        {
            out.push((
                MAJORITY_REACHED,
                format!(
                    "group {group} index {index} (term {}, leader {}) applied but durable on {holders:?}, configurations {known:?}",
                    id.0, id.1
                ),
            ));
        }
    }

    /// A core started on the log found on its volume.
    fn recovered(
        &mut self,
        group: Uuid,
        replica: u64,
        purged: Option<u64>,
        entries: BTreeMap<u64, LogPoint>,
        out: &mut Vec<Violation>,
    ) {
        let key = (group, replica);
        self.ran(REBUILD);
        if let Some(purged) = purged {
            self.purged.insert(key, purged);
        }
        let old = self.durable.insert(key, entries).unwrap_or_default();
        for (index, id) in old {
            if !self.holds(&key, index, id) && self.is_applied(group, index, id) {
                self.majority(group, index, id, out);
            }
        }
    }

    /// DedupUnique: the entry that wrote the staged records applied, so they are visible.
    fn publish_dedup(&mut self, origin: Origin, group: Uuid, out: &mut Vec<Violation>) {
        let Some(node) = origin.node else { return };
        for (identity, commit_ts, digest) in self.staged.remove(&(node, group)).unwrap_or_default()
        {
            self.ran(DEDUP_UNIQUE);
            let DedupIdentity {
                table,
                key,
                request_id,
            } = &identity;
            let record = *self
                .dedup
                .entry(identity.clone())
                .or_insert((commit_ts, digest));
            if record != (commit_ts, digest) {
                out.push((
                    DEDUP_UNIQUE,
                    format!(
                        "table {table} key {key:?} request {request_id}: dedup record at {commit_ts} differs from the one at {}",
                        record.0
                    ),
                ));
            } else if !self
                .dedup_seen
                .insert((node, origin.incarnation, identity.clone()))
            {
                out.push((
                    DEDUP_UNIQUE,
                    format!(
                        "table {table} key {key:?} request {request_id}: n{node} wrote a second dedup record in one incarnation"
                    ),
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
        self.ran(APPLIED_PREFIXES_AGREE);
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
            Some((first, _)) if *first != id => out.push((
                LOG_MATCHING,
                format!(
                    "group {group} index {}: replica {replica} applied term {} leader {}, first apply was term {} leader {}",
                    entry.index, id.0, id.1, first.0, first.1
                ),
            )),
            Some((_, first)) if *first != digest => out.push((
                APPLIED_PREFIXES_AGREE,
                format!(
                    "group {group} index {}: replica {replica} applied digest {digest:?}, first apply had {first:?}",
                    entry.index
                ),
            )),
            Some(_) => {}
            None => {
                self.applied.insert((group, entry.index), (id, digest));
                self.ran(MAJORITY_REACHED);
                self.majority(group, entry.index, id, out);
            }
        }
    }

    /// The node's process stopped: its unpublished dedup records and ledgers end with it.
    pub fn crashed(&mut self, node: u64) {
        self.native.remove(&node);
        self.ledgers.remove(&node);
        self.staged.retain(|(owner, _), _| *owner != node);
    }

    /// The node's volume was replaced: its replicas lost their logs. A vote the old
    /// volume saved still binds the replica (MonotonicVote has no exemption).
    pub fn wiped(&mut self, node: u64) {
        let lost: Vec<_> = self
            .replica_node
            .iter()
            .filter(|(_, owner)| **owner == node)
            .map(|(replica, _)| *replica)
            .collect();
        for replica in lost {
            self.durable.remove(&replica);
            self.purged.remove(&replica);
        }
    }

    /// Native leases outstanding on `node` in its current incarnation.
    pub fn native_outstanding(&self, node: u64) -> u64 {
        self.native
            .get(&node)
            .map_or(0, |(acquired, released)| acquired.saturating_sub(*released))
    }

    /// Starts a new ledger snapshot of `node`.
    pub fn clear_ledgers(&mut self, node: u64) {
        self.ledgers.remove(&node);
    }

    /// The ledgers `node` reported since [`Self::clear_ledgers`].
    pub fn ledgers(&self, node: u64) -> Ledgers {
        self.ledgers.get(&node).cloned().unwrap_or_default()
    }

    /// KeyspaceCoverage: the root of `table` committed at `ts` must reach pages, as of that
    /// timestamp, that cover the key space once. `WritesResolved` triggers it, when every
    /// record of the commit is in place.
    fn check_route(&mut self, table: Uuid, ts: u64) -> Option<Violation> {
        let Some(Some((first, epoch))) = self.roots.get(&table)?.get(&ts) else {
            return None;
        };
        let (mut spans, mut seen, mut next) = (Vec::new(), BTreeSet::new(), *first);
        let mut broken = None;
        while let Some(page_id) = next {
            let page = self
                .pages
                .get(&(table, page_id))
                .and_then(|page| page.range(..=ts).next_back());
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
        let violation = (broken.is_some() || !covers_keyspace(&spans)).then(|| {
            (
                KEYSPACE_COVERAGE,
                format!("table {table} epoch {epoch} at {ts}: routes {spans:?}, unreadable page {broken:?}"),
            )
        });
        self.ran(KEYSPACE_COVERAGE);
        violation
    }

    /// Checks every root of the run again at its end, also those of a commit whose last
    /// write never resolved.
    pub fn check_routes(&mut self) -> Vec<Violation> {
        let roots: Vec<(Uuid, u64)> = self
            .roots
            .iter()
            .flat_map(|(table, versions)| versions.keys().map(|ts| (*table, *ts)))
            .collect();
        roots
            .into_iter()
            .filter_map(|(table, ts)| self.check_route(table, ts))
            .collect()
    }

    /// Identities of applied direct commits, with the virtual time of the first apply.
    pub fn committed(&self) -> impl Iterator<Item = (&DedupIdentity, u64)> {
        self.committed
            .iter()
            .map(|(identity, t_ns)| (identity, *t_ns))
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
        assert_eq!(kinds(&other), [APPLIED_PREFIXES_AGREE]);
        let other = protocol.observe(origin(3), &applied(3, at(1, 2), 9));
        assert_eq!(kinds(&other), [LOG_MATCHING]);
    }

    fn recovered(
        replica: u64,
        purged: Option<AppliedPosition>,
        entries: &[AppliedPosition],
    ) -> Probe {
        Probe::Recovered {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica,
            purged,
            entries: entries.to_vec(),
        }
    }

    #[test]
    fn logs_rebuilt_from_the_volume_must_keep_a_majority() {
        let mut protocol = Protocol::new();
        let log = [at(1, 1), at(2, 1)];
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &log));
        }
        protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        protocol.observe(origin(1), &applied(1, at(2, 1), 9));
        // Purged entries are in the state machine, and a two of three majority remains.
        assert!(
            protocol
                .observe(origin(1), &recovered(1, Some(at(2, 1)), &[]))
                .is_empty()
        );
        assert!(
            protocol
                .observe(origin(2), &recovered(2, None, &log[..1]))
                .is_empty()
        );
        let lost = protocol.observe(origin(3), &recovered(3, None, &log[..1]));
        assert_eq!(kinds(&lost), [MAJORITY_REACHED]);
    }

    #[test]
    fn a_later_configuration_may_commit_an_earlier_entry() {
        let member = |index, voters| Probe::Appended {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica: 4,
            entries: vec![LogEntry {
                at: at(index, 1),
                voters: Some(voters),
            }],
        };
        let mut protocol = Protocol::new();
        protocol.observe(origin(4), &member(1, vec![vec![1, 2, 3], vec![4]]));
        protocol.observe(origin(4), &appended(4, &[at(2, 1)]));
        // Only the joint configuration is known: replica 4 alone is no majority of 1, 2, 3.
        let mut joint = Protocol::new();
        joint.observe(origin(4), &member(1, vec![vec![1, 2, 3], vec![4]]));
        joint.observe(origin(4), &appended(4, &[at(2, 1)]));
        let early = joint.observe(origin(4), &applied(4, at(2, 1), 9));
        assert_eq!(kinds(&early), [MAJORITY_REACHED]);
        // Its leader already appended the uniform configuration and counts with it.
        protocol.observe(origin(4), &member(3, vec![vec![4]]));
        assert!(
            protocol
                .observe(origin(4), &applied(4, at(2, 1), 9))
                .is_empty()
        );
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
        let seen = |leader| Probe::LeaderSeen {
            group: GROUP,
            replica: leader,
            term: 2,
            leader,
        };
        assert!(protocol.observe(origin(1), &seen(1)).is_empty());
        assert_eq!(
            kinds(&protocol.observe(origin(5), &seen(1))),
            [ELECTION_SAFETY]
        );
    }

    #[test]
    fn a_wipe_drops_logs_but_not_votes() {
        let mut protocol = Protocol::new();
        let vote = |term| Probe::VoteSaved {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica: 1,
            term,
            leader: 2,
            committed: false,
        };
        for replica in 1..=2 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1)]));
        }
        protocol.observe(origin(1), &vote(5));
        protocol.wiped(1);
        assert_eq!(
            kinds(&protocol.observe(origin(1), &vote(4))),
            [MONOTONIC_VOTE]
        );
        assert_eq!(
            kinds(&protocol.observe(origin(2), &applied(2, at(1, 1), 9))),
            [MAJORITY_REACHED]
        );
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
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]));
        }
        let mut write = |node, index, commit_ts, attempts| {
            for _ in 0..attempts {
                assert!(
                    protocol
                        .observe(origin(node), &dedup(commit_ts, 1))
                        .is_empty()
                );
            }
            kinds(&protocol.observe(origin(node), &applied(node, at(index, 1), 9)))
        };
        // A retried transaction is one record.
        assert!(write(1, 1, 10, 2).is_empty());
        assert!(write(2, 1, 10, 1).is_empty());
        assert_eq!(write(3, 1, 11, 1), [DEDUP_UNIQUE]);
        // The same record from a second entry of one incarnation is a second record.
        assert_eq!(write(1, 2, 10, 1), [DEDUP_UNIQUE]);
        assert_eq!(protocol.committed().count(), 1);

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
        let resolved = |commit_ts| Probe::WritesResolved { commit_ts };
        protocol.observe(origin(1), &publish(5, root()));
        protocol.observe(origin(1), &publish(5, page(vec![span(None, None)])));
        assert!(protocol.observe(origin(1), &resolved(5)).is_empty());
        assert!(protocol.check_routes().is_empty());
        let gap = vec![span(None, Some(5)), span(Some(6), None)];
        protocol.observe(origin(1), &publish(9, root()));
        protocol.observe(origin(1), &publish(9, page(gap)));
        assert_eq!(
            kinds(&protocol.observe(origin(1), &resolved(9))),
            [KEYSPACE_COVERAGE]
        );
        assert_eq!(kinds(&protocol.check_routes()), [KEYSPACE_COVERAGE]);
        // A root whose page never committed is a hole, not a pass.
        let mut missing = Protocol::new();
        missing.observe(origin(1), &publish(5, root()));
        assert_eq!(kinds(&missing.check_routes()), [KEYSPACE_COVERAGE]);
    }

    #[test]
    fn native_leases_are_released_once_and_reset_by_a_crash() {
        let mut protocol = Protocol::new();
        let acquire = Probe::GuardAcquired { class: "query" };
        let release = Probe::GuardReleased;
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

    #[test]
    fn a_ledger_snapshot_sums_the_owners_of_one_node() {
        let mut protocol = Protocol::new();
        let held = |outstanding| Probe::LedgerSnapshot {
            ledger: Ledger::ComputeGuards,
            group: None,
            outstanding,
        };
        protocol.observe(origin(1), &held(1));
        protocol.observe(origin(1), &held(2));
        protocol.observe(origin(2), &held(9));
        let key = (Ledger::ComputeGuards, None);
        assert_eq!(protocol.ledgers(1), Ledgers::from([(key, 3)]));
        protocol.clear_ledgers(1);
        assert!(protocol.ledgers(1).is_empty());
    }
}
