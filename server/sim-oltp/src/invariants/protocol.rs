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
//!
//! # What each rule cannot see
//!
//! No rule sees a step that emits no probe, and none runs before its probe is emitted. A
//! rule that [`Protocol::exercised`] does not name compared nothing in that run.
//!
//! - ElectionSafety: OpenRaft's leader IDs name the Raft node, so a second leader of one vote
//!   is seen only as a second simulated node. Two cores of one replica in one process, and a
//!   leader that takes over again after a restart, look like one leader. `LeaderSeen` covers
//!   Data groups only.
//! - MonotonicVote: the vote a replica reads back at a start is not compared, only the votes
//!   it saves. A vote lost with its volume is seen once the replica saves a lower one.
//! - MonotonicCommit: the applied index, within one incarnation. A commit marker that moves
//!   back without an apply emits no probe. An incarnation ends at a crash or at the restart
//!   after a drain, so a core that stops and starts inside one process stays in it.
//! - LogMatching, AppliedPrefixesAgree: entries some replica applied. What an apply wrote is
//!   not compared, except the dedup records; state that arrives by snapshot, split import or
//!   backup is not compared at all.
//! - MajorityReached: at the first apply, configurations the run appended and the log copies
//!   synced by then. A later configuration that only a holder of the entry has appended
//!   counts, as it does for a leader. At a start, the newest applied configuration, and a
//!   newer one that committed the entry itself: entries another replica synced but nobody
//!   applied are not protected, a loss between two starts is seen at the next start in the
//!   group, and a purged entry counts as held while its purge marker matches an applied
//!   entry, whatever the state machine holds. A configuration the run did not append, as in a
//!   restored group, is unknown, and then nothing has a majority. A wipe of one volume counts
//!   for every replica of its node until they start again. No wipe is exempt, although the
//!   loss of a volume is the fault plan's work.
//! - MajorityReached/truncation: a removed or overwritten entry that some replica applied
//!   before. One applied only afterwards is left to LogMatching.
//! - DedupUnique: records written by applied direct entries. Records that arrive by snapshot,
//!   split import or backup, forward markers and queued requests are not seen, and neither is
//!   a record that disappears. Retention is not modelled: after it expires, 24 hours by
//!   default, a legal second execution is red. The records of a retried transaction, and of two
//!   replicas on one node that apply an entry at the same time, are compared with the others'
//!   one by one, not as a set, and either of those two replicas can be named in a report.
//! - NotAppliedNeverCommitted: direct commits whose dedup record some replica wrote. Queued
//!   commits and commits without a record are not seen.
//! - RangeClockRule1: committed entries of the direct path, by the time they registered. A
//!   read counts once the range admits it, so one that fails later counts as completed. A
//!   commit that never registered is not judged, and one that registered before a read but
//!   was proposed after it counts as proposed before. A split's children inherit the reads of
//!   their source once a Catalog replica resolved the new routes, and an earlier split is
//!   unknown if its routes arrived by snapshot. A latest read (`u64::MAX`) has no timestamp.
//!   Reads that do not take the range clock, as on the queued write path, are not seen.
//! - KeyspaceCoverage: routes that a Catalog replica resolved from their transaction, at each
//!   root. Routes that arrive by snapshot, and a page rewritten without a new root, are not
//!   checked: only a new root publishes an epoch.
//! - ExactlyOnceRelease and ReleaseAfterWork: see [`super::resources`].
//! - ResourceBaseline: see [`super::Invariants::check_resources`].

use std::collections::{BTreeMap, BTreeSet};

use nabedra_oltp_types::{
    sim_probe::{Ledger, Probe, RouteRecord, Span},
    types::{AppliedPosition, Digest},
};
use nabedra_protocol::protocol::Key;
use uuid::Uuid;

use super::resources::{Guards, OVERLAY_BASELINE, RELEASE_AFTER_WORK};

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
pub const PROBE_INVARIANTS: [&str; 14] = [
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
    RELEASE_AFTER_WORK,
    RESOURCE_BASELINE,
    OVERLAY_BASELINE,
];

pub type Violation = (&'static str, String);

/// Where and when a probe was emitted. `live` is false for a probe a destructor emits
/// after its node was cut: it belongs to a dead incarnation and changes no state. `thread`
/// tells apart jobs that a storage pause interleaves: each runs on one thread to its end.
#[derive(Clone, Copy, Debug)]
pub struct Origin {
    pub node: Option<u64>,
    pub incarnation: u64,
    pub t_ns: u64,
    pub live: bool,
    pub thread: u64,
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
    group: Uuid,
    start: Option<OrdKey>,
    end: Option<OrdKey>,
}

impl RouteSpan {
    fn new(span: &Span) -> Self {
        Self {
            group: span.group,
            start: span.start.as_ref().map(OrdKey::new),
            end: span.end.as_ref().map(OrdKey::new),
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        let before = |end: &Option<OrdKey>, start: &Option<OrdKey>| match (end, start) {
            (Some(end), Some(start)) => end <= start,
            _ => false,
        };
        !before(&self.end, &other.start) && !before(&other.end, &self.start)
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

/// A dedup record: the identity, its commit timestamp and its digest.
type DedupRecord = (DedupIdentity, u64, Digest);

/// A direct entry on one node: the node, its group and the log index.
type DirectEntry = (u64, Uuid, u64);

/// A group's applied entry.
struct AppliedEntry {
    id: LogPoint,
    digest: Digest,
    /// The newest configuration with a majority of it at its first apply.
    committed_by: Option<(u64, LogPoint)>,
}

/// A registered direct commit.
struct Proposal {
    term: u64,
    /// The position of its registration in the probe stream.
    seq: u64,
}

type Root = (Option<Uuid>, u64);
type Page = (Option<Uuid>, Vec<RouteSpan>);

#[derive(Default)]
pub struct Protocol {
    /// Probes observed so far.
    seq: u64,
    /// Per replica: its newest durable vote `(term, leader, committed)`.
    votes: BTreeMap<Replica, (u64, u64, bool)>,
    /// Per group and committed vote: the simulated node that claimed it as leader.
    leaders: BTreeMap<(Uuid, LogPoint), u64>,
    /// Per group: voter configurations by log index and log id.
    voters: BTreeMap<Uuid, BTreeMap<(u64, LogPoint), Vec<Vec<u64>>>>,
    /// Per replica: its durable log, index to log id.
    durable: BTreeMap<Replica, BTreeMap<u64, LogPoint>>,
    /// Per replica: the entry up to which its log is purged.
    purged: BTreeMap<Replica, (u64, LogPoint)>,
    /// Per group: its applied entries, as first applied.
    applied: BTreeMap<Uuid, BTreeMap<u64, AppliedEntry>>,
    /// Entries already reported as lost.
    lost: BTreeSet<(Uuid, u64)>,
    /// Per node incarnation and replica: the last applied index.
    last_applied: BTreeMap<(u64, u64, Replica), u64>,
    /// Where each replica runs.
    replica_node: BTreeMap<Replica, u64>,
    /// Identities whose committed entry some replica applied, and when the first did.
    committed: BTreeMap<DedupIdentity, u64>,
    /// Transactions started to apply a direct entry.
    attempts: u64,
    /// Per thread: the direct entry its last transaction started to apply, and the attempt.
    applying: BTreeMap<u64, (DirectEntry, u64)>,
    /// Per direct entry: the dedup records that each attempt to apply it wrote and that are
    /// not published yet. Two replicas of a group can share a node, so each `Applied`
    /// publishes one attempt.
    staged: BTreeMap<DirectEntry, BTreeMap<u64, Vec<DedupRecord>>>,
    /// Per identity: the entry of its first visible dedup record, and the record.
    dedup: BTreeMap<DedupIdentity, (Uuid, u64, u64, Digest)>,
    /// Per direct entry of a group: the identities its first applied transaction recorded.
    recorded: BTreeMap<(Uuid, u64), BTreeSet<DedupIdentity>>,
    /// Dedup records made visible, per node incarnation and replica.
    dedup_seen: BTreeSet<(u64, u64, Replica, DedupIdentity)>,
    /// Per group: the highest snapshot timestamp served on any replica, each time it rose.
    served: BTreeMap<Uuid, Vec<(u64, u64)>>,
    /// Per group and commit timestamp: the registrations of a direct commit.
    proposals: BTreeMap<(Uuid, u64), Vec<Proposal>>,
    /// Committed direct commits by group and timestamp: where they registered, and whether
    /// RangeClockRule1 reported them.
    direct: BTreeMap<(Uuid, u64), (u64, bool)>,
    /// Per group that a split created: its source, and where in the stream the source's
    /// reads stop counting. `None` while no route names the source any more.
    sources: BTreeMap<Uuid, BTreeMap<Uuid, Option<u64>>>,
    /// Per table: committed routing roots and pages by commit timestamp. `None` is a delete.
    roots: BTreeMap<Uuid, BTreeMap<u64, Option<Root>>>,
    pages: BTreeMap<(Uuid, Uuid), BTreeMap<u64, Option<Page>>>,
    /// How often each rule ran on an event of its own.
    exercised: BTreeMap<&'static str, u64>,
    /// Native leases and pending Raft log writes.
    guards: Guards,
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

    pub(crate) fn ran(&mut self, rule: &'static str, times: u64) {
        *self.exercised.entry(rule).or_default() += times;
    }

    /// Checks `probe` and updates the state. Returns what it violates.
    pub fn observe(&mut self, origin: Origin, probe: &Probe) -> Vec<Violation> {
        let mut out = Vec::new();
        if !origin.live {
            return out;
        }
        let node = origin.node;
        self.seq += 1;
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
                for entry in entries {
                    let (index, id) = (entry.at.index, point(&entry.at));
                    let log = self.durable.entry((*group, *replica)).or_default();
                    if let Some(old) = log.insert(index, id).filter(|old| *old != id) {
                        self.removed(*group, *replica, "overwrote", index, old, &mut out);
                    }
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
                    purged.map(|at| (at.index, point(&at))),
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
                for (index, id) in removed {
                    self.removed(*group, *replica, "truncated", index, id, &mut out);
                }
            }
            Probe::Purged {
                group,
                replica,
                upto,
                ..
            } => {
                self.place(*group, *replica, node);
                self.purged
                    .insert((*group, *replica), (upto.index, point(upto)));
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
                self.publish_dedup(origin, (*group, *replica), entry, &mut out);
            }
            Probe::DirectApplying { group, index } => {
                if let Some(node) = node {
                    let entry = (node, *group, *index);
                    self.attempts += 1;
                    self.applying.insert(origin.thread, (entry, self.attempts));
                    self.staged
                        .entry(entry)
                        .or_default()
                        .insert(self.attempts, Vec::new());
                }
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
                let applying = self.applying.get(&origin.thread);
                if let Some((entry, attempt)) = applying
                    && node == Some(entry.0)
                    && *group == entry.1
                    && let Some(records) = self
                        .staged
                        .get_mut(entry)
                        .and_then(|attempts| attempts.get_mut(attempt))
                {
                    records.push((identity, *commit_ts, *digest));
                }
            }
            Probe::SnapshotServed { group, read_ts } => {
                let served = self.served.entry(*group).or_default();
                if served.last().is_none_or(|(_, highest)| highest < read_ts) {
                    served.push((self.seq, *read_ts));
                }
            }
            Probe::DirectProposed {
                group,
                commit_ts,
                term,
            } => self
                .proposals
                .entry((*group, *commit_ts))
                .or_default()
                .push(Proposal {
                    term: *term,
                    seq: self.seq,
                }),
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
                    self.learn_splits(table, &mut out);
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
            Probe::GuardAcquired { .. }
            | Probe::GuardReleased { .. }
            | Probe::WorkStarted { .. }
            | Probe::WorkDone { .. } => {
                match probe {
                    Probe::GuardReleased { .. } => self.ran(EXACTLY_ONCE_RELEASE, 1),
                    Probe::WorkDone { .. } => self.ran(RELEASE_AFTER_WORK, 1),
                    _ => {}
                }
                out.extend(self.guards.observe(node, origin.t_ns, probe));
            }
            Probe::GatewayAttempt { .. } | Probe::QueueCopy { .. } | Probe::Receipt { .. } => {}
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
            self.ran(MONOTONIC_VOTE, 1);
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
        self.ran(ELECTION_SAFETY, 1);
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
            .get(&group)
            .and_then(|entries| entries.get(&index))
            .is_some_and(|entry| entry.id == id)
    }

    /// MajorityReached: a replica removed `index` from its log, which nobody may have applied.
    fn removed(
        &mut self,
        group: Uuid,
        replica: u64,
        how: &str,
        index: u64,
        id: LogPoint,
        out: &mut Vec<Violation>,
    ) {
        self.ran(TRUNCATION, 1);
        if self.is_applied(group, index, id) {
            out.push((
                MAJORITY_REACHED,
                format!(
                    "group {group} replica {replica} {how} applied entry {index} (term {}, leader {})",
                    id.0, id.1
                ),
            ));
        }
    }

    /// True when `replica` holds the entry: in its log, or purged below its state machine.
    /// A purge marker speaks for the entries before it only if it is itself an applied entry.
    fn holds(&self, replica: &Replica, index: u64, id: LogPoint) -> bool {
        self.durable
            .get(replica)
            .is_some_and(|log| log.get(&index) == Some(&id))
            || self.purged.get(replica).is_some_and(|(upto, marker)| {
                (index < *upto || (index == *upto && id == *marker))
                    && self.is_applied(replica.0, *upto, *marker)
            })
    }

    /// MajorityReached for an entry at its first apply. The configurations that may have
    /// committed it are the one in force at its index and any later one: a leader counts with
    /// its newest appended configuration. Each must itself be applied, or be in a holder's
    /// log. Returns the newest that has a majority.
    fn majority(
        &self,
        group: Uuid,
        index: u64,
        id: LogPoint,
        out: &mut Vec<Violation>,
    ) -> Option<(u64, LogPoint)> {
        let replicas = || {
            let logs = self.durable.keys();
            logs.chain(self.purged.keys())
                .filter(|(of, _)| *of == group)
        };
        let holders: BTreeSet<u64> = replicas()
            .filter(|replica| self.holds(replica, index, id))
            .map(|(_, replica)| *replica)
            .collect();
        let known: Vec<((u64, LogPoint), &Vec<Vec<u64>>)> = self
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
            .map(|(config, voters)| (*config, voters))
            .collect();
        let in_force = known.iter().rev().find(|((at, _), _)| *at <= index);
        let later = known.iter().filter(|((at, _), _)| *at > index);
        let newest = in_force
            .into_iter()
            .chain(later)
            .filter(|(_, voters)| quorum(voters, &holders))
            .map(|(config, _)| *config)
            .max();
        if newest.is_none() {
            out.push((
                MAJORITY_REACHED,
                format!(
                    "group {group} index {index} (term {}, leader {}) applied but durable on {holders:?}, configurations {known:?}",
                    id.0, id.1
                ),
            ));
        }
        newest
    }

    /// A core started on the log found on its volume.
    fn recovered(
        &mut self,
        group: Uuid,
        replica: u64,
        purged: Option<(u64, LogPoint)>,
        entries: BTreeMap<u64, LogPoint>,
        out: &mut Vec<Violation>,
    ) {
        let key = (group, replica);
        match purged {
            Some(purged) => self.purged.insert(key, purged),
            None => self.purged.remove(&key),
        };
        self.durable.insert(key, entries);
        self.rebuild(group, replica, out);
    }

    /// MajorityReached at a start of `starting`: every applied entry of the group is durable
    /// on a majority of the voters in force now. Those are the newest applied configuration,
    /// or a newer one that is not applied yet but committed the entry. A configuration that a
    /// newer applied one replaced no longer counts, and neither do the voters it removed.
    fn rebuild(&mut self, group: Uuid, starting: u64, out: &mut Vec<Violation>) {
        let Some(entries) = self.applied.get(&group).filter(|of| !of.is_empty()) else {
            return;
        };
        let configurations = self.voters.get(&group);
        let in_force = configurations
            .into_iter()
            .flatten()
            .rfind(|((at, id), _)| self.is_applied(group, *at, *id));
        let mut found = Vec::new();
        for (index, entry) in entries {
            let holds = |replica: &u64| self.holds(&(group, *replica), *index, entry.id);
            let majority = |voters: &Vec<Vec<u64>>| {
                let holders = voters.iter().flatten().copied().filter(|r| holds(r));
                quorum(voters, &holders.collect())
            };
            let newer = entry
                .committed_by
                .filter(|config| in_force.is_none_or(|(applied, _)| config > applied));
            let held = in_force.is_some_and(|(_, voters)| majority(voters))
                || newer
                    .and_then(|config| configurations?.get(&config))
                    .is_some_and(majority);
            if !held && !self.lost.contains(&(group, *index)) {
                let holders: Vec<u64> = self
                    .replica_node
                    .keys()
                    .filter(|(of, replica)| *of == group && holds(replica))
                    .map(|(_, replica)| *replica)
                    .collect();
                found.push((
                    *index,
                    format!(
                        "group {group} index {index} (term {}, leader {}) was applied, but at the start of replica {starting} it is durable on {holders:?}, voters in force {in_force:?}",
                        entry.id.0, entry.id.1
                    ),
                ));
            }
        }
        self.ran(REBUILD, 1);
        for (index, detail) in found {
            self.lost.insert((group, index));
            out.push((MAJORITY_REACHED, detail));
        }
    }

    /// DedupUnique: `entry` applied on `replica`, so the records one attempt to apply it
    /// wrote are visible. An identity has one record, from one entry, on every replica.
    fn publish_dedup(
        &mut self,
        origin: Origin,
        replica: Replica,
        entry: &AppliedPosition,
        out: &mut Vec<Violation>,
    ) {
        let (Some(node), group, index) = (origin.node, replica.0, entry.index) else {
            return;
        };
        let Some(attempts) = self.staged.get_mut(&(node, group, index)) else {
            return;
        };
        // The newest attempt of the replica that retried is the one that committed.
        let retried = attempts.len() > 1;
        let Some((_, published)) = attempts.pop_last() else {
            return;
        };
        if attempts.is_empty() {
            self.staged.remove(&(node, group, index));
        }
        let records: BTreeSet<DedupIdentity> = published.iter().map(|of| of.0.clone()).collect();
        if !retried {
            let first = self
                .recorded
                .entry((group, index))
                .or_insert_with(|| records.clone());
            if *first != records {
                out.push((
                    DEDUP_UNIQUE,
                    format!(
                        "group {group} index {index}: replica {} on n{node} recorded {records:?}, the first replica {first:?}",
                        replica.1
                    ),
                ));
            }
        }
        for (position, (identity, commit_ts, digest)) in published.iter().enumerate() {
            self.ran(DEDUP_UNIQUE, 1);
            let DedupIdentity {
                table,
                key,
                request_id,
            } = identity;
            let now = (group, index, *commit_ts, *digest);
            let first = *self.dedup.entry(identity.clone()).or_insert(now);
            let twice = published[..position].iter().any(|of| of.0 == *identity);
            let seen = (node, origin.incarnation, replica, identity.clone());
            if first != now || twice {
                out.push((
                    DEDUP_UNIQUE,
                    format!(
                        "table {table} key {key:?} request {request_id}: the dedup record of group {group} index {index} at {commit_ts} is not the first, which group {} index {} wrote at {}",
                        first.0, first.1, first.2
                    ),
                ));
            } else if !self.dedup_seen.insert(seen) {
                out.push((
                    DEDUP_UNIQUE,
                    format!(
                        "table {table} key {key:?} request {request_id}: replica {} on n{node} wrote a second dedup record in one incarnation",
                        replica.1
                    ),
                ));
            }
            self.committed_direct(group, *commit_ts, entry.term, out);
        }
    }

    /// The highest snapshot timestamp that the range of `group` had served before `seq`,
    /// on the group itself and on the ranges it was split from.
    fn served_before(&self, group: Uuid, seq: u64) -> Option<u64> {
        let mut highest = None;
        let mut pending = vec![(group, seq)];
        let mut seen = BTreeSet::new();
        while let Some((group, seq)) = pending.pop() {
            if !seen.insert(group) {
                continue;
            }
            let served = self.served.get(&group).into_iter().flatten().rev();
            highest = highest.max(served.copied().find(|(at, _)| *at < seq).map(|(_, ts)| ts));
            let sources = self.sources.get(&group).into_iter().flatten();
            pending.extend(sources.map(|(of, until)| (*of, until.map_or(seq, |at| at.min(seq)))));
        }
        highest
    }

    /// RangeClockRule1: a direct commit at `commit_ts` was applied in an entry of `term`. Its
    /// registration, the one of that term if there are several, must precede every snapshot
    /// served at or above it. A registration that never commits breaks nothing: a deposed
    /// leader may register below reads its successor served.
    fn committed_direct(
        &mut self,
        group: Uuid,
        commit_ts: u64,
        term: u64,
        out: &mut Vec<Violation>,
    ) {
        let key = (group, commit_ts);
        let Some(proposals) = self.proposals.get(&key) else {
            return;
        };
        let same_term = proposals.iter().rev().find(|of| of.term == term);
        let Some(proposal) = same_term.or_else(|| proposals.last()) else {
            return;
        };
        let seq = proposal.seq;
        if let std::collections::btree_map::Entry::Vacant(commit) = self.direct.entry(key) {
            commit.insert((seq, false));
            if self.served_before(group, seq).is_some() {
                self.ran(RANGE_CLOCK_RULE1, 1);
            }
            self.range_clock(key, out);
        }
    }

    fn range_clock(&mut self, key: (Uuid, u64), out: &mut Vec<Violation>) {
        let Some(&(seq, false)) = self.direct.get(&key) else {
            return;
        };
        let (group, commit_ts) = key;
        if let Some(served) = self.served_before(group, seq)
            && commit_ts <= served
        {
            self.direct.insert(key, (seq, true));
            out.push((
                RANGE_CLOCK_RULE1,
                format!(
                    "group {group}: direct commit at {commit_ts} registered after a snapshot at {served} was served on its range"
                ),
            ));
        }
    }

    /// Learns from the resolved route epochs of `table` which groups a split created, and
    /// judges the direct commits their reads now bind. A source that a newer epoch still
    /// routes keeps only part of the span, so only its reads so far count.
    fn learn_splits(&mut self, table: Uuid, out: &mut Vec<Violation>) {
        let timestamps: Vec<u64> = self.roots[&table].keys().copied().collect();
        let mut learned = BTreeSet::new();
        for pair in timestamps.windows(2) {
            let (Ok(old), Ok(new)) = (self.routes(table, pair[0]), self.routes(table, pair[1]))
            else {
                continue;
            };
            for child in &new {
                if old.iter().any(|route| route.group == child.group) {
                    continue;
                }
                for source in old.iter().filter(|route| route.overlaps(child)) {
                    let routed = new.iter().any(|route| route.group == source.group);
                    let until = routed.then_some(self.seq);
                    let sources = self.sources.entry(child.group).or_default();
                    if let std::collections::btree_map::Entry::Vacant(link) =
                        sources.entry(source.group)
                    {
                        link.insert(until);
                        learned.insert(child.group);
                    }
                }
            }
        }
        if !learned.is_empty() {
            let commits: Vec<_> = self.direct.keys().copied().collect();
            for key in commits {
                self.range_clock(key, out);
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
        self.ran(MONOTONIC_COMMIT, 1);
        self.ran(LOG_MATCHING, 1);
        self.ran(APPLIED_PREFIXES_AGREE, 1);
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
        let entries = self.applied.entry(group).or_default();
        match entries.get(&entry.index) {
            Some(first) if first.id != id => out.push((
                LOG_MATCHING,
                format!(
                    "group {group} index {}: replica {replica} applied term {} leader {}, first apply was term {} leader {}",
                    entry.index, id.0, id.1, first.id.0, first.id.1
                ),
            )),
            Some(first) if first.digest != digest => out.push((
                APPLIED_PREFIXES_AGREE,
                format!(
                    "group {group} index {}: replica {replica} applied digest {digest:?}, first apply had {:?}",
                    entry.index, first.digest
                ),
            )),
            Some(_) => {}
            None => {
                let first = AppliedEntry {
                    id,
                    digest,
                    committed_by: None,
                };
                entries.insert(entry.index, first);
                self.ran(MAJORITY_REACHED, 1);
                let committed_by = self.majority(group, entry.index, id, out);
                let entries = self.applied.entry(group).or_default();
                if let Some(first) = entries.get_mut(&entry.index) {
                    first.committed_by = committed_by;
                }
            }
        }
    }

    /// The node's process stopped: its unpublished dedup records and ledgers end with it.
    pub fn crashed(&mut self, node: u64) {
        self.guards.crashed(node);
        self.ledgers.remove(&node);
        self.staged.retain(|(owner, ..), _| *owner != node);
        self.applying.retain(|_, ((owner, ..), _)| *owner != node);
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

    /// The run's guards.
    pub fn guards(&self) -> &Guards {
        &self.guards
    }

    /// ExactlyOnceRelease over the last snapshot of `node`.
    pub fn count_guards(&mut self, node: u64) -> Vec<Violation> {
        let (compared, out) = self.guards.counted(node, &self.ledgers(node));
        self.ran(EXACTLY_ONCE_RELEASE, compared);
        out
    }

    /// Starts a new ledger snapshot of `node`.
    pub fn clear_ledgers(&mut self, node: u64) {
        self.ledgers.remove(&node);
    }

    /// The ledgers `node` reported since [`Self::clear_ledgers`].
    pub fn ledgers(&self, node: u64) -> Ledgers {
        self.ledgers.get(&node).cloned().unwrap_or_default()
    }

    /// The routes that the root of `table` committed at `ts` reaches, as of that timestamp,
    /// in page order. An unreadable page is the error, after the routes before it.
    fn routes(&self, table: Uuid, ts: u64) -> Result<Vec<RouteSpan>, (Vec<RouteSpan>, Uuid)> {
        let root = self.roots.get(&table).and_then(|roots| roots.get(&ts));
        let mut next = root.and_then(|root| root.and_then(|(first, _)| first));
        let (mut spans, mut seen) = (Vec::new(), BTreeSet::new());
        while let Some(page_id) = next {
            let page = self
                .pages
                .get(&(table, page_id))
                .and_then(|page| page.range(..=ts).next_back());
            match page {
                Some((_, Some((following, routes)))) if seen.insert(page_id) => {
                    spans.extend(routes.iter().cloned());
                    next = *following;
                }
                _ => return Err((spans, page_id)),
            }
        }
        Ok(spans)
    }

    /// KeyspaceCoverage: the root of `table` committed at `ts` must reach pages, as of that
    /// timestamp, that cover the key space once. `WritesResolved` triggers it, when every
    /// record of the commit is in place.
    fn check_route(&mut self, table: Uuid, ts: u64) -> Option<Violation> {
        let Some(Some((_, epoch))) = self.roots.get(&table)?.get(&ts) else {
            return None;
        };
        let (routes, broken) = match self.routes(table, ts) {
            Ok(routes) => (routes, None),
            Err((routes, page)) => (routes, Some(page)),
        };
        let spans: Vec<_> = routes
            .into_iter()
            .map(|route| (route.start, route.end))
            .collect();
        let violation = (broken.is_some() || !covers_keyspace(&spans)).then(|| {
            (
                KEYSPACE_COVERAGE,
                format!("table {table} epoch {epoch} at {ts}: routes {spans:?}, unreadable page {broken:?}"),
            )
        });
        self.ran(KEYSPACE_COVERAGE, 1);
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
    use nabedra_oltp_types::sim_probe::GuardKind;

    use super::*;
    use nabedra_oltp_types::{sim_probe::LogEntry, types::GroupKind};

    const GROUP: Uuid = Uuid::from_u128(7);

    fn origin(node: u64) -> Origin {
        Origin {
            node: Some(node),
            incarnation: 0,
            t_ns: 0,
            live: true,
            thread: 0,
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

    /// Voters move from {1, 2, 3} to `next` after entry 2 was applied, and every current
    /// voter holds the log. Returns the state and the log.
    fn moved_to(next: &[u64]) -> (Protocol, [AppliedPosition; 4]) {
        let mut protocol = Protocol::new();
        let log = [at(1, 1), at(2, 1), at(3, 1), at(4, 1)];
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &log[..2]));
        }
        for entry in &log[..2] {
            assert!(
                protocol
                    .observe(origin(1), &applied(1, *entry, 9))
                    .is_empty()
            );
        }
        let joint = vec![vec![1, 2, 3], next.to_vec()];
        let members = [(log[2], joint), (log[3], vec![next.to_vec()])];
        for (entry, voters) in &members {
            for replica in (1..=3).chain(next.iter().copied()) {
                let probe = Probe::Appended {
                    group: GROUP,
                    kind: GroupKind::Catalog,
                    replica,
                    entries: vec![LogEntry {
                        at: *entry,
                        voters: Some(voters.clone()),
                    }],
                };
                protocol.observe(origin(replica), &probe);
            }
            assert!(
                protocol
                    .observe(origin(1), &applied(1, *entry, 9))
                    .is_empty()
            );
        }
        for replica in next {
            protocol.observe(origin(*replica), &appended(*replica, &log[..2]));
        }
        (protocol, log)
    }

    #[test]
    fn a_loss_after_a_membership_change_is_judged_by_the_current_voters() {
        // B1 of the review of #116: voter 2 and the removed voter 3 hold entry 2.
        for (next, losers) in [([1, 2, 4], [1, 4]), ([1, 4, 5], [4, 5])] {
            let (mut protocol, log) = moved_to(&next);
            let lost = [log[0], log[2], log[3]];
            let first = protocol.observe(origin(losers[0]), &recovered(losers[0], None, &lost));
            assert!(first.is_empty(), "one of three may lose it: {first:?}");
            let second = protocol.observe(origin(losers[1]), &recovered(losers[1], None, &lost));
            assert_eq!(kinds(&second), [MAJORITY_REACHED], "voters {next:?}");
        }
        // The removed voter may lose everything.
        let (mut protocol, _) = moved_to(&[1, 2, 4]);
        assert!(
            protocol
                .observe(origin(3), &recovered(3, None, &[]))
                .is_empty()
        );
    }

    #[test]
    fn a_wiped_voter_no_longer_holds_an_applied_entry() {
        let mut protocol = Protocol::new();
        let log = [at(1, 1), at(2, 1)];
        protocol.observe(origin(1), &appended(1, &log));
        protocol.observe(origin(2), &appended(2, &log));
        protocol.observe(origin(3), &appended(3, &log[..1]));
        protocol.observe(origin(1), &applied(1, log[0], 9));
        assert!(
            protocol
                .observe(origin(1), &applied(1, log[1], 9))
                .is_empty()
        );
        protocol.crashed(2);
        protocol.wiped(2);
        let start = protocol.observe(origin(2), &recovered(2, None, &[]));
        assert_eq!(kinds(&start), [MAJORITY_REACHED]);
    }

    #[test]
    fn overwriting_an_applied_entry_violates_majority_reached() {
        let mut protocol = Protocol::new();
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]));
        }
        protocol.observe(origin(1), &applied(1, at(1, 1), 9));
        // Entry 2 was never applied: a newer leader may replace it.
        assert!(
            protocol
                .observe(origin(3), &appended(3, &[at(2, 2)]))
                .is_empty()
        );
        protocol.observe(origin(1), &applied(1, at(2, 1), 9));
        let overwritten = protocol.observe(origin(2), &appended(2, &[at(2, 2)]));
        assert_eq!(kinds(&overwritten), [MAJORITY_REACHED]);
    }

    #[test]
    fn a_purge_marker_under_another_log_id_holds_nothing() {
        let mut protocol = Protocol::new();
        let log = [at(1, 1), at(2, 1)];
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &log));
        }
        for entry in &log {
            protocol.observe(origin(1), &applied(1, *entry, 9));
        }
        let first = protocol.observe(origin(2), &recovered(2, Some(at(2, 9)), &[]));
        assert!(first.is_empty(), "one of three may lose it: {first:?}");
        let second = protocol.observe(origin(3), &recovered(3, Some(at(2, 9)), &[]));
        assert!(kinds(&second).contains(&MAJORITY_REACHED), "{second:?}");
    }

    fn dedup_at(commit_ts: u64) -> Probe {
        Probe::DedupApplied {
            group: GROUP,
            table: Uuid::from_u128(1),
            anchor: Key::Int64(5),
            request_id: Uuid::from_u128(2),
            commit_ts,
            digest: Digest::from_bytes([1; 32]),
        }
    }

    #[test]
    fn a_second_entry_for_one_identity_is_caught_after_a_restart() {
        let mut protocol = Protocol::new();
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]));
        }
        let mut write = |incarnation, index| {
            let from = Origin {
                incarnation,
                ..origin(1)
            };
            let applying = Probe::DirectApplying {
                group: GROUP,
                index,
            };
            protocol.observe(from, &applying);
            protocol.observe(from, &dedup_at(10));
            kinds(&protocol.observe(from, &applied(1, at(index, 1), 9)))
        };
        assert!(write(0, 1).is_empty());
        // The restart applies the same entry again.
        assert!(write(1, 1).is_empty());
        // Another entry executes the identity, with the same timestamp and digest.
        assert_eq!(write(2, 2), [DEDUP_UNIQUE]);
    }

    #[test]
    fn two_members_of_one_batch_cannot_carry_one_identity() {
        let mut protocol = Protocol::new();
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1)]));
        }
        let applying = Probe::DirectApplying {
            group: GROUP,
            index: 1,
        };
        protocol.observe(origin(1), &applying);
        protocol.observe(origin(1), &dedup_at(10));
        protocol.observe(origin(1), &dedup_at(10));
        assert_eq!(
            kinds(&protocol.observe(origin(1), &applied(1, at(1, 1), 9))),
            [DEDUP_UNIQUE]
        );
    }

    #[test]
    fn a_deposed_leaders_registration_is_not_a_commit() {
        let mut protocol = Protocol::new();
        protocol.observe(
            origin(2),
            &Probe::SnapshotServed {
                group: GROUP,
                read_ts: 20,
            },
        );
        let late = Probe::DirectProposed {
            group: GROUP,
            commit_ts: 15,
            term: 1,
        };
        assert!(protocol.observe(origin(1), &late).is_empty());
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
        let mut write = |node, replica, index, commit_ts, attempts| {
            for _ in 0..attempts {
                let applying = Probe::DirectApplying {
                    group: GROUP,
                    index,
                };
                assert!(protocol.observe(origin(node), &applying).is_empty());
                assert!(
                    protocol
                        .observe(origin(node), &dedup(commit_ts, 1))
                        .is_empty()
                );
            }
            kinds(&protocol.observe(origin(node), &applied(replica, at(index, 1), 9)))
        };
        // A retried transaction is one record.
        assert!(write(1, 1, 1, 10, 2).is_empty());
        // A second replica of the group on the same node has its own.
        assert!(write(1, 4, 1, 10, 1).is_empty());
        assert!(write(2, 2, 1, 10, 1).is_empty());
        assert_eq!(write(3, 3, 1, 11, 1), [DEDUP_UNIQUE]);
        // The same record from a second entry of one incarnation is a second record.
        assert_eq!(write(2, 2, 2, 10, 1), [DEDUP_UNIQUE]);
        assert_eq!(protocol.committed().count(), 1);
    }

    fn served(group: Uuid, read_ts: u64) -> Probe {
        Probe::SnapshotServed { group, read_ts }
    }

    fn proposed(group: Uuid, commit_ts: u64, term: u64) -> Probe {
        Probe::DirectProposed {
            group,
            commit_ts,
            term,
        }
    }

    /// Replica 1 of `group` applies a direct commit at `index`, of term 1.
    fn commits(protocol: &mut Protocol, group: Uuid, index: u64, commit_ts: u64) -> Vec<Violation> {
        let mut out = Vec::new();
        let probes = [
            Probe::Appended {
                group,
                kind: GroupKind::Catalog,
                replica: 1,
                entries: vec![LogEntry {
                    at: at(index, 1),
                    voters: (index == 1).then(|| vec![vec![1]]),
                }],
            },
            Probe::DirectApplying { group, index },
            Probe::DedupApplied {
                group,
                table: Uuid::from_u128(1),
                anchor: Key::Int64(5),
                request_id: Uuid::from_u128(u128::from(commit_ts)),
                commit_ts,
                digest: Digest::from_bytes([1; 32]),
            },
            Probe::Applied {
                group,
                kind: GroupKind::Catalog,
                replica: 1,
                entry: at(index, 1),
                digest: Digest::from_bytes([1; 32]),
            },
        ];
        for probe in &probes {
            out.extend(protocol.observe(origin(1), probe));
        }
        out
    }

    #[test]
    fn a_commit_must_register_above_the_snapshots_served() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &proposed(GROUP, 15, 1));
        protocol.observe(origin(1), &served(GROUP, 20));
        protocol.observe(origin(1), &proposed(GROUP, 21, 1));
        protocol.observe(origin(1), &proposed(GROUP, 20, 1));
        // Registered before the read, and above it.
        assert!(commits(&mut protocol, GROUP, 1, 15).is_empty());
        assert!(commits(&mut protocol, GROUP, 2, 21).is_empty());
        let below = commits(&mut protocol, GROUP, 3, 20);
        assert_eq!(kinds(&below), [RANGE_CLOCK_RULE1]);
        assert_eq!(protocol.exercised()[RANGE_CLOCK_RULE1], 2);
    }

    #[test]
    fn a_commit_is_judged_by_the_registration_of_its_own_term() {
        let mut protocol = Protocol::new();
        protocol.observe(origin(1), &proposed(GROUP, 15, 1));
        protocol.observe(origin(2), &served(GROUP, 20));
        // A deposed leader registers the timestamp again, below a read its successor served.
        protocol.observe(origin(3), &proposed(GROUP, 15, 2));
        assert!(commits(&mut protocol, GROUP, 1, 15).is_empty());
    }

    #[test]
    fn a_split_child_keeps_the_reads_of_its_source() {
        let (table, page_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let [left, right, third] = [21, 22, 23].map(Uuid::from_u128);
        let span = |group, start: Option<i64>, end: Option<i64>| Span {
            group,
            start: start.map(Key::Int64),
            end: end.map(Key::Int64),
        };
        let publish = |protocol: &mut Protocol, commit_ts, spans: Vec<Span>| {
            let root = RouteRecord::Root {
                first_page: Some(page_id),
                routing_epoch: 1,
            };
            let page = RouteRecord::Page {
                page_id,
                next_page: None,
                spans,
            };
            let mut out = Vec::new();
            for record in [root, page] {
                let probe = Probe::RouteEpochPublished {
                    table,
                    commit_ts,
                    record,
                };
                out.extend(protocol.observe(origin(1), &probe));
            }
            out.extend(protocol.observe(origin(1), &Probe::WritesResolved { commit_ts }));
            out
        };
        let halves = || vec![span(left, None, Some(5)), span(right, Some(5), None)];
        let split = |protocol: &mut Protocol| publish(protocol, 30, halves());
        let read = || {
            let mut protocol = Protocol::new();
            assert!(publish(&mut protocol, 5, vec![span(GROUP, None, None)]).is_empty());
            protocol.observe(origin(1), &served(GROUP, 35));
            protocol
        };
        // The child commits below the read, and above it.
        let mut protocol = read();
        assert!(split(&mut protocol).is_empty());
        protocol.observe(origin(1), &proposed(left, 34, 1));
        protocol.observe(origin(1), &proposed(right, 36, 1));
        assert!(commits(&mut protocol, right, 1, 36).is_empty());
        let below = commits(&mut protocol, left, 1, 34);
        assert_eq!(kinds(&below), [RANGE_CLOCK_RULE1]);
        // The child commits before a Catalog replica resolved the routes.
        let mut protocol = read();
        protocol.observe(origin(1), &proposed(left, 34, 1));
        assert!(commits(&mut protocol, left, 1, 34).is_empty());
        assert_eq!(kinds(&split(&mut protocol)), [RANGE_CLOCK_RULE1]);
        // A read before the registration only.
        let mut protocol = Protocol::new();
        publish(&mut protocol, 5, vec![span(GROUP, None, None)]);
        protocol.observe(origin(1), &proposed(left, 34, 1));
        protocol.observe(origin(1), &served(GROUP, 35));
        assert!(split(&mut protocol).is_empty());
        assert!(commits(&mut protocol, left, 1, 34).is_empty());
        // A split of the child, and a source that keeps part of its span: its reads after
        // the split are not on the new child's keys.
        let mut protocol = read();
        split(&mut protocol);
        protocol.observe(origin(1), &served(left, 41));
        let halves = vec![
            span(left, None, Some(3)),
            span(third, Some(3), Some(5)),
            span(right, Some(5), None),
        ];
        assert!(publish(&mut protocol, 50, halves).is_empty());
        protocol.observe(origin(1), &served(left, 60));
        for (commit_ts, index) in [(55, 1), (40, 2), (34, 3)] {
            protocol.observe(origin(1), &proposed(third, commit_ts, 1));
            let found = commits(&mut protocol, third, index, commit_ts);
            let expected: &[&str] = if commit_ts == 55 {
                &[]
            } else {
                &[RANGE_CLOCK_RULE1]
            };
            assert_eq!(kinds(&found), expected, "commit at {commit_ts}");
        }
    }

    #[test]
    fn jobs_that_a_storage_pause_interleaves_keep_their_own_records() {
        // Two replicas of a group on one node apply entries 1 and 2 at the same time.
        let on = |thread| Origin {
            thread,
            ..origin(1)
        };
        let mut protocol = Protocol::new();
        for replica in 1..=3 {
            protocol.observe(origin(replica), &appended(replica, &[at(1, 1), at(2, 1)]));
        }
        let applying = |index| Probe::DirectApplying {
            group: GROUP,
            index,
        };
        let dedup = |request: u128, commit_ts| Probe::DedupApplied {
            group: GROUP,
            table: Uuid::from_u128(1),
            anchor: Key::Int64(5),
            request_id: Uuid::from_u128(request),
            commit_ts,
            digest: Digest::from_bytes([1; 32]),
        };
        protocol.observe(on(1), &applying(1));
        protocol.observe(on(2), &applying(2));
        protocol.observe(on(1), &dedup(7, 10));
        protocol.observe(on(2), &dedup(8, 11));
        assert!(protocol.observe(on(1), &applied(1, at(1, 1), 9)).is_empty());
        assert!(protocol.observe(on(2), &applied(4, at(2, 1), 9)).is_empty());
        // Node 2 applies them one after the other and records the same.
        for (index, request, commit_ts) in [(1, 7, 10), (2, 8, 11)] {
            protocol.observe(origin(2), &applying(index));
            protocol.observe(origin(2), &dedup(request, commit_ts));
            let found = protocol.observe(origin(2), &applied(2, at(index, 1), 9));
            assert!(found.is_empty(), "index {index}: {found:?}");
        }
        // A replica that applies the entry without its record disagrees.
        protocol.observe(origin(3), &applying(1));
        let missing = protocol.observe(origin(3), &applied(3, at(1, 1), 9));
        assert_eq!(kinds(&missing), [DEDUP_UNIQUE]);
    }

    #[test]
    fn a_replica_that_installed_a_snapshot_holds_what_it_purged() {
        let (mut protocol, log) = moved_to(&[1, 2, 4]);
        // Replica 4 never had entries 1 and 2 in its log: they are below its snapshot.
        protocol.observe(origin(4), &recovered(4, None, &log[2..]));
        protocol.observe(
            origin(4),
            &Probe::Purged {
                group: GROUP,
                kind: GroupKind::Catalog,
                replica: 4,
                upto: log[1],
            },
        );
        let lost = [log[0], log[2], log[3]];
        let first = protocol.observe(origin(1), &recovered(1, None, &lost));
        assert!(first.is_empty(), "2 and 4 hold it: {first:?}");
        let second = protocol.observe(origin(2), &recovered(2, None, &lost));
        assert_eq!(kinds(&second), [MAJORITY_REACHED]);
    }

    #[test]
    fn an_unapplied_configuration_that_committed_an_entry_still_protects_it() {
        let member = |replica, index, voters: Vec<Vec<u64>>| Probe::Appended {
            group: GROUP,
            kind: GroupKind::Catalog,
            replica,
            entries: vec![LogEntry {
                at: at(index, 1),
                voters: Some(voters),
            }],
        };
        let mut protocol = Protocol::new();
        let joint = || vec![vec![1, 2, 3], vec![4]];
        for replica in 1..=4 {
            protocol.observe(origin(replica), &member(replica, 1, joint()));
        }
        assert!(
            protocol
                .observe(origin(4), &applied(4, at(1, 1), 9))
                .is_empty()
        );
        protocol.observe(origin(4), &appended(4, &[at(2, 1)]));
        protocol.observe(origin(4), &member(4, 3, vec![vec![4]]));
        assert!(
            protocol
                .observe(origin(4), &applied(4, at(2, 1), 9))
                .is_empty()
        );
        // Voters 1 to 3 never had entry 2, and need not.
        let held = recovered(1, None, &[at(1, 1)]);
        assert!(protocol.observe(origin(1), &held).is_empty());
        let gone = recovered(4, None, &[at(1, 1), at(3, 1)]);
        assert_eq!(
            kinds(&protocol.observe(origin(4), &gone)),
            [MAJORITY_REACHED]
        );
    }

    #[test]
    fn committed_routes_must_cover_the_key_space() {
        let (table, page_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let span = |start: Option<i64>, end: Option<i64>| Span {
            group: GROUP,
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
    fn guards_are_released_once_and_end_with_their_process() {
        let mut protocol = Protocol::new();
        let kind = GuardKind::NativeLease;
        let acquire = |guard| Probe::GuardAcquired {
            guard,
            kind,
            class: "query",
        };
        let release = |guard| Probe::GuardReleased { guard };
        protocol.observe(origin(1), &acquire(1));
        assert_eq!(protocol.guards().held(1, kind), 1);
        assert!(protocol.observe(origin(1), &release(1)).is_empty());
        assert_eq!(
            kinds(&protocol.observe(origin(1), &release(1))),
            [EXACTLY_ONCE_RELEASE]
        );
        protocol.observe(origin(1), &acquire(2));
        protocol.crashed(1);
        protocol.observe(origin(1), &acquire(3));
        let dead = Origin {
            live: false,
            ..origin(1)
        };
        assert!(protocol.observe(dead, &release(2)).is_empty());
        assert_eq!(protocol.guards().held(1, kind), 1);
        let counted = Probe::LedgerSnapshot {
            ledger: Ledger::NativeLeases,
            group: None,
            outstanding: 2,
        };
        protocol.observe(origin(1), &counted);
        assert_eq!(kinds(&protocol.count_guards(1)), [EXACTLY_ONCE_RELEASE]);
        let exercised = protocol.exercised();
        assert_eq!(exercised.get(EXACTLY_ONCE_RELEASE), Some(&3));
        assert_eq!(exercised.get(RELEASE_AFTER_WORK), None);
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
