//! The `direct` section of `/v1/runtime/stats` (plan "Benchmark").
use crate::oltp_replication::service_rpc::direct::FallbackReason;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Upper bounds of the rows-per-entry buckets; the last bucket is open.
pub(crate) const BATCH_BUCKETS: [u64; 5] = [1, 8, 64, 512, 4096];

/// Why an attempt went back to step 3 within the deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryCause {
    /// Rule 1 of the range clock: the timestamp was at or below a served
    /// snapshot.
    StaleTimestamp,
    /// Apply refused a stale expected version or timestamp.
    Conflict,
    /// Leadership changed between the token and the registration or apply.
    Leadership,
    /// A read waiting on a pending commit failed after a leadership change,
    /// or storage was briefly unavailable.
    Unavailable,
    /// The group's fence moved; the next attempt reads the new authority.
    Authority,
}
const RETRY_CAUSES: usize = 5;

/// Why a proposal (a `DirectCommit`, a forward marker, a group activation or
/// an expiry page) produced no reply. Tells a slow election from a stuck
/// leader (T7b).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProposalFailure {
    /// The request deadline expired while the proposal was in flight.
    Deadline,
    /// Raft refused or lost the proposal and this node no longer serves the
    /// term it proposed in (an election moved leadership).
    Leadership,
    /// Raft refused or lost the proposal while this node still served the
    /// same term: quorum, log or apply trouble on a stable leader.
    Stalled,
    /// The group's first activation (`DirectSchema::Activate`) failed.
    Activation,
}
const PROPOSAL_FAILURES: [(&str, ProposalFailure); 4] = [
    ("deadline", ProposalFailure::Deadline),
    ("leadership", ProposalFailure::Leadership),
    ("stalled", ProposalFailure::Stalled),
    ("activation", ProposalFailure::Activation),
];

#[derive(Default)]
pub(crate) struct DirectStats {
    requests: AtomicU64,
    gets: AtomicU64,
    mutations: AtomicU64,
    forwards: AtomicU64,
    /// `DirectCommit` entries proposed.
    proposed: AtomicU64,
    applied: AtomicU64,
    replayed: AtomicU64,
    rows_proposed: AtomicU64,
    index_changes_proposed: AtomicU64,
    batch_buckets: [AtomicU64; BATCH_BUCKETS.len() + 1],
    read_only: AtomicU64,
    fallbacks: [AtomicU64; FallbackReason::ALL.len()],
    retries: [AtomicU64; RETRY_CAUSES],
    proposal_failures: [AtomicU64; PROPOSAL_FAILURES.len()],
    latch_waits: AtomicU64,
    latch_wait_micros: AtomicU64,
    overloaded: AtomicU64,
    stale_routes: AtomicU64,
    not_leader: AtomicU64,
    failed: AtomicU64,
    outcome_unknown: AtomicU64,
    markers_written: AtomicU64,
    expire_entries: AtomicU64,
    expired_records: AtomicU64,
    mutation_micros: AtomicU64,
    mutation_done: AtomicU64,
    get_micros: AtomicU64,
    get_done: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}
fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

impl DirectStats {
    pub(crate) fn request(&self) {
        bump(&self.requests);
    }
    pub(crate) fn get(&self) {
        bump(&self.gets);
    }
    pub(crate) fn mutation(&self) {
        bump(&self.mutations);
    }
    pub(crate) fn forward(&self) {
        bump(&self.forwards);
    }
    pub(crate) fn proposed(&self, rows: usize, index_changes: usize) {
        bump(&self.proposed);
        self.rows_proposed.fetch_add(rows as u64, Ordering::Relaxed);
        self.index_changes_proposed
            .fetch_add(index_changes as u64, Ordering::Relaxed);
        let bucket = BATCH_BUCKETS
            .iter()
            .position(|bound| rows as u64 <= *bound)
            .unwrap_or(BATCH_BUCKETS.len());
        bump(&self.batch_buckets[bucket]);
    }
    pub(crate) fn applied(&self) {
        bump(&self.applied);
    }
    pub(crate) fn replayed(&self) {
        bump(&self.replayed);
    }
    pub(crate) fn read_only(&self) {
        bump(&self.read_only);
    }
    pub(crate) fn fallback(&self, reason: FallbackReason) {
        let index = FallbackReason::ALL
            .iter()
            .position(|r| *r == reason)
            .expect("every reason is listed");
        bump(&self.fallbacks[index]);
    }
    pub(crate) fn retry(&self, cause: RetryCause) {
        bump(&self.retries[cause as usize]);
    }
    pub(crate) fn proposal_failed(&self, reason: ProposalFailure) {
        bump(&self.proposal_failures[reason as usize]);
    }
    pub(crate) fn latch_wait(&self, waited: Duration) {
        bump(&self.latch_waits);
        self.latch_wait_micros
            .fetch_add(micros(waited), Ordering::Relaxed);
    }
    pub(crate) fn overloaded(&self) {
        bump(&self.overloaded);
    }
    pub(crate) fn stale_route(&self) {
        bump(&self.stale_routes);
    }
    pub(crate) fn not_leader(&self) {
        bump(&self.not_leader);
    }
    pub(crate) fn failed(&self, unknown: bool) {
        bump(&self.failed);
        if unknown {
            bump(&self.outcome_unknown);
        }
    }
    pub(crate) fn marker_written(&self) {
        bump(&self.markers_written);
    }
    pub(crate) fn expired(&self, deleted: u64) {
        bump(&self.expire_entries);
        self.expired_records.fetch_add(deleted, Ordering::Relaxed);
    }
    pub(crate) fn mutation_time(&self, elapsed: Duration) {
        bump(&self.mutation_done);
        self.mutation_micros
            .fetch_add(micros(elapsed), Ordering::Relaxed);
    }
    pub(crate) fn get_time(&self, elapsed: Duration) {
        bump(&self.get_done);
        self.get_micros
            .fetch_add(micros(elapsed), Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, in_flight: usize, max_in_flight: usize) -> DirectStatsSnapshot {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let mut batch_sizes = BTreeMap::new();
        for (index, counter) in self.batch_buckets.iter().enumerate() {
            let label = match BATCH_BUCKETS.get(index) {
                Some(bound) => format!("le_{bound}"),
                None => format!("gt_{}", BATCH_BUCKETS[BATCH_BUCKETS.len() - 1]),
            };
            batch_sizes.insert(label, load(counter));
        }
        let fallbacks = FallbackReason::ALL
            .iter()
            .zip(&self.fallbacks)
            .map(|(reason, counter)| (reason.as_str(), load(counter)))
            .collect();
        let causes = [
            ("stale_timestamp", RetryCause::StaleTimestamp),
            ("conflict", RetryCause::Conflict),
            ("leadership", RetryCause::Leadership),
            ("unavailable", RetryCause::Unavailable),
            ("authority", RetryCause::Authority),
        ];
        let retries = causes
            .iter()
            .map(|(name, cause)| (*name, load(&self.retries[*cause as usize])))
            .collect();
        let proposal_failures = PROPOSAL_FAILURES
            .iter()
            .map(|(name, reason)| (*name, load(&self.proposal_failures[*reason as usize])))
            .collect();
        DirectStatsSnapshot {
            requests: load(&self.requests),
            gets: load(&self.gets),
            mutations: load(&self.mutations),
            forwards: load(&self.forwards),
            entries_proposed: load(&self.proposed),
            entries_applied: load(&self.applied),
            replayed: load(&self.replayed),
            read_only: load(&self.read_only),
            rows_proposed: load(&self.rows_proposed),
            index_changes_proposed: load(&self.index_changes_proposed),
            batch_sizes,
            fallbacks,
            retries,
            proposal_failures,
            latch_waits: load(&self.latch_waits),
            latch_wait_micros: load(&self.latch_wait_micros),
            overloaded_429: load(&self.overloaded),
            stale_routes: load(&self.stale_routes),
            not_leader: load(&self.not_leader),
            failed: load(&self.failed),
            outcome_unknown: load(&self.outcome_unknown),
            forward_markers_written: load(&self.markers_written),
            expire_entries: load(&self.expire_entries),
            expired_records: load(&self.expired_records),
            mutation_micros_total: load(&self.mutation_micros),
            mutations_done: load(&self.mutation_done),
            get_micros_total: load(&self.get_micros),
            gets_done: load(&self.get_done),
            in_flight,
            max_in_flight,
        }
    }
}

/// A point-in-time copy of the counters, serialized as the `direct` section.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct DirectStatsSnapshot {
    pub requests: u64,
    pub gets: u64,
    pub mutations: u64,
    pub forwards: u64,
    pub entries_proposed: u64,
    pub entries_applied: u64,
    pub replayed: u64,
    pub read_only: u64,
    pub rows_proposed: u64,
    pub index_changes_proposed: u64,
    /// Rows per proposed entry.
    pub batch_sizes: BTreeMap<String, u64>,
    pub fallbacks: BTreeMap<&'static str, u64>,
    pub retries: BTreeMap<&'static str, u64>,
    /// Proposals that produced no reply, by `ProposalFailure`.
    pub proposal_failures: BTreeMap<&'static str, u64>,
    pub latch_waits: u64,
    pub latch_wait_micros: u64,
    pub overloaded_429: u64,
    pub stale_routes: u64,
    pub not_leader: u64,
    pub failed: u64,
    pub outcome_unknown: u64,
    pub forward_markers_written: u64,
    pub expire_entries: u64,
    pub expired_records: u64,
    /// Leader-side time from admission to reply, summed over mutations.
    pub mutation_micros_total: u64,
    pub mutations_done: u64,
    pub get_micros_total: u64,
    pub gets_done: u64,
    pub in_flight: usize,
    pub max_in_flight: usize,
}
