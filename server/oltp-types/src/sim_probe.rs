//! Simulation-only protocol probes (phase 10 and 11 design, section 3.2 item 3).
//!
//! Product code reports a protocol step at the point where the step takes effect, through
//! a one-statement call site that the `sim` feature gates:
//!
//! ```ignore
//! #[cfg(feature = "sim")]
//! nabedra_oltp_types::sim_probe::emit(|| Probe::SnapshotServed { group, read_ts });
//! ```
//!
//! [`emit`] looks up the run's [`ProbeSink`] with `World::with_ext`. Outside a simulation
//! world, or in a world without a sink, it returns at once and never builds the probe.
//! Without the `sim` feature this module and every call site compile away, so product
//! builds keep their instructions.
//!
//! The simulator runs every node on one thread, and blocking jobs run on its helper while
//! that thread waits, so probes arrive in one deterministic order. The sink runs outside
//! the world's extension slot: a probe emitted while the sink handles another one still
//! reaches it. A sink must not call back into product code.
//!
//! Probes carry identities, positions and digests, never payload bytes.

use std::sync::Arc;

use nabedra_protocol::protocol::Key;
use serde::Serialize;
use uuid::Uuid;

use crate::types::{AppliedPosition, Digest, GroupKind};

/// A transient ledger a probe reports the size of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ledger {
    /// `RangeClock` commits registered and not yet settled, per group.
    RangeClockPending,
}

/// A route's half-open key span. `None` is unbounded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Span {
    pub start: Option<Key>,
    pub end: Option<Key>,
}

/// A synced log entry. `voters` is the joint voter configuration of a membership entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LogEntry {
    #[serde(flatten)]
    pub at: AppliedPosition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voters: Option<Vec<Vec<u64>>>,
}

/// A committed Catalog routing record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteRecord {
    Root {
        first_page: Option<Uuid>,
        routing_epoch: u64,
    },
    RootDeleted,
    Page {
        page_id: Uuid,
        next_page: Option<Uuid>,
        spans: Vec<Span>,
    },
    PageDeleted {
        page_id: Uuid,
    },
}

/// One protocol step. The variants follow design section 3.2. `replica` is the Raft node
/// ID of the reporting replica within `group`; the sink adds the simulated node and time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "probe")]
pub enum Probe {
    /// A replica's Raft append is synced: `entries` are durable on it.
    Appended {
        group: Uuid,
        kind: GroupKind,
        replica: u64,
        entries: Vec<LogEntry>,
    },
    /// A replica removed every log entry after `after` (all of them when `None`).
    Truncated {
        group: Uuid,
        kind: GroupKind,
        replica: u64,
        after: Option<AppliedPosition>,
    },
    /// A replica saved its vote for `leader` in `term`. `committed` means a quorum granted
    /// it, so `leader` leads `term`.
    VoteSaved {
        group: Uuid,
        kind: GroupKind,
        replica: u64,
        term: u64,
        leader: u64,
        committed: bool,
    },
    /// A replica applied the entry at `entry` to its state machine. `digest` is the
    /// command digest the apply metadata carries.
    Applied {
        group: Uuid,
        kind: GroupKind,
        replica: u64,
        entry: AppliedPosition,
        digest: Digest,
    },
    /// Reserved: a replica observed `leader` for `term`. Phase 10 derives leadership from
    /// committed votes.
    LeaderSeen {
        group: Uuid,
        replica: u64,
        term: u64,
        leader: u64,
    },
    /// A direct commit wrote its dedup record in an apply transaction.
    DedupApplied {
        group: Uuid,
        table: Uuid,
        anchor: Key,
        request_id: Uuid,
        commit_ts: u64,
        digest: Digest,
    },
    /// A snapshot read at `read_ts` passed the range clock (rule 2) and may be served.
    SnapshotServed { group: Uuid, read_ts: u64 },
    /// The range leader registered a direct commit at `commit_ts` before proposing it.
    DirectProposed {
        group: Uuid,
        commit_ts: u64,
        term: u64,
    },
    /// A Catalog replica made a routing record visible at `commit_ts`.
    RouteEpochPublished {
        table: Uuid,
        commit_ts: u64,
        record: RouteRecord,
    },
    /// The current size of a transient ledger.
    LedgerSnapshot {
        ledger: Ledger,
        group: Option<Uuid>,
        outstanding: u64,
    },
    /// A gateway sent one attempt of a direct request to `voter`, with `remaining_us`
    /// before the request deadline.
    GatewayAttempt {
        request_id: Uuid,
        group: Uuid,
        voter: u64,
        remaining_us: u64,
    },
    /// Reserved for phase 11: a replica installed a queued request's copy.
    QueueCopy { request_id: Uuid },
    /// Reserved for phase 11: a receipt reached a terminal state.
    Receipt { request_id: Uuid },
    /// `WorkerSharedResources` granted a native lease; `outstanding` counts this one.
    GuardAcquired {
        class: &'static str,
        outstanding: u64,
    },
    /// A native lease settled; `outstanding` no longer counts it.
    GuardReleased { outstanding: u64 },
    /// A native job's work returned, before its lease settles.
    WorkDone { class: &'static str },
}

impl Probe {
    /// The variant name, which is also the trace action name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Appended { .. } => "Appended",
            Self::Truncated { .. } => "Truncated",
            Self::VoteSaved { .. } => "VoteSaved",
            Self::Applied { .. } => "Applied",
            Self::LeaderSeen { .. } => "LeaderSeen",
            Self::DedupApplied { .. } => "DedupApplied",
            Self::SnapshotServed { .. } => "SnapshotServed",
            Self::DirectProposed { .. } => "DirectProposed",
            Self::RouteEpochPublished { .. } => "RouteEpochPublished",
            Self::LedgerSnapshot { .. } => "LedgerSnapshot",
            Self::GatewayAttempt { .. } => "GatewayAttempt",
            Self::QueueCopy { .. } => "QueueCopy",
            Self::Receipt { .. } => "Receipt",
            Self::GuardAcquired { .. } => "GuardAcquired",
            Self::GuardReleased { .. } => "GuardReleased",
            Self::WorkDone { .. } => "WorkDone",
        }
    }
}

type Observer = dyn Fn(Probe) + Send + Sync;

/// The run's probe observer, stored in the world's extension slot.
pub struct ProbeSink(Arc<Observer>);

impl ProbeSink {
    pub fn new(observer: impl Fn(Probe) + Send + Sync + 'static) -> Self {
        Self(Arc::new(observer))
    }

    /// Installs this sink in `world`, replacing any earlier one.
    pub fn install(self, world: &nabedra_rt::sim::World) {
        world.insert_ext(self);
    }
}

/// Reports the probe `probe` builds to the installed sink. Does nothing, and does not
/// call `probe`, outside a simulation world or without a sink.
#[inline]
pub fn emit(probe: impl FnOnce() -> Probe) {
    let Some(world) = nabedra_rt::sim::world() else {
        return;
    };
    let Some(observer) = world.with_ext::<ProbeSink, _>(|sink| sink.0.clone()) else {
        return;
    };
    observer(probe());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn emit_reaches_the_installed_sink_and_is_inert_elsewhere() {
        // No world: the closure never runs.
        emit(|| unreachable!("no world is installed"));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        nabedra_rt::sim::run(1, |world| async move {
            emit(|| unreachable!("no sink is installed"));
            ProbeSink::new(move |probe| {
                // A probe emitted from inside the sink still arrives.
                if let Probe::SnapshotServed { read_ts: 1, group } = probe {
                    emit(|| Probe::SnapshotServed { group, read_ts: 2 });
                }
                observed.lock().unwrap().push(probe.name());
            })
            .install(&world);
            emit(|| Probe::SnapshotServed {
                group: Uuid::nil(),
                read_ts: 1,
            });
        });
        assert_eq!(*seen.lock().unwrap(), ["SnapshotServed", "SnapshotServed"]);
    }
}
