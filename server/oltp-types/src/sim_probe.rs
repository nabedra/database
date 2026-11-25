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

/// A transient ledger whose size [`snapshot_ledgers`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ledger {
    /// Bytes charged to one of the worker's byte arenas.
    ChargedBytes(&'static str),
    /// Working bytes charged to the scheduler: compute grants and retained charges.
    SchedulerBytes,
    /// Compute guards granted and not yet dropped.
    ComputeGuards,
    /// Native leases prepared and not yet settled.
    NativeLeases,
    /// Bytes held by scratch leases.
    ScratchBytes,
    /// Disk reservations held on one device, and their bytes.
    DiskReservations {
        device: u64,
    },
    DiskReservedBytes {
        device: u64,
    },
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
    /// The durable log a replica's Raft core starts on: entries up to `purged` are only in
    /// its state machine or snapshot, and `entries` are in its log.
    Recovered {
        group: Uuid,
        kind: GroupKind,
        replica: u64,
        purged: Option<AppliedPosition>,
        entries: Vec<AppliedPosition>,
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
    /// A Data replica's leadership watcher saw its own core lead `term`.
    LeaderSeen {
        group: Uuid,
        replica: u64,
        term: u64,
        leader: u64,
    },
    /// A replica starts the transaction that applies the direct entry at `index`.
    DirectApplying { group: Uuid, index: u64 },
    /// A direct commit wrote its dedup record in the transaction that applies its Raft
    /// entry. The entry is committed; the record is visible once the matching `Applied`
    /// follows, and a retried transaction reports it again before that.
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
    /// A Catalog replica resolved a routing record at `commit_ts`.
    RouteEpochPublished {
        table: Uuid,
        commit_ts: u64,
        record: RouteRecord,
    },
    /// A replica resolved the last write of a transaction committed at `commit_ts`, so
    /// every routing record of that commit is in place.
    WritesResolved { commit_ts: u64 },
    /// The current size of a transient ledger, from [`snapshot_ledgers`].
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
    /// `WorkerSharedResources` granted a native lease.
    GuardAcquired { class: &'static str },
    /// A native lease settled.
    GuardReleased,
    /// A native job's work returned, before its lease settles.
    WorkDone { class: &'static str },
}

impl Probe {
    /// The variant name, which is also the trace action name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Appended { .. } => "Appended",
            Self::Recovered { .. } => "Recovered",
            Self::Truncated { .. } => "Truncated",
            Self::VoteSaved { .. } => "VoteSaved",
            Self::Applied { .. } => "Applied",
            Self::LeaderSeen { .. } => "LeaderSeen",
            Self::DirectApplying { .. } => "DirectApplying",
            Self::DedupApplied { .. } => "DedupApplied",
            Self::SnapshotServed { .. } => "SnapshotServed",
            Self::DirectProposed { .. } => "DirectProposed",
            Self::RouteEpochPublished { .. } => "RouteEpochPublished",
            Self::WritesResolved { .. } => "WritesResolved",
            Self::LedgerSnapshot { .. } => "LedgerSnapshot",
            Self::GatewayAttempt { .. } => "GatewayAttempt",
            Self::QueueCopy { .. } => "QueueCopy",
            Self::Receipt { .. } => "Receipt",
            Self::GuardAcquired { .. } => "GuardAcquired",
            Self::GuardReleased => "GuardReleased",
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

/// True when a sink is installed, for a probe that needs work before it can be built.
pub fn active() -> bool {
    nabedra_rt::sim::world().is_some_and(|world| world.with_ext::<ProbeSink, _>(|_| ()).is_some())
}

type LedgerSource = Box<dyn Fn() -> bool + Send>;

/// The run's ledger owners, by address, with the node that created each.
struct LedgerSources(Vec<(usize, Option<u64>, LedgerSource)>);

/// Makes [`snapshot_ledgers`] call `report` for `owner` while it is alive. `report` must
/// only read ledgers and [`emit`] their sizes. Tracking an owner again changes nothing.
pub fn track_ledgers<T: Send + Sync + 'static>(owner: &Arc<T>, report: fn(&T)) {
    let Some(world) = nabedra_rt::sim::world() else {
        return;
    };
    let weak = Arc::downgrade(owner);
    let source: LedgerSource =
        Box::new(move || weak.upgrade().map(|owner| report(&owner)).is_some());
    // The source's weak reference keeps the address from being reused.
    let address = Arc::as_ptr(owner).cast::<()>() as usize;
    let mut entry = Some((address, nabedra_rt::sim::current_node(), source));
    let known = world.with_ext::<LedgerSources, _>(|sources| {
        if sources.0.iter().all(|(tracked, ..)| *tracked != address) {
            sources.0.extend(entry.take());
        }
    });
    if known.is_none() {
        world.insert_ext(LedgerSources(entry.take().into_iter().collect()));
    }
}

/// Emits a `LedgerSnapshot` for every live ledger `node` owns.
pub fn snapshot_ledgers(node: u64) {
    let Some(world) = nabedra_rt::sim::world() else {
        return;
    };
    world.with_ext::<LedgerSources, _>(|sources| {
        sources
            .0
            .retain(|(_, owner, report)| *owner != Some(node) || report());
    });
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

    #[test]
    fn ledger_snapshots_cover_live_owners_of_the_current_node() {
        struct Owner(u64);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        nabedra_rt::sim::run(1, |world| async move {
            world.add_node(1, nabedra_rt::sim::NodeClock::default());
            world.add_node(2, nabedra_rt::sim::NodeClock::default());
            ProbeSink::new(move |probe| {
                if let Probe::LedgerSnapshot { outstanding, .. } = probe {
                    observed.lock().unwrap().push(outstanding);
                }
            })
            .install(&world);
            let track = |owner: &Arc<Owner>| {
                track_ledgers(owner, |owner| {
                    emit(|| Probe::LedgerSnapshot {
                        ledger: Ledger::NativeLeases,
                        group: None,
                        outstanding: owner.0,
                    });
                });
            };
            let (first, second, other) =
                (Arc::new(Owner(5)), Arc::new(Owner(6)), Arc::new(Owner(7)));
            nabedra_rt::sim::in_node(1, async { track(&first) }).await;
            nabedra_rt::sim::in_node(1, async { track(&first) }).await;
            nabedra_rt::sim::in_node(1, async { track(&second) }).await;
            nabedra_rt::sim::in_node(2, async { track(&other) }).await;
            snapshot_ledgers(1);
            drop(first);
            snapshot_ledgers(1);
            snapshot_ledgers(2);
        });
        assert_eq!(*seen.lock().unwrap(), [5, 6, 6, 7]);
    }
}
