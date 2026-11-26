//! Range-leader execution of point operations (plan section 6, T5).
//!
//! A gateway sends an eligible request (`table.get`, `insert`, `upsert`,
//! `update`, `delete`, or a one-range `insert_many`) to the leader of the
//! key range's Data group. The leader:
//!
//! 1. admits it against the worker's in-flight and memory budgets, or
//!    answers a typed `429 overloaded` with `not_applied` (`admission`);
//! 2. checks leadership and both epochs, and latches the row keys in a
//!    fixed order, bounded by the deadline (`latches`);
//! 3. takes the leader token, a linearizable barrier (leader lease first),
//!    and looks up the request's dedup record before any fallback decision;
//!    then reads each key's newest version from local applied state;
//! 4. computes the new rows, index changes and the exact result with the
//!    queued planner's row and index functions;
//! 5. allocates `commit_ts`, registers it with the range clock, proposes one
//!    `DirectCommit` and awaits its apply; while `data.direct.batch` is
//!    enabled, commits that arrive while the range's proposal is in flight
//!    share one `DirectBatch` entry (GC2, `batcher`);
//! 6. on a conflict from a leadership race, a stale timestamp or a waiter
//!    failed by a leadership change, goes back to step 3 within the deadline.
//!
//! `table.upsert_many` and `table.delete_many` run each row as a request of
//! its own, in windows that share latching and entries.
//!
//! `table.get` skips latches and timestamps: barrier, wait for any pending
//! commit on its key, read the newest version.
//!
//! Nothing here is on by default. The executor proposes only while its
//! `DirectGate` reports `data.direct.v1` active (T7 turns it on).
mod admission;
mod batcher;
mod executor;
mod latches;
mod schema;
mod stats;
#[cfg(test)]
mod tests;

#[allow(unused_imports)] // T6 and T7 wire these.
pub(crate) use executor::{DirectConfig, DirectExecutor, DirectGate};
#[cfg(test)]
pub(crate) use schema::StaticSchema;
#[allow(unused_imports)]
pub(crate) use schema::{CatalogSchema, DirectSchema, DirectTable, SchemaLookup};
#[allow(unused_imports)]
pub(crate) use stats::DirectStatsSnapshot;

/// The capability flag T4fix added to the worker runtime: set by the
/// `data.direct.v1` activation (T7), off by default.
impl DirectGate for super::worker_runtime::WorkerRuntime {
    fn direct_active(&self) -> bool {
        self.direct_commits_active()
    }
}

/// Test-only tracing for the direct-path suites: with
/// `NABEDRA_DIRECT_TRACE=<targets>` (for example
/// `nabedra_database_server::oltp_replication::hosted=warn,openraft=info`)
/// the process logs those targets to stderr. Without it, nothing changes.
#[cfg(test)]
pub(crate) fn trace_for_test() {
    use tracing_subscriber::prelude::*;
    let Some(targets) = std::env::var("NABEDRA_DIRECT_TRACE").ok() else {
        return;
    };
    let Ok(targets) = targets.parse::<tracing_subscriber::filter::Targets>() else {
        return;
    };
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_filter(targets),
        )
        .try_init();
}
