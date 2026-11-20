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
//!    `DirectCommit` and awaits its apply;
//! 6. on a conflict from a leadership race, a stale timestamp or a waiter
//!    failed by a leadership change, goes back to step 3 within the deadline.
//!
//! `table.get` skips latches and timestamps: barrier, wait for any pending
//! commit on its key, read the newest version.
//!
//! Nothing here is on by default. The executor proposes only while its
//! `DirectGate` reports `data.direct.v1` active (T7 turns it on).
// T6 (gateway) and T7 (activation) construct and call parts of this
// (the Catalog schema, installation, the expiry task); until then they are
// unused outside tests, and some even in tests.
#![allow(dead_code)]
mod admission;
mod executor;
mod latches;
mod schema;
mod stats;
#[cfg(test)]
mod tests;

#[allow(unused_imports)] // T6 and T7 wire these.
pub(crate) use executor::{DirectConfig, DirectExecutor, DirectGate};
#[allow(unused_imports)]
pub(crate) use schema::{CatalogSchema, DirectSchema, DirectTable, SchemaLookup, StaticSchema};
#[allow(unused_imports)]
pub(crate) use stats::DirectStatsSnapshot;

/// The capability flag T4fix added to the worker runtime: set by the
/// `data.direct.v1` activation (T7), off by default.
impl DirectGate for super::worker_runtime::WorkerRuntime {
    fn direct_active(&self) -> bool {
        self.direct_commits_active()
    }
}
