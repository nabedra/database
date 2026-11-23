//! Nabedra's deterministic simulation core (testing plan phase 10). Dev-only: no product
//! crate depends on it, and it depends on no product crate and on no openraft.
//!
//! The per-node context (clocks, task registry, seeded ids) is the `World` in
//! `nabedra_rt::sim`. This crate holds what every simulator harness shares on top of it:
//!
//! - [`trace`]: the FNV-1a trace hash with a bounded tail, the fingerprint of one run;
//! - [`net`]: [`NetPlan`], which plans each message (drop, delay, duplicate) from the net
//!   stream and keeps directed blocks, partitions and paused nodes;
//! - [`nemesis`]: weighted nemesis profiles drawn from a [`nabedra_check::Gen`] tape, and
//!   a timeline of follow-up actions;
//! - [`raft`]: [`RaftObserver`], the Raft safety invariants over
//!   `(group, node, term, index, payload hash)` observations;
//! - [`sweep`]: the seed sweep (seed ranges, `NABEDRA_SIM_JOBS` threads, the double-run
//!   determinism check, failure artifacts) and the planted-bug search;
//! - [`shrink`]: the adapter from a failing run to `nabedra_check::prop::shrink`;
//! - [`rng`] and [`runtime`]: stream derivation and the seeded paused runtime.
//!
//! The SplitMix64, FNV, `NetPlan::plan` and observer logic come from the Events phase 6
//! simulator (`server/events/src/sim`). Where it was cheap they are byte-compatible with
//! it, and `tests` pins the values, so the Events migration can tell whether its trace
//! hashes change.

pub mod nemesis;
pub mod net;
pub mod raft;
pub mod rng;
pub mod runtime;
pub mod shrink;
pub mod sweep;
pub mod trace;

#[cfg(test)]
mod tests;

pub use nemesis::{Nemesis, Profile, Timeline};
pub use net::{Filters, NetPlan, Plan};
pub use raft::{LeaderId, LogId, RaftObserver, Vote};
pub use rng::{Draw, SplitMix64};
pub use sweep::{PlantedSearch, SeedRun, SweepConfig, SweepReport};
pub use trace::{Fnv, Trace, fnv};
