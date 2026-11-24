// The Raft harness requires Tokio's seeded executor. The component model can
// run in an ordinary cache,sim build. The release checks assert discovery of
// seed_sweep when building with --cfg tokio_unstable.
#![allow(
    unexpected_cfgs,
    reason = "tokio_unstable is the opt-in cfg for the seeded Tokio executor"
)]

mod clock_bound;
pub(super) mod hooks;

#[cfg(test)]
mod lease_failover;

#[cfg(all(test, tokio_unstable))]
mod harness;
#[cfg(all(test, tokio_unstable))]
mod history;
#[cfg(all(test, tokio_unstable))]
mod invariants;
#[cfg(all(test, tokio_unstable))]
mod tests;
