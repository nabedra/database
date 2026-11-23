//! Runtime seams: time, ids, task spawning and blocking storage work.
//!
//! Thin re-exports of `nabedra_rt`, the server's runtime seam. Product builds forward to
//! tokio, std and uuid through its inline wrappers and behave exactly as direct calls.
//! With the `sim` feature (`nabedra-rt/sim`) they read the simulated node context of the
//! `nabedra_rt::sim` world: a per-node clock, a seeded id stream, a per-node task registry
//! (so a simulated crash can abort the node's tasks) and inline blocking work, which keeps
//! a whole cluster on one deterministic thread.

pub use nabedra_rt::ids::uuid_v4;
pub use nabedra_rt::task::{JoinHandle, spawn};
pub use nabedra_rt::time::{
    Instant, now, sleep, sleep_until, timeout, timeout_at, unix_ms, wall_now,
};

/// Runs blocking work off the async workers ([`nabedra_rt::task::spawn_blocking`]).
/// Simulation runs it to completion while the simulation thread waits, so no OS scheduling
/// decision reaches the schedule.
#[inline]
pub async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> anyhow::Result<T> {
    Ok(nabedra_rt::task::spawn_blocking(work).await?)
}
