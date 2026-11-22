//! Runtime seams: time, ids, task spawning and blocking storage work.
//!
//! Product builds reach tokio and std through these inline wrappers and behave exactly as
//! direct calls. With the `sim` feature they read the simulated node context in
//! [`crate::sim`]: a per-node clock offset, a seeded id stream, a per-node task registry
//! (so a simulated crash can abort the node's tasks) and inline blocking work, which keeps
//! a whole cluster on one deterministic thread.

use std::future::Future;
use std::time::SystemTime;

pub use tokio::task::JoinHandle;
pub use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

/// The monotonic clock of the current node.
#[inline]
pub fn now() -> Instant {
    #[cfg(feature = "sim")]
    {
        crate::sim::mono_now()
    }
    #[cfg(not(feature = "sim"))]
    {
        Instant::now()
    }
}

/// The wall clock of the current node.
#[inline]
pub fn wall_now() -> SystemTime {
    #[cfg(feature = "sim")]
    {
        crate::sim::wall_now()
    }
    #[cfg(not(feature = "sim"))]
    {
        SystemTime::now()
    }
}

/// A random (version 4) UUID. Seeded per node in simulation.
#[inline]
pub fn uuid_v4() -> Uuid {
    #[cfg(feature = "sim")]
    {
        crate::sim::uuid_v4()
    }
    #[cfg(not(feature = "sim"))]
    {
        Uuid::new_v4()
    }
}

/// Spawns a task. In simulation the task joins the current node's registry.
#[inline]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn(future)
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::spawn(future)
    }
}

/// Sleeps until `deadline` on the current node's monotonic clock ([`now`]).
#[inline]
pub fn sleep_until(deadline: Instant) -> tokio::time::Sleep {
    tokio::time::sleep_until(to_tokio(deadline))
}

/// [`tokio::time::timeout_at`] for a deadline on the current node's clock ([`now`]).
#[inline]
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> tokio::time::Timeout<F> {
    tokio::time::timeout_at(to_tokio(deadline), future)
}

#[inline]
fn to_tokio(deadline: Instant) -> Instant {
    #[cfg(feature = "sim")]
    {
        crate::sim::to_tokio_instant(deadline)
    }
    #[cfg(not(feature = "sim"))]
    {
        deadline
    }
}

/// Runs blocking work off the async workers. Simulation runs it to completion while the
/// simulation thread waits, so no OS scheduling decision reaches the schedule.
#[inline]
pub async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> anyhow::Result<T> {
    #[cfg(feature = "sim")]
    {
        Ok(crate::sim::run_blocking(work))
    }
    #[cfg(not(feature = "sim"))]
    {
        Ok(tokio::task::spawn_blocking(work).await?)
    }
}

/// Milliseconds since the Unix epoch on the current node's wall clock.
#[inline]
pub fn unix_ms() -> u64 {
    wall_now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
