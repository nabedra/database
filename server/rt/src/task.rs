//! The task seam.
//!
//! Product builds forward to tokio and `std::thread::Builder`. Under `sim`, spawned
//! tasks join the current node's registry so a simulated crash can abort them.
//! Blocking work completes before the caller resumes, and services use helper threads.

pub use tokio::task::{JoinError, JoinHandle, JoinSet};

use std::future::Future;

/// [`tokio::spawn`]. Under `sim` the task inherits the current node context and joins
/// that node's task registry.
#[inline]
#[track_caller]
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

/// [`tokio::runtime::Handle::spawn`]. Under `sim` the task inherits the current node
/// context and joins that node's task registry. The handle must belong to the world's
/// runtime while a simulated world is installed.
#[inline]
#[track_caller]
pub fn spawn_on<F>(handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_on(handle, future)
    }
    #[cfg(not(feature = "sim"))]
    {
        handle.spawn(future)
    }
}

/// [`tokio::task::spawn_blocking`]. Under `sim` the closure runs to completion inline,
/// in the current node context, before this returns; the handle then resolves at once.
/// No OS scheduling decision reaches the simulated schedule.
#[inline]
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_blocking(f)
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::task::spawn_blocking(f)
    }
}

/// [`tokio::runtime::Handle::spawn_blocking`]: blocking work on the given runtime's
/// blocking pool, for code that holds a handle rather than running inside the runtime.
/// Under `sim` it runs inline like [`spawn_blocking`]; the handle must belong to the
/// world's runtime while a simulated world is installed. The closure must be able
/// to finish without progress on the simulated runtime.
#[inline]
#[track_caller]
pub fn spawn_blocking_on<F, R>(handle: &tokio::runtime::Handle, f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_blocking_on(handle, f)
    }
    #[cfg(not(feature = "sim"))]
    {
        handle.spawn_blocking(f)
    }
}

/// [`JoinSet::spawn`]: spawns `future` into `set` on the current runtime. Under `sim` the
/// task inherits the current node context and joins that node's task registry, as with
/// [`spawn`], so a task such as a Cache session handler reads its node's clock and a
/// simulated crash aborts it.
#[inline]
#[track_caller]
pub fn spawn_in<T, F>(set: &mut JoinSet<T>, future: F) -> tokio::task::AbortHandle
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_in(set, future)
    }
    #[cfg(not(feature = "sim"))]
    {
        set.spawn(future)
    }
}

/// A long-lived named OS thread (journal, flush, compaction, worker loops).
///
/// Product: `std::thread::Builder::new().name(name).spawn(f)`.
///
/// Under `sim` the service runs on its own helper OS thread, with the spawning node's
/// context installed on that thread and registered so a crash marks it. See
/// the `sim` module for why this phase uses a helper thread and what phase 9 changes.
#[inline]
pub fn spawn_service<F, T>(
    name: impl Into<String>,
    f: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_service(name.into(), f)
    }
    #[cfg(not(feature = "sim"))]
    {
        std::thread::Builder::new().name(name.into()).spawn(f)
    }
}

/// [`spawn_service`] with an explicit stack size, for services that budget their thread
/// stacks (nabedra-kv's threads, scenario O05).
///
/// Product: `std::thread::Builder::new().name(name).stack_size(stack_bytes).spawn(f)`.
/// Under `sim` it is the same helper thread as [`spawn_service`], with this stack size.
#[inline]
pub fn spawn_service_with_stack<F, T>(
    name: impl Into<String>,
    stack_bytes: usize,
    f: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::spawn_service_with_stack(name.into(), Some(stack_bytes), f)
    }
    #[cfg(not(feature = "sim"))]
    {
        std::thread::Builder::new()
            .name(name.into())
            .stack_size(stack_bytes)
            .spawn(f)
    }
}
