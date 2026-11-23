//! The task seam.
//!
//! Product builds forward to tokio and `std::thread::Builder`. Under `sim`, spawned
//! tasks join the current node's registry so a simulated crash can abort them.
//! Blocking work completes before the caller resumes, and services use helper threads.
//!
//! # Stepped services
//!
//! A long-lived loop that other code waits on (nabedra-kv's journal, flush and
//! compaction) implements [`Stepped`] and starts with [`spawn_stepped`]. In a product
//! build it is a named OS thread running [`Stepped::run`], the existing loop. Under `sim`,
//! inside a world, it is a node task that calls [`Stepped::step`] on the simulation's
//! blocking helper, one atomic step at a time, and code that waits for a service calls
//! [`drive_until`], which steps the node's services inline. See `sim/services.rs`.

pub use tokio::task::{JoinError, JoinHandle, JoinSet};

use std::future::Future;

/// The outcome of one [`Stepped::step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// The service did some work and may have more.
    Worked,
    /// Nothing to do until the doorbell rings, or until the given time on the node's
    /// [`crate::time::now_std`] clock (a timed wait or a retry delay).
    Idle(Option<std::time::Instant>),
    /// The service has finished; its thread (or driver) ends.
    Exit,
}

/// A service loop that can run as a thread (product) or one step at a time (simulation).
///
/// The contract: `step` never waits for another service. Where the product loop would
/// block on its [`Stepped::doorbell`], `step` returns [`Step::Idle`]; where it would wait
/// for a service below it in the wait order, it drives that service with [`drive_until`].
pub trait Stepped: Send + Sized + 'static {
    /// One unit of work. Never waits for another service.
    fn step(&mut self) -> Step;

    /// The condition variable this service idles on. A notification on it wakes the
    /// simulated driver as well as the product thread.
    fn doorbell(&self) -> &crate::sync::Condvar;

    /// The product thread body: the existing loop. It calls the same `#[inline]` helpers
    /// as [`Stepped::step`] and waits on the doorbell exactly where `step` returns
    /// [`Step::Idle`].
    fn run(self);
}

/// Starts `service` as a named service.
///
/// Product: `spawn_service_with_stack(name, stack_bytes, move || service.run())`, the
/// same thread name, count and stack size. Under `sim`, inside a world: registers the
/// service with the current node and spawns a node task that steps it. Outside a world:
/// product behaviour.
#[inline]
pub fn spawn_stepped<S: Stepped>(
    name: impl Into<String>,
    stack_bytes: usize,
    service: S,
) -> std::io::Result<ServiceHandle> {
    #[cfg(feature = "sim")]
    {
        crate::sim::services::spawn_stepped(name.into(), stack_bytes, service)
    }
    #[cfg(not(feature = "sim"))]
    {
        spawn_service_with_stack(name, stack_bytes, move || service.run())
            .map(|thread| ServiceHandle { thread })
    }
}

/// The handle of a service started with [`spawn_stepped`].
#[cfg(not(feature = "sim"))]
#[derive(Debug)]
#[repr(transparent)]
pub struct ServiceHandle {
    thread: std::thread::JoinHandle<()>,
}

/// The handle of a service started with [`spawn_stepped`]: a thread, or under `sim` a
/// stepped service in the world's registry.
#[cfg(feature = "sim")]
#[derive(Debug)]
pub struct ServiceHandle {
    pub(crate) inner: crate::sim::services::Handle,
}

impl ServiceHandle {
    /// Waits for the service to finish. Under `sim` it drives the service (and the rest
    /// of its node's services) until it exits; a service of a crashed node is dead and
    /// joins at once. A live simulation service can only be joined from its own
    /// world, on the simulation thread or its blocking helper.
    #[inline]
    pub fn join(self) -> std::thread::Result<()> {
        #[cfg(feature = "sim")]
        {
            self.inner.join()
        }
        #[cfg(not(feature = "sim"))]
        {
            self.thread.join()
        }
    }

    /// Whether the service has finished (or, under `sim`, died in a crash).
    #[inline]
    pub fn is_finished(&self) -> bool {
        #[cfg(feature = "sim")]
        {
            self.inner.is_finished()
        }
        #[cfg(not(feature = "sim"))]
        {
            self.thread.is_finished()
        }
    }

    /// Whether the calling code runs inside this service: its thread in product, its
    /// step (the innermost one on the stack) under `sim`.
    #[inline]
    pub fn is_current(&self) -> bool {
        #[cfg(feature = "sim")]
        {
            self.inner.is_current()
        }
        #[cfg(not(feature = "sim"))]
        {
            self.thread.thread().id() == std::thread::current().id()
        }
    }
}

/// The service running this code: the thread name in product, the innermost stepped
/// service mid-step under `sim` (or the thread name when none is).
#[inline]
pub fn current_service() -> Option<String> {
    #[cfg(feature = "sim")]
    {
        crate::sim::services::current_service()
    }
    #[cfg(not(feature = "sim"))]
    {
        std::thread::current().name().map(str::to_owned)
    }
}

/// A one-shot named thread whose handle is dropped (for example `oltp-damage`).
///
/// Product: `spawn_service_with_stack(name, stack_bytes, f)` with the handle dropped.
/// Under `sim`, inside a world: a deferred job that a node task runs on the blocking
/// helper, so it runs after the current step, deterministically, and a crash of the node
/// before it starts cancels it.
#[inline]
pub fn spawn_oneshot<F>(name: impl Into<String>, stack_bytes: usize, f: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    #[cfg(feature = "sim")]
    {
        crate::sim::services::spawn_oneshot(name.into(), stack_bytes, f)
    }
    #[cfg(not(feature = "sim"))]
    {
        spawn_service_with_stack(name, stack_bytes, f).map(drop)
    }
}

/// Drives the current node's stepped services until `ready` returns `Some`.
///
/// Inside a world: calls `ready`, and while it returns `None`, steps this node's services
/// in registration order, skipping any service already mid-step on this stack. When a
/// full pass makes no progress and a service idles until a deadline, it advances virtual
/// time to the earliest one (a stop-the-world pause). It panics with `site` and every
/// service's state when nothing can progress, so a wait cycle is a deterministic failure
/// rather than a hang.
///
/// Outside a world, and in every product build, it returns `None` without calling
/// `ready`, and the caller falls back to its blocking wait.
#[inline]
pub fn drive_until<T>(site: &'static str, ready: impl FnMut() -> Option<T>) -> Option<T> {
    #[cfg(feature = "sim")]
    {
        crate::sim::services::drive_until(site, ready)
    }
    #[cfg(not(feature = "sim"))]
    {
        let _ = (site, ready);
        None
    }
}

/// Runs `future` to completion on the calling thread (simulation only).
///
/// Inside a world it polls the future with the world's runtime entered and, between
/// polls, drives the current node's services. A future that needs virtual time or
/// another task to make progress cannot complete this way, and the call panics with the
/// caller's location. Outside a world it is an ordinary thread-parking `block_on`.
#[cfg(feature = "sim")]
#[track_caller]
pub fn block_on_inline<F: Future>(future: F) -> F::Output {
    crate::sim::services::block_on_inline(future, std::panic::Location::caller())
}

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
