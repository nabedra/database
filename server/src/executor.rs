//! Bounded admission and detached execution on the caller's Tokio runtime.
//!
//! Authenticate before `try_admit`. Move the unread request body into the closure
//! passed to `Admission::spawn`; the closure is invoked only with a worker permit.
//! Dropping `Operation` or its `receive` future detaches work. It never aborts it.
//! Keep the runtime alive until `Executor::drain` returns. Engine code must await
//! its sequential `spawn_blocking` work inside this operation before returning.
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct Executor {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    workers: Semaphore,
    capacity: usize,
    state: Mutex<State>,
    drained: Notify,
}

#[derive(Debug)]
struct State {
    accepting: bool,
    admitted: usize,
}

/// A reserved admission slot. Dropping an unused guard returns the slot.
/// Existing guards may still submit work after `stop_accepting`.
#[derive(Debug)]
#[must_use]
pub struct Admission {
    inner: Arc<Inner>,
}

/// The result of an independently owned operation. No abort handle is exposed.
#[derive(Debug)]
#[must_use]
pub struct Operation<T> {
    task: JoinHandle<Result<T, ProtocolError>>,
}

impl Executor {
    /// `queue` counts additional admitted waiters beyond `workers`.
    pub fn new(workers: usize, queue: usize) -> Result<Self, ProtocolError> {
        let capacity = workers.checked_add(queue).ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "workers + queue overflows usize",
                Outcome::NotApplied,
            )
        })?;
        if workers == 0 || capacity > Semaphore::MAX_PERMITS {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "workers must be positive and workers + queue must fit Tokio semaphore bounds",
                Outcome::NotApplied,
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                workers: Semaphore::new(workers),
                capacity,
                state: Mutex::new(State {
                    accepting: true,
                    admitted: 0,
                }),
                drained: Notify::new(),
            }),
        })
    }

    /// Immediate admission only. Full and stopped executors return HTTP-429-ready
    /// `Overloaded` errors without reading the request body.
    pub fn try_admit(&self) -> Result<Admission, ProtocolError> {
        let mut state = self.inner.state.lock().expect("executor state poisoned");
        if !state.accepting || state.admitted == self.inner.capacity {
            return Err(ProtocolError::new(
                ErrorCode::Overloaded,
                if state.accepting {
                    "executor admission capacity exhausted"
                } else {
                    "executor is stopping"
                },
                Outcome::NotApplied,
            ));
        }
        state.admitted += 1;
        Ok(Admission {
            inner: self.inner.clone(),
        })
    }

    /// Linearizes with admission. Does not close the worker semaphore, so queued
    /// operations and previously issued guards can continue.
    pub fn stop_accepting(&self) {
        self.inner
            .state
            .lock()
            .expect("executor state poisoned")
            .accepting = false;
    }

    /// Stops admission and waits for every reserved, queued and running operation,
    /// including disconnected callers. May be called concurrently or cancelled
    /// and awaited again. An unused guard must be submitted or dropped to drain.
    pub async fn drain(&self) {
        self.stop_accepting();
        loop {
            let notified = self.inner.drained.notified();
            tokio::pin!(notified);
            // Register before checking the count, including for multiple drainers.
            notified.as_mut().enable();
            if self
                .inner
                .state
                .lock()
                .expect("executor state poisoned")
                .admitted
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}

impl Admission {
    /// Transfers this slot into a detached task. Even constructing the work
    /// future happens after execution admission. Captures must be owned and Send.
    /// No runtime is created; submission outside Tokio returns an internal error.
    pub fn spawn<F, Fut, T>(self, work: F) -> Result<Operation<T>, ProtocolError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ProtocolError>> + Send + 'static,
        T: Send + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| {
            ProtocolError::new(
                ErrorCode::InternalError,
                "executor submission requires a Tokio runtime",
                Outcome::NotApplied,
            )
        })?;
        let task = nabedra_rt::task::spawn_on(&runtime, async move {
            let execution = self.inner.workers.acquire().await.map_err(|_| {
                ProtocolError::new(
                    ErrorCode::InternalError,
                    "executor worker semaphore closed",
                    Outcome::NotApplied,
                )
            })?;
            let result = work().await;
            drop(execution);
            drop(self);
            result
        });
        Ok(Operation { task })
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("executor state poisoned");
        state.admitted -= 1;
        if state.admitted == 0 {
            self.inner.drained.notify_waiters();
        }
    }
}

impl<T> Operation<T> {
    /// Returns the work result unchanged. A task panic or runtime cancellation
    /// becomes an internal error with unknown outcome; neither is retried.
    pub async fn receive(self) -> Result<T, ProtocolError> {
        self.task.await.unwrap_or_else(|error| {
            Err(ProtocolError::new(
                ErrorCode::InternalError,
                if error.is_panic() {
                    "executor operation panicked"
                } else {
                    "executor operation cancelled"
                },
                Outcome::Unknown,
            ))
        })
    }
}
