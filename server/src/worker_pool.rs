//! Internal execution API, public for library wiring and integration tests.
//!
//! Startup retains `WorkerThreads` separately from cloneable dispatch handles. Jobs
//! run directly on their assigned local thread; use the supplied current-thread
//! runtime for IO, then perform synchronous data work outside `block_on`.
//! Callers must not submit/wait for nested pool jobs and must drain requests before
//! shutting down the pool. Per-operation admission windows and joining through
//! operation errors are the caller's responsibility. Keep the coordinator runtime
//! alive through teardown; explicit shutdown is the normal lifecycle path.

use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};
use tokio::{
    runtime::{Handle, Runtime},
    sync::{mpsc, oneshot, watch},
};

pub type WorkerId = usize;
type Work = Box<dyn FnOnce(WorkerId, &Runtime) + Send + 'static>;

struct Shared {
    // Taking this option is the stop boundary. Sending a reserved job uses the
    // same lock, so no job can be admitted after that boundary.
    senders: Mutex<Option<Vec<mpsc::Sender<Work>>>>,
    stopped: watch::Sender<bool>,
    count: usize,
}

impl Shared {
    fn stop(&self) {
        let mut senders = self.senders.lock().unwrap();
        senders.take();
        self.stopped.send_replace(true);
    }
}

#[derive(Clone)]
pub struct WorkerPool {
    shared: Arc<Shared>,
}

/// A result receiver. Dropping it never cancels the accepted job.
pub struct Job<T> {
    result: oneshot::Receiver<Result<T, ProtocolError>>,
}

impl<T> Job<T> {
    pub async fn receive(self) -> Result<T, ProtocolError> {
        self.result
            .await
            .unwrap_or_else(|_| Err(internal("Worker result unavailable")))
    }
}

/// Sanitized startup failure. No OS/runtime error text is exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStartError {
    pub worker_index: WorkerId,
}

impl fmt::Display for PoolStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Could not start worker {}", self.worker_index)
    }
}
impl std::error::Error for PoolStartError {}

struct Worker {
    thread: JoinHandle<()>,
    drained: oneshot::Receiver<()>,
    exit: oneshot::Sender<()>,
}

/// Noncloneable lifecycle owner. Never capture this owner in a worker job.
/// Drop closes admission and schedules detached cleanup without blocking.
pub struct WorkerThreads {
    shared: Arc<Shared>,
    workers: Option<Vec<Worker>>,
    coordinator: Handle,
}

impl WorkerPool {
    pub fn start(
        count: NonZeroUsize,
        coordinator_runtime: Handle,
    ) -> Result<(Self, WorkerThreads), PoolStartError> {
        // Grow only as threads actually start, never allocate from the requested
        // count eagerly. A failed startup closes and joins all partial workers.
        let (stopped, _) = watch::channel(false);
        let mut senders = Vec::new();
        let mut workers: Vec<Worker> = Vec::new();
        for id in 0..count.get() {
            let mut worker_stop = stopped.subscribe();
            let (sender, mut receiver) = mpsc::channel::<Work>(1);
            let (drained_tx, drained) = oneshot::channel();
            let (exit, exit_rx) = oneshot::channel();
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
            let spawned = thread::Builder::new()
                .name(format!("meshdb-worker-{id}"))
                .spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(_) => {
                            let _ = ready_tx.send(false);
                            return;
                        }
                    };
                    let _ = ready_tx.send(true);
                    let mut queue_closed = false;
                    while let Some(work) = runtime.block_on(async {
                        if !queue_closed {
                            tokio::select! {
                                biased;
                                _ = worker_stop.changed() => {
                                    receiver.close();
                                    queue_closed = true;
                                }
                                work = receiver.recv() => return work,
                            }
                        }
                        receiver.recv().await
                    }) {
                        // The typed envelope catches job panics and sends an error.
                        // This outer guard also protects the loop against envelope
                        // teardown panics, leaving a closed result channel sanitized.
                        let _ = catch_unwind(AssertUnwindSafe(|| work(id, &runtime)));
                    }
                    let _ = drained_tx.send(());
                    // An idle/drained worker must still drive spawned IO futures
                    // until every worker has finished its accepted jobs.
                    let _ = runtime.block_on(exit_rx);
                });
            if let Ok(thread) = spawned {
                senders.push(sender);
                workers.push(Worker {
                    thread,
                    drained,
                    exit,
                });
                if ready_rx.recv().unwrap_or(false) {
                    continue;
                }
            }
            stopped.send_replace(true);
            drop(senders);
            // Dropped exit gates allow partially started workers to leave after
            // draining. Join every thread even if one unexpectedly panicked.
            for worker in workers {
                drop(worker.exit);
                let _ = worker.thread.join();
            }
            return Err(PoolStartError { worker_index: id });
        }
        let shared = Arc::new(Shared {
            senders: Mutex::new(Some(senders)),
            stopped,
            count: count.get(),
        });
        Ok((
            Self {
                shared: shared.clone(),
            },
            WorkerThreads {
                shared,
                workers: Some(workers),
                coordinator: coordinator_runtime,
            },
        ))
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.shared.count).unwrap()
    }

    /// Routes the original partition index modulo the fixed worker count.
    /// At most one executing and one queued job per worker are accepted.
    pub async fn submit<T, F>(
        &self,
        partition_index: usize,
        work: F,
    ) -> Result<Job<T>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        let (sender, mut stopped) = {
            let senders = self.shared.senders.lock().unwrap();
            let senders = senders.as_ref().ok_or_else(stopping)?;
            (
                senders[partition_index % self.shared.count].clone(),
                self.shared.stopped.subscribe(),
            )
        };
        // No lock is held while waiting for capacity. Stop also wakes pending
        // reservations, dropping their sender clones so receivers can drain.
        let permit = tokio::select! {
            biased;
            _ = stopped.changed() => return Err(stopping()),
            permit = sender.reserve_owned() => permit.map_err(|_| internal("Worker queue unavailable"))?,
        };
        let senders = self.shared.senders.lock().unwrap();
        if senders.is_none() {
            return Err(stopping());
        }
        let (result_tx, result) = oneshot::channel();
        permit.send(Box::new(move |id, runtime| {
            let result = catch_unwind(AssertUnwindSafe(|| work(id, runtime)))
                .unwrap_or_else(|_| Err(internal("Worker job panicked")));
            let _ = result_tx.send(result);
        }));
        Ok(Job { result })
    }
}

impl WorkerThreads {
    /// Stops admission immediately, even if the returned future is never polled.
    /// Drains all accepted work, releases all exit gates, and joins every thread.
    pub fn shutdown(mut self) -> impl Future<Output = Result<(), ProtocolError>> + Send + 'static {
        let joining = self.begin_shutdown();
        async move {
            joining
                .await
                .unwrap_or_else(|_| Err(internal("Worker teardown unavailable")))
        }
    }

    fn begin_shutdown(&mut self) -> tokio::task::JoinHandle<Result<(), ProtocolError>> {
        self.shared.stop();
        let mut workers = self.workers.take().unwrap();
        let coordinator = self.coordinator.clone();
        self.coordinator.spawn_blocking(move || {
            let mut failed = false;
            coordinator.block_on(async {
                for worker in &mut workers {
                    if (&mut worker.drained).await.is_err() {
                        failed = true;
                    }
                }
            });
            let mut threads = Vec::new();
            for worker in workers {
                let _ = worker.exit.send(());
                threads.push(worker.thread);
            }
            for thread in threads {
                if thread.join().is_err() {
                    failed = true;
                }
            }
            if failed {
                Err(internal("Worker teardown failed"))
            } else {
                Ok(())
            }
        })
    }
}

impl Drop for WorkerThreads {
    fn drop(&mut self) {
        if self.workers.is_some() {
            drop(self.begin_shutdown());
        }
    }
}

fn internal(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::InternalError, message, Outcome::NotApplied)
}
fn stopping() -> ProtocolError {
    internal("Worker pool is stopping")
}
