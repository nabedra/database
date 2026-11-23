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
use crate::{config::WorkerResources, scheduling::ResourceScheduler};
use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    thread::JoinHandle,
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
    senders: Mutex<Option<mpsc::Sender<Work>>>,
    scheduler: ResourceScheduler,
    resources: WorkerResources,
    next_request: std::sync::atomic::AtomicU64,
    stopped: watch::Sender<bool>,
    count: usize,
}

impl Shared {
    fn stop(&self) {
        let mut senders = self.senders.lock().unwrap();
        senders.take();
        self.scheduler.close();
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
        let resources = WorkerResources {
            max_threads: count.get() as u64,
            ..WorkerResources::default()
        };
        Self::start_with_resources(resources, coordinator_runtime)
    }

    pub fn start_with_resources(
        resources: WorkerResources,
        coordinator_runtime: Handle,
    ) -> Result<(Self, WorkerThreads), PoolStartError> {
        resources
            .validate()
            .map_err(|_| PoolStartError { worker_index: 0 })?;
        let count = NonZeroUsize::new(resources.max_threads as usize)
            .ok_or(PoolStartError { worker_index: 0 })?;
        let scheduler = ResourceScheduler::new(vec![(0, resources)])
            .map_err(|_| PoolStartError { worker_index: 0 })?;
        // Grow only as threads actually start, never allocate from the requested
        // count eagerly. A failed startup closes and joins all partial workers.
        let (stopped, _) = watch::channel(false);
        let (sender, receiver) = mpsc::channel::<Work>(count.get());
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let mut workers: Vec<Worker> = Vec::new();
        for id in 0..count.get() {
            let mut worker_stop = stopped.subscribe();
            let receiver = receiver.clone();
            let (drained_tx, drained) = oneshot::channel();
            let (exit, exit_rx) = oneshot::channel();
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
            let spawned =
                nabedra_rt::task::spawn_service(format!("nabedra-worker-{id}"), move || {
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
                    while let Some(work) = runtime.block_on(async {
                        let mut receiver = receiver.lock().await;
                        tokio::select! {
                            biased;
                            _ = worker_stop.changed() => {receiver.close(); receiver.recv().await}
                            work = receiver.recv() => work,
                        }
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
            drop(sender);
            // Dropped exit gates allow partially started workers to leave after
            // draining. Join every thread even if one unexpectedly panicked.
            for worker in workers {
                drop(worker.exit);
                let _ = worker.thread.join();
            }
            return Err(PoolStartError { worker_index: id });
        }
        let shared = Arc::new(Shared {
            senders: Mutex::new(Some(sender)),
            scheduler,
            resources,
            next_request: std::sync::atomic::AtomicU64::new(1),
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

    pub fn resources(&self) -> WorkerResources {
        self.shared.resources
    }
    pub fn scheduler(&self) -> &ResourceScheduler {
        &self.shared.scheduler
    }
    pub fn request_id(&self) -> u64 {
        self.shared
            .next_request
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Jobs use whichever executor becomes free; partition identity never selects a thread.
    pub async fn submit<T, F>(
        &self,
        _partition_index: usize,
        work: F,
    ) -> Result<Job<T>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        self.submit_request(
            self.request_id(),
            1,
            self.shared.resources.task_memory_limit_bytes,
            work,
        )
        .await
    }

    pub async fn submit_request<T, F>(
        &self,
        request_id: u64,
        max_workers: usize,
        bytes: u64,
        work: F,
    ) -> Result<Job<T>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        let (sender, mut stopped) = {
            let senders = self.shared.senders.lock().unwrap();
            (
                senders.as_ref().ok_or_else(stopping)?.clone(),
                self.shared.stopped.subscribe(),
            )
        };
        let reservation = tokio::select! {
            biased;
            _=stopped.changed()=>return Err(stopping()),
            grant=self.shared.scheduler.reserve(request_id,max_workers,bytes)=>grant?,
        };
        self.enqueue_reserved(reservation, sender, stopped, work)
            .await
    }
    pub async fn submit_precharged<T, F>(
        &self,
        request_id: u64,
        max_workers: usize,
        charge: crate::scheduling::MemoryCharge,
        work: F,
    ) -> Result<Job<T>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        let (sender, mut stopped) = {
            let senders = self.shared.senders.lock().unwrap();
            (
                senders.as_ref().ok_or_else(stopping)?.clone(),
                self.shared.stopped.subscribe(),
            )
        };
        let reservation = tokio::select! {
            biased;
            _=stopped.changed()=>return Err(stopping()),
            grant=self.shared.scheduler.reserve_precharged(request_id,max_workers,charge)=>grant?,
        };
        self.enqueue_reserved(reservation, sender, stopped, work)
            .await
    }
    pub async fn submit_buffered<T, F>(
        &self,
        request_id: u64,
        max_workers: usize,
        charges: Vec<crate::scheduling::MemoryCharge>,
        budget: u64,
        work: F,
    ) -> Result<Job<(T, crate::scheduling::MemoryCharge)>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        let (sender, mut stopped) = {
            let senders = self.shared.senders.lock().unwrap();
            (
                senders.as_ref().ok_or_else(stopping)?.clone(),
                self.shared.stopped.subscribe(),
            )
        };
        let reservation = tokio::select! {
            biased;
            _=stopped.changed()=>return Err(stopping()),
            grant=self.shared.scheduler.reserve_with_memories(request_id,max_workers,charges,budget)=>grant?,
        };
        let permit = tokio::select! {
            biased;
            _=stopped.changed()=>return Err(stopping()),
            permit=sender.reserve_owned()=>permit.map_err(|_|stopping())?,
        };
        let senders = self.shared.senders.lock().unwrap();
        if senders.is_none() {
            return Err(stopping());
        }
        let (result_tx, result) = oneshot::channel();
        permit.send(Box::new(move |id, runtime| {
            let result = catch_unwind(AssertUnwindSafe(|| {
                crate::scheduling::with_task_limit(budget, || work(id, runtime))
            }))
            .unwrap_or_else(|_| Err(internal("Worker job panicked")));
            let result = match result {
                Ok(value) => Ok((value, reservation.into_memory())),
                Err(error) => {
                    drop(reservation);
                    Err(error)
                }
            };
            let _ = result_tx.send(result);
        }));
        Ok(Job { result })
    }
    async fn enqueue_reserved<T, F>(
        &self,
        reservation: crate::scheduling::Reservation,
        sender: mpsc::Sender<Work>,
        mut stopped: watch::Receiver<bool>,
        work: F,
    ) -> Result<Job<T>, ProtocolError>
    where
        T: Send + 'static,
        F: FnOnce(WorkerId, &Runtime) -> Result<T, ProtocolError> + Send + 'static,
    {
        let bytes = reservation.bytes();
        let permit = tokio::select! {
            biased;
            _=stopped.changed()=>return Err(stopping()),
            permit=sender.reserve_owned()=>permit.map_err(|_|stopping())?,
        };
        let senders = self.shared.senders.lock().unwrap();
        if senders.is_none() {
            return Err(stopping());
        }
        let (result_tx, result) = oneshot::channel();
        permit.send(Box::new(move |id, runtime| {
            let result = catch_unwind(AssertUnwindSafe(|| {
                crate::scheduling::with_task_limit(bytes, || work(id, runtime))
            }))
            .unwrap_or_else(|_| Err(internal("Worker job panicked")));
            drop(reservation);
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
        #[cfg(feature = "sim")]
        if nabedra_rt::sim::world().is_some() {
            return nabedra_rt::task::spawn_on(&self.coordinator, async move {
                // Accepted jobs can depend on the coordinator. Wait for their drain
                // before using the blocking helper, which pauses the simulated runtime.
                let failed = wait_for_drain(&mut workers).await;
                nabedra_rt::task::spawn_blocking_on(&coordinator, move || {
                    join_drained_workers(workers, failed)
                })
                .await
                .unwrap_or_else(|_| Err(internal("Worker teardown unavailable")))
            });
        }
        nabedra_rt::task::spawn_blocking_on(&self.coordinator, move || {
            let failed = coordinator.block_on(wait_for_drain(&mut workers));
            join_drained_workers(workers, failed)
        })
    }
}

async fn wait_for_drain(workers: &mut [Worker]) -> bool {
    let mut failed = false;
    for worker in workers {
        failed |= (&mut worker.drained).await.is_err();
    }
    failed
}

fn join_drained_workers(workers: Vec<Worker>, mut failed: bool) -> Result<(), ProtocolError> {
    let mut threads = Vec::new();
    for worker in workers {
        let _ = worker.exit.send(());
        threads.push(worker.thread);
    }
    for thread in threads {
        failed |= thread.join().is_err();
    }
    if failed {
        Err(internal("Worker teardown failed"))
    } else {
        Ok(())
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

#[cfg(test)]
mod scheduling_tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    fn poll_once<F: Future + ?Sized>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        std::pin::pin!(tokio::task::unconstrained(future))
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn m02_m03_f03_two_buffered_requests_terminate_under_memory_pressure() {
        let resources = WorkerResources {
            max_threads: 2,
            memory_limit_bytes: 1000,
            task_memory_limit_bytes: 800,
            control_memory_reserve_bytes: 100,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, Handle::current()).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let mut release = Vec::new();
        let mut jobs = Vec::new();
        for request in 1..=2 {
            let charge = pool
                .scheduler()
                .reserve_memory(request, 0, 400)
                .await
                .unwrap();
            let entered = entered_tx.clone();
            let (release_tx, release_rx) = mpsc::channel();
            release.push(release_tx);
            jobs.push(
                pool.submit_buffered(request, 1, vec![charge], 400, move |id, _| {
                    entered.send(id).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok(vec![0u8; 100])
                })
                .await
                .unwrap(),
            );
        }
        // Both real executors must enter before either bounded barrier opens.
        let first = entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let second = entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_ne!(first, second);
        for release in release {
            release.send(()).unwrap();
        }
        let mut retained = Vec::new();
        for job in jobs {
            retained.push(job.receive().await.unwrap());
        }
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 800)]);
        for request in 1..=2 {
            assert!(
                pool.scheduler()
                    .reserve_memory(request, 0, 800)
                    .await
                    .is_err()
            );
        }
        let (value, charge) = retained.pop().unwrap();
        drop(value);
        assert!(
            pool.submit_buffered(3, 1, vec![charge], 800, |_, _| Ok(()))
                .await
                .is_err()
        );
        drop(retained);
        let subsequent = pool.submit_request(4, 1, 800, |_, _| Ok(42)).await.unwrap();
        assert_eq!(subsequent.receive().await.unwrap(), 42);
        threads.shutdown().await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    }
    #[cfg(feature = "sim")]
    #[test]
    fn f03_shutdown_allows_the_coordinator_to_finish_accepted_work() {
        nabedra_rt::sim::run(50, |_| async {
            let (pool, threads) = WorkerPool::start(NonZeroUsize::MIN, Handle::current()).unwrap();
            let (release, wait) = mpsc::channel();
            let job = pool
                .submit(0, move |_, _| {
                    wait.recv_timeout(Duration::from_secs(2))
                        .map_err(|_| internal("Shutdown stalled coordinator progress"))?;
                    Ok(42)
                })
                .await
                .unwrap();

            // Shutdown closes admission immediately. This coordinator must be able to
            // release the admitted job before the returned future is polled.
            let shutdown = threads.shutdown();
            assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
            let _ = release.send(());

            shutdown.await.unwrap();
            assert_eq!(job.receive().await.unwrap(), 42);
            assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn f03_pool_stop_wakes_download_memory_waiter() {
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 1000,
            task_memory_limit_bytes: 900,
            control_memory_reserve_bytes: 100,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, Handle::current()).unwrap();
        let held = pool.scheduler().reserve_memory(1, 0, 900).await.unwrap();
        let mut waiter = Box::pin(pool.scheduler().reserve_memory(2, 0, 100));
        assert!(poll_once(waiter.as_mut()).is_pending());
        let shutdown = threads.shutdown();
        assert!(waiter.await.is_err());
        drop(held);
        shutdown.await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn c06_f01_f02_f03_actual_overlap_refill_errors_panic_and_receiver_drop() {
        let (pool, threads) =
            WorkerPool::start(NonZeroUsize::new(2).unwrap(), Handle::current()).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let slow = pool
            .submit(0, move |id, _| {
                entered_tx.send(id).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(10)
            })
            .await
            .unwrap();
        let slow_id = entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let fast = pool
            .submit(0, move |id, _| Ok(id))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_ne!(slow_id, fast);
        // Same original index can refill the free executor while the first is gated.
        let next = pool
            .submit(0, move |id, _| Ok(id))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(next, fast);
        for code in [
            ErrorCode::ValidationError,
            ErrorCode::StorageUnavailable,
            ErrorCode::InternalError,
            ErrorCode::LimitExceeded,
        ] {
            let failed: Job<()> = pool
                .submit(0, move |_, _| {
                    Err(ProtocolError::new(
                        code,
                        "injected ordinary task failure",
                        Outcome::NotApplied,
                    ))
                })
                .await
                .unwrap();
            let error = failed.receive().await.unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.outcome, Outcome::NotApplied);
            let subsequent = pool.submit(0, |id, _| Ok(id)).await.unwrap();
            assert_eq!(subsequent.receive().await.unwrap(), fast);
        }
        let panic: Job<()> = pool
            .submit(0, |_, _| panic!("injected unwind"))
            .await
            .unwrap();
        assert!(panic.receive().await.is_err());
        let (done_tx, done_rx) = mpsc::channel();
        let abandoned = pool
            .submit(0, move |_, _| {
                done_tx.send(()).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        drop(abandoned);
        done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(slow.receive().await.unwrap(), 10);
        let final_job = pool.submit(0, |_, _| Ok(99)).await.unwrap();
        assert_eq!(final_job.receive().await.unwrap(), 99);
        threads.shutdown().await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    }
}
