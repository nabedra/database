//! Internal execution API, public for library wiring and integration tests.
//!
//! Startup retains `WorkerThreads` separately from cloneable dispatch handles. Jobs
//! run directly on their assigned local thread; use the supplied current-thread
//! runtime for IO, then perform synchronous data work outside `block_on`.
//! Callers must not submit/wait for nested pool jobs and must drain requests before
//! shutting down the pool. Per-operation admission windows and joining through
//! operation errors are the caller's responsibility. Keep the coordinator runtime
//! alive through teardown; explicit shutdown is the normal lifecycle path.
//!
//! # Simulation
//!
//! With the `sim` feature, inside a `nabedra_rt::sim` world, the pool starts no thread
//! and builds no runtime. It spawns one node task per worker, so the slot count is the
//! task count and every job still sees its `WorkerId` in `0..count`. Each task runs the
//! same receive loop (the biased stop `select!`), holds its slot for an optional
//! virtual cost ([`sim::JobCost`], zero by default), then runs the job on the
//! simulation's blocking helper as a one-shot named `nabedra-worker-{id}`, under the
//! same `catch_unwind`. [`WorkerRuntime::block_on`] then polls the future inline: it
//! must complete without virtual time or another task (until phase 13, OLAP job IO
//! stays synchronous), or the job panics with the caller's location and the outer
//! boundary contains it. Shutdown awaits the drained workers and their tasks in a
//! coordinator task. Outside a world, a `sim` build behaves as a product build.

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
type Work = Box<dyn FnOnce(WorkerId, &WorkerRuntime) + Send + 'static>;

/// The runtime a job uses for IO. Do synchronous data work outside `block_on`.
///
/// Product: the worker thread's own current-thread runtime, with inline forwarders.
/// Under `sim`, inside a world, the world's runtime (see the module documentation).
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct WorkerRuntime {
    #[cfg(not(feature = "sim"))]
    runtime: Runtime,
    #[cfg(feature = "sim")]
    backend: sim::Backend,
}

impl WorkerRuntime {
    #[inline]
    fn thread(runtime: Runtime) -> Self {
        #[cfg(not(feature = "sim"))]
        {
            Self { runtime }
        }
        #[cfg(feature = "sim")]
        {
            Self {
                backend: sim::Backend::Thread(runtime),
            }
        }
    }

    /// [`Runtime::block_on`]. Under `sim`, inside a world, the future is polled inline
    /// and must not need virtual time or another task to complete.
    #[inline]
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        #[cfg(not(feature = "sim"))]
        {
            self.runtime.block_on(future)
        }
        #[cfg(feature = "sim")]
        {
            match &self.backend {
                sim::Backend::Thread(runtime) => runtime.block_on(future),
                sim::Backend::Sim(_) => nabedra_rt::task::block_on_inline(future),
            }
        }
    }

    /// The runtime's handle. Under `sim` it is the world runtime's handle, and tasks
    /// spawned through it outlive the worker until their node crashes.
    #[inline]
    pub fn handle(&self) -> &Handle {
        #[cfg(not(feature = "sim"))]
        {
            self.runtime.handle()
        }
        #[cfg(feature = "sim")]
        {
            match &self.backend {
                sim::Backend::Thread(runtime) => runtime.handle(),
                sim::Backend::Sim(worker) => &worker.handle,
            }
        }
    }

    /// Spawns `future` on this runtime. Like dropping the product worker's runtime,
    /// a simulated worker aborts these tasks when it exits.
    #[inline]
    #[track_caller]
    pub fn spawn<F>(&self, future: F) -> nabedra_rt::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        #[cfg(not(feature = "sim"))]
        {
            nabedra_rt::task::spawn_on(self.runtime.handle(), future)
        }
        #[cfg(feature = "sim")]
        {
            match &self.backend {
                sim::Backend::Thread(runtime) => {
                    nabedra_rt::task::spawn_on(runtime.handle(), future)
                }
                sim::Backend::Sim(worker) => worker.spawn(future),
            }
        }
    }
}

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
    thread: WorkerJoin,
    drained: oneshot::Receiver<()>,
    exit: oneshot::Sender<()>,
}

#[cfg(not(feature = "sim"))]
type WorkerJoin = JoinHandle<()>;
#[cfg(feature = "sim")]
use sim::WorkerJoin;

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
        #[cfg(not(feature = "sim"))]
        let threads = 0..count.get();
        // Inside a simulated world every worker is a task and no thread starts.
        #[cfg(feature = "sim")]
        let threads = if nabedra_rt::sim::world().is_some() {
            workers = sim::start(count, &coordinator_runtime, &stopped, &receiver);
            count.get()..count.get()
        } else {
            0..count.get()
        };
        for id in threads {
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
                        Ok(runtime) => WorkerRuntime::thread(runtime),
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
                    thread: thread.into(),
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
        F: FnOnce(WorkerId, &WorkerRuntime) -> Result<T, ProtocolError> + Send + 'static,
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
        F: FnOnce(WorkerId, &WorkerRuntime) -> Result<T, ProtocolError> + Send + 'static,
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
        F: FnOnce(WorkerId, &WorkerRuntime) -> Result<T, ProtocolError> + Send + 'static,
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
        F: FnOnce(WorkerId, &WorkerRuntime) -> Result<T, ProtocolError> + Send + 'static,
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
        F: FnOnce(WorkerId, &WorkerRuntime) -> Result<T, ProtocolError> + Send + 'static,
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
        #[cfg(feature = "sim")]
        if workers.iter().any(|worker| worker.thread.is_task()) {
            return sim::shutdown(&self.coordinator, workers);
        }
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

/// The simulated pool (feature `sim`, inside a `nabedra_rt::sim` world). See the module
/// documentation.
#[cfg(feature = "sim")]
pub mod sim {
    use super::{JoinHandle, Work, Worker, WorkerId, WorkerRuntime, internal};
    use crate::protocol::ProtocolError;
    use std::{
        num::NonZeroUsize,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Mutex, PoisonError},
        time::Duration,
    };
    use tokio::{
        runtime::{Handle, Runtime},
        sync::{mpsc, oneshot, watch},
        task::AbortHandle,
    };

    /// A product worker thread's default stack. A simulated job runs on the simulation's
    /// blocking helper, which ignores it.
    const WORKER_STACK_BYTES: usize = 2 << 20;

    /// The virtual time a simulated worker holds its slot before it runs each job, so
    /// overlap and refill are observable (C04, C06). Install one for a run with
    /// `World::insert_ext`; without one, jobs cost no virtual time.
    pub struct JobCost(Box<dyn FnMut(WorkerId) -> Duration + Send>);

    impl JobCost {
        /// Every job costs `cost`.
        pub fn fixed(cost: Duration) -> Self {
            Self(Box::new(move |_| cost))
        }

        /// Each job costs a time drawn uniformly from `[0, max]` by a SplitMix64 stream
        /// seeded with `seed`, separate from the node's id stream.
        pub fn seeded(seed: u64, max: Duration) -> Self {
            let mut rng = nabedra_rt::sim::SplitMix64::new(seed);
            let bound = u64::try_from(max.as_nanos())
                .unwrap_or(u64::MAX)
                .saturating_add(1);
            Self(Box::new(move |_| Duration::from_nanos(rng.below(bound))))
        }

        /// The cost of each job, in the order workers take them, from the taking worker.
        pub fn from_fn(cost: impl FnMut(WorkerId) -> Duration + Send + 'static) -> Self {
            Self(Box::new(cost))
        }
    }

    fn job_cost(id: WorkerId) -> Duration {
        nabedra_rt::sim::world()
            .and_then(|world| world.with_ext::<JobCost, _>(|cost| (cost.0)(id)))
            .unwrap_or_default()
    }

    fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) enum Backend {
        Thread(Runtime),
        Sim(SimWorker),
    }

    /// One simulated worker: the world runtime's handle and the tasks its jobs spawned.
    #[derive(Clone)]
    pub(super) struct SimWorker {
        pub(super) handle: Handle,
        spawned: Arc<Mutex<Vec<AbortHandle>>>,
    }

    impl SimWorker {
        #[track_caller]
        pub(super) fn spawn<F>(&self, future: F) -> nabedra_rt::task::JoinHandle<F::Output>
        where
            F: std::future::Future + Send + 'static,
            F::Output: Send + 'static,
        {
            let task = nabedra_rt::task::spawn_on(&self.handle, future);
            let mut spawned = lock(&self.spawned);
            spawned.retain(|task| !task.is_finished());
            spawned.push(task.abort_handle());
            task
        }
    }

    pub(super) enum WorkerJoin {
        Thread(JoinHandle<()>),
        Task(nabedra_rt::task::JoinHandle<()>),
    }

    impl From<JoinHandle<()>> for WorkerJoin {
        fn from(thread: JoinHandle<()>) -> Self {
            Self::Thread(thread)
        }
    }

    impl WorkerJoin {
        pub(super) fn is_task(&self) -> bool {
            matches!(self, Self::Task(_))
        }

        pub(super) fn join(self) -> std::thread::Result<()> {
            match self {
                Self::Thread(thread) => thread.join(),
                Self::Task(_) => unreachable!("a simulated worker joins in its async shutdown"),
            }
        }
    }

    /// Spawns one node task per worker on the coordinator runtime, which must be the
    /// world's runtime.
    pub(super) fn start(
        count: NonZeroUsize,
        coordinator: &Handle,
        stopped: &watch::Sender<bool>,
        receiver: &Arc<tokio::sync::Mutex<mpsc::Receiver<Work>>>,
    ) -> Vec<Worker> {
        (0..count.get())
            .map(|id| {
                let mut worker_stop = stopped.subscribe();
                let receiver = receiver.clone();
                let (drained_tx, drained) = oneshot::channel();
                let (exit, exit_rx) = oneshot::channel::<()>();
                let worker = SimWorker {
                    handle: coordinator.clone(),
                    spawned: Arc::default(),
                };
                let task = nabedra_rt::task::spawn_on(coordinator, async move {
                    loop {
                        let work = {
                            let mut receiver = receiver.lock().await;
                            tokio::select! {
                                biased;
                                _ = worker_stop.changed() => {receiver.close(); receiver.recv().await}
                                work = receiver.recv() => work,
                            }
                        };
                        let Some(work) = work else { break };
                        let cost = job_cost(id);
                        if !cost.is_zero() {
                            nabedra_rt::time::sleep(cost).await;
                        }
                        let runtime = WorkerRuntime {
                            backend: Backend::Sim(worker.clone()),
                        };
                        let (done, ran) = oneshot::channel::<()>();
                        // The job runs on the blocking helper after this task yields,
                        // named as the product thread is, under the same outer guard.
                        let started = nabedra_rt::task::spawn_oneshot(
                            format!("nabedra-worker-{id}"),
                            WORKER_STACK_BYTES,
                            move || {
                                let _ = catch_unwind(AssertUnwindSafe(|| work(id, &runtime)));
                                let _ = done.send(());
                            },
                        );
                        if started.is_ok() {
                            let _ = ran.await;
                        }
                    }
                    let _ = drained_tx.send(());
                    let _ = exit_rx.await;
                    // The product worker's runtime drops here, with its spawned tasks.
                    for task in lock(&worker.spawned).drain(..) {
                        task.abort();
                    }
                });
                Worker {
                    thread: WorkerJoin::Task(task),
                    drained,
                    exit,
                }
            })
            .collect()
    }

    /// The product shutdown, as a coordinator task: await every drained worker, open
    /// the exit gates, then await every worker task.
    pub(super) fn shutdown(
        coordinator: &Handle,
        mut workers: Vec<Worker>,
    ) -> tokio::task::JoinHandle<Result<(), ProtocolError>> {
        nabedra_rt::task::spawn_on(coordinator, async move {
            let mut failed = false;
            for worker in &mut workers {
                if (&mut worker.drained).await.is_err() {
                    failed = true;
                }
            }
            let mut tasks = Vec::new();
            for worker in workers {
                let _ = worker.exit.send(());
                tasks.push(worker.thread);
            }
            for task in tasks {
                match task {
                    WorkerJoin::Task(task) => failed |= task.await.is_err(),
                    WorkerJoin::Thread(_) => unreachable!("a simulated pool starts no thread"),
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

#[cfg(all(test, feature = "sim"))]
mod sim_tests;

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
