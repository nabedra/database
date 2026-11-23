//! [`Background`]: the worker-shared flush and compaction threads (replaces the
//! previous engine's background thread pools). One `kv-flush` thread and `threads - 1` `kv-compact` threads
//! serve every store opened with the same `Background`. With one thread, the flush
//! thread also compacts.
//!
//! The threaded mode runs [`run`] on each named thread. With KV's `sim` feature,
//! the runtime drives each [`nabedra_rt::task::Stepped`] service one job per step
//! inside a simulated world, with timed idles in virtual time for retry delays.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

#[cfg(feature = "sim")]
use nabedra_rt::task::{self, Step, Stepped};
use nabedra_rt::time;

use crate::error::Result;
use crate::journal::service::{Condvar, ServiceHandle};

/// Stack size of every thread `nabedra-kv` starts (`kv-flush`, `kv-compact`,
/// `kv-journal`, `kv-journal-prep`). Set explicitly so `RUST_MIN_STACK` cannot change
/// it: the server's native thread budget reserves this much per thread (O05).
pub const THREAD_STACK_BYTES: usize = 2 << 20;

/// Under `sim`: how soon a service whose next job waits for a lock held further down
/// the simulation's stack looks again (see [`Job::would_block`]).
#[cfg(feature = "sim")]
const BLOCKED_POLL: Duration = Duration::from_millis(1);

/// Work a store hands to the pool. Each call returns a delay after which to run it again
/// (for example after `NoSpace`), or `None` when there is nothing left to do.
pub(crate) trait Job: Send + Sync {
    fn run_flush(&self) -> Option<Duration>;
    fn run_compaction(&self) -> Option<Duration>;

    /// Simulation only: whether the flush (or compaction) job would now wait for a lock
    /// that code further down the simulation's single stack holds, for example a caller's
    /// `Store::flush` that drives this service from `Commit::wait` while it holds the
    /// store's flush lock. A product thread would block on that lock until the caller
    /// releases it; a step must never wait, so the service idles briefly instead and the
    /// job stays queued where it was.
    #[cfg(feature = "sim")]
    fn would_block(&self, compaction: bool) -> bool {
        let _ = compaction;
        false
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Flush,
    Compaction,
}

struct Entry {
    ready_at: Instant,
    job: Weak<dyn Job>,
}

#[derive(Default)]
struct Queues {
    flush: VecDeque<Entry>,
    compaction: VecDeque<Entry>,
    shutdown: bool,
}

struct Shared {
    queues: Mutex<Queues>,
    flush_ready: Condvar,
    compaction_ready: Condvar,
    compaction_threads: usize,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

pub struct Background {
    shared: Arc<Shared>,
    threads: Mutex<Vec<ServiceHandle>>,
}

impl std::fmt::Debug for Background {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Background")
            .field("threads", &(self.shared.compaction_threads + 1))
            .finish()
    }
}

impl Background {
    /// Starts `threads` (at least 1) background threads, each with a
    /// [`THREAD_STACK_BYTES`] stack. A thread the OS refuses to start is an
    /// `Error::Io`; the threads already started are stopped and joined first.
    pub fn new(threads: usize) -> Result<Arc<Background>> {
        Self::with_stack(threads, THREAD_STACK_BYTES)
    }

    /// [`Background::new`] with another stack size, for tests of spawn failures.
    #[doc(hidden)]
    pub fn with_stack(threads: usize, stack_bytes: usize) -> Result<Arc<Background>> {
        let threads = threads.max(1);
        let shared = Arc::new(Shared {
            queues: Mutex::new(Queues::default()),
            flush_ready: Condvar::new(),
            compaction_ready: Condvar::new(),
            compaction_threads: threads - 1,
        });
        let background = Background {
            shared,
            threads: Mutex::new(Vec::with_capacity(threads)),
        };
        for n in 0..threads {
            let shared = background.shared.clone();
            let flush_thread = n == 0;
            let name = if flush_thread {
                "kv-flush"
            } else {
                "kv-compact"
            };
            // On failure `background` drops here, which stops and joins the threads
            // started so far.
            #[cfg(feature = "sim")]
            let handle = task::spawn_stepped(
                name,
                stack_bytes,
                Service {
                    shared,
                    flush_thread,
                },
            )?;
            #[cfg(not(feature = "sim"))]
            let handle = crate::journal::service::spawn_thread(name, stack_bytes, move || {
                run(&shared, flush_thread)
            })?;
            lock(&background.threads).push(handle);
        }
        Ok(Arc::new(background))
    }

    /// Number of threads.
    pub fn threads(&self) -> usize {
        self.shared.compaction_threads + 1
    }

    pub(crate) fn queued(&self) -> (usize, usize) {
        let queues = lock(&self.shared.queues);
        (queues.flush.len(), queues.compaction.len())
    }

    pub(crate) fn schedule_flush(&self, job: Weak<dyn Job>, delay: Duration) {
        self.schedule(Kind::Flush, job, delay);
    }

    pub(crate) fn schedule_compaction(&self, job: Weak<dyn Job>, delay: Duration) {
        self.schedule(Kind::Compaction, job, delay);
    }

    fn schedule(&self, kind: Kind, job: Weak<dyn Job>, delay: Duration) {
        let entry = Entry {
            ready_at: time::now_std() + delay,
            job,
        };
        let mut queues = lock(&self.shared.queues);
        if queues.shutdown {
            return;
        }
        match kind {
            Kind::Flush => queues.flush.push_back(entry),
            Kind::Compaction => queues.compaction.push_back(entry),
        }
        drop(queues);
        match kind {
            Kind::Flush => self.shared.flush_ready.notify_one(),
            Kind::Compaction if self.shared.compaction_threads == 0 => {
                self.shared.flush_ready.notify_one()
            }
            Kind::Compaction => self.shared.compaction_ready.notify_one(),
        }
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        lock(&self.shared.queues).shutdown = true;
        self.shared.flush_ready.notify_all();
        self.shared.compaction_ready.notify_all();
        for handle in lock(&self.threads).drain(..) {
            // The last reference can drop on one of the pool's own threads (a store
            // closing there); that thread exits on its own once it sees `shutdown`.
            // Under `sim` a join drives the service until it exits.
            if !handle.is_current() {
                let _ = handle.join();
            }
        }
    }
}

/// Takes the first entry whose time has come, or the time to wait for the next one.
fn take_ready(queue: &mut VecDeque<Entry>, now: Instant) -> Result<Entry, Option<Instant>> {
    match queue.iter().position(|e| e.ready_at <= now) {
        Some(i) => Ok(queue.remove(i).expect("index in range")),
        None => Err(queue.iter().map(|e| e.ready_at).min()),
    }
}

/// The thread body of one `kv-flush` or `kv-compact` thread.
fn run(shared: &Shared, flush_thread: bool) {
    let also_compact = flush_thread && shared.compaction_threads == 0;
    let cv = if flush_thread {
        &shared.flush_ready
    } else {
        &shared.compaction_ready
    };
    let mut queues = lock(&shared.queues);
    loop {
        if queues.shutdown {
            return;
        }
        let now = time::now_std();
        let (kind, entry) = match pick(&mut queues, now, flush_thread, also_compact) {
            Ok(picked) => picked,
            Err(next_wake) => {
                queues = match next_wake {
                    None => cv.wait(queues).unwrap_or_else(PoisonError::into_inner),
                    Some(at) => {
                        cv.wait_timeout(queues, at.saturating_duration_since(now))
                            .unwrap_or_else(PoisonError::into_inner)
                            .0
                    }
                };
                continue;
            }
        };
        drop(queues);
        queues = run_job(shared, kind, entry);
    }
}

/// Takes the job this thread runs next: a ready flush (on the flush thread), else a
/// ready compaction (on the compaction threads, or on the flush thread when it is the
/// only one). Otherwise returns when the next queued job becomes ready, if any is.
#[inline]
fn pick(
    queues: &mut Queues,
    now: Instant,
    flush_thread: bool,
    also_compact: bool,
) -> Result<(Kind, Entry), Option<Instant>> {
    let mut next_wake: Option<Instant> = None;
    let mut picked = None;
    if flush_thread {
        match take_ready(&mut queues.flush, now) {
            Ok(e) => picked = Some((Kind::Flush, e)),
            Err(at) => next_wake = at,
        }
    }
    if picked.is_none() && (!flush_thread || also_compact) {
        match take_ready(&mut queues.compaction, now) {
            Ok(e) => picked = Some((Kind::Compaction, e)),
            Err(at) => {
                next_wake = match (next_wake, at) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            }
        }
    }
    picked.ok_or(next_wake)
}

/// Runs one job without the queue lock, then queues its retry, if it asked for one, and
/// returns the queue lock.
#[inline]
fn run_job(shared: &Shared, kind: Kind, entry: Entry) -> MutexGuard<'_, Queues> {
    let retry = entry.job.upgrade().and_then(|job| match kind {
        Kind::Flush => job.run_flush(),
        Kind::Compaction => job.run_compaction(),
    });
    let mut queues = lock(&shared.queues);
    if let Some(delay) = retry
        && !queues.shutdown
    {
        let entry = Entry {
            ready_at: time::now_std() + delay,
            job: entry.job,
        };
        match kind {
            Kind::Flush => queues.flush.push_back(entry),
            Kind::Compaction => queues.compaction.push_back(entry),
        }
    }
    queues
}

/// Under `sim`: whether the job [`pick`] would take next waits for a lock held further
/// down the stack ([`Job::would_block`]). It looks without taking the job.
#[cfg(feature = "sim")]
fn next_would_block(queues: &Queues, now: Instant, flush_thread: bool, also_compact: bool) -> bool {
    fn first(queue: &VecDeque<Entry>, now: Instant) -> Option<&Entry> {
        queue.iter().find(|e| e.ready_at <= now)
    }
    let mut next = None;
    if flush_thread {
        next = first(&queues.flush, now).map(|e| (false, e));
    }
    if next.is_none() && (!flush_thread || also_compact) {
        next = first(&queues.compaction, now).map(|e| (true, e));
    }
    // The upgrade cannot drop the last reference to the store: the simulation runs one
    // step at a time, so the reference it found stays alive meanwhile.
    next.and_then(|(compaction, e)| e.job.upgrade().map(|job| (compaction, job)))
        .is_some_and(|(compaction, job)| job.would_block(compaction))
}

/// One `kv-flush` or `kv-compact` service.
#[cfg(feature = "sim")]
struct Service {
    shared: Arc<Shared>,
    flush_thread: bool,
}

#[cfg(feature = "sim")]
impl Stepped for Service {
    /// One job of [`run`], or `Idle` until the next queued job is ready (a retry delay
    /// becomes a timed idle in virtual time), or `Exit` at shutdown.
    fn step(&mut self) -> Step {
        let shared = &*self.shared;
        let also_compact = self.flush_thread && shared.compaction_threads == 0;
        let mut queues = lock(&shared.queues);
        if queues.shutdown {
            return Step::Exit;
        }
        let now = time::now_std();
        if next_would_block(&queues, now, self.flush_thread, also_compact) {
            return Step::Idle(Some(now + BLOCKED_POLL));
        }
        match pick(&mut queues, now, self.flush_thread, also_compact) {
            Ok((kind, entry)) => {
                drop(queues);
                drop(run_job(shared, kind, entry));
                Step::Worked
            }
            Err(next_wake) => Step::Idle(next_wake),
        }
    }

    fn doorbell(&self) -> &Condvar {
        if self.flush_thread {
            &self.shared.flush_ready
        } else {
            &self.shared.compaction_ready
        }
    }

    fn run(self) {
        run(&self.shared, self.flush_thread)
    }
}
