//! [`Background`]: the worker-shared flush and compaction threads (replaces the
//! previous engine's background thread pools). One `kv-flush` thread and `threads - 1` `kv-compact` threads
//! serve every store opened with the same `Background`. With one thread, the flush
//! thread also compacts.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::Result;

/// Stack size of every thread `nabedra-kv` starts (`kv-flush`, `kv-compact`,
/// `kv-journal`, `kv-journal-prep`). Set explicitly so `RUST_MIN_STACK` cannot change
/// it: the server's native thread budget reserves this much per thread (O05).
pub const THREAD_STACK_BYTES: usize = 2 << 20;

/// Work a store hands to the pool. Each call returns a delay after which to run it again
/// (for example after `NoSpace`), or `None` when there is nothing left to do.
pub(crate) trait Job: Send + Sync {
    fn run_flush(&self) -> Option<Duration>;
    fn run_compaction(&self) -> Option<Duration>;
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
    threads: Mutex<Vec<JoinHandle<()>>>,
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
            let handle = std::thread::Builder::new()
                .name(name.into())
                .stack_size(stack_bytes)
                .spawn(move || run(&shared, flush_thread))?;
            lock(&background.threads).push(handle);
        }
        Ok(Arc::new(background))
    }

    /// Number of threads.
    pub fn threads(&self) -> usize {
        self.shared.compaction_threads + 1
    }

    pub(crate) fn schedule_flush(&self, job: Weak<dyn Job>, delay: Duration) {
        self.schedule(Kind::Flush, job, delay);
    }

    pub(crate) fn schedule_compaction(&self, job: Weak<dyn Job>, delay: Duration) {
        self.schedule(Kind::Compaction, job, delay);
    }

    fn schedule(&self, kind: Kind, job: Weak<dyn Job>, delay: Duration) {
        let entry = Entry {
            ready_at: Instant::now() + delay,
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
        let me = std::thread::current().id();
        for handle in lock(&self.threads).drain(..) {
            // The last reference can drop on one of the pool's own threads (a store
            // closing there); that thread exits on its own once it sees `shutdown`.
            if handle.thread().id() != me {
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
        let now = Instant::now();
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
        let Some((kind, entry)) = picked else {
            queues = match next_wake {
                None => cv.wait(queues).unwrap_or_else(PoisonError::into_inner),
                Some(at) => {
                    cv.wait_timeout(queues, at.saturating_duration_since(now))
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
            continue;
        };
        drop(queues);
        let retry = entry.job.upgrade().and_then(|job| match kind {
            Kind::Flush => job.run_flush(),
            Kind::Compaction => job.run_compaction(),
        });
        queues = lock(&shared.queues);
        if let Some(delay) = retry
            && !queues.shutdown
        {
            let entry = Entry {
                ready_at: Instant::now() + delay,
                job: entry.job,
            };
            match kind {
                Kind::Flush => queues.flush.push_back(entry),
                Kind::Compaction => queues.compaction.push_back(entry),
            }
        }
    }
}
