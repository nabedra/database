//! One pinned thread per core. A core's state is made on its thread and never leaves it, so
//! cores share no mutable state: they talk only by messages, which are tasks that run on the
//! receiving core with its state. Threads, CPU pins and waits go through `nabedra-rt`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;

use nabedra_rt::sync::Condvar;
use nabedra_rt::task;

use super::sched::{Class, Limits, Scheduler};
use crate::types::{Error, Result};

/// A message: work for the state of one core.
pub type Job<S> = Box<dyn FnOnce(&mut S, &Ctx<S>) + Send>;

struct Queue<S> {
    sched: Mutex<Scheduler<Job<S>>>,
    wake: Condvar,
}

struct Inner<S> {
    queues: Vec<Queue<S>>,
    stop: AtomicBool,
    /// Tasks that panicked. Each was contained; its core went on.
    panics: AtomicU64,
}

impl<S> Inner<S> {
    fn sched(&self, core: usize) -> Result<MutexGuard<'_, Scheduler<Job<S>>>> {
        let q = self.queues.get(core).ok_or(Error::Internal("no such core"))?;
        Ok(q.sched.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn submit(&self, core: usize, class: Class, cost: u64, job: Job<S>) -> Result<()> {
        let mut sched = self.sched(core)?;
        if self.stop.load(Ordering::Acquire) {
            return Err(Error::Overloaded);
        }
        sched.submit(class, cost, job)?;
        drop(sched);
        self.queues[core].wake.notify_one();
        Ok(())
    }
}

/// What a task sees besides its core's state.
pub struct Ctx<S> {
    core: usize,
    cpu: Option<usize>,
    inner: Arc<Inner<S>>,
}

impl<S> Ctx<S> {
    pub fn core(&self) -> usize {
        self.core
    }

    /// The CPU this core's thread is pinned to.
    pub fn cpu(&self) -> Option<usize> {
        self.cpu
    }

    pub fn cores(&self) -> usize {
        self.inner.queues.len()
    }

    /// Sends a message to a core, this one included. It may be refused like any submission.
    pub fn send(
        &self,
        to: usize,
        class: Class,
        cost: u64,
        job: impl FnOnce(&mut S, &Ctx<S>) + Send + 'static,
    ) -> Result<()> {
        self.inner.submit(to, class, cost, Box::new(job))
    }
}

/// The answer of [`Cores::call`]. A task that panicked or never ran answers with an error.
struct Reply<R> {
    value: Mutex<Option<Result<R>>>,
    done: Condvar,
}

struct Answer<R>(Arc<Reply<R>>);

impl<R> Answer<R> {
    fn set(&self, value: Result<R>) {
        let mut v = self.0.value.lock().unwrap_or_else(PoisonError::into_inner);
        if v.is_none() {
            *v = Some(value);
        }
        self.0.done.notify_all();
    }
}

impl<R> Drop for Answer<R> {
    fn drop(&mut self) {
        self.set(Err(Error::Internal("the task did not finish")));
    }
}

pub struct Cores<S: 'static> {
    inner: Arc<Inner<S>>,
    threads: Vec<JoinHandle<()>>,
    cpus: Vec<Option<usize>>,
}

impl<S: 'static> Cores<S> {
    /// Starts `n` cores. Core `i` is pinned to the `i`-th CPU this process may use (cycling when
    /// there are fewer), and makes its state with `make(i)` on its own thread. With `pin` false,
    /// or where pinning fails, it runs unpinned, which [`Cores::cpus`] shows.
    pub fn start(
        n: usize,
        limits: Limits,
        pin: bool,
        make: impl Fn(usize) -> S + Send + Sync + 'static,
    ) -> Result<Cores<S>> {
        if n == 0 {
            return Err(Error::Internal("a runtime needs a core"));
        }
        let queue = || Queue {
            sched: Mutex::new(Scheduler::new(limits)),
            wake: Condvar::new(),
        };
        let inner = Arc::new(Inner {
            queues: (0..n).map(|_| queue()).collect(),
            stop: AtomicBool::new(false),
            panics: AtomicU64::new(0),
        });
        let allowed = task::allowed_cpus().unwrap_or_default();
        let make = Arc::new(make);
        let (tell, told) = std::sync::mpsc::channel();
        let mut threads = Vec::with_capacity(n);
        for core in 0..n {
            let want = (pin && !allowed.is_empty()).then(|| allowed[core % allowed.len()]);
            let (shared, make, tell) = (inner.clone(), make.clone(), tell.clone());
            let run = move || {
                let cpu = want.filter(|c| task::pin_current_thread(*c).is_ok());
                let Ok(mut state) = catch_unwind(AssertUnwindSafe(|| make(core))) else {
                    let _ = tell.send(Err(Error::Io(format!("cannot initialize core {core}"))));
                    return;
                };
                let _ = tell.send(Ok((core, cpu)));
                drop(tell);
                let ctx = Ctx {
                    core,
                    cpu,
                    inner: shared,
                };
                run_core(&mut state, &ctx);
            };
            match task::spawn_service(format!("vector-core-{core}"), run) {
                Ok(t) => threads.push(t),
                Err(e) => {
                    let started = Cores {
                        inner,
                        threads,
                        cpus: Vec::new(),
                    };
                    started.shutdown();
                    return Err(Error::Io(format!("cannot start core {core}: {e}")));
                }
            }
        }
        drop(tell);
        let mut cores = Cores {
            inner,
            threads,
            cpus: vec![None; n],
        };
        for _ in 0..n {
            let (core, cpu) = told
                .recv()
                .map_err(|_| Error::Io("a core exited before initialization".into()))??;
            cores.cpus[core] = cpu;
        }
        Ok(cores)
    }

    pub fn len(&self) -> usize {
        self.inner.queues.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The CPU each core is pinned to.
    pub fn cpus(&self) -> &[Option<usize>] {
        &self.cpus
    }

    pub fn panics(&self) -> u64 {
        self.inner.panics.load(Ordering::Relaxed)
    }

    /// Queues work on a core, or refuses it before it starts.
    pub fn submit(
        &self,
        core: usize,
        class: Class,
        cost: u64,
        job: impl FnOnce(&mut S, &Ctx<S>) + Send + 'static,
    ) -> Result<()> {
        self.inner.submit(core, class, cost, Box::new(job))
    }

    /// Runs `f` on a core and waits for its answer.
    pub fn call<R: Send + 'static>(
        &self,
        core: usize,
        class: Class,
        cost: u64,
        f: impl FnOnce(&mut S, &Ctx<S>) -> R + Send + 'static,
    ) -> Result<R> {
        let reply = Arc::new(Reply {
            value: Mutex::new(None),
            done: Condvar::new(),
        });
        let answer = Answer(reply.clone());
        self.submit(core, class, cost, move |s, ctx| answer.set(Ok(f(s, ctx))))?;
        let value = reply.value.lock().unwrap_or_else(PoisonError::into_inner);
        let mut value = reply
            .done
            .wait_while(value, |v| v.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        value.take().unwrap_or(Err(Error::Internal("no answer")))
    }

    /// Tasks queued on a core.
    pub fn queued(&self, core: usize) -> usize {
        self.inner.sched(core).map_or(0, |s| s.len())
    }

    /// Refuses new work and drops queued tasks unrun; their callers get an error. Running tasks
    /// finish.
    pub fn stop(&self) {
        self.inner.stop.store(true, Ordering::Release);
        for q in &self.inner.queues {
            let dropped = q.sched.lock().unwrap_or_else(PoisonError::into_inner).drain();
            drop(dropped);
            q.wake.notify_all();
        }
    }

    /// [`Cores::stop`], then waits for every core's thread.
    pub fn shutdown(mut self) {
        self.stop_all();
    }

    fn stop_all(&mut self) {
        self.stop();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

impl<S: 'static> Drop for Cores<S> {
    fn drop(&mut self) {
        self.stop_all();
    }
}

fn run_core<S>(state: &mut S, ctx: &Ctx<S>) {
    let q = &ctx.inner.queues[ctx.core];
    loop {
        let guard = q.sched.lock().unwrap_or_else(PoisonError::into_inner);
        let stop = || ctx.inner.stop.load(Ordering::Acquire);
        let mut guard = q
            .wake
            .wait_while(guard, |s| s.is_empty() && !stop())
            .unwrap_or_else(PoisonError::into_inner);
        if stop() {
            return;
        }
        let Some((_, job)) = guard.pop() else {
            continue;
        };
        drop(guard);
        // A task that panics is contained: the core and the other tasks go on (scenario F02).
        if catch_unwind(AssertUnwindSafe(|| job(state, ctx))).is_err() {
            ctx.inner.panics.fetch_add(1, Ordering::Relaxed);
        }
    }
}
