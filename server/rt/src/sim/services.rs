//! Stepped services in simulation: the registry, the driver tasks and inline drives.
//!
//! See the `Stepped services` section of the `sim` module documentation for the model.
//! Everything here runs on the simulation thread or on its blocking helper while that
//! thread waits, so steps never overlap. The step stack (which services are mid-step,
//! innermost last) is therefore one per world, not one per thread.

use std::any::Any;
use std::fmt::Write as _;
use std::future::Future;
use std::panic::{AssertUnwindSafe, Location};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::{
    ROLE, Role, ServiceInfo, ServiceState, SimEvent, SplitMix64, StepOutcome, World, current_node,
    lock, run_blocking, with_node_context, world,
};
use crate::task::{ServiceHandle, Step, Stepped};

/// Passes over a node's services that [`drive`] makes before it gives up on a `ready`
/// condition that never holds while services keep reporting progress or timed idles.
const MAX_PASSES: u64 = 1_000_000;

/// A doorbell: a generation counter and a tokio `Notify` the driver awaits. Ringing it
/// from any thread wakes the driver; the generation catches rings that happen while the
/// driver is not yet waiting (for example during the service's own step).
#[derive(Debug, Default)]
pub struct Bell {
    generation: AtomicU64,
    notify: tokio::sync::Notify,
}

impl Bell {
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub(crate) fn ring(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Returns once the generation differs from `seen`.
    async fn changed(&self, seen: u64) {
        loop {
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            if self.generation() != seen {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) type StepFn = Box<dyn FnMut() -> Step + Send>;

/// One stepped service.
pub(crate) struct Cell {
    name: String,
    node: Option<u64>,
    /// The service's doorbell (its `Stepped::doorbell`).
    bell: Arc<Bell>,
    /// The step function; `None` while it is mid-step and after the service finished.
    step: Mutex<Option<StepFn>>,
    status: Mutex<Status>,
}

enum Status {
    Ready,
    Idle(Option<std::time::Instant>),
    Stepping,
    Exited,
    Panicked(Option<Box<dyn Any + Send>>),
    Dead,
}

impl Status {
    fn state(&self) -> ServiceState {
        match self {
            Status::Ready => ServiceState::Ready,
            Status::Idle(at) => ServiceState::Idle(*at),
            Status::Stepping => ServiceState::Stepping,
            Status::Exited => ServiceState::Exited,
            Status::Panicked(_) => ServiceState::Panicked,
            Status::Dead => ServiceState::Dead,
        }
    }

    fn finished(&self) -> bool {
        matches!(self, Status::Exited | Status::Panicked(_) | Status::Dead)
    }
}

impl Cell {
    fn state(&self) -> ServiceState {
        lock(&self.status).state()
    }

    fn finished(&self) -> bool {
        lock(&self.status).finished()
    }
}

impl std::fmt::Debug for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cell")
            .field("name", &self.name)
            .field("node", &self.node)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// The world's stepped services, bells, step stack and jitter stream.
#[derive(Debug)]
pub(crate) struct Registry {
    cells: Mutex<Vec<Arc<Cell>>>,
    /// Rung by every `sync::Condvar` notification in the node (or outside any node).
    node_bells: Mutex<std::collections::BTreeMap<Option<u64>, Arc<Bell>>>,
    /// Services mid-step, innermost last.
    stack: Mutex<Vec<Arc<Cell>>>,
    jitter: Mutex<Jitter>,
    /// Steps run so far, for tests and reports.
    steps: AtomicU64,
}

#[derive(Debug)]
struct Jitter {
    rng: SplitMix64,
    per_mille: u64,
    max: Duration,
}

impl Registry {
    pub(crate) fn new(seed: u64) -> Registry {
        Registry {
            cells: Mutex::new(Vec::new()),
            node_bells: Mutex::new(std::collections::BTreeMap::new()),
            stack: Mutex::new(Vec::new()),
            jitter: Mutex::new(Jitter {
                // A stream of its own, so jitter draws never shift the nodes' id streams.
                rng: SplitMix64::new(seed ^ 0x5EED_5E41_CE5E_ED00),
                per_mille: 0,
                max: Duration::ZERO,
            }),
            steps: AtomicU64::new(0),
        }
    }

    fn node_bell(&self, node: Option<u64>) -> Arc<Bell> {
        Arc::clone(lock(&self.node_bells).entry(node).or_default())
    }

    /// The node's unfinished services, in registration order.
    fn live(&self, node: Option<u64>) -> Vec<Arc<Cell>> {
        lock(&self.cells)
            .iter()
            .filter(|cell| cell.node == node && !cell.finished())
            .cloned()
            .collect()
    }

    pub(crate) fn infos(&self, node: Option<u64>) -> Vec<ServiceInfo> {
        lock(&self.cells)
            .iter()
            .filter(|cell| cell.node == node)
            .map(|cell| ServiceInfo {
                name: cell.name.clone(),
                state: cell.state(),
            })
            .collect()
    }

    /// Marks every unfinished service of `node` dead and returns their step functions,
    /// for the caller to drop once no lock is held.
    pub(crate) fn kill_node(&self, node: u64) -> Vec<StepFn> {
        let cells: Vec<Arc<Cell>> = lock(&self.cells)
            .iter()
            .filter(|cell| cell.node == Some(node))
            .cloned()
            .collect();
        let mut dead = Vec::new();
        for cell in cells {
            {
                let mut status = lock(&cell.status);
                if status.finished() {
                    continue;
                }
                *status = Status::Dead;
            }
            // A service mid-step drops its own step function when the step returns.
            dead.extend(lock(&cell.step).take());
            cell.bell.ring();
        }
        self.node_bell(Some(node)).ring();
        dead
    }

    /// Drops finished services of `node` from the registry.
    pub(crate) fn forget_finished(&self, node: u64) {
        lock(&self.cells).retain(|cell| cell.node != Some(node) || !cell.finished());
    }

    fn describe(&self, node: Option<u64>) -> String {
        let mut text = String::new();
        for cell in lock(&self.cells).iter().filter(|cell| cell.node == node) {
            let _ = write!(text, "\n  {}: {:?}", cell.name, cell.state());
        }
        if text.is_empty() {
            text.push_str(" none");
        }
        text
    }
}

/// What one attempt to step a service did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Stepped(Step),
    /// The step panicked; the service is finished.
    Panicked,
    /// The service is mid-step further down the stack.
    Busy,
    /// The service has exited, panicked or died.
    Finished,
}

/// Runs one step of `cell` on this thread.
fn step_cell(world: &World, cell: &Arc<Cell>) -> Outcome {
    if lock(&cell.status).finished() {
        return Outcome::Finished;
    }
    let Some(mut step) = lock(&cell.step).take() else {
        return Outcome::Busy;
    };
    *lock(&cell.status) = Status::Stepping;
    lock(&world.services.stack).push(Arc::clone(cell));
    let result = std::panic::catch_unwind(AssertUnwindSafe(&mut step));
    lock(&world.services.stack).pop();
    world.services.steps.fetch_add(1, Ordering::SeqCst);
    let (outcome, keep) = {
        let mut status = lock(&cell.status);
        if matches!(*status, Status::Dead) {
            // The node crashed during the step: the service stays dead.
            (Outcome::Finished, false)
        } else {
            match result {
                Ok(Step::Exit) => {
                    *status = Status::Exited;
                    (Outcome::Stepped(Step::Exit), false)
                }
                Ok(outcome) => {
                    *status = match outcome {
                        Step::Idle(at) => Status::Idle(at),
                        _ => Status::Ready,
                    };
                    (Outcome::Stepped(outcome), true)
                }
                Err(panic) => {
                    *status = Status::Panicked(Some(panic));
                    (Outcome::Panicked, false)
                }
            }
        }
    };
    if keep {
        *lock(&cell.step) = Some(step);
    } else {
        // The service is finished: its state drops here, outside every lock.
        drop(step);
    }
    report(world, cell, outcome)
}

/// The world, when the caller may step services: inside a world, on the simulation
/// thread or its blocking helper. A helper-thread service (`spawn_service`) runs
/// concurrently with the simulation, so it keeps product behaviour.
fn stepping_world() -> Option<Arc<World>> {
    world().filter(|_| !matches!(ROLE.get(), Role::Service(_)))
}

fn report(world: &World, cell: &Cell, outcome: Outcome) -> Outcome {
    let reported = match outcome {
        Outcome::Stepped(Step::Worked) => StepOutcome::Worked,
        Outcome::Stepped(Step::Idle(at)) => {
            StepOutcome::Idle(at.map(|at| at.saturating_duration_since(world.std_start)))
        }
        Outcome::Stepped(Step::Exit) => StepOutcome::Exit,
        Outcome::Panicked => StepOutcome::Panicked,
        Outcome::Busy | Outcome::Finished => return outcome,
    };
    world.emit(&SimEvent::Step {
        node: cell.node,
        service: &cell.name,
        outcome: reported,
    });
    // An exit or a panic may be what a joiner or an idle peer waits for.
    if matches!(reported, StepOutcome::Exit | StepOutcome::Panicked) {
        world.services.node_bell(cell.node).ring();
    }
    outcome
}

/// The driver task of one service.
async fn driver(world: Weak<World>, cell: Arc<Cell>) {
    let node_bell = match world.upgrade() {
        Some(world) => world.services.node_bell(cell.node),
        None => return,
    };
    loop {
        let Some(strong) = world.upgrade() else {
            return;
        };
        if cell.finished() {
            return;
        }
        let strong = if let Some(pause) = jitter(&strong) {
            drop(strong);
            crate::time::sleep(pause).await;
            let Some(strong) = world.upgrade() else {
                return;
            };
            if cell.finished() {
                return;
            }
            strong
        } else {
            strong
        };
        let seen = (cell.bell.generation(), node_bell.generation());
        let job_cell = Arc::clone(&cell);
        let job_world = Arc::clone(&strong);
        drop(strong);
        let outcome =
            match run_blocking(job_world.clone(), move || step_cell(&job_world, &job_cell)) {
                Ok(outcome) => outcome,
                Err(panic) => std::panic::resume_unwind(panic),
            };
        match outcome {
            Outcome::Stepped(Step::Worked) | Outcome::Busy => tokio::task::yield_now().await,
            Outcome::Stepped(Step::Idle(None)) => cell.bell.changed(seen.0).await,
            Outcome::Stepped(Step::Idle(Some(at))) => {
                let wait = at.saturating_duration_since(crate::time::now_std());
                tokio::select! {
                    biased;
                    () = cell.bell.changed(seen.0) => {}
                    () = node_bell.changed(seen.1) => {}
                    () = crate::time::sleep(wait) => {}
                }
            }
            Outcome::Stepped(Step::Exit) | Outcome::Panicked | Outcome::Finished => return,
        }
    }
}

/// Draws whether the driver sleeps before its next step, and for how long.
fn jitter(world: &World) -> Option<Duration> {
    let mut jitter = lock(&world.services.jitter);
    if jitter.per_mille == 0 || jitter.max.is_zero() {
        return None;
    }
    if jitter.rng.below(1_000) >= jitter.per_mille {
        return None;
    }
    let max = u64::try_from(jitter.max.as_nanos()).unwrap_or(u64::MAX);
    Some(Duration::from_nanos(1 + jitter.rng.below(max)))
}

impl World {
    /// Before a step, with probability `per_mille / 1000`, a service's driver sleeps a
    /// seeded virtual time in `(0, max]`, so the interleaving of services with each other
    /// and with other tasks follows the seed (the NS17 analog). Off by default. Inline
    /// drives ([`crate::task::drive_until`]) are not delayed.
    pub fn set_jitter(&self, per_mille: u32, max: Duration) {
        let mut jitter = lock(&self.services.jitter);
        jitter.per_mille = u64::from(per_mille.min(1_000));
        jitter.max = max;
    }

    /// Stepped-service steps run so far in this world.
    pub fn service_steps(&self) -> u64 {
        self.services.steps.load(Ordering::SeqCst)
    }
}

/// The handle behind [`ServiceHandle`].
#[derive(Debug)]
pub(crate) enum Handle {
    /// Outside a world: the product thread.
    Thread(std::thread::JoinHandle<()>),
    Stepped {
        cell: Arc<Cell>,
        world: Weak<World>,
    },
}

impl Handle {
    pub(crate) fn join(self) -> std::thread::Result<()> {
        let (cell, world) = match self {
            Handle::Thread(thread) => return thread.join(),
            Handle::Stepped { cell, world } => (cell, world),
        };
        if let Some(world) = world.upgrade()
            && !cell.finished()
        {
            let on_stack = lock(&world.services.stack)
                .iter()
                .any(|active| Arc::ptr_eq(active, &cell));
            assert!(
                !on_stack,
                "reentrancy: joining service {} from inside its own step",
                cell.name
            );
            assert!(
                stepping_world().is_some_and(|current| Arc::ptr_eq(&current, &world)),
                "a live stepped service must join on its simulation thread or blocking helper"
            );
            with_node_context(cell.node.map(|node| world.node_context(node)), || {
                drive(&world, cell.node, "ServiceHandle::join", || {
                    cell.finished().then_some(())
                });
            });
        }
        match &mut *lock(&cell.status) {
            Status::Panicked(panic) => Err(panic
                .take()
                .unwrap_or_else(|| Box::new("the service panicked (already joined)"))),
            _ => Ok(()),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        match self {
            Handle::Thread(thread) => thread.is_finished(),
            Handle::Stepped { cell, .. } => cell.finished(),
        }
    }

    pub(crate) fn is_current(&self) -> bool {
        match self {
            Handle::Thread(thread) => thread.thread().id() == std::thread::current().id(),
            Handle::Stepped { cell, .. } => stepping_world().is_some_and(|world| {
                lock(&world.services.stack)
                    .last()
                    .is_some_and(|top| Arc::ptr_eq(top, cell))
            }),
        }
    }
}

pub(crate) fn spawn_stepped<S: Stepped>(
    name: String,
    stack_bytes: usize,
    service: S,
) -> std::io::Result<ServiceHandle> {
    let Some(world) = stepping_world() else {
        return std::thread::Builder::new()
            .name(name)
            .stack_size(stack_bytes)
            .spawn(move || service.run())
            .map(|thread| ServiceHandle {
                inner: Handle::Thread(thread),
            });
    };
    let node = current_node();
    let bell = Arc::clone(service.doorbell().bell());
    let mut service = service;
    let cell = Arc::new(Cell {
        name,
        node,
        bell,
        step: Mutex::new(Some(Box::new(move || service.step()))),
        status: Mutex::new(Status::Ready),
    });
    lock(&world.services.cells).push(Arc::clone(&cell));
    let task = driver(Arc::downgrade(&world), Arc::clone(&cell));
    match node {
        Some(node) => {
            if !world.is_up(node) {
                // Started on a crashed node: dead before its first step.
                let dead = world.services.kill_node(node);
                drop(dead);
            }
            drop(world.spawn_node(node, task));
        }
        None => drop(tokio::runtime::Handle::current().spawn(task)),
    }
    Ok(ServiceHandle {
        inner: Handle::Stepped {
            cell,
            world: Arc::downgrade(&world),
        },
    })
}

thread_local! {
    /// The name of the one-shot job running on this thread, for `current_service`.
    static ONESHOT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn current_service() -> Option<String> {
    if let Some(world) = stepping_world() {
        if let Some(top) = lock(&world.services.stack).last() {
            return Some(top.name.clone());
        }
        if let Some(name) = ONESHOT.with(|name| name.borrow().clone()) {
            return Some(name);
        }
    }
    std::thread::current().name().map(str::to_owned)
}

pub(crate) fn spawn_oneshot<F>(name: String, stack_bytes: usize, f: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    let Some(world) = stepping_world() else {
        return std::thread::Builder::new()
            .name(name)
            .stack_size(stack_bytes)
            .spawn(f)
            .map(drop);
    };
    let job_world = Arc::clone(&world);
    let job = async move {
        let outcome = run_blocking(job_world, move || {
            ONESHOT.with(|slot| *slot.borrow_mut() = Some(name));
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(f));
            ONESHOT.with(|slot| *slot.borrow_mut() = None);
            if let Err(panic) = outcome {
                std::panic::resume_unwind(panic);
            }
        });
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    };
    match current_node() {
        Some(node) => drop(world.spawn_node(node, job)),
        None => drop(tokio::runtime::Handle::current().spawn(job)),
    }
    Ok(())
}

/// One pass over live services in registration order. A bell rung during the pass
/// may have made an earlier service ready again, even if every step reported idle.
fn pass(world: &World, node: Option<u64>, mut after_step: impl FnMut() -> bool) -> bool {
    let bell = world.services.node_bell(node);
    let seen = bell.generation();
    let mut progress = false;
    for cell in world.services.live(node) {
        match step_cell(world, &cell) {
            Outcome::Stepped(Step::Worked | Step::Exit) | Outcome::Panicked => progress = true,
            Outcome::Stepped(Step::Idle(_)) => {}
            Outcome::Busy | Outcome::Finished => continue,
        }
        // The async driver may be parked on the wake condition from its last step.
        cell.bell.ring();
        if after_step() {
            break;
        }
    }
    progress || bell.generation() != seen
}

/// The earliest timed idle among the node's live services that are not mid-step.
fn earliest_deadline(world: &World, node: Option<u64>) -> Option<std::time::Instant> {
    world
        .services
        .live(node)
        .iter()
        .filter_map(|cell| match *lock(&cell.status) {
            Status::Idle(Some(at)) => Some(at),
            _ => None,
        })
        .min()
}

/// Steps `node`'s services until `ready` returns `Some`; see [`crate::task::drive_until`].
fn drive<T>(
    world: &World,
    node: Option<u64>,
    site: &'static str,
    mut ready: impl FnMut() -> Option<T>,
) -> T {
    let mut passes = 0u64;
    loop {
        if let Some(value) = ready() {
            return value;
        }
        let mut found = None;
        let progress = pass(world, node, || {
            found = ready();
            found.is_some()
        });
        if let Some(value) = found {
            return value;
        }
        passes += 1;
        assert!(
            passes < MAX_PASSES,
            "drive_until({site}): no result after {MAX_PASSES} passes; services:{}",
            world.services.describe(node)
        );
        if progress {
            continue;
        }
        match earliest_deadline(world, node) {
            Some(at) => {
                let wait = at.saturating_duration_since(crate::time::now_std());
                // A deadline already due would loop forever without moving time.
                let wait = wait.max(Duration::from_micros(1));
                world.pause_all(super::to_runtime_duration(wait));
            }
            None => panic!(
                "drive_until({site}): nothing can progress on node {node:?}; services:{}",
                world.services.describe(node)
            ),
        }
    }
}

pub(crate) fn drive_until<T>(site: &'static str, ready: impl FnMut() -> Option<T>) -> Option<T> {
    let world = stepping_world()?;
    Some(drive(&world, current_node(), site, ready))
}

/// Steps the node's services until a full pass does no work, without moving time.
pub(crate) fn settle(world: &World, node: Option<u64>) {
    for _ in 0..MAX_PASSES {
        if !pass(world, node, || false) {
            return;
        }
    }
    panic!(
        "a thread_sleep could not settle node {node:?}'s services; services:{}",
        world.services.describe(node)
    );
}

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

pub(crate) fn block_on_inline<F: Future>(
    future: F,
    caller: &'static Location<'static>,
) -> F::Output {
    let mut future = pin!(future);
    let Some(world) = stepping_world() else {
        // Outside a world: an ordinary thread-parking block_on.
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            std::thread::park();
        }
    };
    let node = current_node();
    let flag = Arc::new(Flag(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    loop {
        if flag.0.swap(false, Ordering::SeqCst)
            && let Poll::Ready(value) = future.as_mut().poll(&mut cx)
        {
            return value;
        }
        // Give services a turn even if the future woke itself during the poll.
        let was_woken = flag.0.load(Ordering::SeqCst);
        let progress = pass(&world, node, || !was_woken && flag.0.load(Ordering::SeqCst));
        if !progress && !flag.0.load(Ordering::SeqCst) {
            panic!(
                "block_on_inline at {caller}: the future needs virtual time or another task \
                 to make progress, which cannot happen while it blocks the simulation; \
                 services:{}",
                world.services.describe(node)
            );
        }
    }
}

/// A `sync::Condvar` wait. Inside a world, on the simulation thread or its blocking
/// helper, nothing else can run while it waits, so it panics instead of hanging.
pub(crate) fn before_wait(site: &'static str) {
    if stepping_world().is_some() {
        panic!(
            "{site} on the simulation thread or its blocking helper would wait forever: \
             nothing else runs meanwhile. Wait with nabedra_rt::task::drive_until instead."
        );
    }
}

/// A `sync::Condvar` notification: rings the condvar's doorbell and its node's bell.
pub(crate) fn ring(bell: &Arc<Bell>) {
    bell.ring();
    if let Some(world) = world() {
        world.services.node_bell(current_node()).ring();
    }
}

#[cfg(test)]
mod tests;
