//! Deterministic simulation of the runtime seam (feature `sim`). Never in a product build.
//!
//! A simulation runs a whole cluster on one current-thread tokio runtime with paused time
//! ([`runtime`], [`run`]). Shared state lives in one [`World`], installed in a
//! thread-local for the duration of a run. Each task carries the id of the node it belongs
//! to in a tokio task-local, which selects that node's clock, seeded id stream and task
//! registry. [`crate::task::spawn`] copies the node into every task it spawns, so a node's
//! descendants stay on that node.
//!
//! Outside an installed world every function behaves as in a product build, so code
//! compiled with `sim` (for example through feature unification) but not run inside a
//! simulation keeps its normal behaviour.
//!
//! # Clocks
//!
//! Let `E` be the virtual time elapsed on the paused runtime since the world started. Each
//! node has a local monotonic elapsed time `L(E)`, piecewise linear in `E`: it starts at
//! the node's monotonic offset and advances at `1 + drift_ppm / 1_000_000` times the
//! virtual rate. Changing the drift re-anchors the line at the current `E`, so `L` stays
//! continuous and non-decreasing. [`World::advance_mono`] moves `L` forward only.
//!
//! - [`crate::time::now`] is `world start + L(E)`; it never goes backward.
//! - [`crate::time::now_std`] is a std instant captured at world creation plus `L(E)`.
//! - [`crate::time::wall_now`] is `2026-01-01T00:00:00Z + L(E) + wall offset`. The wall
//!   offset is signed and moves with [`World::jump_wall_forward`] and
//!   [`World::jump_wall_backward`] (scenario O09), so the wall clock may jump in either
//!   direction while [`crate::time::now`] does not move.
//!
//! Deadlines and durations given to the node clock are mapped back to the runtime's
//! virtual clock, rounded up so a timer never fires before the node clock reaches its
//! deadline. A timer armed before a later drift change keeps its original mapping.
//!
//! # Ids
//!
//! Each node has a SplitMix64 stream seeded from the world seed and the node id; the world
//! has its own stream for draws outside any node. [`crate::ids::uuid_v4`] takes 128 bits
//! from the stream and sets the version 4 and variant bits.
//!
//! # Tasks, blocking work and services
//!
//! - Every task spawned in a node joins the node's registry. [`World::crash`] marks the
//!   node down, marks its stepped services dead, then aborts every registered task; a
//!   task spawned into a down node is aborted at once. [`World::restart`] marks it up
//!   again with an empty registry. [`World::freeze`] stops polling a node's tasks
//!   without aborting them, until [`World::thaw`].
//! - [`crate::task::spawn_blocking`] runs the closure to completion before it returns,
//!   while the simulation thread waits, so no OS scheduling decision reaches the
//!   schedule and virtual time stands still meanwhile. The closure runs on one helper
//!   thread per simulation thread, not on the simulation thread itself, because blocking
//!   code (nabedra-kv's journal handoff) calls tokio's `blocking_recv`, which tokio
//!   refuses on a runtime thread. The helper has the runtime handle entered and the
//!   node context installed. The returned handle resolves at once, with a panic as a
//!   `JoinError`.
//! - [`crate::task::spawn_service`] starts a helper OS thread with the requested name
//!   (and stack size), the runtime handle entered and the node context installed, and
//!   records it in the node's registry. A crash cannot abort an OS thread: it marks the
//!   node down, which ends the service only when its channels close. Service threads run
//!   concurrently with the simulation thread in real time, so a run whose services do
//!   work is not deterministic. Loops that must be deterministic use
//!   [`crate::task::spawn_stepped`] instead (next section).
//! - [`crate::time::thread_sleep`] on the simulation thread or in blocking work lets the
//!   node's stepped services settle, then pauses the whole world for the duration
//!   ([`World::pause_all`]). On a service thread it sleeps in real time.
//!
//! # Stepped services
//!
//! [`crate::task::spawn_stepped`] inside a world registers the service in the world's
//! registry (`services.rs`), in registration order, and spawns a node task, its driver.
//! The driver runs one [`crate::task::Stepped::step`] at a time on the blocking helper,
//! while the simulation thread waits, so a step is atomic: nothing else runs during it.
//! After `Worked` the driver yields; after `Idle(None)` it waits for the service's
//! doorbell (a notification on its `sync::Condvar`); after `Idle(Some(at))` it also wakes
//! at `at` or on any doorbell of its node. Code that waits for a service calls
//! [`crate::task::drive_until`], which steps the node's services inline, skipping any
//! that is mid-step, and pauses the world to the earliest timed idle when nothing else
//! can progress. When nothing can progress at all it panics with every service's state,
//! so a wait cycle fails deterministically instead of hanging. [`World::set_jitter`]
//! makes drivers sleep a seeded virtual time before some steps, which varies the
//! interleaving with the seed (the NS17 analog).
//!
//! A crash marks the node's services dead before it aborts any task, so destructors that
//! run during the abort never wait for a live service.
//!
//! # Observing a run
//!
//! [`World::observe`] reports every service step, crash, restart and pause, in order, so
//! a harness can hash the run's trace and compare two runs of one seed.

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use tokio::runtime::{Handle, Id, RuntimeFlavor};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio::time::Instant;
use uuid::Uuid;

/// Virtual wall-clock origin: 2026-01-01T00:00:00Z.
pub const EPOCH_MS: u64 = 1_767_225_600_000;

const PPM: i128 = 1_000_000;

tokio::task_local! {
    static NODE: Option<u64>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// The simulation thread, or a thread outside any simulation.
    Main,
    /// The blocking helper while it runs a job for this node.
    Blocking(Option<u64>),
    /// A service thread started for this node.
    Service(Option<u64>),
}

thread_local! {
    static WORLD: RefCell<Option<Arc<World>>> = const { RefCell::new(None) };
    static ROLE: Cell<Role> = const { Cell::new(Role::Main) };
    /// The blocking helper of this simulation thread. It stops when the thread exits.
    static HELPER: RefCell<Option<std::sync::mpsc::Sender<Job>>> = const { RefCell::new(None) };
}

type Job = Box<dyn FnOnce() + Send>;

/// SplitMix64 (Steele, Lea and Flood), copied from the Events simulator: no extra crate.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; `n > 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }
}

/// The seed of `node`'s id stream in a world seeded with `seed`.
fn node_seed(seed: u64, node: u64) -> u64 {
    SplitMix64::new(seed ^ node.wrapping_mul(0xD1B5_4A32_D192_ED03)).next_u64()
}

/// A node's clock settings when it joins the world.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeClock {
    /// Added to the node's monotonic clock from the start.
    pub mono_offset: Duration,
    /// Rate error of the node's clock in parts per million; above -1,000,000.
    pub drift_ppm: i64,
    /// Signed offset of the node's wall clock from the virtual wall clock, in nanoseconds.
    pub wall_offset_nanos: i128,
}

#[derive(Debug)]
struct Clock {
    /// Virtual elapsed time at the last re-anchor.
    anchor_virtual: Duration,
    /// The node's local monotonic elapsed time at the last re-anchor.
    anchor_local: Duration,
    drift_ppm: i64,
    wall_offset_nanos: i128,
}

impl Clock {
    fn new(settings: NodeClock) -> Self {
        assert_drift(settings.drift_ppm);
        Self {
            anchor_virtual: Duration::ZERO,
            anchor_local: settings.mono_offset,
            drift_ppm: settings.drift_ppm,
            wall_offset_nanos: settings.wall_offset_nanos,
        }
    }

    fn rate(&self) -> i128 {
        PPM + i128::from(self.drift_ppm)
    }

    /// `L(E)`. `E` before the anchor (only possible for a frozen blocking job) reads the
    /// anchor, so the clock never goes backward.
    fn local(&self, virtual_elapsed: Duration) -> Duration {
        let since = virtual_elapsed
            .saturating_sub(self.anchor_virtual)
            .as_nanos() as i128;
        let local = since * self.rate() / PPM;
        self.anchor_local.saturating_add(nanos(local))
    }

    /// The earliest `E` with `L(E) >= local`, at or after the anchor.
    fn virtual_at(&self, local: i128) -> Duration {
        let ahead = local - self.anchor_local.as_nanos() as i128;
        if ahead <= 0 {
            return self.anchor_virtual;
        }
        self.anchor_virtual
            .saturating_add(nanos(div_ceil(ahead * PPM, self.rate())))
    }

    fn reanchor(&mut self, virtual_elapsed: Duration) {
        self.anchor_local = self.local(virtual_elapsed);
        self.anchor_virtual = self.anchor_virtual.max(virtual_elapsed);
    }
}

fn assert_drift(drift_ppm: i64) {
    assert!(
        drift_ppm > -1_000_000,
        "a drift of {drift_ppm} ppm stops or reverses the clock"
    );
}

fn div_ceil(numerator: i128, denominator: i128) -> i128 {
    (numerator + denominator - 1) / denominator
}

/// Nonnegative nanoseconds as a duration, saturating at [`Duration::MAX`].
fn nanos(value: i128) -> Duration {
    if value <= 0 {
        return Duration::ZERO;
    }
    let secs = value / 1_000_000_000;
    match u64::try_from(secs) {
        Ok(secs) => Duration::new(secs, (value % 1_000_000_000) as u32),
        Err(_) => Duration::MAX,
    }
}

#[derive(Debug)]
struct Node {
    clock: Clock,
    rng: SplitMix64,
    up: bool,
    tasks: Vec<AbortHandle>,
    services: Vec<String>,
    frozen: bool,
    /// Tasks that found the node frozen, woken by [`World::thaw`].
    parked: Vec<Waker>,
}

#[derive(Debug)]
struct State {
    rng: SplitMix64,
    nodes: BTreeMap<u64, Node>,
}

type Observer = Arc<dyn Fn(&SimEvent<'_>) + Send + Sync>;

/// The run's observers and type-keyed extension slots.
#[derive(Default)]
struct Hooks {
    observers: Mutex<Vec<Observer>>,
    ext: Mutex<BTreeMap<TypeId, Box<dyn Any + Send>>>,
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks").finish_non_exhaustive()
    }
}

/// The shared state of one simulation run.
#[derive(Debug)]
pub struct World {
    me: Weak<World>,
    seed: u64,
    runtime_id: Id,
    start: Instant,
    std_start: std::time::Instant,
    state: Mutex<State>,
    /// Stop-the-world pauses so far, in nanoseconds: virtual time the runtime clock never
    /// ran ([`World::pause_all`]).
    paused_nanos: AtomicU64,
    services: services::Registry,
    volumes: volumes::Table,
    hooks: Hooks,
}

impl World {
    /// A world seeded with `seed`, starting at the current virtual time.
    ///
    /// # Panics
    ///
    /// Outside a current-thread tokio runtime. Use [`runtime`] for its paused clock.
    pub fn new(seed: u64) -> Arc<Self> {
        let runtime = Handle::try_current()
            .expect("create the simulation world inside its runtime (sim::run)");
        assert_eq!(
            runtime.runtime_flavor(),
            RuntimeFlavor::CurrentThread,
            "a simulated world requires a current-thread runtime"
        );
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            seed,
            runtime_id: runtime.id(),
            start: Instant::now(),
            std_start: std::time::Instant::now(),
            state: Mutex::new(State {
                rng: SplitMix64::new(seed),
                nodes: BTreeMap::new(),
            }),
            paused_nanos: AtomicU64::new(0),
            services: services::Registry::new(seed),
            volumes: volumes::Table::default(),
            hooks: Hooks::default(),
        })
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The runtime instant the world started at; `E` is measured from it.
    pub fn start(&self) -> Instant {
        self.start
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Virtual time since the world started, the same on every node. It includes
    /// stop-the-world pauses ([`World::pause_all`]), which the runtime clock never ran.
    pub fn elapsed(&self) -> Duration {
        virtual_now().saturating_duration_since(self.start) + self.paused()
    }

    /// Stop-the-world pauses so far.
    pub fn paused(&self) -> Duration {
        Duration::from_nanos(self.paused_nanos.load(Ordering::SeqCst))
    }

    fn with_node<T>(&self, node: u64, f: impl FnOnce(&mut Node) -> T) -> T {
        let mut state = self.lock();
        let entry = state
            .nodes
            .get_mut(&node)
            .unwrap_or_else(|| panic!("node {node} is not in the simulation"));
        f(entry)
    }

    /// Adds `node`, up, with its own clock and id stream.
    ///
    /// # Panics
    ///
    /// If the node exists, or the drift is at or below -1,000,000 ppm.
    pub fn add_node(&self, node: u64, clock: NodeClock) {
        let entry = Node {
            clock: Clock::new(clock),
            rng: SplitMix64::new(node_seed(self.seed, node)),
            up: true,
            tasks: Vec::new(),
            services: Vec::new(),
            frozen: false,
            parked: Vec::new(),
        };
        let previous = self.lock().nodes.insert(node, entry);
        assert!(previous.is_none(), "node {node} added twice");
    }

    /// Changes `node`'s drift from now on. The clock stays continuous.
    pub fn set_drift_ppm(&self, node: u64, drift_ppm: i64) {
        assert_drift(drift_ppm);
        let elapsed = self.elapsed();
        self.with_node(node, |entry| {
            entry.clock.reanchor(elapsed);
            entry.clock.drift_ppm = drift_ppm;
        });
    }

    /// Moves `node`'s monotonic (and with it, its wall) clock forward, as after a stall.
    pub fn advance_mono(&self, node: u64, by: Duration) {
        let elapsed = self.elapsed();
        self.with_node(node, |entry| {
            entry.clock.reanchor(elapsed);
            entry.clock.anchor_local = entry.clock.anchor_local.saturating_add(by);
        });
    }

    /// Steps `node`'s wall clock forward by `by` (O09). The monotonic clock does not move.
    pub fn jump_wall_forward(&self, node: u64, by: Duration) {
        self.with_node(node, |entry| {
            entry.clock.wall_offset_nanos += by.as_nanos() as i128;
        });
    }

    /// Steps `node`'s wall clock backward by `by` (O09). The monotonic clock does not move.
    pub fn jump_wall_backward(&self, node: u64, by: Duration) {
        self.with_node(node, |entry| {
            entry.clock.wall_offset_nanos -= by.as_nanos() as i128;
        });
    }

    /// Spawns `future` as a task of `node`, registered so a crash aborts it.
    pub fn spawn_node<F>(&self, node: u64, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_node_on(&Handle::current(), node, future)
    }

    fn assert_runtime(&self, target: &Handle) {
        assert_eq!(
            target.id(),
            self.runtime_id,
            "a simulated world cannot spawn onto another runtime"
        );
    }

    fn spawn_node_on<F>(&self, target: &Handle, node: u64, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.assert_runtime(target);
        let task = NODE.scope(Some(node), self.gate(node, future));
        let handle = target.spawn(task);
        self.register(node, handle.abort_handle());
        handle
    }

    fn gate<F>(&self, node: u64, future: F) -> Gate<F> {
        Gate {
            world: self.me.clone(),
            node,
            future: Box::pin(future),
        }
    }

    fn register(&self, node: u64, handle: AbortHandle) {
        let mut state = self.lock();
        match state.nodes.get_mut(&node) {
            Some(entry) => {
                entry.tasks.retain(|task| !task.is_finished());
                if !entry.up {
                    handle.abort();
                }
                entry.tasks.push(handle);
            }
            None => handle.abort(),
        }
    }

    /// Crashes `node`, in this order: halts every volume of the node (the disk cut: no
    /// later call changes an image), marks it down, marks its stepped services dead (a
    /// dead service is never stepped again and joins at once), then aborts every
    /// registered task. Destructors that run during the crash or the abort therefore find
    /// halted volumes and no live service to wait for. Helper-thread services
    /// (`spawn_service`) cannot be stopped; see the module documentation.
    pub fn crash(&self, node: u64) {
        self.volumes.halt_node(node);
        net::crash(self, node);
        let tasks = self.with_node(node, |entry| {
            entry.up = false;
            entry.frozen = false;
            std::mem::take(&mut entry.tasks)
        });
        let parked = self.with_node(node, |entry| std::mem::take(&mut entry.parked));
        let dead = self.services.kill_node(node);
        self.emit(&SimEvent::Crash { node });
        // Service destructors resolve node-local resources against the crashed node.
        with_node_context(Some(node), || drop(dead));
        for task in tasks {
            task.abort();
        }
        for waker in parked {
            waker.wake();
        }
    }

    /// Marks a crashed `node` up again. Its clock and id stream continue, and its finished
    /// stepped services leave the registry.
    pub fn restart(&self, node: u64) {
        self.with_node(node, |entry| entry.up = true);
        self.services.forget_finished(node);
        self.emit(&SimEvent::Restart { node });
    }

    pub fn is_up(&self, node: u64) -> bool {
        self.lock().nodes.get(&node).is_some_and(|entry| entry.up)
    }

    /// Registered tasks of `node` that have not finished.
    pub fn live_tasks(&self, node: u64) -> usize {
        self.with_node(node, |entry| {
            entry.tasks.retain(|task| !task.is_finished());
            entry.tasks.len()
        })
    }

    /// Names of the service threads started for `node`.
    pub fn services(&self, node: u64) -> Vec<String> {
        self.with_node(node, |entry| entry.services.clone())
    }

    /// The stepped services registered for `node`, in registration order, with their
    /// state. Finished services leave the list at [`World::restart`].
    pub fn stepped_services(&self, node: u64) -> Vec<ServiceInfo> {
        self.services.infos(Some(node))
    }

    /// Stops polling `node`'s tasks, and tasks it spawns meanwhile, until [`World::thaw`],
    /// without aborting them, as SIGSTOP would. The node's clocks keep running, and its
    /// stepped services stop with their driver tasks. Code outside the node that drives
    /// its services inline ([`crate::task::drive_until`]) is not stopped.
    pub fn freeze(&self, node: u64) {
        self.with_node(node, |entry| entry.frozen = true);
    }

    /// Resumes a frozen `node`'s tasks.
    pub fn thaw(&self, node: u64) {
        let parked = self.with_node(node, |entry| {
            entry.frozen = false;
            std::mem::take(&mut entry.parked)
        });
        for waker in parked {
            waker.wake();
        }
    }

    pub fn is_frozen(&self, node: u64) -> bool {
        self.lock()
            .nodes
            .get(&node)
            .is_some_and(|entry| entry.frozen)
    }

    /// Parks the current task's waker if `node` is frozen.
    fn park_if_frozen(&self, node: u64, waker: &Waker) -> bool {
        let mut state = self.lock();
        match state.nodes.get_mut(&node) {
            Some(entry) if entry.frozen => {
                if !entry.parked.iter().any(|parked| parked.will_wake(waker)) {
                    entry.parked.push(waker.clone());
                }
                true
            }
            _ => false,
        }
    }

    /// Stores `value` in the run's type-keyed slot for `T`, replacing any earlier one.
    /// Product probe hooks reach it through [`world`].
    pub fn insert_ext<T: Any + Send>(&self, value: T) {
        let previous = lock(&self.hooks.ext).insert(TypeId::of::<T>(), Box::new(value));
        drop(previous);
    }

    /// Runs `f` on the run's `T` slot and returns its result, or `None` when no `T` was
    /// inserted. The slot is taken out while `f` runs, without holding a lock, so `f` may
    /// use other slots and the rest of the world; a nested `with_ext::<T>` from inside
    /// `f` sees `None`.
    pub fn with_ext<T: Any, R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        let key = TypeId::of::<T>();
        let mut slot = lock(&self.hooks.ext).remove(&key)?;
        let value = slot
            .downcast_mut::<T>()
            .expect("extension slots are keyed by their stored type");
        let result = catch_unwind(AssertUnwindSafe(|| f(value)));
        if let std::collections::btree_map::Entry::Vacant(entry) = lock(&self.hooks.ext).entry(key)
        {
            entry.insert(slot);
        }
        match result {
            Ok(value) => Some(value),
            Err(panic) => resume_unwind(panic),
        }
    }

    /// A stop-the-world pause: every node's clocks move forward by `by` at once, and
    /// nothing runs meanwhile. The runtime clock does not move, so timers armed before
    /// the pause fire late by `by`; deadlines set after it are mapped through it.
    pub fn pause_all(&self, by: Duration) {
        if by.is_zero() {
            return;
        }
        let nanos = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        self.paused_nanos.fetch_add(nanos, Ordering::SeqCst);
        self.emit(&SimEvent::Pause { by });
    }

    /// Calls `observer` for every later simulation event, in order, so a harness can hash
    /// the run's trace. Observers run outside the world's locks, on the simulation thread
    /// or its blocking helper, and must not block.
    pub fn observe(&self, observer: Arc<dyn Fn(&SimEvent<'_>) + Send + Sync>) {
        lock(&self.hooks.observers).push(observer);
    }

    fn emit(&self, event: &SimEvent<'_>) {
        let observers = lock(&self.hooks.observers).clone();
        for observer in observers {
            observer(event);
        }
    }
}

/// A stepped service as [`World::stepped_services`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceInfo {
    pub name: String,
    pub state: ServiceState,
}

/// Where a stepped service stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceState {
    /// Started, or did work in its last step.
    Ready,
    /// Idle until its doorbell rings, or until a time on its node's `now_std` clock.
    Idle(Option<std::time::Instant>),
    /// Mid-step.
    Stepping,
    /// Its step returned `Exit`.
    Exited,
    /// A step panicked; joining it returns the panic.
    Panicked,
    /// Its node crashed.
    Dead,
}

/// A step's outcome as [`SimEvent::Step`] reports it. A deadline is node-clock time since
/// the world started, so two runs of one seed report the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepOutcome {
    Worked,
    Idle(Option<Duration>),
    Exit,
    Panicked,
}

/// One simulation event, as [`World::observe`] reports it.
#[derive(Debug)]
#[non_exhaustive]
pub enum SimEvent<'a> {
    /// A stepped service ran one step.
    Step {
        node: Option<u64>,
        service: &'a str,
        outcome: StepOutcome,
    },
    Crash {
        node: u64,
    },
    Restart {
        node: u64,
    },
    /// A stop-the-world pause ([`World::pause_all`], a timed idle that
    /// [`crate::task::drive_until`] skipped, or a `thread_sleep` in simulation).
    Pause {
        by: Duration,
    },
    /// A call on a node's simulated volume, as its `FaultFs` logged it.
    Fs {
        node: u64,
        mount: &'a std::path::Path,
        call: &'a crate::fs::FsCall,
    },
    /// [`World::restart_with`] replaced the node's volumes by their `cut` images.
    Cut {
        node: u64,
        cut: Cut,
    },
    /// [`World::wipe`] replaced a volume by an empty one.
    Wipe {
        node: u64,
        mount: &'a std::path::Path,
        how: Wipe,
    },
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A node task's future: not polled while its node is frozen.
struct Gate<F> {
    world: Weak<World>,
    node: u64,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for Gate<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        if let Some(world) = self.world.upgrade()
            && world.park_if_frozen(self.node, cx.waker())
        {
            return Poll::Pending;
        }
        self.future.as_mut().poll(cx)
    }
}

/// Installs `world` on this thread until the guard drops.
#[must_use = "the world is uninstalled when the guard drops"]
pub struct Installed {
    previous: Option<Arc<World>>,
    // Restoring a thread-local must happen on the thread that installed it.
    thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Installed {
    pub fn new(world: Arc<World>) -> Self {
        let previous = WORLD.with(|slot| slot.borrow_mut().replace(world));
        Self {
            previous,
            thread: std::marker::PhantomData,
        }
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        let previous = self.previous.take();
        WORLD.with(|slot| *slot.borrow_mut() = previous);
    }
}

/// The world installed on this thread.
pub fn world() -> Option<Arc<World>> {
    WORLD.with(|slot| slot.borrow().clone())
}

/// A current-thread tokio runtime with paused time, for one simulation.
///
/// For `tokio::select!` branch order to follow the seed too, use [`runtime_seeded`].
pub fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
}

/// True when [`runtime_seeded`] seeds tokio's own random choices, that is when the build
/// has `--cfg tokio_unstable`. A seed sweep asserts it before it runs.
pub const TOKIO_RNG_SEEDED: bool = cfg!(tokio_unstable);

/// [`runtime`] with tokio's own random choices, such as the branch `tokio::select!`
/// polls first, seeded from `seed` through `Builder::rng_seed`.
///
/// `Builder::rng_seed` exists only with `--cfg tokio_unstable`. Without the flag this
/// builds the same runtime as [`runtime`], and [`TOKIO_RNG_SEEDED`] is false.
///
/// Simulation builds pass the flag through the environment, with their own target
/// directory:
///
/// ```text
/// RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=target/sim \
///     cargo test --locked -p nabedra-rt --features sim,sim-openraft
/// ```
///
/// The flag is never set in a manifest, a `.cargo/config.toml` or a product build step,
/// so product builds never see it, and the separate target directory keeps the two flag
/// sets from rebuilding each other's artifacts.
pub fn runtime_seeded(seed: u64) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_time().start_paused(true);
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    #[cfg(not(tokio_unstable))]
    let _ = seed;
    builder.build()
}

/// Runs one simulation: a paused current-thread runtime ([`runtime_seeded`]), a
/// [`World`] seeded with `seed` and installed on this thread, and `body` driven to
/// completion.
pub fn run<F, Fut>(seed: u64, body: F) -> Fut::Output
where
    F: FnOnce(Arc<World>) -> Fut,
    Fut: Future,
{
    let runtime = runtime_seeded(seed).expect("build the simulation runtime");
    let world = runtime.block_on(async { World::new(seed) });
    let _installed = Installed::new(world.clone());
    runtime.block_on(async { body(world).await })
}

/// The node the current task or thread belongs to, if any.
pub fn current_node() -> Option<u64> {
    NODE.try_with(|node| *node)
        .unwrap_or_else(|_| match ROLE.get() {
            Role::Blocking(node) | Role::Service(node) => node,
            Role::Main => None,
        })
}

/// Runs `future` as part of `node`, without spawning.
pub async fn in_node<F: Future>(node: u64, future: F) -> F::Output {
    NODE.scope(Some(node), future).await
}

/// Runs synchronous code in a service's node context, including on the helper.
fn with_node_context<T>(node: Option<u64>, f: impl FnOnce() -> T) -> T {
    NODE.sync_scope(node, f)
}

/// The paused runtime clock. On the blocking helper it is frozen, because the
/// simulation thread waits for the job.
fn virtual_now() -> Instant {
    Instant::now()
}

fn node_clock<T>(f: impl FnOnce(&World, Option<&Clock>) -> T) -> Option<T> {
    let world = world()?;
    let node = current_node();
    let state = world.lock();
    let clock = node
        .and_then(|node| state.nodes.get(&node))
        .map(|entry| &entry.clock);
    Some(f(&world, clock))
}

/// The local monotonic elapsed time of the current node, or the virtual elapsed time
/// outside a node.
fn local_elapsed(world: &World, clock: Option<&Clock>) -> Duration {
    let elapsed = world.elapsed();
    clock.map_or(elapsed, |clock| clock.local(elapsed))
}

pub(crate) fn mono_now() -> Instant {
    node_clock(|world, clock| {
        let local = local_elapsed(world, clock);
        world.start.checked_add(local).unwrap_or_else(Instant::now)
    })
    .unwrap_or_else(Instant::now)
}

pub(crate) fn mono_now_std() -> std::time::Instant {
    node_clock(|world, clock| {
        let local = local_elapsed(world, clock);
        world.std_start + local
    })
    .unwrap_or_else(std::time::Instant::now)
}

pub(crate) fn wall_now() -> SystemTime {
    node_clock(|world, clock| {
        let local = local_elapsed(world, clock).as_nanos() as i128;
        let offset = clock.map_or(0, |clock| clock.wall_offset_nanos);
        let since_epoch = (i128::from(EPOCH_MS) * 1_000_000 + local).saturating_add(offset);
        if since_epoch < 0 {
            SystemTime::UNIX_EPOCH.checked_sub(nanos(since_epoch.saturating_neg()))
        } else {
            SystemTime::UNIX_EPOCH.checked_add(nanos(since_epoch))
        }
        .expect("the simulated wall clock must fit in SystemTime")
    })
    .unwrap_or_else(SystemTime::now)
}

/// A deadline on the current node's clock as a deadline on the runtime's virtual clock.
pub(crate) fn to_runtime_instant(deadline: Instant) -> Instant {
    node_clock(|world, clock| {
        let paused = world.paused();
        let Some(clock) = clock else {
            // The world clock is the runtime clock plus the pauses.
            return deadline
                .checked_sub(paused)
                .unwrap_or(world.start)
                .max(world.start.min(deadline));
        };
        let local = if deadline >= world.start {
            deadline.duration_since(world.start).as_nanos() as i128
        } else {
            -(world.start.duration_since(deadline).as_nanos() as i128)
        };
        world
            .start
            .checked_add(clock.virtual_at(local).saturating_sub(paused))
            .unwrap_or(deadline)
    })
    .unwrap_or(deadline)
}

/// A duration on the current node's clock as a duration on the runtime's virtual clock.
pub(crate) fn to_runtime_duration(duration: Duration) -> Duration {
    node_clock(|_, clock| match clock {
        Some(clock) if clock.drift_ppm != 0 => {
            nanos(div_ceil(duration.as_nanos() as i128 * PPM, clock.rate()))
        }
        _ => duration,
    })
    .unwrap_or(duration)
}

/// Under a world, on the simulation thread or its blocking helper, a sleep first lets
/// the node's stepped services run what they can, then pauses the whole world for the
/// duration ([`World::pause_all`]): a stop-the-world pause, so a deadline loop in
/// synchronous code (nabedra-kv's journal GC wait) sees time pass. Service threads
/// sleep in real time.
pub(crate) fn thread_sleep(duration: Duration) {
    let world = match world() {
        Some(world) if !matches!(ROLE.get(), Role::Service(_)) => world,
        _ => return std::thread::sleep(duration),
    };
    services::settle(&world, current_node());
    world.pause_all(to_runtime_duration(duration));
}

/// Draws from the current node's stream, or the world's stream outside a node.
fn draw_u64(world: &World) -> u64 {
    let node = current_node();
    let mut state = world.lock();
    let state = &mut *state;
    match node.and_then(|node| state.nodes.get_mut(&node)) {
        Some(entry) => entry.rng.next_u64(),
        None => state.rng.next_u64(),
    }
}

pub(crate) fn random_u64() -> u64 {
    match world() {
        Some(world) => draw_u64(&world),
        None => crate::ids::product_random_u64(),
    }
}

pub(crate) fn uuid_v4() -> Uuid {
    let Some(world) = world() else {
        return Uuid::new_v4();
    };
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&draw_u64(&world).to_le_bytes());
    bytes[8..].copy_from_slice(&draw_u64(&world).to_le_bytes());
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}

pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match (current_node(), world()) {
        (Some(node), Some(world)) => world.spawn_node(node, future),
        _ => tokio::spawn(future),
    }
}

pub(crate) fn spawn_on<F>(handle: &Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match (current_node(), world()) {
        (Some(node), Some(world)) => world.spawn_node_on(handle, node, future),
        (None, Some(world)) => {
            world.assert_runtime(handle);
            handle.spawn(future)
        }
        _ => handle.spawn(future),
    }
}

pub(crate) fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let Some(world) = world() else {
        return tokio::task::spawn_blocking(f);
    };
    let outcome = run_blocking(world, f);
    spawn(async move {
        match outcome {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}

pub(crate) fn spawn_blocking_on<F, R>(handle: &Handle, f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let Some(world) = world() else {
        return handle.spawn_blocking(f);
    };
    world.assert_runtime(handle);
    let outcome = run_blocking(world, f);
    spawn_on(handle, async move {
        match outcome {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}

pub(crate) fn spawn_in<T, F>(set: &mut JoinSet<T>, future: F) -> AbortHandle
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match (current_node(), world()) {
        (Some(node), Some(world)) => {
            world.assert_runtime(&Handle::current());
            let task = set.spawn(NODE.scope(Some(node), world.gate(node, future)));
            world.register(node, task.clone());
            task
        }
        _ => set.spawn(future),
    }
}

/// Runs `work` on this thread's blocking helper and waits for it. Only one job runs at a
/// time and the simulation thread does nothing meanwhile, so the outcome is deterministic.
fn run_blocking<R: Send + 'static>(
    world: Arc<World>,
    work: impl FnOnce() -> R + Send + 'static,
) -> std::thread::Result<R> {
    let node = current_node();
    let handle = tokio::runtime::Handle::try_current().ok();
    let (reply, result) = std::sync::mpsc::sync_channel(1);
    let job: Job = Box::new(move || {
        let _entered = handle.as_ref().map(tokio::runtime::Handle::enter);
        let _installed = Installed::new(world);
        ROLE.set(Role::Blocking(node));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        ROLE.set(Role::Main);
        let _ = reply.send(outcome);
    });
    HELPER.with(|helper| {
        helper
            .borrow_mut()
            .get_or_insert_with(start_helper)
            .send(job)
            .expect("the simulation blocking helper is running")
    });
    result
        .recv()
        .expect("the simulation blocking helper replies")
}

fn start_helper() -> std::sync::mpsc::Sender<Job> {
    let (jobs, queue) = std::sync::mpsc::channel::<Job>();
    std::thread::Builder::new()
        .name("sim-blocking".into())
        .spawn(move || {
            for job in queue {
                job();
            }
        })
        .expect("start the simulation blocking helper");
    jobs
}

pub(crate) fn spawn_service<F, T>(name: String, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_service_with_stack(name, None, f)
}

pub(crate) fn spawn_service_with_stack<F, T>(
    name: String,
    stack_bytes: Option<usize>,
    f: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let mut builder = std::thread::Builder::new().name(name.clone());
    if let Some(stack_bytes) = stack_bytes {
        builder = builder.stack_size(stack_bytes);
    }
    let Some(world) = world() else {
        return builder.spawn(f);
    };
    let node = current_node();
    if let Some(node) = node {
        world.with_node(node, |entry| entry.services.push(name));
    }
    let handle = tokio::runtime::Handle::try_current().ok();
    builder.spawn(move || {
        let _entered = handle.as_ref().map(tokio::runtime::Handle::enter);
        let _installed = Installed::new(world);
        ROLE.set(Role::Service(node));
        f()
    })
}

pub mod net;
#[cfg(feature = "sim-openraft")]
pub mod openraft10;
pub(crate) mod services;
pub mod volumes;

pub use volumes::{Cut, VolumeInfo, Wipe, volume_for};

#[cfg(test)]
mod tests;
