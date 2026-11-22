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
//!   node down and aborts every registered task; a task spawned into a down node is
//!   aborted at once. [`World::restart`] marks it up again with an empty registry.
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
//!   work is not deterministic. Phase 9 reworks services into cooperative tasks on the
//!   simulation runtime; until then a simulation should keep services idle, as the
//!   Events simulator keeps nabedra-kv's flush and compaction threads idle.
//! - [`crate::time::thread_sleep`] does not sleep on the simulation thread or in blocking
//!   work (a real sleep would stall the cluster without advancing virtual time). On a
//!   service thread it sleeps in real time.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use uuid::Uuid;

/// Virtual wall-clock origin: 2026-01-01T00:00:00Z.
pub const EPOCH_MS: u64 = 1_767_225_600_000;

const PPM: i128 = 1_000_000;

tokio::task_local! {
    static NODE: u64;
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
}

#[derive(Debug)]
struct State {
    rng: SplitMix64,
    nodes: BTreeMap<u64, Node>,
}

/// The shared state of one simulation run.
#[derive(Debug)]
pub struct World {
    seed: u64,
    start: Instant,
    std_start: std::time::Instant,
    state: Mutex<State>,
}

impl World {
    /// A world seeded with `seed`, starting at the current virtual time.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime: the world's clock is the runtime's paused clock.
    pub fn new(seed: u64) -> Arc<Self> {
        assert!(
            tokio::runtime::Handle::try_current().is_ok(),
            "create the simulation world inside its runtime (sim::run)"
        );
        Arc::new(Self {
            seed,
            start: Instant::now(),
            std_start: std::time::Instant::now(),
            state: Mutex::new(State {
                rng: SplitMix64::new(seed),
                nodes: BTreeMap::new(),
            }),
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

    fn elapsed(&self) -> Duration {
        virtual_now().saturating_duration_since(self.start)
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
        let handle = tokio::spawn(NODE.scope(node, future));
        self.register(node, &handle);
        handle
    }

    fn register<T>(&self, node: u64, handle: &JoinHandle<T>) {
        let mut state = self.lock();
        match state.nodes.get_mut(&node) {
            Some(entry) => {
                entry.tasks.retain(|task| !task.is_finished());
                entry.tasks.push(handle.abort_handle());
                if !entry.up {
                    handle.abort();
                }
            }
            None => handle.abort(),
        }
    }

    /// Crashes `node`: marks it down and aborts every registered task. Service threads
    /// cannot be aborted; see the module documentation.
    pub fn crash(&self, node: u64) {
        let tasks = self.with_node(node, |entry| {
            entry.up = false;
            std::mem::take(&mut entry.tasks)
        });
        for task in tasks {
            task.abort();
        }
    }

    /// Marks a crashed `node` up again. Its clock and id stream continue.
    pub fn restart(&self, node: u64) {
        self.with_node(node, |entry| entry.up = true);
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
}

/// Installs `world` on this thread until the guard drops.
#[must_use = "the world is uninstalled when the guard drops"]
pub struct Installed {
    previous: Option<Arc<World>>,
}

impl Installed {
    pub fn new(world: Arc<World>) -> Self {
        let previous = WORLD.with(|slot| slot.borrow_mut().replace(world));
        Self { previous }
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
/// For `tokio::select!` branch order to follow the seed too, build with
/// `--cfg tokio_unstable` and set `Builder::rng_seed` (see `server/events/src/sim`).
pub fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
}

/// Runs one simulation: a paused current-thread runtime, a [`World`] seeded with `seed`
/// and installed on this thread, and `body` driven to completion.
pub fn run<F, Fut>(seed: u64, body: F) -> Fut::Output
where
    F: FnOnce(Arc<World>) -> Fut,
    Fut: Future,
{
    let runtime = runtime().expect("build the simulation runtime");
    let world = runtime.block_on(async { World::new(seed) });
    let _installed = Installed::new(world.clone());
    runtime.block_on(body(world))
}

/// The node the current task or thread belongs to, if any.
pub fn current_node() -> Option<u64> {
    NODE.try_with(|node| *node)
        .ok()
        .or_else(|| match ROLE.get() {
            Role::Blocking(node) | Role::Service(node) => node,
            Role::Main => None,
        })
}

/// Runs `future` as part of `node`, without spawning.
pub async fn in_node<F: Future>(node: u64, future: F) -> F::Output {
    NODE.scope(node, future).await
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
        let since_epoch = i128::from(EPOCH_MS) * 1_000_000 + local + offset;
        SystemTime::UNIX_EPOCH + nanos(since_epoch)
    })
    .unwrap_or_else(SystemTime::now)
}

/// A deadline on the current node's clock as a deadline on the runtime's virtual clock.
pub(crate) fn to_runtime_instant(deadline: Instant) -> Instant {
    node_clock(|world, clock| {
        let Some(clock) = clock else {
            return deadline;
        };
        let local = if deadline >= world.start {
            deadline.duration_since(world.start).as_nanos() as i128
        } else {
            -(world.start.duration_since(deadline).as_nanos() as i128)
        };
        world
            .start
            .checked_add(clock.virtual_at(local))
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

pub(crate) fn thread_sleep(duration: Duration) {
    let simulated = world().is_some() && !matches!(ROLE.get(), Role::Service(_));
    if !simulated {
        std::thread::sleep(duration);
    }
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

pub(crate) fn spawn_on<F>(handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match (current_node(), world()) {
        (Some(node), Some(world)) => {
            let task = handle.spawn(NODE.scope(node, future));
            world.register(node, &task);
            task
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

#[cfg(test)]
mod tests;
