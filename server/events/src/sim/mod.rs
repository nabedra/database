//! Deterministic simulation of an Events cluster. See `README.md` in this directory.
//!
//! A whole cluster runs on one current-thread tokio runtime with paused time. Each task
//! carries the id of the node it belongs to (a task-local), which selects that node's
//! clock offset, seeded random stream and task registry. All shared state lives in one
//! [`World`] installed in a thread-local for the duration of a run.

pub mod cluster;
mod history;
pub mod net;
pub mod runtime;
#[cfg(test)]
mod tests;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use uuid::Uuid;

use crate::raft::Broker;

pub use cluster::{Knobs, Report, SimConfig, run};

tokio::task_local! {
    static NODE: u64;
}

thread_local! {
    static WORLD: RefCell<Option<Arc<World>>> = const { RefCell::new(None) };
    /// On a blocking helper thread: the node and the virtual time of the waiting task.
    static HELPER: Cell<Option<(Option<u64>, Instant)>> = const { Cell::new(None) };
}

/// Virtual now: the paused runtime clock, or the instant a blocking helper started at
/// (the simulation thread waits for the helper, so virtual time stands still meanwhile).
fn virtual_now() -> Instant {
    match HELPER.with(Cell::get) {
        Some((_, now)) => now,
        None => Instant::now(),
    }
}

/// Runs blocking storage work on a helper thread while the simulation thread waits.
/// nabedra-kv blocks on its journal thread with `blocking_recv`, which tokio refuses on a
/// runtime thread. Only one helper runs at a time, so the outcome is deterministic.
pub(crate) fn run_blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let world = world();
    let context = (current_node(), virtual_now());
    let (reply, result) = std::sync::mpsc::sync_channel(1);
    let job: Job = Box::new(move || {
        WORLD.with(|slot| *slot.borrow_mut() = world);
        HELPER.with(|slot| slot.set(Some(context)));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        WORLD.with(|slot| *slot.borrow_mut() = None);
        HELPER.with(|slot| slot.set(None));
        let _ = reply.send(outcome);
    });
    POOL.with(|pool| {
        pool.borrow_mut()
            .get_or_insert_with(|| {
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
            })
            .send(job)
            .expect("the simulation blocking helper is running")
    });
    match result.recv().expect("the simulation blocking helper replies") {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

type Job = Box<dyn FnOnce() + Send>;

thread_local! {
    /// The blocking helper of this simulation thread. It stops when the thread exits.
    static POOL: RefCell<Option<std::sync::mpsc::Sender<Job>>> = const { RefCell::new(None) };
}

/// Virtual wall-clock origin: 2026-01-01T00:00:00Z.
const EPOCH_MS: u64 = 1_767_225_600_000;
const TRACE_TAIL: usize = 64;

/// SplitMix64 (Steele, Lea and Flood). A private copy: the library has no dependency on
/// `nabedra-check`.
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

    /// Uniform in `[lo, hi]`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    pub fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

/// FNV-1a, 64 bit: a stable hash for the trace (no per-process keys).
#[derive(Clone, Copy, Debug)]
pub struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    pub fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}

pub(crate) fn fnv(bytes: &[u8]) -> u64 {
    let mut hash = Fnv::default();
    hash.write(bytes);
    hash.finish()
}

pub(crate) struct NodeState {
    pub rng: SplitMix64,
    pub wall_offset_ms: i64,
    pub mono_offset: Duration,
    pub up: bool,
    pub paused: bool,
    pub tasks: Vec<AbortHandle>,
    pub broker: Option<Arc<Broker>>,
}

#[derive(Default)]
pub(crate) struct NetState {
    /// Directed links that drop every message.
    pub blocked: BTreeSet<(u64, u64)>,
    pub drop_per_mille: u64,
    pub dup_per_mille: u64,
    pub delay_max_ms: u64,
    /// Bumped on every connectivity change (partition, heal, crash, restart, pause).
    pub epoch: u64,
}

pub(crate) struct State {
    pub knobs: Knobs,
    pub rng: SplitMix64,
    pub net_rng: SplitMix64,
    pub nodes: BTreeMap<u64, NodeState>,
    pub net: NetState,
    pub trace: Fnv,
    pub trace_events: u64,
    pub trace_tail: VecDeque<String>,
    pub observer: cluster::Observer,
    pub history: history::History,
    pub messages: u64,
}

/// The simulated universe of one run.
pub(crate) struct World {
    state: Mutex<State>,
    start: Instant,
}

impl World {
    pub fn new(seed: u64, knobs: Knobs, nodes: u64) -> Self {
        let mut root = SplitMix64::new(seed);
        let nodes = (1..=nodes)
            .map(|id| {
                (
                    id,
                    NodeState {
                        rng: SplitMix64::new(root.next_u64() ^ id),
                        wall_offset_ms: 0,
                        mono_offset: Duration::ZERO,
                        up: false,
                        paused: false,
                        tasks: Vec::new(),
                        broker: None,
                    },
                )
            })
            .collect();
        Self {
            state: Mutex::new(State {
                knobs,
                rng: SplitMix64::new(root.next_u64()),
                net_rng: SplitMix64::new(root.next_u64()),
                nodes,
                net: NetState::default(),
                trace: Fnv::default(),
                trace_events: 0,
                trace_tail: VecDeque::new(),
                observer: cluster::Observer::default(),
                history: history::History::default(),
                messages: 0,
            }),
            start: Instant::now(),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn elapsed_ms(&self) -> u64 {
        virtual_now().duration_since(self.start).as_millis() as u64
    }

    /// Adds one line to the trace hash. Every run with the same seed must add the same
    /// lines in the same order.
    pub fn trace(&self, line: fmt::Arguments<'_>) {
        let line = format!("{} {line}", self.elapsed_ms());
        let mut state = self.lock();
        state.trace.write(line.as_bytes());
        state.trace.write(b"\n");
        state.trace_events += 1;
        if state.trace_tail.len() == TRACE_TAIL {
            state.trace_tail.pop_front();
        }
        state.trace_tail.push_back(line);
    }

    pub fn spawn_on<F>(&self, node: u64, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn(NODE.scope(node, future));
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
        handle
    }

    pub fn broker(&self, node: u64) -> Option<Arc<Broker>> {
        let state = self.lock();
        let entry = state.nodes.get(&node)?;
        entry.up.then(|| entry.broker.clone()).flatten()
    }

    pub fn is_up(&self, node: u64) -> bool {
        self.lock().nodes.get(&node).is_some_and(|entry| entry.up)
    }

    pub fn is_paused(&self, node: u64) -> bool {
        self.lock()
            .nodes
            .get(&node)
            .is_some_and(|entry| entry.paused)
    }

    pub async fn wait_unpaused(&self, node: u64) {
        while self.is_paused(node) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn knobs(&self) -> Knobs {
        self.lock().knobs
    }
}

/// Installs `world` on this thread until the guard drops.
pub(crate) struct Installed;

impl Installed {
    pub fn new(world: Arc<World>) -> Self {
        WORLD.with(|slot| *slot.borrow_mut() = Some(world));
        Self
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        WORLD.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(crate) fn world() -> Option<Arc<World>> {
    WORLD.with(|slot| slot.borrow().clone())
}

/// The node the current task belongs to, if any.
pub fn current_node() -> Option<u64> {
    NODE.try_with(|node| *node)
        .ok()
        .or_else(|| HELPER.with(Cell::get).and_then(|(node, _)| node))
}

/// Runs `future` as part of `node`.
pub(crate) async fn in_node<F: Future>(node: u64, future: F) -> F::Output {
    NODE.scope(node, future).await
}

pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match (current_node(), world()) {
        (Some(node), Some(world)) => world.spawn_on(node, future),
        _ => tokio::spawn(future),
    }
}

fn node_offsets() -> (Duration, i64) {
    match (current_node(), world()) {
        (Some(node), Some(world)) => world
            .lock()
            .nodes
            .get(&node)
            .map_or((Duration::ZERO, 0), |entry| {
                (entry.mono_offset, entry.wall_offset_ms)
            }),
        _ => (Duration::ZERO, 0),
    }
}

pub(crate) fn mono_now() -> Instant {
    virtual_now() + node_offsets().0
}

pub(crate) fn to_tokio_instant(deadline: Instant) -> Instant {
    let offset = node_offsets().0;
    deadline.checked_sub(offset).unwrap_or(deadline)
}

pub(crate) fn wall_now() -> SystemTime {
    let Some(world) = world() else {
        return SystemTime::now();
    };
    let offset = node_offsets().1;
    let ms = (EPOCH_MS + world.elapsed_ms()).saturating_add_signed(offset);
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms)
}

/// Milliseconds on `node`'s wall clock (what that broker's HTTP layer would stamp).
pub(crate) fn wall_ms_of(world: &World, node: u64) -> i64 {
    let offset = world
        .lock()
        .nodes
        .get(&node)
        .map_or(0, |entry| entry.wall_offset_ms);
    (EPOCH_MS + world.elapsed_ms()) as i64 + offset
}

/// Draws from the current node's stream, or the world's shared stream outside a node.
pub(crate) fn draw_u64() -> u64 {
    let Some(world) = world() else {
        return FALLBACK.with(|rng| rng.borrow_mut().next_u64());
    };
    let node = current_node();
    let mut state = world.lock();
    match node.and_then(|node| state.nodes.get_mut(&node)) {
        Some(entry) => entry.rng.next_u64(),
        None => state.rng.next_u64(),
    }
}

thread_local! {
    static FALLBACK: RefCell<SplitMix64> = RefCell::new(SplitMix64::new(0x5eed));
}

pub(crate) fn uuid_v4() -> Uuid {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&draw_u64().to_le_bytes());
    bytes[8..].copy_from_slice(&draw_u64().to_le_bytes());
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}

/// Entry points the product code calls under `cfg(feature = "sim")`.
pub(crate) mod hooks {
    use openraft::LogId;
    use serde::Serialize;

    use super::{current_node, fnv, world};

    pub fn payload_hash(payload: &impl Serialize) -> u64 {
        fnv(&serde_json::to_vec(payload).unwrap_or_default())
    }

    /// A replica applied `log_id` with this payload.
    pub fn applied(group: &str, log_id: LogId<u64>, payload_hash: u64) {
        let (Some(world), Some(node)) = (world(), current_node()) else {
            return;
        };
        world.trace(format_args!(
            "apply n{node} {group} {}:{} {payload_hash:x}",
            log_id.leader_id.term, log_id.index
        ));
        let now = world.elapsed_ms();
        world
            .lock()
            .observer
            .applied(node, log_id.index, log_id.leader_id.term, payload_hash, now);
    }

    /// A replica made these log entries durable.
    pub fn appended(_group: &str, ids: &[LogId<u64>]) {
        let (Some(world), Some(node)) = (world(), current_node()) else {
            return;
        };
        let mut state = world.lock();
        for id in ids {
            state.observer.appended(node, id.index, id.leader_id.term);
        }
    }

    /// A replica deleted its log from `index` on.
    pub fn truncated(_group: &str, index: u64) {
        let (Some(world), Some(node)) = (world(), current_node()) else {
            return;
        };
        world.lock().observer.truncated(node, index);
    }

    pub fn reuse_offset_after_dedup() -> bool {
        world().is_some_and(|world| world.knobs().reuse_offset_after_dedup)
    }

    pub fn ack_unreachable_appends() -> bool {
        world().is_some_and(|world| world.knobs().ack_unreachable_appends)
    }
}
