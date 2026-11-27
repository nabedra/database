//! Deterministic simulation of an Events cluster. See `README.md` in this directory.
//!
//! A whole cluster runs on one current-thread tokio runtime with paused time. The per-node
//! context (clocks, seeded ids, the task registry and inline blocking work) is the
//! `nabedra_rt::sim` [`World`](nabedra_rt::sim::World), which the product code reaches
//! through `crate::rt`. The network plan, trace hash and Raft observer come from
//! `nabedra-sim`. This module keeps only what is specific to Events: the [`Harness`] that
//! holds the brokers, the knobs, the history and those shared parts for one run.

pub mod cluster;
mod history;
pub mod net;
pub mod runtime;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use nabedra_rt::sim::World;
use nabedra_sim::{LogId, NetPlan, RaftObserver, Trace, Vote};

use crate::raft::Broker;

pub use cluster::{Knobs, Report, SimConfig, run};

/// The harness state of one run, next to the `nabedra_rt` world.
pub(crate) struct State {
    pub net: NetPlan,
    pub trace: Trace,
    pub observer: RaftObserver,
    pub history: history::History,
    /// The broker each running node serves.
    pub brokers: BTreeMap<u64, Arc<Broker>>,
    /// Nodes whose volume lost the group: the empty-volume rule (EVENTS-1) keeps them out
    /// of it, so they neither vote nor acknowledge.
    pub lost: BTreeSet<u64>,
    /// Clients stamp commands from `Group::time_bounds`, as a product leader does after
    /// EVENTS-2. Off only for the planted `unguarded_clock`.
    pub guarded_time: bool,
    /// Per replica, in apply order: the offset each event ID was assigned, and the IDs
    /// retention removed. A new offset for an ID is legal only after its removal (EVT-G03
    /// within retention, EVENTS-9).
    pub assigned: BTreeMap<u64, BTreeMap<uuid::Uuid, u64>>,
    pub retained: BTreeMap<u64, BTreeSet<uuid::Uuid>>,
}

/// The Events part of one simulated run: the `nabedra_rt` world plus the network plan,
/// trace, observer, history and brokers.
pub(crate) struct Harness {
    world: Arc<World>,
    knobs: Knobs,
    state: Mutex<State>,
}

impl Harness {
    pub fn new(world: Arc<World>, knobs: Knobs, voters: u64) -> Arc<Self> {
        let seed = world.seed();
        Arc::new(Self {
            world,
            knobs,
            state: Mutex::new(State {
                net: NetPlan::new(seed),
                trace: Trace::new(),
                observer: RaftObserver::new(voters),
                history: history::History::default(),
                brokers: BTreeMap::new(),
                lost: BTreeSet::new(),
                guarded_time: false,
                assigned: BTreeMap::new(),
                retained: BTreeMap::new(),
            }),
        })
    }

    pub fn world(&self) -> &Arc<World> {
        &self.world
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Virtual milliseconds since the run started, the same on every node.
    pub fn elapsed_ms(&self) -> u64 {
        nabedra_sim::runtime::virtual_ms(&self.world)
    }

    /// Adds one line to the trace hash. Every run with the same seed must add the same
    /// lines in the same order.
    pub fn trace(&self, line: fmt::Arguments<'_>) {
        let now = self.elapsed_ms();
        self.lock().trace.record(now, line);
    }

    pub fn broker(&self, node: u64) -> Option<Arc<Broker>> {
        if !self.world.is_up(node) {
            return None;
        }
        self.lock().brokers.get(&node).cloned()
    }

    pub fn is_up(&self, node: u64) -> bool {
        self.world.is_up(node)
    }

    pub fn is_paused(&self, node: u64) -> bool {
        self.lock().net.is_paused(node)
    }

    pub async fn wait_unpaused(&self, node: u64) {
        while self.is_paused(node) {
            nabedra_rt::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn knobs(&self) -> Knobs {
        self.knobs
    }
}

/// Every installed harness, by the address of its world. Product hooks run on the
/// simulation thread and on `nabedra_rt`'s blocking helper, which both see the world, so
/// the world is the key. A `nabedra_rt` per-world extension slot would replace this.
static HARNESSES: Mutex<Vec<(usize, Arc<Harness>)>> = Mutex::new(Vec::new());

fn harnesses() -> MutexGuard<'static, Vec<(usize, Arc<Harness>)>> {
    HARNESSES.lock().unwrap_or_else(PoisonError::into_inner)
}

fn key(world: &Arc<World>) -> usize {
    Arc::as_ptr(world) as usize
}

/// Registers `harness` for its world until the guard drops.
#[must_use = "the harness is unregistered when the guard drops"]
pub(crate) struct Installed {
    key: usize,
}

impl Installed {
    pub fn new(harness: Arc<Harness>) -> Self {
        let key = key(harness.world());
        let mut all = harnesses();
        assert!(
            all.iter().all(|(other, _)| *other != key),
            "a world has one harness"
        );
        all.push((key, harness));
        Self { key }
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        harnesses().retain(|(key, _)| *key != self.key);
    }
}

/// The harness of the world installed on this thread, if any.
pub(crate) fn harness() -> Option<Arc<Harness>> {
    let world = nabedra_rt::sim::world()?;
    let key = key(&world);
    harnesses()
        .iter()
        .find(|(other, _)| *other == key)
        .map(|(_, harness)| harness.clone())
}

/// The node the current task or blocking job belongs to, if any.
pub fn current_node() -> Option<u64> {
    nabedra_rt::sim::current_node()
}

/// Runs `future` as part of `node`.
pub(crate) async fn in_node<F: std::future::Future>(node: u64, future: F) -> F::Output {
    nabedra_rt::sim::in_node(node, future).await
}

/// Milliseconds on `node`'s wall clock (what that broker's HTTP layer would stamp).
pub(crate) async fn wall_ms_of(node: u64) -> i64 {
    in_node(node, async { crate::rt::unix_ms() as i64 }).await
}

/// openraft 0.9's log id as the observer's.
pub(crate) fn log_id(id: openraft::LogId<u64>) -> LogId {
    LogId::new(id.leader_id.term, id.leader_id.node_id, id.index)
}

/// openraft 0.9's vote as the observer's. Both order by `(term, node)`, then committed.
pub(crate) fn vote(vote: openraft::Vote<u64>) -> Vote {
    let leader = vote.leader_id;
    if vote.committed {
        Vote::new_committed(leader.term, leader.node_id)
    } else {
        Vote::new(leader.term, leader.node_id)
    }
}

/// Entry points the product code calls under `cfg(feature = "sim")`.
pub(crate) mod hooks {
    use openraft::LogId;
    use serde::Serialize;

    use super::{current_node, harness};

    pub fn payload_hash(payload: &impl Serialize) -> u64 {
        nabedra_sim::fnv(&serde_json::to_vec(payload).unwrap_or_default())
    }

    /// A replica applied `log_id` with this payload.
    pub fn applied(group: &str, log_id: LogId<u64>, payload_hash: u64) {
        let (Some(harness), Some(node)) = (harness(), current_node()) else {
            return;
        };
        let now = harness.elapsed_ms();
        let mut state = harness.lock();
        state.trace.record(
            now,
            format_args!("apply n{node} {group} {log_id} {payload_hash:x}"),
        );
        state
            .observer
            .applied(group, node, super::log_id(log_id), payload_hash, now);
    }

    /// A replica made these log entries durable.
    pub fn appended(group: &str, ids: &[LogId<u64>]) {
        let (Some(harness), Some(node)) = (harness(), current_node()) else {
            return;
        };
        let mut state = harness.lock();
        for id in ids {
            state.observer.appended(group, node, super::log_id(*id));
        }
    }

    /// A replica deleted its log from `index` on.
    pub fn truncated(group: &str, index: u64) {
        let (Some(harness), Some(node)) = (harness(), current_node()) else {
            return;
        };
        harness.lock().observer.truncated(group, node, index);
    }

    /// A replica applied a publish that gave `event_id` the new offset `offset`. A
    /// different earlier offset on the same replica must have been removed by retention.
    pub fn assigned(event_id: uuid::Uuid, offset: u64) {
        let (Some(harness), Some(node)) = (harness(), current_node()) else {
            return;
        };
        let now = harness.elapsed_ms();
        let mut state = harness.lock();
        let removed = state
            .retained
            .get(&node)
            .is_some_and(|ids| ids.contains(&event_id));
        let previous = state
            .assigned
            .entry(node)
            .or_default()
            .insert(event_id, offset);
        if let Some(previous) = previous.filter(|previous| *previous != offset && !removed) {
            state.observer.violate(
                now,
                format!(
                    "EVT-G03 dedup-miss: n{node} gave {event_id} offset {offset} while its offset {previous} was retained"
                ),
            );
        }
    }

    /// A replica's retention removed `event_id` with its deduplication record.
    pub fn retained(event_id: uuid::Uuid) {
        let (Some(harness), Some(node)) = (harness(), current_node()) else {
            return;
        };
        harness
            .lock()
            .retained
            .entry(node)
            .or_default()
            .insert(event_id);
    }

    pub fn reuse_offset_after_dedup() -> bool {
        harness().is_some_and(|harness| harness.knobs().reuse_offset_after_dedup)
    }

    pub fn ack_unreachable_appends() -> bool {
        harness().is_some_and(|harness| harness.knobs().ack_unreachable_appends)
    }
}
