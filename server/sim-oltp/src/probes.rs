//! The OLTP probe stream: the product's `sim_probe` sink, protocol invariants, the run
//! trace and nemesis subscriptions.
//!
//! [`Probes::install`] puts a `ProbeSink` in the World. Every probe then:
//!
//! 1. gets a sequence number, its node, the node's incarnation and the virtual time;
//! 2. goes into the run trace (so the trace hash covers it) and, when `NABEDRA_TRACE_DIR`
//!    is set, into the `nabedra.trace/1` file ([`crate::trace`]);
//! 3. runs the probe-driven invariants ([`crate::invariants::protocol`]), which report
//!    through [`Invariants`];
//! 4. reaches every [`Subscription`] whose filter accepts it.
//!
//! A subscription is how a nemesis places a fault at a protocol point, for example a
//! leader crash right after an `Applied` probe for a client's request. Its filter runs
//! inside the probe call, so it must only inspect the event. The nemesis acts on the
//! received event later, from its own task: a fault injected from inside the probe call
//! would run product destructors while the product is mid-step.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, Weak},
    thread::{self, ThreadId},
};

use nabedra_check::history::{Event, EventType};
use nabedra_rt::sim::{self, SimEvent, World};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

pub use nabedra_oltp_types::sim_probe::{Ledger, LogEntry, Probe, ProbeSink, RouteRecord, Span};
use nabedra_oltp_types::{
    sim_probe,
    types::{Digest, GroupKind},
};

use crate::{
    history::Bodies,
    invariants::{
        Invariants,
        protocol::{Ledgers, NOT_APPLIED_NEVER_COMMITTED, Origin, Protocol},
    },
    trace::TraceFile,
};

#[cfg(test)]
mod tests;

/// One probe as subscribers see it.
#[derive(Clone, Debug)]
pub struct ProbeEvent {
    /// Position in the run's probe stream, from 0.
    pub seq: u64,
    /// Virtual time since the World started.
    pub t_ns: u64,
    /// The simulated node that emitted it, if any.
    pub node: Option<u64>,
    /// How many times that node's process ended before.
    pub incarnation: u64,
    /// False when the node was already cut: a destructor of a dead incarnation sent it.
    pub live: bool,
    pub probe: Probe,
}

/// The kind of a fault of the harness itself, as `nabedra_sim` and `sim_bugs.py` name it. A
/// run that reports it is not clean, and `sim_bugs.py` never counts it as a detection.
pub(crate) const HARNESS: &str = "harness";

type Filter = Box<dyn FnMut(&ProbeEvent) -> bool + Send>;

/// Probe events a filter accepted, in stream order.
pub struct Subscription {
    events: mpsc::UnboundedReceiver<ProbeEvent>,
}

impl Subscription {
    /// The next accepted event. `None` once the run's probe stream has closed.
    pub async fn recv(&mut self) -> Option<ProbeEvent> {
        self.events.recv().await
    }

    /// An accepted event that already arrived, without waiting.
    pub fn try_recv(&mut self) -> Option<ProbeEvent> {
        self.events.try_recv().ok()
    }
}

struct Hub {
    seq: u64,
    incarnations: BTreeMap<u64, u64>,
    /// Nodes cut and not restarted yet.
    down: BTreeSet<u64>,
    /// The node whose ledgers are being snapshotted. Its probes come from no node context.
    snapshot: Option<u64>,
    /// The threads that emitted a probe, numbered in order. A job that a storage pause
    /// stops keeps its thread, so the number says which job a probe belongs to.
    threads: HashMap<ThreadId, u64>,
    protocol: Protocol,
    subscribers: Vec<(Filter, mpsc::UnboundedSender<ProbeEvent>)>,
    trace: Option<TraceFile>,
    invariants: Weak<Mutex<super::invariants::Checks>>,
}

/// The run's probe stream. Clones share it.
#[derive(Clone)]
pub struct Probes {
    hub: Arc<Mutex<Hub>>,
}

impl Probes {
    /// Installs the probe sink for this run. `workload` names the run in trace headers.
    pub fn install(world: &Arc<World>, invariants: &Invariants, workload: &str) -> Self {
        let probes = Self {
            hub: Arc::new(Mutex::new(Hub {
                seq: 0,
                incarnations: BTreeMap::new(),
                down: BTreeSet::new(),
                snapshot: None,
                threads: HashMap::new(),
                protocol: Protocol::new(),
                subscribers: Vec::new(),
                trace: TraceFile::from_env(world.seed(), workload),
                invariants: invariants.downgrade(),
            })),
        };
        // Weak references: the World owns the sink and observer, and must not keep
        // this run's state alive after the run.
        let world_ref = Arc::downgrade(world);
        let hub = Arc::downgrade(&probes.hub);
        ProbeSink::new(move |probe| {
            if let (Some(world), Some(hub)) = (world_ref.upgrade(), hub.upgrade()) {
                Self { hub }.receive(&world, probe);
            }
        })
        .install(world);
        let hub = Arc::downgrade(&probes.hub);
        let world_ref = Arc::downgrade(world);
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            if let (Some(world), Some(hub)) = (world_ref.upgrade(), hub.upgrade()) {
                Self { hub }.fault(&world, event);
            }
        }));
        invariants.set_probes(probes.clone());
        probes
    }

    /// Delivers every later probe that `filter` accepts. The filter runs inside the
    /// probe call: it must not call product code or block.
    pub fn subscribe(
        &self,
        filter: impl FnMut(&ProbeEvent) -> bool + Send + 'static,
    ) -> Subscription {
        let (sender, events) = mpsc::unbounded_channel();
        self.lock().subscribers.push((Box::new(filter), sender));
        Subscription { events }
    }

    /// Reads every ledger the live resource owners of `node` hold, through `LedgerSnapshot`
    /// probes.
    pub fn snapshot(&self, node: u64) -> Ledgers {
        {
            let mut hub = self.lock();
            hub.protocol.clear_ledgers(node);
            hub.snapshot = Some(node);
        }
        sim_probe::snapshot_ledgers(node);
        let mut hub = self.lock();
        hub.snapshot = None;
        hub.protocol.ledgers(node)
    }

    /// Native leases the probes saw acquired and not released on `node` in its current
    /// incarnation.
    pub fn native_outstanding(&self, node: u64) -> u64 {
        self.lock().protocol.native_outstanding(node)
    }

    /// NotAppliedNeverCommitted: after a definite `not_applied` answer, no replica ever
    /// applies a direct entry that commits that identity and body (lead decision 12.1). A
    /// retry of the ID excuses nothing. D03 requires a refusal of another body under a
    /// committed identity. The entry carries the product's digest of its body, and `bodies`
    /// has the same digest of every body that was sent, so a refusal is excused only when each
    /// committed digest is the one of another body sent under the identity. A second entry
    /// under the identity is DedupUnique's.
    pub fn check_outcomes(&self, world: &World, events: &[Event], bodies: &Bodies) {
        let committed: BTreeMap<_, _> = self
            .lock()
            .protocol
            .committed()
            .map(|(identity, committed)| {
                let key = format!("{}/{}", identity.table, identity.key.history());
                ((key, identity.request_id.to_string()), committed.clone())
            })
            .collect();
        for event in events {
            if event.kind != EventType::Fail
                || event.value.get("not_applied").and_then(Value::as_bool) != Some(true)
            {
                continue;
            }
            let Some(request) = &event.request_id else {
                continue;
            };
            self.ran(NOT_APPLIED_NEVER_COMMITTED, 1);
            for key in keys(event, events) {
                if let Some((t_ns, digests)) = committed.get(&(key.clone(), request.clone())) {
                    if std::env::var_os("P10W5_OLD_RULE").is_none()
                        && another_body(event, events, bodies, digests)
                    {
                        continue;
                    }
                    self.report(world, NOT_APPLIED_NEVER_COMMITTED, format!(
                        "{key} request {request}: answered not_applied at history event {} but a replica committed it at {t_ns} ns",
                        event.index
                    ));
                }
            }
        }
    }

    /// How often each protocol rule ran on an event of its own. A rule that is absent
    /// was not exercised and must be reported as not run.
    pub fn exercised(&self) -> BTreeMap<&'static str, u64> {
        self.lock().protocol.exercised().clone()
    }

    /// Counts evaluations of a rule that the harness drives.
    pub(crate) fn ran(&self, rule: &'static str, times: u64) {
        self.lock().protocol.ran(rule, times);
    }

    pub(crate) fn check_routes(&self, world: &World) {
        let violations = self.lock().protocol.check_routes();
        for (kind, detail) in violations {
            self.report(world, kind, detail);
        }
    }

    /// Writes the `nabedra.trace/1` file, if one is configured. Called once per run.
    pub fn finish(&self) -> std::io::Result<()> {
        let trace = self.lock().trace.take();
        match trace {
            Some(trace) => trace.write(),
            None => Ok(()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Hub> {
        self.hub
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn receive(&self, world: &World, probe: Probe) {
        let t_ns = u64::try_from(world.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let (event, violations, invariants) = {
            let mut hub = self.lock();
            let node = hub.snapshot.or_else(sim::current_node);
            let live = node.is_none_or(|node| world.is_up(node));
            let incarnation = node.map_or(0, |node| *hub.incarnations.get(&node).unwrap_or(&0));
            let event = ProbeEvent {
                seq: hub.seq,
                t_ns,
                node,
                incarnation,
                live,
                probe,
            };
            hub.seq += 1;
            let known = hub.threads.len() as u64;
            let thread = *hub.threads.entry(thread::current().id()).or_insert(known);
            let violations = hub.protocol.observe(
                Origin {
                    node,
                    incarnation,
                    t_ns,
                    live,
                    thread,
                },
                &event.probe,
            );
            if let Some(trace) = &mut hub.trace {
                trace.probe(&event);
            }
            #[cfg(test)]
            tests::record_control(&event);
            hub.subscribers.retain_mut(|(filter, sender)| {
                !sender.is_closed() && (!filter(&event) || sender.send(event.clone()).is_ok())
            });
            (event, violations, hub.invariants.clone())
        };
        let Some(checks) = invariants.upgrade() else {
            return;
        };
        let invariants = Invariants::from_checks(checks);
        invariants.record_probe(world, &event);
        for (kind, detail) in violations {
            invariants.violate(world, kind, detail);
        }
    }

    fn fault(&self, world: &World, event: &SimEvent<'_>) {
        let t_ns = u64::try_from(world.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let mut beyond = Vec::new();
        let mut hub = self.lock();
        let (node, action) = match event {
            SimEvent::Crash { node } => {
                *hub.incarnations.entry(*node).or_default() += 1;
                hub.down.insert(*node);
                hub.protocol.crashed(*node);
                (*node, "Crash")
            }
            SimEvent::Restart { node } => {
                // A restart without a cut follows a drained process.
                if !hub.down.remove(node) {
                    *hub.incarnations.entry(*node).or_default() += 1;
                    hub.protocol.crashed(*node);
                }
                (*node, "Restart")
            }
            SimEvent::Wipe { node, .. } => {
                beyond = hub.protocol.wiped(*node);
                (*node, "Wipe")
            }
            _ => return,
        };
        if let Some(trace) = &mut hub.trace {
            trace.fault(t_ns, node, action);
        }
        drop(hub);
        for (kind, detail) in beyond {
            self.report(world, kind, detail);
            // What every consumer already refuses to count: a fault of the harness itself.
            self.report(world, HARNESS, format!("{kind}: not a detection"));
        }
    }

    fn report(&self, world: &World, kind: &str, detail: String) {
        let invariants = self.lock().invariants.clone();
        if let Some(checks) = invariants.upgrade() {
            Invariants::from_checks(checks).violate(world, kind, detail);
        }
    }
}

/// The invocation that `refused` answers. `None` unless its process has exactly that one call
/// open, under the same identity: otherwise the history does not say which body was refused.
fn open_call(refused: &Event, events: &[Event]) -> Option<usize> {
    let mut open = Vec::new();
    for event in events
        .iter()
        .filter(|event| event.process == refused.process && event.index < refused.index)
    {
        if event.kind == EventType::Invoke {
            open.push(event);
        } else if open.len() == 1 {
            open.clear();
        } else {
            return None;
        }
    }
    let [call] = open[..] else { return None };
    (call.request_id == refused.request_id && call.f == refused.f).then_some(call.index)
}

/// Whether every committed digest is of a body that was sent under the identity and is not the
/// one that `refused` answers. A body or a digest that the history does not know proves nothing.
fn another_body(
    refused: &Event,
    events: &[Event],
    bodies: &Bodies,
    committed: &BTreeSet<Digest>,
) -> bool {
    let sent =
        |event: &&Event| event.kind == EventType::Invoke && event.request_id == refused.request_id;
    let Some(body) = open_call(refused, events).and_then(|index| bodies.get(&index)) else {
        return false;
    };
    let known: BTreeSet<_> = events
        .iter()
        .filter(sent)
        .filter_map(|invoke| bodies.get(&invoke.index))
        .collect();
    !committed.is_empty()
        && committed
            .iter()
            .all(|digest| digest != body && known.contains(digest))
}

/// The `table/key` names an operation's history event covers: its own key, or the rows its
/// invocation listed.
fn keys(event: &Event, events: &[Event]) -> Vec<String> {
    if let Some(key) = &event.key {
        return vec![key.clone()];
    }
    events
        .iter()
        .filter(|invoke| {
            invoke.kind == EventType::Invoke
                && invoke.process == event.process
                && invoke.request_id == event.request_id
                && invoke.index < event.index
        })
        .filter_map(|invoke| invoke.value.as_array())
        .flatten()
        .filter_map(|row| row.get("key").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

/// The probe's fields without its name.
///
/// The digest of an applied Control entry is left out (design 3.9): each Docker-profile
/// worker registers its restart key there (`RegisterRestartMount`), and
/// `registered/restart.rs` draws that key from `ring`'s `SystemRandom`, which no simulation
/// seeds. Its public key and the evidence over it are the only bytes that differed between
/// two runs of a seed, and they stopped differing once the key was fixed. The invariants
/// still compare these digests within a run, and `control_digests_replay` compares them
/// between runs: it is red until the key is seeded (OLTP-sim-restart-key-entropy).
pub(crate) fn fields(probe: &Probe) -> Map<String, Value> {
    let mut fields = match serde_json::to_value(probe) {
        Ok(Value::Object(fields)) => fields,
        _ => Map::new(),
    };
    fields.remove("probe");
    if matches!(
        probe,
        Probe::Applied {
            kind: GroupKind::Control,
            ..
        }
    ) {
        fields.remove("digest");
    }
    fields
}

/// A short, payload-free description of a probe for the hashed run trace.
pub(crate) fn describe(event: &ProbeEvent) -> String {
    let node = match event.node {
        Some(node) => format!("n{node}/{}", event.incarnation),
        None => "-".to_owned(),
    };
    let fields = Value::Object(fields(&event.probe));
    format!("probe {node} {} {fields}", event.probe.name())
}
