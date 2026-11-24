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
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

use nabedra_check::history::{Event, EventType};
use nabedra_rt::sim::{self, SimEvent, World};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

pub use nabedra_oltp_types::sim_probe::{Ledger, Probe, ProbeSink, Span};

use crate::{
    invariants::{
        Invariants,
        protocol::{DEFAULT_VOTERS, NOT_APPLIED_NEVER_COMMITTED, Origin, Protocol},
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
    /// How many times that node was cut before.
    pub incarnation: u64,
    /// False when the node was already cut: a destructor of a dead incarnation sent it.
    pub live: bool,
    pub probe: Probe,
}

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
                protocol: Protocol::new(DEFAULT_VOTERS),
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

    /// Native leases and RangeClock commits outstanding on `node` in its current
    /// incarnation, as the probes report them.
    pub fn ledgers(&self, node: u64) -> (u64, u64) {
        let hub = self.lock();
        (
            hub.protocol.native_outstanding(node),
            hub.protocol.clock_pending(node),
        )
    }

    /// NotAppliedNeverCommitted: a definite `not_applied` answer for a request ID means
    /// no replica ever commits a direct entry carrying that identity, unless the client
    /// later retries the same ID.
    pub fn check_outcomes(&self, world: &World, events: &[Event]) {
        let mut violations = Vec::new();
        {
            let hub = self.lock();
            let committed: BTreeMap<_, _> = hub
                .protocol
                .dedup_records()
                .map(|(identity, t_ns)| {
                    let key = format!("{}/{}", identity.table, identity.key.history());
                    ((key, identity.request_id.to_string()), t_ns)
                })
                .collect();
            for (index, event) in events.iter().enumerate() {
                if event.kind != EventType::Fail
                    || event.value.get("not_applied").and_then(Value::as_bool) != Some(true)
                {
                    continue;
                }
                let (Some(key), Some(request)) = (&event.key, &event.request_id) else {
                    continue;
                };
                let Some(t_ns) = committed.get(&(key.clone(), request.clone())) else {
                    continue;
                };
                let retried = events[index + 1..].iter().any(|later| {
                    later.kind == EventType::Invoke && later.request_id.as_ref() == Some(request)
                });
                if !retried {
                    violations.push(format!(
                        "{key} request {request}: answered not_applied at history event {} but a replica committed it at {} ns",
                        event.index, t_ns
                    ));
                }
            }
        }
        for detail in violations {
            self.report(world, NOT_APPLIED_NEVER_COMMITTED, detail);
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
        let node = sim::current_node();
        let t_ns = u64::try_from(world.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let live = node.is_none_or(|node| world.is_up(node));
        let (event, violations, invariants) = {
            let mut hub = self.lock();
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
            let violations = hub.protocol.observe(
                Origin {
                    node,
                    incarnation,
                    t_ns,
                    live,
                },
                &event.probe,
            );
            if let Some(trace) = &mut hub.trace {
                trace.probe(&event);
            }
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
        let mut hub = self.lock();
        let (node, action) = match event {
            SimEvent::Crash { node } => {
                *hub.incarnations.entry(*node).or_default() += 1;
                hub.protocol.crashed(*node);
                (*node, "Crash")
            }
            SimEvent::Restart { node } => (*node, "Restart"),
            SimEvent::Wipe { node, .. } => {
                hub.protocol.wiped(*node);
                (*node, "Wipe")
            }
            _ => return,
        };
        if let Some(trace) = &mut hub.trace {
            trace.fault(t_ns, node, action);
        }
    }

    fn report(&self, world: &World, kind: &str, detail: String) {
        let invariants = self.lock().invariants.clone();
        if let Some(checks) = invariants.upgrade() {
            Invariants::from_checks(checks).violate(world, kind, detail);
        }
    }
}

/// The probe's fields without its name and without digests. Digests cover payload bytes,
/// which can hold values from `ring`'s own generator (the registered worker's restart
/// key, for example), so they may differ between replays of one seed. The invariants
/// compare them within a run; traces never record them.
pub(crate) fn fields(probe: &Probe) -> Map<String, Value> {
    let mut fields = match serde_json::to_value(probe) {
        Ok(Value::Object(fields)) => fields,
        _ => Map::new(),
    };
    fields.remove("probe");
    fields.remove("digest");
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
