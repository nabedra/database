//! Drives the real controller group and worker actors on a seeded runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use nabedra_rt::sim::net::{Dial, Envelope, Fate, Planner, SimListener, node_ip};
use nabedra_rt::sim::{self, NodeClock, World};
use nabedra_rt::time;
use nabedra_sim::{Filters, NetPlan, Plan, RaftObserver, runtime};
use uuid::Uuid;

use super::super::{
    clock::{Clock, ClockMode},
    controller::{self, MAX_MOVES_PER_WORKER, RECONCILE_MS, SimConfig, SimRaftMetrics},
    protocol::{
        Admin, CONTROL_FRAME, Heartbeat, LEASE_MS, Member, Progress, RENEW_MS, Reply, Request,
        Slot, Topology, VERSION, WorkerId,
    },
    transport::{Endpoint, PeerConnection, PeerTls},
    worker::{self, SlotSample, Worker},
};
use super::clock_bound::{BoundSpec, SimClockBound};
use super::history::Op;
use super::hooks::{Hooks, Knobs};
use super::invariants::Observations;
use crate::cache::{
    command::{SLOT_COUNT, key_slot},
    listener::Backend,
    store::EvictionPolicy,
    wire::Response,
};

pub(super) const CONTROLLERS: [u64; 3] = [1, 2, 3];
const OPERATOR: u64 = 99;
const CLIENT: u64 = 100;
const CONTROL: u16 = 9200;
const RAFT: u16 = 9201;
const PEER: u16 = 9202;
const RESP: u16 = 9203;
const MIB: usize = 1 << 20;
const START_BUDGET: u64 = 15_000;
// A committed freeze with matching baselines can be between primaries.
// Allow one lease fence, two reconciliation turns, and three heartbeats
// to report the freeze, cut over, and install the grant. One more heartbeat
// covers alignment, clock uncertainty, and message time.
const HANDOFF_BUDGET_MS: u64 = LEASE_MS + 2 * RECONCILE_MS + 4 * RENEW_MS;

fn replica_repair_budget_ms(pending: usize) -> u64 {
    // K03 Convergence is separate from the 15 s client/primary deadline.
    // With links healed and three distinct zones, a full worker can delay the
    // next slot by one 64-move wave. Budget the whole backlog as serial waves.
    // Each wave has two placement ticks (start and commit), three worker
    // heartbeats (install intent, report baseline, install the assignment),
    // and a full round of 256-slot polls on the longest possible peer lane.
    // Reserve another renewal interval per wave for bounded transport,
    // snapshot continuation and tick alignment. Try the assertions as soon as
    // all copies are ready. A stuck transfer still exhausts this deadline.
    let sweep_ms =
        SLOT_COUNT.div_ceil(worker::PROBES) as u64 * worker::REPLICATION_POLL.as_millis() as u64;
    let wave_ms = 4 * RENEW_MS + 2 * RECONCILE_MS + sweep_ms;
    pending.div_ceil(MAX_MOVES_PER_WORKER) as u64 * wave_ms
}

pub(super) type Observed = Arc<Mutex<Observations>>;
type Controller = (SimRaftMetrics, tokio::task::JoinHandle<io::Result<()>>);

/// What a blocked link does to the messages sent across it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Partitions {
    /// The message is lost and its direction stalls until the connection
    /// closes. A peer that gives up never hears of it.
    Loss,
    /// TCP retransmits. The message waits for the heal and arrives in order,
    /// even if its sender has timed out and closed the connection.
    Late,
}

#[derive(Clone, Copy)]
pub(super) struct Config {
    pub partitions: Partitions,
    pub nodes: usize,
    pub groups: usize,
    pub actors: usize,
    pub slot_bytes: usize,
    pub eviction: EvictionPolicy,
    pub clock_mode: ClockMode,
    pub base_error_ns: u64,
    pub declared_ppm: u32,
    pub skew: bool,
    pub gradual_drift: bool,
    pub knobs: Knobs,
    pub check_replica_prefix: bool,
    pub keep_full_trace: bool,
}

struct Network {
    immediate: Arc<AtomicBool>,
    plan: Arc<Mutex<NetPlan>>,
    observed: Observed,
    world: Weak<World>,
    partitions: Partitions,
}

impl Planner for Network {
    fn message(&mut self, envelope: &Envelope<'_>) -> Fate {
        let from = envelope.from.unwrap_or(0);
        let to = envelope.to.unwrap_or(0);
        let (plan, held) = {
            let mut links = self.plan.lock().unwrap();
            let held = self.partitions == Partitions::Late && links.is_blocked(from, to);
            (links.plan(from, to), held)
        };
        let Some(world) = self.world.upgrade() else {
            return Fate::Reset;
        };
        self.observed.lock().unwrap().trace.record(
            runtime::virtual_ms(&world),
            format_args!(
                "frame {from}->{to} conn={} seq={} {:?} bytes={} {plan:?}",
                envelope.conn.0,
                envelope.seq,
                envelope.kind,
                envelope.bytes.len()
            ),
        );
        match plan {
            // `Cluster::link` also black-holes the link, which holds what is
            // sent meanwhile until the heal.
            Plan::Drop if held => {
                self.observed.lock().unwrap().held += 1;
                Fate::NOW
            }
            Plan::Drop => Fate::Drop,
            // The TCP backend delivers one ordered byte stream. Duplicate
            // segments are invisible to its frame parser.
            Plan::Deliver { delay_ms, .. } => Fate::Deliver {
                delay: Duration::from_millis(if self.immediate.load(Ordering::Relaxed) {
                    0
                } else {
                    delay_ms
                }),
            },
        }
    }

    fn dial(&mut self, from: Option<u64>, to: Option<u64>) -> Dial {
        let plan = self.plan.lock().unwrap();
        let (from, to) = (from.unwrap_or(0), to.unwrap_or(0));
        if !plan.connected(from, to) || plan.is_paused(from) || plan.is_paused(to) {
            Dial::Hang
        } else {
            Dial::Connect
        }
    }
}

pub(super) struct Cluster {
    pub world: Arc<World>,
    pub keys: Arc<Vec<String>>,
    pub observed: Observed,
    pub config: Config,
    pub workers: BTreeMap<u64, Worker>,
    controllers: BTreeMap<u64, Controller>,
    plan: Arc<Mutex<NetPlan>>,
    immediate: Arc<AtomicBool>,
    paths: Volume,
    clocks: BTreeMap<u64, Clock>,
    raft: RaftObserver,
    next_incarnation: u64,
    next_admin: u64,
    next_clock_sync_ms: u64,
    ttl_left: BTreeMap<String, u64>,
    crashed_worker: Option<WorkerId>,
}

impl Cluster {
    pub async fn boot(world: Arc<World>, keys: Vec<String>, config: Config) -> Self {
        let keys = Arc::new(keys);
        let watched = keys.iter().map(|key| key_slot(key.as_bytes())).collect();
        let observed = Arc::new(Mutex::new(Observations::new(
            config.check_replica_prefix,
            config.keep_full_trace,
        )));
        let plan = Arc::new(Mutex::new(NetPlan::new(world.seed())));
        let immediate = Arc::new(AtomicBool::new(false));
        let observer = Arc::clone(&observed);
        let commits = Arc::clone(&observed);
        let commit_keys = Arc::clone(&keys);
        let observe_world = Arc::downgrade(&world);
        let commit_world = Arc::downgrade(&world);
        world.insert_ext(Hooks {
            knobs: config.knobs,
            watched,
            expiry_readings: Default::default(),
            observe: Box::new(move |node, event| {
                let world = observe_world.upgrade().unwrap();
                let now = runtime::virtual_ms(&world);
                let truth = u128::from(sim::EPOCH_MS) * 1_000_000
                    + runtime::virtual_elapsed(&world).as_nanos();
                let now_ns = u64::try_from(truth).unwrap_or(u64::MAX);
                observer.lock().unwrap().observe(node, event, now, now_ns);
            }),
            committed: Box::new(move |node, index, term, leader, topology, applied| {
                let now = runtime::virtual_ms(&commit_world.upgrade().unwrap());
                commits.lock().unwrap().committed(
                    node,
                    index,
                    term,
                    leader,
                    now,
                    topology,
                    &commit_keys,
                    applied,
                );
            }),
        });
        world.set_net_planner(Network {
            immediate: Arc::clone(&immediate),
            plan: Arc::clone(&plan),
            observed: Arc::clone(&observed),
            world: Arc::downgrade(&world),
            partitions: config.partitions,
        });
        let mut bounds = SimClockBound::default();
        let nodes = CONTROLLERS
            .into_iter()
            .chain(worker_nodes(config.nodes))
            .chain([OPERATOR, CLIENT]);
        let mut clocks = BTreeMap::new();
        for node in nodes.clone() {
            world.add_node(node, NodeClock::default());
            world.register_host(&host(node), node);
            world.set_certificate(node, fingerprint(node));
            bounds.add_node(
                node,
                BoundSpec {
                    base_error_ns: config.base_error_ns,
                    declared_drift_ppm: config.declared_ppm,
                },
            );
        }
        world.insert_ext(bounds);
        for node in nodes {
            let residual = if config.skew && (11..OPERATOR).contains(&node) {
                (config.base_error_ns as i64) * if node == 11 { -1 } else { 1 }
            } else {
                0
            };
            sim::in_node(node, async {
                clocks.insert(node, Clock::new(config.clock_mode).unwrap());
                world
                    .with_ext::<SimClockBound, _>(|bound| {
                        bound.resync(&world, node, residual).unwrap();
                        if config.gradual_drift && (11..OPERATOR).contains(&node) {
                            let ppm = i64::from(config.declared_ppm) / 2;
                            bound
                                .set_drift_ppm(&world, node, if node % 2 == 0 { ppm } else { -ppm })
                                .unwrap();
                        }
                    })
                    .unwrap();
            })
            .await;
        }
        let mut cluster = Self {
            world,
            keys,
            observed,
            config,
            workers: BTreeMap::new(),
            controllers: BTreeMap::new(),
            plan,
            immediate,
            paths: Volume::new().unwrap(),
            clocks,
            raft: RaftObserver::new(3),
            next_incarnation: 0,
            next_admin: 0,
            next_clock_sync_ms: 10_000,
            ttl_left: BTreeMap::new(),
            crashed_worker: None,
        };
        for node in CONTROLLERS {
            cluster.start_controller(node).await;
        }
        for node in worker_nodes(cluster.config.nodes) {
            let member = member(node, Uuid::nil());
            let reply = cluster.admin(Admin::Enroll { member }).await;
            assert!(
                matches!(reply, Reply::Applied { .. }),
                "enroll {node}: {reply:?}"
            );
            cluster.start_worker(node).await;
        }
        cluster
            .wait_for(START_BUDGET, |topology| {
                topology.workers.len() == config.nodes
            })
            .await;
        let reply = cluster
            .admin(Admin::Init {
                groups: cluster.config.groups,
            })
            .await;
        assert!(matches!(reply, Reply::Applied { .. }), "init: {reply:?}");
        cluster
            .wait_for(START_BUDGET, |topology| {
                topology.ready_primaries == SLOT_COUNT && topology.ready_replicas == SLOT_COUNT
            })
            .await;
        cluster
    }

    pub async fn start_controller(&mut self, node: u64) {
        self.world.restart(node);
        let config = SimConfig {
            cluster: Uuid::from_u128(0xca55e),
            node,
            dir: self.paths.0.join(format!("controller-{node}")),
            clock: self.config.clock_mode,
            voters: CONTROLLERS
                .into_iter()
                .map(|n| (n, endpoint(n, RAFT)))
                .collect(),
            operators: vec![fingerprint(OPERATOR)],
            tls: PeerTls::sim(fingerprint(node)),
        };
        let handles = self
            .world
            .spawn_node(node, async move {
                let app = SimListener::bind(SocketAddr::new(node_ip(node), CONTROL)).unwrap();
                let raft = SimListener::bind(SocketAddr::new(node_ip(node), RAFT)).unwrap();
                controller::sim_start(config, app, raft).await.unwrap()
            })
            .await
            .unwrap();
        self.raft.restarted("cache", node, []);
        self.controllers.insert(node, handles);
        self.trace(format_args!("controller start {node}"));
    }

    pub async fn start_worker(&mut self, node: u64) {
        self.world.restart(node);
        self.next_incarnation += 1;
        let member = member(node, Uuid::from_u128(u128::from(self.next_incarnation)));
        let controllers = CONTROLLERS
            .into_iter()
            .map(|n| (n, endpoint(n, CONTROL)))
            .collect();
        let (mode, actors, slot_bytes, eviction) = (
            self.config.clock_mode,
            self.config.actors,
            self.config.slot_bytes,
            self.config.eviction,
        );
        let worker = self
            .world
            .spawn_node(node, async move {
                let peer = SimListener::bind(SocketAddr::new(node_ip(node), PEER)).unwrap();
                worker::start_sim(
                    member,
                    peer,
                    PeerTls::sim(fingerprint(node)),
                    controllers,
                    mode,
                    actors,
                    slot_bytes,
                    eviction,
                )
                .unwrap()
            })
            .await
            .unwrap();
        self.workers.insert(node, worker);
        self.trace(format_args!("worker start {node}"));
    }

    /// Commits a new incarnation for `node` while its current process keeps
    /// running. The enrollment retires the running incarnation, as an
    /// operator replacement does when the old host is cut off but alive.
    /// The replacement presents the node's certificate, so the request is
    /// sent from the node's address over links opened to the leader until it
    /// answers. The old process shares that address, so its own heartbeat can
    /// cross too: it renews before the enrollment applies, and is refused
    /// after. Read the committed grant after this returns.
    pub async fn enroll_replacement(&mut self, node: u64) -> Reply {
        self.next_incarnation += 1;
        let member = member(node, Uuid::from_u128(u128::from(self.next_incarnation)));
        self.trace(format_args!(
            "enroll replacement incarnation {} for {node}",
            self.next_incarnation
        ));
        let end = self.now() + START_BUDGET;
        loop {
            if let Some(leader) = self.leader() {
                self.link(node, leader, false);
                self.link(leader, node, false);
                let request = Request::Enroll(member.clone());
                let result = sim::in_node(node, async move {
                    PeerTls::sim(fingerprint(node))
                        .connect(&endpoint(leader, CONTROL))
                        .await?
                        .json::<_, Reply>(&request, CONTROL_FRAME)
                        .await
                })
                .await;
                self.link(node, leader, true);
                self.link(leader, node, true);
                if let Ok(reply) = result
                    && !matches!(reply, Reply::Retry)
                {
                    return reply;
                }
            }
            if self.now() >= end {
                return Reply::Retry;
            }
            self.step(25).await;
        }
    }

    pub fn crash(&mut self, node: u64) {
        self.trace(format_args!("crash {node}"));
        self.world.crash(node);
        self.workers.remove(&node);
        self.controllers.remove(&node);
        self.observed.lock().unwrap().crash(node);
    }

    pub fn lose_worker_ram(&mut self, node: u64) {
        self.crashed_worker = Some(self.workers[&node].id());
        self.crash(node);
    }

    pub fn link(&self, from: u64, to: u64, blocked: bool) {
        if blocked {
            self.plan.lock().unwrap().block(from, to);
            self.world.black_hole(Some(from), Some(to));
        } else {
            self.plan.lock().unwrap().unblock(from, to);
            self.world.heal(Some(from), Some(to));
        }
    }

    pub fn isolate_controller_links(&self, node: u64) {
        self.trace(format_args!("fence controller traffic for {node}"));
        for controller in CONTROLLERS {
            self.link(controller, node, true);
            self.link(node, controller, true);
        }
    }

    pub fn partition_operator(&self, node: u64) {
        self.link(OPERATOR, node, true);
        self.link(node, OPERATOR, true);
    }

    pub fn filters(&self, filters: Filters) {
        self.plan.lock().unwrap().set_filters(filters);
        self.trace(format_args!("network filters {filters:?}"));
    }

    pub fn pause(&self, node: u64) {
        self.trace(format_args!("pause {node}"));
        self.plan.lock().unwrap().pause(node);
        self.world.freeze(node);
        self.world.black_hole(Some(node), None);
        self.world.black_hole(None, Some(node));
    }

    pub fn heal(&self) {
        self.trace(format_args!("heal"));
        for node in CONTROLLERS
            .into_iter()
            .chain(worker_nodes(self.config.nodes))
        {
            self.world.thaw(node);
        }
        self.plan.lock().unwrap().heal_all();
        self.world.heal_all();
    }

    pub fn drift(&self, node: u64, ppm: i64) {
        self.trace(format_args!("drift {node} {ppm} ppm"));
        self.world
            .with_ext::<SimClockBound, _>(|bound| bound.set_drift_ppm(&self.world, node, ppm))
            .unwrap()
            .unwrap();
    }

    pub fn immediate_delivery(&self) {
        self.trace(format_args!("zero-delay network for lease-boundary turn"));
        self.immediate.store(true, Ordering::Relaxed);
    }

    pub async fn reconcile_once(&self) {
        let leader = self.leader().expect("cache leader for boundary turn");
        sim::in_node(leader, self.controllers[&leader].0.reconcile_once()).await;
    }

    pub async fn renew_once(&self, node: u64) -> io::Result<()> {
        let leader = self.leader().expect("cache leader for boundary renewal");
        let worker = &self.workers[&node];
        sim::in_node(
            node,
            worker::sim_renew_once(
                worker,
                PeerTls::sim(fingerprint(node)),
                endpoint(leader, CONTROL),
            ),
        )
        .await
    }

    /// The heartbeat `node` would send now: its installed topology stamp and
    /// its actors' progress. A schedule keeps it to deliver late.
    pub async fn capture_heartbeat(&self, node: u64) -> Heartbeat {
        let worker = &self.workers[&node];
        let topology = worker.topology();
        let progress = sim::in_node(node, worker.sim_progress()).await;
        Heartbeat {
            version: VERSION,
            generation: topology.generation,
            revision: topology.revision,
            worker: worker.id(),
            progress,
        }
    }

    /// Delivers `heartbeat` to the leader from `node`'s address and
    /// certificate, as a heartbeat held in the network arrives after newer
    /// ones. `node` must be cut from the controllers: the request opens its
    /// links to the leader for its own round trip and closes them again.
    pub async fn deliver_heartbeat(&self, node: u64, heartbeat: Heartbeat) -> Option<Reply> {
        let leader = self.leader()?;
        self.trace(format_args!(
            "deliver an older heartbeat of {node} to {leader}"
        ));
        self.link(node, leader, false);
        self.link(leader, node, false);
        let request = Request::Heartbeat(heartbeat);
        let result = sim::in_node(node, async move {
            PeerTls::sim(fingerprint(node))
                .connect(&endpoint(leader, CONTROL))
                .await?
                .json::<_, Reply>(&request, CONTROL_FRAME)
                .await
        })
        .await;
        self.link(node, leader, true);
        self.link(leader, node, true);
        result.ok()
    }

    pub fn now(&self) -> u64 {
        runtime::virtual_ms(&self.world)
    }

    pub fn trace(&self, args: std::fmt::Arguments<'_>) {
        self.observed.lock().unwrap().trace.record(self.now(), args);
    }

    pub fn violate(&self, kind: &'static str, detail: String) {
        let time = self.now();
        self.observed
            .lock()
            .unwrap()
            .violate(kind, format!("t={time}ms {detail}"));
    }

    pub fn slot(&self, slot: u16) -> Slot {
        self.observed
            .lock()
            .unwrap()
            .latest
            .as_ref()
            .map_or(Slot::default(), |topology| {
                topology.slots[usize::from(slot)]
            })
    }

    pub fn leader(&self) -> Option<u64> {
        self.controllers
            .values()
            .filter_map(|(metrics, _)| {
                let (term, leader, _) = metrics.read();
                leader.map(|leader| (term, leader))
            })
            .max()
            .map(|(_, leader)| leader)
    }

    pub async fn admin(&mut self, command: Admin) -> Reply {
        self.next_admin += 1;
        let operation = Uuid::from_u128(u128::from(self.next_admin));
        let end = self.now() + START_BUDGET;
        loop {
            let mut controllers = Vec::from(CONTROLLERS);
            if let Some(leader) = self.leader() {
                controllers.retain(|&node| node != leader);
                controllers.insert(0, leader);
            }
            for node in controllers {
                let request = Request::Admin {
                    version: VERSION,
                    operation,
                    command: command.clone(),
                };
                let result = sim::in_node(OPERATOR, async move {
                    PeerTls::sim(fingerprint(OPERATOR))
                        .connect(&endpoint(node, CONTROL))
                        .await?
                        .json::<_, Reply>(&request, CONTROL_FRAME)
                        .await
                })
                .await;
                if let Ok(reply) = result
                    && !matches!(reply, Reply::Retry)
                {
                    return reply;
                }
            }
            if self.now() >= end {
                return Reply::Retry;
            }
            self.step(25).await;
        }
    }

    async fn wait_for(&mut self, budget: u64, wants: impl Fn(&Topology) -> bool) {
        let end = self.now() + budget;
        loop {
            if let Reply::Status(topology) = self.admin(Admin::Status).await
                && wants(&topology)
            {
                return;
            }
            assert!(
                self.now() < end,
                "cache cluster did not become ready ({}ms), latest stamp {:?}",
                self.now(),
                self.observed.lock().unwrap().latest.as_ref().map(|t| (
                    t.generation,
                    t.revision,
                    t.workers.len()
                ))
            );
            self.step(100).await;
        }
    }

    pub async fn step(&mut self, ms: u64) {
        let end = self.now() + ms;
        while self.now() < end {
            time::sleep(Duration::from_millis((end - self.now()).min(10))).await;
            if self.config.gradual_drift && self.now() >= self.next_clock_sync_ms {
                self.refresh_bounds().await;
            }
            self.sample_leases().await;
            let now = self.now();
            for (metrics, service) in self.controllers.values() {
                assert!(!service.is_finished(), "simulated controller stopped");
                metrics.observe(&mut self.raft, now);
            }
        }
    }

    async fn refresh_bounds(&mut self) {
        self.next_clock_sync_ms = self.now() + 10_000;
        for node in CONTROLLERS
            .into_iter()
            .chain(worker_nodes(self.config.nodes))
        {
            if self.world.is_up(node) && !self.world.is_frozen(node) {
                sim::in_node(node, async {
                    self.world
                        .with_ext::<SimClockBound, _>(|bound| bound.resync(&self.world, node, 0))
                        .unwrap()
                        .unwrap();
                })
                .await;
            }
        }
        self.trace(format_args!("refresh clock bounds"));
    }

    pub async fn sample_leases(&mut self) {
        let views = self.observed.lock().unwrap().leases.clone();
        let mut bounds = BTreeMap::new();
        for node in CONTROLLERS
            .into_iter()
            .chain(self.workers.keys().copied())
            .filter(|&node| self.world.is_up(node))
        {
            let world = &self.world;
            let clock = self.clocks.get_mut(&node).unwrap();
            let truth =
                u128::from(sim::EPOCH_MS) * 1_000_000 + runtime::virtual_elapsed(world).as_nanos();
            let (sample, now) = sim::in_node(node, async {
                let sample = world
                    .with_ext::<SimClockBound, _>(|bound| bound.sample(world))
                    .unwrap();
                (sample, clock.now())
            })
            .await;
            if let Ok((lower, upper)) = sample
                && !(u128::from(lower)..=u128::from(upper)).contains(&truth)
            {
                let now = runtime::virtual_ms(world);
                self.observed.lock().unwrap().violate(
                    "ClockAssumption",
                    format!(
                        "t={now}ms node {node} reported [{lower}, {upper}] around truth {truth}"
                    ),
                );
            }
            if let Ok(now) = now
                && !world.is_frozen(node)
            {
                bounds.insert(node, now);
            }
        }
        let mut writable = BTreeMap::new();
        for ((slot, owner), lease) in views {
            // The plan's rule (Leases), written out here rather than calling
            // the product's lease function: the lower bound has reached
            // `not_before` and the upper bound is still before the expiry.
            if let Some(now) = bounds.get(&owner.node)
                && now.lower_ms >= lease.not_before_ms
                && now.upper_ms < lease.expires_ms
                && let Some(previous) = writable.insert(slot, owner)
            {
                self.violate(
                    "LeaseExclusive",
                    format!(
                        "slot {slot} accepts writes on {} and {}",
                        previous.node, owner.node
                    ),
                );
            }
        }
    }

    pub async fn send(&mut self, key: &str, op: Op, node: Option<u64>) -> Option<Response> {
        let slot = key_slot(key.as_bytes());
        let node = node.or_else(|| self.slot(slot).primary.map(|id| id.node))?;
        let worker = self.workers.get(&node)?.clone();
        let topology = worker.topology();
        let owner = topology.slots[usize::from(slot)]
            .primary
            .and_then(|id| topology.member(id));
        let start = self.now();
        let invocation = {
            let mut observed = self.observed.lock().unwrap();
            let index = observed.history.len();
            observed
                .history
                .push(op.invoke(CLIENT, worker.id(), key, start));
            index
        };
        let command = op.command(key);
        let destination = worker.clone();
        let task = self
            .world
            .spawn_node(node, async move { destination.sim_execute(command).await });
        let reply = time::timeout(Duration::from_millis(800), task)
            .await
            .ok()
            .and_then(Result::ok);
        let now = self.now();
        let mut completion = op.complete(CLIENT, worker.id(), key, now, reply.as_ref());
        self.trace(format_args!(
            "client {} {key} node={node} {:?} {:?}",
            op.name(),
            completion.kind,
            reply.as_ref().and_then(|(_, epoch)| *epoch)
        ));
        {
            let mut observed = self.observed.lock().unwrap();
            if completion.kind == nabedra_check::history::EventType::Ok
                && observed.history[invocation].value.get("at_ns").is_some()
            {
                let deadline = observed.history[invocation].value["at_ns"]
                    .as_u64()
                    .unwrap();
                observed.history[invocation].value["at_ns"] =
                    (deadline + (now - start) * 1_000_000).into();
            }
            if matches!(op, Op::Set(..) | Op::Del | Op::Incr(_))
                && let Some((_, Some(epoch))) = reply.as_ref()
                && let Some(sequence) = observed.served_sequence(slot, worker.id(), *epoch)
            {
                completion.value["sequence"] = sequence.into();
            }
            observed.history.push(completion);
        }
        if let Some((response, _)) = &reply {
            let expected = owner.filter(|owner| owner.id != worker.id()).map(|member| {
                format!("MOVED {slot} {}:{}", member.client_host, member.client_port)
            });
            let actual = match response {
                Response::Error(error) if error.starts_with(b"MOVED ") => Some(error.as_ref()),
                _ => None,
            };
            if actual != expected.as_deref().map(str::as_bytes) {
                self.violate(
                    "RedirectAgrees",
                    format!("slot {slot} returned {response:?}, expected redirect {expected:?}"),
                );
            }
        }
        if let Some((Response::Integer(ttl), _)) = &reply {
            if matches!(op, Op::Pttl) && *ttl >= 0 {
                if let Some(last) = self.ttl_left.insert(key.to_owned(), *ttl as u64)
                    && *ttl as u64 > last + 11
                {
                    self.violate(
                        "TtlExtends",
                        format!("PTTL {key} grew from {last} to {ttl}"),
                    );
                }
            } else if !matches!(op, Op::Pttl | Op::Get) {
                self.ttl_left.remove(key);
            }
        } else if matches!(op, Op::Set(..)) {
            self.ttl_left.remove(key);
        }
        reply.map(|(response, _)| response)
    }

    /// Returns a witness key if the fault schedule destroyed every copy of a slot.
    pub async fn verify_quiescence(&mut self, cold: bool) -> Option<String> {
        let handoff_deadline = self.now() + HANDOFF_BUDGET_MS;
        let (latest, reports) = self.recovery_reports().await;
        let mut lost = BTreeSet::new();
        let mut handoffs = BTreeSet::new();
        for (index, &slot) in latest.slots.iter().enumerate() {
            let index = index as u16;
            let primary = baseline(&reports, index, slot.epoch, slot.primary);
            if primary.is_some_and(|p| p.serving_primary) {
                continue;
            }
            if !cold
                && let Some(moving) = slot.moving
                && moving.freeze_requested
                && !moving.replica_only
                && let Some(source) = primary
                && baseline(&reports, index, slot.epoch, Some(moving.target))
                    .is_some_and(|target| target.sequence == source.sequence)
            {
                handoffs.insert(index);
                continue;
            }
            let lost_primary = !cold
                && slot.primary.is_some_and(|owner| {
                    self.crashed_worker == Some(owner) && latest.member(owner).is_none()
                });
            if lost_primary
                && [slot.replica, slot.moving.map(|m| m.target)]
                    .into_iter()
                    .all(|copy| {
                        copy.is_none_or(|id| {
                            latest.member(id).is_none()
                                || progress_at(&reports, index, slot.epoch, id)
                                    .is_some_and(|report| !report.baseline)
                        })
                    })
            {
                // The old incarnation held the current epoch and can never
                // return. A newly assigned copy may still lack its baseline
                // when that primary crashes. Require an explicit fenced reset.
                lost.insert(index);
            } else {
                self.violate(
                    "Liveness",
                    format!("slot {index} has no authorized primary after healing"),
                );
            }
        }
        if !lost.is_empty() {
            self.trace(format_args!(
                "RAM loss removed all baselines on {} slots, first {}",
                lost.len(),
                lost.first().unwrap()
            ));
        }
        self.verify_final_reads(cold, &lost, &handoffs).await;
        let (latest, reports) = if handoffs.is_empty() {
            (latest, reports)
        } else {
            self.verify_handoffs(&handoffs, handoff_deadline).await;
            self.recovery_reports().await
        };
        self.verify_replica_convergence(cold, &lost, &latest, reports)
            .await;
        let slot = *lost.first()?;
        let witness = (0..)
            .map(|number| format!("cache-sim:lost:{number}"))
            .find(|key| key_slot(key.as_bytes()) == slot)
            .expect("generate a key for the lost slot");
        let reply = self.send(&witness, Op::Get, None).await;
        self.check_lost_reply(&witness, reply);
        Some(witness)
    }

    async fn recovery_reports(&self) -> (Topology, BTreeMap<WorkerId, Vec<Progress>>) {
        let latest = self.observed.lock().unwrap().latest.clone().unwrap();
        let mut reports = BTreeMap::new();
        for (&node, worker) in &self.workers {
            let mut progress = sim::in_node(node, worker.sim_progress()).await;
            progress.sort_unstable_by_key(|report| report.slot);
            reports.insert(worker.id(), progress);
        }
        (latest, reports)
    }

    async fn verify_replica_convergence(
        &mut self,
        cold: bool,
        lost: &BTreeSet<u16>,
        latest: &Topology,
        reports: BTreeMap<WorkerId, Vec<Progress>>,
    ) {
        let mut pending = pending_copies(latest, &reports, lost);
        let budget = replica_repair_budget_ms(pending.0);
        let start = self.now();
        while pending.0 > 0 && self.now() - start < budget {
            self.step(RENEW_MS.min(budget - (self.now() - start))).await;
            let (latest, reports) = self.recovery_reports().await;
            pending = pending_copies(&latest, &reports, lost);
        }
        if pending.0 > 0 {
            self.violate(
                "Convergence",
                format!(
                    "{} unconverged slots (first {}) after {budget}ms replica repair budget",
                    pending.0, pending.1
                ),
            );
        }

        let slots: BTreeSet<_> = self
            .keys
            .iter()
            .map(|key| key_slot(key.as_bytes()))
            .collect();
        for slot in slots.difference(lost).copied() {
            let placement = self.slot(slot);
            let primary = self.sample(placement.primary, slot).await;
            let replica = self.sample(placement.replica, slot).await;
            match (primary, replica) {
                (Some(p), Some(r))
                    if p.baseline
                        && r.baseline
                        && (p.generation, p.epoch, p.sequence, p.digest)
                            == (r.generation, r.epoch, r.sequence, r.digest) =>
                {
                    if cold && (p.digest != [0; 20] || p.sequence != 0) {
                        self.violate(
                            "ColdStartEmpty",
                            format!("slot {slot} retained data after cold reset"),
                        );
                    }
                }
                (p, r) => self.violate(
                    "Convergence",
                    format!("slot {slot} primary {p:?}, replica {r:?}, placement {placement:?}"),
                ),
            }
        }

        for violation in self.raft.take_violations() {
            self.violate("Raft", violation);
        }
    }

    async fn verify_final_reads(
        &mut self,
        cold: bool,
        lost: &BTreeSet<u16>,
        handoffs: &BTreeSet<u16>,
    ) {
        let keys = Arc::clone(&self.keys);
        for key in keys.iter() {
            let slot = key_slot(key.as_bytes());
            if handoffs.contains(&slot) {
                continue;
            }
            if lost.contains(&slot) {
                let reply = self.send(key, Op::Get, None).await;
                self.check_lost_reply(key, reply);
            } else {
                self.verify_final_read(key, cold).await;
            }
        }
        if let Some(worker) = self.workers.values().next() {
            let topology = worker.topology();
            if !slots_agree(worker.slots(), &topology) || !shards_agree(worker.shards(), &topology)
            {
                self.violate(
                    "RedirectAgrees",
                    format!(
                        "CLUSTER SLOTS or SHARDS disagrees with worker {} topology",
                        worker.id().node
                    ),
                );
            }
        }
    }

    async fn verify_handoffs(&mut self, handoffs: &BTreeSet<u16>, deadline: u64) {
        let mut pending = handoffs.clone();
        while !pending.is_empty() {
            for slot in std::mem::take(&mut pending) {
                let placement = self.slot(slot);
                if !self
                    .sample(placement.primary, slot)
                    .await
                    .is_some_and(|sample| sample.authorized && sample.epoch == placement.epoch)
                {
                    pending.insert(slot);
                }
            }
            if pending.is_empty() || self.now() >= deadline {
                break;
            }
            self.step(RECONCILE_MS.min(deadline - self.now())).await;
        }
        if let Some(slot) = pending.first() {
            self.violate(
                "Liveness",
                format!("slot {slot} had no authority after its fenced handoff"),
            );
        }
        let keys = Arc::clone(&self.keys);
        for key in keys
            .iter()
            .filter(|key| handoffs.contains(&key_slot(key.as_bytes())))
        {
            self.verify_final_read(key, false).await;
        }
    }

    async fn verify_final_read(&mut self, key: &str, cold: bool) {
        match self.send(key, Op::Get, None).await {
            Some(Response::Bulk(value)) => {
                if cold && value.is_some() {
                    self.violate("ColdStartEmpty", format!("old key {key} still readable"));
                }
            }
            reply => self.violate("Liveness", format!("final read {key}: {reply:?}")),
        }
    }

    fn check_lost_reply(&self, key: &str, reply: Option<Response>) {
        if !matches!(&reply, Some(Response::Error(error)) if error.starts_with(b"CLUSTERDOWN")) {
            self.violate(
                "Liveness",
                format!("slot without a baseline must fail closed for {key}, got {reply:?}"),
            );
        }
    }

    pub async fn sample(&self, id: Option<WorkerId>, slot: u16) -> Option<SlotSample> {
        let id = id?;
        let worker = self.workers.get(&id.node)?.clone();
        if worker.id() != id {
            return None;
        }
        sim::in_node(id.node, worker.sim_slot(slot)).await
    }
}

fn baseline(
    reports: &BTreeMap<WorkerId, Vec<Progress>>,
    slot: u16,
    epoch: u64,
    worker: Option<WorkerId>,
) -> Option<&Progress> {
    progress_at(reports, slot, epoch, worker?).filter(|report| report.baseline)
}

fn progress_at(
    reports: &BTreeMap<WorkerId, Vec<Progress>>,
    slot: u16,
    epoch: u64,
    worker: WorkerId,
) -> Option<&Progress> {
    let reports = reports.get(&worker)?;
    let report = &reports[reports.binary_search_by_key(&slot, |r| r.slot).ok()?];
    (report.epoch == epoch).then_some(report)
}

fn pending_copies(
    topology: &Topology,
    reports: &BTreeMap<WorkerId, Vec<Progress>>,
    lost: &BTreeSet<u16>,
) -> (usize, u16) {
    let mut pending = 0;
    let mut first = 0;
    for (index, slot) in topology.slots.iter().enumerate() {
        let index = index as u16;
        if lost.contains(&index) {
            continue;
        }
        let primary = baseline(reports, index, slot.epoch, slot.primary);
        let replica = baseline(reports, index, slot.epoch, slot.replica);
        if !primary
            .zip(replica)
            .is_some_and(|(p, r)| p.sequence == r.sequence)
        {
            if pending == 0 {
                first = index;
            }
            pending += 1;
        }
    }
    (pending, first)
}

fn slots_agree(response: Response, topology: &Topology) -> bool {
    let Response::Array(rows) = response else {
        return false;
    };
    let mut seen = vec![false; SLOT_COUNT];
    for row in rows {
        let Response::Array(fields) = row else {
            return false;
        };
        let [
            Response::Integer(first),
            Response::Integer(last),
            Response::Array(primary),
            rest @ ..,
        ] = fields.as_slice()
        else {
            return false;
        };
        let replicas = match rest {
            [] => None,
            [Response::Array(replica)] => Some(replica.as_slice()),
            _ => return false,
        };
        if !check_range(*first, *last, topology, &mut seen, |p, r| {
            address_agrees(primary, p)
                && match (replicas, r) {
                    (None, None) => true,
                    (Some(reply), Some(member)) => address_agrees(reply, member),
                    _ => false,
                }
        }) {
            return false;
        }
    }
    projected_coverage(&seen, topology)
}

fn shards_agree(response: Response, topology: &Topology) -> bool {
    let Response::Array(groups) = response else {
        return false;
    };
    let mut seen = vec![false; SLOT_COUNT];
    for group in groups {
        let Response::Array(fields) = group else {
            return false;
        };
        let [
            slots,
            Response::Array(ranges),
            nodes,
            Response::Array(members),
        ] = fields.as_slice()
        else {
            return false;
        };
        if !is_bulk(slots, b"slots") || !is_bulk(nodes, b"nodes") || ranges.len() % 2 != 0 {
            return false;
        }
        let (primary, replica) = match members.as_slice() {
            [Response::Array(primary)] => (primary, None),
            [Response::Array(primary), Response::Array(replica)] => (primary, Some(replica)),
            _ => return false,
        };
        for range in ranges.chunks_exact(2) {
            let [Response::Integer(first), Response::Integer(last)] = range else {
                return false;
            };
            if !check_range(*first, *last, topology, &mut seen, |p, r| {
                shard_member_agrees(primary, p, b"master")
                    && match (replica, r) {
                        (None, None) => true,
                        (Some(reply), Some(member)) => {
                            shard_member_agrees(reply, member, b"replica")
                        }
                        _ => false,
                    }
            }) {
                return false;
            }
        }
    }
    projected_coverage(&seen, topology)
}

fn check_range(
    first: i64,
    last: i64,
    topology: &Topology,
    seen: &mut [bool],
    matches: impl Fn(&Member, Option<&Member>) -> bool,
) -> bool {
    let (Ok(first), Ok(last)) = (usize::try_from(first), usize::try_from(last)) else {
        return false;
    };
    if last < first || last >= topology.slots.len() {
        return false;
    }
    for (slot, seen) in topology.slots[first..=last]
        .iter()
        .zip(&mut seen[first..=last])
    {
        let primary = slot.primary.and_then(|id| topology.member(id));
        let replica = slot.replica.and_then(|id| topology.member(id));
        if *seen || !primary.is_some_and(|p| matches(p, replica)) {
            return false;
        }
        *seen = true;
    }
    true
}

fn projected_coverage(seen: &[bool], topology: &Topology) -> bool {
    topology
        .slots
        .iter()
        .zip(seen)
        .all(|(slot, seen)| *seen == slot.primary.is_some_and(|id| topology.member(id).is_some()))
}

fn address_agrees(response: &[Response], member: &Member) -> bool {
    matches!(response, [host, Response::Integer(port), _]
        if is_bulk(host, member.client_host.as_bytes()) && *port == i64::from(member.client_port))
}

fn shard_member_agrees(response: &[Response], member: &Member, role: &[u8]) -> bool {
    let field = |key| {
        response
            .chunks_exact(2)
            .find(|pair| is_bulk(&pair[0], key))
            .map(|pair| &pair[1])
    };
    field(b"role").is_some_and(|answer| is_bulk(answer, role))
        && field(b"endpoint").is_some_and(|answer| is_bulk(answer, member.client_host.as_bytes()))
        && field(b"port") == Some(&Response::Integer(i64::from(member.client_port)))
}

fn is_bulk(response: &Response, expected: &[u8]) -> bool {
    matches!(response, Response::Bulk(Some(value)) if value.as_ref() == expected)
}

pub(super) async fn operator_connection(node: u64) -> PeerConnection {
    sim::in_node(OPERATOR, async move {
        PeerTls::sim(fingerprint(OPERATOR))
            .connect(&endpoint(node, CONTROL))
            .await
            .expect("operator connection")
    })
    .await
}

pub(super) fn worker_nodes(count: usize) -> std::ops::Range<u64> {
    11..11 + count as u64
}

fn host(node: u64) -> String {
    format!("n{node}.cache.test")
}

fn fingerprint(node: u64) -> [u8; 32] {
    let mut fingerprint = [0; 32];
    fingerprint[..8].copy_from_slice(&node.to_le_bytes());
    fingerprint
}

fn endpoint(node: u64, port: u16) -> Endpoint {
    Endpoint {
        host: host(node),
        port,
        fingerprints: vec![fingerprint(node)],
    }
}

fn member(node: u64, incarnation: Uuid) -> Member {
    Member {
        id: WorkerId { node, incarnation },
        zone: ["a", "b", "c", "a", "b"][(node - 11) as usize].to_owned(),
        client_host: host(node),
        client_port: RESP,
        peer: endpoint(node, PEER),
    }
}

struct Volume(PathBuf);

impl Volume {
    fn new() -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = if std::path::Path::new("/dev/shm").is_dir() {
            PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        loop {
            let dir = root.join(format!(
                "nabedra-cache-sim-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::create_dir(&dir) {
                Ok(()) => return Ok(Self(dir)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for Volume {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn memory_limit(mib: usize) -> usize {
    mib * MIB
}
