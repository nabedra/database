//! The cluster harness: N brokers on one paused runtime, the nemesis, the workload, the
//! Kafka-style invariants after every step, and the end-of-run consistency checks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use nabedra_kv::fs::FaultFs;
use nabedra_rt::sim::{NodeClock, World};
use nabedra_rt::time::sleep;
use nabedra_sim::net::{DELAY_MAX_MS, DROP_PER_MILLE, DUP_PER_MILLE};
use nabedra_sim::rng::{stream, uuid_from};
use nabedra_sim::{Draw, Filters, SplitMix64};
use openraft::ServerState;
use serde_json::{Value, json};
use uuid::Uuid;

use super::history::{self, Event, FinalLog};
use super::{Harness, Installed, in_node, log_id, vote, wall_ms_of};
use crate::partition::{
    self, Command, ErrorCode, Machine, Mutation, PublishEvent, Query, QueryReply, Reply, Start,
};
use crate::raft::{Broker, Group, GroupError};

pub const GROUP: &str = "p0";
const SUBSCRIPTION: &str = "s";
const OWNER: &str = "c1";
const LEASE_TTL_MS: i64 = 4_000;
const TICK: Duration = Duration::from_millis(10);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const LEASE_SETTLE: Duration = Duration::from_millis(LEASE_TTL_MS as u64 + 200);
// The final heal allows one convergence window, lease expiry, and one drain window.
const REDELIVERY_BOUND: Duration =
    Duration::from_millis((2 * RECOVERY_TIMEOUT.as_millis() + LEASE_SETTLE.as_millis()) as u64);
const PRODUCER: u64 = 0;
const CONSUMER: u64 = 1;
const DRAINER: u64 = 2;
const CLIENTS: u64 = 3;

/// Test-only planted bugs. Each must make some seed fail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Knobs {
    /// The peer network reports an unreachable follower's append as acknowledged, so a
    /// leader can commit without a majority.
    pub ack_unreachable_appends: bool,
    /// A dedup hit rewinds the partition's offset counter, so an offset is reused.
    pub reuse_offset_after_dedup: bool,
    /// A restarted broker's volume loses every write fsynced since its previous start.
    pub lose_fsynced_on_restart: bool,
    /// The consumer cannot redeliver until the post-heal deadline has passed.
    pub delay_redelivery_after_heal: bool,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u64,
    /// Virtual milliseconds of workload under the nemesis.
    pub chaos_ms: u64,
    pub knobs: Knobs,
    /// Include volume wipes in the nemesis (an empty-directory rejoin).
    pub wipes: bool,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            nodes: 3,
            chaos_ms: 12_000,
            knobs: Knobs::default(),
            wipes: false,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub messages: u64,
    pub committed_entries: usize,
    pub history_events: usize,
    pub publishes_ok: usize,
    pub polls_ok: usize,
    pub acks_ok: usize,
    pub crashes: u64,
    pub partitions: u64,
    pub pauses: u64,
    pub clock_jumps: u64,
    /// Leaders seen in a term that already had a different leader: openraft's
    /// `(term, node)` election epoch lets a higher vote take over within one term.
    pub same_term_handoffs: u64,
    pub final_events: usize,
    pub virtual_ms: u64,
}

#[derive(Debug)]
pub struct Report {
    pub seed: u64,
    pub trace_hash: u64,
    pub trace_events: u64,
    pub violations: Vec<String>,
    pub history: Vec<Event>,
    pub trace_tail: Vec<String>,
    pub stats: Stats,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

struct Volume {
    root: tempfile::TempDir,
    fs: FaultFs,
    /// The durable image at this incarnation's start (used by a planted bug).
    at_start: FaultFs,
}

fn fresh_volume(root: &tempfile::TempDir) -> FaultFs {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(root.path());
    fs
}

struct Cluster {
    sim: Arc<Harness>,
    config: SimConfig,
    volumes: BTreeMap<u64, Volume>,
    cluster_id: Uuid,
    topic_id: Uuid,
    rng: SplitMix64,
    stats: Stats,
    resume_at: BTreeMap<u64, u64>,
    restart_at: BTreeMap<u64, u64>,
    /// The nodes seen leading in each term.
    term_leaders: BTreeMap<u64, BTreeSet<u64>>,
}

/// Runs one seed to completion.
pub fn run(config: SimConfig) -> Report {
    let seed = config.seed;
    // A paused current-thread runtime whose `select!` branch order follows the seed under
    // `--cfg tokio_unstable`, with a `nabedra_rt::sim` world installed on this thread.
    let (sim, mut violations, stats) = nabedra_rt::sim::run(seed, |world| async move {
        let sim = Harness::new(world.clone(), config.knobs, config.nodes);
        let _installed = Installed::new(sim.clone());
        // Every broker joins the world down; `Cluster::start` brings it up.
        for id in 1..=config.nodes {
            world.add_node(id, NodeClock::default());
            world.crash(id);
        }
        let mut cluster = Cluster::new(sim.clone(), config);
        let result = cluster.run().await;
        let mut violations = Vec::new();
        if let Err(error) = result {
            violations.push(format!("harness: {error:#}"));
        }
        cluster.stop_all();
        (sim, violations, cluster.stats.clone())
    });
    let mut state = sim.lock();
    let mut all = state.observer.take_violations();
    all.append(&mut violations);
    #[cfg(test)]
    match state.history.check_queue(REDELIVERY_BOUND) {
        Ok(violations) => all.extend(violations),
        Err(error) => all.push(format!(
            "queue checker could not read seed history: {error}"
        )),
    }
    Report {
        seed,
        trace_hash: state.trace.hash(),
        trace_events: state.trace.events(),
        violations: all,
        history: std::mem::take(&mut state.history.events),
        trace_tail: state.trace.tail(),
        stats,
    }
}

impl Cluster {
    fn new(sim: Arc<Harness>, config: SimConfig) -> Self {
        let mut rng = stream(config.seed, "nemesis");
        let volumes = (1..=config.nodes)
            .map(|id| {
                let root = tempfile::Builder::new()
                    .prefix("nabedra-events-sim-")
                    .tempdir()
                    .expect("create a simulation volume root");
                let fs = fresh_volume(&root);
                let at_start = fs.crash();
                (id, Volume { root, fs, at_start })
            })
            .collect();
        Self {
            cluster_id: uuid_from(&mut rng),
            topic_id: uuid_from(&mut rng),
            sim,
            config,
            volumes,
            rng,
            stats: Stats::default(),
            resume_at: BTreeMap::new(),
            restart_at: BTreeMap::new(),
            term_leaders: BTreeMap::new(),
        }
    }

    fn now(&self) -> u64 {
        self.sim.elapsed_ms()
    }

    fn world(&self) -> &World {
        self.sim.world()
    }

    fn trace(&self, line: std::fmt::Arguments<'_>) {
        self.sim.trace(line);
    }

    fn violate(&self, message: String) {
        let now = self.now();
        self.sim.lock().observer.violate(now, message);
    }

    async fn start(&mut self, id: u64) -> Result<()> {
        let volume = self.volumes.get_mut(&id).expect("volume");
        volume.at_start = volume.fs.crash();
        let broker = Broker::sim_open(id, self.cluster_id, volume.root.path(), volume.fs.clone())
            .with_context(|| format!("open broker n{id} from its volume"))?;
        self.sim.world().restart(id);
        {
            let mut state = self.sim.lock();
            state.net.resume(id);
            state.brokers.insert(id, broker.clone());
        }
        let group = in_node(id, broker.open_group(GROUP, Arc::new(Machine)))
            .await
            .map_err(|error| anyhow!("open group on n{id}: {error}"))?;
        let log = in_node(id, group.sim_log_ids()).await?;
        self.sim
            .lock()
            .observer
            .restarted(GROUP, id, log.into_iter().map(log_id));
        self.trace(format_args!("start n{id}"));
        Ok(())
    }

    /// Cuts the node's network and disk, then aborts its tasks.
    fn crash(&mut self, id: u64) {
        // Without its broker the network refuses the node; `resume` also bumps the epoch.
        let broker = {
            let mut state = self.sim.lock();
            state.net.resume(id);
            state.brokers.remove(&id)
        };
        let volume = self.volumes.get_mut(&id).expect("volume");
        volume.fs.halt();
        self.sim.world().crash(id);
        drop(broker);
        let image = if self.config.knobs.lose_fsynced_on_restart {
            volume.at_start.crash()
        } else {
            volume.fs.crash()
        };
        volume.fs = image;
        self.resume_at.remove(&id);
        self.stats.crashes += 1;
        self.trace(format_args!("crash n{id}"));
    }

    fn wipe(&mut self, id: u64) {
        let volume = self.volumes.get_mut(&id).expect("volume");
        volume.fs = fresh_volume(&volume.root);
        self.trace(format_args!("wipe n{id}"));
    }

    fn stop_all(&mut self) {
        for id in 1..=self.config.nodes {
            if self.sim.is_up(id) {
                self.crash(id);
            }
        }
    }

    fn group(&self, id: u64) -> Option<Arc<Group>> {
        self.sim.broker(id)?.group(GROUP)
    }

    fn majority(&self) -> u64 {
        self.config.nodes / 2 + 1
    }

    /// Runs the invariants that read replica metrics.
    fn observe(&mut self) {
        let now = self.now();
        let mut seen = Vec::new();
        for id in 1..=self.config.nodes {
            let Some(group) = self.group(id) else {
                continue;
            };
            let metrics = group.sim_metrics();
            seen.push((
                id,
                vote(metrics.vote),
                metrics.state == ServerState::Leader,
                metrics.last_applied.map(|log| log.index),
            ));
        }
        let mut state = self.sim.lock();
        for (id, vote, leader, applied) in seen {
            if leader {
                let leaders = self.term_leaders.entry(vote.leader.term).or_default();
                if leaders.insert(id) && leaders.len() > 1 {
                    self.stats.same_term_handoffs += 1;
                }
            }
            state
                .observer
                .observe(GROUP, id, vote, leader, applied, now);
        }
        state.observer.check_majority(GROUP, now);
    }

    async fn run(&mut self) -> Result<()> {
        for id in 1..=self.config.nodes {
            self.start(id).await?;
        }
        let members: BTreeMap<u64, String> = (1..=self.config.nodes)
            .map(|id| (id, format!("http://127.0.0.1:{}", 7000 + id)))
            .collect();
        let first = self.group(1).context("group on n1")?;
        in_node(1, first.initialize(members))
            .await
            .map_err(|error| anyhow!("initialize: {error}"))?;
        self.setup().await?;

        let stop = Arc::new(AtomicBool::new(false));
        let seed = self.config.seed;
        let nodes = self.config.nodes;
        let producer =
            nabedra_rt::task::spawn(producer(self.sim.clone(), stop.clone(), seed, nodes));
        let consumer =
            nabedra_rt::task::spawn(consumer(self.sim.clone(), stop.clone(), seed, nodes));

        let chaos_end = self.now() + self.config.chaos_ms;
        let mut next_action = self.now() + self.rng.range(200, 800);
        while self.now() < chaos_end {
            sleep(TICK).await;
            self.observe();
            self.scheduled().await?;
            if self.now() >= next_action {
                self.nemesis().await?;
                next_action = self.now() + self.rng.range(150, 1_200);
            }
        }

        stop.store(true, Ordering::Release);
        nabedra_rt::time::timeout(Duration::from_secs(20), producer)
            .await
            .context("producer did not stop")??;
        nabedra_rt::time::timeout(Duration::from_secs(20), consumer)
            .await
            .context("consumer did not stop")??;
        self.heal_all().await?;
        self.converge().await;
        let checkpoint = self.drain().await;
        self.converge().await;
        self.observe();
        self.final_checks(checkpoint).await;
        let state = self.sim.lock();
        self.stats.messages = state.net.messages();
        self.stats.committed_entries = state.observer.committed_len(GROUP);
        self.stats.history_events = state.history.events.len();
        self.stats.virtual_ms = self.sim.elapsed_ms();
        for event in &state.history.events {
            if event.kind == "ok" {
                match event.f {
                    "publish" => self.stats.publishes_ok += 1,
                    "poll" => self.stats.polls_ok += 1,
                    "ack" => self.stats.acks_ok += 1,
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Initializes the partition and opens the subscription.
    async fn setup(&mut self) -> Result<()> {
        let commands = [
            Command::Init {
                topic_id: self.topic_id,
                partition: 0,
            },
            Command::OpenSubscription {
                name: SUBSCRIPTION.into(),
                start: Start::Earliest,
            },
        ];
        for command in commands {
            let command = serde_json::to_value(&command)?;
            let deadline = self.now() + 30_000;
            loop {
                anyhow::ensure!(self.now() < deadline, "setup did not commit within 30 s");
                sleep(Duration::from_millis(50)).await;
                self.observe();
                let Some((id, group)) = leader(&self.sim, self.config.nodes) else {
                    continue;
                };
                match in_node(id, group.write(command.clone())).await {
                    Ok(value) => {
                        let reply: Mutation = serde_json::from_value(value)?;
                        reply.map_err(|error| anyhow!("setup command failed: {error:?}"))?;
                        break;
                    }
                    Err(_) => continue,
                }
            }
        }
        self.trace(format_args!("setup done"));
        Ok(())
    }

    /// Resumes paused nodes and restarts crashed nodes whose time has come.
    async fn scheduled(&mut self) -> Result<()> {
        let now = self.now();
        let due: Vec<u64> = self
            .resume_at
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.resume_at.remove(&id);
            self.set_paused(id, false);
        }
        let due: Vec<u64> = self
            .restart_at
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.restart_at.remove(&id);
            self.start(id).await?;
        }
        Ok(())
    }

    fn set_paused(&mut self, id: u64, paused: bool) {
        let mut state = self.sim.lock();
        if paused {
            state.net.pause(id);
        } else {
            state.net.resume(id);
        }
        drop(state);
        self.trace(format_args!(
            "{} n{id}",
            if paused { "pause" } else { "resume" }
        ));
    }

    fn down(&self) -> Vec<u64> {
        (1..=self.config.nodes)
            .filter(|id| !self.sim.is_up(*id))
            .collect()
    }

    async fn nemesis(&mut self) -> Result<()> {
        let nodes = self.config.nodes;
        let pick = self.rng.range(1, nodes);
        let now = self.now();
        match self.rng.below(100) {
            // Crash a node while at most a minority is down; it restarts later.
            0..=17 => {
                if self.sim.is_up(pick) && (self.down().len() as u64) < nodes - self.majority() {
                    self.crash(pick);
                    if self.config.wipes && self.rng.chance(200) {
                        self.wipe(pick);
                    }
                    let delay = self.rng.range(200, 3_000);
                    self.restart_at.insert(pick, now + delay);
                }
            }
            // Isolate one node, or the current leader.
            18..=37 => {
                let target = if self.rng.chance(500) {
                    leader(&self.sim, nodes).map_or(pick, |(id, _)| id)
                } else {
                    pick
                };
                self.sim.lock().net.isolate(target, 1..=nodes);
                self.stats.partitions += 1;
                self.trace(format_args!("isolate n{target}"));
            }
            // A one-way link failure.
            38..=45 => {
                let to = self.rng.range(1, nodes);
                if to != pick {
                    self.sim.lock().net.block(pick, to);
                    self.stats.partitions += 1;
                    self.trace(format_args!("block n{pick}->n{to}"));
                }
            }
            46..=65 => {
                self.sim.lock().net.heal_links();
                self.record_heal();
            }
            66..=75 => {
                if self.sim.is_up(pick) && !self.sim.is_paused(pick) {
                    self.set_paused(pick, true);
                    let resume = now + self.rng.range(100, 2_500);
                    self.resume_at.insert(pick, resume);
                    self.stats.pauses += 1;
                }
            }
            76..=85 => {
                let wall = self.rng.range(0, 4_000) as i64 - 1_000;
                let mono = Duration::from_millis(self.rng.range(0, 1_500));
                // Moving the monotonic clock moves the wall clock with it, so the wall
                // jump makes up the difference: the wall clock moves by `wall` in all.
                self.world().advance_mono(pick, mono);
                let wall_jump = wall - mono.as_millis() as i64;
                let by = Duration::from_millis(wall_jump.unsigned_abs());
                if wall_jump >= 0 {
                    self.world().jump_wall_forward(pick, by);
                } else {
                    self.world().jump_wall_backward(pick, by);
                }
                self.stats.clock_jumps += 1;
                self.trace(format_args!(
                    "clock n{pick} wall{wall:+}ms mono+{}ms",
                    mono.as_millis()
                ));
            }
            _ => {
                let filters = Filters {
                    drop_per_mille: DROP_PER_MILLE[self.rng.below(5) as usize],
                    dup_per_mille: DUP_PER_MILLE[self.rng.below(3) as usize],
                    delay_max_ms: DELAY_MAX_MS[self.rng.below(4) as usize],
                };
                self.sim.lock().net.set_filters(filters);
                let Filters {
                    drop_per_mille,
                    dup_per_mille,
                    delay_max_ms,
                } = filters;
                self.trace(format_args!(
                    "filters drop={drop_per_mille} dup={dup_per_mille} delay<={delay_max_ms}ms"
                ));
            }
        }
        Ok(())
    }

    async fn heal_all(&mut self) -> Result<()> {
        self.sim.lock().net.heal_all();
        self.resume_at.clear();
        self.restart_at.clear();
        for id in self.down() {
            self.start(id).await?;
        }
        self.record_heal();
        Ok(())
    }

    fn record_heal(&self) {
        self.trace(format_args!("heal"));
        self.sim.lock().history.heal(self.now());
    }

    /// Waits until every replica applied the same entries and a leader exists.
    async fn converge(&mut self) {
        let deadline = self.now() + RECOVERY_TIMEOUT.as_millis() as u64;
        let mut stable = 0;
        while self.now() < deadline {
            sleep(Duration::from_millis(100)).await;
            self.observe();
            let views: Vec<_> = (1..=self.config.nodes)
                .map(|id| {
                    self.group(id).map(|group| {
                        let metrics = group.sim_metrics();
                        (
                            metrics.last_applied,
                            metrics.last_log_index,
                            metrics.current_leader,
                        )
                    })
                })
                .collect();
            let agreed = views.iter().all(|view| view.is_some() && view == &views[0])
                && views[0].is_some_and(|(applied, last, leader)| {
                    leader.is_some() && applied.map(|log| log.index) == last
                });
            stable = if agreed { stable + 1 } else { 0 };
            if stable >= 3 {
                return;
            }
        }
        self.violate("Liveness: replicas did not converge within 30 s after healing".into());
    }

    /// Takes a fresh lease after the old one expires and acknowledges everything.
    async fn drain(&mut self) -> u64 {
        let delay = if self.config.knobs.delay_redelivery_after_heal {
            REDELIVERY_BOUND
        } else {
            LEASE_SETTLE
        };
        sleep(delay).await;
        let mut client = Client::new(
            self.sim.clone(),
            DRAINER,
            self.config.seed,
            self.config.nodes,
        );
        let mut checkpoint = None;
        let deadline = self.now() + RECOVERY_TIMEOUT.as_millis() as u64;
        while self.now() < deadline {
            match client.consume_once().await {
                Step::Drained(at) => {
                    checkpoint = Some(at);
                    break;
                }
                Step::Progress => {}
                Step::Retry => sleep(Duration::from_millis(100)).await,
            }
        }
        if checkpoint.is_none() {
            self.violate("Liveness: the final drain did not finish within 30 s".into());
        }
        checkpoint.unwrap_or(0)
    }

    async fn final_checks(&mut self, checkpoint: u64) {
        let mut replicas = BTreeMap::new();
        for id in 1..=self.config.nodes {
            let Some(group) = self.group(id) else {
                self.violate(format!("harness: n{id} is not running at the end"));
                continue;
            };
            let read = group.sim_local_read(|tx| partition::sim_state(tx, SUBSCRIPTION));
            match in_node(id, read).await {
                Ok(Ok(replica)) => {
                    replicas.insert(id, replica);
                }
                Ok(Err(error)) => self.violate(format!("harness: n{id} final read: {error:?}")),
                Err(error) => self.violate(format!("harness: n{id} final read: {error:#}")),
            }
        }
        let Some((first, reference)) = replicas.first_key_value() else {
            return;
        };
        for (id, replica) in &replicas {
            if replica != reference {
                self.violate(format!(
                    "ConsistentCommittedData: n{first} and n{id} disagree on retained events, tail or checkpoint"
                ));
            }
        }
        self.stats.final_events = reference.events.len();
        let log: FinalLog = reference
            .events
            .iter()
            .map(|event| (event.offset, event.event_id))
            .collect();
        let last = log.last_key_value().map_or(0, |(&offset, _)| offset);
        if last != reference.tail {
            self.violate(format!(
                "EVT-G02 gap: stored tail {} differs from the last retained offset {last}",
                reference.tail
            ));
        }
        if checkpoint != reference.checkpoint {
            self.violate(format!(
                "EVT-G05 checkpoint: poll returned {checkpoint}, storage reports {}",
                reference.checkpoint
            ));
        }
        let anomalies = {
            let state = self.sim.lock();
            history::check(&state.history, &log, reference.checkpoint)
        };
        for anomaly in anomalies {
            self.violate(anomaly);
        }
    }
}

/// The node that reports itself leader, if any (lowest id first).
fn leader(sim: &Harness, nodes: u64) -> Option<(u64, Arc<Group>)> {
    (1..=nodes).find_map(|id| {
        let group = sim.broker(id)?.group(GROUP)?;
        (group.leader() == Some(id)).then_some((id, group))
    })
}

/// Can `node` reach a majority (itself included) over links in both directions?
fn has_quorum(sim: &Harness, node: u64, nodes: u64) -> bool {
    let members: Vec<u64> = (1..=nodes).collect();
    sim.lock()
        .net
        .has_quorum(node, &members, |id| sim.world().is_up(id))
}

enum Step {
    Progress,
    Retry,
    Drained(u64),
}

/// One workload client: it talks to a broker through the in-process Group API, the way
/// the HTTP layer does, and records each operation in the history.
struct Client {
    sim: Arc<Harness>,
    process: u64,
    rng: SplitMix64,
    ops: u64,
    nodes: u64,
    generation: Option<u64>,
}

enum Outcome<T> {
    Ok(T),
    Fail(String),
    Unknown(String),
}

impl Client {
    fn new(sim: Arc<Harness>, process: u64, seed: u64, nodes: u64) -> Self {
        Self {
            sim,
            process,
            rng: stream(seed, &format!("client-{process}")),
            ops: 0,
            nodes,
            generation: None,
        }
    }

    fn uuid(&mut self) -> Uuid {
        uuid_from(&mut self.rng)
    }

    /// The node that claims leadership, or a random running node (which may redirect).
    fn target(&mut self) -> u64 {
        if !self.rng.chance(100)
            && let Some((id, _)) = leader(&self.sim, self.nodes)
        {
            return id;
        }
        self.rng.range(1, self.nodes)
    }

    fn record(
        &mut self,
        kind: &'static str,
        f: &'static str,
        mut value: Value,
        request: &str,
        node: u64,
    ) {
        if matches!(f, "poll" | "ack") {
            if !value.is_object() {
                value = json!({ "error": value });
            }
            value["subscription"] = json!(SUBSCRIPTION);
        }
        let now = self.sim.elapsed_ms();
        self.sim.trace(format_args!(
            "history {kind} p{} {f} {request} n{node} {value}",
            self.process
        ));
        self.sim
            .lock()
            .history
            .push(kind, self.process, f, value, request, node, now);
        if kind == "info" {
            // Each client owns one residue class, so retired process IDs stay unique.
            self.process += CLIENTS;
        }
    }

    /// Sends one Raft command to `node` and returns its domain outcome.
    async fn write(&mut self, node: u64, command: Command, coalesced: bool) -> Outcome<Reply> {
        let Some(group) = self.sim.broker(node).and_then(|broker| broker.group(GROUP)) else {
            return Outcome::Fail("broker unavailable".into());
        };
        let command = match serde_json::to_value(&command) {
            Ok(command) => command,
            Err(error) => return Outcome::Fail(error.to_string()),
        };
        let call = async move {
            if coalesced {
                group.write_coalesced(command).await
            } else {
                group.write(command).await
            }
        };
        match nabedra_rt::time::timeout(CLIENT_TIMEOUT, in_node(node, call)).await {
            Err(_) => Outcome::Unknown("client timeout".into()),
            Ok(Err(error)) => group_outcome(error),
            Ok(Ok(value)) => match serde_json::from_value::<Mutation>(value) {
                Ok(Ok(reply)) => Outcome::Ok(reply),
                Ok(Err(error)) => Outcome::Fail(format!("{:?}: {}", error.code, error.message)),
                Err(error) => Outcome::Unknown(format!("undecodable reply: {error}")),
            },
        }
    }

    /// Publishes `batch` once. Returns whether the outcome is known.
    async fn publish(&mut self, batch: &[(Uuid, u64)]) -> Outcome<()> {
        let request = batch[0].0.to_string();
        let node = self.target();
        self.record(
            "invoke",
            "publish",
            json!(batch.iter().map(|(id, _)| id).collect::<Vec<_>>()),
            &request,
            node,
        );
        let now_ms = wall_ms_of(node).await;
        let events = batch
            .iter()
            .map(|(id, n)| PublishEvent {
                event_id: *id,
                data: json!({ "n": n }),
            })
            .collect();
        let outcome = self
            .write(node, Command::Publish { now_ms, events }, true)
            .await;
        match outcome {
            Outcome::Ok(Reply::Published(published)) => {
                self.record(
                    "ok",
                    "publish",
                    json!({
                        "event_ids": published.events.iter().map(|event| event.event_id).collect::<Vec<_>>(),
                        "offsets": published.events.iter().map(|event| event.offset).collect::<Vec<_>>(),
                        "dedup": published.inserted == 0,
                    }),
                    &request,
                    node,
                );
                Outcome::Ok(())
            }
            Outcome::Ok(other) => {
                self.record(
                    "info",
                    "publish",
                    json!(format!("{other:?}")),
                    &request,
                    node,
                );
                Outcome::Unknown("unexpected reply".into())
            }
            Outcome::Fail(error) => {
                self.record("fail", "publish", json!(error), &request, node);
                Outcome::Fail(error)
            }
            Outcome::Unknown(error) => {
                self.record("info", "publish", json!(error), &request, node);
                Outcome::Unknown(error)
            }
        }
    }

    /// Claim when needed, poll once, and acknowledge what the poll delivered.
    async fn consume_once(&mut self) -> Step {
        let node = self.target();
        let Some(generation) = self.generation else {
            self.ops += 1;
            let request = format!("c-{}", self.ops);
            self.record("invoke", "claim", Value::Null, &request, node);
            let claim = Command::Claim {
                name: SUBSCRIPTION.into(),
                owner: OWNER.into(),
                claim_id: self.uuid(),
                now_ms: wall_ms_of(node).await,
                ttl_ms: LEASE_TTL_MS,
            };
            return match self.write(node, claim, false).await {
                Outcome::Ok(Reply::Lease(lease)) => {
                    self.record("ok", "claim", json!(lease.generation), &request, node);
                    self.generation = Some(lease.generation);
                    Step::Progress
                }
                Outcome::Ok(other) => {
                    self.record("info", "claim", json!(format!("{other:?}")), &request, node);
                    Step::Retry
                }
                Outcome::Fail(error) => {
                    self.record("fail", "claim", json!(error), &request, node);
                    Step::Retry
                }
                Outcome::Unknown(error) => {
                    self.record("info", "claim", json!(error), &request, node);
                    Step::Retry
                }
            };
        };

        self.ops += 1;
        let request = format!("c-{}", self.ops);
        self.record(
            "invoke",
            "poll",
            json!({ "generation": generation }),
            &request,
            node,
        );
        let quorum_before = has_quorum(&self.sim, node, self.nodes);
        let epoch_before = self.sim.lock().net.epoch();
        let Some(group) = self.sim.broker(node).and_then(|broker| broker.group(GROUP)) else {
            self.record("fail", "poll", json!("broker unavailable"), &request, node);
            return Step::Retry;
        };
        let query = Query::Poll {
            name: SUBSCRIPTION.into(),
            owner: OWNER.into(),
            generation,
            now_ms: wall_ms_of(node).await,
            limit: 10,
        };
        let read = in_node(node, async move {
            group.read(move |tx| partition::query(tx, query)).await
        });
        let poll = match nabedra_rt::time::timeout(CLIENT_TIMEOUT, read).await {
            Err(_) => {
                self.record("info", "poll", json!("client timeout"), &request, node);
                return Step::Retry;
            }
            Ok(Err(error)) => {
                let detail = format!("{error}");
                self.record("info", "poll", json!(detail), &request, node);
                return Step::Retry;
            }
            Ok(Ok(Err(error))) => {
                if matches!(error.code, ErrorCode::LeaseLost | ErrorCode::LeaseBusy) {
                    self.generation = None;
                }
                self.record(
                    "fail",
                    "poll",
                    json!(format!("{:?}", error.code)),
                    &request,
                    node,
                );
                return Step::Retry;
            }
            Ok(Ok(Ok(QueryReply::Poll(poll)))) => poll,
            Ok(Ok(Ok(_))) => {
                self.record("info", "poll", json!("unexpected reply"), &request, node);
                return Step::Retry;
            }
        };
        let events: Vec<Value> = poll
            .events
            .iter()
            .map(|event| json!([event.offset, event.event_id.to_string()]))
            .collect();
        self.record(
            "ok",
            "poll",
            json!({ "after": poll.after, "events": events }),
            &request,
            node,
        );
        let epoch_after = self.sim.lock().net.epoch();
        if !quorum_before && epoch_before == epoch_after && !has_quorum(&self.sim, node, self.nodes)
        {
            let now = self.sim.elapsed_ms();
            self.sim.lock().observer.violate(
                now,
                format!("EVT-G06 poll {request} on n{node} answered without a reachable quorum"),
            );
        }
        let Some(last) = poll.last.filter(|_| !poll.events.is_empty()) else {
            return Step::Drained(poll.after);
        };

        self.ops += 1;
        let request = format!("c-{}", self.ops);
        self.record(
            "invoke",
            "ack",
            json!({ "after": poll.after, "through": last }),
            &request,
            node,
        );
        let ack = Command::Ack {
            name: SUBSCRIPTION.into(),
            owner: OWNER.into(),
            generation,
            after: poll.after,
            last,
            through: last,
            now_ms: wall_ms_of(node).await,
        };
        match self.write(node, ack, false).await {
            Outcome::Ok(Reply::Checkpoint { offset }) => {
                self.record("ok", "ack", json!({ "checkpoint": offset }), &request, node);
                Step::Progress
            }
            Outcome::Ok(other) => {
                self.record("info", "ack", json!(format!("{other:?}")), &request, node);
                Step::Retry
            }
            Outcome::Fail(error) => {
                if error.starts_with("LeaseLost") || error.starts_with("LeaseBusy") {
                    self.generation = None;
                }
                self.record("fail", "ack", json!(error), &request, node);
                Step::Retry
            }
            Outcome::Unknown(error) => {
                self.record("info", "ack", json!(error), &request, node);
                Step::Retry
            }
        }
    }
}

/// Every Group error is an unknown outcome to the client: a "not leader" reply can come
/// after the command was appended, and a timeout says nothing.
fn group_outcome<T>(error: GroupError) -> Outcome<T> {
    Outcome::Unknown(error.to_string())
}

async fn producer(sim: Arc<Harness>, stop: Arc<AtomicBool>, seed: u64, nodes: u64) {
    let mut client = Client::new(sim, PRODUCER, seed, nodes);
    let mut acknowledged: Vec<Vec<(Uuid, u64)>> = Vec::new();
    let mut next = 0u64;
    while !stop.load(Ordering::Acquire) {
        let batch = if !acknowledged.is_empty() && client.rng.chance(150) {
            let pick = client.rng.below(acknowledged.len() as u64) as usize;
            acknowledged[pick].clone()
        } else {
            let size = client.rng.range(1, 3);
            (0..size)
                .map(|_| {
                    next += 1;
                    (client.uuid(), next)
                })
                .collect()
        };
        for attempt in 0..4u64 {
            if stop.load(Ordering::Acquire) {
                return;
            }
            match client.publish(&batch).await {
                Outcome::Ok(()) => {
                    acknowledged.push(batch.clone());
                    break;
                }
                Outcome::Fail(_) => break,
                Outcome::Unknown(_) => {
                    let backoff = client.rng.range(20, 150) * (attempt + 1);
                    sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
        let pause = client.rng.range(10, 120);
        sleep(Duration::from_millis(pause)).await;
    }
}

async fn consumer(sim: Arc<Harness>, stop: Arc<AtomicBool>, seed: u64, nodes: u64) {
    let mut client = Client::new(sim, CONSUMER, seed, nodes);
    while !stop.load(Ordering::Acquire) {
        let pause = match client.consume_once().await {
            Step::Progress => client.rng.range(5, 40),
            Step::Retry => client.rng.range(50, 300),
            Step::Drained(_) => client.rng.range(30, 150),
        };
        sleep(Duration::from_millis(pause)).await;
    }
}

#[cfg(test)]
mod rpc_delivery_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use nabedra_sim::Filters;

    #[test]
    fn duplicate_respects_a_partition_opened_by_the_first_delivery() {
        nabedra_rt::sim::run(1, |world| async move {
            let config = SimConfig::new(1);
            let sim = Harness::new(world.clone(), config.knobs, config.nodes);
            let _installed = Installed::new(sim.clone());
            for id in [1, 2] {
                world.add_node(id, NodeClock::default());
                world.crash(id);
            }
            let mut cluster = Cluster::new(sim.clone(), config);
            cluster.start(1).await.unwrap();
            cluster.start(2).await.unwrap();
            sim.lock().net.set_filters(Filters {
                dup_per_mille: 1000,
                ..Filters::default()
            });
            let calls = Arc::new(AtomicUsize::new(0));
            let reply = in_node(
                1,
                super::super::net::call(2, "test", Duration::from_millis(100), {
                    let calls = calls.clone();
                    let sim = sim.clone();
                    move |_| {
                        if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                            sim.lock().net.block(1, 2);
                        }
                        async { Some(()) }
                    }
                }),
            )
            .await;
            assert_eq!(reply, Some(()));
            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            cluster.stop_all();
        });
    }

    #[test]
    fn reply_respects_a_partition_before_the_receiver_resumes() {
        nabedra_rt::sim::run(1, |world| async move {
            let config = SimConfig::new(1);
            let sim = Harness::new(world.clone(), config.knobs, config.nodes);
            let _installed = Installed::new(sim.clone());
            for id in [1, 2] {
                world.add_node(id, NodeClock::default());
                world.crash(id);
            }
            let mut cluster = Cluster::new(sim.clone(), config);
            cluster.start(1).await.unwrap();
            cluster.start(2).await.unwrap();
            let reply = in_node(
                1,
                super::super::net::call(2, "test", Duration::from_millis(100), {
                    let sim = sim.clone();
                    move |_| {
                        sim.lock().net.pause(1);
                        let sim = sim.clone();
                        sim.world().clone().spawn_node(2, async move {
                            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
                            let mut state = sim.lock();
                            state.net.block(2, 1);
                            state.net.resume(1);
                        });
                        async { Some(()) }
                    }
                }),
            )
            .await;
            assert_eq!(reply, None);
            cluster.stop_all();
        });
    }
}
