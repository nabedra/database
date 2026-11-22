//! The cluster harness: N brokers on one paused runtime, the nemesis, the workload, the
//! Kafka-style invariants after every step, and the end-of-run consistency checks.

// `tokio_unstable` is set through RUSTFLAGS for simulation builds only.
#![allow(unexpected_cfgs)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use nabedra_kv::fs::FaultFs;
use openraft::ServerState;
use serde_json::{Value, json};
use uuid::Uuid;

use super::history::{self, Event, FinalLog};
use super::{Installed, SplitMix64, World, in_node, wall_ms_of};
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
const PRODUCER: u64 = 0;
const CONSUMER: u64 = 1;
const DRAINER: u64 = 2;

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

/// Invariants checked after every simulation step, after Kafka's
/// `RaftEventSimulationTest`: `MonotonicHighWatermark`, `MonotonicEpoch`,
/// `SingleLeader`, `MajorityReachedHighWatermark`, and agreeing applied prefixes.
#[derive(Default)]
pub(crate) struct Observer {
    voters: u64,
    /// index -> (term, payload hash) of every entry any replica applied.
    committed: BTreeMap<u64, (u64, u64)>,
    /// Each replica's durable log, from the append and truncate hooks; rebuilt from its
    /// volume on every start.
    logs: BTreeMap<u64, BTreeMap<u64, u64>>,
    /// The highest term each replica has reported, across restarts (the vote is durable).
    max_term: BTreeMap<u64, u64>,
    leaders: BTreeMap<u64, u64>,
    /// The applied index each replica reported in its current incarnation.
    applied_hook: BTreeMap<u64, u64>,
    applied_metric: BTreeMap<u64, u64>,
    pub violations: Vec<String>,
    /// Messages already reported, so a violation that persists is reported once.
    reported: std::collections::BTreeSet<String>,
}

impl Observer {
    pub fn new(voters: u64) -> Self {
        Self {
            voters,
            ..Self::default()
        }
    }

    pub fn violate(&mut self, now_ms: u64, message: String) {
        if self.violations.len() < 32 && self.reported.insert(message.clone()) {
            self.violations.push(format!("t={now_ms}ms {message}"));
        }
    }

    pub fn applied(&mut self, node: u64, index: u64, term: u64, hash: u64, now_ms: u64) {
        match self.committed.get(&index) {
            Some(&(t, h)) if (t, h) != (term, hash) => self.violate(
                now_ms,
                format!(
                    "AppliedPrefixesAgree: n{node} applied {term}:{index} ({hash:x}), another replica applied {t}:{index} ({h:x})"
                ),
            ),
            Some(_) => {}
            None => {
                self.committed.insert(index, (term, hash));
                self.check_entry_majority(index, term, now_ms);
            }
        }
        if let Some(previous) = self
            .applied_hook
            .insert(node, index)
            .filter(|prior| index <= *prior)
        {
            self.violate(
                now_ms,
                format!("MonotonicCommit: n{node} applied index {index} after {previous} in one incarnation"),
            );
        }
    }

    pub fn appended(&mut self, node: u64, index: u64, term: u64) {
        self.logs.entry(node).or_default().insert(index, term);
    }

    pub fn truncated(&mut self, node: u64, from: u64) {
        self.logs.entry(node).or_default().split_off(&from);
    }

    pub fn restarted(&mut self, node: u64, log: BTreeMap<u64, u64>) {
        self.logs.insert(node, log);
        self.applied_hook.remove(&node);
        self.applied_metric.remove(&node);
    }

    fn observe(&mut self, node: u64, term: u64, leader: bool, applied: Option<u64>, now_ms: u64) {
        let max = self.max_term.entry(node).or_default();
        if term < *max {
            let previous = *max;
            self.violate(
                now_ms,
                format!("MonotonicEpoch: n{node} reports term {term} after term {previous}"),
            );
        } else {
            *max = term;
        }
        if leader {
            match self.leaders.get(&term) {
                Some(other) if *other != node => {
                    let other = *other;
                    self.violate(
                        now_ms,
                        format!("SingleLeader: n{node} and n{other} both led term {term}"),
                    );
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(term, node);
                }
            }
        }
        if let Some(applied) = applied {
            if let Some(previous) = self
                .applied_metric
                .insert(node, applied)
                .filter(|prior| applied < *prior)
            {
                self.violate(
                    now_ms,
                    format!("MonotonicHighWatermark: n{node} applied index fell from {previous} to {applied}"),
                );
            }
        }
    }

    fn check_majority(&mut self, now_ms: u64) {
        let Some((&index, &(term, _))) = self.committed.iter().next_back() else {
            return;
        };
        self.check_entry_majority(index, term, now_ms);
    }

    fn check_entry_majority(&mut self, index: u64, term: u64, now_ms: u64) {
        let holders = self
            .logs
            .values()
            .filter(|log| log.get(&index) == Some(&term))
            .count() as u64;
        if holders <= self.voters / 2 {
            let voters = self.voters;
            self.violate(
                now_ms,
                format!(
                    "MajorityReachedHighWatermark: committed entry {term}:{index} is held by {holders} of {voters} replicas"
                ),
            );
        }
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
    world: Arc<World>,
    config: SimConfig,
    volumes: BTreeMap<u64, Volume>,
    cluster_id: Uuid,
    topic_id: Uuid,
    rng: SplitMix64,
    stats: Stats,
    resume_at: BTreeMap<u64, u64>,
    restart_at: BTreeMap<u64, u64>,
}

/// Runs one seed to completion.
pub fn run(config: SimConfig) -> Report {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    // `select!` branch order comes from the runtime's rng; seed it (tokio_unstable).
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(
        &config.seed.to_le_bytes(),
    ));
    let runtime = builder.build().expect("build the simulation runtime");
    let world =
        runtime.block_on(async { Arc::new(World::new(config.seed, config.knobs, config.nodes)) });
    let installed = Installed::new(world.clone());
    let seed = config.seed;
    let (mut violations, stats) = runtime.block_on(async {
        let mut cluster = Cluster::new(world.clone(), config);
        let result = cluster.run().await;
        let mut violations = Vec::new();
        if let Err(error) = result {
            violations.push(format!("harness: {error:#}"));
        }
        cluster.stop_all();
        (violations, cluster.stats.clone())
    });
    drop(runtime);
    drop(installed);
    let mut state = world.lock();
    let mut all = std::mem::take(&mut state.observer.violations);
    all.append(&mut violations);
    Report {
        seed,
        trace_hash: state.trace.finish(),
        trace_events: state.trace_events,
        violations: all,
        history: std::mem::take(&mut state.history.events),
        trace_tail: state.trace_tail.iter().cloned().collect(),
        stats,
    }
}

impl Cluster {
    fn new(world: Arc<World>, config: SimConfig) -> Self {
        let mut rng = SplitMix64::new(config.seed ^ 0x6e65_6d65_7369_7300);
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
        let ids = |rng: &mut SplitMix64| {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&rng.next_u64().to_le_bytes());
            bytes[8..].copy_from_slice(&rng.next_u64().to_le_bytes());
            uuid::Builder::from_random_bytes(bytes).into_uuid()
        };
        Self {
            cluster_id: ids(&mut rng),
            topic_id: ids(&mut rng),
            world,
            config,
            volumes,
            rng,
            stats: Stats::default(),
            resume_at: BTreeMap::new(),
            restart_at: BTreeMap::new(),
        }
    }

    fn now(&self) -> u64 {
        self.world.elapsed_ms()
    }

    fn trace(&self, line: std::fmt::Arguments<'_>) {
        self.world.trace(line);
    }

    fn violate(&self, message: String) {
        let now = self.now();
        self.world.lock().observer.violate(now, message);
    }

    async fn start(&mut self, id: u64) -> Result<()> {
        let volume = self.volumes.get_mut(&id).expect("volume");
        volume.at_start = volume.fs.crash();
        let broker = Broker::sim_open(id, self.cluster_id, volume.root.path(), volume.fs.clone())
            .with_context(|| format!("open broker n{id} from its volume"))?;
        {
            let mut state = self.world.lock();
            let node = state.nodes.get_mut(&id).expect("node");
            node.up = true;
            node.paused = false;
            node.broker = Some(broker.clone());
            state.net.epoch += 1;
        }
        let group = in_node(id, broker.open_group(GROUP, Arc::new(Machine)))
            .await
            .map_err(|error| anyhow!("open group on n{id}: {error}"))?;
        let log = in_node(id, group.sim_log_ids()).await?;
        let log = log
            .into_iter()
            .map(|id| (id.index, id.leader_id.term))
            .collect();
        self.world.lock().observer.restarted(id, log);
        self.trace(format_args!("start n{id}"));
        Ok(())
    }

    /// Cuts the node's network and disk, then aborts its tasks.
    fn crash(&mut self, id: u64) {
        let (broker, tasks) = {
            let mut state = self.world.lock();
            state.net.epoch += 1;
            let node = state.nodes.get_mut(&id).expect("node");
            node.up = false;
            node.paused = false;
            (node.broker.take(), std::mem::take(&mut node.tasks))
        };
        let volume = self.volumes.get_mut(&id).expect("volume");
        volume.fs.halt();
        for task in tasks {
            task.abort();
        }
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
            if self.world.is_up(id) {
                self.crash(id);
            }
        }
    }

    fn group(&self, id: u64) -> Option<Arc<Group>> {
        self.world.broker(id)?.group(GROUP)
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
                metrics.current_term,
                metrics.state == ServerState::Leader,
                metrics.last_applied.map(|log| log.index),
            ));
        }
        let mut state = self.world.lock();
        for (id, term, leader, applied) in seen {
            state.observer.observe(id, term, leader, applied, now);
        }
        state.observer.check_majority(now);
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
        let producer = tokio::spawn(producer(self.world.clone(), stop.clone(), seed));
        let consumer = tokio::spawn(consumer(self.world.clone(), stop.clone(), seed));

        let chaos_end = self.now() + self.config.chaos_ms;
        let mut next_action = self.now() + self.rng.range(200, 800);
        while self.now() < chaos_end {
            tokio::time::sleep(TICK).await;
            self.observe();
            self.scheduled().await?;
            if self.now() >= next_action {
                self.nemesis().await?;
                next_action = self.now() + self.rng.range(150, 1_200);
            }
        }

        stop.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(20), producer)
            .await
            .context("producer did not stop")??;
        tokio::time::timeout(Duration::from_secs(20), consumer)
            .await
            .context("consumer did not stop")??;
        self.heal_all().await?;
        self.converge().await;
        let checkpoint = self.drain().await;
        self.converge().await;
        self.observe();
        self.final_checks(checkpoint).await;
        let state = self.world.lock();
        self.stats.messages = state.messages;
        self.stats.committed_entries = state.observer.committed.len();
        self.stats.history_events = state.history.events.len();
        self.stats.virtual_ms = self.world.elapsed_ms();
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
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.observe();
                let Some((id, group)) = leader(&self.world, self.config.nodes) else {
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
        let mut state = self.world.lock();
        state.net.epoch += 1;
        if let Some(node) = state.nodes.get_mut(&id) {
            node.paused = paused;
        }
        drop(state);
        self.trace(format_args!(
            "{} n{id}",
            if paused { "pause" } else { "resume" }
        ));
    }

    fn down(&self) -> Vec<u64> {
        (1..=self.config.nodes)
            .filter(|id| !self.world.is_up(*id))
            .collect()
    }

    async fn nemesis(&mut self) -> Result<()> {
        let nodes = self.config.nodes;
        let pick = self.rng.range(1, nodes);
        let now = self.now();
        match self.rng.below(100) {
            // Crash a node while at most a minority is down; it restarts later.
            0..=17 => {
                if self.world.is_up(pick) && (self.down().len() as u64) < nodes - self.majority() {
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
                    leader(&self.world, nodes).map_or(pick, |(id, _)| id)
                } else {
                    pick
                };
                let mut state = self.world.lock();
                state.net.epoch += 1;
                for other in (1..=nodes).filter(|other| *other != target) {
                    state.net.blocked.insert((target, other));
                    state.net.blocked.insert((other, target));
                }
                drop(state);
                self.stats.partitions += 1;
                self.trace(format_args!("isolate n{target}"));
            }
            // A one-way link failure.
            38..=45 => {
                let to = self.rng.range(1, nodes);
                if to != pick {
                    let mut state = self.world.lock();
                    state.net.epoch += 1;
                    state.net.blocked.insert((pick, to));
                    drop(state);
                    self.stats.partitions += 1;
                    self.trace(format_args!("block n{pick}->n{to}"));
                }
            }
            46..=65 => {
                let mut state = self.world.lock();
                state.net.epoch += 1;
                state.net.blocked.clear();
                drop(state);
                self.trace(format_args!("heal"));
            }
            66..=75 => {
                if self.world.is_up(pick) && !self.world.is_paused(pick) {
                    self.set_paused(pick, true);
                    let resume = now + self.rng.range(100, 2_500);
                    self.resume_at.insert(pick, resume);
                    self.stats.pauses += 1;
                }
            }
            76..=85 => {
                let wall = self.rng.range(0, 4_000) as i64 - 1_000;
                let mono = Duration::from_millis(self.rng.range(0, 1_500));
                let mut state = self.world.lock();
                if let Some(node) = state.nodes.get_mut(&pick) {
                    node.wall_offset_ms += wall;
                    node.mono_offset += mono;
                }
                drop(state);
                self.stats.clock_jumps += 1;
                self.trace(format_args!(
                    "clock n{pick} wall{wall:+}ms mono+{}ms",
                    mono.as_millis()
                ));
            }
            _ => {
                let drop_per_mille = [0, 0, 10, 50, 150][self.rng.below(5) as usize];
                let dup_per_mille = [0, 10, 50][self.rng.below(3) as usize];
                let delay_max_ms = [0, 5, 30, 120][self.rng.below(4) as usize];
                let mut state = self.world.lock();
                state.net.drop_per_mille = drop_per_mille;
                state.net.dup_per_mille = dup_per_mille;
                state.net.delay_max_ms = delay_max_ms;
                drop(state);
                self.trace(format_args!(
                    "filters drop={drop_per_mille} dup={dup_per_mille} delay<={delay_max_ms}ms"
                ));
            }
        }
        Ok(())
    }

    async fn heal_all(&mut self) -> Result<()> {
        {
            let mut state = self.world.lock();
            state.net.epoch += 1;
            state.net.blocked.clear();
            state.net.drop_per_mille = 0;
            state.net.dup_per_mille = 0;
            state.net.delay_max_ms = 0;
            for node in state.nodes.values_mut() {
                node.paused = false;
            }
        }
        self.resume_at.clear();
        self.restart_at.clear();
        for id in self.down() {
            self.start(id).await?;
        }
        self.trace(format_args!("heal all"));
        Ok(())
    }

    /// Waits until every replica applied the same entries and a leader exists.
    async fn converge(&mut self) {
        let deadline = self.now() + 30_000;
        let mut stable = 0;
        while self.now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        tokio::time::sleep(Duration::from_millis(LEASE_TTL_MS as u64 + 200)).await;
        let mut client = Client::new(self.world.clone(), DRAINER, self.config.seed ^ 0xd7a1);
        let mut checkpoint = None;
        let deadline = self.now() + 30_000;
        while self.now() < deadline {
            match client.consume_once().await {
                Step::Drained(at) => {
                    checkpoint = Some(at);
                    break;
                }
                Step::Progress => {}
                Step::Retry => tokio::time::sleep(Duration::from_millis(100)).await,
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
            let state = self.world.lock();
            history::check(&state.history, &log, reference.checkpoint)
        };
        for anomaly in anomalies {
            self.violate(anomaly);
        }
    }
}

/// The node that reports itself leader, if any (lowest id first).
fn leader(world: &World, nodes: u64) -> Option<(u64, Arc<Group>)> {
    (1..=nodes).find_map(|id| {
        let group = world.broker(id)?.group(GROUP)?;
        (group.leader() == Some(id)).then_some((id, group))
    })
}

/// Can `node` reach a majority (itself included) over links in both directions?
fn has_quorum(world: &World, node: u64, nodes: u64) -> bool {
    let state = world.lock();
    let usable = |id: u64| state.nodes.get(&id).is_some_and(|n| n.up && !n.paused);
    if !usable(node) {
        return false;
    }
    let reachable = (1..=nodes)
        .filter(|other| {
            *other == node
                || (usable(*other)
                    && !state.net.blocked.contains(&(node, *other))
                    && !state.net.blocked.contains(&(*other, node)))
        })
        .count() as u64;
    reachable > nodes / 2
}

enum Step {
    Progress,
    Retry,
    Drained(u64),
}

/// One workload client: it talks to a broker through the in-process Group API, the way
/// the HTTP layer does, and records each operation in the history.
struct Client {
    world: Arc<World>,
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
    fn new(world: Arc<World>, process: u64, seed: u64) -> Self {
        let nodes = world.lock().nodes.len() as u64;
        Self {
            world,
            process,
            rng: SplitMix64::new(seed ^ (process + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            ops: 0,
            nodes,
            generation: None,
        }
    }

    fn uuid(&mut self) -> Uuid {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.rng.next_u64().to_le_bytes());
        bytes[8..].copy_from_slice(&self.rng.next_u64().to_le_bytes());
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }

    /// The node that claims leadership, or a random running node (which may redirect).
    fn target(&mut self) -> u64 {
        if !self.rng.chance(100) {
            if let Some((id, _)) = leader(&self.world, self.nodes) {
                return id;
            }
        }
        self.rng.range(1, self.nodes)
    }

    fn record(&self, kind: &'static str, f: &'static str, value: Value, request: &str, node: u64) {
        let now = self.world.elapsed_ms();
        self.world.trace(format_args!(
            "history {kind} p{} {f} {request} n{node} {value}",
            self.process
        ));
        self.world
            .lock()
            .history
            .push(kind, self.process, f, value, request, node, now);
    }

    /// Sends one Raft command to `node` and returns its domain outcome.
    async fn write(&mut self, node: u64, command: Command, coalesced: bool) -> Outcome<Reply> {
        let Some(group) = self
            .world
            .broker(node)
            .and_then(|broker| broker.group(GROUP))
        else {
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
        match tokio::time::timeout(CLIENT_TIMEOUT, in_node(node, call)).await {
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
        self.ops += 1;
        let request = format!("p-{}", self.ops);
        let node = self.target();
        self.record(
            "invoke",
            "publish",
            history::publish_value(batch),
            &request,
            node,
        );
        let now_ms = wall_ms_of(&self.world, node);
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
                let pairs: Vec<(Uuid, u64)> = published
                    .events
                    .iter()
                    .map(|event| (event.event_id, event.offset))
                    .collect();
                self.record(
                    "ok",
                    "publish",
                    history::publish_value(&pairs),
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
                now_ms: wall_ms_of(&self.world, node),
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
        let quorum_before = has_quorum(&self.world, node, self.nodes);
        let epoch_before = self.world.lock().net.epoch;
        let Some(group) = self
            .world
            .broker(node)
            .and_then(|broker| broker.group(GROUP))
        else {
            self.record("fail", "poll", json!("broker unavailable"), &request, node);
            return Step::Retry;
        };
        let query = Query::Poll {
            name: SUBSCRIPTION.into(),
            owner: OWNER.into(),
            generation,
            now_ms: wall_ms_of(&self.world, node),
            limit: 10,
        };
        let read = in_node(node, async move {
            group.read(move |tx| partition::query(tx, query)).await
        });
        let poll = match tokio::time::timeout(CLIENT_TIMEOUT, read).await {
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
        let epoch_after = self.world.lock().net.epoch;
        if !quorum_before
            && epoch_before == epoch_after
            && !has_quorum(&self.world, node, self.nodes)
        {
            let now = self.world.elapsed_ms();
            self.world.lock().observer.violate(
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
            now_ms: wall_ms_of(&self.world, node),
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

async fn producer(world: Arc<World>, stop: Arc<AtomicBool>, seed: u64) {
    let mut client = Client::new(world, PRODUCER, seed);
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
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
        let pause = client.rng.range(10, 120);
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
}

async fn consumer(world: Arc<World>, stop: Arc<AtomicBool>, seed: u64) {
    let mut client = Client::new(world, CONSUMER, seed);
    while !stop.load(Ordering::Acquire) {
        let pause = match client.consume_once().await {
            Step::Progress => client.rng.range(5, 40),
            Step::Retry => client.rng.range(50, 300),
            Step::Drained(_) => client.rng.range(30, 150),
        };
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
}

#[cfg(test)]
impl Observer {
    pub fn observe_for_test(&mut self, node: u64, term: u64, leader: bool, applied: Option<u64>) {
        self.observe(node, term, leader, applied, 0);
    }
}
