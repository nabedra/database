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
use crate::raft::{Broker, Group, GroupError, Rejoin, rejoin};

pub const GROUP: &str = "p0";
const SUBSCRIPTION: &str = "s";
const OWNER: &str = "c1";
const LEASE_TTL_MS: i64 = 4_000;
const TICK: Duration = Duration::from_millis(10);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_TTL: Duration = Duration::from_millis(450);
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
    /// A broker restarted on an empty volume reopens the group as a voter under its old
    /// membership, as `Control::reopen` did for the metadata group before EVENTS-1.
    pub rejoin_wiped_voter: bool,
    /// The leader stamps time and retention cutoffs from its own wall clock, as before
    /// EVENTS-2, instead of the guarded interval of its voters' clocks.
    pub unguarded_clock: bool,
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
    /// Run the retention loop with this retention period (EVENTS-2, EVENTS-3, EVENTS-8):
    /// guarded time stamps, the EVT-G08 early-removal check, at most one clock beyond the
    /// stated bound at a time, and consumers that reset after `checkpoint_expired`.
    pub retention_ms: Option<i64>,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            nodes: 3,
            chaos_ms: 12_000,
            knobs: Knobs::default(),
            wipes: false,
            retention_ms: None,
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
    /// Nodes the empty-volume rule kept out of the group after a wipe (EVENTS-1).
    pub lost_replicas: u64,
    /// Events retention removed, and acks that advanced past a retention gap (EVENTS-8).
    pub retained_removed: u64,
    /// Clock jumps cut back to the bound, so at most one clock is wrong at once.
    pub clock_steps: u64,
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
    /// Nodes that started on a volume without the group and wait for the empty-volume
    /// rule (EVENTS-1) to decide.
    joining: BTreeSet<u64>,
    /// Every node started once and the group was initialized.
    genesis_done: bool,
    /// Each node's wall-clock offset from true time, from the nemesis's jumps.
    clock_offsets: BTreeMap<u64, i64>,
    /// When each node's clock was last outside the stated bound.
    last_bad: BTreeMap<u64, u64>,
}

/// Runs one seed to completion.
pub fn run(config: SimConfig) -> Report {
    let seed = config.seed;
    #[cfg(test)]
    let retention = config.retention_ms.is_some();
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
    // Retention removes unconsumed events by design, which the queue checker would call
    // lost; the retention workload checks them against EVT-G08 instead.
    #[cfg(test)]
    if !retention {
        match state.history.check_queue(REDELIVERY_BOUND) {
            Ok(violations) => all.extend(violations),
            Err(error) => all.push(format!(
                "queue checker could not read seed history: {error}"
            )),
        }
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
        sim.lock().guarded_time = !config.knobs.unguarded_clock;
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
            joining: BTreeSet::new(),
            genesis_done: false,
            clock_offsets: BTreeMap::new(),
            last_bad: BTreeMap::new(),
        }
    }

    fn lost(&self) -> BTreeSet<u64> {
        self.sim.lock().lost.clone()
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
            state.lost.remove(&id);
        }
        // The product opens only the groups a volume holds (`Control::reopen`). After the
        // first start, a volume without the group waits for the empty-volume rule, unless
        // the planted bug reopens it as a voter the way the metadata group was reopened
        // before EVENTS-1. (The first start of a new cluster opens the group directly;
        // `control::tests` cover the rule's genesis case.)
        let stored = broker.stored_group_ids()?.iter().any(|group| group == GROUP);
        if stored || !self.genesis_done || self.config.knobs.rejoin_wiped_voter {
            self.open_replica(id, &broker).await
        } else {
            self.joining.insert(id);
            self.trace(format_args!("start n{id} on a volume without {GROUP}"));
            Ok(())
        }
    }

    async fn open_replica(&mut self, id: u64, broker: &Arc<Broker>) -> Result<()> {
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

    /// The empty-volume rule (EVENTS-1, `raft::rejoin`) for each node that started on a
    /// volume without the group: it asks every other node over the simulated network what
    /// its volume holds. In a new cluster it opens the group; once any peer holds the
    /// group the node lost its replica and stays out of it, so it never votes under its
    /// old membership. Node 1 is the initializer, as in `run`.
    async fn join_pending(&mut self) -> Result<()> {
        let nodes: BTreeSet<u64> = (1..=self.config.nodes).collect();
        for id in self.joining.clone() {
            let Some(broker) = self.sim.broker(id) else {
                // Down again; its next start re-enters the rule.
                self.joining.remove(&id);
                continue;
            };
            let mut answers = BTreeMap::new();
            for peer in nodes.iter().copied().filter(|peer| *peer != id) {
                let probe = super::net::call(peer, "disk-state", PROBE_TTL, |broker| async move {
                    broker.disk_state(GROUP).ok()
                });
                answers.insert(peer, in_node(id, probe).await);
            }
            match rejoin(id == 1, &nodes, &answers) {
                Rejoin::Wait => {}
                Rejoin::Lost => {
                    self.joining.remove(&id);
                    self.sim.lock().lost.insert(id);
                    self.stats.lost_replicas += 1;
                    self.trace(format_args!("n{id} lost its {GROUP} replica"));
                }
                Rejoin::Genesis => {
                    self.joining.remove(&id);
                    self.open_replica(id, &broker).await?;
                }
            }
        }
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
        self.genesis_done = true;
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
        let retention = self.config.retention_ms.map(|retention_ms| {
            nabedra_rt::task::spawn(retention(
                self.sim.clone(),
                stop.clone(),
                nodes,
                retention_ms,
                self.config.knobs.unguarded_clock,
            ))
        });

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
        if let Some(retention) = retention {
            let removed = nabedra_rt::time::timeout(Duration::from_secs(20), retention)
                .await
                .context("retention did not stop")??;
            self.stats.retained_removed = removed?;
        }
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
        if !self.joining.is_empty() {
            self.join_pending().await?;
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
                    // One lost replica at a time: with a second, a majority has lost its
                    // disks, and the group stays down by design (see
                    // `majority_disk_loss_stays_unavailable`).
                    if self.config.wipes
                        && self.rng.chance(200)
                        && self.joining.is_empty()
                        && self.lost().is_empty()
                    {
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
                let before = *self.clock_offsets.entry(pick).or_default();
                *self.clock_offsets.entry(pick).or_default() += wall;
                if self.config.retention_ms.is_some() {
                    self.keep_one_clock_beyond_the_bound(pick, before);
                }
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

    /// The guarded clock tolerates one wrong clock among three voters (EVT-G08 holds
    /// under that bound), and a leader keeps a peer's sample for up to its age limit. So a
    /// second clock may leave the bound only once no other clock has been outside it for
    /// two sample ages; otherwise the jump of `pick` is cut back to the bound. A stale
    /// sample of a corrected clock would otherwise count as a second wrong clock.
    fn keep_one_clock_beyond_the_bound(&mut self, pick: u64, before: i64) {
        let policy = crate::raft::ClockPolicy::default();
        let bound = policy.max_clock_error_ms;
        let window = 2 * policy.sample_max_age_ms;
        let now = self.now();
        if before.abs() > bound {
            self.last_bad.insert(pick, now);
        }
        let offset = self.clock_offsets[&pick];
        if offset.abs() <= bound {
            return;
        }
        let other_bad = self.clock_offsets.iter().any(|(id, other)| {
            *id != pick
                && (other.abs() > bound
                    || self.last_bad.get(id).is_some_and(|at| at + window > now))
        });
        if !other_bad {
            self.last_bad.insert(pick, now);
            return;
        }
        let target = offset.signum() * bound;
        let by = Duration::from_millis((offset - target).unsigned_abs());
        if offset > target {
            self.world().jump_wall_backward(pick, by);
        } else {
            self.world().jump_wall_forward(pick, by);
        }
        self.clock_offsets.insert(pick, target);
        self.stats.clock_steps += 1;
        self.trace(format_args!("clock clamp n{pick} {:+}ms", target - offset));
    }

    async fn heal_all(&mut self) -> Result<()> {
        self.sim.lock().net.heal_all();
        self.resume_at.clear();
        self.restart_at.clear();
        for id in self.down() {
            self.start(id).await?;
        }
        self.join_pending().await?;
        self.record_heal();
        Ok(())
    }

    /// The nodes that can hold the group: every node but those whose volume lost it.
    fn replicas(&self) -> Vec<u64> {
        let lost = self.lost();
        (1..=self.config.nodes)
            .filter(|id| !lost.contains(id))
            .collect()
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
            if !self.joining.is_empty() {
                let _ = self.join_pending().await;
            }
            let views: Vec<_> = self
                .replicas()
                .into_iter()
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
        for id in self.replicas() {
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
        // Retention can remove every event: an empty log then ends below the floor.
        let last = log
            .last_key_value()
            .map_or(reference.earliest.saturating_sub(1), |(&offset, _)| offset);
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
            let removed: BTreeSet<Uuid> = state.retained.values().flatten().copied().collect();
            history::check(
                &state.history,
                &log,
                reference.checkpoint,
                reference.earliest,
                &removed,
            )
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
    let state = sim.lock();
    state.net.has_quorum(node, &members, |id| {
        sim.world().is_up(id) && !state.lost.contains(&id)
    })
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
    async fn write(&mut self, node: u64, mut command: Command, coalesced: bool) -> Outcome<Reply> {
        let Some(group) = self.sim.broker(node).and_then(|broker| broker.group(GROUP)) else {
            return Outcome::Fail("broker unavailable".into());
        };
        let guarded = self.sim.lock().guarded_time;
        if guarded {
            // The product leader's stamp (EVENTS-2, `control::forward`): nothing is
            // proposed when the interval cannot be established.
            match in_node(node, group.time_bounds()).await {
                Ok(bounds) => partition::stamp(&mut command, bounds),
                Err(error) => return Outcome::Fail(format!("Unavailable: clock: {error}")),
            }
        }
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
        let mut query = Query::Poll {
            name: SUBSCRIPTION.into(),
            owner: OWNER.into(),
            generation,
            now_ms: wall_ms_of(node).await,
            limit: 10,
        };
        let guarded = self.sim.lock().guarded_time;
        if guarded {
            match in_node(node, group.time_bounds()).await {
                Ok(bounds) => partition::stamp_query(&mut query, bounds),
                Err(error) => {
                    self.record("fail", "poll", json!(format!("clock: {error}")), &request, node);
                    return Step::Retry;
                }
            }
        }
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
                if error.code == ErrorCode::CheckpointExpired {
                    self.reset_expired(node).await;
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
                let expired = error.starts_with("CheckpointExpired");
                self.record("fail", "ack", json!(error), &request, node);
                if expired {
                    self.reset_expired(node).await;
                }
                Step::Retry
            }
            Outcome::Unknown(error) => {
                self.record("info", "ack", json!(error), &request, node);
                Step::Retry
            }
        }
    }
}

impl Client {
    /// Retention passed the checkpoint: the operator's `reset_subscription`, which moves
    /// the checkpoint to the retention floor and releases the lease.
    async fn reset_expired(&mut self, node: u64) {
        let reset = Command::ResetExpired {
            name: SUBSCRIPTION.into(),
        };
        let outcome = self.write(node, reset, false).await;
        self.generation = None;
        self.sim.trace(format_args!(
            "reset p{} n{node} {}",
            self.process,
            matches!(outcome, Outcome::Ok(_))
        ));
    }
}

/// The retention loop of `Control::retain` for the simulated group (EVENTS-2, EVENTS-3):
/// the leader trims events older than its cutoff, the guarded lower bound minus the
/// retention period (or its own clock with the planted `unguarded_clock`). After each
/// trim it checks EVT-G08 in true time: no acknowledged event may be removed before the
/// retention period has passed since its first publish attempt. Returns the events removed.
async fn retention(
    sim: Arc<Harness>,
    stop: Arc<AtomicBool>,
    nodes: u64,
    retention_ms: i64,
    unguarded: bool,
) -> Result<u64> {
    let mut earliest = 1u64;
    let mut removed_total = 0u64;
    while !stop.load(Ordering::Acquire) {
        sleep(Duration::from_millis(200)).await;
        let Some((id, group)) = leader(&sim, nodes) else {
            continue;
        };
        let trimmed = in_node(id, async move {
            let now_ms = if unguarded {
                crate::rt::unix_ms() as i64
            } else {
                group.time_bounds().await.ok()?.lower_ms
            };
            let deadline = crate::rt::now() + Duration::from_millis(100);
            Some(partition::trim(&group, now_ms.saturating_sub(retention_ms), deadline).await)
        })
        .await;
        if let Some((removed, Some(now_earliest))) = trimmed {
            removed_total += removed as u64;
            if now_earliest > earliest {
                check_retention(&sim, earliest, now_earliest, retention_ms);
                earliest = now_earliest;
            }
        }
    }
    Ok(removed_total)
}

/// EVT-G08 in true time: offsets `from..to` were removed by now; each acknowledged event
/// among them must have been published at least `retention_ms` ago. Its first publish
/// attempt is a lower bound of its publish time, so the check never blames a late event.
fn check_retention(sim: &Harness, from: u64, to: u64, retention_ms: i64) {
    let now = sim.elapsed_ms();
    let mut state = sim.lock();
    let mut first_invoke: BTreeMap<String, u64> = BTreeMap::new();
    let mut early = Vec::new();
    for event in &state.history.events {
        if event.f != "publish" {
            continue;
        }
        let at = event.time_ns / 1_000_000;
        match event.kind {
            "invoke" => {
                first_invoke.entry(event.request_id.clone()).or_insert(at);
            }
            "ok" => {
                let Some(&published) = first_invoke.get(&event.request_id) else {
                    continue;
                };
                let offsets = event.value["offsets"].as_array().cloned().unwrap_or_default();
                for offset in offsets.iter().filter_map(Value::as_u64) {
                    if (from..to).contains(&offset)
                        && (now as i64) < published as i64 + retention_ms
                    {
                        early.push((offset, published));
                    }
                }
            }
            _ => {}
        }
    }
    early.sort_unstable();
    early.dedup();
    for (offset, published) in early {
        state.observer.violate(
            now,
            format!(
                "EVT-G08 early-removal: offset {offset} was removed by t={now}ms, published no earlier than t={published}ms, retention {retention_ms} ms"
            ),
        );
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

/// EVENTS-1 scripted schedules: disk replacement and the loss of a majority of disks, with
/// the empty-volume rule and with the planted reopen that predates it.
#[cfg(test)]
mod wipe_tests {
    use super::*;

    /// Runs `script` on a new three-node cluster with the group initialized and the
    /// subscription open. Returns every violation the run recorded.
    fn scripted<F, Fut>(seed: u64, knobs: Knobs, script: F) -> Vec<String>
    where
        F: FnOnce(Cluster) -> Fut,
        Fut: std::future::Future<Output = Result<Cluster>>,
    {
        scripted_with(seed, |config| config.knobs = knobs, script)
    }

    fn scripted_with<C, F, Fut>(seed: u64, configure: C, script: F) -> Vec<String>
    where
        C: FnOnce(&mut SimConfig),
        F: FnOnce(Cluster) -> Fut,
        Fut: std::future::Future<Output = Result<Cluster>>,
    {
        let (sim, mut violations) = nabedra_rt::sim::run(seed, |world| async move {
            let mut config = SimConfig::new(seed);
            configure(&mut config);
            let sim = Harness::new(world.clone(), config.knobs, config.nodes);
            let _installed = Installed::new(sim.clone());
            for id in 1..=config.nodes {
                world.add_node(id, NodeClock::default());
                world.crash(id);
            }
            let mut cluster = Cluster::new(sim.clone(), config);
            let mut violations = Vec::new();
            let result = async {
                for id in 1..=3 {
                    cluster.start(id).await?;
                }
                cluster.genesis_done = true;
                let members: BTreeMap<u64, String> = (1..=3)
                    .map(|id| (id, format!("http://127.0.0.1:{}", 7000 + id)))
                    .collect();
                let first = cluster.group(1).context("group on n1")?;
                in_node(1, first.initialize(members))
                    .await
                    .map_err(|error| anyhow!("initialize: {error}"))?;
                cluster.setup().await?;
                script(cluster).await
            }
            .await;
            match result {
                Ok(mut cluster) => cluster.stop_all(),
                Err(error) => violations.push(format!("harness: {error:#}")),
            }
            (sim, violations)
        });
        let mut all = sim.lock().observer.take_violations();
        all.append(&mut violations);
        all
    }

    async fn wait_for_leader(cluster: &mut Cluster, among: &[u64]) -> Result<u64> {
        let deadline = cluster.now() + 10_000;
        while cluster.now() < deadline {
            sleep(Duration::from_millis(50)).await;
            cluster.observe();
            if let Some((id, _)) = leader(&cluster.sim, cluster.config.nodes)
                && among.contains(&id)
            {
                return Ok(id);
            }
        }
        Err(anyhow!("no leader among {among:?} within 10 s"))
    }

    async fn publish_acknowledged(cluster: &mut Cluster, client: &mut Client, n: u64) -> Result<()> {
        let id = client.uuid();
        for _ in 0..40 {
            if let Outcome::Ok(()) = client.publish(&[(id, n)]).await {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
            cluster.observe();
        }
        Err(anyhow!("publish {n} was not acknowledged"))
    }

    /// Lets the run go on for `ms` virtual milliseconds with the checks running.
    async fn run_for(cluster: &mut Cluster, ms: u64) -> Result<()> {
        let end = cluster.now() + ms;
        while cluster.now() < end {
            sleep(TICK).await;
            cluster.observe();
            cluster.scheduled().await?;
        }
        Ok(())
    }

    async fn finish(mut cluster: Cluster) -> Result<Cluster> {
        cluster.heal_all().await?;
        cluster.converge().await;
        let checkpoint = cluster.drain().await;
        cluster.converge().await;
        cluster.observe();
        cluster.final_checks(checkpoint).await;
        Ok(cluster)
    }

    /// The trace of the finding (sim mapping, section 5.1): an entry commits on {L, F}
    /// while n3 is cut off; F restarts on an empty volume; L is cut off and n3, which
    /// never had the entry, campaigns. F must not help it.
    async fn wiped_follower_and_stale_candidate(mut cluster: Cluster) -> Result<Cluster> {
        cluster.sim.lock().net.isolate(3, 1..=3);
        let leader_id = wait_for_leader(&mut cluster, &[1, 2]).await?;
        let follower = if leader_id == 1 { 2 } else { 1 };
        let mut client = Client::new(cluster.sim.clone(), PRODUCER, 5, 3);
        for n in 1..=3 {
            publish_acknowledged(&mut cluster, &mut client, n).await?;
        }
        cluster.crash(follower);
        cluster.wipe(follower);
        cluster.start(follower).await?;
        cluster.join_pending().await?;
        {
            let mut state = cluster.sim.lock();
            state.net.heal_links();
            state.net.isolate(leader_id, 1..=3);
        }
        run_for(&mut cluster, 6_000).await?;
        finish(cluster).await
    }

    #[test]
    fn events_1_wiped_follower_cannot_help_a_stale_candidate() {
        let violations = scripted(11, Knobs::default(), wiped_follower_and_stale_candidate);
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn events_1_planted_rejoin_lets_a_stale_candidate_overwrite_a_commit() {
        let knobs = Knobs {
            rejoin_wiped_voter: true,
            ..Knobs::default()
        };
        let violations = scripted(11, knobs, wiped_follower_and_stale_candidate);
        assert!(
            violations.iter().any(|violation| {
                ["lost-write", "MajorityReachedHighWatermark", "AppliedPrefixesAgree"]
                    .iter()
                    .any(|name| violation.contains(name))
            }),
            "the planted reopen must lose the committed entries: {violations:#?}"
        );
    }

    /// Two of three volumes lost at once: no removal can commit, so the group must stay
    /// unavailable rather than elect from empty logs (plan, "Brokers and storage").
    async fn majority_disk_loss(mut cluster: Cluster) -> Result<Cluster> {
        let leader_id = wait_for_leader(&mut cluster, &[1, 2, 3]).await?;
        let mut client = Client::new(cluster.sim.clone(), PRODUCER, 7, 3);
        for n in 1..=3 {
            publish_acknowledged(&mut cluster, &mut client, n).await?;
        }
        let wiped: Vec<u64> = (1..=3).filter(|id| *id != leader_id).collect();
        for id in &wiped {
            cluster.crash(*id);
            cluster.wipe(*id);
        }
        for id in &wiped {
            cluster.start(*id).await?;
        }
        run_for(&mut cluster, 8_000).await?;
        let lost = cluster.lost();
        anyhow::ensure!(
            cluster.config.knobs.rejoin_wiped_voter || lost == wiped.iter().copied().collect::<BTreeSet<u64>>(),
            "both wiped nodes must report their replica lost, got {lost:?}"
        );
        // The survivor still holds every acknowledged entry, and nothing committed since.
        let late = client.uuid();
        let outcome = client.publish(&[(late, 99)]).await;
        if !cluster.config.knobs.rejoin_wiped_voter && matches!(outcome, Outcome::Ok(())) {
            cluster.violate("majority disk loss: a publish was acknowledged without a quorum".into());
        }
        // Stop here: the fixed design stays unavailable until a backup restore, so the
        // end-of-run convergence does not apply. Check what the survivor holds instead.
        let survivor = cluster.group(leader_id).context("survivor group")?;
        let read = survivor.sim_local_read(|tx| partition::sim_state(tx, SUBSCRIPTION));
        let state = in_node(leader_id, read).await?.map_err(|error| anyhow!("{error:?}"))?;
        if !cluster.config.knobs.rejoin_wiped_voter && state.events.len() < 3 {
            cluster.violate(format!(
                "majority disk loss: the survivor holds {} of 3 acknowledged events",
                state.events.len()
            ));
        }
        Ok(cluster)
    }

    #[test]
    fn events_1_majority_disk_loss_stays_unavailable() {
        let violations = scripted(13, Knobs::default(), majority_disk_loss);
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn events_1_planted_rejoin_elects_from_empty_logs_after_majority_disk_loss() {
        let knobs = Knobs {
            rejoin_wiped_voter: true,
            ..Knobs::default()
        };
        let violations = scripted(13, knobs, majority_disk_loss);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("MajorityReachedHighWatermark")),
            "the planted reopen must elect from empty logs: {violations:#?}"
        );
    }

    /// EVENTS-2: the leader clock (and with `second`, one more clock) jumps `jump_ms`
    /// ahead after five acknowledged publishes; retention (3 s) then runs for 2 s. An
    /// acknowledged event removed before 3 s of true time is an EVT-G08 violation.
    async fn clock_jump(mut cluster: Cluster, jump_ms: u64, second: bool) -> Result<Cluster> {
        let leader_id = wait_for_leader(&mut cluster, &[1, 2, 3]).await?;
        let mut client = Client::new(cluster.sim.clone(), PRODUCER, 3, 3);
        for n in 1..=5 {
            publish_acknowledged(&mut cluster, &mut client, n).await?;
        }
        let jump = Duration::from_millis(jump_ms);
        cluster.world().jump_wall_forward(leader_id, jump);
        if second {
            cluster.world().jump_wall_forward(leader_id % 3 + 1, jump);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let task = nabedra_rt::task::spawn(retention(
            cluster.sim.clone(),
            stop.clone(),
            3,
            3_000,
            cluster.config.knobs.unguarded_clock,
        ));
        run_for(&mut cluster, 2_000).await?;
        stop.store(true, Ordering::Release);
        let removed = nabedra_rt::time::timeout(Duration::from_secs(5), task).await??;
        cluster.stats.retained_removed = removed?;
        Ok(cluster)
    }

    /// EVENTS-2, beyond the stated bound with only two voters reachable: the leader runs
    /// 10 s fast and one follower is cut off. The two samples disagree by more than the
    /// uncertainty budget, so the leader has no interval: publishes are refused and
    /// retention pauses, past the retention period in true time, with nothing removed.
    /// Once the third clock answers again, the outlier is discarded and retention resumes.
    async fn fast_leader_with_a_voter_cut_off(mut cluster: Cluster) -> Result<Cluster> {
        let leader_id = wait_for_leader(&mut cluster, &[1, 2, 3]).await?;
        let mut client = Client::new(cluster.sim.clone(), PRODUCER, 3, 3);
        for n in 1..=5 {
            publish_acknowledged(&mut cluster, &mut client, n).await?;
        }
        let cut_off = leader_id % 3 + 1;
        {
            let mut state = cluster.sim.lock();
            state.net.block(leader_id, cut_off);
            state.net.block(cut_off, leader_id);
        }
        cluster
            .world()
            .jump_wall_forward(leader_id, Duration::from_millis(10_000));
        let stop = Arc::new(AtomicBool::new(false));
        let task = nabedra_rt::task::spawn(retention(cluster.sim.clone(), stop.clone(), 3, 3_000, false));
        // Wait out the sample age, then try to publish and let retention run 6 s.
        run_for(&mut cluster, 1_500).await?;
        let refused = client.uuid();
        if let Some((id, group)) = leader(&cluster.sim, 3)
            && id == leader_id
            && in_node(id, group.time_bounds()).await.is_ok()
        {
            cluster.violate(format!(
                "EVENTS-2: the leader built an interval from a 10 s fast clock and one other ({refused})"
            ));
        }
        run_for(&mut cluster, 6_000).await?;
        stop.store(true, Ordering::Release);
        let paused = nabedra_rt::time::timeout(Duration::from_secs(5), task).await??;
        if paused? != 0 {
            cluster.violate("EVENTS-2: retention removed events without a guarded interval".into());
        }
        cluster.sim.lock().net.heal_links();
        let stop = Arc::new(AtomicBool::new(false));
        let task = nabedra_rt::task::spawn(retention(cluster.sim.clone(), stop.clone(), 3, 3_000, false));
        run_for(&mut cluster, 3_000).await?;
        stop.store(true, Ordering::Release);
        let resumed = nabedra_rt::time::timeout(Duration::from_secs(5), task).await??;
        cluster.stats.retained_removed = resumed?;
        if cluster.stats.retained_removed == 0 {
            cluster.violate("EVENTS-2: retention did not resume once three clocks answered".into());
        }
        Ok(cluster)
    }

    #[test]
    fn events_2_beyond_the_bound_without_a_third_clock_pauses_retention() {
        let violations = scripted_with(19, with_retention(false), fast_leader_with_a_voter_cut_off);
        assert!(violations.is_empty(), "{violations:#?}");
    }

    fn with_retention(unguarded: bool) -> impl FnOnce(&mut SimConfig) {
        move |config| {
            config.retention_ms = Some(3_000);
            config.knobs.unguarded_clock = unguarded;
        }
    }

    /// Beyond the stated bound for one clock: the leader runs 10 s fast. The guarded lower
    /// bound comes from the other two clocks, so nothing is removed early.
    #[test]
    fn events_2_one_fast_leader_clock_does_not_remove_events_early() {
        let violations = scripted_with(17, with_retention(false), |cluster| {
            clock_jump(cluster, 10_000, false)
        });
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// Before EVENTS-2 the leader used its own clock: the same jump removes the events
    /// about 10 s early.
    #[test]
    fn events_2_planted_unguarded_leader_clock_removes_events_early() {
        let violations = scripted_with(17, with_retention(true), |cluster| {
            clock_jump(cluster, 10_000, false)
        });
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("EVT-G08 early-removal")),
            "{violations:#?}"
        );
    }

    /// Beyond the stated bound for the group: two of three clocks run 10 s fast. RF3
    /// tolerates one wrong clock, so the guard can be fooled; this pins that limit (plan,
    /// "Brokers and storage", and the EVT-G08 wording).
    #[test]
    fn events_2_two_fast_clocks_exceed_the_stated_bound() {
        let violations = scripted_with(17, with_retention(false), |cluster| {
            clock_jump(cluster, 10_000, true)
        });
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("EVT-G08 early-removal")),
            "two wrong clocks are outside the bound the guard tolerates: {violations:#?}"
        );
    }

    /// Within the stated bound: the leader runs 200 ms fast (the bound is 250 ms).
    #[test]
    fn events_2_clock_error_within_the_bound_does_not_remove_events_early() {
        let violations = scripted_with(17, with_retention(false), |cluster| {
            clock_jump(cluster, 200, false)
        });
        assert!(violations.is_empty(), "{violations:#?}");
    }
}
