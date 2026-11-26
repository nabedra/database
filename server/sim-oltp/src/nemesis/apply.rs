//! Applies nemesis steps to a running cluster, and heals it at the end of the chaos window.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_protocol::protocol::ErrorCode;
use nabedra_rt::{
    fs::{Fault as FsFault, FaultError, FsOp},
    sim::{SimEvent, World},
    time,
};
use nabedra_sim::{Nemesis, Profile, net::draw_partition};
use serde_json::json;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{Action, FaultId, FollowUp, NetFaults, Rule, Step, WindowId, window};
use crate::{
    bootstrap::{GATEWAYS, ROOT, WORKERS, initial_range},
    config::CLOCK_DRIFT_BOUND_PPM,
    disk::DiskCut,
    invariants::{RECOVERY_LIMIT, STEP, elapsed_ms},
    node::Cluster,
    operator::Split,
    workloads::{
        clauses::{Answer, Effect, Fault},
        clients::{self, Ctx},
        resources::{disturbs, drain_failed},
    },
};

/// Bound for a cold start after the last worker starts (design 3.6, O15).
pub(crate) const COLD_LIMIT: Duration = Duration::from_secs(120);
/// Bound for a split to publish once the cluster is healed.
const SPLIT_LIMIT: Duration = Duration::from_secs(60);
/// Follow-ups that cannot run yet are tried again after this pause.
const DEFER_MS: u64 = 500;
const RESTART_TRIES: u32 = 4;
/// Longest isolation of a leader that no other worker replaces. Voters keep a lease of the
/// longest election timeout, so an election takes longer than the drawn window.
const LEADER_MOVE_MS: u64 = 12_000;
/// Between two starts of one process.
const RESTART_PAUSE_MS: u64 = DEFER_MS * 4;

/// Note a start of `node` that failed. The last one is a violation, and ends the tries.
fn start_failed(cluster: &Cluster, node: u64, failures: u32, last: bool, error: &str) -> bool {
    cluster.record(format!(
        "nemesis restart n{node} failed ({failures}): {error}"
    ));
    let last = last || failures >= RESTART_TRIES;
    if last {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("n{node} did not restart: {error}"),
        );
    }
    last
}

/// Start a stopped process as its supervisor does, and as `Chaos` does for its own faults: a start
/// that fails is made again after the same pause, until the node has failed as often as there
/// (lead decision 18 for a gateway; a worker the same way, its later starts after a kill). `failures`
/// counts over the run. Returns the starts that failed, and whether the process runs.
pub(crate) async fn start_again(
    cluster: &mut Cluster,
    node: u64,
    how: DiskCut,
    failures: &mut BTreeMap<u64, u32>,
) -> (Vec<String>, bool) {
    let mut failed = Vec::new();
    let mut how = how;
    loop {
        let started = if GATEWAYS.contains(&node) {
            cluster.restart_gateway(node).await
        } else {
            cluster.restart_worker(node, how).await
        };
        let Err(error) = started else {
            return (failed, true);
        };
        failed.push(format!(
            "n{node} at {}ms: {error}",
            elapsed_ms(&cluster.world)
        ));
        let count = failures.entry(node).or_default();
        *count += 1;
        if start_failed(cluster, node, *count, false, &error) {
            return (failed, false);
        }
        how = DiskCut::Kill;
        time::sleep(Duration::from_millis(RESTART_PAUSE_MS)).await;
    }
}

/// A graceful stop that has not ended.
struct Drain {
    id: FaultId,
    deadline: u64,
    restart_after: Duration,
}

/// When a sync of a worker's volume failed, as the filesystem reported it.
#[derive(Clone, Default)]
struct SyncFailures(Arc<Mutex<Vec<(u64, u64)>>>);

impl SyncFailures {
    /// Watch every volume of the world. A call is logged on any thread, where the virtual clock
    /// can't be read, so a task reads the time within one step.
    fn watch(world: &Arc<World>) -> (Self, JoinHandle<()>) {
        let failed = Arc::new(Mutex::new(Vec::new()));
        let seen = failed.clone();
        world.observe(Arc::new(move |event| {
            if let SimEvent::Fs { node, mount, call } = event
                && !call.ok
                && mount == &Path::new(ROOT)
                && matches!(call.op, FsOp::SyncData | FsOp::SyncAll)
            {
                seen.lock().unwrap().push(*node);
            }
        }));
        let failures = Self::default();
        let log = failures.clone();
        let world = Arc::downgrade(world);
        let task = nabedra_rt::task::spawn(async move {
            loop {
                time::sleep(STEP).await;
                let Some(world) = world.upgrade() else { return };
                let at = elapsed_ms(&world);
                let nodes = std::mem::take(&mut *failed.lock().unwrap());
                let mut log = log.0.lock().unwrap();
                log.extend(nodes.into_iter().map(|node| (node, at)));
            }
        });
        (failures, task)
    }

    fn first(&self, node: u64, from: u64) -> Option<u64> {
        let failures = self.0.lock().unwrap();
        let mut failures = failures.iter();
        failures
            .find(|(failed, at)| *failed == node && *at >= from)
            .map(|(_, at)| *at)
    }
}

/// The nemesis of one run and what it did.
pub(crate) struct Chaos {
    scheduler: Nemesis<Step>,
    faults: NetFaults,
    pub skipped: BTreeMap<Action, u32>,
    /// Every planned fault, with what happened.
    pub log: Vec<Fault>,
    split: Option<(FaultId, Split)>,
    cold_done: bool,
    /// The heal needed a full cold start.
    pub heal_cold: bool,
    /// The partition or leader isolation that is in force.
    links: Option<FaultId>,
    /// The freeze that holds each frozen worker.
    freezes: BTreeMap<u64, FaultId>,
    windows: BTreeMap<FaultId, WindowId>,
    restart_failures: BTreeMap<u64, u32>,
    certificates: BTreeSet<u64>,
    /// Workers with an armed disk error.
    disks: BTreeMap<u64, FaultId>,
    drains: BTreeMap<u64, Drain>,
    syncs: SyncFailures,
    watcher: JoinHandle<()>,
    beyond_tcp: bool,
    bursts: Vec<(FaultId, JoinHandle<()>)>,
}

impl Drop for Chaos {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

impl Chaos {
    pub(crate) fn new(
        world: &Arc<World>,
        profile: Profile<Action>,
        faults: NetFaults,
        start_ms: u64,
        g: &mut Gen,
    ) -> Self {
        let mut steps = Profile::new(profile.name, profile.gap_ms);
        for (action, weight) in profile.actions() {
            steps = steps.with(Step::Act(*action), weight);
        }
        let (syncs, watcher) = SyncFailures::watch(world);
        Self {
            scheduler: Nemesis::new(steps, start_ms, g),
            faults,
            skipped: BTreeMap::new(),
            log: Vec::new(),
            split: None,
            cold_done: false,
            heal_cold: false,
            links: None,
            freezes: BTreeMap::new(),
            windows: BTreeMap::new(),
            restart_failures: BTreeMap::new(),
            certificates: BTreeSet::new(),
            disks: BTreeMap::new(),
            drains: BTreeMap::new(),
            syncs,
            watcher,
            beyond_tcp: false,
            bursts: Vec::new(),
        }
    }

    pub(crate) fn profile_name(&self) -> &'static str {
        self.scheduler.profile().name
    }

    /// How often each action was applied, or happened.
    pub(crate) fn counts(&self, happened: bool) -> BTreeMap<&'static str, u32> {
        let mut counts = BTreeMap::new();
        for fault in self.log.iter().filter(|f| !happened || f.happened()) {
            *counts.entry(fault.action.name()).or_default() += 1;
        }
        counts
    }

    /// Windows that a blocked loop held for longer than planned: how many, and the longest
    /// excess in ms. Process faults end when the process is ready again, so they have none.
    pub(crate) fn overran(&self) -> [u64; 2] {
        let windows = self.log.iter().filter(|fault| {
            !matches!(
                fault.action,
                Action::CrashLeader
                    | Action::PowerLeader
                    | Action::DrainLeader
                    | Action::DiskError
                    | Action::KillGateway
            )
        });
        let late = windows.filter_map(|fault| fault.to?.checked_sub(fault.until?));
        let late: Vec<u64> = late.filter(|late| *late > DEFER_MS).collect();
        [late.len() as u64, late.into_iter().max().unwrap_or(0)]
    }

    /// The faults aimed at the Data leader: how many happened, and how many hit it.
    pub(crate) fn aimed(&self) -> BTreeMap<&'static str, [u32; 2]> {
        let mut aimed: BTreeMap<_, [u32; 2]> = BTreeMap::new();
        for fault in self.log.iter().filter(|fault| fault.happened()) {
            if let Some(hit) = fault.leader {
                let row = aimed.entry(fault.action.name()).or_default();
                row[0] += 1;
                row[1] += u32::from(hit);
            }
        }
        aimed
    }

    /// Apply the steps that are due.
    pub(crate) async fn tick(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen) {
        let now = elapsed_ms(&cluster.world);
        for step in self.scheduler.poll(now, g) {
            match step {
                Step::Act(action) => self.act(cluster, ctx, g, action).await,
                Step::Follow(follow) => self.follow(cluster, ctx, follow, false).await,
            }
        }
        self.drained(cluster, ctx).await;
        self.observe(cluster, ctx);
    }

    /// End the graceful stops that are over. The loop does not wait for one, because no other
    /// fault could end meanwhile: move seed 128's 4 s partition lasted the 30 s of a drain.
    async fn drained(&mut self, cluster: &mut Cluster, ctx: &Ctx) {
        for node in self.drains.keys().copied().collect::<Vec<_>>() {
            let drained = match cluster.drained(node).await {
                Some(result) => result,
                None if elapsed_ms(&cluster.world) >= self.drains[&node].deadline => {
                    Err(format!("n{node} drain deadline"))
                }
                None => continue,
            };
            let Drain {
                id, restart_after, ..
            } = self.drains.remove(&node).expect("pending drain");
            self.cut(cluster, node, DiskCut::Kill).await;
            if let Err(error) = &drained {
                drain_failed(cluster, node, error);
            }
            self.note(
                cluster,
                ctx,
                "drained",
                json!({"worker": node, "drained": drained.err()}),
            );
            let follow = FollowUp::RestartWorker(node, false, id);
            self.later(cluster, restart_after, follow);
        }
    }

    /// Note what open faults did, and end the ones that ended by themselves.
    fn observe(&mut self, cluster: &mut Cluster, ctx: &Ctx) {
        for id in 0..self.log.len() {
            if self.log[id].to.is_none() && self.log[id].action == Action::SessionDrop {
                self.collect(cluster, id);
                // It drops one session.
                self.log[id].to = self.log[id].effects.first().map(|effect| effect.at);
            }
        }
        let (done, open): (Vec<_>, Vec<_>) = std::mem::take(&mut self.bursts)
            .into_iter()
            .partition(|(_, burst)| burst.is_finished());
        self.bursts = open;
        for (id, _) in done {
            self.burst_done(cluster, ctx, id);
        }
        self.split_published(cluster);
    }

    /// A burst that nothing answered with overload did not exhaust a lane.
    fn burst_done(&mut self, cluster: &Cluster, ctx: &Ctx, id: FaultId) {
        let from = self.log[id].from;
        let refused = {
            let observed = ctx.observed.lock().unwrap();
            let mut attempts = observed.attempts.iter();
            attempts
                .find(|a| a.invoked >= from && a.answer == Answer::Refused(ErrorCode::Overloaded))
                .map(|attempt| attempt.completed)
        };
        self.log[id].effects.extend(refused.map(Effect::at));
        self.close(cluster, id);
    }

    /// Start a fault in the log.
    fn open(&mut self, cluster: &Cluster, action: Action, nodes: &[u64]) -> FaultId {
        self.log.push(Fault {
            action,
            nodes: nodes.to_vec(),
            from: elapsed_ms(&cluster.world),
            to: None,
            until: None,
            quiet: false,
            effects: Vec::new(),
            leader: None,
            isolated: None,
        });
        self.log.len() - 1
    }

    /// The fault took effect now.
    fn effect(&mut self, cluster: &Cluster, id: FaultId) {
        self.log[id]
            .effects
            .push(Effect::at(elapsed_ms(&cluster.world)));
    }

    /// Start a fault that takes effect at once.
    fn hit(&mut self, cluster: &Cluster, action: Action, nodes: &[u64]) -> FaultId {
        let id = self.open(cluster, action, nodes);
        self.effect(cluster, id);
        id
    }

    /// Start a fault whose effect is on the frames of a window.
    fn frames(&mut self, cluster: &Cluster, action: Action, nodes: &[u64], rule: Rule) -> FaultId {
        let id = self.open(cluster, action, nodes);
        self.windows.insert(id, self.faults.start(rule));
        id
    }

    /// Read what the fault's window did.
    fn collect(&mut self, cluster: &Cluster, id: FaultId) {
        if let Some(window) = self.windows.get(&id) {
            let seen = self.faults.effect(*window);
            let resets = seen.resets.into_iter().map(|reset| Effect {
                at: reset.at,
                ends: [reset.from, reset.to],
                unanswered: reset.unanswered,
            });
            self.log[id].effects = resets.chain(seen.first.map(Effect::at)).collect();
        }
        let fault = &mut self.log[id];
        match fault.action {
            Action::LeaderMove => {
                let mut elections = cluster.elections().into_iter();
                let moved =
                    elections.find(|(at, node)| *at >= fault.from && fault.nodes != [*node]);
                fault.effects = moved.map(|(at, _)| Effect::at(at)).into_iter().collect();
            }
            Action::DiskError => {
                let failed = self.syncs.first(fault.nodes[0], fault.from);
                fault.effects = failed.map(Effect::at).into_iter().collect();
            }
            _ => {}
        }
    }

    /// End a fault, once, with what it did.
    fn close(&mut self, cluster: &Cluster, id: FaultId) {
        if self.log[id].to.is_some() {
            return;
        }
        self.collect(cluster, id);
        if let Some(window) = self.windows.get(&id) {
            self.faults.end(*window);
        }
        let timed = self.windows.contains_key(&id) || self.log[id].action == Action::AuthFlap;
        let fault = &mut self.log[id];
        let now = elapsed_ms(&cluster.world);
        fault.to = Some(fault.until.filter(|_| timed).map_or(now, |at| at.min(now)));
        if !fault.happened() {
            cluster.record(format!(
                "nemesis {} from {}ms did not happen",
                fault.action, fault.from
            ));
        }
    }

    /// Stop a worker that is not frozen any more afterwards.
    async fn cut(&mut self, cluster: &mut Cluster, node: u64, how: DiskCut) {
        cluster.crash(node, how).await;
        if let Some(id) = self.freezes.remove(&node)
            && !self.freezes.values().any(|held| *held == id)
        {
            // The kill ended the freeze.
            self.close(cluster, id);
        }
    }

    /// Nothing else is open, a pending split included, and the cluster reports itself
    /// converged.
    async fn quiet(&self, cluster: &Cluster) -> bool {
        // The `resources` overlay cuts, loads and stalls for the whole window.
        !disturbs(&cluster.world)
            && self.log.iter().all(|fault| fault.to.is_some())
            && !self.faults.open()
            && cluster.converged().await
    }

    /// The split's fault ends when it publishes.
    fn split_published(&mut self, cluster: &mut Cluster) {
        let Some((id, split)) = &self.split else {
            return;
        };
        let id = *id;
        if self.log[id].to.is_none()
            && let Some((at, _, result)) = split.published()
        {
            self.log[id].effects = vec![Effect::at(at)];
            self.log[id].to = Some(at);
            let group = result.pointer("/children/0/group_id");
            let group = group.and_then(|id| Uuid::parse_str(id.as_str()?).ok());
            if let Some(group) = group {
                cluster.watch_range(group);
            }
        }
    }

    /// Ask for the pending split once more, immediately before every worker stops.
    pub(crate) async fn before_full_stop(&mut self, cluster: &mut Cluster) {
        if let Some((_, split)) = &mut self.split {
            split.pause().await;
        }
        self.split_published(cluster);
    }

    /// The stop abandoned the pending split. Request it again under a new ID.
    pub(crate) async fn after_full_stop(&mut self) {
        if let Some((_, split)) = &mut self.split {
            split.resume().await;
        }
    }

    /// A split that a full stop abandoned must not have published in the next run.
    pub(crate) async fn check_abandoned(&self) {
        if let Some((_, split)) = &self.split {
            split.check_abandoned().await;
        }
    }

    fn note(&self, cluster: &Cluster, ctx: &Ctx, action: &str, detail: serde_json::Value) {
        cluster.record(format!("nemesis {action} {detail}"));
        ctx.history
            .lock()
            .unwrap()
            .marker(&cluster.world, action, detail);
    }

    fn skip(&mut self, cluster: &Cluster, action: Action, why: &str) {
        cluster.record(format!("nemesis skip {action}: {why}"));
        *self.skipped.entry(action).or_default() += 1;
    }

    fn later(&mut self, cluster: &Cluster, after: Duration, follow: FollowUp) {
        let at = elapsed_ms(&cluster.world) + u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
        let id = follow.fault();
        self.log[id].until = Some(at);
        // The loop may wait for a restart at that time, so these windows end by themselves.
        match &follow {
            FollowUp::ClearNet(id) => {
                let (faults, window) = (self.faults.clone(), self.windows[id]);
                nabedra_rt::task::spawn(async move {
                    time::sleep(after).await;
                    faults.end(window);
                });
            }
            FollowUp::RestoreCertificate(gateway, _) => {
                let (world, gateway) = (Arc::downgrade(&cluster.world), *gateway);
                nabedra_rt::task::spawn(async move {
                    time::sleep(after).await;
                    if let Some(world) = world.upgrade() {
                        world.override_certificate(gateway, None);
                    }
                });
            }
            _ => {}
        }
        self.scheduler.after(at, Step::Follow(follow));
    }

    /// The Data leader, or a worker drawn from the tape when no leader is known.
    fn leader_or_any(&self, cluster: &Cluster, g: &mut Gen) -> Option<u64> {
        if let Some(leader) = cluster.leader() {
            return Some(leader);
        }
        let candidates: Vec<u64> = WORKERS
            .into_iter()
            .filter(|node| cluster.is_running(*node) && !cluster.frozen().contains(node))
            .collect();
        (!candidates.is_empty()).then(|| *g.pick(&candidates))
    }

    /// Stop a worker gracefully, and restart it a while after it stopped.
    pub(crate) async fn drain_worker(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        node: u64,
        restart_after: Duration,
    ) -> FaultId {
        let id = self.hit(cluster, Action::DrainLeader, &[node]);
        if let Err(error) = cluster.start_drain(node).await {
            drain_failed(cluster, node, &error);
        }
        let deadline = elapsed_ms(&cluster.world) + RECOVERY_LIMIT.as_millis() as u64;
        let drain = Drain {
            id,
            deadline,
            restart_after,
        };
        self.drains.insert(node, drain);
        self.note(
            cluster,
            ctx,
            "drain-leader",
            json!({"worker": node, "elected": cluster.elected(), "restart_after_ms": restart_after.as_millis() as u64}),
        );
        id
    }

    /// Whether nothing the nemesis did keeps a worker from being stopped.
    pub(crate) fn may_stop(&self) -> bool {
        self.disks.is_empty() && self.drains.is_empty()
    }

    async fn act(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen, action: Action) {
        let process = matches!(
            action,
            Action::CrashLeader
                | Action::PowerLeader
                | Action::DrainLeader
                | Action::DiskError
                | Action::Cold
        );
        // The same-run restart needs two live voters, and an armed disk stops its worker.
        let down = WORKERS.into_iter().any(|node| !cluster.is_running(node));
        if process && (down || !self.disks.is_empty() || !self.drains.is_empty()) {
            return self.skip(cluster, action, "a worker is already down");
        }
        // A cold start blocks this loop, so no open window could end while it runs.
        if action == Action::Cold
            && (self.links.is_some() || !cluster.frozen().is_empty() || self.faults.open())
        {
            return self.skip(cluster, action, "a fault window is open");
        }
        match action {
            Action::Clock => {
                let nodes: Vec<u64> = WORKERS.into_iter().chain(GATEWAYS).collect();
                let node = *g.pick(&nodes);
                let drift = g.range_i64(-CLOCK_DRIFT_BOUND_PPM, CLOCK_DRIFT_BOUND_PPM);
                let jump = g.range_i64(-2_000, 2_000);
                cluster.world.set_drift_ppm(node, drift);
                let by = Duration::from_millis(jump.unsigned_abs());
                if jump < 0 {
                    cluster.world.jump_wall_backward(node, by);
                } else {
                    cluster.world.jump_wall_forward(node, by);
                }
                let id = self.hit(cluster, action, &[node]);
                self.close(cluster, id);
                self.note(
                    cluster,
                    ctx,
                    "clock",
                    json!({"node": node, "drift_ppm": drift, "wall_jump_ms": jump}),
                );
            }
            Action::FreezeFollower | Action::FreezeTwo => {
                if !cluster.frozen().is_empty() {
                    return self.skip(cluster, action, "a worker is already frozen");
                }
                let Some(leader) = self.leader_or_any(cluster, g) else {
                    return self.skip(cluster, action, "no running worker");
                };
                let mut followers: Vec<u64> = WORKERS
                    .into_iter()
                    .filter(|node| *node != leader && cluster.is_running(*node))
                    .collect();
                if followers.is_empty() {
                    return self.skip(cluster, action, "no running follower");
                }
                let quiet = self.quiet(cluster).await;
                // One, then two (D05): the first follower is frozen alone for a while.
                let first = followers.remove(g.below(followers.len() as u64) as usize);
                cluster.freeze(first);
                let mut id = self.hit(cluster, Action::FreezeFollower, &[first]);
                self.log[id].quiet = quiet;
                self.log[id].leader = Some(cluster.elected() == Some(first));
                let mut frozen = vec![first];
                if action == Action::FreezeTwo
                    && let Some(second) = followers.pop()
                {
                    time::sleep(window(g, 200, 2_000)).await;
                    self.close(cluster, id);
                    cluster.freeze(second);
                    frozen.push(second);
                    id = self.hit(cluster, action, &frozen);
                    let leader = cluster.elected();
                    self.log[id].leader = Some(leader.is_some_and(|node| frozen.contains(&node)));
                }
                self.freezes.extend(frozen.iter().map(|node| (*node, id)));
                let duration = window(g, 500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    action.name(),
                    json!({"nodes": frozen, "leader": leader, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::Thaw(id));
            }
            Action::Partition => {
                if self.links.is_some() {
                    return self.skip(cluster, action, "links already blocked");
                }
                let sides = draw_partition(g, &WORKERS);
                cluster.net().update_plan(|plan| plan.partition(&sides));
                let nodes: Vec<u64> = sides.iter().flatten().copied().collect();
                let id = self.hit(cluster, action, &nodes);
                self.links = Some(id);
                let duration = window(g, 500, 5_000);
                self.note(
                    cluster,
                    ctx,
                    "partition",
                    json!({"sides": sides, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::HealLinks(id));
            }
            Action::LeaderMove => {
                if self.links.is_some() {
                    return self.skip(cluster, action, "links already blocked");
                }
                let Some(leader) = cluster.leader() else {
                    return self.skip(cluster, action, "no known leader");
                };
                cluster.net().update_plan(|plan| {
                    for other in WORKERS.into_iter().filter(|node| *node != leader) {
                        plan.block(leader, other);
                        plan.block(other, leader);
                    }
                });
                let id = self.open(cluster, action, &[leader]);
                self.links = Some(id);
                let duration = window(g, 1_500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    "leader-move",
                    json!({"leader": leader, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::HealLinks(id));
            }
            Action::SessionDrop => {
                let frames = 1 + g.below(200);
                self.frames(cluster, action, &[], Rule::SessionDrop { left: frames });
                self.note(
                    cluster,
                    ctx,
                    "session-drop",
                    json!({"after_frames": frames}),
                );
            }
            Action::LostReply => {
                let Some(leader) = self.leader_or_any(cluster, g) else {
                    return self.skip(cluster, action, "no running worker");
                };
                let per_mille = *g.pick(&[100, 300, 1_000]);
                let worker = leader;
                let rule = Rule::ReplyReset { worker, per_mille };
                let id = self.frames(cluster, action, &[leader], rule);
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "lost-reply", json!({"worker": leader, "per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
            Action::LostHttpReply => {
                let gateway = *g.pick(&GATEWAYS);
                let per_mille = *g.pick(&[100, 300, 1_000]);
                let rule = Rule::HttpReset { gateway, per_mille };
                let id = self.frames(cluster, action, &[gateway], rule);
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "lost-http-reply", json!({"gateway": gateway, "per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
            Action::DeadlineEdge => {
                let Some(leader) = self.leader_or_any(cluster, g) else {
                    return self.skip(cluster, action, "no running worker");
                };
                let deadline = ctx.timeout.saturating_sub(Duration::from_secs(1));
                let percent = 40 + g.below(56);
                let delay = deadline * u32::try_from(percent).unwrap() / 200;
                let (worker, by) = (leader, delay);
                let id = self.frames(cluster, action, &[leader], Rule::Delay { worker, by });
                let duration = window(g, 500, 3_000);
                self.note(cluster, ctx, "deadline-edge", json!({"worker": leader, "delay_ms": delay.as_millis() as u64, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
            Action::AuthFlap => {
                let gateway = *g.pick(&GATEWAYS);
                if self.certificates.contains(&gateway) || !cluster.is_running(gateway) {
                    return self.skip(cluster, action, "gateway unavailable");
                }
                let mut stale = [0_u8; 32];
                for chunk in stale.chunks_mut(8) {
                    chunk.copy_from_slice(&g.u64().to_le_bytes());
                }
                cluster.world.override_certificate(gateway, Some(stale));
                // Existing sessions keep their handshake; reset them so the next dial
                // presents the stale certificate.
                let id = self.open(cluster, action, &[gateway]);
                for connection in cluster.world.connections() {
                    if connection.client == Some(gateway)
                        && connection
                            .server
                            .is_some_and(|node| WORKERS.contains(&node))
                    {
                        cluster.world.reset_connection(connection.id);
                        self.effect(cluster, id);
                    }
                }
                self.certificates.insert(gateway);
                let duration = window(g, 500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    "auth-flap",
                    json!({"gateway": gateway, "ms": duration.as_millis() as u64}),
                );
                let follow = FollowUp::RestoreCertificate(gateway, id);
                self.later(cluster, duration, follow);
            }
            Action::KillGateway => {
                let gateway = *g.pick(&GATEWAYS);
                if !cluster.is_running(gateway) {
                    return self.skip(cluster, action, "the gateway is already down");
                }
                let id = self.hit(cluster, action, &[gateway]);
                cluster.kill_gateway(gateway).await;
                let duration = window(g, 300, 3_000);
                self.note(
                    cluster,
                    ctx,
                    "kill-gateway",
                    json!({"gateway": gateway, "restart_after_ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::RestartGateway(gateway, id));
            }
            Action::CrashLeader | Action::PowerLeader => {
                let Some(leader) = self.leader_or_any(cluster, g) else {
                    return self.skip(cluster, action, "no running worker");
                };
                let power = action == Action::PowerLeader;
                let variant = *g.pick(&["", "before-quorum", "lost-replies"]);
                let followers: Vec<u64> = WORKERS
                    .into_iter()
                    .filter(|node| *node != leader && !cluster.frozen().contains(node))
                    .collect();
                // Before quorum: writes sent while both followers are frozen cannot have
                // committed when the leader dies. Lost replies: the leader's answers are
                // cut first, so writes it committed are still unknown to their clients.
                let mut isolated = None;
                let mut lost = None;
                match variant {
                    "before-quorum" => {
                        for &node in &followers {
                            cluster.freeze(node);
                        }
                        isolated = Some(elapsed_ms(&cluster.world));
                        time::sleep(window(g, 100, 500)).await;
                    }
                    "lost-replies" => {
                        let (worker, per_mille) = (leader, 1_000);
                        lost = Some(self.faults.start(Rule::ReplyReset { worker, per_mille }));
                        time::sleep(window(g, 50, 300)).await;
                    }
                    _ => {}
                }
                let id = self.hit(cluster, action, &[leader]);
                self.log[id].leader = Some(cluster.elected() == Some(leader));
                self.log[id].isolated = isolated;
                let how = if power { DiskCut::Power } else { DiskCut::Kill };
                self.cut(cluster, leader, how).await;
                if isolated.is_some() {
                    for &node in &followers {
                        cluster.thaw(node);
                    }
                }
                if let Some(window) = lost {
                    self.faults.end(window);
                }
                let duration = window(g, 500, 5_000);
                self.note(cluster, ctx, action.name(), json!({"worker": leader, "elected": cluster.elected(), "variant": variant, "restart_after_ms": duration.as_millis() as u64}));
                let follow = FollowUp::RestartWorker(leader, power, id);
                self.later(cluster, duration, follow);
            }
            Action::DrainLeader => {
                let Some(leader) = self.leader_or_any(cluster, g) else {
                    return self.skip(cluster, action, "no running worker");
                };
                let elected = cluster.elected() == Some(leader);
                let restart_after = window(g, 300, 3_000);
                let id = self.drain_worker(cluster, ctx, leader, restart_after).await;
                self.log[id].leader = Some(elected);
            }
            Action::LaneExhaust => {
                let gateway = g.below(2) as usize;
                let count = 64 + g.below(193);
                let quiet = self.quiet(cluster).await;
                let id = self.open(cluster, action, &[GATEWAYS[gateway]]);
                self.log[id].quiet = quiet;
                let first = 1 + g.below(ctx.keys as u64) as i64;
                let burst = clients::burst(ctx.clone(), gateway, count, first);
                self.bursts.push((id, nabedra_rt::task::spawn(burst)));
                self.note(
                    cluster,
                    ctx,
                    "lane-exhaust",
                    json!({"gateway": GATEWAYS[gateway], "requests": count}),
                );
            }
            Action::Split => {
                if self.split.is_some() {
                    return self.skip(cluster, action, "a split was already requested");
                }
                let boundary = 2 + g.below(ctx.keys.max(2) as u64 - 1) as i64;
                let table = Uuid::parse_str(&ctx.table).expect("table UUID");
                let source = initial_range(cluster.config.cluster_id, table);
                let id = self.open(cluster, action, &[]);
                let split = Split::start(cluster.operator(), source, boundary).await;
                let plan = split.plans()[0].id;
                self.note(
                    cluster,
                    ctx,
                    "split",
                    json!({"boundary": boundary, "id": plan}),
                );
                self.split = Some((id, split));
            }
            Action::DiskError => {
                let node = *g.pick(&WORKERS);
                let op = *g.pick(&[FsOp::SyncData, FsOp::SyncAll]);
                let nth = 1 + g.below(20);
                let Some(image) = cluster.world.volume(node, Path::new(ROOT)) else {
                    return self.skip(cluster, action, "no mounted volume");
                };
                image.inject(FsFault::on(op).nth(nth).error(FaultError::Eio));
                // It only happens if a sync fails (review 115, M7).
                let id = self.open(cluster, action, &[node]);
                self.disks.insert(node, id);
                let duration = window(g, 500, 3_000);
                self.note(cluster, ctx, "disk-error", json!({"worker": node, "op": format!("{op:?}"), "nth": nth, "recover_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearDisk(node, id));
            }
            Action::Cold => {
                if self.cold_done {
                    return self.skip(cluster, action, "one cold start per run");
                }
                self.cold_done = true;
                let power = g.bool();
                let delays = [(); 3].map(|()| window(g, 0, 3_000));
                self.note(cluster, ctx, "cold", json!({"power": power, "delays_ms": delays.map(|delay| delay.as_millis() as u64)}));
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                self.before_full_stop(cluster).await;
                let id = self.hit(cluster, action, &WORKERS);
                if let Err(error) = cluster
                    .cold_start_staggered("cold action", cut, delays, COLD_LIMIT)
                    .await
                {
                    cluster.checks.violate(
                        &cluster.world,
                        "Liveness",
                        format!("cold start: {error}"),
                    );
                }
                self.close(cluster, id);
                self.after_full_stop().await;
            }
            Action::FrameDrop => {
                let per_mille = *g.pick(&[5, 20, 50]);
                let id = self.frames(cluster, action, &[], Rule::FrameDrop { per_mille });
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "frame-drop",
                    json!({"per_mille": per_mille, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
            Action::BlackHole => {
                let worker = *g.pick(&WORKERS);
                let stream = None;
                let rule = Rule::BlackHole { worker, stream };
                let id = self.frames(cluster, action, &[worker], rule);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "black-hole",
                    json!({"worker": worker, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
            Action::Reorder => {
                let max_ms = *g.pick(&[5, 30, 120]);
                let id = self.frames(cluster, action, &[], Rule::Reorder { max_ms });
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "reorder",
                    json!({"max_ms": max_ms, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(id));
            }
        }
    }

    async fn follow(&mut self, cluster: &mut Cluster, ctx: &Ctx, follow: FollowUp, healing: bool) {
        match follow {
            FollowUp::Thaw(id) => {
                // A worker that was killed meanwhile, and frozen again, stays frozen.
                let held = self.freezes.iter().filter(|(_, held)| **held == id);
                let nodes: Vec<u64> = held.map(|(node, _)| *node).collect();
                for node in nodes {
                    self.freezes.remove(&node);
                    cluster.thaw(node);
                }
                self.close(cluster, id);
            }
            FollowUp::HealLinks(id) => {
                self.collect(cluster, id);
                let fault = &self.log[id];
                let now = elapsed_ms(&cluster.world);
                if fault.action == Action::LeaderMove
                    && !fault.happened()
                    && !healing
                    && now < fault.from + LEADER_MOVE_MS
                {
                    // Isolated until another one leads (D07).
                    let at = now + STEP.as_millis() as u64 * 10;
                    self.log[id].until = Some(at);
                    self.scheduler
                        .after(at, Step::Follow(FollowUp::HealLinks(id)));
                    return;
                }
                cluster.net().update_plan(|plan| plan.heal_links());
                self.links = None;
                self.close(cluster, id);
                cluster.record("nemesis heal links".into());
            }
            FollowUp::RestoreCertificate(gateway, id) => {
                cluster.world.override_certificate(gateway, None);
                self.certificates.remove(&gateway);
                self.close(cluster, id);
                cluster.record(format!("nemesis restore certificate n{gateway}"));
            }
            FollowUp::ClearNet(id) => self.close(cluster, id),
            FollowUp::ClearDisk(node, id) => {
                if let Some(image) = cluster.world.volume(node, Path::new(ROOT)) {
                    image.clear_faults();
                }
                self.disks.remove(&node);
                self.collect(cluster, id);
                let fired = self.log[id].effects.first().map(|effect| effect.at);
                cluster.record(format!("nemesis disk-error n{node} fired {fired:?}"));
                let Some(at) = fired else {
                    // No sync failed, so the worker has nothing to recover from.
                    return self.close(cluster, id);
                };
                let mut before = cluster.elections();
                before.retain(|(elected, _)| *elected <= at);
                self.log[id].leader =
                    Some(before.last().is_some_and(|(_, leader)| *leader == node));
                // O29: a pool that saw an ambiguous sync error reopens through storage
                // recovery before it serves again, so the operator restarts the worker.
                if cluster.world.is_up(node) {
                    self.cut(cluster, node, DiskCut::Kill).await;
                }
                let restart = FollowUp::RestartWorker(node, false, id);
                Box::pin(self.follow(cluster, ctx, restart, healing)).await;
            }
            FollowUp::RestartGateway(gateway, id) => {
                if cluster.is_running(gateway) {
                    return self.close(cluster, id);
                }
                if !healing && (!cluster.frozen().is_empty() || self.links.is_some()) {
                    // This loop cannot thaw a worker or heal a link while a start blocks it.
                    let at = elapsed_ms(&cluster.world) + DEFER_MS;
                    let again = FollowUp::RestartGateway(gateway, id);
                    self.scheduler.after(at, Step::Follow(again));
                    return;
                }
                if let Err(error) = cluster.restart_gateway(gateway).await {
                    let again = FollowUp::RestartGateway(gateway, id);
                    self.retry(cluster, again, gateway, healing, error);
                } else {
                    self.close(cluster, id);
                }
            }
            FollowUp::RestartWorker(node, power, id) => {
                if cluster.is_running(node) {
                    return self.close(cluster, id);
                }
                if !healing
                    && (!cluster.frozen().is_empty() || self.links.is_some() || self.faults.open())
                {
                    // Retained restarts need the survivors' quorum, and this loop cannot
                    // end a fault window while a restart blocks it.
                    let at = elapsed_ms(&cluster.world) + DEFER_MS;
                    let again = FollowUp::RestartWorker(node, power, id);
                    self.scheduler.after(at, Step::Follow(again));
                    return;
                }
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                if let Err(error) = cluster.restart_worker(node, cut).await {
                    let again = FollowUp::RestartWorker(node, false, id);
                    self.retry(cluster, again, node, healing, error);
                } else {
                    self.close(cluster, id);
                }
            }
        }
    }

    fn retry(
        &mut self,
        cluster: &mut Cluster,
        follow: FollowUp,
        node: u64,
        healing: bool,
        error: String,
    ) {
        let failures = self.restart_failures.entry(node).or_default();
        *failures += 1;
        if start_failed(cluster, node, *failures, healing, &error) {
            return;
        }
        let at = elapsed_ms(&cluster.world) + RESTART_PAUSE_MS;
        self.scheduler.after(at, Step::Follow(follow));
    }

    /// Stop the nemesis, apply every pending follow-up, undo every open fault and restart
    /// whatever is down. The cluster is healthy afterwards unless a violation says why not.
    pub(crate) async fn heal(&mut self, cluster: &mut Cluster, ctx: &Ctx) -> Result<(), String> {
        while !self.drains.is_empty() {
            self.drained(cluster, ctx).await;
            time::sleep(STEP).await;
        }
        let pending = self.scheduler.stop();
        cluster.record(format!("nemesis heal with {} follow-ups", pending.len()));
        ctx.history.lock().unwrap().marker(
            &cluster.world,
            "heal",
            json!({"follow_ups": pending.len()}),
        );
        let (restarts, others): (Vec<_>, Vec<_>) = pending.into_iter().partition(|step| {
            matches!(
                step,
                Step::Follow(FollowUp::RestartWorker(..) | FollowUp::RestartGateway(..))
            )
        });
        for step in others {
            if let Step::Follow(follow) = step {
                self.follow(cluster, ctx, follow, true).await;
            }
        }
        for node in cluster.frozen().clone() {
            cluster.thaw(node);
        }
        self.freezes.clear();
        cluster.net().update_plan(|plan| plan.heal_all());
        self.links = None;
        // The windows of the faults, and a replica move's, end here. The split's fault does not.
        for id in 0..self.log.len() {
            if self.windows.contains_key(&id) {
                self.close(cluster, id);
            }
        }
        self.faults.clear();
        for gateway in std::mem::take(&mut self.certificates) {
            cluster.world.override_certificate(gateway, None);
        }
        if self.beyond_tcp {
            // A stalled direction stays stalled until its session resets.
            for connection in cluster.world.connections() {
                cluster.world.reset_connection(connection.id);
            }
        }
        for (node, exit) in cluster.reap_exited().await {
            cluster.checks.violate(
                &cluster.world,
                "ProcessExit",
                format!("n{node} exited on its own: {exit}"),
            );
        }
        for step in restarts {
            if let Step::Follow(follow) = step {
                self.follow(cluster, ctx, follow, true).await;
            }
        }
        let mut stuck = Vec::new();
        for node in WORKERS {
            if !cluster.is_running(node)
                && let Err(error) = cluster.restart_worker(node, DiskCut::Kill).await
            {
                stuck.push(format!("n{node}: {error}"));
            }
        }
        if !stuck.is_empty() {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("same-run restart failed after the heal: {stuck:?}"),
            );
            // Cold start only to keep collecting the history; the violation stands.
            cluster.record(format!(
                "nemesis heal falls back to a cold start: {stuck:?}"
            ));
            ctx.history.lock().unwrap().marker(
                &cluster.world,
                "heal-cold-start",
                json!({"stuck": stuck}),
            );
            self.heal_cold = true;
            self.before_full_stop(cluster).await;
            cluster
                .cold_start_staggered(
                    "heal cold start",
                    DiskCut::Kill,
                    [Duration::ZERO; 3],
                    COLD_LIMIT,
                )
                .await
                .map_err(|error| format!("heal cold start after {stuck:?}: {error}"))?;
            self.after_full_stop().await;
        }
        for gateway in GATEWAYS {
            if !cluster.is_running(gateway) {
                cluster
                    .restart_gateway(gateway)
                    .await
                    .map_err(|error| format!("heal restart n{gateway}: {error}"))?;
            }
        }
        for (id, burst) in std::mem::take(&mut self.bursts) {
            let _ = burst.await;
            self.burst_done(cluster, ctx, id);
        }
        for id in 0..self.log.len() {
            if self.log[id].action != Action::Split {
                self.close(cluster, id);
            }
        }
        Ok(())
    }

    /// After the heal: a split that did not publish under chaos must publish now (D07). The
    /// operator may replace a refused plan. The bound runs from `from`, so a wait for something
    /// else does not extend it. A split that published since the last tick is noted as well: the
    /// later stages aim at the leader of the range that serves the table now.
    /// Returns whether it was pending.
    pub(crate) async fn settle_split(&mut self, cluster: &mut Cluster, from: Duration) -> bool {
        let pending = self.split_pending();
        let deadline = from + SPLIT_LIMIT;
        while self.split_pending() && cluster.world.elapsed() < deadline {
            time::sleep(Duration::from_millis(250)).await;
        }
        if let Some((_, split)) = &mut self.split {
            split.stop().await;
        }
        self.split_published(cluster);
        if let Some((_, split)) = &self.split
            && split.published().is_none()
        {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!(
                    "the split did not publish within {SPLIT_LIMIT:?} after the heal: {}",
                    split.summary()
                ),
            );
        }
        pending
    }

    /// A split was requested in this run.
    pub(crate) fn split_requested(&self) -> bool {
        self.split.is_some()
    }

    /// A split was requested and has not published.
    pub(crate) fn split_pending(&self) -> bool {
        let split = self.split.as_ref();
        split.is_some_and(|(_, split)| split.published().is_none())
    }

    pub(crate) fn split_summary(&self) -> serde_json::Value {
        let split = self.split.as_ref();
        split.map_or(serde_json::Value::Null, |(_, split)| split.summary())
    }

    pub(crate) fn resets(&self) -> u64 {
        self.faults.resets()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, invariants::Invariants, nemesis::Selection};

    /// Re-review of #115: direct seeds 24 and 564. The split published during the heal, nothing
    /// noted it, and D05 paused the leader of the child that serves its key.
    #[test]
    fn a_split_that_published_after_the_last_tick_is_noted() {
        nabedra_sim::runtime::run(1, |world| async move {
            Config::sample(1).setup(&world);
            let trace: crate::invariants::TraceObserver = Arc::new(|_, _| {});
            let mut cluster = Cluster::new(world, Invariants::new(), 1, trace, |_, _, _| {})
                .await
                .unwrap();
            let faults = NetFaults::for_test(&cluster.world);
            let profile = Selection::parse(&[]).unwrap().profile(1);
            let mut g = Gen::from_seed(1);
            let mut chaos = Chaos::new(&cluster.world, profile, faults, 0, &mut g);
            let id = chaos.open(&cluster, Action::Split, &[]);
            let child = Uuid::from_u128(7);
            let result = json!({"state": "published", "children": [{"group_id": child}]});
            chaos.split = Some((id, Split::published_at(cluster.operator(), 5, result)));
            cluster.watch_range(Uuid::nil());
            let from = cluster.world.elapsed();
            assert!(!chaos.split_pending());
            assert!(!chaos.settle_split(&mut cluster, from).await);
            assert_eq!(cluster.range(), Some(child));
            assert_eq!(chaos.log[id].to, Some(5));
            assert_eq!(chaos.log[id].effects, [Effect::at(5)]);
        });
    }
}
