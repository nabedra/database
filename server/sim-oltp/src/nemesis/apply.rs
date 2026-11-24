//! Applies nemesis steps to a running cluster, and heals it at the end of the chaos window.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_rt::{
    fs::{Fault, FaultError, FsOp},
};
use nabedra_sim::{Nemesis, Profile, net::draw_partition};
use serde_json::json;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{Action, FollowUp, NetFaults, Step, window};
use crate::{
    bootstrap::{GATEWAYS, ROOT, WORKERS},
    disk::DiskCut,
    invariants::elapsed_ms,
    node::Cluster,
    workloads::clients::{self, Ctx},
};

/// Bound for a cold start after the last worker starts (design 3.6, O15).
pub(crate) const COLD_LIMIT: Duration = Duration::from_secs(120);
/// Bound for a split to publish once the cluster is healed.
const SPLIT_LIMIT: Duration = Duration::from_secs(60);
/// Follow-ups that cannot run yet are tried again after this pause.
const DEFER_MS: u64 = 500;
const RESTART_TRIES: u32 = 4;

/// A window in which two of three workers were frozen (D05).
#[derive(Clone, Copy, Debug)]
pub(crate) struct NoQuorum {
    pub from_ms: u64,
    pub to_ms: Option<u64>,
}

/// The nemesis of one run and what it did.
pub(crate) struct Chaos {
    scheduler: Nemesis<Step>,
    faults: NetFaults,
    /// Actions applied, by name, with how often.
    pub executed: BTreeMap<Action, u32>,
    pub skipped: BTreeMap<Action, u32>,
    pub no_quorum: Vec<NoQuorum>,
    /// Whether some worker lost its process while requests were in flight.
    pub leader_lost: bool,
    /// Whether a split published, and the split's ID while it is still pending.
    pub split_published: bool,
    split_pending: Option<Uuid>,
    split_boundary: i64,
    cold_done: bool,
    /// The heal needed a full cold start.
    pub heal_cold: bool,
    links_blocked: bool,
    restart_failures: BTreeMap<u64, u32>,
    certificates: BTreeSet<u64>,
    disks: BTreeSet<u64>,
    /// Every worker that had a disk fault armed.
    disk_targets: BTreeSet<u64>,
    /// Armed disk faults that returned EIO.
    pub disk_fired: u32,
    disk_armed: BTreeMap<u64, (FsOp, u64)>,
    /// Leader losses that hit writes in flight.
    pub leader_lost_with_writes: u32,
    beyond_tcp: bool,
    bursts: Vec<JoinHandle<()>>,
}

impl Chaos {
    pub(crate) fn new(profile: Profile<Action>, faults: NetFaults, start_ms: u64, g: &mut Gen) -> Self {
        let mut steps = Profile::new(profile.name, profile.gap_ms);
        for (action, weight) in profile.actions() {
            steps = steps.with(Step::Act(*action), weight);
        }
        Self {
            scheduler: Nemesis::new(steps, start_ms, g),
            faults,
            executed: BTreeMap::new(),
            skipped: BTreeMap::new(),
            no_quorum: Vec::new(),
            leader_lost: false,
            split_published: false,
            split_pending: None,
            split_boundary: 2,
            cold_done: false,
            heal_cold: false,
            links_blocked: false,
            restart_failures: BTreeMap::new(),
            certificates: BTreeSet::new(),
            disks: BTreeSet::new(),
            disk_targets: BTreeSet::new(),
            disk_fired: 0,
            disk_armed: BTreeMap::new(),
            leader_lost_with_writes: 0,
            beyond_tcp: false,
            bursts: Vec::new(),
        }
    }

    pub(crate) fn profile_name(&self) -> &'static str {
        self.scheduler.profile().name
    }

    /// Apply the steps that are due.
    pub(crate) async fn tick(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen) {
        let now = elapsed_ms(&cluster.world);
        for step in self.scheduler.poll(now, g) {
            match step {
                Step::Act(action) => self.act(cluster, ctx, g, action).await,
                Step::Follow(follow) => self.follow(cluster, follow, false).await,
            }
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
        self.scheduler.after(at, Step::Follow(follow));
    }

    /// The Data leader, or a worker drawn from the tape when no leader is known.
    async fn leader_or_any(&self, cluster: &mut Cluster, g: &mut Gen) -> Option<u64> {
        if let Some(leader) = cluster.leader().await {
            return Some(leader);
        }
        let candidates: Vec<u64> = WORKERS
            .into_iter()
            .filter(|node| cluster.is_running(*node) && !cluster.frozen().contains(node))
            .collect();
        (!candidates.is_empty()).then(|| *g.pick(&candidates))
    }

    async fn act(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen, action: Action) {
        let process = matches!(
            action,
            Action::CrashLeader
                | Action::PowerLeader
                | Action::DrainLeader
                | Action::DiskError
                | Action::Cold
                | Action::Split
        );
        if process && (!cluster.healthy() || self.links_blocked || !self.disks.is_empty()) {
            return self.skip(cluster, action, "cluster is degraded");
        }
        match action {
            Action::Clock => {
                let nodes: Vec<u64> = WORKERS.into_iter().chain(GATEWAYS).collect();
                let node = *g.pick(&nodes);
                let drift = g.range_i64(-200, 200);
                let jump = g.range_i64(-2_000, 2_000);
                cluster.world.set_drift_ppm(node, drift);
                let by = Duration::from_millis(jump.unsigned_abs());
                if jump < 0 {
                    cluster.world.jump_wall_backward(node, by);
                } else {
                    cluster.world.jump_wall_forward(node, by);
                }
                self.note(cluster, ctx, "clock", json!({"node": node, "drift_ppm": drift, "wall_jump_ms": jump}));
            }
            Action::FreezeFollower | Action::FreezeTwo => {
                if !cluster.frozen().is_empty() || !cluster.healthy() {
                    return self.skip(cluster, action, "a worker is already frozen or down");
                }
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let followers: Vec<u64> = WORKERS.into_iter().filter(|node| *node != leader).collect();
                let frozen = if action == Action::FreezeTwo {
                    followers
                } else {
                    vec![*g.pick(&followers)]
                };
                for &node in &frozen {
                    cluster.freeze(node);
                }
                if frozen.len() == 2 {
                    self.no_quorum.push(NoQuorum {
                        from_ms: elapsed_ms(&cluster.world),
                        to_ms: None,
                    });
                }
                let duration = window(g, 500, 4_000);
                self.note(cluster, ctx, action.name(), json!({"nodes": frozen, "leader": leader, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::Thaw(frozen));
            }
            Action::Partition => {
                if self.links_blocked {
                    return self.skip(cluster, action, "links already blocked");
                }
                let sides = draw_partition(g, &WORKERS);
                cluster.net().update_plan(|plan| plan.partition(&sides));
                self.links_blocked = true;
                let duration = window(g, 500, 5_000);
                self.note(cluster, ctx, "partition", json!({"sides": sides, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::HealLinks);
            }
            Action::LeaderMove => {
                if self.links_blocked {
                    return self.skip(cluster, action, "links already blocked");
                }
                let Some(leader) = cluster.leader().await else {
                    return self.skip(cluster, action, "no known leader");
                };
                cluster.net().update_plan(|plan| {
                    for other in WORKERS.into_iter().filter(|node| *node != leader) {
                        plan.block(leader, other);
                        plan.block(other, leader);
                    }
                });
                self.links_blocked = true;
                let duration = window(g, 1_500, 4_000);
                self.note(cluster, ctx, "leader-move", json!({"leader": leader, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::HealLinks);
            }
            Action::SessionDrop => {
                let frames = 1 + g.below(200);
                self.faults.drop_session_after(frames);
                self.note(cluster, ctx, "session-drop", json!({"after_frames": frames}));
            }
            Action::LostReply => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let per_mille = *g.pick(&[100, 300, 1_000]);
                self.faults.lose_replies(leader, per_mille);
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "lost-reply", json!({"worker": leader, "per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::LostHttpReply => {
                let gateway = *g.pick(&GATEWAYS);
                let per_mille = *g.pick(&[100, 300, 1_000]);
                self.faults.lose_http_replies(gateway, per_mille);
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "lost-http-reply", json!({"gateway": gateway, "per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::DeadlineEdge => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let deadline = ctx.timeout.saturating_sub(Duration::from_secs(1));
                let percent = 40 + g.below(56);
                let delay = deadline * u32::try_from(percent).unwrap() / 200;
                self.faults.delay(leader, delay);
                let duration = window(g, 500, 3_000);
                self.note(cluster, ctx, "deadline-edge", json!({"worker": leader, "delay_ms": delay.as_millis() as u64, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
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
                for connection in cluster.world.connections() {
                    if connection.client == Some(gateway)
                        && connection.server.is_some_and(|node| WORKERS.contains(&node))
                    {
                        cluster.world.reset_connection(connection.id);
                    }
                }
                self.certificates.insert(gateway);
                let duration = window(g, 500, 4_000);
                self.note(cluster, ctx, "auth-flap", json!({"gateway": gateway, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestoreCertificate(gateway));
            }
            Action::KillGateway => {
                let gateway = *g.pick(&GATEWAYS);
                if !GATEWAYS.iter().all(|node| cluster.is_running(*node)) {
                    return self.skip(cluster, action, "a gateway is already down");
                }
                cluster.kill_gateway(gateway).await;
                let duration = window(g, 300, 3_000);
                self.note(cluster, ctx, "kill-gateway", json!({"gateway": gateway, "restart_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestartGateway(gateway));
            }
            Action::CrashLeader | Action::PowerLeader => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let power = action == Action::PowerLeader;
                self.lost_leader(ctx);
                cluster
                    .crash(leader, if power { DiskCut::Power } else { DiskCut::Kill })
                    .await;
                let duration = window(g, 500, 5_000);
                self.note(cluster, ctx, action.name(), json!({"worker": leader, "restart_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestartWorker(leader, power));
            }
            Action::DrainLeader => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                self.lost_leader(ctx);
                let drained = cluster.drain(leader).await;
                cluster.crash(leader, DiskCut::Kill).await;
                let duration = window(g, 300, 3_000);
                self.note(cluster, ctx, "drain-leader", json!({"worker": leader, "drained": drained.as_ref().map_err(String::as_str).err(), "restart_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestartWorker(leader, false));
            }
            Action::LaneExhaust => {
                let gateway = g.below(2) as usize;
                let count = 16 + g.below(81);
                let first = 1 + g.below(ctx.keys as u64) as i64;
                self.bursts.push(nabedra_rt::task::spawn(clients::burst(
                    ctx.clone(),
                    gateway,
                    count,
                    first,
                )));
                self.note(cluster, ctx, "lane-exhaust", json!({"gateway": GATEWAYS[gateway], "requests": count}));
            }
            Action::Split => {
                if self.split_pending.is_some() || self.split_published {
                    return self.skip(cluster, action, "the range already split");
                }
                let boundary = 2 + g.below(ctx.keys.max(2) as u64 - 1) as i64;
                let id = cluster.split_id().await;
                self.split_pending = Some(id);
                self.note(cluster, ctx, "split", json!({"boundary": boundary, "id": id}));
                let table = Uuid::parse_str(&ctx.table).expect("table UUID");
                match cluster.split(id, table, boundary, Duration::from_secs(10)).await {
                    Ok(published) => {
                        self.split_published = true;
                        self.split_pending = None;
                        cluster.record(format!("nemesis split published {published}"));
                    }
                    Err(error) => {
                        cluster.record(format!("nemesis split pending: {error}"));
                        self.split_boundary = boundary;
                    }
                }
            }
            Action::DiskError => {
                let node = *g.pick(&WORKERS);
                let op = *g.pick(&[FsOp::SyncData, FsOp::SyncAll]);
                let nth = 1 + g.below(20);
                let Some(image) = cluster.world.volume(node, Path::new(ROOT)) else {
                    return self.skip(cluster, action, "no mounted volume");
                };
                image.clear_calls();
                image.inject(Fault::on(op).nth(nth).error(FaultError::Eio));
                self.disks.insert(node);
                self.disk_targets.insert(node);
                self.disk_armed.insert(node, (op, nth));
                let duration = window(g, 500, 3_000);
                self.note(cluster, ctx, "disk-error", json!({"worker": node, "op": format!("{op:?}"), "nth": nth, "recover_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearDisk(node));
            }
            Action::Cold => {
                if self.cold_done {
                    return self.skip(cluster, action, "one cold start per run");
                }
                self.cold_done = true;
                let power = g.bool();
                let delays = [(); 3].map(|()| window(g, 0, 3_000));
                self.note(cluster, ctx, "cold", json!({"power": power, "delays_ms": delays.map(|delay| delay.as_millis() as u64)}));
                self.leader_lost = true;
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                // A cold start abandons pending operator work, such as a split attempt.
                self.split_pending = None;
                if let Err(error) = cluster
                    .cold_start_staggered("cold action", cut, delays, COLD_LIMIT)
                    .await
                {
                    cluster
                        .checks
                        .violate(&cluster.world, "Liveness", format!("cold start: {error}"));
                }
            }
            Action::FrameDrop => {
                let per_mille = *g.pick(&[5, 20, 50]);
                self.faults.frame_drop(per_mille);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "frame-drop", json!({"per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::BlackHole => {
                let worker = *g.pick(&WORKERS);
                self.faults.black_hole(worker);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "black-hole", json!({"worker": worker, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::Reorder => {
                let max_ms = *g.pick(&[5, 30, 120]);
                self.faults.reorder(max_ms);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "reorder", json!({"max_ms": max_ms, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
        }
        *self.executed.entry(action).or_default() += 1;
    }

    async fn follow(&mut self, cluster: &mut Cluster, follow: FollowUp, healing: bool) {
        match follow {
            FollowUp::Thaw(nodes) => {
                for node in nodes {
                    cluster.thaw(node);
                }
                let now = elapsed_ms(&cluster.world);
                for window in &mut self.no_quorum {
                    window.to_ms.get_or_insert(now);
                }
            }
            FollowUp::HealLinks => {
                cluster.net().update_plan(|plan| plan.heal_links());
                self.links_blocked = false;
                cluster.record("nemesis heal links".into());
            }
            FollowUp::RestoreCertificate(gateway) => {
                cluster.world.override_certificate(gateway, None);
                self.certificates.remove(&gateway);
                cluster.record(format!("nemesis restore certificate n{gateway}"));
            }
            FollowUp::ClearNet(action) => self.faults.clear_kind(action),
            FollowUp::ClearDisk(node) => {
                // O29: a pool that saw an ambiguous sync error reopens through storage
                // recovery before it serves again, so the operator restarts the worker.
                if let Some(image) = cluster.world.volume(node, Path::new(ROOT)) {
                    self.count_fired(node, &image);
                    image.clear_faults();
                }
                self.disks.remove(&node);
                if cluster.is_running(node) {
                    cluster.crash(node, DiskCut::Kill).await;
                }
                Box::pin(self.follow(cluster, FollowUp::RestartWorker(node, false), healing))
                    .await;
            }
            FollowUp::RestartGateway(gateway) => {
                if cluster.is_running(gateway) {
                    return;
                }
                if let Err(error) = cluster.restart_gateway(gateway).await {
                    self.retry(cluster, FollowUp::RestartGateway(gateway), gateway, healing, error);
                }
            }
            FollowUp::RestartWorker(node, power) => {
                if cluster.is_running(node) {
                    return;
                }
                if !healing && (!cluster.frozen().is_empty() || self.links_blocked) {
                    // Retained restarts need the survivors' quorum.
                    let at = elapsed_ms(&cluster.world) + DEFER_MS;
                    self.scheduler.after(at, Step::Follow(FollowUp::RestartWorker(node, power)));
                    return;
                }
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                if let Err(error) = cluster.restart_worker(node, cut).await {
                    self.retry(cluster, FollowUp::RestartWorker(node, false), node, healing, error);
                }
            }
        }
    }

    fn retry(&mut self, cluster: &mut Cluster, follow: FollowUp, node: u64, healing: bool, error: String) {
        let failures = self.restart_failures.entry(node).or_default();
        *failures += 1;
        cluster.record(format!("nemesis restart n{node} failed ({failures}): {error}"));
        if healing || *failures >= RESTART_TRIES {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("n{node} did not restart: {error}"),
            );
            return;
        }
        let at = elapsed_ms(&cluster.world) + DEFER_MS * 4;
        self.scheduler.after(at, Step::Follow(follow));
    }

    /// Stop the nemesis, apply every pending follow-up, undo every open fault and restart
    /// whatever is down. The cluster is healthy afterwards unless a violation says why not.
    pub(crate) async fn heal(&mut self, cluster: &mut Cluster, ctx: &Ctx) -> Result<(), String> {
        let pending = self.scheduler.stop();
        cluster.record(format!("nemesis heal with {} follow-ups", pending.len()));
        ctx.history
            .lock()
            .unwrap()
            .marker(&cluster.world, "heal", json!({"follow_ups": pending.len()}));
        let (restarts, others): (Vec<_>, Vec<_>) = pending.into_iter().partition(|step| {
            matches!(
                step,
                Step::Follow(FollowUp::RestartWorker(..) | FollowUp::RestartGateway(_))
            )
        });
        for step in others {
            if let Step::Follow(follow) = step {
                self.follow(cluster, follow, true).await;
            }
        }
        for node in cluster.frozen().clone() {
            cluster.thaw(node);
        }
        cluster.net().update_plan(|plan| plan.heal_all());
        self.links_blocked = false;
        self.faults.clear();
        for gateway in std::mem::take(&mut self.certificates) {
            cluster.world.override_certificate(gateway, None);
        }
        for node in std::mem::take(&mut self.disks) {
            if let Some(image) = cluster.world.volume(node, Path::new(ROOT)) {
                self.count_fired(node, &image);
                image.clear_faults();
            }
        }
        if self.beyond_tcp {
            // A stalled direction stays stalled until its session resets.
            for connection in cluster.world.connections() {
                cluster.world.reset_connection(connection.id);
            }
        }
        for (node, exit) in cluster.reap_exited().await {
            cluster.record(format!("nemesis heal: n{node} exited {exit}"));
            if !self.disks_failed(node) {
                cluster.checks.violate(
                    &cluster.world,
                    "ProcessExit",
                    format!("n{node} exited without a fault that stops it: {exit}"),
                );
            }
        }
        for step in restarts {
            if let Step::Follow(follow) = step {
                self.follow(cluster, follow, true).await;
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
            cluster.record(format!("nemesis heal falls back to a cold start: {stuck:?}"));
            ctx.history.lock().unwrap().marker(
                &cluster.world,
                "heal-cold-start",
                json!({"stuck": stuck}),
            );
            self.leader_lost = true;
            self.heal_cold = true;
            self.split_pending = None;
            cluster
                .cold_start_staggered("heal cold start", DiskCut::Kill, [Duration::ZERO; 3], COLD_LIMIT)
                .await
                .map_err(|error| format!("heal cold start after {stuck:?}: {error}"))?;
        }
        for gateway in GATEWAYS {
            if !cluster.is_running(gateway) {
                cluster
                    .restart_gateway(gateway)
                    .await
                    .map_err(|error| format!("heal restart n{gateway}: {error}"))?;
            }
        }
        for burst in std::mem::take(&mut self.bursts) {
            let _ = burst.await;
        }
        Ok(())
    }

    fn disks_failed(&self, node: u64) -> bool {
        self.disk_targets.contains(&node)
    }

    /// A logged failure of the armed operation means the fault returned EIO.
    fn count_fired(&mut self, node: u64, image: &nabedra_rt::fs::FaultFs) {
        if let Some((op, _)) = self.disk_armed.remove(&node) {
            let failed = image.calls().iter().any(|call| call.op == op && !call.ok);
            self.disk_fired += u32::from(failed);
        }
    }

    fn lost_leader(&mut self, ctx: &Ctx) {
        self.leader_lost = true;
        if ctx.writes_in_flight.load(std::sync::atomic::Ordering::Acquire) > 0 {
            self.leader_lost_with_writes += 1;
        }
    }

    /// After the heal: a split that did not publish under chaos must publish now (D07).
    pub(crate) async fn finish_split(&mut self, cluster: &mut Cluster, ctx: &Ctx) {
        let Some(id) = self.split_pending.take() else {
            return;
        };
        let table = Uuid::parse_str(&ctx.table).expect("table UUID");
        match cluster.split(id, table, self.split_boundary, SPLIT_LIMIT).await {
            Ok(_) => self.split_published = true,
            Err(error) => cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("split after heal: {error}"),
            ),
        }
    }

    /// A full stop abandons a split attempt whose owner it fences.
    pub(crate) fn abandon_split(&mut self) {
        self.split_pending = None;
    }

    pub(crate) fn resets(&self) -> u64 {
        self.faults.resets()
    }

    pub(crate) fn ran(&self, action: Action) -> bool {
        self.executed.contains_key(&action)
    }
}
