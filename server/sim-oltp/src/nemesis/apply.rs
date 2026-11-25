//! Applies nemesis steps to a running cluster, and heals it at the end of the chaos window.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_rt::{
    fs::{Fault as FsFault, FaultError, FsOp},
    time,
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
    probes::Probe,
    workloads::{
        clauses::Fault,
        clients::{self, Ctx},
    },
};

/// Bound for a cold start after the last worker starts (design 3.6, O15).
pub(crate) const COLD_LIMIT: Duration = Duration::from_secs(120);
/// Bound for a split to publish once the cluster is healed.
const SPLIT_LIMIT: Duration = Duration::from_secs(60);
/// Follow-ups that cannot run yet are tried again after this pause.
const DEFER_MS: u64 = 500;
const RESTART_TRIES: u32 = 4;
/// Two of the executor's `RETRY_PAUSE`s (2 ms): too late for another attempt (design 3.5).
const LAST_ATTEMPT_US: u64 = 4_000;
/// How long a reply cut at a last attempt stays in place.
const LAST_ATTEMPT_CUT: Duration = Duration::from_millis(50);
/// How long a probe-placed fault waits for its protocol point.
const PLACE_LIMIT: Duration = Duration::from_secs(2);

/// A requested split that has not published.
struct PendingSplit {
    id: Uuid,
    boundary: i64,
    next_ms: u64,
    last: String,
    /// The last answer was a definite refusal, so the split had not published.
    refused: bool,
}

/// A split request that a full stop interrupted. Queued work is abandoned when every worker
/// stops, so the next run must not execute it.
struct AbandonedSplit {
    id: Uuid,
    boundary: i64,
    /// Whether its last answer before the stop was a definite refusal.
    refused: bool,
}

/// The nemesis of one run and what it did.
pub(crate) struct Chaos {
    scheduler: Nemesis<Step>,
    faults: NetFaults,
    /// Actions applied, by name, with how often.
    pub executed: BTreeMap<Action, u32>,
    pub skipped: BTreeMap<Action, u32>,
    /// Every applied fault window, for the scenario clauses.
    pub log: Vec<Fault>,
    split: Option<PendingSplit>,
    abandoned: Vec<AbandonedSplit>,
    split_requested: bool,
    cold_done: bool,
    /// The heal needed a full cold start.
    pub heal_cold: bool,
    links_blocked: bool,
    restart_failures: BTreeMap<u64, u32>,
    certificates: BTreeSet<u64>,
    disks: BTreeSet<u64>,
    disk_armed: BTreeMap<u64, (FsOp, u64)>,
    beyond_tcp: bool,
    bursts: Vec<JoinHandle<()>>,
}

impl Chaos {
    pub(crate) fn new(
        profile: Profile<Action>,
        faults: NetFaults,
        start_ms: u64,
        g: &mut Gen,
    ) -> Self {
        let mut steps = Profile::new(profile.name, profile.gap_ms);
        for (action, weight) in profile.actions() {
            steps = steps.with(Step::Act(*action), weight);
        }
        Self {
            scheduler: Nemesis::new(steps, start_ms, g),
            faults,
            executed: BTreeMap::new(),
            skipped: BTreeMap::new(),
            log: Vec::new(),
            split: None,
            abandoned: Vec::new(),
            split_requested: false,
            cold_done: false,
            heal_cold: false,
            links_blocked: false,
            restart_failures: BTreeMap::new(),
            certificates: BTreeSet::new(),
            disks: BTreeSet::new(),
            disk_armed: BTreeMap::new(),
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
        self.reap_bursts(cluster);
        self.try_split(cluster, ctx).await;
    }

    /// Start a fault window in the log.
    fn open(&mut self, cluster: &Cluster, action: Action, nodes: &[u64]) -> &mut Fault {
        self.log.push(Fault {
            action,
            nodes: nodes.to_vec(),
            from: elapsed_ms(&cluster.world),
            to: None,
            quiet: false,
            variant: "",
            request: None,
            observed: false,
        });
        self.log.last_mut().expect("pushed fault")
    }

    /// End the open window of one of `actions` on `node`, or on any node.
    fn close(
        &mut self,
        cluster: &Cluster,
        actions: &[Action],
        node: Option<u64>,
    ) -> Option<&mut Fault> {
        let now = elapsed_ms(&cluster.world);
        let fault = self.log.iter_mut().rev().find(|fault| {
            fault.to.is_none()
                && actions.contains(&fault.action)
                && node.is_none_or(|node| fault.nodes.contains(&node))
        })?;
        fault.to = Some(now);
        Some(fault)
    }

    /// Nothing else is open, a pending split included, and the cluster reports itself
    /// converged.
    async fn quiet(&self, cluster: &Cluster) -> bool {
        self.log.iter().all(|fault| fault.to.is_some())
            && !self.faults.open()
            && cluster.converged().await
    }

    fn reap_bursts(&mut self, cluster: &Cluster) {
        if self.bursts.iter().all(JoinHandle::is_finished) && !self.bursts.is_empty() {
            self.bursts.clear();
            while self.close(cluster, &[Action::LaneExhaust], None).is_some() {}
        }
    }

    /// One attempt at the pending split. The attempts interleave with the other faults.
    async fn try_split(&mut self, cluster: &Cluster, ctx: &Ctx) -> bool {
        let Some(split) = self.split.as_mut() else {
            return true;
        };
        let now = elapsed_ms(&cluster.world);
        if now < split.next_ms {
            return false;
        }
        split.next_ms = now + 250;
        let table = Uuid::parse_str(&ctx.table).expect("table UUID");
        match cluster.split(split.id, table, split.boundary).await {
            Ok(published) => {
                self.split = None;
                if let Some(fault) = self.close(cluster, &[Action::Split], None) {
                    fault.observed = true;
                }
                cluster.record(format!("nemesis split published {published}"));
                true
            }
            Err(error) => {
                if split.last != error {
                    cluster.record(format!("nemesis split pending: {error}"));
                }
                split.refused = error.contains("Conflict");
                split.last = error;
                false
            }
        }
    }

    /// Ask for the pending split once more, immediately before every worker stops.
    pub(crate) async fn before_full_stop(&mut self, cluster: &Cluster, ctx: &Ctx) {
        if let Some(split) = self.split.as_mut() {
            split.next_ms = 0;
            self.try_split(cluster, ctx).await;
        }
    }

    /// The stop abandoned the pending split. Request it again under a new ID.
    pub(crate) async fn after_full_stop(&mut self, cluster: &Cluster) {
        let Some(split) = self.split.as_mut() else {
            return;
        };
        let id = cluster.split_id().await;
        cluster.record(format!(
            "nemesis split {} abandoned by a full stop (refused before it: {}), requested again as {id}",
            split.id, split.refused
        ));
        self.abandoned.push(AbandonedSplit {
            id: std::mem::replace(&mut split.id, id),
            boundary: split.boundary,
            refused: split.refused,
        });
        split.refused = false;
        split.next_ms = 0;
    }

    /// A split that a full stop abandoned must not have published in the next run.
    pub(crate) async fn check_abandoned(&self, cluster: &Cluster, ctx: &Ctx) {
        let table = Uuid::parse_str(&ctx.table).expect("table UUID");
        for split in &self.abandoned {
            let answer = cluster.split(split.id, table, split.boundary).await;
            cluster.record(format!("abandoned split {} answers {answer:?}", split.id));
            if split.refused && answer.is_ok() {
                cluster.checks.violate(
                    &cluster.world,
                    "AbandonedSplitReplayed",
                    format!(
                        "split {} was refused before a full stop and published after it: {answer:?}",
                        split.id
                    ),
                );
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

    /// Wait until a probe shows that `node` (or any worker) applied a direct commit that a
    /// client still waits for: the Raft entry is committed, and no answer has left the leader.
    /// Returns the request and the gateway that carries it.
    async fn applied(cluster: &Cluster, ctx: &Ctx, node: Option<u64>) -> Option<(Uuid, u64)> {
        let mut applied = cluster.checks.probes()?.subscribe(move |event| {
            event.live
                && matches!(event.probe, Probe::DedupApplied { .. })
                && node.is_none_or(|node| event.node == Some(node))
        });
        time::timeout(PLACE_LIMIT, async {
            while let Some(event) = applied.recv().await {
                if let Probe::DedupApplied { request_id, .. } = event.probe
                    && let Some(gateway) = ctx.waiting(request_id)
                {
                    return Some((request_id, gateway));
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }

    /// Wait until a gateway sends an attempt of a request a client waits for with less than
    /// [`LAST_ATTEMPT_US`] left. Returns the request and the voter the attempt goes to.
    async fn last_attempt(cluster: &Cluster, ctx: &Ctx) -> Option<(Uuid, u64)> {
        let mut attempts = cluster.checks.probes()?.subscribe(move |event| {
            event.live
                && matches!(event.probe, Probe::GatewayAttempt { remaining_us, .. }
                    if remaining_us < LAST_ATTEMPT_US)
        });
        time::timeout(PLACE_LIMIT, async {
            while let Some(event) = attempts.recv().await {
                if let Probe::GatewayAttempt {
                    request_id, voter, ..
                } = event.probe
                    && ctx.waiting(request_id).is_some()
                {
                    return Some((request_id, voter));
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
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
        if process && (down || !self.disks.is_empty()) {
            return self.skip(cluster, action, "a worker is already down");
        }
        // A cold start blocks this loop, so no open window could end while it runs.
        if action == Action::Cold
            && (self.links_blocked || !cluster.frozen().is_empty() || self.faults.open())
        {
            return self.skip(cluster, action, "a fault window is open");
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
                self.open(cluster, action, &[node]).to = Some(elapsed_ms(&cluster.world));
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
                let Some(leader) = self.leader_or_any(cluster, g).await else {
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
                let fault = self.open(cluster, Action::FreezeFollower, &[first]);
                fault.quiet = quiet;
                let mut frozen = vec![first];
                if action == Action::FreezeTwo
                    && let Some(second) = followers.pop()
                {
                    time::sleep(window(g, 200, 2_000)).await;
                    self.close(cluster, &[Action::FreezeFollower], Some(first));
                    cluster.freeze(second);
                    frozen.push(second);
                    self.open(cluster, action, &frozen);
                }
                let duration = window(g, 500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    action.name(),
                    json!({"nodes": frozen, "leader": leader, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::Thaw(frozen));
            }
            Action::Partition => {
                if self.links_blocked {
                    return self.skip(cluster, action, "links already blocked");
                }
                let sides = draw_partition(g, &WORKERS);
                cluster.net().update_plan(|plan| plan.partition(&sides));
                self.links_blocked = true;
                let nodes: Vec<u64> = sides.iter().flatten().copied().collect();
                self.open(cluster, action, &nodes);
                let duration = window(g, 500, 5_000);
                self.note(
                    cluster,
                    ctx,
                    "partition",
                    json!({"sides": sides, "ms": duration.as_millis() as u64}),
                );
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
                self.open(cluster, action, &[leader]);
                let duration = window(g, 1_500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    "leader-move",
                    json!({"leader": leader, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::HealLinks);
            }
            Action::SessionDrop => {
                let frames = 1 + g.below(200);
                self.faults.drop_session_after(frames);
                self.open(cluster, action, &[]).to = Some(elapsed_ms(&cluster.world));
                self.note(
                    cluster,
                    ctx,
                    "session-drop",
                    json!({"after_frames": frames}),
                );
            }
            Action::LostReply => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let per_mille = *g.pick(&[100, 300, 1_000]);
                self.faults.lose_replies(leader, per_mille);
                self.open(cluster, action, &[leader]);
                let duration = window(g, 200, 2_000);
                self.note(cluster, ctx, "lost-reply", json!({"worker": leader, "per_mille": per_mille, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::LostHttpReply => {
                let gateway = *g.pick(&GATEWAYS);
                let per_mille = *g.pick(&[100, 300, 1_000]);
                self.faults.lose_http_replies(gateway, per_mille);
                self.open(cluster, action, &[gateway]);
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
                self.open(cluster, action, &[leader]);
                let duration = window(g, 500, 3_000);
                self.note(cluster, ctx, "deadline-edge", json!({"worker": leader, "delay_ms": delay.as_millis() as u64, "ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::ClearNet(action));
                // Cut the reply of an attempt that no retry can follow. A cut that is already
                // open keeps its own end.
                let cut = self
                    .log
                    .iter()
                    .any(|fault| fault.action == Action::LostReply && fault.to.is_none());
                if g.bool()
                    && !cut
                    && let Some((request, voter)) = Self::last_attempt(cluster, ctx).await
                {
                    // A moved replica has a new node ID, whose worker the probe doesn't name.
                    let worker = if WORKERS.contains(&voter) {
                        voter
                    } else {
                        leader
                    };
                    self.faults.lose_replies(worker, 1_000);
                    let fault = self.open(cluster, Action::LostReply, &[worker]);
                    fault.variant = "last-attempt";
                    fault.request = Some(request);
                    self.note(
                        cluster,
                        ctx,
                        "lost-reply",
                        json!({"worker": worker, "per_mille": 1_000, "request": request}),
                    );
                    *self.executed.entry(Action::LostReply).or_default() += 1;
                    self.later(
                        cluster,
                        LAST_ATTEMPT_CUT,
                        FollowUp::ClearNet(Action::LostReply),
                    );
                }
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
                        && connection
                            .server
                            .is_some_and(|node| WORKERS.contains(&node))
                    {
                        cluster.world.reset_connection(connection.id);
                    }
                }
                self.certificates.insert(gateway);
                self.open(cluster, action, &[gateway]);
                let duration = window(g, 500, 4_000);
                self.note(
                    cluster,
                    ctx,
                    "auth-flap",
                    json!({"gateway": gateway, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::RestoreCertificate(gateway));
            }
            Action::KillGateway => {
                let mut gateway = *g.pick(&GATEWAYS);
                // After the leader proposed and applied a request, before its answer (D02).
                let placed = if g.bool() {
                    Self::applied(cluster, ctx, None).await
                } else {
                    None
                };
                if let Some((_, waiting)) = placed {
                    gateway = waiting;
                }
                if !cluster.is_running(gateway) {
                    return self.skip(cluster, action, "the gateway is already down");
                }
                let fault = self.open(cluster, action, &[gateway]);
                if let Some((request, _)) = placed {
                    fault.variant = "after-propose";
                    fault.request = Some(request);
                }
                cluster.kill_gateway(gateway).await;
                let duration = window(g, 300, 3_000);
                self.note(
                    cluster,
                    ctx,
                    "kill-gateway",
                    json!({"gateway": gateway, "restart_after_ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::RestartGateway(gateway));
            }
            Action::CrashLeader | Action::PowerLeader => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                let power = action == Action::PowerLeader;
                let variant = *g.pick(&["", "before-quorum", "lost-replies", "after-quorum"]);
                let followers: Vec<u64> = WORKERS
                    .into_iter()
                    .filter(|node| *node != leader && !cluster.frozen().contains(node))
                    .collect();
                // Before quorum: writes sent while both followers are frozen cannot have
                // committed when the leader dies. Lost replies: the leader's answers are
                // cut first, so writes it committed are still unknown to their clients.
                match variant {
                    "before-quorum" => {
                        for &node in &followers {
                            cluster.freeze(node);
                        }
                        time::sleep(window(g, 100, 500)).await;
                    }
                    "lost-replies" => {
                        self.faults.lose_replies(leader, 1_000);
                        time::sleep(window(g, 50, 300)).await;
                    }
                    _ => {}
                }
                let placed = if variant == "after-quorum" {
                    Self::applied(cluster, ctx, Some(leader)).await
                } else {
                    None
                };
                let fault = self.open(cluster, action, &[leader]);
                fault.variant = variant;
                fault.request = placed.map(|(request, _)| request);
                cluster
                    .crash(leader, if power { DiskCut::Power } else { DiskCut::Kill })
                    .await;
                match variant {
                    "before-quorum" => {
                        for &node in &followers {
                            cluster.thaw(node);
                        }
                    }
                    "lost-replies" => self.faults.clear_kind(Action::LostReply),
                    _ => {}
                }
                let duration = window(g, 500, 5_000);
                self.note(cluster, ctx, action.name(), json!({"worker": leader, "variant": variant, "restart_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestartWorker(leader, power));
            }
            Action::DrainLeader => {
                let Some(leader) = self.leader_or_any(cluster, g).await else {
                    return self.skip(cluster, action, "no running worker");
                };
                // Between `Applied` and the reply: the lost responder.
                let placed = if g.bool() {
                    Self::applied(cluster, ctx, Some(leader)).await
                } else {
                    None
                };
                let fault = self.open(cluster, action, &[leader]);
                if let Some((request, _)) = placed {
                    fault.variant = "after-apply";
                    fault.request = Some(request);
                }
                let drained = cluster.drain(leader).await;
                cluster.crash(leader, DiskCut::Kill).await;
                if let Err(error) = &drained {
                    cluster.checks.violate(
                        &cluster.world,
                        "ProcessExit",
                        format!("drain of n{leader}: {error}"),
                    );
                }
                let duration = window(g, 300, 3_000);
                self.note(cluster, ctx, "drain-leader", json!({"worker": leader, "drained": drained.as_ref().map_err(String::as_str).err(), "restart_after_ms": duration.as_millis() as u64}));
                self.later(cluster, duration, FollowUp::RestartWorker(leader, false));
            }
            Action::LaneExhaust => {
                let gateway = g.below(2) as usize;
                let count = 64 + g.below(193);
                let quiet = self.quiet(cluster).await;
                self.open(cluster, action, &[GATEWAYS[gateway]]).quiet = quiet;
                let first = 1 + g.below(ctx.keys as u64) as i64;
                self.bursts.push(nabedra_rt::task::spawn(clients::burst(
                    ctx.clone(),
                    gateway,
                    count,
                    first,
                )));
                self.note(
                    cluster,
                    ctx,
                    "lane-exhaust",
                    json!({"gateway": GATEWAYS[gateway], "requests": count}),
                );
            }
            Action::Split => {
                if self.split_requested {
                    return self.skip(cluster, action, "the range already split");
                }
                self.split_requested = true;
                let boundary = 2 + g.below(ctx.keys.max(2) as u64 - 1) as i64;
                let id = cluster.split_id().await;
                self.open(cluster, action, &[]);
                self.note(
                    cluster,
                    ctx,
                    "split",
                    json!({"boundary": boundary, "id": id}),
                );
                self.split = Some(PendingSplit {
                    id,
                    boundary,
                    next_ms: 0,
                    last: String::new(),
                    refused: false,
                });
            }
            Action::DiskError => {
                let node = *g.pick(&WORKERS);
                let op = *g.pick(&[FsOp::SyncData, FsOp::SyncAll]);
                let nth = 1 + g.below(20);
                let Some(image) = cluster.world.volume(node, Path::new(ROOT)) else {
                    return self.skip(cluster, action, "no mounted volume");
                };
                image.clear_calls();
                image.inject(FsFault::on(op).nth(nth).error(FaultError::Eio));
                self.disks.insert(node);
                self.disk_armed.insert(node, (op, nth));
                self.open(cluster, action, &[node]);
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
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                self.before_full_stop(cluster, ctx).await;
                self.open(cluster, action, &WORKERS);
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
                self.close(cluster, &[action], None);
                self.after_full_stop(cluster).await;
            }
            Action::FrameDrop => {
                let per_mille = *g.pick(&[5, 20, 50]);
                self.faults.frame_drop(per_mille);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "frame-drop",
                    json!({"per_mille": per_mille, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::BlackHole => {
                let worker = *g.pick(&WORKERS);
                self.faults.black_hole(worker);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "black-hole",
                    json!({"worker": worker, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
            Action::Reorder => {
                let max_ms = *g.pick(&[5, 30, 120]);
                self.faults.reorder(max_ms);
                self.beyond_tcp = true;
                let duration = window(g, 200, 2_000);
                self.note(
                    cluster,
                    ctx,
                    "reorder",
                    json!({"max_ms": max_ms, "ms": duration.as_millis() as u64}),
                );
                self.later(cluster, duration, FollowUp::ClearNet(action));
            }
        }
        *self.executed.entry(action).or_default() += 1;
    }

    async fn follow(&mut self, cluster: &mut Cluster, follow: FollowUp, healing: bool) {
        match follow {
            FollowUp::Thaw(nodes) => {
                for &node in &nodes {
                    cluster.thaw(node);
                }
                self.close(
                    cluster,
                    &[Action::FreezeFollower, Action::FreezeTwo],
                    nodes.first().copied(),
                );
            }
            FollowUp::HealLinks => {
                cluster.net().update_plan(|plan| plan.heal_links());
                self.links_blocked = false;
                self.leader_moved(cluster).await;
                self.close(cluster, &[Action::Partition, Action::LeaderMove], None);
                cluster.record("nemesis heal links".into());
            }
            FollowUp::RestoreCertificate(gateway) => {
                cluster.world.override_certificate(gateway, None);
                self.certificates.remove(&gateway);
                self.close(cluster, &[Action::AuthFlap], Some(gateway));
                cluster.record(format!("nemesis restore certificate n{gateway}"));
            }
            FollowUp::ClearNet(action) => {
                self.faults.clear_kind(action);
                self.close(cluster, &[action], None);
            }
            FollowUp::ClearDisk(node) => {
                // O29: a pool that saw an ambiguous sync error reopens through storage
                // recovery before it serves again, so the operator restarts the worker.
                if let Some(image) = cluster.world.volume(node, Path::new(ROOT)) {
                    self.count_fired(cluster, node, &image);
                    image.clear_faults();
                }
                self.disks.remove(&node);
                if cluster.world.is_up(node) {
                    cluster.crash(node, DiskCut::Kill).await;
                }
                Box::pin(self.follow(cluster, FollowUp::RestartWorker(node, false), healing)).await;
            }
            FollowUp::RestartGateway(gateway) => {
                if cluster.is_running(gateway) {
                    return;
                }
                if let Err(error) = cluster.restart_gateway(gateway).await {
                    self.retry(
                        cluster,
                        FollowUp::RestartGateway(gateway),
                        gateway,
                        healing,
                        error,
                    );
                } else {
                    self.close(cluster, &[Action::KillGateway], Some(gateway));
                }
            }
            FollowUp::RestartWorker(node, power) => {
                if cluster.is_running(node) {
                    return;
                }
                if !healing
                    && (!cluster.frozen().is_empty() || self.links_blocked || self.faults.open())
                {
                    // Retained restarts need the survivors' quorum, and this loop cannot
                    // end a fault window while a restart blocks it.
                    let at = elapsed_ms(&cluster.world) + DEFER_MS;
                    self.scheduler
                        .after(at, Step::Follow(FollowUp::RestartWorker(node, power)));
                    return;
                }
                let cut = if power { DiskCut::Power } else { DiskCut::Kill };
                if let Err(error) = cluster.restart_worker(node, cut).await {
                    self.retry(
                        cluster,
                        FollowUp::RestartWorker(node, false),
                        node,
                        healing,
                        error,
                    );
                } else {
                    let losses = [
                        Action::CrashLeader,
                        Action::PowerLeader,
                        Action::DrainLeader,
                    ];
                    self.close(cluster, &losses, Some(node));
                    self.close(cluster, &[Action::DiskError], Some(node));
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
        cluster.record(format!(
            "nemesis restart n{node} failed ({failures}): {error}"
        ));
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
        ctx.history.lock().unwrap().marker(
            &cluster.world,
            "heal",
            json!({"follow_ups": pending.len()}),
        );
        self.leader_moved(cluster).await;
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
                self.count_fired(cluster, node, &image);
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
            cluster.checks.violate(
                &cluster.world,
                "ProcessExit",
                format!("n{node} exited on its own: {exit}"),
            );
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
            cluster.record(format!(
                "nemesis heal falls back to a cold start: {stuck:?}"
            ));
            ctx.history.lock().unwrap().marker(
                &cluster.world,
                "heal-cold-start",
                json!({"stuck": stuck}),
            );
            self.heal_cold = true;
            self.before_full_stop(cluster, ctx).await;
            cluster
                .cold_start_staggered(
                    "heal cold start",
                    DiskCut::Kill,
                    [Duration::ZERO; 3],
                    COLD_LIMIT,
                )
                .await
                .map_err(|error| format!("heal cold start after {stuck:?}: {error}"))?;
            self.after_full_stop(cluster).await;
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
        let now = elapsed_ms(&cluster.world);
        for fault in self
            .log
            .iter_mut()
            .filter(|fault| fault.action != Action::Split)
        {
            fault.to.get_or_insert(now);
        }
        Ok(())
    }

    /// A logged failure of the armed operation means the fault returned EIO.
    fn count_fired(&mut self, cluster: &Cluster, node: u64, image: &nabedra_rt::fs::FaultFs) {
        if let Some((op, _)) = self.disk_armed.remove(&node) {
            let failed = image.calls().iter().any(|call| call.op == op && !call.ok);
            cluster.record(format!("nemesis disk-error n{node} fired {failed}"));
            let armed = self
                .log
                .iter_mut()
                .rev()
                .find(|fault| fault.action == Action::DiskError && fault.nodes == [node]);
            if let Some(fault) = armed {
                fault.observed |= failed;
            }
        }
    }

    /// Whether another worker served writes while a `leader-move` was open.
    async fn leader_moved(&mut self, cluster: &mut Cluster) {
        let open = self
            .log
            .iter()
            .rposition(|fault| fault.action == Action::LeaderMove && fault.to.is_none());
        if let Some(index) = open {
            let now = cluster.leader().await;
            let fault = &mut self.log[index];
            fault.observed = now.is_some_and(|leader| fault.nodes != [leader]);
        }
    }

    /// After the heal: a split that did not publish under chaos must publish now (D07).
    pub(crate) async fn finish_split(&mut self, cluster: &Cluster, ctx: &Ctx) {
        let deadline = cluster.world.elapsed() + SPLIT_LIMIT;
        while !self.try_split(cluster, ctx).await {
            if cluster.world.elapsed() >= deadline {
                let last = self.split.as_ref().map(|split| split.last.clone());
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!(
                        "the split did not publish within {SPLIT_LIMIT:?} after the heal: {last:?}"
                    ),
                );
                return;
            }
            time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// A split was requested in this run.
    pub(crate) fn split_requested(&self) -> bool {
        self.split_requested
    }

    /// A split was requested and has not published.
    pub(crate) fn split_pending(&self) -> bool {
        self.split.is_some()
    }

    pub(crate) fn resets(&self) -> u64 {
        self.faults.resets()
    }

    pub(crate) fn session_drops(&self) -> Vec<u64> {
        self.faults.session_drops()
    }
}
