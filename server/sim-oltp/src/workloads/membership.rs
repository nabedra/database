//! The `membership` workload (design 3.6, phase 11): `direct` load while workers are restarted,
//! wiped, replaced and moved (O17, O26, R03, R04).
//!
//! Every worker has a key volume apart from its data volume. The workers start staggered. Then
//! the run executes a script that the seed draws:
//!
//! - `rolling`: every worker stops (kill, power loss or drain) and restarts on its retained
//!   mount, with queued requests outstanding at each stop. Each restarted worker is a joiner
//!   that must receive the queue state (R03).
//! - `moves`: the scheduler owner's Data replica moves to a second pool and a third. Each move is
//!   retried under its ID after it completed, also after a later move undid it, with another
//!   body, and next to requests that name a voter that is no member.
//! - `replace`: a worker is lost for good and a spare one takes its place through
//!   `worker.system_replacement.*` (`docs/oltp-recovery.md`), with a fault while it catches up.
//!   The lost worker's process then comes back on its intact disk and must stay out.
//! - `wipe`: a worker's data volume is lost and its keys are not. Its process starts again with
//!   the same identity. Another worker is then frozen: a write acknowledged in that window
//!   needed the vote of the wiped worker. The worker is then replaced.
//! - `cold`: every worker stops with queued requests outstanding (R04).
//! - `stale`: like `wipe` with an older image of the volume. Opt-in only (`STEPS_ENV`): nothing
//!   in the documented procedure allows it, and the rule that decides it needs the probes.
//!
//! The rules are in [`crate::invariants::membership`]. Public observations feed them on every
//! seed; they read no internal state of the product.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_rt::{fs::FaultFs, ids::uuid_v4, sim::Wipe, sim::in_node, time};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    Scenarios,
    clauses::Answer,
    clients::{self, Ctx, Plan},
    direct::{self, CONVERGE_LIMIT, Report},
    moves,
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, SPARES, WORKERS},
    client::{operation, request},
    config::Config,
    disk::{self, DiskCut},
    history::{Completion, History},
    invariants::{
        STEP, elapsed_ms,
        membership::{
            Capability, CapabilityBeforeUse, Certified, NamedChange, QueueHandoff, Receipt,
            RetryCertifiesNamedRemoval, WipedNeverVotes, required_voters,
        },
    },
    nemesis::{NetFaults, Selection, apply::COLD_LIMIT, window},
    node::{Cluster, Disk, Marker, Started},
};

/// Restricts the script to these steps, in this order, instead of drawing one.
pub(crate) const STEPS_ENV: &str = "NABEDRA_SIM_MEMBERSHIP";

/// A seeded defect in the harness's own run or observations, to show that a rule is wired to
/// what the workload sees. Never set in a sweep.
pub(crate) const DEFECT_ENV: &str = "NABEDRA_SIM_MEMBERSHIP_DEFECT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Defect {
    /// A rolling restart that stops every holder at once (`QueueHandoff`).
    FullStop,
    /// The published placement lacks one voter (`RequiredVoters`).
    DropVoter,
    /// The published placement lists a removed voter again (`RetryCertifiesNamedRemoval`).
    OldVoter,
}

impl Defect {
    fn from_env() -> Option<Self> {
        match std::env::var(DEFECT_ENV).ok().as_deref() {
            None | Some("") => None,
            Some("full-stop") => Some(Self::FullStop),
            Some("drop-voter") => Some(Self::DropVoter),
            Some("old-voter") => Some(Self::OldVoter),
            Some(name) => panic!("unknown {DEFECT_ENV} {name:?}"),
        }
    }
}

const POLL: Duration = Duration::from_millis(250);
/// An admin operation that may do work before it answers.
const ADMIN_LIMIT: Duration = Duration::from_secs(15);
/// A move may take as long as its acceptance test allows.
const MOVE_LIMIT: Duration = Duration::from_secs(90);
const MOVE_REQUEST: Duration = Duration::from_secs(75);
/// Survivors must drop a lost worker and prove their quorum within this time. The contract
/// gives no bound. This one, and the next, are the harness's.
const CHECK_LIMIT: Duration = Duration::from_secs(60);
/// A replacement must complete within this time after its last fault ended.
const REPLACE_LIMIT: Duration = Duration::from_secs(120);
/// How long a worker that must stay out is watched.
const REFUSAL_WATCH: Duration = Duration::from_secs(10);
/// How long one voter is frozen beside a wiped one.
const EVIDENCE: Duration = Duration::from_secs(20);
/// One attempt of a write in that window.
const EVIDENCE_ATTEMPT: Duration = Duration::from_secs(3);
/// Messages already on the wire when the voter froze have settled after this time.
const IN_FLIGHT: Duration = Duration::from_millis(500);
const RECEIPT_LIMIT: Duration = Duration::from_secs(30);
/// Queued requests insert keys of their own, outside the clients' key space.
const QUEUED_BASE: i64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Rolling,
    Moves,
    Replace,
    Wipe,
    Cold,
    Stale,
}

impl Step {
    const ALL: [Step; 6] = [
        Step::Rolling,
        Step::Moves,
        Step::Replace,
        Step::Wipe,
        Step::Cold,
        Step::Stale,
    ];

    fn name(self) -> &'static str {
        match self {
            Step::Rolling => "rolling",
            Step::Moves => "moves",
            Step::Replace => "replace",
            Step::Wipe => "wipe",
            Step::Cold => "cold",
            Step::Stale => "stale",
        }
    }

    /// The steps `STEPS_ENV` names. `None` draws them.
    pub(crate) fn from_env() -> Result<Option<Vec<Step>>, String> {
        match std::env::var(STEPS_ENV) {
            Ok(list) if !list.trim().is_empty() => Self::parse(&list).map(Some),
            _ => Ok(None),
        }
    }

    fn parse(list: &str) -> Result<Vec<Step>, String> {
        list.split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(|name| {
                Self::ALL
                    .into_iter()
                    .find(|step| step.name() == name)
                    .ok_or_else(|| format!("unknown {STEPS_ENV} entry {name:?}"))
            })
            .collect()
    }

    /// The script of a seed. A move needs the original workers, so it comes first.
    fn draw(g: &mut Gen) -> Vec<Step> {
        let mut steps = Vec::new();
        if g.below(3) == 0 {
            steps.push(Step::Moves);
        }
        if g.bool() {
            steps.push(Step::Rolling);
        }
        match g.below(4) {
            0 => steps.push(Step::Replace),
            // A wiped worker is replaced afterwards too.
            1 | 2 => steps.push(Step::Wipe),
            _ => {}
        }
        if g.below(3) == 0 {
            steps.push(Step::Replace);
        }
        if g.below(3) == 0 {
            steps.push(Step::Rolling);
        }
        if g.below(4) == 0 {
            steps.push(Step::Cold);
        }
        if steps.is_empty() {
            steps.push(Step::Rolling);
        }
        steps
    }
}

/// A queued insert that the cluster accepted.
#[derive(Clone, Copy)]
struct Queued {
    key: i64,
    value: i64,
    accepted_ms: u64,
}

/// What a fault during the catch-up of a replacement does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatchUpFault {
    None,
    FreezeSurvivor,
    KillCandidate,
    PowerCandidate,
    SlowSurvivors,
    DropSessions,
}

pub(crate) struct Membership {
    steps: Vec<Step>,
    delays: [Duration; 3],
    capability: CapabilityBeforeUse,
    wiped: WipedNeverVotes,
    removal: RetryCertifiesNamedRemoval,
    queue: QueueHandoff,
    queued: BTreeMap<Uuid, Queued>,
    /// Every queued request the run ever had accepted, for the final row check.
    succeeded: BTreeMap<Uuid, Queued>,
    /// The storage generation each worker runs on.
    generations: BTreeMap<u64, u64>,
    spares: Vec<u64>,
    /// Workers whose process may end on its own.
    expected_exit: BTreeSet<u64>,
    /// The pools that moves mounted: the worker and the device.
    second_pools: BTreeSet<(u64, u64)>,
    /// Workers whose resources were captured.
    baselines: BTreeSet<u64>,
    clauses: Scenarios,
    enabled: bool,
    next_sample_ms: u64,
    next_key: i64,
    notes: BTreeMap<&'static str, u64>,
    defect: Option<Defect>,
}

impl Membership {
    pub(crate) fn draw(g: &mut Gen) -> Self {
        let steps = Step::from_env()
            .expect("checked when the sweep started")
            .unwrap_or_else(|| Step::draw(g));
        Self {
            steps,
            delays: [(); 3].map(|()| window(g, 0, 2_000)),
            capability: CapabilityBeforeUse::default(),
            wiped: WipedNeverVotes::default(),
            removal: RetryCertifiesNamedRemoval::default(),
            queue: QueueHandoff::new(Uuid::nil()),
            queued: BTreeMap::new(),
            succeeded: BTreeMap::new(),
            generations: BTreeMap::new(),
            spares: SPARES.into_iter().rev().collect(),
            expected_exit: BTreeSet::new(),
            second_pools: BTreeSet::new(),
            baselines: BTreeSet::new(),
            clauses: Scenarios::new(),
            enabled: false,
            next_sample_ms: 0,
            next_key: QUEUED_BASE,
            notes: BTreeMap::new(),
            defect: Defect::from_env(),
        }
    }

    /// Start the cluster on split volumes, staggered, and check that no capability is in use
    /// before every voter ran.
    pub(crate) async fn start(&mut self, cluster: &mut Cluster) -> Result<(), String> {
        cluster.split_volumes();
        let delays = self.delays;
        let capability = &mut self.capability;
        let mut early = 0;
        let started = cluster
            .start_staggered(delays, async |cluster| {
                early += observe_capability(cluster, capability).await;
            })
            .await;
        self.notes.insert("early capability answers", early);
        started
    }

    pub(crate) async fn run(
        mut self,
        cluster: &mut Cluster,
        config: &Config,
        history: Arc<Mutex<History>>,
        g: &mut Gen,
        seed: u64,
        selection: &Selection,
        quiet: bool,
    ) -> Result<Report, String> {
        cluster.wait_direct().await?;
        let table = direct::create_table(cluster, &history).await?;
        for node in WORKERS {
            self.baseline(cluster, node).await?;
            self.generations.insert(node, 0);
        }
        self.queue = QueueHandoff::new(cluster.config.run_id);
        self.system_votes(cluster);
        let plan = Plan::draw(g, selection.focus.contention, selection.focus.idle);
        let ctx = Ctx::new(
            cluster.world.clone(),
            cluster.checks.clone(),
            history,
            table,
            cluster.config.data_token.clone(),
            config.deadline(),
            plan.keys,
        );
        let faults = NetFaults::install(cluster.net(), &cluster.world, seed);
        config.jump_clocks(&cluster.world);
        let names: Vec<_> = self.steps.iter().map(|step| step.name()).collect();
        cluster.record(format!(
            "workload membership steps {} clients {} keys {}",
            names.join(","),
            plan.programs.len(),
            plan.keys
        ));
        let tasks: Vec<_> = plan
            .programs
            .into_iter()
            .map(|program| nabedra_rt::task::spawn(clients::run(ctx.clone(), program)))
            .collect();

        if !quiet && g.bool() {
            let (per_mille, max) = (1 + g.below(20), window(g, 1, 20));
            cluster.record(format!(
                "storage pauses {per_mille} per mille up to {max:?}"
            ));
            cluster.world.set_storage_pauses(per_mille as u32, max);
        }
        let mut done = Ok(());
        for step in self.steps.clone() {
            self.observe_placement(cluster, &ctx).await;
            done = self.step(step, cluster, &ctx, g, &faults, quiet).await;
            if let Err(error) = &done {
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("step {}: {error}", step.name()),
                );
                break;
            }
            cluster.record(format!("step {} done", step.name()));
            self.pause(cluster, &ctx, window(g, 500, 2_500)).await;
        }
        ctx.stopping.store(true, Ordering::Release);
        faults.clear();
        cluster.world.set_storage_pauses(0, Duration::ZERO);
        for node in cluster.frozen().clone() {
            cluster.thaw(node);
        }
        let mut report = Report::default();
        report
            .assertions
            .push(("every membership step finished", done.is_ok()));
        if done.is_err() {
            ctx.converge_by.store(0, Ordering::Release);
            for task in tasks {
                task.abort();
                let _ = task.await;
            }
            report.scenarios = std::mem::take(&mut self.clauses);
            report.summary = self.summary(&ctx, &names);
            cluster.record("workload abandoned after a liveness violation".into());
            return Ok(report);
        }

        let healed = cluster.world.elapsed();
        cluster
            .checks
            .expect_recovery(&cluster.world, "convergence", CONVERGE_LIMIT);
        let converge_by = elapsed_ms(&cluster.world) + CONVERGE_LIMIT.as_millis() as u64;
        ctx.converge_by.store(converge_by, Ordering::Release);
        let converged = direct::converge(cluster, &ctx, healed + CONVERGE_LIMIT).await;
        report
            .assertions
            .push(("converged after the last step", converged.is_ok()));
        if let Err(error) = &converged {
            cluster
                .checks
                .violate(&cluster.world, "Liveness", format!("convergence: {error}"));
        }
        for task in tasks {
            let _ = task.await;
        }
        cluster.checks.recovered(&cluster.world, "convergence");
        self.settle_receipts(cluster, &ctx, RECEIPT_LIMIT).await;
        self.check_queued_rows(cluster, &ctx).await;
        let final_checks = clients::final_checks(&ctx).await;
        report.assertions.push((
            "final writes and reads through both gateways",
            final_checks.is_ok(),
        ));
        if let Err(error) = final_checks {
            cluster.checks.violate(&cluster.world, "Liveness", error);
        }
        self.observe_placement(cluster, &ctx).await;
        self.final_capability(cluster).await;
        let unresolved = ctx.observed.lock().unwrap().unresolved.clone();
        report
            .assertions
            .push(("every request resolved", unresolved.is_empty()));
        for request in unresolved {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("request unresolved {CONVERGE_LIMIT:?} after the last step: {request}"),
            );
        }
        self.check_resources(cluster).await?;
        report.scenarios = std::mem::take(&mut self.clauses);
        report.summary = self.summary(&ctx, &names);
        cluster.record(format!("workload summary {}", report.summary));
        Ok(report)
    }

    fn summary(&self, ctx: &Ctx, steps: &[&str]) -> Value {
        let observed = ctx.observed.lock().unwrap();
        json!({
            "workload": "membership",
            "steps": steps,
            "keys": ctx.keys,
            "completed": observed.completed,
            "attempts": observed.attempts.len(),
            "queued_succeeded": self.succeeded.len(),
            "notes": self.notes,
        })
    }

    async fn step(
        &mut self,
        step: Step,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        faults: &NetFaults,
        quiet: bool,
    ) -> Result<(), String> {
        cluster.record(format!("step {} starts", step.name()));
        match step {
            Step::Rolling => self.rolling(cluster, ctx, g).await,
            Step::Moves => self.moves(cluster, ctx, g, faults, quiet).await,
            Step::Replace => {
                let index = g.below(3) as usize;
                self.enable(cluster).await?;
                cluster
                    .crash(cluster.config.node(index), DiskCut::Kill)
                    .await;
                self.replace(cluster, ctx, g, faults, quiet, index).await
            }
            Step::Wipe => {
                let index = g.below(3) as usize;
                self.enable(cluster).await?;
                self.wipe(cluster, ctx, g, index).await?;
                self.replace(cluster, ctx, g, faults, quiet, index).await
            }
            Step::Cold => self.cold(cluster, ctx, g).await,
            Step::Stale => {
                let index = g.below(3) as usize;
                self.stale(cluster, ctx, g, index).await
            }
        }
    }

    // Observation.

    fn note(&mut self, what: &'static str) {
        *self.notes.entry(what).or_default() += 1;
    }

    fn judge(&self, cluster: &Cluster, invariant: &str, verdict: Result<(), String>) {
        if let Err(detail) = verdict {
            cluster.checks.violate(&cluster.world, invariant, detail);
        }
    }

    /// Sleep while sampling what the rules watch.
    async fn pause(&mut self, cluster: &mut Cluster, ctx: &Ctx, by: Duration) {
        let until = cluster.world.elapsed() + by;
        while cluster.world.elapsed() < until {
            self.sample(cluster, ctx).await;
            time::sleep(POLL.min(until.saturating_sub(cluster.world.elapsed()))).await;
        }
    }

    /// Once a virtual second: processes, capabilities and the published placement.
    async fn sample(&mut self, cluster: &mut Cluster, ctx: &Ctx) {
        let now = elapsed_ms(&cluster.world);
        if now < self.next_sample_ms {
            return;
        }
        self.next_sample_ms = now + 1_000;
        for (node, result) in cluster.reap_exited().await {
            if !self.expected_exit.contains(&node) {
                cluster.checks.violate(
                    &cluster.world,
                    "ProcessExit",
                    format!("n{node} exited on its own: {result}"),
                );
            }
        }
        observe_capability(cluster, &mut self.capability).await;
        self.observe_placement(cluster, ctx).await;
    }

    /// The published voters of the table's range. An unavailable answer says nothing.
    async fn observe_placement(&mut self, cluster: &Cluster, ctx: &Ctx) -> Option<Value> {
        let (group, placement) = moves::placement(cluster, ctx).await.ok()?;
        let mut members = placement["members"].as_array().cloned().unwrap_or_default();
        if self.defect == Some(Defect::DropVoter) {
            members.pop();
        }
        self.judge(cluster, "RequiredVoters", required_voters(3, &members));
        let group = group_id(&group);
        let mut published = nodes(&members);
        if self.defect == Some(Defect::OldVoter) {
            published.insert(1);
            published.insert(2);
            published.insert(3);
        }
        let verdict = self.removal.published(group, &published);
        self.judge(cluster, "RetryCertifiesNamedRemoval", verdict);
        // A published voter whose worker never lost its storage voted from it.
        for member in &members {
            if let Some(node) = worker_node(cluster, member)
                && self.generations.get(&node) == Some(&0)
                && let Some(replica) = member["node_id"].as_u64()
            {
                let verdict = self.wiped.voted(group, replica, 0);
                self.judge(cluster, "WipedNeverVotes", verdict);
            }
        }
        Some(placement)
    }

    /// The system groups' voters that never lost their storage voted from it.
    fn system_votes(&mut self, cluster: &Cluster) {
        for (_, group, members) in cluster.config.system_groups() {
            for member in &members {
                if let Some(node) = worker_node(cluster, member)
                    && self.generations.get(&node) == Some(&0)
                    && let Some(replica) = member["node_id"].as_u64()
                {
                    let verdict = self.wiped.voted(group, replica, 0);
                    self.judge(cluster, "WipedNeverVotes", verdict);
                }
            }
        }
    }

    /// Each voter of a group, with the storage generation it runs on while it can take part.
    fn taking_part(
        &self,
        cluster: &Cluster,
        members: &[Value],
        frozen: u64,
    ) -> Vec<(u64, Option<u64>)> {
        members
            .iter()
            .filter_map(|member| {
                let replica = member["node_id"].as_u64()?;
                let generation = worker_node(cluster, member)
                    .filter(|node| *node != frozen && cluster.is_running(*node))
                    .and_then(|node| self.generations.get(&node).copied());
                Some((replica, generation))
            })
            .collect()
    }

    async fn final_capability(&mut self, cluster: &Cluster) {
        for node in cluster.config.nodes() {
            let Ok(stats) = cluster.stats(node).await else {
                continue;
            };
            if stats["direct_capability"]["active"] == json!(false) {
                let verdict = self.capability.inactive(Capability::Direct);
                self.judge(cluster, "CapabilityBeforeUse", verdict);
            }
        }
    }

    async fn baseline(&mut self, cluster: &Cluster, node: u64) -> Result<(), String> {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
        self.baselines.insert(node);
        Ok(())
    }

    /// `ResourceBaseline` for every worker that is still part of the cluster. A replacement's
    /// baseline was taken when it became ready, so a leak before that is not seen.
    async fn check_resources(&mut self, cluster: &Cluster) -> Result<(), String> {
        let deadline = cluster.world.elapsed() + Duration::from_secs(2);
        loop {
            let (mut clean, mut snapshots) = (true, Vec::new());
            for node in cluster.config.nodes() {
                let mut stats = cluster.stats(node).await?;
                let pools = &self.second_pools;
                if let Some(rows) = stats["storage_reservations"].as_array_mut() {
                    rows.retain(|row| {
                        row["device"]
                            .as_u64()
                            .is_none_or(|device| !pools.contains(&(node, device)))
                    });
                }
                clean &= cluster.checks.resources_at_baseline(node, &stats);
                snapshots.push((node, stats));
            }
            if clean || cluster.world.elapsed() >= deadline {
                for (node, stats) in &snapshots {
                    cluster.checks.check_resources(&cluster.world, *node, stats);
                }
                cluster.checks.check_direct_path(&cluster.world, &snapshots);
                return Ok(());
            }
            time::sleep(STEP).await;
        }
    }

    // Queued requests (R03, R04).

    /// Submit `count` queued inserts, without waiting for their results.
    async fn submit(&mut self, cluster: &Cluster, ctx: &Ctx, count: u64) {
        for offset in 0..count {
            let key = self.next_key;
            self.next_key += 1;
            let node = CLIENTS[(offset as usize) % CLIENTS.len()];
            let gateway = GATEWAYS[(offset as usize) % GATEWAYS.len()];
            let id = in_node(node, async { uuid_v4() }).await;
            let value = key * 10;
            let body = operation(
                id,
                "table.insert",
                json!({"table_id": ctx.table, "row": {"id": key, "value": value}}),
            );
            let reply = request(
                node,
                gateway,
                "/v1/requests",
                &ctx.token,
                Some(body),
                ADMIN_LIMIT,
            )
            .await;
            match reply {
                Ok(reply)
                    if matches!(reply.status, 200 | 202)
                        && reply.body["request_id"] == json!(id)
                        && reply.body["run_id"] == json!(cluster.config.run_id) =>
                {
                    self.queue.accepted(id);
                    self.queued.insert(
                        id,
                        Queued {
                            key,
                            value,
                            accepted_ms: elapsed_ms(&cluster.world),
                        },
                    );
                    self.note("queued accepted");
                }
                Ok(reply) => {
                    cluster.record(format!("queued {id} not accepted: {}", reply.status));
                    self.note("queued not accepted");
                }
                Err(error) => {
                    cluster.record(format!("queued {id} not accepted: {error}"));
                    self.note("queued not accepted");
                }
            }
        }
    }

    /// One receipt through `gateway`, by its state. An abandoned request answers `not_found`
    /// and says so.
    async fn receipt(
        &self,
        cluster: &Cluster,
        ctx: &Ctx,
        gateway: u64,
        run: Uuid,
        id: Uuid,
    ) -> Option<String> {
        let path = format!("/v1/requests/{run}/{id}");
        let reply = request(CLIENTS[0], gateway, &path, &ctx.token, None, ADMIN_LIMIT)
            .await
            .ok()?;
        if reply.status == 200 {
            return reply.body["state"].as_str().map(str::to_owned);
        }
        let error = &reply.body["error"];
        cluster.record(format!("receipt {run}/{id}: {} {error}", reply.status));
        (error["code"] == "not_found"
            && error["message"]
                .as_str()
                .is_some_and(|message| message.contains("abandoned")))
        .then(|| "abandoned".to_owned())
    }

    /// Poll every outstanding receipt once. Returns whether none is left.
    async fn poll_receipts(&mut self, cluster: &Cluster, ctx: &Ctx) -> bool {
        let run = cluster.config.run_id;
        for (index, id) in self.queue.outstanding().into_iter().enumerate() {
            let gateway = GATEWAYS[index % GATEWAYS.len()];
            let Some(state) = self.receipt(cluster, ctx, gateway, run, id).await else {
                continue;
            };
            let now = match state.as_str() {
                "succeeded" => Receipt::Succeeded,
                "abandoned" => Receipt::Abandoned,
                "failed" => Receipt::Failed,
                _ => Receipt::Outstanding,
            };
            if now == Receipt::Succeeded
                && let Some(queued) = self.queued.get(&id)
            {
                self.succeeded.insert(id, *queued);
            }
            let verdict = self.queue.receipt(id, now);
            self.judge(cluster, "QueueHandoff", verdict);
        }
        self.queue.outstanding().is_empty()
    }

    async fn settle_receipts(&mut self, cluster: &mut Cluster, ctx: &Ctx, limit: Duration) {
        let deadline = cluster.world.elapsed() + limit;
        while !self.poll_receipts(cluster, ctx).await {
            if cluster.world.elapsed() >= deadline {
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!(
                        "accepted requests not settled within {limit:?}: {:?}",
                        self.queue.outstanding()
                    ),
                );
                return;
            }
            self.pause(cluster, ctx, POLL).await;
        }
    }

    /// The row of every queued insert that was reported as succeeded is there.
    async fn check_queued_rows(&mut self, cluster: &Cluster, ctx: &Ctx) {
        for (id, queued) in &self.succeeded {
            let read = in_node(CLIENTS[0], async { uuid_v4() }).await;
            let answer = request(
                CLIENTS[0],
                GATEWAYS[0],
                "/v1/operations",
                &ctx.token,
                Some(operation(
                    read,
                    "table.get",
                    json!({"table_id": ctx.table, "key": queued.key}),
                )),
                ctx.timeout,
            )
            .await
            .and_then(|reply| reply.completion(read));
            match answer {
                Ok(Completion::Ok(row)) if row["value"] == json!(queued.value) => {}
                Ok(Completion::Ok(row)) => cluster.checks.violate(
                    &cluster.world,
                    "QueueHandoff",
                    format!(
                        "request {id} succeeded and its row {} reads {row}",
                        queued.key
                    ),
                ),
                Ok(Completion::Refused(error)) => cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("read of queued row {}: {}", queued.key, error.message),
                ),
                Ok(Completion::Unknown(error)) | Err(error) => cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("read of queued row {}: {error}", queued.key),
                ),
            }
        }
    }

    /// Whether some request accepted before `at` was outstanding then and settled since.
    fn handed_over(&self, at: u64, before: &[Uuid]) -> bool {
        before.iter().any(|id| {
            self.queued
                .get(id)
                .is_some_and(|queued| queued.accepted_ms <= at)
                && self.succeeded.contains_key(id)
        })
    }

    // Steps.

    async fn rolling(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
    ) -> Result<(), String> {
        let mut order = [0, 1, 2];
        for last in (1..order.len()).rev() {
            order.swap(last, g.below(last as u64 + 1) as usize);
        }
        for index in order {
            let node = cluster.config.node(index);
            self.submit(cluster, ctx, 1 + g.below(3)).await;
            if self.defect == Some(Defect::FullStop) {
                cluster
                    .cold_start_staggered(
                        "seeded full stop",
                        DiskCut::Kill,
                        [Duration::ZERO; 3],
                        COLD_LIMIT,
                    )
                    .await?;
                let run = self.queue.run(cluster.config.run_id);
                self.judge(cluster, "QueueHandoff", run);
            }
            let outstanding = self.queue.outstanding();
            let stopped = elapsed_ms(&cluster.world);
            let how = if g.below(4) == 0 {
                if let Err(error) = cluster.drain(node).await {
                    return Err(format!("drain n{node}: {error}"));
                }
                DiskCut::Kill
            } else {
                self.cut(cluster, g, node).await
            };
            self.down(cluster, ctx, window(g, 0, 2_000)).await;
            if let Err(error) = cluster.prepare_restart(node).await {
                // `docs/oltp-recovery.md`, step 7: "Once queue join begins, a process restart
                // requires the normal retained-mount procedure with a new incarnation."
                if SPARES.contains(&node) {
                    cluster.checks.violate(
                        &cluster.world,
                        "ReplacementRestart",
                        format!("the survivors refuse the retained restart of the replacement n{node}: {error}"),
                    );
                }
                return Err(error);
            }
            cluster.start_prepared(index, how).await?;
            self.wait_members(cluster, ctx, CHECK_LIMIT).await?;
            let connection = self.connection(cluster).await?;
            let run = connection["run_id"]
                .as_str()
                .and_then(|run| Uuid::parse_str(run).ok())
                .unwrap_or_default();
            self.judge(cluster, "QueueHandoff", self.queue.run(run));
            self.settle_receipts(cluster, ctx, RECEIPT_LIMIT).await;
            if self.handed_over(stopped, &outstanding) {
                self.clauses.insert("R03:rolling-restart");
            }
        }
        Ok(())
    }

    /// Stop `node` without warning: a kill or a power loss, one half of them at a sync.
    async fn cut(&mut self, cluster: &mut Cluster, g: &mut Gen, node: u64) -> DiskCut {
        let how = if g.bool() {
            DiskCut::Kill
        } else {
            DiskCut::Power
        };
        match g.below(4) {
            0 | 1 => cluster.crash(node, how).await,
            side => {
                let at = cluster
                    .crash_at_sync(node, how, side == 3, Duration::from_millis(500))
                    .await;
                self.note(if at {
                    "cut at a sync"
                } else {
                    "no sync to cut"
                });
            }
        }
        how
    }

    /// Sleep with one worker down: `sample` must not take its absence for an exit.
    async fn down(&mut self, cluster: &mut Cluster, ctx: &Ctx, by: Duration) {
        self.pause(cluster, ctx, by).await;
    }

    async fn connection(&self, cluster: &Cluster) -> Result<Value, String> {
        let deadline = cluster.world.elapsed() + CHECK_LIMIT;
        loop {
            match cluster.admin("cluster.connection", json!({})).await {
                Ok(connection) => return Ok(connection),
                Err(error) if cluster.world.elapsed() >= deadline => return Err(error),
                Err(_) => time::sleep(POLL).await,
            }
        }
    }

    /// Wait until every current worker reports all of them ready.
    async fn wait_members(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        limit: Duration,
    ) -> Result<(), String> {
        let deadline = cluster.world.elapsed() + limit;
        loop {
            let nodes = cluster.config.nodes();
            let mut waiting = Vec::new();
            for node in nodes {
                if !cluster.is_ready(node, nodes.len()).await {
                    waiting.push(node);
                }
            }
            if waiting.is_empty() {
                return Ok(());
            }
            if cluster.world.elapsed() >= deadline {
                return Err(format!("{waiting:?} do not report three ready workers"));
            }
            self.pause(cluster, ctx, POLL).await;
        }
    }

    /// Enable system replacement while every voter is up, as the procedure requires.
    async fn enable(&mut self, cluster: &mut Cluster) -> Result<(), String> {
        if self.enabled {
            return Ok(());
        }
        let id = cluster.split_id().await;
        self.leader_admin(
            cluster,
            id,
            "worker.system_replacement.enable",
            json!({}),
            CHECK_LIMIT,
        )
        .await?;
        self.enabled = true;
        Ok(())
    }

    /// An admin operation that only the Control leader answers, under one ID, on every worker
    /// in turn until one of them accepts it.
    async fn leader_admin(
        &self,
        cluster: &Cluster,
        id: Uuid,
        name: &str,
        payload: Value,
        limit: Duration,
    ) -> Result<Value, String> {
        let deadline = cluster.world.elapsed() + limit;
        loop {
            let mut errors = Vec::new();
            for node in cluster.config.nodes() {
                if !cluster.is_running(node) || cluster.frozen().contains(&node) {
                    continue;
                }
                match cluster
                    .admin_answer(node, id, name, payload.clone(), ADMIN_LIMIT)
                    .await
                {
                    Ok(Completion::Ok(value)) => return Ok(value),
                    Ok(Completion::Refused(error)) => {
                        errors.push(format!("n{node} {:?}: {}", error.code, error.message));
                    }
                    Ok(Completion::Unknown(error)) | Err(error) => {
                        errors.push(format!("n{node} {error}"));
                    }
                }
            }
            if cluster.world.elapsed() >= deadline {
                return Err(format!("{name}: {}", errors.join("; ")));
            }
            time::sleep(POLL).await;
        }
    }

    /// Replace the lost worker in slot `index` by the next spare.
    async fn replace(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        faults: &NetFaults,
        quiet: bool,
        index: usize,
    ) -> Result<(), String> {
        let lost = cluster.config.node(index);
        assert!(!cluster.is_running(lost), "the lost worker is down");
        let Some(spare) = self.spares.pop() else {
            cluster.record("no spare worker is left".into());
            return Ok(());
        };
        let survivors: Vec<u64> = cluster
            .config
            .nodes()
            .into_iter()
            .filter(|node| *node != lost)
            .collect();
        cluster
            .wait_ready_within(&survivors, true, survivors.len(), CHECK_LIMIT)
            .await
            .map_err(|error| format!("survivors never dropped n{lost}: {error}"))?;
        // A preparation that never enrolled does not change who was lost.
        let lost_id = cluster.config.worker_enrollment(index)["worker_id"].clone();
        let previous = self.connection(cluster).await?["control_peers"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|peer| peer["worker_id"] == lost_id)
            .map(identity)
            .ok_or("Control does not list the lost worker")?;
        let check = json!({"run_id": cluster.config.run_id, "previous": previous});
        let id = cluster.split_id().await;
        self.leader_admin(
            cluster,
            id,
            "worker.system_replacement.check",
            check.clone(),
            CHECK_LIMIT,
        )
        .await?;
        let abandon = self.spares.len() > 1 && g.below(3) == 0;
        if abandon {
            self.abandon(cluster, index).await?;
        }
        let mut candidate = cluster.provision_replacement(index, spare).await?;
        let intent = candidate.intent.clone();
        let intent_id = uuid(&intent["id"]);
        let record = self
            .leader_admin(
                cluster,
                intent_id,
                "worker.system_replacement.prepare",
                intent.clone(),
                CHECK_LIMIT,
            )
            .await?;
        let plans = self.check_intent(cluster, &intent, &record);
        self.replay_prepare(cluster, &survivors, &intent, &record)
            .await;
        self.submit(cluster, ctx, 1 + g.below(3)).await;
        let outstanding = self.queue.outstanding();
        let joined = elapsed_ms(&cluster.world);
        self.generations.insert(spare, 0);
        self.capability.started(spare);
        self.expected_exit.insert(spare);
        cluster.start_candidate(&mut candidate, &record).await?;

        let fault = match g.below(6) {
            _ if quiet => CatchUpFault::None,
            0 => CatchUpFault::None,
            1 => CatchUpFault::FreezeSurvivor,
            2 => CatchUpFault::KillCandidate,
            3 => CatchUpFault::PowerCandidate,
            4 => CatchUpFault::SlowSurvivors,
            _ => CatchUpFault::DropSessions,
        };
        let placed = self
            .catch_up_fault(cluster, ctx, g, faults, fault, &survivors, spare, &check)
            .await?;
        let groups = self
            .wait_replaced(cluster, ctx, &survivors, spare, intent_id, &check)
            .await?;
        if placed {
            self.clauses.insert("O17:replacement-fault");
        }
        self.check_replaced(cluster, &plans, &previous, intent_id, &groups);
        let former = cluster.config.worker_enrollment(index).clone();
        let former_pool = cluster.config.pool(index);
        cluster.adopt(candidate, &groups);
        self.system_votes(cluster);
        self.expected_exit.remove(&spare);
        self.generations.remove(&lost);
        self.wait_members(cluster, ctx, CHECK_LIMIT).await?;
        self.baseline(cluster, spare).await?;
        self.clauses.insert("O17:replacement");
        self.settle_receipts(cluster, ctx, RECEIPT_LIMIT).await;
        if self.handed_over(joined, &outstanding) {
            self.clauses.insert("R03:replacement-joiner");
        }
        let connection = self.connection(cluster).await?;
        let run = uuid(&connection["run_id"]);
        self.judge(cluster, "QueueHandoff", self.queue.run(run));
        self.stays_out(cluster, ctx, lost, former, former_pool)
            .await
    }

    /// Prepare an intent that never mounts, abandon it, and check that it stays abandoned.
    async fn abandon(&mut self, cluster: &mut Cluster, index: usize) -> Result<(), String> {
        let Some(spare) = self.spares.pop() else {
            return Ok(());
        };
        let decoy = cluster.provision_replacement(index, spare).await?;
        let id = uuid(&decoy.intent["id"]);
        self.leader_admin(
            cluster,
            id,
            "worker.system_replacement.prepare",
            decoy.intent.clone(),
            CHECK_LIMIT,
        )
        .await?;
        // "An identical retry confirms abandonment."
        for _ in 0..2 {
            let request = cluster.split_id().await;
            let answer = self
                .leader_admin(
                    cluster,
                    request,
                    "worker.system_replacement.abandon",
                    json!({"id": id}),
                    CHECK_LIMIT,
                )
                .await?;
            if answer != json!({"id": id, "abandoned": true}) {
                cluster.checks.violate(
                    &cluster.world,
                    "RetryCertifiesNamedRemoval",
                    format!("abandoning {id} answered {answer}"),
                );
            }
        }
        self.clauses.insert("O17:abandoned-intent");
        Ok(())
    }

    /// The record names exactly the intent's change in each affected group.
    fn check_intent(&mut self, cluster: &Cluster, intent: &Value, record: &Value) -> Vec<Change> {
        if record["intent"] != *intent {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!(
                    "preparing {intent} recorded another intent: {}",
                    record["intent"]
                ),
            );
        }
        let plans: Vec<Change> = record["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|plan| Change::of(plan, intent))
            .collect();
        if plans.is_empty() || plans.len() != record["groups"].as_array().map_or(0, Vec::len) {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!("a group plan does not name the lost and the new worker: {record}"),
            );
        }
        plans
    }

    /// "An identical retry returns the same record", on every survivor, and another intent for
    /// the same lost worker is refused.
    async fn replay_prepare(
        &mut self,
        cluster: &Cluster,
        survivors: &[u64],
        intent: &Value,
        record: &Value,
    ) {
        let id = uuid(&intent["id"]);
        for &node in survivors {
            let answer = cluster
                .admin_answer(
                    node,
                    id,
                    "worker.system_replacement.prepare",
                    intent.clone(),
                    ADMIN_LIMIT,
                )
                .await;
            if let Ok(Completion::Ok(replayed)) = answer
                && replayed != *record
            {
                cluster.checks.violate(
                    &cluster.world,
                    "RetryCertifiesNamedRemoval",
                    format!(
                        "preparing {id} recorded {record}, and its retry on n{node} {replayed}"
                    ),
                );
            }
        }
        let other = cluster.split_id().await;
        let mut conflicting = intent.clone();
        conflicting["id"] = json!(other);
        let answer = cluster
            .admin_answer(
                survivors[0],
                other,
                "worker.system_replacement.prepare",
                conflicting,
                ADMIN_LIMIT,
            )
            .await;
        if let Ok(Completion::Ok(accepted)) = answer {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!("a second intent for the same lost worker was prepared: {accepted}"),
            );
        }
    }

    /// Place `fault` while the replacement catches up. Returns whether it took effect before
    /// the replacement completed.
    #[allow(clippy::too_many_arguments)]
    async fn catch_up_fault(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        faults: &NetFaults,
        fault: CatchUpFault,
        survivors: &[u64],
        spare: u64,
        check: &Value,
    ) -> Result<bool, String> {
        let at = window(g, 50, 2_500);
        let length = window(g, 500, 4_000);
        let survivor = survivors[g.below(survivors.len() as u64) as usize];
        cluster.record(format!("catch-up fault {fault:?} at {at:?} for {length:?}"));
        if fault == CatchUpFault::None {
            return Ok(false);
        }
        self.pause(cluster, ctx, at).await;
        let complete = self
            .replacement_status(cluster, survivors, check)
            .await
            .is_some_and(|status| status["complete"] == true);
        match fault {
            CatchUpFault::None => {}
            CatchUpFault::FreezeSurvivor => {
                cluster.freeze(survivor);
                self.pause(cluster, ctx, length).await;
                cluster.thaw(survivor);
            }
            CatchUpFault::SlowSurvivors => {
                faults.slow(Some((survivors.to_vec(), Duration::from_millis(100))));
                self.pause(cluster, ctx, length).await;
                faults.slow(None);
            }
            CatchUpFault::DropSessions => {
                for _ in 0..3 {
                    faults.drop_session_after(g.below(40));
                    self.pause(cluster, ctx, length / 3).await;
                }
            }
            CatchUpFault::KillCandidate | CatchUpFault::PowerCandidate => {
                let how = if fault == CatchUpFault::KillCandidate {
                    DiskCut::Kill
                } else {
                    DiskCut::Power
                };
                if cluster.ended(spare).await.is_some() {
                    return Err(format!("n{spare} ended before its fault"));
                }
                cluster.crash(spare, how).await;
                self.pause(cluster, ctx, length).await;
                cluster.restart_candidate(spare, how);
            }
        }
        Ok(!complete)
    }

    async fn replacement_status(
        &self,
        cluster: &Cluster,
        survivors: &[u64],
        check: &Value,
    ) -> Option<Value> {
        for &node in survivors {
            if cluster.frozen().contains(&node) {
                continue;
            }
            let id = cluster.split_id().await;
            if let Ok(Completion::Ok(status)) = cluster
                .admin_answer(
                    node,
                    id,
                    "worker.system_replacement.status",
                    check.clone(),
                    ADMIN_LIMIT,
                )
                .await
            {
                return Some(status);
            }
        }
        None
    }

    /// Wait until the replacement is complete and serves. Returns the groups it reports.
    async fn wait_replaced(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        survivors: &[u64],
        spare: u64,
        intent: Uuid,
        check: &Value,
    ) -> Result<Value, String> {
        let deadline = cluster.world.elapsed() + REPLACE_LIMIT;
        let mut last = "no status".to_owned();
        let mut abandoned = false;
        loop {
            if let Some(result) = cluster.ended(spare).await {
                return Err(format!(
                    "the replacement n{spare} ended before it completed: {result}; {last}"
                ));
            }
            if let Some(status) = self.replacement_status(cluster, survivors, check).await {
                last = format!(
                    "mounted {} complete {}",
                    status["mounted"], status["complete"]
                );
                if status["mounted"] == true && !abandoned {
                    // "Abandonment applies only before mount commits."
                    abandoned = true;
                    let id = cluster.split_id().await;
                    let answer = cluster
                        .admin_answer(
                            survivors[0],
                            id,
                            "worker.system_replacement.abandon",
                            json!({"id": intent}),
                            ADMIN_LIMIT,
                        )
                        .await;
                    if let Ok(Completion::Ok(accepted)) = answer {
                        cluster.checks.violate(
                            &cluster.world,
                            "RetryCertifiesNamedRemoval",
                            format!("a mounted intent was abandoned: {accepted}"),
                        );
                    }
                }
                if status["complete"] == true && cluster.is_ready(spare, 3).await {
                    return Ok(status["groups"].clone());
                }
            }
            if cluster.world.elapsed() >= deadline {
                return Err(format!(
                    "the replacement did not complete within {REPLACE_LIMIT:?}: {last}"
                ));
            }
            self.pause(cluster, ctx, POLL).await;
        }
    }

    /// A complete replacement certifies the removal its intent named, in every group.
    fn check_replaced(
        &mut self,
        cluster: &Cluster,
        plans: &[Change],
        previous: &Value,
        intent: Uuid,
        groups: &Value,
    ) {
        let groups = groups.as_array().cloned().unwrap_or_default();
        if groups.len() != plans.len() {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!(
                    "{} groups were planned and {} are reported complete",
                    plans.len(),
                    groups.len()
                ),
            );
        }
        for plan in plans {
            let Some(group) = groups
                .iter()
                .find(|group| group_id(&group["record"]["identity"]) == plan.named.group)
            else {
                cluster.checks.violate(
                    &cluster.world,
                    "RetryCertifiesNamedRemoval",
                    format!("group {} is not reported", plan.named.group),
                );
                continue;
            };
            let members = group["members"].as_array().cloned().unwrap_or_default();
            self.judge(cluster, "RequiredVoters", required_voters(3, &members));
            if members
                .iter()
                .any(|member| member["worker_id"] == previous["worker_id"])
            {
                cluster.checks.violate(
                    &cluster.world,
                    "RetryCertifiesNamedRemoval",
                    format!(
                        "the replacement is complete and the lost worker is a member: {members:?}"
                    ),
                );
            }
            let next = nodes(&members);
            let answer = Certified::Retired {
                named: plan.named.clone(),
                old: plan.old.clone(),
                next: next.clone(),
            };
            // One intent changes several groups: each is a change of its own.
            let id = Uuid::from_u128(intent.as_u128() ^ plan.named.group.as_u128());
            let verdict = self.removal.answered(id, &plan.named, &answer, Some(&next));
            self.judge(cluster, "RetryCertifiesNamedRemoval", verdict);
        }
    }

    /// The removed worker comes back on its intact disk, with its keys and its bootstrap file,
    /// and must never become ready. `plans/native-storage-engine.md`, Raft safety rule 3: "The
    /// damaged identity never rejoins." O17: the replacement's evidence removed it.
    async fn stays_out(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        removed: u64,
        former: Value,
        former_pool: Value,
    ) -> Result<(), String> {
        self.expected_exit.insert(removed);
        // Control must not prepare a restart for it either, under any next incarnation.
        let id = cluster.split_id().await;
        let mut next = former.clone();
        next["incarnation"] = json!(cluster.split_id().await);
        next["enrollment_revision"] =
            json!(former["enrollment_revision"].as_u64().unwrap_or(0) + 1);
        let intent = json!({
            "id": id, "run_id": cluster.config.run_id, "previous": identity(&former),
            "next": next, "pool_id": former_pool, "mount_generation": 1_000,
        });
        for node in cluster.config.nodes() {
            let answer = cluster
                .admin_answer(
                    node,
                    id,
                    "worker.restart.prepare",
                    intent.clone(),
                    ADMIN_LIMIT,
                )
                .await;
            if let Ok(Completion::Ok(prepared)) = answer {
                cluster.checks.violate(
                    &cluster.world,
                    "RemovedNeverRejoins",
                    format!("n{removed} was replaced, and its restart is prepared: {prepared}"),
                );
            }
        }
        cluster.restart_removed(removed);
        let deadline = cluster.world.elapsed() + REFUSAL_WATCH;
        while cluster.world.elapsed() < deadline {
            if let Some(result) = cluster.ended(removed).await {
                cluster.record(format!("removed n{removed} stayed out: {result}"));
                break;
            }
            if cluster.is_ready(removed, 3).await {
                cluster.checks.violate(
                    &cluster.world,
                    "RemovedNeverRejoins",
                    format!("n{removed} was replaced and reports ready again"),
                );
                break;
            }
            self.pause(cluster, ctx, POLL).await;
        }
        if cluster.world.is_up(removed) {
            cluster.crash(removed, DiskCut::Kill).await;
        }
        self.clauses.insert("O17:removed-worker");
        self.wait_members(cluster, ctx, CHECK_LIMIT).await
    }

    /// Lose worker `index`'s data volume and start it again under the same identity. Whatever
    /// its process does, another voter then freezes, and an acknowledged write in that window
    /// counted a vote of the wiped worker. The worker is down at the end.
    async fn wipe(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        index: usize,
    ) -> Result<(), String> {
        let node = cluster.config.node(index);
        let kind = if g.bool() {
            Wipe::Replace
        } else {
            Wipe::Reformat
        };
        let marker = match g.below(4) {
            0 => Marker::Absent,
            1 => Marker::Fresh,
            _ => Marker::Kept,
        };
        let prepared = g.below(3) == 0;
        let how = self.cut(cluster, g, node).await;
        self.down(cluster, ctx, window(g, 0, 3_000)).await;
        if prepared {
            // The operator prepares the ordinary retained restart for it.
            cluster.prepare_restart(node).await?;
        }
        self.expected_exit.insert(node);
        *self.generations.entry(node).or_default() += 1;
        let started = cluster
            .restart_same_identity(node, how, Disk::Empty(kind, marker), REFUSAL_WATCH)
            .await?;
        cluster.record(format!("wiped n{node} (prepared {prepared}): {started:?}"));
        match &started {
            Started::Ready => self.note("wiped worker ready"),
            Started::Running => self.note("wiped worker running"),
            Started::Exited(_) => self.note("wiped worker refused"),
        }
        self.evidence(cluster, ctx, g, node).await;
        if cluster.world.is_up(node) {
            cluster.crash(node, DiskCut::Kill).await;
        }
        self.expected_exit.remove(&node);
        self.clauses.insert("O17:wiped-voter");
        Ok(())
    }

    /// Freeze one voter besides `without`, and judge every write that was proposed and
    /// acknowledged while it was frozen.
    async fn evidence(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen, without: u64) {
        let Some(placement) = self.observe_placement(cluster, ctx).await else {
            self.note("no placement before the evidence window");
            return;
        };
        let (group, _) = match moves::placement(cluster, ctx).await {
            Ok(found) => found,
            Err(_) => return,
        };
        let others: Vec<u64> = cluster
            .config
            .nodes()
            .into_iter()
            .filter(|node| *node != without)
            .collect();
        let frozen = others[g.below(others.len() as u64) as usize];
        let survivor = others
            .iter()
            .copied()
            .find(|node| *node != frozen)
            .expect("three workers");
        cluster.freeze(frozen);
        let from = cluster.world.elapsed() + IN_FLIGHT;
        let until = cluster.world.elapsed() + EVIDENCE;
        time::sleep(IN_FLIGHT).await;
        // A write that needs the Control quorum, and one that needs Timestamps and Catalog.
        let policy = growth_policy(cluster, self.next_sample_ms);
        let policy_id = cluster.split_id().await;
        let table_id = in_node(CLIENTS[0], async { uuid_v4() }).await;
        let (mut control, mut table) = (Err(String::new()), Err(String::new()));
        while cluster.world.elapsed() + EVIDENCE_ATTEMPT < until {
            let began = cluster.world.elapsed();
            let configure = cluster.admin_task(
                survivor,
                policy_id,
                "storage.growth.configure",
                json!({"policy": policy}),
                EVIDENCE_ATTEMPT,
            );
            let create = nabedra_rt::task::spawn(create_probe_table(
                ctx.token.clone(),
                table_id,
                EVIDENCE_ATTEMPT,
            ));
            // The same IDs are sent again: only an answer in the window counts.
            for (known, attempt) in [(&mut control, configure), (&mut table, create)] {
                let answer = attempt.await.unwrap_or_else(|error| Err(error.to_string()));
                if known.is_err() {
                    *known = answer;
                }
            }
            if let Some(wait) = (began + POLL).checked_sub(cluster.world.elapsed()) {
                time::sleep(wait).await;
            }
        }
        while cluster.world.elapsed() < until {
            time::sleep(POLL).await;
        }
        let to = elapsed_ms(&cluster.world);
        let system = [
            (control, &["control"][..]),
            (table, &["timestamps", "catalog"][..]),
        ];
        let groups = cluster.config.system_groups();
        for (answer, needed) in system {
            cluster.record(format!(
                "evidence: {needed:?} beside n{without}: {}",
                match &answer {
                    Ok(_) => "acknowledged",
                    Err(error) => error,
                }
            ));
            if answer.is_err() {
                continue;
            }
            for (label, group, members) in &groups {
                if needed.contains(&label.as_str()) {
                    let voters = self.taking_part(cluster, members, frozen);
                    let verdict = self
                        .wiped
                        .committed(*group, &voters)
                        .map_err(|detail| format!("{label}: {detail}"));
                    self.judge(cluster, "WipedNeverVotes", verdict);
                }
            }
        }
        // Judge with the processes as they were in the window.
        let members = placement["members"].as_array().cloned().unwrap_or_default();
        let voters = self.taking_part(cluster, &members, frozen);
        cluster.thaw(frozen);
        let from = from.as_millis() as u64;
        let writes: Vec<(Uuid, u64, u64)> = ctx
            .observed
            .lock()
            .unwrap()
            .attempts
            .iter()
            .filter(|attempt| {
                attempt.fresh
                    && attempt.answer == Answer::Committed
                    && attempt.invoked >= from
                    && attempt.completed < to
            })
            .map(|attempt| (attempt.id, attempt.invoked, attempt.completed))
            .collect();
        cluster.record(format!(
            "evidence: n{frozen} frozen beside n{without}, voters {voters:?}, {} writes acknowledged",
            writes.len()
        ));
        self.note("evidence windows");
        if let Some((id, invoked, completed)) = writes.first() {
            let verdict = self
                .wiped
                .committed(group_id(&group), &voters)
                .map_err(|detail| {
                    format!(
                        "{detail} ({} writes, the first {id} at {invoked} to {completed} ms)",
                        writes.len()
                    )
                });
            self.judge(cluster, "WipedNeverVotes", verdict);
        }
    }

    /// Opt-in: a worker restarts, properly prepared, on an older image of its own volume.
    async fn stale(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        index: usize,
    ) -> Result<(), String> {
        let node = cluster.config.node(index);
        let image: FaultFs = disk::snapshot(&cluster.world, node);
        self.pause(cluster, ctx, window(g, 2_000, 6_000)).await;
        cluster.crash(node, DiskCut::Kill).await;
        cluster.prepare_restart(node).await?;
        self.expected_exit.insert(node);
        let started = cluster
            .restart_same_identity(node, DiskCut::Kill, Disk::Stale(&image), CHECK_LIMIT)
            .await?;
        cluster.record(format!("stale n{node}: {started:?}"));
        drop(image);
        match started {
            Started::Ready => {
                self.note("stale worker ready");
                let others: Vec<u64> = cluster
                    .config
                    .nodes()
                    .into_iter()
                    .filter(|other| *other != node)
                    .collect();
                let frozen = others[g.below(others.len() as u64) as usize];
                cluster.freeze(frozen);
                time::sleep(EVIDENCE).await;
                cluster.thaw(frozen);
                self.expected_exit.remove(&node);
                self.wait_members(cluster, ctx, CHECK_LIMIT).await
            }
            Started::Running | Started::Exited(_) => {
                self.note("stale worker refused");
                if cluster.world.is_up(node) {
                    cluster.crash(node, DiskCut::Kill).await;
                }
                self.expected_exit.remove(&node);
                Err(format!(
                    "n{node} did not open its stale volume, and the run has no other one: {started:?}"
                ))
            }
        }
    }

    /// R04: "New run has no pending old work; old receipts report abandonment unless a durable
    /// commit decision proves success; committed rows remain."
    async fn cold(&mut self, cluster: &mut Cluster, ctx: &Ctx, g: &mut Gen) -> Result<(), String> {
        self.settle_receipts(cluster, ctx, RECEIPT_LIMIT).await;
        self.submit(cluster, ctx, 2 + g.below(3)).await;
        let outstanding = self.queue.outstanding();
        let run = cluster.config.run_id;
        let how = if g.bool() {
            DiskCut::Kill
        } else {
            DiskCut::Power
        };
        let delays = [(); 3].map(|()| window(g, 0, 2_000));
        cluster
            .cold_start_staggered("cold start", how, delays, COLD_LIMIT)
            .await?;
        self.wait_members(cluster, ctx, CHECK_LIMIT).await?;
        let deadline = cluster.world.elapsed() + RECEIPT_LIMIT;
        let mut pending = outstanding.clone();
        while !pending.is_empty() {
            let mut next = Vec::new();
            for (index, id) in pending.into_iter().enumerate() {
                let gateway = GATEWAYS[index % GATEWAYS.len()];
                let state = self.receipt(cluster, ctx, gateway, run, id).await;
                match state.as_deref() {
                    Some("abandoned" | "failed") => {}
                    Some("succeeded") => {
                        if let Some(queued) = self.queued.get(&id) {
                            self.succeeded.insert(id, *queued);
                        }
                    }
                    Some(state) => {
                        cluster.record(format!("old-run {id} is {state}"));
                        next.push(id);
                    }
                    None => next.push(id),
                }
            }
            pending = next;
            if pending.is_empty() {
                break;
            }
            if cluster.world.elapsed() >= deadline {
                cluster.checks.violate(
                    &cluster.world,
                    "OldRunReceipts",
                    format!("old-run requests {pending:?} have no terminal receipt"),
                );
                break;
            }
            self.pause(cluster, ctx, POLL).await;
        }
        self.queue = QueueHandoff::new(cluster.config.run_id);
        self.queued.clear();
        Ok(())
    }

    /// Move the scheduler owner's Data replica to a second pool and a third, with retries.
    async fn moves(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        faults: &NetFaults,
        quiet: bool,
    ) -> Result<(), String> {
        let deadline = cluster.world.elapsed() + CHECK_LIMIT;
        let owner = loop {
            match moves::owner(cluster, ctx).await {
                Ok(owner) => break owner,
                Err(error) if cluster.world.elapsed() >= deadline => {
                    return Err(format!("scheduler owner: {error}"));
                }
                Err(_) => self.pause(cluster, ctx, POLL).await,
            }
        };
        let mut pool = moves::Move::draw(g);
        let deadline = cluster.world.elapsed() + CHECK_LIMIT;
        let (id, payload) = loop {
            match pool.prepare(cluster, ctx, owner).await {
                Ok(prepared) => break prepared,
                Err(error) if cluster.world.elapsed() >= deadline => {
                    return Err(format!("second pool: {error}"));
                }
                Err(_) => self.pause(cluster, ctx, POLL).await,
            }
        };
        if !quiet && g.bool() {
            let others = cluster
                .config
                .nodes()
                .into_iter()
                .filter(|node| *node != owner)
                .collect();
            faults.slow(Some((others, window(g, 5, 150))));
        }
        // Before the pool holds anything: a request that names a voter that is no member must
        // not move another one there.
        let mut stranger = payload.clone();
        stranger["remove_node"] = json!(700_000 + g.below(1_000));
        let stranger_id = cluster.split_id().await;
        let answer = cluster
            .admin_answer(
                owner,
                stranger_id,
                "storage.replica.move",
                stranger.clone(),
                ADMIN_LIMIT,
            )
            .await;
        self.judge_move(cluster, ctx, stranger_id, &stranger, &answer)
            .await;
        cluster.record(format!("stranger before the move: {}", shown(&answer)));
        let moved = self.drive(cluster, ctx, g, owner, id, &payload).await;
        faults.slow(None);
        moved?;
        self.hostile_retries(cluster, ctx, g, owner, id, &payload)
            .await;

        // On to a third pool, under another ID. The primary pool keeps the first move's retired
        // replica, so it is refused as a target.
        self.second_pools
            .extend(pool.second_pool_of(&cluster.world, owner));
        let index = cluster.config.index_of(owner).expect("known worker");
        let listed = cluster.config.secondary_pools(index);
        pool.another_pool();
        let deadline = cluster.world.elapsed() + CHECK_LIMIT;
        let (back_id, back) = loop {
            match pool.prepare(cluster, ctx, owner).await {
                Ok(prepared) => break prepared,
                Err(error) if cluster.world.elapsed() >= deadline => {
                    return Err(format!("third pool: {error}"));
                }
                Err(_) => self.pause(cluster, ctx, POLL).await,
            }
        };
        cluster.config.list_more_pools(index, listed);
        self.second_pools
            .extend(pool.second_pool_of(&cluster.world, owner));
        self.drive(cluster, ctx, g, owner, back_id, &back).await?;
        // A late retry of the first move, after the second one undid it.
        self.hostile_retries(cluster, ctx, g, owner, id, &payload)
            .await;
        self.hostile_retries(cluster, ctx, g, owner, back_id, &back)
            .await;
        self.clauses.insert("O17:move-sequence");
        Ok(())
    }

    /// One answer to a move, judged against the published placement after it.
    async fn judge_move(
        &mut self,
        cluster: &Cluster,
        ctx: &Ctx,
        id: Uuid,
        payload: &Value,
        answer: &Result<Completion, String>,
    ) -> Certified {
        let request = named(payload);
        let certified = match answer {
            Ok(Completion::Ok(record)) if record["phase"]["state"] == "retired" => {
                Certified::Retired {
                    named: named(&record["request"]),
                    old: nodes(record["old"].as_array().map_or(&[][..], Vec::as_slice)),
                    next: nodes(record["next"].as_array().map_or(&[][..], Vec::as_slice)),
                }
            }
            Ok(Completion::Refused(error))
                if error.outcome == nabedra_protocol::protocol::Outcome::NotApplied =>
            {
                Certified::Refused
            }
            _ => Certified::Pending,
        };
        if let Ok(Completion::Ok(record)) = answer
            && uuid(&record["id"]) != id
        {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!("the answer to {id} is the record of {}", record["id"]),
            );
        }
        let published = match moves::placement(cluster, ctx).await {
            Ok((_, placement)) => Some(nodes(
                placement["members"]
                    .as_array()
                    .map_or(&[][..], Vec::as_slice),
            )),
            Err(_) => None,
        };
        let verdict = self
            .removal
            .answered(id, &request, &certified, published.as_ref());
        self.judge(cluster, "RetryCertifiesNamedRemoval", verdict);
        certified
    }

    /// Send the same move until it is certified as complete. Early attempts give up fast, so
    /// the operator's reply is lost and the same ID is retried.
    async fn drive(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        owner: u64,
        id: Uuid,
        payload: &Value,
    ) -> Result<(), String> {
        let deadline = cluster.world.elapsed() + MOVE_LIMIT;
        let mut short = g.below(3);
        loop {
            let limit = if short > 0 {
                short -= 1;
                window(g, 50, 2_000)
            } else {
                MOVE_REQUEST
            };
            let answer = cluster
                .admin_answer(owner, id, "storage.replica.move", payload.clone(), limit)
                .await;
            let certified = self.judge_move(cluster, ctx, id, payload, &answer).await;
            if matches!(certified, Certified::Retired { .. }) {
                return Ok(());
            }
            if cluster.world.elapsed() >= deadline {
                return Err(format!(
                    "move {id} did not retire within {MOVE_LIMIT:?}: {}",
                    shown(&answer)
                ));
            }
            self.pause(cluster, ctx, POLL).await;
        }
    }

    /// After move `id` completed: the same request again, another body under its ID, and fresh
    /// IDs that name a voter that is no member.
    async fn hostile_retries(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        g: &mut Gen,
        owner: u64,
        id: Uuid,
        payload: &Value,
    ) {
        let Ok((_, before)) = moves::placement(cluster, ctx).await else {
            return;
        };
        let other = before["members"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|member| member["node_id"].as_u64())
            .find(|node| *node != payload["target"]["node_id"])
            .unwrap_or(1);
        let mut another_body = payload.clone();
        another_body["remove_node"] = json!(other);
        let mut stranger = payload.clone();
        stranger["remove_node"] = json!(700_000 + g.below(1_000));
        stranger["expected_generation"] = before["record"]["generation"].clone();
        let fresh = cluster.split_id().await;
        let again = cluster.split_id().await;
        for (id, payload) in [
            (id, payload.clone()),
            (id, another_body),
            (fresh, stranger),
            // The completed change as a new request: its voter is gone.
            (again, payload.clone()),
        ] {
            let answer = cluster
                .admin_answer(
                    owner,
                    id,
                    "storage.replica.move",
                    payload.clone(),
                    ADMIN_LIMIT,
                )
                .await;
            self.judge_move(cluster, ctx, id, &payload, &answer).await;
            cluster.record(format!(
                "retry {id} removing {}: {}",
                payload["remove_node"],
                shown(&answer)
            ));
        }
        if let Ok((_, after)) = moves::placement(cluster, ctx).await
            && (after["members"] != before["members"]
                || after["record"]["generation"] != before["record"]["generation"])
        {
            cluster.checks.violate(
                &cluster.world,
                "RetryCertifiesNamedRemoval",
                format!(
                    "retries of a completed move changed the placement from {before} to {after}"
                ),
            );
        }
    }
}

/// One group's change in a replacement plan.
struct Change {
    named: NamedChange,
    old: BTreeSet<u64>,
}

impl Change {
    /// The change `plan` makes, if it removes the intent's lost worker and adds its new one.
    fn of(plan: &Value, intent: &Value) -> Option<Self> {
        let old = plan["old"].as_array()?;
        let removed = old
            .iter()
            .find(|member| member["worker_id"] == intent["previous"]["worker_id"])?;
        let learner = &plan["learner"];
        if learner["worker_id"] != intent["next"]["worker_id"]
            || learner["pool_id"] != intent["mount"]["identity"]["pool_id"]
        {
            return None;
        }
        Some(Self {
            named: NamedChange {
                group: group_id(&plan["expected"]["identity"]),
                remove: removed["node_id"].as_u64()?,
                add: learner["node_id"].as_u64()?,
            },
            old: nodes(old),
        })
    }
}

fn named(request: &Value) -> NamedChange {
    NamedChange {
        group: group_id(&request["group"]),
        remove: request["remove_node"].as_u64().unwrap_or(0),
        add: request["target"]["node_id"].as_u64().unwrap_or(0),
    }
}

fn nodes(members: &[Value]) -> BTreeSet<u64> {
    members
        .iter()
        .filter_map(|member| member["node_id"].as_u64())
        .collect()
}

fn uuid(value: &Value) -> Uuid {
    value
        .as_str()
        .and_then(|text| Uuid::parse_str(text).ok())
        .unwrap_or_default()
}

fn group_id(identity: &Value) -> Uuid {
    uuid(&identity["group_id"])
}

/// The sim node that runs the worker of a replica.
fn worker_node(cluster: &Cluster, member: &Value) -> Option<u64> {
    cluster
        .config
        .nodes()
        .into_iter()
        .enumerate()
        .find(|(index, _)| {
            cluster.config.worker_enrollment(*index)["worker_id"] == member["worker_id"]
        })
        .map(|(_, node)| node)
}

fn identity(enrollment: &Value) -> Value {
    json!({
        "worker_id": enrollment["worker_id"],
        "incarnation": enrollment["incarnation"],
        "enrollment_revision": enrollment["enrollment_revision"],
        "certificate_sha256": enrollment["certificate_sha256"],
    })
}

fn shown(answer: &Result<Completion, String>) -> String {
    match answer {
        Ok(Completion::Ok(record)) => format!("phase {}", record["phase"]["state"]),
        Ok(Completion::Refused(error)) => format!("{:?} {}", error.code, error.message),
        Ok(Completion::Unknown(error)) | Err(error) => format!("unknown ({error})"),
    }
}

/// A growth policy that changes nothing a run depends on. Configuring it is a Control write.
fn growth_policy(cluster: &Cluster, salt: u64) -> Value {
    let zones: serde_json::Map<String, Value> = (0..WORKERS.len())
        .map(|index| {
            let worker = cluster.config.worker_identity(index);
            let id = worker["worker_id"].as_str().unwrap_or_default().to_owned();
            (id, json!(format!("fixture-{index}")))
        })
        .collect();
    json!({
        "policy_id": Uuid::from_u128(0x5157_0000_0000_0000_0000 + u128::from(salt)),
        "volume_cost_per_gib": 1, "worker_cost": 1,
        "initial_monthly_spend": 0, "monthly_spend_ceiling": 30, "max_attempts": 3,
        "cooldown_seconds": 1, "max_volume_bytes": 2_u64 << 30,
        "zones": zones.values().cloned().collect::<Vec<_>>(),
        "initial_replica_zones": zones,
    })
}

/// Create a table of its own through the first gateway.
async fn create_probe_table(token: String, id: Uuid, limit: Duration) -> Result<Value, String> {
    let payload = json!({
        "name": format!("probe_{}", id.simple()),
        "schema": {"id": "int64", "value": "int64"}, "key": "id", "format": "parquet",
    });
    match request(
        CLIENTS[0],
        GATEWAYS[0],
        "/v1/operations",
        &token,
        Some(operation(id, "table.create", payload)),
        limit,
    )
    .await
    .and_then(|reply| reply.completion(id))?
    {
        Completion::Ok(created) => Ok(created),
        Completion::Refused(error) => Err(format!("{:?} {}", error.code, error.message)),
        Completion::Unknown(error) => Err(error),
    }
}

/// What every running worker reports about the gated capabilities.
///
/// Returns how many workers answered while some voter had not run: only those answers can show
/// a capability that is in use too early.
async fn observe_capability(cluster: &Cluster, rule: &mut CapabilityBeforeUse) -> u64 {
    let mut early = 0;
    let voters = cluster.config.nodes();
    for node in voters {
        if cluster.is_running(node) {
            rule.started(node);
        }
    }
    for node in voters {
        if !cluster.is_running(node) || cluster.frozen().contains(&node) {
            continue;
        }
        let Ok(stats) = cluster.stats(node).await else {
            continue;
        };
        early += u64::from(rule.some_never_ran(&voters));
        let capability = &stats["direct_capability"];
        let applied = stats["direct"]["entries_applied"].as_u64().unwrap_or(0) > 0;
        for (used, capability, what) in [
            (capability["active"] == true, Capability::Direct, "active"),
            (applied, Capability::Direct, "entries applied"),
            (capability["batch"] == true, Capability::Batch, "active"),
        ] {
            if used
                && let Err(detail) = rule.in_use(capability, &format!("{what} on n{node}"), &voters)
            {
                cluster
                    .checks
                    .violate(&cluster.world, "CapabilityBeforeUse", detail);
            }
        }
    }
    early
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_parse_and_an_unknown_one_is_an_error() {
        assert_eq!(
            Step::parse("rolling, wipe").unwrap(),
            [Step::Rolling, Step::Wipe]
        );
        assert!(Step::parse("rolling,bogus").is_err());
    }

    /// Every step but the opt-in one is drawn, and a move never follows a replacement.
    #[test]
    fn drawn_scripts_cover_every_swept_step() {
        let mut seen = BTreeSet::new();
        for seed in 1..=200 {
            let steps = Step::draw(&mut Gen::from_seed(seed));
            assert!(!steps.is_empty());
            let moves = steps.iter().position(|step| *step == Step::Moves);
            assert!(moves.is_none_or(|at| at == 0), "{steps:?}");
            seen.extend(steps.iter().map(|step| step.name()));
        }
        assert_eq!(
            seen,
            BTreeSet::from(["cold", "moves", "replace", "rolling", "wipe"])
        );
    }

    #[test]
    fn a_plan_that_names_other_workers_is_no_change_of_the_intent() {
        let intent = json!({
            "previous": {"worker_id": "lost"},
            "next": {"worker_id": "new"},
            "mount": {"identity": {"pool_id": "pool"}},
        });
        let plan = |removed: &str, added: &str| {
            json!({
                "expected": {"identity": {"group_id": Uuid::from_u128(1)}},
                "old": [
                    {"node_id": 1, "worker_id": removed}, {"node_id": 2, "worker_id": "b"},
                    {"node_id": 3, "worker_id": "c"},
                ],
                "learner": {"node_id": 4, "worker_id": added, "pool_id": "pool"},
            })
        };
        let change = Change::of(&plan("lost", "new"), &intent).unwrap();
        assert_eq!((change.named.remove, change.named.add), (1, 4));
        assert!(Change::of(&plan("a", "new"), &intent).is_none());
        assert!(Change::of(&plan("lost", "a"), &intent).is_none());
    }
}
