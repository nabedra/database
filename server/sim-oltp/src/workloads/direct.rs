//! The `direct` workload and the run phases it shares with `cold` and `move`: clients under
//! chaos, heal, converge, then a final read of every key through both gateways.

use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_sim::Profile;
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use serde_json::{Value, json};

use super::{
    Scenarios, Workload,
    clients::{self, Ctx, Plan},
    cold, moves,
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    client::{operation, request},
    config::Config,
    history::{Completion, History, Operation},
    invariants::{STEP, elapsed_ms},
    nemesis::{Action, NetFaults, Selection, apply::Chaos},
    node::Cluster,
};

/// Virtual time of chaos before the heal (design 3.6).
pub(crate) const CHAOS: Duration = Duration::from_secs(20);
/// Bound for the heal itself: thaw, reconnect and restart what is down.
const HEAL_LIMIT: Duration = Duration::from_secs(120);
/// Bound for convergence after the heal: every retry and the final reads (design 3.6).
pub(crate) const CONVERGE_LIMIT: Duration = Duration::from_secs(30);
const SETUP_DEADLINE: Duration = Duration::from_secs(20);
const TICK: Duration = Duration::from_millis(50);

/// What one workload run executed, for the seed's report.
#[derive(Default)]
pub(crate) struct Report {
    pub scenarios: Scenarios,
    pub assertions: Vec<(&'static str, bool)>,
    pub summary: Value,
}

/// Run `workload` against a started cluster.
pub(crate) async fn run(
    cluster: &mut Cluster,
    config: &Config,
    history: Arc<Mutex<History>>,
    g: &mut Gen,
    seed: u64,
    workload: Workload,
    selection: &Selection,
    quiet: bool,
) -> Result<Report, String> {
    cluster.wait_direct().await?;
    let table = create_table(cluster, &history).await?;
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }
    let plan = Plan::draw(g, selection.focus.contention, selection.focus.idle);
    let ctx = Ctx::new(
        cluster.world.clone(),
        cluster.checks.clone(),
        history.clone(),
        table,
        cluster.config.data_token.clone(),
        config.deadline(),
        plan.keys,
    );
    let faults = NetFaults::install(cluster.net(), seed);
    let start = elapsed_ms(&cluster.world);
    let mut profile = selection.profile(seed);
    if workload == Workload::Move {
        // A worker restart changes the identity the move names; process faults run in
        // the direct workload instead.
        profile = without_process_faults(profile);
    }
    let mut chaos = (!quiet && profile.total_weight() > 0)
        .then(|| Chaos::new(profile, faults.clone(), start, g));
    let mut moving = (workload == Workload::Move).then(|| {
        let mut moving = moves::Move::draw(g);
        moving.set_faults(faults.clone());
        moving
    });
    // The moving worker gains a second pool, so its device gauges change by design.
    let gauged: Vec<u64> = WORKERS
        .into_iter()
        .filter(|node| moving.as_ref().is_none_or(|moving| moving.node() != *node))
        .collect();
    let staggered = (workload == Workload::Cold).then(|| cold::Stop::draw(g));
    cluster.record(format!(
        "workload {workload} profile {} clients {} keys {}",
        chaos.as_ref().map_or("none", |chaos| chaos.profile_name()),
        plan.programs.len(),
        plan.keys,
    ));
    let tasks: Vec<_> = plan
        .programs
        .into_iter()
        .map(|program| nabedra_rt::task::spawn(clients::run(ctx.clone(), program)))
        .collect();

    let chaos_window = if workload == Workload::Cold {
        CHAOS / 2
    } else {
        CHAOS
    };
    let end = cluster.world.elapsed() + chaos_window;
    while cluster.world.elapsed() < end {
        if let Some(chaos) = chaos.as_mut() {
            chaos.tick(cluster, &ctx, g).await;
        }
        if let Some(moving) = moving.as_mut() {
            moving.tick(cluster, &ctx).await;
        }
        time::sleep(TICK).await;
    }
    ctx.stopping.store(true, Ordering::Release);

    let mut report = Report::default();
    let mut cold_stopped = false;
    if let Some(stop) = staggered {
        // Stop every worker while requests are in flight, then reopen them staggered.
        let started = cluster.world.elapsed();
        let cold = stop.run(cluster, &ctx, chaos.as_mut()).await;
        report.assertions.push(("cold start within bound", cold.is_ok()));
        println!(
            "fault model cold workload seed {seed}: cold start {:?} virtual",
            cluster.world.elapsed() - started
        );
        if let Err(error) = cold {
            cluster
                .checks
                .violate(&cluster.world, "Liveness", format!("cold start: {error}"));
            return abandon(cluster, &ctx, tasks, report).await;
        }
        cold_stopped = true;
    }

    cluster
        .checks
        .expect_recovery(&cluster.world, "heal", HEAL_LIMIT);
    if let Some(chaos) = chaos.as_mut()
        && let Err(error) = chaos.heal(cluster, &ctx).await
    {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &ctx, tasks, report).await;
    }
    if let Err(error) = cluster.wait_direct().await {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &ctx, tasks, report).await;
    }
    cluster.checks.recovered(&cluster.world, "heal");
    if let Some(chaos) = chaos.as_mut() {
        chaos.finish_split(cluster, &ctx).await;
    }
    if let Some(moving) = moving.as_mut() {
        match moving.finish(cluster, &ctx).await {
            Ok(()) => {
                report.scenarios.insert("O26");
                report.assertions.push(("replica move retired", true));
            }
            Err(error) => {
                report.assertions.push(("replica move retired", false));
                cluster
                    .checks
                    .violate(&cluster.world, "Liveness", format!("replica move: {error}"));
            }
        }
    }

    cluster
        .checks
        .expect_recovery(&cluster.world, "convergence", CONVERGE_LIMIT);
    let converge_by = elapsed_ms(&cluster.world) + CONVERGE_LIMIT.as_millis() as u64;
    ctx.converge_by.store(converge_by, Ordering::Release);
    for task in tasks {
        let _ = task.await;
    }
    // A process that ran throughout must return to its pre-chaos baseline. A restarted one
    // has none, so it is measured from its own state after a first read pass.
    let mut reads = clients::final_reads(&ctx).await;
    for &node in gauged.iter().filter(|node| cluster.restarted().contains(node)) {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }
    if reads.is_ok() {
        reads = clients::final_reads(&ctx).await;
    }
    report
        .assertions
        .push(("final reads through both gateways", reads.is_ok()));
    if let Err(error) = &reads {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", error.clone());
    }
    cluster.checks.recovered(&cluster.world, "convergence");

    let observed = std::mem::take(&mut *ctx.observed.lock().unwrap());
    report
        .assertions
        .push(("every request resolved", observed.unresolved.is_empty()));
    for request in &observed.unresolved {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("request unresolved {CONVERGE_LIMIT:?} after the heal: {request}"),
        );
    }
    if let Some(chaos) = chaos.as_ref().filter(|chaos| !chaos.no_quorum.is_empty()) {
        let clean = check_no_quorum(cluster, chaos, &observed.fresh_writes);
        report
            .assertions
            .push(("no fresh write succeeds with two followers frozen", clean));
    }
    let stats = settle(cluster).await?;
    if selection.focus.idle {
        // No client ran, so there is no mutation to observe.
    } else if cluster.restarted().len() == WORKERS.len() {
        // Every process restarted: their counters no longer hold the earlier mutations.
        check_direct(cluster, &stats);
    } else {
        cluster.checks.check_direct_path(&cluster.world, &stats);
    }
    for (node, stats) in &stats {
        if gauged.contains(node) {
            cluster.checks.check_resources(&cluster.world, *node, stats);
        }
    }

    if reads.is_ok() {
        tag(&mut report.scenarios, chaos.as_ref(), &observed);
        if cold_stopped {
            report.scenarios.insert("O15");
        }
    }
    report.summary = json!({
        "workload": workload.name(),
        "profile": chaos.as_ref().map(Chaos::profile_name),
        "keys": ctx.keys,
        "executed": chaos.as_ref().map(|chaos| chaos.executed.iter().map(|(a, n)| (a.name(), *n)).collect::<std::collections::BTreeMap<_, _>>()),
        "skipped": chaos.as_ref().map(|chaos| chaos.skipped.iter().map(|(a, n)| (a.name(), *n)).collect::<std::collections::BTreeMap<_, _>>()),
        "session_resets": chaos.as_ref().map(Chaos::resets),
        "completed": observed.completed,
        "resolved_after_unknown": observed.resolved_after_unknown,
        "resolved_across_gateways": observed.resolved_across_gateways,
        "reuse_same_key": observed.reuse_same_key,
        "reuse_other_key": observed.reuse_other_key,
        "not_applied_checks": observed.not_applied_checks,
        "unresolved": observed.unresolved.len(),
        "refused_final_reads": observed.refused_final_reads,
        "first_refusal": observed.first_refusal,
        "heal_cold_start": chaos.as_ref().is_some_and(|chaos| chaos.heal_cold),
    });
    cluster.record(format!("workload summary {}", report.summary));
    Ok(report)
}

fn without_process_faults(profile: Profile<Action>) -> Profile<Action> {
    let mut kept = Profile::new(profile.name, profile.gap_ms);
    for (action, weight) in profile.actions() {
        if !matches!(
            action,
            Action::CrashLeader
                | Action::PowerLeader
                | Action::DrainLeader
                | Action::DiskError
                | Action::Cold
                | Action::KillGateway
        ) {
            kept = kept.with(*action, weight);
        }
    }
    kept
}

/// The run cannot converge: stop the clients and keep the violations already recorded.
async fn abandon(
    cluster: &mut Cluster,
    ctx: &Ctx,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    report: Report,
) -> Result<Report, String> {
    ctx.converge_by.store(0, Ordering::Release);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    cluster.record("workload abandoned after a liveness violation".into());
    Ok(report)
}

/// D05: with two of three workers frozen no quorum exists, so no fresh write invoked
/// after the freeze may succeed before the thaw.
fn check_no_quorum(cluster: &Cluster, chaos: &Chaos, writes: &[(u64, u64, bool)]) -> bool {
    let mut clean = true;
    for window in &chaos.no_quorum {
        let to = window.to_ms.unwrap_or(u64::MAX);
        for &(invoked, completed, committed) in writes {
            if committed && invoked >= window.from_ms && completed <= to {
                clean = false;
                cluster.checks.violate(
                    &cluster.world,
                    "NoQuorumNoSuccess",
                    format!(
                        "a fresh write invoked at {invoked}ms committed at {completed}ms while two followers were frozen from {}ms to {to}ms",
                        window.from_ms
                    ),
                );
            }
        }
    }
    clean
}

/// The direct path served this run: some worker is active and answered a get. Counters
/// restart with their process, so a cold run only guarantees the final reads' gets.
fn check_direct(cluster: &Cluster, stats: &[(u64, Value)]) {
    let active = stats
        .iter()
        .any(|(_, stats)| stats["direct_capability"]["active"] == true);
    let gets = stats
        .iter()
        .any(|(_, stats)| stats["direct"]["gets_done"].as_u64().is_some_and(|n| n > 0));
    if !(active && gets) {
        cluster.checks.violate(
            &cluster.world,
            "DirectPath",
            "worker stats did not observe an active direct path and a completed get",
        );
    }
}

fn tag(scenarios: &mut Scenarios, chaos: Option<&Chaos>, observed: &clients::Observations) {
    if observed.reuse_same_key > 0 && observed.reuse_other_key > 0 {
        scenarios.insert("D03");
    }
    let Some(chaos) = chaos else {
        return;
    };
    if chaos.ran(Action::KillGateway) && observed.resolved_across_gateways > 0 {
        scenarios.insert("D02");
    }
    if chaos.leader_lost_with_writes > 0 && observed.resolved_after_unknown > 0 {
        scenarios.insert("D04");
    }
    // The no-success rule is exercised only by a fresh write invoked inside a window.
    let tested_no_quorum = chaos.no_quorum.iter().any(|window| {
        let to = window.to_ms.unwrap_or(u64::MAX);
        observed
            .fresh_writes
            .iter()
            .any(|(invoked, _, _)| (window.from_ms..to).contains(invoked))
    });
    if tested_no_quorum {
        scenarios.insert("D05");
    }
    if chaos.split_published && chaos.ran(Action::LeaderMove) {
        scenarios.insert("D07");
    }
    if chaos.ran(Action::SessionDrop) && chaos.resets() > 0 && observed.overloaded > 0 {
        scenarios.insert("D10");
    }
    // Quorum loss with a write attempted inside it, and a lost or failing voter disk.
    if tested_no_quorum && (chaos.ran(Action::PowerLeader) || chaos.disk_fired > 0) {
        scenarios.insert("O06");
    }
    if observed.resolved_after_unknown > 0
        && [
            Action::LostReply,
            Action::LostHttpReply,
            Action::AuthFlap,
            Action::KillGateway,
            Action::DeadlineEdge,
        ]
        .into_iter()
        .any(|action| chaos.ran(action))
    {
        scenarios.insert("O10");
    }
    if chaos.disk_fired > 0 {
        scenarios.insert("O29");
    }
    if chaos.ran(Action::Cold) {
        scenarios.insert("O15");
    }
}

/// Create the workload's table through the first gateway, retrying the same ID.
async fn create_table(cluster: &Cluster, history: &Arc<Mutex<History>>) -> Result<String, String> {
    let node = CLIENTS[0];
    let gateway = GATEWAYS[0];
    let id = in_node(node, async { uuid_v4() }).await;
    let payload = json!({
        "name": "sim_direct", "schema": {"id": "int64", "value": "int64"},
        "key": "id", "format": "parquet",
    });
    let mut client = history.lock().unwrap().client();
    let deadline = cluster.world.elapsed() + SETUP_DEADLINE;
    loop {
        let attempt = history.lock().unwrap().invoke(
            &client,
            &cluster.world,
            gateway,
            Operation {
                table: None,
                name: "table.create",
                key: None,
                request_id: id,
                value: payload.clone(),
            },
        );
        let completion = match request(
            node,
            gateway,
            "/v1/operations",
            &cluster.config.data_token,
            Some(operation(id, "table.create", payload.clone())),
            SETUP_DEADLINE,
        )
        .await
        {
            Ok(reply) => reply.completion(id).unwrap_or_else(Completion::Unknown),
            Err(error) => Completion::Unknown(error),
        };
        let result = match &completion {
            Completion::Ok(created) => created["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("table.create lacks table id: {created}")),
            Completion::Refused(error) => Err(format!("table.create: {:?} {}", error.code, error.message)),
            Completion::Unknown(error) => Err(format!("table.create: {error}")),
        };
        let retry = matches!(completion, Completion::Unknown(_));
        history
            .lock()
            .unwrap()
            .complete(&mut client, &cluster.world, attempt, completion);
        if !retry || cluster.world.elapsed() >= deadline {
            return result;
        }
        time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait for transient resource gauges to return to their baseline, or 2 s.
async fn settle(cluster: &Cluster) -> Result<Vec<(u64, Value)>, String> {
    let deadline = cluster.world.elapsed() + Duration::from_secs(2);
    loop {
        let mut clean = true;
        let mut snapshots = Vec::new();
        for node in WORKERS {
            let stats = cluster.stats(node).await?;
            clean &= cluster.checks.resources_at_baseline(node, &stats);
            snapshots.push((node, stats));
        }
        if clean || cluster.world.elapsed() >= deadline {
            return Ok(snapshots);
        }
        time::sleep(STEP).await;
    }
}
