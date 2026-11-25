//! The `direct` workload and the run phases it shares with `cold` and `move`: clients under
//! chaos, heal, converge, then a final read of every key through both gateways.

use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use serde_json::{Value, json};

use super::{
    Scenarios, Workload, clauses,
    clients::{self, Ctx, Plan},
    cold, corrupt, moves, old_peer, paused,
    resources::Overlay,
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    client::{operation, request},
    config::Config,
    history::{Completion, History, Operation},
    invariants::{STEP, elapsed_ms},
    nemesis::{NetFaults, Selection, apply::Chaos},
    node::Cluster,
};

/// Virtual time of chaos before the heal (design 3.6).
pub(crate) const CHAOS: Duration = Duration::from_secs(20);
/// Bound for convergence after the heal: every retry and the final reads (design 3.6).
pub(crate) const CONVERGE_LIMIT: Duration = Duration::from_secs(30);
const SETUP_DEADLINE: Duration = Duration::from_secs(20);
const CONVERGE_POLL: Duration = Duration::from_millis(100);
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
    overlay: bool,
) -> Result<Report, String> {
    cluster.wait_direct().await?;
    let table = create_table(cluster, &history, "sim_direct").await?;
    // Before the resource baseline, as the workload's own table is.
    let overlay = if overlay {
        let table = create_table(cluster, &history, "sim_resources").await?;
        Some((Overlay::draw(seed, CHAOS), table))
    } else {
        None
    };
    for (node, stats) in crate::workload::steady(cluster).await? {
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
    let faults = NetFaults::install(cluster.net(), &cluster.world, seed);
    config.jump_clocks(&cluster.world);
    let start = elapsed_ms(&cluster.world);
    let profile = selection.profile(seed);
    let mut chaos = (!quiet && profile.total_weight() > 0)
        .then(|| Chaos::new(profile, faults.clone(), start, g));
    let mut moving = (workload == Workload::Move).then(|| {
        let mut moving = moves::Move::draw(g);
        moving.set_faults(faults.clone());
        moving
    });
    let staggered = (workload == Workload::Cold).then(|| cold::Stop::draw(g));
    cluster.record(format!(
        "workload {workload} profile {} clients {} keys {}",
        chaos.as_ref().map_or("none", |chaos| chaos.profile_name()),
        plan.programs.len(),
        plan.keys,
    ));
    let mut overlay = overlay.map(|(mut overlay, table)| {
        overlay.arm(cluster, &ctx, table);
        overlay
    });
    let tasks: Vec<_> = plan
        .programs
        .into_iter()
        .map(|program| nabedra_rt::task::spawn(clients::run(ctx.clone(), program)))
        .collect();

    let end = cluster.world.elapsed() + CHAOS;
    while cluster.world.elapsed() < end {
        if let Some(chaos) = chaos.as_mut() {
            chaos.tick(cluster, &ctx, g).await;
        }
        if let Some(moving) = moving.as_mut() {
            moving.tick(cluster, &ctx).await;
        }
        if let Some(overlay) = overlay.as_mut() {
            overlay.tick(cluster, &ctx, chaos.as_mut()).await;
        }
        time::sleep(TICK).await;
    }
    if let Some(overlay) = overlay.as_mut() {
        overlay.stop(cluster).await;
    }

    let mut report = Report::default();
    if let Some(stop) = staggered {
        // Heal, then stop every worker while the clients still send requests, and reopen
        // the workers staggered. Restarts the nemesis owes would only race the full stop.
        if let Some(chaos) = chaos.as_mut() {
            if let Err(error) = chaos.heal(cluster, &ctx).await {
                cluster
                    .checks
                    .violate(&cluster.world, "Liveness", format!("heal: {error}"));
                return abandon(cluster, &ctx, tasks, report).await;
            }
            chaos.before_full_stop(cluster, &ctx).await;
        }
        let started = cluster.world.elapsed();
        let cold = stop.run(cluster, &ctx).await;
        if let Some(chaos) = chaos.as_mut() {
            chaos.after_full_stop(cluster).await;
        }
        report
            .assertions
            .push(("cold start within bound", cold.is_ok()));
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
        report.scenarios.insert("O15:cold-start");
    }
    ctx.stopping.store(true, Ordering::Release);

    if let Some(chaos) = chaos.as_mut()
        && let Err(error) = chaos.heal(cluster, &ctx).await
    {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &ctx, tasks, report).await;
    }
    // A run without a nemesis still delays the old voters of a move until here.
    faults.clear();
    // Everything is undone and every process is started. The bounds below run from here.
    let healed = cluster.world.elapsed();
    cluster
        .checks
        .expect_recovery(&cluster.world, "convergence", CONVERGE_LIMIT);
    let converge_by = elapsed_ms(&cluster.world) + CONVERGE_LIMIT.as_millis() as u64;
    ctx.converge_by.store(converge_by, Ordering::Release);
    if let Err(error) = cluster.wait_direct().await {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &ctx, tasks, report).await;
    }
    // Convergence may be polled for 30 s after the heal. Everything after it is sent once.
    let converged = converge(cluster, &ctx, healed + CONVERGE_LIMIT).await;
    report
        .assertions
        .push(("converged after the heal", converged.is_ok()));
    if let Err(error) = &converged {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("convergence: {error}"));
    }
    for task in tasks {
        let _ = task.await;
    }
    cluster.checks.recovered(&cluster.world, "convergence");
    // The final operations may wait for a running move, within the move's own bound (lead
    // decision 12.3). A move that does not finish is reported by its own rule, below.
    // A pending split is tried meanwhile, and its bound runs from here, not from the end of
    // the wait.
    let quiet_from = cluster.world.elapsed();
    let mut move_wait_ms = 0;
    if let Some(moving) = moving.as_mut().filter(|moving| moving.running()) {
        while moving.waiting(cluster, healed) {
            moving.retry(cluster, &ctx, healed).await;
            if let Some(chaos) = chaos.as_mut() {
                chaos.try_split(cluster, &ctx).await;
            }
        }
        move_wait_ms = elapsed_ms(&cluster.world) - quiet_from.as_millis() as u64;
        cluster.record(format!(
            "final operations waited {move_wait_ms}ms for the replica move, still running {}",
            moving.running()
        ));
    }
    let final_checks = clients::final_checks(&ctx).await;
    report.assertions.push((
        "final writes and reads through both gateways",
        final_checks.is_ok(),
    ));
    if let Err(error) = &final_checks {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", error.clone());
    }
    let mut reread = false;
    if let Some(chaos) = chaos.as_mut().filter(|chaos| chaos.split_pending()) {
        chaos.finish_split(cluster, &ctx, quiet_from).await;
        reread = true;
    }
    if let Some(chaos) = chaos.as_ref() {
        chaos.check_abandoned(cluster, &ctx).await;
    }
    if let Some(moving) = moving.as_mut() {
        let moved = moving.finish(cluster, &ctx, healed).await;
        report
            .assertions
            .push(("replica move retired", moved.is_ok()));
        match moved {
            Ok(()) => {
                if let Err(error) = moving.check_retired(cluster, &ctx).await {
                    cluster.checks.violate(
                        &cluster.world,
                        "Liveness",
                        format!("after the replica move: {error}"),
                    );
                }
                match clients::exact_count(&ctx).await {
                    Ok(None) => {
                        report.scenarios.insert("O26:exact-count");
                    }
                    Ok(Some(wrong)) => {
                        cluster.checks.violate(&cluster.world, "ExactCount", wrong);
                        report.scenarios.insert("O26:exact-count");
                    }
                    Err(error) => cluster.checks.violate(
                        &cluster.world,
                        "Liveness",
                        format!("count after the replica move: {error}"),
                    ),
                }
            }
            Err(error) => {
                cluster
                    .checks
                    .violate(&cluster.world, "Liveness", format!("replica move: {error}"))
            }
        }
        reread = true;
    }
    if reread && let Err(error) = clients::final_checks(&ctx).await {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("after the split or move: {error}"),
        );
    }

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
    // The drain moves the replica once more, so it has its own convergence and final reads.
    if let Some(moving) = moving.as_mut() {
        let split = chaos.as_ref().is_some_and(Chaos::split_requested);
        if let Some(why) = moving.drain_blocked(cluster, split) {
            cluster.record(format!("O26 drain not run: {why}"));
        } else {
            let drained = match moving.drain_pool(cluster, &ctx).await {
                Ok(()) => {
                    let limit = cluster.world.elapsed() + CONVERGE_LIMIT;
                    converge(cluster, &ctx, limit).await
                }
                Err(error) => Err(error),
            };
            let checked = match drained {
                Ok(()) => clients::final_checks(&ctx).await,
                Err(error) => Err(error),
            };
            if let Err(error) = checked {
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("draining the moved pool: {error}"),
                );
            }
        }
        report.scenarios.extend(std::mem::take(&mut moving.clauses));
    }
    if old_peer::selected(seed, workload, selection.focus.old_peer) {
        if converged.is_ok() && final_checks.is_ok() && cluster.converged().await {
            let observed = old_peer::run(cluster, &ctx, &faults).await;
            report.scenarios.extend(observed);
        } else {
            cluster.record("D10 old peer not run: the cluster did not converge".into());
        }
    }
    if paused::selected(config, workload) {
        if converged.is_ok() && final_checks.is_ok() && cluster.converged().await {
            let observed = paused::run(cluster, &ctx, config.deadline()).await;
            report.scenarios.extend(observed);
        } else {
            cluster.record("D05 not run: the cluster did not converge".into());
        }
    }

    if let Some(overlay) = overlay.as_mut() {
        if converged.is_ok() && final_checks.is_ok() && cluster.converged().await {
            report.scenarios.extend(overlay.stage(cluster, config.deadline()).await);
        } else {
            cluster.record("resources overlay stage not run: the cluster did not converge".into());
        }
    }

    let no_quorum = cluster.no_quorum();
    let drops = chaos.as_ref().map(Chaos::session_drops).unwrap_or_default();
    let outcome = clauses::Evidence {
        faults: chaos.as_ref().map_or(&[], |chaos| &chaos.log),
        attempts: &observed.attempts,
        resolved: &observed.resolved,
        reuse_same_key: observed.reuse_same_key,
        reuse_other_key: observed.reuse_other_key,
        session_drops: &drops,
        no_quorum: &no_quorum,
        moving: moving.as_ref().and_then(moves::Move::window),
        deadline_ms: config.deadline().as_millis() as u64,
    }
    .evaluate();
    for (invariant, detail) in outcome.violations {
        cluster.checks.violate(&cluster.world, invariant, detail);
    }
    report.scenarios.extend(outcome.clauses);

    let extra = moving
        .as_ref()
        .map_or(Vec::new(), |moving| moving.extra_pools(&cluster.world));
    let (stats, leftover) = settle(cluster, &extra).await?;
    if !selection.focus.idle {
        cluster.checks.check_direct_path(&cluster.world, &stats);
    }
    for (node, stats) in &stats {
        cluster.checks.check_resources(&cluster.world, *node, stats);
    }
    if let Some((node, row)) = leftover {
        cluster.checks.violate(
            &cluster.world,
            "ResourceBaseline",
            format!("n{node} keeps reservations in an added pool after quiescence: {row}"),
        );
    }
    // Last: a quarantined group stays quarantined, and its leader keeps a replication batch
    // for it, so the resource checks above must not see it.
    if corrupt::selected(seed, workload, selection.focus.corrupt) {
        if converged.is_ok() && final_checks.is_ok() && cluster.converged().await {
            let observed = corrupt::run(cluster, &ctx).await;
            report.scenarios.extend(observed);
            let healed = cluster.world.elapsed();
            let after = match converge(cluster, &ctx, healed + CONVERGE_LIMIT).await {
                Ok(()) => clients::final_checks(&ctx).await,
                Err(error) => Err(error),
            };
            if let Err(error) = after {
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("after the O06 stage: {error}"),
                );
            }
        } else {
            cluster.record("O06 stage not run: the cluster did not converge".into());
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
        "attempts": observed.attempts.len(),
        "resolved_by_retry": observed.resolved.len(),
        "reuse_same_key": observed.reuse_same_key,
        "reuse_other_key": observed.reuse_other_key,
        "not_applied_checks": observed.not_applied_checks,
        "unresolved": observed.unresolved.len(),
        "one_paused_late": outcome.one_paused_late,
        "move_wait_ms": move_wait_ms,
        "heal_cold_start": chaos.as_ref().is_some_and(|chaos| chaos.heal_cold),
    });
    if let Some(overlay) = &overlay {
        report.summary["resources"] = overlay.summary(&cluster.checks, &cluster.world);
    }
    cluster.record(format!("workload summary {}", report.summary));
    Ok(report)
}

/// Poll until every worker and gateway reports the run ready with the direct path active, and
/// a read of one key succeeds through each gateway.
async fn converge(cluster: &Cluster, ctx: &Ctx, deadline: Duration) -> Result<(), String> {
    loop {
        let state = if cluster.converged().await {
            clients::read_each(ctx).await
        } else {
            Err("some worker or gateway is not ready".into())
        };
        match state {
            Ok(()) => return Ok(()),
            Err(error) if cluster.world.elapsed() >= deadline => return Err(error),
            Err(_) => time::sleep(CONVERGE_POLL).await,
        }
    }
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

/// Create the workload's table through the first gateway, retrying the same ID.
async fn create_table(
    cluster: &Cluster,
    history: &Arc<Mutex<History>>,
    name: &str,
) -> Result<String, String> {
    let node = CLIENTS[0];
    let gateway = GATEWAYS[0];
    let id = in_node(node, async { uuid_v4() }).await;
    let payload = json!({
        "name": name, "schema": {"id": "int64", "value": "int64"},
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
            Completion::Refused(error) => {
                Err(format!("table.create: {:?} {}", error.code, error.message))
            }
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

/// Wait for transient resource gauges to return to their baseline, or 2 s. The pools a move
/// mounted have no row in the baseline, so they are split off: they must reserve nothing.
async fn settle(
    cluster: &Cluster,
    extra: &[ExtraPool],
) -> Result<(Vec<(u64, Value)>, Option<(u64, String)>), String> {
    let deadline = cluster.world.elapsed() + Duration::from_secs(2);
    loop {
        let mut clean = true;
        let mut snapshots = Vec::new();
        let mut leftover = None;
        for node in WORKERS {
            let mut stats = cluster.stats(node).await?;
            if let Some(rows) = stats["storage_reservations"].as_array_mut() {
                rows.retain(|row| {
                    let extra = extra
                        .iter()
                        .any(|&(moved, device)| moved == node && row["device"] == device);
                    if extra && (row["reserved_bytes"] != 0 || row["reservations"] != 0) {
                        leftover = Some((node, row.to_string()));
                    }
                    !extra
                });
            }
            clean &= cluster.checks.resources_at_baseline(node, &stats);
            snapshots.push((node, stats));
        }
        if clean && leftover.is_none() || cluster.world.elapsed() >= deadline {
            return Ok((snapshots, leftover));
        }
        time::sleep(STEP).await;
    }
}

/// The node and device of a pool that a replica move mounted.
pub(crate) type ExtraPool = (u64, u64);
