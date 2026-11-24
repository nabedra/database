//! The smoke workload and the per-seed runtime boundary.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_database_server::oltp::pool::registries_empty;
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node, net::ConnId},
    time,
};
use nabedra_sim::{SeedRun, Trace};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    client::{operation, request},
    config::Config,
    disk::DiskCut,
    history::{Client, Completion, History, Operation},
    invariants::{Invariants, STEP, TraceObserver, elapsed_ms},
    node::Cluster,
    sweep::{Defect, RunResult, Setup},
    workloads::{self, Scenarios, Workload},
};

const OPERATIONS: &str = "/v1/operations";
const SETUP_DEADLINE: Duration = Duration::from_secs(20);
const SETTLE_LIMIT: Duration = Duration::from_secs(2);

pub(crate) fn run_seed(seed: u64, generator: &mut Gen, defect: Defect, setup: &Setup) -> RunResult {
    run(seed, generator, defect, None, setup)
}

fn run(
    seed: u64,
    generator: &mut Gen,
    defect: Defect,
    cold_start: Option<DiskCut>,
    setup: &Setup,
) -> RunResult {
    let workload = setup.workloads.of(seed);
    let mut config = Config::sample(seed);
    if workloads::paused::samples(seed, workload) || setup.selection.focus.d05 {
        let deadline = workloads::paused::DEADLINE.as_millis() as u64;
        config.force("request_deadline_ms", json!(deadline));
    }
    if setup.selection.focus.batch {
        config.force("data.direct.batch", json!(true));
    }
    let program = (workload == Workload::Smoke).then(|| Program::generate(generator));
    let (mut result, world) = nabedra_sim::runtime::run(seed, |world| async move {
        let body = Body {
            seed,
            config,
            program,
            defect,
            cold_start,
            workload,
            setup,
            generator,
        };
        let result = execute(body, world.clone()).await;
        (result, world)
    });
    // The runtime has dropped, including cancelled task futures.
    result.blocking_cpu = world.blocking_cpu_time();
    let live = live_after(world);
    let clean = live.is_empty();
    result.assertions.insert("registries empty", clean);
    if !clean {
        result.run.violations.push(format!(
            "t={}ms RegistriesEmpty: live entries after runtime shutdown: {live:?}",
            result.run.virtual_ms
        ));
    }
    result
}

/// What a finished run still keeps alive. The registries hold weak references, so they
/// miss a cycle through the world itself, which owns every mounted image: the world must
/// be gone once this last handle drops. Entries are filtered by this world, so parallel
/// seeds cannot alias its mounts.
pub(crate) fn live_after(world: Arc<World>) -> Vec<&'static str> {
    // The blocking helper replies before it drops its job's handle on the world, and it
    // takes one job at a time, so a later job's reply means that handle is gone.
    nabedra_sim::runtime::run(0, |_| async {
        nabedra_rt::task::spawn_blocking(|| ())
            .await
            .expect("simulation blocking helper");
    });
    let mut live = world.live_registries();
    if !registries_empty(&world) {
        live.push("disk budget registry");
    }
    let released = Arc::downgrade(&world);
    drop(world);
    if released.strong_count() != 0 {
        live.push("World");
    }
    live
}

struct Program {
    key: i64,
    initial: i64,
    following: i64,
    extra_reads: Vec<i64>,
}

impl Program {
    fn generate(generator: &mut Gen) -> Self {
        let key = generator.range_i64(1, 30);
        let initial = generator.range_i64(0, 10_000);
        let following = generator.range_i64(10_001, 20_000);
        let extra = generator.below(8);
        Self {
            key,
            initial,
            following,
            extra_reads: (0..extra).map(|_| generator.range_i64(31, 60)).collect(),
        }
    }
}

struct Body<'a> {
    seed: u64,
    config: Config,
    program: Option<Program>,
    defect: Defect,
    cold_start: Option<DiskCut>,
    workload: Workload,
    setup: &'a Setup,
    generator: &'a mut Gen,
}

async fn execute(body: Body<'_>, world: Arc<World>) -> RunResult {
    let Body {
        seed,
        config,
        program,
        defect,
        cold_start,
        workload,
        setup,
        generator,
    } = body;
    let mut scenarios = Scenarios::new();
    let mut summary = Value::Null;
    config.setup(&world);
    let configuration = config.report();
    let trace = Arc::new(Mutex::new(RunTrace::default()));
    let observer: TraceObserver = {
        let trace = trace.clone();
        Arc::new(move |ms, line| trace.lock().unwrap().record(ms, None, &line))
    };
    observer(0, format!("configuration {}", configuration["hash"]));
    let checks = Invariants::new();
    checks.set_trace(observer.clone());
    let history = Arc::new(Mutex::new(History::default()));
    history.lock().unwrap().set_trace(observer.clone());
    let mut assertions = BTreeMap::new();
    // This ticker uses runtime time. Drifting node clocks never alter the
    // invariant sampling frequency.
    let ticker = {
        let world = world.clone();
        let checks = checks.clone();
        nabedra_rt::task::spawn(async move {
            loop {
                time::sleep(STEP).await;
                checks.step(&world);
            }
        })
    };
    let network_observer = {
        let trace = trace.clone();
        move |ms, conn, line: &str| trace.lock().unwrap().record(ms, conn, line)
    };
    match Cluster::new(
        world.clone(),
        checks.clone(),
        seed,
        observer,
        network_observer,
    )
    .await
    {
        Ok(mut cluster) => {
            let startup = cluster.start().await;
            assertions.insert("cluster readiness", startup.is_ok());
            match startup {
                Ok(()) if workload != Workload::Smoke => {
                    let report = if workload == Workload::Topology {
                        workloads::topology::run(
                            &mut cluster,
                            &config,
                            history.clone(),
                            generator,
                            seed,
                            &setup.selection,
                            setup.quiet,
                        )
                        .await
                    } else {
                        workloads::direct::run(
                            &mut cluster,
                            &config,
                            history.clone(),
                            generator,
                            seed,
                            workload,
                            &setup.selection,
                            setup.quiet,
                        )
                        .await
                    };
                    match report {
                        Ok(report) => {
                            for (name, passed) in report.assertions {
                                assertions.insert(name, passed);
                            }
                            scenarios = report.scenarios;
                            summary = report.summary;
                        }
                        Err(error) => {
                            assertions.insert("public HTTP workload completed", false);
                            checks.violate(&world, "Workload", error);
                        }
                    }
                }
                Ok(()) => {
                    let program = program.as_ref().expect("smoke program");
                    let mut history = history.lock().unwrap();
                    match smoke(&mut cluster, &config, program, defect, &mut history).await {
                        Ok(table) => {
                            assertions.insert("public HTTP workload completed", true);
                            if let Some(cut) = cold_start {
                                let started = world.elapsed();
                                let recovered = verify_cold_start(
                                    &mut cluster,
                                    &mut history,
                                    &table,
                                    program,
                                    cut,
                                )
                                .await;
                                assertions.insert(
                                    "cold start preserved acknowledged data",
                                    recovered.is_ok(),
                                );
                                if let Err(error) = recovered {
                                    checks.violate(&world, "ColdLifecycle", error);
                                }
                                println!(
                                    "fault model cold bootstrap seed {seed}: {:?} virtual",
                                    world.elapsed() - started
                                );
                            }
                            for (name, violation) in [
                                ("direct path observed", "DirectPath"),
                                ("public resource baseline", "ResourceBaseline"),
                                ("original keyed retry result", "DedupUnique"),
                            ] {
                                assertions.insert(name, !has_violation(&checks, violation));
                            }
                        }
                        Err(error) => {
                            assertions.insert("public HTTP workload completed", false);
                            checks.violate(&world, "SmokeWorkload", error);
                        }
                    }
                }
                Err(error) => checks.violate(&world, "Bootstrap", error),
            }
            cluster.teardown().await;
            assertions.insert(
                "crash teardown",
                !has_violation(&checks, "CrashOrder") && !has_violation(&checks, "ProcessExit"),
            );
        }
        Err(error) => checks.violate(&world, "Bootstrap", error),
    }
    ticker.abort();
    let _ = ticker.await;
    checks.finish(&world);
    assertions.insert(
        "mounted device identity",
        !has_violation(&checks, "MountedDeviceIdentity"),
    );
    assertions.insert("liveness", !has_violation(&checks, "Liveness"));
    let history = std::mem::take(&mut *history.lock().unwrap());
    if history.events().iter().any(|event| event.key.is_some()) {
        let verdict = history.check_direct();
        assertions.insert(
            "linearizability and keyed dedup",
            verdict.anomalies.is_empty(),
        );
        checks.check_history(&world, &verdict);
    }
    let mut capture = trace.lock().unwrap();
    capture.flush_frames();
    let trace = &capture.trace;
    RunResult {
        run: SeedRun {
            seed,
            trace_hash: trace.hash(),
            trace_events: trace.events(),
            trace_tail: trace.tail(),
            full_trace: trace.full().map(<[String]>::to_vec),
            violations: checks.violations(),
            history: history.jsonl(),
            config: configuration,
            virtual_ms: elapsed_ms(&world),
            ..SeedRun::default()
        },
        assertions,
        blocking_cpu: None,
        workload,
        scenarios,
        summary,
    }
}

#[derive(Default)]
struct RunTrace {
    trace: Trace,
    frame_time: u64,
    frames: Vec<(ConnId, String)>,
}

impl RunTrace {
    fn record(&mut self, ms: u64, connection: Option<ConnId>, line: &str) {
        if self.frame_time != ms || connection.is_none() {
            self.flush_frames();
        }
        if let Some(connection) = connection {
            self.frame_time = ms;
            self.frames.push((connection, line.to_owned()));
        } else {
            self.trace.record(ms, format_args!("{line}"));
        }
    }

    fn flush_frames(&mut self) {
        // Independent connections can deliver several frames at the same
        // virtual instant. Normalize their order, keeping order within each
        // connection and flushing before any history, probe or nemesis event.
        self.frames.sort_by_key(|(conn, _)| *conn);
        for (_, line) in self.frames.drain(..) {
            self.trace.record(self.frame_time, format_args!("{line}"));
        }
    }
}

fn has_violation(checks: &Invariants, name: &str) -> bool {
    checks
        .violations()
        .iter()
        .any(|line| nabedra_sim::shrink::violation_kind(line) == name)
}

struct Scenario<'a> {
    cluster: &'a Cluster,
    history: &'a mut History,
    actors: [Client; 2],
    deadline: Duration,
}

impl Scenario<'_> {
    async fn call(
        &mut self,
        actor: usize,
        name: &str,
        payload: Value,
        id: Uuid,
        point: Option<(&str, i64)>,
        observed_value: Option<Value>,
    ) -> Result<Value, String> {
        let node = CLIENTS[actor];
        let gateway = GATEWAYS[actor];
        let client = &mut self.actors[actor];
        let table = point.map(|(table, _)| table);
        let key = point.map(|(_, key)| key.to_string());
        let input = match name {
            "table.update" => json!({"id": payload["key"], "value": payload["values"]["value"]}),
            "table.get" => Value::Null,
            _ => payload
                .get("row")
                .cloned()
                .unwrap_or_else(|| payload.clone()),
        };
        let attempt = self.history.invoke(
            client,
            &self.cluster.world,
            gateway,
            Operation {
                table,
                name,
                key: key.as_deref(),
                request_id: id,
                value: input,
            },
        );
        let deadline = if table.is_none() {
            SETUP_DEADLINE
        } else {
            self.deadline
        };
        let completion = match request(
            node,
            gateway,
            OPERATIONS,
            &self.cluster.config.data_token,
            Some(operation(id, name, payload)),
            deadline,
        )
        .await
        {
            Ok(reply) => reply.completion(id),
            Err(error) => Ok(Completion::Unknown(error)),
        };
        let completion = match completion {
            Ok(Completion::Ok(_)) if observed_value.is_some() => {
                Completion::Ok(observed_value.unwrap())
            }
            Ok(completion) => completion,
            Err(error) => Completion::Unknown(error),
        };
        let result = match &completion {
            Completion::Ok(value) => Ok(value.clone()),
            Completion::Refused(error) => Err(format!(
                "{name} via n{gateway}: {:?}: {}",
                error.code, error.message
            )),
            Completion::Unknown(error) => Err(format!("{name} via n{gateway}: {error}")),
        };
        self.history
            .complete(client, &self.cluster.world, attempt, completion);
        result
    }

    async fn id(&self, actor: usize) -> Uuid {
        in_node(CLIENTS[actor], async { uuid_v4() }).await
    }

    async fn get(
        &mut self,
        actor: usize,
        table: &str,
        key: i64,
        substitute: Option<Value>,
    ) -> Result<Value, String> {
        let id = self.id(actor).await;
        self.call(
            actor,
            "table.get",
            json!({"table_id":table,"key":key}),
            id,
            Some((table, key)),
            substitute,
        )
        .await
    }
}

async fn smoke(
    cluster: &mut Cluster,
    config: &Config,
    program: &Program,
    defect: Defect,
    history: &mut History,
) -> Result<String, String> {
    cluster.wait_direct().await?;
    let actors = [history.client(), history.client()];
    let mut scenario = Scenario {
        cluster,
        history,
        actors,
        deadline: config.deadline(),
    };
    let create_id = scenario.id(0).await;
    let created = scenario
        .call(
            0,
            "table.create",
            json!({
                "name":"harness_smoke", "schema":{"id":"int64","value":"int64"},
                "key":"id", "format":"parquet",
            }),
            create_id,
            None,
            None,
        )
        .await?;
    let table = created["id"]
        .as_str()
        .ok_or_else(|| format!("table.create lacks table id: {created}"))?;
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }

    config.jump_clocks(&cluster.world);
    scenario
        .history
        .marker(&cluster.world, "wall-jumps", Value::Null);
    let key = program.key;
    let row = |value| json!({"table_id": table, "row": {"id": key, "value": value}});
    let insert_id = scenario.id(0).await;
    let original = scenario
        .call(
            0,
            "table.insert",
            row(program.initial),
            insert_id,
            Some((table, key)),
            None,
        )
        .await?;
    let old_row = scenario.get(1, table, key, None).await?;
    let update_id = scenario.id(1).await;
    let update = json!({"table_id": table, "key": key, "values": {"value": program.following}});
    scenario
        .call(
            1,
            "table.update",
            update,
            update_id,
            Some((table, key)),
            None,
        )
        .await?;

    // Retry an already acknowledged request where re-execution would conflict.
    // The second gateway must return the original success envelope's payload.
    let replay = scenario
        .call(
            1,
            "table.insert",
            row(program.initial),
            insert_id,
            Some((table, key)),
            None,
        )
        .await?;
    if replay != original {
        cluster.checks.violate(
            &cluster.world,
            "DedupUnique",
            "retry payload disagrees with original success",
        );
    }

    if matches!(defect, Defect::LostWrite) {
        scenario
            .history
            .marker(&cluster.world, "seeded-lost-write", json!({"key":key}));
        // Remove the row after its real acknowledgement. This operation is
        // only the fault injection and is not part of the client's history.
        let id = scenario.id(0).await;
        let reply = request(
            CLIENTS[0],
            GATEWAYS[0],
            OPERATIONS,
            &cluster.config.data_token,
            Some(operation(
                id,
                "table.delete",
                json!({"table_id":table,"key":key}),
            )),
            config.deadline(),
        )
        .await?
        .completion(id)?;
        if !matches!(reply, Completion::Ok(Value::Bool(true))) {
            return Err("seeded lost write could not remove its acknowledged row".into());
        }
    }
    let stale = matches!(defect, Defect::StaleRead).then_some(old_row);
    if stale.is_some() {
        scenario
            .history
            .marker(&cluster.world, "seeded-stale-read", json!({"key":key}));
    }
    scenario.get(0, table, key, stale).await?;
    for &key in &program.extra_reads {
        scenario.get(1, table, key, None).await?;
    }
    let stats = settle(cluster).await?;
    cluster.checks.check_direct_path(&cluster.world, &stats);
    for (node, stats) in stats {
        cluster.checks.check_resources(&cluster.world, node, &stats);
    }
    Ok(table.to_owned())
}

async fn verify_cold_start(
    cluster: &mut Cluster,
    history: &mut History,
    table: &str,
    program: &Program,
    cut: DiskCut,
) -> Result<(), String> {
    let previous = cluster.config.run_id;
    cluster.cold_start(cut).await?;
    if cluster.config.run_id == previous {
        return Err("cold start retained the previous worker run".into());
    }
    let actors = [history.client(), history.client()];
    let mut scenario = Scenario {
        cluster,
        history,
        actors,
        deadline: SETUP_DEADLINE,
    };
    // The history includes both the acknowledged write from the old run and
    // the read through the original gateway in the new run.
    let row = scenario.get(0, table, program.key, None).await?;
    if row != json!({"id":program.key,"value":program.following}) {
        return Err(format!(
            "cold start read {row} for acknowledged key {}",
            program.key
        ));
    }
    Ok(())
}

async fn settle(cluster: &Cluster) -> Result<Vec<(u64, Value)>, String> {
    let deadline = cluster.world.elapsed() + SETTLE_LIMIT;
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

#[cfg(test)]
mod tests {
    use super::*;

    use nabedra_sim::sweep::{SweepConfig, sweep};

    /// Seed 66: with fewer session slots than peers the second gateway never activated the
    /// direct path, so its retry of a committed insert was answered `outcome_unknown` for ever.
    #[test]
    fn smoke_seed_66_reaches_the_direct_path_through_both_gateways() {
        let mut config = SweepConfig::new("oltp-sim-smoke-66", 66, 1);
        config.require_seeded = true;
        config.verify_replay = 1.0;
        config.replay = concat!(
            "RUST_MIN_STACK=16777216 RUSTFLAGS=\"--cfg tokio_unstable\" ",
            "CARGO_TARGET_DIR=target/sim cargo test --locked --release ",
            "-p nabedra-sim-oltp --lib ",
            "workload::tests::smoke_seed_66_reaches_the_direct_path_through_both_gateways ",
            "-- --exact --nocapture --test-threads=1",
        )
        .into();
        if let Some(dir) = std::env::var_os("NABEDRA_SIM_ARTIFACTS") {
            config.artifacts = dir.into();
        }
        let setup = Setup::smoke();
        let report = sweep(&config, |seed, generator| {
            run(seed, generator, Defect::None, None, &setup).run
        });
        report.assert_passed();
    }

    #[test]
    fn cold_start_reopens_mounted_workers() {
        let mut config = SweepConfig::new("oltp-sim-cold-start", 1, 1);
        config.require_seeded = true;
        config.verify_replay = 1.0;
        config.replay = concat!(
            "RUST_MIN_STACK=16777216 RUSTFLAGS=\"--cfg tokio_unstable\" ",
            "CARGO_TARGET_DIR=target/sim cargo test --locked --release ",
            "-p nabedra-sim-oltp --lib workload::tests::cold_start_reopens_mounted_workers ",
            "-- --exact --nocapture --test-threads=1",
        )
        .into();
        if let Some(dir) = std::env::var_os("NABEDRA_SIM_ARTIFACTS") {
            config.artifacts = dir.into();
        }
        let setup = Setup::smoke();
        let report = sweep(&config, |seed, generator| {
            run(seed, generator, Defect::None, Some(DiskCut::Power), &setup).run
        });
        report.assert_passed();
        let seed = &report.seeds[0];
        println!(
            "Fault model cold start seed {} replayed: trace {:016x} ({} events)",
            seed.seed, seed.trace_hash, seed.trace_events,
        );
    }
}
