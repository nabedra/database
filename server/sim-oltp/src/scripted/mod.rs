//! Scripted fault-model schedules: one fixed order of public HTTP calls and link faults.
//!
//! A schedule holds and heals links with [`World::black_hole`] and [`World::heal`]. That is
//! a link outage: bytes already written to a TCP connection arrive, in order, after the heal.
//! No schedule reorders frames inside one connection. These tests are ignored, because they
//! record open findings and no check runs them.
#![cfg(test)]

mod oltp_direct;

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use nabedra_database_server::{oltp::pool::registries_empty, oltp_runtime::sim::Limits};
use nabedra_protocol::protocol::Outcome;
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node, net::ConnId},
    time,
};
use nabedra_sim::{SeedRun, Trace};
use nabedra_transport::SimLimits;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    client::{operation, request},
    config::Config,
    history::{Client, Completion, History, Operation},
    invariants::{Invariants, STEP, TraceObserver, elapsed_ms},
    node::Cluster,
};

/// Longer than the gateway's own deadline, so the gateway always answers first.
const CLIENT_DEADLINE: Duration = Duration::from_secs(30);
/// Covers a delayed attempt's whole budget: the leader starts it on receipt.
const SETTLE: Duration = Duration::from_secs(12);

/// The limits a production process uses (`GATEWAY_DEADLINE`, `DirectConfig::default` and
/// `TransportConfig::default`), instead of the sampled ones.
fn production_limits(world: &World) -> Value {
    world.insert_ext(Limits {
        direct_deadline: Duration::from_secs(10),
        direct_max_in_flight: 256,
        direct_batch: true,
        data_apply_reply_bytes: 1 << 20,
    });
    world.insert_ext(SimLimits {
        session_slots: 256,
        direct_handler_slots: 256,
    });
    json!({
        "direct_deadline_ms": 10_000, "direct_max_in_flight": 256, "direct_batch": true,
        "data_apply_reply_bytes": 1 << 20, "session_slots": 256, "direct_handler_slots": 256,
    })
}

/// Trace lines, with frames of one instant ordered by connection as the smoke workload does.
#[derive(Default)]
struct RunTrace {
    trace: Trace,
    frame_time: u64,
    frames: Vec<(ConnId, String)>,
}

impl RunTrace {
    fn record(&mut self, ms: u64, connection: Option<ConnId>, line: &str) {
        if self.frame_time != ms || connection.is_none() {
            self.flush();
        }
        if let Some(connection) = connection {
            self.frame_time = ms;
            self.frames.push((connection, line.to_owned()));
        } else {
            self.trace.record(ms, format_args!("{line}"));
        }
    }

    fn flush(&mut self) {
        self.frames.sort_by_key(|(conn, _)| *conn);
        for (_, line) in self.frames.drain(..) {
            self.trace.record(self.frame_time, format_args!("{line}"));
        }
    }
}

/// What the client saw. `Unknown` and `NoReply` leave the outcome open.
#[derive(Clone, Debug, PartialEq)]
enum Answer {
    Ok(Value),
    NotApplied(String),
    Unknown(String),
    NoReply(String),
}

/// A request that keeps its ID and body across attempts.
#[derive(Clone)]
struct Request {
    id: Uuid,
    name: &'static str,
    key: i64,
    payload: Value,
}

struct Driver {
    cluster: Cluster,
    history: Arc<Mutex<History>>,
    table: String,
    /// The worker that led the table's only range when the schedule started.
    leader: u64,
}

/// One client actor. Its logical process changes after an unknown outcome.
struct Actor {
    node: u64,
    client: Client,
}

/// Everything an attempt needs, so it can run while the schedule goes on.
#[derive(Clone)]
struct Caller {
    world: Arc<World>,
    history: Arc<Mutex<History>>,
    table: String,
    token: String,
}

impl Caller {
    async fn call(&self, actor: &mut Actor, gateway: u64, sent: &Request) -> Answer {
        let Request {
            id,
            name,
            key,
            payload,
        } = sent;
        let value = match *name {
            "table.update" => json!({"id": key, "value": payload["values"]["value"]}),
            "table.insert" => payload["row"].clone(),
            _ => Value::Null,
        };
        let attempt = self.history.lock().unwrap().invoke(
            &actor.client,
            &self.world,
            gateway,
            Operation {
                table: Some(&self.table),
                name,
                key: Some(&key.to_string()),
                request_id: *id,
                value,
            },
        );
        let reply = request(
            actor.node,
            gateway,
            "/v1/operations",
            &self.token,
            Some(operation(*id, name, payload.clone())),
            CLIENT_DEADLINE,
        )
        .await;
        let completion = match reply.map(|reply| reply.completion(*id)) {
            Ok(Ok(completion)) => completion,
            Ok(Err(error)) | Err(error) => Completion::Unknown(error),
        };
        let answer = match &completion {
            Completion::Ok(value) => Answer::Ok(value.clone()),
            Completion::Refused(error) => {
                let text = format!("{:?}: {}", error.code, error.message);
                match error.outcome {
                    Outcome::NotApplied => Answer::NotApplied(text),
                    Outcome::Unknown => Answer::Unknown(text),
                }
            }
            Completion::Unknown(error) => Answer::NoReply(error.clone()),
        };
        self.history
            .lock()
            .unwrap()
            .complete(&mut actor.client, &self.world, attempt, completion);
        answer
    }
}

impl Driver {
    fn caller(&self) -> Caller {
        Caller {
            world: self.cluster.world.clone(),
            history: self.history.clone(),
            table: self.table.clone(),
            token: self.cluster.config.data_token.clone(),
        }
    }

    fn actor(&self, index: usize) -> Actor {
        Actor {
            node: CLIENTS[index % CLIENTS.len()],
            client: self.history.lock().unwrap().client(),
        }
    }

    async fn request(&self, name: &'static str, key: i64, payload: Value) -> Request {
        Request {
            id: in_node(CLIENTS[0], async { uuid_v4() }).await,
            name,
            key,
            payload,
        }
    }

    async fn insert(&self, key: i64, value: i64) -> Request {
        let payload = json!({"table_id": self.table, "row": {"id": key, "value": value}});
        self.request("table.insert", key, payload).await
    }

    async fn update(&self, key: i64, value: i64) -> Request {
        let payload = json!({"table_id": self.table, "key": key, "values": {"value": value}});
        self.request("table.update", key, payload).await
    }

    async fn delete(&self, key: i64) -> Request {
        let payload = json!({"table_id": self.table, "key": key});
        self.request("table.delete", key, payload).await
    }

    async fn read(&self, actor: &mut Actor, gateway: u64, key: i64) -> Result<Value, String> {
        let payload = json!({"table_id": self.table, "key": key});
        let get = self.request("table.get", key, payload).await;
        match self.caller().call(actor, gateway, &get).await {
            Answer::Ok(value) => Ok(value),
            other => Err(format!("read of {key}: {other:?}")),
        }
    }

    fn mark(&self, action: &str, value: Value) {
        self.history
            .lock()
            .unwrap()
            .marker(&self.cluster.world, action, value);
    }

    /// Holds the links between `gateway` and each of `workers`, both ways.
    fn hold(&self, gateway: u64, workers: &[u64]) {
        self.mark("hold", json!({"gateway": gateway, "workers": workers}));
        for &worker in workers {
            self.cluster.world.black_hole(Some(gateway), Some(worker));
            self.cluster.world.black_hole(Some(worker), Some(gateway));
        }
    }

    fn heal(&self, gateway: u64, workers: &[u64]) {
        self.mark("heal", json!({"gateway": gateway, "workers": workers}));
        for &worker in workers {
            self.cluster.world.heal(Some(gateway), Some(worker));
            self.cluster.world.heal(Some(worker), Some(gateway));
        }
    }

    /// Breaks the held connections, so nothing sent on them is ever delivered.
    fn reset_links(&self, gateway: u64, worker: u64) {
        self.mark("reset", json!({"gateway": gateway, "worker": worker}));
        for conn in self.cluster.world.connections() {
            if (conn.client, conn.server) == (Some(gateway), Some(worker)) {
                self.cluster.world.reset_connection(conn.id);
            }
        }
    }

    /// Public executor counters of the range leader, which name the code path taken.
    async fn mark_counters(&self, label: &str) -> Result<Value, String> {
        let stats = self.cluster.stats(self.leader).await?;
        let direct = &stats["direct"];
        let value = json!({
            "requests": direct["requests"],
            "mutations": direct["mutations"],
            "entries_proposed": direct["entries_proposed"],
            "entries_applied": direct["entries_applied"],
            "read_only": direct["read_only"],
            "replayed": direct["replayed"],
            "failed": direct["failed"],
            "retries": direct["retries"],
            "overloaded_429": direct["overloaded_429"],
            "not_leader": direct["not_leader"],
            "in_flight": direct["in_flight"],
        });
        self.mark(label, value.clone());
        Ok(value)
    }

    fn violate(&self, kind: &str, detail: String) {
        self.cluster
            .checks
            .violate(&self.cluster.world, kind, detail);
    }
}

async fn prepare(
    cluster: &Cluster,
    history: &Arc<Mutex<History>>,
) -> Result<(String, u64), String> {
    cluster.wait_direct().await?;
    let create = in_node(CLIENTS[0], async { uuid_v4() }).await;
    let created = request(
        CLIENTS[0],
        GATEWAYS[0],
        "/v1/operations",
        &cluster.config.data_token,
        Some(operation(
            create,
            "table.create",
            json!({
                "name": "scripted", "schema": {"id": "int64", "value": "int64"},
                "key": "id", "format": "parquet",
            }),
        )),
        CLIENT_DEADLINE,
    )
    .await?
    .completion(create)?;
    let created = match created {
        Completion::Ok(created) => created,
        Completion::Refused(error) => return Err(format!("table.create: {}", error.message)),
        Completion::Unknown(error) => return Err(format!("table.create: {error}")),
    };
    let table = created["id"]
        .as_str()
        .ok_or_else(|| format!("table.create lacks an id: {created}"))?
        .to_owned();
    // Warm both gateways: routes, sessions and role grants. The worker whose applied count
    // moves leads the table's only range.
    let mut applied = Vec::new();
    for worker in WORKERS {
        applied.push(cluster.stats(worker).await?["direct"]["entries_applied"].clone());
    }
    let warm = Caller {
        world: cluster.world.clone(),
        history: history.clone(),
        table: table.clone(),
        token: cluster.config.data_token.clone(),
    };
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let mut actor = Actor {
            node: CLIENTS[index],
            client: history.lock().unwrap().client(),
        };
        let key = 900 + index as i64;
        let insert = Request {
            id: in_node(CLIENTS[index], async { uuid_v4() }).await,
            name: "table.insert",
            key,
            payload: json!({"table_id": table, "row": {"id": key, "value": 1}}),
        };
        match warm.call(&mut actor, gateway, &insert).await {
            Answer::Ok(_) => {}
            other => return Err(format!("warm insert through n{gateway}: {other:?}")),
        }
    }
    let mut leaders = Vec::new();
    for (index, worker) in WORKERS.into_iter().enumerate() {
        let now = cluster.stats(worker).await?["direct"]["entries_applied"].clone();
        if now != applied[index] {
            leaders.push(worker);
        }
    }
    match leaders[..] {
        [leader] => Ok((table, leader)),
        _ => Err(format!("range leaders during warm-up: {leaders:?}")),
    }
}

/// Runs one schedule on a fresh cluster. A schedule that could not reach one of its steps
/// returns an error, reported as `ScheduleStep`: that is not a reproduction.
fn run_schedule<F>(seed: u64, schedule: impl FnOnce(Arc<Driver>) -> F + 'static) -> SeedRun
where
    F: Future<Output = Result<(), String>>,
{
    let (mut run, world) = nabedra_sim::runtime::run(seed, |world| async move {
        let config = Config::sample(seed);
        config.setup(&world);
        let limits = production_limits(&world);
        let trace = Arc::new(Mutex::new(RunTrace::default()));
        let observer: TraceObserver = {
            let trace = trace.clone();
            Arc::new(move |ms, line| trace.lock().unwrap().record(ms, None, &line))
        };
        observer(
            0,
            format!("configuration {} {limits}", config.report()["hash"]),
        );
        let checks = Invariants::new();
        checks.set_trace(observer.clone());
        let history = Arc::new(Mutex::new(History::default()));
        history.lock().unwrap().set_trace(observer.clone());
        let ticker = {
            let (world, checks) = (world.clone(), checks.clone());
            nabedra_rt::task::spawn(async move {
                loop {
                    time::sleep(STEP).await;
                    checks.step(&world);
                }
            })
        };
        let frames = {
            let trace = trace.clone();
            move |ms, conn, line: &str| trace.lock().unwrap().record(ms, conn, line)
        };
        match Cluster::new(world.clone(), checks.clone(), seed, observer, frames).await {
            Err(error) => checks.violate(&world, "Bootstrap", error),
            Ok(mut cluster) => {
                let ready = match cluster.start().await {
                    Ok(()) => prepare(&cluster, &history).await,
                    Err(error) => Err(error),
                };
                match ready {
                    Err(error) => {
                        checks.violate(&world, "Bootstrap", error);
                        cluster.teardown().await;
                    }
                    Ok((table, leader)) => {
                        let driver = Arc::new(Driver {
                            cluster,
                            history: history.clone(),
                            table,
                            leader,
                        });
                        driver.mark("leader", json!(leader));
                        if let Err(error) = schedule(driver.clone()).await {
                            checks.violate(&world, "ScheduleStep", error);
                        }
                        match Arc::try_unwrap(driver) {
                            Ok(mut driver) => driver.cluster.teardown().await,
                            Err(_) => checks.violate(
                                &world,
                                "ScheduleStep",
                                "the schedule kept the cluster",
                            ),
                        }
                    }
                }
            }
        }
        ticker.abort();
        let _ = ticker.await;
        checks.finish(&world);
        let history = history.lock().unwrap();
        let verdict = history.check_direct();
        // Recorded even when clean: whether the history checker sees a finding is a result.
        let checker: Vec<_> = verdict.anomalies.iter().map(|a| a.kind.clone()).collect();
        checks.check_history(&world, &verdict);
        let mut trace = trace.lock().unwrap();
        trace.flush();
        let run = SeedRun {
            seed,
            trace_hash: trace.trace.hash(),
            trace_events: trace.trace.events(),
            trace_tail: trace.trace.tail(),
            full_trace: trace.trace.full().map(<[String]>::to_vec),
            violations: checks.violations(),
            history: history.jsonl(),
            config: json!({
                "sampled": config.report(), "limits": limits, "history_checker": checker,
                "leader_hint": history.events().iter().find(|e| e.f == "leader").map(|e| &e.value),
            }),
            virtual_ms: elapsed_ms(&world),
            ..SeedRun::default()
        };
        (run, world)
    });
    let live = world.live_registries();
    if !(registries_empty(&world) && live.is_empty()) {
        run.violations
            .push(format!("RegistriesEmpty: live entries: {live:?}"));
    }
    run
}
