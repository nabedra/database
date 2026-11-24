//! Launch and stop the real OLTP processes under virtual node contexts.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use nabedra_database_server::oltp_runtime::{api_process, registered_process, sim as process_sim};
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node},
    time,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, Bootstrap, CLIENTS, CONFIG, GATEWAYS, WORKERS},
    client::{operation, request},
    disk::{self, DiskCut},
    history::Completion,
    invariants::{Invariants, RECOVERY_LIMIT, TraceObserver, elapsed_ms},
    net::SimNet,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(2);
const READINESS_POLL: Duration = Duration::from_millis(100);

pub(crate) struct Cluster {
    pub world: Arc<World>,
    pub checks: Invariants,
    pub config: Bootstrap,
    net: SimNet,
    processes: BTreeMap<u64, Process>,
    trace: TraceObserver,
}

struct Process {
    task: JoinHandle<Result<(), &'static str>>,
    stop: Stop,
}

enum Stop {
    Worker(oneshot::Sender<()>),
    Gateway(DuplexStream),
    Signalled,
}

impl Cluster {
    pub async fn new(
        world: Arc<World>,
        checks: Invariants,
        seed: u64,
        trace: TraceObserver,
        net_trace: impl Fn(u64, Option<nabedra_rt::sim::net::ConnId>, &str) + Send + Sync + 'static,
    ) -> Result<Self, String> {
        let net = SimNet::install(
            &world,
            seed,
            WORKERS
                .into_iter()
                .chain(GATEWAYS)
                .chain([BOOTSTRAP_CLIENT])
                .chain(CLIENTS),
        );
        net.set_trace_observer(net_trace);
        let config = in_node(BOOTSTRAP_CLIENT, async { Bootstrap::new() })
            .await
            .map_err(|err| format!("generate cluster bootstrap: {err}"))?;
        for node in WORKERS.into_iter().chain(GATEWAYS) {
            disk::mount(&world, &checks, node);
            world.set_certificate(node, config.certificate_digest(node));
        }
        Ok(Self {
            world,
            checks,
            config,
            net,
            processes: BTreeMap::new(),
            trace,
        })
    }

    fn record(&self, line: String) {
        (self.trace)(elapsed_ms(&self.world), line);
    }

    pub async fn start(&mut self) -> Result<(), String> {
        self.checks
            .expect_recovery(&self.world, "initial bootstrap", RECOVERY_LIMIT);
        self.start_workers().await?;
        self.wait_ready(&WORKERS, true).await?;
        let connection = self.admin("cluster.connection", json!({})).await?;
        self.validate_connection(&connection)?;
        for (index, node) in GATEWAYS.into_iter().enumerate() {
            self.admin(
                "api.enroll",
                json!({
                    "expected": null, "next": self.config.api_enrollment(index),
                }),
            )
            .await?;
            self.start_gateway(index, &connection).await?;
            self.record(format!("gateway n{node} activated"));
        }
        self.wait_ready(&GATEWAYS, false).await?;
        self.checks.recovered(&self.world, "initial bootstrap");
        Ok(())
    }

    async fn start_workers(&mut self) -> Result<(), String> {
        for (index, node) in WORKERS.into_iter().enumerate() {
            in_node(node, async { self.config.write_worker(index) })
                .await
                .map_err(|error| format!("write worker n{node} bootstrap: {error}"))?;
            let (stop, stopped) = oneshot::channel();
            self.record(format!("start worker n{node}"));
            let task = self.world.spawn_node(node, async move {
                in_node(
                    node,
                    process_sim::with_worker_stop(
                        stopped,
                        registered_process::run(PathBuf::from(CONFIG)),
                    ),
                )
                .await
            });
            assert!(
                self.processes
                    .insert(
                        node,
                        Process {
                            task,
                            stop: Stop::Worker(stop)
                        }
                    )
                    .is_none()
            );
        }
        Ok(())
    }

    async fn start_gateway(&mut self, index: usize, connection: &Value) -> Result<(), String> {
        let node = GATEWAYS[index];
        in_node(node, async { self.config.write_gateway(index, connection) })
            .await
            .map_err(|error| format!("write gateway n{node} bootstrap: {error}"))?;
        let (channel, server) = tokio::io::duplex(32 << 10);
        let task = self.world.spawn_node(node, async move {
            in_node(
                node,
                process_sim::with_supervisor(
                    server,
                    api_process::run_api_configured(PathBuf::from(CONFIG), false, false),
                ),
            )
            .await
        });
        assert!(
            self.processes
                .insert(
                    node,
                    Process {
                        task,
                        stop: Stop::Gateway(channel)
                    }
                )
                .is_none()
        );
        let Process {
            stop: Stop::Gateway(channel),
            ..
        } = self.processes.get_mut(&node).unwrap()
        else {
            unreachable!("gateway process channel")
        };
        time::timeout(RECOVERY_LIMIT, async {
            supervisor_send(
                channel,
                &json!({
                    "version": 1, "type": "init", "name": "sim_oltp", "provisioning": "external",
                    "initial_policy": {"mode":"fixed", "desired":1, "min_nodes":1, "max_nodes":1},
                    "bridge": null,
                }),
            )
            .await?;
            supervisor_receive(channel, "prepared").await?;
            supervisor_send(channel, &json!({"version":1, "type":"activate"})).await?;
            supervisor_receive(channel, "ready").await
        })
        .await
        .map_err(|_| format!("gateway n{node} supervisor deadline"))?
        .map_err(|error| format!("gateway n{node}: {error}"))
    }

    pub async fn wait_ready(&mut self, nodes: &[u64], workers: bool) -> Result<(), String> {
        let elapsed = self.world.elapsed();
        let deadline = elapsed + RECOVERY_LIMIT;
        let mut pending = nodes.to_vec();
        loop {
            let mut next = Vec::new();
            let mut last = Vec::new();
            for node in pending {
                self.check_running(node).await?;
                let ready = request(
                    BOOTSTRAP_CLIENT,
                    node,
                    "/health/ready",
                    &self.config.data_token,
                    None,
                    HTTP_TIMEOUT,
                )
                .await;
                let ready = match ready {
                    Ok(reply) => {
                        let valid = reply.status == 200
                            && reply.body["cluster_id"] == json!(self.config.cluster_id)
                            && reply.body["run_id"] == json!(self.config.run_id)
                            && reply.body["ready_workers"] == json!(WORKERS.len())
                            && (!workers
                                || reply.body["replication_factor"] == json!(WORKERS.len()));
                        last.push((node, format!("{} {}", reply.status, reply.body)));
                        valid
                    }
                    Err(error) => {
                        last.push((node, error));
                        false
                    }
                };
                if !ready {
                    next.push(node);
                }
            }
            if next.is_empty() {
                return Ok(());
            }
            if self.world.elapsed() >= deadline {
                return Err(format!("readiness deadline for {next:?}: {last:?}"));
            }
            pending = next;
            time::sleep(READINESS_POLL).await;
        }
    }

    async fn check_running(&mut self, node: u64) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(&node)
            .ok_or_else(|| format!("n{node} has no process"))?;
        if process.task.is_finished() {
            let mut exited = self.processes.remove(&node).unwrap();
            let result = (&mut exited.task).await;
            return Err(format!("n{node} exited during startup: {result:?}"));
        }
        Ok(())
    }

    pub async fn admin(&self, name: &str, payload: Value) -> Result<Value, String> {
        let id = in_node(BOOTSTRAP_CLIENT, async { uuid_v4() }).await;
        let body = operation(id, name, payload);
        let mut errors = Vec::new();
        for node in WORKERS {
            match request(
                BOOTSTRAP_CLIENT,
                node,
                "/v1/cluster/operations",
                &self.config.admin_token,
                Some(body.clone()),
                HTTP_TIMEOUT,
            )
            .await
            .and_then(|reply| reply.completion(id))
            {
                Ok(Completion::Ok(data)) => return Ok(data),
                Ok(Completion::Refused(err)) => {
                    errors.push(format!("n{node} {:?}: {}", err.code, err.message))
                }
                Ok(Completion::Unknown(err)) | Err(err) => errors.push(format!("n{node} {err}")),
            }
        }
        Err(format!("{name}: {}", errors.join("; ")))
    }

    fn validate_connection(&self, data: &Value) -> Result<(), String> {
        if data["cluster_id"] != json!(self.config.cluster_id)
            || data["control_peers"].as_array().map(Vec::len) != Some(WORKERS.len())
        {
            return Err(format!("invalid cluster.connection inventory: {data}"));
        }
        Ok(())
    }

    pub async fn stats(&self, node: u64) -> Result<Value, String> {
        let reply = request(
            BOOTSTRAP_CLIENT,
            node,
            "/v1/runtime/stats",
            &self.config.data_token,
            None,
            HTTP_TIMEOUT,
        )
        .await?;
        if reply.status != 200 {
            return Err(format!("n{node} stats: {} {}", reply.status, reply.body));
        }
        Ok(reply.body)
    }

    pub async fn wait_direct(&self) -> Result<(), String> {
        let deadline = self.world.elapsed() + RECOVERY_LIMIT;
        loop {
            let mut active = true;
            for node in WORKERS {
                let result = self.stats(node).await;
                active &= result.is_ok_and(|stats| {
                    stats["direct_capability"]["active"] == true && stats["direct"].is_object()
                });
            }
            if active {
                return Ok(());
            }
            if self.world.elapsed() >= deadline {
                return Err("direct path activation deadline".into());
            }
            time::sleep(READINESS_POLL).await;
        }
    }

    pub async fn crash(&mut self, node: u64, how: DiskCut) {
        self.record(format!("cut n{node} {how:?}"));
        let image = disk::cut(&self.world, node, how);
        if let Some(process) = self.processes.remove(&node) {
            match process.task.await {
                Err(error) if error.is_cancelled() => {}
                Ok(Ok(())) => {}
                result => {
                    self.checks
                        .violate(&self.world, "ProcessExit", format!("n{node}: {result:?}"))
                }
            }
        }
        self.checks.check_crash(&self.world, node, &image);
    }

    pub async fn teardown(&mut self) {
        for node in GATEWAYS.into_iter().chain(WORKERS) {
            if self.world.is_up(node) {
                self.crash(node, DiskCut::Kill).await;
            }
        }
        for node in [BOOTSTRAP_CLIENT].into_iter().chain(CLIENTS) {
            self.world.crash(node);
        }
        // Let cancelled tasks drop before we return from the seed.
        time::sleep(Duration::from_millis(1)).await;
    }

    /// A complete stop abandons the old RAM queue; the next run opens the durable disks.
    /// The cold workload will drive recovery under pending requests.
    pub async fn cold_start(&mut self, how: DiskCut) -> Result<(), String> {
        assert!(
            !matches!(how, DiskCut::Wipe),
            "cold start requires retained volumes"
        );
        for node in WORKERS {
            self.crash(node, how).await;
        }
        self.checks
            .expect_recovery(&self.world, "cold start", RECOVERY_LIMIT);
        in_node(BOOTSTRAP_CLIENT, async { self.config.prepare_cold_start() }).await;
        for node in WORKERS {
            disk::reopen(&self.world, &self.checks, node, how);
        }
        self.start_workers().await?;
        self.wait_ready(&WORKERS, true).await?;
        self.wait_ready(&GATEWAYS, false).await?;
        self.checks.recovered(&self.world, "cold start");
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn drain(&mut self, node: u64) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(&node)
            .ok_or_else(|| format!("n{node} is not running"))?;
        let stop = std::mem::replace(&mut process.stop, Stop::Signalled);
        match stop {
            Stop::Worker(stop) => stop
                .send(())
                .map_err(|()| format!("n{node} stopped early"))?,
            Stop::Gateway(mut channel) => {
                supervisor_send(&mut channel, &json!({"version":1, "type":"stop"})).await?;
            }
            Stop::Signalled => {}
        }
        match time::timeout(RECOVERY_LIMIT, &mut process.task).await {
            Ok(result) => {
                self.processes.remove(&node);
                result
                    .map_err(|err| err.to_string())?
                    .map_err(str::to_owned)
            }
            Err(_) => Err(format!("n{node} drain deadline")),
        }
    }

    #[allow(dead_code)]
    pub fn freeze(&self, node: u64) {
        self.record(format!("freeze n{node}"));
        self.net.update_plan(|plan| plan.pause(node));
        self.world.freeze(node);
    }

    #[allow(dead_code)]
    pub fn thaw(&self, node: u64) {
        self.record(format!("thaw n{node}"));
        self.world.thaw(node);
        self.net.update_plan(|plan| plan.resume(node));
    }
}

async fn supervisor_send(channel: &mut DuplexStream, json: &Value) -> Result<(), String> {
    let body = serde_json::to_vec(json).map_err(|err| err.to_string())?;
    channel
        .write_u32(
            body.len()
                .try_into()
                .map_err(|_| "supervisor body too large")?,
        )
        .await
        .map_err(|err| err.to_string())?;
    channel
        .write_all(&body)
        .await
        .map_err(|err| err.to_string())
}

async fn supervisor_receive(channel: &mut DuplexStream, phase: &str) -> Result<(), String> {
    let length = channel.read_u32().await.map_err(|err| err.to_string())? as usize;
    if !(1..=(16 << 10)).contains(&length) {
        return Err("invalid supervisor frame length".into());
    }
    let mut body = vec![0; length];
    channel
        .read_exact(&mut body)
        .await
        .map_err(|err| err.to_string())?;
    let message: Value = serde_json::from_slice(&body).map_err(|err| err.to_string())?;
    if message["version"] != 1 || message["type"] != phase {
        return Err(format!("supervisor expected {phase}, received {message}"));
    }
    Ok(())
}
