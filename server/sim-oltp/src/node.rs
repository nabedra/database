//! Launch and stop the real OLTP processes under virtual node contexts.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

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
use uuid::Uuid;

use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, Bootstrap, CLIENTS, CONFIG, GATEWAYS, WORKERS, initial_range},
    client::{operation, request},
    disk::{self, DiskCut},
    history::Completion,
    invariants::{Invariants, RECOVERY_LIMIT, TraceObserver, elapsed_ms},
    net::SimNet,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(2);
const READINESS_POLL: Duration = Duration::from_millis(100);
/// Bound for a retained restart's preparation through the surviving workers.
const PREPARE_LIMIT: Duration = Duration::from_secs(60);

pub(crate) struct Cluster {
    pub world: Arc<World>,
    pub checks: Invariants,
    pub config: Bootstrap,
    net: SimNet,
    processes: BTreeMap<u64, Process>,
    trace: TraceObserver,
    frozen: BTreeSet<u64>,
    /// The last `direct.mutations` counter seen per worker, for leader tracking.
    mutations: BTreeMap<u64, u64>,
    leader: Option<u64>,
    /// Workers whose retained restart is prepared and has not become ready yet.
    prepared: BTreeSet<u64>,
    /// Per worker, the intervals in which it was frozen or had no process.
    outages: BTreeMap<u64, Vec<(u64, Option<u64>)>>,
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
            frozen: BTreeSet::new(),
            mutations: BTreeMap::new(),
            leader: None,
            prepared: BTreeSet::new(),
            outages: BTreeMap::new(),
        })
    }

    pub fn record(&self, line: String) {
        (self.trace)(elapsed_ms(&self.world), line);
    }

    pub fn net(&self) -> &SimNet {
        &self.net
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
        for index in 0..WORKERS.len() {
            self.start_worker(index).await?;
        }
        Ok(())
    }

    async fn start_worker(&mut self, index: usize) -> Result<(), String> {
        let node = WORKERS[index];
        self.outage_ended(node);
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
        let process = Process {
            task,
            stop: Stop::Worker(stop),
        };
        assert!(self.processes.insert(node, process).is_none());
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
        self.wait_ready_within(nodes, workers, WORKERS.len(), RECOVERY_LIMIT)
            .await
    }

    /// Wait until each of `nodes` reports the current run with `ready_workers` ready.
    pub async fn wait_ready_within(
        &mut self,
        nodes: &[u64],
        workers: bool,
        ready_workers: usize,
        limit: Duration,
    ) -> Result<(), String> {
        let elapsed = self.world.elapsed();
        let deadline = elapsed + limit;
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
                            && reply.body["ready_workers"] == json!(ready_workers)
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

    /// Whether `node` has a process that has not exited.
    pub fn is_running(&self, node: u64) -> bool {
        self.processes
            .get(&node)
            .is_some_and(|process| !process.task.is_finished())
    }

    /// Whether every worker runs and none is frozen.
    pub fn healthy(&self) -> bool {
        self.frozen.is_empty() && WORKERS.into_iter().all(|node| self.is_running(node))
    }

    fn outage(&mut self, node: u64) {
        let now = elapsed_ms(&self.world);
        let outages = self.outages.entry(node).or_default();
        if outages.last().is_none_or(|(_, to)| to.is_some()) {
            outages.push((now, None));
        }
    }

    fn outage_ended(&mut self, node: u64) {
        let now = elapsed_ms(&self.world);
        if let Some((_, to)) = self.outages.get_mut(&node).and_then(|all| all.last_mut()) {
            to.get_or_insert(now);
        }
    }

    /// Intervals in which at least two workers were frozen or down.
    pub fn no_quorum(&self) -> Vec<(u64, u64)> {
        let mut edges: Vec<(u64, i32)> = Vec::new();
        for node in WORKERS {
            for (from, to) in self.outages.get(&node).into_iter().flatten() {
                edges.push((*from, 1));
                edges.push((to.unwrap_or(u64::MAX), -1));
            }
        }
        overlaps(edges)
    }

    pub fn frozen(&self) -> &BTreeSet<u64> {
        &self.frozen
    }

    /// Workers whose process ended on its own, with the result it returned.
    pub async fn reap_exited(&mut self) -> Vec<(u64, String)> {
        let mut exited = Vec::new();
        for node in WORKERS.into_iter().chain(GATEWAYS) {
            if self
                .processes
                .get(&node)
                .is_some_and(|process| process.task.is_finished())
            {
                let process = self.processes.remove(&node).unwrap();
                exited.push((node, format!("{:?}", process.task.await)));
            }
        }
        exited
    }

    pub async fn admin(&self, name: &str, payload: Value) -> Result<Value, String> {
        let id = in_node(BOOTSTRAP_CLIENT, async { uuid_v4() }).await;
        self.admin_with_id(&WORKERS, id, name, payload).await
    }

    /// One administration request under a caller-chosen ID, tried on each of `nodes`.
    pub async fn admin_with_id(
        &self,
        nodes: &[u64],
        id: Uuid,
        name: &str,
        payload: Value,
    ) -> Result<Value, String> {
        let body = operation(id, name, payload);
        let mut errors = Vec::new();
        for &node in nodes {
            if !self.processes.contains_key(&node) || self.frozen.contains(&node) {
                continue;
            }
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

    /// One administration request to `node` in its own task, for operations that run for
    /// longer than a health check may wait.
    pub fn admin_task(
        &self,
        node: u64,
        id: Uuid,
        name: &'static str,
        payload: Value,
        limit: Duration,
    ) -> JoinHandle<Result<Value, String>> {
        let token = self.config.admin_token.clone();
        nabedra_rt::task::spawn(async move {
            match request(
                BOOTSTRAP_CLIENT,
                node,
                "/v1/cluster/operations",
                &token,
                Some(operation(id, name, payload)),
                limit,
            )
            .await
            .and_then(|reply| reply.completion(id))
            {
                Ok(Completion::Ok(data)) => Ok(data),
                Ok(Completion::Refused(err)) => {
                    Err(format!("n{node} {:?}: {}", err.code, err.message))
                }
                Ok(Completion::Unknown(err)) | Err(err) => Err(format!("n{node} {err}")),
            }
        })
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

    /// The worker that most recently executed direct mutations, as the acceptance
    /// fixture's `leader_of` finds it: the one whose `direct.mutations` counter grew.
    /// Frozen and stopped workers are never returned.
    pub async fn leader(&mut self) -> Option<u64> {
        let mut best = None::<(u64, u64)>;
        for node in WORKERS {
            if !self.is_running(node) || self.frozen.contains(&node) {
                continue;
            }
            let Ok(stats) = self.stats(node).await else {
                continue;
            };
            let count = stats["direct"]["mutations"].as_u64().unwrap_or(0);
            let previous = self.mutations.insert(node, count).unwrap_or(0);
            // A restarted process counts from zero again.
            let delta = if count < previous {
                count
            } else {
                count - previous
            };
            if delta > 0 && best.is_none_or(|(_, most)| delta > most) {
                best = Some((node, delta));
            }
        }
        if let Some((node, _)) = best {
            self.leader = Some(node);
        }
        self.leader
            .filter(|node| self.is_running(*node) && !self.frozen.contains(node))
    }

    pub async fn crash(&mut self, node: u64, how: DiskCut) {
        self.record(format!("cut n{node} {how:?}"));
        self.unpause(node);
        if WORKERS.contains(&node) {
            self.outage(node);
        }
        // A process that ended before anything stopped it is a failure, whatever it returned.
        let alone = self.processes.get(&node).is_some_and(|process| {
            process.task.is_finished() && !matches!(process.stop, Stop::Signalled)
        });
        let image = disk::cut(&self.world, node, how);
        if let Some(process) = self.processes.remove(&node) {
            match process.task.await {
                Err(error) if error.is_cancelled() => {}
                Ok(Ok(())) if !alone => {}
                result => {
                    self.checks
                        .violate(&self.world, "ProcessExit", format!("n{node}: {result:?}"))
                }
            }
        }
        self.checks.check_crash(&self.world, node, &image);
    }

    /// Restart a stopped worker in the same run, through the retained restart that
    /// `tests/oltp_recovery_operations.py` uses: wait until the survivors drop the lost
    /// incarnation from the ready set, prepare a fresh incarnation through them, then
    /// reopen the retained volume and start the worker.
    pub async fn restart_worker(&mut self, node: u64, how: DiskCut) -> Result<(), String> {
        let index = WORKERS
            .iter()
            .position(|worker| *worker == node)
            .expect("known worker");
        if self.world.is_up(node) {
            self.crash(node, DiskCut::Kill).await;
        }
        if self.prepared.contains(&node) {
            // Control holds one preparation at a time, so a process that did not come up
            // starts again from the bootstrap file it already has, as a supervisor does. Once
            // that process has enrolled, the preparation is spent and a fresh one is needed.
            let connection = self.admin("cluster.connection", json!({})).await?;
            if !self.enrolled(index, &connection) {
                return self.start_prepared(index, how).await;
            }
            self.record(format!("n{node} enrolled before it stopped: prepare again"));
            self.prepared.remove(&node);
        }
        let survivors: Vec<u64> = WORKERS
            .into_iter()
            .filter(|other| *other != node && self.is_running(*other))
            .collect();
        self.wait_ready_within(&survivors, true, survivors.len(), PREPARE_LIMIT)
            .await
            .map_err(|error| format!("survivors never dropped n{node}: {error}"))?;
        let connection = self.admin("cluster.connection", json!({})).await?;
        let (id, plan, next) =
            in_node(BOOTSTRAP_CLIENT, async { self.config.rolling_plan(index) }).await;
        let deadline = self.world.elapsed() + PREPARE_LIMIT;
        let prepared = loop {
            match self
                .admin_with_id(&survivors, id, "worker.restart.prepare", plan.clone())
                .await
            {
                Ok(prepared) => break prepared,
                Err(error) if self.world.elapsed() >= deadline => {
                    return Err(format!("prepare retained restart of n{node}: {error}"));
                }
                Err(_) => time::sleep(Duration::from_millis(250)).await,
            }
        };
        self.config.apply_rolling(
            index,
            id,
            next,
            prepared["identity"].clone(),
            &connection["control_peers"],
        );
        self.prepared.insert(node);
        self.start_prepared(index, how).await
    }

    /// Whether Control lists the enrollment that worker `index` was prepared with.
    fn enrolled(&self, index: usize, connection: &Value) -> bool {
        connection["control_peers"]
            .as_array()
            .is_some_and(|peers| peers.contains(self.config.worker_enrollment(index)))
    }

    async fn start_prepared(&mut self, index: usize, how: DiskCut) -> Result<(), String> {
        let node = WORKERS[index];
        disk::reopen(&self.world, &self.checks, node, how);
        self.start_worker(index).await?;
        let ready = self.wait_ready(&[node], true).await;
        if ready.is_err() {
            self.abort_start(node).await;
        } else {
            self.prepared.remove(&node);
        }
        ready
    }

    /// Stop a process whose start did not complete. It may already have exited with
    /// its startup error, which the caller reports.
    async fn abort_start(&mut self, node: u64) {
        self.record(format!("abort start n{node}"));
        self.unpause(node);
        if WORKERS.contains(&node) {
            self.outage(node);
        }
        let image = disk::cut(&self.world, node, DiskCut::Kill);
        if let Some(process) = self.processes.remove(&node)
            && let Err(error) = process.task.await
            && error.is_panic()
        {
            self.checks.violate(
                &self.world,
                "ProcessExit",
                format!("n{node} panicked during startup: {error}"),
            );
        }
        self.checks.check_crash(&self.world, node, &image);
    }

    /// Kill a gateway's process. Its clients see refused or reset connections.
    pub async fn kill_gateway(&mut self, node: u64) {
        self.crash(node, DiskCut::Kill).await;
    }

    /// Start a stopped gateway again with its original enrollment.
    pub async fn restart_gateway(&mut self, node: u64) -> Result<(), String> {
        let index = GATEWAYS
            .iter()
            .position(|gateway| *gateway == node)
            .expect("known gateway");
        if self.world.is_up(node) {
            self.crash(node, DiskCut::Kill).await;
        }
        let connection = self.admin("cluster.connection", json!({})).await?;
        disk::reopen(&self.world, &self.checks, node, DiskCut::Kill);
        if let Err(error) = self.start_gateway(index, &connection).await {
            self.abort_start(node).await;
            return Err(error);
        }
        let ready = WORKERS
            .into_iter()
            .filter(|worker| self.is_running(*worker))
            .count();
        self.wait_ready_within(&[node], false, ready, RECOVERY_LIMIT)
            .await
    }

    pub async fn teardown(&mut self) {
        for node in self.frozen.clone() {
            self.thaw(node);
        }
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
    pub async fn cold_start(&mut self, how: DiskCut) -> Result<(), String> {
        self.cold_start_staggered("cold start", how, [Duration::ZERO; 3], RECOVERY_LIMIT)
            .await
    }

    /// Stop every worker, then start worker `i` `delays[i]` after the stop. Every
    /// worker and gateway must report the new run within `limit` of the last start.
    pub async fn cold_start_staggered(
        &mut self,
        label: &str,
        how: DiskCut,
        delays: [Duration; 3],
        limit: Duration,
    ) -> Result<(), String> {
        assert!(
            !matches!(how, DiskCut::Wipe),
            "cold start requires retained volumes"
        );
        for node in self.frozen.clone() {
            self.thaw(node);
        }
        for node in WORKERS {
            if self.world.is_up(node) {
                self.crash(node, how).await;
            }
            self.processes.remove(&node);
        }
        self.prepared.clear();
        in_node(BOOTSTRAP_CLIENT, async { self.config.prepare_cold_start() }).await;
        for node in WORKERS {
            disk::reopen(&self.world, &self.checks, node, how);
        }
        let stopped = self.world.elapsed();
        let mut order: Vec<usize> = (0..WORKERS.len()).collect();
        order.sort_by_key(|&index| (delays[index], index));
        for index in order {
            let at = stopped + delays[index];
            if let Some(wait) = at.checked_sub(self.world.elapsed()) {
                time::sleep(wait).await;
            }
            self.start_worker(index).await?;
        }
        self.checks.expect_recovery(&self.world, label, limit);
        self.wait_ready_within(&WORKERS, true, WORKERS.len(), limit)
            .await?;
        self.wait_ready_within(&GATEWAYS, false, WORKERS.len(), limit)
            .await?;
        self.checks.recovered(&self.world, label);
        Ok(())
    }

    /// One attempt to split the table's first range at `boundary` under `id`. Returns the
    /// publication, or why the split is still pending.
    pub async fn split(&self, id: Uuid, table: Uuid, boundary: i64) -> Result<Value, String> {
        let source = initial_range(self.config.cluster_id, table);
        let payload = json!({"source": source, "boundary": boundary, "page_bytes": 1 << 20});
        match self
            .admin_with_id(&WORKERS, id, "storage.shard.split", payload)
            .await
        {
            Ok(result) if result["state"] == "published" => Ok(result),
            Ok(result) => Err(result.to_string()),
            Err(error) => Err(error),
        }
    }

    /// Whether every worker and gateway reports the run ready and the direct path active.
    pub async fn converged(&self) -> bool {
        if !self.healthy() {
            return false;
        }
        for node in WORKERS {
            let stats = self.stats(node).await;
            if !stats.is_ok_and(|stats| stats["direct_capability"]["active"] == true) {
                return false;
            }
        }
        for node in WORKERS.into_iter().chain(GATEWAYS) {
            let ready = request(
                BOOTSTRAP_CLIENT,
                node,
                "/health/ready",
                &self.config.data_token,
                None,
                HTTP_TIMEOUT,
            )
            .await;
            if !ready.is_ok_and(|reply| {
                reply.status == 200 && reply.body["ready_workers"] == json!(WORKERS.len())
            }) {
                return false;
            }
        }
        true
    }

    /// A fresh operation ID from the bootstrap client's stream.
    pub async fn split_id(&self) -> Uuid {
        in_node(BOOTSTRAP_CLIENT, async { uuid_v4() }).await
    }

    /// Stop a process gracefully through its own stop path, with work in flight.
    pub async fn drain(&mut self, node: u64) -> Result<(), String> {
        self.record(format!("drain n{node}"));
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

    pub fn freeze(&mut self, node: u64) {
        self.record(format!("freeze n{node}"));
        self.frozen.insert(node);
        self.outage(node);
        self.net.update_plan(|plan| plan.pause(node));
        self.world.freeze(node);
    }

    /// A killed process is no longer frozen: its next process must reach the network.
    fn unpause(&mut self, node: u64) {
        if self.frozen.remove(&node) {
            self.net.update_plan(|plan| plan.resume(node));
        }
    }

    pub fn thaw(&mut self, node: u64) {
        if !self.frozen.remove(&node) {
            return;
        }
        self.record(format!("thaw n{node}"));
        self.outage_ended(node);
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

/// Intervals in which at least two of the outages that `edges` open and close overlap.
fn overlaps(mut edges: Vec<(u64, i32)>) -> Vec<(u64, u64)> {
    edges.sort_unstable();
    let (mut down, mut since, mut windows) = (0, None, Vec::new());
    for (at, change) in edges {
        down += change;
        match (down >= 2, since) {
            (true, None) => since = Some(at),
            (false, Some(from)) => {
                if at > from {
                    windows.push((from, at));
                }
                since = None;
            }
            _ => {}
        }
    }
    windows
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Cluster, overlaps};
    use crate::{config::Config, disk::DiskCut, invariants::Invariants};

    /// Direct seed 8: a worker killed while frozen kept its links held, so its next process
    /// had no network and never became ready.
    #[test]
    fn a_killed_frozen_worker_gets_its_network_back() {
        nabedra_sim::runtime::run(1, |world| async move {
            Config::sample(1).setup(&world);
            let checks = Invariants::new();
            let mut cluster = Cluster::new(world, checks, 1, Arc::new(|_, _| {}), |_, _, _| {})
                .await
                .unwrap();
            cluster.freeze(2);
            assert!(cluster.net().update_plan(|plan| plan.is_paused(2)));
            cluster.crash(2, DiskCut::Kill).await;
            assert!(!cluster.net().update_plan(|plan| plan.is_paused(2)));
        });
    }

    #[test]
    fn no_quorum_needs_two_overlapping_outages() {
        assert_eq!(overlaps(vec![(1, 1), (5, -1), (5, 1), (9, -1)]), []);
        assert_eq!(overlaps(vec![(1, 1), (9, -1), (3, 1), (6, -1)]), [(3, 6)]);
        assert_eq!(
            overlaps(vec![(1, 1), (u64::MAX, -1), (3, 1), (u64::MAX, -1)]),
            [(3, u64::MAX)]
        );
    }
}
