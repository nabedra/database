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
    bootstrap::{BOOTSTRAP_CLIENT, Bootstrap, CLIENTS, GATEWAYS, WORKERS, initial_range},
    client::{operation, request},
    disk::{self, DiskCut},
    history::Completion,
    invariants::{Invariants, RECOVERY_LIMIT, TraceObserver, elapsed_ms},
    net::SimNet,
    operator::Operator,
};

mod members;
pub(crate) use members::{Disk, Marker, Started};

const HTTP_TIMEOUT: Duration = Duration::from_secs(2);
/// One request of an operation that may run for longer than a health check.
const REQUEST: Duration = Duration::from_secs(10);
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
    /// The Data group that serves the table, whose elections the network watches.
    range: Option<Uuid>,
    /// Workers whose retained restart is prepared and has not become ready yet.
    prepared: BTreeSet<u64>,
    /// Per worker, the intervals in which it was frozen or had no process.
    outages: BTreeMap<u64, Vec<(u64, Option<u64>)>>,
    /// Spare workers that the workload gave a process.
    spares: BTreeSet<u64>,
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
            range: None,
            prepared: BTreeSet::new(),
            outages: BTreeMap::new(),
            spares: BTreeSet::new(),
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
        let node = self.config.node(index);
        in_node(node, async { self.config.write_worker(index) })
            .await
            .map_err(|error| format!("write worker n{node} bootstrap: {error}"))?;
        self.spawn_worker(node);
        Ok(())
    }

    /// Start `node`'s worker process from the bootstrap file that is already in place.
    fn spawn_worker(&mut self, node: u64) {
        self.outage_ended(node);
        let path = PathBuf::from(self.config.config_path(node));
        let (stop, stopped) = oneshot::channel();
        self.record(format!("start worker n{node}"));
        let task = self.world.spawn_node(node, async move {
            in_node(
                node,
                process_sim::with_worker_stop(stopped, registered_process::run(path)),
            )
            .await
        });
        let process = Process {
            task,
            stop: Stop::Worker(stop),
        };
        assert!(self.processes.insert(node, process).is_none());
    }

    async fn start_gateway(&mut self, index: usize, connection: &Value) -> Result<(), String> {
        let node = GATEWAYS[index];
        in_node(node, async { self.config.write_gateway(index, connection) })
            .await
            .map_err(|error| format!("write gateway n{node} bootstrap: {error}"))?;
        let path = PathBuf::from(self.config.config_path(node));
        let (channel, server) = tokio::io::duplex(32 << 10);
        let task = self.world.spawn_node(node, async move {
            in_node(
                node,
                process_sim::with_supervisor(
                    server,
                    api_process::run_api_configured(path, false, false),
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
        self.frozen.is_empty()
            && self
                .config
                .nodes()
                .into_iter()
                .all(|node| self.is_running(node))
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
        for node in self.config.nodes() {
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
        for node in self.config.nodes().into_iter().chain(GATEWAYS) {
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
        self.admin_with_id(&self.config.nodes(), id, name, payload)
            .await
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
            for node in self.config.nodes() {
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

    /// Follow the elections of a Data group, and take its leader for the table's from now on.
    pub fn watch_range(&mut self, group: Uuid) {
        self.net.watch_group(group);
        self.range = Some(group);
    }

    /// Follow the table's initial range.
    pub fn watch_table(&mut self, table: Uuid) {
        let group = initial_range(self.config.cluster_id, table)["group_id"]
            .as_str()
            .and_then(|id| Uuid::parse_str(id).ok());
        self.watch_range(group.expect("derived group ID"));
    }

    /// Every election of the table's ranges: when, and which worker.
    pub fn elections(&self) -> Vec<(u64, u64)> {
        let elections = self.net.elections();
        elections.iter().map(|seen| (seen.at, seen.node)).collect()
    }

    /// The worker that a quorum last elected to lead the table's range, whatever state it is in
    /// now. Before a new range's first append nothing is known.
    pub fn elected(&self) -> Option<u64> {
        let elections = self.net.elections();
        let mut range = elections.iter().rev();
        range
            .find(|seen| Some(seen.group) == self.range)
            .map(|seen| seen.node)
    }

    /// The elected leader, if it runs and is not frozen.
    pub fn leader(&self) -> Option<u64> {
        self.elected()
            .filter(|node| self.is_running(*node) && !self.frozen.contains(node))
    }

    /// The operator of this cluster.
    pub fn operator(&self) -> Operator {
        Operator {
            world: self.world.clone(),
            checks: self.checks.clone(),
            trace: self.trace.clone(),
            token: self.config.admin_token.clone(),
            data_token: self.config.data_token.clone(),
            cluster_id: self.config.cluster_id,
            mounts: self.config.mounts.clone(),
        }
    }

    pub async fn crash(&mut self, node: u64, how: DiskCut) {
        self.record(format!("cut n{node} {how:?}"));
        self.unpause(node);
        if self.config.nodes().contains(&node) {
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
        self.prepare_restart(node).await?;
        let index = self.config.index_of(node);
        self.start_prepared(index.expect("known worker"), how).await
    }

    /// Prepare a retained restart through the survivors, unless one is still unspent.
    pub async fn prepare_restart(&mut self, node: u64) -> Result<(), String> {
        let index = self.config.index_of(node).expect("known worker");
        if self.world.is_up(node) {
            self.crash(node, DiskCut::Kill).await;
        }
        if self.prepared.contains(&node) {
            // Control holds one preparation at a time, so a process that did not come up
            // starts again from the bootstrap file it already has, as a supervisor does. Once
            // that process has enrolled, the preparation is spent and a fresh one is needed.
            let connection = self.connection().await?;
            if !self.enrolled(index, &connection) {
                return Ok(());
            }
            self.record(format!("n{node} enrolled before it stopped: prepare again"));
            self.prepared.remove(&node);
        }
        let survivors: Vec<u64> = self
            .config
            .nodes()
            .into_iter()
            .filter(|other| *other != node && self.is_running(*other))
            .collect();
        self.wait_ready_within(&survivors, true, survivors.len(), PREPARE_LIMIT)
            .await
            .map_err(|error| format!("survivors never dropped n{node}: {error}"))?;
        let connection = self.connection().await?;
        let (id, plan, next) =
            in_node(BOOTSTRAP_CLIENT, async { self.config.rolling_plan(index) }).await;
        let deadline = self.world.elapsed() + PREPARE_LIMIT;
        let prepared = loop {
            let name = "worker.restart.prepare";
            match self.operator().any(id, name, &plan, REQUEST).await {
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
        Ok(())
    }

    /// The cluster's connection inventory. A restart needs it, and an operator does not give up
    /// on one request that reached its deadline.
    async fn connection(&self) -> Result<Value, String> {
        let operator = self.operator();
        let deadline = self.world.elapsed() + PREPARE_LIMIT;
        loop {
            let id = operator.id().await;
            let name = "cluster.connection";
            match operator.any(id, name, &json!({}), REQUEST).await {
                Err(error) if self.world.elapsed() >= deadline => return Err(error),
                Err(_) => time::sleep(Duration::from_millis(250)).await,
                connection => return connection,
            }
        }
    }

    /// Whether Control lists the enrollment that worker `index` was prepared with.
    fn enrolled(&self, index: usize, connection: &Value) -> bool {
        connection["control_peers"]
            .as_array()
            .is_some_and(|peers| peers.contains(self.config.worker_enrollment(index)))
    }

    /// Start a prepared worker on its reopened volume and wait until it is ready.
    pub async fn start_prepared(&mut self, index: usize, how: DiskCut) -> Result<(), String> {
        let node = self.config.node(index);
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
        if self.config.nodes().contains(&node) {
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
        let connection = self.connection().await?;
        disk::reopen(&self.world, &self.checks, node, DiskCut::Kill);
        if let Err(error) = self.start_gateway(index, &connection).await {
            self.abort_start(node).await;
            return Err(error);
        }
        let ready = self
            .config
            .nodes()
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
        let workers = WORKERS.into_iter().chain(self.spares.clone());
        for node in GATEWAYS.into_iter().chain(workers) {
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
        let nodes = self.config.nodes();
        for node in nodes {
            if self.world.is_up(node) {
                self.crash(node, how).await;
            }
            self.processes.remove(&node);
        }
        self.prepared.clear();
        in_node(BOOTSTRAP_CLIENT, async { self.config.prepare_cold_start() }).await;
        for node in nodes {
            disk::reopen(&self.world, &self.checks, node, how);
        }
        let stopped = self.world.elapsed();
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        order.sort_by_key(|&index| (delays[index], index));
        for index in order {
            let at = stopped + delays[index];
            if let Some(wait) = at.checked_sub(self.world.elapsed()) {
                time::sleep(wait).await;
            }
            self.start_worker(index).await?;
        }
        self.checks.expect_recovery(&self.world, label, limit);
        self.wait_ready_within(&nodes, true, nodes.len(), limit)
            .await?;
        self.wait_ready_within(&GATEWAYS, false, nodes.len(), limit)
            .await?;
        self.checks.recovered(&self.world, label);
        Ok(())
    }

    /// Whether every worker and gateway reports the run ready and the direct path active.
    pub async fn converged(&self) -> bool {
        if !self.healthy() {
            return false;
        }
        for node in self.config.nodes() {
            let stats = self.stats(node).await;
            if !stats.is_ok_and(|stats| stats["direct_capability"]["active"] == true) {
                return false;
            }
        }
        for node in self.config.nodes().into_iter().chain(GATEWAYS) {
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

    /// Tell a process to stop gracefully through its own stop path, with work in flight.
    pub async fn start_drain(&mut self, node: u64) -> Result<(), String> {
        self.record(format!("drain n{node}"));
        let process = self
            .processes
            .get_mut(&node)
            .ok_or_else(|| format!("n{node} is not running"))?;
        match std::mem::replace(&mut process.stop, Stop::Signalled) {
            Stop::Worker(stop) => stop.send(()).map_err(|()| format!("n{node} stopped early")),
            Stop::Gateway(mut channel) => {
                supervisor_send(&mut channel, &json!({"version":1, "type":"stop"})).await
            }
            Stop::Signalled => Ok(()),
        }
    }

    /// How a drained process ended, once it did.
    pub async fn drained(&mut self, node: u64) -> Option<Result<(), String>> {
        let process = self.processes.get_mut(&node)?;
        if !process.task.is_finished() {
            return None;
        }
        let result = (&mut process.task).await;
        self.processes.remove(&node);
        Some(
            result
                .map_err(|err| err.to_string())
                .and_then(|result| result.map_err(str::to_owned)),
        )
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

/// The world owns the planner, and the planner owns the fault profiles that a workload
/// installed. A profile may hold the world, so the cluster releases the planner it
/// installed, also when a seed panics before its teardown.
impl Drop for Cluster {
    fn drop(&mut self) {
        self.world.clear_net_planner();
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
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Mutex, Weak},
    };

    use nabedra_rt::sim::World;

    use super::{Cluster, overlaps};
    use crate::{
        config::Config, disk::DiskCut, invariants::Invariants, net::Action, workload::live_after,
    };

    /// A run of a cluster whose frame profile holds the world, as the nemesis profiles do.
    async fn run_with_a_profile_that_holds_the_world(world: Arc<World>, panics: bool) {
        Config::sample(1).setup(&world);
        let checks = Invariants::new();
        let mut cluster = Cluster::new(world.clone(), checks, 1, Arc::new(|_, _| {}), |_, _, _| {})
            .await
            .unwrap();
        cluster.net().set_frame_profile(move |_| {
            let _ = world.elapsed();
            Action::Pass
        });
        assert!(!panics, "seeded harness panic");
        cluster.teardown().await;
    }

    /// `cold` seed 4: a nemesis profile held the world, so no run's disk images were freed.
    #[test]
    fn a_profile_that_holds_the_world_does_not_outlive_its_run() {
        let world = nabedra_sim::runtime::run(1, |world| async move {
            run_with_a_profile_that_holds_the_world(world.clone(), false).await;
            world
        });
        assert_eq!(live_after(world), [""; 0]);
    }

    #[test]
    fn a_profile_that_holds_the_world_does_not_outlive_a_panic() {
        let seen = Mutex::new(Weak::new());
        let result = catch_unwind(AssertUnwindSafe(|| {
            nabedra_sim::runtime::run(1, |world| {
                *seen.lock().unwrap() = Arc::downgrade(&world);
                run_with_a_profile_that_holds_the_world(world, true)
            });
        }));
        assert!(result.is_err());
        let world = seen.into_inner().unwrap().upgrade();
        assert!(world.is_none_or(|world| live_after(world).is_empty()));
    }

    /// The check between seeds reports a world that something still holds.
    #[test]
    fn a_retained_world_is_not_empty() {
        let world = nabedra_sim::runtime::run(1, |world| async move { world });
        let held = world.clone();
        assert_eq!(live_after(world), ["World"]);
        assert_eq!(live_after(held), [""; 0]);
    }

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
