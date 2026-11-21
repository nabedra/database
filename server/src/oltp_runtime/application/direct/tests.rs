//! T6 evidence: the gateway on real 3-voter Data groups (typed openraft over
//! mTLS worker transports, real pools, the T5 leader executor on every
//! worker, the leadership watcher).
//!
//! Modelled: the gateway reaches each leader's executor in process (the
//! `Method::Direct` wire, its Control authorization and the Catalog-backed
//! topology run in `worker_runtime/direct_gateway/tests.rs` on one worker);
//! the topology source is scripted, so a test can publish a split; the
//! Timestamp authority is one shared counter; "the queued path" is
//! `Routed::Queued`, whose forward marker the tests inspect.
use super::*;
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::{
            CanonicalJson, Change, PreparedWrite,
            store::{
                KeySpan, LatestRead, ParticipantCommand, ParticipantHandle, ParticipantMachine,
                ParticipantReply, initial_root, lifecycle, page_root,
            },
        },
        pool::{OltpPool, PoolIdentity, SharedResourceConfig},
    },
    oltp_replication::{
        authority::{
            config::DataConfig,
            consensus::{Consensus, ConsensusFuture},
            status::VerifiedStatusDecision,
            timestamp::{DirectTimestampClient, DirectTimestampRange, DirectTimestampSource},
        },
        hosted::{HostedState, RaftConsensus, batched_config, direct_leadership},
        multiplex::{
            EndpointRegistry, PeerDirectory, Purpose, ResidentGroup, TransportConfig,
            TransportTrust, WorkerTransport,
        },
        owned::OwnedCommand,
        typed_log::TypedLogStore,
        typed_network::{ReplicaEndpoint, TypedNetworkFactory},
        types::{
            AppliedPosition, Authority, CommitTs, DecisionStatement, ManifestRef, Page,
            ParticipantPlan, ReplicaMember, RequestBinding, TxnDecision, TxnId, WorkerEnrollment,
            WorkerIdentity,
        },
    },
    oltp_runtime::direct::{DirectConfig, DirectExecutor, DirectGate, DirectTable, StaticSchema},
    protocol::{TableDescriptor, TableFormat},
};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize},
};
use tokio::{net::TcpListener, sync::oneshot};

#[path = "../../../../tests/support/tls.rs"]
mod tls;

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Peers(BTreeMap<Uuid, WorkerEnrollment>);
impl PeerDirectory for Peers {
    fn verify(
        &self,
        peer: WorkerIdentity,
        _group: &GroupIdentity,
        _purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if WorkerIdentity::from(&self.resolve(peer.worker_id)?) != peer {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "foreign fixture peer",
                Outcome::NotApplied,
            ));
        }
        Ok(TransportTrust::AppliedEnrollment)
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.0.get(&worker).cloned().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "unknown fixture peer",
                Outcome::Unknown,
            )
        })
    }
}

/// One strictly increasing Timestamp authority for the whole fixture.
struct Counter(AtomicU64);
impl Counter {
    fn next(&self) -> CommitTs {
        CommitTs::new(self.0.fetch_add(1, Ordering::SeqCst)).unwrap()
    }
}
impl DirectTimestampSource for Counter {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        let first = self.0.fetch_add(count, Ordering::SeqCst);
        Box::pin(async move {
            DirectTimestampRange::new(
                CommitTs::new(first).unwrap(),
                CommitTs::new(first + count - 1).unwrap(),
            )
        })
    }
}

struct Gate;
impl DirectGate for Gate {
    fn direct_active(&self) -> bool {
        true
    }
}

fn resources() -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        2,
        WorkerResources {
            max_threads: 4,
            ..Default::default()
        },
        SharedResourceConfig::default(),
    )
    .unwrap()
}

/// One worker: a pool, a transport and a leader executor shared by every
/// replica it hosts.
struct Node {
    id: u64,
    worker: WorkerEnrollment,
    pool_id: Uuid,
    shared: Arc<WorkerSharedResources>,
    pool: OltpPool,
    transport: Arc<WorkerTransport>,
    registry: Arc<EndpointRegistry>,
    executor: Arc<DirectExecutor>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
}
struct Replica {
    node: u64,
    consensus: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    handle: Arc<ParticipantHandle>,
    _resident: ResidentGroup,
}
struct Group {
    identity: GroupIdentity,
    span: KeySpan,
    routing_epoch: u64,
    replicas: Vec<Replica>,
}

struct Cluster {
    nodes: Vec<Node>,
    groups: Vec<Group>,
    cluster_id: Uuid,
    database: Text<128>,
    table: Uuid,
    catalog: Uuid,
    authority: Authority,
    timestamps: Arc<Counter>,
    schema: Arc<StaticSchema>,
    _dir: Dir,
}

fn descriptor(table: Uuid) -> TableDescriptor {
    TableDescriptor {
        id: table,
        name: "items".into(),
        schema: BTreeMap::from([
            ("id".to_string(), ColumnType::Int64),
            ("value".to_string(), ColumnType::Int64),
        ]),
        key: "id".into(),
        format: TableFormat::Parquet,
    }
}

fn span(start: Option<i64>, end: Option<i64>) -> KeySpan {
    KeySpan {
        start: start.map(Key::Int64),
        end: end.map(Key::Int64),
    }
}

impl Cluster {
    async fn new() -> Self {
        crate::oltp_runtime::direct::trace_for_test();
        let dir = Dir(std::env::temp_dir().join(format!("t6-gateway-{}", Uuid::new_v4())));
        std::fs::create_dir_all(&dir.0).unwrap();
        let certificates = tls::certificates(&dir.0, 3);
        let cluster_id = Uuid::new_v4();
        let table = Uuid::new_v4();
        let mut listeners = Vec::new();
        let mut workers = Vec::new();
        for tls in &certificates {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            workers.push(WorkerEnrollment {
                worker_id: Uuid::new_v4(),
                incarnation: Uuid::new_v4(),
                certificate_sha256: Digest::parse(tls.certificate_sha256()).unwrap(),
                enrollment_revision: 1,
                endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            });
            listeners.push(listener);
        }
        let peers = Arc::new(Peers(
            workers
                .iter()
                .map(|worker| (worker.worker_id, worker.clone()))
                .collect(),
        ));
        let schema = Arc::new(StaticSchema::default());
        schema.insert(DirectTable {
            descriptor: descriptor(table),
            schema_epoch: 1,
            indexes: Vec::new(),
            // The executor activates each group on its first request (T7).
            lifecycle_id: table,
        });
        let timestamps = Arc::new(Counter(AtomicU64::new(1_000)));
        let mut nodes = Vec::new();
        for (index, ((worker, tls), listener)) in
            workers.iter().zip(&certificates).zip(listeners).enumerate()
        {
            let id = index as u64 + 1;
            let shared = resources();
            let pool_id = Uuid::new_v4();
            let pool = OltpPool::open_development(
                dir.0.join(format!("pool{id}")),
                PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: 2,
                    cluster_id,
                    worker_id: worker.worker_id,
                    pool_id,
                },
                shared.clone(),
            )
            .unwrap();
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            let transport = Arc::new(
                WorkerTransport::new(
                    worker,
                    tls,
                    shared.clone(),
                    peers.clone(),
                    registry.clone(),
                    TransportConfig::default(),
                )
                .unwrap(),
            );
            let (stop, stopped) = oneshot::channel();
            let task = tokio::spawn(transport.clone().run(listener, async {
                let _ = stopped.await;
            }));
            let executor = DirectExecutor::new(
                shared.clone(),
                DirectConfig::default(),
                Arc::new(Gate),
                schema.clone(),
                DirectTimestampClient::new(None, timestamps.clone()),
            );
            nodes.push(Node {
                id,
                worker: worker.clone(),
                pool_id,
                shared,
                pool,
                transport,
                registry,
                executor,
                stop: Some(stop),
                task: Some(task),
            });
        }
        Self {
            nodes,
            groups: Vec::new(),
            cluster_id,
            database: Text::try_from("t6").unwrap(),
            table,
            catalog: Uuid::new_v4(),
            authority: Authority {
                run_id: Uuid::new_v4(),
                owner_term: 1,
            },
            timestamps,
            schema,
            _dir: dir,
        }
    }

    /// A new Data group of the table on all three workers, fenced with
    /// `span` and `routing_epoch`. Only `electors` may start elections.
    async fn add_group(&mut self, span: KeySpan, routing_epoch: u64, electors: &[u64]) -> usize {
        let identity = GroupIdentity {
            cluster_id: self.cluster_id,
            database: self.database.clone(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: self.table,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: 1,
        };
        let members: BTreeMap<u64, ReplicaMember> = self
            .nodes
            .iter()
            .map(|node| {
                (
                    node.id,
                    ReplicaMember {
                        node_id: node.id,
                        worker_id: node.worker.worker_id,
                        pool_id: node.pool_id,
                    },
                )
            })
            .collect();
        let mut replicas = Vec::new();
        for node in &self.nodes {
            let store = node.pool.group(identity.clone()).await.unwrap();
            let (resident, lease) = node
                .registry
                .reserve_replica(identity.clone(), node.id)
                .unwrap();
            let log = TypedLogStore::<DataConfig>::new(store.clone(), node.id)
                .await
                .unwrap()
                .with_resident(lease.clone())
                .unwrap();
            let machine = ParticipantMachine::new(store.clone()).unwrap();
            let state = HostedState::new(machine.clone(), lease.clone()).unwrap();
            let config = batched_config(openraft::Config {
                enable_elect: electors.contains(&node.id),
                heartbeat_interval: 150,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            })
            .unwrap();
            let network = TypedNetworkFactory::<DataConfig>::new(
                node.transport.client(),
                members[&node.id].clone(),
                lease.clone(),
                node.shared.clone(),
            )
            .unwrap();
            let consensus = RaftConsensus::start(config, network, log, state)
                .await
                .unwrap();
            direct_leadership::watch(&consensus);
            let endpoint = Arc::new(
                ReplicaEndpoint::new(
                    consensus.raft.clone(),
                    consensus.state.clone(),
                    lease,
                    node.shared.clone(),
                    members.clone(),
                )
                .unwrap(),
            );
            for purpose in [Purpose::Raft, Purpose::RaftProgress, Purpose::Snapshot] {
                resident.bind(purpose, endpoint.clone()).unwrap();
            }
            let dyn_consensus: Arc<dyn Consensus<ParticipantCommand, ParticipantReply>> =
                consensus.clone();
            replicas.push(Replica {
                node: node.id,
                consensus,
                handle: Arc::new(ParticipantHandle::new(machine, dyn_consensus)),
                _resident: resident,
            });
        }
        let first = replicas
            .iter()
            .position(|replica| electors.contains(&replica.node))
            .unwrap();
        replicas[first]
            .consensus
            .raft
            .initialize(members)
            .await
            .unwrap();
        self.groups.push(Group {
            identity,
            span: span.clone(),
            routing_epoch,
            replicas,
        });
        let group = self.groups.len() - 1;
        let (transition, authority, catalog) = (Uuid::new_v4(), self.authority, self.catalog);
        watchdog(|| async {
            let leader = self.leader(group).await;
            self.groups[group].replicas[leader]
                .handle
                .fence_initial(
                    transition,
                    authority,
                    catalog,
                    1,
                    routing_epoch,
                    span.clone(),
                )
                .await
                .ok()
        })
        .await;
        let leader = self.leader(group).await;
        self.serving(group, leader).await;
        group
    }

    /// The index of the replica that leads `group`.
    async fn leader(&self, group: usize) -> usize {
        watchdog(|| async {
            self.groups[group].replicas.iter().position(|replica| {
                self.nodes[replica.node as usize - 1].task.is_some()
                    && replica
                        .consensus
                        .raft
                        .metrics()
                        .borrow_watched()
                        .current_leader
                        == Some(replica.node)
            })
        })
        .await
    }
    async fn serving(&self, group: usize, replica: usize) {
        let clock = self.groups[group].replicas[replica]
            .handle
            .direct_clock()
            .unwrap()
            .clone();
        watchdog(|| async { clock.leader_token().map(|_| ()) }).await;
    }
    fn fill(&self, groups: &[usize]) -> TableFill {
        TableFill {
            table_id: self.table,
            key_column: "id".into(),
            key_type: ColumnType::Int64,
            schema_epoch: 1,
            shards: groups.iter().map(|g| self.shard(*g)).collect(),
        }
    }
    fn shard(&self, group: usize) -> ShardRoute {
        let group = &self.groups[group];
        ShardRoute {
            group: group.identity.clone(),
            span: group.span.clone(),
            schema_epoch: 1,
            routing_epoch: group.routing_epoch,
        }
    }
    fn voters(&self) -> Vec<Voter> {
        self.nodes
            .iter()
            .map(|node| Voter {
                node_id: node.id,
                worker_id: node.worker.worker_id,
            })
            .collect()
    }
    fn transport(&self) -> Arc<InProcess> {
        let mut routes = HashMap::new();
        for group in &self.groups {
            for replica in &group.replicas {
                let node = &self.nodes[replica.node as usize - 1];
                routes.insert(
                    (group.identity.group_id, replica.node),
                    (node.executor.clone(), replica.handle.clone()),
                );
            }
        }
        Arc::new(InProcess {
            routes: Mutex::new(routes),
            down: Mutex::default(),
            retired: Mutex::default(),
            deny: AtomicBool::new(false),
            lose: AtomicUsize::new(0),
            after_lose: Mutex::default(),
            sent: AtomicU64::new(0),
        })
    }
    fn gateway(
        &self,
        switch: Arc<GatewaySwitch>,
        source: Arc<Source>,
        transport: Arc<InProcess>,
    ) -> Arc<DirectGateway> {
        DirectGateway::new(
            resources(),
            self.database.clone(),
            switch,
            source,
            transport,
        )
    }
    fn body(&self, request_id: Uuid, operation: &str, mut payload: Value) -> Vec<u8> {
        payload["table_id"] = json!(self.table);
        serde_json::to_vec(&json!({
            "request_id": request_id,
            "operation": operation,
            "payload": payload,
        }))
        .unwrap()
    }
    fn row_key(&self, id: i64) -> MvccKey {
        MvccKey::Row {
            table_id: self.table,
            primary_key: Key::Int64(id),
        }
    }
    /// The newest version of `id` in `group` on replica `replica`.
    async fn latest(
        &self,
        group: usize,
        replica: usize,
        id: i64,
    ) -> (Option<CommitTs>, Option<Value>) {
        let handle = &self.groups[group].replicas[replica].handle;
        match handle.direct_latest(self.row_key(id)).await.unwrap() {
            LatestRead::Value { version, value } => (
                version,
                value.map(|value| serde_json::from_str(value.get()).unwrap()),
            ),
            other => panic!("unexpected read {other:?}"),
        }
    }
    /// The dedup record or forward marker of `request_id` at `anchor`.
    async fn settled(&self, group: usize, anchor: i64, request_id: Uuid) -> bool {
        let leader = self.leader(group).await;
        let handle = &self.groups[group].replicas[leader].handle;
        handle.direct_barrier().await.unwrap();
        handle
            .direct_dedup_after_barrier(self.table, Key::Int64(anchor), request_id)
            .await
            .unwrap()
            .is_some()
    }
    async fn propose(&self, group: usize, command: ParticipantCommand) -> ParticipantReply {
        let leader = self.leader(group).await;
        let replica = &self.groups[group].replicas[leader];
        let shared = &self.nodes[replica.node as usize - 1].shared;
        replica
            .handle
            .propose(OwnedCommand::new_owned(command, shared, ByteClass::Progress).unwrap())
            .await
            .unwrap()
            .reply
    }
    async fn stop_node(&mut self, node: u64) {
        for group in &self.groups {
            for replica in group.replicas.iter().filter(|r| r.node == node) {
                replica.consensus.shutdown().await.unwrap();
            }
        }
        let node = &mut self.nodes[node as usize - 1];
        if let Some(stop) = node.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = node.task.take() {
            let _ = task.await;
        }
    }
    async fn shutdown(mut self) {
        for node in 1..=3 {
            if self.nodes[node as usize - 1].task.is_some() {
                self.stop_node(node).await;
            }
        }
    }
}

/// The leaders' executors, reached in process. `lose` drops that many
/// replies after the leader executed them (a lost reply); a node in
/// `down` is unreachable.
/// A replica's executor and handle, by (group id, node id).
type Replicas = HashMap<(Uuid, u64), (Arc<DirectExecutor>, Arc<ParticipantHandle>)>;
struct InProcess {
    routes: Mutex<Replicas>,
    down: Mutex<HashSet<u64>>,
    retired: Mutex<HashSet<Uuid>>,
    deny: AtomicBool,
    lose: AtomicUsize,
    /// Runs once, when a reply is lost (before the gateway resends).
    after_lose: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    sent: AtomicU64,
}
impl DirectTransport for InProcess {
    fn send<'a>(
        &'a self,
        group: &'a GroupIdentity,
        voter: Voter,
        request: &'a DirectRequest,
        forward: bool,
    ) -> SendFuture<'a> {
        Box::pin(async move {
            self.sent.fetch_add(1, Ordering::Relaxed);
            if self.deny.load(Ordering::SeqCst) {
                return Err(DirectSendError::NotApplied(ProtocolError::new(
                    ErrorCode::Unauthorized,
                    "fixture worker denied the caller",
                    Outcome::NotApplied,
                )));
            }
            let unreachable = || {
                DirectSendError::Uncertain(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "unreachable fixture worker",
                    Outcome::Unknown,
                ))
            };
            if self.down.lock().unwrap().contains(&voter.node_id) {
                return Err(unreachable());
            }
            if self.retired.lock().unwrap().contains(&group.group_id) {
                return Ok(DirectResponse::NotLeader { hint: None });
            }
            let Some((executor, handle)) = self
                .routes
                .lock()
                .unwrap()
                .get(&(group.group_id, voter.node_id))
                .cloned()
            else {
                return Err(DirectSendError::NotApplied(ProtocolError::new(
                    ErrorCode::ValidationError,
                    "no such replica",
                    Outcome::NotApplied,
                )));
            };
            let response = if forward {
                executor.forward(&handle, request.clone()).await
            } else {
                executor.execute(&handle, request.clone()).await
            };
            let mut left = self.lose.load(Ordering::SeqCst);
            while left > 0 {
                match self
                    .lose
                    .compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => {
                        if let Some(hook) = self.after_lose.lock().unwrap().take() {
                            hook();
                        }
                        return Err(unreachable());
                    }
                    Err(now) => left = now,
                }
            }
            Ok(response)
        })
    }
}

/// A scripted Catalog/Control: `fills` are returned in order, the last one
/// for every later read.
struct Source {
    fills: Mutex<VecDeque<TableFill>>,
    voters: Vec<Voter>,
    table_reads: AtomicU64,
    placement_reads: AtomicU64,
}
impl Source {
    fn new(fills: Vec<TableFill>, voters: Vec<Voter>) -> Arc<Self> {
        Arc::new(Self {
            fills: Mutex::new(fills.into()),
            voters,
            table_reads: AtomicU64::new(0),
            placement_reads: AtomicU64::new(0),
        })
    }
    fn reads(&self) -> (u64, u64) {
        (
            self.table_reads.load(Ordering::SeqCst),
            self.placement_reads.load(Ordering::SeqCst),
        )
    }
}
impl TopologySource for Source {
    fn table(&self, _table_id: Uuid) -> EndpointFuture<'_, Option<TableFill>> {
        self.table_reads.fetch_add(1, Ordering::SeqCst);
        let mut fills = self.fills.lock().unwrap();
        let fill = if fills.len() > 1 {
            fills.pop_front()
        } else {
            fills.front().cloned()
        };
        Box::pin(async move { Ok(fill) })
    }
    fn placement<'a>(
        &'a self,
        _group: &'a GroupIdentity,
        _refresh: bool,
    ) -> EndpointFuture<'a, Vec<Voter>> {
        self.placement_reads.fetch_add(1, Ordering::SeqCst);
        let voters = self.voters.clone();
        Box::pin(async move { Ok(voters) })
    }
}

fn direct_on() -> Arc<GatewaySwitch> {
    let switch = Arc::new(GatewaySwitch::default());
    switch.set_active(true);
    switch
}

#[derive(Debug, Clone, PartialEq)]
enum Answer {
    Data(Value),
    Queued,
    Error(ErrorCode, Outcome),
}
async fn answer(gateway: &DirectGateway, body: &[u8], request_id: Uuid) -> Answer {
    match gateway.route(body, false).await {
        Ok(Routed::Done(encoded)) => {
            let envelope: Value = serde_json::from_slice(&encoded.body).unwrap();
            assert_eq!(envelope["request_id"], json!(request_id), "{envelope}");
            if encoded.status == 200 {
                Answer::Data(envelope["data"].clone())
            } else {
                panic!("a direct Done is a success envelope: {envelope}")
            }
        }
        Ok(Routed::Queued) => Answer::Queued,
        Err(error) => Answer::Error(error.code, error.outcome),
    }
}

async fn watchdog<T, F: Future<Output = Option<T>>>(mut probe: impl FnMut() -> F) -> T {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("gateway fixture watchdog")
}

struct BlackholedLeader {
    disconnected: Mutex<Option<Instant>>,
}

impl DirectTransport for BlackholedLeader {
    fn send<'a>(
        &'a self,
        _group: &'a GroupIdentity,
        voter: Voter,
        request: &'a DirectRequest,
        _forward: bool,
    ) -> SendFuture<'a> {
        let disconnected = *self.disconnected.lock().unwrap();
        Box::pin(async move {
            let reply = match (voter.node_id, disconnected) {
                (1, Some(_)) => std::future::pending().await,
                (1, None) => DirectResponse::Done(DirectData::from_json("null").unwrap()),
                (2, Some(since)) if since.elapsed() >= Duration::from_millis(1_800) => {
                    assert_eq!(request.digest, digest(request.body.as_ref()));
                    DirectResponse::Done(DirectData::from_json("true").unwrap())
                }
                _ => DirectResponse::NotLeader { hint: Some(1) },
            };
            Ok(reply)
        })
    }
}

/// O06/O18: a cached Data leader stops answering. The gateway must get past
/// both the silent connection and the survivors' stale leader hints.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_o18_cached_blackholed_data_leader_leaves_time_to_retry() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1, 2, 3]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = Arc::new(BlackholedLeader {
        disconnected: Mutex::new(None),
    });
    let gateway = DirectGateway::new(
        resources(),
        cluster.database.clone(),
        direct_on(),
        source.clone(),
        transport.clone(),
    );
    let id = Uuid::new_v4();
    let get = cluster.body(id, "table.get", json!({"key": 1}));
    assert_eq!(answer(&gateway, &get, id).await, Answer::Data(Value::Null));
    *transport.disconnected.lock().unwrap() = Some(Instant::now());
    let id = Uuid::new_v4();
    let update = cluster.body(
        id,
        "table.update",
        json!({"key": 1, "values": {"value": 2}}),
    );
    let result = tokio::time::timeout(Duration::from_secs(4), answer(&gateway, &update, id)).await;
    cluster.shutdown().await;
    assert_eq!(
        result.expect("blackholed voter used the entire client budget"),
        Answer::Data(json!(true))
    );
    assert_eq!(source.reads(), (1, 1));
}

/// O03/D01 (gateway level): every eligible operation goes to the range
/// leader and returns the queued path's result shapes; the steady state
/// makes no Catalog or Control read per request; ineligible operations and
/// the off switch take the queued path without touching the topology.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o03_d01_gateway_routes_eligible_operations_with_zero_topology_reads_per_request() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let switch = direct_on();
    let gateway = cluster.gateway(switch.clone(), source.clone(), transport.clone());

    let cases: Vec<(&str, Value, Value)> = vec![
        ("table.get", json!({"key": 1}), Value::Null),
        (
            "table.insert",
            json!({"row": {"id": 1, "value": 10}}),
            Value::Null,
        ),
        (
            "table.get",
            json!({"key": 1}),
            json!({"id": 1, "value": 10}),
        ),
        (
            "table.update",
            json!({"key": 1, "values": {"value": 11}}),
            json!(true),
        ),
        (
            "table.update",
            json!({"key": 2, "values": {"value": 1}}),
            json!(false),
        ),
        (
            "table.upsert",
            json!({"row": {"id": 2, "value": 20}}),
            Value::Null,
        ),
        (
            "table.insert_many",
            json!({"rows": [{"id": 5, "value": 5}, {"id": 3, "value": 3}, {"id": 4, "value": 4}]}),
            json!(3),
        ),
        ("table.delete", json!({"key": 3}), json!(true)),
        ("table.delete", json!({"key": 3}), json!(false)),
        ("table.get", json!({"key": i64::MIN}), Value::Null),
        (
            "table.get",
            json!({"key": 2}),
            json!({"id": 2, "value": 20}),
        ),
    ];
    let mut latencies = Vec::new();
    for (operation, payload, expected) in &cases {
        let id = Uuid::new_v4();
        let started = std::time::Instant::now();
        let got = answer(&gateway, &cluster.body(id, operation, payload.clone()), id).await;
        latencies.push((*operation, started.elapsed()));
        assert_eq!(got, Answer::Data(expected.clone()), "{operation} {payload}");
    }
    // Definite errors pass through with the leader's code and outcome.
    let id = Uuid::new_v4();
    let conflict = answer(
        &gateway,
        &cluster.body(id, "table.insert", json!({"row": {"id": 1, "value": 0}})),
        id,
    )
    .await;
    assert_eq!(
        conflict,
        Answer::Error(ErrorCode::Conflict, Outcome::NotApplied)
    );
    // A key of the wrong type never reaches a leader: the queued path gives
    // the exact validation error.
    let id = Uuid::new_v4();
    let sent = transport.sent.load(Ordering::Relaxed);
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(id, "table.get", json!({"key": "one"})),
            id
        )
        .await,
        Answer::Queued
    );
    assert_eq!(transport.sent.load(Ordering::Relaxed), sent);
    // One Catalog read and one placement read filled the cache; nothing
    // after that, over every request above.
    assert_eq!(source.reads(), (1, 1));
    let stats = gateway.stats();
    assert_eq!(stats.catalog_reads, 1);
    assert_eq!(stats.placement_reads, 1);
    assert_eq!(stats.direct_done, cases.len() as u64);
    assert_eq!(gateway.cache().len(), 1);
    // Ineligible operations and malformed bodies: queued, no topology read.
    for (operation, payload) in [
        ("table.describe", json!({"name": "items"})),
        ("query.count", json!({"query": {}})),
        ("table.drop", json!({})),
    ] {
        let id = Uuid::new_v4();
        assert_eq!(
            answer(&gateway, &cluster.body(id, operation, payload), id).await,
            Answer::Queued
        );
    }
    assert_eq!(
        gateway
            .route(b"{\"request_id\":1}", false)
            .await
            .ok()
            .map(|r| matches!(r, Routed::Queued)),
        Some(true)
    );
    assert_eq!(source.reads(), (1, 1));
    // O01: with the capability inactive nothing is routed or read.
    switch.set_active(false);
    let sent = transport.sent.load(Ordering::Relaxed);
    for (operation, payload, _) in &cases {
        let id = Uuid::new_v4();
        assert_eq!(
            answer(&gateway, &cluster.body(id, operation, payload.clone()), id).await,
            Answer::Queued
        );
    }
    assert_eq!(transport.sent.load(Ordering::Relaxed), sent);
    // `direct_oltp: false` while active: a mutation is settled at its
    // anchor (a forward marker), then queued. A get is queued as is: no
    // forward, no marker, no topology read (user decision 2026-11-21,
    // RV-FINAL M3).
    switch.set_active(true);
    switch.set_direct_oltp(false);
    let id = Uuid::new_v4();
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(
                id,
                "table.update",
                json!({"key": 1, "values": {"value": 99}})
            ),
            id
        )
        .await,
        Answer::Queued
    );
    assert!(cluster.settled(group, 1, id).await, "forward marker");
    assert_eq!(
        cluster.latest(group, 0, 1).await.1,
        Some(json!({"id": 1, "value": 11}))
    );
    let sent = transport.sent.load(Ordering::Relaxed);
    let get = Uuid::new_v4();
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(get, "table.get", json!({"key": 1})),
            get
        )
        .await,
        Answer::Queued
    );
    assert_eq!(transport.sent.load(Ordering::Relaxed), sent, "no forward");
    assert!(!cluster.settled(group, 1, get).await, "no get marker");
    // `POST /v1/requests` (asynchronous): the same for a get.
    let get = Uuid::new_v4();
    assert!(matches!(
        gateway
            .route(&cluster.body(get, "table.get", json!({"key": 1})), true)
            .await,
        Ok(Routed::Queued)
    ));
    assert_eq!(transport.sent.load(Ordering::Relaxed), sent, "no forward");
    assert!(!cluster.settled(group, 1, get).await, "no get marker");
    // The queued-history window (T7c): mutations are settled and queued,
    // gets read directly.
    switch.set_direct_oltp(true);
    switch.set_history(true);
    let update = Uuid::new_v4();
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(
                update,
                "table.update",
                json!({"key": 1, "values": {"value": 98}})
            ),
            update
        )
        .await,
        Answer::Queued
    );
    assert!(cluster.settled(group, 1, update).await, "forward marker");
    let get = Uuid::new_v4();
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(get, "table.get", json!({"key": 1})),
            get
        )
        .await,
        Answer::Data(json!({"id": 1, "value": 11}))
    );
    assert!(!cluster.settled(group, 1, get).await, "no get marker");
    switch.set_history(false);
    switch.set_direct_oltp(false);
    // The same id directly later: the marker sends it queued again, and it
    // never applies directly.
    switch.set_direct_oltp(true);
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(
                id,
                "table.update",
                json!({"key": 1, "values": {"value": 99}})
            ),
            id
        )
        .await,
        Answer::Queued
    );
    assert_eq!(
        cluster.latest(group, 0, 1).await.1,
        Some(json!({"id": 1, "value": 11}))
    );
    // An oversized batch is settled at its anchor (its smallest key) and queued.
    // Whitespace pads the body past 1 MiB with few rows.
    let pad = " ".repeat(300);
    let rows: Vec<String> = (0..4000)
        .map(|i| format!("{{\"id\":{}{pad},\"value\":{i}}}", 100_000 + i))
        .collect();
    let id = Uuid::new_v4();
    let body = format!(
        r#"{{"request_id":"{id}","operation":"table.insert_many","payload":{{"table_id":"{}","rows":[{}]}}}}"#,
        cluster.table,
        rows.join(",")
    )
    .into_bytes();
    assert!(body.len() > MAX_DIRECT_BATCH_BODY);
    assert_eq!(answer(&gateway, &body, id).await, Answer::Queued);
    assert!(cluster.settled(group, 100_000, id).await);
    assert_eq!(cluster.latest(group, 0, 100_000).await.1, None);
    assert_eq!(source.reads(), (1, 1));
    let total: Duration = latencies.iter().map(|(_, t)| *t).sum();
    eprintln!(
        "o03 gateway latency: {} ops, mean {:.2} ms, per op {:?}",
        latencies.len(),
        total.as_secs_f64() * 1e3 / latencies.len() as f64,
        latencies
            .iter()
            .map(|(op, t)| format!("{op}={:.2}ms", t.as_secs_f64() * 1e3))
            .collect::<Vec<_>>()
    );
    cluster.shutdown().await;
}

/// Section 6 gateway rules and D02/D04 at the gateway: `NotLeader` follows
/// the hint; a lost reply is resent with the same id and body and returns
/// the original result once; the leader's loss moves the request to the new
/// leader; with two voters gone the deadline answers without success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o03_d04_gateway_follows_the_leader_and_resends_after_a_lost_reply() {
    let mut cluster = Cluster::new().await;
    // Node 2 leads; the gateway tries node 1 first and learns the hint.
    let group = cluster.add_group(span(None, None), 1, &[2]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    let id = Uuid::new_v4();
    let insert = cluster.body(id, "table.insert", json!({"row": {"id": 7, "value": 1}}));
    assert_eq!(
        answer(&gateway, &insert, id).await,
        Answer::Data(Value::Null)
    );
    assert_eq!(gateway.stats().not_leader, 1);
    let table = gateway.cache().get(cluster.table).unwrap();
    assert_eq!(table.ranges[0].leader_hint(), Some(2));
    // A lost reply: the leader committed, the gateway resends the same id
    // and body and gets the original result; one version.
    transport.lose.store(1, Ordering::SeqCst);
    let id = Uuid::new_v4();
    let update = cluster.body(
        id,
        "table.update",
        json!({"key": 7, "values": {"value": 2}}),
    );
    assert_eq!(
        answer(&gateway, &update, id).await,
        Answer::Data(json!(true))
    );
    assert_eq!(gateway.stats().resends, 1);
    let (version, row) = cluster.latest(group, 1, 7).await;
    assert_eq!(row, Some(json!({"id": 7, "value": 2})));
    // A retry of the acknowledged request (another gateway) replays.
    let other = cluster.gateway(direct_on(), source.clone(), transport.clone());
    assert_eq!(answer(&other, &update, id).await, Answer::Data(json!(true)));
    assert_eq!(cluster.latest(group, 1, 7).await.0, version);
    assert_eq!(
        source.reads(),
        (2, 2),
        "one fill per gateway, none per request"
    );
    cluster.shutdown().await;
}

/// O28/D07: a cached parent cannot report a newer epoch after its voters
/// retire. Settle the queued mutation at the child from a refreshed Catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o28_d07_queue_settlement_refreshes_a_retired_split_parent() {
    let mut cluster = Cluster::new().await;
    let parent = cluster.add_group(span(None, None), 1, &[1]).await;
    let left = cluster.add_group(span(None, Some(5)), 2, &[1]).await;
    let right = cluster.add_group(span(Some(5), None), 2, &[1]).await;
    let source = Source::new(
        vec![cluster.fill(&[parent]), cluster.fill(&[left, right])],
        cluster.voters(),
    );
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source.clone(), transport.clone())
        .with_deadline(Duration::from_secs(2));
    let id = Uuid::new_v4();
    let get = cluster.body(id, "table.get", json!({"key": 7}));
    assert_eq!(answer(&gateway, &get, id).await, Answer::Data(Value::Null));
    assert_eq!(source.reads(), (1, 1));
    transport
        .retired
        .lock()
        .unwrap()
        .insert(cluster.shard(parent).group.group_id);

    let id = Uuid::new_v4();
    let mutation = cluster.body(id, "table.insert", json!({"row": {"id": 7, "value": 1}}));
    let error = gateway.route(&mutation, true).await.err().unwrap();
    assert_eq!(error.code, ErrorCode::OutcomeUnknown);
    assert_eq!(error.outcome, Outcome::Unknown);
    assert_eq!(source.reads(), (1, 1));

    assert!(matches!(
        gateway.route(&mutation, true).await,
        Ok(Routed::Queued)
    ));
    assert_eq!(source.reads(), (2, 2));
    assert_eq!(answer(&gateway, &mutation, id).await, Answer::Queued);
    assert_eq!(cluster.latest(right, 0, 7).await.1, None);
    cluster.shutdown().await;
}

/// RV-FINAL L2, D02, Q05: an insert commits but its reply is lost. Before
/// the resend reaches the leader, the table's descriptor changes so the body
/// no longer validates; the leader checks that before its dedup lookup and
/// answers a definite `validation_error`. The gateway does not report that
/// failure for a write that committed: it settles the id at the anchor,
/// whose dedup record replays the original result. Before the fix the client
/// got `validation_error`/`not_applied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_q05_rv_final_l2_uncertain_resend_never_reports_a_definite_failure() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    let id = Uuid::new_v4();
    let insert = cluster.body(id, "table.insert", json!({"row": {"id": 7, "value": 1}}));
    let schema = cluster.schema.clone();
    let table = cluster.table;
    *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
        let mut changed = descriptor(table);
        changed.schema.insert("value".to_string(), ColumnType::Utf8);
        schema.insert(DirectTable {
            descriptor: changed,
            schema_epoch: 1,
            indexes: Vec::new(),
            lifecycle_id: table,
        });
    }));
    transport.lose.store(1, Ordering::SeqCst);
    assert_eq!(
        answer(&gateway, &insert, id).await,
        Answer::Data(Value::Null)
    );
    assert!(
        transport.after_lose.lock().unwrap().is_none(),
        "a reply was lost"
    );
    let stats = gateway.stats();
    assert_eq!(stats.resends, 1, "{stats:?}");
    assert_eq!(stats.forward_replayed, 1, "{stats:?}");
    // One committed version of the row.
    let (version, row) = cluster.latest(group, 0, 7).await;
    assert!(version.is_some());
    assert_eq!(row, Some(json!({"id": 7, "value": 1})));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_o10_bug1_refusal_after_lost_reply_preserves_the_commit_outcome() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let switch = direct_on();
    let gateway = cluster.gateway(switch.clone(), source, transport.clone());
    let id = Uuid::new_v4();
    let insert = cluster.body(id, "table.insert", json!({"row": {"id": 7, "value": 1}}));
    let deny = Arc::downgrade(&transport);
    *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
        deny.upgrade().unwrap().deny.store(true, Ordering::SeqCst);
    }));
    transport.lose.store(1, Ordering::SeqCst);

    let lost_reply = answer(&gateway, &insert, id).await;
    let (version, row) = cluster.latest(group, 0, 7).await;
    assert!(version.is_some());
    assert_eq!(row, Some(json!({"id": 7, "value": 1})));
    let refused_id = Uuid::new_v4();
    let refused = cluster.body(
        refused_id,
        "table.insert",
        json!({"row": {"id": 8, "value": 2}}),
    );
    assert_eq!(
        answer(&gateway, &refused, refused_id).await,
        Answer::Error(ErrorCode::Unauthorized, Outcome::NotApplied)
    );
    assert_eq!(cluster.latest(group, 0, 8).await, (None, None));
    transport.deny.store(false, Ordering::SeqCst);
    switch.set_direct_oltp(false);
    assert_eq!(
        answer(&gateway, &insert, id).await,
        Answer::Data(Value::Null)
    );
    let changed = cluster.body(id, "table.insert", json!({"row": {"id": 7, "value": 2}}));
    assert_eq!(
        answer(&gateway, &changed, id).await,
        Answer::Error(ErrorCode::Conflict, Outcome::NotApplied)
    );
    assert_eq!(cluster.latest(group, 0, 7).await.0, version);
    cluster.shutdown().await;
    assert_eq!(
        lost_reply,
        Answer::Error(ErrorCode::OutcomeUnknown, Outcome::Unknown)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o03_bug1_get_without_a_reply_has_no_unknown_mutation_outcome() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source, transport.clone())
        .with_deadline(Duration::from_millis(250));
    transport.down.lock().unwrap().extend([1, 2, 3]);
    let id = Uuid::new_v4();
    let get = cluster.body(id, "table.get", json!({"key": 7}));

    let missing_reply = answer(&gateway, &get, id).await;
    assert!(gateway.stats().resends > 0);
    assert!(!cluster.settled(group, 7, id).await);
    transport.down.lock().unwrap().clear();
    assert_eq!(answer(&gateway, &get, id).await, Answer::Data(Value::Null));
    cluster.shutdown().await;
    assert_eq!(
        missing_reply,
        Answer::Error(ErrorCode::StorageUnavailable, Outcome::NotApplied)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d04_o13_bug1_raft_follower_refuses_the_proposal_before_append() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let follower = &cluster.groups[group].replicas[1];
    let command = ParticipantCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: Some(cluster.authority),
        next: cluster.authority,
        schema_epoch: 1,
        routing_epoch: 1,
        span: span(None, None),
        catalog_group_id: cluster.catalog,
    };
    let command =
        OwnedCommand::new_owned(command, follower.handle.resources(), ByteClass::Progress).unwrap();
    let error = follower
        .handle
        .propose(command)
        .await
        .err()
        .expect("follower forwarded write");
    cluster.shutdown().await;
    assert_eq!(error.code, ErrorCode::StorageUnavailable);
    assert_eq!(error.outcome, Outcome::NotApplied);
}

/// D04/D05 at the gateway with elections: the leader stops, the request
/// completes on the new leader; with two voters gone the deadline answers
/// without a success and the row is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d04_d05_gateway_retries_on_the_new_leader_and_answers_at_the_deadline() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1, 2, 3]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source.clone(), transport.clone())
        .with_deadline(Duration::from_secs(15));
    let id = Uuid::new_v4();
    let insert = cluster.body(id, "table.insert", json!({"row": {"id": 1, "value": 1}}));
    assert_eq!(
        answer(&gateway, &insert, id).await,
        Answer::Data(Value::Null)
    );
    let leader = cluster.groups[group].replicas[cluster.leader(group).await].node;
    transport.down.lock().unwrap().insert(leader);
    cluster.stop_node(leader).await;
    let id = Uuid::new_v4();
    let update = cluster.body(
        id,
        "table.update",
        json!({"key": 1, "values": {"value": 2}}),
    );
    assert_eq!(
        answer(&gateway, &update, id).await,
        Answer::Data(json!(true))
    );
    let survivor = cluster.leader(group).await;
    assert_ne!(cluster.groups[group].replicas[survivor].node, leader);
    assert_eq!(
        cluster.latest(group, survivor, 1).await.1,
        Some(json!({"id": 1, "value": 2}))
    );
    // Two voters gone: no success, and nothing changed.
    let second = cluster.groups[group].replicas[survivor].node;
    transport.down.lock().unwrap().insert(second);
    cluster.stop_node(second).await;
    let short = cluster
        .gateway(direct_on(), source.clone(), transport.clone())
        .with_deadline(Duration::from_secs(3));
    let id = Uuid::new_v4();
    let update = cluster.body(
        id,
        "table.update",
        json!({"key": 1, "values": {"value": 3}}),
    );
    let started = std::time::Instant::now();
    match answer(&short, &update, id).await {
        Answer::Error(ErrorCode::OutcomeUnknown, Outcome::Unknown)
        | Answer::Error(ErrorCode::StorageUnavailable, Outcome::NotApplied) => {}
        other => panic!("two voters down must not succeed: {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(5));
    let last = cluster.groups[group]
        .replicas
        .iter()
        .position(|r| r.node != leader && r.node != second)
        .unwrap();
    let (_, row) = cluster.latest(group, last, 1).await;
    assert_eq!(row, Some(json!({"id": 1, "value": 2})));
    cluster.shutdown().await;
}

// D07/O07: the old cached route made a batch look eligible. After a stale-route
// reply, the gateway must settle at the anchor and queue the entire batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d07_o07_route_refresh_reclassifies_cross_range_batch() {
    let mut cluster = Cluster::new().await;
    let left = cluster.add_group(span(None, Some(500)), 2, &[1]).await;
    let right = cluster.add_group(span(Some(500), None), 2, &[1]).await;
    let mut before = cluster.fill(&[left]);
    before.shards[0].span = span(None, None);
    before.shards[0].routing_epoch = 1;
    let after = cluster.fill(&[left, right]);
    let source = Source::new(vec![before, after], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source.clone(), cluster.transport());
    let request_id = Uuid::new_v4();
    let body = cluster.body(
        request_id,
        "table.insert_many",
        json!({"rows": [{"id": 600, "value": 60}, {"id": 100, "value": 10}]}),
    );

    assert_eq!(answer(&gateway, &body, request_id).await, Answer::Queued);
    assert!(cluster.settled(left, 100, request_id).await);
    assert_eq!(cluster.latest(left, 0, 100).await, (None, None));
    assert_eq!(cluster.latest(right, 0, 600).await, (None, None));
    assert_eq!(source.reads().0, 2);
    cluster.shutdown().await;
}

/// D07/O16: a split is published and a range leader moves while writers
/// run. Each request ends with one outcome (direct success or queued after
/// its forward marker), no row is lost or doubled, and the stale route is
/// refreshed once for all requests. A range held by the split barrier
/// answers `Fallback`, which the gateway settles at the anchor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d07_o16_split_and_leader_move_under_load_give_one_outcome_per_request() {
    let mut cluster = Cluster::new().await;
    // The post-split groups: the source keeps the left half at epoch 2, the
    // right half moved to a new group.
    let left = cluster
        .add_group(span(None, Some(500)), 2, &[1, 2, 3])
        .await;
    let right = cluster
        .add_group(span(Some(500), None), 2, &[1, 2, 3])
        .await;
    // The gateway's first read predates the split: one range, epoch 1.
    let mut before = cluster.fill(&[left]);
    before.shards[0].span = span(None, None);
    before.shards[0].routing_epoch = 1;
    let after = cluster.fill(&[left, right]);
    let source = Source::new(vec![before, after], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    let cluster = Arc::new(cluster);

    // Phase 1: 48 writers, half on each side; a leader moves mid-load.
    let keys: Vec<i64> = (0..48)
        .map(|i| if i % 2 == 0 { i } else { 500 + i })
        .collect();
    let ids: Vec<Uuid> = keys.iter().map(|_| Uuid::new_v4()).collect();
    let mut tasks = Vec::new();
    for (key, id) in keys.iter().copied().zip(ids.iter().copied()) {
        let gateway = gateway.clone();
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let body = cluster.body(
                id,
                "table.insert",
                json!({"row": {"id": key, "value": key}}),
            );
            (key, id, body.clone(), answer(&gateway, &body, id).await)
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let moved = {
        let leader = cluster.leader(left).await;
        let follower = (leader + 1) % 3;
        cluster.groups[left].replicas[follower]
            .consensus
            .raft
            .trigger()
            .elect(false)
            .await
            .unwrap();
        cluster.groups[left].replicas[leader].node
    };
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    for (key, _, _, outcome) in &outcomes {
        assert_eq!(*outcome, Answer::Data(Value::Null), "key {key}");
    }
    let stats = gateway.stats();
    assert!(stats.stale_routes >= 1, "{stats:?}");
    // One fill plus exactly one refresh, however many requests saw it.
    assert_eq!(stats.catalog_reads, 2, "{stats:?}");
    assert_eq!(source.reads().0, 2);
    eprintln!("d07 phase 1: {stats:?}; left leader moved away from node {moved}");

    // Phase 2: the split barrier holds the right range. Its requests fall
    // back, are settled at the anchor and go queued; left requests commit.
    let split_id = Uuid::new_v4();
    let pause = |paused| {
        ParticipantCommand::DirectPause(crate::oltp::mvcc::store::direct::DirectPause {
            authority: cluster.authority,
            split_id,
            paused,
        })
    };
    assert!(matches!(
        cluster.propose(right, pause(true)).await,
        ParticipantReply::DirectPaused { .. }
    ));
    let phase2: Vec<(i64, Uuid)> = (0..16)
        .map(|i| (if i % 2 == 0 { 100 + i } else { 600 + i }, Uuid::new_v4()))
        .collect();
    let mut tasks = Vec::new();
    for (key, id) in phase2.iter().copied() {
        let gateway = gateway.clone();
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let body = cluster.body(
                id,
                "table.insert",
                json!({"row": {"id": key, "value": key}}),
            );
            (key, id, body.clone(), answer(&gateway, &body, id).await)
        }));
    }
    let mut queued = Vec::new();
    for task in tasks {
        let (key, id, body, outcome) = task.await.unwrap();
        if key < 500 && outcome != Answer::Data(Value::Null) {
            for (index, replica) in cluster.groups[left].replicas.iter().enumerate() {
                let metrics = replica.consensus.raft.metrics().borrow_watched().clone();
                eprintln!(
                    "d07 left replica {index} node {}: state {:?} leader {:?} term {} applied {:?} serving {} leadership {:?} executor {:?}",
                    replica.node,
                    metrics.state,
                    metrics.current_leader,
                    metrics.current_term,
                    metrics.last_applied,
                    replica.handle.direct_clock().unwrap().serving(),
                    replica.handle.direct_leadership(),
                    cluster.nodes[replica.node as usize - 1].executor.stats(),
                );
            }
        }
        if key < 500 {
            assert_eq!(outcome, Answer::Data(Value::Null), "key {key}");
        } else {
            assert_eq!(outcome, Answer::Queued, "key {key}");
            assert!(cluster.settled(right, key, id).await, "marker for {key}");
            queued.push((key, id, body));
        }
        outcomes.push((key, id, Vec::new(), outcome));
    }
    assert!(matches!(
        cluster.propose(right, pause(false)).await,
        ParticipantReply::DirectPaused { .. }
    ));
    // After the barrier: the settled ids stay queued (never applied
    // directly); every acknowledged id replays its one result.
    for (key, id, body) in &queued {
        assert_eq!(
            answer(&gateway, body, *id).await,
            Answer::Queued,
            "key {key}"
        );
    }
    for (key, id, body, outcome) in outcomes.iter().filter(|(_, _, body, _)| !body.is_empty()) {
        assert_eq!(answer(&gateway, body, *id).await, *outcome, "replay {key}");
    }
    // No lost or doubled rows: every acknowledged key has exactly its row in
    // the range that owns it; no queued key has a direct row.
    for (key, _, _, outcome) in &outcomes {
        let group = if *key < 500 { left } else { right };
        let leader = cluster.leader(group).await;
        let (version, row) = cluster.latest(group, leader, *key).await;
        match outcome {
            Answer::Data(_) => {
                assert!(version.is_some());
                assert_eq!(row, Some(json!({"id": key, "value": key})));
            }
            Answer::Queued => assert_eq!(row, None, "key {key}"),
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    eprintln!("d07 final: {:?}", gateway.stats());
    if let Ok(cluster) = Arc::try_unwrap(cluster) {
        cluster.shutdown().await;
    }
}

/// D08: direct and queued writers on the same key with a queued intent held
/// open. Direct requests fall back while the intent exists and are settled
/// at the anchor; a key without the intent keeps committing; the forward
/// marker keeps a later direct attempt of a settled id from applying; the
/// versions follow one serial order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d08_direct_requests_fall_back_while_a_queued_intent_is_open() {
    let mut cluster = Cluster::new().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    for (key, value) in [(1, 10), (2, 20)] {
        let id = Uuid::new_v4();
        let body = cluster.body(
            id,
            "table.insert",
            json!({"row": {"id": key, "value": value}}),
        );
        assert_eq!(answer(&gateway, &body, id).await, Answer::Data(Value::Null));
    }
    let (first, _) = cluster.latest(group, 0, 1).await;
    // A queued transaction prepares key 1 (an intent) and stays open.
    let shared = cluster.nodes[0].shared.clone();
    let txn = TxnId {
        request_id: Uuid::new_v4(),
        attempt: 0,
    };
    let request = RequestBinding {
        run_id: cluster.authority.run_id,
        request_id: txn.request_id,
        digest: Digest::from_bytes([1; 32]),
        accepted_context: Digest::from_bytes([2; 32]),
    };
    let manifest = ManifestRef {
        status_group_id: Uuid::new_v4(),
        digest: Digest::from_bytes([3; 32]),
        participant_count: 1,
    };
    let read_ts = cluster.timestamps.next();
    let write = PreparedWrite {
        key: cluster.row_key(1),
        expected_version: first,
        change: Change::Put(
            CanonicalJson::from_serializable(
                &shared,
                &json!({"id": 1, "value": 15}),
                crate::protocol::MAX_BODY_BYTES,
            )
            .unwrap(),
        ),
    };
    let mut plan = ParticipantPlan {
        group: cluster.groups[group].identity.clone(),
        schema_epoch: 1,
        routing_epoch: 1,
        read_ts,
        write_root: Digest::from_bytes([0; 32]),
        write_count: 1,
        page_count: 1,
    };
    let root = initial_root(txn, &plan).unwrap();
    plan.write_root = page_root(root, 0, std::slice::from_ref(&write)).unwrap();
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: Uuid::new_v4(),
            catalog_group_id: cluster.catalog,
            table_id: cluster.table,
            lifecycle_version: None,
            schema_epoch: 1,
            txn,
            request,
            read_ts,
            authority: cluster.authority,
        },
        lifecycle::GuardMode::Shared,
    )
    .into_sealed();
    let prepared = cluster
        .propose(
            group,
            ParticipantCommand::PreparePage {
                authority: cluster.authority,
                txn,
                request,
                manifest,
                plan: plan.clone(),
                page_index: 0,
                previous_root: root,
                guards: Page::new(vec![guard]).unwrap(),
                writes: Page::new(vec![write]).unwrap(),
            },
        )
        .await;
    assert!(
        matches!(prepared, ParticipantReply::PreparedPage { .. }),
        "{prepared:?}"
    );
    // Concurrent direct writers on the intent key all fall back, are
    // settled at the anchor and go queued; key 2 keeps committing.
    let blocked: Vec<(Uuid, Vec<u8>)> = (0..8)
        .map(|i| {
            let id = Uuid::new_v4();
            let body = cluster.body(
                id,
                "table.update",
                json!({"key": 1, "values": {"value": 100 + i}}),
            );
            (id, body)
        })
        .collect();
    let cluster = Arc::new(cluster);
    let mut tasks = Vec::new();
    for (id, body) in blocked.iter().cloned() {
        let gateway = gateway.clone();
        tasks.push(tokio::spawn(
            async move { answer(&gateway, &body, id).await },
        ));
    }
    let id = Uuid::new_v4();
    let other = cluster.body(
        id,
        "table.update",
        json!({"key": 2, "values": {"value": 21}}),
    );
    assert_eq!(
        answer(&gateway, &other, id).await,
        Answer::Data(json!(true))
    );
    for task in tasks {
        assert_eq!(task.await.unwrap(), Answer::Queued);
    }
    for (id, _) in &blocked {
        assert!(cluster.settled(group, 1, *id).await);
    }
    let stats = gateway.stats();
    assert_eq!(stats.fallbacks, 8, "{stats:?}");
    assert_eq!(stats.forwards, 8, "{stats:?}");
    // A get on the intent key also takes the queued path, unsettled
    // (RV-FINAL M3): no forward, no marker.
    let sent = transport.sent.load(Ordering::Relaxed);
    let get = Uuid::new_v4();
    assert_eq!(
        answer(
            &gateway,
            &cluster.body(get, "table.get", json!({"key": 1})),
            get
        )
        .await,
        Answer::Queued
    );
    assert_eq!(
        transport.sent.load(Ordering::Relaxed),
        sent + 1,
        "one direct attempt, no forward"
    );
    assert_eq!(gateway.stats().forwards, 8);
    assert!(!cluster.settled(group, 1, get).await, "no get marker");
    // The queued transaction commits.
    let queued_ts = cluster.timestamps.next();
    assert!(matches!(
        cluster
            .propose(
                group,
                ParticipantCommand::Seal {
                    authority: cluster.authority,
                    txn,
                    manifest,
                    result_digest: Digest::from_bytes([4; 32]),
                },
            )
            .await,
        ParticipantReply::Sealed(_)
    ));
    let proof = VerifiedStatusDecision::model_for_test(DecisionStatement {
        txn,
        request,
        manifest,
        decided_at: AppliedPosition {
            term: 1,
            node_id: 1,
            index: 1,
        },
        decision: TxnDecision::Commit {
            commit_ts: queued_ts,
        },
    })
    .unwrap()
    .into_sealed();
    assert!(matches!(
        cluster
            .propose(
                group,
                ParticipantCommand::Resolve {
                    authority: cluster.authority,
                    proof,
                    max_items: 128,
                },
            )
            .await,
        ParticipantReply::Resolved { complete: true, .. }
    ));
    let (version, row) = cluster.latest(group, 0, 1).await;
    assert_eq!(version, Some(queued_ts));
    assert_eq!(row, Some(json!({"id": 1, "value": 15})));
    // The settled ids never apply directly, even with the intent gone.
    for (id, body) in &blocked {
        assert_eq!(answer(&gateway, body, *id).await, Answer::Queued);
    }
    assert_eq!(cluster.latest(group, 0, 1).await.0, Some(queued_ts));
    // A new direct writer serializes after the queued commit.
    let id = Uuid::new_v4();
    let later = cluster.body(
        id,
        "table.update",
        json!({"key": 1, "values": {"value": 16}}),
    );
    assert_eq!(
        answer(&gateway, &later, id).await,
        Answer::Data(json!(true))
    );
    let (last, row) = cluster.latest(group, 0, 1).await;
    assert_eq!(row, Some(json!({"id": 1, "value": 16})));
    assert!(
        first < Some(queued_ts) && Some(queued_ts) < last,
        "{first:?} {queued_ts:?} {last:?}"
    );
    if let Ok(cluster) = Arc::try_unwrap(cluster) {
        cluster.shutdown().await;
    }
}

/// The scan reads routing fields without decoding rows, and anything it
/// does not recognize is left to the queued path.
#[test]
fn o03_scan_and_route_keys_follow_the_route_key_column() {
    let table = Uuid::new_v4();
    let body = |op: &str, payload: Value| {
        let mut payload = payload;
        payload["table_id"] = json!(table);
        serde_json::to_vec(
            &json!({"request_id": Uuid::new_v4(), "operation": op, "payload": payload}),
        )
        .unwrap()
    };
    let shared = resources();
    let group = |start: Option<i64>, end: Option<i64>| ShardRoute {
        group: GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("t6").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: table,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: 1,
        },
        span: span(start, end),
        schema_epoch: 1,
        routing_epoch: 1,
    };
    let route = TableRoute {
        table_id: table,
        key_column: "id".into(),
        key_type: ColumnType::Int64,
        schema_epoch: 1,
        ranges: vec![
            Arc::new(RangeRoute::new(group(None, Some(10)))),
            Arc::new(RangeRoute::new(group(Some(10), None))),
        ],
        _charge: shared.reserve_scratch(ByteClass::Progress, 1).unwrap(),
    };
    let keys = |bytes: &[u8]| {
        let scanned = scan(bytes).unwrap();
        route_keys(&scanned, &route).map(|k| (k.first, k.anchor, k.one_range))
    };
    let many = body(
        "table.insert_many",
        json!({"rows": [{"value": 1, "id": 7}, {"id": 3}, {"id": 9, "value": [1, {"id": 0}]}]}),
    );
    assert_eq!(
        keys(&many),
        Some((Key::Int64(7), Key::Int64(3), true)),
        "a nested `id` is not the key"
    );
    let across = body(
        "table.insert_many",
        json!({"rows": [{"id": 12}, {"id": -1}]}),
    );
    assert_eq!(keys(&across), Some((Key::Int64(12), Key::Int64(-1), false)));
    let upsert = body("table.upsert", json!({"row": {"value": 2, "id": 11}}));
    assert_eq!(keys(&upsert), Some((Key::Int64(11), Key::Int64(11), true)));
    // Keys that cannot route: a missing or duplicate key column, the wrong
    // type, a get with values, an update without them.
    for bad in [
        body("table.insert", json!({"row": {"value": 2}})),
        body("table.get", json!({"key": "1"})),
        body("table.get", json!({"key": 1, "values": {}})),
        body("table.update", json!({"key": 1})),
        body(
            "table.insert_many",
            json!({"rows": [{"id": 1}, {"id": 1.5}]}),
        ),
    ] {
        assert!(keys(&bad).is_none(), "{}", String::from_utf8_lossy(&bad));
    }
    let duplicate = format!(
        r#"{{"request_id":"{}","operation":"table.insert","payload":{{"table_id":"{table}","row":{{"id":1,"id":2}}}}}}"#,
        Uuid::new_v4()
    );
    assert!(keys(duplicate.as_bytes()).is_none());
    // Not eligible or not an envelope: no scan.
    for other in [
        body("table.describe", json!({"name": "x"})),
        b"[]".to_vec(),
        br#"{"request_id":"not-a-uuid","operation":"table.get","payload":{}}"#.to_vec(),
    ] {
        assert!(scan(&other).is_none());
    }
    let extra = format!(
        r#"{{"request_id":"{}","operation":"table.get","payload":{{"table_id":"{table}","key":1}},"x":1}}"#,
        Uuid::new_v4()
    );
    assert!(scan(extra.as_bytes()).is_none(), "unknown envelope field");
}

#[test]
fn o03_topology_cache_is_a_bounded_lru() {
    let shared = resources();
    let cache = TopologyCache::new(2);
    let route = |table_id| {
        Arc::new(TableRoute {
            table_id,
            key_column: "id".into(),
            key_type: ColumnType::Int64,
            schema_epoch: 1,
            ranges: Vec::new(),
            _charge: shared.reserve_scratch(ByteClass::Progress, 1).unwrap(),
        })
    };
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    cache.insert(route(a));
    cache.insert(route(b));
    assert!(cache.get(a).is_some(), "a is now the most recent");
    cache.insert(route(c));
    assert_eq!(cache.len(), 2);
    assert!(cache.get(b).is_none(), "b was least recently used");
    assert!(cache.get(a).is_some() && cache.get(c).is_some());
    assert_eq!(mode(false, true), DirectMode::Off);
    assert_eq!(mode(true, false), DirectMode::ForwardOnly);
    assert_eq!(mode(true, true), DirectMode::Direct);
}
