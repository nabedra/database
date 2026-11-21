//! T5 evidence: the leader executor on a real 3-voter Data group over mTLS
//! worker transports (typed Raft, `nabedra-otp/2` sessions, real pools).
//!
//! What is real: Raft replication and elections, the participant machine and
//! its `DirectCommit` apply, the range clock fed by the leadership watcher,
//! leases and ReadIndex. What is modelled: the gateway (T6) is the test
//! calling `DirectExecutor::execute`/`forward` on a replica's handle; the
//! Timestamp authority is one shared in-memory counter (T3 tests the real
//! one); the table schema comes from `StaticSchema`.
use super::*;
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::{
            MvccKey,
            index::{IndexDefinition, IndexValue},
            store::{
                LatestRead, ParticipantCommand, ParticipantHandle, ParticipantMachine,
                ParticipantReply, direct::RangeClock,
            },
        },
        pool::{
            ChargedBytes, GroupStore, OltpPool, PoolIdentity, SharedResourceConfig,
            WorkerSharedResources,
        },
    },
    oltp_replication::{
        authority::{
            config::DataConfig,
            consensus::{Consensus, ConsensusFuture},
            timestamp::{DirectTimestampClient, DirectTimestampRange, DirectTimestampSource},
        },
        hosted::{HostedState, RaftConsensus, batched_config, direct_leadership},
        multiplex::{
            EndpointRegistry, PeerDirectory, Purpose, ResidentGroup, TransportConfig,
            TransportTrust, WorkerTransport,
        },
        service_rpc::direct::{DirectData, DirectRequest, DirectResponse, FallbackReason},
        typed_log::TypedLogStore,
        typed_network::{ReplicaEndpoint, TypedNetworkFactory},
        types::{
            Authority, CommitTs, Digest, GroupIdentity, GroupKind, Page, ReplicaMember, Text,
            WorkerEnrollment, WorkerIdentity,
        },
    },
    protocol::{ColumnType, ErrorCode, Key, Outcome, ProtocolError, TableDescriptor, TableFormat},
};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot};
use uuid::Uuid;

#[path = "../../../tests/support/tls.rs"]
mod tls;

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("t5-direct-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
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

/// One Timestamp authority for the whole fixture: strictly increasing.
struct Counter(AtomicU64);
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

/// `.0`: `data.direct.v1` active; `.1`: the queued-history window (T7c).
struct Gate(AtomicBool, AtomicBool);
impl DirectGate for Gate {
    fn direct_active(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    fn history_settling(&self) -> bool {
        self.1.load(Ordering::Acquire)
    }
}

fn shared() -> Arc<WorkerSharedResources> {
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

struct Voter {
    node: u64,
    consensus: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    handle: Arc<ParticipantHandle>,
    store: GroupStore,
    shared: Arc<WorkerSharedResources>,
    executor: Arc<DirectExecutor>,
    _pool: OltpPool,
    _resident: ResidentGroup,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
    running: bool,
}

struct Cluster {
    voters: Vec<Voter>,
    group: GroupIdentity,
    table: Uuid,
    index: Uuid,
    authority: Authority,
    gate: Arc<Gate>,
    schema: Arc<StaticSchema>,
    _dir: Dir,
}

struct Options {
    /// Nodes that may start elections. Only node 1 by default, so a loaded
    /// machine cannot move leadership in the middle of a measurement.
    electors: Vec<u64>,
    config: DirectConfig,
    span: crate::oltp::mvcc::store::KeySpan,
    /// Activate `data.direct.v1` on the group before the test (T7), so
    /// entry counts see only the requests' own entries. Off, the first
    /// direct request activates it.
    activate: bool,
    /// The Timestamp authority; a shared `Counter` by default.
    timestamps: Option<Arc<dyn DirectTimestampSource>>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            electors: vec![1],
            activate: true,
            timestamps: None,
            config: DirectConfig::default(),
            span: crate::oltp::mvcc::store::KeySpan {
                start: None,
                end: None,
            },
        }
    }
}

fn descriptor(table: Uuid) -> TableDescriptor {
    TableDescriptor {
        id: table,
        name: "items".into(),
        schema: BTreeMap::from([
            ("id".to_string(), ColumnType::Int64),
            ("value".to_string(), ColumnType::Int64),
            ("tag".to_string(), ColumnType::Utf8),
            ("score".to_string(), ColumnType::Float64),
        ]),
        key: "id".into(),
        format: TableFormat::Parquet,
    }
}

impl Cluster {
    async fn new(options: Options) -> Self {
        crate::oltp_runtime::direct::trace_for_test();
        let dir = Dir::new();
        let certificates = tls::certificates(&dir.0, 3);
        let cluster_id = Uuid::new_v4();
        let table = Uuid::new_v4();
        let group = GroupIdentity {
            cluster_id,
            database: Text::try_from("t5").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: table,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: 1,
        };
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
        let members: BTreeMap<u64, ReplicaMember> = workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                let node = index as u64 + 1;
                (
                    node,
                    ReplicaMember {
                        node_id: node,
                        worker_id: worker.worker_id,
                        pool_id: Uuid::new_v4(),
                    },
                )
            })
            .collect();
        let peers = Arc::new(Peers(
            workers
                .iter()
                .map(|worker| (worker.worker_id, worker.clone()))
                .collect(),
        ));
        let gate = Arc::new(Gate(AtomicBool::new(true), AtomicBool::new(false)));
        let schema = Arc::new(StaticSchema::default());
        let index = Uuid::new_v4();
        schema.insert(DirectTable {
            descriptor: descriptor(table),
            schema_epoch: 1,
            // A created table's first lifecycle id is its table id.
            lifecycle_id: table,
            indexes: vec![IndexDefinition {
                index_id: index,
                table_id: table,
                name: Text::try_from("by_value").unwrap(),
                columns: Page::new(vec![Text::try_from("value").unwrap()]).unwrap(),
                covering: Page::new(Vec::new()).unwrap(),
            }],
        });
        let timestamps: Arc<dyn DirectTimestampSource> = options
            .timestamps
            .clone()
            .unwrap_or_else(|| Arc::new(Counter(AtomicU64::new(1_000))));
        let mut voters = Vec::new();
        for (index, ((worker, tls), listener)) in
            workers.iter().zip(&certificates).zip(listeners).enumerate()
        {
            let node = index as u64 + 1;
            let shared = shared();
            let pool = OltpPool::open_development(
                dir.0.join(format!("pool{node}")),
                PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: 2,
                    cluster_id,
                    worker_id: worker.worker_id,
                    pool_id: members[&node].pool_id,
                },
                shared.clone(),
            )
            .unwrap();
            let store = pool.group(group.clone()).await.unwrap();
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
            let (resident, lease) = registry.reserve_replica(group.clone(), node).unwrap();
            let log = TypedLogStore::<DataConfig>::new(store.clone(), node)
                .await
                .unwrap()
                .with_resident(lease.clone())
                .unwrap();
            let machine = ParticipantMachine::new(store.clone()).unwrap();
            let state = HostedState::new(machine.clone(), lease.clone()).unwrap();
            let config = batched_config(openraft::Config {
                enable_elect: options.electors.contains(&node),
                heartbeat_interval: 150,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            })
            .unwrap();
            let network = TypedNetworkFactory::<DataConfig>::new(
                transport.client(),
                members[&node].clone(),
                lease.clone(),
                shared.clone(),
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
                    shared.clone(),
                    members.clone(),
                )
                .unwrap(),
            );
            for purpose in [Purpose::Raft, Purpose::RaftProgress, Purpose::Snapshot] {
                resident.bind(purpose, endpoint.clone()).unwrap();
            }
            let (stop, stopped) = oneshot::channel();
            let task = tokio::spawn(transport.run(listener, async {
                let _ = stopped.await;
            }));
            let dyn_consensus: Arc<dyn Consensus<ParticipantCommand, ParticipantReply>> =
                consensus.clone();
            let handle = Arc::new(ParticipantHandle::new(machine, dyn_consensus));
            // What `ParticipantEndpoint::new` does for every served Data
            // replica: the expiry task visits it (T7b).
            crate::oltp_replication::service_rpc::direct::register_replica(&handle);
            let executor = DirectExecutor::new(
                shared.clone(),
                options.config,
                gate.clone(),
                schema.clone(),
                DirectTimestampClient::new(None, timestamps.clone()),
            );
            voters.push(Voter {
                node,
                consensus,
                handle,
                store,
                shared,
                executor,
                _pool: pool,
                _resident: resident,
                stop: Some(stop),
                task: Some(task),
                running: true,
            });
        }
        voters[0].consensus.raft.initialize(members).await.unwrap();
        let cluster = Self {
            voters,
            group,
            table,
            index,
            authority: Authority {
                run_id: Uuid::new_v4(),
                owner_term: 1,
            },
            gate,
            schema,
            _dir: dir,
        };
        // The first proposals after an election can fail while the new
        // leader settles; the same transition is idempotent, so retry it.
        let (transition, catalog) = (Uuid::new_v4(), Uuid::new_v4());
        watchdog(|| async {
            let leader = cluster.leader().await;
            cluster.voters[leader]
                .handle
                .fence_initial(
                    transition,
                    cluster.authority,
                    catalog,
                    1,
                    1,
                    options.span.clone(),
                )
                .await
                .ok()
        })
        .await;
        let leader = cluster.leader().await;
        cluster.serving(leader).await;
        if options.activate {
            let lifecycle = cluster.table;
            watchdog(|| async {
                let leader = cluster.leader().await;
                cluster.voters[leader]
                    .handle
                    .change_direct_schema(
                        crate::oltp::mvcc::store::direct::DirectSchemaChange::Activate {
                            lifecycle,
                        },
                    )
                    .await
                    .ok()
            })
            .await;
        }
        cluster
    }

    /// The index of the voter that leads and serves direct commits.
    async fn leader(&self) -> usize {
        watchdog(|| async {
            for (index, voter) in self.voters.iter().enumerate() {
                if !voter.running {
                    continue;
                }
                let leader = voter
                    .consensus
                    .raft
                    .metrics()
                    .borrow_watched()
                    .current_leader;
                if leader == Some(voter.node) {
                    return Some(index);
                }
            }
            None
        })
        .await
    }
    async fn serving(&self, index: usize) {
        let clock = self.clock(index);
        watchdog(|| async { clock.leader_token().map(|_| ()) }).await;
    }
    fn clock(&self, index: usize) -> Arc<RangeClock> {
        self.voters[index].handle.direct_clock().unwrap().clone()
    }

    fn body(&self, request_id: Uuid, operation: &str, payload: serde_json::Value) -> Vec<u8> {
        let mut payload = payload;
        payload["table_id"] = serde_json::json!(self.table);
        serde_json::to_vec(&serde_json::json!({
            "request_id": request_id,
            "operation": operation,
            "payload": payload,
        }))
        .unwrap()
    }
    fn request(&self, voter: usize, body: &[u8], request_id: Uuid) -> DirectRequest {
        self.request_at(voter, body, request_id, 1, Duration::from_secs(10))
    }
    fn request_at(
        &self,
        voter: usize,
        body: &[u8],
        request_id: Uuid,
        schema_epoch: u64,
        deadline: Duration,
    ) -> DirectRequest {
        DirectRequest {
            database: self.group.database.clone(),
            table_id: self.table,
            schema_epoch,
            routing_epoch: 1,
            group: self.group.group_id,
            request_id,
            digest: Digest::from_bytes(Sha256::digest(body).into()),
            body: self.voters[voter].shared.copy_bytes(body).unwrap(),
            deadline,
        }
    }
    async fn run(
        &self,
        voter: usize,
        request_id: Uuid,
        operation: &str,
        payload: serde_json::Value,
    ) -> DirectResponse {
        let body = self.body(request_id, operation, payload);
        let request = self.request(voter, &body, request_id);
        self.voters[voter]
            .executor
            .execute(&self.voters[voter].handle, request)
            .await
    }
    async fn op(
        &self,
        voter: usize,
        operation: &str,
        payload: serde_json::Value,
    ) -> DirectResponse {
        self.run(voter, Uuid::new_v4(), operation, payload).await
    }
    fn row_key(&self, id: i64) -> MvccKey {
        MvccKey::Row {
            table_id: self.table,
            primary_key: Key::Int64(id),
        }
    }
    fn index_key(&self, id: i64, value: i64) -> MvccKey {
        MvccKey::IndexEntry {
            table_id: self.table,
            index_id: self.index,
            values: Page::new(vec![IndexValue::Int64(value)]).unwrap(),
            primary_key: Key::Int64(id),
        }
    }
    /// The newest version on `voter` from its local applied state.
    async fn latest(&self, voter: usize, key: MvccKey) -> (Option<CommitTs>, Option<String>) {
        match self.voters[voter].handle.direct_latest(key).await.unwrap() {
            LatestRead::Value { version, value } => (version, value.map(|v| v.get().to_owned())),
            other => panic!("unexpected read {other:?}"),
        }
    }
    /// Waits until every running voter applied the leader's whole log. The
    /// leader's log index covers every entry a returned proposal wrote; its
    /// applied metric can lag the apply that answered the proposal.
    async fn converge(&self, leader: usize) {
        let target = self.last_index(leader);
        for voter in self.voters.iter().filter(|v| v.running) {
            watchdog(|| async {
                let applied = voter
                    .consensus
                    .raft
                    .metrics()
                    .borrow_watched()
                    .last_applied
                    .map(|id| id.index)
                    .unwrap_or(0);
                (applied >= target).then_some(())
            })
            .await;
        }
    }
    fn last_index(&self, voter: usize) -> u64 {
        self.voters[voter]
            .consensus
            .raft
            .metrics()
            .borrow_watched()
            .last_log_index
            .unwrap_or(0)
    }
    /// Stops a voter: its Raft and its transport listener.
    async fn stop(&mut self, voter: usize) {
        let v = &mut self.voters[voter];
        if !v.running {
            return;
        }
        v.running = false;
        v.consensus.shutdown().await.unwrap();
        if let Some(stop) = v.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = v.task.take() {
            let _ = task.await;
        }
    }
    async fn shutdown(mut self) {
        for voter in 0..self.voters.len() {
            self.stop(voter).await;
        }
    }
    /// Every key and value of `prefix` in the group's applied state.
    async fn keys_with(&self, voter: usize, prefix: u8) -> usize {
        let raw = self.voters[voter]
            .store
            .read(
                crate::oltp::pool::TransactionBudget {
                    scratch_bytes: 1 << 20,
                    batch_bytes: 0,
                    output_bytes: 64 << 10,
                },
                move |view| {
                    let mut count = 0u64;
                    let mut cursor = vec![prefix];
                    while let Some(record) = view.first_from(&[prefix], &cursor)? {
                        count += 1;
                        cursor = record.key().to_vec();
                        cursor.push(0);
                    }
                    view.encode(&count)
                },
            )
            .await
            .unwrap();
        serde_json::from_slice::<u64>(raw.as_slice()).unwrap() as usize
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
    .expect("direct fixture watchdog")
}

fn done(json: &str) -> DirectResponse {
    DirectResponse::Done(DirectData::from_json(json).unwrap())
}
fn code(response: &DirectResponse) -> Option<(ErrorCode, Outcome)> {
    match response {
        DirectResponse::Failed(error) => Some((error.code, error.outcome)),
        _ => None,
    }
}

/// O03/D01/D06 (wiring): every eligible point operation on the leader,
/// one Raft entry per writing mutation, the queued path's result shapes,
/// index maintenance, typed codec boundaries and read-back on followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o03_d01_point_operations_commit_one_entry_each_and_match_queued_results() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    assert_eq!(cluster.voters[leader].node, 1);
    // Rule 3 wiring: only the leader serves; followers never do.
    for (index, _) in cluster.voters.iter().enumerate() {
        assert_eq!(
            cluster.clock(index).serving(),
            index == leader,
            "voter {index}"
        );
    }
    let mut timings = Vec::new();
    let mut writes = 0u64;
    let entry = |before: u64, after: u64, wrote: bool| {
        assert_eq!(
            after - before,
            u64::from(wrote),
            "one Data entry per writing mutation"
        );
    };
    let extreme = format!(
        r#"{{"id":{},"score":1.5e+300,"tag":"","value":{}}}"#,
        i64::MIN,
        i64::MAX
    );
    let cases: Vec<(&str, serde_json::Value, &str, bool)> = vec![
        (
            "table.insert",
            serde_json::json!({"row": {"id": 1, "value": 10, "tag": "a", "score": -0.0}}),
            "null",
            true,
        ),
        (
            "table.get",
            serde_json::json!({"key": 1}),
            r#"{"id":1,"score":-0.0,"tag":"a","value":10}"#,
            false,
        ),
        (
            "table.update",
            serde_json::json!({"key": 1, "values": {"value": 11, "tag": "é𝄞"}}),
            "true",
            true,
        ),
        (
            "table.get",
            serde_json::json!({"key": 1}),
            r#"{"id":1,"score":-0.0,"tag":"é𝄞","value":11}"#,
            false,
        ),
        (
            "table.upsert",
            serde_json::json!({"row": {"id": i64::MIN, "value": i64::MAX, "tag": "", "score": 1.5e300}}),
            "null",
            true,
        ),
        (
            "table.get",
            serde_json::json!({"key": i64::MIN}),
            extreme.as_str(),
            false,
        ),
        (
            "table.update",
            serde_json::json!({"key": 99, "values": {"value": 1}}),
            "false",
            false,
        ),
        (
            "table.delete",
            serde_json::json!({"key": 99}),
            "false",
            false,
        ),
        ("table.delete", serde_json::json!({"key": 1}), "true", true),
        ("table.get", serde_json::json!({"key": 1}), "null", false),
        // A tombstone keeps its version; insert over it is allowed.
        (
            "table.insert",
            serde_json::json!({"row": {"id": 1, "value": 12, "tag": "b", "score": 0.0}}),
            "null",
            true,
        ),
        (
            "table.insert_many",
            serde_json::json!({"rows": [
            {"id": 7, "value": 70, "tag": "x", "score": 1.0},
            {"id": -3, "value": -30, "tag": "y", "score": 2.0},
            {"id": i64::MAX, "value": 0, "tag": "z", "score": 3.0}]}),
            "3",
            true,
        ),
        (
            "table.get",
            serde_json::json!({"key": -3}),
            r#"{"id":-3,"score":2.0,"tag":"y","value":-30}"#,
            false,
        ),
    ];
    for (operation, payload, expected, wrote) in cases {
        let before = cluster.last_index(leader);
        let started = std::time::Instant::now();
        let response = cluster.op(leader, operation, payload).await;
        timings.push((operation, started.elapsed()));
        assert_eq!(response, done(expected), "{operation}");
        entry(before, cluster.last_index(leader), wrote);
        writes += u64::from(wrote);
    }
    // Insert on an existing key: a conflict, nothing committed.
    let before = cluster.last_index(leader);
    let conflict = cluster
        .op(
            leader,
            "table.insert",
            serde_json::json!({"row": {"id": 7, "value": 1, "tag": "q", "score": 0.0}}),
        )
        .await;
    assert_eq!(
        code(&conflict),
        Some((ErrorCode::Conflict, Outcome::NotApplied))
    );
    entry(before, cluster.last_index(leader), false);
    // A schema violation fails validation before anything is read.
    let invalid = cluster
        .op(
            leader,
            "table.insert",
            serde_json::json!({"row": {"id": 8, "value": "nan", "tag": "q", "score": 0.0}}),
        )
        .await;
    assert_eq!(code(&invalid).map(|c| c.1), Some(Outcome::NotApplied));
    // Index maintenance: the delete tombstoned (1, 11); the re-insert wrote (1, 12).
    let (_, live) = cluster.latest(leader, cluster.index_key(1, 12)).await;
    assert!(live.is_some(), "new index entry");
    let (tombstone, gone) = cluster.latest(leader, cluster.index_key(1, 11)).await;
    assert!(
        tombstone.is_some() && gone.is_none(),
        "old index entry tombstoned"
    );
    let (old, gone) = cluster.latest(leader, cluster.index_key(1, 10)).await;
    assert!(
        old.is_some() && gone.is_none(),
        "index entry moved by the update"
    );
    // Followers converge on the same rows.
    cluster.converge(leader).await;
    for follower in (0..3).filter(|v| *v != leader) {
        assert_eq!(
            cluster.latest(follower, cluster.row_key(-3)).await,
            cluster.latest(leader, cluster.row_key(-3)).await
        );
    }
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(stats.entries_proposed, writes);
    assert_eq!(stats.entries_applied, writes);
    assert_eq!(stats.batch_sizes["le_1"], writes - 1);
    assert_eq!(stats.batch_sizes["le_8"], 1, "the 3-row insert_many");
    assert_eq!(stats.read_only, 2);
    assert_eq!(stats.overloaded_429, 0);
    let mutations: Vec<_> = timings
        .iter()
        .filter(|(op, _)| *op != "table.get")
        .collect();
    let gets: Vec<_> = timings
        .iter()
        .filter(|(op, _)| *op == "table.get")
        .collect();
    let mean = |items: &[&(&str, Duration)]| {
        items.iter().map(|(_, d)| d.as_micros()).sum::<u128>() / items.len() as u128
    };
    eprintln!(
        "t5 latency (leader side, 3 voters over mTLS, debug build): mutations mean {} us over {} \
         (max {} us), gets mean {} us over {}; executor totals: {} us / {} mutations, {} us / {} gets",
        mean(&mutations),
        mutations.len(),
        mutations.iter().map(|(_, d)| d.as_micros()).max().unwrap(),
        mean(&gets),
        gets.len(),
        stats.mutation_micros_total,
        stats.mutations_done,
        stats.get_micros_total,
        stats.gets_done,
    );
    cluster.shutdown().await;
}

/// D03: a retry with the same id and body replays without a second entry;
/// the same id with another body conflicts on the same key and executes on
/// another key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d03_retries_replay_and_reused_ids_conflict_on_the_same_key() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let id = Uuid::new_v4();
    let row = serde_json::json!({"row": {"id": 5, "value": 50, "tag": "a", "score": 0.0}});
    assert_eq!(
        cluster.run(leader, id, "table.insert", row.clone()).await,
        done("null")
    );
    let before = cluster.last_index(leader);
    // An insert re-executed would conflict; the dedup record replays it.
    assert_eq!(
        cluster.run(leader, id, "table.insert", row.clone()).await,
        done("null")
    );
    assert_eq!(
        cluster.last_index(leader),
        before,
        "a replay proposes nothing"
    );
    let other = serde_json::json!({"key": 5, "values": {"value": 51}});
    let reused = cluster.run(leader, id, "table.update", other).await;
    assert_eq!(
        code(&reused),
        Some((ErrorCode::Conflict, Outcome::NotApplied))
    );
    let elsewhere = serde_json::json!({"row": {"id": 6, "value": 60, "tag": "b", "score": 0.0}});
    assert_eq!(
        cluster.run(leader, id, "table.insert", elsewhere).await,
        done("null")
    );
    let (_, row5) = cluster.latest(leader, cluster.row_key(5)).await;
    assert!(
        row5.unwrap().contains(r#""value":50"#),
        "the conflicting body wrote nothing"
    );
    assert_eq!(
        cluster.keys_with(leader, b'D').await,
        2,
        "one dedup record per anchor"
    );
    assert_eq!(cluster.voters[leader].executor.stats().replayed, 1);
    cluster.shutdown().await;
}

/// D02: the caller vanishes after the leader proposed; a retry of the same id
/// and body through another entry point (a fresh executor on the leader)
/// returns the original result, with one version and one dedup record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_retry_after_a_lost_caller_returns_the_original_result() {
    let cluster = Arc::new(Cluster::new(Options::default()).await);
    let leader = cluster.leader().await;
    let id = Uuid::new_v4();
    let body = cluster.body(
        id,
        "table.insert",
        serde_json::json!({"row": {"id": 9, "value": 90, "tag": "a", "score": 0.0}}),
    );
    let request = cluster.request(leader, &body, id);
    let first = {
        let cluster = cluster.clone();
        tokio::spawn(async move {
            cluster.voters[leader]
                .executor
                .execute(&cluster.voters[leader].handle, request)
                .await
        })
    };
    watchdog(|| async {
        (cluster.voters[leader].executor.stats().entries_proposed == 1).then_some(())
    })
    .await;
    first.abort();
    let _ = first.await;
    // Another gateway's executor on the same leader.
    let other = DirectExecutor::new(
        cluster.voters[leader].shared.clone(),
        DirectConfig::default(),
        cluster.gate.clone(),
        cluster.schema.clone(),
        DirectTimestampClient::new(None, Arc::new(Counter(AtomicU64::new(1 << 40)))),
    );
    let retry = other
        .execute(
            &cluster.voters[leader].handle,
            cluster.request(leader, &body, id),
        )
        .await;
    assert_eq!(retry, done("null"));
    let stats = other.stats();
    assert!(
        stats.replayed + stats.entries_applied == 1,
        "replayed or applied once: {stats:?}"
    );
    assert_eq!(cluster.keys_with(leader, b'D').await, 1);
    // One row version (plus one index entry version).
    let (version, _) = cluster.latest(leader, cluster.row_key(9)).await;
    assert!(version.is_some());
    assert_eq!(
        cluster.keys_with(leader, b'v').await,
        2,
        "row + index entry, once"
    );
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// O08: concurrent same-key mutations admit a serial order: exactly one
/// insert wins, concurrent updates lose nothing and leave exactly one live
/// index entry, and every replica ends identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o08_concurrent_same_key_mutations_admit_a_serial_order() {
    let cluster = Arc::new(Cluster::new(Options::default()).await);
    let leader = cluster.leader().await;
    let mut tasks = Vec::new();
    for task in 0..8 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            cluster
                .op(
                    leader,
                    "table.insert",
                    serde_json::json!({"row": {"id": 1, "value": task, "tag": "t", "score": 0.0}}),
                )
                .await
        }));
    }
    let mut won = 0;
    for task in tasks {
        match task.await.unwrap() {
            DirectResponse::Done(_) => won += 1,
            other => assert_eq!(
                code(&other),
                Some((ErrorCode::Conflict, Outcome::NotApplied))
            ),
        }
    }
    assert_eq!(won, 1, "exactly one insert of the key");
    let mut tasks = Vec::new();
    for task in 0..16i64 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            cluster
                .op(
                    leader,
                    "table.update",
                    serde_json::json!({"key": 1, "values": {"value": 100 + task}}),
                )
                .await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), done("true"));
    }
    let (_, row) = cluster.latest(leader, cluster.row_key(1)).await;
    let row: serde_json::Value = serde_json::from_str(&row.unwrap()).unwrap();
    let last = row["value"].as_i64().unwrap();
    assert!((100..116).contains(&last));
    let mut live = 0;
    for value in (0..8).chain(100..116) {
        if cluster
            .latest(leader, cluster.index_key(1, value))
            .await
            .1
            .is_some()
        {
            live += 1;
            assert_eq!(value, last, "the live index entry matches the row");
        }
    }
    assert_eq!(live, 1, "no stale index entry survived the races");
    let stats = cluster.voters[leader].executor.stats();
    eprintln!(
        "o08 stats: latch waits {}, retries {:?}",
        stats.latch_waits, stats.retries
    );
    assert!(
        stats.latch_waits > 0,
        "same-key requests serialized on the latch"
    );
    cluster.converge(leader).await;
    for follower in (0..3).filter(|v| *v != leader) {
        assert_eq!(
            cluster.latest(follower, cluster.row_key(1)).await,
            cluster.latest(leader, cluster.row_key(1)).await
        );
    }
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// D12/O13: offered load above the in-flight budget gets an explicit 429
/// with `not_applied` before execution; every acknowledged id is present and
/// no refused id wrote anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d12_o13_load_beyond_the_in_flight_budget_is_refused_typed_before_execution() {
    let cluster = Arc::new(
        Cluster::new(Options {
            config: DirectConfig {
                max_in_flight: 4,
                ..DirectConfig::default()
            },
            ..Options::default()
        })
        .await,
    );
    let leader = cluster.leader().await;
    let mut tasks = Vec::new();
    for id in 0..64i64 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let response = cluster
                .op(
                    leader,
                    "table.insert",
                    serde_json::json!({"row": {"id": id, "value": id, "tag": "l", "score": 0.0}}),
                )
                .await;
            (id, response)
        }));
    }
    let (mut acknowledged, mut refused) = (Vec::new(), Vec::new());
    for task in tasks {
        let (id, response) = task.await.unwrap();
        match response {
            DirectResponse::Done(_) => acknowledged.push(id),
            other => {
                assert_eq!(
                    code(&other),
                    Some((ErrorCode::Overloaded, Outcome::NotApplied)),
                    "{other:?}"
                );
                assert_eq!(ErrorCode::Overloaded.status_code(), 429);
                refused.push(id);
            }
        }
    }
    eprintln!(
        "d12: {} acknowledged, {} refused with 429",
        acknowledged.len(),
        refused.len()
    );
    assert!(!refused.is_empty(), "the budget was exceeded");
    assert!(!acknowledged.is_empty());
    for id in &acknowledged {
        assert!(
            cluster
                .latest(leader, cluster.row_key(*id))
                .await
                .1
                .is_some(),
            "acknowledged {id}"
        );
    }
    for id in &refused {
        assert_eq!(
            cluster.latest(leader, cluster.row_key(*id)).await,
            (None, None),
            "refused {id}"
        );
    }
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(stats.overloaded_429 as usize, refused.len());
    assert_eq!(stats.entries_proposed as usize, acknowledged.len());
    assert_eq!(stats.in_flight, 0, "every slot returned");
    // The memory charge refuses a body whose working set exceeds the worker's
    // task budget, without allocating it.
    let admission =
        super::admission::DirectAdmission::new(cluster.voters[leader].shared.clone(), 4);
    assert!(matches!(
        admission.try_admit(usize::MAX / 4),
        Err(DirectResponse::Failed(ProtocolError {
            code: ErrorCode::Overloaded,
            outcome: Outcome::NotApplied,
            ..
        }))
    ));
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// O03/O08: stale epochs and keys outside the span get `StaleRoute` and
/// write nothing; an inactive gate falls back and proposes nothing; a
/// request for another group or a body that differs from its digest fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o03_stale_routes_inactive_gate_and_bad_bindings_write_nothing() {
    let cluster = Cluster::new(Options {
        span: crate::oltp::mvcc::store::KeySpan {
            start: Some(Key::Int64(0)),
            end: Some(Key::Int64(1000)),
        },
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let before = cluster.last_index(leader);
    let id = Uuid::new_v4();
    let body = cluster.body(
        id,
        "table.insert",
        serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
    );
    let stale = cluster.voters[leader]
        .executor
        .execute(
            &cluster.voters[leader].handle,
            cluster.request_at(leader, &body, id, 2, Duration::from_secs(5)),
        )
        .await;
    assert_eq!(
        stale,
        DirectResponse::StaleRoute {
            schema_epoch: Some(1),
            routing_epoch: Some(1)
        }
    );
    for key in [-1i64, 1000, i64::MAX] {
        let outside = cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": key, "value": 1, "tag": "a", "score": 0.0}}),
            )
            .await;
        assert!(
            matches!(outside, DirectResponse::StaleRoute { .. }),
            "{key}: {outside:?}"
        );
        let get = cluster
            .op(leader, "table.get", serde_json::json!({"key": key}))
            .await;
        assert!(
            matches!(get, DirectResponse::StaleRoute { .. }),
            "{key}: {get:?}"
        );
    }
    cluster.gate.0.store(false, Ordering::Release);
    let inactive = cluster
        .op(
            leader,
            "table.insert",
            serde_json::json!({"row": {"id": 2, "value": 1, "tag": "a", "score": 0.0}}),
        )
        .await;
    assert_eq!(
        inactive,
        DirectResponse::Fallback {
            reason: FallbackReason::Inactive
        }
    );
    cluster.gate.0.store(true, Ordering::Release);
    let ineligible = cluster
        .op(
            leader,
            "table.describe",
            serde_json::json!({"name": "items"}),
        )
        .await;
    assert!(matches!(
        ineligible,
        DirectResponse::Fallback {
            reason: FallbackReason::Ineligible
        } | DirectResponse::Failed(_)
    ));
    let mut tampered = cluster.request(leader, &body, id);
    tampered.digest = Digest::from_bytes([7; 32]);
    let tampered = cluster.voters[leader]
        .executor
        .execute(&cluster.voters[leader].handle, tampered)
        .await;
    assert_eq!(
        code(&tampered),
        Some((ErrorCode::ValidationError, Outcome::NotApplied))
    );
    let mut foreign = cluster.request(leader, &body, id);
    foreign.group = Uuid::new_v4();
    let foreign = cluster.voters[leader]
        .executor
        .execute(&cluster.voters[leader].handle, foreign)
        .await;
    assert_eq!(
        code(&foreign),
        Some((ErrorCode::ValidationError, Outcome::NotApplied))
    );
    // A follower answers NotLeader with the leader's node as the hint.
    let follower = (0..3).find(|v| *v != leader).unwrap();
    let redirected = cluster
        .op(follower, "table.get", serde_json::json!({"key": 1}))
        .await;
    assert_eq!(
        redirected,
        DirectResponse::NotLeader {
            hint: Some(cluster.voters[leader].node)
        }
    );
    assert_eq!(cluster.last_index(leader), before, "nothing was proposed");
    assert_eq!(cluster.keys_with(leader, b'v').await, 0);
    cluster.shutdown().await;
}

/// D03 (addendum): `DirectForward` settles the id at its anchor. A marker
/// sends later direct attempts to the queued path; a forward after a direct
/// commit returns the committed result; another body conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d03_forward_markers_settle_request_ids_before_the_queued_path() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let v = &cluster.voters[leader];
    let id = Uuid::new_v4();
    let body = cluster.body(
        id,
        "table.insert",
        serde_json::json!({"row": {"id": 3, "value": 3, "tag": "a", "score": 0.0}}),
    );
    let forwarded = DirectResponse::Fallback {
        reason: FallbackReason::Forwarded,
    };
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &body, id))
            .await,
        forwarded
    );
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &body, id))
            .await,
        forwarded,
        "idempotent"
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &body, id))
            .await,
        forwarded
    );
    assert_eq!(
        cluster.latest(leader, cluster.row_key(3)).await,
        (None, None),
        "never applied directly"
    );
    let committed = Uuid::new_v4();
    let update = cluster.body(
        committed,
        "table.upsert",
        serde_json::json!({"row": {"id": 4, "value": 4, "tag": "b", "score": 0.0}}),
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &update, committed))
            .await,
        done("null")
    );
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &update, committed))
            .await,
        done("null")
    );
    let other = cluster.body(committed, "table.delete", serde_json::json!({"key": 4}));
    let conflict = v
        .executor
        .forward(&v.handle, cluster.request(leader, &other, committed))
        .await;
    assert_eq!(
        code(&conflict),
        Some((ErrorCode::Conflict, Outcome::NotApplied))
    );
    assert_eq!(v.executor.stats().forward_markers_written, 1);
    cluster.shutdown().await;
}

/// T7c, O15/D03: inside the queued-history window a leader executes no
/// mutation whose anchor holds no dedup record: it may be the retry of a
/// queued request reserved before activation without a marker, which only
/// the queued path can answer. It answers `Fallback{History}` and writes
/// nothing. The gateway's forward still settles the id (a marker), and an
/// id that already committed directly still replays its result. After the
/// window the same leader executes again.
///
/// Gets (user decision 2026-11-21, RV-FINAL M3): a read has no exactly-once
/// identity, so the window does not hold it back, and a get forward writes
/// no marker (`Ineligible`: no entry, no dedup or expiry key). The cheap
/// local check stays: a get reusing a mutation's id on its key is 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o15_d03_t7c_history_window_leaves_unknown_ids_to_the_queued_path() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let v = &cluster.voters[leader];
    // Committed directly before the window opens on this leader (a leader
    // whose clock passed the window's end, then one whose clock did not).
    let committed = Uuid::new_v4();
    let upsert = cluster.body(
        committed,
        "table.upsert",
        serde_json::json!({"row": {"id": 4, "value": 4, "tag": "b", "score": 0.0}}),
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &upsert, committed))
            .await,
        done("null")
    );
    let entries = v.executor.stats().entries_proposed;
    cluster.gate.1.store(true, Ordering::Release);

    let history = DirectResponse::Fallback {
        reason: FallbackReason::History,
    };
    let id = Uuid::new_v4();
    let insert = cluster.body(
        id,
        "table.insert",
        serde_json::json!({"row": {"id": 3, "value": 3, "tag": "a", "score": 0.0}}),
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &insert, id))
            .await,
        history
    );
    let update = Uuid::new_v4();
    let update_body = cluster.body(
        update,
        "table.update",
        serde_json::json!({"key": 4, "values": {"value": 40}}),
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &update_body, update))
            .await,
        history,
        "an existing row changes nothing either"
    );
    assert_eq!(
        v.executor.stats().entries_proposed,
        entries,
        "nothing proposed"
    );
    assert_eq!(
        cluster.latest(leader, cluster.row_key(3)).await,
        (None, None)
    );
    // The direct commit still replays inside the window.
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &upsert, committed))
            .await,
        done("null")
    );
    // RV-FINAL M3: a get is a read, also inside the window. A retried
    // pre-boundary get is a new read; nothing is proposed.
    let get = Uuid::new_v4();
    let get_body = cluster.body(get, "table.get", serde_json::json!({"key": 4}));
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &get_body, get))
            .await,
        done(r#"{"id":4,"score":0.0,"tag":"b","value":4}"#)
    );
    assert_eq!(v.executor.stats().entries_proposed, entries);
    // The gateway settles the fallen-back id: a marker, then the queued path.
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &insert, id))
            .await,
        DirectResponse::Fallback {
            reason: FallbackReason::Forwarded
        }
    );
    assert_eq!(v.executor.stats().forward_markers_written, 1);
    // A get forward (an older gateway) writes no marker.
    let (entries, dedup, expiry) = (
        v.executor.stats().entries_proposed,
        cluster.keys_with(leader, b'D').await,
        cluster.keys_with(leader, b'E').await,
    );
    let ineligible = DirectResponse::Fallback {
        reason: FallbackReason::Ineligible,
    };
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &get_body, get))
            .await,
        ineligible
    );
    assert_eq!(v.executor.stats().entries_proposed, entries);
    assert_eq!(v.executor.stats().forward_markers_written, 1);
    assert_eq!(cluster.keys_with(leader, b'D').await, dedup);
    assert_eq!(cluster.keys_with(leader, b'E').await, expiry);
    let stats = serde_json::to_value(v.executor.stats()).unwrap();
    assert_eq!(
        stats["fallbacks"]["history"],
        serde_json::json!(2),
        "{stats}"
    );

    // After the window: the marker keeps `id` on the queued path; a new id
    // executes directly again.
    cluster.gate.1.store(false, Ordering::Release);
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &insert, id))
            .await,
        DirectResponse::Fallback {
            reason: FallbackReason::Forwarded
        }
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &update_body, update))
            .await,
        done("true")
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &get_body, get))
            .await,
        done(r#"{"id":4,"score":0.0,"tag":"b","value":40}"#)
    );
    // A queued read leaves nothing at its key: the same id read directly
    // later is a new read (RV-FINAL M3), and no entry is proposed for it.
    let read = Uuid::new_v4();
    let read_body = cluster.body(read, "table.get", serde_json::json!({"key": 4}));
    let entries = v.executor.stats().entries_proposed;
    assert_eq!(
        v.executor
            .forward(&v.handle, cluster.request(leader, &read_body, read))
            .await,
        ineligible
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(leader, &read_body, read))
            .await,
        done(r#"{"id":4,"score":0.0,"tag":"b","value":40}"#)
    );
    assert_eq!(v.executor.stats().entries_proposed, entries);
    // A get reusing a queued mutation's id on its key (its marker)
    // conflicts, as the queued path's identity would.
    let reuse = cluster.body(id, "table.get", serde_json::json!({"key": 3}));
    assert_eq!(
        code(
            &v.executor
                .execute(&v.handle, cluster.request(leader, &reuse, id))
                .await
        ),
        Some((ErrorCode::Conflict, Outcome::NotApplied))
    );
    // A get reusing a direct mutation's id on its key conflicts, as the
    // queued path's identity would.
    let reuse = cluster.body(committed, "table.get", serde_json::json!({"key": 4}));
    assert_eq!(
        code(
            &v.executor
                .execute(&v.handle, cluster.request(leader, &reuse, committed))
                .await
        ),
        Some((ErrorCode::Conflict, Outcome::NotApplied))
    );
    cluster.shutdown().await;
}

/// D03 expiry: the leader's periodic `DirectExpire` deletes dedup records
/// older than the retention on every replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d03_expiry_deletes_old_dedup_records_on_every_replica() {
    let cluster = Cluster::new(Options {
        config: DirectConfig {
            dedup_retention_ms: 1,
            ..DirectConfig::default()
        },
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    for id in 0..3 {
        cluster
            .op(
                leader,
                "table.upsert",
                serde_json::json!({"row": {"id": id, "value": id, "tag": "e", "score": 0.0}}),
            )
            .await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    // A newer commit raises the group's high-water past the old records.
    cluster
        .op(
            leader,
            "table.upsert",
            serde_json::json!({"row": {"id": 10, "value": 1, "tag": "e", "score": 0.0}}),
        )
        .await;
    assert_eq!(cluster.keys_with(leader, b'D').await, 4);
    // Followers never propose.
    let follower = (0..3).find(|v| *v != leader).unwrap();
    assert_eq!(
        cluster.voters[follower]
            .executor
            .expire(&cluster.voters[follower].handle)
            .await
            .unwrap(),
        0
    );
    let deleted = cluster.voters[leader].executor.expire_served().await;
    assert_eq!(deleted, 3);
    assert_eq!(cluster.keys_with(leader, b'D').await, 1, "leader");
    // Followers apply the same entries; wait for replication, not metrics.
    for voter in 0..3 {
        let cluster = &cluster;
        watchdog(|| async { (cluster.keys_with(voter, b'D').await == 1).then_some(()) }).await;
    }
    assert_eq!(cluster.voters[leader].executor.stats().expired_records, 3);
    cluster.shutdown().await;
}

/// D05: with one follower stopped, writes still succeed within the
/// deadline; with both stopped, no write succeeds and the row is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d05_one_stopped_follower_keeps_writes_and_two_stop_every_success() {
    let mut cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let followers: Vec<usize> = (0..3).filter(|v| *v != leader).collect();
    assert_eq!(
        cluster
            .op(
                leader,
                "table.upsert",
                serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}})
            )
            .await,
        done("null")
    );
    cluster.stop(followers[0]).await;
    for value in 2..6 {
        assert_eq!(
            cluster
                .op(
                    leader,
                    "table.update",
                    serde_json::json!({"key": 1, "values": {"value": value}})
                )
                .await,
            done("true"),
            "one follower down"
        );
    }
    let before = cluster.latest(leader, cluster.row_key(1)).await;
    cluster.stop(followers[1]).await;
    let id = Uuid::new_v4();
    let body = cluster.body(
        id,
        "table.update",
        serde_json::json!({"key": 1, "values": {"value": 99}}),
    );
    let request = cluster.request_at(leader, &body, id, 1, Duration::from_millis(1500));
    let started = std::time::Instant::now();
    let response = cluster.voters[leader]
        .executor
        .execute(&cluster.voters[leader].handle, request)
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "bounded by the deadline"
    );
    assert!(
        !matches!(response, DirectResponse::Done(_)),
        "no success without a quorum: {response:?}"
    );
    if let Some((_, outcome)) = code(&response) {
        // A proposal that may still commit is reported as unknown.
        assert!(matches!(outcome, Outcome::Unknown | Outcome::NotApplied));
    }
    // T7b: a lost quorum shows as proposals that never answered, not as a
    // leadership change.
    let stats = cluster.voters[leader].executor.stats();
    if stats.entries_proposed > 5 {
        assert!(
            stats.proposal_failures["deadline"] + stats.proposal_failures["stalled"] >= 1,
            "{stats:?}"
        );
    }
    assert_eq!(
        cluster.latest(leader, cluster.row_key(1)).await,
        before,
        "row unchanged"
    );
    cluster.shutdown().await;
}

/// D04 and rule 3 under a real election: a write acknowledged by the old
/// leader survives its loss, a retry of the same id on the new leader
/// replays it, a write in flight when the leader dies applies at most once,
/// and the new leader serves only after an entry of its term applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d04_leader_loss_before_and_after_quorum_retries_complete_on_the_new_leader() {
    let mut cluster = Cluster::new(Options {
        electors: vec![1, 2, 3],
        ..Options::default()
    })
    .await;
    let old = cluster.leader().await;
    // After quorum: acknowledged, then the leader dies.
    let acked = Uuid::new_v4();
    let acked_body = cluster.body(
        acked,
        "table.insert",
        serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
    );
    let v = &cluster.voters[old];
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(old, &acked_body, acked))
            .await,
        done("null")
    );
    // In flight: proposed, then the leader dies before its reply.
    let inflight = Uuid::new_v4();
    let inflight_body = cluster.body(
        inflight,
        "table.insert",
        serde_json::json!({"row": {"id": 2, "value": 2, "tag": "b", "score": 0.0}}),
    );
    let request = cluster.request(old, &inflight_body, inflight);
    let executor = cluster.voters[old].executor.clone();
    let handle = cluster.voters[old].handle.clone();
    let pending = tokio::spawn(async move { executor.execute(&handle, request).await });
    watchdog(|| async {
        (cluster.voters[old].executor.stats().entries_proposed >= 2).then_some(())
    })
    .await;
    let old_clock = cluster.clock(old);
    cluster.stop(old).await;
    assert!(!old_clock.serving(), "the stopped leader stepped down");
    let first = pending.await.unwrap();
    assert!(
        !matches!(&first, DirectResponse::Done(_)) || first == done("null"),
        "{first:?}"
    );
    let new = cluster.leader().await;
    assert_ne!(new, old);
    cluster.serving(new).await;
    let v = &cluster.voters[new];
    // Retries of both ids on the new leader.
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(new, &acked_body, acked))
            .await,
        done("null")
    );
    assert_eq!(
        v.executor
            .execute(&v.handle, cluster.request(new, &inflight_body, inflight))
            .await,
        done("null")
    );
    let stats = v.executor.stats();
    assert!(
        stats.replayed >= 1,
        "the acknowledged write replayed: {stats:?}"
    );
    for key in [1, 2] {
        assert!(
            cluster.latest(new, cluster.row_key(key)).await.1.is_some(),
            "row {key}"
        );
    }
    // Each id applied exactly once: one dedup record and one row version each
    // (plus one index entry version each).
    assert_eq!(cluster.keys_with(new, b'D').await, 2);
    assert_eq!(cluster.keys_with(new, b'v').await, 4);
    cluster.shutdown().await;
}

/// RV-FINAL H1, D07, O16, NS11: the leader's core stops on a failed apply
/// (a fail-once test hook) and the supervisor's restart runs a new core on
/// the same log and state. The leadership watcher follows the new core: the
/// clock steps down while the core is stopped, the restarted node wins the
/// next election, its clock serves again and a direct update commits. Before
/// the fix the watcher ended with the first core and `serving` stayed false.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d07_o16_rv_final_h1_restarted_core_serves_direct_commits_again() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
            )
            .await,
        done("null")
    );
    let v = &cluster.voters[leader];
    let clock = cluster.clock(leader);
    v.consensus.retry().fail_applies.store(1, Ordering::Release);
    // The next applied entry stops the core. Its outcome is not asserted.
    let executor = v.executor.clone();
    let handle = v.handle.clone();
    let id = Uuid::new_v4();
    let body = cluster.body(
        id,
        "table.update",
        serde_json::json!({"key": 1, "values": {"value": 2}}),
    );
    let request = cluster.request_at(leader, &body, id, 1, Duration::from_secs(3));
    let pending = tokio::spawn(async move { executor.execute(&handle, request).await });
    watchdog(|| async { (!v.consensus.raft.is_running()).then_some(()) }).await;
    watchdog(|| async { (!clock.serving()).then_some(()) }).await;
    // What the supervisor does after its backoff.
    v.consensus.restart().await.unwrap();
    assert_eq!(v.consensus.raft.restarts(), 1);
    let _ = pending.await.unwrap();
    // Only node 1 elects, so the restarted node leads again.
    assert_eq!(cluster.leader().await, leader);
    cluster.serving(leader).await;
    assert!(clock.serving(), "the clock serves on the restarted core");
    let response = cluster
        .op(
            leader,
            "table.update",
            serde_json::json!({"key": 1, "values": {"value": 3}}),
        )
        .await;
    assert_eq!(response, done("true"));
    assert_eq!(
        cluster
            .op(leader, "table.get", serde_json::json!({"key": 1}))
            .await,
        done(r#"{"id":1,"score":0.0,"tag":"a","value":3}"#)
    );
    cluster.shutdown().await;
}

/// RV-FINAL M1, U01, F01/F03: a leader whose device is full refuses the
/// proposal before Raft sees it. The direct update answers that refusal at
/// once (`storage_unavailable`, `not_applied`, the no-space message), as the
/// queued path does, after one proposal and one timestamp fetch; before the
/// fix it looped until its deadline and answered `outcome_unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f01_u01_rv_final_m1_full_leader_answers_not_applied_at_once() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
            )
            .await,
        done("null")
    );
    let v = &cluster.voters[leader];
    let before = v.executor.stats();
    let refused = v.consensus.health().snapshot().proposals_refused_no_space;
    v.consensus.retry().mark_no_space();
    let started = std::time::Instant::now();
    let response = cluster
        .op(
            leader,
            "table.update",
            serde_json::json!({"key": 1, "values": {"value": 2}}),
        )
        .await;
    let elapsed = started.elapsed();
    let DirectResponse::Failed(error) = &response else {
        panic!("{response:?}");
    };
    assert_eq!(
        (error.code, error.outcome),
        (ErrorCode::StorageUnavailable, Outcome::NotApplied),
        "{error:?}"
    );
    assert!(crate::oltp::pool::is_no_space(error), "{error:?}");
    // The request deadline is 10 s; the refusal comes back without waiting.
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    let after = v.executor.stats();
    assert_eq!(
        after.entries_proposed - before.entries_proposed,
        1,
        "{after:?}"
    );
    assert_eq!(after.outcome_unknown, before.outcome_unknown, "{after:?}");
    assert_eq!(
        v.consensus.health().snapshot().proposals_refused_no_space - refused,
        1
    );
    // Nothing applied: the row keeps its value (a get proposes nothing).
    assert_eq!(
        cluster
            .op(leader, "table.get", serde_json::json!({"key": 1}))
            .await,
        done(r#"{"id":1,"score":0.0,"tag":"a","value":1}"#)
    );
    cluster.shutdown().await;
}

/// RV-FINAL L3, C07, F03: a Catalog leader that stops answering table
/// reads holds neither a direct mutation nor a forward past the request
/// deadline. Both answer `not_applied` (nothing was proposed) instead of
/// waiting until the gateway's own deadline cuts the call as uncertain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c07_f03_rv_final_l3_catalog_reads_end_at_the_request_deadline() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let v = &cluster.voters[leader];
    let proposed = v.executor.stats().entries_proposed;
    cluster.schema.stall_table_reads(true);
    for forward in [false, true] {
        let id = Uuid::new_v4();
        let body = cluster.body(
            id,
            "table.upsert",
            serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
        );
        let request = cluster.request_at(leader, &body, id, 1, Duration::from_millis(500));
        let started = std::time::Instant::now();
        let response = tokio::time::timeout(Duration::from_secs(5), async {
            if forward {
                v.executor.forward(&v.handle, request).await
            } else {
                v.executor.execute(&v.handle, request).await
            }
        })
        .await
        .expect("a stalled Catalog read outlived the request deadline");
        assert_eq!(
            code(&response),
            Some((ErrorCode::StorageUnavailable, Outcome::NotApplied)),
            "forward={forward}: {response:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    cluster.schema.stall_table_reads(false);
    assert_eq!(
        v.executor.stats().entries_proposed,
        proposed,
        "nothing was proposed"
    );
    cluster.shutdown().await;
}

/// Rule 2 wiring for `table.get` (plan section 6): a get of a key with a
/// pending commit waits for it, and a get never waits on other keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d06_get_waits_for_a_pending_commit_on_its_key_only() {
    let cluster = Arc::new(Cluster::new(Options::default()).await);
    let leader = cluster.leader().await;
    cluster
        .op(
            leader,
            "table.upsert",
            serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
        )
        .await;
    let clock = cluster.clock(leader);
    let token = clock.leader_token().unwrap();
    let pending = CommitTs::new(u64::MAX - 10).unwrap();
    clock
        .register_pending(token, pending, vec![cluster.row_key(1)])
        .unwrap();
    let other = tokio::time::timeout(
        Duration::from_secs(2),
        cluster.op(leader, "table.get", serde_json::json!({"key": 2})),
    )
    .await
    .expect("another key does not wait");
    assert_eq!(other, done("null"));
    let waiting = {
        let cluster = cluster.clone();
        tokio::spawn(async move {
            cluster
                .op(leader, "table.get", serde_json::json!({"key": 1}))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "the get waits for the pending commit on its key"
    );
    clock.settle(pending);
    assert_eq!(
        waiting.await.unwrap(),
        done(r#"{"id":1,"score":0.0,"tag":"a","value":1}"#)
    );
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

#[test]
fn o02_received_request_takes_its_group_and_database_from_the_session() {
    let table = Uuid::new_v4();
    let group = GroupIdentity {
        cluster_id: Uuid::new_v4(),
        database: Text::try_from("db").unwrap(),
        group_id: Uuid::new_v4(),
        kind: GroupKind::Data {
            table_id: table,
            shard_id: Uuid::new_v4(),
        },
        group_format_version: 1,
    };
    let shared = shared();
    let body: ChargedBytes = shared.copy_bytes(b"{}").unwrap();
    let header = crate::oltp_replication::service_rpc::direct::DirectHeader {
        table_id: table,
        schema_epoch: 1,
        routing_epoch: 1,
        request_id: Uuid::new_v4(),
        digest: Digest::from_bytes([1; 32]),
        deadline_ms: u32::MAX,
    };
    let request = DirectRequest::received(&group, header, body.clone()).unwrap();
    assert_eq!(request.group, group.group_id);
    assert_eq!(request.database, group.database);
    assert_eq!(
        request.deadline,
        crate::oltp_replication::service_rpc::direct::MAX_DIRECT_DEADLINE,
        "clamped"
    );
    assert_eq!(request.header().request_id, header.request_id);
    let wrong_table = crate::oltp_replication::service_rpc::direct::DirectHeader {
        table_id: Uuid::new_v4(),
        ..header
    };
    assert!(DirectRequest::received(&group, wrong_table, body.clone()).is_err());
    let no_deadline = crate::oltp_replication::service_rpc::direct::DirectHeader {
        deadline_ms: 0,
        ..header
    };
    assert!(DirectRequest::received(&group, no_deadline, body).is_err());
}

/// T7, O12 / D11 on three voters: the leader activates the group on its
/// first direct request; a DDL fence (index publication) then sends every
/// request to the queued path until the leader's Catalog serves the
/// published descriptor, and the next direct commits carry the new epoch.
/// A drop fence stops direct writes on the table for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o12_d11_ddl_fences_stop_direct_writers_until_the_descriptor_is_published() {
    use crate::oltp::mvcc::store::direct::{DirectSchema as GroupSchema, DirectSchemaChange};
    let cluster = Cluster::new(Options {
        activate: false,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let handle = cluster.voters[leader].handle.clone();
    assert_eq!(
        handle.direct_schema().await.unwrap(),
        None,
        "not active yet"
    );
    let row =
        |id: i64| serde_json::json!({"row": {"id": id, "value": id, "tag": "a", "score": 0.0}});
    assert_eq!(
        cluster.op(leader, "table.insert", row(1)).await,
        done("null")
    );
    assert_eq!(
        handle.direct_schema().await.unwrap(),
        Some(GroupSchema {
            epoch: 1,
            lifecycle: Some(cluster.table)
        }),
        "the first request activated the group at the table's lifecycle"
    );
    // The DDL coordinator fences before the Catalog publishes `published`.
    let published = Uuid::new_v4();
    assert_eq!(
        handle
            .change_direct_schema(DirectSchemaChange::Fence {
                lifecycle: Some(published)
            })
            .await
            .unwrap(),
        GroupSchema {
            epoch: 2,
            lifecycle: Some(published)
        }
    );
    let before = cluster.last_index(leader);
    let blocked = cluster.op(leader, "table.insert", row(2)).await;
    assert!(
        matches!(blocked, DirectResponse::Fallback { .. }),
        "an unpublished descriptor goes queued: {blocked:?}"
    );
    assert_eq!(cluster.last_index(leader), before, "nothing proposed");
    assert_eq!(cluster.latest(leader, cluster.row_key(2)).await.0, None);
    // The Catalog publishes: the leader's schema now has that lifecycle.
    cluster.schema.insert(DirectTable {
        descriptor: descriptor(cluster.table),
        schema_epoch: 1,
        lifecycle_id: published,
        indexes: Vec::new(),
    });
    assert_eq!(
        cluster.op(leader, "table.insert", row(2)).await,
        done("null")
    );
    assert!(cluster.latest(leader, cluster.row_key(2)).await.0.is_some());
    // A drop fence: no direct write applies to the table again.
    handle
        .change_direct_schema(DirectSchemaChange::Fence { lifecycle: None })
        .await
        .unwrap();
    let dropped = cluster.op(leader, "table.insert", row(3)).await;
    assert_eq!(
        code(&dropped),
        Some((ErrorCode::NotFound, Outcome::NotApplied)),
        "{dropped:?}"
    );
    assert_eq!(cluster.latest(leader, cluster.row_key(3)).await.0, None);
    cluster.shutdown().await;
}

/// T7b item 2 (T7 concern 1): a DDL that read `data.direct.v1` as inactive
/// skips its fence. While it holds the table's exclusive lifecycle guard,
/// no first request may bind the pre-DDL lifecycle to the group: a mutation
/// falls back with nothing proposed, a forward waits. Once the DDL publishes
/// its descriptor (here: a new index) and releases the guard, the group is
/// activated at the new lifecycle and the leader writes with the new
/// descriptor, index entry included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o12_d11_t7b_activation_waits_for_a_ddl_that_read_the_capability_inactive() {
    let cluster = Cluster::new(Options {
        activate: false,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let v = &cluster.voters[leader];
    // The pre-DDL table: no index.
    cluster.schema.insert(DirectTable {
        descriptor: descriptor(cluster.table),
        schema_epoch: 1,
        lifecycle_id: cluster.table,
        indexes: Vec::new(),
    });
    // The DDL holds the guard; its probe saw the capability inactive, so it
    // committed no fence.
    cluster.schema.hold_exclusive(cluster.table, true);
    let row =
        |id: i64| serde_json::json!({"row": {"id": id, "value": id, "tag": "a", "score": 0.0}});
    let before = cluster.last_index(leader);
    let blocked = cluster.op(leader, "table.insert", row(1)).await;
    assert_eq!(
        blocked,
        DirectResponse::Fallback {
            reason: FallbackReason::Schema
        }
    );
    assert_eq!(
        v.handle.direct_schema().await.unwrap(),
        None,
        "not activated"
    );
    assert_eq!(cluster.last_index(leader), before, "nothing proposed");
    // A forward waits for the guard: its marker needs an active group.
    let id = Uuid::new_v4();
    let body = cluster.body(id, "table.insert", row(2));
    let forward = {
        let executor = v.executor.clone();
        let handle = v.handle.clone();
        let request = cluster.request(leader, &body, id);
        tokio::spawn(async move { executor.forward(&handle, request).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!forward.is_finished(), "the forward waits for the DDL");
    assert_eq!(v.handle.direct_schema().await.unwrap(), None);
    // The DDL publishes a new lifecycle with an index, then releases.
    let published = Uuid::new_v4();
    cluster.schema.insert(DirectTable {
        descriptor: descriptor(cluster.table),
        schema_epoch: 1,
        lifecycle_id: published,
        indexes: vec![IndexDefinition {
            index_id: cluster.index,
            table_id: cluster.table,
            name: Text::try_from("by_value").unwrap(),
            columns: Page::new(vec![Text::try_from("value").unwrap()]).unwrap(),
            covering: Page::new(Vec::new()).unwrap(),
        }],
    });
    cluster.schema.hold_exclusive(cluster.table, false);
    assert_eq!(
        forward.await.unwrap(),
        DirectResponse::Fallback {
            reason: FallbackReason::Forwarded
        },
        "the marker lands once the group is active"
    );
    assert_eq!(
        v.handle.direct_schema().await.unwrap(),
        Some(crate::oltp::mvcc::store::direct::DirectSchema {
            epoch: 1,
            lifecycle: Some(published)
        }),
        "bound to the published lifecycle, never the pre-DDL one"
    );
    assert_eq!(
        cluster.op(leader, "table.insert", row(3)).await,
        done("null")
    );
    assert!(
        cluster
            .latest(leader, cluster.index_key(3, 3))
            .await
            .0
            .is_some(),
        "the leader wrote with the published descriptor"
    );
    cluster.shutdown().await;
}

/// T7b item 6 (T6 concern 5): a group activation that fails transiently
/// (an unreachable Catalog) is retried inside the executor within the
/// deadline; the request commits instead of answering
/// `storage_unavailable` with an unknown outcome. Activation that cannot
/// finish before the deadline answers `not_applied`: the request itself was
/// never proposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d04_t7b_activation_failures_are_retried_inside_the_executor() {
    let cluster = Cluster::new(Options {
        activate: false,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    cluster.schema.fail_lifecycle_reads(5);
    let row =
        |id: i64| serde_json::json!({"row": {"id": id, "value": id, "tag": "a", "score": 0.0}});
    assert_eq!(
        cluster.op(leader, "table.insert", row(1)).await,
        done("null")
    );
    let stats = cluster.voters[leader].executor.stats();
    assert!(stats.retries["unavailable"] >= 5, "{stats:?}");
    assert_eq!(stats.outcome_unknown, 0, "{stats:?}");
    cluster.shutdown().await;
    // A fresh group: both followers stop before a never-activated request;
    // activation cannot reach quorum before the deadline.
    let mut cluster = Cluster::new(Options {
        activate: false,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    for follower in (0..3).filter(|v| *v != leader).collect::<Vec<_>>() {
        cluster.stop(follower).await;
    }
    let id = Uuid::new_v4();
    let body = cluster.body(id, "table.insert", row(2));
    let request = cluster.request_at(leader, &body, id, 1, Duration::from_millis(1500));
    let started = std::time::Instant::now();
    let answer = cluster.voters[leader]
        .executor
        .execute(&cluster.voters[leader].handle, request)
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "bounded by the deadline"
    );
    assert_eq!(
        code(&answer),
        Some((ErrorCode::StorageUnavailable, Outcome::NotApplied)),
        "{answer:?}"
    );
    let stats = cluster.voters[leader].executor.stats();
    assert!(stats.proposal_failures["activation"] >= 1, "{stats:?}");
    assert_eq!(stats.entries_proposed, 0, "the request was never proposed");
    cluster.shutdown().await;
}

/// T7b item 7 (T5 concern 3): after a leader change, the new leader's
/// expiry pass deletes old dedup records on a range it leads although it
/// never served a direct request there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d03_t7b_a_new_leader_expires_records_on_ranges_it_never_served() {
    let cluster = Cluster::new(Options {
        electors: vec![1, 2, 3],
        config: DirectConfig {
            dedup_retention_ms: 1,
            ..DirectConfig::default()
        },
        ..Options::default()
    })
    .await;
    let old = cluster.leader().await;
    for id in 0..3 {
        assert_eq!(
            cluster
                .op(
                    old,
                    "table.upsert",
                    serde_json::json!({"row": {"id": id, "value": id, "tag": "e", "score": 0.0}}),
                )
                .await,
            done("null")
        );
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        cluster
            .op(
                old,
                "table.upsert",
                serde_json::json!({"row": {"id": 10, "value": 1, "tag": "e", "score": 0.0}}),
            )
            .await,
        done("null")
    );
    cluster.converge(old).await;
    // Leadership moves to a node whose executor never served this range.
    let next = (old + 1) % 3;
    let new = watchdog(|| async {
        let _ = cluster.voters[next]
            .consensus
            .raft
            .trigger()
            .elect(false)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let leader = cluster.leader().await;
        (leader != old).then_some(leader)
    })
    .await;
    cluster.serving(new).await;
    let executor = &cluster.voters[new].executor;
    assert_eq!(executor.stats().requests, 0, "never served a request");
    let deleted = watchdog(|| async {
        let deleted = executor.expire_served().await;
        (deleted > 0).then_some(deleted)
    })
    .await;
    assert_eq!(deleted, 3);
    for voter in 0..3 {
        let cluster = &cluster;
        watchdog(|| async { (cluster.keys_with(voter, b'D').await == 1).then_some(()) }).await;
    }
    // The old leader, now a follower, proposes nothing.
    assert_eq!(cluster.voters[old].executor.expire_served().await, 0);
    cluster.shutdown().await;
}

/// A Timestamp authority whose answers a test controls: `hold` keeps every
/// allocation pending until it is cleared, `stale` answers the next one
/// with a value below every committed version.
struct Steered {
    next: AtomicU64,
    calls: AtomicU64,
    answered: AtomicU64,
    hold: AtomicBool,
    stale: AtomicBool,
}
impl DirectTimestampSource for Steered {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            while self.hold.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let first = if self.stale.swap(false, Ordering::SeqCst) {
                1
            } else {
                self.next.fetch_add(count, Ordering::SeqCst)
            };
            self.answered.fetch_add(1, Ordering::SeqCst);
            DirectTimestampRange::new(
                CommitTs::new(first).unwrap(),
                CommitTs::new(first + count - 1).unwrap(),
            )
        })
    }
}

/// D01 (PERF2b items 1 and 2): the leader's barrier check, epochs, dedup
/// lookup and newest version come from one Query-class native job, and a
/// small entry's apply takes one job instead of four. Before, a get took
/// four jobs (two Control, two Query).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d01_perf2b_leader_reads_share_one_native_job() {
    use crate::oltp::pool::ExecutionClass;
    const ROUNDS: u64 = 20;
    // Background work on an idle Data leader (none is expected).
    const SLACK: u64 = 4;
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 10, "tag": "a", "score": 2.0}}),
            )
            .await,
        done("null")
    );
    cluster.converge(leader).await;
    let shared = cluster.voters[leader].shared.clone();
    let jobs = || {
        (
            shared.native_jobs(ExecutionClass::Query),
            shared.native_jobs(ExecutionClass::Control),
        )
    };
    let (query, control) = jobs();
    for _ in 0..ROUNDS {
        assert_eq!(
            cluster
                .op(leader, "table.get", serde_json::json!({"key": 1}))
                .await,
            done(r#"{"id":1,"score":2.0,"tag":"a","value":10}"#)
        );
    }
    let (query_after, control_after) = jobs();
    assert!(
        query_after - query <= ROUNDS + SLACK,
        "Query jobs for {ROUNDS} gets: {}",
        query_after - query
    );
    assert!(
        control_after - control <= SLACK,
        "Control jobs for {ROUNDS} gets: {}",
        control_after - control
    );
    eprintln!(
        "perf2b: {ROUNDS} gets took {} Query and {} Control jobs",
        query_after - query,
        control_after - control
    );
    let (query, control) = jobs();
    for round in 0..ROUNDS {
        assert_eq!(
            cluster
                .op(
                    leader,
                    "table.update",
                    serde_json::json!({"key": 1, "values": {"value": round}}),
                )
                .await,
            done("true")
        );
    }
    let (query_after, control_after) = jobs();
    eprintln!(
        "perf2b: {ROUNDS} updates took {} Query and {} Control jobs",
        query_after - query,
        control_after - control
    );
    // These counts include replication (AppendEntries encodes and replies),
    // index reads and the append, so only the change is asserted. Measured
    // on this fixture: base 439 Query and 139 Control jobs for 20 updates
    // (about 22 and 7 each), PERF2b 360 and 102 (18 and 5). Per update, the
    // reads lose the dedup and latest jobs and the epochs and barrier
    // applied reads, and gain the snapshot; the apply loses its digest,
    // reply budget and reply decode jobs.
    assert!(
        query_after - query <= 20 * ROUNDS + SLACK,
        "Query jobs for {ROUNDS} updates: {}",
        query_after - query
    );
    assert!(
        control_after - control <= 6 * ROUNDS + SLACK,
        "Control jobs for {ROUNDS} updates: {}",
        control_after - control
    );
    cluster.shutdown().await;
}

/// O09 (PERF2b item 4): the commit timestamp is fetched while the leader
/// reads, after the token and the latches. A read-only outcome drops the
/// fetch, even one that never answers, and commits nothing. A timestamp
/// that is not above a version the attempt read is replaced before the
/// proposal, so apply never sees it and the commit is still one entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o09_perf2b_the_commit_timestamp_is_fetched_during_the_reads() {
    let source = Arc::new(Steered {
        next: AtomicU64::new(1_000),
        calls: AtomicU64::new(0),
        answered: AtomicU64::new(0),
        hold: AtomicBool::new(false),
        stale: AtomicBool::new(false),
    });
    let cluster = Cluster::new(Options {
        timestamps: Some(source.clone()),
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 10, "tag": "a", "score": 2.0}}),
            )
            .await,
        done("null")
    );
    // Read-only: the fetch started (one call) and was dropped unanswered.
    source.hold.store(true, Ordering::SeqCst);
    let calls = source.calls.load(Ordering::SeqCst);
    let before = cluster.last_index(leader);
    let missing = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.op(
            leader,
            "table.update",
            serde_json::json!({"key": 99, "values": {"value": 1}}),
        ),
    )
    .await
    .expect("a read-only outcome does not wait for its timestamp");
    assert_eq!(missing, done("false"));
    assert_eq!(source.calls.load(Ordering::SeqCst), calls + 1);
    assert_eq!(cluster.last_index(leader), before, "nothing committed");
    // The held call answers nobody once released; later calls follow it.
    source.hold.store(false, Ordering::SeqCst);
    watchdog(|| async {
        (source.answered.load(Ordering::SeqCst) == source.calls.load(Ordering::SeqCst))
            .then_some(())
    })
    .await;
    let (version, _) = cluster.latest(leader, cluster.row_key(1)).await;
    let version = version.expect("the inserted row");
    source.stale.store(true, Ordering::SeqCst);
    let calls = source.calls.load(Ordering::SeqCst);
    let before = cluster.last_index(leader);
    let updated = cluster
        .op(
            leader,
            "table.update",
            serde_json::json!({"key": 1, "values": {"value": 11}}),
        )
        .await;
    assert_eq!(updated, done("true"));
    assert!(
        source.calls.load(Ordering::SeqCst) >= calls + 2,
        "the stale value was replaced by a second fetch"
    );
    assert_eq!(
        cluster.last_index(leader),
        before + 1,
        "one entry: apply never saw the stale timestamp"
    );
    let (newer, value) = cluster.latest(leader, cluster.row_key(1)).await;
    assert!(newer.expect("updated row") > version);
    assert!(value.unwrap().contains(r#""value":11"#));
    cluster.shutdown().await;
}
