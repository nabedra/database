#![cfg(feature = "oltp")]
//! O06/O10/O11/O15: real mutually authenticated TCP peers and durable stores.
//! These are library consensus fixtures, not distributed API/worker acceptance.
use futures_util::StreamExt;
use nabedra_database_server::{
    oltp::{OltpEngine, OltpResources, StateMachine},
    oltp_replication::{
        NetworkFactory, Peer, ReplicatedCommand, Server, TlsMaterial, TypeConfig,
        validate_membership,
    },
    protocol::*,
};
use openraft::{
    Raft, RaftLogReader,
    entry::RaftEntry,
    storage::{IOFlushed, RaftLogStorage},
    type_config::alias::{EntryOf, LogIdOf, VoteOf},
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{RwLock, Semaphore, oneshot},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-raft-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn peer(node: u64, cluster: Uuid, group: Uuid) -> Peer {
    Peer {
        node_id: node,
        engine: "oltp".into(),
        format_version: 2,
        database: "default".into(),
        cluster_id: cluster,
        group_id: group,
        incarnation: Uuid::new_v4(),
        address: "127.0.0.1:1".into(),
        server_name: "localhost".into(),
        certificate_sha256: format!("{node:064x}"),
    }
}
fn resources() -> OltpResources {
    OltpResources {
        cache_bytes: 1024 * 1024,
        write_buffer_bytes: 1024 * 1024,
        // Match the CLI task budget, including decoded input and mutation buffers.
        query_memory_bytes: 512 * 1024 * 1024,
        executor_threads: 1,
        background_threads: 1,
    }
}
fn command(
    operation: Operation,
    run_id: Uuid,
    term: u64,
    table: Option<Uuid>,
) -> ReplicatedCommand {
    let request = Request {
        request_id: Uuid::new_v4(),
        operation,
    };
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(serde_json::to_vec(&request).unwrap())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    ReplicatedCommand::Operation {
        request,
        digest,
        run_id,
        term,
        create_table_id: table,
    }
}
fn query(table_id: Uuid) -> Operation {
    Operation::Count(QueryPayload {
        table_id,
        query: Query {
            select: None,
            filter: None,
            order_by: None,
            limit: None,
        },
    })
}

#[test]
fn o02_o06_production_membership_rejects_foreign_identity_and_duplicate_certificates() {
    let local = peer(1, Uuid::new_v4(), Uuid::new_v4());
    let remote = peer(2, local.cluster_id, local.group_id);
    let valid = BTreeMap::from([(1, local.clone()), (2, remote.clone())]);
    validate_membership(&local, &valid).unwrap();
    for mutation in 0..9 {
        let mut peers = valid.clone();
        let bad = peers.get_mut(&2).unwrap();
        match mutation {
            0 => bad.engine = "olap".into(),
            1 => bad.format_version = 3,
            2 => bad.cluster_id = Uuid::new_v4(),
            3 => bad.group_id = Uuid::new_v4(),
            4 => bad.incarnation = Uuid::nil(),
            5 => bad.node_id = 3,
            6 => bad.certificate_sha256 = local.certificate_sha256.clone(),
            7 => bad.incarnation = local.incarnation,
            _ => bad.database = "other".into(),
        };
        assert!(
            validate_membership(&local, &peers).is_err(),
            "mutation {mutation}"
        );
    }
    assert!(validate_membership(&local, &BTreeMap::new()).is_err());
    assert!(validate_membership(&Peer::default(), &valid).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o11_wide_log_append_is_durable_and_streamed_with_bounded_reads() {
    timeout(Duration::from_secs(60), async {
        let directory = Temporary::new();
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let engine = OltpEngine::open_replica(&directory.0, resources(), cluster, group).unwrap();
        let snapshots = directory.0.join("snapshots");
        let state = StateMachine::new(engine, snapshots, Arc::new(Semaphore::new(2))).unwrap();
        let local = peer(1, cluster, group);
        let mut store = state.log_store(&local).unwrap();
        let vote = VoteOf::<TypeConfig>::new_committed(1, 1);
        store.save_vote(&vote).await.unwrap();
        let mut entries = Vec::new();
        for index in 0..6 {
            let operation = Operation::Insert(RowPayload {
                table_id: Uuid::new_v4(),
                row: Row::from([("id".into(), Scalar::Utf8("x".repeat(6 * 1024 * 1024)))]),
            });
            entries.push(EntryOf::<TypeConfig>::new_normal(
                LogIdOf::<TypeConfig>::new(*vote.leader_id(), index),
                command(operation, Uuid::new_v4(), 1, None),
            ));
        }
        store.append(entries, IOFlushed::noop()).await.unwrap();
        assert_eq!(
            store
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            5
        );
        assert!(
            store.try_get_log_entries(..).await.is_err(),
            "large full-range reads must fail before allocating the whole log"
        );
        let stream = store.entries_stream(..).await;
        tokio::pin!(stream);
        let mut expected = 0;
        while let Some(entry) = stream.next().await {
            assert_eq!(entry.unwrap().index(), expected);
            expected += 1;
        }
        assert_eq!(expected, 6);
        let mut other = state.log_store(&local).unwrap();
        assert_eq!(other.read_vote().await.unwrap(), Some(vote));
        let mut foreign = local.clone();
        foreign.incarnation = Uuid::new_v4();
        assert!(state.log_store(&foreign).is_err());
    })
    .await
    .expect("bounded log fixture");
}

fn openssl(directory: &Path, args: &[&str]) {
    use std::io::Read;
    let diagnostic = directory.join("openssl.stderr");
    let mut child = Command::new("openssl")
        .args(args)
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(fs::File::create(&diagnostic).unwrap()))
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= until {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("openssl fixture watchdog");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut output = String::new();
    fs::File::open(diagnostic)
        .unwrap()
        .take(8192)
        .read_to_string(&mut output)
        .unwrap();
    assert!(status.success(), "openssl: {output}");
}

fn certificates(directory: &Path) -> Vec<TlsMaterial> {
    openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=nabedra-test-ca",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ],
    );
    fs::write(directory.join("extensions"),"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost\n").unwrap();
    let ca = fs::read(directory.join("ca.pem")).unwrap();
    (1..=3)
        .map(|id| {
            let key = format!("node{id}.key");
            let csr = format!("node{id}.csr");
            let certificate = format!("node{id}.pem");
            openssl(
                directory,
                &[
                    "req",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-subj",
                    "/CN=localhost",
                    "-keyout",
                    &key,
                    "-out",
                    &csr,
                ],
            );
            openssl(
                directory,
                &[
                    "x509",
                    "-req",
                    "-in",
                    &csr,
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-out",
                    &certificate,
                    "-days",
                    "1",
                    "-extfile",
                    "extensions",
                ],
            );
            TlsMaterial::from_pem(
                &fs::read(directory.join(certificate)).unwrap(),
                &fs::read(directory.join(key)).unwrap(),
                &ca,
            )
            .unwrap()
        })
        .collect()
}
struct Node {
    raft: Raft<TypeConfig, StateMachine>,
    state: StateMachine,
    control: Arc<Semaphore>,
    shutdown: Option<oneshot::Sender<()>>,
    server: JoinHandle<std::io::Result<()>>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.control.close();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.server.abort();
    }
}

struct Cluster {
    directory: Temporary,
    peers: BTreeMap<u64, Peer>,
    tls: Vec<TlsMaterial>,
    nodes: Vec<Option<Node>>,
}
impl Cluster {
    async fn new() -> Self {
        let directory = Temporary::new();
        let tls = certificates(&directory.0);
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let mut peers = BTreeMap::new();
        let mut listeners = Vec::new();
        for id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut member = peer(id, cluster, group);
            member.address = listener.local_addr().unwrap().to_string();
            member.certificate_sha256 = tls[id as usize - 1].certificate_sha256().into();
            peers.insert(id, member);
            listeners.push(listener);
        }
        let mut result = Self {
            directory,
            peers,
            tls,
            nodes: vec![None, None, None],
        };
        for (index, listener) in listeners.into_iter().enumerate() {
            let node = result.start(index, listener).await;
            result.nodes[index] = Some(node);
        }
        result.nodes[0]
            .as_ref()
            .unwrap()
            .raft
            .initialize(result.peers.clone())
            .await
            .unwrap();
        result
    }
    async fn start(&self, index: usize, listener: TcpListener) -> Node {
        let peer = self.peers[&(index as u64 + 1)].clone();
        let path = self.directory.0.join(format!("store{}", index));
        fs::create_dir_all(&path).unwrap();
        let engine =
            OltpEngine::open_replica(&path, resources(), peer.cluster_id, peer.group_id).unwrap();
        let state =
            StateMachine::new(engine, path.join("snapshots"), Arc::new(Semaphore::new(2))).unwrap();
        let control = Arc::new(Semaphore::new(4));
        let snapshots = Arc::new(Semaphore::new(1));
        let network = NetworkFactory::new(
            peer.clone(),
            self.tls[index].clone(),
            control.clone(),
            snapshots.clone(),
        )
        .unwrap();
        let config = Arc::new(
            openraft::Config {
                heartbeat_interval: 5000,
                election_timeout_min: 15000,
                election_timeout_max: 20000,
                max_payload_entries: 1024,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            peer.node_id,
            config,
            network,
            state.log_store(&peer).unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        let incoming = path.join("incoming");
        fs::create_dir_all(&incoming).unwrap();
        let server = Arc::new(
            Server::new(
                raft.clone(),
                peer,
                self.tls[index].clone(),
                Arc::new(RwLock::new(self.peers.clone())),
                Arc::new(Semaphore::new(4)),
                snapshots,
                incoming,
                Duration::from_secs(10),
            )
            .unwrap(),
        );
        let (tx, rx) = oneshot::channel();
        let server = tokio::spawn(server.run(listener, async {
            let _ = rx.await;
        }));
        Node {
            raft,
            state,
            control,
            shutdown: Some(tx),
            server,
        }
    }
    async fn leader(&self, eligible: &[usize]) -> usize {
        for &index in eligible {
            let node = self.nodes[index].as_ref().unwrap();
            if let Ok(metrics) = node
                .raft
                .wait(Some(Duration::from_secs(60)))
                .metrics(
                    |metrics| {
                        metrics
                            .current_leader
                            .is_some_and(|id| eligible.contains(&(id as usize - 1)))
                    },
                    "eligible leader",
                )
                .await
            {
                let leader = metrics.current_leader.unwrap() as usize - 1;
                if eligible.contains(&leader) {
                    let selected = &self.nodes[leader].as_ref().unwrap().raft;
                    selected
                        .wait(Some(Duration::from_secs(10)))
                        .metrics(
                            |metrics| {
                                metrics.current_leader == Some(leader as u64 + 1)
                                    && metrics.last_quorum_acked.is_some()
                            },
                            "quorum acknowledged leader",
                        )
                        .await
                        .unwrap();
                    timeout(
                        Duration::from_secs(10),
                        selected.ensure_linearizable(openraft::ReadPolicy::ReadIndex),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    return leader;
                }
            }
        }
        panic!("no eligible leader");
    }
    async fn isolate(&mut self, index: usize) {
        let node = self.nodes[index].as_mut().unwrap();
        node.control.close();
        if let Some(tx) = node.shutdown.take() {
            let _ = tx.send(());
        }
        timeout(Duration::from_secs(10), &mut node.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    async fn stop(&mut self, index: usize) {
        if let Some(mut node) = self.nodes[index].take() {
            if let Some(tx) = node.shutdown.take() {
                let _ = tx.send(());
                timeout(Duration::from_secs(10), &mut node.server)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }
            node.raft.shutdown().await.unwrap();
        }
    }
    async fn close(&mut self) {
        for index in 0..3 {
            self.stop(index).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_o10_o15_three_tls_peers_replicate_fence_and_reject_stale_authority() {
    use futures_util::FutureExt;
    let mut cluster = timeout(Duration::from_secs(30), Cluster::new())
        .await
        .expect("cluster setup watchdog");
    let outcome = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(90), async {
        let leader = cluster.leader(&[0, 1, 2]).await;
        let run = Uuid::new_v4();
        let table = Uuid::new_v4();
        let raft = &cluster.nodes[leader].as_ref().unwrap().raft;
        assert!(
            raft.client_write(ReplicatedCommand::Fence {
                expected_run: None,
                expected_term: 0,
                run_id: run,
                term: 1
            })
            .await
            .unwrap()
            .data
            .result
            .is_ok()
        );
        let create = Operation::Create(CreatePayload {
            name: "users".into(),
            schema: Schema::from([
                ("id".into(), ColumnType::Int64),
                ("blob".into(), ColumnType::Utf8),
            ]),
            key: "id".into(),
            format: TableFormat::Parquet,
        });
        assert!(
            raft.client_write(command(create, run, 1, Some(table)))
                .await
                .unwrap()
                .data
                .result
                .is_ok()
        );
        // Six individually valid requests exceed one 32 MiB log/network batch.
        let commands = (0..6)
            .map(|id| {
                command(
                    Operation::Insert(RowPayload {
                        table_id: table,
                        row: Row::from([
                            ("id".into(), Scalar::Int64(id)),
                            ("blob".into(), Scalar::Utf8("x".repeat(6 * 1024 * 1024))),
                        ]),
                    }),
                    run,
                    1,
                    None,
                )
            })
            .collect::<Vec<_>>();
        let mut responses = raft.client_write_many(commands).await.unwrap();
        let mut last = 0;
        let mut completed = 0;
        while let Some(response) = responses.next().await {
            use openraft::type_config::async_runtime::watch::WatchReceiver;
            let response = match response {
                Ok(result) => result.unwrap(),
                Err(error) => {
                    let _ = raft
                        .wait(Some(Duration::from_secs(2)))
                        .metrics(
                            |metrics| metrics.running_state.is_err(),
                            "fatal state published",
                        )
                        .await;
                    panic!(
                        "write stream {error:?}; leader state {:?}",
                        raft.metrics().borrow_watched().running_state
                    );
                }
            };
            assert!(
                response.response.result.is_ok(),
                "{:?}",
                response.response.result
            );
            last = response.log_id.index;
            completed += 1;
        }
        assert_eq!(completed, 6);
        let mut hashes = Vec::new();
        for (index, node) in cluster
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node.as_ref().map(|node| (index, node)))
        {
            node.raft
                .wait(Some(Duration::from_secs(15)))
                .applied_index_at_least(Some(last), "replicated rows")
                .await
                .unwrap();
            assert_eq!(
                node.state
                    .engine()
                    .execute(query(table))
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::UnsupportedOperation,
                "raw replica reads must not bypass authority"
            );
            let snapshot = node
                .state
                .engine()
                .export_snapshot(
                    cluster.directory.0.join(format!("replica-{index}.snap")),
                    serde_json::Value::Null,
                )
                .await
                .unwrap();
            use sha2::{Digest, Sha256};
            use std::io::Read;
            let mut file = fs::File::open(&snapshot.path).unwrap();
            let mut hash = Sha256::new();
            let mut buffer = vec![0u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                hash.update(&buffer[..read]);
            }
            hashes.push(hash.finalize().to_vec());
        }
        assert_eq!(hashes.len(), 3);
        assert!(
            hashes.windows(2).all(|pair| pair[0] == pair[1]),
            "replicas disagree on schema, rows, decisions or applied state"
        );
        let active = cluster.nodes[leader].as_ref().unwrap();
        assert_eq!(
            active.state.read(&active.raft, query(table)).await.unwrap(),
            serde_json::json!(6)
        );
        cluster.isolate(leader).await;
        let eligible: Vec<_> = (0..3).filter(|index| *index != leader).collect();
        let successor = cluster.leader(&eligible).await;
        let active = &cluster.nodes[successor].as_ref().unwrap();
        assert!(
            active
                .raft
                .client_write(ReplicatedCommand::Fence {
                    expected_run: Some(run),
                    expected_term: 1,
                    run_id: run,
                    term: 2
                })
                .await
                .unwrap()
                .data
                .result
                .is_ok()
        );
        let stale = Operation::Insert(RowPayload {
            table_id: table,
            row: Row::from([
                ("id".into(), Scalar::Int64(99)),
                ("blob".into(), Scalar::Utf8("stale".into())),
            ]),
        });
        assert!(
            active
                .raft
                .client_write(command(stale, run, 1, None))
                .await
                .unwrap()
                .data
                .result
                .is_err()
        );
        assert_eq!(
            active.state.read(&active.raft, query(table)).await.unwrap(),
            serde_json::json!(6)
        );
        let isolated = cluster.nodes[leader].as_ref().unwrap();
        let read = timeout(
            Duration::from_secs(4),
            isolated.state.read(&isolated.raft, query(table)),
        )
        .await;
        assert!(
            !matches!(read, Ok(Ok(_))),
            "isolated former leader served a strong read"
        );
    }))
    .catch_unwind()
    .await;
    timeout(Duration::from_secs(30), cluster.close())
        .await
        .expect("cluster cleanup watchdog");
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("three-peer fixture watchdog"),
        Err(error) => std::panic::resume_unwind(error),
    }
}

/// A real follower observes only the transmitted prefix. No application command
/// is committed by this transport fixture; the separate cluster test establishes
/// quorum commit and read authority.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_wide_append_prefix_acknowledges_only_sent_entries_and_conflicts_truncate() {
    use futures_util::FutureExt;
    use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
    use openraft::raft::AppendEntriesRequest;
    let mut harness = timeout(Duration::from_secs(30), async {
        let directory = Temporary::new();
        let tls = certificates(&directory.0);
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let mut peers = BTreeMap::new();
        let mut listeners = Vec::new();
        for id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut peer = peer(id, cluster, group);
            peer.address = listener.local_addr().unwrap().to_string();
            peer.certificate_sha256 = tls[id as usize - 1].certificate_sha256().into();
            peers.insert(id, peer);
            listeners.push(listener);
        }
        let mut harness = Cluster {
            directory,
            peers,
            tls,
            nodes: vec![None, None, None],
        };
        let follower_listener = listeners.swap_remove(1);
        let node = harness.start(1, follower_listener).await;
        harness.nodes[1] = Some(node);
        harness
    })
    .await
    .expect("follower setup watchdog");
    let outcome = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(90), async {
        let mut factory = NetworkFactory::new(
            harness.peers[&1].clone(),
            harness.tls[0].clone(),
            Arc::new(Semaphore::new(2)),
            Arc::new(Semaphore::new(1)),
        )
        .unwrap();
        let mut client = factory.new_client(2, &harness.peers[&2]).await;
        let vote = VoteOf::<TypeConfig>::new_committed(100, 1);
        let entries = (0..6)
            .map(|index| {
                EntryOf::<TypeConfig>::new_normal(
                    LogIdOf::<TypeConfig>::new(*vote.leader_id(), index),
                    command(
                        Operation::Insert(RowPayload {
                            table_id: Uuid::new_v4(),
                            row: Row::from([(
                                "id".into(),
                                Scalar::Utf8("x".repeat(6 * 1024 * 1024)),
                            )]),
                        }),
                        Uuid::new_v4(),
                        1,
                        None,
                    ),
                )
            })
            .collect::<Vec<_>>();
        let request = AppendEntriesRequest {
            vote,
            prev_log_id: None,
            entries: entries.clone(),
            leader_commit: None,
        };
        let acknowledged = client
            .append_entries(request, RPCOption::new(Duration::from_secs(20)))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            acknowledged.index, 0,
            "the original unsent tail must not be acknowledged"
        );
        let follower = harness.nodes[1].as_ref().unwrap();
        let mut log = follower.state.log_store(&harness.peers[&2]).unwrap();
        assert_eq!(
            log.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            0
        );
        let mut acknowledged = acknowledged;
        while acknowledged.index < 5 {
            let remaining = entries[(acknowledged.index + 1) as usize..].to_vec();
            let tail = AppendEntriesRequest {
                vote,
                prev_log_id: Some(acknowledged),
                entries: remaining,
                leader_commit: None,
            };
            let next = client
                .append_entries(tail, RPCOption::new(Duration::from_secs(20)))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(next.index > acknowledged.index && next.index <= 5);
            acknowledged = next;
        }
        assert_eq!(
            log.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            5
        );
        let mut replacement = NetworkFactory::new(
            harness.peers[&3].clone(),
            harness.tls[2].clone(),
            Arc::new(Semaphore::new(2)),
            Arc::new(Semaphore::new(1)),
        )
        .unwrap()
        .new_client(2, &harness.peers[&2])
        .await;
        let new_vote = VoteOf::<TypeConfig>::new_committed(101, 3);
        let new_tail =
            EntryOf::<TypeConfig>::new_blank(LogIdOf::<TypeConfig>::new(*new_vote.leader_id(), 4));
        let replacement_request = AppendEntriesRequest {
            vote: new_vote,
            prev_log_id: Some(entries[3].log_id()),
            entries: vec![new_tail.clone()],
            leader_commit: None,
        };
        assert_eq!(
            replacement
                .append_entries(replacement_request, RPCOption::new(Duration::from_secs(20)))
                .await
                .unwrap()
                .unwrap(),
            Some(new_tail.log_id())
        );
        assert_eq!(
            log.get_log_state().await.unwrap().last_log_id,
            Some(new_tail.log_id())
        );
        let old = AppendEntriesRequest {
            vote: VoteOf::<TypeConfig>::new_committed(100, 1),
            prev_log_id: None,
            entries,
            leader_commit: None,
        };
        assert!(
            client
                .append_entries(old, RPCOption::new(Duration::from_secs(20)))
                .await
                .unwrap()
                .is_err(),
            "a former leader must not restore the conflicting tail"
        );
    }))
    .catch_unwind()
    .await;
    timeout(Duration::from_secs(20), harness.close())
        .await
        .expect("follower cleanup watchdog");
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("prefix fixture watchdog"),
        Err(error) => std::panic::resume_unwind(error),
    }
}
