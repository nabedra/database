//! T1b: real typed Raft voters over mTLS worker transports.
use super::*;
#[path = "../../../tests/support/tls.rs"]
mod tls;
use crate::oltp_replication::{
    multiplex::{PeerDirectory, Purpose, TransportConfig, TransportTrust, WorkerTransport},
    typed_network::{RECEIVED_APPENDS, ReplicaEndpoint, TypedNetworkFactory},
    types::{Digest, WorkerEnrollment, WorkerIdentity},
};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use tokio::{net::TcpListener, sync::oneshot};

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

struct Voter {
    consensus: Arc<RaftConsensus<CounterConfig, Counter>>,
    store: GroupStore,
    pool: OltpPool,
    node: u64,
    _resident: ResidentGroup,
}

/// Three voters of one Catalog group, each on its own worker transport.
/// `one_shot` voters speak only the pre-session protocol. Only node 1
/// elects, so a loaded test machine cannot move leadership mid-measurement.
async fn voters(
    dir: &Path,
    one_shot: &[u64],
) -> (
    Vec<Voter>,
    Vec<oneshot::Sender<()>>,
    Vec<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
    GroupIdentity,
) {
    let certificates = tls::certificates(dir, 3);
    let cluster = Uuid::new_v4();
    let group = GroupIdentity {
        cluster_id: cluster,
        database: Text::try_from("t1b").unwrap(),
        group_id: Uuid::new_v4(),
        kind: GroupKind::Catalog,
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
    let mut voters = Vec::new();
    let mut stops = Vec::new();
    let mut tasks = Vec::new();
    for (index, ((worker, tls), listener)) in
        workers.iter().zip(&certificates).zip(listeners).enumerate()
    {
        let node = index as u64 + 1;
        let shared = shared();
        let pool = OltpPool::open_development(
            dir.join(format!("pool{node}")),
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: cluster,
                worker_id: worker.worker_id,
                pool_id: members[&node].pool_id,
            },
            shared.clone(),
        )
        .unwrap();
        let store = pool.group(group.clone()).await.unwrap();
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let build = if one_shot.contains(&node) {
            WorkerTransport::one_shot_for_test
        } else {
            WorkerTransport::new
        };
        let transport = Arc::new(
            build(
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
        let log = TypedLogStore::<CounterConfig>::new(store.clone(), node)
            .await
            .unwrap()
            .with_resident(lease.clone())
            .unwrap();
        let state = HostedState::new(
            Counter {
                store: store.clone(),
            },
            lease.clone(),
        )
        .unwrap();
        let config = batched_config(openraft::Config {
            enable_elect: node == 1,
            // Generous timeouts: the one-shot voter replicates at the
            // pre-session pace (a TLS handshake and an fsync per entry), and
            // a loaded test machine must not cause leader churn.
            heartbeat_interval: 250,
            election_timeout_min: 2000,
            election_timeout_max: 4000,
            ..Default::default()
        })
        .unwrap();
        let network = TypedNetworkFactory::<CounterConfig>::new(
            transport.client(),
            members[&node].clone(),
            lease.clone(),
            shared.clone(),
        )
        .unwrap();
        let consensus = RaftConsensus::start(config, network, log, state)
            .await
            .unwrap();
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
        stops.push(stop);
        tasks.push(tokio::spawn(transport.run(listener, async {
            let _ = stopped.await;
        })));
        voters.push(Voter {
            consensus,
            store,
            pool,
            node,
            _resident: resident,
        });
    }
    voters[0].consensus.raft.initialize(members).await.unwrap();
    (voters, stops, tasks, group)
}

async fn leader(voters: &[Voter]) -> usize {
    timeout_loop(|| async {
        for (index, voter) in voters.iter().enumerate() {
            let metrics = voter.consensus.raft.metrics();
            let leader = metrics.borrow_watched().current_leader;
            if leader == Some(voter.node) {
                return Some(index);
            }
        }
        None
    })
    .await
}

async fn timeout_loop<T, F: Future<Output = Option<T>>>(mut probe: impl FnMut() -> F) -> T {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replicated fixture watchdog")
}

/// Proposes concurrently so client writes coalesce into append batches. A
/// proposal refused with ForwardToLeader was never accepted, so it is retried
/// once the leader settles; any other failure is reported in full.
async fn propose_all(voter: &Voter, ids: std::ops::Range<u64>) {
    let shared = voter.store.resources().clone();
    let mut tasks = Vec::new();
    for id in ids {
        let consensus = voter.consensus.clone();
        let shared = shared.clone();
        tasks.push(tokio::spawn(async move {
            let started = std::time::Instant::now();
            loop {
                let command = OwnedCommand::new_owned(
                    Bump {
                        id,
                        pad: "x".repeat(256),
                    },
                    &shared,
                    ByteClass::Data,
                )
                .unwrap();
                command
                    .reserve_raft_metadata(
                        &consensus.state._resident,
                        OwnedRaftEntry::<Bump>::normal_metadata_bytes(),
                    )
                    .unwrap();
                match consensus.raft.client_write(command).await {
                    Ok(_) => return,
                    Err(error)
                        if error.forward_to_leader().is_some()
                            && started.elapsed() < Duration::from_secs(60) =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => panic!("proposal {id} failed: {error}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

fn received(group: &GroupIdentity, node: u64) -> (u64, u64, u64) {
    RECEIVED_APPENDS
        .lock()
        .unwrap()
        .get(&(group.group_id, node))
        .copied()
        .unwrap_or_default()
}

/// T1b/O06/O11/D10: a 3-voter group over real `nabedra-otp/2` sessions.
/// Followers receive multi-entry AppendEntries and write each as synced
/// batches, so their fsyncs per replicated entry stay well below one. A
/// voter that speaks only the one-shot protocol still replicates, one
/// entry per RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o06_o11_followers_receive_multi_entry_appends_with_few_fsyncs() {
    let dir = Dir::new();
    let (voters, stops, tasks, group) = voters(&dir.0, &[3]).await;
    let leader = leader(&voters).await;
    assert_eq!(voters[leader].node, 1, "only node 1 elects");
    // One entry first: it opens the sessions that negotiate the feature.
    propose_all(&voters[leader], 0..1).await;
    let before: Vec<_> = voters
        .iter()
        .map(|voter| voter.pool.write_stats().synced_writes)
        .collect();
    let received_before: Vec<_> = voters
        .iter()
        .map(|voter| received(&group, voter.node))
        .collect();
    let commands = 256;
    propose_all(&voters[leader], 1..commands).await;
    for voter in &voters {
        timeout_loop(|| async { (read_count(&voter.store).await == commands).then_some(()) }).await;
    }
    for (index, voter) in voters.iter().enumerate() {
        if index == leader {
            continue;
        }
        let fsyncs = voter.pool.write_stats().synced_writes - before[index];
        let now = received(&group, voter.node);
        let (rpcs, entries, multi) = (
            now.0 - received_before[index].0,
            now.1 - received_before[index].1,
            now.2 - received_before[index].2,
        );
        eprintln!(
            "t1b follower node {}: {entries} entries in {rpcs} append RPCs \
             ({multi} multi-entry), {fsyncs} fsyncs",
            voter.node
        );
        assert!(entries >= commands - 1, "{entries} entries replicated");
        if voter.node == 3 {
            // The one-shot peer: one entry per RPC, as before sessions.
            assert_eq!(multi, 0);
            assert!(rpcs >= entries);
        } else {
            assert!(multi > 0, "no multi-entry append reached the follower");
            assert!(rpcs < entries / 2, "{rpcs} RPCs for {entries} entries");
            // Before T1b a follower fsynced once per entry.
            assert!(
                fsyncs * 4 < entries,
                "{fsyncs} fsyncs for {entries} entries"
            );
        }
    }
    for voter in &voters {
        voter.consensus.shutdown().await.unwrap();
    }
    for stop in stops {
        let _ = stop.send(());
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}
