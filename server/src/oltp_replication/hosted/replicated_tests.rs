//! T1b: real typed Raft voters over mTLS worker transports.
use super::*;
#[path = "../../../tests/support/tls.rs"]
mod tls;
use crate::oltp_replication::{
    multiplex::{
        AppliedPeerDirectory, EndpointFuture, PeerDirectory, Purpose, REVOCATION_REFRESH,
        REVOCATION_STALE_AFTER, RevocationSource, TransportConfig, TransportTrust, WorkerTransport,
    },
    typed_network::{RECEIVED_APPENDS, ReplicaEndpoint, TypedNetworkFactory},
    types::{Digest, WorkerEnrollment, WorkerIdentity},
};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use std::sync::atomic::AtomicU64;
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
    identity: WorkerIdentity,
    transport: Arc<WorkerTransport>,
    /// REP6: the Data-only directory of the voter built by [`data_only_voters`].
    data_only: Option<Arc<AppliedPeerDirectory>>,
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
    voters_with(dir, one_shot, &BTreeMap::new(), false).await
}
/// [`voters`] where node `n`'s pool runs on `faults[n]` (`FaultFs`), and,
/// with `elect_all`, every node may elect (a forced election).
async fn voters_with(
    dir: &Path,
    one_shot: &[u64],
    faults: &BTreeMap<u64, FaultFs>,
    elect_all: bool,
) -> (
    Vec<Voter>,
    Vec<oneshot::Sender<()>>,
    Vec<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
    GroupIdentity,
) {
    voters_inner(dir, one_shot, faults, elect_all, None).await
}
/// [`voters`] where node `data_only` authenticates peers like a worker with
/// no Control replica: an [`AppliedPeerDirectory`] holding the three
/// enrollments, whose revocation view comes from a scripted Control source.
async fn data_only_voters(
    dir: &Path,
    data_only: u64,
) -> (
    Vec<Voter>,
    Vec<oneshot::Sender<()>>,
    Vec<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
    GroupIdentity,
) {
    voters_inner(dir, &[], &BTreeMap::new(), false, Some(data_only)).await
}
async fn voters_inner(
    dir: &Path,
    one_shot: &[u64],
    faults: &BTreeMap<u64, FaultFs>,
    elect_all: bool,
    data_only: Option<u64>,
) -> (
    Vec<Voter>,
    Vec<oneshot::Sender<()>>,
    Vec<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
    GroupIdentity,
) {
    let certificates = tls::certificates(dir, 3);
    let cluster = nabedra_rt::ids::uuid_v4();
    let group = GroupIdentity {
        cluster_id: cluster,
        database: Text::try_from("t1b").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Catalog,
        group_format_version: 1,
    };
    let mut listeners = Vec::new();
    let mut workers = Vec::new();
    for tls in &certificates {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        workers.push(WorkerEnrollment {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
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
                    pool_id: nabedra_rt::ids::uuid_v4(),
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
        let path = dir.join(format!("pool{node}"));
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: cluster,
            worker_id: worker.worker_id,
            pool_id: members[&node].pool_id,
        };
        let pool = match faults.get(&node) {
            Some(fault) => {
                std::fs::create_dir_all(&path).unwrap();
                fault.create_dir_all_durable(&path);
                let fs: Arc<dyn Fs> = Arc::new(fault.clone());
                OltpPool::open_development_tuned(
                    &path,
                    identity,
                    shared.clone(),
                    fs,
                    super::transient::small_segments,
                )
            }
            None => OltpPool::open_development(&path, identity, shared.clone()),
        }
        .unwrap();
        let store = pool.group(group.clone()).await.unwrap();
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let build = if one_shot.contains(&node) {
            WorkerTransport::one_shot_for_test
        } else {
            WorkerTransport::new
        };
        let directory = (data_only == Some(node)).then(|| {
            let control = GroupIdentity {
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Control,
                ..group.clone()
            };
            let directory =
                AppliedPeerDirectory::from_bootstrap(control, workers.clone(), shared.clone())
                    .unwrap();
            for worker in &workers {
                directory.install_worker_for_test(worker.clone()).unwrap();
            }
            directory
        });
        let trust: Arc<dyn PeerDirectory> = match &directory {
            Some(directory) => directory.clone(),
            None => peers.clone(),
        };
        let transport = Arc::new(
            build(
                worker,
                tls,
                shared.clone(),
                trust,
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
            enable_elect: elect_all || node == 1,
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
        tasks.push(nabedra_rt::task::spawn(transport.clone().run(listener, async {
            let _ = stopped.await;
        })));
        voters.push(Voter {
            consensus,
            store,
            pool,
            node,
            identity: WorkerIdentity::from(worker),
            transport,
            data_only: directory,
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
    nabedra_rt::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            nabedra_rt::time::sleep(Duration::from_millis(20)).await;
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
        tasks.push(nabedra_rt::task::spawn(async move {
            let started = nabedra_rt::time::now_std();
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
                crate::oltp_replication::entry::reserve_raft_metadata(
                    &command,
                    &consensus.state._resident,
                    OwnedRaftEntry::<Bump>::normal_metadata_bytes(),
                )
                .unwrap();
                match consensus.raft.client_write(command).await {
                    Ok(_) => return,
                    Err(error)
                        if error.forward_to_leader().is_some()
                            && nabedra_rt::time::elapsed(started) < Duration::from_secs(60) =>
                    {
                        nabedra_rt::time::sleep(Duration::from_millis(100)).await;
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

fn fill_device(dir: &Path, fault: &FaultFs, node: u64) {
    super::transient::fill_device(fault, &dir.join(format!("pool{node}")));
}
fn running(voter: &Voter) -> bool {
    voter
        .consensus
        .raft
        .metrics()
        .borrow_watched()
        .running_state
        .is_ok()
}
async fn applied_everywhere(voters: &[Voter], commands: u64) {
    for voter in voters {
        timeout_loop(|| async { (read_count(&voter.store).await == commands).then_some(()) }).await;
    }
}
async fn stop_all(
    voters: Vec<Voter>,
    stops: Vec<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
) {
    for voter in &voters {
        nabedra_rt::time::timeout(Duration::from_secs(20), voter.consensus.shutdown())
            .await
            .expect("shutdown")
            .unwrap();
    }
    for stop in stops {
        let _ = stop.send(());
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

/// RV-APPLY F1, NS11, O06, O17 (RF3): one follower's device fills up. Its
/// appends retry NoSpace while the other two commit; its core stays up, and
/// once space frees up it catches up to the leader without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns11_o06_o17_rv_apply_rf3_follower_out_of_space_catches_up_without_restart() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let faults = BTreeMap::from([(3, fault.clone())]);
    let (voters, stops, tasks, _group) = voters_with(&dir.0, &[], &faults, false).await;
    let leader = leader(&voters).await;
    assert_eq!(voters[leader].node, 1);
    propose_all(&voters[leader], 0..10).await;
    applied_everywhere(&voters, 10).await;
    fill_device(&dir.0, &fault, 3);
    // Enough traffic to reach the follower's next journal segment roll.
    let follower = &voters[2];
    let mut sent = 10;
    while follower.consensus.health().snapshot().no_space_retries == 0 {
        assert!(sent < 2000, "the follower never met its full device");
        propose_all(&voters[leader], sent..sent + 20).await;
        sent += 20;
    }
    for voter in &voters[..2] {
        timeout_loop(|| async { (read_count(&voter.store).await == sent).then_some(()) }).await;
    }
    assert!(running(follower), "ENOSPC stopped the follower's core");
    assert!(read_count(&follower.store).await < sent);
    assert!(!follower.pool.requires_recovery());
    fault.clear_faults();
    propose_all(&voters[leader], sent..sent + 10).await;
    applied_everywhere(&voters, sent + 10).await;
    assert!(running(follower));
    assert_eq!(
        follower.consensus.raft.restarts(),
        0,
        "caught up on the same core"
    );
    eprintln!(
        "F1 RF3 follower: {:?}",
        follower.consensus.health().snapshot()
    );
    stop_all(voters, stops, tasks).await;
}

/// RV-APPLY F1, NS11, O06 (RF3): a forced election on a follower whose vote
/// save meets a full device. A segment-roll ENOSPC cannot be aimed at one
/// vote write, so a test hook fails the next 20 vote saves with the same
/// `NoSpace` error. `save_vote` retries instead of stopping the core; then the
/// election completes and the group commits on every voter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns11_o06_rv_apply_rf3_save_vote_retries_during_a_forced_election() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let faults = BTreeMap::from([(3, fault.clone())]);
    let (voters, stops, tasks, _group) = voters_with(&dir.0, &[], &faults, true).await;
    let first = leader(&voters).await;
    propose_all(&voters[first], 0..10).await;
    applied_everywhere(&voters, 10).await;
    let candidate = voters.iter().position(|voter| voter.node == 3).unwrap();
    let candidate = &voters[candidate];
    let _ = &fault;
    candidate
        .consensus
        .retry()
        .fail_votes
        .store(20, std::sync::atomic::Ordering::Release);
    candidate
        .consensus
        .raft
        .trigger()
        .elect(false)
        .await
        .unwrap();
    timeout_loop(|| async {
        let retries = candidate.consensus.health().snapshot().retries;
        (retries["vote"] > 0).then_some(())
    })
    .await;
    assert!(running(candidate), "a NoSpace vote stopped the core");
    // The group settles on a leader and commits on all three voters.
    let settled = leader(&voters).await;
    propose_all(&voters[settled], 10..20).await;
    applied_everywhere(&voters, 20).await;
    assert!(voters.iter().all(running));
    assert!(
        voters
            .iter()
            .all(|voter| voter.consensus.raft.restarts() == 0)
    );
    stop_all(voters, stops, tasks).await;
}

/// REP6: a scripted Control revocation source. Each call stands for one
/// majority read (one RPC per Control voter).
#[derive(Default)]
struct ScriptedRevocations {
    calls: AtomicU64,
    failing: AtomicBool,
    revoked: std::sync::Mutex<Vec<WorkerIdentity>>,
}
impl RevocationSource for ScriptedRevocations {
    fn revocations(&self) -> EndpointFuture<'_, Vec<WorkerIdentity>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.failing.load(Ordering::Acquire) {
                return Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "scripted Control majority outage",
                    Outcome::Unknown,
                ));
            }
            Ok(self.revoked.lock().unwrap().clone())
        })
    }
}

/// Seeds the Data-only voter's view and starts its refresh task, as
/// `data_worker_process` does at startup.
async fn start_view(
    voter: &Voter,
) -> (
    Arc<AppliedPeerDirectory>,
    Arc<ScriptedRevocations>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let directory = voter.data_only.clone().expect("Data-only voter");
    let source = Arc::new(ScriptedRevocations::default());
    directory.seed_remote_revocations(&*source).await.unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let refresh = nabedra_rt::task::spawn(
        directory
            .clone()
            .run_revocation_refresh(source.clone(), stopped),
    );
    (directory, source, stop, refresh)
}

fn percentile(samples: &[u64], percent: usize) -> f64 {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let index = (ordered.len() * percent / 100).min(ordered.len() - 1);
    ordered[index] as f64 / 1000.0
}

/// D01/O06/O17 (REP6): a Data-only follower authenticates every
/// AppendEntries against its in-memory revocation view. Its Control reads
/// follow the refresh interval (one majority read per second), not the
/// appends. A Control majority outage leaves the view stale but does not
/// stop replication: with the other follower stopped, every commit needs
/// the Data-only follower's ack, and all of them commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_d01_rep6_data_only_follower_appends_make_no_control_call_and_survive_a_control_outage()
{
    let dir = Dir::new();
    let (voters, stops, tasks, group) = data_only_voters(&dir.0, 3).await;
    let leader = leader(&voters).await;
    assert_eq!(voters[leader].node, 1, "only node 1 elects");
    let (directory, source, stop, refresh) = start_view(&voters[2]).await;
    assert_eq!(source.calls.load(Ordering::Acquire), 1, "the startup seed");

    let started = nabedra_rt::time::now_std();
    let appends_before = received(&group, 3).0;
    let mut commits = Vec::new();
    for id in 0..200 {
        let at = nabedra_rt::time::now_std();
        propose_all(&voters[leader], id..id + 1).await;
        commits.push(nabedra_rt::time::elapsed(at).as_micros() as u64);
    }
    applied_everywhere(&voters, 200).await;
    let elapsed = nabedra_rt::time::elapsed(started);
    let appends = received(&group, 3).0 - appends_before;
    let reads = source.calls.load(Ordering::Acquire) - 1;
    let budget = elapsed.as_secs() / REVOCATION_REFRESH.as_secs() + 2;
    eprintln!(
        "REP6 Data-only follower: {appends} AppendEntries, {reads} Control majority reads in \
         {elapsed:?} (budget {budget}); RF3 commit p50 {:.3} ms p99 {:.3} ms",
        percentile(&commits, 50),
        percentile(&commits, 99),
    );
    assert!(appends >= 50, "the follower received {appends} appends");
    assert!(
        reads <= budget,
        "{reads} Control reads for {appends} appends in {elapsed:?}"
    );

    // Control majority outage: refreshes fail; the view goes stale.
    source.failing.store(true, Ordering::Release);
    timeout_loop(|| async {
        (directory.revocation_view_age().unwrap() > REVOCATION_STALE_AFTER).then_some(())
    })
    .await;
    let failed_reads = source.calls.load(Ordering::Acquire);
    // Stop node 2: each commit now needs node 3, the Data-only follower.
    nabedra_rt::time::timeout(Duration::from_secs(20), voters[1].consensus.shutdown())
        .await
        .expect("shutdown")
        .unwrap();
    let mut outage = Vec::new();
    for id in 200..240 {
        let at = nabedra_rt::time::now_std();
        propose_all(&voters[leader], id..id + 1).await;
        outage.push(nabedra_rt::time::elapsed(at).as_micros() as u64);
    }
    timeout_loop(|| async { (read_count(&voters[2].store).await == 240).then_some(()) }).await;
    assert!(!directory.revoked(voters[0].identity));
    // The refresh keeps retrying once per interval (the commits above can
    // finish within one interval).
    timeout_loop(|| async { (source.calls.load(Ordering::Acquire) > failed_reads).then_some(()) })
        .await;
    eprintln!(
        "REP6 Data-only follower during a Control outage (view {:?} old): commit p50 {:.3} ms \
         p99 {:.3} ms with the Data-only ack on every commit",
        directory.revocation_view_age().unwrap(),
        percentile(&outage, 50),
        percentile(&outage, 99),
    );

    let _ = stop.send(true);
    refresh.await.unwrap();
    for voter in [&voters[0], &voters[2]] {
        nabedra_rt::time::timeout(Duration::from_secs(20), voter.consensus.shutdown())
            .await
            .expect("shutdown")
            .unwrap();
    }
    for stop in stops {
        let _ = stop.send(());
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

/// O17 (REP6): a revocation that Control commits reaches the Data-only
/// follower within one refresh interval. The view wakes the follower's
/// sessions, which close the revoked peer's at once; the follower then
/// refuses its appends, and the group commits through the other voters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o17_rep6_a_learned_revocation_closes_the_data_only_follower_sessions() {
    let dir = Dir::new();
    let (voters, stops, tasks, group) = data_only_voters(&dir.0, 3).await;
    let leader = leader(&voters).await;
    assert_eq!(voters[leader].node, 1, "only node 1 elects");
    let (directory, source, stop, refresh) = start_view(&voters[2]).await;
    propose_all(&voters[leader], 0..5).await;
    applied_everywhere(&voters, 5).await;
    assert_eq!(voters[2].transport.revoked_session_closes(), 0);

    // Control commits the revocation of node 1's identity.
    let revoked = voters[0].identity;
    *source.revoked.lock().unwrap() = vec![revoked];
    let committed = nabedra_rt::time::now_std();
    timeout_loop(|| async { directory.revoked(revoked).then_some(()) }).await;
    let learned = nabedra_rt::time::elapsed(committed);
    timeout_loop(|| async { (voters[2].transport.revoked_session_closes() > 0).then_some(()) })
        .await;
    let closed = nabedra_rt::time::elapsed(committed);
    eprintln!("REP6 revocation learned after {learned:?}; session closed after {closed:?}");
    assert!(
        learned <= REVOCATION_REFRESH + Duration::from_secs(1),
        "{learned:?}"
    );
    // The push closes the session without waiting for the 1 s directory check.
    assert!(closed <= learned + Duration::from_millis(500), "{closed:?}");
    assert!(directory.resolve(revoked.worker_id).is_err());

    // An append that passed its check before the view learned may still be
    // decoding; none can start after.
    nabedra_rt::time::sleep(Duration::from_millis(200)).await;
    let appends = received(&group, 3).0;
    propose_all(&voters[leader], 5..25).await;
    for voter in &voters[..2] {
        timeout_loop(|| async { (read_count(&voter.store).await == 25).then_some(()) }).await;
    }
    nabedra_rt::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        read_count(&voters[2].store).await,
        5,
        "revoked appends applied"
    );
    assert_eq!(received(&group, 3).0, appends, "revoked appends decoded");

    let _ = stop.send(true);
    refresh.await.unwrap();
    stop_all(voters, stops, tasks).await;
}
