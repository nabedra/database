//! NS23 (plan section 3, "Raft safety of a quarantined group", rules 1 to 5)
//! with real workers in one process: each worker has its own mTLS transport,
//! hosted-group manager and pool on `FaultFs`, and the group's Raft traffic
//! crosses the real listeners. The damage is injected with `FaultFs`; crash
//! evidence is that fault model, not a physical power cut.
#[path = "../../../tests/support/tls.rs"]
mod tls;
use super::*;
use crate::{
    config::WorkerResources,
    oltp::pool::{OltpPool, PoolIdentity, ReleaseOutcome, SharedResourceConfig, TransactionBudget},
    oltp_replication::{
        ReadySetHasher,
        authority::timestamp::{TimestampHandle, TimestampMachine},
        multiplex::{PeerDirectory, TransportConfig, TransportTrust, WorkerTransport},
    },
};
use nabedra_kv::fs::{FaultFs, Fs};
use std::{path::PathBuf, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use uuid::Uuid;

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Directory {
    cluster: Uuid,
    workers: BTreeMap<Uuid, WorkerEnrollment>,
}
impl PeerDirectory for Directory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        _: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if group.cluster_id != self.cluster
            || WorkerIdentity::from(&self.resolve(peer.worker_id)?) != peer
        {
            return Err(invalid());
        }
        Ok(TransportTrust::AppliedEnrollment)
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.workers.get(&worker).cloned().ok_or_else(unavailable)
    }
}

/// One worker process: pool on `FaultFs`, manager, transport.
struct Worker {
    fault: FaultFs,
    /// Holds this worker's journal syncs on demand (PERF2d tests).
    gate: Option<crate::oltp_replication::typed_log::tests::GatedSync>,
    shared: Arc<WorkerSharedResources>,
    pool: Option<OltpPool>,
    manager: Option<Arc<HostedGroupManager>>,
    host: Option<ManagedTimestamps>,
    stop: Option<oneshot::Sender<()>>,
    transport: Option<tokio::task::JoinHandle<Result<(), ProtocolError>>>,
}

struct Cluster {
    dir: Temporary,
    tls: Vec<crate::oltp_replication::TlsMaterial>,
    enrollments: Vec<WorkerEnrollment>,
    directory: Arc<Directory>,
    members: BTreeMap<u64, ReplicaMember>,
    group: GroupIdentity,
    workers: Vec<Worker>,
    run_id: Uuid,
}

fn config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    }
}

impl Cluster {
    /// `count` workers, node `n` on worker `n - 1`; `voters` of them form the
    /// group, the others host a spare replica that is not a member yet.
    async fn new(count: usize, voters: usize) -> Self {
        Self::new_with(count, voters, false).await
    }
    /// [`Self::new`], with every worker's journal syncs behind a gate.
    async fn new_with(count: usize, voters: usize, gated: bool) -> Self {
        let dir = Temporary(std::env::temp_dir().join(format!("ns23-{}", Uuid::new_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        let tls = tls::certificates(&dir.0, count as u64);
        let cluster = Uuid::new_v4();
        let group = GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("ns23").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Timestamps,
            group_format_version: 1,
        };
        let mut listeners = Vec::new();
        let mut enrollments = Vec::new();
        for material in &tls {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            enrollments.push(WorkerEnrollment {
                worker_id: Uuid::new_v4(),
                incarnation: Uuid::new_v4(),
                certificate_sha256: Digest::parse(material.certificate_sha256()).unwrap(),
                enrollment_revision: 1,
                endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            });
            listeners.push(listener);
        }
        let members = enrollments
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
            .collect::<BTreeMap<_, _>>();
        let directory = Arc::new(Directory {
            cluster,
            workers: enrollments
                .iter()
                .cloned()
                .map(|worker| (worker.worker_id, worker))
                .collect(),
        });
        let mut cluster = Self {
            dir,
            tls,
            enrollments,
            directory,
            members,
            group,
            workers: Vec::new(),
            run_id: Uuid::new_v4(),
        };
        for (index, listener) in listeners.into_iter().enumerate() {
            let shared = WorkerSharedResources::new(
                index as u64 + 1,
                WorkerResources {
                    max_threads: 4,
                    ..Default::default()
                },
                SharedResourceConfig::default(),
            )
            .unwrap();
            let fault = FaultFs::new();
            let path = cluster.pool_path(index);
            std::fs::create_dir_all(&path).unwrap();
            fault.create_dir_all_durable(&path);
            cluster.workers.push(Worker {
                fault,
                gate: gated.then(crate::oltp_replication::typed_log::tests::GatedSync::new),
                shared,
                pool: None,
                manager: None,
                host: None,
                stop: None,
                transport: None,
            });
            cluster.start(index, Some(listener), true).await.unwrap();
        }
        let initial = cluster
            .members
            .iter()
            .filter(|(node, _)| **node <= voters as u64)
            .map(|(node, member)| (*node, member.clone()))
            .collect::<BTreeMap<_, _>>();
        cluster.workers[0]
            .host
            .as_ref()
            .unwrap()
            .raft
            .initialize(initial)
            .await
            .unwrap();
        cluster
    }

    fn pool_path(&self, index: usize) -> PathBuf {
        self.dir.0.join(format!("pool{index}"))
    }
    fn pool_identity(&self, index: usize) -> PoolIdentity {
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: self.group.cluster_id,
            worker_id: self.enrollments[index].worker_id,
            pool_id: self.members[&(index as u64 + 1)].pool_id,
        }
    }
    fn member(&self, index: usize) -> ReplicaMember {
        self.members[&(index as u64 + 1)].clone()
    }

    async fn replace_damaged(
        &self,
        leader: usize,
        damaged: usize,
        fresh: ReplicaMember,
    ) -> Result<super::DamagedReplacement, ProtocolError> {
        let old = [self.member(0), self.member(1), self.member(2)];
        let next = old
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != damaged)
            .map(|(_, member)| member.clone())
            .chain(std::iter::once(fresh.clone()))
            .collect::<Vec<_>>();
        self.host(leader)
            .replace_damaged_voter(&old, &next, &old[damaged], fresh)
            .await
    }

    /// Starts (or restarts) worker `index`: opens its pool, its transport on
    /// its enrolled address, and the group's replica. The replica start is the
    /// worker's own: it opens the retained group, which a quarantine refuses.
    async fn start(
        &mut self,
        index: usize,
        listener: Option<TcpListener>,
        create: bool,
    ) -> Result<(), ProtocolError> {
        let listener = match listener {
            Some(listener) => listener,
            None => TcpListener::bind(self.enrollments[index].endpoint.as_str())
                .await
                .unwrap(),
        };
        let identity = self.pool_identity(index);
        let path = self.pool_path(index);
        let worker = &mut self.workers[index];
        let fs: Arc<dyn Fs> = match &worker.gate {
            Some(gate) => Arc::new(gate.over(worker.fault.clone())),
            None => Arc::new(worker.fault.clone()),
        };
        let pool = OltpPool::open_development_on(&path, identity, worker.shared.clone(), fs)?;
        let registry = Arc::new(EndpointRegistry::new(worker.shared.clone()));
        let transport = Arc::new(
            WorkerTransport::new(
                &self.enrollments[index],
                &self.tls[index],
                worker.shared.clone(),
                self.directory.clone(),
                registry.clone(),
                TransportConfig::default(),
            )
            .unwrap(),
        );
        let manager = HostedGroupManager::new(worker.shared.clone(), transport.client(), registry);
        let (stop, stopped) = oneshot::channel();
        worker.stop = Some(stop);
        worker.transport = Some(tokio::spawn(transport.run(listener, async {
            let _ = stopped.await;
        })));
        if create {
            // The pool-wide records get a run of their own, so a damaged run
            // block of the group never covers store-wide keys.
            let flushing = pool.clone();
            tokio::task::spawn_blocking(move || flushing.kv_store().flush().unwrap())
                .await
                .unwrap();
        }
        worker.pool = Some(pool.clone());
        worker.manager = Some(manager.clone());
        let store = if create {
            pool.group(self.group.clone()).await
        } else {
            pool.retained_group(self.group.clone()).await
        }?;
        let host = manager
            .start_timestamps(store, self.member(index), self.members.clone(), config())
            .await?;
        self.workers[index].host = Some(host);
        Ok(())
    }

    /// Stops worker `index` like a process exit: replicas, transport, pool.
    async fn stop(&mut self, index: usize) {
        let worker = &mut self.workers[index];
        if let Some(manager) = worker.manager.take() {
            manager.shutdown().await.unwrap();
        }
        worker.host = None;
        if let Some(stop) = worker.stop.take() {
            let _ = stop.send(());
        }
        if let Some(transport) = worker.transport.take() {
            let _ = transport.await;
        }
        worker.pool = None;
        // Only completed writes survive; the next open replays the journal.
        worker.fault = worker.fault.kill();
    }

    /// Power cut of worker `index`: its disk becomes `disk` (a crash or
    /// power-loss image taken earlier). The stopped replica may be fatal.
    async fn power_cut(&mut self, index: usize, disk: FaultFs) {
        let worker = &mut self.workers[index];
        if let Some(manager) = worker.manager.take() {
            let _ = manager.shutdown().await;
        }
        worker.host = None;
        if let Some(stop) = worker.stop.take() {
            let _ = stop.send(());
        }
        if let Some(transport) = worker.transport.take() {
            let _ = transport.await;
        }
        worker.pool = None;
        worker.fault = disk;
    }

    fn host(&self, index: usize) -> &ManagedTimestamps {
        self.workers[index].host.as_ref().unwrap()
    }

    async fn leader(&self, candidates: &[usize]) -> usize {
        timeout(Duration::from_secs(20), async {
            loop {
                for &index in candidates {
                    if let Some(host) = &self.workers[index].host
                        && host.ensure_linearizable().await.is_ok()
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a leader with a quorum")
    }

    /// An acknowledged write: fence the Timestamp authority to `term`.
    async fn fence(&self, leader: usize, term: u64) -> AppliedPosition {
        let ready = |term| ReadyMembership {
            view: AuthorityView {
                run_id: self.run_id,
                owner_term: term,
                membership_revision: 1,
            },
            ready_root: ReadySetHasher::default().finish(),
            ready_count: 0,
        };
        let host = self.host(leader);
        let handle = TimestampHandle::new(
            TimestampMachine::new(host.state.store().clone()).unwrap(),
            host.clone(),
        );
        let expected = (term > 1).then(|| ready(term - 1).view.authority());
        let applied = handle
            .fence(
                Uuid::new_v4(),
                expected,
                ready(term).view.authority(),
                ready(term),
            )
            .await
            .unwrap();
        applied.position
    }

    /// [`Self::fence`] as a task whose outcome the caller inspects.
    fn fence_task(
        &self,
        leader: usize,
        term: u64,
    ) -> tokio::task::JoinHandle<Result<AppliedPosition, ProtocolError>> {
        let run_id = self.run_id;
        let ready = move |term| ReadyMembership {
            view: AuthorityView {
                run_id,
                owner_term: term,
                membership_revision: 1,
            },
            ready_root: ReadySetHasher::default().finish(),
            ready_count: 0,
        };
        let host = self.host(leader).clone();
        tokio::spawn(async move {
            let handle = TimestampHandle::new(
                TimestampMachine::new(host.state.store().clone()).unwrap(),
                host.clone(),
            );
            let expected = (term > 1).then(|| ready(term - 1).view.authority());
            handle
                .fence(
                    Uuid::new_v4(),
                    expected,
                    ready(term).view.authority(),
                    ready(term),
                )
                .await
                .map(|applied| applied.position)
        })
    }

    /// The fenced authority term a replica applied.
    async fn applied_term(&self, index: usize) -> u64 {
        let saved = self
            .host(index)
            .state
            .store()
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                // A restarted replica may not have replayed its state yet.
                |view| match view.get(b"ts/state")? {
                    Some(raw) => view.copy_output(raw.as_ref()),
                    None => view.copy_output(b"null"),
                },
            )
            .await
            .unwrap();
        let saved: serde_json::Value = serde_json::from_slice(saved.as_slice()).unwrap();
        saved["authority"]["owner_term"].as_u64().unwrap_or(0)
    }

    /// The last log index the leader knows `node` replicated.
    fn matched(&self, leader: usize, node: u64) -> Option<u64> {
        let metrics = self.host(leader).raft.metrics();
        let metrics = metrics.borrow_watched();
        metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&node).cloned())
            .flatten()
            .map(|log| log.index)
    }

    async fn shutdown(mut self) {
        for index in 0..self.workers.len() {
            if self.workers[index].manager.is_some() {
                self.stop(index).await;
            }
        }
    }
}

/// Flips one bit of the group's journal records that hold `needle`.
fn damage_journal(fault: &FaultFs, path: &std::path::Path, needle: &[u8]) {
    let journal = path.join("kv").join("journal");
    for segment in fault.list(&journal).unwrap() {
        let bytes = fault.contents(&segment).unwrap();
        if let Some(at) = bytes.windows(needle.len()).position(|w| w == needle) {
            fault
                .flip_bit(&segment, (at + needle.len() / 2) as u64, 1)
                .unwrap();
            return;
        }
    }
    panic!("needle not found in the journal");
}

/// The Timestamp state's authority run ID: every fence record of the group
/// (its Raft entry and its applied state) holds it.
fn needle(cluster: &Cluster) -> Vec<u8> {
    cluster.run_id.to_string().into_bytes()
}

/// NS23 / O06 / O17, section 3 rules 1 to 5, RF3 with real workers. A synced
/// record of group g on one follower is damaged (G1) while that worker is
/// down. After its restart:
/// - the worker stays up and its pool opens with g quarantined; g's replica
///   is refused at start (`storage corrupt`), is never hosted, and the leader
///   sees no replication progress to it while new writes commit;
/// - with a second voter of g stopped the replacement cannot commit, g stays
///   quarantined and nothing is deleted (rule 5);
/// - once that voter is back, the leader replaces the damaged identity with a
///   fresh replica on a fourth worker (`replace_voter`, rule 3); the new
///   replica catches up and holds every acknowledged write;
/// - only then, with that evidence, the damaged worker releases g's bytes
///   (rule 4), and the damaged identity never rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns23_o06_o17_rf3_damaged_replica_is_quarantined_replaced_and_released() {
    timeout(Duration::from_secs(240), async {
        let mut cluster = Cluster::new(4, 3).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let followers: Vec<usize> = (0..3).filter(|index| *index != leader).collect();
        let (damaged, other) = (followers[0], followers[1]);
        let mut term = 0;
        let mut last = None;
        for _ in 0..5 {
            term += 1;
            last = Some(cluster.fence(leader, term).await);
        }
        let acknowledged = term;
        // A fence commits on the leader and one follower. The damaged follower
        // must hold the fence records (the damage needle) before it stops, so
        // wait until it acknowledged the last one: a follower acknowledges an
        // append only once it is durable. A slow runner can otherwise stop it
        // with none of them ("needle not found in the journal", Linux CI).
        let last = last.unwrap().index;
        let damaged_node = damaged as u64 + 1;
        while cluster
            .matched(leader, damaged_node)
            .is_none_or(|index| index < last)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The damaged follower goes down; its journal is damaged (G1).
        cluster.stop(damaged).await;
        let path = cluster.pool_path(damaged);
        damage_journal(&cluster.workers[damaged].fault, &path, &needle(&cluster));
        let refused = cluster.start(damaged, None, false).await.unwrap_err();
        assert!(
            refused.message.starts_with("storage corrupt"),
            "{}",
            refused.message
        );
        let pool = cluster.workers[damaged].pool.clone().unwrap();
        let manager = cluster.workers[damaged].manager.clone().unwrap();
        assert_eq!(pool.quarantined_groups(), vec![cluster.group.group_id]);
        assert!(manager.hosted_replicas(cluster.group.group_id).is_empty());
        assert!(pool.quarantine_refusals() >= 1);
        assert_eq!(
            cluster.workers[damaged]
                .shared
                .storage_stats()
                .quarantined_groups,
            1
        );

        // The group keeps committing without it; the damaged replica never
        // acknowledges again (its matched index stays below the new writes).
        term += 1;
        let after_restart = cluster.fence(leader, term).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            cluster
                .matched(leader, damaged_node)
                .is_none_or(|index| index < after_restart.index),
            "the damaged replica acknowledged an entry after its restart"
        );
        assert!(manager.hosted_replicas(cluster.group.group_id).is_empty());

        // Rule 5: with a second voter stopped the change cannot commit.
        cluster.stop(other).await;
        let fresh = cluster.member(3);
        let blocked = timeout(
            Duration::from_secs(60),
            cluster.replace_damaged(leader, damaged, fresh.clone()),
        )
        .await;
        assert!(
            !matches!(blocked, Ok(Ok(_))),
            "a replacement committed without a quorum"
        );
        assert!(pool.is_group_quarantined(cluster.group.group_id));
        assert_eq!(pool.damage_releases(), 0);

        // The second voter returns; the leader replaces the damaged identity.
        cluster.start(other, None, false).await.unwrap();
        let leader = cluster.leader(&[leader, other]).await;
        let replaced = timeout(
            Duration::from_secs(60),
            cluster.replace_damaged(leader, damaged, fresh.clone()),
        )
        .await
        .expect("replacement finished")
        .unwrap();
        let voters: Vec<u64> = replaced
            .membership
            .replicas()
            .iter()
            .map(|member| member.node_id)
            .collect();
        assert!(voters.contains(&fresh.node_id));
        assert!(!voters.contains(&damaged_node));
        assert_eq!(voters.len(), 3);
        assert_eq!(replaced.proof.removed, cluster.member(damaged));
        assert_eq!(replaced.proof.replacement, fresh);

        // The fresh replica caught up: every acknowledged write is there.
        cluster
            .host(3)
            .raft
            .wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(replaced.membership.applied_at().index), "caught up")
            .await
            .unwrap();
        assert!(cluster.applied_term(3).await > acknowledged);
        term += 1;
        let next = cluster.fence(leader, term).await;
        cluster
            .host(3)
            .raft
            .wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(next.index), "new writes reach the replacement")
            .await
            .unwrap();
        assert_eq!(cluster.applied_term(3).await, term);
        // Idempotent once committed.
        let again = cluster
            .replace_damaged(leader, damaged, fresh.clone())
            .await
            .unwrap();
        assert_eq!(again.proof.removed, cluster.member(damaged));

        // Rule 4: only now are the damaged bytes released.
        assert!(pool.is_group_quarantined(cluster.group.group_id));
        assert_eq!(
            pool.release_damaged_group(replaced.removed.clone())
                .await
                .unwrap(),
            ReleaseOutcome::Released
        );
        assert!(!pool.is_group_quarantined(cluster.group.group_id));
        assert_eq!(pool.damage_releases(), 1);
        // The damaged identity never rejoins: its group record is gone.
        assert!(pool.retained_group(cluster.group.clone()).await.is_err());
        assert!(manager.hosted_replicas(cluster.group.group_id).is_empty());
        drop(pool);
        drop(manager);
        cluster.shutdown().await;
    })
    .await
    .expect("NS23 RF3 watchdog");
}

/// NS23 / O06 rule 5 at RF1: the only replica's group is damaged. The group
/// stays unavailable with `storage corrupt`, its replica never starts, and
/// nothing is deleted across restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns23_o06_rf1_damaged_group_stays_unavailable_and_nothing_is_deleted() {
    timeout(Duration::from_secs(60), async {
        let mut cluster = Cluster::new(1, 1).await;
        let leader = cluster.leader(&[0]).await;
        cluster.fence(leader, 1).await;
        cluster.fence(leader, 2).await;
        cluster.stop(0).await;
        damage_journal(
            &cluster.workers[0].fault,
            &cluster.pool_path(0),
            &needle(&cluster),
        );
        for _ in 0..2 {
            let refused = cluster.start(0, None, false).await.unwrap_err();
            assert!(
                refused.message.starts_with("storage corrupt"),
                "{}",
                refused.message
            );
            assert_eq!(refused.code, ErrorCode::StorageUnavailable);
            let pool = cluster.workers[0].pool.clone().unwrap();
            assert_eq!(pool.quarantined_groups(), vec![cluster.group.group_id]);
            assert_eq!(pool.damage_releases(), 0);
            let manager = cluster.workers[0].manager.clone().unwrap();
            assert!(manager.hosted_replicas(cluster.group.group_id).is_empty());
            drop((pool, manager));
            cluster.stop(0).await;
        }
    })
    .await
    .expect("NS23 RF1 watchdog");
}

struct RecordingSink(std::sync::Mutex<Vec<(GroupIdentity, ReplicaMember)>>);
impl DamageSink for RecordingSink {
    fn damaged(&self, group: GroupIdentity, replica: ReplicaMember, _: OltpPool) {
        self.0.lock().unwrap().push((group, replica));
    }
}

/// NS23 rule 1 at run time: a read meets a damaged run block of the group
/// while its replica runs. The host stops (its route closes and Raft shuts
/// down), the replica is reported to the damage sink, and a restart of the
/// replica is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns23_o06_run_time_damage_stops_the_running_host_and_reports_it() {
    timeout(Duration::from_secs(60), async {
        let mut cluster = Cluster::new(1, 1).await;
        let leader = cluster.leader(&[0]).await;
        for term in 1..=3 {
            cluster.fence(leader, term).await;
        }
        let sink = Arc::new(RecordingSink(Default::default()));
        let manager = cluster.workers[0].manager.clone().unwrap();
        manager.install_damage_sink(sink.clone());
        let pool = cluster.workers[0].pool.clone().unwrap();
        let flushing = pool.clone();
        tokio::task::spawn_blocking(move || flushing.kv_store().flush().unwrap())
            .await
            .unwrap();
        let fault = cluster.workers[0].fault.clone();
        let runs = cluster.pool_path(0).join("kv").join("runs");
        let state = cluster.run_id.to_string().into_bytes();
        // Every block of the group's runs that holds the state is damaged, so
        // the read below meets one whatever the block layout.
        let mut flipped = 0;
        for run in fault.list(&runs).unwrap() {
            let Ok(bytes) = fault.contents(&run) else {
                continue;
            };
            let mut at = 0;
            while let Some(found) = bytes[at..]
                .windows(state.len())
                .position(|w| w == state.as_slice())
            {
                fault.flip_bit(&run, (at + found + 2) as u64, 3).unwrap();
                flipped += 1;
                at += found + state.len();
            }
        }
        assert!(flipped > 0, "the group's state in a run");
        // A block cached before the damage was checked when it was read;
        // evict it so the read below meets the damage on disk (FLAKE1).
        pool.kv_store().evict_cached_blocks();
        let store = cluster.host(0).state.store().clone();
        let read = store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                |view| {
                    let raw = view.get(b"ts/state")?.unwrap();
                    view.copy_output(raw.as_ref())
                },
            )
            .await;
        assert!(read.is_err(), "a damaged block was served");
        drop(store);
        timeout(Duration::from_secs(10), async {
            while !manager.hosted_replicas(cluster.group.group_id).is_empty()
                || sink.0.lock().unwrap().is_empty()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the damaged host stopped and was reported");
        let reported = sink.0.lock().unwrap().clone();
        assert_eq!(reported, vec![(cluster.group.clone(), cluster.member(0))]);
        assert!(pool.is_group_quarantined(cluster.group.group_id));
        let host = cluster.workers[0].host.take().unwrap();
        assert!(
            host.ensure_linearizable().await.is_err(),
            "a stopped host served a read"
        );
        drop(host);
        drop((pool, manager));
        cluster.stop(0).await;
        let refused = cluster.start(0, None, false).await.unwrap_err();
        assert!(
            refused.message.starts_with("storage corrupt"),
            "{}",
            refused.message
        );
        cluster.shutdown().await;
    })
    .await
    .expect("NS23 run-time watchdog");
}

/// RV-APPLY F4, O06, D07 (RF3, real workers): a follower's core stops on a
/// failed apply (a fail-once test hook). The supervisor counts the stop,
/// restarts the core on the same log and state after its backoff, and the
/// follower applies every committed fence; nothing is given up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_d07_rv_apply_fatal_core_is_detected_and_recovered() {
    timeout(Duration::from_secs(120), async {
        let cluster = Cluster::new(3, 3).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let follower = (0..3).find(|index| *index != leader).unwrap();
        cluster.fence(leader, 1).await;
        let host = cluster.host(follower).clone();
        let manager = cluster.workers[follower].manager.clone().unwrap();
        host.retry()
            .fail_applies
            .store(1, std::sync::atomic::Ordering::Release);
        cluster.fence(leader, 2).await;
        timeout(Duration::from_secs(10), async {
            while host.raft.restarts() != 1 || !host.raft.is_running() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the supervisor restarts the stopped core");
        let health = manager.raft_health();
        assert_eq!(
            (
                health.fatal,
                health.fatal_transient,
                health.restarts,
                health.given_up
            ),
            (1, 1, 1, 0),
            "{health:?}"
        );
        assert_eq!(
            manager.hosted_replicas(cluster.group.group_id),
            vec![follower as u64 + 1],
            "the restarted replica stays routed"
        );
        let last = cluster.fence(leader, 3).await;
        host.raft
            .wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(last.index), "the restarted follower catches up")
            .await
            .unwrap();
        assert_eq!(cluster.applied_term(follower).await, 3);
        assert!(host.raft.is_running());
        cluster.shutdown().await;
    })
    .await
    .unwrap();
}

/// Records the replicas the supervisor gave up, with the pool it passed.
#[derive(Default)]
struct GaveUpSink(std::sync::Mutex<Vec<(GroupIdentity, ReplicaMember, OltpPool)>>);
impl DamageSink for GaveUpSink {
    fn damaged(&self, _: GroupIdentity, _: ReplicaMember, _: OltpPool) {}
    fn gave_up(&self, group: GroupIdentity, replica: ReplicaMember, pool: OltpPool) {
        self.0.lock().unwrap().push((group, replica, pool));
    }
}

/// RV-FINAL M4, RV-APPLY F4, O06, O17, NS14b (RF3, real workers): a
/// follower's core keeps stopping (a test hook fails every apply), so the
/// supervisor gives it up. The report carries the replica's open pool (it
/// used to carry none, so nothing was ever released). The group is not
/// quarantined; once the change replacing the replica commits, the release
/// deletes its bytes, counts it and the given-up identity never reopens.
/// Before the fix the release answered `AlreadyReleased` and kept them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_o17_rv_final_m4_given_up_replica_bytes_are_released_after_replacement() {
    timeout(Duration::from_secs(180), async {
        let mut cluster = Cluster::new(4, 3).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let follower = (0..3).find(|index| *index != leader).unwrap();
        cluster.fence(leader, 1).await;
        let sink = Arc::new(GaveUpSink::default());
        let manager = cluster.workers[follower].manager.clone().unwrap();
        manager.install_damage_sink(sink.clone());
        let pool = cluster.workers[follower].pool.clone().unwrap();
        cluster
            .host(follower)
            .retry()
            .fail_applies
            .store(u32::MAX, std::sync::atomic::Ordering::Release);
        cluster.fence(leader, 2).await;
        // Restart backoffs of 1 + 2 + 4 s, then the give-up.
        timeout(Duration::from_secs(60), async {
            while sink.0.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the supervisor gives the replica up");
        let health = manager.raft_health();
        assert_eq!(health.given_up, 1, "{health:?}");
        {
            let reported = sink.0.lock().unwrap();
            let (group, replica, reported_pool) = &reported[0];
            assert_eq!(group, &cluster.group);
            assert_eq!(replica, &cluster.member(follower));
            assert_eq!(reported_pool.identity(), pool.identity());
        }
        sink.0.lock().unwrap().clear();
        assert!(manager.hosted_replicas(cluster.group.group_id).is_empty());
        assert!(!pool.is_group_quarantined(cluster.group.group_id));
        cluster.workers[follower].host = None;

        // Control's replacement (NS14b): the leader replaces the identity.
        let fresh = cluster.member(3);
        let replaced = timeout(
            Duration::from_secs(60),
            cluster.replace_damaged(leader, follower, fresh.clone()),
        )
        .await
        .expect("replacement finished")
        .unwrap();
        assert_eq!(replaced.proof.removed, cluster.member(follower));

        let before = pool.damage_releases();
        let outcome = timeout(Duration::from_secs(30), async {
            loop {
                match pool.release_damaged_group(replaced.removed.clone()).await {
                    // The retired host's last handles are still dropping.
                    Err(error) if error.code == crate::protocol::ErrorCode::Conflict => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    other => break other.unwrap(),
                }
            }
        })
        .await
        .expect("the given-up group's handles close");
        assert_eq!(outcome, ReleaseOutcome::Released);
        assert_eq!(pool.damage_releases(), before + 1);
        // Its bytes are gone: the given-up identity never reopens.
        assert!(pool.retained_group(cluster.group.clone()).await.is_err());
        // Idempotent.
        assert_eq!(
            pool.release_damaged_group(replaced.removed.clone())
                .await
                .unwrap(),
            ReleaseOutcome::AlreadyReleased
        );
        drop(pool);
        drop(manager);
        cluster.shutdown().await;
    })
    .await
    .expect("M4 watchdog");
}

/// RV-APPLY F4, O29, NS09 (RF3, real workers): EIO on a follower's journal sync
/// latches its pool and stops its core. The supervisor classifies it as
/// recovery-required: the replica stops routing and its host shuts down, with
/// no restart loop; the other two voters keep committing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o29_rv_apply_latched_pool_stops_routing_without_a_restart_loop() {
    use nabedra_kv::fs::{Fault, FaultError, FsOp};
    timeout(Duration::from_secs(120), async {
        let cluster = Cluster::new(3, 3).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let follower = (0..3).find(|index| *index != leader).unwrap();
        cluster.fence(leader, 1).await;
        let manager = cluster.workers[follower].manager.clone().unwrap();
        let pool = cluster.workers[follower].pool.clone().unwrap();
        cluster.workers[follower].fault.inject(
            Fault::on(FsOp::SyncData)
                .path_prefix(cluster.pool_path(follower).join("kv").join("journal"))
                .error(FaultError::Eio),
        );
        cluster.fence(leader, 2).await;
        timeout(Duration::from_secs(10), async {
            while !manager.hosted_replicas(cluster.group.group_id).is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the latched replica stops routing");
        assert!(pool.requires_recovery());
        // Longer than every restart backoff: nothing restarts it.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let health = manager.raft_health();
        assert_eq!(
            (
                health.fatal,
                health.fatal_recovery_required,
                health.restarts,
                health.given_up
            ),
            (1, 1, 0, 0),
            "{health:?}"
        );
        assert!(!cluster.host(follower).raft.is_running());
        cluster.fence(leader, 3).await;
        cluster.shutdown().await;
    })
    .await
    .unwrap();
}

/// Waits until replica `index` applied the fenced term `term`.
async fn applied_term_reaches(cluster: &Cluster, index: usize, term: u64) {
    timeout(Duration::from_secs(30), async {
        while cluster.applied_term(index).await != term {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("replica {index} applies term {term}"));
}

/// O29/NS10, PERF2d (RF3, real workers on gated `FaultFs`): the leader's own
/// journal sync is held after its append returned. Both followers sync and
/// acknowledge the entry, so it commits without the leader, but the leader
/// does not count itself and does not apply or acknowledge it before its own
/// sync. Then the leader loses power before that sync: the client is never
/// acknowledged, the followers keep the committed entry and apply it under a
/// new leader, and the old leader catches up from them after its restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o29_ns10_perf2d_leader_power_cut_before_its_sync_keeps_the_quorum_commit() {
    timeout(Duration::from_secs(120), async {
        let mut cluster = Cluster::new_with(3, 3, true).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let followers: Vec<usize> = (0..3).filter(|index| *index != leader).collect();
        let first = cluster.fence(leader, 1).await;
        let gate = cluster.workers[leader].gate.clone().unwrap();
        let held = gate.hold();
        let fence = cluster.fence_task(leader, 2);
        let next = first.index + 1;
        timeout(Duration::from_secs(20), async {
            loop {
                let metrics = cluster.host(leader).raft.metrics().borrow_watched().clone();
                let replicated = followers
                    .iter()
                    .all(|follower| cluster.matched(leader, *follower as u64 + 1) >= Some(next));
                if replicated && metrics.cluster_committed.is_some_and(|id| id.index >= next) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both followers acknowledge and the entry commits");
        assert!(gate.entered() > 0, "the leader's own sync is held");
        assert!(
            cluster.matched(leader, leader as u64 + 1) < Some(next),
            "the leader counted itself before its sync"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !fence.is_finished(),
            "the leader acknowledged before its own sync"
        );
        assert_eq!(cluster.applied_term(leader).await, 1);

        let disk = cluster.workers[leader].fault.crash();
        cluster.workers[leader].fault.halt();
        drop(held);
        let outcome = timeout(Duration::from_secs(20), fence).await;
        assert!(
            !matches!(outcome, Ok(Ok(Ok(_)))),
            "an entry the leader never synced was acknowledged by it"
        );
        cluster.power_cut(leader, disk).await;

        let elected = cluster.leader(&followers).await;
        for follower in &followers {
            applied_term_reaches(&cluster, *follower, 2).await;
        }
        cluster.start(leader, None, false).await.unwrap();
        applied_term_reaches(&cluster, leader, 2).await;
        cluster.fence(elected, 3).await;
        for index in 0..3 {
            applied_term_reaches(&cluster, index, 3).await;
        }
        cluster.shutdown().await;
    })
    .await
    .unwrap();
}

/// O29/NS10, PERF2d (RF3, real workers on gated `FaultFs`): every replica's
/// sync is held, so the entry is pending on all three and none acknowledges
/// it. A power cut of all three loses it everywhere: it was never committed
/// or acknowledged, and the restarted group fences the same term again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o29_ns10_perf2d_power_cut_before_any_sync_loses_the_unacked_entry() {
    timeout(Duration::from_secs(120), async {
        let mut cluster = Cluster::new_with(3, 3, true).await;
        let leader = cluster.leader(&[0, 1, 2]).await;
        let first = cluster.fence(leader, 1).await;
        let gates: Vec<_> = (0..3)
            .map(|index| cluster.workers[index].gate.clone().unwrap())
            .collect();
        let held: Vec<_> = gates.iter().map(|gate| gate.hold()).collect();
        let fence = cluster.fence_task(leader, 2);
        timeout(Duration::from_secs(20), async {
            while gates.iter().any(|gate| gate.entered() == 0) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every replica reaches its held sync");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let metrics = cluster.host(leader).raft.metrics().borrow_watched().clone();
        assert!(
            metrics
                .cluster_committed
                .is_none_or(|id| id.index <= first.index),
            "committed without a synced quorum: {:?}",
            metrics.cluster_committed
        );
        assert!(!fence.is_finished());

        let disks: Vec<_> = (0..3)
            .map(|index| cluster.workers[index].fault.crash())
            .collect();
        for index in 0..3 {
            cluster.workers[index].fault.halt();
        }
        drop(held);
        let outcome = timeout(Duration::from_secs(20), fence).await;
        assert!(!matches!(outcome, Ok(Ok(Ok(_)))));
        for (index, disk) in disks.into_iter().enumerate() {
            cluster.power_cut(index, disk).await;
        }
        for index in 0..3 {
            cluster.start(index, None, false).await.unwrap();
        }
        let leader = cluster.leader(&[0, 1, 2]).await;
        for index in 0..3 {
            applied_term_reaches(&cluster, index, 1).await;
        }
        // Fencing term 2 again expects term 1: the lost entry never applied.
        cluster.fence(leader, 2).await;
        for index in 0..3 {
            applied_term_reaches(&cluster, index, 2).await;
        }
        cluster.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_follower_sync_error_cannot_complete_a_quorum_with_a_delayed_follower() {
    use nabedra_kv::fs::{Fault, FaultError, FsOp};

    let cluster = Cluster::new_with(3, 3, true).await;
    let leader = cluster.leader(&[0, 1, 2]).await;
    let followers: Vec<_> = (0..3).filter(|node| *node != leader).collect();
    let (delayed, failing) = (followers[0], followers[1]);
    let first = cluster.fence(leader, 1).await;
    for follower in &followers {
        applied_term_reaches(&cluster, *follower, 1).await;
    }
    let gate = cluster.workers[delayed].gate.clone().unwrap();
    gate.hold();
    let failing_pool = cluster.workers[failing].pool.clone().unwrap();
    cluster.workers[failing].fault.inject(
        Fault::on(FsOp::SyncData)
            .path_prefix(cluster.pool_path(failing).join("kv/journal"))
            .error(FaultError::Eio),
    );
    let proposal = cluster.fence_task(leader, 2);
    let held = timeout(Duration::from_secs(15), async {
        while gate.entered() == 0 || !failing_pool.requires_recovery() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let metrics = cluster.host(leader).raft.metrics().borrow_watched().clone();
        (
            !proposal.is_finished(),
            metrics
                .cluster_committed
                .is_some_and(|id| id.index == first.index),
            metrics
                .last_applied
                .is_some_and(|id| id.index == first.index),
            followers.iter().all(|node| {
                cluster
                    .matched(leader, *node as u64 + 1)
                    .is_none_or(|index| index <= first.index)
            }),
        )
    })
    .await;
    // Always unblock the journal thread before assertions can unwind.
    gate.release();
    assert_eq!(
        held.expect("both follower syncs reached their fault"),
        (true, true, true, true)
    );
    let applied = timeout(Duration::from_secs(15), proposal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(applied.index, first.index + 1);
    applied_term_reaches(&cluster, delayed, 2).await;
    assert_eq!(
        cluster
            .host(leader)
            .ensure_linearizable()
            .await
            .unwrap()
            .applied,
        applied
    );
    assert!(failing_pool.requires_recovery());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_foreign_pool_and_group_identity_are_refused() {
    let cluster = Cluster::new(3, 3).await;
    let index = 0;
    let pool = cluster.workers[index].pool.as_ref().unwrap();
    let mut foreign = cluster.group.clone();
    foreign.cluster_id = Uuid::new_v4();
    assert!(pool.retained_group(foreign).await.is_err());
    let path = cluster.pool_path(index);
    let identity = cluster.pool_identity(index);
    let shared = cluster.workers[index].shared.clone();
    let mut cluster = cluster;
    cluster.stop(index).await;
    let mut foreign = identity.clone();
    foreign.pool_id = cluster.pool_identity(1).pool_id;
    let fs: Arc<dyn Fs> = Arc::new(cluster.workers[index].fault.clone());
    assert!(OltpPool::open_development_on(&path, foreign, shared.clone(), fs.clone()).is_err());
    let exact = OltpPool::open_development_on(path, identity, shared, fs).unwrap();
    assert!(exact.retained_group(cluster.group.clone()).await.is_ok());
    cluster.shutdown().await;
}

pub(crate) fn acceptance_o06_storage() {
    o06_follower_sync_error_cannot_complete_a_quorum_with_a_delayed_follower();
    crate::oltp_acceptance::clause(
        "O06",
        "stable-storage-before-ack",
        "passed",
        "An RF3 mTLS FaultFs fixture held one follower at journal sync and failed the other with EIO. The leader could neither commit, apply nor reply until the held follower synced.",
    );
    ns23_o06_o17_rf3_damaged_replica_is_quarantined_replaced_and_released();
    o06_foreign_pool_and_group_identity_are_refused();
    crate::oltp::pool::acceptance_damaged_checkpoints();
    crate::oltp_acceptance::clause(
        "O06",
        "corrupt-and-foreign-rejection",
        "passed",
        "The RF3 storage fixture quarantined a corrupted voter log and refused foreign pool and group identities. The pool also rejected corrupt checkpoint membership and an inconsistent header.",
    );
}

pub(crate) fn acceptance_overlapped_append() {
    o29_ns10_perf2d_power_cut_before_any_sync_loses_the_unacked_entry();
    o29_ns10_perf2d_leader_power_cut_before_its_sync_keeps_the_quorum_commit();
}
