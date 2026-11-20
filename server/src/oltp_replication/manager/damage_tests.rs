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
        let fs: Arc<dyn Fs> = Arc::new(worker.fault.clone());
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
                |view| {
                    let raw = view.get(b"ts/state")?.unwrap();
                    view.copy_output(raw.as_ref())
                },
            )
            .await
            .unwrap();
        let saved: serde_json::Value = serde_json::from_slice(saved.as_slice()).unwrap();
        saved["authority"]["owner_term"].as_u64().unwrap()
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
        for _ in 0..5 {
            term += 1;
            cluster.fence(leader, term).await;
        }
        let acknowledged = term;

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
        let damaged_node = damaged as u64 + 1;
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
            cluster
                .host(leader)
                .replace_damaged_voter(&cluster.member(damaged), fresh.clone()),
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
            cluster
                .host(leader)
                .replace_damaged_voter(&cluster.member(damaged), fresh.clone()),
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
            .host(leader)
            .replace_damaged_voter(&cluster.member(damaged), fresh.clone())
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
