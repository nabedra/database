//! NS23 end to end with Control doing the replacement (NS14b). Five real
//! workers in one process, each with its own mTLS listener, transport,
//! hosted-group manager and pool on `FaultFs`:
//!
//! - Control is RF3 on workers 0, 1 and 3, each with a `ControlHandle` over
//!   `ForwardedControl`, the hosted Control endpoints and a `DamageScheduler`;
//! - Data group g is RF3 on workers 0, 1 and 2; worker 2's replica is damaged;
//! - every worker serves `Purpose::GroupDamage` and reports damage to Control
//!   through its own `ControlClient` (`ReportingDamageSink`, `spawn_settlement`).
//!
//! The test injects the damage and, for run-time damage, the read that finds
//! it. Everything after that is production code: the report, the plan, the
//! fresh learner, the leader's `replace_damaged_voter`, the publication and
//! the release. The damage is a `FaultFs` fault model, not a power cut.
#[path = "../../../../tests/support/tls.rs"]
mod tls;
use nabedra_resources::config::WorkerResources;
use super::*;
use crate::{
    oltp::{
        mvcc::store::{KeySpan, ParticipantHandle},
        pool::{OltpPool, PoolIdentity, SharedResourceConfig, TransactionBudget},
    },
    oltp_replication::{
        authority::control::{
            ControlClient, ControlEndpoint, ControlHandle, ControlRoutes, ForwardedControl,
            GroupDamageGate, GroupDamagePhase, GroupDamageReason, GroupDamageRecord,
            GroupDamageReport, GroupRecord, HostedControlEndpoint, LocalOperatorPermit,
            OperatorPolicy, ReplicaInventoryPage, VerifiedMembership,
        },
        multiplex::{
            AuthenticatedSession, PeerDirectory, ResidentGroup, TransportConfig, TransportTrust,
            WorkerTransport,
        },
    },
};
use nabedra_kv::fs::{FaultFs, Fs};
use openraft::RaftLogReader;
use std::{collections::BTreeSet, path::PathBuf, time::Duration};
use nabedra_rt::time::timeout;
use tokio::{net::TcpListener, sync::watch};
use uuid::Uuid;

const WORKERS: usize = 5;
const CONTROL: [usize; 3] = [0, 1, 3];
const DATA: [usize; 3] = [0, 1, 2];
const DAMAGED: usize = 2;
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 64 << 10,
    batch_bytes: 0,
    output_bytes: 16 << 10,
};

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

/// The Control workers, in worker order.
struct Routes(Vec<Uuid>);
impl ControlRoutes for Routes {
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(self
            .0
            .iter()
            .copied()
            .filter(|worker| after.is_none_or(|after| *worker > after))
            .min())
    }
}

/// The test installs an open gate; `DirectCapabilityDamageGate` is the
/// production predicate.
struct Open;
impl GroupDamageGate for Open {
    fn group_damage_supported(&self) -> bool {
        true
    }
}

/// One worker's side of the scheduler's RPCs: its single pool.
struct Host {
    manager: Arc<HostedGroupManager>,
    pool: OltpPool,
    control: Arc<ControlClient>,
    pins: BTreeSet<Digest>,
    offers: bool,
}
impl DamageHost for Host {
    fn manager(&self) -> &Arc<HostedGroupManager> {
        &self.manager
    }
    fn raft_config(&self) -> openraft::Config {
        config()
    }
    fn control(&self) -> &dyn DamageControl {
        &*self.control
    }
    fn authorized(&self, peer: &WorkerIdentity) -> bool {
        self.pins.contains(&peer.certificate_sha256)
    }
    fn placement_pools(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<OltpPool>, ProtocolError>> + Send + '_>> {
        let pools = if self.offers {
            vec![self.pool.clone()]
        } else {
            Vec::new()
        };
        Box::pin(async move { Ok(pools) })
    }
    fn wire(&self, _: &GroupIdentity, _: u64, _: ManagedData) -> Result<(), ProtocolError> {
        Ok(())
    }
}

struct Running {
    pool: OltpPool,
    manager: Arc<HostedGroupManager>,
    control: Option<(ManagedControl, Arc<ControlHandle>, Arc<LocalOperatorPermit>)>,
    data: Option<ManagedData>,
    scheduler: Option<Arc<DamageScheduler>>,
    stop: watch::Sender<bool>,
    transport: tokio::task::JoinHandle<Result<(), ProtocolError>>,
    _control_group: ResidentGroup,
}

struct Worker {
    fault: FaultFs,
    shared: Arc<WorkerSharedResources>,
    running: Option<Running>,
    offers: bool,
}

struct Cluster {
    dir: Temporary,
    tls: Vec<crate::oltp_replication::TlsMaterial>,
    enrollments: Vec<WorkerEnrollment>,
    pools: Vec<Uuid>,
    directory: Arc<Directory>,
    control: GroupIdentity,
    group: GroupIdentity,
    pins: BTreeSet<Digest>,
    workers: Vec<Worker>,
    run_id: Uuid,
    term: u64,
}

fn config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}

impl Cluster {
    /// Starts every worker, Control and g, registers g in Control and makes
    /// `writes` acknowledged writes to g. Workers in `silent` offer no pool.
    async fn new(writes: u64, silent: &[usize]) -> Self {
        let dir = Temporary(std::env::temp_dir().join(format!("ns14b-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        let tls = tls::certificates(&dir.0, WORKERS as u64);
        let cluster = nabedra_rt::ids::uuid_v4();
        let database = Text::try_from("ns14b").unwrap();
        let control = GroupIdentity {
            cluster_id: cluster,
            database: database.clone(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            group_format_version: 1,
        };
        let group = GroupIdentity {
            cluster_id: cluster,
            database,
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Data {
                table_id: nabedra_rt::ids::uuid_v4(),
                shard_id: nabedra_rt::ids::uuid_v4(),
            },
            group_format_version: 1,
        };
        let mut listeners = Vec::new();
        let mut enrollments = Vec::new();
        for material in &tls {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            enrollments.push(WorkerEnrollment {
                worker_id: nabedra_rt::ids::uuid_v4(),
                incarnation: nabedra_rt::ids::uuid_v4(),
                certificate_sha256: Digest::parse(material.certificate_sha256()).unwrap(),
                enrollment_revision: 1,
                endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            });
            listeners.push(listener);
        }
        let directory = Arc::new(Directory {
            cluster,
            workers: enrollments
                .iter()
                .cloned()
                .map(|worker| (worker.worker_id, worker))
                .collect(),
        });
        let pins = enrollments
            .iter()
            .map(|worker| worker.certificate_sha256)
            .collect();
        let mut cluster = Self {
            dir,
            tls,
            pools: (0..WORKERS).map(|_| nabedra_rt::ids::uuid_v4()).collect(),
            enrollments,
            directory,
            control,
            group,
            pins,
            workers: Vec::new(),
            run_id: nabedra_rt::ids::uuid_v4(),
            term: 0,
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
                running: None,
                offers: !silent.contains(&index),
            });
            cluster.start(index, Some(listener), true).await.unwrap();
        }
        cluster
            .running(CONTROL[0])
            .control
            .as_ref()
            .unwrap()
            .0
            .raft
            .initialize(cluster.control_members())
            .await
            .unwrap();
        cluster
            .running(DATA[0])
            .data
            .as_ref()
            .unwrap()
            .raft
            .initialize(cluster.data_members())
            .await
            .unwrap();
        cluster.bootstrap_control().await;
        let leader = cluster.data_leader().await;
        cluster.fence_initial(leader).await;
        for _ in 1..writes {
            cluster.fence(leader).await;
        }
        cluster
    }

    fn pool_path(&self, index: usize) -> PathBuf {
        self.dir.0.join(format!("pool{index}"))
    }
    fn identity(&self, index: usize) -> WorkerIdentity {
        WorkerIdentity::from(&self.enrollments[index])
    }
    fn member(&self, members: &[usize], index: usize) -> ReplicaMember {
        let position = members.iter().position(|worker| *worker == index).unwrap();
        ReplicaMember {
            node_id: position as u64 + 1,
            worker_id: self.enrollments[index].worker_id,
            pool_id: self.pools[index],
        }
    }
    fn members(&self, members: &[usize]) -> BTreeMap<u64, ReplicaMember> {
        members
            .iter()
            .map(|index| {
                let member = self.member(members, *index);
                (member.node_id, member)
            })
            .collect()
    }
    fn control_members(&self) -> BTreeMap<u64, ReplicaMember> {
        self.members(&CONTROL)
    }
    fn data_members(&self) -> BTreeMap<u64, ReplicaMember> {
        self.members(&DATA)
    }
    fn damaged(&self) -> ReplicaMember {
        self.member(&DATA, DAMAGED)
    }
    fn running(&self, index: usize) -> &Running {
        self.workers[index].running.as_ref().unwrap()
    }
    fn policy(&self) -> OperatorPolicy {
        OperatorPolicy {
            policy: Digest::from_bytes([7; 32]),
            certificate_pins: self.pins.clone(),
        }
    }

    /// Opens worker `index`'s pool, transport, Control replica (if it hosts
    /// one), damage endpoint and reporter, and its replica of g. A replica of
    /// a quarantined group is not started: it is reported to Control, as the
    /// data factory's rehost does.
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
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: self.group.cluster_id,
            worker_id: self.enrollments[index].worker_id,
            pool_id: self.pools[index],
        };
        let path = self.pool_path(index);
        let shared = self.workers[index].shared.clone();
        let fs: Arc<dyn Fs> = Arc::new(self.workers[index].fault.clone());
        let pool = OltpPool::open_development_on(&path, identity, shared.clone(), fs)?;
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let transport = Arc::new(
            WorkerTransport::new(
                &self.enrollments[index],
                &self.tls[index],
                shared.clone(),
                self.directory.clone(),
                registry.clone(),
                TransportConfig::default(),
            )
            .unwrap(),
        );
        let manager = HostedGroupManager::new(shared.clone(), transport.client(), registry);
        let (stop, mut stopped) = watch::channel(false);
        let transport = nabedra_rt::task::spawn(transport.run(listener, async move {
            let _ = stopped.wait_for(|stop| *stop).await;
        }));
        if create {
            let flushing = pool.clone();
            nabedra_rt::task::spawn_blocking(move || flushing.kv_store().flush().unwrap())
                .await
                .unwrap();
        }
        // The reporter is installed before any group starts (NS14b).
        let settling: SettlingReplicas = Default::default();
        let sink = ReportingDamageSink::new(settling.clone());
        manager.install_damage_sink(sink.clone());
        let routes = CONTROL
            .iter()
            .map(|worker| self.enrollments[*worker].worker_id)
            .collect();
        let rpc = ControlClient::new(
            manager.client(),
            shared.clone(),
            self.control.clone(),
            Arc::new(Routes(routes)),
        )?;
        sink.connect(&rpc);
        let resident = manager.registry().reserve(self.control.clone())?;
        let control = if CONTROL.contains(&index) {
            let store = if create {
                pool.group(self.control.clone()).await?
            } else {
                pool.retained_group(self.control.clone()).await?
            };
            let raw = manager
                .start_control(
                    store,
                    self.member(&CONTROL, index),
                    self.control_members(),
                    config(),
                )
                .await?;
            let handle = ControlHandle::new(
                crate::oltp_replication::authority::control::ControlMachine::new(
                    raw.state.store().clone(),
                )?,
                ForwardedControl::new(manager.clone(), raw.clone(), self.pins.clone())?,
                self.policy(),
            );
            handle.install_group_damage_gate(Arc::new(Open));
            let reads = ControlEndpoint::new(handle.clone(), shared.clone())?;
            let hosted =
                HostedControlEndpoint::new(reads, raw.clone(), shared.clone(), self.pins.clone())?;
            resident.bind(Purpose::Authority, hosted.clone())?;
            resident.bind(Purpose::ControlDelegation, hosted)?;
            let permit = Arc::new(LocalOperatorPermit::from_local_bootstrap(
                self.identity(index),
                shared.clone(),
                Arc::new(()),
            )?);
            Some((raw, handle, permit))
        } else {
            None
        };
        let endpoint = DamageReplacementEndpoint::new(
            Arc::new(Host {
                manager: manager.clone(),
                pool: pool.clone(),
                control: rpc.clone(),
                pins: self.pins.clone(),
                offers: self.workers[index].offers,
            }),
            self.control.clone(),
        );
        resident.bind(Purpose::GroupDamage, endpoint.clone())?;
        let scheduler = control.as_ref().map(|(raw, handle, permit)| {
            let scheduler = DamageScheduler::new(
                handle.clone(),
                raw.clone(),
                permit.clone(),
                endpoint.clone(),
                Duration::from_millis(200),
            );
            scheduler.clone().spawn(stop.subscribe());
            scheduler
        });
        let mut data = None;
        let mut refused = None;
        if DATA.contains(&index) {
            let store = if create {
                pool.group(self.group.clone()).await
            } else {
                pool.retained_group(self.group.clone()).await
            };
            let started = match store {
                Ok(store) => {
                    manager
                        .start_data(
                            store,
                            self.member(&DATA, index),
                            self.data_members(),
                            config(),
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            match started {
                Ok(running) => data = Some(running),
                Err(error) => {
                    if let Some(damage) = pool.group_damage(self.group.group_id) {
                        spawn_settlement(
                            rpc.clone(),
                            settling.clone(),
                            GroupDamageReport {
                                group: self.group.clone(),
                                replica: self.member(&DATA, index),
                                reason: damage.cause.into(),
                                first_bad_seq: damage.first_bad_seq,
                                refusal: None,
                            },
                            Some(pool.clone()),
                        );
                    }
                    refused = Some(error);
                }
            }
        }
        self.workers[index].running = Some(Running {
            pool,
            manager,
            control,
            data,
            scheduler,
            stop,
            transport,
            _control_group: resident,
        });
        refused.map_or(Ok(()), Err)
    }

    /// Stops worker `index` like a process exit.
    async fn stop(&mut self, index: usize) {
        let Some(running) = self.workers[index].running.take() else {
            return;
        };
        let Running {
            stop,
            manager,
            transport,
            ..
        } = running;
        let _ = stop.send(true);
        manager.shutdown().await.unwrap();
        let _ = transport.await;
        let worker = &mut self.workers[index];
        worker.fault = worker.fault.kill();
    }

    async fn shutdown(mut self) {
        for index in 0..WORKERS {
            self.stop(index).await;
        }
    }

    /// Any live Control handle (proposals and reads forward to the leader).
    fn handle(&self) -> (Arc<ControlHandle>, Arc<LocalOperatorPermit>) {
        CONTROL
            .iter()
            .find_map(|index| {
                self.workers[*index]
                    .running
                    .as_ref()
                    .and_then(|running| running.control.as_ref())
                    .map(|(_, handle, permit)| (handle.clone(), permit.clone()))
            })
            .unwrap()
    }

    async fn retry<T, F, R>(what: &str, mut step: F) -> T
    where
        F: FnMut() -> R,
        R: Future<Output = Result<T, ProtocolError>>,
    {
        timeout(Duration::from_secs(30), async {
            loop {
                match step().await {
                    Ok(value) => return value,
                    Err(error) => {
                        tracing::debug!(what, %error, "retrying");
                        nabedra_rt::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what} timed out"))
    }

    /// Initializes Control, enrolls every worker and registers g.
    async fn bootstrap_control(&self) {
        let (handle, permit) = self.handle();
        let run = nabedra_rt::ids::uuid_v4();
        Self::retry("initialize Control", || {
            handle.initialize_local(&permit, run, run)
        })
        .await;
        let operator = self.identity(CONTROL[0]);
        for index in 0..WORKERS {
            let operation = nabedra_rt::ids::uuid_v4();
            let next = self.enrollments[index].clone();
            let group = self.control.clone();
            Self::retry("enroll", || {
                let handle = handle.clone();
                let next = next.clone();
                let session = AuthenticatedSession::for_test(
                    operator,
                    operator,
                    group.clone(),
                    Purpose::Authority,
                    nabedra_rt::ids::uuid_v4(),
                );
                async move { handle.enroll(&session, operation, None, next, None).await }
            })
            .await;
        }
        let leader = self.data_leader().await;
        let observation = Self::retry("observe g", || {
            self.running(leader)
                .data
                .as_ref()
                .unwrap()
                .observe_membership()
        })
        .await;
        let authority = handle
            .inspect()
            .await
            .unwrap()
            .unwrap()
            .ready
            .view
            .authority();
        let generation = nabedra_rt::ids::uuid_v4();
        let operation = nabedra_rt::ids::uuid_v4();
        Self::retry("begin g", || {
            handle.begin_group_local(&permit, operation, generation, self.group.clone())
        })
        .await;
        let page = Page::new(self.data_members().into_values().collect()).unwrap();
        let operation = nabedra_rt::ids::uuid_v4();
        Self::retry("page g", || {
            handle.group_page_local(&permit, operation, generation, page.clone())
        })
        .await;
        let proof = VerifiedMembership::from_consensus(observation, generation, authority).unwrap();
        handle
            .seal_group_local(&permit, nabedra_rt::ids::uuid_v4(), proof)
            .await
            .unwrap();
        assert_eq!(
            self.placement().await.1,
            self.data_members().into_values().collect::<Vec<_>>()
        );
    }

    /// The published placement of g and its members.
    async fn placement(&self) -> (GroupRecord, Vec<ReplicaMember>) {
        let (handle, _) = self.handle();
        let record = handle.group(self.group.group_id).await.unwrap().unwrap();
        let raw = handle.group_members(record.clone(), None).await.unwrap();
        let page: ReplicaInventoryPage = serde_json::from_slice(raw.as_slice()).unwrap();
        assert!(page.done);
        (record, page.items.iter().cloned().collect())
    }

    async fn record(&self) -> Option<GroupDamageRecord> {
        let (handle, _) = self.handle();
        handle
            .group_damage(self.group.group_id, self.damaged().node_id)
            .await
            .unwrap()
    }

    /// The replica of g on `index`, also a fresh one the scheduler hosted.
    fn data_host(&self, index: usize) -> Option<ManagedData> {
        let running = self.workers[index].running.as_ref()?;
        if let Some(data) = &running.data {
            return Some(data.clone());
        }
        running
            .manager
            .hosted_replicas(self.group.group_id)
            .into_iter()
            .find_map(|node| running.manager.hosted_data(&self.group, node).ok())
    }

    async fn data_leader(&self) -> usize {
        timeout(Duration::from_secs(30), async {
            loop {
                for index in 0..WORKERS {
                    if let Some(host) = self.data_host(index)
                        && host.ensure_linearizable().await.is_ok()
                    {
                        return index;
                    }
                }
                nabedra_rt::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a Data leader with a quorum")
    }

    fn authority(&self, term: u64) -> Authority {
        Authority {
            run_id: self.run_id,
            owner_term: term,
        }
    }
    fn participant(&self, leader: usize) -> ParticipantHandle {
        let host = self.data_host(leader).unwrap();
        ParticipantHandle::new(
            crate::oltp::mvcc::store::ParticipantMachine::new(host.state.store().clone()).unwrap(),
            host,
        )
    }
    async fn fence_initial(&mut self, leader: usize) {
        self.term = 1;
        self.participant(leader)
            .fence_initial(
                nabedra_rt::ids::uuid_v4(),
                self.authority(1),
                nabedra_rt::ids::uuid_v4(),
                1,
                1,
                KeySpan {
                    start: None,
                    end: None,
                },
            )
            .await
            .unwrap();
    }
    /// One acknowledged write: fence g to the next term.
    async fn fence(&mut self, leader: usize) -> AppliedPosition {
        let receipt = self
            .participant(leader)
            .fence_current(
                nabedra_rt::ids::uuid_v4(),
                self.authority(self.term),
                self.authority(self.term + 1),
            )
            .await
            .unwrap();
        self.term += 1;
        receipt.applied_at
    }

    /// The fenced term a replica applied.
    async fn applied_term(host: &ManagedData) -> u64 {
        let raw = host
            .state
            .store()
            .read_metadata(BUDGET, |view| {
                let raw = view.get(b"mvcc/config/v1")?.unwrap();
                view.copy_output(raw.as_ref())
            })
            .await
            .unwrap();
        let config: serde_json::Value = serde_json::from_slice(raw.as_slice()).unwrap();
        config["authority"]["owner_term"].as_u64().unwrap()
    }

    fn needle(&self) -> Vec<u8> {
        self.run_id.to_string().into_bytes()
    }

    /// Waits until Control's record of the damaged replica is replaced.
    async fn replaced(&self, within: Duration) -> GroupDamageRecord {
        timeout(within, async {
            loop {
                if let Ok(Some(record)) = timeout(Duration::from_secs(5), self.record()).await
                    && matches!(record.phase, GroupDamagePhase::Replaced { .. })
                {
                    return record;
                }
                nabedra_rt::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Control replaced the damaged replica")
    }

    /// Rule 3 and 4 after Control's replacement: the published placement and
    /// the Raft voters exclude the damaged replica, the fresh replica holds
    /// every acknowledged write and new writes, and only then the damaged
    /// worker releases g's bytes.
    async fn check_replaced(
        &mut self,
        record: &GroupDamageRecord,
        acknowledged: u64,
    ) -> ReplicaMember {
        let GroupDamagePhase::Replaced {
            replacement,
            committed_at,
            published: Some(published),
        } = record.phase.clone()
        else {
            panic!("unexpected phase {:?}", record.phase);
        };
        let damaged = self.damaged();
        assert_eq!(record.report.replica, damaged);
        let (current, members) = self.placement().await;
        assert_eq!(current, *published);
        assert_eq!(current.membership_position, committed_at);
        assert!(
            !members.contains(&damaged),
            "routes still list the damaged replica"
        );
        assert!(members.contains(&replacement));
        assert_eq!(members.len(), 3);
        assert_ne!(replacement.node_id, damaged.node_id);
        assert_ne!(replacement.pool_id, damaged.pool_id);
        let target = self
            .enrollments
            .iter()
            .position(|worker| worker.worker_id == replacement.worker_id)
            .unwrap();
        let fresh = self
            .running(target)
            .manager
            .hosted_data(&self.group, replacement.node_id)
            .unwrap();
        let leader = self.data_leader().await;
        let voters = self
            .data_host(leader)
            .unwrap()
            .observe_membership()
            .await
            .unwrap();
        let voters: Vec<ReplicaMember> = voters.replicas().to_vec();
        assert_eq!(voters, members, "Raft voters equal the published placement");
        fresh
            .raft
            .wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(committed_at.index), "caught up")
            .await
            .unwrap();
        assert!(
            Self::applied_term(&fresh).await >= acknowledged,
            "an acknowledged write is missing"
        );
        let next = self.fence(leader).await;
        fresh
            .raft
            .wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(next.index), "new writes reach the replacement")
            .await
            .unwrap();
        assert_eq!(Self::applied_term(&fresh).await, self.term);
        // Rule 4: the damaged worker releases g only from the replaced record.
        let pool = self.running(DAMAGED).pool.clone();
        // The reporter polls Control with a backoff of up to 30 s.
        timeout(Duration::from_secs(90), async {
            while pool.is_group_quarantined(self.group.group_id) {
                nabedra_rt::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the damaged worker released g");
        assert_eq!(pool.damage_releases(), 1);
        assert!(pool.retained_group(self.group.clone()).await.is_err());
        replacement
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, "invalid", Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "unavailable",
        Outcome::NotApplied,
    )
}

/// Flips one bit in every journal record of `path` that holds `needle`.
fn damage_journal(fault: &FaultFs, path: &std::path::Path, needle: &[u8], every: bool) -> usize {
    let journal = path.join("kv").join("journal");
    let mut flipped = 0;
    for segment in fault.list(&journal).unwrap() {
        let bytes = fault.contents(&segment).unwrap();
        let mut at = 0;
        while let Some(found) = bytes[at..].windows(needle.len()).position(|w| w == needle) {
            fault
                .flip_bit(&segment, (at + found + needle.len() / 2) as u64, 1)
                .unwrap();
            flipped += 1;
            if !every {
                return flipped;
            }
            at += found + needle.len();
        }
    }
    flipped
}

/// Flips bits in every run block of `path` that holds `needle`.
fn damage_runs(fault: &FaultFs, path: &std::path::Path, needle: &[u8]) -> usize {
    let runs = path.join("kv").join("runs");
    let mut flipped = 0;
    for run in fault.list(&runs).unwrap() {
        let Ok(bytes) = fault.contents(&run) else {
            continue;
        };
        let mut at = 0;
        while let Some(found) = bytes[at..].windows(needle.len()).position(|w| w == needle) {
            fault.flip_bit(&run, (at + found + 2) as u64, 3).unwrap();
            flipped += 1;
            at += found + needle.len();
        }
    }
    flipped
}

/// NS23 / O06 / O17 with Control doing the replacement, G1 and rule 5. A
/// synced journal record of g on worker 2 is damaged while it is down, and a
/// second voter of g (worker 1) is down too. At restart worker 2 reports the
/// quarantine; Control plans the replacement and hosts the fresh learner, but
/// the change cannot commit, so g stays quarantined and nothing is released.
/// Once worker 1 returns, Control finishes on its own: the new placement is
/// published without the damaged replica, the fresh replica holds every
/// acknowledged write, and only then worker 2 releases g's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns23_o06_o17_ns14b_g1_damage_is_replaced_by_control_and_rule5_holds_without_quorum() {
    timeout(Duration::from_secs(300), async {
        let mut cluster = Cluster::new(5, &[]).await;
        let acknowledged = cluster.term;
        cluster.stop(DAMAGED).await;
        cluster.stop(1).await;
        let flipped = damage_journal(
            &cluster.workers[DAMAGED].fault,
            &cluster.pool_path(DAMAGED),
            &cluster.needle(),
            false,
        );
        assert_eq!(flipped, 1);
        let refused = cluster.start(DAMAGED, None, false).await.unwrap_err();
        assert!(
            refused.message.starts_with("storage corrupt"),
            "{}",
            refused.message
        );
        let pool = cluster.running(DAMAGED).pool.clone();
        assert_eq!(pool.quarantined_groups(), vec![cluster.group.group_id]);

        // Control plans the replacement, but without a quorum of g that
        // excludes the damaged replica the change never commits (rule 5).
        let (before, _) = cluster.placement().await;
        let plan = timeout(Duration::from_secs(60), async {
            loop {
                if let Some(record) = cluster.record().await
                    && let Some(plan) = record.plan()
                {
                    return plan.clone();
                }
                nabedra_rt::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Control planned the replacement");
        assert_eq!(plan.damaged, cluster.damaged());
        assert_eq!(plan.expected, before);
        assert!(
            [
                cluster.enrollments[3].worker_id,
                cluster.enrollments[4].worker_id
            ]
            .contains(&plan.replacement.worker_id),
            "the fresh replica is on a worker that hosts no member of g"
        );
        nabedra_rt::time::sleep(Duration::from_secs(5)).await;
        let held = cluster.record().await.unwrap();
        assert!(
            matches!(held.phase, GroupDamagePhase::Replacing { .. }),
            "{:?}",
            held.phase
        );
        assert_eq!(
            cluster.placement().await.0,
            before,
            "nothing published without a quorum"
        );
        assert!(pool.is_group_quarantined(cluster.group.group_id));
        assert_eq!(pool.damage_releases(), 0);

        // The second voter returns; Control finishes the same plan on its own.
        cluster.start(1, None, false).await.unwrap();
        let record = cluster.replaced(Duration::from_secs(150)).await;
        assert_eq!(record.report.reason, GroupDamageReason::JournalRecord);
        let replacement = cluster.check_replaced(&record, acknowledged).await;
        assert_eq!(replacement, plan.replacement, "Control kept its first plan");
        cluster.shutdown().await;
    })
    .await
    .expect("NS14b G1 watchdog");
}

#[derive(Clone, Copy, Debug)]
enum RunTime {
    /// G2: a run block of g.
    RunBlock,
    /// G3: a journal value (a Raft entry body) of g.
    JournalValue,
}

/// Damages g on worker 2 while it runs and makes worker 2 read the damaged
/// bytes, so kv quarantines g and the manager stops and reports the replica.
async fn damage_at_run_time(cluster: &mut Cluster, kind: RunTime) {
    // FLAKE1: a fence is acknowledged once a quorum applied it, so worker 2
    // may apply the last one later. Applied after the flush, it writes g's
    // next config to the memtable (the read below is then served from there)
    // and caches the run block it read. Wait until worker 2 applied every
    // acknowledged write: after that nothing writes g's config.
    let host = cluster.running(DAMAGED).data.clone().unwrap();
    let term = cluster.term;
    timeout(Duration::from_secs(60), async {
        while Cluster::applied_term(&host).await != term {
            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker 2 applied every acknowledged write");
    drop(host);
    let flushing = cluster.running(DAMAGED).pool.clone();
    nabedra_rt::task::spawn_blocking(move || flushing.kv_store().flush().unwrap())
        .await
        .unwrap();
    let path = cluster.pool_path(DAMAGED);
    match kind {
        RunTime::RunBlock => {
            let host = cluster.running(DAMAGED).data.clone().unwrap();
            let pool = cluster.running(DAMAGED).pool.clone();
            // A read after the flush caches g's config block, as any read or
            // late apply could. The block was checked when it was read, so
            // serving it from the cache is correct; the test evicts it below
            // so that its read meets the damage on disk.
            assert_eq!(Cluster::applied_term(&host).await, term);
            let fault = &cluster.workers[DAMAGED].fault;
            assert!(
                damage_runs(fault, &path, &cluster.needle()) > 0,
                "g's config in a run"
            );
            assert!(pool.kv_store().evict_cached_blocks() > 0);
            let before = pool.kv_store().stats();
            let read = host
                .state
                .store()
                .read_metadata(BUDGET, |view| {
                    let raw = view.get(b"mvcc/config/v1")?.unwrap();
                    view.copy_output(raw.as_ref())
                })
                .await;
            let after = pool.kv_store().stats();
            assert!(
                read.is_err(),
                "a damaged block was served: commits {} -> {}, corruption {} -> {}, levels {:?}",
                before.commits,
                after.commits,
                before.corruption_detected,
                after.corruption_detected,
                pool.kv_store().level_runs(),
            );
        }
        RunTime::JournalValue => {
            // Every Raft entry body of g is a journal value (NS7), a JSON
            // object with a `payload` field. A running process serves them
            // from memory, so the damage is met after a restart: the open
            // replays nothing that was flushed and starts the replica, and
            // the first read of an entry body finds it.
            cluster.stop(DAMAGED).await;
            let fault = &cluster.workers[DAMAGED].fault;
            assert!(damage_journal(fault, &path, b"\"payload\"", true) > 0);
            // Raft reads entry bodies while it starts the replica; if that
            // read finds the damage, the start is refused and reported.
            if cluster.start(DAMAGED, None, false).await.is_ok() {
                let host = cluster.running(DAMAGED).data.clone().unwrap();
                let mut log = host.log.clone();
                let read = log.try_get_log_entries(0..).await;
                assert!(read.is_err(), "a damaged journal value was served");
            }
        }
    }
    let pool = cluster.running(DAMAGED).pool.clone();
    assert!(pool.is_group_quarantined(cluster.group.group_id));
    // Like the process, the fixture keeps no handle of the stopped replica:
    // an open handle would block the release (rule 4).
    cluster.workers[DAMAGED].running.as_mut().unwrap().data = None;
}

async fn run_time_damage_is_replaced_by_control(kind: RunTime, reason: GroupDamageReason) {
    let mut cluster = Cluster::new(5, &[]).await;
    let acknowledged = cluster.term;
    damage_at_run_time(&mut cluster, kind).await;
    let manager = cluster.running(DAMAGED).manager.clone();
    timeout(Duration::from_secs(10), async {
        while !manager.hosted_replicas(cluster.group.group_id).is_empty() {
            nabedra_rt::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the damaged host stopped");
    let record = cluster.replaced(Duration::from_secs(120)).await;
    assert_eq!(record.report.reason, reason, "{kind:?}");
    cluster.check_replaced(&record, acknowledged).await;
    cluster.shutdown().await;
}

/// NS23 / O06 / O17 with Control doing the replacement, G2: a damaged run
/// block of g found by a read on worker 2 while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns23_o06_o17_ns14b_g2_run_time_damage_is_replaced_by_control() {
    timeout(
        Duration::from_secs(240),
        run_time_damage_is_replaced_by_control(RunTime::RunBlock, GroupDamageReason::RunBlock),
    )
    .await
    .expect("NS14b G2 watchdog");
}

/// NS23 / O06 / O17 with Control doing the replacement, G3: a damaged journal
/// value (a Raft entry body of g) found when worker 2 reads its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns23_o06_o17_ns14b_g3_journal_value_damage_is_replaced_by_control() {
    timeout(
        Duration::from_secs(240),
        run_time_damage_is_replaced_by_control(
            RunTime::JournalValue,
            GroupDamageReason::JournalValue,
        ),
    )
    .await
    .expect("NS14b G3 watchdog");
}

/// NS23 / O17: the Control leader stops after the Data group enters joint
/// consensus. The successor must finish the same plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns23_o17_ns14b_control_leader_change_mid_replacement_resumes_the_same_plan() {
    timeout(Duration::from_secs(300), async {
        // Worker 3 hosts only Control, so the fresh replica goes to worker 4.
        let mut cluster = Cluster::new(5, &[3]).await;
        let acknowledged = cluster.term;
        let raw = cluster.running(3).control.as_ref().unwrap().0.clone();
        let node = raw.state.resident().node_id();
        timeout(Duration::from_secs(60), async {
            while raw.raft.current_leader().await != Some(node)
                || raw.ensure_linearizable().await.is_err()
            {
                let _ = raw.raft.trigger().elect(false).await;
                nabedra_rt::time::sleep(Duration::from_millis(700)).await;
            }
        })
        .await
        .expect("worker 3 leads Control");
        let reached = Arc::new(tokio::sync::Notify::new());
        let never = Arc::new(tokio::sync::Notify::new());
        *cluster
            .running(3)
            .scheduler
            .as_ref()
            .unwrap()
            .hold_after_host
            .lock()
            .unwrap() = Some((reached.clone(), never.clone()));

        damage_at_run_time(&mut cluster, RunTime::RunBlock).await;
        timeout(Duration::from_secs(60), reached.notified())
            .await
            .expect("worker 3 hosted the fresh replica and holds");
        let plan = cluster.record().await.unwrap().plan().cloned().unwrap();
        assert_eq!(plan.replacement.worker_id, cluster.enrollments[4].worker_id);
        assert!(
            cluster
                .running(4)
                .manager
                .hosted_data(&cluster.group, plan.replacement.node_id)
                .is_ok()
        );
        let begun_by_old = cluster
            .running(3)
            .scheduler
            .as_ref()
            .unwrap()
            .stats
            .begun
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(begun_by_old, 1);

        // Stop at the same durable boundary as a lost reply after the joint
        // entry. Append exactly that entry to keep the uniform step pending.
        let data = cluster.data_host(cluster.data_leader().await).unwrap();
        let old = data.observe_membership().await.unwrap();
        let learner = data
            .replicate_learner(old, plan.replacement.clone())
            .await
            .unwrap();
        let next = plan.next_members();
        let membership = super::super::entry::Membership::new(
            vec![
                plan.members.iter().map(|member| member.node_id).collect(),
                next.iter().map(|member| member.node_id).collect(),
            ],
            plan.members
                .iter()
                .chain(&next)
                .map(|member| (member.node_id, member.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap();
        data.raft
            .append_membership(
                membership,
                super::super::entry::Payload::Blank,
                [openraft::raft::Precondition::LastMembershipLogId {
                    last_membership_log_id: Some(super::super::hosted::log_id(
                        learner.caught_up_through(),
                    )),
                }],
            )
            .await
            .unwrap();
        assert!(data.observe_stable_membership().await.is_err());
        drop(data);
        cluster.stop(3).await;
        let record = cluster.replaced(Duration::from_secs(150)).await;
        let replacement = cluster.check_replaced(&record, acknowledged).await;
        assert_eq!(
            replacement, plan.replacement,
            "the new leader resumed the same plan"
        );
        let mut begun = 0;
        let mut replaced = 0;
        for index in [0, 1] {
            let stats = &cluster.running(index).scheduler.as_ref().unwrap().stats;
            begun += stats.begun.load(std::sync::atomic::Ordering::Relaxed);
            replaced += stats.replaced.load(std::sync::atomic::Ordering::Relaxed);
        }
        assert_eq!(begun, 0, "the new leader never planned a second identity");
        assert_eq!(replaced, 1);
        // Only the plan's fresh replica was ever hosted outside g's members.
        assert_eq!(
            cluster
                .running(4)
                .manager
                .hosted_replicas(cluster.group.group_id),
            vec![plan.replacement.node_id]
        );
        drop(never);
        cluster.shutdown().await;
    })
    .await
    .expect("NS14b leader change watchdog");
}
