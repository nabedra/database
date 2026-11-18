use super::*;
use crate::{
    oltp::{
        mvcc::store::{KeySpan, ParticipantCommand, ParticipantMachine, ParticipantReply},
        pool::ByteClass,
    },
    oltp_replication::{
        authority::config::{CatalogConfig, DataConfig},
        hosted::{RaftConsensus, log_id},
        owned::OwnedCommand,
    },
};
use openraft::{RaftLogReader, ServerState, storage::RaftLogStorage};
use tokio::{sync::Semaphore, task::JoinHandle};

const BODY_BYTES: usize = 256 << 10;
const WAIT: Duration = Duration::from_secs(10);

struct GatedDirectory {
    base: Directory,
    block_progress_for: Arc<Mutex<Option<Uuid>>>,
}
impl PeerDirectory for GatedDirectory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if purpose == Purpose::RaftProgress
            && *self.block_progress_for.lock().unwrap() == Some(group.group_id)
        {
            return Err(unavailable());
        }
        self.base.verify(peer, group, purpose)
    }

    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.base.resolve(worker)
    }
}

struct PausedBulk {
    entered: Semaphore,
    release: Semaphore,
}
impl Endpoint for PausedBulk {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            assert_eq!(session.purpose(), Purpose::Raft);
            assert_eq!(
                super::super::header(header)?.payload_bytes,
                BODY_BYTES as u64
            );
            Ok(ReceivePlan {
                class: TrafficClass::DataConsensus,
                section_lengths: [BODY_BYTES as u64, 0, 0],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        _session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let body = sections.section(0).unwrap();
            assert_eq!(body.len(), BODY_BYTES);
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Ok(ChargedReply {
                header,
                sections: [None, None, None],
            })
        })
    }
}

struct Worker {
    shared: Arc<WorkerSharedResources>,
    manager: Arc<HostedGroupManager>,
    pool: OltpPool,
    stale_client: WorkerClient,
    stop: oneshot::Sender<()>,
    transport: JoinHandle<Result<(), ProtocolError>>,
}

/// C07/M02/O05/O06/O11/O22. Real mTLS and independent Raft WALs
/// with occupied DataConsensus handlers and Query executors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c07_m02_o05_o06_o11_o22_mtls_data_and_catalog_raft_progress_during_bulk_pressure() {
    timeout(Duration::from_secs(75), async {
        let dir = Temporary(std::env::temp_dir().join(format!("raft-progress-{}", Uuid::new_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        let tls = tls::certificates(&dir.0, 2);
        let cluster = Uuid::new_v4();
        let data_kind = GroupKind::Data {
            table_id: Uuid::new_v4(),
            shard_id: Uuid::new_v4(),
        };
        let group = |kind| GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("progress").unwrap(),
            group_id: Uuid::new_v4(),
            kind,
            group_format_version: 1,
        };
        let data_group = group(data_kind.clone());
        let catalog_group = group(GroupKind::Catalog);
        let bulk_group = group(data_kind);
        let bulk = Arc::new(PausedBulk {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let blocked = Arc::new(Mutex::new(None));
        let mut listeners = Vec::new();
        let mut workers = BTreeMap::new();
        let mut members = BTreeMap::new();
        for (index, tls) in tls.iter().enumerate() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let worker = WorkerEnrollment {
                worker_id: Uuid::new_v4(),
                incarnation: Uuid::new_v4(),
                certificate_sha256: Digest::parse(tls.certificate_sha256()).unwrap(),
                enrollment_revision: 2,
                endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            };
            let node_id = index as u64 + 1;
            members.insert(
                node_id,
                ReplicaMember {
                    node_id,
                    worker_id: worker.worker_id,
                    pool_id: Uuid::new_v4(),
                },
            );
            workers.insert(worker.worker_id, worker);
            listeners.push(listener);
        }
        let mut sides = Vec::new();
        for (index, (tls, listener)) in tls.iter().zip(listeners).enumerate() {
            let node = index as u64 + 1;
            let member = &members[&node];
            let worker = &workers[&member.worker_id];
            let shared = WorkerSharedResources::new(
                node,
                WorkerResources {
                    max_threads: 4,
                    ..Default::default()
                },
                SharedResourceConfig::default(),
            )
            .unwrap();
            let pool = OltpPool::open_development(
                dir.0.join(format!("pool{node}")),
                PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: 1,
                    cluster_id: cluster,
                    worker_id: worker.worker_id,
                    pool_id: member.pool_id,
                },
                shared.clone(),
            )
            .unwrap();
            let directory = Arc::new(GatedDirectory {
                base: Directory {
                    cluster,
                    workers: workers.clone(),
                    local: worker.worker_id,
                    blocked: Uuid::nil(),
                },
                block_progress_for: if node == 2 {
                    blocked.clone()
                } else {
                    Arc::default()
                },
            });
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            if node == 2 {
                registry
                    .reserve(bulk_group.clone())
                    .unwrap()
                    .bind(Purpose::Raft, bulk.clone())
                    .unwrap();
            }
            let transport = Arc::new(
                WorkerTransport::new(
                    worker,
                    tls,
                    shared.clone(),
                    directory.clone(),
                    registry.clone(),
                    TransportConfig::default(),
                )
                .unwrap(),
            );
            let mut stale = worker.clone();
            stale.incarnation = Uuid::new_v4();
            stale.enrollment_revision = 1;
            let stale_client = WorkerTransport::new(
                &stale,
                tls,
                shared.clone(),
                directory,
                registry.clone(),
                TransportConfig::default(),
            )
            .unwrap()
            .client();
            let manager = HostedGroupManager::new(shared.clone(), transport.client(), registry);
            let (stop, stopped) = oneshot::channel();
            let transport = tokio::spawn(transport.run(listener, async {
                let _ = stopped.await;
            }));
            sides.push(Worker {
                shared,
                manager,
                pool,
                stale_client,
                stop,
                transport,
            });
        }
        let config = openraft::Config {
            enable_elect: false,
            enable_heartbeat: false,
            heartbeat_interval: 500,
            election_timeout_min: 1_500,
            election_timeout_max: 3_000,
            ..Default::default()
        };
        let mut data = Vec::new();
        let mut catalogs = Vec::new();
        for (index, worker) in sides.iter().enumerate() {
            let member = &members[&(index as u64 + 1)];
            data.push(
                worker
                    .manager
                    .start_data(
                        worker.pool.group(data_group.clone()).await.unwrap(),
                        member.clone(),
                        members.clone(),
                        config.clone(),
                    )
                    .await
                    .unwrap(),
            );
            catalogs.push(
                worker
                    .manager
                    .start_catalog(
                        worker.pool.group(catalog_group.clone()).await.unwrap(),
                        member.clone(),
                        members.clone(),
                        config.clone(),
                    )
                    .await
                    .unwrap(),
            );
        }
        assert_progress::<DataConfig>(
            &data,
            &sides,
            &members,
            &blocked,
            &bulk_group,
            &bulk,
            catalog_group.group_id,
        )
        .await;
        assert_progress::<CatalogConfig>(
            &catalogs,
            &sides,
            &members,
            &blocked,
            &bulk_group,
            &bulk,
            catalog_group.group_id,
        )
        .await;
        for worker in sides {
            worker.manager.shutdown().await.unwrap();
            worker.stop.send(()).unwrap();
            worker.transport.await.unwrap().unwrap();
        }
    })
    .await
    .expect("typed consensus progress watchdog");
}

async fn assert_progress<C>(
    hosts: &[Arc<RaftConsensus<C, ParticipantMachine>>],
    workers: &[Worker],
    members: &BTreeMap<u64, ReplicaMember>,
    blocked: &Mutex<Option<Uuid>>,
    bulk_group: &GroupIdentity,
    bulk: &PausedBulk,
    catalog_group_id: Uuid,
) where
    C: TypedConfig<
            Command = ParticipantCommand,
            Reply = ParticipantReply,
            R = Applied<Option<ParticipantReply>>,
        >,
    ParticipantMachine: TypedMachine<C>,
{
    let (leader, follower) = (&hosts[0], &hosts[1]);
    let group = leader.state.store().identity();
    let client = workers[0].manager.client();
    let destination = members[&2].worker_id;
    let mut follower_log = follower.log.clone();
    leader.raft.initialize(members.clone()).await.unwrap();
    leader.raft.trigger().elect(false).await.unwrap();
    leader
        .raft
        .wait(Some(WAIT))
        .state(ServerState::Leader, "initial typed leader")
        .await
        .unwrap();
    let baseline = follower
        .raft
        .wait(Some(WAIT))
        .applied_index_at_least(Some(1), "initial typed follower")
        .await
        .unwrap()
        .last_applied
        .unwrap();
    let baseline_vote = follower_log.read_vote().await.unwrap().unwrap();

    // Let a real nonempty append reach disk and let the leader commit it.
    // Hold the follower's empty AppendEntries until pressure is in place.
    *blocked.lock().unwrap() = Some(group.group_id);
    let proposed = leader
        .propose(
            OwnedCommand::new_owned(
                ParticipantCommand::Fence {
                    transition_id: Uuid::new_v4(),
                    expected: None,
                    next: Authority {
                        run_id: Uuid::new_v4(),
                        owner_term: 1,
                    },
                    schema_epoch: 1,
                    routing_epoch: 1,
                    span: KeySpan {
                        start: None,
                        end: None,
                    },
                    catalog_group_id,
                },
                &workers[0].shared,
                ByteClass::Data,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(proposed.reply, ParticipantReply::Fenced(_)));
    let cut = log_id(proposed.position);
    assert!(cut.index > baseline.index);
    assert_eq!(
        follower_log.get_log_state().await.unwrap().last_log_id,
        Some(cut)
    );
    assert_eq!(follower_log.read_committed().await.unwrap(), Some(baseline));

    let query = [
        workers[0]
            .shared
            .prepare(ExecutionClass::Query)
            .await
            .unwrap(),
        workers[1]
            .shared
            .prepare(ExecutionClass::Query)
            .await
            .unwrap(),
    ];
    let heartbeat = Header {
        from_node: 1,
        to_node: 2,
        method: Method::Append,
        payload_bytes: 0,
        leadership_transfer_from: None,
        vote: baseline_vote,
        prev_log_id: Some(cut),
        leader_commit: Some(cut),
    };
    let slots = TransportConfig::default().handler_slots[TrafficClass::DataConsensus as usize];
    let mut background = Vec::new();
    for _ in 0..slots {
        let client = client.clone();
        let source = bulk_group.clone();
        let header = workers[0]
            .shared
            .encode_in(
                ByteClass::Data,
                &Header {
                    payload_bytes: BODY_BYTES as u64,
                    ..heartbeat
                },
                1024,
            )
            .unwrap();
        let body = workers[0]
            .shared
            .allocate_bytes(ByteClass::Data, BODY_BYTES)
            .unwrap()
            .freeze();
        background.push(tokio::spawn(async move {
            client
                .request(
                    &source,
                    destination,
                    Purpose::Raft,
                    header,
                    [Some(body), None, None],
                    0,
                )
                .await
        }));
    }
    timeout(WAIT, bulk.entered.acquire_many(slots as u32))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let encoded = workers[0]
        .shared
        .encode_in(ByteClass::Data, &heartbeat, 1024)
        .unwrap();
    assert!(matches!(
        client
            .request_classified(
                group,
                destination,
                Purpose::Raft,
                encoded,
                [None, None, None],
                0
            )
            .await,
        Err(RequestError::OutgoingLaneFull)
    ));

    assert!(leader.ensure_linearizable().await.is_err());
    *blocked.lock().unwrap() = None;
    let barrier = leader.ensure_linearizable().await.unwrap();
    assert_eq!(barrier.applied, proposed.position);
    timeout(WAIT, async {
        while follower_log.read_committed().await.unwrap() != Some(cut) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("follower saved the commit marker during saturation");
    assert_eq!(
        follower.state.clone().applied_state().await.unwrap().0,
        Some(baseline)
    );
    assert!(background.iter().all(|rpc| !rpc.is_finished()));

    let rejected = Header {
        vote: Vote::new_committed(baseline_vote.leader_id().term + 10, 1),
        ..heartbeat
    };
    let malformed = [
        serde_json::to_vec(&Header {
            payload_bytes: BODY_BYTES as u64,
            ..rejected
        })
        .unwrap(),
        b"{\"method\":\"append\",\"to_node\":2,\"vote\":true}".to_vec(),
    ];
    for (case, bytes) in malformed.into_iter().enumerate() {
        let mut encoded = workers[0]
            .shared
            .allocate_bytes(ByteClass::Progress, bytes.len())
            .unwrap();
        encoded.as_mut_slice().copy_from_slice(&bytes);
        let failure = client
            .request_classified(
                group,
                destination,
                Purpose::RaftProgress,
                encoded.freeze(),
                [None, None, None],
                0,
            )
            .await
            .err()
            .expect("malformed Raft progress frame was accepted");
        assert!(
            matches!(
                &failure,
                RequestError::PeerRejected { error, .. } if error.code == ErrorCode::ValidationError
            ),
            "{:?} malformed Raft progress frame {case}: {failure:?}",
            group.kind,
        );
    }
    let encoded = workers[0]
        .shared
        .encode_in(ByteClass::Progress, &rejected, 1024)
        .unwrap();
    assert!(matches!(
        workers[0]
            .stale_client
            .request_classified(
                group,
                destination,
                Purpose::RaftProgress,
                encoded,
                [None, None, None],
                0
            )
            .await,
        Err(RequestError::Unreachable)
    ));
    assert_eq!(follower_log.read_vote().await.unwrap(), Some(baseline_vote));

    // Request a genuine leadership-transfer vote from the current voter.
    // OpenRaft only grants it once the vote is durably saved.
    let mut factory = TypedNetworkFactory::<C>::new(
        workers[1].manager.client(),
        members[&2].clone(),
        follower.state.resident(),
        workers[1].shared.clone(),
    )
    .unwrap();
    let mut candidate = factory.new_client(1, &members[&1]).await;
    let next_vote = Vote::new(baseline_vote.leader_id().term + 1, 2);
    let vote = candidate
        .vote(
            VoteRequest::<C> {
                vote: next_vote,
                last_log_id: Some(cut),
                leadership_transfer_from: Some(baseline_vote),
            },
            RPCOption::new(WAIT),
        )
        .await
        .unwrap();
    assert!(vote.vote_granted);
    assert_eq!(
        leader.log.clone().read_vote().await.unwrap(),
        Some(next_vote)
    );
    assert_eq!(follower_log.read_committed().await.unwrap(), Some(cut));
    assert!(background.iter().all(|rpc| !rpc.is_finished()));

    drop(query);
    follower
        .raft
        .wait(Some(WAIT))
        .applied_index_at_least(Some(cut.index), "apply after Query release")
        .await
        .unwrap();
    assert_eq!(
        follower.state.clone().applied_state().await.unwrap().0,
        Some(cut)
    );
    bulk.release.add_permits(slots);
    for rpc in background {
        rpc.await.unwrap().unwrap();
    }
}
