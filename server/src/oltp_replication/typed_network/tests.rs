use super::*;
#[path = "../../../tests/support/tls.rs"]
mod tls;
use crate::{
    config::WorkerResources,
    oltp::pool::{OltpPool, PoolIdentity, SharedResourceConfig, TransactionBudget},
    oltp_replication::{
        authority::{
            config::TimestampConfig,
            consensus::Consensus,
            timestamp::{TimestampCommand, TimestampHandle, TimestampMachine},
        },
        manager::HostedGroupManager,
        multiplex::*,
        types::*,
    },
};
use openraft::storage::RaftStateMachine;
use std::{collections::BTreeMap, path::PathBuf};
use nabedra_rt::time::timeout;
use tokio::{net::TcpListener, sync::oneshot};
use uuid::Uuid;

#[path = "tests/progress.rs"]
mod progress;

#[cfg(target_os = "linux")]
#[path = "../../../tests/oltp_replicated_durability/fixture.rs"]
mod replicated_durability;

#[test]
fn o26_learner_accepts_current_leader_while_replaying_historical_membership() {
    let member = |node_id| ReplicaMember {
        node_id,
        worker_id: nabedra_rt::ids::uuid_v4(),
        pool_id: nabedra_rt::ids::uuid_v4(),
    };
    let first = member(1);
    let second = member(2);
    let third = member(3);
    let leader = ReplicaMember {
        worker_id: first.worker_id,
        ..member(4)
    };
    let bootstrap = BTreeMap::from([(2, second.clone()), (3, third.clone()), (4, leader.clone())]);
    let history = Membership::new(
        vec![[1, 2, 3].into_iter().collect()],
        BTreeMap::from([(1, first), (2, second), (3, third)]),
    )
    .unwrap();
    let stored = |index| {
        openraft::StoredMembership::new(
            Some(crate::oltp_replication::hosted::log_id(AppliedPosition {
                term: 1,
                node_id: 1,
                index,
            })),
            history.clone(),
        )
    };
    let prefix = stored(1);
    let published_index = 8;
    assert_eq!(
        raft_sender(&prefix, &bootstrap, Some(published_index), 4),
        Some(&leader)
    );
    assert_eq!(raft_sender(&prefix, &bootstrap, None, 4), None);
    assert_eq!(
        raft_sender(&prefix, &bootstrap, Some(published_index), 5),
        None
    );
    assert_eq!(
        raft_sender(
            &stored(published_index),
            &bootstrap,
            Some(published_index),
            4
        ),
        None
    );
}

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Directory {
    cluster: Uuid,
    workers: BTreeMap<Uuid, WorkerEnrollment>,
    local: Uuid,
    blocked: Uuid,
}
impl PeerDirectory for Directory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if purpose == Purpose::Raft
            && (peer.worker_id == self.blocked || self.local == self.blocked)
        {
            return Err(unavailable());
        }
        if group.cluster_id != self.cluster
            || WorkerIdentity::from(&self.resolve(peer.worker_id)?) != peer
        {
            return Err(invalid("foreign fixture peer"));
        }
        Ok(TransportTrust::AppliedEnrollment)
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.workers.get(&worker).cloned().ok_or_else(unavailable)
    }
}

/// O05/O28: two actual typed Raft replicas transfer a file snapshot through the
/// same mTLS listener. Control snapshot traffic progresses with Data wire held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o05_o28_typed_mtls_snapshot_installs_exact_group_cut_with_data_wire_full() {
    timeout(Duration::from_secs(40), async {
        let dir =
            Temporary(std::env::temp_dir().join(format!("typed-snapshot-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        let certificates = tls::certificates(&dir.0, 3);
        let cluster = nabedra_rt::ids::uuid_v4();
        let group = GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("snapshot").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Timestamps,
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
        let members: BTreeMap<_, _> = workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                let node = (index + 1) as u64;
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
        let mut stops = Vec::new();
        let mut transports = Vec::new();
        let mut hosts = Vec::new();
        let mut shared_all = Vec::new();
        let mut managers = Vec::new();
        for (index, ((worker, tls), listener)) in
            workers.iter().zip(certificates).zip(listeners).enumerate()
        {
            let shared = WorkerSharedResources::new(
                index as u64 + 1,
                WorkerResources {
                    max_threads: 4,
                    ..Default::default()
                },
                SharedResourceConfig::default(),
            )
            .unwrap();
            let pool = OltpPool::open_development(
                dir.0.join(format!("pool{index}")),
                PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                    cluster_id: cluster,
                    worker_id: worker.worker_id,
                    pool_id: members[&(index as u64 + 1)].pool_id,
                },
                shared.clone(),
            )
            .unwrap();
            let directory = Arc::new(Directory {
                cluster,
                workers: workers
                    .iter()
                    .cloned()
                    .map(|worker| (worker.worker_id, worker))
                    .collect(),
                local: worker.worker_id,
                blocked: workers[2].worker_id,
            });
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            let transport = Arc::new(
                WorkerTransport::new(
                    worker,
                    &tls,
                    shared.clone(),
                    directory.clone(),
                    registry.clone(),
                    TransportConfig::default(),
                )
                .unwrap(),
            );
            let manager = HostedGroupManager::new(shared.clone(), transport.client(), registry);
            let store = pool.group(group.clone()).await.unwrap();
            let host = manager
                .start_timestamps(
                    store,
                    members[&(index as u64 + 1)].clone(),
                    members.clone(),
                    openraft::Config {
                        enable_elect: index != 2,
                        heartbeat_interval: 50,
                        election_timeout_min: 150,
                        election_timeout_max: 300,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let (stop, stopped) = oneshot::channel();
            stops.push(stop);
            transports.push(nabedra_rt::task::spawn(transport.run(listener, async {
                let _ = stopped.await;
            })));
            hosts.push(host);
            shared_all.push(shared);
            managers.push(manager);
        }
        hosts[0].raft.initialize(members.clone()).await.unwrap();
        let leader = loop {
            if hosts[0].ensure_linearizable().await.is_ok() {
                break 0;
            }
            if hosts[1].ensure_linearizable().await.is_ok() {
                break 1;
            }
            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
        };
        let follower = 2;
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: nabedra_rt::ids::uuid_v4(),
                owner_term: 1,
                membership_revision: 1,
            },
            ready_root: crate::oltp_replication::ReadySetHasher::default().finish(),
            ready_count: 0,
        };
        let handle = TimestampHandle::new(
            TimestampMachine::new(hosts[leader].state.store().clone()).unwrap(),
            hosts[leader].clone(),
        );
        handle
            .fence(nabedra_rt::ids::uuid_v4(), None, ready.view.authority(), ready)
            .await
            .unwrap();
        // SNAP1: what the replication task sends a lagging follower: a fresh
        // cut, whose file is written during the transfer below.
        let mut state = hosts[leader].state.clone();
        let snapshot = state.get_current_snapshot().await.unwrap().unwrap();
        let expected = snapshot.meta.clone();
        let mut factory = TypedNetworkFactory::<TimestampConfig>::new(
            managers[leader].client(),
            members[&(leader as u64 + 1)].clone(),
            hosts[leader].state.resident(),
            shared_all[leader].clone(),
        )
        .unwrap();
        let mut network = factory
            .new_client(follower as u64 + 1, &members[&(follower as u64 + 1)])
            .await;
        let held: Vec<_> = shared_all
            .iter()
            .map(|shared| {
                shared
                    .allocate_bytes(
                        crate::oltp::pool::ByteClass::Data,
                        shared.config().control_bytes - shared.config().progress_bytes,
                    )
                    .unwrap()
            })
            .collect();
        let vote = hosts[leader].raft.metrics().borrow_watched().vote;
        network
            .full_snapshot(
                vote,
                snapshot,
                std::future::pending(),
                RPCOption::new(Duration::from_secs(10)),
            )
            .await
            .unwrap();
        drop(held);
        let budget = TransactionBudget {
            scratch_bytes: 256 << 10,
            batch_bytes: 0,
            output_bytes: 128 << 10,
        };
        let installed = hosts[follower]
            .state
            .store()
            .current_snapshot(budget)
            .await
            .unwrap()
            .expect("real installed snapshot");
        assert_eq!(
            installed.header.applied,
            expected
                .last_log_id
                .map(crate::oltp_replication::hosted::position)
        );
        assert_eq!(installed.header.identity, group);
        for manager in &managers {
            manager.shutdown().await.unwrap();
        }
        for stop in stops {
            let _ = stop.send(());
        }
        for task in transports {
            task.await.unwrap().unwrap();
        }
    })
    .await
    .expect("typed snapshot fixture watchdog");
}

/// O06/O16/O17 component evidence for RF3 replacement of a lost Timestamp
/// voter. The fourth store starts empty and receives the committed snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o16_o17_rf3_lost_timestamp_voter_replacement() {
    timeout(Duration::from_secs(60), async {
        let dir = Temporary(std::env::temp_dir().join(format!("typed-move-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir(&dir.0).unwrap();
        let certificates = tls::certificates(&dir.0, 4);
        let cluster = nabedra_rt::ids::uuid_v4();
        let group = GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("replica-move").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Timestamps,
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
        let peers = workers
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
            .collect::<BTreeMap<_, _>>();
        let mut hosts = Vec::new();
        let mut managers = Vec::new();
        let mut stops = Vec::new();
        let mut transports = Vec::new();
        for (index, ((worker, tls), listener)) in
            workers.iter().zip(certificates).zip(listeners).enumerate()
        {
            let shared = WorkerSharedResources::new(
                index as u64 + 1,
                WorkerResources {
                    max_threads: 4,
                    ..Default::default()
                },
                SharedResourceConfig::default(),
            )
            .unwrap();
            let member = peers[&(index as u64 + 1)].clone();
            let pool = OltpPool::open_development(
                dir.0.join(format!("pool{index}")),
                PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                    cluster_id: cluster,
                    worker_id: worker.worker_id,
                    pool_id: member.pool_id,
                },
                shared.clone(),
            )
            .unwrap();
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            let transport = Arc::new(
                WorkerTransport::new(
                    worker,
                    &tls,
                    shared.clone(),
                    Arc::new(Directory {
                        cluster,
                        workers: workers
                            .iter()
                            .cloned()
                            .map(|worker| (worker.worker_id, worker))
                            .collect(),
                        local: worker.worker_id,
                        blocked: Uuid::nil(),
                    }),
                    registry.clone(),
                    TransportConfig::default(),
                )
                .unwrap(),
            );
            let manager = HostedGroupManager::new(shared, transport.client(), registry);
            let host = manager
                .start_timestamps(
                    pool.group(group.clone()).await.unwrap(),
                    member,
                    peers.clone(),
                    openraft::Config {
                        heartbeat_interval: 50,
                        election_timeout_min: 150,
                        election_timeout_max: 300,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let (stop, stopped) = oneshot::channel();
            stops.push(Some(stop));
            transports.push(nabedra_rt::task::spawn(transport.run(listener, async {
                let _ = stopped.await;
            })));
            hosts.push(host);
            managers.push(manager);
        }
        let voters = peers
            .iter()
            .filter(|(node, _)| **node <= 3)
            .map(|(node, member)| (*node, member.clone()))
            .collect::<BTreeMap<_, _>>();
        hosts[0].raft.initialize(voters).await.unwrap();
        let elected = |excluded| {
            let hosts = &hosts;
            async move {
                loop {
                    for (index, host) in hosts[..3].iter().enumerate() {
                        if Some(index) != excluded && host.ensure_linearizable().await.is_ok() {
                            return index;
                        }
                    }
                    nabedra_rt::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        let lost = elected(None).await;
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: nabedra_rt::ids::uuid_v4(),
                owner_term: 1,
                membership_revision: 1,
            },
            ready_root: crate::oltp_replication::ReadySetHasher::default().finish(),
            ready_count: 0,
        };
        let authority = ready.view.authority();
        let handle = TimestampHandle::new(
            TimestampMachine::new(hosts[lost].state.store().clone()).unwrap(),
            hosts[lost].clone(),
        );
        let before = handle
            .fence(nabedra_rt::ids::uuid_v4(), None, authority, ready)
            .await
            .unwrap();
        drop(handle);
        managers[lost].shutdown().await.unwrap();
        let _ = stops[lost].take().unwrap().send(());
        let leader = elected(Some(lost)).await;
        let old = hosts[leader].observe_membership().await.unwrap();
        let expected = Page::new(old.replicas().to_vec()).unwrap();
        assert_eq!(expected.len(), 3);
        hosts[leader].raft.trigger().snapshot().await.unwrap();
        let snapshot = hosts[leader]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .is_some_and(|entry| entry.index >= before.position.index)
                },
                "snapshot includes the acknowledged Timestamp fence",
            )
            .await
            .unwrap()
            .snapshot
            .unwrap();
        hosts[leader]
            .raft
            .trigger()
            .purge_log(snapshot.index)
            .await
            .unwrap();
        hosts[leader]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| metrics.purged == Some(snapshot),
                "older logs are unavailable to the fresh replica",
            )
            .await
            .unwrap();
        let learner = hosts[leader]
            .replicate_learner(old, peers[&4].clone())
            .await
            .unwrap();
        assert_eq!(learner.target(), &peers[&4]);
        assert!(learner.caught_up_through().index >= before.position.index);
        let still_old = hosts[leader].observe_membership().await.unwrap();
        assert_eq!(still_old.replicas(), expected.as_ref());
        assert!(
            !still_old
                .replicas()
                .iter()
                .any(|member| member.node_id == 4)
        );
        let removed = lost as u64 + 1;
        let next = hosts[leader].replace_voter(learner, removed).await.unwrap();
        let final_members = Page::new(next.replicas().to_vec()).unwrap();
        assert_eq!(final_members.len(), 3);
        assert!(final_members.iter().any(|member| member.node_id == 4));
        assert!(!final_members.iter().any(|member| member.node_id == removed));
        // O16/O26 (REP6 merge regression): a same-ID move's source replica
        // retires only after applying this membership, so the final
        // membership keeps the removed voter as a learner the leader still
        // replicates to. System replacement and NS14 repair drop theirs.
        {
            let metrics = hosts[leader].raft.metrics();
            let metrics = metrics.borrow_watched();
            let membership = metrics.membership_config.membership();
            assert!(membership.nodes().any(|(node, _)| *node == removed));
            assert!(!membership.voter_ids().any(|node| node == removed));
        }
        let target_metrics = hosts[3]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(next.applied_at().index), "target membership applied")
            .await
            .unwrap();
        assert!(
            target_metrics
                .snapshot
                .is_some_and(|entry| entry.index >= snapshot.index)
        );
        let target = hosts[3].state.store();
        let activation = target.replica_activation(&next).await.unwrap();
        let statement = activation.statement();
        assert_eq!(statement.group, group);
        assert_eq!(statement.pool.pool_id, peers[&4].pool_id);
        assert_eq!(statement.minimum, next.applied_at());
        assert!(statement.applied.index >= statement.minimum.index);
        let active_generation = statement.active_generation;
        let snapshot_budget = TransactionBudget {
            scratch_bytes: 1 << 20,
            batch_bytes: 1 << 20,
            output_bytes: 64 << 10,
        };
        let snapshot = target.export_snapshot(snapshot_budget).await.unwrap();
        let mut installing = Box::pin(target.install_snapshot(snapshot, snapshot_budget));
        // Poll at the actual generation-write acquisition. No timing-based
        // inference or arbitrary minimum-index DTO stands in for this guard.
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                std::future::Future::poll(installing.as_mut(), cx).is_pending()
            ))
            .await
        );
        drop(activation);
        timeout(Duration::from_secs(10), installing)
            .await
            .unwrap()
            .unwrap();
        let activated = target.replica_activation(&next).await.unwrap();
        assert!(activated.statement().active_generation > active_generation);
        drop(activated);
        let saved = hosts[3]
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
        assert_eq!(saved["authority"], serde_json::to_value(authority).unwrap());
        let replay = hosts[leader]
            .reconcile_voter_replacement(&expected, &final_members, &peers[&4], removed, None, true)
            .await
            .unwrap();
        assert_eq!(replay.replicas(), final_members.as_ref());
        assert!(
            hosts[leader]
                .reconcile_voter_replacement(
                    &expected,
                    &final_members,
                    &peers[&4],
                    leader as u64 + 1,
                    None,
                    true,
                )
                .await
                .is_err(),
            "stable membership must not certify a different removal"
        );
        let next_authority = Authority {
            run_id: authority.run_id,
            owner_term: 2,
        };
        let next_ready = ReadyMembership {
            view: AuthorityView {
                owner_term: 2,
                ..ready.view
            },
            ..ready
        };
        let handle = TimestampHandle::new(
            TimestampMachine::new(hosts[leader].state.store().clone()).unwrap(),
            hosts[leader].clone(),
        );
        handle
            .fence(nabedra_rt::ids::uuid_v4(), Some(authority), next_authority, next_ready)
            .await
            .unwrap();
        assert_eq!(
            handle.current_authority().await.unwrap(),
            Some(next_authority)
        );
        drop(handle);
        for manager in managers {
            manager.shutdown().await.unwrap();
        }
        for stop in stops.into_iter().flatten() {
            let _ = stop.send(());
        }
        for transport in transports {
            transport.await.unwrap().unwrap();
        }
    })
    .await
    .expect("actual learner replacement watchdog");
}
