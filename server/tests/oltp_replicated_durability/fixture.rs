use super::{Directory, tls};
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::{
            CanonicalJson, Change, MvccKey, PreparedWrite,
            index::IndexValue,
            store::{
                KeySpan, ParticipantCommand, ParticipantMachine, ParticipantReply, initial_root,
                lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
                page_root,
            },
        },
        pool::{
            ApplyMeta, ByteClass, GroupStore, OltpPool, PoolIdentity, SharedResourceConfig,
            TransactionBudget, VerifiedPoolMount, WorkerSharedResources,
            mount::{PoolMountExpectation, PoolMountProfile},
        },
    },
    oltp_replication::{
        TlsMaterial,
        authority::{
            config::{ControlConfig, DataConfig},
            consensus::{Applied, Consensus},
            status::VerifiedStatusDecision,
        },
        entry::{LogId, Payload, Vote},
        hosted::{log_id, position},
        manager::{HostedGroupManager, ManagedData},
        multiplex::{EndpointRegistry, TransportConfig, WorkerTransport},
        owned::{OwnedCommand, serialized_digest},
        typed_log::TypedLogStore,
        types::{
            AppliedPosition, Authority, DecisionStatement, Digest, GroupIdentity, GroupKind,
            ManifestRef, Page, ParticipantPlan, ReplicaMember, RequestBinding, Text, Timestamp,
            TxnDecision, TxnId, WorkerEnrollment, WorkerIdentity,
        },
    },
    protocol::{ErrorCode, Key, Outcome},
};
use openraft::{
    RaftLogReader, ServerState, SnapshotPolicy,
    storage::{RaftLogStorage, RaftStateMachine},
    type_config::async_runtime::watch::WatchReceiver,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tracing_subscriber::{filter::Targets, prelude::*};
use uuid::Uuid;

#[path = "../oltp_native_durability/fixture.rs"]
mod native;

const WAIT: Duration = Duration::from_secs(15);
const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 128 << 10,
    output_bytes: 128 << 10,
};

#[derive(Serialize, Deserialize)]
struct Topology {
    group: GroupIdentity,
    catalog_group_id: Uuid,
    control_group_id: Uuid,
    index_id: Uuid,
    table_id: Uuid,
    authority: Authority,
    txn: TxnId,
    request: RequestBinding,
    manifest: ManifestRef,
    members: BTreeMap<u64, ReplicaMember>,
    control_pools: BTreeMap<u64, Uuid>,
    volume_markers: BTreeMap<u64, Uuid>,
}

#[derive(Deserialize, Serialize)]
struct Pending {
    baseline: AppliedPosition,
    target: AppliedPosition,
    command_digest: Digest,
    wal_payload: String,
}

fn write(root: &Path, name: &str, value: &impl Serialize) {
    native::publish(root, name, serde_json::to_vec(value).unwrap());
}

fn read<T: DeserializeOwned>(root: &Path, name: &str) -> T {
    serde_json::from_slice(&fs::read(root.join(name)).unwrap()).unwrap()
}

async fn stage(root: &Path, name: &str) {
    loop {
        if root.join(name).is_file() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn setup(root: &Path) {
    let tls_dir = root.join("tls");
    fs::create_dir(&tls_dir).unwrap();
    tls::certificates(&tls_dir, 3);
    let table_id = Uuid::new_v4();
    let authority = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 1,
    };
    let txn = TxnId {
        request_id: Uuid::new_v4(),
        attempt: 0,
    };
    let topology = Topology {
        group: GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("rf3-native-durability").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: 1,
        },
        catalog_group_id: Uuid::new_v4(),
        control_group_id: Uuid::new_v4(),
        index_id: Uuid::new_v4(),
        table_id,
        authority,
        txn,
        request: RequestBinding {
            run_id: authority.run_id,
            request_id: txn.request_id,
            digest: Digest::from_bytes([1; 32]),
            accepted_context: Digest::from_bytes([2; 32]),
        },
        manifest: ManifestRef {
            status_group_id: Uuid::new_v4(),
            digest: Digest::from_bytes([3; 32]),
            participant_count: 1,
        },
        members: (1..=3)
            .map(|node_id| {
                (
                    node_id,
                    ReplicaMember {
                        node_id,
                        worker_id: Uuid::new_v4(),
                        pool_id: Uuid::new_v4(),
                    },
                )
            })
            .collect(),
        control_pools: (1..=3).map(|node| (node, Uuid::new_v4())).collect(),
        volume_markers: (1..=3).map(|node| (node, Uuid::new_v4())).collect(),
    };
    write(root, "topology.json", &topology);
}

impl Topology {
    fn identity(&self, node: u64) -> PoolIdentity {
        let member = &self.members[&node];
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: 1,
            cluster_id: self.group.cluster_id,
            worker_id: member.worker_id,
            pool_id: member.pool_id,
        }
    }

    fn records(&self) -> [(MvccKey, Value); 2] {
        let primary_key = Key::Int64(7);
        [
            (
                MvccKey::Row {
                    table_id: self.table_id,
                    primary_key: primary_key.clone(),
                },
                json!({"id": 7, "indexed": 71, "value": "rf3-native-durability"}),
            ),
            (
                MvccKey::IndexEntry {
                    table_id: self.table_id,
                    index_id: self.index_id,
                    values: Page::new(vec![IndexValue::Int64(71)]).unwrap(),
                    primary_key,
                },
                json!({"base_version": null, "cover": {"id": 7, "indexed": 71}}),
            ),
        ]
    }

    fn resolve(&self) -> ParticipantCommand {
        let statement = DecisionStatement {
            txn: self.txn,
            request: self.request,
            manifest: self.manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
            decision: TxnDecision::Commit { commit_ts: ts(11) },
        };
        ParticipantCommand::Resolve {
            authority: self.authority,
            proof: VerifiedStatusDecision::model_for_test(statement)
                .unwrap()
                .into_sealed(),
            max_items: 16,
        }
    }

    async fn prepare(&self, data: &ManagedData) -> Pending {
        let shared = data.state.store().resources();
        let propose =
            |value| data.propose(OwnedCommand::new_owned(value, shared, ByteClass::Data).unwrap());
        assert!(matches!(
            propose(ParticipantCommand::Fence {
                transition_id: Uuid::new_v4(),
                expected: None,
                next: self.authority,
                schema_epoch: 1,
                routing_epoch: 1,
                span: KeySpan {
                    start: None,
                    end: None,
                },
                catalog_group_id: self.catalog_group_id,
            })
            .await
            .expect("initial Fence proposal after verified RF3 startup")
            .reply,
            ParticipantReply::Fenced(_)
        ));
        let mut writes: Vec<_> = self
            .records()
            .into_iter()
            .map(|(key, value)| PreparedWrite {
                key,
                expected_version: None,
                change: Change::Put(
                    CanonicalJson::from_serializable(shared, &value, 4096).unwrap(),
                ),
            })
            .collect();
        writes.sort_by(|left, right| crate::oltp::mvcc::store::compare_keys(&left.key, &right.key));
        let mut plan = ParticipantPlan {
            group: self.group.clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(10),
            write_count: writes.len() as u64,
            page_count: 1,
            write_root: Digest::from_bytes([0; 32]),
        };
        let previous_root = initial_root(self.txn, &plan).unwrap();
        plan.write_root = page_root(previous_root, 0, &writes).unwrap();
        let guard = VerifiedLifecycleGuard::model_for_test(
            LifecycleBinding {
                guard_id: Uuid::new_v4(),
                catalog_group_id: self.catalog_group_id,
                table_id: self.table_id,
                lifecycle_version: None,
                schema_epoch: 1,
                txn: self.txn,
                request: self.request,
                read_ts: plan.read_ts,
                authority: self.authority,
            },
            GuardMode::Shared,
        );
        assert_eq!(
            propose(ParticipantCommand::PreparePage {
                authority: self.authority,
                txn: self.txn,
                request: self.request,
                manifest: self.manifest,
                plan,
                page_index: 0,
                previous_root,
                writes: Page::new(writes).unwrap(),
                guards: Page::new(vec![guard.into_sealed()]).unwrap(),
            })
            .await
            .unwrap()
            .reply,
            ParticipantReply::PreparedPage {
                next_page: 1,
                writes: 2
            }
        );
        let sealed = propose(ParticipantCommand::Seal {
            authority: self.authority,
            txn: self.txn,
            manifest: self.manifest,
            result_digest: Digest::from_bytes([4; 32]),
        })
        .await
        .unwrap();
        assert!(matches!(sealed.reply, ParticipantReply::Sealed(_)));
        let resolve = self.resolve();
        Pending {
            baseline: sealed.position,
            target: AppliedPosition {
                index: sealed.position.index + 1,
                ..sealed.position
            },
            command_digest: serialized_digest(&resolve).unwrap(),
            wal_payload: serde_json::to_string(&resolve).unwrap(),
        }
    }

    async fn rows(&self, store: &GroupStore, committed: bool) -> Vec<Value> {
        let machine = ParticipantMachine::new(store.clone()).unwrap();
        let mut observed = Vec::new();
        for (key, value) in self.records() {
            let bytes = machine.read_at(ts(12), key).await.unwrap();
            let got: Value = serde_json::from_slice(bytes.as_slice()).unwrap();
            if committed {
                assert_eq!(got, json!({"value": {"version": 11, "value": value}}));
            } else {
                assert_eq!(
                    got,
                    json!({"blocked": {"txn": self.txn, "manifest": self.manifest}})
                );
            }
            observed.push(got);
        }
        observed
    }
}

fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}

fn reply() -> ParticipantReply {
    ParticipantReply::Resolved {
        next_ordinal: 2,
        complete: true,
    }
}

fn shared(node: u64) -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        node,
        WorkerResources {
            max_threads: 4,
            ..Default::default()
        },
        SharedResourceConfig::default(),
    )
    .unwrap()
}

fn open_pool(
    root: &Path,
    topology: &Topology,
    node: u64,
    shared: Arc<WorkerSharedResources>,
) -> OltpPool {
    let identity = topology.identity(node);
    if !root.join("registered").is_file() {
        return OltpPool::open_development(root.join("pool"), identity, shared).unwrap();
    }
    let enrollment: WorkerEnrollment = read(root, "enrollment.json");
    let volume_marker = topology.volume_markers[&node];
    let volume_name = format!("native-durability-{volume_marker}");
    let label = root.join(".nabedra-docker-volume.json");
    if !label.is_file() {
        write(
            root,
            ".nabedra-docker-volume.json",
            &json!({"format_version": 1, "volume_name": volume_name,
                "volume_marker": volume_marker}),
        );
    }
    let mount = VerifiedPoolMount::verify(
        PoolMountExpectation {
            profile: PoolMountProfile::Docker { volume_marker },
            identity: identity.clone(),
            worker: WorkerIdentity::from(&enrollment),
            generation: 1,
            mount_path: root.to_owned(),
            provider_volume_id: format!("docker-volume:{volume_name}"),
            filesystem_uuid: String::new(),
        },
        None,
    )
    .unwrap();
    write(root, "mount.json", mount.observation());
    OltpPool::open_registered(mount, identity, shared).unwrap()
}

async fn healthy_control(
    root: &Path,
    topology: &Topology,
    node: u64,
    shared: Arc<WorkerSharedResources>,
) -> Vote {
    let pool = OltpPool::open_development(
        root.join("control"),
        PoolIdentity {
            pool_id: topology.control_pools[&node],
            ..topology.identity(node)
        },
        shared,
    )
    .unwrap();
    let group = pool
        .group(GroupIdentity {
            group_id: topology.control_group_id,
            kind: GroupKind::Control,
            ..topology.group.clone()
        })
        .await
        .unwrap();
    let mut log = TypedLogStore::<ControlConfig>::new(group, node)
        .await
        .unwrap();
    let vote = Vote::new_committed(1, node);
    log.save_vote(&vote).await.unwrap();
    assert_eq!(log.read_vote().await.unwrap(), Some(vote));
    vote
}

async fn verify(
    topology: &Topology,
    pending: &Pending,
    log: &mut TypedLogStore<DataConfig>,
    committed: bool,
) -> Value {
    let state = log.group().applied_state(META).await.unwrap().unwrap();
    let expected = if committed {
        pending.target
    } else {
        pending.baseline
    };
    assert_eq!(state.position, expected);
    assert_eq!(log.read_committed().await.unwrap(), Some(log_id(expected)));
    let rows = topology.rows(log.group(), committed).await;
    if !committed {
        return json!({"applied": state.position, "records": rows});
    }
    assert_eq!(state.command_digest, pending.command_digest);
    let stored = log
        .group()
        .transaction(
            ApplyMeta {
                position: state.position,
                command_digest: state.command_digest,
                membership: None,
            },
            META,
            |_| panic!("exact replay ran the mutation again"),
        )
        .await
        .unwrap();
    let stored: ParticipantReply = serde_json::from_slice(stored.as_slice()).unwrap();
    assert_eq!(stored, reply());
    json!({"applied": state.position, "command_digest": state.command_digest,
        "records": rows, "result": stored})
}

async fn verify_log(log: &mut TypedLogStore<DataConfig>, pending: &Pending) {
    let entries = log
        .try_get_log_entries(pending.target.index..=pending.target.index)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, log_id(pending.target));
    let Payload::Normal(command) = entries[0].payload() else {
        panic!("expected retained resolve command");
    };
    assert_eq!(
        serde_json::to_string(command.payload().unwrap()).unwrap(),
        pending.wal_payload
    );
}

async fn offline(base: &Path, root: &Path, topology: &Topology, node: u64, mode: &str) {
    let pending: Pending = read(base, "prepared.json");
    let shared = shared(node);
    if mode == "corrupt" {
        let corruption = root.join("corrupt");
        let probe = (corruption.clone(), root.join("corrupt-probe"));
        let native = tokio::task::spawn_blocking(move || probe_native_store(&probe.0, &probe.1))
            .await
            .unwrap();
        let refused = OltpPool::open_development(corruption, topology.identity(node), shared)
            .err()
            .expect("OLTP open accepted corrupt voter WAL");
        assert_eq!(refused.code, ErrorCode::StorageUnavailable);
        assert_eq!(refused.message, "OLTP storage unavailable or corrupt");
        write(
            root,
            "corrupt-result.json",
            &json!({"native": native, "pool_error": refused}),
        );
        return;
    }
    let pool = open_pool(root, topology, node, shared);
    assert!(!pool.requires_recovery());
    let group = pool.retained_group(topology.group.clone()).await.unwrap();
    let mut log = TypedLogStore::<DataConfig>::new(group, node).await.unwrap();
    verify_log(&mut log, &pending).await;
    let observation = verify(topology, &pending, &mut log, mode == "recovered").await;
    write(root, &format!("{mode}-result.json"), &observation);
}

/// Opens a copy of the voter's `nabedra-kv` store (`<pool>/kv`, plan section 2 "Files"),
/// so recovery cannot repair the copy that the pool assertion opens next. The store must
/// detect the flipped journal byte: either refuse to open (R1, `Corruption`) or open with
/// the damaged group quarantined (G1). Opening clean, for example by truncating the damage
/// as a torn tail (L2), fails the fixture.
fn probe_native_store(pool: &Path, probe: &Path) -> Value {
    fn copy_tree(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let _ = fs::remove_dir_all(probe);
    copy_tree(&pool.join("kv"), probe);
    let options = nabedra_kv::Options::new(
        nabedra_kv::fs::real(),
        Arc::new(nabedra_kv::BlockCache::new(1 << 20)),
        Arc::new(nabedra_kv::MemtableBudget::new(8 << 20)),
        nabedra_kv::Background::new(1),
    );
    let outcome = match nabedra_kv::Store::open(probe, options) {
        Err(error @ nabedra_kv::Error::Corruption { .. }) => {
            json!({"refused": error.to_string()})
        }
        Err(error) => panic!("native open failed without reporting corruption: {error}"),
        Ok(opened) => {
            let damaged: Vec<_> = opened
                .quarantined
                .iter()
                .filter(|damage| damage.reason == nabedra_kv::DamageReason::JournalRecord)
                .map(|damage| format!("{damage:?}"))
                .collect();
            assert!(
                !damaged.is_empty(),
                "native open accepted corrupt voter journal: {:?}",
                opened.recovery
            );
            json!({"quarantined": damaged})
        }
    };
    fs::remove_dir_all(probe).unwrap();
    outcome
}

fn check_replication(data: &ManagedData, pending: &Pending) -> Value {
    let metrics = data.raft.metrics();
    let metrics = metrics.borrow_watched();
    assert!(metrics.running_state.is_ok(), "{}", &*metrics);
    assert_eq!(metrics.current_leader, Some(1), "{}", &*metrics);
    assert_eq!(metrics.state, ServerState::Leader);
    assert_eq!(
        metrics
            .committed_membership_config
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let base = log_id(pending.baseline);
    assert_eq!(metrics.current_term, pending.baseline.term);
    assert_eq!(metrics.last_log_index, Some(pending.target.index));
    assert_eq!(metrics.local_committed, Some(base));
    assert_eq!(metrics.cluster_committed, Some(base));
    assert_eq!(metrics.last_applied, Some(base));
    let replication = metrics.replication.as_ref().unwrap();
    assert_eq!(replication.get(&1), Some(&Some(log_id(pending.target))));
    assert_eq!(replication.get(&2), Some(&Some(base)));
    assert_eq!(replication.get(&3), Some(&Some(base)));
    json!({"term": metrics.current_term, "leader": metrics.current_leader,
        "last_log": metrics.last_log_index, "committed": metrics.local_committed.map(position),
        "applied": metrics.last_applied.map(position), "follower_2_match": replication[&2].map(position),
        "follower_3_match": replication[&3].map(position)})
}

fn trace_voter(data: &ManagedData, root: &Path) {
    let mut metrics = data.raft.metrics();
    let root = root.to_owned();
    tokio::spawn(async move {
        let mut previous = Value::Null;
        loop {
            let observed = {
                let metrics = metrics.borrow_and_update();
                json!({
                    "node": metrics.id,
                    "state": format!("{:?}", metrics.state),
                    "running_state": format!("{:?}", metrics.running_state),
                    "term": metrics.current_term,
                    "vote": metrics.vote,
                    "leader": metrics.current_leader,
                    "last_log_index": metrics.last_log_index,
                    "local_committed": metrics.local_committed.map(position),
                    "cluster_committed": metrics.cluster_committed.map(position),
                    "last_applied": metrics.last_applied.map(position),
                    "effective_membership": metrics.membership_config.as_ref(),
                    "committed_membership": metrics.committed_membership_config.as_ref(),
                    "replication": metrics.replication,
                })
            };
            if previous != observed {
                tracing::info!(raft = %observed, "voter metrics changed");
                write(&root, "raft.json", &observed);
                previous = observed;
            }
            if metrics.changed().await.is_err() {
                break;
            }
        }
    });
}

async fn initial_quorum(data: &ManagedData, topology: &Topology) -> AppliedPosition {
    // initialize() starts the first campaign and returns after the local membership flush.
    data.raft
        .initialize(topology.members.clone())
        .await
        .expect("initialize RF3 membership");
    let metrics = data
        .raft
        .wait(Some(WAIT))
        .metrics(
            |metrics| {
                metrics.running_state.is_err()
                    || (metrics.state == ServerState::Leader
                        && metrics.current_leader == Some(1)
                        && metrics.last_applied.is_some_and(|id| {
                            let applied = position(id);
                            applied.index > 0
                                && applied.term == metrics.current_term
                                && applied.node_id == 1
                                && metrics.local_committed == Some(id)
                                && metrics.cluster_committed == Some(id)
                        }))
            },
            "initial leader's current-term entry commits and applies",
        )
        .await
        .unwrap();
    assert!(metrics.running_state.is_ok(), "{:?}", metrics.running_state);
    let membership = timeout(WAIT, data.observe_membership())
        .await
        .expect("initial RF3 ReadIndex timed out")
        .expect("initial RF3 ReadIndex and durable membership");
    assert_eq!(membership.group(), &topology.group);
    let members: Vec<_> = topology.members.values().cloned().collect();
    assert_eq!(membership.replicas(), members.as_slice());
    assert_eq!(
        membership.applied_at(),
        position(metrics.last_applied.unwrap())
    );
    membership.applied_at()
}

async fn verify_initial_voter(
    data: &ManagedData,
    topology: &Topology,
    initial: AppliedPosition,
    enrollment: &WorkerEnrollment,
) -> Value {
    let id = log_id(initial);
    let metrics = data
        .raft
        .wait(Some(WAIT))
        .metrics(
            |metrics| {
                metrics.running_state.is_err()
                    || (metrics.last_applied == Some(id)
                        && metrics.local_committed == Some(id)
                        && metrics.cluster_committed == Some(id))
            },
            "voter applies the initial strong-read position",
        )
        .await
        .unwrap();
    assert!(metrics.running_state.is_ok(), "{:?}", metrics.running_state);
    assert_eq!(metrics.current_leader, Some(1));
    let (applied, stored) = data.state.clone().applied_state().await.unwrap();
    assert_eq!(applied, Some(id));
    assert_eq!(data.log.clone().read_committed().await.unwrap(), Some(id));
    assert_eq!(&stored, metrics.committed_membership_config.as_ref());
    let members: BTreeMap<_, _> = stored
        .membership()
        .nodes()
        .map(|(id, member)| (*id, member.clone()))
        .collect();
    assert_eq!(members, topology.members);
    let voters = stored.membership().voter_ids().collect::<Vec<_>>();
    assert_eq!(voters, [1, 2, 3]);
    assert_eq!(stored.membership().get_joint_config().len(), 1);
    json!({
        "node": metrics.id, "leader": metrics.current_leader, "applied": initial,
        "committed": initial, "members": members, "voters": voters, "enrollment": enrollment,
        "group": topology.group,
    })
}

async fn live(base: &Path, root: &Path, topology: &Topology, node: u64, restarted: bool) {
    let shared = shared(node);
    let directory = base.join("tls");
    let tls = TlsMaterial::from_pem(
        &fs::read(directory.join(format!("node{node}.pem"))).unwrap(),
        &fs::read(directory.join(format!("node{node}.key"))).unwrap(),
        &fs::read(directory.join("ca.pem")).unwrap(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let enrolled: WorkerEnrollment = read(root, "identity.json");
    let enrollment = WorkerEnrollment {
        endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
        ..enrolled
    };
    assert_eq!(
        enrollment.certificate_sha256,
        Digest::parse(tls.certificate_sha256()).unwrap()
    );
    assert_eq!(enrollment.worker_id, topology.members[&node].worker_id);
    tracing::info!(node, enrollment = ?enrollment, "listening with pinned worker identity");
    write(root, "enrollment.json", &enrollment);
    let directory_name = if restarted {
        "peers-after.json"
    } else {
        "peers.json"
    };
    stage(base, directory_name).await;
    let workers: BTreeMap<Uuid, WorkerEnrollment> = read(base, directory_name);
    tracing::info!(node, group = %topology.group.group_id, peers = ?workers, "loaded pinned peer enrollments");
    let directory = Arc::new(Directory {
        cluster: topology.group.cluster_id,
        workers,
        local: enrollment.worker_id,
        blocked: Uuid::nil(),
    });
    let pool = open_pool(root, topology, node, shared.clone());
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let transport = Arc::new(
        WorkerTransport::new(
            &enrollment,
            &tls,
            shared.clone(),
            directory,
            registry.clone(),
            TransportConfig::default(),
        )
        .unwrap(),
    );
    let manager = HostedGroupManager::new(shared.clone(), transport.client(), registry);
    let transport = tokio::spawn(transport.run(listener, std::future::pending()));
    let group = if restarted {
        pool.retained_group(topology.group.clone()).await.unwrap()
    } else {
        pool.group(topology.group.clone()).await.unwrap()
    };
    let data = manager
        .start_data(
            group,
            topology.members[&node].clone(),
            topology.members.clone(),
            openraft::Config {
                // Only voter 1 may retry the bootstrap election. Replacement elections are staged.
                enable_elect: node == 1 && !restarted,
                enable_leader_restore: Some(false),
                heartbeat_interval: 500,
                election_timeout_min: 1_500,
                election_timeout_max: 3_000,
                snapshot_policy: SnapshotPolicy::Never,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    trace_voter(&data, root);
    if restarted {
        native::publish(root, "reattached", b"");
        stage(base, "reelect").await;
        if node == 2 {
            data.raft.runtime_config().elect(true);
            let metrics = data
                .raft
                .wait(Some(WAIT))
                .state(ServerState::Leader, "replacement voter")
                .await
                .unwrap();
            let pending: Pending = read(base, "prepared.json");
            assert!(metrics.current_term > pending.target.term);
            let barrier = data.ensure_linearizable().await.unwrap();
            assert!(barrier.applied.index > pending.target.index);
            verify_log(&mut data.log.clone(), &pending).await;
            let rows = topology.rows(data.state.store(), true).await;
            write(
                root,
                "reelected.json",
                &json!({"leader": node, "applied": barrier.applied,
                "retained": pending.target, "records": rows}),
            );
        }
        stage(base, "finish").await;
        manager.shutdown().await.unwrap();
        transport.abort();
        return;
    }

    native::publish(root, "started", b"");
    stage(base, "elect").await;
    if node == 1 {
        let initial = initial_quorum(&data, topology).await;
        write(base, "initial-leader.json", &initial);
    }
    stage(base, "initial-leader.json").await;
    let initial: AppliedPosition = read(base, "initial-leader.json");
    let observation = verify_initial_voter(&data, topology, initial, &enrollment).await;
    write(root, "initial.json", &observation);
    if node == 1 {
        stage(base, "prepare").await;
        let barrier = timeout(WAIT, data.ensure_linearizable())
            .await
            .expect("pre-Fence ReadIndex timed out")
            .expect("pre-Fence ReadIndex failed");
        assert_eq!(barrier.applied, initial);
        let pending = topology.prepare(&data).await;
        assert_eq!(pending.baseline.index, initial.index + 3);
        write(base, "prepared.json", &pending);
    }
    stage(base, "prepared.json").await;
    let pending: Pending = read(base, "prepared.json");
    data.raft
        .wait(Some(WAIT))
        .applied_index_at_least(Some(pending.baseline.index), "prepared transaction")
        .await
        .unwrap();
    let mut log = data.log.clone();
    let baseline = verify(topology, &pending, &mut log, false).await;
    if node == 1 {
        assert_eq!(
            data.ensure_linearizable().await.unwrap().applied,
            pending.baseline
        );
    }
    write(root, "baseline.json", &baseline);

    stage(base, "arm-voters").await;
    if node == 2 || node == 3 {
        native::arm(root, if node == 2 { "gate" } else { "post-sync-eio" });
    }
    stage(base, "propose").await;
    let proposal = (node == 1).then(|| {
        let data = data.clone();
        let command =
            OwnedCommand::new_owned(topology.resolve(), &shared, ByteClass::Data).unwrap();
        let logged_sync = base.join("voter-2/synced");
        let base = base.to_owned();
        let target = pending.target;
        tokio::spawn(async move {
            let applied = data.propose(command).await.unwrap();
            assert!(
                logged_sync.is_file(),
                "client succeeded before follower sync"
            );
            let Applied {
                position,
                reply: actual,
                ..
            } = &applied;
            assert_eq!(*position, target);
            assert_eq!(*actual, reply());
            write(
                &base,
                "acknowledged.json",
                &json!({"applied": position, "result": actual}),
            );
        })
    });
    stage(base, "inspect").await;
    if node == 3 {
        while !pool.requires_recovery() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let alias = data.state.store().clone();
        let refused = alias.applied_state(META).await.err().unwrap();
        assert_eq!(refused.code, ErrorCode::OutcomeUnknown);
        assert_eq!(refused.outcome, Outcome::Unknown);
        assert_eq!(
            pool.group(topology.group.clone()).await.err(),
            Some(refused)
        );
        write(
            root,
            "held.json",
            &json!({"recovery_required": pool.requires_recovery(),
            "healthy_control_vote": healthy_control(root, topology, node, shared).await}),
        );
        return std::future::pending().await;
    }
    if node == 2 {
        assert!(!root.join("synced").exists());
        let control = healthy_control(root, topology, node, shared).await;
        let state = log.group().applied_state(META).await.unwrap().unwrap();
        let committed = log.read_committed().await.unwrap();
        assert_eq!(state.position, pending.baseline);
        assert_eq!(committed, Some(log_id(pending.baseline)));
        assert!(!root.join("synced").exists());
        write(
            root,
            "held.json",
            &json!({"healthy_control_vote": control, "applied": state.position,
            "committed": committed.map(position)}),
        );
    }
    if let Some(proposal) = proposal {
        let wanted = Some(log_id(pending.target));
        loop {
            let metrics = data.raft.metrics();
            if metrics
                .borrow_watched()
                .replication
                .as_ref()
                .and_then(|matches| matches.get(&1))
                .copied()
                .flatten()
                == wanted
            {
                break;
            }
            assert!(!proposal.is_finished());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let observed = verify(topology, &pending, &mut log, false).await;
        let mut samples = Vec::new();
        for _ in 0..5 {
            assert!(!proposal.is_finished());
            assert!(!base.join("voter-2/synced").exists());
            samples.push(check_replication(&data, &pending));
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        write(
            root,
            "held.json",
            &json!({"raft": samples, "data": observed}),
        );
        proposal.await.unwrap();
    }
    data.raft
        .wait(Some(WAIT))
        .applied_index_at_least(Some(pending.target.index), "resolved transaction")
        .await
        .unwrap();
    verify_log(&mut log, &pending).await;
    let resolved = verify(topology, &pending, &mut log, true).await;
    if node == 1 {
        let barrier = data.ensure_linearizable().await.unwrap();
        assert_eq!(barrier.applied, pending.target);
        let metrics = data.raft.metrics();
        let metrics = metrics.borrow_watched();
        assert_eq!(metrics.cluster_committed, Some(log_id(pending.target)));
        let replication = metrics.replication.as_ref().unwrap();
        assert_eq!(replication[&2], Some(log_id(pending.target)));
        assert_eq!(replication[&3], Some(log_id(pending.baseline)));
    }
    write(root, "resolved.json", &resolved);
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "run through oltp_replicated_durability.py on Linux"]
async fn o06_o11_o29_native_wal_ordering_and_recovery_with_three_tls_voters() {
    use tracing::Level;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false)
                .with_filter(
                    Targets::new()
                        .with_target("openraft", Level::INFO)
                        .with_target(
                            "nabedra_database_server::oltp_replication::typed_network",
                            Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::typed_log",
                            Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::hosted",
                            Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::multiplex::transport",
                            Level::DEBUG,
                        ),
                ),
        )
        .try_init()
        .expect("install voter tracing");
    timeout(Duration::from_secs(120), async {
        let root = native::root();
        let mode: String = read(&root, "mode.json");
        if mode == "setup" {
            setup(&root);
            return;
        }
        let base = root.parent().unwrap();
        let topology: Topology = read(base, "topology.json");
        let node = root
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("voter-")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        match mode.as_str() {
            "live" | "restart" => live(base, &root, &topology, node, mode == "restart").await,
            "corrupt" | "fault-recovered" | "recovered" => {
                offline(base, &root, &topology, node, &mode).await;
            }
            _ => panic!("unknown fixture mode {mode}"),
        }
    })
    .await
    .expect("replicated native durability watchdog");
}
