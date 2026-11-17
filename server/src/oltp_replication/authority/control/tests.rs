//! Direct real-storage state assertions. These do not establish quorum reads;
//! the hosted multi-group integration exercises the actual Consensus adapter.
use super::{
    machine::*,
    model::*,
    service::{ControlHandle, OperatorPolicy},
};
use crate::{
    config::WorkerResources,
    oltp::pool::{
        ApplyMeta, ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources,
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, Consensus, ConsensusFuture, ReadBarrier},
            timestamp::ReadyFenceReceipt,
        },
        owned::{OwnedCommand, serialized_digest},
        types::*,
    },
    oltp_runtime::{replica::QueueScope, worker_endpoint::AuthorityReader},
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-control-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Fixture {
    machine: ControlMachine,
    next: u64,
    base: u128,
}
impl Fixture {
    async fn open(directory: &Directory, scope: GroupIdentity, pool: PoolIdentity) -> Self {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(&directory.0, pool, shared).unwrap();
        let store = pool.group(scope).await.unwrap();
        let next = store
            .applied_state(BUDGET)
            .await
            .unwrap()
            .map_or(0, |p| p.position.index + 1);
        Self {
            machine: ControlMachine::new(store).unwrap(),
            next,
            base: (Uuid::new_v4().as_u128() >> 64) << 64,
        }
    }
    async fn apply(&mut self, command: ControlCommand) -> ControlReply {
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: self.next,
        };
        self.next += 1;
        let digest = serialized_digest(&command).unwrap();
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )
        .unwrap();
        let bytes = self
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: digest,
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        decode(bytes.as_ref()).unwrap()
    }
    async fn state(&self) -> State {
        let bytes = self
            .machine
            .store()
            .read(BUDGET, |view| {
                let state: State = decode(view.get(STATE)?.unwrap().as_ref())?;
                view.encode(&state)
            })
            .await
            .unwrap();
        decode(bytes.as_ref()).unwrap()
    }
    async fn initialize(&mut self) -> ReadyMembership {
        let result = self
            .apply(ControlCommand::Initialize {
                operation_id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
                authorization: authorization(),
            })
            .await;
        let ControlReply::Initialized { ready } = result else {
            panic!("unexpected initialization reply: {result:?}")
        };
        ready
    }
    async fn enroll(&mut self, number: u128) -> WorkerIdentity {
        let worker = worker(self.base + number);
        let result = self
            .apply(ControlCommand::Enroll {
                operation_id: Uuid::new_v4(),
                expected: None,
                next: worker.clone(),
                authorization: authorization(),
                previous_process: None,
            })
            .await;
        assert_eq!(
            result,
            ControlReply::Enrolled(WorkerIdentity::from(&worker))
        );
        WorkerIdentity::from(&worker)
    }
    async fn register_group(
        &mut self,
        kind: GroupKind,
        members: Vec<ReplicaMember>,
    ) -> GroupRecord {
        let generation = Uuid::new_v4();
        let group = GroupIdentity {
            group_id: Uuid::new_v4(),
            kind,
            ..self.machine.store().identity().clone()
        };
        assert!(matches!(
            self.apply(ControlCommand::BeginGroup {
                operation_id: Uuid::new_v4(),
                generation,
                identity: group.clone(),
                authorization: authorization()
            })
            .await,
            ControlReply::GroupBuilding { .. }
        ));
        let mut chain = group_seed();
        for page in members.chunks(128) {
            assert!(matches!(
                self.apply(ControlCommand::GroupPage {
                    operation_id: Uuid::new_v4(),
                    generation,
                    replicas: Page::new(page.to_vec()).unwrap()
                })
                .await,
                ControlReply::GroupBuilding { .. }
            ));
            for member in page {
                chain = group_step(chain, member);
            }
        }
        let applied_at = AppliedPosition {
            term: 1,
            node_id: 3,
            index: 10,
        };
        let root = group_root(members.len() as u64, chain);
        let authority = self.state().await.ready.view.authority();
        assert!(matches!(
            self.apply(ControlCommand::SealGroup {
                operation_id: Uuid::new_v4(),
                generation,
                proof: GroupMembershipStamp {
                    group: group.clone(),
                    generation,
                    replica_count: members.len() as u64,
                    replica_root: root,
                    applied_at,
                    authority
                }
            })
            .await,
            ControlReply::GroupRegistered { .. }
        ));
        GroupRecord {
            identity: group,
            generation,
            replica_count: members.len() as u64,
            replica_root: root,
            membership_position: applied_at,
        }
    }
    async fn add_ready(
        &mut self,
        target: WorkerIdentity,
        current_members: &mut Vec<WorkerIdentity>,
    ) -> ReadyMembership {
        let state = self.state().await;
        let id = Uuid::new_v4();
        let result = self
            .apply(ControlCommand::BeginJoin {
                operation_id: id,
                expected: state.ready.view,
                target,
            })
            .await;
        let ControlReply::JoinBegun { ticket, .. } = result else {
            panic!("unexpected join reply: {result:?}")
        };
        current_members.push(target);
        current_members.sort_by_key(|w| w.worker_id);
        for page in current_members.chunks(128) {
            assert!(matches!(
                self.apply(ControlCommand::ReadyPage {
                    operation_id: Uuid::new_v4(),
                    transition_id: id,
                    members: Page::new(page.to_vec()).unwrap()
                })
                .await,
                ControlReply::ReadyBuilding { .. }
            ));
        }
        let result = self
            .apply(ControlCommand::SealReady {
                operation_id: Uuid::new_v4(),
                transition_id: id,
            })
            .await;
        let ControlReply::ReadySealed { expected, next, .. } = result else {
            panic!("unexpected seal: {result:?}")
        };
        let receipt = ReadyFenceReceipt {
            transition_id: id,
            expected,
            next,
            applied_at: AppliedPosition {
                term: 1,
                node_id: 2,
                index: 100,
            },
        };
        self.apply(ControlCommand::TimestampFenced {
            operation_id: Uuid::new_v4(),
            transition_id: id,
            receipt,
        })
        .await;
        let proof = QueueCopyStatement {
            ticket,
            source: state.owner.unwrap_or(target),
            snapshot_id: Uuid::new_v4(),
            snapshot_cut: 0,
            final_cut: 0,
            record_version_hash: hash(b"nabedra/ram-queue-cut/v1\0"),
        };
        assert_eq!(
            self.apply(ControlCommand::PublishReady {
                operation_id: Uuid::new_v4(),
                transition_id: id,
                proof
            })
            .await,
            ControlReply::Published {
                ready: next,
                owner: state.owner.or(Some(target))
            }
        );
        next
    }
}
fn identities() -> (GroupIdentity, PoolIdentity) {
    let cluster = Uuid::new_v4();
    (
        GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("default").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        },
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: 1,
            cluster_id: cluster,
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        },
    )
}
fn authorization() -> AuthorizationStamp {
    AuthorizationStamp {
        authorization_id: Uuid::new_v4(),
        policy: Digest::from_bytes([1; 32]),
        subject: Digest::from_bytes([2; 32]),
        evidence: Digest::from_bytes([3; 32]),
    }
}
fn worker(number: u128) -> WorkerEnrollment {
    WorkerEnrollment {
        worker_id: Uuid::from_u128(number),
        incarnation: Uuid::new_v4(),
        certificate_sha256: hash(&number.to_be_bytes()),
        endpoint: Text::try_from("127.0.0.1:1").unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
        enrollment_revision: 1,
    }
}

#[tokio::test]
async fn r03_control_pending_timestamp_fence_and_publication_survive_each_reopen() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let initial = fixture.initialize().await;
    let target = fixture.enroll(1).await;
    let id = Uuid::new_v4();
    let ControlReply::JoinBegun { ticket, .. } = fixture
        .apply(ControlCommand::BeginJoin {
            operation_id: id,
            expected: initial.view,
            target,
        })
        .await
    else {
        panic!("ticket")
    };
    fixture
        .apply(ControlCommand::ReadyPage {
            operation_id: Uuid::new_v4(),
            transition_id: id,
            members: Page::new(vec![target]).unwrap(),
        })
        .await;
    let ControlReply::ReadySealed { expected, next, .. } = fixture
        .apply(ControlCommand::SealReady {
            operation_id: Uuid::new_v4(),
            transition_id: id,
        })
        .await
    else {
        panic!("seal")
    };
    let proof = QueueCopyStatement {
        ticket,
        source: target,
        snapshot_id: Uuid::new_v4(),
        snapshot_cut: 0,
        final_cut: 0,
        record_version_hash: hash(b"nabedra/ram-queue-cut/v1\0"),
    };
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishReady {
                operation_id: Uuid::new_v4(),
                transition_id: id,
                proof: proof.clone()
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    assert_eq!(fixture.state().await.ready, initial);
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let receipt = ReadyFenceReceipt {
        transition_id: id,
        expected,
        next,
        applied_at: AppliedPosition {
            term: 1,
            node_id: 7,
            index: 42,
        },
    };
    let ack = ControlCommand::TimestampFenced {
        operation_id: Uuid::new_v4(),
        transition_id: id,
        receipt: receipt.clone(),
    };
    let first = fixture.apply(ack.clone()).await;
    assert_eq!(
        first,
        ControlReply::TimestampFenced {
            transition_id: id,
            next
        }
    );
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    assert_eq!(fixture.apply(ack).await, first);
    assert_eq!(fixture.state().await.ready, initial);
    let op = Uuid::new_v4();
    let publish = ControlCommand::PublishReady {
        operation_id: op,
        transition_id: id,
        proof,
    };
    let published = fixture.apply(publish.clone()).await;
    assert_eq!(
        published,
        ControlReply::Published {
            ready: next,
            owner: Some(target)
        }
    );
    assert_eq!(fixture.apply(publish).await, published);
    assert_eq!(fixture.state().await.ready, next);
    let mut members = Vec::new();
    fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let state: State = decode(view.get(STATE)?.unwrap().as_ref())?;
            view.scan_page(
                &ready_prefix(state.ready_generation),
                None,
                128,
                |_, bytes| {
                    members.push(decode::<WorkerIdentity>(bytes)?);
                    Ok(())
                },
            )?;
            assert_eq!(members, vec![target]);
            view.encode(&())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn o06_control_enrollment_cas_requires_exclusion_and_preserves_permanent_group_nodes() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let old = fixture.enroll(1).await;
    let mut members = Vec::new();
    fixture.add_ready(old, &mut members).await;
    let permanent = ReplicaMember {
        node_id: 41,
        worker_id: old.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let group = fixture
        .register_group(GroupKind::Timestamps, vec![permanent.clone()])
        .await;
    let mut next = worker(old.worker_id.as_u128());
    next.enrollment_revision = 2;
    assert_eq!(
        fixture
            .apply(ControlCommand::Enroll {
                operation_id: Uuid::new_v4(),
                expected: Some(old),
                next: next.clone(),
                authorization: authorization(),
                previous_process: None
            })
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    let excluded = ExclusionStamp {
        worker: old,
        authorization: authorization(),
        ownership_fence: Digest::from_bytes([8; 32]),
    };
    assert_eq!(
        fixture
            .apply(ControlCommand::Enroll {
                operation_id: Uuid::new_v4(),
                expected: Some(old),
                next: next.clone(),
                authorization: authorization(),
                previous_process: Some(excluded)
            })
            .await,
        ControlReply::Enrolled(WorkerIdentity::from(&next))
    );
    let state = fixture.state().await;
    assert_eq!(state.invalid_ready, 1);
    assert_eq!(state.ready.ready_count, 1);
    let bytes = fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let member = view
                .get(&ready_key(state.ready_generation, old.worker_id))?
                .unwrap();
            view.copy_output(member.as_ref())
        })
        .await
        .unwrap();
    assert_eq!(decode::<WorkerIdentity>(bytes.as_ref()).unwrap(), old);
    let bytes = fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let member = view
                .get(&member_key(group.generation, permanent.node_id))?
                .unwrap();
            assert_eq!(decode::<ReplicaMember>(member.as_ref())?, permanent);
            view.encode(&())
        })
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"null");
}

/// Model only: forwards to real native storage, not a quorum/leader shortcut
/// used in production. The hosted integration supplies actual OpenRaft.
struct NativeModelConsensus {
    machine: ControlMachine,
    next: AtomicU64,
    reads: AtomicU64,
}
impl Consensus<ControlCommand, ControlReply> for NativeModelConsensus {
    fn propose(
        &self,
        command: OwnedCommand<ControlCommand>,
    ) -> ConsensusFuture<'_, Applied<ControlReply>> {
        Box::pin(async move {
            let position = AppliedPosition {
                term: 1,
                node_id: 1,
                index: self.next.fetch_add(1, Ordering::AcqRel),
            };
            let digest = serialized_digest(&command)?;
            let result = self
                .machine
                .apply(
                    ApplyMeta {
                        position,
                        command_digest: digest,
                        membership: None,
                    },
                    command,
                )
                .await?;
            let owner = Arc::new(
                self.machine
                    .store()
                    .resources()
                    .reserve_scratch(ByteClass::Progress, 4096)?,
            );
            Ok(Applied::owned(position, decode(result.as_ref())?, owner))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let applied = self
                .machine
                .store()
                .applied_state(BUDGET)
                .await?
                .unwrap()
                .position;
            Ok(ReadBarrier { applied })
        })
    }
}

#[tokio::test]
async fn u02_control_model_inventory_pages_empty_intermediate_final_and_exhausted_without_total_cap()
 {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let empty = handle.workers(0, None).await.unwrap();
    let empty: super::service::InventoryPage<WorkerEnrollment> = decode(empty.as_ref()).unwrap();
    assert!(empty.done && empty.items.is_empty());
    for id in 1..=300 {
        fixture.enroll(id).await;
    }
    let mut after = None;
    let mut received = 0;
    for (count, done) in [(128, false), (128, false), (44, true), (0, true)] {
        let page = handle.workers(300, after).await.unwrap();
        let page: super::service::InventoryPage<WorkerEnrollment> = decode(page.as_ref()).unwrap();
        assert_eq!(page.items.len(), count);
        assert_eq!(page.done, done);
        received += page.items.len();
        after = page.after;
    }
    assert_eq!(received, 300);
    assert_eq!(model.reads.load(Ordering::Acquire), 5);
    assert!(handle.workers(299, None).await.is_err());
    let wrong = QueueScope {
        cluster_id: Uuid::new_v4(),
        database: Text::try_from("default").unwrap(),
    };
    assert!(handle.current(&wrong).await.is_err());
}

#[tokio::test]
async fn c07_terminal_reader_requires_current_enrollment_and_same_database_tso_membership() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let reader = fixture.enroll(2).await;
    fixture.add_ready(owner, &mut Vec::new()).await;
    let timestamps = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 7,
                worker_id: reader.worker_id,
                pool_id: Uuid::new_v4(),
            }],
        )
        .await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database,
    };
    assert_eq!(handle.current(&queue_scope).await.unwrap().owner, owner);
    assert_eq!(
        handle
            .terminal_reader_view(&queue_scope, reader, timestamps.identity.group_id)
            .await
            .unwrap()
            .owner,
        owner
    );
    assert!(
        handle
            .terminal_reader_view(&queue_scope, owner, timestamps.identity.group_id)
            .await
            .is_err()
    );
    let mut obsolete = reader;
    obsolete.incarnation = Uuid::new_v4();
    assert!(
        handle
            .terminal_reader_view(&queue_scope, obsolete, timestamps.identity.group_id)
            .await
            .is_err()
    );
    assert!(
        handle
            .terminal_reader_view(&queue_scope, reader, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(model.reads.load(Ordering::Acquire), 5);
}

#[tokio::test]
async fn r04_control_cold_fence_waits_for_every_group_and_old_worker_exclusion() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let ready = fixture.add_ready(owner, &mut Vec::new()).await;
    let group = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 8,
                worker_id: owner.worker_id,
                pool_id: Uuid::new_v4(),
            }],
        )
        .await;
    let transition = Uuid::new_v4();
    let begin = ControlCommand::BeginColdFence {
        operation_id: transition,
        expected: ready,
        next_run: Uuid::new_v4(),
        authorization: authorization(),
    };
    let ControlReply::FenceBegun {
        next,
        required_groups,
        ..
    } = fixture.apply(begin.clone()).await
    else {
        panic!("cold fence")
    };
    assert_eq!(required_groups, 2);
    assert_ne!(next.view.run_id, ready.view.run_id);
    assert_eq!(fixture.state().await.ready, ready);
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishFence {
                operation_id: Uuid::new_v4(),
                transition_id: transition
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    let ack = ControlCommand::GroupFenced {
        operation_id: Uuid::new_v4(),
        receipt: GroupFenceReceipt {
            transition_id: transition,
            group: group.identity,
            expected: Some(ready.view.authority()),
            next: next.view.authority(),
            applied_at: AppliedPosition {
                term: 2,
                node_id: 8,
                index: 20,
            },
            command_digest: hash(b"fence proof"),
        },
    };
    let mut invalid = ack.clone();
    if let ControlCommand::GroupFenced { receipt, .. } = &mut invalid {
        receipt.next.run_id = ready.view.run_id;
    }
    assert_eq!(
        fixture.apply(invalid).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    assert!(matches!(
        fixture.apply(ack.clone()).await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 0,
            ..
        }
    ));
    assert!(matches!(
        fixture.apply(ack).await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 0,
            ..
        }
    ));
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishFence {
                operation_id: Uuid::new_v4(),
                transition_id: transition
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let proof = ExclusionStamp {
        worker: owner,
        authorization: authorization(),
        ownership_fence: hash(b"operator-verified ownership exclusion model"),
    };
    assert!(matches!(
        fixture
            .apply(ControlCommand::ExcludedPage {
                operation_id: Uuid::new_v4(),
                transition_id: transition,
                workers: Page::new(vec![proof]).unwrap()
            })
            .await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 1,
            ..
        }
    ));
    let publish = ControlCommand::PublishFence {
        operation_id: Uuid::new_v4(),
        transition_id: transition,
    };
    assert_eq!(
        fixture.apply(publish.clone()).await,
        ControlReply::Published {
            ready: next,
            owner: None
        }
    );
    assert_eq!(
        fixture.apply(publish).await,
        ControlReply::Published {
            ready: next,
            owner: None
        }
    );
    assert_eq!(
        fixture.apply(begin).await,
        ControlReply::FenceBegun {
            transition_id: transition,
            next,
            required_groups: 2
        }
    );
    assert_eq!(fixture.state().await.ready, next);
}

#[tokio::test]
async fn u02_control_administration_rejects_worker_lane_and_unpinned_certificate() {
    use crate::oltp_replication::multiplex::{AuthenticatedSession, Purpose};
    let directory = Directory::new();
    let (scope, pool) = identities();
    let fixture = Fixture::open(&directory, scope.clone(), pool).await;
    let local = WorkerIdentity::from(&worker(1));
    let remote = WorkerIdentity::from(&worker(2));
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(0),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: [remote.certificate_sha256].into_iter().collect(),
        },
    );
    let wrong_lane = AuthenticatedSession::for_test(
        local,
        remote,
        scope.clone(),
        Purpose::QueueControl,
        Uuid::new_v4(),
    );
    assert!(
        handle
            .initialize(&wrong_lane, Uuid::new_v4(), Uuid::new_v4())
            .await
            .is_err()
    );
    let unknown = AuthenticatedSession::for_test(
        local,
        WorkerIdentity::from(&worker(3)),
        scope.clone(),
        Purpose::Authority,
        Uuid::new_v4(),
    );
    assert!(
        handle
            .initialize(&unknown, Uuid::new_v4(), Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(model.next.load(Ordering::Acquire), 0);
    let allowed =
        AuthenticatedSession::for_test(local, remote, scope, Purpose::Authority, Uuid::new_v4());
    let id = Uuid::new_v4();
    let run = Uuid::new_v4();
    let first = handle.initialize(&allowed, id, run).await.unwrap();
    assert!(matches!(first.reply, ControlReply::Initialized { .. }));
    let retry = handle.initialize(&allowed, id, run).await.unwrap();
    assert_eq!(first.reply, retry.reply);
}

#[tokio::test]
async fn c07_committed_control_apply_uses_private_budget_when_transport_progress_is_full() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let fixture = Fixture::open(&directory, scope, pool).await;
    let command = ControlCommand::Initialize {
        operation_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        authorization: authorization(),
    };
    let digest = serialized_digest(&command).unwrap();
    let shared = fixture.machine.store().resources();
    let command = OwnedCommand::new_owned(command, shared, ByteClass::Progress).unwrap();
    let available = shared.config().progress_bytes - shared.buffer_bytes_used(ByteClass::Progress);
    let saturated = shared
        .allocate_bytes(ByteClass::Progress, available)
        .unwrap();
    assert!(shared.allocate_bytes(ByteClass::Progress, 1).is_err());
    let result = fixture
        .machine
        .apply(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
                command_digest: digest,
                membership: None,
            },
            command,
        )
        .await
        .unwrap();
    assert!(matches!(
        decode::<ControlReply>(result.as_ref()).unwrap(),
        ControlReply::Initialized { .. }
    ));
    drop(saturated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c07_q06_control_authorized_terminal_metadata_progresses_with_query_slot_and_data_wire_full()
 {
    use crate::{
        oltp::pool::ExecutionClass,
        oltp_replication::multiplex::{AuthenticatedSession, ChargedSections, Endpoint, Purpose},
        oltp_runtime::{
            replica::*,
            worker_endpoint::{QueueEndpoint, QueueReply, QueueRequest},
        },
        request_queue::QueueLimits,
    };
    use tokio::time::{Duration, timeout};
    let directory = Directory::new();
    let (scope, pool) = identities();
    let worker_id = pool.worker_id;
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let enrollment = worker(worker_id.as_u128());
    let local = WorkerIdentity::from(&enrollment);
    fixture
        .apply(ControlCommand::Enroll {
            operation_id: Uuid::new_v4(),
            expected: None,
            next: enrollment,
            authorization: authorization(),
            previous_process: None,
        })
        .await;
    let reader = fixture.enroll(2).await;
    fixture.add_ready(local, &mut Vec::new()).await;
    let timestamps = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 2,
                worker_id: reader.worker_id,
                pool_id: Uuid::new_v4(),
            }],
        )
        .await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model,
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
    };
    let authority = handle.current(&queue_scope).await.unwrap();
    let shared = fixture.machine.store().resources().clone();
    let mut replica = WorkerReplica::new(
        shared.clone(),
        queue_scope.clone(),
        local,
        authority,
        [local],
        QueueLimits {
            max_pending_requests: 4,
            max_queue_bytes_per_replica: 1 << 20,
            max_retained_result_bytes_per_worker: 9 << 20,
            result_retention_ms: 1000,
        },
    )
    .unwrap();
    let id = Uuid::new_v4();
    let body =
        format!(r#"{{"request_id":"{id}","operation":"table.list","payload":{{}}}}"#).into_bytes();
    let digest = hash(&body);
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: scope.database.clone(),
            principal: Digest::from_bytes([3; 32]),
            policy_revision: 1,
            ingress_id: local.worker_id,
            ingress_incarnation: local.incarnation,
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: PinIdentity {
                run_id: authority.ready.view.run_id,
                request_id: id,
                digest,
                nonce: Uuid::new_v4(),
            },
            storage_pin: StoragePin {
                pin_id: Uuid::new_v4(),
                read_ts: Timestamp::new(1).unwrap(),
                allocator_epoch: 1,
            },
            catalog_root: Digest::from_bytes([4; 32]),
            schema_epoch: 1,
            routing_epoch: 1,
        }),
    };
    fn copy_local(replica: &mut WorkerReplica, authority: QueueAuthority, mut frame: RecordFrame) {
        loop {
            let ack = replica.install(authority, authority.owner, &frame).unwrap();
            match replica.acknowledge(authority, ack).unwrap() {
                Some(next) => frame = next,
                None => break,
            }
        }
    }
    let frame = replica.propose(authority, body, context, 1).unwrap();
    copy_local(&mut replica, authority, frame);
    let frame = replica.start(authority, id).unwrap();
    copy_local(&mut replica, authority, frame);
    let prefix = format!(r#"{{"request_id":"{id}","data":""#);
    let mut response = prefix.into_bytes();
    response.resize(crate::protocol::MAX_BODY_BYTES - 2, b'x');
    response.extend_from_slice(b"\"}");
    assert_eq!(response.len(), crate::protocol::MAX_BODY_BYTES);
    let input = replica.terminal_input(id).unwrap();
    // No queue lock is held during this native response validation/hash.
    let prepared = shared
        .execute(
            ExecutionClass::Query,
            931,
            (2 * response.len()) as u64,
            move || {
                PreparedTerminal::prepare(input, 200, response, 2)
                    .map_err(crate::request_queue::QueueError::protocol)
            },
        )
        .await
        .unwrap();
    let frame = replica.finish_prepared(authority, prepared).unwrap();
    copy_local(&mut replica, authority, frame);
    let expected = replica.terminal_copy_statement(id).unwrap();
    let endpoint = QueueEndpoint::new(
        Arc::new(std::sync::Mutex::new(replica)),
        shared.clone(),
        queue_scope.clone(),
        handle,
        512 << 20,
    )
    .unwrap();
    let occupied_query = shared.prepare(ExecutionClass::Query).await.unwrap();
    let full_data = shared
        .allocate_bytes(
            ByteClass::Data,
            shared.config().control_bytes - shared.config().progress_bytes,
        )
        .unwrap();
    let header = shared
        .encode_in(
            ByteClass::Progress,
            &QueueRequest::TerminalState {
                scope: queue_scope,
                view: authority.ready.view,
                request_id: id,
                timestamps_group_id: timestamps.identity.group_id,
            },
            MAX_HEADER_BYTES,
        )
        .unwrap();
    let session =
        AuthenticatedSession::for_test(local, reader, scope, Purpose::QueueControl, Uuid::new_v4());
    let plan = timeout(
        Duration::from_secs(2),
        endpoint.preflight(&session, header.as_ref()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(plan.section_lengths, [0; 3]);
    assert_eq!(plan.decode_budget_bytes, 0);
    let sections =
        ChargedSections::for_test(&shared, ByteClass::Progress, [None, None, None], 0).unwrap();
    let reply = timeout(
        Duration::from_secs(2),
        endpoint.handle(session, header, sections),
    )
    .await
    .unwrap()
    .unwrap();
    let QueueReply::TerminalState { statement, .. } = decode(reply.header.as_ref()).unwrap() else {
        panic!("terminal metadata reply")
    };
    assert_eq!(*statement, expected);
    drop((reply, full_data, occupied_query, endpoint));
}
