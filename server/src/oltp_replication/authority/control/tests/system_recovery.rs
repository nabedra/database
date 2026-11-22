use super::*;
use crate::{
    oltp::pool::{
        GroupTransaction, Transition,
        mount::{PoolMountExpectation, PoolMountProfile},
    },
    oltp_replication::{
        authority::control::{
            ReplacementGroup, ReplacementIntent, ReplacementMountProof, ReplacementRecord,
            RestartMountKey, SystemRecoveryCommand,
            machine::system_recovery::{ACTIVE, DEFERRED_DATA, abandoned, by_worker, revoked_id},
        },
        multiplex::{AppliedPeerDirectory, PeerDirectory},
    },
    protocol::ProtocolError,
};

struct Recovery {
    fixture: Fixture,
    group: GroupRecord,
    voters: [WorkerIdentity; 3],
    members: [ReplicaMember; 3],
}

impl Recovery {
    async fn open(directory: &Directory) -> Self {
        let (scope, pool) = identities();
        let mut fixture = Fixture::open(directory, scope, pool).await;
        fixture.initialize().await;
        let voters = [
            fixture.enroll(1).await,
            fixture.enroll(2).await,
            fixture.enroll(3).await,
        ];
        fixture.add_ready(voters[0], &mut Vec::new()).await;
        let members = std::array::from_fn(|i| ReplicaMember {
            node_id: i as u64 + 1,
            worker_id: voters[i].worker_id,
            pool_id: Uuid::new_v4(),
        });
        let group = fixture
            .register_group(GroupKind::Control, members.to_vec())
            .await;
        let revision = fixture.state().await.enrollment_revision;
        assert!(matches!(
            fixture
                .apply(authorized(
                    serialized_digest(&(&group, voters, revision, true, true)).unwrap(),
                    |authorization| SystemRecoveryCommand::Enable {
                        group: Box::new(group.clone()),
                        execution: true,
                        hardening: true,
                        voters,
                        enrollment_revision: revision,
                        authorization,
                    },
                ))
                .await,
            ControlReply::SystemReplacementEnabled(_)
        ));
        Self {
            fixture,
            group,
            voters,
            members,
        }
    }

    async fn prepare(
        &mut self,
        previous: WorkerIdentity,
        suffix: u128,
    ) -> (ReplacementRecord, RestartMountKey) {
        self.prepare_groups(previous, suffix, vec![self.group.clone()])
            .await
    }

    async fn prepare_groups(
        &mut self,
        previous: WorkerIdentity,
        suffix: u128,
        mut records: Vec<GroupRecord>,
    ) -> (ReplacementRecord, RestartMountKey) {
        let key = RestartMountKey::from_seed(&[7; 32]).unwrap();
        let next = worker(self.fixture.base + suffix);
        let state = self.fixture.state().await;
        let intent = ReplacementIntent {
            id: Uuid::new_v4(),
            run_id: state.ready.view.run_id,
            previous,
            mount: PoolMountExpectation {
                profile: PoolMountProfile::Docker {
                    volume_marker: Uuid::new_v4(),
                },
                identity: PoolIdentity {
                    worker_id: next.worker_id,
                    pool_id: Uuid::new_v4(),
                    ..self.fixture.machine.store().pool().identity().clone()
                },
                worker: WorkerIdentity::from(&next),
                generation: 1,
                mount_path: "/test/replacement".into(),
                provider_volume_id: "docker-volume:replacement".into(),
                filesystem_uuid: String::new(),
            },
            next,
            mount_public_key: Some(key.public_key()),
        };
        records.sort_unstable_by_key(|record| record.identity.group_id);
        let groups = records
            .into_iter()
            .map(|record| ReplacementGroup::new(record, self.members.clone(), &intent).unwrap())
            .collect::<Vec<_>>();
        let evidence = serialized_digest(&(
            &intent,
            state.ready,
            state.enrollment_revision,
            state.group_inventory_revision,
            &groups,
        ))
        .unwrap();
        let reply = self
            .fixture
            .apply(authorized(evidence, |authorization| {
                SystemRecoveryCommand::Prepare {
                    intent: Box::new(intent),
                    ready: state.ready,
                    enrollment_revision: state.enrollment_revision,
                    group_inventory_revision: state.group_inventory_revision,
                    groups,
                    authorization,
                }
            }))
            .await;
        let ControlReply::SystemReplacementPrepared(record) = reply else {
            panic!("{reply:?}")
        };
        (*record, key)
    }

    async fn mount(&mut self, record: &ReplacementRecord, key: &RestartMountKey) -> ControlReply {
        let proof =
            ReplacementMountProof::sign(&self.group.identity, record, &record.intent.mount, key)
                .unwrap();
        let state = self.fixture.state().await;
        self.fixture
            .apply(ControlCommand::SystemRecovery {
                operation_id: Uuid::new_v4(),
                command: Box::new(SystemRecoveryCommand::Mount {
                    proof,
                    ready: state.ready,
                    enrollment_revision: state.enrollment_revision,
                    group_inventory_revision: state.group_inventory_revision,
                }),
            })
            .await
    }

    async fn publish(&mut self, record: &ReplacementRecord) {
        self.group = self.publish_group(record, &self.group.clone()).await;
        self.members = record
            .groups
            .iter()
            .find(|plan| plan.expected.identity == self.group.identity)
            .unwrap()
            .next(record.intent.previous.worker_id)
            .unwrap();
    }

    async fn publish_group(
        &mut self,
        record: &ReplacementRecord,
        expected: &GroupRecord,
    ) -> GroupRecord {
        let id = record.intent.id;
        let group = expected.identity.group_id;
        let applied = AppliedPosition {
            index: expected.membership_position.index + 10,
            ..expected.membership_position
        };
        let evidence = serialized_digest(&(id, group, applied)).unwrap();
        let reply = self
            .fixture
            .apply(authorized(evidence, |authorization| {
                SystemRecoveryCommand::Publish {
                    id,
                    group,
                    applied,
                    authorization,
                }
            }))
            .await;
        let ControlReply::SystemReplacementPublished(next) = reply else {
            panic!("{reply:?}")
        };
        assert_eq!(next.identity, expected.identity);
        assert_eq!(next.membership_position, applied);
        next
    }

    async fn seed(
        &mut self,
        set_up: impl FnOnce(&mut GroupTransaction<'_>) -> Result<(), ProtocolError> + Send + 'static,
    ) {
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: self.fixture.next,
        };
        self.fixture.next += 1;
        self.fixture
            .machine
            .store()
            .transaction(
                ApplyMeta {
                    position,
                    command_digest: hash(b"replacement test setup"),
                    membership: None,
                },
                BUDGET,
                move |tx| {
                    set_up(tx)?;
                    Ok(Transition::Commit(tx.encode(&true)?))
                },
            )
            .await
            .unwrap();
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, key: Vec<u8>) -> Option<T> {
        let raw = self
            .fixture
            .machine
            .store()
            .read(BUDGET, move |view| {
                let value = view.get(&key)?.map(|raw| raw.as_ref().to_vec());
                view.encode(&value)
            })
            .await
            .unwrap();
        decode::<Option<Vec<u8>>>(raw.as_ref())
            .unwrap()
            .map(|bytes| decode(&bytes).unwrap())
    }
}

fn authorized(
    evidence: Digest,
    command: impl FnOnce(AuthorizationStamp) -> SystemRecoveryCommand,
) -> ControlCommand {
    let operation_id = Uuid::new_v4();
    ControlCommand::SystemRecovery {
        operation_id,
        command: Box::new(command(AuthorizationStamp {
            authorization_id: operation_id,
            evidence,
            ..authorization()
        })),
    }
}

async fn cold_fence(recovery: &mut Recovery, groups: &[GroupRecord]) -> ReadyMembership {
    let expected = recovery.fixture.state().await.ready;
    let transition = Uuid::new_v4();
    let ControlReply::FenceBegun {
        next,
        required_groups,
        ..
    } = recovery
        .fixture
        .apply(ControlCommand::BeginColdFence {
            operation_id: transition,
            expected,
            next_run: Uuid::new_v4(),
            authorization: authorization(),
        })
        .await
    else {
        panic!("cold fence did not begin")
    };
    assert_eq!(required_groups, 1 + groups.len() as u64);
    for group in groups {
        assert!(matches!(
            recovery
                .fixture
                .apply(ControlCommand::GroupFenced {
                    operation_id: Uuid::new_v4(),
                    receipt: GroupFenceReceipt {
                        group: group.identity.clone(),
                        transition_id: transition,
                        expected: Some(expected.view.authority()),
                        next: next.view.authority(),
                        applied_at: group.membership_position,
                        command_digest: hash(b"cold group fence"),
                    },
                })
                .await,
            ControlReply::FenceProgress { .. }
        ));
    }
    assert!(matches!(
        recovery
            .fixture
            .apply(ControlCommand::ExcludedPage {
                operation_id: Uuid::new_v4(),
                transition_id: transition,
                workers: Page::new(vec![ExclusionStamp {
                    worker: recovery.voters[0],
                    authorization: authorization(),
                    ownership_fence: hash(b"verified old owner"),
                }])
                .unwrap(),
            })
            .await,
        ControlReply::FenceProgress {
            excluded_workers: 1,
            ..
        }
    ));
    assert_eq!(
        recovery
            .fixture
            .apply(ControlCommand::PublishFence {
                operation_id: Uuid::new_v4(),
                transition_id: transition,
            })
            .await,
        ControlReply::Published {
            ready: next,
            owner: None
        }
    );
    next
}

#[tokio::test]
async fn r04_o17_cold_publication_retires_the_unmounted_intent() {
    let directory = Directory::new();
    let mut recovery = Recovery::open(&directory).await;
    let (record, key) = recovery.prepare(recovery.voters[2], 4).await;
    let next = cold_fence(&mut recovery, &[]).await;
    assert_eq!(
        recovery.get::<bool>(abandoned(record.intent.id)).await,
        Some(true)
    );
    assert_eq!(
        recovery
            .get::<Uuid>(by_worker(record.intent.previous.worker_id))
            .await,
        None
    );
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    recovery
        .fixture
        .add_ready(recovery.voters[0], &mut Vec::new())
        .await;
    let (next_record, _) = recovery.prepare(recovery.voters[2], 5).await;
    assert_eq!(next_record.intent.run_id, next.view.run_id);
}

#[tokio::test]
async fn r04_o17_cold_fence_after_mount_completes_publication() {
    let directory = Directory::new();
    let mut recovery = Recovery::open(&directory).await;
    let timestamps = recovery
        .fixture
        .register_group(GroupKind::Timestamps, recovery.members.to_vec())
        .await;
    let (record, key) = recovery
        .prepare_groups(
            recovery.voters[2],
            4,
            vec![recovery.group.clone(), timestamps.clone()],
        )
        .await;
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::SystemReplacementMounted
    );
    let published = recovery.publish_group(&record, &timestamps).await;
    let next = cold_fence(&mut recovery, &[published.clone()]).await;
    assert_ne!(next.view.run_id, record.intent.run_id);
    assert_eq!(
        recovery.get::<Uuid>(ACTIVE.to_vec()).await,
        Some(record.intent.id)
    );
    assert_eq!(
        recovery.publish_group(&record, &timestamps).await,
        published
    );

    recovery.publish(&record).await;
    assert_eq!(recovery.get::<Uuid>(ACTIVE.to_vec()).await, None);
    assert!(
        recovery
            .members
            .iter()
            .any(|member| member.worker_id == record.intent.next.worker_id)
    );
    assert_eq!(recovery.fixture.state().await.ready, next);
    let (next_record, _) = recovery.prepare(recovery.voters[1], 5).await;
    assert_eq!(next_record.intent.run_id, next.view.run_id);
}

#[tokio::test]
async fn o17_revocation_limit_and_failed_refresh_preserve_the_committed_mount() {
    let directory = Directory::new();
    let mut recovery = Recovery::open(&directory).await;
    recovery
        .seed(|tx| {
            for i in 1..1024 {
                let revoked = WorkerIdentity::from(&worker(i));
                let key = revoked_id(revoked.worker_id);
                let raw = tx.encode_value(&revoked)?;
                tx.put(&key, raw.as_ref())?;
            }
            Ok(())
        })
        .await;
    let peers = recovery
        .voters
        .iter()
        .map(|identity| WorkerEnrollment {
            incarnation: identity.incarnation,
            ..worker(identity.worker_id.as_u128())
        })
        .collect();
    let directory_view = AppliedPeerDirectory::from_bootstrap(
        recovery.group.identity.clone(),
        peers,
        recovery.fixture.machine.store().resources().clone(),
    )
    .unwrap();
    directory_view
        .fail_replacement_trust_install
        .store(true, Ordering::Release);
    recovery.fixture.machine = recovery
        .fixture
        .machine
        .clone()
        .with_replacement_directory(directory_view.clone())
        .await
        .unwrap();
    let (record, key) = recovery.prepare(recovery.voters[2], 4).await;
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::SystemReplacementMounted
    );
    assert!(directory_view.revoked(recovery.voters[2]));
    assert!(!directory_view.revoked(recovery.voters[1]));
    assert!(!directory_view.has_mounted_replacement());
    directory_view
        .fail_replacement_trust_install
        .store(false, Ordering::Release);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !directory_view.has_mounted_replacement() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    recovery.publish(&record).await;
    let (record, key) = recovery.prepare(recovery.voters[1], 5).await;
    let before = recovery.fixture.state().await.enrollment_revision;
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::Rejected(Fault::Busy)
    );
    assert_eq!(recovery.fixture.state().await.enrollment_revision, before);
    assert!(
        recovery
            .get::<WorkerRecord>(super::key(WORKERS, record.intent.previous.worker_id))
            .await
            .is_some()
    );
    assert!(
        recovery
            .get::<WorkerRecord>(super::key(WORKERS, record.intent.next.worker_id))
            .await
            .is_none()
    );
    assert_eq!(recovery.get::<Uuid>(ACTIVE.to_vec()).await, None);
    assert!(!directory_view.revoked(recovery.voters[1]));
    assert!(!directory_view.has_mounted_replacement());

    // Import the extra revocation that REP4 could commit before stopping.
    let historical = WorkerIdentity::from(&worker(1024));
    recovery
        .seed(move |tx| {
            let raw = tx.encode_value(&historical)?;
            tx.put(&revoked_id(historical.worker_id), raw.as_ref())
        })
        .await;
    let survivor = recovery.voters[0];
    let restarted = AppliedPeerDirectory::from_bootstrap(
        recovery.group.identity.clone(),
        vec![WorkerEnrollment {
            incarnation: survivor.incarnation,
            ..worker(survivor.worker_id.as_u128())
        }],
        recovery.fixture.machine.store().resources().clone(),
    )
    .unwrap();
    recovery.fixture.machine = ControlMachine::new(recovery.fixture.machine.store().clone())
        .unwrap()
        .with_replacement_directory(restarted.clone())
        .await
        .unwrap();
    assert!(restarted.revoked(historical));
    assert!(!restarted.revoked(survivor));
}

#[tokio::test]
async fn o17_ns14_system_mount_hands_more_than_64_data_groups_to_the_damage_path() {
    let directory = Directory::new();
    let mut recovery = Recovery::open(&directory).await;
    let lost = recovery.members[2].clone();
    let (record, key) = recovery.prepare(recovery.voters[2], 4).await;
    let mut groups = Vec::new();
    for _ in 0..65 {
        groups.push(
            recovery
                .fixture
                .register_group(
                    GroupKind::Data {
                        table_id: Uuid::new_v4(),
                        shard_id: Uuid::new_v4(),
                    },
                    recovery.members.to_vec(),
                )
                .await,
        );
        if groups.len() == 63 {
            assert_eq!(
                recovery.mount(&record, &key).await,
                ControlReply::Rejected(Fault::Stale)
            );
        }
    }
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::SystemReplacementMounted
    );
    recovery.publish(&record).await;
    let mut pages = 0;
    while let Some(after) = recovery.get::<Option<Uuid>>(DEFERRED_DATA.to_vec()).await {
        assert_eq!(
            recovery
                .fixture
                .apply(authorized(
                    serialized_digest(&(b"queue-lost-data".as_slice(), after)).unwrap(),
                    |authorization| SystemRecoveryCommand::QueueLostData {
                        after,
                        authorization
                    },
                ))
                .await,
            ControlReply::SystemReplacementDataQueued,
        );
        pages += 1;
        assert!(pages <= 3);
    }
    assert_eq!(pages, 3);
    for group in groups {
        let saved = recovery
            .get::<super::super::GroupDamageRecord>(super::super::group_damage::record_key(
                group.identity.group_id,
                lost.node_id,
            ))
            .await
            .unwrap();
        assert_eq!(saved.report.group, group.identity);
        assert_eq!(saved.report.replica, lost);
        assert!(saved.pending());
    }
}

/// O17 (REP6): the page read that seeds and refreshes Data-only workers'
/// revocation views lists every committed system revocation, the set
/// Control hosts enforce, in worker order across pages.
#[tokio::test]
async fn o17_rep6_revocation_pages_list_every_committed_revocation() {
    let directory = Directory::new();
    let mut recovery = Recovery::open(&directory).await;
    let (record, key) = recovery.prepare(recovery.voters[2], 4).await;
    assert_eq!(
        recovery.mount(&record, &key).await,
        ControlReply::SystemReplacementMounted
    );
    let historical: Vec<WorkerIdentity> = (0..130)
        .map(|number| WorkerIdentity::from(&worker(5000 + number)))
        .collect();
    let seeded = historical.clone();
    recovery
        .seed(move |tx| {
            for worker in &seeded {
                let raw = tx.encode_value(worker)?;
                tx.put(&revoked_id(worker.worker_id), raw.as_ref())?;
            }
            Ok(())
        })
        .await;
    let handle = ControlHandle::new(
        recovery.fixture.machine.clone(),
        Arc::new(NativeModelConsensus {
            machine: recovery.fixture.machine.clone(),
            next: AtomicU64::new(recovery.fixture.next),
            reads: AtomicU64::new(0),
        }),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let mut listed = Vec::new();
    let mut after = None;
    let mut pages = 0;
    loop {
        let raw = handle.revocations(after).await.unwrap();
        let page: InventoryPage<WorkerIdentity> = serde_json::from_slice(raw.as_ref()).unwrap();
        pages += 1;
        listed.extend(page.items.iter().copied());
        assert_eq!(page.after, listed.last().map(|worker| worker.worker_id));
        after = page.after;
        if page.done {
            break;
        }
    }
    let mut expected = historical;
    expected.push(record.intent.previous);
    expected.sort_by_key(|worker| worker.worker_id);
    assert_eq!(listed, expected);
    assert_eq!(pages, 2);
}
