//! T7: the `data.direct.v1` Control capability (plan section 4, "Mixed
//! versions") on real Control storage.
use super::*;
use crate::oltp_replication::authority::control::{
    DIRECT_VERSION, DirectCapability, DirectCapabilityOperation,
};

impl Fixture {
    async fn direct(
        &mut self,
        actor: WorkerIdentity,
        action: DirectCapabilityOperation,
    ) -> ControlReply {
        self.apply(ControlCommand::DirectCapability {
            operation_id: Uuid::new_v4(),
            actor,
            action,
        })
        .await
    }
    async fn advertise(&mut self, actor: WorkerIdentity) -> DirectCapability {
        let reply = self
            .direct(
                actor,
                DirectCapabilityOperation::Advertise {
                    version: DIRECT_VERSION,
                },
            )
            .await;
        let ControlReply::DirectCapability(capability) = reply else {
            panic!("unexpected advert reply: {reply:?}");
        };
        assert!(capability.advertised);
        capability
    }
    async fn ready_cluster(&mut self, count: u128) -> Vec<WorkerIdentity> {
        self.initialize().await;
        let mut members = Vec::new();
        let mut workers = Vec::new();
        for number in 1..=count {
            let worker = self.enroll(number).await;
            self.add_ready(worker, &mut members).await;
            workers.push(worker);
        }
        workers
    }
    async fn begin_group(&mut self) -> Uuid {
        let generation = Uuid::new_v4();
        let group = GroupIdentity {
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: Uuid::new_v4(),
                shard_id: Uuid::new_v4(),
            },
            ..self.machine.store().identity().clone()
        };
        assert!(matches!(
            self.apply(ControlCommand::BeginGroup {
                operation_id: Uuid::new_v4(),
                generation,
                identity: group,
                authorization: authorization(),
            })
            .await,
            ControlReply::GroupBuilding { .. }
        ));
        generation
    }
}
fn replica(node_id: u64, worker: WorkerIdentity) -> ReplicaMember {
    ReplicaMember {
        node_id,
        worker_id: worker.worker_id,
        pool_id: Uuid::new_v4(),
    }
}

#[tokio::test]
async fn o02_t7_direct_capability_activates_on_unanimous_advertisement_and_is_durable() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers[..2] {
        assert_eq!(fixture.advertise(*worker).await.activation, None);
    }
    let activated = fixture.advertise(workers[2]).await;
    let activation = activated.activation.expect("the last advert activates");
    assert_eq!(activation.run_id, fixture.state().await.ready.view.run_id);
    // A repeated advert or activation attempt keeps the same activation.
    assert_eq!(
        fixture.advertise(workers[0]).await.activation,
        Some(activation)
    );
    // A member that is not enrolled cannot advertise.
    let stranger = WorkerIdentity::from(&worker(fixture.base + 99));
    assert_eq!(
        fixture
            .direct(
                stranger,
                DirectCapabilityOperation::Advertise {
                    version: DIRECT_VERSION
                }
            )
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    // An unknown version is not an advert.
    assert_eq!(
        fixture
            .direct(
                workers[0],
                DirectCapabilityOperation::Advertise { version: 2 }
            )
            .await,
        ControlReply::Rejected(Fault::Invalid)
    );
    // Durable: the reopened Control store keeps the activation.
    drop(fixture);
    let mut reopened = Fixture::open(&directory, scope, pool).await;
    let ControlReply::DirectCapability(after) = reopened
        .direct(workers[1], DirectCapabilityOperation::Activate)
        .await
    else {
        panic!("expected the capability");
    };
    assert_eq!(after.activation, Some(activation));
}

#[tokio::test]
async fn o02_t7_one_member_that_never_advertised_keeps_the_capability_inactive() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers[..2] {
        assert_eq!(fixture.advertise(*worker).await.activation, None);
    }
    // The old member (workers[2]) stays silent: nothing activates, however
    // often the others re-advertise or ask for re-evaluation.
    for _ in 0..3 {
        assert_eq!(fixture.advertise(workers[0]).await.activation, None);
        let ControlReply::DirectCapability(capability) = fixture
            .direct(workers[1], DirectCapabilityOperation::Activate)
            .await
        else {
            panic!("expected the capability");
        };
        assert_eq!(capability.activation, None);
    }
    let ControlReply::DirectCapability(old) = fixture
        .direct(workers[2], DirectCapabilityOperation::Activate)
        .await
    else {
        panic!("expected the capability");
    };
    assert!(!old.advertised && old.activation.is_none());
    // Before activation an old member may still join groups and the ready
    // set: nothing it cannot decode exists yet.
    let generation = fixture.begin_group().await;
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: Uuid::new_v4(),
                generation,
                replicas: Page::new(vec![replica(1, workers[2])]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));
    // Once the old member upgrades and advertises, Control activates.
    assert!(fixture.advertise(workers[2]).await.activation.is_some());
}

#[tokio::test]
async fn o02_t7_after_activation_a_member_without_the_advert_cannot_join_as_voter_or_learner() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers {
        fixture.advertise(*worker).await;
    }
    let ControlReply::DirectCapability(active) = fixture
        .direct(workers[0], DirectCapabilityOperation::Activate)
        .await
    else {
        panic!("expected the capability");
    };
    let activation = active.activation.expect("active");
    // An older build enrolls: it never advertises.
    let old = fixture.enroll(4).await;
    // The capability never deactivates.
    let ControlReply::DirectCapability(still) = fixture
        .direct(workers[1], DirectCapabilityOperation::Activate)
        .await
    else {
        panic!("expected the capability");
    };
    assert_eq!(still.activation, Some(activation));
    // Ready-set join (voter of the system groups): typed refusal.
    let state = fixture.state().await;
    assert_eq!(
        fixture
            .apply(ControlCommand::BeginJoin {
                operation_id: Uuid::new_v4(),
                expected: state.ready.view,
                target: old,
            })
            .await,
        ControlReply::Rejected(Fault::DirectCapabilityRequired)
    );
    assert!(fixture.state().await.pending.is_none(), "nothing began");
    // Replica of a new Data group (voter): typed refusal.
    let generation = fixture.begin_group().await;
    assert_eq!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: Uuid::new_v4(),
                generation,
                replicas: Page::new(vec![replica(1, workers[0]), replica(2, old)]).unwrap(),
            })
            .await,
        ControlReply::Rejected(Fault::DirectCapabilityRequired)
    );
    // A worker that re-enrolled with a new process (a downgrade, say) must
    // advertise again: its old advert named the previous incarnation.
    let previous = WorkerEnrollment {
        worker_id: workers[2].worker_id,
        incarnation: workers[2].incarnation,
        certificate_sha256: workers[2].certificate_sha256,
        endpoint: Text::try_from("127.0.0.1:1").unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
        enrollment_revision: 1,
    };
    assert_eq!(WorkerIdentity::from(&previous), workers[2]);
    let restarted = WorkerEnrollment {
        incarnation: Uuid::new_v4(),
        enrollment_revision: 2,
        ..previous
    };
    let reply = fixture
        .apply(ControlCommand::Enroll {
            operation_id: Uuid::new_v4(),
            expected: Some(workers[2]),
            next: restarted.clone(),
            authorization: authorization(),
            previous_process: Some(ExclusionStamp {
                worker: workers[2],
                authorization: authorization(),
                ownership_fence: hash(b"previous process"),
            }),
        })
        .await;
    if let ControlReply::Enrolled(restarted) = reply {
        let generation = fixture.begin_group().await;
        assert_eq!(
            fixture
                .apply(ControlCommand::GroupPage {
                    operation_id: Uuid::new_v4(),
                    generation,
                    replicas: Page::new(vec![replica(1, restarted)]).unwrap(),
                })
                .await,
            ControlReply::Rejected(Fault::DirectCapabilityRequired)
        );
        fixture.advertise(restarted).await;
    }
    // After advertising, the upgraded member joins normally.
    fixture.advertise(old).await;
    let generation = fixture.begin_group().await;
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: Uuid::new_v4(),
                generation,
                replicas: Page::new(vec![replica(1, workers[0]), replica(2, old)]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));
    let state = fixture.state().await;
    assert!(matches!(
        fixture
            .apply(ControlCommand::BeginJoin {
                operation_id: Uuid::new_v4(),
                expected: state.ready.view,
                target: old,
            })
            .await,
        ControlReply::JoinBegun { .. }
    ));
}
