//! VEC-G14 (task C1, adversarial): the cluster feature `data.vector.v1` on real
//! Control storage. The registry is append-only, so a Control replica of the
//! previous build must apply the new advert byte for byte alike, the feature
//! must wait for every enrolled worker, and once enabled a worker identity
//! that never advertised it cannot join as a ready-set member or a replica.
use super::*;
use crate::oltp_replication::authority::control::{
    DIRECT_BATCH_FEATURE, INDEX_RECLAIM_FEATURE, QUEUED_SNAPSHOT_RETRY_FEATURE, VECTOR_FEATURE,
};

impl Fixture {
    /// Every feature of the build before `data.vector.v1`.
    async fn advertise_cap1_build(&mut self, actor: WorkerIdentity) {
        self.advertise_gc2_build(actor).await;
        self.advertise_index_reclaim(actor).await;
    }
    async fn advertise_vector(&mut self, actor: WorkerIdentity) -> ControlReply {
        self.advertise_feature(VECTOR_FEATURE, actor).await
    }
    async fn join_refused_as_member_and_replica(
        &mut self,
        old: WorkerIdentity,
        first: WorkerIdentity,
    ) {
        let state = self.state().await;
        assert_eq!(
            self.apply(ControlCommand::BeginJoin {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: state.ready.view,
                target: old,
            })
            .await,
            ControlReply::Rejected(Fault::DirectCapabilityRequired)
        );
        let generation = self.begin_group().await;
        assert_eq!(
            self.apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![replica(1, first), replica(2, old)]).unwrap(),
            })
            .await,
            ControlReply::Rejected(Fault::DirectCapabilityRequired)
        );
    }
}

#[tokio::test]
async fn adv_vec_g14_vector_waits_for_every_worker_and_is_sticky_and_durable() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers {
        fixture.advertise_cap1_build(*worker).await;
    }
    assert!(
        fixture
            .feature_enabled(INDEX_RECLAIM_FEATURE)
            .await
            .is_some()
    );
    assert_eq!(
        fixture.feature_enabled(VECTOR_FEATURE).await,
        None,
        "nobody advertised it"
    );
    for worker in &workers[..2] {
        fixture.advertise_vector(*worker).await;
        assert_eq!(
            fixture.feature_enabled(VECTOR_FEATURE).await,
            None,
            "two of three"
        );
    }
    // A repeated advert is refused and enables nothing.
    assert_eq!(
        fixture.advertise_vector(workers[0]).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(fixture.feature_enabled(VECTOR_FEATURE).await, None);
    // An advert of another feature does not stand for this one.
    fixture
        .direct(workers[1], DirectCapabilityOperation::Activate)
        .await;
    assert_eq!(fixture.feature_enabled(VECTOR_FEATURE).await, None);
    fixture.advertise_vector(workers[2]).await;
    let enabled = fixture
        .feature_enabled(VECTOR_FEATURE)
        .await
        .expect("unanimous");
    // An unrelated worker that advertises the older features is refused.
    let old = fixture.enroll(4).await;
    fixture.advertise_cap1_build(old).await;
    assert_eq!(
        fixture.feature_enabled(VECTOR_FEATURE).await,
        Some(enabled),
        "sticky"
    );
    fixture
        .join_refused_as_member_and_replica(old, workers[0])
        .await;
    // After its advert it joins.
    fixture.advertise_vector(old).await;
    let generation = fixture.begin_group().await;
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![replica(1, workers[0]), replica(2, old)]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));
    drop(fixture);
    let reopened = Fixture::open(&directory, scope, pool).await;
    assert_eq!(
        reopened.feature_enabled(VECTOR_FEATURE).await,
        Some(enabled),
        "durable"
    );
}

/// A joiner that advertises the new feature but not an older enabled one is
/// refused just the same: every registered feature that is enabled is required.
#[tokio::test]
async fn adv_vec_g14_a_joiner_missing_any_one_enabled_feature_is_refused() {
    for missing in [
        DIRECT_BATCH_FEATURE,
        QUEUED_SNAPSHOT_RETRY_FEATURE,
        INDEX_RECLAIM_FEATURE,
        VECTOR_FEATURE,
    ] {
        let directory = Directory::new();
        let (scope, pool) = identities();
        let mut fixture = Fixture::open(&directory, scope, pool).await;
        let workers = fixture.ready_cluster(3).await;
        for worker in &workers {
            fixture.advertise_cap1_build(*worker).await;
            fixture.advertise_vector(*worker).await;
        }
        for feature in [
            DIRECT_BATCH_FEATURE,
            QUEUED_SNAPSHOT_RETRY_FEATURE,
            INDEX_RECLAIM_FEATURE,
            VECTOR_FEATURE,
        ] {
            assert!(
                fixture.feature_enabled(feature).await.is_some(),
                "{feature}"
            );
        }
        let joiner = fixture.enroll(4).await;
        fixture.advertise(joiner).await;
        for feature in [
            DIRECT_BATCH_FEATURE,
            QUEUED_SNAPSHOT_RETRY_FEATURE,
            INDEX_RECLAIM_FEATURE,
            VECTOR_FEATURE,
        ] {
            if feature != missing {
                fixture.advertise_feature(feature, joiner).await;
            }
        }
        fixture
            .join_refused_as_member_and_replica(joiner, workers[0])
            .await;
        fixture.advertise_feature(missing, joiner).await;
        let generation = fixture.begin_group().await;
        assert!(
            matches!(
                fixture
                    .apply(ControlCommand::GroupPage {
                        operation_id: nabedra_rt::ids::uuid_v4(),
                        generation,
                        replicas: Page::new(vec![replica(1, workers[0]), replica(2, joiner)])
                            .unwrap(),
                    })
                    .await,
                ControlReply::GroupBuilding { .. }
            ),
            "{missing}"
        );
    }
}

/// A restarted process of an enrolled worker is a new identity: its old advert
/// does not count, also for the newest feature.
#[tokio::test]
async fn adv_vec_g14_a_worker_that_restarted_after_enablement_must_advertise_again() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers {
        fixture.advertise_cap1_build(*worker).await;
        fixture.advertise_vector(*worker).await;
    }
    assert!(fixture.feature_enabled(VECTOR_FEATURE).await.is_some());
    let previous = WorkerEnrollment {
        worker_id: workers[2].worker_id,
        incarnation: workers[2].incarnation,
        certificate_sha256: workers[2].certificate_sha256,
        endpoint: Text::try_from("127.0.0.1:1").unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
        enrollment_revision: 1,
    };
    let restarted = WorkerEnrollment {
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 2,
        ..previous
    };
    let reply = fixture
        .apply(ControlCommand::Enroll {
            operation_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(workers[2]),
            next: restarted,
            authorization: authorization(),
            previous_process: Some(ExclusionStamp {
                worker: workers[2],
                authorization: authorization(),
                ownership_fence: hash(b"previous process"),
            }),
        })
        .await;
    let ControlReply::Enrolled(restarted) = reply else {
        panic!("{reply:?}");
    };
    // The restarted process runs the build it advertises: if it is a build
    // that does not know the feature (a downgrade), it cannot be a replica.
    let generation = fixture.begin_group().await;
    assert_eq!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![replica(1, restarted)]).unwrap(),
            })
            .await,
        ControlReply::Rejected(Fault::DirectCapabilityRequired)
    );
    fixture.advertise_cap1_build(restarted).await;
    // Having advertised everything but the newest feature is still refused.
    let generation = fixture.begin_group().await;
    assert_eq!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![replica(1, restarted)]).unwrap(),
            })
            .await,
        ControlReply::Rejected(Fault::DirectCapabilityRequired)
    );
    fixture.advertise_vector(restarted).await;
    let generation = fixture.begin_group().await;
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![replica(1, restarted)]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));
}

/// A Control replica of the build before `data.vector.v1` applies the
/// adverts and the enablement path of the new feature with the same replies and
/// state (the registry is append-only; the old replica treats the advert as an
/// ordinary `Advertise { version: 1 }`).
#[tokio::test]
async fn adv_vec_g14_vector_adverts_apply_identically_on_a_cap1_control_replica() {
    let directory = Directory::new();
    let legacy = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    fixture.mirror_legacy(&legacy, scope, pool).await;
    fixture.mirror = fixture
        .mirror
        .take()
        .map(|mirror| mirror.known_features_for_test(3));
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers {
        fixture.advertise_cap1_build(*worker).await;
    }
    for worker in &workers[..2] {
        assert!(matches!(
            fixture.advertise_vector(*worker).await,
            ControlReply::DirectCapability(DirectCapability {
                advertised: true,
                activation: Some(_),
                ..
            })
        ));
    }
    assert_eq!(
        fixture.advertise_vector(workers[0]).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    fixture
        .direct(workers[1], DirectCapabilityOperation::Activate)
        .await;
    assert_eq!(fixture.feature_enabled(VECTOR_FEATURE).await, None);
    // A CAP1-build worker joins while nothing new is enabled.
    let old = fixture.enroll(4).await;
    fixture.advertise_cap1_build(old).await;
    let mut members = workers.clone();
    members.sort_by_key(|worker| worker.worker_id);
    fixture.add_ready(old, &mut members).await;
    fixture.advertise_vector(workers[2]).await;
    fixture
        .direct(workers[0], DirectCapabilityOperation::Activate)
        .await;
    assert_eq!(
        fixture.feature_enabled(VECTOR_FEATURE).await,
        None,
        "the CAP1 worker has not upgraded"
    );
    fixture.mirror = None;
    fixture.advertise_vector(old).await;
    let enabled = fixture
        .feature_enabled(VECTOR_FEATURE)
        .await
        .expect("the last advert enables");
    assert_eq!(enabled.activated_at.index, fixture.next - 1);
}

/// Pin the feature advert name and its durable Control key across upgrades.
#[test]
fn adv_vec_g14_the_feature_name_and_its_storage_key_are_the_contracts() {
    use crate::oltp_replication::authority::control::CLUSTER_FEATURES;

    assert_eq!(VECTOR_FEATURE, "data.vector.v1");
    let vector = &CLUSTER_FEATURES[3];
    assert_eq!(vector.name, "data.vector.v1");
    assert_eq!(vector.enabled_key, b"ct/feature/data.vector.v1");
}
