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
            unix_ms: crate::oltp_replication::authority::control::direct::wall_ms(),
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

impl Fixture {
    async fn queued_history(&mut self, owner: WorkerIdentity) -> DirectCapability {
        let reply = self
            .direct(owner, DirectCapabilityOperation::QueuedHistory)
            .await;
        let ControlReply::DirectCapability(capability) = reply else {
            panic!("unexpected queued-history reply: {reply:?}");
        };
        assert!(capability.advertised, "the note is the owner's advert");
        capability
    }
}

/// T7c, O15: a queue owner's `QueuedHistory` note before an unsettled
/// eligible reservation. While Control cannot activate, the note is durable
/// and the later activation carries `queued_history` and its wall clock,
/// which opens the window. When Control can activate, the note activates
/// without history (the owner then settles the request first); after
/// activation a note changes nothing.
#[tokio::test]
async fn o15_t7c_queued_history_note_opens_the_window_only_before_activation() {
    // Recorded while inactive, carried by the activation, durable.
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let workers = fixture.ready_cluster(2).await;
    let before = crate::oltp_replication::authority::control::direct::wall_ms();
    assert_eq!(fixture.queued_history(workers[0]).await.activation, None);
    // Repeated notes are harmless.
    assert_eq!(fixture.queued_history(workers[0]).await.activation, None);
    let activation = fixture
        .advertise(workers[1])
        .await
        .activation
        .expect("the last advert activates");
    assert!(activation.queued_history, "{activation:?}");
    assert!(activation.unix_ms >= before, "{activation:?}");
    assert_eq!(
        activation.history_until(1_000),
        Some(activation.unix_ms + 1_000)
    );
    drop(fixture);
    let mut reopened = Fixture::open(&directory, scope, pool).await;
    // After activation a note answers active and changes nothing.
    assert_eq!(
        reopened.queued_history(workers[1]).await.activation,
        Some(activation)
    );

    // A note that can activate does so, without history.
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let workers = fixture.ready_cluster(2).await;
    assert_eq!(fixture.advertise(workers[1]).await.activation, None);
    let activation = fixture
        .queued_history(workers[0])
        .await
        .activation
        .expect("the owner's note activates");
    assert!(!activation.queued_history, "{activation:?}");
    assert_eq!(activation.history_until(1_000), None);
}

// ---------------------------------------------------------------------------
// GC2: the `data.direct.batch` cluster feature
// ---------------------------------------------------------------------------

impl Fixture {
    /// `actor`'s `data.direct.batch` advert, exactly as a feature-aware
    /// worker sends it.
    async fn advertise_batch(&mut self, actor: WorkerIdentity) -> ControlReply {
        self.apply(ControlCommand::DirectCapability {
            operation_id: crate::oltp_replication::authority::control::feature_operation_id(
                crate::oltp_replication::authority::control::DIRECT_BATCH_FEATURE,
                actor,
            ),
            actor,
            action: DirectCapabilityOperation::Advertise {
                version: DIRECT_VERSION,
            },
            unix_ms: crate::oltp_replication::authority::control::direct::wall_ms(),
        })
        .await
    }
    async fn batch_enabled(
        &self,
    ) -> Option<crate::oltp_replication::authority::control::FeatureActivation> {
        let raw = self
            .machine
            .store()
            .read(BUDGET, |view| {
                let enabled: Option<
                    crate::oltp_replication::authority::control::FeatureActivation,
                > = view
                    .get(super::super::machine::direct::batch_key())?
                    .map(|raw| decode(raw.as_ref()))
                    .transpose()?;
                view.encode(&enabled)
            })
            .await
            .unwrap();
        decode(raw.as_ref()).unwrap()
    }
}

/// The mixed-version proof for the feature's advert channel: a Control
/// replica of the build before cluster features applies every entry a
/// feature-aware cluster produces until the enablement (adverts, repeated
/// adverts, re-evaluations, an old member joining) with the same reply and
/// byte-identical state, so no Control replica diverges. The enablement
/// needs every enrolled worker's advert, that is a cluster in which every
/// Control voter (an enrolled worker) runs a feature-aware build.
#[tokio::test]
async fn o02_gc2_feature_adverts_apply_identically_on_a_pre_feature_control_replica() {
    let directory = Directory::new();
    let legacy = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    fixture.mirror_legacy(&legacy, scope, pool).await;
    let workers = fixture.ready_cluster(3).await;
    for worker in &workers {
        fixture.advertise(*worker).await;
    }
    // Two members run the new build: their feature adverts are ordinary
    // `Advertise { version: 1 }` entries to the old replica.
    for worker in &workers[..2] {
        assert!(matches!(
            fixture.advertise_batch(*worker).await,
            ControlReply::DirectCapability(DirectCapability {
                advertised: true,
                activation: Some(_),
                ..
            })
        ));
    }
    // A repeated advert hits its receipt (another wall clock: a conflict)
    // and re-evaluation is a no-op, on both builds alike.
    assert_eq!(
        fixture.advertise_batch(workers[0]).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    fixture
        .direct(workers[1], DirectCapabilityOperation::Activate)
        .await;
    assert_eq!(fixture.batch_enabled().await, None, "workers[2] is old");
    // A worker of the old build enrolls, advertises `data.direct.v1` and
    // joins the ready set and a Data group: admitted on both builds.
    let old = fixture.enroll(4).await;
    fixture.advertise(old).await;
    let mut members = workers.clone();
    members.sort_by_key(|worker| worker.worker_id);
    fixture.add_ready(old, &mut members).await;
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
    // workers[2] upgrades: still not every enrolled worker (`old`).
    fixture.advertise_batch(workers[2]).await;
    fixture
        .direct(workers[0], DirectCapabilityOperation::Activate)
        .await;
    assert_eq!(fixture.batch_enabled().await, None);
    // The last member upgrades. From here every enrolled worker, so every
    // Control voter and learner, runs a feature-aware build: no pre-feature
    // replica is left to mirror.
    fixture.mirror = None;
    assert!(matches!(
        fixture.advertise_batch(old).await,
        ControlReply::DirectCapability(DirectCapability {
            advertised: true,
            ..
        })
    ));
    let enabled = fixture
        .batch_enabled()
        .await
        .expect("the last advert enables");
    assert_eq!(enabled.activated_at.index, fixture.next - 1);
}

/// Enablement is unanimous over enrolled workers, sticky and durable, and
/// afterwards a joiner whose current identity did not advertise the feature
/// is refused as a ready-set member and as a Data replica.
#[tokio::test]
async fn o02_gc2_batch_feature_is_sticky_and_refuses_joiners_without_the_advert() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let workers = fixture.ready_cluster(3).await;
    // A feature advert is also a `data.direct.v1` advert. Before activation
    // it is recorded but cannot enable the feature.
    for worker in &workers[..2] {
        fixture.advertise_batch(*worker).await;
    }
    assert_eq!(fixture.batch_enabled().await, None);
    // The last member advertises only `data.direct.v1`: that activates, but
    // the feature waits for its advert.
    assert!(fixture.advertise(workers[2]).await.activation.is_some());
    assert_eq!(fixture.batch_enabled().await, None);
    assert!(matches!(
        fixture.advertise_batch(workers[2]).await,
        ControlReply::DirectCapability(DirectCapability {
            activation: Some(_),
            ..
        })
    ));
    let enabled = fixture.batch_enabled().await.expect("unanimous");
    // A new worker of the old build: it advertises only `data.direct.v1`.
    let old = fixture.enroll(4).await;
    fixture.advertise(old).await;
    assert_eq!(fixture.batch_enabled().await, Some(enabled), "sticky");
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
    // A member that re-enrolls with a new process (a downgrade, say) must
    // advertise again: the advert's operation id named its old identity.
    let restarted = WorkerEnrollment {
        worker_id: workers[2].worker_id,
        incarnation: Uuid::new_v4(),
        certificate_sha256: workers[2].certificate_sha256,
        endpoint: Text::try_from("127.0.0.1:1").unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
        enrollment_revision: 2,
    };
    let reply = fixture
        .apply(ControlCommand::Enroll {
            operation_id: Uuid::new_v4(),
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
    if let ControlReply::Enrolled(restarted) = reply {
        fixture.advertise(restarted).await;
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
        fixture.advertise_batch(restarted).await;
        assert!(matches!(
            fixture
                .apply(ControlCommand::GroupPage {
                    operation_id: Uuid::new_v4(),
                    generation,
                    replicas: Page::new(vec![replica(1, restarted)]).unwrap(),
                })
                .await,
            ControlReply::GroupBuilding { .. }
        ));
    }
    // Once it upgrades and advertises, it joins.
    fixture.advertise_batch(old).await;
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
    // Durable: a reopened Control store keeps the enablement.
    drop(fixture);
    let reopened = Fixture::open(&directory, scope, pool).await;
    assert_eq!(reopened.batch_enabled().await, Some(enabled));
}

#[tokio::test]
async fn o02_o08_snapshot_retry_waits_for_every_worker_and_refuses_old_joiners() {
    use crate::oltp_replication::authority::control::{
        QUEUED_SNAPSHOT_RETRY_FEATURE, feature_operation_id,
        machine::direct::SNAPSHOT_RETRY_ENABLED,
    };

    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let workers = fixture.ready_cluster(3).await;
    let advert = |actor| ControlCommand::DirectCapability {
        operation_id: feature_operation_id(QUEUED_SNAPSHOT_RETRY_FEATURE, actor),
        actor,
        action: DirectCapabilityOperation::Advertise {
            version: DIRECT_VERSION,
        },
        unix_ms: 100,
    };
    for worker in &workers[..2] {
        fixture.apply(advert(*worker)).await;
    }
    // The third worker supports DirectBatch and so can run the previous
    // build. Its presence prevents queued retry activation.
    fixture.advertise_batch(workers[2]).await;
    let enabled = |machine: ControlMachine| async move {
        let raw = machine
            .store()
            .read(BUDGET, |view| {
                let active = view.get(SNAPSHOT_RETRY_ENABLED)?.is_some();
                view.encode(&active)
            })
            .await
            .unwrap();
        decode::<bool>(raw.as_ref()).unwrap()
    };
    assert!(!enabled(fixture.machine.clone()).await);
    fixture.apply(advert(workers[2])).await;
    assert!(enabled(fixture.machine.clone()).await);

    let old = fixture.enroll(4).await;
    fixture.advertise_batch(old).await;
    let ready = fixture.state().await.ready.view;
    let join = || ControlCommand::BeginJoin {
        operation_id: Uuid::new_v4(),
        expected: ready,
        target: old,
    };
    assert_eq!(
        fixture.apply(join()).await,
        ControlReply::Rejected(Fault::DirectCapabilityRequired)
    );
    fixture.apply(advert(old)).await;
    assert!(matches!(
        fixture.apply(join()).await,
        ControlReply::JoinBegun { .. }
    ));
}
