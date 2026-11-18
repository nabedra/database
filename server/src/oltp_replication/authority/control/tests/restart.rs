use super::*;
use crate::oltp_replication::authority::control::{RestartIntent, RestartMountKey};

#[tokio::test]
async fn r03_prepared_restart_requires_retained_mount_and_preserves_control_voter() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    let initial = fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let previous = fixture.enroll(2).await;
    let mut ready_members = Vec::new();
    let ready = fixture.add_ready(owner, &mut ready_members).await;
    let permanent = ReplicaMember {
        node_id: 7,
        worker_id: previous.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let group = fixture
        .register_group(GroupKind::Control, vec![permanent.clone()])
        .await;
    let new_enrollment = WorkerEnrollment {
        enrollment_revision: 2,
        certificate_sha256: hash(b"replacement cert"),
        ..worker(previous.worker_id.as_u128())
    };
    let next = WorkerIdentity::from(&new_enrollment);
    let intent = RestartIntent {
        id: Uuid::new_v4(),
        run_id: initial.view.run_id,
        previous,
        next: new_enrollment,
        pool_id: permanent.pool_id,
        mount_generation: 2,
    };
    let prepare = ControlCommand::PrepareRestart {
        operation_id: intent.id,
        authorization: authorization(),
        intent: intent.clone(),
    };
    assert_eq!(
        fixture.apply(prepare.clone()).await,
        ControlReply::Rejected(Fault::NotFound)
    );
    let key = RestartMountKey::from_seed(&[4; 32]).unwrap();
    let register = ControlCommand::RegisterRestartMount {
        operation_id: Uuid::new_v4(),
        worker: previous,
        pool_id: permanent.pool_id,
        generation: 1,
        public_key: key.public_key(),
        authorization: AuthorizationStamp {
            subject: previous.certificate_sha256,
            ..authorization()
        },
    };
    assert_eq!(
        fixture.apply(register).await,
        ControlReply::RestartMountRegistered(previous)
    );
    assert_eq!(
        fixture.apply(prepare.clone()).await,
        ControlReply::RestartPrepared(next)
    );
    assert_eq!(
        fixture.apply(prepare).await,
        ControlReply::RestartPrepared(next)
    );
    assert!(!fixture.state().await.restart.unwrap().enrolled);
    let operation = Uuid::new_v4();
    let exclude = ExclusionStamp {
        worker: previous,
        authorization: AuthorizationStamp {
            authorization_id: operation,
            subject: next.certificate_sha256,
            ..authorization()
        },
        ownership_fence: hash(b"mounted lock"),
    };
    let complete = |proof| ControlCommand::CompleteRestart {
        operation_id: operation,
        intent: intent.clone(),
        proof,
        exclusion: exclude.clone(),
    };
    let mut other_generation = intent.clone();
    other_generation.mount_generation = 3;
    assert_eq!(
        fixture
            .apply(complete(key.sign(&scope, &other_generation).unwrap()))
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    assert_eq!(
        fixture
            .apply(complete(
                RestartMountKey::from_seed(&[5; 32])
                    .unwrap()
                    .sign(&scope, &intent)
                    .unwrap()
            ))
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    assert!(!fixture.state().await.restart.unwrap().enrolled);
    let complete = complete(key.sign(&scope, &intent).unwrap());
    assert_eq!(
        fixture.apply(complete.clone()).await,
        ControlReply::Enrolled(next)
    );
    assert_eq!(fixture.apply(complete).await, ControlReply::Enrolled(next));
    let state = fixture.state().await;
    assert_eq!(state.ready, ready);
    assert_eq!(state.owner, Some(owner));
    assert!(state.restart.unwrap().enrolled);
    fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let raw = view
                .get(&member_key(group.generation, permanent.node_id))?
                .unwrap();
            assert_eq!(decode::<ReplicaMember>(raw.as_ref())?, permanent);
            view.encode(&())
        })
        .await
        .unwrap();
}
