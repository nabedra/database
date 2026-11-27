use nabedra_resources::config::WorkerResources;
use super::*;
use crate::{
    oltp::pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
    oltp_replication::{
        ReadySetHasher,
        authority::{
            proof::{AuthenticatedAck, TerminalVerifier},
            status::{MutationOutcome, RejectedIdentity, RequestKind},
        },
        owned::serialized_digest,
    },
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub(super) struct Dir(pub(super) std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub(super) struct Fixture {
    pub(super) machine: TimestampMachine,
    pub(super) pool: OltpPool,
    pub(super) dir: Dir,
    pub(super) next: u64,
}
impl Fixture {
    pub(super) async fn new() -> Self {
        let dir = Dir(std::env::temp_dir().join(format!("oltp-timestamps-{}", nabedra_rt::ids::uuid_v4())));
        let shared = WorkerSharedResources::new(
            9,
            WorkerResources {
                max_threads: 4,
                ..Default::default()
            },
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(
            &dir.0,
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: nabedra_rt::ids::uuid_v4(),
                worker_id: nabedra_rt::ids::uuid_v4(),
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            shared,
        )
        .unwrap();
        let group = pool
            .group(GroupIdentity {
                cluster_id: pool.identity().cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Timestamps,
                group_format_version: GROUP_FORMAT_VERSION,
            })
            .await
            .unwrap();
        Self {
            machine: TimestampMachine::new(group).unwrap(),
            pool,
            dir,
            next: 0,
        }
    }
    pub(super) async fn apply(&mut self, command: TimestampCommand) -> TimestampReply {
        let resources = self.pool.resources();
        let command = OwnedCommand::new_owned(command, resources, ByteClass::Progress).unwrap();
        let encoded = resources
            .encode_in(ByteClass::Progress, &command, 16 << 10)
            .unwrap();
        let digest = Digest::from_bytes(Sha256::digest(encoded.as_slice()).into());
        drop(encoded);
        let meta = ApplyMeta {
            position: AppliedPosition {
                term: 1,
                node_id: 1,
                index: self.next,
            },
            command_digest: digest,
            membership: None,
        };
        self.next += 1;
        let result = self.machine.apply(meta, command).await.unwrap();
        serde_json::from_slice(result.as_slice()).unwrap()
    }
    pub(super) async fn initialize(&mut self, ready: ReadyMembership) {
        assert!(matches!(
            self.apply(TimestampCommand::Fence {
                transition_id: nabedra_rt::ids::uuid_v4(),
                expected: None,
                next: ready.view.authority(),
                ready
            })
            .await,
            TimestampReply::Fenced {
                allocator_epoch: 1,
                ..
            }
        ));
    }
}
pub(super) fn worker() -> WorkerIdentity {
    WorkerIdentity {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([3; 32]),
        enrollment_revision: 1,
    }
}
pub(super) fn ready(member: WorkerIdentity) -> ReadyMembership {
    let mut hash = ReadySetHasher::default();
    hash.push(member).unwrap();
    ReadyMembership {
        view: AuthorityView {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 1,
            membership_revision: 1,
        },
        ready_root: hash.finish(),
        ready_count: 1,
    }
}
fn binding(view: AuthorityView) -> (PinIdentity, RequestBinding) {
    let request = RequestBinding {
        run_id: view.run_id,
        request_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([4; 32]),
        accepted_context: Digest::from_bytes([5; 32]),
    };
    (
        PinIdentity {
            run_id: view.run_id,
            request_id: request.request_id,
            digest: request.digest,
            nonce: nabedra_rt::ids::uuid_v4(),
        },
        request,
    )
}
fn begin(
    ready: ReadyMembership,
    identity: &PinIdentity,
    request: RequestBinding,
) -> TimestampCommand {
    TimestampCommand::BeginSnapshot {
        view: ready.view,
        identity: identity.clone(),
        reservation: super::super::status::IdentityReservation {
            request_id: request.request_id,
            digest: request.digest,
            original_run: request.run_id,
            reservation_id: identity.nonce,
            kind: super::super::status::RequestKind::Read,
            scoped: None,
        },
        pin_id: nabedra_rt::ids::uuid_v4(),
    }
}
fn bind_context(
    ready: ReadyMembership,
    identity: &PinIdentity,
    pin: StoragePin,
    request: RequestBinding,
) -> TimestampCommand {
    TimestampCommand::BindSnapshotContext {
        view: ready.view,
        identity: identity.clone(),
        pin,
        request,
    }
}

fn proof(
    member: WorkerIdentity,
    ready: ReadyMembership,
    identity: PinIdentity,
    request: RequestBinding,
    pin: StoragePin,
) -> SealedTerminalProof {
    let statement = TerminalCopyStatement {
        ready,
        request,
        terminal_version: 3,
        response_hash: Digest::from_bytes([6; 32]),
        terminal_digest: Some(Digest::from_bytes([7; 32])),
        pin_identity: identity,
        storage_pin: pin,
    };
    let mut verifier = TerminalVerifier::begin(statement.clone(), ready).unwrap();
    let ack = AuthenticatedAck::from_transport(
        member,
        statement_digest(&statement).unwrap(),
        nabedra_rt::ids::uuid_v4(),
    );
    verifier.accept(member, &ack).unwrap();
    verifier.finish(ready).unwrap().into_sealed()
}

/// O09/O15/O19/Q06/R04. The cold release must use the exact prior pin and
/// matching group fence. The retention floor still includes independent pins.
#[tokio::test]
async fn o09_o15_o19_r04_cold_release_advances_floor_past_abandoned_application_pin() {
    use crate::oltp::backup::BackupOwnerRef;
    let mut f = Fixture::new().await;
    let old = ready(worker());
    f.initialize(old).await;
    let authority = old.view.authority();
    let (identity, request) = binding(old.view);
    let command = begin(old, &identity, request);
    let TimestampCommand::BeginSnapshot { reservation, .. } = command else {
        unreachable!()
    };
    let TimestampReply::Snapshot(abandoned) = f.apply(command).await else {
        panic!("old application timestamp");
    };
    assert_eq!(
        f.apply(bind_context(old, &identity, abandoned, request))
            .await,
        TimestampReply::ContextBound(abandoned)
    );
    let old_pin = cold::ColdApplicationPin {
        identity,
        reservation,
        request: Some(request),
        pin: abandoned,
    };

    let owner = MaintenanceOwnerRef {
        catalog_group_id: nabedra_rt::ids::uuid_v4(),
        table_id: nabedra_rt::ids::uuid_v4(),
        index_id: nabedra_rt::ids::uuid_v4(),
        build_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let (index_identity, _) = binding(old.view);
    let index_reservation = IdentityReservation {
        reservation_id: index_identity.nonce,
        request_id: index_identity.request_id,
        original_run: old.view.run_id,
        digest: index_identity.digest,
        kind: RequestKind::Mutation,
        scoped: None,
    };
    let TimestampReply::Snapshot(index) = f
        .apply(TimestampCommand::BeginIndexMetadata {
            authority,
            owner,
            txn: TxnId {
                request_id: index_identity.request_id,
                attempt: 1,
            },
            reservation: index_reservation,
            pin_id: nabedra_rt::ids::uuid_v4(),
        })
        .await
    else {
        panic!("index metadata timestamp");
    };
    let TimestampReply::Snapshot(maintenance) = f
        .apply(TimestampCommand::BeginMaintenance {
            authority,
            owner,
            pin_id: nabedra_rt::ids::uuid_v4(),
        })
        .await
    else {
        panic!("maintenance timestamp");
    };
    let TimestampReply::Snapshot(backup) = f
        .apply(TimestampCommand::BeginBackup {
            authority,
            owner: BackupOwnerRef {
                backup_id: nabedra_rt::ids::uuid_v4(),
                attempt: 1,
            },
            pin_id: nabedra_rt::ids::uuid_v4(),
        })
        .await
    else {
        panic!("backup timestamp");
    };

    let transition_id = nabedra_rt::ids::uuid_v4();
    let fenced = ReadyMembership {
        view: AuthorityView {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 2,
            membership_revision: 2,
        },
        ready_root: ReadySetHasher::default().finish(),
        ready_count: 0,
    };
    let next = fenced.view.authority();
    assert!(matches!(
        f.apply(TimestampCommand::Fence {
            transition_id,
            expected: Some(authority),
            next,
            ready: fenced,
        })
        .await,
        TimestampReply::Fenced { .. }
    ));
    let live = ReadyMembership {
        view: AuthorityView {
            membership_revision: 3,
            ..fenced.view
        },
        ..old
    };
    assert!(matches!(
        f.apply(TimestampCommand::FenceReady {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: fenced,
            next: live,
        })
        .await,
        TimestampReply::ReadyFenced(_)
    ));
    let (live_identity, live_request) = binding(live.view);
    let live_command = begin(live, &live_identity, live_request);
    let TimestampCommand::BeginSnapshot {
        reservation: live_reservation,
        ..
    } = live_command
    else {
        unreachable!()
    };
    let TimestampReply::Snapshot(live_pin) = f.apply(live_command).await else {
        panic!("new live timestamp");
    };

    let floor = || TimestampCommand::AdvanceFloor { authority: next };
    assert!(matches!(f.apply(floor()).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == abandoned.read_ts.get()));
    let release = |transition_id, pin| TimestampCommand::ReleaseColdApplication {
        authority: next,
        transition_id,
        pin,
    };
    for (fence, pin) in [
        (nabedra_rt::ids::uuid_v4(), old_pin.clone()),
        (
            transition_id,
            cold::ColdApplicationPin {
                request: None,
                ..old_pin.clone()
            },
        ),
        (
            transition_id,
            cold::ColdApplicationPin {
                identity: index_identity,
                reservation: index_reservation,
                request: None,
                pin: index,
            },
        ),
        (
            transition_id,
            cold::ColdApplicationPin {
                identity: live_identity.clone(),
                reservation: live_reservation,
                request: None,
                pin: live_pin,
            },
        ),
    ] {
        assert_eq!(
            f.apply(release(fence, pin)).await,
            TimestampReply::Rejected(Fault::InvalidBinding)
        );
    }
    for _ in 0..2 {
        assert_eq!(
            f.apply(release(transition_id, old_pin.clone())).await,
            TimestampReply::Released
        );
    }
    assert!(matches!(f.apply(floor()).await,
        TimestampReply::Retention { safe_ts, .. }
            if safe_ts > abandoned.read_ts.get() && safe_ts == index.read_ts.get()));
    f.machine
        .store()
        .read_metadata(BUDGET, move |view| {
            assert!(view.get(&active_key(abandoned))?.is_none());
            assert!(view.get(&active_key(index))?.is_some());
            assert!(view.get(&maintenance_active_key(maintenance))?.is_some());
            assert!(view.get(&backup_active_key(backup))?.is_some());
            assert!(view.get(&active_key(live_pin))?.is_some());
            view.encode(&())
        })
        .await
        .unwrap();
}

/// O16/O19, R04. A base cut between the child's image and publication
/// cannot release the image pin. The next published archive covers it.
#[tokio::test]
async fn o16_o19_split_retention_tracks_the_published_archive_floor() {
    use crate::oltp::backup::{
        BackupOwnerRef, BackupPublishedStatement, SealedBackupPublished, VerifiedBackupPublished,
    };
    let mut f = Fixture::new().await;
    let member = worker();
    let ready = ready(member);
    f.initialize(ready).await;
    let authority = ready.view.authority();
    let (identity, request) = binding(ready.view);
    let TimestampReply::Snapshot(copy) = f.apply(begin(ready, &identity, request)).await else {
        panic!("copy timestamp");
    };
    assert_eq!(
        f.apply(bind_context(ready, &identity, copy, request)).await,
        TimestampReply::ContextBound(copy)
    );
    assert_eq!(
        f.apply(TimestampCommand::Release {
            proof: proof(member, ready, identity, request, copy),
        })
        .await,
        TimestampReply::Released
    );

    let owner = BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let begin_backup = |owner| TimestampCommand::BeginBackup {
        authority,
        owner,
        pin_id: nabedra_rt::ids::uuid_v4(),
    };
    let TimestampReply::Snapshot(base) = f.apply(begin_backup(owner)).await else {
        panic!("base timestamp");
    };
    let retained = split::retained_key(copy);
    let published = base.read_ts.get() + 1;
    let meta = ApplyMeta {
        position: AppliedPosition {
            term: 1,
            node_id: 1,
            index: f.next,
        },
        command_digest: Digest::from_bytes([1; 32]),
        membership: None,
    };
    f.next += 1;
    f.machine
        .store()
        .transaction(meta, BUDGET, move |tx| {
            tx.put(&retained, &published.to_be_bytes())?;
            commit(tx, &TimestampReply::Released)
        })
        .await
        .unwrap();

    let root = Digest::from_bytes([2; 32]);
    let archive_proof = |owner, pin, root| {
        SealedBackupPublished::from_verified(
            VerifiedBackupPublished::published(BackupPublishedStatement {
                owner,
                pin,
                archive_id: nabedra_rt::ids::uuid_v4(),
                manifest_sha256: root,
                catalog_root: root,
            })
            .unwrap(),
        )
        .unwrap()
    };
    let base_proof = archive_proof(owner, base, root);
    let retain = |proof, previous_root| TimestampCommand::ArchiveControl {
        command: archive_lifecycle::ArchiveControl::Retain {
            authority,
            proof,
            previous_root,
        },
    };
    let advance = TimestampCommand::AdvanceFloor { authority };
    assert_eq!(
        f.apply(retain(base_proof, None)).await,
        TimestampReply::Released
    );
    assert_eq!(
        f.apply(TimestampCommand::ReleaseBackup {
            authority,
            proof: base_proof
        })
        .await,
        TimestampReply::Released
    );
    assert!(matches!(
        f.apply(advance.clone()).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == copy.read_ts.get()
    ));

    let next = BackupOwnerRef {
        attempt: 2,
        ..owner
    };
    let TimestampReply::Snapshot(covered) = f.apply(begin_backup(next)).await else {
        panic!("covering archive timestamp");
    };
    assert_eq!(covered.read_ts.get(), published);
    assert!(matches!(
        f.apply(advance.clone()).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == copy.read_ts.get()
    ));
    let covering_proof = archive_proof(next, covered, Digest::from_bytes([3; 32]));
    assert_eq!(
        f.apply(retain(covering_proof, Some(root))).await,
        TimestampReply::Released
    );
    let retained_removed = f
        .machine
        .store()
        .read_metadata(BUDGET, move |view| {
            let removed = view.pinned(&retained)?.is_none();
            view.encode(&removed)
        })
        .await
        .unwrap();
    assert_eq!(retained_removed.as_slice(), b"true");
    assert!(matches!(
        f.apply(advance).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == covered.read_ts.get()
    ));
}

/// O09/O11: timestamp and retention pin are one atomic transition. Lost replies
/// reuse the nonce; an acknowledged release leaves a permanent nonce tombstone.
#[tokio::test]
async fn o09_o11_snapshot_nonce_is_atomic_idempotent_and_never_reopens_after_release() {
    let mut f = Fixture::new().await;
    let member = worker();
    let ready = ready(member);
    f.initialize(ready).await;
    let (identity, request) = binding(ready.view);
    let TimestampReply::Snapshot(pin) = f.apply(begin(ready, &identity, request)).await else {
        panic!("pin")
    };
    assert_eq!(pin.read_ts.get(), 1);
    assert_eq!(
        f.apply(bind_context(ready, &identity, pin, request)).await,
        TimestampReply::ContextBound(pin)
    );
    assert_eq!(
        f.apply(begin(ready, &identity, request)).await,
        TimestampReply::Snapshot(pin)
    );
    let mut conflicting = request;
    conflicting.accepted_context = Digest::from_bytes([8; 32]);
    assert_eq!(
        f.apply(bind_context(ready, &identity, pin, conflicting))
            .await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    let (other, other_request) = binding(ready.view);
    let TimestampReply::Snapshot(second) = f.apply(begin(ready, &other, other_request)).await
    else {
        panic!("pin")
    };
    assert_eq!(second.read_ts.get(), 2);
    let floor = f
        .apply(TimestampCommand::AdvanceFloor {
            authority: ready.view.authority(),
        })
        .await;
    assert!(matches!(
        floor,
        TimestampReply::Retention { safe_ts: 1, .. }
    ));
    assert_eq!(
        f.apply(TimestampCommand::Release {
            proof: proof(member, ready, identity.clone(), request, pin)
        })
        .await,
        TimestampReply::Released
    );
    assert_eq!(
        f.apply(begin(ready, &identity, request)).await,
        TimestampReply::Rejected(Fault::PinExpired)
    );
    assert!(matches!(
        f.apply(TimestampCommand::AdvanceFloor {
            authority: ready.view.authority()
        })
        .await,
        TimestampReply::Retention { safe_ts: 2, .. }
    ));
}

fn unaccepted_rejection(begin: &TimestampCommand) -> RejectedIdentity {
    let TimestampCommand::BeginSnapshot {
        view,
        identity,
        reservation,
        ..
    } = begin
    else {
        panic!("expected an application snapshot command");
    };
    RejectedIdentity {
        reservation: *reservation,
        view: *view,
        identity: identity.clone(),
        pin: None,
        request: None,
        failure: MutationOutcome::failure(ProtocolError::new(
            ErrorCode::Overloaded,
            "timestamp request did not reach the receiver",
            Outcome::NotApplied,
        ))
        .unwrap(),
        recorded_at: AppliedPosition {
            term: 1,
            node_id: 1,
            index: 10,
        },
    }
}

fn cancel_command(fixture: &Fixture, rejection: &RejectedIdentity) -> TimestampCommand {
    let status_group = GroupIdentity {
        group_id: Uuid::from_u128(1),
        kind: GroupKind::Status { bucket: 0 },
        ..fixture.machine.store().identity().clone()
    };
    // Model the sealed statement the authenticated Status readback supplies.
    let digest = serialized_digest(&(&status_group, rejection)).unwrap();
    let proof = serde_json::from_value(serde_json::json!({
        "status_group": status_group,
        "rejection": rejection,
        "digest": digest,
    }))
    .unwrap();
    TimestampCommand::CancelUnaccepted { proof }
}

/// Q02/Q06/O09/O13/R04. Begin applied, but the owner never observed the pin
/// and a later attempt proved no queue copies began. Rejection closes the pin.
#[tokio::test]
async fn q02_q06_o09_o13_r04_unaccepted_rejection_closes_unbound_lost_snapshot() {
    let mut f = Fixture::new().await;
    let original = ready(worker());
    f.initialize(original).await;
    let (identity, request) = binding(original.view);
    let delayed = begin(original, &identity, request);
    let TimestampReply::Snapshot(pin) = f.apply(delayed.clone()).await else {
        panic!("lost snapshot allocation");
    };
    let (other, other_request) = binding(original.view);
    let other_begin = begin(original, &other, other_request);
    let TimestampReply::Snapshot(other_pin) = f.apply(other_begin.clone()).await else {
        panic!("unrelated snapshot allocation");
    };
    let floor = TimestampCommand::AdvanceFloor {
        authority: original.view.authority(),
    };
    assert!(matches!(f.apply(floor.clone()).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == pin.read_ts.get()));

    let rejection = unaccepted_rejection(&delayed);
    let cancel = cancel_command(&f, &rejection);
    assert_eq!(f.apply(cancel.clone()).await, TimestampReply::Released);
    assert_eq!(f.apply(cancel.clone()).await, TimestampReply::Released);
    let mut changed = rejection;
    changed.recorded_at.index += 1;
    let changed = cancel_command(&f, &changed);
    assert_eq!(
        f.apply(changed).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(
        f.apply(delayed.clone()).await,
        TimestampReply::Rejected(Fault::PinExpired)
    );
    assert_eq!(
        f.apply(bind_context(original, &identity, pin, request))
            .await,
        TimestampReply::Rejected(Fault::PinExpired)
    );
    let mut relabeled = delayed;
    if let TimestampCommand::BeginSnapshot { identity, .. } = &mut relabeled {
        identity.nonce = nabedra_rt::ids::uuid_v4();
    }
    assert_eq!(
        f.apply(relabeled).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(
        f.apply(other_begin.clone()).await,
        TimestampReply::Snapshot(other_pin)
    );
    assert!(matches!(f.apply(floor).await,
        TimestampReply::Retention { safe_ts, .. } if safe_ts == other_pin.read_ts.get()));
    let retained = f
        .machine
        .store()
        .read_metadata(BUDGET, move |view| {
            let retained = view
                .get(&active_key(other_pin))?
                .is_some_and(|nonce| nonce.as_slice() == other.nonce.as_bytes());
            view.encode(&retained)
        })
        .await
        .unwrap();
    assert_eq!(retained.as_slice(), b"true");

    let cold = ReadyMembership {
        view: AuthorityView {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: original.view.owner_term + 1,
            membership_revision: original.view.membership_revision + 1,
        },
        ready_root: ReadySetHasher::default().finish(),
        ready_count: 0,
    };
    assert!(matches!(
        f.apply(TimestampCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(original.view.authority()),
            next: cold.view.authority(),
            ready: cold,
        })
        .await,
        TimestampReply::Fenced { .. }
    ));
    assert_eq!(f.apply(cancel).await, TimestampReply::Released);
    let stale = cancel_command(&f, &unaccepted_rejection(&other_begin));
    assert_eq!(
        f.apply(stale).await,
        TimestampReply::Rejected(Fault::StaleReadyView)
    );
    assert!(matches!(f.apply(TimestampCommand::AdvanceFloor {
        authority: cold.view.authority(),
    }).await, TimestampReply::Retention { safe_ts, .. } if safe_ts == other_pin.read_ts.get()));
}

#[tokio::test]
async fn q02_q06_o09_o13_r04_unaccepted_rejection_preserves_other_snapshot_states() {
    let mut f = Fixture::new().await;
    let member = worker();
    let original = ready(member);
    f.initialize(original).await;
    let (identity, request) = binding(original.view);
    let begin_app = begin(original, &identity, request);
    let TimestampReply::Snapshot(pin) = f.apply(begin_app.clone()).await else {
        panic!("application pin");
    };
    let rejection = unaccepted_rejection(&begin_app);
    let mut wrong_nonce = rejection.clone();
    wrong_nonce.identity.nonce = nabedra_rt::ids::uuid_v4();
    let wrong_nonce = cancel_command(&f, &wrong_nonce);
    assert_eq!(
        f.apply(wrong_nonce).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    let mut wrong_reservation = rejection.clone();
    wrong_reservation.reservation.kind = RequestKind::Mutation;
    let wrong_reservation = cancel_command(&f, &wrong_reservation);
    assert_eq!(
        f.apply(wrong_reservation).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );

    assert_eq!(
        f.apply(bind_context(original, &identity, pin, request))
            .await,
        TimestampReply::ContextBound(pin)
    );
    let unbound = cancel_command(&f, &rejection);
    assert_eq!(
        f.apply(unbound).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(
        f.apply(TimestampCommand::Release {
            proof: proof(member, original, identity, request, pin),
        })
        .await,
        TimestampReply::Released
    );
    let released = cancel_command(
        &f,
        &RejectedIdentity {
            pin: Some(pin),
            request: Some(request),
            ..rejection
        },
    );
    assert_eq!(
        f.apply(released).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );

    let (identity, request) = binding(original.view);
    let mut metadata = unaccepted_rejection(&begin(original, &identity, request));
    metadata.reservation.kind = RequestKind::Mutation;
    let begin_metadata = TimestampCommand::BeginImportMetadata {
        authority: original.view.authority(),
        archive_id: nabedra_rt::ids::uuid_v4(),
        target_id: nabedra_rt::ids::uuid_v4(),
        txn: TxnId {
            request_id: request.request_id,
            attempt: 1,
        },
        reservation: metadata.reservation,
        pin_id: nabedra_rt::ids::uuid_v4(),
    };
    let TimestampReply::Snapshot(metadata_pin) = f.apply(begin_metadata.clone()).await else {
        panic!("import metadata pin");
    };
    for proof_pin in [None, Some(metadata_pin)] {
        metadata.pin = proof_pin;
        let cancel_metadata = cancel_command(&f, &metadata);
        assert_eq!(
            f.apply(cancel_metadata).await,
            TimestampReply::Rejected(Fault::InvalidBinding)
        );
    }
    assert_eq!(
        f.apply(begin_metadata).await,
        TimestampReply::Snapshot(metadata_pin)
    );
}

/// O15/R05: a ready guard fences the old release before Control publishes its
/// next view. Retrying the guard after an interrupted reply returns its first cut.
#[tokio::test]
async fn o15_r05_ready_fence_retries_keep_original_cut_and_reject_old_copy_proofs() {
    let mut f = Fixture::new().await;
    let member = worker();
    let old = ready(member);
    f.initialize(old).await;
    let (identity, request) = binding(old.view);
    let TimestampReply::Snapshot(pin) = f.apply(begin(old, &identity, request)).await else {
        panic!("pin")
    };
    f.apply(bind_context(old, &identity, pin, request)).await;
    let old_proof = proof(member, old, identity.clone(), request, pin);
    let mut next = old;
    next.view.membership_revision += 1;
    let transition_id = nabedra_rt::ids::uuid_v4();
    let command = TimestampCommand::FenceReady {
        transition_id,
        expected: old,
        next,
    };
    let fence_index = f.next;
    let first = f.apply(command.clone()).await;
    let TimestampReply::ReadyFenced(ref receipt) = first else {
        panic!("receipt")
    };
    assert_eq!(receipt.applied_at.index, fence_index);
    assert_eq!(f.apply(command).await, first);
    assert_eq!(
        f.apply(TimestampCommand::Release { proof: old_proof })
            .await,
        TimestampReply::Rejected(Fault::StaleReadyView)
    );
    assert_eq!(
        f.apply(TimestampCommand::Release {
            proof: proof(member, next, identity, request, pin)
        })
        .await,
        TimestampReply::Released
    );
    let mut conflicting = next;
    conflicting.ready_count = 2;
    assert_eq!(
        f.apply(TimestampCommand::FenceReady {
            transition_id,
            expected: old,
            next: conflicting
        })
        .await,
        TimestampReply::Rejected(Fault::TransitionConflict)
    );
}

/// R04/R05: owner takeover retains the original read pin. A new run cannot
/// infer abandonment from a heartbeat or erase retention metadata on its own.
#[tokio::test]
async fn r04_r05_owner_and_run_fences_preserve_old_storage_pins_and_reject_stale_writes() {
    let mut f = Fixture::new().await;
    let old = ready(worker());
    f.initialize(old).await;
    let (identity, request) = binding(old.view);
    let TimestampReply::Snapshot(pin) = f.apply(begin(old, &identity, request)).await else {
        panic!("pin")
    };
    let mut takeover = old;
    takeover.view.owner_term += 1;
    f.apply(TimestampCommand::Fence {
        transition_id: nabedra_rt::ids::uuid_v4(),
        expected: Some(old.view.authority()),
        next: takeover.view.authority(),
        ready: takeover,
    })
    .await;
    assert_eq!(
        f.apply(begin(old, &identity, request)).await,
        TimestampReply::Rejected(Fault::StaleReadyView)
    );
    assert_eq!(
        f.apply(begin(takeover, &identity, request)).await,
        TimestampReply::Snapshot(pin)
    );
    let cold = ReadyMembership {
        view: AuthorityView {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 3,
            membership_revision: 2,
        },
        ready_root: ReadySetHasher::default().finish(),
        ready_count: 0,
    };
    assert!(matches!(
        f.apply(TimestampCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(takeover.view.authority()),
            next: cold.view.authority(),
            ready: cold
        })
        .await,
        TimestampReply::Fenced {
            allocator_epoch: 3,
            ..
        }
    ));
    assert!(matches!(
        f.apply(TimestampCommand::AdvanceFloor {
            authority: cold.view.authority()
        })
        .await,
        TimestampReply::Retention { safe_ts: 1, .. }
    ));
    // Keep the directory owner alive through every native operation.
    assert!(f.dir.0.exists());
}

/// O09/O10/R05: allocation happens after sealed preparation and before the
/// immutable Status decision. A lost reply recovers the same stamp through a
/// freshly authorized owner without allowing the stale owner another allocation.
#[tokio::test]
async fn o09_o10_commit_allocation_is_after_read_and_survives_authorized_owner_takeover() {
    use crate::oltp_replication::authority::status::{CommitPermitStatement, SealedCommitPermit};
    let mut f = Fixture::new().await;
    let old = ready(worker());
    f.initialize(old).await;
    let (identity, request) = binding(old.view);
    let mut begin = begin(old, &identity, request);
    if let TimestampCommand::BeginSnapshot { reservation, .. } = &mut begin {
        reservation.kind = super::super::status::RequestKind::Mutation;
    }
    let TimestampReply::Snapshot(pin) = f.apply(begin).await else {
        panic!("pin")
    };
    f.apply(bind_context(old, &identity, pin, request)).await;
    let statement = CommitPermitStatement {
        authority: old.view.authority(),
        txn: TxnId {
            request_id: request.request_id,
            attempt: 1,
        },
        request,
        manifest: ManifestRef {
            status_group_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([9; 32]),
            participant_count: 2,
        },
        read_ts: pin.read_ts,
        prepared_count: 2,
        prepared_at: AppliedPosition {
            term: 4,
            node_id: 3,
            index: 18,
        },
    };
    let permit = SealedCommitPermit::for_test(statement.clone()).unwrap();
    let allocation_index = f.next;
    let TimestampReply::Commit(stamp) = f
        .apply(TimestampCommand::AllocateCommit {
            permit: permit.clone(),
        })
        .await
    else {
        panic!("commit timestamp")
    };
    assert!(stamp.commit_ts > pin.read_ts);
    assert_eq!(stamp.allocated_at.index, allocation_index);
    assert_eq!(
        f.apply(TimestampCommand::AllocateCommit {
            permit: permit.clone()
        })
        .await,
        TimestampReply::Commit(stamp)
    );
    let mut next = old;
    next.view.owner_term += 1;
    f.apply(TimestampCommand::Fence {
        transition_id: nabedra_rt::ids::uuid_v4(),
        expected: Some(old.view.authority()),
        next: next.view.authority(),
        ready: next,
    })
    .await;
    assert_eq!(
        f.apply(TimestampCommand::AllocateCommit { permit }).await,
        TimestampReply::Rejected(Fault::StaleAuthority)
    );
    let mut refreshed = statement;
    refreshed.authority = next.view.authority();
    let fresh = SealedCommitPermit::for_test(refreshed.clone()).unwrap();
    assert_eq!(
        f.apply(TimestampCommand::AllocateCommit { permit: fresh })
            .await,
        TimestampReply::Commit(stamp)
    );
    refreshed.manifest.digest = Digest::from_bytes([10; 32]);
    assert_eq!(
        f.apply(TimestampCommand::AllocateCommit {
            permit: SealedCommitPermit::for_test(refreshed).unwrap()
        })
        .await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
}

#[tokio::test]
async fn q06_retirement_rechecks_current_tso_release_and_keeps_original_on_retry() {
    let mut fixture = Fixture::new().await;
    let member = worker();
    let original = ready(member);
    fixture.initialize(original).await;
    let (identity, request) = binding(original.view);
    let TimestampReply::Snapshot(pin) = fixture.apply(begin(original, &identity, request)).await else {
        panic!("storage pin");
    };
    fixture.apply(bind_context(original, &identity, pin, request)).await;
    let initial = proof(member, original, identity.clone(), request, pin);
    let initial_position = fixture.next;
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: initial.clone() }).await, TimestampReply::Released);
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: proof(member, original, identity.clone(), request, pin) }).await, TimestampReply::Released);
    assert_eq!(released_at(&fixture, identity.nonce).await, initial_position);
    let mut next = original;
    next.view.membership_revision += 1;
    assert!(matches!(fixture.apply(TimestampCommand::FenceReady {
        transition_id: nabedra_rt::ids::uuid_v4(), expected: original, next,
    }).await, TimestampReply::ReadyFenced(_)));
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: initial.clone() }).await, TimestampReply::Rejected(Fault::StaleReadyView));

    let mut statement = initial.statement.clone();
    statement.ready = next;
    statement.terminal_version += 1;
    let mut changed = statement.clone();
    changed.terminal_digest = Some(Digest::from_bytes([9; 32]));
    let seal = |statement: TerminalCopyStatement| {
        let digest = statement_digest(&statement).unwrap();
        let mut verifier = TerminalVerifier::begin(statement, next).unwrap();
        verifier.accept(member, &AuthenticatedAck::from_transport(member, digest, nabedra_rt::ids::uuid_v4())).unwrap();
        verifier.finish(next).unwrap().into_sealed()
    };
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: seal(changed) }).await, TimestampReply::Rejected(Fault::InvalidBinding));
    let refreshed_at = fixture.next;
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: seal(statement.clone()) }).await, TimestampReply::Released);
    assert_eq!(fixture.apply(TimestampCommand::Release { proof: seal(statement) }).await, TimestampReply::Released);
    assert_eq!(released_at(&fixture, identity.nonce).await, refreshed_at);
}

async fn released_at(fixture: &Fixture, nonce: Uuid) -> u64 {
    let bytes = fixture.machine.store().read_metadata(BUDGET, move |view| {
        let bytes = view.get(&pin_key(nonce))?.unwrap();
        let record: PinRecord = serde_json::from_slice(bytes.as_slice()).unwrap();
        let PinState::Released { applied_at, .. } = record.state else {
            panic!("released storage pin");
        };
        view.encode(&applied_at.index)
    }).await.unwrap();
    serde_json::from_slice(bytes.as_slice()).unwrap()
}

#[tokio::test]
async fn o16_o21_o26_o27_o28_automatic_split_keeps_bound_placement_on_exact_retry() {
    use super::split::{AutomaticSplitOrigin, ImageCut, SplitAttempt, SplitIntent, SplitPlacement,
        SplitSourcePlacement, SplitTimestampCommand, SplitVoter};
    use crate::oltp::mvcc::{
        planner::split::child_group,
        store::{
            KeySpan,
            lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
            lineage::SplitBinding,
            split_usage::ShardEpoch,
        },
    };
    use crate::oltp_replication::authority::{
        control::{GroupRecord, group_root, group_seed, group_step},
        status::RequestKind,
    };
    use crate::protocol::Key;

    let mut fixture = Fixture::new().await;
    let ready = ready(worker());
    fixture.initialize(ready).await;
    let authority = ready.view.authority();
    let table_id = nabedra_rt::ids::uuid_v4();
    let source = GroupIdentity {
        kind: GroupKind::Data { table_id, shard_id: nabedra_rt::ids::uuid_v4() },
        group_id: nabedra_rt::ids::uuid_v4(),
        ..fixture.machine.store().identity().clone()
    };
    let origin = AutomaticSplitOrigin { epoch: ShardEpoch { schema: 1, routing: 2 }, slot: 1 };
    let split_id = origin.id(&source);
    let intent = SplitIntent {
        split_id,
        source: source.clone(),
        automatic: Some(origin),
        boundary: Key::Int64(11),
        page_bytes: 256 << 10,
    };
    let catalog_group_id = nabedra_rt::ids::uuid_v4();
    let reservation = IdentityReservation {
        request_id: split_id,
        kind: RequestKind::Mutation,
        scoped: None,
        reservation_id: nabedra_rt::ids::uuid_v4(),
        original_run: authority.run_id,
        digest: intent.digest().unwrap(),
    };
    assert!(matches!(
        fixture.apply(TimestampCommand::SplitMetadata {
            command: SplitTimestampCommand::Open {
                authority, intent: intent.clone(), catalog_group_id, reservation, pin_id: nabedra_rt::ids::uuid_v4(),
            },
        }).await,
        TimestampReply::Snapshot(_)
    ));
    let read = |store: GroupStore| async move {
        let raw = store.read_metadata(split::BUDGET, move |view| {
            view.get(&split::key(split_id))?.ok_or_else(unknown)
        }).await.unwrap();
        serde_json::from_slice::<SplitAttempt>(raw.as_slice()).unwrap()
    };
    // RANGES-2: an opened attempt is in Timestamps' index of open attempts.
    let listed = fixture
        .machine
        .store()
        .read_metadata(split::BUDGET, move |view| {
            let present = view.get(&split::open_key(split_id))?.is_some();
            view.encode(&present)
        })
        .await
        .unwrap();
    assert!(serde_json::from_slice::<bool>(listed.as_slice()).unwrap());
    let attempt = read(fixture.machine.store().clone()).await;
    let guard = VerifiedLifecycleGuard::model_for_test(
        LifecycleBinding {
            guard_id: nabedra_rt::ids::uuid_v4(),
            catalog_group_id,
            table_id,
            lifecycle_version: None,
            schema_epoch: origin.epoch.schema,
            txn: attempt.txn,
            request: attempt.request,
            read_ts: attempt.snapshot.read_ts,
            authority,
        },
        GuardMode::Exclusive,
    ).into_sealed();
    let TimestampReply::Snapshot(image) = fixture.apply(TimestampCommand::SplitMetadata {
        command: SplitTimestampCommand::Cut {
            authority, split_id, guard: guard.clone(), pin_id: nabedra_rt::ids::uuid_v4(),
        },
    }).await else {
        panic!("split image cut was refused")
    };
    let span = KeySpan { start: None, end: None };
    let children = std::array::from_fn(|index| SplitBinding {
        split_id,
        source: source.clone(),
        source_schema_epoch: origin.epoch.schema,
        source_routing_epoch: origin.epoch.routing,
        source_span: span.clone(),
        target: child_group(&source, split_id, index).unwrap(),
        target_span: if index == 0 {
            KeySpan { start: None, end: Some(intent.boundary.clone()) }
        } else {
            KeySpan { start: Some(intent.boundary.clone()), end: None }
        },
        read_ts: image.read_ts,
        guard: guard.clone(),
    });
    let voters: Vec<_> = [2, 13, 41].into_iter().map(|node_id| {
        let worker = worker();
        SplitVoter {
            worker,
            member: ReplicaMember { node_id, worker_id: worker.worker_id, pool_id: nabedra_rt::ids::uuid_v4() },
            registered_pool_generation: (node_id == 41).then_some(9),
        }
    }).collect();
    let placement = SplitPlacement {
        source: SplitSourcePlacement {
            record: GroupRecord {
                identity: source,
                generation: nabedra_rt::ids::uuid_v4(),
                replica_count: 3,
                replica_root: group_root(3, voters.iter().fold(group_seed(), |prior, voter| {
                    group_step(prior, &voter.member)
                })),
                membership_position: AppliedPosition { term: 3, node_id: 41, index: 20 },
            },
            voters: Page::new(voters).unwrap(),
            domains: Some(Digest::from_bytes([6; 32])),
        },
        pool_generations: Page::new(vec![1, 4, 9]).unwrap(),
        required_bytes: 1 << 30,
    };
    let bind = |placement: Option<SplitPlacement>| TimestampCommand::SplitMetadata {
        command: SplitTimestampCommand::Bind {
            authority, split_id, children: Box::new(children.clone()),
            placement: placement.map(Box::new),
        },
    };
    for _ in 0..2 {
        assert!(matches!(
            fixture.apply(bind(Some(placement.clone()))).await,
            TimestampReply::Snapshot(pin) if pin == image
        ));
    }
    let mut changed_generation = placement.clone();
    changed_generation.source.record.generation = nabedra_rt::ids::uuid_v4();
    let mut changed_mount = placement.clone();
    changed_mount.pool_generations = Page::new(vec![1, 5, 9]).unwrap();
    for substitute in [Some(changed_generation), Some(changed_mount), None] {
        assert!(matches!(
            fixture.apply(bind(substitute)).await,
            TimestampReply::Rejected(Fault::TransitionConflict)
        ));
    }
    let mut wrong_remote_pool = placement.clone();
    wrong_remote_pool.pool_generations = Page::new(vec![1, 4, 8]).unwrap();
    assert!(matches!(
        fixture.apply(bind(Some(wrong_remote_pool))).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    ));
    let attempt = read(fixture.machine.store().clone()).await;
    assert_eq!(attempt.placement(), Some(&placement));
    assert_eq!(attempt.image.as_ref().unwrap().pin, image);
    assert_eq!(attempt.children().unwrap(), &children);

    let mut old = attempt.image.unwrap();
    old.placement = None;
    let old_encoding = serde_json::to_value(&old).unwrap();
    assert!(old_encoding.get("placement").is_none());
    assert_eq!(serde_json::from_value::<ImageCut>(old_encoding).unwrap(), old);

    // RANGES-2: Close (commit here, abort below) removes the attempt from
    // the open index, and the listing then holds only open attempts.
    use crate::oltp_replication::authority::status::{SettlementStatement, VerifiedSettlement};
    let listed = |store: GroupStore| async move {
        let raw = store
            .read_metadata(split::BUDGET, |view| {
                let ids = split::open_page(view, None)?;
                view.encode(&ids)
            })
            .await
            .unwrap();
        serde_json::from_slice::<Vec<Uuid>>(raw.as_slice()).unwrap()
    };
    let open_other = |slot: u8| {
        let origin = AutomaticSplitOrigin { epoch: origin.epoch, slot };
        let intent = SplitIntent { split_id: origin.id(&intent.source), automatic: Some(origin), ..intent.clone() };
        let reservation = IdentityReservation {
            request_id: intent.split_id,
            digest: intent.digest().unwrap(),
            reservation_id: nabedra_rt::ids::uuid_v4(),
            ..reservation
        };
        (intent.split_id, TimestampCommand::SplitMetadata {
            command: SplitTimestampCommand::Open {
                authority, intent, catalog_group_id, reservation, pin_id: nabedra_rt::ids::uuid_v4(),
            },
        })
    };
    let (aborted_id, open_aborted) = open_other(2);
    let (kept_id, open_kept) = open_other(3);
    for command in [open_aborted, open_kept] {
        assert!(matches!(fixture.apply(command).await, TimestampReply::Snapshot(_)));
    }
    let mut ids = vec![split_id, aborted_id, kept_id];
    ids.sort();
    assert_eq!(listed(fixture.machine.store().clone()).await, ids);
    let settle = |attempt: &SplitAttempt, decision: TxnDecision, participant_count: u64| {
        let manifest = ManifestRef {
            status_group_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([9; 32]),
            participant_count,
        };
        let position = AppliedPosition { term: 1, node_id: 1, index: 99 };
        VerifiedSettlement::model_for_test(SettlementStatement {
            txn: attempt.txn,
            request: attempt.request,
            manifest,
            decision: DecisionStatement {
                txn: attempt.txn,
                request: attempt.request,
                manifest,
                decided_at: position,
                decision,
            },
            resolved_count: participant_count,
            settled_at: position,
        })
        .unwrap()
        .into_sealed()
    };
    let close = |id, settled| TimestampCommand::SplitMetadata {
        command: SplitTimestampCommand::Close { authority, split_id: id, settled },
    };
    let bound = read(fixture.machine.store().clone()).await;
    let commit_ts = crate::oltp_replication::types::Timestamp::new(image.read_ts.get() + 1).unwrap();
    assert_eq!(
        fixture.apply(close(split_id, settle(&bound, TxnDecision::Commit { commit_ts }, 1))).await,
        TimestampReply::Released
    );
    let raw = fixture.machine.store().read_metadata(split::BUDGET, move |view| {
        view.get(&split::key(aborted_id))?.ok_or_else(unknown)
    }).await.unwrap();
    let unbound: SplitAttempt = serde_json::from_slice(raw.as_slice()).unwrap();
    let abort = TxnDecision::Abort { reason: AbortReason::ResourceLimit };
    assert_eq!(
        fixture.apply(close(aborted_id, settle(&unbound, abort, 0))).await,
        TimestampReply::Released
    );
    assert_eq!(listed(fixture.machine.store().clone()).await, vec![kept_id]);
}

#[tokio::test]
async fn o08_q06_retry_pin_keeps_original_context_and_releases_with_receipt() {
    use crate::oltp_replication::authority::status::{
        CommitPermitStatement, SealedCommitPermit, SettlementStatement, VerifiedSettlement,
    };

    let mut f = Fixture::new().await;
    let member = worker();
    let ready = ready(member);
    f.initialize(ready).await;
    let (identity, request) = binding(ready.view);
    let mut begin = begin(ready, &identity, request);
    if let TimestampCommand::BeginSnapshot { reservation, .. } = &mut begin {
        reservation.kind = RequestKind::Mutation;
    }
    let TimestampReply::Snapshot(original) = f.apply(begin).await else {
        panic!("expected original pin");
    };
    f.apply(bind_context(ready, &identity, original, request))
        .await;
    let txn = TxnId {
        request_id: request.request_id,
        attempt: 1,
    };
    let manifest = ManifestRef {
        status_group_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([9; 32]),
        participant_count: 1,
    };
    let position = AppliedPosition {
        term: 1,
        node_id: 1,
        index: 9,
    };
    let settlement = |request, reason| {
        let decision = DecisionStatement {
            txn,
            request,
            manifest,
            decided_at: position,
            decision: TxnDecision::Abort { reason },
        };
        VerifiedSettlement::model_for_test(SettlementStatement {
            txn,
            request,
            manifest,
            decision,
            resolved_count: 1,
            settled_at: position,
        })
        .unwrap()
        .into_sealed()
    };
    let retry = |view, request, reason| TimestampCommand::RetrySnapshot {
        view,
        identity: identity.clone(),
        previous: settlement(request, reason),
        pin_id: nabedra_rt::ids::uuid_v4(),
    };
    assert_eq!(
        f.apply(retry(ready.view, request, AbortReason::Conflict))
            .await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    let changed = RequestBinding {
        accepted_context: Digest::from_bytes([7; 32]),
        ..request
    };
    assert_eq!(
        f.apply(retry(ready.view, changed, AbortReason::RetrySnapshot))
            .await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    let TimestampReply::Snapshot(fresh) = f
        .apply(retry(ready.view, request, AbortReason::RetrySnapshot))
        .await
    else {
        panic!("expected fresh pin");
    };
    assert!(fresh.read_ts > original.read_ts && fresh.pin_id != original.pin_id);
    let floor = TimestampCommand::AdvanceFloor {
        authority: ready.view.authority(),
    };
    assert!(
        matches!(f.apply(floor.clone()).await, TimestampReply::Retention { safe_ts, .. }
                     if safe_ts == original.read_ts.get())
    );
    assert_eq!(
        f.apply(retry(ready.view, request, AbortReason::RetrySnapshot))
            .await,
        TimestampReply::Snapshot(fresh)
    );

    let allocate = |attempt, read_ts| TimestampCommand::AllocateCommit {
        permit: SealedCommitPermit::for_test(CommitPermitStatement {
            authority: ready.view.authority(),
            txn: TxnId { attempt, ..txn },
            request,
            manifest,
            read_ts,
            prepared_count: 1,
            prepared_at: position,
        })
        .unwrap(),
    };
    assert_eq!(
        f.apply(allocate(3, fresh.read_ts)).await,
        TimestampReply::Rejected(Fault::UnknownPin)
    );
    assert!(
        matches!(f.apply(allocate(2, fresh.read_ts)).await, TimestampReply::Commit(stamp)
                     if stamp.commit_ts > fresh.read_ts)
    );
    assert_eq!(
        f.apply(TimestampCommand::Release {
            proof: proof(member, ready, identity.clone(), request, original),
        })
        .await,
        TimestampReply::Released
    );
    assert_eq!(
        f.apply(retry(ready.view, request, AbortReason::RetrySnapshot))
            .await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    assert!(
        matches!(f.apply(floor).await, TimestampReply::Retention { safe_ts, .. }
                     if safe_ts > fresh.read_ts.get())
    );
}
