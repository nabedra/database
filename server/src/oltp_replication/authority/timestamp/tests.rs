use super::*;
use crate::{
    config::WorkerResources,
    oltp::pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
    oltp_replication::{
        ReadySetHasher,
        authority::proof::{AuthenticatedAck, TerminalVerifier},
    },
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

struct Dir(std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    machine: TimestampMachine,
    pool: OltpPool,
    dir: Dir,
    next: u64,
}
impl Fixture {
    async fn new() -> Self {
        let dir = Dir(std::env::temp_dir().join(format!("oltp-timestamps-{}", Uuid::new_v4())));
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
                pool_format_version: 1,
                cluster_id: Uuid::new_v4(),
                worker_id: Uuid::new_v4(),
                pool_id: Uuid::new_v4(),
            },
            shared,
        )
        .unwrap();
        let group = pool
            .group(GroupIdentity {
                cluster_id: pool.identity().cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: Uuid::new_v4(),
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
    async fn apply(&mut self, command: TimestampCommand) -> TimestampReply {
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
    async fn initialize(&mut self, ready: ReadyMembership) {
        assert!(matches!(
            self.apply(TimestampCommand::Fence {
                transition_id: Uuid::new_v4(),
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
fn worker() -> WorkerIdentity {
    WorkerIdentity {
        worker_id: Uuid::new_v4(),
        incarnation: Uuid::new_v4(),
        certificate_sha256: Digest::from_bytes([3; 32]),
        enrollment_revision: 1,
    }
}
fn ready(member: WorkerIdentity) -> ReadyMembership {
    let mut hash = ReadySetHasher::default();
    hash.push(member).unwrap();
    ReadyMembership {
        view: AuthorityView {
            run_id: Uuid::new_v4(),
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
        request_id: Uuid::new_v4(),
        digest: Digest::from_bytes([4; 32]),
        accepted_context: Digest::from_bytes([5; 32]),
    };
    (
        PinIdentity {
            run_id: view.run_id,
            request_id: request.request_id,
            digest: request.digest,
            nonce: Uuid::new_v4(),
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
        },
        pin_id: Uuid::new_v4(),
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
        pin_identity: identity,
        storage_pin: pin,
    };
    let mut verifier = TerminalVerifier::begin(statement.clone(), ready).unwrap();
    let ack = AuthenticatedAck::from_transport(
        member,
        statement_digest(&statement).unwrap(),
        Uuid::new_v4(),
    );
    verifier.accept(member, &ack).unwrap();
    verifier.finish(ready).unwrap().into_sealed()
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
    let transition_id = Uuid::new_v4();
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
        transition_id: Uuid::new_v4(),
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
            run_id: Uuid::new_v4(),
            owner_term: 3,
            membership_revision: 2,
        },
        ready_root: ReadySetHasher::default().finish(),
        ready_count: 0,
    };
    assert!(matches!(
        f.apply(TimestampCommand::Fence {
            transition_id: Uuid::new_v4(),
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
            status_group_id: Uuid::new_v4(),
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
        transition_id: Uuid::new_v4(),
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
