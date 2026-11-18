use super::*;
use crate::{
    config::WorkerResources,
    oltp::pool::{OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
};

struct Dir(std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    machine: StatusMachine,
    pool: OltpPool,
    dir: Dir,
    index: u64,
}
impl Fixture {
    async fn new() -> Self {
        let dir = Dir(std::env::temp_dir().join(format!("status-identity-{}", Uuid::new_v4())));
        let shared = WorkerSharedResources::new(
            3,
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
                kind: GroupKind::Status { bucket: 0 },
                group_format_version: 1,
            })
            .await
            .unwrap();
        Self {
            machine: StatusMachine::new(group).unwrap(),
            pool,
            dir,
            index: 0,
        }
    }
    async fn apply(&mut self, command: StatusCommand) -> StatusReply {
        let command =
            OwnedCommand::new_owned(command, self.pool.resources(), ByteClass::Progress).unwrap();
        let meta = ApplyMeta {
            position: AppliedPosition {
                term: 1,
                node_id: 1,
                index: self.index,
            },
            command_digest: serialized_digest(&command).unwrap(),
            membership: None,
        };
        self.index += 1;
        let reply = self.machine.apply(meta, command).await.unwrap();
        serde_json::from_slice(reply.as_slice()).unwrap()
    }
    async fn reopen(self) -> Self {
        let Self {
            machine,
            pool,
            dir,
            index,
        } = self;
        let identity = pool.identity().clone();
        let group = machine.store().identity().clone();
        let shared = pool.resources().clone();
        drop(machine);
        drop(pool);
        let pool = OltpPool::open_development(&dir.0, identity, shared).unwrap();
        let machine = StatusMachine::new(pool.group(group).await.unwrap()).unwrap();
        Self {
            machine,
            pool,
            dir,
            index,
        }
    }
}
fn reserve(authority: Authority, id: Uuid, digest: Digest) -> StatusCommand {
    StatusCommand::ReserveIdentity {
        authority,
        request_id: id,
        digest,
        kind: RequestKind::Mutation,
        reservation_id: Uuid::new_v4(),
    }
}

/// R04/R05/O10: permanent identity metadata prevents an unchanged delayed POST
/// from acquiring a new run after all RAM is lost. No executable body is logged.
#[tokio::test]
async fn r04_r05_o10_reserved_uuid_survives_reopen_and_never_relabels_into_a_new_run() {
    let mut f = Fixture::new().await;
    let authority = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 1,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: None,
        next: authority,
    })
    .await;
    let request = Uuid::new_v4();
    let digest = Digest::from_bytes([3; 32]);
    let first = f.apply(reserve(authority, request, digest)).await;
    let StatusReply::Identity(IdentityLookup::Reserved(reservation)) = first else {
        panic!("reservation")
    };
    assert_eq!(reservation.original_run, authority.run_id);
    assert_eq!(
        f.apply(reserve(authority, request, digest)).await,
        StatusReply::Identity(IdentityLookup::Reserved(reservation))
    );
    let mut f = f.reopen().await;
    let owner = Authority {
        owner_term: 2,
        ..authority
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: Some(authority),
        next: owner,
    })
    .await;
    assert_eq!(
        f.apply(reserve(owner, request, digest)).await,
        StatusReply::Identity(IdentityLookup::Reserved(reservation))
    );
    let cold = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 3,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: Some(owner),
        next: cold,
    })
    .await;
    assert_eq!(
        f.apply(reserve(cold, request, digest)).await,
        StatusReply::Identity(IdentityLookup::Abandoned(reservation))
    );
    assert_eq!(
        f.apply(reserve(owner, Uuid::new_v4(), digest)).await,
        StatusReply::Rejected(StatusFault::StaleAuthority)
    );
    assert_eq!(
        f.apply(reserve(cold, request, Digest::from_bytes([4; 32])))
            .await,
        StatusReply::Rejected(StatusFault::DigestConflict)
    );
    let encoded = serde_json::to_string(&reserve(cold, request, digest)).unwrap();
    for forbidden in [
        "operation",
        "payload",
        "query",
        "authorization",
        "queue_order",
    ] {
        assert!(!encoded.contains(forbidden), "{forbidden}")
    }
}

/// O10/O29: an unreadable permanent identity is Unknown, never treated as absent
/// so the same UUID cannot be admitted as a fresh operation.
#[tokio::test]
async fn o10_o29_corrupt_identity_cannot_be_recomputed_as_absent() {
    let mut f = Fixture::new().await;
    let authority = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 1,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: None,
        next: authority,
    })
    .await;
    let request = Uuid::new_v4();
    let digest = Digest::from_bytes([5; 32]);
    f.apply(reserve(authority, request, digest)).await;
    let index = f.index;
    f.index += 1;
    f.machine
        .store()
        .transaction(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index,
                },
                command_digest: Digest::from_bytes([6; 32]),
                membership: None,
            },
            BUDGET,
            move |tx| {
                tx.put(&key(IDENTITIES, request), b"invalid identity bytes")?;
                Ok(Transition::Commit(tx.copy_output(b"corrupted model")?))
            },
        )
        .await
        .unwrap();
    let command = OwnedCommand::new_owned(
        reserve(authority, request, digest),
        f.pool.resources(),
        ByteClass::Progress,
    )
    .unwrap();
    let result = f
        .machine
        .apply(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: f.index,
                },
                command_digest: serialized_digest(&command).unwrap(),
                membership: None,
            },
            command,
        )
        .await
        .unwrap_err();
    assert_eq!(result.outcome, Outcome::Unknown);
}

/// Q02/F03: rejection freezes only a proven never-accepted reservation. A
/// delayed retry cannot turn that UUID into work or replace its original error.
#[tokio::test]
async fn q02_f03_rejection_cas_is_durable_and_cannot_become_a_transaction() {
    let mut f = Fixture::new().await;
    let authority = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 1,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: None,
        next: authority,
    })
    .await;
    let id = Uuid::new_v4();
    let digest = Digest::from_bytes([17; 32]);
    let StatusReply::Identity(IdentityLookup::Reserved(reservation)) =
        f.apply(reserve(authority, id, digest)).await
    else {
        panic!("reservation");
    };
    let view = AuthorityView {
        run_id: authority.run_id,
        owner_term: 1,
        membership_revision: 1,
    };
    let identity = PinIdentity {
        run_id: authority.run_id,
        request_id: id,
        digest,
        nonce: reservation.reservation_id,
    };
    let failure = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Overloaded,
        "queue capacity unavailable",
        Outcome::NotApplied,
    ))
    .unwrap();
    let command = StatusCommand::RejectUnaccepted {
        view,
        reservation,
        identity,
        pin: None,
        request: None,
        failure,
    };
    let StatusReply::Identity(IdentityLookup::Rejected(original)) = f.apply(command.clone()).await
    else {
        panic!("rejected identity");
    };
    let mut f = f.reopen().await;
    assert_eq!(
        f.apply(command.clone()).await,
        StatusReply::Identity(IdentityLookup::Rejected(original.clone()))
    );
    assert_eq!(
        f.apply(reserve(authority, id, digest)).await,
        StatusReply::Identity(IdentityLookup::Rejected(original))
    );
    let request = RequestBinding {
        run_id: authority.run_id,
        request_id: id,
        digest,
        accepted_context: Digest::from_bytes([18; 32]),
    };
    let outcome = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "changed",
        Outcome::NotApplied,
    ))
    .unwrap();
    assert_eq!(
        f.apply(StatusCommand::TerminalFailure {
            authority,
            request,
            outcome
        })
        .await,
        StatusReply::Rejected(StatusFault::TerminalConflict)
    );
    let StatusCommand::RejectUnaccepted {
        view,
        reservation,
        identity,
        pin,
        request,
        ..
    } = command
    else {
        unreachable!()
    };
    let unknown = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "uncertain",
        Outcome::Unknown,
    ))
    .unwrap();
    assert_eq!(
        f.apply(StatusCommand::RejectUnaccepted {
            view,
            reservation,
            identity,
            pin,
            request,
            failure: unknown
        })
        .await,
        StatusReply::Rejected(StatusFault::InvalidBinding)
    );
}

/// O22: extracting and cloning a terminal's bounded error retains its decoder
/// reservation until the final heap-bearing value is dropped.
#[tokio::test]
async fn o22_extracted_terminal_outcome_retains_reply_decode_owner() {
    let f = Fixture::new().await;
    let shared = f.pool.resources();
    let baseline = shared.reply_decode_bytes_used();
    let owner = Arc::new(shared.reserve_reply_decode(8192).await.unwrap());
    let mut outcome = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "retained error",
        Outcome::NotApplied,
    ))
    .unwrap();
    outcome.retain_owner(owner.clone());
    let clone = outcome.clone();
    drop(owner);
    drop(outcome);
    assert_eq!(shared.reply_decode_bytes_used(), baseline + 8192);
    assert!(clone.result().is_err());
    drop(clone);
    assert_eq!(shared.reply_decode_bytes_used(), baseline);
}

/// R04/O15: cleanup of a fenced preparation must not change an abandoned old
/// receipt into a new failure. Actual earlier failures and COMMIT stay exact.
#[tokio::test]
async fn r04_o15_fence_abort_cleanup_preserves_abandoned_but_not_prior_failures() {
    let mut f = Fixture::new().await;
    let old = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 1,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: None,
        next: old,
    })
    .await;
    let request_id = Uuid::new_v4();
    let digest = Digest::from_bytes([61; 32]);
    let StatusReply::Identity(IdentityLookup::Reserved(reservation)) =
        f.apply(reserve(old, request_id, digest)).await
    else {
        panic!("reserved")
    };
    let request = RequestBinding {
        run_id: old.run_id,
        request_id,
        digest,
        accepted_context: Digest::from_bytes([62; 32]),
    };
    let txn = TxnId {
        request_id,
        attempt: 1,
    };
    let outcome = super::super::MutationOutcome::from_success(
        &crate::oltp::mvcc::CanonicalJson::from_serializable(
            f.pool.resources(),
            &serde_json::Value::Null,
            1024,
        )
        .unwrap(),
    )
    .unwrap();
    let result_digest = outcome.digest().unwrap();
    assert!(matches!(
        f.apply(StatusCommand::BeginManifest {
            authority: old,
            txn,
            request,
            read_ts: Timestamp::new(1).unwrap(),
            participant_count: 1,
            outcome,
            result_digest
        })
        .await,
        StatusReply::Manifest(_)
    ));
    let cold = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 2,
    };
    f.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(),
        expected: Some(old),
        next: cold,
    })
    .await;
    assert_eq!(
        f.apply(reserve(cold, request_id, digest)).await,
        StatusReply::Identity(IdentityLookup::Abandoned(reservation))
    );
    let failure = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "fenced preparation",
        Outcome::NotApplied,
    ))
    .unwrap();
    assert!(matches!(
        f.apply(StatusCommand::Abort {
            authority: cold,
            txn,
            reason: AbortReason::AbandonedByFence,
            outcome: failure
        })
        .await,
        StatusReply::Decided(DecisionStatement {
            decision: TxnDecision::Abort {
                reason: AbortReason::AbandonedByFence
            },
            ..
        })
    ));
    let mut f = f.reopen().await;
    assert_eq!(
        f.apply(reserve(cold, request_id, digest)).await,
        StatusReply::Identity(IdentityLookup::Abandoned(reservation))
    );

    let prior_request = RequestBinding {
        run_id: old.run_id,
        request_id: Uuid::new_v4(),
        digest,
        accepted_context: Digest::from_bytes([63; 32]),
    };
    let prior_reservation = IdentityReservation {
        request_id: prior_request.request_id,
        digest,
        original_run: old.run_id,
        reservation_id: Uuid::new_v4(),
        kind: RequestKind::Mutation,
    };
    let outcome = super::super::MutationOutcome::failure(ProtocolError::new(
        ErrorCode::NotFound,
        "original failure",
        Outcome::NotApplied,
    ))
    .unwrap();
    let terminal = TerminalIdentity::Failed {
        reservation: prior_reservation,
        request: prior_request,
        outcome,
        recorded_at: AppliedPosition {
            term: 1,
            node_id: 1,
            index: 1,
        },
    };
    assert_eq!(
        lookup(
            cold,
            IdentityRecord {
                restore_origin: None,
                reserved_at: Some(AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0
                }),
                reservation: prior_reservation,
                terminal: Some(terminal.clone()),
                rejection: None,
                retirement: None,
            }
        ),
        IdentityLookup::Terminal(terminal)
    );
    let manifest = ManifestRef {
        status_group_id: f.machine.store().identity().group_id,
        digest,
        participant_count: 1,
    };
    let commit = TerminalIdentity::Committed {
        reservation: prior_reservation,
        decision: DecisionStatement {
            txn: TxnId {
                request_id: prior_request.request_id,
                attempt: 1,
            },
            request: prior_request,
            manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 2,
            },
            decision: TxnDecision::Commit {
                commit_ts: Timestamp::new(3).unwrap(),
            },
        },
        outcome: super::super::MutationOutcome::from_success(
            &crate::oltp::mvcc::CanonicalJson::from_serializable(
                f.pool.resources(),
                &serde_json::Value::Null,
                1024,
            )
            .unwrap(),
        )
        .unwrap(),
    };
    assert_eq!(
        lookup(
            cold,
            IdentityRecord {
                restore_origin: None,
                reserved_at: Some(AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0
                }),
                reservation: prior_reservation,
                terminal: Some(commit.clone()),
                rejection: None,
                retirement: None,
            }
        ),
        IdentityLookup::Terminal(commit)
    );
}

#[test]
fn o19_restored_reserved_identity_never_reenters_reused_run() {
    let run_id = Uuid::new_v4();
    let reservation = IdentityReservation {
        request_id: Uuid::new_v4(),
        digest: Digest::from_bytes([23; 32]),
        original_run: run_id,
        reservation_id: Uuid::new_v4(),
        kind: RequestKind::Mutation,
    };
    let record = IdentityRecord {
        reservation,
        reserved_at: None,
        terminal: None,
        rejection: None,
        retirement: None,
        restore_origin: Some(RestoredHistoryOrigin {
            restore_id: Uuid::new_v4(),
            source_cluster: Uuid::new_v4(),
            source_archive: Uuid::new_v4(),
            source_root: Digest::from_bytes([41; 32]),
            through_cut: CommitTs::new(7).unwrap(),
        }),
    };
    assert!(
        matches!(lookup(Authority{run_id,owner_term:1},record),IdentityLookup::Abandoned(old) if old==reservation)
    );
}

#[tokio::test]
async fn o13_o14_q06_retirement_certificate_survives_retry_reopen_and_ready_change() {
    use crate::oltp_replication::{
        authority::proof::{AuthenticatedAck, TerminalVerifier},
        proof::ReadySetHasher,
    };
    let mut fixture = Fixture::new().await;
    let authority = Authority { run_id: Uuid::new_v4(), owner_term: 1 };
    fixture.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(), expected: None, next: authority,
    }).await;
    let workers: Vec<_> = (1..=3u128).map(|id| WorkerIdentity {
        worker_id: Uuid::from_u128(id),
        incarnation: Uuid::new_v4(),
        certificate_sha256: Digest::from_bytes([7; 32]),
        enrollment_revision: 1,
    }).collect();
    let mut ready_set = ReadySetHasher::default();
    for worker in &workers {
        ready_set.push(*worker).unwrap();
    }
    let ready = ReadyMembership {
        view: AuthorityView { run_id: authority.run_id, owner_term: 1, membership_revision: 8 },
        ready_root: ready_set.finish(),
        ready_count: 3,
    };
    let digest = Digest::from_bytes([13; 32]);
    let request_id = Uuid::new_v4();
    let reservation_id = Uuid::new_v4();
    let request = RequestBinding {
        run_id: authority.run_id, request_id, digest, accepted_context: Digest::from_bytes([19; 32]),
    };
    let outcome = MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict, "deterministic terminal mutation", Outcome::NotApplied,
    )).unwrap();
    let original = TerminalCopyStatement {
        request, ready, terminal_version: 5,
        response_hash: outcome.response_hash(request_id).unwrap(),
        terminal_digest: Some(Digest::from_bytes([17; 32])),
        pin_identity: PinIdentity { run_id: authority.run_id, request_id, digest, nonce: reservation_id },
        storage_pin: StoragePin { pin_id: Uuid::new_v4(), read_ts: Timestamp::new(11).unwrap(), allocator_epoch: 1 },
    };
    let mut verifier = TerminalVerifier::begin(original.clone(), ready).unwrap();
    let digest_for_ack = statement_digest(&original).unwrap();
    for worker in workers.iter().take(2) {
        verifier.accept(*worker, &AuthenticatedAck::from_transport(*worker, digest_for_ack, Uuid::new_v4())).unwrap();
    }
    assert!(verifier.finish(ready).is_err(), "quorum alone cannot certify retirement");
    let mut verifier = TerminalVerifier::begin(original.clone(), ready).unwrap();
    for worker in workers {
        verifier.accept(worker, &AuthenticatedAck::from_transport(worker, digest_for_ack, Uuid::new_v4())).unwrap();
    }
    let command = StatusCommand::RetireTerminal {
        authority, proof: verifier.finish(ready).unwrap().into_sealed(),
        timestamp_position: AppliedPosition { term: 2, node_id: 1, index: 11 },
    };
    fixture.apply(StatusCommand::ReserveIdentity {
        authority, request_id, digest, kind: RequestKind::Mutation, reservation_id,
    }).await;
    assert_eq!(fixture.apply(command.clone()).await, StatusReply::Rejected(StatusFault::TerminalConflict));
    let terminal = fixture.apply(StatusCommand::TerminalFailure { authority, request, outcome }).await;
    let first = fixture.apply(command.clone()).await;
    let StatusReply::Retired(ref certificate) = first else { panic!("retirement certificate") };
    let mut newer = original.clone();
    newer.ready.view.membership_revision += 1;
    newer.terminal_version += 1;
    assert!(certificate.matches(&newer));
    assert!(certificate.matches(&original));
    newer.terminal_digest = Some(Digest::from_bytes([21; 32]));
    assert!(!certificate.matches(&newer));
    let mut changed = original.clone();
    changed.terminal_version += 1;
    assert!(!certificate.matches(&changed));

    // The first reply can be lost. Reopening keeps the original applied
    // certificate and exact failure, with only non-executable identity data.
    let mut fixture = fixture.reopen().await;
    assert_eq!(fixture.apply(command.clone()).await, first);
    let stored = fixture.machine.store().read_metadata(BUDGET, move |view| {
        let record: IdentityRecord = read(view, &key(IDENTITIES, request_id))?.unwrap();
        view.encode(&record)
    }).await.unwrap();
    let record: IdentityRecord = serde_json::from_slice(stored.as_slice()).unwrap();
    assert_eq!(record.retirement.as_ref(), Some(certificate));
    let encoded = std::str::from_utf8(stored.as_slice()).unwrap();
    for forbidden in ["operation", "payload", "authorization", "token", "queue_order", "sequence"] {
        assert!(!encoded.contains(&format!("\"{forbidden}\"")), "{encoded}");
    }
    let cold = Authority { run_id: Uuid::new_v4(), owner_term: 2 };
    fixture.apply(StatusCommand::Fence {
        transition_id: Uuid::new_v4(), expected: Some(authority), next: cold,
    }).await;
    assert_eq!(fixture.apply(command).await, StatusReply::Rejected(StatusFault::StaleAuthority));
    assert_eq!(fixture.apply(reserve(cold, request_id, digest)).await, terminal);
}
