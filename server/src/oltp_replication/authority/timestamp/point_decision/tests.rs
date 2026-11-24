use super::super::tests::{Fixture, ready, worker};
use super::*;
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson,
            store::{
                KeySpan, ParticipantCommand, ParticipantHandle, ParticipantMachine,
                ParticipantReply, PointEvidenceRead,
                timestamp_resolution::{SealedTimestampPointDecision, TimestampPointResolution},
            },
        },
        pool::OltpPool,
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, Consensus, ConsensusFuture, ReadBarrier},
            control::{ScopedCatalogRead, ScopedQueuePlacement},
            status::{
                IdentityLookup, RequestKind, ScopedAdmission, StatusCommand, StatusHandle,
                StatusMachine, StatusReply, identity::ScopedStatusEpoch,
            },
        },
        multiplex::{ChargedSections, MAX_SECTIONS},
        owned::serialized_digest,
    },
    oltp_runtime::replica::OriginalAuthorization,
};

// Apply real log entries locally and model only the Raft read barrier.
struct AppliedBarrier(AppliedPosition);

impl<C: OwnedPayload, R> Consensus<C, R> for AppliedBarrier {
    fn propose(&self, _: OwnedCommand<C>) -> ConsensusFuture<'_, Applied<R>> {
        panic!("the read fixture cannot propose log entries");
    }

    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async { Ok(ReadBarrier { applied: self.0 }) })
    }
}

fn position(node_id: u64, index: u64) -> AppliedPosition {
    AppliedPosition {
        term: 1,
        node_id,
        index,
    }
}

fn entry<C: OwnedPayload>(
    pool: &OltpPool,
    position: AppliedPosition,
    command: C,
) -> (ApplyMeta, OwnedCommand<C>) {
    let command = OwnedCommand::new_owned(command, pool.resources(), ByteClass::Progress).unwrap();
    let meta = ApplyMeta {
        position,
        command_digest: serialized_digest(&command).unwrap(),
        membership: None,
    };
    (meta, command)
}

async fn apply_status(
    pool: &OltpPool,
    status: &StatusMachine,
    index: u64,
    command: StatusCommand,
) -> StatusReply {
    let (meta, command) = entry(pool, position(2, index), command);
    let raw = status.apply(meta, command).await.unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

async fn apply_data(
    pool: &OltpPool,
    data: &ParticipantMachine,
    index: u64,
    command: ParticipantCommand,
) -> ParticipantReply {
    let (meta, command) = entry(pool, position(3, index), command);
    let raw = data.apply(meta, command).await.unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

async fn read_completion(
    pool: &OltpPool,
    data: &ParticipantMachine,
    index: u64,
    authority: Authority,
    locator: &TimestampDecisionLocator,
) -> TimestampPointResolution {
    let handle = ParticipantHandle::new(data.clone(), Arc::new(AppliedBarrier(position(3, index))));
    let input = ChargedSections::for_test(
        pool.resources(),
        ByteClass::Progress,
        [const { None }; MAX_SECTIONS],
        8192,
    )
    .unwrap()
    .retain_decoded(locator.clone());
    let raw = handle
        .read_timestamp_point(
            input,
            |locator| locator,
            authority,
            PointEvidenceRead::Completion,
        )
        .await
        .unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

/// The installed Status home has no Data preparation when its run ends.
#[tokio::test]
async fn o10_o15_r04_r05_native_point_cold_projection_keeps_first_positions() {
    let mut fixture = Fixture::new().await;
    let prior = ready(worker());
    fixture.initialize(prior).await;
    let old_authority = prior.view.authority();
    let timestamps_group = fixture.machine.store().identity().clone();
    let status_group = GroupIdentity {
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Status { bucket: 0 },
        ..timestamps_group.clone()
    };
    let table_id = nabedra_rt::ids::uuid_v4();
    let data_group = GroupIdentity {
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Data {
            table_id,
            shard_id: nabedra_rt::ids::uuid_v4(),
        },
        ..timestamps_group.clone()
    };
    let status =
        StatusMachine::new(fixture.pool.group(status_group.clone()).await.unwrap()).unwrap();
    let data =
        ParticipantMachine::new(fixture.pool.group(data_group.clone()).await.unwrap()).unwrap();
    let mut members = [worker(), worker(), worker()];
    members.sort_by_key(|member| member.worker_id);
    let placement = ScopedQueuePlacement {
        group: data_group.clone(),
        data_generation: nabedra_rt::ids::uuid_v4(),
        queue_generation: nabedra_rt::ids::uuid_v4(),
        view: prior.view,
        owner: members[0],
        members,
        unavailable_holder: None,
    };
    let epoch = ScopedTimestampEpoch::from_placement(&placement).unwrap();
    let status_epoch = ScopedStatusEpoch::from_placement(&placement);
    let scoped_status = |command| StatusCommand::Scoped {
        epoch: status_epoch,
        command: Box::new(command),
    };
    let initial_fence = nabedra_rt::ids::uuid_v4();
    assert!(matches!(
        apply_status(
            &fixture.pool,
            &status,
            1,
            StatusCommand::Fence {
                transition_id: initial_fence,
                expected: None,
                next: old_authority,
            }
        )
        .await,
        StatusReply::Fenced(_)
    ));
    let catalog_group_id = nabedra_rt::ids::uuid_v4();
    let data_fence = |transition_id, expected, next| ParticipantCommand::Fence {
        transition_id,
        expected,
        next,
        schema_epoch: 1,
        routing_epoch: 1,
        catalog_group_id,
        span: KeySpan {
            start: None,
            end: None,
        },
    };
    assert!(matches!(
        apply_data(
            &fixture.pool,
            &data,
            1,
            data_fence(initial_fence, None, old_authority)
        )
        .await,
        ParticipantReply::Fenced(_)
    ));

    let request = RequestBinding {
        run_id: old_authority.run_id,
        request_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([1; 32]),
        accepted_context: Digest::from_bytes([2; 32]),
    };
    let identity = PinIdentity {
        run_id: request.run_id,
        request_id: request.request_id,
        digest: request.digest,
        nonce: nabedra_rt::ids::uuid_v4(),
    };
    let StatusReply::Identity(IdentityLookup::Reserved(reservation)) = apply_status(
        &fixture.pool,
        &status,
        2,
        scoped_status(StatusCommand::ReserveIdentity {
            authority: old_authority,
            request_id: request.request_id,
            digest: request.digest,
            kind: RequestKind::Mutation,
            reservation_id: identity.nonce,
            scoped: Some(ScopedAdmission {
                protocol_version: ScopedAdmission::VERSION,
                queue_group_id: data_group.group_id,
                queue_generation: placement.queue_generation,
                status_group_id: status_group.group_id,
            }),
        }),
    )
    .await
    else {
        panic!("expected scoped Status reservation")
    };
    let origin = OriginalAuthorization {
        database: timestamps_group.database.clone(),
        principal: Digest::from_bytes([3; 32]),
        policy_revision: 1,
        ingress_id: nabedra_rt::ids::uuid_v4(),
        ingress_incarnation: nabedra_rt::ids::uuid_v4(),
        credential_expires_at_ms: None,
    };
    let TimestampReply::ScopedSnapshot {
        pin,
        origin: original,
    } = fixture
        .apply(TimestampCommand::ScopedSnapshot(
            scoped_admission::Command::Begin {
                epoch,
                view: prior.view,
                identity: identity.clone(),
                reservation,
                pin_id: nabedra_rt::ids::uuid_v4(),
                origin: origin.clone(),
            },
        ))
        .await
    else {
        panic!("expected native scoped pin")
    };
    assert_eq!(original, origin);
    assert_eq!(
        fixture
            .apply(TimestampCommand::ScopedSnapshot(
                scoped_admission::Command::Bind {
                    epoch,
                    view: prior.view,
                    identity: identity.clone(),
                    pin,
                    request,
                }
            ))
            .await,
        TimestampReply::ContextBound(pin)
    );

    let txn = TxnId {
        request_id: request.request_id,
        attempt: 1,
    };
    let intended = MutationOutcome::from_success(
        &CanonicalJson::from_serializable(fixture.pool.resources(), &true, 1024).unwrap(),
    )
    .unwrap();
    let point = TimestampPointManifest {
        binding: TimestampPointBinding {
            status_group: status_group.clone(),
            timestamps_group: timestamps_group.clone(),
            participant: ParticipantPlan {
                group: data_group.clone(),
                schema_epoch: 1,
                routing_epoch: 1,
                read_ts: pin.read_ts,
                write_root: Digest::from_bytes([4; 32]),
                write_count: 1,
                page_count: 1,
            },
            created_authority: old_authority,
            txn,
            request,
            pin_identity: identity.clone(),
            storage_pin: pin,
            result_digest: intended.digest().unwrap(),
            reservation,
            original_epoch: epoch,
            catalog_root: Digest::from_bytes([5; 32]),
            original_scope: ScopedCatalogRead {
                queue_group_id: data_group.group_id,
                data_generation: placement.data_generation,
                queue_generation: placement.queue_generation,
                queue_view: prior.view,
                request,
                table_id,
                read_ts: pin.read_ts,
            },
        },
        outcome: intended.clone(),
    };
    let manifest = point.manifest().unwrap();
    let locator = TimestampDecisionLocator {
        binding: point.binding.clone(),
        manifest,
    };
    assert!(
        matches!(apply_status(&fixture.pool, &status, 3, scoped_status(StatusCommand::InstallTimestampPoint {
        authority: old_authority, point: Box::new(point),
    })).await, StatusReply::ManifestSealed(stored) if stored == manifest)
    );

    // A syntactically valid late proposal exercises Timestamp apply fencing.
    // No Data seal exists for this installed home, and the proposal must fail.
    let delayed_commit = TimestampCommand::DecidePoint(Box::new(PointCommand {
        authority: old_authority,
        locator: locator.clone(),
        intended: intended.clone(),
        intent: PointIntent::Commit {
            epoch,
            seal: PreparedParticipant {
                txn,
                manifest,
                participant: locator.binding.participant.clone(),
                authority: old_authority,
                sealed_at: position(3, 2),
                result_digest: locator.binding.result_digest,
            },
        },
    }));
    let next_placement = ScopedQueuePlacement {
        view: AuthorityView {
            owner_term: prior.view.owner_term + 1,
            ..prior.view
        },
        data_generation: nabedra_rt::ids::uuid_v4(),
        owner: members[1],
        ..placement
    };
    let next_epoch = ScopedTimestampEpoch::from_placement(&next_placement).unwrap();
    assert!(matches!(fixture.apply(TimestampCommand::FenceScopedQueue {
        transition_id: nabedra_rt::ids::uuid_v4(), previous: epoch, next: next_epoch,
    }).await, TimestampReply::ScopedQueueFenced { epoch, .. } if epoch == next_epoch));
    assert_eq!(
        fixture.apply(delayed_commit.clone()).await,
        TimestampReply::Rejected(Fault::StaleReadyView)
    );

    let transition_id = nabedra_rt::ids::uuid_v4();
    let ready = ReadyMembership {
        view: AuthorityView {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: prior.view.owner_term + 1,
            ..prior.view
        },
        ready_count: 0,
        ..prior
    };
    let authority = ready.view.authority();
    let fence = TimestampCommand::Fence {
        transition_id,
        expected: Some(old_authority),
        next: authority,
        ready,
    };
    let fence_reply = fixture.apply(fence.clone()).await;
    assert!(matches!(
        fence_reply,
        TimestampReply::Fenced {
            allocator_epoch: 2,
            ..
        }
    ));
    assert_eq!(
        fixture.apply(delayed_commit).await,
        TimestampReply::Rejected(Fault::StaleAuthority)
    );
    assert!(matches!(
        apply_status(
            &fixture.pool,
            &status,
            4,
            StatusCommand::Fence {
                transition_id,
                expected: Some(old_authority),
                next: authority,
            }
        )
        .await,
        StatusReply::Fenced(_)
    ));
    assert!(matches!(
        apply_data(
            &fixture.pool,
            &data,
            2,
            data_fence(transition_id, Some(old_authority), authority)
        )
        .await,
        ParticipantReply::Fenced(_)
    ));
    let status_reader = StatusHandle::new(status.clone(), Arc::new(AppliedBarrier(position(2, 4))));
    let cold_pin = cold::ColdApplicationPin {
        identity,
        reservation,
        request: Some(request),
        pin,
    };
    let settled = status_reader
        .cold_application_pin_settled(authority, cold_pin.clone())
        .await
        .unwrap();
    assert!(!serde_json::from_slice::<bool>(settled.as_slice()).unwrap());
    assert!(
        matches!(status_reader.lookup_identity(request.request_id, Some(request.digest)).await.unwrap(),
        Some(IdentityLookup::TimestampPoint(original)) if original == reservation)
    );
    drop(status_reader);

    let decide = |transition_id| {
        TimestampCommand::DecidePoint(Box::new(PointCommand {
            authority,
            locator: locator.clone(),
            intended: intended.clone(),
            intent: PointIntent::AbandonFenced { transition_id },
        }))
    };
    assert_eq!(
        fixture.apply(decide(nabedra_rt::ids::uuid_v4())).await,
        TimestampReply::Rejected(Fault::InvalidBinding)
    );
    let timestamp_position = position(1, fixture.next);
    let TimestampReply::PointDecided(decision) = fixture.apply(decide(transition_id)).await else {
        panic!("expected authoritative Timestamp abandonment");
    };
    assert_eq!(decision.decided_at(), timestamp_position);
    assert_eq!(decision.locator(), &locator);
    assert_eq!(decision.authority(), authority);
    let abandoned = TxnDecision::Abort {
        reason: AbortReason::AbandonedByFence,
    };
    assert_eq!(decision.decision(), abandoned);
    let error = decision.outcome().result().unwrap_err();
    assert_eq!(
        (error.code, error.outcome, error.message.as_str()),
        (
            ErrorCode::Conflict,
            Outcome::NotApplied,
            "original execution authority was fenced"
        )
    );

    // Encode the replicated Data witness from the actual Timestamp apply result.
    let encoded = serde_json::to_vec(&decision).unwrap();
    let digest = hash(&[b"nabedra/mvcc-timestamp-point-decision/v1\0", &encoded]);
    let proof: SealedTimestampPointDecision =
        serde_json::from_value(serde_json::json!({ "decision": decision, "digest": digest }))
            .unwrap();
    let resolve = || ParticipantCommand::ResolveTimestampPoint {
        authority,
        proof: Box::new(proof.clone()),
    };
    let data_position = position(3, 3);
    let completion_reply = apply_data(&fixture.pool, &data, data_position.index, resolve()).await;
    assert!(
        matches!(&completion_reply, ParticipantReply::TimestampPointResolved { evidence, writes: 0 }
        if evidence.completed_at == data_position && evidence.decision == abandoned && evidence.request == request)
    );
    let completion = read_completion(
        &fixture.pool,
        &data,
        data_position.index,
        authority,
        &locator,
    )
    .await;
    assert_eq!(completion.decision(), decision.as_ref());
    assert_eq!(completion.completed_at(), data_position);
    let project = || StatusCommand::ProjectTimestampPoint {
        authority,
        completion: Box::new(completion.clone()),
    };
    let status_position = position(2, 5);
    let StatusReply::TimestampPointProjected(projection) =
        apply_status(&fixture.pool, &status, status_position.index, project()).await
    else {
        panic!("expected Status projection");
    };
    assert_eq!(projection.projected_at(), status_position);
    assert_eq!(projection.completion(), &completion);
    let status_reader =
        StatusHandle::new(status.clone(), Arc::new(AppliedBarrier(status_position)));
    let settled = status_reader
        .cold_application_pin_settled(authority, cold_pin.clone())
        .await
        .unwrap();
    assert!(serde_json::from_slice::<bool>(settled.as_slice()).unwrap());
    drop(status_reader);
    assert_eq!(
        fixture
            .apply(TimestampCommand::ReleaseColdApplication {
                authority,
                transition_id,
                pin: cold_pin
            })
            .await,
        TimestampReply::Released
    );

    let Fixture {
        machine,
        pool,
        dir,
        next,
    } = fixture;
    let pool_identity = pool.identity().clone();
    let resources = pool.resources().clone();
    drop((machine, status, data, pool));
    let pool = OltpPool::open_development(&dir.0, pool_identity, resources).unwrap();
    let machine = TimestampMachine::new(pool.group(timestamps_group).await.unwrap()).unwrap();
    let status = StatusMachine::new(pool.group(status_group).await.unwrap()).unwrap();
    let data = ParticipantMachine::new(pool.group(data_group).await.unwrap()).unwrap();
    let mut fixture = Fixture {
        machine,
        pool,
        dir,
        next,
    };
    assert_eq!(fixture.apply(fence).await, fence_reply);
    assert_eq!(
        fixture.apply(decide(transition_id)).await,
        TimestampReply::PointDecided(decision.clone())
    );
    let timestamp_reader = TimestampHandle::new(
        fixture.machine.clone(),
        Arc::new(AppliedBarrier(position(1, fixture.next - 1))),
    );
    let raw = timestamp_reader
        .read_timestamp_point_decision(locator.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Option<PointDecision>>(raw.as_slice())
            .unwrap()
            .as_ref(),
        Some(decision.as_ref())
    );
    assert_eq!(
        apply_data(&fixture.pool, &data, 4, resolve()).await,
        completion_reply
    );
    assert_eq!(
        read_completion(&fixture.pool, &data, 4, authority, &locator).await,
        completion
    );
    let StatusReply::TimestampPointProjected(replayed) =
        apply_status(&fixture.pool, &status, 6, project()).await
    else {
        panic!("expected original Status projection");
    };
    assert_eq!(replayed, projection);
    let status_reader = StatusHandle::new(status, Arc::new(AppliedBarrier(position(2, 6))));
    let (persisted, _) = status_reader
        .read_timestamp_point_projection(authority, locator.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&persisted, projection.as_ref());
    assert!(
        matches!(status_reader.lookup_identity(request.request_id, Some(request.digest)).await.unwrap(),
        Some(IdentityLookup::Abandoned(original)) if original == reservation)
    );
    let conflict = status_reader
        .lookup_identity(request.request_id, Some(Digest::from_bytes([9; 32])))
        .await
        .unwrap_err();
    assert_eq!(
        conflict,
        nabedra_protocol::QueueError::Conflict.protocol()
    );
}
