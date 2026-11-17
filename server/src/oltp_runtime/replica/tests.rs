use super::*;
use crate::{config::WorkerResources, oltp::pool::SharedResourceConfig};

struct Fixture {
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    members: Vec<WorkerIdentity>,
    authority: QueueAuthority,
}
impl Fixture {
    fn new(count: u128) -> Self {
        let base = (Uuid::new_v4().as_u128() >> 64) << 64;
        let members: Vec<_> = (1..=count)
            .map(|id| WorkerIdentity {
                worker_id: Uuid::from_u128(base + id),
                incarnation: Uuid::new_v4(),
                certificate_sha256: Digest::from_bytes([id as u8; 32]),
                enrollment_revision: 1,
            })
            .collect();
        let mut root = ReadySetHasher::default();
        for member in &members {
            root.push(*member).unwrap();
        }
        Self {
            shared: WorkerSharedResources::new(
                81,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap(),
            scope: QueueScope {
                cluster_id: Uuid::new_v4(),
                database: Text::try_from("default").unwrap(),
            },
            authority: QueueAuthority {
                ready: ReadyMembership {
                    view: AuthorityView {
                        run_id: Uuid::new_v4(),
                        owner_term: 1,
                        membership_revision: 1,
                    },
                    ready_root: root.finish(),
                    ready_count: root.count(),
                },
                owner: members[0],
            },
            members,
        }
    }
    fn replica(&self, index: usize) -> WorkerReplica {
        let shared = if index == 0 {
            self.shared.clone()
        } else {
            WorkerSharedResources::new(
                81 + index as u64,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap()
        };
        WorkerReplica::new(
            shared,
            self.scope.clone(),
            self.members[index],
            self.authority,
            self.members.clone(),
            QueueLimits {
                max_pending_requests: 20,
                max_queue_bytes_per_replica: 4 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 100,
            },
        )
        .unwrap()
    }
    fn body(&self, id: Uuid) -> Vec<u8> {
        format!(" {{\"request_id\":\"{id}\", \"operation\":\"table.list\",\"payload\":{{}}}}\n")
            .into_bytes()
    }
    fn context(&self, id: Uuid, body: &[u8]) -> AcceptedContext {
        AcceptedContext {
            authorization: OriginalAuthorization {
                database: self.scope.database.clone(),
                principal: Digest::from_bytes([6; 32]),
                policy_revision: 4,
                ingress_id: Uuid::new_v4(),
                ingress_incarnation: Uuid::new_v4(),
                credential_expires_at_ms: Some(50),
            },
            read: Some(QueueReadToken {
                identity: PinIdentity {
                    run_id: self.authority.ready.view.run_id,
                    request_id: id,
                    digest: hash(body),
                    nonce: Uuid::new_v4(),
                },
                storage_pin: StoragePin {
                    pin_id: Uuid::new_v4(),
                    read_ts: Timestamp::new(10).unwrap(),
                    allocator_epoch: 1,
                },
                catalog_root: Digest::from_bytes([7; 32]),
                schema_epoch: 8,
                routing_epoch: 9,
            }),
        }
    }
}

fn copy(
    owner: &mut WorkerReplica,
    replicas: &mut [WorkerReplica],
    frame: &RecordFrame,
) -> Option<RecordFrame> {
    let authority = owner.authority();
    let ack = owner.install(authority, authority.owner, frame).unwrap();
    let mut next = owner.acknowledge(authority, ack).unwrap();
    for replica in replicas {
        let ack = replica.install(authority, authority.owner, frame).unwrap();
        if let Some(frame) = owner.acknowledge(authority, ack).unwrap() {
            next = Some(frame);
        }
    }
    next
}

#[test]
fn q03_payload_and_acceptance_evidence_are_separate_all_ready_barriers() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = Uuid::new_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let payload = owner
        .propose(fixture.authority, body.clone(), context.clone(), 1)
        .unwrap();
    let ack = owner
        .install(fixture.authority, fixture.authority.owner, &payload)
        .unwrap();
    assert!(owner.acknowledge(fixture.authority, ack).unwrap().is_none());
    let ack = replicas[0]
        .install(fixture.authority, fixture.authority.owner, &payload)
        .unwrap();
    assert!(owner.acknowledge(fixture.authority, ack).unwrap().is_none());
    assert!(!owner.copies_complete(id));
    assert!(!replicas[0].record(id).unwrap().accepted);
    // A lost coordinator at this boundary cannot fabricate a 202.
    let ack = replicas[1]
        .install(fixture.authority, fixture.authority.owner, &payload)
        .unwrap();
    let accepted = owner.acknowledge(fixture.authority, ack).unwrap().unwrap();
    assert!(!owner.copies_complete(id));
    let ack = owner
        .install(fixture.authority, fixture.authority.owner, &accepted)
        .unwrap();
    owner.acknowledge(fixture.authority, ack).unwrap();
    let ack = replicas[0]
        .install(fixture.authority, fixture.authority.owner, &accepted)
        .unwrap();
    owner.acknowledge(fixture.authority, ack).unwrap();
    assert!(!owner.copies_complete(id));
    assert!(replicas[0].record(id).unwrap().accepted);
    assert!(!replicas[1].record(id).unwrap().accepted);
    let ack = replicas[1]
        .install(fixture.authority, fixture.authority.owner, &accepted)
        .unwrap();
    assert!(owner.acknowledge(fixture.authority, ack).unwrap().is_none());
    assert!(owner.copies_complete(id));
    for replica in &replicas {
        assert_eq!(replica.record(id).unwrap().body, body);
        let bytes = replica.record(id).unwrap().pinned_catalog.as_ref().unwrap();
        assert_eq!(
            serde_json::from_slice::<AcceptedContext>(bytes).unwrap(),
            context
        );
    }
    drop(payload);
    drop(accepted);
    assert_eq!(fixture.shared.control_bytes_used(), 0);
}

#[test]
fn q06_t04_receipt_race_reuses_terminal_version_and_retains_pin_until_every_copy() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = Uuid::new_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let payload = owner
        .propose(fixture.authority, body.clone(), context.clone(), 1)
        .unwrap();
    let accepted = copy(&mut owner, &mut replicas, &payload).unwrap();
    copy(&mut owner, &mut replicas, &accepted);
    let running = owner.start(fixture.authority, id).unwrap();
    copy(&mut owner, &mut replicas, &running);
    let response = format!(r#"{{"request_id":"{id}","data":["exact"]}}"#).into_bytes();
    let terminal = owner
        .finish(fixture.authority, id, 200, response.clone(), 2)
        .unwrap();
    let version = terminal.decode_header().unwrap().version;
    let local_ack = owner
        .install(fixture.authority, fixture.authority.owner, &terminal)
        .unwrap();
    owner.acknowledge(fixture.authority, local_ack).unwrap();
    assert!(owner.terminal_statement(id).is_err());
    // The local result exists, but one surviving Running replica still needs
    // its original pin. Expiration cannot physically discard that evidence.
    assert_eq!(
        owner.record(id).unwrap().pinned_catalog,
        replicas[1].record(id).unwrap().pinned_catalog
    );
    let same = owner
        .finish(fixture.authority, id, 200, response.clone(), 999)
        .unwrap();
    assert_eq!(same.decode_header().unwrap().version, version);
    assert_eq!(owner.record(id).unwrap().completed_at_ms, Some(2));
    for replica in &mut replicas {
        let ack = replica
            .install(fixture.authority, fixture.authority.owner, &same)
            .unwrap();
        owner.acknowledge(fixture.authority, ack).unwrap();
    }
    let proof_statement = owner.terminal_statement(id).unwrap();
    assert_eq!(proof_statement.terminal_version, version);
    assert_eq!(
        proof_statement.storage_pin,
        context.read.unwrap().storage_pin
    );
    assert_eq!(proof_statement.response_hash, hash(&response));
    assert!(matches!(
        owner.finish(
            fixture.authority,
            id,
            200,
            format!(r#"{{"request_id":"{id}","data":[]}}"#).into_bytes(),
            1000
        ),
        Err(QueueError::Conflict)
    ));
    assert!(matches!(
        owner
            .queue
            .lookup(fixture.authority.ready.view.run_id, id, 1000),
        Err(QueueError::NotFound)
    ));
    assert!(owner.record(id).unwrap().pinned_catalog.is_some());
    assert_eq!(
        owner.record(id).unwrap().response_body.as_ref(),
        Some(&response)
    );
}

#[test]
fn r01_r05_takeover_keeps_original_context_and_rejects_stale_owner_or_new_run() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = Uuid::new_v4();
    let body = fixture.body(id);
    let payload = owner
        .propose(
            fixture.authority,
            body.clone(),
            fixture.context(id, &body),
            1,
        )
        .unwrap();
    let accepted = copy(&mut owner, &mut replicas, &payload).unwrap();
    copy(&mut owner, &mut replicas, &accepted);
    let running = owner.start(fixture.authority, id).unwrap();
    copy(&mut owner, &mut replicas, &running);
    let original = replicas[0].record(id).unwrap().pinned_catalog.clone();
    let members = &fixture.members[1..];
    let mut root = ReadySetHasher::default();
    for member in members {
        root.push(*member).unwrap();
    }
    let mut next = fixture.authority;
    next.owner = members[0];
    next.ready.view.owner_term += 1;
    next.ready.view.membership_revision += 1;
    next.ready.ready_root = root.finish();
    next.ready.ready_count = root.count();
    replicas[0]
        .fenced_takeover(fixture.authority, next, members.iter().copied())
        .unwrap();
    assert_eq!(replicas[0].record(id).unwrap().pinned_catalog, original);
    assert_eq!(
        replicas[0].record(id).unwrap().state,
        RequestState::Reconciling
    );
    assert!(matches!(
        replicas[0].install(fixture.authority, fixture.authority.owner, &running),
        Err(QueueError::Stale)
    ));
    let mut cold = next;
    cold.ready.view.run_id = Uuid::new_v4();
    cold.ready.view.owner_term += 1;
    assert!(matches!(
        replicas[0].fenced_takeover(next, cold, members.iter().copied()),
        Err(QueueError::Stale)
    ));
    assert_eq!(replicas[0].pending(), 1);
}

#[test]
fn u02_invalid_auth_or_pin_installs_no_identity_and_wire_keeps_exact_raw_bytes() {
    let fixture = Fixture::new(2);
    let mut owner = fixture.replica(0);
    let mut peer = fixture.replica(1);
    let id = Uuid::new_v4();
    let body = fixture.body(id);
    let mut context = fixture.context(id, &body);
    context.read.as_mut().unwrap().identity.digest = Digest::from_bytes([0; 32]);
    assert!(
        owner
            .propose(fixture.authority, body.clone(), context, 1)
            .is_err()
    );
    assert_eq!(owner.pending(), 0);
    let frame = owner
        .propose(
            fixture.authority,
            body.clone(),
            fixture.context(id, &body),
            1,
        )
        .unwrap();
    assert_eq!(frame.sections[0].as_ref().unwrap().as_ref(), body);
    assert!(frame.header.len() < MAX_HEADER_BYTES);
    let mut stale = fixture.members[0];
    stale.incarnation = Uuid::new_v4();
    assert!(matches!(
        peer.install(fixture.authority, stale, &frame),
        Err(QueueError::Stale)
    ));
    assert_eq!(peer.pending(), 0);
    let mut bad = frame.clone();
    bad.sections[0] = Some(fixture.shared.copy_bytes(b"{}").unwrap());
    assert!(
        peer.install(fixture.authority, fixture.authority.owner, &bad)
            .is_err()
    );
    assert_eq!(peer.pending(), 0);
    // Authorized credential rotation attaches to the accepted snapshot.
    let mut rotated = fixture.context(id, &body);
    rotated.authorization.principal = Digest::from_bytes([9; 32]);
    let duplicate = owner.propose(fixture.authority, body, rotated, 1).unwrap();
    assert_eq!(
        duplicate.sections[1].as_ref().unwrap().as_ref(),
        frame.sections[1].as_ref().unwrap().as_ref()
    );
}

fn joiner(fixture: &Fixture) -> (WorkerReplica, JoinTicket) {
    let local = WorkerIdentity {
        worker_id: Uuid::from_u128(fixture.members.last().unwrap().worker_id.as_u128() + 1),
        incarnation: Uuid::new_v4(),
        certificate_sha256: Digest::from_bytes([3; 32]),
        enrollment_revision: 1,
    };
    let ticket = JoinTicket {
        ticket_id: Uuid::new_v4(),
        target: local,
        source_ready: fixture.authority.ready,
    };
    (
        WorkerReplica::new_joining(
            WorkerSharedResources::new(
                83,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap(),
            fixture.scope.clone(),
            local,
            fixture.authority,
            fixture.members.clone(),
            QueueLimits {
                max_pending_requests: 20,
                max_queue_bytes_per_replica: 4 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 100,
            },
            ticket,
        )
        .unwrap(),
        ticket,
    )
}

#[test]
fn r03_q03_join_empty_single_multiple_and_exhausted_pages_have_correct_done_flag() {
    for count in [0, 1, 3] {
        let fixture = Fixture::new(2);
        let mut owner = fixture.replica(0);
        let mut peers = [fixture.replica(1)];
        for index in 0..count {
            let id = Uuid::from_u128(100 + index);
            let body = fixture.body(id);
            let payload = owner
                .propose(
                    fixture.authority,
                    body.clone(),
                    fixture.context(id, &body),
                    1,
                )
                .unwrap();
            let accepted = copy(&mut owner, &mut peers, &payload).unwrap();
            copy(&mut owner, &mut peers, &accepted);
        }
        let (mut target, ticket) = joiner(&fixture);
        let snapshot = owner.begin_join(fixture.authority, ticket).unwrap();
        assert!(target.join_statement(fixture.authority).is_err());
        let mut cursor = None;
        let mut received = 0;
        loop {
            let page = owner
                .join_page(fixture.authority, snapshot, JoinPhase::Snapshot, cursor)
                .unwrap();
            received += usize::from(page.record.is_some());
            assert_eq!(page.done, received == count as usize);
            target
                .install_join_page(fixture.authority, fixture.authority.owner, &page)
                .unwrap();
            cursor = page.next_sequence;
            if page.done {
                break;
            }
        }
        let exhausted = owner
            .join_page(fixture.authority, snapshot, JoinPhase::Snapshot, cursor)
            .unwrap();
        assert!(exhausted.done && exhausted.record.is_none());
        assert_eq!(exhausted.next_sequence, cursor);
        assert_eq!(target.pending(), count as usize);
        assert!(target.join_statement(fixture.authority).is_err());
        owner.seal_join(fixture.authority, snapshot).unwrap();
        let delta = owner
            .join_page(fixture.authority, snapshot, JoinPhase::Delta, None)
            .unwrap();
        assert!(delta.done && delta.record.is_none());
        target
            .install_join_page(fixture.authority, fixture.authority.owner, &delta)
            .unwrap();
        assert_eq!(
            target.join_statement(fixture.authority).unwrap(),
            owner.join_statement(fixture.authority).unwrap()
        );
        // Copy evidence has not published a new ready revision.
        assert_eq!(target.authority(), fixture.authority);
        assert_eq!(target.authority().ready.ready_count, 2);
    }
}

#[test]
fn r03_snapshot_delta_captures_updates_behind_cursor_before_ready_publication() {
    let fixture = Fixture::new(2);
    let mut owner = fixture.replica(0);
    let mut peers = [fixture.replica(1)];
    let id = Uuid::from_u128(100);
    let body = fixture.body(id);
    let payload = owner
        .propose(
            fixture.authority,
            body.clone(),
            fixture.context(id, &body),
            1,
        )
        .unwrap();
    let accepted = copy(&mut owner, &mut peers, &payload).unwrap();
    copy(&mut owner, &mut peers, &accepted);
    let (mut target, ticket) = joiner(&fixture);
    let snapshot = owner.begin_join(fixture.authority, ticket).unwrap();
    let page = owner
        .join_page(fixture.authority, snapshot, JoinPhase::Snapshot, None)
        .unwrap();
    assert!(page.done);
    target
        .install_join_page(fixture.authority, fixture.authority.owner, &page)
        .unwrap();
    let original_version = target.record(id).unwrap().version;
    let running = owner.start(fixture.authority, id).unwrap();
    copy(&mut owner, &mut peers, &running);
    let new_id = Uuid::from_u128(101);
    let new_body = fixture.body(new_id);
    let payload = owner
        .propose(
            fixture.authority,
            new_body.clone(),
            fixture.context(new_id, &new_body),
            1,
        )
        .unwrap();
    let accepted = copy(&mut owner, &mut peers, &payload).unwrap();
    copy(&mut owner, &mut peers, &accepted);
    owner.seal_join(fixture.authority, snapshot).unwrap();
    assert!(
        owner
            .finish(
                fixture.authority,
                id,
                200,
                format!(r#"{{"request_id":"{id}","data":[]}}"#).into_bytes(),
                2
            )
            .is_err()
    );
    let first = owner
        .join_page(fixture.authority, snapshot, JoinPhase::Delta, None)
        .unwrap();
    assert!(!first.done);
    target
        .install_join_page(fixture.authority, fixture.authority.owner, &first)
        .unwrap();
    let second = owner
        .join_page(
            fixture.authority,
            snapshot,
            JoinPhase::Delta,
            first.next_sequence,
        )
        .unwrap();
    assert!(second.done);
    target
        .install_join_page(fixture.authority, fixture.authority.owner, &second)
        .unwrap();
    assert!(target.record(id).unwrap().version > original_version);
    assert_eq!(target.record(id).unwrap().state, RequestState::Running);
    assert_eq!(target.pending(), 2);
    assert_eq!(
        owner.join_statement(fixture.authority).unwrap(),
        target.join_statement(fixture.authority).unwrap()
    );
}

#[test]
fn q02_q06_retained_result_capacity_never_discards_running_body_or_snapshot_pin() {
    let fixture = Fixture::new(1);
    let mut owner = WorkerReplica::new(
        fixture.shared.clone(),
        fixture.scope.clone(),
        fixture.members[0],
        fixture.authority,
        fixture.members.clone(),
        QueueLimits {
            max_pending_requests: 1,
            max_queue_bytes_per_replica: 1 << 20,
            max_retained_result_bytes_per_worker: 8,
            result_retention_ms: 1,
        },
    )
    .unwrap();
    let id = Uuid::new_v4();
    let body = fixture.body(id);
    let payload = owner
        .propose(
            fixture.authority,
            body.clone(),
            fixture.context(id, &body),
            1,
        )
        .unwrap();
    let accepted = copy(&mut owner, &mut [], &payload).unwrap();
    copy(&mut owner, &mut [], &accepted);
    let running = owner.start(fixture.authority, id).unwrap();
    copy(&mut owner, &mut [], &running);
    let context = owner.record(id).unwrap().pinned_catalog.clone();
    let response = format!(r#"{{"request_id":"{id}","data":[]}}"#).into_bytes();
    assert!(matches!(
        owner.finish(fixture.authority, id, 200, response, 999),
        Err(QueueError::Overloaded)
    ));
    let record = owner.record(id).unwrap();
    assert_eq!(record.state, RequestState::Running);
    assert_eq!(record.body, body);
    assert_eq!(record.pinned_catalog, context);
    assert!(record.response_body.is_none());
    assert!(!record.result_expired);
    assert_eq!(owner.pending(), 1);
}
