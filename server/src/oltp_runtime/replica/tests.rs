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
        let base = (nabedra_rt::ids::uuid_v4().as_u128() >> 64) << 64;
        let members: Vec<_> = (1..=count)
            .map(|id| WorkerIdentity {
                worker_id: Uuid::from_u128(base + id),
                incarnation: nabedra_rt::ids::uuid_v4(),
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
                cluster_id: nabedra_rt::ids::uuid_v4(),
                database: Text::try_from("default").unwrap(),
            },
            authority: QueueAuthority {
                ready: ReadyMembership {
                    view: AuthorityView {
                        run_id: nabedra_rt::ids::uuid_v4(),
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
                ingress_id: nabedra_rt::ids::uuid_v4(),
                ingress_incarnation: nabedra_rt::ids::uuid_v4(),
                credential_expires_at_ms: Some(50),
            },
            read: Some(QueueReadToken {
                identity: PinIdentity {
                    run_id: self.authority.ready.view.run_id,
                    request_id: id,
                    digest: hash(body),
                    nonce: nabedra_rt::ids::uuid_v4(),
                },
                storage_pin: StoragePin {
                    pin_id: nabedra_rt::ids::uuid_v4(),
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

#[tokio::test(start_paused = true)]
async fn c07_m02_f03_o22_o23_read_grant_copy_retries_only_the_blocked_replica() {
    use crate::oltp_replication::{multiplex::EndpointFuture, service_rpc::ReadScope};
    use crate::oltp_runtime::{
        coordinator::{QueueCoordinator, ReplicaTransport},
        worker_endpoint::AuthorityReader,
    };
    use crate::protocol::{ErrorCode, Outcome, ProtocolError};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    struct Current(Mutex<QueueAuthority>);
    impl AuthorityReader for Current {
        fn current<'a>(&'a self, _: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
            Box::pin(async { Ok(*self.0.lock().unwrap()) })
        }
    }

    struct Peers {
        current: QueueAuthority,
        receivers: [Mutex<WorkerReplica>; 2],
        faults: Mutex<VecDeque<ProtocolError>>,
        attempts: Mutex<Vec<(WorkerIdentity, ReadGrantSet)>>,
        refused: Notify,
    }
    impl ReplicaTransport for Peers {
        fn install<'a>(
            &'a self,
            _: WorkerIdentity,
            _: &'a RecordFrame,
        ) -> EndpointFuture<'a, CopyAcknowledgement> {
            panic!("read grant copy must not reinstall the request")
        }
        fn copy_read_grants(
            &self,
            target: WorkerIdentity,
            scope: QueueScope,
            view: AuthorityView,
            id: Uuid,
            set: ReadGrantSet,
        ) -> EndpointFuture<'_, ()> {
            Box::pin(async move {
                assert_eq!(view, self.current.ready.view);
                self.attempts.lock().unwrap().push((target, set.clone()));
                let mut first = self.receivers[0].lock().unwrap();
                if first.local_identity() == target {
                    assert_eq!(*first.scope(), scope);
                    return first
                        .install_read_grants(self.current, self.current.owner, id, set)
                        .map_err(QueueError::protocol);
                }
                drop(first);
                let fault = self.faults.lock().unwrap().pop_front();
                if let Some(error) = &fault
                    && error.outcome == Outcome::NotApplied
                {
                    self.refused.notify_one();
                    return Err(error.clone());
                }
                let mut blocked = self.receivers[1].lock().unwrap();
                assert_eq!(blocked.local_identity(), target);
                assert_eq!(*blocked.scope(), scope);
                blocked
                    .install_read_grants(self.current, self.current.owner, id, set)
                    .map_err(QueueError::protocol)?;
                fault.map_or(Ok(()), Err)
            })
        }
    }

    let fixture = Fixture::new(3);
    let current = fixture.authority;
    let mut owner = fixture.replica(0);
    let mut receivers = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let pending = owner
        .propose(current, body.clone(), context.clone(), 0)
        .unwrap();
    let accepted = copy(&mut owner, &mut receivers, &pending).unwrap();
    copy(&mut owner, &mut receivers, &accepted);
    let running = owner.start(current, id).unwrap();
    copy(&mut owner, &mut receivers, &running);
    drop((pending, accepted, running));
    let template = ReadGrant {
        scope: ReadScope {
            request: RequestBinding {
                run_id: current.ready.view.run_id,
                request_id: id,
                digest: hash(&body),
                accepted_context: hash(
                    owner.record(id).unwrap().pinned_catalog.as_deref().unwrap(),
                ),
            },
            authority: current.ready.view.authority(),
            read_ts: context.read.as_ref().unwrap().storage_pin.read_ts,
        },
        owner: current.owner,
        worker: current.owner,
        group: nabedra_rt::ids::uuid_v4(),
        node: 1,
        ticket: Uuid::nil(),
        input: hash(b"scan page"),
    };
    let shared = fixture.shared.clone();
    let baseline = shared.scheduler().usage();
    let grant = shared.read_attempts.open(&shared, template).unwrap();
    let refused = ProtocolError::new(ErrorCode::Overloaded, "test lane full", Outcome::NotApplied);
    let lost = ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "test copy reply lost",
        Outcome::Unknown,
    );
    let owner = Arc::new(Mutex::new(owner));
    let authority = Arc::new(Current(Mutex::new(current)));
    let peers = Arc::new(Peers {
        current,
        receivers: receivers.map(Mutex::new),
        faults: Mutex::new(VecDeque::from([refused.clone(), lost.clone()])),
        attempts: Mutex::new(Vec::new()),
        refused: Notify::new(),
    });
    let queue = QueueCoordinator::new(owner.clone(), authority.clone(), peers.clone());
    let opening = {
        let queue = queue.clone();
        nabedra_rt::task::spawn(async move { queue.open_read_grant(grant).await })
    };
    peers.refused.notified().await;
    assert!(!opening.is_finished());
    assert!(owner.lock().unwrap().pending_read_grant_copy(id).is_some());
    assert!(peers.receivers[0].lock().unwrap().knows_read_grant(grant));
    assert!(!peers.receivers[1].lock().unwrap().knows_read_grant(grant));
    opening.await.unwrap().unwrap();
    assert!(owner.lock().unwrap().pending_read_grant_copy(id).is_none());
    assert!(
        peers
            .receivers
            .iter()
            .all(|r| r.lock().unwrap().knows_read_grant(grant))
    );
    {
        let attempts = peers.attempts.lock().unwrap();
        let targets = attempts
            .iter()
            .map(|(target, _)| *target)
            .collect::<Vec<_>>();
        assert_eq!(
            targets,
            [
                fixture.members[1],
                fixture.members[2],
                fixture.members[2],
                fixture.members[2]
            ]
        );
        assert!(
            attempts
                .iter()
                .all(|(_, set)| *set == attempts[0].1 && set.grants == [grant])
        );
    }
    queue.settle_read_grant(grant).await.unwrap();
    assert!(
        peers
            .receivers
            .iter()
            .all(|r| !r.lock().unwrap().has_read_grants(id))
    );
    assert_eq!(shared.scheduler().usage(), baseline);

    let grant = shared.read_attempts.open(&shared, template).unwrap();
    peers.faults.lock().unwrap().push_back(refused.clone());
    peers.attempts.lock().unwrap().clear();
    let opening = {
        let queue = queue.clone();
        nabedra_rt::task::spawn(async move { queue.open_read_grant(grant).await })
    };
    peers.refused.notified().await;
    authority.0.lock().unwrap().ready.view.membership_revision += 1;
    let stale = opening.await.unwrap().unwrap_err();
    assert_eq!(stale, QueueError::Stale.protocol());
    assert_eq!(peers.attempts.lock().unwrap().len(), 2);
    assert!(owner.lock().unwrap().pending_read_grant_copy(id).is_some());
    assert!(!peers.receivers[1].lock().unwrap().knows_read_grant(grant));
    *authority.0.lock().unwrap() = current;
    queue.settle_read_grant(grant).await.unwrap();
    assert_eq!(shared.scheduler().usage(), baseline);

    let grant = shared.read_attempts.open(&shared, template).unwrap();
    {
        let mut faults = peers.faults.lock().unwrap();
        faults.push_back(lost);
        faults.extend(std::iter::repeat_n(refused, 130));
    }
    let start = nabedra_rt::time::now();
    let expired = queue.open_read_grant(grant).await.unwrap_err();
    assert_eq!(expired.code, ErrorCode::Overloaded);
    assert_eq!(expired.outcome, Outcome::Unknown);
    assert!((29..=30).contains(&nabedra_rt::time::elapsed(start).as_secs()));
    assert!(owner.lock().unwrap().pending_read_grant_copy(id).is_some());
    assert!(peers.receivers[1].lock().unwrap().knows_read_grant(grant));
    peers.faults.lock().unwrap().clear();
    queue.settle_read_grant(grant).await.unwrap();
    assert_eq!(shared.scheduler().usage(), baseline);
}

#[test]
fn o22_c04_r01_r03_r04_f03_live_read_grants_survive_copy_and_owner_loss() {
    use crate::oltp_replication::service_rpc::ReadScope;
    use crate::oltp_runtime::worker_endpoint::QueueRequest;

    let fixture = Fixture::new(3);
    let current = fixture.authority;
    let mut owner = fixture.replica(0);
    let mut survivors = [fixture.replica(1), fixture.replica(2)];
    let target = survivors[1].shared_resources();
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let pending = owner.propose(current, body.clone(), context.clone(), 0).unwrap();
    let accepted = copy(&mut owner, &mut survivors, &pending).unwrap();
    copy(&mut owner, &mut survivors, &accepted);
    let running = owner.start(current, id).unwrap();
    copy(&mut owner, &mut survivors, &running);
    let record = owner.record(id).unwrap();
    let accepted_context = record.pinned_catalog.clone().unwrap();
    let sequence = record.sequence;
    let scope = ReadScope {
        request: RequestBinding {
            run_id: current.ready.view.run_id,
            request_id: id,
            digest: hash(&body),
            accepted_context: hash(&accepted_context),
        },
        authority: current.ready.view.authority(),
        read_ts: context.read.as_ref().unwrap().storage_pin.read_ts,
    };
    let template = ReadGrant {
        scope, owner: current.owner, worker: fixture.members[2],
        group: nabedra_rt::ids::uuid_v4(), node: 1, ticket: Uuid::nil(), input: hash(b"scan page"),
    };
    let baseline = target.scheduler().usage();
    let mut running = Vec::new();
    for _ in 0..MAX_READ_GRANTS {
        let grant = target.read_attempts.open(&target, template).unwrap();
        assert!(!survivors[1].knows_read_grant(grant));
        let set = owner.update_read_grant(current, grant, true).unwrap().unwrap();
        for worker in &mut survivors {
            worker.install_read_grants(current, current.owner, id, set.clone()).unwrap();
            assert!(worker.knows_read_grant(grant));
        }
        owner.complete_read_grant_copy(current, id, &set).unwrap();
        let (actual, lease) = target.read_attempts.claim(grant.ticket, current.owner, grant.group, grant.node).unwrap();
        assert_eq!(actual, grant);
        running.push((grant, lease));
    }
    assert_eq!(owner.update_read_grant(current, ReadGrant { ticket: nabedra_rt::ids::uuid_v4(), ..template }, true),
        Err(QueueError::Overloaded));

    let joiner = WorkerIdentity { worker_id: nabedra_rt::ids::uuid_v4(), incarnation: nabedra_rt::ids::uuid_v4(), ..fixture.members[2] };
    let ticket = JoinTicket { ticket_id: nabedra_rt::ids::uuid_v4(), target: joiner, source_ready: current.ready };
    let snapshot = owner.begin_join(current, ticket).unwrap();
    let page = owner.join_page(current, snapshot, JoinPhase::Snapshot, None).unwrap();
    let frame_header = page.record.as_ref().unwrap().decode_header().unwrap();
    assert_eq!(frame_header.read_grants.grants.len(), MAX_READ_GRANTS);
    let copy_wire = QueueRequest::JoinPage {
        scope: fixture.scope.clone(), view: current.ready.view, page: Box::new(page.header()),
        record: Some(frame_header),
    };
    assert!(serde_json::to_vec(&copy_wire).unwrap().len() < MAX_RECORD_HEADER_BYTES);
    let mut joining = WorkerReplica::new_joining(
        WorkerSharedResources::new(91, WorkerResources::default(), SharedResourceConfig::default()).unwrap(),
        fixture.scope.clone(), joiner, current, fixture.members.clone(), QueueLimits {
            max_pending_requests: 20,
            max_queue_bytes_per_replica: 4 << 20,
            max_retained_result_bytes_per_worker: 1 << 20,
            result_retention_ms: 100,
        }, ticket,
    ).unwrap();
    joining.install_join_page(current, current.owner, &page).unwrap();
    owner.seal_join(current, snapshot).unwrap();
    let delta = owner.join_page(current, snapshot, JoinPhase::Delta, None).unwrap();
    joining.install_join_page(current, current.owner, &delta).unwrap();
    assert_eq!(joining.join_statement(current).unwrap(), owner.join_statement(current).unwrap());
    assert_eq!(joining.pending_read_grants(id), owner.pending_read_grants(id));
    drop(joining);

    // The previous owner exits before producing a result, while its exact
    // tickets still hold memory on the original target process.
    drop(owner);
    let members = fixture.members[1..].to_vec();
    let mut ready = ReadySetHasher::default();
    for member in &members {
        ready.push(*member).unwrap();
    }
    let next = QueueAuthority {
        owner: fixture.members[1],
        ready: ReadyMembership {
            view: AuthorityView { owner_term: 2, membership_revision: 2, ..current.ready.view },
            ready_root: ready.finish(), ready_count: ready.count(),
        },
    };
    for survivor in &mut survivors {
        survivor.fenced_takeover(current, next, members.clone()).unwrap();
        assert_eq!(survivor.pending_read_grants(id).len(), MAX_READ_GRANTS);
    }
    assert_eq!(survivors[0].start(next, id).unwrap_err(), QueueError::Unavailable);
    for (grant, _) in &running {
        assert!(target.read_attempts.settle(*grant).is_err());
        assert!(target.read_attempts.claim(grant.ticket, next.owner, grant.group, grant.node).is_err());
    }
    for (grant, lease) in running {
        let forged = ReadGrant { input: hash(b"different input"), ..grant };
        assert!(target.read_attempts.settle(forged).is_err());
        assert_eq!(survivors[0].update_read_grant(next, forged, false), Err(QueueError::Conflict));
        drop(lease);
        target.read_attempts.settle(grant).unwrap();
        let cleared = survivors[0].update_read_grant(next, grant, false).unwrap().unwrap();
        survivors[1].install_read_grants(next, next.owner, id, cleared.clone()).unwrap();
        assert!(survivors[0].has_read_grants(id));
        survivors[0].complete_read_grant_copy(next, id, &cleared).unwrap();
    }
    assert_eq!(target.scheduler().usage()[0].2, baseline[0].2 - 256);
    assert!(!survivors[0].has_read_grants(id));
    assert!(!survivors[1].has_read_grants(id));
    assert_eq!(survivors[0].record(id).unwrap().sequence, sequence);
    assert_eq!(survivors[0].record(id).unwrap().pinned_catalog.as_ref(), Some(&accepted_context));
    let [mut owner, survivor] = survivors;
    let recovered = owner.resume(next, id).unwrap();
    copy(&mut owner, &mut [survivor], &recovered);
    assert!(owner.start(next, id).is_ok());

    // No new process uses an old ticket or recovers an old accepted record.
    drop(owner);
    drop(target);
    let cold = Fixture::new(1);
    let restarted = cold.replica(0);
    assert_ne!(restarted.authority().ready.view.run_id, current.ready.view.run_id);
    assert!(restarted.record(id).is_none());
    assert!(!restarted.has_read_grants(id));
}

#[test]
fn o22_c07_f03_failed_receipt_retains_read_pin_until_grant_settlement() {
    use crate::oltp_replication::service_rpc::ReadScope;

    let fixture = Fixture::new(2);
    let current = fixture.authority;
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1)];
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let read = context.read.clone().unwrap();
    let pending = owner.propose(current, body.clone(), context, 0).unwrap();
    let accepted = copy(&mut owner, &mut replicas, &pending).unwrap();
    copy(&mut owner, &mut replicas, &accepted);
    let running = owner.start(current, id).unwrap();
    copy(&mut owner, &mut replicas, &running);
    let pin = owner.record(id).unwrap().pinned_catalog.as_deref().unwrap();
    let template = ReadGrant {
        scope: ReadScope {
            request: RequestBinding {
                run_id: current.ready.view.run_id,
                request_id: id,
                digest: hash(&body),
                accepted_context: hash(pin),
            },
            authority: current.ready.view.authority(),
            read_ts: read.storage_pin.read_ts,
        },
        owner: current.owner,
        worker: fixture.members[1],
        group: nabedra_rt::ids::uuid_v4(),
        node: 1,
        ticket: Uuid::nil(),
        input: hash(b"scan page"),
    };
    let target = replicas[0].shared_resources();
    let scheduler = target.scheduler();
    let baseline = scheduler.usage();
    let grant = target.read_attempts.open(&target, template).unwrap();
    let grant_bytes = scheduler.usage()[0].2 - baseline[0].2;
    assert!(grant_bytes > 0);
    let pending = owner
        .update_read_grant(current, grant, true)
        .unwrap()
        .unwrap();
    replicas[0]
        .install_read_grants(current, current.owner, id, pending.clone())
        .unwrap();
    owner
        .complete_read_grant_copy(current, id, &pending)
        .unwrap();
    let (claimed, attempt) = target
        .read_attempts
        .claim(grant.ticket, grant.owner, grant.group, grant.node)
        .unwrap();
    assert_eq!(claimed, grant);
    let reply = crate::response::encode_error(Some(id), QueueError::Unavailable.protocol());
    let digest = hash(&reply.body);
    let terminal = owner
        .finish(current, id, reply.status, reply.body, 1)
        .unwrap();
    let version = terminal.record_version().unwrap();
    copy(&mut owner, &mut replicas, &terminal);

    assert_eq!(
        owner.retained_record(id, 2).unwrap().state,
        RequestState::Failed
    );
    assert_eq!(owner.terminal_response_hash(id), Ok(digest));
    assert!(owner.copies_complete(id));
    assert_eq!(owner.terminal_statement(id), Err(QueueError::Unavailable));
    assert_eq!(
        owner.terminal_copy_statement(id),
        Err(QueueError::Unavailable)
    );
    assert_eq!(
        replicas[0].terminal_copy_statement(id),
        Err(QueueError::Unavailable)
    );
    let before_settlement = scheduler.usage();
    assert!(target.read_attempts.settle(grant).is_err());
    assert_eq!(scheduler.usage(), before_settlement);

    drop(attempt);
    target.read_attempts.settle(grant).unwrap();
    assert_eq!(scheduler.usage()[0].2 + grant_bytes, before_settlement[0].2);
    let cleared = owner
        .update_read_grant(current, grant, false)
        .unwrap()
        .unwrap();
    replicas[0]
        .install_read_grants(current, current.owner, id, cleared.clone())
        .unwrap();
    assert_eq!(
        owner.terminal_copy_statement(id),
        Err(QueueError::Unavailable)
    );
    assert_eq!(owner.terminal_statement(id), Err(QueueError::Unavailable));
    owner
        .complete_read_grant_copy(current, id, &cleared)
        .unwrap();

    // The new grant revision invalidates the old terminal frame's ACKs.
    assert_eq!(owner.terminal_statement(id), Err(QueueError::Unavailable));
    let settled = owner.resume(current, id).unwrap();
    let settled_version = settled.record_version().unwrap();
    assert_eq!(settled_version.version, version.version);
    assert_ne!(settled_version.digest, version.digest);
    let old_ack = CopyAcknowledgement::authenticated(current.owner, version);
    assert!(matches!(
        owner.acknowledge(current, old_ack),
        Err(QueueError::Stale)
    ));
    let local_ack = owner.install(current, current.owner, &settled).unwrap();
    owner.acknowledge(current, local_ack).unwrap();
    assert_eq!(owner.terminal_statement(id), Err(QueueError::Unavailable));
    let peer_ack = replicas[0]
        .install(current, current.owner, &settled)
        .unwrap();
    owner.acknowledge(current, peer_ack).unwrap();

    let statement = owner.terminal_statement(id).unwrap();
    assert_eq!(statement.response_hash, digest);
    assert_eq!(statement.pin_identity, read.identity);
    assert_eq!(statement.storage_pin, read.storage_pin);
    assert_eq!(replicas[0].terminal_copy_statement(id).unwrap(), statement);
}

#[test]
fn o14_o23_q03_r01_r02_r03_removal_merges_lower_uuid_data_survivor() {
    use crate::oltp_replication::service_rpc::ReadScope;

    let mut fixture = Fixture::new(4);
    let [data_id, failed_id, owner_id, peer_id] = fixture.members.as_slice() else {
        unreachable!()
    };
    fixture.authority.owner = *failed_id;
    let previous = fixture.authority;
    let mut old_owner = fixture.replica(1);
    let mut survivors = [fixture.replica(0), fixture.replica(2), fixture.replica(3)];
    let copy =
        |owner: &mut WorkerReplica, survivors: &mut [WorkerReplica; 3], frame: &RecordFrame| {
            let [data, first_system, second_system] = survivors;
            let data_ack = data.install(previous, previous.owner, frame).unwrap();
            owner.acknowledge_phase(previous, data_ack, false).unwrap();
            let owner_ack =
                CopyAcknowledgement::authenticated(previous.owner, frame.record_version().unwrap());
            owner.acknowledge_phase(previous, owner_ack, false).unwrap();
            for peer in [first_system, second_system] {
                let ack = peer.install(previous, previous.owner, frame).unwrap();
                owner.acknowledge_phase(previous, ack, false).unwrap();
            }
        };
    let accepted = Uuid::from_u128(1);
    let tentative = Uuid::from_u128(2);
    let terminal = Uuid::from_u128(3);
    let mut original = BTreeMap::new();
    for id in [accepted, tentative, terminal] {
        let body = fixture.body(id);
        let context = fixture.context(id, &body);
        let payload = old_owner
            .propose(previous, body.clone(), context.clone(), 0)
            .unwrap();
        original.insert(id, (body, serde_json::to_vec(&context).unwrap()));
        if id == tentative {
            // The earliest ready worker installed one tentative page. Its ACK
            // and every later body copy are absent when the owner fails.
            survivors[0]
                .install(previous, previous.owner, &payload)
                .unwrap();
            continue;
        }
        copy(&mut old_owner, &mut survivors, &payload);
        let evidence = old_owner.accept_staged(previous, id).unwrap();
        if id == accepted {
            // W4 sorts before either electable survivor. It gets the first
            // accepted version, but the all-ready acceptance barrier is open.
            survivors[0]
                .install(previous, previous.owner, &evidence)
                .unwrap();
            assert!(!old_owner.copies_complete(id));
        } else {
            copy(&mut old_owner, &mut survivors, &evidence);
            let running = old_owner.start(previous, id).unwrap();
            copy(&mut old_owner, &mut survivors, &running);
            let grant = ReadGrant {
                scope: ReadScope {
                    request: RequestBinding {
                        run_id: previous.ready.view.run_id,
                        request_id: id,
                        digest: hash(&original[&id].0),
                        accepted_context: hash(&original[&id].1),
                    },
                    authority: previous.ready.view.authority(),
                    read_ts: context.read.as_ref().unwrap().storage_pin.read_ts,
                },
                owner: previous.owner,
                worker: *data_id,
                group: nabedra_rt::ids::uuid_v4(),
                node: 1,
                ticket: nabedra_rt::ids::uuid_v4(),
                input: hash(b"held scan page"),
            };
            let grants = old_owner
                .update_read_grant(previous, grant, true)
                .unwrap()
                .unwrap();
            survivors[0]
                .install_read_grants(previous, previous.owner, id, grants.clone())
                .unwrap();
            let reply = crate::response::encode_error(Some(id), QueueError::Unavailable.protocol());
            let completed = old_owner
                .finish(previous, id, reply.status, reply.body, 1)
                .unwrap();
            survivors[0]
                .install(previous, previous.owner, &completed)
                .unwrap();
            assert_eq!(survivors[0].pending_read_grants(id), vec![grant]);
            assert!(!old_owner.copies_complete(id));
        }
    }
    drop(old_owner);

    let current = QueueAuthority {
        owner: *owner_id,
        ready: ReadyMembership {
            view: AuthorityView {
                owner_term: 2,
                ..previous.ready.view
            },
            ..previous.ready
        },
    };
    for survivor in &mut survivors {
        survivor
            .fenced_takeover(previous, current, fixture.members.clone())
            .unwrap();
    }
    let [mut data, mut owner, mut peer] = survivors;
    assert!(data.record(accepted).unwrap().accepted);
    assert!(!owner.record(accepted).unwrap().accepted);
    assert!(owner.record(tentative).is_none());
    assert!(matches!(
        data.install(current, current.owner, &owner.frame(accepted).unwrap()),
        Err(QueueError::Stale)
    ));
    let terminal_before = data.observation(terminal).unwrap();
    let terminal_hash = data.terminal_response_hash(terminal).unwrap();
    let grants_before = data.pending_read_grants(terminal);
    let ticket = JoinTicket {
        ticket_id: nabedra_rt::ids::uuid_v4(),
        target: *failed_id,
        source_ready: current.ready,
    };
    owner.begin_removal_merge(current, ticket).unwrap();
    assert_eq!(
        owner.begin_removal(current, ticket),
        Err(QueueError::Unavailable)
    );
    assert_eq!(owner.removal_merge_step(ticket), Ok(Some((*data_id, None))));
    let mut pages = 0;
    while let Some((worker, after)) = owner.removal_merge_step(ticket).unwrap() {
        pages += 1;
        assert!(pages <= 9);
        let from = if worker == *data_id {
            Some(&data)
        } else if worker == *peer_id {
            Some(&peer)
        } else {
            None
        };
        let frame = from.and_then(|from| from.removal_record(current, ticket, after).unwrap());
        if let Some(frame) = frame {
            if after.is_none() && worker == *data_id {
                let retry = data
                    .removal_record(current, ticket, after)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    frame.record_version().unwrap(),
                    retry.record_version().unwrap()
                );
            }
            let environment = owner.install_environment(current, current.owner).unwrap();
            if after.is_none() && worker == *peer_id {
                let mut forged = frame.decode_header().unwrap();
                forged.version = owner.record(accepted).unwrap().version;
                let duplicate = RecordFrame {
                    header: owner
                        .shared_resources()
                        .encode(&forged, MAX_RECORD_HEADER_BYTES)
                        .unwrap(),
                    sections: frame.sections.clone(),
                };
                let prepared = PreparedInstall::prepare(environment.clone(), &duplicate).unwrap();
                assert_eq!(
                    owner.merge_removal_record(ticket, worker, after, prepared, None),
                    Err(QueueError::Conflict)
                );
                assert_eq!(owner.removal_merge_step(ticket), Ok(Some((worker, after))));
            }
            let prepared = PreparedInstall::prepare(environment, &frame).unwrap();
            owner
                .merge_removal_record(ticket, worker, after, prepared, None)
                .unwrap();
        } else {
            owner
                .acknowledge_removal_inventory(ticket, worker, after)
                .unwrap();
        }
    }
    assert_eq!(pages, 8);
    let proof = owner.begin_removal(current, ticket).unwrap();
    assert_eq!(proof.snapshot_cut, 3);
    for survivor in [&mut data, &mut peer] {
        for id in [accepted, tentative, terminal] {
            survivor
                .install(current, current.owner, &owner.frame(id).unwrap())
                .unwrap();
        }
        assert_eq!(survivor.removal_statement(ticket).unwrap(), proof);
    }
    for survivor in [&data, &owner, &peer] {
        assert_eq!(survivor.observation(terminal).unwrap(), terminal_before);
        assert_eq!(
            survivor.terminal_response_hash(terminal).unwrap(),
            terminal_hash
        );
        assert_eq!(survivor.pending_read_grants(terminal), grants_before);
        assert!(matches!(
            survivor.retained_record(tentative, 2),
            Err(QueueError::Unavailable)
        ));
        for id in [accepted, tentative, terminal] {
            let (body, context, _) = survivor.input(id).unwrap();
            assert_eq!(
                (body.as_ref(), context.as_ref()),
                (original[&id].0.as_slice(), original[&id].1.as_slice())
            );
        }
    }

    // After the ready cut publishes, the original accepted read can finish
    // with the original pin. Merely copying the W4-only tentative cannot run it.
    let live = [*data_id, *owner_id, *peer_id];
    let mut root = ReadySetHasher::default();
    for worker in live {
        root.push(worker).unwrap();
    }
    let ready = QueueAuthority {
        owner: current.owner,
        ready: ReadyMembership {
            view: AuthorityView {
                membership_revision: 2,
                ..current.ready.view
            },
            ready_root: root.finish(),
            ready_count: root.count(),
        },
    };
    for survivor in [&mut data, &mut owner, &mut peer] {
        survivor.fenced_takeover(current, ready, live).unwrap();
    }
    let replicate = |owner: &mut WorkerReplica,
                     data: &mut WorkerReplica,
                     peer: &mut WorkerReplica,
                     frame: &RecordFrame| {
        let first = data.install(ready, ready.owner, frame).unwrap();
        let local =
            CopyAcknowledgement::authenticated(ready.owner, frame.record_version().unwrap());
        let last = peer.install(ready, ready.owner, frame).unwrap();
        for ack in [first, local, last] {
            assert!(owner.acknowledge(ready, ack).unwrap().is_none());
        }
    };
    assert_eq!(
        owner.start(ready, tentative).unwrap_err(),
        QueueError::Unavailable
    );
    let recovered = owner.resume(ready, accepted).unwrap();
    replicate(&mut owner, &mut data, &mut peer, &recovered);
    let running = owner.start(ready, accepted).unwrap();
    replicate(&mut owner, &mut data, &mut peer, &running);
    let expected =
        format!(r#"{{"request_id":"{accepted}","data":["original read"]}}"#).into_bytes();
    let response = owner
        .finish(ready, accepted, 200, expected.clone(), 2)
        .unwrap();
    replicate(&mut owner, &mut data, &mut peer, &response);
    for survivor in [&data, &owner, &peer] {
        let (record, bytes) = survivor
            .response(previous.ready.view.run_id, accepted, 2)
            .unwrap();
        assert_eq!(record.state, RequestState::Succeeded);
        assert_eq!(bytes.unwrap().as_ref(), expected.as_slice());
        assert_eq!(
            survivor.input(accepted).unwrap().1.as_ref(),
            original[&accepted].1.as_slice()
        );
    }
}

#[test]
fn r01_r02_removal_copy_resumes_exact_ack_cursor_after_cancel_and_lost_reply() {
    let fixture = Fixture::new(3);
    let mut old_owner = fixture.replica(0);
    let mut survivors = [fixture.replica(1), fixture.replica(2)];
    let ids = (1..=3).map(Uuid::from_u128).collect::<Vec<_>>();
    let mut original = Vec::new();
    for id in &ids {
        let mut body = fixture.body(*id);
        body.extend(std::iter::repeat_n(b' ', 96 << 10));
        let context = fixture.context(*id, &body);
        let payload = old_owner
            .propose(fixture.authority, body.clone(), context.clone(), 0)
            .unwrap();
        let accepted = copy(&mut old_owner, &mut survivors, &payload).unwrap();
        copy(&mut old_owner, &mut survivors, &accepted);
        assert!(old_owner.copies_complete(*id));
        original.push((body, context));
    }
    let current = QueueAuthority {
        owner: fixture.members[1],
        ready: ReadyMembership {
            view: AuthorityView {
                owner_term: 2,
                ..fixture.authority.ready.view
            },
            ..fixture.authority.ready
        },
    };
    for worker in &mut survivors {
        worker
            .fenced_takeover(fixture.authority, current, fixture.members.clone())
            .unwrap();
    }
    drop(old_owner);
    let [mut source, mut target] = survivors;
    let ticket = JoinTicket {
        ticket_id: nabedra_rt::ids::uuid_v4(),
        target: fixture.members[0],
        source_ready: current.ready,
    };
    source.begin_removal_merge(current, ticket).unwrap();
    while let Some((worker, after)) = source.removal_merge_step(ticket).unwrap() {
        let frame = if worker == target.local_identity() {
            target.removal_record(current, ticket, after).unwrap()
        } else {
            None
        };
        if let Some(frame) = frame {
            let environment = source.install_environment(current, current.owner).unwrap();
            let prepared = PreparedInstall::prepare(environment, &frame).unwrap();
            source.merge_removal_record(ticket, worker, after, prepared, None).unwrap();
        } else {
            source.acknowledge_removal_inventory(ticket, worker, after).unwrap();
        }
    }
    let root = source.begin_removal(current, ticket).unwrap();
    source
        .acknowledge_removal_worker(ticket, source.local_identity())
        .unwrap();
    assert_eq!(
        source.removal_progress(ticket).unwrap(),
        (Some(source.local_identity().worker_id), None)
    );
    assert_eq!(
        source.acknowledge_removal_worker(ticket, target.local_identity()),
        Err(QueueError::Stale)
    );
    let frame = source.frame(ids[0]).unwrap();
    let lost_ack = target.install(current, current.owner, &frame).unwrap();
    // Reply lost before cursor save: the exact repeated install is safe.
    assert_eq!(source.removal_progress(ticket).unwrap().1, None);
    let replay_ack = target.install(current, current.owner, &frame).unwrap();
    assert_eq!(lost_ack.record(), replay_ack.record());
    let mut forged = replay_ack.record();
    forged.version += 1;
    assert_eq!(
        source.acknowledge_removal_record(ticket, target.local_identity(), None, forged),
        Err(QueueError::Stale)
    );
    source
        .acknowledge_removal_record(ticket, target.local_identity(), None, replay_ack.record())
        .unwrap();
    drop(frame);
    // Dropping/restarting the coordinator future preserves acknowledged RAM
    // progress, without treating the cursor itself as a complete-copy proof.
    assert_eq!(source.begin_removal(current, ticket).unwrap(), root);
    assert_eq!(source.removal_progress(ticket).unwrap().1, Some(ids[0]));
    assert_eq!(
        source.record_after(source.removal_progress(ticket).unwrap().1),
        Some(ids[1])
    );
    let mut wrong = ticket;
    wrong.ticket_id = nabedra_rt::ids::uuid_v4();
    assert_eq!(source.removal_progress(wrong), Err(QueueError::Stale));
    let mut after = Some(ids[0]);
    for id in &ids[1..] {
        let frame = source.frame(*id).unwrap();
        let ack = target.install(current, current.owner, &frame).unwrap();
        source
            .acknowledge_removal_record(ticket, target.local_identity(), after, ack.record())
            .unwrap();
        after = Some(*id);
    }
    source
        .acknowledge_removal_worker(ticket, target.local_identity())
        .unwrap();
    assert_eq!(
        source.removal_statement(ticket).unwrap(),
        target.removal_statement(ticket).unwrap()
    );
    for (id, (body, context)) in ids.iter().zip(original) {
        for replica in [&source, &target] {
            let record = replica.record(*id).unwrap();
            assert!(record.accepted);
            assert_eq!(record.body, body);
            assert_eq!(
                serde_json::from_slice::<AcceptedContext>(
                    record.pinned_catalog.as_deref().unwrap()
                )
                .unwrap(),
                context
            );
        }
    }
    let members = fixture.members[1..].to_vec();
    let mut next = current;
    next.ready.view.membership_revision += 1;
    let mut ready = ReadySetHasher::default();
    for member in &members {
        ready.push(*member).unwrap();
    }
    next.ready.ready_root = ready.finish();
    next.ready.ready_count = ready.count();
    source.fenced_takeover(current, next, members).unwrap();
    assert_eq!(source.removal_progress(ticket), Err(QueueError::Stale));
}

#[test]
fn q03_payload_and_acceptance_evidence_are_separate_all_ready_barriers() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
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
fn b02_r01_queue_latency_consumes_the_first_dispatch_and_drops_fenced_waits() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let stats = |replica: &WorkerReplica| serde_json::to_value(replica.queue_latency_stats()).unwrap();
    let samples = |replica: &WorkerReplica| {
        stats(replica)["bucket_counts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|count| count.as_u64().unwrap())
            .sum::<u64>()
    };
    let enqueue = |owner: &mut WorkerReplica, replicas: &mut [WorkerReplica]| {
        let id = nabedra_rt::ids::uuid_v4();
        let body = fixture.body(id);
        let context = fixture.context(id, &body);
        let payload = owner.propose(fixture.authority, body, context, 1).unwrap();
        let accepted = copy(owner, replicas, &payload).unwrap();
        assert_eq!(stats(owner)["waiting"], 0);
        copy(owner, replicas, &accepted);
        assert_eq!(stats(owner)["waiting"], 1);
        assert_eq!(stats(&replicas[0])["waiting"], 0);
        id
    };

    let id = enqueue(&mut owner, &mut replicas);
    let running = owner.start(fixture.authority, id).unwrap();
    let version = running.record_version().unwrap();
    owner.observe_dispatch(fixture.authority, version);
    assert_eq!(samples(&owner), 0);
    copy(&mut owner, &mut replicas, &running);
    owner.observe_dispatch(fixture.authority, version);
    owner.observe_dispatch(fixture.authority, version);
    assert_eq!(stats(&owner)["waiting"], 0);
    assert_eq!(samples(&owner), 1);
    assert_eq!(samples(&replicas[0]), 0);

    let pending = enqueue(&mut owner, &mut replicas);
    let mut next = fixture.authority;
    next.ready.view.owner_term += 1;
    owner
        .fenced_takeover(fixture.authority, next, fixture.members.iter().copied())
        .unwrap();
    for replica in &mut replicas {
        replica
            .fenced_takeover(fixture.authority, next, fixture.members.iter().copied())
            .unwrap();
    }
    let resumed = owner.resume(next, pending).unwrap();
    copy(&mut owner, &mut replicas, &resumed);
    let resumed = owner.start(next, pending).unwrap();
    copy(&mut owner, &mut replicas, &resumed);
    owner.observe_dispatch(next, resumed.record_version().unwrap());
    assert_eq!(stats(&owner)["discarded_on_authority_change_total"], 1);
    assert_eq!(stats(&owner)["waiting"], 0);
    assert_eq!(samples(&owner), 0);
}

#[test]
fn q04_q06_idle_preflight_preserves_recovery_and_expired_result_work() {
    for count in [1, 3] {
        let fixture = Fixture::new(count);
        let mut owner = fixture.replica(0);
        let mut followers: Vec<_> = (1..count)
            .map(|index| fixture.replica(index as usize))
            .collect();
        let id = nabedra_rt::ids::uuid_v4();
        let mut active = BTreeSet::new();
        assert!(!owner.has_recovery_work(1, &active));
        assert!(!owner.has_retirement_work(0, &active));

        let body = fixture.body(id);
        let context = fixture.context(id, &body);
        let proposed = owner.propose(fixture.authority, body, context, 0).unwrap();
        assert!(owner.has_recovery_work(1, &active));
        let accepted = copy(&mut owner, &mut followers, &proposed).unwrap();
        copy(&mut owner, &mut followers, &accepted);
        assert!(owner.has_recovery_work(1, &active));
        assert!(!owner.has_retirement_work(0, &active));

        let running = owner.start(fixture.authority, id).unwrap();
        copy(&mut owner, &mut followers, &running);
        owner.reconcile(fixture.authority, id).unwrap();
        assert!(owner.has_recovery_work(1, &active));
        assert!(owner.has_retirement_work(1, &active));
        active.insert(id);
        assert!(!owner.has_recovery_work(1, &active));
        assert!(!owner.has_retirement_work(1, &active));
        active.remove(&id);

        let response = format!(r#"{{"request_id":"{id}","data":[]}}"#).into_bytes();
        let terminal = owner
            .finish(fixture.authority, id, 200, response, 2)
            .unwrap();
        let ack = owner
            .install(fixture.authority, fixture.authority.owner, &terminal)
            .unwrap();
        owner.acknowledge(fixture.authority, ack).unwrap();
        assert!(!owner.has_retirement_work(101, &active));
        assert!(matches!(
            owner.retained_record(id, 102),
            Err(QueueError::NotFound)
        ));
        assert!(owner.has_retirement_work(102, &active));
        assert!(owner.has_recovery_work(101, &active));
        assert!(!owner.has_recovery_work(102, &active));
        for follower in &mut followers {
            let ack = follower
                .install(fixture.authority, fixture.authority.owner, &terminal)
                .unwrap();
            owner.acknowledge(fixture.authority, ack).unwrap();
            assert!(follower.has_retirement_work(102, &active));
        }
        owner
            .confirm_terminal_copy(fixture.authority, terminal.record_version().unwrap())
            .unwrap();
        assert!(!owner.has_recovery_work(1, &active));
        assert!(owner.has_retirement_work(102, &active));
    }
}

#[test]
fn q06_t04_receipt_race_reuses_terminal_version_and_retains_pin_until_every_copy() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
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
    // Every copied holder can supply its own metadata without the old owner's
    // private all-copy barrier. The TSO leader checks them independently.
    for replica in &mut replicas {
        assert!(!replica.copies_complete(id));
        assert_eq!(
            replica.terminal_copy_statement(id).unwrap(),
            proof_statement
        );
        let actual = replica.terminal_metadata.get(&id).unwrap().version;
        replica.terminal_metadata.get_mut(&id).unwrap().version = actual - 1;
        assert!(matches!(
            replica.terminal_copy_statement(id),
            Err(QueueError::Stale)
        ));
        replica.terminal_metadata.get_mut(&id).unwrap().version = actual;
    }

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
fn q06_o14_o23_terminal_confirmation_requires_the_exact_ready_view_and_version() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let previous = fixture.authority;
    let confirmed = |replica: &WorkerReplica, authority| {
        replica
            .terminal_copy_confirmation(authority, replica.record(id).unwrap())
            .is_some()
    };
    let payload = owner.propose(previous, body, context.clone(), 1).unwrap();
    let accepted = copy(&mut owner, &mut replicas, &payload).unwrap();
    copy(&mut owner, &mut replicas, &accepted);
    let running = owner.start(previous, id).unwrap();
    copy(&mut owner, &mut replicas, &running);
    let response = format!(r#"{{"request_id":"{id}","data":["exact"]}}"#).into_bytes();
    let terminal = owner.finish(previous, id, 200, response.clone(), 2).unwrap();
    let previous_version = terminal.record_version().unwrap();
    assert_eq!(
        owner.confirm_terminal_copy(previous, previous_version),
        Err(QueueError::Stale)
    );
    copy(&mut owner, &mut replicas, &terminal);
    assert!(owner.copies_complete(id));
    assert!(!confirmed(&owner, previous));
    assert_eq!(owner.recovery_ids(2, None, 1), vec![id]);

    let mut current = previous;
    current.ready.view.membership_revision += 1;
    owner
        .fenced_takeover(previous, current, fixture.members.clone())
        .unwrap();
    for replica in &mut replicas {
        replica
            .fenced_takeover(previous, current, fixture.members.clone())
            .unwrap();
    }
    assert!(!confirmed(&owner, current));
    assert!(!confirmed(&owner, previous));
    let rebased = owner.resume(current, id).unwrap();
    let current_version = rebased.record_version().unwrap();
    assert_ne!(current_version.version, previous_version.version);
    assert_ne!(current_version.digest, previous_version.digest);
    copy(&mut owner, &mut replicas, &rebased);
    for (authority, version) in [
        (previous, previous_version),
        (current, previous_version),
        (
            current,
            RecordVersion {
                digest: previous_version.digest,
                ..current_version
            },
        ),
    ] {
        assert_eq!(
            owner.confirm_terminal_copy(authority, version),
            Err(QueueError::Stale)
        );
    }
    assert!(!confirmed(&owner, current));
    owner
        .confirm_terminal_copy(current, current_version)
        .unwrap();
    assert!(confirmed(&owner, current));
    assert!(!confirmed(&owner, previous));
    let (record, bytes) = owner.response(current.ready.view.run_id, id, 2).unwrap();
    assert_eq!(record.version, current_version.version);
    assert_eq!(bytes.unwrap().as_ref(), response.as_slice());
    assert_eq!(
        owner.terminal_statement(id).unwrap().storage_pin,
        context.read.unwrap().storage_pin
    );
    let mut grants = owner.read_grant_set(id);
    grants.revision += 1;
    assert!(owner.record_read_grants(id, grants));
    assert!(!confirmed(&owner, current));
    assert_eq!(owner.recovery_ids(2, None, 1), vec![id]);
}

#[test]
fn r01_r05_takeover_keeps_original_context_and_rejects_stale_owner_or_new_run() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut replicas = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
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
    cold.ready.view.run_id = nabedra_rt::ids::uuid_v4();
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
    let id = nabedra_rt::ids::uuid_v4();
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
    stale.incarnation = nabedra_rt::ids::uuid_v4();
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
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([3; 32]),
        enrollment_revision: 1,
    };
    let ticket = JoinTicket {
        ticket_id: nabedra_rt::ids::uuid_v4(),
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

#[tokio::test]
async fn r03_q03_join_empty_single_multiple_and_exhausted_pages_have_correct_done_flag() {
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
        assert_eq!(
            owner.begin_join(fixture.authority, ticket).unwrap(),
            snapshot
        );
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
            // A lost authenticated page reply resends the same page. The
            // sender advances its RAM cursor only after that reply arrives.
            assert_eq!(
                owner
                    .join_export_progress(fixture.authority, snapshot, JoinPhase::Snapshot)
                    .unwrap(),
                (cursor, false)
            );
            target
                .install_join_page(fixture.authority, fixture.authority.owner, &page)
                .unwrap();
            owner
                .acknowledge_join_page(fixture.authority, page.header())
                .unwrap();
            assert_eq!(
                owner
                    .join_export_progress(fixture.authority, snapshot, JoinPhase::Snapshot)
                    .unwrap(),
                (page.next_sequence, page.done)
            );
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
        assert_eq!(
            owner.begin_join(fixture.authority, ticket).unwrap(),
            snapshot
        );
        let delta = owner
            .join_page(fixture.authority, snapshot, JoinPhase::Delta, None)
            .unwrap();
        assert!(delta.done && delta.record.is_none());
        target
            .install_join_page(fixture.authority, fixture.authority.owner, &delta)
            .unwrap();
        owner
            .acknowledge_join_page(fixture.authority, delta.header())
            .unwrap();
        assert_eq!(
            owner
                .join_export_progress(fixture.authority, snapshot, JoinPhase::Delta)
                .unwrap(),
            (delta.next_sequence, true)
        );
        assert_eq!(
            target.join_statement(fixture.authority).unwrap(),
            owner.join_statement(fixture.authority).unwrap()
        );
        // O14/O23, R03/Q03, C07/M02: the complete statement's bounded
        // representation needs more decode heap than the former 16 KiB guess.
        // Keep Data full while the protected metadata reply decoder progresses.
        let statement = target.join_statement(fixture.authority).unwrap();
        let full_data = fixture
            .shared
            .allocate_bytes(
                crate::oltp::pool::ByteClass::Data,
                fixture.shared.config().control_bytes - fixture.shared.config().progress_bytes,
            )
            .unwrap();
        let header = fixture
            .shared
            .encode_in(
                crate::oltp::pool::ByteClass::Progress,
                &super::super::worker_endpoint::QueueReply::JoinStatement {
                    worker: ticket.target,
                    statement: Box::new(statement),
                    nonce: nabedra_rt::ids::uuid_v4(),
                },
                MAX_HEADER_BYTES,
            )
            .unwrap();
        let decode = crate::oltp::mvcc::json_decode_budget(header.as_ref()).unwrap();
        assert!(decode > 4 * MAX_HEADER_BYTES);
        let owner = Arc::new(fixture.shared.reserve_reply_decode(decode).await.unwrap());
        let (decoded, owner) = fixture
            .shared
            .execute(
                crate::oltp::pool::ExecutionClass::Control,
                0,
                0,
                move || {
                    let decoded: super::super::worker_endpoint::QueueReply =
                        serde_json::from_slice(header.as_ref()).unwrap();
                    Ok((decoded, owner))
                },
            )
            .await
            .unwrap();
        let super::super::worker_endpoint::QueueReply::JoinStatement {
            worker,
            statement: copied,
            ..
        } = decoded
        else {
            panic!("wrong typed join completion");
        };
        assert_eq!(worker, ticket.target);
        assert_eq!(*copied, statement);
        drop(owner);
        drop(full_data);
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
    let id = nabedra_rt::ids::uuid_v4();
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

#[test]
fn m02_fixed_terminal_cache_slot_is_reserved_at_initial_admission_and_snapshot_replacement() {
    let fixture = Fixture::new(1);
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let context_len = serde_json::to_vec(&context).unwrap().len();
    let limits = QueueLimits {
        max_pending_requests: 4,
        max_queue_bytes_per_replica: 1024 + body.len() + context_len + 32 + PROOF_METADATA_BYTES
            - 1,
        max_retained_result_bytes_per_worker: 1 << 20,
        result_retention_ms: 100,
    };
    let mut replica = WorkerReplica::new(
        fixture.shared.clone(),
        fixture.scope.clone(),
        fixture.members[0],
        fixture.authority,
        fixture.members.clone(),
        limits,
    )
    .unwrap();
    assert!(matches!(
        replica.propose(fixture.authority, body.clone(), context.clone(), 1),
        Err(QueueError::Overloaded)
    ));
    assert_eq!(replica.pending(), 0);
    drop(replica);
    let mut replica = WorkerReplica::new(
        fixture.shared.clone(),
        fixture.scope.clone(),
        fixture.members[0],
        fixture.authority,
        fixture.members.clone(),
        QueueLimits {
            max_queue_bytes_per_replica: limits.max_queue_bytes_per_replica + 1,
            ..limits
        },
    )
    .unwrap();
    replica
        .propose(fixture.authority, body, context, 1)
        .unwrap();
    let bytes = replica.queue.charged_bytes();
    assert!(bytes.0 >= PROOF_METADATA_BYTES + 1024);
    replica
        .queue
        .install_snapshot(replica.queue.snapshot())
        .unwrap();
    assert_eq!(replica.queue.charged_bytes(), bytes);
}

#[test]
fn q03_f03_tentative_copies_keep_original_context_until_explicit_acceptance() {
    let fixture = Fixture::new(3);
    let mut owner = fixture.replica(0);
    let mut first = fixture.replica(1);
    let mut second = fixture.replica(2);
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let context = fixture.context(id, &body);
    let payload = owner
        .propose(fixture.authority, body.clone(), context.clone(), 1)
        .unwrap();
    let ack = owner
        .install(fixture.authority, fixture.authority.owner, &payload)
        .unwrap();
    owner
        .acknowledge_phase(fixture.authority, ack, false)
        .unwrap();
    assert!(!owner.payloads_complete(id));
    assert_eq!(owner.recovery_ids(1, None, 1), vec![id]);
    assert!(owner.has_recovery_work(1, &BTreeSet::new()));
    assert!(!owner.has_recovery_work(1, &BTreeSet::from([id])));
    for replica in [&mut first, &mut second] {
        let ack = replica
            .install(fixture.authority, fixture.authority.owner, &payload)
            .unwrap();
        owner
            .acknowledge_phase(fixture.authority, ack, false)
            .unwrap();
    }
    assert!(owner.payloads_complete(id));
    assert!(!owner.copies_complete(id));
    assert!(!owner.record(id).unwrap().accepted);
    assert_eq!(
        owner.start(fixture.authority, id).unwrap_err(),
        QueueError::Unavailable
    );
    drop(owner);
    let live = fixture.members[1..].to_vec();
    let mut root = ReadySetHasher::default();
    for member in &live {
        root.push(*member).unwrap();
    }
    let next = QueueAuthority {
        owner: live[0],
        ready: ReadyMembership {
            view: AuthorityView {
                run_id: fixture.authority.ready.view.run_id,
                owner_term: 2,
                membership_revision: 2,
            },
            ready_root: root.finish(),
            ready_count: 2,
        },
    };
    first
        .fenced_takeover(fixture.authority, next, live.clone())
        .unwrap();
    second
        .fenced_takeover(fixture.authority, next, live)
        .unwrap();
    let record = first.record(id).unwrap();
    assert_eq!(record.body, body);
    assert!(!record.accepted);
    assert_eq!(first.recovery_ids(1, None, 1), vec![id]);
    assert_eq!(
        serde_json::from_slice::<AcceptedContext>(record.pinned_catalog.as_ref().unwrap()).unwrap(),
        context
    );
    let payload = first.resume(next, id).unwrap();
    let ack = first.install(next, next.owner, &payload).unwrap();
    first.acknowledge_phase(next, ack, false).unwrap();
    let ack = second.install(next, next.owner, &payload).unwrap();
    first.acknowledge_phase(next, ack, false).unwrap();
    assert!(first.payloads_complete(id));
    assert!(!first.copies_complete(id));
    // Production invokes this only after the real TSO context-bind succeeds.
    let accepted = first.accept_staged(next, id).unwrap();
    let ack = first.install(next, next.owner, &accepted).unwrap();
    first.acknowledge(next, ack).unwrap();
    assert!(!first.copies_complete(id));
    let ack = second.install(next, next.owner, &accepted).unwrap();
    first.acknowledge(next, ack).unwrap();
    assert!(first.copies_complete(id));
    assert_eq!(
        serde_json::from_slice::<AcceptedContext>(
            first.record(id).unwrap().pinned_catalog.as_ref().unwrap()
        )
        .unwrap(),
        context
    );
}

#[test]
fn o13_q06_delayed_install_cannot_restore_retired_terminal_and_new_sequences_skip_lookup() {
    use crate::oltp_replication::authority::status::VerifiedTerminalRetirement;
    let fixture = Fixture::new(2);
    let current = fixture.authority;
    let mut owner = fixture.replica(0);
    let mut peers = [fixture.replica(1)];
    let propose = |source: &mut WorkerReplica| {
        let id = nabedra_rt::ids::uuid_v4();
        let body = fixture.body(id);
        let context = fixture.context(id, &body);
        (id, source.propose(current, body, context, 0).unwrap())
    };
    let (_, earlier) = propose(&mut owner);
    let (id, tentative) = propose(&mut owner);
    let accepted = copy(&mut owner, &mut peers, &tentative).unwrap();
    copy(&mut owner, &mut peers, &accepted);
    let response = serde_json::to_vec(&serde_json::json!({ "request_id": id, "data": [] })).unwrap();
    let terminal = owner.finish(current, id, 200, response, 1).unwrap();
    copy(&mut owner, &mut peers, &terminal);
    let statement = owner.terminal_statement(id).unwrap();
    let [mut receiver] = peers;
    let prepare = |receiver: &WorkerReplica, frame: &RecordFrame| {
        PreparedInstall::prepare(receiver.install_environment(current, current.owner).unwrap(), frame).unwrap()
    };

    let delayed = prepare(&receiver, &accepted);
    let header = accepted.decode_header().unwrap();
    assert_eq!(receiver.install_retirement_check(&header), None);
    let proof = || VerifiedTerminalRetirement::test_certificate(statement.clone(), &fixture.members);
    assert_eq!(receiver.retire(current, proof(), 100), Err(QueueError::Stale));
    assert!(receiver.record(id).is_some());
    receiver.retire(current, proof(), 101).unwrap();
    assert_eq!(receiver.stats().charged_queue_bytes, 0);
    assert!(receiver.install_retirement_check(&header).is_some());
    assert!(matches!(receiver.install_prepared_checked(delayed, None), Err(QueueError::Stale)));
    assert!(receiver.record(id).is_none());

    // A slower earlier identity needs a strong negative certificate lookup.
    // A higher new sequence is provably outside this local deletion history.
    let node_generation = receiver.install_retirement_check(&earlier.decode_header().unwrap());
    assert!(node_generation.is_some());
    let prepared = prepare(&receiver, &earlier);
    receiver.install_prepared_checked(prepared, node_generation).unwrap();
    let (new_id, newer) = propose(&mut owner);
    assert_eq!(receiver.install_retirement_check(&newer.decode_header().unwrap()), None);
    let prepared = prepare(&receiver, &newer);
    receiver.install_prepared_checked(prepared, None).unwrap();
    assert!(receiver.record(new_id).is_some());
}

mod scoped_acceptance;

/// D05/LIVE1: the owner removed a holder that stopped answering. When it comes
/// back with the same incarnation its RAM queue is stale and it cannot take
/// over the published ready set. It joins again in place: an empty joining
/// queue for its own ticket, filled by the owner's join copy, keeping its
/// memory charge and queue lease.
#[test]
fn d05_live1_removed_replica_rejoins_in_place() {
    let fixture = Fixture::new(3);
    let current = fixture.authority;
    let limits = QueueLimits {
        max_pending_requests: 20,
        max_queue_bytes_per_replica: 4 << 20,
        max_retained_result_bytes_per_worker: 1 << 20,
        result_retention_ms: 100,
    };
    let mut owner = fixture.replica(0);
    let mut holders = [fixture.replica(1), fixture.replica(2)];
    let id = nabedra_rt::ids::uuid_v4();
    let body = fixture.body(id);
    let pending = owner
        .propose(current, body.clone(), fixture.context(id, &body), 0)
        .unwrap();
    copy(&mut owner, &mut holders, &pending).unwrap();
    let [survivor, mut removed] = holders;
    let removed_shared = removed.shared_resources();
    let charged = removed_shared.scheduler().usage();

    // The owner removes the third holder.
    let authority = |members: &[WorkerIdentity], revision| {
        let mut ready = ReadySetHasher::default();
        for member in members {
            ready.push(*member).unwrap();
        }
        QueueAuthority {
            owner: fixture.members[0],
            ready: ReadyMembership {
                view: AuthorityView { membership_revision: revision, ..current.ready.view },
                ready_root: ready.finish(),
                ready_count: ready.count(),
            },
        }
    };
    let remaining = fixture.members[..2].to_vec();
    let next = authority(&remaining, 2);
    owner.fenced_takeover(current, next, remaining.clone()).unwrap();
    let mut survivor = survivor;
    survivor.fenced_takeover(current, next, remaining.clone()).unwrap();
    assert_eq!(
        removed.fenced_takeover(current, next, remaining.clone()),
        Err(QueueError::Invalid)
    );

    // Its own Join ticket replaces the stale queue in place.
    let local = fixture.members[2];
    let ticket = JoinTicket { ticket_id: nabedra_rt::ids::uuid_v4(), target: local, source_ready: next.ready };
    let foreign = JoinTicket { target: fixture.members[1], ..ticket };
    assert_eq!(removed.rejoin(next, remaining.clone(), limits, foreign), Err(QueueError::Invalid));
    assert_eq!(
        removed.rejoin(next, fixture.members.clone(), limits, ticket),
        Err(QueueError::Invalid)
    );
    removed.rejoin(next, remaining.clone(), limits, ticket).unwrap();
    assert_eq!(removed.authority(), next);
    assert!(removed.record(id).is_none());
    assert!(!removed.is_ready_member());
    assert_eq!(removed_shared.scheduler().usage()[0].2, charged[0].2 - 256);

    // The owner's join copy restores the accepted record, then the published
    // ready set includes the worker again.
    let snapshot = owner.begin_join(next, ticket).unwrap();
    let page = owner.join_page(next, snapshot, JoinPhase::Snapshot, None).unwrap();
    removed.install_join_page(next, next.owner, &page).unwrap();
    owner.seal_join(next, snapshot).unwrap();
    let delta = owner.join_page(next, snapshot, JoinPhase::Delta, None).unwrap();
    removed.install_join_page(next, next.owner, &delta).unwrap();
    assert_eq!(removed.join_statement(next).unwrap(), owner.join_statement(next).unwrap());
    let joined = authority(&fixture.members, 3);
    removed.fenced_takeover(next, joined, fixture.members.clone()).unwrap();
    assert!(removed.is_ready_member());
    assert_eq!(removed.record(id).unwrap().request_id, id);
}
