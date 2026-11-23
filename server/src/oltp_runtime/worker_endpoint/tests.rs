use super::*;
use crate::{
    config::WorkerResources, oltp::pool::SharedResourceConfig, oltp_replication::ReadySetHasher,
    request_queue::QueueLimits,
};
use std::sync::atomic::AtomicBool;
use nabedra_rt::time::{Duration, timeout};
use tokio::sync::Notify;

#[path = "../../../tests/support/tls.rs"]
mod tls;

struct BlockFirstAuthority {
    current: QueueAuthority,
    first: AtomicBool,
    entered: Notify,
    release: Notify,
}
impl AuthorityReader for BlockFirstAuthority {
    fn current<'a>(&'a self, _: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(async move {
            if self.first.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(self.current)
        })
    }
}

struct UnavailableAuthority(AtomicU64, Mutex<ProtocolError>);
impl AuthorityReader for UnavailableAuthority {
    fn current<'a>(&'a self, _: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Err(self.1.lock().unwrap().clone()) })
    }
}

struct ProbePeerDirectory([WorkerEnrollment; 2]);
impl PeerDirectory for ProbePeerDirectory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        _: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if purpose != Purpose::QueueControl
            || WorkerIdentity::from(&self.resolve(peer.worker_id)?) != peer
        {
            return Err(malformed("unexpected queue probe identity"));
        }
        Ok(TransportTrust::AppliedEnrollment)
    }

    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.0
            .iter()
            .find(|entry| entry.worker_id == worker)
            .cloned()
            .ok_or_else(unavailable)
    }
}

struct LegacyProbe;
impl Endpoint for LegacyProbe {
    fn preflight<'a>(
        &'a self,
        _: &'a AuthenticatedSession,
        _: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async { Err(QueueError::Stale.protocol()) })
    }

    fn handle<'a>(
        &'a self,
        _: AuthenticatedSession,
        _: crate::oltp::pool::ChargedBytes,
        _: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        panic!("legacy queue probe must reject during preflight")
    }
}

struct ProbeFixture {
    caller: QueuePeerClient,
    target: WorkerIdentity,
    scope: QueueScope,
    ready: ReadyMembership,
    authority: Arc<UnavailableAuthority>,
    legacy: QueuePeerClient,
    stop: tokio::sync::oneshot::Sender<()>,
    listener: tokio::task::JoinHandle<Result<(), ProtocolError>>,
}

impl ProbeFixture {
    async fn new() -> Self {
        struct Temporary(std::path::PathBuf);
        impl Drop for Temporary {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let temp = Temporary(std::env::temp_dir().join(format!("queue-probe-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir(&temp.0).unwrap();
        let [caller_tls, target_tls]: [_; 2] =
            tls::certificates(&temp.0, 2).try_into().ok().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let enrollment = |certificate, endpoint| WorkerEnrollment {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: Digest::parse(certificate).unwrap(),
            endpoint: Text::try_from(endpoint).unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            enrollment_revision: 1,
        };
        let caller = enrollment(caller_tls.certificate_sha256(), "127.0.0.1:1");
        let target = enrollment(
            target_tls.certificate_sha256(),
            &listener.local_addr().unwrap().to_string(),
        );
        let directory = Arc::new(ProbePeerDirectory([caller.clone(), target.clone()]));
        let target_id = WorkerIdentity::from(&target);
        let caller_id = WorkerIdentity::from(&caller);
        let mut members = [target_id, caller_id];
        members.sort_by_key(|member| member.worker_id);
        let mut root = ReadySetHasher::default();
        for member in members {
            root.push(member).unwrap();
        }
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: nabedra_rt::ids::uuid_v4(),
                owner_term: 1,
                membership_revision: 1,
            },
            ready_root: root.finish(),
            ready_count: members.len() as u64,
        };
        let group = GroupIdentity {
            cluster_id: nabedra_rt::ids::uuid_v4(),
            database: Text::try_from("probe").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        };
        let legacy_group = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            ..group.clone()
        };
        let scope = QueueScope {
            cluster_id: group.cluster_id,
            database: group.database.clone(),
        };
        let shared = |id| {
            WorkerSharedResources::new(
                id,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap()
        };
        let caller_resources = shared(1);
        let target_resources = shared(2);
        let caller_transport = WorkerTransport::new(
            &caller,
            &caller_tls,
            caller_resources.clone(),
            directory.clone(),
            Arc::new(EndpointRegistry::new(caller_resources.clone())),
            TransportConfig::default(),
        )
        .unwrap();
        let replica = WorkerReplica::new(
            target_resources.clone(),
            scope.clone(),
            target_id,
            QueueAuthority {
                ready,
                owner: caller_id,
            },
            members,
            QueueLimits {
                max_pending_requests: 2,
                max_queue_bytes_per_replica: 1 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 60_000,
            },
        )
        .unwrap();
        let authority = Arc::new(UnavailableAuthority(
            AtomicU64::new(0),
            Mutex::new(unavailable()),
        ));
        let endpoint = QueueEndpoint::new(
            Arc::new(Mutex::new(replica)),
            target_resources.clone(),
            scope.clone(),
            authority.clone(),
            128 << 20,
        )
        .unwrap();
        let registry = Arc::new(EndpointRegistry::new(target_resources.clone()));
        registry
            .reserve(group.clone())
            .unwrap()
            .bind(Purpose::QueueControl, endpoint)
            .unwrap();
        registry
            .reserve(legacy_group.clone())
            .unwrap()
            .bind(Purpose::QueueControl, Arc::new(LegacyProbe))
            .unwrap();
        let transport = Arc::new(
            WorkerTransport::new(
                &target,
                &target_tls,
                target_resources,
                directory,
                registry,
                TransportConfig::default(),
            )
            .unwrap(),
        );
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let listener = nabedra_rt::task::spawn(transport.run(listener, async {
            let _ = shutdown.await;
        }));
        Self {
            caller: QueuePeerClient::new(
                caller_transport.client(),
                caller_resources.clone(),
                group,
            )
            .unwrap(),
            target: target_id,
            scope,
            ready,
            authority,
            legacy: QueuePeerClient::new(caller_transport.client(), caller_resources, legacy_group)
                .unwrap(),
            stop,
            listener,
        }
    }
}

#[tokio::test]
async fn q03_r05_mtls_probe_reaches_live_peer_with_unavailable_control() {
    let fixture = ProbeFixture::new().await;
    let mut fresh = fixture.ready;
    fresh.view.owner_term += 1;
    assert_eq!(
        fixture
            .caller
            .probe(fixture.target, fixture.scope.clone(), fresh)
            .await,
        PeerReachability::Reachable
    );
    assert_eq!(fixture.authority.0.load(Ordering::Relaxed), 0);
    let state = fixture
        .caller
        .state(
            fixture.target,
            fixture.scope.clone(),
            fresh.view,
            nabedra_rt::ids::uuid_v4(),
        )
        .await;
    assert_eq!(state.unwrap_err().code, ErrorCode::StorageUnavailable);
    assert_eq!(fixture.authority.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        fixture
            .legacy
            .probe(fixture.target, fixture.scope.clone(), fresh)
            .await,
        PeerReachability::Reachable
    );
    let other = WorkerIdentity {
        incarnation: nabedra_rt::ids::uuid_v4(),
        ..fixture.target
    };
    assert_eq!(
        fixture
            .legacy
            .probe(other, fixture.scope.clone(), fresh)
            .await,
        PeerReachability::Indeterminate
    );
    let held = fixture
        .caller
        .client
        .hold_outgoing_for_test(TrafficClass::QueueControl);
    assert_eq!(
        fixture
            .caller
            .probe(fixture.target, fixture.scope.clone(), fresh)
            .await,
        PeerReachability::Indeterminate
    );
    drop(held);
    fixture.stop.send(()).unwrap();
    fixture.listener.await.unwrap().unwrap();
}

#[tokio::test]
async fn q03_r05_mtls_probe_detects_stopped_worker_listener() {
    let fixture = ProbeFixture::new().await;
    assert_eq!(
        fixture
            .caller
            .probe(fixture.target, fixture.scope.clone(), fixture.ready)
            .await,
        PeerReachability::Reachable
    );
    fixture.stop.send(()).unwrap();
    fixture.listener.await.unwrap().unwrap();
    for _ in 0..3 {
        assert_eq!(
            fixture
                .caller
                .probe(fixture.target, fixture.scope.clone(), fixture.ready)
                .await,
            PeerReachability::Unreachable
        );
    }
}

#[tokio::test]
async fn c07_f03_o23_read_grant_copy_keeps_transport_outcomes_distinct() {
    let fixture = ProbeFixture::new().await;
    let id = nabedra_rt::ids::uuid_v4();
    let copy = || {
        fixture.caller.copy_read_grants(
            fixture.target,
            fixture.scope.clone(),
            fixture.ready.view,
            id,
            ReadGrantSet {
                revision: 2,
                grants: Vec::new(),
            },
        )
    };
    let full = fixture
        .caller
        .client
        .hold_outgoing_for_test(TrafficClass::QueueControl);
    let refused = copy().await.unwrap_err();
    assert_eq!(refused.code, ErrorCode::Overloaded);
    assert_eq!(refused.message, "worker outgoing lane full (QueueControl)");
    assert_eq!(refused.outcome, Outcome::NotApplied);
    assert_eq!(fixture.authority.0.load(Ordering::Relaxed), 0);
    drop(full);

    // This identical error came from the authenticated receiver's Authority
    // lookup. The caller cannot use it as evidence about a copy's application.
    *fixture.authority.1.lock().unwrap() = refused;
    let rejected = copy().await.unwrap_err();
    assert_eq!(rejected.code, ErrorCode::Overloaded);
    assert_eq!(rejected.outcome, Outcome::Unknown);
    assert_eq!(fixture.authority.0.load(Ordering::Relaxed), 1);

    let invalidated = QueueError::Stale.protocol();
    *fixture.authority.1.lock().unwrap() = invalidated.clone();
    assert_eq!(copy().await.unwrap_err(), invalidated);
    assert_eq!(fixture.authority.0.load(Ordering::Relaxed), 2);

    fixture.stop.send(()).unwrap();
    fixture.listener.await.unwrap().unwrap();
    let lost = copy().await.unwrap_err();
    assert_eq!(lost.code, ErrorCode::OutcomeUnknown);
    assert_eq!(lost.outcome, Outcome::Unknown);
}

#[test]
fn m02_nested_raw_predicate_bytes_are_charged_without_a_wide_row_multiplier() {
    let text = "}".repeat(64 << 10);
    let shallow = serde_json::json!({"row":{"text":text}}).to_string();
    let shallow_bound = request_raw_clone_bytes(shallow.as_bytes()).unwrap();
    assert!(shallow_bound < 3 * shallow.len());
    let mut predicate = serde_json::json!({"op":"eq","column":"id","value":text});
    for _ in 1..crate::protocol::MAX_PREDICATE_DEPTH {
        predicate = serde_json::json!({"op":"and","args":[predicate]});
    }
    let deep = serde_json::json!({"payload":{"query":{"filter":predicate}}}).to_string();
    let deep_bound = request_raw_clone_bytes(deep.as_bytes()).unwrap();
    assert!(deep_bound >= text.len() * crate::protocol::MAX_PREDICATE_DEPTH);
    assert!(deep_bound < 40 * deep.len());
}

#[tokio::test]
async fn o16_r02_r03_queued_get_copy_waits_for_receiver_membership() {
    use sha2::{Digest as _, Sha256};

    let resources = |node| {
        WorkerSharedResources::new(
            node,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap()
    };
    let shared = resources(92);
    let worker_base = nabedra_rt::ids::uuid_v4().as_u128() & !0xff;
    let worker = |id: u128| WorkerIdentity {
        worker_id: Uuid::from_u128(worker_base + id),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([id as u8; 32]),
        enrollment_revision: 1,
    };
    let [owner, follower, removed] = [worker(1), worker(2), worker(3)];
    let scope = QueueScope {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("default").unwrap(),
    };
    let run_id = nabedra_rt::ids::uuid_v4();
    let authority = |membership_revision, members: &[WorkerIdentity]| {
        let mut root = ReadySetHasher::default();
        for member in members {
            root.push(*member).unwrap();
        }
        QueueAuthority {
            ready: ReadyMembership {
                view: AuthorityView {
                    run_id,
                    owner_term: 2,
                    membership_revision,
                },
                ready_root: root.finish(),
                ready_count: root.count(),
            },
            owner,
        }
    };
    let previous_members = [owner, follower, removed];
    let members = [owner, follower];
    let previous = authority(3, &previous_members);
    let current = authority(4, &members);
    let limits = QueueLimits {
        max_pending_requests: 4,
        max_queue_bytes_per_replica: 1 << 20,
        max_retained_result_bytes_per_worker: 1 << 20,
        result_retention_ms: 300_000,
    };
    let mut sender = WorkerReplica::new(
        resources(91),
        scope.clone(),
        owner,
        current,
        members,
        limits,
    )
    .unwrap();
    let receiver = Arc::new(Mutex::new(
        WorkerReplica::new(
            shared.clone(),
            scope.clone(),
            follower,
            previous,
            previous_members,
            limits,
        )
        .unwrap(),
    ));
    let endpoint = QueueEndpoint::new(
        receiver.clone(),
        shared.clone(),
        scope.clone(),
        Arc::new(BlockFirstAuthority {
            current,
            first: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }),
        128 << 20,
    )
    .unwrap();
    let group = GroupIdentity {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: GROUP_FORMAT_VERSION,
    };
    let id = nabedra_rt::ids::uuid_v4();
    let body = serde_json::to_vec(&serde_json::json!({
        "request_id": id, "operation": "table.get",
        "payload": {"table_id": nabedra_rt::ids::uuid_v4(), "key": 9}
    }))
    .unwrap();
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: scope.database.clone(),
            principal: Digest::from_bytes([2; 32]),
            policy_revision: 1,
            ingress_id: nabedra_rt::ids::uuid_v4(),
            ingress_incarnation: nabedra_rt::ids::uuid_v4(),
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: PinIdentity {
                run_id,
                request_id: id,
                digest: Digest::from_bytes(Sha256::digest(&body).into()),
                nonce: nabedra_rt::ids::uuid_v4(),
            },
            storage_pin: StoragePin {
                pin_id: nabedra_rt::ids::uuid_v4(),
                read_ts: Timestamp::new(1).unwrap(),
                allocator_epoch: 1,
            },
            catalog_root: Digest::from_bytes([3; 32]),
            schema_epoch: 1,
            routing_epoch: 1,
        }),
    };
    let frame = sender
        .propose(current, body.clone(), context.clone(), 1)
        .unwrap();
    let session = || {
        AuthenticatedSession::for_test(
            follower,
            owner,
            group.clone(),
            Purpose::QueueData,
            nabedra_rt::ids::uuid_v4(),
        )
    };
    let header = |record| {
        shared
            .encode(&QueueRequest::Install { record }, MAX_RECORD_HEADER_BYTES)
            .unwrap()
    };
    let sections =
        || ChargedSections::for_test(&shared, ByteClass::Data, frame.sections.clone(), 0).unwrap();
    let record = frame.decode_header().unwrap();
    let install = header(record.clone());

    let plan = endpoint
        .preflight(&session(), install.as_ref())
        .await
        .unwrap();
    assert_eq!(plan.section_lengths, record.section_lengths);
    let Err(error) = endpoint
        .handle(session(), install.clone(), sections())
        .await
    else {
        panic!("receiver acknowledged a copy before applying the ready set");
    };
    assert_eq!(
        (error.status_code(), error.code, error.outcome),
        (503, ErrorCode::StorageUnavailable, Outcome::Unknown),
        "{error}"
    );
    {
        let mut receiver = receiver.lock().unwrap();
        assert!(receiver.record(id).is_none());
        receiver
            .fenced_takeover(previous, current, members)
            .unwrap();
    }

    let reply = endpoint
        .handle(session(), install, sections())
        .await
        .unwrap();
    let QueueReply::Installed {
        worker,
        record: version,
        ..
    } = serde_json::from_slice(reply.header.as_ref()).unwrap()
    else {
        panic!("expected an installation acknowledgement");
    };
    assert_eq!(worker, follower);
    assert_eq!(version, frame.record_version().unwrap());
    {
        let receiver = receiver.lock().unwrap();
        let installed = receiver.record(id).unwrap();
        assert_eq!(installed.run_id, run_id);
        assert_eq!(installed.body, body);
        assert_eq!(
            installed.pinned_catalog.as_deref(),
            Some(serde_json::to_vec(&context).unwrap().as_slice())
        );
        assert!(!installed.accepted);
    }

    let mut malformed = record;
    malformed.sequence = 0;
    let error = endpoint
        .preflight(&session(), header(malformed).as_ref())
        .await
        .unwrap_err();
    assert_eq!(
        (error.status_code(), error.code, error.outcome),
        (400, ErrorCode::ValidationError, Outcome::NotApplied)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c07_m02_blocked_authority_preflight_and_full_data_arena_leave_receipt_progress() {
    let shared = WorkerSharedResources::new(
        91,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap();
    let local = WorkerIdentity {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([1; 32]),
        enrollment_revision: 1,
    };
    let scope = QueueScope {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("default").unwrap(),
    };
    let mut root = ReadySetHasher::default();
    root.push(local).unwrap();
    let authority = QueueAuthority {
        ready: ReadyMembership {
            view: AuthorityView {
                run_id: nabedra_rt::ids::uuid_v4(),
                owner_term: 1,
                membership_revision: 1,
            },
            ready_root: root.finish(),
            ready_count: 1,
        },
        owner: local,
    };
    let mut replica = WorkerReplica::new(
        shared.clone(),
        scope.clone(),
        local,
        authority,
        [local],
        QueueLimits {
            max_pending_requests: 4,
            max_queue_bytes_per_replica: 1 << 20,
            max_retained_result_bytes_per_worker: 1 << 20,
            result_retention_ms: 300_000,
        },
    )
    .unwrap();
    let id = nabedra_rt::ids::uuid_v4();
    let body =
        format!(r#"{{"request_id":"{id}","operation":"table.list","payload":{{}}}}"#).into_bytes();
    use sha2::{Digest as _, Sha256};
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: scope.database.clone(),
            principal: Digest::from_bytes([2; 32]),
            policy_revision: 1,
            ingress_id: nabedra_rt::ids::uuid_v4(),
            ingress_incarnation: nabedra_rt::ids::uuid_v4(),
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: PinIdentity {
                run_id: authority.ready.view.run_id,
                request_id: id,
                digest: Digest::from_bytes(Sha256::digest(&body).into()),
                nonce: nabedra_rt::ids::uuid_v4(),
            },
            storage_pin: StoragePin {
                pin_id: nabedra_rt::ids::uuid_v4(),
                read_ts: Timestamp::new(1).unwrap(),
                allocator_epoch: 1,
            },
            catalog_root: Digest::from_bytes([3; 32]),
            schema_epoch: 1,
            routing_epoch: 1,
        }),
    };
    let payload = replica.propose(authority, body, context, 1).unwrap();
    let ack = replica.install(authority, local, &payload).unwrap();
    let accepted = replica.acknowledge(authority, ack).unwrap().unwrap();
    let ack = replica.install(authority, local, &accepted).unwrap();
    replica.acknowledge(authority, ack).unwrap();
    let request = QueueRequest::Install {
        record: accepted.decode_header().unwrap(),
    };
    let install_header = serde_json::to_vec(&request).unwrap();
    drop(payload);
    drop(accepted);
    let baseline = shared.scheduler().usage();
    let authority_source = Arc::new(BlockFirstAuthority {
        current: authority,
        first: AtomicBool::new(true),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let endpoint = QueueEndpoint::new(
        Arc::new(Mutex::new(replica)),
        shared.clone(),
        scope.clone(),
        authority_source.clone(),
        128 << 20,
    )
    .unwrap();
    let group = GroupIdentity {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: GROUP_FORMAT_VERSION,
    };
    let session = AuthenticatedSession::for_test(
        local,
        local,
        group.clone(),
        Purpose::QueueData,
        nabedra_rt::ids::uuid_v4(),
    );
    let waiting = {
        let endpoint = endpoint.clone();
        nabedra_rt::task::spawn(async move { endpoint.preflight(&session, &install_header).await })
    };
    timeout(Duration::from_secs(2), authority_source.entered.notified())
        .await
        .unwrap();
    let fill = shared
        .allocate_bytes(
            ByteClass::Data,
            shared.config().control_bytes - shared.config().progress_bytes,
        )
        .unwrap();
    let state = QueueRequest::State {
        scope: scope.clone(),
        view: authority.ready.view,
        request_id: id,
    };
    let state_header = shared
        .encode_in(
            ByteClass::Progress,
            &state,
            super::super::replica::MAX_HEADER_BYTES,
        )
        .unwrap();
    let session = AuthenticatedSession::for_test(
        local,
        local,
        group.clone(),
        Purpose::QueueControl,
        nabedra_rt::ids::uuid_v4(),
    );
    let plan = timeout(
        Duration::from_secs(2),
        endpoint.preflight(&session, state_header.as_ref()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(plan.section_lengths, [0; 3]);
    assert_eq!(plan.decode_budget_bytes, 0);
    let input =
        ChargedSections::for_test(&shared, ByteClass::Progress, [None, None, None], 0).unwrap();
    let reply = timeout(
        Duration::from_secs(2),
        endpoint.handle(session, state_header, input),
    )
    .await
    .unwrap()
    .unwrap();
    let QueueReply::State(state) = serde_json::from_slice(reply.header.as_ref()).unwrap() else {
        panic!("wrong queue reply");
    };
    assert_eq!(state.request_id, id);
    assert_eq!(state.run_id, authority.ready.view.run_id);
    assert!(state.copies_complete);
    assert_eq!(state.state, RequestState::Queued);
    assert_eq!(shared.scheduler().usage(), baseline);
    // The protected lane cannot be used to smuggle an install payload.
    let forbidden = serde_json::to_vec(&request).unwrap();
    let control =
        AuthenticatedSession::for_test(local, local, group, Purpose::QueueControl, nabedra_rt::ids::uuid_v4());
    assert!(endpoint.preflight(&control, &forbidden).await.is_err());
    authority_source.release.notify_one();
    timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(reply);
    drop(fill);
    assert_eq!(shared.control_bytes_used(), 0);
}
