use super::*;
use crate::{
    config::WorkerResources, oltp::pool::SharedResourceConfig, oltp_replication::ReadySetHasher,
    request_queue::QueueLimits,
};
use std::sync::atomic::AtomicBool;
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c07_m02_blocked_authority_preflight_and_full_data_arena_leave_receipt_progress() {
    let shared = WorkerSharedResources::new(
        91,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap();
    let local = WorkerIdentity {
        worker_id: Uuid::new_v4(),
        incarnation: Uuid::new_v4(),
        certificate_sha256: Digest::from_bytes([1; 32]),
        enrollment_revision: 1,
    };
    let scope = QueueScope {
        cluster_id: Uuid::new_v4(),
        database: Text::try_from("default").unwrap(),
    };
    let mut root = ReadySetHasher::default();
    root.push(local).unwrap();
    let authority = QueueAuthority {
        ready: ReadyMembership {
            view: AuthorityView {
                run_id: Uuid::new_v4(),
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
    let id = Uuid::new_v4();
    let body =
        format!(r#"{{"request_id":"{id}","operation":"table.list","payload":{{}}}}"#).into_bytes();
    use sha2::{Digest as _, Sha256};
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: scope.database.clone(),
            principal: Digest::from_bytes([2; 32]),
            policy_revision: 1,
            ingress_id: Uuid::new_v4(),
            ingress_incarnation: Uuid::new_v4(),
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: PinIdentity {
                run_id: authority.ready.view.run_id,
                request_id: id,
                digest: Digest::from_bytes(Sha256::digest(&body).into()),
                nonce: Uuid::new_v4(),
            },
            storage_pin: StoragePin {
                pin_id: Uuid::new_v4(),
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
        group_id: Uuid::new_v4(),
        kind: GroupKind::Control,
        group_format_version: GROUP_FORMAT_VERSION,
    };
    let session = AuthenticatedSession::for_test(
        local,
        local,
        group.clone(),
        Purpose::QueueData,
        Uuid::new_v4(),
    );
    let waiting = {
        let endpoint = endpoint.clone();
        tokio::spawn(async move { endpoint.preflight(&session, &install_header).await })
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
        Uuid::new_v4(),
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
        AuthenticatedSession::for_test(local, local, group, Purpose::QueueControl, Uuid::new_v4());
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
