use crate::{
    oltp::pool::ByteClass,
    oltp_replication::{
        authority::control::{tests::*, *},
        queue_access::AuthorityReader,
        types::*,
    },
};
use std::sync::{Arc, atomic::AtomicU64};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c07_q06_control_authorized_terminal_metadata_progresses_with_query_slot_and_data_wire_full()
 {
    use crate::{
        oltp::pool::ExecutionClass,
        oltp_replication::multiplex::{AuthenticatedSession, ChargedSections, Endpoint, Purpose},
        oltp_runtime::{
            replica::*,
            worker_endpoint::{QueueEndpoint, QueueReply, QueueRequest},
        },
        request_queue::QueueLimits,
    };
    use nabedra_rt::time::{Duration, timeout};
    let directory = Directory::new();
    let (scope, pool) = identities();
    let worker_id = pool.worker_id;
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let enrollment = worker(worker_id.as_u128());
    let local = WorkerIdentity::from(&enrollment);
    fixture
        .apply(ControlCommand::Enroll {
            operation_id: nabedra_rt::ids::uuid_v4(),
            expected: None,
            next: enrollment,
            authorization: authorization(),
            previous_process: None,
        })
        .await;
    let reader = fixture.enroll(2).await;
    fixture.add_ready(local, &mut Vec::new()).await;
    let timestamps = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 2,
                worker_id: reader.worker_id,
                pool_id: nabedra_rt::ids::uuid_v4(),
            }],
        )
        .await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model,
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
    };
    let authority = handle.current(&queue_scope).await.unwrap();
    let shared = fixture.machine.store().resources().clone();
    let mut replica = WorkerReplica::new(
        shared.clone(),
        queue_scope.clone(),
        local,
        authority,
        [local],
        QueueLimits {
            max_pending_requests: 4,
            max_queue_bytes_per_replica: 1 << 20,
            max_retained_result_bytes_per_worker: 9 << 20,
            result_retention_ms: 1000,
        },
    )
    .unwrap();
    let id = nabedra_rt::ids::uuid_v4();
    let body =
        format!(r#"{{"request_id":"{id}","operation":"table.list","payload":{{}}}}"#).into_bytes();
    let digest = {
        use sha2::{Digest as _, Sha256};
        Digest::from_bytes(Sha256::digest(&body).into())
    };
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: scope.database.clone(),
            principal: Digest::from_bytes([3; 32]),
            policy_revision: 1,
            ingress_id: local.worker_id,
            ingress_incarnation: local.incarnation,
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: PinIdentity {
                run_id: authority.ready.view.run_id,
                request_id: id,
                digest,
                nonce: nabedra_rt::ids::uuid_v4(),
            },
            storage_pin: StoragePin {
                pin_id: nabedra_rt::ids::uuid_v4(),
                read_ts: Timestamp::new(1).unwrap(),
                allocator_epoch: 1,
            },
            catalog_root: Digest::from_bytes([4; 32]),
            schema_epoch: 1,
            routing_epoch: 1,
        }),
    };
    fn copy_local(replica: &mut WorkerReplica, authority: QueueAuthority, mut frame: RecordFrame) {
        loop {
            let ack = replica.install(authority, authority.owner, &frame).unwrap();
            match replica.acknowledge(authority, ack).unwrap() {
                Some(next) => frame = next,
                None => break,
            }
        }
    }
    let frame = replica.propose(authority, body, context, 1).unwrap();
    copy_local(&mut replica, authority, frame);
    let frame = replica.start(authority, id).unwrap();
    copy_local(&mut replica, authority, frame);
    let prefix = format!(r#"{{"request_id":"{id}","data":""#);
    let mut response = prefix.into_bytes();
    response.resize(crate::protocol::MAX_BODY_BYTES - 2, b'x');
    response.extend_from_slice(b"\"}");
    assert_eq!(response.len(), crate::protocol::MAX_BODY_BYTES);
    let input = replica.terminal_input(id).unwrap();
    // No queue lock is held during this native response validation/hash.
    let prepared = shared
        .execute(
            ExecutionClass::Query,
            931,
            (2 * response.len()) as u64,
            move || {
                PreparedTerminal::prepare(input, 200, response, 2)
                    .map_err(nabedra_protocol::QueueError::protocol)
            },
        )
        .await
        .unwrap();
    let frame = replica.finish_prepared(authority, prepared).unwrap();
    copy_local(&mut replica, authority, frame);
    let expected = replica.terminal_copy_statement(id).unwrap();
    let endpoint = QueueEndpoint::new(
        Arc::new(std::sync::Mutex::new(replica)),
        shared.clone(),
        queue_scope.clone(),
        handle,
        512 << 20,
    )
    .unwrap();
    let occupied_query = shared.prepare(ExecutionClass::Query).await.unwrap();
    let full_data = shared
        .allocate_bytes(
            ByteClass::Data,
            shared.config().control_bytes - shared.config().progress_bytes,
        )
        .unwrap();
    let header = shared
        .encode_in(
            ByteClass::Progress,
            &QueueRequest::TerminalState {
                scope: queue_scope,
                view: authority.ready.view,
                request_id: id,
                timestamps_group_id: timestamps.identity.group_id,
            },
            MAX_HEADER_BYTES,
        )
        .unwrap();
    let session =
        AuthenticatedSession::for_test(local, reader, scope, Purpose::QueueControl, nabedra_rt::ids::uuid_v4());
    let plan = timeout(
        Duration::from_secs(2),
        endpoint.preflight(&session, header.as_ref()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(plan.section_lengths, [0; 3]);
    assert_eq!(plan.decode_budget_bytes, 0);
    let sections =
        ChargedSections::for_test(&shared, ByteClass::Progress, [None, None, None], 0).unwrap();
    let reply = timeout(
        Duration::from_secs(2),
        endpoint.handle(session, header, sections),
    )
    .await
    .unwrap()
    .unwrap();
    let QueueReply::TerminalState { statement, .. } = serde_json::from_slice(reply.header.as_ref()).unwrap() else {
        panic!("terminal metadata reply")
    };
    assert_eq!(*statement, expected);
    drop((reply, full_data, occupied_query, endpoint));
}

