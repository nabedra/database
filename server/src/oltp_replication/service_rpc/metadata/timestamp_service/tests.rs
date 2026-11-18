use super::*;
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp::mvcc::catalog::CaptureRead,
    oltp_replication::authority::{
        status::{StatusHandle, StatusMachine},
        timestamp::TimestampMachine,
    },
    oltp_runtime::{
        bootstrap::LocalServices,
        replica::{AcceptedContext, OriginalAuthorization, QueueReadToken},
        worker_endpoint::{AuthorityReader, QueuePeerClient},
    },
};
use sha2::{Digest as _, Sha256};
use std::sync::OnceLock;

struct Directory(std::path::PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct DiscardReleaseReply {
    endpoint: Arc<TimestampEndpoint>,
    request_id: Uuid,
    discarded: OnceLock<(SealedTerminalProof, AppliedPosition)>,
}

impl Endpoint for DiscardReleaseReply {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        self.endpoint.preflight(session, bytes)
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let head = self.endpoint.parse(&session, bytes.as_slice())?;
            let discard = head.method
                == (Method::TimestampCall {
                    operation: 5,
                    mutation: true,
                })
                && self.discarded.get().is_none()
                && matches!(
                    serde_json::from_slice::<Request>(sections.section(0).unwrap().as_slice()).unwrap(),
                    Request::Release { statement } if statement.request.request_id == self.request_id
                );
            let response = self.endpoint.handle(session, bytes, sections).await?;
            if discard {
                let header: ReplyHeader =
                    serde_json::from_slice(response.header.as_slice()).unwrap();
                assert!(header.error.is_none());
                let proof =
                    serde_json::from_slice(response.sections[0].as_ref().unwrap().as_slice())
                        .unwrap();
                self.discarded.set(proof).unwrap();
                // Refuse the transport exchange after the genuine service
                // reply is ready, so the caller receives no release proof.
                return Err(unavailable());
            }
            Ok(response)
        })
    }
}

async fn begin_and_bind(
    services: &LocalServices,
    client: &TimestampClient,
    view: AuthorityView,
    id: Uuid,
) -> (Vec<u8>, AcceptedContext, StoragePin) {
    let body =
        format!(r#"{{"request_id":"{id}","operation":"table.list","payload":{{}}}}"#).into_bytes();
    let digest = Digest::from_bytes(Sha256::digest(&body).into());
    let applied = services
        .status
        .reserve_identity(view.authority(), id, digest, RequestKind::Read)
        .await
        .unwrap();
    let StatusReply::Identity(IdentityLookup::Reserved(reservation)) = applied.reply else {
        panic!("expected durable read reservation");
    };
    let identity = PinIdentity {
        run_id: view.run_id,
        request_id: id,
        digest,
        nonce: reservation.reservation_id,
    };
    let pin = client
        .begin_snapshot(view, identity.clone(), reservation)
        .await
        .unwrap();
    let catalog = services
        .catalog
        .snapshot(pin.read_ts, view.authority())
        .unwrap();
    let operation = crate::protocol::parse_request(&body).unwrap().operation;
    let CaptureRead::Captured(captured) = services
        .catalog
        .capture_lineage(&catalog, &operation)
        .await
        .unwrap()
    else {
        panic!("expected captured Catalog lineage");
    };
    let context = AcceptedContext {
        authorization: OriginalAuthorization {
            database: services.scope.database.clone(),
            principal: services.local.certificate_sha256,
            policy_revision: 1,
            ingress_id: services.local.worker_id,
            ingress_incarnation: services.local.incarnation,
            credential_expires_at_ms: None,
        },
        read: Some(QueueReadToken {
            identity: identity.clone(),
            storage_pin: pin,
            catalog_root: captured.catalog_root,
            schema_epoch: captured.schema_epoch,
            routing_epoch: captured.routing_epoch,
        }),
    };
    let request = RequestBinding {
        run_id: view.run_id,
        request_id: id,
        digest,
        accepted_context: serialized_digest(&context).unwrap(),
    };
    client
        .bind_snapshot_context(view, identity, pin, request)
        .await
        .unwrap();
    (body, context, pin)
}

async fn terminal_copy(
    services: &LocalServices,
    client: &TimestampClient,
    view: AuthorityView,
    id: Uuid,
) -> TerminalCopyStatement {
    let (body, context, _) = begin_and_bind(services, client, view, id).await;
    services.queue.admit(body, context, 1).await.unwrap();
    services.queue.start(id).await.unwrap();
    let response = format!(r#"{{"request_id":"{id}","data":[]}}"#).into_bytes();
    services.queue.finish(id, 200, response, 2).await.unwrap();
    services
        .replica
        .lock()
        .unwrap()
        .terminal_statement(id)
        .unwrap()
}

fn raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    }
}

// O09/O13/O14/O22/C07/F03/Q06/R03. Use the actual typed service, durable
// TSO/read pin and authenticated Ready queue copies in a local RF1 worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_release_recovers_durable_reply_while_foreground_snapshot_progresses() {
    tokio::time::timeout(Duration::from_secs(40), exercise_release_and_snapshot())
        .await
        .expect("timestamp release watchdog");
}

async fn exercise_release_and_snapshot() {
    let directory =
        Directory(std::env::temp_dir().join(format!("timestamp-release-{}", Uuid::new_v4())));
    let services = LocalServices::open(
        &directory.0,
        "release",
        WorkerResources {
            max_threads: 1,
            ..Default::default()
        },
        &ExecutionConfig {
            max_pending_requests: 16,
            max_queue_bytes_per_replica: 1 << 20,
            max_retained_result_bytes_per_worker: 1 << 20,
            ..Default::default()
        },
        false,
    )
    .await
    .unwrap();
    let group = services.timestamps.identity();
    let ready = services
        .control_client
        .current(&services.scope)
        .await
        .unwrap()
        .ready;
    let routes = services.control_client.replica_routes(group).await.unwrap();
    let [member] = routes.members.as_slice() else {
        panic!("expected a sole Timestamp voter");
    };
    let node = member.node_id;
    let pool = services.pools.get(member.pool_id).unwrap().unwrap();
    let running = services
        .manager
        .start_timestamps(
            pool.group(group.clone()).await.unwrap(),
            member.clone(),
            [(node, member.clone())].into_iter().collect(),
            raft_config(),
        )
        .await
        .unwrap();
    let status = services.status.identity();
    let status_routes = services
        .control_client
        .replica_routes(status)
        .await
        .unwrap();
    let [status_member] = status_routes.members.as_slice() else {
        panic!("expected a sole Status voter");
    };
    let status_node = status_member.node_id;
    let status_pool = services.pools.get(status_member.pool_id).unwrap().unwrap();
    let status_running = services
        .manager
        .start_status(
            status_pool.group(status.clone()).await.unwrap(),
            status_member.clone(),
            [(status_node, status_member.clone())].into_iter().collect(),
            raft_config(),
        )
        .await
        .unwrap();
    drop(status_routes);
    drop(routes);
    let handle = TimestampHandle::new(
        TimestampMachine::new(running.state.store().clone()).unwrap(),
        running.clone(),
    );
    let transport = services.manager.client();
    let topology = StatusTopology::new(vec![status.clone()]).unwrap();
    let status_handle = StatusHandle::new(
        StatusMachine::new(status_running.state.store().clone()).unwrap(),
        status_running.clone(),
    );
    services
        .manager
        .replace_service_for_test(
            status,
            status_node,
            StatusEndpoint::new(
                status_handle,
                status_node,
                services.control_client.clone(),
                transport.clone(),
                group.clone(),
                topology.clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .install_release_reader(
            services.control_client.clone(),
            Arc::new(
                QueuePeerClient::new(
                    transport.clone(),
                    services.shared.clone(),
                    services.control.identity().clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let lost_id = Uuid::new_v4();
    let endpoint = Arc::new(DiscardReleaseReply {
        endpoint: TimestampEndpoint::new(
            handle.clone(),
            node,
            services.control_client.clone(),
            transport.clone(),
            topology,
        )
        .unwrap(),
        request_id: lost_id,
        discarded: OnceLock::new(),
    });
    services
        .manager
        .bind_service(group, node, endpoint.clone())
        .unwrap();
    let client = TimestampClient::new(
        group.clone(),
        services.shared.clone(),
        transport.clone(),
        services.control_client.clone(),
        ServiceAction::Owner,
    )
    .unwrap();

    let lost = terminal_copy(&services, &client, ready.view, lost_id).await;
    let lost_reply = client
        .release(lost.clone())
        .await
        .err()
        .expect("lost application reply");
    assert_eq!(lost_reply.outcome, Outcome::Unknown);
    let stored = endpoint
        .discarded
        .get()
        .expect("durable proof from the lost reply");
    let slow = terminal_copy(&services, &client, ready.view, Uuid::new_v4()).await;
    let fresh = terminal_copy(&services, &client, ready.view, Uuid::new_v4()).await;
    let (reached, resume) = handle.hold_release_proof_for_test(slow.request.request_id);
    let slow_release = client.release(slow.clone());
    tokio::pin!(slow_release);
    tokio::select! {
        _ = reached.notified() => {}
        _ = &mut slow_release => panic!("release must reach the proof gate"),
    }
    let blocked = transport.hold_outgoing_for_test(TrafficClass::QueueControl);
    tokio::select! {
        _ = &mut slow_release => panic!("the original release must stay in progress"),
        _ = async {
            let shared_client = (*client).clone();
            let second_call = client.releases.try_acquire().unwrap();
            let refused = shared_client.release(fresh.clone()).await.err().expect("outgoing release limit");
            assert_eq!(refused.code, ErrorCode::Overloaded);
            assert_eq!(refused.outcome, Outcome::NotApplied);
            assert_eq!(refused.message, "timestamp terminal release calls already in progress");
            let (_, _, next_pin) = begin_and_bind(&services, &client, ready.view, Uuid::new_v4()).await;
            assert_eq!(next_pin.read_ts.get(), fresh.storage_pin.read_ts.get() + 1);
            drop(second_call);
            let refused = client.release(fresh.clone()).await.err().expect("one native proof in progress");
            assert_eq!(refused.code, ErrorCode::Overloaded);
            assert_eq!(refused.outcome, Outcome::NotApplied);
            assert_eq!(refused.message, "timestamp terminal copy proof already in progress");
            let recovered = client.release(lost.clone()).await.unwrap();
            assert_eq!(&recovered.wire_parts(), stored);
        } => {}
    }
    drop(blocked);
    resume.notify_one();
    let released = slow_release.await.unwrap();
    assert_eq!(released.statement(), &slow);
    let blocked = transport.hold_outgoing_for_test(TrafficClass::QueueControl);
    let missing = client
        .release(fresh.clone())
        .await
        .err()
        .expect("fresh release needs a Ready ACK");
    assert_eq!(missing.code, ErrorCode::Overloaded);
    assert_eq!(missing.outcome, Outcome::NotApplied);
    drop(blocked);
    assert_eq!(
        client.release(fresh.clone()).await.unwrap().statement(),
        &fresh
    );
    assert_eq!(client.releases.available_permits(), RELEASE_CALLS);

    let blocked = transport.hold_outgoing_for_test(TrafficClass::QueueControl);
    let mut unknown = lost.clone();
    unknown.pin_identity.nonce = Uuid::new_v4();
    let absent = client
        .release(unknown)
        .await
        .err()
        .expect("unknown pin must fail");
    assert_eq!(absent.code, ErrorCode::StorageUnavailable);
    assert_eq!(absent.outcome, Outcome::Unknown);
    let mut different = lost.clone();
    different.response_hash = Digest::from_bytes([99; 32]);
    let mismatch = client
        .release(different)
        .await
        .err()
        .expect("changed response must fail");
    assert_eq!(mismatch.code, ErrorCode::ValidationError);
    assert_eq!(mismatch.outcome, Outcome::NotApplied);
    let mut different = lost.clone();
    different.terminal_digest = Some(Digest::from_bytes([97; 32]));
    let mismatch = client
        .release(different)
        .await
        .err()
        .expect("changed terminal digest must fail");
    assert_eq!(mismatch.code, ErrorCode::ValidationError);
    assert_eq!(mismatch.outcome, Outcome::NotApplied);
    let mut different = lost.clone();
    different.ready.ready_root = Digest::from_bytes([98; 32]);
    let mismatch = client
        .release(different)
        .await
        .err()
        .expect("changed Ready set must fail");
    assert_eq!(mismatch.code, ErrorCode::ValidationError);
    assert_eq!(mismatch.outcome, Outcome::NotApplied);

    let mut next = ready;
    next.view.membership_revision += 1;
    handle
        .fence_ready(Uuid::new_v4(), ready, next)
        .await
        .unwrap();
    let stale = client
        .release(lost.clone())
        .await
        .err()
        .expect("old release revision must fail");
    assert_eq!(stale.code, ErrorCode::ValidationError);
    assert_eq!(stale.outcome, Outcome::NotApplied);
    let mut refreshed = lost;
    refreshed.ready = next;
    refreshed.terminal_version += 1;
    let missing = client
        .release(refreshed)
        .await
        .err()
        .expect("new revision needs fresh Ready proof");
    assert_eq!(missing.code, ErrorCode::StorageUnavailable);
    assert_eq!(missing.outcome, Outcome::Unknown);
    drop(blocked);
    services.shutdown().await.unwrap();
}
