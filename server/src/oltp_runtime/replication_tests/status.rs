use super::*;
use crate::oltp::mvcc::CanonicalJson;
use crate::oltp_replication::service_rpc::metadata::status_service::{INSTALL_POINT_MANIFEST, Request};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp_runtime::{bootstrap::LocalServices, worker_endpoint::AuthorityReader},
};
use std::sync::atomic::AtomicBool;

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct StaleReadHint {
    endpoint: Arc<StatusEndpoint>,
    block_next_probe: AtomicBool,
    blocked_probe: AtomicBool,
    release_probe: tokio::sync::Notify,
    block_next_read: AtomicBool,
    blocked_read: AtomicBool,
    release_read: tokio::sync::Notify,
}

impl Endpoint for StaleReadHint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        self.endpoint.preflight(session, header)
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let head: Header = serde_json::from_slice(header.as_slice()).unwrap();
            if head.method == Request::Current.method() {
                if head.placement_generation.is_some()
                    && self.block_next_probe.swap(false, Ordering::Relaxed)
                {
                    self.blocked_probe.store(true, Ordering::Relaxed);
                    self.release_probe.notified().await;
                } else if head.placement_generation.is_none()
                    && self.blocked_probe.load(Ordering::Relaxed)
                {
                    self.release_probe.notify_one();
                }
            }
            if head.placement_generation.is_some()
                && head.method.can_repeat_read()
                && self.block_next_read.swap(false, Ordering::Relaxed)
            {
                self.blocked_read.store(true, Ordering::Relaxed);
                self.release_read.notified().await;
            } else if head.placement_generation.is_none()
                && self.blocked_read.load(Ordering::Relaxed)
            {
                self.release_read.notify_one();
            }
            self.endpoint.handle(session, header, sections).await
        })
    }
}

/// O07/O10/O11. The direct Status RPC replays an old draft at every
/// prefix and validates the original page before returning a sealed ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o07_o10_o11_point_manifest_replays_legacy_prefixes_and_rejects_changed_plan() {
    nabedra_rt::time::timeout(Duration::from_secs(40), async {
        let directory =
            Directory(std::env::temp_dir().join(format!("point-status-{}", nabedra_rt::ids::uuid_v4())));
        let services = LocalServices::open(
            &directory.0,
            "point_manifest",
            WorkerResources {
                max_threads: 1,
                ..Default::default()
            },
            &ExecutionConfig::default(),
            false,
        )
        .await
        .unwrap();
        let (node, endpoint) = services.status_endpoint_for_test().await.unwrap();
        let endpoint = Arc::new(StaleReadHint {
            endpoint,
            block_next_probe: AtomicBool::new(true),
            blocked_probe: AtomicBool::new(false),
            release_probe: tokio::sync::Notify::new(),
            block_next_read: AtomicBool::new(false),
            blocked_read: AtomicBool::new(false),
            release_read: tokio::sync::Notify::new(),
        });
        services
            .manager
            .replace_service_for_test(services.status.identity(), node, endpoint.clone())
            .await
            .unwrap();
        let authority = services
            .control
            .current(&services.scope)
            .await
            .unwrap()
            .ready
            .view
            .authority();
        let client = StatusClient::new(
            services.status.identity().clone(),
            services.shared.clone(),
            services.manager.client(),
            services.control_client.clone(),
            ServiceAction::Owner,
        )
        .unwrap();
        let routes = services
            .control_client
            .replica_routes(client.identity())
            .await
            .unwrap();
        // O06/O14: a stalled probe must give way to the ordinary read.
        // The probe can finish only once that unmarked read reaches Status.
        assert_eq!(client.current_authority().await.unwrap(), Some(authority));
        assert!(endpoint.blocked_probe.load(Ordering::Relaxed));
        assert_eq!(routes.hinted_receiver(node), None);
        services.control_client.invalidate_replica_routes(&routes);
        let routes = services
            .control_client
            .replica_routes(client.identity())
            .await
            .unwrap();
        assert_eq!(client.current_authority().await.unwrap(), Some(authority));
        assert_eq!(routes.hinted_receiver(node), Some(services.local));
        // O06/O14: an unavailable hinted receiver must trigger a fresh
        // routing pass within the same read, even with unchanged membership.
        endpoint.block_next_read.store(true, Ordering::Relaxed);
        assert!(
            client
                .inspect_transaction(TxnId {
                    request_id: nabedra_rt::ids::uuid_v4(),
                    attempt: 1,
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(endpoint.blocked_read.load(Ordering::Relaxed));
        let read_ts = ReadTs::new(1).unwrap();
        let plan = ParticipantPlan {
            group: GroupIdentity {
                cluster_id: client.identity().cluster_id,
                database: client.identity().database.clone(),
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Data {
                    table_id: nabedra_rt::ids::uuid_v4(),
                    shard_id: nabedra_rt::ids::uuid_v4(),
                },
                group_format_version: client.identity().group_format_version,
            },
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts,
            write_root: Digest::from_bytes([42; 32]),
            write_count: 1,
            page_count: 1,
        };
        let result = CanonicalJson::from_serializable(&services.shared, &1_u64, 1024).unwrap();
        for prefix in 0..=3 {
            let request_id = nabedra_rt::ids::uuid_v4();
            let digest = Digest::from_bytes([45; 32]);
            client
                .reserve_identity(authority, request_id, digest, RequestKind::Mutation)
                .await
                .unwrap();
            let txn = TxnId {
                request_id,
                attempt: 1,
            };
            let request = RequestBinding {
                run_id: authority.run_id,
                request_id,
                digest,
                accepted_context: Digest::from_bytes([46; 32]),
            };
            let point = |plan: ParticipantPlan| PointManifest {
                authority,
                txn,
                request,
                read_ts,
                plan,
                result: result.clone(),
            };

            let mut previous_manifest = None;
            if prefix >= 1 {
                let draft = client
                    .begin_manifest(authority, txn, request, read_ts, 1, result.clone())
                    .await
                    .unwrap();
                if prefix >= 2 {
                    client
                        .append_manifest_page(
                            authority,
                            txn,
                            0,
                            draft.initial_root,
                            Page::new(vec![plan.clone()]).unwrap(),
                        )
                        .await
                        .unwrap();
                }
                if prefix == 3 {
                    previous_manifest =
                        Some(client.seal_manifest(authority, txn).await.unwrap());
                }
            }

            let installed = client
                .try_install_point_manifest(point(plan.clone()))
                .await
                .unwrap()
                .expect("Status replica supports the combined request");
            let (manifest, result_digest) = installed;
            assert!(previous_manifest.is_none_or(|old| old == manifest));
            assert_eq!(
                result_digest,
                MutationOutcome::from_success(&result)
                    .unwrap()
                    .digest()
                    .unwrap()
            );
            assert_eq!(
                client
                    .try_install_point_manifest(point(plan.clone()))
                    .await
                    .unwrap(),
                Some(installed),
            );

            let mut changed = plan.clone();
            changed.write_root = Digest::from_bytes([43; 32]);
            let error = client
                .try_install_point_manifest(point(changed))
                .await
                .unwrap_err();
            assert_eq!(error.outcome, Outcome::Unknown);
            let page = client.manifest_page(txn, manifest, None).await.unwrap();
            assert_eq!(&*page.items, std::slice::from_ref(&plan));
            assert!(page.done);
        }

        // The legacy decoder's unknown-variant refusal is an
        // authenticated embedded error, distinct from a bad reply.
        let routes = services
            .control_client
            .replica_routes(client.identity())
            .await
            .unwrap();
        let member = &routes.members[0];
        let unsupported = services
            .shared
            .copy_bytes_in(ByteClass::Progress, br#"{"UnsupportedPointManifest":{}}"#)
            .unwrap();
        assert!(matches!(
            client.client_for_test().call_member(
                ServiceAction::Owner,
                Method::StatusCall { operation: INSTALL_POINT_MANIFEST, mutation: true },
                &Some(unsupported),
                member.node_id,
                member.worker_id.into(),
            ).await,
            Ok(Err(error)) if error == invalid(),
        ));
        services.shutdown().await.unwrap();
    })
    .await
    .expect("Point manifest service watchdog");
}
