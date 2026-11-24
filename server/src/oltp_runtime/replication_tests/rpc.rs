use nabedra_resources::config::{ExecutionConfig, WorkerResources};
use super::*;
use serde::Deserialize;
use crate::{
    oltp::mvcc::{CanonicalJson, store::initial_root},
    oltp_replication::authority::{
        control::ControlRoutes,
        status::{
            IdentityLookup, IdentityReservation, MutationOutcome, RequestKind, StatusReply,
            VerifiedStatusDecision,
        },
        timestamp::TimestampService,
    },
    oltp_runtime::{bootstrap::LocalServices, worker_endpoint::AuthorityReader},
};

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Routes {
    manager: Arc<crate::oltp_replication::manager::HostedGroupManager>,
    control: Uuid,
}
impl ControlRoutes for Routes {
    fn preferred(&self) -> Option<Uuid> {
        self.manager
            .leader_candidates(self.control)
            .ok()?
            .first()
            .map(|member| member.worker_id)
    }
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(self
            .manager
            .leader_candidates(self.control)?
            .into_iter()
            .map(|member| member.worker_id)
            .filter(|worker| after.is_none_or(|after| *worker > after))
            .min())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum SnapshotRequest {
    Begin {
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    },
}

struct ConvergingTimestamp {
    service: Arc<dyn TimestampService>,
    control: Arc<ControlClient>,
    node: u64,
    preflights: AtomicU64,
    legacy_placement_probes: AtomicU64,
    attempts: std::sync::Mutex<Vec<(WorkerIdentity, Uuid, Digest)>>,
    applied: std::sync::OnceLock<StoragePin>,
}
impl Endpoint for ConvergingTimestamp {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let head = parse(session, self.service.identity(), self.node, bytes)?;
            if head.placement_generation.is_some() {
                assert_eq!(
                    head.method,
                    Method::TimestampCall {
                        operation: 0,
                        mutation: false
                    }
                );
                self.legacy_placement_probes.fetch_add(1, Ordering::Relaxed);
                // An older worker's strict service header parser rejects the marker.
                return Err(invalid());
            }
            self.preflights.fetch_add(1, Ordering::Relaxed);
            assert_eq!(
                head.method,
                Method::TimestampCall {
                    operation: 1,
                    mutation: true
                }
            );
            authorize(&self.control, session, &head).await?;
            Ok(ReceivePlan {
                class: head.method.class(),
                section_lengths: [head.payload_bytes, 0, 0],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            use sha2::{Digest as _, Sha256};
            let head = parse(
                &session,
                self.service.identity(),
                self.node,
                bytes.as_slice(),
            )?;
            assert_eq!(head.action, ServiceAction::Owner);
            let raw = sections.section(0).ok_or_else(invalid)?.as_slice();
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push((
                    session.remote(),
                    session.nonce(),
                    Digest::from_bytes(Sha256::digest(raw).into()),
                ));
                attempts.len()
            };
            let shared = self.service.resources();
            let result = match attempt {
                // This is the sole registered replica. The first complete
                // route pass finds no leader before reaching proposal.
                1 => Err(unavailable()),
                2 | 4 => {
                    let SnapshotRequest::Begin {
                        view,
                        identity,
                        reservation,
                    } = serde_json::from_slice(raw).unwrap();
                    let pin = self
                        .service
                        .begin_snapshot(view, identity, reservation)
                        .await?;
                    if attempt == 2 {
                        // The proposal applied. The transport refusal carries
                        // no application reply attesting to the result.
                        self.applied.set(pin).unwrap();
                        return Err(ProtocolError::new(
                            ErrorCode::Overloaded,
                            "injected transport overload after applying snapshot",
                            Outcome::NotApplied,
                        ));
                    } else {
                        Ok(shared.encode_in(ByteClass::Progress, &pin, 4096)?)
                    }
                }
                3 => Err(unavailable()),
                5 => Err(ProtocolError::new(
                    ErrorCode::Overloaded,
                    "worker outgoing lane full (ServiceMutation)",
                    Outcome::NotApplied,
                )),
                _ => {
                    return Err(ProtocolError::new(
                        ErrorCode::Overloaded,
                        "worker handler lane full",
                        Outcome::NotApplied,
                    ));
                }
            };
            reply(shared, &session, self.node, result).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o14_timestamp_service_retries_same_snapshot_after_leader_convergence() {
    nabedra_rt::time::timeout(Duration::from_secs(40), async {
        let directory =
            Directory(std::env::temp_dir().join(format!("timestamp-service-{}", nabedra_rt::ids::uuid_v4())));
        let services = LocalServices::open(
            &directory.0,
            "convergence",
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
        let current = services.control.current(&services.scope).await.unwrap();
        let view = current.ready.view;
        let reserve = |request_id| {
            services.status.reserve_identity(
                view.authority(),
                request_id,
                Digest::from_bytes([19; 32]),
                RequestKind::Read,
            )
        };
        let initial = reserve(nabedra_rt::ids::uuid_v4()).await.unwrap();
        let StatusReply::Identity(IdentityLookup::Reserved(reservation)) = initial.reply else {
            panic!("expected a durable read reservation");
        };
        let identity = PinIdentity {
            run_id: view.run_id,
            request_id: reservation.request_id,
            digest: reservation.digest,
            nonce: reservation.reservation_id,
        };
        let group = services.timestamps.identity();
        let routes = services.control_client.replica_routes(group).await.unwrap();
        let [member] = routes.members.as_slice() else {
            panic!("expected the sole timestamp voter");
        };
        let endpoint = Arc::new(ConvergingTimestamp {
            service: services.timestamps.clone(),
            control: services.control_client.clone(),
            node: member.node_id,
            preflights: AtomicU64::new(0),
            legacy_placement_probes: AtomicU64::new(0),
            attempts: Default::default(),
            applied: Default::default(),
        });
        services
            .manager
            .bind_service(group, member.node_id, endpoint.clone())
            .unwrap();
        drop(routes);
        let client = |action| {
            TimestampClient::new(
                group.clone(),
                services.shared.clone(),
                services.manager.client(),
                services.control_client.clone(),
                action,
            )
            .unwrap()
        };
        let owner = client(ServiceAction::Owner);
        let transport = services.manager.client();
        let held = transport.hold_outgoing_for_test(TrafficClass::ServiceMutation);
        let begin = owner.begin_snapshot(view, identity.clone(), reservation);
        tokio::pin!(begin);
        tokio::select! {
            biased;
            result = &mut begin => panic!("snapshot completed before the outgoing lane opened: {result:?}"),
            _ = transport.outgoing_lane_full_for_test(TrafficClass::ServiceMutation) => {
                assert_eq!(endpoint.preflights.load(Ordering::Relaxed), 0);
                drop(held);
            }
        }
        let pin = begin.await.unwrap();
        assert!(endpoint.legacy_placement_probes.load(Ordering::Relaxed) >= 1);
        assert_eq!(Some(&pin), endpoint.applied.get());
        {
            let attempts = endpoint.attempts.lock().unwrap();
            assert_eq!(attempts.len(), 4);
            assert!(
                attempts.iter().all(
                    |(remote, _, digest)| *remote == services.local && *digest == attempts[0].2
                )
            );
            let nonces: std::collections::BTreeSet<_> =
                attempts.iter().map(|(_, nonce, _)| *nonce).collect();
            assert_eq!(nonces.len(), attempts.len());
        }
        let next = reserve(nabedra_rt::ids::uuid_v4()).await.unwrap();
        let StatusReply::Identity(IdentityLookup::Reserved(next)) = next.reply else {
            panic!("expected another durable read reservation");
        };
        let next_pin = services
            .timestamps
            .begin_snapshot(
                view,
                PinIdentity {
                    run_id: view.run_id,
                    request_id: next.request_id,
                    digest: next.digest,
                    nonce: next.reservation_id,
                },
                next,
            )
            .await
            .unwrap();
        assert_eq!(next_pin.read_ts.get(), pin.read_ts.get() + 1);
        let refusal = owner
            .begin_snapshot(view, identity.clone(), reservation)
            .await
            .unwrap_err();
        assert_eq!(refusal.code, ErrorCode::Overloaded);
        assert_eq!(refusal.outcome, Outcome::NotApplied);
        assert_eq!(endpoint.attempts.lock().unwrap().len(), 5);
        let unknown = owner
            .begin_snapshot(view, identity.clone(), reservation)
            .await
            .unwrap_err();
        assert_eq!(unknown.code, ErrorCode::StorageUnavailable);
        assert_eq!(unknown.outcome, Outcome::Unknown);
        let attempts_after_overload = endpoint.preflights.load(Ordering::Relaxed);
        assert!(attempts_after_overload > 6);
        let unauthorized = client(ServiceAction::ApiIngress)
            .begin_snapshot(view, identity, reservation)
            .await
            .unwrap_err();
        assert_eq!(unauthorized.code, ErrorCode::Unauthorized);
        assert_eq!(unauthorized.outcome, Outcome::NotApplied);
        assert_eq!(
            endpoint.preflights.load(Ordering::Relaxed),
            attempts_after_overload + 1
        );
        services.shutdown().await.unwrap();
    })
    .await
    .expect("timestamp convergence watchdog");
}

/// O10/O15: the registered mTLS service returns the exact durable decision
/// after ReadIndex. A matching wire shape with altered outcome is not a proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o10_o15_status_service_uses_actual_control_routes_and_rejects_forged_outcome() {
    nabedra_rt::time::timeout(std::time::Duration::from_secs(40), async {
        let directory =
            Directory(std::env::temp_dir().join(format!("status-service-{}", nabedra_rt::ids::uuid_v4())));
        let services = LocalServices::open(
            &directory.0,
            "service",
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
        let current = services.control.current(&services.scope).await.unwrap();
        let authority = current.ready.view.authority();
        let request_id = nabedra_rt::ids::uuid_v4();
        let digest = Digest::from_bytes([45; 32]);
        let applied = services
            .status
            .reserve_identity(authority, request_id, digest, RequestKind::Mutation)
            .await
            .unwrap();
        let StatusReply::Identity(IdentityLookup::Reserved(reservation)) = applied.reply else {
            panic!("reserved identity")
        };
        let identity = PinIdentity {
            run_id: authority.run_id,
            request_id,
            digest,
            nonce: reservation.reservation_id,
        };
        let pin = services
            .timestamps
            .begin_snapshot(current.ready.view, identity.clone(), reservation)
            .await
            .unwrap();
        let request = RequestBinding {
            run_id: authority.run_id,
            request_id,
            digest,
            accepted_context: Digest::from_bytes([46; 32]),
        };
        services
            .timestamps
            .bind_snapshot_context(current.ready.view, identity, pin, request)
            .await
            .unwrap();
        let txn = TxnId {
            request_id,
            attempt: 1,
        };
        let draft = services
            .status
            .begin_manifest(
                authority,
                txn,
                request,
                pin.read_ts,
                1,
                CanonicalJson::from_serializable(&services.shared, &serde_json::Value::Null, 1024)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut plan = ParticipantPlan {
            group: services.catalog.participant().identity().clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: pin.read_ts,
            write_root: Digest::from_bytes([0; 32]),
            write_count: 0,
            page_count: 0,
        };
        plan.write_root = initial_root(txn, &plan).unwrap();
        services
            .status
            .append_manifest_page(
                authority,
                txn,
                0,
                draft.initial_root,
                Page::new(vec![plan]).unwrap(),
            )
            .await
            .unwrap();
        let manifest = services.status.seal_manifest(authority, txn).await.unwrap();
        let control_group = services.control.identity().clone();
        let control = ControlClient::new(
            services.manager.client(),
            services.shared.clone(),
            control_group.clone(),
            Arc::new(Routes {
                manager: services.manager.clone(),
                control: control_group.group_id,
            }),
        )
        .unwrap();
        let client = StatusProofClient::new(
            services.status.identity().clone(),
            services.shared.clone(),
            services.manager.client(),
            control.clone(),
            ServiceAction::Owner,
        )
        .unwrap();
        assert!(client.decision(txn, manifest).await.unwrap().is_none());
        let error = ProtocolError::new(
            ErrorCode::Conflict,
            "known durable conflict",
            Outcome::NotApplied,
        );
        services
            .status
            .abort(authority, txn, AbortReason::Conflict, error.clone())
            .await
            .unwrap();
        let decision = client.decision(txn, manifest).await.unwrap().unwrap();
        assert_eq!(decision.statement().request, request);
        assert_eq!(decision.outcome().unwrap().result().unwrap_err(), error);
        let common = Client::new(
            services.status.identity().clone(),
            services.shared.clone(),
            services.manager.client(),
            control,
            ServiceAction::Owner,
        )
        .unwrap();
        let response = common
            .read(Method::StatusDecision { txn, manifest })
            .await
            .unwrap();
        let context = response.status_context();
        let forged = MutationOutcome::failure(ProtocolError::new(
            ErrorCode::Conflict,
            "forged conflict",
            Outcome::NotApplied,
        ))
        .unwrap();
        assert!(
            VerifiedStatusDecision::from_authenticated(
                context,
                decision.statement().clone(),
                forged,
                txn,
                manifest
            )
            .is_err()
        );
        // A guarded planning error has a permanent failure identity but no
        // transaction manifest. Cleanup must use that exact failure, not infer
        // settlement from the missing transaction and not mask it as Unknown.
        let failed_request = RequestBinding {
            run_id: authority.run_id,
            request_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([71; 32]),
            accepted_context: Digest::from_bytes([72; 32]),
        };
        services
            .status
            .reserve_identity(
                authority,
                failed_request.request_id,
                failed_request.digest,
                RequestKind::Mutation,
            )
            .await
            .unwrap();
        services
            .status
            .terminal_failure(
                authority,
                failed_request,
                ProtocolError::new(
                    ErrorCode::Conflict,
                    "duplicate key before preparation",
                    Outcome::NotApplied,
                ),
            )
            .await
            .unwrap();
        let failed_txn = TxnId {
            request_id: failed_request.request_id,
            attempt: 1,
        };
        let expected = services
            .status
            .failure_settled_proof(failed_txn, failed_request)
            .await
            .unwrap()
            .into_sealed();
        let statement = expected.statement().clone();
        let observed = client
            .settlement(failed_txn, statement.manifest)
            .await
            .unwrap()
            .into_sealed();
        assert_eq!(observed.statement(), &statement);
        let mut forged_manifest = statement.manifest;
        forged_manifest.digest = Digest::from_bytes([73; 32]);
        assert!(
            client
                .settlement(failed_txn, forged_manifest)
                .await
                .is_err()
        );
        assert!(
            services
                .status
                .begin_manifest(
                    authority,
                    failed_txn,
                    failed_request,
                    pin.read_ts,
                    1,
                    CanonicalJson::from_serializable(
                        &services.shared,
                        &serde_json::Value::Null,
                        1024
                    )
                    .unwrap()
                )
                .await
                .is_err()
        );
        assert_eq!(
            client
                .settlement(failed_txn, statement.manifest)
                .await
                .unwrap()
                .into_sealed()
                .statement(),
            &statement
        );
        services.shutdown().await.unwrap();
    })
    .await
    .expect("Status service watchdog");
}
