use super::*;
use crate::oltp_replication::authority::{
    control::{group_root, group_seed, group_step},
    timestamp::split::SplitVoter,
};
use crate::oltp_runtime::replica::scoped::ScopedPlacement;
use serde_json::{Value, json};
use std::path::PathBuf;

struct TestDirectory(PathBuf);
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn body(operation: &str, payload: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "request_id": nabedra_rt::ids::uuid_v4(), "operation": operation, "payload": payload,
    }))
    .unwrap()
}

async fn run(runtime: &Arc<WorkerRuntime>, operation: &str, payload: Value) -> Value {
    let accepted = runtime.admit(body(operation, payload)).await.unwrap();
    let reply = runtime.wait(&accepted).await.unwrap();
    let envelope: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(reply.status, 200, "{operation}: {envelope}");
    envelope["data"].clone()
}

async fn pin(runtime: &WorkerRuntime, operation: &str, payload: Value) -> PinnedCatalog {
    let prepared = runtime
        .admission
        .prepare(
            body(operation, payload),
            runtime.authorization.clone(),
            now(),
        )
        .await
        .unwrap();
    let ReservationResult::Reserved(reserved) =
        runtime.admission.reserve(prepared, None).await.unwrap()
    else {
        panic!("fixture requires a new identity");
    };
    let storage = runtime.admission.begin_snapshot(&reserved).await.unwrap();
    let current = runtime
        .services
        .control
        .current(&runtime.services.scope)
        .await
        .unwrap();
    runtime
        .services
        .catalog
        .snapshot(storage.read_ts, current.ready.view.authority())
        .unwrap()
}

fn get(table_id: Uuid, key: protocol::Key) -> protocol::Operation {
    protocol::Operation::Get(protocol::KeyPayload { table_id, key })
}

// Model three Control-supplied voters. The native fixture below has RF1 and
// exercises its own authenticated storage services separately.
fn placement(group: &GroupIdentity, local: WorkerIdentity) -> SplitSourcePlacement {
    let identities = [
        local,
        WorkerIdentity {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: Digest::from_bytes([2; 32]),
            enrollment_revision: 2,
        },
        WorkerIdentity {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: Digest::from_bytes([3; 32]),
            enrollment_revision: 3,
        },
    ];
    let voters = identities
        .into_iter()
        .zip(1..=3)
        .map(|(worker, node_id)| SplitVoter {
            member: ReplicaMember {
                node_id,
                worker_id: worker.worker_id,
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            worker,
            registered_pool_generation: None,
        })
        .collect::<Vec<_>>();
    SplitSourcePlacement {
        record: GroupRecord {
            identity: group.clone(),
            generation: nabedra_rt::ids::uuid_v4(),
            replica_count: 3,
            replica_root: group_root(
                3,
                voters
                    .iter()
                    .fold(group_seed(), |root, voter| group_step(root, &voter.member)),
            ),
            membership_position: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
        },
        voters: Page::new(voters).unwrap(),
        domains: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o03_o16_c02_scoped_point_route_preserves_pinned_table_and_exact_placement() {
    nabedra_rt::time::timeout(std::time::Duration::from_secs(90), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-scoped-point-{}", nabedra_rt::ids::uuid_v4())),
        );
        let runtime = WorkerRuntime::open(
            &directory.0,
            "scoped_point",
            "scoped-point-data",
            ExecutionConfig {
                max_pending_requests: 32,
                max_queue_bytes_per_replica: 8 << 20,
                max_retained_result_bytes_per_worker: 8 << 20,
                ..Default::default()
            },
            WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 2 << 30,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let schema = |key_type| {
            json!({
                "name": "items", "key": "id", "format": "parquet",
                "schema": {"id": key_type, "live": "bool"},
            })
        };
        let table = run(&runtime, "table.create", schema("int64")).await;
        let table_id = serde_json::from_value::<Uuid>(table["id"].clone()).unwrap();
        run(
            &runtime,
            "table.insert",
            json!({
                "table_id": table_id, "row": {"id": i64::MIN, "live": true},
            }),
        )
        .await;
        let pinned = pin(
            &runtime,
            "table.get",
            json!({"table_id": table_id, "key": i64::MIN}),
        )
        .await;
        let hit = get(table_id, protocol::Key::Int64(i64::MIN));
        let point = runtime
            .planner
            .point_route(&pinned, &hit)
            .await
            .unwrap()
            .unwrap();
        let route = point.shard();
        assert_eq!(route.group.cluster_id, runtime.services.scope.cluster_id);
        assert_eq!(route.group.database, runtime.services.scope.database);
        assert!(matches!(route.group.kind, GroupKind::Data {table_id: id, ..} if id == table_id));
        assert!(route.span.start.is_none() && route.span.end.is_none());
        let miss = get(table_id, protocol::Key::Int64(i64::MAX));
        let miss_route = runtime
            .planner
            .point_route(&pinned, &miss)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(miss_route.shard().group, route.group);
        assert!(
            run(
                &runtime,
                "table.get",
                json!({"table_id": table_id, "key": i64::MAX})
            )
            .await
            .is_null()
        );
        let change = json!({"table_id": table_id, "key": i64::MIN, "values": {"live": false}});
        let update = protocol::parse_request(&body("table.update", change.clone()))
            .unwrap()
            .operation;
        let update_route = runtime
            .planner
            .point_route(&pinned, &update)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(update_route.shard().group, route.group);
        assert_eq!(run(&runtime, "table.update", change).await, json!(true));
        assert_eq!(
            run(
                &runtime,
                "table.get",
                json!({"table_id": table_id, "key": i64::MIN})
            )
            .await,
            json!({"id": i64::MIN, "live": false})
        );

        let wrong_type = get(table_id, protocol::Key::Utf8(i64::MIN.to_string()));
        assert!(matches!(
            runtime.planner.point_route(&pinned, &wrong_type).await,
            Err(PlanError::Protocol(ProtocolError {
                code: ErrorCode::ValidationError,
                ..
            }))
        ));
        let list = protocol::Operation::List(protocol::EmptyPayload {});
        assert!(
            runtime
                .planner
                .point_route(&pinned, &list)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            runtime
                .scoped_point_placement(&pinned, &hit)
                .await
                .unwrap()
                .is_none(),
            "RF1 cannot issue scoped RF3 placement"
        );

        let local = runtime.services.local;
        let approved = placement(&route.group, local);
        let scoped = ScopedPointPlacement::from_control(
            point,
            approved.clone(),
            Some(2),
            &runtime.services.shared,
        )
        .unwrap();
        assert_eq!(scoped.route().group, approved.record.identity);
        assert_eq!(scoped.generation(), approved.record.generation);
        assert_eq!(
            scoped.members(),
            [local, approved.voters[1].worker, approved.voters[2].worker]
        );
        assert_eq!(scoped.preferred_worker(), approved.voters[1].worker);
        let queue_view = AuthorityView {
            run_id: pinned.authority.run_id,
            owner_term: 7,
            membership_revision: 1,
        };
        let queue = ScopedPlacement::new(
            scoped.route().clone(),
            scoped.generation(),
            queue_view,
            local,
            scoped.members(),
        )
        .unwrap();
        let old_queue_view = AuthorityView {
            owner_term: 6,
            ..queue_view
        };
        assert_ne!(
            queue,
            ScopedPlacement::new(
                scoped.route().clone(),
                scoped.generation(),
                old_queue_view,
                local,
                scoped.members(),
            )
            .unwrap()
        );
        scoped.verify_target(approved.clone(), local).unwrap();
        let prior_incarnation = WorkerIdentity {
            incarnation: nabedra_rt::ids::uuid_v4(),
            ..local
        };
        assert_eq!(
            scoped
                .verify_target(approved.clone(), prior_incarnation)
                .unwrap_err(),
            stale()
        );
        let mut moved = approved.clone();
        moved.record.generation = nabedra_rt::ids::uuid_v4();
        assert_eq!(scoped.verify_target(moved, local).unwrap_err(), stale());
        let mut voters = approved.voters.clone().into_inner();
        voters[0].worker.incarnation = nabedra_rt::ids::uuid_v4();
        let restarted = SplitSourcePlacement {
            voters: Page::new(voters).unwrap(),
            ..approved
        };
        assert_eq!(scoped.verify_target(restarted, local).unwrap_err(), stale());

        run(&runtime, "table.drop", json!({"table_id": table_id})).await;
        let table = run(&runtime, "table.create", schema("utf8")).await;
        let new_table = serde_json::from_value::<Uuid>(table["id"].clone()).unwrap();
        assert_ne!(new_table, table_id);
        let new_pin = pin(
            &runtime,
            "table.get",
            json!({"table_id": new_table, "key": "é水"}),
        )
        .await;
        assert_eq!(
            runtime
                .verify_scoped_point_target(&scoped, &new_pin)
                .await
                .unwrap_err(),
            stale()
        );
        assert!(
            runtime
                .verify_scoped_point_target(&scoped, &pinned)
                .await
                .is_err(),
            "matching pin cannot grant a modeled placement against live Control"
        );
        assert!(matches!(
            runtime.planner.point_route(&new_pin, &hit).await,
            Err(PlanError::Protocol(ProtocolError {
                code: ErrorCode::NotFound,
                ..
            }))
        ));
        let old = runtime
            .planner
            .point_route(&pinned, &hit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.shard().group, scoped.route().group);
        let new = runtime
            .planner
            .point_route(&new_pin, &get(new_table, protocol::Key::Utf8("é水".into())))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(new.shard().group, old.shard().group);
        assert!(
            matches!(new.shard().group.kind, GroupKind::Data {table_id: id, ..} if id == new_table)
        );
        runtime.drain().await.unwrap();
    })
    .await
    .expect("scoped point placement watchdog");
}
