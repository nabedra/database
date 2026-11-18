//! O12 variations pause the real operator coordinator after durable Building
//! publication. All participant work uses the ordinary authenticated RPC path.
use super::*;
use crate::oltp::mvcc::planner::ParticipantResolver;
use crate::oltp::mvcc::{
    CanonicalJson, Change, MvccKey, PreparedWrite, catalog::RouteRead, index::IndexPhase,
};
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;
struct Gate {
    entered: Notify,
    release: Notify,
    once: AtomicBool,
    source: Mutex<Option<(StoragePin, PinnedCatalog)>>,
}
struct Paused {
    inner: RuntimeIndexes,
    gate: Arc<Gate>,
}
impl IndexBackend for Paused {
    fn planner(&self) -> &NativePlanner {
        self.inner.planner()
    }
    fn begin(
        &self,
        job: Uuid,
        digest: Digest,
        maintenance: MaintenanceOwnerRef,
        fixed_identity: Option<Uuid>,
    ) -> ResolveFuture<'_, IndexAttempt> {
        self.inner.begin(job, digest, maintenance, fixed_identity)
    }
    fn commit(&self, attempt: IndexAttempt, plan: MutationPlan) -> ResolveFuture<'_, ()> {
        self.inner.commit(attempt, plan)
    }
    fn source_pin(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)> {
        Box::pin(async move {
            let source = self.inner.source_pin(authority, owner).await?;
            *self.gate.source.lock().unwrap() = Some(source.clone());
            Ok(source)
        })
    }
    fn refresh(&self, table: Uuid, key: protocol::Key, job: Uuid) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            if !self.gate.once.swap(true, Ordering::AcqRel) {
                self.gate.entered.notify_one();
                self.gate.release.notified().await;
            }
            self.inner.refresh(table, key, job).await
        })
    }
    fn close_source(
        &self,
        authority: Authority,
        proof: VerifiedIndexAttemptClosed,
    ) -> ResolveFuture<'_, ()> {
        self.inner.close_source(authority, proof)
    }
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()> {
        self.inner.help(intent)
    }
    fn help_lifecycle(
        &self,
        binding: crate::oltp::mvcc::store::lifecycle::LifecycleBinding,
    ) -> ResolveFuture<'_, ()> {
        self.inner.help_lifecycle(binding)
    }
    fn fail(
        &self,
        attempt: IndexAttempt,
        guards: Vec<VerifiedLifecycleGuard>,
        pending: Option<LifecycleBinding>,
        error: ProtocolError,
    ) -> ResolveFuture<'_, ()> {
        self.inner.fail(attempt, guards, pending, error)
    }
}
async fn exercise_case(case: &str) {
    let path = std::env::temp_dir().join(format!("nabedra-index-{case}-{}", Uuid::new_v4()));
    let execution = ExecutionConfig {
        max_pending_requests: 32,
        max_queue_bytes_per_replica: 8 << 20,
        max_retained_result_bytes_per_worker: 8 << 20,
        ..Default::default()
    };
    let resources = WorkerResources {
        max_threads: 2,
        memory_limit_bytes: 2 << 30,
        ..Default::default()
    };
    let mut runtime = WorkerRuntime::open(
        &path,
        "index-test",
        "index-test",
        execution.clone(),
        resources,
    )
    .await
    .unwrap();
    let mut app = runtime.application("index-test".into()).unwrap();
    let table:protocol::TableDescriptor=serde_json::from_value(super::tests::call(&app,"table.create",serde_json::json!({"name":"items","schema":{"id":"int64","category":"int64","price":"float64","name":"utf8"},"key":"id","format":"parquet"})).await).unwrap();
    let mut rows:Vec<protocol::Row>=serde_json::from_value(serde_json::json!([{"id":1,"category":0,"price":1.0,"name":"old"},{"id":2,"category":0,"price":1.0,"name":"delete"},{"id":3,"category":1,"price":2.0,"name":"keep"}])).unwrap();
    super::tests::call(
        &app,
        "table.insert_many",
        serde_json::json!({"table_id":table.id,"rows":rows}),
    )
    .await;
    let definition = IndexDefinition {
        index_id: Uuid::new_v4(),
        table_id: table.id,
        name: Text::try_from("by_category").unwrap(),
        columns: Page::new(vec![Text::try_from("category").unwrap()]).unwrap(),
        covering: Page::new(vec![
            Text::try_from("name").unwrap(),
            Text::try_from("price").unwrap(),
        ])
        .unwrap(),
    };
    let build_id = Uuid::new_v4();
    let gate = Arc::new(Gate {
        entered: Notify::new(),
        release: Notify::new(),
        once: AtomicBool::new(false),
        source: Mutex::new(None),
    });
    let paused = Arc::new(Paused {
        inner: RuntimeIndexes {
            runtime: runtime.clone(),
            authority: None,
        },
        gate: gate.clone(),
    });
    let service = IndexService::new(paused.clone());
    let input = definition.clone();
    let building = tokio::spawn(async move { service.create(input, build_id, 1).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), gate.entered.notified())
        .await
        .unwrap();
    let query:protocol::Query=serde_json::from_value(serde_json::json!({"select":null,"filter":{"column":"category","op":"eq","value":0},"order_by":{"column":"price","desc":true},"limit":10})).unwrap();
    let explain = super::tests::call(
        &app,
        "query.explain",
        serde_json::json!({"table_id":table.id,"query":query}),
    )
    .await;
    assert!(!explain.as_str().unwrap().contains("secondary by_category"));
    let (source_pin, source) = gate.source.lock().unwrap().clone().unwrap();
    let record = IndexRecord {
        definition: definition.clone(),
        phase: IndexPhase::Building {
            build_id,
            attempt: 1,
            pin: source_pin,
            source: source.clone(),
        },
        schema_epoch: 1,
        routing_epoch: 1,
    };
    let maintenance = MaintenanceOwnerRef {
        catalog_group_id: source.catalog.group_id,
        table_id: table.id,
        index_id: definition.index_id,
        build_id,
        attempt: 1,
    };
    if case != "drop" {
        super::tests::call(&app,"table.update",serde_json::json!({"table_id":table.id,"key":1,"values":{"category":1,"name":"changed"}})).await;
        rows[0].insert("category".into(), protocol::Scalar::Int64(1));
        rows[0].insert("name".into(), protocol::Scalar::Utf8("changed".into()));
        super::tests::call(
            &app,
            "table.delete",
            serde_json::json!({"table_id":table.id,"key":2}),
        )
        .await;
        rows.retain(|r| r["id"] != protocol::Scalar::Int64(2));
        let row: protocol::Row = serde_json::from_value(
            serde_json::json!({"id":4,"category":0,"price":1.0,"name":"new"}),
        )
        .unwrap();
        super::tests::call(
            &app,
            "table.insert",
            serde_json::json!({"table_id":table.id,"row":row}),
        )
        .await;
        rows.push(row);
        super::tests::check(&app, &table, &rows, query.clone()).await;
    }
    building.abort();
    assert!(building.await.unwrap_err().is_cancelled());
    drop(paused);
    drop(gate);
    match case {
        "resume" => runtime.resume_index(&record).await.unwrap(),
        "cold" => {
            runtime.drain().await.unwrap();
            drop(app);
            drop(runtime);
            runtime = WorkerRuntime::open_configured(
                &path,
                "index-test",
                "index-test",
                execution,
                resources,
                true,
            )
            .await
            .unwrap();
            app = runtime.application("index-test".into()).unwrap();
            assert!(
                runtime.resume_index(&record).await.is_err(),
                "old-run attempt renewed its source snapshot"
            );
            runtime.restart_index(&record, 2).await.unwrap();
        }
        "drop" => {
            let data = match runtime
                .services
                .catalog
                .route(&source, table.id, &protocol::Key::Int64(1))
                .await
                .unwrap()
            {
                RouteRead::Route { page, index } => runtime
                    .services
                    .resolver
                    .participant(&page.shards[index].group)
                    .await
                    .unwrap(),
                _ => panic!("retained source route missing"),
            };
            super::tests::call(&app, "table.drop", serde_json::json!({"table_id":table.id})).await;
            // Attempt to misuse the genuine tombstone cleanup capability for a
            // Data row. The actual native participant must force Status ABORT.
            let backend = RuntimeIndexes {
                runtime: runtime.clone(),
                authority: None,
            };
            let attempt = backend
                .begin(
                    Uuid::new_v4(),
                    serialized_digest(&maintenance).unwrap(),
                    maintenance,
                    None,
                )
                .await
                .unwrap();
            let mut plan = runtime
                .planner
                .plan_retire_index(
                    &attempt.pin,
                    attempt.txn,
                    attempt.request,
                    maintenance,
                    attempt.owner.clone(),
                )
                .await
                .unwrap();
            let txn = attempt.txn;
            let value = CanonicalJson::from_serializable(
                runtime.resources(),
                &serde_json::json!({"id":-999,"category":0,"price":0.0,"name":"forbidden"}),
                crate::protocol::MAX_BODY_BYTES,
            )
            .unwrap();
            plan.participants[0].participant = data;
            plan.participants[0].writes = vec![PreparedWrite {
                key: MvccKey::Row {
                    table_id: table.id,
                    primary_key: protocol::Key::Int64(-999),
                },
                expected_version: None,
                change: Change::Put(value),
            }];
            assert!(
                backend.commit(attempt, plan).await.is_err(),
                "metadata cleanup authorized a Data mutation"
            );
            let status = runtime
                .services
                .status
                .inspect_transaction(txn)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                status.decision.unwrap().decision,
                TxnDecision::Abort { .. }
            ));
            runtime.retire_index(&record).await.unwrap();
        }
        _ => unreachable!(),
    }
    let authority = runtime
        .services
        .control
        .current(&runtime.services.scope)
        .await
        .unwrap()
        .ready
        .view
        .authority();
    let closed = runtime
        .services
        .catalog
        .participant()
        .strong_index_attempt_closed(maintenance)
        .await
        .unwrap();
    assert_eq!(closed.pin(), source_pin);
    if case != "drop" {
        super::tests::check(&app, &table, &rows, query.clone()).await;
        let explain = super::tests::call(
            &app,
            "query.explain",
            serde_json::json!({"table_id":table.id,"query":query}),
        )
        .await;
        assert!(explain.as_str().unwrap().contains("secondary by_category"));
        if case == "resume" {
            let ready_record = IndexRecord {
                phase: IndexPhase::Ready {
                    build_id,
                    attempt: 1,
                    pin: source_pin,
                },
                ..record.clone()
            };
            // Retirement changes the current phase while preserving the
            // immutable original Ready closure and exact pin release proof.
            runtime.retire_index(&ready_record).await.unwrap();
            let retired = runtime
                .services
                .catalog
                .participant()
                .strong_index_attempt_closed(maintenance)
                .await
                .unwrap();
            assert_eq!(retired.pin(), source_pin);
            assert_eq!(
                retired.statement().kind,
                crate::oltp::mvcc::index::IndexClosedKind::Ready
            );
            assert_eq!(retired.terminal_position(), closed.terminal_position());
            let probe = runtime.admit(serde_json::to_vec(&serde_json::json!({
                "request_id":Uuid::new_v4(),"operation":"table.get","payload":{"table_id":table.id,"key":1}
            })).unwrap()).await.unwrap();
            let (_, context, _) = runtime
                .services
                .replica
                .lock()
                .unwrap()
                .input(probe.request_id)
                .unwrap();
            let context: crate::oltp_runtime::replica::AcceptedContext =
                serde_json::from_slice(context.as_ref()).unwrap();
            let current = runtime
                .services
                .catalog
                .snapshot(context.read.unwrap().storage_pin.read_ts, authority)
                .unwrap();
            let crate::oltp::mvcc::catalog::CatalogRead::Value {
                record: Some(current),
                ..
            } = runtime
                .services
                .catalog
                .read::<IndexRecord>(
                    &current,
                    MvccKey::CatalogIndex {
                        table_id: table.id,
                        index_id: definition.index_id,
                    },
                )
                .await
                .unwrap()
            else {
                panic!("retired index metadata missing");
            };
            assert!(matches!(current.phase, IndexPhase::Retired { .. }));
            super::tests::check(&app, &table, &rows, query.clone()).await;
            let explain = super::tests::call(
                &app,
                "query.explain",
                serde_json::json!({"table_id":table.id,"query":query}),
            )
            .await;
            assert!(!explain.as_str().unwrap().contains("secondary by_category"));
        }
    }
    let expired = runtime
        .services
        .timestamps
        .begin_maintenance(authority, maintenance)
        .await
        .unwrap_err();
    assert!(expired.message.contains("PinExpired"));
    runtime.drain().await.unwrap();
    drop(app);
    drop(runtime);
    std::fs::remove_dir_all(path).unwrap();
    eprintln!(
        "{}",
        serde_json::json!({"scenario":"O12","variation":case,"status":"passed","scope":"actual-forced-wire-local-rf1"})
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run through scripts/acceptance_oltp.py"]
async fn acceptance_o12() {
    tokio::time::timeout(std::time::Duration::from_secs(240), async {
        for case in ["resume", "cold", "drop"] {
            exercise_case(case).await;
        }
    })
    .await
    .expect("index lifecycle watchdog");
    eprintln!(
        "NABEDRA_OLTP_ACCEPTANCE {}",
        serde_json::json!({"scenario":"O12","status":"not-run","reason":"local backfill/resume/cold/drop executed; failure while validation/epoch publication and distributed leader failure remain required"})
    );
}
