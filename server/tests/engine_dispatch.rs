use meshdb_server::{
    engine::Engine,
    protocol::{ErrorCode, Operation, Outcome, ProtocolError, parse_request},
    storage::{Namespace, Storage, StorageError},
};
use meshdb_server::{
    partition_size::PartitionSize,
    worker_pool::{WorkerPool, WorkerThreads},
};
use serde_json::{Value, json};
use std::num::NonZeroUsize;
#[path = "support/storage.rs"]
mod support;
use support::Fake;

fn operation(name: &str, payload: Value) -> Operation {
    let operation = parse_request(
        &serde_json::to_vec(&json!({
            "request_id": "11111111-1111-4111-8111-111111111111",
            "operation": name,
            "payload": payload,
        }))
        .unwrap(),
    )
    .unwrap()
    .operation;
    assert_eq!(operation.name(), name);
    assert_eq!(
        operation.is_mutation(),
        matches!(
            name,
            "table.create"
                | "table.drop"
                | "table.insert"
                | "table.insert_many"
                | "table.update"
                | "table.upsert"
                | "table.delete"
        )
    );
    operation
}
async fn fixture() -> (Engine<Fake>, WorkerThreads) {
    let storage = Storage::new(Fake::default(), Namespace::new("dispatch/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    (
        Engine::new(storage, pool, PartitionSize::default()),
        workers,
    )
}
fn create(name: &str) -> Value {
    json!({"name":name,"schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"})
}
async fn data(engine: &Engine<Fake>, name: &str, payload: Value) -> Value {
    serde_json::to_value(engine.execute(operation(name, payload)).await.unwrap()).unwrap()
}

#[tokio::test]
async fn parsed_workflow_dispatches_all_thirteen_wire_shapes() {
    let (engine, workers) = fixture().await;
    let table = data(&engine, "table.create", create("items")).await;
    assert_eq!(table["name"], "items");
    assert_eq!(table["schema"], create("items")["schema"]);
    assert_eq!(table["key"], "id");
    assert_eq!(table["format"], "parquet");
    let id = &table["id"];
    assert!(uuid::Uuid::parse_str(id.as_str().unwrap()).is_ok());
    assert_eq!(
        data(&engine, "table.get", json!({"table_id":id,"key":1})).await,
        Value::Null
    );
    let empty_query =
        json!({"table_id":id,"query":{"select":null,"filter":null,"order_by":null,"limit":null}});
    assert_eq!(
        data(&engine, "query.collect", empty_query.clone()).await,
        json!([])
    );
    assert_eq!(
        data(&engine, "query.count", empty_query.clone()).await,
        json!(0)
    );
    assert!(
        data(&engine, "query.explain", empty_query)
            .await
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    let alpha = data(&engine, "table.create", create("alpha")).await;
    assert_eq!(
        data(&engine, "table.describe", json!({"name":"items"})).await,
        table
    );
    assert_eq!(
        data(&engine, "table.list", json!({})).await,
        json!([alpha, table])
    );
    let row = json!({"id":1,"text":"one"});
    assert_eq!(
        data(&engine, "table.insert", json!({"table_id":id,"row":row})).await,
        Value::Null
    );
    assert_eq!(
        data(
            &engine,
            "table.insert_many",
            json!({"table_id":id,"rows":[{"id":2,"text":"two"},{"id":3,"text":"three"}]})
        )
        .await,
        json!(2)
    );
    assert_eq!(
        data(
            &engine,
            "table.upsert",
            json!({"table_id":id,"row":{"id":1,"text":"replacement"}})
        )
        .await,
        Value::Null
    );
    assert_eq!(
        data(
            &engine,
            "table.update",
            json!({"table_id":id,"key":1,"values":{"text":"updated"}})
        )
        .await,
        json!(true)
    );
    assert_eq!(
        data(&engine, "table.get", json!({"table_id":id,"key":1})).await,
        json!({"id":1,"text":"updated"})
    );
    assert_eq!(
        data(&engine, "table.get", json!({"table_id":id,"key":9})).await,
        Value::Null
    );
    assert_eq!(
        data(&engine, "table.delete", json!({"table_id":id,"key":3})).await,
        json!(true)
    );
    assert_eq!(
        data(&engine, "table.delete", json!({"table_id":id,"key":3})).await,
        json!(false)
    );
    let query =
        json!({"table_id":id,"query":{"select":null,"filter":null,"order_by":null,"limit":null}});
    assert_eq!(
        data(&engine, "query.collect", query.clone()).await,
        json!([{"id":1,"text":"updated"},{"id":2,"text":"two"}])
    );
    assert_eq!(data(&engine, "query.count", query.clone()).await, json!(2));
    let explain = data(&engine, "query.explain", query).await;
    assert!(explain.as_str().is_some_and(|s| !s.is_empty()));
    assert_eq!(
        data(&engine, "table.drop", json!({"table_id":id})).await,
        Value::Null
    );
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn dispatch_preserves_errors_and_uncertain_publication() {
    let (engine, workers) = fixture().await;
    let table = data(&engine, "table.create", create("items")).await;
    let id = &table["id"];
    let duplicate = operation("table.create", create("items"));
    let expected = engine
        .create(match duplicate.clone() {
            Operation::Create(p) => p,
            _ => unreachable!(),
        })
        .await
        .unwrap_err();
    assert_eq!(engine.execute(duplicate).await.unwrap_err(), expected);
    assert_eq!(
        expected,
        ProtocolError::new(
            ErrorCode::Conflict,
            "table name already exists",
            Outcome::NotApplied
        )
    );
    let missing = operation("table.describe", json!({"name":"missing"}));
    assert!(!missing.is_mutation());
    assert_eq!(
        engine.execute(missing).await.unwrap_err(),
        ProtocolError::new(ErrorCode::NotFound, "table not found", Outcome::NotApplied)
    );
    let invalid = operation("table.get", json!({"table_id":id,"key":"wrong_type"}));
    let Operation::Get(payload) = invalid.clone() else {
        unreachable!()
    };
    let expected = engine.get(payload).await.unwrap_err();
    assert_eq!(expected.code, ErrorCode::ValidationError);
    assert_eq!(engine.execute(invalid).await.unwrap_err(), expected);
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .put_faults
        .insert(
            "dispatch/catalog.json".into(),
            (StorageError::Ambiguous, false),
        );
    let insert = operation(
        "table.insert",
        json!({"table_id":id,"row":{"id":1,"text":"one"}}),
    );
    assert!(insert.is_mutation());
    let error = engine.execute(insert).await.unwrap_err();
    assert_eq!(
        error,
        ProtocolError::new(
            ErrorCode::OutcomeUnknown,
            "catalog publication outcome is unknown",
            Outcome::Unknown
        )
    );
    workers.shutdown().await.unwrap();
}
