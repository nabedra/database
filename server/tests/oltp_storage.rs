#![cfg(feature = "oltp")]
use nabedra_database_server::{
    oltp::{OltpEngine, OltpResources},
    protocol::{self, ErrorCode, Operation, Request},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use uuid::Uuid;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-oltp-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn open(dir: &Directory) -> OltpEngine {
    OltpEngine::open(&dir.0, OltpResources::default()).unwrap()
}
fn operation(name: &str, payload: Value) -> Operation {
    request(name, payload).operation
}
fn request(name: &str, payload: Value) -> Request {
    protocol::parse_request(
        &serde_json::to_vec(
            &json!({"request_id":Uuid::new_v4(), "operation":name,"payload":payload}),
        )
        .unwrap(),
    )
    .unwrap()
}
async fn execute(engine: &OltpEngine, name: &str, payload: Value) -> Value {
    engine.execute(operation(name, payload)).await.unwrap()
}
async fn create(engine: &OltpEngine, name: &str) -> Uuid {
    let value = execute(engine,"table.create",json!({"name":name,"schema":{"id":"int64","rank":"float64","text":"utf8","active":"bool"},"key":"id","format":"parquet"})).await;
    serde_json::from_value(value["id"].clone()).unwrap()
}
fn row(id: i64, rank: f64, text: &str) -> Value {
    json!({"id":id,"rank":rank,"text":text,"active":true})
}
fn query(table_id: Uuid, filter: Value, order: Value, select: Value, limit: Value) -> Value {
    json!({"table_id":table_id,"query":{"filter":filter,"order_by":order,"select":select,"limit":limit}})
}

#[tokio::test]
async fn o01_o03_o04_thirteen_operations_order_types_and_stale_handles() {
    let dir = Directory::new();
    let engine = open(&dir);
    let id = create(&engine, "items").await;
    let descriptor = execute(&engine, "table.describe", json!({"name":"items"})).await;
    assert_eq!(descriptor["id"], json!(id));
    assert_eq!(descriptor["format"], "parquet");
    assert_eq!(
        execute(&engine, "table.list", json!({})).await,
        json!([descriptor])
    );
    assert_eq!(
        execute(
            &engine,
            "table.insert",
            json!({"table_id":id,"row":row(-1,0.0,"first")})
        )
        .await,
        Value::Null
    );
    assert_eq!(execute(&engine,"table.insert_many",json!({"table_id":id,"rows":[row(i64::MIN,-0.0,"min"),row(i64::MAX,1.0,"max"),row(0,0.0,"zero"),row(4,9.0,"four")]})).await,5-1);
    assert_eq!(
        execute(&engine, "table.get", json!({"table_id":id,"key":i64::MIN})).await,
        row(i64::MIN, -0.0, "min")
    );
    assert_eq!(
        execute(&engine, "table.get", json!({"table_id":id,"key":5})).await,
        Value::Null
    );
    let all = query(id, Value::Null, Value::Null, json!(["id"]), Value::Null);
    assert_eq!(
        execute(&engine, "query.collect", all.clone()).await,
        json!([{"id":i64::MIN},{"id":-1},{"id":0},{"id":4},{"id":i64::MAX}])
    );
    let ordered = query(
        id,
        Value::Null,
        json!({"column":"rank","desc":true}),
        json!(["id"]),
        json!(4),
    );
    assert_eq!(
        execute(&engine, "query.collect", ordered).await,
        json!([{"id":4},{"id":i64::MAX},{"id":i64::MIN},{"id":-1}])
    );
    let filtered = query(
        id,
        json!({"op":"and","args":[{"op":"ge","column":"id","value":-1},{"op":"not","arg":{"op":"or","args":[{"op":"eq","column":"id","value":4},{"op":"eq","column":"active","value":false}]}}]}),
        Value::Null,
        json!(["id"]),
        json!(2),
    );
    assert_eq!(
        execute(&engine, "query.collect", filtered.clone()).await,
        json!([{"id":-1},{"id":0}])
    );
    assert_eq!(execute(&engine, "query.count", filtered).await, 2);
    let zero = query(
        id,
        json!({"op":"eq","column":"rank","value":-0.0}),
        Value::Null,
        Value::Null,
        Value::Null,
    );
    assert_eq!(execute(&engine, "query.count", zero).await, 3);
    assert_eq!(
        execute(
            &engine,
            "table.update",
            json!({"table_id":id,"key":-1,"values":{"text":"changed"}})
        )
        .await,
        true
    );
    assert_eq!(
        execute(
            &engine,
            "table.update",
            json!({"table_id":id,"key":3,"values":{}})
        )
        .await,
        false
    );
    assert_eq!(
        execute(
            &engine,
            "table.upsert",
            json!({"table_id":id,"row":row(3,3.0,"added")})
        )
        .await,
        Value::Null
    );
    assert_eq!(
        execute(&engine, "table.delete", json!({"table_id":id,"key":3})).await,
        true
    );
    assert_eq!(
        execute(&engine, "table.delete", json!({"table_id":id,"key":3})).await,
        false
    );
    let point = query(
        id,
        json!({"op":"eq","column":"id","value":-1}),
        Value::Null,
        Value::Null,
        Value::Null,
    );
    let explanation = execute(&engine, "query.explain", point.clone()).await;
    assert!(explanation.as_str().unwrap().contains("point get"));
    assert_eq!(
        execute(&engine, "query.collect", point).await,
        json!([row(-1, 0.0, "changed")])
    );
    assert_eq!(
        execute(&engine, "table.drop", json!({"table_id":id})).await,
        Value::Null
    );
    let recreated = create(&engine, "items").await;
    assert_ne!(id, recreated);
    assert_eq!(
        engine
            .execute(operation("query.collect", all))
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn o07_o08_o10_atomic_conflicts_stable_replays_and_database_binding() {
    let dir = Directory::new();
    let engine = open(&dir);
    engine.bind_database("my-db").unwrap();
    let id = create(&engine, "items").await;
    execute(
        &engine,
        "table.insert",
        json!({"table_id":id,"row":row(1,0.0,"original")}),
    )
    .await;
    let conflict = request(
        "table.insert_many",
        json!({"table_id":id,"rows":[row(2,0.0,"prefix"),row(1,0.0,"conflict")]}),
    );
    let request_id = conflict.request_id;
    let digest = "a".repeat(64);
    let run = Uuid::new_v4();
    assert_eq!(
        engine
            .execute_identified(conflict.clone(), digest.clone(), run)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    assert_eq!(
        execute(&engine, "table.get", json!({"table_id":id,"key":2})).await,
        Value::Null
    );
    execute(&engine, "table.delete", json!({"table_id":id,"key":1})).await;
    assert_eq!(
        engine
            .execute_identified(conflict, digest.clone(), Uuid::new_v4())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    let saved = engine.lookup_decision(request_id).await.unwrap().unwrap();
    assert_eq!(saved.run_id, run);
    assert_eq!(saved.digest, digest);
    let insert = request(
        "table.insert",
        json!({"table_id":id,"row":row(7,1.0,"seven")}),
    );
    let request_id = insert.request_id;
    let (left, right) = tokio::join!(
        engine.execute_identified(insert.clone(), "b".repeat(64), run),
        engine.execute_identified(insert.clone(), "b".repeat(64), run)
    );
    assert_eq!(left.unwrap(), Value::Null);
    assert_eq!(right.unwrap(), Value::Null);
    assert_eq!(
        engine
            .execute_identified(insert, "c".repeat(64), run)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    drop(engine);
    let engine = open(&dir);
    engine.bind_database("my-db").unwrap();
    assert_eq!(
        engine.bind_database("other").unwrap_err().code,
        ErrorCode::Conflict
    );
    let saved = engine.lookup_decision(request_id).await.unwrap().unwrap();
    assert_eq!(saved.result, Ok(Value::Null));
    assert_eq!(
        execute(
            &engine,
            "query.count",
            query(id, Value::Null, Value::Null, Value::Null, Value::Null)
        )
        .await,
        1
    );
}

#[tokio::test]
async fn o03_utf8_key_order_and_primary_range_boundaries() {
    let dir = Directory::new();
    let engine = open(&dir);
    let descriptor = execute(
        &engine,
        "table.create",
        json!({"name":"strings","schema":{"id":"utf8"},"key":"id","format":"parquet"}),
    )
    .await;
    let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
    let keys = ["", "\0", "\0\0", "\0a", "a", "a\0", "aa", "é", "🦀"];
    let rows: Vec<_> = keys.iter().rev().map(|key| json!({"id":key})).collect();
    execute(
        &engine,
        "table.insert_many",
        json!({"table_id":id,"rows":rows}),
    )
    .await;
    for (op, expected) in [
        ("lt", vec!["", "\0", "\0\0", "\0a"]),
        ("le", vec!["", "\0", "\0\0", "\0a", "a"]),
        ("eq", vec!["a"]),
        ("ge", vec!["a", "a\0", "aa", "é", "🦀"]),
        ("gt", vec!["a\0", "aa", "é", "🦀"]),
    ] {
        assert_eq!(
            execute(
                &engine,
                "query.collect",
                query(
                    id,
                    json!({"op":op,"column":"id","value":"a"}),
                    Value::Null,
                    Value::Null,
                    Value::Null
                )
            )
            .await,
            Value::Array(expected.iter().map(|key| json!({"id":key})).collect())
        );
    }
    let contradictory = json!({"op":"and","args":[{"op":"eq","column":"id","value":"a"},{"op":"gt","column":"id","value":"a"}]});
    assert_eq!(
        execute(
            &engine,
            "query.count",
            query(id, contradictory, Value::Null, Value::Null, Value::Null)
        )
        .await,
        0
    );
}

#[tokio::test]
async fn o04_seeded_scan_and_top_k_match_serial_oracle() {
    let dir = Directory::new();
    let engine = open(&dir);
    let id = create(&engine, "items").await;
    let mut seed = 42_u64;
    let mut rows = Vec::new();
    for id in -500_i64..500 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        rows.push(row(id, (seed % 17) as f64, "text"));
    }
    execute(
        &engine,
        "table.insert_many",
        json!({"table_id":id,"rows":rows}),
    )
    .await;
    for desc in [false, true] {
        for limit in [0, 1, 7, 100, 1000] {
            // An OR predicate must retain its full scan, even when its children
            // individually constrain the primary key.
            let filter = json!({"op":"and","args":[{"op":"ge","column":"rank","value":4.0},{"op":"or","args":[{"op":"lt","column":"id","value":0},{"op":"ge","column":"id","value":0}]}]});
            let mut expected_all: Vec<_> = rows
                .iter()
                .filter(|row| row["rank"].as_f64().unwrap() >= 4.0)
                .cloned()
                .collect();
            expected_all.sort_by(|a, b| {
                let cmp = a["rank"]
                    .as_f64()
                    .unwrap()
                    .partial_cmp(&b["rank"].as_f64().unwrap())
                    .unwrap();
                (if desc { cmp.reverse() } else { cmp })
                    .then_with(|| a["id"].as_i64().cmp(&b["id"].as_i64()))
            });
            expected_all.truncate(limit);
            let payload = query(
                id,
                filter,
                json!({"column":"rank","desc":desc}),
                Value::Null,
                json!(limit),
            );
            assert_eq!(
                execute(&engine, "query.collect", payload.clone()).await,
                Value::Array(expected_all.clone())
            );
            assert_eq!(
                execute(&engine, "query.count", payload).await,
                json!(expected_all.len())
            );
        }
    }
}

#[tokio::test]
async fn o21_growth_past_old_row_and_byte_limits_is_streamed_and_survives_reopen() {
    let dir = Directory::new();
    let engine = open(&dir);
    let id = create(&engine, "large").await;
    let text = "x".repeat(768);
    let start = Instant::now();
    let total = 110_001_i64;
    for first in (0..total).step_by(1000) {
        let rows: Vec<_> = (first..(first + 1000).min(total))
            .map(|id| row(id, (id % 7) as f64, &text))
            .collect();
        execute(
            &engine,
            "table.insert_many",
            json!({"table_id":id,"rows":rows}),
        )
        .await;
    }
    let insertion = start.elapsed();
    drop(engine);
    let engine = open(&dir);
    assert_eq!(
        execute(&engine, "table.get", json!({"table_id":id,"key":100_000})).await,
        row(100_000, 5.0, &text)
    );
    let all = query(id, Value::Null, Value::Null, Value::Null, Value::Null);
    assert_eq!(
        execute(&engine, "query.count", all.clone()).await,
        json!(total)
    );
    assert_eq!(
        engine
            .execute(operation("query.collect", all))
            .await
            .unwrap_err()
            .code,
        ErrorCode::LimitExceeded
    );
    assert_eq!(
        execute(
            &engine,
            "query.count",
            query(id, Value::Null, Value::Null, Value::Null, json!(100_001))
        )
        .await,
        100_001
    );
    let range = query(
        id,
        json!({"op":"ge","column":"id","value":100_000}),
        Value::Null,
        json!(["id"]),
        json!(3),
    );
    assert_eq!(
        execute(&engine, "query.collect", range).await,
        json!([{"id":100_000},{"id":100_001},{"id":100_002}])
    );
    eprintln!(
        "O21 standalone growth: rows={total}, raw_text_bytes={}, insertion_ms={}",
        total as usize * text.len(),
        insertion.as_millis()
    );
}

#[test]
fn o02_foreign_nonempty_directory_manifest_and_resource_limits_are_rejected() {
    let dir = Directory::new();
    std::fs::write(dir.0.join("catalog.json"), b"{}").unwrap();
    assert!(OltpEngine::open(&dir.0, OltpResources::default()).is_err());
    assert!(!dir.0.join("NABEDRA_OLTP.json").exists());
    let dir = Directory::new();
    let engine = open(&dir);
    drop(engine);
    let file = dir.0.join("NABEDRA_OLTP.json");
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    manifest["engine"] = json!("olap");
    std::fs::write(file, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(OltpEngine::open(&dir.0, OltpResources::default()).is_err());
    let dir = Directory::new();
    assert!(
        OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(!dir.0.join("NABEDRA_OLTP.json").exists());
}

// The child signals only after the fsync-enabled batch returns. Its parent then
// sends SIGKILL without running Rust or RocksDB destructors. This proves process
// crash recovery, not power-loss or provider-volume guarantees.
#[test]
#[ignore]
fn o11_process_crash_child() {
    let Ok(path) = std::env::var("NABEDRA_OLTP_CRASH_TEST") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let engine = OltpEngine::open(&path, OltpResources::default()).unwrap();
        let id = create(&engine, "crash").await;
        let rows: Vec<_> = (0..1000).map(|id| row(id, 1.0, "durable")).collect();
        let request = Request {
            request_id: Uuid::from_u128(77),
            operation: operation("table.insert_many", json!({"table_id":id,"rows":rows})),
        };
        engine
            .execute_identified(request, "d".repeat(64), Uuid::from_u128(78))
            .await
            .unwrap();
        std::fs::write(PathBuf::from(path).join("ack"), id.to_string()).unwrap();
        loop {
            std::thread::park();
        }
    });
}

#[tokio::test]
async fn o11_process_kill_preserves_acknowledged_batch_and_exact_decision() {
    let dir = Directory::new();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "o11_process_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env("NABEDRA_OLTP_CRASH_TEST", &dir.0)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let id = loop {
        if let Ok(text) = std::fs::read_to_string(dir.0.join("ack"))
            && let Ok(id) = text.parse::<Uuid>()
        {
            break id;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("crash child exited before durable barrier: {status}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("durability barrier timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    child.kill().unwrap();
    child.wait().unwrap();
    let engine = open(&dir);
    assert_eq!(
        execute(
            &engine,
            "query.count",
            query(id, Value::Null, Value::Null, Value::Null, Value::Null)
        )
        .await,
        1000
    );
    let decision = engine
        .lookup_decision(Uuid::from_u128(77))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.result, Ok(json!(1000)));
    assert_eq!(decision.run_id, Uuid::from_u128(78));
}
