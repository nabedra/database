//! Opt-in O24 attribution run against the production RF1 worker in process.
use super::{http_runtime::HttpRuntime, standalone_network_runtime, worker_runtime::WorkerRuntime};
use crate::config::{ExecutionConfig, WorkerResources};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{task::JoinSet, time::Instant};
use uuid::Uuid;

#[derive(Deserialize)]
struct Arrival {
    offset_ns: u64,
    request: Value,
    expected: Value,
}

#[derive(Deserialize)]
struct Plan {
    case: String,
    seed: u64,
    rate: f64,
    repetition: usize,
    clients: usize,
    warmup: Vec<Arrival>,
    arrivals: Vec<Arrival>,
}

fn read<T: serde::de::DeserializeOwned>(path: &Path, limit: u64) -> T {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .expect("benchmark file")
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .expect("read benchmark file");
    assert!(bytes.len() as u64 <= limit);
    serde_json::from_slice(&bytes).expect("parse benchmark file")
}

fn env_path(key: &str) -> PathBuf {
    std::env::var_os(key).expect(key).into()
}

async fn execute(runtime: Arc<WorkerRuntime>, request: Arrival, due: Instant) -> Value {
    let id: Uuid =
        serde_json::from_value(request.request["request_id"].clone()).expect("request UUID");
    let body = serde_json::to_vec(&request.request).expect("bounded request JSON");
    let started = Instant::now();
    let record = match runtime.clone().submit_owned(body).await {
        Ok(record) => record,
        Err(error) => {
            assert_eq!(
                error.outcome,
                crate::protocol::Outcome::NotApplied,
                "uncertain in-process submission {id}"
            );
            return json!({
                "request_id": id, "state": "definitively_rejected", "error": error,
                "completion_ms": due.elapsed().as_secs_f64() * 1000.0
            });
        }
    };
    let response = runtime
        .wait(&record)
        .await
        .expect("observe original result");
    let finished = Instant::now();
    let result: Value = serde_json::from_slice(&response.body).expect("decode result envelope");
    let receipt = runtime
        .receipt(record.run_id, id)
        .await
        .expect("resolve accepted identity");
    assert_eq!((receipt.run_id, receipt.request_id), (record.run_id, id));
    assert!(receipt.state.terminal());
    if response.status == 200 {
        assert_eq!(result["request_id"], id.to_string());
        assert_eq!(result["data"], request.expected);
        assert_eq!(
            receipt.result.as_ref().unwrap_or(&Value::Null),
            &request.expected
        );
    } else {
        assert!(receipt.error.is_some());
    }
    json!({
        "request_id": id, "run_id": record.run_id, "state": receipt.state,
        "http_equivalent_status": response.status,
        "completion_ms": (finished - due).as_secs_f64() * 1000.0,
        "driver_queue_ms": (started - due).as_secs_f64() * 1000.0,
        "service_ms": (finished - started).as_secs_f64() * 1000.0,
    })
}

async fn observations(runtime: &WorkerRuntime) -> Value {
    let response = runtime.stats().await.expect("observe same worker");
    serde_json::from_slice(&response.body).expect("parse runtime statistics")
}

#[test]
#[ignore = "optional O24 direct production RF1 attribution; requires retained benchmark volume"]
fn o24_direct_production_rf1() {
    let plan: Plan = read(&env_path("NABEDRA_O24_DIRECT_PLAN"), 16 << 20);
    let resources: WorkerResources = read(&env_path("NABEDRA_O24_RESOURCES"), 64 << 10);
    let execution: ExecutionConfig = read(&env_path("NABEDRA_O24_EXECUTION"), 64 << 10);
    let data = env_path("NABEDRA_O24_DATA");
    let result = env_path("NABEDRA_O24_RESULT");
    assert!((1..=32).contains(&plan.clients));
    assert!((1..=2000).contains(&plan.arrivals.len()) && plan.warmup.len() <= 4000);
    assert!(plan.rate.is_finite() && plan.rate > 0.0);
    let identities: BTreeSet<_> = plan
        .warmup
        .iter()
        .chain(&plan.arrivals)
        .map(|arrival| {
            arrival.request["request_id"]
                .as_str()
                .expect("request UUID")
        })
        .collect();
    assert_eq!(identities.len(), plan.warmup.len() + plan.arrivals.len());
    let last = plan.arrivals.last().unwrap().offset_ns;
    assert!(last <= 240_000_000_000);
    assert!(
        plan.arrivals
            .windows(2)
            .all(|pair| pair[0].offset_ns <= pair[1].offset_ns)
    );
    let database = std::env::var("NABEDRA_O24_DATABASE").expect("benchmark database");
    let token = std::env::var("NABEDRA_TOKEN").expect("benchmark token");
    standalone_network_runtime().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(900), async {
            let runtime = WorkerRuntime::open_configured(&data, &database, &token, execution, resources, true)
                .await.expect("open seeded production RF1 volume");
            for arrival in plan.warmup {
                let observed = execute(runtime.clone(), arrival, Instant::now()).await;
                assert_eq!(observed["state"], "succeeded");
            }
            let before = observations(&runtime).await;
            let mut tasks = JoinSet::new();
            let mut samples = Vec::with_capacity(plan.arrivals.len());
            let origin = Instant::now();
            for (index, arrival) in plan.arrivals.into_iter().enumerate() {
                let due = origin + Duration::from_nanos(arrival.offset_ns);
                tokio::time::sleep_until(due).await;
                while let Some(result) = tasks.try_join_next() {
                    samples.push(result.expect("join admitted direct operation"));
                }
                if tasks.len() >= plan.clients {
                    samples.push((index, json!({"state": "client_capacity_rejected"})));
                } else {
                    let runtime = runtime.clone();
                    tasks.spawn(async move { (index, execute(runtime, arrival, due).await) });
                }
            }
            while let Some(result) = tasks.join_next().await {
                samples.push(result.expect("join admitted direct operation"));
            }
            let elapsed = origin.elapsed().as_secs_f64();
            samples.sort_unstable_by_key(|(index, _)| *index);
            assert!(samples.iter().enumerate().all(|(i, (index, _))| *index == i));
            let after = observations(&runtime).await;
            runtime.stop_accepting();
            runtime.drain().await.expect("drain direct worker");
            let completed = samples.iter().filter(|(_, sample)| sample["state"] == "succeeded").count();
            let accepted = samples.iter().filter(|(_, sample)| sample.get("run_id").is_some()).count();
            let mut completion: Vec<_> = samples.iter().filter_map(|(_, sample)| sample["completion_ms"].as_f64()).collect();
            completion.sort_unstable_by(f64::total_cmp);
            let percentile = |p: usize| {
                completion.get((completion.len() * p).div_ceil(100).saturating_sub(1))
            };
            let file = std::fs::OpenOptions::new().create_new(true).write(true).open(result)
                .expect("create direct evidence file");
            serde_json::to_writer(file, &json!({
                "schema_version": 1, "scenario": "O24", "path": "direct-production-worker",
                "scope": "production RF1 WorkerRuntime submit, wait, and exact receipt verification",
                "case": plan.case, "seed": plan.seed, "rate": plan.rate, "repetition": plan.repetition,
                "clients": plan.clients, "elapsed_seconds_including_receipt_checks": elapsed,
                "completed_per_second_including_receipt_checks": completed as f64 / elapsed,
                "accepted": accepted, "completed": completed,
                "completion": {"p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99)},
                "samples": samples, "runtime_before": before, "runtime_after": after
            })).expect("write direct evidence");
        }).await.expect("direct benchmark watchdog");
    });
}
