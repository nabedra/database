//! B01/B02 resource/pool microbenchmark. Does not stand in for API, storage or provider tests.
//! Run with --output <file>; JSON includes one warmup and three measured repetitions.
#![recursion_limit = "256"]
use futures_util::{StreamExt, stream::FuturesUnordered};
use nabedra_database_server::{
    config::WorkerResources, scheduling::ResourceScheduler, worker_pool::WorkerPool,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Handle,
};

const SEED: u64 = 343687;
const REQUESTS: usize = 24;
const JOBS_PER_REQUEST: usize = 12;
const MAX_PENDING: usize = 16;
const REPETITIONS: usize = 3;

fn work_conserving_event_check() -> Value {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        std::pin::pin!(tokio::task::unconstrained(future))
            .poll(&mut Context::from_waker(Waker::noop()))
    }
    let resources = [1u64, 2, 4].map(|max_threads| WorkerResources {
        max_threads,
        memory_limit_bytes: 8 << 20,
        task_memory_limit_bytes: 1 << 20,
        control_memory_reserve_bytes: 1 << 20,
    });
    let scheduler = ResourceScheduler::new(
        resources
            .into_iter()
            .enumerate()
            .map(|(node, limits)| (node as u64, limits))
            .collect(),
    )
    .unwrap();
    let mut pending = (0..96u64)
        .map(|request| {
            let scheduler = scheduler.clone();
            Some(Box::pin(async move {
                scheduler.reserve(request, 1, 32768).await
            }))
        })
        .collect::<Vec<_>>();
    let mut active = Vec::new();
    for waiter in &mut pending {
        if let Poll::Ready(grant) = poll_once(waiter.as_mut().unwrap().as_mut()) {
            active.push(grant.unwrap());
            *waiter = None;
        }
    }
    assert_eq!(
        scheduler
            .usage()
            .iter()
            .map(|(_, threads, _)| threads)
            .sum::<u64>(),
        7,
        "all feasible compute slots must be allocated after arrivals are registered"
    );
    let mut checked_releases = 0;
    while let Some(grant) = active.pop() {
        drop(grant);
        if pending.iter().any(Option::is_some) {
            // Reservation settlement schedules its successor synchronously.
            // Inspect before polling the woken future or running another task.
            assert_eq!(
                scheduler
                    .usage()
                    .iter()
                    .map(|(_, threads, _)| threads)
                    .sum::<u64>(),
                7,
                "a feasible slot was idle after the resource-release event"
            );
            checked_releases += 1;
        }
        for waiter in pending.iter_mut().filter(|waiter| waiter.is_some()) {
            if let Poll::Ready(grant) = poll_once(waiter.as_mut().unwrap().as_mut()) {
                active.push(grant.unwrap());
                *waiter = None;
            }
        }
    }
    assert!(pending.iter().all(Option::is_none));
    assert!(
        scheduler
            .usage()
            .iter()
            .all(|(_, threads, bytes)| *threads == 0 && *bytes == 0)
    );
    assert_eq!(checked_releases, 89);
    json!({"scope":"logical scheduler reservations at deterministic arrival/release events; CPU execution measured separately",
        "jobs":96,"worker_threads":[1,2,4],"checked_release_events":checked_releases,
        "feasible_idle_slots_after_processed_event":0,"settled_reservations":96})
}

fn next(value: &mut u64) -> u64 {
    *value ^= *value << 13;
    *value ^= *value >> 7;
    *value ^= *value << 17;
    *value
}
fn quantiles(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    let percentile = |p: usize| {
        values
            .get((values.len() * p).div_ceil(100).saturating_sub(1))
            .copied()
    };
    json!({"p50":percentile(50),"p95":percentile(95),"p99":percentile(99),"samples":values.len(),"raw":values})
}
fn command(name: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(name).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
#[cfg(unix)]
fn process_usage() -> (f64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // getrusage initializes the supplied native structure on a successful call.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return (0.0, 0);
    }
    let usage = unsafe { usage.assume_init() };
    let cpu = usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1e6
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1e6;
    let rss = usage.ru_maxrss.max(0) as u64;
    (
        cpu,
        if cfg!(target_os = "macos") {
            rss
        } else {
            rss.saturating_mul(1024)
        },
    )
}
#[cfg(not(unix))]
fn process_usage() -> (f64, u64) {
    (0.0, 0)
}
fn quota() -> Value {
    let cpu_max = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok();
    let v1_quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok();
    let v1_period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok();
    let quota_cores = cpu_max
        .as_deref()
        .and_then(|raw| {
            let mut parts = raw.split_whitespace();
            Some(parts.next()?.parse::<f64>().ok()? / parts.next()?.parse::<f64>().ok()?)
        })
        .or_else(|| {
            let q = v1_quota.as_deref()?.trim().parse::<f64>().ok()?;
            let period = v1_period.as_deref()?.trim().parse::<f64>().ok()?;
            (q > 0.0 && period > 0.0).then_some(q / period)
        });
    json!({"cgroup_v2_cpu_max":cpu_max,"cgroup_v1_quota":v1_quota,"cgroup_v1_period":v1_period,
        "quota_cores":quota_cores,"available_parallelism":std::thread::available_parallelism().ok().map(|n|n.get()),
        "cgroup_memory_max":std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok(),
        "process_limits":std::fs::read_to_string("/proc/self/limits").ok()})
}
struct Completed {
    identity: usize,
    request: usize,
    queue_ns: u64,
    latency_ns: u64,
    error: Option<String>,
}

async fn repetition(
    mixed: bool,
    cap: usize,
    arrival_us: u64,
    number: usize,
    warmup: bool,
) -> Value {
    let nodes = [1u64, 2, 4].map(|threads| WorkerResources {
        max_threads: threads,
        memory_limit_bytes: (threads + 2) * 1024 * 1024,
        task_memory_limit_bytes: 512 * 1024,
        control_memory_reserve_bytes: 1024 * 1024,
    });
    let scheduler = ResourceScheduler::new(
        nodes
            .iter()
            .enumerate()
            .map(|(id, r)| (id as u64, *r))
            .collect(),
    )
    .unwrap();
    let mut pools = Vec::new();
    let mut owners = Vec::new();
    for node in nodes {
        let (pool, owner) = WorkerPool::start_with_resources(node, Handle::current()).unwrap();
        pools.push(pool);
        owners.push(owner);
    }
    let pools = Arc::new(pools);
    let occupied: Arc<Vec<AtomicU64>> = Arc::new((0..3).map(|_| AtomicU64::new(0)).collect());
    let charged_peak: Arc<Vec<AtomicU64>> = Arc::new((0..3).map(|_| AtomicU64::new(0)).collect());
    let ipc_bytes = Arc::new(AtomicU64::new(0));
    let pending = Arc::new(tokio::sync::Semaphore::new(MAX_PENDING));
    let (cpu_before, _) = process_usage();
    let started = Instant::now();
    let mut requests = FuturesUnordered::new();
    let mut rejected_requests = 0usize;
    let mut accepted_requests = 0usize;
    for request in 0..REQUESTS {
        if arrival_us > 0 {
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                started + Duration::from_micros(arrival_us * request as u64),
            ))
            .await;
        }
        let Ok(admission) = pending.clone().try_acquire_owned() else {
            rejected_requests += 1;
            continue;
        };
        accepted_requests += 1;
        let pools = pools.clone();
        let scheduler = scheduler.clone();
        let occupied = occupied.clone();
        let charged_peak = charged_peak.clone();
        let ipc_bytes = ipc_bytes.clone();
        requests.push(tokio::spawn(async move {
            let _admission = admission;
            let arrived = Instant::now();
            let mut jobs = FuturesUnordered::new();
            for ordinal in 0..JOBS_PER_REQUEST {
                let pools = pools.clone();
                let scheduler = scheduler.clone();
                let occupied = occupied.clone();
                let charged_peak = charged_peak.clone();
                let ipc_bytes = ipc_bytes.clone();
                jobs.push(async move {
                    let identity = request * JOBS_PER_REQUEST + ordinal;
                    let mut random = SEED ^ identity as u64;
                    let kind = if mixed { next(&mut random) % 3 } else { 0 };
                    let bytes = if kind == 1 { 256 * 1024 } else { 32 * 1024 };
                    let iterations = if mixed {
                        80_000 + next(&mut random) % 80_000
                    } else {
                        240_000
                    };
                    let result = async {
                        let reservation = if kind == 2 {
                            let node = identity as u64 % 3;
                            let memory = scheduler
                                .reserve_memory(request as u64, node, bytes)
                                .await?;
                            for (id, _, used) in scheduler.usage() {
                                charged_peak[id as usize].fetch_max(used, Ordering::Relaxed);
                            }
                            // Real bounded OS socket IO runs before acquiring a compute slot.
                            #[cfg(unix)]
                            {
                                let (mut reader, mut writer) =
                                    tokio::net::UnixStream::pair().expect("socket pair");
                                let send = vec![identity as u8; bytes as usize / 2];
                                let mut receive = vec![0u8; send.len()];
                                let (write, read) = tokio::join!(
                                    writer.write_all(&send),
                                    reader.read_exact(&mut receive)
                                );
                                write.expect("socket write");
                                read.expect("socket read");
                                assert_eq!(receive, send);
                                ipc_bytes.fetch_add(send.len() as u64, Ordering::Relaxed);
                            }
                            scheduler
                                .reserve_precharged(request as u64, cap, memory)
                                .await?
                        } else {
                            scheduler.reserve(request as u64, cap, bytes).await?
                        };
                        let node = reservation.node_id() as usize;
                        for (id, _, used) in scheduler.usage() {
                            charged_peak[id as usize].fetch_max(used, Ordering::Relaxed);
                        }
                        let occupied = occupied.clone();
                        let job = pools[node]
                            .submit_request(request as u64, 1, bytes, move |_, _| {
                                let began = Instant::now();
                                let queue_ns = arrived.elapsed().as_nanos() as u64;
                                let mut buffer = Vec::new();
                                buffer.try_reserve_exact(bytes as usize).map_err(|_| {
                                    nabedra_database_server::protocol::ProtocolError::new(
                                        nabedra_database_server::protocol::ErrorCode::LimitExceeded,
                                        "benchmark buffer allocation",
                                        nabedra_database_server::protocol::Outcome::NotApplied,
                                    )
                                })?;
                                buffer.resize(bytes as usize, identity as u8);
                                for iteration in 0..iterations {
                                    let index = next(&mut random) as usize % buffer.len();
                                    buffer[index] = buffer[index].wrapping_add(iteration as u8);
                                }
                                std::hint::black_box(buffer);
                                occupied[node].fetch_add(
                                    began.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                                Ok(queue_ns)
                            })
                            .await?;
                        let result = job.receive().await;
                        drop(reservation);
                        result
                    }
                    .await;
                    Completed {
                        identity,
                        request,
                        queue_ns: result.as_ref().copied().unwrap_or(0),
                        latency_ns: arrived.elapsed().as_nanos() as u64,
                        error: result.err().map(|error| format!("{error:?}")),
                    }
                });
            }
            let mut completed = Vec::new();
            while let Some(job) = jobs.next().await {
                completed.push(job);
            }
            completed
        }));
    }
    let mut results = Vec::new();
    while let Some(request) = requests.next().await {
        results.extend(request.expect("driver request panic"));
    }
    let elapsed = started.elapsed().as_secs_f64();
    let (cpu_after, peak_rss) = process_usage();
    let unique: BTreeSet<_> = results.iter().map(|job| job.identity).collect();
    let succeeded = results.iter().filter(|job| job.error.is_none()).count();
    let failed = results.len() - succeeded;
    let lost = accepted_requests * JOBS_PER_REQUEST - unique.len();
    let duplicate = results.len() - unique.len();
    assert_eq!((lost, duplicate), (0, 0));
    for owner in owners {
        owner.shutdown().await.unwrap();
    }
    assert!(
        scheduler
            .usage()
            .iter()
            .all(|(_, threads, bytes)| *threads == 0 && *bytes == 0)
    );
    assert!(
        pools
            .iter()
            .all(|pool| pool.scheduler().usage() == vec![(0, 0, 0)])
    );
    let request_completion: Vec<_> = (0..REQUESTS)
        .filter_map(|request| {
            results
                .iter()
                .filter(|job| job.request == request)
                .map(|job| job.latency_ns)
                .max()
        })
        .collect();
    json!({"case":if mixed {"B02"} else {"B01"},"warmup":warmup,"repetition":number,"seed":SEED,
        "request_node_cap":cap,"arrival_request_interval_us":arrival_us,"worker_resources":nodes,
        "requests_offered":REQUESTS,"accepted_requests":accepted_requests,"rejected_requests":rejected_requests,
        "driver_pending_request_limit":MAX_PENDING,"jobs_per_request":JOBS_PER_REQUEST,
        "accepted_jobs":accepted_requests*JOBS_PER_REQUEST,"completed_jobs":results.len(),"succeeded_jobs":succeeded,"failed_jobs":failed,
        "lost_identities":lost,"duplicate_identities":duplicate,"elapsed_seconds":elapsed,
        "completed_jobs_per_second":results.len() as f64/elapsed,"successful_jobs_per_second":succeeded as f64/elapsed,
        "queue_wait_ns":quantiles(results.iter().filter(|job|job.error.is_none()).map(|job|job.queue_ns).collect()),
        "job_completion_ns":quantiles(results.iter().map(|job|job.latency_ns).collect()),"request_completion_ns":quantiles(request_completion),
        "process_cpu_seconds":cpu_after-cpu_before,"process_cpu_core_equivalents":(cpu_after-cpu_before)/elapsed,
        "process_peak_rss_bytes":peak_rss,"rss_scope":"process lifetime high-water mark",
        "per_worker_compute_occupied_ns":occupied.iter().map(|value|value.load(Ordering::Relaxed)).collect::<Vec<_>>(),
        "per_worker_compute_slot_occupancy":occupied.iter().zip(nodes).map(|(value,node)|value.load(Ordering::Relaxed) as f64/(elapsed*1e9*node.max_threads as f64)).collect::<Vec<_>>(),
        "per_worker_peak_charged_bytes":charged_peak.iter().map(|value|value.load(Ordering::Relaxed)).collect::<Vec<_>>(),
        "socket_payload_bytes":ipc_bytes.load(Ordering::Relaxed),"network_bytes":0,"queue_replica_bytes":0,"retained_result_bytes":0,
        "cas_conflicts":0,"retry_work":0,"result_checksum":"all accepted job IDs settle exactly once",
        "errors":results.iter().filter_map(|job|job.error.as_ref().map(|error|json!({"job":job.identity,"error":error}))).collect::<Vec<_>>()})
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let output = match (args.next().as_deref(), args.next(), args.next()) {
        (Some("--output"), Some(path), None) => PathBuf::from(path),
        _ => {
            eprintln!("usage: resource_scheduler_benchmark --output <json-file>");
            std::process::exit(2);
        }
    };
    let work_conserving_assertions = work_conserving_event_check();
    let mut runs = Vec::new();
    for mixed in [false, true] {
        for cap in [1, 2, 3] {
            let arrivals: &[u64] = if mixed { &[0, 250, 1000] } else { &[0] };
            for &arrival in arrivals {
                runs.push(repetition(mixed, cap, arrival, 0, true).await);
                for rep in 1..=REPETITIONS {
                    runs.push(repetition(mixed, cap, arrival, rep, false).await);
                }
            }
        }
    }
    let artifact = json!({"schema_version":1,"benchmark":"real WorkerPool and ResourceScheduler synthetic partition jobs",
        "scope":"resource/pool microbenchmark; excludes database API, replicated queue, catalog CAS, external storage, remote transport and providers",
        "build":{"profile":"dev","optimization":"unoptimized","debug_info":"disabled",
            "CARGO_PROFILE_DEV_DEBUG":"0","CARGO_PROFILE_TEST_DEBUG":"0","CARGO_INCREMENTAL":"0",
            "provenance":"orchestrator build contract; not release throughput"},
        "example_source_sha256":Sha256::digest(include_bytes!("resource_scheduler_benchmark.rs")).iter().map(|byte|format!("{byte:02x}")).collect::<String>(),
        "commit":command("git",&["rev-parse","HEAD"]),"dirty":command("git",&["status","--porcelain"]),
        "os":command("uname",&["-srm"]),"rustc":command("rustc",&["--version"]),"architecture":std::env::consts::ARCH,
        "work_conserving_assertions":work_conserving_assertions,"cpu_quota":quota(),"host_ram":std::fs::read_to_string("/proc/meminfo").ok().or_else(||command("sysctl",&["-n","hw.memsize"])),
        "logical_job_payload_bytes":[32768,262144],"encoded_partition_bytes":0,"decoded_partition_bytes":0,
        "distribution":"seeded xorshift; B01 CPU only; B02 equal-probability CPU/memory/socket IO with bounded buffers",
        "cpu_iterations":{"B01":240000,"B02_min":80000,"B02_max":159999},"warmup_repetitions_per_configuration":1,
        "measured_repetitions_per_configuration":REPETITIONS,"control_runtime_threads":2,
        "os_process_memory_enforcement":"external process/container limits only; no limit installed by this benchmark",
        "queue_replication":"not exercised","latency_clock":"monotonic std::time::Instant","runs":runs});
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&output, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
    assert!(
        artifact["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|run| run["failed_jobs"].as_u64() == Some(0)),
        "controlled benchmark jobs must all succeed; inspect the written artifact"
    );
    println!("{}", output.display());
}
