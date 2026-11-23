//! GC1 measurement: CPU, wall time, entries, fsyncs and bytes per committed
//! direct write on a real RF3 Data group (or several sharing the pools), so
//! per-entry costs can be told apart from per-append and per-fsync costs.
//!
//! Each voter runs on its own Tokio runtime with its own native threads, so
//! on Linux the CPU of every thread can be charged to one voter (the
//! leader's executor runs on the leader's runtime). Elsewhere only the
//! process total is reported. Opt-in and ignored by default:
//!
//! ```sh
//! GC1_RATES=100,400,1000 GC1_RANGES=1,8 GC1_SECONDS=10 cargo test --release \
//!   --no-default-features --features oltp --lib gc1_measure -- --ignored --nocapture
//! ```
//!
//! Every line printed with the `GC1 ` prefix is one JSON record.
//!
//! GC2: `GC1_BATCH=off,on` runs every phase without and with
//! `data.direct.batch` (one `DirectBatch` entry for the commits that arrive
//! while a range's proposal is in flight), on the same runner and binary.
use super::*;
use std::collections::{BTreeSet, HashMap};

/// One voter: its runtime, pool, transport and one replica per range.
struct MVoter {
    node: u64,
    runtime: Option<tokio::runtime::Runtime>,
    shared: Arc<WorkerSharedResources>,
    pool: OltpPool,
    executor: Arc<DirectExecutor>,
    groups: Vec<MReplica>,
    threads: BTreeSet<u32>,
    stop: Option<oneshot::Sender<()>>,
}

struct MReplica {
    consensus: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    handle: Arc<ParticipantHandle>,
    _resident: ResidentGroup,
}

pub(super) struct Measure {
    voters: Vec<MVoter>,
    groups: Vec<GroupIdentity>,
    spans: Vec<(i64, i64)>,
    table: Uuid,
    _dir: Dir,
    driver_threads: BTreeSet<u32>,
    /// GC2: flips `data.direct.batch` on the leader's executor.
    gate: Arc<Gate>,
}

/// The tids of this process (Linux only; empty elsewhere).
fn tids() -> BTreeSet<u32> {
    std::fs::read_dir("/proc/self/task")
        .map(|dir| {
            dir.filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// (thread name, nanoseconds on CPU) of every live thread (Linux only).
fn thread_cpu() -> HashMap<u32, (String, u64)> {
    let mut out = HashMap::new();
    for tid in tids() {
        let base = format!("/proc/self/task/{tid}");
        let name = std::fs::read_to_string(format!("{base}/comm"))
            .unwrap_or_default()
            .trim()
            .to_owned();
        // `schedstat` is nanoseconds on CPU; `stat` (ticks) is the fallback.
        let ns = std::fs::read_to_string(format!("{base}/schedstat"))
            .ok()
            .and_then(|s| s.split_whitespace().next()?.parse::<u64>().ok())
            .or_else(|| {
                let stat = std::fs::read_to_string(format!("{base}/stat")).ok()?;
                let rest = &stat[stat.rfind(')')? + 2..];
                let fields: Vec<&str> = rest.split_whitespace().collect();
                let ticks: u64 =
                    fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
                Some(ticks * 10_000_000)
            })
            .unwrap_or(0);
        out.insert(tid, (name, ns));
    }
    out
}

/// Process CPU (user + system), nanoseconds.
fn process_cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the struct it is given.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    ns(usage.ru_utime) + ns(usage.ru_stime)
}

/// CPU per voter (index 0..3), the driver (the test runtime) and other
/// threads, plus the process total.
#[derive(Clone, Debug, Default)]
struct Cpu {
    voters: [u64; 3],
    driver: u64,
    other: u64,
    total: u64,
}

impl Measure {
    pub(super) async fn new(ranges: usize, rows: i64) -> Self {
        let dir = Dir::new();
        let certificates = tls::certificates(&dir.0, 3);
        let cluster_id = nabedra_rt::ids::uuid_v4();
        let table = nabedra_rt::ids::uuid_v4();
        let spans: Vec<(i64, i64)> = (0..ranges as i64)
            .map(|g| (g * rows / ranges as i64, (g + 1) * rows / ranges as i64))
            .collect();
        let groups: Vec<GroupIdentity> = (0..ranges)
            .map(|_| GroupIdentity {
                cluster_id,
                database: Text::try_from("gc1").unwrap(),
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Data {
                    table_id: table,
                    shard_id: nabedra_rt::ids::uuid_v4(),
                },
                group_format_version: 1,
            })
            .collect();
        let mut listeners = Vec::new();
        let mut workers = Vec::new();
        for tls in &certificates {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            workers.push(WorkerEnrollment {
                worker_id: nabedra_rt::ids::uuid_v4(),
                incarnation: nabedra_rt::ids::uuid_v4(),
                certificate_sha256: Digest::parse(tls.certificate_sha256()).unwrap(),
                enrollment_revision: 1,
                endpoint: Text::new(listener.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            });
            listeners.push(listener);
        }
        let members: BTreeMap<u64, ReplicaMember> = workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                let node = index as u64 + 1;
                (
                    node,
                    ReplicaMember {
                        node_id: node,
                        worker_id: worker.worker_id,
                        pool_id: nabedra_rt::ids::uuid_v4(),
                    },
                )
            })
            .collect();
        let peers = Arc::new(Peers(
            workers
                .iter()
                .map(|worker| (worker.worker_id, worker.clone()))
                .collect(),
        ));
        let gate: Arc<Gate> = Arc::new(Gate(
            AtomicBool::new(true),
            AtomicBool::new(false),
            AtomicBool::new(false),
        ));
        let schema = Arc::new(StaticSchema::default());
        schema.insert(DirectTable {
            descriptor: descriptor(table),
            schema_epoch: 1,
            lifecycle_id: table,
            indexes: Vec::new(),
        });
        let timestamps: Arc<dyn DirectTimestampSource> = Arc::new(Counter(AtomicU64::new(1_000)));
        let driver_threads = tids();
        let mut voters = Vec::new();
        for (index, ((worker, tls), listener)) in workers
            .into_iter()
            .zip(certificates)
            .zip(listeners)
            .enumerate()
        {
            let node = index as u64 + 1;
            let before = tids();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name(format!("gc1-v{node}"))
                .enable_all()
                .build()
                .unwrap();
            let build = {
                let (dir, members, peers, groups) = (
                    dir.0.clone(),
                    members.clone(),
                    peers.clone(),
                    groups.clone(),
                );
                let (gate, schema, timestamps) = (gate.clone(), schema.clone(), timestamps.clone());
                async move {
                    let shared = shared();
                    let pool = OltpPool::open_development(
                        dir.join(format!("pool{node}")),
                        PoolIdentity {
                            engine: "oltp".into(),
                            pool_format_version: 2,
                            cluster_id,
                            worker_id: worker.worker_id,
                            pool_id: members[&node].pool_id,
                        },
                        shared.clone(),
                    )
                    .unwrap();
                    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
                    let transport = Arc::new(
                        WorkerTransport::new(
                            &worker,
                            &tls,
                            shared.clone(),
                            peers,
                            registry.clone(),
                            TransportConfig::default(),
                        )
                        .unwrap(),
                    );
                    let mut replicas = Vec::new();
                    for group in &groups {
                        let store = pool.group(group.clone()).await.unwrap();
                        let (resident, lease) =
                            registry.reserve_replica(group.clone(), node).unwrap();
                        let log = TypedLogStore::<DataConfig>::new(store.clone(), node)
                            .await
                            .unwrap()
                            .with_resident(lease.clone())
                            .unwrap();
                        let machine = ParticipantMachine::new(store.clone()).unwrap();
                        let state = HostedState::new(machine.clone(), lease.clone()).unwrap();
                        let config = batched_config(openraft::Config {
                            enable_elect: node == 1,
                            heartbeat_interval: 150,
                            election_timeout_min: 1500,
                            election_timeout_max: 3000,
                            ..Default::default()
                        })
                        .unwrap();
                        let network = TypedNetworkFactory::<DataConfig>::new(
                            transport.client(),
                            members[&node].clone(),
                            lease.clone(),
                            shared.clone(),
                        )
                        .unwrap();
                        let consensus = RaftConsensus::start(config, network, log, state)
                            .await
                            .unwrap();
                        direct_leadership::watch(&consensus);
                        let endpoint = Arc::new(
                            ReplicaEndpoint::new(
                                consensus.raft.clone(),
                                consensus.state.clone(),
                                lease,
                                shared.clone(),
                                members.clone(),
                            )
                            .unwrap(),
                        );
                        for purpose in [Purpose::Raft, Purpose::RaftProgress, Purpose::Snapshot] {
                            resident.bind(purpose, endpoint.clone()).unwrap();
                        }
                        let dyn_consensus: Arc<
                            dyn Consensus<ParticipantCommand, ParticipantReply>,
                        > = consensus.clone();
                        let handle = Arc::new(ParticipantHandle::new(machine, dyn_consensus));
                        crate::oltp_replication::service_rpc::direct::register_replica(&handle);
                        replicas.push(MReplica {
                            consensus,
                            handle,
                            _resident: resident,
                        });
                    }
                    let (stop, stopped) = oneshot::channel::<()>();
                    let listener = TcpListener::from_std(listener).unwrap();
                    nabedra_rt::task::spawn(transport.run(listener, async {
                        let _ = stopped.await;
                    }));
                    let executor = DirectExecutor::new(
                        shared.clone(),
                        DirectConfig::default(),
                        gate,
                        schema,
                        DirectTimestampClient::new(None, timestamps),
                    );
                    (shared, pool, replicas, executor, stop)
                }
            };
            let (shared, pool, groups, executor, stop) = nabedra_rt::task::spawn_on(runtime.handle(), build).await.unwrap();
            let threads = tids().difference(&before).copied().collect();
            voters.push(MVoter {
                node,
                runtime: Some(runtime),
                shared,
                pool,
                executor,
                groups,
                threads,
                stop: Some(stop),
            });
        }
        let measure = Self {
            gate: gate.clone(),
            voters,
            groups,
            spans,
            table,
            _dir: dir,
            driver_threads,
        };
        let authority = Authority {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 1,
        };
        for (g, (lo, hi)) in measure.spans.clone().into_iter().enumerate() {
            measure.voters[0].groups[g]
                .consensus
                .raft
                .initialize(members.clone())
                .await
                .unwrap();
            let span = crate::oltp::mvcc::store::KeySpan {
                start: (g > 0).then_some(Key::Int64(lo)),
                end: (g + 1 < measure.spans.len()).then_some(Key::Int64(hi)),
            };
            let (transition, catalog) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
            let handle = measure.voters[0].groups[g].handle.clone();
            watchdog(|| async {
                handle
                    .fence_initial(transition, authority, catalog, 1, 1, span.clone())
                    .await
                    .ok()
            })
            .await;
            let clock = handle.direct_clock().unwrap().clone();
            watchdog(|| async { clock.leader_token().map(|_| ()) }).await;
            let lifecycle = table;
            watchdog(|| async {
                handle
                    .change_direct_schema(
                        crate::oltp::mvcc::store::direct::DirectSchemaChange::Activate {
                            lifecycle,
                        },
                    )
                    .await
                    .ok()
            })
            .await;
        }
        measure
    }

    fn range_of(&self, key: i64) -> usize {
        self.spans
            .iter()
            .position(|(lo, hi)| key >= *lo && key < *hi)
            .unwrap()
    }

    fn request(&self, group: usize, operation: &str, payload: serde_json::Value) -> DirectRequest {
        let request_id = nabedra_rt::ids::uuid_v4();
        let mut payload = payload;
        payload["table_id"] = serde_json::json!(self.table);
        let body = serde_json::to_vec(&serde_json::json!({
            "request_id": request_id,
            "operation": operation,
            "payload": payload,
        }))
        .unwrap();
        DirectRequest {
            database: self.groups[group].database.clone(),
            table_id: self.table,
            schema_epoch: 1,
            routing_epoch: 1,
            group: self.groups[group].group_id,
            request_id,
            digest: Digest::from_bytes(Sha256::digest(&body).into()),
            body: self.voters[0].shared.copy_bytes(&body).unwrap(),
            deadline: Duration::from_secs(10),
        }
    }

    /// Runs one request on the leader's runtime; returns the response and
    /// the time from `due` (its scheduled arrival) to its completion.
    fn spawn(
        &self,
        group: usize,
        request: DirectRequest,
        due: std::time::Instant,
    ) -> tokio::task::JoinHandle<(DirectResponse, Duration)> {
        let executor = self.voters[0].executor.clone();
        let handle = self.voters[0].groups[group].handle.clone();
        let runtime = self.voters[0].runtime.as_ref().unwrap().handle();
        nabedra_rt::task::spawn_on(runtime, async move {
            let response = executor.execute(&handle, request).await;
            (response, nabedra_rt::time::elapsed(due))
        })
    }

    pub(super) async fn seed(&self, rows: i64) {
        for (g, (lo, hi)) in self.spans.clone().into_iter().enumerate() {
            let mut start = lo;
            while start < hi {
                let end = (start + 1000).min(hi);
                let rows: Vec<_> = (start..end)
                    .map(
                        |id| serde_json::json!({"id": id, "value": 0, "tag": "seed", "score": 0.5}),
                    )
                    .collect();
                let request =
                    self.request(g, "table.insert_many", serde_json::json!({ "rows": rows }));
                let (response, _) = self
                    .spawn(g, request, nabedra_rt::time::now_std())
                    .await
                    .unwrap();
                assert!(
                    matches!(response, DirectResponse::Done(_)),
                    "seed {response:?}"
                );
                start = end;
            }
        }
        let _ = rows;
    }

    fn cpu(&self) -> Cpu {
        let mut cpu = Cpu {
            total: process_cpu_ns(),
            ..Default::default()
        };
        for (tid, (name, ns)) in thread_cpu() {
            let voter = self
                .voters
                .iter()
                .position(|v| v.threads.contains(&tid) || name == format!("gc1-v{}", v.node));
            match voter {
                Some(v) => cpu.voters[v] += ns,
                None if self.driver_threads.contains(&tid) => cpu.driver += ns,
                None => cpu.other += ns,
            }
        }
        cpu
    }

    fn storage(&self) -> Vec<nabedra_kv::StoreStats> {
        self.voters
            .iter()
            .map(|v| v.pool.kv_store().stats())
            .collect()
    }

    pub(super) async fn shutdown(mut self) {
        for voter in &mut self.voters {
            for replica in &voter.groups {
                let _ = replica.consensus.shutdown().await;
            }
            if let Some(stop) = voter.stop.take() {
                let _ = stop.send(());
            }
        }
        for voter in &mut self.voters {
            if let Some(runtime) = voter.runtime.take() {
                runtime.shutdown_background();
            }
        }
    }
}

fn stage_counts(snapshot: &serde_json::Value, name: &str) -> (u64, u64) {
    let stage = &snapshot[name];
    (
        stage["count"].as_u64().unwrap_or(0),
        stage["sum_us"].as_u64().unwrap_or(0),
    )
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index].as_secs_f64() * 1e3
}

const STAGES: &[&str] = &[
    "leader.mutation.total",
    "leader.attempt.propose",
    "raft.data.client_write",
    "raft.data.append",
    "raft.data.append_encode",
    "raft.data.append_write",
    "raft.data.apply_entry",
    "raft.data.apply_machine",
    "raft.data.append_merged",
    "raft.data.append_order_wait",
    "raft.data.net.append",
    "raft.data.net.append_handle",
];

/// One open-loop phase (`rate` > 0) or a closed-loop serial phase
/// (`rate` = 0, `serial` requests one after another).
/// `insert_rows` = 0: updates of seeded rows. Otherwise each request
/// inserts that many fresh rows (`table.insert` for 1, `table.insert_many`
/// above) into the last range; per-write figures are then per row.
async fn phase(
    measure: &Measure,
    rate: f64,
    seconds: f64,
    serial: usize,
    rows: i64,
    insert_rows: usize,
) -> serde_json::Value {
    static FRESH: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1 << 40);
    let make = |key: i64, i: usize| -> (usize, DirectRequest) {
        match insert_rows {
            0 => {
                let g = measure.range_of(key);
                let request = measure.request(
                    g,
                    "table.update",
                    serde_json::json!({"key": key, "values": {"value": i as i64}}),
                );
                (g, request)
            }
            count => {
                let first = FRESH.fetch_add(count as i64, Ordering::Relaxed);
                let row = |id: i64| serde_json::json!({"id": id, "value": i as i64, "tag": "gc1", "score": 0.5});
                let g = measure.groups.len() - 1;
                let request = if count == 1 {
                    measure.request(g, "table.insert", serde_json::json!({"row": row(first)}))
                } else {
                    let rows: Vec<_> = (first..first + count as i64).map(row).collect();
                    measure.request(g, "table.insert_many", serde_json::json!({"rows": rows}))
                };
                (g, request)
            }
        }
    };
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (rate as u64);
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let stages_before = crate::oltp_runtime::stage_timing::snapshot();
    let storage_before = measure.storage();
    let direct_before = measure.voters[0].executor.stats();
    let cpu_before = measure.cpu();
    let started = nabedra_rt::time::now_std();
    let mut latencies = Vec::new();
    let (mut ok, mut refused, mut failed) = (0u64, 0u64, 0u64);
    let mut count = |response: &DirectResponse,
                     latency: Duration,
                     latencies: &mut Vec<Duration>| {
        match response {
            DirectResponse::Done(_) => {
                ok += 1;
                latencies.push(latency);
            }
            DirectResponse::Failed(error) if error.code == ErrorCode::Overloaded => refused += 1,
            _ => failed += 1,
        }
    };
    if rate == 0.0 {
        for i in 0..serial {
            let key = (next() % rows as u64) as i64;
            let (g, request) = make(key, i);
            let (response, latency) = measure
                .spawn(g, request, nabedra_rt::time::now_std())
                .await
                .unwrap();
            count(&response, latency, &mut latencies);
        }
    } else {
        let total = (rate * seconds) as usize;
        let interval = Duration::from_secs_f64(1.0 / rate);
        let origin = nabedra_rt::time::now();
        let origin_std = nabedra_rt::time::now_std();
        let mut tasks = Vec::with_capacity(total);
        for i in 0..total {
            let due = origin + interval * i as u32;
            nabedra_rt::time::sleep_until(due).await;
            let key = (next() % rows as u64) as i64;
            let (g, request) = make(key, i);
            // Latency from the scheduled arrival (open loop).
            let task = measure.spawn(g, request, origin_std + interval * i as u32);
            tasks.push(task);
        }
        for task in tasks {
            let (response, latency) = task.await.unwrap();
            count(&response, latency, &mut latencies);
        }
    }
    let wall = nabedra_rt::time::elapsed(started).as_secs_f64();
    let cpu_after = measure.cpu();
    let storage_after = measure.storage();
    let stages_after = crate::oltp_runtime::stage_timing::snapshot();
    let direct_after = measure.voters[0].executor.stats();
    latencies.sort();
    // Requests that succeeded become rows written for the per-write figures.
    let requests = ok;
    let ok = ok * insert_rows.max(1) as u64;
    let per = |ns: u64| {
        if ok == 0 {
            0.0
        } else {
            ns as f64 / 1e6 / ok as f64
        }
    };
    let mut stages = serde_json::Map::new();
    for name in STAGES {
        let (c0, s0) = stage_counts(&stages_before, name);
        let (c1, s1) = stage_counts(&stages_after, name);
        stages.insert(
            (*name).to_owned(),
            serde_json::json!({
                "count": c1 - c0,
                "per_write": if ok == 0 { 0.0 } else { (c1 - c0) as f64 / ok as f64 },
                "mean_us": if c1 == c0 { 0.0 } else { (s1 - s0) as f64 / (c1 - c0) as f64 },
            }),
        );
    }
    let appends = stage_counts(&stages_after, "raft.data.append").0
        - stage_counts(&stages_before, "raft.data.append").0;
    let applied = stage_counts(&stages_after, "raft.data.apply_entry").0
        - stage_counts(&stages_before, "raft.data.apply_entry").0;
    let storage: Vec<serde_json::Value> = storage_before
        .iter()
        .zip(&storage_after)
        .map(|(b, a)| {
            serde_json::json!({
                "journal_syncs_per_write": (a.journal_syncs - b.journal_syncs) as f64 / ok.max(1) as f64,
                "commits_per_write": (a.commits - b.commits) as f64 / ok.max(1) as f64,
                "journal_bytes_per_write": (a.journal_bytes - b.journal_bytes) as f64 / ok.max(1) as f64,
                "user_bytes_per_write": (a.user_bytes - b.user_bytes) as f64 / ok.max(1) as f64,
            })
        })
        .collect();
    let cpu_voters: Vec<f64> = (0..3)
        .map(|v| per(cpu_after.voters[v] - cpu_before.voters[v]))
        .collect();
    serde_json::json!({
        "ranges": measure.groups.len(),
        "workload": if insert_rows == 0 { "update".to_owned() } else { format!("insert x{insert_rows}") },
        "requests_ok": requests,
        "offered_per_s": rate,
        "serial": rate == 0.0,
        "wall_s": wall,
        "ok": ok,
        "refused": refused,
        "failed": failed,
        "ack_per_s": ok as f64 / wall,
        "p50_ms": percentile(&latencies, 0.50),
        "p99_ms": percentile(&latencies, 0.99),
        "cpu_ms_per_write": {
            "process": per(cpu_after.total - cpu_before.total),
            "leader": cpu_voters[0],
            "follower2": cpu_voters[1],
            "follower3": cpu_voters[2],
            "driver": per(cpu_after.driver - cpu_before.driver),
            "other": per(cpu_after.other - cpu_before.other),
        },
        "cpu_cores": {
            "leader": (cpu_after.voters[0] - cpu_before.voters[0]) as f64 / 1e9 / wall,
            "follower2": (cpu_after.voters[1] - cpu_before.voters[1]) as f64 / 1e9 / wall,
            "process": (cpu_after.total - cpu_before.total) as f64 / 1e9 / wall,
        },
        "entries_per_append": if appends == 0 { 0.0 } else { applied as f64 / appends as f64 },
        "direct_entries_proposed": direct_after.entries_proposed - direct_before.entries_proposed,
        "direct_batch_entries": direct_after.batch_entries - direct_before.batch_entries,
        "direct_batched_commits": direct_after.batched_commits - direct_before.batched_commits,
        "data_entries_per_write": if ok == 0 { 0.0 } else { applied as f64 / 3.0 / requests as f64 },
        "stages": stages,
        "storage": storage,
    })
}

/// GC1 step 1. Prints one `GC1 {json}` line per phase.
#[test]
#[ignore = "GC1 measurement; run in release with --ignored"]
fn gc1_measure_direct_write_cost() {
    let list = |key: &str, default: &str| -> Vec<f64> {
        std::env::var(key)
            .unwrap_or_else(|_| default.to_owned())
            .split(',')
            .map(|v| v.trim().parse().unwrap())
            .collect()
    };
    let rates = list("GC1_RATES", "100,400,1000");
    let ranges = list("GC1_RANGES", "1,8");
    let seconds = list("GC1_SECONDS", "10")[0];
    let serial = list("GC1_SERIAL", "300")[0] as usize;
    let rows = list("GC1_ROWS", "5000")[0] as i64;
    let packing = list("GC1_PACKING", "800");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("gc1-driver")
        .enable_all()
        .build()
        .unwrap();
    let modes: Vec<bool> = std::env::var("GC1_MODES")
        .unwrap_or_else(|_| "on".to_owned())
        .split(',')
        .map(|mode| mode.trim() == "on")
        .collect();
    let batches: Vec<bool> = std::env::var("GC1_BATCH")
        .unwrap_or_else(|_| "off".to_owned())
        .split(',')
        .map(|mode| mode.trim() == "on")
        .collect();
    let modes: Vec<(bool, bool)> = modes
        .iter()
        .flat_map(|coalesce| batches.iter().map(move |batch| (*coalesce, *batch)))
        .collect();
    runtime.block_on(async {
        for (coalesce, batch) in modes {
            crate::oltp_replication::typed_log::COALESCE_APPENDS.store(coalesce, Ordering::Relaxed);
            for &ranges in &ranges {
                let measure = Measure::new(ranges as usize, rows).await;
                measure.gate.2.store(batch, Ordering::Release);
                measure.seed(rows).await;
                // Warm up the sessions, caches and the timestamp client.
                let _ = phase(&measure, 50.0, 2.0, 0, rows, 0).await;
                let mut records = Vec::new();
                if ranges as usize == 1 && serial > 0 {
                    records.push(phase(&measure, 0.0, 0.0, serial, rows, 0).await);
                }
                for &rate in &rates {
                    records.push(phase(&measure, rate, seconds, 0, rows, 0).await);
                }
                // Upper bound for packing requests into one entry: the same
                // row rate as single-row inserts and as 8-row `insert_many`.
                if ranges as usize == 1 {
                    for &row_rate in &packing {
                        records.push(phase(&measure, row_rate, seconds, 0, rows, 1).await);
                        records.push(phase(&measure, row_rate / 8.0, seconds, 0, rows, 8).await);
                    }
                }
                for mut record in records {
                    record["coalesce_appends"] = serde_json::json!(coalesce);
                    record["direct_batch"] = serde_json::json!(batch);
                    println!("\nGC1 {record}");
                }
                measure.shutdown().await;
            }
        }
    });
    runtime.shutdown_background();
}
