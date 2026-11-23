#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
#![cfg(all(feature = "oltp", unix))]
//! NS10 phase B: OLTP pools on `nabedra-kv` killed with SIGKILL under write load.
//!
//! A child process (this test binary, `kv_crash_child`) opens real development pools on
//! the real filesystem and runs one writer per group through the pool's public write
//! path, in the order a Raft host uses it:
//! 1. a synced log append (`log_transaction`, key `log/<i>`);
//! 2. the state batch with its applied position (`transaction` at position `i`, unsynced
//!    apply hint, key `st/<i>`);
//! 3. every 16 entries, a synced log truncation (`delete_range` below `i - 8`).
//!
//! Before each step it appends `P` (pending), and after each acknowledgement `L`, `S` or
//! `T`, to an ack file. The parent kills the child at a random moment, cold-starts every
//! pool in-process and checks:
//! - every acknowledged write is present (a process kill keeps every completed write,
//!   synced or not), with its exact value;
//! - nothing beyond what was submitted exists (outstanding requests are abandoned, never
//!   replayed or fabricated);
//! - the state is a contiguous prefix whose applied position equals its last entry, the
//!   state never runs ahead of the log, and truncated log entries stay gone (O11
//!   agreement of data, applied position, log and truncation);
//! - the pool opens without recovery-required state or quarantined groups.
//!
//! The child is a fixture around the pool's storage stack (worker shared resources,
//! executors, `nabedra-kv`), not the `nabedra-db` binary: no network, Raft or Control.
//! This is real SIGKILL evidence for the process-kill model only. Power loss is covered
//! on `FaultFs` (a fault model, not physical power-loss evidence: O29 wording) by the kv
//! campaign and the hosted Raft tests.

use nabedra_database_server::{
    config::WorkerResources,
    oltp::pool::{
        AppliedPosition, ApplyMeta, OltpPool, POOL_FORMAT_VERSION, PoolIdentity,
        SharedResourceConfig, TransactionBudget, Transition, WorkerSharedResources,
    },
    oltp_replication::types::{Digest, GroupIdentity, GroupKind, Text},
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

const CHILD_ENV: &str = "NABEDRA_KV_CRASH_CHILD";
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 64 << 10,
    batch_bytes: 64 << 10,
    output_bytes: 64 << 10,
};
const TRUNCATE_EVERY: u64 = 16;
const KEEP_LOG: u64 = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Worker {
    dir: PathBuf,
    pool: PoolIdentity,
    groups: Vec<GroupIdentity>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChildConfig {
    worker: Worker,
    /// First entry index per group.
    start: Vec<u64>,
    acks: PathBuf,
    /// Value size salt, fixed per worker for the whole test.
    pad: usize,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn shared(id: u64) -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        id,
        WorkerResources {
            max_threads: 8,
            memory_limit_bytes: 512 << 20,
            task_memory_limit_bytes: 128 << 20,
            control_memory_reserve_bytes: 64 << 20,
        },
        SharedResourceConfig {
            background_threads: 2,
            ..Default::default()
        },
    )
    .unwrap()
}

fn log_key(i: u64) -> Vec<u8> {
    format!("log/{i:010}").into_bytes()
}

fn state_key(i: u64) -> Vec<u8> {
    format!("st/{i:010}").into_bytes()
}

/// The value of entry `i`; its size varies with the entry and `salt` (from the run seed).
fn value(kind: &str, group: usize, i: u64, salt: usize) -> Vec<u8> {
    let pad = (i as usize * 131 + group * 17 + salt) % 700;
    let mut v = format!("{kind}:{group}:{i}:").into_bytes();
    v.resize(v.len() + pad, b'v');
    v
}

fn meta(i: u64) -> ApplyMeta {
    let mut digest = [0u8; 32];
    digest[..8].copy_from_slice(&i.to_be_bytes());
    ApplyMeta {
        position: AppliedPosition {
            term: 1,
            node_id: 1,
            index: i,
        },
        command_digest: Digest::from_bytes(digest),
        membership: None,
    }
}

/// The child worker: writes until killed.
#[test]
#[ignore = "fixture worker, started only by this integration test"]
fn kv_crash_child() {
    let path = std::env::var(CHILD_ENV).expect("child config is required");
    let config: ChildConfig = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(child(config));
}

async fn child(config: ChildConfig) {
    let acks = Arc::new(Mutex::new(
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.acks)
            .unwrap(),
    ));
    let ack = move |acks: &Mutex<fs::File>, kind: char, group: usize, i: u64| {
        let line = format!("{kind} {group} {i}\n");
        acks.lock().unwrap().write_all(line.as_bytes()).unwrap();
    };
    let pool = OltpPool::open_development(
        &config.worker.dir,
        config.worker.pool.clone(),
        shared(config.worker.pool.pool_id.as_u128() as u64),
    )
    .unwrap();
    let mut tasks = Vec::new();
    for (g, identity) in config.worker.groups.iter().enumerate() {
        let group = pool.group(identity.clone()).await.unwrap();
        let acks = acks.clone();
        let start = config.start[g];
        let pad = config.pad;
        tasks.push(tokio::spawn(async move {
            for i in start.. {
                ack(&acks, 'P', g, i);
                let v = value("log", g, i, pad);
                group
                    .log_transaction(BUDGET, move |tx| {
                        tx.put(&log_key(i), &v)?;
                        Ok(Transition::Commit(tx.copy_output(b"l")?))
                    })
                    .await
                    .unwrap();
                ack(&acks, 'L', g, i);
                let v = value("st", g, i, pad);
                group
                    .transaction(meta(i), BUDGET, move |tx| {
                        tx.put(&state_key(i), &v)?;
                        Ok(Transition::Commit(tx.copy_output(b"s")?))
                    })
                    .await
                    .unwrap();
                ack(&acks, 'S', g, i);
                if i % TRUNCATE_EVERY == 0 && i > KEEP_LOG {
                    let below = i - KEEP_LOG;
                    group
                        .log_transaction(BUDGET, move |tx| {
                            tx.delete_range(&log_key(0), &log_key(below))?;
                            Ok(Transition::Commit(tx.copy_output(b"t")?))
                        })
                        .await
                        .unwrap();
                    ack(&acks, 'T', g, below);
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// What the ack file says about one group.
#[derive(Clone, Debug, Default)]
struct Acked {
    pending: Option<u64>,
    log: Option<u64>,
    state: Option<u64>,
    truncated_below: u64,
}

fn read_acks(path: &Path, groups: usize) -> Vec<Acked> {
    let mut out = vec![Acked::default(); groups];
    let Ok(text) = fs::read_to_string(path) else {
        return out;
    };
    let complete = match text.rfind('\n') {
        Some(end) => &text[..=end],
        None => "",
    };
    for line in complete.lines() {
        let mut parts = line.split(' ');
        let kind = parts.next().unwrap();
        let g: usize = parts.next().unwrap().parse().unwrap();
        let i: u64 = parts.next().unwrap().parse().unwrap();
        let a = &mut out[g];
        match kind {
            "P" => a.pending = Some(a.pending.map_or(i, |p| p.max(i))),
            "L" => a.log = Some(a.log.map_or(i, |p| p.max(i))),
            "S" => a.state = Some(a.state.map_or(i, |p| p.max(i))),
            "T" => a.truncated_below = a.truncated_below.max(i),
            other => panic!("bad ack line kind {other}"),
        }
    }
    out
}

/// Across all runs of one worker so far.
#[derive(Clone, Debug, Default)]
struct History {
    acked: Vec<Acked>,
    /// Next entry index per group.
    next: Vec<u64>,
    pad: usize,
}

fn merge(history: &mut History, run: &[Acked]) {
    for (h, r) in history.acked.iter_mut().zip(run) {
        h.pending = h.pending.max(r.pending);
        h.log = h.log.max(r.log);
        h.state = h.state.max(r.state);
        h.truncated_below = h.truncated_below.max(r.truncated_below);
    }
}

fn spawn(dir: &Path, worker: &Worker, history: &History, run: usize) -> (Child, PathBuf) {
    let acks = dir.join(format!("acks-{}-{run}.txt", worker.pool.pool_id));
    let config = ChildConfig {
        worker: worker.clone(),
        start: history.next.clone(),
        acks: acks.clone(),
        pad: history.pad,
    };
    let path = dir.join(format!("child-{}.json", worker.pool.pool_id));
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let log =
        fs::File::create(dir.join(format!("child-{}-{run}.log", worker.pool.pool_id))).unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "kv_crash_child", "--nocapture"])
        .env(CHILD_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    (child, acks)
}

fn acked_lines(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|t| t.bytes().filter(|&b| b == b'\n').count())
        .unwrap_or(0)
}

/// SIGKILL, and reap.
fn kill(child: &mut Child) -> bool {
    use std::os::unix::process::ExitStatusExt;
    let running = child.try_wait().unwrap().is_none();
    let _ = child.kill();
    let status = child.wait().unwrap();
    assert!(
        !running || status.signal() == Some(libc::SIGKILL),
        "child exited on its own: {status:?}"
    );
    running
}

/// Presence per index in `lo..=hi`: 0 absent, 1 the expected value, 2 another value.
async fn presence(
    group: &nabedra_database_server::oltp::pool::GroupStore,
    g: usize,
    kind: &'static str,
    lo: u64,
    hi: u64,
    pad: usize,
) -> Vec<u8> {
    let work = move |view: &mut nabedra_database_server::oltp::pool::GroupRead<'_>| {
        let mut bits = Vec::with_capacity((hi + 1 - lo) as usize);
        for i in lo..=hi {
            let key = if kind == "log" {
                log_key(i)
            } else {
                state_key(i)
            };
            bits.push(match view.pinned(&key)? {
                None => 0,
                Some(v) if v.as_ref() == value(kind, g, i, pad).as_slice() => 1,
                Some(_) => 2,
            });
        }
        view.copy_output(&bits)
    };
    // Log entries live in the group's log namespace, state in its state namespace.
    let out = if kind == "log" {
        group.log_read(BUDGET, work).await
    } else {
        group.read(BUDGET, work).await
    }
    .unwrap();
    out.as_slice().to_vec()
}

/// Cold start: opens `worker`'s pool, checks every group against `history`, and sets
/// the next entry index per group. Returns the number of acknowledged writes checked.
async fn verify(worker: &Worker, history: &mut History, ctx: &str) -> u64 {
    let pool = OltpPool::open_development(
        &worker.dir,
        worker.pool.clone(),
        shared(worker.pool.pool_id.as_u128() as u64 ^ 1),
    )
    .unwrap_or_else(|e| panic!("{ctx}: cold start failed: {e:?}"));
    assert!(!pool.requires_recovery(), "{ctx}: pool requires recovery");
    assert!(
        pool.quarantined_groups().is_empty(),
        "{ctx}: quarantined {:?}",
        pool.quarantined_groups()
    );
    let mut checked = 0;
    for (g, identity) in worker.groups.iter().enumerate() {
        let acked = history.acked[g].clone();
        let Some(pending) = acked.pending else {
            continue;
        };
        let group = pool
            .group(identity.clone())
            .await
            .unwrap_or_else(|e| panic!("{ctx}: group {g}: {e:?}"));
        let first = 1;
        // Look a little past what was submitted: nothing may be there.
        let hi = pending + 2;
        let state = presence(&group, g, "st", first, hi, history.pad).await;
        let log = presence(&group, g, "log", first, hi, history.pad).await;
        let at = |bits: &[u8], i: u64| bits[(i - first) as usize];
        assert!(
            !state.contains(&2) && !log.contains(&2),
            "{ctx}: group {g}: a key holds a wrong value"
        );
        // State: a contiguous prefix, at least every acknowledged entry, at most pending.
        let applied_last = state
            .iter()
            .rposition(|&b| b == 1)
            .map(|p| p as u64 + first);
        if let Some(last) = applied_last {
            for i in first..=last {
                assert_eq!(
                    at(&state, i),
                    1,
                    "{ctx}: group {g}: state hole at {i} (last {last})"
                );
            }
            assert!(
                last <= pending,
                "{ctx}: group {g}: state {last} beyond pending {pending}"
            );
        }
        if let Some(s) = acked.state {
            assert!(
                applied_last.is_some_and(|l| l >= s),
                "{ctx}: group {g}: acknowledged state {s} lost (last present {applied_last:?})"
            );
        }
        let applied = group.applied_state(BUDGET).await.unwrap();
        let position = applied.map(|a| a.position.index);
        if let Some(last) = applied_last {
            assert_eq!(
                position,
                Some(last),
                "{ctx}: group {g}: applied position disagrees with the state"
            );
        }
        // Log: truncated entries stay gone; every acknowledged entry above the
        // truncation point is present; the state never runs ahead of the log.
        let log_last = log.iter().rposition(|&b| b == 1).map(|p| p as u64 + first);
        let log_first = log.iter().position(|&b| b == 1).map(|p| p as u64 + first);
        if let Some(first_present) = log_first {
            assert!(
                first_present >= acked.truncated_below.max(first),
                "{ctx}: group {g}: log {first_present} survived the acknowledged truncation below {}",
                acked.truncated_below
            );
            for i in first_present..=log_last.unwrap() {
                assert_eq!(at(&log, i), 1, "{ctx}: group {g}: log hole at {i}");
            }
            assert!(
                log_last.unwrap() <= pending,
                "{ctx}: group {g}: log beyond pending"
            );
        }
        if let Some(l) = acked.log
            && l >= acked.truncated_below
        {
            assert!(
                log_last.is_some_and(|last| last >= l),
                "{ctx}: group {g}: acknowledged log {l} lost (last present {log_last:?})"
            );
        }
        if let Some(last) = applied_last {
            assert!(
                log_last.is_some_and(|l| l >= last)
                    || last < acked.truncated_below.max(first) + KEEP_LOG,
                "{ctx}: group {g}: state {last} ahead of the log {log_last:?}"
            );
        }
        // Acknowledged state entries and acknowledged log entries above the truncation.
        checked += acked.state.map_or(0, |s| s + 1 - first)
            + acked.log.map_or(0, |l| {
                (l + 1).saturating_sub(acked.truncated_below.max(first))
            });
        history.next[g] = applied_last.map_or(first, |l| l + 1);
    }
    pool_close(pool).await;
    checked
}

async fn pool_close(pool: OltpPool) {
    // The store's last handle joins its threads: drop it off the runtime worker.
    tokio::task::spawn_blocking(move || drop(pool))
        .await
        .unwrap();
}

struct Scratch(PathBuf);
impl Scratch {
    fn new(name: &str) -> Scratch {
        let dir = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("nabedra-{name}-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn worker(root: &Path, cluster: Uuid, index: usize, groups: usize) -> Worker {
    let pool = PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: POOL_FORMAT_VERSION,
        cluster_id: cluster,
        worker_id: Uuid::new_v4(),
        pool_id: Uuid::new_v4(),
    };
    let dir = root.join(format!("worker{index}"));
    fs::create_dir_all(&dir).unwrap();
    Worker {
        dir,
        pool,
        groups: (0..groups)
            .map(|_| GroupIdentity {
                cluster_id: cluster,
                database: Text::try_from("default").unwrap(),
                group_id: Uuid::new_v4(),
                kind: GroupKind::Catalog,
                group_format_version: 1,
            })
            .collect(),
    }
}

/// SplitMix64.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Runs `rounds` rounds over `workers`: start every worker, wait for a random number
/// of acknowledgements, SIGKILL every worker, cold-start and verify every pool.
async fn kill_rounds(name: &str, workers: usize, groups: usize, rounds: u64, seed: u64) {
    let scratch = Scratch::new(name);
    let cluster = Uuid::new_v4();
    let mut rng = Rng(seed);
    let all: Vec<Worker> = (0..workers)
        .map(|i| worker(&scratch.0, cluster, i, groups))
        .collect();
    let mut histories: Vec<History> = (0..workers)
        .map(|_| History {
            acked: vec![Acked::default(); groups],
            next: vec![1; groups],
            pad: rng.below(1000) as usize,
        })
        .collect();
    let started = Instant::now();
    let (mut checked, mut killed_running, mut lines) = (0u64, 0u64, 0usize);
    for round in 0..rounds as usize {
        let target = 5 + rng.below(120) as usize;
        let mut children = Vec::new();
        for (w, h) in all.iter().zip(histories.iter_mut()) {
            children.push(spawn(&scratch.0, w, h, round));
        }
        // Kill at a random moment once every worker is writing (or after a deadline).
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let ready = children.iter().all(|(_, acks)| acked_lines(acks) >= target);
            if ready || Instant::now() > deadline {
                break;
            }
            for (child, _) in children.iter_mut() {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("{name} round {round}: a worker exited early: {status:?}");
                }
            }
            tokio::time::sleep(Duration::from_millis(1 + rng.below(5))).await;
        }
        for (child, _) in children.iter_mut() {
            killed_running += u64::from(kill(child));
        }
        for (w, ((_, acks), h)) in all.iter().zip(children.iter().zip(histories.iter_mut())) {
            lines += acked_lines(acks);
            let run = read_acks(acks, groups);
            merge(h, &run);
            checked += verify(
                w,
                h,
                &format!("{name} round {round} worker {}", w.pool.pool_id),
            )
            .await;
        }
    }
    eprintln!(
        "{name}: {rounds} rounds x {workers} workers x {groups} groups in {:.1} s: {killed_running} SIGKILLs of running workers, {lines} ack lines, {checked} acknowledged entries checked, summed over the cold starts (state, plus log above the truncation), final next indices {:?}",
        started.elapsed().as_secs_f64(),
        histories.iter().map(|h| h.next.clone()).collect::<Vec<_>>()
    );
    assert_eq!(
        killed_running,
        rounds * workers as u64,
        "every kill hit a running worker"
    );
}

/// NS16: SIGKILL of a worker process under write load, 50 iterations (`NS16_ITERATIONS`).
/// Every acknowledged write is present and recovery succeeds every time. Also O11: the
/// kills land between the log sync, the state batch with its applied position and the
/// log truncation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns16_o11_sigkill_worker_under_write_load() {
    let iterations = env_u64("NS16_ITERATIONS", 50);
    kill_rounds("ns16", 1, 4, iterations, env_u64("NS10_SEED_BASE", 0) ^ 16).await;
}

/// D09 and R04: kill every worker under write load, then cold-start them all
/// (`NS10_D09_ROUNDS`, default 10, three workers with their own pools). Committed
/// mutations remain; outstanding requests are abandoned, never replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d09_r04_kill_every_worker_under_load_then_cold_start() {
    let rounds = env_u64("NS10_D09_ROUNDS", 10);
    kill_rounds("d09", 3, 3, rounds, env_u64("NS10_SEED_BASE", 0) ^ 9).await;
}
