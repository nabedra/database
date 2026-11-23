//! K01 expiry, K02 capacity and K03 ownership and replica fault-model checks.

use std::path::{Path, PathBuf};

use nabedra_check::{Gen, checker::relaxed_kv, history::write_jsonl};
use nabedra_sim::{Filters, Nemesis, Profile, SeedRun, SweepConfig, sweep};
use serde_json::json;

use super::super::{clock::ClockMode, protocol::Admin};
use super::harness::{Cluster, Config};
use super::history::Op;
use super::hooks::Knobs;
use crate::cache::{
    command::{SLOT_COUNT, key_slot},
    store::EvictionPolicy,
    wire::Response,
};

#[path = "mutants.rs"]
mod mutants;

const FAULT_MS: u64 = 20_000;
const HEAL_MS: u64 = 15_000;
const FIRST_WORKER: u64 = 11;
const MIB: usize = 1 << 20;
const REPLAY: &str = concat!(
    "RUSTFLAGS='--cfg tokio_unstable' cargo test --release --no-default-features ",
    "--features cache,sim --lib cache::cluster::sim::tests::seed_sweep -- --exact --nocapture"
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Counter,
    Register,
    Ttl,
    Memory,
}

impl Workload {
    fn for_seed(seed: u64) -> Self {
        [Self::Counter, Self::Register, Self::Ttl, Self::Memory][(seed % 4) as usize]
    }

    fn keys(self, groups: usize, worker_loss: bool) -> Vec<String> {
        let count = match self {
            Self::Counter => 32,
            Self::Register => 64,
            Self::Ttl => 16,
            Self::Memory => 48,
        };
        // A crashed worker loses every baseline. Placement repairs in ascending
        // order with a per-worker move cap. Keep the RAM-loss workload in the
        // first repair batch while exercising the full map in every run.
        let mut key_number = 0;
        let mut tag_in_group = |group: usize| {
            loop {
                let tag = format!("{{cache-sim:{self:?}:{key_number}}}");
                key_number += 1;
                let slot = usize::from(key_slot(tag.as_bytes()));
                let first = (group * SLOT_COUNT).div_ceil(groups);
                if (first..first + 32).contains(&slot) {
                    break tag;
                }
            }
        };
        let hot = tag_in_group(0);
        (0..count)
            .map(|i| match self {
                Self::Memory => format!("{hot}:{i}"),
                Self::Register if i < 16 => format!("{hot}:{i}"),
                _ if worker_loss => tag_in_group(0),
                _ => format!("cache-sim:{self:?}:{i}"),
            })
            .collect()
    }

    fn operation(self, choice: u64, serial: u64, g: &mut Gen) -> Op {
        match self {
            Self::Counter => {
                if choice < 8 {
                    Op::Incr(1 + g.below(7))
                } else {
                    Op::Get
                }
            }
            Self::Register => match choice {
                0 => Op::Del,
                1..4 => Op::Get,
                _ => Op::Set(format!("register-{serial}"), None),
            },
            Self::Ttl => match choice {
                0 => Op::Del,
                1 => Op::Expire(0),
                2 => Op::Expire(300 + g.below(700) as i64),
                3 | 4 => Op::Pttl,
                5 | 6 => Op::Get,
                _ => Op::Set(format!("ttl-{serial}"), Some(200 + g.below(900))),
            },
            Self::Memory => Op::Set(memory_value(serial, 4 + g.below(61) as usize), None),
        }
    }
}

fn memory_value(serial: u64, kib: usize) -> String {
    let prefix = format!("{serial}:");
    let mut value = String::with_capacity(kib << 10);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', (kib << 10) - prefix.len()));
    value
}

fn config(g: &mut Gen) -> Config {
    let groups = 3 + g.below(2) as usize;
    Config {
        nodes: groups + 1,
        groups,
        actors: 1 + g.below(2) as usize,
        slot_bytes: MIB << g.below(2),
        eviction: if g.bool() {
            EvictionPolicy::NoEviction
        } else {
            EvictionPolicy::SampledLru
        },
        clock_mode: ClockMode::ClockBound,
        base_error_ns: 200_000 + g.below(1_800_001),
        declared_ppm: 10 + g.below(141) as u32,
        skew: g.bool(),
        gradual_drift: true,
        knobs: Knobs::default(),
        check_replica_prefix: true,
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Clock,
    Delay,
    PauseWorker,
    PartitionPrimary,
    PartitionZone,
    ReplicaLag,
    Rebalance,
    CrashController,
    CrashWorker,
    RecoverLinks,
    RestartController(u64),
    RestartWorker(u64),
}

fn profile(worker_loss: bool) -> Profile<Action> {
    let profile = Profile::new("cache-cluster", (150, 1200))
        .with(Action::Clock, 3)
        .with(Action::Delay, 2)
        .with(Action::PauseWorker, 2)
        .with(Action::PartitionPrimary, 3)
        .with(Action::PartitionZone, 1)
        .with(Action::ReplicaLag, 2)
        .with(Action::Rebalance, 2)
        .with(Action::CrashController, 2);
    if worker_loss {
        profile.with(Action::CrashWorker, 4)
    } else {
        profile
    }
}

async fn apply_action(
    cluster: &mut Cluster,
    action: Action,
    slot: u16,
    nemesis: &mut Nemesis<Action>,
    g: &mut Gen,
    worker_crashed: &mut bool,
) {
    let workers: Vec<_> = cluster.workers.keys().copied().collect();
    let worker = workers[g.below(workers.len() as u64) as usize];
    let owner = cluster
        .slot(slot)
        .primary
        .map_or(worker, |owner| owner.node);
    let replica = cluster.slot(slot).replica.map_or(worker, |copy| copy.node);

    // A restarted worker has empty RAM. A second crash before complete repair
    // can legitimately destroy both copies. Exercise full loss through cold_reset.
    let recovery = match action {
        Action::RecoverLinks => {
            cluster.heal();
            return;
        }
        Action::RestartController(node) => {
            cluster.start_controller(node).await;
            return;
        }
        Action::RestartWorker(node) => {
            cluster.start_worker(node).await;
            return;
        }
        Action::Clock => {
            let declared = i64::from(cluster.config.declared_ppm);
            cluster.drift(worker, g.range_i64(-declared, declared));
            return;
        }
        _ if nemesis.pending_follow_ups() != 0 => return,
        Action::Delay => {
            cluster.filters(Filters {
                drop_per_mille: g.below(51),
                dup_per_mille: 0,
                delay_max_ms: 1 + g.below(15),
            });
            Some((200 + g.below(500), Action::RecoverLinks))
        }
        Action::PauseWorker => {
            cluster.pause(worker);
            Some((500 + g.below(3_501), Action::RecoverLinks))
        }
        Action::PartitionPrimary => {
            cluster.isolate_controller_links(owner);
            Some((500 + g.below(3_501), Action::RecoverLinks))
        }
        Action::PartitionZone => {
            for peer in [1, 2, 3].into_iter().chain(workers) {
                if peer != worker {
                    cluster.link(worker, peer, true);
                    cluster.link(peer, worker, true);
                }
            }
            Some((500 + g.below(2_501), Action::RecoverLinks))
        }
        Action::ReplicaLag => {
            if replica == owner {
                return;
            }
            cluster.link(owner, replica, true);
            cluster.link(replica, owner, true);
            Some((300 + g.below(600), Action::RecoverLinks))
        }
        Action::Rebalance => {
            if worker != owner && worker != replica {
                let _ = cluster
                    .admin(Admin::Rebalance {
                        first: slot,
                        last: slot,
                        target: worker,
                    })
                    .await;
            }
            None
        }
        Action::CrashController => {
            let node = if g.bool() {
                cluster.leader().unwrap_or(1)
            } else {
                1 + g.below(3)
            };
            cluster.crash(node);
            Some((250 + g.below(1_001), Action::RestartController(node)))
        }
        Action::CrashWorker if *worker_crashed => return,
        Action::CrashWorker => {
            *worker_crashed = true;
            let node = if g.bool() { owner } else { replica };
            cluster.crash(node);
            Some((700 + g.below(2_000), Action::RestartWorker(node)))
        }
    };
    if let Some((delay, recovery)) = recovery {
        nemesis.after(cluster.now() + delay, recovery);
    }
}

fn assert_reply(
    cluster: &Cluster,
    key: &str,
    result: Option<Response>,
    expected: i64,
    description: &str,
) {
    if !matches!(result, Some(Response::Integer(actual)) if actual == expected) {
        cluster.violate(
            "TtlSemantics",
            format!(
                "{key}: {description} expected {expected}, got {}",
                reply_kind(&result)
            ),
        );
    }
}

fn reply_kind(response: &Option<Response>) -> String {
    match response {
        Some(Response::Error(error)) => String::from_utf8_lossy(error).into_owned(),
        Some(Response::Integer(value)) => value.to_string(),
        Some(Response::Bulk(None)) => "nil".into(),
        Some(_) => "other successful reply".into(),
        None => "timeout".into(),
    }
}

async fn assert_ttl(cluster: &mut Cluster, key: &str) {
    let result = cluster.send(key, Op::Pttl, None).await;
    assert_reply(cluster, key, result, -2, "PTTL of an absent key");
    let _ = cluster
        .send(key, Op::Set("persistent".into(), Some(200)), None)
        .await;
    let result = cluster.send(key, Op::Persist, None).await;
    assert_reply(cluster, key, result, 1, "PERSIST of an expiring key");
    cluster.step(210).await;
    let result = cluster.send(key, Op::Pttl, None).await;
    assert_reply(cluster, key, result, -1, "PTTL after PERSIST");
    let result = cluster.send(key, Op::Expire(0), None).await;
    assert_reply(cluster, key, result, 1, "non-positive PEXPIRE");
    let result = cluster.send(key, Op::Pttl, None).await;
    assert_reply(cluster, key, result, -2, "PTTL after non-positive PEXPIRE");

    let _ = cluster
        .send(key, Op::Set("expiring".into(), Some(300)), None)
        .await;
    let first = cluster.send(key, Op::Pttl, None).await;
    cluster.step(50).await;
    let second = cluster.send(key, Op::Pttl, None).await;
    if !matches!((first, second), (Some(Response::Integer(a)), Some(Response::Integer(b))) if b > 0 && b < a)
    {
        cluster.violate("TtlSemantics", format!("{key}: reading extended its TTL"));
    }
    cluster.step(270).await;
    let result = cluster.send(key, Op::Pttl, None).await;
    assert_reply(cluster, key, result, -2, "PTTL after expiry");
}

async fn assert_memory(cluster: &mut Cluster, keys: &[String]) {
    let mut oom = 0;
    let mut ok = 0;
    for (serial, key) in keys.iter().enumerate() {
        match cluster
            .send(key, Op::Set(memory_value(serial as u64, 64), None), None)
            .await
        {
            Some(Response::Simple(_)) => ok += 1,
            Some(Response::Error(error)) if error.starts_with(b"OOM") => oom += 1,
            response => {
                cluster.violate(
                    "MemoryCap",
                    format!("pressure write failed: {}", reply_kind(&response)),
                );
                return;
            }
        }
    }
    let valid = match cluster.config.eviction {
        EvictionPolicy::NoEviction => oom > 0 && ok > 0,
        EvictionPolicy::SampledLru => oom == 0 && ok == keys.len(),
    };
    if !valid {
        cluster.violate(
            "MemoryCap",
            format!(
                "pressure writes returned {ok} OK and {oom} OOM with {:?}",
                cluster.config.eviction
            ),
        );
    }
}

async fn cold_reset(cluster: &mut Cluster) {
    let nodes: Vec<_> = cluster.workers.keys().copied().collect();
    for &node in &nodes {
        cluster.crash(node);
    }
    cluster.step(super::super::protocol::LEASE_MS + 1000).await;
    for node in nodes {
        cluster.start_worker(node).await;
    }
    cluster.step(2_000).await;
    if !matches!(
        cluster.admin(Admin::ColdReset { slot: None }).await,
        super::super::protocol::Reply::Applied { .. }
    ) {
        cluster.violate(
            "ColdStartEmpty",
            "controller refused reset after full worker stop".into(),
        );
    }
    cluster.step(HEAL_MS).await;
    cluster.verify_quiescence(true).await;
}

fn result(cluster: &Cluster, seed: u64, workload: Workload) -> SeedRun {
    let observed = cluster.observed.lock().expect("observation lock");
    let mut history = Vec::new();
    write_jsonl(&mut history, &observed.history).expect("write Cache history");
    // Sampled-LRU pressure can remove values independently of replica or expiry
    // behavior. That workload checks admission and capacity directly via the actors.
    let mut violations: Vec<_> = observed
        .violations
        .iter()
        .map(|(kind, detail)| format!("{kind}: {detail}"))
        .collect();
    let options = relaxed_kv::Options {
        expiry_tolerance_ns: 2
            * (cluster.config.base_error_ns
                + u64::from(cluster.config.declared_ppm) * cluster.now()
                + 1000)
            + 1_000_000,
        ..relaxed_kv::Options::default()
    };
    let verdict =
        if workload == Workload::Memory && cluster.config.eviction == EvictionPolicy::SampledLru {
            let events = observed
                .history
                .iter()
                .filter(|event| event.f != "get")
                .cloned()
                .collect::<Vec<_>>();
            relaxed_kv::check_with(&events, &options)
        } else {
            relaxed_kv::check_with(&observed.history, &options)
        };
    violations.extend(
        verdict
            .anomalies
            .into_iter()
            .map(|anomaly| format!("{}: {}", anomaly.kind, anomaly.detail)),
    );
    SeedRun {
        seed,
        trace_hash: observed.trace.hash(),
        trace_events: observed.trace.events(),
        violations,
        trace_tail: observed.trace.tail(),
        full_trace: observed.trace.full().map(<[_]>::to_vec),
        history,
        config: json!({
            "workload": format!("{workload:?}"),
            "actors": cluster.config.actors,
            "workers": cluster.config.nodes,
            "groups": cluster.config.groups,
            "slot_bytes": cluster.config.slot_bytes,
            "eviction": format!("{:?}", cluster.config.eviction),
            "base_error_ns": cluster.config.base_error_ns,
            "declared_ppm": cluster.config.declared_ppm,
            "skew": cluster.config.skew,
        }),
        virtual_ms: cluster.now(),
        tape: Vec::new(),
    }
}

fn run_seed(seed: u64, g: &mut Gen) -> SeedRun {
    let workload = Workload::for_seed(seed);
    let config = config(g);
    nabedra_sim::runtime::run(seed, |world| async move {
        let worker_loss = (4..8).contains(&(seed % 16));
        let keys = workload.keys(config.groups, worker_loss);
        let mut cluster = Cluster::boot(world, keys, config).await;
        let keys = cluster.keys.clone();
        match workload {
            Workload::Ttl => assert_ttl(&mut cluster, &keys[0]).await,
            Workload::Memory => assert_memory(&mut cluster, &keys).await,
            _ => {}
        }
        let start = cluster.now();
        let end = start + FAULT_MS;
        let mut nemesis = Nemesis::new(profile(worker_loss), start, g);
        let mut serial = 10_000;
        let mut completed = 0;
        let mut worker_crashed = false;
        while cluster.now() < end {
            let key = g.pick(&keys);
            let slot = key_slot(key.as_bytes());
            for action in nemesis.poll(cluster.now(), g) {
                apply_action(
                    &mut cluster,
                    action,
                    slot,
                    &mut nemesis,
                    g,
                    &mut worker_crashed,
                )
                .await;
            }
            let op = workload.operation(g.below(10), serial, g);
            let node = (serial % 8 == 0).then(|| FIRST_WORKER + g.below(config.nodes as u64));
            if cluster
                .send(key, op, node)
                .await
                .is_some_and(|response| !matches!(response, Response::Error(_)))
            {
                completed += 1;
            }
            serial += 1;
            cluster.step(40 + g.below(121)).await;
        }
        if completed == 0 || nemesis.picked() == 0 {
            cluster.violate(
                "Liveness",
                format!(
                    "workload {workload:?} completed {completed} operations with {} fault actions",
                    nemesis.picked()
                ),
            );
        }

        for action in nemesis.stop() {
            apply_action(
                &mut cluster,
                action,
                key_slot(keys[0].as_bytes()),
                &mut nemesis,
                g,
                &mut worker_crashed,
            )
            .await;
        }
        cluster.heal();
        cluster.step(HEAL_MS).await;
        cluster.verify_quiescence(false).await;
        if seed.is_multiple_of(16) {
            cold_reset(&mut cluster).await;
        }
        result(&cluster, seed, workload)
    })
}

fn keep_history(dir: &Path, run: &SeedRun) {
    std::fs::write(dir.join(format!("cache-{}.jsonl", run.seed)), &run.history)
        .expect("write retained Cache history");
}

#[test]
fn seed_sweep() {
    let config = SweepConfig::from_env("cache", REPLAY);
    let histories = std::env::var_os("NABEDRA_SIM_HISTORY_DIR").map(PathBuf::from);
    if let Some(dir) = &histories {
        std::fs::create_dir_all(dir).expect("create Cache history directory");
    }
    let report = sweep::sweep(&config, |seed, g| {
        let run = run_seed(seed, g);
        if let Some(dir) = &histories {
            keep_history(dir, &run);
        }
        run
    });
    println!(
        "NABEDRA_SIM_SUMMARY {}",
        json!({
            "seeds": report.evidence_seeds(),
            "virtual_hours": report.virtual_ms() as f64 / 3_600_000.0,
            "fault_model": "Cache K01 expiry, K02 memory, K03 cluster",
            "replayed": report.replayed(),
        })
    );
    report.assert_passed();
}

#[test]
fn same_seed_same_trace() {
    let start = sweep::env_u64("NABEDRA_SEED", 1);
    for seed in start..=start.saturating_add(3) {
        let a = run_seed(seed, &mut sweep::workload_gen(seed));
        let b = run_seed(seed, &mut sweep::workload_gen(seed));
        assert!(a.passed(), "seed {seed}: {:?}", a.violations);
        assert_eq!(a.trace_hash, b.trace_hash, "seed {seed}");
        assert_eq!(a.trace_events, b.trace_events, "seed {seed}");
        assert_eq!(a.history, b.history, "seed {seed}");
        assert_eq!(a.violations, b.violations, "seed {seed}");
    }
}
