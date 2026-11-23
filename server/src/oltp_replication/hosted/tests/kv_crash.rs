//! NS10 phase B on `FaultFs`: a single-voter Raft group hosted on a pool whose store
//! runs on `FaultFs`, cut at sampled filesystem operations under concurrent proposals.
//!
//! Crash and power-loss evidence here is the `FaultFs` fault model, not a physical power
//! cut (O29 wording).
//!
//! - O11, D09: the machine stops after the n-th mutating operation, sampled over a run
//!   that crosses log syncs, state batches with their applied positions, snapshots
//!   (checkpoints) and log purges (truncation). The image is a crash (only synced bytes),
//!   a 512 B power loss or a 4 KiB power loss. After a reopen Raft replays the log; every
//!   acknowledged command is applied exactly once (the counter refuses a second
//!   application), the count equals the applied markers, and a new command applies once.
//! - O29: EIO on the journal sync while proposals are in flight. They fail ambiguously,
//!   the pool and its aliases latch, nothing retries, and after a reopen every
//!   acknowledged command is present and each ambiguous one is applied at most once;
//!   retrying the absent ones applies each exactly once.
use super::*;
use nabedra_kv::fs::{Fault, FaultError, FsOp, PowerLoss};
use openraft::type_config::async_runtime::watch::WatchReceiver;

const WAVE: u64 = 8;

/// A node whose Raft takes snapshots often and purges its log behind them.
async fn open_node(
    path: &Path,
    layout: &Layout,
    shared: Arc<WorkerSharedResources>,
    fs: &FaultFs,
) -> Node {
    let fs: Arc<dyn Fs> = Arc::new(fs.clone());
    let pool =
        OltpPool::open_development_on(path, layout.pool.clone(), shared.clone(), fs).unwrap();
    let store = pool.group(layout.group.clone()).await.unwrap();
    let registry = Arc::new(EndpointRegistry::new(shared));
    let (resident, lease) = registry.reserve_replica(layout.group.clone(), 1).unwrap();
    let log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
        .await
        .unwrap()
        .with_resident(lease.clone())
        .unwrap();
    let state = HostedState::new(
        Counter {
            store: store.clone(),
        },
        lease,
    )
    .unwrap();
    let config = batched_config(openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(24),
        max_in_snapshot_log_to_keep: 4,
        purge_batch_size: 1,
        ..Default::default()
    })
    .unwrap();
    let consensus = RaftConsensus::start(config, NoPeers, log, state)
        .await
        .unwrap();
    Node {
        consensus,
        store,
        pool,
        _registry: registry,
        _resident: resident,
    }
}

/// Proposes `ids` in concurrent waves. Returns the acknowledged `(id, index)` pairs and
/// the ids whose proposal failed or timed out (outcome unknown). Stops after the first
/// wave with a failure.
async fn propose_waves(node: &Node, ids: std::ops::Range<u64>) -> (Vec<(u64, u64)>, Vec<u64>) {
    let shared = node.store.resources().clone();
    let mut acked = Vec::new();
    let mut unknown = Vec::new();
    let mut next = ids.start;
    while next < ids.end {
        let wave: Vec<u64> = (next..(next + WAVE).min(ids.end)).collect();
        next += wave.len() as u64;
        let mut tasks = Vec::new();
        for &id in &wave {
            let consensus = node.consensus.clone();
            let command = OwnedCommand::new_owned(
                Bump {
                    id,
                    pad: "x".repeat(200 + (id as usize % 7) * 90),
                },
                &shared,
                ByteClass::Data,
            )
            .unwrap();
            tasks.push(nabedra_rt::task::spawn(async move {
                nabedra_rt::time::timeout(Duration::from_secs(5), consensus.propose(command)).await
            }));
        }
        let mut failed = false;
        for (id, task) in wave.into_iter().zip(tasks) {
            match task.await.unwrap() {
                Ok(Ok(applied)) => acked.push((id, applied.position.index)),
                _ => {
                    unknown.push(id);
                    failed = true;
                }
            }
        }
        if failed {
            break;
        }
    }
    (acked, unknown)
}

async fn stop(node: Node) {
    let _ = nabedra_rt::time::timeout(Duration::from_secs(10), node.consensus.shutdown()).await;
    drop(node);
}

/// Waits until the reopened node applied everything its log holds.
async fn replay(node: &Node, at_least: u64, ctx: &str) {
    let raft = &node.consensus.raft;
    raft.wait(Some(Duration::from_secs(20)))
        .applied_index_at_least(Some(at_least), "replay acknowledged commands")
        .await
        .unwrap_or_else(|e| panic!("{ctx}: replay to {at_least}: {e:?}"));
    let last_log = raft.metrics().borrow_watched().last_log_index;
    if let Some(last) = last_log {
        raft.wait(Some(Duration::from_secs(20)))
            .applied_index_at_least(Some(last), "replay the whole log")
            .await
            .unwrap_or_else(|e| panic!("{ctx}: replay to the log end {last}: {e:?}"));
    }
}

/// Presence of each id's marker.
async fn markers(store: &GroupStore, ids: &[u64]) -> Vec<bool> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        out.push(read_markers(store, id..id + 1).await == 1);
    }
    out
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// O11, D09, NS10: a crash, a 512 B power loss or a 4 KiB power loss after the n-th
/// mutating operation of a run of concurrent proposals with snapshots and purges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
pub(super) async fn o11_d09_ns10_raft_writes_survive_cuts_at_sampled_operations() {
    let commands = env_u64("NS10_RAFT_COMMANDS", 96);
    let points = env_u64("NS10_RAFT_POINTS", 18);
    // Reference run: how many mutating operations the proposals take.
    let (base_ops, total_ops) = {
        let dir = Dir::new();
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let fault = FaultFs::new();
        fault.create_dir_all_durable(&path);
        let layout = Layout::new();
        let node = open_node(&path, &layout, shared(), &fault).await;
        node.lead(&layout, true).await;
        let base = fault.mutating_ops();
        let (acked, unknown) = propose_waves(&node, 0..commands).await;
        assert!(unknown.is_empty() && acked.len() as u64 == commands);
        let total = fault.mutating_ops();
        let snapshot = node.consensus.raft.metrics().borrow_watched().snapshot;
        let purged = node.consensus.raft.metrics().borrow_watched().purged;
        eprintln!(
            "O11 reference: {commands} commands take {} mutating operations; snapshot {snapshot:?}, purged {purged:?}",
            total - base
        );
        assert!(snapshot.is_some(), "the run must take a snapshot");
        stop(node).await;
        (base, total)
    };
    let span = (total_ops - base_ops).max(1);
    let mut cuts = [0u64; 3];
    let (mut acked_total, mut unknown_total, mut unknown_applied) = (0usize, 0usize, 0usize);
    for p in 0..points {
        let n = span * (2 * p + 1) / (2 * points) + p % 3;
        let dir = Dir::new();
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let fault = FaultFs::new();
        fault.create_dir_all_durable(&path);
        let layout = Layout::new();
        let shared = shared();
        let node = open_node(&path, &layout, shared.clone(), &fault).await;
        node.lead(&layout, true).await;
        fault.crash_after(fault.mutating_ops() + n);
        let (acked, unknown) = propose_waves(&node, 0..commands).await;
        fault.halt();
        stop(node).await;
        let kind = p % 3;
        cuts[kind as usize] += 1;
        let image = match kind {
            0 => fault.crash(),
            1 => fault.power_loss(p),
            _ => fault.power_loss_with(p, PowerLoss::destroy_4k()),
        };
        let ctx = format!(
            "cut {p} after {n} of {span} operations (kind {kind}), {} acknowledged",
            acked.len()
        );
        let node = open_node(&path, &layout, shared.clone(), &image).await;
        assert!(!node.pool.requires_recovery(), "{ctx}: requires recovery");
        assert!(
            node.pool.quarantined_groups().is_empty(),
            "{ctx}: quarantined"
        );
        node.lead(&layout, false).await;
        let last = acked.iter().map(|&(_, index)| index).max().unwrap_or(0);
        replay(&node, last, &ctx).await;
        let acked_ids: Vec<u64> = acked.iter().map(|&(id, _)| id).collect();
        let present = markers(&node.store, &acked_ids).await;
        if let Some(i) = present.iter().position(|&p| !p) {
            panic!("{ctx}: acknowledged command {} lost", acked_ids[i]);
        }
        let all: Vec<u64> = (0..commands).collect();
        let applied = markers(&node.store, &all)
            .await
            .iter()
            .filter(|&&p| p)
            .count() as u64;
        assert_eq!(
            node.count().await,
            applied,
            "{ctx}: count disagrees with the markers"
        );
        for &id in &unknown {
            unknown_applied += usize::from(read_markers(&node.store, id..id + 1).await == 1);
        }
        // Nothing beyond what was proposed: proposals never started are absent.
        let proposed = acked.len() + unknown.len();
        let beyond: Vec<u64> = (proposed as u64..commands).collect();
        assert!(
            markers(&node.store, &beyond).await.iter().all(|&p| !p),
            "{ctx}: a command that was never proposed is applied"
        );
        // A new command still applies exactly once.
        let before = node.count().await;
        let (more, failed) = propose_waves(&node, commands..commands + 1).await;
        assert!(
            failed.is_empty() && more.len() == 1,
            "{ctx}: a new command failed"
        );
        assert_eq!(
            node.count().await,
            before + 1,
            "{ctx}: the new command applied twice"
        );
        acked_total += acked.len();
        unknown_total += unknown.len();
        stop(node).await;
    }
    eprintln!(
        "O11/D09: {points} cuts (crash {}, power loss {}, 4 KiB power loss {}) over {span} operations: {acked_total} acknowledged commands all present once, {unknown_applied} of {unknown_total} outcome-unknown commands applied once",
        cuts[0], cuts[1], cuts[2]
    );
}

/// O29, NS09: EIO on the journal sync while proposals are in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o29_ns10_ambiguous_journal_sync_error_under_raft_writes() {
    let dir = Dir::new();
    let path = dir.0.join("pool");
    std::fs::create_dir_all(&path).unwrap();
    let fault = FaultFs::new();
    fault.create_dir_all_durable(&path);
    let layout = Layout::new();
    let shared = shared();
    let node = open_node(&path, &layout, shared.clone(), &fault).await;
    node.lead(&layout, true).await;
    let (first, unknown) = propose_waves(&node, 0..24).await;
    assert!(unknown.is_empty() && first.len() == 24);
    let alias = node.pool.clone();
    let calls_before = fault.calls().len();
    fault.inject(
        Fault::on(FsOp::SyncData)
            .path_prefix(path.join("kv").join("journal"))
            .error(FaultError::Eio),
    );
    let (second, ambiguous) = propose_waves(&node, 24..48).await;
    assert!(
        !ambiguous.is_empty(),
        "the injected sync failure must fail a proposal"
    );
    assert!(node.pool.requires_recovery() && alias.requires_recovery());
    let journal = path.join("kv").join("journal");
    // A sync that a background commit issued between `calls_before` and the injection
    // succeeded and is not a retry (CI run 44811636932 counted one): what must hold is
    // one failed journal sync and no journal sync after it.
    let syncs: Vec<bool> = fault.calls()[calls_before..]
        .iter()
        .filter(|c| c.op == FsOp::SyncData && c.path.starts_with(&journal))
        .map(|c| c.ok)
        .collect();
    let failed = syncs.iter().position(|ok| !ok);
    assert!(
        failed.is_some() && syncs[..failed.unwrap()].iter().all(|&ok| ok),
        "one journal sync fails first: {syncs:?}"
    );
    assert_eq!(
        syncs.len(),
        failed.unwrap() + 1,
        "the failed journal sync was retried: {syncs:?}"
    );
    // Later proposals fail too: nothing is applied from uncertain state.
    let (later, refused) = propose_waves(&node, 48..49).await;
    assert!(later.is_empty() && refused == [48]);
    fault.halt();
    stop(node).await;
    drop(alias);

    let image = fault.crash();
    let node = open_node(&path, &layout, shared.clone(), &image).await;
    assert!(!node.pool.requires_recovery());
    node.lead(&layout, false).await;
    let acked: Vec<(u64, u64)> = first.iter().chain(&second).copied().collect();
    let last = acked.iter().map(|&(_, index)| index).max().unwrap();
    replay(&node, last, "O29").await;
    let acked_ids: Vec<u64> = acked.iter().map(|&(id, _)| id).collect();
    assert!(
        markers(&node.store, &acked_ids)
            .await
            .into_iter()
            .all(|p| p),
        "an acknowledged command is lost"
    );
    let present = markers(&node.store, &ambiguous).await;
    let applied = present.iter().filter(|&&p| p).count();
    assert_eq!(node.count().await, (acked.len() + applied) as u64);
    // Retry only what recovery shows absent: each applies exactly once.
    let absent: Vec<u64> = ambiguous
        .iter()
        .zip(&present)
        .filter(|&(_, &p)| !p)
        .map(|(&id, _)| id)
        .collect();
    for &id in &absent {
        let (ok, failed) = propose_waves(&node, id..id + 1).await;
        assert!(failed.is_empty() && ok.len() == 1);
    }
    assert_eq!(node.count().await, (acked.len() + ambiguous.len()) as u64);
    assert_eq!(markers(&node.store, &[48]).await, [false]);
    eprintln!(
        "O29: {} acknowledged before and {} during the fault, {} ambiguous ({applied} found applied after recovery, {} retried once)",
        first.len(),
        second.len(),
        ambiguous.len(),
        absent.len()
    );
    stop(node).await;
}
