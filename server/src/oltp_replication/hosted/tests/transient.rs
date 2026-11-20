//! RV-APPLY: transient storage and capacity conditions never stop a Raft core
//! (F1, F2, F3, F5, F6), on a single voter whose pool runs on `FaultFs`. The
//! full device is the `FaultFs` ENOSPC model, not a physical disk.
use super::*;
use crate::oltp_replication::hosted::transient::RaftHealthSnapshot;
use nabedra_kv::fs::{Fault, FaultError, FsOp};
use openraft::type_config::async_runtime::watch::WatchReceiver;

async fn open_node(
    path: &Path,
    layout: &Layout,
    fs: &FaultFs,
    snapshot_every: Option<u64>,
) -> Node {
    let shared = shared();
    let fs: Arc<dyn Fs> = Arc::new(fs.clone());
    let pool = OltpPool::open_development_tuned(
        path,
        layout.pool.clone(),
        shared.clone(),
        fs,
        small_segments,
    )
    .unwrap();
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
    let mut config = openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    };
    if let Some(every) = snapshot_every {
        config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(every);
        config.max_in_snapshot_log_to_keep = 4;
        config.purge_batch_size = 1;
    }
    let consensus = RaftConsensus::start(batched_config(config).unwrap(), NoPeers, log, state)
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

fn fault_dir() -> (Dir, PathBuf, FaultFs) {
    let dir = Dir::new();
    let path = dir.0.join("pool");
    std::fs::create_dir_all(&path).unwrap();
    let fault = FaultFs::new();
    fault.create_dir_all_durable(&path);
    (dir, path, fault)
}

fn command(node: &Node, id: u64, pad: &str) -> OwnedCommand<Bump> {
    OwnedCommand::new_owned(
        Bump {
            id,
            pad: pad.to_owned(),
        },
        node.store.resources(),
        ByteClass::Data,
    )
    .unwrap()
}

fn running(node: &Node) -> bool {
    node.consensus
        .raft
        .metrics()
        .borrow_watched()
        .running_state
        .is_ok()
}

fn health(node: &Node) -> RaftHealthSnapshot {
    node.consensus.health().snapshot()
}

/// 32 KiB journal segments, so a few proposals reach a segment roll.
pub(super) fn small_segments(options: &mut nabedra_kv::Options) {
    options.journal.segment_bytes = 32 << 10;
}

/// The device is full: every journal segment preallocation fails with ENOSPC.
/// kv preallocates each segment, so the next roll is where a full device
/// shows (a voided `NoSpace`, NS11). An ENOSPC from a write inside a
/// preallocated segment would be an ambiguous partial write, which latches
/// the pool (O29); a preallocated segment does not produce one.
pub(super) fn fill_device(fault: &FaultFs, path: &Path) {
    fault.inject(
        Fault::on(FsOp::Preallocate)
            .path_prefix(path.join("kv").join("journal"))
            .error(FaultError::Enospc)
            .repeat(),
    );
}

/// Proposes `next..` one at a time until one does not complete within 3 s (it
/// met the full device and is retrying) or is refused as NoSpace. Returns the
/// acknowledged ids and the pending proposal, if any.
async fn propose_until_full(
    node: &Node,
    next: &mut u64,
) -> (
    Vec<u64>,
    Option<(
        u64,
        tokio::task::JoinHandle<Result<Applied<u64>, ProtocolError>>,
    )>,
) {
    let mut acked = Vec::new();
    for _ in 0..2000 {
        let id = *next;
        *next += 1;
        let consensus = node.consensus.clone();
        let command = command(node, id, &"y".repeat(512));
        let mut task = tokio::spawn(async move { consensus.propose(command).await });
        match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
            Ok(result) => match result.unwrap() {
                Ok(_) => acked.push(id),
                Err(error) if crate::oltp::pool::is_no_space(&error) => return (acked, None),
                Err(error) => panic!("proposal {id}: {error:?}"),
            },
            Err(_) => return (acked, Some((id, task))),
        }
    }
    panic!("2000 proposals never met the full device");
}

/// F1, NS11, O06, O17: a full device on a single voter. The first proposal's
/// append hits ENOSPC and is retried; the core stays up, later proposals are
/// refused with an explicit not-applied NoSpace, and once space frees up the
/// pending proposal commits on the same core, every acknowledged command
/// applied exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns11_o06_o17_rv_apply_enospc_keeps_raft_cores_alive() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, None).await;
    node.lead(&layout, true).await;
    for id in 0..8 {
        node.consensus
            .propose(command(&node, id, "before"))
            .await
            .unwrap();
    }
    fill_device(&fault, &path);
    let mut next = 8;
    let (mut acked, pending) = propose_until_full(&node, &mut next).await;
    acked.extend(0..8);
    // The pending proposal (or a Raft metadata write) keeps meeting a full
    // device and retries.
    tokio::time::timeout(Duration::from_secs(10), async {
        while health(&node).no_space_retries < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "no NoSpace retries: running {}, latched {}, health {:?}",
            running(&node),
            node.pool.requires_recovery(),
            health(&node)
        )
    });
    assert!(running(&node), "ENOSPC stopped the core");
    assert!(!node.pool.requires_recovery(), "NoSpace latched the pool");
    let mut refused = Vec::new();
    for _ in 0..5 {
        let id = next;
        next += 1;
        match tokio::time::timeout(
            Duration::from_secs(5),
            node.consensus.propose(command(&node, id, "refused")),
        )
        .await
        {
            Ok(Err(error)) => {
                assert!(crate::oltp::pool::is_no_space(&error), "{error:?}");
                assert_eq!(error.outcome, Outcome::NotApplied);
                refused.push(id);
            }
            other => panic!("proposal {id} while the device is full: {other:?}"),
        }
    }
    assert!(running(&node), "ENOSPC stopped the core");
    let full = health(&node);
    assert!(full.proposals_refused_no_space >= 5, "{full:?}");

    fault.clear_faults();
    if let Some((id, pending)) = pending {
        tokio::time::timeout(Duration::from_secs(20), pending)
            .await
            .expect("the pending proposal completes once space is back")
            .unwrap()
            .unwrap();
        acked.push(id);
    }
    for _ in 0..6 {
        let id = next;
        next += 1;
        node.consensus
            .propose(command(&node, id, "after"))
            .await
            .unwrap();
        acked.push(id);
    }
    assert!(running(&node));
    assert_eq!(node.consensus.raft.restarts(), 0, "the same core serves");
    for &id in &acked {
        assert_eq!(
            read_markers(&node.store, id..id + 1).await,
            1,
            "acknowledged {id}"
        );
    }
    for &id in &refused {
        assert_eq!(
            read_markers(&node.store, id..id + 1).await,
            0,
            "refused {id}"
        );
    }
    assert_eq!(node.count().await, acked.len() as u64, "each applied once");
    eprintln!(
        "F1 single voter: {} acknowledged, {:?}",
        acked.len(),
        health(&node)
    );
    node.consensus.shutdown().await.unwrap();
}

/// F1: shutdown never hangs on a core blocked in a NoSpace retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns11_rv_apply_shutdown_ends_a_no_space_retry() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, None).await;
    node.lead(&layout, true).await;
    fill_device(&fault, &path);
    let (_, pending) = propose_until_full(&node, &mut 0).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while health(&node).no_space_retries == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), node.consensus.shutdown())
        .await
        .expect("shutdown waits for a NoSpace retry")
        .ok();
    if let Some((_, pending)) = pending {
        let _ = tokio::time::timeout(Duration::from_secs(5), pending).await;
    }
}

/// F2, NS11, O16: a policy snapshot build that fails transiently is deferred,
/// the core stays up, and a later build succeeds and purges the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns11_o16_rv_apply_failed_snapshot_build_is_deferred() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, Some(24)).await;
    node.lead(&layout, true).await;
    // Every export until the hook runs out fails with NoSpace.
    node.consensus
        .retry()
        .fail_exports
        .store(u32::MAX, std::sync::atomic::Ordering::Release);
    for id in 0..60 {
        node.consensus
            .propose(command(&node, id, "x"))
            .await
            .unwrap();
    }
    assert!(running(&node), "a failed snapshot build stopped the core");
    assert!(
        node.consensus
            .raft
            .metrics()
            .borrow_watched()
            .snapshot
            .is_none()
    );
    assert!(health(&node).snapshot_builds_deferred > 0);
    node.consensus
        .retry()
        .fail_exports
        .store(0, std::sync::atomic::Ordering::Release);
    // Past the deferral window, the next policy build succeeds.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    for id in 60..90 {
        node.consensus
            .propose(command(&node, id, "x"))
            .await
            .unwrap();
    }
    node.consensus
        .raft
        .wait(Some(Duration::from_secs(20)))
        .metrics(
            |m| m.snapshot.is_some_and(|s| s.index >= 60) && m.purged.is_some(),
            "a snapshot past index 60 and a purge",
        )
        .await
        .unwrap();
    assert!(running(&node));
    assert_eq!(node.count().await, 90);
    node.consensus.shutdown().await.unwrap();
}

/// F3, O16: snapshot metadata pressure waits instead of stopping the core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o16_rv_apply_snapshot_metadata_pressure_waits() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, None).await;
    node.lead(&layout, true).await;
    for id in 0..10 {
        node.consensus
            .propose(command(&node, id, "x"))
            .await
            .unwrap();
    }
    let resident = node.consensus.state.resident();
    let budget = crate::oltp::pool::RAFT_REPLICA_BYTES / 2;
    let hold = resident.metadata(budget - (128 << 10)).unwrap();
    node.consensus.raft.trigger().snapshot().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        running(&node),
        "snapshot metadata pressure stopped the core"
    );
    assert!(
        node.consensus
            .raft
            .metrics()
            .borrow_watched()
            .snapshot
            .is_none(),
        "the build waits for metadata room"
    );
    drop(hold);
    node.consensus
        .raft
        .wait(Some(Duration::from_secs(20)))
        .metrics(|m| m.snapshot.is_some(), "the waiting snapshot completes")
        .await
        .unwrap();
    assert!(running(&node));
    node.consensus.shutdown().await.unwrap();
}

/// F5: the leader's own Membership and Blank entries wait for log metadata
/// room in `append` instead of stopping the core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_rv_apply_leader_membership_append_waits_for_log_metadata() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, None).await;
    let resident = node.consensus.state.resident();
    let hold = resident.log_metadata((2 << 20) - 64).unwrap();
    let raft = node.consensus.raft.clone();
    let member = layout.member();
    let initialize =
        tokio::spawn(async move { raft.initialize(BTreeMap::from([(1, member)])).await });
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(running(&node), "a log metadata refusal stopped the core");
    assert_eq!(
        node.consensus
            .raft
            .metrics()
            .borrow_watched()
            .current_leader,
        None,
        "the membership entry waits for room"
    );
    drop(hold);
    initialize.await.unwrap().unwrap();
    node.lead(&layout, false).await;
    node.consensus
        .propose(command(&node, 1, "x"))
        .await
        .unwrap();
    assert!(running(&node));
    node.consensus.shutdown().await.unwrap();
}

/// F6: a reply over the decode budget drops the proposer's responder; the
/// entry stays applied and the core keeps applying.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_rv_apply_oversized_reply_drops_the_responder() {
    let (_dir, path, fault) = fault_dir();
    let layout = Layout::new();
    let node = open_node(&path, &layout, &fault, None).await;
    node.lead(&layout, true).await;
    let oversized = tokio::time::timeout(
        Duration::from_secs(10),
        node.consensus.propose(command(&node, 1, BIG_REPLY)),
    )
    .await
    .expect("the proposer is answered");
    assert!(oversized.is_err(), "{oversized:?}");
    assert!(running(&node), "an oversized reply stopped the core");
    assert_eq!(health(&node).replies_dropped, 1);
    assert_eq!(
        read_markers(&node.store, 1..2).await,
        1,
        "the entry applied"
    );
    node.consensus
        .propose(command(&node, 2, "x"))
        .await
        .unwrap();
    assert_eq!(node.count().await, 2);
    node.consensus.shutdown().await.unwrap();
}
