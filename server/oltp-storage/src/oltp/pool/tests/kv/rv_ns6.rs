//! Regressions for the RV-NS6 review of the pool on `nabedra-kv`.
use super::*;

/// O05 / F03 (RV-NS6 F1): creating a brand-new pool on a runtime worker whose
/// task has spent its cooperative budget returns. The `POOL_KEY` write waits in
/// `wait_commit`, which used to park that worker forever: a budgeted poll of
/// the commit deferred its waker until the task yielded, and it never did.
#[test]
fn o05_f03_pool_creation_on_a_worker_without_coop_budget_returns() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = Dir::new();
    let path = dir.0.clone();
    let result = runtime.block_on(async move {
        let task = nabedra_rt::task::spawn(async move {
            while tokio::task::coop::has_budget_remaining() {
                let (tx, rx) = tokio::sync::oneshot::channel();
                tx.send(()).unwrap();
                rx.await.unwrap();
            }
            OltpPool::open_development(
                &path,
                pool_identity(),
                shared(SharedResourceConfig::default()),
            )
            .map(|pool| pool.identity().pool_id)
        });
        // Generous: about 2 s alone, much longer under a loaded full suite. A
        // parked worker never returns at all.
        nabedra_rt::time::timeout(Duration::from_secs(60), task).await
    });
    // A parked worker would keep a normal shutdown waiting forever.
    runtime.shutdown_background();
    let opened = result.expect("pool creation parked its runtime worker in wait_commit");
    assert!(!opened.unwrap().unwrap().is_nil());
}

fn log_put(
    key: Vec<u8>,
    value: Vec<u8>,
) -> impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError> + Send + 'static {
    move |tx| {
        tx.put(&key, &value)?;
        Ok(Transition::Commit(tx.copy_output(&[1])?))
    }
}
fn row(n: usize) -> Vec<u8> {
    format!("row-{n:02}").into_bytes()
}
fn row_value(group: char, n: usize) -> Vec<u8> {
    let mut value = format!("<<{group}-row-{n:02}>>").into_bytes();
    value.resize(1 << 10, b'.');
    value
}
fn find_in(fault: &FaultFs, dir: &Path, needle: &[u8]) -> (PathBuf, u64) {
    let mut hits = Vec::new();
    for path in fault.list(dir).unwrap() {
        let Ok(bytes) = fault.contents(&path) else {
            continue;
        };
        for (at, window) in bytes.windows(needle.len()).enumerate() {
            if window == needle {
                hits.push((path.clone(), at as u64));
            }
        }
    }
    assert_eq!(hits.len(), 1, "{hits:?}");
    hits.remove(0)
}
fn flush(pool: &OltpPool) {
    pool.inner.store.flush().unwrap();
}

/// NS07 G2 / O06, section 3 rule 1 (RV-NS6 F3): damage found at run time stops
/// the group at once. A read meets a damaged run block of group A, which kv
/// quarantines. A's next append and vote are refused with a typed storage
/// error and write nothing; reopening A by identity is refused although its
/// handle is still open; the pool is not latched and group B keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns07_o06_run_time_damage_refuses_the_next_append_and_other_groups_serve() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let tune: fn(&mut nabedra_kv::Options) = |options| {
        // Keep every flushed run where the test put it.
        options.levels.l0_compaction_trigger = 64;
        options.levels.l0_slowdown_writes = 63;
        options.levels.l0_stop_writes = 64;
    };
    let open = |fault: &FaultFs| {
        OltpPool::open_development_tuned(
            &dir.0,
            identity.clone(),
            resources.clone(),
            Arc::new(fault.clone()),
            tune,
        )
        .unwrap()
    };
    let pool = open(&fault);
    // The pool identity gets a run of its own, so A's run holds only A's scope.
    let flushing = pool.clone();
    nabedra_rt::task::spawn_blocking(move || flush(&flushing))
        .await
        .unwrap();
    let a = pool.group(group_identity(&pool)).await.unwrap();
    for n in 0..40 {
        a.log_transaction(budget(), log_put(row(n), row_value('a', n)))
            .await
            .unwrap();
    }
    let flushing = pool.clone();
    nabedra_rt::task::spawn_blocking(move || flush(&flushing))
        .await
        .unwrap();
    let b = pool.group(group_identity(&pool)).await.unwrap();
    b.log_transaction(budget(), log_put(row(0), row_value('b', 0)))
        .await
        .unwrap();
    let (a_id, b_id) = (a.identity().clone(), b.identity().clone());
    drop((a, b, pool));
    let (run, at) = find_in(&fault, &dir.0.join("kv").join("runs"), b"<<a-row-35>>");
    fault.flip_bit(&run, at + 4, 2).unwrap();

    // The footer is intact, so the open finds nothing.
    let pool = open(&fault);
    assert!(pool.quarantined_groups().is_empty());
    let a = pool.retained_group(a_id.clone()).await.unwrap();
    let b = pool.retained_group(b_id.clone()).await.unwrap();
    let damaged = a
        .log_read(budget(), |view| {
            view.get(b"row-35").and_then(|v| v.ok_or_else(capacity))
        })
        .await;
    assert!(damaged.is_err());
    assert_eq!(pool.quarantined_groups(), vec![a_id.group_id]);

    // The running group can no longer append or vote.
    let append = a
        .log_transaction(budget(), log_put(row(99), b"after damage".to_vec()))
        .await
        .expect_err("a quarantined group appended");
    assert_eq!(append.code, ErrorCode::StorageUnavailable);
    assert!(append.message.contains("quarantined"), "{}", append.message);
    let vote = a
        .log_transaction_metadata(budget(), log_put(b"vote".to_vec(), b"term 9".to_vec()))
        .await
        .expect_err("a quarantined group persisted a vote");
    assert!(vote.message.contains("quarantined"), "{}", vote.message);
    let empty = a
        .log_transaction(budget(), |tx| Ok(Transition::Commit(tx.copy_output(&[0])?)))
        .await
        .expect_err("a quarantined group acknowledged an empty synced write");
    assert!(empty.message.contains("quarantined"), "{}", empty.message);
    let reopened = pool.retained_group(a_id).await.err().unwrap();
    assert!(
        reopened.message.contains("quarantined"),
        "{}",
        reopened.message
    );

    // Only the group stopped.
    assert!(!pool.requires_recovery());
    b.log_transaction(budget(), log_put(row(1), row_value('b', 1)))
        .await
        .unwrap();
    let read = b
        .log_read(budget(), |view| {
            view.get(b"row-01").and_then(|v| v.ok_or_else(capacity))
        })
        .await
        .unwrap();
    assert_eq!(read.as_slice(), row_value('b', 1));
}

/// O11 / T10 (RV-NS6 F5): the recorded unsynced application tail keeps the
/// newest write by journal sequence. The tail is stored after `submit`
/// returns, so the next apply may store its newer tail first.
#[tokio::test]
async fn o11_the_unsynced_apply_tail_never_moves_backwards() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    // Sequences far past the journal's: neither is durable.
    group.record_deferred_tail(6, u64::MAX - 1);
    group.record_deferred_tail(5, u64::MAX - 2);
    assert_eq!(group.write_stats().unsynced_applied_index, Some(6));
    group.record_deferred_tail(7, u64::MAX);
    assert_eq!(group.write_stats().unsynced_applied_index, Some(7));
}

/// O11, RV-T2 Q5 (RV-NS6 F6): only the host that turned deferred application
/// on turns it off. An old host dropped after a new one started for the same
/// group leaves the new host's deferral alone.
#[tokio::test]
async fn o11_an_old_host_cannot_clear_a_new_hosts_deferral() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let old = group.defer_apply_sync(&group).unwrap();
    let new = group.defer_apply_sync(&group).unwrap();
    drop(old);
    assert!(group.applies_deferred());
    group
        .transaction(meta(0), budget(), put_entry(0, b"row", vec![0]))
        .await
        .unwrap();
    assert_eq!(group.write_stats().deferred_writes, 1);
    drop(new);
    assert!(!group.applies_deferred());
}

/// O11, O16 (RV-NS6 F6): an application tail written while a host deferred its
/// sync stays unsynced after that host stops. Snapshot export still makes it
/// durable before naming its index, and log purge still checks it.
#[tokio::test]
async fn o11_o16_export_syncs_an_unsynced_apply_tail_after_its_host_stopped() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let host = group.defer_apply_sync(&group).unwrap();
    group
        .transaction(meta(0), budget(), put_entry(0, b"row", vec![0]))
        .await
        .unwrap();
    assert_eq!(group.write_stats().unsynced_applied_index, Some(0));
    drop(host);
    assert!(!group.applies_deferred());
    let refused = group.ensure_applied_durable(1).await.err();
    assert!(
        refused.is_some(),
        "a purge passed the unsynced applied state"
    );
    group.sync_deferred_applies().await.unwrap();
    assert_eq!(group.write_stats().unsynced_applied_index, None);
    assert!(!pool.has_unsynced_writes());
}

/// O06 / O11 (RV-NS6 F10): the pool's `synced_writes` counts real journal
/// fsyncs; a group's synced *requests* carry their own name, so two adjacent
/// stats structs never use one name for two meanings.
#[tokio::test]
async fn o06_o11_group_stats_name_synced_requests_not_fsyncs() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    group
        .log_transaction(budget(), put_entry(0, b"log", vec![1]))
        .await
        .unwrap();
    let stats = serde_json::to_value(group.write_stats()).unwrap();
    let mut keys: Vec<&str> = stats
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "deferred_writes",
            "synced_requests",
            "unsynced_applied_index"
        ]
    );
    assert_eq!(stats["synced_requests"], 1);
    let pool_stats = serde_json::to_value(pool.write_stats()).unwrap();
    assert!(pool_stats["synced_writes"].as_u64().unwrap() >= 1);
}

/// O05 (RV-NS6 F11): when the worker's stats call ends up holding a pool's
/// last handle, the store closes after the registry lock is released. The
/// close waits here for a journal fsync held by the test, while another stats
/// call (and so `register`, which takes the same lock) still completes.
#[test]
#[allow(clippy::disallowed_methods)] // short-lived helper threads that race two stats calls; not services
fn o05_stats_never_close_a_store_under_the_registry_lock() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let gated = Gated {
        inner: fault,
        closed: Arc::default(),
    };
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development_on(
        &dir.0,
        pool_identity(),
        resources.clone(),
        Arc::new(gated.clone()),
    )
    .unwrap();
    // A synced write held in the journal's fsync keeps the store's close waiting.
    gated.set(true);
    let mut batch = WriteBatch::new();
    batch.put(b"\0held", b"until the gate opens");
    let commit = pool
        .inner
        .store
        .submit(batch, Durability::Sync, Box::new(()))
        .unwrap();
    // The probe drops our handle while the first stats call holds its own.
    let (probed, probe_ran) = std::sync::mpsc::channel();
    let mut handle = Some(pool);
    *resources.storage.stats_probe.lock().unwrap() = Some(Box::new(move || {
        drop(handle.take());
        probed.send(()).unwrap();
    }));
    let last_owner = resources.clone();
    let closing = std::thread::spawn(move || last_owner.storage_stats());
    probe_ran.recv_timeout(Duration::from_secs(10)).unwrap();
    let other = resources.clone();
    let (sent, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || sent.send(other.storage_stats()).unwrap());
    let other = received.recv_timeout(Duration::from_secs(3));
    gated.set(false);
    let closing = closing.join().unwrap();
    assert!(commit.wait().is_ok());
    let other = other.expect("a stats call waited behind a store close under the registry lock");
    assert_eq!(closing.pools, 1);
    assert_eq!(other.pools, 0);
}
