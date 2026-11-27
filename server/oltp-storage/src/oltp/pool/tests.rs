use super::*;
use nabedra_oltp_types::types::{GroupKind, Text};
use nabedra_resources::config::WorkerResources;
use std::{
    path::PathBuf,
    time::Duration,
};

#[cfg(test)]
use std::sync::{Condvar, Mutex};

pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        Self(std::env::temp_dir().join(format!("oltp-pool-{}", nabedra_rt::ids::uuid_v4())))
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
pub fn shared(config: SharedResourceConfig) -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        17,
        WorkerResources {
            max_threads: 16,
            ..Default::default()
        },
        config,
    )
    .unwrap()
}
pub fn pool_identity() -> PoolIdentity {
    PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
        cluster_id: nabedra_rt::ids::uuid_v4(),
        worker_id: nabedra_rt::ids::uuid_v4(),
        pool_id: nabedra_rt::ids::uuid_v4(),
    }
}
pub fn group_identity(pool: &OltpPool) -> GroupIdentity {
    GroupIdentity {
        cluster_id: pool.identity().cluster_id,
        database: Text::try_from("default").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Catalog,
        group_format_version: 1,
    }
}
pub fn budget() -> TransactionBudget {
    TransactionBudget {
        scratch_bytes: 64 << 10,
        batch_bytes: 64 << 10,
        output_bytes: 64 << 10,
    }
}
pub fn meta(index: u64) -> ApplyMeta {
    ApplyMeta {
        position: AppliedPosition {
            term: 0,
            node_id: 0,
            index,
        },
        command_digest: Digest::from_bytes([index as u8; 32]),
        membership: None,
    }
}
pub async fn get(group: &GroupStore, key: &'static [u8]) -> Vec<u8> {
    group
        .read(budget(), move |view| {
            view.get(key).and_then(|value| value.ok_or_else(capacity))
        })
        .await
        .unwrap()
        .as_slice()
        .to_vec()
}

#[tokio::test]
async fn o05_o11_shared_pool_groups_atomically_isolate_state_logs_membership_and_replays() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let a = pool.group(group_identity(&pool)).await.unwrap();
    let b = pool.group(group_identity(&pool)).await.unwrap();
    let baseline = resources.scheduler().usage();
    let mut first = meta(0);
    first.membership = Some(resources.copy_bytes(b"membership-a").unwrap());
    let result = a
        .transaction(first.clone(), budget(), |tx| {
            tx.put(b"key", b"a")?;
            assert!(tx.get(b"key")?.is_none());
            Ok(Transition::Commit(tx.copy_output(b"ok-a")?))
        })
        .await
        .unwrap();
    assert_eq!(result.as_slice(), b"ok-a");
    drop(result);
    let replay = a
        .transaction(first, budget(), |_| {
            panic!("exact replay must not rerun transition")
        })
        .await
        .unwrap();
    assert_eq!(replay.as_slice(), b"ok-a");
    drop(replay);
    b.transaction(meta(0), budget(), |tx| {
        tx.put(b"key", b"b")?;
        Ok(Transition::Commit(tx.copy_output(b"ok-b")?))
    })
    .await
    .unwrap();
    a.log_transaction(budget(), |tx| {
        tx.put(b"key", b"raft-log")?;
        Ok(Transition::Commit(tx.copy_output(b"logged")?))
    })
    .await
    .unwrap();
    assert_eq!(get(&a, b"key").await, b"a");
    assert_eq!(get(&b, b"key").await, b"b");
    assert_eq!(
        a.log_read(budget(), |view| view
            .get(b"key")
            .and_then(|value| value.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"raft-log"
    );
    a.transaction(meta(1), budget(), |tx| {
        tx.put(b"key", b"rejected-candidate")?;
        Ok(Transition::Reject(tx.copy_output(b"conflict")?))
    })
    .await
    .unwrap();
    assert_eq!(get(&a, b"key").await, b"a");
    let applied = a.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(1).position);
    assert_eq!(applied.membership.unwrap().as_slice(), b"membership-a");
    assert_eq!(
        b.applied_state(budget()).await.unwrap().unwrap().position,
        meta(0).position
    );
    assert_eq!(resources.control_bytes_used(), 0);
    assert_eq!(resources.scheduler().usage(), baseline);
}

#[tokio::test]
async fn o05_m02_historical_group_inventory_does_not_grow_resident_handles_or_reset_pool_budget() {
    let dir = Dir::new();
    let second = Dir::new();
    let resources = shared(SharedResourceConfig {
        max_group_handles: 2,
        max_resident_groups: 1,
        max_open_pools: 2,
        ..Default::default()
    });
    let first = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let baseline = resources.scheduler().usage()[0].2;
    let mut other_id = pool_identity();
    other_id.worker_id = first.identity().worker_id;
    let other = OltpPool::open_development(&second.0, other_id, resources.clone()).unwrap();
    assert_eq!(resources.scheduler().usage()[0].2, baseline + (64 << 20));
    for _ in 0..140 {
        let group = first.group(group_identity(&first)).await.unwrap();
        assert_eq!(first.resident_storage_handles().await, 1);
        drop(group);
    }
    assert_eq!(first.resident_storage_handles().await, 0);
    let mut cursor = None;
    let mut identities = std::collections::BTreeSet::new();
    loop {
        let encoded = first.list_groups(cursor, 17, budget()).await.unwrap();
        let page: serde_json::Value = serde_json::from_slice(encoded.as_slice()).unwrap();
        for item in page["items"].as_array().unwrap() {
            assert!(identities.insert(item["group_id"].as_str().unwrap().to_owned()));
        }
        if page["exhausted"].as_bool().unwrap() {
            break;
        }
        cursor = Some(Uuid::parse_str(page["after"].as_str().unwrap()).unwrap());
    }
    assert_eq!(identities.len(), 140);
    let a = first.group(group_identity(&first)).await.unwrap();
    let b = other.group(group_identity(&other)).await.unwrap();
    assert!(first.group(group_identity(&first)).await.is_err());
    let residency = resources.reserve_group_residency().unwrap();
    assert!(resources.reserve_group_residency().is_err());
    drop(a);
    assert!(resources.reserve_group_residency().is_err());
    drop(residency);
    assert!(resources.reserve_group_residency().is_ok());
    drop(b);
    drop(other);
    assert_eq!(resources.scheduler().usage()[0].2, baseline);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o10_o29_pool_recovery_state_is_shared_and_private_failures_do_not_commit_applied_metadata()
{
    let dir = Dir::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
    let groupid = group_identity(&pool);
    let a = pool.group(groupid.clone()).await.unwrap();
    let b = pool.group(groupid.clone()).await.unwrap();
    let mut tiny = budget();
    tiny.batch_bytes = 24;
    assert!(
        a.transaction(meta(0), tiny, |tx| {
            tx.put(b"key", b"value")?;
            Ok(Transition::Commit(tx.copy_output(b"unreachable")?))
        })
        .await
        .is_err()
    );
    assert!(a.applied_state(budget()).await.unwrap().is_none());
    assert!(
        a.transaction(meta(0), budget(), |tx| {
            tx.put(b"key", b"private")?;
            panic!("model failure before native write")
        })
        .await
        .is_err()
    );
    assert!(!pool.requires_recovery());
    assert!(a.applied_state(budget()).await.unwrap().is_none());
    a.transaction(meta(0), budget(), |tx| {
        tx.put(b"key", b"committed")?;
        Ok(Transition::Commit(tx.copy_output(b"ok")?))
    })
    .await
    .unwrap();
    // Model the shared recovery latch after durable WAL/live-view uncertainty.
    // This is not injection of a real native WAL or fsync failure.
    pool.inner.recovery.store(true, Ordering::Release);
    assert!(b.applied_state(budget()).await.is_err());
    assert!(
        b.transaction(meta(1), budget(), |_| panic!(
            "uncertain pool must not reevaluate"
        ))
        .await
        .is_err()
    );
    assert!(OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).is_err());
    drop(a);
    drop(b);
    drop(pool);
    let reopened = OltpPool::open_development(&dir.0, identity, resources.clone()).unwrap();
    let group = reopened.group(groupid).await.unwrap();
    assert!(!reopened.requires_recovery());
    assert_eq!(get(&group, b"key").await, b"committed");
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        0
    );
    assert_eq!(resources.control_bytes_used(), 0);
}

#[test]
fn c07_m02_small_encoded_control_values_charge_actual_capacity_and_clones_retain_it() {
    let resources = shared(SharedResourceConfig {
        control_bytes: 1024,
        progress_bytes: 256,
        ..Default::default()
    });
    let tiny = resources.encode(&true, 1 << 20).unwrap();
    assert_eq!(tiny.as_slice(), b"true");
    assert_eq!(resources.control_bytes_used(), 4);
    let clone = tiny.clone();
    drop(tiny);
    assert_eq!(resources.control_bytes_used(), 4);
    assert!(resources.copy_bytes(&[0; 765]).is_err());
    drop(clone);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[cfg(test)]
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}
#[cfg(test)]
impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let (open, _) = self
            .changed
            .wait_timeout_while(self.open.lock().unwrap(), Duration::from_secs(30), |open| {
                !*open
            })
            .unwrap();
        let released = *open;
        drop(open);
        assert!(released, "native test gate was not released");
    }
}
#[cfg(test)]
struct ReleaseOnDrop(Arc<Gate>);
#[cfg(test)]
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// O26/NS04: a closed pool reopens at once although a child process forked
/// while it was open still holds a copy of every descriptor. Test fixtures fork
/// `openssl` from other test threads, and a child that has not reached `exec`
/// yet shares the store lock's open file description. Before the store unlocked
/// that description explicitly, the reopen failed with "operation would block"
/// until the child exec'd (CIfix2, Linux CI).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o26_ns04_pool_reopens_while_a_forked_child_holds_its_descriptors() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let identity = pool_identity();
    let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
    // SAFETY: the child only waits for a signal (async-signal-safe) and is
    // killed below; it never returns into the test harness.
    let child = unsafe { libc::fork() };
    if child == 0 {
        loop {
            unsafe { libc::pause() };
        }
    }
    assert!(child > 0, "fork failed");
    drop(pool);
    let reopened = OltpPool::open_development(&dir.0, identity, resources.clone());
    // SAFETY: `child` is this test's own child process.
    unsafe {
        libc::kill(child, libc::SIGKILL);
        libc::waitpid(child, std::ptr::null_mut(), 0);
    }
    drop(reopened.expect("a forked child kept the closed pool's store locked"));
    resources.drain().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o26_f03_pool_close_waits_for_cancelled_snapshot_file_owner_without_worker_drain() {
    nabedra_rt::time::timeout(Duration::from_secs(30), async {
        let dir = Dir::new();
        let resources = shared(SharedResourceConfig {
            query_threads: 2,
            ..Default::default()
        });
        let identity = pool_identity();
        let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
        let group_id = group_identity(&pool);
        let group = pool.group(group_id.clone()).await.unwrap();
        drop(
            group
                .transaction(meta(0), budget(), |tx| {
                    tx.put(b"kept", b"committed")?;
                    Ok(Transition::Commit(tx.encode(&())?))
                })
                .await
                .unwrap(),
        );
        let snapshot = group.export_snapshot(budget()).await.unwrap();
        let file = snapshot.file.clone();
        drop(snapshot);
        drop(group);
        let gate = Arc::new(Gate {
            open: Mutex::new(false),
            changed: Condvar::new(),
        });
        let _release = ReleaseOnDrop(gate.clone());
        let (entered, arrival) = tokio::sync::oneshot::channel();
        let (settled, completed) = tokio::sync::oneshot::channel();
        let work = nabedra_rt::task::spawn({
            let shared = resources.clone();
            let gate = gate.clone();
            async move {
                shared
                    .execute(ExecutionClass::Query, 0, 0, move || {
                        let _ = entered.send(());
                        gate.wait();
                        let _ = file.metadata().map_err(|_| storage_error_message())?;
                        drop(file);
                        let _ = settled.send(());
                        Ok(())
                    })
                    .await
            }
        });
        arrival.await.unwrap();
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        assert_eq!(
            resources.scheduler().usage()[0].1,
            1,
            "cancelled native file work remains charged"
        );
        let prepared = resources.prepare(ExecutionClass::Query).await.unwrap();
        let compute = prepared.reserve_compute(0, 0).await.unwrap();
        let pool = match pool
            .close_with_native(nabedra_rt::ids::uuid_v4(), prepared, compute)
            .await
            .unwrap()
        {
            PoolCloseOutcome::StillOwned(pool) => pool,
            _ => panic!("a live snapshot file produced a pool-close proof"),
        };
        assert!(
            OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).is_err(),
            "store lock disappeared while transfer retained its file"
        );
        gate.release();
        completed.await.unwrap();
        let prepared = resources.prepare(ExecutionClass::Query).await.unwrap();
        let compute = prepared.reserve_compute(0, 0).await.unwrap();
        let proof = match pool
            .close_with_native(nabedra_rt::ids::uuid_v4(), prepared, compute)
            .await
            .unwrap()
        {
            PoolCloseOutcome::Closed(proof) => proof,
            _ => panic!("settled unique pool did not close"),
        };
        assert_eq!(proof.statement().identity, identity);
        assert!(
            proof.registered_owner().is_none(),
            "a development pool cannot attest provider detach"
        );
        let reopened = OltpPool::open_development(&dir.0, identity, resources.clone()).unwrap();
        let reopened_group = reopened.group(group_id).await.unwrap();
        assert_eq!(get(&reopened_group, b"kept").await, b"committed");
        drop(reopened_group);
        drop(reopened);
        resources.drain().await;
        assert_eq!(resources.scheduler().usage()[0].1, 0);
    })
    .await
    .expect("pool close/transfer watchdog");
}

#[test]
fn c01_c07_f03_owned_executor_exceeds_callers_blocking_capacity_and_retains_cancelled_native_charges()
 {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let resources = shared(SharedResourceConfig {
        query_threads: 7,
        control_threads: 1,
        ..Default::default()
    });
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseOnDrop(gate.clone());
    let (entered, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for id in 1..=7 {
        let resources = resources.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        tasks.push(nabedra_rt::task::spawn_on(runtime.handle(), async move {
            resources
                .execute(ExecutionClass::Query, id, 1024, move || {
                    entered.send(std::thread::current().id()).unwrap();
                    gate.wait();
                    Ok(())
                })
                .await
        }));
    }
    let mut threads = std::collections::HashSet::new();
    runtime.block_on(async {
        for _ in 0..7 {
            threads.insert(
                nabedra_rt::time::timeout(Duration::from_secs(10), arrivals.recv())
                    .await
                    .expect("all seven Query jobs reached the native gate")
                    .expect("native arrival channel remained open"),
            );
        }
    });
    assert_eq!(threads.len(), 7);
    assert_eq!(resources.scheduler().usage()[0].1, 7);
    let cancelled = tasks.remove(0);
    cancelled.abort();
    runtime.block_on(async {
        assert!(
            nabedra_rt::time::timeout(Duration::from_secs(10), cancelled)
                .await
                .expect("the Query caller observed cancellation")
                .unwrap_err()
                .is_cancelled()
        );
    });
    assert_eq!(resources.scheduler().usage()[0].1, 7);
    let control = runtime
        .block_on(async {
            nabedra_rt::time::timeout(
                Duration::from_secs(10),
                resources.execute(ExecutionClass::Control, 99, 0, || {
                    Ok(std::thread::current().id())
                }),
            )
            .await
            .expect("Control progresses while every Query slot is held")
        })
        .unwrap();
    assert!(!threads.contains(&control));
    runtime.block_on(async {
        let mut drain = std::pin::pin!(resources.drain());
        assert!(futures_util::poll!(&mut drain).is_pending());
        drop(release);
        for task in tasks {
            nabedra_rt::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("released Query work settled")
                .unwrap()
                .unwrap();
        }
        nabedra_rt::time::timeout(Duration::from_secs(10), drain)
            .await
            .expect("native drain completed after releasing Query work");
    });
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    assert!(
        runtime
            .block_on(async {
                nabedra_rt::time::timeout(
                    Duration::from_secs(10),
                    resources.prepare(ExecutionClass::Query),
                )
                .await
                .expect("drained worker refused preparation")
            })
            .is_err()
    );
}

#[test]
fn c07_m02_data_saturation_keeps_progress_buffers_and_decode_scratch_available() {
    let resources = shared(SharedResourceConfig {
        control_bytes: 4096,
        progress_bytes: 1024,
        ..Default::default()
    });
    let mut data = resources.allocate_bytes(ByteClass::Data, 3072).unwrap();
    data.as_mut_slice()[0] = 7;
    assert!(resources.copy_bytes(b"more").is_err());
    let header = resources
        .encode_in(ByteClass::Progress, &true, 4096)
        .unwrap();
    let scratch = resources.reserve_scratch(ByteClass::Progress, 512).unwrap();
    assert_eq!(scratch.bytes(), 512);
    assert_eq!(resources.buffer_bytes_used(ByteClass::Data), 3072);
    let bytes = data.freeze();
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(clone.as_slice()[0], 7);
    assert_eq!(resources.buffer_bytes_used(ByteClass::Progress), 516);
    drop(clone);
    drop(header);
    drop(scratch);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o11_o16_group_snapshot_activates_atomically_preserves_other_groups_and_reclaims_unpinned_paths()
 {
    let source_dir = Dir::new();
    let target_dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let source =
        OltpPool::open_development(&source_dir.0, pool_identity(), resources.clone()).unwrap();
    let mut target_id = pool_identity();
    target_id.cluster_id = source.identity().cluster_id;
    target_id.worker_id = source.identity().worker_id;
    let target = OltpPool::open_development(&target_dir.0, target_id, resources.clone()).unwrap();
    let identity = group_identity(&source);
    let a = source.group(identity.clone()).await.unwrap();
    let b = target.group(identity).await.unwrap();
    let other = target.group(group_identity(&target)).await.unwrap();
    for group in [&a, &b, &other] {
        group
            .transaction(meta(0), budget(), |tx| {
                tx.put(b"key", b"old")?;
                Ok(Transition::Commit(tx.copy_output(b"initial")?))
            })
            .await
            .unwrap();
    }
    b.log_transaction(budget(), |tx| {
        tx.put(b"raft", b"keep-log")?;
        Ok(Transition::Commit(tx.copy_output(b"logged")?))
    })
    .await
    .unwrap();
    a.transaction(meta(1), budget(), |tx| {
        tx.put(b"key", b"first")?;
        tx.put(b"new", b"added")?;
        Ok(Transition::Commit(tx.copy_output(b"first-result")?))
    })
    .await
    .unwrap();
    let first = a.export_snapshot(budget()).await.unwrap();
    a.transaction(meta(2), budget(), |tx| {
        tx.put(b"key", b"second")?;
        Ok(Transition::Commit(tx.copy_output(b"second-result")?))
    })
    .await
    .unwrap();
    let second = a.export_snapshot(budget()).await.unwrap();
    // SNAP1: each export is a transient outgoing file owned by its handles.
    assert!(first.path.exists());
    let first_path = first.path.clone();
    assert!(first.file.metadata().unwrap().len() > 0);
    let old = target.snapshot();
    let old_prefix = b.prefix(&old, false).unwrap();
    b.install_snapshot(first.clone(), budget()).await.unwrap();
    assert_eq!(get(&b, b"key").await, b"first");
    b.install_snapshot(second, budget()).await.unwrap();
    assert_eq!(get(&b, b"key").await, b"second");
    assert!(b.install_snapshot(first, budget()).await.is_err());
    assert!(
        !first_path.exists(),
        "the last handle left its outgoing file"
    );
    let current = b.current_snapshot(budget()).await.unwrap().unwrap();
    assert_eq!(current.header.applied.unwrap().index, 2);
    assert_eq!(get(&b, b"new").await, b"added");
    let old_key = [old_prefix.as_slice(), b"key"].concat();
    assert_eq!(old.get(&old_key).unwrap().unwrap().as_ref(), b"old");
    assert_eq!(get(&other, b"key").await, b"old");
    assert_eq!(
        other
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        0
    );
    assert_eq!(
        b.log_read(budget(), |view| view
            .get(b"raft")
            .and_then(|v| v.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"keep-log"
    );
    assert_eq!(
        b.applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        2
    );
    let snapshots = target_dir
        .0
        .join("snapshots")
        .join(b.identity().group_id.to_string());
    assert_eq!(fs::read_dir(snapshots).unwrap().count(), 1);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[cfg_attr(test, tokio::test)]
#[cfg_attr(not(test), tokio::main(flavor = "current_thread"))]
async fn o11_o16_inconsistent_snapshot_header_body_is_rejected_without_changing_active_state() {
    use sha2::{Digest as _, Sha256};
    let source_dir = Dir::new();
    let target_dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let source =
        OltpPool::open_development(&source_dir.0, pool_identity(), resources.clone()).unwrap();
    let mut target_id = pool_identity();
    target_id.cluster_id = source.identity().cluster_id;
    target_id.worker_id = source.identity().worker_id;
    let target = OltpPool::open_development(&target_dir.0, target_id, resources.clone()).unwrap();
    let identity = group_identity(&source);
    let a = source.group(identity.clone()).await.unwrap();
    let b = target.group(identity).await.unwrap();
    a.transaction(meta(0), budget(), |tx| {
        tx.put(b"key", b"incoming")?;
        Ok(Transition::Commit(tx.copy_output(b"ok")?))
    })
    .await
    .unwrap();
    let original = a.export_snapshot(budget()).await.unwrap();
    let bytes = fs::read(&original.path).unwrap();
    let old_len = u32::from_be_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let mut header = original.header.clone();
    header.applied.as_mut().unwrap().index = 9;
    let encoded = serde_json::to_vec(&header).unwrap();
    let mut corrupted = bytes[..16].to_vec();
    corrupted.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    corrupted.extend_from_slice(&encoded);
    corrupted.extend_from_slice(&bytes[20 + old_len..bytes.len() - 33]);
    let checksum = Sha256::digest(&corrupted);
    corrupted.push(255);
    corrupted.extend_from_slice(&checksum);
    let path = target_dir.0.join("inconsistent-test.bin");
    fs::write(&path, corrupted).unwrap();
    let file = File::open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    let incoming = GroupSnapshot {
        file: PoolSnapshotFile::new(file, b.pool().clone()),
        path,
        bytes: length,
        header,
    };
    assert!(b.install_snapshot(incoming, budget()).await.is_err());
    let path = target_dir.0.join("truncated-test.bin");
    fs::write(&path, &bytes[..bytes.len() - 17]).unwrap();
    let file = File::open(&path).unwrap();
    let truncated = GroupSnapshot {
        file: PoolSnapshotFile::new(file, b.pool().clone()),
        path,
        bytes: (bytes.len() - 17) as u64,
        header: original.header.clone(),
    };
    assert!(b.install_snapshot(truncated, budget()).await.is_err());

    assert_eq!(
        u64::from_be_bytes(
            target
                .get_latest(&group_key(b.identity().group_id, b'a', &[]))
                .unwrap()
                .unwrap()
                .as_ref()
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(b.applied_state(budget()).await.unwrap().is_none());
    assert!(
        b.read(budget(), |view| view
            .get(b"key")
            .and_then(|v| v.ok_or_else(capacity)))
            .await
            .is_err()
    );
    assert!(
        target
            .get_latest(&group_key(b.identity().group_id, b't', &[]))
            .unwrap()
            .is_none()
    );
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o11_o16_snapshot_boundary_interruption_models_recover_old_or_new_generation_and_clean_stages()
 {
    let source_dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let source =
        OltpPool::open_development(&source_dir.0, pool_identity(), resources.clone()).unwrap();
    let identity = group_identity(&source);
    let a = source.group(identity.clone()).await.unwrap();
    a.transaction(meta(0), budget(), |tx| {
        tx.put(b"key", b"new")?;
        Ok(Transition::Commit(tx.copy_output(b"new-result")?))
    })
    .await
    .unwrap();
    let snapshot = a.export_snapshot(budget()).await.unwrap();
    for after_activation in [false, true] {
        let target_dir = Dir::new();
        let mut target_id = pool_identity();
        target_id.cluster_id = source.identity().cluster_id;
        target_id.worker_id = source.identity().worker_id;
        let target =
            OltpPool::open_development(&target_dir.0, target_id.clone(), resources.clone())
                .unwrap();
        let b = target.group(identity.clone()).await.unwrap();
        let other_id = group_identity(&target);
        let other = target.group(other_id.clone()).await.unwrap();
        other
            .transaction(meta(0), budget(), |tx| {
                tx.put(b"key", b"unrelated")?;
                Ok(Transition::Commit(tx.copy_output(b"other")?))
            })
            .await
            .unwrap();
        // A panic at a durable boundary models interrupted process execution.
        // This assertion does not inject native power loss or a torn WAL.
        let result = b
            .install_with_hook(snapshot.clone(), budget(), move |point| {
                if matches!(point, snapshot::InstallPoint::AfterActivation) == after_activation {
                    panic!("snapshot durable-boundary interruption model");
                }
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert!(target.requires_recovery());
        drop(b);
        drop(other);
        drop(target);
        let recovered =
            OltpPool::open_development(&target_dir.0, target_id, resources.clone()).unwrap();
        let b = recovered.group(identity.clone()).await.unwrap();
        let other = recovered.group(other_id).await.unwrap();
        assert_eq!(get(&other, b"key").await, b"unrelated");
        if after_activation {
            assert_eq!(get(&b, b"key").await, b"new");
            assert_eq!(
                b.applied_state(budget())
                    .await
                    .unwrap()
                    .unwrap()
                    .position
                    .index,
                0
            );
        } else {
            assert!(b.applied_state(budget()).await.unwrap().is_none());
        }
        assert!(
            recovered
                .get_latest(&group_key(b.identity().group_id, b't', &[]))
                .unwrap()
                .is_none()
        );
        let directory = target_dir
            .0
            .join("snapshots")
            .join(b.identity().group_id.to_string());
        assert_eq!(
            fs::read_dir(directory).unwrap().count(),
            usize::from(after_activation)
        );
    }
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn c07_m02_native_authority_read_progresses_while_query_owns_all_unreserved_memory() {
    let limits = WorkerResources {
        max_threads: 4,
        memory_limit_bytes: 512 << 20,
        control_memory_reserve_bytes: 64 << 20,
        task_memory_limit_bytes: 128 << 20,
    };
    let resources = WorkerSharedResources::new(
        17,
        limits,
        SharedResourceConfig {
            max_open_pools: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let mut identity = group_identity(&pool);
    identity.kind = GroupKind::Control;
    let group = pool.group(identity).await.unwrap();
    group
        .transaction(meta(0), budget(), |tx| {
            tx.put(b"authority", b"current")?;
            Ok(Transition::Commit(tx.copy_output(b"ok")?))
        })
        .await
        .unwrap();
    let scheduler = resources.scheduler();
    let node = resources.scheduler_node_id();
    let baseline = scheduler.usage();
    let task_bytes = 64 << 20;
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseOnDrop(gate.clone());
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let query = nabedra_rt::task::spawn({
        let resources = resources.clone();
        async move {
            resources
                .execute(ExecutionClass::Query, 1, task_bytes, move || {
                    let _ = entered.send(());
                    gate.wait();
                    Ok(())
                })
                .await
        }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    let query_bytes = baseline[0].2 + task_bytes;
    assert_eq!(scheduler.usage(), [(node, 1, query_bytes)]);
    // Fill the remaining working memory after the native query has its grant.
    let retained = scheduler
        .charge_memory(
            node,
            limits
                .working_bytes()
                .checked_sub(query_bytes)
                .expect("working memory must fit the pool and query"),
        )
        .unwrap();
    let saturated = [(node, 1, limits.working_bytes())];
    assert_eq!(scheduler.usage(), saturated);
    let authority = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        group.read(budget(), |view| {
            view.get(b"authority").and_then(|v| v.ok_or_else(capacity))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authority.as_slice(), b"current");
    assert_eq!(scheduler.usage(), saturated);
    drop(authority);
    drop(release);
    nabedra_rt::time::timeout(Duration::from_secs(10), query)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(retained);
    assert_eq!(scheduler.usage(), baseline);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[test]
fn c02_o05_worker_identity_cannot_reset_budgets_or_adopt_a_second_resource_owner() {
    let resources = shared(SharedResourceConfig::default());
    let worker = nabedra_rt::ids::uuid_v4();
    resources.bind_worker(worker).unwrap();
    assert!(resources.bind_worker(nabedra_rt::ids::uuid_v4()).is_err());
    let alias = resources.clone();
    alias.bind_worker(worker).unwrap();
    let second = shared(SharedResourceConfig::default());
    assert!(second.bind_worker(worker).is_err());
    drop(alias);
    drop(resources);
    second.bind_worker(worker).unwrap();
}

#[test]
fn q02_m02_one_physical_queue_owner_preserves_worker_identity_until_last_owner_releases() {
    let resources = shared(SharedResourceConfig::default());
    let worker = nabedra_rt::ids::uuid_v4();
    assert!(resources.claim_queue().is_err());
    resources.bind_worker(worker).unwrap();
    let queue = resources.claim_queue().unwrap();
    assert!(resources.claim_queue().is_err());
    let replacement = shared(SharedResourceConfig::default());
    drop(resources);
    assert!(replacement.bind_worker(worker).is_err());
    drop(queue);
    replacement.bind_worker(worker).unwrap();
    assert!(replacement.claim_queue().is_ok());
}

#[tokio::test]
async fn o05_c07_committed_storage_apply_uses_private_capacity_with_full_transport_arenas() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let data = pool.group(group_identity(&pool)).await.unwrap();
    let mut control_id = group_identity(&pool);
    control_id.kind = GroupKind::Control;
    let control = pool.group(control_id).await.unwrap();
    control
        .transaction(meta(0), budget(), |tx| {
            tx.put(b"authority", b"current")?;
            Ok(Transition::Commit(tx.copy_output(b"ok")?))
        })
        .await
        .unwrap();
    let config = resources.config();
    let wire = resources
        .allocate_bytes(
            ByteClass::Data,
            config.control_bytes - config.progress_bytes,
        )
        .unwrap();
    let progress_wire = resources
        .allocate_bytes(ByteClass::Progress, config.progress_bytes)
        .unwrap();
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseOnDrop(gate.clone());
    let (entered, arrived) = tokio::sync::oneshot::channel();
    // ApplyMeta models an entry already ordered by consensus. This test covers
    // the native storage transition, not a multi-node quorum history.
    let applying = nabedra_rt::task::spawn({
        let data = data.clone();
        async move {
            data.transaction(meta(0), budget(), move |tx| {
                let _ = entered.send(());
                gate.wait();
                let first = tx.encode_value(&17)?;
                let second = tx.encode_value(&23)?;
                tx.put(b"first", first.as_slice())?;
                tx.put(b"second", second.as_slice())?;
                let one = tx.encode(&true)?;
                let two = tx.encode(&false)?;
                assert_eq!(one.as_slice(), b"true");
                assert_eq!(two.as_slice(), b"false");
                Ok(Transition::Commit(tx.copy_output(b"applied")?))
            })
            .await
        }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    let authority = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        control.read(budget(), |view| {
            view.get(b"authority")
                .and_then(|value| value.ok_or_else(capacity))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authority.as_slice(), b"current");
    drop(release);
    let result = nabedra_rt::time::timeout(Duration::from_secs(10), applying)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.as_slice(), b"applied");
    assert_eq!(resources.apply_bytes_used(), 0);
    let row = data
        .read(budget(), |view| {
            view.get(b"first")
                .and_then(|value| value.ok_or_else(capacity))
        })
        .await
        .unwrap();
    assert_eq!(row.as_slice(), b"17");
    // Returned bytes retain reply ownership, never a native-apply reservation.
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(
        data.applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        0
    );
    drop(row);
    drop(result);
    drop(authority);
    drop(wire);
    drop(progress_wire);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn f03_query_reply_capacity_wait_has_no_cpu_and_does_not_block_control_progress() {
    let resources = shared(SharedResourceConfig {
        query_reply_bytes: 64,
        control_reply_bytes: 256,
        ..Default::default()
    });
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let data = pool.group(group_identity(&pool)).await.unwrap();
    let mut identity = group_identity(&pool);
    identity.kind = GroupKind::Control;
    let control = pool.group(identity).await.unwrap();
    let small = TransactionBudget {
        output_bytes: 64,
        ..budget()
    };
    let retained = data
        .read(small, |view| view.copy_output(&[7u8; 64]))
        .await
        .unwrap();
    let waiting = nabedra_rt::task::spawn({
        let data = data.clone();
        async move { data.read(small, |view| view.copy_output(b"next")).await }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    let answer = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        control.read(small, |view| view.copy_output(b"authority")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(answer.as_slice(), b"authority");
    let committed = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        data.transaction(meta(0), small, |view| {
            Ok(Transition::Commit(view.copy_output(b"committed")?))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(committed.as_slice(), b"committed");
    assert_eq!(resources.reply_waiters(ExecutionClass::Query), 1);
    drop(committed);
    waiting.abort();
    let _ = waiting.await;
    drop(retained);
    drop(answer);
    resources.drain().await;
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o05_m01_wide_internal_log_read_retains_dynamic_owner_without_apply_or_reply_escape() {
    use crate::oltp::{
        mvcc::{
            CanonicalJson, Change, MAX_FIXED_COMMAND_BYTES, MvccKey, PreparedWrite,
            json_decode_budget,
        },
        pool::ScratchLease,
    };
    use nabedra_oltp_types::owned::OwnedCommand;
    use sha2::{Digest as _, Sha256};
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let encoded = {
        // The original row is <8MiB, but fixing its key separately makes the
        // durable internal command >8MiB. The frontend limit is unchanged.
        let key = "x".repeat(7 << 20);
        let row = CanonicalJson::from_serializable(
            &resources,
            &serde_json::json!({"id":key.clone()}),
            crate::protocol::MAX_BODY_BYTES,
        )
        .unwrap();
        let write = PreparedWrite {
            key: MvccKey::Row {
                table_id: nabedra_rt::ids::uuid_v4(),
                primary_key: crate::protocol::Key::Utf8(key),
            },
            expected_version: None,
            change: Change::Put(row),
        };
        let command = OwnedCommand::new_owned(write, &resources, ByteClass::Data).unwrap();
        resources.encode(&command, MAX_FIXED_COMMAND_BYTES).unwrap()
    };
    assert!(encoded.len() > crate::protocol::MAX_BODY_BYTES);
    assert!(encoded.len() > resources.config().query_reply_bytes);
    let expected_hash = Digest::from_bytes(Sha256::digest(encoded.as_slice()).into());
    group
        .log_transaction(
            TransactionBudget {
                scratch_bytes: 1 << 20,
                batch_bytes: 20 << 20,
                output_bytes: 1024,
            },
            move |tx| {
                tx.put(b"entry", encoded.as_slice())?;
                Ok(Transition::Commit(tx.copy_output(b"stored")?))
            },
        )
        .await
        .unwrap();
    assert_eq!(resources.buffer_bytes_used(ByteClass::Data), 0);
    #[derive(Serialize, Deserialize)]
    struct Footprint {
        bytes: usize,
        decode_bytes: usize,
        digest: Digest,
    }
    let probe = group
        .log_read_metadata(budget(), |view| {
            let footprint = view.inspect(b"entry", |bytes| {
                let bytes = bytes.ok_or_else(capacity)?;
                Ok(Footprint {
                    bytes: bytes.len(),
                    decode_bytes: json_decode_budget(bytes)?
                        .checked_add(1024)
                        .ok_or_else(capacity)?,
                    digest: Digest::from_bytes(Sha256::digest(bytes).into()),
                })
            })?;
            view.encode(&footprint)
        })
        .await
        .unwrap();
    let footprint: Footprint = serde_json::from_slice(probe.as_slice()).unwrap();
    assert_eq!(footprint.digest, expected_hash);
    drop(probe);
    // The memory-only await precedes reacquiring native/CPU. A log adapter
    // retries its probe if uncommitted entries changed during this interval.
    let owner = Arc::new(
        resources
            .reserve_scratch_wait(ByteClass::Data, footprint.decode_bytes, 900)
            .await
            .unwrap(),
    );
    let expected = footprint.digest;
    let total = owner.bytes();
    let command = group
        .log_read_owned(
            TransactionBudget {
                scratch_bytes: 20 << 20,
                batch_bytes: 0,
                output_bytes: 0,
            },
            owner.clone(),
            move |view, owner: Arc<ScratchLease>| {
                let bytes = view.get(b"entry")?.ok_or_else(capacity)?;
                if Digest::from_bytes(Sha256::digest(bytes.as_slice()).into()) != expected {
                    return Err(capacity());
                }
                OwnedCommand::<PreparedWrite>::decode_with_owner(
                    &bytes,
                    owner.clone(),
                    owner.bytes(),
                )
            },
        )
        .await
        .unwrap();
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(resources.buffer_bytes_used(ByteClass::Data), 0);
    let Change::Put(value) = &command.payload().unwrap().change else {
        panic!("expected fixed row")
    };
    let extracted = value.clone();
    drop(command);
    drop(owner);
    assert_eq!(resources.working_decode_bytes(), total);
    assert!(extracted.as_str().len() > 7 << 20);
    drop(extracted);
    assert_eq!(resources.working_decode_bytes(), 0);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[test]
fn m02_f03_retained_wire_and_decode_owners_prevent_worker_budget_reset() {
    let resources = shared(SharedResourceConfig::default());
    let worker = nabedra_rt::ids::uuid_v4();
    resources.bind_worker(worker).unwrap();
    let wire = resources.copy_bytes(b"retained").unwrap();
    let decoded = resources.reserve_scratch(ByteClass::Data, 1024).unwrap();
    let replacement = shared(SharedResourceConfig::default());
    drop(resources);
    assert!(replacement.bind_worker(worker).is_err());
    drop(wire);
    assert!(replacement.bind_worker(worker).is_err());
    drop(decoded);
    replacement.bind_worker(worker).unwrap();
}

/// C02, O05, NS04 (CIfix1): a finished native job has released its lease, and
/// with it the executor's hold on the worker resources, by the time its caller
/// sees the result. Otherwise a caller that drops its last owner and reopens
/// the worker's pool right away can find the old owner still alive and be
/// refused with "worker storage capacity unavailable" (seen on Linux CI in
/// `ns04_ns13_o11_crash_at_every_relocation_step_keeps_the_log`). The future is
/// spun with a no-op waker so the caller observes the result as soon as the
/// executor thread publishes it, not after a thread wake-up.
#[test]
fn c02_o05_ns04_native_job_releases_its_worker_owner_before_its_result() {
    let resources = shared(SharedResourceConfig::default());
    let worker = nabedra_rt::ids::uuid_v4();
    resources.bind_worker(worker).unwrap();
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for round in 0..2000 {
        let mut job = std::pin::pin!(resources.execute(ExecutionClass::Control, 0, 0, || Ok(())));
        loop {
            if let std::task::Poll::Ready(result) = job.as_mut().poll(&mut context) {
                result.unwrap();
                break;
            }
            std::hint::spin_loop();
        }
        assert_eq!(
            Arc::strong_count(&resources),
            1,
            "round {round}: the executor still owns the worker resources"
        );
    }
    drop(resources);
    shared(SharedResourceConfig::default())
        .bind_worker(worker)
        .unwrap();
}

#[tokio::test]
async fn f03_native_drain_waits_for_cancelled_guard_drop() {
    struct HeldDrop {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        gate: Arc<Gate>,
    }
    impl Drop for HeldDrop {
        fn drop(&mut self) {
            let _ = self.entered.take().unwrap().send(());
            self.gate.wait();
        }
    }

    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        let resources = shared(SharedResourceConfig::default());
        let baseline = resources.scheduler().usage();
        let prepared = resources.prepare(ExecutionClass::Query).await.unwrap();
        let reservation = prepared.reserve_compute(44, 1024).await.unwrap();
        let work_gate = Arc::new(Gate {
            open: Mutex::new(false),
            changed: Condvar::new(),
        });
        let drop_gate = Arc::new(Gate {
            open: Mutex::new(false),
            changed: Condvar::new(),
        });
        let release_work = ReleaseOnDrop(work_gate.clone());
        let release_drop = ReleaseOnDrop(drop_gate.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        let (dropping, cleanup) = tokio::sync::oneshot::channel();
        let task = nabedra_rt::task::spawn(async move {
            prepared
                .run_with_guard(
                    (
                        HeldDrop {
                            entered: Some(dropping),
                            gate: drop_gate,
                        },
                        reservation,
                    ),
                    move || {
                        started.send(()).unwrap();
                        work_gate.wait();
                        Ok(())
                    },
                )
                .await
        });
        running.await.expect("native work started");
        task.abort();
        assert!(task.await.err().is_some_and(|error| error.is_cancelled()));
        drop(release_work);
        cleanup.await.expect("cancelled guard started dropping");

        let mut drain = std::pin::pin!(resources.drain());
        assert!(
            futures_util::poll!(&mut drain).is_pending(),
            "native drain returned while the cancelled guard still held its scheduler permit"
        );
        let usage = resources.scheduler().usage();
        assert_eq!(usage[0].1, 1);
        assert_eq!(usage[0].2, baseline[0].2 + 1024);
        drop(release_drop);
        drain.await;
        assert_eq!(resources.scheduler().usage(), baseline);
    })
    .await
    .expect("native work, cancellation cleanup and drain watchdog");
}

#[tokio::test]
async fn f02_f03_owned_executor_survives_cancelled_receiver_guard_drop_panic() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("model guard destructor panic after receiver cancellation");
        }
    }
    let resources = shared(SharedResourceConfig::default());
    let prepared = resources.prepare(ExecutionClass::Query).await.unwrap();
    let reservation = resources
        .scheduler()
        .reserve_on(44, 1, resources.scheduler_node_id(), 1024)
        .await
        .unwrap();
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseOnDrop(gate.clone());
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let task = nabedra_rt::task::spawn(async move {
        prepared
            .run_with_guard((reservation, PanicOnDrop), move || {
                let _ = entered.send(std::thread::current().id());
                gate.wait();
                Ok(())
            })
            .await
    });
    let original_thread = nabedra_rt::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let _ = task.await;
    drop(release);
    let next = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        resources.execute(ExecutionClass::Query, 45, 0, || {
            Ok(std::thread::current().id())
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(next, original_thread);
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    resources.drain().await;
    assert_eq!(resources.control_bytes_used(), 0);
    assert_eq!(resources.working_decode_bytes(), 0);
}

#[tokio::test]
async fn m03_c07_reply_arena_bounds_bypass_and_protects_aged_large_waiter_headroom() {
    let resources = shared(SharedResourceConfig {
        query_reply_bytes: 64,
        ..Default::default()
    });
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let data = pool.group(group_identity(&pool)).await.unwrap();
    let mut identity = group_identity(&pool);
    identity.kind = GroupKind::Control;
    let control = pool.group(identity).await.unwrap();
    let small = TransactionBudget {
        output_bytes: 1,
        ..budget()
    };
    let large = TransactionBudget {
        output_bytes: 64,
        ..budget()
    };
    let retained = data
        .read(small, |view| view.copy_output(b"h"))
        .await
        .unwrap();
    let large_waiter = nabedra_rt::task::spawn({
        let data = data.clone();
        async move { data.read(large, |view| view.copy_output(&[9u8; 64])).await }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Before aging, each fitting small request finishes while the retained
    // one-byte response prevents the full-capacity request from fitting.
    for _ in 0..32 {
        let answer = nabedra_rt::time::timeout(
            Duration::from_secs(10),
            data.read(small, |view| view.copy_output(b"s")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(answer.as_slice(), b"s");
        drop(answer);
    }
    let small_waiter = nabedra_rt::task::spawn({
        let data = data.clone();
        async move { data.read(small, |view| view.copy_output(b"t")).await }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    assert!(!small_waiter.is_finished());
    let authority = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        control.read(budget(), |view| view.copy_output(b"control")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authority.as_slice(), b"control");
    drop(authority);
    drop(retained);
    let granted = nabedra_rt::time::timeout(Duration::from_secs(10), large_waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(granted.as_slice(), &[9u8; 64]);
    assert!(!small_waiter.is_finished());
    assert_eq!(resources.reply_waiters(ExecutionClass::Query), 1);
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    drop(granted);
    let small_result = nabedra_rt::time::timeout(Duration::from_secs(10), small_waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(small_result.as_slice(), b"t");
    drop(small_result);
    assert_eq!(resources.reply_waiters(ExecutionClass::Query), 0);
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn m03_f03_cancelling_aged_reply_waiter_restores_small_work_headroom() {
    let resources = shared(SharedResourceConfig {
        query_reply_bytes: 64,
        ..Default::default()
    });
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let data = pool.group(group_identity(&pool)).await.unwrap();
    let small = TransactionBudget {
        output_bytes: 1,
        ..budget()
    };
    let large = TransactionBudget {
        output_bytes: 64,
        ..budget()
    };
    let retained = data
        .read(small, |view| view.copy_output(b"h"))
        .await
        .unwrap();
    let head = nabedra_rt::task::spawn({
        let data = data.clone();
        async move { data.read(large, |view| view.copy_output(&[0; 64])).await }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for _ in 0..32 {
        drop(
            nabedra_rt::time::timeout(
                Duration::from_secs(10),
                data.read(small, |view| view.copy_output(b"s")),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    let follower = nabedra_rt::task::spawn({
        let data = data.clone();
        async move { data.read(small, |view| view.copy_output(b"f")).await }
    });
    nabedra_rt::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    head.abort();
    assert!(head.await.unwrap_err().is_cancelled());
    // The held byte still exists. Progress follows cancellation of headroom
    // protection, rather than the large allocation suddenly becoming feasible.
    let answer = nabedra_rt::time::timeout(Duration::from_secs(10), follower)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(answer.as_slice(), b"f");
    assert_eq!(retained.as_slice(), b"h");
    drop(answer);
    drop(retained);
    assert_eq!(resources.reply_waiters(ExecutionClass::Query), 0);
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o04_m01_scans_reuse_wide_key_scratch_and_copy_only_one_cursor_per_page() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let write = TransactionBudget {
        scratch_bytes: 16 << 10,
        batch_bytes: 2 << 20,
        output_bytes: 1024,
    };
    group
        .transaction(meta(0), write, |tx| {
            let padding = "x".repeat(8 << 10);
            for index in 0..130 {
                let key = format!("row/{index:04}{padding}");
                tx.put(key.as_bytes(), b"v")?;
            }
            Ok(Transition::Commit(tx.copy_output(b"ok")?))
        })
        .await
        .unwrap();
    #[derive(Deserialize)]
    struct Page {
        keys: Vec<u32>,
        cursor: Option<String>,
        done: bool,
    }
    let mut cursor: Option<Vec<u8>> = None;
    let mut seen = Vec::new();
    let mut pages = 0;
    loop {
        let next = cursor.take();
        let response = group
            .read(
                TransactionBudget {
                    scratch_bytes: 16 << 10,
                    batch_bytes: 0,
                    output_bytes: 64 << 10,
                },
                move |view| {
                    let mut keys = Vec::new();
                    let progress =
                        view.scan_page(b"row/", next.as_deref(), 128, |key, value| {
                            assert_eq!(value, b"v");
                            keys.push(
                                std::str::from_utf8(&key[4..8])
                                    .unwrap()
                                    .parse::<u32>()
                                    .unwrap(),
                            );
                            Ok(())
                        })?;
                    #[derive(Serialize)]
                    struct Wire<'a> {
                        keys: Vec<u32>,
                        cursor: Option<&'a str>,
                        done: bool,
                    }
                    view.encode(&Wire {
                        keys,
                        cursor: progress
                            .last_key
                            .as_ref()
                            .map(|key| std::str::from_utf8(key.as_slice()).unwrap()),
                        done: progress.exhausted,
                    })
                },
            )
            .await
            .unwrap();
        let page: Page = serde_json::from_slice(response.as_slice()).unwrap();
        seen.extend(page.keys);
        pages += 1;
        if page.done {
            break;
        }
        cursor = Some(page.cursor.unwrap().into_bytes());
    }
    assert_eq!(pages, 2);
    assert_eq!(seen, (0..130).collect::<Vec<u32>>());
    assert_eq!(resources.apply_bytes_used(), 0);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o11_scoped_log_range_delete_and_tail_marker_commit_in_one_batch() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let a = pool.group(group_identity(&pool)).await.unwrap();
    let b = pool.group(group_identity(&pool)).await.unwrap();
    for group in [&a, &b] {
        group
            .log_transaction(budget(), |tx| {
                for key in [b"log/1", b"log/2", b"log/3"] {
                    tx.put(key, b"entry")?;
                }
                tx.put(b"tail", b"3")?;
                Ok(Transition::Commit(tx.copy_output(b"ok")?))
            })
            .await
            .unwrap();
    }
    assert!(
        a.log_transaction(budget(), |tx| {
            tx.delete_range(b"log/2", b"log/4")?;
            tx.put(b"tail", b"1")?;
            Err(capacity())
        })
        .await
        .is_err()
    );
    assert_eq!(
        a.log_read(budget(), |view| view
            .get(b"log/3")
            .and_then(|value| value.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"entry"
    );
    assert_eq!(
        a.log_read(budget(), |view| view
            .get(b"tail")
            .and_then(|value| value.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"3"
    );
    a.log_transaction(budget(), |tx| {
        tx.delete_range(b"log/2", b"log/4")?;
        tx.put(b"tail", b"1")?;
        Ok(Transition::Commit(tx.copy_output(b"truncated")?))
    })
    .await
    .unwrap();
    for key in [b"log/2", b"log/3"] {
        assert_eq!(
            a.log_read(budget(), move |view| {
                let absent = view.get(key)?.is_none();
                view.encode(&absent)
            })
            .await
            .unwrap()
            .as_slice(),
            b"true"
        );
    }
    assert_eq!(
        a.log_read(budget(), |view| view
            .get(b"tail")
            .and_then(|value| value.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"1"
    );
    assert_eq!(
        b.log_read(budget(), |view| view
            .get(b"log/3")
            .and_then(|value| value.ok_or_else(capacity)))
            .await
            .unwrap()
            .as_slice(),
        b"entry"
    );
    assert_eq!(resources.control_bytes_used(), 0);
}

#[cfg_attr(test, tokio::test)]
#[cfg_attr(not(test), tokio::main(flavor = "current_thread"))]
async fn o11_snapshot_membership_comes_from_immutable_cut_and_rejects_corrupt_record() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let mut first = meta(0);
    first.membership = Some(resources.copy_bytes(b"membership-a").unwrap());
    group
        .transaction(first, budget(), |tx| {
            Ok(Transition::Commit(tx.copy_output(b"old")?))
        })
        .await
        .unwrap();
    let original = group.export_snapshot(budget()).await.unwrap();
    let mut bytes = fs::read(&original.path).unwrap();
    let mut second = meta(1);
    second.membership = Some(resources.copy_bytes(b"membership-b").unwrap());
    group
        .transaction(second, budget(), |tx| {
            Ok(Transition::Commit(tx.copy_output(b"new")?))
        })
        .await
        .unwrap();
    let _current = group.export_snapshot(budget()).await.unwrap();
    let exact = group
        .snapshot_membership(original.clone(), budget())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.as_slice(), b"membership-a");
    drop(exact);
    let offset = bytes
        .windows(b"membership-a".len())
        .position(|bytes| bytes == b"membership-a")
        .unwrap();
    bytes[offset + b"membership-".len()] = b'x';
    let path = dir.0.join("corrupt-member.bin");
    fs::write(&path, &bytes).unwrap();
    let file = File::open(&path).unwrap();
    let damaged = GroupSnapshot {
        file: PoolSnapshotFile::new(file, group.pool().clone()),
        path,
        bytes: bytes.len() as u64,
        header: original.header,
    };
    assert!(group.snapshot_membership(damaged, budget()).await.is_err());
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .membership
            .unwrap()
            .as_slice(),
        b"membership-b"
    );
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn c07_o05_consensus_metadata_progresses_with_pending_data_apply_and_query_slot_held() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let query = resources.prepare(ExecutionClass::Query).await.unwrap();
    let mut apply = Box::pin(group.transaction(meta(0), budget(), |tx| {
        tx.put(b"row", b"committed")?;
        Ok(Transition::Commit(tx.copy_output(b"applied")?))
    }));
    assert!(futures_util::poll!(apply.as_mut()).is_pending());
    // It has reached the native query-capacity wait while retaining state order.
    assert!(group.inner.order.try_lock().is_err());
    assert_eq!(
        resources
            .scheduler()
            .usage()
            .iter()
            .map(|entry| entry.1)
            .sum::<u64>(),
        0
    );
    nabedra_rt::time::timeout(
        Duration::from_secs(5),
        group.log_transaction_metadata(budget(), |tx| {
            tx.put(b"vote", b"term-7")?;
            Ok(Transition::Commit(tx.copy_output(b"saved")?))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let vote = nabedra_rt::time::timeout(
        Duration::from_secs(5),
        group.log_read_metadata(budget(), |view| view.get(b"vote")?.ok_or_else(capacity)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(vote.as_slice(), b"term-7");
    drop(vote);
    // Large entry probes must still wait for the held query slot.
    let mut probe = Box::pin(group.log_read_probe(budget(), |view| view.copy_output(b"probe")));
    assert!(futures_util::poll!(probe.as_mut()).is_pending());
    assert_eq!(
        resources
            .scheduler()
            .usage()
            .iter()
            .map(|entry| entry.1)
            .sum::<u64>(),
        0
    );
    drop(query);
    assert_eq!(apply.await.unwrap().as_slice(), b"applied");
    assert_eq!(probe.await.unwrap().as_slice(), b"probe");
    assert_eq!(get(&group, b"row").await, b"committed");
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position,
        meta(0).position
    );
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn o11_c07_committed_membership_serializes_in_apply_with_all_wire_bytes_held() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let config = resources.config();
    let progress = resources
        .allocate_bytes(ByteClass::Progress, config.progress_bytes)
        .unwrap();
    let data = resources
        .allocate_bytes(
            ByteClass::Data,
            config.control_bytes - config.progress_bytes,
        )
        .unwrap();
    let reply = group
        .transaction_with_membership(meta(0), budget(), Some([3u64, 5, 8]), |tx| {
            tx.put(b"row", b"same-batch")?;
            Ok(Transition::Commit(tx.copy_output(b"member")?))
        })
        .await
        .unwrap();
    assert_eq!(reply.as_slice(), b"member");
    drop(reply);
    let applied = group.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(0).position);
    assert_eq!(applied.membership.unwrap().as_slice(), b"[3,5,8]");
    // An exact replay cannot evaluate either application logic or serialization.
    struct CannotSerialize;
    impl Serialize for CannotSerialize {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("replay must not serialize membership")
        }
    }
    group
        .transaction_with_membership(meta(0), budget(), Some(CannotSerialize), |_| {
            panic!("replay must not rerun application")
        })
        .await
        .unwrap();
    assert_eq!(get(&group, b"row").await, b"same-batch");
    drop((progress, data));
    assert_eq!(resources.control_bytes_used(), 0);
}

#[tokio::test]
async fn m02_c07_reply_decode_has_independent_bounded_ownership_with_raw_and_wire_arenas_full() {
    let resources = shared(SharedResourceConfig::default());
    let config = resources.config();
    let raw = resources
        .reply_budget(ExecutionClass::Control, config.control_reply_bytes)
        .await
        .unwrap();
    let progress = resources
        .allocate_bytes(ByteClass::Progress, config.progress_bytes)
        .unwrap();
    let data = resources
        .allocate_bytes(
            ByteClass::Data,
            config.control_bytes - config.progress_bytes,
        )
        .unwrap();
    let owner = Arc::new(
        nabedra_rt::time::timeout(Duration::from_secs(5), resources.reserve_reply_decode(4096))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(owner.bytes(), 4096);
    let retained = owner.clone();
    drop(owner);
    assert_eq!(resources.reply_decode_bytes_used(), 4096);
    assert!(
        resources
            .reserve_reply_decode(config.reply_decode_bytes + 1)
            .await
            .is_err()
    );
    drop((retained, raw, progress, data));
    assert_eq!(resources.control_bytes_used(), 0);
}

pub mod kv;
#[cfg(test)]
mod snap1;

pub fn acceptance_damaged_checkpoints() {
    o11_o16_inconsistent_snapshot_header_body_is_rejected_without_changing_active_state();
    o11_snapshot_membership_comes_from_immutable_cut_and_rejects_corrupt_record();
}

#[tokio::test]
async fn o22_c07_dependent_pool_writers_wait_without_compute() {
    use std::{future::Future as _, task::Poll};
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let data = pool.group(group_identity(&pool)).await.unwrap();
    let baseline = resources.scheduler().usage();
    let order = data.inner.order.lock().await;
    let first = data.transaction(meta(0), budget(), |tx| {
        tx.put(b"first", b"1")?;
        Ok(Transition::Commit(tx.copy_output(b"1")?))
    });
    let second = data.transaction(meta(1), budget(), |tx| {
        tx.put(b"second", b"2")?;
        Ok(Transition::Commit(tx.copy_output(b"2")?))
    });
    tokio::pin!(first, second);
    std::future::poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        assert!(second.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(resources.scheduler().usage(), baseline);
    let read = nabedra_rt::time::timeout(
        Duration::from_secs(10),
        data.read(budget(), |view| {
            assert!(view.get(b"first")?.is_none());
            view.copy_output(b"available")
        }),
    )
    .await
    .expect("read blocked behind writer ownership")
    .unwrap();
    assert_eq!(read.as_slice(), b"available");
    drop(read);
    drop(order);
    let (first, second) = nabedra_rt::time::timeout(Duration::from_secs(10), async {
        tokio::join!(first, second)
    })
    .await
    .expect("waiting writers did not settle");
    assert_eq!(first.unwrap().as_slice(), b"1");
    assert_eq!(second.unwrap().as_slice(), b"2");
    let result = data
        .read(budget(), |view| {
            assert_eq!(view.get(b"first")?.unwrap().as_slice(), b"1");
            assert_eq!(view.get(b"second")?.unwrap().as_slice(), b"2");
            view.copy_output(b"done")
        })
        .await
        .unwrap();
    drop(result);
    resources.drain().await;
    assert_eq!(resources.scheduler().usage(), baseline);
}
