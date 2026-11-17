use super::*;
use crate::{
    config::WorkerResources,
    oltp_replication::types::{GroupKind, Text},
};
use std::{
    path::PathBuf,
    sync::{Condvar, Mutex},
    time::Duration,
};

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("oltp-pool-{}", Uuid::new_v4())))
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn shared(config: SharedResourceConfig) -> Arc<WorkerSharedResources> {
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
fn pool_identity() -> PoolIdentity {
    PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: 1,
        cluster_id: Uuid::new_v4(),
        worker_id: Uuid::new_v4(),
        pool_id: Uuid::new_v4(),
    }
}
fn group_identity(pool: &OltpPool) -> GroupIdentity {
    GroupIdentity {
        cluster_id: pool.identity().cluster_id,
        database: Text::try_from("default").unwrap(),
        group_id: Uuid::new_v4(),
        kind: GroupKind::Catalog,
        group_format_version: 1,
    }
}
fn budget() -> TransactionBudget {
    TransactionBudget {
        scratch_bytes: 64 << 10,
        batch_bytes: 64 << 10,
        output_bytes: 64 << 10,
    }
}
fn meta(index: u64) -> ApplyMeta {
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
async fn get(group: &GroupStore, key: &'static [u8]) -> Vec<u8> {
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

struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
}
struct ReleaseOnDrop(Arc<Gate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
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
        tasks.push(runtime.spawn(async move {
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
                tokio::time::timeout(Duration::from_secs(10), arrivals.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
    });
    assert_eq!(threads.len(), 7);
    assert_eq!(resources.scheduler().usage()[0].1, 7);
    tasks[0].abort();
    runtime.block_on(async {
        tokio::task::yield_now().await;
    });
    assert_eq!(resources.scheduler().usage()[0].1, 7);
    let control = runtime
        .block_on(resources.execute(ExecutionClass::Control, 99, 0, || {
            Ok(std::thread::current().id())
        }))
        .unwrap();
    assert!(!threads.contains(&control));
    let draining = runtime.spawn({
        let resources = resources.clone();
        async move { resources.drain().await }
    });
    runtime.block_on(async {
        tokio::task::yield_now().await;
    });
    assert!(!draining.is_finished());
    gate.release();
    drop(release);
    runtime.block_on(async {
        for task in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), draining)
            .await
            .unwrap()
            .unwrap();
    });
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    assert!(
        runtime
            .block_on(resources.prepare(ExecutionClass::Query))
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
    assert!(!first.path.exists());
    assert!(first.file.metadata().unwrap().len() > 0);
    let old = target.inner.db.snapshot();
    let old_prefix = b.prefix(&old, false).unwrap();
    b.install_snapshot(first.clone(), budget()).await.unwrap();
    assert_eq!(get(&b, b"key").await, b"first");
    b.install_snapshot(second, budget()).await.unwrap();
    assert_eq!(get(&b, b"key").await, b"second");
    assert!(b.install_snapshot(first, budget()).await.is_err());
    let current = b.current_snapshot(budget()).await.unwrap().unwrap();
    assert_eq!(current.header.applied.unwrap().index, 2);
    assert_eq!(get(&b, b"new").await, b"added");
    let old_key = [old_prefix.as_slice(), b"key"].concat();
    assert_eq!(old.get_pinned(old_key).unwrap().unwrap().as_ref(), b"old");
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

#[tokio::test]
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
        file: Arc::new(file),
        path,
        bytes: length,
        header,
    };
    assert!(b.install_snapshot(incoming, budget()).await.is_err());
    assert_eq!(
        u64::from_be_bytes(
            target
                .inner
                .db
                .get_pinned(group_key(b.identity().group_id, b'a', &[]))
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
            .inner
            .db
            .get_pinned(group_key(b.identity().group_id, b't', &[]))
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
                .inner
                .db
                .get_pinned(group_key(b.identity().group_id, b't', &[]))
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
            query_threads: 1,
            control_threads: 1,
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
    let task_bytes = limits.task_memory_limit_bytes;
    let already = resources.scheduler().usage()[0].2;
    let retained = resources
        .scheduler()
        .charge_memory(17, limits.working_bytes() - already - task_bytes)
        .unwrap();
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let release = ReleaseOnDrop(gate.clone());
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let query = tokio::spawn({
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
    tokio::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resources.scheduler().usage()[0].2, limits.working_bytes());
    let authority = tokio::time::timeout(
        Duration::from_secs(10),
        group.read(budget(), |view| {
            view.get(b"authority").and_then(|v| v.ok_or_else(capacity))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authority.as_slice(), b"current");
    assert_eq!(resources.scheduler().usage()[0].1, 1);
    drop(authority);
    drop(release);
    query.await.unwrap().unwrap();
    drop(retained);
    assert_eq!(resources.control_bytes_used(), 0);
}

#[test]
fn c02_o05_worker_identity_cannot_reset_budgets_or_adopt_a_second_resource_owner() {
    let resources = shared(SharedResourceConfig::default());
    let worker = Uuid::new_v4();
    resources.bind_worker(worker).unwrap();
    assert!(resources.bind_worker(Uuid::new_v4()).is_err());
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
    let worker = Uuid::new_v4();
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
    let applying = tokio::spawn({
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
    tokio::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    let authority = tokio::time::timeout(
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
    let result = tokio::time::timeout(Duration::from_secs(10), applying)
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
    let waiting = tokio::spawn({
        let data = data.clone();
        async move { data.read(small, |view| view.copy_output(b"next")).await }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while resources.reply_waiters(ExecutionClass::Query) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(resources.scheduler().usage()[0].1, 0);
    let answer = tokio::time::timeout(
        Duration::from_secs(10),
        control.read(small, |view| view.copy_output(b"authority")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(answer.as_slice(), b"authority");
    let committed = tokio::time::timeout(
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
    use crate::oltp_replication::owned::OwnedCommand;
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
                table_id: Uuid::new_v4(),
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
    let worker = Uuid::new_v4();
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
    let task = tokio::spawn(async move {
        prepared
            .run_with_guard((reservation, PanicOnDrop), move || {
                let _ = entered.send(std::thread::current().id());
                gate.wait();
                Ok(())
            })
            .await
    });
    let original_thread = tokio::time::timeout(Duration::from_secs(10), arrived)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let _ = task.await;
    drop(release);
    let next = tokio::time::timeout(
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
