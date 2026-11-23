//! Pool-level fault model on the same per-node volume as the KV store.
use super::tests::{group_identity, meta, shared};
use super::*;
use nabedra_rt::{
    fs::FsCall,
    sim::{self, Cut, NodeClock, VolumeInfo, Wipe, World},
};
use std::sync::Mutex;

const NODE: u64 = 1;
const DATA: &str = "/sim/n1/data";
const KEYS: &str = "/sim/n1/keys";
const SEED: u64 = 41;

fn identity() -> PoolIdentity {
    PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: POOL_FORMAT_VERSION,
        cluster_id: Uuid::from_u128(1),
        worker_id: Uuid::from_u128(2),
        pool_id: Uuid::from_u128(3),
    }
}

fn open() -> OltpPool {
    OltpPool::open_development_on(
        DATA,
        identity(),
        shared(SharedResourceConfig::default()),
        nabedra_rt::fs::real(),
    )
    .unwrap()
}

fn put(pool: &OltpPool, key: &[u8], durability: Durability) -> Result<(), ProtocolError> {
    let mut batch = WriteBatch::new();
    batch.put(key, b"present");
    let commit = pool.submit(&[], batch, durability, Box::new(()))?;
    wait_commit(commit).map_err(|error| pool.failed(error))?;
    Ok(())
}

fn assert_key(pool: &OltpPool, key: &[u8], present: bool) {
    assert_eq!(
        pool.get_latest(key).unwrap().as_deref(),
        present.then_some(b"present".as_slice()),
        "key {key:?}"
    );
}

fn mount_data(world: &World) -> VolumeInfo {
    world.add_node(NODE, NodeClock::default());
    world.mount(NODE, DATA)
}

fn commit_cut(cut: Option<u64>) -> (u64, Vec<FsCall>) {
    sim::run(SEED, |world| async move {
        let info = mount_data(&world);
        let pool = sim::in_node(NODE, async {
            let pool = open();
            assert_eq!(pool.inner.disk.device(), info.dev);
            put(&pool, b"base", Durability::Sync).unwrap();
            pool
        })
        .await;
        world.crash(NODE);
        drop(pool);
        world.restart_with(NODE, Cut::Crash);

        let volume = world.volume(NODE, Path::new(DATA)).unwrap();
        let (pool, acknowledged, operations, trace) = sim::in_node(NODE, async {
            assert_eq!(
                serde_json::from_slice::<PoolIdentity>(
                    &fs::read(Path::new(DATA).join(POOL_MANIFEST))
                        .expect("the pool manifest must survive the crash"),
                )
                .unwrap(),
                identity(),
            );
            let pool = open();
            assert_key(&pool, b"base", true);
            let start = volume.mutating_ops();
            volume.clear_calls();
            if let Some(cut) = cut {
                world.crash_volume_after(NODE, Path::new(DATA), start + cut);
            }
            let acknowledged = put(&pool, b"next", Durability::Sync).is_ok();
            (
                pool,
                acknowledged,
                volume.mutating_ops() - start,
                volume.calls(),
            )
        })
        .await;
        world.crash(NODE);
        drop(pool);
        world.restart_with(NODE, Cut::Crash);
        sim::in_node(NODE, async {
            let recovered = open();
            assert_key(&recovered, b"base", true);
            let next = recovered.get_latest(b"next").unwrap();
            if acknowledged {
                assert_eq!(next.as_deref(), Some(b"present".as_slice()));
            }
            assert!(next.is_none() || next.as_deref() == Some(b"present".as_slice()));
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
        (operations, trace)
    })
}

#[test]
fn ns04_o11_restart_and_crash_at_each_commit_operation_replay() {
    let reference = commit_cut(None);
    assert!(reference.0 > 0, "the test must enumerate volume mutations");
    assert_eq!(commit_cut(None), reference);
    for cut in 0..=reference.0 {
        let first = commit_cut(Some(cut));
        assert_eq!(first.0, cut);
        assert_eq!(commit_cut(Some(cut)), first, "cut after {cut} operations");
    }
    eprintln!(
        "fault model: seed {SEED}, {} commit cuts replayed",
        reference.0 + 1
    );
}

fn observe(world: &World) -> Arc<Mutex<Vec<String>>> {
    let trace = Arc::new(Mutex::new(Vec::new()));
    world.observe({
        let trace = trace.clone();
        Arc::new(move |event| trace.lock().unwrap().push(format!("{event:?}")))
    });
    trace
}

#[test]
fn ns04_o11_process_kill_preserves_unsynced_pool_writes() {
    let replay = |cut, retained| {
        sim::run(SEED, |world| async move {
            mount_data(&world);
            let trace = observe(&world);
            let pool = sim::in_node(NODE, async {
                let pool = open();
                put(&pool, b"base", Durability::Sync).unwrap();
                put(&pool, b"tail", Durability::NoSync).unwrap();
                assert!(pool.has_unsynced_writes());
                assert_key(&pool, b"tail", true);
                pool
            })
            .await;

            // Halt services before they can close or sync the pool.
            world.crash(NODE);
            drop(pool);
            world.restart_with(NODE, cut);
            sim::in_node(NODE, async {
                let pool = open();
                assert_key(&pool, b"base", true);
                assert_key(&pool, b"tail", retained);
            })
            .await;
            assert_eq!(world.unmounted_path_errors(), 0);
            let events = trace.lock().unwrap();
            assert!(!events.is_empty());
            events.clone()
        })
    };
    for (cut, retained) in [(Cut::Kill, true), (Cut::Crash, false)] {
        assert_eq!(replay(cut, retained), replay(cut, retained), "{cut:?}");
    }
}

fn expectation(info: &VolumeInfo) -> mount::PoolMountExpectation {
    let identity = identity();
    mount::PoolMountExpectation {
        profile: mount::PoolMountProfile::Ebs,
        worker: mount::tests::worker_identity(identity.worker_id, Uuid::from_u128(4)),
        identity,
        generation: 1,
        mount_path: info.mount_path.clone(),
        provider_volume_id: "vol-01234567".into(),
        filesystem_uuid: info.filesystem_uuid.to_string(),
    }
}

#[test]
fn o17_mount_identity_and_disk_budget_follow_volume() {
    let replay = || {
        sim::run(SEED, |world| async move {
            let info = mount_data(&world);
            let keys = world.mount(NODE, KEYS);
            let expected = expectation(&info);
            let trace = observe(&world);
            let (old_inode, pool) = sim::in_node(NODE, async {
                let key = Path::new(KEYS).join("worker-key");
                fs::write(&key, b"identity").unwrap();
                File::open(&key).unwrap().sync_all().unwrap();
                fs::sync_dir(KEYS).unwrap();
                let mount = VerifiedPoolMount::verify(expected.clone(), None).unwrap();
                let observed = mount.observation();
                assert_eq!(observed.mount_id, info.mount_id);
                assert_eq!(
                    (observed.device_major, observed.device_minor),
                    (info.major, info.minor)
                );
                assert_eq!(
                    observed.total_bytes,
                    nabedra_rt::sim::volumes::CAPACITY_BYTES
                );
                assert!(VerifiedPoolMount::verify(expected.clone(), None).is_err());
                let first = disk::PoolDisk::open(Path::new(DATA), 4096).unwrap();
                let second = disk::PoolDisk::open(Path::new(KEYS), 4096).unwrap();
                assert_eq!(first.device(), info.dev);
                assert_eq!(second.device(), keys.dev);
                assert_ne!(first.device(), second.device());
                let _reservation = first.reserve(512, 1).unwrap();
                assert_eq!(
                    serde_json::to_value(first.stats()).unwrap()["reservations"],
                    1
                );
                assert_eq!(
                    serde_json::to_value(second.stats()).unwrap()["reservations"],
                    0
                );
                let old_inode = observed.root_inode;
                let pool = OltpPool::open_registered(
                    mount,
                    identity(),
                    shared(SharedResourceConfig::default()),
                )
                .unwrap();
                put(&pool, b"mounted", Durability::Sync).unwrap();
                (old_inode, pool)
            })
            .await;
            world.crash(NODE);
            drop(pool);
            world.restart_with(NODE, Cut::Crash);
            sim::in_node(NODE, async {
                let mount = VerifiedPoolMount::verify(expected.clone(), None).unwrap();
                assert_eq!(mount.observation().root_inode, old_inode);
                let pool = OltpPool::open_registered(
                    mount,
                    identity(),
                    shared(SharedResourceConfig::default()),
                )
                .unwrap();
                assert_key(&pool, b"mounted", true);
            })
            .await;

            world.crash(NODE);
            world.wipe(NODE, Path::new(DATA), Wipe::Reformat);
            world.restart_with(NODE, Cut::Crash);
            let reformatted = world.volume_info(NODE, Path::new(DATA)).unwrap();
            assert_eq!(reformatted.dev, info.dev);
            assert_ne!(reformatted.filesystem_uuid, info.filesystem_uuid);
            sim::in_node(NODE, async {
                assert!(
                    expected.inspect_mount().is_err(),
                    "old inventory accepted wiped data"
                );
                assert_eq!(
                    fs::read(Path::new(KEYS).join("worker-key")).unwrap(),
                    b"identity"
                );
                let mount = VerifiedPoolMount::verify(expectation(&reformatted), None).unwrap();
                assert_ne!(mount.observation().root_inode, old_inode);
            })
            .await;
            assert_eq!(world.unmounted_path_errors(), 0);
            let events = trace.lock().unwrap().clone();
            assert!(!events.is_empty());
            events
        })
    };
    assert_eq!(replay(), replay());
}

#[test]
fn o11_snapshot_install_recovers_the_generation_at_activation() {
    const SOURCE: &str = "/sim/n1/source";
    let replay = |after_activation| {
        sim::run(SEED, |world| async move {
            mount_data(&world);
            world.mount(NODE, SOURCE);
            let volume = world.volume(NODE, Path::new(DATA)).unwrap();
            let budget = TransactionBudget {
                scratch_bytes: 64 << 10,
                batch_bytes: 64 << 10,
                output_bytes: 64 << 10,
            };
            let (target, identity) = sim::in_node(NODE, async {
                let target = open();
                let identity = group_identity(&target);
                let source = OltpPool::open_development_on(
                    SOURCE,
                    PoolIdentity {
                        pool_id: Uuid::from_u128(7),
                        ..self::identity()
                    },
                    target.resources().clone(),
                    nabedra_rt::fs::real(),
                )
                .unwrap();
                let source_group = source.group(identity.clone()).await.unwrap();
                let target_group = target.group(identity.clone()).await.unwrap();
                for (group, index, value) in [
                    (&target_group, 0, b"old".as_slice()),
                    (&source_group, 1, b"new".as_slice()),
                ] {
                    group
                        .transaction(meta(index), budget, move |tx| {
                            tx.put(b"key", value)?;
                            Ok(Transition::Commit(tx.copy_output(b"ok")?))
                        })
                        .await
                        .unwrap();
                }
                let snapshot = source_group.export_snapshot(budget).await.unwrap();
                let cut = volume.clone();
                assert!(
                    target_group
                        .install_with_hook(snapshot, budget, move |point| {
                            if matches!(point, snapshot::InstallPoint::AfterActivation)
                                == after_activation
                            {
                                cut.halt();
                                return Err(recovery_unknown());
                            }
                            Ok(())
                        })
                        .await
                        .is_err()
                );
                assert!(volume.is_crashed(), "installation must reach the cut");
                (target, identity)
            })
            .await;
            world.crash(NODE);
            drop(target);
            world.restart_with(NODE, Cut::Crash);
            sim::in_node(NODE, async {
                let recovered = open();
                let group = recovered.group(identity).await.unwrap();
                let value = group
                    .read(budget, |view| {
                        view.get(b"key")?.ok_or_else(recovery_unknown)
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    value.as_slice(),
                    if after_activation { b"new" } else { b"old" }
                );
            })
            .await;
            assert_eq!(world.unmounted_path_errors(), 0);
            volume.calls()
        })
    };
    for after_activation in [false, true] {
        assert_eq!(replay(after_activation), replay(after_activation));
    }
}
