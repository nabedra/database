//! Simulated volumes. Each test fails when the property it names is broken.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{CAPACITY_BYTES, Cut, MAJOR, VolumeInfo, Wipe, makedev, volume_for};
use crate::fs::os::tests::{host_dir, scenario};
use crate::fs::{self, FaultFs, Fs, FsKind, OpenMode, os};
use crate::sim::{NodeClock, SimEvent, World, in_node, run};
use crate::sync::Condvar;
use crate::task::{self, Step, Stepped};

fn p(path: &str) -> PathBuf {
    PathBuf::from(path)
}

fn node_with(world: &World, node: u64, mounts: &[&str]) -> Vec<VolumeInfo> {
    world.add_node(node, NodeClock::default());
    mounts.iter().map(|path| world.mount(node, *path)).collect()
}

/// Runs synchronous `f` inside `node` on the simulation thread.
async fn on<T>(node: u64, f: impl FnOnce() -> T) -> T {
    in_node(node, async move { f() }).await
}

#[test]
fn fs_os_on_a_volume_matches_the_host_transcript() {
    let host_root = host_dir("contract");
    let host = scenario(&host_root);
    std::fs::remove_dir_all(&host_root).unwrap();
    let simulated = run(1, |world| async move {
        node_with(&world, 1, &["/sim/n1/data"]);
        let transcript = on(1, || scenario(Path::new("/sim/n1/data"))).await;
        assert_eq!(world.unmounted_path_errors(), 0);
        transcript
    });
    assert_eq!(
        simulated, host,
        "fs::os on a simulated volume differs from std::fs"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn directory_descriptor_follows_rename_on_a_volume() {
    run(1, |world| async move {
        let root = Path::new("/sim/n1/data");
        node_with(&world, 1, &["/sim/n1/data"]);
        let volume = world.volume(1, root).unwrap();
        on(1, || {
            let (directory, relative_identity) =
                crate::fs::os::tests::directory_descriptor_after_rename(root);
            let syncs: Vec<_> = volume
                .calls()
                .into_iter()
                .filter(|call| call.op == fs::FsOp::SyncDir)
                .map(|call| call.path)
                .collect();
            assert_eq!(syncs, vec![root.join("old-pool"); 2]);

            os::remove_dir_all(root.join("old-pool")).unwrap();
            assert!(directory.metadata().is_err());
            assert!(directory.sync_all().is_err());
            assert!(directory.sync_data().is_err());
            assert!(directory.try_lock_exclusive().is_err());
            assert!(os::read(&relative_identity).is_err());
            drop(directory);
            assert!(os::read(&relative_identity).is_err());
            assert_eq!(
                os::read(root.join("pool/identity")).unwrap(),
                b"replacement"
            );
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn ns04_o11_directory_descriptor_sync_preserves_pre_rename_child_after_crash() {
    use crate::fs::os::tests::{BEFORE_RENAME_BYTES, sync_pre_rename_child};

    run(21, |world| async move {
        let root = Path::new("/sim/n1/data");
        node_with(&world, 1, &["/sim/n1/data"]);
        let manifest = on(1, || sync_pre_rename_child(root)).await;

        world.restart_with(1, Cut::Crash);
        on(1, || {
            assert_eq!(os::read(&manifest).unwrap(), BEFORE_RENAME_BYTES);
            assert!(!os::try_exists(root.join("staging")).unwrap());
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
    });
}

#[test]
fn a_mount_has_a_seeded_identity() {
    let infos = |seed| {
        run(seed, |world| async move {
            let infos = node_with(&world, 1, &["/sim/n1/keys", "/sim/n1/data"]);
            let stat = world
                .volume(1, Path::new("/sim/n1/data"))
                .unwrap()
                .stat(Path::new("/sim/n1/data"))
                .unwrap();
            (infos, stat)
        })
    };
    let (first, stat) = infos(7);
    assert_eq!(infos(7).0, first, "one seed, one identity");
    assert_ne!(infos(8).0[0].filesystem_uuid, first[0].filesystem_uuid);
    let (keys, data) = (&first[0], &first[1]);
    assert_eq!((keys.major, data.major), (MAJOR, MAJOR));
    assert_ne!(keys.minor, data.minor);
    assert_eq!(data.dev, makedev(MAJOR, data.minor));
    assert_ne!(keys.mount_id, data.mount_id);
    assert_eq!(data.filesystem_uuid.get_version_num(), 4);
    assert_eq!((stat.kind, stat.dev), (FsKind::Dir, data.dev));
    assert_eq!(makedev(259, 1), (259 << 8) | 1);
    assert_eq!(makedev(8, 300), 0x0010_0000 | (8 << 8) | 0x2c);
}

#[test]
fn paths_route_to_the_longest_mount_and_outside_paths_fail() {
    run(2, |world| async move {
        node_with(&world, 1, &["/sim/n1", "/sim/n1/data"]);
        node_with(&world, 2, &[]);
        on(1, || {
            os::write("/sim/n1/data/f", b"data").unwrap();
            os::write("/sim/n1/top", b"top").unwrap();
            let error = os::read("/elsewhere/f").unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(error.to_string().contains("/elsewhere/f"), "{error}");
            assert_eq!(
                os::read("relative/f").unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
            assert_eq!(os::euid(), FaultFs::UID);
            assert_eq!(
                volume_for(Path::new("/sim/n1/data/x")).unwrap().mount_path,
                p("/sim/n1/data")
            );
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 2);
        let data = world.volume(1, Path::new("/sim/n1/data")).unwrap();
        let top = world.volume(1, Path::new("/sim/n1")).unwrap();
        assert_eq!(data.contents(Path::new("/sim/n1/data/f")).unwrap(), b"data");
        assert!(top.contents(Path::new("/sim/n1/data/f")).is_err());
        assert_eq!(top.contents(Path::new("/sim/n1/top")).unwrap(), b"top");
        // A rename across volumes is EXDEV, as on Linux.
        on(1, || {
            assert_eq!(
                os::rename("/sim/n1/top", "/sim/n1/data/top")
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::CrossesDevices
            );
        })
        .await;
        // A node without mounts, and code outside any node, use the host.
        let host = host_dir("unmounted");
        let file = host.join("f");
        on(2, || os::write(&file, b"host").unwrap()).await;
        assert_eq!(std::fs::read(&file).unwrap(), b"host");
        assert!(os::try_exists(&file).unwrap());
        std::fs::remove_dir_all(&host).unwrap();
        assert_eq!(world.unmounted_path_errors(), 2);
    });
}

#[test]
fn fs_real_requires_a_new_handle_after_restart() {
    run(3, |world| async move {
        node_with(&world, 1, &["/sim/n1/data"]);
        let path = p("/sim/n1/data/store.log");
        let (fs, file) = on(1, || {
            let fs = fs::real();
            let file = fs.open(&path, OpenMode::CreateNew).unwrap();
            file.write_all_at(b"synced", 0).unwrap();
            file.sync_data().unwrap();
            fs.sync_dir(Path::new("/sim/n1/data")).unwrap();
            file.write_all_at(b"+unsynced", 6).unwrap();
            (fs, file)
        })
        .await;
        // Outside any node, fs::real() is the host filesystem.
        assert!(
            fs::real()
                .exists(Path::new("/sim/n1/data"))
                .is_ok_and(|e| !e)
        );
        world.restart_with(1, Cut::Crash);
        // The old handle points at the halted image.
        assert!(file.write_all_at(b"late", 0).is_err());
        // The old NodeFs cannot reopen or alter any path in the replacement image.
        assert!(
            fs.open(&p("/sim/n1/data/late"), OpenMode::CreateNew)
                .is_err()
        );
        // A fresh NodeFs sees the new image and only the synced bytes.
        let bytes = on(1, || {
            assert!(!os::try_exists("/sim/n1/data/late").unwrap());
            let reopened = fs::real().open(&path, OpenMode::Read).unwrap();
            let mut bytes = vec![0; reopened.len().unwrap() as usize];
            reopened.read_exact_at(&mut bytes, 0).unwrap();
            bytes
        })
        .await;
        assert_eq!(bytes, b"synced");
    });
}

#[test]
fn restart_cuts_keep_what_each_failure_keeps() {
    let after = |cut: Cut| {
        run(4, move |world| async move {
            node_with(&world, 1, &["/sim/n1/data"]);
            on(1, || {
                let file = os::File::create("/sim/n1/data/f").unwrap();
                (&file).write_all(&[1u8; 4096]).unwrap();
                file.sync_all().unwrap();
                os::sync_dir("/sim/n1/data").unwrap();
                file.write_all_at(&[2u8; 4096], 0).unwrap();
            })
            .await;
            world.restart_with(1, cut);
            assert!(world.is_up(1));
            on(1, || os::read("/sim/n1/data/f").unwrap()).await
        })
    };
    assert_eq!(
        after(Cut::Kill),
        vec![2u8; 4096],
        "a kill keeps completed writes"
    );
    assert_eq!(
        after(Cut::Crash),
        vec![1u8; 4096],
        "a crash keeps only synced data"
    );
    let lossy = after(Cut::PowerLoss { seed: 9 });
    assert_eq!(lossy, after(Cut::PowerLoss { seed: 9 }), "seeded");
    assert!(lossy.iter().all(|&b| b == 1 || b == 2));
    let pages = after(Cut::PowerLoss4k { seed: 3 });
    assert_eq!(pages.len(), 4096);
}

/// Writes a file on the node's volume when its state drops. A crash drops it on the
/// simulation thread, outside the node, so it writes through the volume it captured.
struct WritesOnDrop {
    bell: Condvar,
    volume: FaultFs,
    path: &'static str,
}

impl Stepped for WritesOnDrop {
    fn step(&mut self) -> Step {
        Step::Idle(None)
    }
    fn doorbell(&self) -> &Condvar {
        &self.bell
    }
    fn run(self) {}
}

impl Drop for WritesOnDrop {
    fn drop(&mut self) {
        if let Ok(file) = self.volume.open(Path::new(self.path), OpenMode::CreateNew) {
            let _ = file.write_all_at(b"written after the cut", 0);
        }
    }
}

struct TaskWritesOnDrop(&'static str);

impl Drop for TaskWritesOnDrop {
    fn drop(&mut self) {
        let _ = os::write(self.0, b"written after the cut");
    }
}

/// P2: the crash cuts the disks before any destructor runs, so nothing a dying service
/// or task writes reaches the image a restart sees.
#[test]
fn no_mutating_call_reaches_a_volume_after_a_crash() {
    run(5, |world| async move {
        node_with(&world, 1, &["/sim/n1/data"]);
        let volume = world.volume(1, Path::new("/sim/n1/data")).unwrap();
        in_node(1, async {
            task::spawn_stepped(
                "writer",
                64 * 1024,
                WritesOnDrop {
                    bell: Condvar::new(),
                    volume: volume.clone(),
                    path: "/sim/n1/data/service",
                },
            )
            .unwrap();
        })
        .await;
        let guard = TaskWritesOnDrop("/sim/n1/data/task");
        let task = world.spawn_node(1, async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        let before = volume.mutating_ops();
        world.crash(1);
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            volume.mutating_ops(),
            before,
            "a destructor wrote after the cut"
        );
        world.restart_with(1, Cut::Kill);
        on(1, || {
            assert!(!os::try_exists("/sim/n1/data/service").unwrap());
            assert!(!os::try_exists("/sim/n1/data/task").unwrap());
        })
        .await;
    });
}

#[test]
fn immediate_restart_fences_a_cancelled_tasks_destructor() {
    run(18, |world| async move {
        node_with(&world, 1, &["/sim/n1/data"]);
        let path = "/sim/n1/data/late";
        let (started, ready) = tokio::sync::oneshot::channel();
        let dying = world.spawn_node(1, async move {
            let _guard = TaskWritesOnDrop(path);
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .unwrap()
            .unwrap();

        // Aborting schedules cancellation. Restart installs the new image before
        // Tokio gets another poll to drop the aborted task.
        world.restart_with(1, Cut::Kill);
        assert!(!dying.is_finished());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), dying)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        on(1, || assert!(!os::try_exists(path).unwrap())).await;
    });
}

/// P5 and the RV-REP #3 precondition (O17): a data volume wiped after its identity
/// committed, next to a surviving key volume.
#[test]
fn a_wipe_replaces_one_volume_and_keeps_the_others() {
    run(6, |world| async move {
        let infos = node_with(&world, 1, &["/sim/n1/keys", "/sim/n1/data"]);
        let (anchor, open) = on(1, || {
            os::write("/sim/n1/keys/signing", b"key").unwrap();
            os::create_dir("/sim/n1/data/pool").unwrap();
            os::write("/sim/n1/data/pool/identity", b"id").unwrap();
            let meta = os::metadata("/sim/n1/data/pool").unwrap();
            let open = os::File::open("/sim/n1/data/pool/identity").unwrap();
            ((meta.dev(), meta.ino()), open)
        })
        .await;
        world.wipe(1, Path::new("/sim/n1/data"), Wipe::Reformat);
        let after = world.volume_info(1, Path::new("/sim/n1/data")).unwrap();
        let old = &infos[1];
        assert_eq!(
            (after.dev, after.major, after.minor),
            (old.dev, old.major, old.minor)
        );
        assert_ne!(after.filesystem_uuid, old.filesystem_uuid);
        assert_ne!(after.mount_id, old.mount_id);
        // Files opened before the wipe fail.
        assert!(open.sync_all().is_err());
        let (key, identity, recreated) = on(1, || {
            os::create_dir("/sim/n1/data/pool").unwrap();
            let meta = os::metadata("/sim/n1/data/pool").unwrap();
            (
                os::read("/sim/n1/keys/signing").unwrap(),
                os::try_exists("/sim/n1/data/pool/identity").unwrap(),
                (meta.dev(), meta.ino()),
            )
        })
        .await;
        assert_eq!(key, b"key", "the key volume survives");
        assert!(!identity, "the data volume is empty");
        assert_eq!(recreated.0, anchor.0, "a reformat keeps the device");
        assert_ne!(
            recreated.1, anchor.1,
            "a reformat must not reuse inode numbers (P5)"
        );
        world.wipe(1, Path::new("/sim/n1/data"), Wipe::Replace);
        let replaced = world.volume_info(1, Path::new("/sim/n1/data")).unwrap();
        assert_ne!(replaced.dev, after.dev);
        assert_eq!(world.mounts(1), vec![p("/sim/n1/keys"), p("/sim/n1/data")]);
    });
}

#[test]
fn crash_volume_after_freezes_the_volume_at_a_boundary() {
    let surviving = |ops: u64| {
        run(7, move |world| async move {
            node_with(&world, 1, &["/sim/n1/data"]);
            world.crash_volume_after(1, Path::new("/sim/n1/data"), ops);
            on(1, || {
                for name in ["a", "b", "c"] {
                    let path = format!("/sim/n1/data/{name}");
                    if os::write(&path, b"x").is_err() || os::sync_dir("/sim/n1/data").is_err() {
                        break;
                    }
                }
            })
            .await;
            world.restart_with(1, Cut::Crash);
            on(1, || {
                ["a", "b", "c"]
                    .iter()
                    .filter(|name| os::try_exists(format!("/sim/n1/data/{name}")).unwrap())
                    .count()
            })
            .await
        })
    };
    // Each file is create, write, sync_dir: three mutating operations.
    assert_eq!(surviving(0), 0);
    assert_eq!(surviving(2), 0);
    assert_eq!(surviving(3), 1);
    assert_eq!(surviving(9), 3);
}

#[test]
fn volume_calls_cuts_and_wipes_are_observed_in_order() {
    let trace = |seed| {
        run(seed, |world| async move {
            let events = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&events);
            world.observe(Arc::new(move |event: &SimEvent<'_>| {
                let line = match event {
                    SimEvent::Fs { node, mount, call } => {
                        format!("fs {node} {} {:?} {}", mount.display(), call.op, call.ok)
                    }
                    other => format!("{other:?}"),
                };
                sink.lock().unwrap().push(line);
            }));
            node_with(&world, 1, &["/sim/n1/data"]);
            on(1, || os::write("/sim/n1/data/f", b"x").unwrap()).await;
            world.restart_with(1, Cut::PowerLoss { seed });
            on(1, || os::try_exists("/sim/n1/data/f").unwrap()).await;
            world.wipe(1, Path::new("/sim/n1/data"), Wipe::Reformat);
            events.lock().unwrap().clone()
        })
    };
    let first = trace(11);
    assert_eq!(first, trace(11));
    for expected in [
        "fs 1 /sim/n1/data Open true",
        "fs 1 /sim/n1/data Write true",
        "Crash { node: 1 }",
        "Cut { node: 1, cut: PowerLoss { seed: 11 } }",
        "Restart { node: 1 }",
        "fs 1 /sim/n1/data Exists true",
    ] {
        assert!(
            first.iter().any(|line| line == expected),
            "{expected}: {first:#?}"
        );
    }
    assert!(first.last().unwrap().starts_with("Wipe { node: 1"));
}

#[test]
fn statvfs_reports_the_volume_device_and_space() {
    run(8, |world| async move {
        let info = node_with(&world, 1, &["/sim/n1/data"]).remove(0);
        let space = on(1, || {
            let file = os::File::create("/sim/n1/data/f").unwrap();
            file.write_all_at(&[0; 10_000], 0).unwrap();
            file.statvfs().unwrap()
        })
        .await;
        assert_eq!(space.fsid, info.dev);
        assert_eq!(space.blocks, CAPACITY_BYTES / 4096);
        assert_eq!(space.blocks - space.available, 3);
    });
}
