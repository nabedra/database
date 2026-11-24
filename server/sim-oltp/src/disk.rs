//! One mounted FaultFs image per server process, and an opt-in second one for its keys.

use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use nabedra_rt::{
    fs::{FaultFs, Fs, FsFile, FsKind, OpenMode},
    sim::{Cut, Wipe, World},
};

use crate::{
    bootstrap::{KEYS, ROOT},
    invariants::Invariants,
};

#[derive(Clone, Copy, Debug)]
pub enum DiskCut {
    /// Keep unsynced bytes, as when the OS survives a process kill.
    Kill,
    /// Preserve the durable image and hold the node's network links.
    Power,
    /// Replace the volume with a blank device before opening a new process.
    Wipe,
}

pub(crate) fn mount(world: &World, invariants: &Invariants, node: u64) {
    invariants.track_volume(node, world.mount(node, ROOT));
}

/// World cuts the network, halts the volume and aborts the node's tasks
/// before the harness releases the process handle.
pub(crate) fn cut(world: &Arc<World>, node: u64, how: DiskCut) -> FaultFs {
    let image = world.volume(node, Path::new(ROOT)).expect("mounted node");
    match how {
        DiskCut::Kill => world.crash(node),
        DiskCut::Power | DiskCut::Wipe => world.power_off(node),
    }
    image
}

pub(crate) fn reopen(world: &Arc<World>, checks: &Invariants, node: u64, how: DiskCut) {
    match how {
        DiskCut::Kill => world.restart_with(node, Cut::Kill),
        DiskCut::Power => world.restart_with(node, Cut::Crash),
        DiskCut::Wipe => {
            world.wipe(node, Path::new(ROOT), Wipe::Replace);
            let info = world
                .volume_info(node, Path::new(ROOT))
                .expect("replaced mount");
            checks.track_volume(node, info);
            world.restart(node);
        }
    }
}

/// What a worker's data volume holds when the worker starts again under the same identity.
#[derive(Clone, Copy)]
pub(crate) enum DataVolume<'a> {
    /// Nothing: the volume was replaced, or formatted again.
    Empty(Wipe),
    /// An earlier durable state of the same volume (a storage snapshot that was put back).
    Stale(&'a FaultFs),
}

/// Opt in to a key volume apart from the data volume of `node`.
pub(crate) fn mount_keys(world: &World, node: u64) {
    world.mount(node, KEYS);
}

/// The durable state of `node`'s data volume now, as a storage snapshot of a running worker
/// takes it.
pub(crate) fn snapshot(world: &World, node: u64) -> FaultFs {
    world
        .volume(node, Path::new(ROOT))
        .expect("mounted node")
        .crash()
}

/// Start `node` again after `how` with only its data volume lost or rolled back. Every other
/// volume of the node, such as its keys, holds what `how` left there.
pub(crate) fn reopen_data(
    world: &Arc<World>,
    checks: &Invariants,
    node: u64,
    how: DiskCut,
    data: DataVolume<'_>,
) {
    assert!(!world.is_up(node), "n{node} still runs");
    let cut = match how {
        DiskCut::Kill => Cut::Kill,
        DiskCut::Power => Cut::Crash,
        DiskCut::Wipe => panic!("name the lost volume with DataVolume::Empty"),
    };
    match data {
        DataVolume::Empty(wipe) => {
            // The volume is lost while the node is down. The restart then opens what is left.
            world.wipe(node, Path::new(ROOT), wipe);
            let info = world
                .volume_info(node, Path::new(ROOT))
                .expect("replaced mount");
            checks.track_volume(node, info);
            world.restart_with(node, cut);
        }
        DataVolume::Stale(image) => {
            world.restart_with(node, cut);
            let live = world.volume(node, Path::new(ROOT)).expect("mounted node");
            roll_back(&live, image, Path::new(ROOT)).expect("roll the data volume back");
        }
    }
}

/// Make the durable contents of `live` under `root` equal to those of `old`. Names that exist
/// in both keep their inode, as in a block-level restore, so directory and lock identities
/// that a process retained still match.
fn roll_back(live: &FaultFs, old: &FaultFs, root: &Path) -> io::Result<()> {
    let (live_dirs, live_files) = tree(live, root)?;
    let (old_dirs, old_files) = tree(old, root)?;
    for file in live_files.difference(&old_files) {
        live.remove(file)?;
    }
    // A child sorts after its parent.
    for dir in live_dirs.iter().rev() {
        if !old_dirs.contains(dir) {
            live.remove_dir(dir)?;
        }
    }
    for dir in old_dirs.difference(&live_dirs) {
        live.create_dir(dir)?;
    }
    for dir in &old_dirs {
        live.set_mode(dir, old.stat(dir)?.mode)?;
    }
    for path in &old_files {
        let mode = old.stat(path)?.mode;
        let file = if live_files.contains(path) {
            live.open_file(path, OpenMode::ReadWrite, None)?
        } else {
            live.open_file(path, OpenMode::CreateNew, Some(mode))?
        };
        let bytes = old.contents(path)?;
        file.set_len(0)?;
        file.write_all_at(&bytes, 0)?;
        file.sync_all()?;
        live.set_mode(path, mode)?;
    }
    for dir in old_dirs.iter().map(PathBuf::as_path).chain([root]) {
        live.sync_dir(dir)?;
    }
    Ok(())
}

/// The directories and the files below `root`.
fn tree(fs: &FaultFs, root: &Path) -> io::Result<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)> {
    let (mut dirs, mut files) = (BTreeSet::new(), BTreeSet::new());
    let mut next = vec![root.to_path_buf()];
    while let Some(dir) = next.pop() {
        for path in fs.list(&dir)? {
            if fs.stat(&path)?.kind == FsKind::Dir {
                next.push(path.clone());
                dirs.insert(path);
            } else {
                files.insert(path);
            }
        }
    }
    Ok((dirs, files))
}

#[cfg(test)]
mod tests {
    use nabedra_rt::{
        fs::os,
        sim::{NodeClock, in_node},
    };

    use super::*;

    const NODE: u64 = 1;

    async fn written(path: &str, bytes: &[u8]) {
        in_node(NODE, async {
            os::write(path, bytes).unwrap();
            os::File::open(path).unwrap().sync_all().unwrap();
            os::sync_dir(Path::new(path).parent().unwrap()).unwrap();
        })
        .await;
    }

    async fn contents(path: &str) -> Option<Vec<u8>> {
        in_node(NODE, async { os::read(path).ok() }).await
    }

    fn setup(world: &Arc<World>) -> Invariants {
        world.add_node(NODE, NodeClock::default());
        let checks = Invariants::new();
        mount(world, &checks, NODE);
        mount_keys(world, NODE);
        checks
    }

    fn observed(world: &Arc<World>) -> Arc<std::sync::Mutex<Vec<String>>> {
        let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = trace.clone();
        world.observe(Arc::new(move |event| {
            seen.lock().unwrap().push(format!("{event:?}"));
        }));
        trace
    }

    /// The RV-REP shape of design 3.4: the data volume is lost and the keys are not.
    #[test]
    fn an_empty_data_volume_keeps_the_key_volume() {
        for (wipe, cut) in [
            (Wipe::Replace, DiskCut::Kill),
            (Wipe::Replace, DiskCut::Power),
            (Wipe::Reformat, DiskCut::Kill),
        ] {
            let replay = || {
                nabedra_sim::runtime::run(1, |world| async move {
                    let checks = setup(&world);
                    let trace = observed(&world);
                    written("/sim-oltp/pool", b"votes").await;
                    written("/sim-oltp-keys/node.key", b"key").await;
                    let old = world.volume_info(NODE, Path::new(ROOT)).unwrap();
                    let keys = world.volume_info(NODE, Path::new(KEYS)).unwrap();
                    let image = cut_for_test(&world, cut);
                    reopen_data(&world, &checks, NODE, cut, DataVolume::Empty(wipe));
                    assert_eq!(contents("/sim-oltp/pool").await, None);
                    assert_eq!(
                        contents("/sim-oltp-keys/node.key").await.as_deref(),
                        Some(&b"key"[..])
                    );
                    let new = world.volume_info(NODE, Path::new(ROOT)).unwrap();
                    assert_ne!(new.filesystem_uuid, old.filesystem_uuid);
                    assert_eq!(new.dev == old.dev, wipe == Wipe::Reformat);
                    assert_eq!(
                        world.volume_info(NODE, Path::new(KEYS)).unwrap(),
                        keys,
                        "the key volume keeps its identity"
                    );
                    assert!(image.is_crashed(), "the lost volume takes no more writes");
                    // The node writes to the new volume.
                    written("/sim-oltp/pool", b"new").await;
                    checks.step(&world);
                    assert_eq!(checks.violations(), [""; 0]);
                    trace.lock().unwrap().clone()
                })
            };
            let first = replay();
            assert!(!first.is_empty());
            assert_eq!(replay(), first, "{wipe:?} {cut:?}");
        }
    }

    /// With `mkfs` inodes, a formatted volume repeats the `(dev, ino)` of its root. Only the
    /// filesystem uuid tells it from the old one.
    #[test]
    fn a_formatted_data_volume_repeats_its_inodes() {
        nabedra_sim::runtime::run(1, |world| async move {
            world.set_mkfs_inodes(true);
            let checks = setup(&world);
            let root = |world: &World| {
                let volume = world.volume(NODE, Path::new(ROOT)).unwrap();
                let stat = volume.stat(Path::new(ROOT)).unwrap();
                (stat.dev, stat.ino)
            };
            let before = root(&world);
            let old = world.volume_info(NODE, Path::new(ROOT)).unwrap();
            cut_for_test(&world, DiskCut::Kill);
            reopen_data(
                &world,
                &checks,
                NODE,
                DiskCut::Kill,
                DataVolume::Empty(Wipe::Reformat),
            );
            assert_eq!(root(&world), before);
            let new = world.volume_info(NODE, Path::new(ROOT)).unwrap();
            assert_ne!(new.filesystem_uuid, old.filesystem_uuid);
        });
    }

    /// A stale volume holds an earlier durable state, keeps its identity, and keeps the
    /// inodes that a process may have retained.
    #[test]
    fn a_stale_data_volume_holds_the_earlier_durable_state() {
        let replay = || {
            nabedra_sim::runtime::run(1, |world| async move {
                let checks = setup(&world);
                let trace = observed(&world);
                written("/sim-oltp/lock", b"").await;
                written("/sim-oltp/log", b"one").await;
                in_node(NODE, async {
                    os::create_dir_all("/sim-oltp/run").unwrap();
                    os::sync_dir(Path::new(ROOT)).unwrap();
                })
                .await;
                written("/sim-oltp/run/1", b"old run").await;
                // Written, but not durable when the snapshot is taken.
                in_node(NODE, async { os::write("/sim-oltp/tail", b"tail").unwrap() }).await;
                let image = snapshot(&world, NODE);
                written("/sim-oltp/log", b"one two").await;
                written("/sim-oltp/new", b"new").await;
                in_node(NODE, async { os::remove_dir_all("/sim-oltp/run").unwrap() }).await;
                written("/sim-oltp-keys/node.key", b"rotated").await;
                let live = world.volume(NODE, Path::new(ROOT)).unwrap();
                let inode = |fs: &FaultFs, path: &str| fs.stat(Path::new(path)).unwrap().ino;
                let before = [ROOT, "/sim-oltp/lock", "/sim-oltp/log"].map(|p| inode(&live, p));
                let identity = world.volume_info(NODE, Path::new(ROOT)).unwrap();

                cut_for_test(&world, DiskCut::Kill);
                reopen_data(
                    &world,
                    &checks,
                    NODE,
                    DiskCut::Kill,
                    DataVolume::Stale(&image),
                );
                assert_eq!(
                    contents("/sim-oltp/log").await.as_deref(),
                    Some(&b"one"[..])
                );
                assert_eq!(
                    contents("/sim-oltp/run/1").await.as_deref(),
                    Some(&b"old run"[..])
                );
                assert_eq!(contents("/sim-oltp/new").await, None);
                assert_eq!(contents("/sim-oltp/tail").await, None);
                assert_eq!(
                    contents("/sim-oltp-keys/node.key").await.as_deref(),
                    Some(&b"rotated"[..]),
                    "only the data volume is rolled back"
                );
                let live = world.volume(NODE, Path::new(ROOT)).unwrap();
                let after = [ROOT, "/sim-oltp/lock", "/sim-oltp/log"].map(|p| inode(&live, p));
                assert_eq!(after, before);
                assert_eq!(
                    world.volume_info(NODE, Path::new(ROOT)).unwrap(),
                    identity
                );
                // The restored state is durable: a power loss now keeps it.
                cut_for_test(&world, DiskCut::Power);
                world.restart_with(NODE, Cut::Crash);
                assert_eq!(
                    contents("/sim-oltp/log").await.as_deref(),
                    Some(&b"one"[..])
                );
                assert_eq!(contents("/sim-oltp/new").await, None);
                checks.step(&world);
                assert_eq!(checks.violations(), [""; 0]);
                trace.lock().unwrap().clone()
            })
        };
        let first = replay();
        assert!(!first.is_empty());
        assert_eq!(replay(), first);
    }

    #[test]
    #[should_panic(expected = "still runs")]
    fn a_live_data_volume_is_never_replaced() {
        nabedra_sim::runtime::run(1, |world| async move {
            let checks = setup(&world);
            reopen_data(
                &world,
                &checks,
                NODE,
                DiskCut::Kill,
                DataVolume::Empty(Wipe::Replace),
            );
        });
    }

    fn cut_for_test(world: &Arc<World>, how: DiskCut) -> FaultFs {
        super::cut(world, NODE, how)
    }
}
