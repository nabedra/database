//! The filesystem backup repository on a simulated volume (DUR-2).
use super::*;
use crate::{config::WorkerResources, oltp::pool::SharedResourceConfig};
use nabedra_rt::sim::{self, Cut, NodeClock};

const NODE: u64 = 1;
const ROOT: &str = "/sim/n1/backups";
const SEED: u64 = 41;

fn shared() -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap()
}

fn open_root() -> File {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(ROOT)
        .unwrap()
}

const KEY: ArchiveKey = ArchiveKey {
    cluster_id: uuid::Uuid::from_u128(1),
    archive_id: uuid::Uuid::from_u128(2),
};

/// Puts a first object with the volume cut after `cut` mutating operations, kills the
/// node, puts a second object (which must be acknowledged), then loses every unsynced
/// write: the acknowledged object must read back. Returns the operations of the first put.
fn object_cut(cut: Option<u64>) -> u64 {
    sim::run(SEED, |world| async move {
        world.add_node(NODE, NodeClock::default());
        world.mount(NODE, ROOT);
        let volume = world.volume(NODE, Path::new(ROOT)).unwrap();
        let operations = sim::in_node(NODE, async {
            let shared = shared();
            let root = open_root();
            let start = volume.mutating_ops();
            if let Some(cut) = cut {
                world.crash_volume_after(NODE, Path::new(ROOT), start + cut);
            }
            let _ = put_native(&root, KEY, "part-0", shared.copy_bytes(b"abcd").unwrap());
            volume.mutating_ops() - start
        })
        .await;
        world.restart_with(NODE, Cut::Kill);
        sim::in_node(NODE, async {
            let shared = shared();
            put_native(
                &open_root(),
                KEY,
                "part-1",
                shared.copy_bytes(b"efgh").unwrap(),
            )
            .unwrap_or_else(|error| panic!("put after a kill at cut {cut:?}: {error:?}"));
        })
        .await;
        world.restart_with(NODE, Cut::Crash);
        sim::in_node(NODE, async {
            let shared = shared();
            let object =
                read_native(&open_root(), KEY, "part-1", 64, &shared).unwrap_or_else(|error| {
                    panic!("the acknowledged object was lost at cut {cut:?}: {error:?}")
                });
            assert_eq!(object.as_ref(), b"efgh");
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
        operations
    })
}

/// DUR-2: a kill at every step of the first put into an archive, a second put, then a
/// power loss. Before the fix the second put found the cluster or archive directory,
/// skipped its parent sync, and its acknowledged object was lost with the directory.
#[test]
fn dur2_acknowledged_object_survives_a_kill_at_every_step_then_power_loss() {
    let total = object_cut(None);
    assert!(total > 0, "the test must enumerate volume mutations");
    for cut in 0..total {
        assert_eq!(object_cut(Some(cut)), cut);
    }
    eprintln!("filesystem repository: {total} cuts replayed");
}
