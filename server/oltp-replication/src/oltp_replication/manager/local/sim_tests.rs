//! The worker data directory on a simulated volume (DUR-5).
use super::*;
use nabedra_rt::sim::{self, Cut, NodeClock, World};

const NODE: u64 = 1;
const DATA: &str = "/sim/n1/data";
/// Two levels below the mount root, so the open creates an intermediate directory too.
const WORKER: &str = "/sim/n1/data/workers/w1";
const SEED: u64 = 41;

fn mount(world: &World) {
    world.add_node(NODE, NodeClock::default());
    world.mount(NODE, DATA);
}

/// Locks the worker directory with the volume cut after `cut` mutating operations, kills
/// the node, locks it again and writes its topology (synced inside the directory), then
/// loses every unsynced write: the topology must survive. Returns the operations of the
/// first lock.
fn data_directory_cut(cut: Option<u64>) -> u64 {
    sim::run(SEED, |world| async move {
        mount(&world);
        let volume = world.volume(NODE, Path::new(DATA)).unwrap();
        let operations = sim::in_node(NODE, async {
            let start = volume.mutating_ops();
            if let Some(cut) = cut {
                world.crash_volume_after(NODE, Path::new(DATA), start + cut);
            }
            drop(lock_directory(Path::new(WORKER)));
            volume.mutating_ops() - start
        })
        .await;
        world.restart_with(NODE, Cut::Kill);
        let database = Text::try_from("db").unwrap();
        let created = sim::in_node(NODE, async {
            let owner = lock_directory(Path::new(WORKER))
                .unwrap_or_else(|error| panic!("retry after a kill at cut {cut:?}: {error:?}"));
            topology(&owner, &database).unwrap()
        })
        .await;
        world.restart_with(NODE, Cut::Crash);
        sim::in_node(NODE, async {
            let bytes = fs::read(Path::new(WORKER).join("NABEDRA_TYPED_WORKER.json"))
                .unwrap_or_else(|error| {
                    panic!("the worker directory was lost at cut {cut:?}: {error:?}")
                });
            let stored: LocalTopology = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(stored.cluster_id, created.cluster_id);
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
        operations
    })
}

/// DUR-5: a kill at every step of the first open of a worker directory, a retry, then a
/// power loss. Before the fix no open synced the directory in its parent.
#[test]
fn dur5_worker_directory_survives_a_kill_at_every_step_then_power_loss() {
    let total = data_directory_cut(None);
    assert!(total > 0, "the test must enumerate volume mutations");
    for cut in 0..total {
        assert_eq!(data_directory_cut(Some(cut)), cut);
    }
    eprintln!("worker directory: {total} cuts replayed");
}

/// A registered worker's directory is its mount root: the fix syncs nothing outside the
/// volume for it.
#[test]
fn dur5_mount_root_worker_directory_syncs_nothing_outside_the_volume() {
    sim::run(SEED, |world| async move {
        mount(&world);
        sim::in_node(NODE, async {
            drop(lock_directory(Path::new(DATA)).unwrap());
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
    });
}
