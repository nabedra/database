//! The native backup destination on a simulated volume (DUR-4).
use super::*;
use nabedra_rt::sim::{self, Cut, NodeClock};

const NODE: u64 = 1;
const ROOT: &str = "/sim/n1/backups";
const DESTINATION: &str = "/sim/n1/backups/archive";
const SEED: u64 = 41;

/// Creates the destination with the volume cut after `cut` mutating operations, kills the
/// node, ensures it again and writes the locator, then loses every unsynced write: the
/// locator must read back. Returns the operations of the first call.
fn destination_cut(cut: Option<u64>) -> u64 {
    sim::run(SEED, |world| async move {
        world.add_node(NODE, NodeClock::default());
        world.mount(NODE, ROOT);
        let volume = world.volume(NODE, Path::new(ROOT)).unwrap();
        let destination = Path::new(DESTINATION);
        let operations = sim::in_node(NODE, async {
            let start = volume.mutating_ops();
            if let Some(cut) = cut {
                world.crash_volume_after(NODE, Path::new(ROOT), start + cut);
            }
            let _ = ensure_private_directory(destination);
            volume.mutating_ops() - start
        })
        .await;
        world.restart_with(NODE, Cut::Kill);
        sim::in_node(NODE, async {
            ensure_private_directory(destination)
                .unwrap_or_else(|error| panic!("retry after a kill at cut {cut:?}: {error:?}"));
            write_local_record(destination, "manifest.json", b"{}", 64).unwrap();
        })
        .await;
        world.restart_with(NODE, Cut::Crash);
        sim::in_node(NODE, async {
            let record =
                read_local_record(&destination.join("manifest.json"), 64).unwrap_or_else(|error| {
                    panic!("the backup destination was lost at cut {cut:?}: {error:?}")
                });
            assert_eq!(record, b"{}");
        })
        .await;
        assert_eq!(world.unmounted_path_errors(), 0);
        operations
    })
}

/// DUR-4: a kill at every step of creating the destination, a retry and a synced locator,
/// then a power loss. Before the fix no call synced the destination in its parent.
#[test]
fn dur4_backup_destination_survives_a_kill_at_every_step_then_power_loss() {
    let total = destination_cut(None);
    assert!(total > 0, "the test must enumerate volume mutations");
    for cut in 0..total {
        assert_eq!(destination_cut(Some(cut)), cut);
    }
    eprintln!("backup destination: {total} cuts replayed");
}
