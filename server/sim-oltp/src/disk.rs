//! One mounted FaultFs image per server process.

use std::{path::Path, sync::Arc};

use nabedra_rt::{
    fs::FaultFs,
    sim::{Cut, Wipe, World},
};

use crate::{bootstrap::ROOT, invariants::Invariants};

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
