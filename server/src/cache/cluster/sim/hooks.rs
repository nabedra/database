use std::io;

use nabedra_rt::sim;

use super::clock_bound::SimClockBound;

pub(in crate::cache::cluster) fn clock_bound() -> Option<io::Result<(u64, u64)>> {
    let world = sim::world()?;
    world.with_ext::<SimClockBound, _>(|bound| bound.sample(&world))
}

#[cfg(not(target_os = "linux"))]
pub(in crate::cache::cluster) fn has_clock_bound() -> bool {
    sim::world()
        .and_then(|world| world.with_ext::<SimClockBound, _>(|_| ()))
        .is_some()
}

use super::super::protocol::{Lease, Topology, WorkerId};

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::cache::cluster) struct Knobs {
    pub lease_ignores_error_bound: bool,
    pub replica_keeps_deleted_keys: bool,
    pub owner_change_keeps_epoch: bool,
    pub cold_reset_ignores_live_lease: bool,
}

pub(in crate::cache::cluster) enum Observed {
    Lease {
        worker: WorkerId,
        slot: u16,
        writable: bool,
        epoch: u64,
        lease: Option<Lease>,
    },
    Published {
        worker: WorkerId,
        generation: u64,
        slot: u16,
        epoch: u64,
        sequence: u64,
        digest: [u8; 20],
    },
    ReplicaApplied {
        worker: WorkerId,
        source: WorkerId,
        generation: u64,
        slot: u16,
        epoch: u64,
        sequence: u64,
        digest: [u8; 20],
    },
    Served {
        worker: WorkerId,
        slot: u16,
        epoch: u64,
    },
    Charged {
        worker: WorkerId,
        slot: u16,
        slot_bytes: usize,
        slot_limit: usize,
        actor_bytes: usize,
        actor_limit: usize,
    },
    Installed {
        worker: WorkerId,
        generation: u64,
        revision: u64,
    },
}

type Observe = dyn Fn(u64, Observed) + Send + Sync;
type Commit = dyn Fn(u64, u64, u64, u64, &Topology) + Send + Sync;

pub(super) struct Hooks {
    pub knobs: Knobs,
    pub watched: std::collections::BTreeSet<u16>,
    pub observe: Box<Observe>,
    pub committed: Box<Commit>,
}

pub(in crate::cache::cluster) fn knobs() -> Knobs {
    sim::world()
        .and_then(|world| world.with_ext::<Hooks, _>(|hooks| hooks.knobs))
        .unwrap_or_default()
}

pub(in crate::cache::cluster) fn watched(slot: u16) -> bool {
    sim::world()
        .and_then(|world| world.with_ext::<Hooks, _>(|hooks| hooks.watched.contains(&slot)))
        .unwrap_or_default()
}

pub(in crate::cache::cluster) fn observe(event: Observed) {
    if let (Some(world), Some(node)) = (sim::world(), sim::current_node()) {
        world.with_ext::<Hooks, _>(|hooks| (hooks.observe)(node, event));
    }
}

pub(in crate::cache::cluster) fn committed(
    index: u64,
    term: u64,
    leader: u64,
    topology: &Topology,
) {
    if let (Some(world), Some(node)) = (sim::world(), sim::current_node()) {
        world.with_ext::<Hooks, _>(|hooks| (hooks.committed)(node, index, term, leader, topology));
    }
}

pub(in crate::cache::cluster) fn watched_slots() -> Vec<u16> {
    sim::world()
        .and_then(|world| {
            world.with_ext::<Hooks, _>(|hooks| hooks.watched.iter().copied().collect())
        })
        .unwrap_or_default()
}
