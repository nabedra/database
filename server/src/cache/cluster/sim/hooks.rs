use std::collections::BTreeMap;
use std::io;

use bytes::Bytes;
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

use super::super::{
    controller::StagedPromotion,
    protocol::{Lease, Topology, WorkerId},
};
use crate::cache::store::{Effect, Effects};

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::cache::cluster) struct Knobs {
    pub lease_ignores_error_bound: bool,
    // The replica discards every DEL effect. It does not order a SET after a
    // DEL, so it stands in for P2 only in its result.
    pub replica_drops_deletes: bool,
    // A fault, not a defect: the replica receives the SET of an expiring key
    // a second time, right after the primary's DEL of that key.
    pub redeliver_set_after_expiry: bool,
    // P2: with the redelivery, the replica applies the SET after the DEL.
    pub replica_accepts_stale_delta: bool,
    pub owner_change_keeps_epoch: bool,
    pub cold_reset_ignores_live_lease: bool,
    pub staged_ignores_fence: bool,
    pub staged_ignores_lag: bool,
    // Reads decision 6's text literally: a retired source counts as
    // fenced, so the target's grant starts at once instead of at the
    // source's last committed expiry (design review R-cache-14).
    pub staged_retire_is_fence: bool,
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
    Redelivered {
        worker: WorkerId,
        slot: u16,
        epoch: u64,
        sequence: u64,
        current: u64,
        applied: bool,
    },
    Promoted {
        slot: u16,
        epoch: u64,
        prefix_epoch: u64,
        prefix_sequence: u64,
    },
    Served {
        worker: WorkerId,
        slot: u16,
        epoch: u64,
        sequence: u64,
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
type Commit = dyn Fn(u64, u64, u64, u64, &Topology, &[(u16, StagedPromotion)]) + Send + Sync;

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
    staged: &[(u16, StagedPromotion)],
) {
    if let (Some(world), Some(node)) = (sim::world(), sim::current_node()) {
        world.with_ext::<Hooks, _>(|hooks| {
            (hooks.committed)(node, index, term, leader, topology, staged)
        });
    }
}

pub(in crate::cache::cluster) fn watched_slots() -> Vec<u16> {
    sim::world()
        .and_then(|world| {
            world.with_ext::<Hooks, _>(|hooks| hooks.watched.iter().copied().collect())
        })
        .unwrap_or_default()
}

/// One replicated delta, kept for a second delivery.
pub(in crate::cache::cluster) struct Stale {
    pub epoch: u64,
    pub sequence: u64,
    pub effects: Effects,
    pub digest: [u8; 32],
    // The primary gave the key a deadline, so its DEL can be a synthesized
    // one. An Effect does not say why its key was deleted.
    expires: bool,
}

/// The first SET of each live key of a watched slot, as the replica got it.
#[derive(Default)]
pub(in crate::cache::cluster) struct Redelivery(BTreeMap<(u16, Bytes), Stale>);

impl Redelivery {
    pub fn forget(&mut self, slot: u16) {
        self.0.retain(|(id, _), _| *id != slot);
    }

    /// Records an applied delta. Returns the SETs of the expiring keys it
    /// deletes, which the network now delivers again.
    pub fn applied(
        &mut self,
        slot: u16,
        epoch: u64,
        sequence: u64,
        effects: &Effects,
        digest: [u8; 32],
    ) -> Vec<Stale> {
        let mut again = Vec::new();
        if !watched(slot) || !knobs().redeliver_set_after_expiry {
            return again;
        }
        for effect in effects {
            let key = (slot, effect.key().clone());
            match effect {
                Effect::Upsert { deadline_ms, .. } => {
                    self.0
                        .entry(key)
                        .or_insert_with(|| Stale {
                            epoch,
                            sequence,
                            effects: effects.clone(),
                            digest,
                            expires: false,
                        })
                        .expires = deadline_ms.is_some();
                }
                Effect::Delete(_) => {
                    again.extend(self.0.remove(&key).filter(|stale| stale.expires));
                }
            }
        }
        again
    }
}
