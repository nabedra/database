use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::clock::ClockBounds;
use super::super::protocol::{
    Heartbeat, Lease, Move, Progress, ReplicaSeed, Slot, Topology, WorkerId,
};
use super::state::State;

// Bound the number of immutable slot transfers started on each worker at once.
pub(in crate::cache::cluster) const MAX_MOVES_PER_WORKER: usize = 64;
const MAX_CHANGES: usize = 256;
const REPORT_AGE_MS: u64 = 3_000;
pub(in crate::cache::cluster) const MAX_CUTOVER_LAG: u64 = 32;
// Give healthy candidates a full report window after the last lease fence.
// This also covers fresh observations after a controller leadership change.
pub(in crate::cache::cluster) const STAGED_RESET_GRACE_MS: u64 = REPORT_AGE_MS;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Order {
    pub id: Uuid,
    pub first: u16,
    pub last: u16,
    pub target: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Change {
    Slot {
        index: u16,
        expected: Slot,
        next: Slot,
        evidence: Option<Progress>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        staged: Option<StagedPromotion>,
    },
    Drained(u64),
    OrderDone(Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::cache::cluster) enum SourceFence {
    Retired,
    LeaseExpired,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(in crate::cache::cluster) struct StagedPromotion {
    pub target: WorkerId,
    pub retained_epoch: u64,
    pub retained_sequence: u64,
    pub target_revision: u64,
    pub source: WorkerId,
    pub source_sequence: u64,
    pub prior_lease: Option<Lease>,
    pub fence: SourceFence,
}

impl StagedPromotion {
    fn cutoff(self, lower_ms: u64) -> u64 {
        #[cfg(feature = "sim")]
        if super::super::sim::hooks::knobs().staged_ignores_fence {
            return lower_ms;
        }
        let _ = lower_ms;
        self.prior_lease.map_or(0, |lease| lease.expires_ms)
    }

    fn within_lag(self) -> bool {
        #[cfg(feature = "sim")]
        if super::super::sim::hooks::knobs().staged_ignores_lag {
            return true;
        }
        self.source_sequence.saturating_sub(self.retained_sequence) <= MAX_CUTOVER_LAG
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Staging {
    pub revision: u64,
    pub source_sequence: Option<u64>,
}

#[derive(Default)]
pub(super) struct Observations {
    term: u64,
    generation: u64,
    reports: BTreeMap<WorkerId, Report>,
}

struct Report {
    revision: u64,
    received_ms: u64,
    slots: HashMap<u16, Progress>,
}

impl Observations {
    pub fn reset_if_changed(&mut self, term: u64, generation: u64) {
        if self.term != term || self.generation != generation {
            self.term = term;
            self.generation = generation;
            self.reports.clear();
        }
    }

    pub fn record(
        &mut self,
        topo: &Topology,
        heartbeat: Heartbeat,
        received_ms: u64,
    ) -> Vec<Progress> {
        if heartbeat.generation != topo.generation {
            return Vec::new();
        }
        self.reports.retain(|id, report| {
            topo.member(*id).is_some()
                && received_ms < report.received_ms.saturating_add(REPORT_AGE_MS)
        });
        let worker = heartbeat.worker;
        let mut staged_progress = Vec::new();
        let slots = heartbeat
            .progress
            .into_iter()
            .filter_map(|report| {
                let slot = topo.slots.get(usize::from(report.slot))?;
                if report.epoch != slot.epoch {
                    return None;
                }
                if slot.primary == Some(worker) {
                    if report.baseline && slot.moving.is_some_and(|moving| moving.replica_only) {
                        staged_progress.push(report);
                    }
                } else if slot.replica != Some(worker)
                    && slot.moving.is_none_or(|moving| moving.target != worker)
                {
                    return None;
                }
                Some((report.slot, report))
            })
            .collect();
        self.reports.insert(
            worker,
            Report {
                revision: heartbeat.revision,
                received_ms,
                slots,
            },
        );
        staged_progress
    }

    fn live(&self, worker: WorkerId, now: ClockBounds) -> bool {
        self.reports
            .get(&worker)
            .is_some_and(|report| now.upper_ms < report.received_ms.saturating_add(REPORT_AGE_MS))
    }

    fn baseline(
        &self,
        worker: Option<WorkerId>,
        slot: u16,
        epoch: u64,
        now: ClockBounds,
    ) -> Option<Progress> {
        let worker = worker?;
        if !self.live(worker, now) {
            return None;
        }
        self.reports
            .get(&worker)?
            .slots
            .get(&slot)
            .copied()
            .filter(|value| value.epoch == epoch && value.baseline)
    }

    pub fn ready(&self, topo: &Topology, now: ClockBounds) -> (usize, usize) {
        let mut replicas = 0;
        let mut primaries = 0;
        for (index, slot) in topo.slots.iter().enumerate() {
            let index = index as u16;
            if slot.primary.and_then(|id| topo.member(id)).is_some()
                && self
                    .baseline(slot.primary, index, slot.epoch, now)
                    .is_some_and(|report| report.serving_primary)
                && slot.lease.is_some_and(|lease| {
                    now.allows(lease.expires_ms) && now.fenced(lease.not_before_ms)
                })
                && slot.moving.is_none_or(|moving| !moving.freeze_requested)
            {
                primaries += 1;
            }
            if slot.replica.and_then(|id| topo.member(id)).is_some()
                && self
                    .baseline(slot.replica, index, slot.epoch, now)
                    .is_some()
            {
                replicas += 1;
            }
        }
        (primaries, replicas)
    }

    pub fn plan(&self, state: &State, now: ClockBounds) -> Vec<Change> {
        let topo = &state.topology;
        let mut placement = Placement::new(topo, self, now);
        let mut changes = Vec::new();
        let mut staged_recovery = Vec::new();
        for (index, slot) in topo.slots.iter().copied().enumerate() {
            let index = index as u16;
            if let Some(change) = placement.recover_staged(index, slot, state) {
                staged_recovery.push(change);
                if staged_recovery.len() == MAX_CHANGES {
                    return staged_recovery;
                }
                continue;
            }
            if changes.len() < MAX_CHANGES
                && let Some(next) = placement.plan_slot(index, slot, state)
            {
                let advances_ownership = next.epoch != slot.epoch;
                #[cfg(feature = "sim")]
                let advances_ownership = advances_ownership || next.primary != slot.primary;
                let starts_staging =
                    slot.moving.is_none() && next.moving.is_some_and(|moving| moving.replica_only);
                let evidence = self.baseline(
                    if advances_ownership || starts_staging {
                        next.primary
                    } else {
                        next.replica
                    },
                    index,
                    slot.epoch,
                    now,
                );
                changes.push(Change::Slot {
                    index,
                    expected: slot,
                    next,
                    evidence,
                    staged: None,
                });
            }
        }
        if !staged_recovery.is_empty() {
            staged_recovery.extend(
                changes
                    .into_iter()
                    .take(MAX_CHANGES - staged_recovery.len()),
            );
            changes = staged_recovery;
        }
        if changes.len() == MAX_CHANGES {
            return changes;
        }

        for order in &state.orders {
            if topo.slots[usize::from(order.first)..=usize::from(order.last)]
                .iter()
                .all(|slot| {
                    slot.primary.is_some_and(|id| id.node == order.target) && slot.moving.is_none()
                })
            {
                changes.push(Change::OrderDone(order.id));
                if changes.len() == MAX_CHANGES {
                    return changes;
                }
            }
        }

        // Termination is safe only when the worker is unreferenced and the
        // surviving placement has a verified baseline for each assigned slot.
        for &worker in &topo.draining {
            if !topo.drained.contains(&worker)
                && topo.slots.iter().enumerate().all(|(index, slot)| {
                    let index = index as u16;
                    !names_node(*slot, worker)
                        && (!state.initialized()
                            || (self
                                .baseline(slot.primary, index, slot.epoch, now)
                                .is_some()
                                && self
                                    .baseline(slot.replica, index, slot.epoch, now)
                                    .is_some()))
                })
            {
                changes.push(Change::Drained(worker));
            }
        }
        changes
    }
}

impl State {
    pub(super) fn reconcile(&mut self, lower_ms: u64, changes: Vec<Change>) -> bool {
        let Ok(revision) = self.next_revision() else {
            return false;
        };
        if changes.is_empty() || changes.len() > MAX_CHANGES + super::super::protocol::MAX_WORKERS {
            return false;
        }
        let mut changed = false;
        for change in changes {
            match change {
                Change::Slot {
                    index,
                    expected,
                    next,
                    evidence,
                    staged,
                } => {
                    let Some(old) = self.topology.slots.get(usize::from(index)).copied() else {
                        continue;
                    };
                    // Compare the committed grant as well as the placement.
                    // A renewal that raced the planner invalidates its proof.
                    if old != expected {
                        continue;
                    }
                    let valid = match staged {
                        Some(proof) => {
                            self.valid_staged(index, lower_ms, old, next, evidence, proof)
                        }
                        None => valid_change(&self.topology, index, lower_ms, old, next, evidence),
                    };
                    if !valid {
                        continue;
                    }
                    if next.epoch != old.epoch || next.moving != old.moving {
                        self.staging.remove(&index);
                        if next.moving.is_some_and(|moving| moving.replica_only) {
                            let source_sequence = evidence
                                .filter(|source| {
                                    source.baseline
                                        && source.slot == index
                                        && source.epoch == old.epoch
                                })
                                .map(|source| source.sequence);
                            self.staging.insert(
                                index,
                                Staging {
                                    revision,
                                    source_sequence,
                                },
                            );
                        }
                    }
                    #[cfg(feature = "sim")]
                    if let Some(proof) = staged {
                        self.applied_staged.push((index, proof));
                    }
                    self.topology.slots[usize::from(index)] = next;
                    changed = true;
                }
                Change::Drained(worker) => {
                    if self.topology.draining.contains(&worker)
                        && self
                            .topology
                            .slots
                            .iter()
                            .all(|slot| !names_node(*slot, worker))
                    {
                        changed |= self.complete_drain(worker);
                    }
                }
                Change::OrderDone(id) => {
                    let len = self.orders.len();
                    self.orders.retain(|order| {
                        order.id != id
                            || !self.topology.slots
                                [usize::from(order.first)..=usize::from(order.last)]
                                .iter()
                                .all(|slot| {
                                    slot.primary.is_some_and(|id| id.node == order.target)
                                        && slot.moving.is_none()
                                })
                    });
                    changed |= len != self.orders.len();
                }
            }
        }
        if changed {
            self.topology.revision = revision;
        }
        changed
    }
}

impl State {
    fn valid_staged(
        &self,
        index: u16,
        lower_ms: u64,
        old: Slot,
        next: Slot,
        evidence: Option<Progress>,
        proof: StagedPromotion,
    ) -> bool {
        let topo = &self.topology;
        let cutoff = proof.cutoff(lower_ms);
        old.moving
            .is_some_and(|moving| moving.replica_only && moving.target == proof.target)
            && old.primary == Some(proof.source)
            && proof.target != proof.source
            && old.lease == proof.prior_lease
            && source_fenced(self, proof.source, old.lease, lower_ms, proof.fence)
            && self.staging.get(&index).is_some_and(|staging| {
                staging.source_sequence == Some(proof.source_sequence)
                    && (staging.revision..=topo.revision).contains(&proof.target_revision)
            })
            && proof.retained_epoch == old.epoch
            && proof.within_lag()
            && evidence.is_some_and(|copy| {
                copy.baseline
                    && copy.slot == index
                    && copy.epoch == proof.retained_epoch
                    && copy.sequence == proof.retained_sequence
            })
            && next.primary == Some(proof.target)
            && old.epoch.checked_add(1) == Some(next.epoch)
            && !next.cold
            && next.moving.is_none()
            && next.replica_seed.is_none()
            && next.lease == Some(fenced_lease(cutoff))
            && valid_members(topo, old, next)
    }
}

fn source_fenced(
    state: &State,
    source: WorkerId,
    lease: Option<Lease>,
    lower_ms: u64,
    fence: SourceFence,
) -> bool {
    #[cfg(feature = "sim")]
    if super::super::sim::hooks::knobs().staged_ignores_fence {
        return true;
    }
    match fence {
        SourceFence::Retired => state.is_retired(source),
        SourceFence::LeaseExpired => lease.is_none_or(|lease| lower_ms >= lease.expires_ms),
    }
}

fn staged_reset_at(lease: Option<Lease>) -> u64 {
    lease
        .map_or(0, |lease| lease.expires_ms)
        .saturating_add(STAGED_RESET_GRACE_MS)
}

fn fenced_lease(cutoff: u64) -> Lease {
    Lease {
        expires_ms: cutoff,
        not_before_ms: cutoff,
    }
}

fn valid_members(topo: &Topology, old: Slot, next: Slot) -> bool {
    let Some(primary) = next.primary.and_then(|id| topo.member(id)) else {
        return false;
    };
    // Enrollment retires the old incarnation before a replica-only move can
    // verify its replacement. Preserve that existing assignment while
    // staging or canceling the move. A new assignment must be enrolled.
    let replica_unchanged =
        next.primary == old.primary && next.epoch == old.epoch && next.replica == old.replica;
    next.replica
        .is_none_or(|replica| match topo.member(replica) {
            Some(member) => primary.zone != member.zone,
            None => replica_unchanged,
        })
}

fn valid_change(
    topo: &Topology,
    index: u16,
    lower_ms: u64,
    old: Slot,
    next: Slot,
    evidence: Option<Progress>,
) -> bool {
    if !valid_members(topo, old, next) {
        return false;
    }
    if next.cold && old.epoch.checked_add(1) == Some(next.epoch) {
        return old.moving.is_some_and(|moving| moving.replica_only)
            && lower_ms >= staged_reset_at(old.lease)
            && next == old.cold_reset(next.primary.unwrap(), next.replica);
    }
    let evidence =
        evidence.filter(|proof| proof.baseline && proof.slot == index && proof.epoch == old.epoch);
    if next.primary == old.primary && next.epoch == old.epoch {
        if next.lease != old.lease || next.cold != old.cold {
            return false;
        }
        if old.moving.is_some_and(|moving| moving.freeze_requested) && next.moving.is_none() {
            return false;
        }
        if next.replica == old.replica {
            return next.replica_seed == old.replica_seed;
        }
        return next.replica_seed.is_none()
            && evidence.is_some()
            && old
                .moving
                .is_some_and(|moving| moving.replica_only && Some(moving.target) == next.replica);
    }

    let epoch_advances = old.epoch.checked_add(1) == Some(next.epoch);
    #[cfg(feature = "sim")]
    let epoch_advances = epoch_advances
        || (old.primary != next.primary
            && old.epoch == next.epoch
            && super::super::sim::hooks::knobs().owner_change_keeps_epoch);
    if !epoch_advances || next.cold || next.moving.is_some() {
        return false;
    }
    let Some(proof) = evidence else {
        return false;
    };
    let old_deadline = old.lease.map_or(0, |lease| lease.expires_ms);
    let has_fence = |cutoff| lower_ms >= cutoff && next.lease == Some(fenced_lease(cutoff));
    if next.primary == old.replica && next.replica_seed.is_none() && has_fence(old_deadline) {
        return true;
    }
    let Some(moving) = old.moving else {
        return false;
    };
    let Some(frozen) = moving.frozen else {
        return false;
    };
    if proof.sequence != frozen.sequence || moving.replica_only {
        return false;
    }
    if next.primary == old.primary {
        return next.replica_seed.is_none()
            && proof.frozen == Some(frozen)
            && has_fence(old_deadline);
    }
    let cutoff = if frozen.lease_expires_ms == old_deadline {
        frozen.upper_ms
    } else {
        old_deadline
    };
    let Some(previous_primary) = old.primary else {
        return false;
    };
    next.primary == Some(moving.target)
        && has_fence(cutoff)
        && next.replica.is_some()
        && (next.replica == old.primary || next.replica == old.replica)
        && next.replica_seed
            == Some(ReplicaSeed {
                previous_primary,
                previous_epoch: old.epoch,
                sequence: frozen.sequence,
            })
}

struct Placement<'a> {
    topo: &'a Topology,
    observed: &'a Observations,
    now: ClockBounds,
    moves: BTreeMap<u64, usize>,
    primary_load: BTreeMap<u64, usize>,
    replica_load: BTreeMap<u64, usize>,
    drain_operations: BTreeSet<u64>,
}

impl<'a> Placement<'a> {
    fn new(topo: &'a Topology, observed: &'a Observations, now: ClockBounds) -> Self {
        let mut placement = Self {
            topo,
            observed,
            now,
            moves: BTreeMap::new(),
            primary_load: BTreeMap::new(),
            replica_load: BTreeMap::new(),
            drain_operations: topo.draining.difference(&topo.drained).copied().collect(),
        };
        for slot in &topo.slots {
            if let Some(worker) = slot.primary {
                *placement.primary_load.entry(worker.node).or_default() += 1;
                // A retired incarnation can no longer participate in copying.
                // Its stale intent cannot reserve capacity on a live worker.
                if let Some(moving) = slot.moving
                    && topo.member(worker).is_some()
                    && topo.member(moving.target).is_some()
                {
                    placement.add_move(worker.node, moving.target.node);
                }
            }
            if let Some(worker) = slot.replica {
                *placement.replica_load.entry(worker.node).or_default() += 1;
            }
        }
        placement
    }

    fn staged_copy(
        &self,
        index: u16,
        slot: Slot,
        target: WorkerId,
        state: &State,
    ) -> Option<(Progress, u64)> {
        let staging = state.staging.get(&index)?;
        let revision = self.observed.reports.get(&target)?.revision;
        // A heartbeat from before this transfer may name a copy that the
        // target has already discarded, even if the slot epoch is unchanged.
        if !(staging.revision..=self.topo.revision).contains(&revision) {
            return None;
        }
        let copy = self
            .observed
            .baseline(Some(target), index, slot.epoch, self.now)?;
        Some((copy, revision))
    }

    fn recover_staged(&mut self, index: u16, slot: Slot, state: &State) -> Option<Change> {
        let source = slot.primary?;
        let moving = slot.moving.filter(|moving| moving.replica_only)?;
        let retired = state.is_retired(source);
        let source_progress = self
            .observed
            .baseline(Some(source), index, slot.epoch, self.now);
        let unavailable = retired || source_progress.is_none();
        #[cfg(feature = "sim")]
        let unavailable = unavailable || super::super::sim::hooks::knobs().staged_ignores_fence;
        if !unavailable {
            return None;
        }

        // Prefer the existing failover path if the assigned replica is ready.
        if slot
            .replica
            .is_some_and(|replica| self.topo.member(replica).is_some())
            && self
                .observed
                .baseline(slot.replica, index, slot.epoch, self.now)
                .is_some()
        {
            return None;
        }

        let fence = if retired {
            SourceFence::Retired
        } else {
            SourceFence::LeaseExpired
        };
        if source_fenced(state, source, slot.lease, self.now.lower_ms, fence)
            && self.topo.member(moving.target).is_some()
            && let Some((copy, target_revision)) =
                self.staged_copy(index, slot, moving.target, state)
            && let Some(source_sequence) = state
                .staging
                .get(&index)
                .and_then(|staging| staging.source_sequence)
        {
            let proof = StagedPromotion {
                target: moving.target,
                retained_epoch: copy.epoch,
                retained_sequence: copy.sequence,
                target_revision,
                source,
                source_sequence,
                prior_lease: slot.lease,
                fence,
            };
            if proof.within_lag() {
                // Retirement prevents further renewals. An isolated old
                // incarnation can still spend a grant it already received.
                let cutoff = proof.cutoff(self.now.lower_ms);
                let next = self.switch_owner(slot, moving.target, cutoff, None)?;
                return Some(Change::Slot {
                    index,
                    expected: slot,
                    next,
                    evidence: Some(copy),
                    staged: Some(proof),
                });
            }
        }

        if !self.now.fenced(staged_reset_at(slot.lease)) {
            return None;
        }
        let primary = self
            .topo
            .workers
            .get(&source.node)
            .map(|member| member.id)
            .filter(|&id| self.eligible(id))
            .or_else(|| {
                self.topo
                    .workers
                    .values()
                    .map(|member| member.id)
                    .find(|&id| self.eligible(id))
            })?;
        let replica = self.replica_for(primary, None);
        slot.epoch.checked_add(1)?;
        Some(Change::Slot {
            index,
            expected: slot,
            next: slot.cold_reset(primary, replica),
            evidence: None,
            staged: None,
        })
    }

    fn plan_slot(&mut self, index: u16, slot: Slot, state: &State) -> Option<Slot> {
        let primary = slot.primary?;
        let source = self
            .observed
            .baseline(Some(primary), index, slot.epoch, self.now);
        let replica = self
            .observed
            .baseline(slot.replica, index, slot.epoch, self.now);
        let deadline = slot.lease.map_or(0, |lease| lease.expires_ms);
        let expired = self.now.fenced(deadline);

        if let Some(moving) = slot.moving {
            if moving.replica_only {
                if let (Some(source), Some((target, _))) =
                    (source, self.staged_copy(index, slot, moving.target, state))
                    && close_enough(source, target)
                {
                    return Some(Slot {
                        replica: Some(moving.target),
                        moving: None,
                        replica_seed: None,
                        ..slot
                    });
                }
            } else {
                let target =
                    self.observed
                        .baseline(Some(moving.target), index, slot.epoch, self.now);
                if let Some(step) = self.plan_move(slot, moving, source, target, replica, expired) {
                    return Some(step);
                }
            }
            // An unfrozen owner can keep serving while its replica is
            // repaired. Release a same-zone handoff whose third member
            // retired. Its order stays pending and can resume after repair.
            let cannot_keep_replica =
                !moving.replica_only && !self.has_replica_zone(moving.target, slot);
            if !moving.freeze_requested
                && (!self.eligible(moving.target) || cannot_keep_replica)
                && source.is_some()
            {
                return Some(Slot {
                    moving: None,
                    ..slot
                });
            }
            // A connected actor with an invalid clock can report its RAM
            // baseline but cannot acknowledge freeze. The committed lease
            // still fences it when the controller's lower bound expires.
            let unavailable_before_freeze = moving.freeze_requested && moving.frozen.is_none();
            if expired && (source.is_none() || unavailable_before_freeze) && replica.is_some() {
                return self.switch_owner(slot, slot.replica?, deadline, None);
            }
            // A move can stall after freezing. Resume the same verified source
            // under a fresh epoch once its latest committed lease is fenced.
            if expired && moving.frozen.is_some() && source.is_some() {
                return self.switch_owner(slot, primary, deadline, slot.replica);
            }
            return None;
        }

        if expired && source.is_none() && replica.is_some() {
            return self.switch_owner(slot, slot.replica?, deadline, None);
        }
        if source.is_none() {
            return None;
        }
        let replica_needs_replacement = slot.replica.is_none_or(|id| {
            self.topo.member(id).is_none()
                || self.drain_operations.contains(&id.node)
                || !self.observed.live(id, self.now)
        });
        if replica_needs_replacement
            && self.move_capacity(primary.node)
            && let Some(candidate) = self.replica_for(primary, slot.replica)
        {
            self.add_move(primary.node, candidate.node);
            *self.replica_load.entry(candidate.node).or_default() += 1;
            return Some(Slot {
                moving: Some(Move {
                    operation: Uuid::from_u128(u128::from(candidate.node)),
                    target: candidate,
                    freeze_requested: false,
                    frozen: None,
                    replica_only: true,
                }),
                ..slot
            });
        }

        let request = state
            .orders
            .iter()
            .find(|order| index >= order.first && index <= order.last)
            .map(|order| (order.id, order.target))
            .or_else(|| {
                if !self.drain_operations.contains(&primary.node) {
                    return None;
                }
                let target = self
                    .topo
                    .workers
                    .values()
                    .filter(|member| {
                        member.id != primary
                            && self.eligible(member.id)
                            && self.move_capacity(member.id.node)
                    })
                    .min_by_key(|member| {
                        self.primary_load.get(&member.id.node).copied().unwrap_or(0)
                    })?;
                Some((Uuid::from_u128(u128::from(primary.node)), target.id.node))
            });
        let (operation, target) = request?;
        let target = self.topo.workers.get(&target)?.id;
        if target == primary
            || !self.eligible(target)
            || !self.move_capacity(primary.node)
            || !self.move_capacity(target.node)
        {
            return None;
        }
        self.add_move(primary.node, target.node);
        *self.primary_load.entry(target.node).or_default() += 1;
        Some(Slot {
            moving: Some(Move {
                operation,
                target,
                freeze_requested: false,
                frozen: None,
                replica_only: false,
            }),
            ..slot
        })
    }

    fn plan_move(
        &mut self,
        slot: Slot,
        moving: Move,
        source: Option<Progress>,
        target: Option<Progress>,
        replica_progress: Option<Progress>,
        expired: bool,
    ) -> Option<Slot> {
        let deadline = slot.lease.map_or(0, |lease| lease.expires_ms);
        if moving.frozen.is_none()
            && let Some(frozen) = source.and_then(|progress| progress.frozen)
            && moving.freeze_requested
            && (frozen.lease_expires_ms == deadline || expired)
            && frozen.upper_ms <= self.now.upper_ms.saturating_add(10)
        {
            return Some(Slot {
                moving: Some(Move {
                    frozen: Some(frozen),
                    ..moving
                }),
                ..slot
            });
        }
        let target = target?;
        if let Some(frozen) = moving.frozen {
            let exact = frozen.lease_expires_ms == deadline;
            let cutoff = if exact { frozen.upper_ms } else { deadline };
            if target.sequence == frozen.sequence && self.now.fenced(cutoff) {
                let prior = slot
                    .primary
                    .filter(|&source| {
                        self.observed.live(source, self.now)
                            && self.distinct_zones(moving.target, source)
                    })
                    .or_else(|| {
                        let replica = slot.replica?;
                        (self.distinct_zones(moving.target, replica)
                            && replica_progress
                                .is_some_and(|replica| replica.sequence == frozen.sequence))
                        .then_some(replica)
                    });
                if let Some(replica) = prior {
                    let mut next = self.switch_owner(slot, moving.target, cutoff, Some(replica))?;
                    next.replica_seed = Some(ReplicaSeed {
                        previous_primary: slot.primary?,
                        previous_epoch: slot.epoch,
                        sequence: frozen.sequence,
                    });
                    return Some(next);
                }
            }
            return None;
        }
        let source = source?;
        if !moving.freeze_requested
            && close_enough(source, target)
            && self.has_replica_zone(moving.target, slot)
        {
            return Some(Slot {
                moving: Some(Move {
                    freeze_requested: true,
                    ..moving
                }),
                ..slot
            });
        }
        None
    }

    fn switch_owner(
        &mut self,
        slot: Slot,
        primary: WorkerId,
        cutoff: u64,
        previous: Option<WorkerId>,
    ) -> Option<Slot> {
        if self.topo.member(primary).is_none() {
            return None;
        }
        let replica = previous
            .filter(|id| self.observed.live(*id, self.now) && self.distinct_zones(primary, *id))
            .or_else(|| {
                slot.replica
                    .filter(|id| self.eligible(*id) && self.distinct_zones(primary, *id))
            })
            .or_else(|| self.replica_for(primary, None));
        *self.primary_load.entry(primary.node).or_default() += 1;
        if let Some(replica) = replica {
            *self.replica_load.entry(replica.node).or_default() += 1;
        }
        let epoch = slot.epoch.checked_add(1)?;
        #[cfg(feature = "sim")]
        let epoch = if slot.primary != Some(primary)
            && super::super::sim::hooks::knobs().owner_change_keeps_epoch
        {
            slot.epoch
        } else {
            epoch
        };
        Some(Slot {
            epoch,
            primary: Some(primary),
            replica,
            lease: Some(fenced_lease(cutoff)),
            moving: None,
            replica_seed: None,
            cold: false,
        })
    }

    fn distinct_zones(&self, primary: WorkerId, replica: WorkerId) -> bool {
        self.topo
            .member(primary)
            .zip(self.topo.member(replica))
            .is_some_and(|(primary, replica)| primary.zone != replica.zone)
    }

    fn has_replica_zone(&self, target: WorkerId, slot: Slot) -> bool {
        [slot.primary, slot.replica]
            .into_iter()
            .flatten()
            .any(|worker| self.distinct_zones(target, worker))
    }

    fn replica_for(&self, primary: WorkerId, old: Option<WorkerId>) -> Option<WorkerId> {
        self.topo
            .workers
            .values()
            .map(|member| member.id)
            .filter(|&candidate| {
                Some(candidate) != old
                    && self.eligible(candidate)
                    && self.move_capacity(candidate.node)
                    && self.distinct_zones(primary, candidate)
            })
            .min_by_key(|candidate| self.replica_load.get(&candidate.node).copied().unwrap_or(0))
    }

    fn eligible(&self, worker: WorkerId) -> bool {
        !self.topo.draining.contains(&worker.node) && self.observed.live(worker, self.now)
    }

    fn move_capacity(&self, worker: u64) -> bool {
        self.moves.get(&worker).copied().unwrap_or(0) < MAX_MOVES_PER_WORKER
    }

    fn add_move(&mut self, source: u64, target: u64) {
        *self.moves.entry(source).or_default() += 1;
        *self.moves.entry(target).or_default() += 1;
    }
}

fn close_enough(source: Progress, target: Progress) -> bool {
    target.sequence <= source.sequence && source.sequence - target.sequence <= MAX_CUTOVER_LAG
}

fn names_node(slot: Slot, worker: u64) -> bool {
    [
        slot.primary,
        slot.replica,
        slot.moving.map(|moving| moving.target),
    ]
    .into_iter()
    .flatten()
    .any(|id| id.node == worker)
}
