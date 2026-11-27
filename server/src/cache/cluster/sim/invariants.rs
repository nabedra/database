//! Invariants over controller commits and the actual records installed by workers.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use nabedra_check::history::{Event, EventType, NEMESIS};
use nabedra_sim::Trace;
use serde_json::json;

use super::super::{
    controller::MAX_CUTOVER_LAG,
    protocol::{Lease, Slot, Topology, WorkerId},
};
use super::hooks::{Applied, Observed};
use crate::cache::command::{SLOT_COUNT, key_slot};

pub(super) type Version = (u64, u16, u64, u64);
type Digest = [u8; 20];

#[derive(Default)]
pub(super) struct Observations {
    pub trace: Trace,
    pub history: Vec<Event>,
    pub violations: BTreeMap<&'static str, String>,
    pub latest: Option<Topology>,
    pub leases: BTreeMap<(u16, WorkerId), Lease>,
    /// When each lease view could first authorize, in true nanoseconds: the
    /// later of its `not_before` and its installation.
    view_start: BTreeMap<(u16, WorkerId), u64>,
    pub reset_slots: BTreeSet<u16>,
    commits: BTreeMap<u64, (u64, u64, u64)>,
    pub revisions: BTreeSet<(u64, u64)>,
    applied: BTreeMap<u64, u64>,
    served: BTreeMap<u16, (u64, WorkerId, u64)>,
    failover_markers: BTreeMap<(u16, u64), Vec<usize>>,
    /// Committed staged promotions with the virtual time of first apply.
    pub staged: Vec<(u64, u16, super::super::controller::StagedPromotion)>,
    /// Slots the planner reset on its own, with the virtual time.
    pub automatic_resets: Vec<(u64, u16)>,
    /// The prefix each successor kept, by slot and the epoch it came from.
    promoted: BTreeMap<(u16, u64), (WorkerId, u64)>,
    /// Latest (generation, epoch, sequence) a primary published and a copy
    /// applied, by slot and worker.
    published: BTreeMap<(u16, WorkerId), (u64, u64, u64)>,
    copied: BTreeMap<(u16, WorkerId), (u64, u64, u64)>,
    primary: BTreeMap<(Version, WorkerId), Digest>,
    replicas: BTreeMap<(Version, WorkerId), (WorkerId, Digest)>,
    pub check_replica_prefix: bool,
    /// Directed schedules only: a completed transfer must hold the source's
    /// actual prefix within the cutover limit. The sweep's reports can trail
    /// the actual sequence by a heartbeat of writes, so it does not check it.
    pub check_transfer_lag: bool,
    /// Directed schedules only: an automatic reset must not discard a staged
    /// copy that holds the current epoch's baseline. The sweep cannot tell
    /// whether the controller had heard of that copy.
    pub check_last_copy: bool,
    /// Redelivered SETs that a replica refused and applied.
    pub redelivered: (u64, u64),
    /// Messages that a late partition held.
    pub held: u64,
}

impl Observations {
    pub fn new(check_replica_prefix: bool, keep_full_trace: bool) -> Self {
        Self {
            trace: Trace::new()
                .keep_full(keep_full_trace || nabedra_sim::trace::full_trace_requested()),
            check_replica_prefix,
            ..Self::default()
        }
    }

    pub fn violate(&mut self, kind: &'static str, detail: String) {
        self.violations.entry(kind).or_insert(detail);
    }

    pub fn crash(&mut self, node: u64) {
        self.leases.retain(|(_, worker), _| worker.node != node);
        self.view_start.retain(|(_, worker), _| worker.node != node);
        self.published.retain(|(_, worker), _| worker.node != node);
        self.copied.retain(|(_, worker), _| worker.node != node);
        self.applied.remove(&node);
    }

    pub fn served_sequence(&self, slot: u16, worker: WorkerId, epoch: u64) -> Option<u64> {
        let &(served_epoch, owner, sequence) = self.served.get(&slot)?;
        (served_epoch == epoch && owner == worker).then_some(sequence)
    }

    pub fn committed(
        &mut self,
        node: u64,
        index: u64,
        term: u64,
        leader: u64,
        now: u64,
        state: &Topology,
        keys: &[String],
        applied: &Applied,
    ) {
        let staged = &applied.staged;
        let signature = (term, leader, topology_hash(state));
        if let Some(prior) = self.applied.insert(node, index)
            && prior > index
        {
            self.violate(
                "MonotonicCommit",
                format!("controller {node} applied {index} after {prior}"),
            );
        }
        self.trace.record(
            now,
            format_args!(
                "apply node={node} term={term} leader={leader} index={index} hash={:x}",
                signature.2
            ),
        );
        if let Some(prior) = self.commits.insert(index, signature) {
            if prior != signature {
                self.violate(
                    "AppliedPrefixesAgree",
                    format!("state differs at controller log index {index}"),
                );
            }
            return;
        }
        self.staged
            .extend(staged.iter().map(|&(slot, proof)| (now, slot, proof)));
        self.automatic_resets
            .extend(applied.resets.iter().map(|&slot| (now, slot)));
        self.revisions.insert((state.generation, state.revision));
        if let Some(err) = invalid_placement(state) {
            self.violate("SlotOwnership", err);
        }
        if self
            .commits
            .last_key_value()
            .is_some_and(|(&last, _)| last > index)
        {
            return;
        }
        if let Some(prior) = self.latest.take() {
            if prior.generation > state.generation || prior.revision > state.revision {
                self.violate(
                    "SlotOwnership",
                    format!("controller topology went back at {index}"),
                );
            }
            for (i, (old, new)) in prior.slots.iter().zip(&state.slots).enumerate() {
                if new.epoch < old.epoch || (new.primary != old.primary && new.epoch == old.epoch) {
                    self.violate("SlotOwnership", format!("slot {i} changed primary at the same epoch or went backwards at {index}"));
                    break;
                }
            }
            let cold = |old: Slot, new: Slot| old.epoch > 0 && new.cold && new.epoch > old.epoch;
            self.transitions(&prior, state, keys, applied);
            for key in keys {
                let slot = key_slot(key.as_bytes());
                let Some((&old, &new)) = prior
                    .slots
                    .get(usize::from(slot))
                    .zip(state.slots.get(usize::from(slot)))
                else {
                    continue;
                };
                let marker = if cold(old, new) {
                    self.reset_slots.insert(slot);
                    let true_ms = now + nabedra_rt::sim::EPOCH_MS;
                    if old.lease.is_some_and(|lease| true_ms < lease.expires_ms) {
                        self.violate(
                            "LiveLeaseReset",
                            format!(
                                "slot {slot} reset at {true_ms} with a live grant {:?}",
                                old.lease
                            ),
                        );
                    }
                    "cold-reset"
                } else if planned_handover(old, new) {
                    // A cutover to a target holding exactly the frozen
                    // prefix, or a frozen owner resuming under a new epoch.
                    // The source accepted nothing after its freeze, so no
                    // acknowledged write may be lost across it.
                    "handover"
                } else if old.primary.is_some()
                    && (old.epoch != new.epoch || old.primary != new.primary)
                {
                    "failover"
                } else {
                    continue;
                };
                if marker != "cold-reset" {
                    // The successor's retained prefix replaces the planned one
                    // when it installs. Later failovers cannot erase that proof.
                    self.failover_markers
                        .entry((slot, new.epoch))
                        .or_default()
                        .push(self.history.len());
                }
                let mut value = json!({"epoch": new.epoch});
                if marker == "failover" {
                    // The old primary can acknowledge nothing once its latest
                    // committed grant expires, so a loss is bounded by the
                    // lag window before that instant, not before this commit.
                    if let Some(lease) = old.lease {
                        value["authority_end_ns"] =
                            lease.expires_ms.saturating_mul(1_000_000).into();
                    }
                }
                if marker == "failover"
                    && let Some((_, proof)) = staged.iter().find(|(index, _)| *index == slot)
                {
                    self.trace
                        .record(now, format_args!("staged promotion slot={slot} {proof:?}"));
                    value["prefix_epoch"] = proof.retained_epoch.into();
                    value["prefix_sequence"] = proof.retained_sequence.into();
                    let published = self
                        .published
                        .get(&(slot, proof.source))
                        .filter(|&&(generation, epoch, _)| {
                            generation == prior.generation && epoch == proof.retained_epoch
                        })
                        .map(|&(_, _, sequence)| sequence);
                    value["staged_promotion"] = json!({
                        "reported_sequence": proof.retained_sequence,
                        "source_sequence": proof.source_sequence,
                        // The source's actual last publication, for the record.
                        // The cutover limit is a policy on the controller's
                        // committed watermark (CACHE-4), so it is not checked
                        // against this.
                        "source_published_sequence": published,
                        "max_lag": MAX_CUTOVER_LAG,
                        // The last verified copy is promoted whatever its lag (CACHE-4).
                        "last_copy": proof.last_copy,
                    });
                }
                self.history
                    .push(event(EventType::Info, NEMESIS, marker, key, value, now));
            }
        }
        self.latest = Some(state.clone());
    }

    /// Checks on one committed transition of the watched slots that need the
    /// actual worker progress, not the controller's reports.
    fn transitions(
        &mut self,
        prior: &Topology,
        state: &Topology,
        keys: &[String],
        applied: &Applied,
    ) {
        if !self.check_transfer_lag && !self.check_last_copy {
            return;
        }
        let slots: BTreeSet<u16> = keys.iter().map(|key| key_slot(key.as_bytes())).collect();
        let at = |progress: &BTreeMap<(u16, WorkerId), (u64, u64, u64)>, slot, worker, epoch| {
            progress
                .get(&(slot, worker))
                .filter(|&&(generation, at, _)| generation == prior.generation && at == epoch)
                .map(|&(_, _, sequence)| sequence)
        };
        for slot in slots {
            let (old, new) = (
                prior.slots[usize::from(slot)],
                state.slots[usize::from(slot)],
            );
            let Some(moving) = old.moving.filter(|moving| moving.replica_only) else {
                continue;
            };
            let target = moving.target;
            let completes = new.epoch == old.epoch
                && new.moving.is_none()
                && new.replica == Some(target)
                && new.primary == old.primary;
            if self.check_transfer_lag
                && completes
                && let Some(source) = old.primary
            {
                let source_sequence = at(&self.published, slot, source, old.epoch).unwrap_or(0);
                let copy = at(&self.copied, slot, target, old.epoch).unwrap_or(0);
                if source_sequence.saturating_sub(copy) > MAX_CUTOVER_LAG {
                    self.violate(
                        "TransferLag",
                        format!(
                            "slot {slot} made {} its replica at sequence {copy}, source {} at {source_sequence}",
                            target.node, source.node
                        ),
                    );
                }
            }
            if self.check_last_copy
                && applied.resets.contains(&slot)
                && prior.member(target).is_some()
                && old
                    .primary
                    .is_some_and(|source| prior.member(source).is_some())
                && at(&self.copied, slot, target, old.epoch).is_some()
            {
                self.violate(
                    "LastCopyDiscarded",
                    format!(
                        "slot {slot} reset while staged target {} held the epoch {} baseline",
                        target.node, old.epoch
                    ),
                );
            }
        }
    }

    pub fn observe(&mut self, node: u64, event: Observed, now: u64, now_ns: u64) {
        match event {
            Observed::Lease {
                worker,
                slot,
                writable,
                epoch,
                lease,
            } => {
                let key = (slot, worker);
                let next = lease.filter(|_| writable);
                let old = self.leases.remove(&key);
                let old_start = self.view_start.remove(&key);
                if let Some(lease) = next {
                    // A renewal keeps the view's start. A new grant cannot
                    // authorize before it reaches the worker.
                    let start = old_start
                        .filter(|_| old.is_some_and(|old| old.not_before_ms == lease.not_before_ms))
                        .unwrap_or_else(|| {
                            lease.not_before_ms.saturating_mul(1_000_000).max(now_ns)
                        });
                    let end = lease.expires_ms.saturating_mul(1_000_000);
                    // CACHE-G06 in closed form, without product code: under
                    // the clock assumption a view authorizes only within
                    // [start, expiry) of true time. Two workers' views of one
                    // slot must not overlap.
                    let overlap = self
                        .leases
                        .iter()
                        .find(|&(&(other_slot, other), view)| {
                            other_slot == slot
                                && other.node != worker.node
                                && self.view_start.get(&(other_slot, other)).is_some_and(
                                    |&other_start| {
                                        start < view.expires_ms.saturating_mul(1_000_000)
                                            && other_start < end
                                    },
                                )
                        })
                        .map(|(&(_, other), _)| other);
                    if let Some(other) = overlap {
                        self.violate(
                            "LeaseExclusive",
                            format!(
                                "slot {slot} grant {lease:?} on {} overlaps the view on {}",
                                worker.node, other.node
                            ),
                        );
                    }
                    self.leases.insert(key, lease);
                    self.view_start.insert(key, start);
                }
                if old != next {
                    self.trace.record(
                        now,
                        format_args!("lease node={node} slot={slot} epoch={epoch} {next:?}"),
                    );
                }
            }
            Observed::Published {
                worker,
                generation,
                slot,
                epoch,
                sequence,
                digest,
            } => {
                self.primary
                    .insert(((generation, slot, epoch, sequence), worker), digest);
                self.published
                    .insert((slot, worker), (generation, epoch, sequence));
                self.trace.record(now, format_args!("publish node={node} slot={slot} epoch={epoch} seq={sequence} digest={digest:x?}"));
                // CACHE-G05 and decision 5: the lag window ends at the
                // handover. Once a successor installed its prefix of this
                // epoch, a later mutation in it can never reach a copy.
                if let Some(&(successor, prefix)) = self.promoted.get(&(slot, epoch))
                    && successor != worker
                    && sequence > prefix
                {
                    self.violate(
                        "AckAfterHandover",
                        format!(
                            "worker {} published slot {slot} epoch {epoch} sequence {sequence} after a successor kept prefix {prefix}",
                            worker.node
                        ),
                    );
                }
            }
            Observed::ReplicaApplied {
                worker,
                source,
                generation,
                slot,
                epoch,
                sequence,
                digest,
            } => {
                let version = (generation, slot, epoch, sequence);
                self.replicas.insert((version, worker), (source, digest));
                self.copied
                    .insert((slot, worker), (generation, epoch, sequence));
                if self.check_replica_prefix {
                    self.replica_prefix(version, worker, source, digest);
                }
                self.trace.record(now, format_args!("replica node={node} source={} slot={slot} epoch={epoch} seq={sequence} digest={digest:x?}", source.node));
            }
            Observed::Redelivered {
                worker,
                slot,
                epoch,
                sequence,
                current,
                applied,
            } => {
                if applied {
                    self.redelivered.1 += 1;
                } else {
                    self.redelivered.0 += 1;
                }
                self.trace.record(now, format_args!("redelivered SET node={} slot={slot} epoch={epoch} seq={sequence} current={current} applied={applied}", worker.node));
            }
            Observed::Promoted {
                worker,
                slot,
                epoch,
                prefix_epoch,
                prefix_sequence,
            } => {
                // Use the copy actually retained at install. Replication can
                // keep advancing after the controller commits the promotion.
                self.promoted
                    .entry((slot, prefix_epoch))
                    .and_modify(|kept| kept.1 = kept.1.min(prefix_sequence))
                    .or_insert((worker, prefix_sequence));
                if let Some(markers) = self.failover_markers.remove(&(slot, epoch)) {
                    for index in markers {
                        let value = &mut self.history[index].value;
                        value["prefix_epoch"] = prefix_epoch.into();
                        value["prefix_sequence"] = prefix_sequence.into();
                    }
                }
            }
            Observed::Served {
                worker,
                slot,
                epoch,
                sequence,
                lease,
            } => {
                // CACHE-G06 without the product's lease function: under the
                // clock assumption, a turn may be authorized only at a true
                // instant inside its grant.
                let inside = lease.is_some_and(|lease| {
                    lease.not_before_ms.saturating_mul(1_000_000) <= now_ns
                        && now_ns < lease.expires_ms.saturating_mul(1_000_000)
                });
                if !inside {
                    self.violate(
                        "ServedWithinLease",
                        format!(
                            "worker {} served slot {slot} epoch {epoch} at {now_ns} ns with grant {lease:?}",
                            worker.node
                        ),
                    );
                }
                if let Some((last, owner, _)) = self.served.get(&slot).copied()
                    && (epoch < last || (epoch == last && worker != owner))
                {
                    self.violate(
                        "ServedEpochOrder",
                        format!(
                            "slot {slot} served epoch {epoch} on {node} after {last} on {}",
                            owner.node
                        ),
                    );
                }
                self.served.insert(slot, (epoch, worker, sequence));
            }
            Observed::Charged {
                worker,
                slot,
                slot_bytes,
                slot_limit,
                actor_bytes,
                actor_limit,
            } => {
                if slot_bytes > slot_limit || actor_bytes > actor_limit {
                    self.violate("MemoryCap", format!("worker {} slot {slot}: slot {slot_bytes}/{slot_limit}, actor {actor_bytes}/{actor_limit}", worker.node));
                }
            }
            Observed::Installed {
                worker,
                generation,
                revision,
            } => {
                if (generation, revision) != (1, 0)
                    && !self.revisions.contains(&(generation, revision))
                {
                    self.violate(
                        "RedirectAgrees",
                        format!(
                            "worker {} installed uncommitted topology {generation}/{revision}",
                            worker.node
                        ),
                    );
                }
            }
        }
    }

    fn replica_prefix(
        &mut self,
        version: Version,
        worker: WorkerId,
        source: WorkerId,
        digest: Digest,
    ) {
        match self.primary.get(&(version, source)) {
            Some(expected) if *expected == digest => {}
            Some(_) => self.violate(
                "ReplicaPrefix",
                format!(
                    "worker {} differs from primary {} at {version:?}",
                    worker.node, source.node
                ),
            ),
            None => {
                let (generation, slot, epoch, sequence) = version;
                let any_prior = self
                    .primary
                    .range(
                        ((generation, slot, epoch, 0), source)
                            ..=((generation, slot, epoch, sequence), source),
                    )
                    .any(|(&(_, owner), _)| owner == source);
                if any_prior {
                    self.violate(
                        "ReplicaNotAhead",
                        format!(
                            "worker {} reports unobserved source sequence {version:?} from {}",
                            worker.node, source.node
                        ),
                    );
                }
            }
        }
    }
}

/// A planned owner change: the cutover of an online move to a target seeded
/// with the frozen prefix, or a frozen source resuming under a new epoch.
fn planned_handover(old: Slot, new: Slot) -> bool {
    let Some(moving) = old
        .moving
        .filter(|moving| !moving.replica_only && moving.frozen.is_some())
    else {
        return false;
    };
    !new.cold
        && new.epoch > old.epoch
        && ((new.primary == Some(moving.target) && new.replica_seed.is_some())
            || (new.primary == old.primary && new.replica_seed.is_none()))
}

fn topology_hash(topology: &Topology) -> u64 {
    let Topology {
        generation,
        revision,
        workers,
        slots,
        draining,
        drained,
        ready_primaries,
        ready_replicas,
    } = topology;
    let mut hash = DefaultHasher::new();
    (
        generation,
        revision,
        workers,
        draining,
        drained,
        ready_primaries,
        ready_replicas,
        slots.len(),
    )
        .hash(&mut hash);
    // Renewals usually leave long runs of identical placement and grants.
    // Hash the complete map as (length, slot) runs to avoid hashing the same
    // identifiers and grant for every member of a run on every Raft apply.
    for run in slots.chunk_by(|left, right| left == right) {
        (run.len(), run[0]).hash(&mut hash);
    }
    hash.finish()
}

fn invalid_placement(topology: &Topology) -> Option<String> {
    if topology.slots.len() != SLOT_COUNT {
        return Some(format!(
            "committed topology has {} slots",
            topology.slots.len()
        ));
    }
    let initialized = topology.slots.iter().any(|slot| slot.primary.is_some());
    for (index, slot) in topology.slots.iter().enumerate() {
        let primary = slot.primary.and_then(|id| topology.workers.get(&id.node));
        let replica = slot.replica.and_then(|id| topology.workers.get(&id.node));
        if initialized && (primary.is_none() || slot.epoch == 0)
            || slot.replica.is_some() && replica.is_none()
            || primary
                .zip(replica)
                .is_some_and(|(p, r)| p.id.node == r.id.node || p.zone == r.zone)
        {
            return Some(format!(
                "committed slot {index} has invalid placement {slot:?}"
            ));
        }
    }
    None
}

pub(super) fn event(
    kind: EventType,
    process: u64,
    f: &str,
    key: &str,
    value: serde_json::Value,
    now: u64,
) -> Event {
    let mut event = Event::new(kind, process, f).with_key(key).with_value(value);
    event.time_ns = Some((nabedra_rt::sim::EPOCH_MS + now) * 1_000_000);
    event
}

#[test]
fn resumed_owner_marks_retained_prefix_in_history() {
    let key = String::from("{resumed-owner}");
    let slot = key_slot(key.as_bytes());
    let mut topology = Topology::default();
    let place = &mut topology.slots[usize::from(slot)];
    place.primary = Some(WorkerId {
        node: 11,
        incarnation: uuid::Uuid::from_u128(1),
    });
    place.epoch = 1;
    let mut observations = Observations::default();
    observations.committed(
        1,
        1,
        1,
        1,
        0,
        &topology,
        std::slice::from_ref(&key),
        &Applied::default(),
    );
    topology.slots[usize::from(slot)].epoch = 2;
    observations.committed(1, 2, 1, 1, 1, &topology, &[key], &Applied::default());
    observations.observe(
        11,
        Observed::Promoted {
            worker: WorkerId {
                node: 12,
                incarnation: uuid::Uuid::from_u128(1),
            },
            slot,
            epoch: 2,
            prefix_epoch: 1,
            prefix_sequence: 42,
        },
        2,
        2_000_000,
    );
    assert_eq!(observations.history.len(), 1);
    let marker = &observations.history[0];
    assert_eq!(marker.f, "failover");
    assert_eq!(
        marker.value,
        json!({"epoch": 2, "prefix_epoch": 1, "prefix_sequence": 42})
    );
}
