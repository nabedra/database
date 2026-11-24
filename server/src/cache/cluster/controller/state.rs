use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::protocol::{
    Admin, Grant, Lease, MAX_WORKERS, Member, Progress, Reply, Slot, SlotRange, Topology, Update,
    VERSION, WorkerId,
};
use super::super::transport::{Fingerprint, bounded_json};
use super::placement::{Change, Order};
use crate::cache::command::SLOT_COUNT;

const MAX_OPERATIONS: usize = 8_192;
const MAX_RETIRED_BOOTS: usize = 8_192;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Command {
    Enroll {
        member: Member,
        certificate: Fingerprint,
    },
    Renew {
        generation: u64,
        revision: u64,
        worker: WorkerId,
        expires_ms: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        staged_progress: Vec<Progress>,
    },
    Admin {
        id: Uuid,
        action: Admin,
        lower_ms: u64,
    },
    Reconcile {
        generation: u64,
        revision: u64,
        lower_ms: u64,
        changes: Vec<Change>,
    },
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Enroll { .. } => "enroll cache worker",
            Self::Renew { .. } => "renew cache authority",
            Self::Admin { .. } => "cache topology operation",
            Self::Reconcile { .. } => "reconcile cache placement",
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub(super) struct State {
    pub topology: Topology,
    pub orders: Vec<Order>,
    tickets: BTreeMap<u64, Member>,
    retired: BTreeSet<WorkerId>,
    operations: BTreeMap<Uuid, Receipt>,
    groups: usize,
    // Only replica-only moves need a source watermark across controller terms.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(super) staged_sequences: BTreeMap<u16, u64>,
    #[cfg(feature = "sim")]
    #[serde(skip)]
    pub(super) applied_staged: Vec<(u16, super::placement::StagedPromotion)>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    digest: [u8; 32],
    revision: u64,
}

impl State {
    pub(super) fn is_retired(&self, worker: WorkerId) -> bool {
        self.retired.contains(&worker)
    }

    pub fn apply(&mut self, command: Command, respond: bool) -> Option<Reply> {
        match command {
            Command::Enroll {
                member,
                certificate,
            } => {
                let result = self.enroll(member, certificate);
                respond.then(|| {
                    result.map_or_else(refused, |_| Reply::Applied {
                        operation: Uuid::nil(),
                        revision: self.topology.revision,
                    })
                })
            }
            Command::Renew {
                generation,
                revision,
                worker,
                expires_ms,
                staged_progress,
            } => self.renew(
                generation,
                revision,
                worker,
                expires_ms,
                staged_progress,
                respond,
            ),
            Command::Admin {
                id,
                action,
                lower_ms,
            } => {
                let result = self.admin(id, action, lower_ms);
                respond.then(|| {
                    result.map_or_else(refused, |revision| Reply::Applied {
                        operation: id,
                        revision,
                    })
                })
            }
            Command::Reconcile {
                generation,
                revision,
                lower_ms,
                changes,
            } => {
                if generation != self.topology.generation || revision != self.topology.revision {
                    return respond.then_some(Reply::Retry);
                }
                let changed = self.reconcile(lower_ms, changes);
                respond.then_some(if changed {
                    Reply::Applied {
                        operation: Uuid::nil(),
                        revision: self.topology.revision,
                    }
                } else {
                    Reply::Retry
                })
            }
        }
    }

    fn enroll(&mut self, member: Member, certificate: Fingerprint) -> Result<(), &'static str> {
        if !member.valid()
            || !member.authenticates(certificate)
            || self.retired.contains(&member.id)
            || self.topology.drained.contains(&member.id.node)
        {
            return Err("invalid worker or retired incarnation");
        }
        let current = self.topology.workers.get(&member.id.node);
        if current == Some(&member) {
            return Ok(());
        }
        let Some(ticket) = self.tickets.get(&member.id.node) else {
            return Err("worker has no committed enrollment authorization");
        };
        let mut authorized = ticket.clone();
        authorized.id = member.id;
        if authorized != member {
            return Err("worker enrollment differs from committed authorization");
        }

        let revision = self.next_revision()?;
        if let Some(previous) = current {
            if self.retired.len() == MAX_RETIRED_BOOTS {
                return Err("worker incarnation history is full");
            }
            self.retired.insert(previous.id);
        }
        self.topology.workers.insert(member.id.node, member);
        self.topology.revision = revision;
        Ok(())
    }

    fn renew(
        &mut self,
        generation: u64,
        revision: u64,
        worker: WorkerId,
        expires_ms: u64,
        staged_progress: Vec<Progress>,
        respond: bool,
    ) -> Option<Reply> {
        let topo = &mut self.topology;
        if topo.member(worker).is_none() {
            return respond.then(|| refused("worker incarnation is not enrolled"));
        }
        let mut grants: Vec<Grant> = Vec::new();
        if generation == topo.generation {
            for progress in staged_progress {
                if progress.baseline
                    && topo
                        .slots
                        .get(usize::from(progress.slot))
                        .is_some_and(|slot| {
                            slot.epoch == progress.epoch
                                && slot.primary == Some(worker)
                                && slot.moving.is_some_and(|moving| moving.replica_only)
                        })
                {
                    let sequence = self.staged_sequences.entry(progress.slot).or_default();
                    *sequence = (*sequence).max(progress.sequence);
                }
            }
            for (index, slot) in topo.slots.iter_mut().enumerate() {
                if slot.primary != Some(worker)
                    || slot.moving.is_some_and(|moving| moving.freeze_requested)
                {
                    continue;
                }
                let prior = slot.lease.unwrap_or(Lease {
                    expires_ms: 0,
                    not_before_ms: 0,
                });
                // Do not mint a retroactive grant from a regressed clock.
                if expires_ms < prior.expires_ms || expires_ms <= prior.not_before_ms {
                    continue;
                }
                let lease = Lease {
                    expires_ms,
                    not_before_ms: prior.not_before_ms,
                };
                slot.lease = Some(lease);
                if respond {
                    let index = index as u16;
                    if let Some(last) = grants.last_mut()
                        && last.range.last + 1 == index
                        && last.range.epoch == slot.epoch
                        && last.lease == lease
                    {
                        last.range.last = index;
                    } else {
                        grants.push(Grant {
                            range: SlotRange {
                                first: index,
                                last: index,
                                epoch: slot.epoch,
                            },
                            lease,
                        });
                    }
                }
            }
        }
        respond.then(|| {
            Reply::Update(Update {
                version: VERSION,
                generation: topo.generation,
                revision: topo.revision,
                worker,
                topology: (generation != topo.generation || revision != topo.revision)
                    .then(|| topo.clone()),
                grants,
            })
        })
    }

    fn admin(&mut self, id: Uuid, action: Admin, lower_ms: u64) -> Result<u64, &'static str> {
        if id.is_nil() {
            return Err("operation must have a nonzero ID");
        }
        let digest: [u8; 32] = Sha256::digest(
            bounded_json(&action, 16 << 10).map_err(|_| "operator command exceeds its bound")?,
        )
        .into();
        if let Some(old) = self.operations.get(&id) {
            return if digest == old.digest {
                Ok(old.revision)
            } else {
                Err("operation ID already names a different command")
            };
        }
        if self.operations.len() == MAX_OPERATIONS {
            return Err("operator receipt history is full");
        }
        let revision = self.next_revision()?;
        match action {
            Admin::Init { groups } => self.init(groups)?,
            Admin::Enroll { member } | Admin::Replace { member } => self.authorize(member)?,
            Admin::Rebalance {
                first,
                last,
                target,
            } => self.order(id, first, last, target)?,
            Admin::Drain { worker } => self.drain(worker)?,
            Admin::ColdReset { slot } => self.reset(slot, lower_ms)?,
            Admin::Status => return Err("status is a read operation"),
        }
        self.topology.revision = revision;
        self.operations.insert(id, Receipt { digest, revision });
        Ok(revision)
    }

    fn drain(&mut self, worker: u64) -> Result<(), &'static str> {
        if self.topology.drained.contains(&worker) {
            return Ok(());
        }
        if !self.topology.workers.contains_key(&worker) {
            return Err("worker is not enrolled");
        }
        if self.orders.iter().any(|order| order.target == worker)
            || self.topology.slots.iter().any(|slot| {
                slot.moving
                    .is_some_and(|moving| moving.target.node == worker)
            })
        {
            return Err("worker has an incoming slot move");
        }
        if self
            .topology
            .workers
            .values()
            .filter(|member| {
                member.id.node != worker && !self.topology.draining.contains(&member.id.node)
            })
            .map(|member| &member.zone)
            .collect::<BTreeSet<_>>()
            .len()
            < 2
        {
            return Err("drain would leave fewer than two replica zones");
        }
        self.topology.draining.insert(worker);
        Ok(())
    }

    pub fn complete_drain(&mut self, worker: u64) -> bool {
        if self.topology.drained.contains(&worker) || self.retired.len() == MAX_RETIRED_BOOTS {
            return false;
        }
        if let Some(member) = self.topology.workers.remove(&worker) {
            self.retired.insert(member.id);
        }
        self.tickets.remove(&worker);
        self.topology.drained.insert(worker)
    }

    fn authorize(&mut self, mut member: Member) -> Result<(), &'static str> {
        if !member.id.incarnation.is_nil() {
            return Err("worker enrollment authorization requires a nil incarnation");
        }
        member.id.incarnation = Uuid::from_u128(1);
        if !member.valid() {
            return Err("invalid worker enrollment authorization");
        }
        member.id.incarnation = Uuid::nil();
        if !self.tickets.contains_key(&member.id.node) && self.tickets.len() == MAX_WORKERS {
            return Err("worker limit reached");
        }
        if self
            .tickets
            .values()
            .chain(self.topology.workers.values())
            .any(|old| {
                old.id.node != member.id.node
                    && (old
                        .peer
                        .fingerprints
                        .iter()
                        .any(|fingerprint| member.peer.fingerprints.contains(fingerprint))
                        || (old.client_host == member.client_host
                            && old.client_port == member.client_port)
                        || (old.peer.host == member.peer.host && old.peer.port == member.peer.port))
            })
        {
            return Err("worker endpoint or certificate duplicates another identity");
        }
        if self.topology.drained.remove(&member.id.node) {
            self.topology.draining.remove(&member.id.node);
        }
        self.tickets.insert(member.id.node, member);
        Ok(())
    }

    fn init(&mut self, groups: usize) -> Result<(), &'static str> {
        if self.groups == groups && groups > 0 {
            return Ok(());
        }
        if self.groups != 0 || !(3..=MAX_WORKERS).contains(&groups) {
            return Err("invalid initial group count or topology already initialized");
        }
        let owners: Vec<_> = self
            .topology
            .workers
            .values()
            .filter(|member| !self.topology.draining.contains(&member.id.node))
            .take(groups)
            .collect();
        if owners.len() != groups
            || owners
                .iter()
                .map(|member| &member.zone)
                .collect::<BTreeSet<_>>()
                .len()
                < 3
        {
            return Err("initial placement requires enough enrolled workers in three zones");
        }
        let replicas = assign_replicas(&owners).ok_or("no distinct-zone replica assignment")?;
        for (index, slot) in self.topology.slots.iter_mut().enumerate() {
            let primary = index * groups / SLOT_COUNT;
            *slot = Slot {
                epoch: 1,
                primary: Some(owners[primary].id),
                replica: Some(owners[replicas[primary]].id),
                cold: true,
                ..Slot::default()
            };
        }
        self.groups = groups;
        Ok(())
    }

    fn order(&mut self, id: Uuid, first: u16, last: u16, target: u64) -> Result<(), &'static str> {
        if first > last
            || usize::from(last) >= SLOT_COUNT
            || self.groups == 0
            || self.topology.draining.contains(&target)
            || !self.topology.workers.contains_key(&target)
        {
            return Err("invalid rebalance target or slot range");
        }
        let slots = &self.topology.slots[usize::from(first)..=usize::from(last)];
        if slots.iter().any(|slot| slot.moving.is_some())
            || self
                .orders
                .iter()
                .any(|order| first <= order.last && last >= order.first)
        {
            return Err("slot already has a pending move");
        }
        self.orders.push(Order {
            id,
            first,
            last,
            target,
        });
        Ok(())
    }

    fn reset(&mut self, slot: Option<u16>, lower_ms: u64) -> Result<(), &'static str> {
        if self.groups == 0 || slot.is_some_and(|slot| usize::from(slot) >= SLOT_COUNT) {
            return Err("invalid cold reset scope");
        }
        let (first, last) = slot.map_or((0, SLOT_COUNT - 1), |index| {
            (usize::from(index), usize::from(index))
        });
        let scoped = &self.topology.slots[first..=last];
        let authority_is_live = scoped
            .iter()
            .any(|slot| slot.lease.is_some_and(|lease| lower_ms < lease.expires_ms));
        #[cfg(feature = "sim")]
        let authority_is_live =
            authority_is_live && !super::super::sim::hooks::knobs().cold_reset_ignores_live_lease;
        if authority_is_live {
            return Err("prior authority is still live");
        }
        if scoped.iter().any(|slot| slot.epoch == u64::MAX) {
            return Err("slot epoch exhausted");
        }
        let assignments = scoped
            .iter()
            .map(|slot| {
                let primary = slot
                    .primary
                    .and_then(|id| self.topology.workers.get(&id.node))
                    .filter(|member| !self.topology.draining.contains(&member.id.node))
                    .or_else(|| {
                        self.topology
                            .workers
                            .values()
                            .find(|member| !self.topology.draining.contains(&member.id.node))
                    })?;
                let replica = slot
                    .replica
                    .and_then(|id| self.topology.workers.get(&id.node))
                    .filter(|member| {
                        member.zone != primary.zone
                            && !self.topology.draining.contains(&member.id.node)
                    })
                    .or_else(|| {
                        self.topology.workers.values().find(|member| {
                            member.zone != primary.zone
                                && !self.topology.draining.contains(&member.id.node)
                        })
                    });
                Some((primary.id, replica.map(|member| member.id)))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or("cold reset has no eligible worker")?;

        if slot.is_none() {
            self.topology.generation = self
                .topology
                .generation
                .checked_add(1)
                .ok_or("cluster generation exhausted")?;
        }
        self.orders
            .retain(|order| usize::from(order.last) < first || usize::from(order.first) > last);
        self.staged_sequences
            .retain(|&index, _| !(first..=last).contains(&usize::from(index)));
        for (slot, (primary, replica)) in self.topology.slots[first..=last]
            .iter_mut()
            .zip(assignments)
        {
            *slot = slot.cold_reset(primary, replica);
        }
        Ok(())
    }

    pub fn valid_snapshot(&self) -> bool {
        self.operations.len() <= MAX_OPERATIONS
            && self.tickets.len() <= MAX_WORKERS
            && self.topology.workers.len() <= MAX_WORKERS
            && self.retired.len() <= MAX_RETIRED_BOOTS
            && self.groups <= MAX_WORKERS
            && self.staged_sequences.keys().all(|&index| {
                self.topology
                    .slots
                    .get(usize::from(index))
                    .is_some_and(|slot| slot.moving.is_some_and(|moving| moving.replica_only))
            })
            && self
                .orders
                .iter()
                .all(|order| order.first <= order.last && usize::from(order.last) < SLOT_COUNT)
    }

    pub fn initialized(&self) -> bool {
        self.groups != 0
    }

    pub fn next_revision(&self) -> Result<u64, &'static str> {
        self.topology
            .revision
            .checked_add(1)
            .ok_or("topology revision exhausted")
    }
}

fn refused(message: &'static str) -> Reply {
    Reply::Refused(message.into())
}

// Augment one matching at a time. A balanced deployment uses one replica
// group per worker, and every pair spans distinct zones.
fn assign_replicas(owners: &[&Member]) -> Option<Vec<usize>> {
    fn place(
        src: usize,
        owners: &[&Member],
        matching: &mut [Option<usize>],
        seen: &mut [bool],
    ) -> bool {
        for offset in 1..owners.len() {
            let dst = (src + offset) % owners.len();
            if seen[dst] || owners[dst].zone == owners[src].zone {
                continue;
            }
            seen[dst] = true;
            if matching[dst].is_none_or(|previous| place(previous, owners, matching, seen)) {
                matching[dst] = Some(src);
                return true;
            }
        }
        false
    }

    let mut matching = vec![None; owners.len()];
    for source in 0..owners.len() {
        if !place(
            source,
            owners,
            &mut matching,
            &mut vec![false; owners.len()],
        ) {
            return None;
        }
    }
    let mut replica = vec![0; owners.len()];
    for (destination, source) in matching.into_iter().enumerate() {
        replica[source?] = destination;
    }
    Some(replica)
}
