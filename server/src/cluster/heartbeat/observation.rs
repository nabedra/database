//! Local evidence only. Call on the bounded coordinator executor outside locks.
use super::*;
use crate::cluster::{
    controller::{Owner, OwnershipObservation, OwnershipSample},
    member::{Node, NodeState, Registration},
    membership::ExpiryInput,
    state::{Control, Controller, LeaseState},
};
use std::time::Duration;

struct Entry {
    binding: HeartbeatBinding,
    member: Node,
    grace: Instant,
    sequence: Option<u64>,
    advance: Option<Instant>,
    heartbeat: Option<Heartbeat>,
}
// Pending coordinator runtime. One entry per current eligible member, never per read.
#[allow(dead_code)]
pub(crate) struct Observations {
    boot: Uuid,
    generation: u64,
    exhausted: bool,
    last_time: Instant,
    recover_time: bool,
    observation: Option<OwnershipObservation>,
    controller: Option<Controller>,
    entries: Vec<Entry>,
    autoscale: AutoscaleState,
}
// Pending C06b composition. Private, immutable, non-Clone and non-Serde.
#[allow(dead_code)]
pub(crate) struct DrainHeartbeat {
    observation: OwnershipObservation,
    boot: Uuid,
    generation: u64,
    binding: HeartbeatBinding,
    member: Node,
    heartbeat: Heartbeat,
    sampled_at: Instant,
    expiry: Duration,
}
impl std::fmt::Debug for Observations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Observations")
    }
}
impl std::fmt::Debug for DrainHeartbeat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DrainHeartbeat")
    }
}
#[allow(dead_code)] // Remove when C06b consumes these borrowed facts under live authority.
impl DrainHeartbeat {
    pub(crate) fn sampled_at(&self) -> Instant {
        self.sampled_at
    }
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(crate) fn controller_boot(&self) -> Uuid {
        self.boot
    }
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
    pub(crate) fn binding(&self) -> &HeartbeatBinding {
        &self.binding
    }
    pub(crate) fn member(&self) -> &Node {
        &self.member
    }
    pub(crate) fn heartbeat(&self) -> &Heartbeat {
        &self.heartbeat
    }
}
fn binding(root: &Control, n: &Node) -> Option<HeartbeatBinding> {
    if !matches!(
        n.state,
        NodeState::Starting | NodeState::Ready | NodeState::Draining
    ) || n.registration == Registration::Unregistered
    {
        return None;
    }
    let session = n.session.as_ref()?;
    Some(HeartbeatBinding {
        resources: n.resources,
        cluster_id: root.cluster_id,
        node_id: n.node_id,
        incarnation: n.incarnation?,
        session_id: session.id,
        verifier: session.verifier.clone(),
        operation_slots: n.operation_slots,
        partition_slots: n.partition_slots,
        queue_size: n.queue_size,
    })
}
#[allow(dead_code)] // Pending coordinator/runtime callers; no I/O or authority here.
impl Observations {
    pub(crate) fn new(controller_boot: Uuid, now: Instant) -> Self {
        Self {
            boot: controller_boot,
            generation: 1,
            exhausted: false,
            last_time: now,
            recover_time: false,
            observation: None,
            controller: None,
            entries: Vec::new(),
            autoscale: AutoscaleState::default(),
        }
    }
    fn bump(&mut self) -> Result<(), StorageError> {
        if self.exhausted {
            return Err(StorageError::LimitExceeded);
        }
        match self.generation.checked_add(1) {
            Some(generation) => {
                self.generation = generation;
                Ok(())
            }
            None => {
                self.exhausted = true;
                for entry in &mut self.entries {
                    entry.heartbeat = None;
                }
                Err(StorageError::LimitExceeded)
            }
        }
    }
    fn invalidate(&mut self, restart: bool) -> Result<(), StorageError> {
        self.autoscale_reset()?;
        for entry in &mut self.entries {
            entry.heartbeat = None;
        }
        self.recover_time |= restart;
        self.bump()
    }
    fn time(&mut self, now: Instant) -> Result<(), StorageError> {
        if self.exhausted {
            return Err(StorageError::LimitExceeded);
        }
        self.autoscale_deadlines(now)?;
        if now.checked_duration_since(self.last_time).is_none() {
            self.invalidate(true)?;
            return Err(StorageError::Unavailable);
        }
        if self.recover_time {
            for entry in &mut self.entries {
                entry.grace = now;
            }
            self.recover_time = false;
        }
        self.last_time = now;
        Ok(())
    }
    fn index(&self, binding: &HeartbeatBinding) -> Result<usize, StorageError> {
        self.entries
            .iter()
            .position(|e| &e.binding == binding)
            .ok_or(StorageError::Conflict)
    }
    pub(crate) fn reconcile_members(
        &mut self,
        root: &Control,
        now: Instant,
    ) -> Result<(), StorageError> {
        let observation = match OwnershipObservation::new(root) {
            Ok(value) => value,
            Err(error) => {
                self.autoscale_reset()?;
                return Err(error);
            }
        };
        if let Some(old) = &self.observation {
            if observation.revision() < old.revision() {
                self.autoscale_reset()?;
                return Err(StorageError::Conflict);
            }
            if observation.revision() == old.revision() && &observation != old {
                self.invalidate(true)?;
                return Err(StorageError::InvalidCatalog);
            }
        }
        self.time(now)?;
        if self.observation.as_ref() == Some(&observation) {
            return Ok(());
        }
        self.autoscale_reconcile(root)?;
        let controller_changed = self.controller.as_ref().is_none_or(|old| {
            old.owner_id != root.controller.owner_id
                || old.epoch != root.controller.epoch
                || old.lease_state != root.controller.lease_state
        });
        self.bump()?;
        let mut previous = std::mem::take(&mut self.entries);
        for node in &root.nodes {
            let Some(binding) = binding(root, node) else {
                continue;
            };
            let mut entry = if let Some(i) = previous.iter().position(|e| e.binding == binding) {
                previous.swap_remove(i)
            } else {
                Entry {
                    binding,
                    member: node.clone(),
                    grace: now,
                    sequence: None,
                    advance: None,
                    heartbeat: None,
                }
            };
            if controller_changed {
                entry.grace = now;
                entry.heartbeat = None;
            }
            if entry.member != *node {
                entry.heartbeat = None;
            }
            entry.member = node.clone();
            self.entries.push(entry);
        }
        self.observation = Some(observation);
        self.controller = Some(root.controller.clone());
        Ok(())
    }
    pub(crate) fn observe(
        &mut self,
        binding: &HeartbeatBinding,
        heartbeat: &Heartbeat,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.autoscale_reset()?;
        self.observe_internal(binding, heartbeat, now)
    }
    fn observe_internal(
        &mut self,
        binding: &HeartbeatBinding,
        heartbeat: &Heartbeat,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.time(now)?;
        let i = self.index(binding)?;
        if !binding.matches(heartbeat)
            || heartbeat.validate().is_err()
            || self.entries[i]
                .sequence
                .is_some_and(|s| heartbeat.sequence < s)
        {
            self.invalidate(false)?;
            return Err(StorageError::InvalidCatalog);
        }
        let entry = &self.entries[i];
        if entry.sequence == Some(heartbeat.sequence) {
            // A repeated contradictory DTO may close evidence, but never replace it.
            if entry.heartbeat.as_ref().is_some_and(|old| old != heartbeat) {
                self.invalidate(false)?;
                return Err(StorageError::InvalidCatalog);
            }
            return Ok(());
        }
        self.bump()?;
        let entry = &mut self.entries[i];
        let progressing = entry.sequence.is_some();
        entry.sequence = Some(heartbeat.sequence);
        entry.advance = Some(now);
        entry.grace = now;
        entry.heartbeat = progressing.then(|| heartbeat.clone());
        Ok(())
    }
    pub(crate) fn read_failed(
        &mut self,
        binding: &HeartbeatBinding,
        now: Instant,
    ) -> Result<(), StorageError> {
        self.time(now)?;
        self.autoscale_reset()?;
        let i = self.index(binding)?;
        self.entries[i].heartbeat = None;
        self.bump()
    }
    fn deadlines(
        &mut self,
        binding: &HeartbeatBinding,
        now: Instant,
        expiry: Duration,
    ) -> Result<(usize, Instant, Instant), StorageError> {
        self.time(now)?;
        let i = self.index(binding)?;
        let entry = &self.entries[i];
        let ends = if expiry.is_zero() {
            None
        } else {
            entry
                .grace
                .checked_add(expiry)
                .zip(entry.advance.unwrap_or(entry.grace).checked_add(expiry))
        };
        match ends {
            Some((grace, advance)) => Ok((i, grace, advance)),
            None => {
                self.invalidate(true)?;
                Err(StorageError::LimitExceeded)
            }
        }
    }
    pub(crate) fn fresh(
        &mut self,
        binding: &HeartbeatBinding,
        now: Instant,
        expiry: Duration,
    ) -> Result<bool, StorageError> {
        let (i, _, end) = self.deadlines(binding, now, expiry)?;
        Ok(self.entries[i].heartbeat.is_some() && now < end)
    }
    pub(crate) fn expiry_input(
        &mut self,
        binding: &HeartbeatBinding,
        now: Instant,
        expiry: Duration,
    ) -> Result<Option<ExpiryInput>, StorageError> {
        let (i, grace, advance) = self.deadlines(binding, now, expiry)?;
        let entry = &self.entries[i];
        if now < grace
            || now < advance
            || !matches!(entry.member.state, NodeState::Starting | NodeState::Ready)
        {
            return Ok(None);
        }
        Ok(Some(ExpiryInput {
            cluster_id: binding.cluster_id,
            member: entry.member.clone(),
            observation: self.observation.clone().ok_or(StorageError::Conflict)?,
            generation: self.generation,
            controller_boot: self.boot,
            grace_anchor: entry.grace,
            last_sequence: entry.sequence,
            last_advance: entry.advance,
            sampled_at: now,
        }))
    }
    /// Recompute immediately before candidate construction/submission under the live owner.
    pub(crate) fn matches_expiry(
        &mut self,
        binding: &HeartbeatBinding,
        captured: &ExpiryInput,
        now: Instant,
        expiry: Duration,
    ) -> Result<bool, StorageError> {
        let Some(live) = self.expiry_input(binding, now, expiry)? else {
            return Ok(false);
        };
        Ok(live.cluster_id == captured.cluster_id
            && live.member == captured.member
            && live.observation == captured.observation
            && live.controller_boot == captured.controller_boot
            && live.generation == captured.generation
            && live.grace_anchor == captured.grace_anchor
            && live.last_sequence == captured.last_sequence
            && live.last_advance == captured.last_advance
            && captured.sampled_at <= now)
    }
    /// Live comparison before C06b uses a previously issued receipt. No authority is installed.
    pub(crate) fn matches_drain(
        &mut self,
        receipt: &DrainHeartbeat,
        now: Instant,
    ) -> Result<bool, StorageError> {
        if !self.fresh(&receipt.binding, now, receipt.expiry)? {
            return Ok(false);
        }
        let entry = &self.entries[self.index(&receipt.binding)?];
        Ok(self.observation.as_ref() == Some(&receipt.observation)
            && self.boot == receipt.boot
            && self.generation == receipt.generation
            && entry.member == receipt.member
            && entry.heartbeat.as_ref() == Some(&receipt.heartbeat)
            && receipt.sampled_at <= now)
    }
    pub(crate) fn drain_heartbeat(
        &mut self,
        root: &Control,
        binding: &HeartbeatBinding,
        loaded: &LoadedHeartbeat,
        now: Instant,
        expiry: Duration,
    ) -> Result<DrainHeartbeat, StorageError> {
        self.reconcile_members(root, now)?;
        if expiry != Duration::from_millis(root.timing.expiry_ms) {
            self.invalidate(true)?;
            return Err(StorageError::InvalidCatalog);
        }
        if root.controller.owner_id != Some(self.boot)
            || root.controller.lease_state != LeaseState::Held
            || loaded.binding() != binding
        {
            self.invalidate(false)?;
            return Err(StorageError::Conflict);
        }
        self.observe(binding, loaded.heartbeat(), now)?;
        if !self.fresh(binding, now, expiry)? {
            return Err(StorageError::Unavailable);
        }
        let entry = &self.entries[self.index(binding)?];
        let heartbeat = entry.heartbeat.as_ref().ok_or(StorageError::Unavailable)?;
        let drain = entry.member.drain.as_ref().ok_or(StorageError::Conflict)?;
        if entry.member.state != NodeState::Draining || heartbeat.drain_id != Some(drain.id) {
            return Err(StorageError::Conflict);
        }
        Ok(DrainHeartbeat {
            observation: self.observation.clone().ok_or(StorageError::Conflict)?,
            boot: self.boot,
            generation: self.generation,
            binding: binding.clone(),
            member: entry.member.clone(),
            heartbeat: heartbeat.clone(),
            sampled_at: now,
            expiry,
        })
    }
}
#[cfg(test)]
mod tests;

#[derive(Default)]
struct AutoscaleState {
    serial: u64,
    identity: Option<Control>,
    terminal: std::collections::BTreeSet<(Uuid, Uuid)>,
    members: Vec<AutoscaleMember>,
    low: Option<OwnershipSample>,
    last_sample: Option<OwnershipSample>,
}
// Pending C07b/c and runtime consumers. Facts are borrowed only from a snapshot.
#[allow(dead_code)]
pub(crate) struct AutoscaleMember {
    member: Node,
    binding: Option<HeartbeatBinding>,
    heartbeat: Option<Heartbeat>,
    advanced_at: Option<Instant>,
    deadline: Option<Instant>,
    ready: bool,
}
#[allow(dead_code)] // Pending C07b selection and C07c live submission preflight.
impl AutoscaleMember {
    pub(crate) fn member(&self) -> &Node {
        &self.member
    }
    pub(crate) fn binding(&self) -> Option<&HeartbeatBinding> {
        self.binding.as_ref()
    }
    pub(crate) fn heartbeat(&self) -> Option<&Heartbeat> {
        self.ready.then_some(self.heartbeat.as_ref()).flatten()
    }
    pub(crate) fn advanced_at(&self) -> Option<Instant> {
        self.ready.then_some(self.advanced_at).flatten()
    }
}
impl std::fmt::Debug for AutoscaleMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AutoscaleMember")
    }
}
#[allow(dead_code)] // Pending C07b/c and runtime; no live ownership authority.
pub(crate) struct AutoscaleSnapshot {
    observation: OwnershipObservation,
    boot: Uuid,
    owner: Owner,
    generation: u64,
    serial: u64,
    sample: OwnershipSample,
    members: Vec<AutoscaleMember>,
    low: Option<OwnershipSample>,
}
impl std::fmt::Debug for AutoscaleSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AutoscaleSnapshot")
    }
}
#[allow(dead_code)] // Pending C07b/c borrowed component observations.
impl AutoscaleSnapshot {
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(crate) fn controller_boot(&self) -> Uuid {
        self.boot
    }
    pub(crate) fn owner(&self) -> Owner {
        self.owner
    }
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
    pub(crate) fn serial(&self) -> u64 {
        self.serial
    }
    pub(crate) fn sample(&self) -> &OwnershipSample {
        &self.sample
    }
    pub(crate) fn members(&self) -> &[AutoscaleMember] {
        &self.members
    }
    pub(crate) fn low_anchor(&self) -> Option<&OwnershipSample> {
        self.low.as_ref()
    }
}
fn zero(h: &Heartbeat) -> bool {
    h.accepting
        && h.running_operations == 0
        && h.running_partitions == 0
        && h.queued_operations == 0
        && h.queued_partitions == 0
}
#[allow(dead_code)] // Pending C07b/c and the bounded coordinator runtime.
impl Observations {
    fn autoscale_reset(&mut self) -> Result<(), StorageError> {
        self.autoscale.low = None;
        for m in &mut self.autoscale.members {
            m.ready = false;
            m.deadline = None;
        }
        match self.autoscale.serial.checked_add(1) {
            Some(serial) if !self.exhausted => {
                self.autoscale.serial = serial;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(StorageError::LimitExceeded)
            }
        }
    }
    fn autoscale_deadlines(&mut self, now: Instant) -> Result<(), StorageError> {
        if self
            .autoscale
            .members
            .iter()
            .any(|m| m.deadline.is_some_and(|end| now >= end))
        {
            self.autoscale_reset()?;
        }
        Ok(())
    }
    fn autoscale_time(
        &mut self,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<OwnershipSample, StorageError> {
        let sample = match sample {
            Ok(s) => s,
            Err(e) => {
                self.autoscale_reset()?;
                return Err(e);
            }
        };
        self.time(sample.monotonic())?;
        let backwards = self
            .autoscale
            .last_sample
            .is_some_and(|old| sample.unix_ms() < old.unix_ms());
        let drift = self.autoscale.low.is_some_and(|anchor| {
            let mono = sample
                .monotonic()
                .checked_duration_since(anchor.monotonic());
            let wall = sample.unix_ms().checked_sub(anchor.unix_ms());
            match (mono, wall) {
                (Some(m), Some(w)) => {
                    m.as_nanos().abs_diff(u128::from(w) * 1_000_000) > 2_000_000_000
                }
                _ => true,
            }
        });
        if backwards || drift {
            self.autoscale_reset()?;
            return Err(StorageError::Unavailable);
        }
        self.autoscale.last_sample = Some(sample);
        Ok(sample)
    }
    fn autoscale_reconcile(&mut self, root: &Control) -> Result<(), StorageError> {
        use crate::cluster::member::{Action, IntentOutcome, IntentState};
        let terminal: std::collections::BTreeSet<_> = root
            .intents
            .iter()
            .filter(|i| {
                (i.action == Action::Start
                    && i.state == IntentState::Failed
                    && matches!(
                        i.outcome,
                        Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
                    ))
                    || (i.action == Action::Stop
                        && matches!(i.state, IntentState::Completed | IntentState::Failed)
                        && i.outcome == Some(IntentOutcome::Terminated))
            })
            .map(|i| (i.node_id, i.launch_id))
            .collect();
        let mut identity = root.clone();
        identity.revision = 0;
        identity.change_id = Uuid::nil();
        identity.scheduler.cooldown_until_ms = 0;
        identity.scheduler.low_since_ms = None;
        identity.scheduler.backoff_until_ms = 0;
        identity.scheduler.failures = 0;
        identity.controller.lease_until_ms = None;
        identity.intents.retain(|i| {
            matches!(
                i.state,
                IntentState::Pending | IntentState::InProgress | IntentState::Reconciling
            )
        });
        let changed = self.autoscale.identity.as_ref() != Some(&identity)
            || self.autoscale.terminal != terminal;
        let extension = self.controller.as_ref().is_some_and(|old| {
            old != &root.controller
                && old.owner_id == root.controller.owner_id
                && old.epoch == root.controller.epoch
                && old.lease_state == LeaseState::Held
                && root.controller.lease_state == LeaseState::Held
                && root.controller.lease_until_ms > old.lease_until_ms
        });
        if changed || (self.controller.as_ref() != Some(&root.controller) && !extension) {
            self.autoscale_reset()?;
            self.autoscale.members = root
                .nodes
                .iter()
                .filter(|n| {
                    !n.launch_id
                        .is_some_and(|l| terminal.contains(&(n.node_id, l)))
                })
                .map(|n| AutoscaleMember {
                    member: n.clone(),
                    binding: binding(root, n),
                    heartbeat: None,
                    advanced_at: None,
                    deadline: None,
                    ready: false,
                })
                .collect();
        } else if extension {
            // The later coordinator suspends issuance until matching ack + fresh
            // guard.check. Retain the anchor and OLD deadlines provisionally.
            for m in &mut self.autoscale.members {
                m.ready = false;
            }
        }
        self.autoscale.identity = Some(identity);
        self.autoscale.terminal = terminal;
        Ok(())
    }
    pub(crate) fn autoscale_failed(
        &mut self,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<(), StorageError> {
        self.autoscale_reset()?;
        self.autoscale_time(sample).map(|_| ())
    }
    pub(crate) fn autoscale_observe(
        &mut self,
        root: &Control,
        loaded: LoadedHeartbeat,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<(), StorageError> {
        let sample = self.autoscale_time(sample)?;
        self.reconcile_members(root, sample.monotonic())?;
        let b = loaded.binding();
        let h = loaded.heartbeat();
        let i = match self.index(b) {
            Ok(i) => i,
            Err(e) => {
                self.autoscale_reset()?;
                return Err(e);
            }
        };
        // Independent last complete loaded record also detects contradictions
        // after C03d has cleared its usable metrics.
        let a = self
            .autoscale
            .members
            .binary_search_by_key(&b.node_id, |m| m.member.node_id)
            .ok();
        if a.is_some_and(|a| {
            self.autoscale.members[a]
                .heartbeat
                .as_ref()
                .is_some_and(|old| old.sequence == h.sequence && old != h)
        }) {
            self.invalidate(false)?;
            return Err(StorageError::InvalidCatalog);
        }
        let advancing = self.entries[i].sequence.is_some_and(|seq| h.sequence > seq);
        self.observe_internal(b, h, sample.monotonic())?;
        if !zero(h) {
            self.autoscale_reset()?;
        }
        if let Some(a) = a {
            let expiry = Duration::from_millis(root.timing.expiry_ms);
            if advancing {
                let Some(deadline) = sample.monotonic().checked_add(expiry) else {
                    self.invalidate(true)?;
                    return Err(StorageError::LimitExceeded);
                };
                let m = &mut self.autoscale.members[a];
                m.heartbeat = Some(h.clone());
                m.advanced_at = Some(sample.monotonic());
                m.deadline = Some(deadline);
                m.ready = true;
            } else {
                // Retain the last complete loaded record for contradiction
                // checks, without restoring usable evidence or freshness.
                self.autoscale.members[a].heartbeat = Some(h.clone());
            }
        }
        if self.autoscale.low.is_none() && self.autoscale_low_eligible(root) {
            self.autoscale.low = Some(sample);
        }
        Ok(())
    }
    fn autoscale_low_eligible(&self, root: &Control) -> bool {
        use crate::cluster::{
            member::IntentState,
            state::{PolicyMode, Provisioning},
        };
        root.provisioning == Provisioning::Managed
            && root.policy.mode == PolicyMode::Autoscale
            && !self.autoscale.members.is_empty()
            && !root.intents.iter().any(|i| {
                matches!(
                    i.state,
                    IntentState::Pending | IntentState::InProgress | IntentState::Reconciling
                )
            })
            && self.autoscale.members.iter().all(|m| {
                m.member.state == NodeState::Ready
                    && m.member.registration == Registration::Verified
                    && m.member.drain.is_none()
                    && m.binding.is_some()
                    && m.ready
                    && m.heartbeat.as_ref().is_some_and(zero)
            })
    }
    pub(crate) fn autoscale_snapshot(
        &mut self,
        root: &Control,
        owner: Owner,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<AutoscaleSnapshot, StorageError> {
        let sample = self.autoscale_time(sample)?;
        self.reconcile_members(root, sample.monotonic())?;
        if owner.process_id != self.boot
            || root.controller.owner_id != Some(owner.process_id)
            || root.controller.epoch != owner.epoch
            || root.controller.lease_state != LeaseState::Held
        {
            self.autoscale_reset()?;
            return Err(StorageError::Conflict);
        }
        let members = self
            .autoscale
            .members
            .iter()
            .map(|m| AutoscaleMember {
                member: m.member.clone(),
                binding: m.binding.clone(),
                heartbeat: m.heartbeat.clone(),
                advanced_at: m.advanced_at,
                deadline: m.deadline,
                ready: m.ready,
            })
            .collect();
        Ok(AutoscaleSnapshot {
            observation: self.observation.clone().ok_or(StorageError::Conflict)?,
            boot: self.boot,
            owner,
            generation: self.generation,
            serial: self.autoscale.serial,
            sample,
            members,
            low: self
                .autoscale_low_eligible(root)
                .then_some(self.autoscale.low)
                .flatten(),
        })
    }
    pub(crate) fn matches_autoscale(
        &mut self,
        snapshot: &AutoscaleSnapshot,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<bool, StorageError> {
        let now = self.autoscale_time(sample)?;
        Ok(self.boot == snapshot.boot
            && self.generation == snapshot.generation
            && self.autoscale.serial == snapshot.serial
            && self.observation.as_ref() == Some(&snapshot.observation)
            && snapshot.sample.monotonic() <= now.monotonic()
            && snapshot.sample.unix_ms() <= now.unix_ms()
            && self.controller.as_ref().is_some_and(|c| {
                c.owner_id == Some(snapshot.owner.process_id)
                    && c.epoch == snapshot.owner.epoch
                    && c.lease_state == LeaseState::Held
            }))
    }
}

impl Observations {
    /// Status reports only an advancing, authenticated heartbeat that remains
    /// within its original freshness interval. Reads never refresh its age.
    pub(crate) fn status_heartbeat(
        &mut self,
        binding: &HeartbeatBinding,
        now: Instant,
        expiry: Duration,
    ) -> Result<Option<(Heartbeat, Duration)>, StorageError> {
        if !self.fresh(binding, now, expiry)? {
            return Ok(None);
        }
        let entry = &self.entries[self.index(binding)?];
        Ok(entry
            .heartbeat
            .clone()
            .zip(entry.advance)
            .map(|(h, at)| (h, now.saturating_duration_since(at))))
    }
}
