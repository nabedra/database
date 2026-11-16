//! Bounded local owners and the common receiver state used by payload installation.

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // R04b/c will consume these local refusal reasons.
pub(super) enum Refusal {
    Invalid,
    Ineligible,
    Closed,
    Full,
    Conflict,
    Exhausted,
    Stale,
}

#[derive(Debug)]
#[allow(dead_code)] // Capacity is embedded in the pending receiver owner.
struct Capacity {
    workers: usize,
    queue: usize,
}
#[allow(dead_code)] // R04b/c construction is pending.
impl Capacity {
    fn new(workers: usize, queue: usize) -> Result<Self, Refusal> {
        if !(1..=1024).contains(&workers) || queue > 4096 {
            return Err(Refusal::Invalid);
        }
        Ok(Self { workers, queue })
    }
}

use super::auth::{DependencyBinding, Readiness, ReceiverIdentity, Sender, WorkerSigningKey};
use crate::cluster::controller::OwnershipObservation;
use std::sync::Arc;
use uuid::Uuid;

const DEPENDENCIES: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
#[allow(dead_code)] // R04c copies only checked DependencyBinding getters.
pub(super) struct Binding {
    coordinator: Arc<Sender>,
    operation: Uuid,
    admission: Uuid,
    digest: [u8; 32],
}
impl std::fmt::Debug for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Binding").finish_non_exhaustive()
    }
}
#[allow(dead_code)] // Checked binding adapter for R04c.
impl From<&DependencyBinding> for Binding {
    fn from(b: &DependencyBinding) -> Self {
        Self {
            coordinator: Arc::new(b.coordinator().clone()),
            operation: b.operation_id(),
            admission: b.admission_id(),
            digest: *b.request_digest(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // The future common registry retains these exact identities.
pub(super) enum Kind {
    Operation,
    Partition,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Monotonic generations protect reused fixed positions.
pub(super) struct WorkKey {
    kind: Kind,
    slot: usize,
    generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Dependency tickets never identify a slot alone.
pub(super) struct DependencyKey {
    slot: usize,
    generation: u64,
}
#[derive(Debug)]
#[allow(dead_code)] // Move-only use receipt; Drop never releases the retained record.
pub(super) struct DependencyUse {
    key: DependencyKey,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Charges survive every phase until reaping.
enum WorkPhase {
    Queued,
    RunningReserved,
    Executing,
    Settling,
    Cleanup,
    Reapable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Settlement must preserve the original running/queued charge.
enum Charge {
    Running,
    Queued,
}
#[derive(Debug)]
#[allow(dead_code)] // Installed owner, independent of observer/ticket lifetime.
struct WorkRecord {
    key: WorkKey,
    binding: Binding,
    dependency: DependencyUse,
    job: Option<(Uuid, Uuid)>,
    phase: WorkPhase,
    charge: Charge,
}
#[derive(Debug)]
#[allow(dead_code)] // The dependency owner survives close, delivery and cleanup.
struct DependencyRecord {
    key: DependencyKey,
    binding: Binding,
    permit: Uuid,
    closing: bool,
    uses: usize,
    operation: Option<WorkKey>,
    payload_account: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Permanent per-boot barrier; no reopen transition exists.
enum Barrier {
    Open,
    Draining(Uuid),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // R04b ProcessReady + R07 InitializedWorker must complete this identity.
enum InitState {
    Uninitialized { identity: Uuid },
    Initialized { identity: Uuid },
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(dead_code)] // Derived from records, never supplied by a caller.
struct Occupancy {
    running: usize,
    queued: usize,
}
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // Local evidence only; no authenticated idle/drain receipt.
pub(super) struct LocalSnapshot {
    operations: Occupancy,
    partitions: Occupancy,
    dependencies: usize,
    closing: usize,
    initialized: bool,
    barrier: Barrier,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Issued from typed durable checks by R04c, not listener booleans.
enum Eligibility {
    Registered,
    Ready,
    Draining(Uuid),
    RegisteredDraining(Uuid),
    Denied,
}
/// Constructed privately from the original eligible request and checked root.
#[allow(dead_code)] // R04c2 observes this during its final transaction.
pub(super) struct DurableView {
    boot: Arc<ReceiverIdentity>,
    observation: OwnershipObservation,
    local: Eligibility,
    coordinators: Vec<Arc<Sender>>,
}
/// No Clone or caller-visible constructor. R04c2 owns retained-start issuance.
#[allow(dead_code)] // R04c will issue the receipt outside the receiver lock.
pub(super) struct StartCheck {
    key: WorkKey,
    boot: Arc<ReceiverIdentity>,
    observation: OwnershipObservation,
    binding: Binding,
    dependency: DependencyKey,
}
#[derive(Debug)]
#[allow(dead_code)] // Actual R04b registered handles consume tickets, never their Drop.
pub(super) struct WorkTicket {
    key: WorkKey,
}
#[derive(Debug)]
#[allow(dead_code)] // Execution dispatch is intentionally absent in mini 1.
pub(super) struct StartTicket {
    key: WorkKey,
}
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // Closed on absence is not evidence of historical remote completion.
pub(super) enum Close {
    Pending,
    Closed,
}

#[allow(dead_code)] // R04b will embed this directly under the ONE ReceiverState mutex.
pub(super) struct LocalAdmission {
    boot: Arc<ReceiverIdentity>,
    coordinator: Arc<Sender>,
    init: InitState,
    barrier: Barrier,
    operation_capacity: Capacity,
    partition_capacity: Capacity,
    operations: Vec<Option<WorkRecord>>,
    partitions: Vec<Option<WorkRecord>>,
    dependencies: Vec<Option<DependencyRecord>>,
    generation: u64,
    evidence: Arc<WorkEvidence>,
    durable: Option<DurableView>,
    integrity_failed: bool,
}
#[allow(dead_code)] // Concrete local transitions awaiting R04b/c common-lock callers.
impl LocalAdmission {
    pub(super) fn permit_for_binding(&self, binding: &Binding) -> Option<Uuid> {
        self.dependencies
            .iter()
            .flatten()
            .find(|d| &d.binding == binding)
            .map(|d| d.permit)
    }
    pub(super) fn new(
        cluster: Uuid,
        key: &WorkerSigningKey,
        coordinator: Arc<Sender>,
        initialization: Uuid,
        operation_workers: usize,
        partition_workers: usize,
        queue: usize,
    ) -> Result<Self, Refusal> {
        if operation_workers > 256 || partition_workers > 256 {
            return Err(Refusal::Invalid);
        }
        Self::new_with_job_slots(
            cluster,
            key,
            coordinator,
            initialization,
            operation_workers,
            partition_workers,
            queue,
        )
    }
    pub(super) fn new_with_job_slots(
        cluster: Uuid,
        key: &WorkerSigningKey,
        coordinator: Arc<Sender>,
        initialization: Uuid,
        operation_workers: usize,
        partition_workers: usize,
        queue: usize,
    ) -> Result<Self, Refusal> {
        if operation_workers > 256 {
            return Err(Refusal::Invalid);
        }
        let operation_capacity = Capacity::new(operation_workers, queue)?;
        let partition_capacity = Capacity::new(partition_workers, queue)?;
        if coordinator.public_key().map_err(|_| Refusal::Invalid)? != key.public_key() {
            return Err(Refusal::Conflict);
        }
        let boot = ReceiverIdentity::new(
            cluster,
            coordinator.node_id().ok_or(Refusal::Invalid)?,
            coordinator.incarnation().ok_or(Refusal::Invalid)?,
            coordinator.session_id().ok_or(Refusal::Invalid)?,
            key,
        );
        Ok(Self {
            boot: Arc::new(boot),
            coordinator,
            init: InitState::Uninitialized {
                identity: initialization,
            },
            barrier: Barrier::Open,
            operation_capacity,
            partition_capacity,
            operations: (0..operation_workers + queue).map(|_| None).collect(),
            partitions: (0..partition_workers + queue).map(|_| None).collect(),
            dependencies: (0..DEPENDENCIES).map(|_| None).collect(),
            generation: 0,
            evidence: Arc::new(WorkEvidence::default()),
            durable: None,
            integrity_failed: false,
        })
    }
    fn initialized(&self) -> bool {
        matches!(self.init, InitState::Initialized { .. })
    }
    pub(super) fn observe(&mut self, view: DurableView) -> Result<(), Refusal> {
        if self.integrity_failed {
            return Err(Refusal::Ineligible);
        }
        if !self.boot.same_boot(&view.boot) {
            return Err(Refusal::Stale);
        }
        if let Some(old) = &self.durable
            && (view.observation.revision() < old.observation.revision()
                || (view.observation.revision() == old.observation.revision()
                    && view.observation != old.observation))
        {
            if self
                .durable
                .as_ref()
                .is_some_and(|old| view.observation.revision() == old.observation.revision())
            {
                self.evidence.record();
                self.integrity_failed = true;
            }
            return Err(Refusal::Stale);
        }
        if let Eligibility::Draining(id) | Eligibility::RegisteredDraining(id) = view.local {
            self.observe_drain(&view.boot, id)?;
        }
        if self.durable.as_ref().is_none_or(|old| {
            old.observation != view.observation
                || old.local != view.local
                || old.coordinators != view.coordinators
        }) {
            self.evidence.admit()?;
            self.durable = Some(view);
        }
        Ok(())
    }
    pub(super) fn observe_drain(
        &mut self,
        boot: &ReceiverIdentity,
        id: Uuid,
    ) -> Result<(), Refusal> {
        if !self.boot.same_boot(boot) {
            return Err(Refusal::Stale);
        }
        match self.barrier {
            Barrier::Open => {
                self.evidence.admit()?;
                self.barrier = Barrier::Draining(id);
                Ok(())
            }
            Barrier::Draining(old) if old == id => Ok(()),
            _ => Err(Refusal::Conflict),
        }
    }
    fn eligible(&self, binding: Option<&Binding>, fresh: bool) -> Result<(), Refusal> {
        let v = self.durable.as_ref().ok_or(Refusal::Ineligible)?;
        if self.integrity_failed
            || !self.initialized()
            || !matches!(v.local, Eligibility::Ready | Eligibility::Draining(_))
        {
            return Err(Refusal::Ineligible);
        }
        if fresh && (v.local != Eligibility::Ready || self.barrier != Barrier::Open) {
            return Err(Refusal::Closed);
        }
        if let Some(b) = binding
            && !v.coordinators.contains(&b.coordinator)
            // Verified local eligibility also proves this boot as coordinator.
            && b.coordinator != self.coordinator
        {
            return Err(Refusal::Ineligible);
        }
        Ok(())
    }
    fn table(&self, kind: Kind) -> &[Option<WorkRecord>] {
        match kind {
            Kind::Operation => &self.operations,
            Kind::Partition => &self.partitions,
        }
    }
    fn table_mut(&mut self, kind: Kind) -> &mut [Option<WorkRecord>] {
        match kind {
            Kind::Operation => &mut self.operations,
            Kind::Partition => &mut self.partitions,
        }
    }
    fn capacity(&self, kind: Kind) -> &Capacity {
        match kind {
            Kind::Operation => &self.operation_capacity,
            Kind::Partition => &self.partition_capacity,
        }
    }
    fn occupancy(&self, kind: Kind) -> Occupancy {
        let mut n = Occupancy::default();
        for r in self.table(kind).iter().flatten() {
            match r.charge {
                Charge::Running => n.running += 1,
                Charge::Queued => n.queued += 1,
            }
        }
        n
    }
    fn room(&self, kind: Kind) -> Result<(usize, Charge), Refusal> {
        let n = self.occupancy(kind);
        let c = self.capacity(kind);
        let charge = if n.running < c.workers {
            Charge::Running
        } else if n.queued < c.queue {
            Charge::Queued
        } else {
            return Err(Refusal::Full);
        };
        let slot = self
            .table(kind)
            .iter()
            .position(Option::is_none)
            .ok_or(Refusal::Full)?;
        Ok((slot, charge))
    }
    fn next_generation(&self, count: u64) -> Result<u64, Refusal> {
        self.generation.checked_add(count).ok_or(Refusal::Exhausted)
    }
    fn dependency(&self, key: DependencyKey) -> Result<&DependencyRecord, Refusal> {
        self.dependencies
            .get(key.slot)
            .and_then(Option::as_ref)
            .filter(|d| d.key == key)
            .ok_or(Refusal::Stale)
    }
    fn find_dependency(&self, binding: &Binding, permit: Uuid) -> Result<DependencyKey, Refusal> {
        let d = self
            .dependencies
            .iter()
            .flatten()
            .find(|d| d.permit == permit)
            .ok_or(Refusal::Stale)?;
        if &d.binding != binding {
            return Err(Refusal::Conflict);
        }
        Ok(d.key)
    }
    pub(super) fn reserve_operation(
        &mut self,
        binding: Binding,
        admission: Uuid,
        permit: Uuid,
    ) -> Result<WorkTicket, Refusal> {
        self.eligible(Some(&binding), true)?;
        if binding.coordinator != self.coordinator || binding.admission != admission {
            return Err(Refusal::Conflict);
        }
        if self
            .dependencies
            .iter()
            .flatten()
            .any(|d| d.permit == permit || d.binding.admission == admission)
        {
            return Err(Refusal::Conflict);
        }
        let (slot, charge) = self.room(Kind::Operation)?;
        let dep_slot = self
            .dependencies
            .iter()
            .position(Option::is_none)
            .ok_or(Refusal::Full)?;
        self.evidence.admit()?;
        let generation = self.next_generation(2)?;
        let key = WorkKey {
            kind: Kind::Operation,
            slot,
            generation,
        };
        let dep = DependencyKey {
            slot: dep_slot,
            generation: generation - 1,
        };
        self.dependencies[dep_slot] = Some(DependencyRecord {
            key: dep,
            binding: binding.clone(),
            permit,
            closing: false,
            uses: 0,
            operation: Some(key),
            payload_account: false,
        });
        self.operations[slot] = Some(Self::record(key, binding, dep, None, charge));
        self.generation = generation;
        Ok(WorkTicket { key })
    }
    fn record(
        key: WorkKey,
        binding: Binding,
        dependency: DependencyKey,
        job: Option<(Uuid, Uuid)>,
        charge: Charge,
    ) -> WorkRecord {
        WorkRecord {
            key,
            binding,
            dependency: DependencyUse { key: dependency },
            job,
            charge,
            phase: if charge == Charge::Running {
                WorkPhase::RunningReserved
            } else {
                WorkPhase::Queued
            },
        }
    }
    pub(super) fn open_dependency(
        &mut self,
        binding: Binding,
        permit: Uuid,
    ) -> Result<DependencyKey, Refusal> {
        if let Some(d) = self
            .dependencies
            .iter()
            .flatten()
            .find(|d| d.permit == permit)
        {
            if d.binding != binding {
                return Err(Refusal::Conflict);
            }
            if d.closing {
                return Err(Refusal::Closed);
            }
            self.eligible(Some(&binding), false)?;
            return Ok(d.key);
        }
        self.eligible(Some(&binding), true)?;
        let slot = self
            .dependencies
            .iter()
            .position(Option::is_none)
            .ok_or(Refusal::Full)?;
        self.evidence.admit()?;
        let generation = self.next_generation(1)?;
        let key = DependencyKey { slot, generation };
        self.dependencies[slot] = Some(DependencyRecord {
            key,
            binding,
            permit,
            closing: false,
            uses: 0,
            operation: None,
            payload_account: false,
        });
        self.generation = generation;
        Ok(key)
    }
    pub(super) fn reserve_partition(
        &mut self,
        binding: Binding,
        permit: Uuid,
        job: Uuid,
        attempt: Uuid,
    ) -> Result<WorkTicket, Refusal> {
        self.eligible(Some(&binding), false)?;
        let dep = self.find_dependency(&binding, permit)?;
        let d = self.dependency(dep)?;
        if d.closing {
            return Err(Refusal::Closed);
        }
        if self
            .partitions
            .iter()
            .flatten()
            .any(|r| r.binding == binding && r.job == Some((job, attempt)))
        {
            return Err(Refusal::Conflict);
        }
        let uses = d.uses.checked_add(1).ok_or(Refusal::Exhausted)?;
        let (slot, charge) = self.room(Kind::Partition)?;
        self.evidence.admit()?;
        let generation = self.next_generation(1)?;
        let key = WorkKey {
            kind: Kind::Partition,
            slot,
            generation,
        };
        self.partitions[slot] = Some(Self::record(
            key,
            binding,
            dep,
            Some((job, attempt)),
            charge,
        ));
        self.dependencies[dep.slot]
            .as_mut()
            .expect("checked dependency")
            .uses = uses;
        self.generation = generation;
        Ok(WorkTicket { key })
    }
    fn work(&self, key: WorkKey) -> Result<&WorkRecord, Refusal> {
        self.table(key.kind)
            .get(key.slot)
            .and_then(Option::as_ref)
            .filter(|r| r.key == key)
            .ok_or(Refusal::Stale)
    }
    fn work_mut(&mut self, key: WorkKey) -> Result<&mut WorkRecord, Refusal> {
        self.table_mut(key.kind)
            .get_mut(key.slot)
            .and_then(Option::as_mut)
            .filter(|r| r.key == key)
            .ok_or(Refusal::Stale)
    }
    pub(super) fn next_start(
        &mut self,
        kind: Kind,
        check: StartCheck,
    ) -> Result<StartTicket, Refusal> {
        let r = self.work(check.key)?;
        self.eligible(Some(&r.binding), false)?;
        let v = self.durable.as_ref().ok_or(Refusal::Ineligible)?;
        if kind != check.key.kind
            || !self.boot.same_boot(&check.boot)
            || check.observation != v.observation
            || check.binding != r.binding
            || check.dependency != r.dependency.key
        {
            return Err(Refusal::Stale);
        }
        self.dependency(r.dependency.key)?; // Closing preserves this accepted use.
        let queued = r.phase == WorkPhase::Queued;
        if !queued && r.phase != WorkPhase::RunningReserved {
            return Err(Refusal::Conflict);
        }
        if queued && self.occupancy(kind).running >= self.capacity(kind).workers {
            return Err(Refusal::Full);
        }
        self.evidence.admit()?;
        let r = self.work_mut(check.key)?;
        r.charge = Charge::Running;
        r.phase = WorkPhase::Executing;
        Ok(StartTicket { key: check.key })
    }
    fn begin_close(&mut self, binding: &Binding, permit: Uuid) -> Result<Close, Refusal> {
        self.evidence.record();
        if !self
            .dependencies
            .iter()
            .flatten()
            .any(|d| d.permit == permit)
        {
            return Ok(Close::Closed);
        }
        let key = self.find_dependency(binding, permit)?;
        self.dependencies[key.slot]
            .as_mut()
            .expect("checked dependency")
            .closing = true;
        Ok(if self.reap_dependency(key) {
            Close::Closed
        } else {
            Close::Pending
        })
    }
    fn reap_dependency(&mut self, key: DependencyKey) -> bool {
        let d = self.dependencies[key.slot]
            .as_ref()
            .expect("retained dependency");
        if d.closing && d.uses == 0 && d.operation.is_none() && !d.payload_account {
            self.evidence.record();
            self.dependencies[key.slot] = None;
            true
        } else {
            false
        }
    }
    /// Called by registered handle integration after execution actually settles.
    /// Also handles a queued cancellation whose dispatch gate has settled.
    fn execution_settled(&mut self, key: WorkKey) -> Result<(), Refusal> {
        self.evidence.record();
        let r = self.work_mut(key)?;
        if !matches!(
            r.phase,
            WorkPhase::Queued | WorkPhase::RunningReserved | WorkPhase::Executing
        ) {
            return Err(Refusal::Conflict);
        }
        r.phase = WorkPhase::Settling;
        Ok(())
    }
    /// Publication/reply delivery settled; worker disposal may now begin.
    fn delivery_settled(&mut self, key: WorkKey) -> Result<(), Refusal> {
        self.evidence.record();
        let r = self.work_mut(key)?;
        if r.phase != WorkPhase::Settling {
            return Err(Refusal::Conflict);
        }
        r.phase = WorkPhase::Cleanup;
        if key.kind == Kind::Operation {
            let dep = r.dependency.key;
            self.dependencies[dep.slot]
                .as_mut()
                .expect("retained self")
                .closing = true;
        }
        Ok(())
    }
    fn cleanup_settled(&mut self, key: WorkKey) -> Result<(), Refusal> {
        self.evidence.record();
        let r = self.work_mut(key)?;
        if r.phase != WorkPhase::Cleanup {
            return Err(Refusal::Conflict);
        }
        r.phase = WorkPhase::Reapable;
        Ok(())
    }
    /// Only actual registered handle termination may drive this transition.
    fn reap(&mut self, key: WorkKey) -> Result<(), Refusal> {
        self.evidence.record();
        let r = self.work(key)?;
        if r.phase != WorkPhase::Reapable {
            return Err(Refusal::Conflict);
        }
        let dep = r.dependency.key;
        self.dependency(dep)?;
        self.table_mut(key.kind)[key.slot] = None;
        let d = self.dependencies[dep.slot]
            .as_mut()
            .expect("retained dependency");
        match key.kind {
            Kind::Operation => d.operation = None,
            Kind::Partition => d.uses -= 1,
        }
        self.reap_dependency(dep);
        Ok(())
    }
    fn readiness(&self, boot: &ReceiverIdentity, challenge: &Readiness) -> Result<Uuid, Refusal> {
        if !self.boot.same_boot(boot) {
            return Err(Refusal::Stale);
        }
        let v = self.durable.as_ref().ok_or(Refusal::Ineligible)?;
        if self.integrity_failed
            || !self.initialized()
            || self.barrier != Barrier::Open
            || !matches!(v.local, Eligibility::Registered | Eligibility::Ready)
        {
            return Err(Refusal::Ineligible);
        }
        Ok(challenge.challenge())
    }
    pub(super) fn snapshot(&self) -> LocalSnapshot {
        LocalSnapshot {
            operations: self.occupancy(Kind::Operation),
            partitions: self.occupancy(Kind::Partition),
            dependencies: self.dependencies.iter().flatten().count(),
            closing: self
                .dependencies
                .iter()
                .flatten()
                .filter(|d| d.closing)
                .count(),
            initialized: self.initialized(),
            barrier: self.barrier,
        }
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::{
        cluster::{
            configuration::{Template, Timing},
            member::{NodeState, Registration},
            state::Control,
        },
        node::auth::{ActualRoute, Auth, WorkerSigningKey, worker_verifier},
    };
    use serde_json::json;
    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }
    fn probe(n: u128) -> Readiness {
        Readiness::decode(&serde_json::to_vec(&json!({"version":1,"challenge":id(n)})).unwrap())
            .unwrap()
    }
    fn hex(b: &[u8]) -> String {
        b.iter().map(|b| format!("{b:02x}")).collect()
    }
    pub(super) struct Fixture {
        pub(super) key: WorkerSigningKey,
        root: Control,
        pub(super) sender: Arc<Sender>,
        peers: Vec<Arc<Sender>>,
    }
    impl Fixture {
        pub(super) fn new() -> Self {
            let key = WorkerSigningKey::new().unwrap();
            let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
            let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
                .unwrap()
                .fingerprint()
                .unwrap();
            let value = json!({"version":1,"cluster_id":id(1),"name":"Synthetic","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":hex(&worker_verifier(id(1),id(3),id(4),id(5),&key.public_key()))},"drain":null}],"intents":[]});
            let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
            let sender = Arc::new(Self::sender(&key, id(3), id(4), id(5)));
            Self {
                key,
                root,
                sender,
                peers: vec![],
            }
        }
        fn sender(key: &WorkerSigningKey, node: Uuid, incarnation: Uuid, session: Uuid) -> Sender {
            let text = format!(
                r#"{{"version":1,"cluster_id":"{}","scope":"dependency_open","sender":{{"role":"worker","process_id":"{incarnation}","node_id":"{node}","incarnation":"{incarnation}","session_id":"{session}","public_key":"{}"}},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":1,"method":"POST","path":"/internal/v1/dependencies/open","operation_id":"{}","admission_id":"{}","job_id":null,"attempt_id":null,"request_digest":"{}","control_sha256":"{}","dependency_id":null}}"#,
                id(1),
                hex(&key.public_key()),
                id(3),
                id(4),
                id(5),
                id(6),
                id(7),
                "a".repeat(64),
                "b".repeat(64)
            );
            Auth::decode(
                &hex(text.as_bytes()),
                &ActualRoute::new("POST", "/internal/v1/dependencies/open", None).unwrap(),
            )
            .unwrap()
            .sender()
            .clone()
        }
        fn add_peer(&mut self, n: u128) -> Binding {
            let key = WorkerSigningKey::new().unwrap();
            let sender = Arc::new(Self::sender(&key, id(33), id(44), id(55)));
            let mut node = self.root.nodes[0].clone();
            node.node_id = id(33);
            node.incarnation = Some(id(44));
            node.resource_id = Some(format!("external:{}", id(33)));
            node.session = Some(crate::cluster::member::Session {
                id: id(55),
                verifier: hex(&worker_verifier(
                    id(1),
                    id(33),
                    id(44),
                    id(55),
                    &key.public_key(),
                )),
            });
            self.root.nodes.push(node);
            self.peers.push(Arc::clone(&sender));
            Binding {
                coordinator: sender,
                ..self.binding(n)
            }
        }
        fn boot(&self) -> ReceiverIdentity {
            ReceiverIdentity::new(id(1), id(3), id(4), id(5), &self.key)
        }
        // Private stand-in for R04c's issuer. Uses validated Control/observation,
        // exact member key commitments and concrete eligibility, never raw auth proof fields.
        pub(super) fn view(&self) -> DurableView {
            let root = Control::decode(&self.root.encode().unwrap()).unwrap();
            let current = |sender: &Sender| {
                root.nodes.iter().find(|n| {
                    n.node_id == sender.node_id().unwrap()
                        && n.incarnation == sender.incarnation()
                        && n.session.as_ref().is_some_and(|s| {
                            s.id == sender.session_id().unwrap()
                                && s.verifier
                                    == hex(&worker_verifier(
                                        root.cluster_id,
                                        n.node_id,
                                        n.incarnation.unwrap(),
                                        s.id,
                                        &sender.public_key().unwrap(),
                                    ))
                        })
                        && ((n.registration == Registration::Verified
                            && matches!(n.state, NodeState::Ready | NodeState::Draining))
                            || (n.registration == Registration::Registered
                                && n.state == NodeState::Starting))
                })
            };
            let local = match current(&self.sender) {
                Some(n) if n.state == NodeState::Ready => Eligibility::Ready,
                Some(n) if n.state == NodeState::Starting => Eligibility::Registered,
                Some(n) => Eligibility::Draining(n.drain.as_ref().unwrap().id),
                None => Eligibility::Denied,
            };
            DurableView {
                boot: Arc::new(self.boot()),
                observation: OwnershipObservation::new(&root).unwrap(),
                local,
                coordinators: std::iter::once(&self.sender)
                    .chain(&self.peers)
                    .filter(|sender| {
                        current(sender).is_some_and(|n| n.state != NodeState::Starting)
                    })
                    .cloned()
                    .collect(),
            }
        }
        fn core(&self, operations: usize, partitions: usize, queue: usize) -> LocalAdmission {
            let mut a = LocalAdmission::new(
                id(1),
                &self.key,
                self.sender.clone(),
                id(30),
                operations,
                partitions,
                queue,
            )
            .unwrap();
            a.observe(self.view()).unwrap();
            self.initialize(&mut a, id(30)).unwrap();
            a
        }
        // Only fixtures may supply the absent ProcessReady/InitializedWorker half.
        fn initialize(&self, a: &mut LocalAdmission, identity: Uuid) -> Result<(), Refusal> {
            if !a.boot.same_boot(&self.boot()) || a.init != (InitState::Uninitialized { identity })
            {
                return Err(Refusal::Stale);
            }
            a.init = InitState::Initialized { identity };
            Ok(())
        }
        fn binding(&self, n: u128) -> Binding {
            Binding {
                coordinator: self.sender.clone(),
                operation: id(n),
                admission: id(n + 10000),
                digest: [7; 32],
            }
        }
        fn check(&self, a: &LocalAdmission, ticket: &WorkTicket) -> StartCheck {
            let r = a.work(ticket.key).unwrap();
            let view = self.view();
            StartCheck {
                key: ticket.key,
                boot: view.boot,
                observation: view.observation,
                binding: r.binding.clone(),
                dependency: r.dependency.key,
            }
        }
    }
    fn settle(a: &mut LocalAdmission, t: WorkTicket) {
        a.execution_settled(t.key).unwrap();
        a.delivery_settled(t.key).unwrap();
        a.cleanup_settled(t.key).unwrap();
        a.reap(t.key).unwrap();
    }
    fn op(a: &mut LocalAdmission, f: &Fixture, n: u128) -> WorkTicket {
        a.reserve_operation(f.binding(n), id(n + 10000), id(n + 20000))
            .unwrap()
    }
    #[test]
    fn distributed_container_has_configured_ownership_tables() {
        let f = Fixture::new();
        for (o, p, q) in [(2, 3, 4), (256, 256, 4096)] {
            let state = ReceiverState::distributed(f.core(o, p, q));
            assert!(
                !state.legacy_mode(),
                "distributed boot must close every legacy path"
            );
            assert_eq!(
                state.payload.bounds(),
                (1 + o + q + 4096 + 48, o + q + p + q + 64)
            );
            let (payload, records) = state.payload.bounds();
            assert!(payload <= 8497 && records <= 8768);
        }
    }
    #[test]
    fn separate_worker_bounds_and_initialization_identity() {
        let f = Fixture::new();
        for (o, p, q) in [(0, 1, 0), (1, 0, 0), (257, 1, 0), (1, 257, 0), (1, 1, 4097)] {
            assert!(matches!(
                LocalAdmission::new(id(1), &f.key, f.sender.clone(), id(30), o, p, q),
                Err(Refusal::Invalid)
            ));
        }
        let other_key = WorkerSigningKey::new().unwrap();
        assert!(matches!(
            LocalAdmission::new(id(1), &other_key, f.sender.clone(), id(30), 1, 1, 0),
            Err(Refusal::Conflict)
        ));
        for (o, p) in [(1, 256), (256, 1)] {
            let a = f.core(o, p, 4096);
            assert_eq!(a.operations.len(), o + 4096);
            assert_eq!(a.partitions.len(), p + 4096);
        }
        let mut a = LocalAdmission::new(id(1), &f.key, f.sender.clone(), id(30), 1, 1, 0).unwrap();
        a.observe(f.view()).unwrap();
        assert_eq!(a.readiness(&f.boot(), &probe(42)), Err(Refusal::Ineligible));
        assert!(matches!(
            a.reserve_operation(f.binding(40), id(50), id(60)),
            Err(Refusal::Ineligible)
        ));
        assert_eq!(f.initialize(&mut a, id(31)), Err(Refusal::Stale));
        f.initialize(&mut a, id(30)).unwrap();
        assert_eq!(f.initialize(&mut a, id(30)), Err(Refusal::Stale));
        assert_eq!(a.readiness(&f.boot(), &probe(42)), Ok(id(42)));
        let mut f = f;
        f.root.revision += 1;
        f.root.change_id = id(43);
        f.root.nodes[0].state = NodeState::Starting;
        f.root.nodes[0].registration = Registration::Registered;
        a.observe(f.view()).unwrap();
        assert_eq!(a.readiness(&f.boot(), &probe(42)), Ok(id(42)));
        assert!(matches!(
            a.reserve_operation(f.binding(40), id(10040), id(20040)),
            Err(Refusal::Ineligible)
        ));
    }
    #[test]
    fn independent_running_queue_saturation_and_reaping() {
        let f = Fixture::new();
        for queue in [0, 2] {
            let mut a = f.core(1, 2, queue);
            let mut operations = vec![op(&mut a, &f, 100)];
            for n in 0..queue {
                operations.push(op(&mut a, &f, 101 + n as u128));
            }
            assert!(matches!(
                a.reserve_operation(f.binding(200), id(10200), id(20200)),
                Err(Refusal::Full)
            ));
            let b = f.binding(300);
            a.open_dependency(b.clone(), id(20300)).unwrap();
            let mut jobs = Vec::new();
            for n in 0..2 + queue {
                jobs.push(
                    a.reserve_partition(b.clone(), id(20300), id(400 + n as u128), id(500))
                        .unwrap(),
                );
            }
            assert_eq!(
                a.snapshot().operations,
                Occupancy {
                    running: 1,
                    queued: queue
                }
            );
            assert_eq!(
                a.snapshot().partitions,
                Occupancy {
                    running: 2,
                    queued: queue
                }
            );
            assert!(matches!(
                a.reserve_partition(b.clone(), id(20300), id(600), id(500)),
                Err(Refusal::Full)
            ));
            let first = operations.remove(0);
            a.execution_settled(first.key).unwrap();
            a.delivery_settled(first.key).unwrap();
            a.cleanup_settled(first.key).unwrap();
            assert!(matches!(
                a.reserve_operation(f.binding(200), id(10200), id(20200)),
                Err(Refusal::Full)
            ));
            a.reap(first.key).unwrap();
            let replacement = op(&mut a, &f, 200);
            let job = jobs.remove(0);
            a.execution_settled(job.key).unwrap();
            a.delivery_settled(job.key).unwrap();
            a.cleanup_settled(job.key).unwrap();
            assert!(matches!(
                a.reserve_partition(b.clone(), id(20300), id(600), id(500)),
                Err(Refusal::Full)
            ));
            a.reap(job.key).unwrap();
            jobs.push(
                a.reserve_partition(b.clone(), id(20300), id(600), id(500))
                    .unwrap(),
            );
            for t in operations.into_iter().chain([replacement]) {
                settle(&mut a, t);
            }
            assert_eq!(a.begin_close(&b, id(20300)), Ok(Close::Pending));
            for t in jobs {
                settle(&mut a, t);
            }
            assert_eq!(a.snapshot().dependencies, 0);
        }
    }
    #[test]
    fn full_dependency_table_includes_self_and_closing_atomic_rollback() {
        let f = Fixture::new();
        let mut a = f.core(2, 1, 0);
        let operation = op(&mut a, &f, 1);
        let b = f.binding(2);
        a.open_dependency(b.clone(), id(20002)).unwrap();
        let job = a
            .reserve_partition(b.clone(), id(20002), id(8), id(9))
            .unwrap();
        assert_eq!(a.begin_close(&b, id(20002)), Ok(Close::Pending));
        for n in 3..=4096 {
            a.open_dependency(f.binding(n), id(n + 20000)).unwrap();
        }
        assert_eq!(a.snapshot().dependencies, DEPENDENCIES);
        let before = a.snapshot();
        let generation = a.generation;
        assert!(matches!(
            a.reserve_operation(f.binding(5000), id(15000), id(25000)),
            Err(Refusal::Full)
        ));
        assert_eq!(a.snapshot(), before);
        assert_eq!(a.generation, generation);
        assert!(matches!(
            a.open_dependency(f.binding(5000), id(25000)),
            Err(Refusal::Full)
        ));
        settle(&mut a, job);
        let extra = op(&mut a, &f, 5000);
        settle(&mut a, operation);
        settle(&mut a, extra);
        for n in 3..=4096 {
            assert_eq!(
                a.begin_close(&f.binding(n), id(n + 20000)),
                Ok(Close::Closed)
            );
        }
        assert_eq!(a.snapshot().dependencies, 0);
    }
    #[test]
    fn exact_retry_conflicts_and_close_retains_delivery_cleanup() {
        let f = Fixture::new();
        let mut a = f.core(1, 1, 0);
        let b = f.binding(80);
        let key = a.open_dependency(b.clone(), id(90)).unwrap();
        assert_eq!(a.open_dependency(b.clone(), id(90)), Ok(key));
        for changed in [
            Binding {
                digest: [9; 32],
                ..b.clone()
            },
            Binding {
                admission: id(91),
                ..b.clone()
            },
            Binding {
                operation: id(92),
                ..b.clone()
            },
            Binding {
                coordinator: Arc::new(Fixture::sender(
                    &WorkerSigningKey::new().unwrap(),
                    id(3),
                    id(4),
                    id(5),
                )),
                ..b.clone()
            },
        ] {
            assert_eq!(
                a.open_dependency(changed.clone(), id(90)),
                Err(Refusal::Conflict)
            );
            assert_eq!(a.begin_close(&changed, id(90)), Err(Refusal::Conflict));
        }
        for sender in [
            Fixture::sender(&f.key, id(33), id(4), id(5)),
            Fixture::sender(&f.key, id(3), id(44), id(5)),
            Fixture::sender(&f.key, id(3), id(4), id(55)),
        ] {
            let changed = Binding {
                coordinator: Arc::new(sender),
                ..b.clone()
            };
            assert_eq!(a.open_dependency(changed, id(90)), Err(Refusal::Conflict));
        }
        let t = a
            .reserve_partition(b.clone(), id(90), id(100), id(101))
            .unwrap();
        assert_eq!(a.begin_close(&b, id(90)), Ok(Close::Pending));
        assert_eq!(a.open_dependency(b.clone(), id(90)), Err(Refusal::Closed));
        assert!(matches!(
            a.reserve_partition(b.clone(), id(90), id(102), id(101)),
            Err(Refusal::Closed)
        ));
        a.execution_settled(t.key).unwrap();
        assert_eq!(a.begin_close(&b, id(90)), Ok(Close::Pending));
        a.delivery_settled(t.key).unwrap();
        assert_eq!(a.begin_close(&b, id(90)), Ok(Close::Pending));
        a.cleanup_settled(t.key).unwrap();
        assert_eq!(a.begin_close(&b, id(90)), Ok(Close::Pending));
        a.reap(t.key).unwrap();
        assert_eq!(a.begin_close(&b, id(90)), Ok(Close::Closed));
    }
    #[test]
    fn permanent_barrier_self_peer_progress_and_closing_queue_promotion() {
        let mut f = Fixture::new();
        let peer = f.add_peer(200);
        let mut a = f.core(1, 1, 3);
        let o = op(&mut a, &f, 100);
        let self_binding = a.work(o.key).unwrap().binding.clone();
        a.open_dependency(peer.clone(), id(20200)).unwrap();
        let self_job = a
            .reserve_partition(self_binding.clone(), id(20100), id(1), id(2))
            .unwrap();
        let queued = a
            .reserve_partition(peer.clone(), id(20200), id(3), id(4))
            .unwrap();
        let accepted_self = a
            .reserve_partition(self_binding.clone(), id(20100), id(7), id(8))
            .unwrap();
        assert!(matches!(
            a.next_start(Kind::Partition, f.check(&a, &queued)),
            Err(Refusal::Full)
        ));
        a.observe_drain(&f.boot(), id(99)).unwrap();
        a.observe_drain(&f.boot(), id(99)).unwrap();
        assert_eq!(a.observe_drain(&f.boot(), id(98)), Err(Refusal::Conflict));
        a.observe(f.view()).unwrap(); // A ready-looking root cannot reopen.
        let before = a.snapshot();
        assert!(matches!(
            a.reserve_operation(f.binding(400), id(10400), id(20400)),
            Err(Refusal::Closed)
        ));
        assert_eq!(
            a.open_dependency(f.binding(401), id(20401)),
            Err(Refusal::Closed)
        );
        assert_eq!(a.snapshot(), before);
        let extra = a
            .reserve_partition(peer.clone(), id(20200), id(5), id(6))
            .unwrap();
        assert_eq!(a.begin_close(&peer, id(20200)), Ok(Close::Pending));
        settle(&mut a, self_job);
        let check = f.check(&a, &queued);
        let start = a.next_start(Kind::Partition, check).unwrap();
        assert_eq!(start.key, queued.key);
        settle(&mut a, o);
        assert!(a.find_dependency(&self_binding, id(20100)).is_ok());
        settle(&mut a, accepted_self);
        assert!(matches!(
            a.find_dependency(&self_binding, id(20100)),
            Err(Refusal::Stale)
        ));
        settle(&mut a, queued);
        settle(&mut a, extra);
        assert_eq!(a.snapshot().dependencies, 0);
    }
    #[test]
    fn queued_new_root_and_boot_checks_preserve_owners() {
        let mut f = Fixture::new();
        let mut a = f.core(1, 1, 1);
        let first = op(&mut a, &f, 100);
        let queued = op(&mut a, &f, 101);
        let old_check = f.check(&a, &queued);
        settle(&mut a, first);
        f.root.revision += 1;
        f.root.change_id = id(60);
        a.observe(f.view()).unwrap();
        let before = a.snapshot();
        assert!(matches!(
            a.next_start(Kind::Operation, old_check),
            Err(Refusal::Stale)
        ));
        assert_eq!(a.snapshot(), before);
        let mut wrong_boot = f.check(&a, &queued);
        wrong_boot.boot = Arc::new(Fixture::new().boot());
        assert!(matches!(
            a.next_start(Kind::Operation, wrong_boot),
            Err(Refusal::Stale)
        ));
        assert_eq!(a.snapshot(), before);
        // Independently decoded durable root now replaces the exact session.
        f.root.revision += 1;
        f.root.change_id = id(61);
        f.root.nodes[0].session.as_mut().unwrap().id = id(62);
        a.observe(f.view()).unwrap();
        assert!(matches!(
            a.next_start(Kind::Operation, f.check(&a, &queued)),
            Err(Refusal::Ineligible)
        ));
        assert_eq!(a.snapshot(), before);
        settle(&mut a, queued);
        assert_eq!(a.snapshot().dependencies, 0);
    }
    #[test]
    fn work_evidence_survives_rollback_settlement_and_exhaustion() {
        let f = Fixture::new();
        let mut a = f.core(2, 1, 0);
        let baseline = a.evidence.settled().unwrap();
        let allocation = a.generation();
        let ticket = op(&mut a, &f, 100);
        a.rollback_work(ticket, allocation, false);
        assert_eq!(a.generation(), allocation);
        assert_eq!(a.snapshot().dependencies, 0);
        assert!(a.evidence.settled().unwrap() > baseline);
        let ticket = op(&mut a, &f, 101);
        let accepted = a.evidence.settled().unwrap();
        a.evidence.0.lock().unwrap().generation = u64::MAX;
        assert!(matches!(
            a.reserve_operation(f.binding(102), id(10102), id(20102)),
            Err(Refusal::Exhausted)
        ));
        settle(&mut a, ticket);
        assert_eq!(a.snapshot().dependencies, 0);
        assert!(a.evidence.settled().is_none());
        assert!(accepted > baseline);
    }
    #[test]
    fn work_evidence_unchanged_root_and_sticky_integrity() {
        let f = Fixture::new();
        let mut a = f.core(1, 1, 0);
        let before = a.evidence.settled();
        a.observe(f.view()).unwrap();
        assert_eq!(a.evidence.settled(), before);
        let mut wrong = f.view();
        wrong.observation = OwnershipObservation::new(&{
            let mut root = f.root.clone();
            root.change_id = id(999);
            root
        })
        .unwrap();
        assert_eq!(a.observe(wrong), Err(Refusal::Stale));
        assert_ne!(a.evidence.settled(), before);
        assert_eq!(a.observe(f.view()), Err(Refusal::Ineligible));
    }
    #[test]
    fn stale_tickets_exhaustion_and_drop_never_reap() {
        let f = Fixture::new();
        let mut a = f.core(1, 1, 0);
        let t = op(&mut a, &f, 100);
        let old = t.key;
        let before = a.snapshot();
        {
            let _abandoned = t;
        }
        assert_eq!(a.snapshot(), before);
        settle(&mut a, WorkTicket { key: old });
        let t = op(&mut a, &f, 101);
        assert_eq!(t.key.slot, old.slot);
        assert_ne!(t.key.generation, old.generation);
        assert_eq!(a.execution_settled(old), Err(Refusal::Stale));
        settle(&mut a, t);
        a.generation = u64::MAX - 1;
        let before = a.snapshot();
        assert!(matches!(
            a.reserve_operation(f.binding(102), id(10102), id(20102)),
            Err(Refusal::Exhausted)
        ));
        assert_eq!(a.snapshot(), before);
        let dep = a.open_dependency(f.binding(103), id(20103)).unwrap();
        assert!(matches!(
            a.reserve_partition(f.binding(103), id(20103), id(1), id(2)),
            Err(Refusal::Exhausted)
        ));
        assert_eq!(a.dependency(dep).unwrap().uses, 0);
        assert_eq!(
            a.open_dependency(f.binding(104), id(20104)),
            Err(Refusal::Exhausted)
        );
        a.begin_close(&f.binding(103), id(20103)).unwrap();
        assert!(matches!(a.dependency(dep), Err(Refusal::Stale)));
    }
    #[test]
    fn root_integrity_failure_is_sticky() {
        let mut f = Fixture::new();
        let mut a = f.core(1, 1, 0);
        let baseline = a.readiness(&f.boot(), &probe(70));
        assert_eq!(baseline, Ok(id(70)));
        f.root.change_id = id(71); // Same revision, independently checked conflicting root.
        assert_eq!(a.observe(f.view()), Err(Refusal::Stale));
        assert_eq!(a.readiness(&f.boot(), &probe(70)), Err(Refusal::Ineligible));
        f.root.revision += 1;
        f.root.change_id = id(72);
        assert_eq!(a.observe(f.view()), Err(Refusal::Ineligible));
    }
    #[test]
    fn durable_drain_missing_and_lost_association_retain_owners() {
        for missing in [false, true] {
            let mut f = Fixture::new();
            let mut a = f.core(1, 1, 1);
            let operation = op(&mut a, &f, 100);
            let b = a.work(operation.key).unwrap().binding.clone();
            let first = a
                .reserve_partition(b.clone(), id(20100), id(1), id(2))
                .unwrap();
            let queued = a
                .reserve_partition(b.clone(), id(20100), id(3), id(4))
                .unwrap();
            f.root.revision += 1;
            f.root.change_id = id(70);
            f.root.nodes[0].state = NodeState::Draining;
            f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
                id: id(71),
                requested_at_ms: 0,
                ack_seq: None,
                confirmed_idle: false,
            });
            a.observe(f.view()).unwrap();
            assert_eq!(a.snapshot().barrier, Barrier::Draining(id(71)));
            assert!(matches!(
                a.reserve_operation(f.binding(101), id(10101), id(20101)),
                Err(Refusal::Closed)
            ));
            settle(&mut a, first);
            f.root.revision += 1;
            f.root.change_id = id(72);
            if missing {
                f.root.nodes.clear();
            } else {
                f.root.nodes[0].state = NodeState::Lost;
            }
            a.observe(f.view()).unwrap();
            let before = a.snapshot();
            assert!(matches!(
                a.next_start(Kind::Partition, f.check(&a, &queued)),
                Err(Refusal::Ineligible)
            ));
            assert!(matches!(
                a.reserve_partition(b.clone(), id(20100), id(5), id(6)),
                Err(Refusal::Ineligible)
            ));
            assert_eq!(a.snapshot(), before);
            assert_eq!(a.begin_close(&b, id(20100)), Ok(Close::Pending));
            settle(&mut a, operation);
            settle(&mut a, queued);
            assert_eq!(a.snapshot().dependencies, 0);
        }
    }
    fn checked_view(
        f: &Fixture,
        scope: &str,
        root: &Control,
        boot: Arc<ReceiverIdentity>,
    ) -> Result<DurableRequestView, Refusal> {
        use super::super::auth::{check_request_authority, verify_request};
        use ring::signature::{Ed25519KeyPair, KeyPair};
        let api = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let (path, operation, digest) = match scope {
            "operation" => (
                "/internal/v1/operations",
                format!("\"{}\"", id(600)),
                format!("\"{}\"", "07".repeat(32)),
            ),
            "drain" => ("/internal/v1/drain", "null".into(), "null".into()),
            _ => ("/internal/v1/readiness", "null".into(), "null".into()),
        };
        let text = format!(
            r#"{{"version":1,"cluster_id":"{}","scope":"{scope}","sender":{{"role":"api","process_id":"{}","node_id":null,"incarnation":null,"session_id":null,"public_key":"{}"}},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":1,"method":"POST","path":"{path}","operation_id":{operation},"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":{digest},"control_sha256":"{}","dependency_id":null}}"#,
            id(1),
            id(2),
            hex(api.public_key().as_ref()),
            id(3),
            id(4),
            id(5),
            "a".repeat(64)
        );
        let mut message = b"nabedra.http-request.v1\0".to_vec();
        message.extend_from_slice(text.as_bytes());
        let signed = verify_request(
            &hex(text.as_bytes()),
            &hex(api.sign(&message).as_ref()),
            &ActualRoute::new("POST", path, None).unwrap(),
        )
        .unwrap();
        let eligible = check_request_authority(
            &signed,
            &f.root,
            &f.boot(),
            &api.public_key().as_ref().try_into().unwrap(),
        )
        .unwrap();
        DurableRequestView::prepare(root, eligible, boot)
    }
    #[test]
    fn api_view_local_reservation_compares_retained_identity_without_key_decoding() {
        let f = Fixture::new();
        let local = f.core(1, 1, 0);
        let view = checked_view(&f, "operation", &f.root, local.boot()).unwrap();
        assert!(view.durable.coordinators.is_empty());
        assert!(view.eligible.auth().route() == super::super::auth::Route::Operations);
        let binding = Binding {
            coordinator: Arc::new((*f.sender).clone()),
            ..f.binding(600)
        };
        assert!(!Arc::ptr_eq(&binding.coordinator, &f.sender));
        let other_key = WorkerSigningKey::new().unwrap();
        let foreign = Binding {
            coordinator: Arc::new(Fixture::sender(&other_key, id(3), id(4), id(5))),
            ..f.binding(601)
        };
        assert_ne!(
            foreign.coordinator.public_key().unwrap(),
            f.key.public_key()
        );
        let before_control = Sender::key_decode_count();
        assert_eq!(f.sender.public_key().unwrap(), f.key.public_key());
        assert_eq!(Sender::key_decode_count(), before_control + 1);
        let receiver = std::sync::Mutex::new(ReceiverState::distributed(local));
        let (accepted, foreign_result, decodes, empty) = {
            let mut state = receiver.lock().unwrap();
            let (_, local) = state.parts();
            let local = local.unwrap();
            local.observe(view.durable).unwrap();
            let before = Sender::key_decode_count();
            let reservation = local.reserve_operation(binding, id(10600), id(20600));
            let foreign_result = local.eligible(Some(&foreign), true);
            let decodes = Sender::key_decode_count() - before;
            let accepted = reservation.is_ok();
            if let Ok(ticket) = reservation {
                settle(local, ticket);
            }
            (
                accepted,
                foreign_result,
                decodes,
                local.counts() == (0, 0, 0, 0, 0),
            )
        };
        assert!(accepted);
        assert_eq!(foreign_result, Err(Refusal::Ineligible));
        assert!(empty);
        assert_eq!(decodes, 0, "locked identity checks must not decode keys");
    }
    #[test]
    fn checked_views_preserve_phase_boot_and_complete_root() {
        let mut f = Fixture::new();
        let boot = Arc::new(f.boot());
        let view = checked_view(&f, "readiness", &f.root, boot.clone()).unwrap();
        assert!(Arc::ptr_eq(&view.durable.boot, &boot));
        assert_eq!(view.durable.local, Eligibility::Ready);
        assert!(view.eligible.local().same_boot(&boot));
        let mut other = f.root.clone();
        other.change_id = id(66);
        assert!(matches!(
            checked_view(&f, "readiness", &other, boot.clone()),
            Err(Refusal::Stale)
        ));
        assert!(matches!(
            checked_view(&f, "readiness", &f.root, Arc::new(Fixture::new().boot())),
            Err(Refusal::Stale)
        ));
        let mut starting = Fixture::new();
        starting.root.nodes[0].state = NodeState::Starting;
        starting.root.nodes[0].registration = Registration::Registered;
        let registered = checked_view(
            &starting,
            "readiness",
            &starting.root,
            Arc::new(starting.boot()),
        )
        .unwrap();
        assert_eq!(registered.durable.local, Eligibility::Registered);
        for denied in [NodeState::Lost, NodeState::Stopped] {
            let mut denied_root = f.root.clone();
            denied_root.nodes[0].state = denied;
            assert!(matches!(
                checked_view(&f, "readiness", &denied_root, boot.clone()),
                Err(Refusal::Stale)
            ));
        }
        let mut local = f.core(1, 1, 0);
        local.observe(view.durable).unwrap();
        let view = checked_view(&f, "readiness", &f.root, boot.clone()).unwrap();
        local.observe(view.durable).unwrap();
        for registration in [Registration::Verified, Registration::Registered] {
            f.root.revision += 1;
            f.root.change_id = id(f.root.revision as u128 + 80);
            f.root.nodes[0].state = NodeState::Draining;
            f.root.nodes[0].registration = registration;
            f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
                id: id(99),
                requested_at_ms: 0,
                ack_seq: None,
                confirmed_idle: false,
            });
            let view = checked_view(&f, "drain", &f.root, boot.clone()).unwrap();
            assert_eq!(
                view.durable.local,
                if registration == Registration::Verified {
                    Eligibility::Draining(id(99))
                } else {
                    Eligibility::RegisteredDraining(id(99))
                }
            );
            local.observe(view.durable).unwrap();
            assert_eq!(local.snapshot().barrier, Barrier::Draining(id(99)));
            assert_eq!(
                local.eligible(None, false).is_ok(),
                registration == Registration::Verified
            );
            assert!(local.eligible(None, true).is_err());
        }
        let view = checked_view(&f, "drain", &f.root, boot.clone()).unwrap();
        f.root.change_id = id(109);
        let conflicting = checked_view(&f, "drain", &f.root, boot).unwrap();
        local.observe(view.durable).unwrap();
        assert_eq!(local.observe(conflicting.durable), Err(Refusal::Stale));
        assert!(local.integrity_failed);
    }
}

/// Common container for local admission and registered payload supervisors.
/// Distributed construction fixes the reviewed boot and validated ownership bounds.
pub(super) struct ReceiverState {
    pub(super) payload: super::payload_owner::PayloadRegistry,
    mode: ReceiverMode,
}
enum ReceiverMode {
    Legacy,
    #[allow(dead_code)] // R04c adds replay and the final checked commit to this closed mode.
    Distributed(Box<LocalAdmission>, super::auth::ReplayState),
}
impl ReceiverState {
    pub(super) fn legacy(payload: super::payload_owner::PayloadRegistry) -> Self {
        Self {
            payload,
            mode: ReceiverMode::Legacy,
        }
    }
    pub(super) fn legacy_mode(&self) -> bool {
        matches!(self.mode, ReceiverMode::Legacy)
    }
}

impl ReceiverState {
    #[allow(dead_code)] // R04c/R07 construct the fixed receiver before listeners.
    pub(super) fn distributed(local: LocalAdmission) -> Self {
        let payload = super::payload_owner::PayloadRegistry::bounded(&local);
        let replay = super::auth::ReplayState::for_receiver(local.boot());
        Self {
            payload,
            mode: ReceiverMode::Distributed(Box::new(local), replay),
        }
    }
    pub(super) fn parts(
        &mut self,
    ) -> (
        &mut super::payload_owner::PayloadRegistry,
        Option<&mut LocalAdmission>,
    ) {
        (
            &mut self.payload,
            match &mut self.mode {
                ReceiverMode::Legacy => None,
                ReceiverMode::Distributed(local, _) => Some(local),
            },
        )
    }
    pub(super) fn local(&self) -> Option<&LocalAdmission> {
        match &self.mode {
            ReceiverMode::Legacy => None,
            ReceiverMode::Distributed(local, _) => Some(local),
        }
    }
}

#[allow(dead_code)] // Concrete retained-key seams consumed by R04c's typed transaction.
impl LocalAdmission {
    pub(super) fn boot(&self) -> Arc<ReceiverIdentity> {
        self.boot.clone()
    }
    pub(super) fn initialization_matches(&self, boot: &ReceiverIdentity, init: Uuid) -> bool {
        self.boot.same_boot(boot)
            && self.init == (InitState::Uninitialized { identity: init })
            && !self.integrity_failed
    }
    pub(super) fn work_key(ticket: &WorkTicket) -> WorkKey {
        ticket.key
    }
    pub(super) fn work_dependency(&self, key: WorkKey) -> Result<DependencyKey, Refusal> {
        Ok(self.work(key)?.dependency.key)
    }
    pub(super) fn hold_account(&mut self, key: DependencyKey) -> Result<(), Refusal> {
        self.evidence.admit()?;
        let d = self
            .dependencies
            .get_mut(key.slot)
            .and_then(Option::as_mut)
            .filter(|d| d.key == key)
            .ok_or(Refusal::Stale)?;
        if d.payload_account {
            return Err(Refusal::Conflict);
        }
        d.payload_account = true;
        Ok(())
    }
    pub(super) fn release_account(&mut self, key: DependencyKey) -> Result<(), Refusal> {
        self.evidence.record();
        let d = self.dependency(key)?;
        if !d.payload_account || !d.closing || d.uses != 0 {
            return Err(Refusal::Conflict);
        }
        // The self-dependency's operation may outlive its separate payload account.
        // reap_dependency retains the metadata until that operation also reaps.
        self.dependencies[key.slot]
            .as_mut()
            .unwrap()
            .payload_account = false;
        self.reap_dependency(key);
        Ok(())
    }
    pub(super) fn dependency_key(
        &self,
        binding: &Binding,
        permit: Uuid,
    ) -> Result<DependencyKey, Refusal> {
        self.find_dependency(binding, permit)
    }
    pub(super) fn dependency_closing(&self, key: DependencyKey) -> bool {
        self.dependency(key).is_ok_and(|d| d.closing)
    }
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
    pub(super) fn rollback_work(&mut self, ticket: WorkTicket, generation: u64, account: bool) {
        self.evidence.record();
        let key = ticket.key;
        let record = self.table_mut(key.kind)[key.slot]
            .take()
            .expect("tentative work");
        let dep = record.dependency.key;
        if key.kind == Kind::Operation {
            self.dependencies[dep.slot] = None;
        } else {
            let d = self.dependencies[dep.slot].as_mut().unwrap();
            d.uses -= 1;
            if account {
                d.payload_account = false;
            }
        }
        self.generation = generation;
    }
    pub(super) fn retain_payload_use(&self, key: WorkKey) -> Result<DependencyUse, Refusal> {
        Ok(DependencyUse {
            key: self.work(key)?.dependency.key,
        })
    }
    pub(super) fn reap_payload_work(
        &mut self,
        key: WorkKey,
        retained: &DependencyUse,
    ) -> Result<(), Refusal> {
        if self.work(key)?.dependency.key != retained.key {
            return Err(Refusal::Stale);
        }
        // Only actual supervisor termination after delivery/Discard reaches here.
        if matches!(
            self.work(key)?.phase,
            WorkPhase::Queued | WorkPhase::RunningReserved | WorkPhase::Executing
        ) {
            self.execution_settled(key)?;
        }
        if self.work(key)?.phase == WorkPhase::Settling {
            self.delivery_settled(key)?;
        }
        if self.work(key)?.phase == WorkPhase::Cleanup {
            self.cleanup_settled(key)?;
        }
        self.reap(key)
    }
}

#[cfg(test)]
pub(super) fn distributed_fixture(o: usize, p: usize, q: usize) -> ReceiverState {
    let f = ownership_tests::Fixture::new();
    let mut local = LocalAdmission::new(
        Uuid::from_u128(1),
        &f.key,
        f.sender.clone(),
        Uuid::from_u128(30),
        o,
        p,
        q,
    )
    .unwrap();
    local.observe(f.view()).unwrap();
    ReceiverState::distributed(local)
}
#[cfg(test)]
impl ReceiverState {
    pub(super) fn fixture_initialize(
        &mut self,
        receipt: super::payload_owner::ProcessReady,
    ) -> bool {
        if !receipt.check_locked(self) {
            return false;
        }
        let (_, local) = self.parts();
        let local = local.unwrap();
        local.evidence.record();
        local.init = InitState::Initialized {
            identity: Uuid::from_u128(30),
        };
        true
    }
}
#[cfg(test)]
impl LocalAdmission {
    pub(super) fn fixture_binding(&self, n: u128) -> Binding {
        Binding {
            coordinator: self.coordinator.clone(),
            operation: Uuid::from_u128(n),
            admission: Uuid::from_u128(n + 10000),
            digest: [7; 32],
        }
    }
    pub(super) fn counts(&self) -> (usize, usize, usize, usize, usize) {
        let s = self.snapshot();
        (
            s.operations.running,
            s.operations.queued,
            s.partitions.running,
            s.partitions.queued,
            s.dependencies,
        )
    }
}
impl LocalSnapshot {
    #[allow(dead_code)] // R04c reads initialization; this never completes it.
    pub(super) fn initialized(&self) -> bool {
        self.initialized
    }
}
impl LocalAdmission {
    pub(super) fn close_payload_dependencies(&mut self) {
        if self.dependencies.iter().flatten().any(|d| !d.closing) {
            self.evidence.record();
        }
        for d in self.dependencies.iter_mut().flatten() {
            d.closing = true;
        }
        for i in 0..self.dependencies.len() {
            if let Some(d) = &self.dependencies[i] {
                self.reap_dependency(d.key);
            }
        }
    }
}

impl Binding {
    pub(super) fn admission_id(&self) -> Uuid {
        self.admission
    }
}
impl ReceiverState {
    #[allow(dead_code)] // R04c commits this concrete transition after replay, without waiting.
    pub(super) fn close_dependency_locked(
        &mut self,
        binding: &Binding,
        permit: Uuid,
    ) -> Result<Close, Refusal> {
        let (payload, local) = self.parts();
        let local = local.ok_or(Refusal::Invalid)?;
        let key = local.dependency_key(binding, permit).ok();
        let result = local.begin_close(binding, permit)?;
        if let Some(key) = key {
            payload.cancel_dependency_locked(key);
        }
        Ok(result)
    }
}

/// Retention sample only. A21's drain-observer/publication receipts remain R04c.
#[allow(dead_code)] // R04c samples this under the same lock as its barrier/replay transaction.
pub(super) struct ReceiverSnapshot {
    pub(super) local: Option<LocalSnapshot>,
    pub(super) payload: super::payload_owner::RetentionSnapshot,
    pub(super) boot: Option<Arc<ReceiverIdentity>>,
    pub(super) initialization: Option<(Uuid, bool)>,
}
impl ReceiverState {
    #[allow(dead_code)] // No sampled count grants drain or termination authority.
    pub(super) fn snapshot_locked(&self) -> ReceiverSnapshot {
        ReceiverSnapshot {
            local: self.local().map(LocalAdmission::snapshot),
            payload: self.payload.retention_locked(),
            boot: self.local().map(LocalAdmission::boot),
            initialization: self.local().map(|l| {
                (
                    match l.init {
                        InitState::Uninitialized { identity }
                        | InitState::Initialized { identity } => identity,
                    },
                    l.initialized(),
                )
            }),
        }
    }
}

impl LocalAdmission {
    pub(super) fn owner_bounds(&self) -> (usize, usize) {
        (self.operations.len(), self.partitions.len())
    }
}
impl LocalSnapshot {
    #[allow(dead_code)] // R04c supplies these derived counts to heartbeat samples.
    pub(super) fn occupancy(&self) -> (usize, usize, usize, usize) {
        (
            self.operations.running,
            self.operations.queued,
            self.partitions.running,
            self.partitions.queued,
        )
    }
    #[allow(dead_code)] // A permanent barrier identity, without drain/idle authority.
    pub(super) fn drain_id(&self) -> Option<Uuid> {
        match self.barrier {
            Barrier::Open => None,
            Barrier::Draining(id) => Some(id),
        }
    }
    #[allow(dead_code)] // Open/closing records include account and accepted-use retention.
    pub(super) fn dependencies(&self) -> (usize, usize) {
        (self.dependencies - self.closing, self.closing)
    }
}

impl Binding {
    pub(super) fn operation_id(&self) -> Uuid {
        self.operation
    }
}

/// Retains the original provisional request. Final replay/reservation is R04c2.
#[allow(dead_code)] // R04c2 consumes both fields in its locked transaction.
pub(super) struct DurableRequestView {
    eligible: super::auth::EligibleRequest,
    durable: DurableView,
    membership: super::auth::ReplayMembership,
}
impl std::fmt::Debug for DurableRequestView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DurableRequestView")
    }
}
impl DurableRequestView {
    #[allow(dead_code)] // R04c2 prepares outside the receiver lock.
    pub(super) fn prepare(
        root: &crate::cluster::state::Control,
        eligible: super::auth::EligibleRequest,
        boot: Arc<ReceiverIdentity>,
    ) -> Result<Self, Refusal> {
        use crate::cluster::member::{NodeState, Registration};
        let observation = OwnershipObservation::new(root).map_err(|_| Refusal::Invalid)?;
        if observation != *eligible.observation() || !boot.same_boot(eligible.local()) {
            return Err(Refusal::Stale);
        }
        let target = boot.check_current(root).map_err(|_| Refusal::Ineligible)?;
        let local = match (target.state, target.registration) {
            (NodeState::Ready, Registration::Verified) => Eligibility::Ready,
            (NodeState::Starting, Registration::Registered) => Eligibility::Registered,
            (NodeState::Draining, registration) => {
                let drain = target.drain.as_ref().ok_or(Refusal::Ineligible)?.id;
                match registration {
                    Registration::Verified => Eligibility::Draining(drain),
                    Registration::Registered => Eligibility::RegisteredDraining(drain),
                    _ => Eligibility::Denied,
                }
            }
            _ => Eligibility::Denied,
        };
        let mut coordinators = Vec::with_capacity(1);
        if eligible.auth().sender().role() == super::auth::Role::Worker {
            let sender = eligible.auth().sender();
            if super::auth::checked_worker(root, sender).is_ok_and(super::auth::verified_work) {
                coordinators.push(Arc::new(sender.clone()));
            }
        }
        let membership =
            super::auth::ReplayMembership::prepare(root).map_err(|_| Refusal::Invalid)?;
        Ok(Self {
            eligible,
            membership,
            durable: DurableView {
                boot,
                observation,
                local,
                coordinators,
            },
        })
    }
}
impl ReceiverState {
    #[allow(dead_code)] // R07 supplies configured validation after real startup settles.
    pub(super) fn complete_initialization(
        &mut self,
        process: super::payload_owner::ProcessReady,
        configured: super::initialization::InitializedWorker,
    ) -> Result<(), Refusal> {
        if !process.check_locked(self) || !process.matches_initialization(&configured) {
            return Err(Refusal::Stale);
        }
        let (_, local) = self.parts();
        let local = local.ok_or(Refusal::Stale)?;
        let InitState::Uninitialized { identity } = local.init else {
            return Err(Refusal::Stale);
        };
        local.evidence.admit()?;
        local.init = InitState::Initialized { identity };
        Ok(())
    }
}

#[cfg(test)]
mod receiver_tests;

struct LocalControlReservation<'a> {
    local: &'a mut LocalAdmission,
    generation: u64,
    opened: Option<DependencyKey>,
    close: Option<DependencyKey>,
    result: ControlResult,
    committed: bool,
}
#[allow(dead_code)] // The transport encodes these results after acceptance.
pub(super) enum ControlResult {
    Readiness(Uuid),
    Open(DependencyKey),
    Close(Close),
    Drain(Uuid),
}
impl<'a> LocalControlReservation<'a> {
    fn open(
        local: &'a mut LocalAdmission,
        binding: Binding,
        permit: Uuid,
    ) -> Result<Self, Refusal> {
        local.eligible(Some(&binding), true)?;
        let generation = local.generation;
        let existing = local
            .dependencies
            .iter()
            .flatten()
            .any(|d| d.permit == permit);
        let key = local.open_dependency(binding, permit)?;
        Ok(Self {
            local,
            generation,
            opened: if existing { None } else { Some(key) },
            close: None,
            result: ControlResult::Open(key),
            committed: false,
        })
    }
    fn prepare(
        local: &'a mut LocalAdmission,
        pending: &super::auth::PendingLocalChecks,
        content: &super::auth::CheckedContent,
    ) -> Result<Self, Refusal> {
        use super::auth::PendingLocalChecks as P;
        local.evidence.admit()?;
        let mut close = None;
        let result = match pending {
            P::Open { binding } => return Self::open(local, Binding::from(binding), content.dependency().ok_or(Refusal::Invalid)?.permit_id()),
            P::Probe => ControlResult::Readiness(local.readiness(&local.boot, content.readiness().ok_or(Refusal::Invalid)?)?),
            P::Close { binding } => {
                let binding = Binding::from(binding);
                let permit = content.dependency().ok_or(Refusal::Invalid)?.permit_id();
                if local.dependencies.iter().flatten().any(|d| d.permit == permit) {
                    let key = local.find_dependency(&binding, permit)?;
                    let d = local.dependency(key)?;
                    close = Some(key);
                    ControlResult::Close(if d.uses == 0 && d.operation.is_none() && !d.payload_account { Close::Closed } else { Close::Pending })
                } else { ControlResult::Close(Close::Closed) }
            }
            P::Drain { drain_id } if content.drain().is_some_and(|d| d.drain_id() == *drain_id)
                && local.barrier == Barrier::Draining(*drain_id)
                && local.durable.as_ref().is_some_and(|d| matches!(d.local, Eligibility::Draining(id) | Eligibility::RegisteredDraining(id) if id == *drain_id)) => ControlResult::Drain(*drain_id),
            _ => return Err(Refusal::Invalid),
        };
        Ok(Self {
            generation: local.generation,
            local,
            opened: None,
            close,
            result,
            committed: false,
        })
    }
    fn commit(mut self) -> (ControlResult, Option<DependencyKey>) {
        self.local.evidence.record();
        if let Some(key) = self.close {
            self.local.dependencies[key.slot].as_mut().unwrap().closing = true;
            self.local.reap_dependency(key);
        }
        self.committed = true;
        let result = std::mem::replace(&mut self.result, ControlResult::Close(Close::Closed));
        (result, self.close)
    }
}
impl Drop for LocalControlReservation<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Some(key) = self.opened
        {
            self.local.evidence.record();
            self.local.dependencies[key.slot] = None;
            self.local.generation = self.generation;
        }
    }
}

/// Prepared outside the receiver mutex. IDs carry no authority.
#[allow(dead_code)] // Future HTTP prelude prepares the closed route.
pub(super) struct OperationCandidates {
    admission: Uuid,
    permit: Uuid,
}
impl OperationCandidates {
    #[allow(dead_code)] // Future HTTP prelude.
    pub(super) fn new() -> Self {
        Self {
            admission: Uuid::new_v4(),
            permit: Uuid::new_v4(),
        }
    }
}
#[allow(dead_code)] // Future HTTP prelude.
pub(super) enum PreparedRoute {
    Operation(super::payload_owner::PreparedPair, OperationCandidates),
    Job(super::payload_owner::PreparedPair),
    Control(super::payload_owner::PreparedEntry),
}
#[derive(Debug)]
pub(super) enum AcceptError {
    Auth(super::auth::Error),
    Admission(Refusal),
    Owner(super::payload_owner::OwnerError),
}
impl From<Refusal> for AcceptError {
    fn from(e: Refusal) -> Self {
        Self::Admission(e)
    }
}
impl From<super::auth::Error> for AcceptError {
    fn from(e: super::auth::Error) -> Self {
        Self::Auth(e)
    }
}
impl From<super::payload_owner::OwnerError> for AcceptError {
    fn from(e: super::payload_owner::OwnerError) -> Self {
        Self::Owner(e)
    }
}
#[allow(dead_code)] // Typed transport dispatch and signed response encoding follow.
pub(super) enum AcceptedRoute {
    Operation(AcceptedWork),
    Job(AcceptedWork),
    Readiness(AcceptedControl),
    Open(AcceptedControl),
    Close(AcceptedControl),
    Drain(AcceptedControl),
}
#[allow(dead_code)] // Future scheduler and transport retain these concrete owners.
pub(super) struct AcceptedWork {
    pub(super) owners: super::payload_owner::InstalledPair,
    pub(super) start: StartSubject,
}
#[allow(dead_code)] // Future transport retains response until actual delivery settles.
pub(super) struct AcceptedControl {
    pub(super) owner: super::payload_owner::ControlLease,
    pub(super) response: super::payload_owner::DeliveryHold,
    pub(super) result: ControlResult,
}
pub(super) struct RetainedRequestAuthority {
    pub(super) binding: super::auth::RequestBinding,
    pub(super) observation: OwnershipObservation,
    pub(super) local: ReceiverIdentity,
    pub(super) pending: super::auth::PendingLocalChecks,
    work: Option<(WorkKey, Binding, DependencyKey)>,
    boot: Arc<ReceiverIdentity>,
}
#[derive(Clone)]
pub(super) struct StartSubject {
    key: WorkKey,
    authority: Arc<std::sync::OnceLock<RetainedRequestAuthority>>,
}
pub(super) struct CheckedStart {
    subject: StartSubject,
    authority: super::auth::AcceptedRequestAuthority,
    durable: DurableView,
    check: StartCheck,
}
impl StartSubject {
    pub(super) fn binding(&self) -> Result<Binding, Refusal> {
        self.authority
            .get()
            .and_then(|a| a.work.as_ref())
            .map(|(_, b, _)| b.clone())
            .ok_or(Refusal::Stale)
    }
    #[allow(dead_code)] // Fresh GET/scheduler integration follows; validation runs outside lock.
    pub(super) fn check(
        &self,
        root: &crate::cluster::state::Control,
        pin: &[u8; 32],
    ) -> Result<CheckedStart, AcceptError> {
        let retained = self.authority.get().ok_or(Refusal::Stale)?;
        let authority = retained
            .binding
            .check_accepted_authority(root, &retained.local, pin)?;
        if authority.observation().revision() < retained.observation.revision()
            || !matches!(
                retained.pending,
                super::auth::PendingLocalChecks::NewOperation { .. }
                    | super::auth::PendingLocalChecks::Job { .. }
            )
        {
            return Err(Refusal::Stale.into());
        }
        let (key, binding, dependency) = retained.work.as_ref().ok_or(Refusal::Stale)?;
        if *key != self.key {
            return Err(Refusal::Stale.into());
        }
        let target = authority.local().check_current(root)?;
        use crate::cluster::member::{NodeState, Registration};
        let local = match (target.state, target.registration) {
            (NodeState::Ready, Registration::Verified) => Eligibility::Ready,
            (NodeState::Draining, Registration::Verified) => {
                Eligibility::Draining(target.drain.as_ref().ok_or(Refusal::Ineligible)?.id)
            }
            _ => return Err(Refusal::Ineligible.into()),
        };
        let observation = authority.observation().clone();
        // The original retained boot is used, never an incoming Auth reconstruction.
        let boot = retained.boot.clone();
        let check = StartCheck {
            key: *key,
            boot: boot.clone(),
            observation: observation.clone(),
            binding: binding.clone(),
            dependency: *dependency,
        };
        let durable = DurableView {
            boot,
            observation,
            local,
            coordinators: vec![binding.coordinator.clone()],
        };
        Ok(CheckedStart {
            subject: StartSubject {
                key: self.key,
                authority: self.authority.clone(),
            },
            authority,
            durable,
            check,
        })
    }
}
impl ReceiverState {
    #[allow(dead_code)] // Local scheduler final check, before later dispatch integration.
    pub(super) fn start_locked(
        &mut self,
        checked: CheckedStart,
    ) -> Result<StartTicket, AcceptError> {
        let ReceiverMode::Distributed(local, replay) = &mut self.mode else {
            return Err(Refusal::Invalid.into());
        };
        // A foreign start cannot contribute to this receiver's root history.
        if !local.boot.same_boot(&checked.durable.boot)
            || !local.boot.same_boot(checked.authority.local())
        {
            return Err(Refusal::Stale.into());
        }
        let replay_order = replay.observe(&checked.durable.observation);
        let local_order = local.observe(checked.durable);
        replay_order?;
        local_order?;
        let retained = checked.subject.authority.get().ok_or(Refusal::Stale)?;
        if !checked.authority.matches_binding(&retained.binding)
            || !self
                .payload
                .retains_request(checked.subject.key, &checked.subject.authority)
            || !local.boot.same_boot(checked.authority.local())
        {
            return Err(Refusal::Stale.into());
        }
        Ok(local.next_start(checked.check.key.kind, checked.check)?)
    }
    #[allow(dead_code)] // HTTP/prelude will supply the same original checked request once.
    pub(super) fn accept_locked(
        &mut self,
        view: DurableRequestView,
        content: super::auth::CheckedContent,
        prepared: &mut PreparedRoute,
        process: &super::payload_owner::PayloadLease,
    ) -> Result<AcceptedRoute, AcceptError> {
        use super::auth::PendingLocalChecks as P;
        let ReceiverMode::Distributed(local, replay) = &mut self.mode else {
            return Err(Refusal::Invalid.into());
        };
        let DurableRequestView {
            eligible,
            durable,
            membership,
        } = view;
        // A foreign boot is not an observation of this receiver's root history.
        if !local.boot.same_boot(&durable.boot) || !local.boot.same_boot(eligible.local()) {
            return Err(Refusal::Stale.into());
        }
        let replay_order = replay.observe(&durable.observation);
        let local_order = local.observe(durable);
        replay_order?;
        local_order?;
        replay.observe_membership(membership)?;
        if !eligible.matches_content(&content) || !local.boot.same_boot(eligible.local()) {
            return Err(Refusal::Stale.into());
        }
        let evidence = local.evidence.clone();
        match (prepared, eligible.pending()) {
            (
                PreparedRoute::Operation(pair, candidates),
                P::NewOperation {
                    operation_id,
                    request_digest,
                },
            ) if content.operation().is_some() => {
                let binding = Binding {
                    coordinator: local.coordinator.clone(),
                    operation: *operation_id,
                    admission: candidates.admission,
                    digest: *request_digest,
                };
                let limits = process.receiver_limits()?;
                let reservation = self.payload.reserve_pair_locked(
                    local,
                    pair,
                    process,
                    binding,
                    candidates.permit,
                    None,
                    limits,
                )?;
                reservation.check_request_positions()?;
                evidence.admit()?;
                let handoff = replay.consume(eligible, content)?.into_handoff();
                let (owners, start) = reservation.install_request_locked(handoff);
                Ok(AcceptedRoute::Operation(AcceptedWork { owners, start }))
            }
            (
                PreparedRoute::Job(pair),
                P::Job {
                    binding,
                    permit_id,
                    job_id,
                    attempt_id,
                },
            ) if content.job().is_some() => {
                let binding = Binding::from(binding);
                let limits = process.receiver_limits()?;
                let reservation = self.payload.reserve_pair_locked(
                    local,
                    pair,
                    process,
                    binding,
                    *permit_id,
                    Some((*job_id, *attempt_id)),
                    limits,
                )?;
                reservation.check_request_positions()?;
                evidence.admit()?;
                let handoff = replay.consume(eligible, content)?.into_handoff();
                let (owners, start) = reservation.install_request_locked(handoff);
                Ok(AcceptedRoute::Job(AcceptedWork { owners, start }))
            }
            (PreparedRoute::Control(entry), _) => {
                // Reserve the bounded anti-reopen fence before spending replay
                // or installing any owner. A pending or absent dependency is
                // fenced alike, so a delayed older Open cannot follow closed:true.
                let closed_permit = match eligible.pending() {
                    P::Open { .. } => {
                        replay.check_dependency_open(
                            eligible.auth().sender(),
                            content.dependency().ok_or(Refusal::Invalid)?.permit_id(),
                        )?;
                        None
                    }
                    P::Close { .. } => Some(replay.prepare_dependency_close(
                        eligible.auth().sender(),
                        content.dependency().ok_or(Refusal::Invalid)?.permit_id(),
                        eligible.auth().sequence(),
                    )?),
                    _ => None,
                };
                let reservation =
                    LocalControlReservation::prepare(local, eligible.pending(), &content)?;
                let mut owner = self.payload.reserve_control_locked(entry)?;
                owner.check_request_positions()?;
                let response = owner.reserve_response()?;
                evidence.admit()?;
                let handoff = replay.consume(eligible, content)?.into_handoff();
                let control = owner.install_request_locked(handoff, reservation.local.boot());
                let (result, close) = reservation.commit();
                if let Some(closed_permit) = closed_permit {
                    replay.commit_dependency_close(closed_permit);
                }
                if let Some(key) = close {
                    self.payload.cancel_dependency_locked(key);
                }
                let accepted = AcceptedControl {
                    owner: control,
                    response,
                    result,
                };
                Ok(match accepted.result {
                    ControlResult::Readiness(_) => AcceptedRoute::Readiness(accepted),
                    ControlResult::Open(_) => AcceptedRoute::Open(accepted),
                    ControlResult::Close(_) => AcceptedRoute::Close(accepted),
                    ControlResult::Drain(_) => AcceptedRoute::Drain(accepted),
                })
            }
            _ => Err(Refusal::Invalid.into()),
        }
    }
}

impl RetainedRequestAuthority {
    pub(super) fn from_handoff(
        handoff: super::auth::RequestHandoff,
        boot: Arc<ReceiverIdentity>,
        work: Option<(WorkKey, Binding, DependencyKey)>,
    ) -> (Self, Box<[u8]>, super::auth::RequestBody) {
        let (binding, bytes, body, observation, local, pending) = handoff.into_parts();
        (
            Self {
                binding,
                observation,
                local,
                pending,
                boot,
                work,
            },
            bytes,
            body,
        )
    }
}

impl StartSubject {
    pub(super) fn new(
        key: WorkKey,
        authority: Arc<std::sync::OnceLock<RetainedRequestAuthority>>,
    ) -> Self {
        Self { key, authority }
    }
}
impl LocalAdmission {
    pub(super) fn retained_work(&self, key: WorkKey) -> (WorkKey, Binding, DependencyKey) {
        let r = self.work(key).expect("reserved work");
        (key, r.binding.clone(), r.dependency.key)
    }
}

impl std::fmt::Display for AcceptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auth(e) => write!(f, "auth {e}"),
            Self::Admission(e) => write!(f, "admission {e:?}"),
            Self::Owner(e) => write!(f, "owner {e}"),
        }
    }
}

/// Shared bounded handoff. No receiver acquisition, disk work, or notification.
#[derive(Default)]
pub(super) struct WorkEvidence(std::sync::Mutex<EvidenceState>);
#[derive(Default)]
struct EvidenceState {
    generation: u64,
    pending: usize,
    unrecorded: bool,
    exhausted: bool,
}
impl EvidenceState {
    fn advance(&mut self) {
        match self.generation.checked_add(1) {
            Some(n) if !self.exhausted => self.generation = n,
            _ => self.exhausted = true,
        }
    }
}
impl WorkEvidence {
    fn admit(&self) -> Result<(), Refusal> {
        let mut state = self.0.lock().unwrap();
        state.advance();
        if state.exhausted {
            Err(Refusal::Exhausted)
        } else {
            Ok(())
        }
    }
    pub(super) fn record(&self) {
        self.0.lock().unwrap().advance();
    }
    pub(super) fn begin(self: &Arc<Self>, new: bool) -> Result<WorkChange, Refusal> {
        let mut state = self.0.lock().unwrap();
        // Check both entry and settlement increments before admitting new work.
        if new && (state.exhausted || state.generation.checked_add(2).is_none()) {
            state.exhausted = true;
            return Err(Refusal::Exhausted);
        }
        state.advance();
        state.pending = state
            .pending
            .checked_add(1)
            .expect("bounded concrete transitions");
        Ok(WorkChange(self.clone()))
    }
    pub(super) fn record_locked(&self) -> Option<u64> {
        let mut state = self.0.lock().unwrap();
        if state.exhausted || state.pending != 0 {
            return None;
        }
        state.unrecorded = false;
        Some(state.generation)
    }
    fn settled(&self) -> Option<u64> {
        let state = self.0.lock().unwrap();
        (!state.exhausted && state.pending == 0 && !state.unrecorded).then_some(state.generation)
    }
}
pub(super) struct WorkChange(Arc<WorkEvidence>);
impl Drop for WorkChange {
    fn drop(&mut self) {
        let mut state = self.0.0.lock().unwrap();
        state.advance();
        state.pending -= 1;
        state.unrecorded = true;
    }
}
impl LocalAdmission {
    pub(super) fn evidence(&self) -> Arc<WorkEvidence> {
        self.evidence.clone()
    }
}

/// All complete-root construction and allocation occurs before locking ReceiverState.
#[allow(dead_code)] // c3d consumes this private preparation path.
pub(super) struct BarrierPreparation {
    boot: Arc<ReceiverIdentity>,
    observation: OwnershipObservation,
    binding: crate::cluster::heartbeat::HeartbeatBinding,
    drain: Uuid,
}
impl BarrierPreparation {
    #[allow(dead_code)] // c3d prepares the checked root outside the mutex.
    pub(super) fn prepare(
        root: &crate::cluster::state::Control,
        boot: Arc<ReceiverIdentity>,
    ) -> Result<Self, Refusal> {
        use crate::cluster::member::{NodeState, Registration};
        let observation = OwnershipObservation::new(root).map_err(|_| Refusal::Invalid)?;
        let node = boot.check_current(root).map_err(|_| Refusal::Stale)?;
        if node.state != NodeState::Draining || node.registration != Registration::Verified {
            return Err(Refusal::Ineligible);
        }
        let session = node.session.as_ref().ok_or(Refusal::Invalid)?;
        let binding = crate::cluster::heartbeat::HeartbeatBinding {
            resources: node.resources,
            cluster_id: root.cluster_id,
            node_id: node.node_id,
            incarnation: node.incarnation.ok_or(Refusal::Invalid)?,
            session_id: session.id,
            verifier: session.verifier.clone(),
            operation_slots: node.operation_slots,
            partition_slots: node.partition_slots,
            queue_size: node.queue_size,
        };
        Ok(Self {
            boot,
            observation,
            binding,
            drain: node.drain.as_ref().ok_or(Refusal::Invalid)?.id,
        })
    }
}
/// Local evidence, with the original owning authority cell. Never an idle receipt.
#[allow(dead_code)] // c3c/d retain this value through their concrete lifecycle.
pub(super) struct BarrierSample {
    prepared: BarrierPreparation,
    process: Uuid,
    init: InitState,
    observer: super::payload_owner::ControlKey,
    authority: Arc<std::sync::OnceLock<RetainedRequestAuthority>>,
    generation: u64,
    heartbeat: crate::cluster::heartbeat::HeartbeatSample,
}
impl std::fmt::Debug for BarrierSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BarrierSample")
    }
}
impl BarrierSample {
    pub(super) fn proof_observer_matches(&self, key: &super::payload_owner::ControlKey) -> bool {
        self.observer == *key
    }

    pub(super) fn binding(&self) -> &crate::cluster::heartbeat::HeartbeatBinding {
        &self.prepared.binding
    }
    pub(super) fn request_binding(&self) -> &super::auth::RequestBinding {
        &self.authority.get().expect("checked authority").binding
    }
    pub(super) fn heartbeat(&self) -> &crate::cluster::heartbeat::HeartbeatSample {
        &self.heartbeat
    }
}
impl ReceiverState {
    #[allow(dead_code)] // c3c/d compose with the registered writer lifecycle.
    pub(super) fn sample_barrier(
        &self,
        prepared: BarrierPreparation,
        process: Uuid,
        observer: super::payload_owner::ControlKey,
    ) -> Result<BarrierSample, Refusal> {
        let local = self.local().ok_or(Refusal::Invalid)?;
        let generation = 0;
        let authority = self
            .payload
            .control_authority(&observer)
            .ok_or(Refusal::Stale)?;
        let snapshot = local.snapshot();
        let mut sample = BarrierSample {
            prepared,
            process,
            init: local.init,
            observer,
            authority,
            generation,
            heartbeat: crate::cluster::heartbeat::HeartbeatSample {
                available_threads: None,
                available_memory_bytes: None,
                running_operations: snapshot.operations.running as u64,
                running_partitions: snapshot.partitions.running as u64,
                queued_operations: snapshot.operations.queued as u64,
                queued_partitions: snapshot.partitions.queued as u64,
                drain_id: snapshot.drain_id(),
                accepting: false,
            },
        };
        if !self.barrier_facts_match(&sample) {
            return Err(Refusal::Stale);
        }
        sample.generation = local.evidence.record_locked().ok_or(Refusal::Exhausted)?;
        if self.barrier_matches(&sample) {
            Ok(sample)
        } else {
            Err(Refusal::Stale)
        }
    }
    #[allow(dead_code)] // c3c/d compose with the registered writer lifecycle.
    pub(super) fn barrier_matches(&self, sample: &BarrierSample) -> bool {
        let Some(local) = self.local() else {
            return false;
        };
        local.evidence.settled() == Some(sample.generation)
            && self.barrier_facts_match(sample)
            && local.evidence.settled() == Some(sample.generation)
    }
    fn barrier_facts_match(&self, sample: &BarrierSample) -> bool {
        let Some(local) = self.local() else {
            return false;
        };
        let p = &sample.prepared;
        if !self.payload.sample_handoff_settled() {
            return false;
        }
        if local.integrity_failed
            || !local.boot.same_boot(&p.boot)
            || local.init != sample.init
            || local.init
                != (InitState::Initialized {
                    identity: sample.process,
                })
            || local.barrier != Barrier::Draining(p.drain)
            || local.durable.as_ref().is_none_or(|v| {
                v.observation != p.observation
                    || !matches!(v.local, Eligibility::Draining(id) if id == p.drain)
            })
        {
            return false;
        }
        let Some(authority) = sample.authority.get() else {
            return false;
        };
        if !matches!(authority.pending, super::auth::PendingLocalChecks::Drain { drain_id } if drain_id == p.drain)
            || !authority.local.same_boot(&p.boot)
            || self
                .payload
                .control_authority(&sample.observer)
                .is_none_or(|a| !Arc::ptr_eq(&a, &sample.authority))
        {
            return false;
        }
        let owners = self
            .payload
            .drain_work_locked(sample.process, &sample.observer, local);
        owners != super::payload_owner::DrainWork::InvalidOwner
    }
}

#[cfg(test)]
impl WorkEvidence {
    pub(super) fn fixture_generation(&self, generation: u64) {
        self.0.lock().unwrap().generation = generation;
    }
}

#[cfg(test)]
impl ReceiverState {
    pub(super) fn fixture_exhaust_work(&mut self) {
        let local = self.local().unwrap();
        local.evidence.0.lock().unwrap().generation = u64::MAX;
    }
    pub(super) fn fixture_consume_unspent(
        &mut self,
        view: DurableRequestView,
        content: super::auth::CheckedContent,
    ) -> bool {
        let ReceiverMode::Distributed(_, replay) = &mut self.mode else {
            return false;
        };
        replay.consume(view.eligible, content).is_ok()
    }
}

/// A local, one-use result retaining the original authenticated request and cycle.
/// It supplies no controller or process termination authority.
pub(super) struct DrainObservation {
    proof: super::drain_observation::ReapedProof,
    idle: bool,
    sequence: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DrainRefusal {
    Invalid,
    Unavailable,
}
impl std::fmt::Debug for DrainObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DrainObservation")
    }
}
impl DrainObservation {
    pub(super) fn finish(
        receiver: &std::sync::Mutex<ReceiverState>,
        proof: super::drain_observation::ReapedProof,
    ) -> Result<Self, DrainRefusal> {
        // Keep proof destruction outside ReceiverState, including every refusal.
        let checked = {
            let state = receiver.lock().unwrap();
            state.check_drain_completion(&proof)
        };
        let (idle, sequence) = checked?;
        Ok(Self {
            proof,
            idle,
            sequence,
        })
    }
    pub(super) fn request_binding(&self) -> &super::auth::RequestBinding {
        self.proof.sample().request_binding()
    }
    pub(super) fn drain_id(&self) -> Uuid {
        self.proof.sample().prepared.drain
    }
    pub(super) fn idle(&self) -> bool {
        self.idle
    }
    pub(super) fn sequence(&self) -> u64 {
        self.sequence
    }
}
impl ReceiverState {
    fn check_drain_completion(
        &self,
        proof: &super::drain_observation::ReapedProof,
    ) -> Result<(bool, u64), DrainRefusal> {
        let sample = proof.sample();
        let ack = proof.acknowledgement().ok_or(DrainRefusal::Unavailable)?;
        if ack.sequence() == 0
            || ack.binding() != sample.binding()
            || ack.sample() != sample.heartbeat()
        {
            return Err(DrainRefusal::Invalid);
        }
        if !self.barrier_facts_match(sample) {
            return Err(DrainRefusal::Invalid);
        }
        let local = self.local().ok_or(DrainRefusal::Invalid)?;
        let generation = local.evidence.settled().ok_or(DrainRefusal::Unavailable)?;
        let owners =
            self.payload
                .drain_work_for_reaped(sample.process, &sample.observer, local, proof);
        if owners == super::payload_owner::DrainWork::InvalidOwner {
            return Err(DrainRefusal::Invalid);
        }
        if local.evidence.settled() != Some(generation) {
            return Err(DrainRefusal::Unavailable);
        }
        Ok((
            generation == sample.generation
                && owners == super::payload_owner::DrainWork::ExemptOnly,
            ack.sequence(),
        ))
    }
}
