//! Pure ownership proposals. An owner pair grants no authority.
use super::state::{Control, ControlError, Controller, LeaseState};
use crate::storage::StorageError;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Owner {
    pub(crate) process_id: Uuid,
    pub(crate) epoch: u64,
}
// Remove this allowance when runtime constructs production ownership changes.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnershipChange {
    Acquire { process_id: Uuid },
    Renew(Owner),
    Release(Owner),
}
/// Construct validated metadata only. The caller supplies one fresh random UUID
/// shared with later guard admission and store preparation. A candidate cannot
/// authorize callbacks; conditional storage and local certainty are still needed.
pub(crate) fn ownership_candidate(
    root: &Control,
    change: OwnershipChange,
    unix_ms: u64,
    change_id: Uuid,
) -> Result<Control, StorageError> {
    // validate includes the existing bounded encoder, including for constructed
    // values. Ownership allocates no records and needs no new-admission check.
    root.validate().map_err(control_error)?;
    if change_id == root.change_id {
        return Err(StorageError::Conflict);
    }
    let old = &root.controller;
    let controller = match change {
        OwnershipChange::Acquire { process_id } => {
            if old.lease_state == LeaseState::Held
                && (old.owner_id == Some(process_id)
                    || old.lease_until_ms.is_some_and(|until| unix_ms < until))
            {
                return Err(StorageError::Conflict);
            }
            Controller {
                owner_id: Some(process_id),
                epoch: old
                    .epoch
                    .checked_add(1)
                    .ok_or(StorageError::LimitExceeded)?,
                lease_state: LeaseState::Held,
                lease_until_ms: Some(
                    unix_ms
                        .checked_add(root.timing.lease_ms)
                        .ok_or(StorageError::LimitExceeded)?,
                ),
            }
        }
        OwnershipChange::Renew(owner) | OwnershipChange::Release(owner) => {
            if old.lease_state != LeaseState::Held
                || old.owner_id != Some(owner.process_id)
                || old.epoch != owner.epoch
            {
                return Err(StorageError::Conflict);
            }
            match change {
                OwnershipChange::Renew(_) => {
                    let until = unix_ms
                        .checked_add(root.timing.lease_ms)
                        .ok_or(StorageError::LimitExceeded)?;
                    if !old.lease_until_ms.is_some_and(|previous| until > previous) {
                        return Err(StorageError::Conflict);
                    }
                    Controller {
                        lease_until_ms: Some(until),
                        ..old.clone()
                    }
                }
                _ => Controller {
                    owner_id: None,
                    epoch: old.epoch,
                    lease_state: LeaseState::Released,
                    lease_until_ms: None,
                },
            }
        }
    };
    let revision = root
        .revision
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    let mut candidate = root.clone();
    candidate.revision = revision;
    candidate.change_id = change_id;
    candidate.controller = controller;
    candidate.validate().map_err(control_error)?;
    Ok(candidate)
}
fn control_error(error: ControlError) -> StorageError {
    match error {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}

use super::store::{LoadedControl, OwnershipOutcome, PreparedOwnership};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Sample outside the mutex. Pass conversion errors through begin/finish/check,
/// so a current operation closes while obsolete completions remain harmless.
#[derive(Clone, Copy)]
#[allow(dead_code)] // Remove with runtime sampling/guard integration.
pub(crate) struct OwnershipSample {
    monotonic: Instant,
    unix_ms: u64,
}
#[allow(dead_code)] // Remove with runtime sampling/guard integration.
impl OwnershipSample {
    pub(crate) fn monotonic(&self) -> Instant {
        self.monotonic
    }
    pub(crate) fn unix_ms(&self) -> u64 {
        self.unix_ms
    }
    pub(crate) fn from_system_time(
        monotonic: Instant,
        wall: SystemTime,
    ) -> Result<Self, StorageError> {
        let elapsed = wall
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StorageError::Unavailable)?;
        Ok(Self {
            monotonic,
            unix_ms: elapsed
                .as_millis()
                .try_into()
                .map_err(|_| StorageError::LimitExceeded)?,
        })
    }
}

/// Complete bounded encoding and hashing happen here, OUTSIDE the guard mutex.
/// No arbitrary digest constructor, backend token or root body is retained.
#[derive(Clone, PartialEq, Eq)]
#[allow(dead_code)] // Remove with runtime observation/guard integration.
pub(crate) struct OwnershipObservation {
    revision: u64,
    change_id: Uuid,
    digest: [u8; 32],
    controller: Controller,
}
#[allow(dead_code)] // Remove with runtime observation/guard integration.
impl OwnershipObservation {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }
    pub(crate) fn new(root: &Control) -> Result<Self, StorageError> {
        let bytes = root.encode().map_err(control_error)?;
        Ok(Self {
            revision: root.revision,
            change_id: root.change_id,
            digest: Sha256::digest(bytes).into(),
            controller: root.controller.clone(),
        })
    }
}

/// Precompute outside the mutex using the SAME proposal UUID and original Unix
/// sample supplied to PreparedOwnership::prepare. Even failed preparation is
/// retained here so begin observes the base and closes certainty first.
#[allow(dead_code)] // Remove with runtime proposal/guard integration.
pub(crate) struct OwnershipProposal {
    base: OwnershipObservation,
    change: OwnershipChange,
    proposal_id: Uuid,
    sample: Result<OwnershipSample, StorageError>,
    expected: Result<OwnershipObservation, StorageError>,
    deadline: Result<Instant, StorageError>,
}
#[allow(dead_code)] // Remove with runtime proposal/guard integration.
impl OwnershipProposal {
    pub(crate) fn new(
        base: &Control,
        change: OwnershipChange,
        proposal_id: Uuid,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<Self, StorageError> {
        let observation = OwnershipObservation::new(base)?;
        let expected = sample.and_then(|start| {
            ownership_candidate(base, change, start.unix_ms, proposal_id)
                .and_then(|root| OwnershipObservation::new(&root))
        });
        let deadline = sample.and_then(|start| {
            start
                .monotonic
                .checked_add(Duration::from_millis(base.timing.lease_ms))
                .ok_or(StorageError::LimitExceeded)
        });
        Ok(Self {
            base: observation,
            change,
            proposal_id,
            sample,
            expected,
            deadline,
        })
    }
    /// After successful begin, prepare OUTSIDE the mutex. On any error or
    /// cancellation, finish the original stamp. This preserves the UUID and
    /// original Unix sample without exposing either as mutable caller inputs.
    pub(crate) fn prepare(&self, base: LoadedControl) -> Result<PreparedOwnership, StorageError> {
        if OwnershipObservation::new(base.root())? != self.base {
            return Err(StorageError::Conflict);
        }
        PreparedOwnership::prepare(base, self.change, self.sample?.unix_ms, self.proposal_id)
    }
}

/// Convert a store result outside the mutex, including encoding/hashing any
/// loaded root. finish still checks stamp obsolescence before inspecting errors.
#[allow(dead_code)] // Remove with runtime completion/guard integration.
pub(crate) struct OwnershipEvidence {
    acknowledged: bool,
    observation: Option<OwnershipObservation>,
}
#[allow(dead_code)] // Remove with runtime completion/guard integration.
impl OwnershipEvidence {
    pub(crate) fn new(outcome: OwnershipOutcome) -> Result<Self, StorageError> {
        let (acknowledged, loaded) = match outcome {
            OwnershipOutcome::Acknowledged(root) => (true, Some(root)),
            OwnershipOutcome::ObservedExact(root) => (false, Some(root)),
            OwnershipOutcome::Conflict { current } | OwnershipOutcome::Unknown { current } => {
                (false, current)
            }
        };
        Ok(Self {
            acknowledged,
            observation: loaded
                .as_ref()
                .map(|root| OwnershipObservation::new(root.root()))
                .transpose()?,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwnershipStamp {
    boot: Uuid,
    generation: u64,
}
struct ActiveOwnership {
    stamp: OwnershipStamp,
    expected: OwnershipObservation,
    sample: OwnershipSample,
    deadline: Instant,
    owner: Option<Owner>,
}
struct Certainty {
    controller: Controller,
    sample: OwnershipSample,
    deadline: Instant,
    owner: Owner,
}
impl Certainty {
    fn check(&self, sample: OwnershipSample) -> Result<Owner, StorageError> {
        let until = self
            .controller
            .lease_until_ms
            .ok_or(StorageError::Unavailable)?;
        check_ownership_time(self.sample, self.deadline, Some(until), sample)?;
        Ok(self.owner)
    }
}

fn check_ownership_time(
    anchor: OwnershipSample,
    deadline: Instant,
    unix_deadline: Option<u64>,
    sample: OwnershipSample,
) -> Result<(), StorageError> {
    let elapsed = sample
        .monotonic
        .checked_duration_since(anchor.monotonic)
        .ok_or(StorageError::Unavailable)?;
    let monotonic_ns =
        i128::try_from(elapsed.as_nanos()).map_err(|_| StorageError::LimitExceeded)?;
    let wall_ms = i128::from(sample.unix_ms) - i128::from(anchor.unix_ms);
    if sample.monotonic >= deadline
        || unix_deadline.is_some_and(|until| sample.unix_ms >= until)
        || (wall_ms * 1_000_000 - monotonic_ns).abs() > 2_000_000_000
    {
        return Err(StorageError::Unavailable);
    }
    Ok(())
}

/// Synchronous memory-only state. Caller holds a short mutex for these methods,
/// never for preparation, encoding, storage, command validation or an await.
/// Owner/stamps are metadata. They cannot authorize effects after this check.
#[allow(dead_code)] // Remove item/impl allowances when runtime owns this guard.
pub(crate) struct OwnershipGuard {
    boot: Uuid,
    generation: u64,
    exhausted: bool,
    high_water: Option<OwnershipObservation>,
    active: Option<ActiveOwnership>,
    certainty: Option<Certainty>,
}
#[allow(dead_code)] // Remove when runtime owns this guard; no dummy callers.
impl OwnershipGuard {
    /// Runtime supplies one freshly generated UUID per boot, never a stored ID.
    pub(crate) fn new(boot: Uuid) -> Self {
        Self {
            boot,
            generation: 0,
            exhausted: false,
            high_water: None,
            active: None,
            certainty: None,
        }
    }
    fn advance(&mut self) -> Result<(), StorageError> {
        if self.exhausted {
            return Err(StorageError::LimitExceeded);
        }
        match self.generation.checked_add(1) {
            Some(next) => {
                self.generation = next;
                Ok(())
            }
            None => {
                self.exhausted = true;
                self.active = None;
                self.certainty = None;
                Err(StorageError::LimitExceeded)
            }
        }
    }
    pub(crate) fn invalidate(&mut self) -> Result<(), StorageError> {
        self.active = None;
        self.certainty = None;
        self.advance()
    }
    pub(crate) fn observe(&mut self, root: &OwnershipObservation) -> Result<(), StorageError> {
        if self.exhausted {
            return Err(StorageError::LimitExceeded);
        }
        if let Some(previous) = &self.high_water {
            if root.revision < previous.revision {
                return Err(StorageError::Conflict);
            }
            if root.revision == previous.revision {
                if root != previous {
                    self.invalidate()?;
                    return Err(StorageError::InvalidCatalog);
                }
                return Ok(());
            }
        }
        let changed = self
            .certainty
            .as_ref()
            .is_some_and(|c| c.controller != root.controller);
        if changed || self.active.is_some() {
            self.invalidate()?;
        }
        self.high_water = Some(root.clone());
        Ok(())
    }
    pub(crate) fn begin(
        &mut self,
        proposal: &OwnershipProposal,
    ) -> Result<OwnershipStamp, StorageError> {
        self.observe(&proposal.base)?;
        if self.active.is_some() {
            return Err(StorageError::Conflict);
        }
        self.advance()?;
        self.certainty = None;
        let expected = proposal.expected.clone()?;
        let sample = proposal.sample?;
        let deadline = proposal.deadline?;
        let owner = match proposal.change {
            OwnershipChange::Acquire { process_id } => Some(Owner {
                process_id,
                epoch: expected.controller.epoch,
            }),
            OwnershipChange::Renew(owner) => Some(owner),
            OwnershipChange::Release(owner) => {
                if owner.process_id != self.boot {
                    return Err(StorageError::Conflict);
                }
                None
            }
        };
        if owner.is_some_and(|owner| owner.process_id != self.boot) {
            return Err(StorageError::Conflict);
        }
        let stamp = OwnershipStamp {
            boot: self.boot,
            generation: self.generation,
        };
        self.active = Some(ActiveOwnership {
            stamp,
            expected,
            sample,
            deadline,
            owner,
        });
        Ok(stamp)
    }
    pub(crate) fn finish(
        &mut self,
        stamp: OwnershipStamp,
        result: Result<OwnershipEvidence, StorageError>,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<bool, StorageError> {
        if self
            .active
            .as_ref()
            .is_none_or(|active| active.stamp != stamp)
        {
            return Ok(false);
        }
        // Every active terminal path releases only its own slot. begin already
        // closed certainty, so errors cannot preserve an old lease.
        let active = self.active.take().expect("checked active stamp");
        let evidence = result?;
        if !evidence.acknowledged {
            if let Some(root) = evidence.observation {
                self.observe(&root)?;
            }
            return Ok(false);
        }
        let root = evidence.observation.ok_or(StorageError::InvalidCatalog)?;
        if root != active.expected {
            return Err(StorageError::InvalidCatalog);
        }
        // Compare high-water BEFORE recording this operation's own advance.
        if let Some(high) = &self.high_water {
            if root.revision < high.revision {
                return Err(StorageError::Conflict);
            }
            if root.revision == high.revision && root != *high {
                return Err(StorageError::InvalidCatalog);
            }
        }
        self.high_water = Some(root.clone());
        let Some(owner) = active.owner else {
            return Ok(false);
        };
        if root.controller.owner_id != Some(self.boot)
            || root.controller.epoch != owner.epoch
            || root.controller.lease_state != LeaseState::Held
        {
            return Err(StorageError::InvalidCatalog);
        }
        let certainty = Certainty {
            controller: root.controller,
            sample: active.sample,
            deadline: active.deadline,
            owner,
        };
        if let Err(error) = sample.and_then(|now| certainty.check(now)) {
            self.invalidate()?;
            return Err(error);
        }
        self.certainty = Some(certainty);
        Ok(true)
    }
    pub(crate) fn check(
        &mut self,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<Owner, StorageError> {
        if self.exhausted {
            return Err(StorageError::LimitExceeded);
        }
        let now = match sample {
            Ok(now) => now,
            Err(error) => {
                self.invalidate()?;
                return Err(error);
            }
        };
        // Ordinary closed admission during preparation/renewal is not a clock
        // failure and must not discard the pending ownership completion.
        let Some(certainty) = &self.certainty else {
            if let Some(active) = &self.active {
                // A release has no proposed Unix lease, but its original
                // monotonic deadline and clock anchor still apply.
                if let Err(error) = check_ownership_time(
                    active.sample,
                    active.deadline,
                    active.expected.controller.lease_until_ms,
                    now,
                ) {
                    self.invalidate()?;
                    return Err(error);
                }
            }
            return Err(StorageError::Unavailable);
        };
        let result = certainty.check(now);
        if result.is_err() {
            self.invalidate()?;
        }
        result
    }
    pub(crate) fn authorization_generation(&self) -> u64 {
        self.generation
    }
    /// After observing the fresh root and validating the command OUTSIDE the
    /// mutex, final-check the captured generation, exact observation and time.
    pub(crate) fn authorize(
        &mut self,
        generation: u64,
        observed: &OwnershipObservation,
        sample: Result<OwnershipSample, StorageError>,
    ) -> Result<Owner, StorageError> {
        if generation != self.generation || self.high_water.as_ref() != Some(observed) {
            return Err(StorageError::Conflict);
        }
        self.check(sample)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        configuration::{Template, Timing},
        state::ControlError,
    };
    use serde_json::{Value, json};
    pub(super) fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }
    fn root() -> Value {
        let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
        let hash = Template::decode(&serde_json::to_vec(&template).unwrap())
            .unwrap()
            .fingerprint()
            .unwrap();
        json!({"version":1,"cluster_id":id(1),"name":"Fleet_1","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":hash,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":"meshdb.callbacks.v1"},"provisioning":"managed","template":template,"timing":Timing::default(),"revision":1,"change_id":id(2),"policy":{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[],"intents":[]})
    }
    fn decode(v: &Value) -> Result<Control, ControlError> {
        Control::decode(&serde_json::to_vec(v).unwrap())
    }
    pub(super) fn fleet(n: usize) -> Control {
        let mut v = root();
        let hash = v["config"]["template_fingerprint"].clone();
        for k in 0..n {
            let node = id(100 + k as u128);
            let launch = id(10000 + k as u128);
            v["nodes"].as_array_mut().unwrap().push(json!({"node_id":node,"launch_id":launch,"resource_id":format!("resource/{k}"),"ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}));
            v["intents"].as_array_mut().unwrap().push(json!({"request_id":launch,"node_id":node,"launch_id":launch,"incarnation_id":null,"resource_id":format!("resource/{k}"),"action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":hash,"delivery":null,"outcome":"resource_known"}));
        }
        decode(&v).unwrap()
    }
    fn stop(c: &mut Control, k: usize, seq: u128) {
        let mut v = serde_json::to_value(&c.intents[k]).unwrap();
        v["request_id"] = json!(id(seq));
        v["action"] = json!("stop");
        v["state"] = json!("pending");
        v["outcome"] = Value::Null;
        v["template_fingerprint"] = Value::Null;
        c.intents.push(serde_json::from_value(v).unwrap());
        c.intents.sort_by_key(|i| i.request_id);
    }

    fn acquire(root: &Control, now: u64) -> Result<Control, StorageError> {
        ownership_candidate(
            root,
            OwnershipChange::Acquire { process_id: id(3) },
            now,
            id(4),
        )
    }
    fn held() -> Control {
        let mut c = fleet(0);
        c.controller = Controller {
            owner_id: Some(id(3)),
            epoch: 9,
            lease_state: LeaseState::Held,
            lease_until_ms: Some(20000),
        };
        c
    }
    fn rejected(
        c: &Control,
        change: OwnershipChange,
        now: u64,
        proposal: Uuid,
        error: StorageError,
    ) {
        let before = c.clone();
        assert_eq!(ownership_candidate(c, change, now, proposal), Err(error));
        assert_eq!(c, &before);
    }
    #[test]
    fn acquisition_obeys_expiry_boot_identity_and_epoch_continuity() {
        let mut released = fleet(0);
        released.controller.epoch = 7;
        let c = acquire(&released, 100).unwrap();
        assert_eq!(
            c.controller,
            Controller {
                owner_id: Some(id(3)),
                epoch: 8,
                lease_state: LeaseState::Held,
                lease_until_ms: Some(100 + released.timing.lease_ms)
            }
        );
        assert_eq!(acquire(&fleet(0), 0).unwrap().controller.epoch, 1);
        let old = held();
        old.validate().unwrap();
        let takeover = OwnershipChange::Acquire { process_id: id(5) };
        rejected(&old, takeover, 19999, id(4), StorageError::Conflict);
        let next = ownership_candidate(&old, takeover, 20000, id(4)).unwrap();
        assert_eq!(next.controller.epoch, 10);
        assert_eq!(next.controller.owner_id, Some(id(5)));
        assert_eq!(
            next.controller.lease_until_ms,
            Some(20000 + old.timing.lease_ms)
        );
        for now in [19999, 20000, 20001] {
            rejected(
                &old,
                OwnershipChange::Acquire { process_id: id(3) },
                now,
                id(4),
                StorageError::Conflict,
            );
        }
    }
    #[test]
    fn exact_owner_renewal_and_release_require_held_state_and_deadline_advance() {
        let c = held();
        let owner = Owner {
            process_id: id(3),
            epoch: 9,
        };
        let same_deadline = 20000 - c.timing.lease_ms;
        for now in [same_deadline - 1, same_deadline] {
            rejected(
                &c,
                OwnershipChange::Renew(owner),
                now,
                id(4),
                StorageError::Conflict,
            );
        }
        for now in [same_deadline + 1, 20001] {
            let next = ownership_candidate(&c, OwnershipChange::Renew(owner), now, id(4)).unwrap();
            assert_eq!(next.controller.owner_id, c.controller.owner_id);
            assert_eq!(next.controller.epoch, 9);
            assert_eq!(next.controller.lease_state, LeaseState::Held);
            assert_eq!(
                next.controller.lease_until_ms,
                Some(now + c.timing.lease_ms)
            );
        }
        let released =
            ownership_candidate(&c, OwnershipChange::Release(owner), u64::MAX, id(4)).unwrap();
        assert_eq!(
            released.controller,
            Controller {
                owner_id: None,
                epoch: 9,
                lease_state: LeaseState::Released,
                lease_until_ms: None
            }
        );
        assert_eq!(released.revision, c.revision + 1);
        for wrong in [
            Owner {
                process_id: id(5),
                ..owner
            },
            Owner { epoch: 8, ..owner },
        ] {
            for change in [
                OwnershipChange::Renew(wrong),
                OwnershipChange::Release(wrong),
            ] {
                rejected(&c, change, 20001, id(4), StorageError::Conflict);
            }
        }
        for change in [
            OwnershipChange::Renew(owner),
            OwnershipChange::Release(owner),
        ] {
            rejected(&released, change, 20001, id(6), StorageError::Conflict);
        }
    }
    fn assert_preserved(base: &Control, candidate: &Control, proposal: Uuid) {
        assert_eq!(candidate.revision, base.revision + 1);
        assert_eq!(candidate.change_id, proposal);
        candidate.validate().unwrap();
        let mut restored = candidate.clone();
        restored.revision = base.revision;
        restored.change_id = base.change_id;
        restored.controller = base.controller.clone();
        assert_eq!(&restored, base);
        assert_eq!(restored.encode().unwrap(), base.encode().unwrap());
        assert_eq!(candidate.capacity().unwrap(), base.capacity().unwrap());
    }
    #[test]
    fn only_revision_change_id_and_controller_change_in_representative_roots() {
        let mut c = fleet(2);
        stop(&mut c, 0, 20000);
        c.revision = 99;
        c.policy.revision = 7;
        c.admin_seq = 3;
        c.admin_head = Some(id(50));
        c.scheduler.cooldown_until_ms = 123;
        c.scheduler.backoff_until_ms = 456;
        c.scheduler.failures = 8;
        c.scheduler.low_since_ms = Some(42);
        let snapshot = c.clone();
        let acquired = acquire(&c, 100).unwrap();
        assert_preserved(&c, &acquired, id(4));
        assert_eq!(c, snapshot);
        let owner = Owner {
            process_id: id(3),
            epoch: 1,
        };
        let renewed =
            ownership_candidate(&acquired, OwnershipChange::Renew(owner), 101, id(6)).unwrap();
        assert_preserved(&acquired, &renewed, id(6));
        let released =
            ownership_candidate(&renewed, OwnershipChange::Release(owner), 0, id(7)).unwrap();
        assert_preserved(&renewed, &released, id(7));
    }
    #[test]
    fn retained_roots_need_no_new_admission_room() {
        let mut c = fleet(1024);
        for k in 0..1026 {
            stop(&mut c, 0, 20000 + k);
        }
        c.validate().unwrap();
        assert_eq!(c.check_new_admission(), Err(ControlError::LimitExceeded));
        let before = c.clone();
        let acquired = acquire(&c, 1).unwrap();
        assert_preserved(&c, &acquired, id(4));
        let owner = Owner {
            process_id: id(3),
            epoch: 1,
        };
        let renewed =
            ownership_candidate(&acquired, OwnershipChange::Renew(owner), 2, id(6)).unwrap();
        assert_preserved(&acquired, &renewed, id(6));
        let released =
            ownership_candidate(&renewed, OwnershipChange::Release(owner), 0, id(7)).unwrap();
        assert_preserved(&renewed, &released, id(7));
        assert_eq!(c, before);
    }
    #[test]
    fn checked_exhaustion_duplicate_proposal_and_local_validation_fail_without_mutation() {
        let owner = Owner {
            process_id: id(3),
            epoch: 9,
        };
        let mut c = held();
        c.revision = u64::MAX;
        c.validate().unwrap();
        for change in [
            OwnershipChange::Acquire { process_id: id(5) },
            OwnershipChange::Renew(owner),
            OwnershipChange::Release(owner),
        ] {
            rejected(&c, change, 20001, id(4), StorageError::LimitExceeded);
        }
        let mut c = fleet(0);
        c.controller.epoch = u64::MAX;
        c.validate().unwrap();
        rejected(
            &c,
            OwnershipChange::Acquire { process_id: id(3) },
            0,
            id(4),
            StorageError::LimitExceeded,
        );
        for (base, change) in [
            (fleet(0), OwnershipChange::Acquire { process_id: id(3) }),
            (held(), OwnershipChange::Renew(owner)),
        ] {
            base.validate().unwrap();
            rejected(&base, change, u64::MAX, id(4), StorageError::LimitExceeded);
        }
        for change in [
            OwnershipChange::Acquire { process_id: id(5) },
            OwnershipChange::Renew(owner),
            OwnershipChange::Release(owner),
        ] {
            let c = held();
            rejected(&c, change, 20001, c.change_id, StorageError::Conflict);
        }
        let mut malformed = fleet(0);
        malformed.name = "PRIVATE-invalid-name".into();
        assert_eq!(malformed.validate(), Err(ControlError::InvalidRecord));
        rejected(
            &malformed,
            OwnershipChange::Acquire { process_id: id(3) },
            0,
            id(4),
            StorageError::InvalidCatalog,
        );
        let mut malformed = held();
        malformed.controller.lease_until_ms = None;
        assert_eq!(malformed.validate(), Err(ControlError::InvalidRecord));
        rejected(
            &malformed,
            OwnershipChange::Release(owner),
            0,
            id(4),
            StorageError::InvalidCatalog,
        );
        let mut excessive = fleet(1);
        for k in 0..3072 {
            stop(&mut excessive, 0, 20000 + k);
        }
        assert_eq!(excessive.validate(), Err(ControlError::LimitExceeded));
        rejected(
            &excessive,
            OwnershipChange::Acquire { process_id: id(3) },
            0,
            id(4),
            StorageError::LimitExceeded,
        );
        assert_eq!(
            format!("{owner:?}"),
            format!("Owner {{ process_id: {}, epoch: 9 }}", id(3))
        );
        for error in [
            StorageError::Conflict,
            StorageError::InvalidCatalog,
            StorageError::LimitExceeded,
        ] {
            let diagnostic = format!(
                "{error}: {error:?} {malformed:?} {:?}",
                OwnershipChange::Renew(owner)
            );
            for private in [
                "PRIVATE",
                "Fleet_1",
                "/token",
                "fingerprint",
                "template",
                "resource/",
            ] {
                assert!(!diagnostic.contains(private));
            }
        }
    }
}

#[cfg(test)]
mod guard_tests;
