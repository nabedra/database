//! The live scoped retirement record contains only identity and proof metadata.

use super::{handle::rejected, *};
use crate::{
    oltp_replication::{
        authority::{consensus::Applied, control::ScopedQueuePlacement},
        multiplex::{Purpose, ReceivedReply},
        owned::serialized_digest,
        proof::hash,
    },
    oltp_runtime::replica::RecordVersion,
};
use uuid::Uuid;

pub(crate) use nabedra_oltp_types::authority::ScopedTimestampEpoch;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedTerminalStatement {
    pub epoch: ScopedTimestampEpoch,
    pub request: RequestBinding,
    pub pin_identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub payload: RecordVersion,
    pub terminal: RecordVersion,
    pub response_status: u16,
    pub response_hash: Digest,
    pub completed_at_ms: u64,
}

impl ScopedTerminalStatement {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if !self.epoch.valid()
            || self.request.run_id != self.epoch.view.run_id
            || self.request.request_id.is_nil()
            || self.payload.request_id != self.request.request_id
            || self.payload.version == 0
            || self.terminal.version <= self.payload.version
            || self.terminal.request_id != self.request.request_id
            || self.terminal.version == u64::MAX
            || self.pin_identity.run_id != self.request.run_id
            || self.pin_identity.request_id != self.request.request_id
            || self.pin_identity.digest != self.request.digest
            || self.pin_identity.nonce.is_nil()
            || self.storage_pin.pin_id.is_nil()
            || self.storage_pin.allocator_epoch == 0
            || !(self.response_status == 200 || (400..600).contains(&self.response_status))
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn for_placement(
        &self,
        placement: &ScopedQueuePlacement,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.epoch != ScopedTimestampEpoch::from_placement(placement)? {
            return Err(unknown());
        }
        Ok(())
    }

    pub(crate) fn rebases(&self, old: &Self) -> bool {
        self.epoch.advances(old.epoch)
            && self.request == old.request
            && self.pin_identity == old.pin_identity
            && self.storage_pin == old.storage_pin
            && self.payload.version == old.payload.version
            && self.terminal.version == old.terminal.version
            && self.completed_at_ms == old.completed_at_ms
            && self.response_hash == old.response_hash
            && self.response_status == old.response_status
    }

    pub(crate) fn matches_reservation(&self, reservation: IdentityReservation) -> bool {
        reservation.binds(self.request)
            && reservation.reservation_id == self.pin_identity.nonce
            && self.epoch.admits(reservation.scoped)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedTerminalWitnessReply {
    pub worker: WorkerIdentity,
    pub statement: ScopedTerminalStatement,
    pub nonce: Uuid,
}

/// Proof material can be obtained only from the authenticated response. The
/// request caller never sends either the witnesses or the sealed copy root.
pub(crate) struct ScopedTerminalWitness(ScopedTerminalWitnessReply);

impl ScopedTerminalWitness {
    pub(crate) fn authenticate(
        reply: &ReceivedReply,
        group: &GroupIdentity,
        target: WorkerIdentity,
        expected: &ScopedTerminalStatement,
        decoded: ScopedTerminalWitnessReply,
    ) -> Result<Self, ProtocolError> {
        if reply.group() != group
            || reply.remote_identity() != target
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
            || decoded.worker != target
            || decoded.statement != *expected
            || decoded.nonce.is_nil()
            || decoded.nonce != reply.nonce()
            || serialized_digest(&decoded)? != hash(&[reply.header.as_slice()])
        {
            return Err(invalid());
        }
        Ok(Self(decoded))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedTerminalCopies {
    pub statement: ScopedTerminalStatement,
    witnesses: [WorkerIdentity; 2],
    witness_root: Digest,
}

impl ScopedTerminalCopies {
    pub(super) fn seal(
        placement: &ScopedQueuePlacement,
        statement: ScopedTerminalStatement,
        mut witnesses: [ScopedTerminalWitness; 2],
    ) -> Result<Self, ProtocolError> {
        statement.for_placement(placement)?;
        witnesses.sort_unstable_by_key(|witness| witness.0.worker.worker_id);
        let [first, second] = witnesses.map(|witness| witness.0);
        if first.statement != statement
            || second.statement != statement
            || first.worker.worker_id == second.worker.worker_id
            || first.nonce == second.nonce
            || !placement.members.contains(&first.worker)
            || !placement.members.contains(&second.worker)
        {
            return Err(invalid());
        }
        Ok(Self {
            statement,
            witnesses: [first.worker, second.worker],
            witness_root: serialized_digest(&(
                "nabedra/scoped-terminal/witnesses-v1",
                first,
                second,
            ))?,
        })
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        self.statement.validate()?;
        let [first, second] = self.witnesses;
        if first.validate().is_err()
            || second.validate().is_err()
            || first.worker_id >= second.worker_id
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedCopyProof {
    copies: ScopedTerminalCopies,
    pub applied_at: AppliedPosition,
}

impl ScopedCopyProof {
    pub(crate) fn statement(&self) -> &ScopedTerminalStatement {
        &self.copies.statement
    }

    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.copies.validate()?;
        if self.applied_at.term == 0 || self.applied_at.index == 0 {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedRetirementMarker {
    pub proof: ScopedCopyProof,
    pub applied_at: AppliedPosition,
}

impl ScopedRetirementMarker {
    pub(crate) fn validate(&self, expected: &ScopedTerminalStatement) -> Result<(), ProtocolError> {
        self.proof.validate()?;
        if self.proof.statement() != expected
            || self.applied_at.term == 0
            || self.applied_at.index == 0
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedPinRelease {
    pub marker: ScopedRetirementMarker,
    pub applied_at: AppliedPosition,
}

impl ScopedPinRelease {
    pub(crate) fn validate(&self, expected: &ScopedTerminalStatement) -> Result<(), ProtocolError> {
        self.marker.validate(expected)?;
        if self.applied_at.term == 0 || self.applied_at.index == 0 {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(super) fn fence(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    transition_id: Uuid,
    previous: ScopedTimestampEpoch,
    next: ScopedTimestampEpoch,
    digest: Digest,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    if let Some(reply) = receipt(tx, transition_id, digest)? {
        return commit(tx, &reply);
    }
    if transition_id.is_nil() || !previous.valid() || !next.valid() || !next.advances(previous) {
        return reject(tx, Fault::InvalidBinding);
    }
    if state.is_none_or(|state| state.authority.run_id != next.view.run_id) {
        return reject(tx, Fault::StaleAuthority);
    }
    let key = next.key();
    if load::<ScopedTimestampEpoch>(tx, &key)?
        .is_some_and(|stored| stored != next && !next.advances(stored))
    {
        return reject(tx, Fault::StaleReadyView);
    }
    let reply = TimestampReply::ScopedQueueFenced {
        transition_id,
        epoch: next,
        applied_at: position,
    };
    put(tx, &key, &next)?;
    save_receipt(tx, transition_id, digest, &reply)?;
    commit(tx, &reply)
}

fn exact_pin(record: &PinRecord, statement: &ScopedTerminalStatement) -> bool {
    record.purpose == PinPurpose::Application
        && statement.matches_reservation(record.reservation)
        && record.identity == statement.pin_identity
        && record.pin == statement.storage_pin
        && record.request == Some(statement.request)
}

pub(super) fn stage(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    copies: ScopedTerminalCopies,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let statement = &copies.statement;
    if copies.validate().is_err() {
        return reject(tx, Fault::InvalidBinding);
    }
    let epoch = statement.epoch;
    let epoch_key = epoch.key();
    let stored_epoch = load::<ScopedTimestampEpoch>(tx, &epoch_key)?;
    if state.is_none_or(|state| state.authority.run_id != statement.request.run_id)
        || stored_epoch.is_some_and(|stored| stored != epoch)
    {
        return reject(tx, Fault::StaleReadyView);
    }
    let key = pin_key(statement.pin_identity.nonce);
    let Some(mut pin) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if !exact_pin(&pin, statement) {
        return reject(tx, Fault::InvalidBinding);
    }
    if !matches!(
        pin.state,
        PinState::Active | PinState::ScopedReleased { .. }
    ) {
        return reject(tx, Fault::PinExpired);
    }
    if let Some(existing) = &pin.scoped_copy_proof {
        if existing.statement() == statement {
            return commit(tx, &TimestampReply::ScopedCopies(existing.clone()));
        }
        if !statement.rebases(existing.statement()) {
            return reject(tx, Fault::InvalidBinding);
        }
    }
    let proof = ScopedCopyProof {
        copies,
        applied_at: position,
    };
    if stored_epoch.is_none() {
        put(tx, &epoch_key, &epoch)?;
    }
    pin.scoped_copy_proof = Some(proof.clone());
    put(tx, &key, &pin)?;
    commit(tx, &TimestampReply::ScopedCopies(proof))
}

pub(super) fn release(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    marker: ScopedRetirementMarker,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let statement = marker.proof.statement();
    marker.validate(statement)?;
    if state.is_none_or(|state| state.authority.run_id != statement.request.run_id)
        || load::<ScopedTimestampEpoch>(tx, &statement.epoch.key())? != Some(statement.epoch)
    {
        return reject(tx, Fault::StaleReadyView);
    }
    let key = pin_key(statement.pin_identity.nonce);
    let Some(mut pin) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if !exact_pin(&pin, statement) || pin.scoped_copy_proof.as_ref() != Some(&marker.proof) {
        return reject(tx, Fault::InvalidBinding);
    }
    match pin.state {
        PinState::Active => {
            let active = active_key(pin.pin);
            if tx.get(&active)?.as_ref().map(|value| value.as_slice())
                != Some(pin.identity.nonce.as_bytes().as_slice())
            {
                return Err(unknown());
            }
            tx.delete(&active)?;
        }
        PinState::ScopedReleased { release } if release.marker == marker => {
            return commit(tx, &TimestampReply::ScopedPinReleased(release));
        }
        PinState::ScopedReleased { release }
            if statement.rebases(release.marker.proof.statement()) => {}
        _ => return reject(tx, Fault::PinExpired),
    }
    let release = ScopedPinRelease {
        marker,
        applied_at: position,
    };
    pin.state = PinState::ScopedReleased {
        release: release.clone(),
    };
    put(tx, &key, &pin)?;
    commit(tx, &TimestampReply::ScopedPinReleased(release))
}

impl TimestampHandle {
    pub(crate) async fn scoped_copy_proof(
        &self,
        statement: ScopedTerminalStatement,
    ) -> Result<Option<ScopedCopyProof>, ProtocolError> {
        statement.validate()?;
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state = view.get(STATE)?.ok_or_else(unknown)?;
                let state: State =
                    serde_json::from_slice(state.as_slice()).map_err(|_| unknown())?;
                if state.authority.run_id != statement.request.run_id {
                    return Err(unknown());
                }
                let epoch = view
                    .get(&statement.epoch.key())?
                    .map(|raw| {
                        serde_json::from_slice::<ScopedTimestampEpoch>(raw.as_slice())
                            .map_err(|_| unknown())
                    })
                    .transpose()?;
                if epoch.is_some_and(|epoch| epoch != statement.epoch) {
                    return Err(unknown());
                }
                let data = view
                    .get(&pin_key(statement.pin_identity.nonce))?
                    .ok_or_else(unknown)?;
                let pin: PinRecord =
                    serde_json::from_slice(data.as_slice()).map_err(|_| unknown())?;
                if !exact_pin(&pin, &statement) {
                    return Err(invalid());
                }
                if !matches!(
                    pin.state,
                    PinState::Active | PinState::ScopedReleased { .. }
                ) {
                    return Err(unknown());
                }
                if let Some(proof) = pin.scoped_copy_proof {
                    proof.validate()?;
                    if epoch.is_none() {
                        return Err(unknown());
                    }
                    if *proof.statement() == statement {
                        return view.encode(&Some(proof));
                    }
                    if !statement.rebases(proof.statement()) {
                        return Err(invalid());
                    }
                }
                view.encode(&None::<ScopedCopyProof>)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn witness_scoped_terminal(
        &self,
        placement: &ScopedQueuePlacement,
        statement: ScopedTerminalStatement,
    ) -> Result<ScopedCopyProof, ProtocolError> {
        use futures_util::{StreamExt, stream::FuturesUnordered};

        statement.for_placement(placement)?;
        if let Some(proof) = self.scoped_copy_proof(statement.clone()).await? {
            return Ok(proof);
        }
        let _slot = self.release_proof.try_lock().map_err(|_| {
            ProtocolError::new(
                ErrorCode::Overloaded,
                "timestamp terminal copy proof already in progress",
                Outcome::NotApplied,
            )
        })?;
        let reader = self.release_reader.get().ok_or_else(unknown)?;
        let witnesses = placement
            .members
            .iter()
            .copied()
            .filter(|worker| placement.has_holder(*worker))
            .map(|worker| {
                reader.queue.scoped_terminal_state(
                    worker,
                    placement,
                    &statement,
                    self.identity().group_id,
                )
            });
        let mut pending: FuturesUnordered<_> = witnesses.collect();
        let mut first = None;
        let witnesses = loop {
            match pending.next().await {
                Some(Ok(witness)) => match first.take() {
                    Some(first) => break [first, witness],
                    None => first = Some(witness),
                },
                Some(Err(_)) => {}
                None => return Err(unknown()),
            }
        };
        drop(pending);
        if reader
            .control
            .scoped_queue_by_id(statement.epoch.queue_group_id)
            .await?
            .as_ref()
            != Some(placement)
        {
            return Err(unknown());
        }
        let copies = ScopedTerminalCopies::seal(placement, statement.clone(), witnesses)?;
        match self
            .submit(TimestampCommand::StageScopedCopies { copies })
            .await?
            .reply
        {
            TimestampReply::ScopedCopies(proof) if proof.statement() == &statement => {
                proof.validate()?;
                Ok(proof)
            }
            reply => Err(rejected(reply)),
        }
    }

    pub(crate) async fn scoped_queue_fence_receipt(
        &self,
        transition_id: Uuid,
        previous: ScopedTimestampEpoch,
        next: ScopedTimestampEpoch,
    ) -> Result<Option<TimestampReply>, ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let digest = serialized_digest(&TimestampCommand::FenceScopedQueue {
            transition_id,
            previous,
            next,
        })?;
        let raw = self.machine.store().read_metadata(BUDGET, move |view| {
            let receipt = view.get(&fence_key(transition_id))?
                .map(|raw| serde_json::from_slice::<FenceReceipt>(raw.as_slice()).map_err(|_| unknown()))
                .transpose()?;
            let Some(receipt) = receipt else {
                return view.encode(&None::<TimestampReply>);
            };
            let state = view.get(STATE)?.ok_or_else(unknown)?;
            let state: State = serde_json::from_slice(state.as_slice()).map_err(|_| unknown())?;
            let epoch = view.get(&next.key())?.ok_or_else(unknown)?;
            let epoch: ScopedTimestampEpoch = serde_json::from_slice(epoch.as_slice()).map_err(|_| unknown())?;
            if receipt.command_digest != digest
                || state.authority.run_id != next.view.run_id
                || epoch != next
                || !matches!(&receipt.reply, TimestampReply::ScopedQueueFenced {
                    transition_id: id, epoch, applied_at,
                } if *id == transition_id && *epoch == next
                    && applied_at.term > 0 && applied_at.index > 0
                    && applied_at.index <= barrier.applied.index && applied_at.term <= barrier.applied.term)
            {
                return Err(unknown());
            }
            view.encode(&Some(receipt.reply))
        }).await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn confirmed_scoped_release(
        &self,
        statement: ScopedTerminalStatement,
    ) -> Result<Option<ScopedPinRelease>, ProtocolError> {
        statement.validate()?;
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state = view.get(STATE)?.ok_or_else(unknown)?;
                let state: State =
                    serde_json::from_slice(state.as_slice()).map_err(|_| unknown())?;
                if state.authority.run_id != statement.request.run_id {
                    return Err(unknown());
                }
                let epoch = view
                    .get(&statement.epoch.key())?
                    .map(|raw| {
                        serde_json::from_slice::<ScopedTimestampEpoch>(raw.as_slice())
                            .map_err(|_| unknown())
                    })
                    .transpose()?;
                if epoch.is_some_and(|epoch| epoch != statement.epoch) {
                    return Err(unknown());
                }
                let data = view
                    .get(&pin_key(statement.pin_identity.nonce))?
                    .ok_or_else(unknown)?;
                let pin: PinRecord =
                    serde_json::from_slice(data.as_slice()).map_err(|_| unknown())?;
                if !exact_pin(&pin, &statement) {
                    return Err(invalid());
                }
                let release = match pin.state {
                    PinState::ScopedReleased { release }
                        if release.marker.proof.statement() == &statement =>
                    {
                        release.validate(&statement)?;
                        if epoch.is_none()
                            || view.get(&active_key(pin.pin))?.is_some()
                            || pin.scoped_copy_proof.as_ref() != Some(&release.marker.proof)
                        {
                            return Err(unknown());
                        }
                        Some(release)
                    }
                    PinState::Active | PinState::ScopedReleased { .. } => None,
                    _ => return Err(unknown()),
                };
                view.encode(&release)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn complete_scoped_release(
        &self,
        marker: ScopedRetirementMarker,
    ) -> Result<ScopedPinRelease, ProtocolError> {
        let expected = marker.proof.statement().clone();
        match self
            .submit(TimestampCommand::ReleaseScoped {
                marker: marker.clone(),
            })
            .await?
            .reply
        {
            TimestampReply::ScopedPinReleased(release) => {
                release.validate(&expected)?;
                if release.marker != marker {
                    return Err(unknown());
                }
                Ok(release)
            }
            reply => Err(rejected(reply)),
        }
    }

    pub(crate) async fn fence_scoped_queue(
        &self,
        transition_id: Uuid,
        previous: ScopedTimestampEpoch,
        next: ScopedTimestampEpoch,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        self.submit(TimestampCommand::FenceScopedQueue {
            transition_id,
            previous,
            next,
        })
        .await
    }
}
