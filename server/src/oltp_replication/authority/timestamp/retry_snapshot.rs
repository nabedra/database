//! Attempt pins are kept under the original application pin. The original
//! timestamp protects every newer snapshot from GC until terminal or cold
//! release, which deletes all attempt pins in the same batch.
use super::*;
use crate::oltp_replication::authority::status::{
    RequestKind, SealedSettlement, VerifiedSettlement,
};
use uuid::Uuid;

pub(super) type Pins = [Option<StoragePin>; MAX_QUEUED_MUTATION_ATTEMPTS as usize - 1];

fn offset(attempt: u64) -> usize {
    (attempt - 2) as usize
}

fn timestamp_key(read_ts: ReadTs) -> Vec<u8> {
    [
        b"timestamp/retry-ts/".as_slice(),
        &read_ts.get().to_be_bytes(),
    ]
    .concat()
}

pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    view: AuthorityView,
    identity: &PinIdentity,
    previous: &SealedSettlement,
    pin_id: Uuid,
) -> Result<Transition, ProtocolError> {
    let Some(mut state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.ready.view != view {
        return reject(tx, Fault::StaleReadyView);
    }
    let proof = previous.statement();
    if !proof.decision.decision.retries_snapshot()
        || !(1..MAX_QUEUED_MUTATION_ATTEMPTS).contains(&proof.txn.attempt)
        || proof.manifest.participant_count == 0
        || proof.settled_at.index < proof.decision.decided_at.index
        || previous
            .validate_for(proof.txn, &proof.request, &proof.manifest)
            .is_err()
        || proof.txn.request_id != identity.request_id
        || identity.run_id != view.run_id
        || pin_id.is_nil()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = pin_key(identity.nonce);
    let Some(mut original) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if original.identity != *identity
        || original.request != Some(proof.request)
        || original.reservation.kind != RequestKind::Mutation
        || original.reservation.scoped.is_some()
        || original.purpose != PinPurpose::Application
        || !matches!(original.state, PinState::Active)
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let pins = original.retry_pins.get_or_insert_default();
    if proof.txn.attempt > 1 && pins[offset(proof.txn.attempt)].is_none() {
        return reject(tx, Fault::UnknownPin);
    }
    let slot = &mut pins[offset(proof.txn.attempt + 1)];
    if let Some(pin) = *slot {
        return commit(tx, &TimestampReply::Snapshot(pin));
    }
    let Some(next) = state.high_watermark.checked_add(1) else {
        return reject(tx, Fault::TimestampExhausted);
    };
    let pin = StoragePin {
        pin_id,
        read_ts: Timestamp::new(next).map_err(|_| unknown())?,
        allocator_epoch: state.allocator_epoch,
    };
    *slot = Some(pin);
    put(tx, &key, &original)?;
    put(tx, &timestamp_key(pin.read_ts), &identity.nonce)?;
    state.high_watermark = next;
    put_state(tx, &state)?;
    commit(tx, &TimestampReply::Snapshot(pin))
}

pub(super) fn for_commit(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
    read_ts: ReadTs,
) -> Result<Option<PinRecord>, ProtocolError> {
    if txn.attempt <= 1 || txn.attempt > MAX_QUEUED_MUTATION_ATTEMPTS {
        return Ok(None);
    }
    let Some(nonce) = load::<Uuid>(tx, &timestamp_key(read_ts))? else {
        return Ok(None);
    };
    let mut record = load::<PinRecord>(tx, &pin_key(nonce))?.ok_or_else(unknown)?;
    let Some(pin) = record.retry_pins.and_then(|pins| pins[offset(txn.attempt)]) else {
        return Ok(None);
    };
    if pin.read_ts != read_ts || !matches!(record.state, PinState::Active) {
        return Ok(None);
    }
    record.pin = pin;
    Ok(Some(record))
}

pub(super) fn release(
    tx: &mut GroupTransaction<'_>,
    original: &mut PinRecord,
) -> Result<(), ProtocolError> {
    if let Some(pins) = original.retry_pins.take() {
        for pin in pins.into_iter().flatten() {
            tx.delete(&timestamp_key(pin.read_ts))?;
        }
    }
    Ok(())
}

impl TimestampHandle {
    pub(crate) async fn retry_snapshot(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        previous: VerifiedSettlement,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::RetrySnapshot {
                view,
                identity,
                previous: previous.into_sealed(),
                pin_id: nabedra_rt::ids::uuid_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(handle::rejected(reply)),
        }
    }
}
