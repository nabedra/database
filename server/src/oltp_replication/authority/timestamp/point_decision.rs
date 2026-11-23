//! The Timestamp log is the decision home for marked single-participant attempts.
use super::*;
use crate::oltp::{
    mvcc::store::timestamp_resolution::TimestampPointRejection,
    pool::{ByteClass, ExecutionClass},
};
use crate::oltp_replication::{
    authority::status::{MutationOutcome, identity::transaction::timestamp_point::valid_outcome},
    proof::hash,
    service_rpc::{
        VerifiedTimestampPointLocator, VerifiedTimestampPointRejection, VerifiedTimestampPointSeal,
    },
};
use uuid::Uuid;

#[cfg(test)]
mod tests;

pub(crate) use crate::oltp_replication::authority::status::identity::transaction::timestamp_point::{
    TimestampDecisionLocator, TimestampPointBinding, TimestampPointManifest,
};

pub(crate) use nabedra_oltp_types::authority::{PointDecision, matches_epoch};
use nabedra_oltp_types::authority::{abandon_outcome, conflict_outcome, valid_position};

pub(crate) const STATUS_INDEX: &[u8] = b"ts/point/status/";
const MARKERS: &[u8] = b"ts/point/marker/";
const RECORDS: &[u8] = b"ts/point/decision/";
const READ_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 32 << 10,
    batch_bytes: 0,
    output_bytes: BUDGET.output_bytes,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PointCommand {
    authority: Authority,
    locator: TimestampDecisionLocator,
    intended: MutationOutcome,
    intent: PointIntent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PointIntent {
    Commit {
        epoch: ScopedTimestampEpoch,
        seal: PreparedParticipant,
    },
    Reject {
        epoch: ScopedTimestampEpoch,
        rejection: TimestampPointRejection,
    },
    AbandonFenced {
        transition_id: Uuid,
    },
}

impl PointCommand {
    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.locator.heap_bytes()
            + self.intended.heap_bytes()
            + match &self.intent {
                PointIntent::Commit { seal, .. } => {
                    seal.participant.group.database.capacity_bytes()
                }
                PointIntent::Reject { .. } | PointIntent::AbandonFenced { .. } => 0,
            }
    }

    fn valid(&self, scope: &GroupIdentity) -> Result<bool, ProtocolError> {
        let binding = &self.locator.binding;
        if self.locator.validate().is_err()
            || binding.timestamps_group != *scope
            || binding.request.accepted_context == Digest::from_bytes([0; 32])
            || !valid_outcome(&self.intended, binding.result_digest)?
            || self.authority.validate().is_err()
        {
            return Ok(false);
        }
        Ok(match &self.intent {
            PointIntent::Commit { epoch, seal } => {
                binding.accepts_authority(self.authority)
                    && matches_epoch(binding, *epoch)
                    && self.locator.matches_seal(seal)
                    && seal.authority.owner_term <= self.authority.owner_term
            }
            PointIntent::Reject { epoch, rejection } => {
                binding.accepts_authority(self.authority)
                    && matches_epoch(binding, *epoch)
                    && rejection
                        .validate_for(&self.locator, self.authority)
                        .is_ok()
            }
            PointIntent::AbandonFenced { transition_id } => {
                !transition_id.is_nil()
                    && self.authority.owner_term > binding.created_authority.owner_term
                    && self.authority.run_id != binding.created_authority.run_id
            }
        })
    }
}

fn valid_in_state(
    decision: &PointDecision,
    scope: &GroupIdentity,
    state: &State,
    request_id: Uuid,
) -> Result<bool, ProtocolError> {
    let binding = &decision.locator.binding;
    if binding.timestamps_group != *scope
        || binding.txn.request_id != request_id
        || decision.authority.owner_term > state.authority.owner_term
        || (decision.authority.owner_term == state.authority.owner_term
            && decision.authority != state.authority)
        || matches!(decision.decision, TxnDecision::Commit { commit_ts }
            if commit_ts.get() > state.high_watermark)
    {
        return Ok(false);
    }
    decision.valid_receipt()
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid Timestamp point decision binding",
        Outcome::NotApplied,
    )
}

fn key(prefix: &[u8], request_id: Uuid) -> Vec<u8> {
    [prefix, request_id.as_bytes()].concat()
}

fn index_key(locator: &TimestampDecisionLocator) -> Vec<u8> {
    [
        STATUS_INDEX,
        locator.manifest.status_group_id.as_bytes(),
        locator.binding.txn.request_id.as_bytes(),
        &locator.binding.txn.attempt.to_be_bytes(),
    ]
    .concat()
}

pub(super) fn record_exists(
    request_id: Uuid,
    mut get: impl FnMut(&[u8]) -> Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<bool, ProtocolError> {
    Ok(get(&key(MARKERS, request_id))
        .map_err(|_| unknown())?
        .is_some()
        || get(&key(RECORDS, request_id))
            .map_err(|_| unknown())?
            .is_some())
}

fn checked_record(
    scope: &GroupIdentity,
    state: &State,
    locator: &TimestampDecisionLocator,
    mut get: impl FnMut(&[u8]) -> Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<Option<PointDecision>, ProtocolError> {
    let record = read_record_by_uuid(scope, state, locator.binding.txn.request_id, &mut get)?;
    if record.is_none() && get(&index_key(locator))?.is_some() {
        return Err(unknown());
    }
    Ok(record)
}

pub(super) fn read_record_by_uuid(
    scope: &GroupIdentity,
    state: &State,
    uuid: Uuid,
    mut get: impl FnMut(&[u8]) -> Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<Option<PointDecision>, ProtocolError> {
    let marker = get(&key(MARKERS, uuid)).map_err(|_| unknown())?;
    let raw = get(&key(RECORDS, uuid)).map_err(|_| unknown())?;
    match (marker, raw) {
        (None, None) => Ok(None),
        (Some(marker), Some(raw)) => {
            if raw.as_slice().len() > BUDGET.output_bytes
                || marker.as_slice() != hash(&[raw.as_slice()]).as_bytes()
            {
                return Err(unknown());
            }
            let decision: PointDecision =
                serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
            if !valid_in_state(&decision, scope, state, uuid)?
                || !get(&index_key(&decision.locator))?
                    .is_some_and(|index| index.as_slice() == marker.as_slice())
            {
                return Err(unknown());
            }
            Ok(Some(decision))
        }
        _ => Err(unknown()),
    }
}

fn legacy_request_prefix(locator: &TimestampDecisionLocator) -> Vec<u8> {
    [
        COMMIT_PREFIX,
        locator.manifest.status_group_id.as_bytes(),
        locator.binding.txn.request_id.as_bytes(),
    ]
    .concat()
}

fn cold_fence_covers(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    command: &PointCommand,
    transition_id: Uuid,
) -> Result<bool, ProtocolError> {
    let fence = load::<FenceReceipt>(tx, &fence_key(transition_id))?;
    Ok(matches!(fence.map(|fence| fence.reply),
        Some(TimestampReply::Fenced { allocator_epoch, receipt, .. })
            if receipt.group == *scope
                && receipt.transition_id == transition_id
                && receipt.next.run_id == command.authority.run_id
                && receipt.next.owner_term <= command.authority.owner_term
                && receipt.expected.is_some_and(|prior| prior.run_id != receipt.next.run_id)
                && command.locator.binding.storage_pin.allocator_epoch < allocator_epoch
    ))
}

fn exact_pin(pin: &PinRecord, binding: &TimestampPointBinding) -> bool {
    pin.identity == binding.pin_identity
        && pin.request == Some(binding.request)
        && pin.pin == binding.storage_pin
        && pin.purpose == PinPurpose::Application
        && pin.reservation == binding.reservation
}

fn active_pin_in_state(state: &State, pin: &PinRecord) -> bool {
    matches!(pin.state, PinState::Active)
        && pin.pin.allocator_epoch <= state.allocator_epoch
        && pin.pin.read_ts.get() <= state.high_watermark
        && pin.pin.read_ts.get() >= state.gc_safe_point
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: Option<State>,
    command: &PointCommand,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let Some(mut state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if !command.valid(scope)? {
        return reject(tx, Fault::InvalidBinding);
    }
    if state.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    let locator = &command.locator;
    let binding = &locator.binding;
    let legacy = legacy_request_prefix(locator);
    if tx.scan_page(&legacy, None, 1, |_, _| Ok(()))?.visited != 0 {
        return reject(tx, Fault::DecisionUnavailable);
    }
    if let Some(record) = checked_record(scope, &state, locator, |key| tx.get(key))? {
        if record.locator != *locator {
            return reject(tx, Fault::TransitionConflict);
        }
        let conflicting_evidence = match (record.decision, &command.intent) {
            (TxnDecision::Commit { .. }, PointIntent::Commit { seal, .. }) => {
                record.seal_at != Some(seal.sealed_at) || record.outcome != command.intended
            }
            (
                TxnDecision::Abort {
                    reason: AbortReason::Conflict,
                },
                PointIntent::Reject { rejection, .. },
            ) => record.rejection_at != Some(rejection.rejected_at()),
            _ => false,
        };
        if conflicting_evidence {
            return reject(tx, Fault::TransitionConflict);
        }
        return commit(tx, &TimestampReply::PointDecided(Box::new(record)));
    }
    if !valid_position(position) {
        return Err(unknown());
    }
    let nonce = binding.pin_identity.nonce;
    let Some(pin) = load::<PinRecord>(tx, &pin_key(nonce))? else {
        return reject(tx, Fault::UnknownPin);
    };
    if !exact_pin(&pin, binding) {
        return reject(tx, Fault::InvalidBinding);
    }
    if !active_pin_in_state(&state, &pin) {
        return reject(tx, Fault::PinExpired);
    }
    if !tx
        .get(&active_key(pin.pin))?
        .is_some_and(|raw| raw.as_slice() == nonce.as_bytes())
        || tx.get(&rejected_key(nonce))?.is_some()
    {
        return Err(unknown());
    }

    let (queue_epoch, seed) = match &command.intent {
        PointIntent::Commit { epoch, .. } | PointIntent::Reject { epoch, .. } => {
            if state.ready.ready_count == 0 {
                return reject(tx, Fault::StaleReadyView);
            }
            let epoch_key = epoch.key();
            let seed = match load::<ScopedTimestampEpoch>(tx, &epoch_key)? {
                Some(stored) if stored == *epoch => None,
                None if *epoch == binding.original_epoch => Some((epoch_key, epoch)),
                _ => return reject(tx, Fault::StaleReadyView),
            };
            (Some(*epoch), seed)
        }
        PointIntent::AbandonFenced { transition_id } => {
            if !cold_fence_covers(tx, scope, command, *transition_id)? {
                return reject(tx, Fault::InvalidBinding);
            }
            (None, None)
        }
    };
    let (decision, outcome, seal_at, rejection_at) = match &command.intent {
        PointIntent::Commit { seal, .. } => {
            let Some(next) = state.high_watermark.checked_add(1) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            let commit_ts = Timestamp::new(next).map_err(|_| unknown())?;
            state.high_watermark = next;
            put_state(tx, &state)?;
            (
                TxnDecision::Commit { commit_ts },
                command.intended.clone(),
                Some(seal.sealed_at),
                None,
            )
        }
        PointIntent::Reject { rejection, .. } => (
            TxnDecision::Abort {
                reason: AbortReason::Conflict,
            },
            conflict_outcome()?,
            None,
            Some(rejection.rejected_at()),
        ),
        PointIntent::AbandonFenced { .. } => (
            TxnDecision::Abort {
                reason: AbortReason::AbandonedByFence,
            },
            abandon_outcome()?,
            None,
            None,
        ),
    };
    if let Some((key, epoch)) = seed {
        put(tx, &key, epoch)?;
    }
    let record = PointDecision {
        locator: locator.clone(),
        authority: command.authority,
        decided_at: position,
        queue_epoch,
        seal_at,
        rejection_at,
        decision,
        outcome,
    };
    let uuid = binding.txn.request_id;
    let encoded = tx.encode_value(&record)?;
    let digest = hash(&[encoded.as_slice()]);
    tx.put(&key(MARKERS, uuid), digest.as_bytes())?;
    tx.put(&key(RECORDS, uuid), encoded.as_slice())?;
    tx.put(&index_key(locator), digest.as_bytes())?;
    commit(tx, &TimestampReply::PointDecided(Box::new(record)))
}

impl TimestampHandle {
    pub(crate) async fn verify_timestamp_point_pin(
        &self,
        binding: TimestampPointBinding,
    ) -> Result<(), ProtocolError> {
        binding.roots()?;
        if binding.timestamps_group != *self.identity()
            || binding.request.accepted_context == Digest::from_bytes([0; 32])
        {
            return Err(invalid());
        }
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(READ_BUDGET, move |view| {
                let state: State =
                    serde_json::from_slice(view.get(STATE)?.ok_or_else(unknown)?.as_slice())
                        .map_err(|_| unknown())?;
                let nonce = binding.pin_identity.nonce;
                let pin: PinRecord = serde_json::from_slice(
                    view.get(&pin_key(nonce))?.ok_or_else(unknown)?.as_slice(),
                )
                .map_err(|_| unknown())?;
                let epoch = view
                    .get(&binding.original_epoch.key())?
                    .map(|raw| {
                        serde_json::from_slice::<ScopedTimestampEpoch>(raw.as_slice())
                            .map_err(|_| unknown())
                    })
                    .transpose()?;
                if state.authority != binding.created_authority
                    || state.ready.ready_count == 0
                    || !exact_pin(&pin, &binding)
                    || !active_pin_in_state(&state, &pin)
                    || epoch.is_some_and(|epoch| epoch != binding.original_epoch)
                    || view.get(&rejected_key(nonce))?.is_some()
                    || !view
                        .get(&active_key(pin.pin))?
                        .is_some_and(|raw| raw.as_slice() == nonce.as_bytes())
                {
                    return Err(unknown());
                }
                view.encode(&())
            })
            .await
            .map(|_| ())
    }

    pub(crate) async fn commit_timestamp_point(
        &self,
        expected: &TimestampDecisionLocator,
        authority: Authority,
        epoch: ScopedTimestampEpoch,
        status: VerifiedTimestampPointLocator,
        data: VerifiedTimestampPointSeal,
    ) -> Result<ChargedBytes, ProtocolError> {
        let seal = data.evidence_for(expected).map_err(|_| unknown())?;
        let command = self.own_point_command(
            expected,
            authority,
            &status,
            PointIntent::Commit {
                epoch,
                seal: seal.clone(),
            },
        )?;
        drop((status, data));
        self.propose_point(expected, command).await
    }

    pub(crate) async fn abort_timestamp_point(
        &self,
        expected: &TimestampDecisionLocator,
        authority: Authority,
        epoch: ScopedTimestampEpoch,
        status: VerifiedTimestampPointLocator,
        data: VerifiedTimestampPointRejection,
    ) -> Result<ChargedBytes, ProtocolError> {
        let rejection = data.evidence_for(expected).map_err(|_| unknown())?;
        let command = self.own_point_command(
            expected,
            authority,
            &status,
            PointIntent::Reject {
                epoch,
                rejection: rejection.clone(),
            },
        )?;
        drop((status, data));
        self.propose_point(expected, command).await
    }

    pub(crate) async fn abandon_timestamp_point(
        &self,
        expected: &TimestampDecisionLocator,
        authority: Authority,
        transition_id: Uuid,
        status: VerifiedTimestampPointLocator,
    ) -> Result<ChargedBytes, ProtocolError> {
        let command = self.own_point_command(
            expected,
            authority,
            &status,
            PointIntent::AbandonFenced { transition_id },
        )?;
        drop(status);
        self.propose_point(expected, command).await
    }

    fn own_point_command(
        &self,
        expected: &TimestampDecisionLocator,
        authority: Authority,
        status: &VerifiedTimestampPointLocator,
        intent: PointIntent,
    ) -> Result<OwnedCommand<TimestampCommand>, ProtocolError> {
        if status.locator() != expected || expected.binding.timestamps_group != *self.identity() {
            return Err(unknown());
        }
        OwnedCommand::new_owned(
            TimestampCommand::DecidePoint(Box::new(PointCommand {
                authority,
                locator: expected.clone(),
                intended: status.outcome().clone(),
                intent,
            })),
            self.resources(),
            ByteClass::Progress,
        )
    }

    async fn propose_point(
        &self,
        expected: &TimestampDecisionLocator,
        command: OwnedCommand<TimestampCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let (_, reply, owner) = self.consensus.propose(command).await?.into_parts()?;
        let TimestampReply::PointDecided(decision) = reply else {
            return Err(unknown());
        };
        decision.verify_locator(expected)?;
        let shared = self.resources().clone();
        shared
            .clone()
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = owner;
                shared.encode_in(ByteClass::Progress, &decision, BUDGET.output_bytes)
            })
            .await
    }

    /// Return the original decision under a fresh barrier, even after pin release.
    pub(crate) async fn read_timestamp_point_decision(
        &self,
        locator: TimestampDecisionLocator,
    ) -> Result<ChargedBytes, ProtocolError> {
        let scope = self.identity().clone();
        if locator.validate().is_err() || locator.binding.timestamps_group != scope {
            return Err(unknown());
        }
        let applied = self
            .consensus
            .ensure_linearizable()
            .await
            .map_err(|_| unknown())?
            .applied;
        self.machine
            .store()
            .read_metadata(READ_BUDGET, move |view| {
                let state: State =
                    serde_json::from_slice(view.get(STATE)?.ok_or_else(unknown)?.as_slice())
                        .map_err(|_| unknown())?;
                let legacy = legacy_request_prefix(&locator);
                if view.scan_page(&legacy, None, 1, |_, _| Ok(()))?.visited != 0 {
                    return Err(unknown());
                }
                let record = checked_record(&scope, &state, &locator, |key| view.get(key))?;
                if record.as_ref().is_some_and(|saved| {
                    let decided = saved.decided_at();
                    saved.locator != locator
                        || decided.index > applied.index
                        || decided.term > applied.term
                        || (decided.index == applied.index && decided != applied)
                }) {
                    return Err(unknown());
                }
                view.encode(&record)
            })
            .await
            .map_err(|_| unknown())
    }
}
