//! Durable timestamp/pin transitions. This module is driven by the typed Raft
//! adapter; it contains no request executor and cannot reconstruct queued work.
use super::super::{
    owned::{OwnedCommand, OwnedPayload},
    types::*,
};
use super::proof::{SealedTerminalProof, statement_digest};
use super::status::{CommitStamp, IdentityReservation, SealedCommitPermit};
use crate::{
    oltp::pool::{
        ApplyMeta, ChargedBytes, GroupStore, GroupTransaction, ScratchLease, TransactionBudget,
        Transition,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt, sync::Arc};
mod handle;
pub(crate) use handle::TimestampHandle;

const STATE: &[u8] = b"ts/state";
const PIN_PREFIX: &[u8] = b"ts/pin/";
const ACTIVE_PREFIX: &[u8] = b"ts/active/";
const FENCE_PREFIX: &[u8] = b"ts/fence/";
const COMMIT_PREFIX: &[u8] = b"ts/commit/";
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 32 << 10,
    batch_bytes: 32 << 10,
    output_bytes: 4096,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TimestampCommand {
    Fence {
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    },
    FenceReady {
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    },
    BeginSnapshot {
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin_id: uuid::Uuid,
    },
    BindSnapshotContext {
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    AllocateCommit {
        permit: SealedCommitPermit,
    },
    Release {
        proof: SealedTerminalProof,
    },
    AdvanceFloor {
        authority: Authority,
    },
}
impl fmt::Display for TimestampCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fence { .. } => "timestamp.fence",
            Self::FenceReady { .. } => "timestamp.ready_fence",
            Self::BeginSnapshot { .. } => "timestamp.begin_snapshot",
            Self::BindSnapshotContext { .. } => "timestamp.bind_snapshot_context",
            Self::AllocateCommit { .. } => "timestamp.allocate_commit",
            Self::Release { .. } => "timestamp.release_pin",
            Self::AdvanceFloor { .. } => "timestamp.advance_floor",
        })
    }
}
impl OwnedPayload for TimestampCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(0)
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Fault {
    NotInitialized,
    StaleAuthority,
    StaleReadyView,
    InvalidBinding,
    TransitionConflict,
    PinExpired,
    UnknownPin,
    TimestampExhausted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TimestampReply {
    Fenced {
        ready: ReadyMembership,
        allocator_epoch: u64,
        receipt: GroupFenceReceipt,
    },
    Snapshot(StoragePin),
    ContextBound(StoragePin),
    Commit(CommitStamp),
    ReadyFenced(ReadyFenceReceipt),
    Released,
    Retention {
        authority: Authority,
        allocator_epoch: u64,
        safe_ts: u64,
        applied_at: AppliedPosition,
    },
    Rejected(Fault),
}
/// A durable receipt for Control's pending -> TSO-fenced -> published phases.
/// Re-proposing the same transition returns its original applied position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadyFenceReceipt {
    pub transition_id: uuid::Uuid,
    pub expected: ReadyMembership,
    pub next: ReadyMembership,
    pub applied_at: AppliedPosition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    authority: Authority,
    allocator_epoch: u64,
    high_watermark: u64,
    gc_safe_point: u64,
    ready: ReadyMembership,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PinState {
    Active,
    Released {
        terminal_version: u64,
        response_hash: Digest,
        proof:SealedTerminalProof,
        applied_at:AppliedPosition,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinRecord {
    identity: PinIdentity,
    reservation: IdentityReservation,
    request: Option<RequestBinding>,
    pin: StoragePin,
    state: PinState,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceReceipt {
    command_digest: Digest,
    reply: TimestampReply,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitAllocation {
    permit: SealedCommitPermit,
    stamp: CommitStamp,
}

#[derive(Clone)]
pub(crate) struct TimestampMachine {
    store: GroupStore,
}
impl crate::oltp_replication::hosted::TypedMachine<super::config::TimestampConfig>
    for TimestampMachine
{
    fn store(&self) -> &GroupStore {
        self.store()
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<TimestampCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.apply(meta, command).await
    }
}
impl TimestampMachine {
    pub(crate) fn new(store: GroupStore) -> Result<Self, ProtocolError> {
        if store.identity().kind != GroupKind::Timestamps {
            return Err(invalid());
        }
        Ok(Self { store })
    }
    pub(crate) fn store(&self) -> &GroupStore {
        &self.store
    }
    pub(crate) async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<TimestampCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        command.ensure_charged()?;
        let digest = meta.command_digest;
        let position = meta.position;
        let scope = self.store.identity().clone();
        self.store
            .transaction(meta, BUDGET, move |tx| {
                if crate::oltp_replication::owned::serialized_digest(&command)? != digest {
                    return Err(unknown());
                }
                apply(tx, &scope, command.payload()?, digest, position)
            })
            .await
    }
}
fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    command: &TimestampCommand,
    digest: Digest,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let state: Option<State> = load(tx, STATE)?;
    match command {
        TimestampCommand::Fence {
            transition_id,
            expected,
            next,
            ready,
        } => {
            if let Some(reply) = receipt(tx, *transition_id, digest)? {
                return commit(tx, &reply);
            }
            if transition_id.is_nil()
                || next.validate().is_err()
                || ready.view.validate().is_err()
                || ready.view.authority() != *next
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let epoch = match &state {
                None if expected.is_none() => 1,
                Some(old)
                    if Some(old.authority) == *expected
                        && old.authority.owner_term.checked_add(1) == Some(next.owner_term) =>
                {
                    let owner_change = old.authority.run_id == next.run_id;
                    if owner_change
                        && (old.ready.ready_root != ready.ready_root
                            || old.ready.ready_count != ready.ready_count
                            || old.ready.view.membership_revision != ready.view.membership_revision)
                    {
                        return reject(tx, Fault::InvalidBinding);
                    }
                    if !owner_change
                        && (ready.view.membership_revision < old.ready.view.membership_revision
                            || ready.ready_count != 0)
                    {
                        return reject(tx, Fault::InvalidBinding);
                    }
                    old.allocator_epoch.checked_add(1).ok_or_else(unknown)?
                }
                _ => return reject(tx, Fault::StaleAuthority),
            };
            // Every old pin remains retained across the fence. Clearing abandoned
            // application pins requires a separate all-groups recovery proof.
            let old_high = state.as_ref().map_or(0, |old| old.high_watermark);
            let old_gc = state.as_ref().map_or(0, |old| old.gc_safe_point);
            let new = State {
                authority: *next,
                allocator_epoch: epoch,
                high_watermark: old_high,
                gc_safe_point: old_gc,
                ready: *ready,
            };
            put(tx, STATE, &new)?;
            let reply = TimestampReply::Fenced {
                ready: *ready,
                allocator_epoch: epoch,
                receipt: GroupFenceReceipt {
                    group: scope.clone(),
                    transition_id: *transition_id,
                    expected: *expected,
                    next: *next,
                    applied_at: position,
                    command_digest: digest,
                },
            };
            save_receipt(tx, *transition_id, digest, &reply)?;
            commit(tx, &reply)
        }
        TimestampCommand::FenceReady {
            transition_id,
            expected,
            next,
        } => {
            if let Some(reply) = receipt(tx, *transition_id, digest)? {
                return commit(tx, &reply);
            }
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if transition_id.is_nil()
                || expected.view.authority() != next.view.authority()
                || next.view.validate().is_err()
                || expected.view.membership_revision.checked_add(1)
                    != Some(next.view.membership_revision)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if state.ready != *expected {
                return reject(tx, Fault::StaleReadyView);
            }
            state.ready = *next;
            put(tx, STATE, &state)?;
            let reply = TimestampReply::ReadyFenced(ReadyFenceReceipt {
                transition_id: *transition_id,
                expected: *expected,
                next: *next,
                applied_at: position,
            });
            save_receipt(tx, *transition_id, digest, &reply)?;
            commit(tx, &reply)
        }
        TimestampCommand::BeginSnapshot {
            view,
            identity,
            reservation,
            pin_id,
        } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.ready.view != *view {
                return reject(tx, Fault::StaleReadyView);
            }
            if state.ready.ready_count == 0
                || pin_id.is_nil()
                || identity.nonce.is_nil()
                || reservation.request_id.is_nil()
                || reservation.reservation_id.is_nil()
                || identity.run_id != state.authority.run_id
                || identity.run_id != reservation.original_run
                || identity.request_id != reservation.request_id
                || identity.digest != reservation.digest
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = pin_key(identity.nonce);
            if let Some(existing) = load::<PinRecord>(tx, &key)? {
                if existing.identity != *identity || existing.reservation != *reservation {
                    return reject(tx, Fault::InvalidBinding);
                }
                return match existing.state {
                    PinState::Active => commit(tx, &TimestampReply::Snapshot(existing.pin)),
                    PinState::Released { .. } => reject(tx, Fault::PinExpired),
                };
            }
            let Some(next) = state.high_watermark.checked_add(1) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            let read_ts = Timestamp::new(next).map_err(|_| unknown())?;
            let pin = StoragePin {
                pin_id: *pin_id,
                read_ts,
                allocator_epoch: state.allocator_epoch,
            };
            let record = PinRecord {
                identity: identity.clone(),
                reservation: *reservation,
                request: None,
                pin,
                state: PinState::Active,
            };
            put(tx, &key, &record)?;
            tx.put(&active_key(pin), identity.nonce.as_bytes())?;
            state.high_watermark = next;
            put(tx, STATE, &state)?;
            commit(tx, &TimestampReply::Snapshot(pin))
        }
        TimestampCommand::BindSnapshotContext {
            view,
            identity,
            pin,
            request,
        } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.ready.view != *view {
                return reject(tx, Fault::StaleReadyView);
            }
            if request.run_id != state.authority.run_id
                || request.run_id != identity.run_id
                || request.request_id != identity.request_id
                || request.digest != identity.digest
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = pin_key(identity.nonce);
            let Some(mut record) = load::<PinRecord>(tx, &key)? else {
                return reject(tx, Fault::UnknownPin);
            };
            if record.identity != *identity || record.pin != *pin {
                return reject(tx, Fault::InvalidBinding);
            }
            if matches!(record.state, PinState::Released { .. }) {
                return reject(tx, Fault::PinExpired);
            }
            if let Some(existing) = record.request {
                return if existing == *request {
                    commit(tx, &TimestampReply::ContextBound(*pin))
                } else {
                    reject(tx, Fault::InvalidBinding)
                };
            }
            record.request = Some(*request);
            put(tx, &key, &record)?;
            commit(tx, &TimestampReply::ContextBound(*pin))
        }
        TimestampCommand::AllocateCommit { permit } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if permit.validate().is_err() {
                return reject(tx, Fault::InvalidBinding);
            }
            let statement = permit.statement();
            if state.authority != statement.authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let key = commit_key(statement.manifest.status_group_id, statement.txn);
            if let Some(existing) = load::<CommitAllocation>(tx, &key)? {
                return if existing.permit.allocation_digest()? == permit.allocation_digest()? {
                    commit(tx, &TimestampReply::Commit(existing.stamp))
                } else {
                    reject(tx, Fault::InvalidBinding)
                };
            }
            if statement.read_ts.get() > state.high_watermark {
                return reject(tx, Fault::InvalidBinding);
            }
            let mut prefix = [0; ACTIVE_PREFIX.len() + 8];
            prefix[..ACTIVE_PREFIX.len()].copy_from_slice(ACTIVE_PREFIX);
            prefix[ACTIVE_PREFIX.len()..].copy_from_slice(&statement.read_ts.get().to_be_bytes());
            let mut nonce = None;
            tx.scan_page(&prefix, None, 1, |_, value| {
                nonce = Some(uuid::Uuid::from_slice(value).map_err(|_| unknown())?);
                Ok(())
            })?;
            let Some(nonce) = nonce else {
                return reject(tx, Fault::UnknownPin);
            };
            let Some(pin) = load::<PinRecord>(tx, &pin_key(nonce))? else {
                return Err(unknown());
            };
            if pin.pin.read_ts != statement.read_ts
                || pin.request != Some(statement.request)
                || pin.reservation.kind != super::status::RequestKind::Mutation
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let Some(next) = state.high_watermark.checked_add(1) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            let stamp = CommitStamp {
                permit_digest: permit.allocation_digest()?,
                commit_ts: Timestamp::new(next).map_err(|_| unknown())?,
                allocated_at: position,
            };
            put(
                tx,
                &key,
                &CommitAllocation {
                    permit: permit.clone(),
                    stamp,
                },
            )?;
            state.high_watermark = next;
            put(tx, STATE, &state)?;
            commit(tx, &TimestampReply::Commit(stamp))
        }
        TimestampCommand::Release { proof } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            let statement = &proof.statement;
            if statement.validate().is_err()
                || proof.statement_digest != statement_digest(statement).map_err(|_| unknown())?
                || proof.acknowledgement_count != statement.ready.ready_count
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if statement.ready != state.ready {
                return reject(tx, Fault::StaleReadyView);
            }
            let key = pin_key(statement.pin_identity.nonce);
            let Some(mut pin) = load::<PinRecord>(tx, &key)? else {
                return reject(tx, Fault::UnknownPin);
            };
            if pin.identity != statement.pin_identity
                || pin.request != Some(statement.request)
                || pin.pin != statement.storage_pin
            {
                return reject(tx, Fault::InvalidBinding);
            }
            match pin.state {
                PinState::Released {
                    terminal_version,
                    response_hash,
                    ..
                } => {
                    if terminal_version > statement.terminal_version
                        || response_hash != statement.response_hash
                    {
                        return reject(tx, Fault::InvalidBinding);
                    }
                }
                PinState::Active => {
                    pin.state = PinState::Released {
                        terminal_version: statement.terminal_version,
                        response_hash: statement.response_hash,
                        proof:proof.clone(),applied_at:position,
                    };
                    put(tx, &key, &pin)?;
                    tx.delete(&active_key(pin.pin))?;
                }
            }
            commit(tx, &TimestampReply::Released)
        }
        TimestampCommand::AdvanceFloor { authority } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let mut minimum = None;
            tx.scan_page(ACTIVE_PREFIX, None, 1, |key, _| {
                let suffix = key.strip_prefix(ACTIVE_PREFIX).ok_or_else(unknown)?;
                if suffix.len() != 24 {
                    return Err(unknown());
                }
                minimum = Some(u64::from_be_bytes(
                    suffix[..8].try_into().map_err(|_| unknown())?,
                ));
                Ok(())
            })?;
            let next = minimum.unwrap_or(state.high_watermark);
            if next < state.gc_safe_point || next > state.high_watermark {
                return Err(unknown());
            }
            state.gc_safe_point = next;
            put(tx, STATE, &state)?;
            commit(
                tx,
                &TimestampReply::Retention {
                    authority: state.authority,
                    allocator_epoch: state.allocator_epoch,
                    safe_ts: next,
                    applied_at: position,
                },
            )
        }
    }
}
/// Created only after an actual durable TSO release/strong confirmation. It
/// carries no executable work and cannot be deserialized by ordinary callers.
#[derive(Clone)]
pub(crate) struct VerifiedPinRelease {proof:SealedTerminalProof,confirmed_at:AppliedPosition}
impl VerifiedPinRelease {
    pub(crate) fn statement(&self)->&TerminalCopyStatement{&self.proof.statement}
    pub(crate) fn confirmed_at(&self)->AppliedPosition{self.confirmed_at}
    pub(in crate::oltp_replication::authority) fn into_proof(self)->SealedTerminalProof{self.proof}
}
fn receipt(
    tx: &mut GroupTransaction<'_>,
    id: uuid::Uuid,
    digest: Digest,
) -> Result<Option<TimestampReply>, ProtocolError> {
    let receipt: Option<FenceReceipt> = load(tx, &fence_key(id))?;
    match receipt {
        Some(receipt) if receipt.command_digest == digest => Ok(Some(receipt.reply)),
        Some(_) => Ok(Some(TimestampReply::Rejected(Fault::TransitionConflict))),
        None => Ok(None),
    }
}
fn save_receipt(
    tx: &mut GroupTransaction<'_>,
    id: uuid::Uuid,
    digest: Digest,
    reply: &TimestampReply,
) -> Result<(), ProtocolError> {
    put(
        tx,
        &fence_key(id),
        &FenceReceipt {
            command_digest: digest,
            reply: reply.clone(),
        },
    )
}
fn load<T: DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.get(key)
        .map_err(|_| unknown())?
        .map(|bytes| serde_json::from_slice(bytes.as_slice()).map_err(|_| unknown()))
        .transpose()
}
fn put(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &impl Serialize,
) -> Result<(), ProtocolError> {
    let bytes = tx.encode_value(value)?;
    tx.put(key, bytes.as_slice())
}
fn commit(
    tx: &mut GroupTransaction<'_>,
    reply: &TimestampReply,
) -> Result<Transition, ProtocolError> {
    Ok(Transition::Commit(tx.encode(reply)?))
}
fn reject(tx: &mut GroupTransaction<'_>, fault: Fault) -> Result<Transition, ProtocolError> {
    Ok(Transition::Reject(
        tx.encode(&TimestampReply::Rejected(fault))?,
    ))
}
fn pin_key(nonce: uuid::Uuid) -> [u8; PIN_PREFIX.len() + 16] {
    let mut key = [0; PIN_PREFIX.len() + 16];
    key[..PIN_PREFIX.len()].copy_from_slice(PIN_PREFIX);
    key[PIN_PREFIX.len()..].copy_from_slice(nonce.as_bytes());
    key
}
fn fence_key(id: uuid::Uuid) -> [u8; FENCE_PREFIX.len() + 16] {
    let mut key = [0; FENCE_PREFIX.len() + 16];
    key[..FENCE_PREFIX.len()].copy_from_slice(FENCE_PREFIX);
    key[FENCE_PREFIX.len()..].copy_from_slice(id.as_bytes());
    key
}
fn active_key(pin: StoragePin) -> [u8; ACTIVE_PREFIX.len() + 24] {
    let mut key = [0; ACTIVE_PREFIX.len() + 24];
    key[..ACTIVE_PREFIX.len()].copy_from_slice(ACTIVE_PREFIX);
    key[ACTIVE_PREFIX.len()..ACTIVE_PREFIX.len() + 8]
        .copy_from_slice(&pin.read_ts.get().to_be_bytes());
    key[ACTIVE_PREFIX.len() + 8..].copy_from_slice(pin.pin_id.as_bytes());
    key
}
fn commit_key(group: uuid::Uuid, txn: TxnId) -> [u8; COMMIT_PREFIX.len() + 40] {
    let mut key = [0; COMMIT_PREFIX.len() + 40];
    let n = COMMIT_PREFIX.len();
    key[..n].copy_from_slice(COMMIT_PREFIX);
    key[n..n + 16].copy_from_slice(group.as_bytes());
    key[n + 16..n + 32].copy_from_slice(txn.request_id.as_bytes());
    key[n + 32..].copy_from_slice(&txn.attempt.to_be_bytes());
    key
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "timestamp adapter requires a Timestamps group",
        Outcome::NotApplied,
    )
}
fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "timestamp durable state is unavailable",
        Outcome::Unknown,
    )
}

#[cfg(test)]
mod tests;
