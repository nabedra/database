//! Permanent request identity is reserved before RAM admission. These records
//! cannot reconstruct application work: there is no operation, body, queue
//! position or authorization text. A new run never relabels an old reservation.
use super::*;
use crate::{
    oltp::pool::{
        ApplyMeta, ByteClass, ChargedBytes, GroupRead, GroupStore, GroupTransaction, ScratchLease,
        TransactionBudget, Transition,
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, Consensus},
            proof::{SealedTerminalProof, statement_digest},
        },
        owned::{OwnedCommand, OwnedPayload, serialized_digest},
        types::*,
    },
};
use serde::de::DeserializeOwned;
use std::{fmt, sync::Arc};
use uuid::Uuid;

const STATE: &[u8] = b"status/authority";
const IDENTITIES: &[u8] = b"status/identity/";
const FENCES: &[u8] = b"status/fence/";
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 1 << 20,
    batch_bytes: 1 << 20,
    output_bytes: 128 << 10,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestKind {
    Read,
    Mutation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IdentityReservation {
    pub request_id: Uuid,
    pub digest: Digest,
    pub original_run: Uuid,
    pub reservation_id: Uuid,
    pub kind: RequestKind,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TerminalIdentity {
    ReadExpired {
        reservation: IdentityReservation,
        request: RequestBinding,
        terminal_version: u64,
        response_hash: Digest,
        recorded_at: AppliedPosition,
    },
    Committed {
        reservation: IdentityReservation,
        decision: DecisionStatement,
        outcome: super::MutationOutcome,
    },
    Failed {
        reservation: IdentityReservation,
        request: RequestBinding,
        outcome: super::MutationOutcome,
        recorded_at: AppliedPosition,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum IdentityLookup {
    Reserved(IdentityReservation),
    Abandoned(IdentityReservation),
    Terminal(TerminalIdentity),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRecord {
    reservation: IdentityReservation,
    terminal: Option<TerminalIdentity>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StatusCommand {
    Fence {
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    },
    ReserveIdentity {
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        reservation_id: Uuid,
    },
    RetireRead {
        authority: Authority,
        proof: SealedTerminalProof,
    },
    BeginManifest {
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        outcome: super::MutationOutcome,
        result_digest: Digest,
    },
    ManifestPage {
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    },
    SealManifest {
        authority: Authority,
        txn: TxnId,
    },
    RecordPrepared {
        authority: Authority,
        evidence: PreparedParticipant,
    },
    DecideCommit {
        authority: Authority,
        permit: super::SealedCommitPermit,
        stamp: super::CommitStamp,
    },
    Abort {
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        outcome: super::MutationOutcome,
    },
    RecordResolved {
        authority: Authority,
        evidence: crate::oltp::mvcc::store::ResolutionEvidence,
    },
    TerminalFailure {
        authority: Authority,
        request: RequestBinding,
        outcome: super::MutationOutcome,
    },
}
impl fmt::Display for StatusCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fence { .. } => "status.fence",
            Self::ReserveIdentity { .. } => "status.reserve_identity",
            Self::RetireRead { .. } => "status.retire_read",
            Self::BeginManifest { .. } => "status.begin_manifest",
            Self::ManifestPage { .. } => "status.manifest_page",
            Self::SealManifest { .. } => "status.seal_manifest",
            Self::RecordPrepared { .. } => "status.prepared",
            Self::DecideCommit { .. } => "status.commit",
            Self::Abort { .. } => "status.abort",
            Self::RecordResolved { .. } => "status.resolved",
            Self::TerminalFailure { .. } => "status.terminal_failure",
        })
    }
}
impl OwnedPayload for StatusCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(match self {
            Self::BeginManifest { outcome, .. }
            | Self::Abort { outcome, .. }
            | Self::TerminalFailure { outcome, .. } => outcome.heap_bytes(),
            Self::ManifestPage { participants, .. } => {
                participants.capacity() * std::mem::size_of::<ParticipantPlan>()
                    + participants
                        .iter()
                        .map(|p| p.group.database.capacity_bytes())
                        .sum::<usize>()
            }
            Self::RecordPrepared { evidence, .. } => {
                evidence.participant.group.database.capacity_bytes()
            }
            Self::RecordResolved { evidence, .. } => evidence.group.database.capacity_bytes(),
            _ => 0,
        })
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
pub(crate) enum StatusFault {
    NotInitialized,
    StaleAuthority,
    InvalidBinding,
    DigestConflict,
    TransitionConflict,
    UnknownIdentity,
    ReadOnlyIdentity,
    TerminalConflict,
    Incomplete,
    AlreadyDecided,
    Abandoned,
    IdentityNotReserved,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StatusReply {
    Fenced(GroupFenceReceipt),
    Identity(IdentityLookup),
    Rejected(StatusFault),
    Manifest(super::ManifestDraft),
    ManifestSealed(ManifestRef),
    Prepared { count: u64, complete: bool },
    Decided(DecisionStatement),
    Resolved { count: u64, complete: bool },
}
#[derive(Serialize, Deserialize)]
struct FenceRecord {
    digest: Digest,
    receipt: GroupFenceReceipt,
}

#[derive(Clone)]
pub(crate) struct StatusMachine {
    store: GroupStore,
}
impl StatusMachine {
    pub(crate) fn new(store: GroupStore) -> Result<Self, ProtocolError> {
        if !matches!(store.identity().kind, GroupKind::Status { .. }) {
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
        command: OwnedCommand<StatusCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        command.ensure_charged()?;
        let digest = meta.command_digest;
        let position = meta.position;
        let scope = self.store.identity().clone();
        self.store
            .transaction(meta, BUDGET, move |tx| {
                if serialized_digest(&command)? != digest {
                    return Err(unknown());
                }
                apply(tx, &scope, position, digest, command.payload()?)
            })
            .await
    }
}
impl crate::oltp_replication::hosted::TypedMachine<super::super::config::StatusConfig>
    for StatusMachine
{
    fn store(&self) -> &GroupStore {
        self.store()
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<StatusCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.apply(meta, command).await
    }
}
fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    digest: Digest,
    command: &StatusCommand,
) -> Result<Transition, ProtocolError> {
    let current: Option<Authority> = load(tx, STATE)?;
    match command {
        StatusCommand::Fence {
            transition_id,
            expected,
            next,
        } => {
            let key = key(FENCES, *transition_id);
            if let Some(existing) = load::<FenceRecord>(tx, &key)? {
                return if existing.digest == digest {
                    commit(tx, StatusReply::Fenced(existing.receipt))
                } else {
                    reject(tx, StatusFault::TransitionConflict)
                };
            }
            if transition_id.is_nil() || next.validate().is_err() {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let valid = match current {
                None => expected.is_none(),
                Some(old) => {
                    Some(old) == *expected && old.owner_term.checked_add(1) == Some(next.owner_term)
                }
            };
            if !valid {
                return reject(tx, StatusFault::StaleAuthority);
            }
            let receipt = GroupFenceReceipt {
                group: scope.clone(),
                transition_id: *transition_id,
                expected: *expected,
                next: *next,
                applied_at: position,
                command_digest: digest,
            };
            put(tx, STATE, next)?;
            put(
                tx,
                &key,
                &FenceRecord {
                    digest,
                    receipt: receipt.clone(),
                },
            )?;
            commit(tx, StatusReply::Fenced(receipt))
        }
        StatusCommand::ReserveIdentity {
            authority,
            request_id,
            digest,
            kind,
            reservation_id,
        } => {
            let Some(current) = current else {
                return reject(tx, StatusFault::NotInitialized);
            };
            if current != *authority {
                return reject(tx, StatusFault::StaleAuthority);
            }
            if request_id.is_nil() || reservation_id.is_nil() {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let identity_key = key(IDENTITIES, *request_id);
            if let Some(record) = load::<IdentityRecord>(tx, &identity_key)? {
                if record.reservation.digest != *digest || record.reservation.kind != *kind {
                    return reject(tx, StatusFault::DigestConflict);
                }
                return commit(tx, StatusReply::Identity(lookup(current, record)));
            }
            let reservation = IdentityReservation {
                request_id: *request_id,
                digest: *digest,
                original_run: current.run_id,
                reservation_id: *reservation_id,
                kind: *kind,
            };
            put(
                tx,
                &identity_key,
                &IdentityRecord {
                    reservation,
                    terminal: None,
                },
            )?;
            commit(
                tx,
                StatusReply::Identity(IdentityLookup::Reserved(reservation)),
            )
        }
        StatusCommand::RetireRead { authority, proof } => {
            let Some(current) = current else {
                return reject(tx, StatusFault::NotInitialized);
            };
            if current != *authority {
                return reject(tx, StatusFault::StaleAuthority);
            }
            let statement = &proof.statement;
            if statement.validate().is_err()
                || statement.ready.view.run_id != current.run_id
                || statement.ready.view.owner_term > current.owner_term
                || statement_digest(statement).map_err(|_| invalid())? != proof.statement_digest
                || proof.acknowledgement_count != statement.ready.ready_count
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let identity_key = key(IDENTITIES, statement.request.request_id);
            let Some(mut record) = load::<IdentityRecord>(tx, &identity_key)? else {
                return reject(tx, StatusFault::UnknownIdentity);
            };
            if record.reservation.kind != RequestKind::Read {
                return reject(tx, StatusFault::ReadOnlyIdentity);
            }
            if record.reservation.original_run != statement.request.run_id
                || record.reservation.digest != statement.request.digest
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            if let Some(existing) = record.terminal {
                return match &existing {
                    TerminalIdentity::ReadExpired {
                        request,
                        terminal_version,
                        response_hash,
                        ..
                    } if *request == statement.request
                        && *terminal_version <= statement.terminal_version
                        && *response_hash == statement.response_hash =>
                    {
                        commit(
                            tx,
                            StatusReply::Identity(IdentityLookup::Terminal(existing)),
                        )
                    }
                    _ => reject(tx, StatusFault::TerminalConflict),
                };
            }
            let terminal = TerminalIdentity::ReadExpired {
                reservation: record.reservation,
                request: statement.request,
                terminal_version: statement.terminal_version,
                response_hash: statement.response_hash,
                recorded_at: position,
            };
            record.terminal = Some(terminal.clone());
            put(tx, &identity_key, &record)?;
            commit(
                tx,
                StatusReply::Identity(IdentityLookup::Terminal(terminal)),
            )
        }
        command => transaction::apply(tx, scope, position, current, command),
    }
}
fn lookup(authority: Authority, record: IdentityRecord) -> IdentityLookup {
    match record.terminal {
        Some(terminal) => IdentityLookup::Terminal(terminal),
        None if record.reservation.original_run == authority.run_id => {
            IdentityLookup::Reserved(record.reservation)
        }
        None => IdentityLookup::Abandoned(record.reservation),
    }
}
fn key(prefix: &[u8], id: Uuid) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 16);
    out.extend_from_slice(prefix);
    out.extend_from_slice(id.as_bytes());
    out
}
fn load<T: DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.get(key)?
        .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
        .transpose()
}
fn put(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &impl Serialize,
) -> Result<(), ProtocolError> {
    let value = tx.encode_value(value)?;
    tx.put(key, value.as_slice())
}
fn commit(tx: &mut GroupTransaction<'_>, reply: StatusReply) -> Result<Transition, ProtocolError> {
    Ok(Transition::Commit(tx.encode(&reply)?))
}
fn reject(tx: &mut GroupTransaction<'_>, fault: StatusFault) -> Result<Transition, ProtocolError> {
    Ok(Transition::Reject(
        tx.encode(&StatusReply::Rejected(fault))?,
    ))
}
fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "durable Status identity cannot be read authoritatively",
        Outcome::Unknown,
    )
}

/// Private proposal capability; ordinary worker request handlers receive only
/// the admission/lookup methods. No API can retrieve an operation from this log.
pub(crate) struct StatusHandle {
    machine: StatusMachine,
    consensus: Arc<dyn Consensus<StatusCommand, StatusReply>>,
}
impl StatusHandle {
    pub(crate) async fn retire_read(&self,authority:Authority,release:super::super::timestamp::VerifiedPinRelease)->Result<VerifiedReadRetirement,ProtocolError>{
        let proof=release.into_proof();
        match self.submit(StatusCommand::RetireRead{authority,proof}).await?.reply{
            StatusReply::Identity(IdentityLookup::Terminal(TerminalIdentity::ReadExpired{request,terminal_version,response_hash,recorded_at,..}))=>Ok(VerifiedReadRetirement{request,terminal_version,response_hash,recorded_at}),
            _=>Err(invalid()),
        }
    }
    pub(crate) async fn current_authority(&self)->Result<Option<Authority>,ProtocolError>{self.consensus.ensure_linearizable().await?;let raw=self.machine.store().read_metadata(BUDGET,|view|{let authority:Option<Authority>=read(view,STATE)?;view.encode(&authority)}).await?;serde_json::from_slice(raw.as_slice()).map_err(|_|unknown())}
    pub(crate) async fn fence(&self,transition_id:Uuid,expected:Option<Authority>,next:Authority)->Result<Applied<StatusReply>,ProtocolError>{self.submit(StatusCommand::Fence{transition_id,expected,next}).await}
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    pub(crate) fn new(
        machine: StatusMachine,
        consensus: Arc<dyn Consensus<StatusCommand, StatusReply>>,
    ) -> Arc<Self> {
        Arc::new(Self { machine, consensus })
    }
    pub(crate) async fn reserve_identity(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        let command = OwnedCommand::new_owned(
            StatusCommand::ReserveIdentity {
                authority,
                request_id,
                digest,
                kind,
                reservation_id: Uuid::new_v4(),
            },
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.consensus.propose(command).await
    }
    pub(crate) async fn lookup(
        &self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read(BUDGET, move |view| {
                let authority: Authority = read(view, STATE)?.ok_or_else(unknown)?;
                let record: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                if record.as_ref().is_some_and(|record| {
                    digest.is_some_and(|digest| record.reservation.digest != digest)
                }) {
                    return Err(invalid());
                }
                view.encode(&record.map(|record| lookup(authority, record)))
            })
            .await
    }
}
#[derive(Clone,Copy,Debug)]
pub(crate) struct VerifiedReadRetirement {request:RequestBinding,terminal_version:u64,response_hash:Digest,recorded_at:AppliedPosition}
impl VerifiedReadRetirement {
    pub(crate) fn request(&self)->&RequestBinding{&self.request}
    pub(crate) fn terminal_version(&self)->u64{self.terminal_version}
    pub(crate) fn response_hash(&self)->Digest{self.response_hash}
    pub(crate) fn recorded_at(&self)->AppliedPosition{self.recorded_at}
}
fn read<T: DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    view.get(key)?
        .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
        .transpose()
}

#[cfg(test)]
mod tests;

mod transaction;
pub(crate) use transaction::{ManifestDraft,ManifestInventoryPage,TransactionInspection};
