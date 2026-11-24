//! Permanent request identity is reserved before RAM admission. These records
//! cannot reconstruct application work: there is no operation, body, queue
//! position or authorization text. A new run never relabels an old reservation.
use super::*;
use transaction::timestamp_point::{
    LocatedTimestampManifest, TimestampPointManifest, home_key, projection,
};
use crate::oltp::mvcc::store::timestamp_resolution::TimestampPointResolution;
use crate::oltp_replication::authority::timestamp::{
    ScopedAdmissionClosure, ScopedAdmissionFailure, ScopedCopyProof, ScopedRetirementMarker,
};
mod scoped_retirement;
pub(crate) use scoped_retirement::ScopedTerminalReceipt;
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

pub use nabedra_oltp_types::authority::{IdentityReservation, RequestKind, ScopedAdmission, TerminalIdentity};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum IdentityLookup {
    Reserved(IdentityReservation),
    Abandoned(IdentityReservation),
    TimestampPoint(IdentityReservation),
    Terminal(TerminalIdentity),
    Rejected(RejectedIdentity),
}
impl IdentityLookup {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        if let Self::Terminal(
            TerminalIdentity::Committed { outcome, .. } | TerminalIdentity::Failed { outcome, .. },
        ) = self
        {
            outcome.retain_owner(owner);
        } else if let Self::Rejected(rejection) = self {
            rejection.failure.retain_owner(owner);
        }
    }
}
pub use nabedra_oltp_types::authority::{RestoredHistoryOrigin};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRecord {
    reservation: IdentityReservation,
    #[serde(default)]
    restore_origin: Option<RestoredHistoryOrigin>,
    #[serde(default)]
    reserved_at: Option<AppliedPosition>,
    terminal: Option<TerminalIdentity>,
    #[serde(default)]
    rejection: Option<RejectedIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retirement: Option<RetirementCertificate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetirementCertificate {
    proof: SealedTerminalProof,
    timestamp_position: AppliedPosition,
    recorded_at: AppliedPosition,
}

pub use nabedra_oltp_types::authority::{RejectedIdentity};

pub(crate) fn scoped_route_moved_failure() -> Result<MutationOutcome, ProtocolError> {
    MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "pinned Catalog route moved before queue admission",
        Outcome::NotApplied,
    ))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedAdmissionRejection {
    pub(super) status_group: GroupIdentity,
    pub(super) rejection: RejectedIdentity,
    digest: Digest,
}
impl SealedAdmissionRejection {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if !matches!(self.status_group.kind, GroupKind::Status { .. })
            || self.digest != serialized_digest(&(&self.status_group, &self.rejection))?
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn rejection(&self) -> &RejectedIdentity {
        &self.rejection
    }
    pub(crate) fn status_group(&self) -> &GroupIdentity {
        &self.status_group
    }
}
pub(crate) struct VerifiedAdmissionRejection {
    sealed: SealedAdmissionRejection,
}
impl VerifiedAdmissionRejection {
    pub(crate) fn from_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        mut sealed: SealedAdmissionRejection,
        operation: u8,
    ) -> Result<Self, ProtocolError> {
        if operation != 18 {
            return Err(invalid());
        }
        context.status(operation, &sealed)?;
        if context.group() != sealed.status_group() {
            return Err(invalid());
        }
        sealed.validate()?;
        sealed.rejection.failure.retain_owner(context.owner());
        Ok(Self { sealed })
    }
    pub(crate) fn from_lookup_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        mut rejection: RejectedIdentity,
    ) -> Result<Self, ProtocolError> {
        context.status(1, &Some(IdentityLookup::Rejected(rejection.clone())))?;
        let status_group = context.group().clone();
        let digest = serialized_digest(&(&status_group, &rejection))?;
        rejection.failure.retain_owner(context.owner());
        Ok(Self {
            sealed: SealedAdmissionRejection {
                status_group,
                rejection,
                digest,
            },
        })
    }
    pub(crate) fn into_sealed(self) -> SealedAdmissionRejection {
        self.sealed
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedStatusEpoch {
    pub queue_group_id: Uuid,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
}

impl ScopedStatusEpoch {
    pub(crate) fn from_scope(
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
    ) -> Self {
        Self {
            queue_group_id: scope.queue_group_id,
            queue_generation: scope.queue_generation,
            view: scope.queue_view,
        }
    }
    pub(crate) fn from_placement(
        placement: &crate::oltp_replication::authority::control::ScopedQueuePlacement,
    ) -> Self {
        Self {
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            view: placement.view,
        }
    }
    fn key(self) -> [u8; 50] {
        let mut key = [0; 50];
        key[..2].copy_from_slice(b"sq");
        key[2..18].copy_from_slice(self.queue_group_id.as_bytes());
        key[18..34].copy_from_slice(self.queue_generation.as_bytes());
        key[34..].copy_from_slice(self.view.run_id.as_bytes());
        key
    }
    fn valid(self, current: Option<Authority>) -> bool {
        !self.queue_group_id.is_nil()
            && !self.queue_generation.is_nil()
            && self.view.validate().is_ok()
            && current.is_some_and(|authority| authority.run_id == self.view.run_id)
    }
    fn admits(self, binding: Option<ScopedAdmission>, status_group: Uuid) -> bool {
        binding
            == Some(ScopedAdmission {
                protocol_version: ScopedAdmission::VERSION,
                queue_group_id: self.queue_group_id,
                queue_generation: self.queue_generation,
                status_group_id: status_group,
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StatusCommand {
    FenceScopedQueue {
        transition_id: Uuid,
        previous: ScopedStatusEpoch,
        next: ScopedStatusEpoch,
    },
    Scoped {
        epoch: ScopedStatusEpoch,
        command: Box<StatusCommand>,
    },
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scoped: Option<ScopedAdmission>,
    },
    RetireRead {
        authority: Authority,
        proof: SealedTerminalProof,
    },
    RetireTerminal {
        authority: Authority,
        proof: SealedTerminalProof,
        timestamp_position: AppliedPosition,
    },
    RecordScopedRetirement {
        proof: ScopedCopyProof,
    },
    RejectUnaccepted {
        view: AuthorityView,
        reservation: IdentityReservation,
        identity: PinIdentity,
        pin: Option<StoragePin>,
        request: Option<RequestBinding>,
        failure: super::MutationOutcome,
    },
    RejectScopedRouteMoved {
        authority: Authority,
        reservation: IdentityReservation,
        closure: ScopedAdmissionClosure,
    },
    InstallTimestampPoint {
        authority: Authority,
        point: Box<TimestampPointManifest>,
    },
    ProjectTimestampPoint {
        authority: Authority,
        completion: Box<TimestampPointResolution>,
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
            Self::FenceScopedQueue { .. } => "status.fence_scoped_queue",
            Self::Scoped { .. } => "status.scoped",
            Self::Fence { .. } => "status.fence",
            Self::ReserveIdentity { .. } => "status.reserve_identity",
            Self::RetireRead { .. } => "status.retire_read",
            Self::RetireTerminal { .. } => "status.retire_terminal",
            Self::RecordScopedRetirement { .. } => "status.record_scoped_retirement",
            Self::RejectUnaccepted { .. } => "status.reject_unaccepted",
            Self::RejectScopedRouteMoved { .. } => "status.reject_scoped_route_moved",
            Self::InstallTimestampPoint { .. } => "status.install_timestamp_point",
            Self::ProjectTimestampPoint { .. } => "status.project_timestamp_point",
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
            Self::Scoped { command, .. } => {
                return Ok(std::mem::size_of::<Self>() + command.heap_bytes()?);
            }
            Self::BeginManifest { outcome, .. }
            | Self::Abort { outcome, .. }
            | Self::TerminalFailure { outcome, .. } => outcome.heap_bytes(),
            Self::RejectUnaccepted { failure, .. } => failure.heap_bytes(),
            Self::InstallTimestampPoint { point, .. } => {
                std::mem::size_of::<TimestampPointManifest>() + point.heap_bytes()
            }
            Self::ProjectTimestampPoint { completion, .. } => {
                std::mem::size_of::<TimestampPointResolution>() + completion.heap_bytes()
            }
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
    ScopedQueueFenced {
        transition_id: Uuid,
        epoch: ScopedStatusEpoch,
        applied_at: AppliedPosition,
    },
    Fenced(GroupFenceReceipt),
    Identity(IdentityLookup),
    Rejected(StatusFault),
    Manifest(super::ManifestDraft),
    ManifestSealed(ManifestRef),
    TimestampPointProjected(Box<projection::TimestampPointProjection>),
    Prepared { count: u64, complete: bool },
    Decided(DecisionStatement),
    Resolved { count: u64, complete: bool },
    Retired(RetirementCertificate),
    ScopedRetired(ScopedRetirementMarker),
}
impl StatusReply {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        match self {
            Self::Identity(identity) => identity.retain_owner(owner),
            Self::Manifest(draft) => draft.outcome.retain_owner(owner),
            _ => {}
        }
    }
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
    let (epoch, command) = match command {
        StatusCommand::Scoped { epoch, command } => {
            if !epoch.valid(current) {
                return reject(tx, StatusFault::StaleAuthority);
            }
            let key = epoch.key();
            if let Some(fence) = load::<ScopedStatusEpoch>(tx, &key)? {
                if fence != *epoch {
                    return reject(tx, StatusFault::StaleAuthority);
                }
            } else {
                if !matches!(command.as_ref(), StatusCommand::ReserveIdentity { .. }) {
                    return reject(tx, StatusFault::StaleAuthority);
                }
                put(tx, &key, epoch)?;
            }
            (Some(*epoch), command.as_ref())
        }
        _ => (None, command),
    };
    if let StatusCommand::FenceScopedQueue {
        transition_id,
        previous,
        next,
    } = command
    {
        let receipt_key = key(b"status/scoped-fence/", *transition_id);
        if let Some((stored_digest, reply)) = load::<(Digest, StatusReply)>(tx, &receipt_key)? {
            return if stored_digest == digest {
                commit(tx, reply)
            } else {
                reject(tx, StatusFault::TransitionConflict)
            };
        }
        if epoch.is_some()
            || transition_id.is_nil()
            || !previous.valid(current)
            || !next.valid(current)
            || previous.key() != next.key()
            || previous.view.owner_term >= next.view.owner_term
            || previous.view.membership_revision > next.view.membership_revision
        {
            return reject(tx, StatusFault::InvalidBinding);
        }
        let key = next.key();
        if let Some(stored) = load::<ScopedStatusEpoch>(tx, &key)? {
            if stored.view.owner_term > next.view.owner_term
                || stored.view.owner_term == next.view.owner_term && stored != *next
            {
                return reject(tx, StatusFault::StaleAuthority);
            }
        }
        let reply = StatusReply::ScopedQueueFenced {
            transition_id: *transition_id,
            epoch: *next,
            applied_at: position,
        };
        put(tx, &key, next)?;
        put(tx, &receipt_key, &(digest, &reply))?;
        return commit(tx, reply);
    }
    let identity_request = match command {
        StatusCommand::RejectScopedRouteMoved { reservation, .. } => Some(reservation.request_id),
        StatusCommand::InstallTimestampPoint { point, .. } => Some(point.binding.txn.request_id),
        StatusCommand::ProjectTimestampPoint { completion, .. } => {
            Some(completion.decision().locator().binding.txn.request_id)
        }
        StatusCommand::BeginManifest { txn, .. }
        | StatusCommand::ManifestPage { txn, .. }
        | StatusCommand::SealManifest { txn, .. }
        | StatusCommand::Abort { txn, .. } => Some(txn.request_id),
        StatusCommand::RecordPrepared { evidence, .. } => Some(evidence.txn.request_id),
        StatusCommand::DecideCommit { permit, .. } => Some(permit.statement().txn.request_id),
        StatusCommand::RecordResolved { evidence, .. } => Some(evidence.txn.request_id),
        StatusCommand::TerminalFailure { request, .. } => Some(request.request_id),
        StatusCommand::RecordScopedRetirement { proof } => Some(proof.statement().request.request_id),
        _ => None,
    };
    if let Some(request_id) = identity_request {
        let identity = load::<IdentityRecord>(tx, &key(IDENTITIES, request_id))?;
        let marked = tx.get(&home_key(request_id))?.is_some();
        if marked
            && !matches!(
                command,
                StatusCommand::InstallTimestampPoint { .. }
                    | StatusCommand::ProjectTimestampPoint { .. }
                    | StatusCommand::RecordScopedRetirement { .. }
            )
        {
            return reject(tx, StatusFault::TransitionConflict);
        }
        if let Some(epoch) = epoch {
            if !identity.as_ref().is_some_and(|record| {
                epoch.admits(record.reservation.scoped, scope.group_id)
                    && (!marked || record.reservation.original_run == epoch.view.run_id)
            }) {
                return reject(tx, StatusFault::InvalidBinding);
            }
        } else if identity.as_ref().is_some_and(|record| {
            record.reservation.scoped.is_some()
                && current
                    .is_some_and(|authority| authority.run_id == record.reservation.original_run)
        }) {
            return reject(tx, StatusFault::StaleAuthority);
        }
    } else if epoch.is_some() && !matches!(command, StatusCommand::ReserveIdentity { .. }) {
        return reject(tx, StatusFault::InvalidBinding);
    }
    match command {
        StatusCommand::InstallTimestampPoint { authority, point } => {
            let Some(epoch) = epoch else {
                return reject(tx, StatusFault::StaleAuthority);
            };
            if current != Some(*authority) {
                return reject(tx, StatusFault::StaleAuthority);
            }
            transaction::timestamp_point::install(tx, scope, *authority, epoch, point)
        }
        StatusCommand::ProjectTimestampPoint { authority, completion } => {
            let binding = &completion.decision().locator().binding;
            if current != Some(*authority)
                || (epoch.is_none()
                    && (binding.request.run_id == authority.run_id
                        || binding.created_authority.owner_term >= authority.owner_term))
            {
                return reject(tx, StatusFault::StaleAuthority);
            }
            projection::apply(tx, scope, position, *authority, completion)
        }
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
            scoped,
        } => {
            let Some(current) = current else {
                return reject(tx, StatusFault::NotInitialized);
            };
            if current != *authority {
                return reject(tx, StatusFault::StaleAuthority);
            }
            if request_id.is_nil()
                || reservation_id.is_nil()
                || scoped.is_some_and(|binding| !binding.valid_for(scope.group_id))
                || epoch.is_some_and(|epoch| !epoch.admits(*scoped, scope.group_id))
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let identity_key = key(IDENTITIES, *request_id);
            if let Some(record) = load::<IdentityRecord>(tx, &identity_key)? {
                if record.reservation.request_id != *request_id {
                    return Err(unknown());
                }
                verify_lookup_record(scope, &record, |decision, outcome| {
                    transaction::verify_terminal_in_apply(tx, decision, outcome)
                })?;
                if record.reservation.digest != *digest || record.reservation.kind != *kind {
                    return reject(tx, StatusFault::DigestConflict);
                }
                if scoped.is_some() && record.reservation.scoped != *scoped {
                    return reject(tx, StatusFault::StaleAuthority);
                }
                let home: Option<LocatedTimestampManifest> = load(tx, &home_key(*request_id))?;
                if let Some(home) = &home {
                    projection::verify_in_apply(tx, scope, &record, home, current, Some(position))?;
                }
                return commit(tx, StatusReply::Identity(classify_home(current, record, home.is_some())));
            }
            let reservation = IdentityReservation {
                request_id: *request_id,
                digest: *digest,
                original_run: current.run_id,
                reservation_id: *reservation_id,
                kind: *kind,
                scoped: *scoped,
            };
            put(
                tx,
                &identity_key,
                &IdentityRecord {
                    restore_origin: None,
                    reservation,
                    reserved_at: Some(position),
                    terminal: None,
                    rejection: None,
                    retirement: None,
                },
            )?;
            commit(
                tx,
                StatusReply::Identity(IdentityLookup::Reserved(reservation)),
            )
        }
        StatusCommand::RejectUnaccepted {
            view,
            reservation,
            identity,
            pin,
            request,
            failure,
        } => {
            if current != Some(view.authority()) {
                return reject(tx, StatusFault::StaleAuthority);
            }
            if view.run_id != reservation.original_run
                || identity.run_id != view.run_id
                || identity.request_id != reservation.request_id
                || identity.digest != reservation.digest
                || identity.nonce != reservation.reservation_id
                || request.is_some_and(|request| {
                    request.run_id != identity.run_id
                        || request.request_id != identity.request_id
                        || request.digest != identity.digest
                })
                || !failure.is_not_applied()
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            record_rejection(
                tx,
                RejectedIdentity {
                    reservation: *reservation,
                    view: *view,
                    identity: identity.clone(),
                    pin: *pin,
                    request: *request,
                    failure: failure.clone(),
                    recorded_at: position,
                },
                false,
            )
        }
        StatusCommand::RejectScopedRouteMoved {
            authority,
            reservation,
            closure,
        } => {
            let Some(epoch) = epoch else {
                return reject(tx, StatusFault::StaleAuthority);
            };
            if current != Some(*authority) || epoch.view.run_id != reservation.original_run {
                return reject(tx, StatusFault::StaleAuthority);
            }
            if !epoch.admits(reservation.scoped, scope.group_id)
                || closure.reservation != *reservation
                || closure.failure != ScopedAdmissionFailure::ScopeSuperseded
                || closure.identity.run_id != reservation.original_run
                || closure.identity.request_id != reservation.request_id
                || closure.identity.digest != reservation.digest
                || closure.identity.nonce != reservation.reservation_id
                || closure.identity.nonce.is_nil()
                || closure.pin.pin_id.is_nil()
                || closure.pin.allocator_epoch == 0
                || closure.applied_at.term == 0
                || closure.applied_at.node_id == 0
                || closure.applied_at.index == 0
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            record_rejection(
                tx,
                RejectedIdentity {
                    reservation: *reservation,
                    view: epoch.view,
                    identity: closure.identity.clone(),
                    pin: Some(closure.pin),
                    request: None,
                    failure: scoped_route_moved_failure()?,
                    recorded_at: position,
                },
                true,
            )
        }
        StatusCommand::RecordScopedRetirement { proof } => {
            scoped_retirement::record(tx, scope, position, epoch, proof)
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
            if record.reservation.kind != RequestKind::Read || record.reservation.scoped.is_some() {
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
        StatusCommand::RetireTerminal {
            authority,
            proof,
            timestamp_position,
        } => {
            if current != Some(*authority) {
                return reject(tx, StatusFault::StaleAuthority);
            }
            let statement = &proof.statement;
            if statement.validate().is_err()
                || statement.ready.view.authority() != *authority
                || statement.terminal_digest.is_none()
                || statement_digest(statement).map_err(|_| invalid())? != proof.statement_digest
                || proof.acknowledgement_count != statement.ready.ready_count
                || timestamp_position.term == 0
                || timestamp_position.index == 0
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let identity_key = key(IDENTITIES, statement.request.request_id);
            let Some(mut record) = load::<IdentityRecord>(tx, &identity_key)? else {
                return reject(tx, StatusFault::UnknownIdentity);
            };
            if record.reservation.scoped.is_some()
                || record.reservation.original_run != statement.request.run_id
                || record.reservation.digest != statement.request.digest
                || record.rejection.is_some()
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            if let Some(existing) = record.retirement {
                return if existing.matches(statement) {
                    commit(tx, StatusReply::Retired(existing))
                } else {
                    reject(tx, StatusFault::TerminalConflict)
                };
            }
            match (&record.terminal, record.reservation.kind) {
                (None, RequestKind::Read) => {
                    record.terminal = Some(TerminalIdentity::ReadExpired {
                        reservation: record.reservation,
                        request: statement.request,
                        terminal_version: statement.terminal_version,
                        response_hash: statement.response_hash,
                        recorded_at: position,
                    });
                }
                (
                    Some(TerminalIdentity::ReadExpired {
                        request,
                        terminal_version,
                        response_hash,
                        ..
                    }),
                    RequestKind::Read,
                ) if *request == statement.request
                    && *terminal_version <= statement.terminal_version
                    && *response_hash == statement.response_hash => {}
                (
                    Some(TerminalIdentity::Committed { decision, outcome, .. }),
                    RequestKind::Mutation,
                ) if decision.request == statement.request
                    && outcome.response_hash(statement.request.request_id)?
                        == statement.response_hash => {}
                (
                    Some(TerminalIdentity::Failed { request, outcome, .. }),
                    RequestKind::Mutation,
                ) if *request == statement.request
                    && outcome.response_hash(statement.request.request_id)?
                        == statement.response_hash => {}
                _ => return reject(tx, StatusFault::TerminalConflict),
            }
            let certificate = RetirementCertificate {
                proof: proof.clone(),
                timestamp_position: *timestamp_position,
                recorded_at: position,
            };
            record.retirement = Some(certificate.clone());
            put(tx, &identity_key, &record)?;
            commit(tx, StatusReply::Retired(certificate))
        }
        command => transaction::apply(tx, scope, position, current, command),
    }
}

fn record_rejection(
    tx: &mut GroupTransaction<'_>,
    mut candidate: RejectedIdentity,
    require_live: bool,
) -> Result<Transition, ProtocolError> {
    let request_id = candidate.reservation.request_id;
    let identity_key = key(IDENTITIES, request_id);
    let Some(mut record) = load::<IdentityRecord>(tx, &identity_key)? else {
        return reject(tx, StatusFault::UnknownIdentity);
    };
    if record.reservation != candidate.reservation
        || record.terminal.is_some()
        || require_live && (record.restore_origin.is_some() || record.retirement.is_some())
        || tx.get(&home_key(request_id))?.is_some()
        || transaction::has_transaction(tx, request_id)?
    {
        return reject(tx, StatusFault::TerminalConflict);
    }
    if let Some(existing) = record.rejection {
        if require_live {
            // A transferred owner can present the same immutable closure.
            candidate.view = existing.view;
        }
        if !existing.same_disposition(&candidate) {
            return reject(tx, StatusFault::TerminalConflict);
        }
        return commit(tx, StatusReply::Identity(IdentityLookup::Rejected(existing)));
    }
    record.rejection = Some(candidate.clone());
    put(tx, &identity_key, &record)?;
    commit(tx, StatusReply::Identity(IdentityLookup::Rejected(candidate)))
}

fn verify_lookup_record(
    scope: &GroupIdentity,
    record: &IdentityRecord,
    verify_decision: impl FnOnce(&DecisionStatement, &MutationOutcome) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError> {
    let reservation = record.reservation;
    if reservation.scoped.is_some_and(|binding| !binding.valid_for(scope.group_id)) {
        return Err(unknown());
    }
    let Some(terminal) = &record.terminal else {
        return Ok(());
    };
    if record.rejection.is_some() || !reservation.binds(terminal.request_binding()) {
        return Err(unknown());
    }
    match terminal {
        TerminalIdentity::ReadExpired {
            reservation: saved, ..
        } if reservation == *saved && reservation.kind == RequestKind::Read => Ok(()),
        TerminalIdentity::Failed {
            reservation: saved,
            outcome: MutationOutcome::Failure(_),
            ..
        } if reservation == *saved && reservation.kind == RequestKind::Mutation => Ok(()),
        TerminalIdentity::Committed {
            reservation: saved,
            decision,
            outcome,
        } if reservation == *saved
            && reservation.kind == RequestKind::Mutation
            && decision.txn.request_id == reservation.request_id
            && decision.manifest.status_group_id == scope.group_id
            && !decision.decision.retries_snapshot()
            && matches!(
                (decision.decision, outcome),
                (TxnDecision::Commit { .. }, MutationOutcome::Success(_))
                    | (TxnDecision::Abort { .. }, MutationOutcome::Failure(_))
            ) =>
        {
            // Restore retains the original receipt and archives its source
            // transaction history separately from the live transaction keys.
            if record.restore_origin.is_none() {
                verify_decision(decision, outcome)?;
            }
            Ok(())
        }
        _ => Err(unknown()),
    }
}

fn classify_home(authority: Authority, record: IdentityRecord, marked: bool) -> IdentityLookup {
    if marked && record.terminal.is_none() {
        return IdentityLookup::TimestampPoint(record.reservation);
    }
    lookup(authority, record)
}

fn lookup(authority: Authority, record: IdentityRecord) -> IdentityLookup {
    let restored = record.restore_origin.is_some();
    if let Some(rejection) = record.rejection {
        return IdentityLookup::Rejected(rejection);
    }
    match record.terminal {
        Some(TerminalIdentity::Committed { ref decision, .. })
            if record.reservation.original_run != authority.run_id
                && matches!(
                    decision.decision,
                    TxnDecision::Abort {
                        reason: AbortReason::AbandonedByFence
                    }
                ) =>
        {
            // Resolving an abandoned preparation is cleanup, not a new
            // user-visible outcome. Preserve durable COMMIT and real failures.
            IdentityLookup::Abandoned(record.reservation)
        }
        Some(terminal) => IdentityLookup::Terminal(terminal),
        None if !restored && record.reservation.original_run == authority.run_id => {
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
pub(super) fn unknown() -> ProtocolError {
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
    scoped: Option<ScopedStatusEpoch>,
}
impl StatusHandle {
    pub(crate) async fn record_scoped_route_moved(
        &self,
        authority: Authority,
        reservation: IdentityReservation,
        closure: ScopedAdmissionClosure,
    ) -> Result<RejectedIdentity, ProtocolError> {
        let applied = self
            .submit(StatusCommand::RejectScopedRouteMoved {
                authority,
                reservation,
                closure,
            })
            .await?;
        match applied.reply {
            StatusReply::Identity(IdentityLookup::Rejected(rejection)) => Ok(rejection),
            StatusReply::Rejected(StatusFault::StaleAuthority) => Err(unknown()),
            _ => Err(invalid()),
        }
    }

    /// The caller retains the actual admission gate through uncertainty. An
    /// error never grants permission to release its pin or admit the UUID again.
    pub(crate) async fn reject_unaccepted(
        &self,
        cap: &crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        if cap.scope().cluster_id != self.identity().cluster_id
            || cap.scope().database != self.identity().database
        {
            return Err(invalid());
        }
        let failure = super::MutationOutcome::failure(cap.failure().clone())?;
        if !failure.is_not_applied() {
            return Err(invalid());
        }
        let expected = RejectedIdentity {
            reservation: cap.reservation(),
            view: cap.view(),
            identity: cap.identity().clone(),
            pin: cap.pin(),
            request: cap.request(),
            failure: failure.clone(),
            recorded_at: AppliedPosition {
                term: 0,
                node_id: 0,
                index: 0,
            },
        };
        self.reject_expected(expected).await
    }
    pub(crate) async fn reject_owner_observation(
        &self,
        proof: crate::oltp_replication::service_rpc::OwnerAdmissionObservation,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        if proof.status_group() != self.identity() || !proof.expected().failure.is_not_applied() {
            return Err(invalid());
        }
        self.reject_expected(proof.expected().clone()).await
    }
    async fn reject_expected(
        &self,
        expected: RejectedIdentity,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        match self
            .submit(StatusCommand::RejectUnaccepted {
                view: expected.view,
                reservation: expected.reservation,
                identity: expected.identity.clone(),
                pin: expected.pin,
                request: expected.request,
                failure: expected.failure.clone(),
            })
            .await?
            .reply
        {
            StatusReply::Identity(IdentityLookup::Rejected(_)) => {}
            _ => return Err(invalid()),
        }
        let Some(IdentityLookup::Rejected(rejection)) = self
            .lookup_identity(
                expected.reservation.request_id,
                Some(expected.reservation.digest),
            )
            .await?
        else {
            return Err(unknown());
        };
        if !rejection.same_disposition(&expected) {
            return Err(unknown());
        }
        self.seal_rejection(rejection)
    }
    pub(crate) async fn rejected_identity(
        &self,
        expected: &RejectedIdentity,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        let Some(IdentityLookup::Rejected(rejection)) = self
            .lookup_identity(
                expected.reservation.request_id,
                Some(expected.reservation.digest),
            )
            .await?
        else {
            return Err(unknown());
        };
        if rejection != *expected {
            return Err(invalid());
        }
        self.seal_rejection(rejection)
    }
    fn seal_rejection(
        &self,
        rejection: RejectedIdentity,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        let status_group = self.identity().clone();
        let digest = serialized_digest(&(&status_group, &rejection))?;
        Ok(VerifiedAdmissionRejection {
            sealed: SealedAdmissionRejection {
                status_group,
                rejection,
                digest,
            },
        })
    }
    pub(crate) async fn retire_terminal(
        &self,
        release: super::super::timestamp::VerifiedPinRelease,
    ) -> Result<VerifiedTerminalRetirement, ProtocolError> {
        let expected = release.statement().clone();
        let timestamp_position = release.confirmed_at();
        match self
            .submit(StatusCommand::RetireTerminal {
                authority: expected.ready.view.authority(),
                proof: release.into_proof(),
                timestamp_position,
            })
            .await?
            .reply
        {
            StatusReply::Retired(certificate) if certificate.matches(&expected) => {
                Ok(VerifiedTerminalRetirement(certificate))
            }
            _ => Err(unknown()),
        }
    }
    pub(crate) async fn retirement(
        &self,
        request_id: Uuid,
        digest: Digest,
    ) -> Result<Option<VerifiedTerminalRetirement>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let record: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                let certificate = record.and_then(|record| record.retirement);
                if certificate.as_ref().is_some_and(|certificate| {
                    certificate.proof.statement.request.request_id != request_id
                        || certificate.proof.statement.request.digest != digest
                }) {
                    return Err(invalid());
                }
                view.encode(&certificate)
            })
            .await?;
        let certificate = serde_json::from_slice::<Option<RetirementCertificate>>(raw.as_slice())
            .map_err(|_| unknown())?;
        certificate
            .map(|certificate| {
                certificate.validate()?;
                Ok(VerifiedTerminalRetirement(certificate))
            })
            .transpose()
    }
    pub(crate) async fn current_authority(&self) -> Result<Option<Authority>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, |view| {
                let authority: Option<Authority> = read(view, STATE)?;
                view.encode(&authority)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }
    pub(crate) async fn fence(
        &self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        self.submit(StatusCommand::Fence {
            transition_id,
            expected,
            next,
        })
        .await
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.machine.store().resources()
    }
    pub(crate) fn new(
        machine: StatusMachine,
        consensus: Arc<dyn Consensus<StatusCommand, StatusReply>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            machine,
            consensus,
            scoped: None,
        })
    }
    pub(crate) fn with_scoped_epoch(&self, epoch: ScopedStatusEpoch) -> Self {
        Self {
            machine: self.machine.clone(),
            consensus: self.consensus.clone(),
            scoped: Some(epoch),
        }
    }

    pub(crate) async fn fence_scoped_queue(
        &self,
        transition_id: Uuid,
        previous: ScopedStatusEpoch,
        next: ScopedStatusEpoch,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        self.submit(StatusCommand::FenceScopedQueue {
            transition_id,
            previous,
            next,
        })
        .await
    }

    pub(crate) async fn scoped_queue_fence_receipt(
        &self,
        transition_id: Uuid,
        previous: ScopedStatusEpoch,
        next: ScopedStatusEpoch,
    ) -> Result<Option<StatusReply>, ProtocolError> {
        let digest = serialized_digest(&StatusCommand::FenceScopedQueue {
            transition_id, previous, next,
        })?;
        let barrier = self.consensus.ensure_linearizable().await?;
        let value = self.machine.store().read_metadata(BUDGET, move |view| {
            let receipt = read::<(Digest, StatusReply)>(
                view, &key(b"status/scoped-fence/", transition_id),
            )?;
            let current: Option<Authority> = read(view, STATE)?;
            let Some((stored_digest, reply)) = receipt else {
                return view.encode(&None::<StatusReply>);
            };
            if stored_digest != digest
                || !next.valid(current)
                || read::<ScopedStatusEpoch>(view, &next.key())? != Some(next)
                || !matches!(&reply, StatusReply::ScopedQueueFenced {
                    transition_id: id, epoch, applied_at,
                } if *id == transition_id && *epoch == next
                    && applied_at.term > 0 && applied_at.index > 0
                    && applied_at.index <= barrier.applied.index && applied_at.term <= barrier.applied.term)
            {
                return Err(unknown());
            }
            view.encode(&Some(reply))
        }).await?;
        serde_json::from_slice(value.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn reserve_identity(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        self.reserve_admission(authority, request_id, digest, kind, None)
            .await
    }
    pub(crate) async fn reserve_admission(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        scoped: Option<ScopedAdmission>,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        self.submit(StatusCommand::ReserveIdentity {
            authority,
            request_id,
            digest,
            kind,
            reservation_id: nabedra_rt::ids::uuid_v4(),
            scoped,
        })
        .await
    }
    pub(crate) async fn lookup(
        &self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        self.machine
            .store()
            .read(BUDGET, move |view| {
                let authority: Authority = read(view, STATE)?.ok_or_else(unknown)?;
                let record: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                let home: Option<LocatedTimestampManifest> = read(view, &home_key(request_id))?;
                if let Some(record) = &record {
                    if record.reservation.request_id != request_id {
                        return Err(unknown());
                    }
                    verify_lookup_record(&scope, record, |decision, outcome| {
                        transaction::verify_terminal_in_read(view, decision, outcome)
                    })?;
                    if let Some(home) = &home {
                        projection::verify_in_read(view, &scope, record, home, authority, Some(barrier.applied))?;
                    }
                    if digest.is_some_and(|digest| record.reservation.digest != digest) {
                        return Err(if record.reservation.scoped.is_some() {
                            crate::request_queue::QueueError::Conflict.protocol()
                        } else {
                            invalid()
                        });
                    }
                } else if home.is_some() {
                    return Err(unknown());
                }
                view.encode(&record.map(|record| classify_home(authority, record, home.is_some())))
            })
            .await
    }
    pub(crate) async fn admission_reservation(
        &self,
        request_id: Uuid,
    ) -> Result<Option<IdentityReservation>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let record: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                if record.as_ref().is_some_and(|record| {
                    record.reservation.request_id != request_id
                        || record
                            .reservation
                            .scoped
                            .is_some_and(|policy| !policy.valid_for(scope.group_id))
                }) {
                    return Err(unknown());
                }
                view.encode(&record.map(|record| record.reservation))
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn verify_scoped_admission(
        &self,
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
    ) -> Result<(), ProtocolError> {
        let reservation = self
            .admission_reservation(scope.request.request_id)
            .await?
            .ok_or_else(invalid)?;
        if !reservation.binds(scope.request)
            || reservation.kind != RequestKind::Mutation
            || reservation.scoped
                != Some(ScopedAdmission {
                    protocol_version: ScopedAdmission::VERSION,
                    queue_group_id: scope.queue_group_id,
                    queue_generation: scope.queue_generation,
                    status_group_id: self.identity().group_id,
                })
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) async fn lookup_identity(
        &self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<Option<IdentityLookup>, ProtocolError> {
        let raw = self.lookup(request_id, digest).await?;
        let shared = self.machine.store().resources().clone();
        let owner = Arc::new(
            shared
                .reserve_reply_decode(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        shared
            .execute(
                crate::oltp::pool::ExecutionClass::Control,
                0,
                0,
                move || {
                    let mut identity: Option<IdentityLookup> =
                        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                    if let Some(identity) = &mut identity {
                        identity.retain_owner(owner);
                    }
                    Ok(identity)
                },
            )
            .await
    }
    async fn propose_owned(
        &self,
        command: OwnedCommand<StatusCommand>,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        let (position, mut reply, owner) = self.consensus.propose(command).await?.into_parts()?;
        reply.retain_owner(owner.clone());
        Ok(crate::oltp_replication::authority::consensus::owned_applied(position, reply, owner))
    }
}
impl RetirementCertificate {
    fn validate(&self) -> Result<(), ProtocolError> {
        let statement = &self.proof.statement;
        statement.validate().map_err(|_| invalid())?;
        if statement.terminal_digest.is_none()
            || self.proof.statement_digest != statement_digest(statement).map_err(|_| invalid())?
            || self.proof.acknowledgement_count != statement.ready.ready_count
            || self.timestamp_position.term == 0
            || self.timestamp_position.index == 0
            || self.recorded_at.term == 0
            || self.recorded_at.index == 0
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn matches(&self, current: &TerminalCopyStatement) -> bool {
        let original = &self.proof.statement;
        let before = original.ready.view;
        let after = current.ready.view;
        if original.request != current.request
            || original.pin_identity != current.pin_identity
            || original.storage_pin != current.storage_pin
            || original.response_hash != current.response_hash
            || original.terminal_digest != current.terminal_digest
            || before.run_id != after.run_id
        {
            return false;
        }
        if original.ready == current.ready {
            return original.terminal_version == current.terminal_version;
        }
        // A ready transition can rebase the physical version of an immutable
        // terminal copy. It cannot change the result or original acceptance.
        let Some(term_advance) = after.owner_term.checked_sub(before.owner_term) else {
            return false;
        };
        let Some(revision_advance) = after.membership_revision.checked_sub(before.membership_revision) else {
            return false;
        };
        current.terminal_version.checked_sub(original.terminal_version).is_some_and(|advance| {
            advance != 0 && advance <= term_advance.saturating_add(revision_advance)
        })
    }
}

#[derive(Debug)]
pub(crate) struct VerifiedTerminalRetirement(RetirementCertificate);
impl VerifiedTerminalRetirement {
    pub(crate) fn into_certificate(self) -> RetirementCertificate {
        self.0
    }
    pub(crate) fn from_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        value: RetirementCertificate,
        expected: &TerminalCopyStatement,
    ) -> Result<Self, ProtocolError> {
        context.status(17, &value)?;
        value.validate()?;
        if !value.matches(expected) {
            return Err(invalid());
        }
        Ok(Self(value))
    }
    pub(crate) fn from_lookup_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        value: Option<RetirementCertificate>,
        request_id: Uuid,
        digest: Digest,
    ) -> Result<Option<Self>, ProtocolError> {
        context.status(24, &value)?;
        value
            .map(|certificate| {
                certificate.validate()?;
                if certificate.proof.statement.request.request_id != request_id
                    || certificate.proof.statement.request.digest != digest
                {
                    return Err(invalid());
                }
                Ok(Self(certificate))
            })
            .transpose()
    }
    pub(crate) fn request_id(&self) -> Uuid {
        self.0.proof.statement.request.request_id
    }
    pub(crate) fn matches(&self, current: &TerminalCopyStatement) -> bool {
        self.0.matches(current)
    }

    #[cfg(test)]
    pub(crate) fn test_certificate(statement: TerminalCopyStatement, workers: &[WorkerIdentity]) -> Self {
        use crate::oltp_replication::authority::proof::{AuthenticatedAck, TerminalVerifier};
        let ready = statement.ready;
        let digest = statement_digest(&statement).unwrap();
        let mut verifier = TerminalVerifier::begin(statement, ready).unwrap();
        for worker in workers {
            verifier.accept(*worker, &AuthenticatedAck::from_transport(*worker, digest, nabedra_rt::ids::uuid_v4())).unwrap();
        }
        let position = AppliedPosition { term: 1, node_id: 1, index: 2 };
        let certificate = RetirementCertificate {
            proof: verifier.finish(ready).unwrap().into_sealed(),
            timestamp_position: position,
            recorded_at: position,
        };
        certificate.validate().unwrap();
        Self(certificate)
    }
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

pub(crate) mod transaction;
pub(crate) use transaction::{
    ArchiveCursor, ArchivedDecision, ArchivedIdentity, ArchivedIdentityDisposition,
    CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage, StatusArchiveCutStatement,
    VerifiedStatusArchiveCut,
};
pub(crate) use transaction::{ManifestDraft, ManifestInventoryPage, TransactionInspection};
