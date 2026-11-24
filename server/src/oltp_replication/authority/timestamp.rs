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
pub(crate) mod archive;
pub(crate) mod archive_lifecycle;
pub(crate) mod cold;
pub(crate) mod direct;
pub(crate) mod direct_client;
mod handle;
mod index_metadata;
pub(crate) mod point_decision;
mod retry_snapshot;
mod scoped_admission;
mod scoped_retirement;
mod service;
pub(crate) mod split;
pub(crate) use handle::TimestampHandle;
pub(crate) use index_metadata::IndexMetadataPin;
pub(crate) use point_decision::{
    PointDecision, TimestampDecisionLocator, TimestampPointBinding, TimestampPointManifest,
};
pub(crate) use scoped_admission::{ScopedAdmissionClosure, ScopedAdmissionFailure};
pub(crate) use scoped_retirement::{
    ScopedCopyProof, ScopedPinRelease, ScopedRetirementMarker, ScopedTerminalStatement,
    ScopedTerminalWitness, ScopedTerminalWitnessReply, ScopedTimestampEpoch,
};
// Stable surface for the direct path (T4 RangeClock, T5 executor, T7 gate).
#[allow(unused_imports)]
pub(crate) use direct::{
    DEFAULT_DIRECT_WINDOW, DirectTimestampGate, DirectTimestampRange, DirectTimestampSource,
    DirectWindowStats, MAX_DIRECT_COUNT, MAX_DIRECT_WINDOW,
};
#[allow(unused_imports)]
pub(crate) use direct_client::DirectTimestampClient;
pub(crate) use service::TimestampService;

/// Per-Status-group archive closure. Only the TSO leader's allocation/decision
/// closure service can construct this capability; deserializing a cut is not
/// evidence that every earlier allocation reached an immutable settled result.
pub(crate) struct VerifiedArchiveClosure {
    cut: CommitTs,
    authority: Authority,
    status: GroupIdentity,
    status_applied_at: AppliedPosition,
    archive_id: uuid::Uuid,
    statement: archive::ArchiveClosureStatement,
    _owner: Arc<ScratchLease>,
}
impl VerifiedArchiveClosure {
    pub(crate) fn cut(&self) -> CommitTs {
        self.cut
    }
    pub(crate) fn authority(&self) -> Authority {
        self.authority
    }
    pub(crate) fn covers_status(&self, group: &GroupIdentity) -> bool {
        self.status == *group && matches!(group.kind, GroupKind::Status { .. })
    }
    pub(crate) fn status_applied_at(&self) -> AppliedPosition {
        self.status_applied_at
    }
    pub(crate) fn archive_id(&self) -> uuid::Uuid {
        self.archive_id
    }
    pub(crate) fn base_exclusive(&self) -> Option<CommitTs> {
        self.statement.base_exclusive
    }
    pub(crate) fn statement(&self) -> &archive::ArchiveClosureStatement {
        &self.statement
    }
}

const STATE: &[u8] = b"ts/state";
const PIN_PREFIX: &[u8] = b"ts/pin/";
const ACTIVE_PREFIX: &[u8] = b"ts/active/";
const FENCE_PREFIX: &[u8] = b"ts/fence/";
const COMMIT_PREFIX: &[u8] = b"ts/commit/";
const REJECTED_PREFIX: &[u8] = b"ts/rejected/";
const MAINTENANCE_PREFIX: &[u8] = b"ts/maintenance/";
const MAINTENANCE_ACTIVE: &[u8] = b"ts/maintenance-active/";
const BACKUP_PREFIX: &[u8] = b"ts/backup/";
const BACKUP_ACTIVE: &[u8] = b"ts/backup-active/";
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
    RetrySnapshot {
        view: AuthorityView,
        identity: PinIdentity,
        previous: super::status::SealedSettlement,
        pin_id: uuid::Uuid,
    },
    BindSnapshotContext {
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    ScopedSnapshot(scoped_admission::Command),
    AllocateCommit {
        permit: SealedCommitPermit,
    },
    DecidePoint(Box<point_decision::PointCommand>),
    Release {
        proof: SealedTerminalProof,
    },
    FenceScopedQueue {
        transition_id: uuid::Uuid,
        previous: ScopedTimestampEpoch,
        next: ScopedTimestampEpoch,
    },
    StageScopedCopies {
        copies: scoped_retirement::ScopedTerminalCopies,
    },
    ReleaseScoped {
        marker: ScopedRetirementMarker,
    },
    CancelUnaccepted {
        proof: super::status::SealedAdmissionRejection,
    },
    ReleaseColdApplication {
        authority: Authority,
        transition_id: uuid::Uuid,
        pin: cold::ColdApplicationPin,
    },
    BeginMaintenance {
        authority: Authority,
        owner: MaintenanceOwnerRef,
        pin_id: uuid::Uuid,
    },
    ReleaseMaintenance {
        authority: Authority,
        closure: MaintenanceClosure,
    },
    BeginIndexMetadata {
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        pin_id: uuid::Uuid,
    },
    ReleaseIndexMetadata {
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: super::status::SealedSettlement,
    },
    BeginImportMetadata {
        authority: Authority,
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
        pin_id: uuid::Uuid,
    },
    ReleaseImportMetadata {
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: super::status::SealedSettlement,
    },
    BeginBackup {
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
        pin_id: uuid::Uuid,
    },
    ReleaseBackup {
        authority: Authority,
        proof: crate::oltp::backup::SealedBackupPublished,
    },
    SealArchive {
        proof: archive::SealedArchiveClosure,
    },
    ArchiveControl {
        command: archive_lifecycle::ArchiveControl,
    },
    SplitMetadata {
        command: split::SplitTimestampCommand,
    },
    AdvanceFloor {
        authority: Authority,
    },
    /// Raise `high_watermark` by `count` for the leader's in-memory direct
    /// window. Proposed only behind `DirectTimestampGate`, because a replica
    /// built before this variant cannot decode it.
    ReserveWindow {
        authority: Authority,
        count: u64,
    },
}
impl fmt::Display for TimestampCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fence { .. } => "timestamp.fence",
            Self::FenceReady { .. } => "timestamp.ready_fence",
            Self::BeginSnapshot { .. } => "timestamp.begin_snapshot",
            Self::RetrySnapshot { .. } => "timestamp.retry_snapshot",
            Self::BindSnapshotContext { .. } => "timestamp.bind_snapshot_context",
            Self::ScopedSnapshot(command) => command.name(),
            Self::AllocateCommit { .. } => "timestamp.allocate_commit",
            Self::DecidePoint(_) => "timestamp.decide_point",
            Self::Release { .. } => "timestamp.release_pin",
            Self::FenceScopedQueue { .. } => "timestamp.fence_scoped_queue",
            Self::StageScopedCopies { .. } => "timestamp.stage_scoped_copies",
            Self::ReleaseScoped { .. } => "timestamp.release_scoped_pin",
            Self::CancelUnaccepted { .. } => "timestamp.cancel_unaccepted",
            Self::ReleaseColdApplication { .. } => "timestamp.release_cold_application",
            Self::BeginMaintenance { .. } => "timestamp.begin_maintenance",
            Self::ReleaseMaintenance { .. } => "timestamp.release_maintenance",
            Self::BeginIndexMetadata { .. } => "timestamp.begin_index_metadata",
            Self::ReleaseIndexMetadata { .. } => "timestamp.release_index_metadata",
            Self::AdvanceFloor { .. } => "timestamp.advance_floor",
            Self::BeginImportMetadata { .. } => "timestamp.begin_import_metadata",
            Self::ReleaseImportMetadata { .. } => "timestamp.release_import_metadata",
            Self::BeginBackup { .. } => "timestamp.begin_backup",
            Self::ReleaseBackup { .. } => "timestamp.release_backup",
            Self::SealArchive { .. } => "timestamp.seal_archive",
            Self::ArchiveControl { .. } => "timestamp.archive_control",
            Self::SplitMetadata { .. } => "timestamp.split_metadata",
            Self::ReserveWindow { .. } => "timestamp.reserve_window",
        })
    }
}
impl OwnedPayload for TimestampCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(match self {
            Self::SealArchive { proof } => proof.heap_bytes(),
            Self::SplitMetadata { command } => command.heap_bytes()?,
            Self::DecidePoint(command) => command.heap_bytes(),
            Self::ScopedSnapshot(command) => command.heap_bytes(),
            Self::ReleaseMaintenance { closure, .. } => closure
                .restored_observation
                .as_ref()
                .map_or(0, |observation| {
                    observation.target.database.capacity_bytes()
                }),
            Self::CancelUnaccepted { proof } => {
                proof.status_group().database.capacity_bytes()
                    + proof.rejection().failure.heap_bytes()
            }
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
pub(crate) enum Fault {
    NotInitialized,
    StaleAuthority,
    StaleReadyView,
    InvalidBinding,
    TransitionConflict,
    PinExpired,
    UnknownPin,
    TimestampExhausted,
    DecisionUnavailable,
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
    ScopedSnapshot {
        pin: StoragePin,
        origin: nabedra_oltp_types::queue::OriginalAuthorization,
    },
    ContextBound(StoragePin),
    Commit(CommitStamp),
    PointDecided(Box<PointDecision>),
    ReadyFenced(ReadyFenceReceipt),
    Released,
    ScopedQueueFenced {
        transition_id: uuid::Uuid,
        epoch: ScopedTimestampEpoch,
        applied_at: AppliedPosition,
    },
    ScopedCopies(ScopedCopyProof),
    ScopedPinReleased(ScopedPinRelease),
    Archive(archive::ArchiveClosureStatement),
    Retention {
        authority: Authority,
        allocator_epoch: u64,
        safe_ts: u64,
        applied_at: AppliedPosition,
    },
    Rejected(Fault),
    ScopedAdmissionClosed(ScopedAdmissionClosure),
    /// `ReserveWindow` result: `[first, last]` now lies at or below the
    /// durable `high_watermark`.
    Window {
        authority: Authority,
        first: CommitTs,
        last: CommitTs,
    },
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
        proof: SealedTerminalProof,
        applied_at: AppliedPosition,
    },
    ScopedReleased { release: ScopedPinRelease },
    Cancelled,
    ColdReleased,
    MetadataReleased {
        decision: Digest,
        applied_at: AppliedPosition,
    },
    ScopeSuperseded {
        applied_at: AppliedPosition,
    },
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PinPurpose {
    #[default]
    Application,
    IndexMetadata {
        owner: MaintenanceOwnerRef,
        txn: TxnId,
    },
    ImportMetadata {
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
    },
    SplitMetadata { txn: TxnId },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinRecord {
    identity: PinIdentity,
    reservation: IdentityReservation,
    request: Option<RequestBinding>,
    pin: StoragePin,
    state: PinState,
    #[serde(default)]
    purpose: PinPurpose,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_pins: Option<retry_snapshot::Pins>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scoped_copy_proof: Option<ScopedCopyProof>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MaintenanceClosure {
    statement: crate::oltp::mvcc::store::index::IndexClosureStatement,
    digest: Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    restored_observation: Option<crate::oltp::mvcc::store::index::RestoredIndexClosureObservation>,
}
impl MaintenanceClosure {
    fn from_verified(
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> Result<Self, ProtocolError> {
        let statement = proof.statement();
        let digest = crate::oltp_replication::owned::serialized_digest(&statement)?;
        Ok(Self {
            statement,
            digest,
            restored_observation: proof.restored_observation().cloned(),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceRecord {
    owner: MaintenanceOwnerRef,
    authority: Authority,
    pin: StoragePin,
    closed: Option<MaintenanceClosure>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRecord {
    owner: crate::oltp::backup::BackupOwnerRef,
    authority: Authority,
    pin: StoragePin,
    published: Option<crate::oltp::backup::SealedBackupPublished>,
}

#[derive(Clone)]
pub(crate) struct TimestampMachine {
    store: GroupStore,
    direct: Arc<direct::DirectWindows>,
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
        Ok(Self {
            direct: direct::DirectWindows::for_store(&store),
            store,
        })
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
        let windows = self.direct.clone();
        let budget = if matches!(command.payload()?, TimestampCommand::SplitMetadata { .. }) {
            split::BUDGET
        } else {
            BUDGET
        };
        self.store
            .transaction(meta, budget, move |tx| {
                if crate::oltp_replication::owned::serialized_digest(&command)? != digest {
                    return Err(unknown());
                }
                let capture = direct::StateCapture::begin();
                let transition = apply(tx, &scope, command.payload()?, digest, position)?;
                // Discard the direct window before this reply can reach anyone.
                if let (Transition::Commit(_), Some(mark)) = (&transition, capture.finish()) {
                    windows.observe(mark);
                }
                Ok(transition)
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
        TimestampCommand::FenceScopedQueue { transition_id, previous, next } => {
            scoped_retirement::fence(tx, state, *transition_id, *previous, *next, digest, position)
        }
        TimestampCommand::ScopedSnapshot(command) => {
            scoped_admission::apply(tx, scope, state, command, position)
        }
        TimestampCommand::StageScopedCopies { copies } => {
            scoped_retirement::stage(tx, state, copies.clone(), position)
        }
        TimestampCommand::ReleaseScoped { marker } => {
            scoped_retirement::release(tx, state, marker.clone(), position)
        }
        TimestampCommand::ArchiveControl { command } => {
            archive_lifecycle::apply(tx, state, command)
        }
        TimestampCommand::SplitMetadata { command } => {
            split::apply(tx, state, scope, command, position)
        }
        TimestampCommand::SealArchive { proof } => archive::apply_closure(tx, scope, state, proof),
        TimestampCommand::ReserveWindow { authority, count } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            if *count == 0 || *count > direct::MAX_DIRECT_WINDOW {
                return reject(tx, Fault::InvalidBinding);
            }
            let (Some(first), Some(last)) = (
                state.high_watermark.checked_add(1),
                state.high_watermark.checked_add(*count),
            ) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            state.high_watermark = last;
            put_state(tx, &state)?;
            commit(
                tx,
                &TimestampReply::Window {
                    authority: *authority,
                    first: Timestamp::new(first).map_err(|_| unknown())?,
                    last: Timestamp::new(last).map_err(|_| unknown())?,
                },
            )
        }
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
            let old_high = match &state {
                Some(old) => old.high_watermark,
                None => archive::restored_watermark(tx, scope)?,
            };
            let old_gc = state.as_ref().map_or(0, |old| old.gc_safe_point);
            let new = State {
                authority: *next,
                allocator_epoch: epoch,
                high_watermark: old_high,
                gc_safe_point: old_gc,
                ready: *ready,
            };
            put_state(tx, &new)?;
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
            put_state(tx, &state)?;
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
        } => scoped_admission::begin(tx, state, *view, identity, reservation, *pin_id, None),
        TimestampCommand::RetrySnapshot {
            view,
            identity,
            previous,
            pin_id,
        } => retry_snapshot::begin(tx, state, *view, identity, previous, *pin_id),
        TimestampCommand::BindSnapshotContext {
            view,
            identity,
            pin,
            request,
        } => scoped_admission::bind(tx, state, *view, identity, *pin, *request, None),
        TimestampCommand::DecidePoint(command) => {
            point_decision::apply(tx, scope, state, command, position)
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
            if point_decision::record_exists(statement.txn.request_id, |key| tx.get(key))? {
                return reject(tx, Fault::DecisionUnavailable);
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
            let pin = if let Some(nonce) = nonce {
                load::<PinRecord>(tx, &pin_key(nonce))?.ok_or_else(unknown)?
            } else if let Some(pin) =
                retry_snapshot::for_commit(tx, statement.txn, statement.read_ts)?
            {
                pin
            } else {
                return reject(tx, Fault::UnknownPin);
            };
            if pin.pin.read_ts != statement.read_ts
                || pin.request != Some(statement.request)
                || pin.reservation.kind != super::status::RequestKind::Mutation
                || pin.reservation.scoped.is_some_and(|binding| {
                    binding.status_group_id != statement.manifest.status_group_id
                        || pin.purpose != PinPurpose::Application
                        || !pin.reservation.binds(statement.request)
                        || !matches!(pin.state, PinState::Active)
                })
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
            put_state(tx, &state)?;
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
            if pin.reservation.scoped.is_some()
                || pin.identity != statement.pin_identity
                || pin.request != Some(statement.request)
                || pin.pin != statement.storage_pin
                || pin.purpose != PinPurpose::Application
            {
                return reject(tx, Fault::InvalidBinding);
            }
            match &pin.state {
                PinState::Released {
                    terminal_version,
                    response_hash,
                    proof: stored,
                    ..
                } => {
                    if *terminal_version > statement.terminal_version
                        || *response_hash != statement.response_hash
                        || stored
                            .statement
                            .terminal_digest
                            .is_some_and(|digest| statement.terminal_digest != Some(digest))
                        || ((*terminal_version == statement.terminal_version)
                            != (stored.statement.ready == statement.ready))
                    {
                        return reject(tx, Fault::InvalidBinding);
                    }
                    if stored.statement == *statement {
                        return commit(tx, &TimestampReply::Released);
                    }
                }
                PinState::Active => {
                    tx.delete(&active_key(pin.pin))?;
                    retry_snapshot::release(tx, &mut pin)?;
                }
                PinState::ScopedReleased { .. }
                | PinState::ScopeSuperseded { .. }
                | PinState::Cancelled
                | PinState::ColdReleased
                | PinState::MetadataReleased { .. } => {
                    return reject(tx, Fault::PinExpired);
                }
            }
            pin.state = PinState::Released {
                terminal_version: statement.terminal_version,
                response_hash: statement.response_hash,
                proof: proof.clone(),
                applied_at: position,
            };
            put(tx, &key, &pin)?;
            commit(tx, &TimestampReply::Released)
        }
        TimestampCommand::CancelUnaccepted { proof } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            proof.validate()?;
            let rejection = proof.rejection();
            if proof.status_group().cluster_id != scope.cluster_id
                || proof.status_group().database != scope.database
                || rejection.view.owner_term > state.authority.owner_term
            {
                return reject(tx, Fault::StaleReadyView);
            }
            let nonce = rejection.identity.nonce;
            if nonce.is_nil()
                || nonce != rejection.reservation.reservation_id
                || rejection.identity.run_id != rejection.reservation.original_run
                || rejection.identity.run_id != rejection.view.run_id
                || rejection.identity.request_id != rejection.reservation.request_id
                || rejection.identity.digest != rejection.reservation.digest
                || (rejection.pin.is_none() && rejection.request.is_some())
                || !rejection.failure.is_not_applied()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let marker = rejected_key(nonce);
            if let Some(existing) = load::<super::status::SealedAdmissionRejection>(tx, &marker)? {
                return if serialized_rejection(&existing)? == serialized_rejection(proof)? {
                    commit(tx, &TimestampReply::Released)
                } else {
                    reject(tx, Fault::InvalidBinding)
                };
            }
            let key = pin_key(nonce);
            if let Some(mut record) = load::<PinRecord>(tx, &key)? {
                if record.identity != rejection.identity
                    || record.reservation != rejection.reservation
                    || record.purpose != PinPurpose::Application
                    || record.request != rejection.request
                    || !matches!(record.state, PinState::Active)
                {
                    return reject(tx, Fault::InvalidBinding);
                }
                if let Some(pin) = rejection.pin {
                    if record.pin != pin {
                        return reject(tx, Fault::InvalidBinding);
                    }
                } else if rejection.view != state.ready.view {
                    // The owner proved absence of a queue copy under this view's
                    // live UUID gate. After a view change that observation alone
                    // cannot rule out staging by a successor.
                    return reject(tx, Fault::StaleReadyView);
                }
                let active = active_key(record.pin);
                if !tx
                    .get(&active)?
                    .is_some_and(|value| value.as_slice() == nonce.as_bytes())
                {
                    return Err(unknown());
                }
                record.state = PinState::Cancelled;
                put(tx, &key, &record)?;
                tx.delete(&active)?;
            } else if rejection.pin.is_some() {
                return reject(tx, Fault::UnknownPin);
            }
            put(tx, &marker, proof)?;
            commit(tx, &TimestampReply::Released)
        }
        TimestampCommand::ReleaseColdApplication {
            authority,
            transition_id,
            pin,
        } => cold::release(tx, state, *authority, *transition_id, pin),
        TimestampCommand::AdvanceFloor { authority } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let independent = retention_candidate(tx, state.high_watermark, None)?;
            let next = split::advance_retention(tx, independent)?;
            if next < state.gc_safe_point || next > state.high_watermark {
                return Err(unknown());
            }
            state.gc_safe_point = next;
            put_state(tx, &state)?;
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
        TimestampCommand::BeginMaintenance {
            authority,
            owner,
            pin_id,
        } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            if owner.attempt == 0
                || owner.catalog_group_id.is_nil()
                || owner.table_id.is_nil()
                || owner.index_id.is_nil()
                || owner.build_id.is_nil()
                || pin_id.is_nil()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = maintenance_key(owner);
            if let Some(record) = load::<MaintenanceRecord>(tx, &key)? {
                return if record.owner == *owner
                    && record.closed.is_none()
                    && record.authority.run_id == authority.run_id
                {
                    commit(tx, &TimestampReply::Snapshot(record.pin))
                } else {
                    reject(tx, Fault::PinExpired)
                };
            }
            if archive::metadata::has_restored_maintenance(tx, *owner)? {
                return reject(tx, Fault::PinExpired);
            }
            let Some(next) = state.high_watermark.checked_add(1) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            let pin = StoragePin {
                pin_id: *pin_id,
                read_ts: Timestamp::new(next).map_err(|_| unknown())?,
                allocator_epoch: state.allocator_epoch,
            };
            let record = MaintenanceRecord {
                owner: *owner,
                authority: *authority,
                pin,
                closed: None,
            };
            put(tx, &key, &record)?;
            tx.put(&maintenance_active_key(pin), &key)?;
            state.high_watermark = next;
            put_state(tx, &state)?;
            commit(tx, &TimestampReply::Snapshot(pin))
        }
        TimestampCommand::ReleaseMaintenance { authority, closure } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            if crate::oltp_replication::owned::serialized_digest(&closure.statement)?
                != closure.digest
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = maintenance_key(&closure.statement.owner);
            let Some(mut record) = load::<MaintenanceRecord>(tx, &key)? else {
                let Some(observation) = &closure.restored_observation else {
                    return reject(tx, Fault::UnknownPin);
                };
                return match archive::metadata::restored_index_closure(
                    tx, scope, closure.statement, observation,
                )? {
                    Some(true) => commit(tx, &TimestampReply::Released),
                    Some(false) => reject(tx, Fault::TransitionConflict),
                    None => reject(tx, Fault::UnknownPin),
                };
            };
            if closure.restored_observation.is_some()
                || record.owner != closure.statement.owner
                || record.pin != closure.statement.pin
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if let Some(prior) = &record.closed {
                if prior.statement.owner != record.owner
                    || prior.statement.pin != record.pin
                    || crate::oltp_replication::owned::serialized_digest(&prior.statement)?
                        != prior.digest
                {
                    return Err(unknown());
                }
                return if prior.digest == closure.digest {
                    commit(tx, &TimestampReply::Released)
                } else {
                    reject(tx, Fault::TransitionConflict)
                };
            }
            record.closed = Some(closure.clone());
            put(tx, &key, &record)?;
            tx.delete(&maintenance_active_key(record.pin))?;
            commit(tx, &TimestampReply::Released)
        }
        TimestampCommand::BeginBackup {
            authority,
            owner,
            pin_id,
        } => {
            let Some(mut state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            if owner.backup_id.is_nil() || owner.attempt == 0 || pin_id.is_nil() {
                return reject(tx, Fault::InvalidBinding);
            }
            if archive_lifecycle::cancelled(tx, *owner)? {
                return reject(tx, Fault::PinExpired);
            }
            let key = backup_key(*owner);
            if let Some(record) = load::<BackupRecord>(tx, &key)? {
                return if record.published.is_none() {
                    commit(tx, &TimestampReply::Snapshot(record.pin))
                } else {
                    reject(tx, Fault::PinExpired)
                };
            }
            let Some(next) = state.high_watermark.checked_add(1) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            let pin = StoragePin {
                pin_id: *pin_id,
                read_ts: Timestamp::new(next).map_err(|_| unknown())?,
                allocator_epoch: state.allocator_epoch,
            };
            put(
                tx,
                &key,
                &BackupRecord {
                    owner: *owner,
                    authority: *authority,
                    pin,
                    published: None,
                },
            )?;
            tx.put(&backup_active_key(pin), &key)?;
            state.high_watermark = next;
            put_state(tx, &state)?;
            commit(tx, &TimestampReply::Snapshot(pin))
        }
        TimestampCommand::ReleaseBackup { authority, proof } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let statement = proof.validate()?;
            let key = backup_key(statement.owner);
            let Some(mut record) = load::<BackupRecord>(tx, &key)? else {
                return reject(tx, Fault::UnknownPin);
            };
            if record.owner != statement.owner || record.pin != statement.pin {
                return reject(tx, Fault::InvalidBinding);
            }
            if let Some(prior) = record.published {
                return if crate::oltp_replication::owned::serialized_digest(&prior)?
                    == crate::oltp_replication::owned::serialized_digest(proof)?
                {
                    commit(tx, &TimestampReply::Released)
                } else {
                    reject(tx, Fault::TransitionConflict)
                };
            }
            record.published = Some(*proof);
            put(tx, &key, &record)?;
            tx.delete(&backup_active_key(record.pin))?;
            commit(tx, &TimestampReply::Released)
        }
        TimestampCommand::BeginIndexMetadata {
            authority,
            owner,
            txn,
            reservation,
            pin_id,
        } => {
            if owner.attempt == 0
                || owner.catalog_group_id.is_nil()
                || owner.table_id.is_nil()
                || owner.index_id.is_nil()
                || owner.build_id.is_nil()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            begin_metadata(
                tx,
                state,
                authority,
                txn,
                reservation,
                pin_id,
                PinPurpose::IndexMetadata {
                    owner: *owner,
                    txn: *txn,
                },
            )
        }
        TimestampCommand::BeginImportMetadata {
            authority,
            archive_id,
            target_id,
            txn,
            reservation,
            pin_id,
        } => {
            if archive_id.is_nil() || target_id.is_nil() {
                return reject(tx, Fault::InvalidBinding);
            }
            begin_metadata(
                tx,
                state,
                authority,
                txn,
                reservation,
                pin_id,
                PinPurpose::ImportMetadata {
                    archive_id: *archive_id,
                    target_id: *target_id,
                    txn: *txn,
                },
            )
        }
        TimestampCommand::ReleaseIndexMetadata {
            authority,
            identity,
            pin,
            settled,
        }
        | TimestampCommand::ReleaseImportMetadata {
            authority,
            identity,
            pin,
            settled,
        } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let key = pin_key(identity.nonce);
            let Some(mut record) = load::<PinRecord>(tx, &key)? else {
                return reject(tx, Fault::UnknownPin);
            };
            let txn = match (
                record.purpose,
                matches!(command, TimestampCommand::ReleaseImportMetadata { .. }),
            ) {
                (PinPurpose::IndexMetadata { txn, .. }, false)
                | (PinPurpose::ImportMetadata { txn, .. }, true) => txn,
                _ => return reject(tx, Fault::InvalidBinding),
            };
            let Some(request) = record.request else {
                return reject(tx, Fault::InvalidBinding);
            };
            if record.identity != *identity
                || record.pin != *pin
                || settled
                    .validate_for(txn, &request, &settled.statement().manifest)
                    .is_err()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let digest = crate::oltp_replication::owned::serialized_digest(settled)?;
            match record.state {
                PinState::MetadataReleased { decision, .. } if decision == digest => {
                    return commit(tx, &TimestampReply::Released);
                }
                PinState::Active => {}
                _ => return reject(tx, Fault::PinExpired),
            }
            record.state = PinState::MetadataReleased {
                decision: digest,
                applied_at: position,
            };
            put(tx, &key, &record)?;
            tx.delete(&active_key(*pin))?;
            commit(tx, &TimestampReply::Released)
        }
    }
}
fn begin_metadata(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    authority: &Authority,
    txn: &TxnId,
    reservation: &IdentityReservation,
    pin_id: &uuid::Uuid,
    purpose: PinPurpose,
) -> Result<Transition, ProtocolError> {
    let Some(mut state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.authority != *authority {
        return reject(tx, Fault::StaleAuthority);
    }
    if state.ready.ready_count == 0
        || txn.attempt == 0
        || txn.request_id != reservation.request_id
        || reservation.original_run != authority.run_id
        || reservation.kind != super::status::RequestKind::Mutation
        || reservation.reservation_id.is_nil()
        || pin_id.is_nil()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let identity = PinIdentity {
        run_id: reservation.original_run,
        request_id: reservation.request_id,
        digest: reservation.digest,
        nonce: reservation.reservation_id,
    };
    let key = pin_key(identity.nonce);
    if let Some(record) = load::<PinRecord>(tx, &key)? {
        if record.identity != identity
            || record.reservation != *reservation
            || record.purpose != purpose
        {
            return reject(tx, Fault::InvalidBinding);
        }
        return if matches!(record.state, PinState::Active) {
            commit(tx, &TimestampReply::Snapshot(record.pin))
        } else {
            reject(tx, Fault::PinExpired)
        };
    }
    if tx.get(&rejected_key(identity.nonce))?.is_some() {
        return reject(tx, Fault::PinExpired);
    }
    let Some(next) = state.high_watermark.checked_add(1) else {
        return reject(tx, Fault::TimestampExhausted);
    };
    let pin = StoragePin {
        pin_id: *pin_id,
        read_ts: Timestamp::new(next).map_err(|_| unknown())?,
        allocator_epoch: state.allocator_epoch,
    };
    put(
        tx,
        &key,
        &PinRecord {
            identity: identity.clone(),
            reservation: *reservation,
            request: None,
            pin,
            state: PinState::Active,
            purpose,
            scoped_copy_proof: None,
            retry_pins: None,
        },
    )?;
    tx.put(&active_key(pin), identity.nonce.as_bytes())?;
    state.high_watermark = next;
    put_state(tx, &state)?;
    commit(tx, &TimestampReply::Snapshot(pin))
}
fn maintenance_key(owner: &MaintenanceOwnerRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(MAINTENANCE_PREFIX.len() + 72);
    key.extend_from_slice(MAINTENANCE_PREFIX);
    for id in [
        owner.catalog_group_id,
        owner.table_id,
        owner.index_id,
        owner.build_id,
    ] {
        key.extend_from_slice(id.as_bytes());
    }
    key.extend_from_slice(&owner.attempt.to_be_bytes());
    key
}
fn backup_key(owner: crate::oltp::backup::BackupOwnerRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(BACKUP_PREFIX.len() + 24);
    key.extend_from_slice(BACKUP_PREFIX);
    key.extend_from_slice(owner.backup_id.as_bytes());
    key.extend_from_slice(&owner.attempt.to_be_bytes());
    key
}
fn backup_active_key(pin: StoragePin) -> Vec<u8> {
    let mut key = Vec::with_capacity(BACKUP_ACTIVE.len() + 24);
    key.extend_from_slice(BACKUP_ACTIVE);
    key.extend_from_slice(&pin.read_ts.get().to_be_bytes());
    key.extend_from_slice(pin.pin_id.as_bytes());
    key
}

fn retention_candidate(
    tx: &mut GroupTransaction<'_>,
    mut floor: u64,
    replacing_backup: Option<&[u8]>,
) -> Result<u64, ProtocolError> {
    for prefix in [ACTIVE_PREFIX, MAINTENANCE_ACTIVE, BACKUP_ACTIVE] {
        let replaced = replacing_backup.filter(|_| prefix == BACKUP_ACTIVE);
        tx.scan_page(
            prefix,
            None,
            1 + usize::from(replaced.is_some()),
            |key, _| {
                if replaced == Some(key) {
                    return Ok(());
                }
                let suffix = key
                    .strip_prefix(prefix)
                    .filter(|suffix| suffix.len() == 24)
                    .ok_or_else(unknown)?;
                let ts = u64::from_be_bytes(suffix[..8].try_into().map_err(|_| unknown())?);
                floor = floor.min(ts);
                Ok(())
            },
        )?;
    }
    Ok(floor)
}
fn maintenance_active_key(pin: StoragePin) -> Vec<u8> {
    let mut key = Vec::with_capacity(MAINTENANCE_ACTIVE.len() + 24);
    key.extend_from_slice(MAINTENANCE_ACTIVE);
    key.extend_from_slice(&pin.read_ts.get().to_be_bytes());
    key.extend_from_slice(pin.pin_id.as_bytes());
    key
}
/// Created only after an actual durable TSO release/strong confirmation. It
/// carries no executable work and cannot be deserialized by ordinary callers.
#[derive(Clone)]
pub(crate) struct VerifiedPinRelease {
    proof: SealedTerminalProof,
    confirmed_at: AppliedPosition,
}
impl VerifiedPinRelease {
    pub(crate) fn wire_parts(&self) -> (SealedTerminalProof, AppliedPosition) {
        (self.proof.clone(), self.confirmed_at)
    }
    pub(crate) fn from_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        proof: SealedTerminalProof,
        confirmed_at: AppliedPosition,
        expected: &TerminalCopyStatement,
        operation: u8,
    ) -> Result<Self, ProtocolError> {
        if operation != 5 && operation != 6 {
            return Err(invalid());
        }
        context.timestamp(operation, &(&proof, confirmed_at))?;
        if proof.statement != *expected
            || proof.statement_digest != statement_digest(expected).map_err(|_| invalid())?
            || proof.acknowledgement_count != expected.ready.ready_count
            || confirmed_at.term == 0
            || confirmed_at.index == 0
        {
            return Err(invalid());
        }
        Ok(Self {
            proof,
            confirmed_at,
        })
    }
    pub(crate) fn statement(&self) -> &TerminalCopyStatement {
        &self.proof.statement
    }
    pub(crate) fn confirmed_at(&self) -> AppliedPosition {
        self.confirmed_at
    }
    pub(in crate::oltp_replication::authority) fn into_proof(self) -> SealedTerminalProof {
        self.proof
    }
}
fn rejected_key(nonce: uuid::Uuid) -> Vec<u8> {
    let mut key = Vec::with_capacity(REJECTED_PREFIX.len() + 16);
    key.extend_from_slice(REJECTED_PREFIX);
    key.extend_from_slice(nonce.as_bytes());
    key
}
fn serialized_rejection(
    proof: &super::status::SealedAdmissionRejection,
) -> Result<Digest, ProtocolError> {
    crate::oltp_replication::owned::serialized_digest(proof)
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
    // Timestamp state writes must go through `put_state` so that the direct
    // window sees every watermark move.
    debug_assert_ne!(key, STATE);
    let bytes = tx.encode_value(value)?;
    tx.put(key, bytes.as_slice())
}
fn put_state(tx: &mut GroupTransaction<'_>, state: &State) -> Result<(), ProtocolError> {
    let bytes = tx.encode_value(state)?;
    tx.put(STATE, bytes.as_slice())?;
    direct::record_state_write(direct::StateMark {
        high_watermark: state.high_watermark,
        authority: state.authority,
        allocator_epoch: state.allocator_epoch,
    });
    Ok(())
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
