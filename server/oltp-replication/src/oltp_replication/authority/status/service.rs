//! Named metadata operations shared by hosted and remote services.
use super::*;
use crate::oltp::{
    mvcc::{
        CanonicalJson,
        store::ParticipantHandle,
    },
    pool::{ChargedBytes, ScratchLease, WorkerSharedResources},
};
use crate::oltp_replication::authority::timestamp::VerifiedPinRelease;
use crate::oltp_replication::authority::{
    control::ScopedQueuePlacement, timestamp::ScopedAdmissionClosure,
};
use crate::oltp_replication::{
    authority::consensus::{Applied, ConsensusFuture},
    types::*,
};
use std::sync::Arc;
use uuid::Uuid;
/// A bounded service request. Its three durable commands retain their existing
/// wire formats, fencing checks, page receipts and applied-position semantics.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct PointManifest {
    pub authority: Authority,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub read_ts: ReadTs,
    pub plan: ParticipantPlan,
    pub result: CanonicalJson,
}

impl PointManifest {
    #[doc(hidden)]
    pub fn qualifies(plan: &ParticipantPlan, result: &CanonicalJson) -> bool {
        matches!(plan.group.kind, GroupKind::Data { .. })
            && plan.write_count == 1
            && plan.page_count == 1
            && result.as_str().len() <= 256
    }
}

#[doc(hidden)]
pub trait StatusService: Send + Sync {
    fn native_archive_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> ConsensusFuture<'a, super::identity::transaction::archive_images::MetadataArchivePage>;
    fn archive_cut<'a>(
        &'a self,
        authority: Authority,
    ) -> ConsensusFuture<'a, VerifiedStatusArchiveCut>;
    fn committed_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> ConsensusFuture<'a, CommittedArchivePage>;
    fn identity_history_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> ConsensusFuture<'a, IdentityArchivePage>;
    fn identity(&self) -> &GroupIdentity;
    fn group_for_request(&self, _request_id: Uuid) -> &GroupIdentity {
        self.identity()
    }
    fn with_scoped_execution(
        &self,
        _scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
    ) -> Result<Arc<dyn StatusService>, ProtocolError> {
        Err(super::invalid())
    }
    fn fence_scoped_queue<'a>(
        &'a self,
        _transfer: &'a crate::oltp_replication::authority::control::ScopedQueueTransition,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn scoped_receipt<'a>(
        &'a self,
        _placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        _request: RequestBinding,
        _txn: TxnId,
        _manifest: ManifestRef,
    ) -> ConsensusFuture<'a, (VerifiedStatusDecision, VerifiedSettlement)> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn retired_scoped_receipt<'a>(
        &'a self,
        _run: Uuid,
        _request_id: Uuid,
        _digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<ScopedTerminalReceipt>> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn prior_scoped_receipt<'a>(
        &'a self,
        _run: Uuid,
        _request_id: Uuid,
        _digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn scoped_terminal_receipt<'a>(
        &'a self,
        _placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        _request_id: Uuid,
        _digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<ScopedTerminalReceipt>> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn lookup_identity_for_scoped<'a>(
        &'a self,
        _placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        _request_id: Uuid,
        _digest: Digest,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn resources(&self) -> &Arc<WorkerSharedResources>;
    fn reserve_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> ConsensusFuture<'a, Applied<StatusReply>>;
    fn reserve_scoped_identity<'a>(
        &'a self,
        _authority: Authority,
        _request_id: Uuid,
        _digest: Digest,
        _kind: RequestKind,
        _scoped: ScopedAdmission,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async {
            Err(crate::oltp::failure(
                crate::protocol::ErrorCode::UnsupportedOperation,
                "scoped reservation requires authenticated owner transport",
            ))
        })
    }
    fn lookup<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, ChargedBytes>;
    fn lookup_identity<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>>;
    fn lookup_scoped_reservation<'a>(
        &'a self,
        _placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        _request_id: Uuid,
        _digest: Digest,
    ) -> ConsensusFuture<'a, IdentityReservation> {
        Box::pin(async {
            Err(crate::oltp::failure(
                crate::protocol::ErrorCode::UnsupportedOperation,
                "scoped reservation lookup requires authenticated holder transport",
            ))
        })
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>>;
    fn fence<'a>(
        &'a self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> ConsensusFuture<'a, Applied<StatusReply>>;
    fn reject_unaccepted<'a>(
        &'a self,
        cap: &'a dyn crate::oltp_replication::queue_access::NeverAcceptedAdmission,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection>;
    fn reject_scoped_route_moved<'a>(
        &'a self,
        _placement: &'a ScopedQueuePlacement,
        _authority: Authority,
        _reservation: IdentityReservation,
        _closure: &'a ScopedAdmissionClosure,
    ) -> ConsensusFuture<'a, RejectedIdentity> {
        Box::pin(async { Err(super::invalid()) })
    }
    fn rejected_identity<'a>(
        &'a self,
        expected: &'a RejectedIdentity,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection>;
    fn retire_terminal<'a>(
        &'a self,
        release: VerifiedPinRelease,
    ) -> ConsensusFuture<'a, VerifiedTerminalRetirement>;
    fn retirement<'a>(
        &'a self,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, Option<VerifiedTerminalRetirement>>;
    fn install_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        point: TimestampPointManifest,
    ) -> ConsensusFuture<'a, ManifestRef>;
    fn timestamp_point_home<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, TimestampPointHome>;
    fn timestamp_point_projection<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, Option<(TimestampPointProjection, Arc<ScratchLease>)>>;
    fn project_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, (TimestampPointProjection, Arc<ScratchLease>)>;
    fn try_install_point_manifest<'a>(
        &'a self,
        _point: PointManifest,
    ) -> ConsensusFuture<'a, Option<(ManifestRef, Digest)>> {
        Box::pin(async { Ok(None) })
    }
    fn begin_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        result: CanonicalJson,
    ) -> ConsensusFuture<'a, ManifestDraft>;
    fn append_manifest_page<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> ConsensusFuture<'a, ManifestDraft>;
    fn seal_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, ManifestRef>;
    fn record_participant_prepared<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()>;
    fn record_participant_resolved<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()>;
    fn prepare_point_permit<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Result<SealedCommitPermit, ProtocolError>> {
        Box::pin(async move {
            self.record_participant_prepared(authority, participant, txn, manifest)
                .await?;
            // Permit failures occur after the prepare was recorded. Keep
            // them outside the coordinator's prepare-failure abort path.
            Ok(self.commit_permit(authority, txn).await)
        })
    }
    fn commit_permit<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, SealedCommitPermit>;
    fn decide_commit<'a>(
        &'a self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision>;
    fn abort<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision>;
    fn lookup_decision<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Option<VerifiedStatusDecision>>;
    fn settled_proof<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, VerifiedSettlement>;
    fn terminal_failure<'a>(
        &'a self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, TerminalIdentity>;
    fn manifest_page<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> ConsensusFuture<'a, ManifestInventoryPage>;
    fn failure_settled_proof<'a>(
        &'a self,
        txn: TxnId,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, VerifiedSettlement>;
    fn inspect_transaction<'a>(
        &'a self,
        txn: TxnId,
    ) -> ConsensusFuture<'a, Option<TransactionInspection>>;
    fn abandon_fenced<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision>;
}
impl StatusService for StatusHandle {
    fn native_archive_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> ConsensusFuture<'a, super::identity::transaction::archive_images::MetadataArchivePage>
    {
        Box::pin(StatusHandle::native_archive_page(self, closure, after))
    }
    fn archive_cut<'a>(
        &'a self,
        authority: Authority,
    ) -> ConsensusFuture<'a, VerifiedStatusArchiveCut> {
        Box::pin(async move { StatusHandle::archive_cut(self, authority).await })
    }
    fn committed_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> ConsensusFuture<'a, CommittedArchivePage> {
        Box::pin(
            async move { StatusHandle::committed_page(self, closure, base_exclusive, after).await },
        )
    }
    fn identity_history_page<'a>(
        &'a self,
        closure: &'a crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> ConsensusFuture<'a, IdentityArchivePage> {
        Box::pin(async move { StatusHandle::identity_history_page(self, closure, after).await })
    }
    fn identity(&self) -> &GroupIdentity {
        StatusHandle::identity(self)
    }
    fn resources(&self) -> &Arc<WorkerSharedResources> {
        StatusHandle::resources(self)
    }
    fn reserve_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move {
            StatusHandle::reserve_identity(self, authority, request_id, digest, kind).await
        })
    }
    fn lookup<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, ChargedBytes> {
        Box::pin(async move { StatusHandle::lookup(self, request_id, digest).await })
    }
    fn lookup_identity<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        Box::pin(async move { StatusHandle::lookup_identity(self, request_id, digest).await })
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>> {
        Box::pin(async move { StatusHandle::current_authority(self).await })
    }
    fn fence<'a>(
        &'a self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move { StatusHandle::fence(self, transition_id, expected, next).await })
    }
    fn reject_unaccepted<'a>(
        &'a self,
        cap: &'a dyn crate::oltp_replication::queue_access::NeverAcceptedAdmission,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        Box::pin(async move { StatusHandle::reject_unaccepted(self, cap).await })
    }
    fn rejected_identity<'a>(
        &'a self,
        expected: &'a RejectedIdentity,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        Box::pin(StatusHandle::rejected_identity(self, expected))
    }
    fn retire_terminal<'a>(
        &'a self,
        release: VerifiedPinRelease,
    ) -> ConsensusFuture<'a, VerifiedTerminalRetirement> {
        Box::pin(StatusHandle::retire_terminal(self, release))
    }
    fn retirement<'a>(
        &'a self,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, Option<VerifiedTerminalRetirement>> {
        Box::pin(StatusHandle::retirement(self, request_id, digest))
    }
    fn install_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        point: TimestampPointManifest,
    ) -> ConsensusFuture<'a, ManifestRef> {
        Box::pin(StatusHandle::install_timestamp_point(self, authority, point))
    }
    fn timestamp_point_home<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, TimestampPointHome> {
        Box::pin(StatusHandle::timestamp_point_home(self, authority, request_id, digest))
    }
    fn timestamp_point_projection<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, Option<(TimestampPointProjection, Arc<ScratchLease>)>> {
        Box::pin(self.read_timestamp_point_projection(authority, locator.clone()))
    }
    fn project_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, (TimestampPointProjection, Arc<ScratchLease>)> {
        Box::pin(async move {
            self.read_timestamp_point_projection(authority, locator.clone())
                .await?
                .ok_or_else(super::identity::unknown)
        })
    }
    fn begin_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        result: CanonicalJson,
    ) -> ConsensusFuture<'a, ManifestDraft> {
        Box::pin(async move {
            StatusHandle::begin_manifest(
                self,
                authority,
                txn,
                request,
                read_ts,
                participant_count,
                result,
            )
            .await
        })
    }
    fn append_manifest_page<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> ConsensusFuture<'a, ManifestDraft> {
        Box::pin(async move {
            StatusHandle::append_manifest_page(
                self,
                authority,
                txn,
                page_index,
                previous_root,
                participants,
            )
            .await
        })
    }
    fn seal_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, ManifestRef> {
        Box::pin(async move { StatusHandle::seal_manifest(self, authority, txn).await })
    }
    fn record_participant_prepared<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            let proof = participant.strong_seal(txn, manifest).await?;
            self.observe_prepared(authority, proof).await
        })
    }
    fn record_participant_resolved<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            let proof = participant.strong_resolution(txn, manifest).await?;
            self.observe_resolved(authority, proof).await
        })
    }
    fn commit_permit<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, SealedCommitPermit> {
        Box::pin(async move { StatusHandle::commit_permit(self, authority, txn).await })
    }
    fn decide_commit<'a>(
        &'a self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusHandle::decide_commit(self, authority, permit, stamp).await })
    }
    fn abort<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusHandle::abort(self, authority, txn, reason, error).await })
    }
    fn lookup_decision<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Option<VerifiedStatusDecision>> {
        Box::pin(async move { StatusHandle::lookup_decision(self, txn, manifest).await })
    }
    fn settled_proof<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        Box::pin(async move { StatusHandle::settled_proof(self, txn, manifest).await })
    }
    fn terminal_failure<'a>(
        &'a self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, TerminalIdentity> {
        Box::pin(
            async move { StatusHandle::terminal_failure(self, authority, request, error).await },
        )
    }
    fn manifest_page<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> ConsensusFuture<'a, ManifestInventoryPage> {
        Box::pin(async move { StatusHandle::manifest_page(self, txn, manifest, after).await })
    }
    fn failure_settled_proof<'a>(
        &'a self,
        txn: TxnId,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        Box::pin(async move { StatusHandle::failure_settled_proof(self, txn, request).await })
    }
    fn inspect_transaction<'a>(
        &'a self,
        txn: TxnId,
    ) -> ConsensusFuture<'a, Option<TransactionInspection>> {
        Box::pin(async move { StatusHandle::inspect_transaction(self, txn).await })
    }
    fn abandon_fenced<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusHandle::abandon_fenced(self, authority, txn).await })
    }
}
