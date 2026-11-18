//! Named metadata operations shared by hosted and remote services.
use super::*;
use crate::oltp::{
    mvcc::{
        CanonicalJson,
        store::{VerifiedParticipantResolution, VerifiedParticipantSeal},
    },
    pool::{ChargedBytes, WorkerSharedResources},
};
use crate::oltp_replication::authority::timestamp::VerifiedPinRelease;
use crate::oltp_replication::{
    authority::consensus::{Applied, ConsensusFuture},
    types::*,
};
use std::sync::Arc;
use uuid::Uuid;
pub(crate) trait StatusService: Send + Sync {
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
    fn resources(&self) -> &Arc<WorkerSharedResources>;
    fn reserve_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> ConsensusFuture<'a, Applied<StatusReply>>;
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
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>>;
    fn fence<'a>(
        &'a self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> ConsensusFuture<'a, Applied<StatusReply>>;
    fn reject_unaccepted<'a>(
        &'a self,
        cap: &'a crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection>;
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
    fn observe_prepared<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantSeal,
    ) -> ConsensusFuture<'a, ()>;
    fn observe_resolved<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantResolution,
    ) -> ConsensusFuture<'a, ()>;
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
        cap: &'a crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted,
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
    fn observe_prepared<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantSeal,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { StatusHandle::observe_prepared(self, authority, proof).await })
    }
    fn observe_resolved<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantResolution,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { StatusHandle::observe_resolved(self, authority, proof).await })
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
