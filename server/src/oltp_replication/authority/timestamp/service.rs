//! Named metadata operations shared by hosted and remote services.
use super::*;
use crate::oltp::{
    mvcc::{
        CanonicalJson,
        store::{VerifiedParticipantResolution, VerifiedParticipantSeal},
    },
    pool::{ChargedBytes, WorkerSharedResources},
};
use crate::oltp_replication::authority::status::{
    CommitStamp, IdentityReservation, SealedCommitPermit, VerifiedAdmissionRejection,
};
use crate::oltp_replication::{
    authority::consensus::{Applied, ConsensusFuture},
    types::*,
};
use std::sync::Arc;
use uuid::Uuid;
pub(crate) trait TimestampService: Send + Sync {
    fn advance_floor(
        &self,
        authority: Authority,
        retired_at: Option<CommitTs>,
    ) -> ConsensusFuture<'_, u64>;
    fn native_archive_page<'a>(&'a self, closure: &'a VerifiedArchiveClosure, after: Option<Vec<u8>>) -> ConsensusFuture<'a, crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage>;
    fn archive_closure<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
        pin: StoragePin,
        archive_id: Uuid,
        base_exclusive: Option<CommitTs>,
        status: Arc<dyn crate::oltp_replication::authority::status::StatusService>,
    ) -> ConsensusFuture<'a, VerifiedArchiveClosure>;
    fn with_scoped_execution(
        &self,
        _scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
    ) -> Result<Arc<dyn TimestampService>, ProtocolError> {
        Err(super::invalid())
    }
    fn commit_timestamp_point<'a>(
        &'a self,
        _locator: &'a TimestampDecisionLocator,
        _reconcile: bool,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(async { Err(unknown()) })
    }
    fn abort_timestamp_point<'a>(
        &'a self,
        _locator: &'a TimestampDecisionLocator,
        _reconcile: bool,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(async { Err(unknown()) })
    }
    fn observe_timestamp_point<'a>(
        &'a self,
        _locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, Option<(PointDecision, Arc<ScratchLease>)>> {
        Box::pin(async { Err(unknown()) })
    }
    fn abandon_timestamp_point<'a>(
        &'a self,
        _locator: &'a TimestampDecisionLocator,
        _transition_id: Uuid,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(async { Err(unknown()) })
    }
    fn identity(&self) -> &GroupIdentity;
    fn resources(&self) -> &Arc<WorkerSharedResources>;
    fn begin_import_metadata<'a>(
        &'a self,
        authority: Authority,
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn release_import_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()>;
    fn retain_archive<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
        previous_root: Option<Digest>,
    ) -> ConsensusFuture<'a, ()>;
    fn cancel_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::lifecycle::VerifiedBackupCancelled,
    ) -> ConsensusFuture<'a, ()>;
    fn begin_backup<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn release_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
    ) -> ConsensusFuture<'a, ()>;
    fn begin_index_metadata<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn index_metadata_pin<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, IndexMetadataPin>;
    fn release_index_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()>;
    fn begin_maintenance<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn release_maintenance<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> ConsensusFuture<'a, ()>;
    fn cancel_unaccepted<'a>(
        &'a self,
        rejection: crate::oltp_replication::authority::status::VerifiedAdmissionRejection,
    ) -> ConsensusFuture<'a, ()>;
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>>;
    fn release<'a>(
        &'a self,
        statement: TerminalCopyStatement,
    ) -> ConsensusFuture<'a, VerifiedPinRelease>;
    fn begin_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn retry_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        previous: super::super::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, StoragePin>;
    fn bind_snapshot_context<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, ()>;
    fn allocate_commit<'a>(
        &'a self,
        permit: SealedCommitPermit,
    ) -> ConsensusFuture<'a, CommitStamp>;
    fn fence_ready<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> ConsensusFuture<'a, ReadyFenceReceipt>;
    fn fence<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> ConsensusFuture<'a, Applied<TimestampReply>>;
}
impl TimestampService for TimestampHandle {
    fn advance_floor(
        &self,
        authority: Authority,
        retired_at: Option<CommitTs>,
    ) -> ConsensusFuture<'_, u64> {
        Box::pin(TimestampHandle::advance_floor(self, authority, retired_at))
    }
    fn native_archive_page<'a>(&'a self, closure: &'a VerifiedArchiveClosure, after: Option<Vec<u8>>) -> ConsensusFuture<'a, crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage>{
        Box::pin(TimestampHandle::native_archive_page(self, closure, after))
    }

    fn retain_archive<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
        previous_root: Option<Digest>,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(TimestampHandle::retain_archive(
            self,
            authority,
            proof,
            previous_root,
        ))
    }
    fn cancel_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::lifecycle::VerifiedBackupCancelled,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(TimestampHandle::cancel_backup(self, authority, proof))
    }
    fn archive_closure<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
        pin: StoragePin,
        archive_id: Uuid,
        base_exclusive: Option<CommitTs>,
        status: Arc<dyn crate::oltp_replication::authority::status::StatusService>,
    ) -> ConsensusFuture<'a, VerifiedArchiveClosure> {
        Box::pin(TimestampHandle::archive_closure(
            self,
            authority,
            owner,
            pin,
            archive_id,
            base_exclusive,
            status,
        ))
    }
    fn identity(&self) -> &GroupIdentity {
        TimestampHandle::identity(self)
    }
    fn resources(&self) -> &Arc<WorkerSharedResources> {
        TimestampHandle::resources(self)
    }
    fn begin_import_metadata<'a>(
        &'a self,
        authority: Authority,
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move {
            TimestampHandle::begin_import_metadata(
                self,
                authority,
                archive_id,
                target_id,
                txn,
                reservation,
            )
            .await
        })
    }
    fn release_import_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampHandle::release_import_metadata(self, authority, identity, pin, settled).await
        })
    }
    fn begin_backup<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move { TimestampHandle::begin_backup(self, authority, owner).await })
    }
    fn release_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampHandle::release_backup(self, authority, proof).await })
    }
    fn begin_index_metadata<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move {
            TimestampHandle::begin_index_metadata(self, authority, owner, txn, reservation).await
        })
    }
    fn index_metadata_pin<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, IndexMetadataPin> {
        Box::pin(TimestampHandle::index_metadata_pin(self, authority, owner, txn, reservation, request))
    }
    fn release_index_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampHandle::release_index_metadata(self, authority, identity, pin, settled).await
        })
    }
    fn begin_maintenance<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move { TimestampHandle::begin_maintenance(self, authority, owner).await })
    }
    fn release_maintenance<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampHandle::release_maintenance(self, authority, proof).await })
    }
    fn cancel_unaccepted<'a>(
        &'a self,
        rejection: crate::oltp_replication::authority::status::VerifiedAdmissionRejection,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampHandle::cancel_unaccepted(self, rejection).await })
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>> {
        Box::pin(async move { TimestampHandle::current_authority(self).await })
    }
    fn release<'a>(
        &'a self,
        statement: TerminalCopyStatement,
    ) -> ConsensusFuture<'a, VerifiedPinRelease> {
        Box::pin(async move { TimestampHandle::release(self, statement).await })
    }
    fn begin_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(
            async move { TimestampHandle::begin_snapshot(self, view, identity, reservation).await },
        )
    }
    fn retry_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        previous: super::super::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(TimestampHandle::retry_snapshot(
            self, view, identity, previous,
        ))
    }
    fn bind_snapshot_context<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampHandle::bind_snapshot_context(self, view, identity, pin, request).await
        })
    }
    fn allocate_commit<'a>(
        &'a self,
        permit: SealedCommitPermit,
    ) -> ConsensusFuture<'a, CommitStamp> {
        Box::pin(async move { TimestampHandle::allocate_commit(self, permit).await })
    }
    fn fence_ready<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> ConsensusFuture<'a, ReadyFenceReceipt> {
        Box::pin(
            async move { TimestampHandle::fence_ready(self, transition_id, expected, next).await },
        )
    }
    fn fence<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> ConsensusFuture<'a, Applied<TimestampReply>> {
        Box::pin(
            async move { TimestampHandle::fence(self, transition_id, expected, next, ready).await },
        )
    }
}
