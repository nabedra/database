use super::*;
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson,
            store::ParticipantHandle,
        },
        pool::{ChargedBytes, ScratchLease, WorkerSharedResources},
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, ConsensusFuture},
            control::ScopedQueuePlacement,
            timestamp::{ScopedAdmissionClosure, VerifiedArchiveClosure, VerifiedPinRelease},
        },
        types::*,
    },
};
use std::sync::Arc;
use uuid::Uuid;

use super::identity::transaction::archive_images::MetadataArchivePage;

#[doc(hidden)]
pub use nabedra_oltp_types::authority::StatusTopology;
use nabedra_oltp_types::authority::invalid_status_route as invalid_route;

/// Request operations route directly to their home. Group operations with
/// observable applied positions are performed on individual services.
#[doc(hidden)]
pub struct PartitionedStatus {
    topology: StatusTopology,
    services: Vec<Arc<dyn StatusService>>,
}

impl PartitionedStatus {
    #[doc(hidden)]
    pub fn topology(&self) -> &StatusTopology {
        &self.topology
    }

    #[doc(hidden)]
    pub fn new(
        topology: StatusTopology,
        services: Vec<Arc<dyn StatusService>>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if services.len() != topology.groups().len()
            || services
                .iter()
                .zip(topology.groups())
                .any(|(service, group)| service.identity() != group)
        {
            return Err(invalid_route());
        }
        Ok(Arc::new(Self { topology, services }))
    }

    fn request(&self, request_id: Uuid) -> &dyn StatusService {
        self.services[self.topology.bucket(request_id)].as_ref()
    }

    fn manifest(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<&dyn StatusService, ProtocolError> {
        self.topology.manifest(txn, manifest)?;
        Ok(self.request(txn.request_id))
    }

    fn group(&self, identity: &GroupIdentity) -> Result<&dyn StatusService, ProtocolError> {
        if !self.topology.contains(identity) {
            return Err(invalid_route());
        }
        let GroupKind::Status { bucket } = identity.kind else {
            return Err(invalid_route());
        };
        Ok(self.services[bucket as usize].as_ref())
    }

    fn single(&self) -> Result<&dyn StatusService, ProtocolError> {
        match self.services.as_slice() {
            [only] => Ok(only.as_ref()),
            _ => Err(invalid_route()),
        }
    }
}

impl StatusService for PartitionedStatus {
    fn native_archive_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> ConsensusFuture<'a, MetadataArchivePage> {
        Box::pin(async move {
            self.group(&closure.statement().status)?
                .native_archive_page(closure, after)
                .await
        })
    }
    fn archive_cut<'a>(
        &'a self,
        authority: Authority,
    ) -> ConsensusFuture<'a, VerifiedStatusArchiveCut> {
        Box::pin(async move { self.single()?.archive_cut(authority).await })
    }
    fn committed_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> ConsensusFuture<'a, CommittedArchivePage> {
        Box::pin(async move {
            self.group(&closure.statement().status)?
                .committed_page(closure, base_exclusive, after)
                .await
        })
    }
    fn identity_history_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> ConsensusFuture<'a, IdentityArchivePage> {
        Box::pin(async move {
            self.group(&closure.statement().status)?
                .identity_history_page(closure, after)
                .await
        })
    }
    fn identity(&self) -> &GroupIdentity {
        &self.topology.groups()[0]
    }
    fn with_scoped_execution(
        &self,
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
    ) -> Result<Arc<dyn StatusService>, ProtocolError> {
        self.request(scope.request.request_id)
            .with_scoped_execution(scope)
    }
    fn fence_scoped_queue<'a>(
        &'a self,
        transfer: &'a crate::oltp_replication::authority::control::ScopedQueueTransition,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            for service in &self.services {
                service.fence_scoped_queue(transfer).await?;
            }
            Ok(())
        })
    }
    fn scoped_receipt<'a>(
        &'a self,
        placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        request: RequestBinding,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, (VerifiedStatusDecision, VerifiedSettlement)> {
        self.request(request.request_id)
            .scoped_receipt(placement, request, txn, manifest)
    }
    fn retired_scoped_receipt<'a>(
        &'a self,
        run: Uuid,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<ScopedTerminalReceipt>> {
        self.request(request_id).retired_scoped_receipt(run, request_id, digest)
    }
    fn prior_scoped_receipt<'a>(
        &'a self,
        run: Uuid,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        self.request(request_id).prior_scoped_receipt(run, request_id, digest)
    }
    fn scoped_terminal_receipt<'a>(
        &'a self,
        placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<ScopedTerminalReceipt>> {
        self.request(request_id).scoped_terminal_receipt(placement, request_id, digest)
    }
    fn lookup_identity_for_scoped<'a>(
        &'a self,
        placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        self.request(request_id)
            .lookup_identity_for_scoped(placement, request_id, digest)
    }
    fn resources(&self) -> &Arc<WorkerSharedResources> {
        self.services[0].resources()
    }
    fn reserve_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        self.request(request_id)
            .reserve_identity(authority, request_id, digest, kind)
    }
    fn reserve_scoped_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        scoped: ScopedAdmission,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        self.request(request_id)
            .reserve_scoped_identity(authority, request_id, digest, kind, scoped)
    }
    fn lookup<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, ChargedBytes> {
        self.request(request_id).lookup(request_id, digest)
    }
    fn lookup_identity<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        self.request(request_id).lookup_identity(request_id, digest)
    }
    fn lookup_scoped_reservation<'a>(
        &'a self,
        placement: &'a crate::oltp_replication::authority::control::ScopedQueuePlacement,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, IdentityReservation> {
        self.request(request_id)
            .lookup_scoped_reservation(placement, request_id, digest)
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>> {
        Box::pin(async move {
            let expected = self.services[0].current_authority().await?;
            for service in &self.services[1..] {
                if service.current_authority().await? != expected {
                    return Err(invalid_route());
                }
            }
            Ok(expected)
        })
    }
    fn fence<'a>(
        &'a self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move { self.single()?.fence(transition_id, expected, next).await })
    }
    fn reject_unaccepted<'a>(
        &'a self,
        cap: &'a dyn crate::oltp_replication::queue_access::NeverAcceptedAdmission,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        self.request(cap.reservation().request_id)
            .reject_unaccepted(cap)
    }
    fn reject_scoped_route_moved<'a>(
        &'a self,
        placement: &'a ScopedQueuePlacement,
        authority: Authority,
        reservation: IdentityReservation,
        closure: &'a ScopedAdmissionClosure,
    ) -> ConsensusFuture<'a, RejectedIdentity> {
        self.request(reservation.request_id)
            .reject_scoped_route_moved(placement, authority, reservation, closure)
    }
    fn rejected_identity<'a>(
        &'a self,
        expected: &'a RejectedIdentity,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        self.request(expected.reservation.request_id)
            .rejected_identity(expected)
    }
    fn retire_terminal<'a>(
        &'a self,
        release: VerifiedPinRelease,
    ) -> ConsensusFuture<'a, VerifiedTerminalRetirement> {
        self.request(release.statement().request.request_id)
            .retire_terminal(release)
    }
    fn retirement<'a>(
        &'a self,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, Option<VerifiedTerminalRetirement>> {
        self.request(request_id).retirement(request_id, digest)
    }
    fn install_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        point: TimestampPointManifest,
    ) -> ConsensusFuture<'a, ManifestRef> {
        self.request(point.binding.txn.request_id).install_timestamp_point(authority, point)
    }
    fn timestamp_point_home<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, TimestampPointHome> {
        self.request(request_id).timestamp_point_home(authority, request_id, digest)
    }
    fn timestamp_point_projection<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, Option<(TimestampPointProjection, Arc<ScratchLease>)>> {
        self.request(locator.binding.txn.request_id).timestamp_point_projection(authority, locator)
    }
    fn project_timestamp_point<'a>(
        &'a self,
        authority: Authority,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, (TimestampPointProjection, Arc<ScratchLease>)> {
        self.request(locator.binding.txn.request_id).project_timestamp_point(authority, locator)
    }
    fn try_install_point_manifest<'a>(
        &'a self,
        point: PointManifest,
    ) -> ConsensusFuture<'a, Option<(ManifestRef, Digest)>> {
        self.request(point.txn.request_id)
            .try_install_point_manifest(point)
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
        self.request(txn.request_id).begin_manifest(
            authority,
            txn,
            request,
            read_ts,
            participant_count,
            result,
        )
    }
    fn append_manifest_page<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> ConsensusFuture<'a, ManifestDraft> {
        self.request(txn.request_id).append_manifest_page(
            authority,
            txn,
            page_index,
            previous_root,
            participants,
        )
    }
    fn seal_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, ManifestRef> {
        self.request(txn.request_id).seal_manifest(authority, txn)
    }
    fn record_participant_prepared<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            self.manifest(txn, manifest)?
                .record_participant_prepared(authority, participant, txn, manifest)
                .await
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
            self.manifest(txn, manifest)?
                .record_participant_resolved(authority, participant, txn, manifest)
                .await
        })
    }
    fn prepare_point_permit<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Result<SealedCommitPermit, ProtocolError>> {
        Box::pin(async move {
            self.manifest(txn, manifest)?
                .prepare_point_permit(authority, participant, txn, manifest)
                .await
        })
    }
    fn commit_permit<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, SealedCommitPermit> {
        self.request(txn.request_id).commit_permit(authority, txn)
    }
    fn decide_commit<'a>(
        &'a self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move {
            self.manifest(permit.statement().txn, permit.statement().manifest)?
                .decide_commit(authority, permit, stamp)
                .await
        })
    }
    fn abort<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        self.request(txn.request_id)
            .abort(authority, txn, reason, error)
    }
    fn lookup_decision<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Option<VerifiedStatusDecision>> {
        Box::pin(async move {
            self.manifest(txn, manifest)?
                .lookup_decision(txn, manifest)
                .await
        })
    }
    fn settled_proof<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        Box::pin(async move {
            self.manifest(txn, manifest)?
                .settled_proof(txn, manifest)
                .await
        })
    }
    fn terminal_failure<'a>(
        &'a self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, TerminalIdentity> {
        self.request(request.request_id)
            .terminal_failure(authority, request, error)
    }
    fn manifest_page<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> ConsensusFuture<'a, ManifestInventoryPage> {
        Box::pin(async move {
            self.manifest(txn, manifest)?
                .manifest_page(txn, manifest, after)
                .await
        })
    }
    fn failure_settled_proof<'a>(
        &'a self,
        txn: TxnId,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        self.request(txn.request_id)
            .failure_settled_proof(txn, request)
    }
    fn inspect_transaction<'a>(
        &'a self,
        txn: TxnId,
    ) -> ConsensusFuture<'a, Option<TransactionInspection>> {
        self.request(txn.request_id).inspect_transaction(txn)
    }
    fn abandon_fenced<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        self.request(txn.request_id).abandon_fenced(authority, txn)
    }

    fn group_for_request(&self, request_id: Uuid) -> &GroupIdentity {
        self.topology.request(request_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(bucket: u32) -> GroupIdentity {
        GroupIdentity {
            cluster_id: Uuid::from_u128(42),
            database: Text::try_from("routing").unwrap(),
            group_id: Uuid::from_u128(u128::from(bucket) + 100),
            kind: GroupKind::Status { bucket },
            group_format_version: GROUP_FORMAT_VERSION,
        }
    }

    #[test]
    fn o10_q05_status_routing_pins_uuid_home_and_rejects_wrong_manifest() {
        let legacy = StatusTopology::new(vec![group(0)]).unwrap();
        let two = StatusTopology::new(vec![group(0), group(1)]).unwrap();
        let three = StatusTopology::new(vec![group(0), group(1), group(2)]).unwrap();
        for (id, bucket_two, bucket_three) in
            [(1, 1, 2), (2, 0, 2), (3, 1, 0), (4, 0, 2), (5, 1, 1)]
        {
            let request_id = Uuid::from_u128(id);
            assert_eq!(legacy.request(request_id), &group(0));
            assert_eq!(two.request(request_id), &group(bucket_two));
            assert_eq!(three.request(request_id), &group(bucket_three));
            let manifest = ManifestRef {
                status_group_id: group(bucket_two).group_id,
                digest: Digest::from_bytes([87; 32]),
                participant_count: 2,
            };
            for attempt in [1, 2, u64::MAX] {
                let txn = TxnId {
                    request_id,
                    attempt,
                };
                assert_eq!(two.manifest(txn, manifest).unwrap(), &group(bucket_two));
                let other = ManifestRef {
                    status_group_id: group(1 - bucket_two).group_id,
                    ..manifest
                };
                assert!(two.manifest(txn, other).is_err());
            }
        }
        assert!(StatusTopology::new(Vec::new()).is_err());
        assert!(StatusTopology::new(vec![group(0), group(2)]).is_err());
        assert!(StatusTopology::new(vec![group(1), group(0)]).is_err());
        let mut foreign = group(1);
        foreign.cluster_id = Uuid::from_u128(413);
        assert!(StatusTopology::new(vec![group(0), foreign]).is_err());
        let mut duplicate = group(1);
        duplicate.group_id = group(0).group_id;
        assert!(StatusTopology::new(vec![group(0), duplicate]).is_err());
    }
}
