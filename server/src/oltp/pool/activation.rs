//! Target placement evidence requires an actual stable quorum observation.
//! The returned native lease pins generation/membership through publication;
//! a parsed DTO or an arbitrary applied-index number cannot construct it.
use super::*;
use nabedra_oltp_types::{
    owned::serialized_digest,
    types::{Page, ReplicaMember, WorkerIdentity},
};
use tokio::sync::OwnedRwLockReadGuard;

/// A stable membership observed by replication, including the decoder for
/// the durable membership that will be checked under the generation guard.
pub(crate) trait ActivationMembershipProof<const EXACT: bool>: Sync {
    fn group(&self) -> &GroupIdentity;
    fn voters(&self) -> &[ReplicaMember];
    fn position(&self) -> AppliedPosition;

    fn matches_stored_membership(
        bytes: &[u8],
        voters: &[ReplicaMember],
        minimum: AppliedPosition,
    ) -> Result<bool, ProtocolError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaActivationStatement {
    pub group: GroupIdentity,
    pub pool: PoolIdentity,
    pub registered_owner: Option<(WorkerIdentity, u64)>,
    pub minimum: AppliedPosition,
    pub minimum_voters_root: Digest,
    pub active_generation: u64,
    pub applied: AppliedPosition,
    pub command_digest: Digest,
    pub membership_digest: Digest,
    pub lineage_digest: Digest,
}
pub(crate) struct VerifiedReplicaActivation {
    statement: ReplicaActivationStatement,
    _generation: OwnedRwLockReadGuard<()>,
    _store: GroupStore,
    _owner: Arc<ScratchLease>,
}
impl VerifiedReplicaActivation {
    pub(crate) fn statement(&self) -> &ReplicaActivationStatement {
        &self.statement
    }
}
impl GroupStore {
    pub(crate) async fn replica_activation<P: ActivationMembershipProof<false>>(
        &self,
        minimum: &P,
    ) -> Result<VerifiedReplicaActivation, ProtocolError> {
        self.replica_activation_at::<P, false>(minimum).await
    }

    pub(crate) async fn replica_activation_applied<P: ActivationMembershipProof<true>>(
        &self,
        minimum: &P,
    ) -> Result<VerifiedReplicaActivation, ProtocolError> {
        self.replica_activation_at::<P, true>(minimum).await
    }

    async fn replica_activation_at<P: ActivationMembershipProof<EXACT>, const EXACT: bool>(
        &self,
        proof: &P,
    ) -> Result<VerifiedReplicaActivation, ProtocolError> {
        let group = proof.group();
        let voters = proof.voters();
        let minimum = proof.position();
        if group != self.identity()
            || !voters.iter().any(|replica| {
                replica.pool_id == self.pool().identity().pool_id
                    && replica.worker_id == self.pool().identity().worker_id
            })
        {
            return Err(storage_error_message());
        }
        let owner = Arc::new(
            self.resources()
                .reserve_scratch_wait(ByteClass::Data, 128 << 10, 0)
                .await?,
        );
        let voters = Page::new(voters.to_vec()).map_err(|_| storage_error_message())?;
        let generation = self.inner.generation.clone().read_owned().await;
        let state_owner = self.inner.order.clone().lock_owned().await;
        let group = self.clone();
        let raw = self
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 128 << 10,
                    batch_bytes: 0,
                    output_bytes: 64 << 10,
                },
                move |view| {
                    group.pool().healthy()?;
                    let applied = group
                        .read_applied(view.snapshot, &mut view.scratch)?
                        .ok_or_else(recovery_unknown)?;
                    if applied.position.index < minimum.index
                        || applied.position.term < minimum.term
                        || (applied.position.index == minimum.index && applied.position != minimum)
                        || (applied.position.term == minimum.term
                            && applied.position.node_id != minimum.node_id)
                    {
                        return Err(recovery_unknown());
                    }
                    let membership = applied.membership.as_ref().ok_or_else(recovery_unknown)?;
                    if !P::matches_stored_membership(membership.as_slice(), &voters, minimum)? {
                        return Err(recovery_unknown());
                    }
                    let native_generation = view
                        .snapshot
                        .get(&group_key(group.identity().group_id, b'a', &[]))
                        .map_err(storage_error)?
                        .ok_or_else(recovery_unknown)?;
                    let active_generation = u64::from_be_bytes(
                        native_generation
                            .as_ref()
                            .try_into()
                            .map_err(|_| recovery_unknown())?,
                    );
                    let minimum_voters_root = serialized_digest(&(
                        b"nabedra/activation-voters/v1",
                        group.identity(),
                        minimum,
                        &voters,
                    ))?;
                    let membership_digest = {
                        use sha2::{Digest as _, Sha256};
                        Digest::from_bytes(Sha256::digest(membership.as_slice()).into())
                    };
                    let registered_owner = match &group.pool().inner._mount {
                        Some(mount) => {
                            let observed = mount.reobserve()?;
                            Some((observed.expected.worker, observed.expected.generation))
                        }
                        None => None,
                    };
                    let lineage_digest = serialized_digest(&(
                        b"nabedra/activation-lineage/v1",
                        group.identity(),
                        group.pool().identity(),
                        registered_owner,
                        minimum,
                        minimum_voters_root,
                        active_generation,
                        applied.position,
                        applied.command_digest,
                        membership_digest,
                    ))?;
                    view.encode(&ReplicaActivationStatement {
                        group: group.identity().clone(),
                        pool: group.pool().identity().clone(),
                        registered_owner,
                        minimum,
                        minimum_voters_root,
                        active_generation,
                        applied: applied.position,
                        command_digest: applied.command_digest,
                        membership_digest,
                        lineage_digest,
                    })
                },
            )
            .await?;
        drop(state_owner);
        // Fixed bounded metadata remains owned after the temporary native read.
        #[derive(Deserialize)]
        struct Decoded {
            group: GroupIdentity,
            pool: PoolIdentity,
            registered_owner: Option<(WorkerIdentity, u64)>,
            minimum: AppliedPosition,
            minimum_voters_root: Digest,
            active_generation: u64,
            applied: AppliedPosition,
            command_digest: Digest,
            membership_digest: Digest,
            lineage_digest: Digest,
        }
        let decoded: Decoded =
            serde_json::from_slice(raw.as_slice()).map_err(|_| storage_error_message())?;
        let statement = ReplicaActivationStatement {
            group: decoded.group,
            pool: decoded.pool,
            registered_owner: decoded.registered_owner,
            minimum: decoded.minimum,
            minimum_voters_root: decoded.minimum_voters_root,
            active_generation: decoded.active_generation,
            applied: decoded.applied,
            command_digest: decoded.command_digest,
            membership_digest: decoded.membership_digest,
            lineage_digest: decoded.lineage_digest,
        };
        Ok(VerifiedReplicaActivation {
            statement,
            _generation: generation,
            _store: self.clone(),
            _owner: owner,
        })
    }
}
