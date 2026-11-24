use super::*;
use crate::{
    oltp::pool::mount::{MountedPoolObservation, PoolMountExpectation},
    oltp_deployment::{
        controller::{GrowthPhase, GrowthRecord, MountedGrowthStamp},
        planner::Action,
    },
};

/// The signer is pinned by the retired worker's native mount enrollment.
/// Observations are claims by that dedicated controller, not signatures from AWS or Kubernetes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksExclusion {
    pub(crate) drain_id: Uuid,
    pub(crate) launch_digest: Digest,
    pub(crate) attachment_revision: String,
    pub(crate) describe_volumes_request_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksAddedPool {
    pub(crate) mount: PoolMountExpectation,
    pub(crate) pvc_uid: Uuid,
    pub(crate) pv_uid: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksCandidate {
    pub(crate) drain_id: Uuid,
    pub(crate) exclusion_operation: Uuid,
    pub(crate) launch_digest: Digest,
    pub(crate) next: super::super::DataWorkerIntent,
    pub(crate) added: EksAddedPool,
    pub(crate) pod_revision: String,
    pub(crate) projected_token_sha256: Digest,
    pub(crate) projected_token_expires_at: u64,
    pub(crate) service_account_uid: Uuid,
    pub(crate) node_uid: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    content = "evidence",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum EksReplacementAction {
    Exclude(EksExclusion),
    Authorize(Box<EksCandidate>),
}

impl EksReplacementAction {
    pub(crate) fn drain_id(&self) -> Uuid {
        match self {
            Self::Exclude(value) => value.drain_id,
            Self::Authorize(value) => value.drain_id,
        }
    }

    pub(super) fn validate(&self) -> bool {
        let revision = |value: &str| {
            !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_graphic())
        };
        !self.drain_id().is_nil()
            && match self {
                Self::Exclude(value) => {
                    revision(&value.attachment_revision)
                        && !value.describe_volumes_request_id.is_nil()
                        && value.launch_digest.as_bytes() != &[0; 32]
                }
                Self::Authorize(value) => {
                    revision(&value.pod_revision)
                        && value.projected_token_sha256.as_bytes() != &[0; 32]
                        && value.projected_token_expires_at != 0
                        && !value.exclusion_operation.is_nil()
                        && !value.service_account_uid.is_nil()
                        && !value.node_uid.is_nil()
                }
            }
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Exclude(value) => value.attachment_revision.capacity(),
            Self::Authorize(value) => {
                std::mem::size_of::<EksCandidate>()
                    + value.next.heap_bytes()
                    + mount_heap(&value.added.mount)
                    + value.pod_revision.capacity()
            }
        }
    }
}

fn mount_heap(mount: &PoolMountExpectation) -> usize {
    mount.mount_path.capacity()
        + mount.provider_volume_id.capacity()
        + mount.filesystem_uuid.capacity()
        + mount.identity.engine.capacity()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedEksReplacement {
    pub(crate) action: EksReplacementAction,
    pub(crate) signature: RestartMountProof,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksReplacementRecord {
    pub(crate) drain_id: Uuid,
    pub(crate) add_pool_claim: Uuid,
    pub(crate) exclusion_operation: Uuid,
    pub(crate) exclusion: EksExclusion,
    pub(crate) exclusion_signature: RestartMountProof,
    pub(crate) authorization: Option<EksAuthorization>,
    pub(crate) published: bool,
    pub(crate) zone: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksAuthorization {
    pub(crate) operation: Uuid,
    pub(crate) candidate: Box<EksCandidate>,
    pub(crate) signature: RestartMountProof,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksReplacementPermit {
    pub(crate) drain_id: Uuid,
    pub(crate) add_pool_claim: Uuid,
    pub(crate) previous: PoolMountExpectation,
    pub(crate) previous_root_inode: u64,
    pub(crate) next: super::super::DataWorkerIntent,
    pub(crate) added: EksAddedPool,
}

impl crate::oltp::pool::mount::PoolReopenProof for EksReplacementPermit {
    type Key = RestartMountKey;

    fn previous(&self) -> &PoolMountExpectation {
        &self.previous
    }

    fn previous_root_inode(&self) -> u64 {
        self.previous_root_inode
    }

    fn next(&self) -> &PoolMountExpectation {
        &self.next.mount
    }

    fn next_mount_public_key(&self) -> Digest {
        self.next.mount_public_key
    }

    fn drain_id(&self) -> Uuid {
        self.drain_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EksMountedReplacement {
    pub(crate) drain_id: Uuid,
    pub(crate) primary: MountedGrowthStamp,
    pub(crate) added: MountedGrowthStamp,
    pub(crate) signature: RestartMountProof,
}

impl EksMountedReplacement {
    fn message(
        &self,
        group: &GroupIdentity,
        permit: &EksReplacementPermit,
    ) -> Result<Digest, ProtocolError> {
        serialized_digest(&(
            b"nabedra/eks-replacement-mount/v1".as_slice(),
            group,
            permit,
            &self.primary,
            &self.added,
        ))
    }

    pub(crate) fn from_native(
        group: &GroupIdentity,
        permit: &EksReplacementPermit,
        primary: &MountedPoolObservation,
        added: &MountedPoolObservation,
        key: &RestartMountKey,
    ) -> Result<Self, ProtocolError> {
        if primary.expected != permit.next.mount
            || added.expected != permit.added.mount
            || key.public_key() != permit.next.mount_public_key
            || (primary.device_major, primary.device_minor)
                == (added.device_major, added.device_minor)
        {
            return Err(refused());
        }
        let stamp = |item: &MountedPoolObservation| MountedGrowthStamp {
            worker: item.expected.worker,
            pool_id: item.expected.identity.pool_id,
            pool_generation: item.expected.generation,
            provider_volume_id: item.expected.provider_volume_id.clone(),
            total_bytes: item.total_bytes,
        };
        let primary = stamp(primary);
        let added = stamp(added);
        let signature = key.sign_message(serialized_digest(&(
            b"nabedra/eks-replacement-mount/v1".as_slice(),
            group,
            permit,
            &primary,
            &added,
        ))?);
        Ok(Self {
            drain_id: permit.drain_id,
            primary,
            added,
            signature,
        })
    }

    pub(crate) fn verify(
        &self,
        group: &GroupIdentity,
        permit: &EksReplacementPermit,
        original: &GrowthRecord,
        growth: &GrowthRecord,
    ) -> Result<(), ProtocolError> {
        let matches =
            |stamp: &MountedGrowthStamp, mount: &PoolMountExpectation, claim: &GrowthRecord| {
                stamp.worker == mount.worker
                    && stamp.pool_id == mount.identity.pool_id
                    && stamp.pool_generation == mount.generation
                    && stamp.provider_volume_id == mount.provider_volume_id
                    && stamp.total_bytes >= claim.intent.requested_bytes()
            };
        if self.drain_id != permit.drain_id
            || !matches(&self.primary, &permit.next.mount, original)
            || !matches(&self.added, &permit.added.mount, growth)
        {
            return Err(refused());
        }
        self.signature
            .verify_message(self.message(group, permit)?, permit.next.mount_public_key)
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.primary.provider_volume_id.capacity() + self.added.provider_volume_id.capacity()
    }
}

impl EksReplacementRecord {
    pub(crate) fn permit(
        &self,
        drain: &WorkerDrainRecord,
    ) -> Result<EksReplacementPermit, ProtocolError> {
        let WorkerDrainPhase::Closed { mount, .. } = &drain.phase else {
            return Err(refused());
        };
        let candidate = &self.authorization.as_ref().ok_or_else(refused)?.candidate;
        if self.drain_id != drain.id || candidate.drain_id != drain.id || drain.retained_eks_pool.is_some() {
            return Err(refused());
        }
        Ok(EksReplacementPermit {
            drain_id: drain.id,
            add_pool_claim: self.add_pool_claim,
            previous: mount.expected.clone(),
            previous_root_inode: mount.root_inode,
            next: candidate.next.clone(),
            added: candidate.added.clone(),
        })
    }
}

pub(crate) fn eligible_add_pool(
    drain: &WorkerDrainIntent,
    original: &GrowthRecord,
    growth: &GrowthRecord,
) -> bool {
    let Action::AddWorker { zone, .. } = &original.intent.action else {
        return false;
    };
    drain.retained_replacement_drain.is_none()
        && matches!(&growth.intent.action, Action::AddPool { worker, zone: growth_zone, .. }
        if *worker == drain.worker.worker_id && growth_zone == zone)
        && drain.replacement_growth_claim == Some(growth.intent.claim_id)
        && growth.phase == GrowthPhase::AwaitingMount
        && growth.intent.worker == drain.worker
        && growth.intent.pool_id != original.intent.pool_id
        && growth.provider_resource.as_ref().is_some_and(|resource| {
            !resource.is_empty() && Some(resource) != original.provider_resource.as_ref()
        })
}

pub(crate) fn eks_statement(
    group: &GroupIdentity,
    operation: Uuid,
    drain: &WorkerDrainRecord,
    enrollment: &super::super::DataWorkerIntent,
    original: &GrowthRecord,
    growth: &GrowthRecord,
    action: &EksReplacementAction,
    exclusion: Option<&EksReplacementRecord>,
) -> Result<Digest, ProtocolError> {
    let Some(key) = enrollment.eks_replacement_key else {
        return Err(refused());
    };
    let Action::AddWorker { zone, .. } = &original.intent.action else {
        return Err(refused());
    };
    if operation.is_nil()
        || !action.validate()
        || action.drain_id() != drain.id
        || !eligible_add_pool(&drain.intent, original, growth)
    {
        return Err(refused());
    }
    let WorkerDrainPhase::Closed { mount, .. } = &drain.phase else {
        return Err(refused());
    };
    if enrollment.mount != mount.expected || !drain.intent.matches_registration(enrollment) {
        return Err(refused());
    }
    let prior_operation = match (action, exclusion) {
        (EksReplacementAction::Exclude(_), None) => None,
        (EksReplacementAction::Authorize(candidate), Some(old))
            if old.drain_id == drain.id
                && !old.published
                && old.authorization.is_none()
                && old.add_pool_claim == growth.intent.claim_id
                && old.exclusion_operation == candidate.exclusion_operation
                && old.exclusion.launch_digest == candidate.launch_digest
                && operation != old.exclusion_operation =>
        {
            let permit = WorkerStopPermit::from_closed(
                drain.clone(),
                Some((zone.clone(), enrollment.mount_public_key)),
            )?;
            permit.check_replacement(group, &candidate.next)?;
            let added = &candidate.added;
            added.mount.validate()?;
            let Some(NativeWorkerTarget::Eks {
                pvc_uid, pv_uid, ..
            }) = &candidate.next.native_target
            else {
                return Err(refused());
            };
            if candidate.next.eks_replacement_key != Some(key)
                || added.mount.profile != PoolMountProfile::Ebs
                || added.mount.worker != candidate.next.worker()
                || added.mount.identity.cluster_id != group.cluster_id
                || added.mount.identity.pool_id != growth.intent.pool_id
                || added.mount.generation != growth.intent.pool_generation
                || Some(&added.mount.provider_volume_id) != growth.provider_resource.as_ref()
                || added.mount.filesystem_uuid == mount.expected.filesystem_uuid
                || added
                    .mount
                    .mount_path
                    .starts_with(&mount.expected.mount_path)
                || mount
                    .expected
                    .mount_path
                    .starts_with(&added.mount.mount_path)
                || added.pvc_uid.is_nil()
                || added.pv_uid.is_nil()
                || added.pvc_uid == added.pv_uid
                || [pvc_uid, pv_uid].contains(&&added.pvc_uid)
                || [pvc_uid, pv_uid].contains(&&added.pv_uid)
            {
                return Err(refused());
            }
            Some(old.exclusion_operation)
        }
        _ => return Err(refused()),
    };
    serialized_digest(&(
        b"nabedra/eks-replacement-provider/v1".as_slice(),
        group,
        operation,
        drain,
        growth,
        zone,
        key,
        prior_operation,
        action,
    ))
}

pub(crate) fn refused() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "EKS replacement does not match the closed worker, retained mount or active growth claim",
        Outcome::NotApplied,
    )
}
