//! A fresh data worker is prepared by an operator against an exact durable
//! growth claim. Its mounted key is provisioned out of band with that volume.
use nabedra_oltp_types::queue::QueueAuthority;
use super::{
    restart::{RestartMountKey, RestartMountProof},
    worker_drain::NativeWorkerTarget,
};
use crate::{
    oltp::pool::mount::{MountedPoolObservation, PoolMountExpectation, PoolMountProfile},
    oltp_deployment::{
        controller::{GrowthPhase, GrowthRecord, MountedGrowthStamp},
        live::InventoryPin,
        planner::Action,
    },
    oltp_replication::{owned::serialized_digest, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataWorkerIntent {
    pub claim_id: Uuid,
    pub run_id: Uuid,
    pub enrollment: WorkerEnrollment,
    pub mount: PoolMountExpectation,
    pub mount_public_key: Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_target: Option<NativeWorkerTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eks_replacement_key: Option<Digest>,
}

impl DataWorkerIntent {
    pub(crate) fn validate(&self, control: &GroupIdentity) -> Result<(), ProtocolError> {
        self.enrollment.validate().map_err(|_| refused())?;
        self.mount.validate()?;
        if self.run_id.is_nil()
            || self.claim_id.is_nil()
            || self.mount.worker != self.worker()
            || self.mount.identity.cluster_id != control.cluster_id
            || self.mount.mount_path.as_os_str().len() > 1024
            || self.mount_public_key.as_bytes() == &[0; 32]
            || self.native_target.as_ref().is_some_and(|target| {
                !target.valid() || self.mount.profile != PoolMountProfile::Ebs
            })
            || self.eks_replacement_key.is_some_and(|key| {
                key.as_bytes() == &[0; 32]
                    || key == self.mount_public_key
                    || !matches!(self.native_target, Some(NativeWorkerTarget::Eks { .. }))
            })
        {
            return Err(refused());
        }
        Ok(())
    }

    pub(super) fn worker(&self) -> WorkerIdentity {
        WorkerIdentity::from(&self.enrollment)
    }

    pub(super) fn matches_claim(&self, record: &GrowthRecord) -> bool {
        record.phase == GrowthPhase::AwaitingMount && self.matches_growth_identity(record)
    }

    fn matches_growth_identity(&self, record: &GrowthRecord) -> bool {
        matches!(record.intent.action, Action::AddWorker { .. })
            && record.intent.claim_id == self.claim_id
            && record.intent.worker == self.worker()
            && record.intent.pool_id == self.mount.identity.pool_id
            && record.intent.pool_generation == self.mount.generation
            && record.provider_resource.as_deref() == Some(self.mount.provider_volume_id.as_str())
    }

    pub(super) fn heap_bytes(&self) -> usize {
        self.enrollment.endpoint.capacity_bytes()
            + self.enrollment.server_name.capacity_bytes()
            + self.mount.mount_path.capacity()
            + self.mount.provider_volume_id.capacity()
            + self.mount.filesystem_uuid.capacity()
            + self.mount.identity.engine.capacity()
            + self
                .native_target
                .as_ref()
                .map_or(0, NativeWorkerTarget::heap_bytes)
    }

    fn message(
        &self,
        control: &GroupIdentity,
        stamp: &MountedGrowthStamp,
    ) -> Result<Digest, ProtocolError> {
        serialized_digest(&(
            b"nabedra/data-worker-mount/v1".as_slice(),
            control,
            self,
            stamp,
        ))
    }

    pub(super) fn prove(
        &self,
        control: &GroupIdentity,
        mount: &MountedPoolObservation,
        key: &RestartMountKey,
    ) -> Result<(MountedGrowthStamp, RestartMountProof), ProtocolError> {
        self.validate(control)?;
        if self.mount != mount.expected || self.mount_public_key != key.public_key() {
            return Err(refused());
        }
        let stamp = MountedGrowthStamp {
            worker: self.worker(),
            pool_id: self.mount.identity.pool_id,
            pool_generation: self.mount.generation,
            provider_volume_id: self.mount.provider_volume_id.clone(),
            total_bytes: mount.total_bytes,
        };
        let signature = key.sign_message(self.message(control, &stamp)?);
        Ok((stamp, signature))
    }

    pub(super) fn verify(
        &self,
        control: &GroupIdentity,
        stamp: &MountedGrowthStamp,
        proof: &RestartMountProof,
        claim: &GrowthRecord,
    ) -> Result<(), ProtocolError> {
        if !self.matches_claim(claim)
            || stamp.worker != self.worker()
            || stamp.pool_id != self.mount.identity.pool_id
            || stamp.pool_generation != self.mount.generation
            || stamp.provider_volume_id != self.mount.provider_volume_id
            || stamp.total_bytes < claim.intent.requested_bytes()
        {
            return Err(refused());
        }
        proof.verify_message(self.message(control, stamp)?, self.mount_public_key)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DataWorkerRegistration {
    pub intent: DataWorkerIntent,
    pub mount: MountedGrowthStamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement: Option<MountedGrowthStamp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataWorkerGrowthProof {
    pub pin: InventoryPin,
    pub authority: QueueAuthority,
    pub primary: DataWorkerGrowthPool,
    pub added: Option<DataWorkerGrowthPool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataWorkerGrowthPool {
    pub claim_id: Uuid,
    pub mount: MountedGrowthStamp,
}

impl DataWorkerGrowthPool {
    pub(crate) fn matches_mount(&self, observed: &MountedPoolObservation) -> bool {
        let expected = &observed.expected;
        expected.worker == self.mount.worker
            && expected.identity.pool_id == self.mount.pool_id
            && expected.generation == self.mount.pool_generation
            && expected.provider_volume_id == self.mount.provider_volume_id
            && observed.mount_id != 0
            && observed.total_bytes >= self.mount.total_bytes
            && observed.available_bytes <= observed.total_bytes
    }
}

impl DataWorkerRegistration {
    pub(super) fn completed(&self, claim: &GrowthRecord) -> bool {
        claim.phase == GrowthPhase::Complete
            && (self.intent.matches_growth_identity(claim)
                || self.replacement.is_some()
                    && matches!(claim.intent.action, Action::AddWorker { .. })
                    && claim.intent.claim_id == self.intent.claim_id
                    && claim.intent.worker.worker_id == self.intent.worker().worker_id
                    && claim.intent.worker.enrollment_revision < self.intent.worker().enrollment_revision
                    && claim.intent.pool_id == self.mount.pool_id
                    && claim.intent.pool_generation.checked_add(1) == Some(self.mount.pool_generation)
                    && claim.provider_resource.as_deref() == Some(self.mount.provider_volume_id.as_str()))
            && self.mount.worker == self.intent.worker()
            && self.mount.pool_id == self.intent.mount.identity.pool_id
            && self.mount.pool_generation == self.intent.mount.generation
            && self.mount.provider_volume_id == self.intent.mount.provider_volume_id
            && if self.replacement.is_some() {
                self.mount.total_bytes >= claim.intent.requested_bytes()
            } else {
                claim.mounted_total_bytes == Some(self.mount.total_bytes)
            }
    }

    pub(super) fn mounted_pool(&self, pool: Uuid) -> Option<&MountedGrowthStamp> {
        if self.mount.pool_id == pool { Some(&self.mount) }
        else { self.replacement.as_ref().filter(|added| added.pool_id == pool) }
    }
}

fn refused() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "data worker growth claim, mount or signing key differs",
        Outcome::NotApplied,
    )
}
