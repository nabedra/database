//! System replacement is a separate Control log capability. Its first entry
//! requires an authenticated advert from every current Control voter.
use super::{AuthorizationStamp, GroupRecord, RestartMountKey, restart::RestartMountProof};
use crate::{
    oltp::pool::mount::{PoolMountExpectation, PoolMountProfile},
    oltp_replication::types::*,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const CAPABILITY: &str = "control.system-replacement.v1";
pub(crate) const EXECUTION_CAPABILITY: &str = "control.system-replacement.execute.v1";
pub(crate) const MAX_GROUPS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementIntent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub previous: WorkerIdentity,
    pub next: WorkerEnrollment,
    pub mount: PoolMountExpectation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_public_key: Option<Digest>,
}
impl ReplacementIntent {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.id.is_nil()
            || self.run_id.is_nil()
            || self.previous.validate().is_err()
            || self.next.validate().is_err()
            || self.next.worker_id == self.previous.worker_id
            || self.next.incarnation == self.previous.incarnation
            || self.next.certificate_sha256 == self.previous.certificate_sha256
            || self.next.enrollment_revision != 1
            || self.mount.worker != WorkerIdentity::from(&self.next)
            || self.mount.identity.worker_id != self.next.worker_id
            || self.mount.generation != 1
            || !matches!(self.mount.profile, PoolMountProfile::Docker { .. })
        {
            return Err(invalid());
        }
        self.mount.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementGroup {
    pub expected: GroupRecord,
    pub old: [ReplicaMember; 3],
    pub learner: ReplicaMember,
}

impl ReplacementGroup {
    pub(crate) fn next(&self, lost: Uuid) -> Result<[ReplicaMember; 3], ProtocolError> {
        let mut next = self.old.clone();
        let removed = next
            .iter_mut()
            .find(|member| member.worker_id == lost)
            .ok_or_else(invalid)?;
        *removed = self.learner.clone();
        next.sort_by_key(|member| member.node_id);
        validate_voters(&next).map_err(|_| invalid())?;
        Ok(next)
    }

    pub(crate) fn new(
        expected: GroupRecord,
        old: [ReplicaMember; 3],
        intent: &ReplacementIntent,
    ) -> Result<Self, ProtocolError> {
        let node_id = old
            .iter()
            .map(|member| member.node_id)
            .max()
            .and_then(|id| id.checked_add(1))
            .ok_or_else(invalid)?;
        let learner = ReplicaMember {
            node_id,
            worker_id: intent.next.worker_id,
            pool_id: intent.mount.identity.pool_id,
        };
        Ok(Self {
            expected,
            old,
            learner,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementRecord {
    pub intent: ReplacementIntent,
    pub groups: Vec<ReplacementGroup>,
    pub applied_at: AppliedPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementMountProof {
    pub id: Uuid,
    pub worker: WorkerIdentity,
    pub signature: RestartMountProof,
}

impl ReplacementMountProof {
    fn message(scope: &GroupIdentity, record: &ReplacementRecord) -> Result<Digest, ProtocolError> {
        crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/system-replacement-mount/v1".as_slice(),
            scope,
            record,
        ))
    }

    pub(crate) fn sign(
        scope: &GroupIdentity,
        record: &ReplacementRecord,
        mounted: &PoolMountExpectation,
        key: &RestartMountKey,
    ) -> Result<Self, ProtocolError> {
        if mounted != &record.intent.mount
            || record.intent.mount_public_key != Some(key.public_key())
        {
            return Err(invalid());
        }
        Ok(Self {
            id: record.intent.id,
            worker: WorkerIdentity::from(&record.intent.next),
            signature: key.sign_message(Self::message(scope, record)?),
        })
    }

    pub(super) fn verify(
        &self,
        scope: &GroupIdentity,
        record: &ReplacementRecord,
    ) -> Result<(), ProtocolError> {
        if self.id != record.intent.id || self.worker != WorkerIdentity::from(&record.intent.next) {
            return Err(invalid());
        }
        self.signature.verify_message(
            Self::message(scope, record)?,
            record.intent.mount_public_key.ok_or_else(invalid)?,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SystemRecoveryCommand {
    Enable {
        group: Box<GroupRecord>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        execution: bool,
        voters: [WorkerIdentity; 3],
        enrollment_revision: u64,
        authorization: AuthorizationStamp,
    },
    Mount {
        proof: ReplacementMountProof,
        ready: ReadyMembership,
        enrollment_revision: u64,
        group_inventory_revision: u64,
    },
    Publish {
        id: Uuid,
        group: Uuid,
        applied: AppliedPosition,
        authorization: AuthorizationStamp,
    },
    Prepare {
        intent: Box<ReplacementIntent>,
        ready: ReadyMembership,
        enrollment_revision: u64,
        group_inventory_revision: u64,
        groups: Vec<ReplacementGroup>,
        authorization: AuthorizationStamp,
    },
}

impl SystemRecoveryCommand {
    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Mount { .. } | Self::Publish { .. } => 0,
            Self::Enable { group, .. } => {
                size_of::<GroupRecord>() + group.identity.database.capacity_bytes()
            }
            Self::Prepare { intent, groups, .. } => {
                size_of::<ReplacementIntent>()
                    + intent.next.endpoint.capacity_bytes()
                    + intent.next.server_name.capacity_bytes()
                    + intent.mount.mount_path.capacity()
                    + intent.mount.provider_volume_id.capacity()
                    + intent.mount.filesystem_uuid.capacity()
                    + intent.mount.identity.engine.capacity()
                    + groups.capacity() * size_of::<ReplacementGroup>()
                    + groups
                        .iter()
                        .map(|group| group.expected.identity.database.capacity_bytes())
                        .sum::<usize>()
            }
        }
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "system replacement requires a fresh worker, certificate, and Docker mount",
        Outcome::NotApplied,
    )
}
