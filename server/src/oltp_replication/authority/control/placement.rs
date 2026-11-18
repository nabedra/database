use super::model::{GroupMembershipStamp, GroupRecord};
use crate::{
    oltp::{
        mvcc::store::lifecycle::{LifecycleBinding, SealedLifecycleGuard},
        pool::{ReplicaActivationStatement, VerifiedReplicaActivation},
    },
    oltp_replication::types::*,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaMoveRequest {
    pub group: GroupIdentity,
    pub expected_generation: Uuid,
    pub remove_node: u64,
    pub target: ReplicaMember,
    pub worker: WorkerIdentity,
    pub pool_generation: u64,
}

impl ReplicaMoveRequest {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.group.validate().is_err()
            || !matches!(self.group.kind, GroupKind::Data { .. })
            || self.expected_generation.is_nil()
            || self.remove_node == 0
            || self.remove_node == self.target.node_id
            || self.target.validate().is_err()
            || self.worker.validate().is_err()
            || self.target.worker_id != self.worker.worker_id
            || self.pool_generation == 0
        {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid RF3 Data replica move",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaMove {
    pub id: Uuid,
    pub request: ReplicaMoveRequest,
    pub owner: WorkerIdentity,
    pub authority: Authority,
    pub expected: GroupRecord,
    pub old: [ReplicaMember; 3],
    pub next: [ReplicaMember; 3],
    pub lineage: ReplicaMoveLineage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_domains: Option<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_source: Option<RemoteSourceBinding>,
    pub guard: Option<SealedLifecycleGuard>,
    pub phase: ReplicaMovePhase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaMoveLineage {
    pub catalog: Uuid,
    pub table: Uuid,
    pub table_version: CommitTs,
    pub schema_epoch: u64,
    pub route_digest: Digest,
}

impl ReplicaMove {
    pub(crate) fn remote_target(&self) -> bool {
        self.request.worker.worker_id != self.owner.worker_id
    }

    pub(crate) fn source_worker(&self) -> WorkerIdentity {
        self.remote_source.as_ref().map_or(self.owner, |source| source.worker)
    }

    pub(crate) fn removed(&self) -> Option<&ReplicaMember> {
        self.old
            .iter()
            .find(|member| member.node_id == self.request.remove_node)
    }

    pub(crate) fn binding(&self) -> Result<Digest, ProtocolError> {
        let original = crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/replica-move/v1",
            self.id,
            &self.request,
            self.owner,
            self.authority,
            &self.expected,
            &self.old,
            &self.lineage,
        ))?;
        let original = match self.failure_domains {
            Some(domains) => crate::oltp_replication::owned::serialized_digest(&(
                b"nabedra/replica-move/v2",
                original,
                domains,
            )),
            None => Ok(original),
        }?;
        match &self.remote_source {
            Some(source) => crate::oltp_replication::owned::serialized_digest(&(
                b"nabedra/replica-move/v3",
                original,
                source,
            )),
            None => Ok(original),
        }
    }

    pub(crate) fn lifecycle(&self) -> Result<LifecycleBinding, ProtocolError> {
        let digest = self.binding()?;
        let id = crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/replica-move-guard/v1",
            self.id,
        ))?;
        let request_id = Uuid::from_slice(&id.as_bytes()[..16]).map_err(|_| invalid())?;
        Ok(LifecycleBinding {
            guard_id: self.id,
            catalog_group_id: self.lineage.catalog,
            table_id: self.lineage.table,
            lifecycle_version: Some(self.lineage.table_version),
            schema_epoch: self.lineage.schema_epoch,
            txn: TxnId {
                request_id,
                attempt: 1,
            },
            request: RequestBinding {
                run_id: self.authority.run_id,
                request_id,
                digest,
                accepted_context: id,
            },
            read_ts: self.lineage.table_version,
            authority: self.authority,
        })
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid replica move lifecycle binding",
        Outcome::NotApplied,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub(crate) enum ReplicaMovePhase {
    Claimed,
    Moving,
    Published {
        placement: GroupRecord,
        activated_generation: u64,
        activated_at: AppliedPosition,
        activation_lineage: Digest,
    },
    Retired {
        placement: GroupRecord,
        activated_generation: u64,
        activated_at: AppliedPosition,
        activation_lineage: Digest,
        retired_at: AppliedPosition,
    },
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaMovePublication {
    pub worker: WorkerIdentity,
    pub membership: GroupMembershipStamp,
    pub activation: ReplicaActivationStatement,
    pub guard: SealedLifecycleGuard,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<super::restart::RestartMountProof>,
}

mod remote;
pub(in crate::oltp_replication::authority::control) use remote::failure_domains;
mod source;
pub(crate) use remote::{RemoteActivationAttestation, RemoteMoveView, RemotePlacementReply, VerifiedRemoteActivation, decode_reply};
pub(super) use remote::{RemoteTargetBinding, target_binding, verify as verify_remote_activation};
pub(crate) use source::{RemoteRetirementAttestation, RemoteSourceBinding, VerifiedRemoteRetirement};
pub(super) use source::{source_binding, verify as verify_remote_retirement};

#[derive(Clone, Copy)]
pub(crate) enum ReplicaActivationEvidence<'a> {
    Local(&'a VerifiedReplicaActivation),
    Remote(&'a VerifiedRemoteActivation),
}
