use super::super::{
    data_worker::DataWorkerRegistration,
    restart::{RestartMountKey, RestartMountProof},
};
use super::*;
use crate::{
    oltp_deployment::{
        controller::{GrowthPolicy, GrowthRecord},
        planner::Action,
    },
    oltp_replication::{
        multiplex::{Purpose, ReceivedReply},
        owned::serialized_digest,
    },
};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Copy)]
pub(in crate::oltp_replication::authority::control) struct RemoteTargetBinding {
    pub domains: Digest,
    pub mount_public_key: Digest,
}

pub(in crate::oltp_replication::authority::control) fn target_binding(
    request: &ReplicaMoveRequest,
    authority: Authority,
    old: &[ReplicaMember; 3],
    next: &[ReplicaMember; 3],
    policy: &GrowthPolicy,
    mut registration: impl FnMut(
        Uuid,
    ) -> Result<
        Option<(DataWorkerRegistration, GrowthRecord)>,
        ProtocolError,
    >,
) -> Result<Option<RemoteTargetBinding>, ProtocolError> {
    let Some((target, growth)) = registration(request.worker.worker_id)? else {
        return Ok(None);
    };
    let Action::AddWorker { zone, .. } = &growth.intent.action else {
        return Ok(None);
    };
    if target.intent.worker() != request.worker
        || target.intent.run_id != authority.run_id
        || target.intent.mount.identity.cluster_id != request.group.cluster_id
        || target.mounted_pool(request.target.pool_id).is_none_or(|mount| mount.pool_generation != request.pool_generation)
        || !target.completed(&growth)
        || !policy.zones.contains(zone)
        || policy
            .initial_replica_zones
            .contains_key(&request.worker.worker_id)
    {
        return Ok(None);
    }
    let Some(domains) = failure_domains(old, next, policy, registration)? else {
        return Ok(None);
    };
    Ok(Some(RemoteTargetBinding {
        domains,
        mount_public_key: target.intent.mount_public_key,
    }))
}

pub(in crate::oltp_replication::authority::control) fn failure_domains(
    old: &[ReplicaMember; 3],
    next: &[ReplicaMember; 3],
    policy: &GrowthPolicy,
    mut registration: impl FnMut(Uuid) -> Result<Option<(DataWorkerRegistration, GrowthRecord)>, ProtocolError>,
) -> Result<Option<Digest>, ProtocolError> {
    let mut zones =
        |voters: &[ReplicaMember; 3]| -> Result<Option<Vec<(Uuid, String)>>, ProtocolError> {
            let mut map = Vec::with_capacity(3);
            for voter in voters {
                let worker = voter.worker_id;
                let label = if let Some((record, claim)) = registration(worker)? {
                    match &claim.intent.action {
                        Action::AddWorker { zone, .. }
                            if record.completed(&claim)
                                && record.mounted_pool(voter.pool_id).is_some() =>
                        {
                            zone.clone()
                        }
                        _ => return Ok(None),
                    }
                } else if let Some(label) = policy.initial_replica_zones.get(&worker) {
                    label.clone()
                } else {
                    return Ok(None);
                };
                if !policy.zones.contains(&label) {
                    return Ok(None);
                }
                map.push((worker, label));
            }
            Ok(Some(map))
        };
    let (Some(old_zones), Some(next_zones)) = (zones(old)?, zones(next)?) else {
        return Ok(None);
    };
    let domains = |values: &[(Uuid, String)]| {
        values
            .iter()
            .map(|(_, zone)| zone.clone())
            .collect::<BTreeSet<_>>()
    };
    let old_domains = domains(&old_zones);
    if old_domains.len() != 3 || old_domains != domains(&next_zones) {
        return Ok(None);
    }
    Ok(Some(serialized_digest(&(
        b"nabedra/replica-failure-domains/v1".as_slice(),
        old_zones,
        next_zones,
    ))?))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteMoveView {
    pub plan: ReplicaMove,
    pub eligible: bool,
    pub fenced: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteActivationAttestation {
    activation: ReplicaActivationStatement,
    signature: RestartMountProof,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemotePlacementReply<T> {
    pub nonce: Uuid,
    pub request: Digest,
    pub result: Result<T, ProtocolError>,
}

impl RemoteActivationAttestation {
    pub(crate) fn from_native(
        control: &GroupIdentity,
        plan: &ReplicaMove,
        native: &VerifiedReplicaActivation,
        key: &RestartMountKey,
    ) -> Result<Self, ProtocolError> {
        let activation = native.statement().clone();
        Ok(Self {
            signature: key.sign_message(message(control, plan, &activation)?),
            activation,
        })
    }
}

pub(crate) struct VerifiedRemoteActivation {
    attestation: RemoteActivationAttestation,
    _owner: Arc<crate::oltp::pool::ScratchLease>,
}

impl VerifiedRemoteActivation {
    pub(crate) fn from_authenticated(
        reply: &ReceivedReply,
        control: &GroupIdentity,
        plan: &ReplicaMove,
        minimum: AppliedPosition,
        request: Digest,
        key: Digest,
    ) -> Result<Self, ProtocolError> {
        let value = decode_reply::<RemoteActivationAttestation>(
            reply,
            control,
            plan.request.worker,
            request,
        )?;
        if value.activation.minimum != minimum {
            return Err(invalid());
        }
        verify(control, plan, &value.activation, &value.signature, key)?;
        Ok(Self {
            attestation: value,
            _owner: reply.scratch_lease(),
        })
    }

    pub(in crate::oltp_replication::authority::control) fn parts(
        &self,
    ) -> (&ReplicaActivationStatement, &RestartMountProof) {
        (&self.attestation.activation, &self.attestation.signature)
    }
}

pub(crate) fn decode_reply<T: serde::de::DeserializeOwned + Clone>(
    reply: &ReceivedReply,
    control: &GroupIdentity,
    target: WorkerIdentity,
    request: Digest,
) -> Result<T, ProtocolError> {
    if reply.group() != control
        || reply.purpose() != Purpose::ReplicaPlacement
        || reply.remote_identity() != target
        || reply.sections.iter().any(Option::is_some)
    {
        return Err(invalid());
    }
    let decoded = reply.decode_header::<RemotePlacementReply<T>>()?;
    if decoded.request != request || decoded.nonce != reply.nonce() {
        return Err(invalid());
    }
    decoded.result.clone()
}

pub(in crate::oltp_replication::authority::control) fn verify(
    control: &GroupIdentity,
    plan: &ReplicaMove,
    activation: &ReplicaActivationStatement,
    signature: &RestartMountProof,
    mount_key: Digest,
) -> Result<(), ProtocolError> {
    signature.verify_message(message(control, plan, activation)?, mount_key)
}

fn message(
    control: &GroupIdentity,
    plan: &ReplicaMove,
    activation: &ReplicaActivationStatement,
) -> Result<Digest, ProtocolError> {
    let request = &plan.request;
    if control.kind != GroupKind::Control
        || control.cluster_id != request.group.cluster_id
        || control.database != request.group.database
        || plan.failure_domains.is_none()
        || plan.owner == request.worker
        || activation.group != request.group
        || activation.pool.pool_id != request.target.pool_id
        || activation.pool.worker_id != request.worker.worker_id
        || activation.registered_owner != Some((request.worker, request.pool_generation))
        || activation.minimum.index <= plan.expected.membership_position.index
    {
        return Err(invalid());
    }
    serialized_digest(&(
        b"nabedra/remote-replica-activation/v1".as_slice(),
        control,
        plan.binding()?,
        activation,
    ))
}
