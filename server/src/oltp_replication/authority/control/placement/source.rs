use super::super::{
    ReplicaRetiredStamp,
    data_worker::DataWorkerRegistration,
    machine::restart::MountRegistration,
    restart::{RestartMountKey, RestartMountProof},
};
use super::*;
use crate::{
    oltp_deployment::{
        controller::{GrowthPhase, GrowthPolicy, GrowthRecord},
        planner::Action,
    },
    oltp_replication::{
        manager::ReplicaRetired, multiplex::ReceivedReply, owned::serialized_digest,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteSourceBinding {
    pub worker: WorkerIdentity,
    pub pool_generation: u64,
    pub mount_public_key: Digest,
    pub target_claim: Uuid,
}

pub(in crate::oltp_replication::authority::control) fn source_binding(
    request: &ReplicaMoveRequest,
    owner: WorkerIdentity,
    authority: Authority,
    (old, next): (&[ReplicaMember; 3], &[ReplicaMember; 3]),
    (policy, target): (&GrowthPolicy, &GrowthRecord),
    bootstrap_mount: Option<MountRegistration>,
    mut registration: impl FnMut(
        Uuid,
    ) -> Result<
        Option<(DataWorkerRegistration, GrowthRecord)>,
        ProtocolError,
    >,
) -> Result<Option<(RemoteSourceBinding, Digest)>, ProtocolError> {
    let Some(removed) = old
        .iter()
        .find(|member| member.node_id == request.remove_node)
    else {
        return Ok(None);
    };
    if !matches!(request.group.kind, GroupKind::Data { .. })
        || removed.worker_id == owner.worker_id
        || removed.pool_id == request.target.pool_id
        || target.phase != GrowthPhase::Complete
        || target.intent.pool_id != request.target.pool_id
    {
        return Ok(None);
    }
    let target_intent = &target.intent;
    let (worker, pool_generation, mount_public_key, source_zone) =
        if let Some(zone) = policy.initial_replica_zones.get(&removed.worker_id) {
            let Some(mount) = bootstrap_mount else {
                return Ok(None);
            };
            if mount.worker.worker_id != removed.worker_id
                || mount.pool_id != removed.pool_id
                || request.worker.worker_id == owner.worker_id
                || registration(removed.worker_id)?.is_some()
                || !matches!(&target_intent.action, Action::AddWorker { .. })
            {
                return Ok(None);
            }
            (
                mount.worker,
                mount.generation,
                mount.public_key,
                zone.clone(),
            )
        } else {
            let Some((source, growth)) = registration(removed.worker_id)? else {
                return Ok(None);
            };
            let Action::AddWorker { zone, .. } = &growth.intent.action else {
                return Ok(None);
            };
            let Some(mount) = source.mounted_pool(removed.pool_id) else {
                return Ok(None);
            };
            let worker = source.intent.worker();
            if mount.worker != worker
                || source.intent.run_id != authority.run_id
                || source.intent.mount.identity.cluster_id != request.group.cluster_id
                || !source.completed(&growth)
            {
                return Ok(None);
            }
            (
                worker,
                mount.pool_generation,
                source.intent.mount_public_key,
                zone.clone(),
            )
        };
    let target_matches = if request.worker == owner {
        let exact = target_intent.worker == owner
            && target_intent.pool_generation == request.pool_generation;
        let retained = target_intent.worker.worker_id == owner.worker_id
            && target_intent.worker.enrollment_revision < owner.enrollment_revision
            && target_intent.pool_generation < request.pool_generation;
        (exact || retained)
            && policy.initial_replica_zones.get(&owner.worker_id) == Some(&source_zone)
            && matches!(&target_intent.action, Action::AddPool { worker, zone, .. }
                if *worker == owner.worker_id && zone == &source_zone)
    } else {
        let Some((registered, growth)) = registration(request.worker.worker_id)? else {
            return Ok(None);
        };
        let Some(mount) = registered.mounted_pool(request.target.pool_id) else {
            return Ok(None);
        };
        registered.intent.worker() == request.worker
            && registered.intent.run_id == authority.run_id
            && registered.intent.mount.identity.cluster_id == request.group.cluster_id
            && registered.completed(&growth)
            && mount.worker == request.worker
            && mount.pool_generation == request.pool_generation
            && match &target_intent.action {
                Action::AddWorker { zone, .. } => {
                    removed.worker_id != request.worker.worker_id
                        && zone == &source_zone
                        && target == &growth
                }
                Action::AddPool { worker, zone, .. } => {
                    *worker == request.worker.worker_id
                        && zone == &source_zone
                        && request.target.pool_id != registered.mount.pool_id
                        && target_intent.worker.worker_id == request.worker.worker_id
                        && target_intent.worker.enrollment_revision
                            < request.worker.enrollment_revision
                        && target_intent.pool_generation == mount.pool_generation
                        && target.provider_resource.as_deref()
                            == Some(mount.provider_volume_id.as_str())
                        && target.mounted_total_bytes == Some(mount.total_bytes)
                }
                Action::Expand { .. } => false,
            }
    };
    if !target_matches {
        return Ok(None);
    }
    let Some(domains) = super::remote::failure_domains(old, next, policy, registration)? else {
        return Ok(None);
    };
    Ok(Some((
        RemoteSourceBinding {
            worker,
            pool_generation,
            mount_public_key,
            target_claim: target_intent.claim_id,
        },
        domains,
    )))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteRetirementAttestation {
    stamp: ReplicaRetiredStamp,
    signature: RestartMountProof,
}

impl RemoteRetirementAttestation {
    pub(crate) fn from_native(
        control: &GroupIdentity,
        plan: &ReplicaMove,
        retired: &ReplicaRetired,
        key: &RestartMountKey,
    ) -> Result<Self, ProtocolError> {
        if plan
            .remote_source
            .as_ref()
            .is_none_or(|source| source.mount_public_key != key.public_key())
        {
            return Err(invalid());
        }
        let stamp = retired.stamp().clone();
        Ok(Self {
            signature: key.sign_message(message(control, plan, &stamp)?),
            stamp,
        })
    }
}

pub(crate) struct VerifiedRemoteRetirement {
    attestation: RemoteRetirementAttestation,
    _owner: std::sync::Arc<crate::oltp::pool::ScratchLease>,
}

impl VerifiedRemoteRetirement {
    pub(crate) fn from_authenticated(
        reply: &ReceivedReply,
        control: &GroupIdentity,
        plan: &ReplicaMove,
        request: Digest,
    ) -> Result<Self, ProtocolError> {
        let value = super::decode_reply::<RemoteRetirementAttestation>(
            reply,
            control,
            plan.source_worker(),
            request,
        )?;
        verify(control, plan, &value.stamp, &value.signature)?;
        Ok(Self {
            attestation: value,
            _owner: reply.scratch.clone(),
        })
    }

    pub(in crate::oltp_replication::authority::control) fn parts(
        &self,
    ) -> (&ReplicaRetiredStamp, &RestartMountProof) {
        (&self.attestation.stamp, &self.attestation.signature)
    }
}

pub(in crate::oltp_replication::authority::control) fn verify(
    control: &GroupIdentity,
    plan: &ReplicaMove,
    stamp: &ReplicaRetiredStamp,
    signature: &RestartMountProof,
) -> Result<(), ProtocolError> {
    let source = plan.remote_source.as_ref().ok_or_else(invalid)?;
    signature.verify_message(message(control, plan, stamp)?, source.mount_public_key)
}

fn message(
    control: &GroupIdentity,
    plan: &ReplicaMove,
    stamp: &ReplicaRetiredStamp,
) -> Result<Digest, ProtocolError> {
    let (placement, activated_generation, activated_at, activation_lineage) = match &plan.phase {
        ReplicaMovePhase::Published {
            placement,
            activated_generation,
            activated_at,
            activation_lineage,
        }
        | ReplicaMovePhase::Retired {
            placement,
            activated_generation,
            activated_at,
            activation_lineage,
            ..
        } => (
            placement,
            activated_generation,
            activated_at,
            activation_lineage,
        ),
        _ => return Err(invalid()),
    };
    if control.kind != GroupKind::Control
        || control.cluster_id != stamp.group.cluster_id
        || control.database != stamp.group.database
        || plan.remote_source.is_none()
        || plan.source_worker() != stamp.worker
        || plan.removed() != Some(&stamp.replica)
        || plan.request.group != stamp.group
        || plan.id != stamp.generation
        || stamp.applied_at.index < placement.membership_position.index
        || (stamp.applied_at.index == placement.membership_position.index
            && stamp.applied_at != placement.membership_position)
    {
        return Err(invalid());
    }
    serialized_digest(&(
        b"nabedra/remote-replica-retirement/v1".as_slice(),
        control,
        plan.binding()?,
        placement,
        activated_generation,
        activated_at,
        activation_lineage,
        stamp,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        oltp::pool::{PoolIdentity, mount::{PoolMountExpectation, PoolMountProfile}},
        oltp_deployment::controller::{GrowthIntent, MountedGrowthStamp},
        oltp_replication::authority::control::DataWorkerIntent,
    };

    fn registered(worker: u128, pool: u128, cluster: Uuid, run: Uuid, zone: &str)
        -> (DataWorkerRegistration, GrowthRecord)
    {
        let enrollment = WorkerEnrollment {
            worker_id: Uuid::from_u128(worker),
            incarnation: Uuid::from_u128(worker + 100),
            enrollment_revision: 1,
            certificate_sha256: Digest::from_bytes([worker as u8; 32]),
            endpoint: Text::try_from("127.0.0.1:8000").unwrap(),
            server_name: Text::try_from("worker.local").unwrap(),
        };
        let worker = WorkerIdentity::from(&enrollment);
        let pool_id = Uuid::from_u128(pool);
        let claim_id = Uuid::from_u128(pool + 100);
        let volume = format!("volume-{pool}");
        let intent = DataWorkerIntent {
            claim_id,
            run_id: run,
            enrollment,
            mount: PoolMountExpectation {
                profile: PoolMountProfile::default(),
                identity: PoolIdentity {
                    engine: "oltp".into(), pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                    cluster_id: cluster, worker_id: worker.worker_id, pool_id,
                },
                worker,
                generation: 1,
                mount_path: "/mounted".into(),
                provider_volume_id: volume.clone(),
                filesystem_uuid: Uuid::from_u128(pool + 200).to_string(),
            },
            mount_public_key: Digest::from_bytes([pool as u8; 32]),
            native_target: None,
            eks_replacement_key: None,
        };
        let mount = MountedGrowthStamp {
            worker, pool_id, pool_generation: 1, provider_volume_id: volume.clone(), total_bytes: 100,
        };
        let growth = GrowthRecord {
            intent: GrowthIntent {
                policy_id: Uuid::from_u128(32), claim_id,
                action: Action::AddWorker { zone: zone.into(), bytes: 100 },
                worker, pool_id, pool_generation: 1, quoted_monthly_cost: 1,
                monthly_spend_ceiling: 100, max_attempts: 2, cooldown_seconds: 1,
            },
            phase: GrowthPhase::Complete, attempts: 1, last_attempt: Some(1),
            provider_resource: Some(volume), mounted_total_bytes: Some(100), prior_total_bytes: None,
        };
        (DataWorkerRegistration { intent, mount, replacement: None }, growth)
    }

    #[test]
    fn o16_o26_o27_remote_source_requires_exact_target_and_three_zones() {
        let cluster_id = Uuid::from_u128(30);
        let authority = Authority { run_id: Uuid::from_u128(31), owner_term: 2 };
        let (mut source, source_claim) = registered(3, 13, cluster_id, authority.run_id, "zone-c");
        let (mut target, mut target_claim) = registered(4, 14, cluster_id, authority.run_id, "zone-c");
        let owner = registered(1, 11, cluster_id, authority.run_id, "zone-a").0.intent.worker();
        let member = |node, worker, pool| ReplicaMember {
            node_id: node, worker_id: Uuid::from_u128(worker), pool_id: Uuid::from_u128(pool),
        };
        let old = [member(1, 1, 11), member(2, 2, 12), member(3, 3, 13)];
        let mut request = ReplicaMoveRequest {
            group: GroupIdentity {
                cluster_id, database: Text::try_from("move").unwrap(), group_id: Uuid::from_u128(33),
                kind: GroupKind::Data { table_id: Uuid::from_u128(34), shard_id: Uuid::from_u128(35) },
                group_format_version: GROUP_FORMAT_VERSION,
            },
            expected_generation: Uuid::from_u128(36),
            remove_node: 3,
            target: member(4, 4, 14),
            worker: target.intent.worker(),
            pool_generation: target.mount.pool_generation,
        };
        let policy = GrowthPolicy {
            policy_id: Uuid::from_u128(32), volume_cost_per_gib: 1, worker_cost: 1,
            initial_monthly_spend: 0, monthly_spend_ceiling: 100, max_attempts: 2,
            cooldown_seconds: 1, max_volume_bytes: 1000,
            zones: ["zone-a", "zone-b", "zone-c"].map(String::from).to_vec(),
            initial_replica_zones: [(owner.worker_id, "zone-a".into()), (old[1].worker_id, "zone-b".into())]
                .into_iter().collect(),
            automatic: None,
        };
        let bind = |request: &ReplicaMoveRequest, source: &DataWorkerRegistration,
                    target: &DataWorkerRegistration, claim: &GrowthRecord| {
            let next = [old[0].clone(), old[1].clone(), request.target.clone()];
            source_binding(request, owner, authority, (&old, &next), (&policy, claim), None, |worker| {
                Ok(if worker == source.intent.worker().worker_id {
                    Some((source.clone(), source_claim.clone()))
                } else if worker == target.intent.worker().worker_id {
                    Some((target.clone(), claim.clone()))
                } else {
                    None
                })
            }).unwrap()
        };
        let (bound, _) = bind(&request, &source, &target, &target_claim).unwrap();
        assert_eq!(bound.worker, source.intent.worker());
        assert_eq!(bound.pool_generation, source.mount.pool_generation);
        assert_eq!(bound.target_claim, target_claim.intent.claim_id);
        request.pool_generation += 1;
        assert!(bind(&request, &source, &target, &target_claim).is_none());
        request.pool_generation -= 1;
        target_claim.intent.action = Action::AddWorker { zone: "zone-b".into(), bytes: 100 };
        assert!(bind(&request, &source, &target, &target_claim).is_none());
        target_claim.intent.action = Action::AddWorker { zone: "zone-c".into(), bytes: 100 };
        target.intent.run_id = Uuid::from_u128(37);
        assert!(bind(&request, &source, &target, &target_claim).is_none());
        source.intent.run_id = Uuid::from_u128(38);
        target.intent.run_id = authority.run_id;
        assert!(bind(&request, &source, &target, &target_claim).is_none());

        source.intent.run_id = authority.run_id;
        source.intent.enrollment.enrollment_revision += 1;
        source.intent.mount.worker = source.intent.worker();
        source.intent.mount.generation += 1;
        source.mount.worker = source.intent.worker();
        source.mount.pool_generation += 1;
        request.worker = source.intent.worker();
        request.target = member(4, 3, 14);
        target_claim.intent.worker = source_claim.intent.worker;
        target_claim.intent.action = Action::AddPool {
            worker: request.worker.worker_id,
            zone: "zone-c".into(),
            bytes: 100,
        };
        assert!(bind(&request, &source, &source, &target_claim).is_none());
        source.replacement = Some(MountedGrowthStamp {
            worker: request.worker,
            ..target.mount
        });
        let (bound, _) = bind(&request, &source, &source, &target_claim).unwrap();
        assert_eq!(bound.worker, request.worker);
        assert_eq!(bound.pool_generation, 2);
        assert_eq!(bound.target_claim, target_claim.intent.claim_id);
        target_claim.mounted_total_bytes = None;
        assert!(bind(&request, &source, &source, &target_claim).is_none());
    }

    #[test]
    fn o16_o21_o26_o28_bootstrap_source_requires_primary_and_enrolled_same_zone_target() {
        let cluster_id = Uuid::from_u128(40);
        let authority = Authority {
            run_id: Uuid::from_u128(41),
            owner_term: 2,
        };
        let owner = registered(1, 11, cluster_id, authority.run_id, "zone-a")
            .0
            .intent
            .worker();
        let source = registered(2, 12, cluster_id, authority.run_id, "zone-b")
            .0
            .intent
            .worker();
        let (target, target_claim) = registered(4, 14, cluster_id, authority.run_id, "zone-b");
        let member = |node, worker, pool| ReplicaMember {
            node_id: node,
            worker_id: Uuid::from_u128(worker),
            pool_id: Uuid::from_u128(pool),
        };
        let old = [member(1, 1, 11), member(2, 2, 12), member(3, 3, 13)];
        let request = ReplicaMoveRequest {
            group: GroupIdentity {
                cluster_id,
                database: Text::try_from("move").unwrap(),
                group_id: Uuid::from_u128(42),
                kind: GroupKind::Data {
                    table_id: Uuid::from_u128(43),
                    shard_id: Uuid::from_u128(44),
                },
                group_format_version: GROUP_FORMAT_VERSION,
            },
            expected_generation: Uuid::from_u128(45),
            remove_node: 2,
            target: member(4, 4, 14),
            worker: target.intent.worker(),
            pool_generation: target.mount.pool_generation,
        };
        let mut policy = GrowthPolicy {
            policy_id: target_claim.intent.policy_id,
            volume_cost_per_gib: 1,
            worker_cost: 1,
            initial_monthly_spend: 0,
            monthly_spend_ceiling: 100,
            max_attempts: 2,
            cooldown_seconds: 1,
            max_volume_bytes: 1000,
            zones: ["zone-a", "zone-b", "zone-c"].map(String::from).to_vec(),
            initial_replica_zones: old
                .iter()
                .zip(["zone-a", "zone-b", "zone-c"])
                .map(|(member, zone)| (member.worker_id, zone.into()))
                .collect(),
            automatic: None,
        };
        let mount_key = RestartMountKey::from_seed(&[7; 32]).unwrap().public_key();
        let mount = |pool_id| MountRegistration {
            worker: source,
            pool_id,
            generation: 7,
            public_key: mount_key,
        };
        let bind = |request: &ReplicaMoveRequest, policy: &GrowthPolicy, bootstrap_mount| {
            let next = [old[0].clone(), old[2].clone(), request.target.clone()];
            source_binding(
                request,
                owner,
                authority,
                (&old, &next),
                (policy, &target_claim),
                bootstrap_mount,
                |worker| {
                    Ok((worker == target.intent.worker().worker_id)
                        .then(|| (target.clone(), target_claim.clone())))
                },
            )
            .unwrap()
        };
        let (bound, _) = bind(&request, &policy, Some(mount(old[1].pool_id))).unwrap();
        assert_eq!(bound.worker, source);
        assert_eq!(bound.pool_generation, 7);
        assert_eq!(bound.mount_public_key, mount_key);
        assert_eq!(bound.target_claim, target_claim.intent.claim_id);
        assert!(bind(&request, &policy, None).is_none());
        assert!(bind(&request, &policy, Some(mount(Uuid::from_u128(112)))).is_none());

        let mut stale = request.clone();
        stale.worker.incarnation = Uuid::from_u128(500);
        assert!(bind(&stale, &policy, Some(mount(old[1].pool_id))).is_none());
        stale = request.clone();
        stale.pool_generation += 1;
        assert!(bind(&stale, &policy, Some(mount(old[1].pool_id))).is_none());
        policy
            .initial_replica_zones
            .insert(source.worker_id, "zone-a".into());
        assert!(bind(&request, &policy, Some(mount(old[1].pool_id))).is_none());
    }
}
