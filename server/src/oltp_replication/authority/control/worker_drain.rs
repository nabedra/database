use super::{DrainMount, RestartMountKey, data_worker::DataWorkerRegistration, model::unavailable, restart::RestartMountProof};
use crate::{
    oltp::pool::{VerifiedPoolClosed, mount::PoolMountProfile},
    oltp_deployment::{controller::{GrowthPhase, GrowthRecord, MountedGrowthStamp}, planner::Action},
    oltp_replication::{owned::serialized_digest, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod replacement;
pub(crate) use replacement::*;

/// Recorded in the protected data-worker enrollment and covered by its native
/// mount proof. Kubernetes names derive from the stable worker and pool UUIDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NativeWorkerTarget {
    Ec2 {
        instance_id: String,
    },
    Eks {
        namespace: String,
        statefulset_uid: Uuid,
        pod_uid: Uuid,
        pvc_uid: Uuid,
        pv_uid: Uuid,
    },
}

impl NativeWorkerTarget {
    pub(super) fn valid(&self) -> bool {
        match self {
            Self::Ec2 { instance_id } => instance_id.strip_prefix("i-").is_some_and(|id| {
                matches!(id.len(), 8 | 17)
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            }),
            Self::Eks {
                namespace,
                statefulset_uid,
                pod_uid,
                pvc_uid,
                pv_uid,
            } => {
                let literal = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
                let ids = [statefulset_uid, pod_uid, pvc_uid, pv_uid];
                namespace.len() <= 63
                    && namespace.bytes().next().is_some_and(literal)
                    && namespace.bytes().last().is_some_and(literal)
                    && namespace.bytes().all(|byte| literal(byte) || byte == b'-')
                    && ids
                        .iter()
                        .enumerate()
                        .all(|(index, id)| !id.is_nil() && !ids[..index].contains(id))
            }
        }
    }

    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Ec2 { instance_id } => instance_id.capacity(),
            Self::Eks { namespace, .. } => namespace.capacity(),
        }
    }

    fn replaces(&self, previous: &Self) -> bool {
        match (self, previous) {
            (Self::Ec2 { instance_id: next }, Self::Ec2 { instance_id: old }) => next != old,
            (
                Self::Eks {
                    namespace,
                    statefulset_uid,
                    pod_uid,
                    pvc_uid,
                    pv_uid,
                },
                Self::Eks {
                    namespace: old_namespace,
                    statefulset_uid: old_set,
                    pod_uid: old_pod,
                    pvc_uid: old_pvc,
                    pv_uid: old_pv,
                },
            ) => {
                namespace == old_namespace
                    && statefulset_uid != old_set
                    && pod_uid != old_pod
                    && pvc_uid == old_pvc
                    && pv_uid == old_pv
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerDrainIntent {
    pub worker: WorkerIdentity,
    pub run_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_container_id: Option<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_target: Option<NativeWorkerTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_growth_claim: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_replacement_drain: Option<Uuid>,
}
impl WorkerDrainIntent {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.worker.validate().is_err()
            || self.run_id.is_nil()
            || self.replacement_growth_claim.is_some_and(|claim| {
                claim.is_nil() || !matches!(self.native_target, Some(NativeWorkerTarget::Eks { .. }))
            })
            || self.retained_replacement_drain.is_some_and(|drain| {
                drain.is_nil()
                    || self.replacement_growth_claim.is_some()
                    || !matches!(self.native_target, Some(NativeWorkerTarget::Eks { .. }))
            })
            || match (&self.docker_container_id, &self.native_target) {
                (Some(container), None) => container.as_bytes() == &[0; 32],
                (None, Some(target)) => !target.valid(),
                _ => true,
            }
        {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "an exact data worker, run and one provider target are required",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }

    pub(super) fn matches_registration(&self, enrollment: &super::DataWorkerIntent) -> bool {
        enrollment.worker() == self.worker
            && enrollment.run_id == self.run_id
            && match enrollment.mount.profile {
                PoolMountProfile::Docker { .. } => {
                    enrollment.native_target.is_none()
                        && self.native_target.is_none()
                        && self.docker_container_id.is_some()
                }
                PoolMountProfile::Ebs => {
                    self.docker_container_id.is_none()
                        && enrollment
                            .native_target
                            .as_ref()
                            .is_some_and(|target| self.native_target.as_ref() == Some(target))
                }
            }
    }

    pub(super) fn heap_bytes(&self) -> usize {
        self.native_target
            .as_ref()
            .map_or(0, NativeWorkerTarget::heap_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum WorkerDrainPhase {
    Draining,
    Removing {
        transition: Uuid,
    },
    QueueRemoved {
        ready: ReadyMembership,
    },
    Closed {
        ready: ReadyMembership,
        mount: Box<DrainMount>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_mounts: Option<Box<[DrainMount; 2]>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedEksPool {
    pub claim_id: Uuid,
    pub pool: EksAddedPool,
}

impl RetainedEksPool {
    pub(super) fn from_published(
        previous: &WorkerDrainRecord,
        published: &EksReplacementRecord,
        registration: &DataWorkerRegistration,
        growth: &GrowthRecord,
        zone: &str,
    ) -> Option<Self> {
        let added = registration.replacement.as_ref()?;
        let permit = published.permit(previous).ok()?;
        let retained = Self { claim_id: permit.add_pool_claim, pool: permit.added };
        let Some(NativeWorkerTarget::Eks { statefulset_uid, pod_uid, pvc_uid, pv_uid, .. }) =
            registration.intent.native_target.as_ref()
        else {
            return None;
        };
        let primary_uids = [statefulset_uid, pod_uid, pvc_uid, pv_uid];
        (published.published
            && published.zone == zone
            && permit.next == registration.intent
            && previous.intent.replacement_growth_claim == Some(retained.claim_id)
            && growth.intent.worker == previous.intent.worker
            && retained.pool.pvc_uid != retained.pool.pv_uid
            && [&retained.pool.pvc_uid, &retained.pool.pv_uid].iter().all(|uid| {
                !uid.is_nil() && !primary_uids.contains(uid)
            })
            && retained.matches_claim(added, growth, zone))
            .then_some(retained)
    }

    pub(super) fn matches_claim(
        &self,
        stamp: &MountedGrowthStamp,
        growth: &GrowthRecord,
        zone: &str,
    ) -> bool {
        let mount = &self.pool.mount;
        matches!(&growth.intent.action, Action::AddPool { worker, zone: growth_zone, .. }
            if *worker == mount.worker.worker_id && growth_zone == zone)
            && growth.phase == GrowthPhase::Complete
            && growth.intent.claim_id == self.claim_id
            && growth.intent.worker.worker_id == mount.worker.worker_id
            && growth.intent.pool_id == mount.identity.pool_id
            && growth.intent.pool_generation == mount.generation
            && growth.provider_resource.as_ref() == Some(&mount.provider_volume_id)
            && growth.mounted_total_bytes == Some(stamp.total_bytes)
            && stamp.total_bytes >= growth.intent.requested_bytes()
            && stamp.worker == mount.worker
            && stamp.pool_id == mount.identity.pool_id
            && stamp.pool_generation == mount.generation
            && stamp.provider_volume_id == mount.provider_volume_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerDrainRecord {
    pub id: Uuid,
    pub intent: WorkerDrainIntent,
    pub growth_claim: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_eks_pool: Option<Box<RetainedEksPool>>,
    pub phase: WorkerDrainPhase,
}

impl WorkerDrainRecord {
    pub(super) fn matches_closure(
        &self,
        primary: &crate::oltp::pool::mount::PoolMountExpectation,
        mount: &DrainMount,
        native_mounts: Option<&[DrainMount; 2]>,
    ) -> bool {
        let observed = |mount: &DrainMount| mount.mount_id != 0 && mount.root_inode != 0;
        if &mount.expected != primary || !observed(mount) {
            return false;
        }
        match (&self.retained_eks_pool, native_mounts) {
            (None, None) => self.intent.retained_replacement_drain.is_none(),
            (Some(retained), Some([first, second])) => {
                self.intent.retained_replacement_drain.is_some()
                    && first.expected.identity.pool_id < second.expected.identity.pool_id
                    && first.mount_id != second.mount_id
                    && (first.device_major, first.device_minor)
                        != (second.device_major, second.device_minor)
                    && ((first == mount && second.expected == retained.pool.mount && observed(second))
                        || (second == mount && first.expected == retained.pool.mount && observed(first)))
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerCloseAttestation {
    pub(super) id: Uuid,
    pub(super) worker: WorkerIdentity,
    pub(super) ready: ReadyMembership,
    pub(super) mount: Box<DrainMount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) native_mounts: Option<Box<[DrainMount; 2]>>,
    proof: RestartMountProof,
}
impl WorkerCloseAttestation {
    pub(crate) fn from_native(
        group: &GroupIdentity,
        record: &WorkerDrainRecord,
        verified: &VerifiedPoolClosed,
        verified_added: Option<&VerifiedPoolClosed>,
        key: &RestartMountKey,
    ) -> Result<Self, ProtocolError> {
        let WorkerDrainPhase::QueueRemoved { ready } = record.phase else {
            return Err(unavailable("worker queue removal is not settled"));
        };
        let worker = record.intent.worker;
        let closed_mount = |verified: &VerifiedPoolClosed| {
            let statement = verified.statement();
            let mount = statement
                .registered
                .clone()
                .ok_or_else(|| unavailable("registered mount closure is absent"))?;
            if statement.close_id != record.id
                || statement.identity != mount.expected.identity
                || mount.expected.worker != worker
            {
                return Err(unavailable("native closure differs from the worker drain"));
            }
            Ok(DrainMount::from(mount))
        };
        let mount = Box::new(closed_mount(verified)?);
        let native_mounts = if let Some(verified) = verified_added {
            let mut mounts = [(*mount).clone(), closed_mount(verified)?];
            mounts.sort_unstable_by_key(|closed| closed.expected.identity.pool_id);
            Some(Box::new(mounts))
        } else {
            None
        };
        if !record.matches_closure(&mount.expected, &mount, native_mounts.as_deref()) {
            return Err(unavailable("native closure set differs from the worker drain"));
        }
        let proof = key.sign_message(message(group, record, ready, &mount, native_mounts.as_deref())?);
        Ok(Self {
            id: record.id,
            worker,
            ready,
            mount,
            native_mounts,
            proof,
        })
    }

    pub(super) fn verify(
        &self,
        group: &GroupIdentity,
        record: &WorkerDrainRecord,
        key: Digest,
    ) -> Result<(), ProtocolError> {
        self.proof.verify_message(
            message(group, record, self.ready, &self.mount, self.native_mounts.as_deref())?,
            key,
        )
    }

    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<DrainMount>()
            + self.mount.heap_bytes()
            + self.native_mounts.as_ref().map_or(0, |mounts| {
                std::mem::size_of::<[DrainMount; 2]>()
                    + mounts.iter().map(DrainMount::heap_bytes).sum::<usize>()
            })
    }
}
fn message(
    group: &GroupIdentity,
    record: &WorkerDrainRecord,
    ready: ReadyMembership,
    mount: &DrainMount,
    native_mounts: Option<&[DrainMount; 2]>,
) -> Result<Digest, ProtocolError> {
    if let Some(mounts) = native_mounts {
        return serialized_digest(&(
            b"nabedra/data-worker-stop/v2".as_slice(),
            group,
            record.id,
            &record.intent,
            record.growth_claim,
            &record.retained_eks_pool,
            ready,
            mount,
            mounts,
        ));
    }
    serialized_digest(&(
        b"nabedra/data-worker-stop/v1".as_slice(),
        group,
        record.id,
        &record.intent,
        record.growth_claim,
        ready,
        mount,
    ))
}

/// Fresh Control reads can issue the same immutable permit for lost provider replies.
/// A native permit carries the original physical zone from the completed claim.
#[derive(Serialize)]
pub(crate) struct WorkerStopPermit {
    record: WorkerDrainRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_zone: Option<String>,
    #[serde(skip)]
    native_mount_key: Option<Digest>,
}

#[derive(Serialize)]
pub(crate) struct WorkerReplacementCheck {
    drain_id: Uuid,
    growth_claim: Uuid,
    previous: WorkerIdentity,
    next: WorkerIdentity,
    retained_pool: Uuid,
    next_mount_generation: u64,
    state: &'static str,
}

impl WorkerStopPermit {
    pub(super) fn from_closed(
        record: WorkerDrainRecord,
        native: Option<(String, Digest)>,
    ) -> Result<Self, ProtocolError> {
        let WorkerDrainPhase::Closed { mount, native_mounts, .. } = &record.phase else {
            return Err(unavailable("worker native closure is absent"));
        };
        if record.intent.native_target.is_some() != native.is_some()
            || !record.matches_closure(&mount.expected, mount, native_mounts.as_deref())
        {
            return Err(unavailable("native provider enrollment is absent"));
        }
        let (native_zone, native_mount_key) = native.unzip();
        Ok(Self {
            record,
            native_zone,
            native_mount_key,
        })
    }

    pub(super) fn check_replacement(
        &self,
        control: &GroupIdentity,
        next: &super::DataWorkerIntent,
    ) -> Result<WorkerReplacementCheck, ProtocolError> {
        next.validate(control)?;
        let WorkerDrainPhase::Closed { mount, .. } = &self.record.phase else {
            return Err(unavailable("worker native closure is absent"));
        };
        let previous = self.record.intent.worker;
        let worker = next.worker();
        let valid_target = next
            .native_target
            .as_ref()
            .zip(self.record.intent.native_target.as_ref())
            .is_some_and(|(next, previous)| next.replaces(previous));
        let old = &mount.expected;
        if self.record.retained_eks_pool.is_some()
            || !valid_target
            || next.claim_id != self.record.growth_claim
            || next.run_id != self.record.intent.run_id
            || worker.worker_id != previous.worker_id
            || worker.incarnation == previous.incarnation
            || worker.certificate_sha256 == previous.certificate_sha256
            || worker.enrollment_revision <= previous.enrollment_revision
            || next.mount.identity != old.identity
            || next.mount.provider_volume_id != old.provider_volume_id
            || next.mount.filesystem_uuid != old.filesystem_uuid
            || next.mount.mount_path != old.mount_path
            || Some(next.mount.generation) != old.generation.checked_add(1)
            || Some(next.mount_public_key) != self.native_mount_key
        {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "replacement differs from the closed generation and original growth claim",
                Outcome::NotApplied,
            ));
        }
        Ok(WorkerReplacementCheck {
            drain_id: self.record.id,
            growth_claim: self.record.growth_claim,
            previous,
            next: worker,
            retained_pool: old.identity.pool_id,
            next_mount_generation: next.mount.generation,
            state: "awaiting_provider_exclusion",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        oltp::pool::{
            PoolIdentity,
            mount::{MountedPoolObservation, PoolMountExpectation},
        },
        oltp_deployment::{
            controller::{GrowthIntent, GrowthPhase, GrowthRecord},
            planner::Action,
        },
        oltp_replication::authority::control::DataWorkerIntent,
    };

    fn enrollment() -> (GroupIdentity, DataWorkerIntent, RestartMountKey) {
        let group = GroupIdentity {
            cluster_id: Uuid::from_u128(1),
            database: Text::try_from("fixture").unwrap(),
            group_id: Uuid::from_u128(2),
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        };
        let enrollment = WorkerEnrollment {
            worker_id: Uuid::from_u128(3),
            incarnation: Uuid::from_u128(4),
            endpoint: Text::try_from("127.0.0.1:9000").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            certificate_sha256: Digest::from_bytes([4; 32]),
            enrollment_revision: 1,
        };
        let mount = PoolMountExpectation {
            profile: PoolMountProfile::Ebs,
            identity: PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: 1,
                cluster_id: group.cluster_id,
                worker_id: enrollment.worker_id,
                pool_id: Uuid::from_u128(5),
            },
            worker: WorkerIdentity::from(&enrollment),
            generation: 1,
            mount_path: "/data".into(),
            provider_volume_id: "vol-0123456789abcdef0".into(),
            filesystem_uuid: Uuid::from_u128(6).to_string(),
        };
        let key = RestartMountKey::from_seed(&[7; 32]).unwrap();
        let intent = DataWorkerIntent {
            claim_id: Uuid::from_u128(7),
            run_id: Uuid::from_u128(8),
            enrollment,
            mount,
            mount_public_key: key.public_key(),
            native_target: Some(NativeWorkerTarget::Eks {
                namespace: "fixture".into(),
                statefulset_uid: Uuid::from_u128(9),
                pod_uid: Uuid::from_u128(10),
                pvc_uid: Uuid::from_u128(11),
                pv_uid: Uuid::from_u128(12),
            }),
            eks_replacement_key: None,
        };
        (group, intent, key)
    }

    fn observed(intent: &DataWorkerIntent) -> MountedPoolObservation {
        MountedPoolObservation {
            expected: intent.mount.clone(),
            mount_id: 7,
            device_major: 259,
            device_minor: 3,
            root_inode: 1,
            total_bytes: 100,
            available_bytes: 90,
        }
    }

    #[test]
    fn o18_a02_native_target_is_covered_by_the_mount_enrollment_signature() {
        let (group, enrolled, key) = enrollment();
        let claim = GrowthRecord {
            intent: GrowthIntent {
                policy_id: Uuid::from_u128(13),
                claim_id: enrolled.claim_id,
                action: Action::AddWorker {
                    zone: "zone-a".into(),
                    bytes: 100,
                },
                worker: enrolled.worker(),
                pool_id: enrolled.mount.identity.pool_id,
                pool_generation: enrolled.mount.generation,
                quoted_monthly_cost: 1,
                monthly_spend_ceiling: 100,
                max_attempts: 2,
                cooldown_seconds: 1,
            },
            phase: GrowthPhase::AwaitingMount,
            attempts: 1,
            last_attempt: Some(1),
            provider_resource: Some(enrolled.mount.provider_volume_id.clone()),
            mounted_total_bytes: None,
            prior_total_bytes: None,
        };
        let (stamp, signature) = enrolled.prove(&group, &observed(&enrolled), &key).unwrap();
        enrolled.verify(&group, &stamp, &signature, &claim).unwrap();
        let mut request = WorkerDrainIntent {
            replacement_growth_claim: None,
            retained_replacement_drain: None,
            worker: enrolled.worker(),
            run_id: enrolled.run_id,
            native_target: enrolled.native_target.clone(),
            docker_container_id: None,
        };
        request.validate().unwrap();
        assert!(request.matches_registration(&enrolled));
        let Some(NativeWorkerTarget::Eks { pod_uid, .. }) = request.native_target.as_mut() else {
            unreachable!()
        };
        *pod_uid = Uuid::from_u128(16);
        assert!(!request.matches_registration(&enrolled));
        let mut altered = enrolled.clone();
        altered.native_target = request.native_target.clone();
        assert!(altered.verify(&group, &stamp, &signature, &claim).is_err());
        altered.native_target = None;
        assert!(!request.matches_registration(&altered));
        request.docker_container_id = Some(Digest::from_bytes([1; 32]));
        assert!(request.validate().is_err());
    }

    #[test]
    fn o18_a02_docker_drain_keeps_the_original_wire_shape() {
        let (_, mut enrolled, _) = enrollment();
        enrolled.native_target = None;
        enrolled.mount.profile = PoolMountProfile::Docker {
            volume_marker: enrolled.mount.identity.pool_id,
        };
        let old = serde_json::json!({
            "worker": enrolled.worker(), "run_id": enrolled.run_id,
            "docker_container_id": Digest::from_bytes([6; 32]),
        });
        let intent: WorkerDrainIntent = serde_json::from_value(old.clone()).unwrap();
        assert!(intent.matches_registration(&enrolled));
        assert_eq!(serde_json::to_value(intent).unwrap(), old);
    }

    #[test]
    fn o17_o18_o26_o27_a02_r03_stop_proof_binds_sorted_complete_mount_closures() {
        let (group, enrolled, key) = enrollment();
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: enrolled.run_id,
                owner_term: 1,
                membership_revision: 5,
            },
            ready_root: Digest::from_bytes([8; 32]),
            ready_count: 3,
        };
        let primary = DrainMount::from(observed(&enrolled));
        let mut added = primary.clone();
        added.expected.identity.pool_id = Uuid::from_u128(2);
        added.expected.generation = 3;
        added.expected.mount_path = "/secondary".into();
        added.expected.filesystem_uuid = Uuid::from_u128(18).to_string();
        added.expected.provider_volume_id = "vol-0123456789abcdef1".into();
        added.mount_id += 1;
        added.device_minor += 1;
        let mut record = WorkerDrainRecord {
            id: Uuid::from_u128(14),
            intent: WorkerDrainIntent {
                worker: enrolled.worker(),
                run_id: enrolled.run_id,
                docker_container_id: None,
                native_target: enrolled.native_target.clone(),
                replacement_growth_claim: None,
                retained_replacement_drain: None,
            },
            growth_claim: enrolled.claim_id,
            retained_eks_pool: None,
            phase: WorkerDrainPhase::QueueRemoved { ready },
        };
        let legacy = serialized_digest(&(
            b"nabedra/data-worker-stop/v1".as_slice(),
            &group, record.id, &record.intent, record.growth_claim, ready, &primary,
        )).unwrap();
        assert_eq!(legacy, message(&group, &record, ready, &primary, None).unwrap());
        record.intent.retained_replacement_drain = Some(Uuid::from_u128(19));
        record.retained_eks_pool = Some(Box::new(RetainedEksPool {
            claim_id: Uuid::from_u128(20),
            pool: EksAddedPool {
                mount: added.expected.clone(),
                pvc_uid: Uuid::from_u128(21),
                pv_uid: Uuid::from_u128(22),
            },
        }));
        let sorted = [added.clone(), primary.clone()];
        assert!(record.matches_closure(&enrolled.mount, &primary, Some(&sorted)));
        assert!(!record.matches_closure(&enrolled.mount, &primary, None));
        assert!(!record.matches_closure(
            &enrolled.mount, &primary, Some(&[primary.clone(), added.clone()])
        ));
        let signature = key.sign_message(message(&group, &record, ready, &primary, Some(&sorted)).unwrap());
        let mut proof = WorkerCloseAttestation {
            id: record.id,
            worker: enrolled.worker(),
            ready,
            mount: Box::new(primary.clone()),
            native_mounts: Some(Box::new(sorted)),
            proof: signature,
        };
        proof.verify(&group, &record, key.public_key()).unwrap();
        let mounts = proof.native_mounts.as_mut().unwrap();
        mounts[0].expected.generation += 1;
        assert!(!record.matches_closure(&enrolled.mount, &primary, Some(mounts.as_ref())));
        assert!(proof.verify(&group, &record, key.public_key()).is_err());
        added.device_minor = primary.device_minor;
        assert!(!record.matches_closure(&enrolled.mount, &primary, Some(&[added, primary.clone()])));
    }

    #[test]
    fn o27_r03_replacement_check_retains_original_claim_and_mount_generation_fence() {
        let (group, old, key) = enrollment();
        let intent = WorkerDrainIntent {
            replacement_growth_claim: None,
            retained_replacement_drain: None,
            worker: old.worker(),
            run_id: old.run_id,
            native_target: old.native_target.clone(),
            docker_container_id: None,
        };
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: old.run_id,
                owner_term: 1,
                membership_revision: 5,
            },
            ready_root: Digest::from_bytes([8; 32]),
            ready_count: 3,
        };
        let mut record = WorkerDrainRecord {
            id: Uuid::from_u128(14),
            intent,
            growth_claim: old.claim_id,
            retained_eks_pool: None,
            phase: WorkerDrainPhase::QueueRemoved { ready },
        };
        let witness = Some(("zone-a".to_string(), key.public_key()));
        assert!(WorkerStopPermit::from_closed(record.clone(), witness.clone()).is_err());
        record.phase = WorkerDrainPhase::Closed {
            ready,
            mount: Box::new(observed(&old).into()),
            native_mounts: None,
        };
        let permit = WorkerStopPermit::from_closed(record.clone(), witness).unwrap();
        let mut next = old.clone();
        next.enrollment.incarnation = Uuid::from_u128(15);
        next.enrollment.enrollment_revision += 1;
        next.enrollment.certificate_sha256 = Digest::from_bytes([9; 32]);
        next.mount.worker = next.worker();
        next.mount.generation += 1;
        assert!(permit.check_replacement(&group, &next).is_err());
        let Some(NativeWorkerTarget::Eks {
            statefulset_uid,
            pod_uid,
            ..
        }) = next.native_target.as_mut()
        else {
            unreachable!()
        };
        *statefulset_uid = Uuid::from_u128(16);
        *pod_uid = Uuid::from_u128(17);
        let check = permit.check_replacement(&group, &next).unwrap();
        assert_eq!(check.state, "awaiting_provider_exclusion");
        assert_eq!(check.growth_claim, old.claim_id);
        assert_eq!(check.next_mount_generation, old.mount.generation + 1);
        let mut changed_claim = next.clone();
        changed_claim.claim_id = Uuid::from_u128(18);
        assert!(permit.check_replacement(&group, &changed_claim).is_err());
        let Some(NativeWorkerTarget::Eks { pvc_uid, .. }) = next.native_target.as_mut() else {
            unreachable!()
        };
        *pvc_uid = Uuid::from_u128(19);
        assert!(permit.check_replacement(&group, &next).is_err());
        assert_eq!(permit.record, record);
    }

    #[test]
    fn o18_o27_a02_r03_eks_statements_bind_the_original_claim_token_and_both_mounts() {
        let (group, mut old, mount_key) = enrollment();
        let verifier = RestartMountKey::from_seed(&[22; 32]).unwrap();
        old.eks_replacement_key = Some(verifier.public_key());
        let add_pool_id = Uuid::from_u128(23);
        let original = GrowthRecord {
            intent: GrowthIntent {
                policy_id: Uuid::from_u128(13),
                claim_id: old.claim_id,
                action: Action::AddWorker { zone: "zone-a".into(), bytes: 100 },
                worker: old.worker(), pool_id: old.mount.identity.pool_id,
                pool_generation: old.mount.generation, quoted_monthly_cost: 1,
                monthly_spend_ceiling: 100, max_attempts: 2, cooldown_seconds: 1,
            },
            phase: GrowthPhase::Complete, attempts: 1, last_attempt: Some(1),
            provider_resource: Some(old.mount.provider_volume_id.clone()),
            mounted_total_bytes: Some(100), prior_total_bytes: None,
        };
        let mut growth = original.clone();
        growth.intent.claim_id = Uuid::from_u128(24);
        growth.intent.pool_id = add_pool_id;
        growth.intent.action = Action::AddPool { worker: old.worker().worker_id, zone: "zone-a".into(), bytes: 100 };
        growth.provider_resource = Some("vol-0123456789abcdef1".into());
        growth.phase = GrowthPhase::AwaitingMount;
        growth.mounted_total_bytes = None;
        let drain = WorkerDrainRecord {
            id: Uuid::from_u128(25), growth_claim: old.claim_id,
            intent: WorkerDrainIntent { worker: old.worker(), run_id: old.run_id,
                native_target: old.native_target.clone(), docker_container_id: None,
                replacement_growth_claim: Some(growth.intent.claim_id),
                retained_replacement_drain: None,
            },
            retained_eks_pool: None,
            phase: WorkerDrainPhase::Closed {
                ready: ReadyMembership {
                    view: AuthorityView { run_id: old.run_id, owner_term: 1, membership_revision: 5 },
                    ready_root: Digest::from_bytes([8; 32]), ready_count: 3,
                },
                mount: Box::new(observed(&old).into()),
                native_mounts: None,
            },
        };
        let exclusion = EksExclusion {
            drain_id: drain.id, launch_digest: Digest::from_bytes([9; 32]),
            attachment_revision: "891".into(), describe_volumes_request_id: Uuid::from_u128(26),
        };
        let operation = Uuid::from_u128(27);
        let statement = eks_statement(&group, operation, &drain, &old, &original, &growth,
            &EksReplacementAction::Exclude(exclusion.clone()), None).unwrap();
        let signature = verifier.sign_message(statement);
        signature.verify_message(statement, verifier.public_key()).unwrap();
        assert!(signature.verify_message(statement, mount_key.public_key()).is_err());
        let mut record = EksReplacementRecord {
            drain_id: drain.id, add_pool_claim: growth.intent.claim_id, exclusion_operation: operation,
            exclusion, exclusion_signature: signature, authorization: None, published: false, zone: "zone-a".into(),
        };

        let mut next = old.clone();
        next.enrollment.incarnation = Uuid::from_u128(28);
        next.enrollment.enrollment_revision += 1;
        next.enrollment.certificate_sha256 = Digest::from_bytes([10; 32]);
        next.mount.worker = next.worker();
        next.mount.generation += 1;
        let Some(NativeWorkerTarget::Eks { statefulset_uid, pod_uid, .. }) = next.native_target.as_mut() else { unreachable!(); };
        *statefulset_uid = Uuid::from_u128(29);
        *pod_uid = Uuid::from_u128(30);
        let mut added = next.mount.clone();
        added.generation = growth.intent.pool_generation;
        added.identity.pool_id = add_pool_id;
        added.filesystem_uuid = Uuid::from_u128(31).to_string();
        added.provider_volume_id = growth.provider_resource.clone().unwrap();
        added.mount_path = "/added".into();
        let candidate = Box::new(EksCandidate {
            drain_id: drain.id, launch_digest: record.exclusion.launch_digest,
            exclusion_operation: operation, next,
            added: EksAddedPool { mount: added, pvc_uid: Uuid::from_u128(32), pv_uid: Uuid::from_u128(33) },
            pod_revision: "892".into(), service_account_uid: Uuid::from_u128(34), node_uid: Uuid::from_u128(35),
            projected_token_sha256: Digest::from_bytes([11; 32]), projected_token_expires_at: 600,
        });
        let authorize_operation = Uuid::from_u128(36);
        let check = |candidate: Box<EksCandidate>, prior: Option<&EksReplacementRecord>| {
            eks_statement(&group, authorize_operation, &drain, &old, &original, &growth,
                &EksReplacementAction::Authorize(candidate), prior)
        };
        assert!(check(candidate.clone(), None).is_err());
        let statement = check(candidate.clone(), Some(&record)).unwrap();
        let signature = verifier.sign_message(statement);
        let mut different = candidate.clone();
        different.projected_token_sha256 = Digest::from_bytes([12; 32]);
        let changed = check(different, Some(&record)).unwrap();
        assert!(signature.verify_message(changed, verifier.public_key()).is_err());
        record.authorization = Some(EksAuthorization { operation: authorize_operation, candidate, signature });
        assert!(check(record.authorization.as_ref().unwrap().candidate.clone(), Some(&record)).is_err());
        let permit = record.permit(&drain).unwrap();
        let primary = observed(&permit.next);
        let secondary = MountedPoolObservation { expected: permit.added.mount.clone(), device_minor: 4, ..primary.clone() };
        let mut mounted = EksMountedReplacement::from_native(&group, &permit, &primary, &secondary, &mount_key).unwrap();
        mounted.verify(&group, &permit, &original, &growth).unwrap();
        let mut registration = DataWorkerRegistration {
            intent: permit.next.clone(),
            mount: mounted.primary.clone(),
            replacement: Some(mounted.added.clone()),
        };
        let mut complete = growth.clone();
        complete.phase = GrowthPhase::Complete;
        complete.mounted_total_bytes = Some(mounted.added.total_bytes);
        assert!(RetainedEksPool::from_published(&drain, &record, &registration, &complete, "zone-a").is_none());
        record.published = true;
        let retained = RetainedEksPool::from_published(&drain, &record, &registration, &complete, "zone-a").unwrap();
        assert_eq!(retained.pool, permit.added);
        assert!(RetainedEksPool::from_published(&drain, &record, &registration, &growth, "zone-a").is_none());
        registration.intent.enrollment.enrollment_revision += 1;
        assert!(RetainedEksPool::from_published(&drain, &record, &registration, &complete, "zone-a").is_none());
        mounted.added.total_bytes += 1;
        assert!(mounted.verify(&group, &permit, &original, &growth).is_err());
        let alias = MountedPoolObservation { device_minor: primary.device_minor, ..secondary };
        assert!(EksMountedReplacement::from_native(&group, &permit, &primary, &alias, &mount_key).is_err());
        assert_eq!(growth.intent.worker, old.worker());
    }
}
