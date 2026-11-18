use super::*;
use crate::{
    oltp::pool::PoolIdentity, oltp_deployment::controller::GrowthIntent,
    oltp_replication::types::Digest,
};

#[test]
fn o23_r03_retained_secondary_requires_its_completed_claim_and_fresh_mount() {
    let previous = WorkerIdentity {
        worker_id: Uuid::new_v4(),
        incarnation: Uuid::new_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::from_bytes([1; 32]),
    };
    let worker = WorkerIdentity {
        incarnation: Uuid::new_v4(),
        enrollment_revision: 2,
        certificate_sha256: Digest::from_bytes([2; 32]),
        ..previous
    };
    let pool_id = Uuid::new_v4();
    let mount = MountedPoolObservation {
        expected: PoolMountExpectation {
            profile: PoolMountProfile::Docker {
                volume_marker: Uuid::new_v4(),
            },
            identity: PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: 1,
                cluster_id: Uuid::new_v4(),
                worker_id: worker.worker_id,
                pool_id,
            },
            worker,
            generation: 2,
            mount_path: "/retained-secondary".into(),
            provider_volume_id: "docker-volume:retained-secondary".into(),
            filesystem_uuid: String::new(),
        },
        mount_id: 10,
        device_major: 1,
        device_minor: 2,
        root_inode: 3,
        total_bytes: 4 << 30,
        available_bytes: 2 << 30,
    };
    let mut claim = GrowthRecord {
        intent: GrowthIntent {
            policy_id: Uuid::new_v4(),
            claim_id: Uuid::new_v4(),
            action: Action::AddPool {
                worker: previous.worker_id,
                zone: "zone".into(),
                bytes: 1 << 30,
            },
            worker: previous,
            pool_id,
            pool_generation: 1,
            quoted_monthly_cost: 4,
            monthly_spend_ceiling: 8,
            max_attempts: 1,
            cooldown_seconds: 60,
        },
        phase: GrowthPhase::Complete,
        attempts: 1,
        last_attempt: Some(1),
        provider_resource: Some(mount.expected.provider_volume_id.clone()),
        mounted_total_bytes: Some(mount.total_bytes),
        prior_total_bytes: None,
    };
    assert!(completion_matches(Some(&claim), &mount, true));
    assert!(!completion_matches(None, &mount, true));
    assert!(!completion_matches(Some(&claim), &mount, false));
    claim.intent.pool_generation = mount.expected.generation;
    assert!(!completion_matches(Some(&claim), &mount, true));
    claim.intent.pool_generation = 1;
    claim.phase = GrowthPhase::AwaitingMount;
    assert!(!completion_matches(Some(&claim), &mount, true));
    claim.phase = GrowthPhase::Complete;
    claim.provider_resource = Some("docker-volume:different".into());
    assert!(!completion_matches(Some(&claim), &mount, true));
}
