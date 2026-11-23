//! CAP1: `ReclaimIndex` (IDXGC1) on a real RF3 Data group over mTLS
//! sessions, gated by Control's `data.index.reclaim` cluster feature. The
//! sessions negotiate `nabedra-otp/4` (this build no longer offers otp/3), so
//! the entry also shows that no per-connection capability is needed to
//! replicate it.
use super::*;
use crate::oltp::{mvcc::store::index_reclaim::ReclaimIndex, pool::ByteClass};
use crate::oltp_replication::{
    authority::control::{EnabledFeatures, INDEX_RECLAIM_FEATURE},
    owned::OwnedCommand,
};

fn reclaim(cluster: &Cluster, voter: usize) -> OwnedCommand<ParticipantCommand> {
    OwnedCommand::new_owned(
        ParticipantCommand::ReclaimIndex(ReclaimIndex {
            authority: cluster.authority,
            catalog_group_id: nabedra_rt::ids::uuid_v4(),
            table_id: cluster.table,
            index_id: cluster.index,
            retired_at: CommitTs::new(1).unwrap(),
            safe_ts: CommitTs::new(2).unwrap(),
            routing_version: None,
        }),
        &cluster.voters[voter].shared,
        ByteClass::Progress,
    )
    .unwrap()
}

/// Before Control enabled the feature a leader proposes no `ReclaimIndex`:
/// with no feature view (a core not started by the hosted-group manager)
/// and with a view in which the feature is not enabled, the proposal is
/// refused typed as not applied and the log does not grow. Once the
/// worker observed the enablement, the same proposal commits as one entry,
/// and every voter applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cap1_o02_o12_reclaim_index_is_proposed_only_once_the_feature_is_enabled() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let before = cluster.last_index(leader);
    let refused = |error: ProtocolError| {
        assert_eq!(
            (error.code, error.outcome),
            (ErrorCode::StorageUnavailable, Outcome::NotApplied),
            "{}",
            error.message
        );
    };
    let consensus = &cluster.voters[leader].consensus;
    refused(
        consensus
            .propose(reclaim(&cluster, leader))
            .await
            .unwrap_err(),
    );
    let features = Arc::new(EnabledFeatures::default());
    for voter in &cluster.voters {
        assert!(
            voter
                .consensus
                .cluster_features
                .set(features.clone())
                .is_ok()
        );
    }
    refused(
        consensus
            .propose(reclaim(&cluster, leader))
            .await
            .unwrap_err(),
    );
    assert_eq!(cluster.last_index(leader), before, "nothing proposed");

    features.enable(INDEX_RECLAIM_FEATURE);
    let applied = consensus.propose(reclaim(&cluster, leader)).await.unwrap();
    // The catalog group differs from the range's, so the apply refuses it
    // without writing: the gate, not the cleanup, is under test here.
    assert!(
        matches!(applied.reply, ParticipantReply::Rejected(_)),
        "{:?}",
        applied.reply
    );
    let index = cluster.last_index(leader);
    assert_eq!(index, before + 1, "one ReclaimIndex entry");
    watchdog(|| async {
        cluster
            .voters
            .iter()
            .all(|voter| voter.consensus.applied_index() >= Some(index))
            .then_some(())
    })
    .await;
    cluster.shutdown().await;
}
