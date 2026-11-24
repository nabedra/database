//! Independent review tests for ACC3-delegation and CAP1 (PR #121 review),
//! written from 6ad8a99c, 2920a442, 9be40820, 414dcf79 and their records.
use super::*;
use crate::oltp::mvcc::store::{ParticipantCommand, ParticipantReply, index_reclaim::ReclaimIndex};
use crate::oltp_replication::authority::control::INDEX_RECLAIM_FEATURE;
use crate::protocol::ErrorCode;

/// What a starting worker treats as transient (bootstrap_transient).
fn startup_retries(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}

/// ACC3-delegation, end to end over mTLS: the other Control voters still
/// authenticate a Control voter as prepared-only. Its ReadIndex and its
/// proposals must wait for their promotion and then succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r121_acc3_delegation_heals_when_the_promotion_applies() {
    timeout(Duration::from_secs(120), async {
        let cluster = Cluster::new(1, &[]).await;
        // A caller that does not lead Control, so both operations forward.
        let caller = timeout(Duration::from_secs(30), async {
            loop {
                for index in CONTROL {
                    let raw = &cluster.running(index).control.as_ref().unwrap().0;
                    let node = raw.state.resident().node_id();
                    let leader = raw.raft.current_leader().await;
                    if leader.is_some_and(|leader| leader != node) {
                        return index;
                    }
                }
                nabedra_rt::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let (_, handle, permit) = cluster.running(caller).control.as_ref().unwrap();
        let (handle, permit) = (handle.clone(), permit.clone());
        Cluster::retry("warm read", || handle.inspect()).await;
        let voters: Vec<usize> = CONTROL
            .into_iter()
            .filter(|index| *index != caller)
            .collect();
        let peer = cluster.enrollments[caller].worker_id;
        let lag = |lagging: bool| {
            for voter in &voters {
                let mut set = cluster.directories[*voter].prepared.lock().unwrap();
                if lagging {
                    set.insert(peer);
                } else {
                    set.remove(&peer);
                }
            }
        };

        lag(true);
        let reading = {
            let handle = handle.clone();
            nabedra_rt::task::spawn(async move {
                let started = std::time::Instant::now();
                (handle.inspect().await.map(|_| ()), started.elapsed())
            })
        };
        nabedra_rt::time::sleep(Duration::from_millis(800)).await;
        lag(false);
        let (read, elapsed) = reading.await.unwrap();
        eprintln!("r121 ReadIndex: {read:?} after {elapsed:?}");

        lag(true);
        let (operation, generation) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
        let fresh = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            ..cluster.group.clone()
        };
        let proposing = nabedra_rt::task::spawn(async move {
            let mut refusals = 0u32;
            loop {
                match handle
                    .begin_group_local(&permit, operation, generation, fresh.clone())
                    .await
                {
                    Ok(applied) => return (Ok(applied.reply), refusals),
                    Err(error) if startup_retries(&error) && refusals < 200 => refusals += 1,
                    Err(error) => return (Err(error), refusals),
                }
                nabedra_rt::time::sleep(Duration::from_millis(100)).await;
            }
        });
        nabedra_rt::time::sleep(Duration::from_millis(800)).await;
        lag(false);
        let (proposed, refusals) = proposing.await.unwrap();
        eprintln!(
            "r121 proposal: {:?} after {refusals} refusals",
            proposed.as_ref().map(|_| ())
        );
        assert!(
            read.is_ok() && proposed.is_ok(),
            "a retained worker stops for good at a voter that has not applied its promotion: read {read:?}, proposal {:?}",
            proposed.as_ref().map(|_| ())
        );
        assert!(
            elapsed >= Duration::from_millis(500) && refusals > 0,
            "the lag was in the way of both operations: {elapsed:?}, {refusals}"
        );
        cluster.shutdown().await;
    })
    .await
    .expect("r121 delegation watchdog");
}

async fn adverts(cluster: &Cluster, members: &[ReplicaMember]) -> Vec<(Uuid, bool, bool)> {
    let (handle, _) = cluster.handle();
    let mut result = Vec::new();
    for member in members {
        let index = cluster
            .enrollments
            .iter()
            .position(|worker| worker.worker_id == member.worker_id)
            .unwrap();
        let state = handle
            .cluster_feature(cluster.identity(index), INDEX_RECLAIM_FEATURE)
            .await
            .unwrap();
        result.push((member.worker_id, state.advertised, state.enabled.is_some()));
    }
    result
}

/// CAP1: no worker advertised `data.index.reclaim` and Control enabled
/// nothing. A ReclaimIndex entry must not reach a Data log. If it does, a
/// worker that never advertised the feature can still join the group later
/// and has to apply the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r121_cap1_no_reclaim_entry_where_a_member_lacks_the_advert() {
    timeout(Duration::from_secs(300), async {
        let mut cluster = Cluster::new(5, &[3]).await;
        let acknowledged = cluster.term;
        let leader = cluster.data_leader().await;
        let host = cluster.data_host(leader).unwrap();
        let GroupKind::Data { table_id, .. } = cluster.group.kind else {
            unreachable!()
        };
        let reclaim = |cluster: &Cluster| {
            crate::oltp_replication::owned::OwnedCommand::new_owned(
                ParticipantCommand::ReclaimIndex(ReclaimIndex {
                    authority: cluster.authority(cluster.term),
                    catalog_group_id: nabedra_rt::ids::uuid_v4(),
                    table_id,
                    index_id: nabedra_rt::ids::uuid_v4(),
                    retired_at: CommitTs::new(1).unwrap(),
                    safe_ts: CommitTs::new(2).unwrap(),
                    routing_version: None,
                }),
                &cluster.workers[leader].shared,
                crate::oltp::pool::ByteClass::Progress,
            )
            .unwrap()
        };
        let mut committed = None;
        let mut last = None;
        for _ in 0..50 {
            match host.propose(reclaim(&cluster)).await {
                Ok(applied) => {
                    committed = Some(applied.position);
                    break;
                }
                Err(error) => last = Some(error),
            }
            nabedra_rt::time::sleep(Duration::from_millis(100)).await;
        }
        let Some(position) = committed else {
            let error = last.unwrap();
            assert_eq!(
                (error.code, error.outcome),
                (ErrorCode::StorageUnavailable, Outcome::NotApplied),
                "{error}"
            );
            // The refusal is the gate's: the same core proposes once its
            // worker observed the feature.
            cluster
                .running(leader)
                .manager
                .cluster_features()
                .enable(INDEX_RECLAIM_FEATURE);
            let applied = host.propose(reclaim(&cluster)).await.unwrap();
            assert!(
                matches!(applied.reply, ParticipantReply::Rejected(_)),
                "{:?}",
                applied.reply
            );
            cluster.shutdown().await;
            return;
        };
        let (_, members) = cluster.placement().await;
        let before = adverts(&cluster, &members).await;
        drop(host);
        // The second half of the record: a worker joins after the entry.
        damage_at_run_time(&mut cluster, RunTime::RunBlock).await;
        let record = cluster.replaced(Duration::from_secs(120)).await;
        let joiner = cluster.check_replaced(&record, acknowledged).await;
        let joined = adverts(&cluster, std::slice::from_ref(&joiner)).await;
        panic!(
            "ReclaimIndex committed at {position:?}; members (worker, advertised, enabled) {before:?}; \
             joiner after the entry {joined:?}"
        );
    })
    .await
    .expect("r121 CAP1 watchdog");
}
