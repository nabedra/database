use super::*;
use crate::{
    oltp::{
        mvcc::store::direct::{DirectMemberReply, DirectReply},
        pool::ByteClass,
    },
    oltp_replication::owned::OwnedCommand,
};
use std::{pin::Pin, sync::OnceLock, task::Poll};
use tokio::{
    sync::Notify,
    time::{Instant, advance, pause, resume},
};

struct HoldLone {
    inner: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    shared: Arc<WorkerSharedResources>,
    release: Notify,
    replay: Option<(Uuid, Arc<OnceLock<Instant>>)>,
}

impl Consensus<ParticipantCommand, ParticipantReply> for HoldLone {
    fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ConsensusFuture<'_, Applied<ParticipantReply>> {
        Box::pin(async move {
            match command.payload()? {
                ParticipantCommand::Direct(_) => {
                    self.release.notified().await;
                    self.inner.propose(command).await
                }
                ParticipantCommand::DirectBatch(batch) => {
                    let (id, deadline) = self.replay.as_ref().expect("expected a lone proposal");
                    assert_eq!(batch.members.len(), 2);
                    assert_eq!(batch.members[0].request_id, *id);

                    // Apply the earlier proposal after the retry read its key
                    // and queued, but before the retry's batch applies.
                    let original = OwnedCommand::new_owned(
                        ParticipantCommand::Direct(batch.members[0].clone()),
                        &self.shared,
                        ByteClass::Data,
                    )?;
                    let original = self.inner.propose(original).await?;
                    assert!(matches!(
                        original.reply,
                        ParticipantReply::Direct(DirectReply::Applied { .. })
                    ));
                    let applied = self.inner.propose(command).await?;
                    assert!(matches!(
                        &applied.reply,
                        ParticipantReply::Direct(DirectReply::Batch(outcomes))
                            if matches!(outcomes.as_slice(), [
                                DirectMemberReply::Replayed,
                                DirectMemberReply::Applied { .. },
                            ])
                    ));
                    pause_near(*deadline.get().unwrap()).await;
                    Ok(applied)
                }
                unexpected => panic!("unexpected proposal: {unexpected:?}"),
            }
        })
    }

    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.inner.ensure_linearizable()
    }

    fn lease_read_index(&self) -> ConsensusFuture<'_, u64> {
        self.inner.lease_read_index()
    }

    fn current_leader(&self) -> Option<u64> {
        self.inner.current_leader()
    }

    fn local_node(&self) -> Option<u64> {
        self.inner.local_node()
    }
}

fn held_handle(
    voter: &Voter,
    replay: Option<(Uuid, Arc<OnceLock<Instant>>)>,
) -> (Arc<ParticipantHandle>, Arc<HoldLone>) {
    let consensus = Arc::new(HoldLone {
        inner: voter.consensus.clone(),
        shared: voter.shared.clone(),
        release: Notify::new(),
        replay,
    });
    let handle = Arc::new(ParticipantHandle::new(
        ParticipantMachine::new(voter.store.clone()).unwrap(),
        consensus.clone(),
    ));
    (handle, consensus)
}

fn insert(cluster: &Cluster, request_id: Uuid, key: i64) -> Vec<u8> {
    cluster.body(
        request_id,
        "table.insert",
        serde_json::json!({"row": {"id": key, "value": key, "tag": "fixa", "score": 0.0}}),
    )
}

fn distinct_keys(cluster: &Cluster, count: usize) -> Vec<i64> {
    use crate::oltp_runtime::direct::latches::{DEFAULT_STRIPES, KeyLatches};
    use std::collections::HashSet;

    let latches = KeyLatches::new(DEFAULT_STRIPES);
    let mut used = HashSet::new();
    (1..)
        .filter(|key| {
            let encoded = serde_json::to_vec(&cluster.row_key(*key)).unwrap();
            used.insert(latches.stripes_of([encoded.as_slice()])[0])
        })
        .take(count)
        .collect()
}

async fn at_proposal(
    voter: &Voter,
    mut request: Pin<&mut impl Future<Output = DirectResponse>>,
    count: u64,
) {
    nabedra_rt::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            if voter.executor.stats().entries_proposed == count {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("request did not reach the batcher");
}

async fn pause_near(deadline: Instant) {
    pause();
    let near = deadline - Duration::from_millis(1);
    assert!(
        nabedra_rt::time::now() <= near,
        "test reached the deadline too early"
    );
    advance(near - nabedra_rt::time::now()).await;
}

fn timed_insert(cluster: &Cluster, leader: usize, key: i64) -> DirectRequest {
    let id = nabedra_rt::ids::uuid_v4();
    cluster.request(leader, &insert(cluster, id, key), id)
}

#[tokio::test]
async fn d02_fixa_batch_replay_near_deadline_preserves_the_committed_outcome() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Default::default()
    })
    .await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let keys = distinct_keys(&cluster, 3);
    let id = nabedra_rt::ids::uuid_v4();
    let body = insert(&cluster, id, keys[1]);
    let deadline = Arc::new(OnceLock::new());
    let (handle, consensus) = held_handle(voter, Some((id, deadline.clone())));
    let mut lone = Box::pin(
        voter
            .executor
            .execute(&handle, timed_insert(&cluster, leader, keys[0])),
    );
    at_proposal(voter, lone.as_mut(), 1).await;

    let request = cluster.request(leader, &body, id);
    pause();
    deadline.set(nabedra_rt::time::now() + request.deadline).unwrap();
    let mut retry = Box::pin(voter.executor.execute(&handle, request));
    assert!(futures_util::poll!(retry.as_mut()).is_pending());
    resume();
    at_proposal(voter, retry.as_mut(), 2).await;
    let mut peer = Box::pin(
        voter
            .executor
            .execute(&handle, timed_insert(&cluster, leader, keys[2])),
    );
    at_proposal(voter, peer.as_mut(), 3).await;

    consensus.release.notify_one();
    assert_eq!(lone.await, done("null"));
    let (response, peer) = tokio::join!(retry, peer);
    resume();
    assert_eq!(peer, done("null"));
    let version = cluster
        .latest(leader, cluster.row_key(keys[1]))
        .await
        .0
        .unwrap();
    assert_eq!(
        voter
            .executor
            .execute(&voter.handle, cluster.request(leader, &body, id))
            .await,
        done("null")
    );
    assert_eq!(
        cluster.latest(leader, cluster.row_key(keys[1])).await.0,
        Some(version)
    );
    assert_eq!(cluster.keys_with(leader, b'D').await, 3);
    cluster.shutdown().await;
    assert!(
        response == done("null")
            || code(&response).is_some_and(|(_, outcome)| outcome == Outcome::Unknown),
        "an applied batch replay must return its result or an unknown outcome: {response:?}"
    );
}

#[tokio::test]
async fn d04_fixa_shutdown_refuses_a_batch_that_never_reached_raft() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Default::default()
    })
    .await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let keys = distinct_keys(&cluster, 2);
    let (handle, _) = held_handle(voter, None);
    let before = cluster.last_index(leader);
    let mut lone = Box::pin(
        voter
            .executor
            .execute(&handle, timed_insert(&cluster, leader, keys[0])),
    );
    at_proposal(voter, lone.as_mut(), 1).await;

    let request = timed_insert(&cluster, leader, keys[1]);
    pause();
    let deadline = nabedra_rt::time::now() + request.deadline;
    let mut queued = Box::pin(voter.executor.execute(&handle, request));
    assert!(futures_util::poll!(queued.as_mut()).is_pending());
    resume();
    at_proposal(voter, queued.as_mut(), 2).await;
    // Drop on a thread with no Tokio runtime, as on shutdown.
    std::thread::scope(|scope| scope.spawn(|| drop(lone)).join().unwrap());
    pause_near(deadline).await;
    let response = queued.await;
    resume();
    assert_eq!(cluster.last_index(leader), before);
    assert_eq!(
        cluster.latest(leader, cluster.row_key(keys[1])).await,
        (None, None)
    );
    cluster.shutdown().await;
    assert_eq!(
        code(&response),
        Some((ErrorCode::StorageUnavailable, Outcome::NotApplied)),
        "{response:?}"
    );
}

struct ApplyAfterTimeout {
    inner: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    release: Arc<Notify>,
    applied: Arc<Notify>,
}

impl Consensus<ParticipantCommand, ParticipantReply> for ApplyAfterTimeout {
    fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ConsensusFuture<'_, Applied<ParticipantReply>> {
        let inner = self.inner.clone();
        let release = self.release.clone();
        let applied = self.applied.clone();
        Box::pin(async move {
            // Dropping the wait for a submitted proposal does not cancel Raft's work.
            let submitted = tokio::spawn(async move {
                release.notified().await;
                let result = inner.propose(command).await;
                applied.notify_one();
                result
            });
            submitted.await.expect("submitted proposal task")
        })
    }

    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.inner.ensure_linearizable()
    }

    fn lease_read_index(&self) -> ConsensusFuture<'_, u64> {
        self.inner.lease_read_index()
    }

    fn current_leader(&self) -> Option<u64> {
        self.inner.current_leader()
    }

    fn local_node(&self) -> Option<u64> {
        self.inner.local_node()
    }
}

#[tokio::test]
async fn d05_unknown_proposal_keeps_range_clock_pending_until_apply() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let clock = cluster.clock(leader);
    let consensus = Arc::new(ApplyAfterTimeout {
        inner: voter.consensus.clone(),
        release: Arc::new(Notify::new()),
        applied: Arc::new(Notify::new()),
    });
    let handle = Arc::new(ParticipantHandle::new(
        ParticipantMachine::new(voter.store.clone()).unwrap(),
        consensus.clone(),
    ));
    let request = timed_insert(&cluster, leader, 1);
    let deadline = request.deadline;
    let mut response = Box::pin(voter.executor.execute(&handle, request));
    at_proposal(voter, response.as_mut(), 1).await;
    assert_eq!(clock.pending_len(), 1);
    let token = clock.leader_token().unwrap();

    pause();
    advance(deadline + Duration::from_millis(1)).await;
    let response = response.await;
    resume();
    assert_eq!(
        code(&response).map(|(_, outcome)| outcome),
        Some(Outcome::Unknown)
    );
    assert_eq!(clock.leader_token(), Some(token));
    assert_eq!(
        clock.pending_len(),
        1,
        "an unknown proposal can still apply"
    );

    consensus.release.notify_one();
    nabedra_rt::time::timeout(Duration::from_secs(5), consensus.applied.notified())
        .await
        .expect("submitted proposal finishes");
    assert!(cluster.latest(leader, cluster.row_key(1)).await.0.is_some());
    assert_eq!(
        clock.pending_len(),
        0,
        "apply settled the pending timestamp"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn d05_deadline_before_raft_enqueue_settles_range_clock() {
    use crate::oltp::test_hooks::index_holds::{Checkpoint, register_checkpoint};

    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let clock = cluster.clock(leader);
    let request = timed_insert(&cluster, leader, 1);
    let reached = register_checkpoint(request.request_id, Checkpoint::StorageReserved);
    let deadline = request.deadline;
    let mut response = Box::pin(voter.executor.execute(&voter.handle, request));
    let held = nabedra_rt::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            held = reached => held.expect("storage reservation reached"),
            reply = &mut response => panic!("proposal ended before the reservation: {reply:?}"),
        }
    })
    .await
    .expect("proposal reaches the pre-enqueue checkpoint");
    assert_eq!(clock.pending_len(), 1);
    let token = clock.leader_token().unwrap();

    pause();
    advance(deadline + Duration::from_millis(1)).await;
    let response = response.await;
    resume();

    assert_eq!(
        code(&response).map(|(_, outcome)| outcome),
        Some(Outcome::Unknown)
    );
    assert!(
        held.release.send(true).is_err(),
        "pre-enqueue wait was cancelled"
    );
    assert_eq!(clock.leader_token(), Some(token));
    assert_eq!(
        clock.pending_len(),
        0,
        "an unsubmitted command cannot apply"
    );
    assert_eq!(
        cluster.latest(leader, cluster.row_key(1)).await,
        (None, None)
    );
    cluster.shutdown().await;
}

struct RefuseQueuedProposal {
    inner: Arc<RaftConsensus<DataConfig, ParticipantMachine>>,
    first: Uuid,
    release_first: Notify,
    queued_taken: Notify,
    refuse_queued: Notify,
    refused: Notify,
}

impl Consensus<ParticipantCommand, ParticipantReply> for RefuseQueuedProposal {
    fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ConsensusFuture<'_, Applied<ParticipantReply>> {
        Box::pin(async move {
            if matches!(command.payload()?, ParticipantCommand::Direct(commit)
                if commit.request_id == self.first)
            {
                self.release_first.notified().await;
                self.inner.propose(command).await
            } else {
                assert!(matches!(
                    command.payload()?,
                    ParticipantCommand::DirectBatch(batch) if batch.members.len() == 2
                ));
                self.queued_taken.notify_one();
                self.refuse_queued.notified().await;
                self.refused.notify_one();
                Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "proposal refused before append",
                    Outcome::NotApplied,
                ))
            }
        })
    }

    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.inner.ensure_linearizable()
    }

    fn lease_read_index(&self) -> ConsensusFuture<'_, u64> {
        self.inner.lease_read_index()
    }

    fn current_leader(&self) -> Option<u64> {
        self.inner.current_leader()
    }

    fn local_node(&self) -> Option<u64> {
        self.inner.local_node()
    }
}

#[tokio::test]
async fn d05_batch_driver_settles_late_definitive_refusal() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Default::default()
    })
    .await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let clock = cluster.clock(leader);
    let keys = distinct_keys(&cluster, 3);
    let first = timed_insert(&cluster, leader, keys[0]);
    let consensus = Arc::new(RefuseQueuedProposal {
        inner: voter.consensus.clone(),
        first: first.request_id,
        release_first: Notify::new(),
        queued_taken: Notify::new(),
        refuse_queued: Notify::new(),
        refused: Notify::new(),
    });
    let handle = Arc::new(ParticipantHandle::new(
        ParticipantMachine::new(voter.store.clone()).unwrap(),
        consensus.clone(),
    ));
    let mut lone = Box::pin(voter.executor.execute(&handle, first));
    at_proposal(voter, lone.as_mut(), 1).await;
    let request = timed_insert(&cluster, leader, keys[1]);
    let deadline = request.deadline;
    let mut queued = Box::pin(voter.executor.execute(&handle, request));
    at_proposal(voter, queued.as_mut(), 2).await;
    let mut companion = timed_insert(&cluster, leader, keys[2]);
    companion.deadline += Duration::from_secs(5);
    let mut companion = Box::pin(voter.executor.execute(&handle, companion));
    at_proposal(voter, companion.as_mut(), 3).await;

    consensus.release_first.notify_one();
    assert_eq!(lone.await, done("null"));
    nabedra_rt::time::timeout(Duration::from_secs(5), consensus.queued_taken.notified())
        .await
        .expect("batch driver takes the queued member");

    pause();
    advance(deadline + Duration::from_millis(1)).await;
    let response = queued.await;
    assert_eq!(
        code(&response).map(|(_, outcome)| outcome),
        Some(Outcome::Unknown)
    );
    assert_eq!(clock.pending_len(), 2);
    consensus.refuse_queued.notify_one();
    // The driver's deadline follows the later companion, so it can still
    // receive the refusal after the first member's caller timed out.
    nabedra_rt::time::timeout(Duration::from_secs(5), consensus.refused.notified())
        .await
        .expect("driver observes a definitive rejection");
    resume();
    drop(companion);
    assert_eq!(clock.pending_len(), 0);
    assert_eq!(
        cluster.latest(leader, cluster.row_key(keys[1])).await,
        (None, None)
    );
    cluster.shutdown().await;
}
