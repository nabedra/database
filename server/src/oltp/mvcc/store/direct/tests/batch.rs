//! GC2 store evidence: `DirectBatch` (several requests' direct commits to one
//! range in one Data entry). Entries are driven directly, as the leader's
//! batcher proposes them; the log order is the apply order.
use super::*;

fn batch(members: Vec<DirectCommit>) -> ParticipantCommand {
    ParticipantCommand::DirectBatch(DirectBatch { members })
}
fn batch_reply(members: Vec<DirectMemberReply>) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Batch(members))
}
fn member_applied(commit_ts: u64) -> DirectMemberReply {
    DirectMemberReply::Applied {
        commit_ts: ts(commit_ts),
    }
}

/// A one-row upsert with its own request id and body byte.
fn put_as(f: &Fixture, key: i64, expected: Option<u64>, commit_ts: u64, body: u8) -> DirectCommit {
    f.commit(
        Uuid::new_v4(),
        body,
        commit_ts,
        vec![f.write(
            key,
            expected,
            Some(serde_json::json!({"id": key, "ts": commit_ts})),
        )],
    )
}

#[tokio::test]
async fn o11_d03_gc2_batch_applies_each_member_with_its_own_record_like_one_entry_commits() {
    let mut f = Fixture::new().await;
    let mut follower = f.replica_of().await;
    let mut singles = f.replica_of().await;
    let members = vec![
        put_as(&f, 1, None, 12, 1),
        put_as(&f, 2, None, 10, 2),
        put_as(&f, 3, None, 11, 3),
    ];
    let entry = batch(members.clone());
    let reply = batch_reply(vec![
        member_applied(12),
        member_applied(10),
        member_applied(11),
    ]);
    assert_eq!(f.submit(entry.clone()).await, reply);
    // Follower apply equals the leader's: same reply, same state.
    assert_eq!(follower.submit(entry).await, reply);
    let state = f.dump().await;
    assert_eq!(follower.dump().await, state);
    // The batch leaves exactly the state its members leave as one-entry
    // commits (the carried high-waters included).
    for member in &members {
        assert_eq!(
            singles
                .submit(ParticipantCommand::Direct(member.clone()))
                .await,
            applied(member.commit_ts.get())
        );
    }
    assert_eq!(singles.dump().await, state);
    assert_eq!(count_tag(&state, DEDUP), 3, "one dedup record per member");
    assert_eq!(count_tag(&state, EXPIRY), 3);
    for (key, commit_ts) in [(1, 12), (2, 10), (3, 11)] {
        assert_eq!(f.version(20, key).await, Some(commit_ts));
    }
    for member in &members {
        let anchor = match &member.writes[0].key {
            MvccKey::Row {
                primary_key: Key::Int64(key),
                ..
            } => *key,
            _ => unreachable!(),
        };
        assert_eq!(
            f.lookup(anchor, member.request_id).await,
            Some(DedupLookup::Committed {
                digest: member.digest,
                commit_ts: member.commit_ts,
                result: DirectResult(
                    RawValue::from_string(member.result.as_str().to_owned()).unwrap()
                ),
            })
        );
    }
}

#[tokio::test]
async fn d03_gc2_every_member_retried_replays_its_own_result() {
    let mut f = Fixture::new().await;
    let members = vec![put_as(&f, 1, None, 10, 1), put_as(&f, 2, None, 11, 2)];
    assert_eq!(
        f.submit(batch(members.clone())).await,
        batch_reply(vec![member_applied(10), member_applied(11)])
    );
    let state = f.dump().await;
    // Every member retried in one batch (new timestamps, as a retry has):
    // each replays, nothing is written.
    let retried: Vec<_> = members
        .iter()
        .map(|member| DirectCommit {
            commit_ts: ts(member.commit_ts.get() + 10),
            ..member.clone()
        })
        .collect();
    assert_eq!(
        f.submit(batch(retried.clone())).await,
        batch_reply(vec![DirectMemberReply::Replayed; 2])
    );
    assert_eq!(f.dump().await, state);
    // Each member retried alone returns its own original result.
    for (member, retry) in members.iter().zip(&retried) {
        assert_eq!(f.direct(retry).await, replayed(member));
    }
    assert_eq!(f.dump().await, state);
    // The same id with another body conflicts, per member.
    let reused = DirectCommit {
        digest: digest(99),
        ..retried[0].clone()
    };
    assert_eq!(
        f.submit(batch(vec![reused, put_as(&f, 3, None, 30, 3)]))
            .await,
        batch_reply(vec![
            DirectMemberReply::Rejected(Fault::Conflict),
            member_applied(30)
        ])
    );
}

#[tokio::test]
async fn o07_d08_gc2_mixed_rejections_write_nothing_for_their_member_and_the_others_apply() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    // Key 2 holds a queued intent.
    let write = f.queued_put(2, None, serde_json::json!({"queued": true}));
    let plan = f.queued_plan(10, std::slice::from_ref(&write));
    assert!(matches!(
        f.prepare(&plan, vec![write]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    let forward = f.forward_of(4, Uuid::new_v4(), 4);
    assert_eq!(f.forward(&forward).await, forwarded_reply());
    let before = f.dump().await;
    let stale = put_as(&f, 1, Some(9), 20, 1);
    let intent = put_as(&f, 2, None, 21, 2);
    let fine = put_as(&f, 3, None, 22, 3);
    let marked = f.commit(
        forward.request_id,
        4,
        23,
        vec![f.write(4, None, Some(serde_json::json!({"id": 4})))],
    );
    let mut wrong_epoch = put_as(&f, 5, None, 24, 5);
    wrong_epoch.schema_epoch = 7;
    assert_eq!(
        f.submit(batch(vec![
            stale,
            intent,
            fine.clone(),
            marked,
            wrong_epoch
        ]))
        .await,
        batch_reply(vec![
            DirectMemberReply::Rejected(Fault::Conflict),
            DirectMemberReply::Rejected(Fault::Incomplete),
            member_applied(22),
            DirectMemberReply::Forwarded,
            DirectMemberReply::Rejected(Fault::StaleEpoch),
        ])
    );
    // Only the applied member changed state, exactly as it would alone.
    let after = f.dump().await;
    assert_eq!(count_tag(&after, DEDUP), count_tag(&before, DEDUP) + 1);
    assert_eq!(f.version(30, 1).await, Some(10));
    assert_eq!(f.version(30, 3).await, Some(22));
    assert_eq!(f.version(30, 4).await, None);
    assert_eq!(f.version(30, 5).await, None);
    // A batch whose every member is rejected is a rejected entry.
    let before = f.dump().await;
    assert_eq!(
        f.submit(batch(vec![
            put_as(&f, 1, None, 40, 1),
            put_as(&f, 2, None, 41, 2)
        ]))
        .await,
        batch_reply(vec![
            DirectMemberReply::Rejected(Fault::Conflict),
            DirectMemberReply::Rejected(Fault::Incomplete),
        ])
    );
    assert_eq!(f.dump().await, before);
}

#[tokio::test]
async fn o07_gc2_apply_refuses_a_member_sharing_a_key_with_an_earlier_member() {
    let mut f = Fixture::new().await;
    let first = put_as(&f, 1, None, 10, 1);
    // A second request on the same row that read the same (empty) head.
    let same_row = put_as(&f, 1, None, 11, 2);
    // Another row whose index entry is the first one's: the same index key.
    let index_key = MvccKey::IndexEntry {
        table_id: f.table,
        index_id: Uuid::new_v4(),
        values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
        primary_key: Key::Int64(2),
    };
    let mut with_index = put_as(&f, 2, None, 12, 3);
    with_index.writes[0].index.push(IndexChange {
        key: index_key.clone(),
        expected_version: None,
        value: Some(f.json(serde_json::json!({"id": 2}))),
    });
    let mut index_again = with_index.clone();
    index_again.request_id = Uuid::new_v4();
    index_again.commit_ts = ts(13);
    assert_eq!(
        f.submit(batch(vec![
            first,
            same_row.clone(),
            with_index,
            index_again
        ]))
        .await,
        batch_reply(vec![
            member_applied(10),
            DirectMemberReply::Rejected(Fault::Conflict),
            member_applied(12),
            DirectMemberReply::Rejected(Fault::Conflict),
        ])
    );
    let state = f.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 2);
    assert_eq!(f.version(20, 1).await, Some(10));
    assert_eq!(f.version(20, 2).await, Some(12));
    // Re-read and retried alone, the refused request applies.
    assert_eq!(
        f.direct(&DirectCommit {
            writes: vec![f.write(1, Some(10), Some(serde_json::json!({"id": 1})))],
            commit_ts: ts(14),
            ..same_row
        })
        .await,
        applied(14)
    );
}

#[tokio::test]
async fn o07_gc2_batch_bounds_and_split_barrier_apply_per_member() {
    let mut f = Fixture::new().await;
    let before = f.dump().await;
    assert_eq!(
        f.submit(batch(Vec::new())).await,
        rejected(Fault::InvalidBinding)
    );
    let too_many = (0..=MAX_DIRECT_BATCH_MEMBERS as i64)
        .map(|key| put_as(&f, key, None, 10 + key as u64, 1))
        .collect();
    assert_eq!(
        f.submit(batch(too_many)).await,
        rejected(Fault::InvalidBinding)
    );
    assert_eq!(f.dump().await, before);
    let full: Vec<_> = (0..MAX_DIRECT_BATCH_MEMBERS as i64)
        .map(|key| put_as(&f, key, None, 100 + key as u64, 1))
        .collect();
    let ParticipantReply::Direct(DirectReply::Batch(replies)) = f.submit(batch(full.clone())).await
    else {
        panic!("a full batch applies");
    };
    assert!(
        replies
            .iter()
            .all(|reply| matches!(reply, DirectMemberReply::Applied { .. }))
    );
    // The split barrier: new members answer `SplitInProgress`, an exact
    // retry still replays.
    let split = Uuid::new_v4();
    assert!(matches!(
        f.submit(pause(&f, split, true)).await,
        ParticipantReply::DirectPaused { paused: true, .. }
    ));
    let state = f.dump().await;
    let retry = DirectCommit {
        commit_ts: ts(500),
        ..full[0].clone()
    };
    assert_eq!(
        f.submit(batch(vec![retry, put_as(&f, 900, None, 501, 2)]))
            .await,
        batch_reply(vec![
            DirectMemberReply::Replayed,
            DirectMemberReply::Rejected(Fault::SplitInProgress)
        ])
    );
    assert_eq!(f.dump().await, state);
}

#[tokio::test]
async fn d06_gc2_applying_a_batch_settles_every_member_in_the_range_clock() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    let members = vec![put_as(&f, 1, None, 15, 1), put_as(&f, 2, Some(3), 16, 2)];
    for member in &members {
        register(&clock, member.commit_ts.get(), member.keys()).unwrap();
    }
    let scan = tokio::spawn({
        let machine = f.machine().clone();
        let table = f.table;
        async move { scan_versions(&machine, table, 20).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!scan.is_finished(), "the scan waits for both members");
    // One applied and one rejected member: both leave the clock.
    assert_eq!(
        f.submit(batch(members)).await,
        batch_reply(vec![
            member_applied(15),
            DirectMemberReply::Rejected(Fault::Conflict)
        ])
    );
    assert_eq!(scan.await.unwrap(), BTreeMap::from([(1, 15)]));
    assert_eq!((clock.pending_len(), clock.pending_key_count()), (0, 0));
}

#[tokio::test]
async fn o11_gc2_crash_replay_of_a_batch_entry_is_idempotent() {
    let f = Fixture::new().await;
    let members = vec![put_as(&f, 1, None, 10, 1), put_as(&f, 2, None, 11, 2)];
    let command = batch(members.clone());
    let entry = || OwnedCommand::new_owned(command.clone(), f.shared(), ByteClass::Data).unwrap();
    let meta = |index| ApplyMeta {
        position: AppliedPosition {
            term: 1,
            node_id: 1,
            index,
        },
        command_digest: digest(9),
        membership: None,
    };
    let first = f
        .machine()
        .apply(meta(f.replica.index), entry())
        .await
        .unwrap();
    let state = f.dump().await;
    // Raft redelivers the entry after a crash between its append and the
    // applied index being observed: the stored reply returns, nothing twice.
    let again = f
        .machine()
        .apply(meta(f.replica.index), entry())
        .await
        .unwrap();
    assert_eq!(again.as_slice(), first.as_slice());
    assert_eq!(f.dump().await, state);
    drop((first, again));
    let Fixture {
        replica,
        pool,
        pool_identity,
        group,
        dir,
        ..
    } = f;
    let next_index = replica.index + 1;
    let shared = replica.shared.clone();
    drop(replica);
    drop(pool);
    let pool = OltpPool::open_development(&dir.0, pool_identity, shared.clone()).unwrap();
    let mut reopened = Replica {
        machine: ParticipantMachine::new(pool.group(group).await.unwrap()).unwrap(),
        shared,
        index: next_index,
    };
    assert_eq!(reopened.dump().await, state, "both members are durable");
    // Every member proposed again (one alone, one in a new batch) replays.
    assert_eq!(
        reopened
            .submit(ParticipantCommand::Direct(DirectCommit {
                commit_ts: ts(20),
                ..members[0].clone()
            }))
            .await,
        replayed(&members[0])
    );
    assert_eq!(
        reopened
            .submit(batch(vec![DirectCommit {
                commit_ts: ts(21),
                ..members[1].clone()
            }]))
            .await,
        batch_reply(vec![DirectMemberReply::Replayed])
    );
    assert_eq!(reopened.dump().await, state);
}

#[tokio::test]
async fn o02_gc2_direct_batch_uses_a_new_tag_that_an_older_decoder_rejects() {
    let mut f = Fixture::new().await;
    let command = batch(vec![f.put(1, None, 10)]);
    assert!(command.is_direct());
    let value = serde_json::to_value(&command).unwrap();
    assert!(value.get("direct_batch").is_some());
    // An older build's participant decoder knows no `direct_batch` tag: the
    // same object with the tag renamed to one this build does not know fails
    // to decode, which is how an older replica fails on the entry.
    let unknown = serde_json::json!({"direct_batch_v0": value["direct_batch"].clone()});
    assert!(serde_json::from_value::<ParticipantCommand>(unknown).is_err());
    // A one-entry commit still encodes as before (`direct`), so a gate-off
    // leader emits nothing new.
    let single = serde_json::to_value(ParticipantCommand::Direct(f.put(2, None, 11))).unwrap();
    assert!(single.get("direct").is_some() && single.get("direct_batch").is_none());
    // Nested in a scoped envelope, a batch is refused at apply.
    let before = f.dump().await;
    let scoped = ParticipantCommand::Scoped {
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead {
            queue_group_id: Uuid::new_v4(),
            data_generation: Uuid::new_v4(),
            queue_generation: Uuid::new_v4(),
            queue_view: crate::oltp_replication::types::AuthorityView {
                run_id: f.authority.run_id,
                owner_term: f.authority.owner_term,
                membership_revision: 1,
            },
            request: RequestBinding {
                run_id: f.authority.run_id,
                request_id: Uuid::new_v4(),
                digest: digest(1),
                accepted_context: digest(2),
            },
            table_id: f.table,
            read_ts: ts(9),
        },
        command: OwnedCommand::new_owned(command, f.shared(), ByteClass::Data).unwrap(),
    };
    assert_eq!(
        f.submit(scoped).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(f.dump().await, before);
}
