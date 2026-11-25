//! Per-entry change records (DB-5).
use super::*;
use crate::oltp::mvcc::store::row::VECTOR;

fn row(key: i64, v: u64) -> serde_json::Value {
    serde_json::json!({"id": key, "v": v})
}
/// The index of the entry that started the feed: fence, activation, search fence.
const START: u64 = 2;

async fn page(s: &S, after: u64) -> OwnedCommand<ChangePage> {
    s.f.machine().change_page(after, 1024, 8 << 20).await.unwrap()
}
async fn purge(s: &S, from: u64, through: u64) {
    s.f.machine()
        .store()
        .log_transaction(BUDGET, move |tx| {
            tx.purge_changes(from, through)?;
            Ok(Transition::Commit(tx.copy_output(b"")?))
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn vs02_the_feed_says_what_each_entry_did() {
    let mut s = S::new().await;
    let first = s.write(1, None, Some(row(1, 1)), vec![s.set(PLAIN, s.dense(1))]);
    assert_eq!(s.direct(10, first).await, applied(10)); // 3
    let stale = s.write(1, None, Some(row(1, 9)), Vec::new());
    assert_eq!(s.direct(11, stale).await, rejected(Fault::Conflict)); // 4
    let commit = s.commit(12, vec![s.write(2, None, Some(row(2, 2)), Vec::new())]);
    s.f.direct(&commit).await; // 5
    assert_eq!(s.f.direct(&commit).await, replayed(&commit)); // 6
    let members = vec![
        s.commit(14, vec![s.write(3, None, Some(row(3, 3)), Vec::new())]),
        s.commit(13, vec![s.write(2, Some(9), Some(row(2, 9)), Vec::new())]),
        s.commit(13, vec![s.write(1, Some(10), None, Vec::new())]),
    ];
    s.f.submit(ParticipantCommand::DirectBatch(DirectBatch { members })).await; // 7
    let writes = vec![
        s.put_row(1, Some(13), row(1, 4), vec![s.set(PLAIN, s.dense(4))]),
        s.f.queued_put(2, Some(12), row(2, 4)),
        PreparedWrite { change: Change::Keep, ..s.f.queued_put(3, Some(14), row(3, 0)) },
    ];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await; // 8
    s.f.seal_and_resolve(&plan, 21).await; // 9, 10
    let writes = vec![s.f.queued_put(9, None, row(9, 9))];
    let plan = s.f.queued_plan(21, &writes);
    s.f.prepare(&plan, writes).await; // 11
    s.abort(&plan).await; // 12
    s.close(30).await; // 13
    s.close(25).await; // 14

    let full = |key: i64, v, at: u64, vector: Option<(u64, u8)>| Got {
        version: Some(at),
        value: Some(row(key, v)),
        vectors: vector
            .map(|(at, n)| (PLAIN, (at, vec![n; 8])))
            .into_iter()
            .collect(),
        ..Got::default()
    };
    let expected: Vec<Seen> = vec![
        (3, 1, 10, Some(full(1, 1, 10, Some((10, 1))))),
        (5, 2, 12, Some(full(2, 2, 12, None))),
        (7, 3, 14, Some(full(3, 3, 14, None))),
        (7, 1, 13, None),
        (10, 1, 21, Some(full(1, 4, 21, Some((21, 4))))),
        (10, 2, 21, Some(full(2, 4, 21, None))),
        (13, i64::MIN, 30, None),
    ];
    assert_eq!(seen(&s.changes(START, 1024).await), expected);
    // Pages are contiguous after any position.
    for entries in [1, 2] {
        assert_eq!(seen(&s.changes(START, entries).await), expected);
    }
    for after in START..=s.last() {
        let tail: Vec<_> = expected.iter().filter(|change| change.0 > after).collect();
        assert_eq!(seen(&s.changes(after, 1).await).iter().collect::<Vec<_>>(), tail);
    }
    // Before the feed started, past the applied entry, and without one.
    assert!(matches!(page(&s, START - 1).await.payload().unwrap(), ChangePage::Gone { .. }));
    let ahead = page(&s, s.last() + 1).await;
    let ChangePage::Behind { applied } = ahead.payload().unwrap() else { panic!() };
    assert_eq!(applied.index, s.last());
    let plain = S::plain(Some(&FaultFs::new())).await;
    assert!(matches!(page(&plain, 0).await.payload().unwrap(), ChangePage::Inactive));
}

#[tokio::test]
async fn vs02_a_redelivered_entry_leaves_its_one_record() {
    let mut s = S::new().await;
    let commit = s.commit(10, vec![s.write(1, None, Some(row(1, 1)), Vec::new())]);
    let command = || {
        OwnedCommand::new_owned(ParticipantCommand::Direct(commit.clone()), s.f.shared(), ByteClass::Data).unwrap()
    };
    let meta = ApplyMeta { position: position(s.f.replica.index), command_digest: digest(9), membership: None };
    for _ in 0..3 {
        s.f.machine().apply(meta.clone(), command()).await.unwrap();
    }
    s.f.replica.index += 1;
    assert_eq!(seen(&s.changes(START, 8).await).len(), 1);
}

#[tokio::test]
async fn vs02_purged_records_and_collected_values_answer_explicitly() {
    let mut s = S::new().await;
    for n in 0..6 {
        let write = s.write(n, None, Some(row(n, 1)), vec![s.set(PLAIN, s.dense(n as u8))]);
        s.direct(10 + n as u64, write).await;
    }
    let all = seen(&s.changes(START, 1024).await);
    assert_eq!(all.len(), 6);
    purge(&s, 0, 4).await;
    for after in [START, 3] {
        assert!(matches!(page(&s, after).await.payload().unwrap(), ChangePage::Gone { .. }));
    }
    assert_eq!(seen(&s.changes(4, 1024).await), all[2..]);
    // Purging past the newest record is a gap for every older position only.
    purge(&s, 5, s.last()).await;
    assert!(matches!(page(&s, s.last() - 1).await.payload().unwrap(), ChangePage::Gone { .. }));
    assert!(s.changes(s.last(), 8).await.is_empty());
    let write = s.write(9, None, Some(row(9, 1)), Vec::new());
    s.direct(30, write).await;
    assert_eq!(seen(&s.changes(s.last() - 1, 8).await).len(), 1);
    assert!(matches!(page(&s, 5).await.payload().unwrap(), ChangePage::Gone { .. }));

    // A row version or a vector that a collector removed is never skipped.
    for tag in [VALUE, VECTOR] {
        let mut s = S::new().await;
        let write = s.write(1, None, Some(row(1, 1)), vec![s.set(PLAIN, s.dense(1))]);
        s.direct(10, write).await;
        let write = s.write(1, Some(10), Some(row(1, 2)), vec![s.set(PLAIN, s.dense(2))]);
        s.direct(11, write).await;
        // Newest first for row versions, oldest first for vectors.
        let records = s.records(tag).await;
        let key = records[if tag == VALUE { 1 } else { 0 }].0.clone();
        s.tamper(move |tx| tx.delete(&key)).await;
        assert!(matches!(page(&s, START).await.payload().unwrap(), ChangePage::Gone { .. }));
        assert_eq!(seen(&s.changes(3, 8).await).len(), 1);
    }
}

#[tokio::test]
async fn vs02_a_page_stops_at_its_byte_budget() {
    let mut s = S::new().await;
    for n in 0..8 {
        let value = s.value(VectorKind::Sparse, &vec![n as u8; 100 << 10]);
        let write = s.write(n, None, Some(row(n, 1)), vec![s.set(SPARSE, value)]);
        s.direct(10 + n as u64, write).await;
    }
    let mut after = START;
    let mut found = 0;
    while after < s.last() {
        let page = s.f.machine().change_page(after, 1024, 400 << 10).await.unwrap();
        let ChangePage::Changes { entries, through } = page.payload().unwrap() else { panic!() };
        assert_eq!(entries.len(), 2);
        found += entries.len();
        after = through.index;
    }
    assert_eq!(found, 8);
    let refused = s.f.machine().change_page(START, 1024, 64 << 10).await.err().unwrap();
    assert_eq!(refused.code, ErrorCode::LimitExceeded);
}

#[tokio::test]
async fn vs02_a_replica_snapshot_is_exact_and_leaves_an_explicit_gap() {
    let mut s = S::new().await;
    let mut other = s.f.replica_of_inactive().await;
    let activation = s.f.activation();
    other.submit(activation).await;
    let fence = s.fence_command(1, columns());
    other.submit(fence).await;
    // The follower applies two entries, then falls behind.
    for n in 0..8u64 {
        let write = s.write(1, (n > 0).then_some(9 + n), Some(row(1, n)), vec![s.set(PLAIN, s.dense(n as u8))]);
        let commit = s.commit(10 + n, vec![write]);
        assert_eq!(s.f.direct(&commit).await, applied(10 + n));
        if n < 2 {
            other.submit(ParticipantCommand::Direct(commit)).await;
        }
    }
    s.close(17).await;
    let snapshot = s.f.machine().store().export_snapshot(BUDGET).await.unwrap();
    let store = other.machine.store().clone();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    other.index = s.f.replica.index;
    assert_eq!(other.dump().await, s.f.dump().await);
    // It still has the two records it wrote, and answers for nothing between.
    let old = changes(&other.machine, START, 8).await;
    assert!(old.is_none());
    for after in START..s.last() {
        let gone = other.machine.change_page(after, 1, 1 << 20).await.unwrap();
        let only_old = matches!(gone.payload().unwrap(), ChangePage::Changes { entries, .. } if entries.len() == 1 && after < 4);
        assert!(only_old || matches!(gone.payload().unwrap(), ChangePage::Gone { .. }), "{after}");
    }
    assert!(changes(&other.machine, s.last(), 8).await.unwrap().is_empty());
    // From the cut on, its feed is the leader's.
    let write = s.write(1, Some(17), Some(row(1, 99)), Vec::new());
    let commit = s.commit(30, vec![write]);
    s.f.direct(&commit).await;
    other.submit(ParticipantCommand::Direct(commit)).await;
    let cut = s.last() - 1;
    assert_eq!(
        seen(&changes(&other.machine, cut, 8).await.unwrap()),
        seen(&s.changes(cut, 8).await)
    );
    assert_eq!(other.dump().await, s.f.dump().await);
}

/// What apply reported, against what the records say, over random logs with
/// rejected, repeated, batched, queued and aborted work and a purge at a random
/// place.
#[tokio::test]
async fn vs02_random_logs_feed_exactly_what_applied() {
    for seed in 0..30 {
        let mut rng = Rng(seed);
        let mut s = S::new().await;
        let mut heads: BTreeMap<i64, (u64, bool)> = BTreeMap::new();
        let mut expected: Vec<(u64, i64, u64, Option<u64>)> = Vec::new();
        let mut last_commit: Option<DirectCommit> = None;
        for step in 0..50u64 {
            let key = rng.below(4) as i64;
            let at = s.tick();
            let head = heads.get(&key).map(|(at, _)| *at);
            let stale = rng.one_in(5);
            let expects = if stale { Some(1) } else { head };
            let value = (!rng.one_in(5) || !heads.get(&key).is_some_and(|(_, live)| *live))
                .then(|| row(key, step));
            match rng.below(5) {
                0 | 1 => {
                    let commit = s.commit(at, vec![s.write(key, expects, value.clone(), Vec::new())]);
                    let reply = s.f.direct(&commit).await;
                    assert_eq!(reply == applied(at), !stale, "seed {seed} step {step}");
                    if !stale {
                        expected.push((s.last(), key, at, value.as_ref().map(|_| step)));
                        heads.insert(key, (at, value.is_some()));
                    }
                    last_commit = Some(commit);
                }
                2 => {
                    if let Some(commit) = &last_commit {
                        s.f.direct(commit).await;
                    }
                }
                3 => {
                    let Some(value) = value else { continue };
                    let writes = vec![s.f.queued_put(key, expects, value)];
                    let plan = s.f.queued_plan(at, &writes);
                    let prepared = s.f.prepare(&plan, writes).await;
                    if stale {
                        assert_eq!(prepared, ParticipantReply::Rejected(Fault::Conflict));
                        continue;
                    }
                    if rng.one_in(3) {
                        s.abort(&plan).await;
                        continue;
                    }
                    let at = s.tick();
                    s.f.seal_and_resolve(&plan, at).await;
                    expected.push((s.last(), key, at, Some(step)));
                    heads.insert(key, (at, true));
                }
                _ => {
                    let other = (key + 1) % 4;
                    let live = heads.get(&other).map(|(at, _)| *at);
                    let members = vec![
                        s.commit(at, vec![s.write(key, expects, Some(row(key, step)), Vec::new())]),
                        s.commit(at, vec![s.write(other, live, Some(row(other, step)), Vec::new())]),
                    ];
                    s.f.submit(ParticipantCommand::DirectBatch(DirectBatch { members })).await;
                    if !stale {
                        expected.push((s.last(), key, at, Some(step)));
                        heads.insert(key, (at, true));
                    }
                    expected.push((s.last(), other, at, Some(step)));
                    heads.insert(other, (at, true));
                }
            }
        }
        let flat = |entries: &[EntryChanges]| -> Vec<(u64, i64, u64, Option<u64>)> {
            seen(entries)
                .into_iter()
                .map(|(index, key, at, row)| {
                    (index, key, at, row.map(|row| row.value.unwrap()["v"].as_u64().unwrap()))
                })
                .collect()
        };
        let entries = 1 + rng.below(5) as u16;
        assert_eq!(flat(&s.changes(START, entries).await), expected, "seed {seed}");
        let through = START + rng.next() % (s.last() - START);
        purge(&s, 0, through).await;
        for after in START..=s.last() {
            let tail: Vec<_> = expected.iter().filter(|change| change.0 > after).copied().collect();
            let lost = expected.iter().any(|change| change.0 > after && change.0 <= through);
            let found = changes(s.f.machine(), after, 64).await;
            assert_eq!(found.is_none(), lost, "seed {seed} after {after} purged {through}");
            if let Some(found) = found {
                assert_eq!(flat(&found), tail, "seed {seed} after {after}");
            }
        }
    }
}
