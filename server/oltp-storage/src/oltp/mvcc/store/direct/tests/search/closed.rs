//! VEC-G02: closed timestamps at apply (`specs/vector/snapshot`).
use super::*;

fn row(key: i64) -> Option<serde_json::Value> {
    Some(serde_json::json!({"id": key}))
}

/// Mutant `direct_ignores_floor`: DB-1 and DB-2.
#[tokio::test]
async fn vs04_no_commit_lands_at_or_below_a_closed_timestamp() {
    let mut s = S::new().await;
    assert_eq!(s.close(50).await, SearchReply::Closed { floor: ts(50) });
    for at in [1, 49, 50] {
        let before = s.f.dump().await;
        let write = s.write(1, None, row(1), Vec::new());
        assert_eq!(s.direct(at, write).await, rejected(Fault::Conflict), "{at}");
        assert_eq!(s.f.dump().await, before);
    }
    // The leader retries with a new timestamp, and the floor never goes back.
    let write = s.write(1, None, row(1), Vec::new());
    assert_eq!(s.direct(51, write).await, applied(51));
    let before = s.f.dump().await;
    for late in [10, 50] {
        assert_eq!(s.close(late).await, SearchReply::Closed { floor: ts(50) });
    }
    assert_eq!(s.f.dump().await, before);
    assert_eq!(s.close(60).await, SearchReply::Closed { floor: ts(60) });
    // A new run's fence keeps it.
    let next = Authority {
        run_id: nabedra_rt::ids::uuid_v4(),
        owner_term: 2,
    };
    let fence = ParticipantCommand::Fence {
        transition_id: nabedra_rt::ids::uuid_v4(),
        expected: Some(s.f.authority),
        next,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        catalog_group_id: s.f.catalog_group_id,
    };
    assert!(matches!(s.f.submit(fence).await, ParticipantReply::Fenced(_)));
    assert_eq!(s.close(70).await, refused(Fault::StaleAuthority));
    s.f.authority = next;
    let write = s.write(2, None, row(2), Vec::new());
    assert_eq!(s.direct(60, write).await, rejected(Fault::Conflict));
}

/// Mutant `closed_ignores_intents`.
#[tokio::test]
async fn vs04_an_open_intent_or_a_frozen_range_refuses_to_close() {
    let mut s = S::new().await;
    let writes = vec![s.f.queued_put(1, None, serde_json::json!({"id": 1}))];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    let before = s.f.dump().await;
    assert_eq!(s.close(30).await, refused(Fault::Incomplete));
    assert_eq!(s.f.dump().await, before);
    // The transaction commits below the timestamp that was not closed.
    s.f.seal_and_resolve(&plan, 25).await;
    assert_eq!(s.close(30).await, SearchReply::Closed { floor: ts(30) });

    let split = nabedra_rt::ids::uuid_v4();
    let freeze = ParticipantCommand::SplitFreeze {
        authority: s.f.authority,
        children: Box::new(split_bindings(&s.f, split, 40)),
    };
    assert!(matches!(s.f.submit(freeze).await, ParticipantReply::SplitSourceFrozen(_)));
    let before = s.f.dump().await;
    assert_eq!(s.close(50).await, refused(Fault::SplitInProgress));
    assert_eq!(s.f.dump().await, before);
    // Only a range with search columns closes.
    let mut plain = S::plain(Some(&FaultFs::new())).await;
    assert_eq!(plain.close(10).await, refused(Fault::DirectInactive));
}

/// Takes, ClosedTakes and FloorAt over random logs of one key: what applied
/// matches the spec, and no entry lands at or below an effective `Closed`.
#[tokio::test]
async fn vs04_random_logs_take_exactly_what_the_snapshot_spec_takes() {
    for seed in 0..40 {
        let mut rng = Rng(seed);
        let mut s = S::new().await;
        let (mut tso, mut floor, mut newest) = (10u64, 0u64, 0u64);
        let mut open: Option<QueuedPlan> = None;
        // Timestamps handed out to attempts that have not been proposed yet.
        let mut attempts = Vec::new();
        let mut effective: Vec<(usize, u64)> = Vec::new();
        for step in 0..60 {
            match rng.below(6) {
                0 => {
                    tso += 1;
                    attempts.push(tso);
                }
                1 | 2 if !attempts.is_empty() => {
                    // Any attempt, so a commit of unknown outcome lands late.
                    let at = attempts.swap_remove(rng.below(attempts.len()));
                    let head = (newest != 0).then_some(newest);
                    let write = s.write(1, head, row(1), Vec::new());
                    let takes = open.is_none() && at > newest && at > floor;
                    let reply = s.direct(at, write).await;
                    assert_eq!(reply == applied(at), takes, "seed {seed} step {step} {reply:?}");
                    if takes {
                        newest = at;
                        effective.push((step, at));
                    }
                }
                3 if open.is_none() => {
                    let head = (newest != 0).then_some(newest);
                    let writes = vec![s.f.queued_put(1, head, serde_json::json!({"id": 1}))];
                    tso += 1;
                    let plan = s.f.queued_plan(tso, &writes);
                    let reply = s.f.prepare(&plan, writes).await;
                    assert!(matches!(reply, ParticipantReply::PreparedPage { .. }), "{reply:?}");
                    open = Some(plan);
                }
                4 => {
                    if let Some(plan) = open.take() {
                        // The commit timestamp is taken after the prepare.
                        tso += 1;
                        if rng.one_in(3) {
                            s.abort(&plan).await;
                        } else {
                            s.f.seal_and_resolve(&plan, tso).await;
                            newest = tso;
                            effective.push((step, tso));
                        }
                    }
                }
                _ => {
                    let at = 1 + rng.next() % tso;
                    let reply = s.close(at).await;
                    if open.is_some() {
                        assert_eq!(reply, refused(Fault::Incomplete), "seed {seed} step {step}");
                    } else {
                        floor = floor.max(at);
                        assert_eq!(reply, SearchReply::Closed { floor: ts(floor) });
                        // ClosedIsFinal, over what already applied.
                        effective.push((step, u64::MAX));
                    }
                }
            }
        }
        // ClosedIsFinal, from the feed: no row change after a close is at or
        // below it.
        let origin = s.f.machine().change_page(s.last(), 1, 1 << 20).await.unwrap();
        assert!(matches!(origin.payload().unwrap(), ChangePage::Changes { .. }));
        let mut closed = 0;
        for (_, key, at, row) in seen(&s.changes(2, 1024).await) {
            if key == i64::MIN {
                assert!(at > closed, "the floor only rises");
                closed = at;
            } else {
                assert!(row.is_some() && at > closed, "seed {seed}");
            }
        }
        assert_eq!(closed, floor, "seed {seed}");
    }
}
