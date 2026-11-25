//! Legal and illegal fixtures for every rule, and for each way a rule must not fire.

use super::*;
use serde_json::json;

const RUN: Uuid = Uuid::from_u128(100);
const NEXT: Uuid = Uuid::from_u128(101);

fn body(id: u128, key: i64, value: i64) -> Body {
    Body {
        id: Uuid::from_u128(id),
        text: format!("upsert {key} {value}"),
        anchor: Some(key),
        writes: vec![(key, value)],
        mutation: true,
    }
}

fn submit(body: Body, at: u64, answer: Answer) -> Submission {
    Submission {
        body,
        route: Route::Queued,
        asynchronous: true,
        gateway: 101,
        closed: false,
        fresh: true,
        invoked: at,
        completed: at + 10,
        answer,
    }
}

fn accepted(id: u128, at: u64) -> Submission {
    submit(body(id, 1, id as i64), at, Answer::Accepted(RUN))
}

fn succeeded(body: Body, at: u64, result: Value) -> Submission {
    submit(body, at, Answer::Terminal(Terminal::Succeeded(result)))
}

fn direct(body: Body, at: u64, result: Value) -> Submission {
    Submission {
        route: Route::MaybeDirect,
        ..succeeded(body, at, result)
    }
}

fn lookup(id: u128, at: u64, seen: Seen) -> Lookup {
    Lookup {
        run: RUN,
        id: Uuid::from_u128(id),
        gateway: 101,
        invoked: at,
        completed: at + 10,
        seen,
    }
}

fn ok(value: Value) -> Seen {
    Seen::Terminal(Terminal::Succeeded(value))
}

fn refused() -> Terminal {
    Terminal::failed("conflict")
}

fn unknown() -> Seen {
    Seen::Terminal(Terminal::Failed {
        code: "storage_unavailable".into(),
        unknown: true,
    })
}

fn frozen(nodes: &[u64], ready: Option<u64>) -> Frozen {
    Frozen {
        nodes: nodes.to_vec(),
        from: 1_000,
        to: 2_000,
        ready,
    }
}

fn read(key: i64, value: Option<i64>, at: u64) -> Read {
    Read {
        key,
        value,
        invoked: at,
        completed: at + 10,
    }
}

fn log() -> Log {
    Log {
        runs: vec![Run {
            id: RUN,
            stopped: None,
        }],
        retention_ms: 5_000,
        clock_slack_ms: 100,
        ..Log::default()
    }
}

fn kinds(log: &Log) -> Vec<&'static str> {
    log.judge().kinds().into_iter().collect()
}

#[test]
fn accepted_work_settles_or_expires_in_its_run() {
    let mut legal = log();
    legal.submissions = vec![accepted(1, 100), accepted(2, 100)];
    legal.lookups = vec![
        lookup(1, 200, Seen::Pending),
        lookup(1, 300, ok(json!(null))),
        lookup(1, 400, Seen::Unknown),
        // Pending until 200 ms, so a result expires at 5,200 ms at the earliest.
        lookup(1, 5_400, Seen::Missing),
        lookup(2, 300, Seen::Terminal(refused())),
    ];
    assert!(kinds(&legal).is_empty(), "{:?}", legal.judge());
    assert!(legal.judge().judged[ACCEPTED_SETTLES] > 0);

    let mut lost = log();
    lost.submissions = vec![accepted(1, 100)];
    lost.lookups = vec![lookup(1, 1_000, Seen::Missing)];
    assert_eq!(kinds(&lost), [ACCEPTED_SETTLES]);

    let mut abandoned = log();
    abandoned.submissions = vec![accepted(1, 100)];
    abandoned.lookups = vec![lookup(1, 9_000, Seen::Abandoned)];
    assert_eq!(kinds(&abandoned), [ACCEPTED_SETTLES]);

    // Seen pending later, so the expiry moves with it.
    let mut pending = log();
    pending.submissions = vec![accepted(1, 100)];
    pending.lookups = vec![
        lookup(1, 4_000, Seen::Pending),
        lookup(1, 6_000, Seen::Missing),
    ];
    assert_eq!(kinds(&pending), [ACCEPTED_SETTLES]);
}

/// Seed 10 (legacy-off, network): attempts with unknown answers from 51 s on, a 202 at 98 s,
/// and an expired receipt right after it. The first attempt may have settled long before.
#[test]
fn an_earlier_unknown_attempt_may_have_settled_and_expired() {
    let mut log = log();
    let unknown = submit(body(1, 1, 1), 100, Answer::Unknown);
    let late = Submission {
        fresh: false,
        ..accepted(1, 9_000)
    };
    log.submissions = vec![unknown, late];
    log.lookups = vec![lookup(1, 9_100, Seen::Missing)];
    assert!(kinds(&log).is_empty());
    // Seen pending in between, so it had not settled.
    log.lookups.insert(0, lookup(1, 8_900, Seen::Pending));
    assert_eq!(kinds(&log), [ACCEPTED_SETTLES]);
}

#[test]
fn a_full_stop_or_two_stopped_workers_end_the_retention_claim() {
    let mut stopped = log();
    stopped.runs[0].stopped = Some(500);
    stopped.submissions = vec![accepted(1, 100)];
    stopped.lookups = vec![lookup(1, 600, Seen::Abandoned)];
    assert!(kinds(&stopped).is_empty());

    let mut down = log();
    down.two_down = vec![500];
    down.submissions = vec![accepted(1, 100)];
    down.lookups = vec![lookup(1, 600, Seen::Missing)];
    assert!(kinds(&down).is_empty());
    // Two workers down before the acceptance say nothing about its copies.
    down.two_down = vec![50];
    assert_eq!(kinds(&down), [ACCEPTED_SETTLES]);
    // A lookup that raced the acceptance reply proves nothing.
    down.lookups = vec![lookup(1, 100, Seen::Missing)];
    assert!(kinds(&down).is_empty());
}

/// Review 132, M4: accepted mutations settled as failed with an unknown outcome in six seeds,
/// and nothing said so.
#[test]
fn a_failed_receipt_without_an_outcome_is_a_missing_outcome() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![lookup(1, 300, unknown())];
    assert_eq!(kinds(&log), [ACCEPTED_SETTLES]);
    assert_eq!(log.unsettled(RUN), [Uuid::from_u128(1)]);
    assert_eq!(log.without_outcome(RUN).len(), 1);
    assert!(log.without_outcome(NEXT).is_empty());
    // The same identity is answered definitely later.
    log.submissions
        .push(succeeded(body(1, 1, 1), 400, json!(null)));
    assert!(kinds(&log).is_empty());
    // Before it, that says nothing.
    log.submissions[1].completed = 250;
    assert_eq!(kinds(&log), [ACCEPTED_SETTLES]);
    log.submissions.pop();
    // A full stop ends the claim.
    log.runs[0].stopped = Some(200);
    assert!(kinds(&log).is_empty());
    log.runs[0].stopped = None;
    // A definite failure is an outcome.
    log.lookups = vec![lookup(
        1,
        300,
        Seen::Terminal(Terminal::failed("storage_unavailable")),
    )];
    assert!(kinds(&log).is_empty());
    assert!(log.unsettled(RUN).is_empty());
    // A read changes nothing, so its failure hides nothing.
    log.submissions[0].body.mutation = false;
    log.lookups = vec![lookup(1, 300, unknown())];
    assert!(kinds(&log).is_empty());
    assert!(log.unsettled(RUN).is_empty());
}

#[test]
fn no_acknowledgment_while_a_ready_worker_cannot_install_a_copy() {
    let mut log = log();
    log.frozen = vec![frozen(&[1, 2], None)];
    log.submissions = vec![
        accepted(1, 1_995),
        submit(body(2, 1, 2), 1_500, Answer::Unknown),
    ];
    assert!(kinds(&log).is_empty(), "ended after the window, or unknown");
    assert!(log.judge().judged[RETENTION] > 0);
    log.submissions = vec![accepted(1, 1_500)];
    assert_eq!(kinds(&log), [RETENTION]);
    // Review 132, M2: one frozen worker leaves the copies as the only obstacle.
    log.frozen = vec![frozen(&[1], Some(3))];
    assert_eq!(kinds(&log), [RETENTION]);
    // It may have left the ready set, or nothing is known about it.
    for ready in [Some(2), None] {
        log.frozen = vec![frozen(&[1], ready)];
        assert!(kinds(&log).is_empty());
        assert!(!log.judge().judged.contains_key(RETENTION));
    }
    // Two of three scoped copies are enough.
    log.frozen = vec![frozen(&[1], Some(3))];
    log.scoped = true;
    assert!(kinds(&log).is_empty());
    log.frozen = vec![frozen(&[1, 2], None)];
    assert_eq!(kinds(&log), [RETENTION]);
    log.scoped = false;
    // A retry attaches to a request that may have been replicated before.
    log.submissions[0].fresh = false;
    assert!(kinds(&log).is_empty());
    // A direct write is judged by the direct rules.
    log.submissions = vec![direct(body(3, 1, 3), 1_500, json!(null))];
    assert!(kinds(&log).is_empty());
}

/// Review 132, M1: 60 old records made up for 12 requests that the worker never received.
#[test]
fn a_holder_has_every_new_record_beyond_the_old_ones() {
    let mut log = log();
    let held = |before, after| Retained {
        node: 2,
        holder: Holder::Ready,
        required: 3,
        before,
        after,
        pending: 0,
        at: 900,
    };
    log.retained = vec![held(0, 3), held(60, 63), held(60, 67)];
    assert!(kinds(&log).is_empty());
    for (before, after) in [(0, 2), (60, 60), (60, 62), (60, 12)] {
        log.retained = vec![held(before, after)];
        assert_eq!(kinds(&log), [RETENTION], "{before} to {after}");
    }
    // Nothing was accepted, so nothing is compared.
    log.retained[0].required = 0;
    assert!(kinds(&log).is_empty());
    assert!(!log.judge().judged.contains_key(RETENTION));
}

#[test]
fn a_new_run_starts_without_pending_records() {
    let mut log = log();
    let fresh = |pending_records| Fresh {
        node: 1,
        run: NEXT,
        pending_records,
        at: 900,
    };
    log.fresh_runs = vec![fresh(0)];
    assert!(kinds(&log).is_empty());
    log.fresh_runs = vec![fresh(1)];
    assert_eq!(kinds(&log), [RETENTION]);
}

#[test]
fn an_abandoned_mutation_has_no_effect() {
    let mut log = log();
    log.runs[0].stopped = Some(500);
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![lookup(1, 900, Seen::Abandoned)];
    log.reads = vec![read(1, Some(7), 1_000), read(2, Some(1), 1_000)];
    assert!(kinds(&log).is_empty(), "other values and other keys");
    log.reads = vec![read(1, Some(1), 1_000)];
    assert_eq!(kinds(&log), [RETENTION]);
    // Visible before the stop counts as well: the decision was durable then.
    log.reads = vec![read(1, Some(1), 300)];
    assert_eq!(kinds(&log), [RETENTION]);
    log.reads.clear();
    log.submissions
        .push(succeeded(body(1, 1, 1), 200, json!(null)));
    assert_eq!(kinds(&log), [RETENTION]);
    // So does a receipt that reported success before the stop.
    log.submissions.pop();
    log.lookups.insert(0, lookup(1, 300, ok(json!(true))));
    assert_eq!(kinds(&log), [RETENTION]);
    // A success that changed no row decided nothing durable.
    log.lookups[0] = lookup(1, 300, ok(json!(false)));
    assert!(kinds(&log).is_empty());
}

#[test]
fn one_identity_has_one_result_and_one_body() {
    let mut legal = log();
    legal.submissions = vec![
        accepted(1, 100),
        succeeded(body(1, 1, 1), 200, json!(null)),
        // Another body is refused.
        submit(body(1, 1, 10), 300, Answer::Terminal(refused())),
    ];
    legal.lookups = vec![lookup(1, 400, ok(json!(null)))];
    assert!(kinds(&legal).is_empty(), "{:?}", legal.judge());
    assert!(legal.judge().judged[ONE_EXECUTION] > 0);

    // Two bodies under one identity, on two keys of the queued path.
    let mut reused = log();
    reused.submissions = vec![
        succeeded(body(1, 1, 10), 100, json!(null)),
        succeeded(body(1, 2, 20), 200, json!(null)),
    ];
    assert_eq!(kinds(&reused), [ONE_EXECUTION]);

    // One body, two results.
    let mut replayed = log();
    replayed.submissions = vec![
        succeeded(body(1, 1, 10), 100, json!(true)),
        succeeded(body(1, 1, 10), 200, json!(false)),
    ];
    assert_eq!(kinds(&replayed), [ONE_EXECUTION]);

    // The receipt and the synchronous answer are one result.
    let mut receipt = log();
    receipt.submissions = vec![accepted(1, 100), succeeded(body(1, 1, 1), 200, json!(true))];
    receipt.lookups = vec![lookup(1, 300, ok(json!(null)))];
    assert_eq!(kinds(&receipt), [ONE_EXECUTION]);

    // A success, and a conflict for the same body.
    let mut refused_retry = log();
    refused_retry.submissions = vec![
        succeeded(body(1, 1, 10), 100, json!(null)),
        submit(body(1, 1, 10), 200, Answer::Terminal(refused())),
    ];
    assert_eq!(kinds(&refused_retry), [ONE_EXECUTION]);
}

/// Review 132, M6.
#[test]
fn two_bodies_accepted_under_one_identity_are_two_executions() {
    let mut log = log();
    let other = submit(body(1, 2, 20), 300, Answer::Accepted(RUN));
    log.submissions = vec![accepted(1, 100), other];
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // The refused body was not accepted.
    log.submissions[1].answer = Answer::Terminal(refused());
    assert!(kinds(&log).is_empty());
    // Neither answer arrived, and both values are visible.
    log.submissions = vec![
        submit(body(1, 1, 1), 100, Answer::Unknown),
        submit(body(1, 2, 20), 300, Answer::Unknown),
    ];
    log.reads = vec![read(1, Some(1), 700)];
    assert!(kinds(&log).is_empty(), "one of them ran");
    assert!(log.judge().judged[ONE_EXECUTION] > 0);
    log.reads.push(read(2, Some(20), 700));
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // On the direct path another key is another identity.
    log.submissions[1].route = Route::MaybeDirect;
    assert!(kinds(&log).is_empty());
    // The same key is not.
    log.submissions[1].body = body(1, 1, 20);
    log.reads[1].key = 1;
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // Two runs: the first body may have been abandoned.
    log.reads.clear();
    log.submissions = vec![
        accepted(1, 100),
        submit(body(1, 2, 20), 300, Answer::Accepted(NEXT)),
    ];
    assert!(kinds(&log).is_empty());
}

/// Review 132, M8: OLTP-G03's second sentence. The product does not do this today (DR-1).
#[test]
fn a_direct_retry_returns_the_first_definite_result() {
    let answers = |first: Terminal, second: Terminal| {
        let mut log = log();
        log.submissions = [(100, first), (200, second)]
            .map(|(at, terminal)| Submission {
                route: Route::MaybeDirect,
                asynchronous: false,
                ..submit(body(1, 1, 10), at, Answer::Terminal(terminal))
            })
            .to_vec();
        log
    };
    let done = |value| Terminal::Succeeded(value);
    for same in [done(json!(false)), done(json!(true)), refused()] {
        let log = answers(same.clone(), same);
        assert!(kinds(&log).is_empty());
        assert_eq!(log.judge().judged[FIRST_RESULT], 1);
    }
    let contradicted = [
        (done(json!(false)), done(json!(true))),
        (done(json!(true)), done(json!(false))),
        (done(json!(false)), refused()),
        (refused(), done(json!(false))),
    ];
    for (first, second) in contradicted {
        let log = answers(first, second);
        assert_eq!(kinds(&log), [FIRST_RESULT], "{log:?}");
    }
    // A committed result and a conflict are one execution's rule.
    for (first, second) in [
        (done(json!(true)), refused()),
        (refused(), done(json!(null))),
    ] {
        assert_eq!(kinds(&answers(first, second)), [ONE_EXECUTION]);
    }
    // No sentence covers a no-op that the other path answers again (see NOT_RUN).
    let mut crossed = answers(done(json!(false)), done(json!(true)));
    crossed.submissions[1].route = Route::Queued;
    assert!(kinds(&crossed).is_empty());
    // A refusal or an unknown answer is not a result.
    let mut refusal = answers(done(json!(true)), done(json!(true)));
    refusal.submissions[0].answer = Answer::Refused("overloaded".into());
    assert!(kinds(&refusal).is_empty());
    assert!(!refusal.judge().judged.contains_key(FIRST_RESULT));
}

/// Review 132: one success and one read counted as two comparisons.
#[test]
fn judged_counts_comparisons() {
    let mut log = log();
    log.submissions = vec![succeeded(body(1, 1, 10), 100, json!(null))];
    log.reads = vec![read(1, Some(10), 200)];
    assert!(log.judge().judged.is_empty());
    log.reads.push(read(1, Some(11), 300));
    assert_eq!(log.judge().judged[ONE_EXECUTION], 1);
}

/// Review 132, M7: `abandon` passed no jumps, so the slack was 3 s whatever was jumped.
#[test]
fn the_slack_of_every_clock_jump_counts() {
    let mut log = log();
    log.retention_ms = 8_000;
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![
        lookup(1, 200, ok(json!(null))),
        lookup(1, 4_900, Seen::Missing),
    ];
    log.clock_slack_ms = clock_slack(4);
    assert!(kinds(&log).is_empty());
    log.clock_slack_ms = clock_slack(0);
    assert_eq!(kinds(&log), [ACCEPTED_SETTLES]);
}

#[test]
fn the_path_boundary_keys_identity_by_table_and_key() {
    let queued = succeeded(body(1, 1, 10), 100, json!(null));
    let mut log = log();
    // Another key on the direct path is another identity (D03).
    log.submissions = vec![queued.clone(), direct(body(1, 2, 20), 200, json!(null))];
    assert!(kinds(&log).is_empty());
    log.submissions = vec![queued.clone(), direct(body(1, 1, 20), 200, json!(null))];
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // A direct answer that changed no row leaves no record.
    log.submissions = vec![queued, direct(body(1, 1, 20), 200, json!(false))];
    assert!(kinds(&log).is_empty());
    // Reads are outside the mutation rule.
    let mut get = body(1, 1, 0);
    get.mutation = false;
    get.writes.clear();
    log.submissions = vec![
        succeeded(get.clone(), 100, json!(1)),
        succeeded(get, 200, json!(2)),
    ];
    assert!(kinds(&log).is_empty());
}

#[test]
fn a_replaced_value_never_comes_back() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.reads = vec![
        read(1, Some(1), 200),
        read(1, Some(9), 300),
        read(1, None, 400),
    ];
    assert!(kinds(&log).is_empty());
    log.reads.push(read(1, Some(1), 500));
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // Absent and back is an execution too.
    log.reads = vec![
        read(1, Some(1), 200),
        read(1, None, 300),
        read(1, Some(1), 400),
    ];
    assert_eq!(kinds(&log), [ONE_EXECUTION]);
    // Overlapping reads have no order.
    log.reads = vec![
        read(1, Some(1), 200),
        read(1, Some(9), 205),
        read(1, Some(1), 212),
    ];
    assert!(kinds(&log).is_empty());
    // A value nobody in the log wrote is not judged here.
    log.submissions.clear();
    log.reads = vec![
        read(1, Some(1), 200),
        read(1, None, 300),
        read(1, Some(1), 400),
    ];
    assert!(kinds(&log).is_empty());
}

#[test]
fn receipts_never_change_or_appear_from_nothing() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![
        lookup(1, 200, ok(json!(true))),
        lookup(1, 300, ok(json!(true))),
    ];
    assert!(kinds(&log).is_empty());
    assert!(log.judge().judged[RECEIPTS] > 0);
    log.lookups.push(lookup(1, 400, Seen::Terminal(refused())));
    assert_eq!(kinds(&log), [RECEIPTS]);

    log.submissions.clear();
    log.lookups = vec![lookup(5, 200, Seen::Missing)];
    log.never_submitted = BTreeSet::from([Uuid::from_u128(5)]);
    assert!(kinds(&log).is_empty());
    log.lookups = vec![lookup(5, 200, ok(json!(null)))];
    assert_eq!(kinds(&log), [RECEIPTS]);
}

#[test]
fn pending_after_terminal_is_reported_not_judged() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![
        lookup(1, 200, ok(json!(null))),
        lookup(1, 300, Seen::Pending),
    ];
    assert!(kinds(&log).is_empty());
    assert_eq!(log.regressions(), 1);
}

#[test]
fn open_across_needs_a_pending_receipt_after_the_fault() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![
        lookup(1, 300, Seen::Pending),
        lookup(1, 500, ok(json!(null))),
    ];
    assert!(log.open_across(300).is_some());
    assert!(log.open_across(50).is_none(), "accepted after the fault");
    assert!(log.open_across(301).is_none(), "looked up before the fault");
    log.lookups.pop();
    assert!(log.open_across(300).is_none(), "never seen terminal");
    // Review 132: looked up for the first time long after the fault.
    log.lookups = vec![lookup(1, 90_000, ok(json!(null)))];
    assert!(log.open_across(60_000).is_none());
}

#[test]
fn unsettled_lists_only_accepted_ids_without_a_definite_receipt() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100), accepted(2, 100), accepted(3, 100)];
    log.lookups = vec![
        lookup(1, 200, Seen::Pending),
        lookup(2, 200, ok(json!(null))),
        lookup(3, 200, Seen::Unknown),
    ];
    assert_eq!(log.unsettled(RUN), [Uuid::from_u128(1), Uuid::from_u128(3)]);
    assert!(log.unsettled(NEXT).is_empty());
}

#[test]
fn every_rule_has_a_name_and_every_gap_a_reason() {
    assert_eq!(RULES.iter().collect::<BTreeSet<_>>().len(), RULES.len());
    for (name, why) in NOT_RUN {
        assert!(!name.is_empty() && !why.is_empty());
    }
}
