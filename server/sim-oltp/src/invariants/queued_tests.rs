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
        invoked: at,
        completed: at + 10,
        seen,
    }
}

fn ok(value: Value) -> Seen {
    Seen::Terminal(Terminal::Succeeded(value))
}

fn refused() -> Terminal {
    Terminal::Failed("conflict".into())
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

#[test]
fn no_acknowledgment_while_two_workers_are_frozen() {
    let mut log = log();
    log.two_frozen = vec![(1_000, 2_000)];
    log.submissions = vec![
        accepted(1, 1_995),
        submit(body(2, 1, 2), 1_500, Answer::Unknown),
    ];
    assert!(kinds(&log).is_empty(), "ended after the window, or unknown");
    assert!(log.judge().judged[RETENTION] > 0);
    log.submissions = vec![accepted(1, 1_500)];
    assert_eq!(kinds(&log), [RETENTION]);
    // A retry attaches to a request that may have been replicated before.
    log.submissions[0].fresh = false;
    assert!(kinds(&log).is_empty());
    // A direct write is judged by the direct rules.
    log.submissions = vec![direct(body(3, 1, 3), 1_500, json!(null))];
    assert!(kinds(&log).is_empty());
}

#[test]
fn a_holder_keeps_every_outstanding_record() {
    let mut log = log();
    let held = |accepted_records| Retained {
        node: 2,
        required: 3,
        accepted_records,
        at: 900,
    };
    log.retained = vec![held(3), held(7)];
    assert!(kinds(&log).is_empty());
    log.retained = vec![held(2)];
    assert_eq!(kinds(&log), [RETENTION]);
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
fn open_across_needs_a_terminal_receipt_after_the_fault() {
    let mut log = log();
    log.submissions = vec![accepted(1, 100)];
    log.lookups = vec![
        lookup(1, 200, Seen::Pending),
        lookup(1, 500, ok(json!(null))),
    ];
    assert!(log.open_across(300));
    assert!(!log.open_across(50), "sent after the fault");
    assert!(!log.open_across(600), "settled before the fault");
    log.lookups.pop();
    assert!(!log.open_across(300), "never seen terminal");
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
