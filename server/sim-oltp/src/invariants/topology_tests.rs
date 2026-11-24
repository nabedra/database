//! Legal and illegal observations for every topology rule.

use super::*;

const NONE: [&str; 0] = [];

fn write(key: i64, value: Option<i64>, insert: bool, at: (u64, u64), ended: Ended) -> Write {
    Write {
        key,
        value,
        insert,
        invoked: at.0,
        completed: at.1,
        ended,
    }
}

fn insert(key: i64, value: i64, invoked: u64, completed: u64) -> Write {
    write(key, Some(value), true, (invoked, completed), Ended::Acked)
}

fn update(key: i64, value: i64, invoked: u64, completed: u64) -> Write {
    write(key, Some(value), false, (invoked, completed), Ended::Acked)
}

fn delete(key: i64, invoked: u64, completed: u64) -> Write {
    write(key, None, false, (invoked, completed), Ended::Acked)
}

fn scan(invoked: u64, completed: u64, rows: &[(i64, i64)]) -> Scan {
    Scan {
        invoked,
        completed,
        rows: rows.to_vec(),
    }
}

fn kinds(writes: &[Write], scans: &[Scan]) -> Vec<&'static str> {
    audit_scans(writes, scans)
        .into_iter()
        .map(|(kind, _)| kind)
        .collect()
}

#[test]
fn covering_index_complete() {
    let acked = [insert(1, 10, 1, 2)];
    assert_eq!(kinds(&acked, &[scan(3, 4, &[(1, 10)])]), NONE);
    assert_eq!(kinds(&acked, &[scan(3, 4, &[])]), [COVERING]);
    // Another row does not stand in for it.
    let two = [insert(1, 10, 1, 2), insert(2, 20, 1, 2)];
    assert_eq!(kinds(&two, &[scan(3, 4, &[(2, 20)])]), [COVERING]);
    // The scan began before the acknowledgment, so its snapshot may be earlier.
    assert_eq!(kinds(&acked, &[scan(0, 4, &[])]), NONE);
    // The row may already have moved on when a later write overlaps the scan.
    let moving = [insert(1, 10, 1, 2), update(1, 11, 3, 9)];
    assert_eq!(kinds(&moving, &[scan(4, 5, &[(1, 11)])]), NONE);
    assert_eq!(kinds(&moving, &[scan(4, 5, &[(1, 10)])]), NONE);
    // A refused later write leaves the insert in place.
    let refused = [
        insert(1, 10, 1, 2),
        write(1, Some(11), false, (3, 4), Ended::Refused),
    ];
    assert_eq!(kinds(&refused, &[scan(5, 6, &[(1, 10)])]), NONE);
    assert_eq!(kinds(&refused, &[scan(5, 6, &[])]), [COVERING]);
    // Neither a refused nor an unanswered insert has to appear.
    for ended in [Ended::Refused, Ended::Unknown] {
        let open = [write(1, Some(10), true, (1, 2), ended)];
        assert_eq!(kinds(&open, &[scan(3, 4, &[])]), NONE);
    }
    // A deleted row is gone.
    let gone = [insert(1, 10, 1, 2), delete(1, 3, 4)];
    assert_eq!(kinds(&gone, &[scan(5, 6, &[])]), NONE);
}

#[test]
fn index_exactly_once() {
    let updated = [insert(1, 10, 1, 2), update(1, 11, 3, 4)];
    assert_eq!(kinds(&updated, &[scan(5, 6, &[(1, 11)])]), NONE);
    // Both entries, the stale entry alone, and no entry.
    assert_eq!(
        kinds(&updated, &[scan(5, 6, &[(1, 10), (1, 11)])]),
        [EXACTLY_ONCE, EXACTLY_ONCE]
    );
    assert_eq!(
        kinds(&updated, &[scan(5, 6, &[(1, 10)])]),
        [EXACTLY_ONCE, EXACTLY_ONCE]
    );
    assert_eq!(kinds(&updated, &[scan(5, 6, &[])]), [EXACTLY_ONCE]);
    // The same entry twice.
    let once = [insert(1, 10, 1, 2)];
    assert_eq!(
        kinds(&once, &[scan(3, 4, &[(1, 10), (1, 10)])]),
        [EXACTLY_ONCE]
    );
    // An entry of a deleted row.
    let gone = [insert(1, 10, 1, 2), delete(1, 3, 4)];
    assert_eq!(kinds(&gone, &[scan(5, 6, &[(1, 10)])]), [EXACTLY_ONCE]);
    // The delete overlaps the scan.
    assert_eq!(kinds(&gone, &[scan(2, 4, &[(1, 10)])]), NONE);
    // A row nobody wrote, and one whose write was sent only after the scan.
    assert_eq!(kinds(&[], &[scan(3, 4, &[(9, 9)])]), [EXACTLY_ONCE]);
    assert_eq!(kinds(&once, &[scan(0, 1, &[(1, 10)])]), [EXACTLY_ONCE]);
    // An unanswered update may land at any later time, or never.
    let open = [
        insert(1, 10, 1, 2),
        write(1, Some(11), false, (3, 4), Ended::Unknown),
    ];
    assert_eq!(kinds(&open, &[scan(5, 6, &[(1, 11)])]), NONE);
    assert_eq!(kinds(&open, &[scan(5, 6, &[(1, 10)])]), NONE);
}

#[test]
fn refused_write_in_index() {
    let refused = [
        insert(1, 10, 1, 2),
        write(1, Some(11), false, (3, 4), Ended::Refused),
    ];
    // Reported as what it is, and not as a missing insert.
    assert_eq!(
        kinds(&refused, &[scan(5, 6, &[(1, 11)])]),
        [REFUSED_WRITE, REFUSED_WRITE]
    );
    // A refused delete that removed the row.
    let deleted = [
        insert(1, 10, 1, 2),
        write(1, None, false, (3, 4), Ended::Refused),
    ];
    assert_eq!(kinds(&deleted, &[scan(5, 6, &[(1, 10)])]), NONE);
    assert_eq!(kinds(&deleted, &[scan(5, 6, &[])]), [REFUSED_WRITE]);
    // The same value under a write that succeeded is legal.
    assert_eq!(
        kinds(
            &[insert(1, 10, 1, 2), update(1, 11, 3, 4)],
            &[scan(5, 6, &[(1, 11)])]
        ),
        NONE
    );
}

fn status(index: &str, at: (u64, u64), state: &str) -> Status {
    Status {
        index: index.into(),
        invoked: at.0,
        completed: at.1,
        state: state.into(),
    }
}

fn chosen(index: &str, invoked: u64, completed: u64) -> Chosen {
    Chosen {
        index: index.into(),
        invoked,
        completed,
    }
}

#[test]
fn incomplete_index_chosen() {
    let created = BTreeSet::from(["a".to_owned(), "b".to_owned()]);
    let check = |statuses: &[Status], choice: Chosen| {
        incomplete_chosen(&created, statuses, &[choice]).len()
    };
    let life = [
        status("a", (1, 2), "building"),
        status("a", (5, 6), "ready"),
        status("a", (20, 21), "retired"),
    ];
    assert_eq!(check(&life, chosen("a", 7, 8)), 0);
    // Between the last building answer and the first ready one it may be ready.
    assert_eq!(check(&life, chosen("a", 3, 4)), 0);
    // The explain overlaps the retirement.
    assert_eq!(check(&life, chosen("a", 19, 22)), 0);
    // Building after the explain, retired before it, failed, and never declared.
    assert_eq!(
        check(&[status("a", (5, 6), "building")], chosen("a", 3, 4)),
        1
    );
    assert_eq!(check(&life, chosen("a", 22, 23)), 1);
    assert_eq!(
        check(&[status("b", (1, 2), "failed")], chosen("b", 3, 4)),
        1
    );
    assert_eq!(check(&life, chosen("c", 7, 8)), 1);
    // Another index's status says nothing about this one.
    assert_eq!(
        check(&[status("b", (5, 6), "building")], chosen("a", 3, 4)),
        0
    );
}

#[test]
fn dropped_table_silent() {
    let call = |invoked, success| TableCall {
        what: "table.insert".into(),
        invoked,
        success,
    };
    // Before the acknowledgment, overlapping it, and refused after it.
    let legal = [call(1, true), call(9, true), call(11, false)];
    assert!(dropped_silent(10, &legal).is_empty());
    assert_eq!(dropped_silent(10, &[call(11, true)]).len(), 1);
}

#[test]
fn stale_route_refreshes_once() {
    let refresh = |first, stale_answers| Refresh {
        gateway: 101,
        key: 1,
        first,
        stale_answers,
    };
    assert!(refreshes_once(&[refresh(true, 1), refresh(false, 0)]).is_empty());
    assert!(refreshes_once(&[refresh(true, 0)]).is_empty());
    assert_eq!(refreshes_once(&[refresh(true, 2)]).len(), 1);
    assert_eq!(refreshes_once(&[refresh(false, 1)]).len(), 1);
}

#[test]
fn refused_ddl_applied() {
    let build = Uuid::from_u128(1);
    let refused = BTreeMap::from([(build, "conflict".to_owned())]);
    assert!(refused_ddl(&refused, &BTreeSet::from([Uuid::from_u128(2)])).is_empty());
    assert_eq!(refused_ddl(&refused, &BTreeSet::from([build])).len(), 1);
}

#[test]
fn fence_public_form() {
    let rest = |schema_fallbacks| AtRest {
        table: "main",
        gateway: 101,
        writes: 1,
        schema_fallbacks,
        polled_ms: 30_000,
    };
    assert!(fence_at_rest(&[rest(0)]).is_empty());
    assert_eq!(fence_at_rest(&[rest(0), rest(1)]).len(), 1);
}

#[test]
fn route_coverage() {
    assert!(route_count("main", 0, 0, 1).is_none());
    assert!(route_count("main", 2, 0, 3).is_none());
    assert!(route_count("main", 1, 0, 1).is_some());
    assert!(route_count("main", 0, 0, 2).is_some());
    // An unanswered split may have published, and no more than that.
    assert!(route_count("main", 0, 1, 1).is_none());
    assert!(route_count("main", 0, 1, 2).is_none());
    assert!(route_count("main", 0, 1, 3).is_some());
}

#[test]
fn schema_epoch_at_commit() {
    let (table, other) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let epochs = |commits: &[(u64, u64)]| {
        let mut epochs = SchemaEpochs::default();
        // Commits can be reported before the publication they follow.
        for &(epoch, at) in commits {
            epochs.committed(table, epoch, at);
        }
        epochs.published(table, Some(1), 10);
        epochs.published(table, Some(2), 20);
        epochs.published(table, None, 30);
        epochs.published(other, Some(7), 5);
        epochs.finish().len()
    };
    assert_eq!(epochs(&[(1, 10), (1, 19), (2, 20), (2, 29)]), 0);
    // A retired epoch, an epoch used early, a dropped table and an undeclared one.
    assert_eq!(epochs(&[(1, 20)]), 1);
    assert_eq!(epochs(&[(2, 19)]), 1);
    assert_eq!(epochs(&[(2, 30)]), 1);
    assert_eq!(epochs(&[(1, 9)]), 1);
    // Another table's epoch does not count.
    assert_eq!(epochs(&[(7, 15)]), 1);
}

#[test]
fn retired_leader_silent() {
    let (range, other) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let mut leaders = RetiredLeaders::default();
    assert!(leaders.answered(range, 1, true).is_none());
    leaders.retire_replica(range, 1);
    assert!(leaders.answered(range, 1, false).is_none());
    assert!(leaders.answered(range, 2, true).is_none());
    assert!(leaders.answered(other, 1, true).is_none());
    assert!(leaders.answered(range, 1, true).is_some());
    // A split retires every replica of the source range, and not its children.
    leaders.retire_group(other);
    assert!(leaders.answered(other, 3, true).is_some());
    assert!(leaders.answered(Uuid::from_u128(3), 3, true).is_none());
}

#[test]
fn fence_after_validation() {
    let (table, group, sibling) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
    let (first, second) = (Some(Uuid::from_u128(10)), Some(Uuid::from_u128(11)));
    let started = || {
        let mut fences = DataFences::default();
        fences.published(table, first);
        fences.fenced(table, group, 1, first);
        fences.fenced(table, sibling, 1, first);
        fences
    };
    // Fenced, then published.
    let mut fences = started();
    fences.began(table);
    fences.fenced(table, group, 2, second);
    fences.fenced(table, sibling, 2, second);
    fences.published(table, second);
    assert!(fences.ended(table).is_empty());
    assert!(fences.finish().is_empty());
    // Dropped.
    fences.began(table);
    fences.fenced(table, group, 3, None);
    fences.fenced(table, sibling, 3, None);
    fences.published(table, None);
    assert!(fences.ended(table).is_empty());
    // Rejected before any fence.
    let mut fences = started();
    fences.began(table);
    assert!(fences.ended(table).is_empty());
    // Rejected after one group was fenced.
    let mut fences = started();
    fences.began(table);
    fences.fenced(table, group, 2, second);
    assert_eq!(fences.ended(table).len(), 1);
    // Published without fencing the sibling.
    let mut fences = started();
    fences.began(table);
    fences.fenced(table, group, 2, second);
    fences.published(table, second);
    assert_eq!(fences.ended(table).len(), 1);
    // Another change is still open, so nothing is judged yet. At the end it is.
    let mut fences = started();
    fences.began(table);
    fences.began(table);
    fences.fenced(table, group, 2, second);
    assert!(fences.ended(table).is_empty());
    assert_eq!(fences.ended(table).len(), 1);
    assert_eq!(fences.finish().len(), 1);
}

/// Every public rule is in the list the sweep reports, and none is also listed as not run.
#[test]
fn rules_are_listed() {
    let public = [
        COVERING,
        EXACTLY_ONCE,
        REFUSED_WRITE,
        INCOMPLETE,
        DROPPED,
        STALE_ROUTE,
        REFUSED_DDL,
        FENCE,
        ROUTES,
    ];
    assert_eq!(public, RULES);
    for rule in public {
        assert!(NOT_RUN.iter().all(|(name, _)| *name != rule));
    }
}
