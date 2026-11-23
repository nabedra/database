//! Known-bad observations for each invariant. The first test is the Events simulator's
//! `kafka_invariants_flag_known_bad_observations`, ported to the generic types.

use crate::raft::{LeaderId, LogId, MAX_VIOLATIONS, RaftObserver, Vote};

const G: &str = "p0";

fn has(o: &RaftObserver, name: &str) -> bool {
    o.violations().iter().any(|v| v.contains(name))
}

#[test]
fn kafka_invariants_flag_known_bad_observations() {
    let same_term = Vote::new_committed(3, 1);
    let mut o = RaftObserver::default();
    o.observe(G, 1, same_term, true, Some(1), 0);
    o.observe(G, 2, same_term, true, Some(1), 0);
    assert!(has(&o, "SingleLeader"), "{:?}", o.violations());

    let mut o = RaftObserver::default();
    o.observe(G, 1, same_term, true, Some(1), 0);
    o.observe(G, 2, Vote::new_committed(3, 2), true, Some(1), 0);
    assert!(o.violations().is_empty(), "{:?}", o.violations());

    let mut o = RaftObserver::default();
    o.observe(G, 1, Vote::new(5, 2), false, None, 0);
    o.observe(G, 1, Vote::new(5, 1), false, None, 0);
    assert!(has(&o, "MonotonicEpoch"), "{:?}", o.violations());

    let mut o = RaftObserver::default();
    o.observe(G, 1, same_term, false, Some(9), 0);
    o.observe(G, 1, same_term, false, Some(8), 0);
    assert!(has(&o, "MonotonicHighWatermark"), "{:?}", o.violations());

    let first = LogId::new(1, 1, 1);
    let next = LogId::new(1, 1, 2);
    let conflict = LogId::new(1, 2, 1);
    let mut o = RaftObserver::new(3);
    o.appended(G, 1, first);
    o.appended(G, 2, first);
    o.applied(G, 1, first, 0xaa, 0);
    o.applied(G, 2, first, 0xbb, 0);
    assert!(has(&o, "AppliedPrefixesAgree"), "{:?}", o.violations());

    let mut o = RaftObserver::new(3);
    o.appended(G, 1, first);
    o.appended(G, 2, conflict);
    o.applied(G, 1, first, 0xaa, 0);
    // The next entry reaches a majority before the next metrics sample.
    o.appended(G, 1, next);
    o.appended(G, 2, next);
    o.applied(G, 1, next, 0xaa, 0);
    assert!(
        has(&o, "MajorityReachedHighWatermark"),
        "{:?}",
        o.violations()
    );

    let mut o = RaftObserver::new(3);
    o.appended(G, 1, first);
    o.appended(G, 2, first);
    o.applied(G, 1, first, 0xaa, 0);
    assert_eq!(o.violations(), Vec::<String>::new());
}

#[test]
fn monotonic_commit_holds_within_an_incarnation_only() {
    let mut o = RaftObserver::new(1);
    let entry = LogId::new(1, 1, 4);
    o.appended(G, 1, entry);
    o.applied(G, 1, entry, 1, 0);
    o.applied(G, 1, entry, 1, 5);
    assert!(has(&o, "MonotonicCommit"));

    let mut o = RaftObserver::new(1);
    o.appended(G, 1, entry);
    o.applied(G, 1, entry, 1, 0);
    o.observe(G, 1, Vote::new_committed(1, 1), true, Some(4), 0);
    // A restart replays from its snapshot: lower indexes are fine in a new incarnation.
    o.restarted(G, 1, [LogId::new(1, 1, 1), entry]);
    o.applied(G, 1, LogId::new(1, 1, 1), 7, 10);
    o.observe(G, 1, Vote::new_committed(1, 1), true, Some(1), 10);
    assert_eq!(o.violations(), Vec::<String>::new());
}

#[test]
fn truncation_and_restart_rebuild_the_durable_log() {
    let mut o = RaftObserver::new(3);
    let entry = LogId::new(2, 1, 7);
    for node in [1, 2] {
        o.appended(G, node, entry);
    }
    o.truncated(G, 2, 7);
    o.applied(G, 1, entry, 3, 0);
    assert!(has(&o, "MajorityReachedHighWatermark"));

    let mut o = RaftObserver::new(3);
    o.appended(G, 1, entry);
    o.restarted(G, 2, [entry]);
    o.applied(G, 1, entry, 3, 0);
    o.check_majorities(5);
    assert_eq!(o.violations(), Vec::<String>::new());
    // A replica that loses the entry on restart breaks the majority at the next check.
    o.restarted(G, 2, []);
    o.check_majority(G, 6);
    assert!(has(&o, "MajorityReachedHighWatermark"));
}

#[test]
fn groups_are_independent_and_have_their_own_voters() {
    let mut o = RaftObserver::new(3);
    o.set_voters("data-1", 5);
    let entry = LogId::new(1, 1, 1);
    // Index 1 of two groups carries different payloads: no conflict across groups.
    for group in ["control", "data-1"] {
        o.appended(group, 1, entry);
        o.appended(group, 2, entry);
    }
    o.applied("control", 1, entry, 1, 0);
    assert_eq!(o.violations(), Vec::<String>::new());
    // Two of five is not a majority.
    o.applied("data-1", 1, entry, 2, 0);
    assert_eq!(o.violations().len(), 1);
    assert!(o.violations()[0].contains("MajorityReachedHighWatermark: data-1"));
    o.observe("control", 1, Vote::new(4, 1), false, None, 0);
    o.observe("data-1", 1, Vote::new(2, 1), false, None, 0);
    assert_eq!(o.violations().len(), 1, "votes are per group");
    assert_eq!(o.groups().collect::<Vec<_>>(), ["control", "data-1"]);
    assert_eq!(o.committed_len("control"), 1);
    assert_eq!(o.committed_len("none"), 0);
}

#[test]
fn election_safety_flags_two_leaders_under_one_vote() {
    let mut o = RaftObserver::new(3);
    let vote = Vote::new_committed(7, 2);
    o.leader_seen(G, 2, vote, 0);
    o.leader_seen(G, 2, vote, 1);
    assert!(o.violations().is_empty());
    o.leader_seen(G, 3, vote, 2);
    assert!(has(&o, "ElectionSafety"), "{:?}", o.violations());
}

#[test]
fn violations_are_reported_once_stamped_and_capped() {
    let mut o = RaftObserver::new(3);
    o.violate(12, "Liveness: stuck".into());
    o.violate(13, "Liveness: stuck".into());
    assert_eq!(o.violations(), ["t=12ms Liveness: stuck"]);
    for i in 0..100 {
        o.violate(i, format!("X: {i}"));
    }
    assert_eq!(o.violations().len(), MAX_VIOLATIONS);
    assert_eq!(o.take_violations().len(), MAX_VIOLATIONS);
    assert!(o.violations().is_empty());
}

#[test]
fn ids_order_and_print_like_openraft() {
    assert!(Vote::new(5, 1) < Vote::new(5, 2));
    assert!(Vote::new(5, 2) < Vote::new_committed(5, 2));
    assert!(Vote::new_committed(5, 9) < Vote::new(6, 1));
    assert_eq!(Vote::new_committed(3, 1).to_string(), "T3-N1:committed");
    assert_eq!(Vote::new(3, 1).to_string(), "T3-N1:uncommitted");
    assert_eq!(LogId::new(1, 2, 5).to_string(), "T1-N2-5");
    assert_eq!(LeaderId::new(4, 3).to_string(), "T4-N3");
    assert_eq!(Vote::default(), Vote::new(0, 0));
}
