//! Legal and illegal fixtures of each membership rule. A rule that accepts an illegal fixture,
//! or refuses a legal one, is wrong whatever a sweep says.

use serde_json::json;

use super::membership::*;
use uuid::Uuid;

const GROUP: Uuid = Uuid::from_u128(7);
const OTHER: Uuid = Uuid::from_u128(8);

fn voters(ids: &[u64]) -> std::collections::BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn change(remove: u64, add: u64) -> NamedChange {
    NamedChange {
        group: GROUP,
        remove,
        add,
    }
}

fn retired(named: NamedChange, old: &[u64], next: &[u64]) -> Certified {
    Certified::Retired {
        named,
        old: voters(old),
        next: voters(next),
    }
}

mod wiped_never_votes {
    use super::*;

    /// Three voters that voted from their first storage.
    fn rule() -> WipedNeverVotes {
        let mut rule = WipedNeverVotes::default();
        for replica in 1..=3 {
            rule.voted(GROUP, replica, 0).unwrap();
        }
        rule
    }

    #[test]
    fn legal_a_restart_keeps_its_storage_and_votes() {
        let mut rule = rule();
        rule.voted(GROUP, 2, 0).unwrap();
        rule.committed(GROUP, &[(1, Some(0)), (2, Some(0)), (3, None)])
            .unwrap();
    }

    #[test]
    fn legal_two_retained_voters_commit_beside_a_wiped_one() {
        let mut rule = rule();
        rule.committed(GROUP, &[(1, Some(0)), (2, Some(0)), (3, Some(1))])
            .unwrap();
    }

    #[test]
    fn legal_a_replacement_votes_under_its_own_identity() {
        let mut rule = rule();
        rule.voted(GROUP, 4, 1).unwrap();
        rule.committed(GROUP, &[(1, Some(0)), (2, None), (4, Some(1))])
            .unwrap();
    }

    #[test]
    fn legal_groups_are_independent() {
        let mut rule = rule();
        rule.voted(OTHER, 3, 1).unwrap();
        rule.committed(OTHER, &[(1, None), (2, Some(0)), (3, Some(1))])
            .unwrap();
    }

    #[test]
    fn illegal_a_wiped_replica_votes_again() {
        let error = rule().voted(GROUP, 3, 1).unwrap_err();
        assert!(error.contains("replica 3"), "{error}");
    }

    #[test]
    fn illegal_a_majority_counts_a_wiped_replica() {
        let mut rule = rule();
        let error = rule
            .committed(GROUP, &[(1, None), (2, Some(0)), (3, Some(1))])
            .unwrap_err();
        assert!(error.contains("[3]"), "{error}");
    }

    #[test]
    fn illegal_no_quorum_at_all() {
        let mut rule = rule();
        assert!(
            rule.committed(GROUP, &[(1, None), (2, Some(0)), (3, None)])
                .is_err()
        );
    }
}

mod retry_certifies_named_removal {
    use super::*;

    #[test]
    fn legal_a_move_completes_and_its_retries_repeat_it() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let id = Uuid::from_u128(1);
        let request = change(3, 4);
        let done = retired(request.clone(), &[1, 2, 3], &[1, 2, 4]);
        rule.answered(id, &request, &Certified::Pending, None).unwrap();
        rule.answered(id, &request, &Certified::Refused, None).unwrap();
        rule.answered(id, &request, &done, Some(&voters(&[1, 2, 4])))
            .unwrap();
        // A second move takes the first one's replacement away, and a late retry of the first
        // still reports the first.
        let next = change(4, 5);
        rule.answered(
            Uuid::from_u128(2),
            &next,
            &retired(next.clone(), &[1, 2, 4], &[1, 2, 5]),
            Some(&voters(&[1, 2, 5])),
        )
        .unwrap();
        rule.answered(id, &request, &done, Some(&voters(&[1, 2, 5])))
            .unwrap();
        rule.published(GROUP, &voters(&[1, 2, 5])).unwrap();
    }

    #[test]
    fn legal_another_body_under_the_id_is_refused() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let id = Uuid::from_u128(1);
        rule.answered(id, &change(3, 4), &Certified::Pending, None)
            .unwrap();
        rule.answered(id, &change(2, 4), &Certified::Refused, None)
            .unwrap();
    }

    #[test]
    fn illegal_the_answer_names_another_voter() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let answer = retired(change(2, 4), &[1, 2, 3], &[1, 3, 4]);
        let error = rule
            .answered(Uuid::from_u128(1), &change(3, 4), &answer, None)
            .unwrap_err();
        assert!(error.contains("certifies"), "{error}");
    }

    #[test]
    fn illegal_another_voter_left() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let request = change(3, 4);
        let answer = retired(request.clone(), &[1, 2, 3], &[1, 3, 4]);
        assert!(
            rule.answered(Uuid::from_u128(1), &request, &answer, None)
                .is_err()
        );
    }

    #[test]
    fn illegal_the_named_voter_was_no_member() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let request = change(9, 4);
        let answer = retired(request.clone(), &[1, 2, 3], &[1, 2, 3]);
        assert!(
            rule.answered(Uuid::from_u128(1), &request, &answer, None)
                .is_err()
        );
    }

    #[test]
    fn illegal_the_removed_voter_is_still_published() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let request = change(3, 4);
        let answer = retired(request.clone(), &[1, 2, 3], &[1, 2, 4]);
        let error = rule
            .answered(
                Uuid::from_u128(1),
                &request,
                &answer,
                Some(&voters(&[1, 2, 3])),
            )
            .unwrap_err();
        assert!(error.contains("still published"), "{error}");
        assert!(rule.published(GROUP, &voters(&[2, 3, 4])).is_err());
    }

    #[test]
    fn illegal_a_retry_certifies_another_change() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let id = Uuid::from_u128(1);
        let request = change(3, 4);
        rule.answered(
            id,
            &request,
            &retired(request.clone(), &[1, 2, 3], &[1, 2, 4]),
            None,
        )
        .unwrap();
        let error = rule
            .answered(
                id,
                &request,
                &retired(request.clone(), &[2, 3, 5], &[2, 4, 5]),
                None,
            )
            .unwrap_err();
        assert!(error.contains("its retry"), "{error}");
    }

    #[test]
    fn illegal_another_body_under_the_id_is_certified() {
        let mut rule = RetryCertifiesNamedRemoval::default();
        let id = Uuid::from_u128(1);
        rule.answered(id, &change(3, 4), &Certified::Pending, None)
            .unwrap();
        let other = change(2, 4);
        let answer = retired(other.clone(), &[1, 2, 3], &[1, 3, 4]);
        assert!(rule.answered(id, &other, &answer, None).is_err());
    }
}

mod required_voters_rule {
    use super::*;

    #[test]
    fn legal_three_voters_on_three_workers() {
        let members = [(1, "a"), (2, "b"), (4, "c")]
            .map(|(node, worker)| json!({"node_id": node, "worker_id": worker}));
        required_voters(3, &members).unwrap();
    }

    #[test]
    fn illegal_a_lost_a_doubled_and_a_shared_voter() {
        for members in [
            vec![(1, "a"), (2, "b")],
            vec![(1, "a"), (2, "b"), (2, "c")],
            vec![(1, "a"), (2, "b"), (3, "b")],
            vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")],
        ] {
            let members: Vec<_> = members
                .into_iter()
                .map(|(node, worker)| json!({"node_id": node, "worker_id": worker}))
                .collect();
            assert!(required_voters(3, &members).is_err(), "{members:?}");
        }
    }
}

mod capability_before_use {
    use super::*;

    #[test]
    fn legal_use_after_every_voter_ran() {
        let mut rule = CapabilityBeforeUse::default();
        for worker in 1..=3 {
            rule.inactive(Capability::Direct).unwrap();
            rule.started(worker);
        }
        rule.in_use(Capability::Direct, "active", &[1, 2, 3])
            .unwrap();
        // Another capability activates on its own.
        rule.inactive(Capability::Batch).unwrap();
        // A spare that is not enrolled says nothing about the voters.
        rule.in_use(Capability::Direct, "applied", &[1, 2, 3])
            .unwrap();
    }

    #[test]
    fn legal_a_replacement_that_ran_before_it_is_a_voter() {
        let mut rule = CapabilityBeforeUse::default();
        for worker in [1, 2, 3, 4] {
            rule.started(worker);
        }
        rule.in_use(Capability::Batch, "active", &[1, 2, 4]).unwrap();
    }

    #[test]
    fn illegal_use_while_a_voter_lacks_it() {
        let mut rule = CapabilityBeforeUse::default();
        rule.started(1);
        rule.started(2);
        let error = rule
            .in_use(Capability::Direct, "active", &[1, 2, 3])
            .unwrap_err();
        assert!(error.contains("[3]"), "{error}");
    }

    #[test]
    fn illegal_an_activation_reverts() {
        let mut rule = CapabilityBeforeUse::default();
        rule.started(1);
        rule.in_use(Capability::Direct, "active", &[1]).unwrap();
        assert!(rule.inactive(Capability::Direct).is_err());
    }
}

mod queue_handoff {
    use super::*;

    const RUN: Uuid = Uuid::from_u128(9);
    const REQUEST: Uuid = Uuid::from_u128(10);

    #[test]
    fn legal_an_accepted_request_stays_until_it_succeeds() {
        let mut rule = QueueHandoff::new(RUN);
        rule.accepted(REQUEST);
        rule.run(RUN).unwrap();
        rule.receipt(REQUEST, Receipt::Outstanding).unwrap();
        assert_eq!(rule.outstanding(), [REQUEST]);
        rule.receipt(REQUEST, Receipt::Succeeded).unwrap();
        rule.receipt(REQUEST, Receipt::Succeeded).unwrap();
        assert!(rule.outstanding().is_empty());
        let refused = Uuid::from_u128(13);
        rule.accepted(refused);
        rule.receipt(refused, Receipt::Failed).unwrap();
    }

    #[test]
    fn illegal_an_accepted_request_is_abandoned() {
        let mut rule = QueueHandoff::new(RUN);
        rule.accepted(REQUEST);
        let error = rule
            .receipt(REQUEST, Receipt::Abandoned)
            .unwrap_err();
        assert!(error.contains("abandoned"), "{error}");
    }

    #[test]
    fn illegal_a_result_is_forgotten_the_run_changes_or_a_stranger_appears() {
        let mut rule = QueueHandoff::new(RUN);
        rule.accepted(REQUEST);
        rule.receipt(REQUEST, Receipt::Succeeded).unwrap();
        assert!(rule.receipt(REQUEST, Receipt::Outstanding).is_err());
        assert!(rule.run(Uuid::from_u128(11)).is_err());
        assert!(
            rule.receipt(Uuid::from_u128(12), Receipt::Outstanding)
                .is_err()
        );
    }
}
