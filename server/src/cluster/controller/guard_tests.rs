use super::tests::{fleet, id};
use super::*;

fn sample(start: Instant, elapsed: u64, wall: u64) -> Result<OwnershipSample, StorageError> {
    OwnershipSample::from_system_time(
        start + Duration::from_millis(elapsed),
        UNIX_EPOCH + Duration::from_millis(wall),
    )
}
fn proposal(
    base: &Control,
    change: OwnershipChange,
    start: Instant,
    wall: u64,
    idn: u128,
) -> OwnershipProposal {
    OwnershipProposal::new(base, change, id(idn), sample(start, 0, wall)).unwrap()
}
fn ack(p: &OwnershipProposal) -> Result<OwnershipEvidence, StorageError> {
    Ok(OwnershipEvidence {
        acknowledged: true,
        observation: Some(p.expected.clone().unwrap()),
    })
}
#[test]
fn boot_is_closed_and_only_ack_installs_pre_io_deadline() {
    let t = Instant::now();
    let base = fleet(0);
    let mut g = OwnershipGuard::new(id(3));
    assert_eq!(g.check(sample(t, 0, 100)), Err(StorageError::Unavailable));
    let p = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        t,
        100,
        4,
    );
    let stamp = g.begin(&p).unwrap();
    assert!(g.finish(stamp, ack(&p), sample(t, 10, 110)).unwrap());
    assert_eq!(g.check(sample(t, 11, 111)).unwrap().epoch, 1);
    assert_eq!(
        g.check(sample(t, base.timing.lease_ms, 100 + base.timing.lease_ms)),
        Err(StorageError::Unavailable)
    );
}

fn installed() -> (OwnershipGuard, Control, Instant) {
    let t = Instant::now();
    let base = fleet(0);
    let p = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        t,
        100,
        4,
    );
    let mut g = OwnershipGuard::new(id(3));
    let stamp = g.begin(&p).unwrap();
    assert!(g.finish(stamp, ack(&p), sample(t, 1, 101)).unwrap());
    (
        g,
        ownership_candidate(
            &base,
            OwnershipChange::Acquire { process_id: id(3) },
            100,
            id(4),
        )
        .unwrap(),
        t,
    )
}
#[test]
fn observations_preserve_anchor_and_reject_superseded_authorization() {
    let (mut g, mut root, t) = installed();
    let old = OwnershipObservation::new(&root).unwrap();
    let generation = g.authorization_generation();
    root.revision += 1;
    root.change_id = id(5);
    root.admin_seq = 1;
    root.admin_head = Some(id(6));
    let fresh = OwnershipObservation::new(&root).unwrap();
    g.observe(&fresh).unwrap();
    assert_eq!(g.authorization_generation(), generation);
    assert_eq!(
        g.authorize(generation, &old, sample(t, 2, 102)),
        Err(StorageError::Conflict)
    );
    assert!(g.authorize(generation, &fresh, sample(t, 2, 102)).is_ok());
    assert_eq!(
        g.check(sample(t, root.timing.lease_ms, 100 + root.timing.lease_ms)),
        Err(StorageError::Unavailable)
    );
    g.observe(&fresh).unwrap();
    assert!(g.check(sample(t, 3, 103)).is_err());
}
#[test]
fn equal_revision_integrity_and_older_reads() {
    let (mut g, mut root, t) = installed();
    let older = OwnershipObservation::new(&fleet(0)).unwrap();
    assert_eq!(g.observe(&older), Err(StorageError::Conflict));
    assert!(g.check(sample(t, 2, 102)).is_ok());
    root.scheduler.failures = 1;
    assert_eq!(
        g.observe(&OwnershipObservation::new(&root).unwrap()),
        Err(StorageError::InvalidCatalog)
    );
    assert!(g.check(sample(t, 2, 102)).is_err());
}
#[test]
fn stale_completions_cannot_clear_recovery_or_release_new_slot() {
    let (mut g, root, t) = installed();
    let owner = Owner {
        process_id: id(3),
        epoch: 1,
    };
    let p = proposal(&root, OwnershipChange::Renew(owner), t, 101, 5);
    let old = g.begin(&p).unwrap();
    assert_eq!(g.begin(&p), Err(StorageError::Conflict));
    g.invalidate().unwrap();
    let new = g.begin(&p).unwrap();
    assert!(
        !g.finish(
            old,
            Err(StorageError::Unavailable),
            Err(StorageError::LimitExceeded)
        )
        .unwrap()
    );
    assert_eq!(g.begin(&p), Err(StorageError::Conflict));
    assert!(g.finish(new, ack(&p), sample(t, 2, 103)).unwrap());
    for result in [ack(&p), Err(StorageError::Unavailable)] {
        assert!(
            !g.finish(old, result, Err(StorageError::Unavailable))
                .unwrap()
        );
        assert!(g.check(sample(t, 3, 104)).is_ok());
    }
}
#[test]
fn newer_same_tuple_observation_invalidates_pending_and_changed_tuple_revokes() {
    let (mut g, mut root, t) = installed();
    let p = proposal(
        &root,
        OwnershipChange::Renew(Owner {
            process_id: id(3),
            epoch: 1,
        }),
        t,
        101,
        5,
    );
    let stamp = g.begin(&p).unwrap();
    root.revision += 1;
    root.change_id = id(6);
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    assert!(!g.finish(stamp, ack(&p), sample(t, 2, 103)).unwrap());
    assert!(
        g.begin(&proposal(
            &root,
            OwnershipChange::Renew(Owner {
                process_id: id(3),
                epoch: 1
            }),
            t,
            102,
            7
        ))
        .is_ok()
    );
    let (mut g, mut root, t) = installed();
    root.revision += 1;
    root.change_id = id(8);
    root.controller.lease_until_ms = Some(90000);
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    assert!(g.check(sample(t, 2, 102)).is_err());
}
#[test]
fn mismatched_uuid_and_preparation_failure_release_only_current_slot() {
    let t = Instant::now();
    let base = fleet(0);
    let change = OwnershipChange::Acquire { process_id: id(3) };
    let p = proposal(&base, change, t, 100, 4);
    let q = proposal(&base, change, t, 100, 5);
    let mut g = OwnershipGuard::new(id(3));
    let stamp = g.begin(&p).unwrap();
    assert_eq!(
        g.finish(stamp, ack(&q), sample(t, 1, 101)),
        Err(StorageError::InvalidCatalog)
    );
    let stamp = g.begin(&p).unwrap();
    assert_eq!(
        g.finish(stamp, Err(StorageError::InvalidCatalog), sample(t, 1, 101)),
        Err(StorageError::InvalidCatalog)
    );
    assert!(g.begin(&p).is_ok());
}
#[test]
fn exact_expiry_discontinuities_tolerance_and_retained_drift() {
    for (elapsed, wall, valid) in [
        (0, 2100, true),
        (1, 2102, false),
        (2100, 200, true),
        (2101, 200, false),
    ] {
        let (mut g, _, t) = installed();
        assert_eq!(g.check(sample(t, elapsed, wall)).is_ok(), valid);
    }
    let (mut g, _, t) = installed();
    assert!(g.check(sample(t, 1000, 2100)).is_ok());
    assert!(g.check(sample(t, 2000, 4100)).is_ok());
    assert_eq!(
        g.check(sample(t, 3000, 6101)),
        Err(StorageError::Unavailable)
    );
    let t = Instant::now();
    let base = fleet(0);
    let p = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        t,
        100,
        4,
    );
    let mut g = OwnershipGuard::new(id(3));
    let stamp = g.begin(&p).unwrap();
    assert_eq!(
        g.finish(
            stamp,
            ack(&p),
            sample(t, base.timing.lease_ms, 100 + base.timing.lease_ms)
        ),
        Err(StorageError::Unavailable)
    );
}
#[test]
fn checked_time_and_generation_exhaustion_fail_closed_permanently() {
    let (mut g, _, t) = installed();
    assert!(matches!(
        OwnershipSample::from_system_time(t, UNIX_EPOCH - Duration::from_millis(1)),
        Err(StorageError::Unavailable)
    ));
    assert_eq!(
        g.check(Err(StorageError::LimitExceeded)),
        Err(StorageError::LimitExceeded)
    );
    let (mut g, root, t) = installed();
    assert_eq!(
        g.check(sample(t - Duration::from_millis(1), 0, 100)),
        Err(StorageError::Unavailable)
    );
    g.generation = u64::MAX;
    assert_eq!(g.invalidate(), Err(StorageError::LimitExceeded));
    let p = proposal(
        &root,
        OwnershipChange::Renew(Owner {
            process_id: id(3),
            epoch: 1,
        }),
        t,
        101,
        5,
    );
    assert_eq!(g.begin(&p), Err(StorageError::LimitExceeded));
    assert_eq!(g.generation, u64::MAX);
}

#[test]
fn submillisecond_monotonic_drift_above_tolerance_closes() {
    let (mut g, _, t) = installed();
    let now = OwnershipSample::from_system_time(
        t + Duration::from_micros(2_000_001),
        UNIX_EPOCH + Duration::from_millis(100),
    );
    assert_eq!(g.check(now), Err(StorageError::Unavailable));
}
#[test]
fn precomputed_arithmetic_failures_close_before_io_and_leave_slot_free() {
    let (mut g, root, t) = installed();
    let change = OwnershipChange::Renew(Owner {
        process_id: id(3),
        epoch: 1,
    });
    let bad = proposal(&root, change, t, u64::MAX, 5);
    assert_eq!(g.begin(&bad), Err(StorageError::LimitExceeded));
    assert!(g.check(sample(t, 1, 101)).is_err());
    let good = proposal(&root, change, t, 101, 6);
    let stamp = g.begin(&good).unwrap();
    assert_eq!(
        g.finish(stamp, Err(StorageError::Unavailable), sample(t, 2, 103)),
        Err(StorageError::Unavailable)
    );
    assert!(g.begin(&good).is_ok());

    // Derive the platform's last representable whole-second Instant. This is
    // capacity exhaustion, independently of wall time or root arithmetic.
    let mut low = 0_u64;
    let mut high = u64::MAX;
    while low < high {
        let mid = low + (high - low) / 2 + 1;
        if t.checked_add(Duration::from_secs(mid)).is_some() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let end = t.checked_add(Duration::from_secs(low)).unwrap();
    let base = fleet(0);
    let p = OwnershipProposal::new(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        id(4),
        sample(end, 0, 100),
    )
    .unwrap();
    let mut g = OwnershipGuard::new(id(3));
    assert_eq!(g.begin(&p), Err(StorageError::LimitExceeded));
    let wall = UNIX_EPOCH
        .checked_add(Duration::from_secs(u64::MAX / 1000 + 1))
        .unwrap();
    assert!(matches!(
        OwnershipSample::from_system_time(t, wall),
        Err(StorageError::LimitExceeded)
    ));
}
#[test]
fn full_candidate_contents_are_bound_and_owner_must_be_this_boot() {
    let t = Instant::now();
    let base = fleet(0);
    let p = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        t,
        100,
        4,
    );
    let mut g = OwnershipGuard::new(id(3));
    let stamp = g.begin(&p).unwrap();
    let mut changed = ownership_candidate(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        100,
        id(4),
    )
    .unwrap();
    changed.scheduler.failures = 1;
    let evidence = OwnershipEvidence {
        acknowledged: true,
        observation: Some(OwnershipObservation::new(&changed).unwrap()),
    };
    assert_eq!(
        g.finish(stamp, Ok(evidence), sample(t, 1, 101)),
        Err(StorageError::InvalidCatalog)
    );
    let wrong_boot = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(9) },
        t,
        100,
        5,
    );
    assert_eq!(g.begin(&wrong_boot), Err(StorageError::Conflict));
    assert!(g.begin(&p).is_ok());
}

#[test]
fn unrelated_admin_member_and_claim_content_preserves_original_deadline() {
    let t = Instant::now();
    let base = fleet(1);
    let p = proposal(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        t,
        100,
        4,
    );
    let mut g = OwnershipGuard::new(id(3));
    let stamp = g.begin(&p).unwrap();
    assert!(g.finish(stamp, ack(&p), sample(t, 1, 101)).unwrap());
    let mut root = ownership_candidate(
        &base,
        OwnershipChange::Acquire { process_id: id(3) },
        100,
        id(4),
    )
    .unwrap();
    let generation = g.authorization_generation();
    for n in 0..3 {
        root.revision += 1;
        root.change_id = id(10 + n);
        match n {
            0 => {
                root.admin_seq = 1;
                root.admin_head = Some(id(20));
            }
            1 => {
                root.nodes[0].resource_id = Some("resource/updated".into());
                root.intents[0].resource_id = root.nodes[0].resource_id.clone();
            }
            _ => root.intents[0].attempts += 1,
        }
        g.observe(&OwnershipObservation::new(&root).unwrap())
            .unwrap();
        assert_eq!(g.authorization_generation(), generation);
        assert!(g.check(sample(t, 10, 110)).is_ok());
    }
    assert_eq!(
        g.check(sample(t, base.timing.lease_ms, 100 + base.timing.lease_ms)),
        Err(StorageError::Unavailable)
    );
}

#[test]
fn closed_admission_during_renewal_does_not_cancel_valid_pending_completion() {
    let (mut g, root, t) = installed();
    let p = proposal(
        &root,
        OwnershipChange::Renew(Owner {
            process_id: id(3),
            epoch: 1,
        }),
        t,
        101,
        5,
    );
    let stamp = g.begin(&p).unwrap();
    assert_eq!(g.check(sample(t, 1, 102)), Err(StorageError::Unavailable));
    assert!(g.finish(stamp, ack(&p), sample(t, 2, 103)).unwrap());
}

#[test]
fn pending_renewal_clock_failure_makes_later_acknowledgment_obsolete() {
    let lease_ms = fleet(0).timing.lease_ms;
    for (elapsed, wall, preserves_pending) in [
        (1, 13002, false),
        (1, 6999, false),
        (1, 12001, true),
        (1, 8001, true),
        (lease_ms, 10000 + lease_ms, false),
        (lease_ms - 1000, 10000 + lease_ms, false),
    ] {
        let (mut g, root, t) = installed();
        let p = proposal(
            &root,
            OwnershipChange::Renew(Owner {
                process_id: id(3),
                epoch: 1,
            }),
            t,
            10000,
            5,
        );
        let stamp = g.begin(&p).unwrap();
        assert_eq!(
            g.check(sample(t, elapsed, wall)),
            Err(StorageError::Unavailable)
        );
        // A normal clock after a rejected sample cannot revive an invalidated
        // operation; an exact acknowledgment is still an obsolete completion.
        assert_eq!(
            g.finish(stamp, ack(&p), sample(t, 2, 10002)).unwrap(),
            preserves_pending,
            "elapsed={elapsed}, wall={wall}"
        );
        if !preserves_pending {
            assert_eq!(g.check(sample(t, 3, 10003)), Err(StorageError::Unavailable));
            // Recovery needs a distinct proposal and acknowledged ownership
            // operation; an observation or returned clock alone is insufficient.
            let applied = ownership_candidate(&root, p.change, 10000, id(5)).unwrap();
            let recovery = proposal(&applied, p.change, t + Duration::from_millis(4), 10004, 6);
            let fresh = g.begin(&recovery).unwrap();
            assert!(
                g.finish(fresh, ack(&recovery), sample(t, 5, 10005))
                    .unwrap()
            );
        }
    }
}

#[test]
fn actual_forward_and_backward_wall_steps_at_finish_and_check() {
    for wall in [7999, 8000, 12000, 12001] {
        let t = Instant::now();
        let base = fleet(0);
        let p = proposal(
            &base,
            OwnershipChange::Acquire { process_id: id(3) },
            t,
            10000,
            4,
        );
        let mut g = OwnershipGuard::new(id(3));
        let stamp = g.begin(&p).unwrap();
        let valid = wall == 8000 || wall == 12000;
        assert_eq!(g.finish(stamp, ack(&p), sample(t, 0, wall)).is_ok(), valid);
        if !valid {
            assert!(g.check(sample(t, 1, 10001)).is_err());
        }
        let mut g = OwnershipGuard::new(id(3));
        let stamp = g.begin(&p).unwrap();
        assert!(g.finish(stamp, ack(&p), sample(t, 0, 10000)).unwrap());
        assert_eq!(g.check(sample(t, 0, wall)).is_ok(), valid);
    }
}
