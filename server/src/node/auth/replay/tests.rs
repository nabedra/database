use super::*;
use crate::node::auth::verify::consume_tests::{Fixture, pair, roundtrip};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

#[test]
fn equal_sequence_one_winner_and_unseen_out_of_order_accepted() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let a = pair(&f, 1);
    let b = pair(&f, 1);
    state.observe(a.0.observation()).unwrap();
    let state = Arc::new(Mutex::new(state));
    let mut handles = Vec::new();
    let mut gates = Vec::new();
    let (ready_tx, ready_rx) = mpsc::channel();
    for (e, c) in [a, b] {
        let (tx, rx) = mpsc::channel();
        gates.push(tx);
        let ready = ready_tx.clone();
        let state = Arc::clone(&state);
        handles.push(
            nabedra_rt::task::spawn_service("nabedra-test", move || {
                ready.send(()).unwrap();
                rx.recv_timeout(Duration::from_secs(10)).unwrap();
                state.lock().unwrap().consume(e, c).map(|_| ())
            })
            .expect("spawn test thread"),
        );
    }
    let observed = (0..2).all(|_| ready_rx.recv_timeout(Duration::from_secs(10)).is_ok());
    for tx in gates {
        let _ = tx.send(());
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join()).collect();
    assert!(observed);
    let results: Vec<_> = results.into_iter().map(Result::unwrap).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.iter().filter(|r| **r == Err(Error::Replay)).count(),
        1
    );

    let (done_tx, done_rx) = mpsc::channel();
    let high = pair(&f, 12);
    let low = pair(&f, 11);
    let high_state = Arc::clone(&state);
    let high_thread = nabedra_rt::task::spawn_service("nabedra-test", move || {
        let result = high_state
            .lock()
            .unwrap()
            .consume(high.0, high.1)
            .map(|_| ());
        let _ = done_tx.send(());
        result
    })
    .expect("spawn test thread");
    let low_thread = nabedra_rt::task::spawn_service("nabedra-test", move || {
        let observed = done_rx.recv_timeout(Duration::from_secs(10));
        (
            observed,
            state.lock().unwrap().consume(low.0, low.1).map(|_| ()),
        )
    })
    .expect("spawn test thread");
    let high_result = high_thread.join();
    let low_result = low_thread.join();
    assert_eq!(high_result.unwrap(), Ok(()));
    let (observed, result) = low_result.unwrap();
    assert!(observed.is_ok());
    assert_eq!(result, Ok(()));
}

#[test]
fn association_boot_root_and_sticky_integrity_spend_nothing() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let (e, c) = pair(&f, 1);
    assert!(matches!(state.consume(e, c), Err(Error::Invalid)));
    let (e, _) = pair(&f, 1);
    state.observe(e.observation()).unwrap();
    let (_, c) = pair(&f, 1);
    assert!(matches!(state.consume(e, c), Err(Error::Invalid)));
    let (e, c) = pair(&f, 1);
    state.consume(e, c).unwrap();
    let (e, c) = pair(&f, u64::MAX);
    drop(state.consume(e, c).unwrap().into_handoff());
    let (e, c) = pair(&f, u64::MAX - 1);
    state.consume(e, c).unwrap();
    for seq in [1, u64::MAX - 1, u64::MAX] {
        let (e, c) = pair(&f, seq);
        assert!(matches!(state.consume(e, c), Err(Error::Replay)));
    }
    let mut newer = f.root.clone();
    newer.revision += 1;
    newer.change_id = uuid::Uuid::from_u128(99);
    let mut newer = roundtrip(&newer);
    let observation = OwnershipObservation::new(&newer).unwrap();
    state.observe(&observation).unwrap();
    state.observe(&observation).unwrap();
    assert_eq!(
        state.observe(&OwnershipObservation::new(&f.root).unwrap()),
        Err(Error::Invalid)
    );
    assert!(!state.failed);
    assert!(state.observation.as_ref() == Some(&observation));
    assert_eq!(state.apis[0].highest, u64::MAX);
    let mut divergent = newer.clone();
    divergent.scheduler.failures += 1;
    let divergent = roundtrip(&divergent);
    assert_eq!(
        state.observe(&OwnershipObservation::new(&divergent).unwrap()),
        Err(Error::Invalid)
    );
    assert!(state.failed);
    assert!(state.observation.as_ref() == Some(&observation));
    assert_eq!(state.apis.len(), 1);
    assert_eq!(state.apis[0].highest, u64::MAX);
    newer.revision += 1;
    newer.change_id = uuid::Uuid::from_u128(100);
    let mut f = f;
    f.root = roundtrip(&newer);
    state
        .observe(&OwnershipObservation::new(&f.root).unwrap())
        .unwrap();
    let (e, c) = pair(&f, u64::MAX);
    assert!(matches!(state.consume(e, c), Err(Error::Invalid)));
}

#[test]
fn actual_identity_limits_existing_progress_and_no_eviction() {
    use crate::node::auth::verify::consume_tests::{id, identity_pair};
    for worker in [false, true] {
        let mut f = Fixture::new();
        let mut state = ReplayState::new(f.local());
        let limit = if worker { WORKERS } else { APIS };
        let capacities = (state.workers.capacity(), state.apis.capacity());
        for n in 0..limit {
            let (e, c) = identity_pair(&mut f, worker, n, 1);
            state.observe(e.observation()).unwrap();
            state.consume(e, c).unwrap();
        }
        let (e, c) = identity_pair(&mut f, worker, limit, 1);
        state.observe(e.observation()).unwrap();
        assert!(matches!(state.consume(e, c), Err(Error::Exhausted)));
        let (e, c) = identity_pair(&mut f, worker, 0, 2);
        state.observe(e.observation()).unwrap();
        state.consume(e, c).unwrap();
        let (e, c) = identity_pair(&mut f, worker, 0, 1);
        state.observe(e.observation()).unwrap();
        let retained_auth = e.auth().clone();
        assert!(matches!(state.consume(e, c), Err(Error::Replay)));
        assert_eq!(
            (state.workers.len(), state.apis.len()),
            if worker { (WORKERS, 0) } else { (0, APIS) }
        );
        assert_eq!(
            (state.workers.capacity(), state.apis.capacity()),
            capacities
        );
        // Entry 0 has been absent from thousands of intermediate worker roots.
        // Retire it explicitly, retain the sequence, and advance unrelated identity 1.
        if worker {
            f.root.nodes[1].state = crate::cluster::member::NodeState::Lost;
            f.root.nodes[1].retired_incarnations.push(id(10_000));
            f.root.nodes[1].incarnation = Some(id(888_888));
            f.root.revision += 1;
            f.root.change_id = id(999_999);
            f.root = roundtrip(&f.root);
            state
                .observe(&OwnershipObservation::new(&f.root).unwrap())
                .unwrap();
            let route =
                crate::node::auth::ActualRoute::new("POST", "/internal/v1/dependencies/open", None)
                    .unwrap();
            let signed = crate::node::auth::verify_request(
                &crate::node::auth::verify::consume_tests::hex(
                    &retained_auth.canonical_bytes().unwrap(),
                ),
                &crate::node::auth::verify::consume_tests::hex(
                    &f.key.sign_request(&retained_auth).unwrap(),
                ),
                &route,
            )
            .unwrap();
            assert!(
                crate::node::auth::check_request_authority(&signed, &f.root, &f.local(), &f.pin())
                    .is_err()
            );

            let (e, c) = identity_pair(&mut f, true, 1, 2);
            state.observe(e.observation()).unwrap();
            state.consume(e, c).unwrap();
            let (e, c) = identity_pair(&mut f, true, 0, 2);
            state.observe(e.observation()).unwrap();
            assert!(matches!(state.consume(e, c), Err(Error::Replay)));
        }
    }
}

fn dependency_pair(
    f: &Fixture,
    scope: &str,
    sequence: u64,
    permit: uuid::Uuid,
    source: Option<&crate::node::auth::Auth>,
) -> (
    crate::node::auth::EligibleRequest,
    crate::node::auth::CheckedContent,
) {
    use crate::node::auth::{ActualRoute, check_request_authority, check_request_content};
    let body = format!(r#"{{"version":1,"permit_id":"{permit}"}}"#).into_bytes();
    let (path, text) = if let Some(source) = source {
        let path = if scope == "dependency_close" {
            "/internal/v1/dependencies/close"
        } else {
            "/internal/v1/dependencies/open"
        };
        let auth = crate::node::auth::Auth::request(
            source.cluster_id(),
            source.sender().clone(),
            source.receiver().clone(),
            if scope == "dependency_close" {
                crate::node::auth::wire::Route::DependencyClose
            } else {
                crate::node::auth::wire::Route::DependencyOpen
            },
            crate::node::auth::wire::RequestIds {
                operation: source.operation_id(),
                admission: source.admission_id(),
                job: None,
                attempt: None,
                dependency: None,
                digest: source.request_digest().map(str::to_owned),
            },
            crate::node::auth::verify::consume_tests::hex(
                ring::digest::digest(&ring::digest::SHA256, &body).as_ref(),
            ),
        )
        .unwrap()
        .with_sequence(sequence)
        .unwrap();
        (
            path,
            String::from_utf8(auth.canonical_bytes().unwrap()).unwrap(),
        )
    } else {
        let (text, _) = f.auth(scope, &body);
        let path = if scope == "dependency_close" {
            "/internal/v1/dependencies/close"
        } else {
            "/internal/v1/dependencies/open"
        };
        (
            path,
            text.replace("\"sequence\":1", &format!("\"sequence\":{sequence}")),
        )
    };
    let signed = f.sign_text(&text, &ActualRoute::new("POST", path, None).unwrap());
    (
        check_request_authority(&signed, &f.root, &f.local(), &f.pin()).unwrap(),
        check_request_content(&signed, body).unwrap(),
    )
}
#[test]
fn q05_c05_signed_reordered_concurrent_packets_are_accepted_once_and_window_bounded() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    for sequence in (1..=100).rev() {
        let (e, c) = pair(&f, sequence);
        state.observe(e.observation()).unwrap();
        state.consume(e, c).unwrap();
    }
    for sequence in [1, 50, 100] {
        let (e, c) = pair(&f, sequence);
        assert!(matches!(state.consume(e, c), Err(Error::Replay)));
    }
    let (e, c) = pair(&f, SEQUENCE_WINDOW as u64 + 101);
    state.consume(e, c).unwrap();
    let (e, c) = pair(&f, 100);
    assert!(matches!(state.consume(e, c), Err(Error::Replay)));
    let (e, c) = pair(&f, 102);
    state.consume(e, c).unwrap();
    assert_eq!(state.apis[0].seen.len(), SEQUENCE_WINDOW / 64);
    assert_eq!(state.apis.len(), 1);
}
#[test]
fn f03_c04_close_before_delayed_open_remains_fenced_until_original_window_expires() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let permit = uuid::Uuid::from_u128(900);
    let (close, content) = dependency_pair(&f, "dependency_close", 2, permit, None);
    let sender = close.auth().sender().clone();
    state.observe(close.observation()).unwrap();
    let fence = state.prepare_dependency_close(&sender, permit, 2).unwrap();
    state.consume(close, content).unwrap();
    state.commit_dependency_close(fence);
    assert_eq!(
        state.check_dependency_open(&sender, permit),
        Err(Error::Replay)
    );
    let (open, content) = dependency_pair(&f, "dependency_open", 1, permit, None);
    assert_eq!(
        state.check_dependency_open(open.auth().sender(), permit),
        Err(Error::Replay)
    );
    // The packet itself is an unseen valid reordered sequence. Admission's
    // separate close fence is what prevents this delayed Open from making work.
    assert!(state.consume(open, content).is_ok());
    let (e, c) = dependency_pair(
        &f,
        "dependency_open",
        SEQUENCE_WINDOW as u64 + 3,
        uuid::Uuid::from_u128(901),
        None,
    );
    state.consume(e, c).unwrap();
    assert!(state.closed_permits.is_empty());
    let (open, content) = dependency_pair(&f, "dependency_open", 1, permit, None);
    assert!(matches!(state.consume(open, content), Err(Error::Replay)));
}
#[test]
fn r05_authoritative_membership_fence_collects_dormant_sender_close_tombstones() {
    use crate::node::auth::verify::consume_tests::{id, identity_pair};
    let mut f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let (e, c) = identity_pair(&mut f, true, 0, 1);
    let original = e.auth().clone();
    let sender = original.sender().clone();
    state.observe(e.observation()).unwrap();
    state.consume(e, c).unwrap();
    let permit = id(900);
    let (e, c) = dependency_pair(&f, "dependency_close", 2, permit, Some(&original));
    let close = state.prepare_dependency_close(&sender, permit, 2).unwrap();
    state.consume(e, c).unwrap();
    state.commit_dependency_close(close);
    assert_eq!(state.closed_permits.len(), 1);
    let old_membership = ReplayMembership::prepare(&f.root).unwrap();
    let old_incarnation = f.root.nodes[1].incarnation.unwrap();
    f.root.nodes[1].retired_incarnations.push(old_incarnation);
    f.root.nodes[1].incarnation = Some(id(903));
    f.root.nodes[1].state = crate::cluster::member::NodeState::Lost;
    f.root.revision += 1;
    f.root.change_id = id(902);
    f.root = roundtrip(&f.root);
    state
        .observe(&OwnershipObservation::new(&f.root).unwrap())
        .unwrap();
    assert_eq!(
        state.observe_membership(old_membership),
        Err(Error::Invalid)
    );
    assert_eq!(state.closed_permits.len(), 1);
    state
        .observe_membership(ReplayMembership::prepare(&f.root).unwrap())
        .unwrap();
    assert!(state.closed_permits.is_empty());
}
#[test]
fn q02_closed_permit_capacity_is_reserved_before_sequence_is_spent() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let (e, c) = dependency_pair(&f, "dependency_open", 1, uuid::Uuid::from_u128(99), None);
    let sender = e.auth().sender().clone();
    state.observe(e.observation()).unwrap();
    state.consume(e, c).unwrap();
    // Exercise the same bounded window and close commits without thousands of
    // repeated signature checks; all these sequences are simultaneously retained.
    for sequence in 1..=CLOSED_PERMITS as u64 {
        if sequence > 1 {
            state.workers[0].consume(sequence).unwrap();
        }
        let close = state
            .prepare_dependency_close(
                &sender,
                uuid::Uuid::from_u128(1000 + sequence as u128),
                sequence,
            )
            .unwrap();
        state.commit_dependency_close(close);
    }
    assert_eq!(state.closed_permits.len(), CLOSED_PERMITS);
    let highest = state.workers[0].highest;
    assert!(matches!(
        state.prepare_dependency_close(&sender, uuid::Uuid::from_u128(99999), highest + 1),
        Err(Error::Exhausted)
    ));
    assert_eq!(state.workers[0].highest, highest);
    state.workers[0].consume(highest + 1).unwrap();
    state.collect_closed_permits();
    assert_eq!(state.closed_permits.len(), CLOSED_PERMITS - 1);
    assert!(
        state
            .prepare_dependency_close(&sender, uuid::Uuid::from_u128(99999), highest + 2)
            .is_ok()
    );
}
