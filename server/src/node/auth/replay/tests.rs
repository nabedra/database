use super::*;
use crate::node::auth::verify::consume_tests::{Fixture, pair, roundtrip};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

#[test]
fn equal_sequence_one_winner_and_forced_order() {
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
        handles.push(std::thread::spawn(move || {
            ready.send(()).unwrap();
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
            state.lock().unwrap().consume(e, c).map(|_| ())
        }));
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
    let high_thread = std::thread::spawn(move || {
        let result = high_state
            .lock()
            .unwrap()
            .consume(high.0, high.1)
            .map(|_| ());
        let _ = done_tx.send(());
        result
    });
    let low_thread = std::thread::spawn(move || {
        let observed = done_rx.recv_timeout(Duration::from_secs(10));
        (
            observed,
            state.lock().unwrap().consume(low.0, low.1).map(|_| ()),
        )
    });
    let high_result = high_thread.join();
    let low_result = low_thread.join();
    assert_eq!(high_result.unwrap(), Ok(()));
    let (observed, result) = low_result.unwrap();
    assert!(observed.is_ok());
    assert_eq!(result, Err(Error::Replay));
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
