use super::*;

#[tokio::test]
async fn legacy_refusal_retains_registered_dormant_owner() {
    let (owners, threads, parent, _) = tests::fixture();
    let process = owners.try_process(parent.clone(), tests::limits()).unwrap();
    let refused = owners.try_process(parent.clone(), tests::limits()).is_err();
    let retained = {
        let state = owners.shared.state.lock().unwrap();
        let r = &state.payload;
        r.entries.len() + r.preparations.iter().flatten().count()
    };
    drop(process);
    settle(owners, threads, parent).await;
    assert!(refused);
    assert_eq!(
        retained, 2,
        "refusal must leave the original registered task"
    );
}

fn memory_guards(permit: Option<OwnedSemaphorePermit>) -> Admission {
    Admission::legacy(permit, None)
}

const WATCHDOG: std::time::Duration = std::time::Duration::from_secs(10);

async fn settle(
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    parent: PathBuf,
) {
    let drain = nabedra_rt::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let joined = nabedra_rt::time::timeout(WATCHDOG, threads.shutdown()).await;
    let empty = std::fs::read_dir(&parent).unwrap().count() == 0;
    std::fs::remove_dir(parent).unwrap();
    assert!(matches!(drain, Ok(Ok(()))));
    assert!(matches!(joined, Ok(Ok(()))));
    assert!(empty);
}

#[tokio::test]
async fn fixed_shares_retain_original_handles_and_charges_through_return() {
    let (owners, threads, parent, _) = tests::fixture();
    let data_charge = Arc::new(Semaphore::new(24));
    let control_charge = Arc::new(Semaphore::new(8));
    let mut tickets = Vec::new();
    for _ in 0..24 {
        tickets.push(
            owners
                .prepare_entry(
                    Kind::Stream,
                    None,
                    memory_guards(Some(data_charge.clone().try_acquire_owned().unwrap())),
                    false,
                )
                .unwrap(),
        );
    }
    let data_full = owners
        .prepare_entry(Kind::Stream, None, memory_guards(None), false)
        .is_err();
    for _ in 0..8 {
        tickets.push(
            owners
                .prepare_control(control_charge.clone().try_acquire_owned().unwrap())
                .unwrap(),
        );
    }
    let control_full = owners
        .prepare_control(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap())
        .is_err();
    let mut returns = Vec::new();
    let mut releases = Vec::new();
    let identities = {
        let state = owners.shared.state.lock().unwrap();
        let r = &state.payload;
        r.preparations
            .iter()
            .flatten()
            .map(|p| (p.id, p.entry.as_ref().unwrap().handle.id()))
            .collect::<Vec<_>>()
    };
    for ticket in &tickets {
        let (at_return, returned) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        ticket.slot.hooks.lock().unwrap().returning = Some((at_return, gate));
        returns.push(returned);
        releases.push(release);
    }
    let mut pending = Vec::new();
    for ticket in &tickets {
        let (polled, observed) = oneshot::channel();
        ticket.slot.hooks.lock().unwrap().dormant_pending = Some(polled);
        pending.push(observed);
    }
    let dormant_polled = nabedra_rt::time::timeout(WATCHDOG, async {
        for pending in pending {
            pending.await.map_err(|_| ())?;
        }
        Ok::<_, ()>(())
    })
    .await;
    let no_work = tickets.iter().all(|p| {
        let h = p.slot.hooks.lock().unwrap();
        h.executed.is_empty() && h.created.is_empty()
    }) && std::fs::read_dir(&parent).unwrap().count() == 0;
    drop(tickets);
    let observed = nabedra_rt::time::timeout(WATCHDOG, async {
        for returned in returns {
            returned.await.map_err(|_| ())?;
        }
        Ok::<_, ()>(())
    })
    .await;
    let original_retained = {
        let state = owners.shared.state.lock().unwrap();
        let r = &state.payload;
        r.preparations
            .iter()
            .flatten()
            .map(|p| (p.id, p.entry.as_ref().unwrap().handle.id()))
            .collect::<Vec<_>>()
            == identities
            && r.preparations.iter().flatten().all(|p| {
                p.entry
                    .as_ref()
                    .unwrap()
                    .slot
                    .cleanup
                    .load(Ordering::Acquire)
            })
    };
    let charged = (
        data_charge.available_permits(),
        control_charge.available_permits(),
    );
    let mut drain = Box::pin(owners.close_and_drain());
    let pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    for release in releases {
        let _ = release.send(());
    }
    settle(owners, threads, parent).await;
    assert!(data_full && control_full && no_work && original_retained && pending);
    assert!(matches!(observed, Ok(Ok(()))));
    assert!(matches!(dormant_polled, Ok(Ok(()))));
    assert_eq!(charged, (0, 0));
    assert_eq!(
        (
            data_charge.available_permits(),
            control_charge.available_permits()
        ),
        (24, 8)
    );
}

#[tokio::test]
async fn close_sees_empty_placeholder_and_later_published_handle() {
    let (owners, threads, parent, _) = tests::fixture();
    let transport = Arc::new(Semaphore::new(1));
    // Deterministic boundary between the actual reservation and spawn phases.
    let id = owners
        .reserve_preparation(
            memory_guards(Some(transport.clone().try_acquire_owned().unwrap())),
            false,
            None,
        )
        .unwrap();
    let mut drain = Box::pin(owners.close_and_drain());
    let empty_pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    let ticket = owners.spawn_preparation(id, Kind::Stream, None).unwrap();
    let (at_return, returned) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    ticket.slot.hooks.lock().unwrap().returning = Some((at_return, gate));
    drop(ticket);
    let observed = nabedra_rt::time::timeout(WATCHDOG, returned).await;
    let retained = transport.available_permits() == 0
        && owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .preparations
            .iter()
            .flatten()
            .any(|p| p.id == id && p.entry.is_some());
    let _ = release.send(());
    settle(owners, threads, parent).await;
    assert!(empty_pending && retained && matches!(observed, Ok(Ok(()))));
    assert_eq!(transport.available_permits(), 1);
}

#[tokio::test]
async fn dormant_abandonment_needs_no_live_worker_and_refusal_stays_bounded() {
    let (owners, threads, parent, _) = tests::fixture();
    nabedra_rt::time::timeout(WATCHDOG, threads.shutdown())
        .await
        .unwrap()
        .unwrap();
    let mut tickets = Vec::new();
    for _ in 0..24 {
        tickets.push(
            owners
                .prepare_entry(Kind::Process, None, memory_guards(None), false)
                .unwrap(),
        );
    }
    let refused = (0..100).all(|_| owners.try_process(parent.clone(), tests::limits()).is_err());
    let count = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .preparations
        .iter()
        .flatten()
        .count();
    drop(tickets);
    let drain = nabedra_rt::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let empty = std::fs::read_dir(&parent).unwrap().count() == 0;
    std::fs::remove_dir(parent).unwrap();
    assert!(refused && empty && matches!(drain, Ok(Ok(()))));
    assert_eq!(count, 24);
}

#[tokio::test]
async fn boot_activates_original_supervisor_before_any_transport() {
    let (owners, threads, parent, _) = tests::fixture();
    let ticket = owners
        .prepare_entry(Kind::Process, None, memory_guards(None), false)
        .unwrap();
    let id = ticket.slot.id;
    let original = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .preparations
        .iter()
        .flatten()
        .find(|p| p.id == id)
        .unwrap()
        .entry
        .as_ref()
        .unwrap()
        .handle
        .id();
    let mut process = owners
        .activate_legacy(
            ticket,
            None,
            Kind::Process,
            Initial::Process(parent.clone(), tests::limits()),
            |_, _| None,
        )
        .unwrap();
    let initialized = matches!(
        nabedra_rt::time::timeout(WATCHDOG, process.ready()).await,
        Ok(Ok(Reply::Ready))
    );
    let same = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .entries
        .iter()
        .any(|e| e.slot.id == id && e.handle.id() == original);
    let worker_created = process.slot.hooks.lock().unwrap().created.clone();
    drop(process);
    settle(owners, threads, parent).await;
    assert!(initialized && same);
    assert_eq!(worker_created.len(), 1);
    assert_ne!(worker_created[0], std::thread::current().id());
}

#[tokio::test]
async fn pre_spawn_failure_rolls_back_and_identity_exhaustion_never_wraps() {
    let (owners, threads, parent, _) = tests::fixture();
    let transport = Arc::new(Semaphore::new(1));
    let outside_runtime = PayloadOwners {
        shared: owners.shared.clone(),
    };
    let permit = transport.clone().try_acquire_owned().unwrap();
    let rolled_back = std::thread::spawn(move || outside_runtime.prepare_control(permit).is_err())
        .join()
        .unwrap();
    let empty = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .preparations
        .iter()
        .all(Option::is_none);
    owners.shared.state.lock().unwrap().payload.next = u64::MAX;
    let refused = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .is_err();
    let next = owners.shared.state.lock().unwrap().payload.next;
    settle(owners, threads, parent).await;
    assert!(rolled_back && empty && refused);
    assert_eq!(next, u64::MAX);
    assert_eq!(transport.available_permits(), 1);
}
