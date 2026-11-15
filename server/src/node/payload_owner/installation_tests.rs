use super::*;

#[tokio::test]
async fn accepted_storage_is_reserved_before_registration() {
    let (owners, threads, path, _) = tests::fixture();
    let storage = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .entries
        .capacity();
    let drained =
        tokio::time::timeout(std::time::Duration::from_secs(10), owners.close_and_drain()).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), threads.shutdown()).await;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(drained, Ok(Ok(()))));
    assert!(matches!(joined, Ok(Ok(()))));
    assert!(
        storage >= 1 + 4096 + 32,
        "accepted storage must exist before locking"
    );
}

#[tokio::test]
async fn reservation_rolls_back_parent_and_installs_original_task() {
    let (owners, threads, path, guard) = tests::fixture();
    let mut process = owners.try_process(path.clone(), tests::limits()).unwrap();
    let ready = tokio::time::timeout(std::time::Duration::from_secs(10), process.ready()).await;
    let prepared = owners
        .prepare_entry(
            Kind::Operation,
            None,
            Admission::legacy(None, Some(guard.clone())),
            false,
        )
        .unwrap();
    let mut prepared = prepared;
    let (original, rollback, full, installed) = {
        let mut state = owners.shared.state.lock().unwrap();
        let r = &mut state.payload;
        let original = r
            .preparations
            .iter()
            .flatten()
            .find(|p| p.id == prepared.slot.id)
            .unwrap()
            .entry
            .as_ref()
            .unwrap()
            .handle
            .id();
        let reserved = r
            .reserve_entry_locked(
                &mut prepared,
                Some(&process),
                Kind::Operation,
                &owners.shared,
            )
            .unwrap();
        drop(reserved);
        let rollback = *process.slot.children.lock().unwrap() == 0 && r.entries.len() == 1;
        let limit = r.accepted_limit;
        r.accepted_limit = 1;
        let full = r
            .reserve_entry_locked(
                &mut prepared,
                Some(&process),
                Kind::Operation,
                &owners.shared,
            )
            .is_err();
        r.accepted_limit = limit;
        let reservation = r
            .reserve_entry_locked(
                &mut prepared,
                Some(&process),
                Kind::Operation,
                &owners.shared,
            )
            .unwrap();
        let lease = reservation.install_entry_locked(Initial::Operation(tests::limits()), None);
        (original, rollback, full, lease)
    };
    let same_handle = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .entries
        .iter()
        .any(|e| e.slot.id == installed.slot.id && e.handle.id() == original);
    drop(prepared);
    drop(installed);
    drop(process);
    let drained =
        tokio::time::timeout(std::time::Duration::from_secs(10), owners.close_and_drain()).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), threads.shutdown()).await;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(ready, Ok(Ok(Reply::Ready))));
    assert!(matches!(drained, Ok(Ok(()))));
    assert!(matches!(joined, Ok(Ok(()))));
    assert!(rollback && full && same_handle);
}

#[tokio::test]
async fn pending_guard_release_remains_visible_after_handle_reaping() {
    let (owners, threads, path, _) = tests::fixture();
    let transport = Arc::new(Semaphore::new(1));
    let prepared = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .unwrap();
    let slot = prepared.slot.clone();
    drop(prepared);
    let finished = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        poll_fn(|cx| {
            let mut state = owners.shared.state.lock().unwrap();
            let entry = state
                .payload
                .preparations
                .iter_mut()
                .flatten()
                .find(|p| p.id == slot.id)
                .unwrap()
                .entry
                .as_mut()
                .unwrap();
            match Pin::new(&mut entry.handle).poll(cx) {
                Poll::Ready(result) => {
                    entry.terminated = true;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }),
    )
    .await;
    let (at_release, observed) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *owners.shared.releasing.lock().unwrap() = Some((at_release, gate));
    let shared = owners.shared.clone();
    let reaper = std::thread::spawn(move || reap_shared(&shared, None));
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10), observed).await;
    let snapshot = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    let charged = transport.available_permits();
    let _ = release.send(());
    let joined_reaper = reaper.join();
    let after = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    let drained =
        tokio::time::timeout(std::time::Duration::from_secs(10), owners.close_and_drain()).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), threads.shutdown()).await;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(finished, Ok(Ok(()))));
    assert!(matches!(observed, Ok(Ok(()))));
    assert!(joined_reaper.is_ok());
    assert!(matches!(drained, Ok(Ok(()))));
    assert!(matches!(joined, Ok(Ok(()))));
    assert_eq!(
        (
            snapshot.accepted,
            snapshot.preparations,
            snapshot.pending_releases
        ),
        (0, 0, 1)
    );
    assert_eq!(charged, 0);
    assert_eq!(after.pending_releases, 0);
    assert_eq!(transport.available_permits(), 1);
}

fn distributed(
    o: usize,
    p: usize,
    q: usize,
) -> (PayloadOwners, crate::worker_pool::WorkerThreads, PathBuf) {
    let (legacy, threads, path, _) = tests::fixture();
    let state = Arc::new(Mutex::new(super::super::admission::distributed_fixture(
        o, p, q,
    )));
    let owners = PayloadOwners::from_receiver(legacy.shared.pool.clone(), state);
    (owners, threads, path)
}
#[tokio::test]
async fn exact_boot_receipt_needs_no_transport_and_never_initializes() {
    let (owners, threads, path) = distributed(2, 3, 0);
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let foreign_boot = super::super::admission::distributed_fixture(2, 3, 0)
        .local()
        .unwrap()
        .boot();
    let wrong_boot = owners
        .start_boot(
            &foreign_boot,
            uuid::Uuid::from_u128(30),
            path.clone(),
            tests::limits(),
        )
        .is_err();
    let wrong = owners
        .start_boot(
            &boot,
            uuid::Uuid::from_u128(31),
            path.clone(),
            tests::limits(),
        )
        .is_err();
    let mut process = owners
        .start_boot(
            &boot,
            uuid::Uuid::from_u128(30),
            path.clone(),
            tests::limits(),
        )
        .unwrap();
    let duplicate = owners
        .start_boot(
            &boot,
            uuid::Uuid::from_u128(30),
            path.clone(),
            tests::limits(),
        )
        .is_err();
    let receipt =
        tokio::time::timeout(std::time::Duration::from_secs(10), process.process_ready()).await;
    let valid = receipt.as_ref().is_ok_and(|r| {
        r.as_ref()
            .is_ok_and(|r| r.check_locked(&owners.shared.state.lock().unwrap()))
    });
    let initialized = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .snapshot()
        .initialized();
    let lifetime = Arc::new(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap());
    let legacy = owners.try_process(path.clone(), tests::limits()).is_err()
        && owners
            .try_operation(&process, tests::limits(), lifetime.clone())
            .is_err()
        && owners
            .try_admit(&process, Role::Operation, lifetime.clone())
            .is_err()
        && owners
            .try_admit_until(&process, Role::Dependency, lifetime.clone(), None)
            .is_err()
        && owners
            .try_bound_operation(
                &process,
                tests::limits(),
                lifetime,
                uuid::Uuid::from_u128(2),
            )
            .is_err();
    drop(process);
    let drained =
        tokio::time::timeout(std::time::Duration::from_secs(10), owners.close_and_drain()).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), threads.shutdown()).await;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(drained, Ok(Ok(()))) && matches!(joined, Ok(Ok(()))));
    assert!(wrong_boot && wrong && duplicate && valid && legacy && !initialized);
}

const WATCHDOG: std::time::Duration = std::time::Duration::from_secs(10);
async fn booted(
    o: usize,
    p: usize,
    q: usize,
) -> (
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    PathBuf,
    PayloadLease,
) {
    let (owners, threads, path) = distributed(o, p, q);
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let mut process = owners
        .start_boot(
            &boot,
            uuid::Uuid::from_u128(30),
            path.clone(),
            tests::limits(),
        )
        .unwrap();
    let receipt = tokio::time::timeout(WATCHDOG, process.process_ready()).await;
    let initialized = if let Ok(Ok(receipt)) = receipt {
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .fixture_initialize(receipt)
    } else {
        false
    };
    if !initialized {
        drop(process);
        finish_distributed(owners, threads, path).await;
        panic!("boot fixture initialization failed after settled teardown");
    }
    (owners, threads, path, process)
}
async fn finish_distributed(
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    path: PathBuf,
) {
    finish_distributed_with_watchdog(owners, threads, path, WATCHDOG).await;
}
async fn finish_distributed_with_watchdog(
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    path: PathBuf,
    watchdog: std::time::Duration,
) {
    let drain = owners.close_and_drain();
    tokio::pin!(drain);
    let drained = tokio::time::timeout(watchdog, &mut drain).await;
    if drained.is_err() {
        // A watchdog reports failure; it does not end registered ownership.
        // Keep the pool available until the original drain has settled/reaped.
        let _ = drain.await;
    }
    let settled = {
        let state = owners.shared.state.lock().unwrap();
        let snapshot = state.snapshot_locked();
        let p = snapshot.payload;
        state.local().is_some_and(|l| l.counts() == (0, 0, 0, 0, 0))
            && p.accepted == 0
            && p.records == 0
            && p.payload_positions == 0
            && p.preparations == 0
            && p.supervisors == 0
            && p.pending_releases == 0
            && !p.failed
    };
    let shutdown = threads.shutdown();
    tokio::pin!(shutdown);
    let joined = tokio::time::timeout(watchdog, &mut shutdown).await;
    if joined.is_err() {
        // This future owns the actual worker joins. Never detach it on timeout.
        let _ = shutdown.await;
    }
    let empty = std::fs::read_dir(&path).unwrap().count() == 0;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(drained, Ok(Ok(()))) && matches!(joined, Ok(Ok(()))) && empty && settled);
}
#[tokio::test]
async fn data_saturation_preserves_eight_original_memory_controls_through_delivery_and_reaping() {
    let (owners, threads, path) = distributed(1, 1, 0);
    let data = Arc::new(Semaphore::new(12));
    let control = Arc::new(Semaphore::new(8));
    let mut pairs = Vec::new();
    for _ in 0..12 {
        pairs.push(
            owners
                .prepare_pair(
                    Role::Operation,
                    data.clone().try_acquire_owned().unwrap(),
                    None,
                )
                .unwrap(),
        );
    }
    let full = owners
        .prepare_pair(
            Role::Operation,
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            None,
        )
        .is_err();
    let mut leases = Vec::new();
    let mut releases = Vec::new();
    let mut returns = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..8 {
        let mut prepared = owners
            .prepare_control(control.clone().try_acquire_owned().unwrap())
            .unwrap();
        let (release, gate) = oneshot::channel();
        let (at_return, returned) = oneshot::channel();
        prepared.slot.hooks.lock().unwrap().returning = Some((at_return, gate));
        let mut state = owners.shared.state.lock().unwrap();
        let id = state
            .payload
            .preparations
            .iter()
            .flatten()
            .find(|p| p.id == prepared.slot.id)
            .unwrap()
            .entry
            .as_ref()
            .unwrap()
            .handle
            .id();
        ids.push(id);
        let reserved = state.payload.reserve_control_locked(&mut prepared).unwrap();
        leases.push(reserved.install_control_locked());
        releases.push(release);
        returns.push(returned);
    }
    let hold = leases[0].retain_response().unwrap();
    let mut ready = true;
    for lease in &mut leases {
        ready &= matches!(
            tokio::time::timeout(WATCHDOG, lease.ready()).await,
            Ok(Ok(()))
        );
    }
    drop(pairs);
    drop(leases);
    let mut drain = Box::pin(owners.close_and_drain());
    let pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    let held = control.available_permits() == 0;
    drop(hold);
    let returned = tokio::time::timeout(WATCHDOG, async {
        for returned in returns {
            returned.await.map_err(|_| ())?;
        }
        Ok::<_, ()>(())
    })
    .await;
    let (same, records, no_work) = {
        let state = owners.shared.state.lock().unwrap();
        let r = &state.payload;
        (
            r.controls
                .iter()
                .map(|e| e.handle.id())
                .eq(ids.iter().copied()),
            r.records.iter().flatten().count(),
            r.controls
                .iter()
                .all(|e| e.slot.hooks.lock().unwrap().executed.is_empty()),
        )
    };
    for release in releases {
        let _ = release.send(());
    }
    finish_distributed(owners, threads, path).await;
    assert!(full && ready && pending && held && same && no_work && matches!(returned, Ok(Ok(()))));
    assert_eq!(records, 8);
    assert_eq!(control.available_permits(), 8);
    assert_eq!(data.available_permits(), 12);
}
#[tokio::test]
async fn operation_pair_rollback_original_handles_and_retained_publication() {
    let (owners, threads, path, process) = booted(1, 1, 1).await;
    let transport = Arc::new(Semaphore::new(1));
    let observed = observe_operation_publication(&owners, process, &transport, false).await;
    finish_distributed(owners, threads, path).await;
    assert!(observed.rollback && observed.ready && observed.same && observed.pending);
    assert!(observed.publication.is_ok());
    assert_eq!(observed.operations, 1);
    assert_eq!(observed.available_transport, 0);
    assert_eq!(transport.available_permits(), 1);
}

#[tokio::test]
async fn publication_initialization_failure_settles_before_observation_assertions() {
    use futures_util::FutureExt;

    let (owners, threads, path, process) = booted(1, 1, 1).await;
    let transport = Arc::new(Semaphore::new(1));
    // Catch an observation panic so this regression also cleans up when it is red.
    let observed = AssertUnwindSafe(observe_operation_publication(
        &owners, process, &transport, true,
    ))
    .catch_unwind()
    .await;
    finish_distributed(owners, threads, path.clone()).await;
    assert!(!path.exists());
    assert_eq!(transport.available_permits(), 1);
    let observed = observed.expect("failed readiness must reach teardown without panicking");
    assert!(observed.rollback && !observed.ready);
    assert_eq!(observed.publication, Err(OwnerError::Cancelled));
}

struct PublicationObservations {
    rollback: bool,
    ready: bool,
    publication: Result<()>,
    same: bool,
    pending: bool,
    operations: usize,
    available_transport: usize,
}

async fn observe_operation_publication(
    owners: &PayloadOwners,
    process: PayloadLease,
    transport: &Arc<Semaphore>,
    fail_initialization: bool,
) -> PublicationObservations {
    let mut prepared = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    prepared.account.slot.hooks.lock().unwrap().panic = fail_initialization;
    let mut original = Vec::with_capacity(32);
    let (rollback, installed) = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, local) = state.parts();
        let local = local.unwrap();
        original.extend(
            r.preparations
                .iter()
                .flatten()
                .map(|p| (p.id, p.entry.as_ref().unwrap().handle.id())),
        );
        let binding = local.fixture_binding(100);
        let before = local.counts();
        let reservation = r
            .reserve_pair_locked(
                local,
                &mut prepared,
                &process,
                binding.clone(),
                uuid::Uuid::from_u128(20100),
                None,
                tests::limits(),
            )
            .unwrap();
        drop(reservation);
        let rollback = local.counts() == before
            && r.positions.iter().flatten().count() == 1
            && r.records.iter().flatten().count() == 0
            && *process.slot.children.lock().unwrap() == 0;
        let reservation = r
            .reserve_pair_locked(
                local,
                &mut prepared,
                &process,
                binding,
                uuid::Uuid::from_u128(20100),
                None,
                tests::limits(),
            )
            .unwrap();
        (rollback, reservation.install_entry_locked())
    };
    drop(prepared);
    let mut account = installed.account.unwrap();
    let mut stream = installed.stream;
    let ready = matches!(
        tokio::time::timeout(WATCHDOG, account.ready()).await,
        Ok(Ok(_))
    ) && matches!(
        tokio::time::timeout(WATCHDOG, stream.ready()).await,
        Ok(Ok(_))
    );
    let publication = account.retain_delivery();
    let publication_result = publication.as_ref().map(|_| ()).map_err(|error| *error);
    let same = {
        let state = owners.shared.state.lock().unwrap();
        original.iter().all(|(id, handle)| {
            state
                .payload
                .entries
                .iter()
                .any(|e| e.slot.id == *id && e.handle.id() == *handle)
        })
    };
    drop(stream);
    drop(account);
    drop(process);
    let mut drain = Box::pin(owners.close_and_drain());
    let pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    let counts = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let charged = transport.available_permits();
    drop(publication);
    PublicationObservations {
        rollback,
        ready,
        publication: publication_result,
        same,
        pending,
        operations: counts.0,
        available_transport: charged,
    }
}

fn worker_gate(
    slot: &Arc<Slot>,
    discard: bool,
) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (at_gate, observed) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let mut hooks = slot.hooks.lock().unwrap();
    if discard {
        hooks.install_discard = Some((at_gate, gate));
    } else {
        hooks.install_command = Some((at_gate, gate));
    }
    (observed, release)
}
#[tokio::test]
async fn dependency_attempts_share_exact_account_and_closing_waits_for_child_and_account_reaping() {
    let (owners, threads, path, process) = booted(1, 1, 1).await;
    let transport = Arc::new(Semaphore::new(3));
    let binding = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .fixture_binding(2);
    let permit = uuid::Uuid::from_u128(20002);
    {
        let mut state = owners.shared.state.lock().unwrap();
        state
            .parts()
            .1
            .unwrap()
            .open_dependency(binding.clone(), permit)
            .unwrap();
    }
    let mut streams = Vec::new();
    let mut account_id = None;
    let mut reused = true;
    for attempt in 0..2 {
        let mut prepared = owners
            .prepare_pair(
                Role::Dependency,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap();
        let installed = {
            let mut state = owners.shared.state.lock().unwrap();
            let (r, local) = state.parts();
            r.reserve_pair_locked(
                local.unwrap(),
                &mut prepared,
                &process,
                binding.clone(),
                permit,
                Some((uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(4 + attempt))),
                tests::limits(),
            )
            .unwrap()
            .install_entry_locked()
        };
        drop(prepared);
        let mut stream = installed.stream;
        let ready = tokio::time::timeout(WATCHDOG, stream.ready()).await;
        reused &= matches!(ready, Ok(Ok(_))) && installed.account.is_none();
        let id = {
            let state = owners.shared.state.lock().unwrap();
            let mut ids = state
                .payload
                .entries
                .iter()
                .filter(|e| matches!(e.guards.owner(), Some(DistributedOwner::Dependency(_))))
                .map(|e| e.slot.id);
            let id = ids.next().unwrap();
            reused &= ids.next().is_none();
            id
        };
        if let Some(old) = account_id {
            reused &= old == id;
        } else {
            account_id = Some(id);
        }
        streams.push(stream);
    }
    let counts = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let mut wrong = owners
        .prepare_pair(
            Role::Dependency,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let mismatch = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, local) = state.parts();
        let local = local.unwrap();
        let other = local.fixture_binding(99);
        r.reserve_pair_locked(
            local,
            &mut wrong,
            &process,
            other,
            permit,
            Some((uuid::Uuid::from_u128(10), uuid::Uuid::from_u128(11))),
            tests::limits(),
        )
        .is_err()
    };
    let mut first = streams.remove(0);
    let (at_command, command_release) = worker_gate(&first.slot, false);
    let (at_discard, discard_release) = worker_gate(&first.slot, true);
    let (at_return, returned) = oneshot::channel();
    let (return_release, return_gate) = oneshot::channel();
    first.slot.hooks.lock().unwrap().returning = Some((at_return, return_gate));
    let (job, result) = tests::controls();
    let mut command = Box::pin(first.command(Command::BeginRows { job, result }));
    let command_pending = poll_fn(|cx| Poll::Ready(command.as_mut().poll(cx).is_pending())).await;
    let command_entered = tokio::time::timeout(WATCHDOG, at_command).await;
    drop(command);
    drop(first);
    let close = {
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .close_dependency_locked(&binding, permit)
    };
    let closing_refused = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, local) = state.parts();
        r.reserve_pair_locked(
            local.unwrap(),
            &mut wrong,
            &process,
            binding.clone(),
            permit,
            Some((uuid::Uuid::from_u128(12), uuid::Uuid::from_u128(13))),
            tests::limits(),
        )
        .is_err()
    };
    let before = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let blocked = transport.available_permits() == 0;
    drop(wrong);
    let _ = command_release.send(());
    let discard_entered = tokio::time::timeout(WATCHDOG, at_discard).await;
    let during = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let _ = discard_release.send(());
    let at_return = tokio::time::timeout(WATCHDOG, returned).await;
    let retained = {
        let state = owners.shared.state.lock().unwrap();
        state.local().unwrap().counts() == during
            && state
                .payload
                .entries
                .iter()
                .any(|e| e.slot.id == account_id.unwrap())
    };
    let mut drain = Box::pin(owners.close_and_drain());
    let pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    let _ = return_release.send(());
    drop(streams);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(
        reused && mismatch && closing_refused && command_pending && blocked && retained && pending
    );
    assert!(
        matches!(command_entered, Ok(Ok(())))
            && matches!(discard_entered, Ok(Ok(())))
            && matches!(at_return, Ok(Ok(())))
    );
    assert!(matches!(close, Ok(super::super::admission::Close::Pending)));
    assert_eq!(counts, (0, 0, 1, 1, 1));
    assert_eq!(before, counts);
    assert_eq!(during, counts);
    assert_eq!(transport.available_permits(), 3);
}

#[tokio::test]
async fn configured_stream_role_bound_and_operation_cleanup_leave_job_and_control_ownership_available()
 {
    let (owners, threads, path, process) = booted(17, 1, 0).await;
    let transport = Arc::new(Semaphore::new(17));
    let jobs = Arc::new(Semaphore::new(1));
    let controls = Arc::new(Semaphore::new(1));
    let mut accepted = Vec::new();
    let mut ready = true;
    for n in 100..116 {
        let mut pair = owners
            .prepare_pair(
                Role::Operation,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap();
        let mut installed = {
            let mut state = owners.shared.state.lock().unwrap();
            let (r, l) = state.parts();
            let l = l.unwrap();
            let b = l.fixture_binding(n);
            r.reserve_pair_locked(
                l,
                &mut pair,
                &process,
                b,
                uuid::Uuid::from_u128(n + 20000),
                None,
                tests::limits(),
            )
            .unwrap()
            .install_entry_locked()
        };
        drop(pair);
        ready &= matches!(
            tokio::time::timeout(WATCHDOG, installed.stream.ready()).await,
            Ok(Ok(_))
        );
        accepted.push(installed);
    }
    let mut overflow = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let (full, unchanged, bounds) = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, l) = state.parts();
        let l = l.unwrap();
        let before = l.counts();
        let b = l.fixture_binding(116);
        let full = r
            .reserve_pair_locked(
                l,
                &mut overflow,
                &process,
                b,
                uuid::Uuid::from_u128(20116),
                None,
                tests::limits(),
            )
            .is_err();
        (full, l.counts() == before, r.bounds())
    };
    drop(overflow);
    let first = accepted.remove(0);
    let account = first.account.unwrap();
    let account_slot = account.slot.clone();
    let (at_discard, discard_release) = worker_gate(&first.stream.slot, true);
    let (at_return, returned) = oneshot::channel();
    let (return_release, return_gate) = oneshot::channel();
    account_slot.hooks.lock().unwrap().returning = Some((at_return, return_gate));
    drop(first.stream);
    drop(account);
    let discard_entered = tokio::time::timeout(WATCHDOG, at_discard).await;
    let binding = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .fixture_binding(500);
    let permit = uuid::Uuid::from_u128(20500);
    {
        let mut state = owners.shared.state.lock().unwrap();
        state
            .parts()
            .1
            .unwrap()
            .open_dependency(binding.clone(), permit)
            .unwrap();
    }
    let mut pair = owners
        .prepare_pair(
            Role::Dependency,
            jobs.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let job = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, l) = state.parts();
        r.reserve_pair_locked(
            l.unwrap(),
            &mut pair,
            &process,
            binding,
            permit,
            Some((uuid::Uuid::from_u128(501), uuid::Uuid::from_u128(502))),
            tests::limits(),
        )
        .unwrap()
        .install_entry_locked()
    };
    drop(pair);
    let mut prepared = owners
        .prepare_control(controls.clone().try_acquire_owned().unwrap())
        .unwrap();
    let mut control = {
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .reserve_control_locked(&mut prepared)
            .unwrap()
            .install_control_locked()
    };
    let control_ready = tokio::time::timeout(WATCHDOG, control.ready()).await;
    let counts = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let _ = discard_release.send(());
    drop(prepared);
    drop(control);
    drop(job);
    drop(accepted);
    drop(process);
    let mut drain = Box::pin(owners.close_and_drain());
    let at_return = tokio::time::timeout(WATCHDOG, async {
        tokio::select! { r=returned=>r.map_err(|_|()), _=&mut drain=>Err(()) }
    })
    .await;
    drop(drain);
    let retained = {
        let state = owners.shared.state.lock().unwrap();
        state
            .payload
            .entries
            .iter()
            .any(|e| e.slot.id == account_slot.id && e.slot.cleanup.load(Ordering::Acquire))
            && state.local().unwrap().counts().0 > 0
    };
    let mut cancelled = Box::pin(owners.close_and_drain());
    let mut survivor = Box::pin(owners.close_and_drain());
    let both_pending = poll_fn(|cx| {
        Poll::Ready(
            cancelled.as_mut().poll(cx).is_pending() && survivor.as_mut().poll(cx).is_pending(),
        )
    })
    .await;
    drop(cancelled);
    let _ = return_release.send(());
    let survived = tokio::time::timeout(WATCHDOG, &mut survivor).await;
    drop(survivor);
    finish_distributed(owners, threads, path).await;
    assert!(ready && full && unchanged && retained && both_pending);
    assert!(
        matches!(discard_entered, Ok(Ok(())))
            && matches!(control_ready, Ok(Ok(())))
            && matches!(at_return, Ok(Ok(())))
            && matches!(survived, Ok(Ok(())))
    );
    assert_eq!(bounds, (1 + 17 + 4096 + 32, 17 + 1 + 64));
    assert_eq!((counts.0, counts.2), (16, 1));
    assert_eq!(transport.available_permits(), 17);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(controls.available_permits(), 1);
}

#[tokio::test]
async fn failed_pool_quarantines_original_distributed_keys_and_handles() {
    let (owners, threads, path, process) = booted(1, 1, 0).await;
    let transport = Arc::new(Semaphore::new(1));
    let mut prepared = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let mut installed = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, l) = state.parts();
        let l = l.unwrap();
        let b = l.fixture_binding(100);
        r.reserve_pair_locked(
            l,
            &mut prepared,
            &process,
            b,
            uuid::Uuid::from_u128(20100),
            None,
            tests::limits(),
        )
        .unwrap()
        .install_entry_locked()
    };
    drop(prepared);
    let ready = tokio::time::timeout(WATCHDOG, installed.stream.ready()).await;
    let joined = tokio::time::timeout(WATCHDOG, threads.shutdown()).await;
    let mut original = Vec::with_capacity(32);
    {
        let state = owners.shared.state.lock().unwrap();
        original.extend(
            state
                .payload
                .entries
                .iter()
                .map(|e| (e.slot.id, e.handle.id())),
        );
    }
    drop(installed);
    drop(process);
    let failed = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let retained = {
        let state = owners.shared.state.lock().unwrap();
        state.local().unwrap().counts().0 == 1
            && state.payload.failed
            && original.iter().all(|(id, h)| {
                state
                    .payload
                    .entries
                    .iter()
                    .any(|e| e.slot.id == *id && e.handle.id() == *h)
            })
    };
    let charged = transport.available_permits();
    // Existing intentional-failure recovery joins originals and disposes cells on its joined thread.
    tokio::time::timeout(WATCHDOG, tests::quarantine_teardown(&owners))
        .await
        .unwrap();
    let empty = std::fs::read_dir(&path).unwrap().count() == 0;
    std::fs::remove_dir(path).unwrap();
    assert!(
        matches!(ready, Ok(Ok(_)))
            && matches!(joined, Ok(Ok(())))
            && matches!(failed, Ok(Err(OwnerError::FailedDrain)))
    );
    assert!(retained && empty);
    assert_eq!(charged, 0);
    assert_eq!(transport.available_permits(), 1);
}

#[tokio::test]
async fn incomplete_preparation_and_checked_ids_refuse_without_extra_task_or_charge() {
    let (owners, threads, path) = distributed(1, 1, 0);
    let transport = Arc::new(Semaphore::new(25));
    let mut tickets = Vec::new();
    for _ in 0..23 {
        tickets.push(
            owners
                .prepare_entry(
                    Kind::Stream,
                    None,
                    Admission::distributed(
                        DistributedOwner::Prepared,
                        Some(Arc::new(transport.clone().try_acquire_owned().unwrap())),
                    ),
                    false,
                )
                .unwrap(),
        );
    }
    let mut original = Vec::with_capacity(32);
    let before = {
        let state = owners.shared.state.lock().unwrap();
        original.extend(
            state
                .payload
                .preparations
                .iter()
                .flatten()
                .map(|p| (p.id, p.entry.as_ref().unwrap().handle.id())),
        );
        state.payload.next
    };
    let refused = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .is_err();
    let unchanged = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.next == before
            && state
                .payload
                .preparations
                .iter()
                .flatten()
                .map(|p| (p.id, p.entry.as_ref().unwrap().handle.id()))
                .eq(original.iter().copied())
    };
    let charged = transport.available_permits();
    let scratch = std::fs::read_dir(&path).unwrap().count();
    drop(tickets);
    let reaped = tokio::time::timeout(WATCHDOG, wait_reap(&owners.shared, None)).await;
    let deadline = Instant::now() + std::time::Duration::from_secs(3600);
    let pair = owners
        .prepare_pair(
            Role::Dependency,
            transport.clone().try_acquire_owned().unwrap(),
            Some(deadline),
        )
        .unwrap();
    let account_lifetime =
        pair.account.slot.deadline.is_none() && pair.stream.slot.deadline == Some(deadline);
    drop(pair);
    let deadline_reaped = tokio::time::timeout(WATCHDOG, wait_reap(&owners.shared, None)).await;
    owners.shared.state.lock().unwrap().payload.next = u64::MAX - 1;
    let exhausted = owners
        .prepare_pair(
            Role::Dependency,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .is_err();
    let no_placeholder = {
        let s = owners.shared.state.lock().unwrap();
        s.payload.next == u64::MAX - 1 && s.payload.preparations.iter().all(Option::is_none)
    };
    finish_distributed(owners, threads, path).await;
    assert!(refused && unchanged && exhausted && no_placeholder && matches!(reaped, Ok(Ok(()))));
    assert!(
        account_lifetime && matches!(deadline_reaped, Ok(Ok(()))),
        "account lifetime must be independent of its first attempt deadline"
    );
    assert_eq!(charged, 2);
    assert_eq!(scratch, 0);
    assert_eq!(transport.available_permits(), 25);
}

#[tokio::test]
async fn tentative_local_conflict_rolls_back_both_parent_claims_and_positions() {
    let (owners, threads, path, process) = booted(2, 1, 0).await;
    let transport = Arc::new(Semaphore::new(2));
    let mut first = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let installed = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, l) = state.parts();
        let l = l.unwrap();
        let b = l.fixture_binding(100);
        r.reserve_pair_locked(
            l,
            &mut first,
            &process,
            b,
            uuid::Uuid::from_u128(20100),
            None,
            tests::limits(),
        )
        .unwrap()
        .install_entry_locked()
    };
    drop(first);
    let mut second = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let mut original_positions = vec![None; 1 + 2 + 4096 + 32];
    let (refused, unchanged) = {
        let mut state = owners.shared.state.lock().unwrap();
        let (r, l) = state.parts();
        let l = l.unwrap();
        let b = l.fixture_binding(100);
        original_positions.copy_from_slice(&r.positions);
        let before = (
            l.counts(),
            r.records.iter().flatten().count(),
            *process.slot.children.lock().unwrap(),
        );
        let refused = r
            .reserve_pair_locked(
                l,
                &mut second,
                &process,
                b,
                uuid::Uuid::from_u128(20100),
                None,
                tests::limits(),
            )
            .is_err();
        (
            refused,
            l.counts() == before.0
                && r.positions == original_positions
                && r.records.iter().flatten().count() == before.1
                && *process.slot.children.lock().unwrap() == before.2
                && *second.account.slot.children.lock().unwrap() == 0,
        )
    };
    drop(second);
    drop(installed);
    drop(process);
    owners.release_pending();
    finish_distributed(owners, threads, path).await;
    assert!(refused && unchanged);
    assert_eq!(transport.available_permits(), 2);
}

#[tokio::test]
async fn accepted_record_exhaustion_retains_refused_original_preparation() {
    let (legacy, threads, path, _) = tests::fixture();
    let mut state = super::super::admission::distributed_fixture(1, 1, 0);
    let production_bounds = state.payload.bounds();
    // Physical exhaustion fault fixture, before this state has a mutex or owners.
    // Production always reserves the full exact O+P+64 table above.
    state.payload.records.truncate(1);
    let shared = Arc::new(Mutex::new(state));
    let owners = PayloadOwners::from_receiver(legacy.shared.pool.clone(), shared.clone());
    let same_state = Arc::ptr_eq(&shared, &owners.shared.state);
    let transport = Arc::new(Semaphore::new(2));
    let mut first = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .unwrap();
    let first_control = {
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .reserve_control_locked(&mut first)
            .unwrap()
            .install_control_locked()
    };
    let mut second = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .unwrap();
    let (refused, same) = {
        let mut state = owners.shared.state.lock().unwrap();
        let r = &mut state.payload;
        let handle = r
            .preparations
            .iter()
            .flatten()
            .find(|p| p.id == second.slot.id)
            .unwrap()
            .entry
            .as_ref()
            .unwrap()
            .handle
            .id();
        let refused = r.reserve_control_locked(&mut second).is_err();
        (
            refused,
            r.records.iter().flatten().count() == 1
                && r.preparations.iter().flatten().any(|p| {
                    p.id == second.slot.id && p.entry.as_ref().unwrap().handle.id() == handle
                }),
        )
    };
    let charged = transport.available_permits();
    drop(second);
    drop(first);
    drop(first_control);
    finish_distributed(owners, threads, path).await;
    assert!(same_state);
    assert_eq!(production_bounds, (1 + 1 + 4096 + 32, 1 + 1 + 64));
    assert!(refused && same);
    assert_eq!(charged, 0);
    assert_eq!(transport.available_permits(), 2);
}

// A boot receipt cannot be duplicated into two initialization completions.
const _: fn() = || {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <ProcessReady as AmbiguousIfClone<_>>::marker;
};

// A signed receiver fixture; all execution/teardown uses the existing owner fixtures.
use crate::cluster::{
    configuration::{Template, Timing},
    member::NodeState,
    state::Control,
};
use crate::node::auth::{
    ActualRoute, Auth, CheckedContent, Error, ReceiverIdentity, RequestBody, SignedRequest,
    WorkerSigningKey, check_request_authority, check_request_content, verify_request,
    worker_verifier,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use uuid::Uuid;
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
struct ReceiverProofs {
    key: WorkerSigningKey,
    api: Ed25519KeyPair,
    root: Control,
}
impl ReceiverProofs {
    fn new() -> Self {
        let key = WorkerSigningKey::new().unwrap();
        let api = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
        let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
            .unwrap()
            .fingerprint()
            .unwrap();
        let value = json!({"version":1,"cluster_id":id(1),"name":"Synthetic","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":hex(&worker_verifier(id(1),id(3),id(4),id(5),&key.public_key()))},"drain":null}],"intents":[]});
        let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
        Self { key, api, root }
    }
    fn local(&self) -> ReceiverIdentity {
        ReceiverIdentity::new(id(1), id(3), id(4), id(5), &self.key)
    }
    fn pin(&self) -> [u8; 32] {
        self.api.public_key().as_ref().try_into().unwrap()
    }
    fn auth(&self, scope: &str, body: &[u8]) -> (String, ActualRoute) {
        let worker = matches!(scope, "job" | "dependency_open" | "dependency_close");
        let path = match scope {
            "operation" => "/internal/v1/operations",
            "job" => "/internal/v1/jobs",
            "dependency_open" => "/internal/v1/dependencies/open",
            "dependency_close" => "/internal/v1/dependencies/close",
            "readiness" => "/internal/v1/readiness",
            _ => "/internal/v1/drain",
        };
        let public = if worker {
            self.key.public_key()
        } else {
            self.pin()
        };
        let sender = format!(
            r#"{{"role":"{}","process_id":"{}","node_id":{},"incarnation":{},"session_id":{},"public_key":"{}"}}"#,
            if worker { "worker" } else { "api" },
            if worker { id(4) } else { id(2) },
            if worker { json!(id(3)) } else { json!(null) },
            if worker { json!(id(4)) } else { json!(null) },
            if worker { json!(id(5)) } else { json!(null) },
            hex(&public)
        );
        let op = worker || scope == "operation";
        let hash = hex(ring::digest::digest(&ring::digest::SHA256, body).as_ref());
        let text = format!(
            r#"{{"version":1,"cluster_id":"{}","scope":"{scope}","sender":{sender},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":1,"method":"POST","path":"{path}","operation_id":{},"admission_id":{},"job_id":{},"attempt_id":{},"request_digest":{},"control_sha256":"{hash}","dependency_id":{}}}"#,
            id(1),
            id(3),
            id(4),
            id(5),
            if op { json!(id(6)) } else { json!(null) },
            if worker { json!(id(7)) } else { json!(null) },
            if scope == "job" {
                json!(id(8))
            } else {
                json!(null)
            },
            if scope == "job" {
                json!(id(9))
            } else {
                json!(null)
            },
            if op {
                json!(if scope == "operation" {
                    hash.clone()
                } else {
                    "a".repeat(64)
                })
            } else {
                json!(null)
            },
            if scope == "job" {
                json!(id(10))
            } else {
                json!(null)
            }
        );
        (
            text,
            ActualRoute::new(
                "POST",
                path,
                if scope == "job" {
                    Some("00000000-0000-0000-0000-00000000000a")
                } else {
                    None
                },
            )
            .unwrap(),
        )
    }
    fn signed(&self, scope: &str, body: &[u8]) -> SignedRequest {
        let (text, route) = self.auth(scope, body);
        self.sign_text(&text, &route)
    }
    fn sign_text(&self, text: &str, route: &ActualRoute) -> SignedRequest {
        let auth = Auth::decode(&hex(text.as_bytes()), route).unwrap();
        let sig = if auth.sender().role() == crate::node::auth::Role::Worker {
            self.key.sign_request(&auth).unwrap().to_vec()
        } else {
            let mut input = b"meshdb.http-request.v1\0".to_vec();
            input.extend_from_slice(text.as_bytes());
            self.api.sign(&input).as_ref().to_vec()
        };
        verify_request(&hex(text.as_bytes()), &hex(&sig), route).unwrap()
    }
}

impl ReceiverProofs {
    fn request(
        &self,
        scope: &str,
        seq: u64,
        boot: Arc<ReceiverIdentity>,
    ) -> (super::super::admission::DurableRequestView, CheckedContent) {
        let (view, content, _) = self.request_with_bytes(scope, seq, boot);
        (view, content)
    }
    fn request_with_bytes(
        &self,
        scope: &str,
        seq: u64,
        boot: Arc<ReceiverIdentity>,
    ) -> (
        super::super::admission::DurableRequestView,
        CheckedContent,
        usize,
    ) {
        let body = match scope {
            "operation" => format!(r#"{{"request_id":"{}","operation":"table.list","payload":{{}}}}"#, id(6)),
            "readiness" => format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)),
            "dependency_open" | "dependency_close" => format!(r#"{{"version":1,"permit_id":"{}"}}"#, id(10)),
            "drain" => format!(r#"{{"version":1,"drain_id":"{}"}}"#, id(11)),
            "job" => serde_json::to_string(&json!({"version":1,"cluster_id":id(1),"operation_id":id(6),"job_id":id(8),"attempt_id":id(9),"request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":id(15),"catalog_etag":"token"},"ordinal":0,"table":{"id":id(16),"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":format!("tables/{}/{}.parquet",id(16),id(17)),"row_count":1}},"key":1}}})).unwrap(),
            _ => unreachable!(),
        };
        let mut body = if scope == "job" {
            body.replace(&id(9).to_string(), &id(seq as u128 + 7).to_string())
                .into_bytes()
        } else {
            body.into_bytes()
        };
        body.shrink_to_fit();
        let pointer = body.as_ptr() as usize;
        let (text, route) = self.auth(scope, &body);
        let text = if scope == "job" {
            text.replace(&id(9).to_string(), &id(seq as u128 + 7).to_string())
        } else {
            text
        };
        let text = text.replace("\"sequence\":1", &format!("\"sequence\":{seq}"));
        let signed = self.sign_text(&text, &route);
        let eligible = check_request_authority(&signed, &self.root, &boot, &self.pin()).unwrap();
        let content = check_request_content(&signed, body).unwrap();
        (
            super::super::admission::DurableRequestView::prepare(&self.root, eligible, boot)
                .unwrap(),
            content,
            pointer,
        )
    }
}

async fn signed_booted_with_queue(
    queue: usize,
) -> (
    ReceiverProofs,
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    PathBuf,
    PayloadLease,
) {
    use super::super::admission::{LocalAdmission, ReceiverState};
    let f = ReceiverProofs::new();
    let body = format!(r#"{{"version":1,"permit_id":"{}"}}"#, id(10));
    let signed = f.signed("dependency_open", body.as_bytes());
    let local = LocalAdmission::new(
        id(1),
        &f.key,
        Arc::new(signed.auth().sender().clone()),
        id(30),
        1,
        1,
        queue,
    )
    .unwrap();
    let boot = local.boot();
    let (legacy, threads, path, _) = tests::fixture();
    let state = Arc::new(Mutex::new(ReceiverState::distributed(local)));
    let owners = PayloadOwners::from_receiver(legacy.shared.pool.clone(), state);
    let mut process = owners
        .start_boot(&boot, id(30), path.clone(), tests::limits())
        .unwrap();
    let ready = tokio::time::timeout(WATCHDOG, process.process_ready()).await;
    let initialized = ready
        .ok()
        .and_then(|r| r.ok())
        .is_some_and(|r| owners.shared.state.lock().unwrap().fixture_initialize(r));
    if !initialized {
        drop(process);
        finish_distributed(owners, threads, path).await;
        panic!("signed boot fixture failed after teardown");
    }
    (f, owners, threads, path, process)
}

async fn signed_booted() -> (
    ReceiverProofs,
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    PathBuf,
    PayloadLease,
) {
    signed_booted_with_queue(0).await
}

#[tokio::test]
async fn receiver_signed_replay_and_response_reservation() {
    use super::super::admission::{AcceptError, AcceptedRoute, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(8));
    let mut first = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let mut second = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let a = f.request("readiness", 1, boot.clone());
    let b = f.request("readiness", 1, boot.clone());
    let (winner, loser) = {
        let mut state = owners.shared.state.lock().unwrap();
        (
            state.accept_locked(a.0, a.1, &mut first, &process),
            state.accept_locked(b.0, b.1, &mut second, &process),
        )
    };
    let accepted = matches!(&winner, Ok(AcceptedRoute::Readiness(_)));
    let replay = matches!(loser, Err(AcceptError::Auth(Error::Replay)));
    drop(winner);
    drop(first);
    drop(second);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(accepted && replay);
}

#[tokio::test]
async fn receiver_operation_capacity_payload_and_retained_start() {
    use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(8));
    let mut first = PreparedRoute::Operation(
        owners
            .prepare_pair(
                Role::Operation,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
        OperationCandidates::new(),
    );
    let mut second = PreparedRoute::Operation(
        owners
            .prepare_pair(
                Role::Operation,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
        OperationCandidates::new(),
    );
    let a = f.request_with_bytes("operation", 1, boot.clone());
    let original_bytes = a.2;
    let original_parsed = a.1.operation().unwrap() as *const crate::protocol::Request as usize;
    let b = f.request("operation", 2, boot.clone());
    let winner = owners.accept(a.0, a.1, &mut first, &process);
    let full = owners.accept(b.0, b.1, &mut second, &process).is_err();
    let mut facts = (false, false, false, false, false);
    if let Ok(AcceptedRoute::Operation(mut accepted)) = winner {
        let started = accepted
            .start
            .check(&f.root, &f.pin())
            .and_then(|check| owners.shared.state.lock().unwrap().start_locked(check));
        let mut account = accepted.owners.account.take().unwrap();
        let ready = tokio::time::timeout(WATCHDOG, account.ready()).await;
        let stream_ready = tokio::time::timeout(WATCHDOG, accepted.owners.stream.ready()).await;
        let payload = account.take_request();
        let second_take = account.take_request().is_err();
        let same_box = payload.as_ref().is_ok_and(|p| p.bytes.as_ptr() as usize == original_bytes && matches!(&p.body, RequestBody::Operation(r) if r.as_ref() as *const crate::protocol::Request as usize == original_parsed));
        let authority_after_take = accepted.start.check(&f.root, &f.pin()).is_ok();
        facts = (
            started.is_ok(),
            matches!(ready, Ok(Ok(Reply::Ready))) && matches!(stream_ready, Ok(Ok(Reply::Ready))),
            same_box,
            second_take,
            authority_after_take,
        );
        drop(payload);
        drop(accepted.owners.stream);
        let disposed = tokio::time::timeout(WATCHDOG, account.dispose()).await;
        facts.1 &= matches!(disposed, Ok(Ok(())));
        // The surviving subject retains immutable authority, not the body or owner.
        facts.4 &= accepted.start.check(&f.root, &f.pin()).is_ok();
        let retry = f.request("operation", 2, boot);
        let retried = owners.accept(retry.0, retry.1, &mut second, &process);
        facts.0 &= matches!(retried, Ok(AcceptedRoute::Operation(_)));
        drop(retried);
    }
    drop(first);
    drop(second);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(
        full && facts.0 && facts.1 && facts.2 && facts.3 && facts.4,
        "{facts:?}"
    );
}

#[tokio::test]
async fn receiver_control_response_failure_rolls_back_without_spending() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let entry = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .unwrap();
    let slot = entry.slot.clone();
    let mut route = PreparedRoute::Control(entry);
    *slot.delivery_holds.lock().unwrap() = usize::MAX;
    let proof = f.request("dependency_open", 1, boot.clone());
    let refused = owners
        .accept(proof.0, proof.1, &mut route, &process)
        .is_err();
    let rolled_back = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts()
        .4
        == 0;
    *slot.delivery_holds.lock().unwrap() = 0;
    let proof = f.request("dependency_open", 1, boot);
    let result = owners.accept(proof.0, proof.1, &mut route, &process);
    let accepted = matches!(&result, Ok(AcceptedRoute::Open(_)));
    drop(result);
    drop(route);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(refused && rolled_back && accepted);
}

#[tokio::test]
async fn receiver_first_draining_job_and_closing_retained_start() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted_with_queue(1).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(8));
    let mut open = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let proof = f.request("dependency_open", 1, boot.clone());
    let opened = owners.accept(proof.0, proof.1, &mut open, &process);
    let open_ok = matches!(&opened, Ok(AcceptedRoute::Open(_)));
    // A checked older operation/open remains independently signed when the job
    // is the first request to bring the receiver's newer durable barrier.
    let old_operation = f.request("operation", 1, boot.clone());
    let old_open = f.request("dependency_open", 4, boot.clone());
    f.root.revision += 1;
    f.root.change_id = id(40);
    f.root.nodes[0].state = NodeState::Draining;
    f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
        id: id(11),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    f.root = Control::decode(&f.root.encode().unwrap()).unwrap();
    let mut blocker = PreparedRoute::Job(
        owners
            .prepare_pair(
                Role::Dependency,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
    );
    let proof = f.request("job", 2, boot.clone());
    let blocker_accepted = owners.accept(proof.0, proof.1, &mut blocker, &process);
    let proof = f.request("job", 3, boot.clone());
    let original = proof.1.job().unwrap() as *const JobEnvelope as usize;
    let mut job = PreparedRoute::Job(
        owners
            .prepare_pair(
                Role::Dependency,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
    );
    let result = owners.accept(proof.0, proof.1, &mut job, &process);
    let mut facts = (false, false, false, false, false);
    if let Ok(AcceptedRoute::Job(mut accepted)) = result {
        let mut close = PreparedRoute::Control(
            owners
                .prepare_control(transport.clone().try_acquire_owned().unwrap())
                .unwrap(),
        );
        let proof = f.request("dependency_close", 4, boot.clone());
        let closed = owners.accept(proof.0, proof.1, &mut close, &process);
        facts.0 = matches!(&closed, Ok(AcceptedRoute::Close(_)));
        // Closing preserves this queued accepted use. Release the other actual
        // registered worker and its charge before promotion.
        let blocker_settled = if let Ok(AcceptedRoute::Job(blocking)) = blocker_accepted {
            matches!(
                tokio::time::timeout(WATCHDOG, blocking.owners.stream.dispose()).await,
                Ok(Ok(()))
            )
        } else {
            false
        };
        facts.0 &= blocker_settled;
        let check = accepted.start.check(&f.root, &f.pin());
        facts.1 = check
            .and_then(|c| owners.shared.state.lock().unwrap().start_locked(c))
            .is_ok();
        let mut later = PreparedRoute::Job(
            owners
                .prepare_pair(
                    Role::Dependency,
                    transport.clone().try_acquire_owned().unwrap(),
                    None,
                )
                .unwrap(),
        );
        let proof = f.request("job", 5, boot.clone());
        facts.2 = owners
            .accept(proof.0, proof.1, &mut later, &process)
            .is_err();
        let ready = tokio::time::timeout(WATCHDOG, accepted.owners.stream.ready()).await;
        let payload = accepted.owners.stream.take_request();
        facts.3 = matches!(ready, Ok(Ok(Reply::Ready))) && payload.as_ref().is_ok_and(|p| matches!(&p.body, RequestBody::Job(j) if j.as_ref() as *const JobEnvelope as usize == original));
        facts.4 = accepted.start.check(&f.root, &f.pin()).is_ok();
        // Transfer the original parsed box into an actually accepted worker command.
        if let Ok(OwnedRequestPayload {
            bytes,
            body: RequestBody::Job(job),
            delivery,
        }) = payload
        {
            let (entered, seen) = oneshot::channel();
            let (release, gate) = std::sync::mpsc::channel();
            accepted
                .owners
                .stream
                .slot
                .hooks
                .lock()
                .unwrap()
                .install_command = Some((entered, gate));
            let observed = {
                let command = accepted
                    .owners
                    .stream
                    .command(Command::StartSend { job, request: true });
                tokio::pin!(command);
                tokio::time::timeout(WATCHDOG, async {
                    tokio::select! { _ = &mut command => false, result = seen => result.is_ok() }
                })
                .await
            }; // Cancel the observer while the original accepted command is gated.
            let retained = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .retention_locked()
                .accepted
                > 1;
            let _ = release.send(());
            let command_settled = tokio::time::timeout(WATCHDOG, async {
                owners
                    .shared
                    .pool
                    .submit(0, |_, _| Ok(()))
                    .await?
                    .receive()
                    .await
            })
            .await;
            drop(bytes);
            drop(delivery);
            facts.3 &= matches!(command_settled, Ok(Ok(())));
            let settled = tokio::time::timeout(WATCHDOG, accepted.owners.stream.dispose()).await;
            facts.3 &= matches!(observed, Ok(true)) && retained && matches!(settled, Ok(Ok(())));
        }
        drop(closed);
        drop(close);
        drop(later);
    }
    let mut op = PreparedRoute::Operation(
        owners
            .prepare_pair(
                Role::Operation,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
        super::super::admission::OperationCandidates::new(),
    );
    let mut new_open = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let refused_operation = owners
        .accept(old_operation.0, old_operation.1, &mut op, &process)
        .is_err();
    let refused_open = owners
        .accept(old_open.0, old_open.1, &mut new_open, &process)
        .is_err();
    drop(opened);
    drop(open);
    drop(job);
    drop(blocker);
    drop(op);
    drop(new_open);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(
        open_ok
            && refused_operation
            && refused_open
            && facts.0
            && facts.1
            && facts.2
            && facts.3
            && facts.4,
        "{facts:?}"
    );
}

#[tokio::test]
async fn receiver_same_sequence_contenders_install_exactly_one_operation() {
    use super::super::admission::{AcceptError, AcceptedRoute, OperationCandidates, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted_with_queue(1).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(2));
    let contenders: Vec<_> = (0..2)
        .map(|_| {
            let proof = f.request("operation", 1, boot.clone());
            let pair = owners
                .prepare_pair(
                    Role::Operation,
                    transport.clone().try_acquire_owned().unwrap(),
                    None,
                )
                .unwrap();
            let original = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .preparations
                .iter()
                .flatten()
                .find(|p| p.id == pair.account.slot.id)
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .handle
                .id();
            (
                proof,
                PreparedRoute::Operation(pair, OperationCandidates::new()),
                original,
            )
        })
        .collect();
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let mut gates = Vec::new();
        for (proof, mut route, original) in contenders {
            let (release, gate) = std::sync::mpsc::channel();
            gates.push(release);
            let state = &owners.shared.state;
            let process = &process;
            handles.push(scope.spawn(move || {
                let gate = gate.recv_timeout(WATCHDOG);
                let result = if gate.is_ok() {
                    state
                        .lock()
                        .unwrap()
                        .accept_locked(proof.0, proof.1, &mut route, process)
                } else {
                    Err(AcceptError::Auth(Error::Invalid))
                };
                (result, route, original)
            }));
        }
        for gate in gates {
            let _ = gate.send(());
        }
        handles.into_iter().map(|h| h.join()).collect::<Vec<_>>()
    });
    owners.release_pending();
    let mut wins = 0;
    let mut replays = 0;
    let mut same_handle = false;
    for (result, route, original) in results.into_iter().flatten() {
        match &result {
            Ok(AcceptedRoute::Operation(a)) => {
                wins += 1;
                same_handle = owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .entries
                    .iter()
                    .any(|e| {
                        e.handle.id() == original
                            && a.owners
                                .account
                                .as_ref()
                                .is_some_and(|a| Arc::ptr_eq(&a.slot, &e.slot))
                    });
            }
            Err(AcceptError::Auth(Error::Replay)) => replays += 1,
            _ => {}
        }
        drop(result);
        drop(route);
    }
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(wins == 1 && replays == 1 && same_handle);
}

#[tokio::test]
async fn receiver_lost_authority_preserves_queued_owner_and_divergence_is_sticky() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted_with_queue(1).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(6));
    let mut open = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let proof = f.request("dependency_open", 1, boot.clone());
    let opened = owners.accept(proof.0, proof.1, &mut open, &process);
    let mut job = PreparedRoute::Job(
        owners
            .prepare_pair(
                Role::Dependency,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
    );
    let proof = f.request("job", 2, boot.clone());
    let accepted = owners.accept(proof.0, proof.1, &mut job, &process);
    let mut queued = PreparedRoute::Job(
        owners
            .prepare_pair(
                Role::Dependency,
                transport.clone().try_acquire_owned().unwrap(),
                None,
            )
            .unwrap(),
    );
    let proof = f.request("job", 3, boot.clone());
    let queued_accepted = owners.accept(proof.0, proof.1, &mut queued, &process);
    let before = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts();
    let authority_refused = if let Ok(AcceptedRoute::Job(a)) = &queued_accepted {
        f.root.nodes[0].state = NodeState::Lost;
        f.root.revision += 1;
        f.root.change_id = id(41);
        let lost = a.start.check(&f.root, &f.pin()).is_err();
        f.root.nodes[0].state = NodeState::Ready;
        let verifier = f.root.nodes[0].session.as_ref().unwrap().verifier.clone();
        f.root.nodes[0].session.as_mut().unwrap().verifier = "0".repeat(64);
        let replaced = a.start.check(&f.root, &f.pin()).is_err();
        f.root.nodes[0].session.as_mut().unwrap().verifier = verifier;
        lost && replaced
    } else {
        false
    };
    let retained = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .counts()
        == before;
    // Order one newer complete root, then a conflicting root at the same revision.
    let mut control = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let proof = f.request("readiness", 1, boot.clone());
    let first = owners.accept(proof.0, proof.1, &mut control, &process);
    f.root.change_id = id(42);
    let mut conflicting = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let proof = f.request("readiness", 2, boot.clone());
    let divergence = owners
        .accept(proof.0, proof.1, &mut conflicting, &process)
        .is_err();
    f.root.revision += 1;
    f.root.change_id = id(43);
    let proof = f.request("readiness", 2, boot);
    let sticky = owners
        .accept(proof.0, proof.1, &mut conflicting, &process)
        .is_err();
    drop(first);
    drop(control);
    drop(conflicting);
    drop(queued_accepted);
    drop(queued);
    drop(accepted);
    drop(job);
    drop(opened);
    drop(open);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(before.3 == 1 && authority_refused && retained && divergence && sticky);
}

#[tokio::test]
async fn receiver_foreign_boot_and_independent_content_do_not_spend() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let foreign = ReceiverProofs::new();
    let proof = foreign.request("readiness", 1, Arc::new(foreign.local()));
    let transport = Arc::new(Semaphore::new(1));
    let mut route = PreparedRoute::Control(
        owners
            .prepare_control(transport.clone().try_acquire_owned().unwrap())
            .unwrap(),
    );
    let foreign_refused = owners
        .accept(proof.0, proof.1, &mut route, &process)
        .is_err();
    let a = f.request("readiness", 1, boot.clone());
    let b = f.request("readiness", 1, boot.clone());
    let mismatched = owners.accept(a.0, b.1, &mut route, &process).is_err();
    let proof = f.request("readiness", 1, boot);
    let result = owners.accept(proof.0, proof.1, &mut route, &process);
    let accepted = matches!(&result, Ok(AcceptedRoute::Readiness(_)));
    drop(result);
    drop(route);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(foreign_refused && mismatched && accepted);
}

#[tokio::test]
async fn receiver_open_and_permanent_barrier_both_lock_orders() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let mut outcomes = Vec::new();
    for open_first in [true, false] {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let transport = Arc::new(Semaphore::new(2));
        let mut open = PreparedRoute::Control(
            owners
                .prepare_control(transport.clone().try_acquire_owned().unwrap())
                .unwrap(),
        );
        let mut drain = PreparedRoute::Control(
            owners
                .prepare_control(transport.clone().try_acquire_owned().unwrap())
                .unwrap(),
        );
        let open_proof = f.request("dependency_open", 1, boot.clone());
        f.root.revision += 1;
        f.root.change_id = id(40);
        f.root.nodes[0].state = NodeState::Draining;
        f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
            id: id(11),
            requested_at_ms: 0,
            ack_seq: None,
            confirmed_idle: false,
        });
        let drain_proof = f.request("drain", 1, boot);
        let (opened, drained) = {
            let mut state = owners.shared.state.lock().unwrap();
            if open_first {
                let o = state.accept_locked(open_proof.0, open_proof.1, &mut open, &process);
                (
                    o,
                    state.accept_locked(drain_proof.0, drain_proof.1, &mut drain, &process),
                )
            } else {
                let d = state.accept_locked(drain_proof.0, drain_proof.1, &mut drain, &process);
                (
                    state.accept_locked(open_proof.0, open_proof.1, &mut open, &process),
                    d,
                )
            }
        };
        let barrier = owners
            .shared
            .state
            .lock()
            .unwrap()
            .snapshot_locked()
            .local
            .unwrap()
            .drain_id()
            == Some(id(11));
        outcomes.push(
            matches!(&opened, Ok(AcceptedRoute::Open(_))) == open_first
                && matches!(&drained, Ok(AcceptedRoute::Drain(_)))
                && barrier,
        );
        drop(opened);
        drop(drained);
        drop(open);
        drop(drain);
        drop(process);
        finish_distributed(owners, threads, path).await;
    }
    assert!(outcomes.into_iter().all(|ok| ok));
}

#[tokio::test]
async fn receiver_original_request_survives_publication_discard_and_unreaped_return() {
    use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
    let (f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let pair = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let slot = pair.account.slot.clone();
    let original = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .preparations
        .iter()
        .flatten()
        .find(|p| p.id == slot.id)
        .unwrap()
        .entry
        .as_ref()
        .unwrap()
        .handle
        .id();
    let (discarded, at_discard) = oneshot::channel();
    let (release_discard, discard_gate) = std::sync::mpsc::channel();
    let (returning, at_return) = oneshot::channel();
    let (release_return, return_gate) = oneshot::channel();
    {
        let mut hooks = slot.hooks.lock().unwrap();
        hooks.install_discard = Some((discarded, discard_gate));
        hooks.returning = Some((returning, return_gate));
    }
    let proof = f.request("operation", 1, boot);
    let mut route = PreparedRoute::Operation(pair, OperationCandidates::new());
    let result = owners.accept(proof.0, proof.1, &mut route, &process);
    let mut ready = false;
    let mut publication = Err(OwnerError::Refused);
    let mut subject = None;
    if let Ok(AcceptedRoute::Operation(mut accepted)) = result {
        if let Some(mut account) = accepted.owners.account.take() {
            ready = matches!(
                tokio::time::timeout(WATCHDOG, account.ready()).await,
                Ok(Ok(Reply::Ready))
            ) && matches!(
                tokio::time::timeout(WATCHDOG, accepted.owners.stream.ready()).await,
                Ok(Ok(Reply::Ready))
            );
            publication = account.retain_delivery();
            drop(account);
        }
        subject = Some(accepted.start);
        drop(accepted.owners.stream);
    }
    drop(route);
    drop(process);
    let retained = || {
        let state = owners.shared.state.lock().unwrap();
        state.payload.entries.iter().any(|e| {
            e.slot.id == slot.id
                && e.handle.id() == original
                && e.request.authority.get().is_some()
                && e.request.payload.is_some()
        }) && state.local().unwrap().counts().0 == 1
    };
    let publication_ok = publication.is_ok();
    let mut drain = Box::pin(owners.close_and_drain());
    let pending = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await;
    drop(drain);
    let during_publication = retained();
    drop(publication);
    let mut drain = Box::pin(owners.close_and_drain());
    let discarded = tokio::time::timeout(WATCHDOG, async {
        tokio::select! { r = at_discard => r.is_ok(), _ = &mut drain => false }
    })
    .await;
    drop(drain);
    let during_discard = retained();
    let _ = release_discard.send(());
    let mut drain = Box::pin(owners.close_and_drain());
    let returning = tokio::time::timeout(WATCHDOG, async {
        tokio::select! { r = at_return => r.is_ok(), _ = &mut drain => false }
    })
    .await;
    drop(drain);
    let before_return = retained() && slot.cleanup.load(Ordering::Acquire);
    let _ = release_return.send(());
    drop(subject);
    finish_distributed(owners, threads, path).await;
    assert!(
        ready
            && publication_ok
            && pending
            && during_publication
            && during_discard
            && before_return
            && matches!(discarded, Ok(true))
            && matches!(returning, Ok(true))
    );
    assert_eq!(transport.available_permits(), 1);
}

#[tokio::test]
async fn receiver_association_foreign_start_preserves_local_replay_and_start() {
    use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
    let (a, owners_a, threads_a, path_a, process_a) = signed_booted().await;
    let (b, owners_b, threads_b, path_b, process_b) = signed_booted().await;
    let boot_a = owners_a
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .boot();
    let boot_b = owners_b
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .boot();
    let independent =
        !boot_a.same_boot(&boot_b) && a.root.revision == b.root.revision && a.root != b.root;
    let transport = Arc::new(Semaphore::new(3));
    let mut route_a = owners_a
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .map(|pair| PreparedRoute::Operation(pair, OperationCandidates::new()));
    let mut route_b = owners_b
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .map(|pair| PreparedRoute::Operation(pair, OperationCandidates::new()));
    let accepted_a = route_a.as_mut().ok().map(|route| {
        let proof = a.request("operation", 1, boot_a.clone());
        owners_a.accept(proof.0, proof.1, route, &process_a)
    });
    let accepted_b = route_b.as_mut().ok().map(|route| {
        let proof = b.request("operation", 1, boot_b.clone());
        owners_b.accept(proof.0, proof.1, route, &process_b)
    });
    let work_a = match &accepted_a {
        Some(Ok(AcceptedRoute::Operation(work))) => Some(work),
        _ => None,
    };
    let work_b = match &accepted_b {
        Some(Ok(AcceptedRoute::Operation(work))) => Some(work),
        _ => None,
    };
    let admitted = work_a.is_some() && work_b.is_some();
    let foreign_refused = work_b.is_some_and(|work| {
        work.start
            .check(&b.root, &b.pin())
            .and_then(|checked| owners_a.shared.state.lock().unwrap().start_locked(checked))
            .is_err()
    });
    // The foreign refusal must leave A's replay history usable for a new request.
    let mut control = owners_a
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let local_request = control.as_mut().ok().map(|route| {
        let proof = a.request("readiness", 2, boot_a);
        owners_a.accept(proof.0, proof.1, route, &process_a)
    });
    let local_accepted = matches!(&local_request, Some(Ok(AcceptedRoute::Readiness(_))));
    let local_started = work_a.is_some_and(|work| {
        work.start
            .check(&a.root, &a.pin())
            .and_then(|checked| owners_a.shared.state.lock().unwrap().start_locked(checked))
            .is_ok()
    });
    let other_started = work_b.is_some_and(|work| {
        work.start
            .check(&b.root, &b.pin())
            .and_then(|checked| owners_b.shared.state.lock().unwrap().start_locked(checked))
            .is_ok()
    });
    drop(local_request);
    drop(control);
    drop(accepted_a);
    drop(accepted_b);
    drop(route_a);
    drop(route_b);
    drop(process_a);
    drop(process_b);
    // Join both teardowns even if one reports a failed observation after cleanup.
    let (clean_a, clean_b) = tokio::join!(
        tokio::spawn(finish_distributed(owners_a, threads_a, path_a)),
        tokio::spawn(finish_distributed(owners_b, threads_b, path_b)),
    );
    assert!(clean_a.is_ok() && clean_b.is_ok());
    assert!(independent && admitted && foreign_refused && other_started);
    assert!(
        local_accepted && local_started,
        "foreign start damaged local acceptance/start: {local_accepted}/{local_started}"
    );
}

#[tokio::test]
async fn receiver_association_control_keys_resolve_only_the_original_owner() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    use crate::cluster::controller::OwnershipObservation;
    let (a, owners_a, threads_a, path_a, process_a) = signed_booted().await;
    let (b, owners_b, threads_b, path_b, process_b) = signed_booted().await;
    let boot_a = owners_a
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .boot();
    let boot_b = owners_b
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .boot();
    let observation_a = OwnershipObservation::new(&a.root).unwrap();
    let observation_b = OwnershipObservation::new(&b.root).unwrap();
    let transport = Arc::new(Semaphore::new(2));
    let mut route_a = owners_a
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let mut route_b = owners_b
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let accepted_a = route_a.as_mut().ok().map(|route| {
        let proof = a.request("readiness", 1, boot_a.clone());
        owners_a.accept(proof.0, proof.1, route, &process_a)
    });
    let accepted_b = route_b.as_mut().ok().map(|route| {
        let proof = b.request("readiness", 7, boot_b.clone());
        owners_b.accept(proof.0, proof.1, route, &process_b)
    });
    let keys = match (&accepted_a, &accepted_b) {
        (Some(Ok(AcceptedRoute::Readiness(a))), Some(Ok(AcceptedRoute::Readiness(b)))) => {
            Some((a.owner.key(), b.owner.key()))
        }
        _ => None,
    };
    let mut own = (false, false);
    let mut cross_refused = (false, false);
    let mut distinct = false;
    let mut colliding_ids = false;
    if let Some((key_a, key_b)) = &keys {
        colliding_ids = key_a.entry == key_b.entry;
        distinct = key_a != key_b;
        {
            let state = owners_a.shared.state.lock().unwrap();
            own.0 = state
                .payload
                .control_request_locked(key_a)
                .is_some_and(|r| {
                    r.binding.auth().sequence() == 1
                        && r.observation == observation_a
                        && r.local.same_boot(&boot_a)
                })
                && state.payload.control_retention_locked(key_a).is_some();
            cross_refused.0 = state.payload.control_request_locked(key_b).is_none()
                && state.payload.control_retention_locked(key_b).is_none();
        }
        {
            let state = owners_b.shared.state.lock().unwrap();
            own.1 = state
                .payload
                .control_request_locked(key_b)
                .is_some_and(|r| {
                    r.binding.auth().sequence() == 7
                        && r.observation == observation_b
                        && r.local.same_boot(&boot_b)
                })
                && state.payload.control_retention_locked(key_b).is_some();
            cross_refused.1 = state.payload.control_request_locked(key_a).is_none()
                && state.payload.control_retention_locked(key_a).is_none();
        }
    }
    drop(accepted_a);
    drop(accepted_b);
    drop(route_a);
    drop(route_b);
    drop(process_a);
    drop(process_b);
    let (clean_a, clean_b) = tokio::join!(
        tokio::spawn(finish_distributed(owners_a, threads_a, path_a)),
        tokio::spawn(finish_distributed(owners_b, threads_b, path_b)),
    );
    // Keep the keys alive through cleanup: they must not keep registered owners alive.
    drop(keys);
    assert!(clean_a.is_ok() && clean_b.is_ok());
    assert!(colliding_ids && own.0 && own.1);
    assert!(
        distinct && cross_refused.0 && cross_refused.1,
        "control keys crossed registries: distinct={distinct}, refused={cross_refused:?}"
    );
}

#[tokio::test]
async fn drain_classifier_signed_baseline_and_extra_delivery() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    f.root.revision += 1;
    f.root.change_id = id(40);
    f.root.nodes[0].state = NodeState::Draining;
    f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
        id: id(11),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    let transport = Arc::new(Semaphore::new(2));
    let mut route = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("drain", 1, boot);
    let mut accepted = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process));
    let mut outcomes = None;
    if let Some(Ok(AcceptedRoute::Drain(a))) = accepted.as_mut() {
        let incomplete = classify(&owners, &a.owner.key());
        let ready = tokio::time::timeout(WATCHDOG, a.owner.ready()).await;
        let key = a.owner.key();
        let classify = || {
            let state = owners.shared.state.lock().unwrap();
            state
                .payload
                .drain_work_locked(id(30), &key, state.local().unwrap())
        };
        let baseline = classify();
        let full = owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .retention_locked();
        let extra = a.owner.retain_response();
        let blocked = classify();
        drop(extra);
        let restored = classify();
        outcomes = Some((incomplete, ready, baseline, full, blocked, restored));
    }
    drop(accepted);
    drop(route);
    drop(process);
    finish_distributed(owners, threads, path).await;
    let (incomplete, ready, baseline, full, blocked, restored) =
        outcomes.expect("accepted signed Drain after cleanup");
    assert_eq!(incomplete, DrainWork::InvalidOwner);
    assert!(matches!(ready, Ok(Ok(()))));
    assert_eq!(baseline, DrainWork::ExemptOnly);
    assert_eq!(blocked, DrainWork::WorkPresent);
    assert_eq!(restored, DrainWork::ExemptOnly);
    assert_eq!((full.accepted, full.controls, full.deliveries), (2, 1, 1));
}

async fn classifier_observer(
    f: &mut ReceiverProofs,
    owners: &PayloadOwners,
    process: &PayloadLease,
) -> (Option<super::super::admission::AcceptedControl>, bool) {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    f.root.revision += 1;
    f.root.change_id = id(40);
    f.root.nodes[0].state = NodeState::Draining;
    f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
        id: id(11),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    let transport = Arc::new(Semaphore::new(1));
    let mut route = owners
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("drain", 99, boot);
    let accepted = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, process));
    let mut observer = match accepted {
        Some(Ok(AcceptedRoute::Drain(a))) => Some(a),
        _ => None,
    };
    let ready = if let Some(a) = observer.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    (observer, ready)
}
fn classify(owners: &PayloadOwners, key: &ControlKey) -> DrainWork {
    let state = owners.shared.state.lock().unwrap();
    state
        .payload
        .drain_work_locked(id(30), key, state.local().unwrap())
}

#[tokio::test]
async fn drain_classifier_foreign_collisions_and_wrong_identity() {
    let (mut fa, a, ta, pa, process_a) = signed_booted().await;
    let (mut fb, b, tb, pb, process_b) = signed_booted().await;
    let (oa, ready_a) = classifier_observer(&mut fa, &a, &process_a).await;
    let (ob, ready_b) = classifier_observer(&mut fb, &b, &process_b).await;
    let outcome = oa.as_ref().zip(ob.as_ref()).map(|(oa, ob)| {
        let ka = oa.owner.key();
        let kb = ob.owner.key();
        let wrong = {
            let state = a.shared.state.lock().unwrap();
            state
                .payload
                .drain_work_locked(id(31), &ka, state.local().unwrap())
        };
        (
            ka.entry == kb.entry,
            classify(&a, &ka),
            classify(&b, &kb),
            classify(&a, &kb),
            classify(&b, &ka),
            wrong,
        )
    });
    drop(oa);
    drop(ob);
    drop(process_a);
    drop(process_b);
    let (done_a, done_b) = tokio::join!(
        tokio::spawn(finish_distributed(a, ta, pa)),
        tokio::spawn(finish_distributed(b, tb, pb)),
    );
    assert!(done_a.is_ok() && done_b.is_ok() && ready_a && ready_b);
    assert_eq!(
        outcome,
        Some((
            true,
            DrainWork::ExemptOnly,
            DrainWork::ExemptOnly,
            DrainWork::InvalidOwner,
            DrainWork::InvalidOwner,
            DrainWork::InvalidOwner
        ))
    );
}

#[tokio::test]
async fn drain_classifier_dormant_preparation_and_unrelated_control() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(2));
    let mut route = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("readiness", 1, boot);
    let mut unrelated = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process));
    let control_ready = if let Some(Ok(AcceptedRoute::Readiness(a))) = unrelated.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let retained = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let wrong_route = unrelated.as_ref().and_then(|r| match r {
        Ok(AcceptedRoute::Readiness(a)) => Some(classify(&owners, &a.owner.key())),
        _ => None,
    });
    drop(unrelated);
    drop(route);
    let reaped = tokio::time::timeout(WATCHDOG, async {
        loop {
            owners.release_pending();
            if owners.shared.state.lock().unwrap().payload.controls.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let baseline = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let preparation = owners.prepare_control(transport.try_acquire_owned().unwrap());
    let dormant = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    drop(preparation);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && control_ready && reaped.is_ok());
    assert_eq!(retained, Some(DrainWork::WorkPresent));
    assert_eq!(wrong_route, Some(DrainWork::InvalidOwner));
    assert_eq!(baseline, Some(DrainWork::ExemptOnly));
    assert_eq!(dormant, Some(DrainWork::WorkPresent));
}

#[tokio::test]
async fn drain_classifier_account_publication_discard_return_and_release() {
    use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let pair = owners
        .prepare_pair(
            Role::Operation,
            transport.try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let slot = pair.account.slot.clone();
    let (discarded, at_discard) = oneshot::channel();
    let (release_discard, discard_gate) = std::sync::mpsc::channel();
    let (returning, at_return) = oneshot::channel();
    let (release_return, return_gate) = oneshot::channel();
    {
        let mut h = slot.hooks.lock().unwrap();
        h.install_discard = Some((discarded, discard_gate));
        h.returning = Some((returning, return_gate));
    }
    let mut route = PreparedRoute::Operation(pair, OperationCandidates::new());
    let proof = f.request("operation", 1, boot);
    let accepted = owners.accept(proof.0, proof.1, &mut route, &process);
    let mut account = None;
    let mut work_ready = false;
    if let Ok(AcceptedRoute::Operation(mut a)) = accepted {
        work_ready = matches!(
            tokio::time::timeout(WATCHDOG, a.owners.stream.ready()).await,
            Ok(Ok(_))
        );
        account = a.owners.account.take();
        if let Some(a) = account.as_mut() {
            work_ready &= matches!(tokio::time::timeout(WATCHDOG, a.ready()).await, Ok(Ok(_)));
        }
    }
    drop(route);
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let observe = || observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let active = observe();
    let publication = account.as_ref().map(PayloadLease::retain_delivery);
    let publication_ok = matches!(publication, Some(Ok(_)));
    drop(account);
    let held = observe();
    drop(publication);
    let discarded = tokio::time::timeout(WATCHDOG, async {
        tokio::pin!(at_discard);
        loop {
            owners.release_pending();
            tokio::select! { r = &mut at_discard => break r, _ = tokio::task::yield_now() => {} }
        }
    })
    .await;
    let during_discard = observe();
    let _ = release_discard.send(());
    let returned = tokio::time::timeout(WATCHDOG, at_return).await;
    let before_return = observe();
    let cleanup = slot.cleanup.load(Ordering::Acquire);
    owners.release_pending();
    let still_retained = observe();
    let _ = release_return.send(());
    let finished = tokio::time::timeout(WATCHDOG, async {
        loop {
            let finished = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .entries
                .iter()
                .find(|e| e.slot.id == slot.id)
                .is_some_and(|e| e.handle.is_finished());
            if finished {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let unreaped = observe();
    let (at_release, releasing) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *owners.shared.releasing.lock().unwrap() = Some((at_release, gate));
    let shared = owners.shared.clone();
    let reaper = std::thread::spawn(move || reap_shared(&shared, None));
    let releasing = tokio::time::timeout(WATCHDOG, releasing).await;
    let pending_release = observe();
    let _ = release.send(());
    let joined_reaper = reaper.join();
    let after = observe();
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(
        work_ready
            && ready
            && publication_ok
            && cleanup
            && finished.is_ok()
            && joined_reaper.is_ok()
    );
    assert!(
        matches!(discarded, Ok(Ok(())))
            && matches!(returned, Ok(Ok(())))
            && matches!(releasing, Ok(Ok(())))
    );
    for result in [
        active,
        held,
        during_discard,
        before_return,
        still_retained,
        unreaped,
        pending_release,
    ] {
        assert_eq!(result, Some(DrainWork::WorkPresent));
    }
    assert_eq!(after, Some(DrainWork::ExemptOnly));
}

#[tokio::test]
async fn drain_classifier_dependency_without_payload_account() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let mut route = owners
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("dependency_open", 1, boot);
    let mut opened = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process));
    let open_ready = if let Some(Ok(AcceptedRoute::Open(a))) = opened.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    drop(opened);
    drop(route);
    let reaped = tokio::time::timeout(WATCHDOG, async {
        loop {
            owners.release_pending();
            if owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .controls
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let evidence = observer.as_ref().map(|a| {
        let state = owners.shared.state.lock().unwrap();
        let local = state.local().unwrap().snapshot();
        (
            state
                .payload
                .drain_work_locked(id(30), &a.owner.key(), state.local().unwrap()),
            local.occupancy(),
            local.dependencies(),
            state.payload.entries.len(),
        )
    });
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && open_ready && reaped.is_ok());
    assert_eq!(
        evidence,
        Some((DrainWork::WorkPresent, (0, 0, 0, 0), (1, 0), 1))
    );
}

#[tokio::test]
async fn drain_classifier_cancelled_and_cleanup_unreaped_exemptions_refused() {
    for cancel_process in [false, true] {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let key = observer.as_ref().map(|a| a.owner.key());
        let baseline = key.as_ref().map(|key| classify(&owners, key));
        let slot = if cancel_process {
            Some(process.slot.clone())
        } else {
            observer.as_ref().map(|a| a.owner.0.slot.clone())
        };
        let (at_return, returned) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        if let Some(slot) = &slot {
            slot.hooks.lock().unwrap().returning = Some((at_return, gate));
            slot.cancel();
        }
        let cancelled = key.as_ref().map(|key| classify(&owners, key));
        drop(observer);
        let returned = tokio::time::timeout(WATCHDOG, returned).await;
        let cleanup = slot
            .as_ref()
            .is_some_and(|s| s.cleanup.load(Ordering::Acquire));
        let unreaped = key.as_ref().map(|key| classify(&owners, key));
        let _ = release.send(());
        let finished = tokio::time::timeout(WATCHDOG, async {
            loop {
                let done = {
                    let state = owners.shared.state.lock().unwrap();
                    slot.as_ref().is_some_and(|slot| {
                        state
                            .payload
                            .entries
                            .iter()
                            .chain(&state.payload.controls)
                            .any(|e| Arc::ptr_eq(&e.slot, slot) && e.handle.is_finished())
                    })
                };
                if done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let terminated_unreaped = key.as_ref().map(|key| classify(&owners, key));
        drop(process);
        finish_distributed(owners, threads, path).await;
        assert!(ready && cleanup && finished.is_ok() && matches!(returned, Ok(Ok(()))));
        assert_eq!(baseline, Some(DrainWork::ExemptOnly));
        assert_eq!(cancelled, Some(DrainWork::InvalidOwner));
        assert_eq!(unreaped, Some(DrainWork::InvalidOwner));
        assert_eq!(terminated_unreaped, Some(DrainWork::InvalidOwner));
    }
}

#[tokio::test]
async fn drain_classifier_extra_hold_cannot_replace_original_response() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let mut outcome = None;
    if let Some(a) = observer {
        let key = a.owner.key();
        let baseline = classify(&owners, &key);
        let extra = a.owner.retain_response();
        let acquired = extra.is_ok();
        drop(a.response);
        let substituted = classify(&owners, &key);
        drop(extra);
        let released = classify(&owners, &key);
        outcome = Some((baseline, acquired, substituted, released));
        drop(a.owner);
    }
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(
        outcome,
        Some((
            DrainWork::ExemptOnly,
            true,
            DrainWork::WorkPresent,
            DrainWork::ExemptOnly
        ))
    );
}

#[tokio::test]
async fn drain_classifier_failed_original_process_stays_failed() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let baseline = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let transport = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.controls.first().and_then(|entry| {
            if let Admission::Distributed {
                transport: Some(permit),
                ..
            } = &*entry.guards
            {
                Some(Arc::downgrade(permit))
            } else {
                None
            }
        })
    };
    let (observer_returning, observer_at_return) = oneshot::channel();
    let (observer_release, observer_gate) = oneshot::channel();
    if let Some(a) = &observer {
        a.owner.0.slot.hooks.lock().unwrap().returning = Some((observer_returning, observer_gate));
    }
    let (returning, at_return) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    process.slot.hooks.lock().unwrap().returning = Some((returning, gate));
    process.slot.cancel();
    let returned = tokio::time::timeout(WATCHDOG, at_return).await;
    {
        let state = owners.shared.state.lock().unwrap();
        if let Some(e) = state
            .payload
            .entries
            .iter()
            .find(|e| e.slot.id == process.slot.id)
        {
            e.handle.abort();
        }
    }
    let _ = release.send(());
    let finished = tokio::time::timeout(WATCHDOG, async {
        loop {
            let done = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .entries
                .iter()
                .any(|e| e.slot.id == process.slot.id && e.handle.is_finished());
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    owners.release_pending();
    let failed = process.slot.failed.load(Ordering::Acquire);
    let refusal = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    drop(observer);
    drop(process);
    let observer_returned = tokio::time::timeout(WATCHDOG, observer_at_return).await;
    let drained = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let retained_failed = owners.shared.state.lock().unwrap().payload.failed;
    let retained_observer = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.controls.len() == 1
            && state.payload.controls[0]
                .slot
                .cleanup
                .load(Ordering::Acquire)
            && !state.payload.controls[0].handle.is_finished()
            && transport.as_ref().is_some_and(|t| t.strong_count() != 0)
    };
    let _ = observer_release.send(());
    // Global failure makes close_and_drain return early. Continue polling the
    // registered survivor until its original handle and guards are reaped.
    let observer_reaped = tokio::time::timeout(WATCHDOG, async {
        let waker = Waker::from(owners.shared.reap_wake.clone());
        loop {
            let notified = owners.shared.reap_wake.0.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            reap_shared(&owners.shared, Some(&mut Context::from_waker(&waker)));
            let settled = {
                let state = owners.shared.state.lock().unwrap();
                state.payload.controls.is_empty() && state.payload.pending_releases == 0
            };
            if settled {
                break;
            }
            notified.await;
        }
    })
    .await;
    let observer_settled = {
        let state = owners.shared.state.lock().unwrap();
        (
            state.payload.controls.len(),
            state
                .payload
                .records
                .iter()
                .filter(|r| matches!(r, Some(AcceptedOwner::Control(_))))
                .count(),
            state.payload.pending_releases,
            transport.as_ref().map(|t| t.strong_count()),
        )
    };
    // Test-only recovery still runs if an observation/watchdog failed. These
    // memory-only controls retain their original handles until they are joined.
    let remaining = std::mem::take(&mut owners.shared.state.lock().unwrap().payload.controls);
    for mut entry in remaining {
        entry.slot.cancel();
        if !entry.terminated
            && tokio::time::timeout(WATCHDOG, &mut entry.handle)
                .await
                .is_err()
        {
            entry.handle.abort();
            let _ = (&mut entry.handle).await;
        }
        if let Some(record) = entry.record {
            owners.shared.state.lock().unwrap().payload.records[record] = None;
        }
        drop(ManuallyDrop::into_inner(entry.guards));
    }
    let quarantine = tests::quarantine_teardown(&owners);
    tokio::pin!(quarantine);
    let quarantined = tokio::time::timeout(WATCHDOG, &mut quarantine).await;
    if quarantined.is_err() {
        quarantine.await;
    }
    let shutdown = threads.shutdown();
    tokio::pin!(shutdown);
    let joined = tokio::time::timeout(WATCHDOG, &mut shutdown).await;
    if joined.is_err() {
        let _ = shutdown.await;
    }
    let empty = std::fs::read_dir(&path).map(|mut r| r.next().is_none());
    let removed = std::fs::remove_dir(path);
    assert!(
        ready
            && failed
            && retained_failed
            && finished.is_ok()
            && quarantined.is_ok()
            && matches!(returned, Ok(Ok(())))
            && matches!(observer_returned, Ok(Ok(())))
            && retained_observer
            && observer_reaped.is_ok()
    );
    assert!(matches!(drained, Ok(Err(OwnerError::FailedDrain))) && matches!(joined, Ok(Ok(()))));
    assert!(matches!(empty, Ok(true)) && removed.is_ok());
    assert_eq!(observer_settled, (0, 0, 0, Some(0)));
    assert_eq!(baseline, Some(DrainWork::ExemptOnly));
    assert_eq!(refusal, Some(DrainWork::InvalidOwner));
}

#[tokio::test]
async fn drain_classifier_failed_observer_is_retained_without_exemption() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let key = observer.as_ref().map(|a| a.owner.key());
    let baseline = key.as_ref().map(|key| classify(&owners, key));
    if let Some(key) = &key {
        let state = owners.shared.state.lock().unwrap();
        if let Some(e) = state.payload.controls.iter().find(|e| key.matches(&e.slot)) {
            e.handle.abort();
        }
    }
    let finished = tokio::time::timeout(WATCHDOG, async {
        loop {
            let done = {
                let state = owners.shared.state.lock().unwrap();
                key.as_ref().is_some_and(|key| {
                    state
                        .payload
                        .controls
                        .iter()
                        .any(|e| key.matches(&e.slot) && e.handle.is_finished())
                })
            };
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    owners.release_pending();
    let refusal = key.as_ref().map(|key| classify(&owners, key));
    let failed_retained = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.failed
            && state
                .payload
                .controls
                .iter()
                .any(|e| e.terminated && e.slot.failed.load(Ordering::Acquire))
    };
    drop(observer);
    drop(process);
    let drained = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    // Intentional abort quarantine: the original memory-only handle was polled
    // above. Move its guards out only for test recovery, after recording failure.
    let controls = std::mem::take(&mut owners.shared.state.lock().unwrap().payload.controls);
    let recovered = std::thread::spawn(move || {
        for e in controls {
            drop(ManuallyDrop::into_inner(e.guards));
        }
    })
    .join();
    let quarantine = tests::quarantine_teardown(&owners);
    tokio::pin!(quarantine);
    let quarantined = tokio::time::timeout(WATCHDOG, &mut quarantine).await;
    if quarantined.is_err() {
        quarantine.await;
    }
    let shutdown = threads.shutdown();
    tokio::pin!(shutdown);
    let joined = tokio::time::timeout(WATCHDOG, &mut shutdown).await;
    if joined.is_err() {
        let _ = shutdown.await;
    }
    let empty = std::fs::read_dir(&path).map(|mut r| r.next().is_none());
    let removed = std::fs::remove_dir(path);
    assert!(
        ready && failed_retained && finished.is_ok() && recovered.is_ok() && quarantined.is_ok()
    );
    assert!(matches!(drained, Ok(Err(OwnerError::FailedDrain))) && matches!(joined, Ok(Ok(()))));
    assert!(matches!(empty, Ok(true)) && removed.is_ok());
    assert_eq!(baseline, Some(DrainWork::ExemptOnly));
    assert_eq!(refusal, Some(DrainWork::InvalidOwner));
}

#[tokio::test]
async fn drain_classifier_unchecked_memory_control_cannot_be_observer() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let transport = Arc::new(Semaphore::new(1));
    let mut prepared = owners
        .prepare_control(transport.try_acquire_owned().unwrap())
        .unwrap();
    let mut raw = {
        let mut state = owners.shared.state.lock().unwrap();
        state
            .payload
            .reserve_control_locked(&mut prepared)
            .map(|r| r.install_control_locked())
    };
    let raw_ready = if let Ok(raw) = raw.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, raw.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    let invalid = raw.as_ref().ok().map(|raw| classify(&owners, &raw.key()));
    let blocked = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    drop(raw);
    drop(prepared);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && raw_ready);
    assert_eq!(invalid, Some(DrainWork::InvalidOwner));
    assert_eq!(blocked, Some(DrainWork::WorkPresent));
}

#[tokio::test]
async fn barrier_sample_signed_delivery_history() {
    use super::super::admission::BarrierPreparation;
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let mut outcome = None;
    if let Some(a) = &observer {
        let prepared = BarrierPreparation::prepare(&f.root, boot);
        if let Ok(prepared) = prepared {
            let sampled =
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), a.owner.key());
            if let Ok(sample) = sampled {
                let before = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                let hold = a.owner.retain_response();
                let busy = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                drop(hold);
                let after = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                outcome = Some((
                    before,
                    busy,
                    after,
                    sample.heartbeat().accepting,
                    sample.heartbeat().drain_id,
                ));
            }
        }
    }
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(outcome, Some((true, false, false, false, Some(id(11)))));
}

#[tokio::test]
async fn barrier_sample_worker_cleanup_and_release_handoff() {
    use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let pair = owners
        .prepare_pair(
            Role::Operation,
            transport.try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let slot = pair.account.slot.clone();
    let (discarded, at_discard) = oneshot::channel();
    let (release_discard, discard_gate) = std::sync::mpsc::channel();
    let (returning, at_return) = oneshot::channel();
    let (release_return, return_gate) = oneshot::channel();
    {
        let mut h = slot.hooks.lock().unwrap();
        h.install_discard = Some((discarded, discard_gate));
        h.returning = Some((returning, return_gate));
    }
    let mut route = PreparedRoute::Operation(pair, OperationCandidates::new());
    let proof = f.request("operation", 1, boot);
    let accepted = owners.accept(proof.0, proof.1, &mut route, &process);
    let mut account = None;
    let mut work_ready = false;
    if let Ok(AcceptedRoute::Operation(mut a)) = accepted {
        work_ready = matches!(
            tokio::time::timeout(WATCHDOG, a.owners.stream.ready()).await,
            Ok(Ok(_))
        );
        account = a.owners.account.take();
        if let Some(a) = account.as_mut() {
            work_ready &= matches!(tokio::time::timeout(WATCHDOG, a.ready()).await, Ok(Ok(_)));
        }
    }
    drop(route);
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let observe = || observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let sample_now = || {
        observer.as_ref().and_then(|a| {
            let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
            let prepared =
                super::super::admission::BarrierPreparation::prepare(&f.root, boot).ok()?;
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key())
                .ok()
        })
    };
    let old_sample = sample_now();
    let active = observe();
    let publication = account.as_ref().map(PayloadLease::retain_delivery);
    let publication_ok = matches!(publication, Some(Ok(_)));
    drop(account);
    let held = observe();
    drop(publication);
    let discarded = tokio::time::timeout(WATCHDOG, async {
        tokio::pin!(at_discard);
        loop {
            owners.release_pending();
            tokio::select! { r = &mut at_discard => break r, _ = tokio::task::yield_now() => {} }
        }
    })
    .await;
    let during_discard = observe();
    let discard_sample_refused = sample_now().is_none();
    let _ = release_discard.send(());
    let returned = tokio::time::timeout(WATCHDOG, at_return).await;
    let before_return = observe();
    let cleanup = slot.cleanup.load(Ordering::Acquire);
    owners.release_pending();
    let still_retained = observe();
    let _ = release_return.send(());
    let finished = tokio::time::timeout(WATCHDOG, async {
        loop {
            let finished = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .entries
                .iter()
                .find(|e| e.slot.id == slot.id)
                .is_some_and(|e| e.handle.is_finished());
            if finished {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let unreaped = observe();
    let (at_release, releasing) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *owners.shared.releasing.lock().unwrap() = Some((at_release, gate));
    let shared = owners.shared.clone();
    let reaper = std::thread::spawn(move || reap_shared(&shared, None));
    let releasing = tokio::time::timeout(WATCHDOG, releasing).await;
    let pending_release = observe();
    let release_sample_refused = sample_now().is_none();
    let _ = release.send(());
    let joined_reaper = reaper.join();
    let after = observe();
    let settled_sample = sample_now();
    let old_invalid = old_sample
        .as_ref()
        .is_some_and(|s| !owners.shared.state.lock().unwrap().barrier_matches(s));
    let settled_valid = settled_sample
        .as_ref()
        .is_some_and(|s| owners.shared.state.lock().unwrap().barrier_matches(s));
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(
        work_ready
            && ready
            && publication_ok
            && cleanup
            && finished.is_ok()
            && joined_reaper.is_ok()
    );
    assert!(
        matches!(discarded, Ok(Ok(())))
            && matches!(returned, Ok(Ok(())))
            && matches!(releasing, Ok(Ok(())))
    );
    for result in [
        active,
        held,
        during_discard,
        before_return,
        still_retained,
        unreaped,
        pending_release,
    ] {
        assert_eq!(result, Some(DrainWork::WorkPresent));
    }
    assert_eq!(after, Some(DrainWork::ExemptOnly));
    assert!(discard_sample_refused && release_sample_refused && old_invalid && settled_valid);
}

#[tokio::test]
async fn barrier_sample_unchanged_root_and_fully_reaped_preparation() {
    use super::super::admission::BarrierPreparation;
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let mut outcome = None;
    if let Some(a) = &observer {
        let prepared = BarrierPreparation::prepare(&f.root, boot.clone());
        if let Ok(prepared) = prepared {
            let sample =
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), a.owner.key());
            if let Ok(sample) = sample {
                owners.release_pending();
                owners.release_pending();
                let unchanged = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                let seq = sample.request_binding().auth().sequence();
                let cluster = sample.binding().cluster_id;
                let mut conflicting_root = f.root.clone();
                conflicting_root.change_id = id(9999);
                let conflicting = BarrierPreparation::prepare(&conflicting_root, boot.clone())
                    .ok()
                    .map(|p| {
                        owners
                            .shared
                            .state
                            .lock()
                            .unwrap()
                            .sample_barrier(p, id(30), a.owner.key())
                            .is_err()
                    });
                let preserved = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                let transport = Arc::new(Semaphore::new(1));
                let preparation = owners.prepare_control(transport.try_acquire_owned().unwrap());
                let pending_id = preparation.as_ref().ok().map(|p| p.slot.id);
                drop(preparation);
                let settled = if let Some(id) = pending_id {
                    matches!(
                        tokio::time::timeout(WATCHDOG, wait_reap(&owners.shared, Some(id))).await,
                        Ok(Ok(()))
                    )
                } else {
                    false
                };
                let after = owners.shared.state.lock().unwrap().barrier_matches(&sample);
                let full = owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .retention_locked();
                outcome = Some((
                    unchanged,
                    preserved,
                    conflicting,
                    settled,
                    after,
                    seq,
                    cluster,
                    full.accepted,
                    full.preparations,
                ));
            }
        }
    }
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(
        outcome,
        Some((
            true,
            true,
            Some(true),
            true,
            false,
            99,
            f.root.cluster_id,
            2,
            0
        ))
    );
}

#[tokio::test]
async fn barrier_sample_foreign_key_and_initialization_refuse() {
    use super::super::admission::BarrierPreparation;
    let (mut fa, a, ta, pa, process_a) = signed_booted().await;
    let (mut fb, b, tb, pb, process_b) = signed_booted().await;
    let (oa, ra) = classifier_observer(&mut fa, &a, &process_a).await;
    let (ob, rb) = classifier_observer(&mut fb, &b, &process_b).await;
    let boot = a.shared.state.lock().unwrap().local().unwrap().boot();
    let make = || BarrierPreparation::prepare(&fa.root, boot.clone());
    let mut outcome = None;
    if let (Some(oa), Some(ob)) = (&oa, &ob) {
        let valid = make().ok().map(|p| {
            a.shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(p, id(30), oa.owner.key())
                .is_ok()
        });
        let foreign = make().ok().map(|p| {
            a.shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(p, id(30), ob.owner.key())
                .is_err()
        });
        let init = make().ok().map(|p| {
            a.shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(p, id(31), oa.owner.key())
                .is_err()
        });
        let foreign_boot = b.shared.state.lock().unwrap().local().unwrap().boot();
        let foreign_receiver = BarrierPreparation::prepare(&fb.root, foreign_boot)
            .ok()
            .map(|p| {
                a.shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), oa.owner.key())
                    .is_err()
            });
        let mut wrong_root = fa.root.clone();
        wrong_root.nodes[0].drain.as_mut().unwrap().id = id(111);
        let wrong_drain = BarrierPreparation::prepare(&wrong_root, boot.clone())
            .ok()
            .map(|p| {
                a.shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), oa.owner.key())
                    .is_err()
            });
        oa.owner.0.slot.cancel();
        let cancelled = make().ok().map(|p| {
            a.shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(p, id(30), oa.owner.key())
                .is_err()
        });
        outcome = Some((
            valid,
            foreign,
            init,
            cancelled,
            foreign_receiver,
            wrong_drain,
        ));
    }
    drop(oa);
    drop(ob);
    drop(process_a);
    drop(process_b);
    let (cleanup_a, cleanup_b) = tokio::join!(
        tokio::spawn(finish_distributed(a, ta, pa)),
        tokio::spawn(finish_distributed(b, tb, pb))
    );
    assert!(cleanup_a.is_ok() && cleanup_b.is_ok());
    assert!(ra && rb);
    assert_eq!(
        outcome,
        Some((
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true)
        ))
    );
}

#[tokio::test]
async fn barrier_sample_exhaustion_precedes_real_replay_consumption() {
    use super::super::admission::{BarrierPreparation, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let mut route = owners
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    owners.shared.state.lock().unwrap().fixture_exhaust_work();
    let proof = f.request("drain", 100, boot.clone());
    let refused = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process).is_err());
    let proof = f.request("drain", 100, boot.clone());
    let unspent = owners
        .shared
        .state
        .lock()
        .unwrap()
        .fixture_consume_unspent(proof.0, proof.1);
    let sample_refused = observer.as_ref().and_then(|a| {
        BarrierPreparation::prepare(&f.root, boot).ok().map(|p| {
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(p, id(30), a.owner.key())
                .is_err()
        })
    });
    let extra_refused = observer
        .as_ref()
        .map(|a| a.owner.retain_response().is_err());
    drop(route);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && unspent);
    assert_eq!(
        (refused, sample_refused, extra_refused),
        (Some(true), Some(true), Some(true))
    );
}

#[tokio::test]
async fn work_evidence_regression_pair_reservation_invalidates_before_publication() {
    use super::super::admission::BarrierPreparation;
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let sample = observer.as_ref().and_then(|a| {
        let prepared = BarrierPreparation::prepare(&f.root, boot).ok()?;
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .sample_barrier(prepared, id(30), a.owner.key())
            .ok()
    });
    let before = sample
        .as_ref()
        .is_some_and(|s| owners.shared.state.lock().unwrap().barrier_matches(s));
    let (reserved, at_reservation) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *owners.shared.pair_preparing.lock().unwrap() = Some((reserved, gate));
    let runtime = tokio::runtime::Handle::current();
    let shared = owners.shared.clone();
    let transport = Arc::new(Semaphore::new(1));
    let permit = transport.clone().try_acquire_owned().unwrap();
    let preparing = std::thread::spawn(move || {
        let _entered = runtime.enter();
        PayloadOwners { shared }.prepare_pair(Role::Operation, permit, None)
    });
    let reached = tokio::time::timeout(WATCHDOG, at_reservation).await;
    let during = sample.as_ref().map(|sample| {
        let state = owners.shared.state.lock().unwrap();
        (
            state.payload.preparations.iter().flatten().count(),
            state
                .payload
                .preparations
                .iter()
                .flatten()
                .filter(|p| p.entry.is_some())
                .count(),
            state.barrier_matches(sample),
        )
    });
    let _ = release.send(());
    let prepared = preparing.join();
    let preparation_ok = matches!(&prepared, Ok(Ok(_)));
    drop(prepared);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && before && preparation_ok && matches!(reached, Ok(Ok(()))));
    assert_eq!(transport.available_permits(), 1);
    assert_eq!(during, Some((2, 0, false)));
}

#[tokio::test]
async fn work_evidence_regression_exhausted_pair_preparation_is_unchanged() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let next = owners.shared.state.lock().unwrap().payload.next;
    owners.shared.state.lock().unwrap().fixture_exhaust_work();
    let transport = Arc::new(Semaphore::new(1));
    let prepared = owners.prepare_pair(
        Role::Operation,
        transport.clone().try_acquire_owned().unwrap(),
        None,
    );
    let refused = matches!(&prepared, Err(OwnerError::Refused));
    let after = {
        let state = owners.shared.state.lock().unwrap();
        (
            state.payload.next == next,
            state.payload.preparations.iter().flatten().count(),
            transport.available_permits(),
        )
    };
    drop(prepared);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert!(
        refused,
        "exhausted evidence must refuse before pair reservation"
    );
    assert_eq!(after, (true, 0, 1));
    assert_eq!(transport.available_permits(), 1);
}

#[tokio::test]
async fn work_evidence_regression_response_boundary_preserves_replay_and_cleanup() {
    use super::super::admission::PreparedRoute;
    let mut outcomes = Vec::new();
    for remaining in [1, 2, 3] {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let transport = Arc::new(Semaphore::new(1));
        let prepared = owners.prepare_control(transport.clone().try_acquire_owned().unwrap());
        if let Ok(prepared) = &prepared {
            prepared.slot.hooks.lock().unwrap().response_generation = Some(u64::MAX - remaining);
        }
        let mut route = prepared.map(PreparedRoute::Control);
        let proof = f.request("drain", 100, boot.clone());
        let accepted = route
            .as_mut()
            .ok()
            .map(|route| owners.accept(proof.0, proof.1, route, &process));
        let refused = accepted.as_ref().map(|r| r.is_err());
        let reached = match &route {
            Ok(PreparedRoute::Control(prepared)) => prepared
                .slot
                .hooks
                .lock()
                .unwrap()
                .response_generation
                .is_none(),
            _ => false,
        };
        let proof = f.request("drain", 100, boot);
        let unspent = owners
            .shared
            .state
            .lock()
            .unwrap()
            .fixture_consume_unspent(proof.0, proof.1);
        drop(accepted);
        drop(route);
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        outcomes.push((
            remaining,
            ready,
            reached,
            refused,
            unspent,
            transport.available_permits(),
        ));
    }
    assert_eq!(
        outcomes,
        vec![
            (1, true, true, Some(true), true, 1),
            (2, true, true, Some(true), true, 1),
            (3, true, true, Some(false), false, 1),
        ]
    );
}

#[tokio::test]
async fn proof_cycle_registered_ack_reaps_once() {
    use super::super::{admission::BarrierPreparation, drain_observation::ProofError};
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("proof").unwrap(),
    ));
    let mut outcome = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot.clone()).unwrap();
        let sample =
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key());
        if let Ok(sample) = sample {
            let charges = (
                owners.shared.operations.available_permits(),
                owners.shared.operation_transport.available_permits(),
                owners.shared.dependency_transport.available_permits(),
            );
            let published =
                owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG);
            if let Ok(ticket) = published {
                let unchanged = charges
                    == (
                        owners.shared.operations.available_permits(),
                        owners.shared.operation_transport.available_permits(),
                        owners.shared.dependency_transport.available_permits(),
                    );
                let before = owners.take_proof(&ticket, &a.owner.key()).is_none();
                let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
                let second = owners.shared.state.lock().unwrap().sample_barrier(
                    prepared,
                    id(30),
                    a.owner.key(),
                );
                let busy = second.is_ok_and(|sample| {
                    matches!(
                        owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG),
                        Err(ProofError::Busy)
                    )
                });
                let terminated = tokio::time::timeout(WATCHDOG, async {
                    while !owners.shared.state.lock().unwrap().payload.proof.finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_ok();
                let unreaped = owners.take_proof(&ticket, &a.owner.key()).is_none();
                let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
                let completion = owners.take_proof(&ticket, &a.owner.key());
                let exact = completion.as_ref().is_some_and(|c| {
                    c.acknowledgement().is_some_and(|ack| ack.sequence() == 1)
                        && owners
                            .shared
                            .state
                            .lock()
                            .unwrap()
                            .barrier_matches(c.sample())
                });
                let twice = owners.take_proof(&ticket, &a.owner.key()).is_none();
                outcome = Some((
                    before && unchanged && terminated && unreaped,
                    busy,
                    matches!(waited, Ok(Ok(()))),
                    exact,
                    twice,
                ));
                drop(completion);
                drop(ticket);
            }
        }
    }
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(outcome, Some((true, true, true, true, true)));
    assert_eq!(storage.backend().state.lock().unwrap().puts, 1);
}

#[tokio::test]
async fn proof_cycle_gates_cancelled_waiter_and_retained_ack() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::{Fake, ReadGate},
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let owners = Arc::new(owners);
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let io = Arc::new(ReadGate::default());
    let ack = Arc::new(ReadGate::default());
    let returning = Arc::new(ReadGate::default());
    {
        let mut s = owners.shared.state.lock().unwrap();
        s.payload.proof.gates.before_io = Some(io.clone());
        s.payload.proof.gates.buffered = Some(ack.clone());
        s.payload.proof.gates.returning = Some(returning.clone());
    }
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("gated").unwrap(),
    ));
    let mut outcome = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
        let sample =
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key());
        if let Ok(sample) = sample
            && let Ok(ticket) =
                owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
        {
            let ticket = Arc::new(ticket);
            let entered = tokio::time::timeout(WATCHDOG, io.entered.acquire())
                .await
                .is_ok();
            let snapshot = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .retention_locked();
            let absent_io = storage.backend().state.lock().unwrap().puts == 0;
            let waiter_a = {
                let o = owners.clone();
                let t = ticket.clone();
                tokio::spawn(async move { o.wait_proof(&t).await })
            };
            let mut waiter_b = {
                let o = owners.clone();
                let t = ticket.clone();
                tokio::spawn(async move { o.wait_proof(&t).await })
            };
            waiter_a.abort();
            let cancelled = waiter_a.await.is_err();
            io.release.add_permits(1);
            let buffered = tokio::time::timeout(WATCHDOG, ack.entered.acquire())
                .await
                .is_ok();
            let retained = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .retention_locked();
            let early = owners.take_proof(&ticket, &a.owner.key()).is_none();
            ack.release.add_permits(1);
            let cleanup = tokio::time::timeout(WATCHDOG, returning.entered.acquire())
                .await
                .is_ok();
            let still_early = owners.take_proof(&ticket, &a.owner.key()).is_none();
            returning.release.add_permits(1);
            let survivor = tokio::time::timeout(WATCHDOG, &mut waiter_b).await;
            if survivor.is_err() {
                waiter_b.abort();
                let _ = waiter_b.await;
            }
            let completion = owners.take_proof(&ticket, &a.owner.key());
            let good = completion.as_ref().is_some_and(|c| {
                c.acknowledgement().is_some()
                    && owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .barrier_matches(c.sample())
            });
            outcome = Some(
                entered
                    && absent_io
                    && snapshot.proof_cycles == 1
                    && snapshot.supervisors == 3
                    && cancelled
                    && buffered
                    && retained.proof_results == 1
                    && early
                    && cleanup
                    && still_early
                    && matches!(survivor, Ok(Ok(Ok(()))))
                    && good,
            );
            drop(completion);
            drop(ticket);
        }
    }
    io.release.add_permits(1);
    ack.release.add_permits(1);
    returning.release.add_permits(1);
    drop(observer);
    drop(process);
    let owners = Arc::try_unwrap(owners).ok().unwrap();
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(outcome, Some(true));
}

#[tokio::test]
async fn proof_cycle_no_ack_and_panic_remain_owned() {
    use super::super::{admission::BarrierPreparation, drain_observation::ProofError};
    use crate::{
        storage::{Namespace, Storage, StorageError},
        test_storage::Fake,
    };
    let mut outcomes = Vec::new();
    for mode in 0..5 {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let storage = Arc::new(Storage::new(
            Fake::default(),
            Namespace::new("failure").unwrap(),
        ));
        {
            let mut s = storage.backend().state.lock().unwrap();
            if mode == 4 {
                s.race = Some(vec![0]);
            }
            if mode == 1 || mode == 2 {
                s.lose_put_response = true;
                s.read_failure = mode == 2;
            }
        }
        if mode == 3 {
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .proof
                .gates
                .panic = true;
        }
        let mut result = None;
        if let Some(a) = &observer {
            let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
            let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
            let sample =
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), a.owner.key());
            if let Ok(sample) = sample {
                // Derive the actual named key rather than assuming fixture incarnation.
                if mode == 0 {
                    let mut s = storage.backend().state.lock().unwrap();
                    s.put_faults.clear();
                    s.put_faults.insert(
                        format!(
                            "failure/{}",
                            crate::storage::ObjectKey::heartbeat(sample.binding().incarnation)
                                .as_str()
                        ),
                        (StorageError::InvalidCatalog, false),
                    );
                }
                if let Ok(ticket) =
                    owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
                {
                    let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
                    let completion = owners.take_proof(&ticket, &a.owner.key());
                    let no_ack = completion
                        .as_ref()
                        .is_none_or(|c| c.acknowledgement().is_none());
                    let snap = owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .payload
                        .retention_locked();
                    let writer = owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .payload
                        .proof
                        .writer_retained();
                    result = Some((
                        matches!(waited, Ok(Err(ProofError::Failed))),
                        snap.failed,
                        snap.proof_cycles,
                        no_ack,
                        writer,
                        completion.is_some(),
                    ));
                    drop(completion);
                    drop(ticket);
                }
            }
        }
        // Fake puts/reads above have returned synchronously. Panic handles were polled
        // to termination by wait_proof. Quarantine is test-only, never settlement.
        let quarantined = owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .proof
            .quarantine();
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        outcomes.push((ready, result, quarantined));
    }
    for (mode, (ready, result, quarantined)) in outcomes.into_iter().enumerate() {
        assert!(ready);
        if mode == 0 {
            assert_eq!(result, Some((false, false, 1, true, true, true)));
            assert!(!quarantined);
        } else {
            assert_eq!(result, Some((true, true, 1, true, true, false)));
            assert!(quarantined);
        }
    }
}

#[tokio::test]
async fn proof_cycle_placeholder_shutdown_is_resumable() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let owners = Arc::new(owners);
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("placeholder").unwrap(),
    ));
    let (entered, at) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    owners.shared.state.lock().unwrap().payload.proof.preparing = Some((entered, gate));
    let mut task = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
        let sample =
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key());
        if let Ok(sample) = sample {
            let o = owners.clone();
            let s = storage.clone();
            task = Some(tokio::task::spawn_blocking(move || {
                o.publish_proof(sample, s, Instant::now() + WATCHDOG)
            }));
        }
    }
    let reached = tokio::time::timeout(WATCHDOG, at).await;
    let snapshot = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    let before = storage.backend().state.lock().unwrap().puts;
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        owners.close_and_drain(),
    )
    .await
    .is_err();
    let _ = release.send(());
    let ticket = if let Some(task) = task {
        task.await.ok().and_then(|result| result.ok())
    } else {
        None
    };
    drop(ticket);
    drop(observer);
    drop(process);
    let owners = Arc::try_unwrap(owners).ok().unwrap();
    finish_distributed(owners, threads, path).await;
    assert!(ready && matches!(reached, Ok(Ok(()))) && cancelled);
    assert_eq!(
        (snapshot.proof_cycles, snapshot.supervisors, before),
        (1, 2, 0)
    );
}

#[tokio::test]
async fn proof_cycle_exact_key_exhaustion_and_independent_work() {
    use super::super::{admission::BarrierPreparation, drain_observation::ProofError};
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let (mut fa, a, ta, pa, process_a) = signed_booted().await;
    let (mut fb, b, tb, pb, process_b) = signed_booted().await;
    let (oa, ra) = classifier_observer(&mut fa, &a, &process_a).await;
    let (ob, rb) = classifier_observer(&mut fb, &b, &process_b).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("identity").unwrap(),
    ));
    let mut result = None;
    if let (Some(oa), Some(ob)) = (&oa, &ob) {
        let boot = a.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&fa.root, boot.clone()).unwrap();
        let sample =
            a.shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), oa.owner.key());
        if let Ok(sample) = sample
            && let Ok(ticket) = a.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
        {
            let waited = tokio::time::timeout(WATCHDOG, a.wait_proof(&ticket)).await;
            let foreign = a.take_proof(&ticket, &ob.owner.key()).is_none();
            let hold = oa.owner.retain_response();
            drop(hold);
            let completion = a.take_proof(&ticket, &oa.owner.key());
            let stale = completion
                .as_ref()
                .is_some_and(|c| !a.shared.state.lock().unwrap().barrier_matches(c.sample()));
            drop(completion);
            drop(ticket);
            reap_shared(&a.shared, None);
            a.shared
                .state
                .lock()
                .unwrap()
                .payload
                .proof
                .last_cycle_next();
            let prepared = BarrierPreparation::prepare(&fa.root, boot.clone()).unwrap();
            let last =
                a.shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), oa.owner.key());
            let mut reused = false;
            if let Ok(sample) = last
                && let Ok(ticket) =
                    a.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
            {
                let waited = tokio::time::timeout(WATCHDOG, a.wait_proof(&ticket)).await;
                let completion = a.take_proof(&ticket, &oa.owner.key());
                reused = matches!(waited, Ok(Ok(())))
                    && completion.as_ref().is_some_and(|c| {
                        c.acknowledgement().is_some_and(|ack| ack.sequence() == 2)
                    });
                drop(completion);
                drop(ticket);
                reap_shared(&a.shared, None);
            }
            let prepared = BarrierPreparation::prepare(&fa.root, boot).unwrap();
            let sample =
                a.shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), oa.owner.key());
            let exhausted = sample.is_ok_and(|s| {
                matches!(
                    a.publish_proof(s, storage.clone(), Instant::now() + WATCHDOG),
                    Err(ProofError::Exhausted)
                )
            });
            result = Some(matches!(waited, Ok(Ok(()))) && foreign && stale && reused && exhausted);
        }
    }
    drop(oa);
    drop(ob);
    drop(process_a);
    drop(process_b);
    let ca = tokio::spawn(finish_distributed(a, ta, pa));
    let cb = tokio::spawn(finish_distributed(b, tb, pb));
    let (ca, cb) = tokio::join!(ca, cb);
    assert!(ca.is_ok() && cb.is_ok() && ra && rb);
    assert_eq!(result, Some(true));
}
struct PendingProofFake {
    fake: Arc<crate::test_storage::Fake>,
    gate: Arc<crate::test_storage::ReadGate>,
    effect: Mutex<
        Option<tokio::task::JoinHandle<std::result::Result<String, crate::storage::StorageError>>>,
    >,
}
impl crate::storage::ObjectStore for PendingProofFake {
    async fn get(
        &self,
        key: &str,
    ) -> std::result::Result<crate::storage::Download, crate::storage::StorageError> {
        crate::storage::ObjectStore::get(&*self.fake, key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: crate::storage::WriteCondition<'_>,
    ) -> std::result::Result<String, crate::storage::StorageError> {
        // This fixture's first writer uses Create. The effect survives adapter cancellation,
        // with its sole handle retained here and explicitly joined by the test.
        assert!(matches!(condition, crate::storage::WriteCondition::Create));
        let fake = self.fake.clone();
        let gate = self.gate.clone();
        let key = key.to_owned();
        let (send, receive) = oneshot::channel();
        let task = tokio::spawn(async move {
            gate.entered.add_permits(1);
            if let Ok(Ok(permit)) = tokio::time::timeout(WATCHDOG, gate.release.acquire()).await {
                permit.forget();
            }
            let result = crate::storage::ObjectStore::put(
                &*fake,
                &key,
                bytes,
                crate::storage::WriteCondition::Create,
            )
            .await;
            let _ = send.send(());
            result
        });
        *self.effect.lock().unwrap() = Some(task);
        let _ = receive.await;
        Err(crate::storage::StorageError::Ambiguous)
    }
}

#[tokio::test]
async fn proof_cycle_cancelled_ticket_ambiguous_provider_is_not_recovered_by_readback() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::{Fake, ReadGate},
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let gate = Arc::new(ReadGate::default());
    let storage = Arc::new(Storage::new(
        PendingProofFake {
            fake: Arc::new(Fake::default()),
            gate: gate.clone(),
            effect: Mutex::new(None),
        },
        Namespace::new("pending").unwrap(),
    ));
    let mut ticket = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
        let sample =
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key());
        if let Ok(sample) = sample {
            ticket = owners
                .publish_proof(
                    sample,
                    storage.clone(),
                    Instant::now() + std::time::Duration::from_millis(50),
                )
                .ok();
        }
    }
    let entered = tokio::time::timeout(WATCHDOG, gate.entered.acquire())
        .await
        .is_ok();
    let active = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    drop(ticket);
    let drain = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let retained = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    let outcome = Some(active.proof_cycles == 1 && retained.failed && retained.proof_cycles == 1);
    gate.release.add_permits(1);
    let effect = storage.backend().effect.lock().unwrap().take();
    let settled = if let Some(effect) = effect {
        matches!(effect.await, Ok(Ok(_)))
    } else {
        false
    };
    reap_shared(&owners.shared, None);
    let still_failed = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked()
        .failed;
    let quarantined = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .proof
        .quarantine();
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && entered && settled && still_failed && quarantined);
    assert!(matches!(drain, Ok(Err(OwnerError::FailedDrain))));
    assert_eq!(outcome, Some(true));
}

#[tokio::test]
async fn proof_cycle_buffered_ack_cancel_and_resume_shutdown() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::{Fake, ReadGate},
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let gate = Arc::new(ReadGate::default());
    owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .proof
        .gates
        .returning = Some(gate.clone());
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("shutdown").unwrap(),
    ));
    let mut ticket = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
        let sample =
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .sample_barrier(prepared, id(30), a.owner.key());
        if let Ok(sample) = sample {
            ticket = owners
                .publish_proof(sample, storage, Instant::now() + WATCHDOG)
                .ok();
        }
    }
    let entered = tokio::time::timeout(WATCHDOG, gate.entered.acquire())
        .await
        .is_ok();
    let drain_cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        owners.close_and_drain(),
    )
    .await
    .is_err();
    let retained = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    gate.release.add_permits(1);
    drop(observer);
    drop(process);
    // Keep the cancelled ticket alive through resumed full drain. Its shared
    // identity must not keep a buffered outcome/provider after actual reap.
    let drained = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let final_snapshot = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    drop(ticket);
    finish_distributed(owners, threads, path).await;
    assert!(ready && entered && drain_cancelled && matches!(drained, Ok(Ok(()))));
    assert_eq!((retained.proof_cycles, retained.proof_results), (1, 1));
    assert_eq!(
        (final_snapshot.proof_cycles, final_snapshot.proof_results),
        (0, 0)
    );
}

#[tokio::test]
async fn proof_cycle_regression_drops_preserve_work_generation() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let mut outcomes = Vec::new();
    for ticket_first in [true, false] {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let storage = Arc::new(Storage::new(
            Fake::default(),
            Namespace::new("proof-drop").unwrap(),
        ));
        let mut outcome = None;
        if let Some(a) = &observer {
            let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
            let prepared = BarrierPreparation::prepare(&f.root, boot.clone());
            let retained = BarrierPreparation::prepare(&f.root, boot);
            let sample = prepared.ok().and_then(|p| {
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), a.owner.key())
                    .ok()
            });
            let baseline = retained.ok().and_then(|p| {
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), a.owner.key())
                    .ok()
            });
            if let (Some(sample), Some(baseline)) = (sample, baseline) {
                let mut ticket = owners
                    .publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
                    .ok();
                let waited = if let Some(t) = &ticket {
                    matches!(
                        tokio::time::timeout(WATCHDOG, owners.wait_proof(t)).await,
                        Ok(Ok(()))
                    )
                } else {
                    false
                };
                let completion = ticket
                    .as_ref()
                    .and_then(|t| owners.take_proof(t, &a.owner.key()));
                let before = completion.as_ref().is_some_and(|c| {
                    c.acknowledgement().is_some_and(|ack| ack.sequence() == 1)
                        && owners
                            .shared
                            .state
                            .lock()
                            .unwrap()
                            .barrier_matches(c.sample())
                });
                if ticket_first {
                    drop(ticket.take());
                }
                let live = completion.as_ref().is_some_and(|c| {
                    owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .barrier_matches(c.sample())
                });
                let retained = owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .retention_locked()
                    .proof_cycles;
                let (entered, at_release) = oneshot::channel();
                let (release, gate) = std::sync::mpsc::channel();
                *owners.shared.releasing.lock().unwrap() = Some((entered, gate));
                drop(completion);
                let shared = owners.shared.clone();
                let reaper = std::thread::spawn(move || reap_shared(&shared, None));
                let reached =
                    matches!(tokio::time::timeout(WATCHDOG, at_release).await, Ok(Ok(())));
                let pending = {
                    let state = owners.shared.state.lock().unwrap();
                    let snapshot = state.payload.retention_locked();
                    (
                        snapshot.proof_cycles,
                        snapshot.pending_releases,
                        state.barrier_matches(&baseline),
                    )
                };
                let _ = release.send(());
                let joined = reaper.join().is_ok();
                let settled = {
                    let state = owners.shared.state.lock().unwrap();
                    let snapshot = state.payload.retention_locked();
                    (
                        snapshot.proof_cycles,
                        snapshot.pending_releases,
                        state.barrier_matches(&baseline),
                    )
                };
                drop(ticket);
                let after_ticket = owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .barrier_matches(&baseline);
                outcome = Some((
                    waited,
                    before,
                    live,
                    retained,
                    reached,
                    pending,
                    joined,
                    settled,
                    after_ticket,
                ));
            }
        }
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        outcomes.push((ticket_first, ready, outcome));
    }
    for (ticket_first, ready, outcome) in outcomes {
        assert!(ready);
        assert_eq!(
            outcome,
            Some((
                true,
                true,
                true,
                1,
                true,
                (0, 1, false),
                true,
                (0, 0, true),
                true
            )),
            "ticket_first={ticket_first}"
        );
    }
}

#[tokio::test]
async fn proof_cycle_regression_spawn_allows_receiver_inspection() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("spawn-lock").unwrap(),
    ));
    let mut outcome = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let sample = BarrierPreparation::prepare(&f.root, boot)
            .ok()
            .and_then(|p| {
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), a.owner.key())
                    .ok()
            });
        if let Some(sample) = sample {
            let (probe, observed) = oneshot::channel();
            owners.shared.state.lock().unwrap().payload.proof.spawning = Some(probe);
            let ticket = owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG);
            let inspection = tokio::time::timeout(WATCHDOG, observed).await;
            let waited = if let Ok(t) = &ticket {
                matches!(
                    tokio::time::timeout(WATCHDOG, owners.wait_proof(t)).await,
                    Ok(Ok(()))
                )
            } else {
                false
            };
            let completion = ticket
                .as_ref()
                .ok()
                .and_then(|t| owners.take_proof(t, &a.owner.key()));
            let acknowledged = completion
                .as_ref()
                .is_some_and(|c| c.acknowledgement().is_some());
            outcome = Some((matches!(inspection, Ok(Ok(true))), waited, acknowledged));
            drop(completion);
            drop(ticket);
        }
    }
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(outcome, Some((true, true, true)));
}

#[tokio::test]
async fn proof_cycle_waiting_drain_survives_handle_publication() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::Fake,
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let owners = Arc::new(owners);
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("waiting-drain").unwrap(),
    ));
    let (entered, at_preparation) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    owners.shared.state.lock().unwrap().payload.proof.preparing = Some((entered, gate));
    let mut publishing = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let sample = BarrierPreparation::prepare(&f.root, boot)
            .ok()
            .and_then(|p| {
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(p, id(30), a.owner.key())
                    .ok()
            });
        if let Some(sample) = sample {
            let retained_owners = owners.clone();
            let retained_storage = storage.clone();
            publishing = Some(tokio::task::spawn_blocking(move || {
                retained_owners.publish_proof(
                    sample,
                    retained_storage,
                    Instant::now() + WATCHDOG * 3,
                )
            }));
        }
    }
    let reached = matches!(
        tokio::time::timeout(WATCHDOG, at_preparation).await,
        Ok(Ok(()))
    );
    drop(observer);
    drop(process);
    let draining_owners = owners.clone();
    let mut draining = tokio::spawn(async move { draining_owners.close_and_drain().await });
    let ordinary_reaped = tokio::time::timeout(WATCHDOG, async {
        loop {
            let ordinary_empty = {
                let state = owners.shared.state.lock().unwrap();
                let snapshot = state.payload.retention_locked();
                snapshot.accepted == 0
                    && snapshot.records == 0
                    && snapshot.payload_positions == 0
                    && snapshot.preparations == 0
                    && snapshot.supervisors == 0
                    && snapshot.pending_releases == 0
                    && snapshot.proof_cycles == 1
                    && snapshot.proof_results == 0
                    && !snapshot.failed
            };
            if ordinary_empty {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let still_waiting = !draining.is_finished();
    let puts_before_publication = storage.backend().state.lock().unwrap().puts;
    let _ = release.send(());
    let ticket = if let Some(publishing) = publishing {
        publishing.await.ok().and_then(|published| published.ok())
    } else {
        None
    };
    // Keep the original ticket alive: shutdown already cancelled this cycle.
    let original_result = tokio::time::timeout(WATCHDOG, &mut draining).await;
    let completed_without_rescue = matches!(original_result, Ok(Ok(Ok(()))));
    let mut settled = completed_without_rescue;
    if original_result.is_err() {
        // Recovery is only teardown after the failed observation was captured.
        owners.shared.reap_wake.0.notify_waiters();
        let recovered = tokio::time::timeout(WATCHDOG, &mut draining).await;
        settled = matches!(recovered, Ok(Ok(Ok(()))));
        if recovered.is_err() {
            draining.abort();
            let _ = draining.await;
        }
    }
    let final_snapshot = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    let published = ticket.is_some();
    drop(ticket);
    let puts = storage.backend().state.lock().unwrap().puts;
    let owners = Arc::try_unwrap(owners).ok().unwrap();
    finish_distributed(owners, threads, path).await;
    assert!(ready && reached && ordinary_reaped && still_waiting && published && settled);
    assert_eq!(puts_before_publication, 0);
    assert_eq!(puts, 1);
    assert_eq!(
        (
            final_snapshot.proof_cycles,
            final_snapshot.supervisors,
            final_snapshot.pending_releases
        ),
        (0, 0, 0)
    );
    assert!(
        completed_without_rescue,
        "the original drain waiter missed handle publication"
    );
}

#[tokio::test]
async fn final_drain_receipt_reaped_original_and_ticket_drop() {
    use super::super::admission::BarrierPreparation;
    use crate::{
        storage::{Namespace, Storage},
        test_storage::{Fake, ReadGate},
    };
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(Storage::new(
        Fake::default(),
        Namespace::new("final").unwrap(),
    ));
    let gate = Arc::new(ReadGate::default());
    owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .proof
        .gates
        .returning = Some(gate.clone());
    let mut outcome = None;
    if let Some(a) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
        let sample = owners
            .shared
            .state
            .lock()
            .unwrap()
            .sample_barrier(prepared, id(30), a.owner.key())
            .unwrap();
        let ticket = owners
            .publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
            .unwrap();
        let reached = tokio::time::timeout(WATCHDOG, gate.entered.acquire())
            .await
            .is_ok();
        let early = owners.take_proof(&ticket, &a.owner.key()).is_none();
        gate.release.add_permits(1);
        let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
        let completion = owners.take_proof(&ticket, &a.owner.key());
        drop(ticket);
        let ordinary = classify(&owners, &a.owner.key());
        let result = completion.and_then(|c| owners.finish_drain(c).ok());
        let reply = result
            .as_ref()
            .map(crate::node::auth::DrainReply::from_observation);
        outcome = Some((
            reached,
            early,
            matches!(waited, Ok(Ok(()))),
            ordinary == DrainWork::WorkPresent,
            reply.is_some_and(|r| {
                r.idle() && r.barrier() && r.heartbeat_sequence() == 1 && r.encode().is_ok()
            }) && result.as_ref().is_some_and(|o| final_reply_verified(&f, o)),
        ));
        drop(result);
    }
    gate.release.add_permits(1);
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(outcome, Some((true, true, true, true, true)));
}

fn final_reply_verified(
    f: &ReceiverProofs,
    observation: &super::super::admission::DrainObservation,
) -> bool {
    use crate::node::auth::{
        DrainReply, ReplyAuth, RequestContext, check_reply_content, verify_reply,
    };
    let reply = DrainReply::from_observation(observation);
    let Ok(body) = reply.encode() else {
        return false;
    };
    let original = observation.request_binding().auth();
    let initial = format!(r#"{{"version":1,"drain_id":"{}"}}"#, reply.drain_id()).into_bytes();
    let Ok(context) = RequestContext::for_send(original.clone(), initial, &f.root) else {
        return false;
    };
    let sender = format!(
        r#"{{"role":"worker","process_id":"{}","node_id":"{}","incarnation":"{}","session_id":"{}","public_key":"{}"}}"#,
        id(4),
        id(3),
        id(4),
        id(5),
        hex(&f.key.public_key())
    );
    let receiver = serde_json::to_string(original.sender()).unwrap();
    let text = format!(r#"{{"version":1,"cluster_id":"{}","request_sequence":{},"scope":"drain","sender":{},"receiver":{},"status":200,"control_sha256":"{}"}}"#,
        original.cluster_id(), original.sequence(), sender, receiver, hex(ring::digest::digest(&ring::digest::SHA256, &body).as_ref())).into_bytes();
    let Ok(auth) = ReplyAuth::decode(&hex(&text)) else {
        return false;
    };
    let Ok(signature) = f.key.sign_response(&auth) else {
        return false;
    };
    verify_reply(&hex(&text), &hex(&signature), 200, &context)
        .is_ok_and(|signed| check_reply_content(&signed, body).is_ok())
}
async fn final_proof(
    f: &ReceiverProofs,
    owners: &PayloadOwners,
    observer: &ControlLease,
    storage: &Arc<crate::storage::Storage<crate::test_storage::Fake>>,
) -> Option<super::super::drain_observation::ReapedProof> {
    use super::super::admission::BarrierPreparation;
    let boot = owners.shared.state.lock().unwrap().local()?.boot();
    let prepared = BarrierPreparation::prepare(&f.root, boot).ok()?;
    let sample = owners
        .shared
        .state
        .lock()
        .unwrap()
        .sample_barrier(prepared, id(30), observer.key())
        .ok()?;
    let ticket = owners
        .publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
        .ok()?;
    let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
    let result = if matches!(waited, Ok(Ok(()))) {
        owners.take_proof(&ticket, &observer.key())
    } else {
        None
    };
    drop(ticket);
    result
}
async fn final_cycle_released(owners: &PayloadOwners) -> bool {
    tokio::time::timeout(WATCHDOG, async {
        loop {
            owners.release_pending();
            if owners.shared.state.lock().unwrap().payload.proof.retained() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}
fn final_nonpositive(
    owners: &PayloadOwners,
    proof: &mut Option<super::super::drain_observation::ReapedProof>,
) -> bool {
    proof.take().is_some_and(|p| match owners.finish_drain(p) {
        Ok(o) => !o.idle(),
        Err(_) => true,
    })
}

#[tokio::test]
async fn final_drain_receipt_account_publication_cleanup_and_release() {
    for phase in 0..7 {
        use super::super::admission::{AcceptedRoute, OperationCandidates, PreparedRoute};
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let transport = Arc::new(Semaphore::new(1));
        let pair = owners
            .prepare_pair(
                Role::Operation,
                transport.try_acquire_owned().unwrap(),
                None,
            )
            .unwrap();
        let slot = pair.account.slot.clone();
        let (discarded, at_discard) = oneshot::channel();
        let (release_discard, discard_gate) = std::sync::mpsc::channel();
        let (returning, at_return) = oneshot::channel();
        let (release_return, return_gate) = oneshot::channel();
        {
            let mut h = slot.hooks.lock().unwrap();
            h.install_discard = Some((discarded, discard_gate));
            h.returning = Some((returning, return_gate));
        }
        let mut route = PreparedRoute::Operation(pair, OperationCandidates::new());
        let proof = f.request("operation", 1, boot);
        let accepted = owners.accept(proof.0, proof.1, &mut route, &process);
        let mut account = None;
        let mut work_ready = false;
        if let Ok(AcceptedRoute::Operation(mut a)) = accepted {
            work_ready = matches!(
                tokio::time::timeout(WATCHDOG, a.owners.stream.ready()).await,
                Ok(Ok(_))
            );
            account = a.owners.account.take();
            if let Some(a) = account.as_mut() {
                work_ready &= matches!(tokio::time::timeout(WATCHDOG, a.ready()).await, Ok(Ok(_)));
            }
        }
        drop(route);
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let observe = || observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
        let storage = Arc::new(crate::storage::Storage::new(
            crate::test_storage::Fake::default(),
            crate::storage::Namespace::new("composition").unwrap(),
        ));
        let mut proof = if let Some(a) = &observer {
            final_proof(&f, &owners, &a.owner, &storage).await
        } else {
            None
        };
        let had_proof = proof.is_some();
        let mut final_checked = false;
        if phase == 0 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let active = observe();
        let publication = account.as_ref().map(PayloadLease::retain_delivery);
        let publication_ok = matches!(publication, Some(Ok(_)));
        drop(account);
        let held = observe();
        if phase == 1 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        drop(publication);
        let discarded = tokio::time::timeout(WATCHDOG, async {
        tokio::pin!(at_discard);
        loop {
            owners.release_pending();
            tokio::select! { r = &mut at_discard => break r, _ = tokio::task::yield_now() => {} }
        }
    })
    .await;
        let during_discard = observe();
        if phase == 2 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let _ = release_discard.send(());
        let returned = tokio::time::timeout(WATCHDOG, at_return).await;
        let before_return = observe();
        if phase == 3 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let cleanup = slot.cleanup.load(Ordering::Acquire);
        owners.release_pending();
        let still_retained = observe();
        if phase == 4 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let _ = release_return.send(());
        let finished = tokio::time::timeout(WATCHDOG, async {
            loop {
                let finished = owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .entries
                    .iter()
                    .find(|e| e.slot.id == slot.id)
                    .is_some_and(|e| e.handle.is_finished());
                if finished {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let unreaped = observe();
        if phase == 5 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let (at_release, releasing) = oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        *owners.shared.releasing.lock().unwrap() = Some((at_release, gate));
        let shared = owners.shared.clone();
        let reaper = std::thread::spawn(move || reap_shared(&shared, None));
        let releasing = tokio::time::timeout(WATCHDOG, releasing).await;
        let pending_release = observe();
        if phase == 6 {
            final_checked = final_nonpositive(&owners, &mut proof);
        }
        let _ = release.send(());
        let joined_reaper = reaper.join();
        drop(proof);
        let released = final_cycle_released(&owners).await;
        let after = observe();
        let fresh = if let Some(a) = &observer {
            final_proof(&f, &owners, &a.owner, &storage)
                .await
                .and_then(|p| owners.finish_drain(p).ok())
                .is_some_and(|o| o.idle())
        } else {
            false
        };
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        assert!(
            work_ready
                && ready
                && publication_ok
                && cleanup
                && finished.is_ok()
                && joined_reaper.is_ok()
        );
        assert!(
            matches!(discarded, Ok(Ok(())))
                && matches!(returned, Ok(Ok(())))
                && matches!(releasing, Ok(Ok(())))
        );
        for result in [
            active,
            held,
            during_discard,
            before_return,
            still_retained,
            unreaped,
            pending_release,
        ] {
            assert_eq!(result, Some(DrainWork::WorkPresent));
        }
        assert_eq!(after, Some(DrainWork::ExemptOnly));
        assert!(
            had_proof && final_checked && released && fresh,
            "phase {phase}"
        );
    }
}

#[tokio::test]
async fn final_drain_receipt_history_preparation_and_extra_response() {
    for mode in 0..3 {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let storage = Arc::new(crate::storage::Storage::new(
            crate::test_storage::Fake::default(),
            crate::storage::Namespace::new("history-final").unwrap(),
        ));
        let mut result = None;
        if let Some(a) = &observer {
            let proof = final_proof(&f, &owners, &a.owner, &storage).await;
            let had_proof = proof.is_some();
            let transport = Arc::new(Semaphore::new(1));
            let mut preparation = None;
            let mut delivery = None;
            let mut settled = true;
            if mode == 2 {
                delivery = a.owner.retain_response().ok();
            } else {
                preparation = owners
                    .prepare_control(transport.try_acquire_owned().unwrap())
                    .ok();
                if mode == 0 {
                    let owner_id = preparation.as_ref().map(|p| p.slot.id);
                    drop(preparation.take());
                    settled = if let Some(owner_id) = owner_id {
                        matches!(
                            tokio::time::timeout(
                                WATCHDOG,
                                wait_reap(&owners.shared, Some(owner_id))
                            )
                            .await,
                            Ok(Ok(()))
                        )
                    } else {
                        false
                    };
                }
            }
            let empty = mode != 0
                || owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .preparations
                    .iter()
                    .all(Option::is_none);
            let old = proof
                .and_then(|p| owners.finish_drain(p).ok())
                .map(|o| o.idle());
            drop(preparation);
            drop(delivery);
            let released = final_cycle_released(&owners).await;
            let settled_all = tokio::time::timeout(WATCHDOG, async {
                loop {
                    owners.release_pending();
                    if classify(&owners, &a.owner.key()) == DrainWork::ExemptOnly {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            let fresh = final_proof(&f, &owners, &a.owner, &storage)
                .await
                .and_then(|p| owners.finish_drain(p).ok())
                .is_some_and(|o| o.idle());
            result = Some((
                had_proof
                    && if mode == 0 {
                        old == Some(false)
                    } else {
                        old != Some(true)
                    },
                settled,
                empty,
                released,
                settled_all,
                fresh,
            ));
        }
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        assert!(ready);
        assert_eq!(
            result,
            Some((true, true, true, true, true, true)),
            "mode {mode}"
        );
    }
}
#[tokio::test]
async fn final_drain_receipt_foreign_cycle_preserves_both_receivers() {
    let (mut fa, a, ta, pa, process_a) = signed_booted().await;
    let (mut fb, b, tb, pb, process_b) = signed_booted().await;
    let (oa, ra) = classifier_observer(&mut fa, &a, &process_a).await;
    let (ob, rb) = classifier_observer(&mut fb, &b, &process_b).await;
    let storage_a = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("foreign-a").unwrap(),
    ));
    let storage_b = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("foreign-b").unwrap(),
    ));
    let mut result = None;
    if let (Some(oa), Some(ob)) = (&oa, &ob) {
        let ca = final_proof(&fa, &a, &oa.owner, &storage_a).await;
        let cb = final_proof(&fb, &b, &ob.owner, &storage_b).await;
        let foreign = ca.is_some_and(|p| b.finish_drain(p).is_err());
        let local_b = cb
            .and_then(|p| b.finish_drain(p).ok())
            .is_some_and(|o| o.idle());
        let released = final_cycle_released(&a).await;
        let local_a = final_proof(&fa, &a, &oa.owner, &storage_a)
            .await
            .and_then(|p| a.finish_drain(p).ok())
            .is_some_and(|o| o.idle());
        result = Some((foreign, local_b, released, local_a));
    }
    drop(oa);
    drop(ob);
    drop(process_a);
    drop(process_b);
    let cleanup_a = tokio::spawn(finish_distributed(a, ta, pa));
    let cleanup_b = tokio::spawn(finish_distributed(b, tb, pb));
    let (a, b) = tokio::join!(cleanup_a, cleanup_b);
    assert!(a.is_ok() && b.is_ok() && ra && rb);
    assert_eq!(result, Some((true, true, true, true)));
}
#[tokio::test]
async fn final_drain_receipt_cancelled_observer_and_stale_root_refuse() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    for mode in 0..2 {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (mut observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let storage = Arc::new(crate::storage::Storage::new(
            crate::test_storage::Fake::default(),
            crate::storage::Namespace::new("stale-final").unwrap(),
        ));
        let mut result = None;
        if let Some(a) = &observer {
            let proof = final_proof(&f, &owners, &a.owner, &storage).await;
            if mode == 0 {
                a.owner.0.slot.cancel();
            } else {
                f.root.revision += 1;
                f.root.change_id = id(788);
            }
            let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
            let mut route = owners
                .prepare_control(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap())
                .map(PreparedRoute::Control);
            // A real admitted Drain observes the complete new root.
            let checked = f.request("drain", 100, boot);
            let mut replacement = route
                .as_mut()
                .ok()
                .map(|r| owners.accept(checked.0, checked.1, r, &process));
            let replacement_ready = if let Some(Ok(AcceptedRoute::Drain(a))) = replacement.as_mut()
            {
                matches!(
                    tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
                    Ok(Ok(()))
                )
            } else {
                false
            };
            let refused = proof.is_some_and(|p| owners.finish_drain(p).is_err());
            drop(observer.take());
            let released = final_cycle_released(&owners).await;
            let fresh = if let Some(Ok(AcceptedRoute::Drain(a))) = &replacement {
                let settled = tokio::time::timeout(WATCHDOG, async {
                    loop {
                        owners.release_pending();
                        if owners.shared.state.lock().unwrap().payload.controls.len() == 1 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_ok();
                settled
                    && final_proof(&f, &owners, &a.owner, &storage)
                        .await
                        .and_then(|p| owners.finish_drain(p).ok())
                        .is_some_and(|o| o.idle())
            } else {
                false
            };
            result = Some((replacement_ready, refused, released, fresh));
            drop(replacement);
            drop(route);
        }
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        assert!(ready);
        assert_eq!(result, Some((true, true, true, true)), "mode {mode}");
    }
}

#[tokio::test]
async fn final_drain_receipt_unrelated_control_and_preparation() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(2));
    let mut route = owners
        .prepare_control(transport.clone().try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("readiness", 1, boot);
    let mut unrelated = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process));
    let control_ready = if let Some(Ok(AcceptedRoute::Readiness(a))) = unrelated.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("fixture-final").unwrap(),
    ));
    let mut completion = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage).await
    } else {
        None
    };

    let retained = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let wrong_route = unrelated.as_ref().and_then(|r| match r {
        Ok(AcceptedRoute::Readiness(a)) => Some(classify(&owners, &a.owner.key())),
        _ => None,
    });
    let control_denied = final_nonpositive(&owners, &mut completion);
    let control_released = final_cycle_released(&owners).await;
    drop(unrelated);
    drop(route);
    let reaped = tokio::time::timeout(WATCHDOG, async {
        loop {
            owners.release_pending();
            if owners.shared.state.lock().unwrap().payload.controls.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let baseline = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let preparation = owners.prepare_control(transport.try_acquire_owned().unwrap());
    completion = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage).await
    } else {
        None
    };
    let dormant = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    drop(preparation);
    let final_denied = final_nonpositive(&owners, &mut completion);
    let released = final_cycle_released(&owners).await;
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && control_ready && reaped.is_ok());
    assert_eq!(retained, Some(DrainWork::WorkPresent));
    assert_eq!(wrong_route, Some(DrainWork::InvalidOwner));
    assert_eq!(baseline, Some(DrainWork::ExemptOnly));
    assert_eq!(dormant, Some(DrainWork::WorkPresent));
    assert!(control_denied && control_released && final_denied && released);
}

#[tokio::test]
async fn final_drain_receipt_dependency_without_payload() {
    use super::super::admission::{AcceptedRoute, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
    let transport = Arc::new(Semaphore::new(1));
    let mut route = owners
        .prepare_control(transport.try_acquire_owned().unwrap())
        .map(PreparedRoute::Control);
    let proof = f.request("dependency_open", 1, boot);
    let mut opened = route
        .as_mut()
        .ok()
        .map(|r| owners.accept(proof.0, proof.1, r, &process));
    let open_ready = if let Some(Ok(AcceptedRoute::Open(a))) = opened.as_mut() {
        matches!(
            tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
            Ok(Ok(()))
        )
    } else {
        false
    };
    drop(opened);
    drop(route);
    let reaped = tokio::time::timeout(WATCHDOG, async {
        loop {
            owners.release_pending();
            if owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .controls
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("fixture-final").unwrap(),
    ));
    let mut completion = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage).await
    } else {
        None
    };

    let evidence = observer.as_ref().map(|a| {
        let state = owners.shared.state.lock().unwrap();
        let local = state.local().unwrap().snapshot();
        (
            state
                .payload
                .drain_work_locked(id(30), &a.owner.key(), state.local().unwrap()),
            local.occupancy(),
            local.dependencies(),
            state.payload.entries.len(),
        )
    });
    let final_denied = final_nonpositive(&owners, &mut completion);
    let released = final_cycle_released(&owners).await;
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready && open_ready && reaped.is_ok());
    assert_eq!(
        evidence,
        Some((DrainWork::WorkPresent, (0, 0, 0, 0), (1, 0), 1))
    );
    assert!(final_denied && released);
}

#[tokio::test]
async fn final_drain_receipt_failed_process() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("fixture-final").unwrap(),
    ));
    let positive = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage)
            .await
            .and_then(|p| owners.finish_drain(p).ok())
            .is_some_and(|o| o.idle())
    } else {
        false
    };
    let first_released = final_cycle_released(&owners).await;
    let mut completion = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage).await
    } else {
        None
    };

    let baseline = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    let transport = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.controls.first().and_then(|entry| {
            if let Admission::Distributed {
                transport: Some(permit),
                ..
            } = &*entry.guards
            {
                Some(Arc::downgrade(permit))
            } else {
                None
            }
        })
    };
    let (observer_returning, observer_at_return) = oneshot::channel();
    let (observer_release, observer_gate) = oneshot::channel();
    if let Some(a) = &observer {
        a.owner.0.slot.hooks.lock().unwrap().returning = Some((observer_returning, observer_gate));
    }
    let (returning, at_return) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    process.slot.hooks.lock().unwrap().returning = Some((returning, gate));
    process.slot.cancel();
    let returned = tokio::time::timeout(WATCHDOG, at_return).await;
    {
        let state = owners.shared.state.lock().unwrap();
        if let Some(e) = state
            .payload
            .entries
            .iter()
            .find(|e| e.slot.id == process.slot.id)
        {
            e.handle.abort();
        }
    }
    let _ = release.send(());
    let finished = tokio::time::timeout(WATCHDOG, async {
        loop {
            let done = owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .entries
                .iter()
                .any(|e| e.slot.id == process.slot.id && e.handle.is_finished());
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    owners.release_pending();
    let failed = process.slot.failed.load(Ordering::Acquire);
    let final_denied = final_nonpositive(&owners, &mut completion);
    let released = final_cycle_released(&owners).await;
    let refusal = observer.as_ref().map(|a| classify(&owners, &a.owner.key()));
    drop(observer);
    drop(process);
    let observer_returned = tokio::time::timeout(WATCHDOG, observer_at_return).await;
    let drained = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    let retained_failed = owners.shared.state.lock().unwrap().payload.failed;
    let retained_observer = {
        let state = owners.shared.state.lock().unwrap();
        state.payload.controls.len() == 1
            && state.payload.controls[0]
                .slot
                .cleanup
                .load(Ordering::Acquire)
            && !state.payload.controls[0].handle.is_finished()
            && transport.as_ref().is_some_and(|t| t.strong_count() != 0)
    };
    let _ = observer_release.send(());
    // Global failure makes close_and_drain return early. Continue polling the
    // registered survivor until its original handle and guards are reaped.
    let observer_reaped = tokio::time::timeout(WATCHDOG, async {
        let waker = Waker::from(owners.shared.reap_wake.clone());
        loop {
            let notified = owners.shared.reap_wake.0.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            reap_shared(&owners.shared, Some(&mut Context::from_waker(&waker)));
            let settled = {
                let state = owners.shared.state.lock().unwrap();
                state.payload.controls.is_empty() && state.payload.pending_releases == 0
            };
            if settled {
                break;
            }
            notified.await;
        }
    })
    .await;
    let observer_settled = {
        let state = owners.shared.state.lock().unwrap();
        (
            state.payload.controls.len(),
            state
                .payload
                .records
                .iter()
                .filter(|r| matches!(r, Some(AcceptedOwner::Control(_))))
                .count(),
            state.payload.pending_releases,
            transport.as_ref().map(|t| t.strong_count()),
        )
    };
    // Test-only recovery still runs if an observation/watchdog failed. These
    // memory-only controls retain their original handles until they are joined.
    let remaining = std::mem::take(&mut owners.shared.state.lock().unwrap().payload.controls);
    for mut entry in remaining {
        entry.slot.cancel();
        if !entry.terminated
            && tokio::time::timeout(WATCHDOG, &mut entry.handle)
                .await
                .is_err()
        {
            entry.handle.abort();
            let _ = (&mut entry.handle).await;
        }
        if let Some(record) = entry.record {
            owners.shared.state.lock().unwrap().payload.records[record] = None;
        }
        drop(ManuallyDrop::into_inner(entry.guards));
    }
    let quarantine = tests::quarantine_teardown(&owners);
    tokio::pin!(quarantine);
    let quarantined = tokio::time::timeout(WATCHDOG, &mut quarantine).await;
    if quarantined.is_err() {
        quarantine.await;
    }
    let shutdown = threads.shutdown();
    tokio::pin!(shutdown);
    let joined = tokio::time::timeout(WATCHDOG, &mut shutdown).await;
    if joined.is_err() {
        let _ = shutdown.await;
    }
    let empty = std::fs::read_dir(&path).map(|mut r| r.next().is_none());
    let removed = std::fs::remove_dir(path);
    assert!(
        ready
            && failed
            && retained_failed
            && finished.is_ok()
            && quarantined.is_ok()
            && matches!(returned, Ok(Ok(())))
            && matches!(observer_returned, Ok(Ok(())))
            && retained_observer
            && observer_reaped.is_ok()
    );
    assert!(matches!(drained, Ok(Err(OwnerError::FailedDrain))) && matches!(joined, Ok(Ok(()))));
    assert!(matches!(empty, Ok(true)) && removed.is_ok());
    assert_eq!(observer_settled, (0, 0, 0, Some(0)));
    assert_eq!(baseline, Some(DrainWork::WorkPresent));
    assert!(positive && first_released && final_denied && released);
    assert_eq!(refusal, Some(DrainWork::InvalidOwner));
}

#[tokio::test]
async fn final_drain_receipt_no_ack_and_failed_outcomes() {
    use super::super::{admission::BarrierPreparation, drain_observation::ProofError};
    use crate::{
        storage::{Namespace, Storage, StorageError},
        test_storage::Fake,
    };
    let mut outcomes = Vec::new();
    for mode in 0..5 {
        let (mut f, owners, threads, path, process) = signed_booted().await;
        let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
        let storage = Arc::new(Storage::new(
            Fake::default(),
            Namespace::new("failure").unwrap(),
        ));
        {
            let mut s = storage.backend().state.lock().unwrap();
            if mode == 4 {
                s.race = Some(vec![0]);
            }
            if mode == 1 || mode == 2 {
                s.lose_put_response = true;
                s.read_failure = mode == 2;
            }
        }
        if mode == 3 {
            owners
                .shared
                .state
                .lock()
                .unwrap()
                .payload
                .proof
                .gates
                .panic = true;
        }
        let mut result = None;
        if let Some(a) = &observer {
            let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
            let prepared = BarrierPreparation::prepare(&f.root, boot).unwrap();
            let sample =
                owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .sample_barrier(prepared, id(30), a.owner.key());
            if let Ok(sample) = sample {
                // Derive the actual named key rather than assuming fixture incarnation.
                if mode == 0 {
                    let mut s = storage.backend().state.lock().unwrap();
                    s.put_faults.clear();
                    s.put_faults.insert(
                        format!(
                            "failure/{}",
                            crate::storage::ObjectKey::heartbeat(sample.binding().incarnation)
                                .as_str()
                        ),
                        (StorageError::InvalidCatalog, false),
                    );
                }
                if let Ok(ticket) =
                    owners.publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
                {
                    let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
                    let completion = owners.take_proof(&ticket, &a.owner.key());
                    let no_ack = completion
                        .as_ref()
                        .is_none_or(|c| c.acknowledgement().is_none());
                    let snap = owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .payload
                        .retention_locked();
                    let writer = owners
                        .shared
                        .state
                        .lock()
                        .unwrap()
                        .payload
                        .proof
                        .writer_retained();
                    result = Some((
                        matches!(waited, Ok(Err(ProofError::Failed))),
                        snap.failed,
                        snap.proof_cycles,
                        no_ack,
                        writer,
                        completion.is_some(),
                    ));
                    let final_denied = completion.is_none_or(|p| owners.finish_drain(p).is_err());
                    result = result.filter(|_| final_denied);
                    drop(ticket);
                }
            }
        }
        // Fake puts/reads above have returned synchronously. Panic handles were polled
        // to termination by wait_proof. Quarantine is test-only, never settlement.
        let quarantined = owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .proof
            .quarantine();
        drop(observer);
        drop(process);
        finish_distributed(owners, threads, path).await;
        outcomes.push((ready, result, quarantined));
    }
    for (mode, (ready, result, quarantined)) in outcomes.into_iter().enumerate() {
        assert!(ready);
        if mode == 0 {
            assert_eq!(result, Some((false, false, 1, true, true, true)));
            assert!(!quarantined);
        } else {
            assert_eq!(result, Some((true, true, 1, true, true, false)));
            assert!(quarantined);
        }
    }
}

#[tokio::test]
async fn final_drain_receipt_shutdown_retains_observation() {
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("shutdown-final").unwrap(),
    ));
    let observation = if let Some(a) = &observer {
        final_proof(&f, &owners, &a.owner, &storage)
            .await
            .and_then(|p| owners.finish_drain(p).ok())
    } else {
        None
    };
    let positive = observation
        .as_ref()
        .is_some_and(|o| o.idle() && format!("{o:?}") == "DrainObservation");
    let before = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked();
    drop(observer);
    drop(process);
    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        owners.close_and_drain(),
    )
    .await
    .is_err();
    let retained = owners
        .shared
        .state
        .lock()
        .unwrap()
        .payload
        .retention_locked()
        .proof_cycles;
    drop(observation);
    finish_distributed(owners, threads, path).await;
    assert!(ready && positive && blocked);
    assert_eq!(
        (
            before.proof_cycles,
            before.accepted,
            before.controls,
            retained
        ),
        (1, 2, 1, 1)
    );
}
#[test]
fn final_drain_receipt_opaque_traits() {
    use super::super::admission::DrainObservation;
    trait NoClone<M> {
        fn marker() {}
    }
    impl<T> NoClone<()> for T {}
    impl<T: Clone> NoClone<u8> for T {}
    trait NoSerialize<M> {
        fn marker() {}
    }
    impl<T> NoSerialize<()> for T {}
    impl<T: serde::Serialize> NoSerialize<u8> for T {}
    trait NoDeserialize<M> {
        fn marker() {}
    }
    impl<T> NoDeserialize<()> for T {}
    impl<T: serde::Deserialize<'static>> NoDeserialize<u8> for T {}
    let _ = <DrainObservation as NoClone<_>>::marker;
    let _ = <DrainObservation as NoSerialize<_>>::marker;
    let _ = <DrainObservation as NoDeserialize<_>>::marker;
}

#[tokio::test]
async fn final_drain_receipt_replacement_cannot_take_original_cycle() {
    use super::super::admission::{AcceptedRoute, BarrierPreparation, PreparedRoute};
    let (mut f, owners, threads, path, process) = signed_booted().await;
    let (mut observer, ready) = classifier_observer(&mut f, &owners, &process).await;
    let storage = Arc::new(crate::storage::Storage::new(
        crate::test_storage::Fake::default(),
        crate::storage::Namespace::new("replacement-final").unwrap(),
    ));
    let mut result = None;
    if let Some(original) = &observer {
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let preparation = BarrierPreparation::prepare(&f.root, boot.clone()).unwrap();
        let key = original.owner.key();
        let sample = owners
            .shared
            .state
            .lock()
            .unwrap()
            .sample_barrier(preparation, id(30), original.owner.key())
            .unwrap();
        let ticket = owners
            .publish_proof(sample, storage.clone(), Instant::now() + WATCHDOG)
            .unwrap();
        let waited = tokio::time::timeout(WATCHDOG, owners.wait_proof(&ticket)).await;
        drop(observer.take());
        let gone = tokio::time::timeout(WATCHDOG, async {
            loop {
                owners.release_pending();
                if owners
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .payload
                    .controls
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let retained = owners.shared.state.lock().unwrap().payload.proof.retained();
        let mut route = owners
            .prepare_control(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap())
            .map(PreparedRoute::Control);
        let checked = f.request("drain", 100, boot);
        let mut replacement = route
            .as_mut()
            .ok()
            .map(|r| owners.accept(checked.0, checked.1, r, &process));
        if let Some(Ok(AcceptedRoute::Drain(a))) = replacement.as_mut() {
            let new_ready = matches!(
                tokio::time::timeout(WATCHDOG, a.owner.ready()).await,
                Ok(Ok(()))
            );
            let cannot_take = owners.take_proof(&ticket, &a.owner.key()).is_none();
            let old_refused = owners
                .take_proof(&ticket, &key)
                .is_some_and(|p| owners.finish_drain(p).is_err());
            drop(ticket);
            let released = final_cycle_released(&owners).await;
            let fresh = final_proof(&f, &owners, &a.owner, &storage)
                .await
                .and_then(|p| owners.finish_drain(p).ok())
                .is_some_and(|o| o.idle());
            result = Some(
                matches!(waited, Ok(Ok(())))
                    && gone
                    && retained == 1
                    && new_ready
                    && cannot_take
                    && old_refused
                    && released
                    && fresh,
            );
        } else {
            drop(ticket);
        }
        drop(replacement);
        drop(route);
    }
    drop(observer);
    drop(process);
    finish_distributed(owners, threads, path).await;
    assert!(ready);
    assert_eq!(result, Some(true));
}

#[tokio::test]
async fn final_drain_cleanup_watchdog_keeps_owner_drain_before_pool_shutdown() {
    let (owners, threads, path, process) = booted(1, 1, 0).await;
    let shared = owners.shared.clone();
    let pool = owners.shared.pool.clone();
    let retained = Arc::downgrade(&process.slot);
    let scratch = path.clone();
    let (returning, at_return) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    process.slot.hooks.lock().unwrap().returning = Some((returning, gate));
    drop(process);
    let reached_return = tokio::time::timeout(WATCHDOG, at_return).await;
    let mut cleanup = tokio::spawn(finish_distributed_with_watchdog(
        owners,
        threads,
        path,
        std::time::Duration::from_millis(20),
    ));
    let early = tokio::time::timeout(std::time::Duration::from_millis(200), &mut cleanup).await;
    let still_owned = early.is_err();
    let scratch_retained = scratch.exists();
    let pool_available = match pool.submit(0, |_, _| Ok(())).await {
        Ok(job) => job.receive().await.is_ok(),
        Err(_) => false,
    };
    let _ = release.send(());
    let reported = match early {
        Ok(result) => result,
        Err(_) => cleanup.await,
    };
    // Also settle the original registered handle on the pre-correction path,
    // where the helper has already returned without reaping it.
    let survivor = async {
        let waker = Waker::from(shared.reap_wake.clone());
        loop {
            let notified = shared.reap_wake.0.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            reap_shared(&shared, Some(&mut Context::from_waker(&waker)));
            if shared.state.lock().unwrap().payload.entries.is_empty() {
                break;
            }
            notified.await;
        }
    };
    tokio::pin!(survivor);
    let reaped = tokio::time::timeout(WATCHDOG, &mut survivor).await;
    if reaped.is_err() {
        survivor.await;
    }
    let settled = shared.state.lock().unwrap().payload.retention_locked();
    let removed = !scratch.exists();
    assert!(matches!(reached_return, Ok(Ok(()))) && reaped.is_ok());
    assert!(reported.is_err_and(|error| error.is_panic()));
    assert!(removed && settled.accepted == 0 && settled.supervisors == 0);
    assert_eq!(retained.strong_count(), 0);
    assert!(still_owned && scratch_retained && pool_available);
}

#[tokio::test]
async fn final_drain_cleanup_watchdog_keeps_original_worker_join() {
    struct WorkerExit(Option<oneshot::Sender<()>>);
    impl Drop for WorkerExit {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (owners, threads, path) = distributed(1, 1, 0);
    let scratch = path.clone();
    let (entered, at_work) = oneshot::channel();
    let (exited, at_exit) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let job = owners
        .shared
        .pool
        .submit(0, move |_, runtime| {
            // Runtime teardown drops this witness before the thread is joined.
            let witness = WorkerExit(Some(exited));
            let exit_task = runtime.spawn(async move {
                let _witness = witness;
                std::future::pending::<()>().await;
            });
            let _ = entered.send(());
            Ok((gate.recv_timeout(WATCHDOG).is_ok(), exit_task))
        })
        .await;
    let reached_work = tokio::time::timeout(WATCHDOG, at_work).await;
    let mut cleanup = tokio::spawn(finish_distributed_with_watchdog(
        owners,
        threads,
        path,
        std::time::Duration::from_millis(20),
    ));
    let early = tokio::time::timeout(std::time::Duration::from_millis(200), &mut cleanup).await;
    let still_owned = early.is_err();
    let scratch_retained = scratch.exists();
    let _ = release.send(());
    let (completed_work, exit_task) = match job {
        Ok(job) => match job.receive().await {
            Ok((completed, task)) => (completed, Some(task)),
            Err(_) => (false, None),
        },
        Err(_) => (false, None),
    };
    let reported = match early {
        Ok(result) => result,
        Err(_) => cleanup.await,
    };
    let witness_joined = match exit_task {
        Some(task) => task.await.is_err_and(|error| error.is_cancelled()),
        None => false,
    };
    // The pre-correction path loses its join future; still wait for the actual
    // worker runtime to exit before reporting the regression's observations.
    tokio::pin!(at_exit);
    let exited = tokio::time::timeout(WATCHDOG, &mut at_exit).await;
    if exited.is_err() {
        let _ = at_exit.await;
    }
    let removed = !scratch.exists();
    assert!(matches!(reached_work, Ok(Ok(()))) && completed_work);
    assert!(reported.is_err_and(|error| error.is_panic()));
    assert!(matches!(exited, Ok(Ok(()))) && removed && witness_joined);
    assert!(still_owned && scratch_retained);
}

// Regression for the production prelude handing an already-advanced TCP reader
// to the retained request owner. This proves the continuation, not a cluster.
#[tokio::test]
async fn checked_control_continues_original_tcp_reader_through_eof() {
    use super::super::{
        admission::{AcceptedRoute, DurableRequestView, PreparedRoute},
        frame::{FrameKind, FrameReader, FrameWriter},
        payload_stream::receive_checked_request,
    };
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    for case in 0..4 {
        let (f, owners, threads, path, process) = signed_booted().await;
        let boot = owners.shared.state.lock().unwrap().local().unwrap().boot();
        let transport = Arc::new(Semaphore::new(4));
        let mut open = PreparedRoute::Control(
            owners
                .prepare_control(transport.clone().try_acquire_owned().unwrap())
                .unwrap(),
        );
        let proof = f.request("dependency_open", 1, boot.clone());
        let opened = owners
            .accept(proof.0, proof.1, &mut open, &process)
            .unwrap();
        let (_, content) = f.request("job", 2, boot.clone());
        let bytes = encode_job(content.job().unwrap()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sending = tokio::spawn(async move {
            let mut tcp = TcpStream::connect(address).await.unwrap();
            {
                let mut writer = FrameWriter::new(&mut tcp);
                writer
                    .write_frame(FrameKind::Control, &bytes)
                    .await
                    .unwrap();
                if case == 1 {
                    writer
                        .write_frame(FrameKind::Control, &bytes)
                        .await
                        .unwrap();
                }
                if case != 2 {
                    writer.finish().await.unwrap();
                }
            }
            if case == 3 {
                tcp.write_all(b"trailer").await.unwrap();
            }
            tcp.shutdown().await.unwrap();
        });
        let (tcp, _) = listener.accept().await.unwrap();
        let mut reader =
            FrameReader::new(Box::new(tcp) as Box<dyn tokio::io::AsyncRead + Unpin + Send>);
        let first = reader.next_frame().await.unwrap().unwrap();
        assert_eq!(first.kind, FrameKind::Control);
        let (text, route) = f.auth("job", &first.payload);
        let text = text.replace("\"sequence\":1", "\"sequence\":2");
        let signed = f.sign_text(&text, &route);
        let eligible = check_request_authority(&signed, &f.root, &boot, &f.pin()).unwrap();
        let content = check_request_content(&signed, first.payload).unwrap();
        let original = content.job().unwrap() as *const JobEnvelope as usize;
        let view = DurableRequestView::prepare(&f.root, eligible, boot).unwrap();
        let mut job = PreparedRoute::Job(
            owners
                .prepare_pair(
                    Role::Dependency,
                    transport.clone().try_acquire_owned().unwrap(),
                    None,
                )
                .unwrap(),
        );
        let AcceptedRoute::Job(accepted) =
            owners.accept(view, content, &mut job, &process).unwrap()
        else {
            panic!("job refused")
        };
        let slot = accepted.owners.stream.slot.clone();
        let received = receive_checked_request(reader, accepted.owners.stream).await;
        let valid = match received {
            Ok(received) => {
                let same = matches!(&slot.disk.lock().unwrap().as_ref().unwrap().processor,
                    Processor::AcceptedRequest(job) if job.as_ref() as *const JobEnvelope as usize == original);
                received.lease.dispose().await.unwrap();
                same
            }
            Err(_) => false,
        };
        sending.await.unwrap();
        drop(accepted.owners.account);
        drop(opened);
        drop(open);
        drop(job);
        drop(process);
        finish_distributed(owners, threads, path).await;
        assert_eq!(valid, case == 0, "TCP case {case}");
    }
}

#[tokio::test]
async fn self_dependency_account_reaps_while_original_operation_response_is_retained() {
    let (owners, threads, path, process) = booted(1, 1, 0).await;
    let transport = Arc::new(Semaphore::new(2));
    let binding = owners
        .shared
        .state
        .lock()
        .unwrap()
        .local()
        .unwrap()
        .fixture_binding(100);
    let permit = uuid::Uuid::from_u128(20100);
    let mut operation = owners
        .prepare_pair(
            Role::Operation,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let installed = {
        let mut state = owners.shared.state.lock().unwrap();
        let (registry, local) = state.parts();
        registry
            .reserve_pair_locked(
                local.unwrap(),
                &mut operation,
                &process,
                binding.clone(),
                permit,
                None,
                tests::limits(),
            )
            .unwrap()
            .install_entry_locked()
    };
    drop(operation);
    let mut account = installed.account.unwrap();
    let mut stream = installed.stream;
    account.ready().await.unwrap();
    stream.ready().await.unwrap();
    let response = account.retain_delivery().unwrap();

    // Self TCP jobs use the same dependency permit as their original operation.
    let mut job = owners
        .prepare_pair(
            Role::Dependency,
            transport.clone().try_acquire_owned().unwrap(),
            None,
        )
        .unwrap();
    let installed = {
        let mut state = owners.shared.state.lock().unwrap();
        let (registry, local) = state.parts();
        registry
            .reserve_pair_locked(
                local.unwrap(),
                &mut job,
                &process,
                binding.clone(),
                permit,
                Some((uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(4))),
                tests::limits(),
            )
            .unwrap()
            .install_entry_locked()
    };
    drop(job);
    let mut job_stream = installed.stream;
    job_stream.ready().await.unwrap();
    let dependency_account = {
        let state = owners.shared.state.lock().unwrap();
        state
            .payload
            .entries
            .iter()
            .find(|entry| matches!(entry.guards.owner(), Some(DistributedOwner::Dependency(_))))
            .unwrap()
            .slot
            .id
    };
    let closed = owners
        .shared
        .state
        .lock()
        .unwrap()
        .close_dependency_locked(&binding, permit);
    drop(job_stream);
    // Drive the real handles through job cleanup and account reaping while the
    // original operation and its response hold are deliberately still alive.
    let released = tokio::time::timeout(
        WATCHDOG,
        poll_fn(|cx| {
            reap_shared(&owners.shared, Some(cx));
            let state = owners.shared.state.lock().unwrap();
            if state.payload.failed {
                Poll::Ready(false)
            } else if state
                .payload
                .entries
                .iter()
                .all(|entry| entry.slot.id != dependency_account)
            {
                Poll::Ready(true)
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
    let (counts, retained_operation, retained_response) = {
        let state = owners.shared.state.lock().unwrap();
        let retained = state
            .payload
            .entries
            .iter()
            .any(|entry| Arc::ptr_eq(&entry.slot, &account.slot));
        (
            state.local().unwrap().counts(),
            retained,
            *account.slot.delivery_holds.lock().unwrap() == 1,
        )
    };
    let operation_scratch_retained = account
        .slot
        .disk
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|disk| disk.operation.is_some());
    let charged_transport = transport.available_permits();
    drop(response);
    drop(stream);
    drop(account);
    drop(process);
    let drained = tokio::time::timeout(WATCHDOG, owners.close_and_drain()).await;
    if !matches!(drained, Ok(Ok(()))) {
        // The pre-fix failure quarantines original handles. Dispose them before
        // reporting the regression so the failing test leaves no worker/scratch.
        tokio::time::timeout(WATCHDOG, tests::quarantine_teardown(&owners))
            .await
            .unwrap();
    }
    let joined = tokio::time::timeout(WATCHDOG, threads.shutdown()).await;
    let empty = std::fs::read_dir(&path).unwrap().count() == 0;
    std::fs::remove_dir(path).unwrap();
    assert!(matches!(
        closed,
        Ok(super::super::admission::Close::Pending)
    ));
    assert!(
        matches!(released, Ok(true)),
        "self dependency account release: {released:?}"
    );
    assert_eq!(counts, (1, 0, 0, 0, 1));
    assert!(retained_operation && retained_response && operation_scratch_retained);
    assert_eq!(charged_transport, 1);
    assert!(matches!(drained, Ok(Ok(()))) && matches!(joined, Ok(Ok(()))) && empty);
    assert_eq!(transport.available_permits(), 2);
}
