use super::*;
#[tokio::test]
async fn worker_anchors_and_explicit_disposal() {
    let (owners, threads, parent, guard) = fixture();
    let mut process = owners.try_process(parent.clone(), limits()).unwrap();
    process.ready().await.unwrap();
    let mut operation = owners
        .try_operation(&process, limits(), guard.clone())
        .unwrap();
    operation.ready().await.unwrap();
    let mut lease = owners
        .try_admit(&operation, Role::Operation, guard)
        .unwrap();
    lease.ready().await.unwrap();
    lease.dispose().await.unwrap();
    operation.dispose().await.unwrap();
    process.dispose().await.unwrap();
    owners.close_and_drain().await.unwrap();
    threads.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    std::fs::remove_dir(parent).unwrap();
}
pub(super) fn limits() -> SpoolLimits {
    SpoolLimits::new(4096, 2).unwrap()
}
pub(super) fn fixture() -> (
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    std::path::PathBuf,
    Arc<tokio::sync::OwnedSemaphorePermit>,
) {
    let (pool, threads) = WorkerPool::start(
        std::num::NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let parent = std::env::temp_dir().join(format!("meshdb-owner-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&parent).unwrap();
    let guard = Arc::new(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap());
    (
        PayloadOwners::new(
            pool,
            Arc::new(Semaphore::new(1)),
            Arc::new(Semaphore::new(1)),
            Arc::new(Semaphore::new(1)),
        ),
        threads,
        parent,
        guard,
    )
}

#[derive(Default)]
pub(super) struct Hooks {
    pub(super) response_generation: Option<u64>,
    pub(super) install_command: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    pub(super) install_discard: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    pub(super) dormant_pending: Option<oneshot::Sender<()>>,
    pub(super) local_gate: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    pub(super) local_panic: bool,
    pub(super) local_ready: Option<oneshot::Sender<()>>,
    part_open_quota: Option<Arc<Mutex<Vec<bool>>>>,
    read_count: Option<Arc<std::sync::atomic::AtomicUsize>>,
    read_gate: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    append_gate: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    gate: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    pub(super) submit_pending: Option<oneshot::Sender<()>>,
    pub(super) panic: bool,
    fail_create: bool,
    pub(super) created: Vec<std::thread::ThreadId>,
    pub(super) executed: Vec<std::thread::ThreadId>,
    pub(super) disposed: Vec<std::thread::ThreadId>,
    pub(super) returning: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
pub(super) fn before_command(slot: &Slot) {
    let (gate, panic) = {
        let mut h = slot.hooks.lock().unwrap();
        h.executed.push(std::thread::current().id());
        (h.gate.take(), h.panic)
    };
    if let Some((started, release)) = gate {
        let _ = started.send(());
        let _ = release.recv();
    }
    assert!(!panic, "injected command panic");
}
pub(super) fn after_create(slot: &Slot) {
    let fail = {
        let mut h = slot.hooks.lock().unwrap();
        h.created.push(std::thread::current().id());
        h.fail_create
    };
    assert!(!fail, "injected anchor failure");
}
pub(super) async fn before_return(slot: &Slot) {
    let gate = slot.hooks.lock().unwrap().returning.take();
    if let Some((started, release)) = gate {
        let _ = started.send(());
        let _ = release.await;
    }
}

pub(super) fn controls() -> (Box<JobEnvelope>, Box<ResultEnvelope>) {
    use serde_json::json;
    let j = json!({"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","operation_id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000003","attempt_id":"00000000-0000-0000-0000-000000000004","request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"token"},"ordinal":0,"table":{"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":"tables/00000000-0000-0000-0000-000000000006/00000000-0000-0000-0000-000000000007.parquet","row_count":1}},"key":1}}});
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] =
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":32,"found":false}});
    (
        Box::new(serde_json::from_value(j).unwrap()),
        Box::new(serde_json::from_value(r).unwrap()),
    )
}
async fn anchors(
    owners: &PayloadOwners,
    parent: PathBuf,
    guard: Arc<OwnedSemaphorePermit>,
    quota: SpoolLimits,
) -> (PayloadLease, PayloadLease) {
    let mut process = owners.try_process(parent, limits()).unwrap();
    process.ready().await.unwrap();
    let mut op = owners.try_operation(&process, quota, guard).unwrap();
    op.ready().await.unwrap();
    (process, op)
}
async fn begin(lease: &mut PayloadLease) {
    let (job, result) = controls();
    lease
        .command(Command::BeginRows { job, result })
        .await
        .unwrap();
}
fn gate(slot: &Slot) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (tx, started) = oneshot::channel();
    let (release, rx) = std::sync::mpsc::channel();
    slot.hooks.lock().unwrap().gate = Some((tx, rx));
    (started, release)
}
async fn finish(
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    parent: PathBuf,
) {
    owners.close_and_drain().await.unwrap();
    threads.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    std::fs::remove_dir(parent).unwrap();
}
#[tokio::test]
async fn cancelled_append_and_panic_dispose_on_worker() {
    for panic in [false, true] {
        let (owners, threads, parent, guard) = fixture();
        let (process, op) = anchors(
            &owners,
            parent.clone(),
            guard.clone(),
            SpoolLimits::new(2, 1).unwrap(),
        )
        .await;
        let mut lease = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        lease.ready().await.unwrap();
        begin(&mut lease).await;
        let account = op.slot.clone();
        assert!(
            owners
                .shared
                .pool
                .submit(0, move |_, _| {
                    let d = account.disk.lock().unwrap();
                    Ok(d.as_ref()
                        .unwrap()
                        .operation
                        .as_ref()
                        .unwrap()
                        .writer(2)
                        .is_err())
                })
                .await
                .unwrap()
                .receive()
                .await
                .unwrap()
        );
        let slot = lease.slot.clone();
        let (started, release) = gate(&slot);
        slot.hooks.lock().unwrap().panic = panic;
        let task =
            tokio::spawn(async move { lease.command(Command::Append(b"[]".to_vec())).await });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            owners
                .try_admit(&op, Role::Operation, guard.clone())
                .is_err()
        );
        assert!(!slot.cleanup.load(Ordering::Acquire));
        // The sole file slot is still charged while accepted work is gated.
        assert_eq!(
            std::fs::read_dir(
                std::fs::read_dir(&parent)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .path()
            )
            .unwrap()
            .count(),
            1
        );
        release.send(()).unwrap();
        wait_reap(&owners.shared, Some(slot.id)).await.unwrap();
        {
            let h = slot.hooks.lock().unwrap();
            assert!(!h.disposed.is_empty());
            assert!(
                h.executed
                    .iter()
                    .chain(&h.disposed)
                    .all(|id| *id != std::thread::current().id())
            );
        }
        let mut again = owners.try_admit(&op, Role::Operation, guard).unwrap();
        again.ready().await.unwrap();
        begin(&mut again).await;
        again
            .command(Command::Append(b"[]".to_vec()))
            .await
            .unwrap();
        again.dispose().await.unwrap();
        drop(op);
        drop(process);
        finish(owners, threads, parent).await;
    }
}
#[tokio::test]
async fn buffered_ticket_and_cleanup_complete_stay_registered_until_reaped() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    lease.ready().await.unwrap();
    begin(&mut lease).await;
    lease
        .command(Command::Append(b"[]".to_vec()))
        .await
        .unwrap();
    lease.command(Command::CloseRows).await.unwrap();
    lease.command(Command::CommitEof).await.unwrap();
    let slot = lease.slot.clone();
    let (tx, returned) = oneshot::channel();
    let (release, rx) = oneshot::channel();
    slot.hooks.lock().unwrap().returning = Some((tx, rx));
    let (ticket, buffered) = oneshot::channel();
    ticket.send(lease).unwrap();
    drop(buffered);
    returned.await.unwrap();
    assert!(slot.cleanup.load(Ordering::Acquire));
    assert!(
        owners
            .try_admit(&op, Role::Operation, guard.clone())
            .is_err()
    );
    let mut drain = Box::pin(owners.close_and_drain());
    poll_fn(|cx| {
        assert!(drain.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(drain);
    assert!(
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .entries
            .iter()
            .any(|e| e.slot.id == slot.id)
    );
    release.send(()).unwrap();
    owners.close_and_drain().await.unwrap();
    assert!(
        owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .entries
            .is_empty()
    );
    drop(op);
    drop(process);
    finish(owners, threads, parent).await;
}
#[tokio::test]
async fn concurrent_disposal_waiters_survive_reaping_and_waiter_cancellation() {
    let mut outcomes = Vec::new();
    for cancel_latest in [false, true] {
        let (owners, threads, parent, guard) = fixture();
        let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
        let mut first = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        let mut second = owners.try_admit(&op, Role::Dependency, guard).unwrap();
        first.ready().await.unwrap();
        second.ready().await.unwrap();
        let (first_returning, first_at_return) = oneshot::channel();
        let (first_release, first_gate) = oneshot::channel();
        first.slot.hooks.lock().unwrap().returning = Some((first_returning, first_gate));
        let (second_returning, second_at_return) = oneshot::channel();
        let (second_release, second_gate) = oneshot::channel();
        second.slot.hooks.lock().unwrap().returning = Some((second_returning, second_gate));

        let (mut first_task, first_polled) = disposal_waiter(first);
        first_polled.await.unwrap();
        let (second_task, second_polled) = disposal_waiter(second);
        second_polled.await.unwrap();
        first_at_return.await.unwrap();
        second_at_return.await.unwrap();
        // The second independently scheduled task polled the handles last. Both
        // supervisors are cleanup-complete but cannot terminate before release.
        let mut second_task = Some(second_task);
        if cancel_latest {
            let task = second_task.take().unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        first_release.send(()).unwrap();
        let first_result =
            tokio::time::timeout(std::time::Duration::from_secs(2), &mut first_task).await;
        let first_completed = matches!(first_result, Ok(Ok(Ok(()))));
        if first_result.is_err() {
            first_task.abort();
            let _ = first_task.await;
        }
        // First disposal must finish while the second supervisor is still
        // gated; completing every task at once would mask a missed wakeup.
        second_release.send(()).unwrap();
        let second_completed = if let Some(mut task) = second_task {
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await;
            if result.is_err() {
                task.abort();
                let _ = task.await;
            }
            matches!(result, Ok(Ok(Ok(()))))
        } else {
            true
        };
        drop(op);
        drop(process);
        finish(owners, threads, parent).await;
        outcomes.push((first_completed, second_completed));
    }
    assert_eq!(outcomes, vec![(true, true), (true, true)]);
}

fn disposal_waiter(lease: PayloadLease) -> (JoinHandle<Result<()>>, oneshot::Receiver<()>) {
    let (polled, observed) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut dispose = Box::pin(lease.dispose());
        let mut polled = Some(polled);
        poll_fn(|cx| {
            let result = dispose.as_mut().poll(cx);
            if result.is_pending()
                && let Some(polled) = polled.take()
            {
                let _ = polled.send(());
            }
            result
        })
        .await
    });
    (task, observed)
}

#[tokio::test]
async fn early_receive_errors_reap_before_returning() {
    let mut outcomes = Vec::new();
    for initialization_fails in [true, false] {
        let (owners, threads, parent, guard) = fixture();
        let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
        let lease = owners
            .try_admit_until(&op, Role::Operation, guard.clone(), Some(Instant::now()))
            .unwrap();
        let slot = lease.slot.clone();
        slot.hooks.lock().unwrap().panic = initialization_fails;
        // Consume the supervisor result without removing its registered entry.
        // Readiness is buffered: either initialization failed, or it succeeded
        // before the expired supervisor closed the receive channel.
        poll_fn(|cx| {
            let mut state = owners.shared.state.lock().unwrap();
            let registry = &mut state.payload;
            let entry = registry
                .entries
                .iter_mut()
                .find(|entry| entry.slot.id == slot.id)
                .unwrap();
            match Pin::new(&mut entry.handle).poll(cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    entry.terminated = true;
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        assert_eq!(owners.shared.operation_transport.available_permits(), 0);
        let (writer, reader) = tokio::io::duplex(64);
        drop(writer);
        let result = crate::node::payload_stream::receive_request(
            crate::node::frame::FrameReader::new(Box::new(reader)),
            lease,
        )
        .await;
        let reaped = !owners
            .shared
            .state
            .lock()
            .unwrap()
            .payload
            .entries
            .iter()
            .any(|entry| entry.slot.id == slot.id);
        outcomes.push((
            result.is_err(),
            reaped,
            owners.shared.operation_transport.available_permits(),
        ));
        drop(result);
        drop(op);
        drop(process);
        finish(owners, threads, parent).await;
    }
    assert_eq!(outcomes, vec![(true, true, 1), (true, true, 1)]);
}

#[tokio::test]
async fn take_rows_and_reap_before_admission() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut lease = owners
        .try_admit(&op, Role::Dependency, guard.clone())
        .unwrap();
    lease.ready().await.unwrap();
    begin(&mut lease).await;
    lease
        .command(Command::Append(b"[]".to_vec()))
        .await
        .unwrap();
    lease.command(Command::CloseRows).await.unwrap();
    lease.command(Command::CommitEof).await.unwrap();
    assert!(
        matches!(lease.command(Command::TakeRows).await.unwrap(), Reply::Rows(rows) if rows.is_empty())
    );
    let slot = lease.slot.clone();
    drop(lease);
    // Poll the original handle by borrowing it, without reaping. Admission must reclaim it.
    poll_fn(|cx| {
        let mut state = owners.shared.state.lock().unwrap();
        let r = &mut state.payload;
        let e = r.entries.iter_mut().find(|e| e.slot.id == slot.id).unwrap();
        if e.handle.is_finished() {
            Poll::Ready(())
        } else {
            match Pin::new(&mut e.handle).poll(cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    e.terminated = true;
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        }
    })
    .await;
    let mut again = owners.try_admit(&op, Role::Dependency, guard).unwrap();
    again.ready().await.unwrap();
    again.dispose().await.unwrap();
    drop(op);
    drop(process);
    finish(owners, threads, parent).await;
}
#[tokio::test]
async fn initialization_failure_and_rejection_have_no_async_anchor_drop() {
    let (owners, threads, parent, _guard) = fixture();
    let mut process = owners.try_process(parent.clone(), limits()).unwrap();
    process.slot.hooks.lock().unwrap().fail_create = true;
    assert_eq!(process.ready().await.unwrap_err(), OwnerError::Invalid);
    let slot = process.slot.clone();
    process.dispose().await.unwrap();
    assert_eq!(slot.hooks.lock().unwrap().created.len(), 1);
    finish(owners, threads, parent).await;

    let (owners, threads, parent, _) = fixture();
    threads.shutdown().await.unwrap();
    let mut process = owners.try_process(parent.clone(), limits()).unwrap();
    assert_eq!(process.ready().await.unwrap_err(), OwnerError::FailedDrain);
    assert_eq!(owners.close_and_drain().await, Err(OwnerError::FailedDrain));
    assert!(process.slot.hooks.lock().unwrap().created.is_empty());
    quarantine_teardown(&owners).await;
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    std::fs::remove_dir(parent).unwrap();
}
// Explicit test-only recovery of retained cells, never a production cleanup fallback.
pub(super) async fn quarantine_teardown(owners: &PayloadOwners) {
    let entries = std::mem::take(&mut owners.shared.state.lock().unwrap().payload.entries);
    let mut handles = Vec::new();
    let mut slots = Vec::new();
    let mut guards = Vec::new();
    for entry in entries {
        handles.push((entry.handle, entry.terminated));
        slots.push(entry.slot);
        guards.push(entry.guards);
    }
    std::thread::spawn(move || {
        for g in guards {
            drop(ManuallyDrop::into_inner(g));
        }
    })
    .join()
    .unwrap();
    for (h, terminated) in handles {
        if !terminated {
            h.await.unwrap();
        }
    }
    std::thread::spawn(move || {
        for slot in slots {
            slot.discard();
        }
    })
    .join()
    .unwrap();
}
#[tokio::test]
async fn pending_submit_rejected_by_real_stop_retains_disk_and_guards() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    lease.ready().await.unwrap();
    begin(&mut lease).await;
    let slot = lease.slot.clone();
    let (started, running) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let running_job = owners
        .shared
        .pool
        .submit(0, move |_, _| {
            let _ = started.send(());
            let _ = gate.recv();
            Ok(())
        })
        .await
        .unwrap();
    running.await.unwrap();
    let queued = owners.shared.pool.submit(0, |_, _| Ok(())).await.unwrap();
    let (pending, observed) = oneshot::channel();
    slot.hooks.lock().unwrap().submit_pending = Some(pending);
    let task = tokio::spawn(async move { lease.command(Command::Append(b"[]".to_vec())).await });
    observed.await.unwrap();
    let stopped = threads.shutdown();
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap_err(), OwnerError::FailedDrain);
    running_job.receive().await.unwrap();
    queued.receive().await.unwrap();
    stopped.await.unwrap();
    assert_eq!(owners.close_and_drain().await, Err(OwnerError::FailedDrain));
    assert!(slot.hooks.lock().unwrap().disposed.is_empty());
    assert_eq!(owners.shared.operation_transport.available_permits(), 0);
    assert!(owners.try_admit(&op, Role::Operation, guard).is_err());
    quarantine_teardown(&owners).await;
    drop(op);
    drop(process);
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    std::fs::remove_dir(parent).unwrap();
}

#[tokio::test]
async fn abandonment_is_sticky_before_eof_and_deadlines_do_not_release_capacity() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    lease.ready().await.unwrap();
    begin(&mut lease).await;
    lease
        .command(Command::Append(b"[]".to_vec()))
        .await
        .unwrap();
    lease.command(Command::CloseRows).await.unwrap();
    drop(lease.command(Command::CommitEof));
    assert_eq!(
        lease.command(Command::CommitEof).await.unwrap_err(),
        OwnerError::Cancelled
    );
    lease.dispose().await.unwrap();
    let mut expired = owners
        .try_admit_until(&op, Role::Operation, guard, Some(Instant::now()))
        .unwrap();
    expired.ready().await.unwrap();
    assert_eq!(
        expired.command(Command::CommitEof).await.unwrap_err(),
        OwnerError::Cancelled
    );
    expired.dispose().await.unwrap();
    drop(op);
    drop(process);
    finish(owners, threads, parent).await;
}

pub(in crate::node) fn receive_gates(
    lease: &PayloadLease,
) -> (
    oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (tx, started) = oneshot::channel();
    let (release, rx) = std::sync::mpsc::channel();
    let (done, disposed) = oneshot::channel();
    let (finish, wait) = oneshot::channel();
    let mut hooks = lease.slot.hooks.lock().unwrap();
    hooks.append_gate = Some((tx, rx));
    hooks.returning = Some((done, wait));
    (started, release, disposed, finish)
}
pub(super) fn before_append(slot: &Slot) {
    let gate = slot.hooks.lock().unwrap().append_gate.take();
    if let Some((started, release)) = gate {
        let _ = started.send(());
        let _ = release.recv();
    }
}

pub(in crate::node) async fn quota_refused(
    owners: &PayloadOwners,
    operation: &PayloadLease,
) -> bool {
    let slot = operation.slot.clone();
    owners
        .shared
        .pool
        .submit(1, move |_, _| {
            let disk = slot.disk.lock().unwrap();
            Ok(disk
                .as_ref()
                .unwrap()
                .operation
                .as_ref()
                .unwrap()
                .writer(1)
                .is_err())
        })
        .await
        .unwrap()
        .receive()
        .await
        .unwrap()
}

pub(in crate::node) fn send_gates(
    lease: &PayloadLease,
    gate_read: bool,
    count: Arc<std::sync::atomic::AtomicUsize>,
) -> (
    oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (started, release, disposed, finish) = receive_gates(lease);
    let mut hooks = lease.slot.hooks.lock().unwrap();
    hooks.read_count = Some(count);
    if gate_read {
        hooks.read_gate = hooks.append_gate.take();
    } else {
        hooks.append_gate.take();
    }
    (started, release, disposed, finish)
}
pub(super) fn before_read(slot: &Slot) {
    let gate = {
        let mut hooks = slot.hooks.lock().unwrap();
        if let Some(count) = &hooks.read_count {
            count.fetch_add(1, Ordering::SeqCst);
        }
        hooks.read_gate.take()
    };
    if let Some((started, release)) = gate {
        let _ = started.send(());
        let _ = release.recv();
    }
}

pub(in crate::node) fn observe_part_open_quota(lease: &PayloadLease) -> Arc<Mutex<Vec<bool>>> {
    let observations = Arc::new(Mutex::new(Vec::new()));
    lease.slot.hooks.lock().unwrap().part_open_quota = Some(observations.clone());
    observations
}

pub(super) fn before_part_open(slot: &Slot, operation: &SpoolOperation) {
    let observations = slot.hooks.lock().unwrap().part_open_quota.clone();
    if let Some(observations) = observations {
        // The temporary writer and any final file owner are destroyed on this worker.
        let refused = operation.writer(1).is_err();
        observations.lock().unwrap().push(refused);
    }
}

pub(super) fn installation_gate(slot: &Slot, discard: bool) {
    let gate = {
        let mut hooks = slot.hooks.lock().unwrap();
        if discard {
            hooks.install_discard.take()
        } else {
            hooks.install_command.take()
        }
    };
    if let Some((at_gate, release)) = gate {
        let _ = at_gate.send(());
        let _ = release.recv_timeout(std::time::Duration::from_secs(10));
    }
}
