use super::tests::{controls, fixture, limits};
use super::*;
use crate::test_storage as backend;
use crate::{
    node::job::{JobResult, Task},
    storage::{Namespace, ObjectStore, Storage, WriteCondition},
};

#[tokio::test]
async fn captured_count_ignores_limit_and_writes_nothing() {
    let (owners, threads, parent, guard) = fixture();
    let (partition, partition_threads) = WorkerPool::start(
        std::num::NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let (mut job, _) = controls();
    let store = Arc::new(Storage::new(
        backend::Fake::default(),
        Namespace::new("test/").unwrap(),
    ));
    let rows = vec![[("id".into(), crate::protocol::Scalar::Int64(1))].into()];
    let bytes = crate::snapshot::encode(&job.table, &rows).unwrap();
    let Task::Get { file, .. } = &job.task else {
        unreachable!()
    };
    let file = file.clone();
    let crate::node::job::FileInput::Legacy { object, .. } = &file else {
        unreachable!()
    };
    store
        .backend()
        .put(&format!("test/{object}"), bytes, WriteCondition::Create)
        .await
        .unwrap();
    let puts = store.backend().state.lock().unwrap().puts;
    job.task = Task::Count {
        file,
        query: serde_json::from_value(
            serde_json::json!({"select":null,"filter":null,"order_by":null,"limit":0}),
        )
        .unwrap(),
    };
    let mut process = owners.try_process(parent.clone(), limits()).unwrap();
    process.ready().await.unwrap();
    let mut op = owners
        .try_bound_operation(&process, limits(), guard.clone(), job.operation_id)
        .unwrap();
    op.ready().await.unwrap();
    let execution = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    let mut lease = owners
        .try_local(
            &op,
            Role::Operation,
            guard,
            None,
            job.cluster_id,
            job,
            store.clone(),
            partition,
            execution,
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        )
        .unwrap();
    let Reply::Accepted(result) = lease.ready().await.unwrap() else {
        panic!("expected typed completion")
    };
    assert!(matches!(
        result.result,
        JobResult::Count {
            matched_rows: 1,
            source_rows: 1,
            ..
        }
    ));
    assert_eq!(store.backend().state.lock().unwrap().puts, puts);
    lease.dispose().await.unwrap();
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    owners.close_and_drain().await.unwrap();
    partition_threads.shutdown().await.unwrap();
    threads.shutdown().await.unwrap();
    std::fs::remove_dir(parent).unwrap();
}
pub(super) fn before_deposit(slot: &Slot) {
    let (gate, panic) = {
        let mut h = slot.hooks.lock().unwrap();
        (h.local_gate.take(), h.local_panic)
    };
    if let Some((started, release)) = gate {
        let _ = started.send(());
        let _ = release.recv();
    }
    assert!(!panic, "injected local candidate panic");
}

struct LocalFixture {
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    partition: WorkerPool,
    partition_threads: crate::worker_pool::WorkerThreads,
    process: PayloadLease,
    operation: PayloadLease,
    guard: Arc<OwnedSemaphorePermit>,
    store: Arc<Storage<backend::Fake>>,
    job: Box<JobEnvelope>,
    parent: PathBuf,
    puts: usize,
    put_keys: Vec<String>,
}
impl LocalFixture {
    async fn new() -> Self {
        Self::with_files(32).await
    }
    async fn with_files(files: u64) -> Self {
        let (owners, threads, parent, guard) = fixture();
        let (partition, partition_threads) = WorkerPool::start(
            std::num::NonZeroUsize::new(1).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let (mut job, _) = controls();
        job.table
            .schema
            .insert("v".into(), crate::protocol::ColumnType::Bool);
        let store = Arc::new(Storage::new(
            backend::Fake::default(),
            Namespace::new("test/").unwrap(),
        ));
        let rows = [3, 1, 2]
            .into_iter()
            .map(|id| {
                [
                    ("id".into(), crate::protocol::Scalar::Int64(id)),
                    ("v".into(), crate::protocol::Scalar::Bool(true)),
                ]
                .into()
            })
            .collect::<Vec<crate::protocol::Row>>();
        let bytes = crate::snapshot::encode(&job.table, &rows).unwrap();
        let Task::Get {
            file: crate::node::job::FileInput::Legacy { object, row_count },
            ..
        } = &mut job.task
        else {
            unreachable!()
        };
        *row_count = 3;
        store
            .backend()
            .put(&format!("test/{object}"), bytes, WriteCondition::Create)
            .await
            .unwrap();
        // A newer catalog is deliberately incompatible and must never be consulted.
        store
            .backend()
            .put(
                "test/catalog.json",
                b"newer incompatible catalog".to_vec(),
                WriteCondition::Create,
            )
            .await
            .unwrap();
        let singleton = crate::snapshot::encode(&job.table, &rows[..1]).unwrap();
        let single_key =
            crate::storage::ObjectKey::snapshot(job.table.id, uuid::Uuid::from_u128(8));
        store
            .backend()
            .put(
                &format!("test/{}", single_key.as_str()),
                singleton,
                WriteCondition::Create,
            )
            .await
            .unwrap();
        let (puts, put_keys) = {
            let state = store.backend().state.lock().unwrap();
            (state.puts, state.put_keys.clone())
        };
        let quota = SpoolLimits::new(1024 * 1024, 32).unwrap();
        let mut process = owners.try_process(parent.clone(), quota).unwrap();
        process.ready().await.unwrap();
        let mut operation = owners
            .try_bound_operation(
                &process,
                SpoolLimits::new(1024 * 1024, files).unwrap(),
                guard.clone(),
                job.operation_id,
            )
            .unwrap();
        operation.ready().await.unwrap();
        Self {
            owners,
            threads,
            partition,
            partition_threads,
            process,
            operation,
            guard,
            store,
            job,
            parent,
            puts,
            put_keys,
        }
    }
    fn start(&self, job: Box<JobEnvelope>) -> Result<PayloadLease> {
        self.owners.try_local(
            &self.operation,
            Role::Operation,
            self.guard.clone(),
            None,
            self.job.cluster_id,
            job,
            self.store.clone(),
            self.partition.clone(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        )
    }
    async fn run(&self, job: Box<JobEnvelope>) -> Result<(Box<ResultEnvelope>, PayloadLease)> {
        self.start(job)?.local_complete().await
    }
    async fn finish(self) {
        let operation = self.operation.dispose().await;
        let process = self.process.dispose().await;
        let drained = self.owners.close_and_drain().await;
        let partition = self.partition_threads.shutdown().await;
        let coordinator = self.threads.shutdown().await;
        let removed = std::fs::remove_dir(self.parent);
        // Assertions must not bypass the remaining fixture teardown.
        assert!(operation.is_ok());
        assert!(process.is_ok());
        assert!(drained.is_ok());
        assert!(partition.is_ok());
        assert!(coordinator.is_ok());
        assert!(removed.is_ok());
        let state = self.store.backend().state.lock().unwrap();
        assert_eq!(state.puts, self.puts);
        assert_eq!(state.put_keys, self.put_keys);
    }
}
fn file(job: &JobEnvelope) -> crate::node::job::FileInput {
    let Task::Get { file, .. } = &job.task else {
        unreachable!()
    };
    file.clone()
}
fn prepare(job: &JobEnvelope, mutation: serde_json::Value, input: bool) -> Box<JobEnvelope> {
    let mut j = job.clone();
    j.task = Task::Prepare {
        file: input.then(|| file(job)),
        range: crate::storage::KeyRange {
            lower: None,
            upper: None,
        },
        mutation: serde_json::from_value(mutation).unwrap(),
        target_bytes: 1,
    };
    Box::new(j)
}
#[tokio::test]
async fn local_candidate_cancel_panic_and_buffered_drop_reap_on_workers() {
    for mode in ["cancel", "panic", "buffered"] {
        let f = LocalFixture::new().await;
        let j = prepare(
            &f.job,
            serde_json::json!({"kind":"insert","payload":{"table_id":f.job.table.id,"row":{"id":8,"v":true}}}),
            false,
        );
        let execution_capacity = Arc::new(Semaphore::new(1));
        let lifetime_capacity = Arc::new(Semaphore::new(1));
        let lifetime = Arc::new(lifetime_capacity.clone().try_acquire_owned().unwrap());
        let lease = f
            .owners
            .try_local(
                &f.operation,
                Role::Operation,
                lifetime,
                None,
                f.job.cluster_id,
                j,
                f.store.clone(),
                f.partition.clone(),
                execution_capacity.clone().try_acquire_owned().unwrap(),
                Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            )
            .unwrap();
        let slot = lease.slot.clone();
        let mut lease = Some(lease);
        let (started_tx, started) = oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let mut release = Some(release);
        let (returning_tx, returning) = oneshot::channel();
        let (reap_release, reap_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let mut h = slot.hooks.lock().unwrap();
            h.local_gate = Some((started_tx, release_rx));
            h.local_panic = mode == "panic";
            h.returning = Some((returning_tx, reap_rx));
            h.local_ready = Some(ready_tx);
        }
        let watchdog = std::time::Duration::from_secs(10);
        let observed: std::result::Result<(), &'static str> = async {
            tokio::time::timeout(watchdog, started)
                .await
                .map_err(|_| "producer did not reach its gate")?
                .map_err(|_| "producer gate closed")?;
            if f.start(f.job.clone()).is_ok()
                || execution_capacity.available_permits() != 0
                || lifetime_capacity.available_permits() != 0
            {
                return Err("production released admission or permits early");
            }
            if !tokio::time::timeout(watchdog, quota_charged(&f))
                .await
                .map_err(|_| "production quota probe stalled")??
            {
                return Err("production released scratch early");
            }
            let rejected = tokio::time::timeout(
                watchdog,
                run_command(&f.owners.shared.pool, &slot, Command::TakeRows),
            )
            .await
            .map_err(|_| "coordinator command waited on gated production")?;
            if !matches!(rejected, Err(OwnerError::Invalid)) {
                return Err("coordinator command was not rejected during production");
            }
            if mode == "cancel" {
                drop(lease.as_mut().ok_or("missing producer lease")?.ready());
            }
            release
                .take()
                .ok_or("producer gate already released")?
                .send(())
                .map_err(|_| "producer gate receiver closed")?;
            if mode == "buffered" {
                tokio::time::timeout(watchdog, ready_rx)
                    .await
                    .map_err(|_| "completion was not buffered")?
                    .map_err(|_| "completion gate closed")?;
                // The causal partition barrier proves the completion reply was buffered.
                tokio::time::timeout(watchdog, async {
                    f.partition.submit(0, |_, _| Ok(())).await?.receive().await
                })
                .await
                .map_err(|_| "partition barrier stalled")?
                .map_err(|_| "partition barrier failed")?;
            }
            // Drop the original receiver before observing disposal, including buffered results.
            drop(lease.take());
            tokio::time::timeout(watchdog, returning)
                .await
                .map_err(|_| "supervisor did not reach its return gate")?
                .map_err(|_| "supervisor return gate closed")?;
            let (executed, disposed) = {
                let h = slot.hooks.lock().unwrap();
                (h.executed.first().copied(), h.disposed.clone())
            };
            if f.start(f.job.clone()).is_ok()
                || disposed.is_empty()
                || disposed.iter().any(|id| *id == std::thread::current().id())
                || executed.is_none()
                || executed == disposed.last().copied()
                || !slot.cleanup.load(Ordering::Acquire)
                || execution_capacity.available_permits() != 1
                || lifetime_capacity.available_permits() != 0
            {
                return Err("cleanup ownership or worker placement changed before reaping");
            }
            if tokio::time::timeout(watchdog, quota_charged(&f))
                .await
                .map_err(|_| "disposed quota probe stalled")??
            {
                return Err("disposed scratch remained charged");
            }
            Ok(())
        }
        .await;
        // Every observation failure reaches both releases and the full teardown.
        if let Some(release) = release.take() {
            let _ = release.send(());
        }
        drop(lease.take());
        let _ = reap_release.send(());
        let reaped = wait_reap(&f.owners.shared, Some(slot.id)).await;
        let lifetime_after_reap = lifetime_capacity.available_permits();
        let recovered = match tokio::time::timeout(watchdog, f.run(f.job.clone())).await {
            Ok(Ok((r, lease))) => {
                let found = matches!(r.result, JobResult::Get { found: true, .. });
                let disposed = lease.dispose().await;
                found && disposed.is_ok()
            }
            _ => false,
        };
        f.finish().await;
        assert!(observed.is_ok(), "{mode}: {observed:?}");
        assert!(reaped.is_ok(), "{mode}: {reaped:?}");
        assert_eq!(lifetime_after_reap, 1);
        assert!(recovered, "{mode}: admission did not recover");
    }
}

#[tokio::test]
async fn local_partition_rejection_disposes_registered_empty_cell() {
    let f = LocalFixture::new().await;
    f.partition_threads.shutdown().await.unwrap();
    let lease = f
        .owners
        .try_local(
            &f.operation,
            Role::Operation,
            f.guard.clone(),
            None,
            f.job.cluster_id,
            f.job.clone(),
            f.store.clone(),
            f.partition.clone(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        )
        .unwrap();
    let slot = lease.slot.clone();
    assert!(lease.local_complete().await.is_err());
    assert!(slot.cleanup.load(Ordering::Acquire));
    assert_eq!(f.owners.shared.operation_transport.available_permits(), 1);
    assert_eq!(f.store.backend().state.lock().unwrap().gets, 0);
    assert_eq!(f.store.backend().state.lock().unwrap().puts, f.puts);
    f.operation.dispose().await.unwrap();
    f.process.dispose().await.unwrap();
    f.owners.close_and_drain().await.unwrap();
    f.threads.shutdown().await.unwrap();
    std::fs::remove_dir(f.parent).unwrap();
}

#[path = "../../engine/job/tests.rs"]
mod handler_tests;

async fn quota_charged(f: &LocalFixture) -> std::result::Result<bool, &'static str> {
    let operation = f.operation.slot.clone();
    f.owners
        .shared
        .pool
        .submit(0, move |_, _| {
            let disk = operation.disk.lock().unwrap();
            let account = disk.as_ref().unwrap().operation.as_ref().unwrap();
            let mut writer = account.writer(1024 * 1024).unwrap();
            Ok(writer.append(&vec![0; 1024 * 1024]).is_err())
        })
        .await
        .map_err(|_| "quota probe submission failed")?
        .receive()
        .await
        .map_err(|_| "quota probe failed")
}

#[path = "composition_tests.rs"]
mod composition_tests;
