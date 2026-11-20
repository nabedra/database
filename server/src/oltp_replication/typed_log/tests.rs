use super::*;
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::{CanonicalJson, Change, MvccKey, PreparedWrite},
        pool::{OltpPool, PoolIdentity, ScratchLease, SharedResourceConfig, WorkerSharedResources},
    },
    oltp_replication::{
        entry::CommittedLeaderId,
        owned::OwnedPayload,
        types::{GroupIdentity, GroupKind, Text},
    },
    protocol::Key,
};
use openraft::type_config::TypeConfigExt;
use openraft::vote::RaftLeaderId;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Command(PreparedWrite);
impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fixed.write")
    }
}
impl OwnedPayload for Command {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.0.heap_bytes()
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.0.attach_fields(owner)
    }
    fn fields_attached(&self) -> bool {
        self.0.fields_attached()
    }
}
openraft::declare_raft_types!(TestConfig:D=OwnedCommand<Command>,R=(),NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<Command>);
impl TypedConfig for TestConfig {
    type Command = Command;
    type Reply = ();
    fn accepts(kind: &GroupKind) -> bool {
        matches!(kind, GroupKind::Catalog)
    }
}

#[derive(Serialize, Deserialize)]
struct MetadataCommand {
    value: u64,
}
impl std::fmt::Display for MetadataCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("control.metadata")
    }
}
impl OwnedPayload for MetadataCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(0)
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
openraft::declare_raft_types!(ControlTestConfig:D=OwnedCommand<MetadataCommand>,R=(),NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<MetadataCommand>);
impl TypedConfig for ControlTestConfig {
    type Command = MetadataCommand;
    type Reply = ();
    fn accepts(kind: &GroupKind) -> bool {
        matches!(kind, GroupKind::Control)
    }
}
struct Dir(std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    log: TypedLogStore<TestConfig>,
    _pool: OltpPool,
    _dir: Dir,
}
impl Fixture {
    async fn new() -> Self {
        let dir = Dir(std::env::temp_dir().join(format!("typed-log-{}", Uuid::new_v4())));
        let shared = WorkerSharedResources::new(
            2,
            WorkerResources {
                max_threads: 4,
                ..Default::default()
            },
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(
            &dir.0,
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: Uuid::new_v4(),
                worker_id: Uuid::new_v4(),
                pool_id: Uuid::new_v4(),
            },
            shared,
        )
        .unwrap();
        let group = pool
            .group(GroupIdentity {
                cluster_id: pool.identity().cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: Uuid::new_v4(),
                kind: GroupKind::Catalog,
                group_format_version: 1,
            })
            .await
            .unwrap();
        let log = TypedLogStore::new(group, 1).await.unwrap();
        Self {
            log,
            _pool: pool,
            _dir: dir,
        }
    }
}
fn id(index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}
fn blank(index: u64) -> OwnedRaftEntry<Command> {
    OwnedRaftEntry::new(id(index), Payload::Blank)
}

/// A `FaultFs` whose `sync_data` (the journal's fdatasync) waits while a gate is
/// closed. It counts the syncs that entered and passed the gate, so a test can
/// hold a commit inside the actual journal sync and observe what waits for it.
#[derive(Clone)]
struct GatedSync {
    inner: nabedra_kv::fs::FaultFs,
    gate: Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>,
}
#[derive(Default)]
struct GateState {
    closed: bool,
    entered: usize,
    passed: usize,
}
struct GatedFile {
    inner: Arc<dyn nabedra_kv::fs::FsFile>,
    gate: Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>,
}
impl GatedSync {
    fn new() -> Self {
        Self {
            inner: nabedra_kv::fs::FaultFs::new(),
            gate: Default::default(),
        }
    }
    fn state<T>(&self, read: impl FnOnce(&mut GateState) -> T) -> T {
        let result = read(&mut self.gate.0.lock().unwrap());
        self.gate.1.notify_all();
        result
    }
}
impl nabedra_kv::fs::Fs for GatedSync {
    fn open(
        &self,
        path: &std::path::Path,
        mode: nabedra_kv::fs::OpenMode,
    ) -> io::Result<Arc<dyn nabedra_kv::fs::FsFile>> {
        Ok(Arc::new(GatedFile {
            inner: self.inner.open(path, mode)?,
            gate: self.gate.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove(&self, path: &std::path::Path) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &std::path::Path) -> io::Result<bool> {
        self.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &std::path::Path) -> io::Result<Vec<std::path::PathBuf>> {
        self.inner.list(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn lock(&self, path: &std::path::Path) -> io::Result<nabedra_kv::fs::FsLock> {
        self.inner.lock(path)
    }
}
impl nabedra_kv::fs::FsFile for GatedFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().unwrap();
        if state.closed {
            state.entered += 1;
            changed.notify_all();
            while state.closed {
                state = changed.wait(state).unwrap();
            }
        }
        drop(state);
        let result = self.inner.sync_data();
        if result.is_ok() {
            lock.lock().unwrap().passed += 1;
            changed.notify_all();
        }
        result
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
}

/// O06/O11, C07/F03: hold the actual journal sync of a typed append (a gated
/// `FaultFs` under the pool's store) and observe the typed callback. Formerly a
/// Linux-only test driven by a Python harness that gated the previous engine's WAL;
/// it now runs everywhere. While the sync is held: the callback has not fired,
/// a cancelled append keeps its owned write and order, the next append waits
/// behind it, no executor thread waits for the fsync, and an unrelated pool's
/// vote writes progress. After release both acks arrive in order, and only the
/// bytes a completed sync covered survive a crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o11_journal_sync_precedes_typed_append_ack() {
    tokio::time::timeout(Duration::from_secs(30), async {
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        };

        let dir = Dir(std::env::temp_dir().join(format!("typed-log-gate-{}", Uuid::new_v4())));
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let gated = GatedSync::new();
        gated.inner.create_dir_all_durable(&path);
        let shared = WorkerSharedResources::new(
            2,
            WorkerResources {
                max_threads: 4,
                ..Default::default()
            },
            SharedResourceConfig::default(),
        )
        .unwrap();
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        };
        let group_id = GroupIdentity {
            cluster_id: identity.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        let pool = OltpPool::open_development_on(
            &path,
            identity.clone(),
            shared.clone(),
            Arc::new(gated.clone()),
        )
        .unwrap();
        let target = pool.group(group_id.clone()).await.unwrap();
        let log = TypedLogStore::<TestConfig>::new(target, 1).await.unwrap();
        let healthy = OltpPool::open_development(
            dir.0.join("healthy"),
            PoolIdentity {
                pool_id: Uuid::new_v4(),
                ..identity.clone()
            },
            shared.clone(),
        )
        .unwrap();
        let control = healthy
            .group(GroupIdentity {
                group_id: Uuid::new_v4(),
                kind: GroupKind::Control,
                ..group_id.clone()
            })
            .await
            .unwrap();
        let mut control = TypedLogStore::<ControlTestConfig>::new(control, 1)
            .await
            .unwrap();
        let baseline = shared.scheduler().usage();
        let passed_at_close = gated.state(|gate| {
            gate.closed = true;
            gate.passed
        });

        let vote = Vote::new_committed(2, 1);
        control.save_vote(&vote).await.unwrap();
        assert_eq!(
            gated.state(|gate| gate.entered),
            0,
            "an unrelated pool's write entered the gated journal"
        );

        let (sender, mut callback) = TestConfig::oneshot();
        let first = tokio::spawn({
            let mut log = log.clone();
            async move { log.append([blank(0)], IOFlushed::signal(sender)).await }
        });
        while gated.state(|gate| gate.entered) == 0 {
            assert!(
                !first.is_finished(),
                "append finished without reaching its journal sync"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(matches!(
            callback.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let callback = Arc::new(Mutex::new(callback));
        let released = Arc::new(AtomicBool::new(false));
        let first_ack_observed = Arc::new(AtomicBool::new(false));
        *log.append_hook.lock().unwrap() = Some(Arc::new({
            let callback = callback.clone();
            let released = released.clone();
            let observed = first_ack_observed.clone();
            move || {
                assert!(released.load(Ordering::Acquire));
                assert!(matches!(callback.lock().unwrap().try_recv(), Ok(Ok(()))));
                observed.store(true, Ordering::Release);
            }
        }));
        let (sender, mut next_callback) = TestConfig::oneshot();
        let second = tokio::spawn({
            let mut log = log.clone();
            async move { log.append([blank(1)], IOFlushed::signal(sender)).await }
        });
        while log.pending.count.load(Ordering::Acquire) != 2 {
            assert!(!second.is_finished());
            tokio::task::yield_now().await;
        }
        assert!(log.writes.try_lock().is_err());
        // The batch charge stays held until the journal thread drops it.
        assert!(shared.apply_bytes_used() > 0);
        // Waiting for the journal sync holds no executor thread (NS6).
        assert_eq!(shared.scheduler().usage()[0].1, baseline[0].1);
        assert_eq!(control.read_vote().await.unwrap(), Some(vote));
        let vote = Vote::new_committed(3, 1);
        control.save_vote(&vote).await.unwrap();
        assert_eq!(control.read_vote().await.unwrap(), Some(vote));
        assert_eq!(gated.state(|gate| gate.passed), passed_at_close);
        let mut drain = Box::pin(log.drain());
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx).is_pending()))
                .await
        );
        assert!(matches!(
            callback.lock().unwrap().try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            next_callback.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        released.store(true, Ordering::Release);
        gated.state(|gate| gate.closed = false);

        next_callback.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        drain.await;
        assert!(first_ack_observed.load(Ordering::Acquire));
        assert_eq!(log.pending.count.load(Ordering::Acquire), 0);
        assert_eq!(shared.apply_bytes_used(), 0);
        assert_eq!(shared.working_decode_bytes(), 0);
        assert_eq!(shared.control_bytes_used(), 0);
        assert_eq!(shared.scheduler().usage(), baseline);
        let mut log = log;
        log.save_committed(Some(id(0))).await.unwrap();
        // A synced metadata write makes the unsynced commit hint durable too.
        log.save_vote(&Vote::new_committed(1, 1)).await.unwrap();
        drop(log);
        drop(pool);

        // Only what a completed sync covered: both acknowledged appends.
        let crashed = gated.inner.crash();
        let pool =
            OltpPool::open_development_on(&path, identity, shared, Arc::new(crashed)).unwrap();
        let group = pool.retained_group(group_id).await.unwrap();
        let mut log = TypedLogStore::<TestConfig>::new(group, 1).await.unwrap();
        let entries = log.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            [id(0), id(1)]
        );
        assert_eq!(log.read_committed().await.unwrap(), Some(id(0)));
        assert_eq!(log.get_log_state().await.unwrap().last_log_id, Some(id(1)));
    })
    .await
    .expect("typed journal sync watchdog");
}

/// O05/O11: range truncation and metadata are one native batch, while an
/// arbitrarily long retained log is consumed through a one-entry stream.
#[tokio::test]
async fn o05_o11_typed_wal_streams_beyond_collection_bound_and_atomically_truncates() {
    let mut f = Fixture::new().await;
    for index in 0..140 {
        f.log
            .append([blank(index)], IOFlushed::noop())
            .await
            .unwrap();
    }
    assert!(f.log.try_get_log_entries(..).await.is_err());
    let limited = f.log.limited_get_log_entries(0, 140).await.unwrap();
    assert_eq!(limited.len(), MAX_APPEND_ENTRIES);
    drop(limited);
    let mut stream = Box::pin(f.log.entries_stream(..).await);
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        assert_eq!(entry.unwrap().index(), count);
        count += 1;
    }
    assert_eq!(count, 140);
    drop(stream);
    f.log.save_committed(Some(id(10))).await.unwrap();
    f.log.purge(id(10)).await.unwrap();
    f.log.truncate_after(Some(id(10))).await.unwrap();
    let state = f.log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(10)));
    assert_eq!(state.last_log_id, Some(id(10)));
    assert_eq!(f.log.read_committed().await.unwrap(), Some(id(10)));
    assert!(f.log.try_get_log_entries(..).await.unwrap().is_empty());
    f.log.append([blank(11)], IOFlushed::noop()).await.unwrap();
    assert_eq!(
        f.log.get_log_state().await.unwrap().last_log_id,
        Some(id(11))
    );
    assert!(
        TypedLogStore::<TestConfig>::new(f.log.group().clone(), 2)
            .await
            .is_err()
    );
}

/// O11, NS7: each purge deletes only `(previous purge, id]`. A repeated purge
/// deletes nothing, consecutive purges leave exactly the suffix, a purge past
/// the tail empties the log, and a backwards purge is refused. Entry bodies
/// are journal values; every read resolves their locators.
#[tokio::test]
async fn o11_ns07_purge_deletes_only_the_range_after_the_previous_purge() {
    let mut f = Fixture::new().await;
    for index in 0..40 {
        f.log
            .append([blank(index)], IOFlushed::noop())
            .await
            .unwrap();
    }
    let retained = |entries: Vec<OwnedRaftEntry<Command>>| {
        entries
            .iter()
            .map(|entry| entry.index())
            .collect::<Vec<_>>()
    };
    for (upto, again) in [(5, false), (5, true), (6, false), (20, false)] {
        f.log.purge(id(upto)).await.unwrap();
        let state = f.log.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(id(upto)), "again {again}");
        assert_eq!(state.last_log_id, Some(id(39)));
        assert_eq!(
            retained(f.log.try_get_log_entries(..).await.unwrap()),
            (upto + 1..40).collect::<Vec<_>>()
        );
    }
    assert!(f.log.purge(id(19)).await.is_err(), "purge moves backwards");
    f.log.purge(id(60)).await.unwrap();
    let state = f.log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(60)));
    assert_eq!(state.last_log_id, Some(id(60)));
    assert!(f.log.try_get_log_entries(..).await.unwrap().is_empty());
    f.log.append([blank(61)], IOFlushed::noop()).await.unwrap();
    assert_eq!(retained(f.log.try_get_log_entries(..).await.unwrap()), [61]);
}

/// O05/M01/F03: actual14MiB fixed bytes survive native log decode and retain
/// their dynamic owner after both the reader's apply arena and entry are gone.
#[tokio::test]
async fn o05_m01_wide_entry_retains_extracted_payload_without_reply_or_apply_escape() {
    let mut f = Fixture::new().await;
    let shared = f.log.group().resources().clone();
    let fixed = CanonicalJson::from_serializable(&shared, &"x".repeat(7 << 20), 8 << 20).unwrap();
    let command = OwnedCommand::new_owned(
        Command(PreparedWrite {
            key: MvccKey::Row {
                table_id: Uuid::new_v4(),
                primary_key: Key::Utf8("k".repeat(7 << 20)),
            },
            expected_version: None,
            change: Change::Put(fixed),
        }),
        &shared,
        ByteClass::Data,
    )
    .unwrap();
    f.log
        .append(
            [OwnedRaftEntry::new(id(0), Payload::Normal(command))],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
    let decoded = f.log.try_get_log_entries(0..1).await.unwrap();
    assert_eq!(decoded.len(), 1);
    let Payload::Normal(command) = decoded[0].payload() else {
        panic!("normal")
    };
    let Change::Put(field) = &command.payload().unwrap().0.change else {
        panic!("put")
    };
    let field = field.clone();
    assert_eq!(shared.apply_bytes_used(), 0);
    assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
    let retained = shared.working_decode_bytes();
    assert!(retained > 14 << 20);
    drop(decoded);
    assert_eq!(shared.working_decode_bytes(), retained);
    assert_eq!(field.as_str().len(), (7 << 20) + 2);
    drop(field);
    assert_eq!(shared.working_decode_bytes(), 0);
}

/// F03/O11: cancellation cannot abandon a live native write or its callback.
/// A second append and replica drain stay behind the first actual sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f03_cancelled_append_keeps_callback_order_and_drain_until_native_settles() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let (entered, observed) = std::sync::mpsc::channel();
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        let release = Release(gate.clone());
        *f.log.append_hook.lock().unwrap() = Some(Arc::new(move || {
            entered.send(()).unwrap();
            let mut open = gate.0.lock().unwrap();
            while !*open {
                open = gate.1.wait(open).unwrap();
            }
        }));
        let (sent, mut done) = TestConfig::oneshot();
        let mut writer = f.log.clone();
        let first =
            tokio::spawn(async move { writer.append([blank(0)], IOFlushed::signal(sent)).await });
        while observed.try_recv().is_err() {
            tokio::task::yield_now().await;
        }
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut done)
                .await
                .is_err()
        );
        assert!(f.log.writes.try_lock().is_err());
        let mut second = f.log.clone();
        let second =
            tokio::spawn(async move { second.append([blank(1)], IOFlushed::noop()).await });
        let log = f.log.clone();
        let drain = tokio::spawn(async move { log.drain().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        assert!(!drain.is_finished());
        assert!(f.log.group().resources().apply_bytes_used() > 0);
        drop(release);
        done.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        drain.await.unwrap();
        let mut log = f.log.clone();
        assert_eq!(log.get_log_state().await.unwrap().last_log_id, Some(id(1)));
        assert_eq!(
            log.pending.count.load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert_eq!(log.group().resources().apply_bytes_used(), 0);
        assert_eq!(log.group().resources().working_decode_bytes(), 0);
    })
    .await
    .expect("typed append cancellation watchdog");
}

/// C07/F03: pending append ownership starts before a native lease exists.
/// All Query slots can remain held while Data-group vote metadata progresses.
#[tokio::test]
async fn c07_f03_cancelled_append_waiting_for_native_slot_stays_tracked_while_votes_progress() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let shared = f.log.group().resources();
        let query = shared.prepare(ExecutionClass::Query).await.unwrap();
        let (sent, mut callback) = TestConfig::oneshot();
        let mut append = f.log.clone();
        let append =
            tokio::spawn(async move { append.append([blank(0)], IOFlushed::signal(sent)).await });
        while f
            .log
            .pending
            .count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            tokio::task::yield_now().await;
        }
        append.abort();
        assert!(append.await.unwrap_err().is_cancelled());
        let mut vote = f.log.clone();
        vote.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
        assert_eq!(
            vote.read_vote().await.unwrap(),
            Some(Vote::new_committed(3, 1))
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut callback)
                .await
                .is_err()
        );
        let log = f.log.clone();
        let drain = tokio::spawn(async move { log.drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        assert_eq!(shared.scheduler().usage()[0].1, 0);
        drop(query);
        callback.await.unwrap().unwrap();
        drain.await.unwrap();
        let mut log = f.log.clone();
        assert_eq!(log.get_log_state().await.unwrap().last_log_id, Some(id(0)));
    })
    .await
    .expect("native-wait ownership watchdog");
}

/// C07/M02: actual Control entry ownership and WAL recovery do not depend on
/// free Data wire bytes, Data working memory or a Query native slot.
#[tokio::test]
async fn c07_m02_control_append_and_read_progress_with_all_data_memory_and_query_capacity_held() {
    let f = Fixture::new().await;
    let shared = f.log.group().resources().clone();
    let mut identity = f.log.group().identity().clone();
    identity.group_id = Uuid::new_v4();
    identity.kind = GroupKind::Control;
    let mut control =
        TypedLogStore::<ControlTestConfig>::new(f._pool.group(identity).await.unwrap(), 1)
            .await
            .unwrap();
    let limits = WorkerResources {
        max_threads: 4,
        ..Default::default()
    };
    let remaining = limits.working_bytes() - shared.scheduler().usage()[0].2;
    let working = shared
        .scheduler()
        .charge_memory(shared.scheduler_node_id(), remaining)
        .unwrap();
    assert!(shared.reserve_scratch(ByteClass::Data, 1).is_err());
    let data = shared
        .allocate_bytes(
            ByteClass::Data,
            shared.config().control_bytes - shared.config().progress_bytes,
        )
        .unwrap();
    let query = shared.prepare(ExecutionClass::Query).await.unwrap();
    let command =
        OwnedCommand::new_owned(MetadataCommand { value: 42 }, &shared, ByteClass::Progress)
            .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        control.append(
            [OwnedRaftEntry::new(id(0), Payload::Normal(command))],
            IOFlushed::noop(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let read = tokio::time::timeout(Duration::from_secs(5), control.try_get_log_entries(0..1))
        .await
        .unwrap()
        .unwrap();
    let Payload::Normal(command) = read[0].payload() else {
        panic!("normal")
    };
    assert_eq!(command.payload().unwrap().value, 42);
    assert_eq!(shared.working_decode_bytes(), 0);
    drop(read);
    drop(query);
    drop(data);
    drop(working);
    assert_eq!(shared.buffer_bytes_used(ByteClass::Progress), 0);
}

/// O01/C07: transient Progress wire exhaustion must not become a fatal Raft
/// WriteLogs error after the proposal already owns its command. The live host
/// uses bounded entry residency and acknowledges only the actual sync.
#[tokio::test]
async fn o01_live_append_uses_residency_with_progress_wire_completely_full() {
    let f = Fixture::new().await;
    let shared = f.log.group().resources().clone();
    let mut identity = f.log.group().identity().clone();
    identity.group_id = Uuid::new_v4();
    identity.kind = GroupKind::Control;
    let registry = crate::oltp_replication::multiplex::EndpointRegistry::new(shared.clone());
    let group = registry.reserve(identity.clone()).unwrap();
    let resident = group.activate_replica(1, &shared).unwrap();
    let mut log =
        TypedLogStore::<ControlTestConfig>::new(f._pool.group(identity).await.unwrap(), 1)
            .await
            .unwrap()
            .with_resident(resident.clone())
            .unwrap();
    let command =
        OwnedCommand::new_owned(MetadataCommand { value: 73 }, &shared, ByteClass::Progress)
            .unwrap();
    let remaining = shared.config().progress_bytes - shared.buffer_bytes_used(ByteClass::Progress);
    let full = shared
        .allocate_bytes(ByteClass::Progress, remaining)
        .unwrap();
    assert!(shared.reserve_scratch(ByteClass::Progress, 1).is_err());
    let (send, done) = ControlTestConfig::oneshot();
    tokio::time::timeout(
        Duration::from_secs(5),
        log.append(
            [OwnedRaftEntry::new(id(0), Payload::Normal(command))],
            IOFlushed::signal(send),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(log.get_log_state().await.unwrap().last_log_id, Some(id(0)));
    log.drain().await;
    assert_eq!(resident.log_metadata_used(), 0);
    drop(full);
    let read = log.try_get_log_entries(0..1).await.unwrap();
    let Payload::Normal(command) = read[0].payload() else {
        panic!("normal")
    };
    assert_eq!(command.payload().unwrap().value, 73);
}
