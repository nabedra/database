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
        let dir = Dir(std::env::temp_dir().join(format!("typed-log-{}", nabedra_rt::ids::uuid_v4())));
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
                cluster_id: nabedra_rt::ids::uuid_v4(),
                worker_id: nabedra_rt::ids::uuid_v4(),
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            shared,
        )
        .unwrap();
        let group = pool
            .group(GroupIdentity {
                cluster_id: pool.identity().cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: nabedra_rt::ids::uuid_v4(),
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
pub(crate) struct GatedSync {
    inner: nabedra_kv::fs::FaultFs,
    gate: Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>,
}
#[must_use = "dropping the guard releases journal syncs"]
pub(crate) struct HeldSync<'a>(&'a GatedSync);
impl Drop for HeldSync<'_> {
    fn drop(&mut self) {
        self.0.state(|gate| gate.closed = false);
    }
}
#[derive(Default)]
struct GateState {
    closed: bool,
    entered: usize,
    passed: usize,
    fail_after_sync: bool,
}
struct GatedFile {
    inner: Arc<dyn nabedra_kv::fs::FsFile>,
    gate: Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>,
}
impl GatedSync {
    pub(crate) fn new() -> Self {
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
    /// The same gate over another `FaultFs` (a restarted machine's disk).
    pub(crate) fn over(&self, inner: nabedra_kv::fs::FaultFs) -> Self {
        Self {
            inner,
            gate: self.gate.clone(),
        }
    }
    /// Holds later journal syncs until the returned guard drops.
    pub(crate) fn hold(&self) -> HeldSync<'_> {
        self.state(|gate| {
            assert!(!gate.closed, "journal sync gate is already held");
            gate.closed = true;
            gate.entered = 0;
        });
        HeldSync(self)
    }
    /// The next successful native sync persists its bytes but reports EIO.
    pub(crate) fn fail_after_sync(&self) {
        self.state(|gate| gate.fail_after_sync = true);
    }
    /// Syncs that reached the closed gate since [`Self::hold`].
    pub(crate) fn entered(&self) -> usize {
        self.state(|gate| gate.entered)
    }
    async fn wait_for_sync(&self) {
        nabedra_rt::time::timeout(Duration::from_secs(10), async {
            while self.entered() == 0 {
                nabedra_rt::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("pending append did not reach the journal sync gate");
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
            (state, _) = changed
                .wait_timeout_while(state, Duration::from_secs(60), |state| state.closed)
                .unwrap();
            if state.closed {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "test journal sync gate was not released",
                ));
            }
        }
        let fail_after_sync = std::mem::take(&mut state.fail_after_sync);
        drop(state);
        let result = self.inner.sync_data();
        if result.is_ok() {
            lock.lock().unwrap().passed += 1;
            changed.notify_all();
        }
        if fail_after_sync && result.is_ok() {
            return Err(io::Error::from_raw_os_error(libc::EIO));
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
/// the append already returned and its entry is readable (PERF2d), the next
/// append's write waits behind it, no executor thread waits for the fsync, and
/// an unrelated pool's vote writes progress. After release both acks arrive in
/// order, and only the bytes a completed sync covered survive a crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o11_journal_sync_precedes_typed_append_ack() {
    nabedra_rt::time::timeout(Duration::from_secs(30), async {
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        };

        let dir = Dir(std::env::temp_dir().join(format!("typed-log-gate-{}", nabedra_rt::ids::uuid_v4())));
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
            cluster_id: nabedra_rt::ids::uuid_v4(),
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let group_id = GroupIdentity {
            cluster_id: identity.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
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
        let mut log = TypedLogStore::<TestConfig>::new(target, 1).await.unwrap();
        let healthy = OltpPool::open_development(
            dir.0.join("healthy"),
            PoolIdentity {
                pool_id: nabedra_rt::ids::uuid_v4(),
                ..identity.clone()
            },
            shared.clone(),
        )
        .unwrap();
        let control = healthy
            .group(GroupIdentity {
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Control,
                ..group_id.clone()
            })
            .await
            .unwrap();
        let mut control = TypedLogStore::<ControlTestConfig>::new(control, 1)
            .await
            .unwrap();
        let baseline = shared.scheduler().usage();
        let held = gated.hold();
        let passed_at_close = gated.state(|gate| gate.passed);

        let vote = Vote::new_committed(2, 1);
        control.save_vote(&vote).await.unwrap();
        assert_eq!(
            gated.state(|gate| gate.entered),
            0,
            "an unrelated pool's write entered the gated journal"
        );

        let (sender, mut callback) = TestConfig::oneshot();
        log.append([blank(0)], IOFlushed::signal(sender))
            .await
            .unwrap();
        gated.wait_for_sync().await;
        assert!(matches!(
            callback.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
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
        // Wait for the append submission itself. The number of pending
        // guards is an implementation detail and cannot signal submission.
        log.append([blank(1)], IOFlushed::signal(sender))
            .await
            .unwrap();
        // PERF2d: replication can read both appends before either syncs.
        assert_eq!(
            log_ids(log.limited_get_log_entries(0, 2).await.unwrap()),
            [id(0), id(1)]
        );
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
        drop(held);

        next_callback.await.unwrap().unwrap();
        drain.await;
        assert!(first_ack_observed.load(Ordering::Acquire));
        assert_eq!(log.pending.count.load(Ordering::Acquire), 0);
        assert_eq!(shared.apply_bytes_used(), 0);
        assert_eq!(shared.working_decode_bytes(), 0);
        assert_eq!(shared.control_bytes_used(), 0);
        assert_eq!(shared.scheduler().usage(), baseline);
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

/// C07/O05: a bounded entry stream ends at its exclusive end without another
/// native read. OpenRaft applies `since..end` through this stream and answers
/// each proposer inside the apply. A trailing read for index `end` needed a
/// Query slot after the last reply was already visible, so a caller that took
/// the worker's only Query slot on that reply stalled the leader's applied
/// index, and every linearizable read waiting for it, until it let go (CIfix2:
/// the C07 progress test hung on Linux CI).
#[tokio::test]
async fn c07_o05_bounded_entry_stream_ends_without_reading_past_its_end() {
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
        let mut f = Fixture::new().await;
        for index in 0..3 {
            f.log
                .append([blank(index)], IOFlushed::noop())
                .await
                .unwrap();
        }
        let shared = f.log.group().resources().clone();
        let mut stream = Box::pin(f.log.entries_stream(0..3).await);
        for index in 0..3 {
            assert_eq!(stream.next().await.unwrap().unwrap().index(), index);
        }
        let query = shared.prepare(ExecutionClass::Query).await.unwrap();
        let end = nabedra_rt::time::timeout(Duration::from_secs(2), stream.next()).await;
        drop(query);
        assert!(
            matches!(end, Ok(None)),
            "the stream needed a Query slot after its last entry"
        );
    })
    .await
    .expect("bounded entry stream watchdog");
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
    // `append` returns before the sync (PERF2d); this test reads the store.
    f.log.drain().await;
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
    f.log.drain().await;
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
                table_id: nabedra_rt::ids::uuid_v4(),
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
    // Read the decoded store copy, not the pending entry (PERF2d).
    f.log.drain().await;
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
/// A second append's write and callback, and replica drain, stay behind the
/// first actual sync. Both appends return before their writes (PERF2d).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f03_cancelled_append_keeps_callback_order_and_drain_until_native_settles() {
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
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
            nabedra_rt::task::spawn(async move { writer.append([blank(0)], IOFlushed::signal(sent)).await });
        while observed.try_recv().is_err() {
            tokio::task::yield_now().await;
        }
        first.abort();
        first.await.unwrap().unwrap();
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(20), &mut done)
                .await
                .is_err()
        );
        assert!(f.log.writes.try_lock().is_err());
        let (sent, mut second_done) = TestConfig::oneshot();
        let mut second = f.log.clone();
        second
            .append([blank(1)], IOFlushed::signal(sent))
            .await
            .unwrap();
        let log = f.log.clone();
        let drain = nabedra_rt::task::spawn(async move { log.drain().await });
        tokio::task::yield_now().await;
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(20), &mut second_done)
                .await
                .is_err()
        );
        assert!(!drain.is_finished());
        assert!(f.log.group().resources().apply_bytes_used() > 0);
        drop(release);
        done.await.unwrap().unwrap();
        second_done.await.unwrap().unwrap();
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
/// While all Query slots are held, the append has returned and holds no
/// Control slot, and another group's vote metadata progresses. The same
/// group's vote queues behind its pending append (OpenRaft orders vote and
/// log IO, PERF2d) and completes after that append's sync and callback.
#[tokio::test]
async fn c07_f03_append_waiting_for_native_slot_stays_tracked_and_orders_its_group_vote() {
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let shared = f.log.group().resources();
        let query = shared.prepare(ExecutionClass::Query).await.unwrap();
        let (sent, mut callback) = TestConfig::oneshot();
        let mut append = f.log.clone();
        append
            .append([blank(0)], IOFlushed::signal(sent))
            .await
            .unwrap();
        assert_eq!(
            f.log
                .pending
                .count
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        let mut identity = f.log.group().identity().clone();
        identity.group_id = nabedra_rt::ids::uuid_v4();
        identity.kind = GroupKind::Control;
        let mut other =
            TypedLogStore::<ControlTestConfig>::new(f._pool.group(identity).await.unwrap(), 1)
                .await
                .unwrap();
        other.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
        assert_eq!(
            other.read_vote().await.unwrap(),
            Some(Vote::new_committed(3, 1))
        );
        let mut vote = f.log.clone();
        let vote = nabedra_rt::task::spawn(async move {
            vote.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
            vote
        });
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(20), &mut callback)
                .await
                .is_err()
        );
        let log = f.log.clone();
        let drain = nabedra_rt::task::spawn(async move { log.drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        assert!(!vote.is_finished());
        assert_eq!(shared.scheduler().usage()[0].1, 0);
        drop(query);
        callback.await.unwrap().unwrap();
        drain.await.unwrap();
        let mut vote = vote.await.unwrap();
        assert_eq!(
            vote.read_vote().await.unwrap(),
            Some(Vote::new_committed(3, 1))
        );
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
    identity.group_id = nabedra_rt::ids::uuid_v4();
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
    nabedra_rt::time::timeout(
        Duration::from_secs(5),
        control.append(
            [OwnedRaftEntry::new(id(0), Payload::Normal(command))],
            IOFlushed::noop(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    // Read the decoded store copy, not the pending entry (PERF2d).
    nabedra_rt::time::timeout(Duration::from_secs(5), control.drain())
        .await
        .unwrap();
    let read = nabedra_rt::time::timeout(Duration::from_secs(5), control.try_get_log_entries(0..1))
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
    identity.group_id = nabedra_rt::ids::uuid_v4();
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
    nabedra_rt::time::timeout(
        Duration::from_secs(5),
        log.append(
            [OwnedRaftEntry::new(id(0), Payload::Normal(command))],
            IOFlushed::signal(send),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    nabedra_rt::time::timeout(Duration::from_secs(5), done)
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

fn log_ids(entries: Vec<OwnedRaftEntry<Command>>) -> Vec<LogId> {
    entries.iter().map(|entry| entry.log_id).collect()
}
/// A Catalog group's pool on a gated `FaultFs`, as in the O06/O11 test above.
struct GatedLog {
    _dir: Dir,
    path: std::path::PathBuf,
    gated: GatedSync,
    shared: Arc<WorkerSharedResources>,
    identity: PoolIdentity,
    group: GroupIdentity,
}
impl GatedLog {
    fn new() -> Self {
        let dir = Dir(std::env::temp_dir().join(format!("typed-log-perf2d-{}", nabedra_rt::ids::uuid_v4())));
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
            cluster_id: nabedra_rt::ids::uuid_v4(),
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let group = GroupIdentity {
            cluster_id: identity.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        Self {
            _dir: dir,
            path,
            gated,
            shared,
            identity,
            group,
        }
    }
    async fn open(
        &self,
        fs: Arc<dyn nabedra_kv::fs::Fs>,
        create: bool,
    ) -> (OltpPool, TypedLogStore<TestConfig>) {
        let pool = OltpPool::open_development_on(
            &self.path,
            self.identity.clone(),
            self.shared.clone(),
            fs,
        )
        .unwrap();
        let group = if create {
            pool.group(self.group.clone()).await
        } else {
            pool.retained_group(self.group.clone()).await
        }
        .unwrap();
        let log = TypedLogStore::<TestConfig>::new(group, 1).await.unwrap();
        (pool, log)
    }
    /// Waits until a sync reached the closed gate (GC1; lost in the merge
    /// of GC1 into `nabedra/hardening`, restored by GC2).
    async fn entered(&self) {
        while self.gated.state(|gate| gate.entered) == 0 {
            nabedra_rt::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// O29/NS10 (FaultFs crash and power-loss models), PERF2d: a crash between an
/// append's overlay publish and its sync. Before the crash the entry is
/// readable for replication but neither acknowledged, applied nor named by a
/// durable commit marker. After it, every acknowledged entry survives, the
/// unsynced one survives only as an unacknowledged tail, and the commit marker
/// never names it. The held sync fails on the halted machine, so its callback
/// reports an error, never a flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o29_ns10_perf2d_crash_between_overlay_publish_and_sync_keeps_only_acked_entries() {
    nabedra_rt::time::timeout(Duration::from_secs(60), async {
        let rig = GatedLog::new();
        let (pool, mut log) = rig.open(Arc::new(rig.gated.clone()), true).await;
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(0)], IOFlushed::signal(sent))
            .await
            .unwrap();
        acked.await.unwrap().unwrap();
        log.save_committed(Some(id(0))).await.unwrap();
        // The previous marker must finish its background write before the
        // gate can hold a journal batch that would delay that write.
        assert_eq!(log.read_committed().await.unwrap(), Some(id(0)));

        let held = rig.gated.hold();
        let (sent, mut pending) = TestConfig::oneshot();
        log.append([blank(1)], IOFlushed::signal(sent))
            .await
            .unwrap();
        rig.gated.wait_for_sync().await;
        assert_eq!(
            log_ids(log.limited_get_log_entries(0, 2).await.unwrap()),
            [id(0), id(1)]
        );
        assert_eq!(
            log_ids(log.try_get_log_entries(..).await.unwrap()),
            [id(0), id(1)]
        );
        let mut apply = Box::pin(log.entries_stream(1..2).await);
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(50), apply.next())
                .await
                .is_err(),
            "applying an entry must wait for its local sync"
        );
        drop(apply);
        // A quorum of other replicas may commit it first: the marker waits.
        log.save_committed(Some(id(1))).await.unwrap();
        assert_eq!(log.read_committed().await.unwrap(), Some(id(0)));
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(20), &mut pending)
                .await
                .is_err()
        );

        // Power cut while the sync is held.
        let crashed = rig.gated.inner.crash();
        let losses = (0..8)
            .map(|seed| rig.gated.inner.power_loss(seed))
            .collect::<Vec<_>>();
        rig.gated.inner.halt();
        drop(held);
        assert!(
            pending.await.unwrap().is_err(),
            "a sync that failed must not report a flush"
        );
        let (sent, failed) = TestConfig::oneshot();
        assert!(
            log.append([blank(2)], IOFlushed::signal(sent))
                .await
                .is_err()
        );
        assert!(failed.await.unwrap().is_err());
        log.drain().await;
        drop(log);
        drop(pool);

        for (case, fs) in std::iter::once(crashed).chain(losses).enumerate() {
            let (pool, mut log) = rig.open(Arc::new(fs), false).await;
            let entries = log_ids(log.try_get_log_entries(..).await.unwrap());
            if case == 0 {
                assert_eq!(entries, [id(0)], "crash keeps exactly the synced log");
            } else {
                assert!(
                    entries == [id(0)] || entries == [id(0), id(1)],
                    "power loss {case}: {entries:?}"
                );
            }
            assert_eq!(
                log.get_log_state().await.unwrap().last_log_id,
                entries.last().copied()
            );
            let committed = log.read_committed().await.unwrap();
            assert!(
                committed.is_none_or(|committed| committed == id(0)),
                "case {case}: commit marker {committed:?} names an unsynced entry"
            );
            drop(log);
            drop(pool);
        }
    })
    .await
    .expect("PERF2d crash watchdog");
}

/// O11/PERF2d: a commit of an entry whose local sync still runs is recorded
/// after that sync, and applying it waits for the sync. Replication reads it
/// meanwhile. A later marker still advances directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o11_perf2d_commit_marker_and_apply_follow_the_local_sync() {
    nabedra_rt::time::timeout(Duration::from_secs(30), async {
        let rig = GatedLog::new();
        let (_pool, mut log) = rig.open(Arc::new(rig.gated.clone()), true).await;
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(0)], IOFlushed::signal(sent))
            .await
            .unwrap();
        acked.await.unwrap().unwrap();
        log.save_committed(Some(id(0))).await.unwrap();
        assert_eq!(log.read_committed().await.unwrap(), Some(id(0)));

        let held = rig.gated.hold();
        let (sent, mut pending) = TestConfig::oneshot();
        log.append([blank(1), blank(2)], IOFlushed::signal(sent))
            .await
            .unwrap();
        rig.gated.wait_for_sync().await;
        let mut reader = log.clone();
        let mut apply = Box::pin(reader.entries_stream(1..3).await);
        log.save_committed(Some(id(2))).await.unwrap();
        assert_eq!(log.read_committed().await.unwrap(), Some(id(0)));
        assert_eq!(
            log_ids(log.limited_get_log_entries(1, 3).await.unwrap()),
            [id(1), id(2)]
        );
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(30), apply.next())
                .await
                .is_err(),
            "applying the batch must wait for its local sync"
        );
        assert!(
            nabedra_rt::time::timeout(Duration::from_millis(1), &mut pending)
                .await
                .is_err()
        );

        drop(held);
        pending.await.unwrap().unwrap();
        let applied = apply.map(|entry| entry.unwrap().log_id).collect::<Vec<_>>();
        assert_eq!(applied.await, [id(1), id(2)]);
        while log.read_committed().await.unwrap() != Some(id(2)) {
            nabedra_rt::time::sleep(Duration::from_millis(5)).await;
        }
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(3)], IOFlushed::signal(sent))
            .await
            .unwrap();
        acked.await.unwrap().unwrap();
        log.save_committed(Some(id(3))).await.unwrap();
        assert_eq!(log.read_committed().await.unwrap(), Some(id(3)));
        log.drain().await;
    })
    .await
    .expect("PERF2d deferred commit watchdog");
}

/// O29/F03, PERF2d: a failed append drops its pending entries; appends queued
/// behind it fail without writing (no hole), a later append fails at once,
/// and a core that reads the log state again starts on the durable log.
#[tokio::test]
async fn o29_perf2d_failed_append_fails_queued_appends_until_the_log_state_is_read_again() {
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let mut log = f.log.clone();
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(0)], IOFlushed::signal(sent))
            .await
            .unwrap();
        acked.await.unwrap().unwrap();

        let query = f
            .log
            .group()
            .resources()
            .prepare(ExecutionClass::Query)
            .await
            .unwrap();
        log.fail_append
            .store(true, std::sync::atomic::Ordering::Release);
        let (sent, first) = TestConfig::oneshot();
        log.append([blank(1)], IOFlushed::signal(sent))
            .await
            .unwrap();
        let (sent, second) = TestConfig::oneshot();
        log.append([blank(2)], IOFlushed::signal(sent))
            .await
            .unwrap();
        // Pending entries are read without a store read (the Query slot is held).
        assert_eq!(
            log_ids(log.limited_get_log_entries(1, 3).await.unwrap()),
            [id(1), id(2)]
        );
        drop(query);
        assert!(first.await.unwrap().is_err());
        assert!(second.await.unwrap().is_err());
        log.drain().await;
        assert_eq!(
            log_ids(log.limited_get_log_entries(0, 3).await.unwrap()),
            [id(0)]
        );
        let (sent, refused) = TestConfig::oneshot();
        assert!(
            log.append([blank(1)], IOFlushed::signal(sent))
                .await
                .is_err()
        );
        assert!(refused.await.unwrap().is_err());

        assert_eq!(log.get_log_state().await.unwrap().last_log_id, Some(id(0)));
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(1)], IOFlushed::signal(sent))
            .await
            .unwrap();
        acked.await.unwrap().unwrap();
        assert_eq!(
            log_ids(log.try_get_log_entries(..).await.unwrap()),
            [id(0), id(1)]
        );
    })
    .await
    .expect("PERF2d failed append watchdog");
}

/// O11/C07, PERF2d item 2: the commit marker of a durable entry is written in
/// the background, so the Raft core hands a commit to the apply worker without
/// a native write. With the only Control slot held, `save_committed` returns at
/// once; reading the marker and draining the replica wait for the writer.
#[tokio::test]
async fn o11_c07_perf2d_commit_marker_is_written_off_the_commit_path() {
    nabedra_rt::time::timeout(Duration::from_secs(15), async {
        let f = Fixture::new().await;
        let mut log = f.log.clone();
        let (sent, acked) = TestConfig::oneshot();
        log.append([blank(0)], IOFlushed::signal(sent)).await.unwrap();
        acked.await.unwrap().unwrap();
        let control = f
            .log
            .group()
            .resources()
            .prepare(ExecutionClass::Control)
            .await
            .unwrap();
        nabedra_rt::time::timeout(Duration::from_secs(1), log.save_committed(Some(id(0))))
            .await
            .expect("save_committed waited for a native write")
            .unwrap();
        let read = nabedra_rt::task::spawn({
            let mut log = log.clone();
            async move { log.read_committed().await.unwrap() }
        });
        let drain = nabedra_rt::task::spawn({
            let log = log.clone();
            async move { log.drain().await }
        });
        nabedra_rt::time::sleep(Duration::from_millis(20)).await;
        assert!(!read.is_finished());
        assert!(!drain.is_finished());
        drop(control);
        assert_eq!(read.await.unwrap(), Some(id(0)));
        drain.await.unwrap();
    })
    .await
    .expect("PERF2d marker watchdog");
}

/// O06/O11 (fsync before ack), GC1: appends queued behind a held sync share
/// one synced write, and a vote queued between appends is never merged
/// across. With the first append's sync held, appends 1 and 2 queue, then a
/// vote, then appends 3 and 4. After the release the writes are `[0]`,
/// `[1, 2]`, the vote, `[3, 4]`: four journal syncs. Before GC1 every append
/// was its own synced write (six); merging across the vote would give three.
/// No callback fires while the first sync is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o06_o11_gc1_queued_appends_share_one_sync_and_never_cross_a_vote() {
    use std::sync::atomic::{AtomicU64, Ordering};

    nabedra_rt::time::timeout(Duration::from_secs(30), async {
        let rig = GatedLog::new();
        let (pool, mut log) = rig.open(Arc::new(rig.gated.clone()), true).await;
        let syncs = || pool.kv_store().stats().journal_syncs;
        let before = syncs();
        let held_sync = rig.gated.hold();
        let mut acks = Vec::new();
        let mut ack = |index: u64| {
            let (sent, acked) = TestConfig::oneshot();
            acks.push(acked);
            ([blank(index)], IOFlushed::signal(sent))
        };
        let (entries, flushed) = ack(0);
        log.append(entries, flushed).await.unwrap();
        rig.gated.wait_for_sync().await;
        for index in [1, 2] {
            let (entries, flushed) = ack(index);
            log.append(entries, flushed).await.unwrap();
        }
        // The vote takes its writer place behind appends 0 to 2.
        let mut voter = log.clone();
        let new_vote = Vote::new_committed(2, 1);
        let mut vote = Box::pin(voter.save_vote(&new_vote));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(vote.as_mut().poll(cx).is_pending()))
                .await,
            "the vote waits for the held append"
        );
        let syncs_at_post_vote_append = Arc::new(AtomicU64::new(0));
        *log.append_hook.lock().unwrap() = Some(Arc::new({
            let pool = pool.clone();
            let observed = syncs_at_post_vote_append.clone();
            move || observed.store(pool.kv_store().stats().journal_syncs, Ordering::Release)
        }));
        for index in [3, 4] {
            let (entries, flushed) = ack(index);
            log.append(entries, flushed).await.unwrap();
        }
        for acked in &mut acks {
            assert!(matches!(
                acked.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
        }
        drop(held_sync);
        vote.await.unwrap();
        for acked in acks {
            acked.await.unwrap().unwrap();
        }
        log.drain().await;
        assert_eq!(
            syncs_at_post_vote_append.load(Ordering::Acquire),
            before + 3,
            "the vote must sync before the post-vote batch starts"
        );
        assert_eq!(
            syncs() - before,
            4,
            "writes [0], [1, 2], vote, [3, 4]: one sync each"
        );
        assert_eq!(
            log_ids(log.try_get_log_entries(..).await.unwrap()),
            (0..5).map(id).collect::<Vec<_>>()
        );
        assert_eq!(
            voter.read_vote().await.unwrap(),
            Some(Vote::new_committed(2, 1))
        );
    })
    .await
    .expect("GC1 append coalescing watchdog");
}

/// KVfix4, NS13/O05: once `drain` returns, no append task still holds what it
/// captured (the group store, charged entries, the worker's resource owner).
/// The hosted NS13 test drops its pool after a drain and reopens the worker at
/// once; an append task that released its pending guard before dropping its
/// captured store let `bind_worker` meet the old owner ("worker storage
/// capacity unavailable"). The task's last copy of the hook drops slowly, so a
/// guard released first is caught every time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns13_o05_drain_waits_until_the_append_task_dropped_its_state() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct SlowDrop(Arc<AtomicBool>);
    impl Drop for SlowDrop {
        fn drop(&mut self) {
            nabedra_rt::time::thread_sleep(Duration::from_millis(200));
            self.0.store(true, Ordering::Release);
        }
    }
    let mut f = Fixture::new().await;
    let dropped = Arc::new(AtomicBool::new(false));
    let slow = SlowDrop(dropped.clone());
    *f.log.append_hook.lock().unwrap() = Some(Arc::new(move || {
        let _ = &slow;
    }));
    let (sender, callback) = TestConfig::oneshot();
    f.log
        .append([blank(0)], IOFlushed::signal(sender))
        .await
        .unwrap();
    callback.await.unwrap().unwrap();
    nabedra_rt::time::timeout(Duration::from_secs(10), f.log.drain())
        .await
        .expect("drain");
    assert!(
        dropped.load(Ordering::Acquire),
        "drain returned while the append task still held its captured state"
    );
}
