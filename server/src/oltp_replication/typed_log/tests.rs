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
    fn accepts(kind:&GroupKind)->bool {matches!(kind,GroupKind::Catalog)}
}

#[derive(Serialize,Deserialize)]
struct MetadataCommand{value:u64}
impl std::fmt::Display for MetadataCommand{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str("control.metadata")}}
impl OwnedPayload for MetadataCommand{
    fn heap_bytes(&self)->Result<usize,ProtocolError>{Ok(0)}
    fn attach_fields(&self,_:Arc<ScratchLease>)->Result<(),ProtocolError>{Ok(())}
    fn fields_attached(&self)->bool{true}
}
openraft::declare_raft_types!(ControlTestConfig:D=OwnedCommand<MetadataCommand>,R=(),NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<MetadataCommand>);
impl TypedConfig for ControlTestConfig{type Command=MetadataCommand;type Reply=();fn accepts(kind:&GroupKind)->bool{matches!(kind,GroupKind::Control)}}
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
                pool_format_version: 1,
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
    assert_eq!(limited.len(), 128);
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
    tokio::time::timeout(Duration::from_secs(15),async {
        let f=Fixture::new().await;
        let shared=f.log.group().resources();
        let query=shared.prepare(ExecutionClass::Query).await.unwrap();
        let (sent,mut callback)=TestConfig::oneshot();
        let mut append=f.log.clone();let append=tokio::spawn(async move{append.append([blank(0)],IOFlushed::signal(sent)).await});
        while f.log.pending.count.load(std::sync::atomic::Ordering::Acquire)==0{tokio::task::yield_now().await;}
        append.abort();assert!(append.await.unwrap_err().is_cancelled());
        let mut vote=f.log.clone();
        vote.save_vote(&Vote::new_committed(3,1)).await.unwrap();
        assert_eq!(vote.read_vote().await.unwrap(),Some(Vote::new_committed(3,1)));
        assert!(tokio::time::timeout(Duration::from_millis(20),&mut callback).await.is_err());
        let log=f.log.clone();let drain=tokio::spawn(async move{log.drain().await});
        tokio::task::yield_now().await;assert!(!drain.is_finished());
        assert_eq!(shared.scheduler().usage()[0].1,0);
        drop(query);callback.await.unwrap().unwrap();drain.await.unwrap();
        let mut log=f.log.clone();assert_eq!(log.get_log_state().await.unwrap().last_log_id,Some(id(0)));
    }).await.expect("native-wait ownership watchdog");
}

/// C07/M02: actual Control entry ownership and WAL recovery do not depend on
/// free Data wire bytes, Data working memory or a Query native slot.
#[tokio::test]
async fn c07_m02_control_append_and_read_progress_with_all_data_memory_and_query_capacity_held() {
    let f=Fixture::new().await;
    let shared=f.log.group().resources().clone();
    let mut identity=f.log.group().identity().clone();identity.group_id=Uuid::new_v4();identity.kind=GroupKind::Control;
    let mut control=TypedLogStore::<ControlTestConfig>::new(f._pool.group(identity).await.unwrap(),1).await.unwrap();
    let limits=WorkerResources{max_threads:4,..Default::default()};
    let remaining=limits.working_bytes()-shared.scheduler().usage()[0].2;
    let working=shared.scheduler().charge_memory(shared.scheduler_node_id(),remaining).unwrap();
    assert!(shared.reserve_scratch(ByteClass::Data,1).is_err());
    let data=shared.allocate_bytes(ByteClass::Data,shared.config().control_bytes-shared.config().progress_bytes).unwrap();
    let query=shared.prepare(ExecutionClass::Query).await.unwrap();
    let command=OwnedCommand::new_owned(MetadataCommand{value:42},&shared,ByteClass::Progress).unwrap();
    tokio::time::timeout(Duration::from_secs(5),control.append([OwnedRaftEntry::new(id(0),Payload::Normal(command))],IOFlushed::noop())).await.unwrap().unwrap();
    let read=tokio::time::timeout(Duration::from_secs(5),control.try_get_log_entries(0..1)).await.unwrap().unwrap();
    let Payload::Normal(command)=read[0].payload() else{panic!("normal")};assert_eq!(command.payload().unwrap().value,42);
    assert_eq!(shared.working_decode_bytes(),0);
    drop(read);drop(query);drop(data);drop(working);
    assert_eq!(shared.buffer_bytes_used(ByteClass::Progress),0);
}
