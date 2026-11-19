//! T2 group-commit regressions: batched synced appends, unsynced application
//! and commit hints, and the purge/snapshot durability boundary.
use super::*;
use crate::{
    config::WorkerResources,
    oltp::pool::{
        ByteClass, OltpPool, PoolIdentity, ScratchLease, SharedResourceConfig,
        TEST_UNSYNCED_WAL_IN_MEMORY, TransactionBudget, WorkerSharedResources,
    },
    oltp_replication::{
        entry::OwnedRaftEntry,
        multiplex::{EndpointRegistry, ResidentGroup},
        owned::OwnedPayload,
        typed_log::{MAX_APPEND_BYTES, MAX_APPEND_ENTRIES, append_chunks},
        types::{GroupIdentity, ReplicaMember, Text},
    },
};
use openraft::{
    RaftLogReader,
    entry::RaftEntry,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse},
    storage::{IOFlushed, RaftLogStorage},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use uuid::Uuid;

const CRASH_DIR: &str = "NABEDRA_T2_CRASH_DIR";
const COMMANDS: u64 = 200;
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 64 << 10,
    batch_bytes: 64 << 10,
    output_bytes: 4 << 10,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bump {
    id: u64,
    pad: String,
}
impl std::fmt::Display for Bump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("test.bump")
    }
}
impl OwnedPayload for Bump {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(self.pad.capacity())
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
openraft::declare_raft_types!(
    CounterConfig:
        D=OwnedCommand<Bump>,R=Applied<Option<u64>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<Bump>,
);
impl TypedConfig for CounterConfig {
    type Command = Bump;
    type Reply = u64;
    fn accepts(kind: &GroupKind) -> bool {
        matches!(kind, GroupKind::Catalog)
    }
}

/// Counts each command once. A second application of the same command ID is
/// a storage failure, so a double apply cannot hide behind a final count.
#[derive(Clone)]
struct Counter {
    store: GroupStore,
}
impl TypedMachine<CounterConfig> for Counter {
    fn store(&self) -> &GroupStore {
        &self.store
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<Bump>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let id = command.payload()?.id;
        self.store
            .transaction(meta, BUDGET, move |tx| {
                let marker = marker(id);
                if tx.get(&marker)?.is_some() {
                    return Err(ProtocolError::new(
                        ErrorCode::StorageUnavailable,
                        "test command applied twice",
                        Outcome::Unknown,
                    ));
                }
                let count = match tx.get(b"count")? {
                    Some(raw) => u64::from_be_bytes(raw.as_slice().try_into().unwrap()),
                    None => 0,
                } + 1;
                tx.put(&marker, b"1")?;
                tx.put(b"count", &count.to_be_bytes())?;
                Ok(Transition::Commit(
                    tx.copy_output(count.to_string().as_bytes())?,
                ))
            })
            .await
    }
}
fn marker(id: u64) -> Vec<u8> {
    let mut key = b"applied/".to_vec();
    key.extend_from_slice(&id.to_be_bytes());
    key
}

/// A single voter never opens a peer connection.
struct NoPeers;
struct NoPeer;
impl RaftNetworkFactory<CounterConfig> for NoPeers {
    type Network = NoPeer;
    async fn new_client(&mut self, _: u64, _: &ReplicaMember) -> NoPeer {
        NoPeer
    }
}
impl RaftNetworkV2<CounterConfig> for NoPeer {
    type SnapshotData = TypedSnapshot;
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<CounterConfig>,
        _: RPCOption,
    ) -> Result<AppendResult<CounterConfig>, RPCError<CounterConfig>> {
        Err(RPCError::Unreachable(Unreachable::from_string("no peers")))
    }
    async fn vote(
        &mut self,
        _: VoteRequest<CounterConfig>,
        _: RPCOption,
    ) -> Result<VoteResponse<CounterConfig>, RPCError<CounterConfig>> {
        Err(RPCError::Unreachable(Unreachable::from_string("no peers")))
    }
    async fn full_snapshot(
        &mut self,
        _: crate::oltp_replication::entry::Vote,
        _: SnapshotOf<CounterConfig, TypedSnapshot>,
        _: impl Future<Output = ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<CounterConfig>, StreamingError<CounterConfig>> {
        Err(StreamingError::Unreachable(Unreachable::from_string(
            "no peers",
        )))
    }
}
fn shared() -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        2,
        WorkerResources {
            max_threads: 4,
            ..Default::default()
        },
        SharedResourceConfig::default(),
    )
    .unwrap()
}
#[derive(Clone, Serialize, Deserialize)]
struct Layout {
    pool: PoolIdentity,
    group: GroupIdentity,
}
impl Layout {
    fn new() -> Self {
        let pool = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: 1,
            cluster_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        };
        let group = GroupIdentity {
            cluster_id: pool.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        Self { pool, group }
    }
    fn member(&self) -> ReplicaMember {
        ReplicaMember {
            node_id: 1,
            worker_id: self.pool.worker_id,
            pool_id: self.pool.pool_id,
        }
    }
}
struct Node {
    consensus: Arc<RaftConsensus<CounterConfig, Counter>>,
    store: GroupStore,
    pool: OltpPool,
    _registry: Arc<EndpointRegistry>,
    _resident: ResidentGroup,
}
impl Node {
    async fn open(path: &Path, layout: &Layout) -> Self {
        let shared = shared();
        let pool = OltpPool::open_development(path, layout.pool.clone(), shared.clone()).unwrap();
        let store = pool.group(layout.group.clone()).await.unwrap();
        let registry = Arc::new(EndpointRegistry::new(shared));
        let (resident, lease) = registry.reserve_replica(layout.group.clone(), 1).unwrap();
        let log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
            .await
            .unwrap()
            .with_resident(lease.clone())
            .unwrap();
        let state = HostedState::new(
            Counter {
                store: store.clone(),
            },
            lease,
        )
        .unwrap();
        let config = batched_config(openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        })
        .unwrap();
        let consensus = RaftConsensus::start(config, NoPeers, log, state)
            .await
            .unwrap();
        Self {
            consensus,
            store,
            pool,
            _registry: registry,
            _resident: resident,
        }
    }
    async fn lead(&self, layout: &Layout, initialize: bool) {
        if initialize {
            self.consensus
                .raft
                .initialize(BTreeMap::from([(1, layout.member())]))
                .await
                .unwrap();
        }
        self.consensus
            .raft
            .wait(Some(Duration::from_secs(20)))
            .current_leader(1, "single voter leads")
            .await
            .unwrap();
    }
    /// Proposes concurrently so client writes coalesce into append batches.
    async fn propose_all(&self, ids: std::ops::Range<u64>) -> u64 {
        let shared = self.store.resources().clone();
        let mut tasks = Vec::new();
        for id in ids {
            let consensus = self.consensus.clone();
            let command = OwnedCommand::new_owned(
                Bump {
                    id,
                    pad: "x".repeat(256),
                },
                &shared,
                ByteClass::Data,
            )
            .unwrap();
            tasks.push(tokio::spawn(async move {
                consensus.propose(command).await.unwrap().position.index
            }));
        }
        let mut last = 0;
        for task in tasks {
            last = last.max(task.await.unwrap());
        }
        last
    }
    async fn count(&self) -> u64 {
        read_count(&self.store).await
    }
}
async fn read_count(store: &GroupStore) -> u64 {
    let raw = store
        .read(BUDGET, |view| match view.get(b"count")? {
            Some(raw) => view.copy_output(raw.as_slice()),
            None => view.copy_output(&0u64.to_be_bytes()),
        })
        .await
        .unwrap();
    u64::from_be_bytes(raw.as_slice().try_into().unwrap())
}
async fn read_markers(store: &GroupStore, ids: std::ops::Range<u64>) -> u64 {
    let mut found = 0;
    for id in ids {
        let present = store
            .read(BUDGET, move |view| {
                let present = view.get(&marker(id))?.is_some();
                view.copy_output(&[u8::from(present)])
            })
            .await
            .unwrap();
        found += u64::from(present.as_slice() == [1]);
    }
    found
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("t2-hosted-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Serialize, Deserialize)]
struct Acknowledged {
    layout: Layout,
    commands: u64,
    last_index: u64,
}

/// Child half of D09/O11/O29. Every proposal returns after its entry is
/// committed and applied; the process then aborts without flushing RocksDB.
#[test]
#[ignore = "child process of d09_o11_o29_lost_unsynced_applies_replay_once_from_the_synced_log"]
fn d09_o11_unsynced_apply_crash_child() {
    let Some(dir) = std::env::var_os(CRASH_DIR) else {
        return;
    };
    assert!(std::env::var_os(TEST_UNSYNCED_WAL_IN_MEMORY).is_some());
    let dir = PathBuf::from(dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let layout = Layout::new();
        let node = Node::open(&dir.join("pool"), &layout).await;
        node.lead(&layout, true).await;
        let last_index = node.propose_all(0..COMMANDS).await;
        assert_eq!(node.count().await, COMMANDS);
        let ack = Acknowledged {
            layout,
            commands: COMMANDS,
            last_index,
        };
        let temporary = dir.join("ack.new");
        std::fs::write(&temporary, serde_json::to_vec(&ack).unwrap()).unwrap();
        std::fs::rename(temporary, dir.join("ack")).unwrap();
        // No destructor runs: RocksDB never writes its in-memory WAL buffer.
        std::process::abort();
    });
}

/// D09 (durability half), O11, O29: application and the commit hint are
/// unsynced, and a crash drops them with the WAL tail. The synced log in the
/// same WAL survives, replays every lost application, and nothing applies
/// twice. Power loss is modelled by keeping unsynced WAL records in process
/// memory (`manual_wal_flush`) and aborting the process; this is not an actual
/// power-loss or fsync fault injection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d09_o11_o29_lost_unsynced_applies_replay_once_from_the_synced_log() {
    let dir = Dir::new();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "oltp_replication::hosted::tests::d09_o11_unsynced_apply_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CRASH_DIR, &dir.0)
        .env(TEST_UNSYNCED_WAL_IN_MEMORY, "1")
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let ack: Acknowledged = loop {
        if let Ok(bytes) = std::fs::read(dir.0.join("ack")) {
            break serde_json::from_slice(&bytes).unwrap();
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("crash child exited before acknowledging: {status}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("crash child timed out");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let status = child.wait().unwrap();
    assert!(!status.success(), "child must abort, not exit cleanly");

    let path = dir.0.join("pool");
    let layout = ack.layout;
    {
        // Inspect storage before Raft replays anything.
        let pool = OltpPool::open_development(&path, layout.pool.clone(), shared()).unwrap();
        let store = pool.retained_group(layout.group.clone()).await.unwrap();
        let applied = store.applied_state(BUDGET).await.unwrap().unwrap();
        assert!(
            applied.position.index < ack.last_index,
            "the unsynced application tail must be lost: applied {} last {}",
            applied.position.index,
            ack.last_index
        );
        let before = read_count(&store).await;
        assert!(before < ack.commands);
        assert_eq!(read_markers(&store, 0..ack.commands).await, before);
        let mut log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
            .await
            .unwrap();
        assert_eq!(
            log.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|id| id.index),
            Some(ack.last_index),
            "every acknowledged entry was in the synced log"
        );
        let committed = log.read_committed().await.unwrap().map(|id| id.index);
        assert!(committed.is_none_or(|index| index <= ack.last_index));
        drop(log);
        drop(store);
        drop(pool);
    }

    let node = Node::open(&path, &layout).await;
    node.lead(&layout, false).await;
    node.consensus
        .raft
        .wait(Some(Duration::from_secs(20)))
        .applied_index_at_least(Some(ack.last_index), "replay the lost applications")
        .await
        .unwrap();
    assert_eq!(node.count().await, ack.commands);
    assert_eq!(
        read_markers(&node.store, 0..ack.commands).await,
        ack.commands
    );
    // A new write still applies exactly once after the replay.
    node.propose_all(ack.commands..ack.commands + 1).await;
    assert_eq!(node.count().await, ack.commands + 1);
    node.consensus.shutdown().await.unwrap();
}

/// O06/O11: concurrent client writes coalesce into batched synced appends,
/// while application writes and commit hints issue no fsync of their own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o06_o11_group_commit_needs_fewer_fsyncs_than_entries() {
    let dir = Dir::new();
    let layout = Layout::new();
    let node = Node::open(&dir.0.join("pool"), &layout).await;
    node.lead(&layout, true).await;
    assert!(node.store.applies_deferred());
    let pool_before = node.pool.write_stats();
    let group_before = node.store.write_stats();
    let commands = 256;
    node.propose_all(0..commands).await;
    assert_eq!(node.count().await, commands);
    let pool_after = node.pool.write_stats();
    let group_after = node.store.write_stats();
    let fsyncs = pool_after.synced_writes - pool_before.synced_writes;
    let deferred = group_after.deferred_writes - group_before.deferred_writes;
    eprintln!("o06_o11 group commit: {commands} entries, {fsyncs} fsyncs, {deferred} unsynced");
    // Every application write is unsynced; commit hints add more.
    assert!(deferred >= commands, "{deferred} unsynced writes");
    // Before T2 this was about three fsyncs per entry.
    assert!(fsyncs < commands, "{fsyncs} fsyncs for {commands} entries");
    assert_eq!(
        group_after.synced_writes - group_before.synced_writes,
        fsyncs
    );
    node.consensus.shutdown().await.unwrap();
}

/// O11: a purge never passes the applied index and never outlives unsynced
/// application state; a snapshot never names an unsynced applied index.
#[tokio::test]
async fn o11_purge_and_snapshot_never_pass_the_durable_applied_index() {
    let dir = Dir::new();
    let layout = Layout::new();
    let pool =
        OltpPool::open_development(dir.0.join("pool"), layout.pool.clone(), shared()).unwrap();
    let store = pool.group(layout.group.clone()).await.unwrap();
    let mut log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
        .await
        .unwrap();
    let id = |index| {
        log_id(AppliedPosition {
            term: 1,
            node_id: 1,
            index,
        })
    };
    let entries: Vec<_> = (1..=10)
        .map(|index| OwnedRaftEntry::<Bump>::new(id(index), Payload::Blank))
        .collect();
    log.append(entries, IOFlushed::noop()).await.unwrap();
    assert!(store.defer_apply_sync(log.group()));
    let apply = |index: u64| {
        store.transaction(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index,
                },
                command_digest: Digest::from_bytes([index as u8; 32]),
                membership: None,
            },
            BUDGET,
            move |tx| {
                tx.put(b"last", &index.to_be_bytes())?;
                Ok(Transition::Commit(tx.copy_output(b"null")?))
            },
        )
    };
    for index in 1..=6 {
        apply(index).await.unwrap();
    }
    assert_eq!(store.write_stats().unsynced_applied_index, Some(6));
    // Entries 7..=10 are not applied: they are the only copy of that state.
    assert!(log.purge(id(7)).await.is_err());
    assert_eq!(store.write_stats().unsynced_applied_index, Some(6));
    assert!(!pool.requires_recovery());
    let before = pool.write_stats().synced_writes;
    log.purge(id(4)).await.unwrap();
    // One WAL sync for the applied state, then the synced purge itself.
    assert_eq!(pool.write_stats().synced_writes - before, 2);
    assert_eq!(store.write_stats().unsynced_applied_index, None);
    assert_eq!(
        log.get_log_state().await.unwrap().last_purged_log_id,
        Some(id(4))
    );
    // Purging again within durable state needs no extra sync.
    let before = pool.write_stats().synced_writes;
    log.purge(id(5)).await.unwrap();
    assert_eq!(pool.write_stats().synced_writes - before, 1);

    apply(7).await.unwrap();
    assert_eq!(store.write_stats().unsynced_applied_index, Some(7));
    let snapshot = store.export_snapshot(BUDGET).await.unwrap();
    assert_eq!(snapshot.header.applied.map(|a| a.index), Some(7));
    assert_eq!(store.write_stats().unsynced_applied_index, None);
    let entries = log.try_get_log_entries(6..11).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.index())
            .collect::<Vec<_>>(),
        [6, 7, 8, 9, 10]
    );
}

/// O06: one synced write per append chunk of at most 64 entries and 1 MiB.
#[test]
fn o06_append_chunks_honour_64_entries_and_1_mib() {
    assert_eq!(MAX_APPEND_ENTRIES, 64);
    assert_eq!(MAX_APPEND_BYTES, 1 << 20);
    assert_eq!(append_chunks(&[]), Vec::<std::ops::Range<usize>>::new());
    assert_eq!(append_chunks(&[100; 130]), [0..64, 64..128, 128..130]);
    assert_eq!(append_chunks(&[600 << 10; 3]), [0..1, 1..2, 2..3]);
    let oversized = append_chunks(&[2 << 20]);
    assert_eq!((oversized.len(), oversized[0].clone()), (1, 0..1));
    assert_eq!(
        append_chunks(&[500 << 10, 500 << 10, 100 << 10]),
        [0..2, 2..3]
    );
    assert_eq!(append_chunks(&[1 << 20, 1]), [0..1, 1..2]);
    let config = batched_config(openraft::Config::default()).unwrap();
    assert_eq!(config.max_append_entries, Some(64));
    assert_eq!(config.api_batch_capacity, Some(64));
    assert_eq!(config.api_batch_linger_ms, Some(0));
    assert_eq!(config.max_payload_entries, REPLICATION_PAYLOAD_ENTRIES);
}

/// O06/O11: a 64-entry append of about 2.5 MiB is written as bounded synced
/// batches, survives intact, and a replication read stays within the bound.
#[tokio::test]
async fn o06_o11_wide_append_writes_bounded_synced_batches() {
    let dir = Dir::new();
    let layout = Layout::new();
    let shared = shared();
    let pool = OltpPool::open_development(dir.0.join("pool"), layout.pool.clone(), shared.clone())
        .unwrap();
    let store = pool.group(layout.group.clone()).await.unwrap();
    let mut log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
        .await
        .unwrap();
    let entries: Vec<_> = (0..64)
        .map(|index| {
            let command = OwnedCommand::new_owned(
                Bump {
                    id: index,
                    pad: "p".repeat(40 << 10),
                },
                &shared,
                ByteClass::Data,
            )
            .unwrap();
            OwnedRaftEntry::new(
                log_id(AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index,
                }),
                Payload::Normal(command),
            )
        })
        .collect();
    let sizes: Vec<_> = entries
        .iter()
        .map(|entry| serde_json::to_vec(entry).unwrap().len())
        .collect();
    let expected = append_chunks(&sizes);
    assert!(expected.len() >= 3, "{expected:?}");
    assert!(expected.iter().all(|chunk| {
        chunk.len() <= MAX_APPEND_ENTRIES
            && sizes[chunk.clone()].iter().sum::<usize>() <= MAX_APPEND_BYTES
    }));
    let before = store.write_stats().synced_writes;
    log.append(entries, IOFlushed::noop()).await.unwrap();
    assert_eq!(
        store.write_stats().synced_writes - before,
        expected.len() as u64
    );
    assert_eq!(
        log.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .map(|id| id.index),
        Some(63)
    );
    let limited = log.limited_get_log_entries(0, 64).await.unwrap();
    assert_eq!(limited.len(), expected[0].len());
    let mut stream = Box::pin(log.entries_stream(..).await);
    let mut next = 0;
    while let Some(entry) = futures_util::StreamExt::next(&mut stream).await {
        assert_eq!(entry.unwrap().index(), next);
        next += 1;
    }
    assert_eq!(next, 64);
}

/// T1b: real typed Raft voters over mTLS worker transports.
#[path = "replicated_tests.rs"]
mod replicated;
