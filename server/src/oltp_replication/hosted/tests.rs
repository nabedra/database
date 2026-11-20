//! T2 group-commit regressions: batched synced appends, unsynced application
//! and commit hints, and the purge/snapshot durability boundary. NS7: the Raft
//! log on journal values at pool level (NS13, NS04 relocation crashes, write
//! amplification).
use super::*;
use crate::{
    config::WorkerResources,
    oltp::pool::{
        ByteClass, OltpPool, PoolIdentity, ScratchLease, SharedResourceConfig, TransactionBudget,
        WorkerSharedResources,
    },
    oltp_replication::{
        entry::OwnedRaftEntry,
        multiplex::{EndpointRegistry, ResidentGroup},
        owned::OwnedPayload,
        typed_log::{MAX_APPEND_BYTES, MAX_APPEND_ENTRIES, append_chunks},
        types::{GroupIdentity, ReplicaMember, Text},
    },
};
use nabedra_kv::fs::{FaultFs, Fs};
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
    time::Duration,
};
use uuid::Uuid;

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
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
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
        Self::open_with(path, layout, shared(), None).await
    }
    /// `fs` runs the pool's store on a `FaultFs` (crash and power-loss models).
    async fn open_with(
        path: &Path,
        layout: &Layout,
        shared: Arc<WorkerSharedResources>,
        fs: Option<FaultFs>,
    ) -> Self {
        let pool = match fs {
            Some(fs) => {
                let fs: Arc<dyn Fs> = Arc::new(fs);
                OltpPool::open_development_on(path, layout.pool.clone(), shared.clone(), fs)
            }
            None => OltpPool::open_development(path, layout.pool.clone(), shared.clone()),
        }
        .unwrap();
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

/// D09 (durability half), O11, O29: application and the commit hint are
/// unsynced, and a crash drops them with the journal tail. The synced log in
/// the same journal survives, replays every lost application, and nothing
/// applies twice. The store runs on `FaultFs`: after every proposal returned,
/// the machine halts and only bytes a completed sync covered survive. This is
/// the FaultFs crash model, not a physical power cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d09_o11_o29_lost_unsynced_applies_replay_once_from_the_synced_log() {
    let dir = Dir::new();
    let path = dir.0.join("pool");
    std::fs::create_dir_all(&path).unwrap();
    let fault = FaultFs::new();
    fault.create_dir_all_durable(&path);
    let layout = Layout::new();
    let shared = shared();
    let commands = COMMANDS;
    let last_index = {
        let node = Node::open_with(&path, &layout, shared.clone(), Some(fault.clone())).await;
        node.lead(&layout, true).await;
        let last_index = node.propose_all(0..commands).await;
        assert_eq!(node.count().await, commands);
        // The machine stops here: nothing after this point reaches the disk.
        fault.halt();
        let _ = node.consensus.shutdown().await;
        drop(node);
        last_index
    };
    let fault = fault.crash();
    let fs = || -> Arc<dyn Fs> { Arc::new(fault.clone()) };
    {
        // Inspect storage before Raft replays anything.
        let pool = OltpPool::open_development_on(&path, layout.pool.clone(), shared.clone(), fs())
            .unwrap();
        let store = pool.retained_group(layout.group.clone()).await.unwrap();
        let applied = store.applied_state(BUDGET).await.unwrap().unwrap();
        assert!(
            applied.position.index < last_index,
            "the unsynced application tail must be lost: applied {} last {}",
            applied.position.index,
            last_index
        );
        let before = read_count(&store).await;
        assert!(before < commands);
        assert_eq!(read_markers(&store, 0..commands).await, before);
        let mut log = TypedLogStore::<CounterConfig>::new(store.clone(), 1)
            .await
            .unwrap();
        assert_eq!(
            log.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|id| id.index),
            Some(last_index),
            "every acknowledged entry was in the synced log"
        );
        let committed = log.read_committed().await.unwrap().map(|id| id.index);
        assert!(committed.is_none_or(|index| index <= last_index));
        drop(log);
        drop(store);
        drop(pool);
    }

    let node = Node::open_with(&path, &layout, shared.clone(), Some(fault.clone())).await;
    node.lead(&layout, false).await;
    node.consensus
        .raft
        .wait(Some(Duration::from_secs(20)))
        .applied_index_at_least(Some(last_index), "replay the lost applications")
        .await
        .unwrap();
    assert_eq!(node.count().await, commands);
    assert_eq!(read_markers(&node.store, 0..commands).await, commands);
    // The replayed entries were journal values; every locator resolved.
    assert_eq!(node.pool.kv_store().stats().recycled_value_reads, 0);
    // A new write still applies exactly once after the replay.
    node.propose_all(commands..commands + 1).await;
    assert_eq!(node.count().await, commands + 1);
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
    // Real journal fsyncs (RV-T2 Q1), not synced write requests.
    let fsyncs = pool_after.synced_writes - pool_before.synced_writes;
    let requests = group_after.synced_requests - group_before.synced_requests;
    let deferred = group_after.deferred_writes - group_before.deferred_writes;
    eprintln!(
        "o06_o11 group commit: {commands} entries, {fsyncs} fsyncs, {requests} synced requests, {deferred} unsynced"
    );
    // Every application write is unsynced; commit hints add more.
    assert!(deferred >= commands, "{deferred} unsynced writes");
    // Before T2 this was about three fsyncs per entry; batches of up to 64
    // entries need a handful (RV-T2 Q3: a bound tied to the batch size).
    assert!(
        fsyncs <= commands.div_ceil(8) + 4,
        "{fsyncs} fsyncs for {commands} entries"
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
    let _deferred = store.defer_apply_sync(log.group()).unwrap();
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
    let before = pool.write_stats().synced_requests;
    log.purge(id(4)).await.unwrap();
    // RV-T2 Q2: no separate sync for the applied state. The synced purge record
    // follows the applied writes in the journal, so it makes them durable.
    assert_eq!(pool.write_stats().synced_requests - before, 1);
    assert_eq!(store.write_stats().unsynced_applied_index, None);
    assert!(!pool.has_unsynced_writes());
    assert_eq!(
        log.get_log_state().await.unwrap().last_purged_log_id,
        Some(id(4))
    );
    let before = pool.write_stats().synced_requests;
    log.purge(id(5)).await.unwrap();
    assert_eq!(pool.write_stats().synced_requests - before, 1);

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
    let before = store.write_stats().synced_requests;
    log.append(entries, IOFlushed::noop()).await.unwrap();
    assert_eq!(
        store.write_stats().synced_requests - before,
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

// NS7 / NS13 at pool level: Raft log entries are journal values.

const NS13_SEGMENT: u64 = 32 << 10;
const NS13_MAX_JOURNAL: u64 = 6 * NS13_SEGMENT;
const LAGGING: usize = 0;
const BUSY: usize = 1;

/// Small journal segments and budget. Log entries are journal values, so the
/// memtable holds only locators; the replay window alone decides when a flush runs
/// by itself, and it is kept above every phase of these workloads.
fn ns13_tune(options: &mut nabedra_kv::Options) {
    options.journal.segment_bytes = NS13_SEGMENT;
    options.journal.max_group_bytes = 16 << 10;
    options.max_journal_bytes = NS13_MAX_JOURNAL;
    options.max_replay_bytes = 8 << 20;
}

/// The expected retained log of each group: index -> the entry's encoded bytes.
#[derive(Clone, Default, PartialEq, Debug)]
struct LogOracle {
    entries: [BTreeMap<u64, Vec<u8>>; 2],
    purged: [Option<u64>; 2],
    next: [u64; 2],
}

/// Two typed logs (a lagging group and a busy one) in one pool on `FaultFs`.
struct LogRig {
    logs: Vec<TypedLogStore<CounterConfig>>,
    pool: OltpPool,
    shared: Arc<WorkerSharedResources>,
}
#[derive(Clone)]
struct LogLayout {
    pool: PoolIdentity,
    groups: [GroupIdentity; 2],
}
impl LogLayout {
    fn new() -> Self {
        let layout = Layout::new();
        let busy = GroupIdentity {
            group_id: Uuid::new_v4(),
            ..layout.group.clone()
        };
        Self {
            pool: layout.pool,
            groups: [layout.group, busy],
        }
    }
}
fn ns13_id(index: u64) -> LogId {
    log_id(AppliedPosition {
        term: 1,
        node_id: 1,
        index,
    })
}
impl LogRig {
    async fn open(
        path: &Path,
        layout: &LogLayout,
        fault: &FaultFs,
        retained: bool,
    ) -> Result<Self, String> {
        let shared = shared();
        let fs: Arc<dyn Fs> = Arc::new(fault.clone());
        let pool = OltpPool::open_development_tuned(
            path,
            layout.pool.clone(),
            shared.clone(),
            fs,
            ns13_tune,
        )
        .map_err(|error| error.to_string())?;
        let mut logs = Vec::new();
        for group in &layout.groups {
            let store = if retained {
                pool.retained_group(group.clone()).await
            } else {
                pool.group(group.clone()).await
            }
            .map_err(|error| error.to_string())?;
            logs.push(
                TypedLogStore::<CounterConfig>::new(store, 1)
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        Ok(Self { logs, pool, shared })
    }
    async fn append(&mut self, group: usize, oracle: &mut LogOracle) -> std::io::Result<()> {
        let index = oracle.next[group];
        let id = (group as u64) << 32 | index;
        let pad_len = 1200 + (index as usize * 37) % 600;
        let pad = char::from(b'a' + (index % 26) as u8)
            .to_string()
            .repeat(pad_len);
        let command =
            OwnedCommand::new_owned(Bump { id, pad }, &self.shared, ByteClass::Data).unwrap();
        let entry = OwnedRaftEntry::new(ns13_id(index), Payload::Normal(command));
        let bytes = serde_json::to_vec(&entry).unwrap();
        self.logs[group].append([entry], IOFlushed::noop()).await?;
        oracle.entries[group].insert(index, bytes);
        oracle.next[group] = index + 1;
        Ok(())
    }
    async fn purge(
        &mut self,
        group: usize,
        upto: u64,
        oracle: &mut LogOracle,
    ) -> std::io::Result<()> {
        self.logs[group].purge(ns13_id(upto)).await?;
        oracle.entries[group].retain(|index, _| *index > upto);
        oracle.purged[group] = Some(upto);
        Ok(())
    }
    /// The busy group appends `count` entries and keeps only its last four.
    async fn busy(
        &mut self,
        count: usize,
        flush_every: Option<usize>,
        oracle: &mut LogOracle,
    ) -> std::io::Result<()> {
        for n in 0..count {
            self.append(BUSY, oracle).await?;
            let last = oracle.next[BUSY] - 1;
            if last % 8 == 7 {
                self.purge(BUSY, last - 4, oracle).await?;
            }
            if flush_every.is_some_and(|every| n % every == every - 1) {
                self.maintain(Maintenance::Flush).await.unwrap();
                self.maintain(Maintenance::Gc).await.unwrap();
            }
        }
        Ok(())
    }
    /// Blocking store maintenance, off the async runtime.
    async fn maintain(&self, step: Maintenance) -> Result<(), nabedra_kv::Error> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let store = pool.kv_store();
            match step {
                Maintenance::Flush => store.flush(),
                Maintenance::Compact => store.compact(),
                Maintenance::Gc => store.gc_journal().map(|_| ()),
            }
        })
        .await
        .unwrap()
    }
    fn stats(&self) -> nabedra_kv::StoreStats {
        self.pool.kv_store().stats()
    }
    /// Every retained entry of both groups, read back through the typed log
    /// (point gets that resolve journal locators), against the oracle.
    async fn check(&mut self, oracle: &LogOracle, context: &str) {
        for group in [LAGGING, BUSY] {
            let log = &mut self.logs[group];
            let mut stream = Box::pin(log.entries_stream(..).await);
            let mut found = BTreeMap::new();
            while let Some(entry) = futures_util::StreamExt::next(&mut stream).await {
                let entry = entry.unwrap_or_else(|error| panic!("{context}: read: {error}"));
                found.insert(entry.index(), serde_json::to_vec(&entry).unwrap());
            }
            drop(stream);
            assert!(
                found == oracle.entries[group],
                "{context}: group {group} retained {:?}, expected {:?}",
                found.keys().collect::<Vec<_>>(),
                oracle.entries[group].keys().collect::<Vec<_>>()
            );
            let state = log.get_log_state().await.unwrap();
            assert_eq!(
                state.last_purged_log_id.map(|id| id.index),
                oracle.purged[group],
                "{context}: group {group} purge marker"
            );
            let last = oracle.entries[group]
                .keys()
                .next_back()
                .copied()
                .or(oracle.purged[group]);
            assert_eq!(
                state.last_log_id.map(|id| id.index),
                last,
                "{context}: group {group} tail"
            );
        }
        assert_eq!(self.stats().recycled_value_reads, 0, "{context}");
    }
}
#[derive(Clone, Copy, Debug)]
enum Maintenance {
    Flush,
    Compact,
    Gc,
}
/// Live journal segment numbers (`<n>.jnl`), oldest first.
fn live_segments(fault: &FaultFs, path: &Path) -> Vec<u64> {
    let mut live: Vec<u64> = fault
        .list(&path.join("kv").join("journal"))
        .unwrap()
        .iter()
        .filter(|file| file.extension().is_some_and(|ext| ext == "jnl"))
        .filter_map(|file| file.file_stem()?.to_str()?.parse().ok())
        .collect();
    live.sort_unstable();
    live
}
fn lag_bytes(oracle: &LogOracle) -> u64 {
    oracle.entries[LAGGING]
        .values()
        .map(|bytes| bytes.len() as u64)
        .sum()
}

/// NS13, O06, O11 at pool level. A lagging group's unpurged entries pin the
/// oldest journal segment, and with it every later one (retirement keeps every
/// segment at or above the lowest one a run or memtable points into). Once the
/// journal passes `max_journal_bytes`, GC relocates the lagging entries
/// (`relocated_bytes` counts at least their bytes) and the old segments recycle,
/// so the journal stays near its budget. After the lagging group purges, its
/// relocated copies are dead too. Log reads match an oracle after every phase,
/// across flush, compaction, journal GC, and two crash-model reopens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns13_o06_o11_lagging_group_pins_the_oldest_segment_until_gc_relocates_it() {
    let dir = Dir::new();
    let path = dir.0.join("pool");
    std::fs::create_dir_all(&path).unwrap();
    let fault = FaultFs::new();
    fault.create_dir_all_durable(&path);
    let layout = LogLayout::new();
    let mut oracle = LogOracle::default();
    let mut rig = LogRig::open(&path, &layout, &fault, false).await.unwrap();

    // Phase 1: the lagging group appends and stops; the busy group runs and
    // purges behind itself, inside the budget. The oldest segment stays.
    for _ in 0..4 {
        rig.append(LAGGING, &mut oracle).await.unwrap();
    }
    let lagging = lag_bytes(&oracle);
    let pinned = live_segments(&fault, &path)[0];
    rig.busy(24, Some(8), &mut oracle).await.unwrap();
    rig.maintain(Maintenance::Flush).await.unwrap();
    rig.maintain(Maintenance::Gc).await.unwrap();
    rig.check(&oracle, "phase 1").await;
    let live = live_segments(&fault, &path);
    eprintln!(
        "NS13 phase 1: live segments {live:?}, stats {:?}",
        rig.stats()
    );
    assert_eq!(live[0], pinned, "the lagging group pins the oldest segment");
    assert_eq!(
        rig.stats().relocated_bytes,
        0,
        "inside the budget nothing moves"
    );

    // Phase 2: the busy group writes far past the budget. GC relocates the
    // lagging entries out of the oldest segment and the old segments recycle.
    rig.busy(160, Some(8), &mut oracle).await.unwrap();
    rig.maintain(Maintenance::Flush).await.unwrap();
    rig.maintain(Maintenance::Compact).await.unwrap();
    rig.maintain(Maintenance::Gc).await.unwrap();
    rig.check(&oracle, "phase 2").await;
    let live = live_segments(&fault, &path);
    let stats = rig.stats();
    eprintln!("NS13 phase 2: live segments {live:?}, {lagging} lagging bytes, stats {stats:?}");
    assert!(
        stats.journal_bytes > 3 * NS13_MAX_JOURNAL,
        "the busy group wrote well past the journal budget"
    );
    assert!(
        live[0] > pinned,
        "GC moved the lagging entries off segment {pinned}"
    );
    assert!(
        stats.relocated_bytes >= lagging,
        "relocated {} bytes, the lagging group holds {lagging}",
        stats.relocated_bytes
    );
    assert!(
        live.len() as u64 * NS13_SEGMENT <= NS13_MAX_JOURNAL + 3 * NS13_SEGMENT,
        "the journal stays near its budget: {live:?}"
    );

    // Phase 3: the lagging group purges and the busy group runs on. The
    // journal still recycles and reads stay exact.
    rig.purge(LAGGING, 3, &mut oracle).await.unwrap();
    rig.busy(160, Some(8), &mut oracle).await.unwrap();
    rig.maintain(Maintenance::Flush).await.unwrap();
    rig.maintain(Maintenance::Compact).await.unwrap();
    rig.maintain(Maintenance::Gc).await.unwrap();
    rig.check(&oracle, "phase 3").await;
    let live = live_segments(&fault, &path);
    eprintln!(
        "NS13 phase 3: live segments {live:?}, stats {:?}",
        rig.stats()
    );
    assert!(live.len() as u64 * NS13_SEGMENT <= NS13_MAX_JOURNAL + 3 * NS13_SEGMENT);

    // A last lagging entry stays unflushed in the memtable, then the machine
    // crashes: the reopen replays it and reads every entry back.
    rig.append(LAGGING, &mut oracle).await.unwrap();
    drop(rig);
    let crashed = fault.crash();
    let mut rig = LogRig::open(&path, &layout, &crashed, true).await.unwrap();
    assert!(rig.pool.kv_store().quarantined().is_empty());
    rig.check(&oracle, "reopen").await;
    drop(rig);
    let mut rig = LogRig::open(&path, &layout, &crashed.crash(), true)
        .await
        .unwrap();
    rig.check(&oracle, "second reopen").await;
}

/// The NS13 relocation workload up to the GC window: lagging entries in the
/// oldest segment, then a busy tail far past the budget with no flush.
async fn ns13_relocation_prefix(rig: &mut LogRig, oracle: &mut LogOracle) {
    for _ in 0..4 {
        rig.append(LAGGING, oracle).await.unwrap();
    }
    rig.busy(16, Some(8), oracle).await.unwrap();
    rig.busy(48, None, oracle).await.unwrap();
}
const NS13_WINDOW: [Maintenance; 4] = [
    Maintenance::Flush,
    Maintenance::Gc,
    Maintenance::Compact,
    Maintenance::Gc,
];

/// NS04, NS13, O11 at pool level: a `FaultFs` crash at every mutating operation
/// of a journal-GC window (relocation, the flush that lets it retire, the
/// compaction and the retirement) recovers the exact log, both groups, with no
/// dangling locator, and a second crash-reopen reads the same. The window writes
/// no log entry, so the oracle is exact. Crash points are counted from the start
/// of the window; background flushes and compactions may shift a point by a few
/// operations between runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns04_ns13_o11_crash_at_every_relocation_step_keeps_the_log() {
    // Reference run: the window's length, and that it relocates.
    let (window, relocated) = {
        let dir = Dir::new();
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let fault = FaultFs::new();
        fault.create_dir_all_durable(&path);
        let layout = LogLayout::new();
        let mut oracle = LogOracle::default();
        let mut rig = LogRig::open(&path, &layout, &fault, false).await.unwrap();
        ns13_relocation_prefix(&mut rig, &mut oracle).await;
        let start = fault.mutating_ops();
        let relocated_before = rig.stats().relocated_bytes;
        for step in NS13_WINDOW {
            rig.maintain(step).await.unwrap();
        }
        let window = fault.mutating_ops() - start;
        let relocated = rig.stats().relocated_bytes - relocated_before;
        rig.check(&oracle, "reference").await;
        (window, relocated)
    };
    eprintln!(
        "NS04/NS13 pool: window of {window} mutating operations, {relocated} bytes relocated"
    );
    assert!(relocated > 0, "the window relocates");
    let mut crashed_runs = 0;
    for k in 0..=window + 4 {
        let dir = Dir::new();
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let fault = FaultFs::new();
        fault.create_dir_all_durable(&path);
        let layout = LogLayout::new();
        let mut oracle = LogOracle::default();
        let mut rig = LogRig::open(&path, &layout, &fault, false).await.unwrap();
        ns13_relocation_prefix(&mut rig, &mut oracle).await;
        fault.crash_after(fault.mutating_ops() + k);
        for step in NS13_WINDOW {
            if rig.maintain(step).await.is_err() {
                break;
            }
        }
        crashed_runs += usize::from(fault.is_crashed());
        drop(rig);
        let context = format!("crash after window op {k}");
        let image = fault.crash();
        let mut rig = LogRig::open(&path, &layout, &image, true)
            .await
            .unwrap_or_else(|error| panic!("{context}: reopen: {error}"));
        assert!(rig.pool.kv_store().quarantined().is_empty(), "{context}");
        rig.check(&oracle, &context).await;
        drop(rig);
        let mut rig = LogRig::open(&path, &layout, &image.crash(), true)
            .await
            .unwrap_or_else(|error| panic!("{context}: second reopen: {error}"));
        rig.check(&oracle, &format!("{context}, second reopen"))
            .await;
    }
    eprintln!(
        "NS04/NS13 pool: {} crash points, {crashed_runs} crashed inside the window",
        window + 5
    );
    assert!(
        crashed_runs as u64 * 10 >= window * 9,
        "the crash points fell inside the window"
    );
}

/// NS13 write amplification: bytes the store writes (journal, flush and
/// compaction) per byte of Raft log appended, from store stats. Appends are
/// synced batches of 16 entries; the follower purges `lag` entries behind its
/// tail. With entries as journal values the LSM sees 32-byte locators, so flush
/// and compaction write almost nothing per log byte even when purges lag past
/// a flush. Printed for the NS7 report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns13_raft_log_write_amplification_from_store_stats() {
    fn tune(options: &mut nabedra_kv::Options) {
        options.memtable_bytes = 1 << 20;
        options.max_replay_bytes = 2 << 20;
    }
    let mut lsm = Vec::new();
    for lag in [64u64, 2048] {
        let dir = Dir::new();
        let path = dir.0.join("pool");
        std::fs::create_dir_all(&path).unwrap();
        let fault = FaultFs::new();
        fault.create_dir_all_durable(&path);
        let layout = Layout::new();
        let shared = shared();
        let fs: Arc<dyn Fs> = Arc::new(fault.clone());
        let pool =
            OltpPool::open_development_tuned(&path, layout.pool.clone(), shared.clone(), fs, tune)
                .unwrap();
        let store = pool.group(layout.group.clone()).await.unwrap();
        let mut log = TypedLogStore::<CounterConfig>::new(store, 1).await.unwrap();
        let before = pool.kv_store().stats();
        let mut log_bytes = 0u64;
        let mut next = 0u64;
        for _ in 0..256 {
            let entries: Vec<_> = (0..16)
                .map(|n| {
                    let index = next + n;
                    let command = OwnedCommand::new_owned(
                        Bump {
                            id: index,
                            pad: "w".repeat(1200 + (index as usize * 37) % 600),
                        },
                        &shared,
                        ByteClass::Data,
                    )
                    .unwrap();
                    OwnedRaftEntry::new(ns13_id(index), Payload::Normal(command))
                })
                .collect();
            log_bytes += entries
                .iter()
                .map(|entry| serde_json::to_vec(entry).unwrap().len() as u64)
                .sum::<u64>();
            log.append(entries, IOFlushed::noop()).await.unwrap();
            next += 16;
            if next > lag {
                log.purge(ns13_id(next - lag - 1)).await.unwrap();
            }
        }
        let flushed = pool.clone();
        tokio::task::spawn_blocking(move || {
            flushed.kv_store().flush().unwrap();
            flushed.kv_store().compact().unwrap();
        })
        .await
        .unwrap();
        let after = pool.kv_store().stats();
        let journal = after.journal_bytes - before.journal_bytes;
        let flush = after.flush_bytes - before.flush_bytes;
        let compaction = after.compaction_bytes - before.compaction_bytes;
        let relocated = after.relocated_bytes - before.relocated_bytes;
        let total = journal + flush + compaction;
        eprintln!(
            "NS7 write amplification, purge lag {lag}: {log_bytes} log bytes appended; \
             journal {journal} ({:.2}x), flush {flush} ({:.3}x), compaction {compaction} ({:.3}x), \
             relocated {relocated}; total {:.2}x",
            journal as f64 / log_bytes as f64,
            flush as f64 / log_bytes as f64,
            compaction as f64 / log_bytes as f64,
            total as f64 / log_bytes as f64,
        );
        lsm.push((lag, flush + compaction, log_bytes));
    }
    // Before NS7 the LSM wrote 0.21x (lag 64) and 2.16x (lag 2048) of the log
    // bytes; with journal values it writes 0.01x and 0.11x (the retained IDs,
    // tail markers and locators).
    for (lag, written, log_bytes) in lsm {
        assert!(
            written * 5 < log_bytes,
            "purge lag {lag}: the LSM wrote {written} bytes for {log_bytes} log bytes"
        );
    }
}

/// T1b: real typed Raft voters over mTLS worker transports.
#[path = "replicated_tests.rs"]
mod replicated;
