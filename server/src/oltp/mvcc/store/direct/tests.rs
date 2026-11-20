//! Store-level evidence for direct commits (plan sections 4 and 5): real
//! group stores, model consensus (sequential apply positions, no Raft).
use super::*;
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::{Change, PreparedWrite, index::IndexValue},
        pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
    },
    oltp_replication::{
        authority::status::VerifiedStatusDecision,
        owned::OwnedCommand,
        types::{DecisionStatement, Page, Text, Timestamp, TxnDecision},
    },
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::Duration,
};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}
fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}
fn full_span() -> KeySpan {
    KeySpan {
        start: None,
        end: None,
    }
}
fn shared() -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap()
}
fn json(shared: &WorkerSharedResources, value: serde_json::Value) -> CanonicalJson {
    CanonicalJson::from_serializable(shared, &value, crate::protocol::MAX_BODY_BYTES).unwrap()
}
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 32 << 20,
    batch_bytes: 32 << 20,
    output_bytes: 64 << 10,
};

/// One replica of one Data group: a machine plus its next apply index.
struct Replica {
    machine: ParticipantMachine,
    shared: Arc<WorkerSharedResources>,
    index: u64,
}
impl Replica {
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        let index = self.index;
        self.index += 1;
        apply_at(&self.machine, &self.shared, index, command).await
    }
    async fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        dump(&self.machine).await
    }
}

async fn apply_at(
    machine: &ParticipantMachine,
    shared: &WorkerSharedResources,
    index: u64,
    command: ParticipantCommand,
) -> ParticipantReply {
    let command = OwnedCommand::new_owned(command, shared, ByteClass::Data).unwrap();
    let digest = crate::oltp_replication::owned::serialized_digest(&command).unwrap();
    let raw = machine
        .apply(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index,
                },
                command_digest: digest,
                membership: None,
            },
            command,
        )
        .await
        .unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

/// Every key and value in the group's applied-state namespace. The applied
/// position and reply live in separate metadata keys, so a rejected entry
/// leaves this dump unchanged.
async fn dump(machine: &ParticipantMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let raw = machine
        .store()
        .read(BUDGET, |view| {
            let mut out = Vec::new();
            let mut cursor = Vec::new();
            while let Some(record) = view.first_from(&[], &cursor)? {
                out.push((record.key().to_vec(), record.value().to_vec()));
                cursor = record.key().to_vec();
                cursor.push(0);
            }
            view.encode(&out)
        })
        .await
        .unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}
fn count_tag(dump: &[(Vec<u8>, Vec<u8>)], tag: u8) -> usize {
    dump.iter()
        .filter(|(key, _)| key.first() == Some(&tag))
        .count()
}

struct Fixture {
    replica: Replica,
    pool: OltpPool,
    pool_identity: PoolIdentity,
    group: GroupIdentity,
    authority: Authority,
    table: Uuid,
    catalog_group_id: Uuid,
    /// The exact first entry, so another replica can apply the same log.
    first_fence: ParticipantCommand,
    dir: Dir,
}
/// A replica in its own pool: keeps the pool and directory alive.
struct OtherReplica {
    replica: Replica,
    _pool: OltpPool,
    _dir: Dir,
}
impl std::ops::Deref for OtherReplica {
    type Target = Replica;
    fn deref(&self) -> &Replica {
        &self.replica
    }
}
impl std::ops::DerefMut for OtherReplica {
    fn deref_mut(&mut self) -> &mut Replica {
        &mut self.replica
    }
}
impl Fixture {
    /// Another worker's replica of this group, with the fence applied.
    async fn replica_of(&self) -> OtherReplica {
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", Uuid::new_v4())));
        let mut identity = self.pool_identity.clone();
        identity.worker_id = Uuid::new_v4();
        identity.pool_id = Uuid::new_v4();
        // Another worker: its own storage and memory budgets.
        let shared = shared();
        let pool = OltpPool::open_development(&dir.0, identity, shared.clone()).unwrap();
        let store = pool.group(self.group.clone()).await.unwrap();
        let mut replica = Replica {
            machine: ParticipantMachine::new(store).unwrap(),
            shared,
            index: 0,
        };
        assert!(matches!(
            replica.submit(self.first_fence.clone()).await,
            ParticipantReply::Fenced(_)
        ));
        OtherReplica {
            replica,
            _pool: pool,
            _dir: dir,
        }
    }
    async fn new() -> Self {
        Self::with(Uuid::new_v4(), full_span()).await
    }
    async fn with(table: Uuid, span: KeySpan) -> Self {
        let shared = shared();
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", Uuid::new_v4())));
        let pool_identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        };
        let pool =
            OltpPool::open_development(&dir.0, pool_identity.clone(), shared.clone()).unwrap();
        let group = GroupIdentity {
            cluster_id: pool_identity.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: table,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: 1,
        };
        let store = pool.group(group.clone()).await.unwrap();
        let authority = Authority {
            run_id: Uuid::new_v4(),
            owner_term: 1,
        };
        let catalog_group_id = Uuid::new_v4();
        let first_fence = ParticipantCommand::Fence {
            transition_id: Uuid::new_v4(),
            expected: None,
            next: authority,
            schema_epoch: 1,
            routing_epoch: 1,
            span,
            catalog_group_id,
        };
        let mut f = Self {
            replica: Replica {
                machine: ParticipantMachine::new(store).unwrap(),
                shared,
                index: 0,
            },
            pool,
            pool_identity,
            group,
            authority,
            table,
            catalog_group_id,
            first_fence: first_fence.clone(),
            dir,
        };
        let fence = f.first_fence.clone();
        assert!(matches!(f.submit(fence).await, ParticipantReply::Fenced(_)));
        f
    }
    fn fence(&self, schema_epoch: u64, routing_epoch: u64, span: KeySpan) -> ParticipantCommand {
        ParticipantCommand::Fence {
            transition_id: Uuid::new_v4(),
            expected: None,
            next: self.authority,
            schema_epoch,
            routing_epoch,
            span,
            catalog_group_id: self.catalog_group_id,
        }
    }
    fn machine(&self) -> &ParticipantMachine {
        &self.replica.machine
    }
    fn shared(&self) -> &Arc<WorkerSharedResources> {
        &self.replica.shared
    }
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        self.replica.submit(command).await
    }
    async fn direct(&mut self, commit: &DirectCommit) -> ParticipantReply {
        self.submit(ParticipantCommand::Direct(commit.clone()))
            .await
    }
    async fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.replica.dump().await
    }
    fn key(&self, key: i64) -> MvccKey {
        MvccKey::Row {
            table_id: self.table,
            primary_key: Key::Int64(key),
        }
    }
    fn json(&self, value: serde_json::Value) -> CanonicalJson {
        json(self.shared(), value)
    }
    fn write(
        &self,
        key: i64,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
    ) -> DirectWrite {
        DirectWrite {
            key: self.key(key),
            expected_version: expected.map(ts),
            value: value.map(|value| self.json(value)),
            index: Vec::new(),
        }
    }
    fn commit(
        &self,
        request_id: Uuid,
        body: u8,
        commit_ts: u64,
        writes: Vec<DirectWrite>,
    ) -> DirectCommit {
        DirectCommit {
            authority: self.authority,
            table_id: self.table,
            schema_epoch: 1,
            routing_epoch: 1,
            request_id,
            digest: digest(body),
            commit_ts: ts(commit_ts),
            writes,
            result: self.json(serde_json::json!({"request": request_id, "ts": commit_ts})),
            applied_unix_ms: 1_000 + commit_ts,
        }
    }
    /// A one-row upsert of `key` committed at `commit_ts`.
    fn put(&self, key: i64, expected: Option<u64>, commit_ts: u64) -> DirectCommit {
        self.commit(
            Uuid::new_v4(),
            1,
            commit_ts,
            vec![self.write(
                key,
                expected,
                Some(serde_json::json!({"id": key, "ts": commit_ts})),
            )],
        )
    }
    async fn read(&self, s: u64, key: i64) -> serde_json::Value {
        let raw = self.machine().read_at(ts(s), self.key(key)).await.unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
    async fn version(&self, s: u64, key: i64) -> Option<u64> {
        self.read(s, key).await["value"]["version"].as_u64()
    }
    /// Rule 3 for the fixture: this node leads term 1, which already applied.
    fn lead(&self) {
        self.machine().range_clock().lead(1);
        assert!(self.machine().range_clock().serving());
    }

    // Queued-path helpers, as in the participant store tests.
    fn queued_plan(&self, read: u64, writes: &[PreparedWrite]) -> QueuedPlan {
        let txn = TxnId {
            request_id: Uuid::new_v4(),
            attempt: 0,
        };
        let request = RequestBinding {
            run_id: self.authority.run_id,
            request_id: txn.request_id,
            digest: digest(1),
            accepted_context: digest(2),
        };
        let manifest = ManifestRef {
            status_group_id: Uuid::new_v4(),
            digest: digest(3),
            participant_count: 1,
        };
        let mut plan = ParticipantPlan {
            group: self.group.clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(read),
            write_root: digest(0),
            write_count: writes.len() as u64,
            page_count: 1,
        };
        let root = initial_root(txn, &plan).unwrap();
        plan.write_root = page_root(root, 0, writes).unwrap();
        QueuedPlan {
            txn,
            request,
            manifest,
            plan,
            root,
        }
    }
    async fn prepare(&mut self, p: &QueuedPlan, writes: Vec<PreparedWrite>) -> ParticipantReply {
        let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id: Uuid::new_v4(),
                catalog_group_id: self.catalog_group_id,
                table_id: self.table,
                lifecycle_version: None,
                schema_epoch: 1,
                txn: p.txn,
                request: p.request,
                read_ts: p.plan.read_ts,
                authority: self.authority,
            },
            lifecycle::GuardMode::Shared,
        )
        .into_sealed();
        self.submit(ParticipantCommand::PreparePage {
            authority: self.authority,
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            plan: p.plan.clone(),
            page_index: 0,
            previous_root: p.root,
            guards: Page::new(vec![guard]).unwrap(),
            writes: Page::new(writes).unwrap(),
        })
        .await
    }
    async fn seal_and_resolve(&mut self, p: &QueuedPlan, commit_ts: u64) {
        assert!(matches!(
            self.submit(ParticipantCommand::Seal {
                authority: self.authority,
                txn: p.txn,
                manifest: p.manifest,
                result_digest: digest(4),
            })
            .await,
            ParticipantReply::Sealed(_)
        ));
        let proof = VerifiedStatusDecision::model_for_test(DecisionStatement {
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
            decision: TxnDecision::Commit {
                commit_ts: ts(commit_ts),
            },
        })
        .unwrap()
        .into_sealed();
        assert!(matches!(
            self.submit(ParticipantCommand::Resolve {
                authority: self.authority,
                proof,
                max_items: 128,
            })
            .await,
            ParticipantReply::Resolved { complete: true, .. }
        ));
    }
    fn queued_put(
        &self,
        key: i64,
        expected: Option<u64>,
        value: serde_json::Value,
    ) -> PreparedWrite {
        PreparedWrite {
            key: self.key(key),
            expected_version: expected.map(ts),
            change: Change::Put(self.json(value)),
        }
    }
}
struct QueuedPlan {
    txn: TxnId,
    request: RequestBinding,
    manifest: ManifestRef,
    plan: ParticipantPlan,
    root: Digest,
}

fn applied(commit_ts: u64) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Applied {
        commit_ts: ts(commit_ts),
    })
}
fn rejected(fault: Fault) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Rejected(fault))
}
fn replayed(commit: &DirectCommit) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Replayed {
        result: DirectResult(RawValue::from_string(commit.result.as_str().to_owned()).unwrap()),
    })
}

/// Submit `commit` and assert it is rejected with `fault` and writes nothing.
async fn assert_rejected_without_writes(f: &mut Fixture, commit: &DirectCommit, fault: Fault) {
    let before = f.dump().await;
    assert_eq!(f.direct(commit).await, rejected(fault));
    assert_eq!(
        f.dump().await,
        before,
        "a rejected direct commit wrote state"
    );
}

// ---------------------------------------------------------------------------
// Apply and dedup (O11, D03)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o11_d03_direct_commit_writes_versions_heads_index_and_one_dedup_record() {
    let mut f = Fixture::new().await;
    let index_key = MvccKey::IndexEntry {
        table_id: f.table,
        index_id: Uuid::new_v4(),
        values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
        primary_key: Key::Int64(1),
    };
    let mut write = f.write(1, None, Some(serde_json::json!({"id": 1, "v": 7})));
    write.index.push(IndexChange {
        key: index_key.clone(),
        expected_version: None,
        value: Some(f.json(serde_json::json!({"id": 1}))),
    });
    let commit = f.commit(Uuid::new_v4(), 1, 10, vec![write]);
    assert_eq!(f.direct(&commit).await, applied(10));

    assert_eq!(f.version(10, 1).await, Some(10));
    assert_eq!(f.read(10, 1).await["value"]["value"]["v"], 7);
    assert_eq!(
        f.version(9, 1).await,
        None,
        "nothing visible below the commit"
    );
    let index = f.machine().read_at(ts(10), index_key).await.unwrap();
    let index: serde_json::Value = serde_json::from_slice(index.as_slice()).unwrap();
    assert_eq!(index["value"]["version"], 10);

    let state = f.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 1, "exactly one dedup record");
    assert_eq!(count_tag(&state, EXPIRY), 1, "and its expiry index entry");
    assert_eq!(count_tag(&state, VALUE), 2, "row and index versions");
    assert_eq!(count_tag(&state, HEAD), 2, "row and index heads");
}

#[tokio::test]
async fn d03_exact_retry_replays_the_original_result_without_a_second_version() {
    let mut f = Fixture::new().await;
    let first = f.put(1, None, 10);
    assert_eq!(f.direct(&first).await, applied(10));
    let state = f.dump().await;
    // The gateway resends the same id and body; the new leader computed it
    // again from stale state with a new timestamp.
    let mut retry = first.clone();
    retry.commit_ts = ts(20);
    retry.result = f.json(serde_json::json!({"recomputed": true}));
    assert_eq!(f.direct(&retry).await, replayed(&first));
    assert_eq!(f.dump().await, state, "a replay writes nothing");
    assert_eq!(f.version(25, 1).await, Some(10));
}

#[tokio::test]
async fn d03_same_id_different_body_conflicts_on_the_same_key_and_executes_on_another_key() {
    let mut f = Fixture::new().await;
    let id = Uuid::new_v4();
    let first = f.commit(
        id,
        1,
        10,
        vec![f.write(1, None, Some(serde_json::json!({"a": 1})))],
    );
    assert_eq!(f.direct(&first).await, applied(10));
    // Same id, different body, same anchor key: the existing conflict.
    let same_key = f.commit(
        id,
        2,
        11,
        vec![f.write(1, Some(10), Some(serde_json::json!({"a": 2})))],
    );
    assert_rejected_without_writes(&mut f, &same_key, Fault::Conflict).await;
    assert_eq!(f.read(15, 1).await["value"]["value"]["a"], 1);
    // Same id, different body, another anchor: detection is per table and
    // key, so this executes (plan decision "Request IDs").
    let other_key = f.commit(
        id,
        2,
        12,
        vec![f.write(2, None, Some(serde_json::json!({"a": 3})))],
    );
    assert_eq!(f.direct(&other_key).await, applied(12));
    assert_eq!(count_tag(&f.dump().await, DEDUP), 2);
    // Its own retry replays its own result.
    assert_eq!(f.direct(&other_key).await, replayed(&other_key));
}

#[tokio::test]
async fn d03_multi_row_commit_anchors_its_dedup_record_on_the_smallest_key() {
    let mut f = Fixture::new().await;
    let commit = f.commit(
        Uuid::new_v4(),
        1,
        10,
        vec![
            f.write(3, None, Some(serde_json::json!({"id": 3}))),
            f.write(8, None, Some(serde_json::json!({"id": 8}))),
        ],
    );
    assert_eq!(f.direct(&commit).await, applied(10));
    let anchor = physical(DEDUP, &commit.dedup_key().unwrap()).unwrap();
    assert!(f.dump().await.iter().any(|(key, _)| *key == anchor));
    assert_eq!(commit.keys(), vec![f.key(3), f.key(8)]);
}

// ---------------------------------------------------------------------------
// Apply-order validation: one test per rejection, nothing written (O07)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o07_direct_rejects_an_uninitialized_group() {
    let f = Fixture::new().await;
    let mut group = f.group.clone();
    group.group_id = Uuid::new_v4();
    let store = f.pool.group(group).await.unwrap();
    let mut empty = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: 0,
    };
    let reply = empty
        .submit(ParticipantCommand::Direct(f.put(1, None, 10)))
        .await;
    assert_eq!(reply, rejected(Fault::NotInitialized));
    assert!(empty.dump().await.is_empty());
}

#[tokio::test]
async fn o07_direct_rejects_malformed_commands() {
    let mut f = Fixture::new().await;
    let mut nil = f.put(1, None, 10);
    nil.request_id = Uuid::nil();
    let unsorted = f.commit(
        Uuid::new_v4(),
        1,
        10,
        vec![f.write(2, None, None), f.write(1, None, None)],
    );
    let empty = f.commit(Uuid::new_v4(), 1, 10, Vec::new());
    let mut other_table = f.put(1, None, 10);
    other_table.table_id = Uuid::new_v4();
    let mut stray_index = f.write(1, None, None);
    stray_index.index.push(IndexChange {
        key: MvccKey::IndexEntry {
            table_id: f.table,
            index_id: Uuid::new_v4(),
            values: Page::new(vec![IndexValue::Int64(1)]).unwrap(),
            primary_key: Key::Int64(2),
        },
        expected_version: None,
        value: None,
    });
    let stray_index = f.commit(Uuid::new_v4(), 1, 10, vec![stray_index]);
    let mut not_a_row = f.write(1, None, None);
    not_a_row.key = MvccKey::CatalogTable { table_id: f.table };
    let not_a_row = f.commit(Uuid::new_v4(), 1, 10, vec![not_a_row]);
    for command in [nil, unsorted, empty, other_table, stray_index, not_a_row] {
        assert_rejected_without_writes(&mut f, &command, Fault::InvalidBinding).await;
    }
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_authority() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.authority.owner_term += 1;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleAuthority).await;
}

#[tokio::test]
async fn o07_o08_direct_rejects_a_stale_schema_epoch() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.schema_epoch = 2;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleEpoch).await;
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_routing_epoch() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.routing_epoch = 2;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleEpoch).await;
}

#[tokio::test]
async fn o07_direct_rejects_keys_outside_the_group_span() {
    let mut f = Fixture::with(
        Uuid::new_v4(),
        KeySpan {
            start: Some(Key::Int64(0)),
            end: Some(Key::Int64(100)),
        },
    )
    .await;
    let outside = f.put(100, None, 10);
    assert_rejected_without_writes(&mut f, &outside, Fault::InvalidBinding).await;
    // One key inside, one outside.
    let straddling = f.commit(
        Uuid::new_v4(),
        1,
        10,
        vec![f.write(5, None, None), f.write(-1, None, None)],
    );
    let straddling = DirectCommit {
        writes: straddling.writes.into_iter().rev().collect(),
        ..straddling
    };
    assert_rejected_without_writes(&mut f, &straddling, Fault::InvalidBinding).await;
    assert_eq!(f.direct(&f.put(99, None, 10)).await, applied(10));
}

#[tokio::test]
async fn o07_d08_direct_rejects_a_key_with_a_queued_intent() {
    let mut f = Fixture::new().await;
    let write = f.queued_put(1, None, serde_json::json!({"queued": true}));
    let plan = f.queued_plan(10, std::slice::from_ref(&write));
    assert!(matches!(
        f.prepare(&plan, vec![write]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    let command = f.put(1, None, 11);
    assert_rejected_without_writes(&mut f, &command, Fault::Incomplete).await;
    // Once the queued transaction commits, the head moved: a direct commit
    // must name the queued version.
    f.seal_and_resolve(&plan, 12).await;
    assert_eq!(f.direct(&f.put(1, Some(12), 13)).await, applied(13));
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_expected_version() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    let missing = f.put(1, None, 11);
    assert_rejected_without_writes(&mut f, &missing, Fault::Conflict).await;
    let older = f.put(1, Some(9), 11);
    assert_rejected_without_writes(&mut f, &older, Fault::Conflict).await;
    // A deleted row keeps its tombstone's timestamp as its head version.
    let delete = f.commit(Uuid::new_v4(), 1, 11, vec![f.write(1, Some(10), None)]);
    assert_eq!(f.direct(&delete).await, applied(11));
    assert_eq!(
        f.read(12, 1).await["value"]["value"],
        serde_json::Value::Null
    );
    let as_absent = f.put(1, None, 12);
    assert_rejected_without_writes(&mut f, &as_absent, Fault::Conflict).await;
    assert_eq!(f.direct(&f.put(1, Some(11), 12)).await, applied(12));
}

#[tokio::test]
async fn o07_direct_rejects_a_commit_ts_not_above_the_newest_version() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    for commit_ts in [10, 9] {
        let command = f.put(1, Some(10), commit_ts);
        assert_rejected_without_writes(&mut f, &command, Fault::Conflict).await;
    }
    assert_eq!(f.direct(&f.put(1, Some(10), 11)).await, applied(11));
}

#[tokio::test]
async fn o07_direct_validation_follows_the_plan_order() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(2, None, 10)).await, applied(10));
    let write = f.queued_put(1, None, serde_json::json!({}));
    let plan = f.queued_plan(10, std::slice::from_ref(&write));
    f.prepare(&plan, vec![write]).await;
    // Key 1 has an intent; key 2 has head 10. Each command breaks several
    // rules and must report the earliest one.
    let mut all = f.commit(Uuid::new_v4(), 1, 5, vec![f.write(1, Some(3), None)]);
    all.authority.owner_term += 1;
    all.schema_epoch = 9;
    assert_rejected_without_writes(&mut f, &all, Fault::StaleAuthority).await;
    all.authority = f.authority;
    assert_rejected_without_writes(&mut f, &all, Fault::StaleEpoch).await;
    all.schema_epoch = 1;
    assert_rejected_without_writes(&mut f, &all, Fault::Incomplete).await;
    let version_then_ts = f.put(2, Some(3), 5);
    assert_rejected_without_writes(&mut f, &version_then_ts, Fault::Conflict).await;
}

// ---------------------------------------------------------------------------
// Queued prepares see direct commits as committed versions (O07)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o07_queued_prepare_conflicts_on_a_direct_commit_and_reads_it_as_committed() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(5, None, 11)).await, applied(11));
    // A queued plan whose snapshot predates the direct commit conflicts.
    let stale = f.queued_put(5, None, serde_json::json!({"queued": "stale"}));
    let plan = f.queued_plan(10, std::slice::from_ref(&stale));
    assert_eq!(
        f.prepare(&plan, vec![stale]).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    // One that read the direct version prepares and commits above it.
    let fresh = f.queued_put(5, Some(11), serde_json::json!({"queued": "fresh"}));
    let plan = f.queued_plan(12, std::slice::from_ref(&fresh));
    assert!(matches!(
        f.prepare(&plan, vec![fresh]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    f.seal_and_resolve(&plan, 13).await;
    assert_eq!(f.read(12, 5).await["value"]["value"]["ts"], 11);
    assert_eq!(f.read(13, 5).await["value"]["value"]["queued"], "fresh");
}

// ---------------------------------------------------------------------------
// Crash replay (O11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o11_crash_replay_of_a_direct_commit_entry_is_idempotent() {
    let f = Fixture::new().await;
    let commit = f.put(1, None, 10);
    let command = ParticipantCommand::Direct(commit.clone());
    let entry = || OwnedCommand::new_owned(command.clone(), f.shared(), ByteClass::Data).unwrap();
    let meta = |index| ApplyMeta {
        position: AppliedPosition {
            term: 1,
            node_id: 1,
            index,
        },
        command_digest: digest(9),
        membership: None,
    };
    let first = f.machine().apply(meta(1), entry()).await.unwrap();
    let state = f.dump().await;
    // Raft redelivers the same entry after a crash before the applied index
    // was observed: the stored reply returns and nothing is applied twice.
    let again = f.machine().apply(meta(1), entry()).await.unwrap();
    assert_eq!(again.as_slice(), first.as_slice());
    assert_eq!(f.dump().await, state);
    drop((first, again));

    // Close everything and reopen the pool from disk.
    let Fixture {
        replica,
        pool,
        pool_identity,
        group,
        dir,
        first_fence,
        ..
    } = f;
    drop(first_fence);
    let shared = replica.shared.clone();
    drop(replica);
    drop(pool);
    let pool = OltpPool::open_development(&dir.0, pool_identity, shared.clone()).unwrap();
    let mut reopened = Replica {
        machine: ParticipantMachine::new(pool.group(group).await.unwrap()).unwrap(),
        shared,
        index: 2,
    };
    assert_eq!(reopened.dump().await, state, "the commit is durable");
    // The same request proposed again as a new entry replays.
    assert_eq!(
        reopened
            .submit(ParticipantCommand::Direct(DirectCommit {
                commit_ts: ts(20),
                ..commit.clone()
            }))
            .await,
        replayed(&commit)
    );
    assert_eq!(reopened.dump().await, state);
}

// ---------------------------------------------------------------------------
// Dedup records survive snapshots, splits, backups and restore (D03)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d03_dedup_records_survive_raft_snapshot_install() {
    let mut f = Fixture::new().await;
    let commit = f.put(1, None, 10);
    assert_eq!(f.direct(&commit).await, applied(10));
    let snapshot = f.machine().store().export_snapshot(BUDGET).await.unwrap();

    let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", Uuid::new_v4())));
    let mut identity = f.pool_identity.clone();
    identity.pool_id = Uuid::new_v4();
    let pool = OltpPool::open_development(&dir.0, identity, f.shared().clone()).unwrap();
    let store = pool.group(f.group.clone()).await.unwrap();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    let mut follower = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: f.replica.index,
    };
    assert_eq!(follower.dump().await, f.dump().await);
    let retry = DirectCommit {
        commit_ts: ts(20),
        ..commit.clone()
    };
    assert_eq!(
        follower.submit(ParticipantCommand::Direct(retry)).await,
        replayed(&commit)
    );
}

#[tokio::test]
async fn d03_o16_dedup_records_follow_their_anchor_row_through_a_split() {
    use super::super::{image_archive::ImagePage, lineage::SplitBinding};
    let mut f = Fixture::new().await;
    let left = f.put(1, None, 10);
    let right = f.put(4, None, 11);
    let late = f.put(5, None, 13);
    for (commit, ts) in [(&left, 10), (&right, 11), (&late, 13)] {
        assert_eq!(f.direct(commit).await, applied(ts));
    }

    let split_txn = TxnId {
        request_id: Uuid::new_v4(),
        attempt: 1,
    };
    let split_request = RequestBinding {
        run_id: f.authority.run_id,
        request_id: split_txn.request_id,
        digest: digest(1),
        accepted_context: digest(2),
    };
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: Uuid::new_v4(),
            catalog_group_id: f.catalog_group_id,
            table_id: f.table,
            lifecycle_version: Some(ts(1)),
            schema_epoch: 1,
            txn: split_txn,
            request: split_request,
            read_ts: ts(11),
            authority: f.authority,
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    let spans = [
        KeySpan {
            start: None,
            end: Some(Key::Int64(3)),
        },
        KeySpan {
            start: Some(Key::Int64(3)),
            end: None,
        },
    ];
    let mut children = Vec::new();
    let mut targets = Vec::new();
    for span in &spans {
        let mut target = f.group.clone();
        target.group_id = Uuid::new_v4();
        target.kind = GroupKind::Data {
            table_id: f.table,
            shard_id: Uuid::new_v4(),
        };
        let mut child = Replica {
            machine: ParticipantMachine::new(f.pool.group(target.clone()).await.unwrap()).unwrap(),
            shared: f.shared().clone(),
            index: 0,
        };
        assert!(matches!(
            child.submit(f.fence(1, 2, span.clone())).await,
            ParticipantReply::Fenced(_)
        ));
        targets.push(target);
        children.push(child);
    }
    let source = f.group.clone();
    let bindings = |cut: u64| -> [SplitBinding; 2] {
        [0, 1].map(|i| SplitBinding {
            split_id: guard.binding().guard_id,
            source: source.clone(),
            source_schema_epoch: 1,
            source_routing_epoch: 1,
            source_span: full_span(),
            target: targets[i].clone(),
            target_span: spans[i].clone(),
            read_ts: ts(cut),
            guard: guard.clone(),
        })
    };
    // A direct commit (13) above the cut already applied: the freeze is
    // refused, because the child images would miss it.
    assert_eq!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(bindings(12)),
        })
        .await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    let bindings = bindings(14);
    assert!(matches!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(bindings.clone()),
        })
        .await,
        ParticipantReply::SplitSourceFrozen(_)
    ));
    // The frozen source takes no direct commit.
    let frozen = f.put(6, None, 15);
    assert_rejected_without_writes(&mut f, &frozen, Fault::StaleEpoch).await;

    for (child, binding) in children.iter_mut().zip(bindings.iter()) {
        assert_eq!(
            child
                .submit(ParticipantCommand::SplitStage {
                    authority: f.authority,
                    binding: Box::new(binding.clone()),
                })
                .await,
            ParticipantReply::SplitStaged
        );
    }
    // A staged child that has not imported everything takes none either.
    let mut early = f.put(1, Some(10), 16);
    early.routing_epoch = 2;
    let before = children[0].dump().await;
    assert_eq!(
        children[0].submit(ParticipantCommand::Direct(early)).await,
        rejected(Fault::StaleEpoch)
    );
    assert_eq!(children[0].dump().await, before);

    let mut after = None;
    let mut after_digest = None;
    let mut root = lineage::initial_root(&bindings[0]).unwrap();
    let mut page_index = 0;
    let mut imported = [0; 2];
    loop {
        let request = OwnedCommand::new_owned(
            bindings[0].image_request(after.clone(), 64 << 10),
            f.shared(),
            ByteClass::Data,
        )
        .unwrap();
        let page = f.machine().committed_image_page(request).await.unwrap();
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { exhausted, .. } = page.payload().unwrap() else {
            panic!("settled source blocked")
        };
        let exhausted = *exhausted;
        let mut next_root = None;
        for i in 0..2 {
            let (command, expected_root) = bindings[i]
                .import_page_from_image(f.shared(), page_index, root, after_digest, page.clone())
                .await
                .unwrap();
            next_root = Some(expected_root);
            let ParticipantReply::SplitImported {
                images, complete, ..
            } = children[i].apply_owned(command).await
            else {
                panic!("child rejected an image page")
            };
            imported[i] = images;
            assert_eq!(complete, exhausted);
        }
        root = next_root.unwrap();
        page_index += 1;
        after_digest = children[0]
            .machine
            .split_progress(&bindings[0], 64 << 10)
            .await
            .unwrap()
            .payload()
            .unwrap()
            .after_digest;
        after = next;
        if exhausted {
            break;
        }
    }
    // Rows plus their dedup records, each with its anchor's child.
    assert_eq!(imported, [2, 4]);
    for (child, keys) in children
        .iter()
        .zip([vec![left.clone()], vec![right.clone(), late.clone()]])
    {
        let state = child.dump().await;
        assert_eq!(count_tag(&state, DEDUP), keys.len());
        assert_eq!(count_tag(&state, EXPIRY), keys.len());
    }
    // A retry routed to the child replays the source's exact result.
    for (child, commit, expected) in [
        (0usize, &left, Some(10)),
        (1, &right, Some(11)),
        (1, &late, Some(13)),
    ] {
        let mut retry = DirectCommit {
            commit_ts: ts(20),
            routing_epoch: 2,
            ..commit.clone()
        };
        retry.writes[0].expected_version = expected.map(ts);
        assert_eq!(
            children[child]
                .submit(ParticipantCommand::Direct(retry))
                .await,
            replayed(commit)
        );
    }
    // A new commit on the child cannot land at or below its import cut.
    let mut at_cut = f.put(4, Some(11), 14);
    at_cut.routing_epoch = 2;
    let before = children[1].dump().await;
    assert_eq!(
        children[1]
            .submit(ParticipantCommand::Direct(at_cut.clone()))
            .await,
        rejected(Fault::Conflict)
    );
    assert_eq!(children[1].dump().await, before);
    at_cut.commit_ts = ts(15);
    assert_eq!(
        children[1].submit(ParticipantCommand::Direct(at_cut)).await,
        applied(15)
    );
}

impl Replica {
    async fn apply_owned(&mut self, command: OwnedCommand<ParticipantCommand>) -> ParticipantReply {
        let digest = crate::oltp_replication::owned::serialized_digest(&command).unwrap();
        let index = self.index;
        self.index += 1;
        let raw = self
            .machine
            .apply(
                ApplyMeta {
                    position: AppliedPosition {
                        term: 1,
                        node_id: 1,
                        index,
                    },
                    command_digest: digest,
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}

#[tokio::test]
async fn d03_o19_dedup_records_survive_backup_images_and_restore() {
    use super::super::{image_archive::ImageRequest, restore};
    let mut f = Fixture::new().await;
    let kept = f.put(1, None, 10);
    let above_cut = f.put(2, None, 30);
    assert_eq!(f.direct(&kept).await, applied(10));
    assert_eq!(f.direct(&above_cut).await, applied(30));
    let cut = 20;

    // Export committed images at the cut, as a full backup does.
    let mut images = Vec::new();
    let mut after = None;
    loop {
        let request = OwnedCommand::new_owned(
            ImageRequest {
                read_ts: ts(cut),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
                after: after.clone(),
                max_items: 128,
                max_bytes: 64 << 10,
            },
            f.shared(),
            ByteClass::Data,
        )
        .unwrap();
        let page = f.machine().committed_image_page(request).await.unwrap();
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let image_archive::ImagePage::Images {
            images: page_images,
            exhausted,
            ..
        } = page.payload().unwrap()
        else {
            panic!("blocked")
        };
        images.extend(page_images.iter().cloned());
        if *exhausted {
            break;
        }
        after = next;
    }
    assert_eq!(
        images.len(),
        2,
        "row 1 and its dedup record; nothing above the cut"
    );
    assert!(images.iter().all(|image| image.commit_ts == ts(10)));
    assert!(
        images
            .iter()
            .any(|image| matches!(image.key, MvccKey::DirectDedup { .. }))
    );
    for image in &images {
        restore::validate_archive_image(
            &crate::oltp::backup::history::GroupImageScope {
                group: f.group.clone(),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
            },
            &image.key,
        )
        .unwrap();
    }

    // Install them into a new group, as an offline restore does, with the
    // restore genesis that sets the group's floor to the cut.
    let mut target = f.group.clone();
    target.group_id = Uuid::new_v4();
    let store = f.pool.group(target.clone()).await.unwrap();
    let genesis = restore::ParticipantGenesis {
        restore_id: Uuid::new_v4(),
        source_cluster: f.group.cluster_id,
        target_group: target.clone(),
        catalog_group_id: f.catalog_group_id,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        cut: ts(cut),
    };
    let install = {
        let target = target.clone();
        let images = images.clone();
        move |tx: &mut GroupTransaction<'_>| {
            tx.put_serialized(restore::RESTORE_GENESIS, &genesis)?;
            for image in &images {
                restore::install_image(tx, &target, &full_span(), image)?;
            }
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        }
    };
    store
        .transaction(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
                command_digest: digest(7),
                membership: None,
            },
            BUDGET,
            install,
        )
        .await
        .unwrap();
    let mut restored = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: 1,
    };
    assert!(matches!(
        restored.submit(f.fence(1, 1, full_span())).await,
        ParticipantReply::Fenced(_)
    ));
    let state = restored.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 1);
    assert_eq!(count_tag(&state, EXPIRY), 1);
    // The restored request replays.
    let retry = DirectCommit {
        commit_ts: ts(40),
        ..kept.clone()
    };
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(retry)).await,
        replayed(&kept)
    );
    // A request that committed above the cut is not restored: it executes.
    let mut again = above_cut.clone();
    again.commit_ts = ts(41);
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(again)).await,
        applied(41)
    );
    // No direct commit can land at or below the restored floor (the cut).
    let below = f.put(3, None, cut);
    let before = restored.dump().await;
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(below)).await,
        rejected(Fault::Conflict)
    );
    assert_eq!(restored.dump().await, before);
}

// ---------------------------------------------------------------------------
// Dedup expiry (deterministic replicated command)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d03_direct_expire_deletes_exactly_the_records_before_the_cutoff_on_every_replica() {
    let mut a = Fixture::new().await;
    // A second replica of the same group, fenced by the same command.
    let mut b = a.replica_of().await;
    let commits: Vec<DirectCommit> = (1..=3)
        .map(|n| {
            let mut commit = a.put(n, None, 10 + n as u64);
            commit.applied_unix_ms = 100 * n as u64;
            commit
        })
        .collect();
    for commit in &commits {
        let expected = applied(commit.commit_ts.get());
        assert_eq!(a.direct(commit).await, expected);
        assert_eq!(
            b.submit(ParticipantCommand::Direct(commit.clone())).await,
            expected
        );
    }
    let authority = a.authority;
    let expire = move |cutoff, max_items| {
        ParticipantCommand::DirectExpire(DirectExpire {
            authority,
            cutoff_unix_ms: cutoff,
            max_items,
        })
    };
    // Invalid and stale commands delete nothing.
    let before = a.dump().await;
    for (command, fault) in [
        (expire(250, 0), Fault::InvalidBinding),
        (
            expire(250, MAX_DIRECT_EXPIRE_ITEMS + 1),
            Fault::InvalidBinding,
        ),
        (expire(0, 1), Fault::InvalidBinding),
        (
            ParticipantCommand::DirectExpire(DirectExpire {
                authority: Authority {
                    owner_term: 9,
                    ..authority
                },
                cutoff_unix_ms: 250,
                max_items: 1,
            }),
            Fault::StaleAuthority,
        ),
    ] {
        assert_eq!(a.submit(command).await, ParticipantReply::Rejected(fault));
    }
    assert_eq!(a.dump().await, before);
    // Two pages: records at 100 and 200 go, oldest first; 300 stays.
    for (deleted, complete) in [(1, false), (1, true), (0, true)] {
        let reply = ParticipantReply::DirectExpired { deleted, complete };
        assert_eq!(a.submit(expire(250, 1)).await, reply);
        assert_eq!(b.submit(expire(250, 1)).await, reply);
    }
    let state = a.dump().await;
    assert_eq!(state, b.dump().await, "replicas stay identical");
    assert_eq!(count_tag(&state, DEDUP), 1);
    assert_eq!(count_tag(&state, EXPIRY), 1);
    // The kept record still replays; an expired one no longer does.
    let mut kept = commits[2].clone();
    kept.commit_ts = ts(50);
    assert_eq!(a.direct(&kept).await, replayed(&commits[2]));
    let mut expired = commits[0].clone();
    expired.commit_ts = ts(51);
    assert_eq!(a.direct(&expired).await, rejected(Fault::Conflict));
    assert_eq!(expiry_cutoff(1_000, 400), Some(600));
    assert_eq!(expiry_cutoff(100, 400), None);
}

// ---------------------------------------------------------------------------
// RangeClock (D06 store half, O09)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d06_registration_requires_leadership_and_an_applied_entry_of_the_term() {
    let f = Fixture::new().await;
    let clock = f.machine().range_clock().clone();
    assert_eq!(
        clock.register_pending(ts(10), vec![f.key(1)]),
        Err(ClockRejection::NotServing),
        "a clock that never led refuses commits"
    );
    // The fixture applied term-1 entries. Leading term 2 holds until an
    // entry of term 2 applies; a blank entry reaches the clock through the
    // executor's Raft metrics, not the participant machine.
    clock.lead(2);
    assert!(!clock.serving());
    assert_eq!(
        clock.register_pending(ts(10), vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
    let read = tokio::spawn({
        let machine = f.machine().clone();
        let key = f.key(1);
        async move { machine.read_at(ts(5), key).await.map(drop) }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!read.is_finished(), "a held leader serves no snapshot read");
    clock.observe_applied(AppliedPosition {
        term: 2,
        node_id: 1,
        index: 9,
    });
    read.await.unwrap().unwrap();
    assert!(clock.serving());
    assert_eq!(clock.register_pending(ts(10), vec![f.key(1)]), Ok(()));
    assert_eq!(
        clock.register_pending(ts(10), vec![f.key(1)]),
        Err(ClockRejection::Duplicate)
    );
    clock.step_down();
    assert_eq!(clock.pending_len(), 0);
    assert_eq!(
        clock.register_pending(ts(11), vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
}

#[tokio::test]
async fn d06_a_commit_cannot_land_at_or_below_an_already_served_snapshot() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    assert_eq!(f.version(20, 1).await, None);
    assert_eq!(clock.max_read_ts(), Some(ts(20)));
    for commit_ts in [19, 20] {
        assert!(clock.needs_new_timestamp(ts(commit_ts)));
        assert_eq!(
            clock.register_pending(ts(commit_ts), vec![f.key(1)]),
            Err(ClockRejection::StaleTimestamp {
                max_read_ts: ts(20)
            })
        );
    }
    assert!(!clock.needs_new_timestamp(ts(21)));
    clock.register_pending(ts(21), vec![f.key(1)]).unwrap();
    assert_eq!(f.direct(&f.put(1, None, 21)).await, applied(21));
    assert_eq!(clock.pending_len(), 0, "apply settles the pending entry");
    assert_eq!(
        f.version(20, 1).await,
        None,
        "the served snapshot is unchanged"
    );
    assert_eq!(f.version(21, 1).await, Some(21));
}

#[tokio::test]
async fn d06_t3_a_window_value_below_the_gc_safe_point_is_safe() {
    // `AdvanceFloor` may move the Timestamp GC safe point (here 60) past
    // window values still reserved or in flight (here 45). Snapshot reads
    // are never below the safe point, and Data participants delete no
    // version for it, so such a commit is safe exactly when rule 1 holds.
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    let safe_point = 60;
    // Nothing read this range yet: the low commit lands, and every snapshot
    // a reader may still take (at or above the safe point) sees it.
    clock.register_pending(ts(45), vec![f.key(1)]).unwrap();
    assert_eq!(f.direct(&f.put(1, None, 45)).await, applied(45));
    assert_eq!(f.version(safe_point, 1).await, Some(45));
    // A snapshot at the safe point already read the range: another value
    // from that old window is refused and must be re-allocated.
    assert_eq!(f.version(safe_point, 2).await, None);
    assert!(matches!(
        clock.register_pending(ts(46), vec![f.key(2)]),
        Err(ClockRejection::StaleTimestamp { .. })
    ));
    clock.register_pending(ts(61), vec![f.key(2)]).unwrap();
    assert_eq!(f.direct(&f.put(2, None, 61)).await, applied(61));
    assert_eq!(f.version(safe_point, 2).await, None);
    assert_eq!(f.version(61, 2).await, Some(61));
}

#[tokio::test]
async fn d06_o07_reads_and_scans_wait_for_pending_commits_at_or_below_their_snapshot() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    clock.register_pending(ts(15), vec![f.key(1)]).unwrap();
    clock.register_pending(ts(25), vec![f.key(2)]).unwrap();
    let scan = tokio::spawn({
        let machine = f.machine().clone();
        let table = f.table;
        async move { scan_versions(&machine, table, 20).await }
    });
    let above = tokio::spawn({
        let machine = f.machine().clone();
        let key = f.key(2);
        async move { machine.read_at(ts(10), key).await.map(drop) }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    above.await.unwrap().unwrap();
    assert!(
        !scan.is_finished(),
        "the scan at 20 waits for the commit at 15"
    );
    assert_eq!(f.direct(&f.put(1, None, 15)).await, applied(15));
    let seen = scan.await.unwrap();
    assert_eq!(seen, BTreeMap::from([(1, 15)]));
    // A latest-version read waits only for pending commits on its own key.
    let latest = tokio::spawn({
        let clock = clock.clone();
        let key = f.key(2);
        async move { clock.wait_latest(&key).await }
    });
    clock.wait_latest(&f.key(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!latest.is_finished());
    // A rejected entry settles its pending timestamp too.
    assert_eq!(
        f.direct(&f.put(2, Some(3), 25)).await,
        rejected(Fault::Conflict)
    );
    latest.await.unwrap().unwrap();
    assert_eq!(clock.pending_len(), 0);
    assert_eq!(
        scan_versions(f.machine(), f.table, 30).await,
        BTreeMap::from([(1, 15)])
    );
}

#[tokio::test]
async fn d06_a_split_can_pause_direct_commits_and_wait_for_the_range_to_drain() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    clock.register_pending(ts(10), vec![f.key(1)]).unwrap();
    let pause = clock.pause_commits();
    assert_eq!(
        clock.register_pending(ts(11), vec![f.key(2)]),
        Err(ClockRejection::Paused)
    );
    let drained = tokio::spawn({
        let clock = clock.clone();
        async move { clock.drained().await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!drained.is_finished());
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    drained.await.unwrap().unwrap();
    drop(pause);
    clock.register_pending(ts(11), vec![f.key(2)]).unwrap();
}

async fn scan_versions(machine: &ParticipantMachine, table: Uuid, s: u64) -> BTreeMap<i64, u64> {
    use super::super::scan::{ScanDomain, ScanPage, ScanRequest};
    let mut seen = BTreeMap::new();
    let mut after = None;
    loop {
        let page = machine
            .scan_page(ScanRequest {
                domain: ScanDomain::Rows { table_id: table },
                read_ts: ts(s),
                after: after.clone(),
                after_owner: None,
                lower: None,
                upper: None,
                max_items: 128,
                max_bytes: crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
            })
            .await
            .unwrap();
        let ScanPage::Records { records, exhausted } = page.payload().unwrap() else {
            panic!("scan blocked")
        };
        for record in records.iter() {
            let MvccKey::Row {
                primary_key: Key::Int64(key),
                ..
            } = record.key
            else {
                panic!("unexpected scan key")
            };
            if let Some(version) = record.version {
                assert!(version.get() <= s, "scan at {s} saw {version:?}");
                if record.value.is_some() {
                    seen.insert(key, version.get());
                }
            }
            after = Some(record.key.clone());
        }
        if *exhausted {
            return seen;
        }
    }
}

/// Seeded concurrent writers and scanners share one timestamp source, as
/// direct and queued callers share the Timestamp authority. Every scan at `s`
/// must return exactly the commits at or below `s`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d06_o09_concurrent_scans_see_exactly_the_commits_at_or_below_their_snapshot() {
    let f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    let source = Arc::new(AtomicU64::new(100));
    let next_index = Arc::new(tokio::sync::Mutex::new(f.replica.index));
    let mut writers = Vec::new();
    for writer in 0..4i64 {
        let machine = f.machine().clone();
        let shared = f.shared().clone();
        let source = source.clone();
        let clock = clock.clone();
        let next_index = next_index.clone();
        let template = f.put(0, None, 1);
        let key_of = f.key(0);
        writers.push(tokio::spawn(async move {
            let mut rng = 0x9e37_79b9_u64.wrapping_mul(writer as u64 + 1);
            let mut committed = Vec::new();
            let mut stale = 0;
            for n in 0..20 {
                let key = writer * 1000 + n;
                let MvccKey::Row { table_id, .. } = &key_of else {
                    unreachable!()
                };
                let row = MvccKey::Row {
                    table_id: *table_id,
                    primary_key: Key::Int64(key),
                };
                let commit_ts = loop {
                    let commit_ts = source.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    tokio::time::sleep(Duration::from_micros(rng % 2000)).await;
                    match clock.register_pending(ts(commit_ts), vec![row.clone()]) {
                        Ok(()) => break commit_ts,
                        Err(ClockRejection::StaleTimestamp { .. }) => stale += 1,
                        Err(other) => panic!("{other:?}"),
                    }
                };
                let mut commit = template.clone();
                commit.request_id = Uuid::new_v4();
                commit.commit_ts = ts(commit_ts);
                commit.writes[0].key = row;
                let mut index = next_index.lock().await;
                let reply = apply_at(
                    &machine,
                    &shared,
                    *index,
                    ParticipantCommand::Direct(commit),
                )
                .await;
                *index += 1;
                drop(index);
                assert_eq!(reply, applied(commit_ts));
                committed.push((key, commit_ts));
            }
            (committed, stale)
        }));
    }
    let mut readers = Vec::new();
    for reader in 0..2u64 {
        let machine = f.machine().clone();
        let source = source.clone();
        let table = f.table;
        readers.push(tokio::spawn(async move {
            let mut rng = 0x2545_f491_u64.wrapping_mul(reader + 7);
            let mut scans = Vec::new();
            for _ in 0..30 {
                let s = source.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                tokio::time::sleep(Duration::from_micros(rng % 1500)).await;
                scans.push((s, scan_versions(&machine, table, s).await));
            }
            scans
        }));
    }
    let mut committed = BTreeMap::new();
    let mut stale = 0;
    for writer in writers {
        let (rows, retries) = writer.await.unwrap();
        stale += retries;
        committed.extend(rows);
    }
    assert_eq!(committed.len(), 80);
    let mut scans = 0;
    for reader in readers {
        for (s, seen) in reader.await.unwrap() {
            let expected: BTreeMap<i64, u64> = committed
                .iter()
                .filter(|(_, commit_ts)| **commit_ts <= s)
                .map(|(key, commit_ts)| (*key, *commit_ts))
                .collect();
            assert_eq!(seen, expected, "scan at {s}");
            scans += 1;
        }
    }
    assert_eq!(scans, 60);
    eprintln!("d06 concurrent: 80 commits, 60 scans, {stale} stale-timestamp retries");
}

// ---------------------------------------------------------------------------
// Mixed versions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o02_direct_variants_use_new_tags_that_an_older_decoder_rejects() {
    let f = Fixture::new().await;
    let command = serde_json::to_value(ParticipantCommand::Direct(f.put(1, None, 10))).unwrap();
    assert!(command.get("direct").is_some());
    let expire = serde_json::to_value(ParticipantCommand::DirectExpire(DirectExpire {
        authority: f.authority,
        cutoff_unix_ms: 1,
        max_items: 1,
    }))
    .unwrap();
    assert!(expire.get("direct_expire").is_some());
    // The participant command decoder rejects a variant it does not know,
    // which is how an older replica fails on these tags.
    let unknown = serde_json::json!({"direct_v2": {}});
    assert!(serde_json::from_value::<ParticipantCommand>(unknown).is_err());
    // Nested in a scoped envelope, a direct command is refused at apply.
    let mut f = f;
    let before = f.dump().await;
    let scoped = ParticipantCommand::Scoped {
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead {
            queue_group_id: Uuid::new_v4(),
            data_generation: Uuid::new_v4(),
            queue_generation: Uuid::new_v4(),
            queue_view: crate::oltp_replication::types::AuthorityView {
                run_id: f.authority.run_id,
                owner_term: f.authority.owner_term,
                membership_revision: 1,
            },
            request: RequestBinding {
                run_id: f.authority.run_id,
                request_id: Uuid::new_v4(),
                digest: digest(1),
                accepted_context: digest(2),
            },
            table_id: f.table,
            read_ts: ts(9),
        },
        command: OwnedCommand::new_owned(
            ParticipantCommand::Direct(f.put(1, None, 10)),
            f.shared(),
            ByteClass::Data,
        )
        .unwrap(),
    };
    assert_eq!(
        f.submit(scoped).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(f.dump().await, before);
}
