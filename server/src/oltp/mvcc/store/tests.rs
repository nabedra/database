use super::*;
use crate::{
    config::WorkerResources,
    oltp::{
        mvcc::CanonicalJson,
        pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
    },
    oltp_replication::{
        authority::status::VerifiedStatusDecision,
        types::{AbortReason, DecisionStatement, Text, Timestamp},
    },
};
use std::path::PathBuf;
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    machine: ParticipantMachine,
    shared: Arc<WorkerSharedResources>,
    authority: Authority,
    index: u64,
    table: Uuid,
    catalog_group_id: Uuid,
    _dir: Dir,
}
impl Fixture {
    async fn new() -> Self {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-participant-{}", Uuid::new_v4())));
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: 1,
            cluster_id: Uuid::new_v4(),
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        };
        let pool = OltpPool::open_development(&dir.0, identity.clone(), shared.clone()).unwrap();
        let table = Uuid::new_v4();
        let group = pool
            .group(GroupIdentity {
                cluster_id: identity.cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: Uuid::new_v4(),
                kind: GroupKind::Data {
                    table_id: table,
                    shard_id: Uuid::new_v4(),
                },
                group_format_version: 1,
            })
            .await
            .unwrap();
        let authority = Authority {
            run_id: Uuid::new_v4(),
            owner_term: 1,
        };
        let mut f = Self {
            catalog_group_id: Uuid::new_v4(),
            machine: ParticipantMachine::new(group).unwrap(),
            shared,
            authority,
            index: 0,
            table,
            _dir: dir,
        };
        assert!(matches!(
            f.submit(ParticipantCommand::Fence {
                transition_id: Uuid::new_v4(),
                expected: None,
                next: authority,
                schema_epoch: 1,
                routing_epoch: 1,
                catalog_group_id: f.catalog_group_id,
                span: KeySpan {
                    start: None,
                    end: None
                }
            })
            .await,
            ParticipantReply::Fenced(_)
        ));
        f
    }
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        let command = OwnedCommand::new_owned(command, &self.shared, ByteClass::Data).unwrap();
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: self.index,
        };
        self.index += 1;
        let raw = self
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: Digest::from_bytes([position.index as u8; 32]),
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
    fn row(&self, key: i64, expected: Option<u64>, change: Change) -> PreparedWrite {
        PreparedWrite {
            key: MvccKey::Row {
                table_id: self.table,
                primary_key: Key::Int64(key),
            },
            expected_version: expected.map(ts),
            change,
        }
    }
    fn put(&self, value: serde_json::Value) -> Change {
        Change::Put(
            CanonicalJson::from_serializable(&self.shared, &value, crate::protocol::MAX_BODY_BYTES)
                .unwrap(),
        )
    }
    fn plan(&self, read: u64, pages: &[Vec<PreparedWrite>]) -> Plan {
        let txn = TxnId {
            request_id: Uuid::new_v4(),
            attempt: 0,
        };
        let request = RequestBinding {
            run_id: self.authority.run_id,
            request_id: txn.request_id,
            digest: Digest::from_bytes([1; 32]),
            accepted_context: Digest::from_bytes([2; 32]),
        };
        let manifest = ManifestRef {
            status_group_id: Uuid::new_v4(),
            digest: Digest::from_bytes([3; 32]),
            participant_count: 1,
        };
        let mut plan = ParticipantPlan {
            group: self.machine.store.identity().clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(read),
            write_root: Digest::from_bytes([0; 32]),
            write_count: pages.iter().map(|p| p.len() as u64).sum(),
            page_count: pages.len() as u64,
        };
        let mut root = initial_root(txn, &plan).unwrap();
        let mut roots = Vec::new();
        for (i, p) in pages.iter().enumerate() {
            roots.push(root);
            root = page_root(root, i as u64, p).unwrap();
        }
        plan.write_root = root;
        Plan {
            txn,
            request,
            manifest,
            plan,
            roots,
        }
    }
    async fn prepare(
        &mut self,
        p: &Plan,
        page: usize,
        writes: Vec<PreparedWrite>,
    ) -> ParticipantReply {
        self.submit(ParticipantCommand::PreparePage {
            authority: self.authority,
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            plan: p.plan.clone(),
            page_index: page as u64,
            previous_root: p.roots[page],
            guards: Page::new(vec![
                lifecycle::VerifiedLifecycleGuard::model_for_test(
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
                .into_sealed(),
            ])
            .unwrap(),
            writes: Page::new(writes).unwrap(),
        })
        .await
    }
    async fn seal(&mut self, p: &Plan) -> ParticipantReply {
        self.submit(ParticipantCommand::Seal {
            authority: self.authority,
            txn: p.txn,
            manifest: p.manifest,
            result_digest: Digest::from_bytes([4; 32]),
        })
        .await
    }
    async fn resolve(
        &mut self,
        p: &Plan,
        decision: TxnDecision,
        max_items: u16,
    ) -> ParticipantReply {
        let statement = DecisionStatement {
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            decided_at: AppliedPosition {
                term: 2,
                node_id: 2,
                index: 10,
            },
            decision,
        };
        let proof = VerifiedStatusDecision::model_for_test(statement)
            .unwrap()
            .into_sealed();
        self.submit(ParticipantCommand::Resolve {
            authority: self.authority,
            proof,
            max_items,
        })
        .await
    }
    async fn read(&self, s: u64, key: Key) -> serde_json::Value {
        let raw = self
            .machine
            .read_at(
                ts(s),
                MvccKey::Row {
                    table_id: self.table,
                    primary_key: key,
                },
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}
struct Plan {
    txn: TxnId,
    request: RequestBinding,
    manifest: ManifestRef,
    plan: ParticipantPlan,
    roots: Vec<Digest>,
}
fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}
fn committed(value: u64) -> TxnDecision {
    TxnDecision::Commit {
        commit_ts: ts(value),
    }
}
fn aborted() -> TxnDecision {
    TxnDecision::Abort {
        reason: AbortReason::Conflict,
    }
}

#[tokio::test]
async fn o07_o09_fixed_pages_seal_exact_plan_and_delayed_commit_stays_discoverable() {
    let mut f = Fixture::new().await;
    let writes = vec![
        f.row(-1, None, f.put(serde_json::json!({"id":-1,"v":"a"}))),
        f.row(2, None, f.put(serde_json::json!({"id":2,"v":"b"}))),
    ];
    let p = f.plan(10, &[writes.clone()]);
    assert_eq!(
        f.read(10, Key::Int64(-1)).await,
        serde_json::json!({"value":{"version":null,"value":null}})
    );
    assert_eq!(
        f.prepare(&p, 0, writes.clone()).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 2
        }
    );
    assert!(f.read(20, Key::Int64(-1)).await.get("blocked").is_some());
    assert_eq!(
        f.prepare(&p, 0, writes.clone()).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 2
        }
    );
    let mut changed = writes;
    changed[0].change = Change::Delete;
    assert_eq!(
        f.prepare(&p, 0, changed).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert!(matches!(f.seal(&p).await, ParticipantReply::Sealed(_)));
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: false
        }
    );
    assert_eq!(
        f.read(10, Key::Int64(-1)).await["value"]["version"],
        serde_json::Value::Null
    );
    assert_eq!(f.read(11, Key::Int64(-1)).await["value"]["value"]["v"], "a");
    assert!(f.read(11, Key::Int64(2)).await.get("blocked").is_some());
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 2,
            complete: true
        }
    );
    assert_eq!(f.read(11, Key::Int64(2)).await["value"]["value"]["v"], "b");
    assert_eq!(
        f.resolve(&p, committed(11), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 2,
            complete: true
        }
    );
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o08_t06_tombstone_exact_version_and_keep_validate_target_without_new_version() {
    let mut f = Fixture::new().await;
    let first = vec![f.row(1, None, Change::Delete)];
    let p = f.plan(1, &[first.clone()]);
    f.prepare(&p, 0, first).await;
    f.seal(&p).await;
    f.resolve(&p, committed(2), 128).await;
    assert_eq!(
        f.read(2, Key::Int64(1)).await,
        serde_json::json!({"value":{"version":2,"value":null}})
    );
    let stale = vec![f.row(1, None, Change::Keep)];
    let bad = f.plan(2, &[stale.clone()]);
    assert_eq!(
        f.prepare(&bad, 0, stale).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    let keep = vec![f.row(1, Some(2), Change::Keep)];
    let p = f.plan(2, &[keep.clone()]);
    assert_eq!(
        f.prepare(&p, 0, keep).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 1
        }
    );
    f.seal(&p).await;
    f.resolve(&p, committed(3), 128).await;
    assert_eq!(f.read(3, Key::Int64(1)).await["value"]["version"], 2);
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o07_o15_r05_abort_before_late_prepare_and_partial_prepare_retains_rollback_marker() {
    let mut f = Fixture::new().await;
    let writes = vec![f.row(1, None, Change::Delete)];
    let p = f.plan(1, &[writes.clone()]);
    assert_eq!(
        f.resolve(&p, aborted(), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 0,
            complete: true
        }
    );
    assert_eq!(
        f.prepare(&p, 0, writes).await,
        ParticipantReply::Rejected(Fault::RolledBack)
    );
    let pages = vec![
        vec![f.row(1, None, Change::Delete)],
        vec![f.row(2, None, Change::Delete)],
    ];
    let partial = f.plan(1, &pages);
    f.prepare(&partial, 0, pages[0].clone()).await;
    assert_eq!(
        f.seal(&partial).await,
        ParticipantReply::Rejected(Fault::Incomplete)
    );
    assert_eq!(
        f.resolve(&partial, aborted(), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: true
        }
    );
    assert_eq!(
        f.prepare(&partial, 1, pages[1].clone()).await,
        ParticipantReply::Rejected(Fault::RolledBack)
    );
    assert!(f.read(20, Key::Int64(1)).await.get("value").is_some());
    let old = f.authority;
    f.authority = Authority {
        run_id: Uuid::new_v4(),
        owner_term: 2,
    };
    assert!(matches!(
        f.submit(ParticipantCommand::Fence {
            transition_id: Uuid::new_v4(),
            expected: Some(old),
            next: f.authority,
            catalog_group_id: f.catalog_group_id,
            schema_epoch: 1,
            routing_epoch: 1,
            span: KeySpan {
                start: None,
                end: None
            }
        })
        .await,
        ParticipantReply::Fenced(_)
    ));
    assert_eq!(
        f.submit(ParticipantCommand::Seal {
            authority: old,
            txn: partial.txn,
            manifest: partial.manifest,
            result_digest: Digest::from_bytes([4; 32])
        })
        .await,
        ParticipantReply::Rejected(Fault::StaleAuthority)
    );
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o07_m01_wide_fixed_prepare_and_resolve_fit_private_apply_without_row_tree_decode() {
    let mut f = Fixture::new().await;
    let key = "k".repeat(7 << 20);
    let change = f.put(serde_json::json!({"id":key}));
    let write = PreparedWrite {
        key: MvccKey::Row {
            table_id: f.table,
            primary_key: Key::Utf8(key.clone()),
        },
        expected_version: None,
        change,
    };
    let p = f.plan(1, &[vec![write.clone()]]);
    assert_eq!(
        f.prepare(&p, 0, vec![write]).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 1
        }
    );
    assert!(matches!(f.seal(&p).await, ParticipantReply::Sealed(_)));
    assert_eq!(
        f.resolve(&p, committed(2), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: true
        }
    );
    let read = f.read(2, Key::Utf8(key.clone())).await;
    assert_eq!(read["value"]["value"]["id"].as_str(), Some(key.as_str()));
    assert_eq!(f.shared.apply_bytes_used(), 0);
    assert_eq!(f.shared.control_bytes_used(), 0);
}
