//! Golden bytes of a table without search columns (contract, "Storage"): every
//! log entry, every stored record and every digest of one fixed history. The
//! expected values were recorded on the build before vector search existed.
use super::{
    direct::{
        DirectBatch, DirectCommit, DirectForward, DirectPause, DirectSchemaChange,
        DirectSchemaFence, DirectWrite, IndexChange,
    },
    image_archive::ImagePage,
    lineage::SplitBinding,
    *,
};
use crate::oltp::{
    mvcc::{CanonicalJson, index::IndexValue},
    pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
};
use nabedra_oltp_types::types::{DecisionStatement, Text, Timestamp};
use nabedra_resources::config::WorkerResources;

fn id(n: u128) -> Uuid {
    Uuid::from_u128(0x4000_8000_0000_0000_0000 + n)
}
fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}
fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}
fn row_key(key: i64) -> MvccKey {
    MvccKey::Row {
        table_id: id(1),
        primary_key: Key::Int64(key),
    }
}
const AUTHORITY: fn() -> Authority = || Authority {
    run_id: id(2),
    owner_term: 1,
};

struct Golden {
    shared: Arc<WorkerSharedResources>,
    entries: Sha256,
}
struct Replica {
    machine: ParticipantMachine,
    index: u64,
}
impl Golden {
    fn json(&self, value: serde_json::Value) -> CanonicalJson {
        CanonicalJson::from_serializable(&self.shared, &value, 1 << 20).unwrap()
    }
    fn write(&self, key: i64, expected: Option<u64>, value: Option<serde_json::Value>) -> DirectWrite {
        serde_json::from_value::<DirectWrite>(serde_json::json!({
            "key": row_key(key),
            "expected_version": expected,
            "value": null,
            "index": [],
        }))
        .map(|write| DirectWrite {
            value: value.map(|value| self.json(value)),
            ..write
        })
        .unwrap()
    }
    fn commit(&self, request: u128, commit_ts: u64, writes: Vec<DirectWrite>) -> DirectCommit {
        DirectCommit {
            authority: AUTHORITY(),
            table_id: id(1),
            schema_epoch: 1,
            routing_epoch: 1,
            request_id: id(request),
            digest: digest(9),
            commit_ts: ts(commit_ts),
            writes,
            result: self.json(serde_json::json!({"request": request as u64})),
            applied_unix_ms: 1_000 + commit_ts,
        }
    }
    async fn submit(&mut self, replica: &mut Replica, command: ParticipantCommand) -> ParticipantReply {
        let command = OwnedCommand::new_owned(command, &self.shared, ByteClass::Data).unwrap();
        self.submit_owned(replica, command).await
    }
    async fn submit_owned(
        &mut self,
        replica: &mut Replica,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ParticipantReply {
        let encoded = serde_json::to_vec(&command).unwrap();
        self.entries.update((encoded.len() as u64).to_be_bytes());
        self.entries.update(&encoded);
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: replica.index,
        };
        replica.index += 1;
        let raw = replica
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: nabedra_oltp_types::owned::serialized_digest(&command).unwrap(),
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}

/// Every key and value the group stores, metadata included.
fn stored(pool: &OltpPool, group: Uuid) -> (usize, String) {
    let mut scope = vec![0x40];
    scope.extend_from_slice(group.as_bytes());
    let mut end = scope.clone();
    end.push(0xff);
    let snapshot = pool.kv_store().snapshot();
    let mut iterator = snapshot.iter(
        std::ops::Bound::Included(&scope),
        std::ops::Bound::Excluded(&end),
        false,
    );
    iterator.seek(&scope);
    let mut hash = Sha256::new();
    let mut records = 0;
    while iterator.valid() {
        let value = iterator.value().unwrap();
        for bytes in [iterator.key(), &value] {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        records += 1;
        iterator.next();
    }
    iterator.status().unwrap();
    (records, hex(hash))
}
fn hex(hash: Sha256) -> String {
    hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn vec_g10_a_table_without_search_columns_keeps_its_bytes() {
    let shared = WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("mvcc-golden-{}", nabedra_rt::ids::uuid_v4()));
    let pool = OltpPool::open_development(
        &dir,
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: id(3),
            worker_id: id(4),
            pool_id: id(5),
        },
        shared.clone(),
    )
    .unwrap();
    let group = |n: u128| GroupIdentity {
        cluster_id: id(3),
        database: Text::try_from("default").unwrap(),
        group_id: id(n),
        kind: GroupKind::Data {
            table_id: id(1),
            shard_id: id(n + 100),
        },
        group_format_version: 1,
    };
    let mut g = Golden {
        shared: shared.clone(),
        entries: Sha256::new(),
    };
    let fence = |routing_epoch, span| ParticipantCommand::Fence {
        transition_id: id(6),
        expected: None,
        next: AUTHORITY(),
        schema_epoch: 1,
        routing_epoch,
        span,
        catalog_group_id: id(7),
    };
    let activate = || {
        ParticipantCommand::DirectSchema(DirectSchemaFence {
            authority: AUTHORITY(),
            table_id: id(1),
            change: DirectSchemaChange::Activate { lifecycle: id(1) },
        })
    };
    let mut source = Replica {
        machine: ParticipantMachine::new(pool.group(group(10)).await.unwrap()).unwrap(),
        index: 0,
    };
    let full = KeySpan {
        start: None,
        end: None,
    };
    g.submit(&mut source, fence(1, full.clone())).await;
    g.submit(&mut source, activate()).await;

    // The direct path: rows with an index entry, a batch with a refused
    // member, an update, a delete and a forward marker.
    let mut first = g.write(1, None, Some(serde_json::json!({"id": 1, "body": "a", "v": 7})));
    first.index.push(IndexChange {
        key: MvccKey::IndexEntry {
            table_id: id(1),
            index_id: id(8),
            values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
            primary_key: Key::Int64(1),
        },
        expected_version: None,
        value: Some(g.json(serde_json::json!({"id": 1}))),
    });
    let second = g.write(2, None, Some(serde_json::json!({"id": 2, "body": "b"})));
    let commit = g.commit(20, 10, vec![first, second]);
    let reply = g.submit(&mut source, ParticipantCommand::Direct(commit)).await;
    assert!(matches!(reply, ParticipantReply::Direct(direct::DirectReply::Applied { .. })));
    let members = vec![
        g.commit(21, 11, vec![g.write(3, None, Some(serde_json::json!({"id": 3})))]),
        g.commit(22, 12, vec![g.write(1, Some(9), Some(serde_json::json!({"id": 1})))]),
        g.commit(23, 13, vec![g.write(2, Some(10), None)]),
    ];
    g.submit(&mut source, ParticipantCommand::DirectBatch(DirectBatch { members }))
        .await;
    g.submit(
        &mut source,
        ParticipantCommand::DirectForward(DirectForward {
            authority: AUTHORITY(),
            table_id: id(1),
            anchor: Key::Int64(3),
            request_id: id(24),
            digest: digest(8),
            applied_unix_ms: 5_000,
        }),
    )
    .await;

    // The queued path: put, delete and keep.
    let writes = vec![
        PreparedWrite {
            key: row_key(1),
            expected_version: Some(ts(10)),
            change: Change::Put(g.json(serde_json::json!({"id": 1, "body": "c"}))),
        },
        PreparedWrite {
            key: row_key(2),
            expected_version: Some(ts(13)),
            change: Change::Keep,
        },
        PreparedWrite {
            key: row_key(3),
            expected_version: Some(ts(11)),
            change: Change::Delete,
        },
        PreparedWrite {
            key: row_key(4),
            expected_version: None,
            change: Change::Put(g.json(serde_json::json!({"id": 4}))),
        },
    ];
    let txn = TxnId {
        request_id: id(30),
        attempt: 0,
    };
    let request = RequestBinding {
        run_id: id(2),
        request_id: txn.request_id,
        digest: digest(1),
        accepted_context: digest(2),
    };
    let manifest = ManifestRef {
        status_group_id: id(31),
        digest: digest(3),
        participant_count: 1,
    };
    let mut plan = ParticipantPlan {
        group: group(10),
        schema_epoch: 1,
        routing_epoch: 1,
        read_ts: ts(14),
        write_root: digest(0),
        write_count: writes.len() as u64,
        page_count: 1,
    };
    let root = initial_root(txn, &plan).unwrap();
    plan.write_root = page_root(root, 0, &writes).unwrap();
    let guard = |guard_id, read_ts, mode| {
        lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id,
                catalog_group_id: id(7),
                table_id: id(1),
                lifecycle_version: None,
                schema_epoch: 1,
                txn,
                request,
                read_ts,
                authority: AUTHORITY(),
            },
            mode,
        )
        .into_sealed()
    };
    let prepared = g
        .submit(
            &mut source,
            ParticipantCommand::PreparePage {
                authority: AUTHORITY(),
                txn,
                request,
                manifest,
                plan,
                page_index: 0,
                previous_root: root,
                guards: Page::new(vec![guard(id(32), ts(14), lifecycle::GuardMode::Shared)]).unwrap(),
                writes: Page::new(writes).unwrap(),
            },
        )
        .await;
    assert!(matches!(prepared, ParticipantReply::PreparedPage { writes: 4, .. }), "{prepared:?}");
    let (_, staged) = stored(&pool, id(10));
    g.submit(
        &mut source,
        ParticipantCommand::Seal {
            authority: AUTHORITY(),
            txn,
            manifest,
            result_digest: digest(4),
        },
    )
    .await;
    let resolved = g
        .submit(
            &mut source,
            ParticipantCommand::Resolve {
                authority: AUTHORITY(),
                proof: SealedStatusDecision::seal(DecisionStatement {
                    txn,
                    request,
                    manifest,
                    decided_at: AppliedPosition {
                        term: 1,
                        node_id: 1,
                        index: 1,
                    },
                    decision: TxnDecision::Commit { commit_ts: ts(15) },
                })
                .unwrap(),
                max_items: 128,
            },
        )
        .await;
    assert!(matches!(resolved, ParticipantReply::Resolved { complete: true, .. }));

    // A split: barrier, freeze, image pages and imports, with their roots.
    let split_guard = guard(id(40), ts(15), lifecycle::GuardMode::Exclusive);
    g.submit(
        &mut source,
        ParticipantCommand::DirectPause(DirectPause {
            authority: AUTHORITY(),
            split_id: id(40),
            paused: true,
        }),
    )
    .await;
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
    let bindings = [0usize, 1].map(|i| SplitBinding {
        split_id: id(40),
        source: group(10),
        source_schema_epoch: 1,
        source_routing_epoch: 1,
        source_span: full.clone(),
        target: group(11 + i as u128),
        target_span: spans[i].clone(),
        read_ts: ts(20),
        guard: split_guard.clone(),
    });
    let frozen = g
        .submit(
            &mut source,
            ParticipantCommand::SplitFreeze {
                authority: AUTHORITY(),
                children: Box::new(bindings.clone()),
            },
        )
        .await;
    assert!(matches!(frozen, ParticipantReply::SplitSourceFrozen(_)), "{frozen:?}");
    let mut children = Vec::new();
    for (i, binding) in bindings.iter().enumerate() {
        let mut child = Replica {
            machine: ParticipantMachine::new(pool.group(group(11 + i as u128)).await.unwrap()).unwrap(),
            index: 0,
        };
        g.submit(&mut child, fence(2, spans[i].clone())).await;
        g.submit(&mut child, activate()).await;
        let staged = g
            .submit(
                &mut child,
                ParticipantCommand::SplitStage {
                    authority: AUTHORITY(),
                    binding: Box::new(binding.clone()),
                },
            )
            .await;
        assert_eq!(staged, ParticipantReply::SplitStaged);
        children.push(child);
    }
    let mut images = Sha256::new();
    let request = OwnedCommand::new_owned(
        bindings[0].image_request(None, 64 << 10),
        &shared,
        ByteClass::Data,
    )
    .unwrap();
    let page = source.machine.committed_image_page(request).await.unwrap();
    images.update(serde_json::to_vec(page.payload().unwrap()).unwrap());
    assert!(matches!(page.payload().unwrap(), ImagePage::Images { exhausted: true, .. }));
    for (binding, child) in bindings.iter().zip(&mut children) {
        let root = lineage::initial_root(binding).unwrap();
        let (command, _) = binding
            .import_page_from_image(&shared, 0, root, None, page.clone())
            .await
            .unwrap();
        let imported = g.submit_owned(child, command).await;
        assert!(
            matches!(imported, ParticipantReply::SplitImported { complete: true, .. }),
            "{imported:?}"
        );
    }

    let recorded = [
        ("entries", hex(std::mem::take(&mut g.entries))),
        ("images", hex(images)),
        ("staged", staged),
        ("source", format!("{:?}", stored(&pool, id(10)))),
        ("left", format!("{:?}", stored(&pool, id(11)))),
        ("right", format!("{:?}", stored(&pool, id(12)))),
    ];
    let _ = std::fs::remove_dir_all(&dir);
    for (actual, expected) in recorded.iter().zip(GOLDEN) {
        assert_eq!(actual.0, expected.0);
        assert_eq!(actual.1, expected.1, "{} changed: {recorded:#?}", actual.0);
    }
}

const GOLDEN: [(&str, &str); 6] = [
    ("entries", "203674d5bdef7cd96d291d3dfce51bb5e51c991bfade841a2a94fd8b01b97775"),
    ("images", "005de1c277e22890f7c6c5f1f4d9eb61000cf4054460cb8f6bfc9ceda0935560"),
    ("staged", "9d8904dbf1a5497269977ac4bff4202894a8c3144d0c04619cd2edec6651cd44"),
    ("source", "(40, \"f1d6db39e9dec62636409bf626dc64942e62bb58ee5659f7e30af1c3e6f29cf4\")"),
    ("left", "(20, \"a03b8184c681360f6084d2c2bf61f9764131fb61944445d1a9bacb6e5defb8eb\")"),
    ("right", "(18, \"ab8db7d23d9f670c0825655f6ee8060a6323c29b7ea0bada619540b74c60ad36\")"),
];
