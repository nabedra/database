//! T7 store evidence: the `data.direct.v1` gate on a Data group and the DDL
//! schema fence (plan section 4 "Mixed versions" and section 7). Direct
//! commits are driven directly, as the leader executor (T5) would propose
//! them; the log order is the apply order, so a race between a writer and a
//! DDL is an interleaving of entries.
use super::*;

fn schema_change(f: &Fixture, change: DirectSchemaChange) -> ParticipantCommand {
    ParticipantCommand::DirectSchema(DirectSchemaFence {
        authority: f.authority,
        table_id: f.table,
        change,
    })
}
fn schema_reply(epoch: u64, lifecycle: Option<Uuid>) -> ParticipantReply {
    ParticipantReply::DirectSchema(DirectSchema { epoch, lifecycle })
}
fn fence(f: &Fixture, lifecycle: Option<Uuid>) -> ParticipantCommand {
    fence_at(f, lifecycle, 1)
}
/// DDL-FENCE: the fence of a coordinator that probed epoch `expected`.
fn fence_at(f: &Fixture, lifecycle: Option<Uuid>, expected: u64) -> ParticipantCommand {
    schema_change(
        f,
        DirectSchemaChange::Fence {
            lifecycle,
            expected_epoch: Some(expected),
        },
    )
}
fn index_key(f: &Fixture, index_id: Uuid, key: i64) -> MvccKey {
    MvccKey::IndexEntry {
        table_id: f.table,
        index_id,
        values: Page::new(vec![IndexValue::Int64(key)]).unwrap(),
        primary_key: Key::Int64(key),
    }
}
/// A one-row insert of `key` computed from the descriptor at `epoch`; with
/// `index`, the descriptor includes that (newly published) index.
fn insert(f: &Fixture, key: i64, commit_ts: u64, epoch: u64, index: Option<Uuid>) -> DirectCommit {
    let mut write = f.write(key, None, Some(serde_json::json!({"id": key})));
    if let Some(index_id) = index {
        write.index.push(IndexChange {
            key: index_key(f, index_id, key),
            expected_version: None,
            value: Some(f.json(serde_json::json!({"id": key}))),
        });
    }
    let mut commit = f.commit(nabedra_rt::ids::uuid_v4(), 1, commit_ts, vec![write]);
    commit.schema_epoch = epoch;
    commit
}
async fn schema_of(f: &Fixture) -> Option<DirectSchema> {
    let raw = f
        .machine()
        .store()
        .read_metadata(BUDGET, |view| {
            let raw = view.pinned(CONFIG)?.unwrap();
            let config: Config = serde_json::from_slice(&raw).unwrap();
            view.encode(&config.direct_schema)
        })
        .await
        .unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

#[tokio::test]
async fn o02_t7_the_store_gate_rejects_direct_family_commands_before_activation() {
    let mut f = Fixture::inactive(nabedra_rt::ids::uuid_v4(), full_span()).await;
    let before = f.dump().await;
    assert_eq!(schema_of(&f).await, None);
    let commands = [
        ParticipantCommand::Direct(f.put(1, None, 10)),
        ParticipantCommand::DirectForward(f.forward_of(1, nabedra_rt::ids::uuid_v4(), 1)),
        ParticipantCommand::DirectExpire(DirectExpire {
            authority: f.authority,
            cutoff_unix_ms: 10,
            retention_ms: 1,
            max_items: 1,
        }),
        ParticipantCommand::DirectPause(DirectPause {
            authority: f.authority,
            split_id: nabedra_rt::ids::uuid_v4(),
            paused: true,
        }),
    ];
    for command in commands {
        let reply = f.submit(command.clone()).await;
        assert!(
            reply == rejected(Fault::DirectInactive)
                || reply == ParticipantReply::Rejected(Fault::DirectInactive),
            "{command}: {reply:?}"
        );
        assert_eq!(
            f.dump().await,
            before,
            "{command} wrote state before activation"
        );
    }
    // Activation opens the gate at the group's schema epoch, bound to the
    // table's current lifecycle.
    let activation = f.activation();
    assert_eq!(f.submit(activation).await, schema_reply(1, Some(f.table)));
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    // A repeated activation changes nothing.
    let again = schema_change(
        &f,
        DirectSchemaChange::Activate {
            lifecycle: nabedra_rt::ids::uuid_v4(),
        },
    );
    assert_eq!(f.submit(again).await, schema_reply(1, Some(f.table)));
}

#[tokio::test]
async fn o02_t7_direct_schema_is_a_new_tag_and_an_active_config_is_unreadable_by_an_older_build() {
    let mut f = Fixture::new().await;
    let command = fence(&f, Some(nabedra_rt::ids::uuid_v4()));
    assert!(command.is_direct(), "gated like Direct");
    let value = serde_json::to_value(&command).unwrap();
    assert!(value.get("direct_schema").is_some());
    let reply = serde_json::to_value(schema_reply(2, None)).unwrap();
    assert!(reply.get("direct_schema").is_some());
    // An older participant decoder does not know the variant (fail closed).
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "snake_case", deny_unknown_fields)]
    #[allow(dead_code)]
    enum OlderCommand {
        Direct(serde_json::Value),
        DirectExpire(serde_json::Value),
        DirectForward(serde_json::Value),
        DirectPause(serde_json::Value),
        ChildPublished(serde_json::Value),
    }
    assert!(serde_json::from_value::<OlderCommand>(value).is_err());
    // Nested in a scoped envelope it is refused at apply.
    let before = f.dump().await;
    let scoped = ParticipantCommand::Scoped {
        scope: nabedra_oltp_types::authority::ScopedCatalogRead {
            queue_group_id: nabedra_rt::ids::uuid_v4(),
            data_generation: nabedra_rt::ids::uuid_v4(),
            queue_generation: nabedra_rt::ids::uuid_v4(),
            queue_view: nabedra_oltp_types::types::AuthorityView {
                run_id: f.authority.run_id,
                owner_term: f.authority.owner_term,
                membership_revision: 1,
            },
            request: RequestBinding {
                run_id: f.authority.run_id,
                request_id: nabedra_rt::ids::uuid_v4(),
                digest: digest(1),
                accepted_context: digest(2),
            },
            table_id: f.table,
            read_ts: ts(9),
        },
        command: OwnedCommand::new_owned(command, f.shared(), ByteClass::Data).unwrap(),
    };
    assert_eq!(
        f.submit(scoped).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(f.dump().await, before);
    // Downgrade: the config of an activated group carries `direct_schema`,
    // which the previous build's config type (deny_unknown_fields) rejects.
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct OlderConfig {
        catalog_group_id: Uuid,
        authority: Authority,
        schema_epoch: u64,
        routing_epoch: u64,
        span: KeySpan,
        gc_floor: u64,
        #[serde(default)]
        split_source: Option<serde_json::Value>,
        #[serde(default)]
        split_child: Option<serde_json::Value>,
        #[serde(default)]
        direct_pause: Option<Uuid>,
    }
    let raw = f
        .machine()
        .store()
        .read_metadata(BUDGET, |view| {
            let raw = view.pinned(CONFIG)?.unwrap();
            view.copy_output(&raw)
        })
        .await
        .unwrap();
    assert!(serde_json::from_slice::<OlderConfig>(raw.as_slice()).is_err());
    let inactive = Fixture::inactive(nabedra_rt::ids::uuid_v4(), full_span()).await;
    let raw = inactive
        .machine()
        .store()
        .read_metadata(BUDGET, |view| {
            let raw = view.pinned(CONFIG)?.unwrap();
            view.copy_output(&raw)
        })
        .await
        .unwrap();
    serde_json::from_slice::<OlderConfig>(raw.as_slice())
        .expect("a group never activated stays readable by the older build");
}

/// O08 / D11: an index publication races direct writers. Every entry the
/// DDL fence follows in the log fails unless it was computed from the new
/// descriptor, so no row committed after the fence lacks the new index.
#[tokio::test]
async fn o08_o12_d11_direct_writes_racing_an_index_publication_use_the_new_descriptor() {
    for seed in 1..=8u64 {
        let mut f = Fixture::new().await;
        let index_id = nabedra_rt::ids::uuid_v4();
        let published = nabedra_rt::ids::uuid_v4();
        let mut rng = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let fence_at = 2 + next() % 12;
        let mut epoch_seen = 1u64;
        let mut descriptor_has_index = false;
        let mut commit_ts = 10u64;
        let mut fenced = false;
        let mut rows: Vec<(i64, bool)> = Vec::new();
        let mut key = 0i64;
        for step in 0..16u64 {
            if step == fence_at {
                // The DDL coordinator fences before the Catalog publishes.
                assert_eq!(
                    f.submit(fence(&f, Some(published))).await,
                    schema_reply(2, Some(published))
                );
                fenced = true;
            }
            // A leader computes from its cached descriptor and epoch.
            key += 1;
            commit_ts += 1;
            let commit = insert(
                &f,
                key,
                commit_ts,
                epoch_seen,
                descriptor_has_index.then_some(index_id),
            );
            let reply = f.direct(&commit).await;
            if fenced && epoch_seen == 1 {
                assert_eq!(
                    reply,
                    rejected(Fault::StaleEpoch),
                    "seed {seed} step {step}"
                );
                // The leader refreshes: the group's schema names the
                // published lifecycle, and the Catalog now serves it.
                let schema = schema_of(&f).await.unwrap();
                assert!(!schema.admits(f.table), "the old descriptor is retired");
                assert!(schema.admits(published));
                epoch_seen = schema.epoch;
                descriptor_has_index = true;
                commit_ts += 1;
                let retry = DirectCommit {
                    commit_ts: ts(commit_ts),
                    ..insert(&f, key, commit_ts, epoch_seen, Some(index_id))
                };
                assert_eq!(f.direct(&retry).await, applied(commit_ts));
            } else {
                assert_eq!(reply, applied(commit_ts), "seed {seed} step {step}");
            }
            rows.push((key, fenced));
        }
        // A repeated fence for the same publication is a no-op.
        assert_eq!(
            f.submit(fence(&f, Some(published))).await,
            schema_reply(2, Some(published))
        );
        for (key, after_fence) in rows {
            // Decode and release the charged read before the next one: a
            // held charge would make the next read wait for its memory.
            let entry: serde_json::Value = {
                let raw = f
                    .machine()
                    .read_at(ts(commit_ts), index_key(&f, index_id, key))
                    .await
                    .unwrap();
                serde_json::from_slice(raw.as_slice()).unwrap()
            };
            assert_eq!(
                entry["value"]["version"].is_u64(),
                after_fence,
                "seed {seed}: row {key} index entry (after fence: {after_fence})"
            );
            assert!(f.version(commit_ts, key).await.is_some());
        }
    }
}

/// O08 / D11: a drop fence stops every direct commit on the table's groups,
/// whatever epoch it carries, while the queued path keeps its own epochs.
#[tokio::test]
async fn o08_d11_a_drop_fence_stops_every_direct_commit_and_leaves_queued_epochs_alone() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    let racing = f.put(2, None, 11);
    assert_eq!(f.submit(fence(&f, None)).await, schema_reply(2, None));
    for epoch in [1, 2, 3] {
        let mut commit = racing.clone();
        commit.schema_epoch = epoch;
        assert_rejected_without_writes(&mut f, &commit, Fault::StaleEpoch).await;
    }
    assert_eq!(
        f.version(20, 2).await,
        None,
        "no row lands in a dropped table"
    );
    // The repeated drop fence is a no-op.
    assert_eq!(f.submit(fence(&f, None)).await, schema_reply(2, None));
    // Queued writers keep the group's own schema epoch (1): the direct fence
    // does not touch it, so a DDL that fails after fencing leaves the queued
    // path working.
    let write = f.queued_put(3, None, serde_json::json!({"queued": true}));
    let plan = f.queued_plan(20, std::slice::from_ref(&write));
    assert!(matches!(
        f.prepare(&plan, vec![write]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    f.seal_and_resolve(&plan, 21).await;
    assert_eq!(f.read(21, 3).await["value"]["value"]["queued"], true);
    // If the drop never published, a later DDL fence binds a lifecycle again.
    let later = nabedra_rt::ids::uuid_v4();
    assert_eq!(
        f.submit(fence_at(&f, Some(later), 2)).await,
        schema_reply(3, Some(later))
    );
    assert_eq!(f.direct(&insert(&f, 4, 30, 3, None)).await, applied(30));
}

/// The fence and activation are replicated state: they survive a run fence,
/// a second replica replaying the same log ends identical, and an
/// activation that arrives after a DDL fence never undoes it.
#[tokio::test]
async fn o02_d11_schema_fences_are_deterministic_and_survive_a_run_fence() {
    let mut f = Fixture::inactive(nabedra_rt::ids::uuid_v4(), full_span()).await;
    let mut other = f.replica_of_inactive().await;
    let published = nabedra_rt::ids::uuid_v4();
    let log = [
        fence(&f, Some(published)),
        // The activator read the old lifecycle before the DDL began; its
        // entry lands after the DDL fence and must not undo it.
        f.activation(),
    ];
    for command in log {
        let mine = f.submit(command.clone()).await;
        let theirs = other.submit(command).await;
        assert_eq!(mine, theirs);
    }
    assert_eq!(
        schema_of(&f).await,
        Some(DirectSchema {
            epoch: 2,
            lifecycle: Some(published)
        })
    );
    let next = Authority {
        run_id: f.authority.run_id,
        owner_term: 2,
    };
    let run_fence = ParticipantCommand::Fence {
        transition_id: nabedra_rt::ids::uuid_v4(),
        expected: Some(f.authority),
        next,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        catalog_group_id: f.catalog_group_id,
    };
    assert!(matches!(
        f.submit(run_fence.clone()).await,
        ParticipantReply::Fenced(_)
    ));
    assert!(matches!(
        other.submit(run_fence).await,
        ParticipantReply::Fenced(_)
    ));
    f.authority = next;
    assert_eq!(
        schema_of(&f).await,
        Some(DirectSchema {
            epoch: 2,
            lifecycle: Some(published)
        })
    );
    let commit = insert(&f, 1, 10, 2, None);
    assert_eq!(f.direct(&commit).await, applied(10));
    assert_eq!(
        other.submit(ParticipantCommand::Direct(commit)).await,
        applied(10)
    );
    // A fence under the old authority is refused and changes nothing.
    let mut stale = fence(&f, Some(nabedra_rt::ids::uuid_v4()));
    if let ParticipantCommand::DirectSchema(fence) = &mut stale {
        fence.authority.owner_term = 1;
    }
    let before = f.dump().await;
    assert_eq!(
        f.submit(stale.clone()).await,
        ParticipantReply::Rejected(Fault::StaleAuthority)
    );
    other.submit(stale).await;
    assert_eq!(f.dump().await, before);
    assert_eq!(f.dump().await, other.dump().await);
    // A fence for another table's id is malformed.
    let mut foreign = fence(&f, None);
    if let ParticipantCommand::DirectSchema(fence) = &mut foreign {
        fence.table_id = nabedra_rt::ids::uuid_v4();
    }
    assert_eq!(
        f.submit(foreign).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
}

/// DDL-FENCE: a late copy of an earlier DDL's fence is refused and never
/// reinstalls that DDL's lifecycle (the shipped fence bumped the epoch for any
/// other lifecycle); the probe reports without changing anything.
#[tokio::test]
async fn d11_ddl_fence_a_late_fence_of_an_earlier_ddl_is_refused() {
    let mut f = Fixture::new().await;
    let (first, second) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
    let probe = schema_change(
        &f,
        DirectSchemaChange::Fence {
            lifecycle: Some(first),
            expected_epoch: None,
        },
    );
    assert_eq!(f.submit(probe.clone()).await, schema_reply(1, Some(f.table)));
    let late = fence_at(&f, Some(first), 1);
    assert_eq!(f.submit(late.clone()).await, schema_reply(2, Some(first)));
    // A retry of the same fence is a no-op.
    assert_eq!(f.submit(late.clone()).await, schema_reply(2, Some(first)));
    assert_eq!(
        f.submit(fence_at(&f, Some(second), 2)).await,
        schema_reply(3, Some(second))
    );
    let before = f.dump().await;
    assert_eq!(
        f.submit(late).await,
        ParticipantReply::Rejected(Fault::Conflict),
        "the late fence of the first DDL"
    );
    assert_eq!(
        f.submit(fence_at(&f, None, 1)).await,
        ParticipantReply::Rejected(Fault::Conflict),
        "a stale drop fence"
    );
    assert_eq!(f.submit(probe).await, schema_reply(3, Some(second)));
    assert_eq!(f.dump().await, before);
    assert_eq!(f.direct(&insert(&f, 1, 10, 3, None)).await, applied(10));
}
