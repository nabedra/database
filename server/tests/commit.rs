use meshdb_server::{
    commit::{Coordinator, PendingObject},
    protocol::{ColumnType, ErrorCode, Key, Outcome, TableFormat},
    storage::*,
};
#[path = "support/storage.rs"]
mod fake;
use fake::{Fake, ReadGate};
use std::{collections::BTreeMap, sync::Arc, task::Poll};
use uuid::Uuid;

async fn coordinator() -> Coordinator<Fake> {
    let storage = Storage::new(Fake::default(), Namespace::new("test").unwrap());
    storage.initialize_catalog().await.unwrap();
    Coordinator::new(storage)
}
fn create(candidate: &mut Catalog) {
    let id = Uuid::from_u128(42);
    candidate.tables.insert(
        "users".into(),
        CatalogEntry {
            id,
            schema: BTreeMap::from([("id".into(), ColumnType::Utf8)]),
            key: "id".into(),
            format: TableFormat::Parquet,
            data: meshdb_server::storage::TableData::Legacy {
                snapshot: ObjectKey::snapshot(id, candidate.commit_id).as_str().into(),
            },
            row_count: 0,
        },
    );
}
fn fault(c: &Coordinator<Fake>, key: &str, error: StorageError, apply: bool) {
    c.storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .put_faults
        .insert(format!("test/{key}"), (error, apply));
}
fn assert_error(e: meshdb_server::protocol::ProtocolError, code: ErrorCode, outcome: Outcome) {
    assert_eq!((e.code, e.outcome), (code, outcome));
}
#[tokio::test]
async fn snapshot_then_catalog_and_drop_use_fresh_revision_and_uuid() {
    let c = coordinator().await;
    let session = c.begin().await.unwrap();
    let original = session.catalog().clone();
    let mut candidate = session.candidate();
    assert_eq!(candidate.commit_id, session.commit_id());
    assert_ne!(candidate.commit_id, original.commit_id);
    assert_eq!(candidate.commit_id.get_version_num(), 4);
    create(&mut candidate);
    let snapshot = candidate.tables["users"]
        .legacy_snapshot()
        .unwrap()
        .to_owned();
    let committed = session
        .publish(
            candidate.clone(),
            Some(b"closed validated parquet".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(committed.catalog, candidate);
    assert_eq!(original.tables.len(), 0);
    let session = c.begin().await.unwrap();
    assert_eq!(session.catalog(), &candidate);
    let mut updated = session.candidate();
    let entry = updated.tables.get_mut("users").unwrap();
    entry.row_count = 1;
    entry.data = meshdb_server::storage::TableData::Legacy {
        snapshot: ObjectKey::snapshot(entry.id, updated.commit_id)
            .as_str()
            .into(),
    };
    let updated_snapshot = entry.legacy_snapshot().unwrap().to_owned();
    session
        .publish(updated.clone(), Some(vec![2]))
        .await
        .unwrap();
    let session = c.begin().await.unwrap();
    let mut dropped = session.candidate();
    dropped.tables.clear();
    assert_eq!(dropped.revision, 3);
    assert_ne!(dropped.commit_id, candidate.commit_id);
    session.publish(dropped, None).await.unwrap();
    let state = c.storage().backend().state.lock().unwrap();
    assert_eq!(
        state.put_keys,
        vec![
            "test/catalog.json".to_string(),
            format!("test/{snapshot}"),
            "test/catalog.json".into(),
            format!("test/{updated_snapshot}"),
            "test/catalog.json".into(),
            "test/catalog.json".into()
        ]
    );
    assert!(state.objects.contains_key(&format!("test/{snapshot}")));
}
#[tokio::test]
async fn snapshot_failures_never_publish_and_catalog_conflict_preserves_orphan() {
    for error in [
        StorageError::Conflict,
        StorageError::Unavailable,
        StorageError::Ambiguous,
    ] {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        create(&mut candidate);
        fault(
            &c,
            candidate.tables["users"].legacy_snapshot().unwrap(),
            error,
            error == StorageError::Ambiguous,
        );
        let e = session.publish(candidate, Some(vec![1])).await.unwrap_err();
        assert_error(
            e,
            if error == StorageError::Conflict {
                ErrorCode::Conflict
            } else {
                ErrorCode::StorageUnavailable
            },
            Outcome::NotApplied,
        );
        assert_eq!(
            c.storage().load_catalog().await.unwrap().catalog.revision,
            0
        );
        assert_eq!(c.storage().backend().state.lock().unwrap().puts, 2);
    }
    for error in [StorageError::Conflict, StorageError::Unavailable] {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        create(&mut candidate);
        if error == StorageError::Conflict {
            // A real external ETag change makes the session's condition stale.
            let loaded = c.storage().load_catalog().await.unwrap();
            c.storage()
                .put(
                    &ObjectKey::catalog(),
                    loaded.catalog.encode().unwrap(),
                    WriteCondition::Match(&loaded.etag),
                )
                .await
                .unwrap();
        } else {
            fault(&c, "catalog.json", error, false);
        }
        let snapshot = candidate.tables["users"]
            .legacy_snapshot()
            .unwrap()
            .to_owned();
        assert_error(
            session.publish(candidate, Some(vec![1])).await.unwrap_err(),
            if error == StorageError::Conflict {
                ErrorCode::Conflict
            } else {
                ErrorCode::StorageUnavailable
            },
            Outcome::NotApplied,
        );
        assert_eq!(
            c.storage().load_catalog().await.unwrap().catalog.revision,
            0
        );
        assert!(
            c.storage()
                .backend()
                .state
                .lock()
                .unwrap()
                .objects
                .contains_key(&format!("test/{snapshot}"))
        );
    }
}
#[tokio::test]
async fn ambiguous_catalog_is_confirmed_only_by_commit_uuid() {
    for (apply, read_failure) in [(true, false), (false, false), (true, true)] {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        create(&mut candidate);
        fault(&c, "catalog.json", StorageError::Ambiguous, apply);
        c.storage().backend().state.lock().unwrap().read_failure = read_failure;
        let result = session.publish(candidate.clone(), Some(vec![1])).await;
        if apply && !read_failure {
            assert_eq!(result.unwrap().catalog, candidate);
        } else {
            assert_error(
                result.unwrap_err(),
                ErrorCode::OutcomeUnknown,
                Outcome::Unknown,
            );
        }
        let state = c.storage().backend().state.lock().unwrap();
        assert_eq!(state.puts, 3);
        assert_eq!(state.gets, 3); // initialization, admission, one reconciliation
    }
}
#[tokio::test]
async fn reconciliation_keeps_lock_and_next_session_loads_fresh_catalog() {
    let c = Arc::new(coordinator().await);
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    create(&mut candidate);
    let expected = candidate.clone();
    fault(&c, "catalog.json", StorageError::Ambiguous, true);
    let gate = Arc::new(ReadGate::default());
    c.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
    let publish = async move { session.publish(candidate, Some(vec![1])).await };
    tokio::pin!(publish);
    // Drive publication until reconciliation is gated, without sleeps or scheduling guesses.
    assert!(std::future::poll_fn(|cx| Poll::Ready(publish.as_mut().poll(cx).is_pending())).await);
    gate.entered.acquire().await.unwrap().forget();
    let next = c.begin();
    tokio::pin!(next);
    assert!(std::future::poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx).is_pending())).await);
    assert_eq!(c.storage().backend().state.lock().unwrap().gets, 2);
    gate.release.add_permits(1);
    publish.await.unwrap();
    assert_eq!(next.await.unwrap().catalog(), &expected);
}
#[tokio::test]
async fn malformed_candidates_and_overflow_never_write() {
    let c = coordinator().await;
    for mode in 0..6 {
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        create(&mut candidate);
        match mode {
            0 => candidate.revision = 10,
            1 => {
                candidate.tables.get_mut("users").unwrap().data =
                    meshdb_server::storage::TableData::Legacy {
                        snapshot: ObjectKey::snapshot(Uuid::from_u128(42), Uuid::nil())
                            .as_str()
                            .into(),
                    };
            }
            2 => {
                let mut second = candidate.tables["users"].clone();
                second.id = Uuid::from_u128(43);
                second.data = meshdb_server::storage::TableData::Legacy {
                    snapshot: ObjectKey::snapshot(second.id, candidate.commit_id)
                        .as_str()
                        .into(),
                };
                candidate.tables.insert("other".into(), second);
            }
            3 => candidate.commit_id = Uuid::nil(),
            4 => candidate.tables.clear(),
            _ => candidate.tables.get_mut("users").unwrap().row_count = 100_001,
        }
        assert_error(
            session.publish(candidate, Some(vec![1])).await.unwrap_err(),
            ErrorCode::ValidationError,
            Outcome::NotApplied,
        );
    }
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    create(&mut candidate);
    assert_error(
        session
            .publish(
                candidate,
                Some(vec![
                    0;
                    meshdb_server::commit::MAX_ENCODED_SNAPSHOT_BYTES + 1
                ]),
            )
            .await
            .unwrap_err(),
        ErrorCode::LimitExceeded,
        Outcome::NotApplied,
    );
    assert_eq!(c.storage().backend().state.lock().unwrap().puts, 1);
    let loaded = c.storage().load_catalog().await.unwrap();
    let mut overflow = loaded.catalog;
    overflow.revision = u64::MAX;
    c.storage()
        .put(
            &ObjectKey::catalog(),
            overflow.encode().unwrap(),
            WriteCondition::Match(&loaded.etag),
        )
        .await
        .unwrap();
    assert_error(
        c.begin().await.err().unwrap(),
        ErrorCode::LimitExceeded,
        Outcome::NotApplied,
    );
    assert_eq!(c.storage().backend().state.lock().unwrap().puts, 2);
}

// Publication trusts the caller's closed codec output; small bytes isolate storage behavior.
fn partitions(candidate: &mut Catalog) -> Vec<PendingObject> {
    create(candidate);
    let entry = candidate.tables.get_mut("users").unwrap();
    entry.row_count = 2;
    let boundary = Some(Key::Utf8("m".into()));
    let files: Vec<_> = (0..2)
        .map(|ordinal| PartitionFile {
            object: ObjectKey::partition(entry.id, candidate.commit_id, ordinal)
                .as_str()
                .into(),
            range: KeyRange {
                lower: if ordinal == 0 { None } else { boundary.clone() },
                upper: if ordinal == 0 { boundary.clone() } else { None },
            },
            row_count: 1,
            byte_size: 3,
            payload_bytes: 1,
        })
        .collect();
    let uploads = files
        .iter()
        .map(|file| PendingObject {
            key: ObjectKey::parse_partition(&file.object, entry.id).unwrap(),
            bytes: vec![1, 2, 3],
        })
        .collect();
    entry.data = TableData::Partitioned { files };
    uploads
}
#[tokio::test]
async fn multiple_objects_finish_before_one_catalog_put_and_failure_keeps_old_catalog() {
    for error in [
        None,
        Some(StorageError::Unavailable),
        Some(StorageError::Ambiguous),
        Some(StorageError::Conflict),
    ] {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let original = session.catalog().clone();
        let mut candidate = session.candidate();
        let uploads = partitions(&mut candidate);
        let keys: Vec<_> = uploads
            .iter()
            .map(|o| format!("test/{}", o.key.as_str()))
            .collect();
        if let Some(error) = error {
            fault(
                &c,
                uploads[1].key.as_str(),
                error,
                error == StorageError::Ambiguous,
            );
        }
        let result = session.publish_partitions(candidate.clone(), uploads).await;
        if let Some(error) = error {
            assert_error(
                result.unwrap_err(),
                if error == StorageError::Conflict {
                    ErrorCode::Conflict
                } else {
                    ErrorCode::StorageUnavailable
                },
                Outcome::NotApplied,
            );
            assert_eq!(c.storage().load_catalog().await.unwrap().catalog, original);
        } else {
            assert_eq!(result.unwrap().catalog, candidate);
        }
        let state = c.storage().backend().state.lock().unwrap();
        let mut expected = vec!["test/catalog.json".to_string()];
        expected.extend(keys.clone());
        if error.is_none() {
            expected.push("test/catalog.json".into());
        }
        assert_eq!(state.put_keys, expected);
        assert!(state.objects.contains_key(&keys[0]));
    }
}
#[tokio::test]
async fn partition_references_and_complete_catalog_are_validated_before_any_upload() {
    for mode in 0..12 {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        let mut uploads = partitions(&mut candidate);
        match mode {
            0 => {
                uploads.pop();
            }
            1 => uploads.push(PendingObject {
                key: ObjectKey::partition(Uuid::from_u128(42), candidate.commit_id, 9),
                bytes: vec![1; 3],
            }),
            2 => uploads.push(PendingObject {
                key: uploads[0].key.clone(),
                bytes: uploads[0].bytes.clone(),
            }),
            3 => uploads[1].bytes.pop().map(|_| ()).unwrap(),
            4 | 5 => {
                let entry = candidate.tables.get_mut("users").unwrap();
                let TableData::Partitioned { files } = &mut entry.data else {
                    unreachable!()
                };
                let key = ObjectKey::partition(
                    if mode == 4 {
                        Uuid::from_u128(99)
                    } else {
                        entry.id
                    },
                    Uuid::new_v4(),
                    1,
                );
                files[1].object = key.as_str().into();
                uploads[1].key = key;
            }
            6 => candidate.revision += 1,
            7 => candidate.commit_id = Uuid::new_v4(),
            8 => candidate.tables.get_mut("users").unwrap().row_count = 3,
            9 => {
                let other = candidate.tables["users"].clone();
                candidate.tables.insert("other".into(), other);
            }
            10 => {
                let TableData::Partitioned { files } =
                    &mut candidate.tables.get_mut("users").unwrap().data
                else {
                    unreachable!()
                };
                files[1].byte_size =
                    (meshdb_server::partition_size::MAX_PARTITION_BYTES + 1) as u64;
            }
            _ => {
                let TableData::Partitioned { files } =
                    &mut candidate.tables.get_mut("users").unwrap().data
                else {
                    unreachable!()
                };
                files[1].object = files[0].object.clone();
            }
        }
        assert!(
            session
                .publish_partitions(candidate, uploads)
                .await
                .is_err(),
            "mode {mode}"
        );
        assert_eq!(
            c.storage().backend().state.lock().unwrap().puts,
            1,
            "mode {mode}"
        );
    }
}
#[tokio::test]
async fn migration_reuse_range_expansion_empty_and_drop_need_only_new_objects() {
    let c = coordinator().await;
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    create(&mut candidate);
    session.publish(candidate, Some(vec![1])).await.unwrap();
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    let uploads = partitions(&mut candidate);
    session
        .publish_partitions(candidate, uploads)
        .await
        .unwrap();
    // Delete the first partition's rows and expand the retained file's routing range.
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    let entry = candidate.tables.get_mut("users").unwrap();
    entry.row_count = 1;
    let TableData::Partitioned { files } = &mut entry.data else {
        unreachable!()
    };
    files.remove(0);
    files[0].range.lower = None;
    session.publish_partitions(candidate, vec![]).await.unwrap();
    for mode in 0..6 {
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        let entry = candidate.tables.get_mut("users").unwrap();
        let TableData::Partitioned { files } = &mut entry.data else {
            unreachable!()
        };
        match mode {
            0 => files[0].byte_size += 1,
            1 => files[0].payload_bytes += 1,
            2 => {
                files[0].row_count += 1;
                entry.row_count += 1;
            }
            3 => {
                files[0].object = ObjectKey::partition(entry.id, Uuid::new_v4(), 0)
                    .as_str()
                    .into()
            }
            4 => entry
                .schema
                .insert("other".into(), ColumnType::Utf8)
                .map(|_| ())
                .unwrap_or(()),
            _ => {
                entry.data = TableData::Legacy {
                    snapshot: ObjectKey::snapshot(entry.id, candidate.commit_id)
                        .as_str()
                        .into(),
                }
            }
        }
        let before = c.storage().backend().state.lock().unwrap().puts;
        let result = if mode == 5 {
            session.publish(candidate, Some(vec![1])).await
        } else {
            session.publish_partitions(candidate, vec![]).await
        };
        assert_error(
            result.unwrap_err(),
            ErrorCode::ValidationError,
            Outcome::NotApplied,
        );
        assert_eq!(c.storage().backend().state.lock().unwrap().puts, before);
    }
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    let entry = candidate.tables.get_mut("users").unwrap();
    entry.row_count = 0;
    entry.data = TableData::Partitioned { files: vec![] };
    session.publish_partitions(candidate, vec![]).await.unwrap();
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    candidate.tables.clear();
    session.publish_partitions(candidate, vec![]).await.unwrap();
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    create(&mut candidate);
    candidate.tables.get_mut("users").unwrap().data = TableData::Partitioned { files: vec![] };
    session.publish_partitions(candidate, vec![]).await.unwrap();
}
#[tokio::test]
async fn partition_catalog_conflict_and_ambiguity_keep_existing_outcomes() {
    for (error, apply) in [
        (StorageError::Conflict, false),
        (StorageError::Ambiguous, true),
        (StorageError::Ambiguous, false),
    ] {
        let c = coordinator().await;
        let session = c.begin().await.unwrap();
        let mut candidate = session.candidate();
        let uploads = partitions(&mut candidate);
        if error == StorageError::Conflict {
            let loaded = c.storage().load_catalog().await.unwrap();
            c.storage()
                .put(
                    &ObjectKey::catalog(),
                    loaded.catalog.encode().unwrap(),
                    WriteCondition::Match(&loaded.etag),
                )
                .await
                .unwrap();
        } else {
            fault(&c, "catalog.json", error, apply);
        }
        let result = session.publish_partitions(candidate.clone(), uploads).await;
        if apply {
            assert_eq!(result.unwrap().catalog, candidate);
        } else {
            assert_error(
                result.unwrap_err(),
                if error == StorageError::Conflict {
                    ErrorCode::Conflict
                } else {
                    ErrorCode::OutcomeUnknown
                },
                if error == StorageError::Conflict {
                    Outcome::NotApplied
                } else {
                    Outcome::Unknown
                },
            );
        }
        let state = c.storage().backend().state.lock().unwrap();
        assert_eq!(
            state.puts,
            if error == StorageError::Conflict {
                5
            } else {
                4
            }
        );
        assert_eq!(state.gets, 3);
    }
}

#[tokio::test]
async fn bounded_catalog_and_immutable_partition_keys_precede_publication() {
    let c = coordinator().await;
    let loaded = c.storage().load_catalog().await.unwrap();
    let mut admitted = loaded.catalog;
    create(&mut admitted);
    let entry = admitted.tables.get_mut("users").unwrap();
    // Each table has the maximum 128-column schema with long valid identifiers.
    for i in 0..127 {
        entry.schema.insert(format!("c{i:063}"), ColumnType::Utf8);
    }
    let base = entry.clone();
    admitted.tables.clear();
    for i in 0..1000 {
        let mut entry = base.clone();
        entry.id = Uuid::from_u128(1000 + i);
        entry.data = TableData::Partitioned { files: vec![] };
        let name = format!("t{i:063}");
        admitted.tables.insert(name.clone(), entry);
        if admitted.encode().is_err() {
            admitted.tables.remove(&name);
            break;
        }
    }
    c.storage()
        .put(
            &ObjectKey::catalog(),
            admitted.encode().unwrap(),
            WriteCondition::Match(&loaded.etag),
        )
        .await
        .unwrap();
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    let uploads = partitions(&mut candidate);
    candidate.tables.get_mut("users").unwrap().schema = base.schema;
    assert_eq!(candidate.encode().unwrap_err(), StorageError::LimitExceeded);
    let before = c.storage().backend().state.lock().unwrap().puts;
    assert_error(
        session
            .publish_partitions(candidate, uploads)
            .await
            .unwrap_err(),
        ErrorCode::LimitExceeded,
        Outcome::NotApplied,
    );
    assert_eq!(c.storage().backend().state.lock().unwrap().puts, before);

    let c = coordinator().await;
    let session = c.begin().await.unwrap();
    let mut candidate = session.candidate();
    let uploads = partitions(&mut candidate);
    // A collision must fail Create, preserving both the old bytes and old catalog.
    let collision = format!("test/{}", uploads[1].key.as_str());
    c.storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert(collision.clone(), (vec![9], "existing".into()));
    assert_error(
        session
            .publish_partitions(candidate, uploads)
            .await
            .unwrap_err(),
        ErrorCode::Conflict,
        Outcome::NotApplied,
    );
    assert_eq!(
        c.storage().load_catalog().await.unwrap().catalog.revision,
        0
    );
    let state = c.storage().backend().state.lock().unwrap();
    assert_eq!(state.objects[&collision].0, vec![9]);
    assert_eq!(state.puts, 3);
}
