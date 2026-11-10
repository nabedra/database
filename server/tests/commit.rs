use meshdb_server::{
    commit::Coordinator,
    protocol::{ColumnType, ErrorCode, Outcome, TableFormat},
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
            snapshot: ObjectKey::snapshot(id, candidate.commit_id).as_str().into(),
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
    let snapshot = candidate.tables["users"].snapshot.clone();
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
    entry.snapshot = ObjectKey::snapshot(entry.id, updated.commit_id)
        .as_str()
        .into();
    let updated_snapshot = entry.snapshot.clone();
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
            &candidate.tables["users"].snapshot,
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
        let snapshot = candidate.tables["users"].snapshot.clone();
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
                candidate.tables.get_mut("users").unwrap().snapshot =
                    ObjectKey::snapshot(Uuid::from_u128(42), Uuid::nil())
                        .as_str()
                        .into()
            }
            2 => {
                let mut second = candidate.tables["users"].clone();
                second.id = Uuid::from_u128(43);
                second.snapshot = ObjectKey::snapshot(second.id, candidate.commit_id)
                    .as_str()
                    .into();
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
