use meshdb_server::{engine::Engine, protocol, snapshot, storage};
#[path = "support/storage.rs"]
mod support;

use protocol::{ColumnType, CreatePayload, ErrorCode, Outcome, TableFormat};
use storage::{Namespace, ObjectKey, Storage};
use support::Fake;

fn payload(name: &str) -> CreatePayload {
    CreatePayload {
        name: name.into(),
        schema: [
            ("id".into(), ColumnType::Int64),
            ("text".into(), ColumnType::Utf8),
            ("number".into(), ColumnType::Float64),
            ("flag".into(), ColumnType::Bool),
        ]
        .into(),
        key: "id".into(),
        format: TableFormat::Parquet,
    }
}

async fn engine() -> Engine<Fake> {
    let storage = Storage::new(Fake::default(), Namespace::new("test/").unwrap());
    storage.initialize_catalog().await.unwrap();
    Engine::new(storage)
}

#[tokio::test]
async fn creation_publishes_closed_empty_parquet_and_canonical_reference() {
    let engine = engine().await;
    let clone = engine.clone(); // Fake deliberately does not implement Clone.
    assert!(std::sync::Arc::ptr_eq(
        engine.coordinator(),
        clone.coordinator()
    ));
    let table = clone.create(payload("mixed")).await.unwrap();
    let loaded = engine.storage().load_catalog().await.unwrap();
    let entry = &loaded.catalog.tables["mixed"];
    assert_eq!(entry.descriptor("mixed"), table);
    assert_eq!(entry.row_count, 0);
    assert_ne!(table.id, loaded.catalog.commit_id);
    assert_eq!(
        entry.snapshot,
        ObjectKey::snapshot(table.id, loaded.catalog.commit_id).as_str()
    );
    let object = engine
        .storage()
        .read(
            &entry.snapshot_key().unwrap(),
            snapshot::MAX_ENCODED_SNAPSHOT_BYTES,
        )
        .await
        .unwrap();
    assert!(object.bytes.starts_with(b"PAR1"));
    assert!(object.bytes.ends_with(b"PAR1"));
    assert!(
        snapshot::decode(&table, object.bytes.into(), Some(0))
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn fresh_sorted_reads_and_rejected_creates_preserve_catalog() {
    let engine = engine().await;
    let z = engine.create(payload("zebra")).await.unwrap();
    let a = engine.clone().create(payload("alpha")).await.unwrap();
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    assert_eq!(engine.list().await.unwrap(), vec![a.clone(), z.clone()]);
    assert_eq!(engine.describe("alpha".into()).await.unwrap(), a);
    assert_eq!(
        engine.storage().backend().state.lock().unwrap().gets,
        gets + 2
    );
    let before = engine.storage().load_catalog().await.unwrap();
    let puts = engine.storage().backend().state.lock().unwrap().puts;
    let duplicate = engine.create(payload("zebra")).await.unwrap_err();
    assert_eq!(
        (duplicate.code, duplicate.outcome),
        (ErrorCode::Conflict, Outcome::NotApplied)
    );
    for invalid in [
        payload("bad-name"),
        CreatePayload {
            key: "flag".into(),
            ..payload("bad_key")
        },
        CreatePayload {
            schema: Default::default(),
            ..payload("no_columns")
        },
    ] {
        assert_eq!(
            engine.create(invalid).await.unwrap_err().code,
            ErrorCode::ValidationError
        );
    }
    assert_eq!(
        engine.describe("bad-name".into()).await.unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(
        engine.describe("absent".into()).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(engine.storage().load_catalog().await.unwrap(), before);
    assert_eq!(engine.storage().backend().state.lock().unwrap().puts, puts);
}

#[tokio::test]
async fn drop_retains_snapshot_and_stale_identity_while_read_failures_never_reset() {
    let engine = engine().await;
    let old = engine.create(payload("same")).await.unwrap();
    let before = engine.storage().load_catalog().await.unwrap();
    let key = before.catalog.tables["same"].snapshot_key().unwrap();
    let put_count = engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .put_keys
        .len();
    engine.drop(old.id).await.unwrap();
    {
        let state = engine.storage().backend().state.lock().unwrap();
        assert_eq!(&state.put_keys[put_count..], &["test/catalog.json"]);
    }
    assert!(engine.list().await.unwrap().is_empty());
    let new = engine.create(payload("same")).await.unwrap();
    assert_ne!(old.id, new.id);
    assert_eq!(
        engine.drop(old.id).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(engine.describe("same".into()).await.unwrap(), new);
    let retained = engine
        .storage()
        .read(&key, snapshot::MAX_ENCODED_SNAPSHOT_BYTES)
        .await
        .unwrap();
    assert!(
        snapshot::decode(&old, retained.bytes.into(), Some(0))
            .unwrap()
            .is_empty()
    );
    let puts = engine.storage().backend().state.lock().unwrap().puts;
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .read_failure = true;
    assert_eq!(
        engine.list().await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        engine.describe("same".into()).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        engine.create(payload("failed")).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        engine.drop(new.id).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    {
        let mut state = engine.storage().backend().state.lock().unwrap();
        state.read_failure = false;
        state.objects.remove("test/catalog.json");
    }
    assert_eq!(
        engine.list().await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert(
            "test/catalog.json".into(),
            (b"invalid".to_vec(), "etag".into()),
        );
    assert_eq!(
        engine.list().await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(engine.storage().backend().state.lock().unwrap().puts, puts);
}
