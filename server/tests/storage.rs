use meshdb_server::storage::*;
#[path = "support/storage.rs"]
mod fake;
use fake::Fake;

fn storage() -> Storage<Fake> {
    Storage::new(Fake::default(), Namespace::new("sandbox///").unwrap())
}
fn populated() -> Catalog {
    serde_json::from_value(serde_json::json!({
        "format_version":1,"revision":1,"commit_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "tables":{"users":{"id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "schema":{"id":"utf8"},"key":"id","format":"parquet",
        "snapshot":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.parquet","row_count":1}}
    })).unwrap()
}
#[tokio::test]
async fn load_initialize_and_conditional_race() {
    let store = storage();
    assert_eq!(
        store.load_catalog().await.unwrap_err(),
        StorageError::NotFound
    );
    let initial = store.initialize_catalog().await.unwrap();
    assert_eq!(initial.catalog.revision, 0);
    assert_eq!(store.initialize_catalog().await.unwrap(), initial);
    assert_eq!(store.backend().state.lock().unwrap().puts, 1);
    let next = populated();
    let etag = store
        .put(
            &ObjectKey::catalog(),
            next.encode().unwrap(),
            WriteCondition::Match(&initial.etag),
        )
        .await
        .unwrap();
    assert_eq!(store.load_catalog().await.unwrap().etag, etag);
    assert_eq!(
        store
            .put(
                &ObjectKey::catalog(),
                vec![],
                WriteCondition::Match(&initial.etag)
            )
            .await
            .unwrap_err(),
        StorageError::Conflict
    );
    let race = storage();
    race.backend().state.lock().unwrap().race = Some(next.encode().unwrap());
    assert_eq!(race.initialize_catalog().await.unwrap().catalog, next);
    let lost = storage();
    lost.backend().state.lock().unwrap().lose_put_response = true;
    assert_eq!(lost.initialize_catalog().await.unwrap().catalog.revision, 0);
    assert_eq!(lost.backend().state.lock().unwrap().puts, 1);
}
#[tokio::test]
async fn unreadable_catalog_never_resets() {
    let store = storage();
    store.backend().state.lock().unwrap().denied = true;
    assert_eq!(
        store.initialize_catalog().await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(store.backend().state.lock().unwrap().puts, 0);
    store.backend().state.lock().unwrap().denied = false;
    let valid = serde_json::to_value(populated()).unwrap();
    let mut corrupt = Vec::new();
    for (field, value) in [
        ("id", serde_json::json!("not-a-uuid")),
        ("schema", serde_json::json!({"id":"float64"})),
        ("key", serde_json::json!("absent")),
        ("format", serde_json::json!("csv")),
        ("row_count", serde_json::json!(100_001)),
        ("snapshot", serde_json::json!("../outside.parquet")),
    ] {
        let mut value_doc = valid.clone();
        value_doc["tables"]["users"][field] = value;
        corrupt.push(serde_json::to_vec(&value_doc).unwrap());
    }
    for bytes in [b"bad json".to_vec(), b"{\"format_version\":2,\"revision\":0,\"commit_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\",\"tables\":{}}".to_vec(), vec![b' '; 1024*1024+1]].into_iter().chain(corrupt) {
        store.backend().state.lock().unwrap().objects.insert("sandbox/catalog.json".into(), (bytes.clone(), "token".into()));
        assert!(store.initialize_catalog().await.is_err());
        let state = store.backend().state.lock().unwrap();
        assert_eq!(state.puts, 0);
        assert_eq!(state.objects["sandbox/catalog.json"].0, bytes);
    }
}
#[tokio::test]
async fn namespace_snapshot_validation_and_read_bounds() {
    for prefix in [
        "",
        "/root",
        "s3://bucket/key",
        "a/../b",
        "a//b",
        "a/./b",
        "a\\b",
    ] {
        assert!(Namespace::new(prefix).is_err(), "{prefix}");
    }
    let valid = populated();
    assert_eq!(Catalog::decode(&valid.encode().unwrap()).unwrap(), valid);
    let mut catalog = valid;
    for path in [
        "/tables/x",
        "https://evil/x",
        "tables/../x",
        "tables/cccccccc-cccc-4ccc-8ccc-cccccccccccc/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.parquet",
    ] {
        catalog.tables.get_mut("users").unwrap().snapshot = path.into();
        assert!(catalog.encode().is_err());
    }
    let mut catalog = populated();
    catalog
        .tables
        .insert("other".into(), catalog.tables["users"].clone());
    assert!(catalog.encode().is_err());
    let mut catalog = populated();
    catalog.tables.get_mut("users").unwrap().row_count = 100_001;
    assert!(catalog.encode().is_err());
    let store = storage();
    let snapshot = populated().tables["users"].snapshot_key().unwrap();
    let etag = store
        .put(&snapshot, vec![7], WriteCondition::Create)
        .await
        .unwrap();
    assert_eq!(
        store
            .put(&snapshot, vec![8], WriteCondition::Create)
            .await
            .unwrap_err(),
        StorageError::Conflict
    );
    assert_eq!(
        store
            .put(&snapshot, vec![8], WriteCondition::Match(&etag))
            .await
            .unwrap_err(),
        StorageError::InvalidPath
    );
    assert_eq!(store.read(&snapshot, 1).await.unwrap().bytes, vec![7]);
    store
        .put(&ObjectKey::catalog(), vec![1; 17], WriteCondition::Create)
        .await
        .unwrap();
    assert_eq!(
        store.read(&ObjectKey::catalog(), 16).await.unwrap_err(),
        StorageError::LimitExceeded
    );
    assert_eq!(
        store
            .read(&ObjectKey::catalog(), 17)
            .await
            .unwrap()
            .bytes
            .len(),
        17
    );
}

#[test]
fn catalog_codec_rejects_duplicate_names_and_bounds_encoding() {
    let mut future_version = populated();
    future_version.format_version = 2;
    let unsupported = serde_json::to_vec(&future_version).unwrap();
    assert_eq!(
        Catalog::decode(&unsupported).unwrap_err(),
        StorageError::UnsupportedCatalog
    );
    let valid = populated().encode().unwrap();
    let text = String::from_utf8(valid).unwrap();
    assert_eq!(
        Catalog::decode(
            text.replace("\"id\":\"utf8\"", "\"id\":\"utf8\",\"id\":\"int64\"")
                .as_bytes()
        )
        .unwrap_err(),
        StorageError::InvalidCatalog
    );
    let mut large = Catalog::empty();
    let entry = populated().tables.remove("users").unwrap();
    for n in 1..=5000_u128 {
        let mut entry = entry.clone();
        entry.id = uuid::Uuid::from_u128(n);
        entry.snapshot = ObjectKey::snapshot(entry.id, large.commit_id)
            .as_str()
            .into();
        large.tables.insert(format!("table_{n}"), entry);
    }
    assert_eq!(large.encode().unwrap_err(), StorageError::LimitExceeded);
}
