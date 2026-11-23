#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::storage::*;
#[path = "support/storage.rs"]
mod fake;
use fake::Fake;

fn storage() -> Storage<Fake> {
    Storage::new(Fake::default(), Namespace::new("sandbox///").unwrap())
}
fn populated() -> Catalog {
    let mut catalog: Catalog = serde_json::from_value(serde_json::json!({
        "format_version":1,"revision":1,"commit_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "tables":{"users":{"id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "schema":{"id":"utf8"},"key":"id","format":"parquet",
        "snapshot":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.parquet","row_count":1}}
    })).unwrap();
    catalog.format_version = 2;
    catalog
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
        (
            "data",
            serde_json::json!({"layout":"legacy","snapshot":"../outside.parquet"}),
        ),
    ] {
        let mut value_doc = valid.clone();
        value_doc["tables"]["users"][field] = value;
        corrupt.push(serde_json::to_vec(&value_doc).unwrap());
    }
    for bytes in [b"bad json".to_vec(), b"{\"format_version\":3,\"revision\":0,\"commit_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\",\"tables\":{}}".to_vec(), vec![b' '; 1024*1024+1]].into_iter().chain(corrupt) {
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
        catalog.tables.get_mut("users").unwrap().data =
            nabedra_database_server::storage::TableData::Legacy {
                snapshot: path.into(),
            };
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
    let table = populated().tables["users"].id;
    let partition = ObjectKey::partition(table, uuid::Uuid::nil(), u32::MAX);
    assert_eq!(
        ObjectKey::parse_relative(partition.as_str()).unwrap(),
        partition
    );
    for path in [
        partition.as_str().replace("4294967295", "04294967295"),
        partition.as_str().replace("4294967295", "4294967296"),
        partition.as_str().replace("bbbb", "BBBB"),
        partition.as_str().replace("4294967295", "../0"),
        partition.as_str().replace("4294967295", &"9".repeat(1024)),
    ] {
        assert!(ObjectKey::parse_relative(&path).is_err());
    }
    store
        .put(&partition, vec![1], WriteCondition::Create)
        .await
        .unwrap();
    assert_eq!(store.read(&partition, 1).await.unwrap().bytes, vec![1]);
    assert_eq!(
        store
            .put(&partition, vec![2], WriteCondition::Match("token"))
            .await
            .unwrap_err(),
        StorageError::InvalidPath
    );
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
    future_version.format_version = 3;
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
        entry.data = nabedra_database_server::storage::TableData::Legacy {
            snapshot: ObjectKey::snapshot(entry.id, large.commit_id)
                .as_str()
                .into(),
        };
        large.tables.insert(format!("table_{n}"), entry);
    }
    assert_eq!(large.encode().unwrap_err(), StorageError::LimitExceeded);
}

const FROZEN_V1: &str = r#"{"format_version":1,"revision":1,"commit_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","tables":{"users":{"id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","schema":{"id":"utf8"},"key":"id","format":"parquet","snapshot":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.parquet","row_count":1}}}"#;

#[tokio::test]
async fn frozen_v1_reads_without_migration_and_new_writes_are_v2() {
    let old = Catalog::decode(FROZEN_V1.as_bytes()).unwrap();
    assert_eq!(old.format_version, 1);
    let store = storage();
    store.backend().state.lock().unwrap().objects.insert(
        "sandbox/catalog.json".into(),
        (FROZEN_V1.as_bytes().to_vec(), "frozen".into()),
    );
    assert_eq!(store.initialize_catalog().await.unwrap().catalog, old);
    let state = store.backend().state.lock().unwrap();
    assert_eq!(state.puts, 0);
    assert_eq!(state.objects.len(), 1);
    assert_eq!(
        state.objects["sandbox/catalog.json"].0,
        FROZEN_V1.as_bytes()
    );
    drop(state);
    for text in [
        FROZEN_V1.replace("\"revision\":1", "\"revision\":1,\"revision\":2"),
        FROZEN_V1.replace("\"id\":\"utf8\"", "\"id\":\"utf8\",\"id\":\"int64\""),
        FROZEN_V1.replace("\"row_count\":1", "\"row_count\":1,\"unexpected\":0"),
        FROZEN_V1.replace("\"row_count\":1", "\"row_count\":1,\"row_count\":2"),
    ] {
        assert!(Catalog::decode(text.as_bytes()).is_err());
    }
    assert!(old.tables["users"].snapshot_key().is_ok());
    let written: serde_json::Value = serde_json::from_slice(&old.encode().unwrap()).unwrap();
    assert_eq!(written["format_version"], 2);
    assert_eq!(written["tables"]["users"]["data"]["layout"], "legacy");
    assert_eq!(Catalog::empty().format_version, 2);
}

fn partitioned_document() -> serde_json::Value {
    let mut doc: serde_json::Value = serde_json::from_str(FROZEN_V1).unwrap();
    doc["format_version"] = 2.into();
    let entry = doc["tables"]["users"].as_object_mut().unwrap();
    entry.remove("snapshot");
    entry.insert("schema".into(), serde_json::json!({"id":"int64"}));
    entry.insert("row_count".into(), 3.into());
    entry.insert("data".into(), serde_json::json!({"layout":"partitioned","files":[
        {"object":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/0.parquet","range":{"lower":null,"upper":-10},"row_count":1,"byte_size":100,"payload_bytes":8},
        {"object":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/1.parquet","range":{"lower":-10,"upper":2},"row_count":1,"byte_size":100,"payload_bytes":8},
        {"object":"tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/2.parquet","range":{"lower":2,"upper":null},"row_count":1,"byte_size":100,"payload_bytes":8}
    ]}));
    doc
}

#[test]
fn mixed_v2_roundtrip_and_typed_routing() {
    use nabedra_database_server::protocol::Key;
    let mut doc = partitioned_document();
    let legacy = Catalog::decode(FROZEN_V1.as_bytes()).unwrap();
    let mut catalog = Catalog::decode(&serde_json::to_vec(&doc).unwrap()).unwrap();
    let mut old = legacy.tables["users"].clone();
    old.id = uuid::Uuid::nil();
    old.data = TableData::Legacy {
        snapshot: ObjectKey::snapshot(old.id, catalog.commit_id)
            .as_str()
            .into(),
    };
    catalog.tables.insert("old".into(), old);
    assert_eq!(
        Catalog::decode(&catalog.encode().unwrap()).unwrap(),
        catalog
    );
    let text = String::from_utf8(catalog.encode().unwrap()).unwrap();
    for invalid in [
        text.replace(
            "\"layout\":\"partitioned\"",
            "\"layout\":\"partitioned\",\"layout\":\"partitioned\"",
        ),
        text.replace("\"byte_size\":100", "\"byte_size\":100,\"byte_size\":100"),
        text.replace("\"lower\":-10", "\"lower\":-10,\"lower\":-10"),
        text.replace(
            "\"payload_bytes\":8",
            "\"payload_bytes\":8,\"unexpected\":0",
        ),
    ] {
        assert!(Catalog::decode(invalid.as_bytes()).is_err());
    }
    let entry = &catalog.tables["users"];
    assert!(entry.snapshot_key().is_err());
    for (key, ordinal) in [(-11, 0), (-10, 1), (-1, 1), (2, 2), (10, 2)] {
        assert!(
            entry
                .route(&Key::Int64(key))
                .unwrap()
                .unwrap()
                .object
                .ends_with(&format!("/{ordinal}.parquet"))
        );
    }
    assert!(entry.route(&Key::Utf8("2".into())).is_err());
    doc["tables"]["users"]["schema"]["id"] = "utf8".into();
    let files = doc["tables"]["users"]["data"]["files"]
        .as_array_mut()
        .unwrap();
    files[0]["range"]["upper"] = "é".into();
    files[1]["range"]["lower"] = "é".into();
    files[1]["range"]["upper"] = "界".into();
    files[2]["range"]["lower"] = "界".into();
    let catalog = Catalog::decode(&serde_json::to_vec(&doc).unwrap()).unwrap();
    for (key, ordinal) in [("z", 0), ("é", 1), ("ê", 1), ("界", 2), ("😀", 2)] {
        assert!(
            catalog.tables["users"]
                .route(&Key::Utf8(key.into()))
                .unwrap()
                .unwrap()
                .object
                .ends_with(&format!("/{ordinal}.parquet"))
        );
    }
}

#[test]
fn partition_metadata_rejects_invalid_manifests() {
    let valid = partitioned_document();
    for (pointer, value) in [
        (
            "/tables/users/data/files/0",
            serde_json::json!([
                valid["tables"]["users"]["data"]["files"][0]["object"],
                {"lower": null, "upper": -10},
                1,
                100,
                8
            ]),
        ),
        (
            "/tables/users/data/files/0/range",
            serde_json::json!([null, -10]),
        ),
        (
            "/tables/users/data/files/0/range/lower",
            serde_json::json!(-20),
        ),
        (
            "/tables/users/data/files/2/range/upper",
            serde_json::json!(20),
        ),
        (
            "/tables/users/data/files/1/range/lower",
            serde_json::json!(-9),
        ),
        (
            "/tables/users/data/files/1/range/lower",
            serde_json::json!(-11),
        ),
        (
            "/tables/users/data/files/1/range/upper",
            serde_json::json!(-10),
        ),
        (
            "/tables/users/data/files/1/range/upper",
            serde_json::json!("2"),
        ),
        (
            "/tables/users/data/files/1/object",
            valid["tables"]["users"]["data"]["files"][0]["object"].clone(),
        ),
        (
            "/tables/users/data/files/1/object",
            serde_json::json!(
                "tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/01.parquet"
            ),
        ),
        (
            "/tables/users/data/files/1/object",
            serde_json::json!(
                "tables/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/4294967296.parquet"
            ),
        ),
        ("/tables/users/row_count", serde_json::json!(4)),
        ("/tables/users/data/files/0/row_count", serde_json::json!(0)),
        (
            "/tables/users/data/files/0/row_count",
            serde_json::json!(u64::MAX),
        ),
        (
            "/tables/users/data/files/0/payload_bytes",
            serde_json::json!(64 * 1024 * 1024),
        ),
        (
            "/tables/users/data/files/0/payload_bytes",
            serde_json::json!(u64::MAX),
        ),
        (
            "/tables/users/data/files/0/byte_size",
            serde_json::json!(
                (nabedra_database_server::partition_size::MAX_PARTITION_BYTES + 1) as u64
            ),
        ),
        ("/tables/users/data/files/0/byte_size", serde_json::json!(0)),
    ] {
        let mut doc = valid.clone();
        *doc.pointer_mut(pointer).unwrap() = value;
        assert!(
            Catalog::decode(&serde_json::to_vec(&doc).unwrap()).is_err(),
            "{pointer}"
        );
    }
    let mut empty = valid.clone();
    empty["tables"]["users"]["data"]["files"] = serde_json::json!([]);
    assert!(Catalog::decode(&serde_json::to_vec(&empty).unwrap()).is_err());
    empty["tables"]["users"]["row_count"] = 0.into();
    assert!(Catalog::decode(&serde_json::to_vec(&empty).unwrap()).is_ok());
    let mut ambiguous = valid;
    ambiguous["tables"]["users"]["snapshot"] = "extra".into();
    assert!(Catalog::decode(&serde_json::to_vec(&ambiguous).unwrap()).is_err());
}

// A stream may fail after yielding data or never end. Neither may be collected
// without enforcing the bound while reading.
#[tokio::test]
async fn neutral_reader_bounds_and_partial_failure() {
    use std::{
        io,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, ReadBuf};
    struct Reader {
        consumed: Arc<AtomicUsize>,
        fail: bool,
    }
    impl AsyncRead for Reader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.fail && self.consumed.load(Ordering::SeqCst) > 0 {
                return Poll::Ready(Err(io::Error::other("private backend diagnostic")));
            }
            let n = buf.remaining().min(8);
            buf.put_slice(&[7; 8][..n]);
            self.consumed.fetch_add(n, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }
    struct Backend {
        consumed: Arc<AtomicUsize>,
        fail: bool,
    }
    impl ObjectStore for Backend {
        async fn get(&self, _: &str) -> Result<Download, StorageError> {
            Ok(Download::new(
                Reader {
                    consumed: self.consumed.clone(),
                    fail: self.fail,
                },
                "version".into(),
            ))
        }
        async fn put(
            &self,
            _: &str,
            _: Vec<u8>,
            _: WriteCondition<'_>,
        ) -> Result<String, StorageError> {
            unreachable!()
        }
    }
    for (fail, limit, expected, consumed) in [
        (false, 17, StorageError::LimitExceeded, 18),
        (false, 0, StorageError::LimitExceeded, 1),
        (true, 17, StorageError::Unavailable, 8),
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let store = Storage::new(
            Backend {
                consumed: count.clone(),
                fail,
            },
            Namespace::new("sandbox").unwrap(),
        );
        assert_eq!(
            store.read(&ObjectKey::catalog(), limit).await.unwrap_err(),
            expected
        );
        assert_eq!(count.load(Ordering::SeqCst), consumed);
    }
    let mut download = Download::from_bytes(vec![1, 2], "version".into());
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut download.body, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, vec![1, 2]);
}
