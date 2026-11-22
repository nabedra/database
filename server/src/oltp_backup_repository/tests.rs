//! Archive transport assertions. The opaque history fixtures do not establish
//! database backup consistency. Only the opt-in live probe contacts AWS.
use super::*;
use crate::{config::WorkerResources, oltp::pool::SharedResourceConfig};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-repository-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
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

fn private_key(directory: &Path, name: &str, bytes: &[u8; 32]) -> PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, bytes).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
    path
}

fn key_config(entries: &[(&str, &Path)], write_key: Option<&str>) -> ArchiveEncryptionConfig {
    let keyring = entries
        .iter()
        .map(|(id, file)| serde_json::json!({"id": id, "file": file}))
        .collect::<Vec<_>>();
    serde_json::from_value(serde_json::json!({"write_key": write_key, "keyring": keyring})).unwrap()
}

#[cfg(not(feature = "oltp-backup-s3"))]
#[tokio::test]
async fn o19_s3_archive_configuration_requires_the_backup_feature() {
    let result = S3ArchiveConfig {
        bucket: "nabedra-archive-example".into(),
        prefix: "test/archive".into(),
        region: "us-east-1".into(),
    }
    .connect(shared())
    .await;
    let error = result.err().expect("disabled feature accepted S3 archives");
    assert_eq!(error.code, ErrorCode::UnsupportedOperation);
    assert_eq!(error.outcome, Outcome::NotApplied);
}

#[cfg(feature = "oltp-backup-s3")]
#[tokio::test]
async fn o19_s3_conditional_put_reconciles_only_verified_exact_bytes() {
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::State,
        http::{Method, Request, StatusCode},
        response::Response,
    };
    use std::{collections::BTreeMap, sync::Mutex};

    type Objects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;
    async fn exact(State(objects): State<Objects>, request: Request<Body>) -> Response {
        let key = request.uri().path().to_owned();
        match *request.method() {
            Method::PUT => {
                assert_eq!(request.headers()["if-none-match"], "*");
                let body = to_bytes(request.into_body(), stored_limit(&chunk_name(0)))
                    .await
                    .unwrap();
                if key.ends_with(&chunk_name(2)) {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap();
                }
                let mut objects = objects.lock().unwrap();
                let status = match objects.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(body.to_vec());
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {
                        StatusCode::PRECONDITION_FAILED
                    }
                };
                Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap()
            }
            Method::GET => {
                let body = objects.lock().unwrap().get(&key).cloned();
                match body {
                    Some(body) => Response::builder()
                        .header("content-length", body.len())
                        .header("x-amz-meta-nabedra-sha256", hash(&body).to_string())
                        .body(Body::from(body))
                        .unwrap(),
                    None => Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .unwrap(),
                }
            }
            _ => panic!("unexpected S3 operation"),
        }
    }

    let objects = Objects::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = nabedra_rt::task::spawn(async move {
        axum::serve(listener, Router::new().fallback(exact).with_state(objects))
            .await
            .unwrap();
    });
    let shared = shared();
    let repository = super::s3::S3Repository::connect(
        super::s3::S3RepositoryConfig {
            bucket: "nabedra-archive-example".into(),
            prefix: "private/archive".into(),
            region: "us-east-1".into(),
            credentials: Some(aws_sdk_s3::config::Credentials::new(
                "test",
                "test",
                None,
                None,
                "local archive fixture",
            )),
            fixture_endpoint: Some(endpoint),
        },
        shared.clone(),
    )
    .await
    .unwrap();
    let key = ArchiveKey {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        archive_id: nabedra_rt::ids::uuid_v4(),
    };
    for _ in 0..2 {
        repository
            .put(
                key,
                "root.json".into(),
                shared.copy_bytes(b"original immutable root").unwrap(),
            )
            .await
            .unwrap();
    }
    let mismatch = repository
        .put(
            key,
            "root.json".into(),
            shared.copy_bytes(b"cancel").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatch.code, ErrorCode::Conflict);
    let missing = repository
        .put(
            key,
            chunk_name(2),
            shared.copy_bytes(b"not acknowledged").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code, ErrorCode::StorageUnavailable);
    assert_eq!(missing.outcome, Outcome::Unknown);
    assert_eq!(
        repository
            .get(key, "root.json".into(), MAX_ROOT_BYTES)
            .await
            .unwrap()
            .as_slice(),
        b"original immutable root"
    );
    let key_file = Directory::new();
    let file = private_key(&key_file.0, "key", &[42; 32]);
    let key_config = key_config(&[("online", &file)], Some("online"));
    let protected = ArchiveRepository::new(
        repository.clone(),
        ArchiveKeys::load(Some(&key_config)).unwrap(),
    );
    let scope = ArchiveKey {
        archive_id: nabedra_rt::ids::uuid_v4(),
        ..key
    };
    let name = chunk_name(0);
    let data = shared.copy_bytes(b"protected loopback bytes").unwrap();
    let first = protected
        .put(scope, name.clone(), data.clone())
        .await
        .unwrap();
    let repeated = protected
        .put(scope, name.clone(), data.clone())
        .await
        .unwrap();
    assert_eq!(repeated, first);
    let stored = repository
        .get(scope, name, stored_limit(&chunk_name(0)))
        .await
        .unwrap();
    assert_eq!(hash(stored.as_ref()), first);
    assert!(stored.as_ref().starts_with(b"NABEDRA-ARCHIVE\0\x01"));
    assert_eq!(
        protected.put(scope, chunk_name(2), data).await.unwrap_err(),
        unavailable()
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn o19_filesystem_archive_retries_are_immutable_bounded_and_root_publishes_last() {
    let directory = Directory::new();
    let shared = shared();
    let repository = ArchiveRepository::new(
        FilesystemRepository::open(&directory.0, shared.clone()).unwrap(),
        ArchiveKeys::load(None).unwrap(),
    );
    let key = ArchiveKey {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        archive_id: nabedra_rt::ids::uuid_v4(),
    };
    let mut upload = UploadSession::new(repository.clone(), key, 4).unwrap();
    let first = upload
        .append(shared.copy_bytes(b"abcd").unwrap())
        .await
        .unwrap();
    assert!(read_root(&repository, key).await.is_err());
    upload
        .append(shared.copy_bytes(b"efgh").unwrap())
        .await
        .unwrap();
    upload
        .append(shared.copy_bytes(b"ij").unwrap())
        .await
        .unwrap();
    assert!(
        upload
            .append(shared.copy_bytes(b"k").unwrap())
            .await
            .is_err()
    );
    let published = upload
        .publish(
            shared
                .copy_bytes(br#"{"format_version":1,"history_root":"fixture-only"}"#)
                .unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(published.key(), key);
    assert_eq!(published.header().part_count, 3);
    assert_eq!(published.header().total_bytes, 10);
    assert_eq!(published.header().archive_sha256, hash(b"abcdefghij"));
    let mut reader = ArchiveReader::open(repository.clone(), key).await.unwrap();
    assert!(reader.verify_complete().is_err());
    let mut actual = Vec::new();
    while let Some(bytes) = reader.next().await.unwrap() {
        actual.extend_from_slice(bytes.as_ref());
    }
    assert_eq!(actual, b"abcdefghij");
    reader.verify_complete().unwrap();
    assert_eq!(reader.root().root_sha256(), published.root_sha256());
    let mut retry = UploadSession::new(repository.clone(), key, 4).unwrap();
    for chunk in [b"abcd".as_slice(), b"efgh".as_slice(), b"ij".as_slice()] {
        retry
            .append(shared.copy_bytes(chunk).unwrap())
            .await
            .unwrap();
    }
    let repeated = retry
        .publish(
            shared
                .copy_bytes(br#"{"format_version":1,"history_root":"fixture-only"}"#)
                .unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(repeated.root_sha256(), published.root_sha256());
    let mut conflicting = UploadSession::new(repository.clone(), key, 4).unwrap();
    assert_eq!(
        conflicting
            .append(shared.copy_bytes(b"xxxx").unwrap())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    assert_eq!(
        read_chunk(&repository, key, first).await.unwrap().as_ref(),
        b"abcd"
    );
    // A same-length externally corrupted immutable object is detected by its
    // expected per-part digest and again by complete archive/root verification.
    let part = directory
        .0
        .join(key.cluster_id.to_string())
        .join(key.archive_id.to_string())
        .join(chunk_name(0));
    std::fs::set_permissions(&part, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&part, b"xxxx").unwrap();
    assert!(read_chunk(&repository, key, first).await.is_err());
    let mut corrupted = ArchiveReader::open(repository.clone(), key).await.unwrap();
    assert!(loop {
        match corrupted.next().await {
            Ok(Some(_)) => {}
            Ok(None) => break false,
            Err(_) => break true,
        }
    });
    drop(corrupted);
    drop(conflicting);
    drop(repeated);
    drop(reader);
    drop(published);
    assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
}

#[cfg(feature = "oltp-backup-s3")]
#[tokio::test]
#[ignore = "requires explicitly authorized fresh live S3 fixture and external teardown"]
async fn o19_live_s3_archive_reopens_and_reconciles_exact_retries() {
    assert_eq!(
        std::env::var("NABEDRA_LIVE_ARCHIVE_PROBE").ok().as_deref(),
        Some("1")
    );
    let config = S3ArchiveConfig {
        bucket: std::env::var("NABEDRA_S3_BUCKET").unwrap(),
        prefix: std::env::var("NABEDRA_S3_PREFIX")
            .unwrap()
            .trim_end_matches('/')
            .to_owned(),
        region: std::env::var("AWS_REGION").unwrap(),
    };
    let shared = shared();
    let transport = config.clone().connect(shared.clone()).await.unwrap();
    let keys = Directory::new();
    let key_path = private_key(&keys.0, "fixture-key", &[42; 32]);
    let key_config = key_config(&[("fixture", &key_path)], Some("fixture"));
    let repository = ArchiveRepository::new(
        transport.clone(),
        ArchiveKeys::load(Some(&key_config)).unwrap(),
    );
    let key = ArchiveKey {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        archive_id: nabedra_rt::ids::uuid_v4(),
    };
    let chunks = [b"abcd".as_slice(), b"ef".as_slice()];
    let history = br#"{"format_version":1,"history_root":"live-transport-fixture"}"#;

    let mut first = UploadSession::new(repository.clone(), key, 4).unwrap();
    first
        .append(shared.copy_bytes(chunks[0]).unwrap())
        .await
        .unwrap();
    assert!(read_root(&repository, key).await.is_err());
    first
        .append(shared.copy_bytes(chunks[1]).unwrap())
        .await
        .unwrap();
    let published = first
        .publish(shared.copy_bytes(history).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(published.header().archive_sha256, hash(b"abcdef"));
    assert_eq!(published.header().part_count, 2);
    assert_eq!(published.header().total_bytes, 6);

    let mut repeat = UploadSession::new(repository.clone(), key, 4).unwrap();
    for chunk in chunks {
        repeat
            .append(shared.copy_bytes(chunk).unwrap())
            .await
            .unwrap();
    }
    let same = repeat
        .publish(shared.copy_bytes(history).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(same.root_sha256(), published.root_sha256());
    let mut changed = UploadSession::new(repository, key, 4).unwrap();
    assert_eq!(
        changed
            .append(shared.copy_bytes(b"WXYZ").unwrap())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    let stored = transport
        .get(key, chunk_name(0), stored_limit(&chunk_name(0)))
        .await
        .unwrap();
    assert!(stored.as_ref().starts_with(b"NABEDRA-ARCHIVE\0\x01"));

    let reopened = ArchiveRepository::new(
        config.connect(shared).await.unwrap(),
        ArchiveKeys::load(Some(&key_config)).unwrap(),
    );
    let mut reader = ArchiveReader::open(reopened, key).await.unwrap();
    for chunk in chunks {
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), chunk);
    }
    assert!(reader.next().await.unwrap().is_none());
    reader.verify_complete().unwrap();
    assert_eq!(reader.root().root_sha256(), published.root_sha256());
}

#[tokio::test]
async fn o19_encrypted_archive_authenticates_objects_and_reconciles_exact_retries() {
    let directory = Directory::new();
    let keys = Directory::new();
    let first_file = private_key(&keys.0, "first", &[17; 32]);
    let second_file = private_key(&keys.0, "second", &[23; 32]);
    let original = key_config(&[("first", &first_file)], Some("first"));
    let rotated = key_config(
        &[("first", &first_file), ("second", &second_file)],
        Some("second"),
    );
    let read_only = key_config(&[("first", &first_file), ("second", &second_file)], None);
    let unknown_id = key_config(&[("second", &second_file)], None);
    let wrong_key = key_config(&[("first", &second_file)], None);
    let shared = shared();
    let transport = FilesystemRepository::open(&directory.0, shared.clone()).unwrap();
    let repository =
        |config| ArchiveRepository::new(transport.clone(), ArchiveKeys::load(config).unwrap());
    let scope = ArchiveKey {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        archive_id: nabedra_rt::ids::uuid_v4(),
    };
    let other = ArchiveKey {
        archive_id: nabedra_rt::ids::uuid_v4(),
        ..scope
    };
    let bytes = b"abcd";
    let history = shared.copy_bytes(br#"{"history":"private"}"#).unwrap();

    let mut first = UploadSession::new(repository(Some(&original)), scope, 4).unwrap();
    let spec = first
        .append(shared.copy_bytes(bytes).unwrap())
        .await
        .unwrap();
    let part = transport
        .get(scope, chunk_name(0), stored_limit(&chunk_name(0)))
        .await
        .unwrap();
    assert!(part.as_ref().starts_with(b"NABEDRA-ARCHIVE\0\x01\x05first"));
    assert_eq!(
        read_root(&repository(Some(&original)), scope)
            .await
            .err()
            .unwrap(),
        unavailable()
    );
    let original_root = first.publish(history.clone(), None).await.unwrap();
    let stored = transport
        .get(scope, ROOT_OBJECT.into(), stored_limit(ROOT_OBJECT))
        .await
        .unwrap();
    assert_ne!(&part.as_ref()[23..35], &stored.as_ref()[23..35]);
    assert_eq!(hash(stored.as_ref()), original_root.root_sha256());
    assert!(
        !stored
            .as_ref()
            .windows(b"private".len())
            .any(|window| window == b"private")
    );

    let mut retry = UploadSession::new(repository(Some(&rotated)), scope, 4).unwrap();
    retry
        .append(shared.copy_bytes(bytes).unwrap())
        .await
        .unwrap();
    let retried = retry.publish(history.clone(), None).await.unwrap();
    assert_eq!(retried.root_sha256(), original_root.root_sha256());
    assert_eq!(
        transport
            .get(scope, ROOT_OBJECT.into(), stored_limit(ROOT_OBJECT))
            .await
            .unwrap()
            .as_ref(),
        stored.as_ref()
    );
    assert_eq!(
        repository(Some(&rotated))
            .put(
                scope,
                ROOT_OBJECT.into(),
                shared.copy_bytes(b"cancel").unwrap()
            )
            .await
            .unwrap_err(),
        conflict()
    );
    assert_eq!(
        repository(Some(&rotated))
            .put(scope, chunk_name(0), shared.copy_bytes(b"xxxx").unwrap())
            .await
            .unwrap_err(),
        conflict()
    );
    assert_eq!(
        repository(Some(&read_only))
            .put(other, ROOT_OBJECT.into(), stored.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedOperation
    );

    assert_eq!(
        read_chunk(&repository(Some(&read_only)), scope, spec)
            .await
            .unwrap()
            .as_ref(),
        bytes
    );
    for config in [None, Some(&unknown_id)] {
        let error = read_root(&repository(config), scope).await.err().unwrap();
        assert_eq!(error.code, ErrorCode::StorageUnavailable);
        assert_eq!(error.outcome, Outcome::NotApplied);
        assert_eq!(error.message, "archive encryption key is unavailable");
    }
    assert_eq!(
        read_root(&repository(Some(&wrong_key)), scope)
            .await
            .err()
            .unwrap(),
        corrupt()
    );

    // The tag authenticates the archive UUID and object name independently.
    transport
        .put(other, ROOT_OBJECT.into(), stored.clone())
        .await
        .unwrap();
    assert_eq!(
        read_root(&repository(Some(&read_only)), other)
            .await
            .err()
            .unwrap(),
        corrupt()
    );
    let wrong_name = chunk_name(12);
    transport
        .put(scope, wrong_name.clone(), part)
        .await
        .unwrap();
    assert_eq!(
        repository(Some(&read_only))
            .get_part(
                scope,
                wrong_name,
                4,
                encryption::ObjectProtection::Encrypted
            )
            .await
            .unwrap_err(),
        corrupt()
    );

    let root_path = directory
        .0
        .join(scope.cluster_id.to_string())
        .join(scope.archive_id.to_string());
    let part_path = root_path.join(chunk_name(0));
    std::fs::set_permissions(&part_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&part_path, bytes).unwrap();
    let mut reader = ArchiveReader::open(repository(Some(&read_only)), scope)
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err(), corrupt());
    drop(reader);

    let stored_root_path = root_path.join(ROOT_OBJECT);
    std::fs::set_permissions(&stored_root_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut tampered = stored.as_ref().to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    std::fs::write(&stored_root_path, tampered).unwrap();
    assert_eq!(
        read_root(&repository(Some(&read_only)), scope)
            .await
            .err()
            .unwrap(),
        corrupt()
    );

    let legacy = ArchiveKey {
        archive_id: nabedra_rt::ids::uuid_v4(),
        ..scope
    };
    let mut plaintext = UploadSession::new(repository(None), legacy, 32).unwrap();
    let prefix_collision = b"NABEDRA-ARCHIVE\0\x01inside a part";
    plaintext
        .append(shared.copy_bytes(prefix_collision).unwrap())
        .await
        .unwrap();
    plaintext.publish(history, None).await.unwrap();
    let mut mixed_reader = ArchiveReader::open(repository(Some(&read_only)), legacy)
        .await
        .unwrap();
    assert_eq!(
        mixed_reader.next().await.unwrap().unwrap().as_ref(),
        prefix_collision
    );
    assert!(mixed_reader.next().await.unwrap().is_none());

    let cancelled = ArchiveKey {
        archive_id: nabedra_rt::ids::uuid_v4(),
        ..scope
    };
    let rotated_repository = repository(Some(&rotated));
    let cancellation = shared.copy_bytes(b"cancel").unwrap();
    let digest = rotated_repository
        .put(cancelled, ROOT_OBJECT.into(), cancellation.clone())
        .await
        .unwrap();
    assert_eq!(
        rotated_repository
            .put(cancelled, ROOT_OBJECT.into(), cancellation)
            .await
            .unwrap(),
        digest
    );
    assert_eq!(
        rotated_repository
            .put(cancelled, ROOT_OBJECT.into(), stored.clone())
            .await
            .unwrap_err(),
        conflict()
    );

    let max_chunk = shared
        .allocate_bytes(ByteClass::Data, MAX_CHUNK_BYTES)
        .unwrap()
        .freeze();
    let name = chunk_name(0);
    rotated_repository
        .put(cancelled, name.clone(), max_chunk.clone())
        .await
        .unwrap();
    let opened = rotated_repository
        .get_part(
            cancelled,
            name,
            MAX_CHUNK_BYTES,
            encryption::ObjectProtection::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(opened.as_ref(), max_chunk.as_ref());
    drop(opened);
    drop(max_chunk);

    drop(mixed_reader);
    drop(retried);
    drop(original_root);
    drop(stored);
    assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
}

#[test]
fn o19_archive_keyring_rejects_exposed_malformed_and_unselected_key_files() {
    let directory = Directory::new();
    let file = private_key(&directory.0, "protected", &[7; 32]);
    let bad_mode = directory.0.join("public");
    std::fs::write(&bad_mode, [7; 32]).unwrap();
    std::fs::set_permissions(&bad_mode, std::fs::Permissions::from_mode(0o644)).unwrap();
    let symlink = directory.0.join("symlink");
    std::os::unix::fs::symlink(&file, &symlink).unwrap();
    let wrong_size = directory.0.join("wrong-size");
    std::fs::write(&wrong_size, [7; 31]).unwrap();
    std::fs::set_permissions(&wrong_size, std::fs::Permissions::from_mode(0o600)).unwrap();

    for config in [
        key_config(&[("a", &bad_mode)], Some("a")),
        key_config(&[("a", &symlink)], Some("a")),
        key_config(&[("a", &wrong_size)], Some("a")),
        key_config(&[("a", &file), ("a", &file)], Some("a")),
        key_config(&[("invalid id", &file)], None),
        key_config(&[("a", &file)], Some("b")),
    ] {
        let error = ArchiveKeys::load(Some(&config)).err().unwrap();
        assert_eq!(error.code, ErrorCode::ValidationError);
        assert_eq!(
            error.message,
            "invalid private archive encryption configuration"
        );
    }

    let missing = directory.0.join("missing");
    let config = key_config(&[("a", &missing)], None);
    let error = ArchiveKeys::load(Some(&config)).err().unwrap();
    assert_eq!(error.code, ErrorCode::StorageUnavailable);
    assert_eq!(error.message, "archive encryption key is unavailable");
}
