use meshdb_server::storage::*;

#[test]
fn canonical_metadata_allowlist() {
    for path in [
        "decisions/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json",
        "control-decisions/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json",
        "cluster/control.json",
        "cluster/heartbeats/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json",
    ] {
        assert_eq!(ObjectKey::parse_relative(path).unwrap().as_str(), path);
    }
    for path in [
        "decisions/AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA.json",
        "cluster/other.json",
        "cluster/heartbeats/no.json",
    ] {
        assert!(ObjectKey::parse_relative(path).is_err());
    }
}

struct Service {
    child: std::process::Child,
    directory: std::path::PathBuf,
    url: String,
}
impl Service {
    fn start(directory: std::path::PathBuf) -> Self {
        Self::launch(directory, false)
    }
    fn launch(directory: std::path::PathBuf, tls: bool) -> Self {
        use std::io::BufRead;
        std::fs::create_dir_all(&directory).unwrap();
        let mut command = std::process::Command::new("python3.11");
        command.arg("-B");
        if tls {
            command
                .arg("tests/support/http_store_tls.py")
                .arg(&directory);
        } else {
            command
                .args(["../scripts/local_store.py", "--port", "0", "--database"])
                .arg(directory.join("store.sqlite"));
        }
        let mut child = command
            .env("MESHDB_STORE_TOKEN", "disposable-test-token")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut line = String::new();
            let result = std::io::BufReader::new(stdout).read_line(&mut line);
            let _ = tx.send(result.map(|_| line));
        });
        let mut service = Self {
            child,
            directory,
            url: String::new(),
        };
        let line = rx.recv_timeout(std::time::Duration::from_secs(10));
        if line.is_err() {
            service.stop();
        }
        reader.join().unwrap();
        let value: serde_json::Value = serde_json::from_str(&line.unwrap().unwrap()).unwrap();
        service.url = format!("http://127.0.0.1:{}", value["port"].as_u64().unwrap());
        service
    }
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    fn storage(&self) -> Storage<HttpStore> {
        let ns = Namespace::new("test/nested").unwrap();
        Storage::new(
            HttpStore::new(&self.url, ns.clone(), "disposable-test-token", None).unwrap(),
            ns,
        )
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test]
async fn api_reopens_persisted_partitions_and_preserves_query_and_batch_atomicity() {
    use axum::{Router, body::Body, http::Request};
    use http_body_util::BodyExt;
    use meshdb_server::{api, engine::Engine, executor::Executor, worker_pool::WorkerPool};
    use serde_json::{Value, json};
    use std::num::NonZeroUsize;
    use tower::ServiceExt;

    async fn request(app: &Router, operation: &str, payload: Value) -> (u16, Value) {
        let id = "11111111-1111-4111-8111-111111111111";
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/operations")
                    .header("authorization", "Bearer api-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"request_id":id,"operation":operation,"payload":payload})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["request_id"], id);
        (status, body)
    }

    let directory =
        std::env::temp_dir().join(format!("meshdb-engine-http-{}", uuid::Uuid::new_v4()));
    let mut service = Service::start(directory.clone());
    let storage = service.storage();
    storage.initialize_catalog().await.unwrap();
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, "1 B".parse().unwrap());
    let app = api::router(
        engine.clone(),
        Executor::new(1, 0).unwrap(),
        "api-token".into(),
    );
    let (status, created) = request(
        &app,
        "table.create",
        json!({"name":"items","schema":{"id":"int64","rank":"int64","label":"utf8"},"key":"id","format":"parquet"}),
    )
    .await;
    assert_eq!(status, 200);
    let table_id = created["data"]["id"].clone();
    let rows: Vec<_> = (0..12)
        .rev()
        .map(|id| json!({"id":id,"rank":id % 3,"label":format!("row{id}")}))
        .collect();
    assert_eq!(
        request(
            &app,
            "table.insert_many",
            json!({"table_id":table_id,"rows":rows})
        )
        .await,
        (
            200,
            json!({"request_id":"11111111-1111-4111-8111-111111111111","data":12})
        )
    );
    let catalog = engine.storage().load_catalog().await.unwrap().catalog;
    let TableData::Partitioned { files } = &catalog.tables["items"].data else {
        panic!("write must publish a partition manifest")
    };
    assert_eq!(files.len(), 12);
    workers.shutdown().await.unwrap();
    drop(app);
    drop(engine);
    service.stop();

    let restarted = Service::start(directory.clone());
    service.directory = directory.join("unused");
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(restarted.storage(), pool, "1 B".parse().unwrap());
    let app = api::router(
        engine.clone(),
        Executor::new(1, 0).unwrap(),
        "api-token".into(),
    );
    let query = json!({"select":["label"],"filter":{"op":"ge","column":"rank","value":1},"order_by":{"column":"rank","desc":true},"limit":3});
    let (status, result) = request(
        &app,
        "query.collect",
        json!({"table_id":table_id,"query":query}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        result["data"],
        json!([{"label":"row2"},{"label":"row5"},{"label":"row8"}])
    );

    let before = engine.storage().load_catalog().await.unwrap().catalog;
    let (status, result) = request(
        &app,
        "table.insert_many",
        json!({"table_id":table_id,"rows":[{"id":12,"rank":2,"label":"must not persist"},{"id":8,"rank":2,"label":"duplicate"}]}),
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(result["error"]["code"], "conflict");
    assert_eq!(result["error"]["outcome"], "not_applied");
    assert_eq!(
        engine.storage().load_catalog().await.unwrap().catalog,
        before
    );
    let (status, missing) = request(&app, "table.get", json!({"table_id":table_id,"key":12})).await;
    assert_eq!(status, 200);
    assert_eq!(missing["data"], Value::Null);

    for (operation, payload) in [
        (
            "table.update",
            json!({"table_id":table_id,"key":2,"values":{"rank":-1}}),
        ),
        ("table.delete", json!({"table_id":table_id,"key":5})),
    ] {
        let (status, result) = request(&app, operation, payload).await;
        assert_eq!(status, 200);
        assert_eq!(result["data"], true);
    }
    let (status, result) = request(
        &app,
        "query.collect",
        json!({"table_id":table_id,"query":query}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        result["data"],
        json!([{"label":"row8"},{"label":"row11"},{"label":"row1"}])
    );
    let (status, result) = request(&app, "query.count", json!({"table_id":table_id,"query":{"select":null,"filter":null,"order_by":null,"limit":null}})).await;
    assert_eq!(status, 200);
    assert_eq!(result["data"], 11);
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn actual_service_cas_restart_bounds_and_guards() {
    let directory = std::env::temp_dir().join(format!("meshdb-http-{}", uuid::Uuid::new_v4()));
    let mut service = Service::start(directory.clone());
    let store = service.storage();
    let key = ObjectKey::catalog();
    assert_eq!(
        store.read(&key, 20).await.unwrap_err(),
        StorageError::NotFound
    );
    let first = store
        .put(&key, b"same".to_vec(), WriteCondition::Create)
        .await
        .unwrap();
    let second = store
        .put(&key, b"same".to_vec(), WriteCondition::Match(&first))
        .await
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(
        store
            .put(&key, vec![], WriteCondition::Match(&first))
            .await
            .unwrap_err(),
        StorageError::Conflict
    );
    let barrier = tokio::sync::Barrier::new(2);
    let writer = |bytes| {
        let (barrier, store, key, second) = (&barrier, &store, &key, &second);
        async move {
            barrier.wait().await;
            store
                .put(key, vec![bytes], WriteCondition::Match(second))
                .await
        }
    };
    let (a, b) = tokio::join!(writer(1), writer(2));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(&StorageError::Conflict)
    );
    let before = store.read(&key, 1).await.unwrap();
    assert_eq!(
        store.read(&key, 0).await.unwrap_err(),
        StorageError::LimitExceeded
    );
    service.stop();
    let mut restarted = Service::start(directory.clone());
    // Transfer directory ownership so the old service cannot remove live state.
    service.directory = directory.join("unused");
    let store = restarted.storage();
    assert_eq!(store.read(&key, 1).await.unwrap(), before);
    assert_eq!(
        store
            .put(&key, vec![], WriteCondition::Match(&second))
            .await
            .unwrap_err(),
        StorageError::Conflict
    );
    for key in [
        ObjectKey::control(),
        ObjectKey::heartbeat(uuid::Uuid::new_v4()),
    ] {
        let tag = store
            .put(&key, vec![], WriteCondition::Create)
            .await
            .unwrap();
        store
            .put(&key, vec![1], WriteCondition::Match(&tag))
            .await
            .unwrap();
    }
    for key in [
        ObjectKey::decision(uuid::Uuid::new_v4()),
        ObjectKey::control_decision(uuid::Uuid::new_v4()),
    ] {
        store
            .put(&key, vec![], WriteCondition::Create)
            .await
            .unwrap();
        assert_eq!(
            store
                .put(&key, vec![], WriteCondition::Match("\"g1\""))
                .await
                .unwrap_err(),
            StorageError::InvalidPath
        );
    }
    let bad = HttpStore::new(
        &restarted.url,
        Namespace::new("test/nested").unwrap(),
        "wrong",
        None,
    )
    .unwrap();
    assert!(matches!(
        bad.get("test/nested/catalog.json").await,
        Err(StorageError::Unavailable)
    ));
    assert_eq!(
        bad.put("test/nested/catalog.json", vec![], WriteCondition::Create)
            .await
            .unwrap_err(),
        StorageError::Unavailable
    );
    assert!(matches!(
        store.backend().get("test/nested-other/catalog.json").await,
        Err(StorageError::InvalidPath)
    ));
    assert_eq!(
        store
            .backend()
            .put(
                "test/nested/catalog.json",
                vec![],
                WriteCondition::Match("bad\r\nheader")
            )
            .await
            .unwrap_err(),
        StorageError::Configuration
    );
    restarted.stop();
}

#[test]
fn constructor_rejects_unsafe_configuration_locally() {
    for base in [
        "http://user:pass@localhost",
        "http://localhost/?q=x",
        "http://localhost/#x",
        "http://localhost/a/../b",
        "http://localhost/a//b",
        "http://localhost//",
        "http://localhost/%61",
        "ftp://localhost",
        "http://localhost/\\x",
        "http://localhost:bogus",
        "http://localhost:65536",
        "http://localhost:",
        "http://:80",
        "http://[::1]:bogus",
        "http://[::1]:65536",
        "http://[::1]:",
    ] {
        assert!(HttpStore::new(base, Namespace::new("test").unwrap(), "token", None).is_err());
    }
    for base in [
        "http://localhost",
        "http://localhost:80",
        "http://localhost:65535",
        "http://[::1]",
        "http://[::1]:80",
    ] {
        assert!(HttpStore::new(base, Namespace::new("test").unwrap(), "token", None).is_ok());
    }
    for namespace in ["space here", "café", &"a".repeat(4090)] {
        assert!(
            HttpStore::new(
                "http://localhost",
                Namespace::new(namespace).unwrap(),
                "token",
                None
            )
            .is_err()
        );
    }
    for token in ["", "x\r\nInjected: y", "has space", "é"] {
        assert!(
            HttpStore::new(
                "http://localhost",
                Namespace::new("test").unwrap(),
                token,
                None
            )
            .is_err()
        );
    }
    assert!(
        HttpStore::new(
            "https://localhost",
            Namespace::new("test").unwrap(),
            "token",
            Some(b"bad PEM")
        )
        .is_err()
    );
}

// One raw TCP fixture, intentionally bypassing an HTTP server's framing repair.
// It consumes the complete request, counts any resend, and owns its task.
async fn fixture(response: Vec<u8>, put: bool) -> (Result<Vec<u8>, StorageError>, usize) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let text = String::from_utf8(request).unwrap();
        let length = text
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; length];
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(body, if put { vec![42] } else { vec![] });
        assert!(text.starts_with(if put {
            "PUT /prefix/v1/objects/test/catalog.json "
        } else {
            "GET /prefix/v1/objects/test/catalog.json "
        }));
        let _ = socket.write_all(&response).await;
        drop(socket);
        let extra = tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
            .await
            .is_ok();
        1 + usize::from(extra)
    });
    let ns = Namespace::new("test").unwrap();
    let backend = HttpStore::new(
        &format!("http://{address}/prefix/"),
        ns.clone(),
        "token",
        None,
    )
    .unwrap();
    let storage = Storage::new(backend, ns);
    let result = if put {
        storage
            .put(&ObjectKey::catalog(), vec![42], WriteCondition::Create)
            .await
            .map(String::into_bytes)
    } else {
        storage
            .read(&ObjectKey::catalog(), 10)
            .await
            .map(|object| object.bytes)
    };
    (result, task.await.unwrap())
}

#[tokio::test]
async fn incomplete_and_malformed_responses_never_claim_success_or_retry() {
    for response in [
        b"HTTP/1.1 200 OK\r\nETag: \"opaque\"\r\nContent-Length: 8\r\n\r\nshort".to_vec(),
        b"HTTP/1.1 200 OK\r\nETag: W/\"weak\"\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nETag: \"a\"\r\nETag: \"b\"\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
    ] {
        let (result, count) = fixture(response, false).await;
        assert_eq!(result.unwrap_err(), StorageError::Unavailable);
        assert_eq!(count, 1);
    }
    // GET does not require Content-Length. Strong ETags are opaque, including commas.
    let (result, count) = fixture(
        b"HTTP/1.1 200 OK\r\nETag: \"a,b\"\r\nConnection: close\r\n\r\nok".to_vec(),
        false,
    )
    .await;
    assert_eq!(result.unwrap(), b"ok");
    assert_eq!(count, 1);
    let (result, count) = fixture(
        b"HTTP/1.1 200 OK\r\nETag: \"opaque\xff\"\r\nContent-Length: 0\r\n\r\n".to_vec(),
        false,
    )
    .await;
    assert_eq!(result.unwrap(), b"");
    assert_eq!(count, 1);
    for response in [
        vec![],
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 204 No Content\r\nETag: W/\"weak\"\r\n\r\n".to_vec(),
        b"HTTP/1.1 204 No Content\r\nETag: \"ok\"\r\nContent-Length: 1\r\n\r\nx".to_vec(),
        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /prefix/v1/objects/test/catalog.json\r\nContent-Length: 0\r\n\r\n".to_vec(),
    ] {
        let (result, count) = fixture(response, true).await;
        assert_eq!(result.unwrap_err(), StorageError::Ambiguous);
        assert_eq!(count, 1);
    }
}

#[tokio::test]
async fn tls_requires_trusted_ca_and_retains_verification() {
    let service = Service::launch(
        std::env::temp_dir().join(format!("meshdb-http-tls-{}", uuid::Uuid::new_v4())),
        true,
    );
    let pem = std::fs::read(service.directory.join("tls/ca.crt")).unwrap();
    let url = service.url.replace("http:", "https:");
    let namespace = Namespace::new("test").unwrap();
    let trusted = Storage::new(
        HttpStore::new(&url, namespace.clone(), "token", Some(&pem)).unwrap(),
        namespace.clone(),
    );
    assert_eq!(
        trusted.read(&ObjectKey::catalog(), 2).await.unwrap().bytes,
        b"ok"
    );
    let untrusted = HttpStore::new(&url, namespace, "token", None).unwrap();
    assert!(matches!(
        untrusted.get("test/catalog.json").await,
        Err(StorageError::Unavailable)
    ));
}
