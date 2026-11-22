use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::header,
};
use nabedra_kv::fs::FaultFs;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use super::*;
use crate::{
    partition::Start,
    raft::{Broker, BrokerConfig, Group, test_options},
};

const TOPIC_ID: Uuid = Uuid::from_u128(100);
const PARTITIONS: &str = "/v1/topics/orders/partitions";
const EVENTS: &str = "/v1/topics/orders/partitions/0/events";
const BATCHES: &str = "/v1/topics/orders/partitions/0/batches";
const CLAIM: &str = "/v1/topics/orders/partitions/0/subscriptions/replay/claim";
const RENEW: &str = "/v1/topics/orders/partitions/0/subscriptions/replay/renew";
const RELEASE: &str = "/v1/topics/orders/partitions/0/subscriptions/replay/release";
const POLL: &str = "/v1/topics/orders/partitions/0/subscriptions/replay/events";
const CHECKPOINT: &str = "/v1/topics/orders/partitions/0/subscriptions/replay/checkpoint";
const OWNER: &str = "test/reader";

#[derive(Default)]
struct MemoryBackend {
    group: Option<Arc<Group>>,
    read_error: Mutex<Option<BackendError>>,
    write_error: Mutex<Option<BackendError>>,
}

impl Backend for MemoryBackend {
    fn partitions<'a>(
        &'a self,
        topic: &'a str,
        after: Option<u32>,
        limit: usize,
    ) -> BackendFuture<'a, PartitionPage> {
        Box::pin(async move {
            if topic != "orders" {
                return Err(BackendError::NotFound);
            }
            let partitions: Vec<_> = [0]
                .into_iter()
                .filter(|part| after.is_none_or(|after| *part > after))
                .take(limit)
                .collect();
            Ok(PartitionPage {
                topic_id: TOPIC_ID,
                partitions,
                next_after: None,
            })
        })
    }

    fn resolve<'a>(&'a self, topic: &'a str, partition: u32) -> BackendFuture<'a, PartitionRoute> {
        Box::pin(async move {
            if topic != "orders" || partition != 0 {
                return Err(BackendError::NotFound);
            }
            Ok(PartitionRoute {
                topic_id: TOPIC_ID,
                partition,
                group_id: "orders-0".into(),
            })
        })
    }

    fn write<'a>(
        &'a self,
        _route: &'a PartitionRoute,
        command: Command,
    ) -> BackendFuture<'a, partition::Mutation> {
        Box::pin(async move {
            if let Some(error) = self.write_error.lock().unwrap().take() {
                return Err(error);
            }
            let group = self.group.as_ref().expect("unexpected backend write");
            let reply = group
                .write(serde_json::to_value(command).unwrap())
                .await
                .unwrap();
            Ok(serde_json::from_value(reply).unwrap())
        })
    }

    fn read<'a>(
        &'a self,
        _route: &'a PartitionRoute,
        query: partition::Query,
    ) -> BackendFuture<'a, Result<QueryReply, partition::Error>> {
        Box::pin(async move {
            if let Some(error) = self.read_error.lock().unwrap().take() {
                return Err(error);
            }
            let group = self.group.as_ref().expect("unexpected backend read");
            Ok(group
                .read(move |tx| partition::query(tx, query))
                .await
                .unwrap())
        })
    }
}

struct Fixture {
    _disk: tempfile::TempDir,
    broker: Arc<Broker>,
    backend: Arc<MemoryBackend>,
    group: Arc<Group>,
    router: Router,
    keys: Arc<Keyring>,
}

impl Fixture {
    async fn new(lease_ms: i64) -> Self {
        let disk = tempfile::tempdir().unwrap();
        let fs = FaultFs::new();
        fs.create_dir_all_durable(disk.path());
        let mut options = test_options(&fs);
        // Use enough space for a maximum-size publish and its Raft record.
        options.memtable_bytes = 4 << 20;
        options.max_replay_bytes = 8 << 20;
        options.journal.segment_bytes = 4 << 20;
        options.journal.max_group_bytes = 4 << 20;
        let keys = keyring();
        let broker = Broker::open_with(
            BrokerConfig {
                id: 1,
                cluster_id: keys.cluster_id(),
                data_dir: disk.path().into(),
                peer_token: "fixture-only-peer-token-with-32-bytes".into(),
                peer_ca: None,
                peer_identity: None,
                allow_plaintext: true,
            },
            options,
        )
        .unwrap();
        let group = broker
            .open_group("orders-0", Arc::new(partition::Machine))
            .await
            .unwrap();
        group
            .initialize(BTreeMap::from([(1, "http://127.0.0.1:9".into())]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while group.leader() != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("RF1 partition elects its leader");
        let backend = Arc::new(MemoryBackend {
            group: Some(group.clone()),
            ..MemoryBackend::default()
        });
        let router = router(
            backend.clone(),
            keys.clone(),
            HttpLimits {
                lease_ms,
                coalesce_ms: 0,
                ..HttpLimits::default()
            },
        )
        .unwrap();
        let fixture = Self {
            _disk: disk,
            broker,
            backend,
            group,
            router,
            keys,
        };
        fixture
            .write(Command::Init {
                topic_id: TOPIC_ID,
                partition: 0,
            })
            .await
            .unwrap();
        fixture
            .write(Command::OpenSubscription {
                name: "replay".into(),
                start: Start::Earliest,
            })
            .await
            .unwrap();
        fixture
    }

    async fn write(&self, command: Command) -> partition::Mutation {
        let reply = self
            .group
            .write(serde_json::to_value(command).unwrap())
            .await
            .unwrap();
        serde_json::from_value(reply).unwrap()
    }

    async fn close(self) {
        self.broker.close_group(self.group.id()).await.unwrap();
    }
}

fn keyring() -> Arc<Keyring> {
    let signing_keys = |id: &str, byte: &str| SigningKeysConfig {
        active: id.into(),
        keys: vec![SigningKeyConfig {
            id: id.into(),
            secret_hex: byte.repeat(32),
            accept_until_ms: i64::MAX,
        }],
    };
    Arc::new(
        Keyring::new(KeyringConfig {
            cluster_id: Uuid::from_u128(1),
            grant: signing_keys("grant", "01"),
            session: signing_keys("session", "02"),
        })
        .unwrap(),
    )
}

fn grant(keys: &Keyring, subject: &str, scope: GrantScope, issued_ms: i64) -> String {
    keys.issue_grant(
        Grant {
            tenant: "test".into(),
            subject: subject.into(),
            scope,
            expires_at_ms: issued_ms + 600_000,
        },
        issued_ms,
    )
    .unwrap()
}

fn publish_grant(keys: &Keyring, issued_ms: i64) -> String {
    grant(
        keys,
        "writer",
        GrantScope::Publish {
            topic: "orders".into(),
        },
        issued_ms,
    )
}

fn read_grant(keys: &Keyring, subject: &str) -> String {
    grant(
        keys,
        subject,
        GrantScope::Read {
            topic: "orders".into(),
            subscription: "replay".into(),
        },
        now_ms(),
    )
}

#[derive(Clone, Copy)]
struct Client<'a> {
    router: &'a Router,
    bearer: &'a str,
    lease: Option<&'a str>,
}

impl<'a> Client<'a> {
    fn new(router: &'a Router, bearer: &'a str) -> Self {
        Self {
            router,
            bearer,
            lease: None,
        }
    }

    fn leased(self, lease: &'a str) -> Self {
        Self {
            lease: Some(lease),
            ..self
        }
    }

    async fn send(self, method: &str, path: &str, body: impl Into<Body>) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {}", self.bearer))
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(lease) = self.lease {
            request = request.header("x-events-lease", lease);
        }
        self.router
            .clone()
            .oneshot(request.body(body.into()).unwrap())
            .await
            .unwrap()
    }

    async fn post(self, path: &str, body: impl Into<Body>) -> Response {
        self.send("POST", path, body).await
    }

    async fn get(self, path: &str) -> Response {
        self.send("GET", path, "").await
    }

    async fn ack(self, delivery: &Value, through: &str) -> Response {
        let body = json!({"delivery": delivery, "through": through});
        self.send("PUT", CHECKPOINT, body.to_string()).await
    }

    async fn claim(self, id: u128) -> Value {
        response_json(self.post(CLAIM, claim_body(id)).await, 200).await
    }
}

async fn response_json(response: Response, status: u16) -> Value {
    let actual_status = response.status();
    let bytes = to_bytes(response.into_body(), 2 << 20).await.unwrap();
    assert_eq!(
        actual_status.as_u16(),
        status,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

async fn error(response: Response, status: u16, code: &str, message: &str) {
    assert_eq!(
        response_json(response, status).await,
        json!({"code": code, "message": message})
    );
}

async fn too_large(response: Response) {
    error(
        response,
        413,
        "payload_too_large",
        "request exceeds its byte limit",
    )
    .await;
}

fn event(id: u128, data: Value) -> Value {
    json!({"event_id": Uuid::from_u128(id), "data": data})
}

fn sized_event(id: u128, bytes: usize) -> String {
    let base = event(id, json!("")).to_string().len();
    let event = event(id, json!("a".repeat(bytes - base))).to_string();
    assert_eq!(event.len(), bytes);
    event
}

fn claim_body(id: u128) -> String {
    json!({"claim_id": Uuid::from_u128(id)}).to_string()
}

#[tokio::test]
async fn publish_byte_and_count_boundaries() {
    let fixture = Fixture::new(30_000).await;
    let token = publish_grant(&fixture.keys, now_ms());
    let client = Client::new(&fixture.router, &token);
    for (id, bytes) in [(1, 65_535), (2, 65_536)] {
        let reply = response_json(client.post(EVENTS, sized_event(id, bytes)).await, 201).await;
        assert_eq!(reply["offset"], id.to_string());
    }
    let oversized = sized_event(3, 65_537);
    too_large(client.post(EVENTS, oversized.clone()).await).await;
    let batch = format!(r#"{{"events":[{oversized}]}}"#);
    too_large(client.post(BATCHES, batch).await).await;

    let mut padded = json!({"events": [event(3, json!(null))]}).to_string();
    padded.extend(std::iter::repeat_n(' ', 1_048_576 - padded.len()));
    let reply = response_json(client.post(BATCHES, padded.clone()).await, 200).await;
    assert_eq!(reply["events"][0]["offset"], "3");
    padded.push(' ');
    too_large(client.post(BATCHES, padded).await).await;

    error(
        client.post(BATCHES, r#"{"events":[]}"#).await,
        400,
        "invalid_request",
        "a publish batch must contain at least one event",
    )
    .await;

    // A compact maximum-size input fits the request limit, but adding offsets and
    // timestamps to its receipts would exceed the independent response limit.
    let mut events: Vec<_> = (10..25).map(|id| sized_event(id, 65_536)).collect();
    let last_bytes = 1_048_576 - r#"{"events":[]}"#.len() - 15 * (65_536 + 1);
    events.push(sized_event(25, last_bytes));
    let body = format!(r#"{{"events":[{}]}}"#, events.join(","));
    assert_eq!(body.len(), 1_048_576);
    error(
        client.post(BATCHES, body).await,
        413,
        "payload_too_large",
        "batch receipts exceed the byte limit",
    )
    .await;

    let mut events: Vec<_> = (100..201).map(|id| event(id, json!(id))).collect();
    error(
        client
            .post(BATCHES, json!({"events": events}).to_string())
            .await,
        400,
        "invalid_request",
        "invalid JSON request",
    )
    .await;
    events.pop();
    let reply = response_json(
        client
            .post(BATCHES, json!({"events": events}).to_string())
            .await,
        200,
    )
    .await;
    let offsets: Vec<_> = reply["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["offset"].as_str().unwrap())
        .collect();
    assert_eq!(
        offsets,
        (4..104)
            .map(|offset| offset.to_string())
            .collect::<Vec<_>>()
    );
    fixture.close().await;
}

#[tokio::test]
async fn publish_conflicts_preserve_original_receipt_and_tail() {
    let fixture = Fixture::new(30_000).await;
    let token = publish_grant(&fixture.keys, now_ms());
    let client = Client::new(&fixture.router, &token);
    let original = event(1, json!({"order": 42})).to_string();
    let first = response_json(client.post(EVENTS, original.clone()).await, 201).await;
    assert_eq!(first["offset"], "1");
    assert_eq!(
        response_json(client.post(EVENTS, original).await, 200).await,
        first
    );
    let second = event(2, json!(2));
    for (events, message) in [
        (
            vec![second.clone(), event(1, json!({"order": 43}))],
            "event ID has different data",
        ),
        (
            vec![second.clone(), second.clone()],
            "event ID repeats within the batch",
        ),
    ] {
        let body = json!({"events": events}).to_string();
        error(client.post(BATCHES, body).await, 409, "conflict", message).await;
    }
    let reply = response_json(client.post(EVENTS, second.to_string()).await, 201).await;
    assert_eq!(reply["offset"], "2");
    fixture.close().await;
}

#[tokio::test]
async fn lease_limits_and_stale_checkpoint_errors() {
    let fixture = Fixture::new(300_000).await;
    let token = read_grant(&fixture.keys, "reader");
    let client = Client::new(&fixture.router, &token);
    let writer = publish_grant(&fixture.keys, now_ms());
    let writer = Client::new(&fixture.router, &writer);
    for id in 1..=2 {
        response_json(
            writer.post(EVENTS, event(id, json!(id)).to_string()).await,
            201,
        )
        .await;
    }
    let before = now_ms();
    let initial = client.claim(1).await;
    let expires_at =
        OffsetDateTime::parse(initial["expires_at"].as_str().unwrap(), &Rfc3339).unwrap();
    let expires_ms = i64::try_from(expires_at.unix_timestamp_nanos() / 1_000_000).unwrap();
    assert!((before + 300_000..=now_ms() + 300_000).contains(&expires_ms));

    let too_long = fixture
        .write(Command::Renew {
            name: "replay".into(),
            owner: OWNER.into(),
            generation: 1,
            now_ms: before,
            ttl_ms: 300_001,
        })
        .await
        .unwrap_err();
    error(
        Into::<ApiError>::into(too_long).into_response(),
        400,
        "invalid_request",
        "invalid lease lifetime",
    )
    .await;
    let stale = client.leased(initial["lease"].as_str().unwrap());
    let rival = read_grant(&fixture.keys, "rival");
    let rival = Client::new(&fixture.router, &rival);
    error(
        rival.post(CLAIM, claim_body(2)).await,
        409,
        "lease_busy",
        "another grant already holds this partition",
    )
    .await;
    let original = response_json(stale.get(POLL).await, 200).await;
    let release = stale.post(RELEASE, "").await;
    assert_eq!(release.status(), StatusCode::NO_CONTENT);
    assert!(to_bytes(release.into_body(), 1).await.unwrap().is_empty());

    let replacement = client.claim(3).await;
    let current = client.leased(replacement["lease"].as_str().unwrap());
    error(
        stale.ack(&original["delivery"], "1").await,
        409,
        "lease_lost",
        "lease is expired or no longer held",
    )
    .await;
    error(
        current.ack(&original["delivery"], "1").await,
        400,
        "invalid_delivery",
        "delivery credential is invalid",
    )
    .await;
    let poll = response_json(current.get(POLL).await, 200).await;
    assert_eq!(poll["after"], "0");
    assert_eq!(poll["events"], original["events"]);
    error(
        current.ack(&poll["delivery"], "3").await,
        400,
        "invalid_request",
        "acknowledgment is outside the delivered prefix",
    )
    .await;
    assert_eq!(
        response_json(current.ack(&poll["delivery"], "1").await, 200).await,
        json!({"offset": "1"})
    );

    fixture
        .write(Command::Retain {
            cutoff_ms: now_ms() + 1,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        response_json(current.get(POLL).await, 410).await,
        json!({
            "code": "checkpoint_expired", "message": "checkpoint is behind the retention floor",
            "partition": 0, "earliest_offset": "3"
        })
    );
    fixture.close().await;
}

#[tokio::test]
async fn backend_errors_and_poll_without_quorum() {
    let fixture = Fixture::new(30_000).await;
    let token = read_grant(&fixture.keys, "reader");
    let reader = Client::new(&fixture.router, &token);
    let claimed = reader.claim(1).await;
    let reader = reader.leased(claimed["lease"].as_str().unwrap());
    let empty = json!({"after": "0", "events": [], "delivery": null});
    assert_eq!(response_json(reader.get(POLL).await, 200).await, empty);
    *fixture.backend.read_error.lock().unwrap() = Some(BackendError::Unavailable);
    error(
        reader.get(POLL).await,
        503,
        "unavailable",
        "Events quorum or route is unavailable",
    )
    .await;
    assert_eq!(response_json(reader.get(POLL).await, 200).await, empty);

    let token = publish_grant(&fixture.keys, now_ms());
    let writer = Client::new(&fixture.router, &token);
    let body = event(1, json!(null)).to_string();
    for (failure, status, code, message) in [
        (
            BackendError::Overloaded,
            429,
            "overloaded",
            "Events capacity is busy",
        ),
        (
            BackendError::InsufficientStorage,
            507,
            "insufficient_storage",
            "broker storage is below its free-space limit",
        ),
    ] {
        *fixture.backend.write_error.lock().unwrap() = Some(failure);
        let reply = writer.post(EVENTS, body.clone()).await;
        if status == 429 {
            assert_eq!(reply.headers()[header::RETRY_AFTER], "1");
        }
        error(reply, status, code, message).await;
        assert_eq!(response_json(reader.get(POLL).await, 200).await, empty);
    }
    *fixture.backend.write_error.lock().unwrap() = Some(BackendError::OutcomeUnknown);
    assert_eq!(
        response_json(writer.post(EVENTS, body).await, 503).await,
        json!({
            "code": "outcome_unknown", "message": "write may have committed", "outcome": "unknown"
        })
    );
    *fixture.backend.read_error.lock().unwrap() = Some(BackendError::Internal);
    error(
        reader.get(POLL).await,
        500,
        "internal_error",
        "Events could not complete the request",
    )
    .await;
    fixture.close().await;
}

#[tokio::test(start_paused = true)]
async fn malformed_requests_and_body_deadline() {
    let keys = keyring();
    let router = router(
        Arc::new(MemoryBackend::default()),
        keys.clone(),
        HttpLimits::default(),
    )
    .unwrap();
    let writer = publish_grant(&keys, now_ms());
    let writer = Client::new(&router, &writer);
    let reader = read_grant(&keys, "reader");
    let reader = Client::new(&router, &reader);
    for (method, route, client) in [
        ("POST", EVENTS, writer),
        ("POST", BATCHES, writer),
        ("POST", CLAIM, reader),
        ("POST", RENEW, reader),
        ("POST", RELEASE, reader),
        ("PUT", CHECKPOINT, reader),
    ] {
        error(
            client.send(method, route, "{").await,
            400,
            "invalid_request",
            "invalid JSON request",
        )
        .await;
    }
    for data in [
        r#"{"same":1,"same":2}"#.to_owned(),
        format!("{}0{}", "[".repeat(65), "]".repeat(65)),
    ] {
        let id = Uuid::from_u128(1);
        let body = format!(r#"{{"event_id":"{id}","data":{data}}}"#);
        error(
            writer.post(EVENTS, body).await,
            400,
            "invalid_request",
            "invalid, repeated, or deeply nested event data",
        )
        .await;
    }
    for (client, path) in [
        (writer, format!("{PARTITIONS}?limit=0")),
        (writer, format!("{PARTITIONS}?limit=101")),
        (reader, format!("{POLL}?limit=101")),
    ] {
        error(
            client.get(&path).await,
            400,
            "invalid_request",
            "page limit is out of range",
        )
        .await;
    }
    error(
        writer.get(EVENTS).await,
        405,
        "method_not_allowed",
        "method does not match the route",
    )
    .await;
    error(
        writer.get("/missing").await,
        404,
        "not_found",
        "Events resource not found",
    )
    .await;
    error(
        reader.post(EVENTS, "{}").await,
        403,
        "forbidden",
        "grant does not allow this operation",
    )
    .await;
    let body = Body::from_stream(futures_util::stream::pending::<
        Result<bytes::Bytes, std::io::Error>,
    >());
    error(
        writer.post(EVENTS, body).await,
        408,
        "request_timeout",
        "request body was not received before the deadline",
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn auth_clock_skew_boundary() {
    let keys = keyring();
    let fixed_now = 100_000;
    let mut headers = HeaderMap::new();
    for skew in [4_999, 5_000, 5_001] {
        let token = publish_grant(&keys, fixed_now + skew);
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        let result = keys.authorize(&headers, Access::Discover("orders"), fixed_now);
        if skew <= 5_000 {
            assert_eq!(result.unwrap().tenant, "test");
        } else {
            error(
                result.unwrap_err().into_response(),
                401,
                "unauthorized",
                "bearer grant is missing or invalid",
            )
            .await;
        }
    }

    let router = router(
        Arc::new(MemoryBackend::default()),
        keys.clone(),
        HttpLimits::default(),
    )
    .unwrap();
    let token = publish_grant(&keys, now_ms());
    let client = Client::new(&router, &token);
    assert_eq!(
        response_json(client.get(PARTITIONS).await, 200).await,
        json!({"topic_id": TOPIC_ID, "partitions": [0], "next_after": null})
    );
    let future_token = publish_grant(&keys, now_ms() + 60_000);
    for token in [&future_token, "invalid"] {
        let reply = Client::new(&router, token).get(PARTITIONS).await;
        assert_eq!(reply.headers()[header::WWW_AUTHENTICATE], "Bearer");
        error(
            reply,
            401,
            "unauthorized",
            "bearer grant is missing or invalid",
        )
        .await;
    }
}

#[test]
fn configured_lease_duration_limit() {
    for (lease_ms, valid) in [(0, false), (1, true), (300_000, true), (300_001, false)] {
        let limits = HttpLimits {
            lease_ms,
            ..HttpLimits::default()
        };
        let result = router(Arc::new(MemoryBackend::default()), keyring(), limits);
        if valid {
            assert!(result.is_ok(), "{lease_ms}");
        } else {
            assert_eq!(
                result.unwrap_err().to_string(),
                "invalid Events request, coalescing, or lease timeout"
            );
        }
    }
}
