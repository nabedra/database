use std::{fmt::Write, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use nabedra_events::{
    http::{
        Backend, BackendError, BackendFuture, Grant, GrantScope, HttpLimits, Keyring,
        KeyringConfig, PartitionPage, PartitionRoute, SigningKeyConfig, SigningKeysConfig, now_ms,
        router,
    },
    partition::{self, Command, QueryReply},
};
use tokio::runtime::Runtime;
use tower::ServiceExt;

const PUBLISH_ONE: &str = "/v1/topics/fuzz/partitions/0/events";
const PUBLISH_BATCH: &str = "/v1/topics/fuzz/partitions/0/batches";
const SUBSCRIPTION: &str = "/v1/topics/fuzz/partitions/0/subscriptions/test";

/// Returns the decoder verdicts, for example `event=accept batch=reject`.
/// `accept` means the request passed every decoder and reached route
/// resolution, which this harness answers with 404.
pub fn fuzz_events_publish(data: &[u8]) -> String {
    let single = check_json("POST", PUBLISH_ONE, data);
    let batch_status = check_json("POST", PUBLISH_BATCH, data);
    if single == StatusCode::NOT_FOUND {
        let mut batch = b"{\"events\":[".to_vec();
        batch.extend_from_slice(data);
        batch.extend_from_slice(b"]}");
        assert_eq!(status("POST", PUBLISH_BATCH, &batch), single);
    }
    format!("event={} batch={}", verdict(single), verdict(batch_status))
}

pub fn fuzz_events_poll(data: &[u8]) -> String {
    let uri = poll_uri(data);
    let result = status("GET", &uri, b"");
    assert!(matches!(
        result,
        StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND
    ));
    if result == StatusCode::NOT_FOUND {
        let unknown = format!("{uri}&unexpected=1");
        assert_eq!(status("GET", &unknown, b""), StatusCode::BAD_REQUEST);
    }
    verdict(result).to_owned()
}

pub fn fuzz_events_ack(data: &[u8]) -> String {
    verdict(check_json(
        "PUT",
        &format!("{SUBSCRIPTION}/checkpoint"),
        data,
    ))
    .to_owned()
}

pub fn fuzz_events_lease(data: &[u8]) -> String {
    let verdicts: Vec<_> = ["claim", "renew", "release"]
        .into_iter()
        .map(|action| {
            let result = check_json("POST", &format!("{SUBSCRIPTION}/{action}"), data);
            format!("{action}={}", verdict(result))
        })
        .collect();
    verdicts.join(" ")
}

fn verdict(status: StatusCode) -> &'static str {
    match status {
        StatusCode::NOT_FOUND => "accept",
        StatusCode::BAD_REQUEST => "reject",
        StatusCode::PAYLOAD_TOO_LARGE => "too-large",
        other => panic!("unexpected decoder status {other}"),
    }
}

fn poll_uri(query: &[u8]) -> String {
    let mut uri = format!("{SUBSCRIPTION}/events?");
    for &byte in query {
        // Preserve form separators and existing percent escapes. Escape bytes
        // that cannot appear literally in an HTTP request target.
        if byte.is_ascii_graphic() && byte != b'#' {
            uri.push(char::from(byte));
        } else {
            write!(&mut uri, "%{byte:02X}").expect("writing to String is infallible");
        }
    }
    uri
}

fn check_json(method: &str, uri: &str, data: &[u8]) -> StatusCode {
    let result = status(method, uri, data);
    assert!(matches!(
        result,
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE | StatusCode::NOT_FOUND,
    ));
    if result == StatusCode::NOT_FOUND {
        let mut trailing = data.to_vec();
        trailing.push(b'!');
        assert!(matches!(
            status(method, uri, &trailing),
            StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE,
        ));
    }
    result
}

// A 404 from resolve marks acceptance by the HTTP decoders. Stop before
// dispatching any command/query or requiring a live lease or storage.
struct Unresolved;

impl Backend for Unresolved {
    fn partitions<'a>(
        &'a self,
        _: &'a str,
        _: Option<u32>,
        _: usize,
    ) -> BackendFuture<'a, PartitionPage> {
        unreachable!("decoder routes do not discover partitions")
    }

    fn resolve<'a>(&'a self, _: &'a str, _: u32) -> BackendFuture<'a, PartitionRoute> {
        Box::pin(async { Err(BackendError::NotFound) })
    }

    fn write<'a>(
        &'a self,
        _: &'a PartitionRoute,
        _: Command,
    ) -> BackendFuture<'a, partition::Mutation> {
        unreachable!("route resolution failed before write")
    }

    fn read<'a>(
        &'a self,
        _: &'a PartitionRoute,
        _: partition::Query,
    ) -> BackendFuture<'a, Result<QueryReply, partition::Error>> {
        unreachable!("route resolution failed before read")
    }
}

struct HttpHarness {
    runtime: Runtime,
    keys: Arc<Keyring>,
}

thread_local! {
    static HTTP: HttpHarness = HttpHarness::new();
}

impl HttpHarness {
    fn new() -> Self {
        fn signing_keys(byte: &str) -> SigningKeysConfig {
            SigningKeysConfig {
                active: "test".into(),
                keys: vec![SigningKeyConfig {
                    id: "test".into(),
                    secret_hex: byte.repeat(32),
                    accept_until_ms: i64::MAX,
                }],
            }
        }
        let keys = Keyring::new(KeyringConfig {
            cluster_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse().unwrap(),
            grant: signing_keys("aa"),
            session: signing_keys("bb"),
        })
        .expect("valid test keyring");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("in-process HTTP runtime");
        Self {
            runtime,
            keys: Arc::new(keys),
        }
    }

    fn authorization(&self, scope: GrantScope) -> String {
        let now = now_ms();
        let grant = Grant {
            tenant: "fuzz".into(),
            subject: "test".into(),
            scope,
            expires_at_ms: now + 60_000,
        };
        let token = self.keys.issue_grant(grant, now).expect("valid test grant");
        format!("Bearer {token}")
    }

    fn router(&self) -> Router {
        // Each request gets a fresh admission budget.
        router(
            Arc::new(Unresolved),
            self.keys.clone(),
            HttpLimits::default(),
        )
        .unwrap()
    }
}

fn status(method: &str, uri: &str, data: &[u8]) -> StatusCode {
    HTTP.with(|http| {
        let scope = if uri.starts_with(SUBSCRIPTION) {
            GrantScope::Read {
                topic: "fuzz".into(),
                subscription: "test".into(),
            }
        } else {
            GrantScope::Publish {
                topic: "fuzz".into(),
            }
        };
        let Ok(request) = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, http.authorization(scope))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(data.to_vec()))
        else {
            // The HTTP URI parser can reject arbitrary query bytes before routing.
            return StatusCode::BAD_REQUEST;
        };
        http.runtime
            .block_on(http.router().oneshot(request))
            .unwrap()
            .status()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reaches_events_decoders() {
        let one = include_bytes!("../corpus/events_publish/event.json");
        let batch = include_bytes!("../corpus/events_publish/batch.json");
        let duplicate = include_bytes!("../corpus/events_publish/duplicate-data-key.json");
        assert_eq!(status("POST", PUBLISH_ONE, one), StatusCode::NOT_FOUND);
        assert_eq!(status("POST", PUBLISH_BATCH, batch), StatusCode::NOT_FOUND);
        assert_eq!(
            status("POST", PUBLISH_ONE, duplicate),
            StatusCode::BAD_REQUEST
        );

        let valid = include_bytes!("../corpus/events_poll/max-limit.query");
        let invalid = include_bytes!("../corpus/events_poll/zero.query");
        assert_eq!(status("GET", &poll_uri(valid), b""), StatusCode::NOT_FOUND);
        assert_eq!(
            status("GET", &poll_uri(invalid), b""),
            StatusCode::BAD_REQUEST
        );

        let valid = include_bytes!("../corpus/events_ack/max-offset.json");
        let invalid = include_bytes!("../corpus/events_ack/leading-zero.json");
        let ack = format!("{SUBSCRIPTION}/checkpoint");
        assert_eq!(status("PUT", &ack, valid), StatusCode::NOT_FOUND);
        assert_eq!(status("PUT", &ack, invalid), StatusCode::BAD_REQUEST);

        let valid = include_bytes!("../corpus/events_lease/claim.json");
        let invalid = include_bytes!("../corpus/events_lease/invalid-uuid.json");
        let claim = format!("{SUBSCRIPTION}/claim");
        assert_eq!(status("POST", &claim, valid), StatusCode::NOT_FOUND);
        assert_eq!(status("POST", &claim, invalid), StatusCode::BAD_REQUEST);
        for action in ["renew", "release"] {
            let uri = format!("{SUBSCRIPTION}/{action}");
            assert_eq!(status("POST", &uri, b"{}"), StatusCode::NOT_FOUND);
            assert_eq!(status("POST", &uri, valid), StatusCode::BAD_REQUEST);
        }
    }
}
