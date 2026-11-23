#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::{
    cluster::{
        configuration::{Template, Timing},
        state::{Control, MAX_CONTROL_BYTES, PolicyMode},
    },
    storage::*,
};
use serde_json::{Value, json};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::Barrier,
    time::{Duration, Instant},
};
use uuid::Uuid;
#[path = "support/storage.rs"]
mod support;
use support::Fake;
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn root() -> Value {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let hash = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    json!({"version":1,"cluster_id":id(1),"name":"Fleet_1","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":hash,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":"meshdb.callbacks.v1"},"provisioning":"managed","template":template,"timing":Timing::default(),"revision":1,"change_id":id(2),"policy":{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[],"intents":[]})
}
fn initial() -> Control {
    Control::decode(&serde_json::to_vec(&root()).unwrap()).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[derive(Clone, Copy, Default)]
enum Mode {
    #[default]
    Normal,
    EmptyAck,
    EmptyAckMissing,
    EmptyTokenReconcile,
    HangReconcile,
    Reconcile(StorageError),
    CorruptReconcile,
    HangGet,
    HangBody,
    HangPut,
    ExpireMissing,
    Truncated,
    Oversize,
}
#[derive(Default)]
struct Backend {
    fake: Fake,
    mode: Mode,
    gets: AtomicUsize,
    barrier: Option<Arc<Barrier>>,
}
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let n = self.gets.fetch_add(1, Ordering::SeqCst);
        if n > 0 {
            match self.mode {
                Mode::Reconcile(e) => return Err(e),
                Mode::HangReconcile => return std::future::pending().await,
                Mode::EmptyTokenReconcile => {
                    return Ok(Download::from_bytes(
                        initial().encode().unwrap(),
                        String::new(),
                    ));
                }
                Mode::CorruptReconcile => {
                    return Ok(Download::from_bytes(b"{}".to_vec(), "bad".into()));
                }
                _ => {}
            }
        }
        match self.mode {
            Mode::HangGet => std::future::pending().await,
            Mode::HangBody => Ok(Download::new(
                Reader {
                    remaining: 0,
                    kind: 0,
                },
                "body".into(),
            )),
            Mode::Truncated => Ok(Download::new(
                Truncated(std::io::Cursor::new(initial().encode().unwrap())),
                "body".into(),
            )),
            Mode::Oversize => Ok(Download::new(
                Reader {
                    remaining: MAX_CONTROL_BYTES + 1,
                    kind: 2,
                },
                "body".into(),
            )),
            Mode::ExpireMissing => {
                // A synchronously completing backend can run past timeout_at's
                // first clock check. The caller must check before submitting PUT.
                std::thread::sleep(Duration::from_millis(30));
                Err(StorageError::NotFound)
            }
            _ => {
                let result = self.fake.get(key).await;
                if n < 2
                    && let Some(barrier) = &self.barrier
                {
                    barrier.wait().await;
                }
                result
            }
        }
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let token = self.fake.put(key, bytes, condition).await?;
        match self.mode {
            Mode::EmptyAck => Ok(String::new()),
            Mode::EmptyAckMissing => {
                self.fake.state.lock().unwrap().objects.remove(key);
                Ok(String::new())
            }
            Mode::HangPut => std::future::pending().await,
            _ => Ok(token),
        }
    }
}
// Deliver a complete, codec-valid prefix and then fail instead of clean EOF.
// A loader must finish the transfer before accepting the prefix as a root.
struct Truncated(std::io::Cursor<Vec<u8>>);
impl AsyncRead for Truncated {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.0.position() as usize == self.0.get_ref().len() {
            Poll::Ready(Err(std::io::Error::other("truncated private response")))
        } else {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }
}
struct Reader {
    remaining: usize,
    kind: u8,
}
impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.kind {
            0 => Poll::Pending,
            _ => {
                let n = self.remaining.min(buf.remaining());
                buf.put_slice(&vec![b' '; n]);
                self.remaining -= n;
                Poll::Ready(Ok(()))
            }
        }
    }
}
fn store(backend: Backend) -> Storage<Backend> {
    Storage::new(backend, Namespace::new("test").unwrap())
}
fn seed(s: &Storage<Backend>, bytes: Vec<u8>, token: &str) {
    s.backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (bytes, token.into()));
}
fn counts(s: &Storage<Backend>) -> (usize, usize) {
    let st = s.backend().fake.state.lock().unwrap();
    (s.backend().gets.load(Ordering::SeqCst), st.puts)
}
#[tokio::test]
async fn creates_once_reloads_and_preserves_later_existing_root() {
    let s = store(Backend::default());
    let c = initial();
    let loaded = s.initialize_control(&c, deadline()).await.unwrap();
    assert_eq!(loaded.root(), &c);
    assert_eq!(s.load_control(deadline()).await.unwrap(), loaded);
    assert_eq!(counts(&s), (2, 1));
    let mut later = c.clone();
    later.revision = 91;
    later.policy.revision = 7;
    later.admin_seq = 3;
    later.admin_head = Some(id(44));
    later.controller.epoch = 9;
    seed(&s, later.encode().unwrap(), "PRIVATE-TOKEN");
    let mut other = initial();
    other.cluster_id = id(99);
    other.change_id = id(100);
    other.policy.desired = 1024;
    other.policy.min_nodes = 1024;
    other.policy.max_nodes = 1024;
    let mut v = serde_json::to_value(&other).unwrap();
    v["template"]["storage"]["options"]["token_file"] = json!("/different/local/path");
    other.template = Template::decode(&serde_json::to_vec(&v["template"]).unwrap()).unwrap();
    other.config.template_fingerprint = other.template.fingerprint().unwrap();
    let observed = s.initialize_control(&other, deadline()).await.unwrap();
    assert_eq!(observed.root(), &later);
    assert_eq!(counts(&s), (3, 1));
    let debug = format!("{observed:?}");
    for private in ["PRIVATE", "Fleet_1", "/token", "fingerprint", "template"] {
        assert!(!debug.contains(private));
    }
    assert_eq!(
        s.backend().fake.state.lock().unwrap().objects[KEY],
        (later.encode().unwrap(), "PRIVATE-TOKEN".into())
    );
}
#[tokio::test]
async fn two_creators_read_absence_then_actual_cas_selects_one() {
    let s = store(Backend {
        barrier: Some(Arc::new(Barrier::new(2))),
        ..Default::default()
    });
    let a = initial();
    let mut b = a.clone();
    b.change_id = id(42);
    b.policy.desired = 2;
    b.policy.min_nodes = 2;
    b.policy.max_nodes = 2;
    let (a_result, b_result) = tokio::join!(
        s.initialize_control(&a, deadline()),
        s.initialize_control(&b, deadline())
    );
    let winner = a_result.unwrap();
    assert_eq!(winner, b_result.unwrap());
    assert!(winner.root() == &a || winner.root() == &b);
    assert_eq!(counts(&s), (3, 2));
    assert_eq!(winner, s.load_control(deadline()).await.unwrap());
    let st = s.backend().fake.state.lock().unwrap();
    assert_eq!(st.generation, 1);
    assert_eq!(st.put_keys, vec![KEY, KEY]);
    assert_eq!(st.objects[KEY].0, winner.root().encode().unwrap());
}
#[tokio::test]
async fn lost_response_and_empty_ack_reconcile_without_resubmitting() {
    for empty in [false, true] {
        let s = store(Backend {
            mode: if empty { Mode::EmptyAck } else { Mode::Normal },
            ..Default::default()
        });
        s.backend().fake.state.lock().unwrap().lose_put_response = !empty;
        let c = initial();
        let observed = s.initialize_control(&c, deadline()).await.unwrap();
        assert_eq!(observed.root(), &c);
        assert_eq!(counts(&s), (2, 1));
        assert_eq!(observed, s.load_control(deadline()).await.unwrap());
    }
}
#[tokio::test]
async fn unresolved_possible_write_stays_ambiguous_and_conflict_keeps_read_error() {
    for mode in [
        Mode::Reconcile(StorageError::NotFound),
        Mode::Reconcile(StorageError::Unavailable),
        Mode::CorruptReconcile,
        Mode::EmptyTokenReconcile,
    ] {
        for write_error in [StorageError::Ambiguous, StorageError::Conflict] {
            let s = store(Backend {
                mode,
                ..Default::default()
            });
            s.backend().fake.state.lock().unwrap().put_faults.insert(
                KEY.into(),
                (write_error, write_error == StorageError::Ambiguous),
            );
            let expected = if write_error == StorageError::Ambiguous {
                StorageError::Ambiguous
            } else {
                match mode {
                    Mode::Reconcile(e) => e,
                    Mode::EmptyTokenReconcile => StorageError::Unavailable,
                    _ => StorageError::InvalidCatalog,
                }
            };
            assert_eq!(
                s.initialize_control(&initial(), deadline())
                    .await
                    .unwrap_err(),
                expected
            );
            assert_eq!(counts(&s), (2, 1));
        }
    }
    for error in [
        StorageError::Unavailable,
        StorageError::Configuration,
        StorageError::LimitExceeded,
    ] {
        let s = store(Backend::default());
        s.backend()
            .fake
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert(KEY.into(), (error, false));
        assert_eq!(
            s.initialize_control(&initial(), deadline())
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(counts(&s), (1, 1));
    }
}
#[tokio::test]
async fn invalid_initial_state_rejects_before_any_io_even_with_existing_root() {
    let s = store(Backend::default());
    seed(&s, initial().encode().unwrap(), "existing");
    let mut candidates = vec![];
    let c = initial();
    let mut x = c.clone();
    x.revision = 2;
    candidates.push(x);
    let mut x = c.clone();
    x.policy.revision = 2;
    candidates.push(x);
    let mut x = c.clone();
    x.admin_seq = 1;
    x.admin_head = Some(id(5));
    candidates.push(x);
    let mut x = c.clone();
    x.controller.epoch = 1;
    candidates.push(x);
    let mut x = c.clone();
    x.scheduler.failures = 1;
    candidates.push(x);
    let mut x = c.clone();
    x.scheduler.cooldown_until_ms = 1;
    candidates.push(x);
    let mut x = c.clone();
    x.scheduler.backoff_until_ms = 1;
    candidates.push(x);
    let mut x = c.clone();
    x.scheduler.low_since_ms = Some(0);
    candidates.push(x);
    let mut x = c.clone();
    x.policy.mode = PolicyMode::Autoscale;
    x.policy.min_nodes = 1;
    x.policy.desired = 2;
    x.policy.max_nodes = 3;
    candidates.push(x);
    let mut x = c.clone();
    x.name = "invalid-name".into();
    candidates.push(x);
    for candidate in candidates {
        assert_eq!(
            s.initialize_control(&candidate, deadline())
                .await
                .unwrap_err(),
            StorageError::InvalidCatalog
        );
    }
    assert_eq!(counts(&s), (0, 0));
    for desired in [0, 1024] {
        let s = store(Backend::default());
        let mut x = c.clone();
        x.policy.desired = desired;
        x.policy.min_nodes = desired;
        x.policy.max_nodes = desired;
        assert_eq!(
            s.initialize_control(&x, deadline()).await.unwrap().root(),
            &x
        );
    }
    let s = store(Backend::default());
    let mut x = c;
    x.policy.mode = PolicyMode::Autoscale;
    x.policy.desired = 1;
    x.policy.min_nodes = 1;
    x.policy.max_nodes = 1024;
    s.initialize_control(&x, deadline()).await.unwrap();
}
#[tokio::test]
async fn bounded_complete_original_reads_reject_corruption_and_empty_tokens() {
    for (bytes, token, error) in [
        (b"{}".to_vec(), "opaque", StorageError::InvalidCatalog),
        (initial().encode().unwrap(), "", StorageError::Unavailable),
        (
            root()
                .to_string()
                .replace("\"version\":1", "\"version\":1,\"version\":1")
                .into_bytes(),
            "opaque",
            StorageError::InvalidCatalog,
        ),
    ] {
        let s = store(Backend::default());
        seed(&s, bytes, token);
        assert_eq!(
            s.initialize_control(&initial(), deadline())
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(counts(&s), (1, 0));
    }
    for (mode, error) in [
        (Mode::Truncated, StorageError::Unavailable),
        (Mode::Oversize, StorageError::LimitExceeded),
    ] {
        let s = store(Backend {
            mode,
            ..Default::default()
        });
        assert_eq!(s.load_control(deadline()).await.unwrap_err(), error);
        assert_eq!(counts(&s), (1, 0));
    }
}
#[tokio::test]
async fn absolute_deadline_bounds_get_body_and_possibly_applied_create() {
    for mode in [
        Mode::HangGet,
        Mode::HangBody,
        Mode::ExpireMissing,
        Mode::HangPut,
    ] {
        let s = store(Backend {
            mode,
            ..Default::default()
        });
        let result = s
            .initialize_control(&initial(), Instant::now() + Duration::from_millis(10))
            .await;
        let submitted = matches!(mode, Mode::HangPut);
        assert_eq!(
            result.unwrap_err(),
            if submitted {
                StorageError::Ambiguous
            } else {
                StorageError::Unavailable
            }
        );
        assert_eq!(counts(&s), (1, usize::from(submitted)));
        if submitted {
            assert!(
                s.backend()
                    .fake
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .contains_key(KEY)
            );
        }
    }
    let s = store(Backend::default());
    assert_eq!(
        s.initialize_control(&initial(), Instant::now())
            .await
            .unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(counts(&s), (0, 0));
}

#[tokio::test]
async fn empty_ack_and_expired_reconciliation_never_authorize_another_create() {
    let s = store(Backend {
        mode: Mode::EmptyAckMissing,
        ..Default::default()
    });
    assert_eq!(
        s.initialize_control(&initial(), deadline())
            .await
            .unwrap_err(),
        StorageError::Ambiguous
    );
    assert_eq!(counts(&s), (2, 1));
    for error in [StorageError::Ambiguous, StorageError::Conflict] {
        let s = store(Backend {
            mode: Mode::HangReconcile,
            ..Default::default()
        });
        s.backend()
            .fake
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert(KEY.into(), (error, error == StorageError::Ambiguous));
        assert_eq!(
            s.initialize_control(&initial(), Instant::now() + Duration::from_millis(20))
                .await
                .unwrap_err(),
            if error == StorageError::Ambiguous {
                error
            } else {
                StorageError::Unavailable
            }
        );
        assert_eq!(counts(&s), (2, 1));
    }
}
#[tokio::test]
async fn retained_root_without_new_admission_room_still_loads_unchanged() {
    let mut v = root();
    let hash = v["config"]["template_fingerprint"].clone();
    for k in 0..1024u128 {
        let node = id(100 + k);
        let launch = id(10000 + k);
        v["nodes"].as_array_mut().unwrap().push(json!({"node_id":node,"launch_id":launch,"resource_id":format!("resource/{k}"),"ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}));
        v["intents"].as_array_mut().unwrap().push(json!({"request_id":launch,"node_id":node,"launch_id":launch,"incarnation_id":null,"resource_id":format!("resource/{k}"),"action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":hash,"delivery":null,"outcome":"resource_known"}));
    }
    for k in 0..1026u128 {
        let mut stop = v["intents"][0].clone();
        stop["request_id"] = json!(id(20000 + k));
        stop["action"] = json!("stop");
        stop["state"] = json!("pending");
        stop["outcome"] = Value::Null;
        stop["template_fingerprint"] = Value::Null;
        v["intents"].as_array_mut().unwrap().push(stop);
    }
    let c = Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap();
    assert_eq!(
        c.check_new_admission(),
        Err(nabedra_database_server::cluster::state::ControlError::LimitExceeded)
    );
    let s = store(Backend::default());
    let bytes = c.encode().unwrap();
    seed(&s, bytes.clone(), "retained-token");
    let observed = s.initialize_control(&initial(), deadline()).await.unwrap();
    assert_eq!(observed.root(), &c);
    assert_eq!(counts(&s), (1, 0));
    // Same root with a different backend token is a different loaded observation.
    seed(&s, bytes, "next-token");
    assert_ne!(s.load_control(deadline()).await.unwrap(), observed);
    let before = counts(&s);
    assert_eq!(
        s.initialize_control(&c, deadline()).await.unwrap_err(),
        StorageError::InvalidCatalog
    );
    assert_eq!(counts(&s), before);
}
