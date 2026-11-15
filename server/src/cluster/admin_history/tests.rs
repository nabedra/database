use super::*;
use crate::cluster::store::LoadedControl;
use crate::cluster::{
    configuration::{Template, Timing},
    state::Control,
};
use crate::storage::{
    ControlDecision, MAX_CONTROL_BODY_BYTES, MAX_CONTROL_DECISION_BYTES, ObjectKey, ObjectStore,
    Storage, StorageError,
};
use crate::storage::{Download, Namespace, WriteCondition};
use crate::test_storage as support;
use serde_json::{Value, json};
use support::Fake;
use tokio::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn root() -> Value {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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

fn decision(seq: u64, base: u64) -> ControlDecision {
    let request_id = id(10000 + u128::from(seq));
    ControlDecision {
        version: 1,
        id: id(100 + u128::from(seq)),
        parent: (seq > 1).then(|| id(99 + u128::from(seq))),
        admin_seq: seq,
        cluster_id: id(1),
        base_root_revision: base,
        published_root_revision: base + 1,
        request_id,
        request_body: format!(
            r#"{{"request_id":"{request_id}","operation":"cluster.scale","payload":{{"nodes":0}}}}"#
        ),
        response_status: 200,
        response_body: format!(
            r#"{{"request_id":"{request_id}","data":{{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0}}}}"#
        ),
    }
}
fn insert(fake: &Fake, record: &ControlDecision) {
    fake.state.lock().unwrap().objects.insert(
        format!("test/{}", ObjectKey::control_decision(record.id).as_str()),
        (record.encode().unwrap(), "decision".into()),
    );
}
async fn fixture(
    root: Control,
    records: &[ControlDecision],
) -> (Storage<Fake>, LoadedControl, AdminHistoryBudget) {
    let fake = Fake::default();
    fake.state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (root.encode().unwrap(), "private-token".into()));
    for record in records {
        insert(&fake, record);
    }
    let storage = Storage::new(fake, Namespace::new("test/").unwrap());
    let budget = AdminHistoryBudget::new(deadline()).unwrap();
    let loaded = storage.load_control(budget.deadline()).await.unwrap();
    (storage, loaded, budget)
}
fn headed(seq: u64, revision: u64) -> Control {
    let mut root = initial();
    root.admin_seq = seq;
    root.admin_head = Some(id(100 + u128::from(seq)));
    root.revision = revision;
    root
}
#[tokio::test]
async fn replay_interleaved_revisions_exact_bytes_and_prefix() {
    let mut old = decision(1, 2);
    old.request_body = old
        .request_body
        .replace("cluster.scale", r"cluster.\u0073cale");
    old.response_status = 400;
    old.response_body = format!(
        r#" {{"request_id":"{}","error":{{"code":"validation_error","message":"original π","outcome":"not_applied"}}}} "#,
        old.request_id
    );
    let head = decision(2, 9);
    let mut root = headed(2, u64::MAX);
    root.policy.revision = u64::MAX;
    root.policy.desired = 3;
    root.policy.min_nodes = 3;
    root.policy.max_nodes = 3;
    let (s, l, b) = fixture(root, &[old.clone(), head.clone()]).await;
    assert_eq!(
        s.lookup_admin(&l, old.request_id, old.request_body.as_bytes(), b)
            .await,
        Ok(AdminLookup::Found(old.clone()))
    );
    assert_eq!(
        s.lookup_admin(
            &l,
            old.request_id,
            format!("{} ", old.request_body).as_bytes(),
            b
        )
        .await,
        Ok(AdminLookup::IdentityConflict)
    );
    assert_eq!(
        s.lookup_admin(&l, id(999), b"{}", b).await,
        Ok(AdminLookup::Absent)
    );
    assert_eq!(
        s.lookup_admin(
            &l,
            old.request_id,
            old.request_body
                .replace("nodes\":0", "nodes\":1")
                .as_bytes(),
            b
        )
        .await,
        Ok(AdminLookup::IdentityConflict)
    );
    s.backend().state.lock().unwrap().objects.remove(&format!(
        "test/{}",
        ObjectKey::control_decision(old.id).as_str()
    ));
    assert_eq!(
        s.lookup_admin(&l, head.request_id, head.request_body.as_bytes(), b)
            .await,
        Ok(AdminLookup::Found(head))
    );
    assert_eq!(s.backend().state.lock().unwrap().gets, 10);
    let debug = format!("{:?}", AdminLookup::Found(old.clone()));
    assert!(!debug.contains("original"));
    assert!(!debug.contains("request_body"));
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
}
#[tokio::test]
async fn zero_and_admission_deadline_before_io() {
    let (s, l, b) = fixture(initial(), &[]).await;
    assert_eq!(
        s.lookup_admin(&l, id(9), b"{}", b).await,
        Ok(AdminLookup::Absent)
    );
    assert_eq!(
        s.lookup_admin(&l, id(9), &vec![0; MAX_CONTROL_BODY_BYTES + 1], b)
            .await,
        Err(StorageError::LimitExceeded)
    );
    assert!(matches!(
        AdminHistoryBudget::new(Instant::now()),
        Err(StorageError::Unavailable)
    ));
    let expired = AdminHistoryBudget {
        deadline: Instant::now(),
    };
    assert_eq!(
        s.lookup_admin(&l, id(9), b"{}", expired).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(s.backend().state.lock().unwrap().gets, 1);
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    let copy = b;
    assert_eq!(copy.deadline(), b.deadline());
    let before = Instant::now();
    let capped = AdminHistoryBudget::new(before + Duration::from_secs(60)).unwrap();
    assert!(capped.deadline() >= before + Duration::from_secs(5));
    assert!(capped.deadline() <= Instant::now() + Duration::from_secs(5));
    assert!(b.deadline() <= deadline());
}
#[tokio::test]
async fn independent_invalid_links_and_missing_records() {
    for fault in 0..16 {
        let mut old = decision(1, 2);
        let mut head = decision(2, 9);
        let mut root = headed(2, 12);
        match fault {
            0 => head.cluster_id = id(8),
            1 => head.id = id(8),
            2 => {
                head.admin_seq = 3;
            }
            3 => root.revision = 9,
            4 => {
                old.base_root_revision = 10;
                old.published_root_revision = 11;
            }
            5 => {
                old.admin_seq = 2;
                old.parent = Some(id(999));
            }
            _ => (),
        }
        let (s, l, b) = fixture(root, &[]).await;
        insert(s.backend(), &old);
        let key = format!("test/{}", ObjectKey::control_decision(id(102)).as_str());
        let mut bytes = head.encode().unwrap();
        if fault == 6 {
            bytes.truncate(bytes.len() / 2);
        }
        if fault == 7 {
            bytes = b"{}".to_vec();
        }
        if fault == 10 {
            s.backend().state.lock().unwrap().objects.remove(&format!(
                "test/{}",
                ObjectKey::control_decision(old.id).as_str()
            ));
        }
        if fault >= 11 {
            let mut value = serde_json::to_value(&head).unwrap();
            match fault {
                11 => value["parent"] = json!(head.id),
                12 => value["request_id"] = json!(id(999)),
                13 => value["parent"] = Value::Null,
                14 => {
                    value["base_root_revision"] = json!(0);
                    value["published_root_revision"] = json!(1);
                }
                15 => {
                    value["base_root_revision"] = json!(u64::MAX);
                    value["published_root_revision"] = json!(0);
                }
                _ => unreachable!(),
            }
            bytes = serde_json::to_vec(&value).unwrap();
        }
        if fault != 8 {
            s.backend()
                .state
                .lock()
                .unwrap()
                .objects
                .insert(key, (bytes, "x".into()));
        }
        if fault == 9 {
            s.backend().state.lock().unwrap().read_failure = true;
        }
        let result = s.lookup_admin(&l, id(9999), b"{}", b).await;
        assert_eq!(
            result,
            Err(if fault == 9 {
                StorageError::Unavailable
            } else {
                StorageError::InvalidCatalog
            }),
            "fault {fault}"
        );
        assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    }
}
#[tokio::test]
async fn hard_record_limit_stops_before_next_get() {
    let (s, l, b) = fixture(headed(4097, 9000), &[]).await;
    for seq in 1..=4097 {
        insert(s.backend(), &decision(seq, seq * 2));
    }
    assert_eq!(
        s.lookup_admin(&l, id(99999), b"{}", b).await,
        Err(StorageError::LimitExceeded)
    );
    assert_eq!(s.backend().state.lock().unwrap().gets, 4097);
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
}
#[tokio::test(start_paused = true)]
async fn hard_byte_limit_is_not_deadline_exhaustion() {
    let (s, l, b) = fixture(headed(9, 30), &[]).await;
    for seq in 1..=9 {
        let d = decision(seq, seq * 2);
        let mut bytes = d.encode().unwrap();
        bytes.resize(MAX_CONTROL_DECISION_BYTES, b' ');
        s.backend().state.lock().unwrap().objects.insert(
            format!("test/{}", ObjectKey::control_decision(d.id).as_str()),
            (bytes, "x".into()),
        );
    }
    assert_eq!(
        s.lookup_admin(&l, id(99999), b"{}", b).await,
        Err(StorageError::LimitExceeded)
    );
    assert!(Instant::now() < b.deadline());
    assert_eq!(s.backend().state.lock().unwrap().gets, 9);
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
}

struct FaultBackend {
    fake: Fake,
    mode: u8,
}
impl ObjectStore for FaultBackend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let download = self.fake.get(key).await?;
        if key == KEY {
            return Ok(download);
        }
        if self.mode == 0 {
            return std::future::pending().await;
        }
        let prefix = decision(1, 2).encode().unwrap();
        Ok(Download::new(
            std::io::Cursor::new(prefix).chain(FailingReader {
                pending: self.mode == 2,
            }),
            "x".into(),
        ))
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.fake.put(key, bytes, condition).await
    }
}
struct FailingReader {
    pending: bool,
}
impl tokio::io::AsyncRead for FailingReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(Err(std::io::Error::other("truncated")))
        }
    }
}
use tokio::io::AsyncReadExt;
#[tokio::test]
async fn stalled_get_and_partial_body_are_errors() {
    for mode in 0..3 {
        let fake = Fake::default();
        let d = decision(1, 2);
        insert(&fake, &d);
        fake.state
            .lock()
            .unwrap()
            .objects
            .insert(KEY.into(), (headed(1, 4).encode().unwrap(), "x".into()));
        let s = Storage::new(
            FaultBackend { fake, mode },
            Namespace::new("test/").unwrap(),
        );
        let b = AdminHistoryBudget::new(Instant::now() + Duration::from_millis(100)).unwrap();
        let l = s.load_control(b.deadline()).await.unwrap();
        assert_eq!(
            s.lookup_admin(&l, d.request_id, d.request_body.as_bytes(), b)
                .await,
            Err(StorageError::Unavailable)
        );
        assert_eq!(s.backend().fake.state.lock().unwrap().gets, 2);
        assert_eq!(s.backend().fake.state.lock().unwrap().puts, 0);
    }
}
