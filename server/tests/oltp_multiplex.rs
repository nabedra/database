#![cfg(feature = "oltp")]
//! Real mTLS routing/progress fixtures, independent of the authority/Raft tests.
#[path = "support/tls.rs"]
mod tls;
use futures_util::FutureExt;
use nabedra_database_server::{
    config::WorkerResources,
    oltp::pool::{ByteClass, SharedResourceConfig, WorkerSharedResources},
    oltp_replication::{
        multiplex::*,
        types::{
            Digest, GROUP_FORMAT_VERSION, GroupIdentity, GroupKind, Text, WorkerEnrollment,
            WorkerIdentity,
        },
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot},
    time::timeout,
};
use uuid::Uuid;

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-multiplex-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Directory {
    cluster: Uuid,
    workers: RwLock<BTreeMap<Uuid, WorkerEnrollment>>,
    recovery: AtomicBool,
}
impl PeerDirectory for Directory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        _: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        let enrolled = self.resolve(peer.worker_id)?;
        if group.cluster_id != self.cluster || WorkerIdentity::from(&enrolled) != peer {
            return Err(invalid("untrusted worker"));
        }
        Ok(if self.recovery.load(Ordering::Acquire) {
            TransportTrust::RecoveryControlRaft
        } else {
            TransportTrust::AppliedEnrollment
        })
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.workers
            .read()
            .unwrap()
            .get(&worker)
            .cloned()
            .ok_or_else(|| invalid("unknown worker"))
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn shared(id: u64) -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        id,
        WorkerResources {
            max_threads: 8,
            memory_limit_bytes: 512 << 20,
            task_memory_limit_bytes: 128 << 20,
            control_memory_reserve_bytes: 64 << 20,
        },
        SharedResourceConfig {
            background_threads: 2,
            max_resident_groups: 2,
            ..Default::default()
        },
    )
    .unwrap()
}
fn group(cluster: Uuid, kind: GroupKind) -> GroupIdentity {
    GroupIdentity {
        cluster_id: cluster,
        database: Text::try_from("fixture").unwrap(),
        group_id: Uuid::new_v4(),
        kind,
        group_format_version: GROUP_FORMAT_VERSION,
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct Echo {
    group: Uuid,
    nonce: Uuid,
    worker: Uuid,
}
struct EchoEndpoint {
    shared: Arc<WorkerSharedResources>,
    entered: Semaphore,
    release: Semaphore,
    calls: AtomicUsize,
}
impl Endpoint for EchoEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let class = match session.purpose() {
                Purpose::Authority => TrafficClass::Authority,
                Purpose::QueueControl if header == b"state" => TrafficClass::QueueControl,
                Purpose::QueueControl => return Err(invalid("data method on control lane")),
                Purpose::QueueData => {
                    if header == b"blocked" {
                        self.entered.add_permits(1);
                        let permit = self.release.acquire().await.unwrap();
                        permit.forget();
                    }
                    TrafficClass::QueueData
                }
                Purpose::Raft => TrafficClass::ControlConsensus,
                _ => return Err(invalid("unsupported fixture purpose")),
            };
            Ok(ReceivePlan {
                class,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        _: nabedra_database_server::oltp::pool::ChargedBytes,
        _: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let class = if session.purpose() == Purpose::QueueData {
                ByteClass::Data
            } else {
                ByteClass::Progress
            };
            Ok(ChargedReply {
                header: self.shared.encode_in(
                    class,
                    &Echo {
                        group: session.group().group_id,
                        nonce: session.nonce(),
                        worker: session.remote().worker_id,
                    },
                    1024,
                )?,
                sections: std::array::from_fn(|_| None),
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c07_m02_o06_one_tls_listener_routes_groups_and_preserves_control_during_blocked_data_preflight()
 {
    let directory = Temporary::new();
    let certificates = tls::certificates(&directory.0, 2);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster = Uuid::new_v4();
    let enrollments: Vec<_> = certificates
        .iter()
        .enumerate()
        .map(|(index, tls)| WorkerEnrollment {
            worker_id: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            certificate_sha256: Digest::parse(tls.certificate_sha256()).unwrap(),
            endpoint: Text::try_from(
                if index == 1 {
                    listener.local_addr().unwrap().to_string()
                } else {
                    "127.0.0.1:1".into()
                }
                .as_str(),
            )
            .unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            enrollment_revision: 1,
        })
        .collect();
    let peers = Arc::new(Directory {
        cluster,
        workers: RwLock::new(
            enrollments
                .iter()
                .map(|worker| (worker.worker_id, worker.clone()))
                .collect(),
        ),
        recovery: AtomicBool::new(false),
    });
    let a = shared(1);
    let b = shared(2);
    let a_registry = Arc::new(EndpointRegistry::new(a.clone()));
    let registry = Arc::new(EndpointRegistry::new(b.clone()));
    let control = group(cluster, GroupKind::Control);
    let timestamps = group(cluster, GroupKind::Timestamps);
    let endpoint = Arc::new(EchoEndpoint {
        shared: b.clone(),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let resident = registry.reserve(control.clone()).unwrap();
    for purpose in [
        Purpose::QueueControl,
        Purpose::QueueData,
        Purpose::Authority,
        Purpose::Raft,
    ] {
        resident.bind(purpose, endpoint.clone()).unwrap();
    }
    registry
        .reserve(timestamps.clone())
        .unwrap()
        .bind(Purpose::Authority, endpoint.clone())
        .unwrap();
    let additional = group(cluster, GroupKind::Status { bucket: 1000 });
    assert_eq!(
        registry.reserve(additional.clone()).err().unwrap().code,
        ErrorCode::Overloaded
    );
    assert!(
        a_registry.reserve(additional).is_ok(),
        "capacity is per worker, not a logical group ceiling"
    );
    let server = Arc::new(
        WorkerTransport::new(
            &enrollments[1],
            &certificates[1],
            b.clone(),
            peers.clone(),
            registry,
            TransportConfig::default(),
        )
        .unwrap(),
    );
    let source = WorkerTransport::new(
        &enrollments[0],
        &certificates[0],
        a.clone(),
        peers.clone(),
        a_registry,
        TransportConfig::default(),
    )
    .unwrap();
    let client = source.client();
    let target = enrollments[1].worker_id;
    let (stop, shutdown) = oneshot::channel();
    let mut server_task = tokio::spawn(server.run(listener, async {
        let _ = shutdown.await;
    }));
    let mut pending = None;
    let result = AssertUnwindSafe(timeout(Duration::from_secs(30), async {
        for identity in [&control, &timestamps] {
            let reply = client
                .request(
                    identity,
                    target,
                    Purpose::Authority,
                    a.copy_bytes_in(ByteClass::Progress, b"authority").unwrap(),
                    std::array::from_fn(|_| None),
                    16384,
                )
                .await
                .unwrap();
            let decoded = reply.decode_header::<Echo>().unwrap();
            assert_eq!(decoded.group, identity.group_id);
            assert_eq!(decoded.nonce, reply.nonce());
            assert_eq!(
                reply.remote_identity(),
                WorkerIdentity::from(&enrollments[1])
            );
            assert_eq!(decoded.worker, enrollments[0].worker_id);
        }
        let mut wrong = control.clone();
        wrong.database = Text::try_from("foreign").unwrap();
        assert!(
            client
                .request(
                    &wrong,
                    target,
                    Purpose::Authority,
                    a.copy_bytes_in(ByteClass::Progress, b"authority").unwrap(),
                    std::array::from_fn(|_| None),
                    4096
                )
                .await
                .is_err()
        );
        let before = endpoint.calls.load(Ordering::Acquire);
        let error = client
            .request(
                &control,
                target,
                Purpose::QueueControl,
                a.copy_bytes_in(ByteClass::Progress, b"install").unwrap(),
                std::array::from_fn(|_| None),
                16384,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, ErrorCode::ValidationError);
        assert_eq!(endpoint.calls.load(Ordering::Acquire), before);
        peers.recovery.store(true, Ordering::Release);
        assert!(
            client
                .request(
                    &control,
                    target,
                    Purpose::QueueControl,
                    a.copy_bytes_in(ByteClass::Progress, b"state").unwrap(),
                    std::array::from_fn(|_| None),
                    4096
                )
                .await
                .is_err()
        );
        let recovery = client
            .request(
                &control,
                target,
                Purpose::Raft,
                a.copy_bytes_in(ByteClass::Progress, b"raft").unwrap(),
                std::array::from_fn(|_| None),
                16384,
            )
            .await
            .unwrap();
        drop(recovery);
        peers.recovery.store(false, Ordering::Release);
        let blocked_client = client.clone();
        let blocked_group = control.clone();
        let source = a.clone();
        pending = Some(tokio::spawn(async move {
            blocked_client
                .request(
                    &blocked_group,
                    target,
                    Purpose::QueueData,
                    source.copy_bytes_in(ByteClass::Data, b"blocked").unwrap(),
                    std::array::from_fn(|_| None),
                    16384,
                )
                .await
        }));
        let entered = endpoint.entered.acquire().await.unwrap();
        entered.forget();
        let config = b.config();
        let remaining =
            config.control_bytes - config.progress_bytes - b.buffer_bytes_used(ByteClass::Data);
        let filled = b.allocate_bytes(ByteClass::Data, remaining).unwrap();
        let state = client
            .request(
                &control,
                target,
                Purpose::QueueControl,
                a.copy_bytes_in(ByteClass::Progress, b"state").unwrap(),
                std::array::from_fn(|_| None),
                16384,
            )
            .await
            .unwrap();
        assert_eq!(
            state.decode_header::<Echo>().unwrap().group,
            control.group_id
        );
        assert!(!pending.as_ref().unwrap().is_finished());
        assert_eq!(
            b.scheduler().usage()[0].1,
            0,
            "preflight/network waits hold no compute"
        );
        drop(state);
        drop(filled);
        endpoint.release.add_permits(1);
        let reply = pending.as_mut().unwrap().await.unwrap().unwrap();
        drop(reply);
        pending = None;
    }))
    .catch_unwind()
    .await;
    if let Some(task) = pending {
        task.abort();
        let _ = task.await;
    }
    let _ = stop.send(());
    match timeout(Duration::from_secs(35), &mut server_task).await {
        Ok(joined) => joined.unwrap().unwrap(),
        Err(_) => {
            server_task.abort();
            let _ = server_task.await;
            panic!("worker listener shutdown watchdog")
        }
    }
    assert_eq!(a.buffer_bytes_used(ByteClass::Data), 0);
    assert_eq!(b.buffer_bytes_used(ByteClass::Data), 0);
    assert_eq!(a.buffer_bytes_used(ByteClass::Progress), 0);
    assert_eq!(b.buffer_bytes_used(ByteClass::Progress), 0);
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("multiplex fixture watchdog"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
