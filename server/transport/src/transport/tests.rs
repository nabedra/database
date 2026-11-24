use super::*;
use nabedra_oltp_types::types::{Text, WorkerEnrollment};
use nabedra_resources::{
    config::WorkerResources, memory::SharedResourceConfig, scheduling::ResourceScheduler,
};
use std::{future::ready, path::PathBuf, pin::Pin, sync::mpsc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc as async_mpsc, oneshot};

mod sessions;
mod tls;

pub(super) struct TestMemory(Arc<WorkerMemory>);

impl Deref for TestMemory {
    type Target = WorkerMemory;

    fn deref(&self) -> &WorkerMemory {
        &self.0
    }
}

impl ResourceOwner for TestMemory {
    fn worker_memory(&self) -> &Arc<WorkerMemory> {
        &self.0
    }

    fn bind_worker(self: &Arc<Self>, _: Uuid) -> Result<(), ProtocolError> {
        Ok(())
    }
}

pub(super) fn test_memory(node: u64, config: SharedResourceConfig) -> Arc<TestMemory> {
    let limits = WorkerResources::default();
    let scheduler = ResourceScheduler::new(vec![(node, limits)]).unwrap();
    let memory = Arc::new(
        WorkerMemory::new(
            config,
            scheduler,
            node,
            limits.task_memory_limit_bytes as usize,
            0,
        )
        .unwrap(),
    );
    let owner = Arc::new(TestMemory(memory));
    let erased: Arc<dyn std::any::Any + Send + Sync> = owner.clone();
    owner.bind_owner(Arc::downgrade(&erased));
    owner
}

const GONE: &str = "removed-worker.example:443";
const OTHER: &str = "other-worker.example:443";
const OFFLINE_PROGRESS: &str = "offline-progress.example:443";
const LIVE: &str = "surviving-worker.example:443";

struct GatedLookup {
    started: async_mpsc::UnboundedReceiver<String>,
    releases: BTreeMap<&'static str, mpsc::Sender<()>>,
}

impl GatedLookup {
    fn resolver(held: &[&'static str], address: SocketAddr) -> (PeerResolver, Self) {
        let (started, events) = async_mpsc::unbounded_channel();
        let (releases, gates): (BTreeMap<_, _>, BTreeMap<_, _>) = held
            .iter()
            .map(|&endpoint| {
                let (release, gate) = mpsc::channel();
                ((endpoint, release), (endpoint, gate))
            })
            .unzip();
        let gates = Mutex::new(gates);
        let resolver = PeerResolver::with_lookup(move |endpoint| {
            let gate = gates.lock().unwrap().remove(endpoint);
            let _ = started.send(endpoint.to_owned());
            if let Some(gate) = gate {
                let _ = gate.recv_timeout(Duration::from_secs(10));
                return Vec::new();
            }
            vec![address]
        });
        (
            resolver,
            Self {
                started: events,
                releases,
            },
        )
    }

    fn release(&mut self, endpoint: &str) {
        self.releases.remove(endpoint).unwrap().send(()).unwrap();
    }

    async fn next(&mut self, endpoint: &str) {
        assert_eq!(self.started.recv().await.unwrap(), endpoint);
    }
}

async fn assert_pending<T>(waiter: Pin<&mut impl Future<Output = T>>) {
    tokio::select! {
        biased;
        _ = waiter => panic!("operation finished before release"),
        _ = ready(()) => {}
    }
}

fn identity() -> WorkerIdentity {
    WorkerIdentity {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([1; 32]),
        enrollment_revision: 1,
    }
}

#[test]
fn reply_header_checks_bounds_before_deserializing() {
    let worker = test_memory(1, SharedResourceConfig::default());
    let resources = MemoryHandle::new(worker);
    let local = identity();
    let group = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: "reply-bounds".try_into().unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: 1,
    };
    let reply = |bytes: &[u8], budget| ReceivedReply {
        reply: ChargedReply {
            header: resources.copy_bytes_in(ByteClass::Data, bytes).unwrap(),
            sections: [None, None, None],
        },
        scratch: Arc::new(resources.reserve_scratch(ByteClass::Data, budget).unwrap()),
        session: AuthenticatedSession::for_test(
            local,
            local,
            group.clone(),
            Purpose::Authority,
            nabedra_rt::ids::uuid_v4(),
        ),
        resources: resources.clone(),
    };
    for (bytes, budget, message) in [
        (
            vec![b'1'; MAX_HEADER_BYTES + 1],
            8192,
            "RPC reply header exceeds JSON bounds",
        ),
        (vec![b'['; 33], 8192, "RPC reply header exceeds JSON bounds"),
        (
            b"1".to_vec(),
            4096,
            "RPC reply decode reservation is too small",
        ),
        (b"invalid".to_vec(), 8192, "invalid RPC reply header"),
    ] {
        let error = reply(&bytes, budget)
            .decode_header::<u64>()
            .err()
            .expect("invalid reply header");
        assert_eq!(error.code, ErrorCode::ValidationError);
        assert_eq!(error.outcome, Outcome::NotApplied);
        assert_eq!(error.message, message);
    }
    assert_eq!(*reply(b"1", 4098).decode_header::<u64>().unwrap(), 1);
}

#[test]
fn o05_c07_sections_keep_decode_and_wire_charges_until_drop() {
    let owner = test_memory(1, SharedResourceConfig::default());
    let other = test_memory(2, SharedResourceConfig::default());
    let usage = || {
        (
            owner.buffer_bytes_used(ByteClass::Data),
            owner.working_decode_bytes(),
        )
    };
    let baseline = usage();
    let body = b"123";
    let budget = json_decode_budget(body).unwrap();
    let sections = |body| {
        ChargedSections::for_test(
            &owner,
            ByteClass::Data,
            [
                Some(owner.copy_bytes_in(ByteClass::Data, body).unwrap()),
                None,
                None,
            ],
            budget,
        )
        .unwrap()
    };

    let decoded = sections(body).decode::<u64>(0).unwrap();
    assert_eq!(*decoded, 123);
    assert_eq!(usage(), (baseline.0 + body.len(), baseline.1 + budget));
    drop(decoded);
    assert_eq!(usage(), baseline);

    assert!(sections(b"bad").decode::<u64>(0).is_err());
    assert_eq!(usage(), baseline);

    assert!(sections(body).reserve_decode(&other, budget).is_err());
    assert_eq!(usage(), baseline);
}

#[test]
fn lane_refusal_keeps_the_server_rpc_trace_target() {
    struct EventTarget(mpsc::Sender<&'static str>);

    impl tracing::Subscriber for EventTarget {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            self.0.send(event.metadata().target()).unwrap();
        }
    }

    let (send, receive) = mpsc::channel();
    tracing::subscriber::with_default(EventTarget(send), || {
        LaneRefusals::default().outgoing(TrafficClass::ApiIngress, true);
    });
    assert_eq!(
        receive.try_recv().unwrap(),
        "nabedra_database_server::oltp_replication::multiplex::transport",
    );
}

#[test]
fn o05_m02_failed_replica_reservation_releases_unused_group_capacity() {
    let shared = test_memory(
        1,
        SharedResourceConfig {
            max_resident_groups: 2,
            precharge_group_capacity: true,
            ..Default::default()
        },
    );
    let registry = EndpointRegistry::new(shared.clone());
    let first = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("capacity").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Catalog,
        group_format_version: 1,
    };
    let second = GroupIdentity {
        group_id: nabedra_rt::ids::uuid_v4(),
        ..first.clone()
    };
    let (group, replica) = registry.reserve_replica(first.clone(), 1).unwrap();
    let (same_group, learner) = registry.reserve_replica(first, 2).unwrap();
    assert!(Arc::ptr_eq(&group.0, &same_group.0));
    let error = registry.reserve_replica(second.clone(), 1).err().unwrap();
    assert_eq!(error.code, ErrorCode::Overloaded);
    assert_eq!(error.outcome, Outcome::NotApplied);
    assert_eq!(
        error.message,
        "worker active Raft replica capacity exhausted"
    );
    assert_eq!(registry.groups.lock().unwrap().len(), 1);
    drop(learner);
    let replacement = registry.reserve_replica(second, 1).unwrap();
    assert_eq!(registry.groups.lock().unwrap().len(), 2);
    assert_eq!(shared.resident_group_usage(), (2, 2));
    drop((group, replica, same_group, replacement));
    registry.close_worker().unwrap();
    assert_eq!(shared.resident_group_usage(), (0, 2));
}

/// O14/O22/C07/F03. A canceled awaiter releases its RPC but the host lookup
/// retains ownership while native code is still blocked.
#[tokio::test(start_paused = true)]
async fn o14_o22_c07_f03_hostname_lookup_owns_slots_across_cancellations() {
    let address = "127.0.0.1:443".parse().unwrap();
    let (resolver, mut lookup) = GatedLookup::resolver(&[GONE, OTHER, OFFLINE_PROGRESS], address);
    let gone = Text::try_from(GONE).unwrap();
    let other = Text::try_from(OTHER).unwrap();
    let offline_progress = Text::try_from(OFFLINE_PROGRESS).unwrap();
    let live = Text::try_from(LIVE).unwrap();
    let changed_port = Text::try_from("REMOVED-WORKER.example.:444").unwrap();
    let first = identity();
    let second = identity();
    let bulk = TrafficClass::DataConsensus;
    let progress = TrafficClass::ControlConsensus;

    let mut waiting = Box::pin(resolver.resolve(first, &gone, bulk));
    assert_pending(waiting.as_mut()).await;
    lookup.next(GONE).await;
    drop(waiting);
    let mut waiting = Box::pin(resolver.resolve(second, &other, bulk));
    assert_pending(waiting.as_mut()).await;
    lookup.next(OTHER).await;
    drop(waiting);

    for class in [bulk, progress, TrafficClass::Settlement] {
        for _ in 0..16 {
            let mut waiter = Box::pin(resolver.resolve(first, &gone, class));
            assert_pending(waiter.as_mut()).await;
        }
    }
    assert_eq!(resolver.active_for_test(), [2, 0]);
    assert!(lookup.started.try_recv().is_err());
    assert!(matches!(
        resolver.resolve(second, &live, bulk).await,
        Err(ResolveError::LocallyUnavailable)
    ));
    let replacement = WorkerIdentity {
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 2,
        ..first
    };
    assert!(matches!(
        resolver.resolve(replacement, &changed_port, progress).await,
        Err(ResolveError::LocallyUnavailable)
    ));
    let mut waiter = Box::pin(resolver.resolve(second, &offline_progress, progress));
    assert_pending(waiter.as_mut()).await;
    lookup.next(OFFLINE_PROGRESS).await;
    drop(waiter);
    assert!(matches!(
        resolver
            .resolve(replacement, &offline_progress, progress)
            .await,
        Err(ResolveError::LocallyUnavailable)
    ));
    assert_eq!(resolver.active_for_test(), [2, 1]);
    assert_eq!(
        resolver
            .resolve(replacement, &live, progress)
            .await
            .unwrap()
            .as_ref(),
        &[address]
    );
    lookup.next(LIVE).await;

    let mut waiter = Box::pin(resolver.resolve(first, &gone, progress));
    assert_pending(waiter.as_mut()).await;
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_pending(waiter.as_mut()).await;
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(matches!(
        waiter.await,
        Err(ResolveError::LocallyUnavailable)
    ));
    assert_eq!(resolver.active_for_test(), [2, 1]);

    lookup.release(GONE);
    assert!(matches!(
        resolver.resolve(first, &gone, progress).await,
        Err(ResolveError::NoAddresses)
    ));
    assert_eq!(resolver.active_for_test(), [1, 1]);
    assert!(matches!(
        resolver.resolve(first, &gone, bulk).await,
        Err(ResolveError::NoAddresses)
    ));
    assert!(lookup.started.try_recv().is_err());
    assert_eq!(
        resolver
            .resolve(replacement, &gone, progress)
            .await
            .unwrap()
            .as_ref(),
        &[address]
    );
    lookup.next(GONE).await;
    tokio::time::advance(Duration::from_millis(251)).await;
    assert_eq!(
        resolver
            .resolve(first, &gone, progress)
            .await
            .unwrap()
            .as_ref(),
        &[address]
    );
    lookup.next(GONE).await;
    assert_eq!(
        resolver.resolve(first, &gone, bulk).await.unwrap().as_ref(),
        &[address]
    );
    assert!(lookup.started.try_recv().is_err());
    tokio::time::advance(Duration::from_millis(1001)).await;
    resolver.resolve(first, &gone, progress).await.unwrap();
    lookup.next(GONE).await;

    lookup.release(OTHER);
    assert!(matches!(
        resolver.resolve(second, &other, bulk).await,
        Err(ResolveError::NoAddresses)
    ));
    lookup.release(OFFLINE_PROGRESS);
    assert!(matches!(
        resolver.resolve(second, &offline_progress, progress).await,
        Err(ResolveError::NoAddresses)
    ));
    assert_eq!(resolver.active_for_test(), [0, 0]);
}

struct TestDirectory {
    workers: Mutex<BTreeMap<Uuid, WorkerEnrollment>>,
    apis: Mutex<Vec<WorkerIdentity>>,
    revoked: Mutex<Vec<WorkerIdentity>>,
    changes: tokio::sync::watch::Sender<u64>,
}

impl Default for TestDirectory {
    fn default() -> Self {
        Self {
            workers: Mutex::default(),
            apis: Mutex::default(),
            revoked: Mutex::default(),
            changes: tokio::sync::watch::channel(0).0,
        }
    }
}

impl TestDirectory {
    fn install(&self, item: WorkerEnrollment) {
        self.workers.lock().unwrap().insert(item.worker_id, item);
    }
    fn install_api(&self, item: WorkerEnrollment) {
        self.apis.lock().unwrap().push(WorkerIdentity::from(&item));
        self.install(item);
    }
    fn revoke(&self, peer: WorkerIdentity) {
        self.revoked.lock().unwrap().push(peer);
        self.changes.send_modify(|revision| *revision += 1);
    }
}

impl PeerDirectory for TestDirectory {
    fn is_api(&self, peer: WorkerIdentity) -> bool {
        self.apis.lock().unwrap().contains(&peer)
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        if self
            .revoked
            .lock()
            .unwrap()
            .iter()
            .any(|peer| peer.worker_id == worker)
        {
            return Err(unavailable());
        }
        self.workers
            .lock()
            .unwrap()
            .get(&worker)
            .cloned()
            .ok_or_else(unavailable)
    }

    fn verify(
        &self,
        peer: WorkerIdentity,
        _group: &GroupIdentity,
        _purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if self.revoked(peer) {
            return Err(revoked_peer());
        }
        if WorkerIdentity::from(&self.resolve(peer.worker_id)?) != peer {
            return Err(malformed("worker differs from test enrollment"));
        }
        Ok(TransportTrust::AppliedEnrollment)
    }

    fn revoked(&self, peer: WorkerIdentity) -> bool {
        self.revoked.lock().unwrap().contains(&peer)
    }

    fn trust_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changes.subscribe())
    }
}

#[derive(Default)]
struct Echo(std::sync::atomic::AtomicUsize);

impl Endpoint for Echo {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        _header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            Ok(ReceivePlan {
                class: traffic(session.group(), session.purpose())?,
                section_lengths: [0; MAX_SECTIONS],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        _session: AuthenticatedSession,
        header: ChargedBytes,
        _sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(ChargedReply {
                header,
                sections: [None, None, None],
            })
        })
    }
}

struct TestDirectoryPath(PathBuf);

impl Drop for TestDirectoryPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct RejectProbe;

impl Endpoint for RejectProbe {
    fn preflight<'a>(
        &'a self,
        _: &'a AuthenticatedSession,
        _: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async { Err(unavailable()) })
    }

    fn handle<'a>(
        &'a self,
        _: AuthenticatedSession,
        _: ChargedBytes,
        _: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        panic!("probe should stop at preflight")
    }
}

/// R05/F03. Probes can observe a live worker again when native lookup slots
/// settle. Local resolver contention must not contribute to its miss streak.
#[tokio::test]
async fn r05_f03_queue_probe_resolver_contention_is_indeterminate() {
    let temp = TestDirectoryPath(
        std::env::temp_dir().join(format!("probe-dns-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(&temp.0).unwrap();
    let tls = tls::certificates(&temp.0, 2);
    let group = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("probe").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: 1,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (resolver, mut lookup) =
        GatedLookup::resolver(&[GONE, OTHER], listener.local_addr().unwrap());
    let enrollment = |index: usize, endpoint: &str| WorkerEnrollment {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
        endpoint: Text::try_from(endpoint).unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
    };
    let caller = enrollment(0, "127.0.0.1:1");
    let live = enrollment(1, LIVE);
    let first = enrollment(1, GONE);
    let second = enrollment(1, OTHER);
    let alias = enrollment(1, "REMOVED-WORKER.example.:444");
    let peers = Arc::new(TestDirectory::default());
    for worker in [&caller, &live, &first, &second, &alias] {
        peers.install(worker.clone());
    }

    let transport = |local: &WorkerEnrollment, node, index: usize| {
        let shared = test_memory(node, SharedResourceConfig::default());
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        WorkerTransport::new(
            local,
            &tls[index],
            shared,
            peers.clone(),
            registry,
            TransportConfig::default(),
        )
        .unwrap()
    };
    let receiver = Arc::new(transport(&live, 2, 1));
    receiver
        .registry
        .reserve(group.clone())
        .unwrap()
        .bind(Purpose::QueueControl, Arc::new(RejectProbe))
        .unwrap();
    let mut caller = transport(&caller, 1, 0);
    Arc::get_mut(&mut caller.common).unwrap().resolver = resolver;
    let client = caller.client();
    let probe_header = || {
        caller
            .common
            .shared
            .encode_in(ByteClass::Progress, &"ping", 1024)
            .unwrap()
    };
    let probe = |peer: &WorkerEnrollment| {
        client.request_queue_probe(&group, WorkerIdentity::from(peer), probe_header(), 4096)
    };
    let (stop, shutdown) = oneshot::channel();
    let worker = nabedra_rt::task::spawn(receiver.run(listener, async {
        let _ = shutdown.await;
    }));

    timeout(Duration::from_secs(5), async {
        for (peer, endpoint) in [(&first, GONE), (&second, OTHER)] {
            let mut waiter = Box::pin(probe(peer));
            assert_pending(waiter.as_mut()).await;
            lookup.next(endpoint).await;
        }
        assert_eq!(caller.common.resolver.active_for_test(), [0, 2]);
        for _ in 0..3 {
            assert!(matches!(probe(&live).await, Err(RequestError::Protocol(error)) if error.code == ErrorCode::StorageUnavailable));
        }
        let header = probe_header();
        assert!(matches!(
            caller
                .client()
                .request_classified(
                    &group,
                    WorkerIdentity::from(&live),
                    Purpose::QueueControl,
                    header,
                    [None, None, None],
                    0,
                )
                .await,
            Err(RequestError::Unreachable)
        ));
        assert!(lookup.started.try_recv().is_err());

        lookup.release(OTHER);
        assert!(matches!(probe(&second).await, Err(RequestError::Unreachable)));
        assert_eq!(caller.common.resolver.active_for_test(), [0, 1]);
        assert!(matches!(probe(&alias).await, Err(RequestError::Protocol(error)) if error.code == ErrorCode::StorageUnavailable));
        assert!(lookup.started.try_recv().is_err());

        let mut waiter = Box::pin(probe(&first));
        assert_pending(waiter.as_mut()).await;
        assert!(matches!(waiter.await, Err(RequestError::Protocol(error)) if error.code == ErrorCode::StorageUnavailable));
        assert_eq!(caller.common.resolver.active_for_test(), [0, 1]);
        lookup.release(GONE);
        assert!(matches!(probe(&first).await, Err(RequestError::Unreachable)));
        assert_eq!(caller.common.resolver.active_for_test(), [0, 0]);

        assert!(matches!(
            probe(&live).await,
            Err(RequestError::PeerRejected { worker, .. }) if worker == WorkerIdentity::from(&live)
        ));
        lookup.next(LIVE).await;
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    worker.await.unwrap().unwrap();
    assert!(matches!(probe(&live).await, Err(RequestError::Unreachable)));
}

/// O14/O22/C07/F03. Numeric and hostname targets use real authenticated
/// transport while both ordinary DNS slots remain occupied.
#[tokio::test]
async fn o14_o22_c07_f03_mtls_progress_survives_held_hostname_lookups() {
    let temp = TestDirectoryPath(
        std::env::temp_dir().join(format!("peer-dns-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(&temp.0).unwrap();
    let tls = tls::certificates(&temp.0, 2);
    let group = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("progress").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Catalog,
        group_format_version: 1,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let enrollment = |index: usize, endpoint: Text<255>| WorkerEnrollment {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
        endpoint,
        server_name: Text::try_from("localhost").unwrap(),
    };
    let caller = enrollment(0, Text::try_from("127.0.0.1:1").unwrap());
    let mut target = enrollment(1, Text::new(address.to_string()).unwrap());
    let gone = enrollment(1, Text::try_from(GONE).unwrap());
    let other = enrollment(1, Text::try_from(OTHER).unwrap());
    let peers = Arc::new(TestDirectory::default());
    for worker in [&caller, &target, &gone, &other] {
        peers.install(worker.clone());
    }

    let transport = |local: &WorkerEnrollment, node, index: usize| {
        let shared = test_memory(node, SharedResourceConfig::default());
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        WorkerTransport::new(
            local,
            &tls[index],
            shared,
            peers.clone(),
            registry,
            TransportConfig::default(),
        )
        .unwrap()
    };
    let receiver = Arc::new(transport(&target, 2, 1));
    let echo = Arc::new(Echo::default());
    receiver
        .registry
        .reserve(group.clone())
        .unwrap()
        .bind(Purpose::RaftProgress, echo.clone())
        .unwrap();
    let (resolver, mut lookup) = GatedLookup::resolver(&[GONE, OTHER], address);
    let mut caller = transport(&caller, 1, 0);
    Arc::get_mut(&mut caller.common).unwrap().resolver = resolver;
    let client = caller.client();
    let (stop, shutdown) = oneshot::channel();
    let worker = nabedra_rt::task::spawn(receiver.run(listener, async {
        let _ = shutdown.await;
    }));

    timeout(Duration::from_secs(5), async {
        let request = |target, purpose, class: ByteClass, text: &str| {
            let header = caller.common.shared.encode_in(class, &text, 1024).unwrap();
            client.request_classified(&group, target, purpose, header, [None, None, None], 0)
        };
        for (peer, endpoint) in [(&gone, GONE), (&other, OTHER)] {
            let mut waiter = Box::pin(request(
                WorkerIdentity::from(peer),
                Purpose::Raft,
                ByteClass::Data,
                "unreachable",
            ));
            assert_pending(waiter.as_mut()).await;
            lookup.next(endpoint).await;
        }
        assert_eq!(caller.common.resolver.active_for_test(), [2, 0]);

        let progress = |peer: &WorkerEnrollment, text| {
            request(
                WorkerIdentity::from(peer),
                Purpose::RaftProgress,
                ByteClass::Progress,
                text,
            )
        };
        let reply = progress(&target, "numeric").await.unwrap();
        assert_eq!(reply.header.as_slice(), b"\"numeric\"");
        assert_eq!(reply.remote_identity(), WorkerIdentity::from(&target));
        assert!(lookup.started.try_recv().is_err());

        target.endpoint = Text::try_from(LIVE).unwrap();
        peers.install(target.clone());
        let reply = progress(&target, "hostname").await.unwrap();
        assert_eq!(reply.header.as_slice(), b"\"hostname\"");
        assert_eq!(reply.remote_identity(), WorkerIdentity::from(&target));
        lookup.next(LIVE).await;
        let reply = progress(&target, "cached").await.unwrap();
        assert_eq!(reply.header.as_slice(), b"\"cached\"");
        assert!(lookup.started.try_recv().is_err());

        target.enrollment_revision += 1;
        target.certificate_sha256 = caller.common.local.certificate_sha256;
        peers.install(target.clone());
        for _ in 0..2 {
            assert!(matches!(
                progress(&target, "wrong fingerprint").await,
                Err(RequestError::Protocol(error))
                    if error.message == "target certificate or ALPN differs"
            ));
        }
        lookup.next(LIVE).await;
        assert!(lookup.started.try_recv().is_err());
        assert_eq!(echo.0.load(Ordering::Relaxed), 3);
        assert_eq!(caller.common.resolver.active_for_test(), [2, 0]);

        for (peer, endpoint) in [(&gone, GONE), (&other, OTHER)] {
            lookup.release(endpoint);
            assert!(matches!(
                request(
                    WorkerIdentity::from(peer),
                    Purpose::Raft,
                    ByteClass::Data,
                    "settle"
                )
                .await,
                Err(RequestError::Unreachable)
            ));
        }
        assert_eq!(caller.common.resolver.active_for_test(), [0, 0]);
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    worker.await.unwrap().unwrap();
}

/// O14/O23/C07/R03. A role read waits before dispatch under Authority pressure,
/// then uses the first available slot without blocking other Authority reads.
#[tokio::test]
async fn o14_o23_c07_r03_authority_dependency_waits_for_progress() {
    let temp = TestDirectoryPath(
        std::env::temp_dir().join(format!("authority-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(&temp.0).unwrap();
    let tls = tls::certificates(&temp.0, 2);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let enrollment = |index: usize, endpoint: String| WorkerEnrollment {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
        endpoint: Text::new(endpoint).unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
    };
    let caller = enrollment(0, "127.0.0.1:1".to_owned());
    let target = enrollment(1, listener.local_addr().unwrap().to_string());
    let peers = Arc::new(TestDirectory::default());
    peers.install(caller.clone());
    peers.install(target.clone());
    let authority = TrafficClass::Authority as usize;
    let mut config = TransportConfig::default();
    config.handler_slots[authority] = 2;
    config.preflight_slots[authority] = 1;
    let transport = |local, node, index: usize| {
        let shared = test_memory(node, SharedResourceConfig::default());
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        WorkerTransport::new(
            local,
            &tls[index],
            shared,
            peers.clone(),
            registry,
            config.clone(),
        )
        .unwrap()
    };
    let group = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("authority").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: 1,
    };
    let receiver = Arc::new(transport(&target, 2, 1));
    let echo = Arc::new(Echo::default());
    receiver
        .registry
        .reserve(group.clone())
        .unwrap()
        .bind(Purpose::Authority, echo.clone())
        .unwrap();
    let caller = transport(&caller, 1, 0);
    let client = caller.client();
    let (stop, shutdown) = oneshot::channel();
    let worker = nabedra_rt::task::spawn(receiver.run(listener, async {
        let _ = shutdown.await;
    }));
    let used_bytes = || caller.common.shared.buffer_bytes_used(ByteClass::Progress);
    let baseline = used_bytes();
    let header = |bytes: &[u8]| {
        caller
            .common
            .shared
            .copy_bytes_in(ByteClass::Progress, bytes)
            .unwrap()
    };
    let dependency = |bytes: &[u8]| {
        client.request_authority_dependency(&group, target.worker_id, header(bytes), 0)
    };

    tokio::time::pause();
    let held = client.hold_outgoing_for_test(TrafficClass::Authority);
    let mut pending = Box::pin(dependency(b"cancelled"));
    assert_pending(pending.as_mut()).await;
    assert!(used_bytes() > baseline + b"cancelled".len());
    assert!(matches!(
        dependency(b"excess").await,
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    drop(pending);
    assert_eq!(used_bytes(), baseline);
    let mut pending = Box::pin(dependency(b"expired"));
    assert_pending(pending.as_mut()).await;
    tokio::time::advance(config.deadline).await;
    assert!(matches!(
        pending.await,
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    assert_eq!(caller.common.authority_waiters.available_permits(), 1);
    assert_eq!(echo.0.load(Ordering::Relaxed), 0);
    assert_eq!(used_bytes(), baseline);
    drop(held);
    tokio::time::resume();

    timeout(Duration::from_secs(5), async {
        let held = client
            .authority_slots(target.worker_id, nabedra_rt::time::now() + config.deadline)
            .await
            .unwrap();
        let mut pending = Box::pin(dependency(b"role"));
        assert_pending(pending.as_mut()).await;
        assert_eq!(caller.common.authority_waiters.available_permits(), 0);
        let ordinary = client
            .request(
                &group,
                target.worker_id,
                Purpose::Authority,
                header(b"current"),
                [None, None, None],
                0,
            )
            .await
            .unwrap();
        assert_eq!(ordinary.header.as_slice(), b"current");
        assert_eq!(echo.0.load(Ordering::Relaxed), 1);
        drop(held);
        let reply = pending.await.unwrap();
        assert_eq!(reply.header.as_slice(), b"role");
        assert_eq!(reply.remote_identity(), WorkerIdentity::from(&target));
        assert_eq!(echo.0.load(Ordering::Relaxed), 2);
        assert_eq!(caller.common.authority_waiters.available_permits(), 1);
        assert_eq!(
            caller
                .common
                .outgoing
                .lane(
                    target.worker_id,
                    TrafficClass::Authority,
                    &caller.common.config
                )
                .available_permits(),
            2
        );
        drop((ordinary, reply));
        assert_eq!(used_bytes(), baseline);
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    worker.await.unwrap().unwrap();
}

/// PERF1 (O03, D01): a direct request that finds its leader's lane full
/// waits for a slot, in arrival order, until its deadline, instead of being
/// refused at once. Waiting callers hold at most one slot less than the lane,
/// so a caller that refuses at once (a queued submission next to direct load,
/// D11-drop in run 44904328496) still finds one. A wait that times out is a
/// local, not-applied `OutgoingLaneFull`.
#[tokio::test]
async fn perf1_direct_waits_for_a_full_lane_within_its_deadline() {
    let temp = TestDirectoryPath(
        std::env::temp_dir().join(format!("lanes-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(&temp.0).unwrap();
    let tls = tls::certificates(&temp.0, 1);
    let local = WorkerEnrollment {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::parse(tls[0].certificate_sha256()).unwrap(),
        endpoint: Text::new("127.0.0.1:1".to_owned()).unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
    };
    let peers = Arc::new(TestDirectory::default());
    peers.install(local.clone());
    let config = TransportConfig::default();
    let shared = test_memory(1, SharedResourceConfig::default());
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let transport =
        WorkerTransport::new(&local, &tls[0], shared, peers, registry, config.clone()).unwrap();
    let client = transport.client();
    let group = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("direct").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Data {
            table_id: nabedra_rt::ids::uuid_v4(),
            shard_id: nabedra_rt::ids::uuid_v4(),
        },
        group_format_version: 1,
    };
    let class = TrafficClass::ApiIngress;
    let lane = config.handler_slots[class as usize];
    let leader = nabedra_rt::ids::uuid_v4();
    let reserve = |until| client.reserve_outgoing(&group, leader, Purpose::ApiIngress, until);
    // Waiting callers get `lane - 1` slots at once.
    let mut held = Vec::new();
    for _ in 0..lane - 1 {
        held.push(reserve(nabedra_rt::time::now()).await.unwrap());
    }
    // Their share is full: refused at the deadline, counted as a local refusal.
    let refused = transport.common.refusals.snapshot();
    let start = nabedra_rt::time::now();
    assert!(matches!(
        reserve(start + Duration::from_millis(50)).await,
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    assert!(nabedra_rt::time::elapsed(start) >= Duration::from_millis(50));
    assert_ne!(transport.common.refusals.snapshot(), refused);
    // The last slot is still there for a caller that refuses at once.
    let headroom = client.try_outgoing(leader, class).unwrap();
    assert!(matches!(
        client.try_outgoing(leader, class),
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    // A waiter gets the slot a finished request frees.
    let waiter = nabedra_rt::task::spawn({
        let client = client.clone();
        let group = group.clone();
        async move {
            client
                .reserve_outgoing(
                    &group,
                    leader,
                    Purpose::ApiIngress,
                    nabedra_rt::time::now() + Duration::from_secs(10),
                )
                .await
        }
    });
    nabedra_rt::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !waiter.is_finished(),
        "the waiter must wait while the lane is full"
    );
    held.pop();
    let admitted = nabedra_rt::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the freed slot admits the waiter")
        .unwrap()
        .unwrap();
    held.push(admitted);
    // The bound holds: `lane` slots in all, the waiter's included.
    assert!(matches!(
        client.try_outgoing(leader, class),
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    drop((held, headroom));
    assert!(client.try_outgoing(leader, class).is_ok());
}

/// NS11 finding 8: local lane-full refusals are counted per class for
/// `/v1/runtime/stats`; a waiting Authority probe that later succeeds is not.
#[tokio::test]
async fn ns11_f8_lane_full_refusals_are_counted_per_class() {
    let temp = TestDirectoryPath(
        std::env::temp_dir().join(format!("refusals-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(&temp.0).unwrap();
    let tls = tls::certificates(&temp.0, 1);
    let local = WorkerEnrollment {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: 1,
        certificate_sha256: Digest::parse(tls[0].certificate_sha256()).unwrap(),
        endpoint: Text::new("127.0.0.1:1".to_owned()).unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
    };
    let peers = Arc::new(TestDirectory::default());
    peers.install(local.clone());
    let config = TransportConfig::default();
    let shared = test_memory(1, SharedResourceConfig::default());
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let transport =
        WorkerTransport::new(&local, &tls[0], shared, peers, registry, config.clone()).unwrap();
    let client = transport.client();
    let empty = serde_json::json!({
        "outgoing_lane_full": {"total": 0, "by_class": {}},
        "handler_lane_full": {"total": 0, "by_class": {}},
    });
    assert_eq!(client.lane_refusals(), empty);
    let class = TrafficClass::ApiObservation;
    let target = nabedra_rt::ids::uuid_v4();
    let held: Vec<_> = (0..config.handler_slots[class as usize])
        .map(|_| client.try_outgoing(target, class).unwrap())
        .collect();
    for _ in 0..2 {
        assert!(matches!(
            client.try_outgoing(target, class),
            Err(RequestError::OutgoingLaneFull { .. })
        ));
    }
    // A probe that waits for its slot is not a refusal.
    assert!(client.try_outgoing_slot(target, class).is_err());
    let refusals = client.lane_refusals();
    assert_eq!(refusals["outgoing_lane_full"]["total"], 2);
    assert_eq!(
        refusals["outgoing_lane_full"]["by_class"]["ApiObservation"],
        2
    );
    assert_eq!(refusals["handler_lane_full"]["total"], 0);
    drop(held);
    assert!(client.try_outgoing(target, class).is_ok());
    assert_eq!(client.lane_refusals()["outgoing_lane_full"]["total"], 2);
}

/// G6: after every worker cold-starts onto a new run with its certificate, a
/// running API gateway still addresses each worker's previous incarnation.
/// It may reach Control authority there, to learn the new enrollments; nothing
/// else accepts a stale audience, and no worker sender gets it.
#[test]
fn g6_api_reaches_control_authority_at_a_newer_incarnation_only() {
    let enrollment = |revision| WorkerEnrollment {
        worker_id: Uuid::from_u128(7),
        incarnation: nabedra_rt::ids::uuid_v4(),
        enrollment_revision: revision,
        certificate_sha256: Digest::from_bytes([3; 32]),
        endpoint: Text::new("127.0.0.1:1".to_owned()).unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
    };
    let local = WorkerIdentity::from(&enrollment(2));
    let stale = WorkerIdentity::from(&enrollment(1));
    let directory = TestDirectory::default();
    let api = WorkerEnrollment {
        worker_id: Uuid::from_u128(9),
        ..enrollment(1)
    };
    let worker = WorkerEnrollment {
        worker_id: Uuid::from_u128(11),
        ..enrollment(1)
    };
    directory.install_api(api.clone());
    directory.install(worker.clone());
    let control = GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("g6").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Control,
        group_format_version: 1,
    };
    let catalog = GroupIdentity {
        kind: GroupKind::Catalog,
        ..control.clone()
    };
    let hello = |from: &WorkerEnrollment, to, purpose| Hello {
        cluster_id: control.cluster_id,
        group_id: control.group_id,
        group_digest: Digest::from_bytes([0; 32]),
        purpose,
        from: WorkerIdentity::from(from),
        to,
        nonce: nabedra_rt::ids::uuid_v4(),
        header_len: 0,
    };
    let reached = |hello: &Hello, group: &GroupIdentity| audience(&directory, hello, group, &local);
    // The exact incarnation is always the audience.
    assert!(reached(&hello(&worker, local, Purpose::Raft), &control));
    // The API's Control authority calls reach the newer incarnation.
    assert!(reached(&hello(&api, stale, Purpose::Authority), &control));
    // Nothing else does: other purposes, other groups, workers, a newer or
    // different worker, or another certificate.
    assert!(!reached(&hello(&api, stale, Purpose::ApiIngress), &control));
    assert!(!reached(
        &hello(&api, stale, Purpose::ControlDelegation),
        &control
    ));
    assert!(!reached(&hello(&api, stale, Purpose::Authority), &catalog));
    assert!(!reached(
        &hello(&worker, stale, Purpose::Authority),
        &control
    ));
    let newer = WorkerIdentity::from(&enrollment(3));
    assert!(!reached(&hello(&api, newer, Purpose::Authority), &control));
    let other = WorkerIdentity {
        worker_id: Uuid::from_u128(8),
        ..stale
    };
    assert!(!reached(&hello(&api, other, Purpose::Authority), &control));
    let rekeyed = WorkerIdentity {
        certificate_sha256: Digest::from_bytes([4; 32]),
        ..stale
    };
    assert!(!reached(
        &hello(&api, rekeyed, Purpose::Authority),
        &control
    ));
    // An API whose own incarnation differs from its enrollment is not one.
    let impostor = WorkerEnrollment {
        incarnation: nabedra_rt::ids::uuid_v4(),
        ..api
    };
    assert!(!reached(
        &hello(&impostor, stale, Purpose::Authority),
        &control
    ));
}
