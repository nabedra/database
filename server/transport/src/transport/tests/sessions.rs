//! D10/O05/O06/C07: persistent peer sessions and backend parity.
use super::*;
use crate::backend;
use nabedra_rt::net::Target;
#[cfg(feature = "sim")]
use nabedra_rt::sim;
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::watch,
    task::JoinHandle,
};

const WAIT: Duration = Duration::from_secs(20);
#[cfg(feature = "sim")]
const CALLER_NODE: u64 = 1;
#[cfg(feature = "sim")]
const RECEIVER_NODE: u64 = 2;
#[cfg(feature = "sim")]
const OTHER_NODE: u64 = 3;

fn fixture_certificates() -> &'static [TlsMaterial] {
    static CERTIFICATES: std::sync::OnceLock<Vec<TlsMaterial>> = std::sync::OnceLock::new();
    CERTIFICATES.get_or_init(|| {
        let temp = TestDirectoryPath(
            std::env::temp_dir().join(format!("sessions-{}", nabedra_rt::ids::uuid_v4())),
        );
        std::fs::create_dir(&temp.0).unwrap();
        tls::certificates(&temp.0, 3)
    })
}

enum Relay {
    Tcp(TcpRelay),
    #[cfg(feature = "sim")]
    Sim(Arc<sim::World>),
}

impl Relay {
    async fn start(target: SocketAddr) -> Self {
        #[cfg(feature = "sim")]
        if let Some(world) = sim::world() {
            return Self::Sim(world);
        }
        Self::Tcp(TcpRelay::start(target).await)
    }

    fn address(&self, target: SocketAddr) -> SocketAddr {
        match self {
            Self::Tcp(relay) => {
                let _ = target;
                relay.address
            }
            #[cfg(feature = "sim")]
            Self::Sim(_) => target,
        }
    }

    fn sever(&self) {
        match self {
            Self::Tcp(relay) => relay.sever(),
            #[cfg(feature = "sim")]
            Self::Sim(world) => {
                let connections = world.connections();
                assert_eq!(connections.len(), 1);
                world.reset_connection(connections[0].id);
            }
        }
    }

    fn accepted(&self) -> u64 {
        match self {
            Self::Tcp(relay) => relay.accepted.load(Ordering::Acquire) as u64,
            #[cfg(feature = "sim")]
            Self::Sim(world) => world.connections().last().unwrap().id.0,
        }
    }

    fn black_hole(&self) {
        match self {
            Self::Tcp(relay) => relay.black_hole(),
            #[cfg(feature = "sim")]
            Self::Sim(_) => unreachable!("relay black-hole cases use real loopback TCP"),
        }
    }

    fn restore(&self) {
        match self {
            Self::Tcp(relay) => relay.restore(),
            #[cfg(feature = "sim")]
            Self::Sim(_) => unreachable!("relay black-hole cases use real loopback TCP"),
        }
    }
}

/// Echoes the request header. `held` callers block until released. A header
/// `"big:<n>"` announces an `n`-byte section.
struct Gate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    handled: std::sync::atomic::AtomicUsize,
    hold: AtomicBool,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            handled: Default::default(),
            hold: Default::default(),
        }
    }
}
impl Endpoint for Gate {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let text: String = serde_json::from_slice(header).unwrap_or_default();
            let section = text
                .strip_prefix("big:")
                .map_or(0, |bytes| bytes.parse().unwrap());
            Ok(ReceivePlan {
                class: traffic(session.group(), session.purpose())?,
                section_lengths: [section, 0, 0],
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
            if self.hold.load(Ordering::Acquire) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            self.handled.fetch_add(1, Ordering::AcqRel);
            Ok(ChargedReply {
                header,
                sections: [None, None, None],
            })
        })
    }
}

/// A TCP relay whose live connections can be severed (both directions closed)
/// or black-holed (bytes silently dropped, connections left open), to fail a
/// session without touching either endpoint. A black-holed connection stays
/// black-holed; connections accepted after `restore` forward again.
struct TcpRelay {
    address: SocketAddr,
    generation: watch::Sender<u64>,
    black_hole: watch::Sender<bool>,
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    task: JoinHandle<()>,
}
impl TcpRelay {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (generation, watcher) = watch::channel(0u64);
        let (black_hole, holes) = watch::channel(false);
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task = nabedra_rt::task::spawn({
            let accepted = accepted.clone();
            async move {
                loop {
                    let Ok((inbound, _)) = listener.accept().await else {
                        return;
                    };
                    accepted.fetch_add(1, Ordering::AcqRel);
                    let mut watcher = watcher.clone();
                    let started = *watcher.borrow_and_update();
                    let holes = holes.clone();
                    nabedra_rt::task::spawn(async move {
                        let Ok(outbound) = TcpStream::connect(target).await else {
                            return;
                        };
                        let holed = Arc::new(AtomicBool::new(false));
                        let (in_read, in_write) = inbound.into_split();
                        let (out_read, out_write) = outbound.into_split();
                        tokio::select! {
                            _ = pump(in_read, out_write, holes.clone(), holed.clone()) => {}
                            _ = pump(out_read, in_write, holes, holed) => {}
                            _ = async {
                                while *watcher.borrow_and_update() == started {
                                    if watcher.changed().await.is_err() {
                                        std::future::pending::<()>().await;
                                    }
                                }
                            } => {}
                        }
                    });
                }
            }
        });
        Self {
            address,
            generation,
            black_hole,
            accepted,
            task,
        }
    }
    fn sever(&self) {
        self.generation.send_modify(|generation| *generation += 1);
    }
    fn black_hole(&self) {
        self.black_hole.send_replace(true);
    }
    fn restore(&self) {
        self.black_hole.send_replace(false);
    }
}
async fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    holes: watch::Receiver<bool>,
    holed: Arc<AtomicBool>,
) {
    let mut buffer = vec![0; 64 << 10];
    loop {
        let Ok(read) = from.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            let _ = to.shutdown().await;
            return;
        }
        if *holes.borrow() {
            holed.store(true, Ordering::Release);
        }
        if holed.load(Ordering::Acquire) {
            continue;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
impl Drop for TcpRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One calling worker: its transport and client.
struct Caller {
    transport: WorkerTransport,
    client: WorkerClient,
    #[cfg(feature = "sim")]
    node: u64,
}
impl Caller {
    fn call(
        &self,
        target: &WorkerEnrollment,
        group: &GroupIdentity,
        purpose: Purpose,
        text: &str,
        section: Option<usize>,
    ) -> impl Future<Output = Result<ReceivedReply, RequestError>> + use<> {
        let class = traffic(group, purpose).unwrap().byte_class();
        let shared = &self.transport.common.shared;
        let header = shared.encode_in(class, &text, 1024).unwrap();
        let section = section.map(|bytes| shared.copy_bytes_in(class, &vec![7; bytes]).unwrap());
        let client = self.client.clone();
        let group = group.clone();
        let target = WorkerIdentity::from(target);
        let call = async move {
            client
                .request_classified(&group, target, purpose, header, [section, None, None], 0)
                .await
        };
        #[cfg(feature = "sim")]
        let call = sim::in_node(self.node, call);
        call
    }
}

struct Fixture {
    peers: Arc<TestDirectory>,
    caller: WorkerEnrollment,
    target: WorkerEnrollment,
    client: WorkerClient,
    caller_transport: WorkerTransport,
    /// A second calling worker, for receiver capacity tests.
    other: Caller,
    receiver: Arc<WorkerTransport>,
    endpoint: Arc<Gate>,
    group: GroupIdentity,
    data_group: GroupIdentity,
    listen: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<Result<(), ProtocolError>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Release {
    Current,
    SessionV2,
    /// The builds from IDXGC1 to before RD3: they also offer the retired
    /// `nabedra-otp/3` (index reclamation frames), no direct admission.
    SessionV3,
    OneShotOnly,
}

impl Fixture {
    async fn start(
        caller_release: Release,
        receiver_release: Release,
        caller_config: TransportConfig,
        receiver_config: TransportConfig,
        relay: bool,
    ) -> (Self, Option<Relay>) {
        let tls = fixture_certificates();
        let bind = backend::current().bind("127.0.0.1:0".parse().unwrap());
        #[cfg(feature = "sim")]
        let bind = sim::in_node(RECEIVER_NODE, bind);
        let listener = bind.await.unwrap();
        let listen = listener.local_addr().unwrap();
        let relay = if relay {
            Some(Relay::start(listen).await)
        } else {
            None
        };
        let reach = relay.as_ref().map_or(listen, |relay| relay.address(listen));
        let enrollment = |index: usize, endpoint: String| WorkerEnrollment {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            enrollment_revision: 1,
            certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
            endpoint: Text::new(endpoint).unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
        };
        let caller = enrollment(0, "127.0.0.1:1".to_owned());
        let target = enrollment(1, reach.to_string());
        let second = enrollment(2, "127.0.0.1:2".to_owned());
        #[cfg(feature = "sim")]
        if let Some(world) = sim::world() {
            for (node, peer) in [
                (CALLER_NODE, &caller),
                (RECEIVER_NODE, &target),
                (OTHER_NODE, &second),
            ] {
                world.set_certificate(node, *peer.certificate_sha256.as_bytes());
            }
        }
        let peers = Arc::new(TestDirectory::default());
        peers.install(caller.clone());
        peers.install(target.clone());
        peers.install(second.clone());
        let transport = |local: &WorkerEnrollment, node, index: usize, release, config| {
            let shared = test_memory(node, SharedResourceConfig::default());
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            let mut transport = if release == Release::OneShotOnly {
                WorkerTransport::one_shot_for_test(
                    local,
                    &tls[index],
                    shared,
                    peers.clone(),
                    registry,
                    config,
                )
            } else {
                WorkerTransport::new(local, &tls[index], shared, peers.clone(), registry, config)
            }
            .unwrap();
            if release == Release::SessionV2 {
                let common = Arc::get_mut(&mut transport.common).unwrap();
                (common.connector, common.acceptor) =
                    tls[index].transports(&[SESSION_ALPN, ONE_SHOT_ALPN]);
            }
            if release == Release::SessionV3 {
                let common = Arc::get_mut(&mut transport.common).unwrap();
                (common.connector, common.acceptor) = tls[index].transports(&[
                    LEGACY_INDEX_RECLAIM_ALPN,
                    SESSION_ALPN,
                    ONE_SHOT_ALPN,
                ]);
            }
            transport
        };
        let group = GroupIdentity {
            cluster_id: nabedra_rt::ids::uuid_v4(),
            database: Text::try_from("sessions").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        let data_group = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Data {
                table_id: nabedra_rt::ids::uuid_v4(),
                shard_id: nabedra_rt::ids::uuid_v4(),
            },
            ..group.clone()
        };
        let receiver = Arc::new(transport(&target, 2, 1, receiver_release, receiver_config));
        let endpoint = Arc::new(Gate::default());
        for group in [&group, &data_group] {
            let resident = receiver.registry.reserve(group.clone()).unwrap();
            for purpose in [Purpose::Raft, Purpose::RaftProgress, Purpose::Storage] {
                resident.bind(purpose, endpoint.clone()).unwrap();
            }
        }
        let caller_transport = transport(&caller, 1, 0, caller_release, caller_config.clone());
        let client = caller_transport.client();
        let other_transport = transport(&second, 3, 2, caller_release, caller_config);
        let other = Caller {
            client: other_transport.client(),
            transport: other_transport,
            #[cfg(feature = "sim")]
            node: OTHER_NODE,
        };
        let (stop, shutdown) = oneshot::channel();
        let serve = receiver.clone().run(listener, async {
            let _ = shutdown.await;
        });
        #[cfg(not(feature = "sim"))]
        let worker = nabedra_rt::task::spawn(serve);
        #[cfg(feature = "sim")]
        let worker = if let Some(world) = sim::world() {
            world.spawn_node(RECEIVER_NODE, serve)
        } else {
            nabedra_rt::task::spawn(serve)
        };
        (
            Self {
                peers,
                caller,
                target,
                client,
                caller_transport,
                other,
                receiver,
                endpoint,
                group,
                data_group,
                listen,
                stop: Some(stop),
                worker: Some(worker),
            },
            relay,
        )
    }
    fn caller(&self) -> Caller {
        Caller {
            transport: WorkerTransport {
                common: self.caller_transport.common.clone(),
                registry: self.caller_transport.registry.clone(),
            },
            client: self.client.clone(),
            #[cfg(feature = "sim")]
            node: CALLER_NODE,
        }
    }
    fn call(
        &self,
        group: &GroupIdentity,
        purpose: Purpose,
        text: &str,
    ) -> impl Future<Output = Result<ReceivedReply, RequestError>> + use<> {
        self.caller().call(&self.target, group, purpose, text, None)
    }
    async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(WAIT, self.worker.take().unwrap())
            .await
            .expect("listener stops with live sessions")
            .unwrap()
            .unwrap();
    }
}

fn echoed(reply: Result<ReceivedReply, RequestError>, text: &str) {
    let reply = reply.unwrap_or_else(|error| panic!("{text}: {error:?}"));
    assert_eq!(reply.header.as_slice(), format!("\"{text}\"").as_bytes());
}

async fn eventually(mut done: impl FnMut() -> bool, what: &str) {
    timeout(WAIT, async {
        while !done() {
            nabedra_rt::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// D10/O05: many sequential and concurrent calls share one TLS handshake per
/// lane. Raft append and progress lanes each get their own session.
async fn d10_o05_one_session_per_lane_carries_many_calls() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    timeout(WAIT, async {
        for index in 0..200 {
            let text = format!("serial-{index}");
            echoed(
                fixture
                    .call(&fixture.group, Purpose::RaftProgress, &text)
                    .await,
                &text,
            );
        }
        assert_eq!(fixture.client.connections_for_test(), 1);
        let progress =
            TransportConfig::default().handler_slots[TrafficClass::RaftProgress as usize];
        let mut concurrent = Vec::new();
        for index in 0..progress {
            let text = format!("concurrent-{index}");
            let call = fixture.call(&fixture.group, Purpose::RaftProgress, &text);
            concurrent.push(nabedra_rt::task::spawn(async move { (call.await, text) }));
        }
        for call in concurrent {
            let (reply, text) = call.await.unwrap();
            echoed(reply, &text);
        }
        assert_eq!(fixture.client.connections_for_test(), 1);
        // Raft appends use the DataConsensus lane: one more session.
        for index in 0..20 {
            let text = format!("append-{index}");
            echoed(
                fixture
                    .call(&fixture.data_group, Purpose::Raft, &text)
                    .await,
                &text,
            );
        }
        assert_eq!(fixture.client.connections_for_test(), 2);
        assert_eq!(fixture.client.live_sessions_for_test(), 2);
        assert_eq!(
            fixture.endpoint.handled.load(Ordering::Acquire),
            220 + progress
        );
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10: severing a session while a request is in flight gives that caller an
/// unknown outcome (the handler still runs), and the next call reconnects.
async fn d10_dropped_session_mid_stream_is_unknown_and_next_call_reconnects() {
    let (fixture, relay) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        true,
    )
    .await;
    let relay = relay.unwrap();
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "warm")
                .await,
            "warm",
        );
        assert_eq!(fixture.client.connections_for_test(), 1);
        fixture.endpoint.hold.store(true, Ordering::Release);
        let in_flight =
            nabedra_rt::task::spawn(fixture.call(&fixture.group, Purpose::RaftProgress, "lost"));
        fixture.endpoint.entered.acquire().await.unwrap().forget();
        relay.sever();
        let failure = in_flight
            .await
            .unwrap()
            .err()
            .expect("severed call succeeded");
        assert!(matches!(failure, RequestError::Unreachable), "{failure:?}");
        let error = ProtocolError::from(failure);
        assert_eq!(error.outcome, Outcome::Unknown);
        fixture.endpoint.hold.store(false, Ordering::Release);
        fixture.endpoint.release.add_permits(1);
        // The receiver applied the request whose reply was lost.
        eventually(
            || fixture.endpoint.handled.load(Ordering::Acquire) == 2,
            "held handler completes",
        )
        .await;
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "after")
                .await,
            "after",
        );
        assert_eq!(fixture.client.connections_for_test(), 2);
        assert_eq!(relay.accepted(), 2);
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/C07/O05: a saturated Data consensus lane returns overload errors on
/// both sides while the Raft progress lane keeps answering.
async fn d10_c07_o05_exhausted_lane_overloads_while_raft_progress_advances() {
    let data = TrafficClass::DataConsensus as usize;
    let caller_config = TransportConfig::default();
    let mut receiver_config = TransportConfig::default();
    // The receiver admits fewer Data consensus handlers than the caller sends.
    receiver_config.handler_slots[data] = 1;
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        caller_config.clone(),
        receiver_config,
        false,
    )
    .await;
    timeout(WAIT, async {
        fixture.endpoint.hold.store(true, Ordering::Release);
        let held =
            nabedra_rt::task::spawn(fixture.call(&fixture.data_group, Purpose::Raft, "held"));
        fixture.endpoint.entered.acquire().await.unwrap().forget();
        // The receiver's handler lane is full: an explicit overload, not applied.
        let rejected = fixture
            .call(&fixture.data_group, Purpose::Raft, "receiver-full")
            .await
            .err()
            .expect("receiver handler lane admitted a second stream");
        let RequestError::PeerRejected { error, .. } = rejected else {
            panic!("expected a peer overload: {rejected:?}");
        };
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);

        // The held stream owns one caller slot. Take the rest of the lane:
        // the next Data consensus call is refused locally, before any write.
        let rest = fixture.client.take_outgoing_for_test(
            fixture.target.worker_id,
            TrafficClass::DataConsensus,
            caller_config.handler_slots[data] as u32 - 1,
        );
        let Err(refusal @ RequestError::OutgoingLaneFull { .. }) = fixture
            .call(&fixture.data_group, Purpose::Raft, "caller-full")
            .await
        else {
            panic!("expected a local caller refusal");
        };
        let error = ProtocolError::from(refusal);
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);
        assert_eq!(error.message, "worker outgoing lane full (DataConsensus)");

        // Raft progress traffic for the same group advances on its own lane.
        fixture.endpoint.hold.store(false, Ordering::Release);
        for index in 0..32 {
            let text = format!("heartbeat-{index}");
            echoed(
                fixture
                    .call(&fixture.data_group, Purpose::RaftProgress, &text)
                    .await,
                &text,
            );
        }
        assert!(!held.is_finished());
        drop(rest);
        fixture.endpoint.release.add_permits(1);
        echoed(held.await.unwrap(), "held");
        echoed(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "recovered")
                .await,
            "recovered",
        );
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10: a peer that offers only the one-shot `NABOTP01` protocol still
/// interoperates in both directions, one connection per call.
async fn d10_old_one_shot_peer_interoperates_in_both_directions() {
    for (caller, receiver) in [
        (Release::Current, Release::OneShotOnly),
        (Release::OneShotOnly, Release::Current),
    ] {
        let (fixture, _) = Fixture::start(
            caller,
            receiver,
            TransportConfig::default(),
            TransportConfig::default(),
            false,
        )
        .await;
        timeout(WAIT, async {
            for index in 0..5 {
                let text = format!("legacy-{index}");
                echoed(
                    fixture
                        .call(&fixture.group, Purpose::RaftProgress, &text)
                        .await,
                    &text,
                );
                echoed(
                    fixture
                        .call(&fixture.data_group, Purpose::Raft, &text)
                        .await,
                    &text,
                );
            }
            let mut concurrent = Vec::new();
            for index in 0..4 {
                let text = format!("legacy-concurrent-{index}");
                let call = fixture.call(&fixture.group, Purpose::RaftProgress, &text);
                concurrent.push(nabedra_rt::task::spawn(async move { (call.await, text) }));
            }
            for call in concurrent {
                let (reply, text) = call.await.unwrap();
                echoed(reply, &text);
            }
            assert_eq!(fixture.client.connections_for_test(), 14);
            assert_eq!(fixture.client.live_sessions_for_test(), 0);
        })
        .await
        .unwrap();
        fixture.stop().await;
    }
}

/// The ALPN that IDXGC1 builds offered for index reclamation frames. CAP1
/// retired it: Control's `data.index.reclaim` cluster feature gates them.
const LEGACY_INDEX_RECLAIM_ALPN: &[u8] = b"nabedra-otp/3";

/// CAP1/D10/O02: with `nabedra-otp/3` retired, this build still forms one
/// session per lane with every older session build, in both directions:
/// otp/2 with a pre-IDXGC1 build and with an IDXGC1 build (which also offers
/// otp/3), and otp/4 with an RD3 build. Raft frames, a reclamation-shaped
/// frame included, travel on the negotiated session; nothing requires a
/// per-connection capability any more.
async fn cap1_d10_o02_sessions_form_with_older_builds_after_otp3_is_retired() {
    for (caller, receiver) in [
        (Release::Current, Release::SessionV2),
        (Release::SessionV2, Release::Current),
        (Release::Current, Release::SessionV3),
        (Release::SessionV3, Release::Current),
        (Release::Current, Release::Current),
    ] {
        let (f, _) = Fixture::start(
            caller,
            receiver,
            TransportConfig::default(),
            TransportConfig::default(),
            false,
        )
        .await;
        timeout(WAIT, async {
            for text in ["append", "reclaim", "append again"] {
                echoed(f.call(&f.data_group, Purpose::Raft, text).await, text);
            }
            assert_eq!(f.endpoint.handled.load(Ordering::Acquire), 3);
            assert_eq!(f.client.connections_for_test(), 1, "one session");
            assert_eq!(f.client.live_sessions_for_test(), 1);
            let session = |feature| {
                f.client
                    .peer_supports(f.target.worker_id, TrafficClass::DataConsensus, feature)
                    .is_some()
            };
            assert!(
                session(PeerFeature::MultiEntryAppend),
                "a session, not one-shot"
            );
            // otp/4 only between two builds that offer it.
            assert_eq!(
                session(PeerFeature::DirectAdmission),
                caller == Release::Current && receiver == Release::Current
            );
        })
        .await
        .unwrap();
        f.stop().await;
    }
}

/// D10: a session closes when either side's peer incarnation leaves the local
/// directory, and a stale incarnation cannot reuse the authenticated session.
async fn d10_session_closes_when_peer_incarnation_leaves_directory() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "first")
                .await,
            "first",
        );
        assert_eq!(fixture.client.live_sessions_for_test(), 1);

        // The receiver no longer recognizes the caller's incarnation and
        // closes the session; the caller observes the closed connection.
        let mut replaced = fixture.caller.clone();
        replaced.incarnation = nabedra_rt::ids::uuid_v4();
        replaced.enrollment_revision = 2;
        fixture.peers.install(replaced);
        eventually(
            || fixture.client.live_sessions_for_test() == 0,
            "receiver closes the departed caller's session",
        )
        .await;
        assert!(matches!(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "stale")
                .await,
            Err(RequestError::Unreachable)
        ));
        fixture.peers.install(fixture.caller.clone());
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "restored")
                .await,
            "restored",
        );
        assert_eq!(fixture.client.live_sessions_for_test(), 1);

        // The caller closes its own session once the target leaves.
        let mut moved = fixture.target.clone();
        moved.incarnation = nabedra_rt::ids::uuid_v4();
        moved.enrollment_revision = 2;
        moved.endpoint = Text::new(fixture.listen.to_string()).unwrap();
        fixture.peers.install(moved);
        eventually(
            || fixture.client.live_sessions_for_test() == 0,
            "caller closes the departed target's session",
        )
        .await;
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/O06 (finding 1): a black-holed peer (bytes dropped, connection left
/// open) costs one reply deadline, not every later call's: the timed-out
/// stream saw nothing at all since its request was written, so the session
/// closes, and the next call reconnects once the network heals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o06_black_holed_session_closes_after_a_silent_reply_deadline() {
    // The ping cannot fire before the reply deadline: this isolates the
    // reply-deadline rule.
    let caller_config = TransportConfig {
        deadline: Duration::from_secs(2),
        ping_timeout: Duration::from_millis(1500),
        ping_idle: Duration::from_secs(60),
        ..TransportConfig::default()
    };
    let (fixture, relay) = Fixture::start(
        Release::Current,
        Release::Current,
        caller_config,
        TransportConfig::default(),
        true,
    )
    .await;
    let relay = relay.unwrap();
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "warm")
                .await,
            "warm",
        );
        relay.black_hole();
        let failure = fixture
            .call(&fixture.group, Purpose::RaftProgress, "lost")
            .await
            .err()
            .expect("black-holed call succeeded");
        assert!(matches!(failure, RequestError::Unreachable), "{failure:?}");
        assert_eq!(ProtocolError::from(failure).outcome, Outcome::Unknown);
        // Closed by the reply deadline itself, before any ping could.
        assert_eq!(fixture.client.live_sessions_for_test(), 0);
        relay.restore();
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "healed")
                .await,
            "healed",
        );
        assert_eq!(fixture.client.connections_for_test(), 2);
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/O06 (finding 1): pings find a black-holed session. An idle session is
/// closed with no call at all, and a call waiting on a black-holed session
/// fails after the ping timeout instead of its 30 s deadline. Connects into
/// the black hole are bounded by the connect timeout, and the lane recovers
/// as soon as the network heals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o06_black_holed_session_is_closed_by_an_unanswered_ping() {
    let caller_config = TransportConfig {
        ping_idle: Duration::from_secs(2),
        ping_timeout: Duration::from_secs(1),
        connect_timeout: Duration::from_secs(2),
        ..TransportConfig::default()
    };
    let (fixture, relay) = Fixture::start(
        Release::Current,
        Release::Current,
        caller_config,
        TransportConfig::default(),
        true,
    )
    .await;
    let relay = relay.unwrap();
    timeout(Duration::from_secs(60), async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "warm")
                .await,
            "warm",
        );
        // Idle: healthy sessions answer pings and stay open.
        nabedra_rt::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "still-warm")
                .await,
            "still-warm",
        );
        assert_eq!(fixture.client.connections_for_test(), 1);

        relay.black_hole();
        eventually(
            || fixture.client.live_sessions_for_test() == 0,
            "an unanswered idle ping closes the session",
        )
        .await;
        // A connect into the black hole fails at the connect timeout.
        let started = nabedra_rt::time::now();
        assert!(matches!(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "connect")
                .await,
            Err(RequestError::Unreachable)
        ));
        assert!(nabedra_rt::time::elapsed(started) < Duration::from_secs(10));

        relay.restore();
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "healed")
                .await,
            "healed",
        );
        assert_eq!(fixture.client.connections_for_test(), 2);

        // A call waiting on a session that goes black fails at the busy ping
        // timeout, far below the 30 s transport deadline.
        relay.black_hole();
        let started = nabedra_rt::time::now();
        let failure = fixture
            .call(&fixture.group, Purpose::RaftProgress, "waiting")
            .await
            .err()
            .expect("black-holed call succeeded");
        assert!(matches!(failure, RequestError::Unreachable), "{failure:?}");
        assert!(
            nabedra_rt::time::elapsed(started) < Duration::from_secs(10),
            "{:?}",
            nabedra_rt::time::elapsed(started)
        );
        relay.restore();
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "healed-again")
                .await,
            "healed-again",
        );
        assert_eq!(fixture.client.connections_for_test(), 3);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10 (missing coverage): a caller dropped after its frame was written sends
/// `CANCEL`; the receiver still runs the handler but skips the reply, and the
/// session stays usable. A caller dropped while its frame is only queued also
/// cancels: the `CANCEL` follows the request on the same ordered queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_cancelled_stream_skips_its_reply_and_keeps_the_session() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    let skipped = || {
        fixture
            .receiver
            .common
            .cancelled_replies
            .load(Ordering::Acquire)
    };
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "warm")
                .await,
            "warm",
        );
        // Written, then dropped while the handler runs.
        fixture.endpoint.hold.store(true, Ordering::Release);
        let written =
            nabedra_rt::task::spawn(fixture.call(&fixture.group, Purpose::RaftProgress, "dropped"));
        fixture.endpoint.entered.acquire().await.unwrap().forget();
        written.abort();
        let _ = written.await;
        assert_eq!(fixture.client.waiting_streams_for_test(), 0);
        // Let the CANCEL arrive before the handler finishes.
        nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        fixture.endpoint.release.add_permits(1);
        eventually(|| skipped() == 1, "receiver skips the cancelled reply").await;
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 2);

        // Queued but not yet written when dropped.
        fixture
            .caller_transport
            .common
            .hold_writes
            .send_replace(true);
        let queued =
            nabedra_rt::task::spawn(fixture.call(&fixture.group, Purpose::RaftProgress, "queued"));
        eventually(
            || fixture.client.waiting_streams_for_test() == 1,
            "call queued",
        )
        .await;
        nabedra_rt::time::sleep(Duration::from_millis(50)).await;
        queued.abort();
        let _ = queued.await;
        fixture
            .caller_transport
            .common
            .hold_writes
            .send_replace(false);
        fixture.endpoint.entered.acquire().await.unwrap().forget();
        nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        fixture.endpoint.release.add_permits(1);
        eventually(|| skipped() == 2, "receiver skips the queued cancel").await;
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 3);

        fixture.endpoint.hold.store(false, Ordering::Release);
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "after")
                .await,
            "after",
        );
        assert_eq!(fixture.client.connections_for_test(), 1);
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10 (missing coverage): frames still queued when their session closes are
/// sent once more on a fresh connection, and each runs exactly once. A frame
/// that already reached the socket is never retried: its caller gets an
/// unknown outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_unsent_frames_retry_once_on_a_fresh_session_and_written_frames_never_do() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    let handled = || fixture.endpoint.handled.load(Ordering::Acquire);
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "warm")
                .await,
            "warm",
        );
        fixture
            .caller_transport
            .common
            .hold_writes
            .send_replace(true);
        let mut calls = Vec::new();
        for index in 0..3 {
            let text = format!("queued-{index}");
            let call = fixture.call(&fixture.group, Purpose::RaftProgress, &text);
            calls.push(nabedra_rt::task::spawn(async move { (call.await, text) }));
        }
        eventually(
            || fixture.client.waiting_streams_for_test() == 3,
            "three frames queued",
        )
        .await;
        fixture.client.close_sessions_for_test();
        fixture
            .caller_transport
            .common
            .hold_writes
            .send_replace(false);
        for call in calls {
            let (reply, text) = call.await.unwrap();
            echoed(reply, &text);
        }
        assert_eq!(handled(), 4, "each unsent frame ran exactly once");
        assert_eq!(fixture.client.connections_for_test(), 2);

        // Written, then the session fails: unknown outcome, no retry.
        fixture.endpoint.hold.store(true, Ordering::Release);
        let written =
            nabedra_rt::task::spawn(fixture.call(&fixture.group, Purpose::RaftProgress, "written"));
        fixture.endpoint.entered.acquire().await.unwrap().forget();
        fixture.client.close_sessions_for_test();
        let failure = written.await.unwrap().err().expect("closed call succeeded");
        assert!(matches!(failure, RequestError::Unreachable), "{failure:?}");
        fixture.endpoint.hold.store(false, Ordering::Release);
        fixture.endpoint.release.add_permits(1);
        eventually(|| handled() == 5, "written frame ran once").await;
        nabedra_rt::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handled(), 5, "a written frame was retried");
        assert_eq!(fixture.client.connections_for_test(), 2);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/O05 (missing coverage): a large frame and small frames share one lane.
/// Replies demultiplex to the right callers, and small calls wait at most for
/// the large frame's bytes, not for its handler or a fresh connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o05_large_and_small_frames_share_one_lane() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    let large = 4 << 20;
    timeout(WAIT, async {
        let caller = fixture.caller();
        let mut tasks = Vec::new();
        for index in 0..3 {
            let text = format!("big:{large}");
            let call = caller.call(
                &fixture.target,
                &fixture.data_group,
                Purpose::Storage,
                &text,
                Some(large),
            );
            tasks.push(nabedra_rt::task::spawn(
                async move { (call.await, text, index) },
            ));
        }
        let mut slowest = Duration::ZERO;
        for index in 0..24 {
            let text = format!("small-{index}");
            let started = nabedra_rt::time::now();
            let reply = loop {
                match caller
                    .call(
                        &fixture.target,
                        &fixture.data_group,
                        Purpose::Storage,
                        &text,
                        None,
                    )
                    .await
                {
                    // Three large calls may hold the lane's four slots briefly.
                    Err(RequestError::OutgoingLaneFull { .. }) => tokio::task::yield_now().await,
                    reply => break reply,
                }
            };
            slowest = slowest.max(nabedra_rt::time::elapsed(started));
            echoed(reply, &text);
        }
        for task in tasks {
            let (reply, text, _) = task.await.unwrap();
            echoed(reply, &text);
        }
        eprintln!("d10_o05 shared lane: slowest small call {slowest:?}");
        assert!(slowest < Duration::from_secs(5), "{slowest:?}");
        assert_eq!(fixture.client.connections_for_test(), 1);
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 27);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/C07 (finding 2): a full session lane answers a new session's first
/// request with a typed overload (not applied), never an unknown outcome,
/// while Raft progress keeps its own session pool. A full pending pool makes
/// the listener offer only the one-shot protocol during the handshake, and a
/// session that never names a lane is closed after the grace period.
async fn d10_c07_full_session_pools_reject_typed_and_keep_raft_progress() {
    let data = TrafficClass::DataConsensus as usize;
    let mut receiver_config = TransportConfig::default();
    receiver_config.session_slots[data] = 1;
    receiver_config.pending_sessions = 1;
    receiver_config.ping_idle = Duration::from_secs(2);
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        receiver_config,
        false,
    )
    .await;
    let other = &fixture.other;
    timeout(WAIT, async {
        // The first caller holds the only Data consensus session.
        echoed(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "first")
                .await,
            "first",
        );
        let rejected = other
            .call(
                &fixture.target,
                &fixture.data_group,
                Purpose::Raft,
                "second",
                None,
            )
            .await
            .err()
            .expect("a full session lane admitted another session");
        let RequestError::PeerRejected { error, .. } = rejected else {
            panic!("expected a typed overload: {rejected:?}");
        };
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 1);
        // Raft progress has its own pool.
        for index in 0..8 {
            let text = format!("progress-{index}");
            echoed(
                other
                    .call(
                        &fixture.target,
                        &fixture.data_group,
                        Purpose::RaftProgress,
                        &text,
                        None,
                    )
                    .await,
                &text,
            );
        }
        eventually(
            || other.client.live_sessions_for_test() == 1,
            "the rejected session closes after its typed reply",
        )
        .await;
        // The first caller's session is untouched.
        echoed(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "first-again")
                .await,
            "first-again",
        );
        assert_eq!(fixture.client.connections_for_test(), 1);

        // A connection that never sends a request holds the pending slot.
        let (mut idle, handshake) = fixture
            .caller_transport
            .common
            .connector
            .connect(
                Target::Addr(fixture.listen),
                ServerName::try_from("localhost").unwrap(),
            )
            .await
            .unwrap();
        assert!(session_alpn(handshake.alpn.as_deref()));
        // A new lane now handshakes without sessions and still works.
        let before = other.client.connections_for_test();
        echoed(
            other
                .call(
                    &fixture.target,
                    &fixture.group,
                    Purpose::Storage,
                    "one-shot",
                    None,
                )
                .await,
            "one-shot",
        );
        assert_eq!(other.client.connections_for_test(), before + 1);
        assert_eq!(other.client.live_sessions_for_test(), 1);
        // The unidentified session is closed after the grace period.
        let mut byte = [0; 1];
        assert!(matches!(
            timeout(Duration::from_secs(10), idle.read(&mut byte)).await,
            Ok(Ok(0) | Err(_))
        ));
        // With the pending slot back, the lane gets a session again.
        echoed(
            other
                .call(
                    &fixture.target,
                    &fixture.group,
                    Purpose::Storage,
                    "session",
                    None,
                )
                .await,
            "session",
        );
        assert_eq!(other.client.live_sessions_for_test(), 2);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/O05 (finding 4): a request refused because the receiver's preflight
/// lane is full gets a typed overload on a session, not an unknown outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o05_full_preflight_lane_is_a_typed_overload() {
    let storage = TrafficClass::Storage as usize;
    let mut receiver_config = TransportConfig::default();
    receiver_config.preflight_slots[storage] = 1;
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        receiver_config,
        false,
    )
    .await;
    timeout(WAIT, async {
        echoed(
            fixture.call(&fixture.group, Purpose::Storage, "warm").await,
            "warm",
        );
        let held = fixture.receiver.common.preflight[storage]
            .clone()
            .try_acquire_owned()
            .unwrap();
        let rejected = fixture
            .call(&fixture.group, Purpose::Storage, "refused")
            .await
            .err()
            .expect("a full preflight lane admitted a stream");
        let RequestError::PeerRejected { error, .. } = rejected else {
            panic!("expected a typed overload: {rejected:?}");
        };
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);
        drop(held);
        echoed(
            fixture
                .call(&fixture.group, Purpose::Storage, "admitted")
                .await,
            "admitted",
        );
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 2);
        assert_eq!(fixture.client.connections_for_test(), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10 (finding 5): callers queued behind one lane's connect share its
/// failure instead of each retrying the connect in turn, and the connect
/// itself is bounded by its own timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_queued_callers_share_a_bounded_failed_connect() {
    let caller_config = TransportConfig {
        connect_timeout: Duration::from_secs(1),
        ..TransportConfig::default()
    };
    let (fixture, relay) = Fixture::start(
        Release::Current,
        Release::Current,
        caller_config,
        TransportConfig::default(),
        true,
    )
    .await;
    let relay = relay.unwrap();
    timeout(WAIT, async {
        relay.black_hole();
        let started = nabedra_rt::time::now();
        let mut calls = Vec::new();
        for index in 0..4 {
            calls.push(nabedra_rt::task::spawn(fixture.call(
                &fixture.group,
                Purpose::RaftProgress,
                &format!("queued-{index}"),
            )));
        }
        for call in calls {
            assert!(matches!(
                call.await.unwrap(),
                Err(RequestError::Unreachable)
            ));
        }
        // One bounded connect, shared: well below four in a row.
        assert!(
            nabedra_rt::time::elapsed(started) < Duration::from_millis(3500),
            "{:?}",
            nabedra_rt::time::elapsed(started)
        );
        assert!(relay.accepted() <= 2);
        relay.restore();
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "healed")
                .await,
            "healed",
        );
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D05/LIVE1: a peer that stops answering (SIGSTOP, a black-holed route) keeps
/// its stalled streams until their deadlines. They fill only that peer's lane:
/// the same class to another peer still sends. With one class-wide pool, the
/// stalled streams took every slot, and Raft votes and heartbeats to the
/// healthy peers were refused locally until the paused peer resumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d05_live1_black_holed_peer_fills_only_its_own_lane() {
    let caller_config = TransportConfig {
        deadline: Duration::from_secs(10),
        ping_timeout: Duration::from_secs(8),
        ping_idle: Duration::from_secs(60),
        ..TransportConfig::default()
    };
    let slots = caller_config.handler_slots[TrafficClass::DataConsensus as usize];
    let (fixture, relay) = Fixture::start(
        Release::Current,
        Release::Current,
        caller_config,
        TransportConfig::default(),
        true,
    )
    .await;
    let relay = relay.unwrap();
    // The second worker also serves the Data group, at a real address.
    let healthy = fixture.other.transport.common.local;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut enrollment = fixture.peers.resolve(healthy.worker_id).unwrap();
    enrollment.endpoint = Text::new(listener.local_addr().unwrap().to_string()).unwrap();
    fixture.peers.install(enrollment.clone());
    let gate = Arc::new(Gate::default());
    fixture
        .other
        .transport
        .registry
        .reserve(fixture.data_group.clone())
        .unwrap()
        .bind(Purpose::Raft, gate.clone())
        .unwrap();
    let server = Arc::new(WorkerTransport {
        common: fixture.other.transport.common.clone(),
        registry: fixture.other.transport.registry.clone(),
    });
    let (stop_healthy, shutdown) = oneshot::channel::<()>();
    let healthy_worker = nabedra_rt::task::spawn(server.run(listener, async {
        let _ = shutdown.await;
    }));
    let caller = fixture.caller();
    let to_healthy =
        |text: &str| caller.call(&enrollment, &fixture.data_group, Purpose::Raft, text, None);
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "warm")
                .await,
            "warm",
        );
        echoed(to_healthy("warm-healthy").await, "warm-healthy");
        relay.black_hole();
        let stalled = (0..slots)
            .map(|index| {
                let call = fixture.call(
                    &fixture.data_group,
                    Purpose::Raft,
                    &format!("stalled-{index}"),
                );
                nabedra_rt::task::spawn(call)
            })
            .collect::<Vec<_>>();
        // The black-holed peer's lane is full: refused locally, before any write.
        let lane = fixture.client.common.outgoing.lane(
            fixture.target.worker_id,
            TrafficClass::DataConsensus,
            &fixture.client.common.config,
        );
        eventually(
            || lane.available_permits() == 0,
            "stalled calls hold the lane",
        )
        .await;
        assert!(matches!(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "excess")
                .await,
            Err(RequestError::OutgoingLaneFull { .. })
        ));
        assert!(stalled.iter().all(|call| !call.is_finished()));
        // The same class to the healthy peer still sends, many times over.
        for index in 0..(4 * slots) {
            let text = format!("healthy-{index}");
            echoed(to_healthy(&text).await, &text);
        }
        assert_eq!(gate.handled.load(Ordering::Acquire), 1 + 4 * slots);
        for call in stalled {
            call.abort();
        }
    })
    .await
    .unwrap();
    stop_healthy.send(()).unwrap();
    let _ = timeout(WAIT, healthy_worker).await;
    fixture.stop().await;
}

/// G1: a local failure after the request reached the peer (here the caller's
/// own reply decode budget is over its class limit, checked on the reply) is
/// not `not_applied`: the peer has already handled the request. A refusal
/// before anything was written stays `not_applied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_local_failure_after_send_has_unknown_outcome() {
    for release in [Release::Current, Release::OneShotOnly] {
        let (fixture, _) = Fixture::start(
            release,
            Release::Current,
            TransportConfig::default(),
            TransportConfig::default(),
            false,
        )
        .await;
        let class = traffic(&fixture.group, Purpose::Storage).unwrap();
        let shared = &fixture.caller_transport.common.shared;
        let header = shared
            .encode_in(class.byte_class(), &"after-send", 1024)
            .unwrap();
        let over_budget = TransportConfig::default().decode_bytes[class as usize] + 1;
        let refused = timeout(
            WAIT,
            fixture.client.request_classified(
                &fixture.group,
                WorkerIdentity::from(&fixture.target),
                Purpose::Storage,
                header,
                [None, None, None],
                over_budget,
            ),
        )
        .await
        .unwrap();
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 1);
        match refused {
            Err(RequestError::Protocol(error)) => {
                assert_eq!(error.outcome, Outcome::Unknown, "{error:?}");
                assert_ne!(
                    ProtocolError::from(RequestError::Protocol(error)).outcome,
                    Outcome::NotApplied
                );
            }
            Err(other) => panic!("expected a local protocol failure, got {other:?}"),
            Ok(_) => panic!("the over-budget reply was accepted"),
        }
        // Before sending, a full lane is still a definitive refusal.
        let _held = fixture.client.hold_outgoing_for_test(class);
        let header = shared.encode_in(class.byte_class(), &"held", 1024).unwrap();
        let full = fixture
            .client
            .request_classified(
                &fixture.group,
                WorkerIdentity::from(&fixture.target),
                Purpose::Storage,
                header,
                [None, None, None],
                0,
            )
            .await;
        let Err(full) = full else {
            panic!("a held lane accepted the call");
        };
        assert!(matches!(full, RequestError::OutgoingLaneFull { .. }));
        assert_eq!(ProtocolError::from(full).outcome, Outcome::NotApplied);
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 1);
        drop(_held);
        fixture.stop().await;
    }
}

/// RD3 (D12, O03, PERF1, C07): a range leader handles direct requests (ingress
/// on a Data group) in their own pool, not in the ingress class's 4 handler
/// slots, so a caller keeps many in flight to it and the leader can batch
/// them. Before RD3 at most 3 waiting direct requests reached the leader at
/// once. Queued ingress (a Control submission that refuses at once) keeps its
/// own slots next to them. To a leader of the previous build (no direct
/// admission negotiated) the caller keeps PERF1's bound: at most 3 waiting
/// direct requests in flight, the rest wait in arrival order, none refused,
/// and the queued caller still finds its slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rd3_d12_o03_direct_requests_beyond_the_ingress_slots_reach_the_leader() {
    let ingress = TransportConfig::default().handler_slots[TrafficClass::ApiIngress as usize];
    let direct = 3 * ingress;
    for receiver_release in [Release::Current, Release::SessionV3] {
        let (f, _) = Fixture::start(
            Release::Current,
            receiver_release,
            TransportConfig::default(),
            TransportConfig::default(),
            false,
        )
        .await;
        let control = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            ..f.group.clone()
        };
        f.receiver
            .registry
            .reserve(control.clone())
            .unwrap()
            .bind(Purpose::ApiIngress, f.endpoint.clone())
            .unwrap();
        f.receiver
            .registry
            .reserve(f.data_group.clone())
            .unwrap()
            .bind(Purpose::ApiIngress, f.endpoint.clone())
            .unwrap();
        // The first call opens the ingress session and negotiates its ALPN.
        echoed(
            f.call(&f.data_group, Purpose::ApiIngress, "open").await,
            "open",
        );
        let wide = receiver_release == Release::Current;
        assert_eq!(
            f.client
                .peer_supports(
                    f.target.worker_id,
                    TrafficClass::ApiIngress,
                    PeerFeature::DirectAdmission
                )
                .is_some(),
            wide
        );
        f.endpoint.hold.store(true, Ordering::Release);
        let refused = f.client.lane_refusals();
        let calls: Vec<_> = (0..direct)
            .map(|index| {
                let client = f.client.clone();
                let group = f.data_group.clone();
                let target = f.target.clone();
                let shared = f.caller_transport.common.shared.clone();
                nabedra_rt::task::spawn(async move {
                    let text = format!("direct-{index}");
                    let reservation = client
                        .reserve_outgoing(
                            &group,
                            target.worker_id,
                            Purpose::ApiIngress,
                            nabedra_rt::time::now() + WAIT,
                        )
                        .await?;
                    let header = shared.encode_in(ByteClass::Data, &text, 1024).unwrap();
                    let reply = client
                        .request_reserved(
                            reservation,
                            &group,
                            WorkerIdentity::from(&target),
                            Purpose::ApiIngress,
                            header,
                            [None, None, None],
                            0,
                        )
                        .await;
                    echoed(reply, &text);
                    Ok::<_, RequestError>(())
                })
            })
            .collect();
        let at_leader = if wide { direct } else { ingress - 1 };
        let entered = |n: usize| {
            let endpoint = f.endpoint.clone();
            async move {
                timeout(WAIT, endpoint.entered.acquire_many(n as u32))
                    .await
                    .unwrap_or_else(|_| panic!("{n} requests reach the leader's handler"))
                    .unwrap()
                    .forget();
            }
        };
        entered(at_leader).await;
        nabedra_rt::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            f.endpoint.entered.available_permits(),
            0,
            "no more than {at_leader} direct requests at the leader at once"
        );
        // A queued submission refuses at once on a full lane; it still gets in.
        let queued = f.call(&control, Purpose::ApiIngress, "queued");
        let queued = nabedra_rt::task::spawn(queued);
        entered(1).await;
        assert_eq!(f.client.lane_refusals(), refused, "nothing refused");
        f.endpoint.hold.store(false, Ordering::Release);
        f.endpoint.release.add_permits(direct + 1);
        echoed(queued.await.unwrap(), "queued");
        for call in calls {
            timeout(WAIT, call)
                .await
                .expect("every direct request completes")
                .unwrap()
                .expect("no direct request is refused");
        }
        assert_eq!(f.client.lane_refusals(), refused, "nothing refused");
        let hold = f.client.hold_outgoing_for_test(TrafficClass::ApiIngress);
        let Err(error @ RequestError::OutgoingLaneFull { .. }) = f
            .client
            .reserve_outgoing(
                &f.data_group,
                f.target.worker_id,
                Purpose::ApiIngress,
                nabedra_rt::time::now(),
            )
            .await
        else {
            panic!("expected a local lane refusal at the deadline");
        };
        let error = ProtocolError::from(error);
        let lane = if wide { "direct" } else { "ingress" };
        assert_eq!(
            error.message,
            format!("worker outgoing lane full (ApiIngress/{lane})")
        );
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);
        drop(hold);
        f.stop().await;
    }
}

/// RV-FINAL L1, C07, M02, LIVE1 (5 peers): caller slots of one class are
/// capped across peer lanes at `OUTGOING_CLASS_LANES` lanes' worth. A paused
/// peer fills only its own lane, so the others keep sending (LIVE1); four
/// full lanes reach the class cap, and a fifth peer's empty lane is refused
/// locally instead of adding a lane of in-flight calls per peer. Other
/// classes (Control consensus) are not affected.
async fn c07_m02_rv_final_l1_class_in_flight_is_capped_across_peer_lanes() {
    let config = TransportConfig::default();
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        config.clone(),
        TransportConfig::default(),
        false,
    )
    .await;
    let transport = &fixture.caller_transport;
    let client = &fixture.client;
    let class = TrafficClass::DataConsensus;
    let lane = config.handler_slots[class as usize];
    let cap = lane * OUTGOING_CLASS_LANES;
    let targets: Vec<Uuid> = (0..=OUTGOING_CLASS_LANES)
        .map(|_| nabedra_rt::ids::uuid_v4())
        .collect();
    let mut held = Vec::new();
    // A paused peer holds only its own lane.
    for _ in 0..lane {
        held.push(client.try_outgoing(targets[0], class).unwrap());
    }
    assert!(matches!(
        client.try_outgoing(targets[0], class),
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    // The other peers still send, up to the class cap.
    for target in &targets[1..OUTGOING_CLASS_LANES] {
        for _ in 0..lane {
            held.push(client.try_outgoing(*target, class).unwrap());
        }
    }
    assert_eq!(held.len(), cap);
    assert_eq!(
        transport.common.outgoing_class[class as usize].available_permits(),
        0
    );
    // A fifth peer's lane is empty, but the class is at its cap.
    let fifth = targets[OUTGOING_CLASS_LANES];
    assert!(matches!(
        client.try_outgoing(fifth, class),
        Err(RequestError::OutgoingLaneFull { .. })
    ));
    // Control traffic is never starved by Data consensus.
    let control = client
        .try_outgoing(fifth, TrafficClass::ControlConsensus)
        .unwrap();
    echoed(
        fixture
            .call(&fixture.group, Purpose::RaftProgress, "protected progress")
            .await,
        "protected progress",
    );
    // A finished call on any lane admits the fifth peer.
    held.pop();
    let admitted = client.try_outgoing(fifth, class).unwrap();
    drop((control, admitted, held));
    assert_eq!(
        transport.common.outgoing_class[class as usize].available_permits(),
        cap
    );
    fixture.stop().await;
}

/// D10/O05: before writing a reply, the writer returns its handler slot so
/// another request can enter the same lane even if the first reply is delayed.
async fn d10_o05_reply_frees_its_handler_slot_before_its_bytes_are_written() {
    let class = TrafficClass::DataConsensus as usize;
    let mut receiver_config = TransportConfig::default();
    receiver_config.handler_slots[class] = 1;
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        receiver_config,
        false,
    )
    .await;
    let hold = &fixture.receiver.common.hold_replies_after_release;
    hold.send_replace(true);
    let first = nabedra_rt::task::spawn(fixture.call(&fixture.data_group, Purpose::Raft, "first"));
    timeout(WAIT, async {
        eventually(
            || {
                fixture.endpoint.handled.load(Ordering::Acquire) == 1
                    && fixture.receiver.common.handlers[class].available_permits() == 1
            },
            "the first handler releases its slot before the writer proceeds",
        )
        .await;
        assert!(!first.is_finished());
        let second =
            nabedra_rt::task::spawn(fixture.call(&fixture.data_group, Purpose::Raft, "second"));
        eventually(
            || fixture.endpoint.handled.load(Ordering::Acquire) == 2,
            "the second request enters the same lane while the first reply waits",
        )
        .await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        hold.send_replace(false);
        echoed(first.await.unwrap(), "first");
        echoed(second.await.unwrap(), "second");
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// REP6/O17 (transport component): a stored revocation closes an established
/// session through the directory watch, and the revoked identity cannot use a
/// new session.
async fn revoked_session_closes() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    // Exercise the watch path without the periodic check also closing the session.
    fixture
        .receiver
        .common
        .suspend_directory_poll_for_test
        .store(true, Ordering::Relaxed);
    timeout(WAIT, async {
        echoed(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "before revoke")
                .await,
            "before revoke",
        );
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
        let revoked = WorkerIdentity::from(&fixture.caller);
        fixture.peers.revoke(revoked);
        eventually(
            || fixture.client.live_sessions_for_test() == 0,
            "receiver closes its authenticated session with the revoked worker",
        )
        .await;
        assert_eq!(fixture.receiver.revoked_session_closes(), 1);
        assert!(matches!(
            fixture
                .call(&fixture.group, Purpose::RaftProgress, "after revoke")
                .await,
            Err(RequestError::Unreachable)
        ));
        assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// A CA-signed peer with a different fingerprint must fail at the transport
/// pin check before a request can reach the endpoint.
async fn peer_certificate_pin_rejects_other_ca_signed_certificate() {
    let (fixture, _) = Fixture::start(
        Release::Current,
        Release::Current,
        TransportConfig::default(),
        TransportConfig::default(),
        false,
    )
    .await;
    let wrong = fixture.other.transport.common.local.certificate_sha256;
    let mut pinned = fixture.target.clone();
    // The real listener presents its own valid TLS certificate. Change its
    // cached pin so it differs. SimNet can change the certificate it presents.
    #[cfg(feature = "sim")]
    let simulated = sim::world();
    #[cfg(not(feature = "sim"))]
    let presenting_different_certificate = false;
    #[cfg(feature = "sim")]
    let presenting_different_certificate = if let Some(world) = &simulated {
        world.override_certificate(RECEIVER_NODE, Some(*wrong.as_bytes()));
        true
    } else {
        false
    };
    if !presenting_different_certificate {
        pinned.certificate_sha256 = wrong;
        fixture.peers.install(pinned.clone());
    }
    let result = fixture
        .caller()
        .call(
            &pinned,
            &fixture.group,
            Purpose::RaftProgress,
            "bad pin",
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(RequestError::Protocol(error))
            if error.code == ErrorCode::ValidationError && error.outcome == Outcome::NotApplied
                && error.message == "target certificate or ALPN differs"
    ));
    assert_eq!(fixture.client.live_sessions_for_test(), 0);
    assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 0);

    #[cfg(feature = "sim")]
    if let Some(world) = simulated {
        world.override_certificate(RECEIVER_NODE, None);
    }
    fixture.peers.install(fixture.target.clone());
    echoed(
        fixture
            .call(&fixture.group, Purpose::RaftProgress, "good pin")
            .await,
        "good pin",
    );
    assert_eq!(fixture.endpoint.handled.load(Ordering::Acquire), 1);
    fixture.stop().await;
}

macro_rules! parity_cases {
    ($($case:ident => $session:literal),+ $(,)?) => {
        mod real {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                async fn $case() {
                    super::$case().await;
                }
            )+
        }

        #[cfg(feature = "sim")]
        mod sim_parity {
            $(
                #[test]
                fn $case() {
                    super::run_parity(super::$case, $session);
                }
            )+
        }
    };
}

parity_cases!(
    d10_o05_one_session_per_lane_carries_many_calls => true,
    d10_dropped_session_mid_stream_is_unknown_and_next_call_reconnects => true,
    d10_c07_o05_exhausted_lane_overloads_while_raft_progress_advances => true,
    d10_old_one_shot_peer_interoperates_in_both_directions => false,
    cap1_d10_o02_sessions_form_with_older_builds_after_otp3_is_retired => true,
    d10_session_closes_when_peer_incarnation_leaves_directory => true,
    d10_c07_full_session_pools_reject_typed_and_keep_raft_progress => true,
    d10_o05_reply_frees_its_handler_slot_before_its_bytes_are_written => true,
    c07_m02_rv_final_l1_class_in_flight_is_capped_across_peer_lanes => true,
    revoked_session_closes => true,
    peer_certificate_pin_rejects_other_ca_signed_certificate => true,
);

#[cfg(feature = "sim")]
fn run_parity<F, Fut>(case: F, expects_session: bool)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    use crate::{FrameKind, StreamFrame};
    use sha2::{Digest as _, Sha256};
    use sim::net::{Direction, Envelope, Fate, Kind, Planner};

    #[derive(Default)]
    struct Trace {
        messages: u64,
        requests: u64,
        replies: u64,
        hash: Sha256,
    }

    struct Recorder(Arc<Mutex<Trace>>);
    impl Planner for Recorder {
        fn message(&mut self, envelope: &Envelope<'_>) -> Fate {
            let mut trace = self.0.lock().unwrap();
            trace.messages += 1;
            trace.hash.update(envelope.conn.0.to_le_bytes());
            trace.hash.update(envelope.seq.to_le_bytes());
            trace.hash.update(envelope.from.unwrap_or(0).to_le_bytes());
            trace.hash.update(envelope.to.unwrap_or(0).to_le_bytes());
            trace
                .hash
                .update([envelope.kind as u8, envelope.direction as u8]);
            trace
                .hash
                .update((envelope.bytes.len() as u64).to_le_bytes());
            trace.hash.update(envelope.bytes);
            if envelope.kind == Kind::Data
                && let Some(frame) = StreamFrame::from_message(envelope.bytes)
            {
                let direction = match frame.kind() {
                    FrameKind::Request => {
                        trace.requests += 1;
                        Some(Direction::ToServer)
                    }
                    FrameKind::Reply => {
                        trace.replies += 1;
                        Some(Direction::ToClient)
                    }
                    FrameKind::Ping => Some(Direction::ToServer),
                    FrameKind::Pong => Some(Direction::ToClient),
                    FrameKind::Cancel => None,
                };
                if let Some(direction) = direction {
                    assert_eq!(envelope.direction, direction);
                }
                trace.hash.update(frame.stream_id().to_le_bytes());
            }
            Fate::NOW
        }
    }

    let seeded = sim::TOKIO_RNG_SEEDED;
    assert!(
        seeded,
        "transport parity requires RUSTFLAGS='--cfg tokio_unstable'"
    );

    // This same case also runs in a real Tokio runtime, with no World installed.
    // Initialize the shared TLS material before generating any simulated IDs.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(case());

    const SEED: u64 = 0x4e41_4245_4452_4108;
    let run = || {
        sim::run(SEED, |world| {
            let case = &case;
            async move {
                for node in [CALLER_NODE, RECEIVER_NODE, OTHER_NODE] {
                    world.add_node(node, sim::NodeClock::default());
                }
                let trace = Arc::new(Mutex::new(Trace::default()));
                world.set_net_planner(Recorder(trace.clone()));
                sim::in_node(CALLER_NODE, case()).await;
                let trace = trace.lock().unwrap();
                assert!(
                    trace.messages > 0,
                    "the case must exercise the simulated backend"
                );
                if expects_session {
                    assert!(
                        trace.requests > 0 && trace.replies > 0,
                        "whole request and reply frames"
                    );
                } else {
                    assert_eq!(
                        (trace.requests, trace.replies),
                        (0, 0),
                        "only one-shot messages"
                    );
                }
                <[u8; 32]>::from(trace.hash.clone().finalize())
            }
        })
    };
    assert_eq!(run(), run(), "network trace diverged for seed {SEED}");
}
