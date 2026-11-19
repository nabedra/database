//! D10/O05/C07: persistent peer sessions over real mTLS and ephemeral ports.
use super::*;
use tokio::{sync::watch, task::JoinHandle};

const WAIT: Duration = Duration::from_secs(20);

/// Echoes the request header. `held` callers block until released.
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

/// A TCP relay whose live connections can be severed, to drop a session
/// mid-stream without touching either endpoint.
struct Relay {
    address: SocketAddr,
    generation: watch::Sender<u64>,
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    task: JoinHandle<()>,
}
impl Relay {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (generation, watcher) = watch::channel(0u64);
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task = tokio::spawn({
            let accepted = accepted.clone();
            async move {
                loop {
                    let Ok((mut inbound, _)) = listener.accept().await else {
                        return;
                    };
                    accepted.fetch_add(1, Ordering::AcqRel);
                    let mut watcher = watcher.clone();
                    let started = *watcher.borrow_and_update();
                    tokio::spawn(async move {
                        let Ok(mut outbound) = TcpStream::connect(target).await else {
                            return;
                        };
                        tokio::select! {
                            _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
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
            accepted,
            task,
        }
    }
    fn sever(&self) {
        self.generation.send_modify(|generation| *generation += 1);
    }
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    _temp: TestDirectoryPath,
    peers: Arc<TestDirectory>,
    caller: WorkerEnrollment,
    target: WorkerEnrollment,
    client: WorkerClient,
    caller_transport: WorkerTransport,
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
        let temp =
            TestDirectoryPath(std::env::temp_dir().join(format!("sessions-{}", Uuid::new_v4())));
        std::fs::create_dir(&temp.0).unwrap();
        let tls = tls::certificates(&temp.0, 2);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap();
        let relay = if relay {
            Some(Relay::start(listen).await)
        } else {
            None
        };
        let reach = relay.as_ref().map_or(listen, |relay| relay.address);
        let enrollment = |index: usize, endpoint: String| WorkerEnrollment {
            worker_id: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            enrollment_revision: 1,
            certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
            endpoint: Text::new(endpoint).unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
        };
        let caller = enrollment(0, "127.0.0.1:1".to_owned());
        let target = enrollment(1, reach.to_string());
        let peers = Arc::new(TestDirectory::default());
        peers.install(caller.clone());
        peers.install(target.clone());
        let transport = |local: &WorkerEnrollment, node, index: usize, release, config| {
            let shared = WorkerSharedResources::new(
                node,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap();
            let registry = Arc::new(EndpointRegistry::new(shared.clone()));
            let build = match release {
                Release::Current => WorkerTransport::new,
                Release::OneShotOnly => WorkerTransport::one_shot_for_test,
            };
            build(local, &tls[index], shared, peers.clone(), registry, config).unwrap()
        };
        let group = GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("sessions").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        let data_group = GroupIdentity {
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: Uuid::new_v4(),
                shard_id: Uuid::new_v4(),
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
        let caller_transport = transport(&caller, 1, 0, caller_release, caller_config);
        let client = caller_transport.client();
        let (stop, shutdown) = oneshot::channel();
        let worker = tokio::spawn(receiver.run(listener, async {
            let _ = shutdown.await;
        }));
        (
            Self {
                _temp: temp,
                peers,
                caller,
                target,
                client,
                caller_transport,
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
    fn call(
        &self,
        group: &GroupIdentity,
        purpose: Purpose,
        text: &str,
    ) -> impl Future<Output = Result<ReceivedReply, RequestError>> + use<> {
        let class = traffic(group, purpose).unwrap().byte_class();
        let header = self
            .caller_transport
            .common
            .shared
            .encode_in(class, &text, 1024)
            .unwrap();
        let client = self.client.clone();
        let group = group.clone();
        let target = WorkerIdentity::from(&self.target);
        async move {
            client
                .request_classified(&group, target, purpose, header, [None, None, None], 0)
                .await
        }
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
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// D10/O05: many sequential and concurrent calls share one TLS handshake per
/// lane. Raft append and progress lanes each get their own session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
            concurrent.push(tokio::spawn(async move { (call.await, text) }));
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        let in_flight = tokio::spawn(fixture.call(&fixture.group, Purpose::RaftProgress, "lost"));
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
        assert_eq!(relay.accepted.load(Ordering::Acquire), 2);
        assert_eq!(fixture.client.live_sessions_for_test(), 1);
    })
    .await
    .unwrap();
    fixture.stop().await;
}

/// D10/C07/O05: a saturated Data consensus lane returns overload errors on
/// both sides while the Raft progress lane keeps answering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        let held = tokio::spawn(fixture.call(&fixture.data_group, Purpose::Raft, "held"));
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
        let rest = fixture.client.common.outgoing[data]
            .clone()
            .try_acquire_many_owned(caller_config.handler_slots[data] as u32 - 1)
            .unwrap();
        assert!(matches!(
            fixture
                .call(&fixture.data_group, Purpose::Raft, "caller-full")
                .await,
            Err(RequestError::OutgoingLaneFull)
        ));
        let error = ProtocolError::from(RequestError::OutgoingLaneFull);
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert_eq!(error.outcome, Outcome::NotApplied);

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
                concurrent.push(tokio::spawn(async move { (call.await, text) }));
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

/// D10: a session closes when either side's peer incarnation leaves the local
/// directory, and a stale incarnation cannot reuse the authenticated session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        replaced.incarnation = Uuid::new_v4();
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
        moved.incarnation = Uuid::new_v4();
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
