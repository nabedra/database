use std::{
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use nabedra_oltp_types::types::{Digest, GroupIdentity, GroupKind, Text, WorkerEnrollment};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_resources::{
    config::WorkerResources,
    memory::{ByteClass, ChargedBytes, ResourceOwner, SharedResourceConfig, WorkerMemory},
    scheduling::ResourceScheduler,
};
use nabedra_rt::sim::{self, NodeClock};
use nabedra_transport::{
    AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture,
    EndpointRegistry, Purpose, ReceivePlan, ReceivedReply, RequestError, TransportConfig,
    WorkerClient, backend,
};
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

use super::*;

#[path = "../../../transport/src/transport/tests/tls.rs"]
mod certificates;

use crate::tls;

const CLIENT: u64 = 1;
const SERVER: u64 = 2;

fn run_replays(name: &str, scenario: impl AsyncFn(&Arc<World>, &SimNet, u64)) {
    let seeded = sim::TOKIO_RNG_SEEDED;
    assert!(seeded, "set RUSTFLAGS='--cfg tokio_unstable'");
    let count: u64 = std::env::var("NABEDRA_CASES").map_or(50, |value| value.parse().unwrap());
    assert!(count > 0);
    let first: u64 = std::env::var("NABEDRA_SEED").map_or(1, |value| value.parse().unwrap());
    let tls = fixture_tls();
    assert_eq!(tls.len(), 2);
    for seed in first..first.checked_add(count).unwrap() {
        let run = || {
            let scenario = &scenario;
            sim::run(seed, |world| async move {
                for node in [CLIENT, SERVER] {
                    world.add_node(node, NodeClock::default());
                }
                let net = SimNet::install(&world, seed, [CLIENT, SERVER]);
                timeout(scenario(&world, &net, seed)).await;
                let trace = net.trace();
                assert!(trace.events() > 0, "missing trace for {name} seed {seed}");
                trace.hash()
            })
        };
        let hash = run();
        assert_eq!(hash, run(), "{name}: replay diverged at seed {seed}");
        println!("sim-net {name} seed={seed} replay={hash:016x}");
    }
    println!("sim-net {name}: {count}/{count} passing with matching replay hashes");
}

async fn timeout<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("simulated network scenario deadline")
}

fn fixture_tls() -> &'static [tls::TlsMaterial] {
    static MATERIAL: OnceLock<Vec<tls::TlsMaterial>> = OnceLock::new();
    MATERIAL.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("nabedra-sim-net-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&dir).unwrap();
        let material = certificates::certificates(&dir, 2);
        std::fs::remove_dir_all(dir).unwrap();
        material
    })
}

struct Memory(Arc<WorkerMemory>);

impl Memory {
    fn new(node: u64) -> Arc<Self> {
        let limits = WorkerResources::default();
        let scheduler = ResourceScheduler::new(vec![(node, limits)]).unwrap();
        let owner = Arc::new(Self(Arc::new(
            WorkerMemory::new(
                SharedResourceConfig::default(),
                scheduler,
                node,
                limits.task_memory_limit_bytes as usize,
                0,
            )
            .unwrap(),
        )));
        let erased: Arc<dyn std::any::Any + Send + Sync> = owner.clone();
        owner.0.bind_owner(Arc::downgrade(&erased));
        owner
    }
}

impl ResourceOwner for Memory {
    fn worker_memory(&self) -> &Arc<WorkerMemory> {
        &self.0
    }

    fn bind_worker(self: &Arc<Self>, _: Uuid) -> Result<(), ProtocolError> {
        Ok(())
    }
}

#[derive(Default)]
struct Echo(AtomicUsize);

impl Endpoint for Echo {
    fn preflight<'a>(
        &'a self,
        _: &'a AuthenticatedSession,
        _: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async {
            Ok(ReceivePlan {
                class: nabedra_transport::TrafficClass::RaftProgress,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        _: AuthenticatedSession,
        header: ChargedBytes,
        _: ChargedSections,
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

struct WorkerFixture {
    _client_transport: nabedra_transport::WorkerTransport,
    client: WorkerClient,
    target: WorkerEnrollment,
    group: GroupIdentity,
    memory: Arc<Memory>,
    echo: Arc<Echo>,
    stop: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), ProtocolError>>,
}

impl WorkerFixture {
    async fn start(world: &Arc<World>, protocol: PeerProtocol) -> Self {
        let listen = sim::in_node(
            SERVER,
            backend::current().bind("127.0.0.1:0".parse().unwrap()),
        )
        .await
        .unwrap();
        let address = listen.local_addr().unwrap();
        let directory = Arc::new(PeerDirectory::default());
        let tls = fixture_tls();
        let enroll = |index: usize, address: SocketAddr, node: u64| {
            let peer = WorkerEnrollment {
                worker_id: Uuid::from_u128(u128::from(node)),
                incarnation: Uuid::from_u128(100 + u128::from(node)),
                enrollment_revision: 1,
                certificate_sha256: Digest::parse(tls[index].certificate_sha256()).unwrap(),
                endpoint: Text::new(address.to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
            };
            directory.register(world, node, peer.clone());
            peer
        };
        let caller = enroll(0, "10.0.0.2:1".parse().unwrap(), CLIENT);
        let target = enroll(1, address, SERVER);
        let group = GroupIdentity {
            cluster_id: Uuid::from_u128(200),
            group_id: Uuid::from_u128(300),
            group_format_version: 1,
            database: Text::try_from("sim").unwrap(),
            kind: GroupKind::Catalog,
        };
        let config = TransportConfig {
            deadline: Duration::from_millis(500),
            ..Default::default()
        };
        let memory = Memory::new(CLIENT);
        let client_registry = Arc::new(EndpointRegistry::new(memory.clone()));
        let caller = PeerProtocol::Current
            .transport(
                &caller,
                &tls[0],
                memory.clone(),
                directory.clone(),
                client_registry,
                config.clone(),
            )
            .unwrap();
        let server_memory = Memory::new(SERVER);
        let server_registry = Arc::new(EndpointRegistry::new(server_memory.clone()));
        let echo = Arc::new(Echo::default());
        server_registry
            .reserve(group.clone())
            .unwrap()
            .bind(Purpose::RaftProgress, echo.clone())
            .unwrap();
        let receiver = protocol
            .transport(
                &target,
                &tls[1],
                server_memory,
                directory,
                server_registry,
                config,
            )
            .unwrap();
        let (stop, shutdown) = oneshot::channel();
        let server = world.spawn_node(
            SERVER,
            Arc::new(receiver).run(listen, async {
                let _ = shutdown.await;
            }),
        );
        Self {
            client: caller.client(),
            _client_transport: caller,
            target,
            group,
            memory,
            echo,
            stop,
            server,
        }
    }

    fn call(
        &self,
        value: u64,
    ) -> impl Future<Output = Result<ReceivedReply, RequestError>> + use<> {
        let header = self
            .memory
            .0
            .encode_in(ByteClass::Progress, &value, 1024)
            .unwrap();
        let group = self.group.clone();
        let target = self.target.worker_id;
        let client = self.client.clone();
        sim::in_node(CLIENT, async move {
            client
                .request_classified(
                    &group,
                    target,
                    Purpose::RaftProgress,
                    header,
                    [None, None, None],
                    0,
                )
                .await
        })
    }

    async fn stop(self) {
        let _ = self.stop.send(());
        self.server.await.unwrap().unwrap();
    }
}

async fn ok(future: impl Future<Output = Result<ReceivedReply, RequestError>>, value: u64) {
    let reply = future.await.unwrap();
    assert_eq!(reply.header.as_slice(), value.to_string().as_bytes());
}

async fn cut_case(
    world: &Arc<World>,
    net: &SimNet,
    kind: FrameKind,
    direction: Direction,
    applied: usize,
) {
    let fixture = WorkerFixture::start(world, PeerProtocol::Current).await;
    ok(fixture.call(0), 0).await;

    let mut armed = true;
    net.set_frame_profile(move |frame| {
        if armed && frame.kind == kind && frame.direction == direction {
            armed = false;
            Action::Reset
        } else {
            Action::Pass
        }
    });
    let error = match fixture.call(1).await {
        Err(error) => ProtocolError::from(error),
        Ok(_) => panic!("cut call must fail"),
    };
    assert_eq!(error.code, ErrorCode::StorageUnavailable);
    // The caller cannot prove whether a written request reached the handler.
    assert_eq!(error.outcome, Outcome::Unknown);
    assert_eq!(fixture.echo.0.load(Ordering::Relaxed), applied);
    ok(fixture.call(2), 2).await;
    fixture.stop().await;
}

#[test]
fn request_cut() {
    run_replays("request-cut", async |world, net, _| {
        cut_case(world, net, FrameKind::Request, Direction::ToServer, 1).await;
    });
}

#[test]
fn reply_cut() {
    run_replays("reply-cut", async |world, net, _| {
        cut_case(world, net, FrameKind::Reply, Direction::ToClient, 2).await;
    });
}

#[test]
fn stream_delay() {
    run_replays("stream-delay", async |world, net, seed| {
        let fixture = WorkerFixture::start(world, PeerProtocol::Current).await;
        ok(fixture.call(0), 0).await;

        let planned = Arc::new(Notify::new());
        let delay = Duration::from_millis(30 + seed % 17);
        let mut first = true;
        net.set_frame_profile({
            let planned = planned.clone();
            move |frame| {
                if first && frame.kind == FrameKind::Reply {
                    first = false;
                    planned.notify_one();
                    Action::Delay(delay)
                } else {
                    Action::Pass
                }
            }
        });

        let slow = world.spawn_node(CLIENT, fixture.call(1));
        planned.notified().await;
        let start = Instant::now();
        ok(fixture.call(2), 2).await;
        ok(fixture.call(3), 3).await;
        assert!(start.elapsed() < delay);
        assert!(!slow.is_finished());
        let reply = slow.await.unwrap().unwrap();
        assert_eq!(reply.header.as_slice(), b"1");
        assert!(start.elapsed() >= delay);
        assert_eq!(world.connections().len(), 1, "calls must share a session");
        fixture.stop().await;
    });
}

#[test]
fn fallback_only() {
    run_replays("fallback-only", async |world, net, _| {
        net.set_frame_profile(|frame| panic!("fallback sent a multiplexed frame: {frame:?}"));
        let fixture = WorkerFixture::start(world, PeerProtocol::OneShot).await;
        ok(fixture.call(1), 1).await;
        assert_eq!(fixture.echo.0.load(Ordering::Relaxed), 1);
        assert!(
            net.trace()
                .tail()
                .iter()
                .any(|line| line.contains("OneShot"))
        );
        fixture.stop().await;
    });
}
