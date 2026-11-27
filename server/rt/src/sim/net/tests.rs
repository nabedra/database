//! Simulated network tests (fault model). Each one fails when the property it names is
//! broken.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    ConnId, Dial, Direction, EXTERNAL_IP, FIRST_EPHEMERAL_PORT, Fate, Handshake, Kind, Pending,
    Planner, SimListener, SimStream, Target, dial, node_ip, resolve,
};
use crate::net::{AnyConnector, AnyListener, Conn, Connector, Established};
use crate::sim::{NodeClock, SplitMix64, World, in_node, run};
use crate::time;

const OTP4: &[u8] = b"nabedra-otp/4";
const OTP2: &[u8] = b"nabedra-otp/2";
const WORKER1: &[u8] = b"nabedra-oltp-worker/1";
const FULL: &[&[u8]] = &[OTP4, OTP2, WORKER1];
const ONE_SHOT: &[&[u8]] = &[WORKER1];

fn digest(node: u64) -> [u8; 32] {
    [node as u8; 32]
}

fn any(port: u16) -> SocketAddr {
    SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port)
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Adds `nodes`, each with certificate digest `[n; 32]` and host name `node<n>`.
fn nodes(world: &Arc<World>, nodes: &[u64]) {
    for &node in nodes {
        world.add_node(node, NodeClock::default());
        world.set_certificate(node, digest(node));
        world.register_host(&format!("node{node}"), node);
    }
}

type Side = io::Result<(SimStream, Handshake)>;

/// A connection from `client` to a fresh listener on `server`, both sides handshaking.
async fn pair(
    world: &Arc<World>,
    client: u64,
    server: u64,
    offer: &'static [&'static [u8]],
    protocols: &'static [&'static [u8]],
) -> (Side, Side) {
    let listener = world.listen(server, any(0)).unwrap();
    let addr = listener.local_addr();
    let accepted = world.spawn_node(server, async move {
        let (pending, _) = listener.accept().await?;
        pending.server_handshake(protocols).await
    });
    let dialed = in_node(client, async move {
        dial(Target::Addr(addr))
            .await?
            .client_handshake(offer)
            .await
    })
    .await;
    (dialed, accepted.await.unwrap())
}

async fn streams(world: &Arc<World>, client: u64, server: u64) -> (SimStream, SimStream) {
    let (dialed, accepted) = pair(world, client, server, FULL, FULL).await;
    (dialed.unwrap().0, accepted.unwrap().0)
}

async fn send(stream: &mut SimStream, bytes: &[u8]) -> io::Result<()> {
    stream.write_all(bytes).await?;
    stream.flush().await
}

async fn read_some(stream: &mut SimStream, within: Duration) -> Option<io::Result<Vec<u8>>> {
    let mut buf = [0; 64];
    match time::timeout(within, stream.read(&mut buf)).await {
        Ok(Ok(n)) => Some(Ok(buf[..n].to_vec())),
        Ok(Err(error)) => Some(Err(error)),
        Err(_) => None,
    }
}

/// Every planned message, as `(conn, direction, from, to, seq, kind, bytes)`.
type Record = (
    ConnId,
    Direction,
    Option<u64>,
    Option<u64>,
    u64,
    Kind,
    Vec<u8>,
);

#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<Record>>>);

impl Log {
    fn take(&self) -> Vec<Record> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

/// Records each message, then applies `rule`.
struct Recorder<F> {
    log: Log,
    rule: F,
}

impl<F: FnMut(&super::Envelope<'_>) -> Fate + Send + 'static> Planner for Recorder<F> {
    fn message(&mut self, envelope: &super::Envelope<'_>) -> Fate {
        self.log.0.lock().unwrap().push((
            envelope.conn,
            envelope.direction,
            envelope.from,
            envelope.to,
            envelope.seq,
            envelope.kind,
            envelope.bytes.to_vec(),
        ));
        (self.rule)(envelope)
    }
}

fn record(
    world: &Arc<World>,
    rule: impl FnMut(&super::Envelope<'_>) -> Fate + Send + 'static,
) -> Log {
    let log = Log::default();
    world.set_net_planner(Recorder {
        log: log.clone(),
        rule,
    });
    log
}

#[test]
fn handshake_negotiates_alpn_and_exchanges_certificate_digests() {
    run(1, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = in_node(1, async { SimListener::bind(any(7000)) })
            .await
            .unwrap();
        assert_eq!(listener.local_addr(), SocketAddr::new(node_ip(1), 7000));
        let server = world.spawn_node(1, async move {
            let (pending, peer) = listener.accept().await.unwrap();
            assert_eq!(peer.ip(), node_ip(2));
            pending.server_handshake(FULL).await
        });
        let (client, seen_by_client) = in_node(2, async {
            dial(Target::HostPort("node1", 7000))
                .await
                .unwrap()
                .client_handshake(&[OTP2, WORKER1])
                .await
        })
        .await
        .unwrap();
        let (server, seen_by_server) = server.await.unwrap().unwrap();
        // Each side receives the other's identity as a value; a zeroed digest fails here.
        assert_eq!(
            seen_by_client,
            Handshake {
                alpn: Some(OTP2.to_vec()),
                peer_certificate_sha256: digest(1),
            }
        );
        assert_eq!(
            seen_by_server,
            Handshake {
                alpn: Some(OTP2.to_vec()),
                peer_certificate_sha256: digest(2),
            }
        );
        assert_eq!(client.peer_addr(), server.local_addr());
        assert_eq!(server.peer_addr(), client.local_addr());
        assert_eq!(client.local_addr().ip(), node_ip(2));
    });
}

#[test]
fn alpn_follows_the_server_preference_like_rustls() {
    run(2, |world| async move {
        nodes(&world, &[1, 2]);
        let cases = [
            (FULL, FULL, Some(OTP4)),
            // The server picks from its own list, whatever order the client offers.
            (&[WORKER1, OTP2], FULL, Some(OTP2)),
            // NABEDRA_BENCH_NO_DIRECT_ADMISSION offers no otp/4.
            (&[OTP2, WORKER1], FULL, Some(OTP2)),
            // D10: a full caller reaching the one-shot-only acceptor falls back.
            (FULL, ONE_SHOT, Some(WORKER1)),
            // An old one-shot peer reaching a full listener.
            (ONE_SHOT, FULL, Some(WORKER1)),
            (&[], FULL, None),
            (FULL, &[], None),
        ];
        for (offer, protocols, expected) in cases {
            let (dialed, accepted) = pair(&world, 2, 1, offer, protocols).await;
            let expected = expected.map(<[u8]>::to_vec);
            assert_eq!(dialed.unwrap().1.alpn, expected, "{offer:?} {protocols:?}");
            assert_eq!(
                accepted.unwrap().1.alpn,
                expected,
                "{offer:?} {protocols:?}"
            );
        }
        let (dialed, accepted) = pair(&world, 2, 1, &[OTP4], ONE_SHOT).await;
        assert_eq!(dialed.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(accepted.unwrap_err().kind(), io::ErrorKind::InvalidData);
    });
}

#[test]
fn both_sides_need_a_certificate() {
    run(3, |world| async move {
        nodes(&world, &[1]);
        world.add_node(3, NodeClock::default());
        let (dialed, accepted) = pair(&world, 3, 1, FULL, FULL).await;
        assert_eq!(
            accepted.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(dialed.unwrap_err().kind(), io::ErrorKind::InvalidData);
        let (dialed, accepted) = pair(&world, 1, 3, FULL, FULL).await;
        assert_eq!(
            accepted.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(dialed.unwrap_err().kind(), io::ErrorKind::InvalidData);
    });
}

#[test]
fn a_certificate_override_impersonates_another_node() {
    run(4, |world| async move {
        nodes(&world, &[1, 2, 3]);
        world.override_certificate(3, Some(digest(1)));
        let (_, accepted) = pair(&world, 3, 2, FULL, FULL).await;
        assert_eq!(accepted.unwrap().1.peer_certificate_sha256, digest(1));
        world.override_certificate(3, None);
        let (_, accepted) = pair(&world, 3, 2, FULL, FULL).await;
        assert_eq!(accepted.unwrap().1.peer_certificate_sha256, digest(3));
    });
}

#[test]
fn each_flush_is_one_message_with_its_stream_and_direction_visible() {
    run(5, |world| async move {
        nodes(&world, &[1, 2]);
        let log = record(&world, |envelope| {
            match envelope.kind {
                Kind::Handshake => assert_eq!(envelope.alpn, None),
                Kind::Data => assert_eq!(envelope.alpn, Some(OTP4)),
            }
            Fate::NOW
        });
        let (mut client, mut server) = streams(&world, 2, 1).await;
        let conn = client.id();
        assert_eq!(server.id(), conn);
        client.write_all(b"ab").await.unwrap();
        client.write_all(b"cd").await.unwrap();
        client.flush().await.unwrap();
        client.write_all(b"ef").await.unwrap();
        client.shutdown().await.unwrap();
        send(&mut server, b"reply").await.unwrap();
        let mut request = Vec::new();
        server.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"abcdef");
        let to_server = |seq, kind, bytes: &[u8]| {
            (
                conn,
                Direction::ToServer,
                Some(2),
                Some(1),
                seq,
                kind,
                bytes.to_vec(),
            )
        };
        let to_client = |seq, kind, bytes: &[u8]| {
            (
                conn,
                Direction::ToClient,
                Some(1),
                Some(2),
                seq,
                kind,
                bytes.to_vec(),
            )
        };
        assert_eq!(
            log.take(),
            [
                to_server(0, Kind::Handshake, b""),
                to_client(0, Kind::Handshake, b""),
                to_server(1, Kind::Data, b"abcd"),
                to_server(2, Kind::Data, b"ef"),
                to_client(1, Kind::Data, b"reply"),
            ]
        );
        assert_eq!(
            read_some(&mut client, ms(10)).await.unwrap().unwrap(),
            b"reply"
        );
    });
}

#[test]
fn a_dropped_reply_stalls_its_direction_until_the_connection_resets() {
    // The request and replies share a stream id in the first eight bytes of each frame.
    run(6, |world| async move {
        nodes(&world, &[1, 2]);
        let request_id = 23_u64.to_be_bytes();
        let other_id = 24_u64.to_be_bytes();
        let log = record(&world, move |envelope| match envelope {
            super::Envelope {
                direction: Direction::ToClient,
                bytes,
                ..
            } if bytes.starts_with(&request_id) => Fate::Drop,
            super::Envelope {
                direction: Direction::ToClient,
                bytes,
                ..
            } if bytes.starts_with(&other_id) && bytes.ends_with(b"reset") => Fate::Reset,
            _ => Fate::NOW,
        });
        let (mut client, mut server) = streams(&world, 2, 1).await;
        let request = [request_id.as_slice(), b"request"].concat();
        send(&mut client, &request).await.unwrap();
        assert_eq!(
            read_some(&mut server, ms(10)).await.unwrap().unwrap(),
            request
        );

        send(&mut server, &[request_id.as_slice(), b"reply"].concat())
            .await
            .unwrap();
        // A later frame on the same TCP direction is still planned, but cannot arrive.
        send(&mut server, &[other_id.as_slice(), b"reply"].concat())
            .await
            .unwrap();
        assert!(read_some(&mut client, ms(1_000)).await.is_none());
        assert_eq!(log.take().iter().filter(|r| r.5 == Kind::Data).count(), 3);

        // The reverse direction still works, and a later plan can reset the connection.
        send(&mut client, b"more").await.unwrap();
        assert_eq!(
            read_some(&mut server, ms(10)).await.unwrap().unwrap(),
            b"more"
        );
        let error = send(&mut server, &[other_id.as_slice(), b"reset"].concat())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        let error = read_some(&mut client, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    });
}

#[test]
fn a_planned_reset_fails_the_sender_and_the_reader() {
    run(7, |world| async move {
        nodes(&world, &[1, 2]);
        let (mut client, mut server) = streams(&world, 2, 1).await;
        send(&mut client, b"kept").await.unwrap();
        record(&world, |envelope| match envelope.kind {
            Kind::Data => Fate::Reset,
            Kind::Handshake => Fate::NOW,
        });
        let error = send(&mut client, b"boom").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        // Unread data is discarded, as the kernel does on a reset.
        let error = read_some(&mut server, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(world.connections()[0].reset);
    });
}

#[test]
fn delays_take_virtual_time_and_keep_order() {
    run(8, |world| async move {
        nodes(&world, &[1, 2]);
        let (dialed, accepted) = pair(&world, 2, 1, FULL, ONE_SHOT).await;
        for (mut client, mut server) in [(dialed.unwrap().0, accepted.unwrap().0), {
            let listener = world.listen(1, any(0)).unwrap();
            let addr = listener.local_addr();
            let client = in_node(2, dial(Target::Addr(addr))).await.unwrap().plain();
            let (server, _) = listener.accept().await.unwrap();
            (client, server.plain())
        }] {
            let mut delays = [ms(50), ms(0)].into_iter();
            record(&world, move |_| Fate::Deliver {
                delay: delays.next().unwrap(),
            });
            let start = tokio::time::Instant::now();
            send(&mut client, b"first").await.unwrap();
            send(&mut client, b"second").await.unwrap();
            let mut got = [0; 11];
            server.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"firstsecond");
            assert_eq!(start.elapsed(), ms(50));
        }

        record(&world, |_| Fate::Deliver { delay: ms(20) });
        let start = tokio::time::Instant::now();
        let (mut client, mut server) = streams(&world, 2, 1).await;
        assert_eq!(start.elapsed(), ms(40), "both handshake flights take time");
        record(&world, |envelope| Fate::Frame {
            stream: u64::from(envelope.bytes[0]),
            delay: if envelope.bytes == b"2slow" {
                ms(30)
            } else {
                ms(0)
            },
        });

        send(&mut client, b"1partial").await.unwrap();
        assert_eq!(server.read_u8().await.unwrap(), b'1');
        world.black_hole(Some(2), Some(1));
        send(&mut client, b"2slow").await.unwrap();
        send(&mut client, b"2later").await.unwrap();
        send(&mut client, b"3fast").await.unwrap();
        assert_eq!(
            read_some(&mut server, ms(10)).await.unwrap().unwrap(),
            b"partial"
        );
        assert!(read_some(&mut server, ms(10)).await.is_none());
        world.heal(Some(2), Some(1));
        let start = tokio::time::Instant::now();
        let mut got = [0; 16];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"3fast2slow2later");
        assert_eq!(start.elapsed(), ms(20));
    });
}

#[test]
fn a_crash_resets_connections_and_closes_listeners_before_tasks_abort() {
    run(9, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(7100)).unwrap();
        let addr = listener.local_addr();
        let acceptor = world.spawn_node(1, async move {
            loop {
                let (pending, _) = listener.accept().await?;
                // A connection task that writes and then waits.
                crate::task::spawn(async move {
                    let (mut stream, _) = pending.server_handshake(FULL).await?;
                    send(&mut stream, b"unread").await?;
                    std::future::pending::<io::Result<()>>().await
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
        let (mut client, _) = in_node(2, async {
            dial(Target::Addr(addr)).await?.client_handshake(FULL).await
        })
        .await
        .unwrap();
        tokio::time::sleep(ms(1)).await;
        assert_eq!(world.live_tasks(1), 2);
        assert_eq!(world.listening(1), [addr]);

        world.crash(1);
        let error = read_some(&mut client, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(acceptor.await.unwrap_err().is_cancelled());
        assert!(world.listening(1).is_empty());
        let refused = in_node(2, dial(Target::Addr(addr))).await.unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);

        world.restart(1);
        let _again = world.listen(1, any(7100)).unwrap();
        let pending = in_node(2, dial(Target::Addr(addr))).await.unwrap();
        assert_eq!(pending.peer_addr(), addr);
    });
}

#[test]
fn power_off_discards_frames_without_reserving_the_restarted_listener() {
    run(9, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(7100)).unwrap();
        let addr = listener.local_addr();
        let acceptor = world.spawn_node(1, async move {
            let (pending, _) = listener.accept().await?;
            let (mut stream, _) = pending.server_handshake(FULL).await?;
            send(&mut stream, b"unread").await?;
            std::future::pending::<io::Result<()>>().await
        });
        let (mut old, _) = in_node(2, async {
            dial(Target::Addr(addr)).await?.client_handshake(FULL).await
        })
        .await
        .unwrap();
        tokio::time::sleep(ms(1)).await;
        world.power_off(1);
        assert!(acceptor.await.unwrap_err().is_cancelled());
        send(&mut old, b"vanishes").await.unwrap();
        assert!(read_some(&mut old, ms(10)).await.is_none());
        let refused = in_node(2, dial(Target::Addr(addr))).await.unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);

        world.restart(1);
        let _listener = world.listen(1, any(7100)).unwrap();
        let new = in_node(2, dial(Target::Addr(addr))).await.unwrap();
        assert_eq!(new.peer_addr(), addr);
        assert!(read_some(&mut old, ms(10)).await.is_none());
    });
}

#[test]
fn a_crashed_client_resets_the_server_side() {
    run(10, |world| async move {
        nodes(&world, &[1, 2]);
        let (_client, mut server) = streams(&world, 2, 1).await;
        world.crash(2);
        let error = read_some(&mut server, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    });
}

#[test]
fn close_and_drop_end_the_stream_after_its_data() {
    run(11, |world| async move {
        nodes(&world, &[1, 2]);
        {
            let (mut client, mut server) = streams(&world, 2, 1).await;
            send(&mut client, b"queued").await.unwrap();
            world.close_connection(client.id());
            let mut got = Vec::new();
            server.read_to_end(&mut got).await.unwrap();
            assert_eq!(got, b"queued");
            let error = send(&mut server, b"x").await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        }
        assert!(world.connections().is_empty());

        let (mut client, mut server) = streams(&world, 2, 1).await;
        // Written but never flushed: the bytes still leave when the stream drops.
        client.write_all(b"last words").await.unwrap();
        drop(client);
        let mut got = Vec::new();
        server.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"last words");
        let error = send(&mut server, b"x").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        drop(server);
        assert!(world.connections().is_empty());
    });
}

#[test]
fn a_black_hole_holds_messages_and_dials_until_healed() {
    run(12, |world| async move {
        nodes(&world, &[1, 2]);
        let (mut client, mut server) = streams(&world, 2, 1).await;
        let mut delays = vec![ms(20)].into_iter();
        record(&world, move |_| Fate::Deliver {
            delay: delays.next().unwrap_or_default(),
        });
        // In flight when the hole opens: held too.
        send(&mut client, b"a").await.unwrap();
        world.black_hole(Some(2), Some(1));
        send(&mut client, b"b").await.unwrap();
        assert!(read_some(&mut server, ms(1_000)).await.is_none());
        // The other direction is open.
        send(&mut server, b"z").await.unwrap();
        assert_eq!(read_some(&mut client, ms(10)).await.unwrap().unwrap(), b"z");

        let listener = world.listen(1, any(0)).unwrap();
        let addr = listener.local_addr();
        let dialing = world.spawn_node(2, dial(Target::Addr(addr)));
        tokio::time::sleep(ms(1_000)).await;
        assert!(!dialing.is_finished());

        world.heal(Some(2), Some(1));
        let mut got = [0; 2];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"ab");
        assert!(dialing.await.unwrap().is_ok());

        // A frame handed to the reader stays available through a later partition.
        send(&mut client, b"cd").await.unwrap();
        let mut byte = [0];
        server.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"c");
        world.black_hole(Some(2), Some(1));
        assert_eq!(read_some(&mut server, ms(10)).await.unwrap().unwrap(), b"d");
    });
}

#[test]
fn a_black_hole_holds_shutdown_until_healed() {
    run(18, |world| async move {
        nodes(&world, &[1, 2]);
        let (mut client, mut server) = streams(&world, 2, 1).await;
        world.black_hole(Some(2), Some(1));
        client.shutdown().await.unwrap();

        assert!(read_some(&mut server, ms(1_000)).await.is_none());
        world.heal(Some(2), Some(1));
        assert_eq!(read_some(&mut server, ms(10)).await.unwrap().unwrap(), b"");

        world.black_hole(Some(2), Some(1));
        assert_eq!(read_some(&mut server, ms(10)).await.unwrap().unwrap(), b"");
    });
}

#[test]
fn dropping_a_listener_refuses_a_waiting_dial() {
    run(19, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(0)).unwrap();
        let addr = listener.local_addr();
        world.black_hole(Some(2), Some(1));
        let mut dialing = world.spawn_node(2, dial(Target::Addr(addr)));
        assert!(time::timeout(ms(1), &mut dialing).await.is_err());

        drop(listener);
        // A bind at the same address belongs to a new listener generation.
        let _replacement = world.listen(1, addr).unwrap();
        let error = time::timeout(ms(10), dialing)
            .await
            .expect("dial should wake when the listener closes")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    });
}

#[test]
fn the_planner_refuses_or_hangs_dials() {
    struct Dials(Dial);
    impl Planner for Dials {
        fn dial(&mut self, from: Option<u64>, to: Option<u64>) -> Dial {
            assert_eq!((from, to), (Some(2), Some(1)));
            self.0
        }
    }
    run(13, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(0)).unwrap();
        let addr = listener.local_addr();
        world.set_net_planner(Dials(Dial::Refuse));
        let error = in_node(2, dial(Target::Addr(addr))).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        world.set_net_planner(Dials(Dial::Hang));
        let dialing = world.spawn_node(2, dial(Target::Addr(addr)));
        tokio::time::sleep(ms(500)).await;
        assert!(!dialing.is_finished());
        world.set_net_planner(Dials(Dial::Connect));
        assert!(dialing.await.unwrap().is_ok());
    });
}

#[test]
fn addresses_are_per_node_and_deterministic() {
    run(14, |world| async move {
        nodes(&world, &[1, 2]);
        let first = world.listen(1, any(0)).unwrap();
        assert_eq!(
            first.local_addr(),
            SocketAddr::new(node_ip(1), FIRST_EPHEMERAL_PORT)
        );
        let error = world.listen(1, first.local_addr()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        let error = world
            .listen(1, SocketAddr::new(node_ip(2), 80))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
        // The same port on another node is free.
        let other = world.listen(2, any(first.local_addr().port())).unwrap();
        assert_eq!(other.local_addr().ip(), node_ip(2));

        // Loopback is the caller's own node.
        let own = in_node(1, dial(Target::HostPort("localhost", FIRST_EPHEMERAL_PORT)))
            .await
            .unwrap();
        assert_eq!(own.peer_addr(), first.local_addr());
        assert_eq!(own.local_addr().ip(), node_ip(1));
        let (accepted, peer) = first.accept().await.unwrap();
        assert_eq!(peer, own.local_addr());
        assert_eq!(accepted.peer_addr(), own.local_addr());

        assert_eq!(
            resolve("NODE2", 9).unwrap(),
            [SocketAddr::new(node_ip(2), 9)]
        );
        let error = resolve("nowhere", 9).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        // Code outside any node has the external address.
        let external = dial(Target::Addr(other.local_addr())).await.unwrap();
        assert_eq!(external.local_addr().ip(), EXTERNAL_IP);
        assert_eq!(world.connections().last().unwrap().client, None);
    });
}

#[test]
fn a_plain_side_and_a_handshaking_side_do_not_mix() {
    run(15, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(0)).unwrap();
        let addr = listener.local_addr();
        let accepted = world.spawn_node(1, async move {
            let (pending, _) = listener.accept().await?;
            pending.server_handshake(FULL).await.map(|_| ())
        });
        let mut plain = in_node(2, dial(Target::Addr(addr))).await.unwrap().plain();
        send(&mut plain, b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let error = accepted.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let error = read_some(&mut plain, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    });
}

#[test]
fn a_listener_dropped_before_accept_resets_its_backlog() {
    run(16, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(1, any(0)).unwrap();
        let pending: Pending = in_node(2, dial(Target::Addr(listener.local_addr())))
            .await
            .unwrap();
        drop(listener);
        let mut stream = pending.plain();
        let error = read_some(&mut stream, ms(10)).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    });
}

#[test]
fn ambient_types_use_the_simulation_inside_a_world() {
    // The HTTP path: plain streams from AnyListener and AnyConnector, as axum and an HTTP
    // client would use them.
    run(17, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = in_node(1, AnyListener::bind(any(8080))).await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(addr, SocketAddr::new(node_ip(1), 8080));
        let server = world.spawn_node(1, async move {
            let (conn, peer) = listener.accept().await?;
            assert!(matches!(conn, Conn::Sim(_)));
            assert_eq!(conn.peer_addr()?, peer);
            let mut stream = conn.plain();
            let mut request = [0; 16];
            stream.read_exact(&mut request).await?;
            assert_eq!(&request, b"GET / HTTP/1.1\r\n");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await?;
            stream.shutdown().await
        });
        let conn = in_node(2, AnyConnector.connect(Target::HostPort("node1", 8080)))
            .await
            .unwrap();
        let mut stream: Established<crate::net::TcpStream> = conn.plain();
        assert!(matches!(stream, Established::Sim(_)));
        stream.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        stream.flush().await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.ends_with("\r\n\r\nok"), "{response}");
        server.await.unwrap().unwrap();
        let connections = world.connections();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].client, Some(2));
        assert_eq!(connections[0].server, Some(1));
        assert_eq!(connections[0].server_addr, addr);
        drop(stream);
        assert!(world.connections().is_empty());
    });
}

#[test]
fn the_simulated_network_needs_an_installed_world() {
    let runtime = crate::sim::runtime().unwrap();
    let error = runtime.block_on(dial(Target::Addr(any(1)))).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(
        SimListener::bind(any(1)).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

/// A seeded workload: two clients, random delays, a crash. Returns everything observable.
fn traced_run(seed: u64) -> Vec<String> {
    run(seed, |world| async move {
        nodes(&world, &[1, 2, 3]);
        let mut rng = SplitMix64::new(world.seed());
        let log = record(&world, move |envelope| match envelope.kind {
            Kind::Data => Fate::Deliver {
                delay: ms(rng.below(30)),
            },
            Kind::Handshake => Fate::NOW,
        });
        let listener = world.listen(1, any(0)).unwrap();
        let addr = listener.local_addr();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let server_trace = trace.clone();
        let start = world.start();
        world.spawn_node(1, async move {
            loop {
                let Ok((pending, peer)) = listener.accept().await else {
                    return;
                };
                let trace = server_trace.clone();
                crate::task::spawn(async move {
                    let Ok((mut stream, _)) = pending.server_handshake(FULL).await else {
                        return;
                    };
                    let mut buf = [0; 8];
                    while let Ok(n) = stream.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        trace.lock().unwrap().push(format!(
                            "{:?} {peer} {:?}",
                            start.elapsed(),
                            &buf[..n]
                        ));
                        let _ = send(&mut stream, &buf[..n]).await;
                    }
                });
            }
        });
        let mut clients = Vec::new();
        for node in [2, 3] {
            clients.push(world.spawn_node(node, async move {
                let (mut stream, _) = dial(Target::Addr(addr))
                    .await?
                    .client_handshake(FULL)
                    .await?;
                for i in 0..5u8 {
                    send(&mut stream, &[node as u8, i]).await?;
                }
                let mut echoed = [0; 10];
                stream.read_exact(&mut echoed).await?;
                Ok::<_, io::Error>(echoed)
            }));
        }
        tokio::time::sleep(ms(40)).await;
        world.crash(1);
        let mut out: Vec<String> = Vec::new();
        for client in clients {
            out.push(format!("{:?}", client.await.unwrap().map_err(|e| e.kind())));
        }
        out.extend(trace.lock().unwrap().iter().cloned());
        out.extend(log.take().iter().map(|r| format!("{r:?}")));
        out
    })
}

#[test]
fn the_same_seed_gives_the_same_network_trace() {
    let first = traced_run(21);
    assert!(first.len() > 10, "{first:#?}");
    assert_eq!(first, traced_run(21));
    assert_ne!(first, traced_run(22));
}

/// The addresses a scan over the connections finds held. `gone` remembers the connections
/// whose client closed before they were accepted, also once they are accepted.
fn assert_held(world: &Arc<World>, gone: &mut std::collections::BTreeSet<u64>) {
    let net = super::net_of(world);
    let state = net.lock();
    let mut held = std::collections::BTreeMap::new();
    for (id, conn) in &state.conns {
        if !conn.open[0] && !conn.accepted {
            gone.insert(*id);
        }
        if conn.reset || conn.power_cut {
            continue;
        }
        for end in conn.ends.iter().skip(usize::from(gone.contains(id))) {
            *held.entry(end.addr).or_default() += 1;
        }
    }
    assert_eq!(state.ports.lock().ends, held);
}

fn lookups(world: &Arc<World>) -> u64 {
    super::net_of(world).lock().ports.lock().lookups
}

fn connections(world: &Arc<World>) -> usize {
    super::net_of(world).lock().conns.len()
}

async fn plain(addr: SocketAddr) -> SimStream {
    in_node(1, dial(Target::Addr(addr))).await.unwrap().plain()
}

/// The client has closed, but its request is still there, and then the end of the stream.
#[test]
fn a_connection_closed_before_it_is_accepted_arrives_with_its_data() {
    run(50, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(2, any(0)).unwrap();
        let addr = listener.local_addr();
        let mut client = plain(addr).await;
        send(&mut client, b"GET / HTTP/1.1").await.unwrap();
        let port = client.local_addr();
        drop(client);
        // A second one that says nothing.
        drop(plain(addr).await);
        time::sleep(ms(5_000)).await;

        let (pending, peer) = in_node(2, listener.accept()).await.unwrap();
        assert_eq!((peer, pending.peer_addr()), (port, port));
        let mut server = pending.plain();
        let mut text = Vec::new();
        server.read_to_end(&mut text).await.unwrap();
        assert_eq!(text, b"GET / HTTP/1.1");
        let (pending, _) = in_node(2, listener.accept()).await.unwrap();
        let mut rest = Vec::new();
        pending.plain().read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        drop(server);
        assert_eq!(connections(&world), 0);
        assert_held(&world, &mut Default::default());
    });
}

/// Only a client that closed before it was accepted gives its port back early.
#[test]
fn an_unaccepted_connection_frees_its_port_when_the_client_closes() {
    run(51, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(2, any(80)).unwrap();
        let addr = listener.local_addr();
        let taken = |port| {
            let error = world.listen(1, port).err();
            error.map(|error| error.kind()) == Some(io::ErrorKind::AddrInUse)
        };
        let waiting = plain(addr).await;
        let port = waiting.local_addr();
        assert!(taken(port));
        drop(waiting);
        assert!(!taken(port));

        // Accepted first: half closed, and the port stays taken until the server closes too.
        let client = plain(addr).await;
        let port = client.local_addr();
        let stale = in_node(2, listener.accept()).await.unwrap();
        let server = in_node(2, listener.accept()).await.unwrap();
        assert_eq!(server.1, port);
        drop(client);
        assert!(taken(port));
        drop((server, stale));
        assert!(!taken(port));
        assert_held(&world, &mut Default::default());
    });
}

/// Closing the listener ends the server's side of what it has not accepted, so a connection
/// whose client closed first has no handle left and is forgotten.
#[test]
fn a_closed_listener_forgets_what_its_clients_closed() {
    run(54, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(2, any(80)).unwrap();
        let addr = listener.local_addr();
        let kept = plain(addr).await;
        drop(plain(addr).await);
        assert_eq!(connections(&world), 2);
        drop(listener);
        assert_eq!(connections(&world), 1);
        drop(kept);
        assert_eq!(connections(&world), 0);

        // The same when the node crashes.
        let _listener = world.listen(2, any(80)).unwrap();
        drop(plain(addr).await);
        assert_eq!(connections(&world), 1);
        world.crash(2);
        assert_eq!(connections(&world), 0);
        assert_held(&world, &mut Default::default());
    });
}

/// A frozen node accepts nothing. Its peers redial and give up, for much longer than there
/// are ports.
#[test]
fn dials_to_a_frozen_listener_never_run_out_of_ports() {
    run(52, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(2, any(0)).unwrap();
        let addr = listener.local_addr();
        world.freeze(2);
        // Two ports are in use throughout, and the counter passes them once.
        let kept =
            [plain(addr).await, plain(addr).await].map(|stream| (stream.local_addr(), stream));
        let range = u32::from(u16::MAX - FIRST_EPHEMERAL_PORT) + 1;
        let mut seen = std::collections::BTreeSet::new();
        for n in 0..20_000 {
            let before = lookups(&world);
            let client = plain(addr).await;
            let steps = lookups(&world) - before;
            assert!(steps <= 3, "dial {n} looked {steps} times");
            assert!(kept.iter().all(|(port, _)| *port != client.local_addr()));
            seen.insert(client.local_addr().port());
        }
        // Every port of the range but the two that are kept.
        assert_eq!(seen.len(), range as usize - kept.len());
        // The backlog keeps every one of them.
        assert_eq!(connections(&world), 20_000 + kept.len());
        let mut gone = std::collections::BTreeSet::new();
        assert_held(&world, &mut gone);
        world.thaw(2);
        for _ in 0..20_000 + kept.len() {
            drop(in_node(2, listener.accept()).await.unwrap());
        }
        assert_eq!(connections(&world), kept.len());
        assert_held(&world, &mut gone);
    });
}

/// With every port really in use a dial looks at each port once, however many connections
/// there are, and fails.
#[test]
fn a_dial_looks_at_each_port_at_most_once() {
    run(53, |world| async move {
        nodes(&world, &[1, 2]);
        let listener = world.listen(2, any(80)).unwrap();
        let addr = listener.local_addr();
        let range = u64::from(u16::MAX - FIRST_EPHEMERAL_PORT) + 1;
        let mut open = Vec::new();
        for _ in 0..range {
            open.push(in_node(1, dial(Target::Addr(addr))).await.unwrap());
        }
        let before = lookups(&world);
        let error = in_node(1, dial(Target::Addr(addr))).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(lookups(&world) - before, range);
        // Another node has its own ports.
        let before = lookups(&world);
        let other = in_node(2, dial(Target::Addr(addr))).await.unwrap();
        assert_eq!(lookups(&world) - before, 1);
        drop(other);
        // One port comes back, and it is the one that is chosen.
        let freed = open.swap_remove(700).local_addr();
        let again = in_node(1, dial(Target::Addr(addr))).await.unwrap();
        assert_eq!(again.local_addr(), freed);
    });
}

/// Random dials, accepts, closes, resets, crashes and cuts: the addresses held are always
/// those of a scan over the connections, so each choice is the one the scan would make.
#[test]
fn held_ports_agree_with_a_scan_of_the_connections() {
    for seed in 60..70 {
        run(seed, |world| async move {
            nodes(&world, &[1, 2, 3]);
            let mut rng = SplitMix64::new(seed);
            let mut listeners = std::collections::BTreeMap::new();
            let mut handles: Vec<Pending> = Vec::new();
            let mut gone = std::collections::BTreeSet::new();
            for _ in 0..600 {
                let node = 1 + rng.below(3);
                if !world.is_up(node) {
                    world.restart(node);
                }
                let listener = listeners
                    .entry(node)
                    .or_insert_with(|| world.listen(node, any(0)).unwrap());
                let addr = SimListener::local_addr(listener);
                match rng.below(20) {
                    0..=8 => {
                        let from = 1 + rng.below(3);
                        if let Ok(dialed) = in_node(from, dial(Target::Addr(addr))).await {
                            handles.push(dialed);
                        }
                    }
                    9..=11 => {
                        let accepted = in_node(node, listener.accept());
                        if let Ok(Ok((pending, _))) = time::timeout(ms(1), accepted).await {
                            handles.push(pending);
                        }
                    }
                    12..=15 if !handles.is_empty() => {
                        let at = rng.below(handles.len() as u64) as usize;
                        drop(handles.swap_remove(at));
                    }
                    16 => {
                        let ids: Vec<ConnId> =
                            world.connections().iter().map(|conn| conn.id).collect();
                        if !ids.is_empty() {
                            world.reset_connection(ids[rng.below(ids.len() as u64) as usize]);
                        }
                    }
                    17 => {
                        world.crash(node);
                        listeners.remove(&node);
                    }
                    18 => {
                        world.power_off(node);
                        listeners.remove(&node);
                    }
                    _ => {
                        listeners.remove(&node);
                    }
                }
                assert_held(&world, &mut gone);
            }
        });
    }
}

fn fidelity(set: impl FnOnce(&mut crate::sim::Fidelity)) -> crate::sim::Fidelity {
    let mut fidelity = crate::sim::Fidelity::default();
    set(&mut fidelity);
    fidelity
}

#[test]
fn with_fidelity_a_dial_to_a_powered_off_host_hangs_until_it_restarts() {
    run(31, |world| async move {
        nodes(&world, &[1, 2]);
        world.set_fidelity(fidelity(|f| f.power_off_dials_hang = true));
        let listener = world.listen(1, any(7100)).unwrap();
        let addr = listener.local_addr();
        world.power_off(1);
        drop(listener);
        // No host answers the SYN: only the dialer's own timeout ends the dial.
        let hung = in_node(2, time::timeout(ms(500), dial(Target::Addr(addr)))).await;
        assert!(hung.is_err(), "a dial to a powered-off host must hang");
        let waiting = world.spawn_node(2, async move { dial(Target::Addr(addr)).await });
        tokio::time::sleep(ms(5)).await;
        assert!(!waiting.is_finished());
        // The host is back without a listener: its kernel refuses.
        world.restart(1);
        let refused = waiting.await.unwrap().unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
        let _again = world.listen(1, any(7100)).unwrap();
        assert!(in_node(2, dial(Target::Addr(addr))).await.is_ok());
        // A crashed process's host is up: refused at once, as without the fidelity.
        world.crash(1);
        let refused = in_node(2, dial(Target::Addr(addr))).await.unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
    });
}

#[test]
fn with_fidelity_the_first_segment_after_a_reboot_resets_the_old_session() {
    for reset_after_reboot in [false, true] {
        run(32, move |world| async move {
            nodes(&world, &[1, 2]);
            world.set_fidelity(fidelity(|f| f.reset_after_reboot = reset_after_reboot));
            let (mut old, _server) = streams(&world, 2, 1).await;
            world.power_off(1);
            world.restart(1);
            // A reader alone learns nothing: no segment, no RST.
            assert!(read_some(&mut old, ms(10)).await.is_none());
            send(&mut old, b"hello?").await.unwrap();
            let read = read_some(&mut old, ms(10)).await;
            if reset_after_reboot {
                let error = read.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            } else {
                assert!(
                    read.is_none(),
                    "the default black-holes the old session forever"
                );
            }
        });
    }
}

#[test]
fn with_fidelity_a_killed_process_delivers_what_it_sent_then_fin() {
    for per_mille in [0, 1_000] {
        run(33, move |world| async move {
            nodes(&world, &[1, 2]);
            world.set_fidelity(fidelity(|f| f.kill_flush_per_mille = per_mille));
            let (mut client, server) = streams(&world, 2, 1).await;
            let mut server = server;
            // Sent, but not yet read when the process dies.
            send(&mut server, b"last words").await.unwrap();
            world.crash(1);
            let read = read_some(&mut client, ms(10)).await.unwrap();
            if per_mille == 0 {
                assert_eq!(read.unwrap_err().kind(), io::ErrorKind::ConnectionReset);
                return;
            }
            assert_eq!(read.unwrap(), b"last words");
            // Then FIN.
            assert_eq!(read_some(&mut client, ms(10)).await.unwrap().unwrap(), b"");
            // A segment to the dead process comes back as an RST.
            send(&mut client, b"anyone?").await.unwrap();
            let mut buf = [0; 8];
            let error = client.read(&mut buf).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            drop(server);
        });
    }
}

#[test]
fn stream_faults_are_reported_in_order() {
    run(34, |world| async move {
        nodes(&world, &[1, 2]);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        world.observe(Arc::new(move |event| {
            if let crate::sim::SimEvent::Net(fault) = event {
                log.lock().unwrap().push(*fault);
            }
        }));
        struct Rules;
        impl Planner for Rules {
            fn message(&mut self, envelope: &super::Envelope<'_>) -> Fate {
                match envelope.bytes {
                    b"drop" => Fate::Drop,
                    b"reset" => Fate::Reset,
                    _ => Fate::NOW,
                }
            }
            fn dial(&mut self, _: Option<u64>, to: Option<u64>) -> Dial {
                if to == Some(2) {
                    Dial::Refuse
                } else {
                    Dial::Connect
                }
            }
        }
        let (mut a, _b) = streams(&world, 2, 1).await;
        let (mut c, _d) = streams(&world, 2, 1).await;
        world.set_net_planner(Rules);
        send(&mut a, b"drop").await.unwrap();
        assert!(send(&mut c, b"reset").await.is_err());
        world.black_hole(Some(1), Some(2));
        world.heal(Some(1), Some(2));
        world.heal_all();
        let _listener = world.listen(2, any(7200)).unwrap();
        let refused = in_node(1, dial(Target::Addr(SocketAddr::new(node_ip(2), 7200)))).await;
        assert!(refused.is_err());
        world.reset_connection(a.id());
        use super::{NetFault, ResetCause};
        assert_eq!(
            *seen.lock().unwrap(),
            [
                NetFault::Drop {
                    conn: a.id(),
                    from: Some(2),
                    to: Some(1)
                },
                NetFault::Reset {
                    conn: c.id(),
                    from: Some(2),
                    to: Some(1),
                    cause: ResetCause::Planner
                },
                NetFault::BlackHole {
                    from: Some(1),
                    to: Some(2)
                },
                NetFault::Heal {
                    from: Some(1),
                    to: Some(2)
                },
                NetFault::HealAll,
                NetFault::Refuse {
                    from: Some(1),
                    to: Some(2)
                },
                NetFault::Reset {
                    conn: a.id(),
                    from: Some(2),
                    to: Some(1),
                    cause: ResetCause::World
                },
            ]
        );
    });
}
