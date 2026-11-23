use std::net::{Ipv4Addr, SocketAddr};

use nabedra_rt::sim::{self, NodeClock};

use super::*;

#[test]
fn k03_peer_session_checks_fingerprints_and_crash_cuts_connection() {
    sim::run(7, |world| async move {
        for node in [1, 2] {
            world.add_node(node, NodeClock::default());
            world.set_certificate(node, [node as u8; 32]);
        }
        world.register_host("cache-worker", 2);
        let listener = world
            .listen(2, SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0))
            .unwrap();
        let port = listener.local_addr().port();
        let listener_task = world.spawn_node(
            2,
            serve(
                listener,
                PeerTls::sim([2; 32]),
                4,
                128,
                |peer, payload| async move {
                    assert_eq!(peer, [1; 32]);
                    Ok(payload)
                },
                std::future::pending(),
            ),
        );

        sim::in_node(1, async {
            let mut endpoint = Endpoint {
                host: "cache-worker".to_owned(),
                port,
                fingerprints: vec![[3; 32]],
            };
            let tls = PeerTls::sim([1; 32]);
            let error = tls.connect(&endpoint).await.err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

            endpoint.fingerprints = vec![[2; 32]];
            let mut peer = tls.connect(&endpoint).await.unwrap();
            assert_eq!(peer.exchange(b"request", 128).await.unwrap(), b"request");
            world.crash(2);
            assert!(peer.exchange(b"after crash", 128).await.is_err());
        })
        .await;
        assert!(listener_task.await.unwrap_err().is_cancelled());
    });
}
