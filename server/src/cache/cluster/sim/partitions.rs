//! What happens to messages sent across a blocked link (fault model, K03).

use nabedra_rt::sim::{self, net::Target, net::dial, net::node_ip};
use nabedra_sim::runtime;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::super::super::{
    clock::ClockMode,
    protocol::{Admin, CONTROL_FRAME, Reply, Request, VERSION},
};
use super::super::harness::{Cluster, Config, Partitions, memory_limit, operator_connection};
use super::super::hooks::Knobs;
use crate::cache::store::EvictionPolicy;

fn config(partitions: Partitions) -> Config {
    Config {
        partitions,
        nodes: 4,
        groups: 3,
        actors: 1,
        slot_bytes: memory_limit(1),
        eviction: EvictionPolicy::NoEviction,
        clock_mode: ClockMode::ClockBound,
        base_error_ns: 1_000_000,
        declared_ppm: 10,
        skew: false,
        gradual_drift: false,
        knobs: Knobs::default(),
        check_replica_prefix: true,
        keep_full_trace: false,
    }
}

const SENDER: u64 = 11;
const RECEIVER: u64 = 14;

/// The bytes a receiver has read at each point of a one-way partition.
#[derive(Debug, PartialEq, Eq)]
struct Received {
    before: Vec<u8>,
    blocked: Vec<u8>,
    healed: Vec<u8>,
    closed: Vec<u8>,
}

async fn stream_across_partition(cluster: &mut Cluster) -> Received {
    let at = SocketAddr::new(node_ip(RECEIVER), 9300);
    let listener = cluster.world.listen(RECEIVER, at).unwrap();
    let bytes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = bytes.clone();
    cluster.world.spawn_node(RECEIVER, async move {
        let (pending, _) = listener.accept().await.unwrap();
        let mut stream = pending.plain();
        while let Ok(byte) = stream.read_u8().await {
            sink.lock().unwrap().push(byte);
        }
    });
    let read = || bytes.lock().unwrap().clone();
    let mut stream = sim::in_node(SENDER, dial(Target::Addr(at)))
        .await
        .unwrap()
        .plain();
    for byte in *b"a" {
        stream.write_u8(byte).await.unwrap();
        stream.flush().await.unwrap();
    }
    cluster.step(100).await;
    let before = read();
    cluster.link(SENDER, RECEIVER, true);
    for byte in *b"bc" {
        stream.write_u8(byte).await.unwrap();
        stream.flush().await.unwrap();
        cluster.step(100).await;
    }
    cluster.step(2_000).await;
    let blocked = read();
    cluster.heal();
    cluster.step(100).await;
    let healed = read();
    stream.write_u8(b'd').await.unwrap();
    stream.flush().await.unwrap();
    drop(stream);
    cluster.step(30_000).await;
    Received {
        before,
        blocked,
        healed,
        closed: read(),
    }
}

/// A partition used to lose what was sent across it and everything after it
/// on the same connection. Late delivery is ordered and exactly once.
#[test]
fn blocked_link_loses_or_delays_a_stream() {
    for (partitions, healed, closed) in [
        (Partitions::Loss, "a", "a"),
        (Partitions::Late, "abc", "abcd"),
    ] {
        let received = runtime::run(1, |world| async move {
            let mut cluster =
                Cluster::boot(world, vec!["{stream}".into()], config(partitions)).await;
            stream_across_partition(&mut cluster).await
        });
        println!("Cache partitions {partitions:?} stream: {received:?}");
        assert_eq!(
            received,
            Received {
                before: b"a".to_vec(),
                blocked: b"a".to_vec(),
                healed: healed.as_bytes().to_vec(),
                closed: closed.as_bytes().to_vec(),
            },
            "{partitions:?}"
        );
    }
}

/// An operator request through the product transport. The operator times out
/// during the partition. Under late delivery the leader still applies it.
#[test]
fn timed_out_request_applies_only_under_late_delivery() {
    for (partitions, applies) in [(Partitions::Loss, false), (Partitions::Late, true)] {
        let run = || {
            runtime::run(1, |world| async move {
                let mut cluster =
                    Cluster::boot(world, vec!["{request}".into()], config(partitions)).await;
                let leader = cluster.leader().unwrap();
                let worker = 14;
                let mut connection = operator_connection(leader).await;
                cluster.partition_operator(leader);
                let request = Request::Admin {
                    version: VERSION,
                    operation: Uuid::from_u128(u128::MAX),
                    command: Admin::Drain { worker },
                };
                let reply = connection.json::<_, Reply>(&request, CONTROL_FRAME).await;
                assert!(reply.is_err(), "{reply:?}");
                drop(connection);
                cluster.step(1_000).await;
                let blocked = draining(&cluster, worker);
                cluster.heal();
                cluster.step(1_000).await;
                let healed = draining(&cluster, worker);
                cluster.step(20_000).await;
                let observed = cluster.observed.lock().unwrap();
                (
                    blocked,
                    healed,
                    observed
                        .latest
                        .as_ref()
                        .is_some_and(|topology| topology.draining.contains(&worker)),
                    observed.held,
                    observed.trace.hash(),
                )
            })
        };
        let (blocked, healed, later, held, trace) = run();
        println!(
            "Cache partitions {partitions:?} request: applied blocked={blocked} healed={healed} later={later}, held={held}, trace={trace:016x}"
        );
        assert!(!blocked, "{partitions:?} applied across the partition");
        assert_eq!((healed, later), (applies, applies), "{partitions:?}");
        assert_eq!(held > 0, applies, "{partitions:?} held {held}");
        assert_eq!(run(), (blocked, healed, later, held, trace));
    }
}

fn draining(cluster: &Cluster, worker: u64) -> bool {
    let observed = cluster.observed.lock().unwrap();
    observed
        .latest
        .as_ref()
        .is_some_and(|topology| topology.draining.contains(&worker))
}
