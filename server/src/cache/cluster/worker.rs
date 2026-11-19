//! Worker roles, direct RESP routing and background synchronization.
mod actor;
mod peer;
mod sync;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};
use uuid::Uuid;

use crate::cache::command::DataCommand;
use crate::cache::config::{self, Eviction, ListenerConfig};
use crate::cache::listener::{self, Backend};
use crate::cache::store::{EvictionPolicy, TurnLimits};
use crate::cache::wire::Response;

use super::clock::ClockMode;
use super::discovery;
use super::protocol::{MAX_WORKERS, Member, Topology, WorkerId};
use super::transport::{self, Endpoint, PeerTls, TlsConfig};
use actor::{ActorConfig, ActorHandle, ClientTurn};

const MIB: usize = 1 << 20;
const MAX_ACTORS: usize = 32;
const PEER_FRAME: usize = 2 * MIB;
const CONTROL_RESERVE: usize = 24 * MIB;
const OUTBOUND_SESSIONS: usize = 4;
const COPY_HANDOFFS: usize = 2;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node: u64,
    zone: String,
    client_host: String,
    client_port: u16,
    peer_listen: SocketAddr,
    clock: ClockMode,
    #[serde(default = "default_actors")]
    actors: usize,
    #[serde(default = "default_slot_bytes")]
    slot_bytes: usize,
    #[serde(default = "default_transfer_bytes")]
    journal_bytes: usize,
    #[serde(default = "default_snapshot_bytes")]
    snapshot_bytes: usize,
    #[serde(default = "default_peer_connections")]
    peer_connections: usize,
    metrics_file: Option<PathBuf>,
    peer: Endpoint,
    tls: TlsConfig,
    client: config::Config,
    #[serde(deserialize_with = "super::transport::controller_endpoints")]
    controllers: BTreeMap<u64, Endpoint>,
}

fn default_actors() -> usize {
    std::thread::available_parallelism().map_or(1, |count| count.get().min(8))
}

fn default_slot_bytes() -> usize {
    8 * MIB
}
fn default_transfer_bytes() -> usize {
    24 * MIB
}
fn default_snapshot_bytes() -> usize {
    144 * MIB
}
fn default_peer_connections() -> usize {
    4
}

#[derive(Clone)]
struct Worker {
    shared: Arc<Shared>,
}

struct Shared {
    id: WorkerId,
    actors: Vec<ActorHandle>,
    topology: RwLock<Arc<Topology>>,
    pending: Semaphore,
    copy_handoffs: Arc<Semaphore>,
    output_limit: usize,
}

impl Worker {
    fn topology(&self) -> Arc<Topology> {
        Arc::clone(&self.shared.topology.read().expect("topology lock"))
    }

    fn set_topology(&self, topology: Arc<Topology>) {
        *self.shared.topology.write().expect("topology lock") = topology;
    }

    fn actor(&self, slot: u16) -> &ActorHandle {
        &self.shared.actors[usize::from(slot) % self.shared.actors.len()]
    }
}

impl Backend for Worker {
    async fn execute(&self, command: DataCommand) -> Response {
        let slot = match command.slot() {
            Ok(slot) => slot,
            Err(error) => return error.response(),
        };
        let topology = self.topology();
        let placement = &topology.slots[usize::from(slot)];
        let Some(owner) = placement.primary.and_then(|owner| topology.member(owner)) else {
            return unavailable();
        };
        if owner.id != self.shared.id {
            return discovery::moved(slot, owner);
        }
        let Ok(permit) = self.shared.pending.try_acquire() else {
            return overloaded();
        };
        let (reply, result) = oneshot::channel();
        let turn = ClientTurn {
            generation: topology.generation,
            epoch: placement.epoch,
            slot,
            queued: std::time::Instant::now(),
            operation: command.into_operation(),
            reply,
        };
        drop(topology);
        let response = if self.actor(slot).clients.try_send(turn).is_ok() {
            result.await.unwrap_or_else(|_| unavailable())
        } else {
            overloaded()
        };
        drop(permit);
        response
    }

    fn clustered(&self) -> bool {
        true
    }

    fn slots(&self) -> Response {
        discovery::slots(&self.topology(), self.shared.output_limit)
    }

    fn shards(&self) -> Response {
        discovery::shards(&self.topology(), self.shared.output_limit)
    }
}

fn unavailable() -> Response {
    Response::error(b"CLUSTERDOWN cache slot has no valid authority")
}
fn overloaded() -> Response {
    Response::error(b"BUSY cache actor queue is full")
}
fn frozen() -> Response {
    Response::error(b"TRYAGAIN cache slot handoff in progress")
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn run(path: &Path) -> io::Result<()> {
    let config: Config = config::read_toml(path)?;
    let root = path.parent().unwrap_or_else(|| Path::new("."));
    let peer_tls = PeerTls::load(&config.tls, root)?;
    let member = Member {
        id: WorkerId {
            node: config.node,
            incarnation: Uuid::new_v4(),
        },
        zone: config.zone,
        client_host: config.client_host,
        client_port: config.client_port,
        peer: config.peer,
    };
    if !member.valid()
        || !member.authenticates(peer_tls.fingerprint)
        || config.peer_listen.port() == 0
        || config.client.listen.port() == 0
        || config.client.listen.port() != member.client_port
        || !config.client.plaintext_loopback && config.client.tls.is_none()
        || config.client.plaintext_loopback && !matches!(config.clock, ClockMode::LocalDocker)
        || config.controllers.len() != 3
        || config
            .controllers
            .values()
            .any(|endpoint| endpoint.server_name().is_err())
    {
        return Err(invalid("invalid worker identity, listeners or controllers"));
    }
    let eviction = match config.client.eviction {
        Eviction::SampledLru => EvictionPolicy::SampledLru,
        Eviction::Noeviction => EvictionPolicy::NoEviction,
    };
    let client = config.client.transport(root)?;
    let actors = config.actors;
    if !(1..=MAX_ACTORS).contains(&actors)
        || !(1..=16).contains(&config.peer_connections)
        || config.slot_bytes < MIB
        || config.slot_bytes > client.limits.data_bytes
    {
        return Err(invalid("invalid worker capacities"));
    }
    let peer_reserve = (4 * config.peer_connections + COPY_HANDOFFS + 3) * PEER_FRAME
        + (config.peer_connections + OUTBOUND_SESSIONS + 1) * (320 << 10)
        + CONTROL_RESERVE;
    let turn_reserve = client
        .turn_reserve_bytes
        .checked_sub(peer_reserve)
        .and_then(|reserve| reserve.checked_sub(config.journal_bytes))
        .and_then(|reserve| reserve.checked_sub(config.snapshot_bytes))
        .ok_or_else(|| invalid("worker memory does not cover peer and transfer reserves"))?;
    let actor_config = ActorConfig {
        clock: config.clock,
        id: member.id,
        count: actors,
        slot_bytes: config.slot_bytes,
        primary_bytes: client.limits.data_bytes / (2 * actors),
        replica_bytes: client.limits.data_bytes / (2 * actors),
        journal_bytes: config.journal_bytes / actors,
        snapshot_bytes: config.snapshot_bytes / actors,
        turns: TurnLimits {
            growth_bytes: turn_reserve / actors,
            reply_bytes: client.limits.max_reply_bytes,
        },
        max_pending: client.limits.max_pending,
        eviction,
    };
    actor_config.validate()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2.min(actors))
        .max_blocking_threads(4)
        .enable_all()
        .build()?;
    let (worker, threads) = spawn_actors(actor_config)?;
    let result = runtime.block_on(serve(
        worker,
        client,
        member,
        config.peer_listen,
        peer_tls,
        config.controllers,
        config.peer_connections,
        config.metrics_file.map(|path| root.join(path)),
    ));
    for thread in threads {
        if thread.join().is_err() {
            return Err(io::Error::other("cache actor panicked"));
        }
    }
    result
}

fn spawn_actors(config: ActorConfig) -> io::Result<(Worker, Vec<std::thread::JoinHandle<()>>)> {
    let mut actors = Vec::with_capacity(config.count);
    let mut threads = Vec::with_capacity(config.count);
    for index in 0..config.count {
        match actor::spawn(index, config) {
            Ok((handle, thread)) => {
                actors.push(handle);
                threads.push(thread);
            }
            Err(error) => {
                drop(actors);
                for thread in threads {
                    let _ = thread.join();
                }
                return Err(error);
            }
        }
    }
    let shared = Shared {
        id: config.id,
        actors,
        topology: RwLock::new(Arc::new(Topology::default())),
        pending: Semaphore::new(config.max_pending),
        copy_handoffs: Arc::new(Semaphore::new(COPY_HANDOFFS)),
        output_limit: config.turns.reply_bytes,
    };
    Ok((
        Worker {
            shared: Arc::new(shared),
        },
        threads,
    ))
}

async fn serve(
    worker: Worker,
    client: ListenerConfig,
    member: Member,
    peer_listen: SocketAddr,
    tls: PeerTls,
    controllers: BTreeMap<u64, Endpoint>,
    peer_connections: usize,
    metrics_file: Option<PathBuf>,
) -> io::Result<()> {
    let client_socket = TcpListener::bind(client.listen).await?;
    let peer_socket = TcpListener::bind(peer_listen).await?;
    let stop = listener::shutdown()?;
    let background = worker.clone();
    let peers = worker.clone();
    let replicator = worker.clone();
    eprintln!(
        "nabedra-db: cache worker {} incarnation {} listening on {}",
        member.id.node,
        member.id.incarnation,
        client_socket.local_addr()?
    );
    tokio::select! {
        _ = stop => Ok(()),
        result = listener::listen(client_socket, client, worker, std::future::pending()) => result,
        result = transport::serve(peer_socket, tls.clone(), peer_connections, PEER_FRAME,
            move |identity, request| {
                let peers = peers.clone();
                async move { peer::handle(&peers, identity, &request).await }
            }, std::future::pending()) => result,
        result = sync::control(background, tls.clone(), member, controllers, metrics_file) => result,
        result = sync::replicate(replicator, tls) => result,
    }
}
