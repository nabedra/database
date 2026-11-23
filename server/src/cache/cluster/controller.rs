//! Three-voter authority for cache topology and short-lived worker grants.
mod disk;
mod log;
mod machine;
mod network;
mod placement;
#[cfg(all(test, feature = "sim"))]
mod simulation;
mod state;

#[cfg(all(test, feature = "sim"))]
pub(super) use simulation::{SimConfig, SimRaftMetrics, sim_start};

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nabedra_rt::net::AnyListener;
use nabedra_rt::time::{MissedTickBehavior, interval, timeout};
use openraft::{
    BasicNode, ReadPolicy, SnapshotPolicy, type_config::async_runtime::watch::WatchReceiver,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use uuid::Uuid;

use super::clock::{Clock, ClockBounds, ClockMode};
use super::protocol::{Admin, CONTROL_FRAME, Heartbeat, LEASE_MS, Reply, Request, VERSION};
use super::transport::{self, Endpoint, Fingerprint, PeerTls, TlsConfig, bounded_json};
use crate::cache::{command::SLOT_COUNT, config::read_toml, listener::shutdown};
use disk::invalid;
use machine::Machine;
use network::Network;
use placement::Observations;
use state::Command;

// This is the runtime openraft selects by default. Naming it keeps the
// product build on the same type while sim scopes Raft tasks and timers to
// the node and seeds elections.
#[cfg(not(feature = "sim"))]
#[allow(deprecated)]
type Runtime = openraft::impls::TokioRuntime;
#[cfg(feature = "sim")]
type Runtime = nabedra_rt::sim::openraft10::SimRuntime;

openraft::declare_raft_types!(
    RaftConfig:
        D = Command,
        R = Option<Reply>,
        NodeId = u64,
        Node = BasicNode,
        AsyncRuntime = Runtime,
);
type Raft = openraft::Raft<RaftConfig, Machine>;
const RAFT_SESSIONS: usize = 8;
const CONTROL_SESSIONS: usize = 56;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    cluster_id: Uuid,
    node: u64,
    listen: SocketAddr,
    raft_listen: SocketAddr,
    data_dir: PathBuf,
    clock: ClockMode,
    tls: TlsConfig,
    voters: BTreeMap<String, Endpoint>,
    operators: Vec<Fingerprint>,
}

#[derive(Serialize, Deserialize, PartialEq)]
struct DiskIdentity {
    cluster: Uuid,
    node: u64,
    voters: BTreeSet<u64>,
}

pub fn run(path: &Path) -> io::Result<()> {
    let config: Config = read_toml(path)?;
    let root = path.parent().unwrap_or_else(|| Path::new("."));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()?
        .block_on(serve(config, root))
}

async fn serve(config: Config, root: &Path) -> io::Result<()> {
    let tls = PeerTls::load(&config.tls, root)?;
    let voters = Arc::new(validate(&config, &tls)?);
    let network = Network {
        cluster: config.cluster_id,
        node: config.node,
        voters,
        tls,
    };
    let loaded = Loaded::open(
        &root.join(&config.data_dir),
        network,
        config.clock,
        config.operators,
    )?;
    let stop = shutdown()?;
    let app_listener = TcpListener::bind(config.listen).await?;
    let raft_listener = TcpListener::bind(config.raft_listen).await?;
    let started = loaded.start().await?;

    eprintln!(
        "nabedra-db: cache controller {} listening on {}",
        config.node, config.listen
    );
    started
        .serve(app_listener.into(), raft_listener.into(), stop)
        .await
}

struct Loaded {
    network: Network,
    log: log::Log,
    machine: Machine,
    clock: Clock,
    operators: Vec<Fingerprint>,
}

impl Loaded {
    fn open(
        dir: &Path,
        network: Network,
        mode: ClockMode,
        operators: Vec<Fingerprint>,
    ) -> io::Result<Self> {
        let log = log::Log::open(dir)?;
        bind_disk(
            dir,
            DiskIdentity {
                cluster: network.cluster,
                node: network.node,
                voters: network.voters.keys().copied().collect(),
            },
        )?;
        let machine = Machine::open(dir)?;
        let clock = Clock::new(mode)?;
        Ok(Self {
            network,
            log,
            machine,
            clock,
            operators,
        })
    }

    async fn start(self) -> io::Result<Started> {
        let Self {
            network,
            log,
            machine,
            clock,
            operators,
        } = self;
        let raft_config = openraft::Config {
            cluster_name: format!("nabedra-cache-{}", network.cluster),
            heartbeat_interval: 125,
            election_timeout_min: 500,
            election_timeout_max: 950,
            max_payload_entries: 16,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(128),
            max_in_snapshot_log_to_keep: 32,
            purge_batch_size: 32,
            ..openraft::Config::default()
        }
        .validate()
        .map_err(io::Error::other)?;
        let raft = Raft::new(
            network.node,
            Arc::new(raft_config),
            network.clone(),
            log,
            machine.clone(),
        )
        .await
        .map_err(io::Error::other)?;

        // A single designated bootstrap voter proposes the initial three-voter
        // configuration. It does not remove or reinitialize recovered members.
        if network.voters.keys().next() == Some(&network.node)
            && !raft.is_initialized().await.map_err(io::Error::other)?
        {
            let initial = network
                .voters
                .iter()
                .map(|(id, peer)| (*id, BasicNode::new(format!("{}:{}", peer.host, peer.port))))
                .collect::<BTreeMap<_, _>>();
            raft.initialize(initial).await.map_err(io::Error::other)?;
        }

        let privileged = network
            .voters
            .values()
            .flat_map(|voter| voter.fingerprints.iter())
            .chain(&operators)
            .copied()
            .collect();
        let controller = Arc::new(Controller {
            raft,
            machine,
            node: network.node,
            clock: Mutex::new(clock),
            observed: Mutex::new(Observations::default()),
            operators,
            privileged,
        });
        Ok(Started {
            controller,
            network,
        })
    }
}

struct Started {
    controller: Arc<Controller>,
    network: Network,
}

impl Started {
    async fn serve(
        self,
        app_listener: AnyListener,
        raft_listener: AnyListener,
        stop: impl Future<Output = ()>,
    ) -> io::Result<()> {
        let Self {
            controller,
            network,
        } = self;
        let raft = controller.raft.clone();
        let tls = network.tls.clone();
        let raft_rpc = {
            let raft = raft.clone();
            transport::serve(
                raft_listener,
                network.tls.clone(),
                RAFT_SESSIONS,
                CONTROL_FRAME,
                move |fingerprint, bytes| {
                    let network = network.clone();
                    let raft = raft.clone();
                    async move { network.handle(raft, fingerprint, bytes).await }
                },
                std::future::pending(),
            )
        };
        let api = {
            let controller = Arc::clone(&controller);
            transport::serve(
                app_listener,
                tls,
                CONTROL_SESSIONS,
                CONTROL_FRAME,
                move |fingerprint, bytes| {
                    let controller = Arc::clone(&controller);
                    async move { controller.handle(fingerprint, &bytes).await }
                },
                std::future::pending(),
            )
        };

        let result = tokio::select! {
            _ = stop => Ok(()),
            result = raft_rpc => result,
            result = api => result,
            () = controller.reconcile() => Err(invalid("cache placement loop stopped")),
        };
        raft.shutdown().await.map_err(io::Error::other)?;
        result
    }
}

fn validate(config: &Config, tls: &PeerTls) -> io::Result<BTreeMap<u64, Endpoint>> {
    if config.cluster_id.is_nil()
        || config.listen == config.raft_listen
        || config.voters.len() != 3
        || config.operators.is_empty()
        || config.operators.len() > 16
    {
        return Err(invalid(
            "cache controller requires three voters and two private listeners",
        ));
    }
    let mut fingerprints: BTreeSet<_> = config.operators.iter().copied().collect();
    if fingerprints.len() != config.operators.len() {
        return Err(invalid("duplicate operator certificate"));
    }
    let mut voters = BTreeMap::new();
    let mut addresses = BTreeSet::new();
    for (id, endpoint) in &config.voters {
        let id: u64 = id.parse().map_err(|_| invalid("invalid cache voter ID"))?;
        endpoint.server_name()?;
        if id == 0
            || !addresses.insert((&endpoint.host, endpoint.port))
            || endpoint
                .fingerprints
                .iter()
                .any(|fingerprint| !fingerprints.insert(*fingerprint))
            || voters.insert(id, endpoint.clone()).is_some()
        {
            return Err(invalid(
                "duplicate cache voter identity, endpoint or certificate",
            ));
        }
    }
    if !voters
        .get(&config.node)
        .is_some_and(|voter| voter.fingerprints.contains(&tls.fingerprint))
    {
        return Err(invalid("controller certificate does not match voter"));
    }
    Ok(voters)
}

fn bind_disk(dir: &Path, identity: DiskIdentity) -> io::Result<()> {
    let path = dir.join("controller.identity");
    if let Some(previous) = disk::read_atomic::<DiskIdentity>(&path)? {
        if previous != identity {
            return Err(invalid(
                "controller volume belongs to another cluster or voter",
            ));
        }
        return Ok(());
    }
    disk::replace(&path, |file| disk::write_record(file, &identity))?;
    Ok(())
}

struct Controller {
    raft: Raft,
    machine: Machine,
    node: u64,
    clock: Mutex<Clock>,
    observed: Mutex<Observations>,
    operators: Vec<Fingerprint>,
    privileged: BTreeSet<Fingerprint>,
}

impl Controller {
    async fn handle(&self, certificate: Fingerprint, bytes: &[u8]) -> io::Result<Vec<u8>> {
        let request = serde_json::from_slice(bytes).map_err(io::Error::other)?;
        let reply = self.request(certificate, request).await;
        bounded_json(&reply, CONTROL_FRAME)
    }

    fn now(&self) -> io::Result<ClockBounds> {
        self.clock
            .lock()
            .map_err(|_| invalid("cache clock unavailable"))?
            .now()
    }

    fn leader_term(&self) -> Option<u64> {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow_watched();
        (metrics.current_leader == Some(self.node)).then_some(metrics.current_term)
    }

    async fn request(&self, certificate: Fingerprint, request: Request) -> Reply {
        let Some(term) = self.leader_term() else {
            return Reply::Retry;
        };
        match request {
            Request::Enroll(member) => {
                if !member.valid()
                    || !member.authenticates(certificate)
                    || self.privileged.contains(&certificate)
                {
                    return Reply::Refused("invalid enrollment identity".into());
                }
                self.commit(Command::Enroll {
                    member,
                    certificate,
                })
                .await
            }
            Request::Heartbeat(heartbeat) => self.heartbeat(term, certificate, heartbeat).await,
            Request::Admin {
                version,
                operation,
                command,
            } => {
                if version != VERSION || !self.operators.contains(&certificate) {
                    return Reply::Refused(
                        "operator certificate or protocol version not authorized".into(),
                    );
                }
                if let Admin::Enroll { member } | Admin::Replace { member } = &command
                    && member
                        .peer
                        .fingerprints
                        .iter()
                        .any(|fingerprint| self.privileged.contains(fingerprint))
                {
                    return Reply::Refused("a worker requires separate credentials".into());
                }
                if matches!(command, Admin::Status) {
                    return match self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
                        Ok(_) => self.status(term).await,
                        Err(_) => Reply::Retry,
                    };
                }
                let lower_ms = if matches!(command, Admin::ColdReset { .. }) {
                    let Ok(now) = self.now() else {
                        return Reply::Retry;
                    };
                    now.lower_ms
                } else {
                    0
                };
                self.commit(Command::Admin {
                    id: operation,
                    action: command,
                    lower_ms,
                })
                .await
            }
        }
    }

    async fn status(&self, term: u64) -> Reply {
        let now = self.now().ok();
        self.machine
            .read(|state| {
                let mut topo = state.topology.clone();
                if let Some(now) = now
                    && let Ok(mut observed) = self.observed.lock()
                {
                    observed.reset_if_changed(term, topo.generation);
                    (topo.ready_primaries, topo.ready_replicas) = observed.ready(&topo, now);
                }
                Reply::Status(topo)
            })
            .await
    }

    async fn heartbeat(&self, term: u64, certificate: Fingerprint, heartbeat: Heartbeat) -> Reply {
        if heartbeat.version != VERSION || heartbeat.progress.len() > SLOT_COUNT {
            return Reply::Refused("invalid worker heartbeat".into());
        }
        let Ok(now) = self.now() else {
            return Reply::Retry;
        };
        let (generation, revision, worker) =
            (heartbeat.generation, heartbeat.revision, heartbeat.worker);
        let accepted = self
            .machine
            .read(|state| {
                let topo = &state.topology;
                if !topo
                    .member(worker)
                    .is_some_and(|member| member.authenticates(certificate))
                {
                    return false;
                }
                if let Ok(mut observed) = self.observed.lock() {
                    observed.reset_if_changed(term, topo.generation);
                    observed.record(topo, heartbeat, now.lower_ms);
                }
                true
            })
            .await;
        if !accepted {
            return Reply::Refused("heartbeat identity is not enrolled".into());
        }
        let Some(expires_ms) = now.lower_ms.checked_add(LEASE_MS) else {
            return Reply::Retry;
        };
        self.commit(Command::Renew {
            generation,
            revision,
            worker,
            expires_ms,
        })
        .await
    }

    async fn commit(&self, command: Command) -> Reply {
        match self.raft.client_write(command).await {
            Ok(result) => result.data.unwrap_or(Reply::Retry),
            Err(_) => Reply::Retry,
        }
    }

    async fn reconcile(&self) {
        let mut ticks = interval(Duration::from_millis(250));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            self.reconcile_once().await;
        }
    }

    async fn reconcile_once(&self) {
        let Some(term) = self.leader_term() else {
            return;
        };
        let Ok(now) = self.now() else {
            return;
        };
        let proposal = self
            .machine
            .read(|state| {
                let mut observed = self.observed.lock().ok()?;
                observed.reset_if_changed(term, state.topology.generation);
                let changes = observed.plan(state, now);
                (!changes.is_empty()).then_some(Command::Reconcile {
                    generation: state.topology.generation,
                    revision: state.topology.revision,
                    lower_ms: now.lower_ms,
                    changes,
                })
            })
            .await;
        if let Some(proposal) = proposal {
            let _ = timeout(Duration::from_millis(700), self.commit(proposal)).await;
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
