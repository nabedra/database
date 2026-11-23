//! Test driver for the real controller and its durable topology log.
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, Weak},
};

use nabedra_rt::sim::{self, net::SimListener};
use nabedra_rt::task;
use nabedra_sim::{LeaderId, RaftObserver, Vote};
use openraft::ServerState;
use openraft::metrics::RaftMetrics;
use openraft::type_config::alias::WatchReceiverOf;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::super::clock::ClockMode;
use super::super::transport::{Endpoint, Fingerprint, PeerTls};
use super::network::Network;
use super::{Controller, Loaded, RaftConfig, WatchReceiver};

pub(in crate::cache::cluster) struct SimConfig {
    pub cluster: Uuid,
    pub node: u64,
    pub dir: PathBuf,
    pub clock: ClockMode,
    pub voters: BTreeMap<u64, Endpoint>,
    pub operators: Vec<Fingerprint>,
    pub tls: PeerTls,
}

pub(in crate::cache::cluster) struct SimRaftMetrics {
    metrics: WatchReceiverOf<RaftConfig, RaftMetrics<RaftConfig>>,
    controller: Weak<Controller>,
}

impl SimRaftMetrics {
    pub fn observe(&self, observer: &mut RaftObserver, now_ms: u64) {
        let current = self.metrics.borrow_watched();
        let voted_for = current.vote.leader_id();
        let vote = Vote {
            leader: LeaderId::new(voted_for.term, voted_for.node_id),
            committed: current.vote.is_committed(),
        };
        observer.observe(
            "cache",
            current.id,
            vote,
            current.state == ServerState::Leader,
            current.last_applied.map(|id| id.index),
            now_ms,
        );
    }

    /// Drive the real placement turn at a lease boundary in the controller's node.
    pub async fn reconcile_once(&self) {
        let controller = self.controller.upgrade().expect("controller is running");
        assert_eq!(sim::current_node(), Some(controller.node));
        controller.reconcile_once().await;
    }

    /// The local term, elected leader, and local commit frontier.
    pub fn read(&self) -> (u64, Option<u64>, Option<u64>) {
        let current = self.metrics.borrow_watched();
        (
            current.current_term,
            (current.state == ServerState::Leader).then_some(current.id),
            current.local_committed.map(|id| id.index),
        )
    }
}

/// Start Raft and both peer listeners in the caller's simulated node.
pub(in crate::cache::cluster) async fn sim_start(
    config: SimConfig,
    app_listener: SimListener,
    raft_listener: SimListener,
) -> io::Result<(SimRaftMetrics, JoinHandle<io::Result<()>>)> {
    let network = Network {
        cluster: config.cluster,
        node: config.node,
        voters: Arc::new(config.voters),
        tls: config.tls,
    };
    let started = Loaded::open(&config.dir, network, config.clock, config.operators)?
        .start()
        .await?;
    let metrics = SimRaftMetrics {
        metrics: started.controller.raft.metrics(),
        controller: Arc::downgrade(&started.controller),
    };
    let service = task::spawn(started.serve(
        app_listener.into(),
        raft_listener.into(),
        std::future::pending(),
    ));
    Ok((metrics, service))
}
