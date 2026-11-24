//! D10, "offer an old one-shot peer": a process from before persistent sessions takes an enrolled
//! API's place and talks to the running workers. It offers only the one-shot exchange, so the
//! workers must answer it there, and writes through the other gateway keep committing.
//!
//! The old process is a caller. A registered worker or gateway that itself offers only the
//! one-shot protocol is not expressible without a seam in product code.

use std::{collections::BTreeSet, sync::Arc};

use nabedra_database_server::{
    oltp::pool::WorkerSharedResources,
    oltp_replication::authority::control::{ControlClient, ControlRoutes},
};
use nabedra_oltp_types::types::{GroupIdentity, WorkerEnrollment};
use nabedra_protocol::protocol::ProtocolError;
use nabedra_resources::{config::WorkerResources, memory::SharedResourceConfig};
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use nabedra_sim::{Draw, rng::stream};
use nabedra_transport::{EndpointRegistry, TransportConfig, WorkerTransport, tls::TlsMaterial};
use serde_json::json;
use uuid::Uuid;

use super::{
    Workload,
    clients::{self, Ctx, Request},
};
use crate::{
    bootstrap::{CLIENTS, DATABASE, GATEWAYS, initial_range},
    disk::{self, DiskCut},
    nemesis::NetFaults,
    net::PeerDirectory,
    node::Cluster,
};

const LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Whether this seed runs the stage. One `direct` seed in four does.
pub(crate) fn selected(seed: u64, workload: Workload, forced: bool) -> bool {
    workload == Workload::Direct && (forced || stream(seed, "oltp-d10-old-peer").chance(250))
}

/// Run the stage and return the clauses whose assertions ran.
pub(crate) async fn run(cluster: &mut Cluster, ctx: &Ctx, faults: &NetFaults) -> Vec<&'static str> {
    let mut clauses = Vec::new();
    if let Err(error) = stage(cluster, ctx, faults, &mut clauses).await {
        cluster
            .checks
            .violate(&cluster.world, "OldPeer", format!("D10: {error}"));
    }
    clauses
}

async fn stage(
    cluster: &mut Cluster,
    ctx: &Ctx,
    faults: &NetFaults,
    clauses: &mut Vec<&'static str>,
) -> Result<(), String> {
    let node = GATEWAYS[1];
    let connection = cluster.admin("cluster.connection", json!({})).await?;
    let table = Uuid::parse_str(&ctx.table).map_err(|error| error.to_string())?;
    let data: GroupIdentity =
        serde_json::from_value(initial_range(cluster.config.cluster_id, table)).map_err(invalid)?;
    let control: GroupIdentity = serde_json::from_value(json!({
        "cluster_id": cluster.config.cluster_id, "database": DATABASE,
        "group_id": connection["control_group_id"], "group_format_version": 1, "kind": "control",
    }))
    .map_err(invalid)?;
    let peers: Vec<WorkerEnrollment> =
        serde_json::from_value(connection["control_peers"].clone()).map_err(invalid)?;
    let local = cluster.config.api_enrollment(1)["enrollment"].clone();
    let local: WorkerEnrollment = serde_json::from_value(local).map_err(invalid)?;
    let tls = cluster
        .config
        .tls(node)
        .map_err(|error| format!("read the peer's certificate: {error}"))?;

    cluster.kill_gateway(node).await;
    disk::reopen(&cluster.world, &cluster.checks, node, DiskCut::Kill);
    faults.watch(node);
    let world = cluster.world.clone();
    let peer = world.spawn_node(node, async move {
        in_node(node, peer(node, local, tls, control, peers, data)).await
    });
    // Writes keep committing through the other gateway while the old peer is served.
    let id = in_node(CLIENTS[0], async { uuid_v4() }).await;
    let request = Request::row("table.upsert", &ctx.table, 1, 600_000_000, id);
    let mut client = ctx.new_client();
    let write = clients::attempt(ctx, &mut client, CLIENTS[0], GATEWAYS[0], &request, false).await;
    let served = time::timeout(LIMIT, peer)
        .await
        .map_err(|_| "the old peer was not answered within 30 s".to_owned())?
        .map_err(|error| format!("the old peer stopped: {error}"))?;
    let frames = faults.watched();
    cluster.record(format!("D10 old peer n{node}: {served:?}, {frames} frames"));
    clauses.push("D10:one-shot-peer");
    if let Err(error) = served {
        cluster.checks.violate(
            &cluster.world,
            "OldPeer",
            format!("D10: a worker did not answer the one-shot peer: {error}"),
        );
    }
    if frames != 0 {
        cluster.checks.violate(
            &cluster.world,
            "OldPeer",
            format!("D10: the one-shot peer exchanged {frames} multiplexed frames"),
        );
    }
    if let Some(why) = clients::failure(&write) {
        cluster.checks.violate(
            &cluster.world,
            "RaftProgress",
            format!("D10: a write failed while the one-shot peer was served: {why}"),
        );
    }
    cluster.restart_gateway(node).await
}

fn invalid(error: serde_json::Error) -> String {
    error.to_string()
}

struct Routes(BTreeSet<Uuid>);

impl ControlRoutes for Routes {
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(self
            .0
            .iter()
            .copied()
            .find(|id| after.is_none_or(|after| *id > after)))
    }
}

/// The old process: what a remote API does before it serves, over the one-shot exchange.
async fn peer(
    node: u64,
    local: WorkerEnrollment,
    tls: TlsMaterial,
    control: GroupIdentity,
    peers: Vec<WorkerEnrollment>,
    data: GroupIdentity,
) -> Result<(), ProtocolError> {
    let shared = WorkerSharedResources::new(
        node,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )?;
    let ids = peers.iter().map(|peer| peer.worker_id).collect();
    let directory = Arc::new(PeerDirectory::default());
    for peer in peers.into_iter().chain([local.clone()]) {
        directory.publish(peer);
    }
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let transport = WorkerTransport::one_shot_for_test(
        &local,
        &tls,
        shared.clone(),
        directory,
        registry,
        TransportConfig::default(),
    )?;
    let client = ControlClient::new(transport.client(), shared, control, Arc::new(Routes(ids)))?;
    let revision = client.group_inventory_revision().await?;
    client.groups(revision, None).await?;
    client.replica_routes(&data).await?;
    Ok(())
}
