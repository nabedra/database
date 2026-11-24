//! O06, "Reject corrupt/foreign replicas": a stopped worker is started on a volume that claims
//! another pool, and then on its own volume with one journal bit flipped. Neither start may
//! serve: the worker refuses to start, or it quarantines the damaged group. With the bit
//! restored the same volume starts again, and every acknowledged row is still there.

use std::path::{Path, PathBuf};

use nabedra_rt::{fs::os, ids::uuid_v4, sim::in_node};
use nabedra_sim::{Draw, rng::stream};

use super::{
    Workload,
    clients::{self, Ctx, Request},
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, ROOT, WORKERS},
    disk::DiskCut,
    node::Cluster,
};

/// Rows written before the cut. The first one's log record is the one that gets damaged, so
/// valid records follow it and it cannot pass for a torn tail.
const ROWS: i64 = 8;

/// Whether this seed runs the stage. One `direct` seed in four does.
pub(crate) fn selected(seed: u64, workload: Workload, forced: bool) -> bool {
    workload == Workload::Direct && (forced || stream(seed, "oltp-o06-corrupt").chance(250))
}

/// Run the stage and return the clauses whose assertions ran.
pub(crate) async fn run(cluster: &mut Cluster, ctx: &Ctx) -> Vec<&'static str> {
    let mut clauses = Vec::new();
    if let Err(error) = stage(cluster, ctx, &mut clauses).await {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("O06 stage: {error}"));
    }
    clauses
}

async fn stage(
    cluster: &mut Cluster,
    ctx: &Ctx,
    clauses: &mut Vec<&'static str>,
) -> Result<(), String> {
    let index = (ctx.world.seed() % WORKERS.len() as u64) as usize;
    let victim = WORKERS[index];
    let mut client = ctx.new_client();
    let mut marker = None;
    for row in 0..ROWS {
        let id = in_node(CLIENTS[0], async { uuid_v4() }).await;
        let request = Request::row(
            "table.upsert",
            &ctx.table,
            1 + row % ctx.keys,
            700_000_000 + row,
            id,
        );
        let answer = clients::attempt(
            ctx,
            &mut client,
            CLIENTS[0],
            GATEWAYS[(row % 2) as usize],
            &request,
            false,
        )
        .await;
        if let Some(why) = clients::failure(&answer) {
            return Err(format!("the healthy cluster refused a write: {why}"));
        }
        marker.get_or_insert(id);
    }
    let marker = marker.expect("a written row");
    let segments = in_node(victim, async { journal_segments(Path::new(ROOT)) })
        .await
        .map_err(|error| format!("list the journal of n{victim}: {error}"))?;
    let other = (index + 1) % WORKERS.len();

    cluster.crash(victim, DiskCut::Kill).await;
    cluster.prepare_restart(victim).await?;
    let own = cluster.config.pool(index);
    let foreign = cluster.config.pool(other);
    cluster.config.claim_pool(index, &foreign);
    let foreign = cluster.start_prepared(index, DiskCut::Kill).await;
    cluster.config.claim_pool(index, &own);
    cluster.record(format!("O06 foreign pool on n{victim}: {foreign:?}"));
    clauses.push("O06:foreign-replica");
    if foreign.is_ok() {
        cluster.checks.violate(
            &cluster.world,
            "ForeignReplicaServed",
            format!("n{victim} became ready on a volume of another pool"),
        );
        cluster.crash(victim, DiskCut::Kill).await;
    }

    let image = cluster
        .world
        .volume(victim, Path::new(ROOT))
        .ok_or("no mounted volume")?;
    let needle = marker.to_string();
    let found = segments.iter().rev().find_map(|path| {
        let bytes = image.durable_contents(path)?;
        let at = bytes
            .windows(needle.len())
            .rposition(|window| window == needle.as_bytes())?;
        Some((path.clone(), at as u64))
    });
    let Some((path, offset)) = found else {
        return Err(format!(
            "the acknowledged request {marker} is in no durable journal segment of n{victim}: {segments:?}"
        ));
    };
    image
        .flip_bit(&path, offset, 0)
        .map_err(|error| format!("flip a bit: {error}"))?;
    let corrupt = cluster.restart_worker(victim, DiskCut::Kill).await;
    cluster.record(format!(
        "O06 flipped {}@{offset} on n{victim}: {corrupt:?}",
        path.display()
    ));
    clauses.push("O06:corrupt-log");
    if corrupt.is_ok() {
        // The process may run only with the damage reported.
        let stats = cluster.stats(victim).await?;
        cluster.record(format!(
            "O06 damaged n{victim}: refused {} storage {}",
            stats["refused_pools"], stats["storage"]
        ));
        let quarantined = stats
            .pointer("/storage/quarantined_groups")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if quarantined == 0 {
            cluster.checks.violate(
                &cluster.world,
                "CorruptReplicaServed",
                format!(
                    "n{victim} became ready with a flipped bit at {}@{offset} and reports no damage",
                    path.display()
                ),
            );
        }
        cluster.crash(victim, DiskCut::Kill).await;
    }
    let image = cluster
        .world
        .volume(victim, Path::new(ROOT))
        .ok_or("no mounted volume")?;
    image
        .flip_bit(&path, offset, 0)
        .map_err(|error| format!("restore the bit: {error}"))?;
    cluster
        .restart_worker(victim, DiskCut::Kill)
        .await
        .map_err(|error| format!("n{victim} did not start on its restored volume: {error}"))?;
    Ok(())
}

/// The live journal segments under `root`, oldest first.
fn journal_segments(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut segments = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in os::read_dir(&dir)? {
            let path = entry?.path();
            if os::metadata(&path)?.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "jnl") {
                segments.push(path);
            }
        }
    }
    segments.sort();
    Ok(segments)
}
