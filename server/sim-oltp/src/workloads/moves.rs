//! The `move` workload: `direct` load while the operator moves one Data replica onto a new
//! pool (O26, AZ2). As `o26` and `move` in `tests/oltp_growth_acceptance.py` do it, the
//! harness mounts a secondary volume on the scheduler owner, enrolls it as a pool through
//! the `storage.growth.*` operations, and moves that worker's replica of the table's range
//! to it with a same-ID `storage.replica.move`. The new replica is a learner with a new node
//! ID, so it replays the group's historical membership. Frames from the two other (old)
//! voters are delayed while the move runs. The move must retire within 60 s after the heal.
//!
//! The owner is the only worker that can move a replica onto its own pool. A move onto
//! another worker is accepted only for an enrolled Data worker, which the simulation cannot
//! start yet (`O26:worker-4`).

use std::{path::Path, time::Duration};

use nabedra_check::Gen;
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node},
    time,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{clients::Ctx, direct::SecondPool};
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, WORKERS, initial_range},
    invariants::elapsed_ms,
    nemesis::NetFaults,
    node::Cluster,
};

/// Where the secondary volumes of the moving worker are mounted, one per growth claim.
const EXTENSION: &str = "/sim-oltp-ext";
const FINISH_LIMIT: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_millis(500);
/// One move request may run as long as the acceptance test allows it.
const REQUEST_LIMIT: Duration = Duration::from_secs(75);

enum State {
    Waiting,
    Moving {
        id: Uuid,
        payload: Value,
        attempt: Option<JoinHandle<Result<Value, String>>>,
    },
    Retired,
}

/// One growth claim. It names the worker's incarnation, so a worker that restarts before the
/// claim completes needs another claim and another volume. Until then retries keep its IDs.
struct Claim {
    identity: Value,
    path: String,
    policy_id: Uuid,
    pool_id: Uuid,
    claim: Uuid,
    move_id: Uuid,
}

pub(crate) struct Move {
    start_ms: u64,
    /// The scheduler owner that gets the new pool, once it is known.
    node: Option<u64>,
    delay: Duration,
    state: State,
    next_ms: u64,
    last_error: String,
    faults: Option<NetFaults>,
    claim: Option<Claim>,
    claims: u32,
    /// When the old voters' frames were delayed, in virtual ms.
    delayed: Option<(u64, Option<u64>)>,
}

impl Move {
    pub(crate) fn draw(g: &mut Gen) -> Self {
        Self {
            start_ms: 500 + g.below(5_000),
            node: None,
            delay: Duration::from_millis(5 + g.below(196)),
            state: State::Waiting,
            next_ms: 0,
            last_error: String::new(),
            faults: None,
            claim: None,
            claims: 0,
            delayed: None,
        }
    }

    /// The window in which the old voters' frames were delayed: a fault like any other for
    /// the assertions that need an otherwise healthy cluster.
    pub(crate) fn delayed(&self) -> Option<(u64, u64)> {
        self.delayed
            .map(|(from, to)| (from, to.unwrap_or(u64::MAX)))
    }

    /// The heal cleared every network window.
    pub(crate) fn delay_ended(&mut self, cluster: &Cluster) {
        if let Some((_, to)) = &mut self.delayed {
            to.get_or_insert(elapsed_ms(&cluster.world));
        }
    }

    pub(crate) fn set_faults(&mut self, faults: NetFaults) {
        self.faults = Some(faults);
    }

    /// The second pool's node and device once it is mounted.
    pub(crate) fn second_pool(&self, world: &World) -> Option<SecondPool> {
        let node = self.node?;
        let path = &self.claim.as_ref()?.path;
        let info = world.volume_info(node, Path::new(path))?;
        Some((node, info.dev))
    }

    /// Advance the move by at most one operator step.
    pub(crate) async fn tick(&mut self, cluster: &mut Cluster, ctx: &Ctx) {
        let now = elapsed_ms(&cluster.world);
        if now < self.start_ms || now < self.next_ms {
            return;
        }
        self.next_ms = now + RETRY.as_millis() as u64;
        if let Err(error) = self.step(cluster, ctx).await {
            if error != self.last_error {
                cluster.record(format!("move step: {error}"));
            }
            self.last_error = error;
        }
    }

    async fn step(&mut self, cluster: &mut Cluster, ctx: &Ctx) -> Result<(), String> {
        let node = match self.node {
            Some(node) => node,
            None => {
                let node = owner(cluster, ctx).await?;
                cluster.record(format!("move: scheduler owner n{node}"));
                *self.node.insert(node)
            }
        };
        match &mut self.state {
            State::Waiting => {
                let (id, payload) = self.prepare(cluster, ctx, node).await?;
                if let Some(faults) = &self.faults {
                    let old: Vec<u64> = WORKERS.into_iter().filter(|old| *old != node).collect();
                    faults.slow(Some((old, self.delay)));
                    self.delayed = Some((elapsed_ms(&cluster.world), None));
                }
                cluster.record(format!("move {id} prepared on n{node}: {payload}"));
                self.state = State::Moving {
                    id,
                    payload,
                    attempt: None,
                };
                Ok(())
            }
            State::Moving {
                id,
                payload,
                attempt,
            } => {
                let running = attempt.get_or_insert_with(|| {
                    cluster.admin_task(
                        node,
                        *id,
                        "storage.replica.move",
                        payload.clone(),
                        REQUEST_LIMIT,
                    )
                });
                if !running.is_finished() {
                    return Ok(());
                }
                let answer = attempt.take().expect("finished attempt").await;
                let result = answer.map_err(|error| error.to_string())??;
                if result["phase"]["state"] == "retired" {
                    cluster.record(format!("move {id} retired"));
                    if let Some(faults) = &self.faults {
                        faults.slow(None);
                    }
                    self.state = State::Retired;
                    self.delay_ended(cluster);
                    Ok(())
                } else {
                    Err(format!("move {id} phase {}", result["phase"]))
                }
            }
            State::Retired => Ok(()),
        }
    }

    /// Mount and enroll the secondary pool, then build the move request.
    async fn prepare(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        node: u64,
    ) -> Result<(Uuid, Value), String> {
        let worker = WORKERS
            .iter()
            .position(|candidate| *candidate == node)
            .expect("known worker");
        let identity = cluster.config.worker_identity(worker);
        let zones: serde_json::Map<String, Value> = (0..WORKERS.len())
            .map(|index| {
                let worker = cluster.config.worker_identity(index);
                (
                    worker["worker_id"].as_str().unwrap().to_owned(),
                    json!(format!("fixture-{}", WORKERS[index])),
                )
            })
            .collect();
        if self
            .claim
            .as_ref()
            .is_none_or(|claim| claim.identity != identity)
        {
            let ids = in_node(BOOTSTRAP_CLIENT, async { [(); 4].map(|()| uuid_v4()) }).await;
            let [policy_id, pool_id, claim, move_id] = ids;
            self.claims += 1;
            let path = match self.claims {
                1 => EXTENSION.to_owned(),
                claims => format!("{EXTENSION}-{claims}"),
            };
            self.claim = Some(Claim {
                identity: identity.clone(),
                path,
                policy_id,
                pool_id,
                claim,
                move_id,
            });
        }
        let Claim {
            path,
            policy_id,
            pool_id,
            claim,
            move_id,
            ..
        } = self.claim.as_ref().expect("growth claim");
        let (policy_id, pool_id, claim, move_id) = (*policy_id, *pool_id, *claim, *move_id);
        let policy = json!({
            "policy_id": policy_id, "volume_cost_per_gib": 1, "worker_cost": 1,
            "initial_monthly_spend": 0, "monthly_spend_ceiling": 30, "max_attempts": 3,
            "cooldown_seconds": 1, "max_volume_bytes": 2_u64 << 30,
            "zones": zones.values().cloned().collect::<Vec<_>>(),
            "initial_replica_zones": zones,
        });
        cluster
            .admin("storage.growth.configure", json!({"policy": policy}))
            .await?;
        if cluster.world.volume(node, Path::new(path)).is_none() {
            cluster.world.mount(node, path);
        }
        let mount = in_node(node, async {
            cluster.config.secondary_pool(worker, path, pool_id)
        })
        .await
        .map_err(|error| format!("write the secondary volume marker: {error}"))?;
        let intent = json!({
            "policy_id": policy_id, "claim_id": claim, "worker": identity,
            "pool_id": pool_id, "pool_generation": mount["generation"], "quoted_monthly_cost": 1,
            "monthly_spend_ceiling": 30, "max_attempts": 3, "cooldown_seconds": 1,
            "action": {
                "kind": "add_pool", "worker": identity["worker_id"],
                "zone": format!("fixture-{node}"), "bytes": 1_u64 << 30,
            },
        });
        for (name, payload) in [
            ("storage.growth.claim", json!({"intent": intent})),
            ("storage.growth.attempt", json!({"claim_id": claim})),
            (
                "storage.growth.provisioned",
                json!({"claim_id": claim, "resource_id": mount["provider_volume_id"]}),
            ),
        ] {
            cluster.admin(name, payload).await?;
        }
        let mounted = cluster
            .admin_with_id(
                &[node],
                cluster.split_id().await,
                "storage.growth.mounted",
                json!({"claim_id": claim, "mount": mount}),
            )
            .await?;
        if mounted["phase"] != "complete" {
            return Err(format!("secondary pool not mounted: {mounted}"));
        }
        cluster.config.list_secondary_pool(worker, &mount, claim);
        let (group, placement) = placement(cluster, ctx).await?;
        let members = placement["members"].as_array().cloned().unwrap_or_default();
        let source = members
            .iter()
            .find(|member| member["worker_id"] == identity["worker_id"])
            .cloned()
            .ok_or_else(|| format!("n{node} hosts no replica of {group}: {placement}"))?;
        let mut target = source.clone();
        target["node_id"] = json!(
            members
                .iter()
                .filter_map(|member| member["node_id"].as_u64())
                .max()
                .unwrap_or(0)
                + 1
        );
        target["pool_id"] = json!(pool_id);
        let payload = json!({
            "group": group,
            "expected_generation": placement["record"]["generation"],
            "remove_node": source["node_id"],
            "target": target,
            "worker": identity,
            "pool_generation": mount["generation"],
        });
        Ok((move_id, payload))
    }

    /// Drive the move until it retires, within 60 s of the heal.
    pub(crate) async fn finish(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        healed: Duration,
    ) -> Result<(), String> {
        let deadline = healed + FINISH_LIMIT;
        while !matches!(self.state, State::Retired) {
            if let Err(error) = self.step(cluster, ctx).await {
                if error != self.last_error {
                    cluster.record(format!("move step: {error}"));
                }
                self.last_error = error;
            }
            if cluster.world.elapsed() >= deadline && !matches!(self.state, State::Retired) {
                return Err(format!(
                    "the replica move did not retire within {FINISH_LIMIT:?} after the heal: {}",
                    self.last_error
                ));
            }
            time::sleep(RETRY / 5).await;
        }
        Ok(())
    }
}

/// The table's first range and its current placement.
async fn placement(cluster: &Cluster, ctx: &Ctx) -> Result<(Value, Value), String> {
    let table = Uuid::parse_str(&ctx.table).map_err(|error| error.to_string())?;
    let group = initial_range(cluster.config.cluster_id, table);
    let placement = cluster
        .admin("storage.replica.placement", json!({"group": group}))
        .await?;
    Ok((group, placement))
}

/// The worker that schedules Data groups. Each worker is asked to move its own replica to a
/// pool that does not exist: only the owner gets past the owner check, and then refuses the
/// pool.
async fn owner(cluster: &Cluster, ctx: &Ctx) -> Result<u64, String> {
    let (group, placement) = placement(cluster, ctx).await?;
    let members = placement["members"].as_array().cloned().unwrap_or_default();
    let mut refusals = Vec::new();
    for (index, node) in WORKERS.into_iter().enumerate() {
        if !cluster.is_running(node) || cluster.frozen().contains(&node) {
            continue;
        }
        let identity = cluster.config.worker_identity(index);
        let Some(source) = members
            .iter()
            .find(|member| member["worker_id"] == identity["worker_id"])
        else {
            continue;
        };
        let (id, pool) = in_node(BOOTSTRAP_CLIENT, async { (uuid_v4(), uuid_v4()) }).await;
        let mut target = source.clone();
        target["node_id"] = json!(1_000_000);
        target["pool_id"] = json!(pool);
        let payload = json!({
            "group": group, "expected_generation": placement["record"]["generation"],
            "remove_node": source["node_id"], "target": target, "worker": identity,
            "pool_generation": 1,
        });
        match cluster
            .admin_with_id(&[node], id, "storage.replica.move", payload)
            .await
        {
            Ok(record) => return Err(format!("n{node} accepted an unknown pool: {record}")),
            Err(error) if error.contains("current fenced scheduler owner") => {}
            Err(error) if error.contains("deadline") || error.contains("HTTP") => {
                refusals.push(error)
            }
            Err(error) => {
                cluster.record(format!("move: n{node} passed the owner check: {error}"));
                return Ok(node);
            }
        }
    }
    Err(format!(
        "no worker passed the scheduler owner check: {refusals:?}"
    ))
}
