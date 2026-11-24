//! The `move` workload: `direct` load while the operator moves one Data replica onto a new
//! pool (O26, AZ2). As `o26` and `move` in `tests/oltp_growth_acceptance.py` do it, the
//! harness mounts a secondary volume on one worker, enrolls it as a pool through the
//! `storage.growth.*` operations, and moves that worker's replica of the table's range to it
//! with a same-ID `storage.replica.move`. The new replica is a learner with a new node ID,
//! so it replays the group's historical membership. Frames from the two other (old) voters
//! are delayed while the move runs. The move must retire within 60 s after the heal.

use std::{path::Path, time::Duration};

use nabedra_check::Gen;
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use serde_json::{Value, json};
use uuid::Uuid;

use super::clients::Ctx;
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, WORKERS, initial_range},
    invariants::elapsed_ms,
    nemesis::NetFaults,
    node::Cluster,
};

/// Where the secondary volume is mounted on the moving worker.
pub(crate) const EXTENSION: &str = "/sim-oltp-ext";
const FINISH_LIMIT: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_millis(500);

enum State {
    Waiting,
    Moving { id: Uuid, payload: Value },
    Retired(Value),
}

pub(crate) struct Move {
    start_ms: u64,
    /// Index of the worker that gets the new pool.
    worker: usize,
    delay: Duration,
    state: State,
    next_ms: u64,
    last_error: String,
    faults: Option<NetFaults>,
}

impl Move {
    pub(crate) fn draw(g: &mut Gen) -> Self {
        Self {
            start_ms: 500 + g.below(5_000),
            worker: g.below(WORKERS.len() as u64) as usize,
            delay: Duration::from_millis(5 + g.below(196)),
            state: State::Waiting,
            next_ms: 0,
            last_error: String::new(),
            faults: None,
        }
    }

    /// The worker whose pools and resource gauges change during the move.
    pub(crate) fn node(&self) -> u64 {
        WORKERS[self.worker]
    }

    pub(crate) fn set_faults(&mut self, faults: NetFaults) {
        self.faults = Some(faults);
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
        match &self.state {
            State::Waiting => {
                if !cluster.is_running(self.node()) {
                    return Err(format!("n{} is not running", self.node()));
                }
                let (id, payload) = self.prepare(cluster, ctx).await?;
                if let Some(faults) = &self.faults {
                    let old: Vec<u64> = WORKERS
                        .into_iter()
                        .filter(|node| *node != self.node())
                        .collect();
                    faults.slow(Some((old, self.delay)));
                }
                cluster.record(format!("move {id} prepared: {payload}"));
                self.state = State::Moving { id, payload };
                Ok(())
            }
            State::Moving { id, payload } => {
                // The scheduler owner creates the learner's Data group; the others refuse.
                let result = cluster
                    .admin_with_id(&WORKERS, *id, "storage.replica.move", payload.clone())
                    .await?;
                if result["phase"]["state"] == "retired" {
                    cluster.record(format!("move {id} retired"));
                    if let Some(faults) = &self.faults {
                        faults.slow(None);
                    }
                    self.state = State::Retired(result);
                    Ok(())
                } else {
                    Err(format!("move {id} phase {}", result["phase"]))
                }
            }
            State::Retired(_) => Ok(()),
        }
    }

    /// Mount and enroll the secondary pool, then build the move request.
    async fn prepare(&mut self, cluster: &mut Cluster, ctx: &Ctx) -> Result<(Uuid, Value), String> {
        let node = self.node();
        let identity = cluster.config.worker_identity(self.worker);
        let zones: serde_json::Map<String, Value> = (0..WORKERS.len())
            .map(|index| {
                let worker = cluster.config.worker_identity(index);
                (
                    worker["worker_id"].as_str().unwrap().to_owned(),
                    json!(format!("fixture-{}", WORKERS[index])),
                )
            })
            .collect();
        let ids = in_node(BOOTSTRAP_CLIENT, async {
            [(); 4].map(|()| uuid_v4())
        })
        .await;
        let [policy_id, pool_id, claim, move_id] = ids;
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
        if cluster.world.volume(node, Path::new(EXTENSION)).is_none() {
            cluster.world.mount(node, EXTENSION);
        }
        let mount = in_node(node, async {
            cluster.config.secondary_pool(self.worker, EXTENSION, pool_id)
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
        let table = Uuid::parse_str(&ctx.table).map_err(|error| error.to_string())?;
        let group = initial_range(cluster.config.cluster_id, table);
        let placement = cluster
            .admin("storage.replica.placement", json!({"group": group}))
            .await?;
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

    /// After the heal: drive the move until it retires, within 60 s.
    pub(crate) async fn finish(&mut self, cluster: &mut Cluster, ctx: &Ctx) -> Result<(), String> {
        let deadline = cluster.world.elapsed() + FINISH_LIMIT;
        while !matches!(self.state, State::Retired(_)) {
            if let Err(error) = self.step(cluster, ctx).await {
                self.last_error = error;
            }
            if matches!(self.state, State::Retired(_)) {
                break;
            }
            if cluster.world.elapsed() >= deadline {
                return Err(format!(
                    "the replica move did not retire within {FINISH_LIMIT:?} after the heal: {}",
                    self.last_error
                ));
            }
            time::sleep(RETRY).await;
        }
        let State::Retired(result) = &self.state else {
            unreachable!()
        };
        cluster.record(format!("move retired: {}", result["phase"]));
        Ok(())
    }
}
