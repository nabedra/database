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
//! start (`O26:worker-4`).
//!
//! The O26 assertions of that acceptance test that need no SQL query barrier run here: the pool
//! adds storage and nothing else, a mount with another identity is refused and not written, the
//! move keeps identities and failure domains, the count stays exact, and a pool is drained
//! before it may be detached.

use std::{path::Path, time::Duration};

use nabedra_check::Gen;
use nabedra_protocol::protocol::Outcome;
use nabedra_rt::{
    fs::os,
    ids::uuid_v4,
    sim::{World, in_node},
    time,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{clients::Ctx, direct::SecondPool};
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, ROOT, WORKERS, initial_range},
    history::Completion,
    invariants::elapsed_ms,
    nemesis::NetFaults,
    node::Cluster,
};

/// Where the secondary volumes of the moving worker are mounted, one per growth claim.
const EXTENSION: &str = "/sim-oltp-ext";
const FINISH_LIMIT: Duration = Duration::from_secs(60);
/// A pool closes once its last replica is gone (`drain_and_detach`).
const CLOSE_LIMIT: Duration = Duration::from_secs(40);
const DRAIN_REQUEST: Duration = Duration::from_secs(10);
/// The worker memory limit of the bootstrap file.
const WORKER_MEMORY: u64 = 2_048 << 20;
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
    /// The owner's inventory before the pool was added.
    before: Option<Value>,
    /// The range and its placement when the move was requested, and the mount it moved to.
    moved: Option<(Value, Value, Value)>,
    /// O26 clauses whose assertions ran.
    pub clauses: Vec<&'static str>,
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
            before: None,
            moved: None,
            clauses: Vec::new(),
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

    /// The second pool that [`Move::prepare`] mounted on `node`.
    pub(super) fn second_pool_of(&self, world: &World, node: u64) -> Option<SecondPool> {
        let path = &self.claim.as_ref()?.path;
        let info = world.volume_info(node, Path::new(path))?;
        Some((node, info.dev))
    }

    /// The next [`Move::prepare`] mounts another pool. A pool that held a replica of the group
    /// keeps its retired state, and is refused as a target.
    pub(super) fn another_pool(&mut self) {
        self.claim = None;
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
    pub(super) async fn prepare(
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
        if self.before.is_none() {
            self.before = Some(inventory(cluster, node).await?);
        }
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
        if !self.clauses.contains(&"O26:mount-identity") {
            let altered = (worker + 1) % WORKERS.len();
            let altered = cluster.config.worker_identity(altered);
            self.clauses
                .extend(refused_mounts(cluster, node, claim, &mount, altered).await?);
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
        if !self.clauses.contains(&"O26:growth") {
            let after = inventory(cluster, node).await?;
            let before = self.before.as_ref().expect("inventory before the mount");
            check_growth(cluster, node, before, &after).await?;
            self.clauses.extend(["O26:growth", "O26:budgets"]);
        }
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
        self.moved = Some((group, placement, mount));
        Ok((move_id, payload))
    }

    /// After the move retired: the placement changed exactly as the move said, and the pool is
    /// drained before it may be detached.
    pub(crate) async fn check_retired(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
    ) -> Result<(), String> {
        let (State::Retired, Some(node), Some((_, before, mount))) =
            (&self.state, self.node, self.moved.clone())
        else {
            return Ok(());
        };
        let target = before["members"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|member| member["node_id"].as_u64())
            .max()
            .unwrap_or(0)
            + 1;
        let (_, after) = placement(cluster, ctx).await?;
        let worker = worker_of(node);
        let identity = cluster.config.worker_identity(worker);
        let expected: Vec<Value> = sorted(&before)
            .into_iter()
            .map(|mut member| {
                if member["worker_id"] == identity["worker_id"] {
                    member["node_id"] = json!(target);
                    member["pool_id"] = mount["identity"]["pool_id"].clone();
                }
                member
            })
            .collect();
        let mut got = sorted(&after);
        let mut want = expected;
        let by_worker = |member: &Value| member["worker_id"].to_string();
        got.sort_by_key(by_worker);
        want.sort_by_key(by_worker);
        let mut workers: Vec<_> = got.iter().map(by_worker).collect();
        workers.dedup();
        if got != want
            || workers.len() != WORKERS.len()
            || before["record"]["identity"] != after["record"]["identity"]
            || before["record"]["generation"] == after["record"]["generation"]
        {
            cluster.checks.violate(
                &cluster.world,
                "MovePlacement",
                format!("the retired move went from {before} to {after}"),
            );
        }
        self.clauses.push("O26:pool-move");
        self.drain(cluster, ctx, node, &mount).await
    }

    /// `drain_and_detach` of the acceptance test: a pool with a live replica may not be
    /// detached. After the replica moved back, the pool closes and the permit is granted.
    async fn drain(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        node: u64,
        mount: &Value,
    ) -> Result<(), String> {
        let id = cluster.split_id().await;
        let drain =
            json!({"pool_id": mount["identity"]["pool_id"], "generation": mount["generation"]});
        let violate = |cluster: &Cluster, detail: String| {
            cluster
                .checks
                .violate(&cluster.world, "DrainBeforeDetach", detail);
        };
        let answer = cluster
            .admin_answer(node, id, "storage.pool.drain", drain.clone(), DRAIN_REQUEST)
            .await?;
        match &answer {
            Completion::Refused(error) if error.message.contains("replica") => {}
            other => violate(
                cluster,
                format!(
                    "draining a pool with a live replica answered {}",
                    shown(other)
                ),
            ),
        }
        let permit = cluster
            .admin_answer(
                node,
                id,
                "storage.pool.detach_permit",
                json!({}),
                DRAIN_REQUEST,
            )
            .await?;
        if let Completion::Ok(permit) = &permit {
            violate(
                cluster,
                format!("a pool with a live replica may be detached: {permit}"),
            );
        }
        let status = cluster
            .admin_answer(
                node,
                id,
                "storage.pool.drain.status",
                json!({}),
                DRAIN_REQUEST,
            )
            .await?;
        if !matches!(&status, Completion::Ok(status) if status["phase"] == "draining") {
            violate(
                cluster,
                format!("the drain is not recorded: {}", shown(&status)),
            );
        }

        // Move the replica back to the primary pool.
        let worker = worker_of(node);
        let identity = cluster.config.worker_identity(worker);
        let (group, current) = placement(cluster, ctx).await?;
        let members = sorted(&current);
        let source = members
            .iter()
            .find(|member| member["worker_id"] == identity["worker_id"])
            .cloned()
            .ok_or_else(|| format!("n{node} hosts no replica: {current}"))?;
        let mut target = source.clone();
        let next = members
            .iter()
            .filter_map(|member| member["node_id"].as_u64())
            .max()
            .unwrap_or(0)
            + 1;
        target["node_id"] = json!(next);
        target["pool_id"] = cluster.config.pool(worker);
        let payload = json!({
            "group": group, "expected_generation": current["record"]["generation"],
            "remove_node": source["node_id"], "target": target, "worker": identity,
            "pool_generation": cluster.config.mount_generation(worker),
        });
        let move_id = cluster.split_id().await;
        let deadline = cluster.world.elapsed() + FINISH_LIMIT;
        loop {
            let answer = cluster
                .admin_answer(
                    node,
                    move_id,
                    "storage.replica.move",
                    payload.clone(),
                    REQUEST_LIMIT,
                )
                .await;
            match &answer {
                Ok(Completion::Ok(result)) if result["phase"]["state"] == "retired" => break,
                _ if cluster.world.elapsed() >= deadline => {
                    let last = answer.map(|answer| shown(&answer));
                    return Err(format!(
                        "the replica did not move back within {FINISH_LIMIT:?}: {last:?}"
                    ));
                }
                _ => time::sleep(RETRY).await,
            }
        }
        let deadline = cluster.world.elapsed() + CLOSE_LIMIT;
        let closed = loop {
            let answer = cluster
                .admin_answer(node, id, "storage.pool.drain", drain.clone(), DRAIN_REQUEST)
                .await;
            match answer {
                Ok(Completion::Ok(closed)) => break closed,
                other if cluster.world.elapsed() >= deadline => {
                    let last = other.map(|answer| shown(&answer));
                    return Err(format!(
                        "the drained pool did not close within {CLOSE_LIMIT:?}: {last:?}"
                    ));
                }
                _ => time::sleep(RETRY).await,
            }
        };
        let permit = cluster
            .admin_answer(
                node,
                id,
                "storage.pool.detach_permit",
                json!({}),
                DRAIN_REQUEST,
            )
            .await?;
        if closed["phase"] != "closed"
            || closed["mount"]["expected"] != *mount
            || !matches!(&permit, Completion::Ok(permit) if *permit == closed)
        {
            violate(
                cluster,
                format!("the pool closed as {closed}, permit {}", shown(&permit)),
            );
        }
        cluster.config.unlist_secondary_pools(worker);
        self.clauses.push("O26:drain");
        Ok(())
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

fn worker_of(node: u64) -> usize {
    WORKERS
        .iter()
        .position(|candidate| *candidate == node)
        .expect("known worker")
}

fn sorted(placement: &Value) -> Vec<Value> {
    let mut members = placement["members"].as_array().cloned().unwrap_or_default();
    members.sort_by_key(|member| member["node_id"].as_u64());
    members
}

fn shown(answer: &Completion) -> String {
    match answer {
        Completion::Ok(value) => format!("ok {value}"),
        Completion::Refused(error) => format!("{:?} {}", error.code, error.message),
        Completion::Unknown(reason) => format!("unknown ({reason})"),
    }
}

async fn inventory(cluster: &Cluster, node: u64) -> Result<Value, String> {
    let id = cluster.split_id().await;
    cluster
        .admin_with_id(&[node], id, "storage.growth.inventory", json!({}))
        .await
}

/// `o26`: the pool adds storage, and no compute, queue replica, cache or write buffer.
async fn check_growth(
    cluster: &Cluster,
    node: u64,
    before: &Value,
    after: &Value,
) -> Result<(), String> {
    let pools = |inventory: &Value| inventory["pools"].as_array().map_or(0, Vec::len);
    let available = |inventory: &Value| inventory["total_available_bytes"].as_u64().unwrap_or(0);
    let stats = cluster.stats(node).await?;
    let grew = before["worker"] == after["worker"]
        && before["scheduler_node_id"] == after["scheduler_node_id"]
        && pools(after) == pools(before) + 1
        && available(after) > available(before)
        && stats.pointer("/storage/pools").and_then(Value::as_u64) == Some(2);
    if !grew {
        cluster.checks.violate(
            &cluster.world,
            "PoolGrowth",
            format!(
                "the inventory went from {before} to {after}, storage {}",
                stats["storage"]
            ),
        );
    }
    let budgets = [
        "thread_budget",
        "cache_bytes",
        "write_buffer_bytes",
        "native_baseline_bytes",
    ];
    let same = budgets
        .iter()
        .all(|field| !before[*field].is_null() && before[*field] == after[*field]);
    if !same
        || after["charged_memory_bytes"]
            .as_u64()
            .is_none_or(|bytes| bytes > WORKER_MEMORY)
    {
        cluster.checks.violate(
            &cluster.world,
            "AggregateBudgets",
            format!("the budgets went from {before} to {after}"),
        );
    }
    Ok(())
}

/// `add_pool` with `check_identity`: a mount of another path, worker, provider volume or marker
/// is refused before anything is written to it. Returns the clause once every variant was
/// answered.
async fn refused_mounts(
    cluster: &Cluster,
    node: u64,
    claim: Uuid,
    mount: &Value,
    other_worker: Value,
) -> Result<Option<&'static str>, String> {
    let absent = format!("{ROOT}/absent-attachment");
    in_node(node, async {
        os::create_dir_all(&absent)?;
        let mut sentinel = os::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(format!("{absent}/root-disk-sentinel"))?;
        std::io::Write::write_all(&mut sentinel, b"must survive mount refusal")
    })
    .await
    .map_err(|error| format!("write the sentinel: {error}"))?;
    let path = mount["mount_path"].as_str().expect("mount path").to_owned();
    let altered = [
        ("mount_path", json!(absent)),
        ("worker", other_worker),
        ("provider_volume_id", json!("docker-volume:other")),
        (
            "profile",
            json!({"docker": {"volume_marker": Uuid::from_u128(1)}}),
        ),
    ];
    for (field, value) in altered {
        let mut mount = mount.clone();
        mount[field] = value;
        let id = cluster.split_id().await;
        let payload = json!({"claim_id": claim, "mount": mount});
        match cluster
            .admin_answer(node, id, "storage.growth.mounted", payload, DRAIN_REQUEST)
            .await?
        {
            Completion::Refused(error) if error.outcome == Outcome::NotApplied => {}
            // Another fault hid the answer. The next attempt asks again.
            Completion::Refused(error) => {
                return Err(format!("altered {field}: {}", error.message));
            }
            Completion::Unknown(reason) => return Err(format!("altered {field}: {reason}")),
            Completion::Ok(answer) => cluster.checks.violate(
                &cluster.world,
                "MountIdentity",
                format!("a mount with another {field} was accepted: {answer}"),
            ),
        }
    }
    let listed = |dir: String| async move {
        in_node(node, async {
            let mut names = Vec::new();
            for entry in os::read_dir(&dir)? {
                names.push(entry?.file_name().to_string_lossy().into_owned());
            }
            names.sort();
            std::io::Result::Ok(names)
        })
        .await
        .map_err(|error| format!("list the mount: {error}"))
    };
    let (root, pool) = (listed(absent).await?, listed(path).await?);
    if root != ["root-disk-sentinel"] || pool != [".nabedra-docker-volume.json"] {
        cluster.checks.violate(
            &cluster.world,
            "MountIdentity",
            format!("a refused mount was written: {root:?} and {pool:?}"),
        );
    }
    Ok(Some("O26:mount-identity"))
}

/// The table's first range and its current placement.
pub(super) async fn placement(cluster: &Cluster, ctx: &Ctx) -> Result<(Value, Value), String> {
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
pub(super) async fn owner(cluster: &Cluster, ctx: &Ctx) -> Result<u64, String> {
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
