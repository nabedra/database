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
//! start (`O26:worker-4`). Ownership moves when the owner is lost, so the operator runs in its
//! own task, finds the owner again after every refusal, and reads the move's status before it
//! asks for another one.
//!
//! The O26 assertions of that acceptance test that need no SQL query barrier run here: the pool
//! adds storage and nothing else, a mount with another identity is refused and not written, the
//! move keeps identities and failure domains, the count stays exact, and a pool is drained
//! before it may be detached.

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

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

use super::{clients::Ctx, direct::ExtraPool};
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, ROOT, WORKERS, initial_range},
    history::Completion,
    invariants::elapsed_ms,
    nemesis::{NetFaults, Rule, WindowId},
    node::Cluster,
    operator::{Operator, shown},
};

/// Where the secondary volumes of the moving worker are mounted, one per growth claim.
const EXTENSION: &str = "/sim-oltp-ext";
/// The pool the drained replica moves to.
const REPLACEMENT: &str = "/sim-oltp-ext-replacement";
const FINISH_LIMIT: Duration = Duration::from_secs(60);
/// A pool closes once its last replica is gone (`drain_and_detach`).
const CLOSE_LIMIT: Duration = Duration::from_secs(40);
/// One administration request.
const REQUEST: Duration = Duration::from_secs(10);
/// The worker memory limit of the bootstrap file.
const WORKER_MEMORY: u64 = 2_048 << 20;
const RETRY: Duration = Duration::from_millis(500);
/// One move request may run as long as the acceptance test allows it.
const REQUEST_LIMIT: Duration = Duration::from_secs(75);
/// Growth claims, each on the worker that owns the scheduler role at its time.
const CLAIMS: u32 = 4;

#[derive(Clone, Default)]
enum Phase {
    #[default]
    Waiting,
    Moving {
        id: Uuid,
        node: u64,
        payload: Value,
    },
    Retired,
}

/// One growth claim. It names the worker's incarnation, so a worker that restarts before the
/// claim completes needs another claim and another volume. Until then retries keep its IDs.
#[derive(Clone)]
struct Claim {
    node: u64,
    identity: Value,
    path: String,
    policy_id: Uuid,
    pool_id: Uuid,
    claim: Uuid,
    move_id: Uuid,
    /// The worker's bootstrap file lists the pool.
    listed: bool,
}

#[derive(Default)]
struct Inner {
    phase: Phase,
    claim: Option<Claim>,
    claims: u32,
    /// The inventory of a worker before its first pool was added, and that worker.
    before: Option<(Value, Value)>,
    /// The range and its placement when the move was requested, and the mount it moved to.
    moved: Option<(Value, Value, Value)>,
    /// O26 clauses whose assertions ran.
    clauses: Vec<&'static str>,
    /// When the operator moved the replica, in virtual ms.
    moving: Option<(u64, Option<u64>)>,
    /// Every pool that was mounted.
    mounted: Vec<(u64, String)>,
    slow: Option<WindowId>,
    last_error: String,
    /// Steps of the operator, and the workers that owned the role when it asked for a move.
    steps: u64,
    owners: Vec<u64>,
}

pub(crate) struct Move {
    start_ms: u64,
    delay: Duration,
    inner: Arc<Mutex<Inner>>,
    task: Option<JoinHandle<()>>,
}

/// What the operator's task needs.
struct Driver {
    op: Operator,
    table: String,
    faults: NetFaults,
    start_ms: u64,
    delay: Duration,
    inner: Arc<Mutex<Inner>>,
}

impl Move {
    pub(crate) fn draw(g: &mut Gen) -> Self {
        Self {
            start_ms: 500 + g.below(5_000),
            delay: Duration::from_millis(5 + g.below(196)),
            inner: Arc::default(),
            task: None,
        }
    }

    /// Start the operator.
    pub(crate) fn start(&mut self, cluster: &Cluster, ctx: &Ctx, faults: &NetFaults) {
        let driver = Driver {
            op: cluster.operator(),
            table: ctx.table.clone(),
            faults: faults.clone(),
            start_ms: self.start_ms,
            delay: self.delay,
            inner: self.inner.clone(),
        };
        self.task = Some(nabedra_rt::task::spawn(driver.run()));
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }

    /// The window in which the move ran. A move of the leader's own replica stops the range
    /// for seconds without any fault (move seed 7), so an assertion about one fault alone may
    /// not judge a request that overlaps it.
    pub(crate) fn window(&self) -> Option<(u64, u64)> {
        let moving = self.inner().moving;
        moving.map(|(from, to)| (from, to.unwrap_or(u64::MAX)))
    }

    /// The replica move was requested and has not retired.
    pub(crate) fn running(&self) -> bool {
        matches!(self.inner().phase, Phase::Moving { .. })
    }

    fn retired(&self) -> bool {
        matches!(self.inner().phase, Phase::Retired)
    }

    pub(crate) fn clauses(&self) -> Vec<&'static str> {
        self.inner().clauses.clone()
    }

    pub(crate) fn summary(&self) -> Value {
        let inner = self.inner();
        json!({
            "retired": matches!(inner.phase, Phase::Retired),
            "claims": inner.claims, "owners": inner.owners, "steps": inner.steps,
            "window_ms": inner.moving, "last_error": inner.last_error,
        })
    }

    /// The node and device of every pool the move mounted.
    pub(crate) fn extra_pools(&self, world: &World) -> Vec<ExtraPool> {
        let inner = self.inner();
        let device = |(node, path): &(u64, String)| {
            let info = world.volume_info(*node, Path::new(path))?;
            Some((*node, info.dev))
        };
        inner.mounted.iter().filter_map(device).collect()
    }

    /// Whether the move may still retire in time.
    pub(crate) fn waiting(&self, cluster: &Cluster, healed: Duration) -> bool {
        self.running() && cluster.world.elapsed() < healed + FINISH_LIMIT
    }

    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// Wait until the move retires, within 60 s of the heal.
    pub(crate) async fn finish(
        &mut self,
        cluster: &Cluster,
        healed: Duration,
    ) -> Result<(), String> {
        let deadline = healed + FINISH_LIMIT;
        while !self.retired() && cluster.world.elapsed() < deadline {
            time::sleep(RETRY / 5).await;
        }
        self.stop().await;
        if self.retired() {
            return Ok(());
        }
        // A lost reply is not a stuck move.
        let phase = self.inner().phase.clone();
        if let Phase::Moving { id, .. } = phase
            && let Ok(Some(state)) = status(&cluster.operator(), id).await
        {
            cluster.record(format!("move {id} is {state} in Control at the bound"));
            if state == "retired" {
                self.inner().retire(elapsed_ms(&cluster.world));
                return Ok(());
            }
        }
        let inner = self.inner();
        Err(format!(
            "the replica move did not retire within {FINISH_LIMIT:?} after the heal ({} claims, owners {:?}): {}",
            inner.claims, inner.owners, inner.last_error
        ))
    }

    /// After the move retired: the placement changed exactly as the move said.
    pub(crate) async fn check_retired(
        &mut self,
        cluster: &Cluster,
        ctx: &Ctx,
    ) -> Result<(), String> {
        let (claim, moved) = {
            let inner = self.inner();
            (inner.claim.clone(), inner.moved.clone())
        };
        let (Some(claim), Some((_, before, mount))) = (claim, moved) else {
            return Ok(());
        };
        let target = next_node(&sorted(&before));
        let op = cluster.operator();
        let (_, after) = placement(&op, &ctx.table).await?;
        let identity = op.mounts.identity(worker_of(claim.node));
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
        self.inner().clauses.push("O26:pool-move");
        Ok(())
    }

    /// Why the drain schedule cannot run: it moves the replica once more, from the pool and the
    /// range the first move ended with, and only the owner that moved it can do that.
    pub(crate) fn drain_blocked(&self, cluster: &Cluster, split: bool) -> Option<&'static str> {
        let inner = self.inner();
        let (Phase::Retired, Some(claim)) = (&inner.phase, &inner.claim) else {
            return Some("the replica move did not retire");
        };
        if cluster.config.mounts.identity(worker_of(claim.node)) != claim.identity {
            return Some("the moving worker restarted, so its mount and ownership changed");
        }
        split.then_some("a split replaced the moved range")
    }

    /// Drain the pool the replica moved to.
    pub(crate) async fn drain_pool(&mut self, cluster: &Cluster, ctx: &Ctx) -> Result<(), String> {
        let (claim, moved) = {
            let inner = self.inner();
            (inner.claim.clone(), inner.moved.clone())
        };
        let (Some(claim), Some((_, _, mount))) = (claim, moved) else {
            return Err("no replica moved".into());
        };
        self.drain(&cluster.operator(), &ctx.table, &claim, &mount)
            .await
    }

    /// `drain_and_detach` of the acceptance test: a pool with a live replica may not be
    /// detached. After the replica moved back, the pool closes and the permit is granted.
    async fn drain(
        &mut self,
        op: &Operator,
        table: &str,
        claim: &Claim,
        mount: &Value,
    ) -> Result<(), String> {
        let node = claim.node;
        let id = op.id().await;
        let drain =
            json!({"pool_id": mount["identity"]["pool_id"], "generation": mount["generation"]});
        let violate = |detail: String| {
            op.checks.violate(&op.world, "DrainBeforeDetach", detail);
        };
        let ask = |name, payload| op.ask(node, id, name, payload, REQUEST);
        let answer = ask("storage.pool.drain", drain.clone()).await;
        match &answer {
            Completion::Refused(error) if error.message.contains("replica") => {}
            other => violate(format!(
                "draining a pool with a live replica answered {}",
                shown(other)
            )),
        }
        let permit = ask("storage.pool.detach_permit", json!({})).await;
        if let Completion::Ok(permit) = &permit {
            violate(format!(
                "a pool with a live replica may be detached: {permit}"
            ));
        }
        let status = ask("storage.pool.drain.status", json!({})).await;
        if !matches!(&status, Completion::Ok(status) if status["phase"] == "draining") {
            violate(format!("the drain is not recorded: {}", shown(&status)));
        }

        // Move the replica to a replacement pool.
        let worker = worker_of(node);
        let identity = op.mounts.identity(worker);
        self.inner().mounted.push((node, REPLACEMENT.to_owned()));
        let replacement = add_replacement(op, node, claim.policy_id).await?;
        let (group, current) = placement(op, table).await?;
        let members = sorted(&current);
        let source = members
            .iter()
            .find(|member| member["worker_id"] == identity["worker_id"])
            .cloned()
            .ok_or_else(|| format!("n{node} hosts no replica: {current}"))?;
        let mut target = source.clone();
        target["node_id"] = json!(next_node(&members));
        target["pool_id"] = replacement["identity"]["pool_id"].clone();
        let payload = json!({
            "group": group, "expected_generation": current["record"]["generation"],
            "remove_node": source["node_id"], "target": target, "worker": identity,
            "pool_generation": replacement["generation"],
        });
        let move_id = op.id().await;
        let deadline = op.world.elapsed() + FINISH_LIMIT;
        loop {
            let name = "storage.replica.move";
            let answer = op
                .ask(node, move_id, name, payload.clone(), REQUEST_LIMIT)
                .await;
            match &answer {
                Completion::Ok(result) if result["phase"]["state"] == "retired" => break,
                _ if op.world.elapsed() >= deadline => {
                    return Err(format!(
                        "the replica did not leave the draining pool within {FINISH_LIMIT:?}: {}",
                        shown(&answer)
                    ));
                }
                _ => time::sleep(RETRY).await,
            }
        }
        let deadline = op.world.elapsed() + CLOSE_LIMIT;
        let closed = loop {
            match ask("storage.pool.drain", drain.clone()).await {
                Completion::Ok(closed) => break closed,
                other if op.world.elapsed() >= deadline => {
                    return Err(format!(
                        "the drained pool did not close within {CLOSE_LIMIT:?}: {}",
                        shown(&other)
                    ));
                }
                _ => time::sleep(RETRY).await,
            }
        };
        let permit = ask("storage.pool.detach_permit", json!({})).await;
        if closed["phase"] != "closed"
            || closed["mount"]["expected"] != *mount
            || !matches!(&permit, Completion::Ok(permit) if *permit == closed)
        {
            violate(format!(
                "the pool closed as {closed}, permit {}",
                shown(&permit)
            ));
        }
        op.mounts.unlist(worker, &mount["identity"]["pool_id"]);
        self.inner().clauses.push("O26:drain");
        Ok(())
    }
}

impl Drop for Move {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Inner {
    fn retire(&mut self, at: u64) {
        self.phase = Phase::Retired;
        if let Some((_, to)) = &mut self.moving {
            to.get_or_insert(at);
        }
    }
}

impl Driver {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }

    async fn run(self) {
        while elapsed_ms(&self.op.world) < self.start_ms {
            time::sleep(RETRY / 10).await;
        }
        loop {
            let step = self.step().await;
            self.inner().steps += 1;
            match step {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    let mut inner = self.inner();
                    if inner.last_error != error {
                        self.op.record(format!("move step: {error}"));
                        inner.last_error = error;
                    }
                }
            }
            time::sleep(RETRY).await;
        }
    }

    /// One step of the operator. Returns whether the move retired.
    async fn step(&self) -> Result<bool, String> {
        let phase = self.inner().phase.clone();
        let (id, node, payload) = match phase {
            Phase::Retired => return Ok(true),
            Phase::Waiting => {
                let node = owner(&self.op, &self.table).await?;
                self.prepare(node).await?;
                return Ok(false);
            }
            Phase::Moving { id, node, payload } => (id, node, payload),
        };
        let mut last = Vec::new();
        // The worker that started the move, and the others if it can't go on: the role may have
        // moved, and only its holder answers for the move.
        for worker in [node].into_iter().chain(others(node)) {
            let answer = self
                .op
                .ask(
                    worker,
                    id,
                    "storage.replica.move",
                    payload.clone(),
                    REQUEST_LIMIT,
                )
                .await;
            last.push(format!("n{worker} {}", shown(&answer)));
            let (state, answered) = match &answer {
                Completion::Ok(record) => (state(record), true),
                _ => match status(&self.op, id).await {
                    Ok(state) => (state.unwrap_or_else(|| UNKNOWN.to_owned()), false),
                    Err(error) => {
                        last.push(error);
                        continue;
                    }
                },
            };
            match after(&state) {
                Next::Retired => return Ok(self.retired(id, worker)),
                // Never started, or given up by its owner: whoever owns the role now is asked
                // for a new one.
                Next::Again => return Err(self.again(id, &state, &last)),
                Next::Same if answered => return Err(format!("move {id} is {state}")),
                Next::Same => last.push(format!("Control says {state}")),
            }
        }
        Err(last.join("; "))
    }

    fn retired(&self, id: Uuid, worker: u64) -> bool {
        self.op.record(format!("move {id} retired (n{worker})"));
        let mut inner = self.inner();
        if let Some(slow) = inner.slow.take() {
            self.faults.end(slow);
        }
        inner.retire(elapsed_ms(&self.op.world));
        true
    }

    fn again(&self, id: Uuid, state: &str, last: &[String]) -> String {
        let mut inner = self.inner();
        inner.phase = Phase::Waiting;
        // The ID of a cancelled move stays bound to it.
        if state == "cancelled" {
            inner.claim = None;
        }
        format!("move {id} is {state}, asked for again: {}", last.join("; "))
    }

    /// Mount and enroll a secondary pool on the owner, then request the move.
    async fn prepare(&self, node: u64) -> Result<(), String> {
        let op = &self.op;
        let worker = worker_of(node);
        let identity = op.mounts.identity(worker);
        let zones: serde_json::Map<String, Value> = (0..WORKERS.len())
            .map(|index| {
                let worker = op.mounts.identity(index);
                (
                    worker["worker_id"].as_str().unwrap().to_owned(),
                    json!(format!("fixture-{}", WORKERS[index])),
                )
            })
            .collect();
        let known = self.inner().claim.clone();
        let claim = match known {
            Some(claim) if claim.identity == identity => claim,
            _ => {
                let ids = in_node(BOOTSTRAP_CLIENT, async { [(); 4].map(|()| uuid_v4()) }).await;
                let [policy_id, pool_id, claim, move_id] = ids;
                let mut inner = self.inner();
                if inner.claims == CLAIMS {
                    return Err(format!("{CLAIMS} growth claims did not move the replica"));
                }
                inner.claims += 1;
                let path = match inner.claims {
                    1 => EXTENSION.to_owned(),
                    claims => format!("{EXTENSION}-{claims}"),
                };
                inner.mounted.push((node, path.clone()));
                let claim = Claim {
                    node,
                    identity: identity.clone(),
                    path,
                    policy_id,
                    pool_id,
                    claim,
                    move_id,
                    listed: false,
                };
                inner.claim = Some(claim.clone());
                claim
            }
        };
        // Everything below is about this incarnation of the owner. A restart in between makes
        // the observations of two processes.
        let same = || {
            (op.mounts.identity(worker) == identity)
                .then_some(())
                .ok_or_else(|| format!("n{node} restarted while its pool was prepared"))
        };
        let known = self.inner().before.clone();
        let before = match known {
            Some((of, before)) if of == identity => before,
            _ => {
                let before = inventory(op, node).await?;
                same()?;
                self.inner().before = Some((identity.clone(), before.clone()));
                before
            }
        };
        let policy = json!({
            "policy_id": claim.policy_id, "volume_cost_per_gib": 1, "worker_cost": 1,
            "initial_monthly_spend": 0, "monthly_spend_ceiling": 30, "max_attempts": 3,
            "cooldown_seconds": 1, "max_volume_bytes": 2_u64 << 30,
            "zones": zones.values().cloned().collect::<Vec<_>>(),
            "initial_replica_zones": zones,
        });
        admin(op, "storage.growth.configure", json!({"policy": policy})).await?;
        if op.world.volume(node, Path::new(&claim.path)).is_none() {
            op.world.mount(node, &claim.path);
        }
        let mount = in_node(node, async {
            op.mounts.new_pool(worker, &claim.path, claim.pool_id)
        })
        .await
        .map_err(|error| format!("write the secondary volume marker: {error}"))?;
        let intent = json!({
            "policy_id": claim.policy_id, "claim_id": claim.claim, "worker": identity,
            "pool_id": claim.pool_id, "pool_generation": mount["generation"],
            "quoted_monthly_cost": 1,
            "monthly_spend_ceiling": 30, "max_attempts": 3, "cooldown_seconds": 1,
            "action": {
                "kind": "add_pool", "worker": identity["worker_id"],
                "zone": format!("fixture-{node}"), "bytes": 1_u64 << 30,
            },
        });
        // The claim's record says how far it got. Asking for every step again would use up its
        // attempts, and then it can never complete.
        let record = admin(op, "storage.growth.claim", json!({"intent": intent})).await?;
        let mut phase = record["phase"].as_str().unwrap_or_default().to_owned();
        if let Some(payload) = attempt(&phase, claim.claim) {
            admin(op, "storage.growth.attempt", payload).await?;
            phase = "provisioning".into();
        }
        if phase == "provisioning" {
            let resource = &mount["provider_volume_id"];
            let payload = json!({"claim_id": claim.claim, "resource_id": resource});
            admin(op, "storage.growth.provisioned", payload).await?;
        }
        if phase != "complete" {
            if !self.inner().clauses.contains(&"O26:mount-identity") {
                let altered = op.mounts.identity((worker + 1) % WORKERS.len());
                let checked = refused_mounts(op, node, claim.claim, &mount, altered, same).await?;
                self.inner().clauses.extend(checked);
            }
            let payload = json!({"claim_id": claim.claim, "mount": mount});
            let answer = op
                .ask(
                    node,
                    op.id().await,
                    "storage.growth.mounted",
                    payload,
                    REQUEST,
                )
                .await;
            let Completion::Ok(mounted) = &answer else {
                return Err(format!("mount the secondary pool: {}", shown(&answer)));
            };
            if mounted["phase"] != "complete" {
                return Err(format!("secondary pool not mounted: {mounted}"));
            }
        }
        let listed = {
            let mut inner = self.inner();
            let claim = inner.claim.as_mut().expect("growth claim");
            std::mem::replace(&mut claim.listed, true)
        };
        if !listed {
            same()?;
            op.mounts.list(worker, &mount, claim.claim);
        }
        if !self.inner().clauses.contains(&"O26:growth") {
            let after = inventory(op, node).await?;
            let stats = op.stats(node).await?;
            same()?;
            check_growth(op, &before, &after, &stats);
            self.inner().clauses.extend(["O26:growth", "O26:budgets"]);
        }
        let (group, placement) = placement(op, &self.table).await?;
        let members = placement["members"].as_array().cloned().unwrap_or_default();
        let source = members
            .iter()
            .find(|member| member["worker_id"] == identity["worker_id"])
            .cloned()
            .ok_or_else(|| format!("n{node} hosts no replica of {group}: {placement}"))?;
        let mut target = source.clone();
        target["node_id"] = json!(next_node(&members));
        target["pool_id"] = json!(claim.pool_id);
        let payload = json!({
            "group": group,
            "expected_generation": placement["record"]["generation"],
            "remove_node": source["node_id"],
            "target": target,
            "worker": identity,
            "pool_generation": mount["generation"],
        });
        op.record(format!(
            "move {} prepared on n{node}: {payload}",
            claim.move_id
        ));
        let now = elapsed_ms(&op.world);
        let mut inner = self.inner();
        let old: Vec<u64> = others(node).collect();
        if let Some(slow) = inner.slow.take() {
            self.faults.end(slow);
        }
        inner.slow = Some(self.faults.start(Rule::Slow {
            workers: old,
            by: self.delay,
        }));
        inner.moving.get_or_insert((now, None));
        inner.owners.push(node);
        inner.moved = Some((group, placement, mount));
        inner.phase = Phase::Moving {
            id: claim.move_id,
            node,
            payload,
        };
        Ok(())
    }
}

fn others(node: u64) -> impl Iterator<Item = u64> {
    WORKERS.into_iter().filter(move |other| *other != node)
}

fn next_node(members: &[Value]) -> u64 {
    let nodes = members
        .iter()
        .filter_map(|member| member["node_id"].as_u64());
    nodes.max().unwrap_or(0) + 1
}

/// The payload of a claim's next attempt. Only a claim that none was recorded for takes one.
fn attempt(phase: &str, claim: Uuid) -> Option<Value> {
    (phase == "claimed").then(|| json!({"claim_id": claim}))
}

async fn admin(op: &Operator, name: &str, payload: Value) -> Result<Value, String> {
    op.any(op.id().await, name, &payload, REQUEST).await
}

/// Control has no record of the move.
const UNKNOWN: &str = "unknown";

/// What the operator does about a move in a state.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    Retired,
    Same,
    Again,
}

fn after(state: &str) -> Next {
    match state {
        "retired" => Next::Retired,
        "cancelled" | UNKNOWN => Next::Again,
        _ => Next::Same,
    }
}

fn state(record: &Value) -> String {
    let state = record["phase"]["state"].as_str();
    state.unwrap_or("without a state").to_owned()
}

/// The state that Control records for a move, or `None` when it has none.
async fn status(op: &Operator, id: Uuid) -> Result<Option<String>, String> {
    let mut last = Vec::new();
    for node in WORKERS {
        let name = "storage.replica.move.status";
        let answer = op.ask(node, id, name, json!({}), REQUEST).await;
        match &answer {
            Completion::Ok(record) => return Ok(Some(state(record))),
            Completion::Refused(error) if error.message == "replica move is unknown" => {
                return Ok(None);
            }
            _ => last.push(format!("n{node} {}", shown(&answer))),
        }
    }
    Err(format!("move status: {}", last.join("; ")))
}

/// `add_pool`: claim, attempt, provision and mount one more pool on the worker.
async fn add_replacement(op: &Operator, node: u64, policy: Uuid) -> Result<Value, String> {
    let worker = worker_of(node);
    let identity = op.mounts.identity(worker);
    let [pool_id, claim] = in_node(BOOTSTRAP_CLIENT, async { [(); 2].map(|()| uuid_v4()) }).await;
    op.world.mount(node, REPLACEMENT);
    let mount = in_node(node, async {
        op.mounts.new_pool(worker, REPLACEMENT, pool_id)
    })
    .await
    .map_err(|error| format!("write the replacement volume marker: {error}"))?;
    let intent = json!({
        "policy_id": policy, "claim_id": claim, "worker": identity, "pool_id": pool_id,
        "pool_generation": mount["generation"], "quoted_monthly_cost": 1,
        "monthly_spend_ceiling": 30, "max_attempts": 3, "cooldown_seconds": 1,
        "action": {
            "kind": "add_pool", "worker": identity["worker_id"],
            "zone": format!("fixture-{node}"), "bytes": 1_u64 << 30,
        },
    });
    let resource = mount["provider_volume_id"].clone();
    let steps = [
        ("storage.growth.claim", json!({"intent": intent})),
        ("storage.growth.attempt", json!({"claim_id": claim})),
        (
            "storage.growth.provisioned",
            json!({"claim_id": claim, "resource_id": resource}),
        ),
        (
            "storage.growth.mounted",
            json!({"claim_id": claim, "mount": mount}),
        ),
    ];
    let mut last = Value::Null;
    for (name, payload) in steps {
        let answer = op.ask(node, op.id().await, name, payload, REQUEST).await;
        let Completion::Ok(data) = answer else {
            return Err(format!("{name}: {}", shown(&answer)));
        };
        last = data;
    }
    if last["phase"] != "complete" {
        return Err(format!("the replacement pool is not mounted: {last}"));
    }
    op.mounts.list(worker, &mount, claim);
    Ok(mount)
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

async fn inventory(op: &Operator, node: u64) -> Result<Value, String> {
    let name = "storage.growth.inventory";
    let answer = op.ask(node, op.id().await, name, json!({}), REQUEST).await;
    match answer {
        Completion::Ok(inventory) => Ok(inventory),
        other => Err(format!("{name}: {}", shown(&other))),
    }
}

/// `o26`: the pool adds storage, and no compute, queue replica, cache or write buffer.
fn check_growth(op: &Operator, before: &Value, after: &Value, stats: &Value) {
    let pools = |inventory: &Value| inventory["pools"].as_array().map_or(0, Vec::len);
    let available = |inventory: &Value| inventory["total_available_bytes"].as_u64().unwrap_or(0);
    let grew = before["worker"] == after["worker"]
        && before["scheduler_node_id"] == after["scheduler_node_id"]
        && pools(after) == pools(before) + 1
        && available(after) > available(before)
        && stats.pointer("/storage/pools").and_then(Value::as_u64) == Some(pools(after) as u64);
    if !grew {
        op.checks.violate(
            &op.world,
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
        op.checks.violate(
            &op.world,
            "AggregateBudgets",
            format!("the budgets went from {before} to {after}"),
        );
    }
}

/// `add_pool` with `check_identity`: a mount of another path, worker, provider volume or marker
/// is refused before anything is written to it. Returns the clause once every variant was
/// answered.
async fn refused_mounts(
    op: &Operator,
    node: u64,
    claim: Uuid,
    mount: &Value,
    other_worker: Value,
    same: impl Fn() -> Result<(), String>,
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
        let payload = json!({"claim_id": claim, "mount": mount});
        let name = "storage.growth.mounted";
        match op.ask(node, op.id().await, name, payload, REQUEST).await {
            Completion::Refused(error) if error.outcome == Outcome::NotApplied => {}
            // Another fault hid the answer. The next attempt asks again.
            Completion::Refused(error) => {
                return Err(format!("altered {field}: {}", error.message));
            }
            Completion::Unknown(reason) => return Err(format!("altered {field}: {reason}")),
            Completion::Ok(answer) => op.checks.violate(
                &op.world,
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
    same()?;
    if root != ["root-disk-sentinel"] || pool != [".nabedra-docker-volume.json"] {
        op.checks.violate(
            &op.world,
            "MountIdentity",
            format!("a refused mount was written: {root:?} and {pool:?}"),
        );
    }
    Ok(Some("O26:mount-identity"))
}

/// The table's first range and its current placement.
async fn placement(op: &Operator, table: &str) -> Result<(Value, Value), String> {
    let table = Uuid::parse_str(table).map_err(|error| error.to_string())?;
    let group = initial_range(op.cluster_id, table);
    let placement = admin(op, "storage.replica.placement", json!({"group": group})).await?;
    Ok((group, placement))
}

/// The worker that schedules Data groups. Each worker is asked to move its own replica to a
/// pool that does not exist, and only the owner gets as far as the pool.
async fn owner(op: &Operator, table: &str) -> Result<u64, String> {
    let (group, placement) = placement(op, table).await?;
    let members = placement["members"].as_array().cloned().unwrap_or_default();
    let mut refusals = Vec::new();
    for (index, node) in WORKERS.into_iter().enumerate() {
        let identity = op.mounts.identity(index);
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
        let answer = op
            .ask(node, id, "storage.replica.move", payload, REQUEST)
            .await;
        match &answer {
            Completion::Ok(record) => {
                return Err(format!("n{node} accepted an unknown pool: {record}"));
            }
            answer if passed_owner_check(answer) => {
                op.record(format!("move: scheduler owner n{node}"));
                return Ok(node);
            }
            answer => refusals.push(format!("n{node} {}", shown(answer))),
        }
    }
    Err(format!(
        "no worker passed the scheduler owner check: {refusals:?}"
    ))
}

/// Only the owner looks at the target pool. Any other answer, a lost one too, says nothing
/// about the role.
fn passed_owner_check(answer: &Completion) -> bool {
    matches!(answer, Completion::Refused(error) if error.message == "placement pool is not registered")
}

#[cfg(test)]
mod tests {
    use nabedra_protocol::protocol::{ErrorCode, ProtocolError};

    use super::*;

    /// Review 115, H5: a refused connection, a busy Control and an unavailable worker passed
    /// for the owner, and the move then asked that worker for ever.
    #[test]
    fn only_the_answer_about_the_pool_names_the_owner() {
        let refused = |code, message: &str| {
            let error = ProtocolError::new(code, message, Outcome::NotApplied);
            passed_owner_check(&Completion::Refused(error))
        };
        let unavailable = ErrorCode::StorageUnavailable;
        assert!(refused(unavailable, "placement pool is not registered"));
        assert!(!refused(
            unavailable,
            "worker runtime is not accepting requests"
        ));
        assert!(!refused(
            ErrorCode::Conflict,
            "a replica move is still active"
        ));
        let connect = "connect n1: connection refused".to_owned();
        assert!(!passed_owner_check(&Completion::Unknown(connect)));
    }

    /// Move seeds 374, 488 and 683: the owner answered that it had cancelled the move, and the
    /// operator asked for the same one until the end.
    #[test]
    fn a_cancelled_move_is_not_asked_for_under_its_old_id() {
        let cancelled = json!({"phase": {"state": "cancelled"}});
        assert_eq!(after(&state(&cancelled)), Next::Again);
        assert_eq!(after(UNKNOWN), Next::Again);
        assert_eq!(after("retired"), Next::Retired);
        for open in ["claimed", "moving", "published", "without a state"] {
            assert_eq!(after(open), Next::Same);
        }
    }

    /// Move seed 2: every retry asked for the whole claim again. The third one used up its
    /// attempts, and Control answered `NotReady` for the rest of the run.
    #[test]
    fn a_claim_takes_one_attempt() {
        assert!(attempt("claimed", Uuid::nil()).is_some());
        for phase in ["provisioning", "awaiting_mount", "complete"] {
            assert!(attempt(phase, Uuid::nil()).is_none(), "{phase}");
        }
    }

    fn driver(inner: &Arc<Mutex<Inner>>, world: &Arc<World>) -> Driver {
        Driver {
            op: Operator::for_test(world),
            table: String::new(),
            faults: NetFaults::for_test(world),
            start_ms: 0,
            delay: Duration::ZERO,
            inner: inner.clone(),
        }
    }

    /// A move that its owner never started, or cancelled, is asked for again on whoever owns
    /// the role then. Only a cancelled one needs another ID.
    #[test]
    fn an_unknown_or_cancelled_move_is_asked_for_again() {
        nabedra_sim::runtime::run(1, |world| async move {
            for (state, keeps) in [("unknown", true), ("cancelled", false)] {
                let inner = Arc::new(Mutex::new(Inner {
                    phase: Phase::Moving {
                        id: Uuid::nil(),
                        node: 1,
                        payload: Value::Null,
                    },
                    claim: Some(Claim {
                        node: 1,
                        identity: Value::Null,
                        path: EXTENSION.into(),
                        policy_id: Uuid::nil(),
                        pool_id: Uuid::nil(),
                        claim: Uuid::nil(),
                        move_id: Uuid::nil(),
                        listed: false,
                    }),
                    ..Inner::default()
                }));
                driver(&inner, &world).again(Uuid::nil(), state, &[]);
                let inner = inner.lock().unwrap();
                assert!(matches!(inner.phase, Phase::Waiting));
                assert_eq!(inner.claim.is_some(), keeps);
            }
        });
    }
}
