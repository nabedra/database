//! The `topology` workload (design 3.6, phase 11): splits, moves and leader transfers under
//! load, index publication, a table drop, and gateway route caches with random staleness.
//!
//! The phase 10 nemesis runs unchanged next to the topology operations. The run has these
//! tables:
//!
//! - `main` has the direct clients of the phase 10 workloads, auditors and indexes. The
//!   nemesis splits it under load when its profile picks `split`.
//! - `ranged` has fewer clients, auditors and indexes. It is split before any client starts or
//!   under load, as the seed draws. A split under load never publishes today (see
//!   `wave-p8/reports/p10-workloads.md`), so those seeds report `Liveness`.
//! - `victim` is written by its own clients and dropped under load, with an index publication
//!   racing the drop, and then created again under the same name.
//! - `moved` has one client and a replica move, in the seeds that draw one.
//! - `quiet` is written once, before the chaos. Both gateways keep a route to it that the
//!   topology changes since then have aged. Its stage at the end changes the topology once more
//!   and then uses each cached route, with nothing else running.
//! - `fence` has one row. At the end pairs of declarations race for one index name on it, so that
//!   one of them fails its validation while it holds the table's guard.
//!
//! Auditors own keys that nobody else writes and scan a covering index over them, so an
//! acknowledged insert must be in every later scan. The rules are in
//! [`crate::invariants::topology`]; this module records what happened. The history goes through
//! the `direct` checks.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_protocol::protocol::{ErrorCode, Outcome};
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use serde_json::{Value, json};
use tokio::{sync::Semaphore, task::JoinHandle};
use uuid::Uuid;

use super::{
    Scenarios, Workload,
    clauses::{self, Answer},
    clients::{self, Ctx, Plan, Request},
    direct::{self, CHAOS, CONVERGE_LIMIT},
    moves,
};
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, CLIENTS, GATEWAYS, WORKERS, initial_range},
    client::{operation, request},
    config::Config,
    history::{Client, Completion, History, Operation},
    invariants::{
        elapsed_ms,
        topology::{
            self, AtRest, Chosen, Ended, RULES, Refresh, Scan, Status, TableCall, Write,
            audit_scans, dropped_silent, fence_at_rest, incomplete_chosen, refreshes_once,
            refused_ddl, route_count,
        },
    },
    nemesis::{Action, NetFaults, Selection, apply::Chaos},
    node::Cluster,
};

/// `NABEDRA_SIM_TOPOLOGY`: force parts of the drawn plan, for diagnosis and for running a
/// search both ways. A comma list of `split-under-load`, `split-before-load`, `move`,
/// `no-move`, and `drop-race` or `no-drop-race` for the index publication that races the drop.
/// An unknown entry is an error.
pub(crate) const MODE_ENV: &str = "NABEDRA_SIM_TOPOLOGY";
/// `NABEDRA_SIM_TOPOLOGY_DEFECT`: one seeded defect of [`Defect`].
pub(crate) const DEFECT_ENV: &str = "NABEDRA_SIM_TOPOLOGY_DEFECT";

const TICK: Duration = Duration::from_millis(50);
const OPERATIONS: &str = "/v1/operations";
const ADMIN: &str = "/v1/cluster/operations";
const SETUP_LIMIT: Duration = Duration::from_secs(30);
/// Bound for a split or an index build after the heal. The design bounds both but gives a
/// number only for the move, which is the one used here.
const CHANGE_LIMIT: Duration = Duration::from_secs(60);
/// Bound for an index build after the heal. A worker builds one index at a time, and a build
/// sends every row of the table through the queue, so builds of two tables take minutes.
const BUILD_LIMIT: Duration = Duration::from_secs(180);
/// One operator request. A client-side timeout cancels its handler.
const ADMIN_LIMIT: Duration = Duration::from_secs(10);
/// A query takes the queued path, whose acceptance test allows 30 s.
const QUERY_LIMIT: Duration = Duration::from_secs(30);
/// How long a table at rest may keep falling back before it counts as stuck.
const REST_LIMIT: Duration = Duration::from_secs(30);
const STATUS_POLL: Duration = Duration::from_millis(300);
const RETRY: Duration = Duration::from_millis(250);
/// Rounds of definite, retryable refusals before an index is declared under a new build ID, as
/// `create_d11_index` in `tests/oltp_direct_acceptance.py` does it.
const FRESH_BUILD: u32 = 12;
/// Audited values start here, above every value another writer uses.
const FLOOR: i64 = 1_000_000_000_000;
/// Audited keys are at least this far from zero, on both sides of every split boundary.
const AUDIT_KEYS: i64 = 1_000_000;
const QUIET_KEYS: i64 = 10;
/// Declarations that race for one name, until one is refused under the table's guard.
const RACES: usize = 4;
const VICTIM_KEYS: i64 = 6;

/// Seeded defects. `LoseRow` and `FakeDrop` change what the cluster does; the others change
/// one observation the rule reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Defect {
    /// Delete one acknowledged audited row behind the clients' back.
    LoseRow,
    /// Report the same row twice in one scan.
    DuplicateRow,
    /// Report a building index as chosen by an explain.
    ChooseBuilding,
    /// Acknowledge the drop without sending it.
    FakeDrop,
    /// Count a stale-route answer twice.
    RefreshTwice,
    /// Report one accepted index declaration as refused.
    LieAboutRefusal,
    /// Report one mutation at rest as sent to the queued path because of the schema.
    StuckFence,
    /// Report one range too few in a query plan.
    LoseRange,
}

impl Defect {
    const ALL: [(Defect, &'static str); 8] = [
        (Defect::LoseRow, "lose-row"),
        (Defect::DuplicateRow, "duplicate-row"),
        (Defect::ChooseBuilding, "choose-building"),
        (Defect::FakeDrop, "fake-drop"),
        (Defect::RefreshTwice, "refresh-twice"),
        (Defect::LieAboutRefusal, "lie-about-refusal"),
        (Defect::StuckFence, "stuck-fence"),
        (Defect::LoseRange, "lose-range"),
    ];

    fn from_env() -> Result<Option<Self>, String> {
        let Ok(name) = std::env::var(DEFECT_ENV) else {
            return Ok(None);
        };
        if name.is_empty() || name == "none" {
            return Ok(None);
        }
        Self::ALL
            .iter()
            .find(|(_, known)| *known == name)
            .map(|(defect, _)| Some(*defect))
            .ok_or_else(|| format!("unknown {DEFECT_ENV} {name:?}"))
    }
}

/// When `ranged` splits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitTiming {
    BeforeLoad,
    UnderLoad,
}

/// What [`MODE_ENV`] forces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Forced {
    split: Option<SplitTiming>,
    moving: Option<bool>,
    drop_race: Option<bool>,
}

impl Forced {
    fn parse(list: &str) -> Result<Self, String> {
        let mut forced = Self::default();
        for item in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match item {
                "split-under-load" => forced.split = Some(SplitTiming::UnderLoad),
                "split-before-load" => forced.split = Some(SplitTiming::BeforeLoad),
                "move" => forced.moving = Some(true),
                "no-move" => forced.moving = Some(false),
                "drop-race" => forced.drop_race = Some(true),
                "no-drop-race" => forced.drop_race = Some(false),
                other => return Err(format!("unknown {MODE_ENV} entry {other:?}")),
            }
        }
        Ok(forced)
    }

    fn from_env() -> Result<Self, String> {
        Self::parse(&std::env::var(MODE_ENV).unwrap_or_default())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Table {
    Main,
    Ranged,
    Victim,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Table::Main => "main",
            Table::Ranged => "ranged",
            Table::Victim => "victim",
        }
    }
}

/// What an index declaration is expected to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexKind {
    Valid,
    /// Declares the same name as the index it races.
    SameName,
    /// Names a column the table does not have.
    UnknownColumn,
}

#[derive(Clone, Debug)]
struct IndexPlan {
    table: Table,
    name: String,
    kind: IndexKind,
    start: Duration,
    /// Retire the index this long after it is ready.
    retire: Option<Duration>,
}

/// A gateway that stops running while the topology changes, so that it continues with the
/// routes it had.
#[derive(Clone, Copy, Debug)]
struct StaleWindow {
    gateway: u64,
    at: u64,
    hold: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuietChange {
    Split,
    Index,
}

#[derive(Clone, Copy, Debug)]
enum AuditStep {
    Insert { left: bool },
    Update(u64),
    Delete(u64),
}

#[derive(Clone, Copy, Debug)]
struct Audit {
    step: AuditStep,
    gateway: usize,
    switch: bool,
    scan: Option<usize>,
    explain: Option<usize>,
    think: Duration,
}

/// Everything the run draws, from the tape and before it starts.
struct TopoPlan {
    main: Plan,
    ranged: Plan,
    moved: Plan,
    split: SplitTiming,
    split_at: Duration,
    boundary: i64,
    indexes: Vec<IndexPlan>,
    drop_at: Duration,
    drop_gateway: usize,
    moving: Option<moves::Move>,
    stale: Vec<StaleWindow>,
    quiet: QuietChange,
    quiet_boundary: i64,
    quiet_age: Duration,
    rewarm: bool,
    /// Where the search for the rival's moment starts, in per mille of a declaration at rest.
    race_lead: u64,
    audits: Vec<(Table, Vec<Audit>)>,
}

fn ms(g: &mut Gen, from: u64, to: u64) -> Duration {
    Duration::from_millis(from + g.below(to - from + 1))
}

impl TopoPlan {
    fn draw(g: &mut Gen, selection: &Selection, forced: Forced) -> Self {
        let idle = selection.focus.idle;
        let chaos = CHAOS.as_millis() as u64;
        let main = Plan::draw(g, selection.focus.contention, idle);
        let mut ranged = Plan::draw(g, selection.focus.contention, idle);
        ranged.programs.truncate(3);
        let mut moved = Plan::draw(g, selection.focus.contention, idle);
        moved.programs.truncate(1);
        let drawn = if g.bool() {
            SplitTiming::UnderLoad
        } else {
            SplitTiming::BeforeLoad
        };
        let split_at = ms(g, 1_000, 12_000);
        let boundary = 2 + g.below(ranged.keys.max(2) as u64 - 1) as i64;
        let mut indexes = Vec::new();
        for table in [Table::Main, Table::Ranged] {
            let start = ms(g, 500, 12_000);
            let name = format!("{}_value", table.name());
            indexes.push(IndexPlan {
                table,
                name: name.clone(),
                kind: IndexKind::Valid,
                start,
                retire: g.chance(1, 3).then(|| ms(g, 500, 5_000)),
            });
            // A rival declaration of the same name fails its validation, before or after the
            // coordinator holds the table's guard.
            if g.bool() {
                let lead = ms(g, 0, 400);
                indexes.push(IndexPlan {
                    table,
                    name,
                    kind: IndexKind::SameName,
                    start: if g.bool() {
                        start + lead
                    } else {
                        start.saturating_sub(lead)
                    },
                    retire: None,
                });
            }
            if g.chance(1, 3) {
                indexes.push(IndexPlan {
                    table,
                    name: format!("{}_missing", table.name()),
                    kind: IndexKind::UnknownColumn,
                    start: ms(g, 500, 12_000),
                    retire: None,
                });
            }
        }
        let drop_at = ms(g, 3_000, 15_000);
        let drop_gateway = g.below(2) as usize;
        // An index publication that races the drop, in half of the seeds.
        let racing = IndexPlan {
            table: Table::Victim,
            name: "victim_value".into(),
            kind: IndexKind::Valid,
            start: drop_at.saturating_sub(ms(g, 0, 1_500)),
            retire: None,
        };
        let drawn_race = g.bool();
        if forced.drop_race.unwrap_or(drawn_race) {
            indexes.push(racing);
        }
        let moving = g.bool();
        let moving = forced
            .moving
            .unwrap_or(moving)
            .then(|| moves::Move::draw(g));
        let stale = (0..1 + g.below(3))
            .map(|_| StaleWindow {
                gateway: *g.pick(&GATEWAYS),
                at: 500 + g.below(chaos - 4_000),
                hold: 100 + g.below(3_000),
            })
            .collect();
        let quiet = if g.bool() {
            QuietChange::Split
        } else {
            QuietChange::Index
        };
        let audits = [Table::Main, Table::Ranged]
            .into_iter()
            .filter(|_| !idle)
            .map(|table| (table, (0..64).map(|_| draw_audit(g)).collect()))
            .collect();
        Self {
            main,
            ranged,
            moved,
            split: forced.split.unwrap_or(drawn),
            split_at,
            boundary,
            indexes,
            drop_at,
            drop_gateway,
            moving,
            stale,
            quiet,
            quiet_boundary: 2 + g.below(QUIET_KEYS as u64 - 2) as i64,
            quiet_age: ms(g, 0, 5_000),
            rewarm: g.bool(),
            race_lead: 300 + g.below(601),
            audits,
        }
    }
}

fn draw_audit(g: &mut Gen) -> Audit {
    let step = match g.below(10) {
        0..6 => AuditStep::Insert { left: g.bool() },
        6..9 => AuditStep::Update(g.below(1 << 20)),
        _ => AuditStep::Delete(g.below(1 << 20)),
    };
    Audit {
        step,
        gateway: g.below(2) as usize,
        switch: g.bool(),
        scan: g.chance(2, 5).then(|| g.below(2) as usize),
        explain: g.chance(1, 5).then(|| g.below(2) as usize),
        think: Duration::from_millis(20 + g.below(400)),
    }
}

/// What one index build did, in virtual milliseconds.
#[derive(Clone, Debug)]
struct Build {
    table: Table,
    name: String,
    kind: IndexKind,
    declared: Option<u64>,
    ready: Option<u64>,
    retired: Option<u64>,
    /// The declaration was refused for good.
    refused: bool,
    /// A full stop ended the run the build belongs to, so it stays invisible.
    abandoned: bool,
    /// Why the build did not settle before its bound.
    stuck: Option<String>,
    /// A request of it was never answered for good, or the build has not published.
    open: bool,
}

#[derive(Default)]
struct Log {
    writes: BTreeMap<Table, Vec<Write>>,
    scans: BTreeMap<Table, Vec<Scan>>,
    /// Scans in virtual ms, for the scenario clauses.
    scan_ms: Vec<(Table, u64, u64)>,
    created: BTreeSet<String>,
    statuses: Vec<Status>,
    chosen: Vec<Chosen>,
    /// When each explain ran, in virtual ms.
    explain_ms: Vec<(Table, u64)>,
    builds: Vec<Build>,
    /// Refused index builds and their selectors.
    refused: BTreeMap<Uuid, String>,
    selectors: BTreeMap<Uuid, Value>,
    dropped: Option<u64>,
    calls: Vec<TableCall>,
    /// Splits of `ranged`: requested, published.
    split: Option<(u64, Option<u64>)>,
    split_error: Option<String>,
    /// How many split IDs were sent for `ranged`.
    split_ids: u64,
    /// What each fence race did.
    races: Vec<String>,
    /// What the tables did at rest.
    rest: Vec<String>,
    defect: Option<Defect>,
}

/// State the tasks of a run share.
#[derive(Clone)]
struct Topo {
    main: Ctx,
    ranged: Ctx,
    victim: Ctx,
    admin_token: String,
    log: Arc<Mutex<Log>>,
    /// One order over every observation the rules read.
    order: Arc<AtomicU64>,
    /// The worker run, which the workload loop follows.
    run: Arc<Mutex<Uuid>>,
    /// Virtual ms by which topology changes must settle; unbounded during chaos.
    change_by: Arc<AtomicU64>,
    /// A worker serves two administration requests at a time. The operator sends one, so that
    /// the other is free for restarts and the replica move. The heal holds this permit.
    gate: Arc<Semaphore>,
}

/// What the workers answered to one operator request.
enum Reply {
    Ok(Value),
    /// A worker that executes it refused it for good.
    Definite(ErrorCode, String),
    /// Nothing definite, or only refusals that ask for a retry.
    Retry {
        why: String,
        unknown: bool,
    },
}

impl Reply {
    fn shown(&self) -> String {
        match self {
            Reply::Ok(data) => format!("ok {}", data["state"]),
            Reply::Definite(_, why) => why.clone(),
            Reply::Retry { why, unknown } => format!("{why} (unknown {unknown})"),
        }
    }
}

impl Topo {
    fn ctx(&self, table: Table) -> &Ctx {
        match table {
            Table::Main => &self.main,
            Table::Ranged => &self.ranged,
            Table::Victim => &self.victim,
        }
    }

    fn next(&self) -> u64 {
        self.order.fetch_add(1, Ordering::AcqRel)
    }

    fn now(&self) -> u64 {
        elapsed_ms(&self.main.world)
    }

    /// The chaos window is over and the heal has not finished.
    fn healing(&self) -> bool {
        self.main.stopping.load(Ordering::Acquire)
            && self.change_by.load(Ordering::Acquire) == u64::MAX
    }

    fn change_over(&self) -> bool {
        self.now() >= self.change_by.load(Ordering::Acquire)
    }

    fn builds_over(&self) -> bool {
        let extra = (BUILD_LIMIT - CHANGE_LIMIT).as_millis() as u64;
        let by = self.change_by.load(Ordering::Acquire);
        self.now() >= by.saturating_add(if by == 0 { 0 } else { extra })
    }

    fn log<R>(&self, f: impl FnOnce(&mut Log) -> R) -> R {
        f(&mut self.log.lock().unwrap())
    }

    /// Whether the seeded defect is `defect`; a defect that is taken applies once.
    fn take_defect(&self, defect: Defect) -> bool {
        self.log(|log| {
            let due = log.defect == Some(defect);
            if due {
                log.defect = None;
            }
            due
        })
    }

    fn note(&self, what: &str, detail: Value) {
        self.main
            .history
            .lock()
            .unwrap()
            .marker(&self.main.world, what, detail);
    }

    /// One operator request, sent to each worker until one that executes it answers.
    async fn admin(&self, id: Uuid, name: &str, payload: &Value) -> Reply {
        let _permit = self.gate.acquire().await.expect("open gate");
        self.send(id, name, payload).await
    }

    async fn send(&self, id: Uuid, name: &str, payload: &Value) -> Reply {
        let mut why = Vec::new();
        let mut unknown = false;
        for node in WORKERS {
            let body = operation(id, name, payload.clone());
            let answer = request(
                BOOTSTRAP_CLIENT,
                node,
                ADMIN,
                &self.admin_token,
                Some(body),
                ADMIN_LIMIT,
            )
            .await
            .and_then(|reply| reply.completion(id));
            match answer {
                Ok(Completion::Ok(data)) => return Reply::Ok(data),
                Ok(Completion::Refused(error)) => {
                    let message = format!("n{node} {:?}: {}", error.code, error.message);
                    if error.outcome == Outcome::NotApplied && definite(error.code) {
                        return Reply::Definite(error.code, message);
                    }
                    unknown |= error.outcome == Outcome::Unknown;
                    why.push(message);
                }
                Ok(Completion::Unknown(error)) | Err(error) => {
                    // A refused connection carried no request.
                    unknown |= !error.starts_with("connect ");
                    why.push(format!("n{node} {error}"));
                }
            }
        }
        Reply::Retry {
            why: why.join("; "),
            unknown,
        }
    }

    /// One public operation that the per-key checks do not see.
    async fn public(
        &self,
        client: &mut Client,
        node: u64,
        gateway: usize,
        id: Uuid,
        name: &str,
        payload: Value,
    ) -> Completion {
        let limit = match name {
            "table.drop" => ADMIN_LIMIT,
            _ => QUERY_LIMIT,
        };
        let attempt = self.main.history.lock().unwrap().invoke(
            client,
            &self.main.world,
            GATEWAYS[gateway],
            Operation {
                table: None,
                name,
                key: None,
                request_id: id,
                value: payload.clone(),
            },
        );
        let completion = match request(
            node,
            GATEWAYS[gateway],
            OPERATIONS,
            &self.main.token,
            Some(operation(id, name, payload)),
            limit,
        )
        .await
        .and_then(|reply| reply.completion(id))
        {
            Ok(completion) => completion,
            Err(error) => Completion::Unknown(error),
        };
        let recorded = copy(&completion);
        self.main
            .history
            .lock()
            .unwrap()
            .complete(client, &self.main.world, attempt, recorded);
        completion
    }
}

fn definite(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::ValidationError
            | ErrorCode::UnsupportedOperation
            | ErrorCode::NotFound
            | ErrorCode::Conflict
            | ErrorCode::LimitExceeded
    )
}

/// A definite refusal because another schema change or another owner came first. The request
/// ID keeps that result, so the operator asks again under a new one. A name or an ID that is
/// taken stays taken, and a split that its source refuses to freeze is the same split.
fn overtaken(code: ErrorCode, why: &str) -> bool {
    const FINAL: [&str; 4] = [
        "already exists",
        "already binds",
        "differs from Catalog",
        "source refused the exact split freeze",
    ];
    code == ErrorCode::Conflict && !FINAL.iter().any(|text| why.contains(text))
}

fn copy(completion: &Completion) -> Completion {
    match completion {
        Completion::Ok(value) => Completion::Ok(value.clone()),
        Completion::Refused(error) => Completion::Refused(error.clone()),
        Completion::Unknown(reason) => Completion::Unknown(reason.clone()),
    }
}

async fn fresh_id(node: u64) -> Uuid {
    in_node(node, async { uuid_v4() }).await
}

/// Create a table, retrying the same ID.
async fn create_table(
    cluster: &Cluster,
    history: &Arc<Mutex<History>>,
    name: &str,
) -> Result<String, String> {
    let node = CLIENTS[0];
    let id = fresh_id(node).await;
    let payload = json!({
        "name": name, "schema": {"id": "int64", "value": "int64"},
        "key": "id", "format": "parquet",
    });
    let mut client = history.lock().unwrap().client();
    let deadline = cluster.world.elapsed() + SETUP_LIMIT;
    loop {
        let attempt = history.lock().unwrap().invoke(
            &client,
            &cluster.world,
            GATEWAYS[0],
            Operation {
                table: None,
                name: "table.create",
                key: None,
                request_id: id,
                value: payload.clone(),
            },
        );
        let completion = match request(
            node,
            GATEWAYS[0],
            OPERATIONS,
            &cluster.config.data_token,
            Some(operation(id, "table.create", payload.clone())),
            SETUP_LIMIT,
        )
        .await
        .and_then(|reply| reply.completion(id))
        {
            Ok(completion) => completion,
            Err(error) => Completion::Unknown(error),
        };
        let recorded = copy(&completion);
        history
            .lock()
            .unwrap()
            .complete(&mut client, &cluster.world, attempt, recorded);
        match completion {
            Completion::Ok(created) => {
                return created["id"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("table.create of {name} lacks an id: {created}"));
            }
            other if cluster.world.elapsed() >= deadline || !clients::unknown(&other) => {
                let why = clients::failure(&other).unwrap_or_default();
                return Err(format!("table.create of {name}: {why}"));
            }
            _ => time::sleep(Duration::from_millis(200)).await,
        }
    }
}

// Audits.

fn scan_query() -> Value {
    json!({
        "select": ["id", "value"],
        "filter": {"column": "value", "op": "ge", "value": FLOOR},
        "order_by": null, "limit": null,
    })
}

fn rows(data: &Value) -> Option<Vec<(i64, i64)>> {
    data.as_array()?
        .iter()
        .map(|row| Some((row["id"].as_i64()?, row["value"].as_i64()?)))
        .collect()
}

/// The index a plan names, and how many ranges it covers.
fn plan_of(explanation: &str) -> (Option<String>, Option<u64>) {
    let index = explanation
        .split("access secondary ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .map(str::to_owned);
    let ranges = explanation
        .split("scope shards ")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|digits| digits.parse().ok());
    (index, ranges)
}

/// One scan of the audited values. A refusal or a lost reply shows nothing.
async fn scan(
    t: &Topo,
    client: &mut Client,
    node: u64,
    gateway: usize,
    table: Table,
) -> Option<String> {
    let id = fresh_id(node).await;
    let payload = json!({"table_id": t.ctx(table).table, "query": scan_query()});
    let (invoked, from) = (t.next(), t.now());
    let answer = t
        .public(client, node, gateway, id, "query.collect", payload)
        .await;
    let (completed, to) = (t.next(), t.now());
    let Completion::Ok(data) = &answer else {
        return Some(clients::failure(&answer).unwrap_or_default());
    };
    let Some(mut rows) = rows(data) else {
        t.main.checks.violate(
            &t.main.world,
            "HttpEnvelope",
            format!("query.collect answered {data}"),
        );
        return None;
    };
    if let Some(&row) = rows.first()
        && t.take_defect(Defect::DuplicateRow)
    {
        rows.push(row);
    }
    t.log(|log| {
        log.scans.entry(table).or_default().push(Scan {
            invoked,
            completed,
            rows,
        });
        log.scan_ms.push((table, from, to));
    });
    None
}

/// One explain of the same query. Returns the number of ranges it covers.
async fn explain(
    t: &Topo,
    client: &mut Client,
    node: u64,
    gateway: usize,
    table: Table,
) -> Result<Option<u64>, String> {
    let id = fresh_id(node).await;
    let payload = json!({"table_id": t.ctx(table).table, "query": scan_query()});
    let invoked = t.next();
    let answer = t
        .public(client, node, gateway, id, "query.explain", payload)
        .await;
    let completed = t.next();
    let Completion::Ok(Value::String(text)) = &answer else {
        return Err(clients::failure(&answer).unwrap_or_else(|| "no explanation".into()));
    };
    let (mut index, ranges) = plan_of(text);
    if index.is_none() {
        // Any build that is still open will do: it is not ready when the explain runs.
        let building = t.log(|log| {
            log.builds
                .iter()
                .find(|b| b.table == table && b.declared.is_some() && b.ready.is_none())
                .map(|b| b.name.clone())
        });
        if building.is_some() && t.take_defect(Defect::ChooseBuilding) {
            index = building;
        }
    }
    t.log(|log| {
        log.explain_ms.push((table, elapsed_ms(&t.main.world)));
        if let Some(index) = index {
            log.chosen.push(Chosen {
                index,
                invoked,
                completed,
            });
        }
    });
    Ok(ranges)
}

/// Write keys nobody else writes, and scan them through the index.
async fn audit(t: Topo, table: Table, steps: Vec<Audit>) {
    let ctx = t.ctx(table).clone();
    let node = CLIENTS[table as usize % CLIENTS.len()];
    let mut client = ctx.new_client();
    let mut queries = ctx.new_client();
    let mut live: Vec<i64> = Vec::new();
    let mut inserted = 0;
    let mut counter = 0;
    for step in steps.iter().cycle() {
        if ctx.stopping.load(Ordering::Acquire) {
            break;
        }
        time::sleep(step.think).await;
        counter += 1;
        let value = FLOOR + counter;
        let chosen = |at: u64| (!live.is_empty()).then(|| live[at as usize % live.len()]);
        let target = match step.step {
            AuditStep::Update(at) | AuditStep::Delete(at) => chosen(at),
            AuditStep::Insert { .. } => None,
        };
        let id = fresh_id(node).await;
        let (key, request, insert, new) = match (step.step, target) {
            (AuditStep::Update(_), Some(key)) => (
                key,
                Request::update(&ctx.table, key, value, id),
                false,
                Some(value),
            ),
            (AuditStep::Delete(_), Some(key)) => {
                (key, Request::delete(&ctx.table, key, id), false, None)
            }
            (step, _) => {
                inserted += 1;
                let left = matches!(step, AuditStep::Insert { left: true });
                let key = (AUDIT_KEYS + inserted) * if left { -1 } else { 1 };
                (
                    key,
                    Request::row("table.insert", &ctx.table, key, value, id),
                    true,
                    Some(value),
                )
            }
        };
        let invoked = t.next();
        let answer = clients::resolve(
            &ctx,
            &mut client,
            node,
            step.gateway,
            step.switch,
            &request,
            true,
        )
        .await;
        let completed = t.next();
        let ended = match &answer {
            answer if clients::unknown(answer) => Ended::Unknown,
            Completion::Ok(Value::Bool(false)) | Completion::Refused(_) => Ended::Refused,
            _ => Ended::Acked,
        };
        t.log(|log| {
            log.writes.entry(table).or_default().push(Write {
                key,
                value: new,
                insert,
                invoked,
                completed,
                ended,
            });
        });
        match (ended, new) {
            (Ended::Acked, Some(_)) if insert => live.push(key),
            // A key whose state is open, or that is gone, is not written again.
            (Ended::Acked, Some(_)) | (Ended::Refused, _) => {}
            _ => live.retain(|live| *live != key),
        }
        if insert && ended == Ended::Acked && t.take_defect(Defect::LoseRow) {
            lose_row(&t, &ctx, key).await;
        }
        if let Some(gateway) = step.scan {
            scan(&t, &mut queries, node, gateway, table).await;
        }
        if let Some(gateway) = step.explain {
            let _ = explain(&t, &mut queries, node, gateway, table).await;
        }
    }
}

/// Seeded defect: remove an acknowledged row, without recording it.
async fn lose_row(t: &Topo, ctx: &Ctx, key: i64) {
    for _ in 0..20 {
        let id = fresh_id(BOOTSTRAP_CLIENT).await;
        let answer = request(
            BOOTSTRAP_CLIENT,
            GATEWAYS[0],
            OPERATIONS,
            &ctx.token,
            Some(operation(
                id,
                "table.delete",
                json!({"table_id": ctx.table, "key": key}),
            )),
            ADMIN_LIMIT,
        )
        .await
        .and_then(|reply| reply.completion(id));
        if matches!(answer, Ok(Completion::Ok(Value::Bool(_)))) {
            t.note("seeded-lose-row", json!({"key": key}));
            return;
        }
        time::sleep(RETRY).await;
    }
}

// Index builds.

fn record_status(t: &Topo, name: &str, invoked: u64, completed: u64, data: &Value) -> String {
    let state = data["state"].as_str().unwrap_or("").to_owned();
    t.log(|log| {
        log.statuses.push(Status {
            index: name.to_owned(),
            invoked,
            completed,
            state: state.clone(),
        });
    });
    state
}

/// Declare one index, wait until it is ready, and retire it if the plan says so.
async fn index_life(t: Topo, plan: IndexPlan) {
    time::sleep(plan.start).await;
    let table = t.ctx(plan.table).table.clone();
    let mut build = Build {
        table: plan.table,
        name: plan.name.clone(),
        kind: plan.kind,
        declared: None,
        ready: None,
        retired: None,
        refused: false,
        abandoned: false,
        stuck: None,
        open: true,
    };
    let slot = t.log(|log| {
        log.created.insert(plan.name.clone());
        log.builds.push(build.clone());
        log.builds.len() - 1
    });
    let index_id = fresh_id(BOOTSTRAP_CLIENT).await;
    let column = match plan.kind {
        IndexKind::UnknownColumn => "absent",
        _ => "value",
    };
    let definition = json!({
        "index_id": index_id, "table_id": table, "name": plan.name,
        "columns": [column], "covering": ["id"],
    });
    life(&t, &plan, &definition, index_id, &table, &mut build, slot).await;
    t.note(
        "index",
        json!({"name": plan.name, "kind": format!("{:?}", plan.kind), "declared": build.declared,
            "ready": build.ready, "retired": build.retired, "refused": build.refused,
            "abandoned": build.abandoned, "stuck": build.stuck}),
    );
    t.log(|log| log.builds[slot] = build);
}

async fn life(
    t: &Topo,
    plan: &IndexPlan,
    definition: &Value,
    index_id: Uuid,
    table: &str,
    build: &mut Build,
    slot: usize,
) {
    let mut id = fresh_id(BOOTSTRAP_CLIENT).await;
    let run = *t.run.lock().unwrap();
    let selector =
        |id: Uuid| json!({"table_id": table, "index_id": index_id, "build_id": id, "attempt": 1});
    t.log(|log| log.selectors.insert(id, selector(id)));
    let mut open = false;
    let mut opened: Vec<Uuid> = Vec::new();
    let mut refusals = 0;
    let mut state = 'declare: loop {
        let invoked = t.next();
        let reply = match plan.kind {
            // Beside the gate, so that it can overlap the declaration it races. The heal
            // holds the gate, and then the rival waits like any other request.
            IndexKind::SameName if t.gate.available_permits() > 0 || !t.healing() => {
                t.send(id, "storage.index.create", definition).await
            }
            _ => t.admin(id, "storage.index.create", definition).await,
        };
        match reply {
            Reply::Ok(data) => {
                let completed = t.next();
                if t.take_defect(Defect::LieAboutRefusal) {
                    t.log(|log| log.refused.insert(id, "seeded".into()));
                }
                break record_status(t, &plan.name, invoked, completed, &data);
            }
            Reply::Definite(code, why) => {
                // The index of a dropped table, and the two kinds that fail their validation,
                // are refused for good. Nothing else is expected to be.
                if !open {
                    t.log(|log| log.refused.insert(id, format!("{code:?} {why}")));
                }
                if overtaken(code, &why) && !t.builds_over() {
                    // A declaration that was never answered can hold the table's guard until
                    // its own ID is sent again.
                    let mut unanswered = Vec::new();
                    for earlier in std::mem::take(&mut opened) {
                        let invoked = t.next();
                        match t.admin(earlier, "storage.index.create", definition).await {
                            Reply::Ok(data) => {
                                let completed = t.next();
                                id = earlier;
                                break 'declare record_status(
                                    t, &plan.name, invoked, completed, &data,
                                );
                            }
                            Reply::Definite(..) => {}
                            Reply::Retry { .. } => unanswered.push(earlier),
                        }
                    }
                    opened = unanswered;
                    id = fresh_id(BOOTSTRAP_CLIENT).await;
                    t.log(|log| log.selectors.insert(id, selector(id)));
                    (open, refusals) = (false, 0);
                    time::sleep(RETRY).await;
                    continue;
                }
                // The name may be taken by an earlier declaration of this index that was
                // never answered.
                for &earlier in &opened {
                    let invoked = t.next();
                    let ask = fresh_id(BOOTSTRAP_CLIENT).await;
                    let earlier_build = selector(earlier);
                    let asked = t.admin(ask, "storage.index.status", &earlier_build).await;
                    if let Reply::Ok(data) = asked {
                        let completed = t.next();
                        id = earlier;
                        break 'declare record_status(t, &plan.name, invoked, completed, &data);
                    }
                }
                build.refused = true;
                build.open = !opened.is_empty();
                let expected = plan.kind != IndexKind::Valid
                    || plan.table == Table::Victim
                    || (code == ErrorCode::Conflict && why.contains("already exists"));
                if !expected {
                    build.stuck = Some(format!("declaration refused: {why}"));
                }
                return;
            }
            Reply::Retry { why, unknown } => {
                open |= unknown;
                refusals += 1;
                if t.builds_over() {
                    build.stuck = Some(format!("declaration unanswered: {why}"));
                    return;
                }
                if refusals >= FRESH_BUILD {
                    if open {
                        opened.push(id);
                    } else {
                        t.log(|log| log.refused.insert(id, why));
                    }
                    id = fresh_id(BOOTSTRAP_CLIENT).await;
                    t.log(|log| log.selectors.insert(id, selector(id)));
                    (open, refusals) = (false, 0);
                }
                time::sleep(RETRY).await;
            }
        }
    };
    build.declared = Some(t.now());
    t.log(|log| log.builds[slot].declared = build.declared);
    let mut polls = 0_u32;
    let mut last = String::new();
    while state != "ready" {
        if state == "failed" || state == "retired" {
            build.stuck = Some(format!("the build is {state}"));
            return;
        }
        // The text bounds no build of a dropped table. Other builds must not wait for it.
        if plan.table == Table::Victim && t.log(|log| log.dropped.is_some()) {
            return;
        }
        if t.builds_over() {
            build.stuck = Some(format!("still {state}: {last}"));
            return;
        }
        time::sleep(STATUS_POLL).await;
        polls += 1;
        let ask = fresh_id(BOOTSTRAP_CLIENT).await;
        // The worker that ran the build may be gone, so the operator resumes it now and then.
        let name = if polls.is_multiple_of(8) {
            "storage.index.resume"
        } else {
            "storage.index.status"
        };
        let invoked = t.next();
        match t.admin(ask, name, &selector(id)).await {
            Reply::Ok(data) => {
                let completed = t.next();
                state = record_status(t, &plan.name, invoked, completed, &data);
                last.clear();
            }
            Reply::Definite(code, why) => {
                // A build that belongs to an earlier run cannot resume, and stays invisible.
                if name == "storage.index.resume" && *t.run.lock().unwrap() != run {
                    build.abandoned = true;
                    build.open = false;
                    return;
                }
                last = format!("{code:?} {why}");
            }
            Reply::Retry { why, .. } => last = why,
        }
    }
    build.ready = Some(t.now());
    t.log(|log| log.builds[slot].ready = build.ready);
    let Some(after) = plan.retire else {
        build.open = false;
        return;
    };
    time::sleep(after).await;
    loop {
        let ask = fresh_id(BOOTSTRAP_CLIENT).await;
        let invoked = t.next();
        match t.admin(ask, "storage.index.retire", &selector(id)).await {
            Reply::Ok(data) => {
                let completed = t.next();
                let state = record_status(t, &plan.name, invoked, completed, &data);
                if state == "retired" {
                    build.open = false;
                    build.retired = Some(t.now());
                    return;
                }
                last = format!("retire answered {state}");
            }
            Reply::Definite(code, why) => last = format!("{code:?} {why}"),
            Reply::Retry { why, .. } => last = why,
        }
        if t.builds_over() {
            build.stuck = Some(format!("not retired: {last}"));
            return;
        }
        time::sleep(RETRY).await;
    }
}

// The dropped table.

/// Write the victim until the run stops, and keep using it after the drop.
async fn victim_client(t: Topo, index: usize) {
    let ctx = t.victim.clone();
    let node = CLIENTS[index % CLIENTS.len()];
    let mut client = ctx.new_client();
    let mut after_drop = 0;
    for turn in 0_i64.. {
        let dropped = t.log(|log| log.dropped).is_some();
        if ctx.stopping.load(Ordering::Acquire) && after_drop >= 8 {
            break;
        }
        let over = || elapsed_ms(&ctx.world) >= ctx.converge_by.load(Ordering::Acquire);
        if over() {
            break;
        }
        time::sleep(Duration::from_millis(60 + 30 * index as u64)).await;
        let key = 1 + (turn + index as i64) % VICTIM_KEYS;
        let value = (index as i64 + 1) * 1_000_000 + turn;
        let id = fresh_id(node).await;
        let (what, request) = match turn % 4 {
            0 => ("table.get", Request::get(&ctx.table, key, id)),
            1 => (
                "table.insert",
                Request::row("table.insert", &ctx.table, key, value, id),
            ),
            _ => (
                "table.upsert",
                Request::row("table.upsert", &ctx.table, key, value, id),
            ),
        };
        let mut gateway = (turn as usize + index) % GATEWAYS.len();
        for tries in 1_u64.. {
            let invoked = t.next();
            let answer = clients::attempt(
                &ctx,
                &mut client,
                node,
                GATEWAYS[gateway],
                &request,
                tries == 1,
            )
            .await;
            let success = matches!(answer, Completion::Ok(_));
            t.log(|log| {
                log.calls.push(TableCall {
                    what: format!("{what} of key {key} through n{}", GATEWAYS[gateway]),
                    invoked,
                    success,
                });
            });
            if dropped {
                after_drop += 1;
            }
            if !clients::unknown(&answer) || over() {
                break;
            }
            gateway = 1 - gateway;
            time::sleep(Duration::from_millis(50 * tries.min(10))).await;
        }
    }
}

/// Drop the victim, then create a table of the same name and check that it is empty.
async fn drop_table(t: Topo, at: Duration, gateway: usize) -> Option<String> {
    time::sleep(at).await;
    let node = CLIENTS[1];
    let mut client = t.victim.new_client();
    let mut id = fresh_id(node).await;
    let payload = json!({"table_id": t.victim.table});
    let mut gateway = gateway;
    let mut open = false;
    let mut ambiguous = false;
    let mut refusals = 0;
    if t.take_defect(Defect::FakeDrop) {
        t.note("seeded-fake-drop", payload);
    } else {
        loop {
            let answer = t
                .public(
                    &mut client,
                    node,
                    gateway,
                    id,
                    "table.drop",
                    payload.clone(),
                )
                .await;
            match &answer {
                Completion::Ok(_) => break,
                Completion::Refused(error)
                    if error.outcome == Outcome::NotApplied
                        && error.code == ErrorCode::NotFound =>
                {
                    // Nothing else drops this table, so it is gone because of this request, or
                    // of an earlier ID that was given up without an answer.
                    if ambiguous {
                        break;
                    }
                    t.main.checks.violate(
                        &t.main.world,
                        topology::REFUSED_DDL,
                        format!(
                            "table.drop {id} was refused as not applied ({}), and the table is gone (an earlier attempt was unanswered: {open})",
                            error.message
                        ),
                    );
                    return None;
                }
                Completion::Refused(error) if error.outcome == Outcome::NotApplied => {
                    refusals += 1;
                    // A queued request keeps its definite failure, so asking again takes a new
                    // ID. With an unanswered attempt the old ID gets the same time as a build.
                    if !open || overtaken(error.code, &error.message) || refusals >= FRESH_BUILD {
                        ambiguous |= open;
                        id = fresh_id(node).await;
                        (open, refusals) = (false, 0);
                    }
                }
                other => open |= clients::unknown(other),
            }
            if t.change_over() {
                let why = clients::failure(&answer).unwrap_or_default();
                return Some(format!("table.drop {id} unresolved: {why}"));
            }
            gateway = 1 - gateway;
            time::sleep(RETRY).await;
        }
    }
    let dropped = t.next();
    t.log(|log| log.dropped = Some(dropped));
    t.note("dropped", json!({"table": t.victim.table}));
    for (gateway, through) in GATEWAYS.into_iter().enumerate() {
        let id = fresh_id(node).await;
        let invoked = t.next();
        let payload = json!({"name": "sim_victim"});
        let answer = t
            .public(&mut client, node, gateway, id, "table.describe", payload)
            .await;
        let success = matches!(answer, Completion::Ok(_));
        t.log(|log| {
            log.calls.push(TableCall {
                what: format!("table.describe through n{through}"),
                invoked,
                success,
            });
        });
    }
    recreate(&t, &mut client, node).await
}

/// D11: a row of the dropped table must not be in its successor.
async fn recreate(t: &Topo, client: &mut Client, node: u64) -> Option<String> {
    let mut id = fresh_id(node).await;
    let payload = json!({
        "name": "sim_victim", "schema": {"id": "int64", "value": "int64"},
        "key": "id", "format": "parquet",
    });
    let mut gateway = 0;
    let table = loop {
        let answer = t
            .public(client, node, gateway, id, "table.create", payload.clone())
            .await;
        match &answer {
            Completion::Ok(created) => break created["id"].as_str().unwrap_or("").to_owned(),
            other if t.change_over() || !retryable(other) => {
                let why = clients::failure(other).unwrap_or_default();
                return Some(format!(
                    "the dropped table's name cannot be used again: {why}"
                ));
            }
            Completion::Refused(error) if error.outcome == Outcome::NotApplied => {
                id = fresh_id(node).await;
            }
            _ => {}
        }
        gateway = 1 - gateway;
        time::sleep(RETRY).await;
    };
    let query = json!({
        "table_id": table,
        "query": {"select": null, "filter": null, "order_by": null, "limit": null},
    });
    loop {
        let id = fresh_id(node).await;
        let answer = t
            .public(client, node, gateway, id, "query.count", query.clone())
            .await;
        match &answer {
            Completion::Ok(count) if *count == json!(0) => return None,
            Completion::Ok(count) => {
                t.main.checks.violate(
                    &t.main.world,
                    topology::DROPPED,
                    format!("the table created after the drop of sim_victim holds {count} rows"),
                );
                return None;
            }
            other if t.change_over() => {
                let why = clients::failure(other).unwrap_or_default();
                return Some(format!("count of the recreated table: {why}"));
            }
            _ => time::sleep(RETRY).await,
        }
    }
}

fn retryable(completion: &Completion) -> bool {
    match completion {
        Completion::Refused(error) => error.outcome == Outcome::Unknown || !definite(error.code),
        Completion::Unknown(_) => true,
        Completion::Ok(_) => false,
    }
}

// Splits.

/// Split the first range of `table` until the split publishes.
async fn split(t: Topo, table: String, cluster_id: Uuid, boundary: i64, at: Duration) {
    time::sleep(at).await;
    let payload = json!({
        "source": initial_range(cluster_id, Uuid::parse_str(&table).expect("table UUID")),
        "boundary": boundary, "page_bytes": 1 << 20,
    });
    let mut run = *t.run.lock().unwrap();
    let mut id = fresh_id(BOOTSTRAP_CLIENT).await;
    let requested = t.now();
    t.log(|log| {
        log.split = Some((requested, None));
        log.split_ids = 1;
    });
    t.note("split-ranged", json!({"id": id, "boundary": boundary}));
    let mut last = String::new();
    loop {
        // Queued work is abandoned when every worker stops, so the operator asks again.
        let now = *t.run.lock().unwrap();
        if now != run {
            run = now;
            id = fresh_id(BOOTSTRAP_CLIENT).await;
            t.log(|log| log.split_ids += 1);
            t.note(
                "split-ranged",
                json!({"id": id, "boundary": boundary, "run": run}),
            );
        }
        let why = match t.admin(id, "storage.shard.split", &payload).await {
            Reply::Ok(data) if data["state"] == "published" => {
                let published = t.now();
                t.log(|log| log.split = Some((requested, Some(published))));
                t.note("split-ranged-published", data);
                return;
            }
            Reply::Ok(data) => data.to_string(),
            Reply::Definite(code, why) => {
                if overtaken(code, &why) {
                    id = fresh_id(BOOTSTRAP_CLIENT).await;
                    t.log(|log| log.split_ids += 1);
                    t.note(
                        "split-ranged",
                        json!({"id": id, "boundary": boundary, "after": why}),
                    );
                }
                why
            }
            Reply::Retry { why, .. } => why,
        };
        if why != last {
            t.note("split-ranged-pending", json!(why));
            last = why;
        }
        if t.change_over() {
            t.log(|log| log.split_error = Some(last));
            return;
        }
        time::sleep(RETRY).await;
    }
}

// The run.

async fn direct_stat(cluster: &Cluster, pointer: &str) -> Result<u64, String> {
    let mut total = 0;
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        total += stats
            .pointer(pointer)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("n{node} stats lack {pointer}"))?;
    }
    Ok(total)
}

/// Run the workload against a started cluster.
pub(crate) async fn run(
    cluster: &mut Cluster,
    config: &Config,
    history: Arc<Mutex<History>>,
    g: &mut Gen,
    seed: u64,
    selection: &Selection,
    no_nemesis: bool,
) -> Result<direct::Report, String> {
    let forced = Forced::from_env()?;
    let defect = Defect::from_env()?;
    cluster.wait_direct().await?;
    let mut plan = TopoPlan::draw(g, selection, forced);
    let mut tables = BTreeMap::new();
    for name in [
        "sim_main",
        "sim_ranged",
        "sim_victim",
        "sim_quiet",
        "sim_fence",
        "sim_moved",
    ] {
        if name == "sim_moved" && plan.moving.is_none() {
            continue;
        }
        tables.insert(name, create_table(cluster, &history, name).await?);
    }
    let context = |name: &str, keys: i64| {
        Ctx::new(
            cluster.world.clone(),
            cluster.checks.clone(),
            history.clone(),
            tables[name].clone(),
            cluster.config.data_token.clone(),
            config.deadline(),
            keys,
        )
    };
    let main = context("sim_main", plan.main.keys);
    // One stop for every table: the clients share its flags.
    let share = |mut ctx: Ctx| {
        ctx.stopping = main.stopping.clone();
        ctx.converge_by = main.converge_by.clone();
        ctx
    };
    let ranged = share(context("sim_ranged", plan.ranged.keys));
    let victim = share(context("sim_victim", VICTIM_KEYS));
    let quiet = share(context("sim_quiet", QUIET_KEYS));
    let fence = share(context("sim_fence", 1));
    let moved = plan
        .moving
        .is_some()
        .then(|| share(context("sim_moved", plan.moved.keys)));
    let t = Topo {
        main,
        ranged,
        victim,
        admin_token: cluster.config.admin_token.clone(),
        log: Arc::default(),
        order: Arc::new(AtomicU64::new(1)),
        run: Arc::new(Mutex::new(cluster.config.run_id)),
        change_by: Arc::new(AtomicU64::new(u64::MAX)),
        gate: Arc::new(Semaphore::new(1)),
    };
    t.log(|log| log.defect = defect);
    let ctx = t.main.clone();
    warm_quiet(&quiet).await?;
    warm_fence(&fence).await?;
    let cluster_id = cluster.config.cluster_id;
    if plan.split == SplitTiming::BeforeLoad {
        let deadline = t.now() + CHANGE_LIMIT.as_millis() as u64;
        t.change_by.store(deadline, Ordering::Release);
        let table = t.ranged.table.clone();
        split(t.clone(), table, cluster_id, plan.boundary, Duration::ZERO).await;
        t.change_by.store(u64::MAX, Ordering::Release);
        if let Some(why) = t.log(|log| log.split_error.take()) {
            return Err(format!("the split before the load did not publish: {why}"));
        }
    }
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }

    let faults = NetFaults::install(cluster.net(), &cluster.world, seed);
    config.jump_clocks(&cluster.world);
    let start = elapsed_ms(&cluster.world);
    let profile = selection.profile(seed);
    let mut chaos = (!no_nemesis && profile.total_weight() > 0)
        .then(|| Chaos::new(profile, faults.clone(), start, g));
    if no_nemesis {
        plan.stale.clear();
    }
    let mut moving = plan.moving.take().map(|mut moving| {
        moving.set_faults(faults.clone());
        moving
    });
    cluster.record(format!(
        "workload {} profile {} clients {}+{} split {:?} move {} stale {:?} quiet {:?}",
        Workload::Topology,
        chaos.as_ref().map_or("none", |chaos| chaos.profile_name()),
        plan.main.programs.len(),
        plan.ranged.programs.len(),
        plan.split,
        moving.is_some(),
        plan.stale,
        plan.quiet,
    ));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();
    for (ctx, programs) in [
        (
            Some(t.main.clone()),
            std::mem::take(&mut plan.main.programs),
        ),
        (
            Some(t.ranged.clone()),
            std::mem::take(&mut plan.ranged.programs),
        ),
        (moved.clone(), std::mem::take(&mut plan.moved.programs)),
    ] {
        let Some(ctx) = ctx else { continue };
        for program in programs {
            tasks.push(nabedra_rt::task::spawn(clients::run(ctx.clone(), program)));
        }
    }
    for (table, steps) in std::mem::take(&mut plan.audits) {
        tasks.push(nabedra_rt::task::spawn(audit(t.clone(), table, steps)));
    }
    if !selection.focus.idle {
        for index in 0..2 {
            tasks.push(nabedra_rt::task::spawn(victim_client(t.clone(), index)));
        }
    }
    let dropping = nabedra_rt::task::spawn(drop_table(t.clone(), plan.drop_at, plan.drop_gateway));
    let mut changes: Vec<JoinHandle<()>> = plan
        .indexes
        .iter()
        .cloned()
        .map(|index| nabedra_rt::task::spawn(index_life(t.clone(), index)))
        .collect();
    if plan.split == SplitTiming::UnderLoad {
        let table = t.ranged.table.clone();
        changes.push(nabedra_rt::task::spawn(split(
            t.clone(),
            table,
            cluster_id,
            plan.boundary,
            plan.split_at,
        )));
    }

    let mut frozen: Option<(u64, u64)> = None;
    let end = cluster.world.elapsed() + CHAOS;
    while cluster.world.elapsed() < end {
        if let Some(chaos) = chaos.as_mut() {
            chaos.tick(cluster, &ctx, g).await;
        }
        if let (Some(moving), Some(moved)) = (moving.as_mut(), moved.as_ref()) {
            moving.tick(cluster, moved).await;
        }
        *t.run.lock().unwrap() = cluster.config.run_id;
        stale_windows(cluster, &t, &mut plan.stale, &mut frozen, start);
        time::sleep(TICK).await;
    }
    ctx.stopping.store(true, Ordering::Release);
    if let Some((gateway, _)) = frozen.take() {
        cluster.thaw(gateway);
    }

    let mut report = direct::Report::default();
    let gate = t.gate.clone();
    let held = gate.acquire().await.expect("open gate");
    let healed = match chaos.as_mut() {
        Some(chaos) => chaos.heal(cluster, &ctx).await,
        None => Ok(()),
    };
    drop(held);
    if let Err(error) = healed {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        tasks.push(nabedra_rt::task::spawn(async move {
            let _ = dropping.await;
        }));
        tasks.append(&mut changes);
        t.change_by.store(0, Ordering::Release);
        judge(cluster, &t);
        return direct::abandon(cluster, &ctx, tasks, report).await;
    }
    faults.clear();
    *t.run.lock().unwrap() = cluster.config.run_id;
    let healed = cluster.world.elapsed();
    let healed_ms = elapsed_ms(&cluster.world);
    cluster
        .checks
        .expect_recovery(&cluster.world, "convergence", CONVERGE_LIMIT);
    let converge_by = healed_ms + CONVERGE_LIMIT.as_millis() as u64;
    ctx.converge_by.store(converge_by, Ordering::Release);
    t.change_by.store(
        healed_ms + CHANGE_LIMIT.as_millis() as u64,
        Ordering::Release,
    );
    let ready = cluster.wait_direct().await;
    if let Err(error) = ready {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        tasks.push(nabedra_rt::task::spawn(async move {
            let _ = dropping.await;
        }));
        tasks.append(&mut changes);
        t.change_by.store(0, Ordering::Release);
        judge(cluster, &t);
        return direct::abandon(cluster, &ctx, tasks, report).await;
    }
    let mut converged = Ok(());
    for ctx in [Some(&t.main), Some(&t.ranged), moved.as_ref()] {
        let Some(ctx) = ctx else { continue };
        if converged.is_ok() {
            converged = direct::converge(cluster, ctx, healed + CONVERGE_LIMIT).await;
        }
    }
    report
        .assertions
        .push(("converged after the heal", converged.is_ok()));
    if let Err(error) = &converged {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("convergence: {error}"));
    }
    for task in tasks {
        let _ = task.await;
    }
    cluster.checks.recovered(&cluster.world, "convergence");
    // The topology changes have a budget of their own.
    for task in changes {
        let _ = task.await;
    }
    let dropped = dropping.await;
    report.assertions.push((
        "the drop resolved and its name was reused",
        matches!(dropped, Ok(None)),
    ));
    match dropped {
        Ok(None) => {}
        Ok(Some(why)) => cluster.checks.violate(&cluster.world, "Liveness", why),
        Err(error) => cluster
            .checks
            .violate(&cluster.world, "Workload", error.to_string()),
    }
    changes_settled(cluster, &t, &mut report);
    if let Some(chaos) = chaos.as_mut().filter(|chaos| chaos.split_pending()) {
        let from = cluster.world.elapsed();
        chaos.finish_split(cluster, &ctx, from).await;
    }
    if let Some(chaos) = chaos.as_ref() {
        chaos.check_abandoned(cluster, &ctx).await;
    }
    if let (Some(moving), Some(moved)) = (moving.as_mut(), moved.as_ref()) {
        let retired = moving.finish(cluster, moved, healed).await;
        report
            .assertions
            .push(("replica move retired", retired.is_ok()));
        let checked = match retired {
            Ok(()) => moving.check_retired(cluster, moved).await,
            Err(error) => Err(error),
        };
        if let Err(error) = checked {
            cluster
                .checks
                .violate(&cluster.world, "Liveness", format!("replica move: {error}"));
        }
        report.scenarios.extend(std::mem::take(&mut moving.clauses));
    }
    let mut drained = Ok(());
    if let (Some(moving), Some(moved), true) = (
        moving.as_mut(),
        moved.as_ref(),
        report.assertions.contains(&("replica move retired", true)),
    ) {
        if let Some(why) = moving.drain_blocked(cluster, false) {
            cluster.record(format!("O26 drain not run: {why}"));
        } else {
            drained = match moving.drain_pool(cluster, moved).await {
                Ok(()) => {
                    let limit = cluster.world.elapsed() + CONVERGE_LIMIT;
                    direct::converge(cluster, moved, limit).await
                }
                Err(error) => Err(error),
            };
        }
        report.scenarios.extend(std::mem::take(&mut moving.clauses));
    }
    if let Err(error) = drained {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("draining the moved pool: {error}"),
        );
    }
    let mut live = vec![
        (t.main.clone(), Table::Main),
        (t.ranged.clone(), Table::Ranged),
    ];
    for (ctx, table) in &live {
        if let Err(error) = clients::final_checks(ctx).await {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("{}: {error}", table.name()),
            );
        }
    }
    if let Some(moved) = moved.as_ref()
        && let Err(error) = clients::final_checks(moved).await
    {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("moved: {error}"));
    }
    let rested = at_rest(cluster, &t, &mut live).await;
    report
        .assertions
        .push(("direct mutations at rest", rested.is_ok()));
    if let Err(error) = rested {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("at rest: {error}"));
    }
    if let Err(error) = final_scans(cluster, &t, chaos.as_ref(), &mut report).await {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("final scans: {error}"));
    }
    match quiet_stage(cluster, &t, &quiet, &plan, cluster_id).await {
        Ok(clauses) => report.scenarios.extend(clauses),
        Err(error) => {
            cluster
                .checks
                .violate(&cluster.world, "Liveness", format!("quiet stage: {error}"));
        }
    }
    match fence_races(cluster, &t, &fence, plan.race_lead).await {
        Ok(clauses) => report.scenarios.extend(clauses),
        Err(error) => {
            cluster
                .checks
                .violate(&cluster.world, "Liveness", format!("fence races: {error}"));
        }
    }
    refused_builds(cluster, &t).await;
    judge(cluster, &t);

    clauses_of(
        cluster,
        &t,
        chaos.as_ref(),
        moving.as_ref(),
        &mut report.scenarios,
    );
    evaluate(
        cluster,
        config,
        &t,
        moved.as_ref(),
        chaos.as_ref(),
        moving.as_ref(),
        &mut report,
    );
    let extra = moving
        .as_ref()
        .map_or(Vec::new(), |moving| moving.extra_pools(&cluster.world));
    let (stats, leftover) = direct::settle(cluster, &extra).await?;
    if !selection.focus.idle {
        cluster.checks.check_direct_path(&cluster.world, &stats);
    }
    for (node, stats) in &stats {
        cluster.checks.check_resources(&cluster.world, *node, stats);
    }
    if let Some((node, row)) = leftover {
        cluster.checks.violate(
            &cluster.world,
            "ResourceBaseline",
            format!("n{node} keeps reservations in an added pool after quiescence: {row}"),
        );
    }
    summarize(
        cluster,
        &t,
        chaos.as_ref(),
        moving.is_some(),
        plan.split,
        &mut report,
    );
    Ok(report)
}

/// Freeze a gateway at the start of a window and resume it at its end.
fn stale_windows(
    cluster: &mut Cluster,
    t: &Topo,
    windows: &mut Vec<StaleWindow>,
    frozen: &mut Option<(u64, u64)>,
    start: u64,
) {
    let now = elapsed_ms(&cluster.world);
    if let Some((gateway, until)) = *frozen {
        if now >= until {
            cluster.thaw(gateway);
            t.note("stale-gateway-resumed", json!({"gateway": gateway}));
            *frozen = None;
        }
        return;
    }
    let due = windows.iter().position(|window| {
        start + window.at <= now
            && cluster.is_running(window.gateway)
            && !cluster.frozen().contains(&window.gateway)
    });
    if let Some(due) = due {
        let window = windows.remove(due);
        cluster.freeze(window.gateway);
        t.note(
            "stale-gateway",
            json!({"gateway": window.gateway, "ms": window.hold}),
        );
        *frozen = Some((window.gateway, now + window.hold));
    }
}

/// Fill the quiet table and cache its route in both gateways.
async fn warm_quiet(ctx: &Ctx) -> Result<(), String> {
    let mut client = ctx.new_client();
    for key in 1..=QUIET_KEYS {
        let node = CLIENTS[key as usize % CLIENTS.len()];
        let id = fresh_id(node).await;
        let request = Request::row("table.insert", &ctx.table, key, key, id);
        let gateway = key as usize % GATEWAYS.len();
        let answer = clients::resolve(ctx, &mut client, node, gateway, true, &request, true).await;
        if let Some(why) = clients::failure(&answer) {
            return Err(format!("quiet insert of key {key}: {why}"));
        }
    }
    clients::read_each(ctx).await
}

/// Every topology change settled within its bound.
fn changes_settled(cluster: &Cluster, t: &Topo, report: &mut direct::Report) {
    let (builds, split, split_error) =
        t.log(|log| (log.builds.clone(), log.split, log.split_error.take()));
    let mut settled = true;
    for build in &builds {
        if let Some(why) = &build.stuck {
            settled = false;
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("index {} on {}: {why}", build.name, build.table.name()),
            );
        }
    }
    report.assertions.push(("index builds settled", settled));
    let published = split.is_none_or(|(_, published)| published.is_some());
    report
        .assertions
        .push(("ranged split published", published));
    if let Some(why) = split_error {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!(
                "the split of ranged did not publish within {CHANGE_LIMIT:?} after the heal: {why}"
            ),
        );
    }
}

/// [`topology::FENCE`] in its public form: with every schema change settled, a mutation takes
/// the direct path. Convergence may be polled, so a Catalog intent that the first fallback
/// resolves is not a finding.
async fn at_rest(cluster: &Cluster, t: &Topo, live: &mut Vec<(Ctx, Table)>) -> Result<(), String> {
    const SCHEMA: &str = "/direct/fallbacks/schema";
    let mut rest = Vec::new();
    for (ctx, table) in live.drain(..) {
        // A declaration that was never answered, or a build that has not published, leaves a
        // schema change open.
        let open = t.log(|log| {
            let open = |b: &&Build| b.table == table && b.open;
            log.builds.iter().filter(open).count()
        });
        if open > 0 {
            let skipped = format!(
                "{} not judged: {open} schema changes are open",
                table.name()
            );
            t.log(|log| log.rest.push(skipped));
            continue;
        }
        let mut client = ctx.new_client();
        for (index, gateway) in GATEWAYS.into_iter().enumerate() {
            let node = CLIENTS[index % CLIENTS.len()];
            let started = cluster.world.elapsed();
            let keys = BTreeSet::from([1, ctx.keys]);
            loop {
                let before = direct_stat(cluster, SCHEMA).await?;
                for &key in &keys {
                    let id = fresh_id(node).await;
                    let value = 910_000_000 + elapsed_ms(&ctx.world) as i64 * 100 + key;
                    let request = Request::row("table.upsert", &ctx.table, key, value, id);
                    clients::attempt(&ctx, &mut client, node, gateway, &request, true).await;
                }
                let mut schema = direct_stat(cluster, SCHEMA).await?.saturating_sub(before);
                let polled = cluster.world.elapsed() - started;
                if schema > 0 && polled < REST_LIMIT {
                    time::sleep(RETRY).await;
                    continue;
                }
                if t.take_defect(Defect::StuckFence) {
                    schema += 1;
                }
                rest.push(AtRest {
                    table: table.name(),
                    gateway,
                    writes: keys.len() as u64,
                    schema_fallbacks: schema,
                    polled_ms: polled.as_millis() as u64,
                });
                break;
            }
        }
    }
    t.log(|log| {
        log.rest.extend(rest.iter().map(|rest| {
            format!(
                "{} n{}: {} of {} fell back after {} ms",
                rest.table, rest.gateway, rest.schema_fallbacks, rest.writes, rest.polled_ms
            )
        }));
    });
    for found in fence_at_rest(&rest) {
        cluster
            .checks
            .violate(&cluster.world, topology::FENCE, found);
    }
    Ok(())
}

/// With no client running: one scan, one explain and one count per table, not retried.
async fn final_scans(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    report: &mut direct::Report,
) -> Result<(), String> {
    let mut client = t.main.new_client();
    let nemesis_split = chaos.is_some_and(|chaos| {
        chaos
            .log
            .iter()
            .any(|fault| fault.action == Action::Split && fault.observed)
    });
    // The nemesis's split can be pending, and a full stop can have abandoned another.
    let asked = chaos.map_or(0, |chaos| {
        let splits = chaos.log.iter().filter(|f| f.action == Action::Split);
        let stops = chaos.log.iter().filter(|f| f.action == Action::Cold);
        splits.count() as u64 * (1 + stops.count() as u64 + u64::from(chaos.heal_cold))
    });
    let (split, split_ids) = t.log(|log| (log.split, log.split_ids));
    for table in [Table::Main, Table::Ranged] {
        let (published, asked) = match table {
            Table::Main => (nemesis_split, asked),
            _ => (
                split.is_some_and(|(_, published)| published.is_some()),
                split_ids,
            ),
        };
        // One range splits once, whichever of its IDs did it.
        let open = u64::from(!published && asked > 0);
        for (gateway, _) in GATEWAYS.iter().enumerate() {
            let node = CLIENTS[gateway % CLIENTS.len()];
            if let Some(why) = scan(t, &mut client, node, gateway, table).await {
                return Err(format!(
                    "{} through n{}: {why}",
                    table.name(),
                    GATEWAYS[gateway]
                ));
            }
            let ranges = explain(t, &mut client, node, gateway, table).await?;
            let Some(mut ranges) = ranges else {
                return Err(format!("the explain of {} names no ranges", table.name()));
            };
            if t.take_defect(Defect::LoseRange) {
                ranges -= 1;
            }
            if let Some(found) = route_count(table.name(), u64::from(published), open, ranges) {
                cluster
                    .checks
                    .violate(&cluster.world, topology::ROUTES, found);
            }
            if published {
                report.scenarios.insert("O28:routes-after-split");
            }
        }
        if let Some(wrong) = exact_count(t, table).await? {
            cluster.checks.violate(&cluster.world, "ExactCount", wrong);
        }
        if published {
            report.scenarios.insert("O16:coverage");
            report.scenarios.insert("O21:split-keeps-table");
        }
    }
    Ok(())
}

/// `query.count` through one gateway against a read of every key through the other.
async fn exact_count(t: &Topo, table: Table) -> Result<Option<String>, String> {
    let ctx = t.ctx(table);
    let audited: BTreeSet<i64> = t.log(|log| {
        log.writes
            .get(&table)
            .into_iter()
            .flatten()
            .map(|write| write.key)
            .collect()
    });
    let mut client = ctx.new_client();
    let mut held = 0;
    for key in (1..=ctx.keys).chain(audited) {
        let id = fresh_id(CLIENTS[0]).await;
        let read = Request::get(&ctx.table, key, id);
        match clients::attempt(ctx, &mut client, CLIENTS[0], GATEWAYS[0], &read, false).await {
            Completion::Ok(row) => held += i64::from(!row.is_null()),
            other => {
                let why = clients::failure(&other).unwrap_or_default();
                return Err(format!("read of key {key} of {}: {why}", table.name()));
            }
        }
    }
    let payload = json!({
        "table_id": ctx.table,
        "query": {"select": null, "filter": null, "order_by": null, "limit": null},
    });
    let id = fresh_id(CLIENTS[1]).await;
    let answer = t
        .public(&mut client, CLIENTS[1], 1, id, "query.count", payload)
        .await;
    match answer {
        Completion::Ok(count) if count == json!(held) => Ok(None),
        Completion::Ok(count) => Ok(Some(format!(
            "query.count of {} answered {count} and its keys hold {held} rows",
            table.name()
        ))),
        other => Err(format!(
            "query.count of {}: {}",
            table.name(),
            clients::failure(&other).unwrap_or_default()
        )),
    }
}

/// Change the topology under the routes both gateways cached long before, wait for the drawn
/// staleness, and then use each route alone.
async fn quiet_stage(
    cluster: &Cluster,
    t: &Topo,
    ctx: &Ctx,
    plan: &TopoPlan,
    cluster_id: Uuid,
) -> Result<Scenarios, String> {
    const STALE: &str = "/direct/stale_routes";
    let mut clauses = Scenarios::new();
    if plan.rewarm {
        clients::read_each(ctx).await?;
    }
    let deadline = t.now() + CHANGE_LIMIT.as_millis() as u64;
    match plan.quiet {
        QuietChange::Split => {
            let table = Uuid::parse_str(&ctx.table).expect("table UUID");
            let id = cluster.split_id().await;
            let payload = json!({
                "source": initial_range(cluster_id, table),
                "boundary": plan.quiet_boundary, "page_bytes": 1 << 20,
            });
            loop {
                let why = match t.admin(id, "storage.shard.split", &payload).await {
                    Reply::Ok(data) if data["state"] == "published" => break,
                    Reply::Ok(data) => data.to_string(),
                    Reply::Definite(_, why) | Reply::Retry { why, .. } => why,
                };
                if t.now() >= deadline {
                    return Err(format!("the split of an idle range did not publish: {why}"));
                }
                time::sleep(RETRY).await;
            }
        }
        QuietChange::Index => {
            t.change_by.store(deadline, Ordering::Release);
            let index_id = fresh_id(BOOTSTRAP_CLIENT).await;
            let id = fresh_id(BOOTSTRAP_CLIENT).await;
            let definition = json!({
                "index_id": index_id, "table_id": ctx.table, "name": "quiet_value",
                "columns": ["value"], "covering": ["id"],
            });
            let selector = json!({
                "table_id": ctx.table, "index_id": index_id, "build_id": id, "attempt": 1,
            });
            let mut reply = t.admin(id, "storage.index.create", &definition).await;
            loop {
                let why = match reply {
                    Reply::Ok(data) if data["state"] == "ready" => break,
                    Reply::Ok(data) => data.to_string(),
                    Reply::Definite(_, why) | Reply::Retry { why, .. } => why,
                };
                if t.now() >= deadline {
                    return Err(format!("the index of an idle table is not ready: {why}"));
                }
                time::sleep(STATUS_POLL).await;
                let ask = fresh_id(BOOTSTRAP_CLIENT).await;
                reply = t.admin(ask, "storage.index.status", &selector).await;
                if matches!(reply, Reply::Definite(ErrorCode::NotFound, _)) {
                    reply = t.admin(id, "storage.index.create", &definition).await;
                }
            }
        }
    }
    time::sleep(plan.quiet_age).await;
    let mut client = ctx.new_client();
    let mut refreshes = Vec::new();
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        for key in [1, QUIET_KEYS] {
            for turn in 0..3 {
                let id = fresh_id(node).await;
                let (what, request) = match turn {
                    1 => {
                        let value = 2_000 + 10 * key + index as i64;
                        let write = Request::row("table.upsert", &ctx.table, key, value, id);
                        ("table.upsert", write)
                    }
                    _ => ("table.get", Request::get(&ctx.table, key, id)),
                };
                let before = direct_stat(cluster, STALE).await?;
                let answer =
                    clients::attempt(ctx, &mut client, node, gateway, &request, true).await;
                let mut stale = direct_stat(cluster, STALE).await?.saturating_sub(before);
                if let Some(why) = clients::failure(&answer) {
                    return Err(format!(
                        "{what} of key {key} through n{gateway} with a route that the {:?} replaced: {why}",
                        plan.quiet
                    ));
                }
                if turn == 0 && stale > 0 && t.take_defect(Defect::RefreshTwice) {
                    stale += 1;
                }
                refreshes.push(Refresh {
                    gateway,
                    key,
                    first: turn == 0,
                    stale_answers: stale,
                });
            }
        }
    }
    cluster.record(format!("quiet {:?} {refreshes:?}", plan.quiet));
    if plan.quiet == QuietChange::Split && refreshes.iter().any(|r| r.stale_answers > 0) {
        clauses.insert("D07:stale-routes");
    }
    for found in refreshes_once(&refreshes) {
        cluster
            .checks
            .violate(&cluster.world, topology::STALE_ROUTE, found);
    }
    Ok(clauses)
}

async fn warm_fence(ctx: &Ctx) -> Result<(), String> {
    let mut client = ctx.new_client();
    let id = fresh_id(CLIENTS[0]).await;
    let request = Request::row("table.insert", &ctx.table, 1, 1, id);
    let answer = clients::resolve(ctx, &mut client, CLIENTS[0], 0, true, &request, true).await;
    clients::failure(&answer).map_or(Ok(()), |why| Err(format!("fence insert: {why}")))
}

/// One declaration of the fence table, sent again under its ID while it is unanswered.
async fn declare(t: &Topo, table: &str, name: &str) -> (Uuid, Value, Reply) {
    let index_id = fresh_id(BOOTSTRAP_CLIENT).await;
    let id = fresh_id(BOOTSTRAP_CLIENT).await;
    let deadline = t.now() + ADMIN_LIMIT.as_millis() as u64;
    let definition = json!({
        "index_id": index_id, "table_id": table, "name": name,
        "columns": ["value"], "covering": ["id"],
    });
    let selector = json!({
        "table_id": table, "index_id": index_id, "build_id": id, "attempt": 1,
    });
    loop {
        let reply = t.send(id, "storage.index.create", &definition).await;
        if !matches!(reply, Reply::Retry { .. }) || t.now() >= deadline {
            return (id, selector, reply);
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether the build is ready, or why that is not known.
async fn ready(t: &Topo, selector: &Value) -> Result<bool, String> {
    let ask = fresh_id(BOOTSTRAP_CLIENT).await;
    match t.admin(ask, "storage.index.status", selector).await {
        Reply::Ok(data) => Ok(data["state"] == "ready"),
        Reply::Definite(_, why) | Reply::Retry { why, .. } => Err(why),
    }
}

async fn wait_ready(t: &Topo, selector: &Value) -> Result<(), String> {
    let deadline = t.now() + REST_LIMIT.as_millis() as u64;
    loop {
        let state = ready(t, selector).await;
        if matches!(state, Ok(true)) {
            return Ok(());
        }
        if t.now() >= deadline {
            return Err(format!("an index of one row is not ready: {state:?}"));
        }
        time::sleep(STATUS_POLL).await;
    }
}

/// Two declarations of one name, the rival sent some time before the first is done. The loser is
/// refused before it takes the table's guard, or after, by the validation under the guard. From
/// the two answers until the winner's build publishes, no schema change of the table is open, so
/// the direct path must serve it ([`topology::FENCE`]).
async fn fence_races(
    cluster: &Cluster,
    t: &Topo,
    ctx: &Ctx,
    start: u64,
) -> Result<Scenarios, String> {
    const SCHEMA: &str = "/direct/fallbacks/schema";
    let mut clauses = Scenarios::new();
    let mut client = ctx.new_client();
    let alone = t.now();
    let (_, selector, reply) = declare(t, &ctx.table, "fence_alone").await;
    let took = t.now() - alone;
    if !matches!(reply, Reply::Ok(_)) {
        return Err("a declaration at rest was not accepted".into());
    }
    wait_ready(t, &selector).await?;
    let mut rest = Vec::new();
    // A rival sent late finds the name taken before it asks for the guard. One sent early loses
    // the guard itself. The refusal under the guard is between the two, so search for it.
    let (mut late, mut early) = (0_u64, took);
    for turn in 0..RACES {
        let lead = match turn {
            0 => took * start / 1_000,
            _ => late.midpoint(early),
        };
        let name = format!("fence_{turn}");
        let first = declare(t, &ctx.table, &name);
        let rival = async {
            time::sleep(Duration::from_millis(took.saturating_sub(lead))).await;
            declare(t, &ctx.table, &name).await
        };
        let (first, rival) = tokio::join!(first, rival);
        let ((_, selector, _), (loser, refused, why)) = match (first, rival) {
            (won @ (.., Reply::Ok(_)), (id, selector, Reply::Definite(_, why)))
            | ((id, selector, Reply::Definite(_, why)), won @ (.., Reply::Ok(_))) => {
                (won, (id, selector, why))
            }
            ((_, a, first), (_, b, rival)) => {
                // Both are still open, or neither: not the race this is about.
                let both = format!("{} and {}", first.shown(), rival.shown());
                t.log(|log| {
                    log.races
                        .push(format!("{lead} of {took} ms before: {both}"));
                });
                for (selector, reply) in [(a, first), (b, rival)] {
                    if matches!(reply, Reply::Ok(_)) {
                        wait_ready(t, &selector).await?;
                    }
                }
                continue;
            }
        };
        t.log(|log| {
            log.refused.insert(loser, why.clone());
            log.selectors.insert(loser, refused);
        });
        let under_guard = why.contains("name or identity already exists");
        if overtaken(ErrorCode::Conflict, &why) {
            early = lead;
        } else {
            late = lead;
        }
        let began = t.now();
        let (mut fell_back, mut direct) = (0, 0);
        loop {
            let gateway = GATEWAYS[turn % GATEWAYS.len()];
            let before = direct_stat(cluster, SCHEMA).await?;
            let id = fresh_id(CLIENTS[0]).await;
            let value = 20_000 + turn as i64 * 100 + fell_back;
            let request = Request::row("table.upsert", &ctx.table, 1, value, id);
            clients::attempt(ctx, &mut client, CLIENTS[0], gateway, &request, true).await;
            if direct_stat(cluster, SCHEMA).await? > before {
                fell_back += 1;
            } else {
                direct += 1;
            }
            if direct > 0 || ready(t, &selector).await? {
                break;
            }
        }
        rest.push(AtRest {
            table: "fence",
            gateway: GATEWAYS[turn % GATEWAYS.len()],
            writes: (fell_back + direct) as u64,
            // One fallback may have met the publication itself.
            schema_fallbacks: if direct == 0 && fell_back > 1 {
                fell_back as u64
            } else {
                0
            },
            polled_ms: t.now() - began,
        });
        t.log(|log| {
            log.races.push(format!(
                "{lead} of {took} ms before: under guard {under_guard}, direct {direct}, fell back {fell_back}, {why}"
            ));
        });
        wait_ready(t, &selector).await?;
        if under_guard {
            clauses.insert("O12:refused-under-guard");
            break;
        }
    }
    for found in fence_at_rest(&rest) {
        cluster
            .checks
            .violate(&cluster.world, topology::FENCE, found);
    }
    Ok(clauses)
}

/// Whether a build whose declaration was refused exists anyway.
async fn refused_builds(cluster: &Cluster, t: &Topo) {
    let refused = t.log(|log| log.refused.clone());
    let mut found = BTreeSet::new();
    for build in refused.keys() {
        let selector = t.log(|log| log.selectors[build].clone());
        for _ in 0..8 {
            let ask = fresh_id(BOOTSTRAP_CLIENT).await;
            match t.admin(ask, "storage.index.status", &selector).await {
                Reply::Ok(data) if data["build_id"] == json!(build) => {
                    found.insert(*build);
                    break;
                }
                Reply::Ok(_) | Reply::Definite(..) => break,
                Reply::Retry { .. } => time::sleep(RETRY).await,
            }
        }
    }
    for found in refused_ddl(&refused, &found) {
        cluster
            .checks
            .violate(&cluster.world, topology::REFUSED_DDL, found);
    }
}

/// Judge everything the tasks recorded.
fn judge(cluster: &Cluster, t: &Topo) {
    t.log(|log| {
        for table in [Table::Main, Table::Ranged] {
            let none = Vec::new();
            let writes = log.writes.get(&table).unwrap_or(&none);
            let empty = Vec::new();
            let scans = log.scans.get(&table).unwrap_or(&empty);
            for (rule, found) in audit_scans(writes, scans) {
                cluster
                    .checks
                    .violate(&cluster.world, rule, format!("{}: {found}", table.name()));
            }
        }
        for found in incomplete_chosen(&log.created, &log.statuses, &log.chosen) {
            cluster
                .checks
                .violate(&cluster.world, topology::INCOMPLETE, found);
        }
        if let Some(dropped) = log.dropped {
            for found in dropped_silent(dropped, &log.calls) {
                cluster
                    .checks
                    .violate(&cluster.world, topology::DROPPED, found);
            }
        }
    });
}

/// The clauses of O12, O16, O21, O28, D07 and D11 that ran.
fn clauses_of(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    moving: Option<&moves::Move>,
    scenarios: &mut Scenarios,
) {
    let faults = chaos.map_or(&[][..], |chaos| &chaos.log);
    let now = elapsed_ms(&cluster.world);
    let overlaps = |from: u64, to: u64, actions: &[Action]| {
        faults.iter().any(|fault| {
            actions.contains(&fault.action) && fault.from <= to && from <= fault.to.unwrap_or(now)
        })
    };
    let attempts = |ctx: &Ctx| ctx.observed.lock().unwrap().attempts.clone();
    let loads = [
        (Table::Main, attempts(&t.main)),
        (Table::Ranged, attempts(&t.ranged)),
    ];
    let committed = |table: Table, from: u64, to: u64| {
        loads.iter().any(|(loaded, attempts)| {
            *loaded == table
                && attempts.iter().any(|a| {
                    a.answer == Answer::Committed && a.invoked <= to && from <= a.completed
                })
        })
    };
    const LOSSES: [Action; 3] = [
        Action::CrashLeader,
        Action::PowerLeader,
        Action::DrainLeader,
    ];
    t.log(|log| {
        for build in &log.builds {
            if build.refused && build.kind == IndexKind::SameName {
                scenarios.insert("O12:refused-declaration");
            }
            let (Some(from), Some(to)) = (build.declared, build.ready) else {
                continue;
            };
            if build.kind == IndexKind::UnknownColumn {
                continue;
            }
            if committed(build.table, from, to) {
                scenarios.insert("O12:build-under-load");
                scenarios.insert("D11:index");
            }
            let chosen = log.chosen.iter().any(|choice| choice.index == build.name);
            let explained = |from: u64, to: u64| {
                log.explain_ms
                    .iter()
                    .any(|(table, at)| *table == build.table && from <= *at && *at <= to)
            };
            if chosen {
                scenarios.insert("O12:present-once");
            }
            if explained(from, to) {
                scenarios.insert("O12:incomplete-not-chosen");
            }
            if overlaps(from, to, &[Action::FreezeFollower, Action::FreezeTwo]) {
                scenarios.insert("O12:build-paused");
            }
            if overlaps(from, to, &LOSSES) {
                scenarios.insert("O12:worker-lost");
            }
            if let Some(retired) = build.retired
                && chosen
                && explained(retired, now)
            {
                scenarios.insert("O12:retired");
            }
        }
        if let Some((from, published)) = log.split {
            let to = published.unwrap_or(now);
            if committed(Table::Ranged, from, to) {
                // It ran whether or not the split published: one that did not is `Liveness`.
                scenarios.insert("D07:split");
                scenarios.insert("O16:split-during-commits");
            }
            let scanned = log
                .scan_ms
                .iter()
                .any(|(table, a, b)| *table == Table::Ranged && *a <= to && from <= *b);
            if scanned {
                scenarios.insert("O16:split-during-scans");
            }
        }
        let restarted = log
            .scan_ms
            .iter()
            .any(|(_, a, b)| overlaps(*a, *b, &LOSSES));
        if restarted {
            scenarios.insert("O16:restart-during-scans");
        }
        let after = log.dropped.map_or(0, |dropped| {
            log.calls
                .iter()
                .filter(|call| call.invoked > dropped)
                .count()
        });
        let before = log.calls.iter().any(|call| call.success);
        if after > 0 && before {
            scenarios.insert("D11:drop");
        }
    });
    if let Some((from, to)) = moving.and_then(moves::Move::window)
        && (committed(Table::Main, from, to) || committed(Table::Ranged, from, to))
    {
        scenarios.insert("O16:move-during-commits");
    }
}

/// The phase 10 clauses and their assertions, over every table's attempts.
fn evaluate(
    cluster: &Cluster,
    config: &Config,
    t: &Topo,
    moved: Option<&Ctx>,
    chaos: Option<&Chaos>,
    moving: Option<&moves::Move>,
    report: &mut direct::Report,
) {
    let mut attempts = Vec::new();
    let mut resolved = BTreeMap::new();
    let (mut same, mut other) = (0, 0);
    let mut unresolved = Vec::new();
    let contexts = [Some(&t.main), Some(&t.ranged), Some(&t.victim), moved];
    for ctx in contexts.into_iter().flatten() {
        let mut observed = ctx.observed.lock().unwrap();
        // The victim's clients retry without a replay, and its writes end with the drop.
        if !std::ptr::eq(ctx, &t.victim) {
            attempts.extend(observed.attempts.iter().cloned());
            resolved.append(&mut observed.resolved);
            unresolved.append(&mut observed.unresolved);
            same += observed.reuse_same_key;
            other += observed.reuse_other_key;
        }
    }
    attempts.sort_by_key(|attempt| (attempt.completed, attempt.invoked));
    report
        .assertions
        .push(("every request resolved", unresolved.is_empty()));
    for request in unresolved {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("request unresolved {CONVERGE_LIMIT:?} after the heal: {request}"),
        );
    }
    let no_quorum = cluster.no_quorum();
    let drops = chaos.map(Chaos::session_drops).unwrap_or_default();
    let outcome = clauses::Evidence {
        faults: chaos.map_or(&[], |chaos| &chaos.log),
        attempts: &attempts,
        resolved: &resolved,
        reuse_same_key: same,
        reuse_other_key: other,
        session_drops: &drops,
        no_quorum: &no_quorum,
        moving: moving.and_then(moves::Move::window),
        deadline_ms: config.deadline().as_millis() as u64,
    }
    .evaluate();
    for (invariant, detail) in outcome.violations {
        cluster.checks.violate(&cluster.world, invariant, detail);
    }
    report.scenarios.extend(outcome.clauses);
}

/// An observation beside the verdict. The `direct` checks read every `conflict` as a key or
/// body conflict, because nothing else conflicts on the direct path. A mutation that fell back
/// to the queued path during a schema change can be refused with a `conflict` of its own. The
/// verdict of the run keeps the strict reading; this counts those refusals, and the kinds that
/// remain when they are read as any other definite refusal.
fn without_schema_conflicts(history: &History) -> (u64, BTreeMap<String, u64>) {
    let mut events = history.events().to_vec();
    let mut conflicts = 0;
    for event in &mut events {
        let overtaken = event.key.is_some()
            && event.value["code"] == "conflict"
            && event.value["message"] == "captured schema or lifecycle changed";
        if overtaken {
            conflicts += 1;
            event.value["code"] = json!("storage_unavailable");
        }
    }
    let mut kinds = BTreeMap::new();
    if conflicts > 0 {
        for anomaly in nabedra_check::checker::direct::check(&events).anomalies {
            *kinds.entry(anomaly.kind).or_default() += 1;
        }
    }
    (conflicts, kinds)
}

fn summarize(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    moving: bool,
    split: SplitTiming,
    report: &mut direct::Report,
) {
    let counts = |actions: &BTreeMap<Action, u32>| -> BTreeMap<&'static str, u32> {
        actions.iter().map(|(a, n)| (a.name(), *n)).collect()
    };
    let (ddl_conflicts, remaining) = without_schema_conflicts(&t.main.history.lock().unwrap());
    report.summary = t.log(|log| {
        json!({
            "schema_conflicts": ddl_conflicts,
            "anomalies_without_schema_conflicts": remaining,
            "workload": Workload::Topology.name(),
            "profile": chaos.map(Chaos::profile_name),
            "executed": chaos.map(|chaos| counts(&chaos.executed)),
            "skipped": chaos.map(|chaos| counts(&chaos.skipped)),
            "split": format!("{split:?}"),
            "ranged_split_ms": log.split,
            "move": moving,
            "audited_writes": log.writes.values().map(Vec::len).sum::<usize>(),
            "scans": log.scans.values().map(Vec::len).sum::<usize>(),
            "explains": log.explain_ms.len(),
            "explains_on_index": log.chosen.len(),
            "statuses": log.statuses.len(),
            "builds": log.builds.iter().map(|b| json!({
                "name": b.name, "table": b.table.name(), "kind": format!("{:?}", b.kind),
                "declared": b.declared, "ready": b.ready, "retired": b.retired,
                "refused": b.refused, "abandoned": b.abandoned, "stuck": b.stuck, "open": b.open,
            })).collect::<Vec<_>>(),
            "victim_calls": log.calls.len(),
            "dropped": log.dropped.is_some(),
            "fence_races": log.races,
            "at_rest": log.rest,
            "rules": RULES,
            "heal_cold_start": chaos.is_some_and(|chaos| chaos.heal_cold),
        })
    });
    cluster.record(format!("workload summary {}", report.summary));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_plan_parses_and_rejects_unknown_entries() {
        assert_eq!(Forced::parse("").unwrap(), Forced::default());
        let forced = Forced::parse("split-under-load, no-move").unwrap();
        assert_eq!(forced.split, Some(SplitTiming::UnderLoad));
        assert_eq!(forced.moving, Some(false));
        assert!(Forced::parse("split").is_err());
    }

    #[test]
    fn plan_text_names_the_index_and_the_ranges() {
        let indexed = "engine oltp; replicated MVCC\naccess secondary main_value seek; composite-prefix 0; lower >=; upper none; covering true\nscope shards 2; primary-key lower none, upper none";
        assert_eq!(plan_of(indexed), (Some("main_value".into()), Some(2)));
        let primary =
            "engine oltp; replicated MVCC\naccess primary scan on id\nscope shards 1; primary-key";
        assert_eq!(plan_of(primary), (None, Some(1)));
        assert_eq!(plan_of("engine olap"), (None, None));
    }

    #[test]
    fn scan_rows_reject_another_shape() {
        assert_eq!(rows(&json!([{"id": 1, "value": 2}])), Some(vec![(1, 2)]));
        assert_eq!(rows(&json!([{"id": 1}])), None);
        assert_eq!(rows(&json!({"rows": []})), None);
    }

    /// Both split timings, with and without a move, are drawn. The plan never drops a table
    /// before it was used or leaves an index without its table.
    #[test]
    fn plans_cover_both_ways() {
        let selection = Selection::parse(&[]).unwrap();
        let mut seen = BTreeSet::new();
        for seed in 0..64 {
            let plan = TopoPlan::draw(&mut Gen::from_seed(seed), &selection, Forced::default());
            seen.insert((plan.split == SplitTiming::UnderLoad, plan.moving.is_some()));
            assert!(plan.boundary >= 2 && plan.boundary <= plan.ranged.keys.max(2));
            assert!(plan.drop_at < CHAOS && !plan.audits.is_empty());
        }
        assert_eq!(seen.len(), 4);
    }
}
