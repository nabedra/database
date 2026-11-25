//! The `topology` workload (design 3.6, phase 11): splits, moves and leader transfers under
//! load, index publication, a table drop, and gateway route caches with random staleness.
//!
//! The phase 10 nemesis runs unchanged next to the topology operations. The run has these
//! tables:
//!
//! - `main` has the direct clients of the phase 10 workloads, auditors and indexes. The
//!   nemesis splits it under load when its profile picks `split`.
//! - `ranged` has fewer clients, auditors and indexes. It is split before any client starts or
//!   under load, as the seed draws. A split that doesn't publish within its bound after the heal
//!   is reported as `Liveness`.
//! - `victim` is written by its own clients and dropped under load, with an index publication
//!   racing the drop, and then created again under the same name.
//! - `moved` has one client and a replica move, in the seeds that draw one.
//! - `quiet` is written once, before the chaos. Both gateways keep a route to it that the
//!   topology changes since then have aged. Its stage at the end changes the topology once more
//!   and then uses each cached route, with nothing else running.
//! - `fence` has one row. At the end pairs of declarations race for one index name on it, so that
//!   one of them fails its validation while it holds the table's guard.
//!
//! Auditors own keys that nobody else writes and scan them through a query that a covering index
//! can serve, so an acknowledged insert must be in every later scan. The rules are in
//! [`crate::invariants::topology`]; this module records what happened, and how often each rule
//! was judged. The history goes through the `direct` checks.

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
/// Rounds without a final answer before an index is declared under a new build ID. The number is
/// the workload's.
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
/// one observation the rule reads, so they test the rule and its wiring. `LoseRow` can be caught
/// by a scan before any index exists.
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
    /// The declaration was not refused for good, and its build has not published.
    open: bool,
    /// Declarations that were ever unanswered and are not known to be accepted. A refusal of a
    /// later attempt under the same ID does not settle the first one.
    unanswered: BTreeSet<Uuid>,
    /// The ID of the final refusal, if it was never unanswered.
    refused_id: Option<Uuid>,
}

impl Build {
    /// Whether a schema change of its table may be open.
    fn is_open(&self) -> bool {
        self.open || !self.unanswered.is_empty()
    }
}

/// A position in the run-wide order of observations, and the virtual ms.
type Stamp = (u64, u64);

/// When an index was surely building: from the end of the first status that said so to the start
/// of the last one.
#[derive(Clone, Copy, Debug)]
struct Building {
    from: Stamp,
    to: Stamp,
}

/// The split of `ranged`, in virtual ms.
#[derive(Clone, Copy, Debug)]
struct RangedSplit {
    requested: u64,
    /// It had surely not published when this request was sent.
    pending: u64,
    published: Option<u64>,
}

#[derive(Default)]
struct Log {
    writes: BTreeMap<Table, Vec<Write>>,
    scans: BTreeMap<Table, Vec<Scan>>,
    /// Scans in virtual ms, for the scenario clauses.
    scan_ms: Vec<(Table, u64, u64)>,
    /// Scans whose plan used a covering index, by their position in the order.
    indexed: BTreeMap<Table, Vec<u64>>,
    /// Tables whose final scan and exact count were answered and compared.
    counted: BTreeSet<Table>,
    created: BTreeSet<String>,
    statuses: Vec<Status>,
    chosen: Vec<Chosen>,
    /// When each explain ran.
    explains: Vec<(Table, u64, u64)>,
    building: BTreeMap<String, Building>,
    builds: Vec<Build>,
    /// Refused index builds and their selectors.
    refused: BTreeMap<Uuid, String>,
    selectors: BTreeMap<Uuid, Value>,
    /// Refused builds whose status an operator then read.
    refused_judged: BTreeSet<Uuid>,
    dropped: Option<u64>,
    /// When the drop was sent and acknowledged, in virtual ms.
    drop_ms: Option<(u64, u64)>,
    /// The rows of the table created after the drop were counted.
    recreated: bool,
    calls: Vec<TableCall>,
    split: Option<RangedSplit>,
    split_error: Option<String>,
    /// How many split IDs were sent for `ranged`.
    split_ids: u64,
    /// What each fence race did.
    races: Vec<String>,
    /// What the tables did at rest.
    rest: Vec<String>,
    /// Tables and races whose fence could not be judged, and why.
    fence_unjudged: Vec<String>,
    /// How often [`topology::FENCE`], [`topology::ROUTES`] and [`topology::STALE_ROUTE`] were
    /// judged.
    fence_judged: u64,
    routes_judged: u64,
    stale_judged: u64,
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
    /// A worker that executes it refused it for good. The flag says that another worker left it
    /// unanswered in the same round.
    Definite(ErrorCode, String, bool),
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
            Reply::Definite(_, why, unknown) | Reply::Retry { why, unknown } => {
                format!("{why} (unknown {unknown})")
            }
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

    fn stamp(&self) -> Stamp {
        (self.next(), self.now())
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
                        return Reply::Definite(error.code, message, unknown);
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
        log.explains.push((table, invoked, completed));
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

/// How many collects of `table` the workers ran since they started, and how many of them used a
/// covering index.
async fn collects(t: &Topo, table: Table) -> Result<(u64, u64), String> {
    let mut sums = (0, 0);
    for node in WORKERS {
        let id = fresh_id(BOOTSTRAP_CLIENT).await;
        let body = operation(id, "query.statistics", json!({}));
        let answer = request(
            BOOTSTRAP_CLIENT,
            node,
            ADMIN,
            &t.admin_token,
            Some(body),
            ADMIN_LIMIT,
        )
        .await
        .and_then(|reply| reply.completion(id))?;
        let Completion::Ok(data) = &answer else {
            return Err(clients::failure(&answer).unwrap_or_default());
        };
        let (ran, covered) = plans_of(&data["statistics"], &t.ctx(table).table)?;
        sums = (sums.0 + ran, sums.1 + covered);
    }
    Ok(sums)
}

fn plans_of(statistics: &Value, table: &str) -> Result<(u64, u64), String> {
    let shapes = statistics["shapes"].as_array();
    let (Some(shapes), Some(0)) = (shapes, statistics["untracked_executions"].as_u64()) else {
        return Err(format!("query.statistics answered {statistics}"));
    };
    let mut sums = (0, 0);
    for shape in shapes {
        if shape["table_id"] == json!(table) && shape["operation"] == "collect" {
            let count = |value: &Value| value.as_u64().ok_or(format!("a shape is {shape}"));
            sums.0 += count(&shape["executions"])?;
            sums.1 += count(&shape["plans"]["covering_index"])?;
        }
    }
    Ok(sums)
}

/// A scan while nothing else queries the table, so that the workers' own counts say which plan it
/// used.
async fn scan_alone(
    t: &Topo,
    client: &mut Client,
    node: u64,
    gateway: usize,
    table: Table,
) -> Option<String> {
    let before = collects(t, table).await;
    let at = t.next();
    if let Some(why) = scan(t, client, node, gateway, table).await {
        return Some(why);
    }
    match (before, collects(t, table).await) {
        (Ok(before), Ok(after)) => {
            let (ran, covered) = (
                after.0.saturating_sub(before.0),
                after.1.saturating_sub(before.1),
            );
            if ran > 0 && ran == covered {
                t.log(|log| log.indexed.entry(table).or_default().push(at));
            }
        }
        (Err(why), _) | (_, Err(why)) => t.note("scan-plan-unknown", json!(why)),
    }
    None
}

/// Write keys nobody else writes, and scan them.
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

fn record_status(t: &Topo, name: &str, invoked: Stamp, completed: Stamp, data: &Value) -> String {
    let state = data["state"].as_str().unwrap_or("").to_owned();
    t.log(|log| {
        log.statuses.push(Status {
            index: name.to_owned(),
            invoked: invoked.0,
            completed: completed.0,
            state: state.clone(),
        });
        if state == "building" {
            let building = Building {
                from: completed,
                to: invoked,
            };
            log.building
                .entry(name.to_owned())
                .and_modify(|known| known.to = invoked)
                .or_insert(building);
        }
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
        unanswered: BTreeSet::new(),
        refused_id: None,
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
            "abandoned": build.abandoned, "stuck": build.stuck,
            "unanswered": build.unanswered}),
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
    let mut opened: Vec<Uuid> = Vec::new();
    let mut refusals = 0;
    let mut state = 'declare: loop {
        let invoked = t.stamp();
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
                let completed = t.stamp();
                if t.take_defect(Defect::LieAboutRefusal) {
                    t.log(|log| log.refused.insert(id, "seeded".into()));
                }
                break record_status(t, &plan.name, invoked, completed, &data);
            }
            Reply::Definite(code, why, unknown) => {
                if unknown {
                    build.unanswered.insert(id);
                }
                // The index of a dropped table, and the two kinds that fail their validation,
                // are refused for good. Nothing else is expected to be.
                if !build.unanswered.contains(&id) {
                    t.log(|log| log.refused.insert(id, format!("{code:?} {why}")));
                }
                if overtaken(code, &why) && !t.builds_over() {
                    // A declaration that was never answered can hold the table's guard until
                    // its own ID is sent again.
                    let mut unanswered = Vec::new();
                    for earlier in std::mem::take(&mut opened) {
                        let invoked = t.stamp();
                        match t.admin(earlier, "storage.index.create", definition).await {
                            Reply::Ok(data) => {
                                let completed = t.stamp();
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
                    refusals = 0;
                    time::sleep(RETRY).await;
                    continue;
                }
                // The name may be taken by an earlier declaration of this index that was
                // never answered.
                for &earlier in &opened {
                    let invoked = t.stamp();
                    let ask = fresh_id(BOOTSTRAP_CLIENT).await;
                    let earlier_build = selector(earlier);
                    let asked = t.admin(ask, "storage.index.status", &earlier_build).await;
                    if let Reply::Ok(data) = asked {
                        let completed = t.stamp();
                        id = earlier;
                        break 'declare record_status(t, &plan.name, invoked, completed, &data);
                    }
                }
                build.refused = true;
                build.open = false;
                build.refused_id = (!build.unanswered.contains(&id)).then_some(id);
                let expected = plan.kind != IndexKind::Valid
                    || plan.table == Table::Victim
                    || (code == ErrorCode::Conflict && why.contains("already exists"));
                if !expected {
                    build.stuck = Some(format!("declaration refused: {why}"));
                }
                return;
            }
            Reply::Retry { why, unknown } => {
                if unknown {
                    build.unanswered.insert(id);
                }
                refusals += 1;
                if t.builds_over() {
                    build.stuck = Some(format!("declaration unanswered: {why}"));
                    return;
                }
                if refusals >= FRESH_BUILD {
                    if build.unanswered.contains(&id) {
                        opened.push(id);
                    } else {
                        t.log(|log| log.refused.insert(id, why));
                    }
                    id = fresh_id(BOOTSTRAP_CLIENT).await;
                    t.log(|log| log.selectors.insert(id, selector(id)));
                    refusals = 0;
                }
                time::sleep(RETRY).await;
            }
        }
    };
    // Its own outcome is known: it was accepted.
    build.unanswered.remove(&id);
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
        let invoked = t.stamp();
        match t.admin(ask, name, &selector(id)).await {
            Reply::Ok(data) => {
                let completed = t.stamp();
                state = record_status(t, &plan.name, invoked, completed, &data);
                last.clear();
            }
            Reply::Definite(code, why, _) => {
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
        let invoked = t.stamp();
        match t.admin(ask, "storage.index.retire", &selector(id)).await {
            Reply::Ok(data) => {
                let completed = t.stamp();
                let state = record_status(t, &plan.name, invoked, completed, &data);
                if state == "retired" {
                    build.open = false;
                    build.retired = Some(t.now());
                    return;
                }
                last = format!("retire answered {state}");
            }
            Reply::Definite(code, why, _) => last = format!("{code:?} {why}"),
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
        let mut first = None;
        for tries in 1_u64.. {
            let invoked = t.next();
            // A read has no identity.
            let first = match what {
                "table.get" => invoked,
                _ => *first.get_or_insert(invoked),
            };
            let answer = clients::attempt(
                &ctx,
                &mut client,
                node,
                GATEWAYS[gateway],
                &request,
                tries == 1,
            )
            .await;
            t.log(|log| {
                log.calls.push(TableCall {
                    what: format!("{what} of key {key} through n{}", GATEWAYS[gateway]),
                    first,
                    invoked,
                    success: succeeded(&answer),
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

/// Whether an operation succeeded, or `None` if its outcome is unknown.
fn succeeded(answer: &Completion) -> Option<bool> {
    (!clients::unknown(answer)).then_some(matches!(answer, Completion::Ok(_)))
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
    let sent = t.now();
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
    let (dropped, at) = t.stamp();
    t.log(|log| {
        log.dropped = Some(dropped);
        log.drop_ms = Some((sent, at));
    });
    t.note("dropped", json!({"table": t.victim.table}));
    for (gateway, through) in GATEWAYS.into_iter().enumerate() {
        let id = fresh_id(node).await;
        let invoked = t.next();
        let payload = json!({"name": "sim_victim"});
        let answer = t
            .public(&mut client, node, gateway, id, "table.describe", payload)
            .await;
        t.log(|log| {
            log.calls.push(TableCall {
                what: format!("table.describe through n{through}"),
                first: invoked,
                invoked,
                success: succeeded(&answer),
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
            Completion::Ok(count) if *count == json!(0) => {
                t.log(|log| log.recreated = true);
                return None;
            }
            Completion::Ok(count) => {
                t.log(|log| log.recreated = true);
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
    let mut split = RangedSplit {
        requested,
        pending: requested,
        published: None,
    };
    t.log(|log| {
        log.split = Some(split);
        log.split_ids = 1;
    });
    t.note("split-ranged", json!({"id": id, "boundary": boundary}));
    let mut last = String::new();
    // A request that was ever unanswered may have published, whatever a later one is told.
    let mut unanswered = false;
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
        let sent = t.now();
        let reply = t.admin(id, "storage.shard.split", &payload).await;
        if let Reply::Definite(_, _, true) | Reply::Retry { unknown: true, .. } = reply {
            unanswered = true;
        }
        let told = matches!(&reply, Reply::Ok(data) if data["state"] != "published");
        if told || !unanswered && !matches!(reply, Reply::Ok(_)) {
            split.pending = sent;
        }
        let why = match reply {
            Reply::Ok(data) if data["state"] == "published" => {
                split.published = Some(t.now());
                t.log(|log| log.split = Some(split));
                t.note("split-ranged-published", data);
                return;
            }
            Reply::Ok(data) => data.to_string(),
            Reply::Definite(code, why, _) => {
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
    // The nemesis aims at the leader of `main`.
    let watched = Uuid::parse_str(&t.main.table).map_err(|error| format!("table UUID: {error}"))?;
    cluster.watch_table(watched);
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
    for (node, stats) in crate::workload::steady(cluster).await? {
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }

    let faults = NetFaults::install(cluster.net(), &cluster.world, seed);
    config.jump_clocks(&cluster.world);
    let start = elapsed_ms(&cluster.world);
    let profile = selection.profile(seed);
    let mut chaos = (!no_nemesis && profile.total_weight() > 0)
        .then(|| Chaos::new(&cluster.world, profile, faults.clone(), start, g));
    if no_nemesis {
        plan.stale.clear();
    }
    let mut moving = plan.moving.take();
    if let (Some(moving), Some(moved)) = (moving.as_mut(), moved.as_ref()) {
        moving.start(cluster, moved, &faults);
    }
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
    let load_from = elapsed_ms(&cluster.world);
    let end = cluster.world.elapsed() + CHAOS;
    while cluster.world.elapsed() < end {
        if let Some(chaos) = chaos.as_mut() {
            chaos.tick(cluster, &ctx, g).await;
        }
        *t.run.lock().unwrap() = cluster.config.run_id;
        stale_windows(cluster, &t, &mut plan.stale, &mut frozen, start);
        time::sleep(TICK).await;
    }
    ctx.stopping.store(true, Ordering::Release);
    let load = (load_from, elapsed_ms(&cluster.world));
    let mut seen = Seen {
        load,
        work: progress(cluster, &t, moved.as_ref(), load),
        moved_checked: false,
    };
    if cluster.elections().is_empty() {
        cluster.checks.violate(
            &cluster.world,
            "LeaderUnknown",
            "no election of the range of main was seen on the wire",
        );
    }
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
        judge(cluster, &t);
        summarize(
            cluster,
            &t,
            chaos.as_ref(),
            moving.as_ref(),
            plan.split,
            &seen,
            true,
            &mut report,
        );
        abandon(cluster, &t, tasks).await;
        return Ok(report);
    }
    faults.heal();
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
        judge(cluster, &t);
        summarize(
            cluster,
            &t,
            chaos.as_ref(),
            moving.as_ref(),
            plan.split,
            &seen,
            true,
            &mut report,
        );
        abandon(cluster, &t, tasks).await;
        return Ok(report);
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
    // The move and the nemesis's split run on in their own tasks, so their bounds are judged
    // before the wait for the other changes, which have a budget of their own.
    let quiet_from = cluster.world.elapsed();
    let mut retired = None;
    if let Some(moving) = moving.as_mut() {
        retired = Some(moving.finish(cluster, healed).await);
    }
    if let Some(chaos) = chaos.as_mut().filter(|chaos| chaos.split_pending()) {
        chaos.finish_split(cluster, quiet_from).await;
    }
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
    if let Some(chaos) = chaos.as_ref() {
        chaos.check_abandoned().await;
    }
    if let (Some(moving), Some(moved), Some(retired)) = (moving.as_mut(), moved.as_ref(), retired) {
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
    }
    if let Some(moving) = moving.as_ref() {
        report.scenarios.extend(moving.clauses());
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
    if let Some(moved) = moved.as_ref() {
        match clients::final_checks(moved).await {
            Ok(()) => seen.moved_checked = true,
            Err(error) => {
                cluster
                    .checks
                    .violate(&cluster.world, "Liveness", format!("moved: {error}"));
            }
        }
    }
    let rested = at_rest(cluster, &t, &mut live).await;
    // A table with an open schema change is not judged, and says nothing here.
    if let Some(passed) = rested.as_ref().map_or(Some(false), |judged| *judged) {
        report.assertions.push(("direct mutations at rest", passed));
    }
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
        moved.as_ref(),
        &seen,
        report.assertions.contains(&("replica move retired", true)),
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
        moving.as_ref(),
        plan.split,
        &seen,
        false,
        &mut report,
    );
    Ok(report)
}

/// The run cannot converge: stop the clients and the changes, and keep the violations already
/// recorded. None of the final assertions ran, so no scenario is claimed.
async fn abandon(cluster: &Cluster, t: &Topo, tasks: Vec<JoinHandle<()>>) {
    t.change_by.store(0, Ordering::Release);
    t.main.converge_by.store(0, Ordering::Release);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    cluster.record("workload abandoned after a liveness violation".into());
}

/// What the run itself saw, for the clauses and the summary.
struct Seen {
    /// The time in which clients, auditors and the nemesis ran, in virtual ms.
    load: (u64, u64),
    work: Work,
    /// The final operations on `moved` succeeded.
    moved_checked: bool,
}

/// Work that was completed under load.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Work {
    writes: u64,
    reads: u64,
    scans: u64,
}

impl Work {
    fn of(attempts: &[clauses::Attempt], scans: &[(u64, u64)], load: (u64, u64)) -> Self {
        let within = |scan: &&(u64, u64)| load.0 <= scan.0 && scan.1 <= load.1;
        let (writes, reads) = direct::work(attempts, load.0, load.1);
        Self {
            writes,
            reads,
            scans: scans.iter().filter(within).count() as u64,
        }
    }

    fn missing(&self) -> Vec<&'static str> {
        [
            (self.writes, "write committed"),
            (self.reads, "read was answered"),
            (self.scans, "scan was answered"),
        ]
        .into_iter()
        .filter(|(done, _)| *done == 0)
        .map(|(_, what)| what)
        .collect()
    }
}

/// A seed whose clients or auditors completed no work under load proves nothing, so it is never
/// clean (lead decision 17). A scan is work of its own: the index rules are judged by scans.
fn progress(cluster: &Cluster, t: &Topo, moved: Option<&Ctx>, load: (u64, u64)) -> Work {
    let mut attempts = Vec::new();
    for ctx in [Some(&t.main), Some(&t.ranged), moved]
        .into_iter()
        .flatten()
    {
        attempts.extend(ctx.observed.lock().unwrap().attempts.iter().cloned());
    }
    let scans: Vec<_> = t.log(|log| log.scan_ms.iter().map(|(_, a, b)| (*a, *b)).collect());
    let work = Work::of(&attempts, &scans, load);
    for what in work.missing() {
        cluster.checks.violate(
            &cluster.world,
            "Progress",
            format!("no {what} from {}ms to {}ms, under load", load.0, load.1),
        );
    }
    work
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
    let published = split.is_none_or(|split| split.published.is_some());
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

/// What every worker did with the mutations it was sent: fallbacks because of the schema, all
/// fallbacks, and direct proposals.
async fn mutation_stats(cluster: &Cluster) -> Result<[u64; 3], String> {
    let mut sums = [0; 3];
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        let direct = &stats["direct"];
        let count = |value: &Value| value.as_u64().ok_or(format!("n{node} stats: {direct}"));
        sums[0] += count(&direct["fallbacks"]["schema"])?;
        let all = direct["fallbacks"].as_object();
        for fallback in all.ok_or(format!("n{node} stats: {direct}"))?.values() {
            sums[1] += count(fallback)?;
        }
        sums[2] += count(&direct["entries_proposed"])?;
    }
    Ok(sums)
}

/// Upserts of one table sent alone, one after another: how many were sent to the queued path for
/// the schema, and how many committed directly.
async fn mutate_alone(
    cluster: &Cluster,
    ctx: &Ctx,
    client: &mut Client,
    (node, gateway): (u64, u64),
    rows: &[(i64, i64)],
) -> Result<(u64, u64), String> {
    let (mut schema, mut direct) = (0, 0);
    for &(key, value) in rows {
        let before = mutation_stats(cluster).await?;
        let id = fresh_id(node).await;
        let request = Request::row("table.upsert", &ctx.table, key, value, id);
        let answer = clients::attempt(ctx, client, node, gateway, &request, true).await;
        let after = mutation_stats(cluster).await?;
        let grew = |at: usize| after[at].saturating_sub(before[at]);
        schema += grew(0);
        direct += u64::from(matches!(answer, Completion::Ok(_)) && grew(1) == 0 && grew(2) > 0);
    }
    Ok((schema, direct))
}

/// [`topology::FENCE`] in its public form: with every schema change settled, a mutation takes
/// the direct path. Convergence may be polled, so a Catalog intent that the first fallback
/// resolves is not a finding. Returns whether the judged tables pass, or `None` if no table could
/// be judged.
async fn at_rest(
    cluster: &Cluster,
    t: &Topo,
    live: &mut Vec<(Ctx, Table)>,
) -> Result<Option<bool>, String> {
    let mut rest = Vec::new();
    for (ctx, table) in live.drain(..) {
        // A declaration that was ever unanswered, or a build that has not published, can leave a
        // schema change open.
        let open = t.log(|log| {
            let open = |b: &&Build| b.table == table && b.is_open();
            log.builds.iter().filter(open).count()
        });
        if open > 0 {
            let skipped = format!("{}: {open} schema changes are open", table.name());
            t.log(|log| log.fence_unjudged.push(skipped));
            continue;
        }
        let mut client = ctx.new_client();
        for (index, gateway) in GATEWAYS.into_iter().enumerate() {
            let node = CLIENTS[index % CLIENTS.len()];
            let started = cluster.world.elapsed();
            loop {
                let rows: Vec<_> = BTreeSet::from([1, ctx.keys])
                    .into_iter()
                    .map(|key| (key, 910_000_000 + elapsed_ms(&ctx.world) as i64 * 100 + key))
                    .collect();
                let (mut schema, direct) =
                    mutate_alone(cluster, &ctx, &mut client, (node, gateway), &rows).await?;
                let polled = cluster.world.elapsed() - started;
                let writes = rows.len() as u64;
                if (schema > 0 || direct < writes) && polled < REST_LIMIT {
                    time::sleep(RETRY).await;
                    continue;
                }
                if t.take_defect(Defect::StuckFence) {
                    schema += 1;
                }
                rest.push(AtRest {
                    table: table.name(),
                    gateway,
                    writes,
                    schema_fallbacks: schema,
                    direct,
                    polled_ms: polled.as_millis() as u64,
                });
                break;
            }
        }
    }
    let passed = judge_fences(cluster, t, &rest);
    let indirect: Vec<_> = rest.iter().filter(|rest| !rest.judged()).collect();
    if !indirect.is_empty() {
        return Err(format!("no direct commit: {indirect:?}"));
    }
    Ok((!rest.is_empty()).then_some(passed))
}

/// Judge what was sent at rest. Returns whether nothing is wrong with it.
fn judge_fences(cluster: &Cluster, t: &Topo, rest: &[AtRest]) -> bool {
    for rest in rest {
        let shown = format!(
            "{} n{}: {} of {} committed directly and {} fell back for the schema after {} ms",
            rest.table,
            rest.gateway,
            rest.direct,
            rest.writes,
            rest.schema_fallbacks,
            rest.polled_ms
        );
        t.log(|log| {
            if rest.judged() {
                log.fence_judged += 1;
            } else {
                log.fence_unjudged.push(shown.clone());
            }
            log.rest.push(shown);
        });
    }
    let found = fence_at_rest(rest);
    let passed = found.is_empty();
    for found in found {
        cluster
            .checks
            .violate(&cluster.world, topology::FENCE, found);
    }
    passed
}

/// With no client running: scans, explains and one count per table, not retried. The workers'
/// statistics say which plan each scan used.
async fn final_scans(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    report: &mut direct::Report,
) -> Result<(), String> {
    let mut client = t.main.new_client();
    // The nemesis's split can be pending.
    let asked = chaos.is_some_and(Chaos::split_requested);
    let nemesis_split = asked && !chaos.is_some_and(Chaos::split_pending);
    let (split, split_ids) = t.log(|log| (log.split, log.split_ids));
    for table in [Table::Main, Table::Ranged] {
        let (published, asked) = match table {
            Table::Main => (nemesis_split, u64::from(asked)),
            _ => (
                split.is_some_and(|split| split.published.is_some()),
                split_ids,
            ),
        };
        // One range splits once, whichever of its IDs did it.
        let open = u64::from(!published && asked > 0);
        for (gateway, _) in GATEWAYS.iter().enumerate() {
            let node = CLIENTS[gateway % CLIENTS.len()];
            if let Some(why) = scan_alone(t, &mut client, node, gateway, table).await {
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
            t.log(|log| log.routes_judged += 1);
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
        t.log(|log| log.counted.insert(table));
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
            let id = fresh_id(BOOTSTRAP_CLIENT).await;
            let payload = json!({
                "source": initial_range(cluster_id, table),
                "boundary": plan.quiet_boundary, "page_bytes": 1 << 20,
            });
            loop {
                let why = match t.admin(id, "storage.shard.split", &payload).await {
                    Reply::Ok(data) if data["state"] == "published" => break,
                    Reply::Ok(data) => data.to_string(),
                    Reply::Definite(_, why, _) | Reply::Retry { why, .. } => why,
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
                    Reply::Definite(_, why, _) | Reply::Retry { why, .. } => why,
                };
                if t.now() >= deadline {
                    return Err(format!("the index of an idle table is not ready: {why}"));
                }
                time::sleep(STATUS_POLL).await;
                let ask = fresh_id(BOOTSTRAP_CLIENT).await;
                reply = t.admin(ask, "storage.index.status", &selector).await;
                if matches!(reply, Reply::Definite(ErrorCode::NotFound, ..)) {
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
    let met = refreshes.iter().filter(|r| r.stale_answers > 0).count();
    t.log(|log| log.stale_judged += met as u64);
    if plan.quiet == QuietChange::Split && met > 0 {
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

/// One declaration of the fence table, sent again under its ID while it is unanswered. The last
/// value says whether any attempt was unanswered.
async fn declare(t: &Topo, table: &str, name: &str) -> (Uuid, Value, Reply, bool) {
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
    let mut unanswered = false;
    loop {
        let reply = t.send(id, "storage.index.create", &definition).await;
        if let Reply::Definite(_, _, true) | Reply::Retry { unknown: true, .. } = reply {
            unanswered = true;
        }
        if !matches!(reply, Reply::Retry { .. }) || t.now() >= deadline {
            return (id, selector, reply, unanswered);
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether the build is ready, or why that is not known.
async fn ready(t: &Topo, selector: &Value) -> Result<bool, String> {
    let ask = fresh_id(BOOTSTRAP_CLIENT).await;
    match t.admin(ask, "storage.index.status", selector).await {
        Reply::Ok(data) => Ok(data["state"] == "ready"),
        Reply::Definite(_, why, _) | Reply::Retry { why, .. } => Err(why),
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
/// the direct path must serve it ([`topology::FENCE`]). A race with an unanswered attempt is not
/// judged.
async fn fence_races(
    cluster: &Cluster,
    t: &Topo,
    ctx: &Ctx,
    start: u64,
) -> Result<Scenarios, String> {
    let mut clauses = Scenarios::new();
    let mut client = ctx.new_client();
    let alone = t.now();
    let (_, selector, reply, _) = declare(t, &ctx.table, "fence_alone").await;
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
        let unanswered = first.3 || rival.3;
        let ((_, selector, ..), (loser, refused, why, lost)) = match (first, rival) {
            (won @ (_, _, Reply::Ok(_), _), (id, selector, Reply::Definite(_, why, _), lost))
            | ((id, selector, Reply::Definite(_, why, _), lost), won @ (_, _, Reply::Ok(_), _)) => {
                (won, (id, selector, why, lost))
            }
            ((_, a, first, _), (_, b, rival, _)) => {
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
        if !lost {
            t.log(|log| {
                log.refused.insert(loser, why.clone());
                log.selectors.insert(loser, refused);
            });
        }
        let under_guard = why.contains("name or identity already exists");
        if overtaken(ErrorCode::Conflict, &why) {
            early = lead;
        } else {
            late = lead;
        }
        if unanswered {
            let race = format!("fence race {turn}: a declaration was unanswered");
            t.log(|log| log.fence_unjudged.push(race));
            wait_ready(t, &selector).await?;
            continue;
        }
        let began = t.now();
        let gateway = GATEWAYS[turn % GATEWAYS.len()];
        let (mut writes, mut fell_back, mut direct) = (0, 0, 0);
        loop {
            let row = [(1, 20_000 + turn as i64 * 100 + writes as i64)];
            let (schema, done) =
                mutate_alone(cluster, ctx, &mut client, (CLIENTS[0], gateway), &row).await?;
            writes += 1;
            fell_back += schema;
            direct += done;
            let late = t.now() >= began + REST_LIMIT.as_millis() as u64;
            if direct > 0 || late || ready(t, &selector).await? {
                break;
            }
        }
        // Once one mutation committed directly, the fence admits the table. One fallback may have
        // met the publication itself, and then the race shows nothing.
        let stuck = direct == 0 && fell_back > 1;
        rest.push(AtRest {
            table: "fence",
            gateway,
            writes: if direct > 0 { direct } else { writes },
            schema_fallbacks: if stuck { fell_back } else { 0 },
            direct,
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
    judge_fences(cluster, t, &rest);
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
                Reply::Ok(data) => {
                    if data["build_id"] == json!(build) {
                        found.insert(*build);
                    }
                    t.log(|log| log.refused_judged.insert(*build));
                    break;
                }
                Reply::Definite(ErrorCode::NotFound, _, false) => {
                    t.log(|log| log.refused_judged.insert(*build));
                    break;
                }
                // Nothing else says that the record does not exist.
                Reply::Definite(..) | Reply::Retry { .. } => time::sleep(RETRY).await,
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

/// Mutations of a run, for the clauses.
struct Load<'a>(&'a [clauses::Attempt]);

impl Load<'_> {
    /// A mutation that committed and overlaps a window.
    fn commits(&self, from: u64, to: u64) -> bool {
        self.any(|a| a.answer == Answer::Committed && a.invoked <= to && from <= a.completed)
    }

    /// A mutation that committed wholly inside a window.
    fn commits_inside(&self, from: u64, to: u64) -> bool {
        self.any(|a| a.answer == Answer::Committed && from <= a.invoked && a.completed <= to)
    }

    /// A mutation that got its one outcome and overlaps a window.
    fn settles(&self, from: u64, to: u64) -> bool {
        self.any(|a| {
            matches!(a.answer, Answer::Committed | Answer::Ok)
                && a.invoked <= to
                && from <= a.completed
        })
    }

    /// A mutation in a window, whatever its answer.
    fn sends(&self, from: u64, to: u64) -> bool {
        self.any(|a| a.invoked <= to && from <= a.completed)
    }

    fn any(&self, matches: impl Fn(&clauses::Attempt) -> bool) -> bool {
        self.0
            .iter()
            .any(|attempt| attempt.mutation && matches(attempt))
    }
}

/// What ran of D07 and O16 around a split of `ranged` whose final reads and count were compared:
/// requests that overlap the time in which it published, and requests sent after it, under load.
/// A split that never published split nothing.
fn split_clauses(
    split: RangedSplit,
    load: &Load<'_>,
    load_to: u64,
    scans: &[(u64, u64)],
) -> Vec<&'static str> {
    let (from, Some(to)) = (split.pending, split.published) else {
        return Vec::new();
    };
    let mut clauses = Vec::new();
    if to < load_to {
        let after = to + 1;
        if load.settles(from, to)
            && load.any(|a| {
                after <= a.invoked
                    && a.completed <= load_to
                    && matches!(a.answer, Answer::Committed | Answer::Ok)
            })
        {
            clauses.push("D07:split");
        }
        if load.commits(from, to) && load.commits_inside(after, load_to) {
            clauses.push("O16:split-during-commits");
        }
    }
    if scans.iter().any(|(a, b)| *a <= to && from <= *b) {
        clauses.push("O16:split-during-scans");
    }
    clauses
}

/// The clauses of O12, O16, O21, O28, D07 and D11 that ran. A clause runs when its fault or
/// change happened, requests met it, and the observation its assertion reads was made.
#[allow(clippy::too_many_arguments)]
fn clauses_of(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    moving: Option<&moves::Move>,
    moved: Option<&Ctx>,
    seen: &Seen,
    move_retired: bool,
    scenarios: &mut Scenarios,
) {
    let faults = chaos.map_or(&[][..], |chaos| &chaos.log);
    let attempts = |ctx: &Ctx| ctx.observed.lock().unwrap().attempts.clone();
    let loads = BTreeMap::from([
        (Table::Main, attempts(&t.main)),
        (Table::Ranged, attempts(&t.ranged)),
        (Table::Victim, attempts(&t.victim)),
    ]);
    const LOSSES: [Action; 3] = [
        Action::CrashLeader,
        Action::PowerLeader,
        Action::DrainLeader,
    ];
    let no_quorum = cluster.no_quorum();
    t.log(|log| {
        for build in &log.builds {
            let judged = |id: &Uuid| log.refused_judged.contains(id);
            if build.kind == IndexKind::SameName && build.refused_id.as_ref().is_some_and(judged) {
                scenarios.insert("O12:refused-declaration");
            }
            if build.kind == IndexKind::UnknownColumn {
                continue;
            }
            let Some(window) = log.building.get(&build.name) else {
                continue;
            };
            let (from, to) = (window.from, window.to);
            let explained = log.explains.iter().any(|(table, a, b)| {
                *table == build.table
                    && topology::shown_building(&build.name, &log.statuses, *a, *b)
            });
            if explained {
                scenarios.insert("O12:incomplete-not-chosen");
            }
            // What the build left behind is in the rows of a scan that used it, and nowhere else.
            let scans = log.indexed.get(&build.table).map_or(&[][..], Vec::as_slice);
            if build.ready.is_some() && !scans.is_empty() {
                let load = Load(&loads[&build.table]);
                if load.commits_inside(from.1, to.1) && log.counted.contains(&build.table) {
                    scenarios.insert("O12:build-under-load");
                }
                let writes = log.writes.get(&build.table);
                let present = writes.into_iter().flatten().any(|write| {
                    write.ended == Ended::Acked
                        && write.value.is_some()
                        && from.0 < write.invoked
                        && write.completed < to.0
                });
                if present {
                    scenarios.insert("O12:present-once");
                }
                // Without a quorum no step of the build can commit.
                if no_quorum
                    .iter()
                    .any(|(a, b)| from.1 <= *a && a < b && *b <= to.1)
                {
                    scenarios.insert("O12:build-paused");
                }
            }
            let retired = log
                .statuses
                .iter()
                .find(|s| s.index == build.name && s.state == "retired")
                .map(|status| status.completed);
            let chosen = log.chosen.iter().any(|choice| choice.index == build.name);
            if let Some(retired) = retired
                && chosen
                && log
                    .explains
                    .iter()
                    .any(|(table, a, _)| *table == build.table && *a > retired)
                && log
                    .scans
                    .get(&build.table)
                    .into_iter()
                    .flatten()
                    .any(|scan| scan.invoked > retired)
            {
                scenarios.insert("O12:retired");
            }
        }
        let scans_of = |table: Table| -> Vec<(u64, u64)> {
            let of = log.scan_ms.iter().filter(|scan| scan.0 == table);
            of.map(|(_, from, to)| (*from, *to)).collect()
        };
        if let Some(split) = log.split
            && log.counted.contains(&Table::Ranged)
        {
            scenarios.extend(split_clauses(
                split,
                &Load(&loads[&Table::Ranged]),
                seen.load.1,
                &scans_of(Table::Ranged),
            ));
        }
        // A worker that holds a replica of every range stopped or started while a scan ran.
        let restarted = faults
            .iter()
            .filter(|fault| LOSSES.contains(&fault.action) && fault.happened());
        let mut edges = restarted
            .flat_map(|fault| fault.effects.iter().map(|effect| effect.at).chain(fault.to));
        if edges.any(|at| log.scan_ms.iter().any(|(_, a, b)| *a <= at && at <= *b)) {
            scenarios.insert("O16:restart-during-scans");
        }
        if let (Some(dropped), Some((from, to))) = (log.dropped, log.drop_ms)
            && log.recreated
        {
            let load = Load(&loads[&Table::Victim]);
            let judged = log.calls.iter().any(|call| call.judged(dropped));
            if load.commits_inside(0, from) && load.sends(from, to) && judged {
                scenarios.insert("D11:drop");
            }
        }
    });
    if let Some((from, to)) = moving.and_then(moves::Move::window)
        && let Some(moved) = moved
        && seen.moved_checked
        && move_retired
        && Load(&attempts(moved)).commits(from, to)
    {
        scenarios.insert("O16:move-during-commits");
    }
}

/// What a run did not judge, from its summary. A run without one judged nothing.
pub(crate) fn not_judged(summary: Option<&Value>) -> Vec<String> {
    let summary = summary.unwrap_or(&Value::Null);
    let mut unjudged: Vec<String> = RULES
        .iter()
        .filter(|rule| summary["judged"][**rule].as_u64().unwrap_or(0) == 0)
        .map(|rule| (*rule).to_owned())
        .collect();
    let tables = summary["fence_not_judged"].as_array();
    if tables.is_some_and(|tables| !tables.is_empty()) {
        unjudged.push(format!("{} for every table and race", topology::FENCE));
    }
    unjudged
}

/// How often each rule was judged in the run.
fn judgments(log: &Log) -> BTreeMap<&'static str, u64> {
    let mut covering = 0;
    for (table, scans) in &log.indexed {
        let writes = log.writes.get(table).map_or(&[][..], Vec::as_slice);
        for at in scans {
            let expected = |write: &&Write| {
                write.ended == Ended::Acked && write.value.is_some() && write.completed < *at
            };
            covering += u64::from(writes.iter().any(|write| expected(&write)));
        }
    }
    let incomplete = log.explains.iter().filter(|(table, a, b)| {
        log.builds.iter().any(|build| {
            build.table == *table && topology::shown_building(&build.name, &log.statuses, *a, *b)
        })
    });
    let dropped = log.calls.iter().filter(|call| {
        log.dropped
            .is_some_and(|acknowledged| call.judged(acknowledged))
    });
    let scans = log.scans.values().map(Vec::len).sum::<usize>() as u64;
    BTreeMap::from([
        (topology::COVERING, covering),
        (topology::EXACTLY_ONCE, covering),
        (topology::REFUSED_WRITE, scans),
        (topology::INCOMPLETE, incomplete.count() as u64),
        (topology::DROPPED, dropped.count() as u64),
        (topology::STALE_ROUTE, log.stale_judged),
        (topology::REFUSED_DDL, log.refused_judged.len() as u64),
        (topology::FENCE, log.fence_judged),
        (topology::ROUTES, log.routes_judged),
    ])
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
    let (mut main_attempts, main_resolved) = {
        let observed = t.main.observed.lock().unwrap();
        (observed.attempts.clone(), observed.resolved.clone())
    };
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
    main_attempts.sort_by_key(|attempt| (attempt.completed, attempt.invoked));
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
    let elections = cluster.elections();
    let judge = |attempts: &[clauses::Attempt], resolved: &BTreeMap<Uuid, clauses::Resolved>| {
        clauses::Evidence {
            faults: chaos.map_or(&[], |chaos| &chaos.log),
            attempts,
            resolved,
            reuse_same_key: same,
            reuse_other_key: other,
            elections: &elections,
            no_quorum: &no_quorum,
            moving: moving.and_then(moves::Move::window),
            deadline_ms: config.deadline().as_millis() as u64,
        }
        .evaluate()
    };
    // The faults aim at the leader of `main`, and the elections are of its range. Requests of
    // another table can break a rule, and they show no clause about that leader.
    let all = judge(&attempts, &resolved);
    let main = judge(&main_attempts, &main_resolved);
    let violations: BTreeSet<_> = all.violations.into_iter().chain(main.violations).collect();
    for (invariant, detail) in violations {
        cluster.checks.violate(&cluster.world, invariant, detail);
    }
    report.scenarios.extend(main.clauses);
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

#[allow(clippy::too_many_arguments)]
fn summarize(
    cluster: &Cluster,
    t: &Topo,
    chaos: Option<&Chaos>,
    moving: Option<&moves::Move>,
    split: SplitTiming,
    seen: &Seen,
    abandoned: bool,
    report: &mut direct::Report,
) {
    let skipped = |chaos: &Chaos| -> BTreeMap<&'static str, u32> {
        chaos.skipped.iter().map(|(a, n)| (a.name(), *n)).collect()
    };
    let (ddl_conflicts, remaining) = without_schema_conflicts(&t.main.history.lock().unwrap());
    report.summary = t.log(|log| {
        json!({
            "schema_conflicts": ddl_conflicts,
            "anomalies_without_schema_conflicts": remaining,
            "workload": Workload::Topology.name(),
            "profile": chaos.map(Chaos::profile_name),
            "planned": chaos.map(|chaos| chaos.counts(false)),
            "happened": chaos.map(|chaos| chaos.counts(true)),
            "aimed_and_hit_leader": chaos.map(Chaos::aimed),
            "skipped": chaos.map(skipped),
            "windows_overran_and_max_ms": chaos.map(Chaos::overran),
            "nemesis_split": chaos.map(Chaos::split_summary),
            "split": format!("{split:?}"),
            "ranged_split_ms": log.split.map(|split| json!([split.requested, split.pending, split.published])),
            "ranged_split_ids": log.split_ids,
            "move": moving.map(moves::Move::summary),
            "audited_writes": log.writes.values().map(Vec::len).sum::<usize>(),
            "scans": log.scans.values().map(Vec::len).sum::<usize>(),
            "explains": log.explains.len(),
            "explains_on_index": log.chosen.len(),
            "load_ms": seen.load,
            "chaos_writes": seen.work.writes,
            "chaos_reads": seen.work.reads,
            "chaos_scans": seen.work.scans,
            "scans_on_index": log.indexed.values().map(Vec::len).sum::<usize>(),
            "statuses": log.statuses.len(),
            "builds": log.builds.iter().map(|b| json!({
                "name": b.name, "table": b.table.name(), "kind": format!("{:?}", b.kind),
                "declared": b.declared, "ready": b.ready, "retired": b.retired,
                "refused": b.refused, "abandoned": b.abandoned, "stuck": b.stuck, "open": b.open,
                "unanswered": b.unanswered,
            })).collect::<Vec<_>>(),
            "victim_calls": log.calls.len(),
            "dropped": log.dropped.is_some(),
            "fence_races": log.races,
            "at_rest": log.rest,
            "fence_not_judged": log.fence_unjudged,
            "refused_not_judged": log.refused.len() - log.refused_judged.len(),
            // How often each rule of `RULES` was judged. A rule with 0 did not run in this seed.
            "judged": judgments(log),
            "abandoned": abandoned,
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

    fn attempt(mutation: bool, window: (u64, u64), answer: Answer) -> clauses::Attempt {
        clauses::Attempt {
            id: Uuid::nil(),
            mutation,
            fresh: true,
            gateway: GATEWAYS[0],
            invoked: window.0,
            completed: window.1,
            answer,
        }
    }

    /// Review 136, M9: 14 of 24 seeds answered no scan under load.
    #[test]
    fn work_under_load_needs_a_write_a_read_and_a_scan() {
        let load = (1_000, 2_000);
        let write = |window| attempt(true, window, Answer::Committed);
        let read = attempt(false, (1_100, 1_200), Answer::Ok);
        let work = |attempts: &[clauses::Attempt], scans: &[(u64, u64)]| {
            Work::of(attempts, scans, load).missing().len()
        };
        let all = [write((1_100, 1_200)), read.clone()];
        assert_eq!(work(&all, &[(1_500, 1_600)]), 0);
        assert_eq!(work(&all, &[]), 1);
        assert_eq!(work(&all, &[(1_500, 2_100)]), 1, "answered after the load");
        assert_eq!(work(&all, &[(900, 1_100)]), 1, "sent before it");
        assert_eq!(work(&[write((1_900, 2_100)), read], &[(1_500, 1_600)]), 1);
        let unknown = attempt(true, (1_100, 1_200), Answer::Unknown);
        assert_eq!(work(&[unknown], &[]), 3);
    }

    /// Review 136, M2: 20 of 23 `O16:split-during-commits` tags were splits that never published.
    #[test]
    fn a_split_that_did_not_publish_or_met_no_load_is_no_clause() {
        let split = |published| RangedSplit {
            requested: 100,
            pending: 150,
            published,
        };
        let commit = |window| attempt(true, window, Answer::Committed);
        let met = [commit((140, 160)), commit((220, 230))];
        let clauses = |split, attempts: &[clauses::Attempt], scans: &[(u64, u64)]| {
            split_clauses(split, &Load(attempts), 500, scans)
        };
        let both = ["D07:split", "O16:split-during-commits"];
        assert_eq!(clauses(split(Some(200)), &met, &[]), both);
        let scanned = [
            "D07:split",
            "O16:split-during-commits",
            "O16:split-during-scans",
        ];
        assert_eq!(clauses(split(Some(200)), &met, &[(190, 260)]), scanned);
        assert!(clauses(split(None), &met, &[(190, 260)]).is_empty());
        // Commits only before it surely had not published, or none after it.
        assert!(clauses(split(Some(200)), &[commit((110, 120)), met[1].clone()], &[]).is_empty());
        assert!(clauses(split(Some(200)), &met[..1], &[]).is_empty());
        // Published after the load, or a scan that does not overlap it.
        assert!(clauses(split(Some(600)), &met, &[]).is_empty());
        assert!(clauses(split(Some(200)), &[], &[(201, 260)]).is_empty());
        // No commit, only refused or unanswered mutations.
        let unknown = [
            attempt(true, (140, 160), Answer::Unknown),
            attempt(true, (220, 230), Answer::Unknown),
        ];
        assert!(clauses(split(Some(200)), &unknown, &[]).is_empty());
    }

    #[test]
    fn plan_counts_are_of_one_table_and_of_collects() {
        let shape = |table: &str, operation: &str, executions: u64, covering: u64| {
            json!({"table_id": table, "operation": operation, "executions": executions,
                "plans": {"covering_index": covering, "primary_scan": executions - covering}})
        };
        let shapes = [
            shape("a", "collect", 3, 1),
            shape("a", "collect", 2, 2),
            shape("a", "count", 5, 5),
            shape("b", "collect", 7, 7),
        ];
        let report = json!({"untracked_executions": 0, "shapes": shapes});
        assert_eq!(plans_of(&report, "a"), Ok((5, 3)));
        // Beyond its capacity the report misses executions.
        let full = json!({"untracked_executions": 1, "shapes": shapes});
        assert!(plans_of(&full, "a").is_err());
        assert!(plans_of(&json!({}), "a").is_err());
    }

    /// Review 136, M5: declaration 95f75d2b of seed 24 took the guard and never settled, and a
    /// later refusal under its ID closed it.
    #[test]
    fn an_unanswered_declaration_keeps_its_table_open() {
        let mut build = Build {
            table: Table::Main,
            name: "main_value".into(),
            kind: IndexKind::Valid,
            declared: None,
            ready: None,
            retired: None,
            refused: true,
            abandoned: false,
            stuck: None,
            open: false,
            unanswered: BTreeSet::from([Uuid::from_u128(1)]),
            refused_id: None,
        };
        assert!(build.is_open());
        build.unanswered.clear();
        assert!(!build.is_open());
        build.open = true;
        assert!(build.is_open());
    }

    #[test]
    fn a_rule_is_not_judged_without_a_count() {
        assert_eq!(not_judged(None).len(), RULES.len());
        let judged = |count: u64| -> BTreeMap<&str, u64> {
            RULES.into_iter().map(|rule| (rule, count)).collect()
        };
        assert!(not_judged(Some(&json!({"judged": judged(1)}))).is_empty());
        assert_eq!(not_judged(Some(&json!({"judged": judged(0)}))), RULES);
        let summary = json!({"judged": judged(1), "fence_not_judged": ["main: open"]});
        assert_eq!(not_judged(Some(&summary)).len(), 1);
        assert_eq!(judgments(&Log::default()).len(), RULES.len());
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
