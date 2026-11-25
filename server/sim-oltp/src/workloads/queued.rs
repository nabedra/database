//! The `queued` workload (design 3.6, phase 11): `POST /v1/requests` and receipts, cross-range
//! batches and synchronous operations, with the legacy and the scoped queue protocol and with
//! `direct_oltp` on and off.
//!
//! The nemesis of the phase 10 workloads runs next to queue faults of its own ([`Fault`]): a
//! replica frozen before its installation acknowledgment while the submitting coordinator dies,
//! API restarts, a rolling restart of every worker, and at the end a full stop, or workers
//! stopped one by one down to one holder. Clients close their sockets early, race one ID
//! through both APIs, reuse IDs with other bodies, cross the direct-path boundary, and poll
//! receipts past their expiry. Lost terminal responses come from the shared `lost-reply` and
//! `lost-http-reply` actions.
//!
//! The rules are in [`crate::invariants::queued`]; this module only records what happened. The
//! history goes through `txn::check`, per-key linearizability and `outcome::check`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use nabedra_check::{
    Gen,
    checker::{Anomaly, Verdict, linearizable, outcome, txn},
    history::{Event, EventType, NEMESIS},
};
use nabedra_protocol::protocol::{ErrorCode, Outcome as Applied, ProtocolError};
use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use nabedra_sim::{Profile, rng::stream_seed};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{
    Workload,
    clients::Ctx,
    direct,
    queued_clauses::{Facts, Mark, Stop, Work},
};
use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, Bootstrap, CLIENTS, GATEWAYS, QueueSettings, WORKERS},
    client::{operation, request},
    config::Config,
    disk::DiskCut,
    history::{Completion, History, Operation},
    invariants::{
        elapsed_ms,
        queued::{
            Answer, Body, Fresh, Frozen, Holder, Log, Lookup, RULES, Read, Retained, Route, Run,
            Seen, Submission, Terminal, clock_slack,
        },
    },
    nemesis::{
        Action, NetFaults, Selection,
        apply::{COLD_LIMIT, Chaos, start_gateway},
        window,
    },
    node::Cluster,
};

const SUBMIT: &str = "/v1/requests";
const OPERATIONS: &str = "/v1/operations";
const TICK: Duration = Duration::from_millis(50);
/// A receipt lookup has its own bound on the server; this is longer.
const LOOKUP_LIMIT: Duration = Duration::from_secs(10);
/// Keys below and from this boundary are in two ranges, so a batch over both is queued.
const BOUNDARY: i64 = 1_000_000_000;
/// Hot keys per range, for conflicting single-key operations.
const HOT: i64 = 5;
/// The first key of a batch, in each range. Every batch inserts keys of its own.
const BATCH_BASE: i64 = 10_000;
/// Keys that only inserts, deletes and gets use. The transaction checker cannot express a
/// delete, so it skips them; the per-key check covers them.
const DELETED_BASE: i64 = 1_000;
const DELETED: i64 = 3;
/// How often a client retries a refusal that says the request was not applied.
const REFUSALS: u32 = 6;
/// How many violations of one rule a run lists.
const REPORTED: usize = 3;
/// Retentions of at least this long do not expire within a run.
const LONG_RETENTION_MS: u64 = 600_000;
/// Requests that a restart of processes meets.
const BACKLOG: i64 = 40;
const ACCEPT_WAIT: Duration = Duration::from_secs(1);
/// How many receipts are followed from a fault on.
const WATCHED: usize = 8;
/// One frozen worker stays so for at least this long, so that the rest can accept.
const ALONE: Duration = Duration::from_secs(2);
/// How many expired records may stay when new ones are counted.
const STUCK: u64 = 2;
const THAW_LIMIT: Duration = Duration::from_secs(5);
const READY_LIMIT: Duration = Duration::from_secs(2);
const SPLIT_LIMIT: Duration = Duration::from_secs(60);
const SCOPED_LIMIT: Duration = Duration::from_secs(30);

/// `NABEDRA_SIM_QUEUED_MODE`: force the protocol and the direct path, for diagnosis and
/// searches, as `legacy-on`, `legacy-off`, `scoped-on` or `scoped-off`.
pub(crate) const MODE_ENV: &str = "NABEDRA_SIM_QUEUED_MODE";
/// `NABEDRA_SIM_QUEUED_DEFECT`: a seeded defect in the harness's own observations.
pub(crate) const DEFECT_ENV: &str = "NABEDRA_SIM_QUEUED_DEFECT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Mode {
    scoped: bool,
    direct: bool,
    /// How long results are retained. Short retentions expire within the run.
    retention_ms: u64,
}

impl Mode {
    /// Drawn from a stream of its own, so it does not correlate with the rotating profile.
    pub(crate) fn of(seed: u64) -> Result<Self, String> {
        let word = stream_seed(seed, "oltp-queued-mode");
        let mut mode = Self {
            scoped: word & 1 == 1,
            direct: word & 2 == 2,
            retention_ms: [4_000, 8_000, LONG_RETENTION_MS][(word >> 2) as usize % 3],
        };
        if let Ok(forced) = std::env::var(MODE_ENV)
            && !forced.is_empty()
        {
            (mode.scoped, mode.direct) = match forced.as_str() {
                "legacy-on" => (false, true),
                "legacy-off" => (false, false),
                "scoped-on" => (true, true),
                "scoped-off" => (true, false),
                other => return Err(format!("unknown {MODE_ENV} {other:?}")),
            };
        }
        Ok(mode)
    }

    fn name(self) -> &'static str {
        match (self.scoped, self.direct) {
            (false, true) => "legacy-on",
            (false, false) => "legacy-off",
            (true, true) => "scoped-on",
            (true, false) => "scoped-off",
        }
    }

    fn settings(self) -> QueueSettings {
        QueueSettings {
            direct_oltp: self.direct,
            scoped_oltp: self.scoped,
            result_retention_ms: self.retention_ms,
        }
    }
}

/// Seeded defects in what the harness observes. Each models a product fault end to end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Defect {
    /// Report one accepted receipt missing.
    LoseAccepted,
    /// After a full stop, submit one abandoned mutation again behind the client's back.
    ReplayAbandoned,
    /// Execute one settled mutation a second time.
    ExecuteTwice,
    /// Report one `202` while two workers are frozen.
    EarlyAccept,
    /// Change one terminal receipt.
    ConflictingTerminal,
}

impl Defect {
    fn from_env() -> Result<Option<Self>, String> {
        Ok(Some(match std::env::var(DEFECT_ENV).as_deref() {
            Err(_) | Ok("" | "none") => return Ok(None),
            Ok("lose-accepted") => Self::LoseAccepted,
            Ok("replay-abandoned") => Self::ReplayAbandoned,
            Ok("execute-twice") => Self::ExecuteTwice,
            Ok("early-accept") => Self::EarlyAccept,
            Ok("conflicting-terminal") => Self::ConflictingTerminal,
            Ok(other) => return Err(format!("unknown {DEFECT_ENV} {other:?}")),
        }))
    }
}

/// Whether `seed` runs with `direct_oltp`, for its reported configuration. A bad mode is
/// reported when the cluster is configured.
pub(crate) fn direct(seed: u64) -> bool {
    Mode::of(seed).is_ok_and(|mode| mode.direct)
}

/// Prepare the cluster's configuration before any process starts.
pub(crate) fn configure(bootstrap: &mut Bootstrap, seed: u64) -> Result<(), String> {
    Defect::from_env()?;
    bootstrap.set_queue(Mode::of(seed)?.settings());
    Ok(())
}

/// One request: its operation, its history input and its identity.
#[derive(Clone, Debug)]
struct Req {
    name: &'static str,
    payload: Value,
    input: Value,
    body: Body,
}

impl Req {
    fn new(
        id: Uuid,
        name: &'static str,
        payload: Value,
        input: Value,
        keys: &[i64],
        writes: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            body: Body {
                id,
                text: format!("{name} {payload}"),
                anchor: keys.iter().copied().min(),
                writes,
                mutation: !matches!(name, "table.get" | "query.collect"),
            },
            name,
            payload,
            input,
        }
    }

    fn get(table: &str, id: Uuid, key: i64) -> Self {
        let payload = json!({"table_id": table, "key": key});
        Self::new(id, "table.get", payload, Value::Null, &[key], Vec::new())
    }

    fn row(name: &'static str, table: &str, id: Uuid, key: i64, value: i64) -> Self {
        let row = json!({"id": key, "value": value});
        let payload = if name == "table.update" {
            json!({"table_id": table, "key": key, "values": {"value": value}})
        } else {
            json!({"table_id": table, "row": row})
        };
        Self::new(id, name, payload, row, &[key], vec![(key, value)])
    }

    fn delete(table: &str, id: Uuid, key: i64) -> Self {
        let payload = json!({"table_id": table, "key": key});
        Self::new(
            id,
            "table.delete",
            payload.clone(),
            payload,
            &[key],
            Vec::new(),
        )
    }

    fn insert_many(table: &str, id: Uuid, rows: &[(i64, i64)]) -> Self {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
        let json_rows: Vec<Value> = rows
            .iter()
            .map(|(key, value)| json!({"id": key, "value": value}))
            .collect();
        let keys: Vec<i64> = rows.iter().map(|(key, _)| *key).collect();
        Self::new(
            id,
            "table.insert_many",
            json!({"table_id": table, "rows": json_rows}),
            Value::Array(json_rows),
            &keys,
            rows,
        )
    }

    fn collect(table: &str, id: Uuid) -> Self {
        Self::new(
            id,
            "query.collect",
            json!({"table_id": table, "query": {"select": null, "filter": null, "order_by": null, "limit": null}}),
            Value::Null,
            &[],
            Vec::new(),
        )
    }

    /// Whether the request is certainly queued: cross-range batches are never direct.
    fn route(&self, mode: Mode, asynchronous: bool) -> Route {
        let both = self.body.writes.iter().any(|(key, _)| *key < BOUNDARY)
            && self.body.writes.iter().any(|(key, _)| *key >= BOUNDARY);
        if asynchronous || !mode.direct || both {
            Route::Queued
        } else {
            Route::MaybeDirect
        }
    }
}

/// An accepted request whose receipt the workload still follows.
#[derive(Clone, Debug)]
struct Open {
    run: Uuid,
    req: Req,
}

#[derive(Default)]
struct Shared {
    log: Log,
    open: Vec<Open>,
    counts: BTreeMap<&'static str, u64>,
    /// Requests whose submission never got a definite answer.
    undecided: u64,
    defect: Option<Defect>,
    /// Identities whose read already has an observation: its first answer.
    read_ids: BTreeSet<Uuid>,
    /// Bursts so far. Each has values and batch keys of its own.
    bursts: i64,
    /// When the fault loop last ran, before any action of that turn.
    last_turn: u64,
    /// The queue faults that happened.
    marks: Vec<(Mark, u64)>,
    /// Windows in which clients were sending: `Progress` judges each of them.
    windows: Vec<(&'static str, u64, u64)>,
    /// The finale's full stop.
    stop: Option<Stop>,
    /// Every gateway start that failed (lead decision 18).
    failed_starts: Vec<String>,
    /// Comparisons that could not be made, and why.
    skipped: Vec<String>,
    /// The last receipt answer per identity, for the failure messages.
    last: BTreeMap<Uuid, String>,
    /// `POST /v1/requests` answers outside the contract, after the first.
    outside: u64,
    /// Abandoned mutations, for the seeded replay.
    abandoned: Vec<Req>,
}

/// What the clients and the fault schedule share.
#[derive(Clone)]
struct Q {
    ctx: Ctx,
    mode: Mode,
    shared: Arc<Mutex<Shared>>,
}

impl Q {
    fn now(&self) -> u64 {
        elapsed_ms(&self.ctx.world)
    }

    fn count(&self, name: &'static str) {
        *self.shared.lock().unwrap().counts.entry(name).or_default() += 1;
    }

    /// Note a fault that takes effect now.
    fn mark(&self, mark: Mark) {
        let now = self.now();
        self.shared.lock().unwrap().marks.push((mark, now));
    }

    /// Note a comparison that could not be made.
    fn skip(&self, cluster: &Cluster, why: String) {
        cluster.record(format!("queued: {why}"));
        self.shared.lock().unwrap().skipped.push(why);
    }

    fn history(&self) -> std::sync::MutexGuard<'_, History> {
        self.ctx.history.lock().unwrap()
    }

    /// Consume the seeded defect if it is `defect`.
    fn take_defect(&self, defect: Defect) -> bool {
        let mut shared = self.shared.lock().unwrap();
        let hit = shared.defect == Some(defect);
        if hit {
            shared.defect = None;
        }
        hit
    }

    fn peek_defect(&self, defect: Defect) -> bool {
        self.shared.lock().unwrap().defect == Some(defect)
    }

    fn stopping(&self) -> bool {
        self.ctx.stopping.load(Ordering::Acquire)
    }

    fn out_of_time(&self) -> bool {
        self.now() >= self.ctx.converge_by.load(Ordering::Acquire)
    }
}

async fn fresh_id(node: u64) -> Uuid {
    in_node(node, async { uuid_v4() }).await
}

/// Record one attempt in the history. Every attempt is a process of its own, because a client
/// can have several requests open.
fn record(q: &Q, gateway: u64, req: &Req, completion: Option<Completion>) -> Option<HistoryOp> {
    let mut history = q.history();
    let client = history.client();
    let key = req.body.anchor.map(|key| key.to_string());
    let attempt = history.invoke(
        &client,
        &q.ctx.world,
        gateway,
        Operation {
            table: Some(&q.ctx.table),
            name: req.name,
            key: key.as_deref(),
            request_id: req.body.id,
            value: req.input.clone(),
        },
    );
    let mut op = HistoryOp { client, attempt };
    match completion {
        Some(completion) => {
            history.complete(&mut op.client, &q.ctx.world, op.attempt, completion);
            None
        }
        None => Some(op),
    }
}

struct HistoryOp {
    client: crate::history::Client,
    attempt: crate::history::Attempt,
}

impl HistoryOp {
    fn complete(mut self, q: &Q, completion: Completion) {
        q.history()
            .complete(&mut self.client, &q.ctx.world, self.attempt, completion);
    }
}

fn classify(error: &ProtocolError) -> Answer {
    match error.code {
        ErrorCode::NotFound if error.message.contains("abandoned") => Answer::Abandoned,
        ErrorCode::NotFound => Answer::Missing,
        ErrorCode::Conflict => Answer::Terminal(failed(error)),
        _ if error.outcome == Applied::NotApplied => Answer::Refused(code(error)),
        _ => Answer::Unknown,
    }
}

fn failed(error: &ProtocolError) -> Terminal {
    Terminal::Failed {
        code: code(error),
        unknown: error.outcome == Applied::Unknown,
    }
}

/// Whether an error of [`request`] is a connection that ended while it waited for the reply.
fn cut(error: &str) -> bool {
    error.starts_with("HTTP reply:") || error.starts_with("HTTP response body:")
}

fn code(error: &ProtocolError) -> String {
    serde_json::to_value(error.code)
        .ok()
        .and_then(|code| code.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Record the values a get or a whole-table query returned. Only the first answer counts: a
/// later receipt or replay repeats an old result. The read happened between the first
/// submission and that answer.
fn observe_rows(q: &Q, req: &Req, result: &Value, invoked: u64) {
    let seen: Vec<(i64, Option<i64>)> = match (req.name, req.body.anchor) {
        ("table.get", Some(key)) => vec![(key, result["value"].as_i64())],
        ("query.collect", _) => rows(result)
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect(),
        _ => return,
    };
    let mut shared = q.shared.lock().unwrap();
    if !shared.read_ids.insert(req.body.id) {
        return;
    }
    let first = shared
        .log
        .submissions
        .iter()
        .filter(|submission| submission.body.id == req.body.id)
        .map(|submission| submission.invoked)
        .min()
        .unwrap_or(invoked)
        .min(invoked);
    let completed = q.now();
    shared
        .log
        .reads
        .extend(seen.into_iter().map(|(key, value)| Read {
            key,
            value,
            invoked: first,
            completed,
        }));
}

/// Send one attempt. `asynchronous` submits through `POST /v1/requests`.
async fn send(
    q: &Q,
    node: u64,
    gateway: u64,
    req: &Req,
    asynchronous: bool,
    fresh: bool,
    limit: Duration,
) -> Answer {
    attempt(q, node, gateway, req, asynchronous, fresh, limit, false).await
}

/// [`send`], or with `close` a client that closes its socket after `limit`.
#[allow(clippy::too_many_arguments)]
async fn attempt(
    q: &Q,
    node: u64,
    gateway: u64,
    req: &Req,
    asynchronous: bool,
    fresh: bool,
    limit: Duration,
    close: bool,
) -> Answer {
    let op = record(q, gateway, req, None).expect("open history attempt");
    let invoked = q.now();
    let path = if asynchronous { SUBMIT } else { OPERATIONS };
    let reply = request(
        node,
        gateway,
        path,
        &q.ctx.token,
        Some(operation(req.body.id, req.name, req.payload.clone())),
        limit,
    )
    .await;
    let mut envelope = None;
    let mut replayed = false;
    let lost = matches!(&reply, Err(error) if cut(error));
    let deadline = matches!(&reply, Err(error) if error.ends_with("deadline"));
    let (answer, completion) = match reply {
        Err(error) => (Answer::Unknown, Completion::Unknown(error)),
        Ok(reply) if asynchronous && reply.status == 202 => match accepted(&reply.body, req) {
            Ok(run) => (
                Answer::Accepted(run),
                Completion::Unknown(format!("accepted in run {run}")),
            ),
            Err(error) => {
                envelope = Some(error.clone());
                (Answer::Unknown, Completion::Unknown(error))
            }
        },
        Ok(reply) if asynchronous && reply.status == 200 => {
            // Plan: a submission answers "202: {request_id, run_id, state: "queued"}". The
            // answer is still a definite success, and the direct path committed it.
            replayed = true;
            match reply.completion(req.body.id) {
                Ok(Completion::Ok(value)) => {
                    envelope = Some(format!("POST {SUBMIT} answered 200: {value}"));
                    (
                        Answer::Terminal(Terminal::Succeeded(value.clone())),
                        Completion::Ok(value),
                    )
                }
                Ok(_) | Err(_) => {
                    envelope = Some(format!("POST {SUBMIT} answered an invalid 200"));
                    (
                        Answer::Unknown,
                        Completion::Unknown("an invalid 200".into()),
                    )
                }
            }
        }
        Ok(reply) => match reply.completion(req.body.id) {
            Ok(Completion::Ok(value)) => (
                Answer::Terminal(Terminal::Succeeded(value.clone())),
                Completion::Ok(value),
            ),
            Ok(Completion::Refused(error)) => (classify(&error), Completion::Refused(error)),
            Ok(Completion::Unknown(error)) => (Answer::Unknown, Completion::Unknown(error)),
            Err(error) => {
                envelope = Some(error.clone());
                (Answer::Unknown, Completion::Unknown(error))
            }
        },
    };
    if let Some(error) = envelope {
        // One report per run: the violation list is bounded, and this must not fill it.
        let first = {
            let mut shared = q.shared.lock().unwrap();
            shared.outside += 1;
            shared.outside == 1
        };
        if first {
            q.ctx.checks.violate(
                &q.ctx.world,
                "SubmitAnswer",
                format!("{} {} through n{gateway}: {error}", req.name, req.body.id),
            );
        }
    }
    if let Answer::Terminal(Terminal::Succeeded(value)) = &answer {
        observe_rows(q, req, value, invoked);
    }
    op.complete(q, completion);
    let mut shared = q.shared.lock().unwrap();
    if let Answer::Accepted(run) = answer {
        *shared.counts.entry("accepted").or_default() += 1;
        shared.open.push(Open {
            run,
            req: req.clone(),
        });
    }
    if answer == Answer::Abandoned && req.body.mutation {
        shared.abandoned.push(req.clone());
    }
    shared.log.submissions.push(Submission {
        body: req.body.clone(),
        route: if replayed {
            Route::MaybeDirect
        } else {
            req.route(q.mode, asynchronous)
        },
        asynchronous,
        gateway,
        cut: lost,
        closed: close && deadline,
        fresh,
        invoked,
        completed: q.now(),
        answer: answer.clone(),
    });
    answer
}

/// The exact acceptance receipt of the public contract.
fn accepted(body: &Value, req: &Req) -> Result<Uuid, String> {
    let fields = body.as_object().map(|fields| fields.len());
    if fields != Some(3)
        || body["request_id"] != json!(req.body.id)
        || body["state"] != json!("queued")
    {
        return Err(format!("invalid acceptance receipt: {body}"));
    }
    body["run_id"]
        .as_str()
        .and_then(|run| Uuid::parse_str(run).ok())
        .ok_or_else(|| format!("acceptance receipt without a run: {body}"))
}

/// Look up one receipt and record what it says.
async fn lookup(q: &Q, node: u64, gateway: u64, run: Uuid, req: &Req) -> Seen {
    let invoked = q.now();
    let path = format!("{SUBMIT}/{run}/{}", req.body.id);
    let reply = request(node, gateway, &path, &q.ctx.token, None, LOOKUP_LIMIT).await;
    let lost = matches!(&reply, Err(error) if cut(error));
    let mut envelope = None;
    let mut failed = None;
    let mut why = String::new();
    let mut seen = match reply {
        Err(error) => {
            why = error;
            Seen::Unknown
        }
        Ok(reply) if reply.status == 200 => match receipt(&reply.body, run, req.body.id) {
            Ok((seen, error)) => {
                failed = error;
                seen
            }
            Err(error) => {
                envelope = Some(error);
                Seen::Unknown
            }
        },
        Ok(reply) => match reply.completion(req.body.id) {
            Ok(Completion::Refused(error)) => {
                why = format!("{:?} {}", error.code, error.message);
                match classify(&error) {
                    Answer::Abandoned => Seen::Abandoned,
                    Answer::Missing => Seen::Missing,
                    _ => Seen::Unknown,
                }
            }
            _ => {
                envelope = Some("invalid receipt error".into());
                Seen::Unknown
            }
        },
    };
    if let Some(error) = envelope {
        q.ctx.checks.violate(
            &q.ctx.world,
            "HttpEnvelope",
            format!("receipt {run}/{} through n{gateway}: {error}", req.body.id),
        );
    }
    seen = seeded(q, req, seen);
    q.shared
        .lock()
        .unwrap()
        .last
        .insert(req.body.id, format!("{seen:?} {why}"));
    match &seen {
        Seen::Terminal(Terminal::Succeeded(value)) => {
            record(q, gateway, req, Some(Completion::Ok(value.clone())));
            observe_rows(q, req, value, invoked);
        }
        Seen::Terminal(Terminal::Failed { .. }) => {
            if let Some(error) = failed {
                record(q, gateway, req, Some(Completion::Refused(error)));
            }
        }
        _ => {}
    }
    if seen == Seen::Abandoned && req.body.mutation {
        q.shared.lock().unwrap().abandoned.push(req.clone());
    }
    q.shared.lock().unwrap().log.lookups.push(Lookup {
        run,
        id: req.body.id,
        cut: lost,
        invoked,
        completed: q.now(),
        seen: seen.clone(),
    });
    seen
}

/// Seeded defects that change one receipt.
fn seeded(q: &Q, req: &Req, seen: Seen) -> Seen {
    match &seen {
        Seen::Pending | Seen::Terminal(_) if q.take_defect(Defect::LoseAccepted) => Seen::Missing,
        Seen::Terminal(Terminal::Succeeded(value)) if req.body.mutation => {
            let known =
                q.shared.lock().unwrap().log.lookups.iter().any(|lookup| {
                    lookup.id == req.body.id && matches!(lookup.seen, Seen::Terminal(_))
                });
            if known && q.take_defect(Defect::ConflictingTerminal) {
                let changed = json!({"seeded": value});
                return Seen::Terminal(Terminal::Succeeded(changed));
            }
            seen
        }
        _ => seen,
    }
}

/// Parse a receipt and the error of a failed one. Anything outside the public contract is an
/// error.
fn receipt(body: &Value, run: Uuid, id: Uuid) -> Result<(Seen, Option<ProtocolError>), String> {
    let invalid = |why: &str| Err(format!("invalid receipt ({why}): {body}"));
    let Some(fields) = body.as_object() else {
        return invalid("not an object");
    };
    let names: BTreeSet<&str> = fields.keys().map(String::as_str).collect();
    if names != BTreeSet::from(["error", "request_id", "result", "run_id", "state"]) {
        return invalid("fields");
    }
    if body["request_id"] != json!(id) || body["run_id"] != json!(run) {
        return invalid("identity");
    }
    let (result, error) = (&body["result"], &body["error"]);
    match body["state"].as_str() {
        Some("queued" | "running" | "committing" | "reconciling") => {
            if !result.is_null() || !error.is_null() {
                return invalid("pending with an outcome");
            }
            Ok((Seen::Pending, None))
        }
        Some("succeeded") if error.is_null() => {
            Ok((Seen::Terminal(Terminal::Succeeded(result.clone())), None))
        }
        Some("failed") => match serde_json::from_value::<ProtocolError>(error.clone()) {
            Ok(error) => Ok((Seen::Terminal(failed(&error)), Some(error))),
            Err(_) => invalid("failed without an error"),
        },
        Some("abandoned") if result.is_null() && error.is_null() => Ok((Seen::Abandoned, None)),
        _ => invalid("state"),
    }
}

async fn pause(attempt: u32) {
    time::sleep(Duration::from_millis(60 * u64::from(attempt.min(8)))).await;
}

/// Submit until the identity has a definite answer, with the same ID and body, alternating
/// gateways.
async fn drive(
    q: &Q,
    node: u64,
    first: usize,
    req: &Req,
    asynchronous: bool,
    fresh: bool,
    limit: Duration,
) -> Answer {
    let mut attempt = 0;
    loop {
        let gateway = GATEWAYS[(first + attempt as usize) % GATEWAYS.len()];
        let answer = send(
            q,
            node,
            gateway,
            req,
            asynchronous,
            fresh && attempt == 0,
            limit,
        )
        .await;
        match &answer {
            // A refusal that a client retries, a few times.
            Answer::Refused(code)
                if attempt < REFUSALS
                    && matches!(code.as_str(), "overloaded" | "storage_unavailable") => {}
            Answer::Unknown => {}
            _ => return answer,
        }
        attempt += 1;
        if q.out_of_time() {
            if answer == Answer::Unknown {
                q.shared.lock().unwrap().undecided += 1;
            }
            return answer;
        }
        pause(attempt).await;
    }
}

/// Poll one open receipt until it is definite or the budget ends.
async fn poll(q: &Q, node: u64, first: usize, run: Uuid, req: &Req, polls: u32) -> Seen {
    let mut attempt = 0;
    loop {
        let gateway = GATEWAYS[(first + attempt as usize) % GATEWAYS.len()];
        let seen = lookup(q, node, gateway, run, req).await;
        if !matches!(seen, Seen::Pending | Seen::Unknown) {
            // A settled receipt reads the same through the other gateway.
            let other = GATEWAYS[(first + attempt as usize + 1) % GATEWAYS.len()];
            lookup(q, node, other, run, req).await;
            return seen;
        }
        attempt += 1;
        if attempt >= polls || q.out_of_time() {
            return seen;
        }
        pause(attempt).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Get,
    Insert,
    Upsert,
    Update,
    Delete,
    Batch,
    Collect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Style {
    /// `POST /v1/requests`, then poll.
    Submit,
    /// `POST /v1/operations`.
    Sync,
    /// Several submissions, polled later.
    Pile,
    /// The same ID and body through both APIs at once.
    Race,
    /// Submit, then send the same ID and body to `POST /v1/operations`, or the reverse.
    Cross,
    /// An earlier ID with another body.
    Reuse,
    /// Close the client's socket after a few milliseconds, then retry through the other API.
    Close,
}

#[derive(Clone, Debug)]
struct Planned {
    kind: Kind,
    style: Style,
    key: i64,
    rows: i64,
    gateway: usize,
    limit: Duration,
    think: Duration,
    polls: u32,
    other_key: bool,
}

fn plan(g: &mut Gen) -> Vec<Vec<Planned>> {
    let clients = 2 + g.below(4);
    (0..clients)
        .map(|_| {
            (0..40)
                .map(|_| {
                    let kind = match g.below(100) {
                        0..18 => Kind::Get,
                        18..34 => Kind::Insert,
                        34..52 => Kind::Upsert,
                        52..68 => Kind::Update,
                        68..76 => Kind::Delete,
                        76..92 => Kind::Batch,
                        _ => Kind::Collect,
                    };
                    let style = match g.below(100) {
                        0..30 => Style::Submit,
                        30..50 => Style::Sync,
                        50..62 => Style::Pile,
                        62..72 => Style::Race,
                        72..82 => Style::Cross,
                        82..90 => Style::Reuse,
                        _ => Style::Close,
                    };
                    let low = 1 + g.below(HOT as u64) as i64;
                    let hot = if g.bool() { low } else { BOUNDARY + low };
                    let key = match kind {
                        Kind::Delete => DELETED_BASE + g.below(DELETED as u64) as i64,
                        Kind::Insert if g.bool() => DELETED_BASE + g.below(DELETED as u64) as i64,
                        _ => hot,
                    };
                    Planned {
                        kind,
                        style,
                        key,
                        rows: 2 + g.below(3) as i64,
                        gateway: g.below(2) as usize,
                        limit: Duration::from_millis(300 + g.below(4_700)),
                        think: Duration::from_millis(g.below(200)),
                        polls: 1 + g.below(12) as u32,
                        other_key: g.bool(),
                    }
                })
                .collect()
        })
        .collect()
}

fn build(q: &Q, id: Uuid, planned: &Planned, value: i64) -> Req {
    let table = &q.ctx.table;
    match planned.kind {
        Kind::Get => Req::get(table, id, planned.key),
        Kind::Insert => Req::row("table.insert", table, id, planned.key, value),
        Kind::Upsert => Req::row("table.upsert", table, id, planned.key, value),
        Kind::Update => Req::row("table.update", table, id, planned.key, value),
        Kind::Delete => Req::delete(table, id, planned.key),
        Kind::Collect => Req::collect(table, id),
        Kind::Batch => {
            // Keys of this batch alone, in both ranges.
            let rows: Vec<(i64, i64)> = (0..planned.rows)
                .map(|row| {
                    let side = if row % 2 == 0 { 0 } else { BOUNDARY };
                    (side + BATCH_BASE + value + row, value + row)
                })
                .collect();
            Req::insert_many(table, id, &rows)
        }
    }
}

/// Run one client until the chaos window closes.
async fn run_client(q: Q, index: usize, program: Vec<Planned>) {
    let node = CLIENTS[index % CLIENTS.len()];
    let base = (index as i64 + 1) * 20_000;
    let mut sent: Vec<Req> = Vec::new();
    for (step, planned) in program.iter().cycle().enumerate() {
        if q.stopping() {
            return;
        }
        time::sleep(planned.think).await;
        let value = base + step as i64 * 10;
        let id = fresh_id(node).await;
        let req = build(&q, id, planned, value);
        let first = planned.gateway;
        match planned.style {
            Style::Sync => {
                drive(&q, node, first, &req, false, true, planned.limit).await;
            }
            Style::Submit => {
                let answer = drive(&q, node, first, &req, true, true, planned.limit).await;
                if let Answer::Accepted(run) = answer {
                    poll(&q, node, first, run, &req, planned.polls).await;
                }
            }
            Style::Pile => {
                for extra in 0..3 {
                    let id = fresh_id(node).await;
                    let mut planned = planned.clone();
                    planned.kind = [Kind::Upsert, Kind::Batch, Kind::Get][extra];
                    let req = build(&q, id, &planned, value + 4 * (extra as i64 + 1) / 2);
                    drive(&q, node, first, &req, true, true, planned.limit).await;
                    if req.body.mutation {
                        sent.push(req);
                    }
                }
                continue;
            }
            Style::Race => {
                tokio::join!(
                    send(&q, node, GATEWAYS[0], &req, true, false, planned.limit),
                    send(
                        &q,
                        node,
                        GATEWAYS[1],
                        &req,
                        planned.other_key,
                        false,
                        planned.limit
                    ),
                );
                q.count("raced");
                let answer = drive(&q, node, first, &req, true, false, planned.limit).await;
                if let Answer::Accepted(run) = answer {
                    poll(&q, node, first, run, &req, planned.polls).await;
                }
            }
            Style::Cross => {
                let asynchronous = planned.other_key;
                let mut runs = Vec::new();
                for (first, asynchronous, fresh) in [
                    (first, asynchronous, true),
                    (1 - first, !asynchronous, false),
                ] {
                    let limit = planned.limit;
                    let answer = drive(&q, node, first, &req, asynchronous, fresh, limit).await;
                    if let Answer::Accepted(run) = answer {
                        runs.push(run);
                    }
                }
                // The result of the queued half, to compare with the other one.
                for run in runs {
                    poll(&q, node, first, run, &req, planned.polls).await;
                }
            }
            Style::Reuse => {
                let Some(earlier) = sent.last().cloned() else {
                    continue;
                };
                let key = match (planned.other_key, earlier.body.anchor) {
                    (false, Some(key)) => key,
                    _ => planned.key,
                };
                let body = Req::row("table.upsert", &q.ctx.table, earlier.body.id, key, value);
                let asynchronous = planned.polls % 2 == 0;
                drive(&q, node, first, &body, asynchronous, false, planned.limit).await;
                // The ID and body drawn above were not sent.
                continue;
            }
            Style::Close => {
                let close = Duration::from_millis(2 + planned.polls as u64 * 4);
                attempt(&q, node, GATEWAYS[first], &req, true, true, close, true).await;
                let answer = drive(&q, node, 1 - first, &req, true, false, planned.limit).await;
                if let Answer::Accepted(run) = answer {
                    poll(&q, node, 1 - first, run, &req, planned.polls).await;
                }
            }
        }
        if req.body.mutation {
            sent.push(req);
        }
    }
}

/// A queue fault of this workload. Each one applies itself, waits, and undoes itself before it
/// returns, so it never overlaps the shared nemesis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// Freeze one or two workers, submit, and kill the submitting coordinator (Q03, O30, O31).
    FreezeAndKill { two: bool, kill: Kill },
    /// Restart both APIs, one after the other, with accepted work open (Q04).
    RestartApis,
    /// Restart every worker, one after the other, with accepted work open (R01, R03, O14).
    Rolling,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kill {
    Nothing,
    Gateway,
    Worker,
}

struct Scheduled {
    at: u64,
    fault: Fault,
    hold: Duration,
    pick: u64,
    power: bool,
}

/// Every run draws each kind of fault once, at drawn times, so a seed covers all of them.
fn faults(g: &mut Gen, start: u64, chaos_ms: u64) -> Vec<Scheduled> {
    let mut faults: Vec<Scheduled> = [
        Fault::FreezeAndKill {
            two: g.bool(),
            kill: *g.pick(&[Kill::Nothing, Kill::Gateway, Kill::Worker]),
        },
        Fault::RestartApis,
        Fault::Rolling,
    ]
    .into_iter()
    .map(|fault| Scheduled {
        at: start + 500 + g.below(chaos_ms * 3 / 4),
        fault,
        hold: window(g, 200, 2_500),
        pick: g.below(3),
        power: g.bool(),
    })
    .collect();
    faults.sort_by_key(|fault| fault.at);
    faults
}

/// The first of 1,000 values that nothing else writes.
fn fresh_values(q: &Q) -> i64 {
    let mut shared = q.shared.lock().unwrap();
    shared.bursts += 1;
    400_000 + shared.bursts * 1_000
}

/// Submit `count` requests concurrently from their own tasks.
fn burst(q: &Q, gateway: usize, count: i64, retry: bool) -> Vec<JoinHandle<()>> {
    let base = fresh_values(q);
    (0..count)
        .map(|offset| {
            let q = q.clone();
            nabedra_rt::task::spawn(async move {
                let node = CLIENTS[offset as usize % CLIENTS.len()];
                let id = fresh_id(node).await;
                let value = base + offset * 10;
                let table = &q.ctx.table;
                let req = match offset % 3 {
                    0 => Req::row("table.upsert", table, id, 1 + offset % HOT, value),
                    1 => Req::row(
                        "table.update",
                        table,
                        id,
                        BOUNDARY + 1 + offset % HOT,
                        value,
                    ),
                    _ => Req::insert_many(
                        table,
                        id,
                        &[
                            (BATCH_BASE + value, value),
                            (BOUNDARY + BATCH_BASE + value + 1, value + 1),
                        ],
                    ),
                };
                let limit = Duration::from_secs(3);
                if retry {
                    drive(&q, node, gateway, &req, true, true, limit).await;
                } else {
                    send(&q, node, GATEWAYS[gateway], &req, true, true, limit).await;
                }
            })
        })
        .collect()
}

/// Submit reads concurrently. A queued read takes no range's write path, so a frozen Data leader
/// does not hold its acceptance up.
fn queued_reads(q: &Q, gateway: usize) -> Vec<JoinHandle<()>> {
    (0..4)
        .map(|offset| {
            let q = q.clone();
            nabedra_rt::task::spawn(async move {
                let node = CLIENTS[offset as usize % CLIENTS.len()];
                let id = fresh_id(node).await;
                let table = &q.ctx.table;
                let req = if offset % 2 == 0 {
                    Req::collect(table, id)
                } else {
                    Req::get(table, id, offset)
                };
                drive(&q, node, gateway, &req, true, true, Duration::from_secs(3)).await;
            })
        })
        .collect()
}

/// Wait for tasks. A panic in one is a violation, so that a lost client never reads as a quiet
/// one.
async fn join(cluster: &Cluster, tasks: Vec<JoinHandle<()>>) {
    for task in tasks {
        if let Err(error) = task.await
            && error.is_panic()
        {
            cluster.checks.violate(
                &cluster.world,
                "Panic",
                format!("a client task of the queued workload panicked: {error}"),
            );
        }
    }
}

/// Wait until `wanted` requests sent since `since` were accepted, or `limit` ends.
async fn wait_accepted(q: &Q, since: u64, wanted: usize, limit: Duration) {
    let give_up = q.now() + limit.as_millis() as u64;
    loop {
        let accepted = q
            .shared
            .lock()
            .unwrap()
            .log
            .accepted()
            .values()
            .filter(|submission| submission.invoked >= since)
            .count();
        if accepted >= wanted || q.now() >= give_up {
            return;
        }
        time::sleep(Duration::from_millis(2)).await;
    }
}

/// Follow the newest accepted requests that nobody has seen settled, from now on: this is what
/// tells whether a fault met open work.
fn watch(q: &Q, tasks: &mut Vec<JoinHandle<()>>) {
    let open: Vec<Open> = {
        let shared = q.shared.lock().unwrap();
        let run = shared.log.runs.last().expect("a run").id;
        let settled: BTreeSet<Uuid> = shared
            .log
            .lookups
            .iter()
            .filter(|lookup| !matches!(lookup.seen, Seen::Pending | Seen::Unknown))
            .map(|lookup| lookup.id)
            .collect();
        let mut seen = BTreeSet::new();
        shared
            .open
            .iter()
            .rev()
            .filter(|open| open.run == run && !settled.contains(&open.req.body.id))
            .filter(|open| seen.insert(open.req.body.id))
            .take(WATCHED)
            .cloned()
            .collect()
    };
    for (index, open) in open.into_iter().enumerate() {
        let q = q.clone();
        tasks.push(nabedra_rt::task::spawn(async move {
            let node = CLIENTS[index % CLIENTS.len()];
            poll(&q, node, index % 2, open.run, &open.req, 40).await;
        }));
    }
}

/// The ready workers that `node` reports from Control, in the current run.
async fn ready_workers(cluster: &Cluster, node: u64) -> Option<u64> {
    let reply = request(
        BOOTSTRAP_CLIENT,
        node,
        "/health/ready",
        &cluster.config.data_token,
        None,
        READY_LIMIT,
    )
    .await
    .ok()?;
    if reply.status != 200 || reply.body["run_id"] != json!(cluster.config.run_id) {
        return None;
    }
    reply.body["ready_workers"].as_u64()
}

/// Freeze workers, submit while they are frozen, and note the window. The workers stay frozen.
async fn freeze(
    q: &Q,
    cluster: &mut Cluster,
    nodes: &[u64],
    hold: Duration,
    bursts: &mut Vec<JoinHandle<()>>,
) {
    for &node in nodes {
        if !cluster.frozen().contains(&node) {
            cluster.freeze(node);
        }
    }
    let from = q.now() + 1;
    time::sleep(Duration::from_millis(2)).await;
    bursts.extend(burst(q, 0, 6, true));
    bursts.extend(queued_reads(q, 1));
    if nodes.len() > 1 && q.peek_defect(Defect::EarlyAccept) {
        seed_early_accept(q).await;
    }
    time::sleep(hold).await;
    let to = q.now();
    let running = WORKERS
        .into_iter()
        .find(|node| !nodes.contains(node))
        .expect("a running worker");
    let ready = ready_workers(cluster, running).await;
    cluster.record(format!(
        "queued: {nodes:?} frozen, n{running} reports {ready:?} ready workers"
    ));
    q.shared.lock().unwrap().log.frozen.push(Frozen {
        nodes: nodes.to_vec(),
        from,
        to,
        ready,
    });
}

/// The shared nemesis has nothing open, every process runs and the cluster reports itself
/// converged.
async fn quiet(cluster: &Cluster, chaos: Option<&Chaos>, faults: &NetFaults) -> bool {
    chaos.is_none_or(|chaos| chaos.log.iter().all(|fault| fault.to.is_some()))
        && !faults.open()
        && cluster.healthy()
        && cluster.converged().await
}

async fn apply(q: &Q, cluster: &mut Cluster, due: &Scheduled, bursts: &mut Vec<JoinHandle<()>>) {
    cluster.record(format!("queued fault {:?} hold {:?}", due.fault, due.hold));
    q.history().marker(
        &cluster.world,
        "queued-fault",
        json!({"fault": format!("{:?}", due.fault), "hold_ms": due.hold.as_millis() as u64}),
    );
    match due.fault {
        Fault::FreezeAndKill { two, kill } => {
            let mut order = WORKERS;
            order.rotate_left(due.pick as usize);
            let [first, second, third] = order;
            // Two frozen workers also take the Control quorum, so nothing is accepted whatever
            // the copies do. One frozen worker leaves the copies as the only obstacle, unless it
            // is the scheduler owner or the Control leader. So each worker is frozen in turn.
            let alone = due.hold.max(ALONE);
            for node in [second, third] {
                freeze(q, cluster, &[node], alone, bursts).await;
                cluster.thaw(node);
                if let Err(error) = wait_converged(cluster, THAW_LIMIT).await {
                    q.skip(cluster, format!("after n{node} thawed: {error}"));
                    break;
                }
            }
            let mut frozen = vec![first];
            freeze(q, cluster, &frozen, alone, bursts).await;
            if two {
                cluster.thaw(first);
                time::sleep(due.hold / 4).await;
                frozen.push(second);
                freeze(q, cluster, &frozen, due.hold, bursts).await;
            }
            let killed = match kill {
                Kill::Nothing => None,
                Kill::Gateway => Some(GATEWAYS[0]),
                Kill::Worker => WORKERS.into_iter().find(|node| !frozen.contains(node)),
            };
            if let Some(node) = killed {
                let cut = if due.power && kill == Kill::Worker {
                    DiskCut::Power
                } else {
                    DiskCut::Kill
                };
                q.mark(Mark::CoordinatorKilled {
                    gateway: (kill == Kill::Gateway).then_some(node),
                });
                if kill == Kill::Worker {
                    q.mark(Mark::WorkerKilled);
                }
                cluster.crash(node, cut).await;
                watch(q, bursts);
            }
            for node in frozen {
                cluster.thaw(node);
            }
            match (kill, killed) {
                (Kill::Gateway, Some(node)) => {
                    restart(q, cluster, Target::Gateway(node)).await;
                }
                (Kill::Worker, Some(node)) => {
                    restart(q, cluster, Target::Worker(node, due.power)).await;
                }
                _ => {}
            }
        }
        Fault::RestartApis => {
            let since = q.now();
            bursts.extend(burst(q, 1, BACKLOG, true));
            wait_accepted(q, since, 3, ACCEPT_WAIT).await;
            q.mark(Mark::ApiRestart);
            for node in GATEWAYS {
                cluster.kill_gateway(node).await;
                watch(q, bursts);
                time::sleep(due.hold / 4).await;
                restart(q, cluster, Target::Gateway(node)).await;
            }
        }
        Fault::Rolling => {
            let since = q.now();
            bursts.extend(burst(q, 0, BACKLOG, true));
            wait_accepted(q, since, 3, ACCEPT_WAIT).await;
            let mut order = WORKERS;
            order.rotate_left(due.pick as usize);
            let mut all = true;
            let first_stop = q.now();
            for node in order {
                let cut = if due.power {
                    DiskCut::Power
                } else {
                    DiskCut::Kill
                };
                q.mark(Mark::WorkerKilled);
                cluster.crash(node, cut).await;
                watch(q, bursts);
                all &= restart(q, cluster, Target::Worker(node, due.power)).await;
                if !all {
                    break;
                }
            }
            let end = q.now();
            {
                let mut shared = q.shared.lock().unwrap();
                shared.windows.push(("rolling restart", first_stop, end));
                if all {
                    shared.marks.push((Mark::Rolling { end }, first_stop));
                }
            }
            watch(q, bursts);
            if all {
                // The oldest requests too: their receipts outlived every first holder.
                relook(q, first_stop).await;
            }
        }
    }
}

enum Target {
    Gateway(u64),
    Worker(u64, bool),
}

/// Look up some requests that were accepted before `at`, once each.
async fn relook(q: &Q, at: u64) {
    let run = q.shared.lock().unwrap().log.runs.last().expect("a run").id;
    let open: Vec<Open> = {
        let shared = q.shared.lock().unwrap();
        let before: BTreeMap<Uuid, u64> = shared
            .log
            .accepted()
            .into_iter()
            .filter(|((accepted, _), submission)| *accepted == run && submission.completed <= at)
            .map(|((_, id), submission)| (id, submission.completed))
            .collect();
        let mut seen = BTreeSet::new();
        shared
            .open
            .iter()
            .rev()
            .filter(|open| open.run == run && before.contains_key(&open.req.body.id))
            .filter(|open| seen.insert(open.req.body.id))
            .take(WATCHED)
            .cloned()
            .collect()
    };
    for (index, open) in open.iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        lookup(q, node, GATEWAYS[index % 2], run, &open.req).await;
    }
}

/// Restart one process. A failure is the run's `Liveness` violation. A gateway is started again
/// as its supervisor does (lead decision 18); a worker has one start.
async fn restart(q: &Q, cluster: &mut Cluster, target: Target) -> bool {
    let (node, result) = match target {
        Target::Gateway(node) => {
            let (failed, started) = start_gateway(cluster, node).await;
            q.shared.lock().unwrap().failed_starts.extend(failed);
            return started;
        }
        Target::Worker(node, power) => {
            let cut = if power { DiskCut::Power } else { DiskCut::Kill };
            (node, cluster.restart_worker(node, cut).await)
        }
    };
    if let Err(error) = &result {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!("queued fault: n{node} did not restart: {error}"),
        );
    }
    result.is_ok()
}

/// Seeded defect: a 202 that no second copy can back.
async fn seed_early_accept(q: &Q) {
    if !q.take_defect(Defect::EarlyAccept) {
        return;
    }
    let id = fresh_id(CLIENTS[0]).await;
    let req = Req::row("table.upsert", &q.ctx.table, id, 1, 777_777);
    let now = q.now();
    let run = q.shared.lock().unwrap().log.runs.last().map(|run| run.id);
    q.shared.lock().unwrap().log.submissions.push(Submission {
        body: req.body,
        route: Route::Queued,
        asynchronous: true,
        gateway: GATEWAYS[0],
        cut: false,
        closed: false,
        fresh: true,
        invoked: now,
        completed: now,
        answer: Answer::Accepted(run.expect("a run")),
    });
}

/// How the run ends after it converged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finale {
    None,
    FullStop,
    LastHolder,
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
    let mode = Mode::of(seed)?;
    let mut report = direct::Report::default();
    wait_converged(cluster, Duration::from_secs(30))
        .await
        .map_err(|error| format!("initial convergence: {error}"))?;
    if mode.scoped {
        activate_scoped(cluster).await?;
    }
    let table = direct::create_table(cluster, &history).await?;
    split(cluster, &table).await?;
    for node in WORKERS {
        let stats = cluster.stats(node).await?;
        cluster
            .checks
            .capture_resources(&cluster.world, node, &stats);
    }
    let programs = plan(g);
    let ctx = Ctx::new(
        cluster.world.clone(),
        cluster.checks.clone(),
        history.clone(),
        table,
        cluster.config.data_token.clone(),
        config.deadline(),
        HOT,
    );
    let q = Q {
        ctx,
        mode,
        shared: Arc::default(),
    };
    {
        let mut shared = q.shared.lock().unwrap();
        shared.defect = Defect::from_env()?;
        shared.log.retention_ms = mode.retention_ms;
        shared.log.scoped = mode.scoped;
        shared.log.runs.push(Run {
            id: cluster.config.run_id,
            stopped: None,
        });
    }
    copies(&q, cluster, "before the clients").await;
    let faults_net = NetFaults::install(cluster.net(), &cluster.world, seed);
    config.jump_clocks(&cluster.world);
    let start = elapsed_ms(&cluster.world);
    let profile = without_split(selection.profile(seed));
    let mut chaos = (!no_nemesis && profile.total_weight() > 0)
        .then(|| Chaos::new(profile, faults_net.clone(), start, g));
    let chaos_ms = direct::CHAOS.as_millis() as u64;
    let mut schedule = if no_nemesis {
        Vec::new()
    } else {
        faults(g, start, chaos_ms)
    };
    let finale = if no_nemesis {
        Finale::None
    } else {
        match *g.pick(&[Finale::None, Finale::FullStop, Finale::LastHolder]) {
            // Two exact copies among three voters: the last worker may hold neither.
            Finale::LastHolder if mode.scoped => Finale::FullStop,
            finale => finale,
        }
    };
    cluster.record(format!(
        "workload {} {} retention {} clients {} finale {finale:?}",
        Workload::Queued,
        mode.name(),
        mode.retention_ms,
        programs.len(),
    ));
    let mut tasks: Vec<JoinHandle<()>> = programs
        .into_iter()
        .enumerate()
        .map(|(index, program)| nabedra_rt::task::spawn(run_client(q.clone(), index, program)))
        .collect();
    tasks.push(nabedra_rt::task::spawn(never_submitted(q.clone())));
    let mut bursts = Vec::new();

    let end = cluster.world.elapsed() + direct::CHAOS;
    while cluster.world.elapsed() < end {
        q.shared.lock().unwrap().last_turn = q.now();
        if let Some(chaos) = chaos.as_mut() {
            chaos.tick(cluster, &q.ctx, g).await;
        }
        follow_run(&q, cluster);
        let now = q.now();
        if schedule.first().is_some_and(|due| due.at <= now)
            && quiet(cluster, chaos.as_ref(), &faults_net).await
        {
            let due = schedule.remove(0);
            apply(&q, cluster, &due, &mut bursts).await;
        }
        time::sleep(TICK).await;
    }
    q.shared
        .lock()
        .unwrap()
        .windows
        .push(("chaos window", start, elapsed_ms(&cluster.world)));
    // A fault that found no quiet moment runs now, after the heal and with the clients still
    // sending, so every seed runs each of them.
    q.shared.lock().unwrap().last_turn = q.now();
    if let Some(chaos) = chaos.as_mut()
        && let Err(error) = chaos.heal(cluster, &q.ctx).await
    {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &q, tasks, bursts, Some(chaos), report).await;
    }
    follow_run(&q, cluster);
    let late = schedule.len();
    for due in schedule {
        if let Err(error) = wait_converged(cluster, direct::CONVERGE_LIMIT).await {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("before {:?}: {error}", due.fault),
            );
            return abandon(cluster, &q, tasks, bursts, chaos.as_ref(), report).await;
        }
        apply(&q, cluster, &due, &mut bursts).await;
    }
    q.ctx.stopping.store(true, Ordering::Release);
    faults_net.clear();
    follow_run(&q, cluster);
    cluster
        .checks
        .expect_recovery(&cluster.world, "convergence", direct::CONVERGE_LIMIT);
    let converge_by = q.now() + direct::CONVERGE_LIMIT.as_millis() as u64;
    q.ctx.converge_by.store(converge_by, Ordering::Release);
    if let Err(error) = wait_converged(cluster, direct::CONVERGE_LIMIT).await {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("heal: {error}"));
        return abandon(cluster, &q, tasks, bursts, chaos.as_ref(), report).await;
    }
    tasks.append(&mut bursts);
    tokio::join!(
        join(cluster, tasks),
        observe(&q, cluster, cluster.config.run_id)
    );
    settle(&q, cluster, "the heal").await;
    reread(&q, cluster.config.run_id).await;
    let finals = final_reads(&q).await;
    report
        .assertions
        .push(("final reads through both gateways", finals.is_ok()));
    if let Err(error) = &finals {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", error.clone());
    }
    cluster.checks.recovered(&cluster.world, "convergence");
    // Bounds below are their own.
    q.ctx.converge_by.store(u64::MAX, Ordering::Release);
    copies(&q, cluster, "after the heal").await;
    expire(&q).await;
    // The resource check comes before the finale: a full stop empties the gauges it reads.
    let stats = direct::settle(cluster, &[]).await?.0;
    for (node, stats) in &stats {
        cluster.checks.check_resources(&cluster.world, *node, stats);
    }
    if finale != Finale::None
        && let Err(error) = stop(&q, cluster, finale, g).await
    {
        cluster
            .checks
            .violate(&cluster.world, "Liveness", format!("{finale:?}: {error}"));
    }
    if q.peek_defect(Defect::ExecuteTwice) {
        execute_twice(&q).await;
    }
    finish(&q, cluster, chaos.as_ref(), &mut report);
    report.summary["finale"] = json!(format!("{finale:?}"));
    report.summary["late_faults"] = json!(late);
    report.summary["profile"] = json!(chaos.as_ref().map(Chaos::profile_name));
    cluster.record(format!("workload summary {}", report.summary));
    Ok(report)
}

/// The run cannot converge: stop the clients and keep the violations already recorded.
async fn abandon(
    cluster: &mut Cluster,
    q: &Q,
    mut tasks: Vec<JoinHandle<()>>,
    mut bursts: Vec<JoinHandle<()>>,
    chaos: Option<&Chaos>,
    mut report: direct::Report,
) -> Result<direct::Report, String> {
    q.ctx.converge_by.store(0, Ordering::Release);
    tasks.append(&mut bursts);
    for task in &tasks {
        task.abort();
    }
    join(cluster, tasks).await;
    cluster.record("workload abandoned after a liveness violation".into());
    finish(q, cluster, chaos, &mut report);
    report.summary["abandoned"] = json!(true);
    Ok(report)
}

/// Judge the rules and report what ran.
fn finish(q: &Q, cluster: &Cluster, chaos: Option<&Chaos>, report: &mut direct::Report) {
    let mut shared = q.shared.lock().unwrap();
    let jumps = chaos.map_or(0, |chaos| {
        chaos
            .log
            .iter()
            .filter(|fault| fault.action == Action::Clock)
            .count() as u64
    });
    shared.log.clock_slack_ms = clock_slack(jumps);
    let outcome = shared.log.judge();
    // The violation list of a run is bounded. A rule that fires often must not crowd the others
    // out of it, so each rule reports its first few and how many there were.
    for rule in RULES {
        let found: Vec<&String> = outcome
            .violations
            .iter()
            .filter(|(kind, _)| *kind == rule)
            .map(|(_, detail)| detail)
            .collect();
        for detail in found.iter().take(REPORTED) {
            cluster.checks.violate(
                &cluster.world,
                rule,
                format!("{detail} (one of {})", found.len()),
            );
        }
    }
    for rule in RULES {
        if outcome.judged.contains_key(rule) {
            let held = !outcome.violations.iter().any(|(kind, _)| *kind == rule);
            report.assertions.push((rule, held));
        }
    }
    let mut marks = shared.marks.clone();
    for fault in chaos.iter().flat_map(|chaos| &chaos.log) {
        let mark = match fault.action {
            Action::Partition | Action::LeaderMove => Mark::Partition,
            Action::LostReply | Action::LostHttpReply => Mark::LostReply {
                to: fault.to.unwrap_or(u64::MAX),
            },
            _ => continue,
        };
        marks.push((mark, fault.from));
    }
    let facts = Facts {
        log: &shared.log,
        direct: q.mode.direct,
        marks: &marks,
        stop: shared.stop,
    };
    report.scenarios.extend(facts.clauses());
    let mut worked = BTreeMap::new();
    for &(name, from, to) in &shared.windows {
        let work = Work::of(&shared.log, from, to);
        let missing = progress(name, &work);
        if !missing.is_empty() {
            cluster.checks.violate(
                &cluster.world,
                "Progress",
                format!(
                    "{name}, {from}ms to {to}ms: no {} in {} attempts",
                    missing.join(", no "),
                    work.attempts
                ),
            );
        }
        worked.insert(
            format!("{name} {from}"),
            json!({
                "ms": to - from,
                "attempts": work.attempts,
                "writes": work.writes,
                "reads": work.reads,
                "settled": work.settled,
                "longest_gap_ms": work.gap_ms,
            }),
        );
    }
    if shared.undecided > 0 {
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!(
                "{} submissions had no definite answer {:?} after the heal",
                shared.undecided,
                direct::CONVERGE_LIMIT
            ),
        );
    }
    report
        .assertions
        .push(("every submission answered", shared.undecided == 0));
    report.summary = json!({
        "workload": Workload::Queued.name(),
        "mode": q.mode.name(),
        "judged": outcome.judged,
        "counts": shared.counts,
        "submissions": shared.log.submissions.len(),
        "lookups": shared.log.lookups.len(),
        "accepted": shared.log.accepted().len(),
        "undecided": shared.undecided,
        "terminal_regressions": shared.log.regressions(),
        "runs": shared.log.runs.len(),
        "frozen": shared.log.frozen.iter().map(|window| {
            let sent = shared.log.submissions.iter().filter(|s| {
                s.fresh && s.route == Route::Queued && s.invoked >= window.from && s.invoked < window.to
            });
            let judged = shared.log.submissions.iter().filter(|s| {
                shared.log.frozen_window(s).is_some_and(|judged| judged.from == window.from)
            });
            json!({
                "nodes": window.nodes,
                "ms": window.to.saturating_sub(window.from),
                "ready": window.ready,
                "sent": sent.count(),
                "judged": judged.count(),
            })
        }).collect::<Vec<_>>(),
        "held": shared.log.retained.iter().map(|held| json!({
            "node": held.node,
            "holder": format!("{:?}", held.holder),
            "required": held.required,
            "before": held.before,
            "after": held.after,
            "pending": held.pending,
        })).collect::<Vec<_>>(),
        "outside_contract": shared.outside,
        "work": worked,
        "failed_gateway_starts": shared.failed_starts,
        "not_compared": shared.skipped,
        "abandoned": false,
    });
}

/// What a window with sending clients must have completed and did not (lead decision 17). The
/// chaos window needs one request that was accepted and settled as well: without one the queue
/// was not exercised. A rolling restart stops one worker at a time (OLTP-G10: "Writes stay
/// available with one voter paused or lost").
fn progress(window: &str, work: &Work) -> Vec<&'static str> {
    let queue = window == "chaos window";
    [
        ("write committed", work.writes == 0),
        ("read answered", work.reads == 0),
        ("accepted request settled", queue && work.settled == 0),
    ]
    .into_iter()
    .filter(|(_, missing)| *missing)
    .map(|(name, _)| name)
    .collect()
}

/// Note a full stop that the shared nemesis did, by its new run.
fn follow_run(q: &Q, cluster: &Cluster) {
    let mut shared = q.shared.lock().unwrap();
    let run = cluster.config.run_id;
    let last = shared.log.runs.last().copied();
    if last.is_some_and(|last| last.id == run) {
        return;
    }
    let begun = shared.last_turn;
    if let Some(last) = shared.log.runs.last_mut() {
        last.stopped.get_or_insert(begun);
    }
    shared.log.runs.push(Run {
        id: run,
        stopped: None,
    });
}

/// The range this workload's setup split is the one that the shared `split` action splits, so
/// that action could never publish here. Splits under load belong to the topology workload.
fn without_split(profile: Profile<Action>) -> Profile<Action> {
    let mut kept = Profile::new(profile.name, profile.gap_ms);
    for (action, weight) in profile.actions() {
        if *action != Action::Split {
            kept = kept.with(*action, weight);
        }
    }
    kept
}

async fn wait_converged(cluster: &Cluster, limit: Duration) -> Result<(), String> {
    let deadline = cluster.world.elapsed() + limit;
    while !cluster.converged().await {
        if cluster.world.elapsed() >= deadline {
            return Err("the cluster did not report itself converged".into());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// Turn the scoped protocol's admission on, as an operator does.
async fn activate_scoped(cluster: &Cluster) -> Result<(), String> {
    let deadline = cluster.world.elapsed() + SCOPED_LIMIT;
    loop {
        let status = cluster.admin("queue.scoped.status", json!({})).await;
        let last = match status {
            Ok(status) if !status["activation"].is_null() => {
                cluster.record(format!("scoped queue active {status}"));
                return Ok(());
            }
            Ok(status) => cluster
                .admin(
                    "queue.scoped.admission",
                    json!({"expected": status, "enabled": true}),
                )
                .await
                .map(|_| "admission enabled".to_owned())
                .unwrap_or_else(|error| error),
            Err(error) => error,
        };
        if cluster.world.elapsed() >= deadline {
            return Err(format!("scoped queue activation: {last}"));
        }
        time::sleep(Duration::from_millis(250)).await;
    }
}

/// Split the empty table's range, so batches cross ranges.
async fn split(cluster: &Cluster, table: &str) -> Result<(), String> {
    let table = Uuid::parse_str(table).map_err(|error| error.to_string())?;
    let id = cluster.split_id().await;
    let deadline = cluster.world.elapsed() + SPLIT_LIMIT;
    loop {
        match cluster.split(id, table, BOUNDARY).await {
            Ok(_) => return Ok(()),
            Err(error) if cluster.world.elapsed() >= deadline => {
                return Err(format!("setup split: {error}"));
            }
            Err(_) => time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// Look up receipts of identities that no client ever submitted.
async fn never_submitted(q: Q) {
    let node = CLIENTS[0];
    for index in 0.. {
        if q.stopping() {
            return;
        }
        let id = fresh_id(node).await;
        let run = {
            let mut shared = q.shared.lock().unwrap();
            shared.log.never_submitted.insert(id);
            shared.log.runs.last().expect("a run").id
        };
        let req = Req::get(&q.ctx.table, id, 1);
        lookup(&q, node, GATEWAYS[index % 2], run, &req).await;
        time::sleep(Duration::from_millis(2_500)).await;
    }
}

/// Look up every unsettled receipt of `run` until none is left or the budget ends. Returns what
/// is left. A receipt that is terminal without an outcome is looked up a few times and left to
/// `AcceptedSettlesInRun`: waiting longer would hold up the checks after this one.
async fn observe(q: &Q, cluster: &Cluster, run: Uuid) -> BTreeSet<Uuid> {
    let mut round = 0;
    loop {
        let (unsettled, open): (BTreeSet<Uuid>, BTreeSet<Uuid>) = {
            let shared = q.shared.lock().unwrap();
            let unsettled: BTreeSet<Uuid> = shared.log.unsettled(run).into_iter().collect();
            let terminal: BTreeSet<Uuid> = shared.log.without_outcome(run);
            let open = unsettled.difference(&terminal).copied().collect();
            (unsettled, open)
        };
        if open.is_empty() && (unsettled.is_empty() || round >= 3) || q.out_of_time() {
            return open;
        }
        let mut seen = BTreeSet::new();
        let open: Vec<Open> = q
            .shared
            .lock()
            .unwrap()
            .open
            .iter()
            .filter(|open| open.run == run && unsettled.contains(&open.req.body.id))
            .filter(|open| seen.insert(open.req.body.id))
            .cloned()
            .collect();
        for chunk in open.chunks(8) {
            let lookups: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(index, open)| {
                    let (q, open) = (q.clone(), open.clone());
                    nabedra_rt::task::spawn(async move {
                        let node = CLIENTS[index % CLIENTS.len()];
                        let gateway = GATEWAYS[(index + round) % GATEWAYS.len()];
                        lookup(&q, node, gateway, open.run, &open.req).await;
                    })
                })
                .collect();
            join(cluster, lookups).await;
        }
        round += 1;
        time::sleep(Duration::from_millis(200)).await;
    }
}

/// After the heal, every accepted request of the current run must have a terminal receipt
/// within the convergence bound (design 3.7, `Liveness`).
async fn settle(q: &Q, cluster: &Cluster, when: &str) {
    let run = cluster.config.run_id;
    let unsettled = observe(q, cluster, run).await;
    let shared = q.shared.lock().unwrap();
    let total = unsettled.len();
    for id in unsettled.into_iter().take(3) {
        let name = shared
            .open
            .iter()
            .find(|open| open.req.body.id == id)
            .map_or("", |open| open.req.name);
        cluster.checks.violate(
            &cluster.world,
            "Liveness",
            format!(
                "accepted {name} {id} of run {run}, one of {total}, has no terminal receipt {:?} after {when}; last answer {}",
                direct::CONVERGE_LIMIT,
                shared.last.get(&id).map_or("none", String::as_str),
            ),
        );
    }
}

/// Look the settled receipts of `run` up again through both gateways: a terminal answer does
/// not change, whoever served the request's faults.
async fn reread(q: &Q, run: Uuid) {
    let mut seen = BTreeSet::new();
    let settled: Vec<Open> = {
        let shared = q.shared.lock().unwrap();
        shared
            .open
            .iter()
            .rev()
            .filter(|open| open.run == run && seen.insert(open.req.body.id))
            .take(24)
            .cloned()
            .collect()
    };
    for (index, open) in settled.iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        for gateway in GATEWAYS {
            lookup(q, node, gateway, open.run, &open.req).await;
        }
    }
}

/// One worker's queue counters: accepted records, and records that are not terminal.
async fn counters(cluster: &Cluster, node: u64) -> Result<(u64, u64), String> {
    let stats = cluster.stats(node).await?;
    let count = |field: &str| {
        stats["queue"][field]
            .as_u64()
            .ok_or_else(|| format!("n{node} stats have no queue.{field}"))
    };
    let pending =
        count("queued_records")? + count("running_records")? + count("reconciling_records")?;
    Ok((count("accepted_records")?, pending))
}

/// The accepted records of every worker, at a point from which the count can only grow: results
/// of a long retention do not expire within a run, and with a short one this waits until every
/// older result has. A few records that stay although their results expired may leave at any
/// time, so they are not counted: what is held later must then be enough without them.
async fn baseline(q: &Q, cluster: &Cluster) -> Result<BTreeMap<u64, u64>, String> {
    let give_up = q.now() + q.mode.retention_ms + 10_000;
    loop {
        let mut counts = BTreeMap::new();
        for node in WORKERS {
            counts.insert(node, counters(cluster, node).await?.0);
        }
        let empty = counts.values().all(|count| *count == 0);
        if empty || q.mode.retention_ms >= LONG_RETENTION_MS {
            return Ok(counts);
        }
        if q.now() >= give_up {
            if counts.values().any(|count| *count > STUCK) {
                return Err(format!("older records did not expire: {counts:?}"));
            }
            cluster.record(format!("queued: older records did not expire: {counts:?}"));
            return Ok(WORKERS.map(|node| (node, 0)).into());
        }
        time::sleep(Duration::from_millis(250)).await;
    }
}

/// At a quiet point, every ready worker of the legacy protocol holds every request that was
/// accepted since (plan: "to all ready workers before acknowledging acceptance"). Only those:
/// a worker that joined later need not hold older settled records.
async fn copies(q: &Q, cluster: &Cluster, when: &str) {
    if q.mode.scoped {
        return;
    }
    let before = match baseline(q, cluster).await {
        Ok(before) if cluster.converged().await => before,
        Ok(_) => {
            return q.skip(
                cluster,
                format!("copies {when}: the cluster was not converged"),
            );
        }
        Err(error) => return q.skip(cluster, format!("copies {when}: {error}")),
    };
    let since = q.now();
    join(cluster, burst(q, 0, 12, false)).await;
    let sent = q.now();
    let mut after = Vec::new();
    for node in WORKERS {
        match counters(cluster, node).await {
            Ok(counts) => after.push((node, counts)),
            Err(error) => return q.skip(cluster, format!("copies {when}: {error}")),
        }
    }
    let at = q.now();
    // Nothing joined or left: the workers of the count before are the ready ones.
    if !cluster.converged().await {
        return q.skip(cluster, format!("copies {when}: the cluster changed"));
    }
    let required = required(q, cluster.config.run_id, since, sent, at);
    cluster.record(format!(
        "queued copies {when}: {required} accepted, before {before:?}, after {after:?}"
    ));
    for (node, (accepted, pending)) in after {
        q.shared.lock().unwrap().log.retained.push(Retained {
            node,
            holder: Holder::Ready,
            required,
            before: before[&node],
            after: accepted,
            pending,
            at,
        });
    }
    bounded_settle(q, cluster, "the copies were counted").await;
}

/// [`settle`] with a bound of its own.
async fn bounded_settle(q: &Q, cluster: &Cluster, when: &str) {
    let by = q.now() + direct::CONVERGE_LIMIT.as_millis() as u64;
    q.ctx.converge_by.store(by, Ordering::Release);
    settle(q, cluster, when).await;
    q.ctx.converge_by.store(u64::MAX, Ordering::Release);
}

/// Requests of `run` accepted between `since` and `before` whose results cannot have expired
/// by `at`.
fn required(q: &Q, run: Uuid, since: u64, before: u64, at: u64) -> usize {
    let shared = q.shared.lock().unwrap();
    shared
        .log
        .accepted()
        .iter()
        .filter(|((accepted, _), submission)| {
            *accepted == run
                && submission.invoked >= since
                && submission.completed <= before
                && submission.invoked + shared.log.retention_ms > at + 1_000
        })
        .count()
}

/// Read every hot and deleted key and the whole table through both gateways.
async fn final_reads(q: &Q) -> Result<(), String> {
    let mut failed = Vec::new();
    let keys: Vec<i64> = (1..=HOT)
        .flat_map(|low| [low, BOUNDARY + low])
        .chain((0..DELETED).map(|offset| DELETED_BASE + offset))
        .collect();
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        let mut reqs = Vec::new();
        for &key in &keys {
            reqs.push(Req::get(&q.ctx.table, fresh_id(node).await, key));
        }
        reqs.push(Req::collect(&q.ctx.table, fresh_id(node).await));
        for req in reqs {
            let limit = Duration::from_secs(10);
            let answer = send(q, node, gateway, &req, false, true, limit).await;
            if !matches!(answer, Answer::Terminal(Terminal::Succeeded(_))) {
                failed.push(format!(
                    "{} of key {:?} through n{gateway}: {answer:?}",
                    req.name, req.body.anchor
                ));
            }
        }
    }
    let total = failed.len();
    if total == 0 {
        return Ok(());
    }
    failed.truncate(3);
    Err(format!("{total} final reads failed: {}", failed.join("; ")))
}

/// Accept and settle one upsert of `key`, and read it.
async fn settled_upsert(q: &Q, key: i64, value: i64) -> Option<Open> {
    let node = CLIENTS[0];
    let limit = Duration::from_secs(5);
    let req = Req::row(
        "table.upsert",
        &q.ctx.table,
        fresh_id(node).await,
        key,
        value,
    );
    let Answer::Accepted(run) = send(q, node, GATEWAYS[0], &req, true, true, limit).await else {
        return None;
    };
    if !matches!(
        poll(q, node, 0, run, &req, 40).await,
        Seen::Terminal(Terminal::Succeeded(_))
    ) {
        return None;
    }
    let get = Req::get(&q.ctx.table, fresh_id(node).await, key);
    send(q, node, GATEWAYS[1], &get, false, true, limit).await;
    Some(Open { run, req })
}

/// Q06: past the retention, look a settled receipt up and send its identity again. The answer
/// is an explicit expiry or the original result, and a mutation does not run again. Some of the
/// mutations were replaced by a newer value of their key, so that a second run is visible.
async fn expire(q: &Q) {
    if q.mode.retention_ms >= LONG_RETENTION_MS {
        return;
    }
    let mut settled: Vec<Open> = Vec::new();
    let values = fresh_values(q);
    for (index, key) in [1, BOUNDARY + 1, 2, BOUNDARY + 2].into_iter().enumerate() {
        let value = values + 10 * index as i64;
        let old = settled_upsert(q, key, value).await;
        let new = settled_upsert(q, key, value + 1).await;
        if let (Some(old), Some(_)) = (old, new) {
            settled.push(old);
        }
    }
    time::sleep(Duration::from_millis(q.mode.retention_ms + 6_000)).await;
    {
        let shared = q.shared.lock().unwrap();
        let run = shared.log.runs.last().expect("a run").id;
        let newest = shared.open.iter().rev();
        settled.extend(newest.filter(|open| open.run == run).take(8).cloned());
    }
    for (index, open) in settled.iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        let gateway = index % 2;
        lookup(q, node, GATEWAYS[gateway], open.run, &open.req).await;
        let limit = Duration::from_secs(5);
        let asynchronous = index % 3 != 0;
        let answer = send(
            q,
            node,
            GATEWAYS[1 - gateway],
            &open.req,
            asynchronous,
            false,
            limit,
        )
        .await;
        if let Answer::Accepted(run) = answer {
            poll(q, node, gateway, run, &open.req, 40).await;
        }
    }
    // Whatever ran again would have replaced a newer value.
    let _ = final_reads(q).await;
}

/// The finale: accepted work in flight, then every worker stops and the cluster cold starts.
async fn stop(q: &Q, cluster: &mut Cluster, finale: Finale, g: &mut Gen) -> Result<(), String> {
    if !cluster.converged().await {
        return Err("the cluster was not converged before the stop".into());
    }
    let old = cluster.config.run_id;
    let before = if finale == Finale::LastHolder {
        match baseline(q, cluster).await {
            Ok(before) => Some(before),
            Err(error) => {
                q.skip(cluster, format!("last holder: {error}"));
                None
            }
        }
    } else {
        None
    };
    let power = g.bool();
    let cut = if power { DiskCut::Power } else { DiskCut::Kill };
    // A backlog: with two worker threads, most of these are still pending when the workers stop.
    let count = 40 + g.below(40) as i64;
    let since = q.now();
    let tasks = burst(q, 0, count, false);
    // The stop needs accepted work to abandon or to keep. It comes as soon as some is accepted,
    // or after 2 s; a seed without any leaves the clauses unexecuted.
    let wanted = match finale {
        Finale::LastHolder => 3,
        _ => 1 + g.below(6) as usize,
    };
    let give_up = q.now() + 2_000;
    while q.now() < give_up && required(q, old, since, q.now(), q.now()) < wanted {
        time::sleep(Duration::from_millis(2)).await;
    }
    if finale == Finale::LastHolder {
        last_holder(q, cluster, cut, since, before, g).await;
    }
    let begun = q.now();
    q.history().marker(
        &cluster.world,
        "full-stop",
        json!({"finale": format!("{finale:?}"), "power": power}),
    );
    {
        let mut shared = q.shared.lock().unwrap();
        if let Some(last) = shared.log.runs.last_mut() {
            last.stopped.get_or_insert(begun);
        }
    }
    let delays = [(); 3].map(|()| window(g, 0, 2_000));
    let cold = cluster
        .cold_start_staggered("queued finale", cut, delays, COLD_LIMIT)
        .await;
    join(cluster, tasks).await;
    cold.map_err(|error| format!("cold start: {error}"))?;
    let run = cluster.config.run_id;
    // The counters are the legacy queue's alone, so they say nothing about a scoped one.
    if !q.mode.scoped {
        for node in WORKERS {
            let (_, pending_records) = counters(cluster, node).await?;
            let fresh = Fresh {
                node,
                run,
                pending_records,
                at: q.now(),
            };
            q.shared.lock().unwrap().log.fresh_runs.push(fresh);
        }
    }
    q.shared.lock().unwrap().log.runs.push(Run {
        id: run,
        stopped: None,
    });
    // Old receipts through both gateways, until each is definite or 30 s pass.
    let open: Vec<Open> = q
        .shared
        .lock()
        .unwrap()
        .open
        .iter()
        .rev()
        .filter(|open| open.run == old)
        .take(48)
        .cloned()
        .collect();
    let budget = q.now() + direct::CONVERGE_LIMIT.as_millis() as u64;
    let mut abandoned = 0;
    for (index, open) in open.iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        loop {
            let seen = lookup(q, node, GATEWAYS[index % 2], old, &open.req).await;
            abandoned += u64::from(seen == Seen::Abandoned && open.req.body.mutation);
            q.count(match &seen {
                Seen::Abandoned => "old receipt abandoned",
                Seen::Terminal(Terminal::Succeeded(_)) => "old receipt succeeded",
                Seen::Terminal(Terminal::Failed { .. }) => "old receipt failed",
                Seen::Missing => "old receipt missing",
                Seen::Pending => "old receipt pending",
                Seen::Unknown => "old receipt unknown",
            });
            match seen {
                Seen::Pending => {
                    // An old run's request is settled or abandoned, never pending.
                    cluster.checks.violate(
                        &cluster.world,
                        "QueueRetention",
                        format!("old receipt {} is pending in run {run}", open.req.body.id),
                    );
                    break;
                }
                Seen::Unknown if q.now() < budget => {
                    time::sleep(Duration::from_millis(200)).await;
                }
                Seen::Unknown => {
                    cluster.checks.violate(
                        &cluster.world,
                        "Liveness",
                        format!(
                            "old receipt {} never answered after the cold start",
                            open.req.body.id
                        ),
                    );
                    break;
                }
                _ => break,
            }
        }
    }
    cluster.record(format!("queued finale: {abandoned} mutations abandoned"));
    if q.take_defect(Defect::ReplayAbandoned) {
        replay_abandoned(q).await;
    }
    // Time for a replay to run, then the reads that would see it.
    time::sleep(Duration::from_secs(3)).await;
    let reread = final_reads(q).await;
    q.shared.lock().unwrap().stop = Some(Stop {
        run: old,
        at: begun,
        reread: reread.is_ok(),
    });
    reread?;
    // The new run accepts and settles work of its own.
    q.ctx.converge_by.store(
        q.now() + direct::CONVERGE_LIMIT.as_millis() as u64,
        Ordering::Release,
    );
    join(cluster, burst(q, 1, 3, true)).await;
    settle(q, cluster, "the cold start").await;
    q.ctx.converge_by.store(u64::MAX, Ordering::Release);
    Ok(())
}

/// R02: stop two workers one after the other. The last one keeps every accepted request.
async fn last_holder(
    q: &Q,
    cluster: &mut Cluster,
    cut: DiskCut,
    since: u64,
    before: Option<BTreeMap<u64, u64>>,
    g: &mut Gen,
) {
    let mut order = WORKERS;
    order.rotate_left(g.below(3) as usize);
    let [first, second, last] = order;
    let run = cluster.config.run_id;
    let stopping = q.now();
    cluster.crash(first, cut).await;
    time::sleep(window(g, 1, 1_500)).await;
    let two_down = q.now();
    q.shared.lock().unwrap().log.two_down.push(two_down);
    cluster.crash(second, cut).await;
    for pause_ms in [1, 2_000] {
        time::sleep(Duration::from_millis(pause_ms)).await;
        let (accepted, pending) = match counters(cluster, last).await {
            Ok(counts) => counts,
            Err(error) => {
                q.skip(cluster, format!("last holder: {error}"));
                continue;
            }
        };
        let at = q.now();
        let required = required(q, run, since, stopping, at);
        cluster.record(format!(
            "last holder n{last}: {before:?} before, {accepted} records, {pending} pending, {required} required"
        ));
        let Some(before) = before.as_ref().map(|before| before[&last]) else {
            continue;
        };
        q.shared.lock().unwrap().log.retained.push(Retained {
            node: last,
            holder: Holder::Last,
            required,
            before,
            after: accepted,
            pending,
            at,
        });
    }
}

/// Seeded defect: what a queue rebuilt after a full stop would do.
async fn replay_abandoned(q: &Q) {
    let abandoned = q
        .shared
        .lock()
        .unwrap()
        .abandoned
        .iter()
        .rev()
        .find(|req| !req.body.writes.is_empty())
        .cloned();
    let Some(req) = abandoned else { return };
    again(q, &req, "seeded-replay").await;
}

/// Seeded defect: run the newest settled upsert of a hot key again, after a later value.
async fn execute_twice(q: &Q) {
    let settled = {
        let shared = q.shared.lock().unwrap();
        shared.log.reads.iter().find_map(|read| {
            let value = read.value?;
            let later = shared
                .log
                .reads
                .iter()
                .rev()
                .find(|late| late.key == read.key)?;
            let written = shared
                .log
                .submissions
                .iter()
                .any(|submission| submission.body.writes.contains(&(read.key, value)));
            (written && later.invoked > read.completed && later.value != read.value)
                .then_some((read.key, value))
        })
    };
    let Some((key, value)) = settled else { return };
    q.take_defect(Defect::ExecuteTwice);
    let req = Req::row("table.upsert", &q.ctx.table, Uuid::nil(), key, value);
    again(q, &req, "seeded-second-execution").await;
    let _ = final_reads(q).await;
}

/// Apply a request's write under another identity, outside the client history.
async fn again(q: &Q, req: &Req, marker: &str) {
    let node = CLIENTS[0];
    let id = fresh_id(node).await;
    q.history()
        .marker(&q.ctx.world, marker, json!({"key": req.body.anchor}));
    let Some(&(key, value)) = req.body.writes.first() else {
        return;
    };
    let body = Req::row("table.upsert", &q.ctx.table, id, key, value);
    let _ = request(
        node,
        GATEWAYS[0],
        OPERATIONS,
        &q.ctx.token,
        Some(operation(id, body.name, body.payload.clone())),
        Duration::from_secs(10),
    )
    .await;
}

/// One logical request: every attempt and receipt of one identity and body.
struct Logical {
    f: String,
    key: Option<String>,
    request_id: String,
    input: Value,
    call: usize,
    /// The first success: where and what.
    ok: Option<(usize, Value)>,
    /// The last definite refusal, when nothing else is known.
    fail: Option<(usize, Value)>,
    unknown: bool,
}

/// Merge the attempts of one identity and body. The effect lies between the first invoke and
/// the first definite success.
fn logical(events: &[Event]) -> Vec<Logical> {
    let mut ops: Vec<Logical> = Vec::new();
    let mut by_body: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut open: BTreeMap<u64, usize> = BTreeMap::new();
    for (position, event) in events.iter().enumerate() {
        if event.process == NEMESIS {
            continue;
        }
        let Some(request_id) = event.request_id.clone() else {
            continue;
        };
        if event.kind == EventType::Invoke {
            let body = format!("{} {:?} {}", event.f, event.key, event.value);
            let index = *by_body
                .entry((request_id.clone(), body))
                .or_insert_with(|| {
                    ops.push(Logical {
                        f: event.f.clone(),
                        key: event.key.clone(),
                        request_id,
                        input: event.value.clone(),
                        call: position,
                        ok: None,
                        fail: None,
                        unknown: false,
                    });
                    ops.len() - 1
                });
            open.insert(event.process, index);
            continue;
        }
        let Some(index) = open.remove(&event.process) else {
            continue;
        };
        let op = &mut ops[index];
        match event.kind {
            EventType::Ok => {
                op.ok.get_or_insert((position, event.value.clone()));
            }
            EventType::Fail => op.fail = Some((position, event.value.clone())),
            _ => op.unknown = true,
        }
    }
    // A refusal other than a conflict says nothing about an earlier unknown attempt.
    for op in &mut ops {
        let conflict = op
            .fail
            .as_ref()
            .is_some_and(|(_, value)| value["code"] == json!("conflict"));
        if op.unknown && !conflict {
            op.fail = None;
        }
    }
    ops
}

/// The keyed history of the logical requests, one process each.
fn keyed(ops: &[Logical]) -> Vec<Event> {
    let mut events: Vec<(usize, Event)> = Vec::new();
    for (process, op) in ops.iter().enumerate() {
        if op.key.is_none() {
            continue;
        }
        let event = |kind, value: Value| {
            let mut event = Event::new(kind, process as u64, &op.f).with_value(value);
            event.key.clone_from(&op.key);
            event.request_id = Some(op.request_id.clone());
            event
        };
        events.push((op.call, event(EventType::Invoke, op.input.clone())));
        match (&op.ok, &op.fail) {
            (Some((at, value)), _) => events.push((*at, event(EventType::Ok, value.clone()))),
            // A refused upsert, update or delete is an identity conflict. The identity rule
            // judges it; the key's state does not constrain it.
            (None, Some((at, value))) if matches!(op.f.as_str(), "insert" | "insert_many") => {
                events.push((*at, event(EventType::Fail, value.clone())));
            }
            (None, Some((at, value))) if value["code"] != json!("conflict") => {
                events.push((*at, event(EventType::Fail, value.clone())));
            }
            (None, Some((at, _))) => {
                let fail = json!({"not_applied": true, "code": "refused"});
                events.push((*at, event(EventType::Fail, fail)));
            }
            (None, None) => {}
        }
    }
    sequence(events)
}

fn sequence(mut events: Vec<(usize, Event)>) -> Vec<Event> {
    events.sort_by_key(|(position, event)| (*position, event.kind == EventType::Invoke));
    events
        .into_iter()
        .enumerate()
        .map(|(index, (_, mut event))| {
            event.index = index;
            event
        })
        .collect()
}

/// `table/key` as the history qualifies it, without the table.
fn plain(key: &str) -> Option<i64> {
    key.rsplit('/').next()?.parse().ok()
}

fn deletable(key: i64) -> bool {
    (DELETED_BASE..DELETED_BASE + DELETED).contains(&key)
}

/// The transactional history: single-row writes and gets, batches, and whole-table queries.
fn transactions(ops: &[Logical]) -> Vec<Event> {
    let mut universe: BTreeSet<i64> = BTreeSet::new();
    for op in ops {
        if op.f == "insert_many" {
            universe.extend(rows(&op.input).into_iter().map(|(key, _)| key));
        } else if op.f != "query.collect"
            && let Some(key) = op.key.as_deref().and_then(plain)
            && !deletable(key)
        {
            universe.insert(key);
        }
    }
    let mut events = Vec::new();
    for (process, op) in ops.iter().enumerate() {
        let Some((invoke, done)) = mops(op, &universe) else {
            continue;
        };
        let event = |kind, value: Value| {
            let mut event = Event::new(kind, process as u64, "txn").with_value(value);
            event.request_id = Some(op.request_id.clone());
            event
        };
        events.push((op.call, event(EventType::Invoke, invoke.clone())));
        match (&op.ok, &op.fail) {
            (Some((at, _)), _) => events.push((*at, event(EventType::Ok, done))),
            (None, Some((at, _))) => events.push((*at, event(EventType::Fail, invoke))),
            (None, None) => {}
        }
    }
    sequence(events)
}

fn rows(input: &Value) -> Vec<(i64, i64)> {
    input
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| Some((row["id"].as_i64()?, row["value"].as_i64()?)))
        .collect()
}

/// The micro-operations of one logical request, as invoked and as completed.
fn mops(op: &Logical, universe: &BTreeSet<i64>) -> Option<(Value, Value)> {
    let key = op.key.as_deref().and_then(plain);
    let result = op.ok.as_ref().map(|(_, value)| value);
    let both = |mops: Vec<Value>| Some((Value::Array(mops.clone()), Value::Array(mops)));
    match op.f.as_str() {
        "get" => {
            let key = key.filter(|key| !deletable(*key))?;
            let value = result.map_or(Value::Null, |row| row["value"].clone());
            Some((json!([["r", key, null]]), json!([["r", key, value]])))
        }
        "query.collect" => {
            let found: BTreeMap<i64, Value> = result
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|row| Some((row["id"].as_i64()?, row["value"].clone())))
                .collect();
            let read = |value: Option<&Value>, key: i64| {
                json!(["r", key, value.cloned().unwrap_or(Value::Null)])
            };
            let invoke = universe.iter().map(|key| read(None, *key)).collect();
            let done = universe
                .iter()
                .map(|key| read(found.get(key), *key))
                .collect();
            Some((Value::Array(invoke), Value::Array(done)))
        }
        "upsert" => both(vec![json!(["w", key?, op.input["value"]])]),
        "update" => {
            let key = key?;
            if result == Some(&Value::Bool(false)) {
                return both(vec![json!(["r", key, null])]);
            }
            both(vec![json!(["w", key, op.input["value"]])])
        }
        "insert" => {
            let key = key.filter(|key| !deletable(*key))?;
            both(vec![
                json!(["r", key, null]),
                json!(["w", key, op.input["value"]]),
            ])
        }
        "insert_many" => {
            let rows = rows(&op.input);
            let mut mops: Vec<Value> = rows
                .iter()
                .map(|(key, _)| json!(["r", key, null]))
                .collect();
            mops.extend(rows.iter().map(|(key, value)| json!(["w", key, value])));
            both(mops)
        }
        _ => None,
    }
}

/// The transaction checker may print another witness cycle for the same history, and the
/// violation is part of the hashed trace. Keep one anomaly per kind and print the witnesses.
fn stable(verdict: Verdict) -> Verdict {
    let mut kinds = BTreeMap::new();
    for anomaly in verdict.anomalies {
        eprintln!("fault model {}: {}", anomaly.kind, anomaly.detail);
        kinds.entry(anomaly.kind).or_insert(());
    }
    let anomalies = kinds
        .into_keys()
        .map(|kind| Anomaly {
            detail: format!("the transactional history has a {kind}; its witness is in the log"),
            kind,
            events: Vec::new(),
        })
        .collect();
    Verdict::new(verdict.checked_ops, anomalies)
}

/// Every history check of the workload (design 3.8).
pub(crate) fn check(events: &[Event]) -> Verdict {
    let ops = logical(events);
    let keyed = keyed(&ops);
    Verdict::merge([
        stable(txn::check(&transactions(&ops))),
        linearizable::check_kv(&keyed),
        outcome::check(&keyed),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(
        kind: EventType,
        process: u64,
        f: &str,
        key: Option<i64>,
        id: u128,
        value: Value,
    ) -> Event {
        let mut event = Event::new(kind, process, f).with_value(value);
        event.key = key.map(|key| format!("t/{key}"));
        event.request_id = Some(Uuid::from_u128(id).to_string());
        event
    }

    fn pair(
        process: u64,
        f: &str,
        key: Option<i64>,
        id: u128,
        input: Value,
        result: Value,
    ) -> Vec<Event> {
        vec![
            event(EventType::Invoke, process, f, key, id, input),
            event(EventType::Ok, process, f, key, id, result),
        ]
    }

    fn row(key: i64, value: i64) -> Value {
        json!({"id": key, "value": value})
    }

    fn batch(id: u128, process: u64, values: [i64; 2]) -> Vec<Event> {
        let input = json!([row(10_000, values[0]), row(510_000, values[1])]);
        pair(process, "insert_many", Some(10_000), id, input, json!(2))
    }

    fn all(id: u128, process: u64, rows: Value) -> Vec<Event> {
        pair(process, "query.collect", None, id, Value::Null, rows)
    }

    #[test]
    fn a_batch_is_visible_as_a_whole() {
        let mut legal = all(1, 1, json!([]));
        legal.extend(batch(2, 2, [1, 2]));
        legal.extend(all(3, 3, json!([row(10_000, 1), row(510_000, 2)])));
        let verdict = check(&legal);
        assert!(verdict.anomalies.is_empty(), "{:?}", verdict.anomalies);

        // A query that overlaps the batch sees one of its rows.
        let mut torn = vec![event(
            EventType::Invoke,
            2,
            "insert_many",
            Some(10_000),
            2,
            json!([row(10_000, 1), row(510_000, 2)]),
        )];
        torn.extend(all(3, 3, json!([row(10_000, 1)])));
        torn.push(event(
            EventType::Ok,
            2,
            "insert_many",
            Some(10_000),
            2,
            json!(2),
        ));
        assert!(!check(&torn).anomalies.is_empty());

        // A query after it misses it.
        let mut stale = batch(2, 2, [1, 2]);
        stale.extend(all(3, 3, json!([])));
        assert!(!check(&stale).anomalies.is_empty());
    }

    #[test]
    fn an_accepted_request_and_its_receipt_are_one_operation() {
        let invoke = event(EventType::Invoke, 1, "upsert", Some(1), 1, row(1, 10));
        let mut accepted = event(
            EventType::Info,
            1,
            "upsert",
            Some(1),
            1,
            json!({"reason": "accepted"}),
        );
        accepted.key = invoke.key.clone();
        let mut events = vec![invoke, accepted];
        // Visible before the receipt says so.
        events.extend(pair(2, "get", Some(1), 2, Value::Null, row(1, 10)));
        events.extend(pair(3, "upsert", Some(1), 1, row(1, 10), Value::Null));
        events.extend(pair(4, "get", Some(1), 3, Value::Null, row(1, 10)));
        let verdict = check(&events);
        assert!(verdict.anomalies.is_empty(), "{:?}", verdict.anomalies);
        // Settled, and then not there.
        events.extend(pair(5, "get", Some(1), 4, Value::Null, Value::Null));
        assert!(!check(&events).anomalies.is_empty());
    }

    #[test]
    fn a_refusal_after_an_unknown_attempt_stays_open() {
        let attempt = |process, kind, value: Value| {
            vec![
                event(EventType::Invoke, process, "upsert", Some(1), 1, row(1, 10)),
                event(kind, process, "upsert", Some(1), 1, value),
            ]
        };
        let refused = json!({"not_applied": true, "code": "storage_unavailable"});
        let seen = pair(9, "get", Some(1), 9, Value::Null, row(1, 10));
        let mut open = attempt(1, EventType::Info, json!({}));
        open.extend(attempt(2, EventType::Fail, refused.clone()));
        open.extend(seen.clone());
        assert!(check(&open).anomalies.is_empty());
        // A first and only attempt that was refused has no effect.
        let mut fail = attempt(1, EventType::Fail, refused);
        fail.extend(seen);
        assert!(!check(&fail).anomalies.is_empty());
    }

    #[test]
    fn receipts_follow_the_public_contract() {
        let (run, id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let body = |state: &str, result: Value, error: Value| json!({"request_id": id, "run_id": run, "state": state, "result": result, "error": error});
        let parse = |body: Value| receipt(&body, run, id);
        assert_eq!(
            parse(body("running", Value::Null, Value::Null)),
            Ok((Seen::Pending, None))
        );
        assert_eq!(
            parse(body("succeeded", json!(1), Value::Null)),
            Ok((Seen::Terminal(Terminal::Succeeded(json!(1))), None))
        );
        assert_eq!(
            parse(body("abandoned", Value::Null, Value::Null)),
            Ok((Seen::Abandoned, None))
        );
        let error = json!({"code": "conflict", "message": "m", "outcome": "not_applied"});
        let (seen, failed) = parse(body("failed", Value::Null, error)).unwrap();
        assert_eq!(seen, Seen::Terminal(Terminal::failed("conflict")));
        assert_eq!(failed.unwrap().outcome, Applied::NotApplied);
        let error = json!({"code": "storage_unavailable", "message": "m", "outcome": "unknown"});
        let (seen, _) = parse(body("failed", Value::Null, error)).unwrap();
        assert!(matches!(&seen, Seen::Terminal(terminal) if !terminal.decided()));
        assert!(parse(body("running", json!(1), Value::Null)).is_err());
        assert!(parse(body("failed", Value::Null, Value::Null)).is_err());
        assert!(parse(body("bogus", Value::Null, Value::Null)).is_err());
        assert!(receipt(&body("running", Value::Null, Value::Null), id, run).is_err());
        assert!(parse(json!({"state": "running"})).is_err());
    }

    /// Decision 17: 10 of 15 rolling restarts completed nothing (review 132, M5).
    #[test]
    fn a_window_without_work_is_not_progress() {
        let work = |writes, reads, settled| Work {
            attempts: 9,
            writes,
            reads,
            settled,
            gap_ms: 0,
        };
        assert!(progress("chaos window", &work(1, 1, 1)).is_empty());
        assert_eq!(progress("chaos window", &work(0, 0, 0)).len(), 3);
        assert_eq!(
            progress("chaos window", &work(1, 1, 0)),
            ["accepted request settled"]
        );
        assert!(progress("rolling restart", &work(1, 1, 0)).is_empty());
        assert_eq!(
            progress("rolling restart", &work(0, 1, 0)),
            ["write committed"]
        );
    }

    #[test]
    fn only_a_lost_connection_is_a_cut() {
        assert!(cut(
            "HTTP reply: connection closed before message completed"
        ));
        assert!(!cut("connect n101: refused"));
        assert!(!cut("HTTP n101 /v1/requests deadline"));
    }

    #[test]
    fn modes_cover_both_protocols_and_both_switches() {
        let names: BTreeSet<_> = (1..=64)
            .map(|seed| Mode::of(seed).unwrap().name())
            .collect();
        assert_eq!(
            names,
            BTreeSet::from(["legacy-off", "legacy-on", "scoped-off", "scoped-on"])
        );
    }

    /// The setup already split the range that the shared action would split.
    #[test]
    fn the_shared_split_is_left_out_and_nothing_else() {
        let selection = Selection::parse(&[]).unwrap();
        for seed in 0..4 {
            let all = selection.profile(seed);
            let kept = without_split(selection.profile(seed));
            let left: Vec<_> = all
                .actions()
                .filter(|(action, _)| !kept.actions().any(|(other, _)| other == *action))
                .map(|(action, _)| *action)
                .collect();
            assert_eq!(left, [Action::Split]);
        }
    }
}
