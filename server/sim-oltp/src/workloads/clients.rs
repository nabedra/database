//! Public HTTP clients of the direct workload, with retry semantics and request-ID reuse.
//!
//! Each logical client runs its pre-drawn program until the chaos window closes. An attempt
//! with an unknown outcome (a deadline, a cut reply, `outcome_unknown`) is retried with the
//! same request ID and body until it resolves or the convergence budget ends. A program may
//! retry through the other gateway (D02) and reuse an earlier request ID with another body on
//! the same and on another key (D03). After a definite `not_applied` answer that is not a
//! row conflict, the client sends another body under the same identity: if the first had
//! committed, the dedup record refuses it as a conflict, which the client reports as
//! `NotAppliedNeverCommitted` (OLTP-G04).

use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_protocol::protocol::{ErrorCode, Outcome};
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node},
    time,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    bootstrap::{CLIENTS, GATEWAYS},
    client::operation,
    history::{Client, Completion, History, Operation},
    invariants::{Invariants, elapsed_ms},
};

const OPERATIONS: &str = "/v1/operations";
/// Extra client time past the server's request deadline, so deadline answers arrive.
const REPLY_GRACE: Duration = Duration::from_secs(1);
/// The most attempts of one request before the client gives up.
const MAX_ATTEMPTS: u32 = 64;
/// Pause between a definite refusal and the identity check that follows it.
const PROBE_PAUSE: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Get,
    Insert,
    Upsert,
    Update,
    Delete,
    InsertMany,
    /// D03: an earlier request ID with another body on the same key.
    ReuseSameKey,
    /// D03: an earlier request ID on another key.
    ReuseOtherKey,
}

#[derive(Clone, Debug)]
pub(crate) struct Planned {
    kind: Kind,
    key: i64,
    rows: i64,
    gateway: usize,
    /// Alternate gateways between retries of an unknown outcome (D02).
    switch_gateway: bool,
    think: Duration,
}

/// One logical client's program.
#[derive(Clone, Debug)]
pub(crate) struct Program {
    pub index: usize,
    ops: Vec<Planned>,
}

/// The workload's request mix and key space, drawn from the tape.
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    pub keys: i64,
    pub programs: Vec<Program>,
}

impl Plan {
    pub(crate) fn draw(g: &mut Gen, contention: bool, idle: bool) -> Self {
        let clients = if idle { 0 } else { 2 + g.below(5) as usize };
        let keys = if contention {
            2 + g.below(3) as i64
        } else {
            8 + g.below(57) as i64
        };
        let programs = (0..clients)
            .map(|index| Program {
                index,
                ops: (0..96)
                    .map(|_| {
                        let kind = match g.below(100) {
                            0..30 => Kind::Get,
                            30..44 => Kind::Insert,
                            44..58 => Kind::Upsert,
                            58..72 => Kind::Update,
                            72..82 => Kind::Delete,
                            82..90 => Kind::InsertMany,
                            90..95 => Kind::ReuseSameKey,
                            _ => Kind::ReuseOtherKey,
                        };
                        Planned {
                            kind,
                            key: 1 + g.below(keys as u64) as i64,
                            rows: 2 + g.below(3) as i64,
                            gateway: g.below(2) as usize,
                            switch_gateway: g.bool(),
                            think: Duration::from_millis(g.below(150)),
                        }
                    })
                    .collect(),
            })
            .collect();
        Self { keys, programs }
    }
}

/// What the clients observed, for scenario tags and post-run assertions.
#[derive(Default, Debug)]
pub(crate) struct Observations {
    /// Requests that saw an unknown outcome and later resolved.
    pub resolved_after_unknown: u64,
    /// Of those, requests whose retry went through another gateway (D02).
    pub resolved_across_gateways: u64,
    pub reuse_same_key: u64,
    pub reuse_other_key: u64,
    /// Identity checks after a definite refusal, and how many found a commit.
    pub not_applied_checks: u64,
    /// Requests still unresolved when the convergence budget ended.
    pub unresolved: Vec<String>,
    /// First attempts of fresh writes: (invoke ms, completion ms, committed `ok`).
    pub fresh_writes: Vec<(u64, u64, bool)>,
    pub completed: u64,
    /// Final reads answered with a refusal or an unknown outcome before they succeeded.
    pub refused_final_reads: u64,
    pub first_refusal: Option<String>,
    /// Attempts answered `overloaded` (D10, lane exhaustion).
    pub overloaded: u64,
}

/// State the clients, the nemesis and the workload share during a run.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub world: Arc<World>,
    pub checks: Invariants,
    pub history: Arc<Mutex<History>>,
    pub table: String,
    pub token: String,
    pub timeout: Duration,
    pub keys: i64,
    pub observed: Arc<Mutex<Observations>>,
    /// Set when the chaos window closes: clients start no new requests.
    pub stopping: Arc<AtomicBool>,
    /// Virtual ms by which retries must resolve; unbounded during chaos.
    pub converge_by: Arc<AtomicU64>,
    /// Mutation attempts currently awaiting an answer.
    pub writes_in_flight: Arc<AtomicU64>,
}

impl Ctx {
    pub(crate) fn new(
        world: Arc<World>,
        checks: Invariants,
        history: Arc<Mutex<History>>,
        table: String,
        token: String,
        deadline: Duration,
        keys: i64,
    ) -> Self {
        Self {
            world,
            checks,
            history,
            table,
            token,
            timeout: deadline + REPLY_GRACE,
            keys,
            observed: Arc::default(),
            stopping: Arc::default(),
            converge_by: Arc::new(AtomicU64::new(u64::MAX)),
            writes_in_flight: Arc::default(),
        }
    }

    fn now_ms(&self) -> u64 {
        elapsed_ms(&self.world)
    }

    pub(crate) fn new_client(&self) -> Client {
        self.history.lock().unwrap().client()
    }
}

/// One public request: its operation, payload, history input and identity.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    name: &'static str,
    payload: Value,
    input: Value,
    anchor: i64,
    id: Uuid,
}

impl Request {
    fn mutation(&self) -> bool {
        self.name != "table.get"
    }

    pub(crate) fn get(table: &str, key: i64, id: Uuid) -> Self {
        Self {
            name: "table.get",
            payload: json!({"table_id": table, "key": key}),
            input: Value::Null,
            anchor: key,
            id,
        }
    }

    fn row(name: &'static str, table: &str, key: i64, value: i64, id: Uuid) -> Self {
        let row = json!({"id": key, "value": value});
        Self {
            name,
            payload: json!({"table_id": table, "row": row}),
            input: row,
            anchor: key,
            id,
        }
    }

    fn update(table: &str, key: i64, value: i64, id: Uuid) -> Self {
        Self {
            name: "table.update",
            payload: json!({"table_id": table, "key": key, "values": {"value": value}}),
            input: json!({"id": key, "value": value}),
            anchor: key,
            id,
        }
    }

    fn delete(table: &str, key: i64, id: Uuid) -> Self {
        let payload = json!({"table_id": table, "key": key});
        Self {
            name: "table.delete",
            input: payload.clone(),
            payload,
            anchor: key,
            id,
        }
    }

    fn insert_many(table: &str, first: i64, value: i64, count: i64, keys: i64, id: Uuid) -> Self {
        // Distinct keys in key order; the anchor is the smallest key.
        let rows: std::collections::BTreeMap<i64, Value> = (0..count)
            .map(|offset| {
                let key = 1 + (first - 1 + offset) % keys;
                (key, json!({"id": key, "value": value + offset}))
            })
            .collect();
        let anchor = rows.keys().next().copied().unwrap_or(first);
        let rows: Vec<Value> = rows.into_values().collect();
        Self {
            name: "table.insert_many",
            payload: json!({"table_id": table, "rows": rows}),
            input: Value::Array(rows),
            anchor,
            id,
        }
    }
}

/// Send one attempt through `gateway` and record it in the history.
pub(crate) async fn attempt(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    gateway: u64,
    request: &Request,
) -> Completion {
    let key = request.anchor.to_string();
    let invoked = {
        let mut history = ctx.history.lock().unwrap();
        history.invoke(
            client,
            &ctx.world,
            gateway,
            Operation {
                table: Some(&ctx.table),
                name: request.name,
                key: Some(&key),
                request_id: request.id,
                value: request.input.clone(),
            },
        )
    };
    if request.mutation() {
        ctx.writes_in_flight.fetch_add(1, Ordering::AcqRel);
    }
    let completion = match crate::client::request(
        node,
        gateway,
        OPERATIONS,
        &ctx.token,
        Some(operation(request.id, request.name, request.payload.clone())),
        ctx.timeout,
    )
    .await
    {
        Ok(reply) => reply
            .completion(request.id)
            .unwrap_or_else(Completion::Unknown),
        Err(error) => Completion::Unknown(error),
    };
    if request.mutation() {
        ctx.writes_in_flight.fetch_sub(1, Ordering::AcqRel);
    }
    if let Completion::Refused(error) = &completion
        && error.code == ErrorCode::Overloaded
    {
        ctx.observed.lock().unwrap().overloaded += 1;
    }
    let recorded = match &completion {
        Completion::Ok(value) => Completion::Ok(value.clone()),
        Completion::Refused(error) => Completion::Refused(error.clone()),
        Completion::Unknown(reason) => Completion::Unknown(reason.clone()),
    };
    ctx.history
        .lock()
        .unwrap()
        .complete(client, &ctx.world, invoked, recorded);
    completion
}

fn unknown(completion: &Completion) -> bool {
    match completion {
        Completion::Ok(_) => false,
        Completion::Refused(error) => error.outcome == Outcome::Unknown,
        Completion::Unknown(_) => true,
    }
}

/// Whether an `ok` answer committed: `update` and `delete` answer the existence flag.
fn committed(request: &Request, completion: &Completion) -> bool {
    match completion {
        Completion::Ok(Value::Bool(flag)) => *flag,
        Completion::Ok(_) => request.mutation(),
        _ => false,
    }
}

/// The result of one logical request after its retries.
struct Resolved {
    completion: Completion,
    attempts: u32,
}

/// Retry `request` with the same ID and body while its outcome is unknown.
async fn resolve(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    first_gateway: usize,
    switch: bool,
    request: &Request,
    fresh: bool,
) -> Resolved {
    let mut gateway = first_gateway;
    let mut gateways = BTreeSet::new();
    let mut saw_unknown = false;
    let mut attempts = 0;
    loop {
        attempts += 1;
        gateways.insert(gateway);
        let invoked = ctx.now_ms();
        let completion = attempt(ctx, client, node, GATEWAYS[gateway], request).await;
        if fresh && attempts == 1 && request.mutation() {
            ctx.observed.lock().unwrap().fresh_writes.push((
                invoked,
                ctx.now_ms(),
                committed(request, &completion),
            ));
        }
        if !unknown(&completion) {
            let mut observed = ctx.observed.lock().unwrap();
            observed.completed += 1;
            if saw_unknown {
                observed.resolved_after_unknown += 1;
                if gateways.len() > 1 {
                    observed.resolved_across_gateways += 1;
                }
            }
            return Resolved {
                completion,
                attempts,
            };
        }
        saw_unknown = true;
        let budget = ctx.converge_by.load(Ordering::Acquire);
        if attempts >= MAX_ATTEMPTS || ctx.now_ms() >= budget {
            ctx.observed.lock().unwrap().unresolved.push(format!(
                "{} {} key {} after {attempts} attempts",
                request.name, request.id, request.anchor
            ));
            return Resolved {
                completion,
                attempts,
            };
        }
        if switch {
            gateway = 1 - gateway;
        }
        let pause = Duration::from_millis(50 * u64::from(attempts.min(10)));
        time::sleep(pause).await;
    }
}

/// Run one logical client until the chaos window closes.
pub(crate) async fn run(ctx: Ctx, program: Program) {
    let node = CLIENTS[program.index % CLIENTS.len()];
    let base = (program.index as i64 + 1) * 1_000_000;
    let mut client = ctx.new_client();
    // Committed mutations whose IDs D03 may reuse: (request ID, key).
    let mut committed_ids: Vec<(Uuid, i64)> = Vec::new();
    for (index, planned) in program.ops.iter().enumerate() {
        if ctx.stopping.load(Ordering::Acquire) {
            break;
        }
        time::sleep(planned.think).await;
        let value = base + index as i64 * 10;
        let table = &ctx.table;
        let fresh_id = in_node(node, async { uuid_v4() }).await;
        let (request, fresh, reuse) = match planned.kind {
            Kind::Get => (Request::get(table, planned.key, fresh_id), true, None),
            Kind::Insert => (
                Request::row("table.insert", table, planned.key, value, fresh_id),
                true,
                None,
            ),
            Kind::Upsert => (
                Request::row("table.upsert", table, planned.key, value, fresh_id),
                true,
                None,
            ),
            Kind::Update => (Request::update(table, planned.key, value, fresh_id), true, None),
            Kind::Delete => (Request::delete(table, planned.key, fresh_id), true, None),
            Kind::InsertMany => (
                Request::insert_many(table, planned.key, value, planned.rows, ctx.keys, fresh_id),
                true,
                None,
            ),
            Kind::ReuseSameKey | Kind::ReuseOtherKey => {
                let Some(&(id, key)) = committed_ids.last() else {
                    continue;
                };
                let same = planned.kind == Kind::ReuseSameKey;
                let target = if same {
                    key
                } else {
                    1 + (key + planned.key - 1) % ctx.keys.max(2)
                };
                if !same && target == key {
                    continue;
                }
                (
                    Request::row("table.upsert", table, target, value, id),
                    false,
                    Some(same),
                )
            }
        };
        let resolved = resolve(
            &ctx,
            &mut client,
            node,
            planned.gateway,
            planned.switch_gateway,
            &request,
            fresh,
        )
        .await;
        if let Some(same) = reuse
            && !unknown(&resolved.completion)
        {
            let mut observed = ctx.observed.lock().unwrap();
            if same {
                observed.reuse_same_key += 1;
            } else {
                observed.reuse_other_key += 1;
            }
        }
        match &resolved.completion {
            completion @ Completion::Ok(_) if request.mutation() && fresh => {
                if committed(&request, completion) {
                    committed_ids.push((request.id, request.anchor));
                }
            }
            Completion::Refused(error)
                if request.mutation()
                    && fresh
                    && resolved.attempts == 1
                    && error.outcome == Outcome::NotApplied
                    && !matches!(
                        error.code,
                        ErrorCode::Conflict
                            | ErrorCode::ValidationError
                            | ErrorCode::NotFound
                            | ErrorCode::UnsupportedOperation
                            | ErrorCode::LimitExceeded
                    ) =>
            {
                check_not_applied(&ctx, &mut client, node, planned.gateway, &request, value, error.code)
                    .await;
            }
            _ => {}
        }
    }
}

/// After a definite refusal, send another body under the same identity. A dedup record
/// of the refused request makes the product refuse it as a conflict.
async fn check_not_applied(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    gateway: usize,
    refused: &Request,
    value: i64,
    code: ErrorCode,
) {
    time::sleep(PROBE_PAUSE).await;
    let probe = Request::row("table.upsert", &ctx.table, refused.anchor, value + 5, refused.id);
    let resolved = resolve(ctx, client, node, gateway, true, &probe, false).await;
    ctx.observed.lock().unwrap().not_applied_checks += 1;
    if let Completion::Refused(error) = &resolved.completion
        && error.code == ErrorCode::Conflict
    {
        ctx.checks.violate(
            &ctx.world,
            "NotAppliedNeverCommitted",
            format!(
                "{} {} on key {} was answered {code:?} not_applied, yet its identity holds a dedup record: another body under it was refused with conflict ({})",
                refused.name, refused.id, refused.anchor, error.message
            ),
        );
    }
}

/// How long a final read may be refused before it counts as a liveness failure.
const FINAL_READ_LIMIT: Duration = Duration::from_secs(5);

/// Read every key through both gateways after convergence. A refused read is retried for
/// a short while, so a stale route shows up in `refused_final_reads` rather than as loss.
pub(crate) async fn final_reads(ctx: &Ctx) -> Result<(), String> {
    let mut client = ctx.new_client();
    for gateway in 0..GATEWAYS.len() {
        for key in 1..=ctx.keys {
            let node = CLIENTS[gateway % CLIENTS.len()];
            let deadline = ctx.world.elapsed() + FINAL_READ_LIMIT;
            loop {
                let id = in_node(node, async { uuid_v4() }).await;
                let request = Request::get(&ctx.table, key, id);
                let resolved =
                    resolve(ctx, &mut client, node, gateway, false, &request, false).await;
                let refusal = match &resolved.completion {
                    Completion::Refused(error) => {
                        let text = format!("{:?} {}", error.code, error.message);
                        if !matches!(
                            error.code,
                            ErrorCode::StorageUnavailable | ErrorCode::Overloaded
                        ) {
                            return Err(format!(
                                "final read of key {key} through n{}: {text}",
                                GATEWAYS[gateway]
                            ));
                        }
                        text
                    }
                    Completion::Ok(_) => break,
                    Completion::Unknown(reason) => reason.clone(),
                };
                let mut observed = ctx.observed.lock().unwrap();
                observed.refused_final_reads += 1;
                if observed.first_refusal.is_none() {
                    observed.first_refusal = Some(format!(
                        "key {key} through n{}: {refusal}",
                        GATEWAYS[gateway]
                    ));
                }
                drop(observed);
                if ctx.world.elapsed() >= deadline {
                    return Err(format!(
                        "final read of key {key} through n{} refused for {FINAL_READ_LIMIT:?}: {refusal}",
                        GATEWAYS[gateway]
                    ));
                }
                time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Ok(())
}

/// A burst of reads at one gateway, past its lanes (D10). Each read is its own process.
pub(crate) async fn burst(ctx: Ctx, gateway: usize, count: u64, first_key: i64) {
    let mut tasks = Vec::new();
    for offset in 0..count {
        let ctx = ctx.clone();
        tasks.push(nabedra_rt::task::spawn(async move {
            let node = CLIENTS[(offset as usize) % CLIENTS.len()];
            let key = 1 + (first_key - 1 + offset as i64) % ctx.keys;
            let id = in_node(node, async { uuid_v4() }).await;
            let mut client = ctx.new_client();
            attempt(&ctx, &mut client, node, GATEWAYS[gateway], &Request::get(&ctx.table, key, id))
                .await;
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
}
