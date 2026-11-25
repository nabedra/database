//! Public HTTP clients of the direct workload, with retry semantics and request-ID reuse.
//!
//! Each logical client runs its pre-drawn program until the chaos window closes. An attempt
//! with an unknown outcome (a deadline, a cut reply, `outcome_unknown`) is retried with the
//! same request ID and body until it resolves or the convergence budget ends. A program may
//! retry through the other gateway (D02) and reuse an earlier request ID with another body on
//! the same and on another key (D03). A success reached by a retry is replayed through the
//! other gateway and must return the original result (D02, D04, O10). After any definite
//! refusal of a write the client reads the key: finding the refused request's unique value
//! is `NotAppliedNeverCommitted` (OLTP-G04). Sending another body under the refused identity
//! would not prove a commit, because a request that fell back to the queued path leaves a
//! forward marker that also refuses the other body.

use std::{
    collections::BTreeMap,
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

use super::clauses::{Answer, Attempt, Resolved};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS},
    client::operation,
    history::{Client, Completion, History, Operation},
    invariants::{Invariants, elapsed_ms},
};

const OPERATIONS: &str = "/v1/operations";
/// Extra client time past the server's request deadline, so deadline answers arrive.
const REPLY_GRACE: Duration = Duration::from_secs(1);
/// Pause between a definite refusal and the identity check that follows it.
const PROBE_PAUSE: Duration = Duration::from_millis(500);
/// A count takes the queued path, whose acceptance test allows 30 s.
const COUNT_LIMIT: Duration = Duration::from_secs(30);

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
    /// Every completed attempt, in completion order.
    pub attempts: Vec<Attempt>,
    /// Mutations whose retries ended in a success that was then replayed.
    pub resolved: BTreeMap<Uuid, Resolved>,
    pub reuse_same_key: u64,
    pub reuse_other_key: u64,
    /// Identity checks after a definite refusal, and how many found a commit.
    pub not_applied_checks: u64,
    /// Requests still unresolved when the convergence budget ended.
    pub unresolved: Vec<String>,
    pub completed: u64,
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
        }
    }

    fn now_ms(&self) -> u64 {
        elapsed_ms(&self.world)
    }

    /// Every client decision passes here. A trace adapter attaches to this call.
    pub(crate) fn step(&self, call: Call<'_>, step: Step<'_>) {
        let answer = match step {
            Step::Answered(completion) => answer(call.request, completion),
            Step::DeadlineExpired | Step::ConnectionLost => Answer::Unknown,
            Step::Sent | Step::Retry | Step::Replay | Step::GaveUp => return,
        };
        self.observed.lock().unwrap().attempts.push(Attempt {
            id: call.request.id,
            mutation: call.request.mutation(),
            fresh: call.fresh,
            gateway: call.gateway,
            invoked: call.invoked,
            completed: self.now_ms(),
            answer,
        });
    }

    fn again<'a>(&self, gateway: u64, request: &'a Request) -> Call<'a> {
        Call {
            gateway,
            request,
            fresh: false,
            invoked: self.now_ms(),
        }
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
    pub(crate) id: Uuid,
}

impl Request {
    fn mutation(&self) -> bool {
        self.name != "table.get"
    }

    /// Whether `value` is the one this request writes at its anchor. Deletes write none.
    fn wrote(&self, value: &Value) -> bool {
        let row = self.input.as_array().map_or(&self.input, |rows| &rows[0]);
        value.is_number() && self.mutation() && row["value"] == *value
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

    pub(crate) fn row(name: &'static str, table: &str, key: i64, value: i64, id: Uuid) -> Self {
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

/// The call a client step is about.
#[derive(Clone, Copy)]
pub(crate) struct Call<'a> {
    pub gateway: u64,
    pub request: &'a Request,
    /// A first attempt under a new identity.
    pub fresh: bool,
    pub invoked: u64,
}

/// One decision or observation of a client. Every one goes through [`Ctx::step`], so a trace
/// adapter sees the whole client protocol in one place.
pub(crate) enum Step<'a> {
    /// The request was sent through this gateway.
    Sent,
    /// The gateway answered, with a definite outcome or `outcome_unknown`.
    Answered(&'a Completion),
    /// The client's deadline expired with the call in flight.
    DeadlineExpired,
    /// The connection was refused, reset or cut before the answer arrived.
    ConnectionLost,
    /// The same ID and body is sent again after an unknown outcome.
    Retry,
    /// A resolved ID and body is sent again to compare its result.
    Replay,
    /// The client stops retrying with the outcome unknown.
    GaveUp,
}

/// Send one attempt through `gateway` and record it in the history.
pub(crate) async fn attempt(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    gateway: u64,
    request: &Request,
    fresh: bool,
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
    let started = ctx.now_ms();
    let call = Call {
        gateway,
        request,
        fresh,
        invoked: started,
    };
    ctx.step(call, Step::Sent);
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
        Ok(reply) => match reply.completion(request.id) {
            Ok(completion) => completion,
            Err(error) => {
                // A complete reply that breaks the public envelope is never just lost.
                ctx.checks.violate(
                    &ctx.world,
                    "HttpEnvelope",
                    format!(
                        "{} {} through n{gateway}: {error}",
                        request.name, request.id
                    ),
                );
                Completion::Unknown(error)
            }
        },
        Err(error) => Completion::Unknown(error),
    };
    ctx.step(
        call,
        match &completion {
            Completion::Unknown(reason) if reason.ends_with(" deadline") => Step::DeadlineExpired,
            Completion::Unknown(_) => Step::ConnectionLost,
            completion => Step::Answered(completion),
        },
    );
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

pub(crate) fn unknown(completion: &Completion) -> bool {
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

fn answer(request: &Request, completion: &Completion) -> Answer {
    match completion {
        completion if unknown(completion) => Answer::Unknown,
        Completion::Refused(error) => Answer::Refused(error.code),
        completion if committed(request, completion) => Answer::Committed,
        _ => Answer::Ok,
    }
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
) -> Completion {
    let mut gateway = first_gateway;
    let mut attempts = 0;
    loop {
        attempts += 1;
        let completion = attempt(
            ctx,
            client,
            node,
            GATEWAYS[gateway],
            request,
            fresh && attempts == 1,
        )
        .await;
        if !unknown(&completion) {
            ctx.observed.lock().unwrap().completed += 1;
            if attempts > 1
                && request.mutation()
                && let Completion::Ok(original) = &completion
            {
                let at = ctx.now_ms();
                let replayed = replay(ctx, client, node, 1 - gateway, request, original).await;
                ctx.observed.lock().unwrap().resolved.insert(
                    request.id,
                    Resolved {
                        attempts,
                        at,
                        gateway: GATEWAYS[gateway],
                        replayed,
                    },
                );
            }
            return completion;
        }
        if give_up(ctx, request, attempts) {
            return completion;
        }
        if switch {
            gateway = 1 - gateway;
        }
        ctx.step(ctx.again(GATEWAYS[gateway], request), Step::Retry);
        pause(attempts).await;
    }
}

fn give_up(ctx: &Ctx, request: &Request, attempts: u32) -> bool {
    let budget = ctx.converge_by.load(Ordering::Acquire);
    // Only the convergence budget ends retries: a long cold start takes many of them.
    let ended = ctx.now_ms() >= budget;
    if ended {
        ctx.step(ctx.again(0, request), Step::GaveUp);
        ctx.observed.lock().unwrap().unresolved.push(format!(
            "{} {} key {} after {attempts} attempts",
            request.name, request.id, request.anchor
        ));
    }
    ended
}

async fn pause(attempts: u32) {
    time::sleep(Duration::from_millis(50 * u64::from(attempts.min(10)))).await;
}

/// Send a resolved request once more through the other gateway: the same identity must
/// return the original result. Returns whether a definite answer was compared.
async fn replay(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    gateway: usize,
    request: &Request,
    original: &Value,
) -> bool {
    let mut attempts = 0;
    loop {
        attempts += 1;
        ctx.step(ctx.again(GATEWAYS[gateway], request), Step::Replay);
        let completion = attempt(ctx, client, node, GATEWAYS[gateway], request, false).await;
        let result = match &completion {
            Completion::Ok(value) => value.to_string(),
            Completion::Refused(error) if !unknown(&completion) => {
                format!("{:?} {}", error.code, error.message)
            }
            _ => {
                if give_up(ctx, request, attempts) {
                    return false;
                }
                pause(attempts).await;
                continue;
            }
        };
        // The identity committed, so `not_applied` is wrong whatever refused it. Retry to
        // compare the result as well.
        if let Completion::Refused(error) = &completion
            && matches!(
                error.code,
                ErrorCode::Overloaded | ErrorCode::StorageUnavailable
            )
        {
            // A result that changed no row committed nothing, so `not_applied` holds for it.
            if committed(request, &Completion::Ok(original.clone())) {
                ctx.checks.violate(
                &ctx.world,
                "NotAppliedNeverCommitted",
                format!(
                    "{} {} on key {} had resolved to {original}, and its replay through n{} was answered not_applied ({result})",
                    request.name, request.id, request.anchor, GATEWAYS[gateway]
                ),
            );
            }
            if give_up(ctx, request, attempts) {
                return false;
            }
            pause(attempts).await;
            continue;
        }
        if !matches!(&completion, Completion::Ok(value) if value == original) {
            ctx.checks.violate(
                &ctx.world,
                "DedupUnique",
                format!(
                    "{} {} on key {} resolved to {original} and its replay through n{} returned {result}",
                    request.name, request.id, request.anchor, GATEWAYS[gateway]
                ),
            );
        }
        return true;
    }
}

/// Run one logical client until the chaos window closes.
pub(crate) async fn run(ctx: Ctx, program: Program) {
    let node = CLIENTS[program.index % CLIENTS.len()];
    let base = (program.index as i64 + 1) * 1_000_000;
    let mut client = ctx.new_client();
    // Committed mutations whose IDs D03 may reuse: (request ID, key).
    let mut committed_ids: Vec<(Uuid, i64)> = Vec::new();
    // The program repeats until the chaos window closes, so load covers all of it.
    for (index, planned) in program.ops.iter().cycle().enumerate() {
        if ctx.stopping.load(Ordering::Acquire) {
            break;
        }
        time::sleep(planned.think).await;
        #[cfg(test)]
        crate::panics::plant(&ctx.world, crate::panics::At::Client);
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
            Kind::Update => (
                Request::update(table, planned.key, value, fresh_id),
                true,
                None,
            ),
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
        let completion = resolve(
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
            && judged_reuse(&completion)
        {
            let mut observed = ctx.observed.lock().unwrap();
            if same {
                observed.reuse_same_key += 1;
            } else {
                observed.reuse_other_key += 1;
            }
        }
        match &completion {
            Completion::Ok(_) if request.mutation() && fresh => {
                if committed(&request, &completion) {
                    committed_ids.push((request.id, request.anchor));
                }
            }
            // Every definite refusal, also one that ended a retry (design 3.7).
            Completion::Refused(error)
                if request.mutation() && fresh && error.outcome == Outcome::NotApplied =>
            {
                check_not_applied(
                    &ctx,
                    &mut client,
                    node,
                    planned.gateway,
                    &request,
                    error.code,
                )
                .await;
            }
            _ => {}
        }
    }
}

/// Whether an answer to a reused request ID is one that D03 speaks about: the same key is a
/// conflict of bodies, another key executes. The wrong success or the wrong conflict counts too,
/// and the checker rejects it. Any other refusal, such as `overloaded` or a lifecycle conflict,
/// says nothing about the reuse.
fn judged_reuse(completion: &Completion) -> bool {
    match completion {
        Completion::Ok(_) => true,
        Completion::Refused(error) => {
            error.code == ErrorCode::Conflict
                && error.message == "request_id was already used with a different request body"
        }
        Completion::Unknown(_) => false,
    }
}

/// After a definite refusal, read the key. Values are unique, so the refused value there
/// means the request had committed.
async fn check_not_applied(
    ctx: &Ctx,
    client: &mut Client,
    node: u64,
    gateway: usize,
    refused: &Request,
    code: ErrorCode,
) {
    time::sleep(PROBE_PAUSE).await;
    let id = in_node(node, async { uuid_v4() }).await;
    let read = Request::get(&ctx.table, refused.anchor, id);
    let completion = resolve(ctx, client, node, gateway, true, &read, false).await;
    if let Completion::Ok(row) = &completion {
        ctx.observed.lock().unwrap().not_applied_checks += 1;
        if refused.wrote(&row["value"]) {
            ctx.checks.violate(
                &ctx.world,
                "NotAppliedNeverCommitted",
                format!(
                    "{} {} on key {} was answered {code:?} not_applied, yet the key holds its value: {row}",
                    refused.name, refused.id, refused.anchor
                ),
            );
        }
    }
}

/// After convergence: one write through each gateway must commit, then every key is read
/// once through each gateway. Nothing is retried, so a refusal or a lost reply fails.
pub(crate) async fn final_checks(ctx: &Ctx) -> Result<(), String> {
    let mut client = ctx.new_client();
    let mut failed = Vec::new();
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        let id = in_node(node, async { uuid_v4() }).await;
        let key = 1 + index as i64;
        let request = Request::row("table.upsert", &ctx.table, key, 900_000_000 + key, id);
        let completion = attempt(ctx, &mut client, node, gateway, &request, true).await;
        if let Some(why) = failure(&completion) {
            failed.push(format!("write of key {key} through n{gateway}: {why}"));
        }
    }
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        for key in 1..=ctx.keys {
            let id = in_node(node, async { uuid_v4() }).await;
            let request = Request::get(&ctx.table, key, id);
            let completion = attempt(ctx, &mut client, node, gateway, &request, false).await;
            if let Some(why) = failure(&completion) {
                failed.push(format!("read of key {key} through n{gateway}: {why}"));
            }
        }
    }
    let total = failed.len();
    if total == 0 {
        return Ok(());
    }
    failed.truncate(3);
    Err(format!(
        "{total} final operations failed: {}",
        failed.join("; ")
    ))
}

/// One read of key 1 through each gateway, not retried.
pub(crate) async fn read_each(ctx: &Ctx) -> Result<(), String> {
    let mut client = ctx.new_client();
    for (index, gateway) in GATEWAYS.into_iter().enumerate() {
        let node = CLIENTS[index % CLIENTS.len()];
        let id = in_node(node, async { uuid_v4() }).await;
        let request = Request::get(&ctx.table, 1, id);
        let completion = attempt(ctx, &mut client, node, gateway, &request, false).await;
        if let Some(why) = failure(&completion) {
            return Err(format!("read of key 1 through n{gateway}: {why}"));
        }
    }
    Ok(())
}

/// `query.count` through one gateway against a read of every key through the other, with no
/// client running. Returns what differs.
pub(crate) async fn exact_count(ctx: &Ctx) -> Result<Option<String>, String> {
    let mut client = ctx.new_client();
    let mut rows = 0;
    for key in 1..=ctx.keys {
        let id = in_node(CLIENTS[0], async { uuid_v4() }).await;
        let request = Request::get(&ctx.table, key, id);
        match attempt(ctx, &mut client, CLIENTS[0], GATEWAYS[0], &request, false).await {
            Completion::Ok(row) => rows += i64::from(!row.is_null()),
            other => {
                let why = failure(&other).unwrap_or_default();
                return Err(format!("read of key {key}: {why}"));
            }
        }
    }
    let payload = json!({
        "table_id": ctx.table,
        "query": {"select": null, "filter": null, "order_by": null, "limit": null},
    });
    // A refused count is asked again under a new ID, as `read_after_transition` does.
    let deadline = ctx.world.elapsed() + COUNT_LIMIT;
    loop {
        let id = in_node(CLIENTS[1], async { uuid_v4() }).await;
        let answer = crate::client::request(
            CLIENTS[1],
            GATEWAYS[1],
            OPERATIONS,
            &ctx.token,
            Some(operation(id, "query.count", payload.clone())),
            COUNT_LIMIT,
        )
        .await
        .and_then(|reply| reply.completion(id))?;
        match answer {
            Completion::Ok(count) if count == json!(rows) => return Ok(None),
            Completion::Ok(count) => {
                return Ok(Some(format!(
                    "query.count answered {count} and the keys hold {rows} rows"
                )));
            }
            other if ctx.world.elapsed() >= deadline => {
                return Err(format!(
                    "query.count: {}",
                    failure(&other).unwrap_or_default()
                ));
            }
            _ => time::sleep(Duration::from_millis(500)).await,
        }
    }
}

pub(crate) fn failure(completion: &Completion) -> Option<String> {
    match completion {
        Completion::Ok(_) => None,
        Completion::Refused(error) => Some(format!("{:?} {}", error.code, error.message)),
        Completion::Unknown(reason) => Some(reason.clone()),
    }
}

/// A burst of reads and writes at one gateway, past its lanes (D10). Each operation is its
/// own process, and the writes carry unique values.
pub(crate) async fn burst(ctx: Ctx, gateway: usize, count: u64, first_key: i64) {
    let mut tasks = Vec::new();
    for offset in 0..count {
        let ctx = ctx.clone();
        tasks.push(nabedra_rt::task::spawn(async move {
            let node = CLIENTS[(offset as usize) % CLIENTS.len()];
            let key = 1 + (first_key - 1 + offset as i64) % ctx.keys;
            let id = in_node(node, async { uuid_v4() }).await;
            let mut client = ctx.new_client();
            let request = if offset % 2 == 0 {
                Request::get(&ctx.table, key, id)
            } else {
                let value = 500_000_000 + ctx.now_ms() as i64 * 1_000 + offset as i64;
                Request::row("table.upsert", &ctx.table, key, value, id)
            };
            attempt(&ctx, &mut client, node, GATEWAYS[gateway], &request, false).await;
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
}

#[cfg(test)]
mod tests {
    use nabedra_protocol::protocol::ProtocolError;

    use super::*;

    /// Review 115, L4: an `overloaded` answer counted as D03's assertion.
    #[test]
    fn only_d03s_own_answers_count_as_a_judged_reuse() {
        let refused = |code, message: &str| {
            let error = ProtocolError::new(code, message, Outcome::NotApplied);
            judged_reuse(&Completion::Refused(error))
        };
        let used = "request_id was already used with a different request body";
        assert!(refused(ErrorCode::Conflict, used));
        assert!(!refused(
            ErrorCode::Conflict,
            "captured schema or lifecycle changed"
        ));
        assert!(!refused(ErrorCode::Overloaded, "full"));
        assert!(judged_reuse(&Completion::Ok(json!(true))));
        assert!(!judged_reuse(&Completion::Unknown("deadline".into())));
    }

    /// A refused write is reported only when its own value is in the row. A conflict on the
    /// refused identity is not enough: cold seed 6 found a forward marker there while the
    /// row still held another request's value.
    #[test]
    fn only_the_refused_value_proves_a_commit() {
        let insert = Request::row("table.insert", "t", 7, 2_000_580, Uuid::nil());
        assert!(insert.wrote(&json!(2_000_580)));
        assert!(!insert.wrote(&json!(2_000_320)));
        assert!(!insert.wrote(&Value::Null));
        let many = Request::insert_many("t", 7, 10, 2, 30, Uuid::nil());
        assert!(many.wrote(&json!(10)));
        let delete = Request::delete("t", 7, Uuid::nil());
        assert!(!delete.wrote(&Value::Null));
        assert!(!Request::get("t", 7, Uuid::nil()).wrote(&Value::Null));
    }
}
