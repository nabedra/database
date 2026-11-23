//! Per-key register linearizability (OLTP-G02 for single-key operations).
//!
//! Each key is an independent register, so by P-compositionality the history is linearizable
//! iff every per-key subhistory is. Each subhistory is searched with the Wing and Gong algorithm
//! with Lowe's memoization: a set of `(linearized ops bitset, register state)` pairs already
//! explored, which prunes the exponential search to the reachable configurations.
//!
//! Operations, by `f`:
//!
//! | f | invoke value | ok completion value | register effect |
//! | --- | --- | --- | --- |
//! | `read`, `get` | ignored | the value read, `null` when absent | reads |
//! | `write`, `set`, `put`, `upsert` | the value written | ignored | writes |
//! | `cas` | `[expected, new]` | ignored | compare and set |
//! | `insert` | the value | ignored | sets from absent only |
//! | `insert_many` | an array of rows | ignored | an `insert` of each row's key |
//! | `update` | the value | the existence flag | sets only when present |
//! | `delete`, `del` | ignored | the existence flag | removes |
//!
//! A value may be wrapped as `{"value": v, ...}`, so a row `{"id": k, "value": u}` compares as
//! `u`. An `insert_many` row is keyed by its `key` field, or else by its `id` (a number becomes
//! its decimal text), and the event's `key` is the request's anchor key, the smallest one.
//! Histories over several tables use table-qualified keys, such as `accounts/7`, both as event
//! keys and as row keys.
//!
//! The existence flag is the direct path's answer for `update` and `delete`: `true` when the row
//! existed (and was changed), `false` when it did not (nothing was written). It is the
//! completion value itself or its `data` field. Without it, an `update` writes only when the
//! key is present and a `delete` constrains nothing about the state before it.
//!
//! `fail` ops are dropped, with one exception: an `insert`, or an `insert_many` of one row,
//! refused with the error code `conflict` reads "present" at a point inside its interval,
//! unless another request body under the same request ID and anchor key could have caused the
//! conflict (the direct path's request ID reuse refusal, D03). The error code is the
//! completion value's `code` or `error` string, or `error.code`.
//!
//! An `info` write, cas, insert, update or delete may take effect at any point after its
//! invoke, or never; an `info` read constrains nothing and is dropped. Ops with other `f`
//! values are ignored.
//!
//! [`Options::request_dedup`] adds the direct path's request identity (OLTP-G03): the dedup
//! record is keyed by table, anchor key and request ID, and a retry with the same body returns
//! the original result. Attempts that share request ID, anchor key, `f` and invoke value are
//! one logical operation. It takes effect at most once, between the first attempt's invoke and
//! the first `ok` answer that proves it committed. An attempt answered without a commit (an
//! `update` or `delete` of a missing row, or an insert `conflict`) ran before the commit, since
//! later attempts replay the stored result. A history in which a retry applied again, or
//! answered from fresh state after the commit, is then not linearizable.

use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Op, Outcome, pair, payload};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Upper bound on memoized configurations per key before the checker gives up and reports a
/// `check-limit` anomaly instead of passing.
pub const DEFAULT_MAX_CONFIGS: usize = 2_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub max_configs: usize,
    /// Treat attempts of one direct-path request identity as one logical operation.
    pub request_dedup: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_configs: DEFAULT_MAX_CONFIGS,
            request_dedup: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegOp {
    Read(u32),
    /// Reads any present value.
    Present,
    Write(u32),
    Cas(u32, u32),
    /// Writes when present; fails when absent.
    Update(u32),
    /// Writes when present; does nothing when absent.
    UpdateAny(u32),
    Delete,
    /// Removes a present value; fails when absent.
    DeleteExisting,
}

impl RegOp {
    fn step(self, state: u32) -> Option<u32> {
        match self {
            RegOp::Read(v) => (v == state).then_some(state),
            RegOp::Present => (state != 0).then_some(state),
            RegOp::Write(v) => Some(v),
            RegOp::Cas(old, new) => (old == state).then_some(new),
            RegOp::Update(v) => (state != 0).then_some(v),
            RegOp::UpdateAny(v) => Some(if state == 0 { 0 } else { v }),
            RegOp::Delete => Some(0),
            RegOp::DeleteExisting => (state != 0).then_some(0),
        }
    }

    fn is_read(self) -> bool {
        matches!(self, RegOp::Read(_) | RegOp::Present)
    }
}

#[derive(Clone, Debug)]
struct KeyOp {
    op: RegOp,
    call: usize,
    /// `None` for an op that may take effect at any later point, or never.
    ret: Option<usize>,
    /// Index into the paired ops, for reports.
    src: usize,
    /// Other attempts merged into this logical op.
    more: Vec<usize>,
    /// Index of the key op this one must be linearized before, if that one takes effect.
    before: Option<usize>,
}

pub fn check_kv(events: &[Event]) -> Verdict {
    check_kv_opts(events, &Options::default())
}

pub fn check_kv_with(events: &[Event], max_configs: usize) -> Verdict {
    check_kv_opts(
        events,
        &Options {
            max_configs,
            ..Options::default()
        },
    )
}

pub fn check_kv_opts(events: &[Event], opts: &Options) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let ops = &paired.ops;
    let ids = Identities::new(ops);
    let mut per_key: BTreeMap<String, Vec<Pre>> = BTreeMap::new();
    for (i, op) in ops.iter().enumerate() {
        match project(op, i, &ids) {
            Ok(pres) => {
                for (key, pre) in pres {
                    per_key.entry(key).or_default().push(pre);
                }
            }
            Err(e) => return Verdict::malformed(e),
        }
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for (key, pres) in per_key {
        let mut interner = Interner::default();
        let key_ops = build(&pres, ops, &ids, opts.request_dedup, &mut interner);
        checked += key_ops.len();
        match search(&key_ops, None, opts.max_configs) {
            Search::Linearizable => {}
            Search::Limit => anomalies.push(Anomaly::new(
                "check-limit",
                format!(
                    "key {key:?}: gave up after {} configurations; not proven linearizable",
                    opts.max_configs
                ),
                key_ops.iter().flat_map(|o| indices(o, ops)).collect(),
            )),
            Search::NotLinearizable => {
                anomalies.push(explain(&key, &key_ops, ops, &interner, opts.max_configs));
            }
        }
    }
    Verdict::new(checked, anomalies)
}

pub(crate) fn classify(f: &str) -> Option<&'static str> {
    match f {
        "read" | "get" => Some("read"),
        "write" | "set" | "put" | "upsert" => Some("write"),
        "cas" => Some("cas"),
        "insert" => Some("insert"),
        "insert_many" => Some("insert_many"),
        "update" => Some("update"),
        "delete" | "del" => Some("delete"),
        _ => None,
    }
}

/// The error code of a refusal: the completion value's `code` or `error` string, or
/// `error.code`.
pub(crate) fn error_code(op: &Op) -> Option<&str> {
    let v = op.complete_value.as_object()?;
    if let Some(c) = v.get("code").and_then(Value::as_str) {
        return Some(c);
    }
    match v.get("error")? {
        Value::String(s) => Some(s),
        Value::Object(e) => e.get("code").and_then(Value::as_str),
        _ => None,
    }
}

/// The existence flag of an `ok` `update` or `delete`: the completion value or its `data`.
pub(crate) fn existed(op: &Op) -> Option<bool> {
    match &op.complete_value {
        Value::Bool(b) => Some(*b),
        Value::Object(m) => m.get("data").and_then(Value::as_bool),
        _ => None,
    }
}

/// For an `ok` mutation: `Some(true)` when the answer shows it committed (or replayed a
/// commit), `Some(false)` when it shows nothing was written, `None` when it does not say.
pub(crate) fn committed_answer(op: &Op) -> Option<bool> {
    if op.outcome != Outcome::Ok {
        return None;
    }
    match classify(&op.f)? {
        "write" | "cas" | "insert" | "insert_many" => Some(true),
        "update" | "delete" => existed(op),
        _ => None,
    }
}

/// The rows of an `insert_many` invoke value, as `(key, row)`, first occurrence of each key.
/// `None` unless it is a non-empty array of rows that all have a key.
pub(crate) fn rows_of(v: &Value) -> Option<Vec<(String, &Value)>> {
    let items = v.as_array()?;
    let mut out: Vec<(String, &Value)> = Vec::with_capacity(items.len());
    for row in items {
        let key = match row.get("key").or_else(|| row.get("id"))? {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return None,
        };
        if !out.iter().any(|(k, _)| *k == key) {
            out.push((key, row));
        }
    }
    (!out.is_empty()).then_some(out)
}

fn rows(op: &Op) -> Result<Vec<(String, &Value)>, String> {
    rows_of(&op.invoke_value).ok_or_else(|| {
        format!(
            "insert_many at event {} needs an array of rows keyed by key or id",
            op.invoke_index
        )
    })
}

/// The key a request's dedup record lives under: the event key, or for an `insert_many`
/// without one, its smallest row key.
pub(crate) fn anchor(op: &Op) -> Option<String> {
    if let Some(k) = &op.key {
        return Some(k.clone());
    }
    if op.f == "insert_many" {
        return rows(op).ok()?.into_iter().map(|(k, _)| k).min();
    }
    None
}

/// What a request's identity and body are made of on the direct path.
pub(crate) fn body(op: &Op) -> String {
    format!("{}\u{0}{}", op.f, canon(&op.invoke_value))
}

/// Mutations indexed by `(anchor key, request ID)`.
pub(crate) struct Identities<'a> {
    ops: &'a [Op],
    pub(crate) by_id: BTreeMap<(String, &'a str), Vec<usize>>,
}

impl<'a> Identities<'a> {
    pub(crate) fn new(ops: &'a [Op]) -> Self {
        let mut by_id: BTreeMap<(String, &'a str), Vec<usize>> = BTreeMap::new();
        for (i, op) in ops.iter().enumerate() {
            let Some(rid) = op.request_id.as_deref() else {
                continue;
            };
            if matches!(classify(&op.f), None | Some("read")) {
                continue;
            }
            if let Some(a) = anchor(op) {
                by_id.entry((a, rid)).or_default().push(i);
            }
        }
        Identities { ops, by_id }
    }

    fn identity(&self, i: usize) -> Option<(String, &'a str, String)> {
        let op = &self.ops[i];
        let rid = op.request_id.as_deref()?;
        Some((anchor(op)?, rid, body(op)))
    }

    /// True when another body under op `i`'s request ID and anchor key may have held a dedup
    /// record before `i` was answered, so a `conflict` refusal of `i` may be the reuse refusal.
    pub(crate) fn reuse_explains(&self, i: usize) -> bool {
        let op = &self.ops[i];
        let (Some(rid), Some(a)) = (op.request_id.as_deref(), anchor(op)) else {
            return false;
        };
        let mine = body(op);
        self.by_id
            .get(&(a, rid))
            .into_iter()
            .flatten()
            .map(|&j| &self.ops[j])
            .any(|w| w.outcome != Outcome::Fail && w.call < op.ret_pos() && body(w) != mine)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// A mutation that took effect (`ok`) or may have (`info`).
    Effect,
    /// An answer that wrote nothing and was decided from fresh state, so in dedup mode it
    /// precedes its identity's commit.
    Precedes,
    /// Neither: reads and answers that do not say whether they wrote.
    Plain,
}

#[derive(Clone, Debug)]
enum Shape {
    Read(Value),
    Present,
    Write(Value),
    Cas(Value, Value),
    Update(Value),
    UpdateAny(Value),
    Delete,
    DeleteExisting,
}

#[derive(Clone, Debug)]
struct Pre {
    src: usize,
    shape: Shape,
    /// The op completed with a definite answer, so it must be linearized.
    must: bool,
    role: Role,
}

/// The per-key projections of one op.
fn project(op: &Op, src: usize, ids: &Identities) -> Result<Vec<(String, Pre)>, String> {
    let Some(class) = classify(&op.f) else {
        return Ok(vec![]);
    };
    let must = op.outcome == Outcome::Ok;
    let pre = |shape: Shape, must: bool, role: Role| Pre {
        src,
        shape,
        must,
        role,
    };
    let conflict = || {
        op.outcome == Outcome::Fail
            && error_code(op) == Some("conflict")
            && !ids.reuse_explains(src)
    };
    if class == "insert_many" {
        if op.outcome == Outcome::Fail {
            // A refusal of several rows says only that some key was present.
            return Ok(match rows(op) {
                Ok(rows) if rows.len() == 1 && conflict() => {
                    vec![(rows[0].0.clone(), pre(Shape::Present, true, Role::Precedes))]
                }
                _ => vec![],
            });
        }
        return Ok(rows(op)?
            .into_iter()
            .map(|(k, row)| {
                let shape = Shape::Cas(Value::Null, payload(row).clone());
                (k, pre(shape, must, Role::Effect))
            })
            .collect());
    }
    let present = class == "insert" && conflict();
    if op.outcome == Outcome::Fail && !present {
        return Ok(vec![]);
    }
    let Some(key) = op.key.clone() else {
        return Err(format!("{} at event {} has no key", op.f, op.invoke_index));
    };
    let value = || payload(&op.invoke_value).clone();
    let p = match (class, op.outcome) {
        (_, Outcome::Fail) => pre(Shape::Present, true, Role::Precedes),
        ("read", Outcome::Ok) => pre(
            Shape::Read(payload(&op.complete_value).clone()),
            true,
            Role::Plain,
        ),
        ("read", _) => return Ok(vec![]),
        ("write", _) => pre(Shape::Write(value()), must, Role::Effect),
        ("cas", _) => {
            let args = payload(&op.invoke_value)
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| format!("cas at event {} needs [expected, new]", op.invoke_index))?;
            pre(
                Shape::Cas(args[0].clone(), args[1].clone()),
                must,
                Role::Effect,
            )
        }
        ("insert", _) => pre(Shape::Cas(Value::Null, value()), must, Role::Effect),
        ("update", Outcome::Info) => pre(Shape::Update(value()), false, Role::Effect),
        ("update", _) => match existed(op) {
            Some(true) => pre(Shape::Update(value()), true, Role::Effect),
            Some(false) => pre(Shape::Read(Value::Null), true, Role::Precedes),
            None => pre(Shape::UpdateAny(value()), true, Role::Plain),
        },
        ("delete", Outcome::Info) => pre(Shape::Delete, false, Role::Effect),
        ("delete", _) => match existed(op) {
            Some(true) => pre(Shape::DeleteExisting, true, Role::Effect),
            Some(false) => pre(Shape::Read(Value::Null), true, Role::Precedes),
            None => pre(Shape::Delete, true, Role::Plain),
        },
        _ => return Ok(vec![]),
    };
    Ok(vec![(key, p)])
}

/// Turns one key's projections into key ops, merging the attempts of one request identity
/// when `dedup` is set.
fn build(
    pres: &[Pre],
    ops: &[Op],
    ids: &Identities,
    dedup: bool,
    interner: &mut Interner,
) -> Vec<KeyOp> {
    let mut out: Vec<KeyOp> = Vec::with_capacity(pres.len());
    let mut groups: HashMap<(String, &str, String), usize> = HashMap::new();
    let mut to_reg = |shape: &Shape| match shape {
        Shape::Read(v) => RegOp::Read(interner.id(v)),
        Shape::Present => RegOp::Present,
        Shape::Write(v) => RegOp::Write(interner.id(v)),
        Shape::Cas(a, b) => RegOp::Cas(interner.id(a), interner.id(b)),
        Shape::Update(v) => RegOp::Update(interner.id(v)),
        Shape::UpdateAny(v) => RegOp::UpdateAny(interner.id(v)),
        Shape::Delete => RegOp::Delete,
        Shape::DeleteExisting => RegOp::DeleteExisting,
    };
    for pre in pres.iter().filter(|p| p.role == Role::Effect) {
        let src = &ops[pre.src];
        let op = to_reg(&pre.shape);
        let ret = if pre.must { src.ret } else { None };
        let id = if dedup { ids.identity(pre.src) } else { None };
        if let Some(id) = id {
            if let Some(&g) = groups.get(&id) {
                let merged = &mut out[g];
                merged.call = merged.call.min(src.call);
                merged.more.push(pre.src);
                if let Some(r) = ret {
                    // The committing answer fixes the op (a committed delete removed a row).
                    merged.op = op;
                    merged.ret = Some(merged.ret.map_or(r, |m| m.min(r)));
                }
                continue;
            }
            groups.insert(id, out.len());
        }
        out.push(KeyOp {
            op,
            call: src.call,
            ret,
            src: pre.src,
            more: vec![],
            before: None,
        });
    }
    for pre in pres.iter().filter(|p| p.role != Role::Effect) {
        let src = &ops[pre.src];
        let before = if dedup && pre.role == Role::Precedes {
            ids.identity(pre.src)
                .and_then(|id| groups.get(&id).copied())
        } else {
            None
        };
        out.push(KeyOp {
            op: to_reg(&pre.shape),
            call: src.call,
            ret: if pre.must { src.ret } else { None },
            src: pre.src,
            more: vec![],
            before,
        });
    }
    out
}

fn indices(o: &KeyOp, ops: &[Op]) -> Vec<usize> {
    std::iter::once(o.src)
        .chain(o.more.iter().copied())
        .flat_map(|i| ops[i].indices())
        .collect()
}

#[derive(Default)]
struct Interner {
    ids: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    fn id(&mut self, v: &Value) -> u32 {
        if v.is_null() {
            return 0;
        }
        let c = canon(v);
        if let Some(&id) = self.ids.get(&c) {
            return id;
        }
        self.names.push(c.clone());
        let id = self.names.len() as u32;
        self.ids.insert(c, id);
        id
    }

    fn name(&self, id: u32) -> &str {
        if id == 0 {
            "nil"
        } else {
            &self.names[id as usize - 1]
        }
    }
}

enum Search {
    Linearizable,
    NotLinearizable,
    Limit,
}

const NONE: usize = usize::MAX;

fn bit(set: &[u64], i: usize) -> bool {
    set[i / 64] & (1 << (i % 64)) != 0
}

/// Wing and Gong search with memoization. With `cutoff = Some(t)`, only ops invoked at or
/// before `t` are considered and ok ops returning after `t` are treated as unknown, which is the
/// history prefix up to `t`.
fn search(ops: &[KeyOp], cutoff: Option<usize>, max_configs: usize) -> Search {
    // Entries: (position, op index, is_call). Node 0 is the head sentinel.
    let mut entries: Vec<(usize, usize, bool)> = Vec::new();
    let mut must = 0usize;
    for (i, op) in ops.iter().enumerate() {
        if cutoff.is_some_and(|t| op.call > t) {
            continue;
        }
        let ret = op.ret.filter(|&r| cutoff.is_none_or(|t| r <= t));
        if ret.is_none() && op.op.is_read() {
            continue; // an unknown read constrains nothing
        }
        entries.push((op.call, i, true));
        if let Some(r) = ret {
            entries.push((r, i, false));
            must += 1;
        }
    }
    if must == 0 {
        return Search::Linearizable;
    }
    entries.sort_unstable_by_key(|e| e.0);
    let n = entries.len() + 1;
    let mut prev = vec![NONE; n];
    let mut next = vec![NONE; n];
    let mut node_op = vec![NONE; n];
    let mut is_call = vec![false; n];
    let mut matched = vec![NONE; n]; // call node -> return node
    let mut ret_node_of_op: HashMap<usize, usize> = HashMap::new();
    for (j, &(_, op, call)) in entries.iter().enumerate() {
        let node = j + 1;
        prev[node] = j;
        next[j] = node;
        node_op[node] = op;
        is_call[node] = call;
        if !call {
            ret_node_of_op.insert(op, node);
        }
    }
    for node in 1..n {
        if is_call[node]
            && let Some(&r) = ret_node_of_op.get(&node_op[node])
        {
            matched[node] = r;
        }
    }
    let unlink = |prev: &mut Vec<usize>, next: &mut Vec<usize>, x: usize| {
        let (p, nx) = (prev[x], next[x]);
        next[p] = nx;
        if nx != NONE {
            prev[nx] = p;
        }
    };
    let relink = |prev: &mut Vec<usize>, next: &mut Vec<usize>, x: usize| {
        let (p, nx) = (prev[x], next[x]);
        next[p] = x;
        if nx != NONE {
            prev[nx] = x;
        }
    };

    let words = ops.len().div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut state = 0u32;
    let mut cache: HashSet<(Vec<u64>, u32)> = HashSet::new();
    let mut stack: Vec<(usize, u32)> = Vec::new(); // (call node, state before)
    let mut done_must = 0usize;
    let mut entry = next[0];
    loop {
        if done_must == must {
            return Search::Linearizable;
        }
        if entry == NONE {
            // Cannot happen for a well-formed list (a pending must-op return lies ahead), but
            // treat it as a dead end.
            match stack.pop() {
                None => return Search::NotLinearizable,
                Some((call, s)) => {
                    entry = undo(
                        call,
                        s,
                        &mut state,
                        &mut linearized,
                        &matched,
                        &node_op,
                        &mut done_must,
                        &mut prev,
                        &mut next,
                        &relink,
                    );
                    continue;
                }
            }
        }
        if is_call[entry] {
            let op = node_op[entry];
            // An answer decided from fresh state cannot follow its identity's commit.
            let ordered = ops[op].before.is_none_or(|b| !bit(&linearized, b));
            if ordered && let Some(new_state) = ops[op].op.step(state) {
                let mut lin = linearized.clone();
                lin[op / 64] |= 1 << (op % 64);
                if cache.insert((lin.clone(), new_state)) {
                    if cache.len() > max_configs {
                        return Search::Limit;
                    }
                    stack.push((entry, state));
                    state = new_state;
                    linearized = lin;
                    unlink(&mut prev, &mut next, entry);
                    if matched[entry] != NONE {
                        unlink(&mut prev, &mut next, matched[entry]);
                        done_must += 1;
                    }
                    entry = next[0];
                    continue;
                }
            }
            entry = next[entry];
        } else {
            match stack.pop() {
                None => return Search::NotLinearizable,
                Some((call, s)) => {
                    entry = undo(
                        call,
                        s,
                        &mut state,
                        &mut linearized,
                        &matched,
                        &node_op,
                        &mut done_must,
                        &mut prev,
                        &mut next,
                        &relink,
                    );
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn undo(
    call: usize,
    s: u32,
    state: &mut u32,
    linearized: &mut [u64],
    matched: &[usize],
    node_op: &[usize],
    done_must: &mut usize,
    prev: &mut Vec<usize>,
    next: &mut Vec<usize>,
    relink: &impl Fn(&mut Vec<usize>, &mut Vec<usize>, usize),
) -> usize {
    *state = s;
    let op = node_op[call];
    linearized[op / 64] &= !(1 << (op % 64));
    if matched[call] != NONE {
        relink(prev, next, matched[call]);
        *done_must -= 1;
    }
    relink(prev, next, call);
    next[call]
}

/// Finds the shortest failing history prefix and reports the op whose completion makes it
/// fail, the ops concurrent with it, and the last write that completed before it was invoked.
fn explain(
    key: &str,
    key_ops: &[KeyOp],
    ops: &[Op],
    interner: &Interner,
    max_configs: usize,
) -> Anomaly {
    let mut rets: Vec<usize> = key_ops.iter().filter_map(|o| o.ret).collect();
    rets.sort_unstable();
    // The full history fails, so the last return is a failing cutoff. Binary search the first.
    let (mut lo, mut hi) = (0usize, rets.len() - 1);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match search(key_ops, Some(rets[mid]), max_configs) {
            Search::NotLinearizable => hi = mid,
            _ => lo = mid + 1,
        }
    }
    let t = rets[lo];
    let culprit = key_ops
        .iter()
        .find(|o| o.ret == Some(t))
        .expect("cutoff is a return");
    let mut window: Vec<&KeyOp> = key_ops
        .iter()
        .filter(|o| o.call <= t && o.ret.is_none_or(|r| r >= culprit.call))
        .collect();
    if let Some(last_write) = key_ops
        .iter()
        .filter(|o| !o.op.is_read() && o.ret.is_some_and(|r| r < culprit.call))
        .max_by_key(|o| o.ret)
    {
        window.push(last_write);
    }
    window.sort_by_key(|o| o.call);
    window.dedup_by_key(|o| o.src);
    let describe = |o: &KeyOp| -> String {
        let src = &ops[o.src];
        let v = |id: u32| interner.name(id).to_string();
        let what = match o.op {
            RegOp::Read(x) => format!("read {}", v(x)),
            RegOp::Present => "conflict (present)".to_string(),
            RegOp::Write(x) => format!("write {}", v(x)),
            RegOp::Cas(a, b) if a == 0 && src.f.starts_with("insert") => {
                format!("insert {}", v(b))
            }
            RegOp::Cas(a, b) => format!("cas {}->{}", v(a), v(b)),
            RegOp::Update(x) => format!("update {}", v(x)),
            RegOp::UpdateAny(x) => format!("update {} (existence unknown)", v(x)),
            RegOp::Delete => "delete".to_string(),
            RegOp::DeleteExisting => "delete (existed)".to_string(),
        };
        let what = if o.op == RegOp::Read(0) && matches!(src.f.as_str(), "update" | "delete") {
            format!("{} of a missing row", src.f)
        } else {
            what
        };
        let status = if o.ret.is_some() { "ok" } else { "info" };
        let status = if src.outcome == Outcome::Fail {
            "fail"
        } else {
            status
        };
        let attempts = if o.more.is_empty() {
            String::new()
        } else {
            format!(
                ", request {:?} with {} attempts",
                src.request_id.as_deref().unwrap_or(""),
                o.more.len() + 1
            )
        };
        format!(
            "p{} {what} ({status}, event {}{attempts})",
            src.process, src.invoke_index
        )
    };
    let detail = format!(
        "key {key:?}: no linearization once {} completes; window: {}",
        describe(culprit),
        window
            .iter()
            .map(|o| describe(o))
            .collect::<Vec<_>>()
            .join("; ")
    );
    Anomaly::new(
        "non-linearizable",
        detail,
        window.iter().flat_map(|o| indices(o, ops)).collect(),
    )
}
