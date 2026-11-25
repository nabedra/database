//! Per-key register linearizability (OLTP-G02 for single-key operations).
//!
//! Individual keys are independent registers. In direct-path histories, a batch joins the
//! registers it touches because the rows and the request record change atomically. Each
//! independent group is searched with the Wing and Gong algorithm with Lowe's memoization.
//!
//! Operations, by `f`:
//!
//! | f | invoke value | ok completion value | register effect |
//! | --- | --- | --- | --- |
//! | `read`, `get` | ignored | the value read, `null` when absent | reads |
//! | `write`, `set`, `put`, `upsert` | the value written | ignored | writes |
//! | `cas` | `[expected, new]` | ignored | compare and set |
//! | `insert` | the value | ignored | sets from absent only |
//! | `insert_many` | an array of rows | ignored | inserts every row |
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
//! An insert conflict requires an existing row, or a record for a different body under the
//! same identity in direct-path histories. A direct-path batch conflict requires at least one
//! existing row, unless its body repeats a key or deduplication explains the refusal.
//!
//! An `info` write, cas, insert, update or delete may take effect at any point after its
//! invoke, or never; an `info` read constrains nothing and is dropped. Ops with other `f`
//! values are ignored.
//!
//! [`Options::request_dedup`] adds the direct path's request identity (OLTP-G03): the dedup
//! record is keyed by table, anchor key and request ID, and a retry with the same body returns
//! the first definite result ([`super::direct::results`]). Attempts that share request ID, anchor
//! key, `f` and invoke value are one logical mutation. It can commit between the first eligible
//! invoke and the first success that proves a commit, or read the row once and change nothing
//! between the first invoke and the first no-op or row conflict. If its answers disagree, the fresh
//! ones are judged one by one. An answer computed from fresh state requires an empty dedup slot.
//! All bodies under an identity share the slot, including those with unknown outcomes, but a no-op
//! or a row conflict does not occupy it.

use super::direct::results::{self, Definite};
use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Op, Outcome, pair, payload};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Upper bound on memoized configurations per independent key group. Large states can have
/// a lower limit so a search reports `check-limit` before it exhausts memory.
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
            RegOp::Write(v) => Some(v),
            RegOp::Cas(old, new) => (old == state).then_some(new),
            RegOp::Update(v) => (state != 0).then_some(v),
            RegOp::UpdateAny(v) => Some(if state == 0 { 0 } else { v }),
            RegOp::Delete => Some(0),
            RegOp::DeleteExisting => (state != 0).then_some(0),
        }
    }

    fn is_read(self) -> bool {
        matches!(self, RegOp::Read(_))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Effect,
    Fresh,
    Conflict,
    Invalid,
    Plain,
}

#[derive(Clone, Copy, Debug)]
struct Record {
    slot: usize,
    body: u32,
}

#[derive(Clone, Debug)]
struct KeyOp {
    rows: Vec<(usize, RegOp)>,
    record: Option<Record>,
    role: Role,
    call: usize,
    /// `None` for an op that may take effect at any later point, or never.
    ret: Option<usize>,
    /// Index into the paired ops, for reports.
    src: usize,
    /// Other attempts merged into this logical op.
    more: Vec<usize>,
}

impl KeyOp {
    fn is_read(&self) -> bool {
        matches!(self.role, Role::Conflict | Role::Fresh)
            || self.role == Role::Plain && self.rows.iter().all(|(_, op)| op.is_read())
    }

    fn slots(&self) -> impl Iterator<Item = usize> + '_ {
        self.rows
            .iter()
            .map(|&(key, _)| key)
            .chain(self.record.map(|r| r.slot))
    }

    fn step(&self, state: &[u32]) -> Option<Vec<u32>> {
        let held = self.record.map_or(0, |r| state[r.slot]);
        match self.role {
            Role::Invalid => return None,
            Role::Conflict => {
                let allowed = if let Some(r) = self.record.filter(|_| held != 0) {
                    held != r.body
                } else {
                    self.rows.iter().any(|(key, _)| state[*key] != 0)
                };
                return allowed.then(|| state.to_vec());
            }
            Role::Effect | Role::Fresh if held != 0 => return None,
            _ => {}
        }

        let mut next = state.to_vec();
        for &(key, op) in &self.rows {
            next[key] = op.step(state[key])?;
        }
        if self.role == Role::Effect
            && let Some(r) = self.record
        {
            next[r.slot] = r.body;
        }
        Some(next)
    }
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
    let mut keys = BTreeMap::new();
    let mut parents = Vec::new();
    let mut projected = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let mut pre = match project(op, i, &ids, opts.request_dedup) {
            Ok(Some(pre)) => pre,
            Ok(None) => continue,
            Err(e) => return Verdict::malformed(e),
        };
        let key_id = |k: &str, keys: &mut BTreeMap<String, usize>, parents: &mut Vec<usize>| {
            *keys.entry(k.to_owned()).or_insert_with(|| {
                let id = parents.len();
                parents.push(id);
                id
            })
        };
        if opts.request_dedup {
            let home = key_id(&pre.anchor, &mut keys, &mut parents);
            for (key, _) in &pre.rows {
                let id = key_id(key, &mut keys, &mut parents);
                let root = component(&mut parents, id);
                parents[root] = component(&mut parents, home);
            }
            projected.push(pre);
        } else if pre.rows.len() <= 1 {
            key_id(&pre.anchor, &mut keys, &mut parents);
            projected.push(pre);
        } else {
            // The generic KV check projects batches into independent registers.
            for (key, shape) in std::mem::take(&mut pre.rows) {
                key_id(&key, &mut keys, &mut parents);
                projected.push(Pre {
                    rows: vec![(key.clone(), shape)],
                    anchor: key,
                    ..pre
                });
            }
        }
    }
    let mut partitions = BTreeMap::<usize, Vec<Pre>>::new();
    for pre in projected {
        let root = component(&mut parents, keys[&pre.anchor]);
        partitions.entry(root).or_default().push(pre);
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    let without_effect = opts
        .request_dedup
        .then(|| results::without_effect(&ids, ops));
    for (_, pres) in partitions {
        let mut interner = Interner::default();
        let (key_ops, width) = build(&pres, ops, &ids, without_effect.as_ref(), &mut interner);
        let label = if pres.iter().all(|p| p.anchor == pres[0].anchor)
            && pres
                .iter()
                .flat_map(|p| &p.rows)
                .all(|(k, _)| k == &pres[0].anchor)
        {
            format!("key {:?}", pres[0].anchor)
        } else {
            "keys linked by a direct batch".to_string()
        };
        checked += key_ops.len();
        // A memo entry stores a bitset and the register state. Bound the memory as well as
        // the number of configurations when many keys or identities share a partition.
        let state_bytes = 8 * key_ops.len().div_ceil(64) + 4 * width + 64;
        let max_configs = opts.max_configs.min(128 * 1024 * 1024 / state_bytes);
        match search(&key_ops, width, None, max_configs) {
            Search::Linearizable => {}
            Search::Limit => anomalies.push(Anomaly::new(
                "check-limit",
                format!(
                    "{label}: gave up after {max_configs} configurations; not proven linearizable"
                ),
                key_ops.iter().flat_map(|o| indices(o, ops)).collect(),
            )),
            Search::NotLinearizable => {
                anomalies.push(explain(
                    &label,
                    &key_ops,
                    width,
                    ops,
                    &interner,
                    max_configs,
                ));
            }
        }
    }
    Verdict::new(checked, anomalies)
}

fn component(parents: &mut [usize], mut key: usize) -> usize {
    while parents[key] != key {
        parents[key] = parents[parents[key]];
        key = parents[key];
    }
    key
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
    code_of(&op.complete_value)
}

/// [`error_code`] of an answer.
pub(crate) fn code_of(answer: &Value) -> Option<&str> {
    let v = answer.as_object()?;
    if let Some(c) = v.get("code").and_then(Value::as_str) {
        return Some(c);
    }
    match v.get("error")? {
        Value::String(s) => Some(s),
        Value::Object(e) => e.get("code").and_then(Value::as_str),
        _ => None,
    }
}

/// The error message of an answer, wherever [`code_of`] finds its code: the value's `message`,
/// or `error.message`.
pub(crate) fn message_of(answer: &Value) -> Option<&str> {
    let v = answer.as_object()?;
    let nested = || v.get("error")?.as_object()?.get("message");
    v.get("message").or_else(nested)?.as_str()
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

/// The rows of an `insert_many` invoke value, as `(key, row)`.
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
        out.push((key, row));
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

    /// The attempts of each identity and body, in invocation order.
    pub(crate) fn requests(&self) -> BTreeMap<(String, &'a str, String), Vec<usize>> {
        let mut out = BTreeMap::<_, Vec<usize>>::new();
        for ((anchor, rid), members) in &self.by_id {
            for &i in members {
                let request = (anchor.clone(), *rid, body(&self.ops[i]));
                out.entry(request).or_default().push(i);
            }
        }
        out
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
            .any(|w| {
                (w.outcome == Outcome::Info || committed_answer(w) == Some(true))
                    && w.call < op.ret_pos()
                    && body(w) != mine
            })
    }
}

#[derive(Clone, Debug)]
enum Shape {
    Read(Value),
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
    anchor: String,
    rows: Vec<(String, Shape)>,
    role: Role,
}

fn project(op: &Op, src: usize, ids: &Identities, direct: bool) -> Result<Option<Pre>, String> {
    let Some(class) = classify(&op.f) else {
        return Ok(None);
    };
    let conflict = op.outcome == Outcome::Fail && error_code(op) == Some("conflict");
    let row_conflict = matches!(class, "insert" | "insert_many");
    if op.outcome == Outcome::Fail && !(conflict && (row_conflict || direct && class != "read")) {
        return Ok(None);
    }
    let batch = if class == "insert_many" {
        match rows(op) {
            Ok(rows) => rows,
            Err(_) if op.outcome == Outcome::Fail => return Ok(None),
            Err(e) => return Err(e),
        }
    } else {
        Vec::new()
    };
    let anchor =
        anchor(op).ok_or_else(|| format!("{} at event {} has no key", op.f, op.invoke_index))?;
    if class == "read" && op.outcome != Outcome::Ok {
        return Ok(None);
    }
    let value = || payload(&op.invoke_value).clone();
    let mut pre = Pre {
        src,
        anchor,
        rows: Vec::new(),
        role: Role::Effect,
    };
    let single = |shape| vec![(op.key.clone().expect("single-key op"), shape)];
    let batch_has_duplicate = {
        let mut seen = HashSet::new();
        batch.iter().any(|(key, _)| !seen.insert(key))
    };
    if op.outcome == Outcome::Fail {
        if batch_has_duplicate || !direct && (batch.len() > 1 || ids.reuse_explains(src)) {
            return Ok(None);
        }
        pre.role = Role::Conflict;
        // The shape is unused for a conflict. These rows are the keys it can inspect.
        pre.rows = match class {
            "insert_many" => batch
                .into_iter()
                .map(|(key, _)| (key, Shape::Delete))
                .collect(),
            "insert" => single(Shape::Delete),
            _ => Vec::new(),
        };
        return Ok(Some(pre));
    }

    pre.rows = match class {
        "read" => {
            pre.role = Role::Plain;
            single(Shape::Read(payload(&op.complete_value).clone()))
        }
        "write" => single(Shape::Write(value())),
        "cas" => {
            let args = payload(&op.invoke_value)
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| format!("cas at event {} needs [expected, new]", op.invoke_index))?;
            single(Shape::Cas(args[0].clone(), args[1].clone()))
        }
        "insert" => single(Shape::Cas(Value::Null, value())),
        "insert_many" => {
            if batch_has_duplicate && op.outcome == Outcome::Info {
                return Ok(None);
            }
            let count = op.complete_value.get("data").unwrap_or(&op.complete_value);
            if batch_has_duplicate
                || op.outcome == Outcome::Ok
                    && !count.is_null()
                    && count.as_u64() != Some(batch.len() as u64)
            {
                pre.role = Role::Invalid;
            }
            batch
                .into_iter()
                .map(|(key, row)| (key, Shape::Cas(Value::Null, payload(row).clone())))
                .collect()
        }
        "update" | "delete" => {
            let update = class == "update";
            single(match (op.outcome, existed(op)) {
                (Outcome::Ok, Some(false)) => {
                    pre.role = Role::Fresh;
                    Shape::Read(Value::Null)
                }
                (Outcome::Ok, None) if direct => {
                    pre.role = Role::Invalid;
                    Shape::Read(Value::Null)
                }
                (Outcome::Ok, None) => {
                    pre.role = Role::Plain;
                    if update {
                        Shape::UpdateAny(value())
                    } else {
                        Shape::Delete
                    }
                }
                _ => {
                    if update {
                        Shape::Update(value())
                    } else if direct || op.outcome == Outcome::Ok {
                        Shape::DeleteExisting
                    } else {
                        Shape::Delete
                    }
                }
            })
        }
        _ => unreachable!("unknown register operation {class}"),
    };
    Ok(Some(pre))
}

/// Assign registers and dedup slots within an independent key group. Merge the attempts of each
/// request: the committing ones, or all of them when its one result is a no-op or a row conflict
/// (`without_effect`). Other answers from fresh state stay separate operations.
fn build(
    pres: &[Pre],
    ops: &[Op],
    ids: &Identities,
    without_effect: Option<&HashMap<usize, Definite>>,
    interner: &mut Interner,
) -> (Vec<KeyOp>, usize) {
    let mut keys = HashMap::new();
    for (key, _) in pres.iter().flat_map(|p| &p.rows) {
        let next = keys.len();
        keys.entry(key).or_insert(next);
    }
    let mut out: Vec<KeyOp> = Vec::with_capacity(pres.len());
    let mut groups = HashMap::new();
    let mut slots = HashMap::new();
    let mut bodies = HashMap::new();
    let mut to_reg = |shape: &Shape| match shape {
        Shape::Read(v) => RegOp::Read(interner.id(v)),
        Shape::Write(v) => RegOp::Write(interner.id(v)),
        Shape::Cas(a, b) => RegOp::Cas(interner.id(a), interner.id(b)),
        Shape::Update(v) => RegOp::Update(interner.id(v)),
        Shape::UpdateAny(v) => RegOp::UpdateAny(interner.id(v)),
        Shape::Delete => RegOp::Delete,
        Shape::DeleteExisting => RegOp::DeleteExisting,
    };
    for pre in pres {
        let src = &ops[pre.src];
        let ret = src.ret;
        let record = if without_effect.is_some() && pre.role != Role::Plain {
            ids.identity(pre.src).and_then(|(anchor, rid, body)| {
                let scope = (anchor, rid);
                if ids.by_id[&scope].len() < 2 {
                    return None;
                }
                let next_slot = keys.len() + slots.len();
                let slot = *slots.entry(scope).or_insert(next_slot);
                let next_body = bodies.len() as u32 + 1;
                let body = *bodies.entry(body).or_insert(next_body);
                Some(Record { slot, body })
            })
        } else {
            None
        };
        let mut op = KeyOp {
            rows: pre
                .rows
                .iter()
                .map(|(key, shape)| (keys[key], to_reg(shape)))
                .collect(),
            record,
            role: pre.role,
            call: src.call,
            ret,
            src: pre.src,
            more: vec![],
        };
        // Attempts with an unknown outcome have the role of an effect, and share the result of
        // their request if it has none.
        let result = without_effect.and_then(|results| results.get(&pre.src));
        let merges = pre.role == Role::Effect
            || result.is_some() && results::definite_result(src) == result.copied();
        if merges && let Some(record) = record {
            let identity = (record.slot, record.body);
            if let Some(&g) = groups.get(&identity) {
                let merged: &mut KeyOp = &mut out[g];
                if merged.role == Role::Effect && op.role != Role::Effect {
                    std::mem::swap(merged, &mut op);
                }
                merged.call = merged.call.min(op.call);
                merged.ret = merged.ret.into_iter().chain(op.ret).min();
                merged.more.push(op.src);
                merged.more.append(&mut op.more);
                continue;
            }
            groups.insert(identity, out.len());
        }
        out.push(op);
    }
    (out, keys.len() + slots.len())
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

/// Wing and Gong search with memoization. With `cutoff = Some(t)`, only ops invoked at or
/// before `t` are considered and ok ops returning after `t` are treated as unknown, which is the
/// history prefix up to `t`.
fn search(ops: &[KeyOp], width: usize, cutoff: Option<usize>, max_configs: usize) -> Search {
    // If exactly one mutation can produce an observed value, it must take effect before
    // that read returns. Tighten its deadline to avoid exploring later impossible schedules.
    let in_prefix = |pos| cutoff.is_none_or(|t| pos <= t);
    let mut writers = HashMap::new();
    // Definite operations that would hide a particular value if they followed its source.
    let mut on_key = HashMap::<usize, Vec<(usize, u32, usize)>>::new();
    for (i, op) in ops.iter().enumerate() {
        if op.role == Role::Invalid || !in_prefix(op.call) {
            continue;
        }
        let ret = op.ret.filter(|&r| in_prefix(r));
        for &(key, row) in &op.rows {
            let definite_value = match (op.role, row) {
                (Role::Conflict, _) => None,
                (_, RegOp::UpdateAny(_)) => None,
                (_, RegOp::Read(v) | RegOp::Write(v) | RegOp::Cas(_, v) | RegOp::Update(v)) => {
                    Some(v)
                }
                (_, RegOp::Delete | RegOp::DeleteExisting) => Some(0),
            };
            if let (Some(r), Some(v)) = (ret, definite_value) {
                on_key.entry(key).or_default().push((i, v, r));
            }
            let written = match row {
                RegOp::Write(v) | RegOp::Cas(_, v) | RegOp::Update(v) | RegOp::UpdateAny(v) => v,
                _ => continue,
            };
            if written != 0 {
                writers
                    .entry((key, written))
                    .and_modify(|source| *source = None)
                    .or_insert(Some(i));
            }
        }
    }
    let mut deadlines: Vec<_> = ops
        .iter()
        .map(|o| o.ret.filter(|&r| in_prefix(r)))
        .collect();
    let mut prerequisites = vec![Vec::new(); ops.len()];
    for op in ops.iter().filter(|o| o.role == Role::Plain) {
        if let (Some(ret), [(key, RegOp::Read(value))]) =
            (op.ret.filter(|&r| in_prefix(r)), op.rows.as_slice())
            && let Some(Some(writer)) = writers.get(&(*key, *value))
            && ops[*writer].call < ret
        {
            let deadline = &mut deadlines[*writer];
            *deadline = Some(deadline.map_or(ret, |d| d.min(ret)));
            // If an earlier completed read saw another value, or a write left another
            // value, the unique source of this read must follow it. Real time already
            // orders predecessors that returned before the source was invoked.
            prerequisites[*writer].extend(on_key[key].iter().filter_map(
                |&(other, other_value, done)| {
                    (other_value != *value && done < op.call && done >= ops[*writer].call)
                        .then_some(other)
                },
            ));
        }
    }

    // Entries: (position, op index, is_call). Node 0 is the head sentinel.
    let mut entries: Vec<(usize, usize, bool)> = Vec::new();
    let mut must = 0usize;
    for (i, op) in ops.iter().enumerate() {
        if cutoff.is_some_and(|t| op.call > t) {
            continue;
        }
        let ret = deadlines[i];
        if ret.is_none() && op.is_read() {
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

    // Before the first pending return, only operations linked to it can matter. A disjoint
    // operation can commute past that return, even if a later operation links their keys.
    let candidates = |next: &[usize], state: &[u32]| {
        let mut calls = Vec::new();
        let mut node = next[0];
        while is_call[node] {
            calls.push(node);
            node = next[node];
        }
        if let Some(&read) = calls.iter().find(|&&call| {
            let op = &ops[node_op[call]];
            op.is_read() && op.step(state).is_some()
        }) {
            // A read that already agrees with the state can take its result now.
            return vec![read].into_iter();
        }
        let mut related = HashSet::new();
        let mut ordered = Vec::new();
        let add = |node: usize, related: &mut HashSet<_>| {
            related.extend(ops[node_op[node]].slots());
        };
        add(node, &mut related);
        loop {
            let len = ordered.len();
            calls.retain(|&call| {
                if ops[node_op[call]]
                    .slots()
                    .any(|slot| related.contains(&slot))
                    || node_op[call] == node_op[node]
                {
                    add(call, &mut related);
                    ordered.push(call);
                    false
                } else {
                    true
                }
            });
            if ordered.len() == len {
                break;
            }
        }
        let first = &ops[node_op[node]];
        ordered.sort_by_cached_key(|&call| {
            let op = &ops[node_op[call]];
            let helps = op
                .step(state)
                .is_some_and(|next| first.step(&next).is_some());
            (node_op[call] != node_op[node], !helps, matched[call])
        });
        ordered.into_iter()
    };

    let words = ops.len().div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut state = vec![0; width];
    let mut cache: HashSet<(Vec<u64>, Vec<u32>)> = HashSet::new();
    let mut stack = Vec::new();
    let mut done_must = 0;
    let mut choices = candidates(&next, &state);
    loop {
        if let Some(entry) = choices.next() {
            let op = node_op[entry];
            if prerequisites[op]
                .iter()
                .all(|&p| linearized[p / 64] & (1 << (p % 64)) != 0)
                && let Some(new_state) = ops[op].step(&state)
            {
                linearized[op / 64] |= 1 << (op % 64);
                if cache.insert((linearized.clone(), new_state.clone())) {
                    if cache.len() > max_configs {
                        return Search::Limit;
                    }
                    unlink(&mut prev, &mut next, entry);
                    if matched[entry] != NONE {
                        unlink(&mut prev, &mut next, matched[entry]);
                        done_must += 1;
                        if done_must == must {
                            return Search::Linearizable;
                        }
                    }
                    stack.push((entry, state, choices));
                    state = new_state;
                    choices = candidates(&next, &state);
                    continue;
                }
                linearized[op / 64] &= !(1 << (op % 64));
            }
            continue;
        }

        let Some((call, old_state, old_choices)) = stack.pop() else {
            return Search::NotLinearizable;
        };
        state = old_state;
        choices = old_choices;
        let op = node_op[call];
        linearized[op / 64] &= !(1 << (op % 64));
        if matched[call] != NONE {
            relink(&mut prev, &mut next, matched[call]);
            done_must -= 1;
        }
        relink(&mut prev, &mut next, call);
    }
}

/// Finds the shortest failing history prefix and reports the op whose completion makes it
/// fail, the ops concurrent with it, and the last write that completed before it was invoked.
fn explain(
    label: &str,
    key_ops: &[KeyOp],
    width: usize,
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
        match search(key_ops, width, Some(rets[mid]), max_configs) {
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
        .filter(|o| !o.is_read() && o.ret.is_some_and(|r| r < culprit.call))
        .max_by_key(|o| o.ret)
    {
        window.push(last_write);
    }
    window.sort_by_key(|o| o.call);
    window.dedup_by_key(|o| o.src);
    let describe = |o: &KeyOp| -> String {
        let src = &ops[o.src];
        let v = |id: u32| interner.name(id).to_string();
        let what = match (o.role, o.rows.as_slice()) {
            (Role::Conflict, _) => "conflict".to_string(),
            (Role::Invalid, _) => format!("{} with an invalid success result", src.f),
            (Role::Fresh, _) => format!("{} of a missing row", src.f),
            (_, [(_, op)]) => match *op {
                RegOp::Read(x) => format!("read {}", v(x)),
                RegOp::Write(x) => format!("write {}", v(x)),
                RegOp::Cas(0, b) if src.f.starts_with("insert") => format!("insert {}", v(b)),
                RegOp::Cas(a, b) => format!("cas {}->{}", v(a), v(b)),
                RegOp::Update(x) => format!("update {}", v(x)),
                RegOp::UpdateAny(x) => format!("update {} (existence unknown)", v(x)),
                RegOp::Delete => "delete".to_string(),
                RegOp::DeleteExisting => "delete (existed)".to_string(),
            },
            _ => format!("{} ({} keys)", src.f, o.rows.len()),
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
        "{label}: no linearization once {} completes; window: {}",
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
