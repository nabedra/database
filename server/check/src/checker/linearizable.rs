//! Per-key register linearizability (OLTP-G02 for single-key operations).
//!
//! Each key is an independent register, so by P-compositionality the history is linearizable
//! iff every per-key subhistory is. Each subhistory is searched with the Wing and Gong algorithm
//! with Lowe's memoization: a set of `(linearized ops bitset, register state)` pairs already
//! explored, which prunes the exponential search to the reachable configurations.
//!
//! Operations, by `f`:
//!
//! | f | invoke value | ok completion value |
//! | --- | --- | --- |
//! | `read`, `get` | ignored | the value read, `null` when absent |
//! | `write`, `set`, `put` | the value written | ignored |
//! | `cas` | `[expected, new]` | ignored |
//! | `delete`, `del` | ignored | ignored |
//!
//! A value may be wrapped as `{"value": v, ...}`; only `v` is compared. `fail` ops are dropped.
//! An `info` write, cas or delete may take effect at any point after its invoke, or never; an
//! `info` read constrains nothing and is dropped. Ops with other `f` values are ignored.

use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Op, Outcome, pair, payload};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Upper bound on memoized configurations per key before the checker gives up and reports a
/// `check-limit` anomaly instead of passing.
pub const DEFAULT_MAX_CONFIGS: usize = 2_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegOp {
    Read(u32),
    Write(u32),
    Cas(u32, u32),
    Delete,
}

impl RegOp {
    fn step(self, state: u32) -> Option<u32> {
        match self {
            RegOp::Read(v) => (v == state).then_some(state),
            RegOp::Write(v) => Some(v),
            RegOp::Cas(old, new) => (old == state).then_some(new),
            RegOp::Delete => Some(0),
        }
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
}

pub fn check_kv(events: &[Event]) -> Verdict {
    check_kv_with(events, DEFAULT_MAX_CONFIGS)
}

pub fn check_kv_with(events: &[Event], max_configs: usize) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let ops = &paired.ops;
    let mut per_key: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, op) in ops.iter().enumerate() {
        if classify(&op.f).is_none() || op.outcome == Outcome::Fail {
            continue;
        }
        let Some(k) = op.key.as_deref() else {
            return Verdict::malformed(format!("{} at event {} has no key", op.f, op.invoke_index));
        };
        if op.f == "cas"
            && payload(&op.invoke_value)
                .as_array()
                .is_none_or(|args| args.len() != 2)
        {
            return Verdict::malformed(format!(
                "cas at event {} needs [expected, new]",
                op.invoke_index
            ));
        }
        per_key.entry(k).or_default().push(i);
    }
    let mut anomalies = Vec::new();
    let mut checked = 0;
    for (key, idxs) in per_key {
        let mut interner = Interner::default();
        let key_ops: Vec<KeyOp> = idxs
            .iter()
            .filter_map(|&i| to_key_op(&ops[i], i, &mut interner))
            .collect();
        checked += key_ops.len();
        match search(&key_ops, None, max_configs) {
            Search::Linearizable => {}
            Search::Limit => anomalies.push(Anomaly::new(
                "check-limit",
                format!(
                    "key {key:?}: gave up after {max_configs} configurations; not proven linearizable"
                ),
                idxs.iter().flat_map(|&i| ops[i].indices()).collect(),
            )),
            Search::NotLinearizable => {
                anomalies.push(explain(key, &key_ops, ops, &interner, max_configs));
            }
        }
    }
    Verdict::new(checked, anomalies)
}

fn classify(f: &str) -> Option<&'static str> {
    match f {
        "read" | "get" => Some("read"),
        "write" | "set" | "put" => Some("write"),
        "cas" => Some("cas"),
        "delete" | "del" => Some("delete"),
        _ => None,
    }
}

#[derive(Default)]
struct Interner {
    ids: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    fn id(&mut self, v: &serde_json::Value) -> u32 {
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

fn to_key_op(op: &Op, src: usize, interner: &mut Interner) -> Option<KeyOp> {
    if op.outcome == Outcome::Fail {
        return None;
    }
    let reg = match classify(&op.f)? {
        "read" => {
            if op.outcome != Outcome::Ok {
                return None;
            }
            RegOp::Read(interner.id(payload(&op.complete_value)))
        }
        "write" => RegOp::Write(interner.id(payload(&op.invoke_value))),
        "cas" => {
            let v = payload(&op.invoke_value);
            let pair = v.as_array().filter(|a| a.len() == 2)?;
            RegOp::Cas(interner.id(&pair[0]), interner.id(&pair[1]))
        }
        _ => RegOp::Delete,
    };
    Some(KeyOp {
        op: reg,
        call: op.call,
        ret: if op.outcome == Outcome::Ok {
            op.ret
        } else {
            None
        },
        src,
    })
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
fn search(ops: &[KeyOp], cutoff: Option<usize>, max_configs: usize) -> Search {
    // Entries: (position, op index, is_call). Node 0 is the head sentinel.
    let mut entries: Vec<(usize, usize, bool)> = Vec::new();
    let mut must = 0usize;
    for (i, op) in ops.iter().enumerate() {
        if cutoff.is_some_and(|t| op.call > t) {
            continue;
        }
        let ret = op.ret.filter(|&r| cutoff.is_none_or(|t| r <= t));
        if ret.is_none() && matches!(op.op, RegOp::Read(_)) {
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
            if let Some(new_state) = ops[op].op.step(state) {
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
        .filter(|o| !matches!(o.op, RegOp::Read(_)) && o.ret.is_some_and(|r| r < culprit.call))
        .max_by_key(|o| o.ret)
    {
        window.push(last_write);
    }
    window.sort_by_key(|o| o.call);
    window.dedup_by_key(|o| o.src);
    let describe = |o: &KeyOp| -> String {
        let src = &ops[o.src];
        let what = match o.op {
            RegOp::Read(v) => format!("read {}", interner.name(v)),
            RegOp::Write(v) => format!("write {}", interner.name(v)),
            RegOp::Cas(a, b) => format!("cas {}->{}", interner.name(a), interner.name(b)),
            RegOp::Delete => "delete".to_string(),
        };
        let status = if o.ret.is_some() { "ok" } else { "info" };
        format!(
            "p{} {what} ({status}, event {})",
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
        window.iter().flat_map(|o| ops[o.src].indices()).collect(),
    )
}
