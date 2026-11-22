//! An Elle-style transactional checker for batches and queued transactions (OLTP-G02).
//!
//! Ops with `f == "txn"` carry Jepsen micro-ops as their value. The invoke has the writes and
//! `null` reads; an `ok` completion has the same micro-ops with the reads filled in:
//!
//! | Micro-op | Meaning |
//! | --- | --- |
//! | `["append", k, v]` | list-append: append the unique `v` to the list at `k` |
//! | `["r", k, [..]]` | list-append read: the whole list (`null` means empty) |
//! | `["w", k, v]` | rw-register: write the unique `v` |
//! | `["r", k, v]` | rw-register read: `null` is the initial version |
//!
//! A key is a list key when any append or any list-valued read touches it, and a register key
//! otherwise. Both kinds may appear in one history.
//!
//! Version order comes from the reads. For list keys the longest read gives it, and every read
//! must be a prefix of it (else `incompatible-order`). For register keys the only sound
//! inferences are that the initial `null` precedes every write, and that a transaction that
//! reads `v1` and then writes `v2` to the same key orders `v1` before `v2`.
//!
//! The dependency graph has write-write (ww), write-read (wr), read-write (rw, anti-dependency)
//! and real-time (rt) edges between `ok` transactions and the `info` transactions whose writes
//! were observed. Cycles are classified as Adya does, checking the cheapest class first in each
//! strongly connected component: `G0` (ww only), `G1c` (ww and wr), `G-single` (exactly one rw),
//! `G2` (two or more rw). A cycle that needs a real-time edge gets the `-realtime` suffix, which
//! violates strict serializability but not serializability. `G1a` (a read of a failed write),
//! `G1b` (a read of a non-final write of a transaction) and `garbage-read` (a read of a value
//! nobody wrote) are checked directly.

use super::{Anomaly, Verdict, canon};
use crate::history::{Event, Op, Outcome, pair};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

const WW: u8 = 1;
const WR: u8 = 2;
const RW: u8 = 4;
const RT: u8 = 8;
const ALL: u8 = WW | WR | RW | RT;

#[derive(Clone, Debug)]
enum Mop {
    Append(String, Value),
    Write(String, Value),
    Read(String, Value),
}

struct Txn<'a> {
    op: &'a Op,
    mops: Vec<Mop>,
}

fn key_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn parse_mops(v: &Value) -> Option<Vec<Mop>> {
    let arr = v.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for m in arr {
        let m = m.as_array().filter(|m| m.len() == 3)?;
        let k = key_str(&m[1]);
        let v = m[2].clone();
        out.push(match m[0].as_str()? {
            "append" => Mop::Append(k, v),
            "w" => Mop::Write(k, v),
            "r" => Mop::Read(k, v),
            _ => return None,
        });
    }
    Some(out)
}

/// Where a `(key, value)` write came from.
#[derive(Clone, Copy)]
struct WriteSite {
    txn: usize,
    /// True when this is the transaction's last write to the key.
    last: bool,
}

pub fn check(events: &[Event]) -> Verdict {
    let paired = match pair(events) {
        Ok(p) => p,
        Err(e) => return Verdict::malformed(e),
    };
    let mut txns: Vec<Txn> = Vec::new();
    let mut anomalies = Vec::new();
    for op in paired.ops.iter().filter(|o| o.f == "txn") {
        match parse_mops(op.value()) {
            Some(mops) => txns.push(Txn { op, mops }),
            None => {
                return Verdict::malformed(format!(
                    "event {}: txn value is not a list of [f, k, v] micro-ops",
                    op.invoke_index
                ));
            }
        }
    }

    // Classify keys, index writes, collect observed values.
    let mut list_keys: HashSet<String> = HashSet::new();
    for t in &txns {
        for m in &t.mops {
            match m {
                Mop::Append(k, _) => {
                    list_keys.insert(k.clone());
                }
                Mop::Read(k, v) if v.is_array() => {
                    list_keys.insert(k.clone());
                }
                _ => {}
            }
        }
    }
    let mut writes: HashMap<(String, String), WriteSite> = HashMap::new();
    for (ti, t) in txns.iter().enumerate() {
        let mut last_write: HashMap<&str, String> = HashMap::new();
        for m in &t.mops {
            if let Mop::Append(k, v) | Mop::Write(k, v) = m {
                last_write.insert(k, canon(v));
            }
        }
        for m in &t.mops {
            if let Mop::Append(k, v) | Mop::Write(k, v) = m {
                let c = canon(v);
                let last = last_write.get(k.as_str()) == Some(&c);
                writes
                    .entry((k.clone(), c))
                    .or_insert(WriteSite { txn: ti, last });
            }
        }
    }
    // Reads that count: those of ok transactions, excluding reads of a key the transaction
    // itself wrote earlier (internal reads).
    let mut reads: Vec<(usize, String, Value)> = Vec::new(); // (txn, key, value)
    for (ti, t) in txns.iter().enumerate() {
        if t.op.outcome != Outcome::Ok {
            continue;
        }
        let mut written: HashSet<&str> = HashSet::new();
        for m in &t.mops {
            match m {
                Mop::Append(k, _) | Mop::Write(k, _) => {
                    written.insert(k);
                }
                Mop::Read(k, v) if !written.contains(k.as_str()) => {
                    reads.push((ti, k.clone(), v.clone()));
                }
                Mop::Read(..) => {}
            }
        }
    }
    let mut observed_info: HashSet<usize> = HashSet::new();
    let mut note_value = |anomalies: &mut Vec<Anomaly>, reader: usize, k: &str, v: &Value| {
        let c = canon(v);
        let reader_op = txns[reader].op;
        match writes.get(&(k.to_string(), c.clone())) {
            None => anomalies.push(Anomaly::new(
                "garbage-read",
                format!(
                    "txn at event {} read {c} from key {k:?}, which no transaction wrote",
                    reader_op.invoke_index
                ),
                reader_op.indices(),
            )),
            Some(site) => {
                let w = txns[site.txn].op;
                if site.txn == reader {
                    return;
                }
                match w.outcome {
                    Outcome::Fail => anomalies.push(Anomaly::new(
                        "G1a",
                        format!(
                            "txn at event {} read {c} from key {k:?}, written by the failed txn at event {}",
                            reader_op.invoke_index, w.invoke_index
                        ),
                        [reader_op.indices(), w.indices()].concat(),
                    )),
                    Outcome::Info => {
                        observed_info.insert(site.txn);
                    }
                    Outcome::Ok => {}
                }
            }
        }
    };
    for (ti, k, v) in &reads {
        if list_keys.contains(k) {
            if let Some(items) = v.as_array() {
                for item in items {
                    note_value(&mut anomalies, *ti, k, item);
                }
            }
        } else if !v.is_null() {
            note_value(&mut anomalies, *ti, k, v);
        }
    }

    // Graph nodes: ok transactions plus info transactions known to have committed.
    let in_graph: Vec<bool> = txns
        .iter()
        .enumerate()
        .map(|(i, t)| t.op.outcome == Outcome::Ok || observed_info.contains(&i))
        .collect();
    let mut g = Graph::new(txns.len());

    // G1b: the last thing read is a non-final write of another transaction.
    let check_intermediate = |anomalies: &mut Vec<Anomaly>, reader: usize, k: &str, v: &Value| {
        if let Some(site) = writes.get(&(k.to_string(), canon(v)))
            && site.txn != reader
            && !site.last
            && txns[site.txn].op.outcome != Outcome::Fail
        {
            let (r, w) = (txns[reader].op, txns[site.txn].op);
            anomalies.push(Anomaly::new(
                "G1b",
                format!(
                    "txn at event {} read {v} from key {k:?}, an intermediate write of the txn at event {}",
                    r.invoke_index, w.invoke_index
                ),
                [r.indices(), w.indices()].concat(),
            ));
        }
    };

    // List keys.
    let mut by_key: BTreeMap<&str, Vec<(usize, &Vec<Value>)>> = BTreeMap::new();
    static EMPTY: Vec<Value> = Vec::new();
    for (ti, k, v) in &reads {
        if list_keys.contains(k) {
            let list = v.as_array().unwrap_or(&EMPTY);
            by_key.entry(k).or_default().push((*ti, list));
        }
    }
    for (k, rs) in &by_key {
        for (ti, list) in rs {
            let mut seen = HashSet::new();
            if list.iter().any(|x| !seen.insert(canon(x))) {
                let op = txns[*ti].op;
                anomalies.push(Anomaly::new(
                    "duplicate-elements",
                    format!(
                        "txn at event {} read key {k:?} with a repeated element: {}",
                        op.invoke_index,
                        Value::Array((*list).clone())
                    ),
                    op.indices(),
                ));
            }
            if let Some(last) = list.last() {
                check_intermediate(&mut anomalies, *ti, k, last);
            }
        }
        let longest = rs
            .iter()
            .map(|r| r.1)
            .max_by_key(|l| l.len())
            .unwrap_or(&EMPTY);
        let mut compatible = true;
        for (ti, list) in rs {
            if longest[..list.len()] != list[..] {
                compatible = false;
                let a = txns[*ti].op;
                anomalies.push(Anomaly::new(
                    "incompatible-order",
                    format!(
                        "key {k:?}: read {} at event {} is not a prefix of read {}",
                        Value::Array((*list).clone()),
                        a.invoke_index,
                        Value::Array(longest.clone())
                    ),
                    a.indices(),
                ));
            }
        }
        if !compatible {
            continue;
        }
        let writer = |v: &Value| writes.get(&(k.to_string(), canon(v))).map(|s| s.txn);
        for w in longest.windows(2) {
            if let (Some(a), Some(b)) = (writer(&w[0]), writer(&w[1])) {
                g.add(a, b, WW, &in_graph);
            }
        }
        for (ti, list) in rs {
            if let Some(last) = list.last()
                && let Some(w) = writer(last)
            {
                g.add(w, *ti, WR, &in_graph);
            }
            if let Some(next) = longest.get(list.len())
                && let Some(w) = writer(next)
            {
                g.add(*ti, w, RW, &in_graph);
            }
        }
    }

    // Register keys.
    let mut reg_writers: HashMap<&str, Vec<usize>> = HashMap::new();
    for ((k, _), site) in &writes {
        if !list_keys.contains(k) && site.last {
            reg_writers.entry(k).or_default().push(site.txn);
        }
    }
    // v1 < v2 when one transaction reads v1 and then writes v2 (its final write) to the key.
    let mut successors: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (ti, t) in txns.iter().enumerate() {
        let mut read_first: HashMap<&str, &Value> = HashMap::new();
        let mut wrote: HashSet<&str> = HashSet::new();
        for m in &t.mops {
            match m {
                Mop::Read(k, v) if !list_keys.contains(k) && !wrote.contains(k.as_str()) => {
                    read_first.entry(k).or_insert(v);
                }
                Mop::Write(k, _) => {
                    wrote.insert(k);
                }
                _ => {}
            }
        }
        if t.op.outcome != Outcome::Ok {
            continue; // an info txn's reads are unknown
        }
        for (k, v) in read_first {
            if wrote.contains(k) && !v.is_null() {
                successors
                    .entry((k.to_string(), canon(v)))
                    .or_default()
                    .push(ti);
            }
        }
    }
    for (ti, k, v) in &reads {
        if list_keys.contains(k) {
            continue;
        }
        if v.is_null() {
            for &w in reg_writers.get(k.as_str()).into_iter().flatten() {
                g.add(*ti, w, RW, &in_graph);
            }
            continue;
        }
        check_intermediate(&mut anomalies, *ti, k, v);
        let key = (k.clone(), canon(v));
        let Some(site) = writes.get(&key) else {
            continue;
        };
        g.add(site.txn, *ti, WR, &in_graph);
        for &next in successors.get(&key).into_iter().flatten() {
            g.add(*ti, next, RW, &in_graph);
        }
    }
    for ((k, c), nexts) in &successors {
        if let Some(site) = writes.get(&(k.clone(), c.clone())) {
            for &n in nexts {
                g.add(site.txn, n, WW, &in_graph);
            }
        }
    }

    // Real-time edges, transitively reduced with Elle's frontier method.
    let mut points: Vec<(usize, bool, usize)> = Vec::new(); // (pos, is_completion, txn)
    for (i, t) in txns.iter().enumerate() {
        if in_graph[i] {
            points.push((t.op.call, false, i));
            if let Some(r) = t.op.ret {
                points.push((r, true, i));
            }
        }
    }
    points.sort_unstable();
    let mut frontier: Vec<usize> = Vec::new();
    let mut at_invoke: HashMap<usize, Vec<usize>> = HashMap::new();
    for (_, done, t) in points {
        if done {
            let before = at_invoke.remove(&t).unwrap_or_default();
            frontier.retain(|x| !before.contains(x));
            frontier.push(t);
        } else {
            for &f in &frontier {
                g.add(f, t, RT, &in_graph);
            }
            at_invoke.insert(t, frontier.clone());
        }
    }

    anomalies.extend(g.cycles(&txns));
    Verdict::new(txns.len(), anomalies)
}

struct Graph {
    edges: HashMap<(usize, usize), u8>,
    adj: Vec<Vec<usize>>,
}

impl Graph {
    fn new(n: usize) -> Self {
        Graph {
            edges: HashMap::new(),
            adj: vec![Vec::new(); n],
        }
    }

    fn add(&mut self, a: usize, b: usize, kind: u8, in_graph: &[bool]) {
        if a == b || !in_graph[a] || !in_graph[b] {
            return;
        }
        let e = self.edges.entry((a, b)).or_insert(0);
        if *e == 0 {
            self.adj[a].push(b);
        }
        *e |= kind;
    }

    fn bits(&self, a: usize, b: usize) -> u8 {
        self.edges.get(&(a, b)).copied().unwrap_or(0)
    }

    /// Strongly connected components with more than one node, using edges in `mask`, among
    /// `members` (all nodes when `None`). Iterative Tarjan.
    fn sccs(&self, mask: u8, members: Option<&HashSet<usize>>) -> Vec<Vec<usize>> {
        let n = self.adj.len();
        let inside = |x: usize| members.is_none_or(|m| m.contains(&x));
        let mut index = vec![usize::MAX; n];
        let mut low = vec![0usize; n];
        let mut on_stack = vec![false; n];
        let mut stack = Vec::new();
        let mut out = Vec::new();
        let mut counter = 0;
        for root in 0..n {
            if index[root] != usize::MAX || !inside(root) {
                continue;
            }
            let mut work: Vec<(usize, usize)> = vec![(root, 0)];
            index[root] = counter;
            low[root] = counter;
            counter += 1;
            stack.push(root);
            on_stack[root] = true;
            while let Some(&mut (v, ref mut i)) = work.last_mut() {
                if *i < self.adj[v].len() {
                    let w = self.adj[v][*i];
                    *i += 1;
                    if !inside(w) || self.bits(v, w) & mask == 0 {
                        continue;
                    }
                    if index[w] == usize::MAX {
                        index[w] = counter;
                        low[w] = counter;
                        counter += 1;
                        stack.push(w);
                        on_stack[w] = true;
                        work.push((w, 0));
                    } else if on_stack[w] {
                        low[v] = low[v].min(index[w]);
                    }
                } else {
                    work.pop();
                    if let Some(&(p, _)) = work.last() {
                        low[p] = low[p].min(low[v]);
                    }
                    if low[v] == index[v] {
                        let mut comp = Vec::new();
                        loop {
                            let w = stack.pop().expect("tarjan stack");
                            on_stack[w] = false;
                            comp.push(w);
                            if w == v {
                                break;
                            }
                        }
                        if comp.len() > 1 {
                            out.push(comp);
                        }
                    }
                }
            }
        }
        out
    }

    /// Shortest path from `from` to `to` over `mask` edges within `members`, as a node list.
    fn path(
        &self,
        from: usize,
        to: usize,
        mask: u8,
        members: &HashSet<usize>,
    ) -> Option<Vec<usize>> {
        let mut parent: HashMap<usize, usize> = HashMap::new();
        let mut queue = VecDeque::from([from]);
        parent.insert(from, from);
        while let Some(v) = queue.pop_front() {
            if v == to {
                let mut p = vec![to];
                let mut cur = to;
                while cur != from {
                    cur = parent[&cur];
                    p.push(cur);
                }
                p.reverse();
                return Some(p);
            }
            for &w in &self.adj[v] {
                if members.contains(&w) && self.bits(v, w) & mask != 0 && !parent.contains_key(&w) {
                    parent.insert(w, v);
                    queue.push_back(w);
                }
            }
        }
        None
    }

    /// A cycle using only `mask` edges inside `members`, as `[a, b, ..., a]`.
    fn cycle_in(&self, mask: u8, members: &HashSet<usize>) -> Option<Vec<usize>> {
        let sub = self.sccs(mask, Some(members));
        let comp: HashSet<usize> = sub.first()?.iter().copied().collect();
        let start = *sub[0].iter().min()?;
        self.adj[start]
            .iter()
            .filter(|&&w| comp.contains(&w) && self.bits(start, w) & mask != 0)
            .filter_map(|&w| {
                self.path(w, start, mask, &comp)
                    .map(|p| [vec![start], p].concat())
            })
            .min_by_key(|c| c.len())
    }

    /// A cycle with exactly one rw edge, the rest from `mask` (which excludes rw).
    fn single_rw(&self, mask: u8, members: &HashSet<usize>) -> Option<Vec<usize>> {
        let mut tries = 0;
        let mut nodes: Vec<usize> = members.iter().copied().collect();
        nodes.sort_unstable();
        for a in nodes {
            for &b in &self.adj[a] {
                if !members.contains(&b) || self.bits(a, b) & RW == 0 {
                    continue;
                }
                tries += 1;
                if tries > 4096 {
                    return None;
                }
                if let Some(p) = self.path(b, a, mask, members) {
                    return Some([vec![a], p].concat());
                }
            }
        }
        None
    }

    fn cycles(&self, txns: &[Txn]) -> Vec<Anomaly> {
        let mut out = Vec::new();
        for comp in self.sccs(ALL, None) {
            let members: HashSet<usize> = comp.iter().copied().collect();
            let found = [
                ("G0", WW, false),
                ("G1c", WW | WR, false),
                ("G-single", WW | WR, true),
                ("G2", WW | WR | RW, false),
                ("G0-realtime", WW | RT, false),
                ("G1c-realtime", WW | WR | RT, false),
                ("G-single-realtime", WW | WR | RT, true),
                ("G2-realtime", ALL, false),
            ]
            .into_iter()
            .find_map(|(kind, mask, one_rw)| {
                let c = if one_rw {
                    self.single_rw(mask, &members)
                } else {
                    self.cycle_in(mask, &members)
                };
                c.map(|c| (kind, mask | if one_rw { RW } else { 0 }, c))
            });
            if let Some((kind, mask, cycle)) = found {
                out.push(self.describe(kind, mask, &cycle, txns));
            }
        }
        out
    }

    fn describe(&self, kind: &str, mask: u8, cycle: &[usize], txns: &[Txn]) -> Anomaly {
        let mut detail = format!("{kind} cycle: ");
        let mut events = Vec::new();
        for w in cycle.windows(2) {
            let bits = self.bits(w[0], w[1]) & mask;
            let label = [(WW, "ww"), (WR, "wr"), (RW, "rw"), (RT, "rt")]
                .iter()
                .find(|(b, _)| bits & b != 0)
                .map_or("?", |x| x.1);
            detail.push_str(&format!("T{} -{label}-> ", txns[w[0]].op.invoke_index));
            events.extend(txns[w[0]].op.indices());
        }
        detail.push_str(&format!("T{}", txns[cycle[0]].op.invoke_index));
        Anomaly::new(kind, detail, events)
    }
}
