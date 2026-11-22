//! Model-based tests of the slot store (KV-G02, KV-G03; scenarios K01, K02).
//!
//! A reference model, a `BTreeMap` from key to value and optional absolute
//! deadline, runs the same generated command sequences as the store. The
//! commands go through the RESP command parser (`Command::parse`) and the
//! standalone prepare, reserve, refresh and commit path. Time is injected, so
//! TTL replies are compared exactly; the clock advances by 0 to 2,000 ms
//! between commands and sometimes lands exactly on, or 1 ms before, a deadline.
//!
//! The model is the semantic reference for replies. It cannot predict which
//! key a sampled eviction picks, so it checks that each evicted key existed and
//! then applies the store's choice. Exact memory charges depend on backing
//! array capacities, so the out-of-memory checks use two bounds: a write whose
//! live bodies alone exceed the cap must fail under `noeviction`, and a write
//! that fits with the worst-case container overhead must succeed without
//! evicting anything.
//!
//! The planted bugs at the end wrap the store with a known defect and require
//! the same property to fail.
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};

use nabedra_check::r#gen::Gen;
use nabedra_check::prop::{self, Config};

use super::records::Record;
use super::*;
use crate::cache::command::Command;
use crate::cache::wire::Response;

/// The per-body charge the store documents in `records.rs`.
const BODY_OVERHEAD: usize = 64;
/// At most six keys exist, so every container has at most 16 slots. This
/// bounds the index (16 hashbrown buckets plus control bytes and padding),
/// the record array and the expiry heap.
const CONTAINER_BOUND: usize = 16 * (size_of::<(Bytes, usize)>() + 1)
    + 64
    + 16 * size_of::<Record>()
    + 16 * size_of::<usize>();
const KEYS: [&[u8]; 6] = [b"a", b"b", b"{k}c", b"d\0\r\n", b"e", b"\xfff"];
const CAPS: [usize; 5] = [2_000, 3_000, 6_000, 12_000, 1 << 20];
/// Canonical extremes and the non-canonical forms Valkey rejects as integers.
const INTEGER_TEXTS: [&[u8]; 13] = [
    b"0",
    b"1",
    b"-1",
    b"42",
    b"9223372036854775807",
    b"-9223372036854775808",
    b"9223372036854775806",
    b"007",
    b"-0",
    b"+1",
    b" 1",
    b"1.5",
    b"99999999999999999999",
];
const OVERFLOW: &str = "ERR increment or decrement would overflow";
const NOT_INTEGER: &str = "ERR value is not an integer or out of range";
const SET_EXPIRY: &str = "ERR invalid expire time in 'set' command";
const OOM: &str = "OOM command not allowed under the cache memory limit";

/// How often the unmutated property reached each interesting situation. The
/// main test requires every counter to be non-zero, so a generator change that
/// stops exercising a branch fails instead of passing vacuously.
static COVERAGE: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
const COVERAGE_NAMES: [&str; 6] = [
    "noeviction OOM",
    "sampled_lru eviction",
    "INCRBY overflow",
    "read exactly at a deadline",
    "sweep stopped by its item limit",
    "non-positive EXPIRE delete",
];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Out {
    Ok,
    Bulk(Option<Bytes>),
    Int(i64),
    Array(Vec<Option<Bytes>>),
    Err(String),
}

impl Out {
    fn from_reply(reply: Reply) -> Self {
        match reply {
            Reply::Ok => Self::Ok,
            Reply::Bulk(value) => Self::Bulk(value),
            Reply::Integer(value) => Self::Int(value),
            Reply::Array(values) => Self::Array(values),
            Reply::Error(error) => Self::Err(error.resp().to_owned()),
        }
    }

    fn from_response(response: Response) -> Self {
        match response {
            Response::Error(text) => Self::Err(String::from_utf8_lossy(&text).into_owned()),
            other => panic!("parser returned a non-error response {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SetTtl {
    Ex(i64),
    Px(i64),
}

#[derive(Clone, Debug)]
enum Cmd {
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        when: SetWhen,
        ttl: Option<SetTtl>,
    },
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    Mget(Vec<Bytes>),
    Ttl(Bytes),
    Pttl(Bytes),
    Expire(Bytes, i64),
    Pexpire(Bytes, i64),
    Persist(Bytes),
    Incrby(Bytes, i64),
}

impl Cmd {
    /// RESP arguments, with randomly cased command names and options.
    fn args(&self, upper: bool) -> Vec<Bytes> {
        let word = |name: &str| {
            Bytes::from(if upper {
                name.to_ascii_uppercase()
            } else {
                name.to_owned()
            })
        };
        let number = |value: i64| Bytes::from(value.to_string());
        let mut args = Vec::new();
        match self {
            Self::Get(key) => args.extend([word("get"), key.clone()]),
            Self::Set {
                key,
                value,
                when,
                ttl,
            } => {
                args.extend([word("set"), key.clone(), value.clone()]);
                match when {
                    SetWhen::Always => {}
                    SetWhen::Absent => args.push(word("nx")),
                    SetWhen::Present => args.push(word("xx")),
                }
                match ttl {
                    None => {}
                    Some(SetTtl::Ex(seconds)) => args.extend([word("ex"), number(*seconds)]),
                    Some(SetTtl::Px(ms)) => args.extend([word("px"), number(*ms)]),
                }
            }
            Self::Del(keys) => {
                args.push(word("del"));
                args.extend(keys.iter().cloned());
            }
            Self::Exists(keys) => {
                args.push(word("exists"));
                args.extend(keys.iter().cloned());
            }
            Self::Mget(keys) => {
                args.push(word("mget"));
                args.extend(keys.iter().cloned());
            }
            Self::Ttl(key) => args.extend([word("ttl"), key.clone()]),
            Self::Pttl(key) => args.extend([word("pttl"), key.clone()]),
            Self::Expire(key, seconds) => {
                args.extend([word("expire"), key.clone(), number(*seconds)])
            }
            Self::Pexpire(key, ms) => args.extend([word("pexpire"), key.clone(), number(*ms)]),
            Self::Persist(key) => args.extend([word("persist"), key.clone()]),
            Self::Incrby(key, delta) => args.extend([word("incrby"), key.clone(), number(*delta)]),
        }
        args
    }

    fn read_keys(&self) -> &[Bytes] {
        match self {
            Self::Get(key) | Self::Ttl(key) | Self::Pttl(key) => std::slice::from_ref(key),
            Self::Exists(keys) | Self::Mget(keys) => keys,
            _ => &[],
        }
    }
}

fn gen_key(g: &mut Gen, key_count: usize) -> Bytes {
    Bytes::from_static(KEYS[g.below(key_count as u64) as usize])
}

fn gen_keys(g: &mut Gen, key_count: usize) -> Vec<Bytes> {
    let len = 1 + g.below(4) as usize;
    (0..len).map(|_| gen_key(g, key_count)).collect()
}

fn gen_value(g: &mut Gen) -> Bytes {
    match g.below(10) {
        0 => Bytes::new(),
        1..=3 => Bytes::from(g.ascii(8)),
        4 => Bytes::from(g.bytes(8)),
        5 | 6 => Bytes::from_static(INTEGER_TEXTS[g.below(INTEGER_TEXTS.len() as u64) as usize]),
        _ => {
            let len = 300 + g.below(1_300) as usize;
            Bytes::from(vec![b'a' + g.below(26) as u8; len])
        }
    }
}

fn gen_delta(g: &mut Gen) -> i64 {
    match g.below(8) {
        0 => i64::MAX,
        1 => i64::MIN,
        2 => i64::MAX - 1,
        3 => -i64::MAX,
        _ => g.range_i64(-1_000, 1_000),
    }
}

fn gen_cmd(g: &mut Gen, key_count: usize) -> Cmd {
    match g.below(17) {
        0 | 1 => Cmd::Get(gen_key(g, key_count)),
        2..=5 => {
            let key = gen_key(g, key_count);
            let value = gen_value(g);
            let when = *g.pick(&[
                SetWhen::Always,
                SetWhen::Always,
                SetWhen::Absent,
                SetWhen::Present,
            ]);
            let ttl = match g.below(6) {
                0 | 1 => None,
                2 => Some(SetTtl::Ex(g.range_i64(1, 3))),
                3 | 4 => Some(SetTtl::Px(g.range_i64(1, 3_000))),
                _ => Some(*g.pick(&[SetTtl::Ex(0), SetTtl::Px(0), SetTtl::Ex(-1), SetTtl::Px(-5)])),
            };
            Cmd::Set {
                key,
                value,
                when,
                ttl,
            }
        }
        6 => Cmd::Del(gen_keys(g, key_count)),
        7 => Cmd::Exists(gen_keys(g, key_count)),
        8 => Cmd::Mget(gen_keys(g, key_count)),
        9 => Cmd::Ttl(gen_key(g, key_count)),
        10 => Cmd::Pttl(gen_key(g, key_count)),
        11 => Cmd::Expire(gen_key(g, key_count), g.range_i64(-2, 3)),
        12 => Cmd::Pexpire(gen_key(g, key_count), g.range_i64(-100, 3_000)),
        13 => Cmd::Persist(gen_key(g, key_count)),
        _ => Cmd::Incrby(gen_key(g, key_count), gen_delta(g)),
    }
}

#[derive(Clone, Debug)]
struct Entry {
    value: Bytes,
    deadline: Option<u64>,
}

impl Entry {
    fn live(&self, now: u64) -> bool {
        self.deadline.is_none_or(|deadline| now < deadline)
    }
}

/// What the command must do if it is admitted.
#[derive(Debug)]
struct Expected {
    out: Out,
    upsert: Option<(Bytes, Entry)>,
    deletes: Vec<Bytes>,
}

impl Expected {
    fn reply(out: Out) -> Self {
        Self {
            out,
            upsert: None,
            deletes: Vec::new(),
        }
    }
}

/// The reference. Entries stay in the map after they expire, until a
/// committed turn deletes or replaces them, so the map is a superset of the
/// store's physical records. Replies only ever look at live entries.
#[derive(Default)]
struct Model {
    map: BTreeMap<Bytes, Entry>,
}

/// Valkey's `string2ll` accepts only canonical signed decimals.
fn model_integer(value: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(value).ok()?;
    let parsed: i64 = text.parse().ok()?;
    (parsed.to_string() == text).then_some(parsed)
}

impl Model {
    fn live(&self, key: &Bytes, now: u64) -> Option<&Entry> {
        self.map.get(key).filter(|entry| entry.live(now))
    }

    fn live_count(&self, now: u64) -> usize {
        self.map.values().filter(|entry| entry.live(now)).count()
    }

    fn expect(&self, cmd: &Cmd, now: u64) -> Expected {
        match cmd {
            Cmd::Get(key) => {
                Expected::reply(Out::Bulk(self.live(key, now).map(|e| e.value.clone())))
            }
            Cmd::Mget(keys) => Expected::reply(Out::Array(
                keys.iter()
                    .map(|key| self.live(key, now).map(|e| e.value.clone()))
                    .collect(),
            )),
            Cmd::Exists(keys) => Expected::reply(Out::Int(
                keys.iter()
                    .filter(|key| self.live(key, now).is_some())
                    .count() as i64,
            )),
            Cmd::Del(keys) => {
                let mut deletes: Vec<Bytes> = Vec::new();
                for key in keys {
                    if self.live(key, now).is_some() && !deletes.contains(key) {
                        deletes.push(key.clone());
                    }
                }
                Expected {
                    out: Out::Int(deletes.len() as i64),
                    upsert: None,
                    deletes,
                }
            }
            Cmd::Ttl(key) | Cmd::Pttl(key) => {
                let milliseconds = matches!(cmd, Cmd::Pttl(_));
                Expected::reply(Out::Int(match self.live(key, now) {
                    None => -2,
                    Some(Entry { deadline: None, .. }) => -1,
                    Some(Entry {
                        deadline: Some(deadline),
                        ..
                    }) => {
                        let remaining = deadline - now;
                        if milliseconds {
                            remaining as i64
                        } else {
                            // Valkey rounds TTL to the nearest second.
                            ((remaining + 500) / 1000) as i64
                        }
                    }
                }))
            }
            Cmd::Set {
                key,
                value,
                when,
                ttl,
            } => {
                let ttl_ms = match ttl {
                    None => None,
                    Some(SetTtl::Ex(seconds)) => Some(seconds.saturating_mul(1000)),
                    Some(SetTtl::Px(ms)) => Some(*ms),
                };
                if ttl_ms.is_some_and(|ttl| ttl <= 0) {
                    return Expected::reply(Out::Err(SET_EXPIRY.into()));
                }
                let exists = self.live(key, now).is_some();
                if (*when == SetWhen::Absent && exists) || (*when == SetWhen::Present && !exists) {
                    return Expected::reply(Out::Bulk(None));
                }
                Expected {
                    out: Out::Ok,
                    upsert: Some((
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            deadline: ttl_ms.map(|ttl| now + ttl as u64),
                        },
                    )),
                    deletes: Vec::new(),
                }
            }
            Cmd::Expire(key, _) | Cmd::Pexpire(key, _) => {
                let ttl_ms = match cmd {
                    Cmd::Expire(_, seconds) if *seconds > 0 => seconds * 1000,
                    Cmd::Expire(_, seconds) | Cmd::Pexpire(_, seconds) => *seconds,
                    _ => unreachable!(),
                };
                let Some(entry) = self.live(key, now) else {
                    return Expected::reply(Out::Int(0));
                };
                if ttl_ms <= 0 {
                    return Expected {
                        out: Out::Int(1),
                        upsert: None,
                        deletes: vec![key.clone()],
                    };
                }
                Expected {
                    out: Out::Int(1),
                    upsert: Some((
                        key.clone(),
                        Entry {
                            value: entry.value.clone(),
                            deadline: Some(now + ttl_ms as u64),
                        },
                    )),
                    deletes: Vec::new(),
                }
            }
            Cmd::Persist(key) => match self.live(key, now) {
                Some(entry) if entry.deadline.is_some() => Expected {
                    out: Out::Int(1),
                    upsert: Some((
                        key.clone(),
                        Entry {
                            value: entry.value.clone(),
                            deadline: None,
                        },
                    )),
                    deletes: Vec::new(),
                },
                _ => Expected::reply(Out::Int(0)),
            },
            Cmd::Incrby(key, delta) => {
                let entry = self.live(key, now);
                let current = match entry {
                    None => 0,
                    Some(entry) => match model_integer(&entry.value) {
                        Some(value) => value,
                        None => return Expected::reply(Out::Err(NOT_INTEGER.into())),
                    },
                };
                let Some(value) = current.checked_add(*delta) else {
                    return Expected::reply(Out::Err(OVERFLOW.into()));
                };
                Expected {
                    out: Out::Int(value),
                    upsert: Some((
                        key.clone(),
                        Entry {
                            value: Bytes::from(value.to_string()),
                            deadline: entry.and_then(|entry| entry.deadline),
                        },
                    )),
                    deletes: Vec::new(),
                }
            }
        }
    }

    fn body(key: &Bytes, entry: &Entry) -> usize {
        BODY_OVERHEAD + key.len() + entry.value.len()
    }

    /// Body bytes of the entries that pass `keep` after `expected` applies.
    fn body_after(&self, expected: &Expected, keep: impl Fn(&Entry) -> bool) -> usize {
        let mut total = 0;
        for (key, entry) in &self.map {
            let replaced = expected.upsert.as_ref().is_some_and(|(k, _)| k == key);
            if !replaced && !expected.deletes.contains(key) && keep(entry) {
                total += Self::body(key, entry);
            }
        }
        if let Some((key, entry)) = &expected.upsert {
            total += Self::body(key, entry);
        }
        total
    }
}

/// Defects planted around the real store. Each must make the property fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutant {
    None,
    /// A GET of a key with a TTL slides its deadline forward.
    ReadExtendsTtl,
    /// EXPIRE or PEXPIRE with a non-positive TTL answers 1 but keeps the key.
    NonPositiveExpireKeeps,
    /// INCRBY overflow answers an error text that differs from Valkey's.
    OverflowText,
}

/// The store under test, driven as the standalone actor drives it.
struct Subject {
    store: SlotStore,
    mutant: Mutant,
}

struct Executed {
    out: Out,
    effects: Vec<Effect>,
    evicted: u64,
    charge_after: usize,
}

impl Subject {
    fn probe(&self, op: &Operation, now: u64) -> Reply {
        // Preparation never publishes, so a probe has no side effects.
        self.store
            .prepare(op, now, TurnLimits::default())
            .reply()
            .clone()
    }

    fn pttl(&self, key: &Bytes, now: u64) -> Reply {
        self.probe(
            &Operation::Ttl {
                key: key.clone(),
                milliseconds: true,
            },
            now,
        )
    }

    fn exists(&self, key: &Bytes, now: u64) -> bool {
        self.probe(&Operation::Exists(vec![key.clone()]), now) == Reply::Integer(1)
    }

    /// The standalone `apply` loop: prepare at `t0`, reserve, refresh the
    /// clock to `t1`, and prepare again at `t1` if the turn went stale.
    fn apply(&mut self, op: &Operation, t0: u64, t1: u64) -> Executed {
        let mut prepare_at = t0;
        for _ in 0..2 {
            let mut prepared = self.store.prepare(op, prepare_at, TurnLimits::default());
            if let Err(error) = self.store.reserve(&mut prepared) {
                return Executed::error(error);
            }
            match prepared.refresh_time(t1) {
                Ok(()) => {
                    let effects = prepared.effects().to_vec();
                    let evicted = prepared.removals().evicted;
                    let charge_after = prepared.charge_after();
                    let reply = self.store.commit(prepared);
                    return Executed {
                        out: Out::from_reply(reply),
                        effects,
                        evicted,
                        charge_after,
                    };
                }
                Err(StoreError::StaleTurn) => prepare_at = t1,
                Err(error) => return Executed::error(error),
            }
        }
        Executed::error(StoreError::StaleTurn)
    }

    fn execute(&mut self, args: Vec<Bytes>, t0: u64, t1: u64) -> Executed {
        let op = match Command::parse(args) {
            Ok(Command::Data(command)) => command.into_operation(),
            Ok(_) => panic!("generated a control command"),
            Err(error) => {
                return Executed {
                    out: Out::from_response(error.response()),
                    effects: Vec::new(),
                    evicted: 0,
                    charge_after: self.store.charged_bytes(),
                };
            }
        };
        if self.mutant == Mutant::NonPositiveExpireKeeps
            && let Operation::Expire { key, ttl_ms } = &op
            && *ttl_ms <= 0
        {
            let exists = self.exists(key, t1);
            return Executed {
                out: Out::Int(i64::from(exists)),
                effects: Vec::new(),
                evicted: 0,
                charge_after: self.store.charged_bytes(),
            };
        }
        let mut executed = self.apply(&op, t0, t1);
        match (self.mutant, &op, &mut executed.out) {
            (Mutant::ReadExtendsTtl, Operation::Get(key), Out::Bulk(Some(_))) => {
                if let Reply::Integer(remaining @ 1..) = self.pttl(key, t1) {
                    let slide = Operation::Expire {
                        key: key.clone(),
                        ttl_ms: remaining + 1_000,
                    };
                    self.apply(&slide, t1, t1);
                }
            }
            (Mutant::OverflowText, _, Out::Err(text)) if text == OVERFLOW => {
                *text = "ERR increment would overflow".into();
            }
            _ => {}
        }
        executed
    }
}

impl Executed {
    fn error(error: StoreError) -> Self {
        Self {
            out: Out::Err(error.resp().to_owned()),
            effects: Vec::new(),
            evicted: 0,
            charge_after: 0,
        }
    }
}

struct Run {
    model: Model,
    subject: Subject,
    cap: usize,
    policy: EvictionPolicy,
    now: u64,
    trace: Vec<String>,
}

impl Run {
    fn cover(&self, index: usize) {
        if self.subject.mutant == Mutant::None {
            COVERAGE[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn fail(&self, message: String) -> Result<(), String> {
        let start = self.trace.len().saturating_sub(24);
        Err(format!(
            "{message}\n  policy {:?}, cap {} bytes, now {} ms\n  last steps:\n    {}",
            self.policy,
            self.cap,
            self.now,
            self.trace[start..].join("\n    ")
        ))
    }

    fn check_memory(&self, charge_after: usize) -> Result<(), String> {
        let charged = self.subject.store.charged_bytes();
        if charged > self.cap || charge_after > self.cap {
            return self.fail(format!(
                "memory over the cap: charged {charged}, staged {charge_after}, cap {}",
                self.cap
            ));
        }
        let live = self.model.live_count(self.now);
        let physical = self.subject.store.len();
        if physical < live || physical > self.model.map.len() {
            return self.fail(format!(
                "store holds {physical} records, model has {live} live and {} physical",
                self.model.map.len()
            ));
        }
        Ok(())
    }

    fn advance(&mut self, g: &mut Gen) {
        let deadlines: Vec<u64> = self
            .model
            .map
            .values()
            .filter_map(|entry| entry.deadline)
            .filter(|deadline| *deadline > self.now)
            .collect();
        if !deadlines.is_empty() && g.chance(1, 4) {
            // Land exactly on a TTL boundary, or 1 ms before it.
            let deadline = *g.pick(&deadlines);
            self.now = if g.bool() { deadline } else { deadline - 1 }.max(self.now);
        } else {
            self.now += g.below(2_001);
        }
    }

    fn sweep(&mut self, g: &mut Gen) -> Result<(), String> {
        let limit = *g.pick(&[1usize, 2, 3, 64]);
        // The time budget is generous so the item limit decides; the budget
        // itself is checked deterministically in `k01_sweep_respects_time_budget`.
        let prepared = self
            .subject
            .store
            .prepare_expiry(self.now, limit, Duration::from_secs(10));
        let effects = prepared.effects().to_vec();
        self.trace.push(format!(
            "t={} sweep limit {limit} -> {} deletes",
            self.now,
            effects.len()
        ));
        if effects.len() > limit {
            return self.fail(format!(
                "sweep removed {} items, limit {limit}",
                effects.len()
            ));
        }
        for effect in &effects {
            let Effect::Delete(key) = effect else {
                return self.fail(format!("sweep staged an upsert {effect:?}"));
            };
            match self.model.map.get(key) {
                None => return self.fail(format!("sweep deleted missing key {key:?}")),
                Some(entry) if entry.live(self.now) => {
                    return self.fail(format!("sweep deleted live key {key:?}"));
                }
                Some(_) => {}
            }
        }
        let charge_after = prepared.charge_after();
        if effects.len() == limit {
            self.cover(4);
        }
        self.subject.store.commit(prepared);
        for effect in &effects {
            self.model.map.remove(effect.key());
        }
        if effects.len() < limit && self.subject.store.len() != self.model.live_count(self.now) {
            return self.fail(format!(
                "sweep stopped early: {} records left, {} live",
                self.subject.store.len(),
                self.model.live_count(self.now)
            ));
        }
        self.check_memory(charge_after)
    }

    fn step(&mut self, g: &mut Gen, cmd: Cmd) -> Result<(), String> {
        let t0 = self.now;
        if g.chance(1, 4) {
            self.now += g.below(3);
        }
        let now = self.now;
        let args = cmd.args(g.bool());
        let expected = self.model.expect(&cmd, now);
        let ttl_before: Vec<Reply> = cmd
            .read_keys()
            .iter()
            .map(|key| self.subject.pttl(key, now))
            .collect();
        let digest_before = self.subject.store.digest(now);
        let executed = self.subject.execute(args, t0, now);
        self.trace.push(format!(
            "t={t0}..{now} {cmd:?} -> {:?} (model {:?})",
            executed.out, expected.out
        ));

        // Reads never extend a TTL.
        for (key, before) in cmd.read_keys().iter().zip(&ttl_before) {
            let after = self.subject.pttl(key, now);
            if after != *before {
                return self.fail(format!(
                    "read extended a TTL: {key:?} PTTL {before:?} before the read, {after:?} after"
                ));
            }
        }
        // An expired key never returns.
        let returned: Vec<(&Bytes, bool)> = match (&cmd, &executed.out) {
            (Cmd::Get(key), Out::Bulk(value)) => vec![(key, value.is_some())],
            (Cmd::Mget(keys), Out::Array(values)) => keys
                .iter()
                .zip(values)
                .map(|(k, v)| (k, v.is_some()))
                .collect(),
            _ => Vec::new(),
        };
        for (key, present) in returned {
            if present && self.model.live(key, now).is_none() {
                return self.fail(format!("expired or missing key {key:?} returned"));
            }
        }

        if cmd
            .read_keys()
            .iter()
            .any(|key| self.model.map.get(key).and_then(|e| e.deadline) == Some(now))
        {
            self.cover(3);
        }
        if executed.out == Out::Err(OOM.into()) && executed.out != expected.out {
            if self.policy == EvictionPolicy::NoEviction {
                self.cover(0);
            }
            return self.check_oom(&expected, digest_before, now);
        }
        if executed.evicted > 0 {
            self.cover(1);
        }
        if expected.out == Out::Err(OVERFLOW.into()) {
            self.cover(2);
        }
        if matches!(cmd, Cmd::Expire(..) | Cmd::Pexpire(..)) && !expected.deletes.is_empty() {
            self.cover(5);
        }
        if executed.out != expected.out {
            return self.fail(format!(
                "reply mismatch: store {:?}, model {:?}",
                executed.out, expected.out
            ));
        }
        // A non-positive EXPIRE deletes.
        if matches!(cmd, Cmd::Expire(..) | Cmd::Pexpire(..))
            && let Some(key) = expected.deletes.first()
            && self.subject.exists(key, now)
        {
            return self.fail(format!("non-positive EXPIRE did not delete {key:?}"));
        }
        self.check_effects(&expected, &executed, now)?;

        // Apply the semantic change, then the store's lazy deletions and evictions.
        for effect in &executed.effects {
            self.model.map.remove(effect.key());
        }
        if let Some((key, entry)) = &expected.upsert {
            self.model.map.insert(key.clone(), entry.clone());
        }
        self.check_memory(executed.charge_after)
    }

    /// The store refused an admitted write for memory. Check it had to.
    fn check_oom(
        &self,
        expected: &Expected,
        digest_before: [u8; 20],
        now: u64,
    ) -> Result<(), String> {
        if self.subject.store.digest(now) != digest_before {
            return self.fail("an OOM reply changed the keyspace".into());
        }
        let Some((key, entry)) = &expected.upsert else {
            return self.fail(format!(
                "OOM for a command that writes nothing ({:?})",
                expected.out
            ));
        };
        let fits = self.model.body_after(expected, |_| true) + CONTAINER_BOUND <= self.cap;
        match self.policy {
            EvictionPolicy::NoEviction if fits => self.fail(format!(
                "noeviction answered OOM although the write fits ({} body bytes after, bound {})",
                self.model.body_after(expected, |_| true),
                CONTAINER_BOUND
            )),
            EvictionPolicy::SampledLru if Model::body(key, entry) + CONTAINER_BOUND <= self.cap => {
                self.fail(format!(
                    "sampled_lru answered OOM although evicting every other key admits the write ({} bytes)",
                    Model::body(key, entry)
                ))
            }
            _ => Ok(()),
        }
    }

    fn check_effects(
        &self,
        expected: &Expected,
        executed: &Executed,
        now: u64,
    ) -> Result<(), String> {
        let upserts: Vec<&Effect> = executed
            .effects
            .iter()
            .filter(|effect| matches!(effect, Effect::Upsert { .. }))
            .collect();
        match (&expected.upsert, upserts.as_slice()) {
            (None, []) => {}
            (
                Some((key, entry)),
                [
                    Effect::Upsert {
                        key: k,
                        value,
                        deadline_ms,
                    },
                ],
            ) if k == key && *value == entry.value && *deadline_ms == entry.deadline => {}
            _ => {
                return self.fail(format!(
                    "staged upserts {upserts:?}, model expects {:?}",
                    expected.upsert
                ));
            }
        }
        let deleted: Vec<&Bytes> = executed
            .effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Delete(key) => Some(key),
                Effect::Upsert { .. } => None,
            })
            .collect();
        for key in &expected.deletes {
            if !deleted.contains(&key) {
                return self.fail(format!("command did not delete {key:?}"));
            }
        }
        let mut evicted = 0;
        for key in deleted {
            let Some(entry) = self.model.map.get(key) else {
                return self.fail(format!(
                    "deleted or evicted a key that never existed: {key:?}"
                ));
            };
            if entry.live(now) && !expected.deletes.contains(key) {
                evicted += 1;
            }
        }
        if evicted > 0 && self.policy == EvictionPolicy::NoEviction {
            return self.fail(format!("noeviction removed {evicted} live keys"));
        }
        if evicted != executed.evicted {
            return self.fail(format!(
                "removed {evicted} live keys, store counted {} evictions",
                executed.evicted
            ));
        }
        if evicted > 0 && self.model.body_after(expected, |_| true) + CONTAINER_BOUND <= self.cap {
            return self.fail(format!("evicted {evicted} keys although the write fits"));
        }
        if self.policy == EvictionPolicy::NoEviction
            && self.model.body_after(expected, |entry| entry.live(now)) > self.cap
        {
            return self
                .fail("noeviction admitted a write whose live bodies exceed the cap".into());
        }
        Ok(())
    }
}

fn run_case(g: &mut Gen, mutant: Mutant) -> Result<(), String> {
    let policy = if g.bool() {
        EvictionPolicy::NoEviction
    } else {
        EvictionPolicy::SampledLru
    };
    let cap = *g.pick(&CAPS);
    let key_count = 1 + g.below(KEYS.len() as u64) as usize;
    let mut run = Run {
        model: Model::default(),
        subject: Subject {
            store: SlotStore::new(cap, policy),
            mutant,
        },
        cap,
        policy,
        now: g.below(10_000),
        trace: Vec::new(),
    };
    let steps = 1 + g.below(80);
    for _ in 0..steps {
        run.advance(g);
        if g.chance(1, 6) {
            run.sweep(g)?;
        }
        let cmd = gen_cmd(g, key_count);
        run.step(g, cmd)?;
    }
    Ok(())
}

#[test]
fn k01_k02_store_matches_model() {
    prop::check("kv store matches the reference model", |g| {
        run_case(g, Mutant::None)
    });
    for (counter, name) in COVERAGE.iter().zip(COVERAGE_NAMES) {
        let hits = counter.load(Ordering::Relaxed);
        eprintln!("coverage: {name}: {hits}");
        assert!(hits > 0, "the generated cases never reached: {name}");
    }
}

/// The sweep checks its elapsed time before each item, so a zero budget
/// processes nothing and the item limit caps a long backlog at 64.
#[test]
fn k01_sweep_respects_item_limit_and_time_budget() {
    let mut store = SlotStore::new(1 << 24, EvictionPolicy::NoEviction);
    for index in 0..200u32 {
        let op = Operation::Set {
            key: Bytes::from(format!("key-{index}")),
            value: Bytes::from_static(b"v"),
            ttl_ms: Some(10 + u64::from(index % 7)),
            when: SetWhen::Always,
        };
        let prepared = store.prepare(&op, 0, TurnLimits::default());
        assert_eq!(store.commit(prepared), Reply::Ok);
    }
    let stalled = store.prepare_expiry(1_000, 64, Duration::ZERO);
    assert!(
        stalled.effects().is_empty(),
        "a zero time budget processed items"
    );
    let mut remaining = 200;
    while remaining > 0 {
        let prepared = store.prepare_expiry(1_000, 64, Duration::from_secs(10));
        let removed = prepared.effects().len();
        assert_eq!(removed, remaining.min(64));
        assert_eq!(prepared.removals().expired, removed as u64);
        store.commit(prepared);
        remaining -= removed;
        assert_eq!(store.len(), remaining);
    }
    assert_eq!(store.charged_bytes(), 0);
}

/// Valkey's exact INCRBY overflow text, independent of the store's constant.
#[test]
fn k01_incrby_overflow_text_matches_valkey() {
    let mut subject = Subject {
        store: SlotStore::new(1 << 20, EvictionPolicy::NoEviction),
        mutant: Mutant::None,
    };
    let key = Bytes::from_static(b"n");
    let set = Cmd::Set {
        key: key.clone(),
        value: Bytes::from_static(b"9223372036854775807"),
        when: SetWhen::Always,
        ttl: None,
    };
    assert_eq!(subject.execute(set.args(true), 0, 0).out, Out::Ok);
    let overflow = subject.execute(Cmd::Incrby(key, 1).args(true), 0, 0).out;
    assert_eq!(
        overflow,
        Out::Err("ERR increment or decrement would overflow".into())
    );
}

/// Run the property against a planted bug and return its failure message.
fn planted(mutant: Mutant) -> String {
    let config = Config {
        cases: 512,
        seed: Some(0x6b76_2d67_3032),
        max_shrink_steps: 2_000,
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        prop::check_with(config, "kv store planted bug", |g| run_case(g, mutant));
    }));
    let Err(panic) = result else {
        panic!("planted bug {mutant:?} was not caught");
    };
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}

#[test]
fn k01_planted_read_extends_ttl_is_caught() {
    let message = planted(Mutant::ReadExtendsTtl);
    assert!(message.contains("read extended a TTL"), "{message}");
}

#[test]
fn k01_planted_non_positive_expire_keeps_key_is_caught() {
    let message = planted(Mutant::NonPositiveExpireKeeps);
    assert!(
        message.contains("non-positive EXPIRE did not delete"),
        "{message}"
    );
}

#[test]
fn k01_planted_incrby_overflow_text_is_caught() {
    let message = planted(Mutant::OverflowText);
    assert!(
        message.contains("reply mismatch") && message.contains(OVERFLOW),
        "{message}"
    );
}
