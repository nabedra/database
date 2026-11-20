//! The `BTreeMap` oracle, the acknowledged-synced prefix rule (NS04, NS05), and read checks
//! (gets and iterator walks, forward and reverse, with bounds and `seek_for_prev`).

use std::collections::BTreeMap;
use std::ops::Bound;

use nabedra_kv::{Error, Snapshot};

use crate::common::{ModelBatch, Rng, group_key, group_prefix};
use crate::workload::{Mix, store_key};

pub type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// What one generation (the run between two recoveries) submitted.
///
/// `batches[..durable]` were acknowledged durable (a waited synced commit, a sync, or a
/// clean close). Every batch in `batches` was submitted; a batch that failed with
/// `NoSpace` is not in the list, because the journal never applies it.
#[derive(Clone, Debug, Default)]
pub struct History {
    pub base: Model,
    pub batches: Vec<ModelBatch>,
    pub durable: usize,
}

impl History {
    pub fn new(base: Model) -> History {
        History {
            base,
            batches: Vec::new(),
            durable: 0,
        }
    }

    /// The prefix `p` in `[durable, batches.len()]` whose oracle state equals `state`.
    pub fn find_prefix(&self, state: &Model) -> Result<(usize, Model), String> {
        let mut m = self.base.clone();
        for b in &self.batches[..self.durable] {
            b.apply(&mut m);
        }
        for p in self.durable..=self.batches.len() {
            if &m == state {
                return Ok((p, m));
            }
            if p < self.batches.len() {
                self.batches[p].apply(&mut m);
            }
        }
        // Diagnose: which prefix does it match, if any, and the first differing key.
        let mut m = self.base.clone();
        let mut any = None;
        for p in 0..=self.batches.len() {
            if &m == state {
                any = Some(p);
                break;
            }
            if p < self.batches.len() {
                self.batches[p].apply(&mut m);
            }
        }
        Err(format!(
            "recovered state is no prefix in [{}, {}] (it matches prefix {any:?}); first difference against the full model: {}",
            self.durable,
            self.batches.len(),
            first_difference(state, &m)
        ))
    }
}

pub fn first_difference(got: &Model, want: &Model) -> String {
    for (k, v) in want {
        match got.get(k) {
            None => return format!("missing {}", show(k)),
            Some(g) if g != v => {
                return format!(
                    "{} = {} want {}",
                    show(k),
                    show(&g[..g.len().min(24)]),
                    show(&v[..v.len().min(24)])
                );
            }
            _ => {}
        }
    }
    for k in got.keys() {
        if !want.contains_key(k) {
            return format!("extra {}", show(k));
        }
    }
    "none".into()
}

pub fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Every key and value in `[lower, upper)` at `snap`, scanning forward or backward.
pub fn scan(
    snap: &Snapshot,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    reverse: bool,
) -> nabedra_kv::Result<Model> {
    let mut it = snap.iter(lower, upper, false);
    let mut out = Model::new();
    if reverse {
        it.seek_to_last();
    } else {
        it.seek_to_first();
    }
    while it.valid() {
        out.insert(it.key().to_vec(), it.value()?.to_vec());
        if reverse {
            it.prev();
        } else {
            it.next();
        }
    }
    it.status()?;
    Ok(out)
}

/// The whole state, scanned both ways; the two scans must agree.
pub fn dump_both(snap: &Snapshot) -> Result<Model, String> {
    dump(snap).map_err(|f| match f {
        DumpFailure::Scan(what, e) => format!("{what} scan: {e}"),
        DumpFailure::Differ(m) => m,
    })
}

/// Why [`dump`] failed.
pub enum DumpFailure {
    /// The named scan ("forward" or "reverse") returned an engine error.
    Scan(&'static str, Error),
    /// Both scans succeeded but disagree.
    Differ(String),
}

/// [`dump_both`] with the engine error kept, so a caller can tell an injected read
/// fault from a wrong answer.
pub fn dump(snap: &Snapshot) -> Result<Model, DumpFailure> {
    let fwd = scan(snap, Bound::Unbounded, Bound::Unbounded, false)
        .map_err(|e| DumpFailure::Scan("forward", e))?;
    let rev = scan(snap, Bound::Unbounded, Bound::Unbounded, true)
        .map_err(|e| DumpFailure::Scan("reverse", e))?;
    if fwd != rev {
        return Err(DumpFailure::Differ(format!(
            "forward and reverse scans differ: {}",
            first_difference(&rev, &fwd)
        )));
    }
    Ok(fwd)
}

pub fn random_key(rng: &mut Rng, mix: &Mix) -> Vec<u8> {
    match rng.below(10) {
        0 => store_key(rng.below(mix.keys)),
        1 => group_prefix(rng.below(mix.groups)),
        _ => group_key(rng.below(mix.groups), rng.below(mix.keys + 6)),
    }
}

fn random_bound(rng: &mut Rng, mix: &Mix) -> Bound<Vec<u8>> {
    match rng.below(4) {
        0 => Bound::Unbounded,
        1 => Bound::Excluded(random_key(rng, mix)),
        _ => Bound::Included(random_key(rng, mix)),
    }
}

fn in_bounds(k: &[u8], lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
    let above = match lo {
        Bound::Included(l) => k >= l.as_slice(),
        Bound::Excluded(l) => k > l.as_slice(),
        Bound::Unbounded => true,
    };
    let below = match hi {
        Bound::Included(h) => k <= h.as_slice(),
        Bound::Excluded(h) => k < h.as_slice(),
        Bound::Unbounded => true,
    };
    above && below
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// A read failed. `Io` is tolerated while a read fault is armed.
pub enum ReadFailure {
    Mismatch(String),
    Error(Error),
}

/// Eight point reads compared with the oracle.
pub fn check_gets(
    rng: &mut Rng,
    mix: &Mix,
    snap: &Snapshot,
    model: &Model,
) -> Result<(), ReadFailure> {
    for _ in 0..8 {
        let key = random_key(rng, mix);
        let got = snap.get(&key).map_err(ReadFailure::Error)?;
        if got.as_deref() != model.get(&key).map(Vec::as_slice) {
            return Err(ReadFailure::Mismatch(format!(
                "get {} = {:?} want {:?}",
                show(&key),
                got.map(|v| show(&v[..v.len().min(24)])),
                model.get(&key).map(|v| show(&v[..v.len().min(24)]))
            )));
        }
    }
    Ok(())
}

/// A random walk compared step by step with the oracle restricted to random bounds:
/// first, last, seek or `seek_for_prev`, then 30 `next`/`prev` steps with direction
/// switches.
pub fn check_walk(
    rng: &mut Rng,
    mix: &Mix,
    snap: &Snapshot,
    model: &Model,
) -> Result<(), ReadFailure> {
    let lo = random_bound(rng, mix);
    let hi = random_bound(rng, mix);
    let expected: Vec<(&Vec<u8>, &Vec<u8>)> = model
        .iter()
        .filter(|(k, _)| in_bounds(k, &lo, &hi))
        .collect();
    let mut it = snap.iter(as_ref(&lo), as_ref(&hi), rng.chance(50));
    let mut trace = vec![format!("bounds {lo:?} {hi:?}")];
    let mut pos: Option<usize> = match rng.below(4) {
        0 => {
            it.seek_to_first();
            trace.push("first".into());
            (!expected.is_empty()).then_some(0)
        }
        1 => {
            it.seek_to_last();
            trace.push("last".into());
            expected.len().checked_sub(1)
        }
        2 => {
            let t = random_key(rng, mix);
            it.seek(&t);
            trace.push(format!("seek {}", show(&t)));
            expected
                .iter()
                .position(|(k, _)| k.as_slice() >= t.as_slice())
        }
        _ => {
            let t = random_key(rng, mix);
            it.seek_for_prev(&t);
            trace.push(format!("seek_for_prev {}", show(&t)));
            expected
                .iter()
                .rposition(|(k, _)| k.as_slice() <= t.as_slice())
        }
    };
    for _ in 0..30 {
        it.status().map_err(ReadFailure::Error)?;
        let Some(p) = pos else {
            if it.valid() {
                return Err(ReadFailure::Mismatch(format!(
                    "{trace:?}: iterator at {}, oracle exhausted",
                    show(it.key())
                )));
            }
            return Ok(());
        };
        if !it.valid() {
            return Err(ReadFailure::Mismatch(format!(
                "{trace:?}: iterator invalid, oracle at {}",
                show(expected[p].0)
            )));
        }
        if it.key() != expected[p].0.as_slice() {
            return Err(ReadFailure::Mismatch(format!(
                "{trace:?}: key {} want {}",
                show(it.key()),
                show(expected[p].0)
            )));
        }
        let v = it.value().map_err(ReadFailure::Error)?;
        if v.as_ref() != expected[p].1.as_slice() {
            return Err(ReadFailure::Mismatch(format!(
                "{trace:?}: value of {} differs",
                show(it.key())
            )));
        }
        if rng.chance(60) {
            it.next();
            trace.push("next".into());
            pos = (p + 1 < expected.len()).then_some(p + 1);
        } else {
            it.prev();
            trace.push("prev".into());
            pos = p.checked_sub(1);
        }
    }
    Ok(())
}
