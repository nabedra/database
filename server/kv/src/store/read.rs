//! Snapshots, point reads and iterators.
//!
//! A [`Snapshot`] holds a registered sequence (so compaction keeps every version it can
//! see) and one `SuperVersion` (so the memtables and runs it reads stay alive). A point
//! read walks the sources newest first, keeping the newest range tombstone covering the
//! key over every source visited; the first point version found is the answer unless
//! that tombstone is newer. Journal values are read through the journal; a failed check
//! there (G3) or in a run block (G2) quarantines the key's scope.

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Damage, DamageReason, Error, FileId, Result};
use crate::iter::range_del::{FragmentedRangeTombstones, RangeDelAggregator};
use crate::iter::{DbIter, MergingIterator, ValueResolver};
use crate::journal::{Journal, VALUE_RECYCLED};
use crate::memtable::Lookup as MemLookup;
use crate::quarantine::{quarantined_error, single_scope};
use crate::run::{Lookup as RunLookup, Run};
use crate::source::InternalIterator;
use crate::types::{Kind, Locator, Seq};
use crate::version::{RunFile, SuperVersion};

use super::Inner;
use super::level_iter::LevelIter;
use super::snapshots::Registered;

/// A consistent read view. Cheap to clone; a clone registers the sequence again.
#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<Inner>,
    seq: Seq,
    sv: Arc<SuperVersion>,
    _handle: Arc<Registered>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").field("seq", &self.seq).finish()
    }
}

struct JournalResolver(Arc<Journal>);

impl ValueResolver for JournalResolver {
    fn resolve(&self, locator: &Locator) -> Result<Bytes> {
        self.0.read_value(locator)
    }
}

/// An iterator that reports an error (a run that failed to open).
struct FailedIter(Error);

impl InternalIterator for FailedIter {
    fn valid(&self) -> bool {
        false
    }
    fn seek_to_first(&mut self) {}
    fn seek_to_last(&mut self) {}
    fn seek(&mut self, _: &[u8]) {}
    fn seek_for_prev(&mut self, _: &[u8]) {}
    fn next(&mut self) {}
    fn prev(&mut self) {}
    fn key(&self) -> &[u8] {
        &[]
    }
    fn value(&self) -> Bytes {
        Bytes::new()
    }
    fn status(&self) -> Result<()> {
        Err(self.0.clone())
    }
}

fn run_tombstones(file: &RunFile, run: &Run) -> Arc<FragmentedRangeTombstones> {
    file.tombstones(run)
}

/// The newest point version of `key` visible at `seq` in `sv`: `(kind, stored value)`,
/// with range tombstones of every source applied. Run damage is reported (G2) when
/// `report` is set; journal GC's relocation check passes false and fails its pass on any
/// error (an unreadable key is never taken for one that no longer holds its locator).
pub(super) fn newest(
    inner: &Inner,
    sv: &SuperVersion,
    key: &[u8],
    seq: Seq,
    report: bool,
) -> Result<Option<(Kind, Bytes)>> {
    for mem in sv.memtables() {
        // A memtable applies its own range tombstones, and every older source holds
        // only older versions, so a hit or a tombstone here is final.
        match mem.mem.get(key, seq) {
            MemLookup::Found(kind, value, _) => return Ok(Some((kind, value))),
            MemLookup::Deleted(_) => return Ok(None),
            MemLookup::NotFound => {}
        }
    }
    let damage = |e: Error| {
        if report { run_damage(inner, e, key) } else { e }
    };
    let version = &sv.version;
    let mut tombstone: Option<Seq> = None;
    let deeper = version.levels[1..]
        .iter()
        .enumerate()
        .filter_map(|(i, _)| version.find_in_level(i + 1, key));
    for file in version.levels[0]
        .iter()
        .filter(|r| r.contains(key))
        .chain(deeper)
    {
        let run = match file.pinned() {
            Some(run) => run.clone(),
            None => inner.table_cache.get(file.id()).map_err(damage)?,
        };
        let covering = if run.meta().num_range_tombstones == 0 {
            None
        } else {
            run_tombstones(file, &run).max_covering_seq(key, seq)
        };
        if let Some(t) = covering {
            tombstone = Some(tombstone.map_or(t, |old| old.max(t)));
        }
        match run.get(key, seq, true).map_err(damage)? {
            RunLookup::Found {
                kind,
                value,
                seq: at,
            } => {
                return Ok(if tombstone.is_some_and(|t| t > at) {
                    None
                } else {
                    Some((kind, value))
                });
            }
            RunLookup::Deleted { .. } => return Ok(None),
            RunLookup::NotFound => {}
        }
    }
    Ok(None)
}

/// G2 at read time: the key's scope is quarantined (store-wide latches).
fn run_damage(inner: &Inner, error: Error, key: &[u8]) -> Error {
    let file = match &error {
        Error::Corruption { file, .. } => *file,
        Error::Io(std::io::ErrorKind::NotFound) => FileId::Other,
        _ => return error,
    };
    let file = match file {
        FileId::Run { .. } => file,
        _ => return error,
    };
    inner.report_damage(Damage {
        scope: inner.scope_bytes(key),
        reason: DamageReason::RunBlock,
        file,
        first_bad_seq: None,
    });
    error
}

impl Snapshot {
    pub(super) fn new(
        inner: Arc<Inner>,
        seq: Seq,
        handle: Registered,
        sv: Arc<SuperVersion>,
    ) -> Snapshot {
        Snapshot {
            inner,
            seq,
            sv,
            _handle: Arc::new(handle),
        }
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// The one group scope run `id` of this snapshot's version lies in, or the empty
    /// (store-wide) scope when it holds store-wide keys, spans scopes or is not found.
    fn run_scope(&self, id: u64) -> Bytes {
        self.sv
            .version
            .runs()
            .find(|r| r.id() == id)
            .and_then(|r| single_scope(self.inner.scope_of, &r.lo, &r.hi, r.hi_exclusive))
            .unwrap_or_default()
    }

    /// Ids of the runs this snapshot pins (`<id>.run` under `runs/`), in no order. For
    /// tests that check a pinned run's file is never deleted (NS03).
    #[doc(hidden)]
    pub fn run_ids(&self) -> Vec<u64> {
        self.sv.version.runs().map(|r| r.id()).collect()
    }

    pub(super) fn super_version(&self) -> &Arc<SuperVersion> {
        &self.sv
    }

    /// The value of `key` at this snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.quarantine.check(key)?;
        let found = self.find(key)?;
        match found {
            None => Ok(None),
            Some((Kind::JournalPut, locator)) => self.resolve(key, &locator).map(Some),
            Some((_, value)) => Ok(Some(value)),
        }
    }

    /// The newest visible point version: `(kind, stored value)`.
    fn find(&self, key: &[u8]) -> Result<Option<(Kind, Bytes)>> {
        newest(&self.inner, &self.sv, key, self.seq, true)
    }

    fn resolve(&self, key: &[u8], stored: &Bytes) -> Result<Bytes> {
        let locator = Locator::decode(stored).ok_or(Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "malformed journal value locator",
        })?;
        self.inner
            .journal()
            .read_value(&locator)
            .map_err(|e| value_damage(&self.inner, e, key))
    }

    /// An iterator over user keys within the bounds at this snapshot. Only sources that
    /// may hold keys in the bounds are opened. `fill_cache = false` reads blocks without
    /// caching them (scans).
    pub fn iter(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>, fill_cache: bool) -> Iter {
        let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
        let mut tombstones = RangeDelAggregator::new(self.seq);
        for mem in self.sv.memtables() {
            children.push(Box::new(mem.mem.iter_range(lower, upper)));
            if mem.mem.tombstone_count() > 0 {
                tombstones.add(mem.mem.fragmented_tombstones());
            }
        }
        let lo: &[u8] = match lower {
            Bound::Included(k) | Bound::Excluded(k) => k,
            Bound::Unbounded => &[],
        };
        let hi: Option<&[u8]> = match upper {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        };
        // L0 runs overlap each other: one child each (they are pinned open anyway).
        for file in self.sv.version.levels[0]
            .iter()
            .filter(|r| r.overlaps(lo, hi))
        {
            let run = match file.pinned() {
                Some(run) => Ok(run.clone()),
                None => self.inner.table_cache.get(file.id()),
            };
            match run {
                Ok(run) => {
                    if run.meta().num_range_tombstones > 0 {
                        tombstones.add(run_tombstones(file, &run));
                    }
                    children.push(Box::new(run.iter(fill_cache)));
                }
                Err(e) => children.push(Box::new(FailedIter(e))),
            }
        }
        // Deeper levels: one lazy child per level, which opens one run at a time. Range
        // tombstones are gathered up front from each run's cached set (a run whose set is
        // not cached yet is opened once through the table cache, and not held).
        for level in &self.sv.version.levels[1..] {
            let runs: Vec<Arc<RunFile>> = level
                .iter()
                .filter(|r| r.overlaps(lo, hi))
                .cloned()
                .collect();
            if runs.is_empty() {
                continue;
            }
            let mut failed = None;
            for file in &runs {
                let set = match file.cached_tombstones() {
                    Some(set) => set,
                    None => match self.inner.table_cache.get(file.id()) {
                        Ok(run) => run_tombstones(file, &run),
                        Err(e) => {
                            failed = Some(e);
                            break;
                        }
                    },
                };
                if !set.is_empty() {
                    tombstones.add(set);
                }
            }
            match failed {
                Some(e) => children.push(Box::new(FailedIter(e))),
                None => children.push(Box::new(LevelIter::new(
                    self.inner.clone(),
                    runs,
                    fill_cache,
                ))),
            }
        }
        let resolver: Arc<dyn ValueResolver> =
            Arc::new(JournalResolver(self.inner.journal().clone()));
        let db = DbIter::new(MergingIterator::new(children), self.seq, lower, upper)
            .with_range_tombstones(tombstones)
            .with_resolver(resolver)
            .with_fill_cache(fill_cache);
        let scope = match (lower, upper) {
            (Bound::Included(l) | Bound::Excluded(l), Bound::Included(u) | Bound::Excluded(u)) => {
                single_scope(
                    self.inner.scope_of,
                    l,
                    u,
                    matches!(upper, Bound::Excluded(_)),
                )
            }
            _ => None,
        };
        Iter {
            inner: db,
            snapshot: self.clone(),
            scope,
            lower: owned(lower),
            upper: owned(upper),
            error: None,
            reported: false,
        }
    }
}

/// G3: a journal value failed its check (`VALUE_OUT_OF_RANGE`, `VALUE_CHECKSUM`). Its
/// key's scope is quarantined (store-wide latches, R5). `VALUE_RECYCLED` is a journal GC
/// invariant violation instead: it latches and is counted, and quarantines nothing.
fn value_damage(inner: &Inner, error: Error, key: &[u8]) -> Error {
    if let Error::Corruption {
        file: file @ FileId::JournalValue { .. },
        what,
        ..
    } = error
    {
        if what == VALUE_RECYCLED {
            // Not media damage: GC retired a segment a reader could still reach.
            inner.recycled_value();
            return error;
        }
        inner.report_damage(Damage {
            scope: inner.scope_bytes(key),
            reason: DamageReason::JournalValue,
            file,
            first_bad_seq: None,
        });
    }
    error
}

/// A merged, snapshot-filtered iterator over user keys. Journal values are read lazily
/// by [`Iter::value`].
pub struct Iter {
    inner: DbIter<MergingIterator>,
    snapshot: Snapshot,
    /// The one scope the bounds lie in, if any: run damage found by this iterator is
    /// attributed to it.
    scope: Option<Bytes>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    error: Option<Error>,
    reported: bool,
}

fn owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    bound.map(<[u8]>::to_vec)
}

/// The larger of two lower bounds.
fn max_lower<'a>(a: Bound<&'a [u8]>, b: Bound<&'a [u8]>) -> Bound<&'a [u8]> {
    match (a, b) {
        (Bound::Unbounded, x) | (x, Bound::Unbounded) => x,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y))
            if x != y =>
        {
            if x > y {
                a
            } else {
                b
            }
        }
        (Bound::Excluded(_), _) => a,
        _ => b,
    }
}

/// The smaller of two upper bounds.
fn min_upper<'a>(a: Bound<&'a [u8]>, b: Bound<&'a [u8]>) -> Bound<&'a [u8]> {
    match (a, b) {
        (Bound::Unbounded, x) | (x, Bound::Unbounded) => x,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y))
            if x != y =>
        {
            if x < y {
                a
            } else {
                b
            }
        }
        (Bound::Excluded(_), _) => a,
        _ => b,
    }
}

impl Iter {
    /// After a move that started at `from` (a seek target, a bound, or the key it left):
    /// the move crossed the range between `from` and where it stopped, in `forward`
    /// direction. Any quarantined scope in that range fails the iterator, whether or not
    /// it landed on one of the scope's keys (G1, G2: reads in a quarantined range return
    /// `Corruption`, never the rest of the range as if the scope were empty).
    ///
    /// `from` is `None` for a step that started before anything was quarantined: only
    /// the key it lands on is checked then.
    fn after_move(&mut self, from: Option<Bound<&[u8]>>, forward: bool) {
        self.error = None;
        let quarantine = &self.snapshot.inner.quarantine;
        if quarantine.len() > 0 && self.inner.status().is_ok() {
            // A step that started before the quarantine: check only where it stopped.
            let from = match from {
                Some(from) => Some(from),
                None if self.inner.valid() => Some(Bound::Included(self.inner.key())),
                None => None,
            };
            let stop = if self.inner.valid() {
                Bound::Included(self.inner.key())
            } else if forward {
                self.upper.as_ref().map(Vec::as_slice)
            } else {
                self.lower.as_ref().map(Vec::as_slice)
            };
            let crossed = match from {
                Some(from) if forward => quarantine.overlaps(from, stop),
                Some(from) => quarantine.overlaps(stop, from),
                None => false,
            };
            if crossed {
                self.error = Some(quarantined_error());
                return;
            }
        }
        if self.inner.valid() {
            return;
        }
        if let Err(error) = self.inner.status()
            && !self.reported
            && let Error::Corruption {
                file: file @ FileId::Run { id },
                ..
            } = error
        {
            self.reported = true;
            // G2: the bounds' scope when they lie in one, else the damaged run's range
            // (plan section 3, "else the run's range"). A run in no single group scope
            // (store-wide keys, or spanning scopes) reports the empty scope, which latches
            // R5, as compaction does when it meets the same run (NS5 deviation 7).
            let scope = match &self.scope {
                Some(scope) => scope.clone(),
                None => self.snapshot.run_scope(id),
            };
            self.snapshot.inner.report_damage(Damage {
                scope,
                reason: DamageReason::RunBlock,
                file,
                first_bad_seq: None,
            });
        }
    }

    pub fn valid(&self) -> bool {
        self.error.is_none() && self.inner.valid()
    }

    /// The key the iterator is on, kept across a step only while something is
    /// quarantined (the start of the range the step crosses).
    fn step_start(&self) -> Option<Vec<u8>> {
        (self.snapshot.inner.quarantine.len() > 0 && self.inner.valid())
            .then(|| self.inner.key().to_vec())
    }

    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        let lower = self.lower.clone();
        self.after_move(Some(lower.as_ref().map(Vec::as_slice)), true);
    }

    pub fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
        let upper = self.upper.clone();
        self.after_move(Some(upper.as_ref().map(Vec::as_slice)), false);
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target);
        let lower = self.lower.clone();
        let from = max_lower(Bound::Included(target), lower.as_ref().map(Vec::as_slice));
        self.after_move(Some(from), true);
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.inner.seek_for_prev(target);
        let upper = self.upper.clone();
        let from = min_upper(Bound::Included(target), upper.as_ref().map(Vec::as_slice));
        self.after_move(Some(from), false);
    }

    /// Requires `valid()`.
    pub fn next(&mut self) {
        let start = self.step_start();
        self.inner.next();
        self.after_move(start.as_deref().map(Bound::Excluded), true);
    }

    /// Requires `valid()`.
    pub fn prev(&mut self) {
        let start = self.step_start();
        self.inner.prev();
        self.after_move(start.as_deref().map(Bound::Excluded), false);
    }

    /// The current user key. Requires `valid()`.
    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    /// The current user value, reading a journal value through the journal.
    pub fn value(&self) -> Result<Bytes> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.inner
            .value()
            .map_err(|e| value_damage(&self.snapshot.inner, e, self.inner.key()))
    }

    pub fn status(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => self.inner.status(),
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
}
