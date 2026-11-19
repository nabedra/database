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
use crate::journal::Journal;
use crate::memtable::{Lookup as MemLookup, SnapshotHandle};
use crate::quarantine::{quarantined_error, single_scope};
use crate::run::{Lookup as RunLookup, Run};
use crate::source::InternalIterator;
use crate::types::{Kind, Locator, MAX_SEQ, Seq};
use crate::version::{RunFile, SuperVersion};

use super::Inner;

/// A consistent read view. Cheap to clone; a clone registers the sequence again.
#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<Inner>,
    seq: Seq,
    sv: Arc<SuperVersion>,
    _handle: Arc<SnapshotHandle>,
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
    file.tombstones(|| {
        Arc::new(FragmentedRangeTombstones::new(
            run.range_tombstones(),
            MAX_SEQ,
        ))
    })
}

impl Snapshot {
    pub(super) fn new(
        inner: Arc<Inner>,
        seq: Seq,
        handle: SnapshotHandle,
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
        let seq = self.seq;
        for mem in self.sv.memtables() {
            // A memtable applies its own range tombstones, and every older source holds
            // only older versions, so a hit or a tombstone here is final.
            match mem.mem.get(key, seq) {
                MemLookup::Found(kind, value, _) => return Ok(Some((kind, value))),
                MemLookup::Deleted(_) => return Ok(None),
                MemLookup::NotFound => {}
            }
        }
        let version = &self.sv.version;
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
            let run = self.open_run(file, key)?;
            if let Some(t) = run.max_covering_tombstone_seq(key, seq) {
                tombstone = Some(tombstone.map_or(t, |old| old.max(t)));
            }
            let lookup = run
                .get(key, seq, true)
                .map_err(|e| self.run_damage(e, key))?;
            match lookup {
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

    fn open_run(&self, file: &RunFile, key: &[u8]) -> Result<Arc<Run>> {
        self.inner
            .table_cache
            .get(file.id())
            .map_err(|e| self.run_damage(e, key))
    }

    /// G2 at read time: the key's scope is quarantined (store-wide latches).
    fn run_damage(&self, error: Error, key: &[u8]) -> Error {
        let file = match &error {
            Error::Corruption { file, .. } => *file,
            Error::Io(std::io::ErrorKind::NotFound) => FileId::Other,
            _ => return error,
        };
        let file = match file {
            FileId::Run { .. } => file,
            _ => return error,
        };
        self.inner.report_damage(Damage {
            scope: self.inner.scope_bytes(key),
            reason: DamageReason::RunBlock,
            file,
            first_bad_seq: None,
        });
        error
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
        for file in self.sv.version.runs().filter(|r| r.overlaps(lo, hi)) {
            match self.inner.table_cache.get(file.id()) {
                Ok(run) => {
                    tombstones.add(run_tombstones(file, &run));
                    children.push(Box::new(run.iter(fill_cache)));
                }
                Err(e) => children.push(Box::new(FailedIter(e))),
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
            error: None,
            reported: false,
        }
    }
}

/// G3: a journal value failed its check. Its key's scope is quarantined (store-wide
/// latches, R5).
fn value_damage(inner: &Inner, error: Error, key: &[u8]) -> Error {
    if let Error::Corruption {
        file: file @ FileId::JournalValue { .. },
        ..
    } = error
    {
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
    error: Option<Error>,
    reported: bool,
}

impl Iter {
    fn after_move(&mut self) {
        self.error = None;
        if self.inner.valid() {
            let quarantine = &self.snapshot.inner.quarantine;
            if quarantine.len() > 0 && quarantine.check(self.inner.key()).is_err() {
                self.error = Some(quarantined_error());
            }
            return;
        }
        if let Err(error) = self.inner.status()
            && !self.reported
            && let Error::Corruption {
                file: file @ FileId::Run { .. },
                ..
            } = error
        {
            self.reported = true;
            let damage = Damage {
                scope: self.scope.clone().unwrap_or_default(),
                reason: DamageReason::RunBlock,
                file,
                first_bad_seq: None,
            };
            if self.scope.is_some() {
                self.snapshot.inner.report_damage(damage);
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.error.is_none() && self.inner.valid()
    }

    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.after_move();
    }

    pub fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
        self.after_move();
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target);
        self.after_move();
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.inner.seek_for_prev(target);
        self.after_move();
    }

    /// Requires `valid()`.
    pub fn next(&mut self) {
        self.inner.next();
        self.after_move();
    }

    /// Requires `valid()`.
    pub fn prev(&mut self) {
        self.inner.prev();
        self.after_move();
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
