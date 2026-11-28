//! The impact index of one search worker for one `full_text` column or one sparse vector column:
//! block-max inverted lists for BM25 and for sparse `dot` and `cosine`, in one format, ranked by
//! MaxScore (`plans/oltp-vector.md`, section 5, "BM25 and sparse").
//!
//! It sits beside the dense index of `index/` and keeps its rules (`specs/vector/index/LocalIndex`):
//! the same row map of slots and versions, generations per range, pins that fix a read point per
//! range and the list of segments in one step, retention of older cuts, dedup by slot, and
//! checkpoints that pin like a scan. Segments take the part of partitions. New entries go to the
//! head segment. A merge copies the entries that a cut may still ask for (`Wanted`) into a new
//! segment in one step, and the segments it merged retire and wait for every reader that began
//! before. Entries of a dropped range are shown to no pin at once, and go with the next merge of
//! their segment.
//!
//! A row ranks on a column only if it shares a term (an index with a nonzero weight) with the query.
//! Scores never come from pruning: MaxScore only skips documents whose bound cannot reach the
//! result, and the score of a returned row is one sum in f64 over the query's terms in a fixed
//! order, rounded once (decision L1). So the result is exact, also without `exact`.

mod checkpoint;
pub mod fusion;
mod mutant;
mod postings;
mod rows;
mod search;

#[cfg(test)]
mod tests;

pub use checkpoint::{Checkpoints, copy_checkpoint};
pub use postings::{BlockRef, PostingStore, RamStore, Term};
pub use search::{Hit, Op, Probe, Query, Scan, Stats};

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::index::{RangeStatus, ReadAt};
use crate::text::Analyzer;
use crate::types::{ColumnType, CommitTs, Error, Key, Pos, RangeId, Result, Value};
use checkpoint::Running;
use mutant::mutant;
use postings::{Dict, Entry, SegState, Segment};
use rows::{Gen, Point, Rows, Slot, Stored};

/// The most distinct terms of a stored document or of a query.
pub const MAX_DOC_TERMS: usize = 1 << 20;
pub const MAX_QUERY_TERMS: usize = 4096;

/// What a column holds.
#[derive(Clone, PartialEq, Debug)]
pub enum Kind {
    /// Text of a `full_text` column in a language of [`crate::text::LANGUAGES`].
    Text(String),
    Sparse,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub kind: Kind,
    /// The filter columns, in the order of a row's `cols`.
    pub columns: Vec<ColumnType>,
    /// Entries of the head segment before a new one starts.
    pub segment_rows: usize,
    /// Segments of about the same size that a merge joins.
    pub merge_factor: usize,
    /// A merge stops growing a segment here.
    pub max_segment_rows: usize,
}

impl Config {
    pub fn new(kind: Kind, columns: Vec<ColumnType>) -> Config {
        Config {
            kind,
            columns,
            segment_rows: 4096,
            merge_factor: 8,
            max_segment_rows: 1 << 20,
        }
    }

    fn check(&self) -> Result<()> {
        let sizes = self.segment_rows > 0
            && self.merge_factor > 1
            && self.max_segment_rows >= self.segment_rows
            && u32::try_from(self.max_segment_rows).is_ok();
        if !sizes {
            return Err(Error::Internal("invalid impact index configuration"));
        }
        Ok(())
    }
}

/// The document of a row on this column.
#[derive(Clone, PartialEq, Debug)]
pub enum Doc {
    Text(Arc<str>),
    /// Indices strictly ascending, one finite weight each.
    Sparse {
        indices: Arc<[u32]>,
        weights: Arc<[f32]>,
    },
}

/// A row image of the feed.
#[derive(Clone, PartialEq, Debug)]
pub struct Row {
    /// `None`: the row has no value here and never ranks on this column.
    pub doc: Option<Doc>,
    /// One value per filter column.
    pub cols: Vec<Value>,
    /// The complete row, returned with a hit.
    pub image: Arc<[u8]>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Change {
    /// `None` deletes the key.
    Row {
        key: Key,
        commit_ts: CommitTs,
        row: Option<Row>,
    },
    /// No later entry of the range commits at or below this timestamp.
    Closed(CommitTs),
}

#[derive(Clone, PartialEq, Debug)]
pub struct Record {
    pub at: Pos,
    pub change: Change,
}

/// Contiguous changes of one range, `(after, through]` in log order.
#[derive(Clone, PartialEq, Debug)]
pub struct Page {
    pub after: Pos,
    pub through: Pos,
    pub records: Vec<Record>,
}

/// The analysis of a column.
#[derive(Clone)]
enum Analysis {
    Text(Analyzer),
    Sparse,
}

impl Analysis {
    fn identity(&self) -> String {
        match self {
            Analysis::Text(a) => a.format(),
            Analysis::Sparse => "sparse/1".to_owned(),
        }
    }
}

/// The newest versions of a range: documents, words, and documents per term.
#[derive(Clone, Default)]
struct Counts {
    docs: u64,
    words: u64,
    df: HashMap<Term, u64>,
}

impl Counts {
    fn add(&mut self, row: &Stored, text: bool) {
        self.docs = self.docs.saturating_add(1);
        if text {
            self.words = self.words.saturating_add(row.norm as u64);
            for (t, _) in row.terms.iter() {
                let n = self.df.entry(*t).or_default();
                *n = n.saturating_add(1);
            }
        }
    }

    fn remove(&mut self, row: &Stored, text: bool) {
        self.docs = self.docs.saturating_sub(1);
        if text {
            self.words = self.words.saturating_sub(row.norm as u64);
            for (t, _) in row.terms.iter() {
                if let Some(n) = self.df.get_mut(t) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        self.df.remove(t);
                    }
                }
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
enum Service {
    /// A range snapshot at this position is being installed.
    Loading(Pos),
    /// Installed or recovered. A fresh barrier was not reached yet.
    CatchingUp,
    Serving,
    Out(String),
}

#[derive(Clone)]
struct Range {
    generation: Gen,
    service: Service,
    /// Every entry up to here is in a segment.
    app: Pos,
    /// The oldest position a scan may ask for.
    low: Pos,
    /// The oldest servable commit timestamp.
    bound: CommitTs,
    /// The newest closed timestamp at or below `app`.
    closed: CommitTs,
    /// Versions above `low` that superseded another one.
    touches: VecDeque<(Pos, CommitTs, Slot)>,
    keys: BTreeMap<Key, Slot>,
    counts: Counts,
}

/// A read point per range and the list of segments, fixed in one step. Dropping it ends it.
#[derive(Clone)]
pub struct Pin(Arc<PinInner>);

struct PinInner {
    id: u64,
    points: BTreeMap<Gen, Point>,
    ranges: Vec<(RangeId, Pos)>,
    segments: Vec<u64>,
    /// An index that it names was replaced.
    failed: AtomicBool,
}

impl Pin {
    /// The applied position of every range it covers.
    pub fn ranges(&self) -> &[(RangeId, Pos)] {
        &self.0.ranges
    }
}

/// The running scans and the checkpoint, by age.
#[derive(Clone, Default)]
struct Readers {
    next: u64,
    live: BTreeMap<u64, Weak<PinInner>>,
}

impl Readers {
    /// Names every reader that began so far.
    fn epoch(&self) -> u64 {
        self.next
    }

    /// Every reader of this epoch ended.
    fn past(&self, epoch: u64) -> bool {
        mutant("free_without_wait") || self.live.range(..epoch).all(|(_, r)| r.strong_count() == 0)
    }

    fn live(&self) -> impl Iterator<Item = Arc<PinInner>> + '_ {
        self.live.values().filter_map(Weak::upgrade)
    }

    fn begin(&mut self, f: impl FnOnce(u64) -> PinInner) -> Arc<PinInner> {
        self.live.retain(|_, r| r.strong_count() > 0);
        let id = self.next;
        self.next += 1;
        let pin = Arc::new(f(id));
        self.live.insert(id, Arc::downgrade(&pin));
        pin
    }

    /// Fails every reader that names a generation.
    fn fail(&mut self, generation: Gen) {
        let failed: Vec<_> = self
            .live()
            .filter(|r| r.points.contains_key(&generation))
            .collect();
        for r in failed {
            r.failed.store(true, Ordering::Release);
            self.live.remove(&r.id);
        }
    }
}

#[cfg_attr(test, derive(Clone))]
pub struct ImpactIndex<S: PostingStore = RamStore> {
    cfg: Config,
    analysis: Analysis,
    ranges: BTreeMap<RangeId, Range>,
    gens: BTreeMap<Gen, RangeId>,
    rows: Rows,
    dict: Dict,
    segments: BTreeMap<u64, Segment>,
    /// The segment that takes new entries.
    head: Option<u64>,
    next_segment: u64,
    next_gen: Gen,
    readers: Readers,
    store: S,
    checkpoints: Option<Checkpoints>,
    checkpoint: Option<Running>,
    /// Where `maintain` looks next.
    cursor: u64,
}

impl<S: PostingStore> ImpactIndex<S> {
    /// An empty index. `store` keeps the postings and `checkpoints` the checkpoints.
    pub fn new(cfg: Config, store: S, checkpoints: Option<Checkpoints>) -> Result<Self> {
        cfg.check()?;
        let analysis = match &cfg.kind {
            Kind::Text(language) => Analysis::Text(Analyzer::new(language)?),
            Kind::Sparse => Analysis::Sparse,
        };
        Ok(ImpactIndex {
            cfg,
            analysis,
            ranges: BTreeMap::new(),
            gens: BTreeMap::new(),
            rows: Rows::default(),
            dict: Dict::default(),
            segments: BTreeMap::new(),
            head: None,
            next_segment: 0,
            next_gen: 0,
            readers: Readers::default(),
            store,
            checkpoints,
            checkpoint: None,
            cursor: 0,
        })
    }

    fn text(&self) -> bool {
        matches!(self.analysis, Analysis::Text(_))
    }

    // ----- Ranges: install, feed, retention, drop.

    fn range(&self, id: RangeId) -> Result<&Range> {
        let r = self.ranges.get(&id).ok_or(Error::NotHeld(id))?;
        match &r.service {
            Service::Out(reason) => Err(Error::OutOfService {
                range: id,
                reason: reason.clone(),
            }),
            _ => Ok(r),
        }
    }

    fn range_mut(&mut self, id: RangeId) -> Result<&mut Range> {
        self.range(id)?;
        self.ranges.get_mut(&id).ok_or(Error::NotHeld(id))
    }

    fn range_of(&self, generation: Gen) -> Option<&Range> {
        self.ranges.get(self.gens.get(&generation)?)
    }

    pub fn status(&self, id: RangeId) -> Result<RangeStatus> {
        let r = self.ranges.get(&id).ok_or(Error::NotHeld(id))?;
        let damage = match &r.service {
            Service::Out(reason) => Some(reason.clone()),
            _ => None,
        };
        Ok(RangeStatus {
            serving: r.service == Service::Serving,
            applied: r.app,
            low: r.low,
            delivered: match r.service {
                Service::Loading(at) => at,
                _ => r.app,
            },
            bound: r.bound,
            closed: r.closed,
            damage,
        })
    }

    fn new_range(&mut self, id: RangeId, service: Service, pos: Pos, low: Pos) -> Gen {
        self.next_gen += 1;
        self.gens.insert(self.next_gen, id);
        let range = Range {
            generation: self.next_gen,
            service,
            app: pos,
            low,
            bound: 0,
            closed: 0,
            touches: VecDeque::new(),
            keys: BTreeMap::new(),
            counts: Counts::default(),
        };
        self.ranges.insert(id, range);
        self.next_gen
    }

    /// Replaces what is held of a range by a generation that a range snapshot at position `at` fills:
    /// `begin_load`, `load_rows`, `finish_load`. `bound` is the largest commit timestamp in the
    /// snapshot, deletes included.
    pub fn begin_load(&mut self, id: RangeId, at: Pos, bound: CommitTs) -> Result<()> {
        self.drop_range(id)?;
        self.new_range(id, Service::Loading(at), 0, 0);
        self.range_mut(id)?.bound = bound;
        Ok(())
    }

    /// Adds rows of the range snapshot.
    pub fn load_rows(&mut self, id: RangeId, rows: &[(Key, CommitTs, Row)]) -> Result<()> {
        let Service::Loading(at) = self.range(id)?.service else {
            return Err(Error::InvalidFeed("no snapshot is being installed"));
        };
        for (key, ts, row) in rows {
            let known = self.range(id)?.keys.contains_key(key);
            if known || self.put_version(id, key, at, *ts, Some(row)).is_err() {
                self.fail_range(id, "the snapshot repeats a key or holds an invalid row");
                return Err(Error::InvalidFeed("invalid snapshot"));
            }
        }
        Ok(())
    }

    /// The snapshot is whole.
    pub fn finish_load(&mut self, id: RangeId) -> Result<()> {
        let r = self.range_mut(id)?;
        let Service::Loading(at) = r.service else {
            return Err(Error::InvalidFeed("no snapshot is being installed"));
        };
        (r.app, r.low, r.service) = (at, at, Service::CatchingUp);
        Ok(())
    }

    /// Serves the range once it applied a barrier taken after it was installed or recovered.
    pub fn caught_up(&mut self, id: RangeId, barrier: Pos) -> Result<()> {
        let r = self.range_mut(id)?;
        if matches!(r.service, Service::Loading(_)) || r.app < barrier {
            return Err(Error::Behind {
                range: id,
                applied: r.app,
            });
        }
        r.service = Service::Serving;
        Ok(())
    }

    fn fits(&self, row: &Row) -> bool {
        let doc = match (&row.doc, &self.analysis) {
            (None, _) => true,
            (Some(Doc::Text(_)), Analysis::Text(_)) => true,
            (Some(Doc::Sparse { indices, weights }), Analysis::Sparse) => {
                sparse_fits(indices, weights)
            }
            _ => false,
        };
        doc && row.cols.len() == self.cfg.columns.len()
            && row
                .cols
                .iter()
                .zip(&self.cfg.columns)
                .all(|(v, t)| v.kind() == *t && !matches!(v, Value::Float(x) if !x.is_finite()))
    }

    /// The stored form of a document. Text terms are interned, and the caller holds or sweeps them.
    fn analyze(&mut self, doc: &Doc, row: &Row, new: &mut Vec<Term>) -> Result<Stored> {
        const LARGE: Error = Error::InvalidFeed("a document has too many terms");
        let (terms, norm) = match (doc, &self.analysis) {
            (Doc::Text(text), Analysis::Text(a)) => {
                let t = a.terms(text);
                if t.terms.len() > MAX_DOC_TERMS {
                    return Err(LARGE);
                }
                let mut terms = Vec::with_capacity(t.terms.len());
                for (name, n) in &t.terms {
                    let known = self.dict.get(name);
                    let id = self.dict.intern(name)?;
                    if known.is_none() {
                        new.push(id);
                    }
                    terms.push((id, *n as f32));
                }
                terms.sort_unstable_by_key(|t| t.0);
                (terms, t.words as f64)
            }
            (Doc::Sparse { indices, weights }, Analysis::Sparse) => {
                if indices.len() > MAX_DOC_TERMS {
                    return Err(LARGE);
                }
                let norm = weights
                    .iter()
                    .map(|w| f64::from(*w).powi(2))
                    .sum::<f64>()
                    .sqrt();
                let terms = indices.iter().zip(weights.iter());
                let terms = terms.filter(|(_, w)| **w != 0.0).map(|(i, w)| (*i, *w));
                (terms.collect(), norm)
            }
            _ => return Err(Error::InvalidFeed("a row does not fit the column")),
        };
        Ok(Stored {
            terms: terms.into(),
            norm,
            cols: row.cols.clone(),
            image: row.image.clone(),
        })
    }

    /// Adds a version to the row map and, if it has a document, its entry to the head segment.
    fn put_version(
        &mut self,
        id: RangeId,
        key: &Key,
        at: Pos,
        ts: CommitTs,
        row: Option<&Row>,
    ) -> Result<()> {
        const ORDER: Error = Error::InvalidFeed("a key's versions are out of order");
        if row.is_some_and(|row| !self.fits(row)) {
            return Err(Error::InvalidFeed("a row does not fit the schema"));
        }
        let r = self.ranges.get(&id).ok_or(Error::NotHeld(id))?;
        let doc = row.and_then(|row| Some((row.doc.as_ref()?, row)));
        if doc.is_none() && !r.keys.contains_key(key) {
            return Ok(());
        }
        if !self.rows.follows(&r.keys, key, at, ts) {
            return Err(ORDER);
        }
        let mut new = Vec::new();
        let stored = match doc {
            Some((doc, row)) => match self.analyze(doc, row, &mut new) {
                Ok(s) => Some(Arc::new(s)),
                Err(e) => {
                    self.dict.sweep(&new);
                    return Err(e);
                }
            },
            None => None,
        };
        let text = self.text();
        let r = self.ranges.get_mut(&id).ok_or(Error::NotHeld(id))?;
        let generation = r.generation;
        let before = r
            .keys
            .get(key)
            .and_then(|&s| self.rows.newest(s)?.row.clone());
        let inserted = self
            .rows
            .insert(&mut r.keys, generation, key, (at, ts, stored.clone()));
        let Some((slot, superseded)) = inserted else {
            self.dict.sweep(&new);
            return Err(ORDER);
        };
        if superseded {
            r.touches.push_back((at, ts, slot));
        }
        if let Some(b) = before {
            r.counts.remove(&b, text);
        }
        if let Some(s) = &stored {
            r.counts.add(s, text);
            self.rows.name(slot, at);
            let entry = Entry {
                slot,
                at,
                generation,
                norm: s.norm,
            };
            let head = self.head_segment();
            self.append(head, entry, &s.terms)?;
        }
        self.dict.sweep(&new);
        Ok(())
    }

    /// The head segment, a new one when it is full or there is none.
    fn head_segment(&mut self) -> u64 {
        let full = |s: &Segment| s.entries.len() >= self.cfg.segment_rows;
        match self.head {
            Some(h) if self.segments.get(&h).is_some_and(|s| !full(s)) => h,
            _ => {
                let h = self.new_segment();
                self.head = Some(h);
                h
            }
        }
    }

    fn new_segment(&mut self) -> u64 {
        let id = self.next_segment;
        self.next_segment += 1;
        self.segments.insert(id, Segment::new());
        id
    }

    fn append(&mut self, seg: u64, entry: Entry, terms: &[(Term, f32)]) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&seg)
            .ok_or(Error::Internal("a segment is gone"))?;
        let mut held = Vec::new();
        let pushed = segment.push(&mut self.store, entry, terms, &mut held);
        if self.text() {
            for t in held {
                self.dict.hold(t);
            }
        }
        pushed
    }

    /// Takes a page of the feed and returns the position it took. Pages may repeat and overlap,
    /// never skip.
    pub fn apply(&mut self, id: RangeId, page: &Page) -> Result<Pos> {
        let r = self.range(id)?;
        let start = match r.service {
            Service::Loading(at) => return Err(Error::Gap { delivered: at }),
            _ => r.app,
        };
        if page.after > start {
            return Err(Error::Gap { delivered: start });
        }
        let in_order = page.records.windows(2).all(|w| w[0].at <= w[1].at);
        let inside = page
            .records
            .iter()
            .all(|r| page.after < r.at && r.at <= page.through);
        if !in_order || !inside {
            return Err(Error::InvalidFeed("a page is not in log order"));
        }
        let fresh = &page.records[page.records.partition_point(|r| r.at <= start)..];
        for record in fresh {
            let put = match &record.change {
                Change::Closed(ts) => {
                    let r = self.range_mut(id)?;
                    r.closed = r.closed.max(*ts);
                    Ok(())
                }
                Change::Row {
                    key,
                    commit_ts,
                    row,
                } => self.put_version(id, key, record.at, *commit_ts, row.as_ref()),
            };
            if let Err(error) = put {
                self.fail_range(id, "the feed is invalid");
                return Err(error);
            }
            self.range_mut(id)?.app = record.at;
        }
        let r = self.range_mut(id)?;
        r.app = r.app.max(page.through);
        Ok(r.app)
    }

    /// `AdvanceLow`: retention moves on, and older cuts are refused from now on.
    pub fn advance_low(&mut self, id: RangeId, low: Pos) -> Result<()> {
        let r = self.ranges.get_mut(&id).ok_or(Error::NotHeld(id))?;
        if matches!(r.service, Service::Loading(_)) || low > r.app {
            return Err(Error::CutTooNew { newest: r.app });
        }
        r.low = r.low.max(low);
        while let Some(&(at, ts, slot)) = r.touches.front()
            && at <= r.low
        {
            r.touches.pop_front();
            r.bound = r.bound.max(ts);
            if self.rows.generation(slot) == Some(r.generation) {
                self.rows.trim(Some(&mut r.keys), slot, r.low);
            }
        }
        Ok(())
    }

    /// `DropRange`: the range moved away, or is about to be installed again. No pin shows its entries
    /// from now on, the checkpoint forgets it on disk first, and scans that pinned it fail.
    pub fn drop_range(&mut self, id: RangeId) -> Result<()> {
        self.checkpoint = None;
        if let Some(store) = &self.checkpoints {
            store.forget(id)?;
        }
        self.discard(id);
        self.ranges.remove(&id);
        Ok(())
    }

    /// Takes the range out of service. Only another install brings it back.
    fn fail_range(&mut self, id: RangeId, reason: &str) {
        self.discard(id);
        if let Some(r) = self.ranges.get_mut(&id) {
            r.service = Service::Out(reason.to_owned());
        }
    }

    fn discard(&mut self, id: RangeId) {
        let Some(r) = self.ranges.get_mut(&id) else {
            return;
        };
        let keys = std::mem::take(&mut r.keys);
        r.touches.clear();
        r.counts = Counts::default();
        self.gens.remove(&r.generation);
        self.readers.fail(r.generation);
        self.checkpoint = None;
        for slot in keys.into_values() {
            self.rows.kill(slot);
        }
    }

    // ----- Pins.

    /// `StartScan`: fixes the read points and the segments in one step, after the caller's barrier.
    /// The rankings of one query on several columns pin every column at the same `Position`.
    pub fn pin(&mut self, ranges: &[(RangeId, ReadAt)]) -> Result<Pin> {
        let mut points = BTreeMap::new();
        let mut pinned = Vec::with_capacity(ranges.len());
        for &(id, read) in ranges {
            let r = self.range(id)?;
            if r.service != Service::Serving {
                return Err(Error::Behind {
                    range: id,
                    applied: r.app,
                });
            }
            let point = point(id, r, read)?;
            points.insert(r.generation, point);
            pinned.push((id, r.app));
        }
        let segments = self.listed();
        let inner = |id| PinInner {
            id,
            points,
            ranges: pinned,
            segments,
            failed: AtomicBool::new(false),
        };
        Ok(Pin(self.readers.begin(inner)))
    }

    /// The segments that a new reader lists.
    fn listed(&self) -> Vec<u64> {
        let ready = |(id, s): (&u64, &Segment)| (s.state == SegState::Ready).then_some(*id);
        self.segments.iter().filter_map(ready).collect()
    }

    // ----- Merges.

    /// Some cut that is or may still be asked for sees this version, or it is not applied yet.
    fn wanted(&self, e: &Entry) -> bool {
        let Some(r) = self.range_of(e.generation) else {
            return false;
        };
        if e.at > r.app {
            return !mutant("merge_ignores_unapplied");
        }
        if mutant("merge_copies_current_only") {
            let now = Point {
                pos: r.app,
                cut: CommitTs::MAX,
            };
            return self.rows.visible(e.slot, e.at, now);
        }
        self.rows
            .successor(e.slot, e.at)
            .is_none_or(|s| s.at > r.low)
    }

    /// Merges segments into a new one in one step: it takes every wanted entry, and they retire.
    fn merge(&mut self, sources: &[u64]) -> Result<()> {
        let mut keep = Vec::new();
        for id in sources {
            let s = self
                .segments
                .get(id)
                .ok_or(Error::Internal("a segment is gone"))?;
            keep.extend(s.entries.iter().filter(|e| self.wanted(e)).copied());
        }
        let target = (!keep.is_empty()).then(|| self.new_segment());
        for e in keep {
            let row = self.rows.version(e.slot, e.at).and_then(|v| v.row.clone());
            let row = row.ok_or(Error::Internal("an entry without its row"))?;
            self.rows.name(e.slot, e.at);
            if let Some(t) = target {
                self.append(t, e, &row.terms)?;
            }
        }
        let waits = self.readers.epoch();
        for id in sources {
            if let Some(s) = self.segments.get_mut(id) {
                s.state = SegState::Retired { waits };
            }
            if self.head == Some(*id) {
                self.head = None;
            }
        }
        Ok(())
    }

    /// Frees a retired segment once every reader that began before it retired ended.
    fn free_segment(&mut self, id: u64) -> bool {
        let Some(s) = self.segments.get(&id) else {
            return false;
        };
        let SegState::Retired { waits } = s.state else {
            return false;
        };
        if !self.readers.past(waits) {
            return false;
        }
        let Some(s) = self.segments.remove(&id) else {
            return false;
        };
        let terms: Vec<Term> = s.free(&mut self.store).collect();
        if self.text() {
            for t in terms {
                self.dict.release(t);
            }
        }
        for e in &s.entries {
            let range = self
                .gens
                .get(&e.generation)
                .and_then(|r| self.ranges.get_mut(r));
            match range {
                Some(r) => self.rows.forget(Some(&mut r.keys), e.slot, e.at, r.low),
                None => self.rows.forget(None, e.slot, e.at, 0),
            }
        }
        true
    }

    /// Background work: frees retired segments, then rewrites one segment that holds many entries no
    /// cut wants, or merges segments of about the same size. Returns whether anything was done.
    pub fn maintain(&mut self) -> Result<bool> {
        let retired: Vec<u64> = (self.segments.iter())
            .filter(|(_, s)| s.state != SegState::Ready)
            .map(|(id, _)| *id)
            .collect();
        let mut done = false;
        for id in retired {
            done |= self.free_segment(id);
        }
        let ready: Vec<(u64, usize)> = (self.segments.iter())
            .filter(|(_, s)| s.state == SegState::Ready)
            .map(|(id, s)| (*id, s.entries.len()))
            .collect();
        // A few segments per call are checked for entries that no cut wants, from where the last call
        // stopped. One where they are a quarter or more is written again without them.
        const SWEEP: usize = 8;
        let start = ready.partition_point(|(id, _)| *id < self.cursor);
        for &(id, n) in ready[start..].iter().chain(&ready[..start]).take(SWEEP) {
            self.cursor = id + 1;
            let unwanted = self
                .segments
                .get(&id)
                .map_or(0, |s| s.entries.iter().filter(|e| !self.wanted(e)).count());
            if unwanted > 0 && unwanted * 4 >= n {
                self.merge(&[id])?;
                return Ok(true);
            }
        }
        // Segments of a tier hold up to `merge_factor` times more entries than those of the one below.
        let tier = |n: usize| {
            let mut t = 0;
            let mut size = self.cfg.segment_rows;
            while n > size {
                size = size.saturating_mul(self.cfg.merge_factor);
                t += 1;
            }
            t
        };
        let mut tiers: BTreeMap<usize, Vec<(u64, usize)>> = BTreeMap::new();
        for &(id, n) in &ready {
            if Some(id) != self.head {
                tiers.entry(tier(n)).or_default().push((id, n));
            }
        }
        for group in tiers.values() {
            if group.len() < self.cfg.merge_factor {
                continue;
            }
            let mut take = Vec::new();
            let mut total = 0usize;
            for &(id, n) in group.iter().take(self.cfg.merge_factor) {
                if total + n > self.cfg.max_segment_rows {
                    break;
                }
                total += n;
                take.push(id);
            }
            if take.len() > 1 {
                self.merge(&take)?;
                return Ok(true);
            }
        }
        Ok(done)
    }

    /// Merges every ready segment into one, as far as the size allows.
    pub fn compact(&mut self) -> Result<()> {
        let ready = self.listed();
        if !ready.is_empty() {
            self.merge(&ready)?;
        }
        while self.maintain()? {}
        Ok(())
    }

    /// Segments, ready and retired.
    pub fn segments(&self) -> (usize, usize) {
        let ready = self.listed().len();
        (ready, self.segments.len() - ready)
    }

    /// The local counts of the newest versions of some ranges, for the terms of a `match` query: the
    /// gateway adds them up over the ranges of a table and sends the sum with each query.
    pub fn counts(&self, ranges: &[RangeId], text: &str) -> Result<Stats> {
        let Analysis::Text(a) = &self.analysis else {
            return Err(Error::InvalidQuery("the column has no full-text index"));
        };
        let terms = a.terms(text).terms;
        if terms.len() > MAX_QUERY_TERMS {
            return Err(Error::InvalidQuery("the query has too many terms"));
        }
        let mut stats = Stats {
            docs: 0,
            words: 0,
            df: vec![0; terms.len()],
        };
        for id in ranges {
            let c = &self.range(*id)?.counts;
            stats.docs = stats.docs.saturating_add(c.docs);
            stats.words = stats.words.saturating_add(c.words);
            for (n, (name, _)) in stats.df.iter_mut().zip(&terms) {
                let t = self.dict.get(name);
                *n = n.saturating_add(t.and_then(|t| c.df.get(&t)).copied().unwrap_or(0));
            }
        }
        Ok(stats)
    }

    /// Distinct terms in the dictionary.
    pub fn terms(&self) -> usize {
        self.dict.len()
    }
}

/// The read point of a range for a pin.
fn point(id: RangeId, r: &Range, read: ReadAt) -> Result<Point> {
    let newest = CommitTs::MAX;
    Ok(match read {
        ReadAt::Latest { barrier } if barrier > r.app => {
            return Err(Error::Behind {
                range: id,
                applied: r.app,
            });
        }
        ReadAt::Latest { .. } => Point {
            pos: r.app,
            cut: newest,
        },
        ReadAt::Position(n) if n < r.low => return Err(Error::CutTooOld { oldest: r.low }),
        ReadAt::Position(n) if n > r.app => return Err(Error::CutTooNew { newest: r.app }),
        ReadAt::Position(pos) => Point { pos, cut: newest },
        ReadAt::Cut(ts) if ts < r.bound => return Err(Error::CutTooOld { oldest: r.bound }),
        ReadAt::Cut(ts) if ts > r.closed => return Err(Error::CutTooNew { newest: r.closed }),
        ReadAt::Cut(cut) => Point { pos: r.app, cut },
    })
}

fn sparse_fits(indices: &[u32], weights: &[f32]) -> bool {
    indices.len() == weights.len()
        && indices.windows(2).all(|w| w[0] < w[1])
        && weights.iter().all(|w| w.is_finite())
}
