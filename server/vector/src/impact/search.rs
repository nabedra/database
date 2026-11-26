//! Rankings of one pin: `match` (BM25) on text, `dot` and `cosine` on sparse vectors. A scan visits
//! one segment per step and walks its lists document by document with MaxScore: lists whose bounds
//! together cannot reach the result's worst score only confirm candidates of the others, and block
//! summaries skip documents and blocks whose bound cannot reach it either. A row is ranked only if
//! it is visible at the pin and matches the filter, before it can take a place in the result.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::Arc;

use super::mutant::mutant;
use super::postings::{Bounds, Cursor, Doc, PostingStore, Segment, Term};
use super::rows::{Slot, Stored};
use super::{Analysis, ImpactIndex, MAX_QUERY_TERMS, Pin, sparse_fits};
use crate::index::Filter;
use crate::types::{CommitTs, Error, Key, MAX_LIMIT, Pos, RangeId, Result, Schema};

/// BM25 parameters, as Lucene and Tantivy.
pub const K1: f64 = 1.2;
pub const B: f64 = 0.75;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    /// BM25 of a text against a `full_text` column.
    Match,
    Dot,
    Cosine,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Probe {
    Text(String),
    /// Indices strictly ascending, one finite weight each.
    Sparse {
        indices: Vec<u32>,
        weights: Vec<f32>,
    },
}

/// Table-wide counts for BM25: rows with a text, their words, and per term of the query (in the
/// order of [`ImpactIndex::counts`]) the rows that hold it. The gateway refreshes them every 5 s and
/// sends the same numbers to every worker, so that all score alike.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Stats {
    pub docs: u64,
    pub words: u64,
    pub df: Vec<u64>,
}

impl Stats {
    /// Adds the counts of other ranges of the same query.
    pub fn add(&mut self, other: &Stats) -> Result<()> {
        if self.df.len() != other.df.len() {
            return Err(Error::InvalidQuery("counts of different queries"));
        }
        self.docs = self.docs.saturating_add(other.docs);
        self.words = self.words.saturating_add(other.words);
        for (a, b) in self.df.iter_mut().zip(&other.df) {
            *a = a.saturating_add(*b);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Query {
    pub op: Op,
    pub probe: Probe,
    pub limit: usize,
    pub filter: Option<Filter>,
    /// Scores every visible row that holds a term of the query, without pruning.
    pub exact: bool,
    /// For `match`; without them the counts of the pinned ranges here.
    pub stats: Option<Stats>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Hit {
    pub range: RangeId,
    pub key: Key,
    pub at: Pos,
    pub commit_ts: CommitTs,
    /// BM25, the dot product or the cosine.
    pub score: f32,
    pub image: Arc<[u8]>,
}

/// One term of a prepared query.
#[derive(Clone, Debug)]
struct QTerm {
    term: Option<Term>,
    /// BM25: `idf · (k1 + 1) · count in the query`. Sparse: the query's weight.
    weight: f64,
}

#[derive(Clone, Debug)]
struct Prepared {
    op: Op,
    /// In a fixed order that does not depend on the worker: text terms by bytes, sparse by index.
    terms: Vec<QTerm>,
    /// BM25: the average number of words.
    avg: f64,
    /// Cosine: the query's length.
    len: f64,
}

impl Prepared {
    fn k(&self, norm: f64) -> f64 {
        K1 * (1.0 - B + B * norm / self.avg)
    }

    /// What a posting adds to the sum of its row.
    fn part(&self, i: usize, value: f32, norm: f64) -> f64 {
        let w = self.terms[i].weight;
        let v = f64::from(value);
        match self.op {
            Op::Match => w * v / (v + self.k(norm)),
            Op::Dot | Op::Cosine => w * v,
        }
    }

    /// The score of a sum of parts.
    fn score(&self, sum: f64, norm: f64) -> f32 {
        let value = match self.op {
            Op::Match | Op::Dot => sum,
            Op::Cosine => (sum / norm / self.len).clamp(-1.0, 1.0),
        };
        round(value)
    }

    /// A part in the units of the score, for bounds.
    fn scaled(&self, part: f64, norm: f64) -> f64 {
        match self.op {
            Op::Cosine => part / norm / self.len,
            _ => part,
        }
    }

    /// The most a posting of term `i` under these bounds adds to a score.
    fn bound(&self, i: usize, b: &Bounds) -> f64 {
        let w = self.terms[i].weight;
        match self.op {
            Op::Match => {
                let v = f64::from(b.max);
                w * v / (v + self.k(b.min_norm))
            }
            Op::Dot => (w * f64::from(b.max)).max(w * f64::from(b.min)),
            Op::Cosine => (w * b.max_per_norm).max(w * b.min_per_norm) / self.len,
        }
    }
}

/// The closest finite f32.
fn round(x: f64) -> f32 {
    // Adding zero turns -0.0 into 0.0, so that they tie.
    (x as f32).clamp(f32::MIN, f32::MAX) + 0.0
}

/// Whether a bound surely stays below the result's worst score. `mass` is the sum of the absolute
/// values of the terms that the bound added up.
///
/// A bound adds parts and block bounds in another order than the score, which sums the parts in the
/// order of the query's terms (decision L1). Each f64 sum of n terms is off by at most n·ε of their
/// absolute mass, and parts of opposite sign can cancel, so the slack follows the mass, never the
/// bound: n is at most `MAX_QUERY_TERMS` + 2, so `SLACK` holds both sums with room to spare. The
/// comparison is of scores as they are returned, rounded to f32, because rounding is monotone: a
/// row whose bound rounds to the worst score may tie with it and win by key, also where f32
/// underflows to zero.
fn below(bound: f64, mass: f64, worst: Option<f64>) -> bool {
    const SLACK: f64 = 1e-9;
    let Some(worst) = worst else {
        return false;
    };
    let slack = if mutant("slack_of_the_bound") {
        (bound.abs() + worst.abs()) * 1e-6
    } else {
        mass.abs() * SLACK
    };
    f64::from(round(bound + slack)) < worst
}

struct Ranked {
    score: f32,
    key: Key,
    slot: Slot,
    at: Pos,
}

impl Ord for Ranked {
    /// The worst is the greatest: the lower score, then the larger key.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.score.total_cmp(&self.score))
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.slot.cmp(&other.slot))
    }
}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}

/// One ranking of one pin. Visits a segment per step.
pub struct Scan {
    pin: Pin,
    prepared: Prepared,
    limit: usize,
    filter: Option<Filter>,
    exact: bool,
    todo: VecDeque<u64>,
    top: BinaryHeap<Ranked>,
    seen: HashSet<Slot>,
    /// Documents that were looked at, and rows whose score was summed, for tests of pruning.
    pub considered: usize,
    pub scored: usize,
}

impl Scan {
    /// The result's worst score once it is full. An exact scan prunes nothing.
    fn worst(&self) -> Option<f64> {
        if self.exact || self.top.len() < self.limit {
            return None;
        }
        self.top.peek().map(|r| f64::from(r.score))
    }

    fn offer(&mut self, r: Ranked) {
        self.top.push(r);
        if self.top.len() > self.limit {
            self.top.pop();
        }
    }
}

/// A list of one segment under one query term.
struct Lane<'a> {
    i: usize,
    cursor: Cursor<'a>,
    /// The most any of its postings adds, at least 0.
    ub: f64,
}

impl<S: PostingStore> ImpactIndex<S> {
    fn prepare(&self, pin: &Pin, query: &Query) -> Result<Prepared> {
        const OP: Error = Error::InvalidQuery("the operation does not fit the column");
        match (&self.analysis, query.op, &query.probe) {
            (Analysis::Text(a), Op::Match, Probe::Text(text)) => {
                let terms = a.terms(text).terms;
                if terms.len() > MAX_QUERY_TERMS {
                    return Err(Error::InvalidQuery("the query has too many terms"));
                }
                let local;
                let stats = match &query.stats {
                    Some(s) if !mutant("local_stats") => s,
                    _ => {
                        let ranges: Vec<RangeId> = pin.ranges().iter().map(|r| r.0).collect();
                        local = self.counts(&ranges, text)?;
                        &local
                    }
                };
                if stats.df.len() != terms.len() {
                    return Err(Error::InvalidQuery("the counts do not fit the query"));
                }
                let n = stats.docs as f64;
                let qterm = |((name, count), df): (&(String, u32), &u64)| {
                    let df = *df as f64;
                    let idf = (1.0 + ((n - df).max(0.0) + 0.5) / (df + 0.5)).ln();
                    QTerm {
                        term: self.dict.get(name),
                        weight: idf * (K1 + 1.0) * f64::from(*count),
                    }
                };
                let avg = match stats.docs {
                    0 => 1.0,
                    docs => (stats.words as f64 / docs as f64).max(f64::MIN_POSITIVE),
                };
                Ok(Prepared {
                    op: Op::Match,
                    terms: terms.iter().zip(&stats.df).map(qterm).collect(),
                    avg,
                    len: 1.0,
                })
            }
            (Analysis::Sparse, Op::Dot | Op::Cosine, Probe::Sparse { indices, weights }) => {
                if !sparse_fits(indices, weights) {
                    return Err(Error::InvalidQuery("the sparse vector is invalid"));
                }
                if indices.len() > MAX_QUERY_TERMS {
                    return Err(Error::InvalidQuery("the query has too many terms"));
                }
                let len = weights
                    .iter()
                    .map(|w| f64::from(*w).powi(2))
                    .sum::<f64>()
                    .sqrt();
                if query.op == Op::Cosine && len == 0.0 {
                    return Err(Error::InvalidQuery("a zero vector has no cosine"));
                }
                let terms = indices.iter().zip(weights).filter(|(_, w)| **w != 0.0);
                let qterm = |(i, w): (&u32, &f32)| QTerm {
                    term: Some(*i),
                    weight: f64::from(*w),
                };
                Ok(Prepared {
                    op: query.op,
                    terms: terms.map(qterm).collect(),
                    avg: 1.0,
                    len,
                })
            }
            _ => Err(OP),
        }
    }

    /// Plans a ranking of a pin.
    pub fn scan(&self, pin: &Pin, query: &Query) -> Result<Scan> {
        Self::check_pin(pin)?;
        if !(1..=MAX_LIMIT).contains(&query.limit) {
            return Err(Error::InvalidQuery("limit is out of bounds"));
        }
        if let Some(f) = &query.filter {
            let schema = Schema {
                dim: 0,
                columns: self.cfg.columns.clone(),
                partition_by: None,
            };
            f.check(&schema)?;
        }
        Ok(Scan {
            prepared: self.prepare(pin, query)?,
            pin: pin.clone(),
            limit: query.limit,
            filter: query.filter.clone(),
            exact: query.exact,
            todo: pin.0.segments.iter().copied().collect(),
            top: BinaryHeap::new(),
            seen: HashSet::new(),
            considered: 0,
            scored: 0,
        })
    }

    fn check_pin(pin: &Pin) -> Result<()> {
        if pin.0.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::PinFailed);
        }
        Ok(())
    }

    /// `Visit`: reads one segment. Returns whether there is more to do.
    pub fn visit(&self, scan: &mut Scan) -> Result<bool> {
        Self::check_pin(&scan.pin)?;
        let Some(id) = scan.todo.pop_front() else {
            return Ok(false);
        };
        let seg = self
            .segments
            .get(&id)
            .ok_or(Error::Internal("a pinned segment is gone"))?;
        self.visit_segment(scan, seg)?;
        Ok(!scan.todo.is_empty())
    }

    /// The row of an entry if the pin shows it and the filter takes it.
    fn shown(
        &self,
        scan: &Scan,
        seg: &Segment,
        doc: Doc,
    ) -> Option<(&Key, &Arc<Stored>, Slot, Pos)> {
        let e = seg.entries.get(doc as usize)?;
        let point = scan.pin.0.points.get(&e.generation)?;
        if !self.rows.visible(e.slot, e.at, *point) {
            return None;
        }
        let row = self.rows.version(e.slot, e.at)?.row.as_ref()?;
        let takes = |f: &Filter| mutant("post_filter") || f.matches(&row.cols);
        if !scan.filter.as_ref().is_none_or(takes) {
            return None;
        }
        Some((self.rows.key(e.slot)?, row, e.slot, e.at))
    }

    fn visit_segment(&self, scan: &mut Scan, seg: &Segment) -> Result<()> {
        let p = &scan.prepared.clone();
        let mut lanes: Vec<Lane> = (p.terms.iter().enumerate())
            .filter_map(|(i, t)| {
                let list = seg.lists.get(&t.term?)?;
                let ub = p.bound(i, &list.all);
                let ub = if mutant("maxscore_unclamped") {
                    ub
                } else {
                    ub.max(0.0)
                };
                Some(Lane {
                    i,
                    cursor: Cursor::new(list),
                    ub,
                })
            })
            .collect();
        lanes.sort_by(|a, b| a.ub.total_cmp(&b.ub).then(a.i.cmp(&b.i)));
        // prefix[e]: the most that the lists before `e` add together, and the mass of that sum.
        let mut prefix = vec![(0.0f64, 0.0f64)];
        for l in &lanes {
            let (sum, mass) = prefix[prefix.len() - 1];
            prefix.push((sum + l.ub, mass + l.ub.abs()));
        }
        let mut parts: Vec<Option<f64>> = vec![None; p.terms.len()];
        let mut rest: Vec<f64> = Vec::with_capacity(lanes.len());
        let store = &self.store;
        let norm_of = |doc: Doc| seg.entries.get(doc as usize).map_or(1.0, |e| e.norm);
        loop {
            let worst = scan.worst();
            // Lists before `ess` cannot lift a row into the result on their own.
            let mut ess = 0;
            while ess < lanes.len() && below(prefix[ess + 1].0, prefix[ess + 1].1, worst) {
                ess += 1;
            }
            if ess == lanes.len() {
                return Ok(());
            }
            let mut doc = None;
            for l in &mut lanes[ess..] {
                if let Some(x) = l.cursor.current(store)? {
                    doc = Some(doc.map_or(x.doc, |d: Doc| d.min(x.doc)));
                }
            }
            let Some(doc) = doc else {
                return Ok(());
            };
            // Whole blocks: no row up to the end of the first essential block can reach the result.
            if worst.is_some() {
                let (mut sum, mut mass) = prefix[ess];
                let mut end = Doc::MAX;
                for l in &lanes[ess..] {
                    if let Some((last, b)) = l.cursor.block() {
                        let bound = p.bound(l.i, b).max(0.0);
                        sum += bound;
                        mass += bound;
                        end = end.min(last);
                    }
                }
                if below(sum, mass, worst) {
                    for l in &mut lanes[ess..] {
                        l.cursor.seek(end.saturating_add(1), store)?;
                    }
                    if end == Doc::MAX {
                        return Ok(());
                    }
                    continue;
                }
            }
            scan.considered += 1;
            let norm = norm_of(doc);
            // The candidate's essential parts are exact, the others bounded by their blocks.
            let (mut sum, mut mass) = (0.0, 0.0);
            for l in &mut lanes[ess..] {
                if let Some(x) = l.cursor.current(store)?
                    && x.doc == doc
                {
                    let part = p.part(l.i, x.value, norm);
                    parts[l.i] = Some(part);
                    let scaled = p.scaled(part, norm);
                    sum += scaled;
                    mass += scaled.abs();
                    l.cursor.advance();
                }
            }
            rest.clear();
            for l in &mut lanes[..ess] {
                rest.push(
                    l.cursor
                        .shallow(doc)
                        .map_or(0.0, |b| p.bound(l.i, b).max(0.0)),
                );
            }
            let rests = rest.iter().sum::<f64>();
            let mut hope = sum + rests;
            // The mass only grows: a block bound that a part replaces stays in it.
            mass += rests;
            let mut out = below(hope, mass, worst);
            for j in (0..ess).rev() {
                if out {
                    break;
                }
                let l = &mut lanes[j];
                hope -= rest[j];
                if let Some(x) = l.cursor.seek(doc, store)?
                    && x.doc == doc
                {
                    let part = p.part(l.i, x.value, norm);
                    parts[l.i] = Some(part);
                    let scaled = p.scaled(part, norm);
                    hope += scaled;
                    mass += scaled.abs();
                }
                out = below(hope, mass, worst);
            }
            if !out {
                self.rank(parts.iter().flatten().sum(), doc, seg, scan)?;
            }
            parts.iter_mut().for_each(|x| *x = None);
        }
    }

    /// Offers a candidate whose parts are all known.
    fn rank(&self, parts: f64, doc: Doc, seg: &Segment, scan: &mut Scan) -> Result<()> {
        let Some((key, row, slot, at)) = self.shown(scan, seg, doc) else {
            return Ok(());
        };
        if scan.seen.contains(&slot) && !mutant("no_slot_dedup") {
            return Ok(());
        }
        scan.seen.insert(slot);
        scan.scored += 1;
        let score = scan.prepared.score(parts, row.norm);
        let ranked = Ranked {
            score,
            key: key.clone(),
            slot,
            at,
        };
        scan.offer(ranked);
        Ok(())
    }

    /// `FinishScan`: the rows, best first and then by key.
    pub fn finish(&self, scan: Scan) -> Result<Vec<Hit>> {
        Self::check_pin(&scan.pin)?;
        let filter = scan.filter.clone();
        let hit = |r: Ranked| {
            let generation = self.rows.generation(r.slot)?;
            let version = self.rows.version(r.slot, r.at)?;
            let row = version.row.as_ref()?;
            Some(Hit {
                range: *self.gens.get(&generation)?,
                key: r.key,
                at: r.at,
                commit_ts: version.ts,
                score: r.score,
                image: row.image.clone(),
            })
            .filter(|_| {
                !mutant("post_filter") || filter.as_ref().is_none_or(|f| f.matches(&row.cols))
            })
        };
        let ranked = scan.top.into_sorted_vec();
        let n = ranked.len();
        let hits: Vec<Hit> = ranked.into_iter().filter_map(hit).collect();
        if hits.len() != n && !mutant("post_filter") {
            return Err(Error::Internal("a pinned row is gone"));
        }
        Ok(hits)
    }

    /// A whole scan.
    pub fn search(&self, pin: &Pin, query: &Query) -> Result<Vec<Hit>> {
        let mut scan = self.scan(pin, query)?;
        while self.visit(&mut scan)? {}
        self.finish(scan)
    }
}
