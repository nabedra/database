//! Scans of one pin: exact over every partition, or nearest units first until a stop rule is content.
//! A row is ranked only if it is visible at the pin and matches the filter.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::Arc;

use super::codec::{Codec, best, direction, dot, exact, goodness, optimistic};
use super::filter::{Bits, Filter, Scalar, evaluate};
use super::mutant::mutant;
use super::rows::Slot;
use super::shard::{Band, Entry, band_span};
use super::tree::{SCAN_BEAM, Walk};
use super::{Index, Meta, Pin, parts_of};
use crate::types::{CommitTs, Error, Key, MAX_LIMIT, Op, PartId, Pos, RangeId, Result, Row};

#[derive(Clone, PartialEq, Debug)]
pub struct Query {
    pub op: Op,
    pub vec: Vec<f32>,
    pub limit: usize,
    pub filter: Option<Filter>,
    /// Scores every visible row that matches.
    pub exact: bool,
    /// The recall that an approximate scan pursues.
    pub recall: f64,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Hit {
    pub range: RangeId,
    pub key: Key,
    pub at: Pos,
    pub commit_ts: CommitTs,
    /// The value of the operation.
    pub score: f32,
    pub row: Arc<Row>,
}

/// How far an approximate scan got.
pub struct Progress<'a> {
    pub limit: usize,
    pub recall: f64,
    /// Units read so far that held a matching row. A unit is a partition, or one length band of it.
    pub units: usize,
    /// For each of the best `limit` rows, the unit that it came from.
    pub origins: &'a [usize],
}

/// Decides when an approximate scan has read enough. It is asked only while `limit` rows are there.
pub trait StopRule: Send {
    fn enough(&mut self, progress: &Progress<'_>) -> bool;
}

/// Estimates recall as if the units that were not read held as many of the best rows as the last
/// units did: `1 - hits(last w units) / limit`, where `w` is the larger of 16 and half of what was
/// read. Units come nearest first, so hits thin out and the estimate is on the safe side for
/// clustered data. It is a heuristic without a guarantee: the recall gates measure it.
#[derive(Clone, Copy, Default, Debug)]
pub struct RecallEstimate;

impl StopRule for RecallEstimate {
    fn enough(&mut self, p: &Progress<'_>) -> bool {
        let window = (p.units / 2).max(16);
        let Some(before) = p.units.checked_sub(window) else {
            return false;
        };
        let late = p.origins.iter().filter(|u| **u >= before).count();
        1.0 - late as f64 / p.limit.max(1) as f64 >= p.recall
    }
}

struct Ranked {
    good: f32,
    key: Key,
    slot: Slot,
    at: Pos,
    unit: usize,
}

impl Ord for Ranked {
    /// The worst is the greatest: less good, then the larger key.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.good.total_cmp(&self.good))
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

/// Units in the order of what a row in them is expected to reach.
struct Stream {
    walk: Walk,
    /// Partitions met so far, and the cosine of their centroid.
    met: Vec<(PartId, f32)>,
    /// Per length band, its span and how far down `met` it was read. One for a cosine, which does
    /// not look at lengths.
    bands: Vec<(Option<i32>, f64, f64, usize)>,
}

enum Plan {
    /// Every one of these, with exact scores.
    All(VecDeque<PartId>),
    Stream(Stream),
}

/// One ranking of one pin. Visits a partition per step.
pub struct Scan {
    pin: Pin,
    op: Op,
    vec: Vec<f32>,
    dir: Vec<f32>,
    len: f64,
    limit: usize,
    filter: Option<Filter>,
    recall: f64,
    stop: Box<dyn StopRule>,
    plan: Plan,
    top: BinaryHeap<Ranked>,
    /// Rows that were scored.
    pub(super) seen: HashSet<Slot>,
    /// Units that held a match.
    units: usize,
    /// Units read so far, or skipped because nothing in them could enter the result.
    pub visited: usize,
}

impl Scan {
    /// The least goodness that still enters the result.
    fn floor(&self) -> Option<f32> {
        (self.top.len() >= self.limit).then(|| self.top.peek().map_or(f32::INFINITY, |r| r.good))
    }

    /// The best goodness of any entry of a partition, or of one band of it: no entry is farther from
    /// the centroid than the radius.
    fn reach(&self, meta: &Meta, band: Option<i32>) -> f64 {
        const SLACK: f64 = 1e-3;
        let angle = |cos: f32| f64::from(cos).clamp(-1.0, 1.0).acos();
        let between = angle(dot(&self.dir, &meta.centroid)) - angle(meta.radius) - SLACK;
        let (lo, hi) = band.map_or((0.0, 0.0), band_span);
        best(self.op, between.max(0.0).cos(), lo, hi, self.len)
    }

    /// Scans may list partitions in any order.
    #[cfg(test)]
    pub fn rotate(&mut self, n: usize) {
        if let Plan::All(todo) = &mut self.plan
            && !todo.is_empty()
        {
            todo.rotate_left(n % todo.len());
        }
    }

    /// Scores a visible row exactly. Returns whether it ranks at all.
    fn offer(&mut self, key: &Key, e: &Entry, vec: &[f32]) -> bool {
        if !self.seen.insert(e.slot) && !mutant("no_slot_dedup") {
            return true;
        }
        let Some(score) = exact(self.op, vec, &self.vec) else {
            return false;
        };
        let good = goodness(self.op, score);
        self.top.push(Ranked {
            good,
            key: key.clone(),
            slot: e.slot,
            at: e.at,
            unit: self.units,
        });
        if self.top.len() > self.limit {
            self.top.pop();
        }
        true
    }
}

impl Stream {
    /// The next unit, and the band that it is limited to.
    fn next(&mut self, op: Op, len: f64) -> Option<(PartId, Option<i32>)> {
        let need = self.bands.iter().map(|b| b.3 + 1).max()?;
        while self.met.len() < need
            && let Some((leaf, cos)) = self.walk.next()
        {
            self.met.push((leaf.part, cos));
        }
        let met = &self.met;
        let promise = |b: &(Option<i32>, f64, f64, usize)| {
            met.get(b.3)
                .map(|(_, cos)| best(op, f64::from(*cos), b.1, b.2, len))
        };
        let (_, i) = self
            .bands
            .iter()
            .enumerate()
            .filter_map(|(i, b)| Some((promise(b)?, i)))
            .max_by(|a, b| a.0.total_cmp(&b.0).then(b.1.cmp(&a.1)))?;
        let band = &mut self.bands[i];
        band.3 += 1;
        Some((met[band.3 - 1].0, band.0))
    }
}

impl<C: Codec> Index<C> {
    /// Plans a ranking of a pin. Every ranking of a query uses the same pin.
    pub fn scan(&self, pin: &Pin, query: &Query, stop: Box<dyn StopRule>) -> Result<Scan> {
        let schema = &self.cfg.schema;
        if query.vec.len() != schema.dim || query.vec.iter().any(|x| !x.is_finite()) {
            return Err(Error::InvalidQuery("the vector does not fit the column"));
        }
        if !(1..=MAX_LIMIT).contains(&query.limit) || !(0.0..=1.0).contains(&query.recall) {
            return Err(Error::InvalidQuery("limit or recall is out of bounds"));
        }
        if let Some(f) = &query.filter {
            f.check(schema)?;
        }
        let direction = direction(&query.vec);
        if query.op == Op::Cosine && query.vec.iter().all(|x| *x == 0.0) {
            return Err(Error::InvalidQuery("a zero vector has no cosine"));
        }
        let only = |c| {
            query
                .filter
                .as_ref()
                .and_then(|f| f.only(c))
                .and_then(Scalar::of)
        };
        if pin.0.group.is_some() && pin.0.group != schema.partition_by.and_then(only) {
            return Err(Error::InvalidQuery("the pin holds another tree"));
        }
        let (dir, len) = direction.unwrap_or_else(|| (query.vec.clone(), 0.0));
        Ok(Scan {
            plan: self.plan(pin, query, &dir, len),
            pin: pin.clone(),
            op: query.op,
            vec: query.vec.clone(),
            dir,
            len,
            limit: query.limit,
            filter: query.filter.clone(),
            recall: query.recall,
            stop,
            top: BinaryHeap::new(),
            seen: HashSet::new(),
            units: 0,
            visited: 0,
        })
    }

    fn plan(&self, pin: &Pin, query: &Query, dir: &[f32], len: f64) -> Plan {
        let summary = &self.shared.summary;
        let entries: usize = pin.0.roots.iter().map(|r| r.entries).sum();
        let few = query
            .filter
            .as_ref()
            .is_some_and(|f| summary.estimate(f) <= self.cfg.flat as f64);
        if query.exact || entries < self.cfg.flat || few || len == 0.0 {
            return Plan::All(parts_of(pin.0.roots.iter()).into_iter().collect());
        }
        let mut walk = Walk::new(dir.to_vec(), SCAN_BEAM);
        let mut exps: Vec<i32> = Vec::new();
        for root in &pin.0.roots {
            walk.add(&root.tree);
            exps.extend(root.bands.keys().filter(|e| **e != i32::MIN));
        }
        exps.sort_unstable();
        exps.dedup();
        let bands = match query.op {
            Op::Cosine => vec![(None, 0.0, 0.0, 0)],
            _ => exps
                .into_iter()
                .map(|e| {
                    let (lo, hi) = band_span(e);
                    (Some(e), lo, hi, 0)
                })
                .collect(),
        };
        Plan::Stream(Stream {
            walk,
            met: Vec::new(),
            bands,
        })
    }

    /// `Visit`: reads one partition. Returns whether there is more to do.
    pub fn visit(&self, scan: &mut Scan) -> Result<bool> {
        if scan.pin.0.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::PinFailed);
        }
        if let Plan::All(todo) = &mut scan.plan {
            return match todo.pop_front() {
                Some(part) => self.visit_part(scan, part, None, false).map(|_| true),
                None => Ok(false),
            };
        }
        // Vectors without a direction have no place in a tree.
        if scan.visited == 0 {
            let lists: Vec<PartId> = scan
                .pin
                .0
                .roots
                .iter()
                .filter_map(|r| r.undirected)
                .collect();
            for part in lists {
                self.visit_part(scan, part, None, false)?;
            }
        }
        scan.visited += 1;
        let Plan::Stream(stream) = &mut scan.plan else {
            return Ok(false);
        };
        let Some((part, band)) = stream.next(scan.op, scan.len) else {
            return Ok(false);
        };
        if self.visit_part(scan, part, band, true)? {
            scan.units += 1;
            if scan.top.len() >= scan.limit {
                let origins: Vec<usize> = scan.top.iter().map(|r| r.unit).collect();
                let (limit, recall, units) = (scan.limit, scan.recall, scan.units);
                let progress = Progress {
                    limit,
                    recall,
                    units,
                    origins: &origins,
                };
                return Ok(!scan.stop.enough(&progress));
            }
        }
        Ok(true)
    }

    /// Offers the visible rows of a partition that match. Returns whether there were any.
    fn visit_part(
        &self,
        scan: &mut Scan,
        part: PartId,
        band: Option<i32>,
        codes: bool,
    ) -> Result<bool> {
        let sh = &self.shared;
        if let Some(f) = &scan.filter
            && !sh.summary.may_match(f, part)
        {
            return Ok(false);
        }
        let Some(meta) = sh.dir.get(&part) else {
            // The mutant that frees partitions early is caught by what the scan then misses.
            let gone = Error::Internal("a pinned partition is gone");
            return if mutant("free_without_wait") {
                Ok(false)
            } else {
                Err(gone)
            };
        };
        // The unit may have matches, but none that could enter the result.
        if codes
            && scan
                .floor()
                .is_some_and(|f| scan.reach(meta, band) < f64::from(f))
        {
            return Ok(true);
        }
        let Some(p) = self.shards[meta.owner].parts.get(&part) else {
            return Ok(false);
        };
        let mut any = false;
        let query = match (&p.centroid, codes) {
            (Some(centroid), true) => {
                Some(self.codec.query(&self.codec.rotate(&scan.dir), centroid))
            }
            _ => None,
        };
        for b in p
            .bands
            .iter()
            .filter(|b| band.is_none_or(|exp| b.exp == exp))
        {
            let bits = scan
                .filter
                .as_ref()
                .map(|f| evaluate(f, &b.cols, b.entries.len()));
            match &query {
                None => any |= self.offer_all(scan, b, bits)?,
                Some(q) => any |= self.offer_best(scan, b, bits, q)?,
            }
        }
        Ok(any)
    }

    /// The entries of a band that match the filter and are visible at the pin.
    fn shown<'a, B>(
        &self,
        pin: &Pin,
        b: &'a Band<B>,
        bits: Option<Bits>,
    ) -> Vec<(usize, &'a Entry)> {
        let shows = |e: &Entry| {
            let point = pin.0.points.get(&e.generation);
            let applied = self.shared.range_of(e.generation).map_or(0, |r| r.app);
            point.is_some_and(|p| self.shared.rows.shows(e.slot, e.at, *p, applied))
        };
        let matching = |i: &usize| bits.as_ref().is_none_or(|bits| bits.get(*i));
        let all = b.entries.iter().enumerate();
        all.filter(|(i, e)| matching(i) && shows(e)).collect()
    }

    fn live(&self, e: &Entry) -> Result<(&Key, Arc<[f32]>)> {
        const GONE: Error = Error::Internal("an entry without its row");
        let key = self.shared.rows.key(e.slot).ok_or(GONE)?;
        let row = self
            .shared
            .rows
            .version(e.slot, e.at)
            .and_then(|v| v.row.as_ref());
        Ok((key, row.and_then(|r| r.vec.clone()).ok_or(GONE)?))
    }

    fn offer_all<B>(&self, scan: &mut Scan, b: &Band<B>, bits: Option<Bits>) -> Result<bool> {
        let mut any = false;
        for (_, e) in self.shown(&scan.pin, b, bits) {
            let (key, vec) = self.live(e)?;
            any |= scan.offer(key, e, &vec);
        }
        Ok(any)
    }

    /// Coarse estimates first. A row whose best case is below the result's worst is not looked at
    /// again, and the others go through the finer estimate to the raw vector.
    fn offer_best(
        &self,
        scan: &mut Scan,
        b: &Band<C::Block>,
        bits: Option<Bits>,
        q: &C::Query,
    ) -> Result<bool> {
        let shown = self.shown(&scan.pin, b, bits);
        let mut coarse = Vec::with_capacity(b.entries.len());
        self.codec.scan(q, &b.codes, &mut coarse);
        let (op, len) = (scan.op, scan.len);
        let hope = |est, i: usize| optimistic(op, est, b.lens[i], len);
        let mut order: Vec<_> = shown
            .iter()
            .map(|&(i, e)| (hope(coarse[i], i), i, e))
            .collect();
        order.sort_by(|x, y| y.0.total_cmp(&x.0).then(x.1.cmp(&y.1)));
        for &(good, i, e) in &order {
            let floor = scan.floor();
            if floor.is_some_and(|f| good < f) {
                break;
            }
            if floor.is_some_and(|f| hope(self.codec.refine(q, &b.codes, i), i) < f) {
                continue;
            }
            let (key, vec) = self.live(e)?;
            scan.offer(key, e, &vec);
        }
        Ok(!order.is_empty())
    }

    /// `FinishScan`: the rows, best first and then by key.
    pub fn finish(&self, scan: Scan) -> Result<Vec<Hit>> {
        if scan.pin.0.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::PinFailed);
        }
        let hit = |r: Ranked| {
            let generation = self.shared.rows.generation(r.slot)?;
            let version = self.shared.rows.version(r.slot, r.at)?;
            Some(Hit {
                range: *self.shared.gens.get(&generation)?,
                key: r.key,
                at: r.at,
                commit_ts: version.ts,
                score: goodness(scan.op, r.good),
                row: version.row.clone()?,
            })
        };
        let hits: Option<Vec<Hit>> = scan.top.into_sorted_vec().into_iter().map(hit).collect();
        hits.ok_or(Error::Internal("a pinned row is gone"))
    }

    /// A whole scan.
    pub fn search(&self, pin: &Pin, query: &Query) -> Result<Vec<Hit>> {
        let mut scan = self.scan(pin, query, Box::new(RecallEstimate))?;
        while self.visit(&mut scan)? {}
        self.finish(scan)
    }
}
