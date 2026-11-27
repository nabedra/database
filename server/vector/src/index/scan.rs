//! Scans of one pin: exact over every partition, or nearest units first until a stop rule is content.
//! A row is ranked only if it is visible at the pin and matches the filter.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::Arc;

use super::codec::{Codec, best, direction, dot, exact, goodness, optimistic};
use super::filter::{Bits, Filter, Scalar, evaluate};
use super::mutant::mutant;
use super::rows::Slot;
use super::shard::{Band, Entry, band_span, ext_key};
use super::tree::{SCAN_BEAM, Walk};
use super::{Index, Meta, Pin, parts_of};
use crate::runtime::{Budget, Meter, Tier, Usage};
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
    vector: Arc<[f32]>,
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
    /// The budget it runs within, if any.
    meter: Option<Meter>,
}

/// Memory a scored row may add: its place in the result and in the set of rows seen.
const ROW_BYTES: u64 = (std::mem::size_of::<Ranked>() + 2 * std::mem::size_of::<Slot>()) as u64;

impl Scan {
    /// What it spent so far, when it runs within a budget.
    pub fn usage(&self) -> Option<Usage> {
        self.meter.as_ref().map(Meter::used)
    }

    /// Charges work before it happens. Running out ends the scan.
    fn charge(&mut self, cpu: u64, io: u64, memory: u64) -> Result<()> {
        let Some(m) = &mut self.meter else {
            return Ok(());
        };
        m.cpu(cpu)?;
        m.io(io)?;
        m.grow(memory)
    }

    fn release(&mut self, memory: u64) {
        if let Some(m) = &mut self.meter {
            m.shrink(memory);
        }
    }

    /// An exact score of `dim` units, and the memory a new row may hold.
    fn charge_score(&mut self) -> Result<()> {
        let fresh = if self.top.len() < self.limit { ROW_BYTES } else { 8 };
        self.charge(self.vec.len() as u64, 0, fresh)
    }

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
    fn offer(&mut self, key: &Key, e: &Entry, vec: Arc<[f32]>) -> bool {
        if !self.seen.insert(e.slot) && !mutant("no_slot_dedup") {
            return true;
        }
        let Some(score) = exact(self.op, &vec, &self.vec) else {
            return false;
        };
        let good = goodness(self.op, score);
        self.top.push(Ranked {
            good,
            key: key.clone(),
            slot: e.slot,
            at: e.at,
            unit: self.units,
            vector: vec,
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
            meter: None,
        })
    }

    /// [`Index::scan`] within a budget. Running out ends it with `OverBudget`, and nothing of it
    /// is returned.
    pub fn scan_within(
        &self,
        pin: &Pin,
        query: &Query,
        stop: Box<dyn StopRule>,
        budget: Budget,
    ) -> Result<Scan> {
        let mut scan = self.scan(pin, query, stop)?;
        scan.meter = Some(Meter::new(budget));
        Ok(scan)
    }

    /// [`Index::search`] within a budget.
    pub fn search_within(&self, pin: &Pin, query: &Query, budget: Budget) -> Result<Vec<Hit>> {
        let mut scan = self.scan_within(pin, query, Box::new(RecallEstimate), budget)?;
        while self.visit(&mut scan)? {}
        self.finish(scan)
    }

    /// The bytes that reading one raw vector costs.
    fn read_cost(&self) -> u64 {
        if self.shared.rows.on_disk() {
            (self.cfg.schema.dim * 4) as u64
        } else {
            0
        }
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
        let (query, temporary) = match (&p.centroid, codes) {
            (Some(centroid), true) => {
                let (work, bytes) = self.codec.query_cost(scan.dir.len());
                scan.charge(work, 0, bytes)?;
                let query = self.codec.query(&self.codec.rotate(&scan.dir), centroid);
                (Some(query), bytes)
            }
            _ => (None, 0),
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
                Some(q) => any |= self.offer_best(scan, part, b, bits, q)?,
            }
        }
        drop(query);
        scan.release(temporary);
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
        Ok((key, self.shared.rows.vector(e.slot, e.at)?.ok_or(GONE)?))
    }

    fn offer_all<B>(&self, scan: &mut Scan, b: &Band<B>, bits: Option<Bits>) -> Result<bool> {
        let mut any = false;
        for (_, e) in self.shown(&scan.pin, b, bits) {
            any |= self.score_entry(scan, e)?;
        }
        Ok(any)
    }

    fn score_entry(&self, scan: &mut Scan, e: &Entry) -> Result<bool> {
        // Keep the winning vectors for the response. A replacement occupies the same
        // space, but the incoming vector needs room while we score it.
        let bytes = (self.cfg.schema.dim * size_of::<f32>()) as u64;
        scan.charge(0, self.read_cost(), bytes)?;
        scan.charge_score()?;
        let (key, vec) = self.live(e)?;
        let before = scan.top.len();
        let offered = scan.offer(key, e, vec);
        if scan.top.len() == before {
            scan.release(bytes);
        }
        Ok(offered)
    }

    /// Coarse estimates first. A row whose best case is below the result's worst is not looked at
    /// again, and the others go through the finer estimate to the raw vector.
    fn offer_best(
        &self,
        scan: &mut Scan,
        part: PartId,
        b: &Band<C::Block>,
        bits: Option<Bits>,
        q: &C::Query,
    ) -> Result<bool> {
        let shown = self.shown(&scan.pin, b, bits);
        let dim = scan.vec.len() as u64;
        let n = b.entries.len() as u64;
        let held = n * (std::mem::size_of::<super::codec::Estimate>() + 24) as u64;
        scan.charge(n * (dim / 8 + 1), 0, held)?;
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
            if let Some(f) = floor
                && hope(self.refine(scan, part, b, q, i, e)?, i) < f
            {
                continue;
            }
            self.score_entry(scan, e)?;
        }
        scan.release(held);
        Ok(!order.is_empty())
    }

    /// The finer estimate of entry `i`, through its extension where the store keeps it: in RAM, or
    /// read from NVMe, which counts as I/O. A store that cannot read it fails the scan and the range.
    fn refine(
        &self,
        scan: &mut Scan,
        part: PartId,
        b: &Band<C::Block>,
        q: &C::Query,
        i: usize,
        e: &Entry,
    ) -> Result<super::codec::Estimate> {
        scan.charge(scan.vec.len() as u64 / 2 + 1, 0, 0)?;
        let Some(store) = self.shared.codes.as_ref().filter(|_| self.codec.ext_bytes() > 0) else {
            return Ok(self.codec.refine(q, &b.codes, i));
        };
        let key = ext_key(part, e.id);
        if store.tier(&key) != Some(Tier::Ram) {
            scan.charge(0, store.bytes() as u64, 0)?;
        }
        match store.get(&key) {
            Ok(ext) => Ok(self.codec.refine_with(q, &b.codes, i, &ext)),
            Err(error) => {
                self.shared.rows.log_damage(e.generation);
                Err(error)
            }
        }
    }

    /// `FinishScan`: the rows, best first and then by key.
    pub fn finish(&self, scan: Scan) -> Result<Vec<Hit>> {
        if scan.pin.0.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::PinFailed);
        }
        if let Some(what) = scan.meter.as_ref().and_then(Meter::over) {
            return Err(Error::OverBudget(what));
        }
        let rows = &self.shared.rows;
        let hit = |r: Ranked| -> Result<Option<Hit>> {
            let Some(row) = rows.full_row(r.slot, r.at, r.vector) else {
                return Ok(None);
            };
            let hit = || {
                let generation = rows.generation(r.slot)?;
                let version = rows.version(r.slot, r.at)?;
                Some(Hit {
                    range: *self.shared.gens.get(&generation)?,
                    key: r.key,
                    at: r.at,
                    commit_ts: version.ts,
                    score: goodness(scan.op, r.good),
                    row,
                })
            };
            Ok(hit())
        };
        let hits: Result<Option<Vec<Hit>>> = scan
            .top
            .into_sorted_vec()
            .into_iter()
            .map(hit)
            .collect::<Result<Vec<_>>>()
            .map(|h| h.into_iter().collect());
        hits?.ok_or(Error::Internal("a pinned row is gone"))
    }

    /// A whole scan.
    pub fn search(&self, pin: &Pin, query: &Query) -> Result<Vec<Hit>> {
        let mut scan = self.scan(pin, query, Box::new(RecallEstimate))?;
        while self.visit(&mut scan)? {}
        self.finish(scan)
    }
}
