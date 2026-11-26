//! The index of one search worker for one vector column of one table: partitions, scans and
//! checkpoints. The contract is `specs/vector/index/LocalIndex.tla`, and functions are named after its
//! actions.
//!
//! A partition belongs to one [`Shard`], which is one core. A step on a partition is a function of its
//! shard. Copies and appends reach a shard through its bounded queue, and [`Shared`] holds what cores
//! agree on: positions, the row map, partition states, trees and pins. Today [`Index`] calls the
//! shards in turn, which makes a run a function of its inputs. Pinned threads replace that loop
//! later, with `Shared` behind its lock, and no step waits for another one: a full queue refuses a
//! copy, and appends wait in the inbox.
//!
//! One thing differs from the spec in form: a range has its own generation, and dropping one takes
//! its entries out of every partition in one step, so with pinned threads a drop stops the cores
//! that hold the range.

mod checkpoint;
mod codec;
mod filter;
mod mutant;
mod rows;
mod scan;
mod shard;
mod tree;

#[cfg(test)]
mod tests;

pub use checkpoint::{Damage, Store, copy_checkpoint};
pub use codec::{Codec, Estimate, ExactCodec, exact};
pub use filter::Filter;
pub use scan::{Hit, Progress, Query, RecallEstimate, Scan, StopRule};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::store::Vectors;
use crate::types::{
    Change, ColumnType, CommitTs, Error, Key, MAX_DIM, Page, PartId, Pos, RangeId, Result, Row,
    Schema,
};
use checkpoint::Running;
use filter::{Scalar, Summary, finite};
use mutant::mutant;
use rows::{Gen, Point, Rows, Slot};
use shard::{Entry, Shard};
use tree::{Leaf, Tree, bisect};

#[derive(Clone, Debug)]
pub struct Config {
    pub schema: Schema,
    /// Cores.
    pub shards: usize,
    /// Messages that a shard's queue holds.
    pub queue: usize,
    /// Positions beyond the applied one, and rows in them. One position is always taken.
    pub max_flight: usize,
    pub max_flight_rows: usize,
    /// Fewer entries, or a filter that fewer match, are scanned in full.
    pub flat: usize,
    pub part_min: usize,
    pub part_max: usize,
    /// Copies and reassignments per partition and maintenance call.
    pub copies: usize,
    pub reassigns: usize,
    /// (value, partition) pairs that the summary of one filter column may use.
    pub summary_pairs: usize,
    /// Where raw vectors live. `None` keeps them in the rows, in RAM.
    pub vectors: Option<Arc<dyn Vectors>>,
}

impl Config {
    pub fn new(schema: Schema, shards: usize) -> Config {
        Config {
            schema,
            shards,
            queue: 1024,
            max_flight: 1024,
            max_flight_rows: 65_536,
            flat: 20_000,
            part_min: 128,
            part_max: 512,
            copies: 64,
            reassigns: 8,
            summary_pairs: 1 << 22,
            vectors: None,
        }
    }

    fn check(&self) -> Result<()> {
        let schema = &self.schema;
        let by = schema.partition_by.map(|c| schema.columns.get(c));
        let sizes = self.shards > 0
            && self.queue > 0
            && self.part_max > 1
            && self.part_min < self.part_max / 2;
        let dims = (1..=MAX_DIM).contains(&schema.dim);
        if !sizes || !dims || matches!(by, Some(None | Some(ColumnType::Tags))) {
            return Err(Error::Internal("invalid index configuration"));
        }
        Ok(())
    }
}

/// Partitions that a split looks at for entries that became nearer to a new one.
const NEIGHBOURS: usize = 8;

/// The value of the `partition_by` column, which names a tree.
type Group = Option<Scalar>;

fn group_of(partition_by: Option<usize>, row: &Row) -> Group {
    Scalar::of(row.cols.get(partition_by?)?)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Ready,
    Draining,
    Retired,
}

/// What the cores know about a partition. One without an entry here is "free".
#[derive(Clone)]
struct Meta {
    state: State,
    group: Group,
    owner: usize,
    /// The list of vectors without a direction. It never drains.
    undirected: bool,
    centroid: Arc<[f32]>,
    entries: usize,
    /// The smallest cosine of an entry to the centroid.
    radius: f32,
    /// Where it drains to, and how many partitions drain into it.
    targets: Vec<PartId>,
    drained_into: usize,
    /// Copies in flight from it and to it.
    leaving: usize,
    arriving: usize,
    /// Moved entries whose copy landed, and the pins that began before that.
    landed: Vec<(u64, u64)>,
    /// The pins that a retired partition waits for.
    waits: u64,
}

#[derive(Clone, Default)]
struct Root {
    tree: Tree,
    undirected: Option<PartId>,
    entries: usize,
    /// Entries per length band.
    bands: BTreeMap<i32, usize>,
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
    /// Every core holds the entries up to here.
    app: Pos,
    /// The oldest position a scan may ask for.
    low: Pos,
    /// The feed was taken up to here.
    delivered: Pos,
    /// Entries beyond `app` that a core still has to append.
    outstanding: BTreeMap<Pos, usize>,
    /// The oldest servable commit timestamp.
    bound: CommitTs,
    /// The newest closed timestamp at or below `app`.
    closed: CommitTs,
    closes: VecDeque<(Pos, CommitTs)>,
    /// Versions above `low` that superseded another one.
    touches: VecDeque<(Pos, CommitTs, Slot)>,
    keys: BTreeMap<Key, Slot>,
}

/// An entry that waits for its core.
#[derive(Clone, Copy)]
struct Append {
    slot: Slot,
    at: Pos,
    generation: Gen,
}

/// An entry on its way to another partition. `source` is the instance it copies.
#[derive(Clone, Copy)]
struct Copy {
    entry: Entry,
    from: PartId,
    source: u64,
    to: PartId,
    moved: bool,
}

#[derive(Clone, Copy)]
enum Msg {
    Append(Append, PartId),
    Copy(Copy),
}

/// A read point per range and the list of partitions, fixed in one step. Dropping it ends it.
#[derive(Clone)]
pub struct Pin(Arc<PinInner>);

struct PinInner {
    id: u64,
    points: BTreeMap<Gen, Point>,
    ranges: Vec<(RangeId, Pos)>,
    roots: Vec<Root>,
    /// The only tree that it lists.
    group: Group,
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
        self.live.range(..epoch).all(|(_, r)| r.strong_count() == 0)
    }

    fn live(&self) -> impl Iterator<Item = Arc<PinInner>> + '_ {
        self.live.values().filter_map(Weak::upgrade)
    }

    fn begin(&mut self, f: impl FnOnce(u64) -> PinInner, holds: bool) -> Arc<PinInner> {
        self.live.retain(|_, r| r.strong_count() > 0);
        let id = self.next;
        self.next += 1;
        let pin = Arc::new(f(id));
        if holds {
            self.live.insert(id, Arc::downgrade(&pin));
        }
        pin
    }

    /// Fails every reader that names a generation.
    fn fail(&mut self, generation: Gen) {
        if mutant("prefix_scan_survives_resync") {
            return;
        }
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

#[derive(Clone)]
struct Shared {
    ranges: BTreeMap<RangeId, Range>,
    gens: BTreeMap<Gen, RangeId>,
    rows: Rows,
    dir: BTreeMap<PartId, Meta>,
    roots: BTreeMap<Group, Root>,
    summary: Summary,
    readers: Readers,
    /// Appends that no queue took yet. Bounded by the rows in flight.
    inbox: VecDeque<Append>,
    queues: Vec<VecDeque<Msg>>,
    queue_len: usize,
    /// Partitions whose neighbours changed.
    recheck: BTreeSet<PartId>,
    next_id: u64,
    next_part: u32,
    next_gen: Gen,
    shards: usize,
    partition_by: Option<usize>,
}

impl Shared {
    fn new_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn range_of(&self, generation: Gen) -> Option<&Range> {
        self.ranges.get(self.gens.get(&generation)?)
    }

    /// Some cut that is or may still be asked for sees this version, or it is not applied yet.
    fn wanted(&self, generation: Gen, slot: Slot, at: Pos) -> bool {
        let Some(r) = self.range_of(generation) else {
            return false;
        };
        if at > r.app {
            return !mutant("prefix_drain_ignores_unapplied");
        }
        if mutant("prefix_drain_copies_current_only") {
            let now = Point {
                pos: r.app,
                cut: CommitTs::MAX,
            };
            return self.rows.visible(slot, at, now);
        }
        self.rows.successor(slot, at).is_none_or(|s| s.at > r.low)
    }

    /// Neither a retained cut nor a pin can see this version again.
    fn collectable(&self, generation: Gen, slot: Slot, at: Pos) -> bool {
        let (Some(r), Some(s)) = (self.range_of(generation), self.rows.successor(slot, at)) else {
            return false;
        };
        let sees = |p: &Point| s.at <= p.pos && s.ts <= p.cut;
        let past = |r: Arc<PinInner>| r.points.get(&generation).is_none_or(sees);
        if mutant("gc_without_epoch") {
            return s.at <= r.app;
        }
        s.at <= r.low && self.readers.live().all(past)
    }

    /// An entry, a copy or an append no longer names a version.
    fn forget(&mut self, generation: Gen, slot: Slot, at: Pos) {
        let range = self
            .gens
            .get(&generation)
            .and_then(|r| self.ranges.get_mut(r));
        match range {
            Some(r) => self.rows.forget(Some(&mut r.keys), slot, at, r.low),
            None => self.rows.forget(None, slot, at, 0),
        }
    }

    /// A core finished the entry of a position.
    fn placed(&mut self, generation: Gen, at: Pos) {
        let range = self
            .gens
            .get(&generation)
            .and_then(|r| self.ranges.get_mut(r));
        if let Some(r) = range
            && let Some(n) = r.outstanding.get_mut(&at)
        {
            *n = n.saturating_sub(1);
            if *n == 0 {
                r.outstanding.remove(&at);
            }
        }
    }

    fn new_part(&mut self, group: Group, centroid: Arc<[f32]>, undirected: bool) -> Option<PartId> {
        let part = PartId(self.next_part);
        self.next_part = self.next_part.checked_add(1)?;
        let meta = Meta {
            state: State::Ready,
            group: group.clone(),
            owner: part.0 as usize % self.shards,
            undirected,
            centroid: centroid.clone(),
            entries: 0,
            radius: 1.0,
            targets: Vec::new(),
            drained_into: 0,
            leaving: 0,
            arriving: 0,
            landed: Vec::new(),
            waits: 0,
        };
        self.dir.insert(part, meta);
        let root = self.roots.entry(group).or_default();
        if undirected {
            root.undirected = Some(part);
        } else {
            root.tree.insert(Leaf {
                part,
                centroid,
                ready: true,
            });
        }
        Some(part)
    }

    /// The partition that takes a new entry.
    fn assign(&mut self, a: &Append) -> Option<PartId> {
        let row = self.rows.version(a.slot, a.at)?.row.clone()?;
        let group = group_of(self.partition_by, &row);
        // A vector the store cannot read is logged, and its range fails after this step.
        let direction = codec::direction(&self.rows.vector(a.slot, a.at).ok()??);
        let root = self.roots.get(&group);
        let part = match &direction {
            None => root.and_then(|r| r.undirected),
            Some((dir, _)) => root.and_then(|r| r.tree.nearest(dir, None)).map(|(p, _)| p),
        };
        match (part, direction) {
            (Some(part), _) => Some(part),
            (None, Some((dir, _))) => self.new_part(group, dir.into(), false),
            (None, None) => self.new_part(group, Arc::new([]), true),
        }
    }

    /// A drained partition leaves the tree and waits for the pins that listed it.
    fn retire(&mut self, part: PartId) {
        let waits = self.readers.epoch();
        let Some(meta) = self.dir.get_mut(&part) else {
            return;
        };
        meta.state = State::Retired;
        meta.waits = waits;
        let targets = std::mem::take(&mut meta.targets);
        let root = self.roots.entry(meta.group.clone()).or_default();
        root.tree.remove(part);
        for t in &targets {
            let Some(meta) = self.dir.get_mut(t) else {
                continue;
            };
            meta.drained_into = meta.drained_into.saturating_sub(1);
            if targets.len() > 1 {
                // Its neighbours may hold entries that are nearer to it now.
                let near = root.tree.near(&meta.centroid, None, NEIGHBOURS);
                self.recheck.extend(near.into_iter().map(|(p, _)| p));
            }
        }
    }
}

/// Where a scan reads a range.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadAt {
    /// The applied position, which reached the caller's barrier.
    Latest { barrier: Pos },
    /// A retained position.
    Position(Pos),
    /// A closed commit timestamp.
    Cut(CommitTs),
}

/// The positions of a range that is held.
#[derive(Clone, PartialEq, Debug)]
pub struct RangeStatus {
    pub serving: bool,
    pub applied: Pos,
    pub low: Pos,
    /// Where the feed continues.
    pub delivered: Pos,
    /// The oldest and the newest timestamp a `snapshot` query may ask for.
    pub bound: CommitTs,
    pub closed: CommitTs,
    /// Why it is out of service.
    pub damage: Option<String>,
}

#[cfg_attr(test, derive(Clone))]
pub struct Index<C: Codec = ExactCodec> {
    cfg: Config,
    codec: C,
    shared: Shared,
    shards: Vec<Shard<C>>,
    store: Option<Store>,
    checkpoint: Option<Running>,
    /// Where `maintain` goes on.
    cursor: PartId,
}

impl<C: Codec> Index<C> {
    /// An empty index. `store` keeps its checkpoints.
    pub fn new(cfg: Config, codec: C, store: Option<Store>) -> Result<Self> {
        cfg.check()?;
        let shared = Shared {
            ranges: BTreeMap::new(),
            gens: BTreeMap::new(),
            rows: Rows::new(cfg.vectors.clone()),
            dir: BTreeMap::new(),
            roots: BTreeMap::new(),
            summary: Summary::new(cfg.schema.columns.len(), cfg.summary_pairs),
            readers: Readers::default(),
            inbox: VecDeque::new(),
            queues: vec![VecDeque::new(); cfg.shards],
            queue_len: cfg.queue,
            recheck: BTreeSet::new(),
            next_id: 0,
            next_part: 0,
            next_gen: 0,
            shards: cfg.shards,
            partition_by: cfg.schema.partition_by,
        };
        Ok(Index {
            shards: vec![Shard::new(); cfg.shards],
            cfg,
            codec,
            shared,
            store,
            checkpoint: None,
            cursor: PartId(0),
        })
    }

    // ----- Ranges: install, feed, retention, drop.

    fn range(&self, id: RangeId) -> Result<&Range> {
        let r = self.shared.ranges.get(&id).ok_or(Error::NotHeld(id))?;
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
        self.shared.ranges.get_mut(&id).ok_or(Error::NotHeld(id))
    }

    pub fn status(&self, id: RangeId) -> Result<RangeStatus> {
        let r = self.shared.ranges.get(&id).ok_or(Error::NotHeld(id))?;
        let damage = match &r.service {
            Service::Out(reason) => Some(reason.clone()),
            _ => None,
        };
        Ok(RangeStatus {
            serving: r.service == Service::Serving,
            applied: r.app,
            low: r.low,
            delivered: r.delivered,
            bound: r.bound,
            closed: r.closed,
            damage,
        })
    }

    /// Replaces what is held of a range by a generation that a range snapshot at position `at` fills:
    /// `begin_load`, `load_rows`, `finish_load`. It is the first half of `Resync`, or an empty range at
    /// position 0. `bound` is the largest commit timestamp in the snapshot, deletes included.
    pub fn begin_load(&mut self, id: RangeId, at: Pos, bound: CommitTs) -> Result<()> {
        self.drop_range(id)?;
        let generation = self.new_generation(id);
        let range = Range {
            generation,
            service: Service::Loading(at),
            app: 0,
            low: 0,
            delivered: at,
            outstanding: BTreeMap::new(),
            bound,
            closed: 0,
            closes: VecDeque::new(),
            touches: VecDeque::new(),
            keys: BTreeMap::new(),
        };
        self.shared.ranges.insert(id, range);
        Ok(())
    }

    fn new_generation(&mut self, id: RangeId) -> Gen {
        self.shared.next_gen += 1;
        self.shared.gens.insert(self.shared.next_gen, id);
        self.shared.next_gen
    }

    /// Adds rows of the range snapshot. `Busy` takes none of them.
    pub fn load_rows(&mut self, id: RangeId, rows: &[(Key, CommitTs, Row)]) -> Result<()> {
        let r = self.range(id)?;
        let Service::Loading(at) = r.service else {
            return Err(Error::InvalidFeed("no snapshot is being installed"));
        };
        let flight = r.outstanding.get(&at).copied().unwrap_or(0);
        if flight > 0 && flight + rows.len() > self.cfg.max_flight_rows {
            return Err(Error::Busy);
        }
        for (key, ts, row) in rows {
            let known = self.range(id)?.keys.contains_key(key);
            let put = match known {
                true => Err(Error::InvalidFeed("invalid snapshot")),
                false => self.put_version(id, key, at, *ts, Some(row)),
            };
            if let Err(error) = put {
                if matches!(error, Error::Store(_)) {
                    self.fail_range(id, "the vector store failed");
                    return Err(error);
                }
                self.fail_range(id, "the snapshot repeats a key or holds an invalid row");
                return Err(Error::InvalidFeed("invalid snapshot"));
            }
        }
        Ok(())
    }

    /// The snapshot is whole. `Busy` while cores still append it.
    pub fn finish_load(&mut self, id: RangeId) -> Result<()> {
        let r = self.range_mut(id)?;
        let Service::Loading(at) = r.service else {
            return Err(Error::InvalidFeed("no snapshot is being installed"));
        };
        if !r.outstanding.is_empty() {
            return Err(Error::Busy);
        }
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

    /// Adds a version to the row map and, if it has a vector, its entry to the pipeline.
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
        let row = row.filter(|row| row.vec.is_some()).cloned().map(Arc::new);
        let has_entry = row.is_some();
        let sh = &mut self.shared;
        let r = sh.ranges.get_mut(&id).ok_or(Error::NotHeld(id))?;
        if !has_entry && !r.keys.contains_key(key) {
            return Ok(());
        }
        let generation = r.generation;
        let inserted = sh.rows.insert(&mut r.keys, generation, key, (at, ts, row));
        let (slot, superseded) = inserted.ok_or(ORDER)?;
        if superseded {
            r.touches.push_back((at, ts, slot));
        }
        if has_entry {
            sh.rows.store_vector(slot, at)?;
            sh.rows.name(slot, at);
            *r.outstanding.entry(at).or_default() += 1;
            sh.inbox.push_back(Append {
                slot,
                at,
                generation,
            });
        }
        Ok(())
    }

    fn fits(&self, row: &Row) -> bool {
        let schema = &self.cfg.schema;
        let vec = row.vec.as_ref();
        vec.is_none_or(|v| v.len() == schema.dim && v.iter().all(|x| x.is_finite()))
            && row.cols.len() == schema.columns.len()
            && row
                .cols
                .iter()
                .zip(&schema.columns)
                .all(|(v, t)| v.kind() == *t && finite(v))
    }

    /// Takes a page of the feed, or a prefix of it, and returns the position it took. Pages may repeat
    /// and overlap, never skip. `Busy` takes nothing.
    pub fn apply(&mut self, id: RangeId, page: &Page) -> Result<Pos> {
        let r = self.range(id)?;
        let start = r.delivered;
        if matches!(r.service, Service::Loading(_)) || page.after > start {
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
        let mut taken = start;
        for group in fresh.chunk_by(|a, b| a.at == b.at) {
            let at = group[0].at;
            let r = self.range(id)?;
            let rows: usize = r.outstanding.values().sum();
            let full = r.outstanding.len() >= self.cfg.max_flight
                || rows + group.len() > self.cfg.max_flight_rows;
            if full && !r.outstanding.is_empty() {
                // Everything before this position is taken.
                taken = taken.max(at - 1);
                self.range_mut(id)?.delivered = taken;
                return if taken > start {
                    Ok(taken)
                } else {
                    Err(Error::Busy)
                };
            }
            for record in group {
                let put = match &record.change {
                    Change::Closed(ts) => {
                        self.range_mut(id)?.closes.push_back((at, *ts));
                        Ok(())
                    }
                    Change::Row {
                        key,
                        commit_ts,
                        row,
                    } => self.put_version(id, key, at, *commit_ts, row.as_ref()),
                };
                if let Err(error) = put {
                    let store = matches!(error, Error::Store(_));
                    let why = if store { "the vector store failed" } else { "the feed is invalid" };
                    self.fail_range(id, why);
                    return Err(error);
                }
            }
            taken = at;
            self.range_mut(id)?.delivered = at;
        }
        let r = self.range_mut(id)?;
        r.delivered = r.delivered.max(page.through);
        Ok(r.delivered)
    }

    /// `Advance`: a position counts as applied once every core finished it.
    pub fn advance(&mut self) {
        for r in self.shared.ranges.values_mut() {
            if matches!(r.service, Service::Loading(_)) {
                continue;
            }
            r.app = r
                .outstanding
                .first_key_value()
                .map_or(r.delivered, |(at, _)| at - 1);
            while let Some(&(at, ts)) = r.closes.front()
                && at <= r.app
            {
                r.closed = r.closed.max(ts);
                r.closes.pop_front();
            }
        }
    }

    /// `AdvanceLow`: retention moves on, and older cuts are refused from now on.
    pub fn advance_low(&mut self, id: RangeId, low: Pos) -> Result<()> {
        let sh = &mut self.shared;
        let r = sh.ranges.get_mut(&id).ok_or(Error::NotHeld(id))?;
        if matches!(r.service, Service::Loading(_)) || low > r.app {
            return Err(Error::CutTooNew { newest: r.app });
        }
        r.low = r.low.max(low);
        while let Some(&(at, ts, slot)) = r.touches.front()
            && at <= r.low
        {
            r.touches.pop_front();
            r.bound = r.bound.max(ts);
            if sh.rows.generation(slot) == Some(r.generation) {
                sh.rows.trim(Some(&mut r.keys), slot, r.low);
            }
        }
        Ok(())
    }

    /// `DropRange`: the range moved away, or is about to be installed again. Its entries are gone at once,
    /// in memory after on disk, and scans that pinned it fail.
    pub fn drop_range(&mut self, id: RangeId) -> Result<()> {
        if let Some(store) = &self.store {
            store.forget(id)?;
        }
        self.discard(id);
        self.shared.ranges.remove(&id);
        Ok(())
    }

    /// Takes the ranges whose vectors the store could not read out of service.
    fn fail_damaged(&mut self) {
        for generation in self.shared.rows.take_damage() {
            if let Some(id) = self.shared.gens.get(&generation).copied() {
                self.fail_range(id, "the vector store failed");
            }
        }
    }

    /// Takes the range out of service. Only another install brings it back.
    fn fail_range(&mut self, id: RangeId, reason: &str) {
        self.discard(id);
        if let Some(r) = self.shared.ranges.get_mut(&id) {
            r.service = Service::Out(reason.to_owned());
        }
    }

    fn discard(&mut self, id: RangeId) {
        let sh = &mut self.shared;
        let Some(r) = sh.ranges.get_mut(&id) else {
            return;
        };
        let keys = std::mem::take(&mut r.keys);
        r.outstanding.clear();
        r.touches.clear();
        r.closes.clear();
        sh.gens.remove(&r.generation);
        sh.readers.fail(r.generation);
        let generation = r.generation;
        self.checkpoint = None;
        for shard in &mut self.shards {
            shard.purge(sh, &self.codec, generation);
        }
        for slot in keys.into_values() {
            sh.rows.kill(slot);
        }
    }

    // ----- Pins.

    /// `StartScan`: fixes the read points and the partitions in one step, after the caller's barrier.
    /// `filter` may limit the partitions to one tree.
    pub fn pin(&mut self, ranges: &[(RangeId, ReadAt)], filter: Option<&Filter>) -> Result<Pin> {
        let (group, roots) = self.roots(filter);
        self.pin_roots(ranges, group, roots)
    }

    fn roots(&self, filter: Option<&Filter>) -> (Group, Vec<Root>) {
        let one = self
            .cfg
            .schema
            .partition_by
            .zip(filter)
            .and_then(|(c, f)| f.only(c));
        let group = one.and_then(Scalar::of);
        let roots = &self.shared.roots;
        match &group {
            Some(_) => (
                group.clone(),
                roots.get(&group).into_iter().cloned().collect(),
            ),
            None => (None, roots.values().cloned().collect()),
        }
    }

    fn pin_roots(
        &mut self,
        ranges: &[(RangeId, ReadAt)],
        group: Group,
        roots: Vec<Root>,
    ) -> Result<Pin> {
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
            let newest = CommitTs::MAX;
            let point = match read {
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
                ReadAt::Cut(ts) if ts < r.bound => {
                    return Err(Error::CutTooOld { oldest: r.bound });
                }
                ReadAt::Cut(ts) if ts > r.closed => {
                    return Err(Error::CutTooNew { newest: r.closed });
                }
                ReadAt::Cut(cut) => Point { pos: r.app, cut },
            };
            points.insert(r.generation, point);
            pinned.push((id, r.app));
        }
        let failed = AtomicBool::new(false);
        let inner = |id| PinInner {
            id,
            points,
            ranges: pinned,
            roots,
            group,
            failed,
        };
        Ok(Pin(self.shared.readers.begin(inner, true)))
    }

    // ----- Queues.

    /// Routes waiting appends to the queues of their cores. Returns whether any moved.
    pub fn pump(&mut self) -> bool {
        let sh = &mut self.shared;
        let mut moved = false;
        while let Some(a) = sh.inbox.front().copied() {
            if sh.gens.contains_key(&a.generation) {
                let Some(part) = sh.assign(&a) else {
                    return moved;
                };
                let owner = part.0 as usize % sh.shards;
                if sh.queues[owner].len() >= sh.queue_len {
                    return moved;
                }
                sh.queues[owner].push_back(Msg::Append(a, part));
            } else {
                sh.forget(a.generation, a.slot, a.at);
            }
            sh.inbox.pop_front();
            moved = true;
        }
        moved
    }

    /// One message of a shard's queue. Returns whether it was handled.
    pub fn step(&mut self, shard: usize) -> bool {
        let sh = &mut self.shared;
        let Some(msg) = sh.queues.get_mut(shard).and_then(VecDeque::pop_front) else {
            return false;
        };
        let handled = match self.shards[shard].handle(sh, &self.codec, msg) {
            None => true,
            Some(wait) => {
                sh.queues[shard].push_back(wait);
                false
            }
        };
        self.fail_damaged();
        handled
    }

    /// Handles every message and advances the positions.
    pub fn run(&mut self) {
        loop {
            let pumped = self.pump();
            let stepped = (0..self.shards.len()).fold(false, |any, s| self.step(s) || any);
            if !pumped && !stepped {
                break;
            }
        }
        self.fail_damaged();
        self.advance();
    }

    // ----- Split, merge and reassignment.

    fn owner(&mut self, part: PartId) -> (&mut Shared, &mut Shard<C>, &C) {
        let owner = part.0 as usize % self.shards.len();
        (&mut self.shared, &mut self.shards[owner], &self.codec)
    }

    /// `StartDrain`. A partition that another one drains into does not start to drain.
    fn may_drain(&self, part: PartId) -> bool {
        let ok = |m: &Meta| {
            m.state == State::Ready
                && !m.undirected
                && (m.drained_into == 0 || mutant("prefix_drain_a_target"))
        };
        self.shared.dir.get(&part).is_some_and(ok)
    }

    fn start_drain(&mut self, part: PartId, targets: Vec<PartId>) {
        let sh = &mut self.shared;
        for t in &targets {
            if let Some(meta) = sh.dir.get_mut(t) {
                meta.drained_into += 1;
            }
        }
        if let Some(meta) = sh.dir.get_mut(&part) {
            meta.state = State::Draining;
            meta.targets = targets;
            if let Some(root) = sh.roots.get_mut(&meta.group) {
                root.tree.set_draining(part);
            }
        }
    }

    /// Drains a partition into two new ones.
    pub fn split(&mut self, part: PartId) -> bool {
        if !self.may_drain(part) {
            return false;
        }
        let (sh, shard, _) = self.owner(part);
        let dirs = shard.directions(sh, part);
        let points: Vec<&[f32]> = dirs.iter().map(Vec::as_slice).collect();
        if points.is_empty() {
            return false;
        }
        let (_, centroids) = bisect(&points);
        let Some(group) = sh.dir.get(&part).map(|m| m.group.clone()) else {
            return false;
        };
        let new = centroids.map(|c| sh.new_part(group.clone(), c.into(), false));
        let [Some(a), Some(b)] = new else {
            return false;
        };
        self.start_drain(part, vec![a, b]);
        true
    }

    /// Drains a partition into an existing one.
    pub fn merge(&mut self, part: PartId, into: PartId) -> bool {
        let (Some(a), Some(b)) = (self.shared.dir.get(&part), self.shared.dir.get(&into)) else {
            return false;
        };
        let fits = part != into && a.group == b.group && b.state == State::Ready && !b.undirected;
        if !fits || !self.may_drain(part) {
            return false;
        }
        self.start_drain(part, vec![into]);
        true
    }

    fn send_copies(&mut self, part: PartId, n: usize) -> usize {
        let (sh, shard, _) = self.owner(part);
        shard.send_copies(sh, part, n)
    }

    fn finish_drain(&mut self, part: PartId) -> bool {
        let (sh, shard, _) = self.owner(part);
        shard.finish_drain(sh, part)
    }

    fn free_part(&mut self, part: PartId) -> bool {
        let (sh, shard, codec) = self.owner(part);
        shard.free_part(sh, codec, part)
    }

    fn remove_moved(&mut self, part: PartId) -> usize {
        let (sh, shard, codec) = self.owner(part);
        shard.remove_moved(sh, codec, part)
    }

    fn collect(&mut self, part: PartId) -> usize {
        let (sh, shard, codec) = self.owner(part);
        shard.collect(sh, codec, part)
    }

    fn reassign(&mut self, part: PartId, id: u64, to: PartId) -> bool {
        let (sh, shard, codec) = self.owner(part);
        let same = sh.dir.get(&part).map(|m| &m.group) == sh.dir.get(&to).map(|m| &m.group);
        same && shard.reassign(sh, codec, part, id, to)
    }

    /// Moves entries that a split left nearer to another partition. One partition per call.
    fn recheck(&mut self) -> bool {
        let Some(part) = self.shared.recheck.pop_first() else {
            return false;
        };
        let n = self.cfg.reassigns;
        let (sh, shard, _) = self.owner(part);
        let strays = shard.strays(sh, part, n);
        let more = strays.len() == n && n > 0;
        for (id, to) in strays {
            self.reassign(part, id, to);
        }
        if more {
            self.shared.recheck.insert(part);
        }
        true
    }

    /// Background work on up to `parts` partitions. Returns whether anything was done.
    pub fn maintain(&mut self, parts: usize) -> bool {
        let mut done = self.recheck();
        for _ in 0..parts.min(self.shared.dir.len()) {
            let after = self
                .shared
                .dir
                .range(PartId(self.cursor.0.saturating_add(1))..);
            let first = self.shared.dir.first_key_value();
            let Some(part) = after.map(|(p, _)| *p).next().or(first.map(|(p, _)| *p)) else {
                break;
            };
            self.cursor = part;
            done |= self.maintain_part(part);
        }
        self.fail_damaged();
        done
    }

    fn maintain_part(&mut self, part: PartId) -> bool {
        let Some(state) = self.shared.dir.get(&part).map(|m| m.state) else {
            return false;
        };
        match state {
            State::Retired => self.free_part(part),
            State::Draining => {
                self.send_copies(part, self.cfg.copies) > 0 || self.finish_drain(part)
            }
            State::Ready => {
                let removed = self.remove_moved(part) + self.collect(part) > 0;
                let entries = self.shared.dir.get(&part).map_or(0, |m| m.entries);
                if entries > self.cfg.part_max {
                    return self.split(part) || removed;
                }
                if entries < self.cfg.part_min
                    && let Some(into) = self.neighbour(part)
                {
                    return self.merge(part, into) || removed;
                }
                removed
            }
        }
    }

    /// The nearest other partition with room for this one.
    fn neighbour(&self, part: PartId) -> Option<PartId> {
        let sh = &self.shared;
        let meta = sh.dir.get(&part).filter(|m| !m.undirected)?;
        let (near, _) = sh
            .roots
            .get(&meta.group)?
            .tree
            .nearest(&meta.centroid, Some(part))?;
        let room = sh.dir.get(&near)?.entries + meta.entries <= self.cfg.part_max;
        room.then_some(near)
    }

    /// The partitions that a new scan lists, without the retired ones.
    fn listed(&self) -> BTreeSet<PartId> {
        parts_of(self.shared.roots.values())
    }
}

/// Every partition under some roots, the lists of vectors without a direction included.
fn parts_of<'a>(roots: impl Iterator<Item = &'a Root>) -> BTreeSet<PartId> {
    roots
        .flat_map(|r| r.tree.parts().into_iter().chain(r.undirected))
        .collect()
}
