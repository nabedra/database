//! Replays random interleavings of the actions of `specs/vector/index/LocalIndex.tla` on the code,
//! one code step per action, next to an oracle that keeps the feed, and checks the spec's properties
//! after every step. Then it does the same with each mutant of the spec and expects the property
//! that the mutant's `expect:` line names to fail.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::fs::FaultFs;

use super::super::codec::{exact, goodness};
use super::super::mutant::MUTANT;
use super::super::*;
use crate::types::*;

const DIM: usize = 2;
const DIR: &str = "/ck";

/// One position of a range's log, and what its feed says.
#[derive(Clone, Debug)]
enum Entry {
    Up {
        key: u8,
        vec: Vec<f32>,
    },
    /// A row without a vector.
    Null {
        key: u8,
    },
    Del {
        key: u8,
    },
    Closed(CommitTs),
}

impl Entry {
    fn key(&self) -> Option<u8> {
        match self {
            Entry::Up { key, .. } | Entry::Null { key } | Entry::Del { key } => Some(*key),
            Entry::Closed(_) => None,
        }
    }
}

/// The oracle of one range.
#[derive(Clone)]
struct Feed {
    id: RangeId,
    log: Vec<Entry>,
    held: bool,
    /// A range snapshot at this position is being installed.
    loading: bool,
}

impl Feed {
    /// Commit timestamps grow per key and not along the log.
    fn ts(&self, pos: Pos) -> CommitTs {
        let key = self.log[pos as usize - 1].key().unwrap_or(0);
        pos * 10 + if key == 1 { 15 } else { 0 }
    }

    fn touched(&self, key: u8, lo: Pos, p: Point) -> bool {
        (lo + 1..=p.pos).any(|n| self.log[n as usize - 1].key() == Some(key) && self.ts(n) <= p.cut)
    }

    /// `VisibleAt`.
    fn visible(&self, pos: Pos, p: Point) -> bool {
        let Entry::Up { key, .. } = self.log[pos as usize - 1] else {
            return false;
        };
        pos <= p.pos && self.ts(pos) <= p.cut && !self.touched(key, pos, p)
    }

    fn live(&self, p: Point) -> Vec<Pos> {
        (1..=p.pos).filter(|&n| self.visible(n, p)).collect()
    }

    fn row(&self, range: u8, pos: Pos) -> Option<Row> {
        let vec = match &self.log[pos as usize - 1] {
            Entry::Up { vec, .. } => Some(vec.as_slice().into()),
            Entry::Null { .. } => None,
            _ => return None,
        };
        Some(Row {
            vec,
            cols: Vec::new(),
            image: Arc::new([range, pos as u8]),
        })
    }

    fn page(&self, range: u8, after: Pos, through: Pos) -> Page {
        let record = |at: Pos| {
            let change = match &self.log[at as usize - 1] {
                Entry::Closed(ts) => Change::Closed(*ts),
                e => Change::Row {
                    key: key_of(range, e.key().unwrap()),
                    commit_ts: self.ts(at),
                    row: self.row(range, at),
                },
            };
            Record { at, change }
        };
        Page {
            after,
            through,
            records: (after + 1..=through).map(record).collect(),
        }
    }
}

fn key_of(range: u8, key: u8) -> Key {
    Arc::new([range, key])
}

struct Reading {
    scan: Option<Scan>,
    query: Vec<f32>,
    op: Op,
    /// What the pin sees: (range, position).
    expect: BTreeSet<(u8, Pos)>,
    ranges: BTreeSet<u8>,
    /// A range of the pin was dropped or installed again.
    replaced: bool,
}

/// A broken property.
type Broken = (&'static str, String);

struct World {
    ix: Index<ExactCodec>,
    fs: FaultFs,
    feeds: Vec<Feed>,
    reading: Vec<Reading>,
    /// Mutant `prefix_probe_before_pin`.
    probed: Option<Vec<Root>>,
    /// Splits, merges and moves left.
    drains: u32,
    moves: u32,
    done: BTreeMap<&'static str, u32>,
    /// The last action did something.
    changed: bool,
    /// How often each action of `act` is tried.
    weights: [u32; 22],
    /// Crashes keep the disk.
    only_disk: bool,
    /// The most feed entries of a range.
    log: usize,
}

fn config(shards: usize) -> Config {
    let schema = Schema {
        dim: DIM,
        columns: Vec::new(),
        partition_by: None,
    };
    Config {
        queue: 3,
        max_flight: 3,
        ..Config::new(schema, shards)
    }
}

fn store(fs: &FaultFs) -> Store {
    Store::new(Arc::new(fs.clone()), DIR)
}

fn vector(g: &mut Gen) -> Vec<f32> {
    let dir = *g.pick(&[
        [1.0f32, 0.0],
        [0.0, 1.0],
        [-1.0, 0.0],
        [0.0, -1.0],
        [0.6, 0.8],
        [-0.8, 0.6],
    ]);
    let len = *g.pick(&[1.0f32, 1.0, 1.0, 3.0, 0.1, 100.0, 0.0]);
    dir.map(|x| x * len).to_vec()
}

impl World {
    fn new(g: &mut Gen) -> World {
        let fs = FaultFs::new();
        let shards = 1 + g.below(3) as usize;
        let feeds = 1 + g.below(2) as u128;
        let mut w = World {
            ix: Index::new(config(shards), ExactCodec, Some(store(&fs))).unwrap(),
            fs,
            feeds: Vec::new(),
            reading: Vec::new(),
            probed: None,
            drains: g.below(6) as u32,
            moves: g.below(6) as u32,
            done: BTreeMap::new(),
            changed: false,
            weights: [0; 22],
            only_disk: g.bool(),
            log: *g.pick(&[4, 6, 12]),
        };
        // Each run prefers some actions and never takes some others, so that rare orders come up.
        for (i, w) in w.weights.iter_mut().enumerate() {
            *w = match i {
                0..=4 => *g.pick(&[4, 8, 16]),
                18 | 19 | 21 => *g.pick(&[0, 0, 1]),
                _ => *g.pick(&[0, 2, 4, 8]),
            };
        }
        if g.bool() {
            // Start with rows that are there.
            for _ in 0..3 {
                for f in 0..w.feeds.len() {
                    let key = g.below(2) as u8;
                    w.feeds[f].log.push(Entry::Up {
                        key,
                        vec: vector(g),
                    });
                    let n = w.feeds[f].log.len() as Pos;
                    w.ix.apply(w.feeds[f].id, &w.feeds[f].page(f as u8, n - 1, n))
                        .unwrap();
                }
                w.ix.run();
            }
        }
        for id in 0..feeds {
            w.feeds.push(Feed {
                id: RangeId(id),
                log: Vec::new(),
                held: true,
                loading: false,
            });
            w.install_empty(id as usize).unwrap();
        }
        // Two partitions, as `MCFirst2`: entries may move at once.
        if g.bool() {
            for dir in [[1.0, 0.0], [-1.0, 0.0]] {
                w.ix.shared.new_part(None, Arc::new(dir), false);
            }
        }
        w
    }

    fn install_empty(&mut self, f: usize) -> Result<()> {
        let id = self.feeds[f].id;
        self.ix.begin_load(id, 0, 0)?;
        self.ix.finish_load(id)?;
        self.ix.caught_up(id, 0)
    }

    fn status(&self, f: usize) -> RangeStatus {
        self.ix.status(self.feeds[f].id).unwrap()
    }

    fn parts(&self, state: State) -> Vec<PartId> {
        let dir = self.ix.shared.dir.iter();
        dir.filter(|(_, m)| m.state == state && !m.undirected)
            .map(|(p, _)| *p)
            .collect()
    }

    fn entries(&self, part: PartId) -> Vec<shard::Entry> {
        let owner = part.0 as usize % self.ix.shards.len();
        let p = self.ix.shards[owner].parts.get(&part).into_iter();
        p.flat_map(|p| p.entries().copied()).collect()
    }

    fn image(&self, e: &shard::Entry) -> Option<(u8, Pos)> {
        let row = self.ix.shared.rows.version(e.slot, e.at)?.row.as_ref()?;
        Some((row.image[0], Pos::from(row.image[1])))
    }

    fn did(&mut self, action: &'static str, did: bool) -> std::result::Result<(), Broken> {
        *self.done.entry(action).or_default() += u32::from(did);
        self.changed |= did;
        Ok(())
    }

    /// One random action. Some do nothing in this state.
    fn act(&mut self, g: &mut Gen) -> std::result::Result<(), Broken> {
        let f = g.below(self.feeds.len() as u64) as usize;
        let shard = g.below(self.ix.shards.len() as u64) as usize;
        let mut all: Vec<PartId> = self.ix.shared.dir.keys().copied().collect();
        let turn = g.below(all.len().max(1) as u64) as usize;
        if !all.is_empty() {
            all.rotate_left(turn);
        }
        let ready = self.parts(State::Ready);
        let mut draining = self.parts(State::Draining);
        if g.bool() {
            draining.reverse();
        }
        let mut ticket = g.below(u64::from(self.weights.iter().sum::<u32>().max(1))) as u32;
        let pick = |w: &u32| {
            let here = ticket < *w;
            ticket = ticket.saturating_sub(*w);
            here
        };
        match self.weights.iter().position(pick).unwrap_or(0) {
            0 => self.new_op(g, f),
            1 => self.deliver(g, f),
            2 => {
                let did = self.ix.pump();
                self.did("pump", did)
            }
            3 => {
                let did = self.ix.step(shard);
                self.did("step", did)
            }
            4 => {
                let before: Vec<Pos> = self.ix.shared.ranges.values().map(|r| r.app).collect();
                self.ix.advance();
                let did = self.ix.shared.ranges.values().map(|r| r.app).ne(before);
                self.did("Advance", did)
            }
            5 if self.feeds[f].held => {
                let s = self.status(f);
                let low = s.low + u64::from(g.bool());
                let did = self.ix.advance_low(self.feeds[f].id, low).is_ok() && low > s.low;
                self.did("AdvanceLow", did)
            }
            6 if self.drains > 0 && !ready.is_empty() => {
                let did = self.ix.split(*g.pick(&ready));
                self.drains -= u32::from(did);
                self.did("split", did)
            }
            7 if self.drains > 0 && ready.len() > 1 => {
                let did = self.ix.merge(*g.pick(&ready), *g.pick(&ready));
                self.drains -= u32::from(did);
                self.did("merge", did)
            }
            8 if self.moves > 0 && ready.len() > 1 => {
                let full = ready.iter().copied().find(|p| !self.entries(*p).is_empty());
                let from = if g.bool() {
                    full.unwrap_or(ready[0])
                } else {
                    *g.pick(&ready)
                };
                let here = self.entries(from);
                let others: Vec<PartId> = ready.iter().copied().filter(|p| *p != from).collect();
                let did = !here.is_empty() && {
                    let e = *g.pick(&here);
                    self.ix.reassign(from, e.id, *g.pick(&others))
                };
                self.moves -= u32::from(did);
                self.did("Reassign", did)
            }
            9 => {
                let n = 1 + g.below(2) as usize;
                let did = draining.iter().any(|p| self.ix.send_copies(*p, n) > 0);
                self.did("SendCopy", did)
            }
            10 => {
                let did = draining.iter().any(|p| self.ix.finish_drain(*p));
                self.did("FinishDrain", did)
            }
            11 => {
                let did = all.iter().any(|p| self.ix.free_part(*p));
                self.did("FreePart", did)
            }
            12 => {
                let did = all.iter().any(|p| self.ix.remove_moved(*p) > 0);
                self.did("RemoveMoved", did)
            }
            13 => {
                let did = all.iter().any(|p| self.ix.collect(*p) > 0);
                self.did("Collect", did)
            }
            14 => self.start_scan(g),
            15 => self.visit(g),
            16 => {
                let did = self.ix.begin_checkpoint().is_ok();
                self.did("BeginCheckpoint", did)
            }
            17 if self.ix.checkpoint.is_some() => self.checkpoint_step(),
            18 => self.crash(g),
            19 => self.resync(g, f),
            20 if self.feeds[f].loading => self.finish_resync(f),
            21 => self.drop_range(f),
            _ => Ok(()),
        }
    }

    fn new_op(&mut self, g: &mut Gen, f: usize) -> std::result::Result<(), Broken> {
        let feed = &self.feeds[f];
        if feed.log.len() >= self.log {
            return Ok(());
        }
        let key = g.below(2) as u8;
        let pos = feed.log.len() as Pos;
        let entry = match g.below(8) {
            0 => Entry::Null { key },
            1 | 2 => Entry::Del { key },
            3 => Entry::Closed((pos + 1) * 10),
            _ => Entry::Up {
                key,
                vec: vector(g),
            },
        };
        self.feeds[f].log.push(entry);
        self.did("NewOp", true)
    }

    /// A page of the feed, also one that repeats.
    fn deliver(&mut self, g: &mut Gen, f: usize) -> std::result::Result<(), Broken> {
        let feed = &self.feeds[f];
        if !feed.held || feed.loading {
            return Ok(());
        }
        let delivered = self.status(f).delivered;
        let end = feed.log.len() as Pos;
        let after = delivered
            - if g.chance(1, 4) {
                g.below(delivered.min(2) + 1)
            } else {
                0
            };
        let through = (after + 1 + g.below(3)).min(end);
        if through < after {
            return Ok(());
        }
        let page = feed.page(f as u8, after, through);
        match self.ix.apply(feed.id, &page) {
            Ok(to) => self.did("deliver", to > delivered),
            Err(Error::Busy) => self.did("busy", true),
            Err(e) => Err(("Feed", format!("{e}"))),
        }
    }

    fn start_scan(&mut self, g: &mut Gen) -> std::result::Result<(), Broken> {
        if self.reading.len() >= 2 {
            return Ok(());
        }
        let mut reads = Vec::new();
        let (mut expect, mut ranges) = (BTreeSet::new(), BTreeSet::new());
        for (f, feed) in self.feeds.iter().enumerate() {
            if !feed.held || feed.loading || (f > 0 && g.below(4) == 0) {
                continue;
            }
            let s = self.status(f);
            let (read, point) = match g.below(3) {
                0 => (
                    ReadAt::Latest { barrier: s.applied },
                    Point {
                        pos: s.applied,
                        cut: CommitTs::MAX,
                    },
                ),
                1 => {
                    let pos = s.low + g.below(s.applied - s.low + 1);
                    (
                        ReadAt::Position(pos),
                        Point {
                            pos,
                            cut: CommitTs::MAX,
                        },
                    )
                }
                _ if s.closed >= s.bound => {
                    let cut = s.bound + g.below(s.closed - s.bound + 1);
                    (
                        ReadAt::Cut(cut),
                        Point {
                            pos: s.applied,
                            cut,
                        },
                    )
                }
                _ => continue,
            };
            reads.push((feed.id, read));
            ranges.insert(f as u8);
            expect.extend(feed.live(point).into_iter().map(|n| (f as u8, n)));
        }
        let pin = if mutant("prefix_probe_before_pin") {
            match self.probed.take() {
                None => {
                    self.probed = Some(self.ix.roots(None).1);
                    return Ok(());
                }
                Some(roots) => self.ix.pin_roots(&reads, None, roots),
            }
        } else {
            self.ix.pin(&reads, None)
        };
        let pin = pin.map_err(|e| ("ExactScan", format!("a pin was refused: {e}")))?;
        let op = *g.pick(&[Op::Dot, Op::L2]);
        let query = Query {
            op,
            vec: vector(g),
            limit: 100,
            filter: None,
            exact: true,
            recall: 1.0,
        };
        let mut scan = self
            .ix
            .scan(&pin, &query, Box::new(RecallEstimate))
            .unwrap();
        scan.rotate(g.below(8) as usize);
        let reading = Reading {
            scan: Some(scan),
            query: query.vec,
            op,
            expect,
            ranges,
            replaced: false,
        };
        self.reading.push(reading);
        self.did("StartScan", true)
    }

    fn visit(&mut self, g: &mut Gen) -> std::result::Result<(), Broken> {
        if self.reading.is_empty() {
            return Ok(());
        }
        let i = g.below(self.reading.len() as u64) as usize;
        let r = &mut self.reading[i];
        let Some(scan) = &mut r.scan else {
            return Ok(());
        };
        match self.ix.visit(scan) {
            Ok(true) => self.did("Visit", true),
            Ok(false) => {
                let mut r = self.reading.remove(i);
                let result = self.ix.finish(r.scan.take().unwrap());
                self.check_scan(&r, result)
            }
            Err(e) => {
                let r = self.reading.remove(i);
                self.check_scan(&r, Err(e))
            }
        }
    }

    fn check_scan(
        &mut self,
        r: &Reading,
        result: Result<Vec<Hit>>,
    ) -> std::result::Result<(), Broken> {
        let hits = match result {
            Ok(hits) => hits,
            Err(Error::PinFailed) if r.replaced => return self.did("scan failed", true),
            Err(e) => return Err(("ExactScan", format!("a scan failed: {e}"))),
        };
        let seen: Vec<(u8, Pos)> = hits
            .iter()
            .map(|h| (h.row.image[0], Pos::from(h.row.image[1])))
            .collect();
        let set: BTreeSet<(u8, Pos)> = seen.iter().copied().collect();
        if set.len() != seen.len() {
            return Err(("NoDuplicate", format!("a row twice: {seen:?}")));
        }
        if let Some(x) = set.difference(&r.expect).next() {
            return Err((
                "NoStaleEntry",
                format!("{x:?} is not visible, expected {:?}", r.expect),
            ));
        }
        if let Some(x) = r.expect.difference(&set).next() {
            return Err(("ExactScan", format!("{x:?} is missing")));
        }
        // The order and the scores of brute force.
        let mut brute: Vec<(f32, Key)> = Vec::new();
        for &(f, pos) in &r.expect {
            let Entry::Up { key, vec } = &self.feeds[f as usize].log[pos as usize - 1] else {
                unreachable!()
            };
            brute.push((exact(r.op, vec, &r.query).unwrap(), key_of(f, *key)));
        }
        brute.sort_by(|a, b| {
            goodness(r.op, b.0)
                .total_cmp(&goodness(r.op, a.0))
                .then(a.1.cmp(&b.1))
        });
        let got: Vec<(f32, Key)> = hits.iter().map(|h| (h.score, h.key.clone())).collect();
        let same = got.len() == brute.len()
            && got
                .iter()
                .zip(&brute)
                .all(|(a, b)| a.0 == b.0 && a.1 == b.1);
        if !same {
            return Err(("ExactScan", format!("{got:?} is not {brute:?}")));
        }
        self.did("FinishScan", true)
    }

    fn checkpoint_step(&mut self) -> std::result::Result<(), Broken> {
        let todo = !self.ix.checkpoint.as_ref().unwrap().is_written();
        let action = if todo { "WriteFile" } else { "InstallManifest" };
        let result = if todo {
            self.ix.write_file().map(|_| ())
        } else {
            self.ix.install_manifest()
        };
        result.map_err(|e| ("Checkpoint", format!("{action}: {e}")))?;
        self.did(action, true)
    }

    fn crash(&mut self, g: &mut Gen) -> std::result::Result<(), Broken> {
        self.reading.clear();
        self.probed = None;
        let shards = self.ix.shards.len();
        let how = if self.only_disk { 0 } else { g.below(3) };
        let fs = match how {
            0 => self.fs.crash(),
            1 => FaultFs::new(),
            _ => {
                let here = FaultFs::new();
                let peer = FaultFs::new();
                let mut other = Index::new(config(shards), ExactCodec, Some(store(&peer))).unwrap();
                for (f, feed) in self.feeds.iter().enumerate().filter(|(_, f)| f.held) {
                    other.begin_load(feed.id, 0, 0).unwrap();
                    other.finish_load(feed.id).unwrap();
                    let n = g.below(feed.log.len() as u64 + 1);
                    let mut at = 0;
                    while at < n {
                        at = other.apply(feed.id, &feed.page(f as u8, at, n)).unwrap();
                        other.run();
                    }
                    other.advance_low(feed.id, g.below(n + 1)).unwrap();
                }
                other.checkpoint().unwrap();
                copy_checkpoint(&store(&peer), &store(&here)).unwrap();
                here.crash()
            }
        };
        let (ix, damage) = Index::recover(config(shards), ExactCodec, store(&fs)).unwrap();
        (self.ix, self.fs) = (ix, fs);
        if !damage.is_empty() {
            return Err((
                "NoLostRow",
                format!("a synced checkpoint does not load: {damage:?}"),
            ));
        }
        for f in 0..self.feeds.len() {
            let id = self.feeds[f].id;
            let there = self.ix.status(id).is_ok();
            let was = self.feeds[f].held && !self.feeds[f].loading;
            self.feeds[f].loading = false;
            match (there, self.feeds[f].held) {
                (true, true) if was || how == 2 => self.ix.caught_up(id, 0).unwrap(),
                (true, _) => return Err(("SingleGeneration", "a dropped range came back".into())),
                (false, true) => self.install_empty(f).unwrap(),
                (false, false) => {}
            }
        }
        self.did(
            ["crash disk", "crash empty", "crash peer"][how as usize],
            true,
        )
    }

    fn replaced(&mut self, f: usize) {
        for r in &mut self.reading {
            r.replaced |= r.ranges.contains(&(f as u8));
        }
    }

    /// The first half of `Resync`, and rows of the snapshot.
    fn resync(&mut self, g: &mut Gen, f: usize) -> std::result::Result<(), Broken> {
        let feed = self.feeds[f].clone();
        if feed.loading {
            return Ok(());
        }
        let from = if feed.held { self.status(f).applied } else { 0 };
        let n = from + g.below(feed.log.len() as Pos - from + 1);
        let bound = (1..=n).map(|p| feed.ts(p)).max().unwrap_or(0);
        self.replaced(f);
        self.ix.begin_load(feed.id, n, bound).unwrap();
        let live = feed.live(Point {
            pos: n,
            cut: CommitTs::MAX,
        });
        let row = |&p: &Pos| {
            let key = key_of(f as u8, feed.log[p as usize - 1].key().unwrap());
            (key, feed.ts(p), feed.row(f as u8, p).unwrap())
        };
        let rows: Vec<_> = live.iter().map(row).collect();
        self.ix.load_rows(feed.id, &rows).unwrap();
        self.feeds[f].held = true;
        self.feeds[f].loading = true;
        self.did("Resync", true)
    }

    fn finish_resync(&mut self, f: usize) -> std::result::Result<(), Broken> {
        let id = self.feeds[f].id;
        let did = self.ix.finish_load(id).is_ok();
        if did {
            self.ix.caught_up(id, 0).unwrap();
            self.feeds[f].loading = false;
        }
        self.did("finish Resync", did)
    }

    fn drop_range(&mut self, f: usize) -> std::result::Result<(), Broken> {
        if !self.feeds[f].held {
            return Ok(());
        }
        self.replaced(f);
        self.ix.drop_range(self.feeds[f].id).unwrap();
        self.feeds[f].held = false;
        self.feeds[f].loading = false;
        self.did("DropRange", true)
    }

    // ----- Properties.

    fn check(&self) -> std::result::Result<(), Broken> {
        self.single_generation()?;
        self.no_lost_row()?;
        self.drain_ends()?;
        if mutant("none") { self.audit() } else { Ok(()) }
    }

    fn single_generation(&self) -> std::result::Result<(), Broken> {
        for part in self.ix.shared.dir.keys() {
            let old = |e: &shard::Entry| !self.ix.shared.gens.contains_key(&e.generation);
            if let Some(e) = self.entries(*part).into_iter().find(old) {
                return Err(("SingleGeneration", format!("{part:?} holds {e:?}")));
            }
        }
        Ok(())
    }

    fn no_lost_row(&self) -> std::result::Result<(), Broken> {
        let listed: Vec<_> = self
            .ix
            .listed()
            .into_iter()
            .flat_map(|p| self.entries(p))
            .collect();
        let there: BTreeSet<(u8, Pos)> = listed.iter().filter_map(|e| self.image(e)).collect();
        for (f, feed) in self
            .feeds
            .iter()
            .enumerate()
            .filter(|(_, f)| f.held && !f.loading)
        {
            let s = self.status(f);
            for pos in s.low..=s.applied {
                for n in feed.live(Point {
                    pos,
                    cut: CommitTs::MAX,
                }) {
                    if !there.contains(&(f as u8, n)) {
                        return Err((
                            "NoLostRow",
                            format!("range {f}: {n} is not there for cut {pos}"),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Under the spec's fairness alone (`Land`, `SendCopy`, `FinishDrain`) every drain ends.
    fn drain_ends(&self) -> std::result::Result<(), Broken> {
        if self.parts(State::Draining).is_empty() {
            return Ok(());
        }
        let mut ix = self.ix.clone();
        let draining = |ix: &Index| {
            let dir = ix.shared.dir.iter();
            dir.filter(|(_, m)| m.state == State::Draining)
                .map(|(p, _)| *p)
                .collect::<Vec<_>>()
        };
        loop {
            let mut progress = ix.pump();
            for s in 0..ix.shards.len() {
                for _ in 0..ix.shared.queues[s].len() {
                    progress |= ix.step(s);
                }
            }
            for p in draining(&ix) {
                progress |= ix.send_copies(p, 8) > 0;
                progress |= ix.finish_drain(p);
            }
            if !progress {
                break;
            }
        }
        match draining(&ix).first() {
            Some(p) => Err(("DrainEnds", format!("{p:?} drains forever"))),
            None => Ok(()),
        }
    }

    /// What the code counts is what is there.
    fn audit(&self) -> std::result::Result<(), Broken> {
        let sh = &self.ix.shared;
        let mut refs: BTreeMap<(Slot, Pos), u32> = BTreeMap::new();
        let fail = |why: String| Err(("Audit", why));
        for (part, meta) in &sh.dir {
            let here = self.entries(*part);
            for e in &here {
                *refs.entry((e.slot, e.at)).or_default() += 1;
                if sh.rows.generation(e.slot) != Some(e.generation) {
                    return fail(format!("{e:?} names a slot of another generation"));
                }
            }
            let copies = || sh.queues.iter().flatten();
            let leaving = copies()
                .filter(|m| matches!(m, Msg::Copy(c) if c.from == *part))
                .count();
            let arriving = copies()
                .filter(|m| matches!(m, Msg::Copy(c) if c.to == *part))
                .count();
            let into = sh.dir.values().filter(|m| m.targets.contains(part)).count();
            let counts = (here.len(), leaving, arriving, into);
            if counts != (meta.entries, meta.leaving, meta.arriving, meta.drained_into) {
                return fail(format!("{part:?} counts wrong: {counts:?}"));
            }
        }
        let queued = sh.queues.iter().flatten().map(|m| match m {
            Msg::Append(a, _) => (a.slot, a.at),
            Msg::Copy(c) => (c.entry.slot, c.entry.at),
        });
        for named in queued.chain(sh.inbox.iter().map(|a| (a.slot, a.at))) {
            *refs.entry(named).or_default() += 1;
        }
        for rows::Census { slot, versions } in sh.rows.census() {
            for (at, counted) in versions {
                let there = refs.remove(&(slot, at)).unwrap_or(0);
                if there != counted {
                    return fail(format!(
                        "slot {slot} version {at} counts {counted}, has {there}"
                    ));
                }
            }
        }
        if !refs.is_empty() {
            return fail(format!("these name a slot that was reused: {refs:?}"));
        }
        if sh.queues.iter().any(|q| q.len() > sh.queue_len) {
            return fail("a queue is too long".into());
        }
        Ok(())
    }
}

const STEPS: usize = 500;

/// One run. Returns how often each action did something.
fn run(g: &mut Gen, steps: usize) -> std::result::Result<BTreeMap<&'static str, u32>, Broken> {
    let mut w = World::new(g);
    for _ in 0..steps {
        w.changed = false;
        w.act(g)?;
        if w.changed {
            w.check()?;
        }
    }
    // Everything settles: scans end, positions catch up, and the index answers like brute force.
    for f in 0..w.feeds.len() {
        for _ in 0..8 {
            w.ix.run();
            let feed = w.feeds[f].clone();
            if feed.loading {
                w.finish_resync(f)?;
            } else if feed.held {
                let page = feed.page(f as u8, w.status(f).delivered, feed.log.len() as Pos);
                match w.ix.apply(feed.id, &page) {
                    Ok(_) | Err(Error::Busy) => {}
                    Err(e) => return Err(("Feed", format!("{e}"))),
                }
            }
            for p in w.ix.shared.dir.keys().copied().collect::<Vec<_>>() {
                w.ix.maintain_part(p);
            }
            w.check()?;
        }
    }
    w.reading.clear();
    w.start_scan(g)?;
    while !w.reading.is_empty() {
        w.visit(g)?;
    }
    Ok(w.done)
}

/// VEC-G05, VEC-G03: `LocalIndex.tla`'s actions in random order keep its properties.
#[test]
fn vs07_random_interleavings_keep_the_spec_properties() {
    let total = std::cell::RefCell::new(BTreeMap::<&'static str, u32>::new());
    let runs = Runs {
        cases: 6 * Runs::default().cases,
        ..Runs::default()
    };
    check_with(runs, "vs07_random_interleavings", |g| {
        let done = run(g, STEPS).map_err(|(p, why)| format!("{p}: {why}"))?;
        for (action, n) in done {
            *total.borrow_mut().entry(action).or_default() += n;
        }
        Ok(())
    });
    // Every action of the spec took place.
    let total = total.into_inner();
    let actions = [
        "NewOp",
        "deliver",
        "busy",
        "pump",
        "step",
        "Advance",
        "AdvanceLow",
        "split",
        "merge",
        "Reassign",
        "SendCopy",
        "FinishDrain",
        "FreePart",
        "RemoveMoved",
        "Collect",
        "StartScan",
        "Visit",
        "FinishScan",
        "scan failed",
        "BeginCheckpoint",
        "WriteFile",
        "InstallManifest",
        "crash disk",
        "crash empty",
        "crash peer",
        "Resync",
        "finish Resync",
        "DropRange",
    ];
    for action in actions {
        assert!(
            total.get(action).copied().unwrap_or(0) >= 20,
            "{action} is too rare: {total:?}"
        );
    }
}

/// Every mutant of `specs/vector/index/mutants` and the property that its `expect:` line names.
const MUTANTS: [(&str, &str); 20] = [
    ("checkpoint_ahead_of_files", "NoLostRow"),
    ("checkpoint_forgets_low", "NoLostRow"),
    ("free_without_wait", "ExactScan"),
    ("gc_without_epoch", "NoLostRow"),
    ("late_copy_after_resync", "SingleGeneration"),
    ("manifest_before_sync", "NoLostRow"),
    ("no_slot_dedup", "NoDuplicate"),
    ("prefix_checkpoint_without_epoch", "NoLostRow"),
    ("prefix_current_liveness", "ExactScan"),
    ("prefix_drain_a_target", "DrainEnds"),
    ("prefix_drain_copies_current_only", "NoLostRow"),
    ("prefix_drain_ignores_unapplied", "NoLostRow"),
    ("prefix_land_needs_ready", "DrainEnds"),
    ("prefix_probe_before_pin", "ExactScan"),
    ("prefix_remove_by_slot", "NoLostRow"),
    ("prefix_scan_survives_resync", "ExactScan"),
    ("prefix_short_version", "NoStaleEntry"),
    ("remove_before_append", "NoLostRow"),
    ("remove_without_wait", "ExactScan"),
    ("show_superseded", "NoStaleEntry"),
];

/// Each design bug, put back, breaks the property that the spec says it breaks.
#[test]
fn vs07_every_mutant_of_the_spec_is_caught() {
    const SEEDS: u64 = 6_000;
    let mut survivors = Vec::new();
    for (name, property) in MUTANTS {
        MUTANT.with(|m| m.set(name));
        let mut others: BTreeMap<&str, u64> = BTreeMap::new();
        let killed = (0..SEEDS).find(|&seed| match run(&mut Gen::from_seed(seed), STEPS) {
            Err((p, _)) if p == property => true,
            Err((p, _)) => {
                *others.entry(p).or_default() += 1;
                false
            }
            Ok(_) => false,
        });
        match killed {
            Some(seed) => {
                println!("{name}: {property} fails at seed {seed}, others first: {others:?}")
            }
            None => survivors.push((name, others)),
        }
    }
    MUTANT.with(|m| m.set("none"));
    assert!(survivors.is_empty(), "not caught: {survivors:?}");
}
