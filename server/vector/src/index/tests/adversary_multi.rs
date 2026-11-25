//! Several ranges that share partitions, which `LocalIndex.tla` does not model: one range resyncs,
//! drops, drains and checkpoints while scans of other ranges, with filters and approximate plans, are
//! half way. Each finished scan is compared with an oracle that keeps every range's feed.

use std::collections::BTreeSet;
use std::sync::Arc;

use nabedra_check::Gen;
use nabedra_rt::fs::FaultFs;

use super::super::codec::exact;
use super::*;

const DIM: usize = 2;
const RANGES: u8 = 3;
const DIR: &str = "/ck";

#[derive(Clone, Debug)]
enum Op {
    Up { key: u8, vec: [f32; 2], tenant: i64, tags: u8 },
    Del { key: u8 },
}

impl Op {
    fn key(&self) -> u8 {
        match self {
            Op::Up { key, .. } | Op::Del { key } => *key,
        }
    }
}

#[derive(Clone)]
struct Feed {
    id: RangeId,
    log: Vec<Op>,
    held: bool,
    loading: bool,
}

fn key_of(range: u8, key: u8) -> Key {
    Arc::new([range, key])
}

fn cols(tenant: i64, tags: u8) -> Vec<Value> {
    let names: Vec<Arc<str>> = ["a", "b"]
        .iter()
        .enumerate()
        .filter(|(i, _)| tags >> i & 1 == 1)
        .map(|(_, n)| Arc::from(*n))
        .collect();
    vec![Value::Int(tenant), Value::Tags(names.into())]
}

impl Feed {
    fn ts(pos: Pos) -> CommitTs {
        pos * 10
    }

    fn row(&self, range: u8, pos: Pos) -> Option<Row> {
        match &self.log[pos as usize - 1] {
            Op::Up { vec, tenant, tags, .. } => Some(Row {
                vec: Some(vec.as_slice().into()),
                cols: cols(*tenant, *tags),
                image: Arc::new([range, pos as u8]),
            }),
            Op::Del { .. } => None,
        }
    }

    fn page(&self, range: u8, after: Pos, through: Pos) -> Page {
        let record = |at: Pos| Record {
            at,
            change: Change::Row {
                key: key_of(range, self.log[at as usize - 1].key()),
                commit_ts: Feed::ts(at),
                row: self.row(range, at),
            },
        };
        Page { after, through, records: (after + 1..=through).map(record).collect() }
    }

    /// The positions whose rows are visible at `pos`.
    fn live(&self, pos: Pos) -> Vec<Pos> {
        let last = |k: u8| (1..=pos).rev().find(|n| self.log[*n as usize - 1].key() == k);
        (0..4).filter_map(last).filter(|n| matches!(self.log[*n as usize - 1], Op::Up { .. })).collect()
    }
}

struct Reading {
    scan: Option<Scan>,
    /// Every visible row of the pin: (range, position).
    expect: BTreeSet<(u8, Pos)>,
    ranges: BTreeSet<u8>,
    replaced: bool,
    op: Op2,
    q: Vec<f32>,
    limit: usize,
    exact: bool,
    filter: Option<Filter>,
}

type Op2 = crate::types::Op;

struct World {
    ix: Index<ExactCodec>,
    fs: FaultFs,
    feeds: Vec<Feed>,
    reading: Vec<Reading>,
    by_tenant: bool,
    flat: usize,
    done: std::collections::BTreeMap<&'static str, u32>,
}

fn config(by_tenant: bool, flat: usize, shards: usize) -> Config {
    Config {
        queue: 4,
        max_flight: 4,
        flat,
        part_min: 2,
        part_max: 6,
        ..Config::new(schema(DIM, &[ColumnType::Int, ColumnType::Tags], by_tenant.then_some(0)), shards)
    }
}

fn vector(g: &mut Gen) -> [f32; 2] {
    let dir = *g.pick(&[[1.0f32, 0.0], [0.0, 1.0], [-1.0, 0.0], [0.6, 0.8], [-0.8, 0.6], [0.6, 0.8]]);
    let len = *g.pick(&[1.0f32, 1.0, 3.0, 0.1, 100.0, 0.0]);
    dir.map(|x| x * len)
}

fn filter(g: &mut Gen) -> Option<Filter> {
    let tag = |t: &str| Filter::Contains(1, Arc::from(t));
    Some(match g.below(9) {
        0 | 1 => return None,
        2 => Filter::Eq(0, Value::Int(g.below(3) as i64)),
        3 => tag("a"),
        4 => Filter::Not(Box::new(tag("b"))),
        5 => Filter::And(vec![Filter::Eq(0, Value::Int(g.below(3) as i64)), tag("a")]),
        6 => Filter::Or(vec![Filter::Eq(0, Value::Int(0)), tag("b")]),
        7 => Filter::ContainsAny(1, vec![Arc::from("a"), Arc::from("b")]),
        _ => Filter::Not(Box::new(Filter::Eq(0, Value::Int(1)))),
    })
}

type Broken = (&'static str, String);
type R<T> = std::result::Result<T, Broken>;

impl World {
    fn new(g: &mut Gen) -> World {
        let fs = FaultFs::new();
        let by_tenant = g.bool();
        let flat = *g.pick(&[0usize, 0, 20_000]);
        let shards = 1 + g.below(3) as usize;
        let ix = Index::new(config(by_tenant, flat, shards), ExactCodec, Some(Store::new(Arc::new(fs.clone()), DIR))).unwrap();
        let mut w = World { ix, fs, feeds: Vec::new(), reading: Vec::new(), by_tenant, flat, done: Default::default() };
        for id in 0..RANGES {
            w.feeds.push(Feed { id: RangeId(u128::from(id)), log: Vec::new(), held: true, loading: false });
            w.install_empty(id as usize);
        }
        w
    }

    fn install_empty(&mut self, f: usize) {
        let id = self.feeds[f].id;
        self.ix.begin_load(id, 0, 0).unwrap();
        self.ix.finish_load(id).unwrap();
        self.ix.caught_up(id, 0).unwrap();
    }

    fn status(&self, f: usize) -> RangeStatus {
        self.ix.status(self.feeds[f].id).unwrap()
    }

    fn did(&mut self, what: &'static str) -> R<()> {
        *self.done.entry(what).or_default() += 1;
        Ok(())
    }

    fn parts(&self, state: State) -> Vec<PartId> {
        let dir = self.ix.shared.dir.iter();
        dir.filter(|(_, m)| m.state == state && !m.undirected).map(|(p, _)| *p).collect()
    }

    fn act(&mut self, g: &mut Gen) -> R<()> {
        let f = g.below(u64::from(RANGES)) as usize;
        let shard = g.below(self.ix.shards.len() as u64) as usize;
        let ready = self.parts(State::Ready);
        let draining = self.parts(State::Draining);
        let all: Vec<PartId> = self.ix.shared.dir.keys().copied().collect();
        let any = |g: &mut Gen, v: &Vec<PartId>| (!v.is_empty()).then(|| *g.pick(v));
        match g.below(40) {
            0..=5 => self.new_op(g, f),
            6..=9 => self.deliver(g, f),
            10 => {
                self.ix.pump();
                Ok(())
            }
            11..=13 => {
                self.ix.step(shard);
                Ok(())
            }
            14 => {
                self.ix.advance();
                Ok(())
            }
            15 if self.feeds[f].held && !self.feeds[f].loading => {
                let low = self.status(f).low + u64::from(g.bool());
                let _ = self.ix.advance_low(self.feeds[f].id, low);
                Ok(())
            }
            16 => {
                if let Some(p) = any(g, &ready) {
                    self.ix.split(p);
                }
                Ok(())
            }
            17 => {
                if let (Some(p), Some(q)) = (any(g, &ready), any(g, &ready)) {
                    self.ix.merge(p, q);
                }
                Ok(())
            }
            18..=20 => {
                if let Some(p) = any(g, &draining) {
                    self.ix.send_copies(p, 1 + g.below(2) as usize);
                    if g.bool() {
                        self.ix.finish_drain(p);
                    }
                }
                Ok(())
            }
            21 => {
                if let Some(p) = any(g, &all) {
                    self.ix.maintain_part(p);
                }
                Ok(())
            }
            22 => {
                if let Some(p) = any(g, &all) {
                    self.ix.free_part(p);
                    self.ix.remove_moved(p);
                    self.ix.collect(p);
                }
                Ok(())
            }
            23 => {
                // A move to another partition of the same tree, if the code finds one.
                if let Some(p) = any(g, &ready) {
                    let owner = p.0 as usize % self.ix.shards.len();
                    let ids: Vec<u64> = self.ix.shards[owner].parts.get(&p).map(|x| x.entries().map(|e| e.id).collect()).unwrap_or_default();
                    if let (Some(id), Some(to)) = (ids.first().copied(), any(g, &ready)) {
                        self.ix.reassign(p, id, to);
                    }
                }
                Ok(())
            }
            24..=27 => self.start_scan(g),
            28..=31 => self.visit(g),
            32 => {
                let _ = self.ix.begin_checkpoint();
                Ok(())
            }
            33 | 34 if self.ix.checkpoint.is_some() => {
                let todo = !self.ix.checkpoint.as_ref().unwrap().is_written();
                let result = if todo { self.ix.write_file().map(|_| ()) } else { self.ix.install_manifest() };
                result.map_err(|e| ("Checkpoint", e.to_string()))
            }
            35 => self.crash(g),
            36 => self.resync(g, f),
            37 if self.feeds[f].loading => {
                let id = self.feeds[f].id;
                if self.ix.finish_load(id).is_ok() {
                    self.ix.caught_up(id, 0).unwrap();
                    self.feeds[f].loading = false;
                }
                Ok(())
            }
            38 => self.drop_range(f),
            _ => Ok(()),
        }
    }

    fn new_op(&mut self, g: &mut Gen, f: usize) -> R<()> {
        if self.feeds[f].log.len() >= 14 {
            return Ok(());
        }
        let key = g.below(4) as u8;
        let op = if g.below(5) == 0 {
            Op::Del { key }
        } else {
            Op::Up { key, vec: vector(g), tenant: g.below(3) as i64, tags: g.below(4) as u8 }
        };
        self.feeds[f].log.push(op);
        Ok(())
    }

    fn deliver(&mut self, g: &mut Gen, f: usize) -> R<()> {
        let feed = &self.feeds[f];
        if !feed.held || feed.loading {
            return Ok(());
        }
        let delivered = self.status(f).delivered;
        let end = feed.log.len() as Pos;
        let through = (delivered + 1 + g.below(3)).min(end);
        if through <= delivered {
            return Ok(());
        }
        match self.ix.apply(feed.id, &feed.page(f as u8, delivered, through)) {
            Ok(_) | Err(Error::Busy) => self.did("deliver"),
            Err(e) => Err(("Feed", e.to_string())),
        }
    }

    fn start_scan(&mut self, g: &mut Gen) -> R<()> {
        if self.reading.len() >= 3 {
            return Ok(());
        }
        let mut reads = Vec::new();
        let (mut expect, mut ranges) = (BTreeSet::new(), BTreeSet::new());
        for (f, feed) in self.feeds.iter().enumerate() {
            if !feed.held || feed.loading || g.below(3) == 0 {
                continue;
            }
            let s = self.status(f);
            let pos = if g.bool() { s.applied } else { s.low + g.below(s.applied - s.low + 1) };
            reads.push((feed.id, ReadAt::Position(pos)));
            ranges.insert(f as u8);
            expect.extend(feed.live(pos).into_iter().map(|n| (f as u8, n)));
        }
        if reads.is_empty() {
            return Ok(());
        }
        let filter = filter(g);
        let pin = self.ix.pin(&reads, filter.as_ref()).map_err(|e| ("Pin", format!("a pin was refused: {e}")))?;
        let op = *g.pick(&[Op2::Cosine, Op2::Dot, Op2::L2]);
        let mut q = vector(g).to_vec();
        if op == Op2::Cosine && q.iter().all(|x| *x == 0.0) {
            q = vec![1.0, 0.5];
        }
        let exact = g.bool();
        let limit = if exact { 100 } else { 1 + g.below(5) as usize };
        let query = Query { op, vec: q.clone(), limit, filter: filter.clone(), exact, recall: *g.pick(&[0.0, 0.9, 1.0]) };
        let scan = self.ix.scan(&pin, &query, Box::new(RecallEstimate)).map_err(|e| ("Scan", e.to_string()))?;
        self.reading.push(Reading { scan: Some(scan), expect, ranges, replaced: false, op, q, limit, exact, filter });
        self.did("StartScan")
    }

    fn visit(&mut self, g: &mut Gen) -> R<()> {
        if self.reading.is_empty() {
            return Ok(());
        }
        let i = g.below(self.reading.len() as u64) as usize;
        let Some(scan) = &mut self.reading[i].scan else { return Ok(()) };
        match self.ix.visit(scan) {
            Ok(true) => Ok(()),
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

    fn cols_of(&self, f: u8, pos: Pos) -> Vec<Value> {
        self.feeds[f as usize].row(f, pos).unwrap().cols
    }

    fn check_scan(&mut self, r: &Reading, result: Result<Vec<Hit>>) -> R<()> {
        let hits = match result {
            Ok(hits) => hits,
            Err(Error::PinFailed) if r.replaced => return self.did("scan failed"),
            Err(e) => return Err(("Scan", format!("a scan failed: {e}"))),
        };
        let at = |h: &Hit| (h.row.image[0], Pos::from(h.row.image[1]));
        let seen: Vec<(u8, Pos)> = hits.iter().map(at).collect();
        let set: BTreeSet<_> = seen.iter().copied().collect();
        if set.len() != seen.len() {
            return Err(("NoDuplicate", format!("a row twice: {seen:?}")));
        }
        let matching: BTreeSet<(u8, Pos)> = r
            .expect
            .iter()
            .filter(|(f, pos)| r.filter.as_ref().is_none_or(|flt| flt.matches(&self.cols_of(*f, *pos))))
            .copied()
            .collect();
        if let Some(x) = set.difference(&matching).next() {
            return Err(("NoStaleEntry", format!("{x:?} is not visible or does not match: {matching:?}")));
        }
        let want = if r.exact { matching.len() } else { matching.len().min(r.limit) };
        let rankable = matching
            .iter()
            .filter(|(f, p)| exact(r.op, match &self.feeds[*f as usize].log[*p as usize - 1] {
                Op::Up { vec, .. } => vec,
                _ => unreachable!(),
            }, &r.q).is_some())
            .count();
        let want = want.min(if r.exact { rankable } else { rankable.min(r.limit) });
        if hits.len() != want {
            return Err((
                "ExactScan",
                format!("{} rows of {} matching, {rankable} rankable, exact {}, limit {}: got {set:?} want {matching:?}", hits.len(), matching.len(), r.exact, r.limit),
            ));
        }
        for h in &hits {
            let (f, p) = at(h);
            let Op::Up { vec, .. } = &self.feeds[f as usize].log[p as usize - 1] else { unreachable!() };
            if exact(r.op, vec, &r.q) != Some(h.score) {
                return Err(("ExactScan", format!("the score of {:?} is {}", at(h), h.score)));
            }
        }
        for w in hits.windows(2) {
            let good = |h: &Hit| super::super::codec::goodness(r.op, h.score);
            if !(good(&w[0]) > good(&w[1]) || good(&w[0]) == good(&w[1]) && w[0].key < w[1].key) {
                return Err(("Order", format!("{seen:?}")));
            }
        }
        if r.exact {
            // The best `limit` rows of the exact scan are the whole answer, so every rankable row is in.
            let mut every: Vec<(f32, Key)> = matching
                .iter()
                .filter_map(|(f, p)| {
                    let Op::Up { vec, key, .. } = &self.feeds[*f as usize].log[*p as usize - 1] else { unreachable!() };
                    Some((exact(r.op, vec, &r.q)?, key_of(*f, *key)))
                })
                .collect();
            every.sort_by(|a, b| {
                super::super::codec::goodness(r.op, b.0).total_cmp(&super::super::codec::goodness(r.op, a.0)).then(a.1.cmp(&b.1))
            });
            let got: Vec<(f32, Key)> = hits.iter().map(|h| (h.score, h.key.clone())).collect();
            if got != every {
                return Err(("ExactScan", format!("{got:?} is not {every:?}")));
            }
        }
        self.did(if r.filter.is_some() { "FinishScan filtered" } else { "FinishScan" })
    }

    fn replaced(&mut self, f: usize) {
        for r in &mut self.reading {
            r.replaced |= r.ranges.contains(&(f as u8));
        }
    }

    fn resync(&mut self, g: &mut Gen, f: usize) -> R<()> {
        let feed = self.feeds[f].clone();
        if feed.loading {
            return Ok(());
        }
        let from = if feed.held { self.status(f).applied } else { 0 };
        let n = from + g.below(feed.log.len() as Pos - from + 1);
        let bound = if n > 0 { Feed::ts(n) } else { 0 };
        self.replaced(f);
        self.ix.begin_load(feed.id, n, bound).unwrap();
        let rows: Vec<_> = feed
            .live(n)
            .iter()
            .map(|&p| (key_of(f as u8, feed.log[p as usize - 1].key()), Feed::ts(p), feed.row(f as u8, p).unwrap()))
            .collect();
        self.ix.load_rows(feed.id, &rows).unwrap();
        self.feeds[f].held = true;
        self.feeds[f].loading = true;
        self.did("Resync")
    }

    fn drop_range(&mut self, f: usize) -> R<()> {
        if !self.feeds[f].held {
            return Ok(());
        }
        self.replaced(f);
        self.ix.drop_range(self.feeds[f].id).unwrap();
        self.feeds[f].held = false;
        self.feeds[f].loading = false;
        self.did("DropRange")
    }

    fn crash(&mut self, _g: &mut Gen) -> R<()> {
        self.reading.clear();
        let shards = self.ix.shards.len();
        let fs = self.fs.crash();
        let store = Store::new(Arc::new(fs.clone()), DIR);
        let (ix, damage) = Index::recover(config(self.by_tenant, self.flat, shards), ExactCodec, store).unwrap();
        if !damage.is_empty() {
            return Err(("Recover", format!("a synced checkpoint does not load: {damage:?}")));
        }
        (self.ix, self.fs) = (ix, fs);
        for f in 0..self.feeds.len() {
            let id = self.feeds[f].id;
            let there = self.ix.status(id).is_ok();
            let was = self.feeds[f].held && !self.feeds[f].loading;
            self.feeds[f].loading = false;
            match (there, self.feeds[f].held) {
                (true, true) if was => self.ix.caught_up(id, 0).unwrap(),
                (true, _) => return Err(("SingleGeneration", "a dropped range came back".into())),
                (false, true) => self.install_empty(f),
                (false, false) => {}
            }
        }
        self.did("crash")
    }

    fn check(&self) -> R<()> {
        let sh = &self.ix.shared;
        for (part, _) in sh.dir.iter() {
            let owner = part.0 as usize % self.ix.shards.len();
            let here = self.ix.shards[owner].parts.get(part).into_iter().flat_map(|p| p.entries().copied());
            if let Some(e) = here.into_iter().find(|e| !sh.gens.contains_key(&e.generation)) {
                return Err(("SingleGeneration", format!("{part:?} holds {e:?}")));
            }
        }
        let listed: BTreeSet<(u8, Pos)> = self
            .ix
            .listed()
            .into_iter()
            .flat_map(|p| {
                let owner = p.0 as usize % self.ix.shards.len();
                let es: Vec<_> = self.ix.shards[owner].parts.get(&p).into_iter().flat_map(|x| x.entries().copied()).collect();
                es
            })
            .filter_map(|e| {
                let row = sh.rows.version(e.slot, e.at)?.row.as_ref()?;
                Some((row.image[0], Pos::from(row.image[1])))
            })
            .collect();
        for (f, feed) in self.feeds.iter().enumerate().filter(|(_, f)| f.held && !f.loading) {
            let s = self.status(f);
            for pos in s.low..=s.applied {
                for n in feed.live(pos) {
                    if !listed.contains(&(f as u8, n)) {
                        return Err(("NoLostRow", format!("range {f}: {n} is not there for cut {pos}")));
                    }
                }
            }
        }
        Ok(())
    }
}

fn run(g: &mut Gen) -> R<std::collections::BTreeMap<&'static str, u32>> {
    let mut w = World::new(g);
    for _ in 0..400 {
        w.act(g)?;
        w.check()?;
    }
    // Everything settles, and then one more scan of every kind.
    for f in 0..w.feeds.len() {
        for _ in 0..10 {
            w.ix.run();
            let feed = w.feeds[f].clone();
            if feed.loading {
                let id = feed.id;
                if w.ix.finish_load(id).is_ok() {
                    w.ix.caught_up(id, 0).unwrap();
                    w.feeds[f].loading = false;
                }
            } else if feed.held {
                let page = feed.page(f as u8, w.status(f).delivered, feed.log.len() as Pos);
                match w.ix.apply(feed.id, &page) {
                    Ok(_) | Err(Error::Busy) => {}
                    Err(e) => return Err(("Feed", e.to_string())),
                }
            }
            for p in w.ix.shared.dir.keys().copied().collect::<Vec<_>>() {
                w.ix.maintain_part(p);
            }
            w.check()?;
        }
    }
    w.reading.clear();
    for _ in 0..4 {
        w.start_scan(g)?;
    }
    for _ in 0..1000 {
        if w.reading.is_empty() {
            break;
        }
        w.visit(g)?;
    }
    Ok(w.done)
}

/// Scans, drains, checkpoints, crashes, resyncs and drops of several ranges that share partitions,
/// in any order, never lose a row, show a stale one, return a row twice or break a filter.
#[test]
fn adv07_several_ranges_sharing_partitions_keep_every_scan_right() {
    let seeds: u64 = std::env::var("ADV_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1500);
    let mut total = std::collections::BTreeMap::<&'static str, u32>::new();
    for seed in 0..seeds {
        match run(&mut Gen::from_seed(seed)) {
            Ok(done) => {
                for (k, n) in done {
                    *total.entry(k).or_default() += n;
                }
            }
            Err((p, why)) => panic!("seed {seed}: {p}: {why}"),
        }
    }
    println!("{total:?}");
    for what in ["StartScan", "FinishScan", "FinishScan filtered", "scan failed", "Resync", "DropRange", "crash"] {
        assert!(total.get(what).copied().unwrap_or(0) >= 50, "{what} is too rare: {total:?}");
    }
}
