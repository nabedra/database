//! Local benchmark of one index (contract-w2 6.3), std and the crate only. It reads a set that
//! `scripts/bench/vector/convert.py` wrote and the ground truth of `gt.py` (float64 numpy, never
//! our own `exact`), builds the index through the feed (`apply`, `run`, `maintain`), and prints one
//! JSON line per measured point: recall@10, latency percentiles and QPS per operation, filter case
//! and stop setting, plus build rate, resident memory and checkpoint write and recover times.
//!
//! cargo run --release -p nabedra-vector --example bench_local -- --data DIR --gt DIR [options]
//!
//! Options (defaults in brackets): --rows N [all of the ground truth], --queries N [all],
//! --ops cosine,dot,l2 [all], --cases a,b [every line of cases.txt], --recalls 0.8,0.9,0.95,0.99,
//! --units 0 [0 = no fixed-unit points; else stop after N units with matches], --limits 10,
//! --exact (adds `exact: true` points), --threads 1,4,8, --qps-seconds 3, --page 1024,
//! --maintain 4 (maintenance calls per page), --settle-parts 16 (partitions per maintenance call
//! once fed), --shards 1, --vectors ram|uring|pread [ram],
//! --scratch DIR (NVMe vectors and checkpoint), --checkpoint (write, recover, check: timed),
//! --save-checkpoint DIR (the same, kept in DIR), --from-checkpoint DIR (recover instead of
//! building: the measured windows skip the slow build), --no-sweep, --meta FILE (one JSON object that
//! every line carries: git SHA, lscpu, kernel, governor), --out FILE [stdout], --label TEXT.
//!
//! Adapter note: it uses `Index`, `Config`, `apply`, `run`, `maintain`, `pin`, `scan`/`visit`/
//! `finish`, `checkpoint` and `recover` as they are at 267e55dc. A per-core index will change
//! `run`/`maintain` (build) and may move scans to a pool; keep the changes inside `build` and `scan_one`.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use nabedra_rt::fs::RealFs;
use nabedra_rt::time;
use nabedra_vector::index::{
    Config, DirectionCodec, Filter, Index, Pin, Progress, Query, ReadAt, RecallEstimate, StopRule,
    Store,
};
use nabedra_vector::store::{Backend, NvmeVectors};
use nabedra_vector::types::{
    Change, ColumnType, Error, Key, Op, Page, Pos, RangeId, Record, Row, Schema, Value,
};

const RANGE: RangeId = RangeId(1);
const K: usize = 10;

type Res<T> = std::result::Result<T, String>;

fn main() {
    if let Err(e) = run() {
        eprintln!("bench_local: {e}");
        std::process::exit(1);
    }
}

// ----- Arguments

#[derive(Clone)]
struct Args {
    data: PathBuf,
    gt: PathBuf,
    rows: Option<usize>,
    queries: Option<usize>,
    ops: Vec<Op>,
    cases: Option<Vec<String>>,
    recalls: Vec<f64>,
    units: Vec<usize>,
    limits: Vec<usize>,
    exact: bool,
    threads: Vec<usize>,
    qps_seconds: f64,
    page: usize,
    maintain: usize,
    settle_parts: usize,
    shards: usize,
    vectors: String,
    scratch: Option<PathBuf>,
    checkpoint: bool,
    save_checkpoint: Option<PathBuf>,
    from_checkpoint: Option<PathBuf>,
    sweep: bool,
    meta: String,
    out: Option<PathBuf>,
    label: String,
}

fn list<T: std::str::FromStr>(s: &str) -> Res<Vec<T>> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.parse().map_err(|_| format!("bad value {x}")))
        .collect()
}

fn op_of(s: &str) -> Res<Op> {
    match s {
        "cosine" => Ok(Op::Cosine),
        "dot" => Ok(Op::Dot),
        "l2" => Ok(Op::L2),
        _ => Err(format!("unknown op {s}")),
    }
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Cosine => "cosine",
        Op::Dot => "dot",
        Op::L2 => "l2",
    }
}

fn args() -> Res<Args> {
    let mut a = Args {
        data: PathBuf::new(),
        gt: PathBuf::new(),
        rows: None,
        queries: None,
        ops: vec![Op::Cosine, Op::Dot, Op::L2],
        cases: None,
        recalls: vec![0.8, 0.9, 0.95, 0.99],
        units: Vec::new(),
        limits: vec![K],
        exact: false,
        threads: vec![1, 4, 8],
        qps_seconds: 3.0,
        page: 1024,
        maintain: 4,
        settle_parts: 16,
        shards: 1,
        vectors: "ram".into(),
        scratch: None,
        checkpoint: false,
        save_checkpoint: None,
        from_checkpoint: None,
        sweep: true,
        meta: "{}".into(),
        out: None,
        label: String::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--data" => a.data = val()?.into(),
            "--gt" => a.gt = val()?.into(),
            "--rows" => a.rows = Some(val()?.parse().map_err(|_| "--rows")?),
            "--queries" => a.queries = Some(val()?.parse().map_err(|_| "--queries")?),
            "--ops" => a.ops = val()?.split(',').map(op_of).collect::<Res<_>>()?,
            "--cases" => a.cases = Some(val()?.split(',').map(String::from).collect()),
            "--recalls" => a.recalls = list(&val()?)?,
            "--units" => {
                a.units = list::<usize>(&val()?)?
                    .into_iter()
                    .filter(|u| *u > 0)
                    .collect()
            }
            "--limits" => a.limits = list(&val()?)?,
            "--exact" => a.exact = true,
            "--threads" => a.threads = list(&val()?)?,
            "--qps-seconds" => a.qps_seconds = val()?.parse().map_err(|_| "--qps-seconds")?,
            "--page" => a.page = val()?.parse().map_err(|_| "--page")?,
            "--maintain" => a.maintain = val()?.parse().map_err(|_| "--maintain")?,
            "--settle-parts" => a.settle_parts = val()?.parse().map_err(|_| "--settle-parts")?,
            "--shards" => a.shards = val()?.parse().map_err(|_| "--shards")?,
            "--vectors" => a.vectors = val()?,
            "--scratch" => a.scratch = Some(val()?.into()),
            "--checkpoint" => a.checkpoint = true,
            "--save-checkpoint" => {
                a.save_checkpoint = Some(val()?.into());
                a.checkpoint = true;
            }
            "--from-checkpoint" => a.from_checkpoint = Some(val()?.into()),
            "--no-sweep" => a.sweep = false,
            "--meta" => {
                let path = val()?;
                a.meta = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
                a.meta = a.meta.trim().to_owned();
            }
            "--out" => a.out = Some(val()?.into()),
            "--label" => a.label = val()?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if a.data.as_os_str().is_empty() || a.gt.as_os_str().is_empty() {
        return Err("--data and --gt are required".into());
    }
    if a.limits.iter().any(|l| *l < K) || a.page == 0 {
        return Err("--limits must be at least 10 and --page positive".into());
    }
    Ok(a)
}

// ----- Files (vecio.py)

struct Bin<T> {
    n: usize,
    d: usize,
    data: Vec<T>,
}

fn header(r: &mut impl Read) -> io::Result<(usize, usize)> {
    let mut h = [0u8; 8];
    r.read_exact(&mut h)?;
    let n = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
    let d = u32::from_le_bytes([h[4], h[5], h[6], h[7]]) as usize;
    Ok((n, d))
}

fn read_bin<T>(path: &Path, rows: Option<usize>, conv: fn([u8; 4]) -> T) -> Res<Bin<T>> {
    let err = |e: io::Error| format!("{}: {e}", path.display());
    let mut r = BufReader::new(File::open(path).map_err(err)?);
    let (n, d) = header(&mut r).map_err(err)?;
    let n = rows.map_or(n, |m| m.min(n));
    let count = n.checked_mul(d).ok_or("file too large")?;
    let mut bytes = vec![0u8; count.checked_mul(4).ok_or("file too large")?];
    r.read_exact(&mut bytes).map_err(err)?;
    let data = bytes
        .chunks_exact(4)
        .map(|c| conv([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(Bin { n, d, data })
}

fn floats(path: &Path, rows: Option<usize>) -> Res<Bin<f32>> {
    read_bin(path, rows, f32::from_le_bytes)
}

fn ints(path: &Path, rows: Option<usize>) -> Res<Bin<u32>> {
    read_bin(path, rows, u32::from_le_bytes)
}

/// Streams base vectors a page at a time: the whole file never sits in memory beside the index.
struct Base {
    r: BufReader<File>,
    d: usize,
    left: usize,
}

impl Base {
    fn open(path: &Path, rows: usize) -> Res<Base> {
        let err = |e: io::Error| format!("{}: {e}", path.display());
        let mut r = BufReader::with_capacity(1 << 20, File::open(path).map_err(err)?);
        let (n, d) = header(&mut r).map_err(err)?;
        if n < rows {
            return Err(format!("{} has {n} rows, {rows} asked", path.display()));
        }
        Ok(Base { r, d, left: rows })
    }

    fn next(&mut self) -> Res<Option<Arc<[f32]>>> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let mut bytes = vec![0u8; self.d * 4];
        self.r.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        let v: Vec<f32> = bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        Ok(Some(v.into()))
    }
}

// ----- Schema and filter cases (gt.py `cases_of`)

/// Columns: u (int), tags, id (int, the row's position), label (int, 0 without labels).
const U: usize = 0;
const TAGS: usize = 1;
const ID: usize = 2;
const LABEL: usize = 3;

fn schema(dim: usize) -> Schema {
    Schema {
        dim,
        columns: vec![
            ColumnType::Int,
            ColumnType::Tags,
            ColumnType::Int,
            ColumnType::Int,
        ],
        partition_by: None,
    }
}

fn tag_sets() -> [Value; 4] {
    let (a, b): (Arc<str>, Arc<str>) = ("a".into(), "b".into());
    [
        Value::Tags(Arc::new([])),
        Value::Tags(Arc::new([a.clone()])),
        Value::Tags(Arc::new([b.clone()])),
        Value::Tags(Arc::new([a, b])),
    ]
}

/// `all`, `u<X`, `ab`, `id>=X`, `label=C`.
fn filter_of(expr: &str) -> Res<Option<Filter>> {
    let int = |s: &str| s.parse::<i64>().map_err(|_| format!("bad case {expr}"));
    Ok(match expr {
        "all" => None,
        "ab" => Some(Filter::And(vec![
            Filter::Contains(TAGS, "a".into()),
            Filter::Contains(TAGS, "b".into()),
        ])),
        _ if expr.starts_with("u<") => Some(Filter::Range(
            U,
            Bound::Unbounded,
            Bound::Excluded(Value::Int(int(&expr[2..])?)),
        )),
        _ if expr.starts_with("id>=") => Some(Filter::Range(
            ID,
            Bound::Included(Value::Int(int(&expr[4..])?)),
            Bound::Unbounded,
        )),
        _ if expr.starts_with("label=") => Some(Filter::Eq(LABEL, Value::Int(int(&expr[6..])?))),
        _ => return Err(format!("unknown case {expr}")),
    })
}

fn key(pos: usize) -> Key {
    Arc::new((pos as u64).to_be_bytes())
}

fn position(key: &Key) -> Option<usize> {
    Some(u64::from_be_bytes((**key).try_into().ok()?) as usize)
}

// ----- Output

struct Out {
    w: Box<dyn Write>,
    common: String,
    /// The machine and code (meta.sh), last on every line so that the measures read first.
    meta: String,
}

impl Out {
    fn line(&mut self, kind: &str, fields: &str) -> Res<()> {
        let (common, meta) = (&self.common, &self.meta);
        writeln!(
            self.w,
            "{{\"kind\":\"{kind}\",{common},{fields},\"meta\":{meta}}}"
        )
        .and_then(|_| self.w.flush())
        .map_err(|e| e.to_string())
    }
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Resident and peak memory of this process, in bytes (Linux).
fn memory() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map_or(0, |kb| kb * 1024)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_bytes(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

// ----- Build through the feed

type Ix = Index<DirectionCodec>;

fn config(a: &Args, dim: usize) -> Res<Config> {
    let mut cfg = Config::new(schema(dim), a.shards);
    let backend = match a.vectors.as_str() {
        "ram" => None,
        "uring" => Some(Backend::Uring),
        "pread" => Some(Backend::Pread),
        v => return Err(format!("unknown --vectors {v}")),
    };
    if let Some(backend) = backend {
        let dir = a
            .scratch
            .as_ref()
            .ok_or("--vectors needs --scratch")?
            .join("vectors");
        let store =
            NvmeVectors::open(&dir, a.shards, dim, backend, 4).map_err(|e| e.to_string())?;
        cfg.vectors = Some(Arc::new(store));
    }
    Ok(cfg)
}

/// Feeds every row as one position, a page at a time, with maintenance between pages.
fn build(
    a: &Args,
    ix: &mut Ix,
    base: &mut Base,
    cols: &Bin<u32>,
    labels: Option<&Bin<u32>>,
    out: &mut Out,
) -> Res<Pos> {
    let e = |e: Error| e.to_string();
    ix.begin_load(RANGE, 0, 0).map_err(e)?;
    ix.finish_load(RANGE).map_err(e)?;
    ix.caught_up(RANGE, 0).map_err(e)?;
    let tags = tag_sets();
    let rows = base.left;
    let started = time::now_std();
    let (mut at, mut maintenance, mut maint_time) = (0usize, 0u64, Duration::ZERO);
    while at < rows {
        let mut records = Vec::with_capacity(a.page);
        while records.len() < a.page {
            let Some(vec) = base.next()? else { break };
            let pos = at + records.len();
            let label = labels.map_or(0, |l| l.data[pos]);
            let row = Row {
                vec: Some(vec),
                cols: vec![
                    Value::Int(i64::from(cols.data[pos * 2])),
                    tags[(cols.data[pos * 2 + 1] & 3) as usize].clone(),
                    Value::Int(pos as i64),
                    Value::Int(i64::from(label)),
                ],
                image: key(pos),
            };
            let seq = (pos + 1) as Pos;
            records.push(Record {
                at: seq,
                change: Change::Row {
                    key: key(pos),
                    commit_ts: seq,
                    row: Some(row),
                },
            });
        }
        let through = (at + records.len()) as Pos;
        let mut page = Page {
            after: at as Pos,
            through,
            records,
        };
        while page.after < page.through {
            match ix.apply(RANGE, &page) {
                Ok(taken) => page.after = taken,
                Err(Error::Busy) => {}
                Err(err) => return Err(format!("apply: {err}")),
            }
            page.records.retain(|r| r.at > page.after);
            ix.run();
        }
        at = through as usize;
        let t = time::now_std();
        for _ in 0..a.maintain {
            maintenance += 1;
            if !ix.maintain(64) {
                break;
            }
            ix.run();
        }
        maint_time += time::elapsed(t);
    }
    let fed = time::elapsed(started);
    // Settle: maintenance until enough calls in a row to visit every partition find nothing to do.
    let t = time::now_std();
    let (mut idle, mut settle_calls) = (0, 0u64);
    let round = |ix: &Ix| {
        let s = ix.stats();
        3 + (s.ready + s.draining + s.retired) / a.settle_parts.max(1)
    };
    // `stats` walks every row, so it is asked only when the round may be over.
    let mut need = round(ix);
    while (idle < need || {
        need = round(ix);
        idle < need
    }) && settle_calls < 10_000_000
    {
        settle_calls += 1;
        idle = if ix.maintain(a.settle_parts) {
            0
        } else {
            idle + 1
        };
        ix.run();
    }
    let settle = time::elapsed(t);
    let status = ix.status(RANGE).map_err(e)?;
    let (rss, peak) = memory();
    let s = ix.stats();
    let stats = format!(
        "\"partitions\":[{},{},{}],\"entries\":{},\"code_bytes\":{},\"ext_ram_bytes\":{},\
         \"ext_nvme_bytes\":{},\"raw_ram_bytes\":{},\"raw_nvme_bytes\":{},\"column_bytes\":{}",
        s.ready,
        s.draining,
        s.retired,
        s.entries,
        s.code_bytes,
        s.ext_ram_bytes,
        s.ext_nvme_bytes,
        s.raw_ram_bytes,
        s.raw_nvme_bytes,
        s.column_bytes,
    );
    let secs = fed.as_secs_f64();
    let total = secs + settle.as_secs_f64();
    out.line(
        "build",
        &format!(
            "\"page\":{},\"maintain_per_page\":{},\"settle_parts\":{},\"shards\":{},\"vectors\":{},\
             \"feed_seconds\":{secs:.3},\"rows_per_second\":{:.0},\"maintain_seconds\":{:.3},\
             \"maintain_calls\":{maintenance},\"settle_seconds\":{:.3},\"settle_calls\":{settle_calls},\
             \"rows_per_second_with_settle\":{:.0},\"applied\":{},\"rss_bytes\":{rss},\
             \"peak_rss_bytes\":{peak},{stats}",
            a.page,
            a.maintain,
            a.settle_parts,
            a.shards,
            json_str(&a.vectors),
            rows as f64 / secs,
            maint_time.as_secs_f64(),
            settle.as_secs_f64(),
            rows as f64 / total,
            status.applied,
        ),
    )?;
    Ok(status.applied)
}

// ----- Queries

/// Stops after a fixed number of units that held a match: the candidate knob, like `nprobe`.
struct Units(usize);

impl StopRule for Units {
    fn enough(&mut self, p: &Progress<'_>) -> bool {
        p.units >= self.0
    }
}

#[derive(Clone, Copy)]
enum Setting {
    Exact,
    Estimate(f64),
    Units(usize),
}

impl Setting {
    fn json(self) -> String {
        match self {
            Setting::Exact => "\"stop\":\"exact\"".into(),
            Setting::Estimate(r) => format!("\"stop\":\"estimate\",\"target\":{r}"),
            Setting::Units(n) => format!("\"stop\":\"units\",\"units\":{n}"),
        }
    }

    fn query(
        self,
        op: Op,
        vec: &[f32],
        limit: usize,
        filter: &Option<Filter>,
    ) -> (Query, Box<dyn StopRule>) {
        let (exact, recall, stop): (bool, f64, Box<dyn StopRule>) = match self {
            Setting::Exact => (true, 1.0, Box::new(RecallEstimate)),
            Setting::Estimate(r) => (false, r, Box::new(RecallEstimate)),
            Setting::Units(n) => (false, 1.0, Box::new(Units(n))),
        };
        let q = Query {
            op,
            vec: vec.to_vec(),
            limit,
            filter: filter.clone(),
            exact,
            recall,
        };
        (q, stop)
    }
}

struct Truth {
    ids: Bin<u32>,
    scores: Bin<f32>,
}

/// Returns the hits (position, score) and the units visited.
fn scan_one(
    ix: &Ix,
    pin: &Pin,
    q: Query,
    stop: Box<dyn StopRule>,
) -> Res<(Vec<(usize, f32)>, usize)> {
    let mut scan = ix.scan(pin, &q, stop).map_err(|e| e.to_string())?;
    while ix.visit(&mut scan).map_err(|e| e.to_string())? {}
    let visited = scan.visited;
    let hits = ix.finish(scan).map_err(|e| e.to_string())?;
    let hits = hits
        .iter()
        .map(|h| Ok((position(&h.key).ok_or("bad key")?, h.score)))
        .collect::<Res<Vec<_>>>()?;
    Ok((hits, visited))
}

/// Hits among the first 10 that the reference holds, counting a score tied with its 10th as found.
fn found(op: Op, hits: &[(usize, f32)], truth: &Truth, q: usize) -> (usize, usize, usize) {
    let k = truth.ids.d;
    let want = K.min(k);
    let ids: HashSet<u32> = truth.ids.data[q * k..q * k + want]
        .iter()
        .copied()
        .collect();
    let kth = truth.scores.data[q * k + want - 1];
    let good = |s: f32| {
        if op == Op::L2 {
            -f64::from(s)
        } else {
            f64::from(s)
        }
    };
    let tol = 1e-6 * f64::from(kth).abs().max(1.0);
    let top = &hits[..hits.len().min(K)];
    let strict = top
        .iter()
        .filter(|(p, _)| ids.contains(&(*p as u32)))
        .count();
    let tied = top
        .iter()
        .filter(|(p, s)| ids.contains(&(*p as u32)) || good(*s) >= good(kth) - tol)
        .count();
    (tied.min(want), strict, want)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

struct Point<'a> {
    op: Op,
    case: &'a str,
    filter: &'a Option<Filter>,
    setting: Setting,
    limit: usize,
}

fn measure(
    a: &Args,
    ix: &Ix,
    pin: &Pin,
    queries: &Bin<f32>,
    truth: &Truth,
    p: &Point,
    out: &mut Out,
) -> Res<()> {
    let d = queries.d;
    let vec = |i: usize| &queries.data[i * d..(i + 1) * d];
    // One thread: recall and latency.
    let (mut lat, mut tied, mut strict, mut want, mut visited, mut worst) =
        (Vec::new(), 0, 0, 0, 0, 1.0f64);
    for i in 0..queries.n {
        let (q, stop) = p.setting.query(p.op, vec(i), p.limit, p.filter);
        let t = time::now_std();
        let (hits, v) = scan_one(ix, pin, q, stop)?;
        lat.push(time::elapsed(t).as_secs_f64() * 1e3);
        let (f, s, w) = found(p.op, &hits, truth, i);
        (tied, strict, want, visited) = (tied + f, strict + s, want + w, visited + v);
        worst = worst.min(f as f64 / w.max(1) as f64);
    }
    lat.sort_by(f64::total_cmp);
    let mean = lat.iter().sum::<f64>() / lat.len().max(1) as f64;
    // QPS per thread count, over the same queries, for a fixed time.
    let mut qps = String::new();
    for &threads in &a.threads {
        let done = AtomicU64::new(0);
        let stop = AtomicBool::new(false);
        let failed = AtomicBool::new(false);
        let t = time::now_std();
        std::thread::scope(|s| {
            for th in 0..threads {
                let (done, stop, failed) = (&done, &stop, &failed);
                s.spawn(move || {
                    let mut i = th * queries.n / threads.max(1);
                    while !stop.load(Ordering::Relaxed) {
                        let (q, rule) =
                            p.setting.query(p.op, vec(i % queries.n), p.limit, p.filter);
                        if scan_one(ix, pin, q, rule).is_err() {
                            failed.store(true, Ordering::Relaxed);
                            return;
                        }
                        done.fetch_add(1, Ordering::Relaxed);
                        i += 1;
                    }
                });
            }
            time::thread_sleep(Duration::from_secs_f64(a.qps_seconds));
            stop.store(true, Ordering::Relaxed);
        });
        if failed.load(Ordering::Relaxed) {
            return Err("a query failed in the QPS loop".into());
        }
        let rate = done.load(Ordering::Relaxed) as f64 / time::elapsed(t).as_secs_f64();
        let _ = write!(
            qps,
            "{}\"{threads}\":{rate:.1}",
            if qps.is_empty() { "" } else { "," }
        );
    }
    out.line(
        "point",
        &format!(
            "\"op\":\"{}\",\"case\":{},{},\"limit\":{},\"queries\":{},\"recall\":{:.4},\
             \"recall_ids\":{:.4},\"worst_query_recall\":{:.2},\"mean_units\":{:.1},\
             \"mean_ms\":{mean:.3},\"p50_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},\"qps\":{{{qps}}}",
            op_name(p.op),
            json_str(p.case),
            p.setting.json(),
            p.limit,
            queries.n,
            tied as f64 / want.max(1) as f64,
            strict as f64 / want.max(1) as f64,
            worst,
            visited as f64 / queries.n.max(1) as f64,
            percentile(&lat, 50.0),
            percentile(&lat, 95.0),
            percentile(&lat, 99.0),
        ),
    )
}

fn sweep(
    a: &Args,
    ix: &mut Ix,
    queries: &Bin<f32>,
    cases: &[(String, String)],
    out: &mut Out,
) -> Res<()> {
    let pin = ix
        .pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None)
        .map_err(|e| e.to_string())?;
    let mut settings: Vec<Setting> = a.recalls.iter().map(|r| Setting::Estimate(*r)).collect();
    settings.extend(a.units.iter().map(|n| Setting::Units(*n)));
    if a.exact {
        settings.push(Setting::Exact);
    }
    for &op in &a.ops {
        for (name, expr) in cases {
            let filter = filter_of(expr)?;
            let file = |ext: &str| a.gt.join(format!("{}.{name}.{ext}", op_name(op)));
            let truth = Truth {
                ids: ints(&file("ibin"), Some(queries.n))?,
                scores: floats(&file("scores.fbin"), Some(queries.n))?,
            };
            if truth.ids.n < queries.n || truth.ids.d == 0 {
                return Err(format!("{}: too few queries", file("ibin").display()));
            }
            for &setting in &settings {
                for &limit in &a.limits {
                    let p = Point {
                        op,
                        case: name,
                        filter: &filter,
                        setting,
                        limit,
                    };
                    measure(a, ix, &pin, queries, &truth, &p, out)?;
                }
            }
        }
    }
    Ok(())
}

// ----- Checkpoint

/// Where `--checkpoint` writes: the kept directory, or a scratch one that is removed after.
fn checkpoint_dir(a: &Args) -> Res<PathBuf> {
    match &a.save_checkpoint {
        Some(dir) => Ok(dir.clone()),
        None => Ok(a
            .scratch
            .as_ref()
            .ok_or("--checkpoint needs --scratch")?
            .join("checkpoint")),
    }
}

/// Recovers a saved index instead of building one.
fn recover(cfg: Config, codec: DirectionCodec, dir: &Path, out: &mut Out) -> Res<Ix> {
    let e = |e: Error| e.to_string();
    let t = time::now_std();
    let (mut ix, damage) = Ix::recover(cfg, codec, Store::new(Arc::new(RealFs), dir)).map_err(e)?;
    let seconds = time::elapsed(t).as_secs_f64();
    if !damage.is_empty() {
        return Err(format!("{}: damaged checkpoint {damage:?}", dir.display()));
    }
    let applied = ix.status(RANGE).map_err(e)?.applied;
    ix.caught_up(RANGE, applied).map_err(e)?;
    let (rss, peak) = memory();
    out.line(
        "recover",
        &format!(
            "\"from\":{},\"recover_seconds\":{seconds:.3},\"bytes\":{},\"applied\":{applied},\
             \"rss_bytes\":{rss},\"peak_rss_bytes\":{peak}",
            json_str(&dir.display().to_string()),
            dir_bytes(dir),
        ),
    )?;
    out.common.push_str(",\"from_checkpoint\":true");
    Ok(ix)
}

fn checkpoint(
    a: &Args,
    mut ix: Ix,
    cfg: Config,
    codec: DirectionCodec,
    queries: &Bin<f32>,
    cases: &[(String, String)],
    out: &mut Out,
) -> Res<()> {
    let dir = checkpoint_dir(a)?;
    let e = |e: Error| e.to_string();
    let applied = ix.status(RANGE).map_err(e)?.applied;
    let t = time::now_std();
    ix.checkpoint().map_err(e)?;
    let write = time::elapsed(t).as_secs_f64();
    let bytes = dir_bytes(&dir);
    drop(ix);
    let (rss_dropped, _) = memory();
    let t = time::now_std();
    let store = Store::new(Arc::new(RealFs), dir.clone());
    let (mut back, damage) = Ix::recover(cfg, codec, store).map_err(e)?;
    let recover = time::elapsed(t).as_secs_f64();
    back.caught_up(RANGE, applied).map_err(e)?;
    let (rss, peak) = memory();
    out.line(
        "checkpoint",
        &format!(
            "\"write_seconds\":{write:.3},\"bytes\":{bytes},\"recover_seconds\":{recover:.3},\
             \"damage\":{},\"rss_after_drop_bytes\":{rss_dropped},\"rss_recovered_bytes\":{rss},\
             \"peak_rss_bytes\":{peak}",
            damage.len()
        ),
    )?;
    // The recovered index answers like the built one: one point per op, unfiltered.
    let all: Vec<(String, String)> = cases.iter().filter(|(_, x)| x == "all").cloned().collect();
    let after = Args {
        recalls: vec![0.95],
        units: Vec::new(),
        limits: vec![K],
        exact: false,
        threads: vec![1],
        ..a.clone()
    };
    out.common.push_str(",\"recovered\":true");
    if a.sweep {
        sweep(&after, &mut back, queries, &all, out)?;
    }
    if a.save_checkpoint.is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    Ok(())
}

// ----- Main

fn run() -> Res<()> {
    let a = args()?;
    // The ground truth names its base prefix and its queries: gt-<rows>-<queries>.
    let name = a.gt.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let part = |i: usize| name.split('-').nth(i)?.parse::<usize>().ok();
    let (Some(gt_rows), Some(gt_queries)) = (part(1), part(2)) else {
        return Err("--gt must be a gt-<rows>-<queries> directory".into());
    };
    let wanted = a.queries.unwrap_or(gt_queries).min(gt_queries);
    let queries = floats(&a.data.join("query.fbin"), Some(wanted))?;
    let cases_txt =
        std::fs::read_to_string(a.gt.join("cases.txt")).map_err(|e| format!("cases.txt: {e}"))?;
    let mut cases: Vec<(String, String)> = cases_txt
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(n, x)| (n.to_owned(), x.trim().to_owned()))
        .collect();
    if let Some(want) = &a.cases {
        cases.retain(|(n, _)| want.contains(n));
    }
    let rows = a.rows.unwrap_or(gt_rows);
    if rows != gt_rows {
        return Err(format!(
            "--rows {rows} but the ground truth is of {gt_rows} rows"
        ));
    }
    let cols = ints(&a.data.join("cols.ibin"), Some(rows))?;
    let labels_path = a.data.join("labels.ibin");
    let labels = match labels_path.exists() {
        true => Some(ints(&labels_path, Some(rows))?),
        false => None,
    };
    let mut base = Base::open(&a.data.join("base.fbin"), rows)?;
    let dim = base.d;
    let w: Box<dyn Write> = match &a.out {
        Some(p) => Box::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .map_err(|e| e.to_string())?,
        ),
        None => Box::new(io::stdout()),
    };
    let set = a
        .data
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("?")
        .to_owned();
    let common = format!(
        "\"label\":{},\"set\":{},\"rows\":{rows},\"dim\":{dim}",
        json_str(&a.label),
        json_str(&set),
    );
    let meta = if a.meta.starts_with('{') {
        a.meta.clone()
    } else {
        "{}".into()
    };
    let mut out = Out { w, common, meta };
    let cfg = config(&a, dim)?;
    let codec = DirectionCodec::new(dim, 0x5eed).map_err(|e| e.to_string())?;
    if let Some(dir) = &a.from_checkpoint {
        drop((cols, labels, base));
        let mut ix = recover(cfg, codec, dir, &mut out)?;
        if a.sweep {
            sweep(&a, &mut ix, &queries, &cases, &mut out)?;
        }
        return Ok(());
    }
    let store = match a.checkpoint {
        true => {
            let dir = checkpoint_dir(&a)?;
            let _ = std::fs::remove_dir_all(&dir);
            Some(Store::new(Arc::new(RealFs), dir))
        }
        false => None,
    };
    let mut ix = Ix::new(cfg.clone(), codec.clone(), store).map_err(|e| e.to_string())?;
    build(&a, &mut ix, &mut base, &cols, labels.as_ref(), &mut out)?;
    drop((cols, labels));
    if a.sweep {
        sweep(&a, &mut ix, &queries, &cases, &mut out)?;
    }
    if a.checkpoint {
        checkpoint(&a, ix, cfg, codec, &queries, &cases, &mut out)?;
    }
    Ok(())
}
