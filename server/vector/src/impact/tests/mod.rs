mod adv_ckpt;
mod adv_fuse;
mod adv_rank;
mod adv_state;
mod crash;
mod diff;
mod fuse;
mod reference;
mod unit;

use std::collections::BTreeMap;
use std::sync::Arc;

use super::rows::Point;
use super::search::{B, K1};
use super::*;
use crate::index::Filter;
use crate::types::{ColumnType, CommitTs, Error, Key, MAX_LIMIT, Pos, RangeId, Value};

const RANGE: RangeId = RangeId(7);

fn key(n: u64) -> Key {
    Arc::new(n.to_be_bytes())
}

fn tags(t: &[&str]) -> Value {
    Value::Tags(t.iter().map(|s| Arc::from(*s)).collect())
}

fn text(t: &str, cols: Vec<Value>, image: u64) -> Row {
    Row {
        doc: Some(Doc::Text(t.into())),
        cols,
        image: Arc::new(image.to_be_bytes()),
    }
}

fn sparse(pairs: &[(u32, f32)], cols: Vec<Value>, image: u64) -> Row {
    Row {
        doc: Some(Doc::Sparse {
            indices: pairs.iter().map(|p| p.0).collect(),
            weights: pairs.iter().map(|p| p.1).collect(),
        }),
        cols,
        image: Arc::new(image.to_be_bytes()),
    }
}

fn text_cfg() -> Config {
    Config::new(Kind::Text("english".into()), vec![ColumnType::Tags])
}

fn sparse_cfg() -> Config {
    Config::new(Kind::Sparse, vec![ColumnType::Tags])
}

/// An index that serves `RANGE`, empty.
fn serving(cfg: Config) -> ImpactIndex {
    let mut ix = ImpactIndex::new(cfg, RamStore::default(), None).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    ix
}

/// Maintenance until a sweep over every segment finds nothing to do.
fn settle(ix: &mut ImpactIndex) {
    let mut idle = 0;
    while idle < 2 * ix.segments().0 + 2 {
        idle = if ix.maintain().unwrap() { 0 } else { idle + 1 };
    }
}

fn latest(ix: &mut ImpactIndex) -> Pin {
    ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).unwrap()
}

fn match_query(t: &str, limit: usize) -> Query {
    Query {
        op: Op::Match,
        probe: Probe::Text(t.into()),
        limit,
        filter: None,
        exact: false,
        stats: None,
    }
}

fn sparse_query(op: Op, pairs: &[(u32, f32)], limit: usize) -> Query {
    Query {
        op,
        probe: Probe::Sparse {
            indices: pairs.iter().map(|p| p.0).collect(),
            weights: pairs.iter().map(|p| p.1).collect(),
        },
        limit,
        filter: None,
        exact: false,
        stats: None,
    }
}

fn images(hits: &[Hit]) -> Vec<u64> {
    let image = |h: &Hit| u64::from_be_bytes((&*h.image).try_into().unwrap());
    hits.iter().map(image).collect()
}

/// The feed of one range as it happened, and what brute force makes of it.
#[derive(Clone, Default)]
struct Model {
    /// (at, commit ts, key, row).
    ops: Vec<(Pos, CommitTs, u64, Option<Row>)>,
    closed: Vec<(Pos, CommitTs)>,
    next: Pos,
}

impl Model {
    /// A page that writes or deletes keys, one position each, or several at one position.
    fn page(&mut self, changes: Vec<(u64, Option<Row>)>, same_position: bool) -> Page {
        let after = self.next;
        let mut records = Vec::new();
        for (i, (k, row)) in changes.into_iter().enumerate() {
            if i == 0 || !same_position {
                self.next += 1;
            }
            let at = self.next;
            // Several changes at one position commit at distinct timestamps.
            let ts = at * 10 + i as u64 % 10;
            self.ops.push((at, ts, k, row.clone()));
            let change = Change::Row {
                key: key(k),
                commit_ts: ts,
                row,
            };
            records.push(Record { at, change });
        }
        Page {
            after,
            through: self.next,
            records,
        }
    }

    fn close(&mut self) -> Page {
        let after = self.next;
        self.next += 1;
        let ts = self.ops.iter().map(|o| o.1).max().unwrap_or(0);
        self.closed.push((self.next, ts));
        Page {
            after,
            through: self.next,
            records: vec![Record {
                at: self.next,
                change: Change::Closed(ts),
            }],
        }
    }

    /// The newest version of every key at a point that has a document.
    fn visible(&self, p: Point) -> BTreeMap<u64, Row> {
        let mut newest: BTreeMap<u64, &Option<Row>> = BTreeMap::new();
        for (at, ts, k, row) in &self.ops {
            if *at <= p.pos && *ts <= p.cut {
                newest.insert(*k, row);
            }
        }
        let with_doc =
            |(k, r): (u64, &Option<Row>)| Some((k, r.clone().filter(|r| r.doc.is_some())?));
        newest.into_iter().filter_map(with_doc).collect()
    }

    fn now(&self) -> Point {
        Point {
            pos: self.next,
            cut: CommitTs::MAX,
        }
    }
}

/// BM25 counts of some rows for the terms of a query.
fn stats_of(a: &crate::text::Analyzer, rows: &BTreeMap<u64, Row>, query: &str) -> Stats {
    let terms = a.terms(query).terms;
    let mut s = Stats {
        docs: 0,
        words: 0,
        df: vec![0; terms.len()],
    };
    for row in rows.values() {
        let Some(Doc::Text(t)) = &row.doc else {
            continue;
        };
        let doc = a.terms(t);
        s.docs += 1;
        s.words += doc.words;
        for (n, (name, _)) in s.df.iter_mut().zip(&terms) {
            *n += u64::from(doc.terms.iter().any(|(d, _)| d == name));
        }
    }
    s
}

fn round(x: f64) -> f32 {
    (x as f32).clamp(f32::MIN, f32::MAX) + 0.0
}

/// What brute force ranks: every row that shares a term with the query and passes the filter, by
/// the score, then the key.
fn brute(
    kind: &Kind,
    rows: &BTreeMap<u64, Row>,
    q: &Query,
    stats: Option<&Stats>,
) -> Vec<(u64, f32)> {
    let mut scored: Vec<(f32, u64)> = Vec::new();
    let analyzer = match kind {
        Kind::Text(l) => Some(crate::text::Analyzer::new(l).unwrap()),
        Kind::Sparse => None,
    };
    let own = match (&analyzer, &q.probe, stats) {
        (Some(a), Probe::Text(query), None) => Some(stats_of(a, rows, query)),
        _ => None,
    };
    for (k, row) in rows {
        if q.filter.as_ref().is_some_and(|f| !f.matches(&row.cols)) {
            continue;
        }
        let score = match (&analyzer, &q.probe, &row.doc) {
            (Some(a), Probe::Text(query), Some(Doc::Text(t))) => {
                let Some(s) = stats.or(own.as_ref()) else {
                    continue;
                };
                let doc = a.terms(t);
                let n = s.docs as f64;
                let avg = if s.docs == 0 {
                    1.0
                } else {
                    (s.words as f64 / n).max(f64::MIN_POSITIVE)
                };
                let mut parts = Vec::new();
                for ((name, qtf), df) in a.terms(query).terms.iter().zip(&s.df) {
                    let Some((_, tf)) = doc.terms.iter().find(|(d, _)| d == name) else {
                        continue;
                    };
                    let df = *df as f64;
                    let idf = (1.0 + ((n - df).max(0.0) + 0.5) / (df + 0.5)).ln();
                    let w = idf * (K1 + 1.0) * f64::from(*qtf);
                    let tf = f64::from(*tf as f32);
                    let len = doc.words as f64;
                    parts.push(w * tf / (tf + K1 * (1.0 - B + B * len / avg)));
                }
                (!parts.is_empty()).then(|| round(parts.iter().sum()))
            }
            (
                None,
                Probe::Sparse { indices, weights },
                Some(Doc::Sparse {
                    indices: di,
                    weights: dw,
                }),
            ) => {
                let mut parts = Vec::new();
                for (i, w) in indices.iter().zip(weights) {
                    if *w == 0.0 {
                        continue;
                    }
                    if let Ok(j) = di.binary_search(i)
                        && dw[j] != 0.0
                    {
                        parts.push(f64::from(*w) * f64::from(dw[j]));
                    }
                }
                let norm = |w: &[f32]| w.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
                let sum: f64 = parts.iter().sum();
                let value = match q.op {
                    Op::Cosine => (sum / norm(dw) / norm(weights)).clamp(-1.0, 1.0),
                    _ => sum,
                };
                (!parts.is_empty()).then(|| round(value))
            }
            _ => None,
        };
        if let Some(s) = score {
            scored.push((s, *k));
        }
    }
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.truncate(q.limit);
    scored.into_iter().map(|(s, k)| (k, s)).collect()
}

fn keys_and_scores(hits: &[Hit]) -> Vec<(u64, f32)> {
    let k = |h: &Hit| u64::from_be_bytes((&*h.key).try_into().unwrap());
    hits.iter().map(|h| (k(h), h.score)).collect()
}

/// Runs `f` with a mutant switched on.
fn with_mutant<T>(name: &'static str, f: impl FnOnce() -> T) -> T {
    super::mutant::MUTANT.with(|m| m.set(name));
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    super::mutant::MUTANT.with(|m| m.set("none"));
    match out {
        Ok(v) => v,
        Err(p) => std::panic::resume_unwind(p),
    }
}
