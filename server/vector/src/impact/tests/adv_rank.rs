//! Adversarial tests of task G, ranking: BM25 and sparse scores against references written from the
//! Lucene and Tantivy formulas and from the definition of dot and cosine (not from the code under
//! test), MaxScore exactness under ties and at every limit, filters, hostile input.

use std::collections::BTreeSet;

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query as TantivyQuery, TermQuery};
use tantivy::schema::{
    IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::{Index, IndexWriter, TantivyDocument, Term as TantivyTerm, doc};

use super::*;

// ---------------------------------------------------------------------------------------------
// References.

/// Words of a text: split at white space, ASCII lowercase. The corpora use `w<number>` words only.
fn toks(s: &str) -> Vec<String> {
    s.split_whitespace()
        .map(|w| w.to_ascii_lowercase())
        .collect()
}

/// Table-wide counts, computed from the definition: documents, words, documents per word.
struct Global {
    n: u64,
    total: u64,
    df: BTreeMap<String, u64>,
}

impl Global {
    fn of(docs: &BTreeMap<u64, String>) -> Global {
        let mut g = Global {
            n: docs.len() as u64,
            total: 0,
            df: BTreeMap::new(),
        };
        for text in docs.values() {
            let t = toks(text);
            g.total += t.len() as u64;
            for w in t.into_iter().collect::<BTreeSet<_>>() {
                *g.df.entry(w).or_default() += 1;
            }
        }
        g
    }

    /// The counts a gateway sends for a query: per distinct query word in byte order.
    fn stats(&self, q: &str) -> Stats {
        let words: BTreeSet<String> = toks(q).into_iter().collect();
        Stats {
            docs: self.n,
            words: self.total,
            df: words
                .iter()
                .map(|w| *self.df.get(w).unwrap_or(&0))
                .collect(),
        }
    }
}

/// BM25 as Lucene (and Tantivy) define it, k1 = 1.2, b = 0.75, without any rounding of lengths:
/// `sum over query words of idf * (k1 + 1) * tf / (tf + k1 * (1 - b + b * dl / avgdl))`, with
/// `idf = ln(1 + (N - n + 0.5) / (n + 0.5))`. A word twice in the query counts twice.
fn ref_bm25(docs: &BTreeMap<u64, String>, g: &Global, q: &str) -> BTreeMap<u64, f64> {
    let (k1, b) = (1.2f64, 0.75f64);
    let avg = g.total as f64 / g.n as f64;
    let mut qtf: BTreeMap<String, u32> = BTreeMap::new();
    for w in toks(q) {
        *qtf.entry(w).or_default() += 1;
    }
    let mut out = BTreeMap::new();
    for (key, text) in docs {
        let d = toks(text);
        let dl = d.len() as f64;
        let (mut s, mut hit) = (0.0, false);
        for (w, c) in &qtf {
            let tf = d.iter().filter(|x| *x == w).count() as f64;
            if tf == 0.0 {
                continue;
            }
            hit = true;
            let n = *g.df.get(w).unwrap_or(&0) as f64;
            let idf = (1.0 + (g.n as f64 - n + 0.5) / (n + 0.5)).ln();
            s += f64::from(*c) * idf * (k1 + 1.0) * tf / (tf + k1 * (1.0 - b + b * dl / avg));
        }
        if hit {
            out.insert(*key, s);
        }
    }
    out
}

fn k(h: &Hit) -> u64 {
    u64::from_be_bytes((&*h.key).try_into().unwrap())
}

/// The hits are the best `limit` of the reference, in the order of the score and then the key.
/// `tol(reference score)` is the absolute error allowed.
fn agree(
    want: &BTreeMap<u64, f64>,
    hits: &[Hit],
    limit: usize,
    tol: &dyn Fn(u64, f64) -> f64,
) -> std::result::Result<(), String> {
    if hits.len() != limit.min(want.len()) {
        return Err(format!(
            "{} hits, want {} (limit {limit}, {} matches)",
            hits.len(),
            limit.min(want.len()),
            want.len()
        ));
    }
    let mut seen = BTreeSet::new();
    for h in hits {
        let Some(r) = want.get(&k(h)) else {
            return Err(format!("row {} does not match", k(h)));
        };
        if !seen.insert(k(h)) {
            return Err(format!("row {} twice", k(h)));
        }
        let t = tol(k(h), *r);
        let got = f64::from(h.score);
        let beyond = r.abs() >= f64::from(f32::MAX) && got.abs() >= f64::from(f32::MAX) * 0.99999;
        if !beyond && (got - r).abs() > t {
            return Err(format!(
                "row {}: score {got}, reference {r}, tolerance {t}",
                k(h)
            ));
        }
    }
    for w in hits.windows(2) {
        let ordered = w[0].score > w[1].score || (w[0].score == w[1].score && k(&w[0]) < k(&w[1]));
        if !ordered {
            return Err(format!(
                "rows {} and {} are out of order",
                k(&w[0]),
                k(&w[1])
            ));
        }
    }
    if let Some(worst) = hits.last().map(|h| want[&k(h)]) {
        for (key, r) in want {
            let slack = tol(*key, *r).max(tol(*key, worst)) * 2.0;
            if !seen.contains(key) && *r > worst + slack {
                return Err(format!("row {key} scores {r}, above the worst hit {worst}"));
            }
        }
    }
    Ok(())
}

fn rel(key_unused: u64, r: f64) -> f64 {
    let _ = key_unused;
    1e-6 * r.abs() + 1e-12
}

fn zipf(g: &mut Gen, n: usize) -> usize {
    let u = (g.below(1 << 20) as f64 + 1.0) / f64::from(1 << 20);
    ((n as f64).powf(u) as usize).saturating_sub(1)
}

fn words(g: &mut Gen, vocab: usize, n: usize) -> Vec<String> {
    (0..n).map(|_| format!("w{}", zipf(g, vocab))).collect()
}

fn text_cfg_with(language: &str, segment_rows: usize, merge_factor: usize) -> Config {
    Config {
        segment_rows,
        merge_factor,
        ..Config::new(Kind::Text(language.into()), vec![ColumnType::Tags])
    }
}

fn load(ix: &mut ImpactIndex, m: &mut Model, rows: Vec<(u64, Row)>) {
    let changes = rows.into_iter().map(|(k, r)| (k, Some(r))).collect();
    ix.apply(RANGE, &m.page(changes, false)).unwrap();
}

// ---------------------------------------------------------------------------------------------
// BM25.

/// Corpora of six shapes: long documents, a word repeated, skewed lengths, one document, a word in
/// every document, many small documents (lists of several blocks).
fn corpus(g: &mut Gen, kind: u64) -> BTreeMap<u64, String> {
    let vocab = 2 + g.below(40) as usize;
    let mut docs = BTreeMap::new();
    match kind {
        0 => {
            for k in 0..3 + g.below(18) {
                let n = 200 + g.below(2300) as usize;
                docs.insert(k, words(g, vocab, n).join(" "));
            }
        }
        1 => {
            for k in 0..2 + g.below(28) {
                let r = 1 + g.below(300) as usize;
                let mut d = vec!["w1".to_owned(); r];
                let n = g.below(5) as usize;
                d.extend(words(g, vocab, n));
                docs.insert(k, d.join(" "));
            }
        }
        2 => {
            docs.insert(0, words(g, vocab, 1500).join(" "));
            for k in 1..6 + g.below(200) {
                let n = 1 + g.below(3) as usize;
                docs.insert(k, words(g, vocab, n).join(" "));
            }
        }
        3 => {
            let (key, n) = (g.below(5), 1 + g.below(50) as usize);
            docs.insert(key, words(g, vocab, n).join(" "));
        }
        4 => {
            for k in 0..2 + g.below(60) {
                let mut d = vec!["w0".to_owned(); 1 + g.below(5) as usize];
                let n = g.below(20) as usize;
                d.extend(words(g, vocab, n));
                docs.insert(k, d.join(" "));
            }
        }
        _ => {
            for k in 0..300 + g.below(900) {
                let n = 1 + g.below(12) as usize;
                docs.insert(k, words(g, vocab.max(3), n).join(" "));
            }
        }
    }
    docs
}

fn queries(g: &mut Gen, vocab_hint: usize) -> Vec<String> {
    (0..4)
        .map(|_| {
            let n = 1 + g.below(5) as usize;
            let mut w = words(g, vocab_hint + 3, n);
            if g.chance(1, 4) {
                w.push("w999".to_owned());
            }
            if g.chance(1, 4) {
                w.push(w[0].clone());
            }
            w.join(" ")
        })
        .collect()
}

#[test]
fn vs07_adv_bm25_equals_the_lucene_formula_on_hostile_corpora() {
    check_with(
        Runs {
            cases: 60,
            ..Runs::default()
        },
        "vs07_adv_bm25_equals_the_lucene_formula_on_hostile_corpora",
        |g| {
            let kind = g.below(6);
            let docs = corpus(g, kind);
            let shard: BTreeMap<u64, String> = if g.bool() {
                docs.iter()
                    .filter(|(k, _)| *k % 2 == 0)
                    .map(|(k, v)| (*k, v.clone()))
                    .collect()
            } else {
                docs.clone()
            };
            let glob = Global::of(&docs);
            let seg = *g.pick(&[1usize, 2, 7, 128, 129, 500, 4096]);
            let mut ix = serving(text_cfg_with("simple", seg, 2 + g.below(3) as usize));
            let mut m = Model::default();
            let rows = shard
                .iter()
                .map(|(k, t)| (*k, text(t, vec![tags(&[])], *k)))
                .collect();
            load(&mut ix, &mut m, rows);
            if g.bool() {
                settle(&mut ix);
            }
            let pin = latest(&mut ix);
            for q in queries(g, 40) {
                let limit = *g.pick(&[1usize, 2, 5, 50, 1000]);
                let exact = g.bool();
                // Table-wide counts that differ from the local ones when the index holds a shard.
                let stats = glob.stats(&q);
                let query = Query {
                    exact,
                    stats: Some(stats),
                    ..match_query(&q, limit)
                };
                let hits = ix.search(&pin, &query).map_err(|e| e.to_string())?;
                let want = ref_bm25(&shard, &glob, &q);
                agree(&want, &hits, limit, &rel).map_err(|e| format!("query {q:?}: {e}"))?;
                // Counts of this worker, from the definition.
                let local = Global::of(&shard);
                let got = ix.counts(&[RANGE], &q).map_err(|e| e.to_string())?;
                if got != local.stats(&q) {
                    return Err(format!("local counts {got:?} != {:?}", local.stats(&q)));
                }
                // Without counts the scores come from the pinned ranges alone.
                let q2 = Query {
                    exact,
                    stats: None,
                    ..match_query(&q, limit)
                };
                let hits = ix.search(&pin, &q2).map_err(|e| e.to_string())?;
                let want = ref_bm25(&shard, &local, &q);
                agree(&want, &hits, limit, &rel).map_err(|e| format!("local, query {q:?}: {e}"))?;
            }
            Ok(())
        },
    );
}

#[test]
fn vs07_adv_a_word_in_every_document_still_scores_above_zero() {
    // The old Lucene formula gives such a word a zero or negative weight. The current one, and
    // Tantivy's, gives ln(1 + 0.5 / (N + 0.5)) > 0, so a shorter document still ranks first.
    let mut ix = serving(text_cfg_with("simple", 2, 2));
    let mut m = Model::default();
    let docs: BTreeMap<u64, String> = [
        (1, "common"),
        (2, "common a"),
        (3, "common a b c d e f g h"),
        (4, "common common common x y"),
    ]
    .into_iter()
    .map(|(k, t)| (k, t.to_owned()))
    .collect();
    let rows = docs
        .iter()
        .map(|(k, t)| (*k, text(t, vec![tags(&[])], *k)))
        .collect();
    load(&mut ix, &mut m, rows);
    let pin = latest(&mut ix);
    let glob = Global::of(&docs);
    let hits = ix.search(&pin, &match_query("common", 10)).unwrap();
    assert_eq!(hits.len(), 4);
    assert!(hits.iter().all(|h| h.score > 0.0), "{hits:?}");
    agree(&ref_bm25(&docs, &glob, "common"), &hits, 10, &rel).unwrap();
    // Table-wide counts that say every one of 1000 rows holds it, and more than there are rows.
    for (docs_n, df) in [(1000u64, 1000u64), (1000, 5000), (1, 1), (0, 0), (0, 7)] {
        let q = Query {
            stats: Some(Stats {
                docs: docs_n,
                words: docs_n * 3,
                df: vec![df],
            }),
            ..match_query("common", 10)
        };
        let hits = ix.search(&pin, &q).unwrap();
        assert_eq!(hits.len(), 4);
        assert!(
            hits.iter().all(|h| h.score.is_finite() && h.score > 0.0),
            "{docs_n} {df} {hits:?}"
        );
        assert_eq!(k(&hits[0]), 1, "the shortest document ranks first");
    }
}

#[test]
fn vs07_adv_a_one_document_corpus_scores_by_the_formula() {
    let docs: BTreeMap<u64, String> = [(5u64, "w1 w2 w2 w3".to_owned())].into();
    let mut ix = serving(text_cfg_with("simple", 4096, 2));
    let mut m = Model::default();
    load(
        &mut ix,
        &mut m,
        vec![(5, text(&docs[&5], vec![tags(&[])], 5))],
    );
    let pin = latest(&mut ix);
    let glob = Global::of(&docs);
    for q in ["w2", "w1 w2", "w2 w2 w9", "w9", "w1 w2 w3"] {
        let hits = ix.search(&pin, &match_query(q, 3)).unwrap();
        agree(&ref_bm25(&docs, &glob, q), &hits, 3, &rel).unwrap_or_else(|e| panic!("{q}: {e}"));
    }
}

// Tantivy as a second reference, on corpora the author did not use. Tantivy keeps a document's
// length in one byte, exact up to 39 and at some larger values, so the lengths below are such
// values, and the test checks that Tantivy holds them exactly.

const EXACT_LENGTHS: &[usize] = &[0, 1, 2, 5, 13, 39, 40, 48, 64, 104, 200];

fn tantivy_scores(docs: &BTreeMap<u64, String>, q: &str) -> BTreeMap<u64, f32> {
    let mut schema = Schema::builder();
    let indexing = TextFieldIndexing::default()
        .set_index_option(IndexRecordOption::WithFreqs)
        .set_tokenizer("default");
    let body = schema.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(indexing),
    );
    let id = schema.add_u64_field("id", STORED);
    let index = Index::create_in_ram(schema.build());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
    for (key, text) in docs {
        writer
            .add_document(doc!(body => text.as_str(), id => *key))
            .unwrap();
    }
    writer.commit().unwrap();
    let searcher = index.reader().unwrap().searcher();
    // Tantivy's lengths are the real ones.
    let mut lens: Vec<u64> = Vec::new();
    for seg in searcher.segment_readers() {
        let norms = seg.get_fieldnorms_reader(body).unwrap();
        lens.extend((0..seg.max_doc()).map(|d| u64::from(norms.fieldnorm(d))));
    }
    let mut want: Vec<u64> = docs.values().map(|t| toks(t).len() as u64).collect();
    want.sort_unstable();
    lens.sort_unstable();
    assert_eq!(lens, want, "tantivy rounded a length");
    let clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = toks(q)
        .into_iter()
        .map(|w| {
            let t = TermQuery::new(
                TantivyTerm::from_field_text(body, &w),
                IndexRecordOption::WithFreqs,
            );
            (Occur::Should, Box::new(t) as Box<dyn TantivyQuery>)
        })
        .collect();
    if clauses.is_empty() {
        return BTreeMap::new();
    }
    let top = searcher
        .search(
            &BooleanQuery::new(clauses),
            &TopDocs::with_limit(docs.len().max(1)).order_by_score(),
        )
        .unwrap();
    top.into_iter()
        .map(|(score, at)| {
            let d: TantivyDocument = searcher.doc(at).unwrap();
            (d.get_first(id).and_then(|v| v.as_u64()).unwrap(), score)
        })
        .collect()
}

fn exact_doc(g: &mut Gen, len: usize, vocab: usize, fill: Option<(&str, usize)>) -> String {
    let mut d: Vec<String> = Vec::new();
    if let Some((w, n)) = fill {
        d.extend(std::iter::repeat_n(w.to_owned(), n.min(len)));
    }
    let rest = len - d.len();
    d.extend(words(g, vocab, rest));
    d.join(" ")
}

#[test]
fn vs07_adv_bm25_equals_tantivy_on_long_repeated_skewed_and_universal_corpora() {
    check_with(
        Runs {
            cases: 40,
            ..Runs::default()
        },
        "vs07_adv_bm25_equals_tantivy_on_long_repeated_skewed_and_universal_corpora",
        |g| {
            let vocab = 3 + g.below(30) as usize;
            let mut docs: BTreeMap<u64, String> = BTreeMap::new();
            let shape = g.below(5);
            let n = match shape {
                3 => 1,
                _ => 2 + g.below(60),
            };
            for key in 0..n {
                let len = match shape {
                    0 => *g.pick(&[104usize, 200, 200, 64]),
                    2 => *g.pick(&[1usize, 1, 1, 200]),
                    _ => *g.pick(EXACT_LENGTHS),
                };
                let fill = match shape {
                    1 => Some(("w1", 1 + g.below(150) as usize)),
                    4 => Some(("w0", 1)),
                    _ => None,
                };
                let len = if shape == 4 { len.max(1) } else { len };
                docs.insert(key, exact_doc(g, len, vocab, fill));
            }
            let glob = Global::of(&docs);
            // Our worker holds half of the rows and is told the counts of all of them.
            let shard: BTreeMap<u64, String> = if g.bool() {
                docs.iter()
                    .filter(|(k, _)| *k % 2 == 1 || docs.len() == 1)
                    .map(|(k, v)| (*k, v.clone()))
                    .collect()
            } else {
                docs.clone()
            };
            let mut ix = serving(text_cfg_with("simple", 1 + g.below(80) as usize, 3));
            let mut m = Model::default();
            let rows = shard
                .iter()
                .map(|(k, t)| (*k, text(t, vec![tags(&[])], *k)))
                .collect();
            load(&mut ix, &mut m, rows);
            let pin = latest(&mut ix);
            for q in queries(g, vocab) {
                let want = tantivy_scores(&docs, &q);
                let query = Query {
                    exact: true,
                    stats: Some(glob.stats(&q)),
                    ..match_query(&q, MAX_LIMIT)
                };
                let hits = ix.search(&pin, &query).map_err(|e| e.to_string())?;
                let want: BTreeMap<u64, f64> = want
                    .into_iter()
                    .filter(|(key, _)| shard.contains_key(key))
                    .map(|(key, s)| (key, f64::from(s)))
                    .collect();
                agree(&want, &hits, MAX_LIMIT, &|_, r| 2e-5 * r.abs() + 1e-6)
                    .map_err(|e| format!("query {q:?}: {e}"))?;
            }
            Ok(())
        },
    );
}

// ---------------------------------------------------------------------------------------------
// Sparse dot and cosine.

fn pick_weight(g: &mut Gen) -> f32 {
    const PALETTE: &[f32] = &[
        1.0,
        -1.0,
        0.5,
        -2.5,
        3.0,
        3.0e38,
        -3.0e38,
        1e30,
        -1e30,
        1e-30,
        -1e-30,
        1e-40,
        -1e-40,
        1.4e-45,
        -1.4e-45,
        f32::MIN_POSITIVE,
        0.0,
        -0.0,
        7.0,
        0.1,
        f32::MAX,
        f32::MIN,
    ];
    if g.bool() {
        return *g.pick(PALETTE);
    }
    loop {
        let f = f32::from_bits(g.u64() as u32);
        if f.is_finite() {
            return f;
        }
    }
}

fn pick_index(g: &mut Gen, universe: u32) -> u32 {
    match g.below(20) {
        0 => u32::MAX,
        1 => 0,
        _ => g.below(u64::from(universe)) as u32,
    }
}

fn sparse_vec(g: &mut Gen, universe: u32, max: usize) -> Vec<(u32, f32)> {
    let mut m: BTreeMap<u32, f32> = BTreeMap::new();
    for _ in 0..g.below(max as u64 + 1) {
        m.insert(pick_index(g, universe), pick_weight(g));
    }
    m.into_iter().collect()
}

/// Dot or cosine from the definition. Returns the score and the magnitude of the sum of absolute
/// products (what rounding errors of the sum are relative to). `None`: no shared nonzero index.
fn ref_sparse(d: &[(u32, f32)], q: &[(u32, f32)], cosine: bool) -> Option<(f64, f64)> {
    let norm = |v: &[(u32, f32)]| {
        v.iter()
            .map(|(_, w)| f64::from(*w) * f64::from(*w))
            .sum::<f64>()
            .sqrt()
    };
    let (mut dot, mut mag, mut hit) = (0.0f64, 0.0f64, false);
    for (i, w) in q {
        if *w == 0.0 {
            continue;
        }
        if let Some((_, x)) = d.iter().find(|(j, x)| j == i && *x != 0.0) {
            let p = f64::from(*w) * f64::from(*x);
            dot += p;
            mag += p.abs();
            hit = true;
        }
    }
    if !hit {
        return None;
    }
    if cosine {
        let n = norm(d) * norm(q);
        Some(((dot / n).clamp(-1.0, 1.0), mag / n))
    } else {
        Some((dot.clamp(-f64::from(f32::MAX), f64::from(f32::MAX)), mag))
    }
}

fn tol_sparse(mag: f64, r: f64) -> f64 {
    1e-6 * mag.min(1e300) + 2e-7 * r.abs() + 1e-44
}

#[test]
fn vs07_adv_sparse_dot_and_cosine_equal_brute_force_on_hostile_weights() {
    check_with(
        Runs {
            cases: 80,
            ..Runs::default()
        },
        "vs07_adv_sparse_dot_and_cosine_equal_brute_force_on_hostile_weights",
        |g| {
            let universe = *g.pick(&[3u32, 6, 12, 40]);
            let n = *g.pick(&[1u64, 5, 40, 700, 1500]);
            let docs: BTreeMap<u64, Vec<(u32, f32)>> = (0..n)
                .map(|k| {
                    let max = if n >= 700 { 4 } else { 12 };
                    (k, sparse_vec(g, universe, max))
                })
                .collect();
            let seg = *g.pick(&[1usize, 3, 64, 129, 4096]);
            let mut ix = serving(Config {
                segment_rows: seg,
                merge_factor: 2 + g.below(3) as usize,
                ..sparse_cfg()
            });
            let mut m = Model::default();
            load(
                &mut ix,
                &mut m,
                docs.iter()
                    .map(|(k, d)| (*k, sparse(d, vec![tags(&[])], *k)))
                    .collect(),
            );
            if g.bool() {
                settle(&mut ix);
            }
            let pin = latest(&mut ix);
            for _ in 0..4 {
                let q = sparse_vec(g, universe, 8);
                let cosine = g.bool();
                let limit = *g.pick(&[1usize, 2, 7, 100, 1000]);
                let op = if cosine { Op::Cosine } else { Op::Dot };
                let zero = q.iter().all(|(_, w)| *w == 0.0);
                for exact in [false, true] {
                    let query = Query {
                        exact,
                        ..sparse_query(op, &q, limit)
                    };
                    let got = ix.search(&pin, &query);
                    if cosine && zero {
                        if !matches!(got, Err(Error::InvalidQuery(_))) {
                            return Err(format!("a zero vector has no cosine: {got:?}"));
                        }
                        continue;
                    }
                    let hits = got.map_err(|e| format!("{q:?}: {e}"))?;
                    let mags: BTreeMap<u64, f64> = docs
                        .iter()
                        .filter_map(|(key, d)| Some((*key, ref_sparse(d, &q, cosine)?.1)))
                        .collect();
                    let want: BTreeMap<u64, f64> = docs
                        .iter()
                        .filter_map(|(key, d)| Some((*key, ref_sparse(d, &q, cosine)?.0)))
                        .collect();
                    agree(&want, &hits, limit, &|key, r| tol_sparse(mags[&key], r))
                        .map_err(|e| format!("{op:?} {q:?} limit {limit} exact {exact}: {e}"))?;
                }
            }
            Ok(())
        },
    );
}

#[test]
fn vs07_adv_zero_length_vectors_never_rank_and_are_still_rows() {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    load(
        &mut ix,
        &mut m,
        vec![
            (1, sparse(&[], vec![tags(&[])], 1)),
            (2, sparse(&[(3, 0.0)], vec![tags(&[])], 2)),
            (3, sparse(&[(3, 2.0)], vec![tags(&[])], 3)),
        ],
    );
    let pin = latest(&mut ix);
    let hits = |ix: &ImpactIndex, q: Query| ix.search(&pin, &q);
    assert_eq!(
        images(&hits(&ix, sparse_query(Op::Dot, &[(3, 1.0)], 10)).unwrap()),
        [3]
    );
    assert_eq!(
        images(&hits(&ix, sparse_query(Op::Cosine, &[(3, 1.0)], 10)).unwrap()),
        [3]
    );
    assert_eq!(hits(&ix, sparse_query(Op::Dot, &[], 10)).unwrap(), []);
    assert!(matches!(
        hits(&ix, sparse_query(Op::Cosine, &[], 10)),
        Err(Error::InvalidQuery(_))
    ));
    assert_eq!(
        hits(&ix, sparse_query(Op::Dot, &[(3, 0.0)], 10)).unwrap(),
        []
    );
    // An empty vector becomes a vector, and the other way round, at the next positions.
    load(
        &mut ix,
        &mut m,
        vec![
            (1, sparse(&[(3, 5.0)], vec![tags(&[])], 11)),
            (3, sparse(&[], vec![tags(&[])], 13)),
        ],
    );
    let pin2 = latest(&mut ix);
    assert_eq!(
        images(
            &ix.search(&pin2, &sparse_query(Op::Dot, &[(3, 1.0)], 10))
                .unwrap()
        ),
        [11]
    );
    assert_eq!(
        images(
            &ix.search(&pin, &sparse_query(Op::Dot, &[(3, 1.0)], 10))
                .unwrap()
        ),
        [3]
    );
}

#[test]
fn vs07_adv_malformed_sparse_values_are_refused_without_a_panic() {
    let bad_docs: Vec<(Vec<u32>, Vec<f32>)> = vec![
        (vec![1, 1], vec![1.0, 1.0]),
        (vec![2, 1], vec![1.0, 1.0]),
        (vec![1, 2], vec![1.0]),
        (vec![1], vec![1.0, 2.0]),
        (vec![1], vec![f32::NAN]),
        (vec![1], vec![f32::INFINITY]),
        (vec![1], vec![f32::NEG_INFINITY]),
    ];
    for (indices, weights) in bad_docs {
        let mut ix = serving(sparse_cfg());
        let mut m = Model::default();
        let row = Row {
            doc: Some(Doc::Sparse {
                indices: indices.clone().into(),
                weights: weights.clone().into(),
            }),
            cols: vec![tags(&[])],
            image: Arc::new(1u64.to_be_bytes()),
        };
        let page = m.page(vec![(1, Some(row))], false);
        assert!(
            matches!(ix.apply(RANGE, &page), Err(Error::InvalidFeed(_))),
            "{indices:?} {weights:?}"
        );
        assert!(ix.status(RANGE).unwrap().damage.is_some());
        assert!(ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).is_err());
    }
    let mut ix = serving(sparse_cfg());
    let pin = latest(&mut ix);
    let probe = |indices: Vec<u32>, weights: Vec<f32>| Query {
        probe: Probe::Sparse { indices, weights },
        ..sparse_query(Op::Dot, &[], 5)
    };
    for (i, w) in [
        (vec![1, 1], vec![1.0, 1.0]),
        (vec![2, 1], vec![1.0, 1.0]),
        (vec![1, 2], vec![1.0]),
        (vec![1], vec![f32::INFINITY]),
        (vec![1], vec![f32::NAN]),
    ] {
        for op in [Op::Dot, Op::Cosine] {
            let q = Query {
                op,
                ..probe(i.clone(), w.clone())
            };
            assert!(
                matches!(ix.search(&pin, &q), Err(Error::InvalidQuery(_))),
                "{i:?} {w:?} {op:?}"
            );
        }
    }
    let too_wide = probe(
        (0..MAX_QUERY_TERMS as u32 + 1).collect(),
        vec![1.0; MAX_QUERY_TERMS + 1],
    );
    assert!(matches!(
        ix.search(&pin, &too_wide),
        Err(Error::InvalidQuery(_))
    ));
    // The text probe on a sparse column and the sparse probe on a text one.
    assert!(matches!(
        ix.search(&pin, &match_query("a", 1)),
        Err(Error::InvalidQuery(_))
    ));
    let mut tx = serving(text_cfg());
    let tpin = latest(&mut tx);
    assert!(matches!(
        tx.search(&tpin, &sparse_query(Op::Dot, &[(1, 1.0)], 1)),
        Err(Error::InvalidQuery(_))
    ));
    assert!(matches!(
        tx.search(
            &tpin,
            &Query {
                op: Op::Dot,
                ..match_query("a", 1)
            }
        ),
        Err(Error::InvalidQuery(_))
    ));
}

// ---------------------------------------------------------------------------------------------
// Ties and limits.

const SCRAMBLED: [u64; 12] = [9, 3, 11, 1, 7, 5, 12, 2, 10, 4, 8, 6];

fn orders() -> Vec<Vec<u64>> {
    let mut asc: Vec<u64> = (1..=12).collect();
    let mut desc = asc.clone();
    desc.reverse();
    asc.rotate_left(5);
    vec![SCRAMBLED.to_vec(), desc, asc]
}

/// Three rows that score higher (20, 21, 22) and twelve that tie below them, inserted in several
/// orders, with merges between and without, at every limit.
fn tied(sparse_column: bool, seg: usize, merged: bool, order: &[u64], exact: bool) {
    let cfg = if sparse_column {
        sparse_cfg()
    } else {
        text_cfg_with("simple", seg, 2)
    };
    let cfg = Config {
        segment_rows: seg,
        merge_factor: 2,
        ..cfg
    };
    let mut ix = serving(cfg);
    let mut m = Model::default();
    let mk = |key: u64, better: bool| {
        if sparse_column {
            let w = if better { 2.0 } else { 1.0 };
            sparse(&[(0, w), (1, 0.0)], vec![tags(&[])], key)
        } else if better {
            text("alpha", vec![tags(&[])], key)
        } else {
            text("alpha beta", vec![tags(&[])], key)
        }
    };
    for chunk in order.chunks(4) {
        load(
            &mut ix,
            &mut m,
            chunk.iter().map(|k| (*k, mk(*k, false))).collect(),
        );
        if merged {
            settle(&mut ix);
        }
    }
    load(
        &mut ix,
        &mut m,
        [22, 20, 21].iter().map(|k| (*k, mk(*k, true))).collect(),
    );
    if merged {
        settle(&mut ix);
    }
    let pin = latest(&mut ix);
    let want: Vec<u64> = [20, 21, 22].into_iter().chain(1..=12).collect();
    for limit in [1usize, 2, 3, 4, 5, 14, 15, 16, 999, 1000] {
        let q = if sparse_column {
            Query {
                exact,
                ..sparse_query(Op::Dot, &[(0, 1.0)], limit)
            }
        } else {
            Query {
                exact,
                ..match_query("alpha", limit)
            }
        };
        let hits = ix.search(&pin, &q).unwrap();
        let got: Vec<u64> = hits.iter().map(k).collect();
        assert_eq!(
            got,
            want[..limit.min(15)],
            "limit {limit} seg {seg} merged {merged} order {order:?} exact {exact}"
        );
        // Equal scores are equal bits.
        for w in hits.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
        let low: BTreeSet<u32> = hits
            .iter()
            .filter(|h| k(h) <= 12)
            .map(|h| h.score.to_bits())
            .collect();
        assert!(low.len() <= 1, "tied rows score alike: {low:?}");
    }
}

#[test]
fn vs07_adv_ties_break_by_key_at_every_limit_in_every_insertion_order() {
    for sparse_column in [false, true] {
        for seg in [1usize, 2, 3, 5, 64] {
            for merged in [false, true] {
                for order in orders() {
                    for exact in [false, true] {
                        tied(sparse_column, seg, merged, &order, exact);
                    }
                }
            }
        }
    }
}

#[test]
fn vs07_adv_limit_zero_and_above_the_maximum_are_refused_and_nothing_else() {
    let mut ix = serving(text_cfg());
    let mut m = Model::default();
    load(&mut ix, &mut m, vec![(1, text("a b", vec![tags(&[])], 1))]);
    let pin = latest(&mut ix);
    for limit in [0usize, MAX_LIMIT + 1, usize::MAX] {
        assert!(
            matches!(
                ix.search(&pin, &match_query("a", limit)),
                Err(Error::InvalidQuery(_))
            ),
            "{limit}"
        );
    }
    for limit in [1usize, MAX_LIMIT] {
        assert_eq!(ix.search(&pin, &match_query("a", limit)).unwrap().len(), 1);
    }
}

#[test]
#[ignore = "open bug VECTOR-impact-maxscore-drops-tied-underflow"]
fn vs07_adv_scores_that_underflow_or_round_to_negative_zero_tie_as_zero() {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    // 1e-30 * 1e-30 is 1e-60 in f64 and 0 in f32.
    load(
        &mut ix,
        &mut m,
        vec![
            (3, sparse(&[(0, 1e-30)], vec![tags(&[])], 3)),
            (2, sparse(&[(0, 1e-30)], vec![tags(&[])], 2)),
            (1, sparse(&[(0, -1e-30)], vec![tags(&[])], 1)),
        ],
    );
    let pin = latest(&mut ix);
    for exact in [false, true] {
        for limit in [1usize, 2, 3, 4] {
            let q = Query {
                exact,
                ..sparse_query(Op::Dot, &[(0, 1e-30)], limit)
            };
            let hits = ix.search(&pin, &q).unwrap();
            let got: Vec<u64> = hits.iter().map(k).collect();
            assert_eq!(got, [1u64, 2, 3][..limit.min(3)]);
            assert!(hits.iter().all(|h| h.score.to_bits() == 0), "{hits:?}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Filters.

#[test]
fn vs07_adv_a_filter_never_costs_a_row_when_limit_rows_match() {
    check_with(
        Runs {
            cases: 60,
            ..Runs::default()
        },
        "vs07_adv_a_filter_never_costs_a_row_when_limit_rows_match",
        |g| {
            let sparse_column = g.bool();
            let n = 20 + g.below(700);
            // The best rows are the ones the filter drops: its matches rank low.
            let pass = |k: u64| k % 7 == 3;
            let pass_set: BTreeSet<u64> = (0..n).filter(|k| pass(*k) && *k % 5 != 0).collect();
            let cfg = Config {
                segment_rows: *g.pick(&[1usize, 8, 100, 4096]),
                merge_factor: 2,
                ..if sparse_column {
                    sparse_cfg()
                } else {
                    text_cfg_with("simple", 1, 2)
                }
            };
            let mut ix = serving(cfg);
            let mut m = Model::default();
            let mut docs_t: BTreeMap<u64, String> = BTreeMap::new();
            let mut docs_s: BTreeMap<u64, Vec<(u32, f32)>> = BTreeMap::new();
            let mut rows = Vec::new();
            for key in 0..n {
                let col = vec![tags(if pass_set.contains(&key) {
                    &["x"]
                } else {
                    &["y"]
                })];
                if sparse_column {
                    // Rows that pass get small weights, the others large ones.
                    let w = if pass_set.contains(&key) {
                        0.5 + (key % 13) as f32 / 8.0
                    } else {
                        100.0 + key as f32
                    };
                    docs_s.insert(key, vec![(0, w)]);
                    rows.push((key, sparse(&[(0, w)], col, key)));
                } else {
                    let rep = if pass_set.contains(&key) {
                        1 + key % 3
                    } else {
                        5 + key % 4
                    };
                    let t = format!(
                        "{} {}",
                        "w1 ".repeat(rep as usize),
                        "w2 ".repeat((key % 9) as usize)
                    );
                    docs_t.insert(key, t.clone());
                    rows.push((key, text(&t, col, key)));
                }
            }
            load(&mut ix, &mut m, rows);
            if g.bool() {
                settle(&mut ix);
            }
            let pin = latest(&mut ix);
            for limit in [1usize, 5, 30, 1000] {
                let filter = Some(Filter::Contains(0, "x".into()));
                let q = if sparse_column {
                    Query {
                        filter,
                        exact: g.bool(),
                        ..sparse_query(Op::Dot, &[(0, 1.0)], limit)
                    }
                } else {
                    Query {
                        filter,
                        exact: g.bool(),
                        ..match_query("w1 w2", limit)
                    }
                };
                let hits = ix.search(&pin, &q).map_err(|e| e.to_string())?;
                let matches = pass_set.len();
                if hits.len() != limit.min(matches) {
                    return Err(format!(
                        "limit {limit}: {} rows of {matches} matches",
                        hits.len()
                    ));
                }
                if hits.iter().any(|h| !pass_set.contains(&k(h))) {
                    return Err("a row that the filter drops was returned".into());
                }
                // The same rows as the unfiltered ranking restricted to the filter.
                let all = if sparse_column {
                    sparse_query(Op::Dot, &[(0, 1.0)], 1000)
                } else {
                    match_query("w1 w2", 1000)
                };
                let full = ix.search(&pin, &all).map_err(|e| e.to_string())?;
                if full.len() < 1000 {
                    let want: Vec<u64> = full
                        .iter()
                        .map(k)
                        .filter(|key| pass_set.contains(key))
                        .take(limit)
                        .collect();
                    let got: Vec<u64> = hits.iter().map(k).collect();
                    if want != got {
                        return Err(format!("filtered {got:?} != restricted {want:?}"));
                    }
                }
            }
            let _ = (docs_t, docs_s);
            Ok(())
        },
    );
}

// ---------------------------------------------------------------------------------------------
// Hostile input.

#[test]
fn vs07_adv_hostile_counts_never_panic_or_give_a_nonfinite_score() {
    let mut ix = serving(text_cfg_with("simple", 3, 2));
    let mut m = Model::default();
    let rows = (0..40u64)
        .map(|k| (k, text(&format!("w{} w1 w2 w2", k % 5), vec![tags(&[])], k)))
        .collect();
    load(&mut ix, &mut m, rows);
    let pin = latest(&mut ix);
    let extremes = [0u64, 1, 2, 40, u64::MAX / 2, u64::MAX - 1, u64::MAX];
    for docs in extremes {
        for words in extremes {
            for df in extremes {
                let q = Query {
                    stats: Some(Stats {
                        docs,
                        words,
                        df: vec![df, df],
                    }),
                    ..match_query("w1 w2", 50)
                };
                let hits = ix.search(&pin, &q).unwrap();
                assert!(
                    hits.iter().all(|h| h.score.is_finite()),
                    "{docs} {words} {df}: {hits:?}"
                );
                for w in hits.windows(2) {
                    assert!(
                        w[0].score > w[1].score
                            || (w[0].score == w[1].score && k(&w[0]) < k(&w[1]))
                    );
                }
            }
        }
    }
    let mut a = Stats {
        docs: u64::MAX,
        words: u64::MAX,
        df: vec![u64::MAX],
    };
    a.add(&Stats {
        docs: 5,
        words: 5,
        df: vec![5],
    })
    .unwrap();
    assert_eq!((a.docs, a.words, a.df[0]), (u64::MAX, u64::MAX, u64::MAX));
    assert!(a.add(&Stats::default()).is_err());
}

#[test]
fn vs07_adv_counts_of_ranges_add_up() {
    let mut ix = serving(text_cfg_with("simple", 2, 2));
    let other = RangeId(8);
    ix.begin_load(other, 0, 0).unwrap();
    ix.finish_load(other).unwrap();
    ix.caught_up(other, 0).unwrap();
    let mut m = Model::default();
    load(
        &mut ix,
        &mut m,
        (0..9)
            .map(|k| (k, text(&format!("w1 w{}", k % 3), vec![tags(&[])], k)))
            .collect(),
    );
    let mut m2 = Model::default();
    let page = m2.page(
        (100..105)
            .map(|k| (k, Some(text("w1 w2 w2 w2", vec![tags(&[])], k))))
            .collect(),
        false,
    );
    ix.apply(other, &page).unwrap();
    let a = ix.counts(&[RANGE], "w1 w2").unwrap();
    let b = ix.counts(&[other], "w1 w2").unwrap();
    let both = ix.counts(&[RANGE, other], "w1 w2").unwrap();
    let mut sum = a.clone();
    sum.add(&b).unwrap();
    assert_eq!(sum, both);
    assert_eq!((both.docs, both.words), (14, 9 * 2 + 5 * 4));
    assert!(ix.counts(&[RangeId(99)], "w1").is_err());
}
