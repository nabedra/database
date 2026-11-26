//! The V6 gate: BM25 scores against Tantivy's. Both index the same terms (Tantivy takes the
//! analyzer's tokens pre-tokenized), so the differential checks scoring, counts and the choice of
//! rows. Documents have at most 38 words, which Tantivy's one-byte field norms hold exactly.

use std::collections::BTreeMap;

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query as TantivyQuery, TermQuery};
use tantivy::schema::{
    IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{Index, IndexWriter, TantivyDocument, Term as TantivyTerm, doc};

use super::*;

fn tokens(a: &crate::text::Analyzer, text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    a.each(text, |t| {
        out.push(Token {
            offset_from: 0,
            offset_to: 0,
            position: out.len(),
            text: t.to_owned(),
            position_length: 1,
        })
    });
    out
}

/// Tantivy's score of every row that holds a term of the query.
fn tantivy_scores(
    a: &crate::text::Analyzer,
    docs: &BTreeMap<u64, String>,
    queries: &[String],
) -> Vec<BTreeMap<u64, f32>> {
    let mut schema = Schema::builder();
    let indexing = TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs);
    let body = schema.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(indexing),
    );
    let id = schema.add_u64_field("id", STORED);
    let index = Index::create_in_ram(schema.build());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
    for (k, text) in docs {
        let pre = PreTokenizedString {
            text: text.clone(),
            tokens: tokens(a, text),
        };
        writer.add_document(doc!(body => pre, id => *k)).unwrap();
    }
    writer.commit().unwrap();
    let searcher = index.reader().unwrap().searcher();
    let one = |q: &String| {
        let clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = tokens(a, q)
            .into_iter()
            .map(|t| {
                let term = TermQuery::new(
                    TantivyTerm::from_field_text(body, &t.text),
                    IndexRecordOption::WithFreqs,
                );
                (Occur::Should, Box::new(term) as Box<dyn TantivyQuery>)
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
    };
    queries.iter().map(one).collect()
}

fn ours(
    language: &str,
    docs: &BTreeMap<u64, String>,
    queries: &[String],
    segment_rows: usize,
) -> Vec<BTreeMap<u64, f32>> {
    let cfg = Config {
        segment_rows,
        merge_factor: 3,
        ..Config::new(Kind::Text(language.into()), vec![ColumnType::Tags])
    };
    let mut ix = serving(cfg);
    let mut m = Model::default();
    let rows = docs
        .iter()
        .map(|(k, t)| (*k, Some(text(t, vec![tags(&[])], *k))))
        .collect();
    ix.apply(RANGE, &m.page(rows, false)).unwrap();
    ix.maintain().unwrap();
    let pin = latest(&mut ix);
    let one = |q: &String| {
        let query = Query {
            exact: true,
            ..match_query(q, MAX_LIMIT)
        };
        keys_and_scores(&ix.search(&pin, &query).unwrap())
            .into_iter()
            .collect()
    };
    queries.iter().map(one).collect()
}

fn agree(want: &BTreeMap<u64, f32>, got: &BTreeMap<u64, f32>) -> std::result::Result<(), String> {
    if want.keys().ne(got.keys()) {
        return Err(format!("rows differ: tantivy {want:?}, ours {got:?}"));
    }
    for (k, w) in want {
        let g = got[k];
        if (w - g).abs() > 1e-5 * w.abs().max(1.0) {
            return Err(format!("row {k}: tantivy {w}, ours {g}"));
        }
    }
    Ok(())
}

fn zipf(g: &mut Gen, n: usize) -> usize {
    let u = (g.below(1 << 20) as f64 + 1.0) / f64::from(1 << 20);
    ((n as f64).powf(u) as usize).saturating_sub(1)
}

#[test]
fn vs07_bm25_equals_tantivy() {
    let simple = crate::text::Analyzer::new("simple").unwrap();
    check_with(
        Runs {
            cases: 40,
            ..Runs::default()
        },
        "vs07_bm25_equals_tantivy",
        |g| {
            let words = 2 + g.below(40) as usize;
            let n = 1 + g.below(300);
            let docs: BTreeMap<u64, String> = (0..n)
                .map(|k| {
                    let len = g.below(39) as usize;
                    (
                        k,
                        (0..len)
                            .map(|_| format!("w{}", zipf(g, words)))
                            .collect::<Vec<_>>()
                            .join(" "),
                    )
                })
                .collect();
            let queries: Vec<String> = (0..8)
                .map(|_| {
                    (0..1 + g.below(4))
                        .map(|_| format!("w{}", zipf(g, words + 3)))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect();
            let want = tantivy_scores(&simple, &docs, &queries);
            let got = ours("simple", &docs, &queries, 1 + g.below(64) as usize);
            for ((w, o), q) in want.iter().zip(&got).zip(&queries) {
                agree(w, o).map_err(|e| format!("query {q:?}: {e}"))?;
            }
            Ok(())
        },
    );
}

#[test]
fn vs07_bm25_with_stemming_equals_tantivy() {
    let english = crate::text::Analyzer::new("english").unwrap();
    let docs: BTreeMap<u64, String> = [
        "The runner was running and runs",
        "Runners run; a run is running",
        "the quick brown fox jumps over the lazy dog",
        "Foxes jumped, dogs were lazier",
        "",
        "Connection connected connecting connections",
        "a",
    ]
    .iter()
    .enumerate()
    .map(|(k, t)| (k as u64, t.to_string()))
    .collect();
    let queries: Vec<String> = [
        "running",
        "the run",
        "connect fox",
        "lazy lazy dog",
        "absent",
        "A",
    ]
    .iter()
    .map(|q| q.to_string())
    .collect();
    let want = tantivy_scores(&english, &docs, &queries);
    let got = ours("english", &docs, &queries, 2);
    for ((w, o), q) in want.iter().zip(&got).zip(&queries) {
        agree(w, o).unwrap_or_else(|e| panic!("query {q:?}: {e}"));
    }
    assert!(!want[0].is_empty() && want[4].is_empty());
}
