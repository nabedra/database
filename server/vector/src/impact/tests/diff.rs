//! Every operation against brute force over the visible rows: `match`, sparse `dot` and `cosine`,
//! exact and pruned, filtered, at every retained cut and snapshot, under churn, merges and pins
//! held across them. And MaxScore against exhaustive scoring on a corpus large enough to prune.

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

use super::*;

const WORDS: [&str; 9] = [
    "apple", "apples", "bread", "cheese", "dates", "eggs", "figs", "grapes", "honey",
];
// Large parts of both signs with small residues make sums cancel (review G, B1).
const WEIGHTS: [f32; 14] = [
    -2.5, -1.0, -0.0, 0.0, 0.25, 1.0, 1.0, 3.0, 1e-30, 1e30, 1048576.0, -1048576.0, 0.001, 0.00105,
];
const TAGS: [&str; 3] = ["a", "b", "c"];

fn doc_text(g: &mut Gen) -> String {
    g.vec(12, |g| *g.pick(&WORDS)).join(" ")
}

fn doc_sparse(g: &mut Gen, max: usize) -> Vec<(u32, f32)> {
    let mut pairs: BTreeMap<u32, f32> = BTreeMap::new();
    for _ in 0..g.below(max as u64 + 1) {
        pairs.insert(g.below(10) as u32, *g.pick(&WEIGHTS));
    }
    pairs.into_iter().collect()
}

fn row(g: &mut Gen, kind: &Kind, image: u64) -> Option<Row> {
    let cols = vec![tags(&TAGS[..g.below(4) as usize])];
    match g.below(8) {
        0 => None,
        1 if *kind == Kind::Sparse => Some(Row {
            doc: None,
            cols,
            image: Arc::new(image.to_be_bytes()),
        }),
        _ => Some(match kind {
            Kind::Text(_) => text(&doc_text(g), cols, image),
            Kind::Sparse => sparse(&doc_sparse(g, 6), cols, image),
        }),
    }
}

fn query(g: &mut Gen, kind: &Kind) -> Query {
    let mut q = match kind {
        Kind::Text(_) => match_query(&g.vec(4, |g| *g.pick(&WORDS)).join(" "), 1),
        Kind::Sparse => {
            let op = *g.pick(&[Op::Dot, Op::Cosine]);
            let mut pairs = doc_sparse(g, 5);
            if op == Op::Cosine && pairs.iter().all(|p| p.1 == 0.0) {
                pairs = vec![(1, 1.0)];
            }
            sparse_query(op, &pairs, 1)
        }
    };
    q.limit = 1 + g.below(6) as usize;
    q.exact = g.bool();
    q.filter = match g.below(4) {
        0 => Some(Filter::Contains(0, (*g.pick(&TAGS)).into())),
        1 => Some(Filter::Not(Box::new(Filter::Contains(
            0,
            (*g.pick(&TAGS)).into(),
        )))),
        _ => None,
    };
    q
}

/// The table-wide counts that a gateway sends: of the newest rows.
fn stats(kind: &Kind, m: &Model, q: &Query) -> Option<Stats> {
    let (Kind::Text(l), Probe::Text(t)) = (kind, &q.probe) else {
        return None;
    };
    Some(stats_of(
        &crate::text::Analyzer::new(l).unwrap(),
        &m.visible(m.now()),
        t,
    ))
}

fn compare(
    ix: &ImpactIndex,
    kind: &Kind,
    m: &Model,
    pin: &Pin,
    point: Point,
    q: &Query,
) -> std::result::Result<(), String> {
    let mut q = q.clone();
    q.stats = stats(kind, m, &q);
    let want = brute(kind, &m.visible(point), &q, q.stats.as_ref());
    for exact in [q.exact, !q.exact] {
        let q = Query { exact, ..q.clone() };
        let got = keys_and_scores(&ix.search(pin, &q).map_err(|e| format!("{e}"))?);
        if got != want {
            return Err(format!("at {point:?}, {q:?}:\n got {got:?}\nwant {want:?}"));
        }
    }
    Ok(())
}

fn run(g: &mut Gen) -> std::result::Result<(), String> {
    let kind = if g.bool() {
        Kind::Text("english".into())
    } else {
        Kind::Sparse
    };
    let cfg = Config {
        segment_rows: 1 + g.below(6) as usize,
        merge_factor: 2 + g.below(3) as usize,
        max_segment_rows: 64,
        ..Config::new(kind.clone(), vec![ColumnType::Tags])
    };
    let mut ix = serving(cfg);
    let mut m = Model::default();
    let mut held: Vec<(Pin, Point)> = Vec::new();
    let mut image = 0;
    for _ in 0..g.below(50) {
        match g.below(12) {
            0..=4 => {
                let n = 1 + g.below(3);
                let changes = (0..n)
                    .map(|_| {
                        image += 1;
                        (g.below(8), row(g, &kind, image))
                    })
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect();
                let page = m.page(changes, g.bool());
                ix.apply(RANGE, &page).map_err(|e| format!("{e}"))?;
            }
            5 => {
                let page = m.close();
                ix.apply(RANGE, &page).map_err(|e| format!("{e}"))?;
            }
            6 | 7 => {
                for _ in 0..1 + g.below(4) {
                    ix.maintain().map_err(|e| format!("{e}"))?;
                }
            }
            8 => {
                let s = ix.status(RANGE).unwrap();
                let low = s.low + g.below(s.applied - s.low + 1);
                ix.advance_low(RANGE, low).map_err(|e| format!("{e}"))?;
            }
            9 if held.len() < 3 => {
                let s = ix.status(RANGE).unwrap();
                let pos = s.low + g.below(s.applied - s.low + 1);
                let pin = ix.pin(&[(RANGE, ReadAt::Position(pos))]).unwrap();
                held.push((
                    pin,
                    Point {
                        pos,
                        cut: CommitTs::MAX,
                    },
                ));
            }
            _ => {
                let s = ix.status(RANGE).unwrap();
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
                    _ if s.closed >= s.bound && s.closed > 0 => {
                        let ts = s.bound + g.below(s.closed - s.bound + 1);
                        (
                            ReadAt::Cut(ts),
                            Point {
                                pos: s.applied,
                                cut: ts,
                            },
                        )
                    }
                    _ => continue,
                };
                let pin = ix
                    .pin(&[(RANGE, read)])
                    .map_err(|e| format!("pin {read:?}: {e}"))?;
                let q = query(g, &kind);
                compare(&ix, &kind, &m, &pin, point, &q)?;
                for (pin, point) in &held {
                    compare(&ix, &kind, &m, pin, *point, &q)?;
                }
                if g.chance(1, 3) {
                    held.clear();
                }
            }
        }
    }
    // The local counts are those of the newest rows.
    if let Kind::Text(_) = kind {
        let probe = "apple bread honey eggs";
        let want = stats(&kind, &m, &match_query(probe, 1));
        let got = ix.counts(&[RANGE], probe).map_err(|e| format!("{e}"))?;
        if Some(&got) != want.as_ref() {
            return Err(format!("counts {got:?} != {want:?}"));
        }
    }
    let s = ix.status(RANGE).unwrap();
    let pin = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).unwrap();
    for _ in 0..4 {
        let q = query(g, &kind);
        compare(
            &ix,
            &kind,
            &m,
            &pin,
            Point {
                pos: s.applied,
                cut: CommitTs::MAX,
            },
            &q,
        )?;
    }
    Ok(())
}

#[test]
fn vs07_every_operation_equals_brute_force() {
    check_with(
        Runs {
            cases: 400,
            ..Runs::default()
        },
        "vs07_every_operation_equals_brute_force",
        run,
    );
}

/// The mutants of visibility and retention, each caught by the same property.
#[test]
fn vs07_brute_force_catches_broken_visibility_and_merges() {
    for mutant in [
        "show_superseded",
        "merge_copies_current_only",
        "free_without_wait",
    ] {
        let caught = with_mutant(mutant, || {
            (0..400).any(|seed| run(&mut Gen::from_seed(seed)).is_err())
        });
        assert!(caught, "{mutant} is not caught");
    }
}

/// A corpus of Zipf-distributed words, so that common words have long lists.
fn corpus(n: u64, words: usize, len: usize) -> Vec<String> {
    let mut g = Gen::from_seed(7);
    let zipf = |g: &mut Gen| {
        // Inverse of a 1/x distribution over `words` ranks.
        let u = (g.below(1 << 30) as f64 + 1.0) / f64::from(1 << 30);
        ((words as f64).powf(u) as usize).saturating_sub(1)
    };
    (0..n)
        .map(|_| {
            (0..1 + g.below(len as u64))
                .map(|_| format!("t{}", zipf(&mut g)))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[test]
fn vs07_maxscore_skips_most_rows_and_equals_exhaustive() {
    let cfg = Config {
        segment_rows: 1000,
        ..Config::new(Kind::Text("simple".into()), vec![ColumnType::Tags])
    };
    let mut ix = serving(cfg);
    let mut m = Model::default();
    let docs = corpus(3000, 400, 30);
    let rows = docs
        .iter()
        .enumerate()
        .map(|(i, d)| (i as u64, Some(text(d, vec![tags(&[])], i as u64))))
        .collect();
    let page = m.page(rows, false);
    ix.apply(RANGE, &page).unwrap();
    let pin = latest(&mut ix);
    let (mut pruned, mut all) = (0, 0);
    for (i, probe) in ["t0 t1 t2", "t0 t5", "t1 t30 t200", "t3 t3 t9 t0", "t2"]
        .iter()
        .enumerate()
    {
        for limit in [1, 10, 100] {
            let mut q = match_query(probe, limit);
            let mut scan = ix.scan(&pin, &q).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            pruned += scan.considered;
            let fast = ix.finish(scan).unwrap();
            q.exact = true;
            let mut scan = ix.scan(&pin, &q).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            all += scan.considered;
            let slow = ix.finish(scan).unwrap();
            assert_eq!(fast, slow, "query {i} limit {limit}");
            let want = brute(&ix.cfg.kind, &m.visible(m.now()), &q, None);
            assert_eq!(keys_and_scores(&fast), want, "query {i} limit {limit}");
        }
    }
    assert!(
        pruned * 4 < all * 3,
        "MaxScore looked at {pruned} documents, exhaustive at {all}"
    );
}

#[test]
fn vs07_sparse_maxscore_skips_most_rows_and_equals_exhaustive() {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    let mut g = Gen::from_seed(11);
    let rows = (0..3000u64)
        .map(|i| {
            let mut pairs: BTreeMap<u32, f32> = BTreeMap::new();
            for _ in 0..1 + g.below(20) {
                let index = (g.below(50) * g.below(50) / 25) as u32;
                pairs.insert(index, (g.below(1000) as f32 - 100.0) / 100.0);
            }
            (
                i,
                Some(sparse(
                    &pairs.into_iter().collect::<Vec<_>>(),
                    vec![tags(&[])],
                    i,
                )),
            )
        })
        .collect();
    let page = m.page(rows, false);
    ix.apply(RANGE, &page).unwrap();
    let pin = latest(&mut ix);
    let (mut pruned, mut all) = (0, 0);
    for op in [Op::Dot, Op::Cosine] {
        for probe in [
            &[(0, 1.0), (1, 0.5)][..],
            &[(0, 0.2), (3, 1.0), (40, 2.0)],
            &[(2, -1.0), (5, 1.0)],
        ] {
            let mut q = sparse_query(op, probe, 10);
            let mut scan = ix.scan(&pin, &q).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            pruned += scan.considered;
            let fast = ix.finish(scan).unwrap();
            q.exact = true;
            let mut scan = ix.scan(&pin, &q).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            all += scan.considered;
            assert_eq!(fast, ix.finish(scan).unwrap(), "{op:?} {probe:?}");
            assert_eq!(
                keys_and_scores(&fast),
                brute(&Kind::Sparse, &m.visible(m.now()), &q, None)
            );
        }
    }
    assert!(
        pruned * 4 < all * 3,
        "MaxScore looked at {pruned} documents, exhaustive at {all}"
    );
}

/// Not a gate: prints what pruning saves on 20,000 rows. `cargo test --release -- --ignored vs19`.
#[test]
#[ignore]
fn vs19_micro_benchmark_of_maxscore() {
    let mut ix = serving(Config::new(
        Kind::Text("simple".into()),
        vec![ColumnType::Tags],
    ));
    let mut m = Model::default();
    let docs = corpus(20_000, 5000, 60);
    let rows = docs
        .iter()
        .enumerate()
        .map(|(i, d)| (i as u64, Some(text(d, vec![tags(&[])], i as u64))))
        .collect();
    let start = nabedra_rt::time::now_std();
    ix.apply(RANGE, &m.page(rows, false)).unwrap();
    let load = nabedra_rt::time::elapsed(start);
    let pin = latest(&mut ix);
    let probes: Vec<String> = (0..200)
        .map(|i| format!("t{} t{} t{}", i % 7, i % 50 + 3, i * 13 % 900 + 10))
        .collect();
    for exact in [false, true] {
        let start = nabedra_rt::time::now_std();
        let mut considered = 0;
        for p in &probes {
            let q = Query {
                exact,
                ..match_query(p, 10)
            };
            let mut scan = ix.scan(&pin, &q).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            considered += scan.considered;
            ix.finish(scan).unwrap();
        }
        let t = nabedra_rt::time::elapsed(start);
        println!(
            "exact={exact}: {} queries in {t:?}, {:?} each, {considered} documents looked at; load {load:?}",
            probes.len(),
            t / probes.len() as u32
        );
    }
}

/// Rows whose large parts cancel, beside rows of small parts only: a bound summed in another order
/// than the score must not lose the row whose residue is best (review G, B1).
fn cancelling(g: &mut Gen) -> std::result::Result<(), String> {
    let big = 2f32.powi(5 + g.below(30) as i32);
    let mut ix = serving(Config {
        segment_rows: 1 + g.below(8) as usize,
        ..sparse_cfg()
    });
    let mut m = Model::default();
    let n = 2 + g.below(12);
    let rows = (0..n)
        .map(|k| {
            let tiny = 2f32.powi(-(g.below(40) as i32)) * (1.0 + g.below(1000) as f32 / 1000.0);
            let pairs: Vec<(u32, f32)> = match g.below(3) {
                0 => vec![(0, big), (1, -big), (2, tiny)],
                1 => vec![(0, -big), (1, big), (2, tiny), (50, big)],
                _ => vec![(2, tiny), (50, big)],
            };
            (k, Some(sparse(&pairs, vec![tags(&[])], k)))
        })
        .collect();
    let page = m.page(rows, false);
    ix.apply(RANGE, &page).map_err(|e| e.to_string())?;
    let pin = latest(&mut ix);
    for op in [Op::Dot, Op::Cosine] {
        for limit in [1, 2, 3] {
            let q = sparse_query(op, &[(0, big), (1, big), (2, 1.0)], limit);
            let want = brute(&Kind::Sparse, &m.visible(m.now()), &q, None);
            for exact in [false, true] {
                let q = Query { exact, ..q.clone() };
                let got = keys_and_scores(&ix.search(&pin, &q).map_err(|e| e.to_string())?);
                if got != want {
                    return Err(format!("{op:?} limit {limit} exact {exact}:\n got {got:?}\nwant {want:?}"));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn vs07_cancelling_parts_equal_brute_force() {
    check_with(Runs { cases: 600, ..Runs::default() }, "vs07_cancelling_parts_equal_brute_force", cancelling);
}

#[test]
fn vs07_a_slack_of_the_bound_alone_is_caught() {
    let caught = with_mutant("slack_of_the_bound", || {
        (0..600).any(|seed| cancelling(&mut Gen::from_seed(seed)).is_err())
    });
    assert!(caught);
}
