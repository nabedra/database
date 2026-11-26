//! One rule per test, each with the mutant that breaks it where there is one.

use super::*;

fn feed(ix: &mut ImpactIndex, m: &mut Model, changes: Vec<(u64, Option<Row>)>) {
    let page = m.page(changes, false);
    ix.apply(RANGE, &page).unwrap();
}

fn found(ix: &mut ImpactIndex, q: &Query) -> Vec<u64> {
    let pin = latest(ix);
    images(&ix.search(&pin, q).unwrap())
}

fn hits(ix: &mut ImpactIndex, q: &Query) -> Vec<(u64, f32)> {
    let pin = latest(ix);
    keys_and_scores(&ix.search(&pin, q).unwrap())
}

fn small(cfg: Config) -> Config {
    Config {
        segment_rows: 3,
        merge_factor: 2,
        ..cfg
    }
}

#[test]
fn vs05_updates_and_deletes_show_at_once_and_old_cuts_keep_theirs() {
    let mut ix = serving(small(text_cfg()));
    let mut m = Model::default();
    let none = || vec![tags(&[])];
    feed(
        &mut ix,
        &mut m,
        vec![
            (1, Some(text("red apple", none(), 1))),
            (2, Some(text("green apple", none(), 2))),
        ],
    );
    let before = ix.status(RANGE).unwrap().applied;
    feed(
        &mut ix,
        &mut m,
        vec![(1, Some(text("blue sky", none(), 3))), (2, None)],
    );
    let q = match_query("apple", 10);
    assert_eq!(hits(&mut ix, &q), []);
    let sky = hits(&mut ix, &match_query("sky", 10));
    assert_eq!(sky.iter().map(|h| h.0).collect::<Vec<_>>(), [1]);
    let old = ix.pin(&[(RANGE, ReadAt::Position(before))]).unwrap();
    let mut got = images(&ix.search(&old, &q).unwrap());
    got.sort_unstable();
    assert_eq!(got, [1, 2]);
    // Retention moves on: the old cut is refused, and merges drop what nobody can see.
    drop(old);
    ix.advance_low(RANGE, ix.status(RANGE).unwrap().applied)
        .unwrap();
    assert_eq!(
        ix.pin(&[(RANGE, ReadAt::Position(before))]).err(),
        Some(Error::CutTooOld { oldest: 4 })
    );
    ix.compact().unwrap();
    assert_eq!(ix.rows.live(), 1);
    assert_eq!(ix.terms(), 2, "blue and sky");
}

#[test]
fn vs05_a_row_without_a_value_supersedes_its_old_one() {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    feed(
        &mut ix,
        &mut m,
        vec![(1, Some(sparse(&[(3, 1.0)], vec![tags(&[])], 1)))],
    );
    let null = Row {
        doc: None,
        cols: vec![tags(&[])],
        image: Arc::new([]),
    };
    feed(
        &mut ix,
        &mut m,
        vec![(1, Some(null.clone())), (9, Some(null))],
    );
    assert_eq!(hits(&mut ix, &sparse_query(Op::Dot, &[(3, 1.0)], 5)), []);
    assert_eq!(
        ix.rows.live(),
        1,
        "a row without a value is kept only to supersede"
    );
}

/// Rows that match the filter score worst, so a result filtered after ranking would miss them.
fn filtered() -> std::result::Result<(), String> {
    let mut ix = serving(small(text_cfg()));
    let mut m = Model::default();
    let rows = (0..20)
        .map(|k| {
            let t = if k < 5 {
                "fox ".to_owned() + &"dog ".repeat(20)
            } else {
                "fox ".repeat(5)
            };
            let tag = if k < 5 { "x" } else { "y" };
            (k, Some(text(&t, vec![tags(&[tag])], k)))
        })
        .collect();
    feed(&mut ix, &mut m, rows);
    let q = Query {
        filter: Some(Filter::Contains(0, "x".into())),
        ..match_query("fox", 5)
    };
    let got: Vec<u64> = hits(&mut ix, &q).iter().map(|h| h.0).collect();
    if got != [0, 1, 2, 3, 4] {
        return Err(format!("filtered: {got:?}"));
    }
    Ok(())
}

#[test]
fn vs07_a_filter_decides_before_a_row_takes_a_place() {
    filtered().unwrap();
    assert!(with_mutant("post_filter", filtered).is_err());
}

/// Two workers that hold one range each score like one that holds both, with the gateway's counts.
fn table_wide() -> std::result::Result<(), String> {
    let docs = [
        "cat dog",
        "cat",
        "cat bird",
        "dog dog dog fish",
        "bird",
        "fish fish",
    ];
    let one = |ranges: &[(RangeId, &[usize])]| {
        let mut ix = ImpactIndex::new(text_cfg(), RamStore::default(), None).unwrap();
        for (id, ds) in ranges {
            ix.begin_load(*id, 1, 0).unwrap();
            let rows: Vec<_> = (ds.iter())
                .map(|&d| (key(d as u64), 5, text(docs[d], vec![tags(&[])], d as u64)))
                .collect();
            ix.load_rows(*id, &rows).unwrap();
            ix.finish_load(*id).unwrap();
            ix.caught_up(*id, 1).unwrap();
        }
        ix
    };
    let (a, b) = (RangeId(1), RangeId(2));
    let mut both = one(&[(a, &[0, 1, 2]), (b, &[3, 4, 5])]);
    let mut left = one(&[(a, &[0, 1, 2])]);
    let mut right = one(&[(b, &[3, 4, 5])]);
    let q = "dog fish";
    let mut stats = left.counts(&[a], q).unwrap();
    stats.add(&right.counts(&[b], q).unwrap()).unwrap();
    assert_eq!(stats, both.counts(&[a, b], q).unwrap());
    let query = Query {
        stats: Some(stats),
        ..match_query(q, 10)
    };
    let search = |ix: &mut ImpactIndex, ranges: &[RangeId]| {
        let at: Vec<_> = ranges
            .iter()
            .map(|r| (*r, ReadAt::Latest { barrier: 0 }))
            .collect();
        let pin = ix.pin(&at).unwrap();
        keys_and_scores(&ix.search(&pin, &query).unwrap())
    };
    let mut split = search(&mut left, &[a]);
    split.extend(search(&mut right, &[b]));
    split.sort_by(|x, y| y.1.total_cmp(&x.1).then(x.0.cmp(&y.0)));
    let whole = search(&mut both, &[a, b]);
    if split != whole {
        return Err(format!("{split:?} != {whole:?}"));
    }
    Ok(())
}

#[test]
fn vs07_workers_score_alike_with_table_wide_counts() {
    table_wide().unwrap();
    assert!(with_mutant("local_stats", table_wide).is_err());
}

fn copied_entry() -> std::result::Result<(), String> {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    feed(
        &mut ix,
        &mut m,
        vec![(1, Some(sparse(&[(0, 1.0)], vec![tags(&[])], 1)))],
    );
    // A copy of the entry in a second segment, as a move would leave it for a moment.
    let seg = *ix.segments.keys().next().unwrap();
    let e = ix.segments[&seg].entries[0];
    let terms = ix
        .rows
        .version(e.slot, e.at)
        .unwrap()
        .row
        .clone()
        .unwrap()
        .terms
        .clone();
    let other = ix.new_segment();
    ix.rows.name(e.slot, e.at);
    ix.append(other, e, &terms).unwrap();
    let got = hits(&mut ix, &sparse_query(Op::Dot, &[(0, 1.0)], 10));
    if got.len() != 1 {
        return Err(format!("{got:?}"));
    }
    Ok(())
}

#[test]
fn vs07_a_row_with_two_entries_ranks_once() {
    copied_entry().unwrap();
    assert!(with_mutant("no_slot_dedup", copied_entry).is_err());
}

/// A merge while a snapshot is being installed keeps what the snapshot appended beyond the applied
/// position.
fn merge_during_load() -> std::result::Result<(), String> {
    let mut ix = ImpactIndex::new(small(text_cfg()), RamStore::default(), None).unwrap();
    ix.begin_load(RANGE, 9, 0).unwrap();
    let rows: Vec<_> = (0..7)
        .map(|k| (key(k), 50 + k, text("word", vec![tags(&[])], k)))
        .collect();
    ix.load_rows(RANGE, &rows).unwrap();
    ix.compact().map_err(|e| e.to_string())?;
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 9).unwrap();
    let got = hits(&mut ix, &match_query("word", 10));
    if got.len() != 7 {
        return Err(format!("{got:?}"));
    }
    Ok(())
}

#[test]
fn vs07_a_merge_keeps_entries_beyond_the_applied_position() {
    merge_during_load().unwrap();
    assert!(with_mutant("merge_ignores_unapplied", merge_during_load).is_err());
}

/// A pin reads the segments it listed, also after they were merged into others.
fn pin_across_merges() -> std::result::Result<(), String> {
    let mut ix = serving(small(text_cfg()));
    let mut m = Model::default();
    let rows = (0..9)
        .map(|k| (k, Some(text("tree", vec![tags(&[])], k))))
        .collect();
    feed(&mut ix, &mut m, rows);
    let pin = latest(&mut ix);
    let more = (0..9)
        .map(|k| (k, Some(text("leaf", vec![tags(&[])], 100 + k))))
        .collect();
    feed(&mut ix, &mut m, more);
    ix.advance_low(RANGE, ix.status(RANGE).unwrap().applied)
        .unwrap();
    ix.compact().map_err(|e| e.to_string())?;
    let got = ix
        .search(&pin, &match_query("tree", 20))
        .map_err(|e| e.to_string())?;
    if images(&got).len() != 9 {
        return Err(format!("{:?}", images(&got)));
    }
    drop(pin);
    ix.maintain().map_err(|e| e.to_string())?;
    if ix.segments() != (1, 0) || ix.rows.live() != 9 {
        return Err(format!("not freed: {:?}", ix.segments()));
    }
    Ok(())
}

#[test]
fn vs07_a_pin_keeps_its_segments_across_merges() {
    pin_across_merges().unwrap();
    assert!(with_mutant("free_without_wait", pin_across_merges).is_err());
}

/// A list of negative contributions bounds a row by 0, not by its own negative maximum.
fn negative_weights() -> std::result::Result<(), String> {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    let mut rows: Vec<_> = (0..5)
        .map(|k| (k, Some(sparse(&[(1, 0.5)], vec![tags(&[])], k))))
        .collect();
    rows.push((5, Some(sparse(&[(0, 1.0)], vec![tags(&[])], 5))));
    rows.push((6, Some(sparse(&[(1, 0.6)], vec![tags(&[])], 6))));
    feed(&mut ix, &mut m, rows);
    let q = sparse_query(Op::Dot, &[(0, -1.0), (1, 1.0)], 5);
    let got: Vec<u64> = hits(&mut ix, &q).iter().map(|h| h.0).collect();
    if got != [6, 0, 1, 2, 3] {
        return Err(format!("{got:?}"));
    }
    Ok(())
}

#[test]
fn vs07_negative_weights_are_bounded_by_zero() {
    negative_weights().unwrap();
    assert!(with_mutant("maxscore_unclamped", negative_weights).is_err());
}

#[test]
fn vs03_rankings_of_several_columns_share_one_position() {
    let mut words = serving(text_cfg());
    let mut weights = serving(sparse_cfg());
    let (mut m1, mut m2) = (Model::default(), Model::default());
    feed(
        &mut words,
        &mut m1,
        vec![(1, Some(text("one", vec![tags(&[])], 1)))],
    );
    feed(
        &mut weights,
        &mut m2,
        vec![(1, Some(sparse(&[(0, 1.0)], vec![tags(&[])], 1)))],
    );
    feed(
        &mut words,
        &mut m1,
        vec![(2, Some(text("one", vec![tags(&[])], 2)))],
    );
    // The worker pins each column at the position that every column applied.
    let at = words
        .status(RANGE)
        .unwrap()
        .applied
        .min(weights.status(RANGE).unwrap().applied);
    let p1 = words.pin(&[(RANGE, ReadAt::Position(at))]).unwrap();
    let p2 = weights.pin(&[(RANGE, ReadAt::Position(at))]).unwrap();
    assert_eq!(
        images(&words.search(&p1, &match_query("one", 5)).unwrap()),
        [1]
    );
    assert_eq!(
        images(
            &weights
                .search(&p2, &sparse_query(Op::Dot, &[(0, 1.0)], 5))
                .unwrap()
        ),
        [1]
    );
}

#[test]
fn vs14_a_drop_fails_its_pins_and_hides_its_entries() {
    let mut ix = serving(small(text_cfg()));
    let mut m = Model::default();
    feed(
        &mut ix,
        &mut m,
        (0..7)
            .map(|k| (k, Some(text("stone", vec![tags(&[])], k))))
            .collect(),
    );
    let pin = latest(&mut ix);
    let mut scan = ix.scan(&pin, &match_query("stone", 10)).unwrap();
    ix.drop_range(RANGE).unwrap();
    assert_eq!(ix.visit(&mut scan).err(), Some(Error::PinFailed));
    assert_eq!(
        ix.search(&pin, &match_query("stone", 10)).err(),
        Some(Error::PinFailed)
    );
    assert_eq!(
        ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]).err(),
        Some(Error::NotHeld(RANGE))
    );
    // Installed again from a snapshot without those rows: none of the old entries shows.
    ix.begin_load(RANGE, 20, 0).unwrap();
    ix.load_rows(RANGE, &[(key(50), 3, text("stone", vec![tags(&[])], 50))])
        .unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 20).unwrap();
    assert_eq!(found(&mut ix, &match_query("stone", 10)), [50]);
    drop((pin, scan));
    ix.compact().unwrap();
    assert_eq!((ix.rows.live(), ix.segments()), (1, (1, 0)));
    assert_eq!(
        ix.counts(&[RANGE], "stone").unwrap(),
        Stats {
            docs: 1,
            words: 1,
            df: vec![1]
        }
    );
}

#[test]
fn vs14_a_range_serves_only_after_a_fresh_barrier() {
    let mut ix = ImpactIndex::new(text_cfg(), RamStore::default(), None).unwrap();
    ix.begin_load(RANGE, 5, 0).unwrap();
    assert!(matches!(
        ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]),
        Err(Error::Behind { .. })
    ));
    assert_eq!(
        ix.apply(
            RANGE,
            &Page {
                after: 5,
                through: 6,
                records: vec![]
            }
        )
        .err(),
        Some(Error::Gap { delivered: 5 })
    );
    ix.finish_load(RANGE).unwrap();
    assert!(matches!(
        ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]),
        Err(Error::Behind { .. })
    ));
    assert!(matches!(ix.caught_up(RANGE, 6), Err(Error::Behind { .. })));
    ix.caught_up(RANGE, 5).unwrap();
    assert!(matches!(
        ix.pin(&[(RANGE, ReadAt::Latest { barrier: 6 })]),
        Err(Error::Behind { .. })
    ));
    assert_eq!(
        ix.pin(&[(RANGE, ReadAt::Position(4))]).err(),
        Some(Error::CutTooOld { oldest: 5 })
    );
    assert_eq!(
        ix.pin(&[(RANGE, ReadAt::Position(6))]).err(),
        Some(Error::CutTooNew { newest: 5 })
    );
    assert_eq!(
        ix.pin(&[(RANGE, ReadAt::Cut(1))]).err(),
        Some(Error::CutTooNew { newest: 0 })
    );
    let closed = Page {
        after: 5,
        through: 7,
        records: vec![Record {
            at: 7,
            change: Change::Closed(70),
        }],
    };
    assert_eq!(ix.apply(RANGE, &closed), Ok(7));
    assert!(ix.pin(&[(RANGE, ReadAt::Cut(70))]).is_ok());
    // A page that skips is refused, one that repeats is taken again.
    assert_eq!(
        ix.apply(
            RANGE,
            &Page {
                after: 8,
                through: 9,
                records: vec![]
            }
        )
        .err(),
        Some(Error::Gap { delivered: 7 })
    );
    assert_eq!(ix.apply(RANGE, &closed), Ok(7));
    let unordered = Page {
        after: 7,
        through: 9,
        records: vec![
            Record {
                at: 9,
                change: Change::Closed(80),
            },
            Record {
                at: 8,
                change: Change::Closed(80),
            },
        ],
    };
    assert_eq!(
        ix.apply(RANGE, &unordered).err(),
        Some(Error::InvalidFeed("a page is not in log order"))
    );
    // A version out of timestamp order takes the range out of service.
    let row = |ts| Record {
        at: 8,
        change: Change::Row {
            key: key(1),
            commit_ts: ts,
            row: Some(text("a", vec![tags(&[])], 1)),
        },
    };
    let page = Page {
        after: 7,
        through: 8,
        records: vec![row(90), row(80)],
    };
    assert!(matches!(ix.apply(RANGE, &page), Err(Error::InvalidFeed(_))));
    assert!(matches!(
        ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })]),
        Err(Error::OutOfService { .. })
    ));
    assert!(ix.status(RANGE).unwrap().damage.is_some());
}

#[test]
fn vs17_documents_at_and_beyond_every_limit() {
    let mut ix = serving(text_cfg());
    let mut m = Model::default();
    let huge_word = "lorem ".repeat(180_000);
    let many: String = (0..60_000).map(|i| format!("w{i} ")).collect();
    let odd = "\u{fffd}\u{0}\u{200d}👩‍👩‍👧 İstanbul ΟΔΟΣ 中文 \u{202e}abc";
    feed(
        &mut ix,
        &mut m,
        vec![
            (1, Some(text(&huge_word, vec![tags(&[])], 1))),
            (2, Some(text(&many, vec![tags(&[])], 2))),
            (3, Some(text("", vec![tags(&[])], 3))),
            (4, Some(text(odd, vec![tags(&[])], 4))),
            (5, Some(text(" .,;", vec![tags(&[])], 5))),
        ],
    );
    assert_eq!(found(&mut ix, &match_query("lorem", 5)), [1]);
    assert_eq!(found(&mut ix, &match_query("w59999 W7", 5)), [2]);
    assert_eq!(found(&mut ix, &match_query("İSTANBUL ΟΔΟΣ", 5)), [4]);
    assert!(found(&mut ix, &match_query("", 5)).is_empty());
    assert_eq!(
        ix.counts(&[RANGE], "lorem").unwrap().docs,
        5,
        "an empty text is a row with 0 words"
    );
    // Queries beyond the limits are refused before they run.
    let pin = latest(&mut ix);
    let wide: String = (0..MAX_QUERY_TERMS + 1).map(|i| format!("q{i} ")).collect();
    let refused = |q: Query| matches!(ix.search(&pin, &q), Err(Error::InvalidQuery(_)));
    assert!(refused(match_query(&wide, 5)));
    assert!(refused(match_query("lorem", 0)));
    assert!(refused(match_query("lorem", MAX_LIMIT + 1)));
    assert!(
        refused(sparse_query(Op::Dot, &[(1, 1.0)], 5)),
        "the column is text"
    );
    assert!(
        refused(Query {
            stats: Some(Stats::default()),
            ..match_query("lorem", 5)
        }),
        "counts of another query"
    );
    assert!(refused(Query {
        filter: Some(Filter::Contains(3, "x".into())),
        ..match_query("lorem", 5)
    }));
}

#[test]
fn vs17_sparse_values_at_and_beyond_every_limit() {
    let mut ix = serving(sparse_cfg());
    let mut m = Model::default();
    let wide: Vec<(u32, f32)> = (0..100_000).map(|i| (i * 3, 1.0)).collect();
    feed(
        &mut ix,
        &mut m,
        vec![
            (
                1,
                Some(sparse(&[(0, f32::MAX), (1, f32::MAX)], vec![tags(&[])], 1)),
            ),
            (
                2,
                Some(sparse(&[(0, f32::MIN_POSITIVE / 8.0)], vec![tags(&[])], 2)),
            ),
            (3, Some(sparse(&wide, vec![tags(&[])], 3))),
            (4, Some(sparse(&[], vec![tags(&[])], 4))),
            (5, Some(sparse(&[(0, 0.0), (7, -0.0)], vec![tags(&[])], 5))),
            (6, Some(sparse(&[(u32::MAX, -3.0)], vec![tags(&[])], 6))),
        ],
    );
    let pin = latest(&mut ix);
    let dot = ix
        .search(&pin, &sparse_query(Op::Dot, &[(0, f32::MAX), (1, 1.0)], 5))
        .unwrap();
    assert_eq!(images(&dot), [1, 3, 2]);
    assert_eq!(
        dot[0].score,
        f32::MAX,
        "a score beyond f32 is the largest one"
    );
    let cos = ix
        .search(&pin, &sparse_query(Op::Cosine, &[(0, 1.0), (1, 1.0)], 5))
        .unwrap();
    assert!(cos.iter().all(|h| (-1.0..=1.0).contains(&h.score)));
    assert_eq!(
        images(
            &ix.search(&pin, &sparse_query(Op::Dot, &[(u32::MAX, 1.0)], 5))
                .unwrap()
        ),
        [6]
    );
    assert_eq!(
        ix.search(&pin, &sparse_query(Op::Dot, &[(7, 1.0)], 5))
            .unwrap(),
        [],
        "a zero weight is no term"
    );
    let refused = |q: Query| matches!(ix.search(&pin, &q), Err(Error::InvalidQuery(_)));
    assert!(
        refused(sparse_query(Op::Cosine, &[(0, 0.0)], 5)),
        "a zero vector has no cosine"
    );
    assert!(
        refused(sparse_query(Op::Dot, &[(2, 1.0), (1, 1.0)], 5)),
        "indices out of order"
    );
    assert!(refused(sparse_query(Op::Dot, &[(1, f32::NAN)], 5)));
    assert!(refused(match_query("a", 5)), "the column is sparse");
    // A value that breaks the rules takes the range out of service.
    let mut bad = sparse(&[(1, 1.0)], vec![tags(&[])], 9);
    bad.doc = Some(Doc::Sparse {
        indices: Arc::new([2, 1]),
        weights: Arc::new([1.0, 1.0]),
    });
    let page = m.page(vec![(9, Some(bad))], false);
    assert!(matches!(ix.apply(RANGE, &page), Err(Error::InvalidFeed(_))));
}

#[test]
fn vs11_memory_returns_after_churn() {
    let mut ix = serving(Config {
        segment_rows: 16,
        ..small(text_cfg())
    });
    let mut m = Model::default();
    let mut most = (0, 0, 0);
    for round in 0..10u64 {
        let rows = (0..150)
            .map(|k| {
                (
                    k,
                    Some(text(&format!("r{round} k{k} common"), vec![tags(&[])], k)),
                )
            })
            .collect();
        feed(&mut ix, &mut m, rows);
        ix.advance_low(RANGE, ix.status(RANGE).unwrap().applied)
            .unwrap();
        settle(&mut ix);
        most = most.max((ix.store.blocks(), ix.segments().0, ix.terms()));
    }
    assert!(most.0 > 0, "full blocks went to the store");
    assert!(
        ix.terms() <= 152,
        "the dictionary holds the newest terms only: {}",
        ix.terms()
    );
    feed(&mut ix, &mut m, (0..150).map(|k| (k, None)).collect());
    ix.advance_low(RANGE, ix.status(RANGE).unwrap().applied)
        .unwrap();
    settle(&mut ix);
    assert_eq!(ix.rows.live(), 0);
    assert_eq!(ix.terms(), 0);
    assert_eq!(ix.store.blocks(), 0);
    assert_eq!(ix.segments().1, 0);
    assert!(ix.segments().0 <= 1, "{:?}", ix.segments());
}

/// Review G, B1: key 0's large parts cancel and leave 0.00105, which beats key 1's 0.001.
fn cancelling_parts() -> std::result::Result<(), String> {
    let big = (1u32 << 20) as f32;
    for op in [Op::Dot, Op::Cosine] {
        let mut ix = serving(sparse_cfg());
        let mut m = Model::default();
        feed(&mut ix, &mut m, vec![
            (1, Some(sparse(&[(2, 0.001)], vec![tags(&[])], 1))),
            (0, Some(sparse(&[(0, big), (1, -big), (2, 0.00105)], vec![tags(&[])], 0))),
        ]);
        let pin = latest(&mut ix);
        for exact in [false, true] {
            let q = Query {
                exact,
                ..sparse_query(op, &[(0, big), (1, big), (2, 1.0)], 1)
            };
            let got = keys_and_scores(&ix.search(&pin, &q).unwrap());
            let want = brute(&Kind::Sparse, &m.visible(m.now()), &q, None);
            if got != want || (op == Op::Dot && got.first().map(|h| h.0) != Some(0)) {
                return Err(format!("{op:?} exact {exact}: {got:?}, brute force {want:?}"));
            }
        }
    }
    Ok(())
}

#[test]
fn vs07_cancelling_sparse_parts_keep_the_best_row() {
    cancelling_parts().unwrap();
    assert!(with_mutant("slack_of_the_bound", cancelling_parts).is_err());
}
