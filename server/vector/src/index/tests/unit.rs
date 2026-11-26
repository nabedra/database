//! Rules of single parts: trees, score bounds, the feed, pins, retention and the row map.

use std::collections::BTreeSet;

use nabedra_check::Gen;
use nabedra_check::prop::check;

use super::super::checkpoint::crc32c;
use super::super::codec::{direction, dot, exact, goodness, optimistic};
use super::super::shard::{band_of, band_span};
use super::super::tree::{Walk, mean};
use super::*;

fn unit(g: &mut Gen, dim: usize) -> Vec<f32> {
    loop {
        let v: Vec<f32> = (0..dim).map(|_| g.range_i64(-50, 50) as f32).collect();
        if let Some((dir, _)) = direction(&v) {
            return dir;
        }
        g.u64();
    }
}

fn leaf(part: u32, centroid: Vec<f32>) -> Leaf {
    Leaf {
        part: PartId(part),
        centroid: centroid.into(),
        ready: true,
    }
}

/// A tree lists what was put in, balanced, and a root that was cloned before never changes.
#[test]
fn vs07_a_tree_is_balanced_and_a_pinned_one_does_not_change() {
    let g = &mut Gen::from_seed(3);
    let mut tree = Tree::default();
    let mut there = BTreeSet::new();
    let mut pins: Vec<(Tree, BTreeSet<PartId>)> = Vec::new();
    for part in 0..3000 {
        tree.insert(leaf(part, unit(g, 6)));
        there.insert(PartId(part));
        if g.chance(1, 5) {
            let gone = *g.pick(&there.iter().copied().collect::<Vec<_>>());
            tree.remove(gone);
            there.remove(&gone);
        }
        if part % 500 == 0 {
            pins.push((tree.clone(), there.clone()));
        }
    }
    pins.push((tree.clone(), there));
    for (tree, want) in &pins {
        assert_eq!(&tree.parts().into_iter().collect::<BTreeSet<_>>(), want);
        // A walk meets every leaf once, the nearest ones early.
        let q = unit(g, 6);
        let mut walk = Walk::new(q.clone(), 4);
        walk.add(tree);
        let order: Vec<(PartId, f32)> = std::iter::from_fn(|| walk.next())
            .map(|(l, _)| (l.part, dot(&l.centroid, &q)))
            .collect();
        assert_eq!(
            &order.iter().map(|(p, _)| *p).collect::<BTreeSet<_>>(),
            want
        );
        assert_eq!(order.len(), want.len());
    }
    // Two levels of at most 128 children hold 3,000 leaves.
    let (depth, widest) = tree.shape();
    assert!(depth == 2 && widest <= 128, "{depth} {widest}");
    for (_, want) in &pins {
        for part in want {
            tree.remove(*part);
        }
    }
    assert_eq!(tree.parts(), []);
}

#[test]
fn vs07_bisect_is_balanced() {
    check("vs07_bisect_is_balanced", |g| {
        let n = 1 + g.below(40) as usize;
        let same = g.chance(1, 4);
        let first = unit(g, 4);
        let points: Vec<Vec<f32>> = (0..n)
            .map(|_| if same { first.clone() } else { unit(g, 4) })
            .collect();
        let points: Vec<&[f32]> = points.iter().map(Vec::as_slice).collect();
        let (side, centroids) = bisect(&points);
        let second = side.iter().filter(|s| **s).count();
        let least = second.min(n - second);
        let unit = |c: &[f32]| (dot(c, c) - 1.0).abs() < 1e-3;
        if side.len() != n
            || (n > 1 && least < (n / 3).max(1))
            || !centroids.iter().all(|c| unit(c))
        {
            return Err(format!("{n} points: {second} on the second side"));
        }
        Ok(())
    });
    // Two groups come apart.
    let (a, b) = ([1.0, 0.0], [0.0, 1.0]);
    let points: Vec<&[f32]> = vec![&a, &b, &a, &b, &a];
    let side = bisect(&points).0;
    assert!(side[0] == side[2] && side[2] == side[4] && side[1] == side[3] && side[0] != side[1]);
    assert_eq!(mean(&[]), Vec::<f32>::new());
}

/// The bound of an estimate that holds the true cosine is never below the score.
#[test]
fn vs07_an_optimistic_score_is_never_below_the_score() {
    check("vs07_optimistic", |g| {
        let dim = 1 + g.below(8) as usize;
        let scale = |g: &mut Gen| *g.pick(&[1.0f32, 1e-3, 1e4, 1e30, 1e-30]);
        let (sa, sb) = (scale(g), scale(g));
        let a: Vec<f32> = unit(g, dim).iter().map(|x| x * sa).collect();
        let b: Vec<f32> = unit(g, dim).iter().map(|x| x * sb).collect();
        let ((da, la), (db, lb)) = (direction(&a).unwrap(), direction(&b).unwrap());
        let err = g.below(100) as f32 / 1000.0 + 1e-5;
        let cos = dot(&da, &db) + err * (g.below(2001) as f32 / 1000.0 - 1.0) * 0.99;
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            let good = goodness(op, exact(op, &a, &b).unwrap());
            let hope = optimistic(op, Estimate { cos, err }, la, lb);
            let slack = 1e-6
                * good
                    .abs()
                    .max((la + lb) as f32 * if op == Op::L2 { 1.0 } else { 0.0 });
            if good > hope + slack {
                return Err(format!("{op:?} {good} above {hope}"));
            }
        }
        Ok(())
    });
}

#[test]
fn vs07_scores_are_finite_for_every_input() {
    let (max, tiny) = ([f32::MAX; 8], [f32::from_bits(1); 8]);
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        for (a, b) in [(&max, &max), (&max, &tiny), (&tiny, &tiny)] {
            assert!(exact(op, a, b).unwrap().is_finite(), "{op:?}");
        }
    }
    assert_eq!(exact(Op::Cosine, &tiny, &tiny), Some(1.0));
    assert_eq!(exact(Op::Cosine, &tiny, &[0.0; 8]), None);
    assert!(direction(&tiny).is_none() && direction(&[0.0; 8]).is_none());
    assert!(direction(&max).is_none(), "its length is no f32");
    // Zero has one sign, so that ties go to the key.
    assert_eq!(exact(Op::Dot, &[-0.0], &[1.0]).unwrap().to_bits(), 0);
}

#[test]
fn vs07_unit_vectors_share_one_band_and_a_band_holds_its_lengths() {
    let g = &mut Gen::from_seed(5);
    for _ in 0..200 {
        let len = direction(&unit(g, 7)).unwrap().1;
        assert_eq!(band_of(len), 0);
        let len = (g.below(2000) as f64 / 10.0 - 100.0).exp2();
        let (lo, hi) = band_span(band_of(len));
        assert!(lo <= len && len <= hi, "{lo} {len} {hi}");
    }
    assert_eq!(band_span(band_of(0.0)), (0.0, 0.0));
}

#[test]
fn vs10_crc32c_is_the_castagnoli_one() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

fn one(x: f32) -> Option<Row> {
    Some(row(&[x, 1.0], Vec::new(), x as u64))
}

fn index() -> Index {
    serving(Config::new(schema(2, &[], None), 2), ExactCodec, None)
}

fn read(ix: &mut Index, at: ReadAt) -> Result<Vec<u64>> {
    let pin = ix.pin(&[(RANGE, at)], None)?;
    let mut got = images(&ix.search(&pin, &query(Op::Dot, &[1.0, 0.0], 100))?);
    got.sort_unstable();
    Ok(got)
}

const LATEST: ReadAt = ReadAt::Latest { barrier: 0 };

/// Pages may repeat and overlap. A gap, a page out of order and a row that does not fit are refused.
#[test]
fn vs02_the_feed_takes_contiguous_pages_once() {
    let mut ix = index();
    let first = page(0, vec![(1, one(1.0)), (2, one(2.0))]);
    assert_eq!(
        ix.apply(RANGE, &page(1, vec![(2, one(2.0))])),
        Err(Error::Gap { delivered: 0 })
    );
    assert_eq!(ix.apply(RANGE, &first), Ok(2));
    assert_eq!(ix.apply(RANGE, &first), Ok(2));
    ix.run();
    let overlap = page(1, vec![(2, one(9.0)), (2, one(3.0))]);
    assert_eq!(ix.apply(RANGE, &overlap), Ok(3));
    ix.run();
    assert_eq!(read(&mut ix, LATEST), Ok(vec![1, 3]));
    // Positions without a change count.
    assert_eq!(
        ix.apply(
            RANGE,
            &Page {
                after: 3,
                through: 9,
                records: Vec::new()
            }
        ),
        Ok(9)
    );
    ix.run();
    assert_eq!(ix.status(RANGE).unwrap().applied, 9);
    // A page that is not in log order is refused before anything is taken.
    let mut wrong = page(9, vec![(5, one(5.0)), (6, one(6.0))]);
    wrong.records.swap(0, 1);
    assert!(matches!(
        ix.apply(RANGE, &wrong),
        Err(Error::InvalidFeed(_))
    ));
    assert_eq!(read(&mut ix, LATEST), Ok(vec![1, 3]));
    // A row that cannot be right takes the range out of service.
    let bad = row(&[1.0, 2.0, 3.0], Vec::new(), 0);
    assert!(matches!(
        ix.apply(RANGE, &page(9, vec![(5, Some(bad))])),
        Err(Error::InvalidFeed(_))
    ));
    assert!(matches!(
        read(&mut ix, LATEST),
        Err(Error::OutOfService { .. })
    ));
}

#[test]
fn vs02_a_row_with_a_value_that_is_no_number_is_refused() {
    for bad in [f32::NAN, f32::INFINITY] {
        let mut ix = index();
        assert!(ix.apply(RANGE, &page(0, vec![(5, one(bad))])).is_err());
    }
}

/// The same key twice in one entry, and a commit timestamp that goes back.
#[test]
fn vs02_versions_of_a_key_are_in_order() {
    let mut ix = index();
    let mut twice = page(0, vec![(1, one(1.0)), (1, one(2.0))]);
    twice.records[1].at = 1;
    assert!(ix.apply(RANGE, &twice).is_err());
    let mut ix = index();
    let mut back = page(0, vec![(1, one(1.0)), (1, one(2.0))]);
    if let Change::Row { commit_ts, .. } = &mut back.records[1].change {
        *commit_ts = 10;
    }
    assert!(ix.apply(RANGE, &back).is_err());
}

/// Work in flight is bounded, and what was taken is a prefix.
#[test]
fn vs17_a_pipeline_that_is_full_takes_a_prefix_and_then_nothing() {
    let cfg = Config {
        max_flight: 3,
        queue: 2,
        ..Config::new(schema(2, &[], None), 2)
    };
    let mut ix = serving(cfg, ExactCodec, None);
    let all = page(0, (1..=10).map(|k| (k, one(k as f32))).collect());
    assert_eq!(ix.apply(RANGE, &all), Ok(3));
    assert_eq!(ix.apply(RANGE, &all), Err(Error::Busy));
    ix.advance();
    assert_eq!(ix.status(RANGE).unwrap().applied, 0);
    assert_eq!(
        read(&mut ix, LATEST),
        Ok(vec![]),
        "appended ahead of the applied position"
    );
    ix.run();
    assert_eq!(read(&mut ix, LATEST), Ok(vec![1, 2, 3]));
    feed(&mut ix, RANGE, &all);
    assert_eq!(read(&mut ix, LATEST).unwrap().len(), 10);
}

/// A pin needs a range that serves, and a barrier that was reached.
#[test]
fn vs01_a_pin_waits_for_its_barrier() {
    let mut ix = index();
    feed(&mut ix, RANGE, &page(0, vec![(1, one(1.0))]));
    assert!(read(&mut ix, ReadAt::Latest { barrier: 1 }).is_ok());
    let behind = Error::Behind {
        range: RANGE,
        applied: 1,
    };
    assert_eq!(read(&mut ix, ReadAt::Latest { barrier: 2 }), Err(behind));
    let other = RangeId(99);
    assert_eq!(
        ix.pin(&[(other, LATEST)], None).err(),
        Some(Error::NotHeld(other))
    );
    // A range that is being installed, or has not caught up, does not serve.
    ix.begin_load(RANGE, 5, 50).unwrap();
    assert!(matches!(read(&mut ix, LATEST), Err(Error::Behind { .. })));
    ix.load_rows(RANGE, &[(key(1), 10, one(4.0).unwrap())])
        .unwrap();
    assert_eq!(ix.finish_load(RANGE), Err(Error::Busy));
    ix.run();
    ix.finish_load(RANGE).unwrap();
    assert!(matches!(read(&mut ix, LATEST), Err(Error::Behind { .. })));
    assert!(matches!(ix.caught_up(RANGE, 6), Err(Error::Behind { .. })));
    ix.caught_up(RANGE, 5).unwrap();
    assert_eq!(read(&mut ix, LATEST), Ok(vec![4]));
}

/// VS09: a cut is refused below what superseded anything that was dropped, below the largest
/// timestamp of an installed snapshot, and above what the range closed.
#[test]
fn vs09_cuts_outside_the_retained_ones_are_refused() {
    let mut ix = index();
    feed(
        &mut ix,
        RANGE,
        &page(
            0,
            vec![(1, one(1.0)), (1, one(2.0)), (2, one(3.0)), (1, None)],
        ),
    );
    let close = Record {
        at: 5,
        change: Change::Closed(35),
    };
    ix.apply(
        RANGE,
        &Page {
            after: 4,
            through: 5,
            records: vec![close],
        },
    )
    .unwrap();
    assert_eq!(read(&mut ix, ReadAt::Cut(0)), Ok(vec![]));
    ix.run();
    assert_eq!(read(&mut ix, ReadAt::Cut(15)), Ok(vec![1]));
    assert_eq!(read(&mut ix, ReadAt::Cut(35)), Ok(vec![2, 3]));
    assert_eq!(
        read(&mut ix, ReadAt::Cut(36)),
        Err(Error::CutTooNew { newest: 35 })
    );
    assert_eq!(read(&mut ix, LATEST), Ok(vec![3]));
    ix.advance_low(RANGE, 3).unwrap();
    assert_eq!(
        read(&mut ix, ReadAt::Cut(19)),
        Err(Error::CutTooOld { oldest: 20 })
    );
    assert_eq!(
        read(&mut ix, ReadAt::Position(2)),
        Err(Error::CutTooOld { oldest: 3 })
    );
    settle(&mut ix);
    assert_eq!(read(&mut ix, ReadAt::Cut(20)), Ok(vec![2]));
    assert_eq!(read(&mut ix, ReadAt::Position(3)), Ok(vec![2, 3]));
    assert_eq!(
        read(&mut ix, ReadAt::Position(6)),
        Err(Error::CutTooNew { newest: 5 })
    );
    assert_eq!(
        ix.advance_low(RANGE, 6),
        Err(Error::CutTooNew { newest: 5 })
    );
    // A snapshot at 5 whose newest commit, a delete, has timestamp 40.
    ix.begin_load(RANGE, 5, 40).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 5).unwrap();
    let close = Record {
        at: 6,
        change: Change::Closed(50),
    };
    feed(
        &mut ix,
        RANGE,
        &Page {
            after: 5,
            through: 6,
            records: vec![close],
        },
    );
    assert_eq!(
        read(&mut ix, ReadAt::Cut(39)),
        Err(Error::CutTooOld { oldest: 40 })
    );
    assert_eq!(read(&mut ix, ReadAt::Cut(40)), Ok(vec![]));
}

/// Positions are compared whole: these differ above the low 32 bits only.
#[test]
fn vs07_a_version_is_the_whole_position() {
    let mut ix = index();
    let start = 1 << 40;
    ix.begin_load(RANGE, start, 0).unwrap();
    ix.load_rows(RANGE, &[(key(1), 1, one(1.0).unwrap())])
        .unwrap();
    ix.run();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, start).unwrap();
    let at = start + (1 << 32);
    let change = Change::Row {
        key: key(1),
        commit_ts: 2,
        row: one(2.0),
    };
    ix.apply(
        RANGE,
        &Page {
            after: start,
            through: at,
            records: vec![Record { at, change }],
        },
    )
    .unwrap();
    ix.run();
    assert_eq!(read(&mut ix, ReadAt::Position(at - 1)), Ok(vec![1]));
    assert_eq!(read(&mut ix, ReadAt::Position(at)), Ok(vec![2]));
}

/// What nothing names any more goes away: rows, versions, partitions and summaries.
#[test]
fn vs05_churn_leaves_nothing_behind() {
    let cfg = Config {
        part_min: 4,
        part_max: 16,
        ..Config::new(schema(2, &[ColumnType::Int], None), 3)
    };
    let mut ix = serving(cfg, ExactCodec, None);
    let mut at = 0;
    for round in 0..40u64 {
        let write = |k: u64| {
            let x = (round * 64 + k) as f32;
            (
                k,
                Some(row(&[x.cos(), x.sin()], vec![Value::Int(round as i64)], k)),
            )
        };
        feed(&mut ix, RANGE, &page(at, (0..64).map(write).collect()));
        at += 64;
        ix.advance_low(RANGE, at).unwrap();
        settle(&mut ix);
    }
    let slots = ix.shared.rows.census();
    assert_eq!(slots.len(), 64, "a slot per row");
    assert!(
        slots.iter().all(|s| s.versions.len() == 1 && s.slot < 400),
        "slots are used again"
    );
    let entries: usize = ix.shared.dir.values().map(|m| m.entries).sum();
    assert_eq!(entries, 64);
    assert!(ix.shared.dir.len() <= 16, "{}", ix.shared.dir.len());
    feed(
        &mut ix,
        RANGE,
        &page(at, (0..64).map(|k| (k, None)).collect()),
    );
    ix.advance_low(RANGE, at + 64).unwrap();
    settle(&mut ix);
    assert!(ix.shared.rows.census().is_empty(), "deleted rows are gone");
    assert_eq!(
        ix.shared.summary.estimate(&Filter::Eq(0, Value::Int(39))),
        0.0
    );
    assert!(ix.shared.ranges[&RANGE].keys.is_empty());
}

/// A pin holds what it sees back, and letting it go is enough to free it.
#[test]
fn vs05_a_pin_holds_back_what_it_sees() {
    let mut ix = index();
    feed(&mut ix, RANGE, &page(0, vec![(1, one(1.0)), (2, one(2.0))]));
    let pin = latest(&mut ix, None);
    feed(&mut ix, RANGE, &page(2, vec![(1, one(3.0)), (2, None)]));
    ix.advance_low(RANGE, 4).unwrap();
    settle(&mut ix);
    let mut seen = images(&ix.search(&pin, &query(Op::L2, &[0.0, 0.0], 9)).unwrap());
    seen.sort_unstable();
    assert_eq!(seen, [1, 2]);
    assert_eq!(ix.shared.dir.values().map(|m| m.entries).sum::<usize>(), 3);
    drop(pin);
    settle(&mut ix);
    assert_eq!(ix.shared.dir.values().map(|m| m.entries).sum::<usize>(), 1);
    assert_eq!(read(&mut ix, LATEST), Ok(vec![3]));
}

/// Every tree is of one `partition_by` value, and a pin of one tree refuses a query for another.
#[test]
fn vs07_each_partition_by_value_has_a_tree_of_its_own() {
    let mut ix = serving(
        Config::new(schema(2, &[ColumnType::Text], Some(0)), 2),
        ExactCodec,
        None,
    );
    let tenant = |t: &str| Value::Text(t.into());
    let write = |k: u64, t: &str| (k, Some(row(&[k as f32, 1.0], vec![tenant(t)], k)));
    feed(
        &mut ix,
        RANGE,
        &page(0, vec![write(1, "a"), write(2, "b"), write(3, "a")]),
    );
    // A row that changes its tenant changes its tree.
    feed(&mut ix, RANGE, &page(3, vec![write(1, "b")]));
    assert_eq!(ix.shared.roots.len(), 2);
    let only = |t: &str| Filter::And(vec![Filter::Eq(0, tenant(t))]);
    let search = |ix: &mut Index, f: Option<Filter>| {
        let pin = latest(ix, f.as_ref());
        let parts = parts_of(pin.0.roots.iter()).len();
        let q = Query {
            filter: f,
            ..query(Op::Dot, &[1.0, 0.0], 9)
        };
        (parts, images(&ix.search(&pin, &q).unwrap()))
    };
    assert_eq!(search(&mut ix, Some(only("a"))), (1, vec![3]));
    assert_eq!(search(&mut ix, Some(only("b"))), (1, vec![2, 1]));
    assert_eq!(search(&mut ix, Some(only("none"))), (0, vec![]));
    assert_eq!(search(&mut ix, None), (2, vec![3, 2, 1]));
    let pin = latest(&mut ix, Some(&only("a")));
    for filter in [None, Some(only("b"))] {
        let q = Query {
            filter,
            ..query(Op::Dot, &[1.0, 0.0], 9)
        };
        assert!(matches!(ix.search(&pin, &q), Err(Error::InvalidQuery(_))));
    }
}

#[test]
fn vs17_invalid_queries_are_refused() {
    let mut ix = serving(
        Config::new(schema(2, &[ColumnType::Tags], None), 1),
        ExactCodec,
        None,
    );
    let pin = latest(&mut ix, None);
    let good = query(Op::Cosine, &[1.0, 0.0], 10);
    assert!(ix.search(&pin, &good).is_ok());
    let deep = (0..100).fold(Filter::Contains(0, "t".into()), |f, _| {
        Filter::Not(Box::new(f))
    });
    let bad = [
        Query {
            vec: vec![1.0],
            ..good.clone()
        },
        Query {
            vec: vec![1.0, f32::NAN],
            ..good.clone()
        },
        Query {
            vec: vec![0.0, -0.0],
            ..good.clone()
        },
        Query {
            limit: 0,
            ..good.clone()
        },
        Query {
            limit: 1001,
            ..good.clone()
        },
        Query {
            recall: 1.5,
            ..good.clone()
        },
        Query {
            filter: Some(Filter::Eq(0, Value::Int(1))),
            ..good.clone()
        },
        Query {
            filter: Some(Filter::Contains(1, "t".into())),
            ..good.clone()
        },
        Query {
            filter: Some(deep),
            ..good.clone()
        },
    ];
    for q in bad {
        assert!(
            matches!(ix.search(&pin, &q), Err(Error::InvalidQuery(_))),
            "{q:?}"
        );
    }
    let zero = Query {
        vec: vec![0.0, 0.0],
        op: Op::L2,
        ..good
    };
    assert!(ix.search(&pin, &zero).is_ok());
    // Configurations that cannot work.
    for cfg in [
        Config::new(schema(0, &[], None), 1),
        Config::new(schema(2, &[], None), 0),
        Config::new(schema(2, &[ColumnType::Tags], Some(0)), 1),
        Config::new(schema(2, &[], Some(0)), 1),
    ] {
        assert!(Index::new(cfg, ExactCodec, None).is_err());
    }
}

/// A resync replaces everything, also with the same rows, and a late page of the old one is refused.
#[test]
fn vs05_a_resync_replaces_the_index_at_once() {
    let mut ix = index();
    feed(&mut ix, RANGE, &page(0, vec![(1, one(1.0)), (2, one(2.0))]));
    let pin = latest(&mut ix, None);
    // Appends of the old generation are still queued.
    ix.apply(RANGE, &page(2, vec![(3, one(9.0))])).unwrap();
    ix.pump();
    ix.begin_load(RANGE, 7, 70).unwrap();
    ix.load_rows(
        RANGE,
        &[
            (key(2), 20, one(2.0).unwrap()),
            (key(4), 60, one(4.0).unwrap()),
        ],
    )
    .unwrap();
    ix.run();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 7).unwrap();
    assert_eq!(
        ix.search(&pin, &query(Op::Dot, &[1.0, 0.0], 9)),
        Err(Error::PinFailed)
    );
    assert_eq!(read(&mut ix, LATEST), Ok(vec![2, 4]));
    assert_eq!(
        ix.apply(RANGE, &page(9, vec![(3, one(9.0))])),
        Err(Error::Gap { delivered: 7 })
    );
    ix.drop_range(RANGE).unwrap();
    assert_eq!(read(&mut ix, LATEST), Err(Error::NotHeld(RANGE)));
    assert!(ix.shared.rows.census().is_empty());
    assert_eq!(ix.shared.dir.values().map(|m| m.entries).sum::<usize>(), 0);
}

/// An entry stays in the tree of its `partition_by` value.
#[test]
fn vs07_nothing_moves_to_another_tree() {
    let mut ix = serving(
        Config::new(schema(2, &[ColumnType::Text], Some(0)), 1),
        ExactCodec,
        None,
    );
    let write = |k: u64, t: &str| (k, Some(row(&[1.0, 0.0], vec![Value::Text(t.into())], k)));
    feed(
        &mut ix,
        RANGE,
        &page(
            0,
            vec![
                write(1, "a"),
                write(2, "b"),
                (3, Some(row(&[0.0; 2], vec![Value::Text("a".into())], 3))),
            ],
        ),
    );
    let parts: Vec<PartId> = ix.shared.dir.keys().copied().collect();
    let entry = |ix: &Index, p| ix.shards[0].parts[&p].entries().next().unwrap().id;
    for (&from, &to) in parts.iter().flat_map(|a| parts.iter().map(move |b| (a, b))) {
        let id = entry(&ix, from);
        assert!(!ix.reassign(from, id, to), "{from:?} to {to:?}");
        assert!(!ix.merge(from, to), "{from:?} into {to:?}");
    }
    // The list of vectors without a direction does not drain.
    let list = ix
        .shared
        .dir
        .iter()
        .find(|(_, m)| m.undirected)
        .map(|(p, _)| *p)
        .unwrap();
    assert!(!ix.split(list));
}
