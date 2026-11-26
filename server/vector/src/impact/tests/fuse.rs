//! Hybrid fusion at the gateway.

use nabedra_check::prop::{Config as Runs, check_with};

use super::super::fusion::*;
use super::*;

fn c(k: u64, score: f32) -> Candidate {
    Candidate { key: key(k), score }
}

fn ks(list: &[Candidate]) -> Vec<u64> {
    list.iter()
        .map(|c| u64::from_be_bytes((&*c.key).try_into().unwrap()))
        .collect()
}

#[test]
fn vs04_one_rankings_lists_merge_over_workers() {
    let a = vec![c(1, 0.9), c(2, 0.5), c(4, 0.5)];
    let b = vec![c(3, 0.7), c(2, 0.5), c(5, 0.1)];
    assert_eq!(
        ks(&merge(&[a.clone(), b.clone()], Order::HigherFirst, 4)),
        [1, 3, 2, 4]
    );
    // `l2` is a distance: the lowest first.
    assert_eq!(ks(&merge(&[a, b], Order::LowerFirst, 10)), [5, 2, 4, 3, 1]);
    assert_eq!(merge(&[], Order::LowerFirst, 10), []);
}

#[test]
fn vs04_rrf_and_weights_fuse_by_rank() {
    let dense = vec![c(1, 0.9), c(2, 0.8), c(3, 0.7)];
    let text = vec![c(3, 12.0), c(1, 4.0)];
    let fused = fuse(&[dense.clone(), text.clone()], &Fusion::Rrf, 10).unwrap();
    assert_eq!(ks(&fused), [1, 3, 2]);
    let want = |ranks: &[f64]| ranks.iter().map(|r| 1.0 / (RRF_K + r)).sum::<f64>() as f32;
    assert_eq!(
        fused.iter().map(|c| c.score).collect::<Vec<_>>(),
        [want(&[1.0, 2.0]), want(&[3.0, 1.0]), want(&[2.0])]
    );
    let weighted = fuse(
        &[dense.clone(), text.clone()],
        &Fusion::Weights(vec![1.0, 0.0]),
        2,
    )
    .unwrap();
    assert_eq!(ks(&weighted), [1, 2]);
    let weighted = fuse(
        &[dense.clone(), text.clone()],
        &Fusion::Weights(vec![0.5, 3.0]),
        10,
    )
    .unwrap();
    assert_eq!(ks(&weighted), [3, 1, 2]);
    // One ranking keeps its own scores.
    assert_eq!(
        fuse(std::slice::from_ref(&text), &Fusion::Rrf, 1).unwrap(),
        [c(3, 12.0)]
    );
    for bad in [
        vec![1.0],
        vec![1.0, -1.0],
        vec![1.0, f64::NAN],
        vec![1.0, f64::INFINITY],
    ] {
        assert!(fuse(&[dense.clone(), text.clone()], &Fusion::Weights(bad), 5).is_err());
    }
    assert!(fuse(&[], &Fusion::Rrf, 5).is_err());
}

#[test]
fn vs04_fusion_is_a_total_order_on_any_lists() {
    check_with(
        Runs {
            cases: 300,
            ..Runs::default()
        },
        "vs04_fusion_is_a_total_order_on_any_lists",
        |g| {
            let n = 2 + g.below(3) as usize;
            let lists: Vec<Vec<Candidate>> = (0..n)
                .map(|_| {
                    let raw = g.vec(8, |g| c(g.below(10), g.below(5) as f32));
                    merge(&[raw], Order::HigherFirst, 6)
                })
                .collect();
            let limit = 1 + g.below(12) as usize;
            let fused = fuse(&lists, &Fusion::Rrf, limit).map_err(|e| e.to_string())?;
            // The reference: each row's sum in the order of the rankings.
            let mut want: BTreeMap<u64, f64> = BTreeMap::new();
            for list in &lists {
                for (i, k) in ks(list).into_iter().enumerate() {
                    *want.entry(k).or_default() += 1.0 / (RRF_K + i as f64 + 1.0);
                }
            }
            let mut want: Vec<(u64, f32)> = want.into_iter().map(|(k, s)| (k, s as f32)).collect();
            want.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            want.truncate(limit);
            let got: Vec<(u64, f32)> = ks(&fused)
                .into_iter()
                .zip(fused.iter().map(|c| c.score))
                .collect();
            if got != want {
                return Err(format!("{got:?} != {want:?}"));
            }
            Ok(())
        },
    );
}
