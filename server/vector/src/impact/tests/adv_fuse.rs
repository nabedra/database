//! Adversarial tests of task G, fusion: RRF rank math with K = 60, weights, ties by key, rankings
//! of different lengths and empty ones, and the merge of workers' lists.

use nabedra_check::Gen;
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

fn list(keys: &[u64]) -> Vec<Candidate> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| c(*k, 100.0 - i as f32))
        .collect()
}

#[test]
fn vs04_adv_rrf_scores_are_one_over_sixty_plus_rank_from_one() {
    let r1 = list(&[1, 2, 3]);
    let r2 = list(&[3, 4]);
    let out = fuse(&[r1, r2], &Fusion::Rrf, 10).unwrap();
    let score = |k: u64| {
        out.iter()
            .find(|c| ks(std::slice::from_ref(c)) == [k])
            .unwrap()
            .score
    };
    let want = |s: f64| s as f32;
    assert_eq!(score(1), want(1.0 / 61.0));
    assert_eq!(score(2), want(1.0 / 62.0));
    assert_eq!(score(3), want(1.0 / 63.0 + 1.0 / 61.0));
    assert_eq!(score(4), want(1.0 / 62.0));
    // 3 leads, 1 follows, and 2 and 4 tie at 1/62 and go by key.
    assert_eq!(ks(&out), [3, 1, 2, 4]);
}

#[test]
fn vs04_adv_weights_scale_each_rankings_share() {
    let r1 = list(&[1, 2]);
    let r2 = list(&[2, 3]);
    let out = fuse(
        &[r1.clone(), r2.clone()],
        &Fusion::Weights(vec![3.0, 1.0]),
        10,
    )
    .unwrap();
    let want = |s: f64| s as f32;
    let by = |k: u64| {
        out.iter()
            .find(|c| ks(std::slice::from_ref(c)) == [k])
            .unwrap()
            .score
    };
    assert_eq!(by(1), want(3.0 / 61.0));
    assert_eq!(by(2), want(3.0 / 62.0 + 1.0 / 61.0));
    assert_eq!(by(3), want(1.0 / 62.0));
    assert_eq!(ks(&out), [2, 1, 3]);
    // A zero weight still lets the row in, at the bottom.
    let out = fuse(
        &[r1.clone(), r2.clone()],
        &Fusion::Weights(vec![1.0, 0.0]),
        10,
    )
    .unwrap();
    assert_eq!(ks(&out), [1, 2, 3]);
    assert_eq!(by_key(&out, 3), 0.0);
    // Weights refuse: a wrong count, negative, NaN, infinite.
    for w in [
        vec![1.0],
        vec![1.0, 1.0, 1.0],
        vec![-1.0, 1.0],
        vec![f64::NAN, 1.0],
        vec![f64::INFINITY, 1.0],
        vec![-0.0001, 0.0],
    ] {
        assert!(
            matches!(
                fuse(&[r1.clone(), r2.clone()], &Fusion::Weights(w.clone()), 5),
                Err(Error::InvalidQuery(_))
            ),
            "{w:?}"
        );
    }
    assert!(matches!(
        fuse(&[r1.clone()], &Fusion::Weights(vec![1.0, 1.0]), 5),
        Err(Error::InvalidQuery(_))
    ));
    // Negative zero is a zero.
    assert!(fuse(&[r1, r2], &Fusion::Weights(vec![-0.0, 1.0]), 5).is_ok());
}

fn by_key(l: &[Candidate], k: u64) -> f32 {
    l.iter()
        .find(|c| ks(std::slice::from_ref(c)) == [k])
        .unwrap()
        .score
}

#[test]
fn vs04_adv_empty_and_uneven_rankings() {
    assert!(matches!(
        fuse(&[], &Fusion::Rrf, 5),
        Err(Error::InvalidQuery(_))
    ));
    assert!(matches!(
        fuse(&[], &Fusion::Weights(vec![]), 5),
        Err(Error::InvalidQuery(_))
    ));
    assert_eq!(fuse(&[vec![], vec![]], &Fusion::Rrf, 5).unwrap(), []);
    assert_eq!(fuse(&[vec![]], &Fusion::Rrf, 5).unwrap(), []);
    // One empty ranking changes nothing for the other, whose ranks count from 1.
    let out = fuse(&[vec![], list(&[9, 8])], &Fusion::Rrf, 5).unwrap();
    assert_eq!(ks(&out), [9, 8]);
    assert_eq!(out[0].score, (1.0f64 / 61.0) as f32);
    // Limit 0, and a limit above the rows.
    assert_eq!(
        fuse(&[list(&[1]), list(&[2])], &Fusion::Rrf, 0).unwrap(),
        []
    );
    assert_eq!(
        fuse(&[list(&[1]), list(&[2])], &Fusion::Rrf, 99)
            .unwrap()
            .len(),
        2
    );
    // A single ranking keeps its own scores, whatever the fusion, cut at the limit.
    let one = list(&[5, 4, 3]);
    assert_eq!(
        fuse(std::slice::from_ref(&one), &Fusion::Rrf, 2).unwrap(),
        one[..2]
    );
    assert_eq!(
        fuse(std::slice::from_ref(&one), &Fusion::Weights(vec![0.0]), 3).unwrap(),
        one
    );
    // A long ranking and a short one.
    let long: Vec<u64> = (0..500).collect();
    let out = fuse(&[list(&long), list(&[499, 0])], &Fusion::Rrf, 3).unwrap();
    assert_eq!(ks(&out), [0, 499, 1]);
    assert_eq!(out[1].score, (1.0f64 / 560.0 + 1.0 / 61.0) as f32);
}

#[test]
fn vs04_adv_mirrored_rankings_tie_and_go_by_key() {
    let out = fuse(&[list(&[7, 3]), list(&[3, 7])], &Fusion::Rrf, 5).unwrap();
    assert_eq!(ks(&out), [3, 7]);
    assert_eq!(out[0].score, out[1].score);
    let out = fuse(
        &[list(&[7, 3, 5]), list(&[3, 5, 7]), list(&[5, 7, 3])],
        &Fusion::Rrf,
        5,
    )
    .unwrap();
    assert_eq!(ks(&out), [3, 5, 7]);
    assert!(out.iter().all(|c| c.score == out[0].score));
}

#[test]
#[ignore = "open bug VECTOR-impact-weighted-score-infinite"]
fn vs04_adv_weighted_scores_stay_finite() {
    // A finite weight can make the f32 score infinite, which has no JSON form.
    for w in [1e39f64, 1e300, f64::MAX] {
        let out = fuse(
            &[list(&[1, 2]), list(&[2, 1])],
            &Fusion::Weights(vec![w, w]),
            5,
        )
        .unwrap();
        assert!(
            out.iter().all(|c| c.score.is_finite()),
            "weight {w}: {out:?}"
        );
    }
}

fn ref_rrf(rankings: &[Vec<u64>], w: &[f64]) -> BTreeMap<u64, f64> {
    let mut out = BTreeMap::new();
    for (r, l) in rankings.iter().enumerate() {
        for (i, k) in l.iter().enumerate() {
            *out.entry(*k).or_insert(0.0) += w[r] / (60.0 + (i + 1) as f64);
        }
    }
    out
}

fn ranking(g: &mut Gen, universe: u64) -> Vec<u64> {
    let mut l: Vec<u64> = (0..universe).collect();
    // A random subset in a random order.
    for i in (1..l.len()).rev() {
        l.swap(i, g.below(i as u64 + 1) as usize);
    }
    l.truncate(g.below(universe + 1) as usize);
    l
}

#[test]
fn vs04_adv_fusion_equals_the_formula_on_random_rankings() {
    check_with(
        Runs {
            cases: 300,
            ..Runs::default()
        },
        "vs04_adv_fusion_equals_the_formula_on_random_rankings",
        |g| {
            let universe = 1 + g.below(40);
            let n = 2 + g.below(4) as usize;
            let rankings: Vec<Vec<u64>> = (0..n).map(|_| ranking(g, universe)).collect();
            let weights: Vec<f64> = (0..n)
                .map(|_| *g.pick(&[0.0, 0.5, 1.0, 2.0, 7.25]))
                .collect();
            let (fusion, w) = if g.bool() {
                (Fusion::Rrf, vec![1.0; n])
            } else {
                (Fusion::Weights(weights.clone()), weights)
            };
            let limit = *g.pick(&[0usize, 1, 3, 10, 1000]);
            let lists: Vec<Vec<Candidate>> = rankings.iter().map(|r| list(r)).collect();
            let out = fuse(&lists, &fusion, limit).map_err(|e| e.to_string())?;
            let want = ref_rrf(&rankings, &w);
            if out.len() != limit.min(want.len()) {
                return Err(format!(
                    "{} rows, want {}",
                    out.len(),
                    limit.min(want.len())
                ));
            }
            for c in &out {
                let k = ks(std::slice::from_ref(c))[0];
                let r = want[&k];
                if (f64::from(c.score) - r).abs() > 1e-6 * r.abs() + 1e-12 {
                    return Err(format!("row {k}: {} want {r}", c.score));
                }
            }
            for p in out.windows(2) {
                let ok = p[0].score > p[1].score
                    || (p[0].score == p[1].score && ks(&p[..1]) < ks(&p[1..]));
                if !ok {
                    return Err(format!("out of order: {out:?}"));
                }
            }
            if let Some(last) = out.last() {
                let worst = want[&ks(std::slice::from_ref(last))[0]];
                let got: std::collections::BTreeSet<u64> = out
                    .iter()
                    .flat_map(|c| ks(std::slice::from_ref(c)))
                    .collect();
                for (k, r) in &want {
                    if !got.contains(k) && *r > worst * (1.0 + 1e-6) + 1e-12 {
                        return Err(format!(
                            "row {k} ({r}) is better than the worst kept ({worst})"
                        ));
                    }
                }
            }
            Ok(())
        },
    );
}

#[test]
fn vs04_adv_merge_keeps_each_keys_best_score_and_orders_by_key_on_ties() {
    check_with(
        Runs {
            cases: 300,
            ..Runs::default()
        },
        "vs04_adv_merge_keeps_each_keys_best_score_and_orders_by_key_on_ties",
        |g| {
            let workers = 1 + g.below(4);
            let lower = g.bool();
            let order = if lower {
                Order::LowerFirst
            } else {
                Order::HigherFirst
            };
            let lists: Vec<Vec<Candidate>> = (0..workers)
                .map(|_| {
                    g.vec(12, |g| {
                        c(g.below(10), *g.pick(&[0.0f32, 0.25, 0.5, 1.0, -1.0, 1e30]))
                    })
                })
                .collect();
            let n = *g.pick(&[0usize, 1, 4, 100]);
            let out = merge(&lists, order, n);
            let mut best: BTreeMap<u64, f32> = BTreeMap::new();
            for cand in lists.iter().flatten() {
                let k = ks(std::slice::from_ref(cand))[0];
                let e = best.entry(k).or_insert(cand.score);
                if (lower && cand.score < *e) || (!lower && cand.score > *e) {
                    *e = cand.score;
                }
            }
            let mut want: Vec<(u64, f32)> = best.into_iter().collect();
            want.sort_by(|a, b| {
                let s = if lower {
                    a.1.total_cmp(&b.1)
                } else {
                    b.1.total_cmp(&a.1)
                };
                s.then(a.0.cmp(&b.0))
            });
            want.truncate(n);
            let got: Vec<(u64, f32)> = out
                .iter()
                .map(|c| (ks(std::slice::from_ref(c))[0], c.score))
                .collect();
            if got != want {
                return Err(format!("{got:?} != {want:?}"));
            }
            // The order of the workers does not matter.
            let mut rev = lists.clone();
            rev.reverse();
            if merge(&rev, order, n) != out {
                return Err("worker order changed the merge".into());
            }
            Ok(())
        },
    );
}
