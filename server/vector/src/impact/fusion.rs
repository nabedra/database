//! Hybrid ranking. Fusion belongs to the gateway (`plans/oltp-vector.md`, section 4, step 5): each
//! worker returns the candidates of every ranking of its pin, and the gateway merges each ranking's
//! lists over the workers, fuses them and applies `limit`. These are pure functions for it.
//!
//! With one ranking `_score` is that ranking's value. With several, `"rrf"` gives a row
//! `Σ 1 / (K + rank)` over the rankings that hold it, rank counted from 1, and a list of weights
//! gives `Σ weight / (K + rank)`. Ties break by key.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::types::{Error, Key, Result};

/// The constant of reciprocal rank fusion, as Cormack et al. and most engines use.
pub const RRF_K: f64 = 60.0;

/// The order of a ranking's scores: `l2` is a distance, the others are similarities.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Order {
    HigherFirst,
    LowerFirst,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Candidate {
    pub key: Key,
    pub score: f32,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Fusion {
    Rrf,
    /// One finite, non-negative weight per ranking.
    Weights(Vec<f64>),
}

fn better(order: Order, a: &Candidate, b: &Candidate) -> Ordering {
    let by_score = match order {
        Order::HigherFirst => b.score.total_cmp(&a.score),
        Order::LowerFirst => a.score.total_cmp(&b.score),
    };
    by_score.then_with(|| a.key.cmp(&b.key))
}

/// One ranking's candidates from every worker, best first and then by key, each key once, at most
/// `n`. A key that two replicas returned keeps its better score.
pub fn merge(lists: &[Vec<Candidate>], order: Order, n: usize) -> Vec<Candidate> {
    let mut best: HashMap<&Key, &Candidate> = HashMap::new();
    for c in lists.iter().flatten() {
        let keep = best
            .get(&c.key)
            .is_none_or(|old| better(order, c, old) == Ordering::Less);
        if keep {
            best.insert(&c.key, c);
        }
    }
    let mut all: Vec<Candidate> = best.into_values().cloned().collect();
    all.sort_by(|a, b| better(order, a, b));
    all.truncate(n);
    all
}

/// Fuses merged rankings, each best first, and keeps the best `limit` rows.
pub fn fuse(rankings: &[Vec<Candidate>], fusion: &Fusion, limit: usize) -> Result<Vec<Candidate>> {
    let weights = match fusion {
        Fusion::Rrf => vec![1.0; rankings.len()],
        Fusion::Weights(w) => {
            let ok = w.len() == rankings.len() && w.iter().all(|x| x.is_finite() && *x >= 0.0);
            if !ok {
                return Err(Error::InvalidQuery(
                    "one finite non-negative weight per ranking",
                ));
            }
            w.clone()
        }
    };
    match rankings {
        [] => Err(Error::InvalidQuery("no ranking")),
        [one] => Ok(one.iter().take(limit).cloned().collect()),
        _ => {
            // In the order of the rankings, so that every gateway sums alike.
            let mut sums: BTreeMap<&Key, f64> = BTreeMap::new();
            for (list, w) in rankings.iter().zip(&weights) {
                for (rank, c) in list.iter().enumerate() {
                    *sums.entry(&c.key).or_default() += w / (RRF_K + rank as f64 + 1.0);
                }
            }
            let mut fused: Vec<Candidate> = sums
                .into_iter()
                .map(|(key, s)| Candidate {
                    key: key.clone(),
                    score: s as f32,
                })
                .collect();
            fused.sort_by(|a, b| better(Order::HigherFirst, a, b));
            fused.truncate(limit);
            Ok(fused)
        }
    }
}
