//! Shrinking a failing step list (delta debugging over step indices).
//!
//! A step list never depends on the store's answers, so every subset of a failing list is
//! a valid workload. Background timing makes runs nondeterministic, so a candidate counts
//! as failing if any of `tries` replays fails. The result is a list of indices into the
//! seeded list, which `regressions.rs` keeps as `(seed, steps, mix, indices)`.

use std::time::{Duration, Instant};

/// Shrinks `0..n` to a small subset for which `fails` still holds, within `budget`.
pub fn ddmin(n: usize, budget: Duration, fails: &mut dyn FnMut(&[usize]) -> bool) -> Vec<usize> {
    let deadline = Instant::now() + budget;
    let mut current: Vec<usize> = (0..n).collect();
    let mut chunks = 2usize;
    while current.len() >= 2 && Instant::now() < deadline {
        let size = current.len().div_ceil(chunks);
        let mut reduced = false;
        let mut start = 0;
        while start < current.len() && Instant::now() < deadline {
            let end = (start + size).min(current.len());
            let candidate: Vec<usize> = current[..start]
                .iter()
                .chain(&current[end..])
                .copied()
                .collect();
            if !candidate.is_empty() && fails(&candidate) {
                current = candidate;
                chunks = (chunks - 1).max(2);
                reduced = true;
                break;
            }
            start = end;
        }
        if !reduced {
            if chunks >= current.len() {
                break;
            }
            chunks = (chunks * 2).min(current.len());
        }
    }
    current
}
