//! A heap-based merge of [`InternalIterator`]s, forward and reverse.
//!
//! The heap holds the indices of the valid children ordered by their current key (a
//! min-heap going forward, a max-heap going backward), so each step costs one child step
//! plus O(log k) comparisons. Changing direction re-seeks every other child around the
//! current key, as LevelDB does. Equal internal keys (which a correct store never
//! produces) tie-break on the child index, so the merge is deterministic.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::source::InternalIterator;
use crate::types::compare_internal_keys;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Forward,
    Reverse,
}

pub struct MergingIterator<I: InternalIterator = Box<dyn InternalIterator>> {
    children: Vec<I>,
    /// Indices of valid children; `heap[0]` is the current one.
    heap: Vec<usize>,
    direction: Direction,
    error: Option<Error>,
    scratch: Vec<u8>,
}

/// True when child `a` comes before child `b` in `direction`.
#[inline]
fn before<I: InternalIterator>(children: &[I], direction: Direction, a: usize, b: usize) -> bool {
    let order = compare_internal_keys(children[a].key(), children[b].key()).then(a.cmp(&b));
    match direction {
        Direction::Forward => order == Ordering::Less,
        Direction::Reverse => order == Ordering::Greater,
    }
}

fn sift_down<I: InternalIterator>(
    heap: &mut [usize],
    children: &[I],
    direction: Direction,
    mut i: usize,
) {
    loop {
        let left = 2 * i + 1;
        if left >= heap.len() {
            return;
        }
        let right = left + 1;
        let mut best = left;
        if right < heap.len() && before(children, direction, heap[right], heap[left]) {
            best = right;
        }
        if !before(children, direction, heap[best], heap[i]) {
            return;
        }
        heap.swap(i, best);
        i = best;
    }
}

impl<I: InternalIterator> MergingIterator<I> {
    pub fn new(children: Vec<I>) -> MergingIterator<I> {
        MergingIterator {
            heap: Vec::with_capacity(children.len()),
            children,
            direction: Direction::Forward,
            error: None,
            scratch: Vec::new(),
        }
    }

    pub fn children(&self) -> &[I] {
        &self.children
    }

    /// Rebuilds the heap from every valid child, after all of them were repositioned.
    fn rebuild(&mut self, direction: Direction) {
        self.direction = direction;
        self.heap.clear();
        if self.error.is_some() {
            return;
        }
        for (i, child) in self.children.iter().enumerate() {
            if child.valid() {
                self.heap.push(i);
            } else if let Err(error) = child.status() {
                self.error = Some(error);
                self.heap.clear();
                return;
            }
        }
        for i in (0..self.heap.len() / 2).rev() {
            sift_down(&mut self.heap, &self.children, direction, i);
        }
    }

    /// Restores the heap after the current child moved.
    fn fix_top(&mut self) {
        let top = self.heap[0];
        if !self.children[top].valid() {
            if let Err(error) = self.children[top].status() {
                self.error = Some(error);
                self.heap.clear();
                return;
            }
            self.heap.swap_remove(0);
        }
        sift_down(&mut self.heap, &self.children, self.direction, 0);
    }

    /// Positions every child except the current one strictly after (forward) or before
    /// (reverse) the current key, then rebuilds the heap in the new direction.
    fn switch_direction(&mut self, direction: Direction) {
        let current = self.heap[0];
        self.scratch.clear();
        self.scratch.extend_from_slice(self.children[current].key());
        let key = &self.scratch;
        for (i, child) in self.children.iter_mut().enumerate() {
            if i == current {
                continue;
            }
            match direction {
                Direction::Forward => {
                    child.seek(key);
                    if child.valid() && child.key() == key.as_slice() {
                        child.next();
                    }
                }
                Direction::Reverse => {
                    child.seek_for_prev(key);
                    if child.valid() && child.key() == key.as_slice() {
                        child.prev();
                    }
                }
            }
        }
        self.rebuild(direction);
    }
}

impl<I: InternalIterator> InternalIterator for MergingIterator<I> {
    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn seek_to_first(&mut self) {
        self.children.iter_mut().for_each(|c| c.seek_to_first());
        self.rebuild(Direction::Forward);
    }

    fn seek_to_last(&mut self) {
        self.children.iter_mut().for_each(|c| c.seek_to_last());
        self.rebuild(Direction::Reverse);
    }

    fn seek(&mut self, target: &[u8]) {
        self.children.iter_mut().for_each(|c| c.seek(target));
        self.rebuild(Direction::Forward);
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.children
            .iter_mut()
            .for_each(|c| c.seek_for_prev(target));
        self.rebuild(Direction::Reverse);
    }

    fn next(&mut self) {
        debug_assert!(self.valid());
        if self.direction != Direction::Forward {
            self.switch_direction(Direction::Forward);
            if !self.valid() {
                return;
            }
        }
        self.children[self.heap[0]].next();
        self.fix_top();
    }

    fn prev(&mut self) {
        debug_assert!(self.valid());
        if self.direction != Direction::Reverse {
            self.switch_direction(Direction::Reverse);
            if !self.valid() {
                return;
            }
        }
        self.children[self.heap[0]].prev();
        self.fix_top();
    }

    fn key(&self) -> &[u8] {
        self.children[self.heap[0]].key()
    }

    fn value(&self) -> Bytes {
        self.children[self.heap[0]].value()
    }

    fn status(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FileId;
    use crate::iter::testutil::{Rng, VecIter};
    use crate::types::{Kind, internal_key};

    fn entries(rng: &mut Rng, n: usize, seq_base: u64) -> Vec<(Vec<u8>, Bytes)> {
        let mut out: Vec<(Vec<u8>, Bytes)> = (0..n)
            .map(|i| {
                let key = [rng.below(30) as u8];
                (
                    internal_key(&key, seq_base + i as u64, Kind::Put),
                    Bytes::from(vec![i as u8]),
                )
            })
            .collect();
        out.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
        out
    }

    #[test]
    fn ns21_merging_walks_match_a_sorted_union_with_direction_switches() {
        let mut rng = Rng::new(0x3E_6E_21);
        for round in 0..300 {
            let k = rng.below(5) as usize;
            let sources: Vec<Vec<(Vec<u8>, Bytes)>> = (0..k)
                .map(|i| {
                    let n = rng.below(12) as usize;
                    entries(&mut rng, n, (i as u64 + 1) * 1000)
                })
                .collect();
            let mut union: Vec<(Vec<u8>, Bytes)> = sources.iter().flatten().cloned().collect();
            union.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
            let mut merged = MergingIterator::new(
                sources
                    .into_iter()
                    .map(|s| Box::new(VecIter::new(s)) as Box<dyn InternalIterator>)
                    .collect(),
            );
            let mut pos: Option<usize> = None;
            for step in 0..60 {
                match rng.below(6) {
                    0 => {
                        merged.seek_to_first();
                        pos = (!union.is_empty()).then_some(0);
                    }
                    1 => {
                        merged.seek_to_last();
                        pos = union.len().checked_sub(1);
                    }
                    2 => {
                        let target =
                            internal_key(&[rng.below(32) as u8], rng.below(6000), Kind::Put);
                        merged.seek(&target);
                        let i = union.partition_point(|e| {
                            compare_internal_keys(&e.0, &target) == Ordering::Less
                        });
                        pos = (i < union.len()).then_some(i);
                    }
                    3 => {
                        let target =
                            internal_key(&[rng.below(32) as u8], rng.below(6000), Kind::Put);
                        merged.seek_for_prev(&target);
                        let i = union.partition_point(|e| {
                            compare_internal_keys(&e.0, &target) != Ordering::Greater
                        });
                        pos = i.checked_sub(1);
                    }
                    4 if pos.is_some() => {
                        merged.next();
                        pos = pos.map(|p| p + 1).filter(|&p| p < union.len());
                    }
                    5 if pos.is_some() => {
                        merged.prev();
                        pos = pos.and_then(|p| p.checked_sub(1));
                    }
                    _ => {}
                }
                assert_eq!(merged.valid(), pos.is_some(), "round {round} step {step}");
                if let Some(p) = pos {
                    assert_eq!(
                        merged.key(),
                        union[p].0.as_slice(),
                        "round {round} step {step}"
                    );
                    assert_eq!(merged.value(), union[p].1);
                }
            }
        }
    }

    #[test]
    fn ns21_merging_reports_child_errors() {
        let good = VecIter::new(vec![(internal_key(b"a", 1, Kind::Put), Bytes::new())]);
        let bad = VecIter::failing(Error::Corruption {
            file: FileId::Run { id: 3 },
            offset: 7,
            what: "test",
        });
        let mut merged = MergingIterator::new(vec![good, bad]);
        merged.seek_to_first();
        assert!(!merged.valid());
        assert!(matches!(merged.status(), Err(Error::Corruption { .. })));
    }
}
