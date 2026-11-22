//! Resolve register version orders that the dependency graph alone leaves open.
//!
//! If a transaction reads a version from one writer, every other writer of that key must
//! serialize before that writer or after the reader. We search only those ordering choices.
//! Every step starts from a topological order of the required edges, so an order that
//! satisfies all read constraints is already a witness.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::Graph;

const MAX_CONFIGURATIONS: usize = 4_096;
const MAX_DEPTH: usize = 128;
const MAX_CONSTRAINT_CHECKS: usize = 4_000_000;
pub(super) const MAX_CONSTRAINTS: usize = 1_000_000;

#[derive(Clone, Copy)]
pub(super) struct ReadOrder {
    pub reader: usize,
    pub source: usize,
    pub other_writer: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Search {
    Witness,
    Impossible,
    Limit,
}

pub(super) fn check(graph: &Graph, mask: u8, constraints: &[ReadOrder]) -> Search {
    let mut edges: Vec<Vec<usize>> = graph
        .adj
        .iter()
        .enumerate()
        .map(|(source, adjacent)| {
            adjacent
                .iter()
                .copied()
                .filter(|&target| graph.bits(source, target) & mask != 0)
                .collect()
        })
        .collect();
    let mut budget = Budget {
        configurations: MAX_CONFIGURATIONS,
        constraint_checks: MAX_CONSTRAINT_CHECKS,
    };
    search(&mut edges, constraints, &mut budget, 0)
}

struct Budget {
    configurations: usize,
    constraint_checks: usize,
}

fn search(
    edges: &mut [Vec<usize>],
    constraints: &[ReadOrder],
    budget: &mut Budget,
    depth: usize,
) -> Search {
    if budget.configurations == 0 {
        return Search::Limit;
    }
    budget.configurations -= 1;
    let Some(position) = topological_positions(edges) else {
        return Search::Impossible;
    };
    let inspected = constraints.len().min(budget.constraint_checks);
    let violated = constraints[..inspected].iter().position(|c| {
        position[c.source] < position[c.other_writer]
            && position[c.other_writer] < position[c.reader]
    });
    let Some(index) = violated else {
        budget.constraint_checks -= inspected;
        return if inspected == constraints.len() {
            Search::Witness
        } else {
            Search::Limit
        };
    };
    budget.constraint_checks -= index + 1;
    let c = constraints[index];
    if depth == MAX_DEPTH {
        return Search::Limit;
    }

    let mut result = Search::Impossible;
    for (a, b) in [(c.other_writer, c.source), (c.reader, c.other_writer)] {
        edges[a].push(b);
        let branch = search(edges, constraints, budget, depth + 1);
        edges[a].pop();
        match branch {
            Search::Witness => return Search::Witness,
            Search::Limit => result = Search::Limit,
            Search::Impossible => {}
        }
    }
    result
}

fn topological_positions(edges: &[Vec<usize>]) -> Option<Vec<usize>> {
    let mut incoming = vec![0usize; edges.len()];
    for &target in edges.iter().flatten() {
        incoming[target] += 1;
    }
    // Transaction IDs follow invocation order, which gives the search a cheap first witness
    // for histories whose operations serialized close to invocation.
    let mut ready: BinaryHeap<_> = incoming
        .iter()
        .enumerate()
        .filter_map(|(i, &count)| (count == 0).then_some(Reverse(i)))
        .collect();
    let mut position = vec![0; edges.len()];
    for index in 0..edges.len() {
        let Reverse(source) = ready.pop()?;
        position[source] = index;
        for &target in &edges[source] {
            incoming[target] -= 1;
            if incoming[target] == 0 {
                ready.push(Reverse(target));
            }
        }
    }
    Some(position)
}
