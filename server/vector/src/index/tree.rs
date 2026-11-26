//! Centroid trees: balanced k-means over directions with a fanout of about 100. Nodes are immutable
//! and shared, and a change copies one path, so a pin takes the whole partition list by cloning a
//! root.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::codec::dot;
use crate::types::PartId;

/// A node splits in two halves of at least a third beyond this.
const MAX_FANOUT: usize = 128;
/// Nodes that an assignment opens per level, and a scan.
const ASSIGN_BEAM: usize = 8;
pub const SCAN_BEAM: usize = 32;

#[derive(Clone)]
pub struct Leaf {
    pub part: PartId,
    pub centroid: Arc<[f32]>,
    /// A draining partition is scanned, and nothing is assigned to it.
    pub ready: bool,
}

enum Kids {
    Leaves(Vec<Leaf>),
    Nodes(Vec<Arc<Node>>),
}

pub struct Node {
    centroid: Arc<[f32]>,
    kids: Kids,
}

#[derive(Clone, Default)]
pub struct Tree {
    root: Option<Arc<Node>>,
}

impl Tree {
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn insert(&mut self, leaf: Leaf) {
        let Some(root) = self.root.take() else {
            self.root = Some(Arc::new(Node::new(Kids::Leaves(vec![leaf]))));
            return;
        };
        let (a, b) = root.insert(leaf);
        self.root = Some(match b {
            None => a,
            Some(b) => Arc::new(Node::new(Kids::Nodes(vec![a, b]))),
        });
    }

    pub fn remove(&mut self, part: PartId) {
        if let Some(root) = &self.root
            && let Some(next) = root.edit(part, &mut |leaves, i| {
                leaves.remove(i);
            })
        {
            self.root = next;
        }
        // A root with one child is not needed.
        while let Some(root) = &self.root
            && let Kids::Nodes(nodes) = &root.kids
            && let [only] = nodes.as_slice()
        {
            self.root = Some(only.clone());
        }
    }

    /// Takes a partition out of assignment.
    pub fn set_draining(&mut self, part: PartId) {
        if let Some(root) = &self.root
            && let Some(next) = root.edit(part, &mut |leaves, i| leaves[i].ready = false)
        {
            self.root = next;
        }
    }

    /// The partitions in no particular order.
    pub fn parts(&self) -> Vec<PartId> {
        let mut out = Vec::new();
        let mut todo: Vec<&Arc<Node>> = self.root.iter().collect();
        while let Some(node) = todo.pop() {
            match &node.kids {
                Kids::Leaves(leaves) => out.extend(leaves.iter().map(|l| l.part)),
                Kids::Nodes(nodes) => todo.extend(nodes),
            }
        }
        out
    }

    /// The first `n` ready partitions that a walk meets, except `not`, and their cosines.
    pub fn near(&self, dir: &[f32], not: Option<PartId>, n: usize) -> Vec<(PartId, f32)> {
        let mut walk = Walk::new(dir.to_vec(), ASSIGN_BEAM);
        walk.add(self);
        let leaves = std::iter::from_fn(|| walk.next());
        let ready = leaves.filter(|(l, _)| l.ready && Some(l.part) != not);
        ready.take(n).map(|(l, cos)| (l.part, cos)).collect()
    }

    /// The first of them.
    pub fn nearest(&self, dir: &[f32], not: Option<PartId>) -> Option<(PartId, f32)> {
        self.near(dir, not, 1).into_iter().next()
    }

    #[cfg(test)]
    pub fn shape(&self) -> (usize, usize) {
        let (mut depth, mut widest) = (0, 0);
        let mut level: Vec<Arc<Node>> = self.root.iter().cloned().collect();
        while !level.is_empty() {
            depth += 1;
            widest = level.iter().map(|n| n.len()).fold(widest, usize::max);
            level = level
                .iter()
                .flat_map(|n| match &n.kids {
                    Kids::Nodes(nodes) => nodes.clone(),
                    Kids::Leaves(_) => Vec::new(),
                })
                .collect();
        }
        (depth, widest)
    }
}

impl Node {
    fn new(kids: Kids) -> Node {
        let points: Vec<&[f32]> = match &kids {
            Kids::Leaves(l) => l.iter().map(|l| &*l.centroid).collect(),
            Kids::Nodes(n) => n.iter().map(|n| &*n.centroid).collect(),
        };
        Node {
            centroid: mean(&points).into(),
            kids,
        }
    }

    fn len(&self) -> usize {
        match &self.kids {
            Kids::Leaves(l) => l.len(),
            Kids::Nodes(n) => n.len(),
        }
    }

    /// This node with one more leaf, as one node or, when it grew too wide, as two.
    fn insert(&self, leaf: Leaf) -> (Arc<Node>, Option<Arc<Node>>) {
        let kids = match &self.kids {
            Kids::Leaves(old) => {
                let mut leaves = old.clone();
                leaves.push(leaf);
                Kids::Leaves(leaves)
            }
            Kids::Nodes(old) => {
                let mut nodes = old.clone();
                let near = nearest_node(&nodes, &leaf.centroid);
                let (a, b) = nodes[near].insert(leaf);
                nodes[near] = a;
                nodes.extend(b);
                Kids::Nodes(nodes)
            }
        };
        let node = Node::new(kids);
        if node.len() <= MAX_FANOUT {
            return (Arc::new(node), None);
        }
        fn halves<T>(all: Vec<T>, side: &[bool]) -> (Vec<T>, Vec<T>) {
            let mut sides = side.iter();
            all.into_iter()
                .partition(|_| *sides.next().unwrap_or(&false))
        }
        let (a, b) = match node.kids {
            Kids::Leaves(all) => {
                let points: Vec<&[f32]> = all.iter().map(|l| &*l.centroid).collect();
                let side = bisect(&points).0;
                let (a, b) = halves(all, &side);
                (Kids::Leaves(a), Kids::Leaves(b))
            }
            Kids::Nodes(all) => {
                let points: Vec<&[f32]> = all.iter().map(|n| &*n.centroid).collect();
                let side = bisect(&points).0;
                let (a, b) = halves(all, &side);
                (Kids::Nodes(a), Kids::Nodes(b))
            }
        };
        (Arc::new(Node::new(a)), Some(Arc::new(Node::new(b))))
    }

    /// This node after an edit of one leaf. `None`: not here. `Some(None)`: nothing is left.
    fn edit(
        &self,
        part: PartId,
        f: &mut dyn FnMut(&mut Vec<Leaf>, usize),
    ) -> Option<Option<Arc<Node>>> {
        let kids = match &self.kids {
            Kids::Leaves(old) => {
                let i = old.iter().position(|l| l.part == part)?;
                let mut leaves = old.clone();
                f(&mut leaves, i);
                Kids::Leaves(leaves)
            }
            Kids::Nodes(old) => {
                let (i, next) = old
                    .iter()
                    .enumerate()
                    .find_map(|(i, n)| Some((i, n.edit(part, f)?)))?;
                let mut nodes = old.clone();
                match next {
                    Some(n) => nodes[i] = n,
                    None => {
                        nodes.remove(i);
                    }
                }
                Kids::Nodes(nodes)
            }
        };
        let node = Node::new(kids);
        Some((node.len() > 0).then(|| Arc::new(node)))
    }
}

fn nearest_node(nodes: &[Arc<Node>], dir: &[f32]) -> usize {
    let cos = |n: &Arc<Node>| dot(&n.centroid, dir);
    (0..nodes.len())
        .max_by(|&a, &b| cos(&nodes[a]).total_cmp(&cos(&nodes[b])))
        .unwrap_or(0)
}

/// Leaves, nearest first as far as the node centroids tell. A node's centroid says little about its
/// nearest leaf, so on each level the `beam` nearest nodes are opened before any leaf is taken. The
/// other nodes wait for their turn.
pub struct Walk {
    dir: Vec<f32>,
    beam: usize,
    /// The roots, until the walk starts.
    roots: Vec<Arc<Node>>,
    heap: BinaryHeap<Item>,
    seq: usize,
}

struct Item {
    cos: f32,
    seq: usize,
    what: What,
}

enum What {
    Node(Arc<Node>),
    Leaf(Leaf),
}

impl Walk {
    pub fn new(dir: Vec<f32>, beam: usize) -> Walk {
        Walk {
            dir,
            beam,
            roots: Vec::new(),
            heap: BinaryHeap::new(),
            seq: 0,
        }
    }

    pub fn add(&mut self, tree: &Tree) {
        self.roots.extend(tree.root.clone());
    }

    fn start(&mut self) {
        let mut level = std::mem::take(&mut self.roots);
        while !level.is_empty() {
            for n in std::mem::take(&mut level) {
                self.push(dot(&n.centroid, &self.dir), What::Node(n));
            }
            // Nodes of this level and those that waited. Leaves go back.
            let mut leaves = Vec::new();
            let mut opened = 0;
            while opened < self.beam
                && let Some(item) = self.heap.pop()
            {
                match item.what {
                    What::Leaf(_) => leaves.push(item),
                    What::Node(n) => {
                        opened += 1;
                        match &n.kids {
                            Kids::Leaves(_) => self.open(&n),
                            Kids::Nodes(nodes) => level.extend(nodes.iter().cloned()),
                        }
                    }
                }
            }
            self.heap.extend(leaves);
        }
    }

    fn open(&mut self, node: &Node) {
        match &node.kids {
            Kids::Leaves(leaves) => {
                for l in leaves {
                    self.push(dot(&l.centroid, &self.dir), What::Leaf(l.clone()));
                }
            }
            Kids::Nodes(nodes) => {
                for n in nodes {
                    self.push(dot(&n.centroid, &self.dir), What::Node(n.clone()));
                }
            }
        }
    }

    fn push(&mut self, cos: f32, what: What) {
        self.seq += 1;
        let seq = self.seq;
        self.heap.push(Item { cos, seq, what });
    }

    pub fn next(&mut self) -> Option<(Leaf, f32)> {
        self.start();
        loop {
            let item = self.heap.pop()?;
            match item.what {
                What::Leaf(leaf) => return Some((leaf, item.cos)),
                What::Node(node) => self.open(&node),
            }
        }
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        // Ties in the order they were met, so that a walk is deterministic.
        (self.cos.total_cmp(&other.cos)).then(other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Item {}

/// The direction of the sum, or the first point when they cancel out.
pub fn mean(points: &[&[f32]]) -> Vec<f32> {
    let Some(first) = points.first() else {
        return Vec::new();
    };
    let mut sum = first.to_vec();
    for p in &points[1..] {
        for (s, x) in sum.iter_mut().zip(*p) {
            *s += x;
        }
    }
    super::codec::direction(&sum).map_or_else(|| first.to_vec(), |(dir, _)| dir)
}

/// Balanced 2-means over unit vectors. Each side gets at least a third, rounded down, and at least
/// one of two or more points. Returns the side of each point and both centroids.
pub fn bisect(points: &[&[f32]]) -> (Vec<bool>, [Vec<f32>; 2]) {
    const ROUNDS: usize = 8;
    let n = points.len();
    let Some(first) = points.first() else {
        return (Vec::new(), [Vec::new(), Vec::new()]);
    };
    let farthest = |from: &[f32]| {
        let cos = |p: &&&[f32]| dot(p, from);
        points
            .iter()
            .min_by(|a, b| cos(a).total_cmp(&cos(b)))
            .unwrap_or(first)
            .to_vec()
    };
    let b = farthest(first);
    let mut c = [farthest(&b), b];
    let mut side = Vec::new();
    for _ in 0..ROUNDS {
        // How much nearer a point is to the first centroid. The smallest third goes to the second
        // one, the largest third to the first.
        let lean: Vec<f32> = points
            .iter()
            .map(|p| dot(p, &c[0]) - dot(p, &c[1]))
            .collect();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&x, &y| lean[x].total_cmp(&lean[y]).then(x.cmp(&y)));
        let least = (n / 3).max(usize::from(n > 1));
        let cut = order
            .partition_point(|&i| lean[i] < 0.0)
            .clamp(least, n - least);
        let mut next = vec![false; n];
        for &i in &order[..cut] {
            next[i] = true;
        }
        if next == side {
            break;
        }
        side = next;
        let of = |s: bool| -> Vec<&[f32]> {
            let here = points.iter().zip(&side).filter(|(_, x)| **x == s);
            here.map(|(p, _)| *p).collect()
        };
        for (s, centroid) in [false, true].into_iter().zip(&mut c) {
            let members = of(s);
            if !members.is_empty() {
                *centroid = mean(&members);
            }
        }
    }
    (side, c)
}
