//! The network rules of a simulated cluster.
//!
//! [`NetPlan::plan`] decides the fate of each message when it is sent, from the run's net
//! stream: dropped (a black hole until the sender's deadline), delayed, or delivered twice.
//! Directed blocks cut single links; a partition or an isolation is a set of blocks. A
//! paused node holds what it sends and receives until it resumes; the transport waits on
//! [`NetPlan::is_paused`]. A crashed target refusing at once, like a closed port, is the
//! harness's part, since only it knows which nodes are up.
//!
//! The planning arithmetic and its draw order are the Events simulator's
//! (`server/events/src/sim/net.rs`), so the same stream gives the same plans.

use std::collections::BTreeSet;

use nabedra_check::Gen;

use crate::rng::{Draw, SplitMix64, stream};

/// Every delivered message takes at least this long.
pub const BASE_DELAY_MS: u64 = 1;

/// The drop, duplicate and delay settings the Events nemesis chooses between.
pub const DROP_PER_MILLE: [u64; 5] = [0, 0, 10, 50, 150];
pub const DUP_PER_MILLE: [u64; 3] = [0, 10, 50];
pub const DELAY_MAX_MS: [u64; 4] = [0, 5, 30, 120];

/// What happens to one message. The `Debug` form is what the Events simulator traces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    Drop,
    Deliver { delay_ms: u64, duplicate: bool },
}

/// Random message faults, applied to every link that is not blocked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Filters {
    pub drop_per_mille: u64,
    pub dup_per_mille: u64,
    /// Extra delay is uniform in `[0, delay_max_ms]`, on top of [`BASE_DELAY_MS`].
    pub delay_max_ms: u64,
}

impl Filters {
    /// One of the Events nemesis settings, drawn from a workload tape. Shrinks toward no
    /// faults.
    pub fn draw(g: &mut Gen) -> Self {
        Self {
            drop_per_mille: *g.pick(&DROP_PER_MILLE),
            dup_per_mille: *g.pick(&DUP_PER_MILLE),
            delay_max_ms: *g.pick(&DELAY_MAX_MS),
        }
    }

    pub fn is_clear(&self) -> bool {
        *self == Self::default()
    }
}

/// The network state of one run.
#[derive(Clone, Debug)]
pub struct NetPlan {
    rng: SplitMix64,
    filters: Filters,
    /// Directed links that drop every message.
    blocked: BTreeSet<(u64, u64)>,
    paused: BTreeSet<u64>,
    epoch: u64,
    messages: u64,
}

impl NetPlan {
    /// A healthy network whose net stream derives from the run seed.
    pub fn new(seed: u64) -> Self {
        Self::with_rng(stream(seed, "net"))
    }

    /// A healthy network drawing from `rng`. The Events simulator passes its own net
    /// stream here to keep its plans.
    pub fn with_rng(rng: SplitMix64) -> Self {
        Self {
            rng,
            filters: Filters::default(),
            blocked: BTreeSet::new(),
            paused: BTreeSet::new(),
            epoch: 0,
            messages: 0,
        }
    }

    /// Plans one message from `from` to `to`. A blocked link drops without a draw;
    /// otherwise the draws are drop (if enabled), delay (if enabled), duplicate (if
    /// enabled), in that order.
    pub fn plan(&mut self, from: u64, to: u64) -> Plan {
        self.messages += 1;
        if self.blocked.contains(&(from, to)) {
            return Plan::Drop;
        }
        let Filters {
            drop_per_mille: drop,
            dup_per_mille: dup,
            delay_max_ms: delay_max,
        } = self.filters;
        let rng = &mut self.rng;
        if drop > 0 && rng.chance(drop) {
            return Plan::Drop;
        }
        let delay_ms = BASE_DELAY_MS
            + if delay_max > 0 {
                rng.below(delay_max + 1)
            } else {
                0
            };
        let duplicate = dup > 0 && rng.chance(dup);
        Plan::Deliver {
            delay_ms,
            duplicate,
        }
    }

    /// Messages planned so far.
    pub fn messages(&self) -> u64 {
        self.messages
    }

    /// Bumped on every connectivity change, so a harness can tell whether the network
    /// stayed the same across an operation.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Records a connectivity change the plan cannot see, such as a crash or a restart.
    pub fn bump_epoch(&mut self) {
        self.epoch += 1;
    }

    pub fn filters(&self) -> Filters {
        self.filters
    }

    pub fn set_filters(&mut self, filters: Filters) {
        self.filters = filters;
    }

    pub fn is_blocked(&self, from: u64, to: u64) -> bool {
        self.blocked.contains(&(from, to))
    }

    /// Both directions between `a` and `b` are open.
    pub fn connected(&self, a: u64, b: u64) -> bool {
        !self.is_blocked(a, b) && !self.is_blocked(b, a)
    }

    /// The directed links that drop every message.
    pub fn blocked(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.blocked.iter().copied()
    }

    /// Blocks the directed link `from -> to` (a one-way link failure).
    pub fn block(&mut self, from: u64, to: u64) {
        self.epoch += 1;
        self.blocked.insert((from, to));
    }

    pub fn unblock(&mut self, from: u64, to: u64) {
        self.epoch += 1;
        self.blocked.remove(&(from, to));
    }

    /// Cuts `target` off from every other node in `nodes`, both ways.
    pub fn isolate(&mut self, target: u64, nodes: impl IntoIterator<Item = u64>) {
        self.epoch += 1;
        for other in nodes.into_iter().filter(|other| *other != target) {
            self.blocked.insert((target, other));
            self.blocked.insert((other, target));
        }
    }

    /// Blocks every link, both ways, between nodes on different sides. A node on no side
    /// keeps its links.
    pub fn partition(&mut self, sides: &[BTreeSet<u64>]) {
        self.epoch += 1;
        for (i, side) in sides.iter().enumerate() {
            for other in sides.iter().skip(i + 1) {
                for &a in side {
                    for &b in other {
                        self.blocked.insert((a, b));
                        self.blocked.insert((b, a));
                    }
                }
            }
        }
    }

    /// Opens every link. Filters and pauses stay.
    pub fn heal_links(&mut self) {
        self.epoch += 1;
        self.blocked.clear();
    }

    /// Opens every link, clears the filters and resumes every paused node.
    pub fn heal_all(&mut self) {
        self.epoch += 1;
        self.blocked.clear();
        self.filters = Filters::default();
        self.paused.clear();
    }

    /// Holds everything `node` sends and receives until [`NetPlan::resume`].
    pub fn pause(&mut self, node: u64) {
        self.epoch += 1;
        self.paused.insert(node);
    }

    pub fn resume(&mut self, node: u64) {
        self.epoch += 1;
        self.paused.remove(&node);
    }

    pub fn is_paused(&self, node: u64) -> bool {
        self.paused.contains(&node)
    }

    /// Whether `node` reaches a majority of `nodes` (itself included) over links open in
    /// both directions, counting only nodes for which `usable` holds and that are not
    /// paused.
    pub fn has_quorum(&self, node: u64, nodes: &[u64], usable: impl Fn(u64) -> bool) -> bool {
        let usable = |id: u64| usable(id) && !self.is_paused(id);
        if !usable(node) {
            return false;
        }
        let reachable = nodes
            .iter()
            .filter(|&&other| other == node || (usable(other) && self.connected(node, other)))
            .count();
        reachable > nodes.len() / 2
    }
}

/// Two non-empty sides of `nodes` (at least two nodes), drawn from a workload tape. Shrinks
/// toward isolating the first node.
pub fn draw_partition(g: &mut Gen, nodes: &[u64]) -> Vec<BTreeSet<u64>> {
    assert!(nodes.len() >= 2, "a partition needs two nodes");
    let mut sides = vec![BTreeSet::new(), BTreeSet::new()];
    // The first node is always on side 1 and the node at `forced` on side 0, so both
    // sides are non-empty; every other node picks a side (false, the minimal choice, is 0).
    let forced = 1 + g.below(nodes.len() as u64 - 1) as usize;
    for (i, &node) in nodes.iter().enumerate() {
        let side = if i == 0 {
            1
        } else if i == forced {
            0
        } else {
            usize::from(g.bool())
        };
        sides[side].insert(node);
    }
    sides
}
