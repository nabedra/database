//! Explicit-state model checker.
//!
//! A [`Model`] lists its initial states, the actions enabled in a state and the
//! successor of each action. [`explore`] runs a breadth-first search over the
//! reachable states with a visited set and parent pointers, and checks the
//! model's invariants on every state when it is first discovered. Because the
//! search is breadth first, the first violation found is at minimal depth, so
//! the returned trace is a shortest counterexample.
//!
//! Exploration is deterministic: states are expanded in discovery order and
//! actions in the order the model pushes them, and no hash-map iteration order
//! is ever observed. Two runs of the same model give the same result and trace.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::{BuildHasherDefault, Hash, Hasher};

/// FxHash (the rustc hasher): deterministic and much cheaper than SipHash, which
/// matters because models run in debug builds. Visited-set hashing is not exposed
/// to untrusted input, so DoS resistance is not needed.
#[derive(Default)]
struct FxHasher(u64);

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut b = [0u8; 8];
            b[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(b));
        }
    }
    fn write_u8(&mut self, i: u8) {
        self.write_u64(i as u64);
    }
    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64);
    }
    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// A finite (or bounded) transition system with invariants.
pub trait Model {
    type State: Clone + Eq + Hash + fmt::Debug;
    type Action: Clone + fmt::Debug;

    fn init_states(&self) -> Vec<Self::State>;
    /// Pushes the actions to try in `state`. The order fixes the exploration order.
    fn actions(&self, state: &Self::State, out: &mut Vec<Self::Action>);
    /// The successor of `state` under `action`, or `None` when the action is not
    /// enabled after all (a guard that is easier to evaluate here than in `actions`).
    fn next_state(&self, state: &Self::State, action: &Self::Action) -> Option<Self::State>;
    /// Err(name and detail of the first violated invariant).
    fn check(&self, state: &Self::State) -> Result<(), String>;
}

/// Size of the explored state graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Explored {
    /// Distinct states discovered, including the initial ones.
    pub states: usize,
    /// Largest number of actions from an initial state to a discovered state.
    pub max_depth: usize,
}

pub enum Outcome<M: Model> {
    /// Every reachable state satisfies every invariant.
    Passed(Explored),
    /// `trace[0]` is an initial state (with no action). Each later entry is the
    /// action taken and the state it produced. The last state violates `invariant`.
    Violated {
        invariant: String,
        trace: Vec<(Option<M::Action>, M::State)>,
    },
    /// The search stopped after discovering `max_states` states without finding
    /// a violation. The space was not exhausted.
    LimitReached(Explored),
}

impl<M: Model> fmt::Debug for Outcome<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Passed(e) => f.debug_tuple("Passed").field(e).finish(),
            Outcome::LimitReached(e) => f.debug_tuple("LimitReached").field(e).finish(),
            Outcome::Violated { invariant, trace } => f
                .debug_struct("Violated")
                .field("invariant", invariant)
                .field("trace", trace)
                .finish(),
        }
    }
}

impl<M: Model> Outcome<M> {
    pub fn is_passed(&self) -> bool {
        matches!(self, Outcome::Passed(_))
    }

    /// The violated invariant's message, if any.
    pub fn violation(&self) -> Option<&str> {
        match self {
            Outcome::Violated { invariant, .. } => Some(invariant),
            _ => None,
        }
    }

    /// A human-readable report: the verdict, and for a violation one numbered
    /// line per trace step.
    pub fn report(&self) -> String {
        match self {
            Outcome::Passed(e) => {
                format!("passed: {} states, max depth {}", e.states, e.max_depth)
            }
            Outcome::LimitReached(e) => format!(
                "limit reached: {} states, max depth {} (space not exhausted)",
                e.states, e.max_depth
            ),
            Outcome::Violated { invariant, trace } => {
                let mut s = format!(
                    "violated: {invariant}\ntrace ({} steps):\n",
                    trace.len().saturating_sub(1)
                );
                // A run of three or more identical actions (such as clock ticks) is
                // printed once with its count and the state after the run.
                let mut i = 0;
                while i < trace.len() {
                    let (action, state) = &trace[i];
                    let Some(a) = action else {
                        s.push_str(&format!("  {i:>3}. init\n       {state:?}\n"));
                        i += 1;
                        continue;
                    };
                    let label = format!("{a:?}");
                    let mut j = i + 1;
                    while j < trace.len()
                        && trace[j].0.as_ref().is_some_and(|b| format!("{b:?}") == label)
                    {
                        j += 1;
                    }
                    if j - i >= 3 {
                        let last = &trace[j - 1].1;
                        s.push_str(&format!(
                            "  {i:>3}-{:<3} {label} x{}\n       {last:?}\n",
                            j - 1,
                            j - i
                        ));
                        i = j;
                    } else {
                        s.push_str(&format!("  {i:>3}. {label}\n       {state:?}\n"));
                        i += 1;
                    }
                }
                s
            }
        }
    }
}

/// Breadth-first exploration of every state reachable from `model.init_states()`.
///
/// Returns `Violated` with a shortest trace on the first state that fails
/// `model.check`, `LimitReached` when more than `max_states` distinct states
/// would be needed, and `Passed` when the reachable space is exhausted.
pub fn explore<M: Model>(model: &M, max_states: usize) -> Outcome<M> {
    let mut states: Vec<M::State> = Vec::new();
    // parent[i] = (index of the parent state, action taken); None for initial states.
    let mut parent: Vec<Option<(usize, M::Action)>> = Vec::new();
    let mut depth: Vec<usize> = Vec::new();
    let mut index: FxMap<M::State, usize> = FxMap::default();
    let mut queue: VecDeque<usize> = VecDeque::new();
    let mut max_depth = 0usize;

    let trace_to = |states: &Vec<M::State>,
                    parent: &Vec<Option<(usize, M::Action)>>,
                    mut i: usize|
     -> Vec<(Option<M::Action>, M::State)> {
        let mut rev = Vec::new();
        loop {
            match &parent[i] {
                None => {
                    rev.push((None, states[i].clone()));
                    break;
                }
                Some((p, a)) => {
                    rev.push((Some(a.clone()), states[i].clone()));
                    i = *p;
                }
            }
        }
        rev.reverse();
        rev
    };

    for s in model.init_states() {
        if index.contains_key(&s) {
            continue;
        }
        if states.len() >= max_states {
            return Outcome::LimitReached(Explored {
                states: states.len(),
                max_depth,
            });
        }
        let i = states.len();
        index.insert(s.clone(), i);
        states.push(s);
        parent.push(None);
        depth.push(0);
        if let Err(invariant) = model.check(&states[i]) {
            return Outcome::Violated {
                invariant,
                trace: trace_to(&states, &parent, i),
            };
        }
        queue.push_back(i);
    }

    let mut actions = Vec::new();
    while let Some(i) = queue.pop_front() {
        actions.clear();
        model.actions(&states[i], &mut actions);
        for a in actions.drain(..) {
            let Some(next) = model.next_state(&states[i], &a) else {
                continue;
            };
            let slot = match index.entry(next) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(v) => v,
            };
            if states.len() >= max_states {
                return Outcome::LimitReached(Explored {
                    states: states.len(),
                    max_depth,
                });
            }
            let j = states.len();
            let d = depth[i] + 1;
            max_depth = max_depth.max(d);
            states.push(slot.key().clone());
            slot.insert(j);
            parent.push(Some((i, a)));
            depth.push(d);
            if let Err(invariant) = model.check(&states[j]) {
                return Outcome::Violated {
                    invariant,
                    trace: trace_to(&states, &parent, j),
                };
            }
            queue.push_back(j);
        }
    }

    Outcome::Passed(Explored {
        states: states.len(),
        max_depth,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counter that can add one or three, bounded by `limit`.
    struct Counter {
        limit: u32,
        bad: Option<u32>,
    }

    impl Model for Counter {
        type State = u32;
        type Action = u32;
        fn init_states(&self) -> Vec<u32> {
            vec![0]
        }
        fn actions(&self, _: &u32, out: &mut Vec<u32>) {
            // +1 first, so a depth-first search would find the long trace 1,1,1,1,1,1.
            out.push(1);
            out.push(3);
        }
        fn next_state(&self, s: &u32, a: &u32) -> Option<u32> {
            let n = s + a;
            (n <= self.limit).then_some(n)
        }
        fn check(&self, s: &u32) -> Result<(), String> {
            match self.bad {
                Some(b) if *s == b => Err(format!("counter-not-{b}: reached {s}")),
                _ => Ok(()),
            }
        }
    }

    /// An unbounded counter: the space never ends.
    struct Unbounded;
    impl Model for Unbounded {
        type State = u64;
        type Action = ();
        fn init_states(&self) -> Vec<u64> {
            vec![0]
        }
        fn actions(&self, _: &u64, out: &mut Vec<()>) {
            out.push(());
        }
        fn next_state(&self, s: &u64, _: &()) -> Option<u64> {
            Some(s + 1)
        }
        fn check(&self, _: &u64) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn counter_passes_and_counts_states() {
        let out = explore(&Counter { limit: 10, bad: None }, 1_000);
        match out {
            Outcome::Passed(e) => {
                assert_eq!(e.states, 11, "states 0..=10");
                // 10 = 3+3+3+1: four steps is the shortest path to the deepest state.
                assert_eq!(e.max_depth, 4);
            }
            other => panic!("expected Passed, got {other:?}"),
        }
    }

    #[test]
    fn reachable_bad_state_gives_shortest_trace() {
        let out = explore(&Counter { limit: 20, bad: Some(6) }, 1_000);
        println!("{}", out.report());
        match out {
            Outcome::Violated { invariant, trace } => {
                assert!(invariant.starts_with("counter-not-6"), "{invariant}");
                // Shortest: 0 -> 3 -> 6. A depth-first search taking +1 first would give six steps.
                let steps: Vec<_> = trace.iter().map(|(a, s)| (*a, *s)).collect();
                assert_eq!(steps, vec![(None, 0), (Some(3), 3), (Some(3), 6)]);
            }
            other => panic!("expected Violated, got {other:?}"),
        }
    }

    #[test]
    fn violating_initial_state_has_one_step_trace() {
        let out = explore(&Counter { limit: 5, bad: Some(0) }, 10);
        match out {
            Outcome::Violated { trace, .. } => assert_eq!(trace, vec![(None, 0)]),
            other => panic!("expected Violated, got {other:?}"),
        }
    }

    #[test]
    fn limit_is_honoured() {
        match explore(&Unbounded, 100) {
            Outcome::LimitReached(e) => {
                assert_eq!(e.states, 100);
                assert_eq!(e.max_depth, 99);
            }
            other => panic!("expected LimitReached, got {other:?}"),
        }
        // A space of exactly max_states states is exhausted, not limited.
        assert!(explore(&Counter { limit: 9, bad: None }, 10).is_passed());
        assert!(matches!(
            explore(&Counter { limit: 10, bad: None }, 10),
            Outcome::LimitReached(_)
        ));
    }

    #[test]
    fn report_collapses_repeated_actions() {
        let out = explore(&Counter { limit: 20, bad: Some(5) }, 1_000);
        let report = out.report();
        // Shortest path to 5 is 1, 1, 3 (BFS tries +1 first): no run of three.
        assert!(report.contains("trace (3 steps)"), "{report}");
        struct Ticks;
        impl Model for Ticks {
            type State = u8;
            type Action = &'static str;
            fn init_states(&self) -> Vec<u8> {
                vec![0]
            }
            fn actions(&self, _: &u8, out: &mut Vec<&'static str>) {
                out.push("Tick");
            }
            fn next_state(&self, s: &u8, _: &&'static str) -> Option<u8> {
                Some(s + 1)
            }
            fn check(&self, s: &u8) -> Result<(), String> {
                if *s == 10 { Err("ten".into()) } else { Ok(()) }
            }
        }
        let report = explore(&Ticks, 100).report();
        assert!(report.contains("1-10  \"Tick\" x10"), "{report}");
    }

    #[test]
    fn exploration_is_deterministic() {
        let m = Counter { limit: 50, bad: Some(37) };
        let a = explore(&m, 1_000).report();
        let b = explore(&m, 1_000).report();
        assert_eq!(a, b);
        assert!(a.starts_with("violated"));
    }
}
