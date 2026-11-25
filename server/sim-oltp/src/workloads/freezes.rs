//! Owns every clock freeze of a run, so that each one ends within its bound.
//!
//! `World::frozen_at_read` is no signal to own a freeze by: a job stopped at a clock read goes on
//! as soon as something outside its node polls it, while the node stays frozen, and then nothing
//! is stopped at a read any more. A freeze rule also outlives whoever set it. Here the
//! `ClockFrozen` event itself starts the bound, whatever stopped the node.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use nabedra_rt::{
    sim::{ClockRead, SimEvent, World},
    time,
};
use tokio::sync::mpsc;

use crate::invariants::{Invariants, STEP};

/// How often an open freeze is looked at.
const TICK: Duration = Duration::from_millis(1);
/// How long after its bound a freeze may still be there before the harness is at fault.
const SLACK: Duration = Duration::from_millis(5);

/// The workers that the nemesis froze, and thaws itself. In the World while the overlay is on.
#[derive(Default)]
pub(crate) struct NemesisFrozen(pub(crate) BTreeSet<u64>);

/// Notes that the nemesis froze or thawed `node`.
pub(crate) fn nemesis(world: &World, node: u64, frozen: bool) {
    world.with_ext::<NemesisFrozen, _>(|nodes| {
        if frozen {
            nodes.0.insert(node);
        } else {
            nodes.0.remove(&node);
        }
    });
}

#[derive(Default)]
struct State {
    /// Stalls whose rule has not fired, and for how long each is to hold.
    armed: BTreeMap<u64, Duration>,
    /// The bound of a seeded freeze.
    seeded: Duration,
    /// Freezes that have not ended, by node: which freeze of the node it is.
    open: BTreeMap<u64, u64>,
    /// Freezes of each node so far.
    count: BTreeMap<u64, u64>,
    ended: u64,
    /// What [`Freezes::end_all`] ended: no freeze up to this one of the node goes on.
    stopped: BTreeMap<u64, u64>,
    /// Per node, the virtual ms from which and until which it is known to have been frozen.
    spans: BTreeMap<u64, Vec<(u64, u64)>>,
}

#[derive(Clone)]
pub(crate) struct Freezes {
    state: Arc<Mutex<State>>,
    world: std::sync::Weak<World>,
}

enum Seen {
    Froze(u64),
    Crashed(u64),
}

impl Freezes {
    /// Watches `world` for the rest of the run. A freeze that outlasts its bound is thawed and
    /// reported as the harness's fault, so that the run ends without a verdict it can't have.
    pub(crate) fn watch(world: &Arc<World>, checks: &Invariants) -> Self {
        let freezes = Self {
            state: Arc::default(),
            world: Arc::downgrade(world),
        };
        let (sender, mut seen) = mpsc::unbounded_channel();
        // The event comes from inside the stopped code, maybe on a helper thread.
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            let _ = match event {
                SimEvent::ClockFrozen { node } => sender.send(Seen::Froze(*node)),
                SimEvent::Crash { node } => sender.send(Seen::Crashed(*node)),
                _ => return,
            };
        }));
        world.insert_ext(NemesisFrozen::default());
        world.insert_ext(freezes.clone());
        let owner = freezes.clone();
        let checks = checks.clone();
        nabedra_rt::task::spawn(async move {
            while let Some(seen) = seen.recv().await {
                match seen {
                    Seen::Froze(node) => {
                        let (freeze, hold) = owner.froze(node);
                        nabedra_rt::task::spawn(owner.clone().end(
                            node,
                            freeze,
                            hold,
                            checks.clone(),
                        ));
                    }
                    Seen::Crashed(node) => owner.ended(node, None),
                }
            }
        });
        freezes
    }

    /// Stops `node` at `read`, for `hold`. False if it has a stall or a freeze already: two of
    /// them would be told apart by nothing.
    pub(crate) fn arm(&self, node: u64, read: ClockRead, hold: Duration) -> bool {
        let Some(world) = self.world.upgrade() else {
            return false;
        };
        {
            let mut state = self.lock();
            if state.armed.contains_key(&node) || state.open.contains_key(&node) {
                return false;
            }
            state.armed.insert(node, hold);
        }
        world.freeze_after_clock_read(node, read);
        true
    }

    /// Stops every process now and then, for at most `hold` each. Zero ends that.
    pub(crate) fn seed(&self, per_mille: u32, hold: Duration) {
        let Some(world) = self.world.upgrade() else {
            return;
        };
        let max = {
            let mut state = self.lock();
            // The bound of those that have begun stays.
            state.seeded = state.seeded.max(hold);
            if per_mille == 0 { Duration::ZERO } else { hold }
        };
        world.set_clock_freezes(per_mille, max);
    }

    /// Ends every freeze that has begun, and the seeded ones. A stall whose rule has not fired
    /// can't be taken back, so it will end as soon as it begins.
    pub(crate) fn end_all(&self) {
        self.seed(0, Duration::ZERO);
        let mut state = self.lock();
        state.stopped = state.open.clone();
        state.seeded = Duration::ZERO;
        for hold in state.armed.values_mut() {
            *hold = Duration::ZERO;
        }
    }

    /// The nodes with a freeze that has not ended.
    pub(crate) fn open(&self) -> BTreeSet<u64> {
        self.lock().open.keys().copied().collect()
    }

    /// When `node` is known to have been frozen, in virtual ms. A freeze is in it up to the last
    /// ms at which it was seen, so an interval is never longer than the freeze.
    pub(crate) fn spans(&self, node: u64) -> Vec<(u64, u64)> {
        self.lock().spans.get(&node).cloned().unwrap_or_default()
    }

    /// Whether a stall has not fired.
    pub(crate) fn armed(&self) -> bool {
        !self.lock().armed.is_empty()
    }

    /// Waits until none of `nodes` has an open freeze, or until `limit`. False at the limit.
    pub(crate) async fn settle(&self, nodes: &[u64], limit: Duration) -> bool {
        let mut left = limit;
        while self.open().iter().any(|node| nodes.contains(node)) {
            if left.is_zero() {
                return false;
            }
            time::sleep(STEP).await;
            left = left.saturating_sub(STEP);
        }
        true
    }

    /// Freezes that began, and that ended.
    pub(crate) fn counts(&self) -> (u64, u64) {
        let state = self.lock();
        (state.ended + state.open.len() as u64, state.ended)
    }

    fn froze(&self, node: u64) -> (u64, Duration) {
        let mut state = self.lock();
        // Either the node's stall or a seeded freeze stopped it. With both possible the bound is
        // the longer one, and the stall's rule has its own event if it fires later.
        let stall = state.armed.remove(&node);
        let hold = stall.unwrap_or(state.seeded);
        let count = state.count.entry(node).or_default();
        *count += 1;
        let freeze = *count;
        state.open.insert(node, freeze);
        let now = self.now();
        state.spans.entry(node).or_default().push((now, now));
        (freeze, hold)
    }

    /// Ends the node's open freeze, or only `freeze`.
    fn ended(&self, node: u64, freeze: Option<u64>) {
        let mut state = self.lock();
        let open = state.open.get(&node).copied();
        if open.is_some() && freeze.is_none_or(|freeze| open == Some(freeze)) {
            state.open.remove(&node);
            state.ended += 1;
        }
    }

    fn is_open(&self, node: u64, freeze: u64) -> bool {
        self.lock().open.get(&node) == Some(&freeze)
    }

    fn now(&self) -> u64 {
        self.world
            .upgrade()
            .map_or(0, |world| crate::invariants::elapsed_ms(&world))
    }

    /// Whether `freeze` goes on: it is open, the node is frozen and nothing ended it.
    fn goes_on(&self, world: &World, node: u64, freeze: u64) -> bool {
        let stopped = self.lock().stopped.get(&node) >= Some(&freeze);
        !stopped && self.is_open(node, freeze) && world.is_frozen(node)
    }

    async fn end(self, node: u64, freeze: u64, hold: Duration, checks: Invariants) {
        let Some(world) = self.world.upgrade() else {
            return;
        };
        let deadline = world.elapsed() + hold;
        // The simulator thaws a seeded freeze by itself, at a time it doesn't tell.
        while world.elapsed() < deadline && self.goes_on(&world, node, freeze) {
            time::sleep(TICK).await;
            if self.goes_on(&world, node, freeze) {
                let now = crate::invariants::elapsed_ms(&world);
                let mut state = self.lock();
                if let Some(span) = state
                    .spans
                    .get_mut(&node)
                    .and_then(|spans| spans.last_mut())
                {
                    span.1 = now;
                }
            }
        }
        let nemesis = |world: &World| {
            world
                .with_ext::<NemesisFrozen, _>(|nodes| nodes.0.contains(&node))
                .unwrap_or(false)
        };
        if self.is_open(node, freeze) && !nemesis(&world) {
            world.thaw(node);
        }
        // Another freeze of the node may begin at once. It has its own event and end.
        self.ended(node, Some(freeze));
        time::sleep(SLACK).await;
        let again = self.lock().count.get(&node) != Some(&freeze);
        if world.is_frozen(node) && !again && !nemesis(&world) {
            world.thaw(node);
            checks.violate(
                &world,
                "harness",
                format!(
                    "n{node} was still frozen {SLACK:?} after the {hold:?} of its clock freeze"
                ),
            );
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A node that is frozen although nothing holds it: not the nemesis, and no freeze of this run.
pub(crate) fn abandoned(world: &World, freezes: &Freezes, nodes: &[u64]) -> Vec<u64> {
    let open = freezes.open();
    let nemesis = world
        .with_ext::<NemesisFrozen, _>(|nodes| nodes.0.clone())
        .unwrap_or_default();
    nodes
        .iter()
        .copied()
        .filter(|node| world.is_frozen(*node) && !open.contains(node) && !nemesis.contains(node))
        .collect()
}

#[cfg(test)]
mod tests {
    use nabedra_rt::sim::{self, NodeClock, in_node};

    use super::*;

    const N: u64 = 1;

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    /// Starts two tasks of the node that read its clock: a poll, and blocking work that a task
    /// outside the node waits for, as a caller of the node does.
    async fn work() {
        in_node(N, async {
            nabedra_rt::task::spawn(async {
                loop {
                    let _ = time::now();
                    time::sleep(ms(2)).await;
                }
            });
        })
        .await;
        nabedra_rt::task::spawn(in_node(N, async {
            loop {
                let work = || {
                    let _ = time::now();
                };
                let _ = nabedra_rt::task::spawn_blocking(work).await;
                time::sleep(ms(3)).await;
            }
        }));
    }

    /// Runs `body` with a watched node whose tasks read the clock all the time.
    fn watched<F: Future<Output = ()> + Send + 'static>(
        body: impl FnOnce(Arc<World>, Freezes, Invariants) -> F + Send + 'static,
    ) -> Vec<String> {
        sim::run(7, move |world| async move {
            world.add_node(N, NodeClock::default());
            let checks = Invariants::new();
            let freezes = Freezes::watch(&world, &checks);
            work().await;
            body(world, freezes, checks.clone()).await;
            checks.violations()
        })
    }

    /// The longest time, in ms, that `N` was frozen without a break until `until`.
    async fn longest(world: &World, until: Duration) -> u64 {
        let (mut longest, mut now) = (0, 0);
        while world.elapsed() < until {
            time::sleep(ms(1)).await;
            now = if world.is_frozen(N) { now + 1 } else { 0 };
            longest = longest.max(now);
        }
        longest
    }

    /// Seed 9: the stall stopped a job, and the job went on at once although its node was frozen.
    /// Whatever the read stops, the stall ends after its time.
    #[test]
    fn a_stall_ends_whatever_it_stopped() {
        for nth in 1..40 {
            let found = watched(move |world, freezes, _| async move {
                assert!(freezes.arm(N, ClockRead::nth(nth), ms(50)));
                // A second stall of the node would have no event of its own.
                assert!(!freezes.arm(N, ClockRead::nth(1), ms(5)));
                let longest = longest(&world, ms(600)).await;
                assert!((45..=52).contains(&longest), "read {nth}: {longest} ms");
                assert_eq!(freezes.counts(), (1, 1), "read {nth}");
                assert!(freezes.settle(&[N], Duration::ZERO).await);
                // What the quorum is judged by is within the freeze.
                let [(from, to)] = freezes.spans(N)[..] else {
                    panic!("{:?}", freezes.spans(N));
                };
                assert!((45..=50).contains(&(to - from)), "{from} {to}");
            });
            assert!(found.is_empty(), "{found:?}");
        }
    }

    /// `stop` ends a stall that holds, and one that has not begun ends when it begins.
    #[test]
    fn the_end_of_the_window_ends_every_stall() {
        let found = watched(|world, freezes, _| async move {
            assert!(freezes.arm(N, ClockRead::nth(1), ms(5_000)));
            time::sleep(ms(20)).await;
            assert!(world.is_frozen(N));
            freezes.end_all();
            time::sleep(ms(3)).await;
            assert!(!world.is_frozen(N) && freezes.open().is_empty());
            assert!(freezes.arm(N, ClockRead::nth(30), ms(5_000)));
            freezes.end_all();
            assert!(freezes.armed());
            let until = world.elapsed() + ms(400);
            assert!(longest(&world, until).await <= 1);
            assert!(!freezes.armed() && freezes.open().is_empty());
            // Seeded freezes end, too.
            freezes.seed(1_000, ms(5_000));
            time::sleep(ms(20)).await;
            freezes.end_all();
            time::sleep(ms(5)).await;
            assert!(!world.is_frozen(N) && abandoned(&world, &freezes, &[N]).is_empty());
            assert!(longest(&world, world.elapsed() + ms(100)).await == 0);
        });
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn seeded_freezes_end_with_and_without_a_stall() {
        for stall in [false, true] {
            let found = watched(move |world, freezes, _| async move {
                freezes.seed(100, ms(30));
                if stall {
                    freezes.arm(N, ClockRead::nth(5), ms(40));
                }
                let longest = longest(&world, ms(900)).await;
                freezes.seed(0, Duration::ZERO);
                // Back to back freezes may add up, but each has its own bound.
                assert!(longest > 0 && longest < 300, "{longest} ms");
                assert!(freezes.settle(&[N], ms(200)).await);
                assert!(abandoned(&world, &freezes, &[N]).is_empty());
                let (began, ended) = freezes.counts();
                assert!(began > 3 && began == ended, "{began} {ended}");
            });
            assert!(found.is_empty(), "{found:?}");
        }
    }

    /// The nemesis freezes the same node, and thaws it later than the stall would.
    #[test]
    fn a_stall_leaves_the_nemesis_its_freeze() {
        let found = watched(|world, freezes, _| async move {
            assert!(freezes.arm(N, ClockRead::nth(1), ms(20)));
            time::sleep(ms(10)).await;
            assert!(world.is_frozen(N));
            world.freeze(N);
            nemesis(&world, N, true);
            time::sleep(ms(60)).await;
            assert!(world.is_frozen(N) && freezes.open().is_empty());
            nemesis(&world, N, false);
            world.thaw(N);
            assert!(!world.is_frozen(N));
        });
        assert!(found.is_empty(), "{found:?}");
    }

    /// A crash ends the node's freeze, and an unfired rule keeps its owner until the restart.
    #[test]
    fn a_crash_ends_a_freeze_and_keeps_a_stall() {
        let found = watched(|world, freezes, _| async move {
            assert!(freezes.arm(N, ClockRead::nth(1), ms(500)));
            time::sleep(ms(10)).await;
            world.crash(N);
            time::sleep(ms(1)).await;
            assert!(freezes.open().is_empty());
            assert!(freezes.arm(N, ClockRead::nth(3), ms(20)));
            time::sleep(ms(50)).await;
            assert!(freezes.armed());
            world.restart(N);
            work().await;
            let until = world.elapsed() + ms(300);
            let longest = longest(&world, until).await;
            assert!(
                (15..=22).contains(&longest),
                "{longest} ms, {:?}",
                freezes.counts()
            );
            assert!(freezes.settle(&[N], Duration::ZERO).await);
        });
        assert!(found.is_empty(), "{found:?}");
    }

    /// A freeze that comes back by other means than this module is the harness's fault, and the
    /// node goes on, so that the run ends.
    #[test]
    fn a_freeze_that_outlasts_its_bound_is_a_harness_error() {
        let found = watched(|world, freezes, _| async move {
            assert!(freezes.arm(N, ClockRead::nth(1), ms(20)));
            time::sleep(ms(10)).await;
            let refreeze = world.clone();
            nabedra_rt::task::spawn(async move {
                while refreeze.is_frozen(N) {
                    time::sleep(Duration::from_micros(100)).await;
                }
                refreeze.freeze(N);
            });
            time::sleep(ms(40)).await;
            assert!(!world.is_frozen(N));
        });
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("harness: n1 was still frozen"),
            "{found:?}"
        );
        // A node that nothing holds is seen at the end, too.
        watched(|world, freezes, _| async move {
            world.freeze(N);
            assert_eq!(abandoned(&world, &freezes, &[N]), [N]);
            world.thaw(N);
        });
    }
}
