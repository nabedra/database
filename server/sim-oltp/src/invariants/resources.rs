//! Resource rules: ExactlyOnceRelease, ReleaseAfterWork and the form of ResourceBaseline that
//! the `resources` overlay adds. Each is a pure state machine over probes or samples.
//!
//! # Text
//!
//! - All of them: OLTP register, OLTP-G09: "Resources stay within budgets and each reservation
//!   settles exactly once." Its hazard: "Work can fail, panic, cancel, or time out while it owns
//!   a reservation."
//! - ExactlyOnceRelease. Design 3.7 (`plans/testing/phase-10-11-oltp-sim.md`): "one release
//!   per acquire". Scheduler contract, F03: "All owned work settles once; compute and byte
//!   counters return to baseline; no dependency is abandoned while still active." OLTP
//!   contract, O05/F03: "assert every charge returns to baseline once". O05/O11/F03: "release
//!   every retained charge once".
//! - ReleaseAfterWork. Design 3.7: "every `GuardReleased` follows its `WorkDone` on the
//!   trace". OLTP contract, O05/F03: "abort the awaiting future, and assert its CPU/memory and
//!   native execution ownership remain charged while native work is still live".
//! - ResourceBaseline/overlay. Design 3.7: "at quiescence, every `WorkerSharedResources`
//!   ledger (charged bytes per class, native leases, pending guards, scratch leases, disk
//!   reservations) and every `RangeClock` pending set is back at baseline". F03 as above.
//!   OLTP contract, O13: "No reservation leaks". The rule named ResourceBaseline
//!   ([`super::Invariants::check_resources`]) is unchanged and still judges the whole run.
//!
//! # What they cannot see
//!
//! - Guards without probes: compute grants, byte charges, scratch leases, disk reservations and
//!   the permits of HTTP, transport lanes and direct admission are only counted, by the ledgers
//!   of ResourceBaseline. HTTP and transport lane permits are in no ledger.
//! - A release whose ledger is wrong for less than the time between two ledger snapshots, and
//!   ledgers while nobody takes a snapshot.
//! - Guards of a dead incarnation: they end with their process, whatever its destructors do.
//! - A guard that is held for ever is seen only at a quiescent point that began after it was
//!   acquired. Two guards that swap their releases look right.
//! - A charge that the point before the overlay already held, and one that the overlay's point
//!   after it does not hold for three samples in a row.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use nabedra_oltp_types::sim_probe::{GuardKind, Ledger, Probe};

use super::{
    SETTLED_PASSES, next_pass,
    protocol::{EXACTLY_ONCE_RELEASE, Ledgers, Violation},
};

pub const RELEASE_AFTER_WORK: &str = "ReleaseAfterWork";
pub const OVERLAY_BASELINE: &str = "ResourceBaseline/overlay";

/// The guards of a run, from `GuardAcquired`, `WorkStarted`, `WorkDone` and `GuardReleased`.
#[derive(Default)]
pub struct Guards {
    /// The state of each named guard, by its name.
    states: BTreeMap<u64, State>,
    /// Those that a live process holds.
    open: BTreeSet<u64>,
    counts: GuardCounts,
}

/// How often something happened to a guard of a live process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GuardCounts {
    pub acquired: u64,
    pub released: u64,
    /// Released without work: the caller gave up before the work was dispatched.
    pub unstarted: u64,
    /// The caller was cancelled before its work returned.
    pub abandoned: u64,
}

#[derive(Clone, Copy, Debug)]
struct State {
    node: Option<u64>,
    kind: GuardKind,
    t_ns: u64,
    /// Whether the process that acquired it ended.
    dead: bool,
    started: bool,
    done: bool,
    released: bool,
}

impl Guards {
    /// Checks one probe of a live process. Other probes change nothing.
    pub fn observe(&mut self, node: Option<u64>, t_ns: u64, probe: &Probe) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut violate = |rule, detail: String| out.push((rule, detail));
        let at = node.map_or_else(|| "a process".to_owned(), |node| format!("n{node}"));
        match *probe {
            Probe::GuardAcquired { guard, kind, class } => {
                self.counts.acquired += 1;
                let state = State {
                    node,
                    kind,
                    t_ns,
                    dead: false,
                    started: false,
                    done: false,
                    released: false,
                };
                self.open.insert(guard);
                if self.states.insert(guard, state).is_some() {
                    violate(
                        EXACTLY_ONCE_RELEASE,
                        format!("{at} acquired {class} guard {guard} twice"),
                    );
                }
            }
            Probe::WorkStarted { guard } => {
                let Some(state) = self.states.get_mut(&guard) else {
                    violate(
                        RELEASE_AFTER_WORK,
                        format!("{at} started work under guard {guard}, which nobody acquired"),
                    );
                    return out;
                };
                if state.released && !state.dead {
                    violate(
                        RELEASE_AFTER_WORK,
                        format!("{at} started work under {state} {guard} after its release"),
                    );
                }
                state.started = true;
            }
            Probe::WorkDone { guard, abandoned } => {
                if let Some(state) = self.states.get_mut(&guard) {
                    state.done = true;
                }
                self.counts.abandoned += u64::from(abandoned);
            }
            Probe::GuardReleased { guard } => {
                let Some(state) = self.states.get_mut(&guard) else {
                    violate(
                        EXACTLY_ONCE_RELEASE,
                        format!("{at} released guard {guard}, which nobody acquired"),
                    );
                    return out;
                };
                self.counts.released += 1;
                if state.released {
                    violate(
                        EXACTLY_ONCE_RELEASE,
                        format!("{at} released {state} {guard} twice"),
                    );
                } else if state.started && !state.done && !state.dead {
                    violate(
                        RELEASE_AFTER_WORK,
                        format!("{at} released {state} {guard} before its work was done"),
                    );
                }
                self.counts.unstarted += u64::from(!state.released && !state.started);
                state.released = true;
                self.open.remove(&guard);
            }
            _ => {}
        }
        out
    }

    /// The node's process ended, and its guards with it.
    pub fn crashed(&mut self, node: u64) {
        let states = &mut self.states;
        self.open.retain(|guard| {
            let Some(state) = states.get_mut(guard) else {
                return false;
            };
            state.dead |= state.node == Some(node);
            !state.dead
        });
    }

    pub fn counts(&self) -> GuardCounts {
        self.counts
    }

    /// Guards of one kind that `node`'s live process holds.
    pub fn held(&self, node: u64, kind: GuardKind) -> u64 {
        self.outstanding(node)
            .filter(|(_, state)| state.kind == kind)
            .count() as u64
    }

    fn outstanding(&self, node: u64) -> impl Iterator<Item = (u64, &State)> {
        self.open.iter().filter_map(move |guard| {
            let state = self.states.get(guard)?;
            (state.node == Some(node)).then_some((*guard, state))
        })
    }

    /// One release per acquire: a ledger that counts guards holds exactly those acquired and
    /// not released. Returns whether there was something to compare, and what differs.
    pub fn counted(&self, node: u64, ledgers: &Ledgers) -> (u64, Vec<Violation>) {
        let mut compared = 0;
        let mut out = Vec::new();
        for (ledger, kind) in [
            (Ledger::NativeLeases, GuardKind::NativeLease),
            (Ledger::PendingWrites, GuardKind::PendingWrite),
        ] {
            let Some(count) = ledgers.get(&(ledger, None)) else {
                continue;
            };
            compared += 1;
            let held = self.held(node, kind);
            if *count != held {
                out.push((
                    EXACTLY_ONCE_RELEASE,
                    format!("n{node} counts {count} in {ledger:?} and holds {held} guards"),
                ));
            }
        }
        (compared, out)
    }

    /// At a quiescent point that began at `since_ns`: a guard from before it must have been
    /// released.
    pub fn lost(&self, node: u64, since_ns: u64) -> Vec<Violation> {
        self.outstanding(node)
            .filter(|(_, state)| state.t_ns < since_ns)
            .map(|(guard, state)| {
                (
                    EXACTLY_ONCE_RELEASE,
                    format!(
                        "n{node} never released {state} {guard}, acquired at {} ns{}",
                        state.t_ns,
                        if state.done { ", after its work" } else { "" }
                    ),
                )
            })
            .collect()
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.kind {
            GuardKind::NativeLease => "native lease",
            GuardKind::PendingWrite => "pending write",
        })
    }
}

/// The resource point before the overlay's quiet stage, and the samples after it.
#[derive(Default)]
pub struct Stage {
    nodes: BTreeMap<u64, Point>,
}

/// One node's point before the stage and its samples after it, up to the last one.
#[derive(Default)]
struct Point {
    before: Option<Ledgers>,
    steady: u32,
    at: Duration,
    /// Samples in a row equal to `before`, up to and including the last one.
    passes: u32,
    off: Vec<String>,
}

impl Stage {
    /// Samples `node` before the stage. True once the last [`SETTLED_PASSES`] samples, each at
    /// least a step after the one before, were equal, so that no message in flight is in them.
    pub fn before(&mut self, node: u64, at: Duration, ledgers: &Ledgers) -> bool {
        let point = self.nodes.entry(node).or_default();
        (point.at, point.steady) = match &point.before {
            Some(before) if before == ledgers => next_pass(at, point.at, point.steady),
            _ => (at, 1),
        };
        point.before = Some(ledgers.clone());
        point.steady >= SETTLED_PASSES
    }

    /// Samples `node` after the stage. True once the last [`SETTLED_PASSES`] samples, each at
    /// least a step after the one before, equalled the point before the stage, and no sample
    /// between them differed.
    pub fn after(&mut self, node: u64, at: Duration, ledgers: &Ledgers) -> bool {
        let point = self.nodes.entry(node).or_default();
        let off = match &point.before {
            Some(before) => differences(before, ledgers),
            None => vec!["no point before the stage".into()],
        };
        if off.is_empty() {
            (point.at, point.passes) = next_pass(at, point.at, point.passes);
        } else {
            point.passes = 0;
            point.off = off;
        }
        point.passes >= SETTLED_PASSES
    }

    /// What keeps `node` from being back at its point before the stage.
    pub fn judge(&self, node: u64) -> Option<Violation> {
        let point = self.nodes.get(&node);
        let settled = point.is_some_and(|point| point.passes >= SETTLED_PASSES);
        let last = point.map_or_else(
            || "no sample".to_owned(),
            |point| format!("last off by {}", point.off.join(", ")),
        );
        (!settled).then(|| {
            (
                OVERLAY_BASELINE,
                format!("n{node} is not back at its point before the overlay's stage: {last}"),
            )
        })
    }
}

/// The ledgers that differ. One that only one side has is zero on the other.
fn differences(before: &Ledgers, after: &Ledgers) -> Vec<String> {
    let keys: BTreeSet<_> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .filter_map(|key| {
            let at = |ledgers: &Ledgers| ledgers.get(key).copied().unwrap_or(0);
            let group = key
                .1
                .map_or_else(String::new, |group| format!(" of group {group}"));
            (at(before) != at(after))
                .then(|| format!("{:?}{group} {} (before {})", key.0, at(after), at(before)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::STEP;

    const LEASE: GuardKind = GuardKind::NativeLease;
    const WRITE: GuardKind = GuardKind::PendingWrite;

    fn acquire(guard: u64, kind: GuardKind) -> Probe {
        Probe::GuardAcquired {
            guard,
            kind,
            class: "query",
        }
    }
    fn release(guard: u64) -> Probe {
        Probe::GuardReleased { guard }
    }
    fn start(guard: u64) -> Probe {
        Probe::WorkStarted { guard }
    }
    fn done(guard: u64) -> Probe {
        Probe::WorkDone {
            guard,
            abandoned: false,
        }
    }

    /// The kinds that the probes of node 1, one nanosecond apart, violate.
    fn kinds(probes: &[Probe]) -> Vec<&'static str> {
        run(probes).1
    }
    fn run(probes: &[Probe]) -> (Guards, Vec<&'static str>) {
        let mut guards = Guards::default();
        let found = probes
            .iter()
            .enumerate()
            .flat_map(|(t, probe)| guards.observe(Some(1), t as u64, probe))
            .map(|(kind, _)| kind)
            .collect();
        (guards, found)
    }

    #[test]
    fn legal_guards() {
        for kind in [LEASE, WRITE] {
            // Work that ran, a caller that gave up first, and work that overlaps another.
            assert!(kinds(&[acquire(1, kind), start(1), done(1), release(1)]).is_empty());
            assert!(kinds(&[acquire(1, kind), release(1)]).is_empty());
            let overlapping = [
                acquire(1, kind),
                acquire(2, kind),
                start(2),
                start(1),
                done(1),
                release(1),
                done(2),
                release(2),
            ];
            let (guards, found) = run(&overlapping);
            assert!(found.is_empty());
            assert!(guards.lost(1, u64::MAX).is_empty());
            assert_eq!(guards.held(1, kind), 0);
        }
    }

    #[test]
    fn a_guard_released_twice_or_by_nobody() {
        let twice = [acquire(1, LEASE), start(1), done(1), release(1), release(1)];
        assert_eq!(kinds(&twice), [EXACTLY_ONCE_RELEASE]);
        // Two errors that cancel out in a count: one lease twice, another never.
        let cancelling = [acquire(1, LEASE), acquire(2, LEASE), release(1), release(1)];
        assert_eq!(kinds(&cancelling), [EXACTLY_ONCE_RELEASE]);
        assert_eq!(kinds(&[release(7)]), [EXACTLY_ONCE_RELEASE]);
        assert_eq!(
            kinds(&[acquire(1, LEASE), acquire(1, LEASE)]),
            [EXACTLY_ONCE_RELEASE]
        );
    }

    #[test]
    fn a_guard_never_released() {
        let (guards, found) = run(&[acquire(1, LEASE), start(1), done(1), acquire(2, WRITE)]);
        assert!(found.is_empty());
        // The point began at 3 ns: guard 2 may still be in use.
        let lost = guards.lost(1, 3);
        assert_eq!(lost.len(), 1);
        assert!(lost[0].1.contains("after its work"), "{lost:?}");
        assert_eq!(guards.lost(1, 4).len(), 2);
        assert!(guards.lost(2, 4).is_empty());
    }

    #[test]
    fn a_process_that_ended_holds_no_guard() {
        let mut guards = Guards::default();
        guards.observe(Some(1), 0, &acquire(1, LEASE));
        guards.observe(Some(1), 0, &start(1));
        guards.observe(Some(2), 0, &acquire(2, LEASE));
        guards.crashed(1);
        assert!(guards.lost(1, u64::MAX).is_empty());
        assert_eq!(guards.lost(2, u64::MAX).len(), 1);
        // Its destructors may run late, with the work unfinished, but each only once.
        assert!(guards.observe(Some(1), 1, &release(1)).is_empty());
        assert_eq!(guards.observe(Some(1), 2, &release(1)).len(), 1);
    }

    #[test]
    fn a_ledger_counts_the_guards_held() {
        let (guards, _) = run(&[
            acquire(1, LEASE),
            acquire(2, LEASE),
            release(1),
            acquire(3, WRITE),
        ]);
        let ledgers = |leases, writes| {
            Ledgers::from([
                ((Ledger::NativeLeases, None), leases),
                ((Ledger::PendingWrites, None), writes),
                ((Ledger::ComputeGuards, None), 9),
            ])
        };
        assert_eq!(guards.counted(1, &ledgers(1, 1)), (2, vec![]));
        // A release that the ledger took twice, and one it missed.
        assert_eq!(guards.counted(1, &ledgers(0, 1)).1.len(), 1);
        assert_eq!(guards.counted(1, &ledgers(1, 2)).1.len(), 1);
        // Nothing to compare is not a pass.
        assert_eq!(guards.counted(1, &Ledgers::new()), (0, vec![]));
    }

    #[test]
    fn work_outside_its_guard() {
        // Released while the work runs, and before it starts.
        let during = [acquire(1, LEASE), start(1), release(1), done(1)];
        assert_eq!(kinds(&during), [RELEASE_AFTER_WORK]);
        let before = [acquire(1, WRITE), release(1), start(1), done(1)];
        assert_eq!(kinds(&before), [RELEASE_AFTER_WORK]);
        assert_eq!(kinds(&[start(3)]), [RELEASE_AFTER_WORK]);
    }

    #[test]
    fn counts_say_what_was_cancelled() {
        let (guards, _) = run(&[
            acquire(1, LEASE),
            release(1),
            acquire(2, LEASE),
            start(2),
            Probe::WorkDone {
                guard: 2,
                abandoned: true,
            },
            release(2),
        ]);
        let counts = GuardCounts {
            acquired: 2,
            released: 2,
            unstarted: 1,
            abandoned: 1,
        };
        assert_eq!(guards.counts(), counts);
    }

    fn charged(bytes: u64) -> Ledgers {
        Ledgers::from([((Ledger::ChargedBytes("data"), None), bytes)])
    }

    /// Judges a node that holds `held(ms)` bytes `ms` after the stage, sampled every 5 ms.
    fn judged(held: impl Fn(u64) -> u64) -> Option<Violation> {
        let mut stage = Stage::default();
        let mut now = Duration::ZERO;
        while !stage.before(1, now, &charged(1_000)) {
            now += STEP;
        }
        assert_eq!(now, 2 * STEP);
        for ms in (0..2_000).step_by(5) {
            let at = now + Duration::from_millis(ms);
            if stage.after(1, at, &charged(held(ms))) {
                break;
            }
        }
        stage.judge(1)
    }

    #[test]
    fn a_stage_that_returns_to_its_point() {
        assert!(judged(|_| 1_000).is_none());
        // Work that ends within the limit, and a message now and then.
        assert!(judged(|ms| if ms < 700 { 1_564 } else { 1_000 }).is_none());
        assert!(judged(|ms| if ms % 50 < 2 { 1_564 } else { 1_000 }).is_none());
    }

    #[test]
    fn a_stage_that_leaks() {
        let leaks: [fn(u64) -> u64; 4] = [
            |_| 1_564,
            |_| 999,
            // A charge that comes back every 20 ms, and one that stays away for one sample.
            |ms| if ms % 20 < 10 { 1_564 } else { 1_000 },
            |ms| if ms == 500 { 1_000 } else { 1_564 },
        ];
        for held in leaks {
            let found = judged(held).expect("a leak");
            assert_eq!(found.0, OVERLAY_BASELINE);
            assert!(found.1.contains("ChargedBytes"), "{}", found.1);
        }
        // A new ledger starts at zero, and a missing one ends there.
        assert_eq!(differences(&Ledgers::new(), &charged(5)).len(), 1);
        assert_eq!(differences(&charged(5), &Ledgers::new()).len(), 1);
        assert!(differences(&Ledgers::new(), &charged(0)).is_empty());
        assert!(Stage::default().judge(1).is_some());
    }

    #[test]
    fn an_unsteady_point_is_no_point() {
        let mut stage = Stage::default();
        let mut now = Duration::ZERO;
        for (bytes, steady) in [(5, false), (6, false), (6, false), (6, true)] {
            assert_eq!(stage.before(1, now, &charged(bytes)), steady);
            now += STEP;
        }
        // Samples at one instant are one sample.
        let mut again = Stage::default();
        for _ in 0..5 {
            assert!(!again.before(1, now, &charged(6)));
        }
    }
}
