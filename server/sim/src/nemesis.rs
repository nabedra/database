//! The nemesis scheduler: weighted profiles of fault actions, drawn from the workload's
//! [`Gen`] so every decision lands on the choice tape and the shrinker can simplify it.
//!
//! A [`Profile`] lists a harness's actions with weights and the gap between actions.
//! Profiles are generic: a sweep uses every profile ([`Profile::round_robin`]), and none is
//! written for one bug. [`Nemesis::poll`] returns the follow-ups that are due (a restart
//! or a resume the harness scheduled with [`Nemesis::after`]), then a new action when the
//! gap has passed. List the mildest action first: the shrinker moves choices toward it.

use std::collections::BTreeMap;

use nabedra_check::Gen;

/// Actions keyed by `(due time, insertion order)`, so equal times keep their order.
#[derive(Clone, Debug)]
pub struct Timeline<A> {
    events: BTreeMap<(u64, u64), A>,
    seq: u64,
}

impl<A> Default for Timeline<A> {
    fn default() -> Self {
        Self {
            events: BTreeMap::new(),
            seq: 0,
        }
    }
}

impl<A> Timeline<A> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, at_ms: u64, action: A) {
        self.events.insert((at_ms, self.seq), action);
        self.seq += 1;
    }

    /// Removes and returns every action due at or before `now_ms`, in order.
    pub fn due(&mut self, now_ms: u64) -> Vec<A> {
        let Some(bound) = now_ms.checked_add(1) else {
            return self.drain();
        };
        let later = self.events.split_off(&(bound, 0));
        std::mem::replace(&mut self.events, later)
            .into_values()
            .collect()
    }

    /// Removes and returns every pending action, in order.
    pub fn drain(&mut self) -> Vec<A> {
        std::mem::take(&mut self.events).into_values().collect()
    }

    /// When the next action is due.
    pub fn next_at(&self) -> Option<u64> {
        self.events.keys().next().map(|(at, _)| *at)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// A weighted set of actions and the gap between them.
#[derive(Clone, Debug)]
pub struct Profile<A> {
    pub name: &'static str,
    actions: Vec<(A, u64)>,
    /// The gap before the next action is uniform in `[gap_ms.0, gap_ms.1]`.
    pub gap_ms: (u64, u64),
}

impl<A: Clone> Profile<A> {
    pub fn new(name: &'static str, gap_ms: (u64, u64)) -> Self {
        assert!(gap_ms.0 <= gap_ms.1, "profile {name}: gap {gap_ms:?}");
        Self {
            name,
            actions: Vec::new(),
            gap_ms,
        }
    }

    /// Adds `action` with `weight`. A zero weight leaves the action out.
    pub fn with(mut self, action: A, weight: u64) -> Self {
        if weight > 0 {
            self.actions.push((action, weight));
        }
        self
    }

    pub fn actions(&self) -> impl Iterator<Item = (&A, u64)> {
        self.actions
            .iter()
            .map(|(action, weight)| (action, *weight))
    }

    pub fn total_weight(&self) -> u64 {
        self.actions.iter().map(|(_, weight)| weight).sum()
    }

    /// One action, with probability proportional to its weight. One choice on the tape.
    pub fn pick(&self, g: &mut Gen) -> A {
        let total = self.total_weight();
        assert!(total > 0, "profile {} has no actions", self.name);
        let mut choice = g.below(total);
        for (action, weight) in &self.actions {
            if choice < *weight {
                return action.clone();
            }
            choice -= weight;
        }
        unreachable!("the choice is below the total weight")
    }

    /// The gap before the next action. One choice on the tape; shrinks toward the shortest.
    pub fn gap(&self, g: &mut Gen) -> u64 {
        let (lo, hi) = self.gap_ms;
        lo + g.below(hi - lo + 1)
    }

    /// The profile seed `seed` uses in a sweep over `profiles`: seeds cycle through them.
    pub fn round_robin(profiles: &[Self], seed: u64) -> &Self {
        assert!(!profiles.is_empty(), "no nemesis profiles");
        &profiles[(seed % profiles.len() as u64) as usize]
    }
}

/// The nemesis of one run.
#[derive(Clone, Debug)]
pub struct Nemesis<A> {
    profile: Profile<A>,
    next_at: Option<u64>,
    follow_ups: Timeline<A>,
    picked: u64,
}

impl<A: Clone> Nemesis<A> {
    /// A nemesis whose first action comes one gap after `start_ms`.
    pub fn new(profile: Profile<A>, start_ms: u64, g: &mut Gen) -> Self {
        let next_at = Some(start_ms + profile.gap(g));
        Self {
            profile,
            next_at,
            follow_ups: Timeline::new(),
            picked: 0,
        }
    }

    pub fn profile(&self) -> &Profile<A> {
        &self.profile
    }

    /// Actions picked from the profile so far (follow-ups not counted).
    pub fn picked(&self) -> u64 {
        self.picked
    }

    /// Schedules a follow-up, such as the restart after a crash.
    pub fn after(&mut self, at_ms: u64, action: A) {
        self.follow_ups.push(at_ms, action);
    }

    /// The due follow-ups in time order, then one new action if its time has come. Picking
    /// draws the action and the next gap from `g`. After [`Nemesis::stop`] only
    /// follow-ups are returned.
    pub fn poll(&mut self, now_ms: u64, g: &mut Gen) -> Vec<A> {
        let mut actions = self.follow_ups.due(now_ms);
        if self.next_at.is_some_and(|at| at <= now_ms) {
            actions.push(self.profile.pick(g));
            self.picked += 1;
            self.next_at = Some(now_ms + self.profile.gap(g));
        }
        actions
    }

    /// Stops picking new actions and returns every pending follow-up, in order, for the
    /// harness to apply before it heals.
    pub fn stop(&mut self) -> Vec<A> {
        self.next_at = None;
        self.follow_ups.drain()
    }

    pub fn is_stopped(&self) -> bool {
        self.next_at.is_none()
    }

    pub fn pending_follow_ups(&self) -> usize {
        self.follow_ups.len()
    }
}
