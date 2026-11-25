//! Frame and HTTP fault windows. The planner calls these profiles with the network locked,
//! so they only read their own state and draw from their own seeded stream.
//!
//! Every window has its own ID, so one that ends never ends another of the same kind, and
//! each records the frames it changed. A window that changed none did not happen.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use nabedra_rt::sim::{
    World,
    net::{ConnId, Direction},
};
use nabedra_sim::{Draw, SplitMix64, rng::stream};

use crate::{
    invariants::elapsed_ms,
    net::{Action as NetAction, Frame, FrameKind, HttpFlush, SimNet},
};

/// What an open window does.
#[derive(Clone, Debug)]
pub(crate) enum Rule {
    /// Reset the session of a reply frame sent by this worker, per mille.
    ReplyReset { worker: u64, per_mille: u64 },
    /// Reset the HTTP connection of a response flushed by this gateway, per mille.
    HttpReset { gateway: u64, per_mille: u64 },
    /// Delay every frame to or from this worker by a fixed amount.
    Delay { worker: u64, by: Duration },
    /// Delay every frame sent by these workers (the old voters of a replica move).
    Slow { workers: Vec<u64>, by: Duration },
    /// Reset the session that carries the n-th worker frame from now, once.
    SessionDrop { left: u64 },
    /// Beyond TCP: stall a direction at a frame, per mille.
    FrameDrop { per_mille: u64 },
    /// Beyond TCP: stall every frame of the first stream seen on a session from this worker.
    BlackHole { worker: u64, stream: Option<u64> },
    /// Beyond TCP: independent per-frame delays up to this bound.
    Reorder { max_ms: u64 },
}

impl Rule {
    /// Whether the window could stall a worker's restart.
    fn stalls(&self) -> bool {
        !matches!(
            self,
            Rule::HttpReset { .. } | Rule::SessionDrop { .. } | Rule::Slow { .. }
        )
    }
}

/// A connection that a window reset, and what it carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Reset {
    pub at: u64,
    pub from: Option<u64>,
    pub to: Option<u64>,
    /// Requests on it that had no delivered reply yet: who sent them, and when.
    pub unanswered: Vec<(u64, u64)>,
}

/// What a window did.
#[derive(Clone, Debug, Default)]
pub(crate) struct Effect {
    pub resets: Vec<Reset>,
    /// Frames it delayed or stalled, and when the first one was sent.
    pub frames: u64,
    pub first: Option<u64>,
}

pub(crate) type WindowId = u64;

struct State {
    rng: SplitMix64,
    open: BTreeMap<WindowId, Rule>,
    effects: BTreeMap<WindowId, Effect>,
    next: WindowId,
    /// The run healed: no window opens any more.
    healed: bool,
    /// Requests without a reply, by session and stream: who sent it, and when.
    unanswered: BTreeMap<(ConnId, u64), (u64, u64)>,
    /// A node whose multiplexed frames are counted, and how many it exchanged.
    watched: Option<(u64, u64)>,
    /// Weak: the World owns the planner that holds this state, so a strong reference would
    /// keep the World and every mounted image alive after the run.
    world: Weak<World>,
}

/// The virtual time of a fault. One that comes due without its World would be missed
/// silently, so that fails the run.
fn now(world: &Weak<World>) -> u64 {
    let world = world
        .upgrade()
        .expect("a network fault came due after its World was freed");
    elapsed_ms(&world)
}

/// Shared fault windows behind the SimNet frame and HTTP profiles of one run.
#[derive(Clone)]
pub(crate) struct NetFaults(Arc<Mutex<State>>);

impl NetFaults {
    fn new(world: &Arc<World>, seed: u64) -> Self {
        NetFaults(Arc::new(Mutex::new(State {
            rng: stream(seed, "oltp-nemesis-net"),
            open: BTreeMap::new(),
            effects: BTreeMap::new(),
            next: 0,
            healed: false,
            unanswered: BTreeMap::new(),
            watched: None,
            world: Arc::downgrade(world),
        })))
    }

    #[cfg(test)]
    pub(crate) fn for_test(world: &Arc<World>) -> Self {
        Self::new(world, 1)
    }

    pub(crate) fn install(net: &SimNet, world: &Arc<World>, seed: u64) -> Self {
        let faults = Self::new(world, seed);
        let frames = faults.clone();
        net.set_frame_profile(move |frame| frames.frame(frame));
        let http = faults.clone();
        net.set_http_profile(move |flush| http.http(flush));
        faults
    }

    /// Open a window.
    pub(crate) fn start(&self, rule: Rule) -> WindowId {
        let mut state = self.0.lock().unwrap();
        let id = state.next;
        state.next += 1;
        if !state.healed {
            state.open.insert(id, rule);
        }
        state.effects.insert(id, Effect::default());
        id
    }

    /// End one window. Faults already applied stay applied.
    pub(crate) fn end(&self, id: WindowId) {
        self.0.lock().unwrap().open.remove(&id);
    }

    /// What the window has done so far.
    pub(crate) fn effect(&self, id: WindowId) -> Effect {
        self.0.lock().unwrap().effects[&id].clone()
    }

    /// Ends every window. Faults already applied stay applied.
    pub(crate) fn clear(&self) {
        self.0.lock().unwrap().open.clear();
    }

    /// Ends every window for the rest of the run.
    pub(crate) fn heal(&self) {
        let mut state = self.0.lock().unwrap();
        state.open.clear();
        state.healed = true;
    }

    /// Whether a fault window is open that could stall a worker's restart.
    pub(crate) fn open(&self) -> bool {
        self.0.lock().unwrap().open.values().any(Rule::stalls)
    }

    /// Count the multiplexed frames `node` exchanges from now on.
    pub(crate) fn watch(&self, node: u64) {
        self.0.lock().unwrap().watched = Some((node, 0));
    }

    /// Stop counting and return the count.
    pub(crate) fn watched(&self) -> u64 {
        self.0
            .lock()
            .unwrap()
            .watched
            .take()
            .map_or(0, |(_, count)| count)
    }

    /// Session resets this run's windows caused.
    pub(crate) fn resets(&self) -> u64 {
        let state = self.0.lock().unwrap();
        state.effects.values().map(|e| e.resets.len() as u64).sum()
    }

    fn frame(&self, frame: Frame) -> NetAction {
        let mut state = self.0.lock().unwrap();
        if let Some((node, count)) = state.watched.as_mut()
            && (frame.from == Some(*node) || frame.to == Some(*node))
        {
            *count += 1;
        }
        // Nothing can be due without an open window.
        let at = if state.open.is_empty() {
            state.world.upgrade().map_or(0, |world| elapsed_ms(&world))
        } else {
            now(&state.world)
        };
        let key = (frame.connection, frame.stream);
        if frame.kind == FrameKind::Request
            && let Some(from) = frame.from
        {
            state.unanswered.insert(key, (from, at));
        }
        let action = state.frame(&frame, at);
        // Only now: a reply that a reset lost leaves its request unanswered in the record.
        match (action, frame.kind) {
            (NetAction::Reset, _) => {
                let lost = frame.connection;
                state.unanswered.retain(|(conn, _), _| *conn != lost);
            }
            (_, FrameKind::Reply | FrameKind::Cancel) => {
                state.unanswered.remove(&key);
            }
            _ => {}
        }
        action
    }

    fn http(&self, flush: HttpFlush) -> NetAction {
        let mut state = self.0.lock().unwrap();
        let State {
            rng,
            open,
            effects,
            world,
            ..
        } = &mut *state;
        if flush.direction != Direction::ToClient {
            return NetAction::Pass;
        }
        for (id, rule) in open.iter() {
            if let Rule::HttpReset { gateway, per_mille } = rule
                && flush.from == Some(*gateway)
                && rng.chance(*per_mille)
            {
                effects.entry(*id).or_default().resets.push(Reset {
                    at: now(world),
                    from: flush.from,
                    to: flush.to,
                    unanswered: Vec::new(),
                });
                return NetAction::Reset;
            }
        }
        NetAction::Pass
    }
}

impl State {
    /// The action of the open windows on one frame. A reset comes first, then a stall, then
    /// the longest delay.
    fn frame(&mut self, frame: &Frame, at: u64) -> NetAction {
        let mut dropped = None;
        for (id, rule) in &mut self.open {
            if let Rule::SessionDrop { left } = rule {
                *left = left.saturating_sub(1);
                if *left == 0 {
                    dropped = dropped.or(Some(*id));
                }
            }
        }
        if let Some(id) = dropped {
            self.open.remove(&id);
        }
        let rng = &mut self.rng;
        let lost = dropped.or_else(|| {
            let mut open = self.open.iter();
            let lost = open.find(|(_, rule)| {
                matches!(rule, Rule::ReplyReset { worker, per_mille }
                    if frame.kind == FrameKind::Reply
                        && frame.from == Some(*worker)
                        && rng.chance(*per_mille))
            });
            lost.map(|(id, _)| *id)
        });
        if let Some(id) = lost {
            let carried = self
                .unanswered
                .iter()
                .filter(|((conn, _), _)| *conn == frame.connection);
            let reset = Reset {
                at,
                from: frame.from,
                to: frame.to,
                unanswered: carried.map(|(_, request)| *request).collect(),
            };
            self.effects.entry(id).or_default().resets.push(reset);
            return NetAction::Reset;
        }
        let mut action = NetAction::Pass;
        for (id, rule) in &mut self.open {
            let delay = match rule {
                Rule::FrameDrop { per_mille } if self.rng.chance(*per_mille) => None,
                Rule::BlackHole { worker, stream }
                    if frame.from == Some(*worker)
                        && matches!(frame.kind, FrameKind::Request | FrameKind::Reply) =>
                {
                    if *stream.get_or_insert(frame.stream) != frame.stream {
                        continue;
                    }
                    None
                }
                Rule::Reorder { max_ms } => Some(Duration::from_millis(self.rng.range(0, *max_ms))),
                Rule::Delay { worker, by }
                    if frame.from == Some(*worker) || frame.to == Some(*worker) =>
                {
                    Some(*by)
                }
                Rule::Slow { workers, by }
                    if frame.from.is_some_and(|node| workers.contains(&node)) =>
                {
                    Some(*by)
                }
                _ => continue,
            };
            let effect = self.effects.entry(*id).or_default();
            effect.frames += 1;
            effect.first.get_or_insert(at);
            action = match (action, delay) {
                (NetAction::Cut, _) | (_, None) => NetAction::Cut,
                (NetAction::Delay(longest), Some(by)) => NetAction::Delay(longest.max(by)),
                (_, Some(by)) => NetAction::Delay(by),
            };
        }
        action
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(connection: u64, kind: FrameKind, from: u64, to: u64) -> Frame {
        Frame {
            connection: ConnId(connection),
            from: Some(from),
            to: Some(to),
            direction: Direction::ToServer,
            stream: 1,
            kind,
        }
    }

    fn run(test: impl FnOnce(NetFaults) + Send + 'static) {
        nabedra_sim::runtime::run(1, |world| async move {
            test(NetFaults::new(&world, 1));
        });
    }

    /// The planner must not keep a finished run's World, and a drop that comes due without
    /// one must not pass silently.
    #[test]
    fn a_due_fault_needs_its_world_and_does_not_keep_it() {
        let (faults, world) = nabedra_sim::runtime::run(1, |world| async move {
            let faults = NetFaults::new(&world, 1);
            let drop = faults.start(Rule::SessionDrop { left: 1 });
            let request = frame(1, FrameKind::Request, 1, 2);
            assert!(matches!(faults.frame(request), NetAction::Reset));
            assert_eq!(faults.effect(drop).resets.len(), 1);
            faults.start(Rule::SessionDrop { left: 1 });
            (faults, Arc::downgrade(&world))
        });
        assert!(world.upgrade().is_none(), "the fault state kept its World");
        let missed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            faults.frame(frame(1, FrameKind::Request, 1, 2));
        }));
        assert!(missed.is_err(), "a missed fault passed silently");
    }

    /// Review 115, M8: two windows of one kind shared a slot, so the end of the first
    /// cleared the second.
    #[test]
    fn a_window_that_ends_leaves_another_of_its_kind_open() {
        run(|faults| {
            let by = Duration::from_millis;
            let first = faults.start(Rule::Delay {
                worker: 1,
                by: by(5),
            });
            let second = faults.start(Rule::Delay {
                worker: 1,
                by: by(9),
            });
            let sent = || faults.frame(frame(1, FrameKind::Request, 1, 2));
            assert_eq!(sent(), NetAction::Delay(by(9)));
            faults.end(second);
            assert_eq!(sent(), NetAction::Delay(by(5)));
            faults.end(first);
            assert_eq!(sent(), NetAction::Pass);
            let frames = [first, second].map(|id| faults.effect(id).frames);
            assert_eq!(frames, [2, 1]);
        });
    }

    /// A window whose frames never came did not happen, and a reset names the requests that
    /// its connection carried.
    #[test]
    fn a_window_records_what_it_changed() {
        run(|faults| {
            let lost = faults.start(Rule::ReplyReset {
                worker: 1,
                per_mille: 1_000,
            });
            for (connection, kind, from, to) in [
                (1, FrameKind::Request, 101, 1),
                (2, FrameKind::Request, 102, 1),
                (3, FrameKind::Reply, 2, 101),
            ] {
                let action = faults.frame(frame(connection, kind, from, to));
                assert_eq!(action, NetAction::Pass);
            }
            assert!(faults.effect(lost).resets.is_empty());
            let reply = frame(1, FrameKind::Reply, 1, 101);
            assert_eq!(faults.frame(reply), NetAction::Reset);
            let resets = faults.effect(lost).resets;
            assert_eq!(resets.len(), 1);
            assert_eq!((resets[0].from, resets[0].to), (Some(1), Some(101)));
            assert_eq!(resets[0].unanswered.len(), 1, "{resets:?}");
            assert_eq!(resets[0].unanswered[0].0, 101);
            assert_eq!(faults.resets(), 1);
        });
    }
}
