//! Frame and HTTP fault windows. The planner calls these profiles with the network locked,
//! so they only read their own state and draw from their own seeded stream.

use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use nabedra_rt::sim::{World, net::Direction};
use nabedra_sim::{Draw, SplitMix64, rng::stream};

use crate::{
    invariants::elapsed_ms,
    net::{Action as NetAction, Frame, FrameKind, HttpFlush, SimNet},
};

#[derive(Default)]
struct Windows {
    /// Reset the session of a reply frame sent by this worker, per mille.
    reply_reset: Option<(u64, u64)>,
    /// Reset the HTTP connection of a response flushed by this gateway, per mille.
    http_reset: Option<(u64, u64)>,
    /// Delay every frame to or from this worker by a fixed amount.
    delay: Option<(u64, Duration)>,
    /// Delay every frame sent by these workers (the old voters of a replica move).
    slow: Option<(Vec<u64>, Duration)>,
    /// Reset the session that carries the n-th worker frame from now.
    session_drop_after: Option<u64>,
    /// Beyond TCP: stall a direction at a frame, per mille.
    frame_drop: Option<u64>,
    /// Beyond TCP: stall every frame of the first stream seen on a session from this worker.
    black_hole: Option<(u64, Option<u64>)>,
    /// Beyond TCP: independent per-frame delays up to this bound.
    reorder_ms: Option<u64>,
}

struct State {
    rng: SplitMix64,
    windows: Windows,
    resets: u64,
    /// When `session-drop` reset a session.
    session_drops: Vec<u64>,
    /// A node whose multiplexed frames are counted, and how many it exchanged.
    watched: Option<(u64, u64)>,
    /// Weak: the World owns the planner that holds this state, so a strong reference would
    /// keep the World and every mounted image alive after the run.
    world: Weak<World>,
}

/// The World a due fault happens in. A fault that comes due without one would be missed
/// silently, so that fails the run.
fn running(world: &Weak<World>) -> Arc<World> {
    world
        .upgrade()
        .expect("a network fault came due after its World was freed")
}

/// Shared fault windows behind the SimNet frame and HTTP profiles of one run.
#[derive(Clone)]
pub(crate) struct NetFaults(Arc<Mutex<State>>);

impl NetFaults {
    pub(crate) fn install(net: &SimNet, world: &Arc<World>, seed: u64) -> Self {
        let faults = NetFaults(Arc::new(Mutex::new(State {
            rng: stream(seed, "oltp-nemesis-net"),
            windows: Windows::default(),
            resets: 0,
            session_drops: Vec::new(),
            watched: None,
            world: Arc::downgrade(world),
        })));
        let frames = faults.clone();
        net.set_frame_profile(move |frame| frames.frame(frame));
        let http = faults.clone();
        net.set_http_profile(move |flush| http.http(flush));
        faults
    }

    fn update(&self, f: impl FnOnce(&mut Windows)) {
        f(&mut self.0.lock().unwrap().windows);
    }

    pub(crate) fn lose_replies(&self, worker: u64, per_mille: u64) {
        self.update(|w| w.reply_reset = Some((worker, per_mille)));
    }

    pub(crate) fn lose_http_replies(&self, gateway: u64, per_mille: u64) {
        self.update(|w| w.http_reset = Some((gateway, per_mille)));
    }

    pub(crate) fn delay(&self, worker: u64, by: Duration) {
        self.update(|w| w.delay = Some((worker, by)));
    }

    /// Delay frames sent by `workers`, or stop delaying them with `None`.
    pub(crate) fn slow(&self, slow: Option<(Vec<u64>, Duration)>) {
        self.update(|w| w.slow = slow);
    }

    pub(crate) fn drop_session_after(&self, frames: u64) {
        self.update(|w| w.session_drop_after = Some(frames));
    }

    pub(crate) fn frame_drop(&self, per_mille: u64) {
        self.update(|w| w.frame_drop = Some(per_mille));
    }

    pub(crate) fn black_hole(&self, worker: u64) {
        self.update(|w| w.black_hole = Some((worker, None)));
    }

    pub(crate) fn reorder(&self, max_ms: u64) {
        self.update(|w| w.reorder_ms = Some(max_ms));
    }

    /// Ends every window. Faults already applied stay applied.
    pub(crate) fn clear(&self) {
        self.update(|w| *w = Windows::default());
    }

    /// Ends the window that `action` opened.
    pub(crate) fn clear_kind(&self, action: super::Action) {
        use super::Action as A;
        self.update(|w| match action {
            A::LostReply => w.reply_reset = None,
            A::LostHttpReply => w.http_reset = None,
            A::DeadlineEdge => w.delay = None,
            A::SessionDrop => w.session_drop_after = None,
            A::FrameDrop => w.frame_drop = None,
            A::BlackHole => w.black_hole = None,
            A::Reorder => w.reorder_ms = None,
            _ => {}
        });
    }

    /// Whether a fault window is open that could stall a worker's restart.
    pub(crate) fn open(&self) -> bool {
        let state = self.0.lock().unwrap();
        let w = &state.windows;
        w.reply_reset.is_some()
            || w.delay.is_some()
            || w.frame_drop.is_some()
            || w.black_hole.is_some()
            || w.reorder_ms.is_some()
    }

    pub(crate) fn session_drops(&self) -> Vec<u64> {
        self.0.lock().unwrap().session_drops.clone()
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
        self.0.lock().unwrap().resets
    }

    fn frame(&self, frame: Frame) -> NetAction {
        let mut state = self.0.lock().unwrap();
        let State {
            rng,
            windows,
            resets,
            session_drops,
            watched,
            world,
        } = &mut *state;
        if let Some((node, count)) = watched
            && (frame.from == Some(*node) || frame.to == Some(*node))
        {
            *count += 1;
        }
        if let Some(left) = windows.session_drop_after.as_mut() {
            *left = left.saturating_sub(1);
            if *left == 0 {
                windows.session_drop_after = None;
                *resets += 1;
                session_drops.push(elapsed_ms(&running(world)));
                return NetAction::Reset;
            }
        }
        if let Some((worker, per_mille)) = windows.reply_reset
            && frame.kind == FrameKind::Reply
            && frame.from == Some(worker)
            && rng.chance(per_mille)
        {
            *resets += 1;
            return NetAction::Reset;
        }
        if let Some(per_mille) = windows.frame_drop
            && rng.chance(per_mille)
        {
            return NetAction::Cut;
        }
        if let Some((worker, stream)) = windows.black_hole.as_mut()
            && frame.from == Some(*worker)
            && matches!(frame.kind, FrameKind::Request | FrameKind::Reply)
        {
            let target = *stream.get_or_insert(frame.stream);
            if target == frame.stream {
                return NetAction::Cut;
            }
        }
        if let Some(max_ms) = windows.reorder_ms {
            return NetAction::Delay(Duration::from_millis(rng.range(0, max_ms)));
        }
        if let Some((worker, by)) = windows.delay
            && (frame.from == Some(worker) || frame.to == Some(worker))
        {
            return NetAction::Delay(by);
        }
        if let Some((workers, by)) = &windows.slow
            && frame.from.is_some_and(|node| workers.contains(&node))
        {
            return NetAction::Delay(*by);
        }
        NetAction::Pass
    }

    fn http(&self, flush: HttpFlush) -> NetAction {
        let mut state = self.0.lock().unwrap();
        let State {
            rng,
            windows,
            resets,
            ..
        } = &mut *state;
        if let Some((gateway, per_mille)) = windows.http_reset
            && flush.direction == Direction::ToClient
            && flush.from == Some(gateway)
            && rng.chance(per_mille)
        {
            *resets += 1;
            return NetAction::Reset;
        }
        NetAction::Pass
    }
}

#[cfg(test)]
mod tests {
    use nabedra_rt::sim::net::ConnId;

    use super::*;

    fn frame() -> Frame {
        Frame {
            connection: ConnId(1),
            from: Some(1),
            to: Some(2),
            direction: Direction::ToServer,
            stream: 1,
            kind: FrameKind::Request,
        }
    }

    fn state(world: &Arc<World>) -> NetFaults {
        NetFaults(Arc::new(Mutex::new(State {
            rng: stream(1, "oltp-nemesis-net"),
            windows: Windows::default(),
            resets: 0,
            session_drops: Vec::new(),
            watched: None,
            world: Arc::downgrade(world),
        })))
    }

    /// The planner must not keep a finished run's World, and a drop that comes due without
    /// one must not pass silently.
    #[test]
    fn a_due_fault_needs_its_world_and_does_not_keep_it() {
        let (faults, world) = nabedra_sim::runtime::run(1, |world| async move {
            let faults = state(&world);
            faults.drop_session_after(1);
            assert!(matches!(faults.frame(frame()), NetAction::Reset));
            assert_eq!(faults.session_drops().len(), 1);
            faults.drop_session_after(1);
            (faults, Arc::downgrade(&world))
        });
        assert!(world.upgrade().is_none(), "the fault state kept its World");
        let missed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            faults.frame(frame());
        }));
        assert!(missed.is_err(), "a missed fault passed silently");
    }
}
