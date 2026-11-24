//! OLTP fault profiles over the runtime's single simulated network.
//!
//! Worker sessions are classified by their negotiated ALPN and fixed frame
//! header. HTTP and one-shot traffic retain byte-stream ordering.

mod http;
mod peers;

pub use http::HttpConnection;
pub use peers::{PeerDirectory, PeerProtocol};

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use nabedra_rt::sim::{
    World,
    net::{ConnId, Dial, Direction, Envelope, Fate, Kind, Planner},
};
use nabedra_sim::{
    net::{NetPlan, Plan},
    trace::Trace,
};
pub use nabedra_transport::FrameKind;
use nabedra_transport::{StreamFrame, session_alpn};

type Link = (Option<u64>, Option<u64>);
type Profile<T, A> = Box<dyn FnMut(T) -> A + Send>;
type TraceObserver = Arc<dyn Fn(u64, Option<ConnId>, &str) + Send + Sync>;

/// A multiplexed frame's routing metadata. The body never reaches the profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub connection: ConnId,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub direction: Direction,
    pub stream: u64,
    pub kind: FrameKind,
}

/// One flush on a plain connection. Sequence numbers start at zero separately
/// in each direction. A streaming response can be cut after any earlier flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpFlush {
    pub connection: ConnId,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub direction: Direction,
    pub sequence: u64,
}

/// A fault selected for one complete frame or byte flush.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Action {
    #[default]
    Pass,
    Delay(Duration),
    /// Leave the connection open while its sending direction stops making progress.
    Cut,
    /// Reset the session immediately. Pending calls see the transport's normal error.
    Reset,
}

struct State {
    plan: NetPlan,
    nodes: BTreeSet<Option<u64>>,
    held: BTreeSet<Link>,
    frame_profile: Option<Profile<Frame, Action>>,
    http_profile: Option<Profile<HttpFlush, Action>>,
    dial_profile: Option<Profile<Link, Dial>>,
    world: Weak<World>,
    trace: Trace,
    trace_observer: Option<TraceObserver>,
}

/// Installs profiles on the network used by the existing transport backend.
/// The handle and the planner share state scoped to one run.
#[derive(Clone)]
pub struct SimNet {
    world: Arc<World>,
    state: Arc<Mutex<State>>,
}

impl SimNet {
    pub fn install(world: &Arc<World>, seed: u64, nodes: impl IntoIterator<Item = u64>) -> Self {
        let state = Arc::new(Mutex::new(State {
            plan: NetPlan::new(seed),
            nodes: nodes.into_iter().map(Some).chain([None]).collect(),
            held: BTreeSet::new(),
            frame_profile: None,
            http_profile: None,
            dial_profile: None,
            world: Arc::downgrade(world),
            trace: Trace::new(),
            trace_observer: None,
        }));
        world.set_net_planner(Router(state.clone()));
        Self {
            world: world.clone(),
            state,
        }
    }

    pub fn add_node(&self, node: u64) {
        self.configure(|state| {
            state.nodes.insert(Some(node));
        });
    }

    /// Change shared net rules. Held links also affect messages already in flight.
    pub fn update_plan<R>(&self, update: impl FnOnce(&mut NetPlan) -> R) -> R {
        self.configure(|state| update(&mut state.plan))
    }

    fn configure<R>(&self, update: impl FnOnce(&mut State) -> R) -> R {
        let (result, opened, closed) = {
            let mut state = self.state.lock().unwrap();
            let result = update(&mut state);
            let next: BTreeSet<_> = state
                .nodes
                .iter()
                .flat_map(|&from| state.nodes.iter().map(move |&to| (from, to)))
                .filter(|&(from, to)| {
                    from.is_some_and(|node| state.plan.is_paused(node))
                        || to.is_some_and(|node| state.plan.is_paused(node))
                        || matches!((from, to), (Some(a), Some(b)) if state.plan.is_blocked(a, b))
                })
                .collect();
            let opened: Vec<_> = state.held.difference(&next).copied().collect();
            let closed: Vec<_> = next.difference(&state.held).copied().collect();
            for link in &opened {
                state.record(format_args!("link {link:?} open"));
            }
            for link in &closed {
                state.record(format_args!("link {link:?} held"));
            }
            state.held = next;
            (result, opened, closed)
        };
        // World calls the planner while holding its network lock.
        for (from, to) in opened {
            self.world.heal(from, to);
        }
        for (from, to) in closed {
            self.world.black_hole(from, to);
        }
        result
    }

    pub fn set_frame_profile(&self, profile: impl FnMut(Frame) -> Action + Send + 'static) {
        self.state.lock().unwrap().frame_profile = Some(Box::new(profile));
    }

    pub fn set_http_profile(&self, profile: impl FnMut(HttpFlush) -> Action + Send + 'static) {
        self.state.lock().unwrap().http_profile = Some(Box::new(profile));
    }

    pub fn set_dial_profile(
        &self,
        mut profile: impl FnMut(Option<u64>, Option<u64>) -> Dial + Send + 'static,
    ) {
        self.state.lock().unwrap().dial_profile =
            Some(Box::new(move |(from, to)| profile(from, to)));
        self.world.net_changed();
    }

    /// Forward frame metadata to a chronological harness trace. The observer
    /// runs under the planner lock and must not call the network.
    pub fn set_trace_observer(
        &self,
        observer: impl Fn(u64, Option<ConnId>, &str) + Send + Sync + 'static,
    ) {
        self.state.lock().unwrap().trace_observer = Some(Arc::new(observer));
    }

    pub fn trace(&self) -> Trace {
        self.state.lock().unwrap().trace.clone()
    }
}

impl State {
    fn record(&mut self, event: std::fmt::Arguments<'_>) {
        self.record_observed(event, event, None);
    }

    fn record_observed(
        &mut self,
        event: std::fmt::Arguments<'_>,
        observed: std::fmt::Arguments<'_>,
        connection: Option<ConnId>,
    ) {
        if let Some(world) = self.world.upgrade() {
            let ms = world.elapsed().as_millis().try_into().unwrap();
            self.trace.record(ms, event);
            if let Some(observer) = &self.trace_observer {
                observer(ms, connection, &observed.to_string());
            }
        }
    }
}

struct Router(Arc<Mutex<State>>);

impl Planner for Router {
    fn dial(&mut self, from: Option<u64>, to: Option<u64>) -> Dial {
        let mut state = self.0.lock().unwrap();
        let dial = state
            .dial_profile
            .as_mut()
            .map_or(Dial::Connect, |profile| profile((from, to)));
        state.record(format_args!("dial {from:?} -> {to:?} {dial:?}"));
        dial
    }

    fn message(&mut self, envelope: &Envelope<'_>) -> Fate {
        let mut state = self.0.lock().unwrap();
        let link = (envelope.from, envelope.to);
        let base = match state
            .plan
            .plan(link.0.unwrap_or(u64::MAX), link.1.unwrap_or(u64::MAX))
        {
            Plan::Drop if state.held.contains(&link) => Action::Pass,
            Plan::Drop => Action::Cut,
            // A TCP connection absorbs duplicate network packets.
            Plan::Deliver { delay_ms, .. } => Action::Delay(Duration::from_millis(delay_ms)),
        };
        let mut stream = None;
        let message = classify(envelope);
        let selected = match message {
            Message::Frame(frame) => {
                stream = Some(frame.stream);
                state
                    .frame_profile
                    .as_mut()
                    .map_or(Action::Pass, |profile| profile(frame))
            }
            Message::Http(flush) => state
                .http_profile
                .as_mut()
                .map_or(Action::Pass, |profile| profile(flush)),
            Message::OneShot | Message::Other => Action::Pass,
        };
        let action = match selected {
            Action::Pass => base,
            action => action,
        };
        let len = envelope.bytes.len();
        let metadata = format_args!(
            "conn={} seq={} {:?} {:?} {:?}",
            envelope.conn.0, envelope.seq, envelope.direction, message, envelope.alpn,
        );
        let event = format_args!("{metadata} len={len} {action:?}");
        if matches!(message, Message::Http(_)) && envelope.direction == Direction::ToClient {
            // Public stats responses contain host-dependent profiling counters.
            // Observe HTTP response boundaries and fates alongside the parsed
            // client history. Worker frames still include their wire sizes.
            state.record_observed(event, format_args!("{metadata} {action:?}"), None);
        } else {
            let connection = matches!(message, Message::Frame(_)).then_some(envelope.conn);
            state.record_observed(event, event, connection);
        }
        let delay = match action {
            Action::Pass => Duration::ZERO,
            Action::Delay(delay) => delay,
            Action::Cut => return Fate::Drop,
            Action::Reset => return Fate::Reset,
        };
        match stream {
            Some(stream) => Fate::Frame { stream, delay },
            None => Fate::Deliver { delay },
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Message {
    Frame(Frame),
    Http(HttpFlush),
    OneShot,
    Other,
}

fn classify(envelope: &Envelope<'_>) -> Message {
    if envelope.kind != Kind::Data {
        return Message::Other;
    }
    if session_alpn(envelope.alpn) {
        return StreamFrame::from_message(envelope.bytes).map_or(Message::Other, |frame| {
            Message::Frame(Frame {
                connection: envelope.conn,
                from: envelope.from,
                to: envelope.to,
                direction: envelope.direction,
                stream: frame.stream_id(),
                kind: frame.kind(),
            })
        });
    }
    if envelope.alpn == Some(nabedra_transport::ONE_SHOT_ALPN) {
        return Message::OneShot;
    }
    if envelope.alpn.is_none() {
        return Message::Http(HttpFlush {
            connection: envelope.conn,
            from: envelope.from,
            to: envelope.to,
            direction: envelope.direction,
            sequence: envelope.seq,
        });
    }
    Message::Other
}

#[cfg(test)]
mod tests;
