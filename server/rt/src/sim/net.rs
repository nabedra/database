//! The simulated network backend (feature `sim`). Never in a product build.
//!
//! [`SimListener`], [`SimConnector`] and [`SimStream`] implement the [`crate::net`]
//! traits in memory, on the world's paused current-thread runtime. Product code reaches
//! them through [`crate::net::AnyListener`] and [`crate::net::AnyConnector`] whenever a
//! world is installed.
//!
//! # Addresses
//!
//! Node `n` has the address [`node_ip`]`(n)` (`10.0.0.1 + n`); code outside any node
//! (a test driver or an HTTP client) has [`EXTERNAL_IP`]. Binding or dialing a loopback
//! or unspecified address means the caller's own address, so a listener bound to
//! `127.0.0.1:0` reports `10.0.0.x:port` and other nodes can dial it. Port 0 takes the
//! next port of a world-wide counter, so ports are deterministic. Host names resolve
//! through [`World::register_host`]; `localhost` is loopback.
//!
//! # Connections and messages
//!
//! A dial to a bound address connects at once (the listener's backlog is unbounded) and
//! returns a [`Pending`]; the listener accepts the other [`Pending`]. A [`Pending`] then
//! runs the simulated handshake, [`Pending::client_handshake`] and
//! [`Pending::server_handshake`], or becomes a plain stream with [`Pending::plain`].
//!
//! The handshake negotiates ALPN with rustls' rule: the server picks the first protocol
//! in its own list that the client offers, there is no protocol when either list is
//! empty, and the handshake fails when both are non-empty and share none. Each side
//! receives the other's certificate digest ([`World::set_certificate`], or the
//! override from [`World::override_certificate`], which impersonates another identity).
//! Both sides must have a certificate, as with mutual TLS.
//!
//! A [`SimStream`] buffers written bytes and sends them as one message on each `flush`
//! (and on `shutdown` and drop), so a transport that flushes once per frame sends one
//! message per frame. Each message is offered to the world's [`Planner`] when it is
//! sent, with its connection id, direction and sequence number, which may deliver it
//! (after a delay), drop it or reset the connection. A dropped message stalls its
//! direction: TCP never delivers what follows a lost segment, so every later message on
//! that direction vanishes too, until the connection is reset. Messages keep their order.
//!
//! # Faults
//!
//! - [`World::crash`] resets every connection of the node and closes its listeners
//!   before it aborts the node's tasks, so peers read `ConnectionReset` (queued data is
//!   discarded, as the kernel does on a reset) and new dials are refused until the
//!   restarted node binds again.
//! - [`World::reset_connection`] and [`World::close_connection`] reset or gracefully
//!   close one connection.
//! - [`World::black_hole`] holds every message from one node to another, including
//!   handshake messages and messages already in flight, until [`World::heal`]; dials
//!   between the two nodes hang meanwhile. This is a link outage: TCP retransmits, so
//!   held messages arrive in order after the heal.
//! - [`Planner`] decides per message and per dial (phase 10's `NetPlan` plugs in here).
//!
//! Buffers are unbounded: a write never waits for the reader.

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};

use super::{World, current_node, world};
pub use crate::net::{Handshake, Target};

/// The address of code outside any node (TEST-NET-1).
pub const EXTERNAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

/// The first port handed out for port 0 and for dialing sides.
pub const FIRST_EPHEMERAL_PORT: u16 = 49_152;

/// The address of `node`: `10.0.0.1 + node`.
///
/// # Panics
///
/// If `node` does not fit in the `10.0.0.0/8` block.
pub fn node_ip(node: u64) -> IpAddr {
    let offset = u32::try_from(node)
        .ok()
        .filter(|node| *node < 0x00FF_FFFE)
        .unwrap_or_else(|| panic!("node {node} has no simulated address"));
    IpAddr::V4(Ipv4Addr::from(0x0A00_0001 + offset))
}

fn host_ip(node: Option<u64>) -> IpAddr {
    node.map_or(EXTERNAL_IP, node_ip)
}

/// `addr` as seen from `node`: loopback and unspecified mean the node's own address.
fn own_view(addr: SocketAddr, node: Option<u64>) -> SocketAddr {
    if addr.ip().is_loopback() || addr.ip().is_unspecified() {
        SocketAddr::new(host_ip(node), addr.port())
    } else {
        addr
    }
}

/// A connection's id, unique in its world, in dial order from 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId(pub u64);

/// Which way a message travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Direction {
    /// From the dialing side to the accepting side.
    ToServer,
    /// From the accepting side to the dialing side.
    ToClient,
}

/// A connection, as [`World::connections`] lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnInfo {
    pub id: ConnId,
    /// The dialing node, `None` outside any node.
    pub client: Option<u64>,
    /// The listening node, `None` outside any node.
    pub server: Option<u64>,
    pub client_addr: SocketAddr,
    pub server_addr: SocketAddr,
    pub reset: bool,
}

/// What a message carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A handshake message; `bytes` is empty.
    Handshake,
    /// The bytes between two flushes.
    Data,
}

/// One message as the [`Planner`] sees it when it is sent.
#[derive(Debug)]
pub struct Envelope<'a> {
    pub conn: ConnId,
    pub direction: Direction,
    /// The sending node.
    pub from: Option<u64>,
    /// The receiving node.
    pub to: Option<u64>,
    /// The message's position in its direction, from 0, counting handshake messages.
    pub seq: u64,
    pub kind: Kind,
    /// Negotiated ALPN for data messages, if the connection used a handshake.
    pub alpn: Option<&'a [u8]>,
    pub bytes: &'a [u8],
}

/// What happens to one message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    /// Delivered after `delay` of virtual time, and never before an earlier message.
    Deliver { delay: Duration },
    /// Deliver a complete transport frame, preserving order within this stream.
    /// The planner must use this only at established, whole-frame boundaries.
    Frame { stream: u64, delay: Duration },
    /// Lost; its direction stalls until the connection is reset.
    Drop,
    /// The connection is reset instead; the sender's flush fails.
    Reset,
}

impl Fate {
    pub const NOW: Fate = Fate::Deliver {
        delay: Duration::ZERO,
    };
}

/// What happens to one dial.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dial {
    Connect,
    /// Refused at once, like a closed port.
    Refuse,
    /// No answer: the dial waits and asks again after [`World::net_changed`].
    Hang,
}

/// The network rules of a run. The planner is called with the network locked, so it
/// must not call back into the simulated network.
pub trait Planner: Send + 'static {
    fn message(&mut self, envelope: &Envelope<'_>) -> Fate {
        let _ = envelope;
        Fate::NOW
    }

    fn dial(&mut self, from: Option<u64>, to: Option<u64>) -> Dial {
        let _ = (from, to);
        Dial::Connect
    }
}

#[derive(Debug)]
enum Body {
    Data(Vec<u8>),
    ClientHello {
        offer: Vec<Vec<u8>>,
        certificate: Option<[u8; 32]>,
    },
    ServerHello {
        alpn: Option<Vec<u8>>,
        certificate: [u8; 32],
    },
    Alert(&'static str),
}

#[derive(Debug)]
struct Message {
    ready_at: Instant,
    stream: Option<u64>,
    body: Body,
    /// Bytes of a `Data` body already read.
    read: usize,
}

#[derive(Debug, Default)]
struct Pipe {
    queue: VecDeque<Message>,
    fin: Fin,
    /// A message was dropped: later ones vanish.
    stalled: bool,
    sent: u64,
    reader: Option<Waker>,
}

/// FIN travels in order with the data and can be held by a black hole.
#[derive(Debug, Default)]
enum Fin {
    #[default]
    Open,
    Sent(Instant),
    Received,
}

impl Pipe {
    fn push(&mut self, body: Body, stream: Option<u64>, delay: Duration) {
        let deadline = Instant::now() + delay;
        let (index, ready_at) = match stream {
            None => {
                let ready_at = self
                    .queue
                    .back()
                    .map_or(deadline, |last| deadline.max(last.ready_at));
                (self.queue.len(), ready_at)
            }
            Some(stream) => {
                // Unframed messages, a partial read, and the same stream are
                // barriers. The queue is sorted by delivery time, FIFO on ties.
                let ready_at = self
                    .queue
                    .iter()
                    .rfind(|message| {
                        message.read != 0
                            || message.stream.is_none()
                            || message.stream == Some(stream)
                    })
                    .map_or(deadline, |last| deadline.max(last.ready_at));
                let index = self
                    .queue
                    .partition_point(|message| message.ready_at <= ready_at);
                (index, ready_at)
            }
        };
        self.queue.insert(
            index,
            Message {
                ready_at,
                stream,
                body,
                read: 0,
            },
        );
        if let Some(waker) = self.reader.take() {
            waker.wake();
        }
    }

    fn finish(&mut self) {
        if matches!(self.fin, Fin::Open) {
            self.fin = Fin::Sent(Instant::now());
        }
        if let Some(waker) = self.reader.take() {
            waker.wake();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Client,
    Server,
}

impl Side {
    /// The index of the pipe this side sends on.
    fn tx(self) -> usize {
        match self {
            Side::Client => 0,
            Side::Server => 1,
        }
    }

    fn rx(self) -> usize {
        1 - self.tx()
    }

    fn direction(self) -> Direction {
        match self {
            Side::Client => Direction::ToServer,
            Side::Server => Direction::ToClient,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct End {
    node: Option<u64>,
    addr: SocketAddr,
}

/// The addresses that live connections hold, with how many ends hold each, so that a free
/// port is found without looking at any connection.
#[derive(Clone, Debug, Default)]
struct Ports(Arc<Mutex<Held>>);

#[derive(Debug, Default)]
struct Held {
    ends: BTreeMap<SocketAddr, usize>,
    /// Addresses looked up, for the tests.
    #[cfg(test)]
    lookups: u64,
}

impl Ports {
    fn lock(&self) -> MutexGuard<'_, Held> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn hold(&self, addr: SocketAddr) -> Hold {
        *self.lock().ends.entry(addr).or_default() += 1;
        Hold {
            ports: self.clone(),
            addr,
        }
    }

    fn is_held(&self, addr: SocketAddr) -> bool {
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut held = self.lock();
        #[cfg(test)]
        {
            held.lookups += 1;
        }
        held.ends.contains_key(&addr)
    }
}

/// One end's claim on its address, until it drops.
#[derive(Debug)]
struct Hold {
    ports: Ports,
    addr: SocketAddr,
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut held = self.ports.lock();
        if let Some(count) = held.ends.get_mut(&self.addr) {
            *count -= 1;
            if *count == 0 {
                held.ends.remove(&self.addr);
            }
        }
    }
}

#[derive(Debug)]
struct Connection {
    ends: [End; 2],
    /// What each end holds of its address. A reset or cut connection holds nothing, and a
    /// client that closed before it was accepted holds nothing either: the peer's kernel
    /// acknowledges the FIN, even for a stopped process, and the port is used again.
    holds: [Option<Hold>; 2],
    /// Whether the listener has accepted it.
    accepted: bool,
    /// `pipes[0]` carries client to server, `pipes[1]` server to client.
    pipes: [Pipe; 2],
    /// Whether each side's handle (pending or stream) is alive.
    open: [bool; 2],
    reset: bool,
    power_cut: bool,
    alpn: Option<Vec<u8>>,
}

impl Connection {
    fn end(&self, side: Side) -> End {
        self.ends[side.tx()]
    }

    fn black_hole(&mut self) {
        self.power_cut = true;
        self.holds = [None, None];
        for pipe in &mut self.pipes {
            pipe.queue.clear();
            pipe.fin = Fin::Open;
            pipe.stalled = true;
            if let Some(waker) = pipe.reader.take() {
                waker.wake();
            }
        }
    }

    fn reset(&mut self) {
        self.reset = true;
        self.holds = [None, None];
        for pipe in &mut self.pipes {
            pipe.queue.clear();
            if let Some(waker) = pipe.reader.take() {
                waker.wake();
            }
        }
    }

    fn info(&self, id: u64) -> ConnInfo {
        ConnInfo {
            id: ConnId(id),
            client: self.ends[0].node,
            server: self.ends[1].node,
            client_addr: self.ends[0].addr,
            server_addr: self.ends[1].addr,
            reset: self.reset,
        }
    }
}

/// Each pending future owns its registration. Cancelling it releases its waker.
#[derive(Debug, Default)]
struct Waiters(Vec<Weak<Waker>>);

impl Waiters {
    fn register(&mut self, waiter: &mut Option<Arc<Waker>>, waker: &Waker) {
        if waiter
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            return;
        }
        let registered = Arc::new(waker.clone());
        *waiter = Some(registered.clone());
        self.0.retain(|entry| entry.strong_count() > 0);
        self.0.push(Arc::downgrade(&registered));
    }

    fn wake_all(&mut self) {
        self.0.retain(|entry| {
            let Some(waker) = entry.upgrade() else {
                return false;
            };
            waker.wake_by_ref();
            true
        });
    }
}

#[derive(Debug)]
struct Slot {
    id: u64,
    node: Option<u64>,
    backlog: VecDeque<u64>,
    acceptors: Waiters,
    dialers: Waiters,
}

#[derive(Default)]
struct State {
    certificates: BTreeMap<u64, [u8; 32]>,
    overrides: BTreeMap<u64, [u8; 32]>,
    hosts: BTreeMap<String, u64>,
    listeners: BTreeMap<SocketAddr, Slot>,
    conns: BTreeMap<u64, Connection>,
    /// Held links `(from, to)` and when each hole opened.
    holes: BTreeMap<(Option<u64>, Option<u64>), Instant>,
    planner: Option<Box<dyn Planner>>,
    next_conn: u64,
    next_listener: u64,
    next_port: u16,
    ports: Ports,
}

impl State {
    fn certificate(&self, node: Option<u64>) -> Option<[u8; 32]> {
        let node = node?;
        self.overrides
            .get(&node)
            .or_else(|| self.certificates.get(&node))
            .copied()
    }

    fn port_free(&self, ip: IpAddr, port: u16) -> bool {
        let addr = SocketAddr::new(ip, port);
        !self.listeners.contains_key(&addr) && !self.ports.is_held(addr)
    }

    /// The next free port on `ip`, from the world-wide counter.
    fn ephemeral_port(&mut self, ip: IpAddr) -> io::Result<u16> {
        for _ in 0..=(u16::MAX - FIRST_EPHEMERAL_PORT) {
            let port = self.next_port.max(FIRST_EPHEMERAL_PORT);
            self.next_port = port.checked_add(1).unwrap_or(FIRST_EPHEMERAL_PORT);
            if self.port_free(ip, port) {
                return Ok(port);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "no free simulated port",
        ))
    }

    /// Wakes every reader, acceptor and dialer, after a change they may be waiting for.
    fn wake_all(&mut self) {
        for conn in self.conns.values_mut() {
            for pipe in &mut conn.pipes {
                if let Some(waker) = pipe.reader.take() {
                    waker.wake();
                }
            }
        }
        for slot in self.listeners.values_mut() {
            slot.acceptors.wake_all();
            slot.dialers.wake_all();
        }
    }

    fn reset(&mut self, conn: u64) {
        if let Some(conn) = self.conns.get_mut(&conn) {
            conn.reset();
        }
    }

    /// Removes the listener at `addr` if it is still `id`, and resets its backlog.
    fn close_listener(&mut self, addr: SocketAddr, id: Option<u64>) {
        if id.is_some_and(|id| self.listeners.get(&addr).is_none_or(|slot| slot.id != id)) {
            return;
        }
        self.disconnect_listener(addr, false);
    }

    fn disconnect_listener(&mut self, addr: SocketAddr, black_hole: bool) {
        let Some(mut slot) = self.listeners.remove(&addr) else {
            return;
        };
        for conn in slot.backlog.drain(..) {
            if let Some(conn) = self.conns.get_mut(&conn) {
                conn.open[Side::Server.tx()] = false;
                if black_hole {
                    conn.black_hole();
                } else {
                    conn.reset();
                }
            }
        }
        slot.acceptors.wake_all();
        slot.dialers.wake_all();
        self.collect();
    }

    /// Forgets connections whose two handles are gone.
    fn collect(&mut self) {
        self.conns
            .retain(|_, conn| conn.open.iter().any(|open| *open));
    }
}

/// The network of one world.
struct Net {
    state: Mutex<State>,
}

impl Net {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

type Registry = Vec<(Weak<World>, Arc<Net>)>;

/// Each world's network, kept beside the world so the world needs no field for it.
static NETS: Mutex<Registry> = Mutex::new(Vec::new());

fn registry() -> MutexGuard<'static, Registry> {
    NETS.lock().unwrap_or_else(PoisonError::into_inner)
}

fn find(registry: &mut Registry, world: &World) -> Option<Arc<Net>> {
    registry.retain(|(weak, _)| weak.strong_count() > 0);
    registry
        .iter()
        .find(|(weak, _)| std::ptr::eq(weak.as_ptr(), world))
        .map(|(_, net)| net.clone())
}

fn net_of(world: &Arc<World>) -> Arc<Net> {
    let mut registry = registry();
    if let Some(net) = find(&mut registry, world) {
        return net;
    }
    let net = Arc::new(Net {
        state: Mutex::new(State {
            next_conn: 1,
            next_listener: 1,
            next_port: FIRST_EPHEMERAL_PORT,
            ..State::default()
        }),
    });
    registry.push((Arc::downgrade(world), net.clone()));
    net
}

fn installed() -> io::Result<Arc<Net>> {
    let world = world().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "the simulated network needs an installed world",
        )
    })?;
    Ok(net_of(&world))
}

/// The crash rule: resets the node's connections and closes its listeners. Called by
/// [`World::crash`] before it aborts the node's tasks.
pub(crate) fn crash(world: &World, node: u64, black_hole: bool) {
    let Some(net) = find(&mut registry(), world) else {
        return;
    };
    let mut state = net.lock();
    let addrs: Vec<SocketAddr> = state
        .listeners
        .iter()
        .filter(|(_, slot)| slot.node == Some(node))
        .map(|(addr, _)| *addr)
        .collect();
    for addr in addrs {
        state.disconnect_listener(addr, black_hole);
    }
    for conn in state.conns.values_mut() {
        if conn.ends.iter().any(|end| end.node == Some(node)) {
            if black_hole {
                conn.black_hole();
            } else {
                conn.reset();
            }
        }
    }
    state.wake_all();
}

impl World {
    fn with_net<T>(self: &Arc<Self>, f: impl FnOnce(&mut State) -> T) -> T {
        let net = net_of(self);
        let mut state = net.lock();
        f(&mut state)
    }

    /// Records `node`'s certificate digest. Tests use the digest of the node's real test
    /// certificate, so enrollments are the same in both modes.
    pub fn set_certificate(self: &Arc<Self>, node: u64, sha256: [u8; 32]) {
        self.with_net(|state| state.certificates.insert(node, sha256));
    }

    /// Makes `node` present `sha256` instead of its own certificate (impersonation), or
    /// its own again with `None`. Applies to handshakes that start afterwards.
    pub fn override_certificate(self: &Arc<Self>, node: u64, sha256: Option<[u8; 32]>) {
        self.with_net(|state| match sha256 {
            Some(sha256) => state.overrides.insert(node, sha256),
            None => state.overrides.remove(&node),
        });
    }

    /// Resolves `name` to `node`'s address.
    pub fn register_host(self: &Arc<Self>, name: &str, node: u64) {
        self.with_net(|state| state.hosts.insert(name.to_ascii_lowercase(), node));
    }

    /// Binds a listener for `node`, as `SimListener::bind` does inside the node.
    pub fn listen(self: &Arc<Self>, node: u64, addr: SocketAddr) -> io::Result<SimListener> {
        bind(net_of(self), Some(node), addr)
    }

    /// The addresses `node` listens on.
    pub fn listening(self: &Arc<Self>, node: u64) -> Vec<SocketAddr> {
        self.with_net(|state| {
            state
                .listeners
                .iter()
                .filter(|(_, slot)| slot.node == Some(node))
                .map(|(addr, _)| *addr)
                .collect()
        })
    }

    /// Open connections, in dial order.
    pub fn connections(self: &Arc<Self>) -> Vec<ConnInfo> {
        self.with_net(|state| {
            state
                .conns
                .iter()
                .map(|(id, conn)| conn.info(*id))
                .collect()
        })
    }

    /// Resets `conn`: both sides read and write `ConnectionReset`; unread data is lost.
    pub fn reset_connection(self: &Arc<Self>, conn: ConnId) {
        self.with_net(|state| state.reset(conn.0));
    }

    /// Closes `conn` gracefully in both directions: queued messages arrive, then EOF.
    pub fn close_connection(self: &Arc<Self>, conn: ConnId) {
        self.with_net(|state| {
            if let Some(conn) = state.conns.get_mut(&conn.0) {
                for pipe in &mut conn.pipes {
                    pipe.finish();
                }
            }
        });
    }

    /// Holds every message from `from` to `to` until [`World::heal`], and hangs dials
    /// between them. `None` is code outside any node.
    pub fn black_hole(self: &Arc<Self>, from: Option<u64>, to: Option<u64>) {
        let now = Instant::now();
        self.with_net(|state| {
            state.holes.entry((from, to)).or_insert(now);
            state.wake_all();
        });
    }

    /// Releases the link from `from` to `to`; held messages arrive in order.
    pub fn heal(self: &Arc<Self>, from: Option<u64>, to: Option<u64>) {
        self.with_net(|state| {
            state.holes.remove(&(from, to));
            state.wake_all();
        });
    }

    pub fn heal_all(self: &Arc<Self>) {
        self.with_net(|state| {
            state.holes.clear();
            state.wake_all();
        });
    }

    /// Installs the planner that decides each message's and each dial's fate.
    pub fn set_net_planner(self: &Arc<Self>, planner: impl Planner) {
        self.with_net(|state| {
            state.planner = Some(Box::new(planner));
            state.wake_all();
        });
    }

    pub fn clear_net_planner(self: &Arc<Self>) {
        self.with_net(|state| {
            state.planner = None;
            state.wake_all();
        });
    }

    /// Tells waiting dials and readers that the planner's rules changed.
    pub fn net_changed(self: &Arc<Self>) {
        self.with_net(State::wake_all);
    }
}

/// Resolves `host` in the installed world: IP literals as given, `localhost` as
/// loopback, and the names from [`World::register_host`].
pub fn resolve(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let name = host.to_ascii_lowercase();
    if name == "localhost" {
        return Ok(vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)]);
    }
    let net = installed()?;
    let node = net.lock().hosts.get(&name).copied();
    match node {
        Some(node) => Ok(vec![SocketAddr::new(node_ip(node), port)]),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("simulated host {host} is not registered"),
        )),
    }
}

/// Dials `target` from the current node (or from outside any node).
pub async fn dial(target: Target<'_>) -> io::Result<Pending> {
    let net = installed()?;
    let from = current_node();
    let addrs = match target {
        Target::Addr(addr) => vec![addr],
        Target::Addrs(addrs) => addrs.to_vec(),
        Target::HostPort(host, port) => resolve(host, port)?,
    };
    let mut last = io::Error::new(
        io::ErrorKind::InvalidInput,
        "could not resolve to any address",
    );
    for addr in addrs {
        match dial_one(&net, from, own_view(addr, from)).await {
            Ok(pending) => return Ok(pending),
            Err(error) => last = error,
        }
    }
    Err(last)
}

async fn dial_one(net: &Arc<Net>, from: Option<u64>, addr: SocketAddr) -> io::Result<Pending> {
    let mut waiter = None;
    let mut waiting_listener = None;
    poll_fn(|cx| {
        let mut state = net.lock();
        let state = &mut *state;
        let Some(slot) = state
            .listeners
            .get(&addr)
            .filter(|slot| waiting_listener.is_none_or(|id| id == slot.id))
        else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("nothing listens on simulated {addr}"),
            )));
        };
        let to = slot.node;
        let verdict =
            if !state.holes.contains_key(&(from, to)) && !state.holes.contains_key(&(to, from)) {
                state
                    .planner
                    .as_mut()
                    .map_or(Dial::Connect, |planner| planner.dial(from, to))
            } else {
                Dial::Hang
            };
        match verdict {
            Dial::Refuse => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("simulated dial to {addr} refused"),
            ))),
            Dial::Hang => {
                waiting_listener = Some(slot.id);
                state
                    .listeners
                    .get_mut(&addr)
                    .unwrap()
                    .dialers
                    .register(&mut waiter, cx.waker());
                Poll::Pending
            }
            Dial::Connect => {
                let ip = host_ip(from);
                let port = match state.ephemeral_port(ip) {
                    Ok(port) => port,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                let id = state.next_conn;
                state.next_conn += 1;
                let client = SocketAddr::new(ip, port);
                state.conns.insert(
                    id,
                    Connection {
                        ends: [
                            End {
                                node: from,
                                addr: client,
                            },
                            End { node: to, addr },
                        ],
                        holds: [client, addr].map(|addr| Some(state.ports.hold(addr))),
                        accepted: false,
                        pipes: [Pipe::default(), Pipe::default()],
                        open: [true, true],
                        reset: false,
                        power_cut: false,
                        alpn: None,
                    },
                );
                let slot = state.listeners.get_mut(&addr).unwrap();
                slot.backlog.push_back(id);
                slot.acceptors.wake_all();
                Poll::Ready(Ok(Pending {
                    half: Half::new(net.clone(), id, Side::Client),
                }))
            }
        }
    })
    .await
}

fn bind(net: Arc<Net>, node: Option<u64>, addr: SocketAddr) -> io::Result<SimListener> {
    let ip = host_ip(node);
    let wanted = own_view(addr, node);
    if wanted.ip() != ip {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!(
                "{} is not the simulated address of this node ({ip})",
                wanted.ip()
            ),
        ));
    }
    let mut state = net.lock();
    let port = match wanted.port() {
        0 => state.ephemeral_port(ip)?,
        port if state.port_free(ip, port) => port,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("simulated {wanted} is in use"),
            ));
        }
    };
    let addr = SocketAddr::new(ip, port);
    let id = state.next_listener;
    state.next_listener += 1;
    state.listeners.insert(
        addr,
        Slot {
            id,
            node,
            backlog: VecDeque::new(),
            acceptors: Waiters::default(),
            dialers: Waiters::default(),
        },
    );
    drop(state);
    Ok(SimListener { net, addr, id })
}

/// A simulated listener. Dropping it, or crashing its node, closes it and resets the
/// connections it has not accepted.
pub struct SimListener {
    net: Arc<Net>,
    addr: SocketAddr,
    id: u64,
}

impl std::fmt::Debug for SimListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimListener")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl SimListener {
    /// Binds `addr` for the current node in the installed world.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        bind(installed()?, current_node(), addr)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn accept(&self) -> io::Result<(Pending, SocketAddr)> {
        let mut waiter = None;
        poll_fn(|cx| {
            let mut state = self.net.lock();
            let state = &mut *state;
            let Some(slot) = state
                .listeners
                .get_mut(&self.addr)
                .filter(|slot| slot.id == self.id)
            else {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "the simulated listener was closed",
                )));
            };
            match slot.backlog.pop_front() {
                Some(id) => {
                    let peer = state.conns.get_mut(&id).map_or(self.addr, |conn| {
                        conn.accepted = true;
                        conn.end(Side::Client).addr
                    });
                    Poll::Ready(Ok((
                        Pending {
                            half: Half::new(self.net.clone(), id, Side::Server),
                        },
                        peer,
                    )))
                }
                None => {
                    slot.acceptors.register(&mut waiter, cx.waker());
                    Poll::Pending
                }
            }
        })
        .await
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        let mut state = self.net.lock();
        state.close_listener(self.addr, Some(self.id));
    }
}

impl crate::net::Listener for SimListener {
    type Conn = Pending;

    fn accept(&self) -> impl Future<Output = io::Result<(Pending, SocketAddr)>> + Send {
        SimListener::accept(self)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }
}

/// Dials the simulated network from the current node.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimConnector;

impl crate::net::Connector for SimConnector {
    type Conn = Pending;

    fn connect(&self, target: Target<'_>) -> impl Future<Output = io::Result<Pending>> + Send {
        dial(target)
    }
}

/// One side's handle on a connection. Dropping it closes that side (FIN).
struct Half {
    net: Arc<Net>,
    conn: u64,
    side: Side,
    sleep: Option<Pin<Box<Sleep>>>,
}

fn reset_error() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionReset, "simulated connection reset")
}

impl Half {
    fn new(net: Arc<Net>, conn: u64, side: Side) -> Self {
        Self {
            net,
            conn,
            side,
            sleep: None,
        }
    }

    fn info(&self) -> Option<ConnInfo> {
        self.net
            .lock()
            .conns
            .get(&self.conn)
            .map(|conn| conn.info(self.conn))
    }

    fn is_reset(&self) -> bool {
        self.net
            .lock()
            .conns
            .get(&self.conn)
            .is_none_or(|conn| conn.reset)
    }

    fn addr(&self, side: Side) -> SocketAddr {
        let state = self.net.lock();
        state
            .conns
            .get(&self.conn)
            .map_or(SocketAddr::new(EXTERNAL_IP, 0), |conn| conn.end(side).addr)
    }

    fn peer(&self) -> Side {
        match self.side {
            Side::Client => Side::Server,
            Side::Server => Side::Client,
        }
    }

    /// Sends one message through the planner.
    fn send(&self, kind: Kind, body: Body) -> io::Result<()> {
        let mut state = self.net.lock();
        let state = &mut *state;
        let Some(conn) = state.conns.get_mut(&self.conn) else {
            return Err(reset_error());
        };
        if conn.reset {
            return Err(reset_error());
        }
        if conn.power_cut {
            return Ok(());
        }
        let tx = self.side.tx();
        if !matches!(conn.pipes[tx].fin, Fin::Open) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the simulated stream was shut down",
            ));
        }
        if !conn.open[self.side.rx()] {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the simulated peer closed the connection",
            ));
        }
        let pipe = &mut conn.pipes[tx];
        let bytes: &[u8] = match &body {
            Body::Data(bytes) => bytes,
            _ => &[],
        };
        let envelope = Envelope {
            conn: ConnId(self.conn),
            direction: self.side.direction(),
            from: conn.ends[tx].node,
            to: conn.ends[self.side.rx()].node,
            seq: pipe.sent,
            kind,
            alpn: (kind == Kind::Data)
                .then_some(conn.alpn.as_deref())
                .flatten(),
            bytes,
        };
        let fate = state
            .planner
            .as_mut()
            .map_or(Fate::NOW, |planner| planner.message(&envelope));
        pipe.sent += 1;
        match fate {
            Fate::Deliver { .. } | Fate::Frame { .. } if pipe.stalled => Ok(()),
            Fate::Deliver { delay } => {
                if let Body::ServerHello { alpn, .. } = &body {
                    conn.alpn.clone_from(alpn);
                }
                pipe.push(body, None, delay);
                Ok(())
            }
            Fate::Frame { stream, delay } => {
                debug_assert!(kind == Kind::Data && conn.alpn.is_some());
                pipe.push(body, Some(stream), delay);
                Ok(())
            }
            Fate::Drop => {
                pipe.stalled = true;
                Ok(())
            }
            Fate::Reset => {
                conn.reset();
                Err(reset_error())
            }
        }
    }

    /// Polls for the next deliverable message and hands the queue to `take`.
    /// `Ok(None)` is EOF.
    fn poll_recv<R>(
        &mut self,
        cx: &mut Context<'_>,
        mut take: impl FnMut(&mut VecDeque<Message>) -> R,
    ) -> Poll<io::Result<Option<R>>> {
        loop {
            let wait_until = {
                let mut state = self.net.lock();
                let state = &mut *state;
                let Some(conn) = state.conns.get_mut(&self.conn) else {
                    return Poll::Ready(Err(reset_error()));
                };
                if conn.reset {
                    return Poll::Ready(Err(reset_error()));
                }
                let from = conn.end(self.peer()).node;
                let to = conn.end(self.side).node;
                let pipe = &mut conn.pipes[self.side.rx()];
                let held = |sent| {
                    state
                        .holes
                        .get(&(from, to))
                        .is_some_and(|opened| sent >= *opened)
                };
                let ready_at = match pipe.queue.front() {
                    Some(message) if message.read != 0 || !held(message.ready_at) => {
                        message.ready_at
                    }
                    None if matches!(pipe.fin, Fin::Received) => return Poll::Ready(Ok(None)),
                    None if matches!(pipe.fin, Fin::Sent(sent) if !held(sent)) => {
                        pipe.fin = Fin::Received;
                        return Poll::Ready(Ok(None));
                    }
                    _ => {
                        pipe.reader = Some(cx.waker().clone());
                        return Poll::Pending;
                    }
                };
                if ready_at <= Instant::now() {
                    return Poll::Ready(Ok(Some(take(&mut pipe.queue))));
                }
                pipe.reader = Some(cx.waker().clone());
                ready_at
            };
            let sleep = self
                .sleep
                .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(wait_until)));
            if sleep.deadline() != wait_until {
                sleep.as_mut().reset(wait_until);
            }
            match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// The next whole message, for the handshake.
    async fn recv(&mut self) -> io::Result<Option<Body>> {
        poll_fn(|cx| {
            self.poll_recv(cx, |queue| {
                queue
                    .pop_front()
                    .expect("poll_recv takes only from a non-empty queue")
                    .body
            })
        })
        .await
    }

    fn fail(&self, alert: &'static str, kind: io::ErrorKind) -> io::Error {
        let _ = self.send(Kind::Handshake, Body::Alert(alert));
        io::Error::new(kind, alert)
    }
}

impl Drop for Half {
    fn drop(&mut self) {
        let mut state = self.net.lock();
        if let Some(conn) = state.conns.get_mut(&self.conn) {
            conn.open[self.side.tx()] = false;
            if self.side == Side::Client && !conn.accepted {
                conn.holds[self.side.tx()] = None;
            }
            let pipe = &mut conn.pipes[self.side.tx()];
            if !pipe.stalled {
                pipe.finish();
            }
            // Forgotten once both handles are gone.
            if !conn.open.iter().any(|open| *open) {
                state.conns.remove(&self.conn);
            }
        }
    }
}

/// A connection before its handshake: dialed ([`dial`]) or accepted
/// ([`SimListener::accept`]). Dropping it closes the connection.
pub struct Pending {
    half: Half,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("conn", &self.half.conn)
            .field("side", &self.half.side)
            .finish_non_exhaustive()
    }
}

impl Pending {
    pub fn id(&self) -> ConnId {
        ConnId(self.half.conn)
    }

    /// The connection's endpoints, while it exists.
    pub fn info(&self) -> Option<ConnInfo> {
        self.half.info()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.half.addr(self.half.side)
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.half.addr(self.half.peer())
    }

    /// The dialing side's handshake. Offers `offer` and the node's certificate, and
    /// returns the negotiated ALPN and the server's certificate digest.
    pub async fn client_handshake(mut self, offer: &[&[u8]]) -> io::Result<(SimStream, Handshake)> {
        if self.half.side != Side::Client {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "client_handshake on an accepted connection",
            ));
        }
        let certificate = {
            let state = self.half.net.lock();
            let node = state
                .conns
                .get(&self.half.conn)
                .and_then(|conn| conn.end(Side::Client).node);
            state.certificate(node)
        };
        self.half.send(
            Kind::Handshake,
            Body::ClientHello {
                offer: offer.iter().map(|protocol| protocol.to_vec()).collect(),
                certificate,
            },
        )?;
        match self.half.recv().await? {
            Some(Body::ServerHello { alpn, certificate }) => Ok((
                SimStream::new(self.half),
                Handshake {
                    alpn,
                    peer_certificate_sha256: certificate,
                },
            )),
            Some(Body::Alert(alert)) => Err(io::Error::new(io::ErrorKind::InvalidData, alert)),
            Some(_) => Err(self.half.fail(
                "the simulated server did not answer the handshake",
                io::ErrorKind::InvalidData,
            )),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the simulated server closed during the handshake",
            )),
        }
    }

    /// The accepting side's handshake with the server's protocol list `protocols`.
    /// Returns the negotiated ALPN and the client's certificate digest.
    pub async fn server_handshake(
        mut self,
        protocols: &[&[u8]],
    ) -> io::Result<(SimStream, Handshake)> {
        if self.half.side != Side::Server {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "server_handshake on a dialed connection",
            ));
        }
        let (offer, client_certificate) = match self.half.recv().await? {
            Some(Body::ClientHello { offer, certificate }) => (offer, certificate),
            Some(Body::Alert(alert)) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, alert));
            }
            Some(_) => {
                return Err(self.half.fail(
                    "the simulated client did not start a handshake",
                    io::ErrorKind::InvalidData,
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the simulated client closed before its handshake",
                ));
            }
        };
        let alpn = if offer.is_empty() || protocols.is_empty() {
            None
        } else {
            match protocols
                .iter()
                .find(|protocol| offer.iter().any(|offered| offered == *protocol))
            {
                Some(protocol) => Some(protocol.to_vec()),
                None => {
                    return Err(self.half.fail(
                        "no application protocol in common",
                        io::ErrorKind::InvalidData,
                    ));
                }
            }
        };
        let Some(client_certificate) = client_certificate else {
            return Err(self.half.fail(
                "the simulated client presented no certificate",
                io::ErrorKind::PermissionDenied,
            ));
        };
        let server_certificate = {
            let state = self.half.net.lock();
            let node = state
                .conns
                .get(&self.half.conn)
                .and_then(|conn| conn.end(Side::Server).node);
            state.certificate(node)
        };
        let Some(server_certificate) = server_certificate else {
            return Err(self.half.fail(
                "the simulated server has no certificate",
                io::ErrorKind::PermissionDenied,
            ));
        };
        self.half.send(
            Kind::Handshake,
            Body::ServerHello {
                alpn: alpn.clone(),
                certificate: server_certificate,
            },
        )?;
        Ok((
            SimStream::new(self.half),
            Handshake {
                alpn,
                peer_certificate_sha256: client_certificate,
            },
        ))
    }

    /// The connection as a plain stream, with no handshake (HTTP without TLS). A peer
    /// that handshakes fails with `InvalidData`.
    pub fn plain(self) -> SimStream {
        SimStream::new(self.half)
    }
}

/// An established simulated stream. Bytes written between two flushes travel as one
/// message.
pub struct SimStream {
    half: Half,
    buffer: Vec<u8>,
    shut: bool,
}

impl std::fmt::Debug for SimStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimStream")
            .field("conn", &self.half.conn)
            .field("side", &self.half.side)
            .finish_non_exhaustive()
    }
}

impl SimStream {
    fn new(half: Half) -> Self {
        Self {
            half,
            buffer: Vec::new(),
            shut: false,
        }
    }

    pub fn id(&self) -> ConnId {
        ConnId(self.half.conn)
    }

    pub fn info(&self) -> Option<ConnInfo> {
        self.half.info()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.half.addr(self.half.side)
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.half.addr(self.half.peer())
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.buffer);
        self.half.send(Kind::Data, Body::Data(bytes))
    }
}

impl AsyncRead for SimStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        let received = this.half.poll_recv(cx, |queue| {
            let message = queue.front_mut().expect("a deliverable message");
            let Body::Data(bytes) = &message.body else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a simulated handshake message reached an established stream",
                ));
            };
            let rest = &bytes[message.read..];
            let n = rest.len().min(buf.remaining());
            buf.put_slice(&rest[..n]);
            message.read += n;
            if message.read == bytes.len() {
                queue.pop_front();
            }
            Ok(())
        });
        match received {
            Poll::Ready(Ok(Some(result))) => Poll::Ready(result),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for SimStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.shut {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the simulated stream was shut down",
            )));
        }
        if this.half.is_reset() {
            return Poll::Ready(Err(reset_error()));
        }
        this.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().send_buffer())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.shut {
            return Poll::Ready(Ok(()));
        }
        this.send_buffer()?;
        this.shut = true;
        let mut state = this.half.net.lock();
        if let Some(conn) = state.conns.get_mut(&this.half.conn) {
            let pipe = &mut conn.pipes[this.half.side.tx()];
            if !pipe.stalled {
                pipe.finish();
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        // Bytes written but not flushed still leave, as a kernel sends them on close.
        let _ = self.send_buffer();
    }
}

#[cfg(test)]
mod tests;
