//! Persistent multiplexed peer sessions, ALPN `nabedra-otp/2`.
//!
//! One mTLS connection per ordered peer pair and lane (traffic class) carries
//! [`StreamFrame`]s. A request body is the one-shot prologue, header and
//! length-prefixed sections. A reply body is the one-shot reply prefix, header
//! and sections. A peer that negotiates only `nabedra-oltp-worker/1` keeps the
//! one-shot `NABOTP01` exchange.
//!
//! The TLS handshake pins the certificate and ALPN once per connection, and the
//! first verified prologue binds the connection to one sender incarnation. Each
//! stream still runs the local synchronous directory check because trust is
//! group- and purpose-specific. A session closes when its peer's incarnation
//! leaves the local directory. A request that fails after its frame was
//! written has an unknown outcome, as with the one-shot exchange.
use super::*;
use crate::oltp_replication::types::WorkerEnrollment;
use std::{
    collections::HashMap,
    io::IoSlice,
    sync::{MutexGuard, PoisonError, atomic::AtomicU64},
};
use tokio::{
    io::ReadHalf,
    sync::{mpsc, oneshot, watch},
    task::AbortHandle,
};
use tokio_rustls::{client, server};

pub(super) const SESSION_ALPN: &[u8] = b"nabedra-otp/2";
pub(super) const REPLY_PREFIX_BYTES: usize = 53;
pub(super) const MAX_SERVER_SESSIONS: usize = 1024;
const FRAME_BYTES: usize = 13;
const REQUEST: u8 = 1;
const REPLY: u8 = 2;
const CANCEL: u8 = 4;
const SECTION_PREFIX_BYTES: usize = 8 * MAX_SECTIONS;
const DRAIN_BYTES: usize = 16 << 10;
const DIRECTORY_CHECK: Duration = Duration::from_secs(1);
const WRITE_QUEUE: usize = 64;

/// Fixed header of every frame on a session. `len` counts the body only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StreamFrame {
    pub stream_id: u64,
    pub flags: u8,
    pub len: u32,
}
impl StreamFrame {
    fn encode(self) -> [u8; FRAME_BYTES] {
        let mut out = [0; FRAME_BYTES];
        out[..8].copy_from_slice(&self.stream_id.to_be_bytes());
        out[8] = self.flags;
        out[9..].copy_from_slice(&self.len.to_be_bytes());
        out
    }
    fn decode(bytes: [u8; FRAME_BYTES]) -> Self {
        Self {
            stream_id: u64::from_be_bytes(bytes[..8].try_into().expect("fixed stream id")),
            flags: bytes[8],
            len: u32::from_be_bytes(bytes[9..].try_into().expect("fixed frame length")),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The reply prefix shared by the one-shot exchange and session replies.
pub(super) struct ReplyPrefix {
    pub status: u8,
    pub nonce: [u8; 16],
    pub header_len: usize,
    pub lengths: [u64; MAX_SECTIONS],
}
pub(super) fn parse_reply_prefix(
    prefix: &[u8; REPLY_PREFIX_BYTES],
    class: TrafficClass,
    config: &TransportConfig,
) -> Result<ReplyPrefix, ProtocolError> {
    if &prefix[..8] != MAGIC || prefix[24] > 1 {
        return Err(malformed("invalid worker response context"));
    }
    let header_len =
        u32::from_be_bytes(prefix[25..29].try_into().expect("fixed response header")) as usize;
    if header_len > header_limit(class) {
        return Err(malformed("worker response header exceeds limit"));
    }
    let lengths = std::array::from_fn(|i| {
        u64::from_be_bytes(
            prefix[29 + i * 8..37 + i * 8]
                .try_into()
                .expect("fixed response length"),
        )
    });
    validate_plan(
        config,
        class,
        ReceivePlan {
            class,
            section_lengths: lengths,
            decode_budget_bytes: 0,
        },
    )?;
    Ok(ReplyPrefix {
        status: prefix[24],
        nonce: prefix[8..24].try_into().expect("fixed nonce"),
        header_len,
        lengths,
    })
}
fn encode_reply_prefix(nonce: Uuid, status: u8, reply: &ChargedReply) -> [u8; REPLY_PREFIX_BYTES] {
    let mut out = [0; REPLY_PREFIX_BYTES];
    out[..8].copy_from_slice(MAGIC);
    out[8..24].copy_from_slice(nonce.as_bytes());
    out[24] = status;
    out[25..29].copy_from_slice(&(reply.header.len() as u32).to_be_bytes());
    for (i, section) in reply.sections.iter().enumerate() {
        let length = section.as_ref().map_or(0, |bytes| bytes.len() as u64);
        out[29 + i * 8..37 + i * 8].copy_from_slice(&length.to_be_bytes());
    }
    out
}

/// A reply whose framing was checked. The caller checks the nonce, status and
/// its decode budget.
pub(super) struct RawReply {
    pub prefix: ReplyPrefix,
    pub header: ChargedBytes,
    pub sections: [Option<ChargedBytes>; MAX_SECTIONS],
}

enum Part {
    Charged(ChargedBytes),
    Length([u8; 8]),
}
impl Part {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Charged(bytes) => bytes.as_slice(),
            Self::Length(bytes) => bytes,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteOutcome {
    Written,
    NotSent,
    Failed,
}
/// One frame. The charged parts stay owned until the frame is on the socket.
struct Outbound {
    head: Vec<u8>,
    parts: Vec<Part>,
    ack: Option<oneshot::Sender<WriteOutcome>>,
}
impl Outbound {
    fn cancel(stream_id: u64) -> Self {
        Self {
            head: StreamFrame {
                stream_id,
                flags: CANCEL,
                len: 0,
            }
            .encode()
            .to_vec(),
            parts: Vec::new(),
            ack: None,
        }
    }
}

/// Write the whole frame with vectored writes, then flush it.
async fn write_frame(stream: &mut (impl AsyncWrite + Unpin), frame: &Outbound) -> io::Result<()> {
    let mut slices: Vec<IoSlice<'_>> = std::iter::once(frame.head.as_slice())
        .chain(frame.parts.iter().map(Part::as_slice))
        .filter(|bytes| !bytes.is_empty())
        .map(IoSlice::new)
        .collect();
    let mut rest = &mut slices[..];
    while !rest.is_empty() {
        let written = stream.write_vectored(rest).await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut rest, written);
    }
    stream.flush().await
}

/// Serializes frames from all streams. A failed write closes the session; the
/// frame being written reports `Failed` (unknown outcome) and queued frames
/// report `NotSent`.
async fn write_frames(
    mut stream: impl AsyncWrite + Unpin,
    mut queue: mpsc::Receiver<Outbound>,
    mut stop: Option<watch::Receiver<bool>>,
    deadline: Duration,
) -> bool {
    let healthy = loop {
        let frame = match stop.as_mut() {
            Some(stop) => tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break true;
                    }
                    continue;
                }
                frame = queue.recv() => frame,
            },
            None => queue.recv().await,
        };
        let Some(frame) = frame else {
            break true;
        };
        let written = matches!(
            timeout(deadline, write_frame(&mut stream, &frame)).await,
            Ok(Ok(()))
        );
        if let Some(ack) = frame.ack {
            let _ = ack.send(if written {
                WriteOutcome::Written
            } else {
                WriteOutcome::Failed
            });
        }
        if !written {
            break false;
        }
    };
    queue.close();
    while let Ok(frame) = queue.try_recv() {
        if let Some(ack) = frame.ack {
            let _ = ack.send(WriteOutcome::NotSent);
        }
    }
    if healthy {
        let _ = timeout(deadline, stream.shutdown()).await;
    }
    healthy
}

async fn drain(stream: &mut (impl AsyncRead + Unpin), mut len: usize) -> io::Result<()> {
    let mut buffer = vec![0; len.min(DRAIN_BYTES)];
    while len != 0 {
        let chunk = len.min(buffer.len());
        stream.read_exact(&mut buffer[..chunk]).await?;
        len -= chunk;
    }
    Ok(())
}

fn enrolled(directory: &dyn PeerDirectory, peer: WorkerIdentity) -> bool {
    directory
        .resolve(peer.worker_id)
        .is_ok_and(|enrollment| WorkerIdentity::from(&enrollment) == peer)
}

fn framing() -> io::Error {
    io::ErrorKind::InvalidData.into()
}

// ---------------------------------------------------------------- client side

enum CallError {
    /// The frame never reached the socket. A fresh session may retry it.
    NotSent,
    Failed(RequestError),
}

struct Waiters {
    closed: bool,
    streams: HashMap<u64, oneshot::Sender<Result<RawReply, RequestError>>>,
}

struct ClientSession {
    enrollment: WorkerEnrollment,
    identity: WorkerIdentity,
    outgoing: mpsc::Sender<Outbound>,
    waiters: Mutex<Waiters>,
    closed: watch::Sender<bool>,
    next: AtomicU64,
    reader: Mutex<Option<AbortHandle>>,
}
impl ClientSession {
    fn start(
        stream: client::TlsStream<TcpStream>,
        enrollment: WorkerEnrollment,
        class: TrafficClass,
        common: &Common,
    ) -> Arc<Self> {
        let (read, write) = tokio::io::split(stream);
        let (outgoing, queue) = mpsc::channel(WRITE_QUEUE);
        let (closed, stop) = watch::channel(false);
        let session = Arc::new(Self {
            identity: WorkerIdentity::from(&enrollment),
            enrollment,
            outgoing,
            waiters: Mutex::new(Waiters {
                closed: false,
                streams: HashMap::new(),
            }),
            closed,
            next: AtomicU64::new(1),
            reader: Mutex::new(None),
        });
        let deadline = common.config.deadline;
        tokio::spawn({
            let session = session.clone();
            async move {
                write_frames(write, queue, Some(stop), deadline).await;
                session.close();
            }
        });
        let mut reader = tokio::spawn(read_replies(
            session.clone(),
            read,
            class,
            common.config.clone(),
            common.shared.clone(),
        ));
        *lock(&session.reader) = Some(reader.abort_handle());
        let directory = common.directory.clone();
        tokio::spawn({
            let session = session.clone();
            async move {
                let mut stop = session.closed.subscribe();
                let mut check = tokio::time::interval(DIRECTORY_CHECK);
                check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = &mut reader => break,
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                break;
                            }
                        }
                        _ = check.tick() => {
                            if !enrolled(&*directory, session.identity) {
                                break;
                            }
                        }
                    }
                }
                session.close();
            }
        });
        session
    }
    fn alive(&self) -> bool {
        !*self.closed.borrow()
    }
    fn close(&self) {
        let streams = {
            let mut waiters = lock(&self.waiters);
            waiters.closed = true;
            std::mem::take(&mut waiters.streams)
        };
        for waiter in streams.into_values() {
            let _ = waiter.send(Err(RequestError::Unreachable));
        }
        self.closed.send_replace(true);
        if let Some(reader) = lock(&self.reader).take() {
            reader.abort();
        }
    }
    fn waiter(&self, stream_id: u64) -> Option<oneshot::Sender<Result<RawReply, RequestError>>> {
        lock(&self.waiters).streams.remove(&stream_id)
    }
    async fn call(
        self: &Arc<Self>,
        hello: &[u8; PROLOGUE_BYTES],
        header: &ChargedBytes,
        sections: &[Option<ChargedBytes>; MAX_SECTIONS],
        deadline: Instant,
        phase: &mut RaftTransportPhase,
    ) -> Result<RawReply, CallError> {
        let len = PROLOGUE_BYTES
            + header.len()
            + SECTION_PREFIX_BYTES
            + sections
                .iter()
                .flatten()
                .map(ChargedBytes::len)
                .sum::<usize>();
        let len = u32::try_from(len).map_err(|_| {
            CallError::Failed(malformed("worker request frame exceeds limit").into())
        })?;
        let stream_id = self.next.fetch_add(1, Ordering::Relaxed);
        let (reply, response) = oneshot::channel();
        {
            let mut waiters = lock(&self.waiters);
            if waiters.closed {
                return Err(CallError::NotSent);
            }
            waiters.streams.insert(stream_id, reply);
        }
        let mut guard = StreamGuard {
            session: self,
            stream_id,
            written: false,
            settled: false,
        };
        let mut head = Vec::with_capacity(FRAME_BYTES + PROLOGUE_BYTES);
        head.extend_from_slice(
            &StreamFrame {
                stream_id,
                flags: REQUEST,
                len,
            }
            .encode(),
        );
        head.extend_from_slice(hello);
        let mut parts = Vec::with_capacity(1 + 2 * MAX_SECTIONS);
        parts.push(Part::Charged(header.clone()));
        for section in sections {
            let bytes = section.as_ref().map_or(0, ChargedBytes::len) as u64;
            parts.push(Part::Length(bytes.to_be_bytes()));
            if let Some(section) = section {
                parts.push(Part::Charged(section.clone()));
            }
        }
        let (ack, written) = oneshot::channel();
        *phase = RaftTransportPhase::SendRequest;
        let frame = Outbound {
            head,
            parts,
            ack: Some(ack),
        };
        match timeout_at(deadline, self.outgoing.send(frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(CallError::NotSent),
            Err(_) => return Err(CallError::Failed(RequestError::Unreachable)),
        }
        match timeout_at(deadline, written).await {
            Ok(Ok(WriteOutcome::Written)) => guard.written = true,
            Ok(Ok(WriteOutcome::NotSent)) => return Err(CallError::NotSent),
            _ => {
                guard.written = true;
                return Err(CallError::Failed(RequestError::Unreachable));
            }
        }
        *phase = RaftTransportPhase::ReceiveReply;
        match timeout_at(deadline, response).await {
            Ok(Ok(result)) => {
                guard.settled = true;
                result.map_err(CallError::Failed)
            }
            Ok(Err(_)) => {
                guard.settled = true;
                Err(CallError::Failed(RequestError::Unreachable))
            }
            Err(_) => Err(CallError::Failed(RequestError::Unreachable)),
        }
    }
}

/// A dropped or expired caller withdraws its waiter. Once its frame was
/// written, the peer is asked to discard the reply; the handler still runs.
struct StreamGuard<'a> {
    session: &'a Arc<ClientSession>,
    stream_id: u64,
    written: bool,
    settled: bool,
}
impl Drop for StreamGuard<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let pending = self.session.waiter(self.stream_id).is_some();
        if pending && self.written {
            let _ = self
                .session
                .outgoing
                .try_send(Outbound::cancel(self.stream_id));
        }
    }
}

async fn read_replies(
    session: Arc<ClientSession>,
    mut stream: ReadHalf<client::TlsStream<TcpStream>>,
    class: TrafficClass,
    config: TransportConfig,
    shared: Arc<WorkerSharedResources>,
) {
    loop {
        let mut head = [0; FRAME_BYTES];
        if stream.read_exact(&mut head).await.is_err() {
            return;
        }
        let frame = StreamFrame::decode(head);
        let waiter = session.waiter(frame.stream_id);
        match frame.flags {
            REPLY => {
                let read = timeout(
                    config.deadline,
                    read_reply(&mut stream, frame.len as usize, class, &config, &shared),
                )
                .await;
                match read {
                    Ok(Ok(result)) => {
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(result);
                        }
                    }
                    _ => {
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(Err(RequestError::Unreachable));
                        }
                        return;
                    }
                }
            }
            CANCEL if frame.len == 0 => {
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Err(RequestError::Unreachable));
                }
            }
            _ => {
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Err(RequestError::Unreachable));
                }
                return;
            }
        }
    }
}

/// The outer error breaks framing and closes the session. The inner error is
/// local to this stream; its body has been consumed.
async fn read_reply(
    stream: &mut (impl AsyncRead + Unpin),
    len: usize,
    class: TrafficClass,
    config: &TransportConfig,
    shared: &WorkerSharedResources,
) -> io::Result<Result<RawReply, RequestError>> {
    if len < REPLY_PREFIX_BYTES {
        return Err(framing());
    }
    let mut prefix = [0; REPLY_PREFIX_BYTES];
    stream.read_exact(&mut prefix).await?;
    let mut rest = len - REPLY_PREFIX_BYTES;
    let prefix = match parse_reply_prefix(&prefix, class, config) {
        Ok(prefix) => prefix,
        Err(error) => {
            drain(stream, rest).await?;
            return Ok(Err(error.into()));
        }
    };
    let body = prefix.lengths.iter().sum::<u64>() + prefix.header_len as u64;
    if body != rest as u64 {
        return Err(framing());
    }
    let byte_class = class.byte_class();
    let mut header = match shared.allocate_bytes(byte_class, prefix.header_len) {
        Ok(header) => header,
        Err(error) => {
            drain(stream, rest).await?;
            return Ok(Err(error.into()));
        }
    };
    stream.read_exact(header.as_mut_slice()).await?;
    rest -= prefix.header_len;
    let mut sections: [Option<ChargedBytes>; MAX_SECTIONS] = std::array::from_fn(|_| None);
    for (slot, &length) in sections.iter_mut().zip(&prefix.lengths) {
        if length == 0 {
            continue;
        }
        let length = length as usize;
        let mut bytes = match shared.allocate_bytes(byte_class, length) {
            Ok(bytes) => bytes,
            Err(error) => {
                drain(stream, rest).await?;
                return Ok(Err(error.into()));
            }
        };
        stream.read_exact(bytes.as_mut_slice()).await?;
        rest -= length;
        *slot = Some(bytes.freeze());
    }
    Ok(Ok(RawReply {
        prefix,
        header: header.freeze(),
        sections,
    }))
}

enum Route {
    Session(Arc<ClientSession>),
    OneShot(Box<client::TlsStream<TcpStream>>),
}

#[derive(Default)]
struct Slot {
    current: Mutex<Option<Arc<ClientSession>>>,
    legacy: Mutex<Option<WorkerEnrollment>>,
    connecting: tokio::sync::Mutex<()>,
}

/// Caller-side sessions, keyed by peer worker, incarnation and lane.
#[derive(Default)]
pub(super) struct PeerSessions {
    slots: Mutex<HashMap<(Uuid, Uuid, usize), Arc<Slot>>>,
    connections: AtomicU64,
}
impl Drop for PeerSessions {
    fn drop(&mut self) {
        for slot in lock(&self.slots).values() {
            if let Some(session) = lock(&slot.current).take() {
                session.close();
            }
        }
    }
}
impl PeerSessions {
    /// TLS connections this caller opened, sessions and one-shot exchanges.
    #[cfg(test)]
    pub(super) fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(super) fn live_for_test(&self) -> usize {
        lock(&self.slots)
            .values()
            .filter(|slot| lock(&slot.current).as_ref().is_some_and(|s| s.alive()))
            .count()
    }
    fn slot(&self, peer: &WorkerEnrollment, class: TrafficClass) -> Arc<Slot> {
        let mut slots = lock(&self.slots);
        let key = (peer.worker_id, peer.incarnation, class as usize);
        if let Some(slot) = slots.get(&key) {
            return slot.clone();
        }
        // A new incarnation retires every lane of the previous one.
        slots.retain(|&(worker, incarnation, _), slot| {
            let keep = worker != peer.worker_id || incarnation == peer.incarnation;
            if !keep && let Some(session) = lock(&slot.current).take() {
                session.close();
            }
            keep
        });
        slots.entry(key).or_default().clone()
    }
    async fn route(
        &self,
        common: &Common,
        peer: &WorkerEnrollment,
        class: TrafficClass,
        probe_resolving: Option<&mut bool>,
        phase: &mut RaftTransportPhase,
    ) -> Result<Route, RequestError> {
        let slot = self.slot(peer, class);
        let current = || {
            lock(&slot.current)
                .as_ref()
                .filter(|session| session.alive() && session.enrollment == *peer)
                .cloned()
        };
        if let Some(session) = current() {
            return Ok(Route::Session(session));
        }
        // Old peers get one connection per call, so they skip the connect lock.
        let legacy = lock(&slot.legacy).as_ref() == Some(peer);
        let _connecting = if legacy {
            None
        } else {
            let guard = slot.connecting.lock().await;
            if let Some(session) = current() {
                return Ok(Route::Session(session));
            }
            Some(guard)
        };
        let stream = connect(common, peer, class, probe_resolving, phase).await?;
        self.connections.fetch_add(1, Ordering::Relaxed);
        if stream.get_ref().1.alpn_protocol() == Some(SESSION_ALPN) {
            let session = ClientSession::start(stream, peer.clone(), class, common);
            if let Some(old) = lock(&slot.current).replace(session.clone()) {
                old.close();
            }
            *lock(&slot.legacy) = None;
            Ok(Route::Session(session))
        } else {
            if let Some(old) = lock(&slot.current).take() {
                old.close();
            }
            *lock(&slot.legacy) = Some(peer.clone());
            Ok(Route::OneShot(Box::new(stream)))
        }
    }
    /// One request on the peer's lane session, or a one-shot exchange when
    /// the peer does not offer sessions. A frame that never reached a stale
    /// session's socket is sent once more on a fresh connection.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn call(
        &self,
        common: &Common,
        peer: &WorkerEnrollment,
        class: TrafficClass,
        hello: &[u8; PROLOGUE_BYTES],
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        deadline: Instant,
        mut probe_resolving: Option<&mut bool>,
        phase: &mut RaftTransportPhase,
    ) -> Result<RawReply, RequestError> {
        for attempt in 0..2 {
            let session = match self
                .route(common, peer, class, probe_resolving.as_deref_mut(), phase)
                .await?
            {
                Route::Session(session) => session,
                Route::OneShot(stream) => {
                    return one_shot(*stream, hello, header, sections, class, common, phase).await;
                }
            };
            match session
                .call(hello, &header, &sections, deadline, phase)
                .await
            {
                Ok(reply) => return Ok(reply),
                Err(CallError::NotSent) => {
                    session.close();
                    if attempt != 0 {
                        break;
                    }
                }
                Err(CallError::Failed(error)) => return Err(error),
            }
        }
        Err(RequestError::Unreachable)
    }
}

/// Open and authenticate a connection: the certificate pin and a supported
/// ALPN protocol are checked here, once per connection.
async fn connect(
    common: &Common,
    peer: &WorkerEnrollment,
    class: TrafficClass,
    mut probe_resolving: Option<&mut bool>,
    phase: &mut RaftTransportPhase,
) -> Result<client::TlsStream<TcpStream>, RequestError> {
    let identity = WorkerIdentity::from(peer);
    let name = ServerName::try_from(peer.server_name.as_str().to_owned())
        .map_err(|_| malformed("invalid target TLS name"))?;
    let socket = if let Ok(address) = peer.endpoint.parse::<SocketAddr>() {
        *phase = RaftTransportPhase::Connect;
        TcpStream::connect(address).await
    } else {
        *phase = RaftTransportPhase::ResolveAddress;
        if let Some(resolving) = probe_resolving.as_deref_mut() {
            *resolving = true;
        }
        let addresses = common
            .resolver
            .resolve(identity, &peer.endpoint, class)
            .await
            .map_err(|error| match error {
                ResolveError::LocallyUnavailable if probe_resolving.is_some() => {
                    unavailable().into()
                }
                _ => RequestError::Unreachable,
            })?;
        if let Some(resolving) = probe_resolving {
            *resolving = false;
        }
        *phase = RaftTransportPhase::Connect;
        TcpStream::connect(addresses.as_ref()).await
    }
    .map_err(|_| RequestError::Unreachable)?;
    let _ = socket.set_nodelay(true);
    *phase = RaftTransportPhase::Tls;
    let stream = common
        .connector
        .connect(name, socket)
        .await
        .map_err(|_| RequestError::Unreachable)?;
    let alpn = stream.get_ref().1.alpn_protocol();
    let supported = alpn == Some(ALPN) || common.sessions_enabled && alpn == Some(SESSION_ALPN);
    if !supported || peer_digest(stream.get_ref().1.peer_certificates())? != peer.certificate_sha256
    {
        return Err(malformed("target certificate or ALPN differs").into());
    }
    Ok(stream)
}

/// The `NABOTP01` exchange for a peer that negotiated the one-shot protocol.
async fn one_shot(
    mut stream: client::TlsStream<TcpStream>,
    hello: &[u8; PROLOGUE_BYTES],
    header: ChargedBytes,
    sections: [Option<ChargedBytes>; MAX_SECTIONS],
    class: TrafficClass,
    common: &Common,
    phase: &mut RaftTransportPhase,
) -> Result<RawReply, RequestError> {
    *phase = RaftTransportPhase::SendRequest;
    stream
        .write_all(hello)
        .await
        .map_err(|_| RequestError::Unreachable)?;
    stream
        .write_all(header.as_slice())
        .await
        .map_err(|_| RequestError::Unreachable)?;
    for section in &sections {
        let bytes = section.as_ref().map_or(&[][..], ChargedBytes::as_slice);
        stream
            .write_u64(bytes.len() as u64)
            .await
            .map_err(|_| RequestError::Unreachable)?;
        stream
            .write_all(bytes)
            .await
            .map_err(|_| RequestError::Unreachable)?;
    }
    stream
        .flush()
        .await
        .map_err(|_| RequestError::Unreachable)?;
    *phase = RaftTransportPhase::ReceiveReply;
    let mut prefix = [0; REPLY_PREFIX_BYTES];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|_| RequestError::Unreachable)?;
    let prefix = parse_reply_prefix(&prefix, class, &common.config)?;
    let byte_class = class.byte_class();
    let mut response_header = common
        .shared
        .allocate_bytes(byte_class, prefix.header_len)?;
    stream
        .read_exact(response_header.as_mut_slice())
        .await
        .map_err(|_| RequestError::Unreachable)?;
    let mut response_sections: [Option<ChargedBytes>; MAX_SECTIONS] = std::array::from_fn(|_| None);
    if prefix.status == 0 {
        for (slot, &length) in response_sections.iter_mut().zip(&prefix.lengths) {
            if length != 0 {
                let mut bytes = common.shared.allocate_bytes(byte_class, length as usize)?;
                stream
                    .read_exact(bytes.as_mut_slice())
                    .await
                    .map_err(|_| RequestError::Unreachable)?;
                *slot = Some(bytes.freeze());
            }
        }
    }
    Ok(RawReply {
        prefix,
        header: response_header.freeze(),
        sections: response_sections,
    })
}

// ---------------------------------------------------------------- server side

#[derive(Default)]
struct ServerSession {
    /// The sender incarnation every later prologue must carry.
    bound: Mutex<Option<WorkerIdentity>>,
    /// Streams in flight; `true` once the caller cancelled the reply.
    active: Mutex<HashMap<u64, Arc<AtomicBool>>>,
}

struct StreamWork {
    stream_id: u64,
    nonce: Uuid,
    group_id: Uuid,
    purpose: Purpose,
    class: TrafficClass,
    session: AuthenticatedSession,
    endpoint: Arc<dyn Endpoint>,
    _lease: ResidentLease,
    header: ChargedBytes,
    sections: [Option<ChargedBytes>; MAX_SECTIONS],
    preflight: OwnedSemaphorePermit,
}

enum Received {
    Stream(Box<StreamWork>),
    /// Fail the stream without a reply, as the one-shot listener closes its
    /// connection. The caller sees an unreachable peer.
    Abort,
    Reject {
        nonce: Uuid,
        class: TrafficClass,
        error: ProtocolError,
    },
    /// The connection itself is no longer trustworthy or framed.
    Close,
}

fn max_request_frame(config: &TransportConfig) -> usize {
    PROLOGUE_BYTES
        + MAX_HEADER_BYTES
        + SECTION_PREFIX_BYTES
        + config.payload_bytes.iter().copied().max().unwrap_or(0)
}

/// Serve one accepted session until the peer closes it, its incarnation leaves
/// the local directory, or the listener stops. In-flight streams finish (each
/// within the transport deadline) before the connection closes.
pub(super) async fn serve(
    transport: Arc<WorkerTransport>,
    stream: server::TlsStream<TcpStream>,
    certificate: Digest,
    mut stop: watch::Receiver<bool>,
    _slot: OwnedSemaphorePermit,
) {
    if *stop.borrow() {
        return;
    }
    let (read, write) = tokio::io::split(stream);
    let (outgoing, queue) = mpsc::channel(WRITE_QUEUE);
    let deadline = transport.common.config.deadline;
    let writer = tokio::spawn(write_frames(write, queue, None, deadline));
    let state = Arc::new(ServerSession::default());
    let mut reader = tokio::spawn(serve_frames(
        transport.clone(),
        read,
        outgoing,
        certificate,
        state.clone(),
    ));
    let mut check = tokio::time::interval(DIRECTORY_CHECK);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let read_finished = loop {
        tokio::select! {
            _ = &mut reader => break true,
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break false;
                }
            }
            _ = check.tick() => {
                let bound = *lock(&state.bound);
                if bound.is_some_and(|peer| !enrolled(&*transport.common.directory, peer)) {
                    break false;
                }
            }
        }
    };
    if !read_finished {
        reader.abort();
        let _ = reader.await;
    }
    let _ = writer.await;
}

async fn serve_frames(
    transport: Arc<WorkerTransport>,
    mut stream: ReadHalf<server::TlsStream<TcpStream>>,
    outgoing: mpsc::Sender<Outbound>,
    certificate: Digest,
    state: Arc<ServerSession>,
) {
    let deadline = transport.common.config.deadline;
    loop {
        let mut head = [0; FRAME_BYTES];
        if stream.read_exact(&mut head).await.is_err() {
            return;
        }
        let frame = StreamFrame::decode(head);
        match frame.flags {
            REQUEST => {
                let received = timeout(
                    deadline,
                    transport.receive(&mut stream, frame, certificate, &state),
                )
                .await;
                match received {
                    Ok(Ok(Received::Stream(work))) => {
                        let cancelled = Arc::new(AtomicBool::new(false));
                        lock(&state.active).insert(work.stream_id, cancelled.clone());
                        tokio::spawn(run_stream(
                            transport.clone(),
                            *work,
                            outgoing.clone(),
                            state.clone(),
                            cancelled,
                        ));
                    }
                    Ok(Ok(Received::Abort)) => {
                        if outgoing
                            .send(Outbound::cancel(frame.stream_id))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(Ok(Received::Reject {
                        nonce,
                        class,
                        error,
                    })) => {
                        let Some(reply) = error_reply(&transport.common, class, &error) else {
                            return;
                        };
                        let frame = reply_frame(frame.stream_id, nonce, 1, reply);
                        if outgoing.send(frame).await.is_err() {
                            return;
                        }
                    }
                    _ => return,
                }
            }
            CANCEL if frame.len == 0 => {
                if let Some(cancelled) = lock(&state.active).get(&frame.stream_id) {
                    cancelled.store(true, Ordering::Release);
                }
            }
            _ => return,
        }
    }
}

fn error_reply(
    common: &Common,
    class: TrafficClass,
    error: &ProtocolError,
) -> Option<ChargedReply> {
    let header = common
        .shared
        .encode_in(ByteClass::Progress, error, header_limit(class).min(2048))
        .ok()?;
    Some(ChargedReply {
        header,
        sections: std::array::from_fn(|_| None),
    })
}

fn reply_frame(stream_id: u64, nonce: Uuid, status: u8, reply: ChargedReply) -> Outbound {
    let prefix = encode_reply_prefix(nonce, status, &reply);
    let len = REPLY_PREFIX_BYTES
        + reply.header.len()
        + reply
            .sections
            .iter()
            .flatten()
            .map(ChargedBytes::len)
            .sum::<usize>();
    let mut head = Vec::with_capacity(FRAME_BYTES + REPLY_PREFIX_BYTES);
    head.extend_from_slice(
        &StreamFrame {
            stream_id,
            flags: REPLY,
            // validate_reply bounds every reply far below u32::MAX.
            len: len as u32,
        }
        .encode(),
    );
    head.extend_from_slice(&prefix);
    let mut parts = vec![Part::Charged(reply.header)];
    parts.extend(reply.sections.into_iter().flatten().map(Part::Charged));
    Outbound {
        head,
        parts,
        ack: None,
    }
}

async fn run_stream(
    transport: Arc<WorkerTransport>,
    work: StreamWork,
    outgoing: mpsc::Sender<Outbound>,
    state: Arc<ServerSession>,
    cancelled: Arc<AtomicBool>,
) {
    let common = &transport.common;
    let StreamWork {
        stream_id,
        nonce,
        group_id,
        purpose,
        class,
        session,
        endpoint,
        _lease,
        header,
        sections,
        preflight,
    } = work;
    let mut trace = RaftTransportTrace {
        group: group_id,
        purpose,
        phase: RaftTransportPhase::Preflight,
    };
    let lengths: [u64; MAX_SECTIONS] =
        std::array::from_fn(|i| sections[i].as_ref().map_or(0, |bytes| bytes.len() as u64));
    let handled = timeout(common.config.deadline, async {
        let header_scratch = common
            .shared
            .reserve_scratch(class.byte_class(), header_decode_bytes(class, header.len()))?;
        let plan = endpoint.preflight(&session, header.as_slice()).await?;
        drop(header_scratch);
        validate_plan(&common.config, class, plan)?;
        let handler = common.handlers[class as usize]
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded("worker handler lane full"))?;
        trace.phase = RaftTransportPhase::ReadSections;
        if plan.section_lengths != lengths {
            return Err(malformed("worker section length differs from preflight"));
        }
        let scratch = Arc::new(
            common
                .shared
                .reserve_scratch(class.byte_class(), plan.decode_budget_bytes)?,
        );
        let sections = ChargedSections {
            sections,
            scratch,
            resources: common.shared.clone(),
        };
        drop(preflight);
        trace.phase = RaftTransportPhase::Handle;
        let reply = endpoint.handle(session, header, sections).await?;
        validate_reply(&common.config, class, &reply)?;
        Ok((reply, handler))
    })
    .await;
    let (frame, handler) = match handled {
        Ok(Ok((reply, handler))) => (Some(reply_frame(stream_id, nonce, 0, reply)), Some(handler)),
        Ok(Err(error)) => {
            if matches!(purpose, Purpose::Raft | Purpose::RaftProgress) {
                tracing::debug!(group=%group_id, ?purpose, phase=?trace.phase,
                    code=?error.code, "Raft transport rejected request");
            }
            (
                error_reply(common, class, &error)
                    .map(|reply| reply_frame(stream_id, nonce, 1, reply)),
                None,
            )
        }
        Err(_) => (None, None),
    };
    trace.phase = RaftTransportPhase::SendReply;
    if !cancelled.load(Ordering::Acquire) {
        let mut frame = frame.unwrap_or_else(|| Outbound::cancel(stream_id));
        let (ack, written) = oneshot::channel();
        frame.ack = Some(ack);
        if outgoing.send(frame).await.is_ok() {
            // The handler lane stays held until the reply reaches the socket.
            let _ = written.await;
        }
    }
    drop(handler);
    lock(&state.active).remove(&stream_id);
    trace.phase = RaftTransportPhase::Complete;
}

impl WorkerTransport {
    /// Read one request frame, authenticate its prologue and charge its bytes.
    /// The preflight lane is held before any header or section bytes are
    /// allocated, as in the one-shot listener.
    async fn receive(
        &self,
        stream: &mut ReadHalf<server::TlsStream<TcpStream>>,
        frame: StreamFrame,
        certificate: Digest,
        state: &ServerSession,
    ) -> io::Result<Received> {
        let common = &self.common;
        let len = frame.len as usize;
        if len < PROLOGUE_BYTES + SECTION_PREFIX_BYTES || len > max_request_frame(&common.config) {
            return Ok(Received::Close);
        }
        if lock(&state.active).contains_key(&frame.stream_id) {
            return Ok(Received::Close);
        }
        let mut bytes = [0; PROLOGUE_BYTES];
        stream.read_exact(&mut bytes).await?;
        let rest = len - PROLOGUE_BYTES;
        let Ok(hello) = Hello::decode(&bytes, certificate, common.local.certificate_sha256) else {
            drain(stream, rest).await?;
            return Ok(Received::Abort);
        };
        if lock(&state.bound).is_some_and(|peer| peer != hello.from) {
            return Ok(Received::Close);
        }
        let Some(sections_total) = rest.checked_sub(hello.header_len + SECTION_PREFIX_BYTES) else {
            return Ok(Received::Close);
        };
        let routed = (|| {
            let (lease, endpoint) = self.registry.lookup(hello.group_id, hello.purpose)?;
            let group = lease.identity();
            if hello.cluster_id != group.cluster_id
                || hello.group_digest != identity_digest(group)?
                || hello.to != common.local
            {
                return Err(malformed("foreign worker/group audience"));
            }
            Ok((lease, endpoint))
        })();
        let Ok((lease, endpoint)) = routed else {
            drain(stream, rest).await?;
            return Ok(Received::Abort);
        };
        let group = lease.identity().clone();
        let trust = match common.directory.verify(hello.from, &group, hello.purpose) {
            Ok(trust) => trust,
            Err(_) if !enrolled(&*common.directory, hello.from) => {
                return Ok(Received::Close);
            }
            Err(_) => {
                drain(stream, rest).await?;
                return Ok(Received::Abort);
            }
        };
        let Ok(class) =
            admit_trust(trust, &group, hello.purpose).and_then(|()| traffic(&group, hello.purpose))
        else {
            drain(stream, rest).await?;
            return Ok(Received::Abort);
        };
        lock(&state.bound).get_or_insert(hello.from);
        if sections_total > common.config.payload_bytes[class as usize] {
            drain(stream, rest).await?;
            return Ok(Received::Reject {
                nonce: hello.nonce,
                class,
                error: malformed("worker receive plan exceeds class limits"),
            });
        }
        let Ok(preflight) = common.preflight[class as usize].clone().try_acquire_owned() else {
            drain(stream, rest).await?;
            return Ok(Received::Abort);
        };
        let byte_class = class.byte_class();
        let Ok(mut header) = common.shared.allocate_bytes(byte_class, hello.header_len) else {
            drain(stream, rest).await?;
            return Ok(Received::Abort);
        };
        stream.read_exact(header.as_mut_slice()).await?;
        let mut rest = rest - hello.header_len;
        let mut sections: [Option<ChargedBytes>; MAX_SECTIONS] = std::array::from_fn(|_| None);
        for (index, slot) in sections.iter_mut().enumerate() {
            let length = stream.read_u64().await?;
            rest -= 8;
            let prefixes = 8 * (MAX_SECTIONS - 1 - index);
            let Some(length) = usize::try_from(length)
                .ok()
                .filter(|&length| length + prefixes <= rest)
            else {
                return Ok(Received::Close);
            };
            if length == 0 {
                continue;
            }
            let mut bytes = match common.shared.allocate_bytes(byte_class, length) {
                Ok(bytes) => bytes,
                Err(error) => {
                    drain(stream, rest).await?;
                    return Ok(Received::Reject {
                        nonce: hello.nonce,
                        class,
                        error,
                    });
                }
            };
            stream.read_exact(bytes.as_mut_slice()).await?;
            rest -= length;
            *slot = Some(bytes.freeze());
        }
        if rest != 0 {
            return Ok(Received::Close);
        }
        Ok(Received::Stream(Box::new(StreamWork {
            stream_id: frame.stream_id,
            nonce: hello.nonce,
            group_id: hello.group_id,
            purpose: hello.purpose,
            class,
            session: AuthenticatedSession {
                local: common.local,
                remote: hello.from,
                group,
                purpose: hello.purpose,
                nonce: hello.nonce,
                recovery_only: trust.recovery_only(),
            },
            endpoint,
            _lease: lease,
            header: header.freeze(),
            sections,
            preflight,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn d10_stream_frame_round_trips_fixed_layout() {
        let frame = StreamFrame {
            stream_id: 0x0102_0304_0506_0708,
            flags: REQUEST | CANCEL,
            len: 0x0a0b_0c0d,
        };
        let bytes = frame.encode();
        assert_eq!(
            bytes,
            [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                REQUEST | CANCEL,
                0x0a,
                0x0b,
                0x0c,
                0x0d
            ]
        );
        assert_eq!(StreamFrame::decode(bytes), frame);
    }

    #[tokio::test]
    async fn d10_vectored_frame_write_emits_exact_bytes_in_order() {
        let shared = WorkerSharedResources::new(
            1,
            crate::config::WorkerResources::default(),
            crate::oltp::pool::SharedResourceConfig::default(),
        )
        .unwrap();
        let header = shared
            .copy_bytes_in(ByteClass::Progress, b"header")
            .unwrap();
        let frame = Outbound {
            head: vec![1, 2, 3],
            parts: vec![
                Part::Charged(header),
                Part::Length(7u64.to_be_bytes()),
                Part::Length([0; 8]),
            ],
            ack: None,
        };
        let mut out = Vec::new();
        write_frame(&mut out, &frame).await.unwrap();
        let mut expected = vec![1, 2, 3];
        expected.extend_from_slice(b"header");
        expected.extend_from_slice(&7u64.to_be_bytes());
        expected.extend_from_slice(&[0; 8]);
        assert_eq!(out, expected);
    }
}
