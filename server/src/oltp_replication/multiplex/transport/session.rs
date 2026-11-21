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
//!
//! Liveness: a caller pings a session that has been silent too long and
//! closes it when nothing comes back, and also closes it when a reply times
//! out with nothing received since the request was written. TCP keepalive is
//! on for both sides. A receiver closes sessions that never name a lane and
//! sessions idle for four ping intervals.
//!
//! Capacity: each traffic class has its own pool of accepted sessions. A
//! session takes a pending slot during the handshake and its lane slot with
//! its first verified request. A full lane answers that request with a typed
//! overload and closes the session after the reply.
use super::*;
use crate::oltp_replication::types::WorkerEnrollment;
use std::{
    collections::HashMap,
    io::IoSlice,
    pin::Pin,
    sync::{MutexGuard, PoisonError, atomic::AtomicU64},
    task::{Context, Poll},
};
use tokio::{
    io::{ReadBuf, ReadHalf},
    sync::{mpsc, oneshot, watch},
    task::AbortHandle,
};
use tokio_rustls::{client, server};

pub(super) const SESSION_ALPN: &[u8] = b"nabedra-otp/2";
pub(super) const INDEX_RECLAIM_ALPN: &[u8] = b"nabedra-otp/3";

pub(super) fn session_alpn(alpn: Option<&[u8]>) -> bool {
    matches!(alpn, Some(SESSION_ALPN | INDEX_RECLAIM_ALPN))
}
pub(super) const REPLY_PREFIX_BYTES: usize = 53;
const FRAME_BYTES: usize = 13;
const REQUEST: u8 = 1;
const REPLY: u8 = 2;
const CANCEL: u8 = 4;
/// Caller liveness probe; the receiver answers `PONG` with the same id.
const PING: u8 = 8;
const PONG: u8 = 16;
/// A receiver closes a session with no streams after this many ping intervals
/// without receiving a byte. A live caller pings every interval.
const SERVER_IDLE_PINGS: u32 = 4;
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

/// Optional protocol features a caller may use on a negotiated session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerFeature {
    /// AppendEntries frames carrying more than one entry.
    MultiEntryAppend,
    IndexReclamation,
}

/// Counts bytes read, so liveness checks see progress inside large frames.
struct Counted<R> {
    inner: R,
    bytes: Arc<AtomicU64>,
}
impl<R: AsyncRead + Unpin> AsyncRead for Counted<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        let read = buf.filled().len() - before;
        if read != 0 {
            self.bytes.fetch_add(read as u64, Ordering::Relaxed);
        }
        polled
    }
}

/// Tracks when a byte counter last moved, sampled by a supervisor tick.
struct Progress {
    bytes: Arc<AtomicU64>,
    seen: u64,
    at: Instant,
}
impl Progress {
    fn new(bytes: Arc<AtomicU64>) -> Self {
        Self {
            seen: bytes.load(Ordering::Relaxed),
            bytes,
            at: Instant::now(),
        }
    }
    /// Time since the counter last moved, sampled now.
    fn silence(&mut self) -> Duration {
        let now = Instant::now();
        let bytes = self.bytes.load(Ordering::Relaxed);
        if bytes != self.seen {
            self.seen = bytes;
            self.at = now;
        }
        now - self.at
    }
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
    /// On the socket at this instant.
    Written(Instant),
    NotSent,
    Failed,
}
/// One frame. The charged parts stay owned until the frame is on the socket.
struct Outbound {
    head: Vec<u8>,
    parts: Vec<Part>,
    ack: Option<oneshot::Sender<WriteOutcome>>,
    /// A reply's handler lane slot. The writer releases it when it starts
    /// writing the frame, so the slot is free before the peer can read the
    /// reply and send its next request (a sequential caller is never refused
    /// by its own previous call).
    release: Option<OwnedSemaphorePermit>,
}
impl Outbound {
    fn control(stream_id: u64, flags: u8) -> Self {
        Self {
            head: StreamFrame {
                stream_id,
                flags,
                len: 0,
            }
            .encode()
            .to_vec(),
            parts: Vec::new(),
            ack: None,
            release: None,
        }
    }
    fn cancel(stream_id: u64) -> Self {
        Self::control(stream_id, CANCEL)
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

async fn stopped(stop: &mut Option<watch::Receiver<bool>>) {
    match stop {
        Some(stop) => {
            let _ = stop.wait_for(|stopped| *stopped).await;
        }
        None => std::future::pending().await,
    }
}

/// Serializes frames from all streams. A failed or stopped write closes the
/// session; the frame being written reports `Failed` (unknown outcome) and
/// queued frames report `NotSent`. `hold` (tests only) keeps the next frame
/// unwritten while it reads `true`; a stop meanwhile reports it `NotSent`.
async fn write_frames(
    mut stream: impl AsyncWrite + Unpin,
    mut queue: mpsc::Receiver<Outbound>,
    mut stop: Option<watch::Receiver<bool>>,
    mut hold: Option<watch::Receiver<bool>>,
    deadline: Duration,
) -> bool {
    let healthy = loop {
        let frame = tokio::select! {
            biased;
            _ = stopped(&mut stop) => break true,
            frame = queue.recv() => frame,
        };
        let Some(mut frame) = frame else {
            break true;
        };
        if let Some(hold) = hold.as_mut() {
            let held_until_stop = tokio::select! {
                biased;
                _ = stopped(&mut stop) => true,
                _ = hold.wait_for(|held| !*held) => false,
            };
            if held_until_stop {
                if let Some(ack) = frame.ack {
                    let _ = ack.send(WriteOutcome::NotSent);
                }
                break true;
            }
        }
        drop(frame.release.take());
        let written = tokio::select! {
            biased;
            written = timeout(deadline, write_frame(&mut stream, &frame)) => {
                matches!(written, Ok(Ok(())))
            }
            _ = stopped(&mut stop) => false,
        };
        if let Some(ack) = frame.ack {
            let _ = ack.send(if written {
                WriteOutcome::Written(Instant::now())
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

/// Resolves when the directory learned a revocation; never without one.
async fn trust_changed(changes: &mut Option<watch::Receiver<u64>>) {
    let closed = match changes.as_mut() {
        Some(receiver) => receiver.changed().await.is_err(),
        None => true,
    };
    if closed {
        *changes = None;
        std::future::pending::<()>().await;
    }
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
    index_reclamation: bool,
    outgoing: mpsc::Sender<Outbound>,
    waiters: Mutex<Waiters>,
    closed: watch::Sender<bool>,
    next: AtomicU64,
    reader: Mutex<Option<AbortHandle>>,
    /// Bytes received on this connection.
    received: Arc<AtomicU64>,
    /// A stream that waited this long for a reply with nothing at all
    /// received meanwhile marks the connection dead (the ping timeout).
    silent_limit: Duration,
}
impl ClientSession {
    fn supports(&self, feature: PeerFeature) -> bool {
        match feature {
            PeerFeature::MultiEntryAppend => true,
            PeerFeature::IndexReclamation => self.index_reclamation,
        }
    }

    fn start(
        stream: client::TlsStream<TcpStream>,
        enrollment: WorkerEnrollment,
        class: TrafficClass,
        common: &Common,
    ) -> Arc<Self> {
        let index_reclamation = stream.get_ref().1.alpn_protocol() == Some(INDEX_RECLAIM_ALPN);
        let (read, write) = tokio::io::split(stream);
        let (outgoing, queue) = mpsc::channel(WRITE_QUEUE);
        let (closed, stop) = watch::channel(false);
        let received = Arc::new(AtomicU64::new(0));
        let session = Arc::new(Self {
            identity: WorkerIdentity::from(&enrollment),
            index_reclamation,
            enrollment,
            outgoing,
            waiters: Mutex::new(Waiters {
                closed: false,
                streams: HashMap::new(),
            }),
            closed,
            next: AtomicU64::new(1),
            reader: Mutex::new(None),
            received: received.clone(),
            silent_limit: common.config.ping_timeout,
        });
        let deadline = common.config.deadline;
        #[cfg(test)]
        let hold = Some(common.hold_writes.subscribe());
        #[cfg(not(test))]
        let hold = None;
        tokio::spawn({
            let session = session.clone();
            async move {
                write_frames(write, queue, Some(stop), hold, deadline).await;
                session.close();
            }
        });
        let reader = tokio::spawn(read_replies(
            session.clone(),
            Counted {
                inner: read,
                bytes: received,
            },
            class,
            common.config.clone(),
            common.shared.clone(),
        ));
        *lock(&session.reader) = Some(reader.abort_handle());
        tokio::spawn(session.clone().supervise(
            reader,
            common.directory.clone(),
            common.config.ping_idle,
            common.config.ping_timeout,
        ));
        session
    }
    /// Close the session when its reader ends, the peer's incarnation leaves
    /// the directory, or a ping goes unanswered. A ping is sent after
    /// `ping_idle` of silence, or after `ping_timeout` while a stream waits.
    /// Its deadline starts once it reaches the socket, so a long write ahead
    /// of it is not mistaken for a dead peer.
    async fn supervise(
        self: Arc<Self>,
        mut reader: tokio::task::JoinHandle<()>,
        directory: Arc<dyn PeerDirectory>,
        ping_idle: Duration,
        ping_timeout: Duration,
    ) {
        let mut stop = self.closed.subscribe();
        let mut check = tokio::time::interval(
            DIRECTORY_CHECK
                .min(ping_timeout / 2)
                .max(Duration::from_millis(10)),
        );
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut progress = Progress::new(self.received.clone());
        let mut queued: Option<oneshot::Receiver<WriteOutcome>> = None;
        let mut written: Option<(Instant, u64)> = None;
        let mut changes = directory.trust_changes();
        loop {
            tokio::select! {
                _ = &mut reader => break,
                _ = stop.wait_for(|closed| *closed) => break,
                _ = trust_changed(&mut changes) => {
                    if !enrolled(&*directory, self.identity) {
                        break;
                    }
                }
                outcome = async { queued.as_mut().expect("ping queued").await }, if queued.is_some() => {
                    queued = None;
                    if !matches!(outcome, Ok(WriteOutcome::Written(_))) {
                        break;
                    }
                    written = Some((Instant::now(), self.received.load(Ordering::Relaxed)));
                }
                _ = check.tick() => {
                    if !enrolled(&*directory, self.identity) {
                        break;
                    }
                    let silence = progress.silence();
                    if let Some((at, mark)) = written {
                        if self.received.load(Ordering::Relaxed) != mark {
                            written = None;
                        } else if at.elapsed() >= ping_timeout {
                            tracing::debug!(peer=%self.identity.worker_id,
                                "peer session ping unanswered; closing");
                            break;
                        }
                    }
                    let limit = if self.waiting() { ping_timeout } else { ping_idle };
                    if queued.is_none() && written.is_none() && silence >= limit {
                        let (ack, outcome) = oneshot::channel();
                        let mut ping = Outbound::control(0, PING);
                        ping.ack = Some(ack);
                        if self.outgoing.try_send(ping).is_ok() {
                            queued = Some(outcome);
                        }
                    }
                }
            }
        }
        self.close();
    }
    fn waiting(&self) -> bool {
        !lock(&self.waiters).streams.is_empty()
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
            queued: false,
            written: None,
            outcome: None,
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
            release: None,
        };
        // Bytes received before the frame was queued: nothing received
        // since then means nothing received since it was written either.
        let mark = self.received.load(Ordering::Relaxed);
        match timeout_at(deadline, self.outgoing.send(frame)).await {
            Ok(Ok(())) => guard.queued = true,
            Ok(Err(_)) => return Err(CallError::NotSent),
            Err(_) => return Err(CallError::Failed(RequestError::Unreachable)),
        }
        // PERF2b: wait for the reply only. The writer's ack is read once
        // the reply (or its failure) is in, so the caller is not woken
        // separately when its frame reaches the socket.
        guard.written = Some((written, mark));
        let response = timeout_at(deadline, response).await;
        if let Ok(Ok(result @ Ok(_))) = response {
            // A reply means the request was written.
            *phase = RaftTransportPhase::ReceiveReply;
            guard.settled = true;
            return result.map_err(CallError::Failed);
        }
        let outcome = guard.write_outcome(deadline).await;
        if matches!(outcome, Some(WriteOutcome::Written(_))) {
            *phase = RaftTransportPhase::ReceiveReply;
        }
        match response {
            _ if outcome == Some(WriteOutcome::NotSent) => {
                guard.settled = true;
                guard.queued = false;
                Err(CallError::NotSent)
            }
            Ok(Ok(result)) => {
                guard.settled = true;
                result.map_err(CallError::Failed)
            }
            Ok(Err(_)) => {
                guard.settled = true;
                Err(CallError::Failed(RequestError::Unreachable))
            }
            // The guard decides whether the silent session is dead.
            Err(_) => Err(CallError::Failed(RequestError::Unreachable)),
        }
    }
}

/// A dropped or expired caller withdraws its waiter. Once its frame was
/// queued, the peer is asked to discard the reply; the handler still runs.
/// The `CANCEL` follows the request on the same ordered queue, so it never
/// overtakes it; if the request is never sent, neither is the `CANCEL`.
///
/// A reply timeout, after the stream waited at least the ping timeout with
/// nothing at all received on the connection since its request was written,
/// closes the session: later calls reconnect instead of waiting out their
/// own deadlines on a dead connection. Shorter waits (Raft's own RPC TTLs)
/// are left to the ping.
struct StreamGuard<'a> {
    session: &'a Arc<ClientSession>,
    stream_id: u64,
    queued: bool,
    /// The writer's ack for the queued request, and the bytes received
    /// before it was queued; the ack, once read, is kept in `outcome`.
    written: Option<(oneshot::Receiver<WriteOutcome>, u64)>,
    outcome: Option<WriteOutcome>,
    settled: bool,
}
impl StreamGuard<'_> {
    /// The writer's ack, waiting for it only if it has not come yet (a
    /// closed session fails the reply before its writer drains the queue).
    async fn write_outcome(&mut self, deadline: Instant) -> Option<WriteOutcome> {
        if self.outcome.is_none()
            && let Some((written, _)) = self.written.as_mut()
        {
            self.outcome = match written.try_recv() {
                Ok(outcome) => Some(outcome),
                Err(oneshot::error::TryRecvError::Empty) => timeout_at(deadline, written)
                    .await
                    .ok()
                    .and_then(Result::ok),
                Err(oneshot::error::TryRecvError::Closed) => None,
            };
        }
        self.outcome
    }
}
impl Drop for StreamGuard<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let pending = self.session.waiter(self.stream_id).is_some();
        if pending && self.queued {
            let _ = self
                .session
                .outgoing
                .try_send(Outbound::cancel(self.stream_id));
        }
        if self.outcome.is_none()
            && let Some((written, _)) = self.written.as_mut()
        {
            self.outcome = written.try_recv().ok();
        }
        if let Some(WriteOutcome::Written(at)) = self.outcome
            && let Some((_, mark)) = self.written
            && pending
            && at.elapsed() >= self.session.silent_limit
            && self.session.received.load(Ordering::Relaxed) == mark
        {
            tracing::debug!(peer=%self.session.identity.worker_id,
                "peer session silent past a reply deadline; closing");
            self.session.close();
        }
    }
}

async fn read_replies(
    session: Arc<ClientSession>,
    mut stream: Counted<ReadHalf<client::TlsStream<TcpStream>>>,
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
            // Liveness only: the bytes were already counted.
            PONG if frame.len == 0 && frame.stream_id == 0 => {}
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
    /// When the last connect under `connecting` failed. Callers that queued
    /// behind it share that failure instead of retrying one after another.
    failed: Mutex<Option<Instant>>,
}

/// Caller-side sessions, keyed by peer worker, incarnation and lane.
#[derive(Default)]
pub(super) struct PeerSessions {
    slots: Mutex<HashMap<(Uuid, Uuid, usize), Arc<Slot>>>,
    connections: AtomicU64,
}
impl Drop for PeerSessions {
    fn drop(&mut self) {
        self.close_all();
    }
}
impl PeerSessions {
    /// Whether the live session to this peer incarnation on `class` offers
    /// `feature`. One-shot peers support none.
    pub(super) fn supports(
        &self,
        peer: &WorkerEnrollment,
        class: TrafficClass,
        feature: PeerFeature,
    ) -> bool {
        let key = (peer.worker_id, peer.incarnation, class as usize);
        let Some(slot) = lock(&self.slots).get(&key).cloned() else {
            return false;
        };
        lock(&slot.current).as_ref().is_some_and(|session| {
            session.alive() && session.enrollment == *peer && session.supports(feature)
        })
    }
    /// Streams waiting for replies on this caller's sessions.
    #[cfg(test)]
    pub(super) fn waiting_for_test(&self) -> usize {
        lock(&self.slots)
            .values()
            .filter_map(|slot| lock(&slot.current).clone())
            .map(|session| lock(&session.waiters).streams.len())
            .sum()
    }
    pub(super) fn close_all(&self) {
        for slot in lock(&self.slots).values() {
            if let Some(session) = lock(&slot.current).take() {
                session.close();
            }
        }
    }
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
        deadline: Instant,
        mut probe_resolving: Option<&mut bool>,
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
            let queued = Instant::now();
            // Waiting on another caller's local connect is as indeterminate
            // as waiting on the resolver, for a liveness probe.
            if let Some(resolving) = probe_resolving.as_deref_mut() {
                *resolving = true;
            }
            let guard = timeout_at(deadline, slot.connecting.lock())
                .await
                .map_err(|_| RequestError::Unreachable)?;
            if let Some(resolving) = probe_resolving.as_deref_mut() {
                *resolving = false;
            }
            if let Some(session) = current() {
                return Ok(Route::Session(session));
            }
            if lock(&slot.failed).is_some_and(|failed| failed > queued) {
                return Err(RequestError::Unreachable);
            }
            Some(guard)
        };
        let attempt = deadline.min(Instant::now() + common.config.connect_timeout);
        let connected = timeout_at(
            attempt,
            connect(common, peer, class, probe_resolving.as_deref_mut(), phase),
        )
        .await
        .unwrap_or_else(|_| {
            if probe_resolving.as_deref().copied() == Some(true) {
                Err(unavailable().into())
            } else {
                Err(RequestError::Unreachable)
            }
        });
        let stream = match connected {
            Ok(stream) => {
                *lock(&slot.failed) = None;
                stream
            }
            Err(error) => {
                if !legacy {
                    *lock(&slot.failed) = Some(Instant::now());
                }
                return Err(error);
            }
        };
        self.connections.fetch_add(1, Ordering::Relaxed);
        if session_alpn(stream.get_ref().1.alpn_protocol()) {
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
        required: Option<PeerFeature>,
        mut probe_resolving: Option<&mut bool>,
        phase: &mut RaftTransportPhase,
    ) -> Result<RawReply, RequestError> {
        for attempt in 0..2 {
            let session = match self
                .route(
                    common,
                    peer,
                    class,
                    deadline,
                    probe_resolving.as_deref_mut(),
                    phase,
                )
                .await?
            {
                Route::Session(session) => session,
                Route::OneShot(_) if required.is_some() => return Err(RequestError::Unreachable),
                Route::OneShot(stream) => {
                    return one_shot(*stream, hello, header, sections, class, common, phase).await;
                }
            };
            if required.is_some_and(|feature| !session.supports(feature)) {
                return Err(RequestError::Unreachable);
            }
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
    tune_socket(&socket);
    *phase = RaftTransportPhase::Tls;
    let stream = common
        .connector
        .connect(name, socket)
        .await
        .map_err(|_| RequestError::Unreachable)?;
    let alpn = stream.get_ref().1.alpn_protocol();
    let supported = alpn == Some(ALPN) || common.sessions_enabled && session_alpn(alpn);
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

struct ServerSession {
    /// The sender incarnation every later prologue must carry.
    bound: Mutex<Option<WorkerIdentity>>,
    /// Streams in flight; `true` once the caller cancelled the reply.
    active: Mutex<HashMap<u64, Arc<AtomicBool>>>,
    /// The handshake's pending slot, held until the first verified request
    /// moves the session into its lane's pool.
    pending: Mutex<Option<OwnedSemaphorePermit>>,
    /// The session's lane and its slot in that lane's pool.
    lane: Mutex<Option<(TrafficClass, OwnedSemaphorePermit)>>,
    /// Set when the lane was full: the session ends after its typed reply.
    closing: AtomicBool,
    received: Arc<AtomicU64>,
}
enum Lane {
    Held,
    Full,
    Mismatch,
}
impl ServerSession {
    fn new(pending: OwnedSemaphorePermit) -> Self {
        Self {
            bound: Mutex::new(None),
            active: Mutex::new(HashMap::new()),
            pending: Mutex::new(Some(pending)),
            lane: Mutex::new(None),
            closing: AtomicBool::new(false),
            received: Arc::new(AtomicU64::new(0)),
        }
    }
    /// Callers keep one lane per connection. The first verified request takes
    /// a slot in that lane's pool and releases the pending slot.
    fn claim(&self, common: &Common, class: TrafficClass) -> Lane {
        let mut lane = lock(&self.lane);
        match &*lane {
            Some((held, _)) if *held == class => Lane::Held,
            Some(_) => Lane::Mismatch,
            None => match common.session_slots[class as usize]
                .clone()
                .try_acquire_owned()
            {
                Ok(slot) => {
                    *lane = Some((class, slot));
                    lock(&self.pending).take();
                    Lane::Held
                }
                Err(_) => Lane::Full,
            },
        }
    }
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
    pending: OwnedSemaphorePermit,
) {
    if *stop.borrow() {
        return;
    }
    let (read, write) = tokio::io::split(stream);
    let (outgoing, queue) = mpsc::channel(WRITE_QUEUE);
    let config = &transport.common.config;
    let deadline = config.deadline;
    let writer = tokio::spawn(write_frames(write, queue, None, None, deadline));
    // The writer closes the socket once every sender is gone. This one keeps it
    // open until the slots below are given back, even after the reader (which
    // owns the other sender) ends or is aborted.
    let hold_open = outgoing.clone();
    let state = Arc::new(ServerSession::new(pending));
    let mut reader = tokio::spawn(serve_frames(
        transport.clone(),
        Counted {
            inner: read,
            bytes: state.received.clone(),
        },
        outgoing,
        certificate,
        state.clone(),
    ));
    let mut check = tokio::time::interval(DIRECTORY_CHECK);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let started = Instant::now();
    let mut progress = Progress::new(state.received.clone());
    let mut changes = transport.common.directory.trust_changes();
    let read_finished = loop {
        tokio::select! {
            _ = &mut reader => break true,
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break false;
                }
            }
            _ = trust_changed(&mut changes) => {
                let bound = *lock(&state.bound);
                if bound.is_some_and(|peer| !enrolled(&*transport.common.directory, peer)) {
                    tracing::debug!(peer=?bound.map(|peer| peer.worker_id),
                        "closing a revoked peer's session");
                    transport.common.revoked_closes.fetch_add(1, Ordering::Relaxed);
                    break false;
                }
            }
            _ = check.tick() => {
                let bound = *lock(&state.bound);
                if bound.is_some_and(|peer| !enrolled(&*transport.common.directory, peer)) {
                    break false;
                }
                // Sessions that never name a lane, and idle sessions whose
                // caller stopped pinging, give their slot back.
                let silence = progress.silence();
                if lock(&state.active).is_empty()
                    && (lock(&state.lane).is_none() && started.elapsed() >= config.ping_idle
                        || silence >= config.ping_idle * SERVER_IDLE_PINGS)
                {
                    tracing::debug!(peer=?bound.map(|peer| peer.worker_id),
                        "closing an idle or unidentified peer session");
                    break false;
                }
            }
        }
    };
    if !read_finished {
        reader.abort();
        let _ = reader.await;
    }
    // No request is read any more. Give the slots back before the writer
    // closes the socket: a peer that sees the close and reconnects must find
    // them free, not wait for this task to be scheduled again (D10/C07,
    // CIfix1). A lane slot is kept while a stream still owes its reply.
    lock(&state.pending).take();
    if lock(&state.active).is_empty() {
        lock(&state.lane).take();
    }
    drop(hold_open);
    let _ = writer.await;
}

async fn serve_frames(
    transport: Arc<WorkerTransport>,
    mut stream: Counted<ReadHalf<server::TlsStream<TcpStream>>>,
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
                        if outgoing.send(frame).await.is_err()
                            || state.closing.load(Ordering::Acquire)
                        {
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
            PING if frame.len == 0 => {
                if outgoing
                    .send(Outbound::control(frame.stream_id, PONG))
                    .await
                    .is_err()
                {
                    return;
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
        release: None,
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
            .map_err(|_| {
                common.refusals.handler(class);
                overloaded("worker handler lane full")
            })?;
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
        if common.directory.revoked(session.remote()) {
            return Err(super::revoked_peer());
        }
        let reply = endpoint.handle(session, header, sections).await?;
        validate_reply(&common.config, class, &reply)?;
        Ok((reply, handler))
    })
    .await;
    let (frame, mut handler) = match handled {
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
    #[cfg(test)]
    if cancelled.load(Ordering::Acquire) {
        common.cancelled_replies.fetch_add(1, Ordering::AcqRel);
    }
    if !cancelled.load(Ordering::Acquire) {
        let mut frame = frame.unwrap_or_else(|| Outbound::cancel(stream_id));
        let (ack, written) = oneshot::channel();
        frame.ack = Some(ack);
        // The handler lane stays held while the reply waits for the socket.
        // The writer frees it as it starts writing the reply, before the peer
        // can see it: freed after the write, the slot could still be held
        // when the peer's next request arrives (D10/O05, CIfix1).
        frame.release = handler.take();
        if outgoing.send(frame).await.is_ok() {
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
        stream: &mut Counted<ReadHalf<server::TlsStream<TcpStream>>>,
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
                || !super::audience(&*common.directory, &hello, group, &common.local)
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
        match state.claim(common, class) {
            Lane::Held => {}
            Lane::Mismatch => return Ok(Received::Close),
            Lane::Full => {
                drain(stream, rest).await?;
                state.closing.store(true, Ordering::Release);
                return Ok(Received::Reject {
                    nonce: hello.nonce,
                    class,
                    error: overloaded("worker session capacity full"),
                });
            }
        }
        if sections_total > common.config.payload_bytes[class as usize] {
            drain(stream, rest).await?;
            return Ok(Received::Reject {
                nonce: hello.nonce,
                class,
                error: malformed("worker receive plan exceeds class limits"),
            });
        }
        // Definite rejections before any execution get a typed reply: the
        // caller knows the request was not applied.
        let Ok(preflight) = common.preflight[class as usize].clone().try_acquire_owned() else {
            drain(stream, rest).await?;
            return Ok(Received::Reject {
                nonce: hello.nonce,
                class,
                error: overloaded("worker preflight lane full"),
            });
        };
        let byte_class = class.byte_class();
        let mut header = match common.shared.allocate_bytes(byte_class, hello.header_len) {
            Ok(header) => header,
            Err(error) => {
                drain(stream, rest).await?;
                return Ok(Received::Reject {
                    nonce: hello.nonce,
                    class,
                    error,
                });
            }
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
            release: None,
        };
        let mut out = Vec::new();
        write_frame(&mut out, &frame).await.unwrap();
        let mut expected = vec![1, 2, 3];
        expected.extend_from_slice(b"header");
        expected.extend_from_slice(&7u64.to_be_bytes());
        expected.extend_from_slice(&[0; 8]);
        assert_eq!(out, expected);
    }

    /// D10/O05 (CIfix1): a reply's handler lane slot is free before any byte
    /// of the reply reaches the socket, so a peer that answers a reply with
    /// its next request always finds the slot free. Released after the write,
    /// a sequential caller on a loaded host was refused with "worker handler
    /// lane full" (`d10_o05_one_session_per_lane_carries_many_calls`, Linux CI).
    #[tokio::test]
    async fn d10_o05_reply_frees_its_handler_slot_before_its_bytes_are_written() {
        struct Probe {
            lane: Arc<Semaphore>,
            free: Arc<Mutex<Vec<usize>>>,
        }
        impl AsyncWrite for Probe {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<io::Result<usize>> {
                self.free
                    .lock()
                    .unwrap()
                    .push(self.lane.available_permits());
                std::task::Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }
        let lane = Arc::new(Semaphore::new(1));
        let free = Arc::new(Mutex::new(Vec::new()));
        let (queue, frames) = mpsc::channel(4);
        let (ack, written) = oneshot::channel();
        let mut frame = Outbound::cancel(7);
        frame.ack = Some(ack);
        frame.release = Some(lane.clone().try_acquire_owned().unwrap());
        queue.send(frame).await.unwrap();
        drop(queue);
        let probe = Probe {
            lane: lane.clone(),
            free: free.clone(),
        };
        assert!(write_frames(probe, frames, None, None, Duration::from_secs(5)).await);
        assert!(matches!(written.await.unwrap(), WriteOutcome::Written(_)));
        let free = free.lock().unwrap().clone();
        assert!(!free.is_empty());
        assert!(
            free.iter().all(|&n| n == 1),
            "the handler slot was held while its reply was written: {free:?}"
        );
    }
}
