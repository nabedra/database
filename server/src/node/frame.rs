//! Bounded frame-v1 I/O, with no extra magic or version bytes. The initial
//! CONTROL DTO carries the version. This codec does not validate job/result
//! ordering, JSON, counts, hashes or payload semantics. Bounded I/O alone is
//! insufficient to validate a result; callers must perform those checks too.
//!
//! Dropping a polled, unfinished operation or encountering I/O failure poisons
//! the codec permanently. Drop the codec and abandon that transport attempt.
//! Neither codec exposes its underlying stream until successful writer finish.
use super::job::MAX_CONTROL_BYTES;
use std::fmt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_DATA_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Control = 1,
    Data = 2,
    PartMeta = 3,
    PayloadEnd = 4,
    StreamEnd = 5,
}

impl FrameKind {
    fn decode(byte: u8) -> Result<Self, FrameError> {
        match byte {
            1 => Ok(Self::Control),
            2 => Ok(Self::Data),
            3 => Ok(Self::PartMeta),
            4 => Ok(Self::PayloadEnd),
            5 => Ok(Self::StreamEnd),
            _ => Err(FrameError::UnknownKind),
        }
    }

    fn validate(self, len: usize) -> Result<(), FrameError> {
        let valid = match self {
            Self::Control | Self::PartMeta => (1..=MAX_CONTROL_BYTES).contains(&len),
            Self::Data => (1..=MAX_DATA_BYTES).contains(&len),
            Self::PayloadEnd | Self::StreamEnd => len == 0,
        };
        if valid {
            Ok(())
        } else {
            Err(FrameError::InvalidLength)
        }
    }
}

/// Fixed local errors never retain transport errors or peer-supplied text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    UnknownKind,
    InvalidLength,
    Truncated,
    TrailingBytes,
    Io,
    Poisoned,
    Completed,
    UseFinish,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownKind => "unknown frame kind",
            Self::InvalidLength => "invalid frame length",
            Self::Truncated => "truncated frame stream",
            Self::TrailingBytes => "trailing frame stream bytes",
            Self::Io => "frame stream I/O failed",
            Self::Poisoned => "frame codec is poisoned",
            Self::Completed => "frame stream is completed",
            Self::UseFinish => "stream end requires finish",
        })
    }
}
impl std::error::Error for FrameError {}

#[derive(Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy)]
enum State {
    Ready,
    Poisoned,
    Completed,
}
impl State {
    fn ready(self) -> Result<(), FrameError> {
        match self {
            Self::Ready => Ok(()),
            Self::Poisoned => Err(FrameError::Poisoned),
            Self::Completed => Err(FrameError::Completed),
        }
    }
}

pub struct FrameReader<R> {
    inner: R,
    state: State,
}
impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            state: State::Ready,
        }
    }

    /// Returns one bounded payload. The codec retains no prior frames; callers
    /// must consume/drop them instead of collecting a whole result in memory.
    /// STREAM_END is consumed internally and returns `None` only after actual
    /// body EOF. A bare EOF, even between frames, is truncation. Every subsequent
    /// operation after `None` returns Completed, rather than another `None`.
    pub async fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        self.state.ready()?;
        // Set before the first await: dropping this future cannot restore Ready.
        self.state = State::Poisoned;
        let mut header = [0; 5];
        self.inner
            .read_exact(&mut header)
            .await
            .map_err(read_error)?;
        let kind = FrameKind::decode(header[0])?;
        let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        kind.validate(len)?;
        if kind == FrameKind::StreamEnd {
            let mut trailing = [0];
            if self
                .inner
                .read(&mut trailing)
                .await
                .map_err(|_| FrameError::Io)?
                != 0
            {
                return Err(FrameError::TrailingBytes);
            }
            self.state = State::Completed;
            return Ok(None);
        }
        // Allocation and payload read happen only after kind/length validation.
        let mut payload = vec![0; len];
        self.inner
            .read_exact(&mut payload)
            .await
            .map_err(read_error)?;
        self.state = State::Ready;
        Ok(Some(Frame { kind, payload }))
    }
}

fn read_error(error: std::io::Error) -> FrameError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        FrameError::Truncated
    } else {
        FrameError::Io
    }
}

pub struct FrameWriter<W> {
    inner: W,
    state: State,
}
impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            state: State::Ready,
        }
    }

    /// Validate before emitting any bytes. Invalid arguments leave Ready intact.
    /// Borrows a single bounded payload without copying it or aggregating data.
    /// Use finish for STREAM_END. No implicit finish occurs on drop.
    pub async fn write_frame(&mut self, kind: FrameKind, payload: &[u8]) -> Result<(), FrameError> {
        self.state.ready()?;
        kind.validate(payload.len())?;
        if kind == FrameKind::StreamEnd {
            return Err(FrameError::UseFinish);
        }
        self.state = State::Poisoned;
        self.emit(kind, payload).await?;
        self.state = State::Ready;
        Ok(())
    }

    /// Emit STREAM_END and flush. This is local completion, not proof of peer
    /// receipt or body EOF. Does NOT shutdown the underlying stream. The owner
    /// must provide actual EOF (e.g. end its HTTP body, or take the completed
    /// writer's stream and shutdown its write half). Further operations reject.
    /// Cancellation/failure during terminator write or flush leaves Poisoned.
    pub async fn finish(&mut self) -> Result<(), FrameError> {
        self.state.ready()?;
        self.state = State::Poisoned;
        self.emit(FrameKind::StreamEnd, &[]).await?;
        self.inner.flush().await.map_err(|_| FrameError::Io)?;
        self.state = State::Completed;
        Ok(())
    }

    /// Recover the stream only after successful finish, for owner-managed EOF.
    /// Calling this early consumes/drops the writer and returns a fixed error.
    pub fn into_inner(self) -> Result<W, FrameError> {
        match self.state {
            State::Completed => Ok(self.inner),
            State::Poisoned => Err(FrameError::Poisoned),
            State::Ready => Err(FrameError::UseFinish),
        }
    }

    async fn emit(&mut self, kind: FrameKind, payload: &[u8]) -> Result<(), FrameError> {
        let len = (payload.len() as u32).to_be_bytes();
        let header = [kind as u8, len[0], len[1], len[2], len[3]];
        self.inner
            .write_all(&header)
            .await
            .map_err(|_| FrameError::Io)?;
        self.inner
            .write_all(payload)
            .await
            .map_err(|_| FrameError::Io)
    }
}
