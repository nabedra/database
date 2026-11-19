//! Bounded RESP2 framing for cache clients.
use std::io::{self, IoSlice};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

const CRLF: &[u8] = b"\r\n";

#[derive(Clone, Copy)]
pub struct FrameLimits {
    pub max_frame_bytes: usize,
    pub max_arguments: usize,
    pub max_bulk_bytes: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 2 << 20,
            max_arguments: 128,
            max_bulk_bytes: 1 << 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolError {
    ExpectedArray,
    ExpectedBulk,
    InvalidLength,
    InvalidTerminator,
    TooManyArguments,
    BulkTooLarge,
    FrameTooLarge,
    Truncated,
}

impl ProtocolError {
    pub fn response(self) -> Response {
        Response::error(match self {
            Self::ExpectedArray => b"ERR Protocol error: expected a RESP2 request array",
            Self::ExpectedBulk => b"ERR Protocol error: expected a bulk string argument",
            Self::InvalidLength => b"ERR Protocol error: invalid length",
            Self::InvalidTerminator => b"ERR Protocol error: invalid bulk terminator",
            Self::TooManyArguments => b"ERR Protocol error: too many arguments",
            Self::BulkTooLarge => b"ERR Protocol error: bulk argument too large",
            Self::FrameTooLarge => b"ERR Protocol error: request frame too large",
            Self::Truncated => b"ERR Protocol error: truncated request",
        })
    }
}

#[derive(Default)]
enum Phase {
    #[default]
    Array,
    BulkLength,
    BulkBody(usize),
}

/// Reads one frame at a time without scanning an incomplete payload repeatedly.
/// The caller caps input growth and queued commands. Close after a framing error
/// because the next trustworthy frame boundary is unknown.
pub struct Decoder {
    limits: FrameLimits,
    phase: Phase,
    offset: usize,
    argument_count: usize,
    arguments: Vec<Bytes>,
}

impl Decoder {
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            limits,
            phase: Phase::default(),
            offset: 0,
            argument_count: 0,
            arguments: Vec::new(),
        }
    }

    pub fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        loop {
            let frame = &input[..input.len().min(self.limits.max_frame_bytes)];
            let pending = &frame[self.offset..];
            match self.phase {
                Phase::Array => {
                    let Some((count, consumed)) = parse_length(
                        pending,
                        b'*',
                        self.limits.max_arguments,
                        ProtocolError::TooManyArguments,
                    )?
                    else {
                        return self.incomplete(input.len());
                    };
                    if count == 0 {
                        return Err(ProtocolError::InvalidLength);
                    }
                    // Each bulk argument needs at least "$0\r\n\r\n".
                    if count > (self.limits.max_frame_bytes - consumed) / 6 {
                        return Err(ProtocolError::FrameTooLarge);
                    }
                    self.argument_count = count;
                    self.offset += consumed;
                    self.phase = Phase::BulkLength;
                }
                Phase::BulkLength => {
                    let Some((length, consumed)) = parse_length(
                        pending,
                        b'$',
                        self.limits.max_bulk_bytes,
                        ProtocolError::BulkTooLarge,
                    )?
                    else {
                        return self.incomplete(input.len());
                    };
                    self.offset += consumed;
                    let available = self.limits.max_frame_bytes - self.offset;
                    if available < CRLF.len() || length > available - CRLF.len() {
                        return Err(ProtocolError::FrameTooLarge);
                    }
                    self.phase = Phase::BulkBody(length);
                }
                Phase::BulkBody(length) => {
                    let end = length + CRLF.len();
                    let Some(argument) = pending.get(..end) else {
                        return self.incomplete(input.len());
                    };
                    if &argument[length..] != CRLF {
                        return Err(ProtocolError::InvalidTerminator);
                    }
                    // Stored values must not keep other request arguments or a
                    // connection's read buffer alive.
                    self.arguments
                        .push(Bytes::copy_from_slice(&argument[..length]));
                    self.offset += end;
                    if self.arguments.len() == self.argument_count {
                        input.advance(self.offset);
                        self.offset = 0;
                        self.phase = Phase::Array;
                        return Ok(Some(std::mem::take(&mut self.arguments)));
                    }
                    self.phase = Phase::BulkLength;
                }
            }
        }
    }

    pub fn eof(&self, input: &BytesMut) -> Result<(), ProtocolError> {
        if input.is_empty() && matches!(self.phase, Phase::Array) {
            Ok(())
        } else {
            Err(ProtocolError::Truncated)
        }
    }

    fn incomplete(&self, length: usize) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        if length >= self.limits.max_frame_bytes {
            Err(ProtocolError::FrameTooLarge)
        } else {
            Ok(None)
        }
    }
}

/// RESP lengths are nonnegative decimal numbers. Reject impossible declared
/// sizes before waiting for their bodies, including unterminated huge numbers.
fn parse_length(
    input: &[u8],
    prefix: u8,
    limit: usize,
    limit_error: ProtocolError,
) -> Result<Option<(usize, usize)>, ProtocolError> {
    let Some(&first) = input.first() else {
        return Ok(None);
    };
    if first != prefix {
        return Err(if prefix == b'*' {
            ProtocolError::ExpectedArray
        } else {
            ProtocolError::ExpectedBulk
        });
    }

    let mut count = 0usize;
    for (index, &byte) in input.iter().enumerate().skip(1) {
        match byte {
            b'\r' if index > 1 => {
                return match input.get(index + 1) {
                    Some(b'\n') => Ok(Some((count, index + 2))),
                    None => Ok(None),
                    _ => Err(ProtocolError::InvalidLength),
                };
            }
            b'0'..=b'9' if index == 1 || input[1] != b'0' => {
                count = count
                    .checked_mul(10)
                    .and_then(|count| count.checked_add(usize::from(byte - b'0')))
                    .filter(|&count| count <= limit)
                    .ok_or(limit_error)?;
            }
            _ => return Err(ProtocolError::InvalidLength),
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Vec<Response>),
    NullArray,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputTooLarge;

impl Response {
    pub fn ok() -> Self {
        Self::simple(b"OK")
    }

    pub fn simple(message: &'static [u8]) -> Self {
        Self::Simple(Bytes::from_static(message))
    }

    pub fn error(message: &'static [u8]) -> Self {
        Self::Error(Bytes::from_static(message))
    }

    pub fn bulk(data: &'static [u8]) -> Self {
        Self::Bulk(Some(Bytes::from_static(data)))
    }

    pub fn encoded_len(&self) -> Option<usize> {
        match self {
            Self::Simple(message) | Self::Error(message) => message.len().checked_add(3),
            Self::Integer(value) => {
                Some(decimal_len(value.unsigned_abs()) + usize::from(*value < 0) + 3)
            }
            Self::Bulk(None) | Self::NullArray => Some(5),
            Self::Bulk(Some(data)) => bulk_len(data.len()),
            Self::Array(items) => items
                .iter()
                .try_fold(decimal_len(items.len() as u64) + 3, |length, item| {
                    length.checked_add(item.encoded_len()?)
                }),
        }
    }

    pub fn encode(&self, max_bytes: usize) -> Result<Bytes, OutputTooLarge> {
        let length = self.checked_len(max_bytes)?;
        if let Some(bytes) = self.static_encoding() {
            return Ok(Bytes::from_static(bytes));
        }
        let mut output = BytesMut::with_capacity(length);
        self.write_into(&mut output);
        Ok(output.freeze())
    }

    /// Write large plaintext bulk replies directly from the value's allocation.
    /// Streams without vectored writes receive one contiguous reply.
    pub async fn write_to<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
        max_bytes: usize,
    ) -> io::Result<()> {
        let output_limit_error = |_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cache reply exceeds output limit",
            )
        };
        if let Self::Bulk(Some(data)) = self
            && data.len() >= 4096
            && stream.is_write_vectored()
        {
            self.checked_len(max_bytes).map_err(output_limit_error)?;
            let mut header = [0; 24];
            header[0] = b'$';
            let mut digits = [0; 20];
            let number = decimal_bytes(data.len() as u64, &mut digits);
            let number_end = 1 + number.len();
            header[1..number_end].copy_from_slice(number);
            header[number_end..number_end + 2].copy_from_slice(CRLF);
            let mut slices = [
                IoSlice::new(&header[..number_end + 2]),
                IoSlice::new(data),
                IoSlice::new(CRLF),
            ];
            let mut pending = slices.as_mut_slice();
            while !pending.is_empty() {
                let written = stream.write_vectored(pending).await?;
                if written == 0 {
                    return Err(io::Error::from(io::ErrorKind::WriteZero));
                }
                IoSlice::advance_slices(&mut pending, written);
            }
            return Ok(());
        }
        let encoded = self.encode(max_bytes).map_err(output_limit_error)?;
        stream.write_all(&encoded).await
    }

    fn checked_len(&self, max_bytes: usize) -> Result<usize, OutputTooLarge> {
        self.encoded_len()
            .filter(|length| *length <= max_bytes)
            .ok_or(OutputTooLarge)
    }

    fn static_encoding(&self) -> Option<&'static [u8]> {
        match self {
            Self::Simple(bytes) if bytes.as_ref() == b"OK" => Some(b"+OK\r\n"),
            Self::Simple(bytes) if bytes.as_ref() == b"PONG" => Some(b"+PONG\r\n"),
            Self::Integer(0) => Some(b":0\r\n"),
            Self::Integer(1) => Some(b":1\r\n"),
            Self::Integer(-1) => Some(b":-1\r\n"),
            Self::Integer(-2) => Some(b":-2\r\n"),
            Self::Bulk(None) => Some(b"$-1\r\n"),
            Self::Bulk(Some(bytes)) if bytes.is_empty() => Some(b"$0\r\n\r\n"),
            Self::Array(items) if items.is_empty() => Some(b"*0\r\n"),
            Self::NullArray => Some(b"*-1\r\n"),
            _ => None,
        }
    }

    fn write_into(&self, out: &mut BytesMut) {
        if let Some(bytes) = self.static_encoding() {
            out.extend_from_slice(bytes);
            return;
        }
        match self {
            Self::Simple(message) => write_line(out, b'+', message),
            Self::Error(message) => write_line(out, b'-', message),
            Self::Integer(value) => {
                out.put_u8(b':');
                if *value < 0 {
                    out.put_u8(b'-');
                }
                write_number(out, value.unsigned_abs());
            }
            Self::Bulk(Some(data)) => {
                out.put_u8(b'$');
                write_number(out, data.len() as u64);
                out.extend_from_slice(data);
                out.extend_from_slice(CRLF);
            }
            Self::Array(items) => {
                out.put_u8(b'*');
                write_number(out, items.len() as u64);
                items.iter().for_each(|item| item.write_into(out));
            }
            Self::Bulk(None) | Self::NullArray => unreachable!("these have static encodings"),
        }
    }
}

/// Size of a bulk reply including its header and terminal CRLF.
pub fn bulk_len(payload_len: usize) -> Option<usize> {
    payload_len.checked_add(decimal_len(payload_len as u64) + 5)
}

fn write_line(out: &mut BytesMut, prefix: u8, message: &[u8]) {
    out.put_u8(prefix);
    // Never allow line-oriented fields to inject a second reply.
    for &byte in message {
        out.put_u8(if matches!(byte, b'\r' | b'\n') {
            b' '
        } else {
            byte
        });
    }
    out.extend_from_slice(CRLF);
}

fn decimal_len(value: u64) -> usize {
    value
        .checked_ilog10()
        .map_or(1, |digits| digits as usize + 1)
}

fn decimal_bytes(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
    let mut offset = digits.len();
    loop {
        offset -= 1;
        digits[offset] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &digits[offset..];
        }
    }
}

fn write_number(out: &mut BytesMut, value: u64) {
    out.extend_from_slice(decimal_bytes(value, &mut [0; 20]));
    out.extend_from_slice(CRLF);
}

#[cfg(test)]
mod tests;
