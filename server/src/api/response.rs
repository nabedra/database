//! Synchronous response encoding. HTTP status/header construction belongs to the caller.
use crate::protocol::{
    ErrorCode, ErrorResponse, MAX_BODY_BYTES, Outcome, ProtocolError, SuccessResponse,
};
use serde::Serialize;
use std::io::{self, Write};
use uuid::Uuid;

/// Compact UTF-8 JSON and its HTTP status, ready for transport ownership.
pub struct EncodedResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// The mutation flag affects only encoding failures: execution may have committed.
pub fn encode_success<T: Serialize + ?Sized>(
    request_id: Uuid,
    data: &T,
    operation_is_mutation: bool,
) -> EncodedResponse {
    match to_body(&SuccessResponse { request_id, data }) {
        Ok(body) => EncodedResponse { status: 200, body },
        Err(code) => fallback(
            Some(request_id),
            code,
            if operation_is_mutation {
                Outcome::Unknown
            } else {
                Outcome::NotApplied
            },
        ),
    }
}

/// Use `None` until a valid request UUID is known. Preserve engine certainty.
pub fn encode_error(request_id: Option<Uuid>, mut error: ProtocolError) -> EncodedResponse {
    if error.code == ErrorCode::SplitPublicationPending {
        error.code = ErrorCode::StorageUnavailable;
    }
    let status = error.code.status_code();
    let outcome = error.outcome;
    match to_body(&ErrorResponse { request_id, error }) {
        Ok(body) => EncodedResponse { status, body },
        Err(code) => fallback(request_id, code, outcome),
    }
}

fn to_body(value: &impl Serialize) -> Result<Vec<u8>, ErrorCode> {
    let mut writer = BoundedBody {
        bytes: Vec::new(),
        overflowed: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    // Remember overflow independently of the Serde error, including a custom
    // serializer that replaces or ignores a returned writer error.
    if writer.overflowed {
        Err(ErrorCode::LimitExceeded)
    } else if result.is_err() {
        Err(ErrorCode::InternalError)
    } else {
        Ok(writer.bytes)
    }
}

fn fallback(request_id: Option<Uuid>, code: ErrorCode, outcome: Outcome) -> EncodedResponse {
    let message = match code {
        ErrorCode::LimitExceeded => "response body exceeds 8 MiB",
        _ => "response encoding failed",
    };
    let envelope = ErrorResponse {
        request_id,
        error: ProtocolError::new(code, message, outcome),
    };
    // Only protocol derives, a UUID, enums, and fixed short strings are encoded
    // here. This envelope is under 256 bytes and has no fallible user serializer.
    let body = to_body(&envelope).expect("fixed protocol fallback must encode within 8 MiB");
    EncodedResponse {
        status: code.status_code(),
        body,
    }
}

struct BoundedBody {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl Write for BoundedBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.overflowed || bytes.len() > MAX_BODY_BYTES - self.bytes.len() {
            self.overflowed = true;
            return Err(io::Error::other("response body limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
