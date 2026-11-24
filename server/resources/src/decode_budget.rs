//! Allocation limits for bounded OLTP JSON decoding.

use nabedra_protocol::protocol::{self, ErrorCode, Outcome, ProtocolError};

pub const MAX_FIXED_COMMAND_BYTES: usize = 4 * protocol::MAX_BODY_BYTES;

/// A conservative pre-decode upper bound for JSON values and container/Arc
/// metadata. It scans without constructing strings, maps, arrays or numbers.
/// Two raw-byte copies cover RawValue ownership plus temporary canonical checks.
#[inline]
pub fn json_decode_budget(bytes: &[u8]) -> Result<usize, ProtocolError> {
    protocol::json_input(bytes, MAX_FIXED_COMMAND_BYTES).map_err(|_| invalid_json())?;
    allocation_budget(bytes)
}
#[inline]
pub fn request_decode_budget(bytes: &[u8]) -> Result<usize, ProtocolError> {
    protocol::request_json_input(bytes).map_err(|_| invalid_json())?;
    allocation_budget(bytes)
}
fn allocation_budget(bytes: &[u8]) -> Result<usize, ProtocolError> {
    let mut in_string = false;
    let mut escaped = false;
    let mut structural = 0usize;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
        } else if *byte == b'"' {
            in_string = true;
        } else if matches!(*byte, b'{' | b'}' | b'[' | b']' | b',' | b':') {
            structural = structural.checked_add(1).ok_or_else(invalid_json)?;
        }
    }
    bytes
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(structural.checked_mul(256)?))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(invalid_json)
}
fn invalid_json() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "invalid or unbudgeted fixed JSON",
        Outcome::NotApplied,
    )
}
