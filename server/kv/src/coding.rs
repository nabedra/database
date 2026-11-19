//! Little helpers for the on-disk encodings: LEB128 varints and fixed-width integers.

/// Appends `value` as an unsigned LEB128 varint (at most 5 bytes).
pub fn put_varint32(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Appends `value` as an unsigned LEB128 varint (at most 10 bytes).
pub fn put_varint64(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Decodes a varint32 from the front of `input`, returning it and the bytes consumed.
/// `None` for truncated input or more than 32 bits.
pub fn get_varint32(input: &[u8]) -> Option<(u32, usize)> {
    let (value, used) = get_varint64(input)?;
    if used > 5 {
        return None;
    }
    Some((u32::try_from(value).ok()?, used))
}

/// Decodes a varint64 from the front of `input`, returning it and the bytes consumed.
pub fn get_varint64(input: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, &byte) in input.iter().enumerate().take(10) {
        let bits = u64::from(byte & 0x7F);
        if i == 9 && byte > 1 {
            return None;
        }
        value |= bits << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Appends a varint32 length followed by `bytes`.
pub fn put_length_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint32(
        out,
        u32::try_from(bytes.len()).expect("slice longer than u32::MAX"),
    );
    out.extend_from_slice(bytes);
}

/// Decodes a length-prefixed slice from the front of `input`, returning the slice and
/// the bytes consumed.
pub fn get_length_prefixed(input: &[u8]) -> Option<(&[u8], usize)> {
    let (len, used) = get_varint32(input)?;
    let end = used.checked_add(len as usize)?;
    Some((input.get(used..end)?, end))
}

pub fn read_u32_le(input: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(input.get(at..at + 4)?.try_into().ok()?))
}

pub fn read_u64_le(input: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(input.get(at..at + 8)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip_and_reject_bad_input() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            assert_eq!(get_varint64(&buf), Some((v, buf.len())));
            assert_eq!(get_varint64(&buf[..buf.len() - 1]), None);
        }
        let mut buf = Vec::new();
        put_varint64(&mut buf, u32::MAX as u64 + 1);
        assert_eq!(get_varint32(&buf), None);
        assert_eq!(get_varint64(&[0xFF; 11]), None);
        let mut buf = Vec::new();
        put_length_prefixed(&mut buf, b"hello");
        assert_eq!(get_length_prefixed(&buf), Some((&b"hello"[..], 6)));
        assert_eq!(get_length_prefixed(&buf[..4]), None);
    }
}
