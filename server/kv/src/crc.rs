//! CRC32C (Castagnoli). Owned by task NS1: hardware paths (SSE4.2, ARMv8 CRC) chosen at
//! run time, a slicing-by-8 software fallback, and known-answer tests.

/// CRC32C of `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    extend(0, data)
}

/// Continues a CRC32C: `extend(crc32c(a), b) == crc32c(a ‖ b)`.
pub fn extend(crc: u32, data: &[u8]) -> u32 {
    // Bitwise reference implementation; NS1 replaces it with the fast paths.
    let mut c = !crc;
    for &b in data {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = if c & 1 != 0 {
                (c >> 1) ^ 0x82F6_3B78
            } else {
                c >> 1
            };
        }
    }
    !c
}

const MASK_DELTA: u32 = 0xA282_EAD8;

/// Masks a CRC before it is stored next to the data it covers, so a CRC of bytes that
/// themselves contain CRCs stays well distributed (the LevelDB/RocksDB convention).
pub fn mask(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Inverse of [`mask`].
pub fn unmask(masked: u32) -> u32 {
    masked.wrapping_sub(MASK_DELTA).rotate_left(15)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_answers() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(extend(crc32c(b"1234"), b"56789"), crc32c(b"123456789"));
        assert_eq!(unmask(mask(0xDEAD_BEEF)), 0xDEAD_BEEF);
    }
}
