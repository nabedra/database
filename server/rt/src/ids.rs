//! The id and non-crypto randomness seam.
//!
//! Product builds forward to `uuid`'s random generator. Under `sim` both functions draw
//! from the current node's seeded stream, so ids are deterministic per seed and differ
//! between nodes.
//!
//! [`random_u64`] is for jitter, sampling and similar choices. It is NEVER for keys,
//! tokens, nonces or anything secret: credentials and encryption use `ring`.

/// A random (version 4) UUID. Seeded per node under `sim`.
#[inline]
pub fn uuid_v4() -> uuid::Uuid {
    #[cfg(feature = "sim")]
    {
        crate::sim::uuid_v4()
    }
    #[cfg(not(feature = "sim"))]
    {
        uuid::Uuid::new_v4()
    }
}

/// A non-crypto random `u64` (jitter, sampling). Seeded per node under `sim`.
///
/// Product builds take 64 of the 122 random bits of a version 4 UUID, whose generator
/// uses the operating system's random source, so no extra crate is needed.
#[inline]
pub fn random_u64() -> u64 {
    #[cfg(feature = "sim")]
    {
        crate::sim::random_u64()
    }
    #[cfg(not(feature = "sim"))]
    {
        random_bits(uuid::Uuid::new_v4())
    }
}

/// The low 62 bits of a version 4 UUID are random (the 2 above them are the variant),
/// and so are the 2 lowest bits of its high half (the version sits at bits 12 to 15).
#[cfg_attr(feature = "sim", allow(dead_code))]
#[inline]
fn random_bits(id: uuid::Uuid) -> u64 {
    let (high, low) = id.as_u64_pair();
    (low & ((1 << 62) - 1)) | ((high & 0b11) << 62)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_bits_takes_only_random_uuid_bits() {
        // A version 4, variant 1 UUID with every random bit clear: nothing leaks through.
        let fixed = uuid::Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0000);
        assert_eq!(random_bits(fixed), 0);
        // Every random bit set: all 64 output bits are set.
        let full = uuid::Uuid::from_u128(0xffff_ffff_ffff_4fff_bfff_ffff_ffff_ffff);
        assert_eq!(random_bits(full), u64::MAX);
    }
}
