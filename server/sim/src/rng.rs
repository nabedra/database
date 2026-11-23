//! Seeded streams. Every random choice of a run comes from a stream derived from its seed,
//! so a run replays from the seed alone.

use uuid::Uuid;

/// SplitMix64 (Steele, Lea and Flood). The same generator as the Events simulator's copy
/// and `nabedra_check::rng`; this crate reuses `nabedra-rt`'s instead of a third copy.
pub use nabedra_rt::sim::SplitMix64;

use crate::trace::fnv;

/// The draws the Events simulator's SplitMix64 had beyond `next_u64` and `below`, with the
/// same arithmetic, so a ported harness draws the same values.
pub trait Draw {
    /// Uniform in `[lo, hi]`. Panics when `lo > hi`.
    fn range(&mut self, lo: u64, hi: u64) -> u64;
    /// True with probability `per_mille / 1000`.
    fn chance(&mut self, per_mille: u64) -> bool;
}

impl Draw for SplitMix64 {
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "range({lo}, {hi})");
        lo + self.below(hi - lo + 1)
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

/// The seed of the stream named `name` in a run seeded with `seed`. Different names give
/// independent streams, so adding draws to one stream never shifts another.
pub fn stream_seed(seed: u64, name: &str) -> u64 {
    SplitMix64::new(seed ^ fnv(name.as_bytes())).next_u64()
}

/// The stream named `name` in a run seeded with `seed`.
pub fn stream(seed: u64, name: &str) -> SplitMix64 {
    SplitMix64::new(stream_seed(seed, name))
}

/// A version 4 UUID from two draws, laid out as the Events simulator and
/// `nabedra_rt::ids::uuid_v4` under `sim` lay it out (little-endian words).
pub fn uuid_from(rng: &mut SplitMix64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&rng.next_u64().to_le_bytes());
    bytes[8..].copy_from_slice(&rng.next_u64().to_le_bytes());
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}
