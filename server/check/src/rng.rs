//! Deterministic pseudo-random numbers. No OS randomness, so every run replays from its seed.

/// SplitMix64 (Steele, Lea and Flood, 2014): one 64-bit word of state, full period, fast.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

/// The Weyl sequence increment SplitMix64 adds to its state on every draw.
pub const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        mix(self.state)
    }
}

/// The SplitMix64 output function: a bijective avalanche of one word.
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
