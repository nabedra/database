use nabedra_simd::Kernels;

use super::{CODE_STEP, MAX_DIM};

const ROUNDS: usize = 4;

/// A seeded random rotation of `dim` values into [`Rotation::padded`] ones.
///
/// Each round flips signs, applies a Walsh-Hadamard transform to every block of the largest power
/// of two that divides the padded length, and, when there are several blocks, deals the values of
/// each block over all of them. Every step is orthogonal, so lengths and inner products keep within
/// a relative 2e-6 of rounding. Only additions, subtractions and products in a fixed order, so a
/// seed gives the same bits on every CPU. Stored codes depend on that: never change the result for
/// a seed.
pub struct Rotation {
    dim: usize,
    padded: usize,
    /// Values per transform.
    block: usize,
    /// Per round, the sign of each value times the scale that makes the transform orthogonal.
    factors: Vec<f32>,
}

impl Rotation {
    /// Panics unless `dim` is in `1..=MAX_DIM`.
    pub fn new(dim: usize, seed: u64) -> Self {
        assert!((1..=MAX_DIM).contains(&dim), "dimension {dim}");
        let padded = dim.next_multiple_of(CODE_STEP);
        let block = 1 << padded.trailing_zeros();
        let scale = (block as f32).sqrt().recip();
        let mut state = seed;
        let mut signs = || {
            let word = split_mix(&mut state);
            (0..64).map(move |i| [scale, -scale][(word >> i & 1) as usize])
        };
        let factors = (0..ROUNDS * padded / 64).flat_map(|_| signs()).collect();
        Self {
            dim,
            padded,
            block,
            factors,
        }
    }

    /// The length of a rotated vector: `dim` rounded up to a multiple of [`CODE_STEP`].
    pub fn padded(&self) -> usize {
        self.padded
    }

    /// Writes the rotation of `v`, followed by zeros up to the padded length, to `out`.
    ///
    /// `v` must be finite with a length of at most 1e30; directions are. Panics unless `v` has
    /// `dim` values and `out` has `padded`.
    pub fn apply(&self, v: &[f32], out: &mut [f32]) {
        assert_eq!(v.len(), self.dim, "rotation input");
        assert_eq!(out.len(), self.padded, "rotation output");
        let kernels = Kernels::detect();
        let blocks = self.padded / self.block;
        let mut dealt = if blocks > 1 {
            vec![0.0; self.padded]
        } else {
            Vec::new()
        };
        out[..self.dim].copy_from_slice(v);
        out[self.dim..].fill(0.0);
        for factors in self.factors.chunks_exact(self.padded) {
            for (x, factor) in out.iter_mut().zip(factors) {
                *x *= factor;
            }
            for block in out.chunks_exact_mut(self.block) {
                kernels.hadamard(block);
            }
            if blocks > 1 {
                for (r, block) in out.chunks_exact(self.block).enumerate() {
                    for (x, hand) in block.iter().zip(dealt.chunks_exact_mut(blocks)) {
                        hand[r] = *x;
                    }
                }
                out.copy_from_slice(&dealt);
            }
        }
    }
}

/// SplitMix64 (Steele, Lea and Flood, 2014).
fn split_mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let z = (*state ^ (*state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
