//! Rotation, 1-bit and 4-bit direction codes, estimators and their error bounds.
//!
//! A vector is split into its exact length and its direction. The direction is rotated by a seeded
//! [`Rotation`] and coded relative to a rotated centroid direction in a [`CodeBlock`]. A
//! [`QueryCode`] estimates the cosine between two directions, and [`bounds`] turns that one
//! estimate and both lengths into an interval of any operation.
//!
//! **Stated bound.** Over rotations, the true cosine of one pair lies within [`Estimate::err`] of
//! [`Estimate::cos`] with probability at least [`COVERAGE`], in the 1-bit and in the 5-bit stage,
//! and above `cos + err` or below `cos - err` with half the rest each. Never prune an exact query
//! with it.
//!
//! **Zero vectors** have no direction and never reach this module: [`CodeBlock::push`], [`bounds`]
//! and the cosine of [`exact`] panic on one. [`direction`] tells them apart.

mod code;
mod rotation;
#[cfg(test)]
mod tests;

pub use code::{CodeBlock, QueryCode};
pub use rotation::Rotation;

use nabedra_simd::{Kernels, wide};

/// The largest dimension of a rotation or a code.
pub const MAX_DIM: usize = 1 << 16;
/// Padded dimensions are multiples of this.
pub const CODE_STEP: usize = 64;
/// Standard deviations in [`Estimate::err`].
pub const EPSILON: f32 = 1.9;
/// The two-sided probability of [`EPSILON`] deviations.
pub const COVERAGE: f64 = 0.94;
/// Absolute rounding error of an estimated cosine, and relative slack of [`bounds`] for rounded
/// lengths and for the rounding of [`exact`].
const ROUNDING: f32 = 8e-6;

/// Estimated cosine and the bound of its error on each side.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Estimate {
    pub cos: f32,
    pub err: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Cosine,
    Dot,
    /// The L2 distance, not squared. The smallest is best.
    L2,
}

/// Score interval `(low, high)` of an operation from a cosine estimate and both lengths.
///
/// It contains the score whenever the true cosine is inside `est.cos ± est.err`. Scores beyond f32
/// count as the closest finite number, as in [`exact`]. Panics unless both lengths are positive
/// normal numbers.
pub fn bounds(op: Op, est: Estimate, len_o: f32, len_q: f32) -> (f32, f32) {
    assert!(
        is_length(len_o) && is_length(len_q),
        "vector lengths {len_o} and {len_q}"
    );
    let (o, q) = (f64::from(len_o), f64::from(len_q));
    let (cos, err, slack) = (f64::from(est.cos), f64::from(est.err), f64::from(ROUNDING));
    let (low, high) = ((cos - err).clamp(-1.0, 1.0), (cos + err).clamp(-1.0, 1.0));
    match op {
        Op::Cosine => (
            saturate((low - slack).max(-1.0)),
            saturate((high + slack).min(1.0)),
        ),
        Op::Dot => (
            saturate(o * q * (low - slack)),
            saturate(o * q * (high + slack)),
        ),
        Op::L2 => {
            // |o - q|^2, as the part the angle cannot change plus the part it decides.
            let l2 = |cos: f64| ((o - q).powi(2) + 2.0 * o * q * (1.0 - cos)).max(0.0);
            let slack = slack * (o + q).powi(2);
            (
                saturate((l2(high) - slack).max(0.0).sqrt()),
                saturate((l2(low) + slack).sqrt()),
            )
        }
    }
}

/// The score of an operation from raw vectors, at f32 precision. Scores beyond f32 become the
/// closest finite number.
///
/// Panics when the lengths differ, and for the cosine of a zero vector.
pub fn exact(op: Op, a: &[f32], b: &[f32]) -> f32 {
    let kernels = Kernels::detect();
    match op {
        Op::Dot => {
            let dot = kernels.dot(a, b);
            if dot.is_finite() {
                dot
            } else {
                saturate(wide::dot(a, b))
            }
        }
        Op::L2 => {
            let squared = kernels.l2_sq(a, b);
            if is_accurate(squared) {
                squared.sqrt()
            } else {
                saturate(wide::l2_sq(a, b).sqrt())
            }
        }
        Op::Cosine => {
            let (norm_a, norm_b) = (kernels.norm_sq(a), kernels.norm_sq(b));
            let cosine = if is_accurate(norm_a) && is_accurate(norm_b) {
                f64::from(kernels.dot(a, b)) / (f64::from(norm_a) * f64::from(norm_b)).sqrt()
            } else {
                let (norm_a, norm_b) = (wide::norm_sq(a), wide::norm_sq(b));
                assert!(norm_a > 0.0 && norm_b > 0.0, "cosine of a zero vector");
                wide::dot(a, b) / (norm_a.sqrt() * norm_b.sqrt())
            };
            cosine.clamp(-1.0, 1.0) as f32
        }
    }
}

/// Writes the direction of `v` to `out` and returns its exact length, or `None` for a vector that
/// the codes cannot hold: zero, or so short or long that its length is no positive normal f32.
/// Panics when the lengths differ.
pub fn direction(v: &[f32], out: &mut [f32]) -> Option<f32> {
    assert_eq!(v.len(), out.len(), "vector lengths");
    let len = wide::norm_sq(v).sqrt();
    if !is_length(len as f32) {
        return None;
    }
    for (out, &x) in out.iter_mut().zip(v) {
        *out = (f64::from(x) / len) as f32;
    }
    Some(len as f32)
}

fn is_length(len: f32) -> bool {
    len.is_normal() && len > 0.0
}

/// Whether an f32 sum of squares lost nothing to underflow or overflow.
fn is_accurate(squared: f32) -> bool {
    (1e-30..=1e30).contains(&squared)
}

/// The closest finite f32. It keeps the order of numbers, so an interval around a score stays one.
fn saturate(x: f64) -> f32 {
    (x as f32).clamp(f32::MIN, f32::MAX)
}
