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
//! **Zero vectors** have no direction. [`direction`] returns `None` for them, and the cosine of
//! [`exact`] and [`score`] too. [`CodeBlock::push`] and [`bounds`] take the lengths that
//! [`direction`] returned and panic on others.

mod code;
mod rotation;
#[cfg(test)]
mod tests;

pub use code::{CodeBlock, QueryCode};
pub use rotation::Rotation;

use nabedra_simd::{Kernels, relative_error, wide};
use rotation::{ROUNDS, transform_size};

/// The largest dimension of a rotation or a code.
pub const MAX_DIM: usize = 1 << 16;
/// Padded dimensions are multiples of this.
pub const CODE_STEP: usize = 64;
/// Standard deviations in [`Estimate::err`].
pub const EPSILON: f32 = 1.9;
/// The two-sided probability of [`EPSILON`] deviations.
pub const COVERAGE: f64 = 0.94;
/// 2^-24, the relative error of one f32 rounding.
const UNIT: f64 = 1.0 / (1u32 << 24) as f64;

/// How far rounding may move a score of vectors of `dim` values, as a difference of their cosine.
///
/// The kernels err by at most `e = relative_error(dim)`: a dot product by `e * |a| |b|`, and a
/// squared length or distance by `e` of itself. So the cosine of [`exact`], a dot product over the
/// root of two squared lengths, errs by at most `2 e` plus its own rounding, a dot product by less,
/// and a squared L2 distance by `e * (|a| + |b|)^2`. The other 8 units cover the rounded lengths (2),
/// the rounded estimate (1), the score as f32 (2), the interval as f32 (2) and second-order terms.
/// It grows up to 1,024 values only.
pub fn slack(dim: usize) -> f64 {
    2.0 * relative_error(dim) + 8.0 * UNIT
}

/// Products of f32 values that underflow lose at most this in a dot product of `dim` values.
fn underflow(dim: usize) -> f64 {
    dim as f64 * 2f64.powi(-149)
}

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
/// It contains the true score and that of [`exact`] for vectors of up to [`MAX_DIM`] values, whenever
/// the true cosine is inside `est.cos ± est.err`. Scores beyond f32 count as the closest finite
/// number, as in [`exact`]. Panics unless both lengths are positive normal numbers.
pub fn bounds(op: Op, est: Estimate, len_o: f32, len_q: f32) -> (f32, f32) {
    assert!(
        is_length(len_o) && is_length(len_q),
        "vector lengths {len_o} and {len_q}"
    );
    let (o, q) = (f64::from(len_o), f64::from(len_q));
    let (cos, err, slack) = (f64::from(est.cos), f64::from(est.err), slack(MAX_DIM));
    let (low, high) = ((cos - err).clamp(-1.0, 1.0), (cos + err).clamp(-1.0, 1.0));
    let (low, high) = match op {
        Op::Cosine => ((low - slack).max(-1.0), (high + slack).min(1.0)),
        Op::Dot => {
            let lost = underflow(MAX_DIM);
            (o * q * (low - slack) - lost, o * q * (high + slack) + lost)
        }
        Op::L2 => {
            // |o - q|^2, as the part the angle cannot change plus the part it decides.
            let l2 = |cos: f64| ((o - q).powi(2) + 2.0 * o * q * (1.0 - cos)).max(0.0);
            let slack = slack * (o + q).powi(2);
            ((l2(high) - slack).max(0.0).sqrt(), (l2(low) + slack).sqrt())
        }
    };
    (saturate(low), saturate(high))
}

/// The score of an operation from raw vectors by the f32 kernels, the same on every CPU and within
/// [`exact_bounds`] of [`score`]. Scores beyond f32 become the closest finite number. `None` for
/// the cosine of a zero vector. Panics when the lengths differ.
pub fn exact(op: Op, a: &[f32], b: &[f32]) -> Option<f32> {
    let kernels = Kernels::detect();
    let fast = match op {
        Op::Dot => Some(kernels.dot(a, b)).filter(|dot| dot.is_finite()),
        Op::L2 => Some(kernels.l2_sq(a, b))
            .filter(|squared| is_accurate(*squared))
            .map(f32::sqrt),
        Op::Cosine => {
            let (norm_a, norm_b) = (kernels.norm_sq(a), kernels.norm_sq(b));
            (is_accurate(norm_a) && is_accurate(norm_b)).then(|| {
                let norms = f64::from(norm_a) * f64::from(norm_b);
                (f64::from(kernels.dot(a, b)) / norms.sqrt()).clamp(-1.0, 1.0) as f32
            })
        }
    };
    fast.or_else(|| score(op, a, b))
}

/// The score that orders final results: f64 sums in one order, rounded to f32 once. Scores beyond
/// f32 become the closest finite number. `None` for the cosine of a zero vector. Panics when the
/// lengths differ.
pub fn score(op: Op, a: &[f32], b: &[f32]) -> Option<f32> {
    let score = match op {
        Op::Dot => wide::dot(a, b),
        Op::L2 => wide::l2_sq(a, b).sqrt(),
        Op::Cosine => {
            let lengths = wide::norm_sq(a).sqrt() * wide::norm_sq(b).sqrt();
            if lengths == 0.0 {
                return None;
            }
            (wide::dot(a, b) / lengths).clamp(-1.0, 1.0)
        }
    };
    Some(saturate(score))
}

/// The interval of [`score`], given the result of [`exact`] for vectors of `dim` values with these
/// lengths. A row may be dropped without its [`score`] only when this interval cannot reach the top.
/// Lengths are [`direction`]'s, or 0 for a zero vector; score other vectors with [`score`].
pub fn exact_bounds(op: Op, exact: f32, dim: usize, len_a: f32, len_b: f32) -> (f32, f32) {
    let (x, slack) = (f64::from(exact), slack(dim));
    let wide = match op {
        Op::Cosine => slack,
        Op::Dot => slack * f64::from(len_a) * f64::from(len_b) + underflow(dim),
        Op::L2 => slack * x,
    };
    match op {
        Op::Cosine => (
            saturate((x - wide).max(-1.0)),
            saturate((x + wide).min(1.0)),
        ),
        Op::Dot | Op::L2 => (saturate(x - wide), saturate(x + wide)),
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
