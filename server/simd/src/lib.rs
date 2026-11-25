//! Vector distance kernels. The only crate with `unsafe` in the search path.
//!
//! A [`Kernels`] value names one instruction set level and can only be built after the CPU was
//! seen to have it, so every kernel is a safe call. [`Kernels::detect`] picks the best level;
//! [`Kernels::at`] forces one, for tests and measurements.
//!
//! Accuracy of the f32 kernels, for finite inputs of `n` values: against the f64 sums in [`wide`],
//! [`Kernels::dot`] is off by at most `(n / 16 + 32) * 2^-24 * sum|a_i * b_i| + n * 2^-149`, and
//! [`Kernels::l2_sq`] and [`Kernels::norm_sq`] likewise against their own terms, which are all
//! positive. That holds while the sum of the terms' magnitudes is below f32::MAX / 2. Past it a
//! result may be infinite or NaN, but it is never a finite number where the true one does not fit
//! f32: use [`wide`] then. The integer kernels and [`Kernels::hadamard`] return the same bits at
//! every level.
#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::sync::OnceLock;

#[cfg(target_arch = "aarch64")]
mod neon;
mod scalar;
#[cfg(target_arch = "x86_64")]
mod x86;

/// f64 reference sums. They cannot overflow or underflow on finite f32 inputs.
pub mod wide {
    /// Panics when the lengths differ.
    pub fn dot(a: &[f32], b: &[f32]) -> f64 {
        crate::same_len(a, b);
        crate::scalar::dot(a, b)
    }

    /// Panics when the lengths differ.
    pub fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
        crate::same_len(a, b);
        crate::scalar::l2_sq(a, b)
    }

    pub fn norm_sq(a: &[f32]) -> f64 {
        crate::scalar::dot(a, a)
    }
}

/// Bit slices of a query in [`Kernels::bit_distances`].
pub const SLICES: usize = 4;

/// An instruction set level. Every level is named on every target; [`Kernels::at`] tells whether
/// this CPU has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Scalar,
    /// AVX2 and FMA.
    Avx2,
    /// AVX-512 F, BW, VL and VPOPCNTDQ, next to AVX2 and FMA.
    Avx512,
    /// NEON, without the dot product extension.
    Neon,
}

impl Level {
    /// Slowest first.
    pub const ALL: [Level; 4] = [Level::Scalar, Level::Avx2, Level::Avx512, Level::Neon];

    fn detected(self) -> bool {
        match self {
            Level::Scalar => true,
            #[cfg(target_arch = "x86_64")]
            Level::Avx2 => is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma"),
            #[cfg(target_arch = "x86_64")]
            Level::Avx512 => {
                Level::Avx2.detected()
                    && is_x86_feature_detected!("avx512f")
                    && is_x86_feature_detected!("avx512bw")
                    && is_x86_feature_detected!("avx512vl")
                    && is_x86_feature_detected!("avx512vpopcntdq")
            }
            #[cfg(target_arch = "aarch64")]
            Level::Neon => std::arch::is_aarch64_feature_detected!("neon"),
            _ => false,
        }
    }
}

/// The kernels of one level. Invariant: `self.0.detected()` held when the value was built, and a
/// CPU does not lose features.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kernels(Level);

/// Calls a kernel at the level of `$k`, or runs `$scalar`.
macro_rules! dispatch {
    ($k:expr, $name:ident $(::<$c:tt>)? ($($arg:expr),*), $scalar:expr) => {
        match $k.0 {
            #[cfg(target_arch = "x86_64")]
            // SAFETY: `Kernels` holds `Avx2` only after `detected` saw AVX2 and FMA.
            Level::Avx2 => unsafe { x86::avx2::$name$(::<$c>)?($($arg),*) },
            #[cfg(target_arch = "x86_64")]
            // SAFETY: `Kernels` holds `Avx512` only after `detected` saw AVX-512 F, BW, VL and
            // VPOPCNTDQ.
            Level::Avx512 => unsafe { x86::avx512::$name$(::<$c>)?($($arg),*) },
            #[cfg(target_arch = "aarch64")]
            // SAFETY: `Kernels` holds `Neon` only after `detected` saw NEON.
            Level::Neon => unsafe { neon::$name$(::<$c>)?($($arg),*) },
            _ => $scalar,
        }
    };
}

impl Kernels {
    /// The best level of this CPU, detected once.
    #[inline]
    pub fn detect() -> Self {
        static BEST: OnceLock<Kernels> = OnceLock::new();
        *BEST.get_or_init(|| {
            let best = Level::ALL.into_iter().rev().find(|level| level.detected());
            Kernels(best.unwrap_or(Level::Scalar))
        })
    }

    /// The kernels of `level`, or `None` when this CPU does not have it.
    pub fn at(level: Level) -> Option<Self> {
        level.detected().then_some(Self(level))
    }

    pub fn level(self) -> Level {
        self.0
    }

    /// `sum a_i * b_i`. Panics when the lengths differ.
    #[inline]
    pub fn dot(self, a: &[f32], b: &[f32]) -> f32 {
        same_len(a, b);
        dispatch!(self, sum::<false>(a, b), scalar::dot(a, b) as f32)
    }

    /// `sum (a_i - b_i)^2`. Panics when the lengths differ.
    #[inline]
    pub fn l2_sq(self, a: &[f32], b: &[f32]) -> f32 {
        same_len(a, b);
        dispatch!(self, sum::<true>(a, b), scalar::l2_sq(a, b) as f32)
    }

    /// `sum a_i^2`.
    #[inline]
    pub fn norm_sq(self, a: &[f32]) -> f32 {
        self.dot(a, a)
    }

    /// Weighted Hamming distance of each 1-bit code to a bit-sliced 4-bit query:
    /// `out[e] = sum_s 2^s * popcount(code_e ^ slice_s)`.
    ///
    /// `query` holds [`SLICES`] slices of `w` words, least significant first, and `codes` holds
    /// `out.len()` codes of `w` words. Panics when the sizes do not match, or when a code has more
    /// than 2^20 words (the distance then fits u32).
    pub fn bit_distances(self, codes: &[u64], query: &[u64], out: &mut [u32]) {
        let words = query.len() / SLICES;
        assert!(
            query.len().is_multiple_of(SLICES) && words <= 1 << 20,
            "bit-sliced query size"
        );
        assert_eq!(
            out.len().checked_mul(words),
            Some(codes.len()),
            "code words"
        );
        if words == 0 {
            return out.fill(0);
        }
        dispatch!(
            self,
            bit_distances(codes, query, out),
            scalar::bit_distances(codes, query, out)
        )
    }

    /// `sum (16 * bit_i + ext_i) * query_i` over one 5-bit code: `bits` holds the top bits, 64 per
    /// word from the lowest, and `ext` the rest, two per byte with the even dimension in the low
    /// half.
    ///
    /// Panics unless `query` has 64 values per word of `bits` and two per byte of `ext`, or when it
    /// has more than 2^18 (the sum then fits i32).
    pub fn code_dot(self, bits: &[u64], ext: &[u8], query: &[i8]) -> i32 {
        assert!(query.len() <= 1 << 18, "query size");
        assert_eq!(bits.len().checked_mul(64), Some(query.len()), "bit words");
        assert_eq!(
            ext.len().checked_mul(2),
            Some(query.len()),
            "extension bytes"
        );
        dispatch!(
            self,
            code_dot(bits, ext, query),
            scalar::code_dot(bits, ext, query)
        )
    }

    /// The unnormalized Walsh-Hadamard transform in place, in natural order: `x_i` becomes
    /// `sum_j (-1)^popcount(i & j) * x_j`. Panics unless the length is a power of two.
    pub fn hadamard(self, x: &mut [f32]) {
        assert!(x.len().is_power_of_two(), "transform size");
        dispatch!(self, hadamard(x), scalar::hadamard(x))
    }
}

/// [`Kernels::dot`] at the best level.
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    Kernels::detect().dot(a, b)
}

/// [`Kernels::l2_sq`] at the best level.
#[inline]
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    Kernels::detect().l2_sq(a, b)
}

/// [`Kernels::norm_sq`] at the best level.
#[inline]
pub fn norm_sq(a: &[f32]) -> f32 {
    Kernels::detect().norm_sq(a)
}

#[inline]
fn same_len(a: &[f32], b: &[f32]) {
    assert_eq!(a.len(), b.len(), "vector lengths");
}
