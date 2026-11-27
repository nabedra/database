use nabedra_simd::{Kernels, SLICES};

use super::{CODE_STEP, EPSILON, Estimate, MAX_DIM, ROUNDS, transform_size};

/// Where the top level of the 5-bit code ends, in multiples of the residual's root mean square.
/// 3 suits the Gaussian values a rotation gives; the others serve residuals with other tails.
const CLIPS: [f64; 13] = [
    1.0, 1.4, 1.75, 2.0, 2.3, 2.6, 2.8, 3.0, 3.2, 3.5, 4.0, 5.0, 8.0,
];
/// The fraction of the golden ratio: its multiples modulo 1 are spread evenly.
const GOLDEN: u32 = 0x9e37_79b9;

/// Codes of directions relative to a centroid direction, for one partition: 1 bit plus a 4-bit
/// extension per padded dimension. `refine` and `scan` need a query prepared against the same
/// centroid.
///
/// With `r = dir - centroid`, the 1-bit code is `x = sign(r)`, and the 5-bit code is the vector `y`
/// of odd multiples of 1/2 up to 31/2 that points closest to `r` among the scales in [`CLIPS`]. As
/// in RaBitQ (arXiv 2405.12497) and its extension (2409.09913), `r . q` is estimated by
/// `(|r|^2 / (x . r)) * (x . q)`. Under Haar rotations its error has mean 0 and a deviation of at
/// most `|r| |q| sqrt(1 / a^2 - 1) / sqrt(d - 1)`, where `a` is the cosine between `x` and `r`; for
/// the rotation here that is measured (see [`super::COVERAGE`]).
///
/// Everything stored, and every estimate, is the same on every CPU: only integer kernels and scalar
/// arithmetic.
#[derive(Clone)]
pub struct CodeBlock {
    padded: usize,
    /// `padded / 64` words per entry.
    bits: Vec<u64>,
    /// `padded / 2` bytes per entry.
    ext: Vec<u8>,
    /// `|r|^2` per entry, or 0 when nothing is left to estimate.
    r2: Vec<f32>,
    /// `|r|^2 / (x . r)` per entry.
    scale1: Vec<f32>,
    factors: Vec<Factors>,
}

/// What only the stages after the 1-bit scan read.
#[derive(Clone, Copy)]
struct Factors {
    /// Exact length of the vector.
    len: f32,
    /// `|r|^2 / (y . r)`.
    scale5: f32,
    /// `|r|^2 (1 / a^2 - 1) / (d - 1)` of the 5-bit code.
    var5: f32,
}

impl CodeBlock {
    /// Panics unless `padded` is a multiple of [`CODE_STEP`] in `1..=MAX_DIM`.
    pub fn new(padded: usize) -> Self {
        assert_padded(padded);
        Self {
            padded,
            bits: Vec::new(),
            ext: Vec::new(),
            r2: Vec::new(),
            scale1: Vec::new(),
            factors: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.factors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.factors.is_empty()
    }

    /// The exact length of entry `i`, as pushed. Panics when there is no entry `i`.
    pub fn length(&self, i: usize) -> f32 {
        self.factors[i].len
    }

    /// Appends the codes of a vector. `dir` and `centroid` are rotated unit vectors, and `len` is
    /// the exact length of the vector.
    ///
    /// Panics unless both have the padded length and are unit within 1e-3, and `len` is a positive
    /// normal number: zero-length vectors have no direction and never reach this module. Panics on
    /// a block that keeps its extensions elsewhere.
    pub fn push(&mut self, dir: &[f32], centroid: &[f32], len: f32) {
        assert_eq!(self.ext.len(), self.len() * self.ext_bytes(), "a detached block");
        let ext = self.encode(dir, centroid, len);
        self.ext.extend_from_slice(&ext);
    }

    /// [`CodeBlock::push`] without keeping the 4-bit extension, which it returns: the caller keeps
    /// it and hands it to [`QueryCode::refine_with`]. A block holds every extension or none, so this
    /// panics on a block that holds some.
    pub fn push_detached(&mut self, dir: &[f32], centroid: &[f32], len: f32) -> Vec<u8> {
        assert!(self.ext.is_empty(), "a block holds every extension or none");
        self.encode(dir, centroid, len)
    }

    /// Appends everything but the extension, which it returns.
    fn encode(&mut self, dir: &[f32], centroid: &[f32], len: f32) -> Vec<u8> {
        assert_direction(dir, self.padded);
        assert_direction(centroid, self.padded);
        assert!(super::is_length(len), "vector length {len}");
        let residual: Vec<f32> = dir.iter().zip(centroid).map(|(o, c)| o - c).collect();
        let r2 = sum(&residual, |r| r * r);
        let mut factors = Factors {
            len,
            scale5: 0.0,
            var5: 0.0,
        };
        let (mut scale, mut scale1) = (0.0, 0.0);
        if r2 > 0.0 {
            scale = best_scale(&residual, r2);
            let code = Fit::new(&residual, scale);
            let length2 = r2 / code.cosine2(r2);
            factors.scale5 = (r2 / code.dot) as f32;
            factors.var5 = ((length2 - r2).max(0.0) / (self.padded as f64 - 1.0)) as f32;
            scale1 = (r2 / sum(&residual, f64::abs)) as f32;
        }
        self.r2.push(r2 as f32);
        self.scale1.push(scale1);
        self.factors.push(factors);
        self.bits.extend(residual.chunks_exact(64).map(|values| {
            let ones = values.iter().map(|&r| u64::from(r >= 0.0));
            ones.rev().fold(0, |word, bit| word << 1 | bit)
        }));
        let nibble = |r: f32| match level(r.abs(), scale) as u8 {
            level if r >= 0.0 => level,
            level => 15 - level,
        };
        let pairs = residual.chunks_exact(2);
        pairs.map(|p| nibble(p[0]) | nibble(p[1]) << 4).collect()
    }

    /// Replaces entry `i` with the last one, which it removes. Panics when there is no entry `i`.
    pub fn swap_remove(&mut self, i: usize) {
        let inline = !self.ext.is_empty();
        self.factors.swap_remove(i);
        self.r2.swap_remove(i);
        self.scale1.swap_remove(i);
        swap_remove_row(&mut self.bits, i, self.padded / 64);
        if inline {
            swap_remove_row(&mut self.ext, i, self.padded / 2);
        }
    }

    /// Bytes of the 4-bit extension of one entry.
    pub fn ext_bytes(&self) -> usize {
        self.padded / 2
    }

    /// Bytes of codes and factors the block holds, extensions included when it keeps them.
    pub fn held(&self) -> usize {
        let per = 2 * size_of::<f32>() + size_of::<Factors>();
        self.bits.len() * size_of::<u64>() + self.ext.len() + self.len() * per
    }

    fn bits(&self, i: usize) -> &[u64] {
        let words = self.padded / 64;
        &self.bits[i * words..][..words]
    }

    fn ext(&self, i: usize) -> &[u8] {
        let bytes = self.padded / 2;
        &self.ext[i * bytes..][..bytes]
    }
}

fn swap_remove_row<T: Copy>(rows: &mut Vec<T>, i: usize, width: usize) {
    let last = rows.len() - width;
    rows.copy_within(last.., i * width);
    rows.truncate(last);
}

/// The level of a magnitude in the 5-bit code, 0 to 15: the code value is `level + 1/2`.
#[inline]
fn level(magnitude: f32, scale: f32) -> i32 {
    (magnitude * scale).min(15.0) as i32
}

/// How well the 5-bit code of one scale fits a residual `r`.
pub(super) struct Fit {
    /// `y . r`.
    dot: f64,
    /// `|y|^2`.
    norm2: f64,
}

impl Fit {
    pub(super) fn new(residual: &[f32], scale: f32) -> Self {
        let y = |r: f64| f64::from(level(r.abs() as f32, scale)) + 0.5;
        Self {
            dot: sum(residual, |r| y(r) * r.abs()),
            norm2: sum(residual, |r| y(r) * y(r)),
        }
    }

    /// The squared cosine between `y` and `r`.
    pub(super) fn cosine2(&self, r2: f64) -> f64 {
        self.dot * self.dot / (self.norm2 * r2)
    }
}

/// The scale whose code points closest to `residual`, of squared length `r2`. Scale 0 gives the
/// direction of the 1-bit code, so the 5-bit one is never worse.
pub(super) fn best_scale(residual: &[f32], r2: f64) -> f32 {
    let rms = (r2 / residual.len() as f64).sqrt();
    let mut best = (0.0, Fit::new(residual, 0.0).cosine2(r2));
    for clip in CLIPS {
        let scale = (16.0 / (clip * rms)) as f32;
        let cosine2 = Fit::new(residual, scale).cosine2(r2);
        if cosine2 > best.1 {
            best = (scale, cosine2);
        }
    }
    best.0
}

/// The sum of `f` over `values`, in lanes that the compiler vectorizes. In f64, so its rounding
/// does not count.
#[inline]
fn sum(values: &[f32], f: impl Fn(f64) -> f64) -> f64 {
    debug_assert!(
        values.len().is_multiple_of(8),
        "only whole lanes: {}",
        values.len()
    );
    let mut lanes = [0.0; 8];
    for values in values.as_chunks::<8>().0 {
        for (lane, &value) in lanes.iter_mut().zip(values) {
            *lane += f(f64::from(value));
        }
    }
    lanes.iter().sum()
}

/// A query prepared against one centroid.
///
/// With `q = query_dir - centroid`, the cosine of the unit vectors is
/// `1 - |r - q|^2 / 2 = 1 - (|r|^2 + |q|^2) / 2 + r . q`. Only `r . q` is estimated. `q` is rounded
/// to 16 levels for the 1-bit stage and to 255 for the 5-bit one, and the length of that rounding
/// error enters the bound.
pub struct QueryCode {
    padded: usize,
    /// `|q|^2`.
    q2: f32,
    /// Bit `s` of each 4-bit level `u`, least significant slice first. `u` stands for
    /// `(u - 7.5) * step4`. A fixed dither decides between rounding up and down, so the error has
    /// mean 0 for every code, also for that of the query itself.
    slices: Vec<u64>,
    step4: f32,
    /// Squared length of the rounding error.
    error4: f32,
    levels: Vec<i8>,
    step8: f32,
    error8: f32,
    /// `15.5 * sum levels`: the 5-bit code value is `16 * bit + ext - 15.5`.
    center8: f64,
    /// The rounding error that an estimate may have besides its random one.
    rounding: f32,
}

impl QueryCode {
    /// `query_dir` and `centroid` are rotated unit vectors. Panics unless they have the same length,
    /// a multiple of [`CODE_STEP`] up to [`MAX_DIM`], and are unit within 1e-3.
    pub fn new(query_dir: &[f32], centroid: &[f32]) -> Self {
        let padded = query_dir.len();
        assert_padded(padded);
        assert_direction(query_dir, padded);
        assert_direction(centroid, padded);
        let q: Vec<f32> = query_dir.iter().zip(centroid).map(|(q, c)| q - c).collect();
        // Steps stay positive. The rounding errors below are measured, so tiny values cost nothing.
        let largest = q.iter().fold(1e-12f32, |max, q| max.max(q.abs()));
        let (step4, step8) = (largest / 7.5, largest / 127.0);
        let (scale4, scale8) = (step4.recip(), step8.recip());

        let words = padded / 64;
        let mut rounded = vec![0.0; padded];
        let mut slices = vec![0; SLICES * words];
        let blocks = q.as_chunks::<64>().0.iter();
        let blocks = blocks.zip(rounded.as_chunks_mut::<64>().0).enumerate();
        for (word, (q, rounded)) in blocks {
            let mut levels = [0; 64];
            for i in 0..64 {
                let dither = ((word * 64 + i) as u32).wrapping_mul(GOLDEN) as i32 as f32;
                let dither = 0.5 + dither / 4_294_967_296.0;
                let level = (q[i] * scale4 + 7.5 + dither).clamp(0.0, 15.0) as i32;
                rounded[i] = (level as f32 - 7.5) * step4;
                levels[i] = level as u8;
            }
            for s in 0..SLICES {
                let eights = levels.as_chunks::<8>().0.iter().rev();
                slices[s * words + word] = eights.fold(0, |word, eight| {
                    word << 8 | low_bits(u64::from_le_bytes(*eight) >> s)
                });
            }
        }
        let error4 = squared_distance(&rounded, &q);

        // To the nearest: the conversion drops the fraction of a positive number.
        let nearest = |q: &f32| ((q * scale8 + 127.5).clamp(0.0, 254.0) as i32 - 127) as i8;
        let levels: Vec<i8> = q.iter().map(nearest).collect();
        for (rounded, &level) in rounded.iter_mut().zip(&levels) {
            *rounded = f32::from(level) * step8;
        }
        let total: i32 = levels.iter().map(|&level| i32::from(level)).sum();
        Self {
            padded,
            q2: sum(&q, |q| q * q) as f32,
            slices,
            step4,
            error4,
            error8: squared_distance(&rounded, &q),
            levels,
            step8,
            center8: 15.5 * f64::from(total),
            rounding: rounding(padded),
        }
    }

    /// Mean squared rounding error of the levels, in squared steps.
    #[cfg(test)]
    pub(super) fn error8_in_steps(&self) -> f64 {
        f64::from(self.error8) / f64::from(self.step8).powi(2) / self.padded as f64
    }

    /// Replaces `out` with the 1-bit estimate of every entry of `block`, in order. Panics when the
    /// block has other padded dimensions.
    pub fn scan(&self, block: &CodeBlock, out: &mut Vec<Estimate>) {
        out.clear();
        self.append_scan(block, out);
    }

    pub(crate) fn append_scan(&self, block: &CodeBlock, out: &mut Vec<Estimate>) {
        assert_eq!(self.padded, block.padded, "padded dimensions");
        const CHUNK: usize = 64;
        out.reserve(block.len());
        let kernels = Kernels::detect();
        let d = self.padded as f32;
        let center = 7.5 * d;
        // The variance is `q2 * r2 (1 / a^2 - 1) / (d - 1) + scale1^2 * error4`, and the 1-bit code
        // has `r2 / a^2 = d * scale1^2`.
        let grow = self.q2 * d / (d - 1.0) + self.error4;
        let shrink = self.q2 / (d - 1.0);
        let factors = block.r2.chunks(CHUNK).zip(block.scale1.chunks(CHUNK));
        let codes = block.bits.chunks(CHUNK * self.padded / 64);
        for ((r2, scale1), codes) in factors.zip(codes) {
            let mut distances = [0; CHUNK];
            let distances = &mut distances[..r2.len()];
            kernels.bit_distances(codes, &self.slices, distances);
            // At most 15 * MAX_DIM, and the signed conversion vectorizes.
            let distances = distances.iter().map(|&n| n as i32 as f32);
            let entries = distances.zip(r2.iter().zip(scale1));
            out.extend(entries.map(|(distance, (&r2, &scale1))| {
                let dot = scale1 * self.step4 * (center - distance);
                self.estimate(r2, dot, scale1 * scale1 * grow - r2 * shrink)
            }));
        }
    }

    /// The 5-bit estimate of entry `i`. Panics when there is no entry `i`, or when the block has
    /// other padded dimensions.
    pub fn refine(&self, block: &CodeBlock, i: usize) -> Estimate {
        self.refine_with(block, i, block.ext(i))
    }

    /// [`QueryCode::refine`] of an entry whose extension [`CodeBlock::push_detached`] returned.
    /// Panics when there is no entry `i`, or on dimensions or an extension of another size.
    pub fn refine_with(&self, block: &CodeBlock, i: usize, ext: &[u8]) -> Estimate {
        assert_eq!(self.padded, block.padded, "padded dimensions");
        assert_eq!(ext.len(), block.ext_bytes(), "extension bytes");
        let (f, r2, d) = (block.factors[i], block.r2[i], self.padded as f32);
        let dot = Kernels::detect().code_dot(block.bits(i), ext, &self.levels);
        let dot = f.scale5 * self.step8 * (f64::from(dot) - self.center8) as f32;
        let var = self.q2 * f.var5 + (r2 + f.var5 * (d - 1.0)) * self.error8 / d;
        self.estimate(r2, dot, var)
    }

    /// From `|r|^2`, the estimate of `r . q` and its variance.
    #[inline]
    fn estimate(&self, r2: f32, dot: f32, var: f32) -> Estimate {
        Estimate {
            cos: 1.0 - 0.5 * (r2 + self.q2) + dot,
            err: EPSILON * var.max(0.0).sqrt() + self.rounding,
        }
    }
}

/// The lowest bit of each byte, the first byte's in bit 0.
fn low_bits(bytes: u64) -> u64 {
    (bytes & 0x0101_0101_0101_0101).wrapping_mul(0x0102_0408_1020_4080) >> 56
}

/// A bound of the rounding of an estimated cosine, from `u = 2^-24`:
///
/// - A direction is off by `u` in length from its own rounding, and each round of the rotation adds
///   one `u` per transform level, one for the factor and one for the product with it. For the two
///   directions, whose distance is at most 2, that moves the cosine by at most twice both errors.
/// - The residuals (8 u), the squared lengths (4 u), the sums (7 u) and the factors of a dot product
///   of at most 6 (7 u each).
fn rounding(padded: usize) -> f32 {
    let direction = 1 + ROUNDS * (transform_size(padded).ilog2() as usize + 2);
    (4 * direction + 64) as f32 / (1u32 << 24) as f32
}

fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    let difference: Vec<f32> = a.iter().zip(b).map(|(a, b)| a - b).collect();
    sum(&difference, |d| d * d) as f32
}

fn assert_padded(padded: usize) {
    assert!(
        (1..=MAX_DIM).contains(&padded) && padded.is_multiple_of(CODE_STEP),
        "padded dimension {padded}"
    );
}

fn assert_direction(v: &[f32], padded: usize) {
    assert_eq!(v.len(), padded, "padded dimensions");
    let norm2 = sum(v, |x| x * x);
    assert!(
        (norm2 - 1.0).abs() <= 2e-3,
        "not a direction: squared length {norm2}"
    );
}
