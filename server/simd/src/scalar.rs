//! The reference kernels. The callers in `lib.rs` check the lengths.

use crate::{LANES, SLICES};

/// The float kernel of every level, in lanes: value `i` goes to lane `i % LANES` by a fused
/// multiply-add, and halves of the lanes are added until one is left.
pub(crate) fn sum<const L2: bool>(a: &[f32], b: &[f32]) -> f32 {
    let mut lanes = [0.0f32; LANES];
    for (a, b) in a.chunks(LANES).zip(b.chunks(LANES)) {
        for (lane, (&x, &y)) in lanes.iter_mut().zip(a.iter().zip(b)) {
            let (x, y) = if L2 { (x - y, x - y) } else { (x, y) };
            *lane = x.mul_add(y, *lane);
        }
    }
    let mut half = LANES / 2;
    while half > 0 {
        for i in 0..half {
            lanes[i] += lanes[half + i];
        }
        half /= 2;
    }
    lanes[0]
}

pub(crate) fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| f64::from(x) * f64::from(y))
        .sum()
}

pub(crate) fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
    let diff = |(&x, &y): (&f32, &f32)| f64::from(x) - f64::from(y);
    a.iter().zip(b).map(diff).map(|d| d * d).sum()
}

pub(crate) fn bit_distances(codes: &[u64], query: &[u64], out: &mut [u32]) {
    let words = query.len() / SLICES;
    for (code, out) in codes.chunks_exact(words).zip(out) {
        let distance = |slice: &[u64]| -> u32 {
            code.iter()
                .zip(slice)
                .map(|(c, q)| (c ^ q).count_ones())
                .sum()
        };
        *out = query
            .chunks_exact(words)
            .enumerate()
            .map(|(s, q)| distance(q) << s)
            .sum();
    }
}

pub(crate) fn code_dot(bits: &[u64], ext: &[u8], query: &[i8]) -> i32 {
    let value = |i: usize| {
        let bit = (bits[i / 64] >> (i % 64)) as i32 & 1;
        bit * 16 + i32::from(ext[i / 2] >> (i % 2 * 4) & 15)
    };
    query
        .iter()
        .enumerate()
        .map(|(i, &q)| value(i) * i32::from(q))
        .sum()
}

/// Only additions and subtractions in a fixed order, so every level gives the same bits. Inlined
/// into the other levels, which compile it with their instructions.
#[inline(always)]
pub(crate) fn hadamard(x: &mut [f32]) {
    // The first three rounds in groups of 8, where fixed indexes vectorize.
    let (groups, rest) = x.as_chunks_mut::<8>();
    for group in groups {
        group_round(group, 1);
        group_round(group, 2);
        group_round(group, 4);
    }
    let mut half = if rest.is_empty() { 8 } else { 1 };
    while half < x.len() {
        for block in x.chunks_exact_mut(2 * half) {
            let (low, high) = block.split_at_mut(half);
            for (a, b) in low.iter_mut().zip(high) {
                (*a, *b) = (*a + *b, *a - *b);
            }
        }
        half *= 2;
    }
}

#[inline(always)]
fn group_round(x: &mut [f32; 8], half: usize) {
    let before = *x;
    for i in 0..8 {
        let sign = if i & half == 0 { 1.0 } else { -1.0 };
        x[i] = before[i ^ half] + sign * before[i];
    }
}
