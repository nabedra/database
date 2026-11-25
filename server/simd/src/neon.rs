//! AArch64 kernels. Every function here needs NEON; `lib.rs` checks it, and the slice lengths,
//! before it calls one. Loads go through references to arrays of the exact size, so no load can
//! leave its slice whatever the lengths are. `vdotq_s32` is not stable on the pinned toolchain, so
//! [`code_dot`] multiplies with widening.

use std::arch::aarch64::*;

use crate::{LANES, SLICES};

#[inline]
#[target_feature(enable = "neon")]
fn load(p: &[f32; 4]) -> float32x4_t {
    // SAFETY: `p` is 16 readable bytes; the load needs only element alignment.
    unsafe { vld1q_f32(p.as_ptr()) }
}

#[inline]
#[target_feature(enable = "neon")]
fn load_words(p: &[u64; 2]) -> uint8x16_t {
    // SAFETY: `p` is 16 readable bytes; the load needs only element alignment.
    vreinterpretq_u8_u64(unsafe { vld1q_u64(p.as_ptr()) })
}

#[inline]
#[target_feature(enable = "neon")]
fn step<const L2: bool>(acc: float32x4_t, x: float32x4_t, y: float32x4_t) -> float32x4_t {
    if L2 {
        let d = vsubq_f32(x, y);
        vfmaq_f32(acc, d, d)
    } else {
        vfmaq_f32(acc, x, y)
    }
}

/// The first values of `p`, then `fill`. The first operand is filled with -0 and the second
/// with +0: their product -0 leaves every sum as it is, also the sign of a zero, as the lanes
/// that the scalar level never reaches.
#[inline]
#[target_feature(enable = "neon")]
fn load_tail(p: &[f32], fill: f32) -> float32x4_t {
    let mut values = [fill; 4];
    for (value, &x) in values.iter_mut().zip(p) {
        *value = x;
    }
    load(&values)
}

#[target_feature(enable = "neon")]
pub(crate) fn sum<const L2: bool>(a: &[f32], b: &[f32]) -> f32 {
    let ((blocks_a, a), (blocks_b, b)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
    let mut acc = [vdupq_n_f32(0.0); 16];
    for (a, b) in blocks_a.iter().zip(blocks_b) {
        let (a, b) = (a.as_chunks::<4>().0, b.as_chunks::<4>().0);
        for i in 0..16 {
            acc[i] = step::<L2>(acc[i], load(&a[i]), load(&b[i]));
        }
    }
    for (acc, (a, b)) in acc.iter_mut().zip(a.chunks(4).zip(b.chunks(4))) {
        *acc = step::<L2>(*acc, load_tail(a, -0.0), load_tail(b, 0.0));
    }
    for half in [8, 4, 2, 1] {
        for i in 0..half {
            acc[i] = vaddq_f32(acc[i], acc[half + i]);
        }
    }
    let pair = vadd_f32(vget_low_f32(acc[0]), vget_high_f32(acc[0]));
    vget_lane_f32::<0>(vpadd_f32(pair, pair))
}

#[target_feature(enable = "neon")]
pub(crate) fn bit_distances(codes: &[u64], query: &[u64], out: &mut [u32]) {
    let words = query.len() / SLICES;
    for (code, out) in codes.chunks_exact(words).zip(out) {
        let (code, code_tail) = code.as_chunks::<2>();
        let mut sum = 0;
        for (k, c) in code.iter().enumerate() {
            let c = load_words(c);
            // Horner over the slices. A byte ends at most at 8 * 15 = 120.
            let mut counts = vdupq_n_u8(0);
            for slice in query.chunks_exact(words).rev() {
                let n = vcntq_u8(veorq_u8(c, load_words(&slice.as_chunks::<2>().0[k])));
                counts = vaddq_u8(vaddq_u8(counts, counts), n);
            }
            sum += u32::from(vaddlvq_u8(counts));
        }
        for (s, slice) in query.chunks_exact(words).enumerate() {
            let tail = &slice[words - code_tail.len()..];
            for (c, q) in code_tail.iter().zip(tail) {
                sum += (c ^ q).count_ones() << s;
            }
        }
        *out = sum;
    }
}

#[target_feature(enable = "neon")]
pub(crate) fn code_dot(bits: &[u64], ext: &[u8], query: &[i8]) -> i32 {
    let bits = bits
        .iter()
        .flat_map(|&w| [0, 16, 32, 48].map(|shift| (w >> shift) as u16));
    let select = vreinterpretq_u8_u64(vdupq_n_u64(0x8040_2010_0804_0201));
    let mut acc = vdupq_n_s32(0);
    for ((bits, ext), q) in bits
        .zip(ext.as_chunks::<8>().0)
        .zip(query.as_chunks::<16>().0)
    {
        let [low, high] = bits.to_le_bytes();
        // SAFETY: `ext` is 8 readable bytes; the load needs only element alignment.
        let ext = unsafe { vld1_u8(ext.as_ptr()) };
        // SAFETY: `q` is 16 readable bytes; the load needs only element alignment.
        let q = unsafe { vld1q_s8(q.as_ptr()) };
        // 0x00hl becomes 0x0h0l: the low half is the even dimension.
        let wide = vmovl_u8(ext);
        let wide = vreinterpretq_u8_u16(vorrq_u16(wide, vshlq_n_u16::<4>(wide)));
        let set = vtstq_u8(vcombine_u8(vdup_n_u8(low), vdup_n_u8(high)), select);
        let code = vorrq_u8(
            vandq_u8(wide, vdupq_n_u8(15)),
            vandq_u8(set, vdupq_n_u8(16)),
        );
        let code = vreinterpretq_s8_u8(code);
        acc = vpadalq_s16(acc, vmull_s8(vget_low_s8(code), vget_low_s8(q)));
        acc = vpadalq_s16(acc, vmull_high_s8(code, q));
    }
    vaddvq_s32(acc)
}

#[target_feature(enable = "neon")]
pub(crate) fn hadamard(x: &mut [f32]) {
    crate::scalar::hadamard(x);
}
