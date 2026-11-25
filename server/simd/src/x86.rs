//! x86-64 kernels. Every function here needs the CPU features its attribute names; `lib.rs` checks
//! them, and the slice lengths, before it calls one. Loads go through references to arrays of the
//! exact size, or through masks of the slice's own length, so no load can leave its slice whatever
//! the lengths are.

use std::arch::x86_64::*;

use crate::{LANES, SLICES};

pub(crate) mod avx2 {
    use super::*;

    #[inline]
    #[target_feature(enable = "avx2")]
    fn load(p: &[f32; 8]) -> __m256 {
        // SAFETY: `p` is 32 readable bytes; the load needs no alignment.
        unsafe { _mm256_loadu_ps(p.as_ptr()) }
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn load_words(p: &[u64; 4]) -> __m256i {
        // SAFETY: `p` is 32 readable bytes; the load needs no alignment.
        unsafe { _mm256_loadu_si256(p.as_ptr().cast()) }
    }

    #[inline]
    #[target_feature(enable = "avx2,fma")]
    fn step<const L2: bool>(acc: __m256, x: __m256, y: __m256) -> __m256 {
        if L2 {
            let d = _mm256_sub_ps(x, y);
            _mm256_fmadd_ps(d, d, acc)
        } else {
            _mm256_fmadd_ps(x, y, acc)
        }
    }

    /// The first 8 values of `p`, zero past its end.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn load_tail(p: &[f32]) -> __m256 {
        let mut values = [0.0; 8];
        for (value, &x) in values.iter_mut().zip(p) {
            *value = x;
        }
        load(&values)
    }

    /// Adds the halves of the vector until one value is left.
    #[inline]
    #[target_feature(enable = "avx2")]
    pub(super) fn add_up(v: __m256) -> f32 {
        let v = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps::<1>(v));
        let v = _mm_add_ps(v, _mm_movehl_ps(v, v));
        _mm_cvtss_f32(_mm_add_ss(v, _mm_movehdup_ps(v)))
    }

    #[target_feature(enable = "avx2,fma")]
    pub(crate) fn sum<const L2: bool>(a: &[f32], b: &[f32]) -> f32 {
        let ((blocks_a, a), (blocks_b, b)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
        let mut acc = [_mm256_setzero_ps(); 8];
        for (a, b) in blocks_a.iter().zip(blocks_b) {
            let (a, b) = (a.as_chunks::<8>().0, b.as_chunks::<8>().0);
            for i in 0..8 {
                acc[i] = step::<L2>(acc[i], load(&a[i]), load(&b[i]));
            }
        }
        for (acc, (a, b)) in acc.iter_mut().zip(a.chunks(8).zip(b.chunks(8))) {
            *acc = step::<L2>(*acc, load_tail(a), load_tail(b));
        }
        for half in [4, 2, 1] {
            for i in 0..half {
                acc[i] = _mm256_add_ps(acc[i], acc[half + i]);
            }
        }
        add_up(acc[0])
    }

    /// Set bits per byte.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn byte_counts(x: __m256i) -> __m256i {
        #[rustfmt::skip]
        let table = _mm256_setr_epi8(
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
        );
        let low = _mm256_set1_epi8(15);
        let high = _mm256_and_si256(_mm256_srli_epi16::<4>(x), low);
        let low = _mm256_and_si256(x, low);
        _mm256_add_epi8(
            _mm256_shuffle_epi8(table, low),
            _mm256_shuffle_epi8(table, high),
        )
    }

    #[target_feature(enable = "avx2")]
    pub(crate) fn bit_distances(codes: &[u64], query: &[u64], out: &mut [u32]) {
        let words = query.len() / SLICES;
        for (code, out) in codes.chunks_exact(words).zip(out) {
            let (code, code_tail) = code.as_chunks::<4>();
            let mut acc = _mm256_setzero_si256();
            for (k, c) in code.iter().enumerate() {
                let c = load_words(c);
                // Horner over the slices. A byte ends at most at 8 * 15 = 120.
                let mut counts = _mm256_setzero_si256();
                for slice in query.chunks_exact(words).rev() {
                    let q = load_words(&slice.as_chunks::<4>().0[k]);
                    let n = byte_counts(_mm256_xor_si256(c, q));
                    counts = _mm256_add_epi8(_mm256_add_epi8(counts, counts), n);
                }
                let sums = _mm256_sad_epu8(counts, _mm256_setzero_si256());
                acc = _mm256_add_epi64(acc, sums);
            }
            let (low, high) = (
                _mm256_castsi256_si128(acc),
                _mm256_extracti128_si256::<1>(acc),
            );
            let acc = _mm_add_epi64(low, high);
            let mut sum =
                _mm_cvtsi128_si64(_mm_add_epi64(acc, _mm_unpackhi_epi64(acc, acc))) as u32;
            for (s, slice) in query.chunks_exact(words).enumerate() {
                let tail = &slice[words - code_tail.len()..];
                for (c, q) in code_tail.iter().zip(tail) {
                    sum += (c ^ q).count_ones() << s;
                }
            }
            *out = sum;
        }
    }

    #[target_feature(enable = "avx2")]
    pub(crate) fn code_dot(bits: &[u64], ext: &[u8], query: &[i8]) -> i32 {
        let bits = bits.iter().flat_map(|&w| [w as i32, (w >> 32) as i32]);
        #[rustfmt::skip]
        let spread = _mm256_setr_epi8(
            0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
        );
        let select = _mm256_set1_epi64x(0x8040_2010_0804_0201_u64 as i64);
        let mut acc = _mm256_setzero_si256();
        for ((bits, ext), q) in bits
            .zip(ext.as_chunks::<16>().0)
            .zip(query.as_chunks::<32>().0)
        {
            // SAFETY: `ext` is 16 readable bytes; the load needs no alignment.
            let ext = unsafe { _mm_loadu_si128(ext.as_ptr().cast()) };
            // SAFETY: `q` is 32 readable bytes; the load needs no alignment.
            let q = unsafe { _mm256_loadu_si256(q.as_ptr().cast()) };
            // 0x00hl becomes 0x0h0l: the low half is the even dimension.
            let wide = _mm256_cvtepu8_epi16(ext);
            let wide = _mm256_or_si256(wide, _mm256_slli_epi16::<4>(wide));
            let low = _mm256_and_si256(wide, _mm256_set1_epi8(15));
            let bits = _mm256_shuffle_epi8(_mm256_set1_epi32(bits), spread);
            let set = _mm256_cmpeq_epi8(_mm256_and_si256(bits, select), select);
            let code = _mm256_or_si256(low, _mm256_and_si256(set, _mm256_set1_epi8(16)));
            // Pairs stay within 2 * 31 * 128, so the i16 sums cannot saturate.
            let pairs = _mm256_maddubs_epi16(code, q);
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(pairs, _mm256_set1_epi16(1)));
        }
        let acc = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256::<1>(acc),
        );
        let acc = _mm_add_epi32(acc, _mm_unpackhi_epi64(acc, acc));
        _mm_cvtsi128_si32(_mm_add_epi32(acc, _mm_shuffle_epi32::<1>(acc)))
    }

    #[target_feature(enable = "avx2")]
    pub(crate) fn hadamard(x: &mut [f32]) {
        crate::scalar::hadamard(x);
    }
}

pub(crate) mod avx512 {
    use super::*;

    #[inline]
    #[target_feature(enable = "avx512f")]
    fn load(p: &[f32; 16]) -> __m512 {
        // SAFETY: `p` is 64 readable bytes; the load needs no alignment.
        unsafe { _mm512_loadu_ps(p.as_ptr()) }
    }

    /// The first 16 values of `p`, zero past its end.
    #[inline]
    #[target_feature(enable = "avx512f")]
    fn load_tail(p: &[f32]) -> __m512 {
        let mask = ((1u32 << p.len().min(16)) - 1) as u16;
        // SAFETY: the mask selects at most the first `p.len()` lanes, and a masked load neither
        // reads nor faults on the others.
        unsafe { _mm512_maskz_loadu_ps(mask, p.as_ptr()) }
    }

    /// The first 8 words of `p`, zero past its end.
    #[inline]
    #[target_feature(enable = "avx512f")]
    fn load_words(p: &[u64]) -> __m512i {
        let mask = ((1u32 << p.len().min(8)) - 1) as u8;
        // SAFETY: the mask selects at most the first `p.len()` lanes, and a masked load neither
        // reads nor faults on the others.
        unsafe { _mm512_maskz_loadu_epi64(mask, p.as_ptr().cast()) }
    }

    #[inline]
    #[target_feature(enable = "avx512f")]
    fn step<const L2: bool>(acc: __m512, x: __m512, y: __m512) -> __m512 {
        if L2 {
            let d = _mm512_sub_ps(x, y);
            _mm512_fmadd_ps(d, d, acc)
        } else {
            _mm512_fmadd_ps(x, y, acc)
        }
    }

    #[target_feature(enable = "avx512f")]
    pub(crate) fn sum<const L2: bool>(a: &[f32], b: &[f32]) -> f32 {
        let ((blocks_a, a), (blocks_b, b)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
        let mut acc = [_mm512_setzero_ps(); 4];
        for (a, b) in blocks_a.iter().zip(blocks_b) {
            let (a, b) = (a.as_chunks::<16>().0, b.as_chunks::<16>().0);
            for i in 0..4 {
                acc[i] = step::<L2>(acc[i], load(&a[i]), load(&b[i]));
            }
        }
        for (acc, (a, b)) in acc.iter_mut().zip(a.chunks(16).zip(b.chunks(16))) {
            *acc = step::<L2>(*acc, load_tail(a), load_tail(b));
        }
        let (even, odd) = (_mm512_add_ps(acc[0], acc[2]), _mm512_add_ps(acc[1], acc[3]));
        let v = _mm512_add_ps(even, odd);
        let high = _mm256_castpd_ps(_mm512_extractf64x4_pd::<1>(_mm512_castps_pd(v)));
        super::avx2::add_up(_mm256_add_ps(_mm512_castps512_ps256(v), high))
    }

    #[target_feature(enable = "avx512f,avx512vpopcntdq")]
    pub(crate) fn bit_distances(codes: &[u64], query: &[u64], out: &mut [u32]) {
        let words = query.len() / SLICES;
        let (low, high) = query.split_at(2 * words);
        let ((q0, q1), (q2, q3)) = (low.split_at(words), high.split_at(words));
        let slices = [q0, q1, q2, q3].map(|q| q.as_chunks::<8>());
        for (code, out) in codes.chunks_exact(words).zip(out) {
            let (code, tail) = code.as_chunks::<8>();
            let mut acc = [_mm512_setzero_si512(); SLICES];
            for (k, c) in code.iter().enumerate() {
                let c = load_words(c);
                for (acc, (q, _)) in acc.iter_mut().zip(slices) {
                    let x = _mm512_xor_si512(c, load_words(&q[k]));
                    *acc = _mm512_add_epi64(*acc, _mm512_popcnt_epi64(x));
                }
            }
            if !tail.is_empty() {
                let c = load_words(tail);
                for (acc, (_, q)) in acc.iter_mut().zip(slices) {
                    let x = _mm512_xor_si512(c, load_words(q));
                    *acc = _mm512_add_epi64(*acc, _mm512_popcnt_epi64(x));
                }
            }
            let low = _mm512_add_epi64(acc[0], _mm512_slli_epi64::<1>(acc[1]));
            let high = _mm512_add_epi64(acc[2], _mm512_slli_epi64::<1>(acc[3]));
            let sum = _mm512_add_epi64(low, _mm512_slli_epi64::<2>(high));
            *out = _mm512_reduce_add_epi64(sum) as u32;
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    pub(crate) fn code_dot(bits: &[u64], ext: &[u8], query: &[i8]) -> i32 {
        let (ext, query) = (ext.as_chunks::<32>().0, query.as_chunks::<64>().0);
        let mut acc = _mm512_setzero_si512();
        for ((&bits, ext), q) in bits.iter().zip(ext).zip(query) {
            // SAFETY: `ext` is 32 readable bytes; the load needs no alignment.
            let ext = unsafe { _mm256_loadu_si256(ext.as_ptr().cast()) };
            // SAFETY: `q` is 64 readable bytes; the load needs no alignment.
            let q = unsafe { _mm512_loadu_si512(q.as_ptr().cast()) };
            // 0x00hl becomes 0x0h0l: the low half is the even dimension.
            let wide = _mm512_cvtepu8_epi16(ext);
            let wide = _mm512_or_si512(wide, _mm512_slli_epi16::<4>(wide));
            let low = _mm512_and_si512(wide, _mm512_set1_epi8(15));
            let code = _mm512_or_si512(low, _mm512_maskz_set1_epi8(bits, 16));
            // Pairs stay within 2 * 31 * 128, so the i16 sums cannot saturate.
            let pairs = _mm512_maddubs_epi16(code, q);
            acc = _mm512_add_epi32(acc, _mm512_madd_epi16(pairs, _mm512_set1_epi16(1)));
        }
        _mm512_reduce_add_epi32(acc)
    }

    #[target_feature(enable = "avx512f")]
    pub(crate) fn hadamard(x: &mut [f32]) {
        crate::scalar::hadamard(x);
    }
}
