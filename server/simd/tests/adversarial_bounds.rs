//! A kernel must read nothing outside its slices. The tests in `kernels.rs` cut their slices out
//! of larger buffers, where an over-read goes unseen. Here every slice is an allocation of its
//! exact size (AddressSanitizer faults on a read past it: see the commands in the report), and
//! the memory around a slice holds poison that must not reach a result.

use nabedra_check::rng::SplitMix64;
use nabedra_simd::{Kernels, Level, SLICES, wide};

fn levels() -> impl Iterator<Item = Kernels> {
    Level::ALL.into_iter().filter_map(Kernels::at)
}

fn floats(rng: &mut SplitMix64, n: usize) -> Vec<f32> {
    (0..n)
        .map(|_| (rng.next_u64() >> 40) as f32 / 1e6 - 8.0)
        .collect()
}

fn words(rng: &mut SplitMix64, n: usize) -> Vec<u64> {
    (0..n).map(|_| rng.next_u64()).collect()
}

/// `data` followed by `poison` copies of `fill`, and the length of `data`.
fn padded<T: Copy>(data: &[T], fill: T, poison: usize) -> Vec<T> {
    let mut v = data.to_vec();
    v.extend(std::iter::repeat_n(fill, poison));
    v
}

#[test]
fn float_kernels_read_only_their_slices() {
    let mut rng = SplitMix64::new(1);
    for n in (0..=200).chain([255, 256, 257, 1023, 1025, 4095, 4097]) {
        let (a, b) = (floats(&mut rng, n), floats(&mut rng, n));
        for k in levels() {
            // Exact-size allocations: for AddressSanitizer.
            let (ea, eb) = (a.clone().into_boxed_slice(), b.clone().into_boxed_slice());
            let want = (k.dot(&ea, &eb), k.l2_sq(&ea, &eb), k.norm_sq(&ea));
            assert!(!want.0.is_nan());
            // Poison next to the slices, ahead of them and behind.
            for fill in [f32::NAN, f32::INFINITY, -f32::MAX, 1e30] {
                let (pa, pb) = (padded(&a, fill, 70), padded(&b, fill, 70));
                let got = (
                    k.dot(&pa[..n], &pb[..n]),
                    k.l2_sq(&pa[..n], &pb[..n]),
                    k.norm_sq(&pa[..n]),
                );
                assert_eq!(
                    (got.0.to_bits(), got.1.to_bits(), got.2.to_bits()),
                    (want.0.to_bits(), want.1.to_bits(), want.2.to_bits()),
                    "{k:?} n={n} poison {fill}"
                );
                let mut lead = vec![fill; 33];
                lead.extend_from_slice(&a);
                assert_eq!(
                    k.norm_sq(&lead[33..]).to_bits(),
                    want.2.to_bits(),
                    "{k:?} n={n} lead"
                );
            }
            // And against the f64 sums, to 1e-5 relative to the magnitude of the terms.
            let terms: f64 = a.iter().zip(&b).map(|(x, y)| f64::from(x * y).abs()).sum();
            assert!((f64::from(want.0) - wide::dot(&a, &b)).abs() <= 1e-5 * terms + 1e-30);
        }
    }
}

#[test]
fn bit_distance_kernels_read_only_their_slices() {
    let mut rng = SplitMix64::new(2);
    for w in 1..=40usize {
        for entries in [0usize, 1, 2, 3, 7] {
            let codes = words(&mut rng, w * entries);
            let query = words(&mut rng, w * SLICES);
            let naive: Vec<u32> = codes
                .chunks_exact(w)
                .map(|c| {
                    (0..SLICES)
                        .map(|s| {
                            c.iter()
                                .zip(&query[s * w..])
                                .map(|(a, b)| (a ^ b).count_ones())
                                .sum::<u32>()
                                << s
                        })
                        .sum()
                })
                .collect();
            for k in levels() {
                let (ec, eq) = (
                    codes.clone().into_boxed_slice(),
                    query.clone().into_boxed_slice(),
                );
                let mut out = vec![u32::MAX; entries].into_boxed_slice();
                k.bit_distances(&ec, &eq, &mut out);
                assert_eq!(&*out, &naive[..], "{k:?} w={w} entries={entries}");
                // Poison after the code words and the query, and after the output.
                for fill in [0u64, u64::MAX, 0xaaaa_aaaa_aaaa_aaaa] {
                    let (pc, pq) = (padded(&codes, fill, 9), padded(&query, fill, 9));
                    let mut out = vec![0xdead_beefu32; entries + 5];
                    k.bit_distances(&pc[..codes.len()], &pq[..query.len()], &mut out[..entries]);
                    assert_eq!(&out[..entries], &naive[..], "{k:?} w={w} poison {fill:x}");
                    assert!(
                        out[entries..].iter().all(|&x| x == 0xdead_beef),
                        "{k:?} wrote past out"
                    );
                }
            }
        }
    }
}

#[test]
fn code_dot_kernels_read_only_their_slices() {
    let mut rng = SplitMix64::new(3);
    for w in 0..=40usize {
        let bits = words(&mut rng, w);
        let ext: Vec<u8> = (0..w * 32).map(|_| rng.next_u64() as u8).collect();
        let query: Vec<i8> = (0..w * 64).map(|_| rng.next_u64() as i8).collect();
        let want: i32 = (0..w * 64)
            .map(|i| {
                let nibble = if i % 2 == 0 {
                    ext[i / 2] & 15
                } else {
                    ext[i / 2] >> 4
                };
                let bit = if bits[i / 64] >> (i % 64) & 1 == 1 {
                    16
                } else {
                    0
                };
                (bit + i32::from(nibble)) * i32::from(query[i])
            })
            .sum();
        for k in levels() {
            let (eb, ee, eq) = (
                bits.clone().into_boxed_slice(),
                ext.clone().into_boxed_slice(),
                query.clone().into_boxed_slice(),
            );
            assert_eq!(k.code_dot(&eb, &ee, &eq), want, "{k:?} w={w}");
            for fill in [0u8, 0xff, 0x5a] {
                let (pe, pq) = (padded(&ext, fill, 70), padded(&query, fill as i8, 70));
                let pb = padded(&bits, u64::from(fill) * 0x0101_0101_0101_0101, 3);
                assert_eq!(
                    k.code_dot(&pb[..w], &pe[..w * 32], &pq[..w * 64]),
                    want,
                    "{k:?} w={w} poison"
                );
            }
        }
    }
}

#[test]
fn hadamard_reads_and_writes_only_its_slice() {
    let mut rng = SplitMix64::new(4);
    for log in 0..=13u32 {
        let n = 1usize << log;
        let x = floats(&mut rng, n);
        let mut want = x.clone();
        Kernels::at(Level::Scalar).unwrap().hadamard(&mut want);
        for k in levels() {
            let mut exact = x.clone().into_boxed_slice();
            k.hadamard(&mut exact);
            assert_eq!(
                exact.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                want.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                "{k:?} n={n}"
            );
            let mut p = padded(&x, f32::NAN, 40);
            k.hadamard(&mut p[..n]);
            assert!(
                p[n..].iter().all(|v| v.is_nan()),
                "{k:?} n={n} wrote past the slice"
            );
            assert!(
                p[..n]
                    .iter()
                    .zip(&want)
                    .all(|(a, b)| a.to_bits() == b.to_bits()),
                "{k:?} n={n}"
            );
        }
    }
}

/// Lengths that are not powers of two, zero and mismatches are refused, not read.
#[test]
fn refusals_come_before_any_read() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    for k in levels() {
        let refuses = |f: &mut dyn FnMut()| catch_unwind(AssertUnwindSafe(f)).is_err();
        for n in [3usize, 6, 12, 100, 1000] {
            let mut x = vec![1.0f32; n];
            assert!(refuses(&mut || k.hadamard(&mut x)), "{k:?} hadamard {n}");
        }
        let mut out = [0u32; 2];
        assert!(
            refuses(&mut || k.bit_distances(&[0; 5], &[0; 4], &mut out)),
            "{k:?} sizes"
        );
        assert!(
            refuses(&mut || k.bit_distances(&[0; 4], &[0; 5], &mut out)),
            "{k:?} query size"
        );
        assert!(
            refuses(&mut || ignore(k.code_dot(&[0; 2], &[0; 64], &[0; 64]))),
            "{k:?} code_dot bits"
        );
        assert!(
            refuses(&mut || ignore(k.code_dot(&[0; 1], &[0; 31], &[0; 64]))),
            "{k:?} code_dot ext"
        );
        assert!(
            refuses(&mut || ignore(k.dot(&[1.0; 3], &[1.0; 4]))),
            "{k:?} dot lens"
        );
        assert!(
            refuses(&mut || ignore(k.l2_sq(&[1.0; 4], &[1.0; 3]))),
            "{k:?} l2 lens"
        );
        // A huge query is refused before any arithmetic: 2^18 + 64 values.
        let big = vec![0i8; (1 << 18) + 64];
        assert!(refuses(&mut || ignore(k.code_dot(
            &vec![0; big.len() / 64],
            &vec![0; big.len() / 2],
            &big
        ))));
    }
}

/// The widest inputs the documented limits allow, with extreme values: the integer sums reach
/// their largest magnitude and must not wrap, at any level.
#[test]
fn integer_kernels_do_not_wrap_at_their_documented_limits() {
    for k in levels() {
        // 2^18 values: all code values 31 against 127, and against -128.
        let n = 1usize << 18;
        let (bits, ext) = (vec![u64::MAX; n / 64], vec![0xffu8; n / 2]);
        for (q, want) in [
            (127i8, 31i64 * 127 * n as i64),
            (-128, -31 * 128 * n as i64),
        ] {
            let query = vec![q; n];
            assert_eq!(
                i64::from(k.code_dot(&bits, &ext, &query)),
                want,
                "{k:?} code_dot q={q}"
            );
        }
        // Alternating signs, to catch an early saturation in the pairwise sums.
        let query: Vec<i8> = (0..n)
            .map(|i| if i % 3 == 0 { -128 } else { 127 })
            .collect();
        let want: i64 = query.iter().map(|&q| 31 * i64::from(q)).sum();
        assert_eq!(
            i64::from(k.code_dot(&bits, &ext, &query)),
            want,
            "{k:?} code_dot mixed"
        );

        // 2^20 words of code against a query of the opposite bits: every slice counts every bit.
        let w = 1usize << 20;
        let codes = vec![u64::MAX; w];
        let query = vec![0u64; w * SLICES];
        let mut out = [0u32];
        k.bit_distances(&codes, &query, &mut out);
        assert_eq!(
            u64::from(out[0]),
            64 * w as u64 * 15,
            "{k:?} bit_distances at 2^20 words"
        );
        // One word more is refused.
        let (codes, query) = (vec![0u64; w + 1], vec![0u64; (w + 1) * SLICES]);
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            k.bit_distances(&codes, &query, &mut [0u32]);
        }))
        .is_err();
        assert!(refused, "{k:?} accepted 2^20 + 1 words");
    }
}

fn ignore<T>(_: T) {}
