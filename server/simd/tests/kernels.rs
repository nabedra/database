//! Every level of this CPU against the scalar reference.

use nabedra_check::{r#gen::Gen, prop, rng::SplitMix64};
use nabedra_simd::{Kernels, Level, SLICES, dot, l2_sq, norm_sq, relative_error, wide};

fn levels() -> impl Iterator<Item = Kernels> {
    Level::ALL.into_iter().filter_map(Kernels::at)
}

/// Binary exponents `[low, high]` of the generated values. With [`MAX_LEN`] values, the sum of
/// the squared differences of the last two still fits f32.
const EXPONENTS: [(i32, i32); 6] = [
    (-2, 1),
    (-149, -127),
    (-149, -100),
    (-20, 20),
    (40, 56),
    (-149, 56),
];
const MAX_LEN: usize = 10_240;
/// Lengths past the exhaustive range: dimensions in use and their neighbours.
const LONG: [usize; 10] = [383, 384, 511, 768, 1023, 1024, 1536, 4095, 4096, MAX_LEN];

fn value(bits: u64, (low, high): (i32, i32)) -> f32 {
    let exponent = low + (bits >> 40) as i32 % (high - low + 1);
    let mantissa = 1.0 + (bits as u32 >> 9) as f64 / f64::from(1 << 23);
    let sign = if bits >> 39 & 1 == 1 { -1.0 } else { 1.0 };
    (sign * mantissa * 2f64.powi(exponent)) as f32
}

fn values(rng: &mut SplitMix64, len: usize, range: (i32, i32)) -> Vec<f32> {
    (0..len).map(|_| value(rng.next_u64(), range)).collect()
}

/// The tolerance in the crate documentation, for terms whose magnitudes sum to `terms`.
fn tolerance(n: usize, terms: f64) -> f64 {
    relative_error(n) * terms + n as f64 * 2f64.powi(-149)
}

fn near(what: &str, level: Level, n: usize, got: f32, want: f64, terms: f64) -> Result<(), String> {
    if (f64::from(got) - want).abs() <= tolerance(n, terms) {
        return Ok(());
    }
    Err(format!(
        "{what} {level:?} n={n}: {got:e}, reference {want:e}, terms {terms:e}"
    ))
}

/// All float kernels on one pair, at every level.
fn float_kernels(a: &[f32], b: &[f32]) -> Result<(), String> {
    let n = a.len();
    let terms: f64 = a
        .iter()
        .zip(b)
        .map(|(&x, &y)| (f64::from(x) * f64::from(y)).abs())
        .sum();
    let naive_dot: f64 = a
        .iter()
        .zip(b)
        .map(|(&x, &y)| f64::from(x) * f64::from(y))
        .sum();
    let naive_l2: f64 = a
        .iter()
        .zip(b)
        .map(|(&x, &y)| (f64::from(x) - f64::from(y)).powi(2))
        .sum();
    if wide::dot(a, b) != naive_dot || wide::l2_sq(a, b) != naive_l2 {
        return Err("the reference is not the sum of its terms".into());
    }
    let bits = |k: Kernels| [k.dot(a, b), k.l2_sq(a, b), k.norm_sq(a)].map(f32::to_bits);
    for k in levels() {
        if bits(k) != bits(Kernels::detect()) {
            return Err(format!("{k:?} n={n}: other bits than the best level"));
        }
        near("dot", k.level(), n, k.dot(a, b), naive_dot, terms)?;
        near("l2_sq", k.level(), n, k.l2_sq(a, b), naive_l2, naive_l2)?;
        near(
            "norm_sq",
            k.level(),
            n,
            k.norm_sq(a),
            wide::norm_sq(a),
            wide::norm_sq(a),
        )?;
    }
    Ok(())
}

#[test]
fn every_level_matches_the_reference_at_every_length_and_offset() {
    let mut rng = SplitMix64::new(1);
    for n in (0..=300).chain(LONG) {
        let range = EXPONENTS[n % EXPONENTS.len()];
        // Every alignment a float slice can have in a 64-byte line, over the lengths.
        let (left, right) = (n % 16, (n / 16) % 16);
        let (a, b) = (
            values(&mut rng, left + n, range),
            values(&mut rng, right + n, range),
        );
        float_kernels(&a[left..], &b[right..]).unwrap();
        float_kernels(&values(&mut rng, n, (-2, 1)), &values(&mut rng, n, (-2, 1))).unwrap();
    }
}

#[test]
fn every_level_matches_the_reference_on_generated_slices() {
    prop::check("simd float kernels", |g: &mut Gen| {
        let n = if g.chance(1, 8) {
            *g.pick(&LONG)
        } else {
            g.below(301) as usize
        };
        let (left, right) = (g.below(16) as usize, g.below(16) as usize);
        let (range_a, range_b) = (*g.pick(&EXPONENTS), *g.pick(&EXPONENTS));
        let mut rng = SplitMix64::new(g.u64());
        let mut a = values(&mut rng, left + n, range_a);
        let mut b = values(&mut rng, right + n, range_b);
        match g.below(4) {
            // Near duplicates, so `l2_sq` subtracts almost equal values.
            0 => {
                for (y, x) in b[right..].iter_mut().zip(&a[left..]) {
                    *y = x * (1.0 + 1e-6);
                }
            }
            // A single value that a kernel must not drop or read twice.
            1 if n > 0 => {
                let i = g.below(n as u64) as usize;
                a[left..].fill(0.0);
                a[left + i] = 3.0;
            }
            _ => {}
        }
        float_kernels(&a[left..], &b[right..])
    });
}

#[test]
fn free_functions_use_the_best_level() {
    let mut rng = SplitMix64::new(2);
    let (a, b) = (
        values(&mut rng, 333, (-2, 1)),
        values(&mut rng, 333, (-2, 1)),
    );
    let best = Kernels::detect();
    assert_eq!(Some(best), levels().last());
    let got = [dot(&a, &b), l2_sq(&a, &b), norm_sq(&a)].map(f32::to_bits);
    assert_eq!(
        got,
        [best.dot(&a, &b), best.l2_sq(&a, &b), best.norm_sq(&a)].map(f32::to_bits)
    );
    for level in Level::ALL {
        assert!(Kernels::at(level).is_none_or(|k| k.level() == level));
    }
    assert!(Kernels::at(Level::Scalar).is_some());
    let foreign = if cfg!(target_arch = "x86_64") {
        Level::Neon
    } else {
        Level::Avx2
    };
    assert!(Kernels::at(foreign).is_none());
}

/// The worst case of the stated error: every lane holds a large value, and each later term is just
/// below half a unit in its last place, so all of them are lost.
#[test]
fn relative_error_holds_where_every_rounding_goes_one_way() {
    for n in [64, 65, 128, 1000, 1024, 1025, 4096, 10_240, 65_536] {
        for small in [-11, -12, -13, -14].map(|e| 2f32.powi(e)) {
            for lanes in [1, 4, 16, 32, 64, 128, 1024] {
                for other in [small, small * small] {
                    let fill = |x: f32| (0..n).map(move |i| if i % 1024 < lanes { 1.0 } else { x });
                    let (a, b): (Vec<f32>, Vec<f32>) =
                        (fill(small).collect(), fill(other).collect());
                    float_kernels(&a, &b).unwrap();
                }
            }
        }
    }
}

/// The sign of a zero result, from products that underflow, at every length and offset in a lane.
#[test]
fn zero_results_have_the_same_sign_at_every_level() {
    let scalar = Kernels::at(Level::Scalar).expect("the scalar level");
    for n in 0..=300 {
        for (x, y) in [
            (-1e-30f32, 1e-30f32),
            (1e-30, 1e-30),
            (-0.0, 0.0),
            (-0.0, -0.0),
        ] {
            for mixed in [false, true] {
                let a: Vec<f32> = (0..n)
                    .map(|i| if mixed && i % 3 == 0 { -x } else { x })
                    .collect();
                let b = vec![y; n];
                let want = [scalar.dot(&a, &b), scalar.l2_sq(&a, &b)].map(f32::to_bits);
                for k in levels() {
                    let got = [k.dot(&a, &b), k.l2_sq(&a, &b)].map(f32::to_bits);
                    assert_eq!(got, want, "{k:?} n={n} {x} {y} {mixed}");
                }
            }
        }
    }
    let (a, b) = ([-1e-30f32; 65], [1e-30f32; 65]);
    assert_eq!(scalar.dot(&a, &b).to_bits(), 0x8000_0000);
}

/// A lane at f32::MAX loses later terms below half its last place, as rounding does elsewhere:
/// the result stays finite and meets the stated bound.
#[test]
fn a_finite_result_meets_the_bound_even_past_f32() {
    for n in [16, 65, 1024, 1025] {
        let mut a = vec![0.0f32; n];
        a[0] = f32::MAX;
        for i in (64..n).step_by(64).take(15) {
            a[i] = 1e31;
        }
        let ones = vec![1.0f32; n];
        for k in levels() {
            let got = k.dot(&a, &ones);
            assert!(got.is_finite(), "{k:?} n={n}");
            near(
                "dot",
                k.level(),
                n,
                got,
                wide::dot(&a, &ones),
                wide::dot(&a, &ones),
            )
            .unwrap();
        }
    }
}

/// A sum past f32 is infinite or NaN at every level and correct in `wide`.
#[test]
fn products_past_f32_give_no_finite_result() {
    let mut rng = SplitMix64::new(3);
    for n in 1..=300 {
        let huge: Vec<f32> = (0..n)
            .map(|_| 1e30 * (1.0 + (rng.next_u64() >> 40) as f32))
            .collect();
        let negative: Vec<f32> = huge.iter().map(|x| -x).collect();
        assert!(wide::dot(&huge, &negative).is_finite() && wide::dot(&huge, &negative) < -1e60);
        for k in levels() {
            for value in [
                k.dot(&huge, &negative),
                k.l2_sq(&huge, &negative),
                k.norm_sq(&huge),
            ] {
                assert!(!value.is_finite(), "{k:?} n={n}");
            }
        }
    }
}

#[test]
fn different_lengths_are_refused() {
    for k in levels() {
        for (a, b) in [(3, 4), (16, 0), (64, 65)] {
            let (a, b) = (vec![1.0; a], vec![1.0; b]);
            assert!(std::panic::catch_unwind(|| k.dot(&a, &b)).is_err(), "{k:?}");
            assert!(
                std::panic::catch_unwind(|| k.l2_sq(&a, &b)).is_err(),
                "{k:?}"
            );
        }
        let refused = |codes: usize, query: usize, out: usize| {
            let (codes, query, mut out) = (vec![0; codes], vec![0; query], vec![0; out]);
            let run = std::panic::AssertUnwindSafe(|| k.bit_distances(&codes, &query, &mut out));
            std::panic::catch_unwind(run).is_err()
        };
        assert!(!refused(6, 12, 2));
        assert!(refused(6, 13, 2) && refused(5, 12, 2) && refused(6, 12, 1) && refused(1, 0, 1));
        let refused = |bits: usize, ext: usize, query: usize| {
            let (bits, ext, query) = (vec![0; bits], vec![0; ext], vec![0; query]);
            std::panic::catch_unwind(|| k.code_dot(&bits, &ext, &query)).is_err()
        };
        assert!(!refused(2, 64, 128));
        assert!(refused(1, 64, 128) && refused(2, 63, 128) && refused(2, 64, 127));
    }
}

fn words(g: &mut SplitMix64, len: usize, fill: u64) -> Vec<u64> {
    (0..len)
        .map(|_| match fill {
            0 => 0,
            1 => u64::MAX,
            _ => g.next_u64(),
        })
        .collect()
}

/// The definition, bit by bit.
fn naive_bit_distance(code: &[u64], query: &[u64]) -> u32 {
    let bit = |words: &[u64], i: usize| (words[i / 64] >> (i % 64)) as u32 & 1;
    let n = code.len() * 64;
    let slice = |s: usize| (0..n).map(move |i| (bit(code, i) ^ bit(query, s * n + i)) << s);
    (0..SLICES).flat_map(slice).sum()
}

#[test]
fn bit_distances_are_equal_at_every_level() {
    prop::check("simd bit distances", |g: &mut Gen| {
        let w = if g.chance(1, 16) {
            160
        } else {
            g.below(33) as usize
        };
        let (entries, offset) = (g.below(6) as usize, g.below(8) as usize);
        let (fill_code, fill_query) = (g.below(4), g.below(4));
        let mut rng = SplitMix64::new(g.u64());
        let codes = words(&mut rng, offset + entries * w, fill_code);
        let query = words(&mut rng, SLICES * w, fill_query);
        let codes = &codes[offset..];
        let want: Vec<u32> = (0..entries)
            .map(|e| naive_bit_distance(&codes[e * w..(e + 1) * w], &query))
            .collect();
        for k in levels() {
            let mut got = vec![u32::MAX; entries];
            k.bit_distances(codes, &query, &mut got);
            if got != want {
                return Err(format!("{k:?} w={w}: {got:?}, want {want:?}"));
            }
        }
        Ok(())
    });
}

#[test]
fn code_dots_are_equal_at_every_level() {
    prop::check("simd code dot", |g: &mut Gen| {
        let w = if g.chance(1, 16) {
            160
        } else {
            g.below(33) as usize
        };
        let (offset, fill, extreme) = (g.below(8) as usize, g.below(4), g.bool());
        let mut rng = SplitMix64::new(g.u64());
        let bits = words(&mut rng, w, fill);
        let ext: Vec<u8> = words(&mut rng, offset + w * 4, fill)
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect();
        let ext = &ext[offset..offset + w * 32];
        let draw = |r: u64| {
            if extreme {
                [-128, 127][r as usize & 1]
            } else {
                r as i8
            }
        };
        let query: Vec<i8> = (0..w * 64).map(|_| draw(rng.next_u64())).collect();
        let want: i32 = (0..w * 64)
            .map(|i| {
                let nibble = if i % 2 == 0 {
                    ext[i / 2] & 15
                } else {
                    ext[i / 2] >> 4
                };
                let bit = if bits[i / 64] & 1 << (i % 64) == 0 {
                    0
                } else {
                    16
                };
                (bit + i32::from(nibble)) * i32::from(query[i])
            })
            .sum();
        for k in levels() {
            let got = k.code_dot(&bits, ext, &query);
            if got != want {
                return Err(format!("{k:?} w={w}: {got}, want {want}"));
            }
        }
        Ok(())
    });
}

#[test]
fn hadamard_is_the_transform_and_bit_identical_at_every_level() {
    prop::check("simd hadamard", |g: &mut Gen| {
        let n = 1usize << g.below(10);
        let offset = g.below(16) as usize;
        let mut rng = SplitMix64::new(g.u64());
        let x = values(&mut rng, n, (-2, 1));
        let total: f64 = x.iter().map(|&v| f64::from(v).abs()).sum();
        let mut want = x.clone();
        Kernels::at(Level::Scalar)
            .ok_or("no scalar")?
            .hadamard(&mut want);
        for (i, &got) in want.iter().enumerate() {
            let sign = |j: usize| {
                if (i & j).count_ones().is_multiple_of(2) {
                    1.0
                } else {
                    -1.0
                }
            };
            let naive: f64 = x
                .iter()
                .enumerate()
                .map(|(j, &v)| sign(j) * f64::from(v))
                .sum();
            if (f64::from(got) - naive).abs() > 1e-6 * total {
                return Err(format!("n={n} i={i}: {got}, definition {naive}"));
            }
        }
        for k in levels() {
            let mut got = vec![0.0; offset + n];
            got[offset..].copy_from_slice(&x);
            k.hadamard(&mut got[offset..]);
            if got[offset..]
                .iter()
                .zip(&want)
                .any(|(a, b)| a.to_bits() != b.to_bits())
            {
                return Err(format!("{k:?} n={n} differs from the scalar level"));
            }
        }
        Ok(())
    });
}
