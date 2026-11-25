//! Adversarial tests of the quantization module: exactness (VEC-G05) and no panic on any legal
//! stored vector (VEC-G11). Written by someone who did not write the module.

use std::panic::{AssertUnwindSafe, catch_unwind};

use nabedra_check::rng::SplitMix64;
use nabedra_simd::{Kernels, Level, wide};
use nabedra_vector::quant::{Estimate, Op, bounds, exact};

const OPS: [Op; 3] = [Op::Cosine, Op::Dot, Op::L2];

pub fn uniform(rng: &mut SplitMix64) -> f64 {
    (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

pub fn gaussian(rng: &mut SplitMix64) -> f64 {
    (-2.0 * (1.0 - uniform(rng)).ln()).sqrt() * (std::f64::consts::TAU * uniform(rng)).cos()
}

fn panics<T>(f: impl FnOnce() -> T) -> bool {
    catch_unwind(AssertUnwindSafe(f)).is_err()
}

fn true_cosine(a: &[f32], b: &[f32]) -> f64 {
    wide::dot(a, b) / (wide::norm_sq(a) * wide::norm_sq(b)).sqrt()
}

// ---------------------------------------------------------------------------------------------
// VEC-G11 and decision L2: a stored vector of any value is legal input.

/// Decision L2: `exact` must not panic on a zero vector for cosine.
#[test]
fn l2_exact_cosine_of_a_zero_vector_does_not_panic() {
    for dim in [1, 2, 7, 64, 100, 4096] {
        let zero = vec![0.0f32; dim];
        let one = vec![1.0f32; dim];
        let tiny = vec![1e-45f32; dim];
        for (a, b) in [
            (&zero, &one),
            (&one, &zero),
            (&zero, &zero),
            (&zero, &tiny),
            (&tiny, &zero),
        ] {
            assert!(
                !panics(|| exact(Op::Cosine, a, b)),
                "exact(Cosine) panicked on a zero vector, dim {dim}"
            );
        }
    }
}

/// Dot and L2 of zero vectors are plain numbers and must stay so.
#[test]
fn zero_vectors_have_plain_dot_and_l2() {
    for dim in [1, 5, 64, 300] {
        let zero = vec![0.0f32; dim];
        let v: Vec<f32> = (0..dim).map(|i| i as f32 - 2.5).collect();
        assert_eq!(exact(Op::Dot, &zero, &v), Some(0.0));
        let l2 = f64::from(exact(Op::L2, &zero, &v).unwrap());
        let want = wide::norm_sq(&v).sqrt();
        assert!((l2 - want).abs() <= 1e-6 * want, "{l2} {want}");
        assert_eq!(exact(Op::L2, &zero, &zero), Some(0.0));
    }
}

/// Values at the edges of f32 in every legal position: no op may give NaN, and an infinity only
/// where `saturate` keeps it finite (it never returns one).
#[test]
fn exact_is_finite_for_every_finite_input() {
    let edges = [
        0.0f32,
        -0.0,
        1e-45,
        -1e-45,
        1e-38,
        1.1754944e-38,
        1e-30,
        1e-20,
        1.0,
        -1.0,
        1e20,
        1e30,
        1e38,
        f32::MAX,
        f32::MIN,
        f32::MIN_POSITIVE,
    ];
    let mut rng = SplitMix64::new(11);
    for dim in [1usize, 2, 3, 8, 9, 16, 17, 63, 64, 65, 100, 4096] {
        for _ in 0..60 {
            let pick = |rng: &mut SplitMix64| -> Vec<f32> {
                let mode = rng.next_u64() % 3;
                (0..dim)
                    .map(|_| match mode {
                        0 => edges[rng.next_u64() as usize % edges.len()],
                        1 => edges[(rng.next_u64() % 4) as usize], // zero or subnormal
                        _ => {
                            if rng.next_u64().is_multiple_of(2) {
                                f32::MAX
                            } else {
                                -f32::MAX
                            }
                        }
                    })
                    .collect()
            };
            let (a, b) = (pick(&mut rng), pick(&mut rng));
            for op in OPS {
                let zero = |v: &[f32]| wide::norm_sq(v) == 0.0;
                if op == Op::Cosine && (zero(&a) || zero(&b)) {
                    continue; // L2 above
                }
                let score = exact(op, &a, &b).unwrap();
                assert!(score.is_finite(), "{op:?} dim {dim} gave {score}");
                if op == Op::Cosine {
                    assert!((-1.0..=1.0).contains(&score), "cosine {score}");
                }
            }
        }
    }
}

/// `exact` agrees with the f64 value to f32 precision at each magnitude, including mixed
/// magnitudes in one vector (terms that underflow next to terms that do not).
#[test]
fn exact_matches_f64_on_mixed_magnitudes() {
    let mut rng = SplitMix64::new(5);
    for dim in [1usize, 3, 8, 17, 64, 100, 1000, 4096] {
        for _ in 0..80 {
            let scale = 10f64.powi((rng.next_u64() % 61) as i32 - 30);
            let mk = |rng: &mut SplitMix64| -> Vec<f32> {
                (0..dim)
                    .map(|_| {
                        let e = 10f64.powi((rng.next_u64() % 21) as i32 - 10);
                        ((gaussian(rng) * scale * e) as f32).clamp(f32::MIN, f32::MAX)
                    })
                    .collect()
            };
            let (a, b) = (mk(&mut rng), mk(&mut rng));
            if wide::norm_sq(&a) == 0.0 || wide::norm_sq(&b) == 0.0 {
                continue;
            }
            let (na, nb) = (wide::norm_sq(&a).sqrt(), wide::norm_sq(&b).sqrt());
            let cos = exact(Op::Cosine, &a, &b).unwrap();
            assert!(
                (f64::from(cos) - true_cosine(&a, &b)).abs() <= 1e-5,
                "cos dim {dim}: {cos} {} a {a:?} b {b:?}",
                true_cosine(&a, &b)
            );
            let dot = f64::from(exact(Op::Dot, &a, &b).unwrap());
            let want = wide::dot(&a, &b);
            let fit = want.abs() < f64::from(f32::MAX);
            if fit {
                assert!(
                    (dot - want).abs() <= 2e-5 * na * nb + 1e-44,
                    "dot dim {dim}: {dot} {want}"
                );
            }
            let l2 = f64::from(exact(Op::L2, &a, &b).unwrap());
            let want = wide::l2_sq(&a, &b).sqrt();
            if want < f64::from(f32::MAX) {
                assert!(
                    (l2 - want).abs() <= 2e-5 * want.max(1e-300) + 1e-44,
                    "l2 dim {dim}"
                );
            }
        }
    }
}

/// `bounds` is documented to hold the score of `exact` whenever the cosine estimate is exact.
/// So the rounding of the kernels must stay inside `ROUNDING` (8e-6) at every dimension up to the
/// largest a vector can have (4,096, and 10,240 for the multi-vector encoding). Inputs where each
/// accumulator lane starts with a large term and then adds many terms below half its ulp: every
/// such term is lost, the worst case of the stated accuracy.
fn lost_terms(dim: usize, lanes: usize, small: f32) -> (Vec<f32>, Vec<f32>) {
    let a: Vec<f32> = (0..dim)
        .map(|i| if i < lanes { 1.0 } else { small })
        .collect();
    (a.clone(), a)
}

fn assert_exact_inside_bounds(a: &[f32], b: &[f32], what: &str) {
    let (la, lb) = (
        wide::norm_sq(a).sqrt() as f32,
        wide::norm_sq(b).sqrt() as f32,
    );
    let cos = true_cosine(a, b) as f32;
    let est = Estimate { cos, err: 0.0 };
    for op in OPS {
        let (low, high) = bounds(op, est, la, lb);
        let score = exact(op, a, b).unwrap();
        assert!(
            low <= score && score <= high,
            "{what}: {op:?} exact {score} outside bounds ({low}, {high}) for the true cosine {cos}"
        );
    }
}

fn exact_inside_bounds_at(dims: &[usize]) {
    let mut rng = SplitMix64::new(3);
    for &dim in dims {
        // Positive, equal, constant.
        let c = vec![0.37f32; dim];
        assert_exact_inside_bounds(&c, &c, &format!("constant {dim}"));
        // Positive with a spread: rounding all one way.
        for lanes in [16usize, 32, 64, 128] {
            for small in [2f32.powi(-12), 2f32.powi(-13), 2f32.powi(-14)] {
                let (a, _) = lost_terms(dim, lanes, small);
                let b: Vec<f32> = a.iter().map(|x| x * 1.0000001).collect();
                assert_exact_inside_bounds(&a, &b, &format!("lost terms {dim} {lanes} {small}"));
                // A second vector whose products are the lost ones, norms are not.
                let b: Vec<f32> = (0..dim)
                    .map(|i| if i < lanes { 1.0 } else { small * small * 0.5 })
                    .collect();
                assert_exact_inside_bounds(&a, &b, &format!("lost products {dim} {lanes} {small}"));
            }
        }
        for _ in 0..20 {
            let a: Vec<f32> = (0..dim).map(|_| uniform(&mut rng) as f32 + 0.5).collect();
            let b: Vec<f32> = (0..dim).map(|_| uniform(&mut rng) as f32 + 0.5).collect();
            assert_exact_inside_bounds(&a, &b, &format!("uniform positive {dim}"));
        }
    }
}

/// The same measure at each level of the kernels that this CPU has, against the f64 sums: the
/// error of one `dot` or `norm_sq`, relative to `|a||b|`, must stay inside `ROUNDING` (8e-6), the
/// slack of `bounds` that stands for it, whichever CPU a replica runs on.
fn worst_dot_error(k: Kernels, dim: usize) -> (f64, String) {
    let mut worst = (0.0f64, String::new());
    for lanes in [4usize, 8, 16, 32, 64, 128] {
        for small in [
            2f32.powi(-11),
            2f32.powi(-12),
            2f32.powi(-13),
            2f32.powi(-14),
        ] {
            let (a, _) = lost_terms(dim, lanes, small);
            let b: Vec<f32> = (0..dim)
                .map(|i| if i < lanes { 1.0 } else { small })
                .collect();
            let want = wide::dot(&a, &b);
            let na = wide::norm_sq(&a).sqrt() * wide::norm_sq(&b).sqrt();
            let err = (f64::from(k.dot(&a, &b)) - want).abs() / na;
            if err > worst.0 {
                worst = (err, format!("dim {dim} lanes {lanes} small {small}"));
            }
        }
    }
    worst
}

fn dot_error_inside_slack(dims: &[usize]) {
    let mut failures = Vec::new();
    for level in Level::ALL {
        let Some(k) = Kernels::at(level) else {
            continue;
        };
        for &dim in dims {
            let (worst, at) = worst_dot_error(k, dim);
            println!("{level:?}: worst relative dot error {worst:e} at {at}");
            if worst > 8e-6 {
                failures.push(format!("{level:?} {worst:e} at {at}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The largest dense vector of the contract.
#[test]
fn every_level_keeps_dot_error_inside_the_slack_of_bounds_at_4096() {
    dot_error_inside_slack(&[1000, 4096]);
}

/// The MUVERA encoding is a dense vector of 10,240 values (plan section 5).
#[test]
fn every_level_keeps_dot_error_inside_the_slack_of_bounds_at_10240() {
    dot_error_inside_slack(&[10_240]);
}

#[test]
fn exact_scores_stay_inside_bounds_of_the_true_cosine_up_to_4096() {
    exact_inside_bounds_at(&[1024, 4096]);
}

#[test]
fn exact_scores_stay_inside_bounds_of_the_true_cosine_at_10240() {
    exact_inside_bounds_at(&[10_240]);
}

/// The cosine of `exact`, computed at one level.
fn cosine_at(k: Kernels, a: &[f32], b: &[f32]) -> f64 {
    f64::from(k.dot(a, b)) / (f64::from(k.norm_sq(a)) * f64::from(k.norm_sq(b))).sqrt()
}

/// Searches products that are lost to the dot's accumulators while the norms keep their terms (or
/// the reverse): the three rounding errors of a cosine then add up instead of cancelling.
#[test]
fn every_level_keeps_cosine_error_inside_rounding_at_4096() {
    let mut failures = Vec::new();
    for level in Level::ALL {
        let Some(k) = Kernels::at(level) else {
            continue;
        };
        let mut worst = (0.0f64, String::new());
        for dim in [2048usize, 4096] {
            for lanes in [8usize, 16, 32, 64] {
                for ea in 6..=16i32 {
                    for eb in 6..=26i32 {
                        let (xa, xb) = (2f32.powi(-ea), 2f32.powi(-eb));
                        let a: Vec<f32> =
                            (0..dim).map(|i| if i < lanes { 1.0 } else { xa }).collect();
                        let b: Vec<f32> =
                            (0..dim).map(|i| if i < lanes { 1.0 } else { xb }).collect();
                        let err = (cosine_at(k, &a, &b) - true_cosine(&a, &b)).abs();
                        if err > worst.0 {
                            worst = (
                                err,
                                format!("dim {dim} lanes {lanes} xa 2^-{ea} xb 2^-{eb}"),
                            );
                        }
                    }
                }
            }
        }
        println!("{level:?}: worst cosine error {:e} at {}", worst.0, worst.1);
        if worst.0 > 8e-6 {
            failures.push(format!("{level:?} {:e} at {}", worst.0, worst.1));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
