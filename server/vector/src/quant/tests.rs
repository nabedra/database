use std::panic::{AssertUnwindSafe, catch_unwind};

use nabedra_check::{r#gen::Gen, prop, rng::SplitMix64};
use nabedra_simd::{Kernels, Level, wide};

use super::*;

fn uniform(rng: &mut SplitMix64) -> f64 {
    (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

fn gaussian(rng: &mut SplitMix64) -> f64 {
    (-2.0 * (1.0 - uniform(rng)).ln()).sqrt() * (std::f64::consts::TAU * uniform(rng)).cos()
}

/// `center + noise`, where the noise has length about `noise` whatever the dimension.
fn near(center: &[f32], noise: f64, rng: &mut SplitMix64) -> Vec<f32> {
    let noise = noise / (center.len() as f64).sqrt();
    center
        .iter()
        .map(|&c| (f64::from(c) + noise * gaussian(rng)) as f32)
        .collect()
}

fn unit(v: &[f32]) -> Vec<f32> {
    let mut out = vec![0.0; v.len()];
    direction(v, &mut out).expect("a nonzero vector");
    out
}

fn random_unit(dim: usize, rng: &mut SplitMix64) -> Vec<f32> {
    unit(&near(&vec![0.0; dim], 1.0, rng))
}

fn rotated(rotation: &Rotation, v: &[f32]) -> Vec<f32> {
    let mut out = vec![f32::NAN; rotation.padded()];
    rotation.apply(v, &mut out);
    out
}

fn one_hot(dim: usize, i: usize) -> Vec<f32> {
    (0..dim).map(|j| if i == j { 1.0 } else { 0.0 }).collect()
}

/// The true cosine of two vectors in f64.
fn cosine(a: &[f32], b: &[f32]) -> f64 {
    wide::dot(a, b) / (wide::norm_sq(a) * wide::norm_sq(b)).sqrt()
}

fn refuses<T>(f: impl FnOnce() -> T) -> bool {
    catch_unwind(AssertUnwindSafe(f)).is_err()
}

// Rotation.

#[test]
fn rotation_preserves_lengths_and_inner_products() {
    for dim in (1..=300).chain([768, 1024, 1536, 3072, 4096, 10_240]) {
        let mut rng = SplitMix64::new(dim as u64);
        let rotation = Rotation::new(dim, rng.next_u64());
        assert_eq!(rotation.padded(), dim.div_ceil(64) * 64);
        for length in [1e-12, 1.0, 1e12] {
            let a = near(&vec![0.0; dim], length, &mut rng);
            let b = near(&a, 0.5 * length, &mut rng);
            let (ra, rb) = (rotated(&rotation, &a), rotated(&rotation, &b));
            let (norm_a, norm_b) = (wide::norm_sq(&a), wide::norm_sq(&b));
            let levels = rotation.padded().trailing_zeros() as usize;
            let relative = 2.0 * (ROUNDS * (levels + 2)) as f64 * UNIT;
            let tolerance = 1.01 * relative * (norm_a * norm_b).sqrt();
            assert!(
                (wide::norm_sq(&ra) - norm_a).abs() <= 1.01 * relative * norm_a,
                "{dim}"
            );
            assert!(
                (wide::dot(&ra, &rb) - wide::dot(&a, &b)).abs() <= tolerance,
                "{dim}"
            );
        }
    }
}

/// Stored codes depend on these bits.
#[test]
fn rotation_gives_the_same_bits_forever() {
    let hash = |dim: usize, seed: u64| {
        let v: Vec<f32> = (0..dim).map(|i| (i as f32 + 1.0) / dim as f32).collect();
        let out = rotated(&Rotation::new(dim, seed), &v);
        out.iter()
            .flat_map(|x| x.to_le_bytes())
            .fold(0xcbf2_9ce4_8422_2325u64, |h, byte| {
                (h ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    };
    let golden = [
        hash(1, 7),
        hash(100, 7),
        hash(768, 7),
        hash(1024, 7),
        hash(768, 8),
    ];
    let want = [
        0x721b7738ec1c3138,
        0x212698a3fb04669e,
        0x1b7c7bf12994c3f4,
        0x9d2ed9d15efe1aa0,
        0x1cebb2ac34155704,
    ];
    assert_eq!(golden, want, "{golden:#x?}");
}

/// A direction must not stay concentrated where it was: over seeds, a single value spreads its
/// energy evenly over the output, also when the transform covers only a window of it.
#[test]
fn rotation_spreads_every_value_over_the_whole_output() {
    for dim in [64, 768, 1088, 1536] {
        for i in [0, 1, dim / 3, dim / 2, dim - 1] {
            let mut energy = [0.0; 4];
            const SEEDS: u64 = 400;
            for seed in 0..SEEDS {
                let out = rotated(&Rotation::new(dim, seed), &one_hot(dim, i));
                let largest = out.iter().fold(0.0f32, |max, x| max.max(x.abs()));
                assert!(
                    largest < 6.5 / (dim as f32).sqrt(),
                    "dim {dim} value {i} seed {seed}"
                );
                for (k, part) in out.chunks(dim / 4).enumerate() {
                    energy[k] += 4.0 * wide::norm_sq(part) / SEEDS as f64;
                }
            }
            let even = energy.iter().all(|e| (e - 1.0).abs() < 0.08);
            assert!(even, "dim {dim} value {i}: {energy:?}");
        }
    }
}

#[test]
fn rotation_checks_its_sizes() {
    assert!(refuses(|| Rotation::new(0, 1)));
    assert!(refuses(|| Rotation::new(MAX_DIM + 1, 1)));
    let rotation = Rotation::new(100, 1);
    assert!(refuses(|| rotation.apply(&[0.0; 99], &mut [0.0; 128])));
    assert!(refuses(|| rotation.apply(&[0.0; 100], &mut [0.0; 100])));
}

// Codes and estimates.

/// Replicas on different CPUs hold the same codes and give the same estimates.
#[test]
fn estimates_are_the_same_bits_on_every_cpu() {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for dim in [1, 64, 100, 768, 1088, 4096] {
        let mut rng = SplitMix64::new(dim as u64);
        let rotation = Rotation::new(dim, 5);
        // Plain arithmetic: `ln` and `cos` are not the same on every platform.
        let mut draw = || -> Vec<f32> {
            let v: Vec<f32> = (0..dim).map(|_| uniform(&mut rng) as f32 - 0.5).collect();
            rotated(&rotation, &unit(&v))
        };
        let (centroid, query) = (draw(), draw());
        let mut block = CodeBlock::new(rotation.padded());
        (0..5).for_each(|_| block.push(&draw(), &centroid, 1.0));
        let code = QueryCode::new(&query, &centroid);
        let mut scanned = Vec::new();
        code.scan(&block, &mut scanned);
        for (i, est) in scanned.into_iter().enumerate() {
            let refined = code.refine(&block, i);
            for x in [est.cos, est.err, refined.cos, refined.err] {
                hash = (hash ^ u64::from(x.to_bits())).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
    }
    assert_eq!(hash, 0x8b53_6273_c5a8_0191, "{hash:#x}");
}

/// Residuals as a rotation gives them, and with the shortest and with long tails: the chosen scale
/// is close to the best of a fine search.
#[test]
fn extension_scale_is_close_to_the_best_one() {
    let mut rng = SplitMix64::new(8);
    type Draw = fn(&mut SplitMix64) -> f64;
    let tails: [(&str, Draw); 3] = [
        ("gaussian", gaussian),
        ("uniform", |rng| 2.0 * uniform(rng) - 1.0),
        ("long", |rng| gaussian(rng).powi(3)),
    ];
    for (name, draw) in tails {
        let residual: Vec<f32> = (0..768).map(|_| draw(&mut rng) as f32).collect();
        let r2 = wide::norm_sq(&residual);
        let rms = (r2 / 768.0).sqrt();
        let loss = |scale: f32| 1.0 - code::Fit::new(&residual, scale).cosine2(r2);
        let fine = (1..2000).map(|i| loss((16.0 / (0.01 * f64::from(i) * rms)) as f32));
        let best = fine.fold(1.0, f64::min);
        let got = loss(code::best_scale(&residual, r2));
        assert!(got <= 1.15 * best, "{name}: {got}, best {best}");
    }
}

/// Rounded to the nearest, a value is off by a uniform part of a step, so its square by 1/12.
#[test]
fn query_levels_are_the_nearest() {
    let mut rng = SplitMix64::new(9);
    for dim in [768, 4096] {
        let (q, c) = (random_unit(dim, &mut rng), random_unit(dim, &mut rng));
        let ratio = QueryCode::new(&q, &c).error8_in_steps();
        assert!((ratio * 12.0 - 1.0).abs() < 0.1, "{dim}: {ratio}");
    }
}

/// Estimates of many pairs over many rotations, against the true cosine.
#[derive(Default)]
struct Coverage {
    pairs: f64,
    above: f64,
    below: f64,
    /// Sum and sum of squares of the error in units of the stated deviation.
    sum: f64,
    squares: f64,
    err: f64,
}

impl Coverage {
    fn add(&mut self, est: Estimate, truth: f64) {
        let error = f64::from(est.cos) - truth;
        let err = f64::from(est.err);
        assert!(err.is_finite() && err > 0.0 && error.is_finite());
        self.pairs += 1.0;
        self.above += f64::from(u8::from(-error > err));
        self.below += f64::from(u8::from(error > err));
        let deviations = error * f64::from(EPSILON) / err;
        self.sum += deviations;
        self.squares += deviations * deviations;
        self.err += err;
    }

    fn check(&self, what: &str) {
        let n = self.pairs;
        let (above, below) = (self.above / n, self.below / n);
        let (mean, deviation) = (self.sum / n, (self.squares / n).sqrt());
        let report = format!(
            "{what}: {n} pairs, above {above:.4}, below {below:.4}, mean {mean:.4}, \
             deviation {deviation:.3}, err {:.4}",
            self.err / n
        );
        println!("{report}");
        assert!(1.0 - above - below >= COVERAGE, "{report}");
        assert!(
            above.max(below) <= (1.0 - COVERAGE) / 2.0 + 0.005,
            "{report}"
        );
        // Unbiased, and not a bound that holds because it is vague.
        assert!(mean.abs() < 0.07, "{report}");
        assert!(deviation > 0.5, "{report}");
        assert!(above + below > 0.005, "{report}");
    }
}

/// Draws a centroid, a query and entries, all unit vectors.
type Sampler = fn(usize, &mut SplitMix64) -> (Vec<f32>, Vec<f32>, Vec<Vec<f32>>);

fn independent(dim: usize, rng: &mut SplitMix64) -> (Vec<f32>, Vec<f32>, Vec<Vec<f32>>) {
    let entries = (0..16).map(|_| random_unit(dim, rng)).collect();
    (random_unit(dim, rng), random_unit(dim, rng), entries)
}

/// Entries near the centroid, queries near an entry, and residuals in few directions.
fn clustered(dim: usize, rng: &mut SplitMix64) -> (Vec<f32>, Vec<f32>, Vec<Vec<f32>>) {
    let center = random_unit(dim, rng);
    let basis: Vec<Vec<f32>> = (0..8).map(|_| random_unit(dim, rng)).collect();
    let entries: Vec<Vec<f32>> = (0..16)
        .map(|_| {
            let mut v = near(&center, 0.2, rng);
            for axis in &basis {
                let weight = 0.3 * gaussian(rng) as f32;
                v.iter_mut().zip(axis).for_each(|(x, a)| *x += weight * a);
            }
            unit(&v)
        })
        .collect();
    let query = unit(&near(&entries[0], 0.4, rng));
    (center, query, entries)
}

/// Everything on a few axes: the rotation has all the work.
fn axes(dim: usize, rng: &mut SplitMix64) -> (Vec<f32>, Vec<f32>, Vec<Vec<f32>>) {
    let mut sparse = || {
        let mut v = one_hot(dim, 0);
        for _ in 0..3 {
            v[rng.next_u64() as usize % dim] += [-1.0, 1.0, 2.0][rng.next_u64() as usize % 3];
        }
        v[0] += 0.5;
        unit(&v)
    };
    let entries = (0..16).map(|_| sparse()).collect();
    (sparse(), sparse(), entries)
}

#[test]
fn true_cosine_lies_inside_the_stated_bound() {
    let samplers: [(&str, Sampler); 3] = [
        ("independent", independent),
        ("clustered", clustered),
        ("axes", axes),
    ];
    for dim in [50, 64, 200, 768] {
        for (name, sample) in samplers {
            let (mut bit1, mut bit5) = (Coverage::default(), Coverage::default());
            for case in 0..100 {
                let mut rng = SplitMix64::new(case * 1000 + dim as u64);
                let (centroid, query, entries) = sample(dim, &mut rng);
                let rotation = Rotation::new(dim, rng.next_u64());
                let mut block = CodeBlock::new(rotation.padded());
                let centroid = rotated(&rotation, &centroid);
                for entry in &entries {
                    block.push(&rotated(&rotation, entry), &centroid, 1.0);
                }
                let code = QueryCode::new(&rotated(&rotation, &query), &centroid);
                let mut estimates = vec![
                    Estimate {
                        cos: f32::NAN,
                        err: 0.0
                    };
                    3
                ];
                code.scan(&block, &mut estimates);
                assert_eq!(estimates.len(), entries.len());
                for (i, (entry, est)) in entries.iter().zip(estimates).enumerate() {
                    let (truth, refined) = (cosine(entry, &query), code.refine(&block, i));
                    bit1.add(est, truth);
                    bit5.add(refined, truth);
                    assert!(refined.err <= est.err, "{refined:?} {est:?}");
                }
            }
            bit1.check(&format!("{name} {dim} 1-bit"));
            bit5.check(&format!("{name} {dim} 5-bit"));
            assert!(
                bit5.err < 0.15 * bit1.err,
                "{name} {dim}: {} {}",
                bit5.err,
                bit1.err
            );
        }
    }
}

/// Rounding the query must not depend on the code it meets, or the estimate of a near duplicate is
/// always too high.
#[test]
fn code_of_the_query_itself_is_inside_the_bound() {
    const SEEDS: u64 = 40;
    for dim in [64, 768, 4096, 10_240] {
        let mut mean = [0.0f32; 2];
        for seed in 0..SEEDS {
            let mut rng = SplitMix64::new(seed);
            let rotation = Rotation::new(dim, seed);
            let centroid = rotated(&rotation, &random_unit(dim, &mut rng));
            let dir = rotated(&rotation, &random_unit(dim, &mut rng));
            let mut block = CodeBlock::new(rotation.padded());
            block.push(&dir, &centroid, 1.0);
            let code = QueryCode::new(&dir, &centroid);
            let mut scanned = Vec::new();
            code.scan(&block, &mut scanned);
            for (mean, est) in mean.iter_mut().zip([scanned[0], code.refine(&block, 0)]) {
                // The code itself is exact here, so only the rounding of the query is left.
                let error = (est.cos - 1.0) / est.err;
                assert!(error.abs() <= 0.75, "dim {dim}: {est:?}");
                *mean += error / SEEDS as f32;
            }
        }
        assert!(mean.iter().all(|m| m.abs() < 0.1), "dim {dim}: {mean:?}");
    }
}

/// Where nothing is estimated, only rounding is left.
#[test]
fn equal_directions_leave_only_rounding() {
    for dim in [1, 3, 64, 100, 768, 4096, 10_240, MAX_DIM] {
        let mut rng = SplitMix64::new(dim as u64);
        let rotation = Rotation::new(dim, 11);
        let (a, b) = (random_unit(dim, &mut rng), random_unit(dim, &mut rng));
        let (ra, rb) = (rotated(&rotation, &a), rotated(&rotation, &b));
        // Entry, centroid and query. In the first two, the entry or the query is the centroid.
        for (entry, centroid, query, truth) in [
            (&ra, &ra, &rb, cosine(&a, &b)),
            (&rb, &ra, &ra, cosine(&a, &b)),
            (&ra, &ra, &ra, 1.0),
        ] {
            let mut block = CodeBlock::new(rotation.padded());
            block.push(entry, centroid, 1.0);
            let code = QueryCode::new(query, centroid);
            let mut scanned = Vec::new();
            code.scan(&block, &mut scanned);
            for est in [scanned[0], code.refine(&block, 0)] {
                assert!(
                    (f64::from(est.cos) - truth).abs() <= f64::from(est.err),
                    "{dim} {est:?}"
                );
                assert!(est.err < 4e-5, "{dim} {est:?}");
            }
        }
    }
}

/// A block after any pushes and removals answers as one that only ever held what is left.
#[test]
fn swap_remove_keeps_codes_and_lengths_aligned() {
    prop::check_with(
        prop::Config {
            cases: 64,
            ..prop::Config::default()
        },
        "quant swap_remove",
        |g: &mut Gen| {
            let dim = 1 + g.below(200) as usize;
            let mut rng = SplitMix64::new(g.u64());
            let rotation = Rotation::new(dim, rng.next_u64());
            let centroid = rotated(&rotation, &random_unit(dim, &mut rng));
            let mut block = CodeBlock::new(rotation.padded());
            let mut model: Vec<(Vec<f32>, f32)> = Vec::new();
            // More than one chunk of the scan.
            for _ in 0..g.below(120) {
                if !model.is_empty() && g.chance(1, 4) {
                    let i = g.below(model.len() as u64) as usize;
                    model.swap_remove(i);
                    block.swap_remove(i);
                } else {
                    let dir = rotated(&rotation, &random_unit(dim, &mut rng));
                    let len = 1.0 + model.len() as f32;
                    block.push(&dir, &centroid, len);
                    model.push((dir, len));
                }
            }
            let query = rotated(&rotation, &random_unit(dim, &mut rng));
            let code = QueryCode::new(&query, &centroid);
            let mut scanned = Vec::new();
            code.scan(&block, &mut scanned);
            if (block.len(), scanned.len()) != (model.len(), model.len()) {
                return Err(format!("{} entries, want {}", block.len(), model.len()));
            }
            for (i, (dir, len)) in model.iter().enumerate() {
                let mut alone = CodeBlock::new(rotation.padded());
                alone.push(dir, &centroid, *len);
                let mut want = Vec::new();
                code.scan(&alone, &mut want);
                let want = (want[0], code.refine(&alone, 0), *len);
                let got = (scanned[i], code.refine(&block, i), block.length(i));
                if got != want {
                    return Err(format!("entry {i}: {got:?}, want {want:?}"));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn preconditions_are_checked() {
    let a = one_hot(64, 0);
    let est = Estimate { cos: 0.0, err: 0.1 };
    for len in [0.0, -1.0, f32::NAN, f32::INFINITY, 1e-45] {
        assert!(refuses(|| CodeBlock::new(64).push(&a, &a, len)), "{len}");
        assert!(refuses(|| bounds(Op::Dot, est, len, 1.0)));
        assert!(refuses(|| bounds(Op::L2, est, 1.0, len)));
    }
    let (zero, long, nan) = (vec![0.0; 64], vec![1.0; 64], vec![f32::NAN; 64]);
    for bad in [&zero, &long, &nan, &one_hot(128, 0), &one_hot(32, 0)] {
        assert!(refuses(|| CodeBlock::new(64).push(bad, &a, 1.0)));
        assert!(refuses(|| CodeBlock::new(64).push(&a, bad, 1.0)));
        assert!(refuses(|| QueryCode::new(bad, &a)));
        assert!(refuses(|| QueryCode::new(&a, bad)));
    }
    for padded in [0, 63, 100, MAX_DIM + 64] {
        assert!(refuses(|| CodeBlock::new(padded)));
    }
    let code = QueryCode::new(&a, &a);
    assert!(refuses(|| code.scan(&CodeBlock::new(128), &mut Vec::new())));
    assert!(refuses(|| code.refine(&CodeBlock::new(64), 0)));
    assert!(refuses(|| CodeBlock::new(64).swap_remove(0)));
    for (x, y) in [(&zero, &a), (&a, &zero), (&zero, &zero)] {
        assert_eq!(exact(Op::Cosine, x, y), None);
        assert_eq!(score(Op::Cosine, x, y), None);
    }
}

// Scores.

/// The exact score in f64 of two vectors with these lengths and this cosine.
fn score_of(op: Op, cos: f64, o: f64, q: f64) -> f64 {
    match op {
        Op::Cosine => cos,
        Op::Dot => o * q * cos,
        Op::L2 => ((o - q).powi(2) + 2.0 * o * q * (1.0 - cos))
            .max(0.0)
            .sqrt(),
    }
}

const OPS: [Op; 3] = [Op::Cosine, Op::Dot, Op::L2];

/// A float in `[0, 1]`, often at its ends.
fn fraction(g: &mut Gen) -> f64 {
    match g.below(8) {
        0 => 0.0,
        1 => 1.0,
        _ => g.below(1 << 30) as f64 / f64::from(1 << 30),
    }
}

fn length(g: &mut Gen) -> f32 {
    10f64.powf(12.0 * fraction(g) - 6.0) as f32
}

#[test]
fn vs07_bounds_contain_the_exact_score() {
    let config = prop::Config {
        cases: 20_000,
        ..prop::Config::default()
    };
    prop::check_with(config, "quant bounds", |g: &mut Gen| {
        let (o, q) = (length(g), if g.chance(1, 4) { 1.0 } else { length(g) });
        let q = if g.chance(1, 4) { o } else { q };
        let cos = 2.0 * fraction(g) - 1.0;
        let err = (fraction(g) * 10f64.powf(-7.0 * fraction(g))) as f32;
        // Up to the very ends of the interval, where the rounded estimate still contains `cos`.
        let est = Estimate {
            cos: (cos + (2.0 * fraction(g) - 1.0) * f64::from(err)) as f32,
            err,
        };
        if (f64::from(est.cos) - cos).abs() > f64::from(err) {
            return Ok(());
        }
        let (fo, fq, slack) = (f64::from(o), f64::from(q), slack(MAX_DIM));
        for op in OPS {
            let (low, high) = bounds(op, est, o, q);
            let (flow, fhigh) = (f64::from(low), f64::from(high));
            let want = score_of(op, cos, fo, fq);
            if !(flow <= want && want <= fhigh) {
                return Err(format!(
                    "{op:?} {o} {q} {cos} {est:?}: {want} not in {low}..{high}"
                ));
            }
            // And nothing wider than the interval of the cosine explains.
            let ends = [-1.0, 1.0].map(|side| {
                let end = f64::from(est.cos) + side * (f64::from(err) + 2.0 * slack);
                score_of(op, end, fo, fq)
            });
            let width = (ends[1] - ends[0]).abs();
            let rounding = match op {
                Op::Cosine => 1e-6,
                Op::Dot => 1e-6 * fo * fq,
                // A squared distance of `slack * (o + q)^2` is the resolution of the codes.
                Op::L2 => (4.0 * slack).sqrt() * (fo + fq),
            };
            if fhigh - flow > width + rounding + 1e-44 {
                return Err(format!(
                    "{op:?} {o} {q} {est:?}: {low}..{high} wider than {width}"
                ));
            }
        }
        Ok(())
    });
}

/// The whole path on raw vectors of very different lengths.
#[test]
fn vs07_bounds_contain_the_score_of_raw_vectors() {
    prop::check("quant bounds end to end", |g: &mut Gen| {
        let dim = 1 + g.below(300) as usize;
        let mut rng = SplitMix64::new(g.u64());
        let (len_o, len_q) = (f64::from(length(g)), f64::from(length(g)));
        let base = random_unit(dim, &mut rng);
        let scaled = |v: &[f32], len: f64| -> Vec<f32> {
            v.iter().map(|&x| (f64::from(x) * len) as f32).collect()
        };
        let o = scaled(&base, len_o);
        let noise = [0.0, 1e-4, 1.0][g.below(3) as usize];
        let q = scaled(
            &near(&base, noise, &mut rng),
            if g.bool() { len_o } else { len_q },
        );
        let mut scratch = vec![0.0; dim];
        let (Some(len_o), Some(len_q)) = (direction(&o, &mut scratch), direction(&q, &mut scratch))
        else {
            return Ok(());
        };
        let est = Estimate {
            cos: cosine(&o, &q) as f32,
            err: 1e-7,
        };
        for op in OPS {
            let ((low, high), got) = (bounds(op, est, len_o, len_q), exact(op, &o, &q).unwrap());
            if !(low <= got && got <= high) {
                return Err(format!("{op:?}: {got} not in {low}..{high}"));
            }
        }
        Ok(())
    });
}

#[test]
fn bounds_saturate_instead_of_overflowing() {
    let est = Estimate { cos: 0.5, err: 0.1 };
    for op in OPS {
        let (low, high) = bounds(op, est, f32::MAX, f32::MAX);
        assert!(
            low.is_finite() && high.is_finite() && low <= high,
            "{op:?}: {low} {high}"
        );
    }
    assert_eq!(bounds(Op::Dot, est, 1e30, 1e30), (f32::MAX, f32::MAX));
    let est = Estimate { cos: 0.0, err: 5.0 };
    let (low, high) = bounds(Op::Cosine, est, 1.0, 1.0);
    assert_eq!((low, high), (-1.0, 1.0));
    assert_eq!(bounds(Op::L2, est, 3.0, 3.0).0, 0.0);
}

/// Against f64 sums, for magnitudes where the f32 sums overflow, vanish or cancel.
#[test]
fn vs07_exact_scores_at_every_magnitude() {
    prop::check("quant exact", |g: &mut Gen| {
        let dim = 1 + g.below(300) as usize;
        let magnitudes = [1.0f64, 1e-6, 1e6, 1e-21, 1e-30, 1e-38, 1e15, 1e20, 1e37];
        let (ma, mb) = (*g.pick(&magnitudes), *g.pick(&magnitudes));
        let mut rng = SplitMix64::new(g.u64());
        let mut draw = |m: f64| -> Vec<f32> {
            (0..dim)
                .map(|_| (m * (2.0 * uniform(&mut rng) - 1.0)) as f32)
                .collect()
        };
        let (a, b) = (draw(ma), draw(mb));
        let b = if g.chance(1, 8) { a.clone() } else { b };
        if wide::norm_sq(&a) == 0.0 || wide::norm_sq(&b) == 0.0 {
            return Ok(());
        }
        let (len_a, len_b) = (wide::norm_sq(&a).sqrt(), wide::norm_sq(&b).sqrt());
        let limit = f64::from(f32::MAX);
        let mut scratch = vec![0.0; dim];
        if let (Some(a_len), Some(b_len)) =
            (direction(&a, &mut scratch), direction(&b, &mut scratch))
        {
            for op in OPS {
                let (got, want) = (exact(op, &a, &b).unwrap(), score(op, &a, &b).unwrap());
                let (low, high) = exact_bounds(op, got, dim, a_len, b_len);
                if !(low <= want && want <= high) {
                    return Err(format!("{op:?}: {want} not in {low}..{high} of {got}"));
                }
            }
        }
        for (op, want, scale) in [
            (Op::Cosine, cosine(&a, &b), 1.0),
            (
                Op::Dot,
                wide::dot(&a, &b).clamp(-limit, limit),
                len_a * len_b,
            ),
            (Op::L2, wide::l2_sq(&a, &b).sqrt().min(limit), len_a + len_b),
        ] {
            let got = f64::from(exact(op, &a, &b).unwrap());
            if (got - want).abs() > 2e-5 * scale + 1e-41 {
                return Err(format!("{op:?} {ma} {mb} dim {dim}: {got}, want {want}"));
            }
        }
        if exact(Op::L2, &a, &a) != Some(0.0) || exact(Op::Cosine, &a, &a).unwrap() < 1.0 - 1e-6 {
            return Err("a vector differs from itself".into());
        }
        Ok(())
    });
    // Zero vectors rank under dot and L2 (they live in a list that is always scanned).
    assert_eq!(exact(Op::Dot, &[0.0, 0.0], &[3.0, 4.0]), Some(0.0));
    assert_eq!(exact(Op::L2, &[0.0, 0.0], &[3.0, 4.0]), Some(5.0));
    assert_eq!(exact(Op::Cosine, &[2.0, 0.0], &[0.0, 5.0]), Some(0.0));
    assert_eq!(exact(Op::Cosine, &[2.0, 0.0], &[-3.0, 0.0]), Some(-1.0));
}

/// Vectors that lose as many terms to rounding as the kernels can: the intervals of L1 still hold
/// the final score, up to the largest dimension, and are no wider than the stated slack.
#[test]
fn vs07_exact_bounds_contain_the_final_score_at_the_worst_rounding() {
    for dim in [64, 1000, 1024, 1025, 4096, 10_240, MAX_DIM] {
        for (lanes, e_a, e_b) in [
            (1, 6, 24),
            (16, 12, 12),
            (64, 12, 25),
            (64, 13, 13),
            (64, 8, 20),
        ] {
            let fill = |e: i32| -> Vec<f32> {
                let small = 2f32.powi(-e);
                (0..dim)
                    .map(|i| if i % 1024 < lanes { 1.0 } else { small })
                    .collect()
            };
            let (a, b) = (fill(e_a), fill(e_b));
            let len = |v: &[f32]| wide::norm_sq(v).sqrt() as f32;
            for op in OPS {
                let (got, want) = (exact(op, &a, &b).unwrap(), score(op, &a, &b).unwrap());
                let (low, high) = exact_bounds(op, got, dim, len(&a), len(&b));
                assert!(
                    low <= want && want <= high,
                    "{op:?} {dim}: {want} in {low}..{high}"
                );
                let unit = match op {
                    Op::Cosine => 1.0,
                    Op::Dot => len(&a) * len(&b),
                    Op::L2 => got,
                };
                let widest = 2.1 * slack(dim) * f64::from(unit) + 1e-40;
                assert!(
                    f64::from(high - low) <= widest,
                    "{op:?} {dim}: {low}..{high}"
                );
                assert!(slack(dim) < 4e-6);
            }
        }
    }
}

/// The slack must hold the worst cosine that kernel errors within `relative_error` allow: the dot
/// product off by `e |a| |b|` and both squared lengths by `e` of themselves, each the bad way.
#[test]
fn slack_holds_the_worst_kernel_errors() {
    for dim in [1, 64, 768, 1024, 4096, MAX_DIM] {
        let e = nabedra_simd::relative_error(dim);
        let mut worst = 0.0f64;
        for i in 0..=200 {
            let cos = f64::from(i) / 100.0 - 1.0;
            for (dot, norms) in [(-e, 1.0 + e), (e, 1.0 - e), (-e, 1.0 - e), (e, 1.0 + e)] {
                let got = ((cos + dot) / norms).clamp(-1.0, 1.0);
                worst = worst.max((got - cos).abs());
            }
        }
        // Plus the rounding of the result to f32.
        assert!(worst + UNIT <= slack(dim), "{dim}: {worst} {}", slack(dim));
        assert!(worst > 1.9 * e, "{dim}: the worst case needs both errors");
    }
}

#[test]
fn direction_has_unit_length_or_is_refused() {
    let mut out = [0.0; 3];
    for scale in [1e-37f32, 1e-6, 1.0, 1e6, 1e37] {
        let v = [3.0 * scale, 0.0, -4.0 * scale];
        let len = direction(&v, &mut out).expect("a length");
        assert!((len / (5.0 * scale) - 1.0).abs() < 1e-6, "{len}");
        assert!((out[0] - 0.6).abs() < 1e-6 && out[1] == 0.0 && (out[2] + 0.8).abs() < 1e-6);
    }
    for v in [[0.0; 3], [1e-45, 0.0, 0.0], [f32::MAX; 3]] {
        assert_eq!(direction(&v, &mut out), None, "{v:?}");
    }
}

// Local measurements, not gates: `cargo test --release -p nabedra-vector quant::tests::measure --
// --ignored --nocapture --test-threads=1`.

fn nanos(iterations: u64, mut f: impl FnMut()) -> f64 {
    let start = nabedra_rt::time::now_std();
    (0..iterations).for_each(|_| f());
    nabedra_rt::time::elapsed(start).as_nanos() as f64 / iterations as f64
}

#[test]
#[ignore = "measurement"]
fn measure_kernels() {
    use std::hint::black_box;
    let mut rng = SplitMix64::new(1);
    let (a, b) = (random_unit(768, &mut rng), random_unit(768, &mut rng));
    let mut words = |n: usize| -> Vec<u64> { (0..n).map(|_| rng.next_u64()).collect() };
    const CODES: usize = 512;
    let (codes, query) = (words(CODES * 12), words(48));
    let ext: Vec<u8> = words(48).iter().flat_map(|w| w.to_le_bytes()).collect();
    let levels: Vec<i8> = words(96)
        .iter()
        .flat_map(|w| w.to_le_bytes())
        .map(|b| b as i8)
        .collect();
    let mut distances = vec![0; CODES];
    let mut x = vec![1.0f32; 512];
    let mut block = CodeBlock::new(768);
    for _ in 0..CODES {
        block.push(&random_unit(768, &mut rng), &a, 1.0);
    }
    let (code, mut out) = (QueryCode::new(&b, &a), Vec::new());
    let scan = nanos(2000, || code.scan(black_box(&block), &mut out));
    println!("block scan {:.0} M codes/s", CODES as f64 / scan * 1e3);
    for k in Level::ALL.into_iter().filter_map(Kernels::at) {
        let dot = nanos(200_000, || {
            _ = black_box(k.dot(black_box(&a), black_box(&b)))
        });
        let l2 = nanos(200_000, || {
            _ = black_box(k.l2_sq(black_box(&a), black_box(&b)))
        });
        let scan = nanos(2000, || {
            k.bit_distances(black_box(&codes), &query, &mut distances)
        });
        let refine = nanos(200_000, || {
            black_box(k.code_dot(black_box(&codes[..12]), &ext, &levels));
        });
        let hadamard = nanos(20_000, || {
            k.hadamard(black_box(&mut x));
            x.iter_mut().for_each(|x| *x *= 0.04);
        });
        println!(
            "{:?}: 768-d dot {dot:.1} ns, l2 {l2:.1} ns, 1-bit {:.0} M codes/s, \
             5-bit code {refine:.1} ns, 512-point hadamard {hadamard:.0} ns",
            k.level(),
            CODES as f64 / scan * 1e3
        );
    }
}

/// The worst fixed pair over rotations: pairs on few axes are what a fast rotation hides worst.
#[test]
#[ignore = "measurement"]
fn measure_fixed_pairs() {
    const SEEDS: u64 = 1000;
    for dim in [64, 128, 192, 256, 320, 576, 1024, 1088, 4032] {
        let (mut worst, mut bias) = ([0.0f64; 2], [0.0f64; 2]);
        for case in 0..if dim < 600 { 40 } else { 8 } {
            let mut rng = SplitMix64::new(case);
            // Few axes whose numbers may cancel as bit patterns.
            let axes: Vec<usize> = match case % 4 {
                0 => vec![0, 1, 2],
                1 => vec![0, 1, 2, 3],
                2 => vec![0, 1, dim / 2, dim / 2 + 1],
                _ => (0..4).map(|_| rng.next_u64() as usize % dim).collect(),
            };
            let mut draw = || {
                let mut v = vec![0.0f32; dim];
                axes.iter().for_each(|&i| v[i] = gaussian(&mut rng) as f32);
                v[axes[rng.next_u64() as usize % axes.len()]] *= 100.0;
                unit(&v)
            };
            let (c, q, e) = (draw(), draw(), draw());
            let truth = cosine(&e, &q);
            let (mut miss, mut sum) = ([0.0; 2], [0.0; 2]);
            for seed in 0..SEEDS {
                let rotation = Rotation::new(dim, seed * 7919 + 1);
                let centroid = rotated(&rotation, &c);
                let mut block = CodeBlock::new(rotation.padded());
                block.push(&rotated(&rotation, &e), &centroid, 1.0);
                let code = QueryCode::new(&rotated(&rotation, &q), &centroid);
                let mut scanned = Vec::new();
                code.scan(&block, &mut scanned);
                for (stage, est) in [scanned[0], code.refine(&block, 0)].into_iter().enumerate() {
                    let error = f64::from(est.cos) - truth;
                    miss[stage] += f64::from(u8::from(error.abs() > f64::from(est.err)));
                    sum[stage] += error / f64::from(est.err) * f64::from(EPSILON);
                }
            }
            for stage in 0..2 {
                worst[stage] = worst[stage].max(miss[stage] / SEEDS as f64);
                bias[stage] = bias[stage].max((sum[stage] / SEEDS as f64).abs());
            }
        }
        println!("{dim}: worst misses {worst:.3?}, largest mean {bias:.3?}");
    }
}

#[test]
#[ignore = "measurement"]
fn measure_recall() {
    const DIM: usize = 768;
    const CLUSTERS: usize = 400;
    const ROWS: usize = 250;
    const QUERIES: usize = 100;
    let mut rng = SplitMix64::new(42);
    let sample = |center: &[f32], axes: &[Vec<f32>], rng: &mut SplitMix64| {
        let mut v = near(center, 0.4, rng);
        for axis in axes {
            let weight = 0.1 * gaussian(rng) as f32;
            v.iter_mut().zip(axis).for_each(|(x, a)| *x += weight * a);
        }
        unit(&v)
    };
    let rotation = Rotation::new(DIM, 9);
    let mut parts = Vec::new();
    for _ in 0..CLUSTERS {
        let center = random_unit(DIM, &mut rng);
        let axes: Vec<Vec<f32>> = (0..32).map(|_| random_unit(DIM, &mut rng)).collect();
        let rows: Vec<Vec<f32>> = (0..ROWS)
            .map(|_| sample(&center, &axes, &mut rng))
            .collect();
        parts.push((center, axes, rows));
    }
    let queries: Vec<Vec<f32>> = (0..QUERIES)
        .map(|i| {
            sample(
                &parts[i * CLUSTERS / QUERIES].0,
                &parts[i * CLUSTERS / QUERIES].1,
                &mut rng,
            )
        })
        .collect();

    let rows = (CLUSTERS * ROWS) as f64;
    let mut coded = Vec::new();
    let mut out = vec![0.0; rotation.padded()];
    let start = nabedra_rt::time::now_std();
    for (center, _, rows) in &parts {
        let (center, mut block) = (
            rotated(&rotation, center),
            CodeBlock::new(rotation.padded()),
        );
        for row in rows {
            rotation.apply(row, &mut out);
            block.push(&out, &center, 1.0);
        }
        coded.push((center, block));
    }
    let encode = nabedra_rt::time::elapsed(start).as_secs_f64() * 1e6 / rows;

    const K: usize = 10;
    // `(score, partition, row)`, best first.
    let top = |mut list: Vec<(f32, usize, usize)>| {
        list.sort_by(|a, b| b.0.total_cmp(&a.0));
        list.truncate(K);
        list
    };
    let (mut hits1, mut hits5, mut hits_staged) = (0, 0, 0);
    let (mut refined, mut reranked) = (0usize, 0usize);
    let (mut scan_time, mut prepare_time) = (0.0, 0.0);
    let mut estimates = Vec::new();
    for query in &queries {
        let rows_of = parts.iter().enumerate();
        let all = rows_of.flat_map(|(p, (_, _, rows))| {
            rows.iter()
                .enumerate()
                .map(move |(i, row)| (score(Op::Cosine, row, query).unwrap(), p, i))
        });
        let truth = top(all.collect());
        let hits = |list: &[(f32, usize, usize)]| {
            list.iter()
                .filter(|a| truth.iter().any(|b| (a.1, a.2) == (b.1, b.2)))
                .count()
        };
        let query_dir = rotated(&rotation, query);
        let (mut first, mut second) = (Vec::new(), Vec::new());
        for (p, (center, block)) in coded.iter().enumerate() {
            let start = nabedra_rt::time::now_std();
            let code = QueryCode::new(&query_dir, center);
            prepare_time += nabedra_rt::time::elapsed(start).as_secs_f64();
            let start = nabedra_rt::time::now_std();
            code.scan(block, &mut estimates);
            scan_time += nabedra_rt::time::elapsed(start).as_secs_f64();
            for (i, est) in estimates.iter().enumerate() {
                first.push((*est, p, i));
                second.push((code.refine(block, i), p, i));
            }
        }
        let cosines =
            |list: &[(Estimate, usize, usize)]| list.iter().map(|e| (e.0.cos, e.1, e.2)).collect();
        hits1 += hits(&top(cosines(&first)));
        hits5 += hits(&top(cosines(&second)));
        // Stages: drop what the bound puts below the tenth best lower bound.
        let floor = |list: &[(Estimate, usize, usize)]| {
            top(list.iter().map(|e| (e.0.cos - e.0.err, e.1, e.2)).collect())[K - 1].0
        };
        let threshold = floor(&first);
        let kept: Vec<_> = first
            .iter()
            .zip(&second)
            .filter(|(one, _)| one.0.cos + one.0.err >= threshold)
            .map(|(_, five)| *five)
            .collect();
        refined += kept.len();
        let threshold = floor(&kept);
        let kept = kept.iter().filter(|e| e.0.cos + e.0.err >= threshold);
        let rerank: Vec<_> = kept
            .map(|&(_, p, i)| (score(Op::Cosine, &parts[p].2[i], query).unwrap(), p, i))
            .collect();
        reranked += rerank.len();
        hits_staged += hits(&top(rerank));
    }
    let recall = |hits: usize| hits as f64 / (QUERIES * K) as f64;
    let per_query = QUERIES as f64;
    println!(
        "{rows} x {DIM}, {QUERIES} queries, level {:?}: recall@10 1-bit {:.3}, 5-bit {:.3}, staged \
         {:.3} with {:.0} refined and {:.1} reranked per query",
        Kernels::detect().level(),
        recall(hits1),
        recall(hits5),
        recall(hits_staged),
        refined as f64 / per_query,
        reranked as f64 / per_query,
    );
    println!(
        "encode {encode:.1} us per row, query preparation {:.2} us per partition, scan {:.0} M \
         codes/s",
        prepare_time * 1e6 / (per_query * CLUSTERS as f64),
        rows * per_query / scan_time / 1e6,
    );
}

/// A block that hands its extensions back estimates as one that keeps them, through pushes and
/// removals, and holds exactly the extension bytes less.
#[test]
fn vs07_a_detached_extension_refines_like_one_in_the_block() {
    let mut rng = SplitMix64::new(41);
    for dim in [1, 3, 64, 100, 768] {
        let rotation = Rotation::new(dim, 7);
        let centroid = rotated(&rotation, &random_unit(dim, &mut rng));
        let (mut inline, mut detached) = (CodeBlock::new(rotation.padded()), CodeBlock::new(rotation.padded()));
        let mut exts: Vec<Vec<u8>> = Vec::new();
        for i in 0..20 {
            let dir = rotated(&rotation, &random_unit(dim, &mut rng));
            let len = 0.5 + i as f32;
            inline.push(&dir, &centroid, len);
            exts.push(detached.push_detached(&dir, &centroid, len));
        }
        for i in [3, 0, 17, 5] {
            inline.swap_remove(i);
            detached.swap_remove(i);
            exts.swap_remove(i);
        }
        assert_eq!(inline.len(), detached.len());
        assert_eq!(inline.held(), detached.held() + inline.len() * inline.ext_bytes(), "dim {dim}");
        let query = QueryCode::new(&rotated(&rotation, &random_unit(dim, &mut rng)), &centroid);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        query.scan(&inline, &mut a);
        query.scan(&detached, &mut b);
        assert_eq!(a, b);
        for (i, ext) in exts.iter().enumerate() {
            assert_eq!(ext.len(), detached.ext_bytes());
            assert_eq!(query.refine(&inline, i), query.refine_with(&detached, i, ext), "dim {dim} entry {i}");
            assert_eq!(inline.length(i), detached.length(i));
        }
        // A block holds every extension or none.
        let dir = rotated(&rotation, &random_unit(dim, &mut rng));
        assert!(refuses(|| detached.clone().push(&dir, &centroid, 1.0)));
        assert!(refuses(|| inline.clone().push_detached(&dir, &centroid, 1.0)));
        assert!(refuses(|| query.refine_with(&detached, 0, &exts[0][1..])));
    }
}

/// Task perf1: the level without an integer conversion is the level, on whole levels, just below
/// them, at the cap and beyond it, so the stored codes do not change.
#[test]
fn vs07_the_float_level_is_the_level() {
    let mut rng = SplitMix64::new(28);
    let mut cases: Vec<(f32, f32)> = (0..=16).flat_map(|k| {
        let k = k as f32;
        [(k, 1.0), (k.next_down().max(0.0), 1.0), (k.next_up(), 1.0), (k / 4.0, 4.0)]
    }).collect();
    cases.extend([(0.0, 0.0), (-0.0, 3.0), (2.0, f32::MAX), (1e-30, 1e-30), (1.0, 1e9)]);
    cases.extend((0..100_000).map(|_| (uniform(&mut rng) as f32 * 2.0, (uniform(&mut rng) * 500.0) as f32)));
    for (m, s) in cases {
        assert_eq!(code::level_f(m, s), code::level(m, s) as f32, "{m} * {s}");
    }
}

/// Task perf1: the f32 scale search ranks scales as the f64 fit does. Its levels are those of the
/// code exactly, integers and halves included, and its cosine is within 1e-5 of the fit's, so
/// only scales that fit as well within that may swap.
#[test]
fn vs07_the_quick_scale_search_agrees_with_the_fit() {
    let mut rng = SplitMix64::new(27);
    for dim in [64, 768, 4096] {
        let mut residual: Vec<f32> = (0..dim).map(|_| (gaussian(&mut rng) * 0.03) as f32).collect();
        // Magnitudes that land on whole levels at scale 100, and at the cap.
        residual[0] = 0.07;
        residual[1] = -0.15;
        residual[2] = 0.5;
        let r2 = wide::norm_sq(&residual);
        let abs: Vec<f32> = residual.iter().map(|x| x.abs()).collect();
        let magnitudes: f64 = abs.iter().map(|&x| f64::from(x)).sum();
        let rms = (r2 / dim as f64).sqrt();
        for scale in [0.0, 100.0, 7.25, (16.0 / (3.0 * rms)) as f32, (16.0 / rms) as f32] {
            let quick = code::quick_cosine2(&abs, scale, magnitudes, r2);
            let fit = code::Fit::new(&residual, scale).cosine2(r2);
            assert!((quick - fit).abs() <= 1e-5 * fit, "{dim} {scale}: {quick} vs {fit}");
        }
    }
}
