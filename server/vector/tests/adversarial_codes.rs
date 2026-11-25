//! Adversarial tests of the codes: the stated bound of one fixed pair over rotations, estimator
//! bias per cluster shape, no panic on legal stored vectors, `swap_remove` against a model.

use std::panic::{AssertUnwindSafe, catch_unwind};

use nabedra_check::rng::SplitMix64;
use nabedra_simd::wide;
use nabedra_vector::quant::{
    COVERAGE, CodeBlock, EPSILON, Estimate, Op, QueryCode, Rotation, bounds, direction, exact,
};

fn uniform(rng: &mut SplitMix64) -> f64 {
    (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

fn gaussian(rng: &mut SplitMix64) -> f64 {
    (-2.0 * (1.0 - uniform(rng)).ln()).sqrt() * (std::f64::consts::TAU * uniform(rng)).cos()
}

fn unit(v: &[f32]) -> Vec<f32> {
    let mut out = vec![0.0; v.len()];
    direction(v, &mut out).expect("a nonzero vector");
    out
}

fn rotated(rotation: &Rotation, v: &[f32]) -> Vec<f32> {
    let mut out = vec![f32::NAN; rotation.padded()];
    rotation.apply(v, &mut out);
    out
}

fn one_hot(dim: usize, i: usize) -> Vec<f32> {
    (0..dim).map(|j| if i == j { 1.0 } else { 0.0 }).collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    wide::dot(a, b) / (wide::norm_sq(a) * wide::norm_sq(b)).sqrt()
}

fn noisy(v: &[f32], noise: f64, rng: &mut SplitMix64) -> Vec<f32> {
    let s = noise / (v.len() as f64).sqrt();
    v.iter()
        .map(|&x| (f64::from(x) + s * gaussian(rng)) as f32)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The stated bound of a fixed pair, over rotations.

/// One shape: a centroid, a query and some entries, as plain vectors.
struct Shape {
    name: &'static str,
    centroid: Vec<f32>,
    query: Vec<f32>,
    entries: Vec<Vec<f32>>,
}

fn shapes(dim: usize) -> Vec<Shape> {
    let mut rng = SplitMix64::new(dim as u64 ^ 0xabcd);
    let mut out = Vec::new();
    let e = |i: usize| one_hot(dim, i % dim);
    let sum = |a: &[f32], b: &[f32], wb: f32| -> Vec<f32> {
        a.iter().zip(b).map(|(x, y)| x + wb * y).collect()
    };
    // One-hot vectors: centroid, query and entries are axes or sums of two.
    out.push(Shape {
        name: "one-hot",
        centroid: e(0),
        query: e(1),
        entries: vec![
            e(0),
            e(1),
            e(2),
            e(2).iter().map(|x| -x).collect(),
            sum(&e(0), &e(1), 1.0),
        ],
    });
    // Constant vectors and their negation.
    let ones = vec![1.0f32; dim];
    let mut bumped = ones.clone();
    bumped[dim / 2] = 1.5;
    out.push(Shape {
        name: "constant",
        centroid: ones.clone(),
        query: ones.clone(),
        entries: vec![
            ones.clone(),
            ones.iter().map(|x| -x).collect(),
            bumped.clone(),
        ],
    });
    out.push(Shape {
        name: "constant-query-vs-onehot-centroid",
        centroid: e(0),
        query: ones.clone(),
        entries: vec![ones.clone(), e(0), e(dim - 1), bumped],
    });
    // Clustered near-duplicates: the residual of each entry is tiny.
    let base: Vec<f32> = (0..dim).map(|_| gaussian(&mut rng) as f32).collect();
    let entries: Vec<Vec<f32>> = (0..6).map(|_| noisy(&base, 1e-3, &mut rng)).collect();
    out.push(Shape {
        name: "near-duplicates",
        centroid: base.clone(),
        query: noisy(&entries[0], 1e-4, &mut rng),
        entries: entries.clone(),
    });
    // Queries unrelated to a tight cluster.
    let far: Vec<f32> = (0..dim).map(|_| gaussian(&mut rng) as f32).collect();
    out.push(Shape {
        name: "tight-cluster-far-query",
        centroid: base.clone(),
        query: far,
        entries: entries.clone(),
    });
    // Query opposite to the centroid, entries opposite too.
    out.push(Shape {
        name: "opposite",
        centroid: base.clone(),
        query: base.iter().map(|x| -x).collect(),
        entries: vec![
            base.iter().map(|x| -x).collect(),
            base.clone(),
            noisy(&base, 0.3, &mut rng),
        ],
    });
    // Sparse: a few non-zero values, in every cluster slot.
    let sparse = |rng: &mut SplitMix64| -> Vec<f32> {
        let mut v = vec![0.0f32; dim];
        for _ in 0..3 {
            v[rng.next_u64() as usize % dim] += [-1.0, 1.0, 2.0][rng.next_u64() as usize % 3];
        }
        v[0] += 0.5;
        v
    };
    out.push(Shape {
        name: "sparse",
        centroid: sparse(&mut rng),
        query: sparse(&mut rng),
        entries: (0..6).map(|_| sparse(&mut rng)).collect(),
    });
    // Heavy tails: one huge coordinate next to a Gaussian background.
    let heavy = |rng: &mut SplitMix64| -> Vec<f32> {
        let mut v: Vec<f32> = (0..dim).map(|_| gaussian(rng) as f32).collect();
        v[rng.next_u64() as usize % dim] *= 100.0;
        v
    };
    out.push(Shape {
        name: "heavy-tail",
        centroid: heavy(&mut rng),
        query: heavy(&mut rng),
        entries: (0..6).map(|_| heavy(&mut rng)).collect(),
    });
    out
}

#[derive(Default, Clone)]
struct Tally {
    n: f64,
    miss: f64,
    sum: f64,
    sq: f64,
}

impl Tally {
    fn add(&mut self, est: Estimate, truth: f64) {
        let error = f64::from(est.cos) - truth;
        let units = error * f64::from(EPSILON) / f64::from(est.err);
        self.n += 1.0;
        self.miss += f64::from(u8::from(error.abs() > f64::from(est.err)));
        self.sum += units;
        self.sq += units * units;
    }
}

/// The stated probability, per fixed pair, over rotations: at most `1 - COVERAGE` of the rotations
/// may miss, with 4 sigma of binomial slack for `rotations` draws.
fn run_pairs(dim: usize, rotations: u64, failures: &mut Vec<String>) {
    let p = 1.0 - COVERAGE;
    let slack = 4.0 * (p * (1.0 - p) / rotations as f64).sqrt();
    for shape in shapes(dim) {
        let pairs = shape.entries.len();
        let truth: Vec<f64> = shape
            .entries
            .iter()
            .map(|e| cosine(e, &shape.query))
            .collect();
        let mut tallies = vec![[Tally::default(), Tally::default()]; pairs];
        let (c, q) = (unit(&shape.centroid), unit(&shape.query));
        let es: Vec<Vec<f32>> = shape.entries.iter().map(|e| unit(e)).collect();
        for seed in 0..rotations {
            let rotation = Rotation::new(dim, seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 7);
            let centroid = rotated(&rotation, &c);
            let mut block = CodeBlock::new(rotation.padded());
            for e in &es {
                block.push(&rotated(&rotation, e), &centroid, 1.0);
            }
            let code = QueryCode::new(&rotated(&rotation, &q), &centroid);
            let mut scanned = Vec::new();
            code.scan(&block, &mut scanned);
            for i in 0..pairs {
                tallies[i][0].add(scanned[i], truth[i]);
                tallies[i][1].add(code.refine(&block, i), truth[i]);
            }
        }
        for (i, t) in tallies.iter().enumerate() {
            for (stage, t) in ["1-bit", "5-bit"].iter().zip(t) {
                let (miss, mean) = (t.miss / t.n, t.sum / t.n);
                let sd = (t.sq / t.n).sqrt();
                // The error measured in stated deviations has mean 0; its own standard error is
                // about sd / sqrt(n).
                let bias_limit = 5.0 * (sd.max(0.05)) / t.n.sqrt();
                let line = format!(
                    "dim {dim} {} pair {i} {stage}: miss {miss:.3} mean {mean:+.3} sd {sd:.2}",
                    shape.name
                );
                if miss > p + slack || (sd > 0.3 && mean.abs() > bias_limit.max(0.12)) {
                    println!("FAIL {line}");
                    failures.push(line);
                }
            }
        }
    }
}

#[test]
fn a_fixed_pair_is_inside_the_bound_over_rotations_for_small_dims() {
    let mut failures = Vec::new();
    for dim in 1..=8 {
        run_pairs(dim, 400, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn a_fixed_pair_is_inside_the_bound_over_rotations_at_padding_boundaries() {
    let mut failures = Vec::new();
    for dim in [
        9, 63, 64, 65, 100, 127, 128, 129, 191, 192, 193, 255, 256, 257, 320, 1000,
    ] {
        run_pairs(dim, 300, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn a_fixed_pair_is_inside_the_bound_over_rotations_at_4096_and_10240() {
    let mut failures = Vec::new();
    for dim in [4096, 10_240] {
        run_pairs(dim, 200, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ---------------------------------------------------------------------------------------------
// VEC-G11: any legal stored vector, through the whole pipeline.

fn legal_vectors(dim: usize, rng: &mut SplitMix64) -> Vec<(&'static str, Vec<f32>)> {
    let mut out: Vec<(&'static str, Vec<f32>)> = vec![
        ("zero", vec![0.0; dim]),
        ("negative zero", vec![-0.0; dim]),
        ("subnormal", vec![1e-45; dim]),
        ("min positive", vec![f32::MIN_POSITIVE; dim]),
        ("tiny", vec![1e-30; dim]),
        ("huge", vec![f32::MAX; dim]),
        ("neg huge", vec![f32::MIN; dim]),
        ("1e30", vec![1e30; dim]),
        (
            "one-hot huge",
            one_hot(dim, 0).iter().map(|x| x * f32::MAX).collect(),
        ),
        (
            "one-hot tiny",
            one_hot(dim, dim - 1).iter().map(|x| x * 1e-45).collect(),
        ),
        (
            "one-hot min normal",
            one_hot(dim, dim / 2)
                .iter()
                .map(|x| x * f32::MIN_POSITIVE)
                .collect(),
        ),
    ];
    let mut mixed: Vec<f32> = (0..dim).map(|_| 1e-40).collect();
    mixed[0] = 1e30;
    out.push(("big and subnormal", mixed));
    let mut mixed: Vec<f32> = (0..dim).map(|_| f32::MAX).collect();
    mixed[dim - 1] = 1e-45;
    out.push(("max with a subnormal", mixed));
    out.push(("gaussian", (0..dim).map(|_| gaussian(rng) as f32).collect()));
    out.push((
        "gaussian 1e-19",
        (0..dim).map(|_| (gaussian(rng) * 1e-19) as f32).collect(),
    ));
    out.push((
        "gaussian 1e35",
        (0..dim).map(|_| (gaussian(rng) * 1e35) as f32).collect(),
    ));
    out
}

/// Everything B does with a stored vector: direction, rotation, push, a query against it, scan,
/// refine, the three bounds and the three exact scores.
fn pipeline(dim: usize, seed: u64, centroid_src: &[f32], stored: &[f32], query: &[f32]) {
    let rotation = Rotation::new(dim, seed);
    let encode = |v: &[f32]| -> Option<(Vec<f32>, f32)> {
        let mut dir = vec![0.0; dim];
        let len = direction(v, &mut dir)?;
        Some((rotated(&rotation, &dir), len))
    };
    let Some((centroid, _)) = encode(centroid_src) else {
        return;
    };
    let mut block = CodeBlock::new(rotation.padded());
    let mut lens = Vec::new();
    if let Some((dir, len)) = encode(stored) {
        block.push(&dir, &centroid, len);
        block.push(&dir, &dir, len);
        block.push(&centroid, &centroid, 1.0);
        lens.extend([len, len, 1.0]);
    }
    let Some((q, qlen)) = encode(query) else {
        return;
    };
    let code = QueryCode::new(&q, &centroid);
    let mut scanned = Vec::new();
    code.scan(&block, &mut scanned);
    assert_eq!(scanned.len(), block.len());
    for (i, (est, len)) in scanned.iter().zip(&lens).enumerate() {
        for est in [*est, code.refine(&block, i)] {
            assert!(
                est.cos.is_finite() && est.err.is_finite() && est.err > 0.0,
                "{est:?}"
            );
            for op in [Op::Cosine, Op::Dot, Op::L2] {
                let (low, high) = bounds(op, est, *len, qlen);
                assert!(
                    low.is_finite() && high.is_finite() && low <= high,
                    "{op:?} {low} {high}"
                );
            }
        }
    }
    for op in [Op::Dot, Op::L2] {
        let s = exact(op, stored, query).unwrap();
        assert!(s.is_finite(), "{op:?} {s}");
    }
}

#[test]
fn no_legal_stored_vector_panics_the_pipeline_or_gives_a_non_finite_number() {
    let mut rng = SplitMix64::new(77);
    let mut failures = Vec::new();
    for dim in [1usize, 2, 3, 5, 7, 8, 9, 63, 64, 65, 128, 130, 4096] {
        let vectors = legal_vectors(dim, &mut rng);
        for (sname, stored) in &vectors {
            for (qname, query) in &vectors {
                for (cname, centroid) in [
                    ("gaussian", &vectors[vectors.len() - 3].1),
                    ("same", stored),
                ] {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        pipeline(dim, dim as u64, centroid, stored, query)
                    }));
                    if result.is_err() {
                        failures.push(format!(
                            "dim {dim} stored {sname} query {qname} centroid {cname}"
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} pipelines panicked: {:#?}",
        failures.len(),
        failures
    );
}

/// A vector of any value is legal, and `direction` is the gate: whatever it accepts, the codes
/// accept.
#[test]
fn whatever_direction_accepts_the_codes_accept_at_every_small_dim() {
    let mut rng = SplitMix64::new(1);
    let specials = [
        0.0f32,
        -0.0,
        1e-45,
        1.2e-38,
        1e-20,
        1.0,
        -3.0,
        1e20,
        1e30,
        3e38,
        f32::MAX,
    ];
    for dim in 1..=9usize {
        let rotation = Rotation::new(dim, 5);
        for _ in 0..3000 {
            let v: Vec<f32> = (0..dim)
                .map(|_| {
                    let x = specials[rng.next_u64() as usize % specials.len()];
                    if rng.next_u64().is_multiple_of(2) {
                        x
                    } else {
                        -x
                    }
                })
                .collect();
            let mut dir = vec![0.0; dim];
            let Some(len) = direction(&v, &mut dir) else {
                continue;
            };
            let r = rotated(&rotation, &dir);
            let mut block = CodeBlock::new(rotation.padded());
            block.push(&r, &r, len);
            let code = QueryCode::new(&r, &r);
            let mut out = Vec::new();
            code.scan(&block, &mut out);
            assert!(
                out[0].cos.is_finite() && code.refine(&block, 0).cos.is_finite(),
                "{v:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// swap_remove against a model.

/// A fresh block with the same entries, in the same order: the codes of an entry do not depend
/// on its neighbours, so the estimates of the two must be bit-equal.
fn assert_same_as_fresh(
    block: &CodeBlock,
    model: &[(Vec<f32>, f32)],
    centroid: &[f32],
    queries: &[QueryCode],
    padded: usize,
) {
    assert_eq!(block.len(), model.len());
    assert_eq!(block.is_empty(), model.is_empty());
    let mut fresh = CodeBlock::new(padded);
    for (dir, len) in model {
        fresh.push(dir, centroid, *len);
    }
    for (i, (_, len)) in model.iter().enumerate() {
        assert_eq!(
            block.length(i).to_bits(),
            len.to_bits(),
            "length of entry {i}"
        );
    }
    for q in queries {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        q.scan(block, &mut a);
        q.scan(&fresh, &mut b);
        assert_eq!(a.len(), b.len());
        for (i, (x, y)) in a.iter().zip(&b).enumerate() {
            assert_eq!(
                (x.cos.to_bits(), x.err.to_bits()),
                (y.cos.to_bits(), y.err.to_bits()),
                "scan entry {i} of {}",
                a.len()
            );
            let (x, y) = (q.refine(block, i), q.refine(&fresh, i));
            assert_eq!(
                (x.cos.to_bits(), x.err.to_bits()),
                (y.cos.to_bits(), y.err.to_bits()),
                "refine entry {i}"
            );
        }
    }
}

#[test]
fn swap_remove_follows_a_model_over_long_random_sequences() {
    for (dim, seed) in [(1usize, 1u64), (64, 2), (100, 3), (192, 4), (300, 5)] {
        let mut rng = SplitMix64::new(seed);
        let rotation = Rotation::new(dim, seed);
        let padded = rotation.padded();
        let centroid = rotated(&rotation, &unit(&noisy(&vec![1.0; dim], 0.5, &mut rng)));
        let queries: Vec<QueryCode> = (0..2)
            .map(|_| {
                let q = unit(&noisy(&vec![0.3; dim], 1.0, &mut rng));
                QueryCode::new(&rotated(&rotation, &q), &centroid)
            })
            .collect();
        let mut block = CodeBlock::new(padded);
        let mut model: Vec<(Vec<f32>, f32)> = Vec::new();
        for step in 0..2500 {
            // Grow, shrink, and spend time near empty and at one entry.
            let grow = match model.len() {
                0 => true,
                1..=3 => rng.next_u64().is_multiple_of(2),
                n if n > 60 => rng.next_u64().is_multiple_of(4),
                _ => !rng.next_u64().is_multiple_of(3),
            };
            if grow {
                let v = unit(&noisy(&vec![1.0; dim], 1.0 + uniform(&mut rng), &mut rng));
                let dir = rotated(&rotation, &v);
                let len = 0.5 + uniform(&mut rng) as f32;
                block.push(&dir, &centroid, len);
                model.push((dir, len));
            } else {
                let i = match rng.next_u64() % 4 {
                    0 => model.len() - 1,
                    1 => 0,
                    _ => rng.next_u64() as usize % model.len(),
                };
                block.swap_remove(i);
                model.swap_remove(i);
            }
            if step % 7 == 0 || model.len() <= 2 {
                assert_same_as_fresh(&block, &model, &centroid, &queries, padded);
            }
        }
        assert_same_as_fresh(&block, &model, &centroid, &queries, padded);
        while !model.is_empty() {
            block.swap_remove(0);
            model.swap_remove(0);
            assert_same_as_fresh(&block, &model, &centroid, &queries, padded);
        }
    }
}

#[test]
fn swap_remove_out_of_range_panics_and_keeps_the_block_intact() {
    let rotation = Rotation::new(64, 1);
    let c = rotated(&rotation, &one_hot(64, 0));
    let mut block = CodeBlock::new(64);
    for i in 1..4 {
        block.push(&rotated(&rotation, &one_hot(64, i)), &c, i as f32);
    }
    for bad in [3usize, 4, usize::MAX] {
        assert!(catch_unwind(AssertUnwindSafe(|| block.swap_remove(bad))).is_err());
        assert_eq!(block.len(), 3);
        assert_eq!(block.length(2), 3.0);
    }
    let q = QueryCode::new(&c, &c);
    let mut out = Vec::new();
    q.scan(&block, &mut out);
    assert_eq!(out.len(), 3);
}

// ---------------------------------------------------------------------------------------------
// Determinism.

#[test]
fn rotation_and_codes_are_a_pure_function_of_the_seed() {
    for dim in [1usize, 5, 64, 100, 192, 4096, 10_240] {
        let v = unit(
            &(0..dim)
                .map(|i| (i as f32 * 0.37).sin() + 0.1)
                .collect::<Vec<_>>(),
        );
        let digest = |seed: u64| -> (Vec<u32>, Vec<u64>, Vec<u8>) {
            let r = Rotation::new(dim, seed);
            let out = rotated(&r, &v);
            let mut block = CodeBlock::new(r.padded());
            let c = rotated(&r, &unit(&vec![1.0; dim]));
            block.push(&out, &c, 2.0);
            let q = QueryCode::new(&out, &c);
            let mut est = Vec::new();
            q.scan(&block, &mut est);
            let e = [est[0], q.refine(&block, 0)];
            (
                out.iter().map(|x| x.to_bits()).collect(),
                e.iter()
                    .flat_map(|e| [u64::from(e.cos.to_bits()), u64::from(e.err.to_bits())])
                    .collect(),
                Vec::new(),
            )
        };
        // Same seed from several threads at once, and in a fresh thread.
        let want = digest(9);
        let got: Vec<_> = std::thread::scope(|s| {
            let threads: Vec<_> = (0..4).map(|_| s.spawn(|| digest(9))).collect();
            threads.into_iter().map(|t| t.join().unwrap()).collect()
        });
        assert!(got.iter().all(|g| *g == want), "dim {dim}");
        assert_ne!(
            digest(10).0,
            want.0,
            "dim {dim}: seeds 9 and 10 gave the same rotation"
        );
    }
}

/// `1 - COVERAGE` plus 4 binomial deviations of the 1,500 rotations of `miss_rates` (about 0.085).
fn allowed_misses() -> f64 {
    let p = 1.0 - COVERAGE;
    p + 4.0 * (p * (1.0 - p) / 1500.0).sqrt()
}

/// Minimal case: a few vectors near the axes of a space of 3, 5 or 8 dimensions (centroid, query
/// and entry each near a different axis). The estimate of this one pair is outside its
/// stated bound for far more than the stated 6% of rotations.
fn miss_rates(dim: usize, centroid: &[f32], query: &[f32], entry: &[f32]) -> (f64, f64) {
    const N: u64 = 1500;
    let truth = cosine(entry, query);
    let (c, q, e) = (unit(centroid), unit(query), unit(entry));
    let (mut miss1, mut miss5) = (0.0, 0.0);
    for seed in 0..N {
        let rotation = Rotation::new(dim, seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 7);
        let centroid = rotated(&rotation, &c);
        let mut block = CodeBlock::new(rotation.padded());
        block.push(&rotated(&rotation, &e), &centroid, 1.0);
        let code = QueryCode::new(&rotated(&rotation, &q), &centroid);
        let mut scanned = Vec::new();
        code.scan(&block, &mut scanned);
        let miss = |est: Estimate| {
            f64::from(u8::from(
                (f64::from(est.cos) - truth).abs() > f64::from(est.err),
            ))
        };
        miss1 += miss(scanned[0]);
        miss5 += miss(code.refine(&block, 0));
    }
    (miss1 / N as f64, miss5 / N as f64)
}

#[test]
#[allow(clippy::type_complexity)]
fn near_axis_vectors_in_3_5_and_8_dims_stay_inside_the_stated_bound() {
    let cases: [(usize, [f32; 3], Vec<f32>, Vec<f32>); 3] = [
        (
            3,
            [1.4709117, -42.494453, 1.8108996],
            vec![0.5364913, -1.2543612, -187.42062],
            vec![49.248837, -1.037063, 1.4481347],
        ),
        (
            5,
            [0.0; 3],
            vec![0.5884968, 0.47768518, 0.33804095, 100.70235, 0.7634117],
            vec![148.6105, -1.0807067, 0.35481843, 0.76013076, 0.14072417],
        ),
        (
            8,
            [0.0; 3],
            vec![
                71.962715,
                0.8870684,
                -0.8896152,
                0.46147606,
                -0.26084355,
                0.3474572,
                0.29736653,
                0.4418029,
            ],
            vec![
                -27.197048,
                1.7021655,
                -0.36556184,
                -1.5688006,
                0.02851683,
                1.0472904,
                0.35593867,
                0.32390285,
            ],
        ),
    ];
    let centroids: [Vec<f32>; 3] = [
        cases[0].1.to_vec(),
        vec![-53.815987, -0.6410192, 1.0274161, 0.6388738, -0.33163065],
        vec![
            -1.4216002,
            -0.41404948,
            0.86307174,
            0.27966762,
            0.44121656,
            -0.01831628,
            -110.18336,
            1.3276073,
        ],
    ];
    let mut failures = Vec::new();
    for ((dim, _, query, entry), centroid) in cases.iter().zip(&centroids) {
        let (m1, m5) = miss_rates(*dim, centroid, query, entry);
        println!("dim {dim}: miss 1-bit {m1:.3}, 5-bit {m5:.3} (stated at most 0.060)");
        if m1 > allowed_misses() || m5 > allowed_misses() {
            failures.push(format!("dim {dim}: miss 1-bit {m1:.3}, 5-bit {m5:.3}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The same pair of the near-axis case, as a rank-3 corner of a 256 or 1,024-dimensional space:
/// data of low intrinsic dimension is common.
#[test]
fn low_rank_data_in_a_large_dim_stays_inside_the_stated_bound() {
    let pad = |v: &[f32], dim: usize| -> Vec<f32> {
        let mut out = v.to_vec();
        out.resize(dim, 0.0);
        out
    };
    let (c, q, e) = (
        [1.4709117, -42.494453, 1.8108996],
        [0.5364913, -1.2543612, -187.42062],
        [49.248837, -1.037063, 1.4481347],
    );
    let mut failures = Vec::new();
    for dim in [64, 256, 1024, 4096] {
        let (m1, m5) = miss_rates(dim, &pad(&c, dim), &pad(&q, dim), &pad(&e, dim));
        println!("rank 3 in dim {dim}: miss 1-bit {m1:.3}, 5-bit {m5:.3} (stated at most 0.060)");
        if m1 > allowed_misses() || m5 > allowed_misses() {
            failures.push(format!(
                "rank 3 in dim {dim}: miss 1-bit {m1:.3}, 5-bit {m5:.3}"
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// `CodeBlock::push` and `QueryCode::new` take `|r|^2`, `|q|^2` and the rounding error of the query
/// from `nabedra_simd::norm_sq` and `l2_sq`, which run at the best level of the CPU, and the
/// levels do not return the same bits. Two replicas on different CPUs therefore store different
/// `r2`, `scale1` and `scale5` for one vector, and the scale that picks the 4-bit extension
/// follows from them. The levels cannot be forced through `push`, so this checks the input it
/// would use: the squared length of the residual of a rotated direction, per level.
#[test]
fn the_squared_length_that_push_stores_is_the_same_at_every_cpu_level() {
    use nabedra_simd::{Kernels, Level};
    let levels: Vec<Kernels> = Level::ALL.into_iter().filter_map(Kernels::at).collect();
    assert!(levels.len() >= 2);
    let mut rng = SplitMix64::new(42);
    let (mut differing, mut total) = (0, 0);
    for dim in [64usize, 100, 768, 1024, 4096] {
        let rotation = Rotation::new(dim, 1);
        for _ in 0..50 {
            let gauss = |rng: &mut SplitMix64| -> Vec<f32> {
                (0..dim).map(|_| gaussian(rng) as f32).collect()
            };
            let c = rotated(&rotation, &unit(&gauss(&mut rng)));
            let d = rotated(&rotation, &unit(&gauss(&mut rng)));
            let r: Vec<f32> = d.iter().zip(&c).map(|(d, c)| d - c).collect();
            let bits: Vec<u32> = levels.iter().map(|k| k.norm_sq(&r).to_bits()).collect();
            total += 1;
            differing += usize::from(bits.iter().any(|b| *b != bits[0]));
        }
    }
    assert_eq!(
        differing, 0,
        "{differing} of {total} residuals have a squared length that depends on the level"
    );
}

#[test]
fn tiny_but_nonzero_residuals_give_finite_estimates() {
    // A residual between the f32 denormals and the noise floor of `push` (1e-24): the estimates
    // must stay finite and sane, and the bound must hold.
    for padded in [64usize, 128, 4096] {
        for tiny in [1e-22f32, 1e-20, 1e-15, 1e-12, 1e-8] {
            let mut centroid = vec![0.0f32; padded];
            centroid[0] = 1.0;
            let mut dir = centroid.clone();
            dir[3] = tiny; // |dir|^2 = 1 + tiny^2: still unit
            let mut block = CodeBlock::new(padded);
            block.push(&dir, &centroid, 1.0);
            let mut query = vec![0.0f32; padded];
            query[1] = 1.0;
            let code = QueryCode::new(&query, &centroid);
            let mut out = Vec::new();
            code.scan(&block, &mut out);
            for est in [out[0], code.refine(&block, 0)] {
                assert!(
                    est.cos.is_finite() && est.err.is_finite(),
                    "{padded} {tiny}: {est:?}"
                );
                // cos(dir, query) = 0 here.
                assert!(est.cos.abs() <= est.err, "{padded} {tiny}: {est:?}");
            }
        }
    }
}
