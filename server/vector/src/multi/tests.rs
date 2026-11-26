use nabedra_check::Gen;
use nabedra_simd::wide;

use super::*;
use crate::types::Op;

fn tokens(dim: usize, values: &[f32]) -> Tokens<'_> {
    Tokens::new(dim, values).unwrap()
}

#[test]
fn vs07_maxsim_chooses_a_best_match_for_each_query_token() {
    let q = MaxSim::new(Op::Dot, tokens(2, &[1.0, 0.0, 0.0, 1.0])).unwrap();
    assert_eq!(
        q.score(tokens(2, &[-1.0, 4.0, 3.0, -2.0])).unwrap(),
        Some(7.0)
    );
    assert_eq!(
        q.score(tokens(2, &[-1.0, -4.0, -3.0, -2.0])).unwrap(),
        Some(-3.0)
    );
    assert_eq!(
        q.score(tokens(2, &[-1.0, -4.0, 0.0, 0.0])).unwrap(),
        Some(0.0)
    );
    assert_eq!(q.score(tokens(2, &[])).unwrap(), None);

    let cos = MaxSim::new(Op::Cosine, tokens(2, &[1.0, 0.0])).unwrap();
    assert_eq!(
        cos.score(tokens(2, &[0.0, 0.0, -1.0, 0.0])).unwrap(),
        Some(-1.0)
    );
    assert_eq!(cos.score(tokens(2, &[0.0, 0.0])).unwrap(), None);
}

#[test]
fn vs07_maxsim_rounds_only_after_summing_the_best_matches() {
    let x = f32::MAX;
    let query = [x, -x * 0.5];
    let q = MaxSim::new(Op::Dot, tokens(1, &query)).unwrap();
    assert_eq!(q.score(tokens(1, &[2.0])).unwrap(), Some(x));
    assert_eq!(q.score(tokens(1, &[-2.0])).unwrap(), Some(-x));
    let z = MaxSim::new(Op::Dot, tokens(1, &[-0.0])).unwrap();
    assert_eq!(z.score(tokens(1, &[1.0])).unwrap().unwrap().to_bits(), 0);
}

#[test]
fn vs17_multi_vector_inputs_are_bounded_and_typed() {
    for (dim, data) in [
        (0, &[1.0][..]),
        (MAX_DIM + 1, &[1.0][..]),
        (2, &[1.0][..]),
        (1, &[f32::NAN][..]),
        (1, &[f32::INFINITY][..]),
    ] {
        assert!(Tokens::new(dim, data).is_err());
    }
    let many = vec![1.0; MAX_TOKENS + 1];
    assert!(Tokens::new(1, &many).is_err());
    assert!(Tokens::new(1, &many[..MAX_TOKENS]).is_ok());
    assert!(MaxSim::new(Op::Dot, tokens(1, &[])).is_err());
    assert!(MaxSim::new(Op::Cosine, tokens(1, &[1.0, 0.0])).is_err());
    assert!(MaxSim::new(Op::L2, tokens(1, &[1.0])).is_err());
    let q = MaxSim::new(Op::Dot, tokens(1, &[1.0])).unwrap();
    assert!(q.score(tokens(2, &[1.0, 0.0])).is_err());
    assert!(q.bounds(&TokenCodes::new(tokens(2, &[1.0, 0.0]))).is_err());
    assert!(Muvera::new(0, 7).is_err());
    assert!(
        Muvera::new(1, 7)
            .unwrap()
            .query(tokens(2, &[1.0, 0.0]))
            .is_err()
    );
}

fn brute_force(op: Op, dim: usize, query: &[f32], doc: &[f32]) -> Option<f32> {
    let norm = |v: &[f32]| v.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>().sqrt();
    let mut maxima = Vec::new();
    for q in query.chunks_exact(dim) {
        let mut scores = Vec::new();
        for p in doc.chunks_exact(dim) {
            let mut value = 0.0;
            for i in 0..dim {
                value += f64::from(q[i]) * f64::from(p[i]);
            }
            if op == Op::Cosine {
                let length = norm(q) * norm(p);
                if length == 0.0 {
                    continue;
                }
                value = (value / length).clamp(-1.0, 1.0);
            }
            scores.push(value);
        }
        scores.sort_by(f64::total_cmp);
        maxima.push(scores.pop()?);
    }
    Some((maxima.into_iter().sum::<f64>() as f32).clamp(f32::MIN, f32::MAX) + 0.0)
}

#[test]
fn vs07_maxsim_and_quantized_bounds_against_brute_force() {
    let options = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        f32::from_bits(1),
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        -1e-12,
        1e12,
        -0.01,
        0.6,
        1.7,
    ];
    for seed in 0..320 {
        let g = &mut Gen::from_seed(seed);
        let dim = *g.pick(&[1, 2, 3, 8, 17, 129]);
        let op = *g.pick(&[Op::Dot, Op::Cosine]);
        let q_count = 1 + g.below(9) as usize;
        let p_count = g.below(10) as usize;
        let mut vector = |n| (0..n).map(|_| *g.pick(&options)).collect::<Vec<_>>();
        let mut q = vector(q_count * dim);
        for token in q.chunks_exact_mut(dim) {
            if op == Op::Cosine && token.iter().all(|&x| x == 0.0) {
                token[0] = 1.0;
            }
        }
        let p = vector(p_count * dim);
        let q_model = MaxSim::new(op, tokens(dim, &q)).unwrap();
        let document = tokens(dim, &p);
        let exact = q_model.score(document).unwrap();
        assert_eq!(exact, brute_force(op, dim, &q, &p), "seed {seed}");
        let packed = TokenCodes::new(document);
        assert_eq!(packed.packed().len(), p.len().div_ceil(2));
        let interval = q_model.bounds(&packed).unwrap();
        match (exact, interval) {
            (Some(score), Some(bounds)) => {
                assert!(
                    bounds.lower <= score && score <= bounds.upper,
                    "seed {seed}: {bounds:?}, raw {score}"
                );
                assert!(bounds.lower <= bounds.estimate && bounds.estimate <= bounds.upper);
            }
            (None, None) => {}
            pair => panic!("seed {seed}: {pair:?}"),
        }
    }
}

#[test]
fn vs07_muvera_uses_asymmetric_fixed_dimensional_encodings() {
    let encoder = Muvera::new(2, 17).unwrap();
    let v = tokens(2, &[1.0, 0.0]);
    let q = encoder.query(v).unwrap();
    let p = encoder.document(v).unwrap().unwrap();
    assert_eq!(q.len(), FDE_DIM);
    assert_eq!(p.len(), FDE_DIM);
    assert_eq!(q.iter().filter(|&&x| x != 0.0).count(), 20 * 16);
    assert!(p.iter().all(|&x| x != 0.0));
    let q_twice = encoder.query(tokens(2, &[1.0, 0.0, 1.0, 0.0])).unwrap();
    let p_twice = encoder
        .document(tokens(2, &[1.0, 0.0, 4.0, 0.0]))
        .unwrap()
        .unwrap();
    assert_eq!(p, p_twice);
    assert_eq!(wide::dot(&q_twice, &p), 2.0 * wide::dot(&q, &p));
    let negative = encoder.document(tokens(2, &[-1.0, 0.0])).unwrap().unwrap();
    assert_eq!(wide::dot(&q, &negative), -wide::dot(&q, &p));
    assert_eq!(encoder.document(tokens(2, &[])).unwrap(), None);
    assert!(
        encoder
            .document(tokens(2, &[0.0, -0.0]))
            .unwrap()
            .unwrap()
            .iter()
            .all(|&x| x == 0.0)
    );
    assert!(encoder.query(tokens(2, &[])).is_err());
    assert_eq!(q, Muvera::new(2, 17).unwrap().query(v).unwrap());
    assert_ne!(q, Muvera::new(2, 18).unwrap().query(v).unwrap());

    let opposed = tokens(2, &[1.0, 0.0, -1.0, 0.0]);
    let q_both = encoder.query(opposed).unwrap();
    let p_both = encoder.document(opposed).unwrap().unwrap();
    assert_eq!(q_both.iter().filter(|&&x| x != 0.0).count(), 2 * 20 * 16);
    assert_eq!(wide::dot(&q_both, &p_both), 2.0 * wide::dot(&q, &p));
    for (query, document) in q.chunks_exact(32 * 16).zip(p_both.chunks_exact(32 * 16)) {
        let plus = query
            .chunks_exact(16)
            .position(|bucket| bucket[0] != 0.0)
            .unwrap();
        let signs = &query[plus * 16..][..16];
        let minus = plus ^ 31;
        for (bucket, block) in document.chunks_exact(16).enumerate() {
            let near_plus = (bucket ^ plus).count_ones() < (bucket ^ minus).count_ones();
            assert!(
                block
                    .iter()
                    .zip(signs)
                    .all(|(&got, &want)| { got == if near_plus { want } else { -want } })
            );
        }
    }
}

#[test]
fn vs19_unit_length_check_allows_the_recall_target_only_in_its_domain() {
    assert!(tokens(2, &[0.6, 0.8, 1.0, 0.0]).is_unit_length());
    assert!(!tokens(2, &[0.6, 0.8, 2.0, 0.0]).is_unit_length());
    assert!(!tokens(2, &[0.0, 0.0]).is_unit_length());
    assert!(!tokens(2, &[]).is_unit_length());
}
