use nabedra_simd::wide;

use super::Tokens;
use crate::types::{Error, Op, Result};

/// A MaxSim query. Cosine skips zero document tokens and refuses a zero query token.
pub struct MaxSim<'a> {
    query: Tokens<'a>,
    lengths: Vec<f64>,
    cosine: bool,
}

/// Bounds for the deterministic raw score. A caller can skip a raw read if the upper bound
/// cannot enter its top-k. Equal scores must still consider the key ordering.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoreBounds {
    pub lower: f32,
    pub estimate: f32,
    pub upper: f32,
}

impl<'a> MaxSim<'a> {
    pub fn new(op: Op, query: Tokens<'a>) -> Result<Self> {
        let cosine = match op {
            Op::Cosine => true,
            Op::Dot => false,
            Op::L2 => return Err(Error::InvalidQuery("L2 is not supported on vector lists")),
        };
        if query.is_empty() {
            return Err(Error::InvalidQuery(
                "a MaxSim query needs at least one vector",
            ));
        }
        let lengths: Vec<_> = query.iter().map(length).collect();
        if cosine && lengths.contains(&0.0) {
            return Err(Error::InvalidQuery(
                "cosine is undefined for a zero query vector",
            ));
        }
        Ok(Self {
            query,
            lengths,
            cosine,
        })
    }

    /// For each query token, select the largest cosine or dot against this document's tokens.
    /// Sum those maxima in query order, with one final finite f32 rounding. An empty document
    /// or a cosine document containing only zeros has no score.
    pub fn score(&self, doc: Tokens<'_>) -> Result<Option<f32>> {
        doc.require_dim(self.query.dim)?;
        if doc.is_empty() {
            return Ok(None);
        }
        let mut sum = 0.0;
        if self.cosine {
            let lengths: Vec<_> = doc.iter().map(length).collect();
            if lengths.iter().all(|&len| len == 0.0) {
                return Ok(None);
            }
            for (q, &ql) in self.query.iter().zip(&self.lengths) {
                let mut best = -1.0_f64;
                for (p, &pl) in doc.iter().zip(&lengths) {
                    if pl != 0.0 {
                        best = best.max(cosine(wide::dot(q, p), ql * pl));
                    }
                }
                sum += best;
            }
        } else {
            for q in self.query.iter() {
                let mut best = f64::NEG_INFINITY;
                for p in doc.iter() {
                    best = best.max(wide::dot(q, p));
                }
                sum += best;
            }
        }
        Ok(Some(round(sum)))
    }

    /// Bound the raw score using 4-bit token codes. With the bound and final tie order a caller
    /// can refine only the candidates that could enter its top-k.
    pub fn bounds(&self, doc: &TokenCodes) -> Result<Option<ScoreBounds>> {
        if self.query.dim != doc.dim {
            return Err(Error::InvalidQuery("vector-list dimensions do not match"));
        }
        if doc.tokens.is_empty() || (self.cosine && doc.tokens.iter().all(|p| p.length == 0.0)) {
            return Ok(None);
        }
        let (mut lower, mut estimate, mut upper, mut mass) = (0.0, 0.0, 0.0, 0.0);
        for (q, &ql) in self.query.iter().zip(&self.lengths) {
            let (mut low, mut mid, mut high) =
                (f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
            for (i, p) in doc.tokens.iter().enumerate() {
                if self.cosine && p.length == 0.0 {
                    continue;
                }
                let center = doc.dot(q, i) * p.step;
                let norms = ql * p.length;
                // Cauchy-Schwarz bounds the dot with the reconstruction residual. The slack
                // covers f64 rounding of both dots, the stored residual and the norms.
                let error = ql * p.residual + norms * SLACK;
                let score = |dot| if self.cosine { cosine(dot, norms) } else { dot };
                low = low.max(score(center - error));
                mid = mid.max(score(center));
                high = high.max(score(center + error));
            }
            lower += low;
            estimate += mid;
            upper += high;
            mass += low.abs().max(high.abs());
        }
        // Rounding of the sums follows their absolute mass even when large maxima cancel.
        let error = mass * SLACK;
        Ok(Some(ScoreBounds {
            lower: round(lower - error),
            estimate: round(estimate),
            upper: round(upper + error),
        }))
    }
}

/// More than the f64 summation error for 4,096 coordinates and 1,024 token maxima.
const SLACK: f64 = 1e-9;

fn length(v: &[f32]) -> f64 {
    wide::norm_sq(v).sqrt()
}

fn cosine(dot: f64, norms: f64) -> f64 {
    (dot / norms).clamp(-1.0, 1.0)
}

fn round(score: f64) -> f32 {
    (score as f32).clamp(f32::MIN, f32::MAX) + 0.0
}

/// Four bits per coordinate, plus per-token norm, reconstruction residual and scale. A nibble
/// stores a signed integer from -7 to 7 with an offset of 7. The even coordinate is the low
/// nibble, including across odd-sized token boundaries.
pub struct TokenCodes {
    dim: usize,
    packed: Vec<u8>,
    tokens: Vec<TokenCode>,
}

struct TokenCode {
    step: f64,
    length: f64,
    residual: f64,
}

impl TokenCodes {
    pub fn new(doc: Tokens<'_>) -> Self {
        let mut packed = vec![0; doc.values.len().div_ceil(2)];
        let mut tokens = Vec::with_capacity(doc.len());
        for (i, token) in doc.iter().enumerate() {
            let maximum = token.iter().map(|x| f64::from(x.abs())).fold(0.0, f64::max);
            let step = maximum / 7.0;
            let mut residual = 0.0;
            for (j, &x) in token.iter().enumerate() {
                let x = f64::from(x);
                let code = if step == 0.0 {
                    0
                } else {
                    (x / step).round().clamp(-7.0, 7.0) as i8
                };
                let delta = x - f64::from(code) * step;
                residual += delta * delta;
                let at = i * doc.dim + j;
                packed[at / 2] |= ((code + 7) as u8) << (4 * (at % 2));
            }
            tokens.push(TokenCode {
                step,
                length: length(token),
                residual: residual.sqrt(),
            });
        }
        Self {
            dim: doc.dim,
            packed,
            tokens,
        }
    }

    pub fn packed(&self) -> &[u8] {
        &self.packed
    }

    fn dot(&self, query: &[f32], token: usize) -> f64 {
        let start = token * self.dim;
        query
            .iter()
            .enumerate()
            .map(|(offset, &x)| {
                let at = start + offset;
                let encoded = (self.packed[at / 2] >> (4 * (at % 2))) & 0x0f;
                f64::from(x) * f64::from(i16::from(encoded) - 7)
            })
            .sum()
    }
}
