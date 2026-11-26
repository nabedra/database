//! Fixed-dimensional encodings from the MUVERA paper, section 2.
//! <https://arxiv.org/abs/2405.19504>

use nabedra_simd::wide;

use super::Tokens;
use crate::types::{Error, MAX_DIM, Result};

const REPETITIONS: usize = 20;
const HASH_BITS: usize = 5;
const BUCKETS: usize = 1 << HASH_BITS;
const PROJECTION: usize = 16;
const PROJECT_SCALE: f64 = 0.25; // 1 / sqrt(PROJECTION)

/// The derived dense vector has 1,280 bytes at one bit per coordinate.
pub const FDE_DIM: usize = REPETITIONS * BUCKETS * PROJECTION;
/// Changes to the encoding require re-encoding derived checkpoint data.
pub const FDE_VERSION: u32 = 1;

/// A seeded, data-independent asymmetric MUVERA encoding for one vector-list dimension.
/// Both replicas and both sides of a search use the same column seed and encoding version.
pub struct Muvera {
    dim: usize,
    planes: Vec<f32>,
    signs: Vec<i8>,
}

impl Muvera {
    pub fn new(dim: usize, seed: u64) -> Result<Self> {
        if !(1..=MAX_DIM).contains(&dim) {
            return Err(Error::InvalidQuery("invalid MUVERA token dimension"));
        }
        let mut random = Random(seed);
        let planes = (0..REPETITIONS * HASH_BITS * dim)
            .map(|_| random.normal())
            .collect();
        let signs = (0..(REPETITIONS * PROJECTION * dim).div_ceil(64))
            .flat_map(|_| {
                let bits = random.next();
                (0..64).map(move |i| if bits >> i & 1 == 0 { -1 } else { 1 })
            })
            .take(REPETITIONS * PROJECTION * dim)
            .collect();
        Ok(Self { dim, planes, signs })
    }

    /// Query buckets sum normalized tokens and leave empty buckets at zero.
    pub fn query(&self, tokens: Tokens<'_>) -> Result<Vec<f32>> {
        tokens.require_dim(self.dim)?;
        if tokens.is_empty() {
            return Err(Error::InvalidQuery(
                "a MUVERA query needs at least one vector",
            ));
        }
        Ok(self.encode(tokens, false))
    }

    /// Document buckets average normalized tokens. Each empty bucket uses the first token
    /// from its nearest occupied bucket in Hamming distance. An empty list is unrankable.
    /// A nonempty list of zero tokens has a zero FDE, with raw-dot scoring still available.
    pub fn document(&self, tokens: Tokens<'_>) -> Result<Option<Vec<f32>>> {
        tokens.require_dim(self.dim)?;
        Ok((!tokens.is_empty()).then(|| self.encode(tokens, true)))
    }

    fn encode(&self, tokens: Tokens<'_>, document: bool) -> Vec<f32> {
        let inverse_lengths: Vec<_> = tokens
            .iter()
            .map(|token| {
                let len = wide::norm_sq(token).sqrt();
                if len == 0.0 { 0.0 } else { len.recip() }
            })
            .collect();
        let mut output = vec![0.0; FDE_DIM];
        if inverse_lengths.iter().all(|&inv| inv == 0.0) {
            return output;
        }
        let mut sums = vec![0.0; BUCKETS * self.dim];
        let mut normalized = vec![0.0; self.dim];
        let repetitions = self
            .planes
            .chunks_exact(HASH_BITS * self.dim)
            .zip(self.signs.chunks_exact(PROJECTION * self.dim))
            .zip(output.chunks_exact_mut(BUCKETS * PROJECTION));
        for ((planes, signs), blocks) in repetitions {
            sums.fill(0.0);
            let mut counts = [0u16; BUCKETS];
            let mut first = [None; BUCKETS];
            for (i, (token, &norm)) in tokens.iter().zip(&inverse_lengths).enumerate() {
                if norm == 0.0 {
                    continue;
                }
                let mut bin = 0;
                for (bit, plane) in planes.chunks_exact(self.dim).enumerate() {
                    // The positive normalization scale does not change a hyperplane's side.
                    bin |= usize::from(wide::dot(token, plane) > 0.0) << bit;
                }
                first[bin].get_or_insert(i);
                counts[bin] += 1;
                let sum = &mut sums[bin * self.dim..][..self.dim];
                for (acc, &x) in sum.iter_mut().zip(token) {
                    *acc += f64::from(x) * norm;
                }
            }

            let mut projected = [None; BUCKETS];
            for (bin, block) in blocks.chunks_exact_mut(PROJECTION).enumerate() {
                let count = counts[bin];
                if count > 0 {
                    let sum = &sums[bin * self.dim..][..self.dim];
                    let divisor = if document { f64::from(count) } else { 1.0 };
                    block.copy_from_slice(&project(signs, sum, PROJECT_SCALE / divisor));
                } else if document {
                    let closest = first
                        .iter()
                        .enumerate()
                        .filter_map(|(bin, token)| Some((bin, (*token)?)))
                        .min_by_key(|&(other, _)| (bin ^ other).count_ones());
                    if let Some((other, token)) = closest {
                        let value = projected[other].get_or_insert_with(|| {
                            let norm = inverse_lengths[token];
                            for (out, &x) in normalized.iter_mut().zip(tokens.at(token)) {
                                *out = f64::from(x) * norm;
                            }
                            project(signs, &normalized, PROJECT_SCALE)
                        });
                        block.copy_from_slice(value);
                    }
                }
            }
        }
        output
    }
}

fn project(signs: &[i8], values: &[f64], scale: f64) -> [f32; PROJECTION] {
    std::array::from_fn(|i| {
        let row = &signs[i * values.len()..][..values.len()];
        let sum = row
            .iter()
            .zip(values)
            .map(|(&sign, &x)| f64::from(sign) * x)
            .sum::<f64>();
        (sum * scale) as f32 + 0.0
    })
}

struct Random(u64);

impl Random {
    // SplitMix64 (Steele, Lea and Flood, 2014).
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let z = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        ((self.next() >> 12) as f64 + 0.5) / (1_u64 << 52) as f64
    }

    // Box?Muller standard normal for the SimHash hyperplanes. These only select candidates.
    fn normal(&mut self) -> f32 {
        let radius = (-2.0 * self.unit().ln()).sqrt();
        let angle = self.unit() * std::f64::consts::TAU;
        (radius * angle.cos()) as f32
    }
}
