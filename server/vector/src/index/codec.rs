//! The seam to `quant`: direction codes and score bounds. [`ExactCodec`] keeps whole directions.

use std::sync::Arc;

use crate::{
    quant::{self, CodeBlock, QueryCode, Rotation},
    types::{Error, Op, Result},
};

pub use crate::quant::Estimate;

/// Codes of the directions in a partition band, relative to its centroid. Directions and centroids
/// are unit vectors that went through `rotate`.
pub trait Codec: Clone + Send + Sync + 'static {
    type Block: Clone + Send;
    type Query;
    fn fits(&self, _dim: usize) -> bool {
        true
    }
    fn rotate(&self, dir: &[f32]) -> Vec<f32>;
    fn block(&self) -> Self::Block;
    fn push(&self, block: &mut Self::Block, dir: &[f32], centroid: &[f32], len: f32);
    fn swap_remove(&self, block: &mut Self::Block, i: usize);
    fn query(&self, dir: &[f32], centroid: &[f32]) -> Self::Query;
    /// Appends one coarse estimate per entry to `out`.
    fn scan(&self, q: &Self::Query, block: &Self::Block, out: &mut Vec<Estimate>);
    /// A finer estimate of one entry.
    fn refine(&self, q: &Self::Query, block: &Self::Block, i: usize) -> Estimate;
}

/// Seeded direction codes for the approximate index. Raw vectors still
/// decide every result score, and exact scans do not visit the codes.
#[derive(Clone)]
pub struct DirectionCodec {
    dim: usize,
    rotation: Arc<Rotation>,
}

impl DirectionCodec {
    pub fn new(dim: usize, seed: u64) -> Result<Self> {
        if !(1..=quant::MAX_DIM).contains(&dim) {
            return Err(Error::InvalidQuery("invalid code dimension"));
        }
        Ok(Self {
            dim,
            rotation: Arc::new(Rotation::new(dim, seed)),
        })
    }
}

impl Codec for DirectionCodec {
    type Block = CodeBlock;
    type Query = QueryCode;

    fn fits(&self, dim: usize) -> bool {
        dim == self.dim
    }
    fn rotate(&self, dir: &[f32]) -> Vec<f32> {
        let mut rotated = vec![0.0; self.rotation.padded()];
        self.rotation.apply(dir, &mut rotated);
        rotated
    }
    fn block(&self) -> CodeBlock {
        CodeBlock::new(self.rotation.padded())
    }
    fn push(&self, block: &mut CodeBlock, dir: &[f32], centroid: &[f32], len: f32) {
        block.push(dir, centroid, len);
    }
    fn swap_remove(&self, block: &mut CodeBlock, i: usize) {
        block.swap_remove(i);
    }
    fn query(&self, dir: &[f32], centroid: &[f32]) -> QueryCode {
        QueryCode::new(dir, centroid)
    }
    fn scan(&self, query: &QueryCode, block: &CodeBlock, out: &mut Vec<Estimate>) {
        query.append_scan(block, out);
    }
    fn refine(&self, query: &QueryCode, block: &CodeBlock, i: usize) -> Estimate {
        query.refine(block, i)
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct ExactCodec;

/// The float error of a dot product of two unit f32 vectors of at most 4,096 dimensions.
const DOT_ERR: f32 = 5e-4;

impl Codec for ExactCodec {
    type Block = Vec<Vec<f32>>;
    type Query = Vec<f32>;

    fn rotate(&self, dir: &[f32]) -> Vec<f32> {
        dir.to_vec()
    }
    fn block(&self) -> Self::Block {
        Vec::new()
    }
    fn push(&self, block: &mut Self::Block, dir: &[f32], _centroid: &[f32], _len: f32) {
        block.push(dir.to_vec());
    }
    fn swap_remove(&self, block: &mut Self::Block, i: usize) {
        block.swap_remove(i);
    }
    fn query(&self, dir: &[f32], _centroid: &[f32]) -> Self::Query {
        dir.to_vec()
    }
    fn scan(&self, q: &Self::Query, block: &Self::Block, out: &mut Vec<Estimate>) {
        out.extend((0..block.len()).map(|i| self.refine(q, block, i)));
    }
    fn refine(&self, q: &Self::Query, block: &Self::Block, i: usize) -> Estimate {
        Estimate {
            cos: dot(q, &block[i]),
            err: DOT_ERR,
        }
    }
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn dot64(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| f64::from(x) * f64::from(y))
        .sum()
}

pub fn length(v: &[f32]) -> f64 {
    dot64(v, v).sqrt()
}

/// The direction and the length of `v`, or `None` when the codes cannot hold it: it is zero, or its
/// length is no positive normal f32.
pub fn direction(v: &[f32]) -> Option<(Vec<f32>, f64)> {
    let len = length(v);
    let dir = || v.iter().map(|&x| (f64::from(x) / len) as f32).collect();
    (len as f32).is_normal().then(|| (dir(), len))
}

/// The closest finite f32.
fn round(x: f64) -> f32 {
    // Adding zero turns -0.0 into 0.0, so that they tie.
    (x as f32).clamp(f32::MIN, f32::MAX) + 0.0
}

/// The value of an operation, or `None` for a cosine with a zero vector. It decides the order of the
/// result, so it is one sum in f64, which neither overflows nor underflows for finite f32 inputs,
/// rounded once, and never comes from a kernel that differs by CPU.
pub fn exact(op: Op, a: &[f32], b: &[f32]) -> Option<f32> {
    let value = match op {
        Op::Dot => dot64(a, b),
        Op::L2 => {
            let square = |(&x, &y): (&f32, &f32)| (f64::from(x) - f64::from(y)).powi(2);
            a.iter().zip(b).map(square).sum::<f64>().sqrt()
        }
        Op::Cosine => {
            let (la, lb) = (length(a), length(b));
            if la == 0.0 || lb == 0.0 {
                return None;
            }
            (dot64(a, b) / la / lb).clamp(-1.0, 1.0)
        }
    };
    Some(round(value))
}

/// A score, or a score back, as "higher is better".
pub fn goodness(op: Op, score: f32) -> f32 {
    (if op == Op::L2 { -score } else { score }) + 0.0
}

/// The best goodness of any vector with this cosine to the query and a length in `[lo, hi]`.
pub fn best(op: Op, cos: f64, lo: f64, hi: f64, len_q: f64) -> f64 {
    match op {
        Op::Cosine => cos,
        Op::Dot => (lo * cos).max(hi * cos) * len_q,
        Op::L2 => {
            let t = (len_q * cos).clamp(lo, hi);
            -(t * t + len_q * len_q - 2.0 * t * len_q * cos)
                .max(0.0)
                .sqrt()
        }
    }
}

/// The largest goodness an entry may have, given the estimate of its cosine.
pub fn optimistic(op: Op, est: Estimate, len_o: f64, len_q: f64) -> f32 {
    let cos = (f64::from(est.cos) + f64::from(est.err)).clamp(-1.0, 1.0);
    round(best(
        op,
        cos,
        len_o * (1.0 - 1e-9),
        len_o * (1.0 + 1e-9),
        len_q,
    ))
}
