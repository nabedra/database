//! Multi-vector candidate encodings and MaxSim scoring.
//!
//! The worker first pins its range versions and filters them. MUVERA selects candidates among
//! those versions for an approximate search. The token-code bounds can avoid raw reads, but
//! only a score from the raw tokens can order the final results. Exact retrieval scores every
//! eligible row, bypassing the MUVERA candidate limit.

mod fde;
mod score;
#[cfg(test)]
mod tests;

use nabedra_simd::wide;

use crate::types::{Error, MAX_DIM, Result};
pub use fde::{FDE_DIM, FDE_VERSION, Muvera};
pub use score::{MaxSim, ScoreBounds, TokenCodes};

/// The maximum number of vectors in one row value or ranking.
pub const MAX_TOKENS: usize = 1024;

/// Validated, row-major tokens borrowed from one typed vector-list column or query.
#[derive(Clone, Copy, Debug)]
pub struct Tokens<'a> {
    dim: usize,
    values: &'a [f32],
}

impl<'a> Tokens<'a> {
    pub fn new(dim: usize, values: &'a [f32]) -> Result<Self> {
        if !(1..=MAX_DIM).contains(&dim) || !values.len().is_multiple_of(dim) {
            return Err(Error::InvalidQuery(
                "the tokens do not fit the vector dimension",
            ));
        }
        if values.len() / dim > MAX_TOKENS {
            return Err(Error::InvalidQuery("too many vectors in the list"));
        }
        if values.iter().any(|x| !x.is_finite()) {
            return Err(Error::InvalidQuery("non-finite value in a vector list"));
        }
        Ok(Self { dim, values })
    }

    pub fn len(self) -> usize {
        self.values.len() / self.dim
    }

    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    /// Whether a normalized candidate search is suitable for the dot-product recall target.
    /// The tolerance admits ordinary f32 normalization error. Exact dot still uses raw vectors.
    pub fn is_unit_length(self) -> bool {
        !self.is_empty() && self.iter().all(|v| (wide::norm_sq(v) - 1.0).abs() <= 1e-5)
    }

    fn iter(self) -> std::slice::ChunksExact<'a, f32> {
        self.values.chunks_exact(self.dim)
    }

    fn at(self, token: usize) -> &'a [f32] {
        &self.values[token * self.dim..][..self.dim]
    }

    fn require_dim(self, dim: usize) -> Result<()> {
        if self.dim != dim {
            return Err(Error::InvalidQuery("vector-list dimensions do not match"));
        }
        Ok(())
    }
}
