//! Synchronous row and prepared-file payload boundaries. Call construction, reads, parsing, writing
//! and final spool/reader drops on the caller's bounded CPU/I/O executor.
//! This module starts no tasks. Callers retain registered worker ownership.
mod prepared;
mod rows;
pub use prepared::{PreparedReceiver, PreparedResult, ValidatedPart};
pub use rows::{decode_rows, encode_rows};

pub(crate) use prepared::LocalPrepared;
/// Checks declared row semantics; cannot prove that a peer omitted no matches.
pub(crate) use rows::validate as validate_rows;
