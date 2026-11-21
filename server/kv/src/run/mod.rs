//! Immutable sorted runs (task NS4).
//!
//! A run is a file of 4 KiB data blocks of internal-key-sorted point entries, a
//! partitioned (two-level) index and whole-user-key bloom filter, a range-tombstone block,
//! a properties block and a 64-byte footer; every block and the footer carry a masked
//! CRC32C (see [`format`]). An open run keeps only its top-level index in memory (charged
//! to the block cache); every index and filter partition is a small cached unit.
//! [`RunWriter`] builds one, [`Run`] reads one through the worker-shared
//! [`crate::cache::BlockCache`], and [`TableCache`] bounds the open descriptors.

mod block;
mod bloom;
mod format;
mod reader;
mod table_cache;
mod writer;

#[cfg(test)]
mod tests;

pub use format::{FOOTER_LEN, RunMeta};
pub use reader::{Lookup, Run, RunIter, RunReadContext, RunStats};
pub use table_cache::{DEFAULT_MAX_OPEN_RUNS, TableCache, parse_run_file_name, run_file_name};
pub use writer::{MAX_VALUE_LEN, RunWriter, RunWriterOptions};
