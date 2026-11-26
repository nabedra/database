//! The NVMe store of a search worker: raw vectors and code extensions (plan section 5,
//! Structure).
//!
//! - [`Vectors`] holds the f32 vector of every row version that has an entry. [`RamVectors`]
//!   keeps them in memory; [`NvmeVectors`] in one append-only file per core of packed records on
//!   `O_DIRECT` pages, each page with a CRC32C, reused only after the row map released the
//!   version. An index without a store keeps today's behaviour: vectors stay in its rows.
//! - [`Extensions`] holds 4-bit code extensions in RAM while the code [`Memory`] lasts, otherwise
//!   on NVMe.
//! - I/O goes through io_uring, or through positioned reads on a thread pool when io_uring is
//!   unavailable or blocked, chosen at run time ([`Backend`]).
//!
//! Restart: the files hold nothing a restart needs. Checkpoints carry raw vectors (decision L4),
//! open empties the files, and recovery writes every restored vector again, so the data path
//! needs no fsync, and a crash can leave nothing that a later read would take for a vector.
//!
//! [`Memory`]: crate::runtime::Memory

mod disk;
mod ext;
mod file;
mod pages;
mod vectors;

#[cfg(test)]
mod tests;

pub use disk::{Backend, Disk, PreadDisk, Req, open};
pub use ext::{ExtKey, Extensions};
pub use file::{FileStats, RecordFile};
pub use pages::{PAGE, Pages};
pub use vectors::{NvmeVectors, RamVectors, Stats, VecKey, Vectors};
