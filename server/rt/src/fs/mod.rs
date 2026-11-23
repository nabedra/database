//! The filesystem seam. Every byte nabedra-kv reads or writes goes through [`Fs`], so
//! tests can inject crashes, torn writes, bit flips and I/O errors with [`FaultFs`].
//! nabedra-kv re-exports this module as `nabedra_kv::fs`, so its paths are unchanged.
//!
//! Sync semantics: [`FsFile::sync_data`] is `fdatasync` on Linux and `F_FULLFSYNC` on
//! Apple targets (what `std::fs::File::sync_data` does); [`FsFile::sync_all`] is `fsync`
//! or `F_FULLFSYNC`. A sync that returns an error must never be retried: nabedra-kv
//! latches `Error::RecoveryRequired`.

use std::any::Any;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod fault;
mod real;

pub use fault::*;
pub use real::RealFs;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    /// Open an existing file read-only.
    Read,
    /// Open an existing file for reading and writing.
    ReadWrite,
    /// Create a new file for reading and writing; fails if it exists.
    CreateNew,
}

/// What a path names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FsKind {
    File,
    Dir,
}

/// The metadata of a file or directory, as [`FaultFs::stat`] reports it (the fields of
/// `lstat` the server reads).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsStat {
    pub kind: FsKind,
    pub len: u64,
    pub dev: u64,
    pub ino: u64,
    pub nlink: u64,
    /// Permission bits only (`st_mode & 0o7777`).
    pub mode: u32,
    pub uid: u32,
}

/// Holds an exclusive advisory lock until dropped.
pub struct FsLock {
    _guard: Box<dyn Any + Send + Sync>,
}

impl FsLock {
    pub fn new(guard: Box<dyn Any + Send + Sync>) -> FsLock {
        FsLock { _guard: guard }
    }
}

pub trait Fs: Send + Sync + 'static {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> io::Result<bool>;
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Entries of `dir` (full paths), in no particular order.
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    /// Makes renames, creations and removals inside `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Takes an exclusive lock on `path` (created if missing); fails if another holder has it.
    fn lock(&self, path: &Path) -> io::Result<FsLock>;
}

#[allow(clippy::len_without_is_empty)]
pub trait FsFile: Send + Sync {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    /// `fdatasync` on Linux, `F_FULLFSYNC` on Apple targets.
    fn sync_data(&self) -> io::Result<()>;
    /// `fsync` on Linux, `F_FULLFSYNC` on Apple targets.
    fn sync_all(&self) -> io::Result<()>;
    /// Reserves `len` bytes so later overwrites inside it change no file metadata.
    fn preallocate(&self, len: u64) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    /// Hint that `[offset, offset + len)` will not be read soon (`posix_fadvise(DONTNEED)`).
    /// A no-op where unsupported.
    fn advise_dont_need(&self, _offset: u64, _len: u64) {}
}

/// The default production filesystem.
pub fn real() -> Arc<dyn Fs> {
    Arc::new(RealFs)
}
