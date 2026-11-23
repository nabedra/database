//! The ambient filesystem for server code that works with paths rather than an injected
//! [`super::Fs`] (OLTP pool, mount identity, manifests).
//!
//! It mirrors the subset of `std::fs` those sites use. In a product build every item
//! forwards to `std::fs` (or `libc` for locks and `fstatvfs`), with `repr(transparent)`
//! newtypes and static dispatch. Under `sim`, inside a world and a node, each call
//! resolves the node's simulated volume for the path (`World::mount`); outside a world
//! it forwards to `std::fs` as in product.
//!
//! There is no escape to `std::fs::File` or a raw descriptor, so a site that compiles in
//! product also compiles under `sim`.

use std::ffi::OsString;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// Free space and identity of the filesystem holding a file (`fstatvfs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Space {
    /// `f_fsid`.
    pub fsid: u64,
    /// `f_frsize`, the unit of `blocks` and `available`.
    pub block_size: u64,
    /// `f_blocks`.
    pub blocks: u64,
    /// `f_bavail`, blocks available to an unprivileged user.
    pub available: u64,
}

/// An open file: `std::fs::File` in product.
#[derive(Debug)]
#[repr(transparent)]
pub struct File(std::fs::File);

impl File {
    /// [`std::fs::File::open`].
    #[inline]
    pub fn open(path: impl AsRef<Path>) -> io::Result<File> {
        sim_unsupported(path.as_ref())?;
        std::fs::File::open(path).map(File)
    }

    /// [`std::fs::File::create`].
    #[inline]
    pub fn create(path: impl AsRef<Path>) -> io::Result<File> {
        sim_unsupported(path.as_ref())?;
        std::fs::File::create(path).map(File)
    }

    /// [`std::fs::File::options`].
    #[inline]
    #[must_use]
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// [`std::fs::File::metadata`].
    #[inline]
    pub fn metadata(&self) -> io::Result<Metadata> {
        self.0.metadata().map(Metadata)
    }

    /// [`std::fs::File::sync_all`].
    #[inline]
    pub fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }

    /// [`std::fs::File::sync_data`].
    #[inline]
    pub fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    /// [`std::fs::File::set_len`].
    #[inline]
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    /// [`FileExt::read_exact_at`].
    #[inline]
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.0, buf, offset)
    }

    /// [`FileExt::write_all_at`].
    #[inline]
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        FileExt::write_all_at(&self.0, buf, offset)
    }

    /// `flock(LOCK_EX | LOCK_NB)`: `Ok(true)` when taken, `Ok(false)` when another open
    /// file description holds it.
    #[inline]
    pub fn try_lock_exclusive(&self) -> io::Result<bool> {
        // SAFETY: flock on a descriptor this File owns.
        if unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    /// `flock(LOCK_UN)`. Errors are ignored, as the sites that release a lock do.
    #[inline]
    pub fn unlock(&self) {
        // SAFETY: flock on a descriptor this File owns.
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }

    /// `fstatvfs` of the filesystem holding this file.
    #[inline]
    pub fn statvfs(&self) -> io::Result<Space> {
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: fstatvfs fills `stats` on success; it is read only then.
        if unsafe { libc::fstatvfs(self.0.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: initialised by the successful call above.
        let stats = unsafe { stats.assume_init() };
        #[allow(clippy::unnecessary_cast)] // the field widths differ between targets
        Ok(Space {
            fsid: stats.f_fsid as u64,
            block_size: stats.f_frsize as u64,
            blocks: stats.f_blocks as u64,
            available: stats.f_bavail as u64,
        })
    }

    /// A path that names this open file: `/proc/self/fd/<fd>` in product, so lookups
    /// relative to an open directory keep their meaning after a rename of an ancestor.
    /// Under `sim` it is the path the file was opened as.
    #[inline]
    #[must_use]
    pub fn fd_path(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.0.as_raw_fd()))
    }
}

impl Read for File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Read for &File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.0).read(buf)
    }
}

impl Write for File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Write for &File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&self.0).write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        (&self.0).flush()
    }
}

impl Seek for File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Seek for &File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&self.0).seek(pos)
    }
}

/// [`std::fs::OpenOptions`] with the Unix `mode` and `custom_flags` extensions.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct OpenOptions(std::fs::OpenOptions);

impl Default for OpenOptions {
    #[inline]
    fn default() -> Self {
        OpenOptions::new()
    }
}

impl OpenOptions {
    #[inline]
    #[must_use]
    pub fn new() -> OpenOptions {
        OpenOptions(std::fs::OpenOptions::new())
    }

    #[inline]
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.0.read(read);
        self
    }

    #[inline]
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.0.write(write);
        self
    }

    #[inline]
    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.0.append(append);
        self
    }

    #[inline]
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.0.truncate(truncate);
        self
    }

    #[inline]
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.0.create(create);
        self
    }

    #[inline]
    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.0.create_new(create_new);
        self
    }

    /// [`OpenOptionsExt::mode`]: the permission bits of a created file.
    #[inline]
    pub fn mode(&mut self, mode: u32) -> &mut OpenOptions {
        self.0.mode(mode);
        self
    }

    /// [`OpenOptionsExt::custom_flags`]. Under `sim`, `O_NOFOLLOW` and `O_DIRECTORY` are
    /// honoured and the other flags ignored.
    #[inline]
    pub fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
        self.0.custom_flags(flags);
        self
    }

    #[inline]
    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        sim_unsupported(path.as_ref())?;
        self.0.open(path).map(File)
    }
}

/// [`std::fs::Metadata`] with the Unix fields the server reads.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Metadata(std::fs::Metadata);

impl Metadata {
    #[inline]
    pub fn file_type(&self) -> FileType {
        FileType(self.0.file_type())
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Always false under `sim`: simulated volumes have no symlinks.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.0.is_symlink()
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    #[inline]
    pub fn dev(&self) -> u64 {
        self.0.dev()
    }

    #[inline]
    pub fn ino(&self) -> u64 {
        self.0.ino()
    }

    #[inline]
    pub fn nlink(&self) -> u64 {
        self.0.nlink()
    }

    /// `st_mode`: the file type and permission bits.
    #[inline]
    pub fn mode(&self) -> u32 {
        self.0.mode()
    }

    #[inline]
    pub fn uid(&self) -> u32 {
        self.0.uid()
    }

    #[inline]
    pub fn rdev(&self) -> u64 {
        self.0.rdev()
    }
}

/// [`std::fs::FileType`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct FileType(std::fs::FileType);

impl FileType {
    #[inline]
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.0.is_symlink()
    }

    #[inline]
    pub fn is_block_device(&self) -> bool {
        self.0.is_block_device()
    }
}

/// [`std::fs::ReadDir`].
#[derive(Debug)]
#[repr(transparent)]
pub struct ReadDir(std::fs::ReadDir);

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|entry| entry.map(DirEntry))
    }
}

/// [`std::fs::DirEntry`].
#[derive(Debug)]
#[repr(transparent)]
pub struct DirEntry(std::fs::DirEntry);

impl DirEntry {
    #[inline]
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    #[inline]
    pub fn file_name(&self) -> OsString {
        self.0.file_name()
    }

    #[inline]
    pub fn metadata(&self) -> io::Result<Metadata> {
        self.0.metadata().map(Metadata)
    }

    #[inline]
    pub fn file_type(&self) -> io::Result<FileType> {
        self.0.file_type().map(FileType)
    }
}

/// [`std::fs::metadata`].
#[inline]
pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sim_unsupported(path.as_ref())?;
    std::fs::metadata(path).map(Metadata)
}

/// [`std::fs::symlink_metadata`].
#[inline]
pub fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sim_unsupported(path.as_ref())?;
    std::fs::symlink_metadata(path).map(Metadata)
}

/// [`std::fs::read_dir`].
#[inline]
pub fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    sim_unsupported(path.as_ref())?;
    std::fs::read_dir(path).map(ReadDir)
}

/// [`std::fs::rename`].
#[inline]
pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(from.as_ref())?;
    std::fs::rename(from, to)
}

/// `renameat2(AT_FDCWD, from, AT_FDCWD, to, RENAME_NOREPLACE)`: fails with
/// `AlreadyExists` instead of replacing `to`.
#[cfg(target_os = "linux")]
#[inline]
pub fn rename_noreplace(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    sim_unsupported(from.as_ref())?;
    let from = std::ffi::CString::new(from.as_ref().as_os_str().as_bytes())?;
    let to = std::ffi::CString::new(to.as_ref().as_os_str().as_bytes())?;
    // SAFETY: both paths are NUL-terminated and outlive the call.
    let status = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// [`std::fs::remove_file`].
#[inline]
pub fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::remove_file(path)
}

/// [`std::fs::remove_dir`].
#[inline]
pub fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::remove_dir(path)
}

/// [`std::fs::remove_dir_all`].
#[inline]
pub fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::remove_dir_all(path)
}

/// [`std::fs::create_dir`].
#[inline]
pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::create_dir(path)
}

/// [`std::fs::create_dir_all`].
#[inline]
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::create_dir_all(path)
}

/// [`std::fs::read`].
#[inline]
pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    sim_unsupported(path.as_ref())?;
    std::fs::read(path)
}

/// [`std::fs::read_to_string`].
#[inline]
pub fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    sim_unsupported(path.as_ref())?;
    std::fs::read_to_string(path)
}

/// [`std::fs::write`].
#[inline]
pub fn write(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::write(path, bytes)
}

/// `std::fs::set_permissions(path, Permissions::from_mode(mode))`.
#[inline]
pub fn set_mode(path: impl AsRef<Path>, mode: u32) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// [`Path::try_exists`].
#[inline]
pub fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    sim_unsupported(path.as_ref())?;
    path.as_ref().try_exists()
}

/// [`std::fs::canonicalize`]. Under `sim` the path is made absolute and normalised, and
/// it must exist (simulated volumes have no symlinks).
#[inline]
pub fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    sim_unsupported(path.as_ref())?;
    std::fs::canonicalize(path)
}

/// Makes renames, creations and removals inside the directory `path` durable: the
/// `File::open(dir)?.sync_all()` idiom.
#[inline]
pub fn sync_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sim_unsupported(path.as_ref())?;
    std::fs::File::open(path)?.sync_all()
}

/// The effective user id (`geteuid`). Under `sim` inside a world it is the uid that owns
/// every file on a simulated volume.
#[inline]
pub fn euid() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

/// Skeleton guard: the simulated branch lands in P9-RT (c). Until then a call inside a
/// simulated node fails instead of touching the host filesystem.
#[inline(always)]
fn sim_unsupported(path: &Path) -> io::Result<()> {
    #[cfg(feature = "sim")]
    if crate::sim::world().is_some() && crate::sim::current_node().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("fs::os under sim lands in P9-RT (c): {}", path.display()),
        ));
    }
    let _ = path;
    Ok(())
}
