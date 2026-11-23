//! The ambient filesystem for server code that works with paths rather than an injected
//! [`super::Fs`] (OLTP pool, mount identity, manifests).
//!
//! It mirrors the subset of `std::fs` those sites use. In a product build every item
//! forwards to `std::fs` (or `libc` for locks, `renameat2` and `fstatvfs`), through
//! `repr(transparent)` newtypes and static dispatch. Under `sim`, inside a simulated node
//! that has volumes (`sim::World::mount`), each call goes to the node's volume for the
//! path, a [`super::FaultFs`]; elsewhere it forwards to `std::fs` as in product.
//!
//! There is no escape to `std::fs::File` or a raw descriptor, so a site that compiles in
//! product also compiles under `sim`. On a simulated volume, `custom_flags` honours
//! `O_DIRECTORY` and ignores the rest (there are no symlinks, so `O_NOFOLLOW` holds);
//! modes are set exactly as given, without a umask; `read_dir` lists entries in path
//! order.

use std::ffi::OsString;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[cfg(feature = "sim")]
mod sim;

/// Runs `$sim` with the volume and the resolved path. Expands to nothing in product builds.
macro_rules! on_volume {
    ($path:expr, $fs:ident, $resolved:ident => $sim:expr) => {
        #[cfg(feature = "sim")]
        if let crate::sim::volumes::Route::Volume($fs, $resolved) =
            crate::sim::volumes::route(::std::convert::AsRef::<Path>::as_ref(&$path))?
        {
            return $sim;
        }
    };
}

/// `$std` on the std value in product; under `sim`, `$sim` on a simulated one.
macro_rules! dispatch {
    ($value:expr, $std:ident => $on_std:expr, $sim:ident => $on_sim:expr) => {{
        #[cfg(feature = "sim")]
        let result = match &$value {
            Inner::Std($std) => $on_std,
            Inner::Sim($sim) => $on_sim,
        };
        #[cfg(not(feature = "sim"))]
        let result = {
            let $std = &$value;
            $on_std
        };
        result
    }};
}

/// The value type of a wrapper: the std type in product, under `sim` the std type or the
/// simulated one. The simulated type is not resolved in a product build.
#[cfg(not(feature = "sim"))]
macro_rules! inner {
    ($std:ty, $sim:ty) => {
        $std
    };
}

#[cfg(feature = "sim")]
macro_rules! inner {
    ($std:ty, $sim:ty) => { Inner<$std, $sim> };
}

#[cfg(feature = "sim")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Inner<S, M> {
    Std(S),
    Sim(M),
}

/// A std value as a wrapper's value.
#[cfg(not(feature = "sim"))]
#[inline(always)]
fn wrap<S>(value: S) -> S {
    value
}

#[cfg(feature = "sim")]
#[inline]
fn wrap<S, M>(value: S) -> Inner<S, M> {
    Inner::Std(value)
}

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
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct File(inner!(std::fs::File, sim::SimFile));

impl File {
    /// [`std::fs::File::open`].
    #[inline]
    pub fn open(path: impl AsRef<Path>) -> io::Result<File> {
        on_volume!(path, fs, resolved => OpenOptions::new().read(true).open_on(fs, resolved.as_ref()));
        std::fs::File::open(path).map(|file| File(wrap(file)))
    }

    /// [`std::fs::File::create`].
    #[inline]
    pub fn create(path: impl AsRef<Path>) -> io::Result<File> {
        on_volume!(path, fs, resolved => OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_on(fs, resolved.as_ref()));
        std::fs::File::create(path).map(|file| File(wrap(file)))
    }

    /// [`std::fs::File::options`].
    #[inline]
    #[must_use]
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// [`std::fs::File::metadata`] (`fstat`).
    #[inline]
    pub fn metadata(&self) -> io::Result<Metadata> {
        dispatch!(self.0, file => file.metadata().map(|m| Metadata(wrap(m))),
            file => file.metadata().map(|stat| Metadata(Inner::Sim(stat))))
    }

    /// [`std::fs::File::sync_all`].
    #[inline]
    pub fn sync_all(&self) -> io::Result<()> {
        dispatch!(self.0, file => file.sync_all(), file => file.sync_all())
    }

    /// [`std::fs::File::sync_data`].
    #[inline]
    pub fn sync_data(&self) -> io::Result<()> {
        dispatch!(self.0, file => file.sync_data(), file => file.sync_data())
    }

    /// [`std::fs::File::set_len`].
    #[inline]
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        dispatch!(self.0, file => file.set_len(len), file => file.set_len(len))
    }

    /// [`FileExt::read_exact_at`].
    #[inline]
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        dispatch!(self.0, file => FileExt::read_exact_at(file, buf, offset),
            file => file.read_exact_at(buf, offset))
    }

    /// [`FileExt::write_all_at`].
    #[inline]
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        dispatch!(self.0, file => FileExt::write_all_at(file, buf, offset),
            file => file.write_all_at(buf, offset))
    }

    /// `flock(LOCK_EX | LOCK_NB)`: `Ok(true)` when taken, `Ok(false)` when another open
    /// file description holds it.
    #[inline]
    pub fn try_lock_exclusive(&self) -> io::Result<bool> {
        dispatch!(self.0, file => std_try_lock(file), file => file.try_lock_exclusive())
    }

    /// `flock(LOCK_UN)`. Errors are ignored, as the sites that release a lock do.
    #[inline]
    pub fn unlock(&self) {
        dispatch!(self.0, file => {
            // SAFETY: flock on a descriptor this File owns.
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        }, file => file.unlock())
    }

    /// `fstatvfs` of the filesystem holding this file.
    #[inline]
    pub fn statvfs(&self) -> io::Result<Space> {
        dispatch!(self.0, file => std_statvfs(file), file => file.statvfs())
    }

    /// A path that names this open file: `/proc/self/fd/<fd>` in product, so lookups
    /// relative to an open directory keep their meaning after a rename of an ancestor.
    /// Under `sim`, the alias is usable within its world and node incarnation while
    /// this descriptor is open and its inode has a name. Descriptor-relative paths on
    /// a simulated volume cannot cross another mount or traverse `..`.
    #[inline]
    #[must_use]
    pub fn fd_path(&self) -> PathBuf {
        dispatch!(self.0, file => PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd())),
            file => file.fd_path())
    }
}

#[inline]
fn std_try_lock(file: &std::fs::File) -> io::Result<bool> {
    // SAFETY: flock on a descriptor the caller's File owns.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[inline]
fn std_statvfs(file: &std::fs::File) -> io::Result<Space> {
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: fstatvfs fills `stats` on success; it is read only then.
    if unsafe { libc::fstatvfs(file.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
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

impl Read for File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Read for &File {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        dispatch!(self.0, file => { let mut file = file; file.read(buf) }, file => file.read(buf))
    }
}

impl Write for File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl Write for &File {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        dispatch!(self.0, file => { let mut file = file; file.write(buf) }, file => file.write(buf))
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        dispatch!(self.0, file => { let mut file = file; file.flush() }, _file => Ok(()))
    }
}

impl Seek for File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&*self).seek(pos)
    }
}

impl Seek for &File {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        dispatch!(self.0, file => { let mut file = file; file.seek(pos) }, file => file.seek(pos))
    }
}

/// [`std::fs::OpenOptions`] with the Unix `mode` and `custom_flags` extensions.
#[derive(Clone, Debug)]
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct OpenOptions {
    std: std::fs::OpenOptions,
    #[cfg(feature = "sim")]
    flags: sim::Flags,
}

impl Default for OpenOptions {
    #[inline]
    fn default() -> Self {
        OpenOptions::new()
    }
}

/// Records `$field = $value` in the simulated flags under `sim`.
macro_rules! flag {
    ($self:ident . $field:ident = $value:expr) => {
        #[cfg(feature = "sim")]
        {
            $self.flags.$field = $value;
        }
    };
}

impl OpenOptions {
    #[inline]
    #[must_use]
    pub fn new() -> OpenOptions {
        OpenOptions {
            std: std::fs::OpenOptions::new(),
            #[cfg(feature = "sim")]
            flags: sim::Flags::default(),
        }
    }

    #[inline]
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.std.read(read);
        flag!(self.read = read);
        self
    }

    #[inline]
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.std.write(write);
        flag!(self.write = write);
        self
    }

    #[inline]
    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.std.append(append);
        flag!(self.append = append);
        self
    }

    #[inline]
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.std.truncate(truncate);
        flag!(self.truncate = truncate);
        self
    }

    #[inline]
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.std.create(create);
        flag!(self.create = create);
        self
    }

    #[inline]
    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.std.create_new(create_new);
        flag!(self.create_new = create_new);
        self
    }

    /// [`OpenOptionsExt::mode`]: the permission bits of a created file.
    #[inline]
    pub fn mode(&mut self, mode: u32) -> &mut OpenOptions {
        self.std.mode(mode);
        flag!(self.mode = Some(mode));
        self
    }

    /// [`OpenOptionsExt::custom_flags`]. Under `sim`, `O_DIRECTORY` is honoured and the
    /// other flags are ignored.
    #[inline]
    pub fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
        self.std.custom_flags(flags);
        flag!(self.custom = flags);
        self
    }

    #[inline]
    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        on_volume!(path, fs, resolved => self.open_on(fs, resolved.as_ref()));
        self.std.open(path).map(|file| File(wrap(file)))
    }

    #[cfg(feature = "sim")]
    fn open_on(&self, fs: super::FaultFs, path: &Path) -> io::Result<File> {
        sim::SimFile::open(fs, path, &self.flags).map(|file| File(Inner::Sim(file)))
    }
}

/// [`std::fs::Metadata`] with the Unix fields the server reads.
#[derive(Clone, Debug)]
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct Metadata(inner!(std::fs::Metadata, super::FsStat));

impl Metadata {
    #[inline]
    pub fn file_type(&self) -> FileType {
        dispatch!(self.0, meta => FileType(wrap(meta.file_type())),
            stat => FileType(Inner::Sim(stat.kind)))
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    /// Always false on a simulated volume, which has no symlinks.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        dispatch!(self.0, meta => meta.len(), stat => stat.len)
    }

    #[inline]
    pub fn dev(&self) -> u64 {
        dispatch!(self.0, meta => meta.dev(), stat => stat.dev)
    }

    #[inline]
    pub fn ino(&self) -> u64 {
        dispatch!(self.0, meta => meta.ino(), stat => stat.ino)
    }

    #[inline]
    pub fn nlink(&self) -> u64 {
        dispatch!(self.0, meta => meta.nlink(), stat => stat.nlink)
    }

    /// `st_mode`: the file type and permission bits.
    #[inline]
    pub fn mode(&self) -> u32 {
        dispatch!(self.0, meta => meta.mode(), stat => sim::st_mode(stat))
    }

    #[inline]
    pub fn uid(&self) -> u32 {
        dispatch!(self.0, meta => meta.uid(), stat => stat.uid)
    }

    /// `st_rdev`: 0 for every file on a simulated volume.
    #[inline]
    pub fn rdev(&self) -> u64 {
        dispatch!(self.0, meta => meta.rdev(), _stat => 0)
    }
}

/// [`std::fs::FileType`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct FileType(inner!(std::fs::FileType, super::FsKind));

impl FileType {
    #[inline]
    pub fn is_dir(&self) -> bool {
        dispatch!(self.0, kind => kind.is_dir(), kind => *kind == super::FsKind::Dir)
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        dispatch!(self.0, kind => kind.is_file(), kind => *kind == super::FsKind::File)
    }

    #[inline]
    pub fn is_symlink(&self) -> bool {
        dispatch!(self.0, kind => kind.is_symlink(), _kind => false)
    }

    #[inline]
    pub fn is_block_device(&self) -> bool {
        dispatch!(self.0, kind => kind.is_block_device(), _kind => false)
    }
}

/// [`std::fs::ReadDir`]. On a simulated volume the entries come in path order.
#[derive(Debug)]
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct ReadDir(inner!(std::fs::ReadDir, std::vec::IntoIter<sim::SimEntry>));

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        #[cfg(feature = "sim")]
        let next = match &mut self.0 {
            Inner::Std(entries) => entries.next().map(|entry| entry.map(|e| DirEntry(wrap(e)))),
            Inner::Sim(entries) => entries.next().map(|entry| Ok(DirEntry(Inner::Sim(entry)))),
        };
        #[cfg(not(feature = "sim"))]
        let next = self.0.next().map(|entry| entry.map(|e| DirEntry(wrap(e))));
        next
    }
}

/// [`std::fs::DirEntry`].
#[derive(Debug)]
#[cfg_attr(not(feature = "sim"), repr(transparent))]
pub struct DirEntry(inner!(std::fs::DirEntry, sim::SimEntry));

impl DirEntry {
    #[inline]
    pub fn path(&self) -> PathBuf {
        dispatch!(self.0, entry => entry.path(), entry => entry.path().to_path_buf())
    }

    #[inline]
    pub fn file_name(&self) -> OsString {
        dispatch!(self.0, entry => entry.file_name(), entry => entry.file_name())
    }

    #[inline]
    pub fn metadata(&self) -> io::Result<Metadata> {
        dispatch!(self.0, entry => entry.metadata().map(|m| Metadata(wrap(m))),
            entry => entry.metadata().map(|stat| Metadata(Inner::Sim(stat))))
    }

    #[inline]
    pub fn file_type(&self) -> io::Result<FileType> {
        dispatch!(self.0, entry => entry.file_type().map(|t| FileType(wrap(t))),
            entry => entry.metadata().map(|stat| FileType(Inner::Sim(stat.kind))))
    }
}

/// [`std::fs::metadata`].
#[inline]
pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    on_volume!(path, fs, resolved => fs.stat(resolved.as_ref()).map(|stat| Metadata(Inner::Sim(stat))));
    std::fs::metadata(path).map(|meta| Metadata(wrap(meta)))
}

/// [`std::fs::symlink_metadata`].
#[inline]
pub fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    on_volume!(path, fs, resolved => fs.stat(resolved.as_ref()).map(|stat| Metadata(Inner::Sim(stat))));
    std::fs::symlink_metadata(path).map(|meta| Metadata(wrap(meta)))
}

/// [`std::fs::read_dir`].
#[inline]
pub fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    on_volume!(path, fs, resolved => {
        sim::read_dir(fs, resolved.as_ref(), path.as_ref()).map(|e| ReadDir(Inner::Sim(e)))
    });
    std::fs::read_dir(path).map(|entries| ReadDir(wrap(entries)))
}

/// [`std::fs::rename`].
#[inline]
pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(from, fs, resolved => sim::rename(fs, resolved.as_ref(), to.as_ref(), false));
    std::fs::rename(from, to)
}

/// `renameat2(AT_FDCWD, from, AT_FDCWD, to, RENAME_NOREPLACE)`: fails with
/// `AlreadyExists` instead of replacing `to`.
#[cfg(target_os = "linux")]
#[inline]
pub fn rename_noreplace(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    on_volume!(from, fs, resolved => sim::rename(fs, resolved.as_ref(), to.as_ref(), true));
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
    on_volume!(path, fs, resolved => super::Fs::remove(&fs, resolved.as_ref()));
    std::fs::remove_file(path)
}

/// [`std::fs::remove_dir`].
#[inline]
pub fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(path, fs, resolved => fs.remove_dir(resolved.as_ref()));
    std::fs::remove_dir(path)
}

/// [`std::fs::remove_dir_all`].
#[inline]
pub fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(path, fs, resolved => fs.remove_dir_all(resolved.as_ref()));
    std::fs::remove_dir_all(path)
}

/// [`std::fs::create_dir`].
#[inline]
pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(path, fs, resolved => fs.create_dir(resolved.as_ref()));
    std::fs::create_dir(path)
}

/// [`std::fs::create_dir_all`].
#[inline]
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(path, fs, resolved => super::Fs::create_dir_all(&fs, resolved.as_ref()));
    std::fs::create_dir_all(path)
}

/// [`std::fs::read`].
#[inline]
pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    on_volume!(path, fs, resolved => sim::read(fs, resolved.as_ref()));
    std::fs::read(path)
}

/// [`std::fs::read_to_string`].
#[inline]
pub fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    on_volume!(path, fs, resolved => sim::read(fs, resolved.as_ref()).and_then(|bytes| {
        String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }));
    std::fs::read_to_string(path)
}

/// [`std::fs::write`].
#[inline]
pub fn write(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> io::Result<()> {
    on_volume!(path, _fs, _resolved => File::create(path.as_ref())?.write_all(bytes.as_ref()));
    std::fs::write(path, bytes)
}

/// `std::fs::set_permissions(path, Permissions::from_mode(mode))`.
#[inline]
pub fn set_mode(path: impl AsRef<Path>, mode: u32) -> io::Result<()> {
    on_volume!(path, fs, resolved => fs.set_mode(resolved.as_ref(), mode));
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// [`Path::try_exists`].
#[inline]
pub fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    on_volume!(path, fs, resolved => super::Fs::exists(&fs, resolved.as_ref()));
    path.as_ref().try_exists()
}

/// [`std::fs::canonicalize`]. On a simulated volume the path must be absolute and must
/// exist; it comes back with `.` and `..` resolved (there are no symlinks).
#[inline]
pub fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    on_volume!(path, fs, resolved => sim::canonicalize(fs, resolved.as_ref()));
    std::fs::canonicalize(path)
}

/// Makes renames, creations and removals inside the directory `path` durable: the
/// `File::open(dir)?.sync_all()` idiom.
#[inline]
pub fn sync_dir(path: impl AsRef<Path>) -> io::Result<()> {
    on_volume!(path, fs, resolved => super::Fs::sync_dir(&fs, resolved.as_ref()));
    std::fs::File::open(path)?.sync_all()
}

/// The effective user id (`geteuid`). In a simulated node with volumes it is the uid that
/// owns every file on them ([`super::FaultFs::UID`]).
#[inline]
pub fn euid() -> u32 {
    #[cfg(feature = "sim")]
    if crate::sim::volumes::in_mounted_node() {
        return super::FaultFs::UID;
    }
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
pub(crate) mod tests;
