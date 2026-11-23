//! `fs::os` on a simulated volume: open flags, file handles with a position, directory
//! handles, listings and renames on a [`FaultFs`].

use std::ffi::OsString;
use std::io::{self, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

use super::Space;
use crate::fs::{
    DescriptorInode, FaultFile, FaultFs, Fs, FsFile, FsKind, FsLock, FsStat, OpenMode,
};
use crate::sim::volumes::{self, CAPACITY_BYTES, DescriptorPath, Route};

/// `S_IFREG` and `S_IFDIR`.
const S_IFREG: u32 = 0o100_000;
const S_IFDIR: u32 = 0o040_000;
const BLOCK: u64 = 4096;

/// The open flags `OpenOptions` collected.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Flags {
    pub(super) read: bool,
    pub(super) write: bool,
    pub(super) append: bool,
    pub(super) truncate: bool,
    pub(super) create: bool,
    pub(super) create_new: bool,
    pub(super) mode: Option<u32>,
    pub(super) custom: i32,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// An open file or directory on a simulated volume.
pub(super) struct SimFile {
    fs: FaultFs,
    path: PathBuf,
    inode: DescriptorInode,
    descriptor: DescriptorPath,
    /// `None` for a directory.
    file: Option<FaultFile>,
    position: Mutex<u64>,
    append: bool,
    readable: bool,
    lock: Mutex<Option<FsLock>>,
}

impl std::fmt::Debug for SimFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimFile")
            .field("path", &self.path)
            .field("directory", &self.file.is_none())
            .field("locked", &lock(&self.lock).is_some())
            .finish_non_exhaustive()
    }
}

impl SimFile {
    /// `open(2)` with `flags` on `fs`.
    pub(super) fn open(fs: FaultFs, path: &Path, flags: &Flags) -> io::Result<SimFile> {
        let writable = flags.write || flags.append;
        if !flags.read && !writable {
            return Err(invalid("open with neither read nor write access"));
        }
        if (flags.create || flags.create_new || flags.truncate) && !writable {
            return Err(invalid("create or truncate without write access"));
        }
        let existing = match fs.stat(path) {
            Ok(stat) => Some(stat),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let kind = existing.map(|stat| stat.kind);
        if flags.custom & libc::O_DIRECTORY != 0 && kind != Some(FsKind::Dir) {
            return Err(match kind {
                None => io::ErrorKind::NotFound.into(),
                Some(_) => io::ErrorKind::NotADirectory.into(),
            });
        }
        let file = match kind {
            Some(FsKind::Dir) if flags.create_new => {
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            Some(FsKind::Dir) if writable => return Err(io::ErrorKind::IsADirectory.into()),
            Some(FsKind::Dir) => None,
            Some(FsKind::File) if flags.create_new => {
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            Some(FsKind::File) => {
                let mode = if writable {
                    OpenMode::ReadWrite
                } else {
                    OpenMode::Read
                };
                Some(fs.open_file(path, mode, None)?)
            }
            None if flags.create || flags.create_new => {
                Some(fs.open_file(path, OpenMode::CreateNew, flags.mode)?)
            }
            None => return Err(io::ErrorKind::NotFound.into()),
        };
        if let Some(file) = &file
            && flags.truncate
            && existing.is_some()
        {
            file.set_len(0)?;
        }
        let inode = match &file {
            Some(file) => file.descriptor_inode(),
            None => {
                DescriptorInode::Directory(existing.expect("opened directory").ino - fs.ino_base())
            }
        };
        let descriptor = DescriptorPath::new(fs.clone(), inode, path);
        Ok(SimFile {
            fs,
            path: path.to_path_buf(),
            inode,
            descriptor,
            file,
            position: Mutex::new(0),
            append: flags.append,
            readable: flags.read,
            lock: Mutex::new(None),
        })
    }

    pub(super) fn fd_path(&self) -> PathBuf {
        self.descriptor.path()
    }

    fn file(&self) -> io::Result<&FaultFile> {
        self.file
            .as_ref()
            .ok_or_else(|| io::ErrorKind::IsADirectory.into())
    }

    pub(super) fn metadata(&self) -> io::Result<FsStat> {
        match self.inode {
            DescriptorInode::File(_) => self.file()?.stat(),
            DescriptorInode::Directory(ino) => self.fs.stat_directory_descriptor(ino, &self.path),
        }
    }

    pub(super) fn sync_all(&self) -> io::Result<()> {
        match self.inode {
            DescriptorInode::File(_) => self.file()?.sync_all(),
            DescriptorInode::Directory(ino) => self.fs.sync_directory_descriptor(ino, &self.path),
        }
    }

    pub(super) fn sync_data(&self) -> io::Result<()> {
        match self.inode {
            DescriptorInode::File(_) => self.file()?.sync_data(),
            DescriptorInode::Directory(ino) => self.fs.sync_directory_descriptor(ino, &self.path),
        }
    }

    pub(super) fn set_len(&self, len: u64) -> io::Result<()> {
        self.file()
            .map_err(|_| invalid("set_len on a directory"))?
            .set_len(len)
    }

    pub(super) fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file()?.read_exact_at(buf, offset)
    }

    pub(super) fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file()?.write_all_at(buf, offset)
    }

    pub(super) fn try_lock_exclusive(&self) -> io::Result<bool> {
        let mut slot = lock(&self.lock);
        match self
            .fs
            .lock_descriptor(self.inode, &self.path, slot.is_some())
        {
            Ok(held) => {
                if let Some(held) = held {
                    *slot = Some(held);
                }
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn unlock(&self) {
        lock(&self.lock).take();
    }

    pub(super) fn statvfs(&self) -> io::Result<Space> {
        let used = self.fs.used_bytes().div_ceil(BLOCK);
        let blocks = CAPACITY_BYTES / BLOCK;
        Ok(Space {
            fsid: self.fs.device(),
            block_size: BLOCK,
            blocks,
            available: blocks.saturating_sub(used),
        })
    }

    pub(super) fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.readable {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        let file = self.file()?;
        let mut position = lock(&self.position);
        let len = file.len()?;
        if *position >= len || buf.is_empty() {
            return Ok(0);
        }
        let n = buf.len().min((len - *position) as usize);
        file.read_exact_at(&mut buf[..n], *position)?;
        *position += n as u64;
        Ok(n)
    }

    pub(super) fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let file = self.file()?;
        if !file.is_writable() {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        let mut position = lock(&self.position);
        let at = if self.append { file.len()? } else { *position };
        file.write_all_at(buf, at)?;
        *position = at + buf.len() as u64;
        Ok(buf.len())
    }

    pub(super) fn seek(&self, to: SeekFrom) -> io::Result<u64> {
        let mut position = lock(&self.position);
        let target = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::Current(delta) => position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.file()?.len()?.checked_add_signed(delta),
        };
        *position = target.ok_or_else(|| invalid("seek to a negative position"))?;
        Ok(*position)
    }
}

/// `st_mode` of a simulated inode: the type bits and the permission bits.
pub(super) fn st_mode(stat: &FsStat) -> u32 {
    let kind = match stat.kind {
        FsKind::File => S_IFREG,
        FsKind::Dir => S_IFDIR,
    };
    kind | stat.mode
}

/// One `read_dir` entry on a simulated volume.
#[derive(Debug)]
pub(super) struct SimEntry {
    fs: FaultFs,
    path: PathBuf,
    descriptor_relative: bool,
}

impl SimEntry {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn file_name(&self) -> OsString {
        self.path.file_name().unwrap_or_default().to_os_string()
    }

    pub(super) fn metadata(&self) -> io::Result<FsStat> {
        if self.descriptor_relative {
            let Route::Volume(fs, path) = volumes::route(&self.path)? else {
                return Err(io::ErrorKind::NotFound.into());
            };
            return fs.stat(&path);
        }
        self.fs.stat(&self.path)
    }
}

pub(super) fn read_dir(
    fs: FaultFs,
    dir: &Path,
    requested: &Path,
) -> io::Result<std::vec::IntoIter<SimEntry>> {
    let mut paths = fs.list(dir)?;
    paths.sort();
    let descriptor_relative = dir != requested;
    Ok(paths
        .into_iter()
        .map(|path| {
            let path = if descriptor_relative {
                requested.join(path.file_name().expect("directory entry has a name"))
            } else {
                path
            };
            SimEntry {
                fs: fs.clone(),
                path,
                descriptor_relative,
            }
        })
        .collect::<Vec<_>>()
        .into_iter())
}

pub(super) fn rename(fs: FaultFs, from: &Path, to: &Path, noreplace: bool) -> io::Result<()> {
    let Route::Volume(target, to) = volumes::route(to)? else {
        return Err(io::ErrorKind::CrossesDevices.into());
    };
    volumes::same_device(&fs, &target)?;
    if noreplace {
        fs.rename_noreplace(from, &to)
    } else {
        fs.rename(from, &to)
    }
}

pub(super) fn read(fs: FaultFs, path: &Path) -> io::Result<Vec<u8>> {
    let file = fs.open_file(path, OpenMode::Read, None)?;
    let mut bytes = vec![0; file.len()? as usize];
    file.read_exact_at(&mut bytes, 0)?;
    Ok(bytes)
}

pub(super) fn canonicalize(fs: FaultFs, path: &Path) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            other => resolved.push(other),
        }
    }
    fs.stat(&resolved)?;
    Ok(resolved)
}
