//! [`Fs`] over the operating system's filesystem.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Fs, FsFile, FsLock, OpenMode};

#[derive(Clone, Copy, Debug, Default)]
pub struct RealFs;

struct RealFile(File);

impl Fs for RealFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        let mut options = OpenOptions::new();
        match mode {
            OpenMode::Read => options.read(true),
            OpenMode::ReadWrite => options.read(true).write(true),
            OpenMode::CreateNew => options.read(true).write(true).create_new(true),
        };
        Ok(Arc::new(RealFile(options.open(path)?)))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(dir)?
            .map(|entry| entry.map(|e| e.path()))
            .collect()
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        sync_directory(&File::open(dir)?)
    }

    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        lock_exclusive(&file)?;
        Ok(FsLock::new(Box::new(RealLock(file))))
    }
}

/// The store directory lock. A flock belongs to the open file description, and
/// a child forked by any thread of this process shares that description until it
/// execs or exits. Closing only our descriptor would leave the store locked for
/// that long, so a reopen right after a close could fail with `WouldBlock`.
/// Dropping the lock therefore unlocks the description explicitly first.
struct RealLock(File);

impl Drop for RealLock {
    fn drop(&mut self) {
        unlock(&self.0);
    }
}

impl FsFile for RealFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        positioned::write_all_at(&self.0, buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        positioned::read_exact_at(&self.0, buf, offset)
    }

    fn sync_data(&self) -> io::Result<()> {
        // std uses fdatasync on Linux and F_FULLFSYNC on Apple targets.
        self.0.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }

    fn preallocate(&self, len: u64) -> io::Result<()> {
        preallocate(&self.0, len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn advise_dont_need(&self, offset: u64, len: u64) {
        advise_dont_need(&self.0, offset, len);
    }
}

#[cfg(unix)]
mod positioned {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

    pub fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
        file.write_all_at(buf, offset)
    }

    pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
        file.read_exact_at(buf, offset)
    }
}

#[cfg(windows)]
mod positioned {
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::FileExt;

    pub fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = file.seek_write(buf, offset)?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            buf = &buf[n..];
            offset += n as u64;
        }
        Ok(())
    }

    pub fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = file.seek_read(buf, offset)?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            buf = &mut buf[n..];
            offset += n as u64;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn lock_exclusive(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: flock on a valid, owned descriptor.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn unlock(file: &File) {
    use std::os::fd::AsRawFd;
    // SAFETY: flock on a valid, owned descriptor.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

#[cfg(not(unix))]
fn unlock(file: &File) {
    let _ = file.unlock();
}

#[cfg(not(unix))]
fn lock_exclusive(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|e| match e {
        fs::TryLockError::Error(e) => e,
        fs::TryLockError::WouldBlock => io::ErrorKind::WouldBlock.into(),
    })
}

#[cfg(target_os = "linux")]
fn preallocate(file: &File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let len =
        libc::off_t::try_from(len).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: fallocate on a valid, owned descriptor; mode 0 allocates and extends.
    loop {
        if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            // Filesystems without fallocate (for example some overlay setups) still work.
            Some(libc::EOPNOTSUPP) => return extend(file, len as u64),
            _ => return Err(error),
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate(file: &File, len: u64) -> io::Result<()> {
    extend(file, len)
}

fn extend(file: &File, len: u64) -> io::Result<()> {
    if file.metadata()?.len() < len {
        file.set_len(len)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_dont_need(file: &File, offset: u64, len: u64) {
    use std::os::fd::AsRawFd;
    let (Ok(offset), Ok(len)) = (libc::off_t::try_from(offset), libc::off_t::try_from(len)) else {
        return;
    };
    // SAFETY: advisory call on a valid, owned descriptor; failure is harmless.
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_dont_need(_file: &File, _offset: u64, _len: u64) {}

/// Directories: `fsync` on Linux. On Apple targets `F_FULLFSYNC` (what `sync_all` does)
/// is tried first and plain `fsync` is used where the filesystem rejects it for a directory.
fn sync_directory(dir: &File) -> io::Result<()> {
    match dir.sync_all() {
        #[cfg(target_vendor = "apple")]
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOTSUP | libc::EINVAL | libc::ENOTTY)
            ) =>
        {
            use std::os::fd::AsRawFd;
            // SAFETY: fsync on a valid, owned descriptor.
            if unsafe { libc::fsync(dir.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("nabedra-kv-realfs-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn real_fs_round_trips_bytes_syncs_and_locks() {
        let dir = temp_dir("basic");
        let fs = RealFs;
        let path = dir.join("f");
        let file = fs.open(&path, OpenMode::CreateNew).unwrap();
        assert!(fs.open(&path, OpenMode::CreateNew).is_err());
        file.preallocate(8192).unwrap();
        assert!(file.len().unwrap() >= 8192);
        file.write_all_at(b"hello", 4096).unwrap();
        file.sync_data().unwrap();
        file.sync_all().unwrap();
        let mut buf = [0u8; 5];
        fs.open(&path, OpenMode::Read)
            .unwrap()
            .read_exact_at(&mut buf, 4096)
            .unwrap();
        assert_eq!(&buf, b"hello");
        let mut zeros = [1u8; 16];
        file.read_exact_at(&mut zeros, 0).unwrap();
        assert_eq!(zeros, [0u8; 16]);
        fs.sync_dir(&dir).unwrap();

        let lock = fs.lock(&dir.join("LOCK")).unwrap();
        assert!(fs.lock(&dir.join("LOCK")).is_err());
        drop(lock);
        fs.lock(&dir.join("LOCK")).unwrap();

        fs.rename(&path, &dir.join("g")).unwrap();
        assert!(!fs.exists(&path).unwrap());
        assert_eq!(fs.list(&dir).unwrap().len(), 2);
        fs.remove(&dir.join("g")).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
}
