//! Private ephemeral payload files with shared retained-byte and file budgets.
//!
//! All disk I/O is synchronous: call on the designated bounded CPU/I/O executor,
//! never on a Tokio reactor thread. No lock spans disk I/O. Scratch is neither
//! fsynced nor recovery state. The caller supplies an existing trusted parent
//! and must keep it and the private descendants free of external mutation.
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpoolError {
    InvalidConfiguration,
    Capacity,
    Io,
    Poisoned,
}
impl fmt::Display for SpoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "invalid scratch configuration",
            Self::Capacity => "scratch capacity exhausted",
            Self::Io => "scratch I/O failed",
            Self::Poisoned => "scratch writer unavailable",
        })
    }
}
impl std::error::Error for SpoolError {}

/// Positive integer budgets. There are no implicit protocol-size caps.
#[derive(Clone, Copy, Debug)]
pub struct SpoolLimits {
    bytes: u64,
    files: u64,
}
impl SpoolLimits {
    pub fn new(bytes: u64, files: u64) -> Result<Self, SpoolError> {
        if bytes == 0 || files == 0 {
            return Err(SpoolError::InvalidConfiguration);
        }
        Ok(Self { bytes, files })
    }
}
#[derive(Default, Clone, Copy)]
struct Usage {
    bytes: u64,
    files: u64,
}
impl Usage {
    fn add(self, bytes: u64, files: u64, limits: SpoolLimits) -> Result<Self, SpoolError> {
        let next = Self {
            bytes: self.bytes.checked_add(bytes).ok_or(SpoolError::Capacity)?,
            files: self.files.checked_add(files).ok_or(SpoolError::Capacity)?,
        };
        if next.bytes > limits.bytes || next.files > limits.files {
            return Err(SpoolError::Capacity);
        }
        Ok(next)
    }
}
struct Root {
    path: PathBuf,
    limits: SpoolLimits,
    usage: Mutex<Usage>,
}
impl Drop for Root {
    fn drop(&mut self) {
        // Never scan or recursively delete. Retained files make this fail closed.
        let _ = fs::remove_dir(&self.path);
    }
}

/// Create once per process and share operations from this manager. Independent
/// managers have independent budgets; runtime wiring must not create one per job.
pub struct Spool {
    root: Arc<Root>,
}
impl Spool {
    pub fn new(parent: impl AsRef<Path>, limits: SpoolLimits) -> Result<Self, SpoolError> {
        let path = parent
            .as_ref()
            .join(format!("meshdb-spool-{}", Uuid::new_v4()));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|_| SpoolError::Io)?;
        // Preserve identity even when the caller supplied a relative parent.
        let canonical = match fs::canonicalize(&path) {
            Ok(path) => path,
            Err(_) => {
                let _ = fs::remove_dir(&path);
                return Err(SpoolError::Io);
            }
        };
        Ok(Self {
            root: Arc::new(Root {
                path: canonical,
                limits,
                usage: Mutex::default(),
            }),
        })
    }
    pub fn operation(&self, limits: SpoolLimits) -> SpoolOperation {
        SpoolOperation {
            inner: Arc::new(Operation {
                root: self.root.clone(),
                limits,
                usage: Mutex::default(),
            }),
        }
    }
}
struct Operation {
    root: Arc<Root>,
    limits: SpoolLimits,
    usage: Mutex<Usage>,
}
impl Operation {
    fn reserve(&self, bytes: u64, files: u64) -> Result<(), SpoolError> {
        // One consistent order makes acquisition across both budgets atomic.
        let mut process = self.root.usage.lock().map_err(|_| SpoolError::Poisoned)?;
        let mut operation = self.usage.lock().map_err(|_| SpoolError::Poisoned)?;
        let next_process = process.add(bytes, files, self.root.limits)?;
        let next_operation = operation.add(bytes, files, self.limits)?;
        *process = next_process;
        *operation = next_operation;
        Ok(())
    }
    fn release(&self, bytes: u64, files: u64) {
        let mut process = self.root.usage.lock().unwrap_or_else(|e| e.into_inner());
        let mut operation = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        process.bytes -= bytes;
        process.files -= files;
        operation.bytes -= bytes;
        operation.files -= files;
    }
}
/// Dropping admission does not invalidate its returned files or readers.
pub struct SpoolOperation {
    inner: Arc<Operation>,
}
impl SpoolOperation {
    pub fn writer(&self, payload_cap: u64) -> Result<SpoolWriter, SpoolError> {
        if payload_cap == 0 {
            return Err(SpoolError::InvalidConfiguration);
        }
        self.inner.reserve(0, 1)?;
        let path = self.inner.root.path.join(Uuid::new_v4().to_string());
        let file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => file,
            Err(_) => {
                self.inner.release(0, 1);
                return Err(SpoolError::Io);
            }
        };
        Ok(SpoolWriter {
            file: Some(file),
            owned: Some(OwnedFile {
                operation: self.inner.clone(),
                path,
                bytes: 0,
            }),
            payload_cap,
            poisoned: false,
        })
    }
}
struct OwnedFile {
    operation: Arc<Operation>,
    path: PathBuf,
    bytes: u64,
}
impl Drop for OwnedFile {
    fn drop(&mut self) {
        match fs::remove_file(&self.path) {
            Ok(()) => self.operation.release(self.bytes, 1),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.operation.release(self.bytes, 1)
            }
            // Keep charges for this manager's remaining lifetime. No GC/retry task.
            Err(_) => (),
        }
    }
}
/// Append borrowed chunks. Any failed append poisons the writer until drop.
pub struct SpoolWriter {
    file: Option<File>,
    owned: Option<OwnedFile>,
    payload_cap: u64,
    poisoned: bool,
}
impl SpoolWriter {
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), SpoolError> {
        if self.poisoned {
            return Err(SpoolError::Poisoned);
        }
        self.poisoned = true;
        let owned = self.owned.as_mut().ok_or(SpoolError::Poisoned)?;
        let count = u64::try_from(bytes.len()).map_err(|_| SpoolError::Capacity)?;
        let next = owned.bytes.checked_add(count).ok_or(SpoolError::Capacity)?;
        if next > self.payload_cap {
            return Err(SpoolError::Capacity);
        }
        owned.operation.reserve(count, 0)?;
        // Charge the whole attempted chunk, even if write_all partially fails.
        owned.bytes = next;
        self.file
            .as_mut()
            .ok_or(SpoolError::Poisoned)?
            .write_all(bytes)
            .map_err(|_| SpoolError::Io)?;
        self.poisoned = false;
        Ok(())
    }
    pub fn finish(mut self) -> Result<SpoolPayload, SpoolError> {
        if self.poisoned {
            return Err(SpoolError::Poisoned);
        }
        drop(self.file.take());
        Ok(SpoolPayload {
            owned: Arc::new(self.owned.take().ok_or(SpoolError::Poisoned)?),
        })
    }
}
impl Drop for SpoolWriter {
    fn drop(&mut self) {
        // Close before deletion, including unsuccessful/cancelled writers.
        drop(self.file.take());
        drop(self.owned.take());
    }
}
/// Immutable, length-bearing payload. Opening a reader is the only file access.
pub struct SpoolPayload {
    owned: Arc<OwnedFile>,
}
impl SpoolPayload {
    pub fn len(&self) -> u64 {
        self.owned.bytes
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn open(&self) -> Result<SpoolReader, SpoolError> {
        let file = File::open(&self.owned.path).map_err(|_| SpoolError::Io)?;
        Ok(SpoolReader {
            file,
            owned: self.owned.clone(),
            position: 0,
        })
    }
}
/// Independent bounded Read + Seek view; retains file and both quota charges.
/// I/O trait errors contain fixed text and no underlying filesystem error.
pub struct SpoolReader {
    file: File,
    owned: Arc<OwnedFile>,
    position: u64,
}
fn read_error() -> io::Error {
    io::Error::other("scratch read failed")
}
impl Read for SpoolReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.owned.bytes - self.position;
        let count = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        if count == 0 {
            return Ok(0);
        }
        let read = self
            .file
            .read(&mut buf[..count])
            .map_err(|_| read_error())?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "scratch payload truncated",
            ));
        }
        self.position += read as u64;
        Ok(read)
    }
}
impl Seek for SpoolReader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let next = match from {
            SeekFrom::Start(pos) => i128::from(pos),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.owned.bytes) + i128::from(offset),
        };
        if next < 0 || next > i128::from(self.owned.bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scratch seek outside payload",
            ));
        }
        let next = u64::try_from(next).map_err(|_| read_error())?;
        self.file
            .seek(SeekFrom::Start(next))
            .map_err(|_| read_error())?;
        self.position = next;
        Ok(next)
    }
}
macro_rules! opaque_debug {
    ($($name:ty),+ $(,)?) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(stringify!($name)) }
    })+ };
}
opaque_debug!(
    Spool,
    SpoolOperation,
    SpoolWriter,
    SpoolPayload,
    SpoolReader
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_error_keeps_attempted_bytes_charged_and_poisons_until_drop() {
        let parent = std::env::temp_dir().join(format!("meshdb-spool-fault-{}", Uuid::new_v4()));
        fs::create_dir(&parent).unwrap();
        let limits = SpoolLimits::new(5, 2).unwrap();
        let spool = Spool::new(&parent, limits).unwrap();
        let op = spool.operation(limits);
        let other = spool.operation(limits);
        let mut writer = op.writer(5).unwrap();
        writer.append(b"abc").unwrap();
        // Private fault injection: replace only our owned write descriptor with
        // a read-only descriptor. No public hook or external filesystem target.
        writer.file = Some(File::open(&writer.owned.as_ref().unwrap().path).unwrap());
        assert_eq!(writer.append(b"de"), Err(SpoolError::Io));
        assert_eq!(writer.append(b""), Err(SpoolError::Poisoned));
        let mut contender = other.writer(5).unwrap();
        assert_eq!(contender.append(b"x"), Err(SpoolError::Capacity));
        drop(contender);
        assert_eq!(writer.finish().unwrap_err(), SpoolError::Poisoned);
        let mut recovered = other.writer(5).unwrap();
        recovered.append(b"abcde").unwrap();
        drop(recovered);
        drop(other);
        drop(op);
        drop(spool);
        fs::remove_dir(parent).unwrap();
    }
}
