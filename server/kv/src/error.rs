//! The engine's error type.

use std::fmt;
use std::io;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The file an error refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileId {
    Journal {
        segment: u64,
    },
    Run {
        id: u64,
    },
    Manifest {
        number: u64,
    },
    Current,
    Lock,
    /// A value read through a journal locator.
    JournalValue {
        segment: u64,
    },
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Stored bytes failed a checksum or a structural check. Never retried.
    Corruption {
        file: FileId,
        offset: u64,
        what: &'static str,
    },
    /// An I/O error other than running out of space.
    Io(io::ErrorKind),
    /// The device is full. No data was corrupted; work can resume once space frees up.
    NoSpace,
    /// A write or sync failed earlier. Its outcome is unknown and the in-memory state may
    /// be ahead of the disk, so the store refuses all further work until it is reopened.
    RecoveryRequired,
    /// The store was closed.
    Closed,
    /// The directory holds a store this build cannot open.
    UnsupportedFormat { found: String },
    /// A caller passed something the engine cannot accept (for example a batch above the limit).
    InvalidArgument(&'static str),
}

impl Error {
    /// True for errors after which the outcome of a write is unknown.
    pub fn is_ambiguous(&self) -> bool {
        matches!(self, Error::RecoveryRequired | Error::Io(_))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Corruption { file, offset, what } => {
                write!(
                    f,
                    "storage corruption in {file:?} at offset {offset}: {what}"
                )
            }
            Error::Io(kind) => write!(f, "storage I/O error: {kind}"),
            Error::NoSpace => f.write_str("storage device is full"),
            Error::RecoveryRequired => {
                f.write_str("an earlier storage write failed; reopen the store to recover")
            }
            Error::Closed => f.write_str("store is closed"),
            Error::UnsupportedFormat { found } => write!(
                f,
                "unsupported store format ({found}); this build only opens nabedra-kv stores"
            ),
            Error::InvalidArgument(what) => write!(f, "invalid argument: {what}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        #[cfg(unix)]
        if error.raw_os_error() == Some(libc::ENOSPC) {
            return Error::NoSpace;
        }
        match error.kind() {
            io::ErrorKind::StorageFull => Error::NoSpace,
            kind => Error::Io(kind),
        }
    }
}
