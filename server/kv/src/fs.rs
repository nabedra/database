//! The filesystem seam. It lives in `nabedra_rt::fs` so server code and the simulator can
//! share it; every `nabedra_kv::fs::...` and `crate::fs::...` path keeps working through
//! this re-export.
//!
//! A sync that returns an error must never be retried: the engine latches
//! [`crate::Error::RecoveryRequired`].

pub use nabedra_rt::fs::*;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::Error;

    /// `FaultFs`'s injected errors map onto the engine's error kinds: ENOSPC is
    /// `Error::NoSpace` and EIO is `Error::Io`, for every operation that can fail.
    #[test]
    fn ns_unit_fault_errors_map_to_engine_errors() {
        for op in [
            FsOp::Write,
            FsOp::SyncData,
            FsOp::SyncAll,
            FsOp::SyncDir,
            FsOp::Rename,
            FsOp::Preallocate,
            FsOp::Open,
            FsOp::Read,
        ] {
            for kind in [FaultError::Eio, FaultError::Enospc] {
                let fs = FaultFs::new();
                fs.create_dir_all_durable(Path::new("/db"));
                let file = fs.open(Path::new("/db/f"), OpenMode::CreateNew).unwrap();
                file.write_all_at(b"base", 0).unwrap();
                fs.inject(Fault::on(op).error(kind));
                let mut buf = [0u8; 1];
                let result = match op {
                    FsOp::Write => file.write_all_at(b"x", 0),
                    FsOp::SyncData => file.sync_data(),
                    FsOp::SyncAll => file.sync_all(),
                    FsOp::SyncDir => fs.sync_dir(Path::new("/db")),
                    FsOp::Rename => fs.rename(Path::new("/db/f"), Path::new("/db/g")),
                    FsOp::Preallocate => file.preallocate(1 << 20),
                    FsOp::Open => fs.open(Path::new("/db/f"), OpenMode::Read).map(drop),
                    FsOp::Read => file.read_exact_at(&mut buf, 0),
                    _ => unreachable!(),
                };
                let error = Error::from(result.unwrap_err());
                match kind {
                    FaultError::Eio => assert!(matches!(error, Error::Io(_)), "{op:?}"),
                    FaultError::Enospc => assert_eq!(error, Error::NoSpace, "{op:?}"),
                }
            }
        }
    }
}
