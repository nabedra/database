//! A completed registered-pool drain cannot reopen by racing the released
//! local lock. Reopening requires a separate authoritative generation change.
use super::*;
pub(super) const CLOSED: &str = ".nabedra-closed-pool-v1.json";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosedGeneration {
    format_version: u32,
    close_id: uuid::Uuid,
    expected: PoolMountExpectation,
}
#[cfg(target_os = "linux")]
pub(super) fn reject_closed(directory: &File) -> Result<(), ProtocolError> {
    use std::os::fd::AsRawFd;
    let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(CLOSED);
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(invalid()),
    }
}
#[cfg(target_os = "linux")]
pub(super) fn check_close_retry(
    directory: &File,
    expected: &PoolMountExpectation,
    close_id: uuid::Uuid,
) -> Result<(), ProtocolError> {
    load_marker(directory, expected, close_id).map(|_| ())
}
#[cfg(target_os = "linux")]
fn load_marker(
    directory: &File,
    expected: &PoolMountExpectation,
    close_id: uuid::Uuid,
) -> Result<bool, ProtocolError> {
    use std::os::fd::AsRawFd;
    let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(CLOSED);
    read_marker(&path, expected, close_id)
}
#[cfg(target_os = "linux")]
pub(super) fn read_marker(
    path: &Path,
    expected: &PoolMountExpectation,
    close_id: uuid::Uuid,
) -> Result<bool, ProtocolError> {
    use std::{io::Read, os::unix::fs::{MetadataExt, OpenOptionsExt}};
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Ok(metadata)
            if metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.nlink() == 1 =>
        {
            let mut bytes = Vec::new();
            std::fs::OpenOptions::new().read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open(path)
                .map_err(|_| invalid())?
                .take((16 << 10) + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid())?;
            if bytes.len() > 16 << 10 {
                return Err(invalid());
            }
            let marker: ClosedGeneration = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if marker.format_version != 1
                || marker.close_id != close_id
                || &marker.expected != expected
            {
                return Err(invalid());
            }
            Ok(true)
        }
        _ => Err(invalid()),
    }
}
impl VerifiedPoolMount {
    pub(in crate::oltp::pool) fn seal_closed_generation(
        &self,
        close_id: uuid::Uuid,
    ) -> Result<(), ProtocolError> {
        if close_id.is_nil() {
            return Err(invalid());
        }
        #[cfg(target_os = "linux")]
        {
            use std::{
                fs::{self, OpenOptions},
                os::{
                    fd::AsRawFd,
                    unix::{ffi::OsStrExt, fs::OpenOptionsExt},
                },
            };
            super::linux::revalidate(&self.directory, &self.observation)?;
            let root = PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()));
            let final_path = root.join(CLOSED);
            let marker = ClosedGeneration {
                format_version: 1,
                close_id,
                expected: self.observation.expected.clone(),
            };
            if load_marker(&self.directory, &marker.expected, close_id)? {
                return self.directory.sync_all().map_err(|_| invalid());
            }
            let temporary = root.join(format!(".nabedra-closing-{}.tmp", uuid::Uuid::new_v4()));
            let result = (|| {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(&temporary)
                    .map_err(|_| invalid())?;
                serde_json::to_writer(&file, &marker).map_err(|_| invalid())?;
                file.sync_all().map_err(|_| invalid())?;
                let from = std::ffi::CString::new(temporary.as_os_str().as_bytes())
                    .map_err(|_| invalid())?;
                let to = std::ffi::CString::new(final_path.as_os_str().as_bytes())
                    .map_err(|_| invalid())?;
                if unsafe {
                    libc::renameat2(
                        libc::AT_FDCWD,
                        from.as_ptr(),
                        libc::AT_FDCWD,
                        to.as_ptr(),
                        libc::RENAME_NOREPLACE,
                    )
                } != 0
                {
                    return Err(invalid());
                }
                self.directory.sync_all().map_err(|_| invalid())
            })();
            let _ = fs::remove_file(&temporary);
            result
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(invalid())
        }
    }
}

#[cfg(target_os = "linux")]
pub(in crate::oltp::pool) fn prove_closed(
    expected: &crate::oltp_replication::authority::control::DrainMount,
    id: uuid::Uuid,
) -> Result<MountedPoolObservation, ProtocolError> {
    use std::{
        fs::OpenOptions,
        os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
    };
    expected.expected.validate()?;
    let anchor = super::linux::inspect_for_close(expected.expected.clone())?;
    let mount = &anchor.observation;
    if crate::oltp_replication::authority::control::DrainMount::from(mount.clone()) != *expected {
        return Err(invalid());
    }
    let path = PathBuf::from(format!("/proc/self/fd/{}", anchor.directory.as_raw_fd()));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path.join(".nabedra-volume.lock"))
        .map_err(|_| invalid())?;
    let metadata = lock.metadata().map_err(|_| invalid())?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
    {
        return Err(invalid());
    }
    super::linux::revalidate(&anchor.directory, mount)?;
    if !load_marker(&anchor.directory, &expected.expected, id)? {
        return Err(invalid());
    }
    anchor.directory.sync_all().map_err(|_| invalid())?;
    Ok(anchor.observation)
}
