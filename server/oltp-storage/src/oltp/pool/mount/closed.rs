//! A completed registered-pool drain cannot reopen by racing the released
//! local lock. Reopening requires a separate authoritative generation change.
use super::*;
use serde::{Deserialize, Serialize};
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
    let path = directory.fd_path().join(CLOSED);
    match fs::symlink_metadata(path) {
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
    let path = directory.fd_path().join(CLOSED);
    read_marker(&path, expected, close_id)
}
#[cfg(target_os = "linux")]
pub(super) fn read_marker(
    path: &Path,
    expected: &PoolMountExpectation,
    close_id: uuid::Uuid,
) -> Result<bool, ProtocolError> {
    use std::io::Read;
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Ok(metadata)
            if metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.nlink() == 1 =>
        {
            let mut bytes = Vec::new();
            fs::OpenOptions::new()
                .read(true)
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
            super::linux::revalidate(&self.directory, &self.observation)?;
            let root = self.directory.fd_path();
            let final_path = root.join(CLOSED);
            let marker = ClosedGeneration {
                format_version: 1,
                close_id,
                expected: self.observation.expected.clone(),
            };
            if load_marker(&self.directory, &marker.expected, close_id)? {
                return self.directory.sync_all().map_err(|_| invalid());
            }
            let temporary = root.join(format!(
                ".nabedra-closing-{}.tmp",
                nabedra_rt::ids::uuid_v4()
            ));
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
                fs::rename_noreplace(&temporary, &final_path).map_err(|_| invalid())?;
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
    expected: &nabedra_oltp_types::authority::DrainMount,
    id: uuid::Uuid,
) -> Result<MountedPoolObservation, ProtocolError> {
    expected.expected.validate()?;
    let anchor = super::linux::inspect_for_close(expected.expected.clone())?;
    let mount = &anchor.observation;
    if nabedra_oltp_types::authority::DrainMount::from(mount.clone()) != *expected {
        return Err(invalid());
    }
    let path = anchor.directory.fd_path();
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path.join(".nabedra-volume.lock"))
        .map_err(|_| invalid())?;
    let metadata = lock.metadata().map_err(|_| invalid())?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != fs::euid()
        || metadata.mode() & 0o077 != 0
        || !lock.try_lock_exclusive().map_err(|_| invalid())?
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
