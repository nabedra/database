//! A registered pool opens only on its expected mounted block device. The
//! directory descriptor and exclusive ownership lock live as long as the store.
//! The caller obtains expectations from authenticated operator inventory.
use super::{Digest, PoolIdentity, ProtocolError};
#[cfg(not(target_os = "linux"))]
use crate::{oltp::failure, protocol::ErrorCode};
use nabedra_oltp_types::types::WorkerIdentity;
#[doc(hidden)]
pub use nabedra_oltp_types::authority::{
    MountedPoolObservation, PoolMountExpectation, PoolMountProfile,
};
use nabedra_oltp_types::authority::mount_unavailable as invalid;
#[cfg(target_os = "linux")]
use nabedra_oltp_types::authority::docker_volume_name;
#[cfg(target_os = "linux")]
use nabedra_rt::fs::os::{File, OpenOptions, self as fs};
use std::path::{Path, PathBuf};
mod closed;
#[cfg(target_os = "linux")]
pub(super) use closed::prove_closed;

/// Verified previous-incarnation exclusion for a retained mount.
pub trait PoolExclusionProof {
    fn excluded_worker(&self) -> WorkerIdentity;
}

/// Constructs the signer while the mount-verified seed is still in scope.
pub trait RestartMountKeyProof: Sized {
    fn from_mount_seed(seed: &[u8; 32]) -> Result<Self, ProtocolError>;
    fn public_key(&self) -> Digest;
}

/// Supplies the authorized retained mount and its next owner.
pub trait PoolReopenProof {
    type Key: RestartMountKeyProof;

    fn previous(&self) -> &PoolMountExpectation;
    fn previous_root_inode(&self) -> u64;
    fn next(&self) -> &PoolMountExpectation;
    fn next_mount_public_key(&self) -> Digest;
    fn drain_id(&self) -> uuid::Uuid;
}

pub struct VerifiedPoolMount {
    observation: MountedPoolObservation,
    #[cfg(target_os = "linux")]
    directory: File,
    #[cfg(target_os = "linux")]
    _lock: linux::VolumeLock,
}
/// Nonwriting physical verification, retained while the manager acquires its
/// directory/process lock and obtains the previous-incarnation exclusion.
pub struct VerifiedMountAnchor {
    observation: MountedPoolObservation,
    #[cfg(target_os = "linux")]
    directory: File,
}
impl VerifiedMountAnchor {
    pub fn observation(&self) -> &MountedPoolObservation {
        &self.observation
    }

    pub fn retained_worker_if_initialized(
        &self,
    ) -> Result<Option<WorkerIdentity>, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::retained_worker(self)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(invalid())
        }
    }

    pub fn inspect(expected: PoolMountExpectation) -> Result<Self, ProtocolError> {
        expected.validate()?;
        #[cfg(target_os = "linux")]
        {
            linux::inspect(expected)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(failure(
                ErrorCode::UnsupportedOperation,
                "registered mounts require Linux mount and filesystem verification",
            ))
        }
    }
}
impl VerifiedPoolMount {
    pub fn reopen_eks(permit: &impl PoolReopenProof) -> Result<Self, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::reopen_eks(permit)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = permit;
            Err(invalid())
        }
    }
    pub fn data_worker_key<K: RestartMountKeyProof>(&self) -> Result<K, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::data_worker_key(self)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(invalid())
        }
    }
    pub(in crate::oltp::pool) fn reobserve_for_close(
        &self,
        close_id: uuid::Uuid,
    ) -> Result<MountedPoolObservation, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::reobserve_for_close(self, close_id)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = close_id;
            Err(invalid())
        }
    }
    pub(crate) fn reobserve(&self) -> Result<MountedPoolObservation, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::reobserve(self)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(invalid())
        }
    }
    pub fn verify(
        expected: PoolMountExpectation,
        exclusion: Option<&dyn PoolExclusionProof>,
    ) -> Result<Self, ProtocolError> {
        Self::from_anchor(VerifiedMountAnchor::inspect(expected)?, exclusion)
    }
    pub fn from_anchor(
        anchor: VerifiedMountAnchor,
        exclusion: Option<&dyn PoolExclusionProof>,
    ) -> Result<Self, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            linux::verify(anchor, exclusion)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (anchor, exclusion);
            Err(failure(
                ErrorCode::UnsupportedOperation,
                "registered mounts require Linux mount and filesystem verification",
            ))
        }
    }
    pub fn observation(&self) -> &MountedPoolObservation {
        &self.observation
    }
    pub(super) fn open_path(&self, identity: &PoolIdentity) -> Result<PathBuf, ProtocolError> {
        if identity != &self.observation.expected.identity {
            return Err(invalid());
        }
        #[cfg(target_os = "linux")]
        {
            linux::revalidate(&self.directory, &self.observation)?;
            closed::reject_closed(&self.directory)?;
            linux::pool_path(self)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(invalid())
        }
    }
}
#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use serde::Deserialize;
    use std::io::{BufRead, BufReader, Read, Write};
    const MANIFEST: &str = ".nabedra-volume-v1.json";
    const LIMIT: usize = 16 << 10;
    pub(super) fn data_worker_key<K: RestartMountKeyProof>(
        mount: &VerifiedPoolMount,
    ) -> Result<K, ProtocolError> {
        reobserve(mount)?;
        read_data_worker_key(mount)
    }
    fn read_data_worker_key<K: RestartMountKeyProof>(
        mount: &VerifiedPoolMount,
    ) -> Result<K, ProtocolError> {
        let path = mount.directory.fd_path()
            .join(".nabedra-data-worker-key");
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| invalid())?;
        let metadata = file.metadata().map_err(|_| invalid())?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.len() != 32
            || metadata.uid() != fs::euid()
            || metadata.mode() & 0o077 != 0
            || mount_id(&file, &mount.observation.expected.mount_path)? != mount.observation.mount_id
        {
            return Err(invalid());
        }
        let mut seed = [0u8; 32];
        let result = file.read_exact(&mut seed).map_err(|_| invalid()).and_then(|()| {
            K::from_mount_seed(&seed)
        });
        seed.fill(0);
        result
    }
    fn bounded(path: &Path, limit: usize) -> Result<Vec<u8>, ProtocolError> {
        let mut bytes = Vec::new();
        File::open(path)
            .map_err(|_| invalid())?
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if bytes.len() > limit {
            return Err(invalid());
        }
        Ok(bytes)
    }
    pub(super) fn retained_worker(
        anchor: &VerifiedMountAnchor,
    ) -> Result<Option<WorkerIdentity>, ProtocolError> {
        revalidate(&anchor.directory, &anchor.observation)?;
        let path =
            anchor.directory.fd_path().join(MANIFEST);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(invalid()),
        };
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(invalid());
        }
        let old: PoolMountExpectation =
            serde_json::from_slice(&bounded(&path, LIMIT)?).map_err(|_| invalid())?;
        old.validate()?;
        if old.identity != anchor.observation.expected.identity {
            return Err(invalid());
        }
        Ok(Some(old.worker))
    }
    fn directory(path: &Path) -> Result<File, ProtocolError> {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| invalid())
    }
    fn mount_id(file: &File, _mount_path: &Path) -> Result<u64, ProtocolError> {
        let descriptor = file.fd_path();
        #[cfg(feature = "sim")]
        if nabedra_rt::sim::volume_for(_mount_path).is_some() {
            let resolved = fs::canonicalize(&descriptor).map_err(|_| invalid())?;
            let volume = nabedra_rt::sim::volume_for(&resolved).ok_or_else(invalid)?;
            return Ok(volume.mount_id);
        }
        let fd = descriptor.file_name().ok_or_else(invalid)?;
        let bytes = bounded(&Path::new("/proc/self/fdinfo").join(fd), 4096)?;
        std::str::from_utf8(&bytes)
            .map_err(|_| invalid())?
            .lines()
            .find_map(|line| {
                line.strip_prefix("mnt_id:")
                    .and_then(|id| id.trim().parse().ok())
            })
            .ok_or_else(invalid)
    }
    pub(super) fn pool_path(mount: &VerifiedPoolMount) -> Result<PathBuf, ProtocolError> {
        let path = mount.directory.fd_path().join("pool");
        match fs::symlink_metadata(&path) {
            Ok(_) if mount_id(&directory(&path)?, &mount.observation.expected.mount_path)?
                == mount.observation.mount_id => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(invalid()),
        }
        Ok(path)
    }
    fn observed_mount(
        file: &File,
        expected: &PoolMountExpectation,
    ) -> Result<(u64, u32, u32), ProtocolError> {
        #[cfg(feature = "sim")]
        if let Some(volume) = nabedra_rt::sim::volume_for(&expected.mount_path) {
            if volume.mount_path != expected.mount_path
                || file.metadata().map_err(|_| invalid())?.dev() != volume.dev
                || mount_id(file, &expected.mount_path)? != volume.mount_id
            {
                return Err(invalid());
            }
            return Ok((volume.mount_id, volume.major, volume.minor));
        }
        let id = mount_id(file, &expected.mount_path)?;
        let line = mount_line(id)?;
        let mut fields = line.split_ascii_whitespace();
        let _id = fields.next().ok_or_else(invalid)?;
        let _parent = fields.next().ok_or_else(invalid)?;
        let device = fields.next().ok_or_else(invalid)?;
        let root = fields.next().ok_or_else(invalid)?;
        let path = decode_path(fields.next().ok_or_else(invalid)?)?;
        let options = fields.next().ok_or_else(invalid)?;
        if path != expected.mount_path
            || (matches!(expected.profile, PoolMountProfile::Ebs) && root != "/")
            || !options.split(',').any(|v| v == "rw")
        {
            return Err(invalid());
        }
        let (major, minor) = device.split_once(':').ok_or_else(invalid)?;
        let major = major.parse::<u32>().map_err(|_| invalid())?;
        let minor = minor.parse::<u32>().map_err(|_| invalid())?;
        let metadata = file.metadata().map_err(|_| invalid())?;
        if libc::major(metadata.dev()) != major
            || libc::minor(metadata.dev()) != minor
            || metadata.dev() == fs::metadata("/").map_err(|_| invalid())?.dev()
        {
            return Err(invalid());
        }
        Ok((id, major, minor))
    }
    fn mount_line(id: u64) -> Result<String, ProtocolError> {
        let mut input = BufReader::with_capacity(
            4096,
            File::open("/proc/self/mountinfo").map_err(|_| invalid())?,
        );
        let mut line = Vec::new();
        line.try_reserve_exact(16 << 10).map_err(|_| invalid())?;
        let mut examined = 0usize;
        loop {
            let available = input.fill_buf().map_err(|_| invalid())?;
            if available.is_empty() {
                return Err(invalid());
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let length = newline.map_or(available.len(), |position| position + 1);
            examined = examined.checked_add(length).ok_or_else(invalid)?;
            if examined > 8 << 20 || line.len().checked_add(length).is_none_or(|n| n > 16 << 10) {
                return Err(invalid());
            }
            line.extend_from_slice(&available[..length]);
            input.consume(length);
            if newline.is_some() {
                let text = std::str::from_utf8(&line).map_err(|_| invalid())?;
                if text
                    .split_ascii_whitespace()
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    == Some(id)
                {
                    return String::from_utf8(line).map_err(|_| invalid());
                }
                line.clear();
            }
        }
    }
    fn device(
        expected: &PoolMountExpectation,
        major: u32,
        minor: u32,
    ) -> Result<(), ProtocolError> {
        if let PoolMountProfile::Docker { volume_marker } = expected.profile {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct DockerVolumeIdentity {
                format_version: u32,
                volume_name: String,
                volume_marker: uuid::Uuid,
            }
            // Created by the Docker provisioner after inspecting the named
            // volume. The worker never labels an arbitrary mount itself.
            let path = expected.mount_path.join(".nabedra-docker-volume.json");
            let meta = fs::symlink_metadata(&path).map_err(|_| invalid())?;
            if !meta.is_file() || meta.file_type().is_symlink() || meta.nlink() != 1 {
                return Err(invalid());
            }
            let observed: DockerVolumeIdentity =
                serde_json::from_slice(&bounded(&path, LIMIT)?).map_err(|_| invalid())?;
            if observed.format_version != 1
                || observed.volume_marker != volume_marker
                || Some(observed.volume_name.as_str())
                    != docker_volume_name(&expected.provider_volume_id)
            {
                return Err(invalid());
            }
            return Ok(());
        }
        #[cfg(feature = "sim")]
        if let Some(volume) = nabedra_rt::sim::volume_for(&expected.mount_path) {
            if uuid::Uuid::parse_str(&expected.filesystem_uuid).ok() != Some(volume.filesystem_uuid)
                || (major, minor) != (volume.major, volume.minor)
            {
                return Err(invalid());
            }
            return Ok(());
        }
        let block = fs::metadata(Path::new("/dev/disk/by-uuid").join(&expected.filesystem_uuid))
            .map_err(|_| invalid())?;
        if !block.file_type().is_block_device()
            || libc::major(block.rdev()) != major
            || libc::minor(block.rdev()) != minor
        {
            return Err(invalid());
        }
        let mut sys =
            fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")).map_err(|_| invalid())?;
        // A filesystem may occupy a partition of its EBS namespace.
        if fs::try_exists(sys.join("partition")).unwrap_or(false) {
            sys = sys.parent().ok_or_else(invalid)?.to_owned();
        }
        let controller = fs::canonicalize(sys.join("device")).map_err(|_| invalid())?;
        let serial = bounded(&controller.join("serial"), 256)?;
        let model = bounded(&controller.join("model"), 256)?;
        let serial = std::str::from_utf8(&serial).map_err(|_| invalid())?.trim();
        let model = std::str::from_utf8(&model).map_err(|_| invalid())?.trim();
        let volume = expected.provider_volume_id.replace('-', "");
        if serial != volume || model != "Amazon Elastic Block Store" {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn inspect(
        expected: PoolMountExpectation,
    ) -> Result<VerifiedMountAnchor, ProtocolError> {
        let anchor = inspect_for_close(expected)?;
        super::closed::reject_closed(&anchor.directory)?;
        Ok(anchor)
    }
    pub(super) fn inspect_for_close(
        expected: PoolMountExpectation,
    ) -> Result<VerifiedMountAnchor, ProtocolError> {
        if fs::canonicalize(&expected.mount_path).map_err(|_| invalid())? != expected.mount_path {
            return Err(invalid());
        }
        let directory = directory(&expected.mount_path)?;
        let (mount_id, device_major, device_minor) = observed_mount(&directory, &expected)?;
        device(&expected, device_major, device_minor)?;
        let stats = directory.statvfs().map_err(|_| invalid())?;
        let observation = MountedPoolObservation {
            expected,
            mount_id,
            device_major,
            device_minor,
            root_inode: directory.metadata().map_err(|_| invalid())?.ino(),
            total_bytes: stats
                .blocks
                .checked_mul(stats.block_size)
                .ok_or_else(invalid)?,
            available_bytes: stats
                .available
                .checked_mul(stats.block_size)
                .ok_or_else(invalid)?,
        };
        revalidate(&directory, &observation)?;
        Ok(VerifiedMountAnchor {
            observation,
            directory,
        })
    }
    pub(super) fn verify(
        anchor: VerifiedMountAnchor,
        exclusion: Option<&dyn PoolExclusionProof>,
    ) -> Result<VerifiedPoolMount, ProtocolError> {
        let VerifiedMountAnchor {
            observation,
            directory,
        } = anchor;
        revalidate(&directory, &observation)?;
        let expected = &observation.expected;
        let base = directory.fd_path();
        let lock = lock_mount(&base, true)?;
        super::closed::reject_closed(&directory)?;
        let path = base.join(MANIFEST);
        match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if !meta.is_file() || meta.file_type().is_symlink() || meta.nlink() != 1 {
                    return Err(invalid());
                }
                let old: PoolMountExpectation =
                    serde_json::from_slice(&bounded(&path, LIMIT)?).map_err(|_| invalid())?;
                old.refuse_legacy_format()?;
                if &old != expected {
                    if old.identity != expected.identity
                        || old.profile != expected.profile
                        || old.mount_path != expected.mount_path
                        || old.provider_volume_id != expected.provider_volume_id
                        || old.filesystem_uuid != expected.filesystem_uuid
                        || expected.generation <= old.generation
                        || exclusion.is_none_or(|proof| proof.excluded_worker() != old.worker)
                    {
                        return Err(invalid());
                    }
                    let temporary =
                        base.join(format!(".nabedra-volume-{}.tmp", nabedra_rt::ids::uuid_v4()));
                    write_manifest(&temporary, &expected)?;
                    fs::rename(&temporary, &path).map_err(|_| invalid())?;
                    directory.sync_all().map_err(|_| invalid())?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !fs::symlink_metadata(base.join("pool"))
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                {
                    return Err(invalid());
                }
                write_manifest(&path, &expected)?;
                directory.sync_all().map_err(|_| invalid())?;
            }
            Err(_) => return Err(invalid()),
        }
        revalidate(&directory, &observation)?;
        Ok(VerifiedPoolMount {
            observation,
            directory,
            _lock: VolumeLock(lock),
        })
    }
    /// The volume lock of a verified mount. A child forked by any thread shares
    /// the lock's open file description until it execs or exits, so dropping
    /// only our descriptor could keep the volume locked after the mount is
    /// gone and fail the close proof or the next open. Drop unlocks it first.
    pub(super) struct VolumeLock(File);
    impl Drop for VolumeLock {
        fn drop(&mut self) {
            self.0.unlock();
        }
    }
    impl std::ops::Deref for VolumeLock {
        type Target = File;
        fn deref(&self) -> &File {
            &self.0
        }
    }

    fn lock_mount(base: &Path, create: bool) -> Result<File, ProtocolError> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(base.join(".nabedra-volume.lock"))
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
        Ok(lock)
    }

    pub(super) fn reopen_eks<P: PoolReopenProof>(
        permit: &P,
    ) -> Result<VerifiedPoolMount, ProtocolError> {
        let previous = permit.previous();
        let next = permit.next();
        let drain_id = permit.drain_id();
        if previous.profile != PoolMountProfile::Ebs
            || next.profile != previous.profile
            || next.identity != previous.identity
            || next.mount_path != previous.mount_path
            || next.filesystem_uuid != previous.filesystem_uuid
            || next.provider_volume_id != previous.provider_volume_id
            || previous.generation.checked_add(1) != Some(next.generation)
            || previous.worker.worker_id != next.worker.worker_id
            || previous.worker.incarnation == next.worker.incarnation
            || drain_id.is_nil()
        {
            return Err(invalid());
        }
        let VerifiedMountAnchor { observation, directory } = inspect_for_close(next.clone())?;
        if observation.root_inode != permit.previous_root_inode() {
            return Err(invalid());
        }
        let base = directory.fd_path();
        let lock = lock_mount(&base, false)?;
        let mount = VerifiedPoolMount { observation, directory, _lock: VolumeLock(lock) };
        revalidate(&mount.directory, &mount.observation)?;
        if read_data_worker_key::<P::Key>(&mount)?.public_key() != permit.next_mount_public_key() {
            return Err(invalid());
        }
        let manifest = base.join(MANIFEST);
        let metadata = fs::symlink_metadata(&manifest).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.nlink() != 1 {
            return Err(invalid());
        }
        let current: PoolMountExpectation = serde_json::from_slice(&bounded(&manifest, LIMIT)?).map_err(|_| invalid())?;
        let closed = base.join(super::closed::CLOSED);
        let receipt = base.join(format!(".nabedra-reopened-{}.json", drain_id));
        let still_closed = super::closed::read_marker(&closed, previous, drain_id)?;
        let reopened = super::closed::read_marker(&receipt, previous, drain_id)?;
        if still_closed == reopened || (&current != next && (!still_closed || &current != previous)) {
            return Err(invalid());
        }
        if &current == previous {
            let temporary = base.join(format!(".nabedra-volume-{}.tmp", nabedra_rt::ids::uuid_v4()));
            let result = write_manifest(&temporary, next).and_then(|()| fs::rename(&temporary, &manifest).map_err(|_| invalid()));
            let _ = fs::remove_file(&temporary);
            result?;
        }
        // Publish the newer manifest before moving the marker that blocks all opens.
        mount.directory.sync_all().map_err(|_| invalid())?;
        if still_closed {
            fs::rename(&closed, &receipt).map_err(|_| invalid())?;
        }
        mount.directory.sync_all().map_err(|_| invalid())?;
        reobserve(&mount)?;
        Ok(mount)
    }
    fn write_manifest(path: &Path, expected: &PoolMountExpectation) -> Result<(), ProtocolError> {
        let bytes = serde_json::to_vec(expected).map_err(|_| invalid())?;
        if bytes.len() > LIMIT {
            return Err(invalid());
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| invalid())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| invalid())
    }
    pub(super) fn revalidate(
        file: &File,
        observation: &MountedPoolObservation,
    ) -> Result<(), ProtocolError> {
        let current = directory(&observation.expected.mount_path)?;
        let anchored = observed_mount(file, &observation.expected)?;
        let present = observed_mount(&current, &observation.expected)?;
        if anchored != present
            || anchored
                != (
                    observation.mount_id,
                    observation.device_major,
                    observation.device_minor,
                )
            || file.metadata().map_err(|_| invalid())?.ino() != observation.root_inode
            || current.metadata().map_err(|_| invalid())?.ino() != observation.root_inode
        {
            return Err(invalid());
        }
        device(
            &observation.expected,
            observation.device_major,
            observation.device_minor,
        )
    }
    pub(super) fn reobserve(
        mount: &VerifiedPoolMount,
    ) -> Result<MountedPoolObservation, ProtocolError> {
        reobserve_inner(mount, None)
    }
    pub(super) fn reobserve_for_close(
        mount: &VerifiedPoolMount,
        close_id: uuid::Uuid,
    ) -> Result<MountedPoolObservation, ProtocolError> {
        reobserve_inner(mount, Some(close_id))
    }
    fn reobserve_inner(
        mount: &VerifiedPoolMount,
        close_id: Option<uuid::Uuid>,
    ) -> Result<MountedPoolObservation, ProtocolError> {
        revalidate(&mount.directory, &mount.observation)?;
        match close_id {
            Some(close_id) => super::closed::check_close_retry(
                &mount.directory,
                &mount.observation.expected,
                close_id,
            )?,
            None => super::closed::reject_closed(&mount.directory)?,
        }
        let root = mount.directory.fd_path();
        let manifest_path = root.join(MANIFEST);
        let metadata = fs::symlink_metadata(&manifest_path).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.nlink() != 1 {
            return Err(invalid());
        }
        let expected: PoolMountExpectation =
            serde_json::from_slice(&bounded(&manifest_path, LIMIT)?).map_err(|_| invalid())?;
        if expected != mount.observation.expected {
            return Err(invalid());
        }
        let lock = mount._lock.metadata().map_err(|_| invalid())?;
        let path_lock =
            fs::symlink_metadata(root.join(".nabedra-volume.lock")).map_err(|_| invalid())?;
        if !path_lock.is_file()
            || path_lock.file_type().is_symlink()
            || lock.nlink() != 1
            || lock.dev() != path_lock.dev()
            || lock.ino() != path_lock.ino()
        {
            return Err(invalid());
        }
        let stats = mount.directory.statvfs().map_err(|_| invalid())?;
        let mut observation = mount.observation.clone();
        observation.total_bytes = stats
            .blocks
            .checked_mul(stats.block_size)
            .ok_or_else(invalid)?;
        observation.available_bytes = stats
            .available
            .checked_mul(stats.block_size)
            .ok_or_else(invalid)?;
        Ok(observation)
    }
    fn decode_path(value: &str) -> Result<PathBuf, ProtocolError> {
        use std::os::unix::ffi::OsStringExt;
        let mut output = Vec::with_capacity(value.len());
        let mut bytes = value.bytes();
        while let Some(byte) = bytes.next() {
            if byte == b'\\' {
                let escaped = [bytes.next(), bytes.next(), bytes.next()];
                output.push(match escaped {
                    [Some(b'0'), Some(b'4'), Some(b'0')] => b' ',
                    [Some(b'0'), Some(b'1'), Some(b'1')] => b'\t',
                    [Some(b'0'), Some(b'1'), Some(b'2')] => b'\n',
                    [Some(b'1'), Some(b'3'), Some(b'4')] => b'\\',
                    _ => return Err(invalid()),
                });
            } else {
                output.push(byte);
            }
        }
        Ok(std::ffi::OsString::from_vec(output).into())
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use nabedra_oltp_types::authority::valid_volume_id;

    pub(in crate::oltp::pool) fn worker_identity(
        worker_id: uuid::Uuid,
        incarnation: uuid::Uuid,
    ) -> WorkerIdentity {
        WorkerIdentity {
            worker_id,
            incarnation,
            certificate_sha256: super::super::Digest::from_bytes([1; 32]),
            enrollment_revision: 1,
        }
    }
    #[test]
    fn o26_mount_expectations_require_physical_volume_and_non_root_path() {
        assert!(valid_volume_id("vol-0123456789abcdef0"));
        for id in ["/tmp/data", "vol-", "vol-../bad", "vol-0123456789ABCDEF0"] {
            assert!(!valid_volume_id(id));
        }
    }
    #[test]
    fn o26_unmounted_directory_is_rejected_without_creating_pool_files() {
        let path =
            std::env::temp_dir().join(format!("nabedra-unmounted-pool-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&path).unwrap();
        let worker_id = nabedra_rt::ids::uuid_v4();
        let expected = PoolMountExpectation {
            profile: PoolMountProfile::Ebs,
            identity: PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: nabedra_rt::ids::uuid_v4(),
                worker_id,
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            worker: worker_identity(worker_id, nabedra_rt::ids::uuid_v4()),
            generation: 1,
            mount_path: path.clone(),
            provider_volume_id: "vol-0123456789abcdef0".into(),
            filesystem_uuid: nabedra_rt::ids::uuid_v4().to_string(),
        };
        assert!(VerifiedPoolMount::verify(expected.clone(), None).is_err());
        assert_eq!(
            std::fs::read_dir(&path).unwrap().count(),
            0,
            "a missing mount must not receive identity or store files"
        );
        let mut root = expected;
        root.mount_path = PathBuf::from("/");
        assert!(VerifiedPoolMount::verify(root, None).is_err());
        std::fs::remove_dir(path).unwrap();
    }
}
