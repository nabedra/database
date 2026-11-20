//! A registered pool opens only on its expected mounted block device. The
//! directory descriptor and exclusive ownership lock live as long as the store.
//! The caller obtains expectations from authenticated operator inventory.
use super::{PoolIdentity, ProtocolError};
use crate::{
    oltp::failure,
    oltp_replication::{authority::control::VerifiedExclusion, types::WorkerIdentity},
    protocol::ErrorCode,
};
use crate::oltp_replication::authority::control::EksReplacementPermit;
use serde::{Deserialize, Serialize};
#[cfg(target_os = "linux")]
use std::fs::File;
use std::path::{Path, PathBuf};
mod closed;
#[cfg(target_os = "linux")]
pub(super) use closed::prove_closed;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PoolMountProfile {
    #[default]
    Ebs,
    Docker {
        volume_marker: uuid::Uuid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolMountExpectation {
    #[serde(default)]
    pub profile: PoolMountProfile,
    pub identity: PoolIdentity,
    pub worker: WorkerIdentity,
    pub generation: u64,
    pub mount_path: PathBuf,
    pub provider_volume_id: String,
    pub filesystem_uuid: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MountedPoolObservation {
    pub expected: PoolMountExpectation,
    pub mount_id: u64,
    pub device_major: u32,
    pub device_minor: u32,
    pub root_inode: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
}
pub struct VerifiedPoolMount {
    observation: MountedPoolObservation,
    #[cfg(target_os = "linux")]
    directory: File,
    #[cfg(target_os = "linux")]
    _lock: File,
}
/// Nonwriting physical verification, retained while the manager acquires its
/// directory/process lock and obtains the previous-incarnation exclusion.
pub(crate) struct VerifiedMountAnchor {
    observation: MountedPoolObservation,
    #[cfg(target_os = "linux")]
    directory: File,
}
impl VerifiedMountAnchor {
    pub(crate) fn observation(&self) -> &MountedPoolObservation {
        &self.observation
    }

    pub(crate) fn retained_worker(&self) -> Result<WorkerIdentity, ProtocolError> {
        self.retained_worker_if_initialized()?.ok_or_else(invalid)
    }

    pub(crate) fn retained_worker_if_initialized(
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
}
impl PoolMountExpectation {
    pub(crate) fn inspect_mount(self) -> Result<VerifiedMountAnchor, ProtocolError> {
        self.validate()?;
        #[cfg(target_os = "linux")]
        {
            linux::inspect(self)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(failure(
                ErrorCode::UnsupportedOperation,
                "registered mounts require Linux mount and filesystem verification",
            ))
        }
    }
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.identity.validate()?;
        self.worker.validate().map_err(|_| invalid())?;
        if self.identity.worker_id != self.worker.worker_id
            || self.generation == 0
            || !self.mount_path.is_absolute()
            || self.mount_path == Path::new("/")
            || self.mount_path.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || self.mount_path.as_os_str().len() > 4096
            || match &self.profile {
                PoolMountProfile::Ebs => {
                    !valid_volume_id(&self.provider_volume_id)
                        || uuid::Uuid::parse_str(&self.filesystem_uuid).is_err()
                }
                PoolMountProfile::Docker { volume_marker } => {
                    volume_marker.is_nil()
                        || !self.filesystem_uuid.is_empty()
                        || docker_volume_name(&self.provider_volume_id).is_none()
                }
            }
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl VerifiedPoolMount {
    pub(crate) fn reopen_eks(permit: &EksReplacementPermit) -> Result<Self, ProtocolError> {
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
    pub(crate) fn data_worker_key(
        &self,
    ) -> Result<crate::oltp_replication::authority::control::RestartMountKey, ProtocolError> {
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
    pub(crate) fn verify(
        expected: PoolMountExpectation,
        exclusion: Option<&VerifiedExclusion>,
    ) -> Result<Self, ProtocolError> {
        Self::from_anchor(expected.inspect_mount()?, exclusion)
    }
    pub(crate) fn from_anchor(
        anchor: VerifiedMountAnchor,
        exclusion: Option<&VerifiedExclusion>,
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
    pub(crate) fn observation(&self) -> &MountedPoolObservation {
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
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "registered pool mount, device or exclusive ownership does not match inventory",
    )
}
fn valid_volume_id(value: &str) -> bool {
    value.strip_prefix("vol-").is_some_and(|id| {
        matches!(id.len(), 8 | 17)
            && id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    })
}
fn docker_volume_name(value: &str) -> Option<&str> {
    value.strip_prefix("docker-volume:").filter(|name| {
        !name.is_empty()
            && name.len() <= 255
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            && name.as_bytes()[0].is_ascii_alphanumeric()
    })
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        io::{BufRead, BufReader, Read, Write},
        os::{
            fd::AsRawFd,
            unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
        },
    };
    const MANIFEST: &str = ".nabedra-volume-v1.json";
    const LIMIT: usize = 16 << 10;
    pub(super) fn data_worker_key(
        mount: &VerifiedPoolMount,
    ) -> Result<crate::oltp_replication::authority::control::RestartMountKey, ProtocolError> {
        reobserve(mount)?;
        read_data_worker_key(mount)
    }
    fn read_data_worker_key(
        mount: &VerifiedPoolMount,
    ) -> Result<crate::oltp_replication::authority::control::RestartMountKey, ProtocolError> {
        let path = PathBuf::from(format!("/proc/self/fd/{}", mount.directory.as_raw_fd()))
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
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || mount_id(&file)? != mount.observation.mount_id
        {
            return Err(invalid());
        }
        let mut seed = [0u8; 32];
        let result = file.read_exact(&mut seed).map_err(|_| invalid()).and_then(|()| {
            crate::oltp_replication::authority::control::RestartMountKey::from_seed(&seed)
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
            PathBuf::from(format!("/proc/self/fd/{}", anchor.directory.as_raw_fd())).join(MANIFEST);
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
    fn mount_id(file: &File) -> Result<u64, ProtocolError> {
        let bytes = bounded(
            Path::new(&format!("/proc/self/fdinfo/{}", file.as_raw_fd())),
            4096,
        )?;
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
        let path = PathBuf::from(format!(
            "/proc/self/fd/{}/pool",
            mount.directory.as_raw_fd()
        ));
        match fs::symlink_metadata(&path) {
            Ok(_) if mount_id(&directory(&path)?)? == mount.observation.mount_id => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(invalid()),
        }
        Ok(path)
    }
    fn observed_mount(
        file: &File,
        expected: &PoolMountExpectation,
    ) -> Result<(u64, u32, u32), ProtocolError> {
        let id = mount_id(file)?;
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
        if sys.join("partition").exists() {
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
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::fstatvfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
            return Err(invalid());
        }
        let stats = unsafe { stats.assume_init() };
        let observation = MountedPoolObservation {
            expected,
            mount_id,
            device_major,
            device_minor,
            root_inode: directory.metadata().map_err(|_| invalid())?.ino(),
            total_bytes: stats
                .f_blocks
                .checked_mul(stats.f_frsize)
                .ok_or_else(invalid)?,
            available_bytes: stats
                .f_bavail
                .checked_mul(stats.f_frsize)
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
        exclusion: Option<&VerifiedExclusion>,
    ) -> Result<VerifiedPoolMount, ProtocolError> {
        let VerifiedMountAnchor {
            observation,
            directory,
        } = anchor;
        revalidate(&directory, &observation)?;
        let expected = &observation.expected;
        let base = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
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
                if &old != expected {
                    if old.identity != expected.identity
                        || old.profile != expected.profile
                        || old.mount_path != expected.mount_path
                        || old.provider_volume_id != expected.provider_volume_id
                        || old.filesystem_uuid != expected.filesystem_uuid
                        || expected.generation <= old.generation
                        || exclusion.is_none_or(|proof| proof.stamp().worker != old.worker)
                    {
                        return Err(invalid());
                    }
                    let temporary =
                        base.join(format!(".nabedra-volume-{}.tmp", uuid::Uuid::new_v4()));
                    write_manifest(&temporary, &expected)?;
                    fs::rename(&temporary, &path).map_err(|_| invalid())?;
                    directory.sync_all().map_err(|_| invalid())?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !base
                    .join("pool")
                    .symlink_metadata()
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
            _lock: lock,
        })
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
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
        {
            return Err(invalid());
        }
        Ok(lock)
    }

    pub(super) fn reopen_eks(permit: &EksReplacementPermit) -> Result<VerifiedPoolMount, ProtocolError> {
        let previous = &permit.previous;
        let next = &permit.next.mount;
        if previous.profile != PoolMountProfile::Ebs
            || next.profile != previous.profile
            || next.identity != previous.identity
            || next.mount_path != previous.mount_path
            || next.filesystem_uuid != previous.filesystem_uuid
            || next.provider_volume_id != previous.provider_volume_id
            || previous.generation.checked_add(1) != Some(next.generation)
            || previous.worker.worker_id != next.worker.worker_id
            || previous.worker.incarnation == next.worker.incarnation
            || permit.drain_id.is_nil()
        {
            return Err(invalid());
        }
        let VerifiedMountAnchor { observation, directory } = inspect_for_close(next.clone())?;
        if observation.root_inode != permit.previous_root_inode {
            return Err(invalid());
        }
        let base = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let lock = lock_mount(&base, false)?;
        let mount = VerifiedPoolMount { observation, directory, _lock: lock };
        revalidate(&mount.directory, &mount.observation)?;
        if read_data_worker_key(&mount)?.public_key() != permit.next.mount_public_key {
            return Err(invalid());
        }
        let manifest = base.join(MANIFEST);
        let metadata = fs::symlink_metadata(&manifest).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.nlink() != 1 {
            return Err(invalid());
        }
        let current: PoolMountExpectation = serde_json::from_slice(&bounded(&manifest, LIMIT)?).map_err(|_| invalid())?;
        let closed = base.join(super::closed::CLOSED);
        let receipt = base.join(format!(".nabedra-reopened-{}.json", permit.drain_id));
        let still_closed = super::closed::read_marker(&closed, previous, permit.drain_id)?;
        let reopened = super::closed::read_marker(&receipt, previous, permit.drain_id)?;
        if still_closed == reopened || (&current != next && (!still_closed || &current != previous)) {
            return Err(invalid());
        }
        if &current == previous {
            let temporary = base.join(format!(".nabedra-volume-{}.tmp", uuid::Uuid::new_v4()));
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
        let root = PathBuf::from(format!("/proc/self/fd/{}", mount.directory.as_raw_fd()));
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
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::fstatvfs(mount.directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
            return Err(invalid());
        }
        let stats = unsafe { stats.assume_init() };
        let mut observation = mount.observation.clone();
        observation.total_bytes = stats
            .f_blocks
            .checked_mul(stats.f_frsize)
            .ok_or_else(invalid)?;
        observation.available_bytes = stats
            .f_bavail
            .checked_mul(stats.f_frsize)
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
mod tests {
    use super::*;
    #[test]
    fn o26_mount_expectations_require_physical_volume_and_non_root_path() {
        assert!(valid_volume_id("vol-0123456789abcdef0"));
        for id in ["/tmp/data", "vol-", "vol-../bad", "vol-0123456789ABCDEF0"] {
            assert!(!valid_volume_id(id));
        }
    }
    #[test]
    fn o26_unmounted_directory_is_rejected_without_creating_pool_files() {
        use crate::oltp_replication::types::Digest;
        let path =
            std::env::temp_dir().join(format!("nabedra-unmounted-pool-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        let worker_id = uuid::Uuid::new_v4();
        let expected = PoolMountExpectation {
            profile: PoolMountProfile::Ebs,
            identity: PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: uuid::Uuid::new_v4(),
                worker_id,
                pool_id: uuid::Uuid::new_v4(),
            },
            worker: WorkerIdentity {
                worker_id,
                incarnation: uuid::Uuid::new_v4(),
                certificate_sha256: Digest::from_bytes([1; 32]),
                enrollment_revision: 1,
            },
            generation: 1,
            mount_path: path.clone(),
            provider_volume_id: "vol-0123456789abcdef0".into(),
            filesystem_uuid: uuid::Uuid::new_v4().to_string(),
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
