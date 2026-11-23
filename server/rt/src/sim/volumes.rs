//! Simulated volumes: a per-node mount table of `FaultFs` images (skeleton: signatures
//! only until P9-RT (c) lands).

use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::World;
use crate::fs::FaultFs;

/// The identity of a simulated volume, as the mount code reads it from the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeInfo {
    /// `st_dev` of every file on the volume.
    pub dev: u64,
    pub major: u32,
    pub minor: u32,
    pub filesystem_uuid: Uuid,
    /// The mount id in `/proc/self/mountinfo`.
    pub mount_id: u64,
    pub mount_path: PathBuf,
}

/// How a node's volumes survive a restart ([`World::restart_with`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cut {
    /// Process kill: every completed write survives.
    Kill,
    /// Only synced state survives.
    Crash,
    /// Power loss with 512-byte sectors, decided by `seed`.
    PowerLoss { seed: u64 },
    /// Power loss with whole 4 KiB pages destroyed, decided by `seed`.
    PowerLoss4k { seed: u64 },
}

/// How [`World::wipe`] replaces a volume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wipe {
    /// Same device numbers, a new filesystem uuid and a new inode base.
    Reformat,
    /// A new device.
    Replace,
}

impl World {
    /// Mounts a fresh volume for `node` at `path`, with a seeded device id and inode base.
    pub fn mount(&self, node: u64, path: impl Into<PathBuf>) -> VolumeInfo {
        let _ = (node, path.into());
        unimplemented!("World::mount lands in P9-RT (c)")
    }

    /// The current image of the volume mounted at `path` on `node`, for checks.
    pub fn volume(&self, node: u64, path: &Path) -> Option<FaultFs> {
        let _ = (node, path);
        unimplemented!("World::volume lands in P9-RT (c)")
    }

    pub fn volume_info(&self, node: u64, path: &Path) -> Option<VolumeInfo> {
        let _ = (node, path);
        unimplemented!("World::volume_info lands in P9-RT (c)")
    }

    /// [`FaultFs::crash_after`] on the volume mounted at `path`.
    pub fn crash_volume_after(&self, node: u64, path: &Path, ops: u64) {
        let _ = (node, path, ops);
        unimplemented!("World::crash_volume_after lands in P9-RT (c)")
    }

    /// Restarts a crashed `node`, replacing each of its volumes by its `cut` image.
    pub fn restart_with(&self, node: u64, cut: Cut) {
        let _ = (node, cut);
        unimplemented!("World::restart_with lands in P9-RT (c)")
    }

    /// Replaces the volume mounted at `path` on `node` with an empty one.
    pub fn wipe(&self, node: u64, path: &Path, how: Wipe) {
        let _ = (node, path, how);
        unimplemented!("World::wipe lands in P9-RT (c)")
    }

    /// Filesystem calls inside a node that named a path outside every mount.
    pub fn unmounted_path_errors(&self) -> u64 {
        0
    }
}
