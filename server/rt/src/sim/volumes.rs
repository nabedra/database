//! Simulated volumes: a per-node mount table of [`FaultFs`] images.
//!
//! [`World::mount`] gives a node a fresh volume at a path, with a device id, a filesystem
//! uuid and an inode base drawn from the world's stream, so a run is a function of its
//! seed. Inside a node, [`crate::fs::real`] returns a `NodeFs` and [`crate::fs::os`]
//! routes every path to the volume with the longest mount prefix. A node that has no
//! mount at all keeps the host filesystem (phase 7 behaviour); once it has one, a path
//! outside every mount fails with `Unsupported`, naming the path, and
//! [`World::unmounted_path_errors`] counts it so a check can assert there were none.
//!
//! [`World::crash`] halts every volume of the node first, so nothing written after the
//! cut reaches a disk image. [`World::restart_with`] replaces each volume by its `kill`,
//! `crash` or power-loss image; files opened before the cut keep pointing at the halted
//! old image and fail. Old node contexts and `NodeFs` handles cannot reopen paths on
//! the replacement image. [`World::wipe`] replaces one volume of a crashed node by an empty
//! one: the RV-REP #3 condition, a wiped data volume next to a surviving key volume, is one call.
//! [`World::snapshot_volume`] and [`World::restore_volume`] put an older image of the same disk
//! back under the same identity, as an in-place backup restore or a VM rollback does (SIM-5).
//! [`World::set_volume_capacity`] gives a volume a size: growth past it fails with `ENOSPC`, and
//! `statvfs` reports it (SIM-17). A power-loss restart reports what each volume kept and tore
//! ([`SimEvent::TornWrite`], SIM-6).

#[cfg(unix)]
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use uuid::Uuid;

use super::{NodeContext, SimEvent, World, current_node_context, lock, world};
#[cfg(unix)]
use crate::fs::DescriptorInode;
use crate::fs::{FaultFs, Fs, FsFile, FsLock, OpenMode, PowerLoss, PowerLossReport};

/// The identity of a simulated volume, as the mount code reads it from the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeInfo {
    /// `st_dev` of every file on the volume: `makedev(major, minor)`.
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

/// How [`World::wipe`] replaces a volume. A new inode base is gentler than any real
/// filesystem; [`World::set_mkfs_inodes`] gives the inode numbers `mkfs` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wipe {
    /// Same device numbers, a new filesystem uuid, a new inode base and a new mount id.
    Reformat,
    /// A new device (new minor number), uuid, inode base and mount id.
    Replace,
}

/// The block device major number of simulated volumes (NVMe).
pub const MAJOR: u32 = 259;

/// The capacity a simulated volume reports through `statvfs`.
pub const CAPACITY_BYTES: u64 = 64 << 30;

/// Linux `makedev`, as glibc encodes it.
pub fn makedev(major: u32, minor: u32) -> u64 {
    let (major, minor) = (u64::from(major), u64::from(minor));
    ((major & 0xffff_f000) << 32)
        | ((major & 0x0000_0fff) << 8)
        | ((minor & 0xffff_ff00) << 12)
        | (minor & 0x0000_00ff)
}

#[derive(Debug)]
struct Volume {
    info: VolumeInfo,
    fs: FaultFs,
}

/// An image of a volume as a block-level snapshot takes it: crash consistent, so what a
/// sync covered when it was taken ([`World::snapshot_volume`]).
#[derive(Clone, Debug)]
pub struct VolumeSnapshot {
    node: u64,
    mount: PathBuf,
    image: FaultFs,
    taken: std::time::Duration,
}

impl VolumeSnapshot {
    /// The virtual time it was taken at ([`World::elapsed`]).
    pub fn taken(&self) -> std::time::Duration {
        self.taken
    }
}

#[derive(Debug, Default)]
struct TableState {
    /// Per node, in mount order.
    volumes: BTreeMap<u64, Vec<Volume>>,
    next_minor: u32,
    next_mount_id: u64,
    mkfs_inodes: bool,
    /// The world seed, when volumes list in a seeded order.
    seeded_listing: Option<u64>,
    unmounted_errors: u64,
    #[cfg(unix)]
    next_descriptor: u64,
    #[cfg(unix)]
    descriptors: BTreeMap<u64, OpenDescriptor>,
}

/// The world's mount table.
#[derive(Debug, Default)]
pub(crate) struct Table {
    state: Mutex<TableState>,
}

impl Table {
    /// The volume with the longest mount prefix of `path` on `node`. `Ok(None)` when the
    /// node has no mount at all (it keeps the host filesystem).
    fn resolve(&self, node: u64, active: bool, path: &Path) -> io::Result<Option<FaultFs>> {
        let mut state = lock(&self.state);
        let Some(volumes) = state.volumes.get(&node).filter(|v| !v.is_empty()) else {
            return Ok(None);
        };
        if !active {
            return Err(io::Error::other(format!(
                "simulated node {node} incarnation is no longer active"
            )));
        }
        let best = volumes
            .iter()
            .filter(|volume| path.is_absolute() && path.starts_with(&volume.info.mount_path))
            .max_by_key(|volume| volume.info.mount_path.components().count());
        match best {
            Some(volume) => Ok(Some(volume.fs.clone())),
            None => {
                state.unmounted_errors += 1;
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "simulated node {node} has no volume mounted for {}",
                        path.display()
                    ),
                ))
            }
        }
    }

    /// Freezes every volume of `node`: later calls fail and change nothing.
    pub(crate) fn halt_node(&self, node: u64) {
        let state = lock(&self.state);
        for volume in state.volumes.get(&node).into_iter().flatten() {
            volume.fs.halt();
        }
    }

    /// Seeded listing order on every volume, present and future, or sorted again.
    pub(crate) fn set_seeded_listing(&self, seed: u64, on: bool) {
        let mut state = lock(&self.state);
        state.seeded_listing = on.then_some(seed);
        for volume in state.volumes.values().flatten() {
            volume
                .fs
                .set_list_seed(list_seed(state.seeded_listing, &volume.info));
        }
    }

    fn find<T>(&self, node: u64, path: &Path, f: impl FnOnce(&mut Volume) -> T) -> Option<T> {
        let mut state = lock(&self.state);
        state
            .volumes
            .get_mut(&node)?
            .iter_mut()
            .find(|volume| volume.info.mount_path == path)
            .map(f)
    }
}

/// A volume's listing seed: the world's, mixed with its identity, so drawing it takes
/// nothing from any stream.
fn list_seed(world_seed: Option<u64>, info: &VolumeInfo) -> Option<u64> {
    world_seed.map(|seed| seed ^ info.dev.rotate_left(17) ^ info.mount_id.rotate_left(41))
}

/// The namespace reserved for descriptor paths in the ambient simulation.
#[cfg(unix)]
const DESCRIPTORS: &str = "/.nabedra-sim-fd";

#[cfg(unix)]
#[derive(Clone, Debug)]
struct OpenDescriptor {
    context: NodeContext,
    fs: FaultFs,
    inode: DescriptorInode,
    opened_as: PathBuf,
}

/// Owns an open descriptor's pathname alias within its world and node incarnation.
#[cfg(unix)]
pub(crate) struct DescriptorPath {
    world: Weak<World>,
    id: u64,
}

#[cfg(unix)]
impl DescriptorPath {
    pub(crate) fn new(fs: FaultFs, inode: DescriptorInode, path: &Path) -> Self {
        let world = world().expect("a simulated descriptor is opened in a world");
        let context = current_node_context().expect("a simulated descriptor is opened on a node");
        let mut table = lock(&world.volumes.state);
        let id = table.next_descriptor;
        table.next_descriptor += 1;
        table.descriptors.insert(
            id,
            OpenDescriptor {
                context,
                fs,
                inode,
                opened_as: path.to_path_buf(),
            },
        );
        Self {
            world: world.me.clone(),
            id,
        }
    }

    pub(crate) fn path(&self) -> PathBuf {
        Path::new(DESCRIPTORS).join(self.id.to_string())
    }
}

#[cfg(unix)]
impl Drop for DescriptorPath {
    fn drop(&mut self) {
        if let Some(world) = self.world.upgrade() {
            lock(&world.volumes.state).descriptors.remove(&self.id);
        }
    }
}

/// Where an ambient filesystem call goes, and the path it uses on that volume.
#[cfg(unix)]
pub(crate) enum Route<'a> {
    /// No world, no node, or a node without mounts: the host filesystem.
    Host,
    Volume(FaultFs, Cow<'a, Path>),
}

/// Routes `path` for the current world and node.
#[cfg(unix)]
pub(crate) fn route(path: &Path) -> io::Result<Route<'_>> {
    if let Ok(descriptor) = path.strip_prefix(DESCRIPTORS) {
        return route_descriptor(descriptor);
    }
    let (Some(world), Some(context)) = (world(), current_node_context()) else {
        return Ok(Route::Host);
    };
    Ok(match world.resolve_node_path(context, path)? {
        Some(fs) => Route::Volume(fs, Cow::Borrowed(path)),
        None => Route::Host,
    })
}

#[cfg(unix)]
fn route_descriptor(relative: &Path) -> io::Result<Route<'_>> {
    let missing = || io::Error::from(io::ErrorKind::NotFound);
    let world = world().ok_or_else(missing)?;
    let context = current_node_context().ok_or_else(missing)?;
    let mut components = relative.components();
    let id = components
        .next()
        .and_then(|id| id.as_os_str().to_str())
        .and_then(|id| id.parse().ok())
        .ok_or_else(missing)?;
    let descriptor = lock(&world.volumes.state)
        .descriptors
        .get(&id)
        .filter(|open| open.context == context)
        .cloned()
        .ok_or_else(missing)?;
    let suffix = components.as_path();
    if suffix
        .components()
        .any(|c| c == std::path::Component::ParentDir)
    {
        return Err(io::ErrorKind::Unsupported.into());
    }
    if !suffix.as_os_str().is_empty() && matches!(descriptor.inode, DescriptorInode::File(_)) {
        return Err(io::ErrorKind::NotADirectory.into());
    }
    let opened = descriptor
        .fs
        .descriptor_path(descriptor.inode, &descriptor.opened_as)?;
    let path = opened.join(suffix);
    let mounted = world
        .resolve_node_path(context, &path)?
        .ok_or_else(missing)?;
    same_device(&descriptor.fs, &mounted)?;
    Ok(Route::Volume(descriptor.fs, Cow::Owned(path)))
}

/// Whether a simulated node with mounts is calling (`fs::os::euid` reports
/// [`FaultFs::UID`] then).
pub(crate) fn in_mounted_node() -> bool {
    let (Some(world), Some(context)) = (world(), current_node_context()) else {
        return false;
    };
    lock(&world.volumes.state)
        .volumes
        .get(&context.id)
        .is_some_and(|volumes| !volumes.is_empty())
}

/// The volume identity for `path` in the current node, for code that reads mount
/// identity from the host (`/proc/self/mountinfo`, `/dev/disk/by-uuid`, `statvfs`).
pub fn volume_for(path: &Path) -> Option<VolumeInfo> {
    let (world, context) = (world()?, current_node_context()?);
    let nodes = world.lock();
    if !context.is_current(nodes.nodes.get(&context.id)?) {
        return None;
    }
    let state = lock(&world.volumes.state);
    state
        .volumes
        .get(&context.id)?
        .iter()
        .filter(|volume| path.is_absolute() && path.starts_with(&volume.info.mount_path))
        .max_by_key(|volume| volume.info.mount_path.components().count())
        .map(|volume| volume.info.clone())
}

/// [`crate::fs::real`] inside a simulated node that has mounts.
pub(crate) fn node_fs() -> Option<Arc<dyn Fs>> {
    if !in_mounted_node() {
        return None;
    }
    let world = world()?;
    Some(Arc::new(NodeFs {
        world: world.me.clone(),
        context: current_node_context()?,
    }))
}

/// The `Fs` of one incarnation of a simulated node. Paths resolve on each call as long
/// as that incarnation is active.
struct NodeFs {
    world: Weak<World>,
    context: NodeContext,
}

impl NodeFs {
    fn fs(&self, path: &Path) -> io::Result<FaultFs> {
        let world = self
            .world
            .upgrade()
            .ok_or_else(|| io::Error::other("the simulated world has ended"))?;
        world.resolve_node_path(self.context, path)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("simulated node {} has no volumes", self.context.id),
            )
        })
    }
}

impl Fs for NodeFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        self.fs(path)?.open(path, mode)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let fs = self.fs(from)?;
        same_device(&fs, &self.fs(to)?)?;
        fs.rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.fs(path)?.remove(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.fs(path)?.exists(path)
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.fs(dir)?.create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.fs(dir)?.list(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.fs(dir)?.sync_dir(dir)
    }

    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        self.fs(path)?.lock(path)
    }
}

/// `EXDEV` for a rename across volumes.
pub(crate) fn same_device(from: &FaultFs, to: &FaultFs) -> io::Result<()> {
    if from.device() == to.device() {
        Ok(())
    } else {
        Err(io::ErrorKind::CrossesDevices.into())
    }
}

impl World {
    fn resolve_node_path(&self, context: NodeContext, path: &Path) -> io::Result<Option<FaultFs>> {
        // Keep the node state locked through mount lookup. A restart installs its images
        // while the node is down, so an active old context can only clone a pre-cut image.
        let nodes = self.lock();
        let active = nodes
            .nodes
            .get(&context.id)
            .is_some_and(|node| context.is_current(node));
        self.volumes.resolve(context.id, active, path)
    }

    fn attach(&self, node: u64, mount: &Path, fs: &FaultFs) {
        let world = self.me.clone();
        let mount = mount.to_path_buf();
        fs.observe(Arc::new(move |call| {
            if let Some(world) = world.upgrade() {
                world.emit(&SimEvent::Fs {
                    node,
                    mount: &mount,
                    call,
                });
            }
        }));
    }

    /// A seeded identity for a new volume at `path`, on `minor` (a new one when `None`).
    fn new_identity(&self, path: &Path, minor: Option<u32>) -> (VolumeInfo, u64) {
        let (uuid_high, uuid_low, base) = {
            let mut state = self.lock();
            (
                state.rng.next_u64(),
                state.rng.next_u64(),
                state.rng.next_u64(),
            )
        };
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&uuid_high.to_le_bytes());
        bytes[8..].copy_from_slice(&uuid_low.to_le_bytes());
        let mut table = lock(&self.volumes.state);
        let minor = minor.unwrap_or_else(|| {
            table.next_minor += 1;
            table.next_minor - 1
        });
        table.next_mount_id += 1;
        let info = VolumeInfo {
            dev: makedev(MAJOR, minor),
            major: MAJOR,
            minor,
            filesystem_uuid: uuid::Builder::from_random_bytes(bytes).into_uuid(),
            mount_id: 100 + table.next_mount_id,
            mount_path: path.to_path_buf(),
        };
        if table.mkfs_inodes {
            return (info, 0);
        }
        // Inode bases are multiples of 2^20 below 2^44, far apart for any test volume.
        (info, (base >> 44) << 20)
    }

    /// Whether volumes created from now on ([`World::mount`], [`World::wipe`]) number their
    /// inodes from the start, as `mkfs` does on every device. A reformat then repeats the
    /// `(dev, ino)` of the mount root, and of every file recreated in the same order, and a
    /// replaced or second volume repeats the inode numbers under its own device. Only the
    /// filesystem uuid and the mount id tell a reformatted volume from the old one.
    ///
    /// Off by default: every volume then has its own inode base, which no real filesystem
    /// gives it, so a check that trusts an inode number alone looks sound. Seeded draws are
    /// the same either way.
    pub fn set_mkfs_inodes(&self, on: bool) {
        lock(&self.volumes.state).mkfs_inodes = on;
    }

    fn new_volume(&self, node: u64, info: VolumeInfo, ino_base: u64) -> Volume {
        let fs = FaultFs::with_device(info.dev, ino_base);
        fs.create_dir_all_durable(&info.mount_path);
        fs.set_list_seed(list_seed(lock(&self.volumes.state).seeded_listing, &info));
        self.attach(node, &info.mount_path, &fs);
        Volume { info, fs }
    }

    /// Mounts a fresh, empty volume for `node` at `path`. The mount path and its ancestors
    /// exist on the volume and are durable.
    ///
    /// # Panics
    ///
    /// If the node does not exist, the path is relative, or the node already has a volume
    /// at exactly this path.
    pub fn mount(&self, node: u64, path: impl Into<PathBuf>) -> VolumeInfo {
        let path = path.into();
        assert!(
            path.is_absolute(),
            "mount a simulated volume at an absolute path"
        );
        self.with_node(node, |_| ());
        assert!(
            self.volumes.find(node, &path, |_| ()).is_none(),
            "node {node} already has a volume at {}",
            path.display()
        );
        let (info, base) = self.new_identity(&path, None);
        let volume = self.new_volume(node, info.clone(), base);
        lock(&self.volumes.state)
            .volumes
            .entry(node)
            .or_default()
            .push(volume);
        info
    }

    /// The current image of the volume mounted at `path` on `node`, for checks.
    pub fn volume(&self, node: u64, path: &Path) -> Option<FaultFs> {
        self.volumes.find(node, path, |volume| volume.fs.clone())
    }

    pub fn volume_info(&self, node: u64, path: &Path) -> Option<VolumeInfo> {
        self.volumes.find(node, path, |volume| volume.info.clone())
    }

    /// The mount paths of `node`, in mount order.
    pub fn mounts(&self, node: u64) -> Vec<PathBuf> {
        lock(&self.volumes.state)
            .volumes
            .get(&node)
            .into_iter()
            .flatten()
            .map(|volume| volume.info.mount_path.clone())
            .collect()
    }

    /// [`FaultFs::crash_after`] on the volume mounted at `path`: after `ops` more mutating
    /// operations in total, the volume freezes, as if the machine stopped there.
    ///
    /// # Panics
    ///
    /// If there is no such volume.
    pub fn crash_volume_after(&self, node: u64, path: &Path, ops: u64) {
        self.volumes
            .find(node, path, |volume| volume.fs.crash_after(ops))
            .unwrap_or_else(|| panic!("node {node} has no volume at {}", path.display()));
    }

    /// Restarts `node` with each of its volumes replaced by its `cut` image. A node that
    /// is still up is crashed first (volumes halted, services dead, tasks aborted).
    pub fn restart_with(&self, node: u64, cut: Cut) {
        if self.is_up(node) {
            self.crash(node);
        }
        let images: Vec<(PathBuf, FaultFs, Option<PowerLossReport>)> = {
            let state = lock(&self.volumes.state);
            state
                .volumes
                .get(&node)
                .into_iter()
                .flatten()
                .map(|volume| {
                    let (image, report) = match cut {
                        Cut::Kill => (volume.fs.kill(), None),
                        Cut::Crash => (volume.fs.crash(), None),
                        Cut::PowerLoss { seed } => {
                            let (image, report) =
                                volume.fs.power_loss_report(seed, PowerLoss::default());
                            (image, Some(report))
                        }
                        Cut::PowerLoss4k { seed } => {
                            let (image, report) =
                                volume.fs.power_loss_report(seed, PowerLoss::destroy_4k());
                            (image, Some(report))
                        }
                    };
                    (volume.info.mount_path.clone(), image, report)
                })
                .collect()
        };
        let mut reports = Vec::new();
        for (path, image, report) in images {
            self.attach(node, &path, &image);
            self.volumes.find(node, &path, |volume| volume.fs = image);
            reports.extend(report.map(|report| (path, report)));
        }
        self.emit(&SimEvent::Cut { node, cut });
        self.restart(node);
        for (mount, report) in &reports {
            self.emit(&SimEvent::TornWrite {
                node,
                mount,
                report,
            });
        }
    }

    /// A crash-consistent image of the volume mounted at `path` on `node` as it is now: what
    /// a sync covered, as a block-level snapshot of a live disk holds. The node may be up.
    ///
    /// # Panics
    ///
    /// If there is no such volume.
    pub fn snapshot_volume(&self, node: u64, path: &Path) -> VolumeSnapshot {
        let image = self
            .volumes
            .find(node, path, |volume| volume.fs.crash())
            .unwrap_or_else(|| panic!("node {node} has no volume at {}", path.display()));
        VolumeSnapshot {
            node,
            mount: path.to_path_buf(),
            image,
            taken: self.elapsed(),
        }
    }

    /// Puts `snapshot` back as the volume it was taken of, on its crashed node, under the
    /// same identity (device, filesystem uuid, mount id): the disk is rolled back, and
    /// nothing at the mount tells the node so. The old image is halted. One snapshot can be
    /// restored many times; each restore is a copy. Then restart the node with
    /// [`World::restart_with`], which also brings its other volumes back from the cut (a
    /// plain [`World::restart`] leaves them halted).
    ///
    /// # Panics
    ///
    /// If the node is up, or no longer has that volume.
    pub fn restore_volume(&self, snapshot: &VolumeSnapshot) {
        let (node, path) = (snapshot.node, snapshot.mount.as_path());
        assert!(
            !self.is_up(node),
            "node {node} is up: crash it before restoring {}",
            path.display()
        );
        let image = snapshot.image.kill();
        let old = self
            .volumes
            .find(node, path, |volume| {
                std::mem::replace(&mut volume.fs, image.clone())
            })
            .unwrap_or_else(|| panic!("node {node} has no volume at {}", path.display()));
        old.halt();
        // The capacity and listing order are the device's, as of now.
        image.set_capacity(old.capacity());
        if let Some(info) = self.volume_info(node, path) {
            image.set_list_seed(list_seed(lock(&self.volumes.state).seeded_listing, &info));
        }
        self.attach(node, path, &image);
        self.emit(&SimEvent::StaleRestore {
            node,
            mount: path,
            taken: snapshot.taken,
        });
    }

    /// Gives the volume mounted at `path` on `node` a capacity of `bytes`: growth past it
    /// fails with `ENOSPC` ([`FaultFs::set_capacity`]) and `statvfs` reports it. `None` is
    /// the default, unbounded, reported as [`CAPACITY_BYTES`]. Images after a restart keep it;
    /// a replaced or reformatted volume ([`World::wipe`]) keeps it too.
    ///
    /// # Panics
    ///
    /// If there is no such volume.
    pub fn set_volume_capacity(&self, node: u64, path: &Path, bytes: Option<u64>) {
        self.volumes
            .find(node, path, |volume| volume.fs.set_capacity(bytes))
            .unwrap_or_else(|| panic!("node {node} has no volume at {}", path.display()));
    }

    /// Replaces the volume mounted at `path` on a crashed `node` with an empty one. The old
    /// image is halted, so files still open on it fail.
    ///
    /// # Panics
    ///
    /// If there is no such volume, or `node` is up: paths resolve on each call, so a live
    /// process would carry on against the empty volume as if nothing had happened, which
    /// no real disk loss allows. Crash the node first.
    pub fn wipe(&self, node: u64, path: &Path, how: Wipe) {
        assert!(
            !self.is_up(node),
            "node {node} is up: crash it before wiping {}",
            path.display()
        );
        let (old_info, old_fs) = self
            .volumes
            .find(node, path, |volume| {
                (volume.info.clone(), volume.fs.clone())
            })
            .unwrap_or_else(|| panic!("node {node} has no volume at {}", path.display()));
        old_fs.halt();
        let capacity = old_fs.capacity();
        let minor = match how {
            Wipe::Reformat => Some(old_info.minor),
            Wipe::Replace => None,
        };
        let (info, mut base) = self.new_identity(path, minor);
        if base == old_fs.ino_base() && !lock(&self.volumes.state).mkfs_inodes {
            base += 1 << 20;
        }
        let volume = self.new_volume(node, info, base);
        volume.fs.set_capacity(capacity);
        self.volumes.find(node, path, |slot| *slot = volume);
        let mkfs = lock(&self.volumes.state).mkfs_inodes;
        self.emit(&SimEvent::Wipe {
            node,
            mount: path,
            how,
            mkfs,
        });
    }

    /// Filesystem calls inside a node with mounts that named a path outside every mount.
    pub fn unmounted_path_errors(&self) -> u64 {
        lock(&self.volumes.state).unmounted_errors
    }
}

#[cfg(test)]
mod tests;
