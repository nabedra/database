//! [`FaultFs`]: an in-memory [`Fs`] for crash, power-loss and I/O error injection.
//!
//! # Model
//!
//! Every file is an inode with two byte layers:
//! - the **OS** layer holds every completed write, as the page cache would;
//! - the **durable** layer holds what a completed `sync_data`/`sync_all` of that file covered.
//!
//! Directory entries (file creations, renames, removals, `create_dir_all`, lock files) take
//! effect in the OS namespace at once and reach the durable namespace only through a
//! completed [`Fs::sync_dir`] of the parent directory. Until then they sit in a list of
//! pending directory operations. A path is durable only when every ancestor directory is.
//!
//! Three ways to stop the "machine":
//! - [`FaultFs::kill`]: process kill. Every completed write survives (both layers are kept,
//!   so a later power loss can still drop what was never synced).
//! - [`FaultFs::crash`]: only durable state survives.
//! - [`FaultFs::power_loss`]: durable state survives, and for each unsynced 512-byte sector
//!   written since its file's last sync, independently, the new bytes, the old bytes or a
//!   torn prefix of the new bytes survive. An unsynced length change survives or not, and
//!   each unsynced directory operation independently survives or is lost (a rename is one
//!   atomic operation). Choices are deterministic for a given seed.
//!
//! [`FaultFs::crash_after`] freezes the state after the n-th mutating operation, so a test
//! can run a workload once, read [`FaultFs::mutating_ops`], and then enumerate every crash
//! boundary. [`FaultFs::inject`] fails chosen calls with EIO or ENOSPC.
//!
//! A failed `sync_data`/`sync_all` behaves like Linux after an fsync error ("fsyncgate"):
//! the dirty sectors are marked clean without reaching the durable layer, so a later
//! successful sync does not make them durable. This is why the engine never retries a sync.
//!
//! Every call is appended to a log ([`FaultFs::calls`]) with its operation, path, offset,
//! length and outcome, so tests can assert which syncs happened (NS15).
//!
//! The whole filesystem sits behind one `Mutex`; it is a test tool, not a fast path.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{Fs, FsFile, FsLock, OpenMode};

/// Size of the unit a power loss keeps, drops or tears.
pub const POWER_LOSS_SECTOR: u64 = 512;

/// An operation on [`Fs`] or [`FsFile`], as recorded in the call log and matched by faults.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FsOp {
    Open,
    Rename,
    Remove,
    Exists,
    CreateDirAll,
    List,
    SyncDir,
    Lock,
    Write,
    Read,
    SyncData,
    SyncAll,
    Preallocate,
    SetLen,
    Len,
    AdviseDontNeed,
}

/// One logged call. `offset` and `len` are zero where they do not apply; for `Open`,
/// `len` is the [`OpenMode`] index (0 read, 1 read-write, 2 create-new).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsCall {
    pub op: FsOp,
    pub path: PathBuf,
    /// The destination of a rename.
    pub target: Option<PathBuf>,
    pub offset: u64,
    pub len: u64,
    pub ok: bool,
}

/// The error an injected fault returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultError {
    /// `EIO`.
    Eio,
    /// `ENOSPC`.
    Enospc,
}

impl FaultError {
    fn to_io(self) -> io::Error {
        #[cfg(unix)]
        {
            io::Error::from_raw_os_error(match self {
                FaultError::Eio => libc::EIO,
                FaultError::Enospc => libc::ENOSPC,
            })
        }
        #[cfg(not(unix))]
        {
            match self {
                FaultError::Eio => io::Error::other("injected EIO"),
                FaultError::Enospc => io::ErrorKind::StorageFull.into(),
            }
        }
    }
}

/// A rule that fails the n-th matching call (counted from when it is injected).
#[derive(Clone, Debug)]
pub struct Fault {
    op: FsOp,
    nth: u64,
    error: FaultError,
    path_prefix: Option<PathBuf>,
    repeat: bool,
    seen: u64,
}

impl Fault {
    /// Fails the first call of `op` with EIO.
    pub fn on(op: FsOp) -> Fault {
        Fault {
            op,
            nth: 1,
            error: FaultError::Eio,
            path_prefix: None,
            repeat: false,
            seen: 0,
        }
    }

    /// Fails the n-th matching call instead (1 is the next one).
    pub fn nth(mut self, n: u64) -> Fault {
        assert!(n >= 1, "calls are counted from 1");
        self.nth = n;
        self
    }

    pub fn error(mut self, error: FaultError) -> Fault {
        self.error = error;
        self
    }

    /// Only calls on paths under `prefix` match (for file calls, the path it was opened as).
    pub fn path_prefix(mut self, prefix: impl Into<PathBuf>) -> Fault {
        self.path_prefix = Some(prefix.into());
        self
    }

    /// Keeps failing every matching call from the n-th on, until [`FaultFs::clear_faults`]
    /// (for example a full disk that later frees up).
    pub fn repeat(mut self) -> Fault {
        self.repeat = true;
        self
    }

    fn fires(&mut self, op: FsOp, path: &Path) -> bool {
        if op != self.op {
            return false;
        }
        if let Some(prefix) = &self.path_prefix
            && !path.starts_with(prefix)
        {
            return false;
        }
        self.seen += 1;
        self.seen == self.nth || (self.repeat && self.seen > self.nth)
    }
}

type Ino = u64;

#[derive(Clone, Debug, Default)]
struct Inode {
    os: Vec<u8>,
    durable: Vec<u8>,
    /// Sectors written since the last sync.
    dirty: BTreeSet<u64>,
    /// The smallest OS length since the last sync: bytes past it were truncated away.
    min_len: u64,
    open_handles: usize,
}

impl Inode {
    fn sync(&mut self) {
        let os_len = self.os.len();
        self.durable.truncate(self.min_len as usize);
        self.durable.resize(os_len, 0);
        for &sector in &self.dirty {
            let (start, end) = sector_range(sector, os_len);
            if start < end {
                self.durable[start..end].copy_from_slice(&self.os[start..end]);
            }
        }
        self.dirty.clear();
        // Bytes a failed sync dropped (see `fail_sync`) stay different from the OS layer.
        self.min_len = os_len as u64;
    }

    /// fsyncgate: the dirty pages are dropped from the writeback set without reaching disk.
    fn fail_sync(&mut self) {
        self.dirty.clear();
    }

    fn unsynced(&self) -> bool {
        !self.dirty.is_empty()
            || self.os.len() != self.durable.len()
            || self.min_len as usize != self.durable.len()
    }

    /// The bytes a power loss leaves, choosing per sector from `rng`.
    fn after_power_loss(&self, rng: &mut SplitMix) -> Vec<u8> {
        if !self.unsynced() {
            return self.durable.clone();
        }
        let length_changed =
            self.os.len() != self.durable.len() || self.min_len as usize != self.durable.len();
        let mut bytes = if length_changed && rng.below(2) == 0 {
            let mut b = self.durable[..self.min_len as usize].to_vec();
            b.resize(self.os.len(), 0);
            b
        } else {
            self.durable.clone()
        };
        let limit = bytes.len().min(self.os.len());
        for &sector in &self.dirty {
            let (start, end) = sector_range(sector, limit);
            if start >= end {
                continue;
            }
            match rng.below(3) {
                0 => bytes[start..end].copy_from_slice(&self.os[start..end]),
                1 => {}
                _ => {
                    let n = end - start;
                    let keep = if n > 1 {
                        1 + rng.below(n as u64 - 1) as usize
                    } else {
                        n
                    };
                    bytes[start..start + keep].copy_from_slice(&self.os[start..start + keep]);
                }
            }
        }
        bytes
    }
}

fn sector_range(sector: u64, limit: usize) -> (usize, usize) {
    let start = (sector * POWER_LOSS_SECTOR).min(limit as u64) as usize;
    let end = ((sector + 1) * POWER_LOSS_SECTOR).min(limit as u64) as usize;
    (start, end)
}

/// One effect of a directory operation on the namespace.
#[derive(Clone, Debug)]
enum Effect {
    /// `Some(ino)` links `path` to `ino`; `None` unlinks it.
    File(PathBuf, Option<Ino>),
    /// Creates or removes a directory.
    Dir(PathBuf, bool),
}

impl Effect {
    fn path(&self) -> &Path {
        match self {
            Effect::File(path, _) | Effect::Dir(path, _) => path,
        }
    }
}

/// The directory entries of one layer.
#[derive(Clone, Debug, Default)]
struct Namespace {
    files: BTreeMap<PathBuf, Ino>,
    dirs: BTreeSet<PathBuf>,
}

impl Namespace {
    fn with_roots() -> Namespace {
        let mut ns = Namespace::default();
        ns.dirs.insert(PathBuf::from("/"));
        ns.dirs.insert(PathBuf::new());
        ns
    }

    fn apply(&mut self, effect: &Effect) {
        match effect {
            Effect::File(path, Some(ino)) => {
                self.files.insert(path.clone(), *ino);
            }
            Effect::File(path, None) => {
                self.files.remove(path);
            }
            Effect::Dir(path, true) => {
                self.dirs.insert(path.clone());
            }
            Effect::Dir(path, false) => {
                self.dirs.remove(path);
            }
        }
    }

    /// Drops entries whose ancestors are not all present.
    fn prune_unreachable(&mut self) {
        let mut dirs: Vec<PathBuf> = self.dirs.iter().cloned().collect();
        // Shorter paths first, so a parent is decided before its children.
        dirs.sort_by_key(|d| d.components().count());
        let mut kept = BTreeSet::new();
        for dir in dirs {
            if is_root(&dir) || kept.contains(&parent(&dir)) {
                kept.insert(dir);
            }
        }
        self.files.retain(|path, _| kept.contains(&parent(path)));
        self.dirs = kept;
    }
}

fn is_root(path: &Path) -> bool {
    path.as_os_str().is_empty() || path == Path::new("/")
}

fn parent(path: &Path) -> PathBuf {
    path.parent().map(Path::to_path_buf).unwrap_or_default()
}

#[derive(Debug, Default)]
struct State {
    inodes: BTreeMap<Ino, Inode>,
    next_ino: Ino,
    os: Namespace,
    durable: Namespace,
    /// Directory operations not yet covered by `sync_dir`, oldest first. Each inner list
    /// is one atomic operation (a rename unlinks and links together).
    pending: Vec<Vec<Effect>>,
    locks: BTreeSet<PathBuf>,
    faults: Vec<Fault>,
    mutating_ops: u64,
    crash_after: Option<u64>,
    crashed: bool,
    calls: Vec<FsCall>,
}

impl State {
    fn fresh() -> State {
        State {
            os: Namespace::with_roots(),
            durable: Namespace::with_roots(),
            ..State::default()
        }
    }

    /// Runs the crash and fault checks for one call. On `Err` the call must have no effect.
    /// `mutating` calls advance the crash counter.
    fn admit(&mut self, op: FsOp, path: &Path, mutating: bool) -> io::Result<()> {
        if self.crashed {
            return Err(crashed());
        }
        if mutating {
            if self
                .crash_after
                .is_some_and(|limit| self.mutating_ops >= limit)
            {
                self.crashed = true;
                return Err(crashed());
            }
            self.mutating_ops += 1;
        }
        for fault in &mut self.faults {
            if fault.fires(op, path) {
                return Err(fault.error.to_io());
            }
        }
        Ok(())
    }

    fn log(
        &mut self,
        op: FsOp,
        path: &Path,
        target: Option<&Path>,
        offset: u64,
        len: u64,
        ok: bool,
    ) {
        self.calls.push(FsCall {
            op,
            path: path.to_path_buf(),
            target: target.map(Path::to_path_buf),
            offset,
            len,
            ok,
        });
    }

    fn record(&mut self, effects: Vec<Effect>) {
        for effect in &effects {
            self.os.apply(effect);
        }
        self.pending.push(effects);
    }

    fn require_dir(&self, dir: &Path) -> io::Result<()> {
        if self.os.dirs.contains(dir) {
            Ok(())
        } else {
            Err(not_found())
        }
    }

    fn new_inode(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        self.inodes.insert(ino, Inode::default());
        ino
    }

    /// Frees inodes that no namespace, pending operation or open handle can reach.
    fn collect_garbage(&mut self) {
        let mut live: BTreeSet<Ino> = self.os.files.values().copied().collect();
        live.extend(self.durable.files.values().copied());
        for op in &self.pending {
            for effect in op {
                if let Effect::File(_, Some(ino)) = effect {
                    live.insert(*ino);
                }
            }
        }
        self.inodes
            .retain(|ino, inode| inode.open_handles > 0 || live.contains(ino));
    }

    /// A new, independent filesystem from a namespace and per-inode bytes (both layers set
    /// to the given bytes, so the new state is fully durable).
    fn rebuilt(mut ns: Namespace, mut bytes_of: impl FnMut(Ino) -> Vec<u8>) -> State {
        ns.prune_unreachable();
        let mut state = State::fresh();
        state.os.dirs = ns.dirs.clone();
        state.durable.dirs = ns.dirs;
        let mut renumber = BTreeMap::new();
        for (path, old) in ns.files {
            let ino = *renumber.entry(old).or_insert_with(|| {
                let bytes = bytes_of(old);
                let ino = state.next_ino;
                state.next_ino += 1;
                state.inodes.insert(
                    ino,
                    Inode {
                        min_len: bytes.len() as u64,
                        os: bytes.clone(),
                        durable: bytes,
                        ..Inode::default()
                    },
                );
                ino
            });
            state.os.files.insert(path.clone(), ino);
            state.durable.files.insert(path, ino);
        }
        state
    }
}

fn crashed() -> io::Error {
    io::Error::other("FaultFs: the simulated machine has crashed")
}

fn not_found() -> io::Error {
    io::ErrorKind::NotFound.into()
}

/// An in-memory [`Fs`] with crash, power-loss and fault injection. Clones share state.
#[derive(Clone, Debug)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
}

impl Default for FaultFs {
    fn default() -> Self {
        FaultFs::new()
    }
}

impl FaultFs {
    /// An empty filesystem whose root (`/`) exists and is durable.
    pub fn new() -> FaultFs {
        FaultFs::from_state(State::fresh())
    }

    fn from_state(state: State) -> FaultFs {
        FaultFs {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Creates `dir` and its ancestors and makes them durable, without logging or counting.
    /// For test setup.
    pub fn create_dir_all_durable(&self, dir: &Path) {
        let mut state = self.state();
        for ancestor in dir.ancestors() {
            if !is_root(ancestor) {
                state.os.dirs.insert(ancestor.to_path_buf());
                state.durable.dirs.insert(ancestor.to_path_buf());
            }
        }
    }

    /// Process kill: a new filesystem holding every completed write. Unsynced data stays
    /// unsynced, so a later [`FaultFs::crash`] or [`FaultFs::power_loss`] can still drop it.
    /// Locks, faults, the counter and the call log start empty.
    pub fn kill(&self) -> FaultFs {
        let state = self.state();
        let mut next = State::fresh();
        next.os = state.os.clone();
        next.durable = state.durable.clone();
        next.pending = state.pending.clone();
        next.next_ino = state.next_ino;
        next.inodes = state
            .inodes
            .iter()
            .map(|(&ino, inode)| {
                (
                    ino,
                    Inode {
                        open_handles: 0,
                        ..inode.clone()
                    },
                )
            })
            .collect();
        next.collect_garbage();
        FaultFs::from_state(next)
    }

    /// Crash with no luck at all: a new filesystem holding only durable state.
    pub fn crash(&self) -> FaultFs {
        let state = self.state();
        let ns = state.durable.clone();
        FaultFs::from_state(State::rebuilt(ns, |ino| state.inodes[&ino].durable.clone()))
    }

    /// Power loss: durable state plus, independently for each unsynced sector and each
    /// unsynced directory operation, the new state, the old state or (sectors only) a torn
    /// prefix of the new bytes. Deterministic for a given `seed`.
    pub fn power_loss(&self, seed: u64) -> FaultFs {
        let state = self.state();
        let mut rng = SplitMix(seed);
        let mut ns = state.durable.clone();
        for op in &state.pending {
            if rng.below(2) == 0 {
                for effect in op {
                    ns.apply(effect);
                }
            }
        }
        // Decide every inode's bytes in inode order, so the result depends only on the seed.
        let bytes: BTreeMap<Ino, Vec<u8>> = state
            .inodes
            .iter()
            .map(|(&ino, inode)| (ino, inode.after_power_loss(&mut rng)))
            .collect();
        FaultFs::from_state(State::rebuilt(ns, |ino| bytes[&ino].clone()))
    }

    /// Adds a fault rule.
    pub fn inject(&self, fault: Fault) {
        self.state().faults.push(fault);
    }

    /// Removes every fault rule.
    pub fn clear_faults(&self) {
        self.state().faults.clear();
    }

    /// Lets `n` more mutating operations in total (counted since creation, see
    /// [`FaultFs::mutating_ops`]) complete; every operation after that fails and the state
    /// freezes, as if the machine stopped right there. Then call [`FaultFs::crash`],
    /// [`FaultFs::power_loss`] or [`FaultFs::kill`].
    pub fn crash_after(&self, n: u64) {
        let mut state = self.state();
        state.crash_after = Some(n);
        if state.mutating_ops >= n {
            state.crashed = true;
        }
    }

    /// Freezes the state now.
    pub fn halt(&self) {
        self.state().crashed = true;
    }

    pub fn is_crashed(&self) -> bool {
        self.state().crashed
    }

    /// Mutating operations admitted so far, including ones an injected fault then failed:
    /// writes, syncs (file and directory), renames, removals, `create_dir_all`,
    /// `preallocate`, `set_len`, `open` with [`OpenMode::CreateNew`], and `lock` when it
    /// creates the lock file. Reads, `len`, `exists`, `list` and other opens do not count.
    pub fn mutating_ops(&self) -> u64 {
        self.state().mutating_ops
    }

    /// Flips one bit in both layers of the file at `path` (where the offset is inside each).
    pub fn flip_bit(&self, path: &Path, byte_offset: u64, bit: u8) -> io::Result<()> {
        assert!(bit < 8, "bit must be 0..8");
        let mut state = self.state();
        let mut inos: Vec<Ino> = Vec::new();
        inos.extend(state.os.files.get(path));
        inos.extend(state.durable.files.get(path));
        inos.dedup();
        if inos.is_empty() {
            return Err(not_found());
        }
        let offset = byte_offset as usize;
        let mut flipped = false;
        for ino in inos {
            let inode = state.inodes.get_mut(&ino).expect("linked inode exists");
            if let Some(b) = inode.os.get_mut(offset) {
                *b ^= 1 << bit;
                flipped = true;
            }
            if let Some(b) = inode.durable.get_mut(offset) {
                *b ^= 1 << bit;
                flipped = true;
            }
        }
        if flipped {
            Ok(())
        } else {
            Err(io::ErrorKind::UnexpectedEof.into())
        }
    }

    /// The OS view of a file's bytes.
    pub fn contents(&self, path: &Path) -> io::Result<Vec<u8>> {
        let state = self.state();
        let ino = state.os.files.get(path).ok_or_else(not_found)?;
        Ok(state.inodes[ino].os.clone())
    }

    /// The durable bytes of the file the durable namespace links at `path`, if any.
    pub fn durable_contents(&self, path: &Path) -> Option<Vec<u8>> {
        let state = self.state();
        let ino = state.durable.files.get(path)?;
        Some(state.inodes[ino].durable.clone())
    }

    /// Every call so far, oldest first.
    pub fn calls(&self) -> Vec<FsCall> {
        self.state().calls.clone()
    }

    pub fn clear_calls(&self) {
        self.state().calls.clear();
    }
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    // A panicking test thread must not hide the state from the others.
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Fs for FaultFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        let mut state = self.state();
        let mode_index = match mode {
            OpenMode::Read => 0,
            OpenMode::ReadWrite => 1,
            OpenMode::CreateNew => 2,
        };
        let result: io::Result<Ino> = (|| {
            state.admit(FsOp::Open, path, mode == OpenMode::CreateNew)?;
            let ino = match mode {
                OpenMode::Read | OpenMode::ReadWrite => {
                    *state.os.files.get(path).ok_or_else(not_found)?
                }
                OpenMode::CreateNew => {
                    state.require_dir(&parent(path))?;
                    if state.os.files.contains_key(path) || state.os.dirs.contains(path) {
                        return Err(io::ErrorKind::AlreadyExists.into());
                    }
                    let ino = state.new_inode();
                    state.record(vec![Effect::File(path.to_path_buf(), Some(ino))]);
                    ino
                }
            };
            state
                .inodes
                .get_mut(&ino)
                .expect("linked inode exists")
                .open_handles += 1;
            Ok(ino)
        })();
        state.log(FsOp::Open, path, None, 0, mode_index, result.is_ok());
        let ino = result?;
        Ok(Arc::new(FaultFile {
            state: Arc::clone(&self.state),
            ino,
            path: path.to_path_buf(),
            writable: mode != OpenMode::Read,
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.state();
        let result = (|| {
            state.admit(FsOp::Rename, from, true)?;
            let ino = *state.os.files.get(from).ok_or_else(not_found)?;
            state.require_dir(&parent(to))?;
            if state.os.dirs.contains(to) {
                return Err(io::ErrorKind::IsADirectory.into());
            }
            if from != to {
                state.record(vec![
                    Effect::File(from.to_path_buf(), None),
                    Effect::File(to.to_path_buf(), Some(ino)),
                ]);
                state.collect_garbage();
            }
            Ok(())
        })();
        state.log(FsOp::Rename, from, Some(to), 0, 0, result.is_ok());
        result
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state();
        let result = (|| {
            state.admit(FsOp::Remove, path, true)?;
            if !state.os.files.contains_key(path) {
                return Err(not_found());
            }
            state.record(vec![Effect::File(path.to_path_buf(), None)]);
            state.collect_garbage();
            Ok(())
        })();
        state.log(FsOp::Remove, path, None, 0, 0, result.is_ok());
        result
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let mut state = self.state();
        let result = state
            .admit(FsOp::Exists, path, false)
            .map(|()| state.os.files.contains_key(path) || state.os.dirs.contains(path));
        state.log(FsOp::Exists, path, None, 0, 0, result.is_ok());
        result
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.state();
        let result = (|| {
            state.admit(FsOp::CreateDirAll, dir, true)?;
            let mut missing: Vec<&Path> = dir
                .ancestors()
                .take_while(|a| !is_root(a) && !state.os.dirs.contains(*a))
                .collect();
            if missing.iter().any(|a| state.os.files.contains_key(*a)) {
                return Err(io::ErrorKind::NotADirectory.into());
            }
            missing.reverse();
            for d in missing {
                state.record(vec![Effect::Dir(d.to_path_buf(), true)]);
            }
            Ok(())
        })();
        state.log(FsOp::CreateDirAll, dir, None, 0, 0, result.is_ok());
        result
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut state = self.state();
        let result = (|| {
            state.admit(FsOp::List, dir, false)?;
            state.require_dir(dir)?;
            let files = state.os.files.keys();
            let dirs = state.os.dirs.iter().filter(|d| !is_root(d));
            Ok(files
                .chain(dirs)
                .filter(|p| p.parent() == Some(dir))
                .cloned()
                .collect())
        })();
        state.log(FsOp::List, dir, None, 0, 0, result.is_ok());
        result
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.state();
        let result = (|| {
            state.admit(FsOp::SyncDir, dir, true)?;
            state.require_dir(dir)?;
            let State {
                pending, durable, ..
            } = &mut *state;
            for op in pending.iter_mut() {
                op.retain(|effect| {
                    let here = effect.path().parent() == Some(dir);
                    if here {
                        durable.apply(effect);
                    }
                    !here
                });
            }
            pending.retain(|op| !op.is_empty());
            state.collect_garbage();
            Ok(())
        })();
        state.log(FsOp::SyncDir, dir, None, 0, 0, result.is_ok());
        result
    }

    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        let mut state = self.state();
        let result: io::Result<()> = (|| {
            let creates = !state.os.files.contains_key(path);
            state.admit(FsOp::Lock, path, creates)?;
            if state.locks.contains(path) {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if creates {
                state.require_dir(&parent(path))?;
                let ino = state.new_inode();
                state.record(vec![Effect::File(path.to_path_buf(), Some(ino))]);
            }
            state.locks.insert(path.to_path_buf());
            Ok(())
        })();
        state.log(FsOp::Lock, path, None, 0, 0, result.is_ok());
        result?;
        Ok(FsLock::new(Box::new(FaultLockGuard {
            state: Arc::clone(&self.state),
            path: path.to_path_buf(),
        })))
    }
}

struct FaultLockGuard {
    state: Arc<Mutex<State>>,
    path: PathBuf,
}

impl Drop for FaultLockGuard {
    fn drop(&mut self) {
        lock(&self.state).locks.remove(&self.path);
    }
}

/// An open file. It keeps its inode across renames and removals, as a descriptor does.
struct FaultFile {
    state: Arc<Mutex<State>>,
    ino: Ino,
    /// The path it was opened as (used in the call log and for fault path filters).
    path: PathBuf,
    writable: bool,
}

impl FaultFile {
    /// Runs one call: crash and fault checks, then `body`, then the log entry.
    fn call<T>(
        &self,
        op: FsOp,
        offset: u64,
        len: u64,
        mutating: bool,
        body: impl FnOnce(&mut Inode) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut state = lock(&self.state);
        let result = state.admit(op, &self.path, mutating).and_then(|()| {
            if mutating && !self.writable {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            let inode = state.inodes.get_mut(&self.ino).expect("open inode exists");
            body(inode)
        });
        state.log(op, &self.path, None, offset, len, result.is_ok());
        result
    }

    fn sync(&self, op: FsOp) -> io::Result<()> {
        let mut state = lock(&self.state);
        let result = state.admit(op, &self.path, true);
        let crashed = state.crashed;
        let inode = state.inodes.get_mut(&self.ino).expect("open inode exists");
        match &result {
            Ok(()) => inode.sync(),
            // A crash leaves the page cache as it was; an I/O error drops the dirty pages.
            Err(_) if crashed => {}
            Err(_) => inode.fail_sync(),
        }
        state.log(op, &self.path, None, 0, 0, result.is_ok());
        result
    }
}

impl Drop for FaultFile {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if let Some(inode) = state.inodes.get_mut(&self.ino) {
            inode.open_handles -= 1;
        }
        state.collect_garbage();
    }
}

impl FsFile for FaultFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.call(FsOp::Write, offset, buf.len() as u64, true, |inode| {
            if buf.is_empty() {
                return Ok(());
            }
            let end = offset as usize + buf.len();
            if inode.os.len() < end {
                inode.os.resize(end, 0);
            }
            inode.os[offset as usize..end].copy_from_slice(buf);
            let first = offset / POWER_LOSS_SECTOR;
            let last = (end as u64 - 1) / POWER_LOSS_SECTOR;
            inode.dirty.extend(first..=last);
            Ok(())
        })
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let len = buf.len() as u64;
        self.call(FsOp::Read, offset, len, false, |inode| {
            let start = offset as usize;
            let end = start + buf.len();
            if end > inode.os.len() {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            buf.copy_from_slice(&inode.os[start..end]);
            Ok(())
        })
    }

    fn sync_data(&self) -> io::Result<()> {
        self.sync(FsOp::SyncData)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.sync(FsOp::SyncAll)
    }

    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.call(FsOp::Preallocate, 0, len, true, |inode| {
            if (inode.os.len() as u64) < len {
                inode.os.resize(len as usize, 0);
            }
            Ok(())
        })
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.call(FsOp::SetLen, 0, len, true, |inode| {
            inode.os.resize(len as usize, 0);
            inode.min_len = inode.min_len.min(len);
            Ok(())
        })
    }

    fn len(&self) -> io::Result<u64> {
        self.call(FsOp::Len, 0, 0, false, |inode| Ok(inode.os.len() as u64))
    }

    fn advise_dont_need(&self, offset: u64, len: u64) {
        let _ = self.call(FsOp::AdviseDontNeed, offset, len, false, |_| Ok(()));
    }
}

/// splitmix64: small, deterministic, good enough to pick crash outcomes.
#[derive(Debug)]
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    const S: usize = POWER_LOSS_SECTOR as usize;

    fn p(path: &str) -> PathBuf {
        PathBuf::from(path)
    }

    /// A filesystem with a durable `/db`.
    fn with_db() -> FaultFs {
        let fs = FaultFs::new();
        fs.create_dir_all_durable(&p("/db"));
        fs
    }

    fn read_all(fs: &FaultFs, path: &str) -> Option<Vec<u8>> {
        let file = fs.open(&p(path), OpenMode::Read).ok()?;
        let mut buf = vec![0u8; file.len().unwrap() as usize];
        file.read_exact_at(&mut buf, 0).unwrap();
        Some(buf)
    }

    /// Creates `path` with `bytes`, syncs its data and its directory.
    fn durable_file(fs: &FaultFs, path: &str, bytes: &[u8]) -> Arc<dyn FsFile> {
        let file = fs.open(&p(path), OpenMode::CreateNew).unwrap();
        file.write_all_at(bytes, 0).unwrap();
        file.sync_data().unwrap();
        fs.sync_dir(p(path).parent().unwrap()).unwrap();
        file
    }

    #[test]
    fn ns_unit_fault_fs_behaves_like_a_filesystem() {
        let fs = with_db();
        let fs_dyn: Arc<dyn Fs> = Arc::new(fs.clone());
        let file = fs_dyn.open(&p("/db/a"), OpenMode::CreateNew).unwrap();
        assert_eq!(
            fs_dyn
                .open(&p("/db/a"), OpenMode::CreateNew)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            fs_dyn
                .open(&p("/db/missing"), OpenMode::Read)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            fs_dyn
                .open(&p("/nodir/a"), OpenMode::CreateNew)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        file.preallocate(8192).unwrap();
        assert_eq!(file.len().unwrap(), 8192);
        file.write_all_at(b"hello", 4096).unwrap();
        let mut buf = [0u8; 5];
        file.read_exact_at(&mut buf, 4096).unwrap();
        assert_eq!(&buf, b"hello");
        let mut zeros = [1u8; 16];
        file.read_exact_at(&mut zeros, 0).unwrap();
        assert_eq!(zeros, [0u8; 16]);
        assert_eq!(
            file.read_exact_at(&mut buf, 8190).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        file.set_len(10).unwrap();
        assert_eq!(file.len().unwrap(), 10);
        file.set_len(20).unwrap();
        let mut tail = [9u8; 10];
        file.read_exact_at(&mut tail, 10).unwrap();
        assert_eq!(tail, [0u8; 10], "bytes past a truncation read as zeros");

        let reader = fs_dyn.open(&p("/db/a"), OpenMode::Read).unwrap();
        assert_eq!(
            reader.write_all_at(b"x", 0).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );

        fs_dyn.create_dir_all(&p("/db/sub/deeper")).unwrap();
        fs_dyn.create_dir_all(&p("/db/sub")).unwrap();
        assert!(fs_dyn.exists(&p("/db/sub/deeper")).unwrap());
        let mut listed = fs_dyn.list(&p("/db")).unwrap();
        listed.sort();
        assert_eq!(listed, vec![p("/db/a"), p("/db/sub")]);

        // A descriptor keeps its inode across rename and removal.
        fs_dyn.rename(&p("/db/a"), &p("/db/b")).unwrap();
        assert!(!fs_dyn.exists(&p("/db/a")).unwrap());
        file.write_all_at(b"renamed", 0).unwrap();
        assert_eq!(&read_all(&fs, "/db/b").unwrap()[..7], b"renamed");
        fs_dyn.remove(&p("/db/b")).unwrap();
        assert_eq!(
            fs_dyn.remove(&p("/db/b")).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        file.write_all_at(b"still open", 0).unwrap();
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"still");
        drop((file, reader));
        // The unsynced creation still pins the inode: a power loss may keep it.
        assert_eq!(fs.state().inodes.len(), 1);
        fs_dyn.sync_dir(&p("/db")).unwrap();
        assert!(
            fs.state().inodes.is_empty(),
            "an unlinked closed file is freed"
        );
    }

    #[test]
    fn ns_unit_fault_crash_keeps_only_synced_data() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/f", &[0xAA; 1000]);
        file.write_all_at(&[0xBB; 100], 900).unwrap();
        file.write_all_at(&[0xCC; 10], 5000).unwrap();
        assert_eq!(fs.contents(&p("/db/f")).unwrap().len(), 5010);

        let after = fs.crash();
        assert_eq!(read_all(&after, "/db/f").unwrap(), vec![0xAA; 1000]);

        file.sync_all().unwrap();
        let after = fs.crash();
        let bytes = read_all(&after, "/db/f").unwrap();
        assert_eq!(bytes.len(), 5010);
        assert_eq!(&bytes[900..1000], &[0xBB; 100][..]);
        assert_eq!(&bytes[5000..], &[0xCC; 10][..]);
        assert_eq!(fs.durable_contents(&p("/db/f")).unwrap(), bytes);

        // An unsynced truncation is lost, a synced one is kept.
        file.set_len(100).unwrap();
        assert_eq!(read_all(&fs.crash(), "/db/f").unwrap().len(), 5010);
        file.sync_data().unwrap();
        assert_eq!(read_all(&fs.crash(), "/db/f").unwrap(), vec![0xAA; 100]);
    }

    #[test]
    fn ns_unit_fault_kill_keeps_completed_writes() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/f", b"synced");
        file.write_all_at(b"SYNCED and more", 0).unwrap();
        let unsynced = fs.open(&p("/db/new"), OpenMode::CreateNew).unwrap();
        unsynced.write_all_at(b"new", 0).unwrap();
        let lock = fs.lock(&p("/db/LOCK")).unwrap();

        let killed = fs.kill();
        assert_eq!(read_all(&killed, "/db/f").unwrap(), b"SYNCED and more");
        assert_eq!(read_all(&killed, "/db/new").unwrap(), b"new");
        // The killed process's lock is gone.
        drop(killed.lock(&p("/db/LOCK")).unwrap());
        // What was never synced can still be lost by a later crash.
        let crashed = killed.crash();
        assert_eq!(read_all(&crashed, "/db/f").unwrap(), b"synced");
        assert!(!crashed.exists(&p("/db/new")).unwrap());
        drop(lock);
    }

    #[test]
    fn ns_unit_fault_directory_entries_need_sync_dir() {
        let fs = with_db();
        // A synced file whose creation was not synced in its directory disappears.
        let file = fs.open(&p("/db/MANIFEST-1"), OpenMode::CreateNew).unwrap();
        file.write_all_at(b"m1", 0).unwrap();
        file.sync_all().unwrap();
        assert!(!fs.crash().exists(&p("/db/MANIFEST-1")).unwrap());
        fs.sync_dir(&p("/db")).unwrap();
        assert_eq!(read_all(&fs.crash(), "/db/MANIFEST-1").unwrap(), b"m1");

        // CURRENT is replaced by temp file + rename; the rename needs the directory sync.
        durable_file(&fs, "/db/CURRENT", b"MANIFEST-0");
        let tmp = fs.open(&p("/db/CURRENT.tmp"), OpenMode::CreateNew).unwrap();
        tmp.write_all_at(b"MANIFEST-1", 0).unwrap();
        tmp.sync_all().unwrap();
        fs.rename(&p("/db/CURRENT.tmp"), &p("/db/CURRENT")).unwrap();
        assert_eq!(read_all(&fs, "/db/CURRENT").unwrap(), b"MANIFEST-1");
        assert_eq!(read_all(&fs.crash(), "/db/CURRENT").unwrap(), b"MANIFEST-0");
        fs.sync_dir(&p("/db")).unwrap();
        let after = fs.crash();
        assert_eq!(read_all(&after, "/db/CURRENT").unwrap(), b"MANIFEST-1");
        assert!(!after.exists(&p("/db/CURRENT.tmp")).unwrap());

        // An unsynced removal comes back.
        fs.remove(&p("/db/MANIFEST-1")).unwrap();
        assert!(fs.crash().exists(&p("/db/MANIFEST-1")).unwrap());
        fs.sync_dir(&p("/db")).unwrap();
        assert!(!fs.crash().exists(&p("/db/MANIFEST-1")).unwrap());

        // A directory and everything in it is durable only once every ancestor's entry is.
        fs.create_dir_all(&p("/db/kv/runs")).unwrap();
        durable_file(&fs, "/db/kv/runs/1.run", b"run");
        assert!(!fs.crash().exists(&p("/db/kv/runs/1.run")).unwrap());
        fs.sync_dir(&p("/db/kv")).unwrap();
        assert!(!fs.crash().exists(&p("/db/kv/runs/1.run")).unwrap());
        fs.sync_dir(&p("/db")).unwrap();
        let after = fs.crash();
        assert_eq!(read_all(&after, "/db/kv/runs/1.run").unwrap(), b"run");
        let mut listed = after.list(&p("/db")).unwrap();
        listed.sort();
        assert_eq!(listed, vec![p("/db/CURRENT"), p("/db/kv")]);
    }

    #[test]
    fn ns_unit_fault_power_loss_keeps_drops_or_tears_each_unsynced_sector() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/j", &[0xAA; 16 * S]);
        // Sectors 4..12 rewritten and not synced.
        file.write_all_at(&[0xBB; 8 * S], 4 * S as u64).unwrap();
        let (mut new, mut old, mut torn) = (0, 0, 0);
        for seed in 0..64 {
            let after = read_all(&fs.power_loss(seed), "/db/j").unwrap();
            assert_eq!(after.len(), 16 * S);
            assert_eq!(read_all(&fs.power_loss(seed), "/db/j").unwrap(), after);
            for sector in 0..16 {
                let bytes = &after[sector * S..(sector + 1) * S];
                if !(4..12).contains(&sector) {
                    assert!(bytes.iter().all(|&b| b == 0xAA), "a synced sector changed");
                    continue;
                }
                let prefix = bytes.iter().take_while(|&&b| b == 0xBB).count();
                assert!(
                    bytes[prefix..].iter().all(|&b| b == 0xAA),
                    "a sector is a prefix of the new bytes followed by the old ones"
                );
                match prefix {
                    0 => old += 1,
                    n if n == S => new += 1,
                    _ => torn += 1,
                }
            }
        }
        assert!(new > 0 && old > 0 && torn > 0, "{new} {old} {torn}");

        // Writes past the synced length: the extension survives or not.
        file.sync_data().unwrap();
        file.write_all_at(&[0xDD; S], 16 * S as u64).unwrap();
        let lengths: BTreeSet<usize> = (0..32)
            .map(|seed| read_all(&fs.power_loss(seed), "/db/j").unwrap().len())
            .collect();
        assert_eq!(lengths, BTreeSet::from([16 * S, 17 * S]));

        // After a sync nothing is left to lose.
        file.sync_data().unwrap();
        for seed in 0..8 {
            assert_eq!(
                read_all(&fs.power_loss(seed), "/db/j").unwrap(),
                fs.contents(&p("/db/j")).unwrap()
            );
        }
    }

    #[test]
    fn ns_unit_fault_power_loss_drops_unsynced_directory_operations_independently() {
        let fs = with_db();
        durable_file(&fs, "/db/old", b"old");
        let a = fs.open(&p("/db/a"), OpenMode::CreateNew).unwrap();
        a.sync_data().unwrap();
        fs.rename(&p("/db/old"), &p("/db/renamed")).unwrap();
        let mut outcomes = BTreeSet::new();
        for seed in 0..64 {
            let after = fs.power_loss(seed);
            let has = |path: &str| after.exists(&p(path)).unwrap();
            // A rename is atomic: exactly one of the two names.
            assert!(has("/db/old") != has("/db/renamed"));
            let name = if has("/db/old") {
                "/db/old"
            } else {
                "/db/renamed"
            };
            assert_eq!(read_all(&after, name).unwrap(), b"old");
            outcomes.insert((has("/db/a"), has("/db/renamed")));
        }
        assert_eq!(
            outcomes.len(),
            4,
            "each operation is kept or lost independently"
        );
    }

    #[test]
    fn ns_unit_fault_injected_errors_fail_the_chosen_call() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/f", b"base");
        fs.inject(Fault::on(FsOp::Write).nth(2).error(FaultError::Enospc));
        file.write_all_at(b"1", 10).unwrap();
        let error = file.write_all_at(b"2", 11).unwrap_err();
        assert_eq!(Error::from(error), Error::NoSpace);
        file.write_all_at(b"3", 12).unwrap();
        assert_eq!(&fs.contents(&p("/db/f")).unwrap()[10..], b"1\x003");

        // Each operation the brief names can fail with EIO or ENOSPC.
        let cases = [
            FsOp::Write,
            FsOp::SyncData,
            FsOp::SyncAll,
            FsOp::SyncDir,
            FsOp::Rename,
            FsOp::Preallocate,
            FsOp::Open,
        ];
        for op in cases {
            for kind in [FaultError::Eio, FaultError::Enospc] {
                let fs = with_db();
                let file = durable_file(&fs, "/db/f", b"base");
                fs.inject(Fault::on(op).error(kind));
                let result = match op {
                    FsOp::Write => file.write_all_at(b"x", 0),
                    FsOp::SyncData => file.sync_data(),
                    FsOp::SyncAll => file.sync_all(),
                    FsOp::SyncDir => fs.sync_dir(&p("/db")),
                    FsOp::Rename => fs.rename(&p("/db/f"), &p("/db/g")),
                    FsOp::Preallocate => file.preallocate(1 << 20),
                    FsOp::Open => fs.open(&p("/db/f"), OpenMode::Read).map(drop),
                    _ => unreachable!(),
                };
                let error = Error::from(result.unwrap_err());
                match kind {
                    FaultError::Eio => assert!(matches!(error, Error::Io(_)), "{op:?}"),
                    FaultError::Enospc => assert_eq!(error, Error::NoSpace, "{op:?}"),
                }
                let last = fs.calls().pop().unwrap();
                assert_eq!((last.op, last.ok), (op, false));
                // The failed call changed nothing.
                assert_eq!(fs.contents(&p("/db/f")).unwrap(), b"base", "{op:?}");
                assert!(!fs.exists(&p("/db/g")).unwrap());
            }
        }

        // Path filters and repeating faults (a full disk until space frees up).
        let fs = with_db();
        fs.create_dir_all_durable(&p("/db/journal"));
        let journal = durable_file(&fs, "/db/journal/1.jnl", b"j");
        let run = durable_file(&fs, "/db/r.run", b"r");
        fs.inject(
            Fault::on(FsOp::Write)
                .path_prefix("/db/journal")
                .error(FaultError::Enospc)
                .repeat(),
        );
        run.write_all_at(b"ok", 0).unwrap();
        for _ in 0..3 {
            assert!(journal.write_all_at(b"x", 0).is_err());
        }
        fs.clear_faults();
        journal.write_all_at(b"x", 0).unwrap();
    }

    #[test]
    fn ns_unit_fault_failed_sync_drops_dirty_pages_like_linux() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/f", b"old!");
        file.write_all_at(b"new!", 0).unwrap();
        fs.inject(Fault::on(FsOp::SyncData));
        assert!(file.sync_data().is_err());
        // The page cache still shows the write, but a retried sync does not persist it.
        assert_eq!(fs.contents(&p("/db/f")).unwrap(), b"new!");
        file.sync_data().unwrap();
        assert_eq!(read_all(&fs.crash(), "/db/f").unwrap(), b"old!");
        for seed in 0..16 {
            assert_eq!(read_all(&fs.power_loss(seed), "/db/f").unwrap(), b"old!");
        }
    }

    /// A small workload: create a file and its directory entry, then append four records,
    /// syncing after each. Returns how many records it saw synced.
    fn workload(fs: &FaultFs) -> io::Result<usize> {
        let file = fs.open(&p("/db/log"), OpenMode::CreateNew)?;
        file.preallocate(4096)?;
        fs.sync_dir(&p("/db"))?;
        let mut synced = 0;
        for i in 0..4u8 {
            file.write_all_at(&[b'a' + i; 8], u64::from(i) * 8)?;
            file.sync_data()?;
            synced += 1;
        }
        Ok(synced)
    }

    #[test]
    fn ns_unit_fault_crash_after_enumerates_every_boundary() {
        let total = {
            let fs = with_db();
            assert_eq!(workload(&fs).unwrap(), 4);
            fs.mutating_ops()
        };
        assert_eq!(total, 1 + 1 + 1 + 4 * 2);
        let mut seen = BTreeSet::new();
        for n in 0..=total {
            let fs = with_db();
            fs.crash_after(n);
            let result = workload(&fs);
            assert_eq!(result.is_ok(), n == total, "boundary {n}");
            assert_eq!(fs.is_crashed(), n < total);
            assert_eq!(fs.mutating_ops(), n);
            // The state is frozen: every later call fails and changes nothing.
            if n < total {
                assert!(fs.exists(&p("/db")).is_err());
                assert!(fs.create_dir_all(&p("/db/x")).is_err());
            }
            let after = fs.crash();
            let records = read_all(&after, "/db/log")
                .map(|bytes| bytes.chunks(8).take_while(|chunk| chunk[0] != 0).count());
            // Records only appear after their sync: ops 4,6,8,10 are the syncs.
            let expected = if n < 3 {
                None
            } else {
                Some((n as usize).saturating_sub(3) / 2)
            };
            assert_eq!(records, expected, "boundary {n}");
            seen.insert(records);
        }
        assert_eq!(seen.len(), 6, "absent, then 0 to 4 records");
    }

    #[test]
    fn ns_unit_fault_flip_bit_changes_both_layers() {
        let fs = with_db();
        let file = durable_file(&fs, "/db/f", &[0u8; 64]);
        fs.flip_bit(&p("/db/f"), 10, 3).unwrap();
        let mut b = [0u8; 1];
        file.read_exact_at(&mut b, 10).unwrap();
        assert_eq!(b[0], 0b1000);
        assert_eq!(fs.durable_contents(&p("/db/f")).unwrap()[10], 0b1000);
        assert_eq!(read_all(&fs.crash(), "/db/f").unwrap()[10], 0b1000);
        fs.flip_bit(&p("/db/f"), 10, 3).unwrap();
        assert_eq!(fs.contents(&p("/db/f")).unwrap(), vec![0u8; 64]);
        assert!(fs.flip_bit(&p("/db/f"), 64, 0).is_err());
        assert!(fs.flip_bit(&p("/db/none"), 0, 0).is_err());
    }

    #[test]
    fn ns_unit_fault_locks_are_exclusive_per_path() {
        let fs = with_db();
        let fs_dyn: Arc<dyn Fs> = Arc::new(fs.clone());
        let lock = fs_dyn.lock(&p("/db/LOCK")).unwrap();
        assert!(fs_dyn.lock(&p("/db/LOCK")).is_err());
        assert!(
            fs.clone().lock(&p("/db/LOCK")).is_err(),
            "clones share locks"
        );
        let other = fs_dyn.lock(&p("/db/OTHER")).unwrap();
        drop(lock);
        drop(fs_dyn.lock(&p("/db/LOCK")).unwrap());
        drop(other);
        assert!(fs.exists(&p("/db/LOCK")).unwrap());
    }

    #[test]
    fn ns_unit_fault_fs_is_thread_safe_and_deterministic() {
        let fs = with_db();
        let file = fs.open(&p("/db/f"), OpenMode::CreateNew).unwrap();
        fs.sync_dir(&p("/db")).unwrap();
        std::thread::scope(|scope| {
            for t in 0..8u8 {
                let file = Arc::clone(&file);
                scope.spawn(move || {
                    for i in 0..64u64 {
                        let offset = (u64::from(t) * 64 + i) * 16;
                        file.write_all_at(&[t + 1; 16], offset).unwrap();
                        if i % 8 == 0 {
                            file.sync_data().unwrap();
                        }
                    }
                });
            }
        });
        let bytes = fs.contents(&p("/db/f")).unwrap();
        for t in 0..8usize {
            assert!(
                bytes[t * 1024..(t + 1) * 1024]
                    .iter()
                    .all(|&b| b == t as u8 + 1)
            );
        }
        assert_eq!(
            read_all(&fs.power_loss(42), "/db/f"),
            read_all(&fs.power_loss(42), "/db/f")
        );
    }

    /// NS15 at the `Fs` level: the call log shows which kind of sync each file got, so
    /// engine tests can assert "journal segments get `sync_data`, runs, the manifest and
    /// directories get `sync_all` / `sync_dir`".
    ///
    /// On the real filesystem, `RealFs::sync_data` is `fdatasync` on Linux and `sync_all`
    /// and `sync_dir` are `fsync`; on Apple targets all three are `fcntl(F_FULLFSYNC)`.
    /// That mapping is confirmed outside the test suite on Linux with
    /// `strace -f -e trace=fdatasync,fsync,fcntl cargo test -p nabedra-kv <engine test>`:
    /// each journal sync shows one `fdatasync(<segment fd>)` and each run, manifest and
    /// directory sync one `fsync(fd)`, matched to paths with `-e trace=openat -y`. On macOS,
    /// `sudo dtruss -t fcntl` shows `fcntl(fd, 0x33 /* F_FULLFSYNC */)` for every sync.
    #[test]
    fn ns15_call_log_records_each_sync_kind_and_path() {
        let fs = with_db();
        fs.create_dir_all(&p("/db/journal")).unwrap();
        fs.sync_dir(&p("/db")).unwrap();
        let segment = fs
            .open(&p("/db/journal/1.jnl"), OpenMode::CreateNew)
            .unwrap();
        segment.preallocate(1 << 16).unwrap();
        fs.sync_dir(&p("/db/journal")).unwrap();
        fs.clear_calls();

        segment.write_all_at(&[1; 100], 0).unwrap();
        segment.sync_data().unwrap();
        let run = fs.open(&p("/db/1.run"), OpenMode::CreateNew).unwrap();
        run.write_all_at(&[2; 4096], 0).unwrap();
        run.sync_all().unwrap();
        fs.sync_dir(&p("/db")).unwrap();

        let calls = fs.calls();
        assert_eq!(
            calls[0],
            FsCall {
                op: FsOp::Write,
                path: p("/db/journal/1.jnl"),
                target: None,
                offset: 0,
                len: 100,
                ok: true,
            }
        );
        let syncs: Vec<(FsOp, PathBuf)> = calls
            .iter()
            .filter(|c| matches!(c.op, FsOp::SyncData | FsOp::SyncAll | FsOp::SyncDir))
            .map(|c| (c.op, c.path.clone()))
            .collect();
        assert_eq!(
            syncs,
            vec![
                (FsOp::SyncData, p("/db/journal/1.jnl")),
                (FsOp::SyncAll, p("/db/1.run")),
                (FsOp::SyncDir, p("/db")),
            ]
        );
        assert!(calls.iter().all(|c| c.ok));
    }
}
