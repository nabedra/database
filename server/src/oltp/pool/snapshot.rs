//! Group-local generation staging. Activation and applied state are one batch;
//! unrelated groups and the generation-independent Raft log are never replaced.
use super::storage::file_error;
use super::*;
use sha2::{Digest as _, Sha256};
use std::{
    io::{Seek, SeekFrom},
    path::PathBuf,
};

const MAGIC: &[u8; 16] = b"NABEDRA-GRP-0001";
const HEADER_LIMIT: usize = 64 << 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSnapshotHeader {
    pub identity: GroupIdentity,
    pub applied: Option<AppliedPosition>,
    pub command_digest: Option<Digest>,
    pub membership_digest: Option<Digest>,
    pub response_digest: Option<Digest>,
}
#[derive(Clone, Debug)]
pub struct GroupSnapshot {
    pub file: Arc<PoolSnapshotFile>,
    pub path: PathBuf,
    pub bytes: u64,
    pub header: GroupSnapshotHeader,
}
/// The actual file owner carries the pool, so cloning only `.file` for a
/// cancelled native transfer still excludes closing/detaching its volume.
pub struct PoolSnapshotFile {
    file: File,
    /// An outgoing file (SNAP1) is transient: its name goes with its last
    /// handle, and pool reopen removes a name a crash left behind.
    unlink: Option<PathBuf>,
    _pool: OltpPool,
}
impl PoolSnapshotFile {
    pub(crate) fn new(file: File, pool: OltpPool) -> Arc<Self> {
        Arc::new(Self {
            file,
            unlink: None,
            _pool: pool,
        })
    }
}
impl Drop for PoolSnapshotFile {
    fn drop(&mut self) {
        if let Some(path) = self.unlink.take() {
            let _ = fs::remove_file(path);
        }
    }
}
impl std::ops::Deref for PoolSnapshotFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}
impl std::fmt::Debug for PoolSnapshotFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSnapshotFile").finish_non_exhaustive()
    }
}

/// SNAP1. A point-in-time cut of one group, taken under the application
/// order after the applied state it names was synced: the header, and for a
/// transferable cut the pinned `nabedra-kv` view it was read from. Taking it
/// rewrites nothing, as a ScyllaDB snapshot hard-links immutable sstables
/// instead of copying them. The state machine itself is the durable snapshot:
/// its applied index never falls below a purged one. The file a follower
/// receives is written from the view only when one needs it ([`Self::file`]),
/// in bounded native jobs off the state-machine task, and the view is
/// released once the file exists.
pub struct SnapshotCut {
    pub header: GroupSnapshotHeader,
    /// Stored membership at the cut, charged to the Control reply budget.
    pub membership: Option<ChargedBytes>,
    group: GroupStore,
    view: std::sync::Mutex<Option<Snapshot>>,
    file: tokio::sync::OnceCell<GroupSnapshot>,
}
impl std::fmt::Debug for SnapshotCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCut")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}
impl SnapshotCut {
    /// The transferable file of this cut, written once and shared by clones.
    /// A cut taken without a view has none.
    pub async fn file(&self, budget: TransactionBudget) -> Result<GroupSnapshot, ProtocolError> {
        self.file
            .get_or_try_init(|| async {
                let view = self
                    .view
                    .lock()
                    .map_err(|_| storage_error_message())?
                    .clone()
                    .ok_or_else(storage_error_message)?;
                let file = self.group.materialize(&self.header, view, budget).await?;
                // The file holds the cut now: release the pinned sequence,
                // memtables and runs.
                if let Ok(mut view) = self.view.lock() {
                    *view = None;
                }
                Ok(file)
            })
            .await
            .cloned()
    }
    /// True while the cut still pins its view (test probe).
    #[cfg(test)]
    pub(crate) fn pins_view(&self) -> bool {
        self.view.lock().map(|view| view.is_some()).unwrap_or(false)
    }
}
/// Records one materialization job writes before it yields its native slot.
const MATERIALIZE_CHUNK_BYTES: usize = 1 << 20;
const MATERIALIZE_BUFFER_BYTES: usize = 64 << 10;
/// A write buffer of up to [`MATERIALIZE_BUFFER_BYTES`] from a job's scratch.
fn write_buffer(
    scratch: &mut ByteReservation,
) -> Result<super::resources::ScratchBuffer<'_>, ProtocolError> {
    let len = MATERIALIZE_BUFFER_BYTES.min(scratch.remaining());
    if len < 4096 {
        return Err(capacity());
    }
    scratch.buffer(len)
}
/// An outgoing file being written from a cut, moved through bounded jobs.
struct Materializing {
    file: Arc<PoolSnapshotFile>,
    path: PathBuf,
    iter: RawIter,
    prefix: Vec<u8>,
    checksum: Sha256,
}
enum Step {
    More(Box<Materializing>),
    Done(Box<GroupSnapshot>),
}
/// Buffered, hashed writes into a charged scratch buffer.
struct HashedOut<'a> {
    file: &'a File,
    hash: &'a mut Sha256,
    buffer: &'a mut [u8],
    used: usize,
}
impl HashedOut<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        self.hash.update(bytes);
        self.raw(bytes)
    }
    fn raw(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        if self.used + bytes.len() > self.buffer.len() {
            self.flush()?;
        }
        if bytes.len() >= self.buffer.len() {
            return (&*self.file).write_all(bytes).map_err(file_error);
        }
        self.buffer[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();
        Ok(())
    }
    fn record(&mut self, tag: u8, key: &[u8], value: &[u8]) -> Result<(), ProtocolError> {
        self.put(&[tag])?;
        self.put(
            &u32::try_from(key.len())
                .map_err(|_| capacity())?
                .to_be_bytes(),
        )?;
        self.put(&(value.len() as u64).to_be_bytes())?;
        self.put(key)?;
        self.put(value)
    }
    fn flush(&mut self) -> Result<(), ProtocolError> {
        if self.used > 0 {
            (&*self.file)
                .write_all(&self.buffer[..self.used])
                .map_err(file_error)?;
            self.used = 0;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct IncomingSnapshot {
    pub file: Arc<PoolSnapshotFile>,
    pub path: PathBuf,
    _cleanup: Arc<IncomingCleanup>,
}
impl std::fmt::Debug for IncomingSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingSnapshot")
            .field("path", &self.path)
            .finish()
    }
}
struct IncomingCleanup {
    path: PathBuf,
    _group: GroupStore,
}
impl Drop for IncomingCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl GroupStore {
    /// The transport receives only a pool-scoped create_new file. Final handle
    /// drop unlinks the name; retained FileArc readers keep their inode pinned.
    pub async fn new_incoming_snapshot(
        &self,
        node_id: u64,
        transfer_id: Uuid,
    ) -> Result<IncomingSnapshot, ProtocolError> {
        if node_id == 0 || transfer_id.is_nil() {
            return Err(failure(
                ErrorCode::ValidationError,
                "invalid incoming snapshot identity",
            ));
        }
        let group = self.clone();
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                group.pool().healthy()?;
                let mut directory = group.pool().inner.path.clone();
                for component in [
                    "snapshots".to_owned(),
                    group.identity().group_id.to_string(),
                    "incoming".to_owned(),
                ] {
                    directory.push(component);
                    match fs::create_dir(&directory) {
                        Ok(()) => {
                            sync_directory(directory.parent().ok_or_else(storage_error_message)?)?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(_) => return Err(storage_error_message()),
                    }
                    let metadata =
                        fs::symlink_metadata(&directory).map_err(|_| storage_error_message())?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(storage_error_message());
                    }
                }
                let path = directory.join(format!("recv-{node_id}-{transfer_id}.bin"));
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(|_| storage_error_message())?;
                sync_directory(&directory)?;
                Ok(IncomingSnapshot {
                    file: PoolSnapshotFile::new(file, group.pool().clone()),
                    path: path.clone(),
                    _cleanup: Arc::new(IncomingCleanup {
                        path,
                        _group: group,
                    }),
                })
            })
            .await
    }

    /// Read membership from the immutable export cut, never from current DB
    /// state. The replica must transfer decoded metadata to its resident owner
    /// before retaining it in cached SnapshotMeta or transfer state.
    pub async fn snapshot_membership(
        &self,
        snapshot: GroupSnapshot,
        budget: TransactionBudget,
    ) -> Result<Option<ChargedBytes>, ProtocolError> {
        let mut output = self
            .resources()
            .reply_budget(ExecutionClass::Control, budget.output_bytes)
            .await?;
        let unused = self
            .resources()
            .reply_budget(ExecutionClass::Control, 0)
            .await?;
        let prepared = self.resources().prepare(ExecutionClass::Control).await?;
        let (mut scratch, _batch, _unused) = prepared
            .buffers(
                TransactionBudget {
                    output_bytes: 0,
                    batch_bytes: 0,
                    ..budget
                },
                unused,
            )
            .await?;
        let identity = self.identity().clone();
        let group = self.clone();
        prepared
            .execute(0, 0, move || {
                group.pool().healthy()?;
                if snapshot.header.identity != identity {
                    return Err(storage_error_message());
                }
                let metadata = snapshot
                    .file
                    .metadata()
                    .map_err(|_| storage_error_message())?;
                if !metadata.is_file() || metadata.len() != snapshot.bytes {
                    return Err(storage_error_message());
                }
                let mut offset = 0u64;
                let mut first = [0u8; 20];
                read_exact_at(&snapshot.file, snapshot.bytes, &mut offset, &mut first)?;
                if &first[..16] != MAGIC {
                    return Err(storage_error_message());
                }
                let length = u32::from_be_bytes(
                    first[16..]
                        .try_into()
                        .expect("fixed snapshot header length"),
                ) as usize;
                if length > HEADER_LIMIT {
                    return Err(capacity());
                }
                let header = {
                    let mut buffer = scratch.buffer(length)?;
                    read_exact_at(
                        &snapshot.file,
                        snapshot.bytes,
                        &mut offset,
                        buffer.as_mut_slice(),
                    )?;
                    serde_json::from_slice::<GroupSnapshotHeader>(buffer.as_slice())
                        .map_err(|_| storage_error_message())?
                };
                if header != snapshot.header {
                    return Err(storage_error_message());
                }
                let mut applied = None;
                let mut member = None;
                let mut prior = 0u8;
                loop {
                    let mut tag = [0u8; 1];
                    read_exact_at(&snapshot.file, snapshot.bytes, &mut offset, &mut tag)?;
                    if matches!(tag[0], 3 | 4 | 255) {
                        break;
                    }
                    if !matches!(tag[0], 1 | 2) || tag[0] <= prior {
                        return Err(storage_error_message());
                    }
                    prior = tag[0];
                    let mut lengths = [0u8; 12];
                    read_exact_at(&snapshot.file, snapshot.bytes, &mut offset, &mut lengths)?;
                    if lengths[..4] != [0; 4] {
                        return Err(storage_error_message());
                    }
                    let length = usize::try_from(u64::from_be_bytes(
                        lengths[4..]
                            .try_into()
                            .expect("fixed snapshot record length"),
                    ))
                    .map_err(|_| capacity())?;
                    if length > HEADER_LIMIT {
                        return Err(capacity());
                    }
                    let mut bytes = scratch.buffer(length)?;
                    read_exact_at(
                        &snapshot.file,
                        snapshot.bytes,
                        &mut offset,
                        bytes.as_mut_slice(),
                    )?;
                    if tag[0] == 1 {
                        applied = Some(decode_applied(bytes.as_slice())?);
                    } else {
                        if Some(hash(bytes.as_slice())) != header.membership_digest {
                            return Err(storage_error_message());
                        }
                        member = Some(output.copy(bytes.as_slice())?);
                    }
                }
                if applied.as_ref().map(|value| value.0) != header.applied
                    || applied.as_ref().map(|value| value.1) != header.command_digest
                    || member.is_some() != header.membership_digest.is_some()
                {
                    return Err(storage_error_message());
                }
                group.pool().healthy()?;
                Ok(member)
            })
            .await
    }
    pub async fn current_snapshot(
        &self,
        budget: TransactionBudget,
    ) -> Result<Option<GroupSnapshot>, ProtocolError> {
        let owner = self.inner.order.clone().lock_owned().await;
        let group = self.clone();
        let output = self
            .resources()
            .reply_budget(ExecutionClass::Control, 0)
            .await?;
        let prepared = self.resources().prepare(self.execution_class()).await?;
        let (mut scratch, _batch, _output) = prepared
            .buffers(
                TransactionBudget {
                    output_bytes: 0,
                    ..budget
                },
                output,
            )
            .await?;
        prepared
            .execute(0, 0, move || {
                let _owner = owner;
                group.pool().healthy()?;
                let Some(current) = group
                    .pool()
                    .snapshot()
                    .get(&group_key(group.identity().group_id, b'f', &[]))
                    .map_err(|_| recovery_unknown())?
                else {
                    return Ok(None);
                };
                let name = std::str::from_utf8(&current).map_err(|_| recovery_unknown())?;
                if !valid_snapshot_name(name) {
                    return Err(recovery_unknown());
                }
                let path = group
                    .pool()
                    .inner
                    .path
                    .join("snapshots")
                    .join(group.identity().group_id.to_string())
                    .join(name);
                let mut file = File::open(&path).map_err(|_| recovery_unknown())?;
                let bytes = file.metadata().map_err(|_| recovery_unknown())?.len();
                let mut prefix = [0u8; 20];
                file.read_exact(&mut prefix)
                    .map_err(|_| recovery_unknown())?;
                if &prefix[..16] != MAGIC {
                    return Err(recovery_unknown());
                }
                let length =
                    u32::from_be_bytes(prefix[16..].try_into().expect("fixed header length"))
                        as usize;
                if length > HEADER_LIMIT {
                    return Err(recovery_unknown());
                }
                let mut buffer = scratch.buffer(length)?;
                file.read_exact(buffer.as_mut_slice())
                    .map_err(|_| recovery_unknown())?;
                let header: GroupSnapshotHeader =
                    serde_json::from_slice(buffer.as_slice()).map_err(|_| recovery_unknown())?;
                if header.identity != *group.identity() {
                    return Err(recovery_unknown());
                }
                group.pool().healthy()?;
                Ok(Some(GroupSnapshot {
                    file: PoolSnapshotFile::new(file, group.pool().clone()),
                    path,
                    bytes,
                    header,
                }))
            })
            .await
    }
    /// SNAP1: a cut and its transferable file at once (tests and tools; Raft
    /// hosts take the cut and write the file separately).
    pub async fn export_snapshot(
        &self,
        budget: TransactionBudget,
    ) -> Result<GroupSnapshot, ProtocolError> {
        self.cut_snapshot(budget, true).await?.file(budget).await
    }
    /// SNAP1: the cut a Raft snapshot names. Under the application order every
    /// applied write of this group has completed; any unsynced one is synced
    /// before the cut can name its index (plan section 2: purge and snapshots
    /// only up to an applied index a later synced write made durable), without
    /// holding a compute permit while storage syncs. The header and, with
    /// `pin`, the view come from one `nabedra-kv` snapshot, so the rows a later
    /// [`SnapshotCut::file`] writes match the header's applied position
    /// exactly. Applies pause only for this: a sync and three point reads.
    pub async fn cut_snapshot(
        &self,
        budget: TransactionBudget,
        pin: bool,
    ) -> Result<SnapshotCut, ProtocolError> {
        let owner = self.inner.order.clone().lock_owned().await;
        self.sync_deferred_applies().await?;
        let mut output = self
            .resources()
            .reply_budget(ExecutionClass::Control, budget.output_bytes)
            .await?;
        let unused = self
            .resources()
            .reply_budget(ExecutionClass::Control, 0)
            .await?;
        let prepared = self.resources().prepare(self.execution_class()).await?;
        let (mut scratch, _batch, _unused) = prepared
            .buffers(
                TransactionBudget {
                    output_bytes: 0,
                    batch_bytes: 0,
                    ..budget
                },
                unused,
            )
            .await?;
        let group = self.clone();
        prepared
            .execute(0, 0, move || {
                let _owner = owner;
                group.pool().healthy()?;
                let view = group.pool().snapshot();
                let id = group.identity().group_id;
                let applied = group.read_applied(&view, &mut scratch)?;
                let membership = view.get(&group_key(id, b'm', &[])).map_err(storage_error)?;
                let response = view.get(&group_key(id, b'r', &[])).map_err(storage_error)?;
                let header = GroupSnapshotHeader {
                    identity: group.identity().clone(),
                    applied: applied.as_ref().map(|v| v.position),
                    command_digest: applied.as_ref().map(|v| v.command_digest),
                    membership_digest: membership.as_ref().map(|v| hash(v)),
                    response_digest: response.as_ref().map(|v| hash(v)),
                };
                drop(applied);
                let membership = membership
                    .map(|value| {
                        if value.len() > HEADER_LIMIT {
                            return Err(capacity());
                        }
                        output.copy(&value)
                    })
                    .transpose()?;
                Ok(SnapshotCut {
                    header,
                    membership,
                    group: group.clone(),
                    view: std::sync::Mutex::new(pin.then_some(view)),
                    file: tokio::sync::OnceCell::new(),
                })
            })
            .await
    }
    /// Writes the file of a cut into `snapshots/<group>/outgoing/` in native
    /// jobs of at most [`MATERIALIZE_CHUNK_BYTES`] of records, so a large
    /// group never holds an executor slot (often the worker's only query
    /// thread) for more than one chunk, and applies of every group interleave
    /// with it. No fsync: the file is transient sender state; the receiver
    /// syncs its copy before it installs, and the name goes with the last
    /// handle (pool reopen removes one a crash left behind). It holds no lock
    /// of the group, so applies, installs and membership changes proceed.
    async fn materialize(
        &self,
        header: &GroupSnapshotHeader,
        view: Snapshot,
        budget: TransactionBudget,
    ) -> Result<GroupSnapshot, ProtocolError> {
        let class = self.execution_class();
        let group = self.clone();
        let header = header.clone();
        let encode = header.clone();
        let mut state = self
            .materialize_job(class, budget, move |scratch| {
                group.pool().healthy()?;
                let encoded = scratch.encode_value(&encode)?;
                if encoded.len() > HEADER_LIMIT {
                    return Err(capacity());
                }
                let path = new_outgoing_path(&group)?;
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(file_error)?;
                let file = Arc::new(PoolSnapshotFile {
                    file,
                    unlink: Some(path.clone()),
                    _pool: group.pool().clone(),
                });
                let prefix = group.prefix(&view, false)?;
                let mut iter = RawIter::bounded(&view, &prefix, &prefix_end(&prefix), false);
                iter.seek(&prefix);
                let mut checksum = Sha256::new();
                {
                    let mut buffer = write_buffer(scratch)?;
                    let mut out = HashedOut {
                        file: &file,
                        hash: &mut checksum,
                        buffer: buffer.as_mut_slice(),
                        used: 0,
                    };
                    out.put(MAGIC)?;
                    out.put(&(encoded.len() as u32).to_be_bytes())?;
                    out.put(encoded.as_slice())?;
                    for (tag, key) in [(1, b'p'), (2, b'm'), (3, b'r')] {
                        if let Some(value) = view
                            .get(&group_key(group.identity().group_id, key, &[]))
                            .map_err(storage_error)?
                        {
                            out.record(tag, &[], &value)?;
                        }
                    }
                    out.flush()?;
                }
                Ok(Materializing {
                    file,
                    path,
                    iter,
                    prefix,
                    checksum,
                })
            })
            .await?;
        loop {
            let group = self.clone();
            let header = header.clone();
            let step = self
                .materialize_job(class, budget, move |scratch| {
                    group.pool().healthy()?;
                    let mut buffer = write_buffer(scratch)?;
                    let Materializing {
                        file,
                        path,
                        mut iter,
                        prefix,
                        mut checksum,
                    } = state;
                    let mut out = HashedOut {
                        file: &file,
                        hash: &mut checksum,
                        buffer: buffer.as_mut_slice(),
                        used: 0,
                    };
                    let mut written = 0usize;
                    let mut done = true;
                    while iter.valid() {
                        if written >= MATERIALIZE_CHUNK_BYTES {
                            done = false;
                            break;
                        }
                        let key = iter.key().ok_or_else(recovery_unknown)?;
                        if !key.starts_with(&prefix) {
                            break;
                        }
                        let value = iter.value()?;
                        out.record(4, &key[prefix.len()..], &value)?;
                        written += key.len() + value.len() + 13;
                        iter.next();
                    }
                    if done {
                        iter.status()?;
                        out.raw(&[255])?;
                        out.flush()?;
                        (&**file)
                            .write_all(&checksum.finalize())
                            .map_err(file_error)?;
                        let bytes = file.metadata().map_err(|_| storage_error_message())?.len();
                        return Ok(Step::Done(Box::new(GroupSnapshot {
                            file,
                            path,
                            bytes,
                            header,
                        })));
                    }
                    out.flush()?;
                    Ok(Step::More(Box::new(Materializing {
                        file,
                        path,
                        iter,
                        prefix,
                        checksum,
                    })))
                })
                .await?;
            match step {
                Step::More(next) => state = *next,
                Step::Done(file) => return Ok(*file),
            }
        }
    }
    /// One bounded materialization job with its own scratch charge.
    async fn materialize_job<T: Send + 'static>(
        &self,
        class: ExecutionClass,
        budget: TransactionBudget,
        work: impl FnOnce(&mut ByteReservation) -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let unused = self
            .resources()
            .reply_budget(ExecutionClass::Control, 0)
            .await?;
        let prepared = self.resources().prepare(class).await?;
        let (mut scratch, _batch, _unused) = prepared
            .buffers(
                TransactionBudget {
                    output_bytes: 0,
                    batch_bytes: 0,
                    ..budget
                },
                unused,
            )
            .await?;
        prepared.execute(0, 0, move || work(&mut scratch)).await
    }

    pub async fn install_snapshot(
        &self,
        source: GroupSnapshot,
        budget: TransactionBudget,
    ) -> Result<(), ProtocolError> {
        self.install_with_hook(source, budget, |_| Ok(())).await
    }
    pub(super) async fn install_with_hook(
        &self,
        source: GroupSnapshot,
        budget: TransactionBudget,
        mut hook: impl FnMut(InstallPoint) -> Result<(), ProtocolError> + Send + 'static,
    ) -> Result<(), ProtocolError> {
        let generation_owner = self.inner.generation.clone().write_owned().await;
        let owner = self.inner.order.clone().lock_owned().await;
        // The application order is held, so every applied write of this group
        // has completed. Sync any unsynced one before a snapshot can name its
        // index, without holding a compute permit while storage syncs.
        self.sync_deferred_applies().await?;
        let group = self.clone();
        let output = self
            .resources()
            .reply_budget(ExecutionClass::Control, 0)
            .await?;
        let prepared = self.resources().prepare(self.execution_class()).await?;
        let reservations = prepared
            .buffers(
                TransactionBudget {
                    output_bytes: 0,
                    ..budget
                },
                output,
            )
            .await?;
        prepared
            .execute(0, 0, move || {
                let _owner = owner;
                let _generation_owner = generation_owner;
                group.pool().healthy()?;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    group.install_sync(source, budget, reservations, &mut hook)
                }));
                match result {
                    Ok(result) => result,
                    Err(_) => {
                        group.pool().inner.recovery.store(true, Ordering::Release);
                        Err(recovery_unknown())
                    }
                }
            })
            .await
    }
    fn install_sync(
        &self,
        source: GroupSnapshot,
        budget: TransactionBudget,
        (mut scratch, batch_charge, _output): (ByteReservation, ByteReservation, ByteReservation),
        hook: &mut impl FnMut(InstallPoint) -> Result<(), ProtocolError>,
    ) -> Result<(), ProtocolError> {
        // The transfer handle pins an immutable file, not a whole-state buffer.
        let metadata = source
            .file
            .metadata()
            .map_err(|_| storage_error_message())?;
        if !metadata.is_file() || metadata.len() != source.bytes {
            return Err(storage_error_message());
        }
        let mut source_file = source
            .file
            .try_clone()
            .map_err(|_| storage_error_message())?;
        source_file
            .seek(SeekFrom::Start(0))
            .map_err(|_| storage_error_message())?;
        let path = new_snapshot_path(self)?;
        let mut retained = false;
        let mut stage = None;
        let result = (|| {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(file_error)?;
            let copy_len = scratch.remaining().min(64 << 10);
            if copy_len == 0 {
                return Err(capacity());
            }
            let mut copy = scratch.buffer(copy_len)?;
            let mut remaining = source.bytes;
            while remaining > 0 {
                let chunk = (remaining.min(copy_len as u64)) as usize;
                source_file
                    .read_exact(&mut copy.as_mut_slice()[..chunk])
                    .map_err(|_| storage_error_message())?;
                file.write_all(&copy.as_mut_slice()[..chunk])
                    .map_err(file_error)?;
                remaining -= chunk as u64;
            }
            drop(copy);
            if file.metadata().map_err(|_| storage_error_message())?.len() != source.bytes {
                return Err(storage_error_message());
            }
            file.sync_all().map_err(file_error)?;
            sync_directory(path.parent().ok_or_else(storage_error_message)?)?;
            file.seek(SeekFrom::Start(0))
                .map_err(|_| storage_error_message())?;
            let mut checksum = Sha256::new();
            let mut magic = [0u8; 16];
            read_hashed(&mut file, &mut checksum, &mut magic)?;
            if &magic != MAGIC {
                return Err(storage_error_message());
            }
            let mut count = [0u8; 4];
            read_hashed(&mut file, &mut checksum, &mut count)?;
            let header_len = u32::from_be_bytes(count) as usize;
            if header_len > HEADER_LIMIT {
                return Err(capacity());
            }
            let mut header_buffer = scratch.buffer(header_len)?;
            read_hashed(&mut file, &mut checksum, header_buffer.as_mut_slice())?;
            let header: GroupSnapshotHeader = serde_json::from_slice(header_buffer.as_mut_slice())
                .map_err(|_| storage_error_message())?;
            drop(header_buffer);
            if header.identity != *self.identity() || header != source.header {
                return Err(storage_error_message());
            }
            let scope = group_scope(self.identity().group_id);
            let current = self.pool().snapshot();
            let applied = self.read_applied(&current, &mut scratch)?;
            match (applied.as_ref(), header.applied) {
                (Some(existing), Some(incoming))
                    if incoming.index < existing.position.index
                        || incoming.index == existing.position.index
                            && (incoming != existing.position
                                || header.command_digest != Some(existing.command_digest)) =>
                {
                    return Err(storage_error_message());
                }
                (Some(_), None) => return Err(storage_error_message()),
                _ => {}
            }
            let active = read_generation(self.pool(), self.identity().group_id, b'a')?;
            let generation = active.checked_add(1).ok_or_else(capacity)?;
            let prefix = group_key(self.identity().group_id, b's', &generation.to_be_bytes());
            let meta_prefix = group_key(self.identity().group_id, b'u', &generation.to_be_bytes());
            let mut start = WriteBatch::new();
            start.delete_range(&prefix, &prefix_end(&prefix));
            start.delete_range(&meta_prefix, &prefix_end(&meta_prefix));
            start.put(
                &group_key(self.identity().group_id, b't', &[]),
                &generation.to_be_bytes(),
            );
            self.pool().durable_write(&scope, start)?;
            stage = Some(generation);
            if budget.batch_bytes < 256 {
                return Err(capacity());
            }
            let mut batch = WriteBatch::with_capacity_bytes(budget.batch_bytes);
            let mut used = 12usize;
            let mut last_tag = 0u8;
            let mut previous_key = Vec::<u8>::new();
            loop {
                let mut tag = [0u8; 1];
                file.read_exact(&mut tag)
                    .map_err(|_| storage_error_message())?;
                if tag[0] == 255 {
                    break;
                }
                checksum.update(tag);
                if !(1..=4).contains(&tag[0])
                    || tag[0] < last_tag
                    || tag[0] < 4 && tag[0] == last_tag
                {
                    return Err(storage_error_message());
                }
                let mut lens = [0u8; 12];
                read_hashed(&mut file, &mut checksum, &mut lens)?;
                let key_len =
                    u32::from_be_bytes(lens[..4].try_into().expect("fixed key length")) as usize;
                let value_len = usize::try_from(u64::from_be_bytes(
                    lens[4..].try_into().expect("fixed value length"),
                ))
                .map_err(|_| capacity())?;
                if key_len > MAX_KEY_BYTES || tag[0] < 4 && key_len != 0 {
                    return Err(storage_error_message());
                }
                let length = key_len.checked_add(value_len).ok_or_else(capacity)?;
                if length
                    .checked_add(key_len)
                    .and_then(|n| n.checked_add(previous_key.capacity().max(key_len)))
                    .and_then(|n| n.checked_add(64))
                    .is_none_or(|n| n > scratch.remaining())
                    || length
                        .checked_add(64)
                        .is_none_or(|n| n > budget.batch_bytes)
                {
                    return Err(capacity());
                }
                let mut record = scratch.buffer(length)?;
                read_hashed(&mut file, &mut checksum, record.as_mut_slice())?;
                let (key, value) = record.as_slice().split_at(key_len);
                if tag[0] == 4 && last_tag == 4 && previous_key.as_slice() >= key {
                    return Err(storage_error_message());
                }
                let mut disk_key = if tag[0] == 4 {
                    prefix.clone()
                } else {
                    meta_prefix.clone()
                };
                if tag[0] == 4 {
                    disk_key.extend_from_slice(key);
                } else {
                    disk_key.push(tag[0]);
                }
                let cost = disk_key
                    .len()
                    .checked_add(value_len)
                    .and_then(|n| n.checked_add(BATCH_OVERHEAD))
                    .ok_or_else(capacity)?;
                if cost.checked_add(12).is_none_or(|n| n > budget.batch_bytes) {
                    return Err(capacity());
                }
                if used
                    .checked_add(cost)
                    .is_none_or(|n| n > budget.batch_bytes)
                {
                    self.pool().durable_write(&scope, batch)?;
                    batch = WriteBatch::with_capacity_bytes(budget.batch_bytes);
                    used = 12;
                }
                batch.put(&disk_key, value);
                used += cost;
                if tag[0] == 4 {
                    previous_key.clear();
                    if previous_key.capacity() < key.len() {
                        previous_key
                            .try_reserve_exact(key.len())
                            .map_err(|_| capacity())?;
                    }
                    previous_key.extend_from_slice(key);
                }
                last_tag = tag[0];
            }
            let mut expected = [0u8; 32];
            file.read_exact(&mut expected)
                .map_err(|_| storage_error_message())?;
            let mut extra = [0u8; 1];
            if file.read(&mut extra).map_err(|_| storage_error_message())? != 0
                || checksum.finalize().as_slice() != expected
            {
                return Err(storage_error_message());
            }
            self.pool().durable_write(&scope, batch)?;
            let staged = self.pool().snapshot();
            let read_stage = |tag| {
                staged
                    .get(&[meta_prefix.as_slice(), &[tag]].concat())
                    .map_err(storage_error)
            };
            let p = read_stage(1)?;
            let m = read_stage(2)?;
            let r = read_stage(3)?;
            let expected_applied = p.as_ref().map(|bytes| decode_applied(bytes)).transpose()?;
            if expected_applied.as_ref().map(|v| v.0) != header.applied
                || expected_applied.as_ref().map(|v| v.1) != header.command_digest
                || m.as_ref().map(|v| hash(v)) != header.membership_digest
                || r.as_ref().map(|v| hash(v)) != header.response_digest
            {
                return Err(storage_error_message());
            }
            hook(InstallPoint::BeforeActivation)?;
            let mut activate = WriteBatch::with_capacity_bytes(budget.batch_bytes);
            let mut cost = 512usize;
            for (tag, value) in [(b'p', p), (b'm', m), (b'r', r)] {
                let key = group_key(self.identity().group_id, tag, &[]);
                if let Some(value) = value {
                    cost = cost
                        .checked_add(value.len() + BATCH_OVERHEAD)
                        .ok_or_else(capacity)?;
                    if cost > batch_charge.remaining() {
                        return Err(capacity());
                    }
                    activate.put(&key, &value);
                } else {
                    activate.delete(&key);
                }
            }
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or_else(storage_error_message)?;
            activate.put(
                &group_key(self.identity().group_id, b'f', &[]),
                name.as_bytes(),
            );
            activate.put(
                &group_key(self.identity().group_id, b'a', &[]),
                &generation.to_be_bytes(),
            );
            activate.delete(&group_key(self.identity().group_id, b't', &[]));
            let old = group_key(self.identity().group_id, b's', &active.to_be_bytes());
            activate.delete_range(&old, &prefix_end(&old));
            activate.delete_range(&meta_prefix, &prefix_end(&meta_prefix));
            self.pool().durable_write(&scope, activate)?;
            retained = true;
            stage = None;
            hook(InstallPoint::AfterActivation)?;
            sync_directory(path.parent().ok_or_else(storage_error_message)?)?;
            cleanup_files(
                &self.pool().inner.path,
                self.identity().group_id,
                Some(name),
            )?;
            Ok(())
        })();
        if let Some(generation) = stage {
            clear_stage(self.pool(), self.identity().group_id, generation)?;
        }
        if !retained {
            let _ = fs::remove_file(&path);
        }
        result
    }
}

#[derive(Clone, Copy)]
pub(super) enum InstallPoint {
    BeforeActivation,
    AfterActivation,
}
fn hash(value: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(value).into())
}
fn decode_applied(data: &[u8]) -> Result<(AppliedPosition, Digest), ProtocolError> {
    if data.len() != 56 {
        return Err(storage_error_message());
    }
    let number = |offset: usize| {
        u64::from_be_bytes(
            data[offset..offset + 8]
                .try_into()
                .expect("fixed applied field"),
        )
    };
    Ok((
        AppliedPosition {
            term: number(0),
            node_id: number(8),
            index: number(16),
        },
        Digest::from_bytes(data[24..].try_into().expect("fixed digest")),
    ))
}
fn read_generation(pool: &OltpPool, id: Uuid, tag: u8) -> Result<u64, ProtocolError> {
    let bytes = pool
        .get_latest(&group_key(id, tag, &[]))?
        .ok_or_else(storage_error_message)?;
    Ok(u64::from_be_bytes(
        bytes
            .as_ref()
            .try_into()
            .map_err(|_| storage_error_message())?,
    ))
}
fn new_snapshot_path(group: &GroupStore) -> Result<PathBuf, ProtocolError> {
    let directory = group
        .pool()
        .inner
        .path
        .join("snapshots")
        .join(group.identity().group_id.to_string());
    fs::create_dir_all(&directory).map_err(|_| storage_error_message())?;
    let metadata = fs::symlink_metadata(&directory).map_err(|_| storage_error_message())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(storage_error_message());
    }
    Ok(directory.join(format!("snap-{}.bin", Uuid::new_v4())))
}
/// SNAP1: a create_new name under `snapshots/<group>/outgoing/`, each level a
/// real directory (no symlink), like the incoming side.
fn new_outgoing_path(group: &GroupStore) -> Result<PathBuf, ProtocolError> {
    let mut directory = group.pool().inner.path.clone();
    for component in [
        "snapshots".to_owned(),
        group.identity().group_id.to_string(),
        OUTGOING.to_owned(),
    ] {
        directory.push(component);
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(storage_error_message()),
        }
        let metadata = fs::symlink_metadata(&directory).map_err(|_| storage_error_message())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(storage_error_message());
        }
    }
    Ok(directory.join(format!("send-{}.bin", Uuid::new_v4())))
}
const OUTGOING: &str = "outgoing";
fn valid_outgoing_name(name: &str) -> bool {
    name.strip_prefix("send-")
        .and_then(|value| value.strip_suffix(".bin"))
        .is_some_and(|value| Uuid::parse_str(value).is_ok())
}
fn read_hashed(file: &mut File, hash: &mut Sha256, bytes: &mut [u8]) -> Result<(), ProtocolError> {
    file.read_exact(bytes)
        .map_err(|_| storage_error_message())?;
    hash.update(bytes);
    Ok(())
}
fn sync_directory(path: &Path) -> Result<(), ProtocolError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| storage_error_message())
}
fn clear_stage(pool: &OltpPool, id: Uuid, generation: u64) -> Result<(), ProtocolError> {
    if read_generation(pool, id, b'a')? == generation {
        return Err(storage_error_message());
    }
    let mut batch = WriteBatch::new();
    for &tag in b"su" {
        let prefix = group_key(id, tag, &generation.to_be_bytes());
        batch.delete_range(&prefix, &prefix_end(&prefix));
    }
    batch.delete(&group_key(id, b't', &[]));
    pool.durable_write(&group_scope(id), batch)
}
pub(super) fn recover_group(
    pool: &OltpPool,
    identity: &GroupIdentity,
) -> Result<(), ProtocolError> {
    let mut ancestor = pool.inner.path.clone();
    for component in [
        "snapshots".to_owned(),
        identity.group_id.to_string(),
        "incoming".to_owned(),
    ] {
        ancestor.push(component);
        match fs::symlink_metadata(&ancestor) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            _ => return Err(storage_error_message()),
        }
    }
    let incoming = pool
        .inner
        .path
        .join("snapshots")
        .join(identity.group_id.to_string())
        .join("incoming");
    if incoming.exists() {
        let metadata = fs::symlink_metadata(&incoming).map_err(|_| storage_error_message())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(storage_error_message());
        }
        for item in fs::read_dir(&incoming).map_err(|_| storage_error_message())? {
            let item = item.map_err(|_| storage_error_message())?;
            let name = item.file_name();
            let Some(name) = name.to_str() else {
                return Err(storage_error_message());
            };
            let valid = name
                .strip_prefix("recv-")
                .and_then(|n| n.strip_suffix(".bin"))
                .and_then(|n| n.split_once('-'))
                .is_some_and(|(node, id)| {
                    node.parse::<u64>().is_ok_and(|n| n != 0)
                        && Uuid::parse_str(id).is_ok_and(|id| !id.is_nil())
                });
            if !valid
                || !item
                    .file_type()
                    .map_err(|_| storage_error_message())?
                    .is_file()
            {
                return Err(storage_error_message());
            }
            fs::remove_file(item.path()).map_err(|_| storage_error_message())?;
        }
        sync_directory(&incoming)?;
    }
    // SNAP1: outgoing files are transient sender state that a crash left.
    let outgoing = pool
        .inner
        .path
        .join("snapshots")
        .join(identity.group_id.to_string())
        .join(OUTGOING);
    match fs::symlink_metadata(&outgoing) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            for item in fs::read_dir(&outgoing).map_err(|_| storage_error_message())? {
                let item = item.map_err(|_| storage_error_message())?;
                let name = item.file_name();
                if !name.to_str().is_some_and(valid_outgoing_name)
                    || !item
                        .file_type()
                        .map_err(|_| storage_error_message())?
                        .is_file()
                {
                    return Err(storage_error_message());
                }
                fs::remove_file(item.path()).map_err(|_| storage_error_message())?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(storage_error_message()),
    }

    if pool
        .get_latest(&group_key(identity.group_id, b't', &[]))?
        .is_some()
    {
        let generation = read_generation(pool, identity.group_id, b't')?;
        clear_stage(pool, identity.group_id, generation)?;
    }
    let current = pool.get_latest(&group_key(identity.group_id, b'f', &[]))?;
    let name = current
        .as_ref()
        .map(|v| std::str::from_utf8(v).map_err(|_| storage_error_message()))
        .transpose()?;
    cleanup_files(&pool.inner.path, identity.group_id, name)
}
/// NS14 rule 4: retires every snapshot file of a group whose damaged bytes
/// were released. Idempotent.
pub(super) fn retire_group_files(root: &Path, id: Uuid) -> Result<(), ProtocolError> {
    cleanup_files(root, id, None)
}
fn cleanup_files(root: &Path, id: Uuid, keep: Option<&str>) -> Result<(), ProtocolError> {
    let directory = root.join("snapshots").join(id.to_string());
    if let Some(name) = keep
        && (!valid_snapshot_name(name) || !directory.join(name).is_file())
    {
        return Err(recovery_unknown());
    }
    let files = match fs::read_dir(&directory) {
        Ok(files) => files,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(storage_error_message()),
    };
    for file in files {
        let file = file.map_err(|_| storage_error_message())?;
        let name = file.file_name();
        let Some(text) = name.to_str() else { continue };
        if Some(text) == keep {
            continue;
        }
        if valid_snapshot_name(text) {
            fs::remove_file(file.path()).map_err(|_| storage_error_message())?;
        }
    }
    sync_directory(&directory)
}

fn valid_snapshot_name(name: &str) -> bool {
    name.strip_prefix("snap-")
        .and_then(|value| value.strip_suffix(".bin"))
        .is_some_and(|value| Uuid::parse_str(value).is_ok())
}

fn read_exact_at(
    file: &File,
    total: u64,
    offset: &mut u64,
    mut target: &mut [u8],
) -> Result<(), ProtocolError> {
    if offset
        .checked_add(target.len() as u64)
        .is_none_or(|end| end > total)
    {
        return Err(storage_error_message());
    }
    while !target.is_empty() {
        #[cfg(unix)]
        let count = {
            use std::os::unix::fs::FileExt;
            file.read_at(target, *offset)
        };
        #[cfg(windows)]
        let count = {
            use std::os::windows::fs::FileExt;
            file.seek_read(target, *offset)
        };
        #[cfg(not(any(unix, windows)))]
        let count: std::io::Result<usize> = Err(std::io::Error::other(
            "positional snapshot reads unsupported",
        ));
        let count = count.map_err(|_| storage_error_message())?;
        if count == 0 {
            return Err(storage_error_message());
        }
        *offset += count as u64;
        target = &mut target[count..];
    }
    Ok(())
}
