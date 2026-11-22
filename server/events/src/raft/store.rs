use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use bytes::Bytes;
use nabedra_kv::batch::MAX_SCOPE_LEN;
use nabedra_kv::fs::Fs;
use nabedra_kv::{
    Background, BatchOp, BlockCache, Damage, Durability, Iter, MemtableBudget, Opened, Snapshot,
    Store, WriteBatch,
};
use openraft::{LogId, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{Notify, OwnedSemaphorePermit, RwLock, Semaphore, TryAcquireError, mpsc};
use uuid::Uuid;

use super::Node;

#[cfg(test)]
mod rv_ev_tests;
/// Small memtables and journal segments on `fs`, for tests outside this module.
#[cfg(test)]
pub(crate) fn test_options(fs: &nabedra_kv::fs::FaultFs) -> nabedra_kv::Options {
    tests::small(fs)
}
#[cfg(test)]
mod tests;

const DATABASE_IDENTITY: &[u8] = b"identity/v1";
const HOSTED: &[u8] = b"hosted/";
const RETIRED: &[u8] = b"retired/v1/";
const GROUP: &[u8] = b"group/";
const MAX_RECORD_KEY_BYTES: usize = 4 * 1024;
pub(super) const MAX_RECORD_VALUE_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCAN_ITEMS: usize = 4096;
const MAX_SCAN_BYTES: usize = 8 * 1024 * 1024;

/// The store lives in `<root>/kv`. A `<root>/rocksdb` directory holds data from a release
/// before nabedra-kv; there is no migration (clean break), so it refuses the broker.
pub(super) const KV_DIR: &str = "kv";
const LEGACY_DIR: &str = "rocksdb";
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const MEMTABLE_BUDGET_BYTES: usize = 48 * 1024 * 1024;
const MEMTABLE_BYTES: usize = 16 * 1024 * 1024;
const BACKGROUND_THREADS: usize = 2;
/// A Raft storage write that meets a full device is retried after this delay, doubling
/// up to [`NO_SPACE_MAX_BACKOFF`], for as long as its group stays open (RV-EV M1).
const NO_SPACE_FIRST_BACKOFF: Duration = Duration::from_millis(10);
const NO_SPACE_MAX_BACKOFF: Duration = Duration::from_secs(1);
/// How often a parked retry checks whether its group is closing.
const RETRY_POLL: Duration = Duration::from_millis(5);

/// The scope of a key: `group/{id}/` for a group key, else store-wide (0). Damage to a
/// group's records quarantines only that group; damage to any other record refuses the
/// store.
pub(super) fn scope_of(key: &[u8]) -> usize {
    key.strip_prefix(GROUP)
        .and_then(|rest| rest.iter().position(|&byte| byte == b'/'))
        .map(|end| GROUP.len() + end + 1)
        .filter(|&len| len > GROUP.len() + 1 && len <= MAX_SCOPE_LEN)
        .unwrap_or(0)
}

/// The group a quarantined scope belongs to.
fn scope_group(scope: &[u8]) -> Option<&str> {
    (scope_of(scope) == scope.len() && !scope.is_empty())
        .then(|| std::str::from_utf8(&scope[GROUP.len()..scope.len() - 1]).ok())
        .flatten()
}

/// A hosted group whose storage is damaged. It is not started: it neither votes nor
/// acknowledges appends until an operator replaces this replica.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantinedGroup {
    pub group: String,
    pub reason: String,
    pub first_bad_seq: Option<u64>,
}

fn quarantined_group(damage: &Damage) -> Option<QuarantinedGroup> {
    Some(QuarantinedGroup {
        group: scope_group(&damage.scope)?.to_owned(),
        reason: format!("{:?} in {:?}", damage.reason, damage.file),
        first_bad_seq: damage.first_bad_seq,
    })
}

pub(super) struct Database {
    store: Option<Store>,
    kv_dir: PathBuf,
    pub root: PathBuf,
    io: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
    damaged: Mutex<Option<mpsc::UnboundedReceiver<String>>>,
    /// Woken when the store may have latched `RecoveryRequired` (store-wide damage or an
    /// ambiguous write failure).
    recovery: Arc<Notify>,
    no_space: NoSpace,
    #[cfg(test)]
    pub(super) inject_no_space: Mutex<BTreeMap<&'static str, u32>>,
}

/// Full-device accounting for `/v1/admin/broker` (RV-EV M1).
#[derive(Default)]
struct NoSpace {
    /// The last write met a full device, and no write has succeeded since.
    active: AtomicBool,
    last_ms: AtomicU64,
    retries: Mutex<BTreeMap<&'static str, u64>>,
}

/// The broker store's health, as `/v1/admin/broker` reports it.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageHealth {
    /// The store refuses all work until the process reopens it (RV-EV M2).
    pub recovery_required: bool,
    /// The last storage write met a full device and none has succeeded since. Raft writes
    /// retry until space frees up (RV-EV M1).
    pub no_space: bool,
    /// Unix milliseconds of the last full-device write, if any.
    pub last_no_space_ms: Option<u64>,
    /// Retries of full-device writes, by Raft storage operation.
    pub no_space_retries: BTreeMap<String, u64>,
    pub journal_bytes: u64,
    pub stall_ms: u64,
}

/// The retry context of the Raft storage job running on this blocking thread: which
/// group it serves and the I/O permit it holds. `Database::write` retries `NoSpace` only
/// inside one, so broker-level writes (hosting, retirement) still fail fast.
struct RetryScope {
    label: &'static str,
    closing: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
    permit: Option<OwnedSemaphorePermit>,
}

thread_local! {
    static RETRY: RefCell<Option<RetryScope>> = const { RefCell::new(None) };
}

/// Clears this thread's retry scope (and drops its permit) when the job ends.
struct RetryGuard;

impl RetryGuard {
    fn enter(scope: RetryScope) -> RetryGuard {
        RETRY.with_borrow_mut(|slot| *slot = Some(scope));
        RetryGuard
    }
}

impl Drop for RetryGuard {
    fn drop(&mut self) {
        RETRY.with_borrow_mut(|slot| slot.take());
    }
}

fn retry_label() -> Option<&'static str> {
    RETRY.with_borrow(|scope| scope.as_ref().map(|scope| scope.label))
}

/// Parks the current Raft storage job for `delay` without holding its I/O permit, then
/// takes a permit again. Fails when there is no retry scope or the group is closing.
fn park_for_space(delay: Duration) -> Result<()> {
    RETRY.with_borrow_mut(|scope| {
        let scope = scope
            .as_mut()
            .ok_or_else(|| anyhow!("no retry scope for this storage write"))?;
        let closing = |scope: &RetryScope| scope.closing.load(Ordering::Acquire);
        // Other groups' storage work proceeds while this one waits for space.
        scope.permit.take();
        let deadline = Instant::now() + delay;
        loop {
            ensure!(!closing(scope), "Raft group storage is closing");
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            std::thread::sleep(RETRY_POLL.min(deadline - now));
        }
        loop {
            ensure!(!closing(scope), "Raft group storage is closing");
            match scope.semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    scope.permit = Some(permit);
                    return Ok(());
                }
                Err(TryAcquireError::NoPermits) => std::thread::sleep(RETRY_POLL),
                Err(TryAcquireError::Closed) => anyhow::bail!("storage permits closed"),
            }
        }
    })
}

fn unix_ms() -> u64 {
    crate::rt::unix_ms()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    cluster: Uuid,
    broker: u64,
}

/// Closes of stores dropped on an async runtime, which run on their own thread because
/// closing joins the store's threads. Reopening the same directory waits for its close.
static CLOSING: Mutex<Vec<(PathBuf, JoinHandle<()>)>> = Mutex::new(Vec::new());

pub(super) fn wait_for_close(dir: &Path) {
    let pending: Vec<_> = {
        let mut closing = CLOSING.lock().unwrap_or_else(PoisonError::into_inner);
        let (done, rest) = std::mem::take(&mut *closing)
            .into_iter()
            .partition(|(path, handle)| path == dir || handle.is_finished());
        *closing = rest;
        done
    };
    for (_, handle) in pending {
        let _ = handle.join();
    }
}

fn close(dir: &Path, store: Store) {
    if let Err(error) = store.close() {
        tracing::error!(dir = %dir.display(), %error, "Events store did not close cleanly");
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let Some(store) = self.store.take() else {
            return;
        };
        let dir = self.kv_dir.clone();
        if tokio::runtime::Handle::try_current().is_err() {
            return close(&dir, store);
        }
        let closing = dir.clone();
        match std::thread::Builder::new()
            .name("events-kv-close".into())
            .spawn(move || close(&closing, store))
        {
            Ok(handle) => CLOSING
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((dir, handle)),
            Err(error) => {
                tracing::error!(%error, "could not start the Events store close thread")
            }
        }
    }
}

impl Database {
    /// Production store options: broker-owned cache, memtable budget and background
    /// threads, with group-scoped damage.
    pub fn options(fs: Arc<dyn Fs>) -> nabedra_kv::Options {
        let mut options = nabedra_kv::Options::new(
            fs,
            Arc::new(BlockCache::new(CACHE_BYTES)),
            Arc::new(MemtableBudget::new(MEMTABLE_BUDGET_BYTES)),
            // `Background::new` returns the OS's refusal to start a thread (RV-NS6 F9);
            // the broker cannot run without its store threads.
            Background::new(BACKGROUND_THREADS).expect("start the Events kv background threads"),
        );
        options.memtable_bytes = MEMTABLE_BYTES;
        options.scope_of = scope_of;
        options
    }

    pub fn open(root: &Path, cluster: Uuid, broker: u64) -> Result<Arc<Self>> {
        Self::open_with(root, cluster, broker, Self::options(nabedra_kv::fs::real()))
    }

    /// Opens the broker store. Opening blocks on I/O and waits for commits, which must not
    /// happen on a thread that drives an async runtime, so it runs on a helper thread.
    pub fn open_with(
        root: &Path,
        cluster: Uuid,
        broker: u64,
        options: nabedra_kv::Options,
    ) -> Result<Arc<Self>> {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("events-kv-open".into())
                .spawn_scoped(scope, || {
                    Self::open_blocking(root, cluster, broker, options)
                })?
                .join()
                .map_err(|_| anyhow!("the Events store open panicked"))?
        })
    }

    fn open_blocking(
        root: &Path,
        cluster: Uuid,
        broker: u64,
        options: nabedra_kv::Options,
    ) -> Result<Arc<Self>> {
        let legacy = root.join(LEGACY_DIR);
        if std::fs::symlink_metadata(&legacy).is_ok() {
            return Err(nabedra_kv::Error::UnsupportedFormat {
                found: format!(
                    "{}: RocksDB data from an earlier release; start on an empty volume",
                    legacy.display()
                ),
            }
            .into());
        }
        std::fs::create_dir_all(root.join("snapshots"))?;
        std::fs::File::open(root)?.sync_all()?;

        let kv_dir = root.join(KV_DIR);
        wait_for_close(&kv_dir);
        let Opened {
            store,
            quarantined,
            recovery,
            ..
        } = Store::open(&kv_dir, options)
            .with_context(|| format!("open Events store {}", kv_dir.display()))?;
        if recovery.tail_truncations > 0 || recovery.manifest_torn_tail {
            tracing::warn!(?recovery, "Events store recovered a torn tail");
        } else {
            tracing::info!(?recovery, "Events store opened");
        }
        let (damaged, receiver) = mpsc::unbounded_channel();
        let recovery = Arc::new(Notify::new());
        let latched = recovery.clone();
        store.on_damage(Box::new(move |damage| match quarantined_group(&damage) {
            Some(group) => {
                tracing::error!(
                    ?group,
                    "Events group storage is damaged; stopping the group"
                );
                let _ = damaged.send(group.group);
            }
            None => {
                tracing::error!(
                    ?damage,
                    "Events store-wide damage; the store refuses work until it is reopened"
                );
                latched.notify_one();
            }
        }));
        let database = Self {
            store: Some(store),
            kv_dir,
            root: root.to_path_buf(),
            io: Arc::new(Semaphore::new(8)),
            bulk: Arc::new(Semaphore::new(1)),
            damaged: Mutex::new(Some(receiver)),
            recovery,
            no_space: NoSpace::default(),
            #[cfg(test)]
            inject_no_space: Mutex::default(),
        };
        for damage in &quarantined {
            match quarantined_group(damage) {
                Some(group) => tracing::error!(
                    ?group,
                    "Events group storage is damaged; the group stays stopped until an operator \
                     re-adds this replica"
                ),
                None => return Err(anyhow!("store-wide damage was quarantined: {damage:?}")),
            }
        }

        let wanted = Identity { cluster, broker };
        let on_disk: Option<Identity> = decode(database.get(DATABASE_IDENTITY)?)?;
        match on_disk {
            Some(on_disk) => ensure!(
                on_disk == wanted,
                "data directory belongs to another Events cluster or broker"
            ),
            None => {
                let mut batch = WriteBatch::new();
                batch.put(DATABASE_IDENTITY, &serde_json::to_vec(&wanted)?);
                database.write(batch)?;
            }
        }
        database.finish_retirements()?;
        Ok(Arc::new(database))
    }

    pub fn kv_dir(&self) -> &Path {
        &self.kv_dir
    }

    fn store(&self) -> &Store {
        self.store
            .as_ref()
            .expect("the Events store is open until drop")
    }

    /// Damage found while serving, by group. Taken once by the broker.
    pub fn take_damage_events(&self) -> Option<mpsc::UnboundedReceiver<String>> {
        self.damaged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// A consistent read view.
    pub fn snapshot(&self) -> Snapshot {
        self.store().snapshot()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.snapshot().get(key)?)
    }

    /// Commit `batch` durably. Blocks: call it only from blocking work (`run`). A batch
    /// whose keys all lie under one `group/{id}/` takes that scope, so damage to it
    /// quarantines only that group; any other batch is store-wide.
    ///
    /// A batch into a quarantined group is refused: that group neither votes nor
    /// acknowledges (section 3, rule 1; RV-EV L3). Writes are paced while L0 is at its
    /// slowdown trigger (RV-FINAL L6). Inside a Raft storage job (`GroupStore::run*`), a
    /// full device (`NoSpace`, nothing written) is retried with capped backoff until it
    /// succeeds or the group closes, so a transient full disk never stops a Raft core
    /// (RV-EV M1).
    pub fn write(&self, mut batch: WriteBatch) -> Result<()> {
        confine(&mut batch);
        if !batch.scope().is_empty() && self.store().is_quarantined(batch.scope()) {
            return Err(nabedra_kv::Error::Corruption {
                file: nabedra_kv::FileId::Other,
                offset: 0,
                what: "write into a quarantined group",
            }
            .into());
        }
        let mut backoff = NO_SPACE_FIRST_BACKOFF;
        loop {
            let label = retry_label();
            let attempt = match label {
                Some(_) => batch.clone(),
                None => std::mem::take(&mut batch),
            };
            match self.submit_sync(attempt, label) {
                Ok(()) => {
                    self.no_space.active.store(false, Ordering::Relaxed);
                    return Ok(());
                }
                Err(nabedra_kv::Error::NoSpace) => {
                    self.no_space.active.store(true, Ordering::Relaxed);
                    self.no_space.last_ms.store(unix_ms(), Ordering::Relaxed);
                    let Some(label) = label else {
                        return Err(nabedra_kv::Error::NoSpace.into());
                    };
                    *self
                        .no_space
                        .retries
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .entry(label)
                        .or_default() += 1;
                    if backoff == NO_SPACE_FIRST_BACKOFF {
                        tracing::warn!(
                            write = label,
                            "Events storage device is full; retrying until space frees up"
                        );
                    }
                    park_for_space(backoff).context("Events storage device is full")?;
                    backoff = (backoff * 2).min(NO_SPACE_MAX_BACKOFF);
                }
                Err(error) => {
                    if error.is_ambiguous() || self.requires_recovery() {
                        self.recovery.notify_one();
                    }
                    return Err(error.into());
                }
            }
        }
    }

    fn submit_sync(
        &self,
        batch: WriteBatch,
        label: Option<&'static str>,
    ) -> nabedra_kv::Result<()> {
        #[cfg(test)]
        if let Some(label) = label
            && let Some(left) = self
                .inject_no_space
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(label)
                .filter(|left| **left > 0)
        {
            *left -= 1;
            return Err(nabedra_kv::Error::NoSpace);
        }
        let _ = label;
        let store = self.store();
        store.pace(batch.encoded_bytes());
        store
            .submit(batch, Durability::Sync, Box::new(()))
            .and_then(nabedra_kv::Commit::wait)?;
        Ok(())
    }

    /// The store latched `RecoveryRequired`: it refuses all work until it is reopened.
    pub fn requires_recovery(&self) -> bool {
        self.store().requires_recovery()
    }

    /// Resolves once the store requires recovery. Checks again every second in case a
    /// latch was not signalled.
    pub async fn recovery_required(&self) {
        loop {
            if self.requires_recovery() {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_secs(1), self.recovery.notified()).await;
        }
    }

    pub fn health(&self) -> StorageHealth {
        let stats = self.store().stats();
        let last = self.no_space.last_ms.load(Ordering::Relaxed);
        StorageHealth {
            recovery_required: self.requires_recovery(),
            no_space: self.no_space.active.load(Ordering::Relaxed),
            last_no_space_ms: (last != 0).then_some(last),
            no_space_retries: self
                .no_space
                .retries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|(label, count)| ((*label).to_owned(), *count))
                .collect(),
            journal_bytes: stats.journal_bytes,
            stall_ms: stats.stall_ms,
        }
    }

    /// Free space the store needs beyond any configured reserve to keep committing Raft
    /// progress: two journal segments (the active one and the next roll) and the output
    /// of one L0 compaction, which is written before its inputs are deleted (RV-EV M1).
    pub fn disk_headroom_bytes() -> u64 {
        let options = nabedra_kv::journal::JournalOptions::default();
        let levels = nabedra_kv::LevelOptions::default();
        2 * options.segment_bytes
            + levels.l0_compaction_trigger as u64 * MEMTABLE_BYTES as u64
            + levels.max_bytes_for_level_base
    }

    /// Groups whose storage is quarantined.
    pub fn quarantined_groups(&self) -> Vec<QuarantinedGroup> {
        self.store()
            .quarantined()
            .iter()
            .filter_map(quarantined_group)
            .collect()
    }

    pub fn is_quarantined(&self, id: &str) -> bool {
        self.quarantined_groups()
            .iter()
            .any(|group| group.group == id)
    }

    /// Release the damaged bytes of quarantined groups whose retirement committed
    /// (section 3, rule 4) but whose release was interrupted.
    fn finish_retirements(&self) -> Result<()> {
        for group in self.quarantined_groups() {
            let retired = self.get(&[RETIRED, group.group.as_bytes()].concat())?;
            if retired.is_some() && self.get(&Self::hosted_key(&group.group))?.is_none() {
                tracing::info!(group = %group.group, "releasing a retired damaged group");
                self.store().release_scope(&group_root(&group.group))?;
            }
        }
        Ok(())
    }

    pub async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.io, work).await
    }

    pub async fn run_bulk<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.bulk, work).await
    }

    async fn run_bounded<T: Send + 'static>(
        self: &Arc<Self>,
        semaphore: &Arc<Semaphore>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_scoped(semaphore, None, work).await
    }

    /// Runs `work` on a blocking thread holding a permit of `semaphore`. With a retry
    /// scope (a Raft storage job), `write` retries a full device and gives the permit
    /// back while it waits.
    async fn run_scoped<T: Send + 'static>(
        self: &Arc<Self>,
        semaphore: &Arc<Semaphore>,
        retry: Option<(&'static str, Arc<AtomicBool>)>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = semaphore.clone().acquire_owned().await?;
        let database = self.clone();
        let semaphore = semaphore.clone();
        crate::rt::blocking(move || {
            let (_permit, _scope) = match retry {
                Some((label, closing)) => (
                    None,
                    Some(RetryGuard::enter(RetryScope {
                        label,
                        closing,
                        semaphore,
                        permit: Some(permit),
                    })),
                ),
                None => (Some(permit), None),
            };
            // The last reference may drop here, which closes the store off the runtime's
            // async workers.
            work(&database)
        })
        .await?
    }

    pub fn stored_group_ids(&self) -> Result<Vec<String>> {
        entries(&self.snapshot(), HOSTED, &prefix_end(HOSTED), true)
            .map(|entry| {
                let (key, _) = entry?;
                Ok(std::str::from_utf8(&key[HOSTED.len()..])?.to_owned())
            })
            .collect()
    }

    pub fn hosted_key(id: &str) -> Vec<u8> {
        [HOSTED, id.as_bytes()].concat()
    }

    pub fn is_hosted(&self, id: &str) -> Result<bool> {
        Ok(self.get(&Self::hosted_key(id))?.is_some())
    }

    pub fn mark_hosted(&self, id: &str) -> Result<()> {
        if !self.is_hosted(id)? {
            let mut write = WriteBatch::new();
            write.put(&Self::hosted_key(id), b"");
            self.write(write)?;
        }
        Ok(())
    }
}

/// Give `batch` the scope of its first key when every operation lies under it.
fn confine(batch: &mut WriteBatch) {
    let first = batch.iter().next().map(|op| match op {
        BatchOp::Put { key, .. } | BatchOp::JournalPut { key, .. } | BatchOp::Delete { key } => key,
        BatchOp::RangeDelete { start, .. } => start,
    });
    let scope = first.map(|key| key[..scope_of(key)].to_vec());
    if let Some(scope) = scope.filter(|scope| !scope.is_empty()) {
        batch.set_scope(&scope);
        if batch.leaves_scope() {
            batch.set_scope(b"");
        }
    }
}

fn group_root(id: &str) -> Vec<u8> {
    format!("group/{id}/").into_bytes()
}

/// The entries in `[start, end)` at `view`, in key order.
pub(super) fn entries(view: &Snapshot, start: &[u8], end: &[u8], fill_cache: bool) -> Entries {
    Entries {
        iter: view.iter(Included(start), Excluded(end), fill_cache),
        started: false,
        done: false,
    }
}

pub(super) struct Entries {
    iter: Iter,
    started: bool,
    done: bool,
}

impl Iterator for Entries {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.started {
            self.iter.next();
        } else {
            self.iter.seek_to_first();
            self.started = true;
        }
        if !self.iter.valid() {
            self.done = true;
            return self.iter.status().err().map(|error| Err(error.into()));
        }
        match self.iter.value() {
            Ok(value) => Some(Ok((Bytes::copy_from_slice(self.iter.key()), value))),
            Err(error) => {
                self.done = true;
                Some(Err(error.into()))
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct GroupStore {
    pub db: Arc<Database>,
    pub id: Arc<str>,
    root: Arc<[u8]>,
    sealed: Arc<RwLock<bool>>,
    /// Set before the group's Raft core shuts down, so a write parked on a full device
    /// gives up instead of holding the shutdown.
    closing: Arc<AtomicBool>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(super) struct StateHeader {
    pub generation: Uuid,
    pub applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, Node>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct StoredSnapshot {
    pub file_id: Uuid,
    pub meta: SnapshotMeta<u64, Node>,
}

impl GroupStore {
    pub fn new(db: Arc<Database>, id: &str) -> Self {
        Self {
            db,
            id: Arc::from(id),
            root: Arc::from(group_root(id)),
            sealed: Arc::default(),
            closing: Arc::default(),
        }
    }

    pub async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.io, "storage", work).await
    }

    /// [`GroupStore::run`] for a Raft storage operation named `label` in the full-device
    /// retry counters.
    pub async fn run_as<T: Send + 'static>(
        &self,
        label: &'static str,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.io, label, work).await
    }

    pub async fn run_bulk<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.bulk, "bulk", work).await
    }

    pub async fn run_bulk_as<T: Send + 'static>(
        &self,
        label: &'static str,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.bulk, label, work).await
    }

    async fn run_bounded<T: Send + 'static>(
        &self,
        pool: &Arc<Semaphore>,
        label: &'static str,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let activity = self.sealed.clone().read_owned().await;
        ensure!(!*activity, "Raft group storage has closed");
        self.db
            .run_scoped(pool, Some((label, self.closing.clone())), move |db| {
                let _activity = activity;
                work(db)
            })
            .await
    }

    /// Stop full-device retries of this group's storage work. Call before shutting its
    /// Raft core down: the core waits for its storage calls.
    pub fn begin_close(&self) {
        self.closing.store(true, Ordering::Release);
    }

    /// Drain this open generation before its identifier can be reused. A
    /// canceled async caller still holds its read permit in the blocking job.
    pub async fn seal(&self) {
        *self.sealed.write().await = true;
    }

    pub fn key(&self, suffix: &[u8]) -> Vec<u8> {
        [&*self.root, suffix].concat()
    }

    pub fn state_prefix(&self, generation: Uuid) -> Vec<u8> {
        [self.key(b"state/").as_slice(), generation.as_bytes(), b"/"].concat()
    }

    pub fn header(&self) -> Result<StateHeader> {
        Ok(self.read(b"state-header")?.unwrap_or_default())
    }

    pub fn read<T: DeserializeOwned>(&self, suffix: &[u8]) -> Result<Option<T>> {
        decode(self.db.get(&self.key(suffix))?)
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let view = self.db.snapshot();
        let header: StateHeader =
            decode(view.get(&self.key(b"state-header"))?)?.unwrap_or_default();
        Ok(Transaction {
            store: self,
            prefix: self.state_prefix(header.generation),
            view,
            header,
            pending: BTreeMap::new(),
            pending_bytes: 0,
        })
    }

    pub fn write_header(&self, write: &mut WriteBatch, header: &StateHeader) -> Result<()> {
        write.put(&self.key(b"state-header"), &serde_json::to_vec(header)?);
        Ok(())
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.db.root.join("snapshots").join(&*self.id)
    }

    pub fn remove_generation(&self, generation: Uuid, write: &mut WriteBatch) {
        let prefix = self.state_prefix(generation);
        let end = prefix_end(&prefix);
        write.delete_range(&prefix, &end);
    }

    pub fn cleanup_interrupted_stage(&self) -> Result<()> {
        if let Some(generation) = self.read(b"staged-generation")? {
            ensure!(
                self.header()?.generation != generation,
                "active snapshot staging marker"
            );
            let mut write = WriteBatch::new();
            self.remove_generation(generation, &mut write);
            write.delete(&self.key(b"staged-generation"));
            self.db.write(write)?;
        }
        Ok(())
    }

    pub fn retired_at(&self) -> Result<Option<u64>> {
        decode(self.db.get(&[RETIRED, self.id.as_bytes()].concat())?)
    }

    pub fn forget(&self, removed_at: u64) -> Result<()> {
        let quarantined = self.db.is_quarantined(&self.id);
        let mut write = WriteBatch::new();
        write.delete(&Database::hosted_key(&self.id));
        if !quarantined {
            write.delete_range(&self.root, &prefix_end(&self.root));
        }
        write.put(
            &[RETIRED, self.id.as_bytes()].concat(),
            &serde_json::to_vec(&removed_at)?,
        );
        self.db.write(write)?;
        if quarantined {
            // The damaged bytes are kept until the retirement is durable (section 3,
            // rule 4); `Database::open` finishes a release a crash interrupted.
            self.db.store().release_scope(&self.root)?;
        }
        let dir = self.snapshot_dir();
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(())
    }

    /// Release the damaged bytes of this group if its retirement is durable but the
    /// release did not finish (section 3, rule 4; RV-EV L1). True if it released them.
    pub fn finish_release(&self) -> Result<bool> {
        if !self.db.is_quarantined(&self.id)
            || self.retired_at()?.is_none()
            || self.db.is_hosted(&self.id)?
        {
            return Ok(false);
        }
        tracing::info!(group = %self.id, "releasing a retired damaged group");
        self.db.store().release_scope(&self.root)?;
        let dir = self.snapshot_dir();
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(true)
    }

    pub fn clean_snapshot_dir(&self) -> Result<()> {
        let dir = self.snapshot_dir();
        if !dir.exists() {
            std::fs::create_dir(&dir)?;
            std::fs::File::open(self.db.root.join("snapshots"))?.sync_all()?;
        }
        let current: Option<StoredSnapshot> = self.read(b"snapshot")?;
        let keep = current.map(|snapshot| snapshot.file_id.to_string());
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() && keep.as_deref() != entry.file_name().to_str() {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

pub(super) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if let Some(incremented) = last.checked_add(1) {
            end.push(incremented);
            return end;
        }
    }
    // All internal prefixes start with a printable ASCII namespace.
    unreachable!("invalid internal store prefix");
}

pub(super) fn decode<T: DeserializeOwned>(data: Option<impl AsRef<[u8]>>) -> Result<Option<T>> {
    data.map(|data| serde_json::from_slice(data.as_ref()))
        .transpose()
        .context("invalid Events disk record")
}

pub(super) fn validate_record(key: &[u8], value: &[u8]) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= MAX_RECORD_KEY_BYTES,
        "invalid record key size"
    );
    ensure!(
        value.len() <= MAX_RECORD_VALUE_BYTES,
        "record exceeds the value size limit"
    );
    Ok(())
}

/// A consistent view of one application's keys. Writes are visible within the
/// transaction and are committed with the associated applied Raft index.
pub struct Transaction<'a> {
    store: &'a GroupStore,
    prefix: Vec<u8>,
    pub(super) view: Snapshot,
    pub(super) header: StateHeader,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pending_bytes: usize,
}

impl Transaction<'_> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.pending.get(key) {
            return Ok(value.clone());
        }
        Ok(self
            .view
            .get(&[self.prefix.as_slice(), key].concat())?
            .map(|value| value.to_vec()))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_record(key, value)?;
        self.stage(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        validate_record(key, &[])?;
        self.stage(key, None)
    }

    fn stage(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let removed = self
            .pending
            .get(key)
            .map_or(0, |prev| key.len() + prev.as_ref().map_or(0, Vec::len));
        let next = self.pending_bytes - removed + key.len() + value.map_or(0, <[u8]>::len);
        ensure!(
            next <= MAX_PENDING_BYTES,
            "transaction exceeds the write size limit"
        );
        self.pending.insert(key.to_vec(), value.map(<[u8]>::to_vec));
        self.pending_bytes = next;
        Ok(())
    }

    /// Return up to the requested limit with this prefix, strictly after the cursor.
    /// Continue with the final record's key until an empty page. The page can end
    /// early at its independent byte limit.
    pub fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        ensure!(
            limit <= MAX_SCAN_ITEMS,
            "record page exceeds the count limit"
        );
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = after.map_or(prefix, |after| after.max(prefix));
        let exclusive = after == Some(start);
        let key_prefix = [self.prefix.as_slice(), prefix].concat();
        let full_start = [self.prefix.as_slice(), start].concat();
        let key_end = prefix_end(&key_prefix);
        let mut disk = entries(&self.view, &full_start, &key_end, true)
            .map(|entry| {
                let (key, value) = entry?;
                Ok::<_, anyhow::Error>((key[self.prefix.len()..].to_vec(), value.to_vec()))
            })
            .peekable();
        if exclusive && matches!(disk.peek(), Some(Ok((key, _))) if key == start) {
            disk.next();
        }

        let bound = if exclusive {
            Excluded(start)
        } else {
            Included(start)
        };
        let mut staged = self
            .pending
            .range::<[u8], _>((bound, Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .peekable();
        let mut page = Vec::new();
        let mut bytes = 0;
        while page.len() < limit {
            let pair = match (disk.peek(), staged.peek()) {
                (None, None) => break,
                (Some(Err(_)), _) => disk.next().transpose()?,
                (Some(Ok((key, _))), Some((pending, _))) if key < *pending => {
                    disk.next().transpose()?
                }
                (Some(Ok((key, _))), Some((pending, _))) if key == *pending => {
                    disk.next();
                    staged
                        .next()
                        .and_then(|(key, value)| Some((key.clone(), value.clone()?)))
                }
                (_, Some(_)) => staged
                    .next()
                    .and_then(|(key, value)| Some((key.clone(), value.clone()?))),
                (Some(_), None) => disk.next().transpose()?,
            };
            if let Some((key, value)) = pair {
                bytes += key.len() + value.len();
                if bytes > MAX_SCAN_BYTES {
                    break;
                }
                page.push((key, value));
            }
        }
        Ok(page)
    }

    pub(super) fn commit(self) -> Result<()> {
        let mut write = WriteBatch::new();
        for (key, value) in self.pending {
            let key = [self.prefix.as_slice(), &key].concat();
            match value {
                Some(value) => write.put(&key, &value),
                None => write.delete(&key),
            }
        }
        self.store.write_header(&mut write, &self.header)?;
        self.store.db.write(write)
    }
}
