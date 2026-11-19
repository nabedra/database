//! The descriptor cache: at most `max_open_runs` open run files, least recently used
//! evicted first. Reopening an evicted run is transparent.

use std::collections::hash_map::RandomState;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cache::Lru;
use crate::error::Result;
use crate::fs::{Fs, OpenMode};
use crate::run::reader::{Run, RunReadContext};

/// Default `max_open_runs`, the same as RocksDB's `max_open_files` today.
pub const DEFAULT_MAX_OPEN_RUNS: usize = 128;

/// `<id>.run`.
pub fn run_file_name(id: u64) -> String {
    format!("{id}.run")
}

/// The id of a run file name, or `None` for any other name.
pub fn parse_run_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".run")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let id: u64 = digits.parse().ok()?;
    (run_file_name(id) == name).then_some(id)
}

/// Opens runs of one store on demand and keeps the most recently used ones open.
///
/// A run evicted here closes once the last [`Arc<Run>`] (for example a live iterator)
/// drops, so while iterators hold evicted runs the number of open descriptors can
/// briefly exceed `max_open_runs`. Opening happens outside the lock.
pub struct TableCache {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    ctx: RunReadContext,
    max_open: usize,
    open: Mutex<Lru<u64, Arc<Run>, RandomState>>,
    opens: AtomicU64,
}

impl TableCache {
    /// Runs live in `dir` as [`run_file_name`]s.
    pub fn new(fs: Arc<dyn Fs>, dir: &Path, ctx: RunReadContext, max_open_runs: usize) -> Self {
        TableCache {
            fs,
            dir: dir.to_path_buf(),
            ctx,
            max_open: max_open_runs.max(1),
            open: Mutex::new(Lru::with_hasher(RandomState::new())),
            opens: AtomicU64::new(0),
        }
    }

    pub fn context(&self) -> &RunReadContext {
        &self.ctx
    }

    pub fn path(&self, id: u64) -> PathBuf {
        self.dir.join(run_file_name(id))
    }

    /// The open run `id`, opening (and verifying the footer of) its file if needed.
    pub fn get(&self, id: u64) -> Result<Arc<Run>> {
        if let Some(run) = lock(&self.open).get(&id) {
            return Ok(Arc::clone(run));
        }
        let file = self.fs.open(&self.path(id), OpenMode::Read)?;
        let run = Run::open(file, id, self.ctx.clone())?;
        self.opens.fetch_add(1, Ordering::Relaxed);
        let mut open = lock(&self.open);
        if let Some(existing) = open.get(&id) {
            // Another reader opened it meanwhile; keep one descriptor.
            return Ok(Arc::clone(existing));
        }
        open.insert(id, Arc::clone(&run), 1);
        while open.len() > self.max_open {
            if open.evict(usize::MAX, |_| true).is_none() {
                break;
            }
        }
        Ok(run)
    }

    /// Closes `id` (for example once the run is deleted). Its cached blocks age out of
    /// the block cache.
    pub fn evict(&self, id: u64) {
        lock(&self.open).remove(&id);
    }

    /// Runs currently held open by this cache.
    pub fn open_runs(&self) -> usize {
        lock(&self.open).len()
    }

    /// Files opened so far, reopenings included.
    pub fn opens(&self) -> u64 {
        self.opens.load(Ordering::Relaxed)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
