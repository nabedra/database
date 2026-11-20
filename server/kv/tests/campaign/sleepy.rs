//! NS17: an `Fs` that sleeps at random in the engine's background threads (`kv-journal`,
//! `kv-journal-prep`, `kv-flush`, `kv-compact`), so flush, compaction, journal GC,
//! retirement and group commit interleave differently with the caller on every run. The
//! caller's own thread never sleeps here.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nabedra_kv::fs::{FaultFs, Fs, FsFile, FsLock, OpenMode};

use crate::common::Rng;

pub struct Sleepy {
    pub inner: FaultFs,
    rng: Mutex<Rng>,
    /// Percent of background calls that sleep (0 to 2 ms).
    percent: u64,
    pub naps: AtomicU64,
}

impl Sleepy {
    pub fn new(inner: FaultFs, seed: u64, percent: u64) -> Arc<Sleepy> {
        Arc::new(Sleepy {
            inner,
            rng: Mutex::new(Rng(seed)),
            percent,
            naps: AtomicU64::new(0),
        })
    }

    fn nap(&self) {
        let background = std::thread::current()
            .name()
            .is_some_and(|n| n.starts_with("kv-"));
        if !background {
            return;
        }
        let us = {
            let mut rng = self.rng.lock().unwrap_or_else(|p| p.into_inner());
            if rng.chance(self.percent) {
                rng.below(2000)
            } else {
                0
            }
        };
        if us > 0 {
            self.naps.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_micros(us));
        }
    }
}

pub struct SleepyFs(pub Arc<Sleepy>);

struct SleepyFile {
    inner: Arc<dyn FsFile>,
    fs: Arc<Sleepy>,
}

impl Fs for SleepyFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        self.0.nap();
        let inner = self.0.inner.open(path, mode)?;
        Ok(Arc::new(SleepyFile {
            inner,
            fs: self.0.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.0.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.0.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.0.inner.list(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.0.nap();
        self.0.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        self.0.inner.lock(path)
    }
}

impl FsFile for SleepyFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.fs.nap();
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.fs.nap();
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.fs.nap();
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.fs.nap();
        self.inner.sync_all()
    }
    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.fs.nap();
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn advise_dont_need(&self, offset: u64, len: u64) {
        self.inner.advise_dont_need(offset, len)
    }
}
