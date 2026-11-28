//! 4-bit code extensions: in RAM while the worker's code memory lasts, otherwise on NVMe. 1-bit
//! codes are charged to the same [`Memory`] and always stay in RAM, so they come first and
//! extensions take what is left: when codes grow, [`Extensions::demote`] moves extensions to NVMe
//! until both fit again, and [`Extensions::promote`] brings them back to RAM, which the index's
//! maintenance does once memory is free again.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use super::disk::Disk;
use super::file::RecordFile;
use crate::runtime::{Memory, Tier};
use crate::types::{Error, Result};

/// The extension of one entry instance of a partition.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ExtKey {
    pub part: u32,
    pub id: u64,
}

#[derive(Debug)]
pub struct Extensions {
    bytes: usize,
    memory: Arc<Memory>,
    leased: AtomicBool,
    ram: Mutex<BTreeMap<ExtKey, Arc<[u8]>>>,
    /// One per core; the key's partition picks it.
    files: Vec<RecordFile<ExtKey>>,
}

/// Keeps one index's extension keys exclusive until the index has cleaned them up.
pub(crate) struct ExtensionLease(Arc<Extensions>);

impl Drop for ExtensionLease {
    fn drop(&mut self) {
        self.0.leased.store(false, Ordering::Release);
    }
}

impl Extensions {
    /// Extensions of `bytes` each, over empty files, one per core.
    pub fn new(bytes: usize, memory: Arc<Memory>, disks: Vec<Arc<dyn Disk>>) -> Result<Self> {
        if disks.is_empty() {
            return Err(Error::Internal("extensions need a core"));
        }
        let files = disks
            .into_iter()
            .enumerate()
            .map(|(c, d)| RecordFile::new(format!("ext-{c}"), d, bytes))
            .collect::<Result<Vec<_>>>()?;
        Ok(Extensions {
            bytes,
            memory,
            leased: AtomicBool::new(false),
            ram: Mutex::new(BTreeMap::new()),
            files,
        })
    }

    pub(crate) fn lease(self: &Arc<Self>) -> Result<ExtensionLease> {
        self.leased
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| Error::Busy)?;
        Ok(ExtensionLease(Arc::clone(self)))
    }

    /// Bytes of one extension.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The code memory that extensions in RAM, and the 1-bit codes, are charged to.
    pub fn memory(&self) -> &Arc<Memory> {
        &self.memory
    }

    fn ram(&self) -> std::sync::MutexGuard<'_, BTreeMap<ExtKey, Arc<[u8]>>> {
        self.ram.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn file(&self, key: &ExtKey) -> &RecordFile<ExtKey> {
        &self.files[key.part as usize % self.files.len()]
    }

    /// Stores an extension and says where it went.
    pub fn put(&self, key: ExtKey, ext: &[u8]) -> Result<Tier> {
        if ext.len() != self.bytes {
            return Err(Error::Internal("an extension of the wrong size"));
        }
        const AGAIN: Error = Error::Internal("a stored extension is stored again");
        // The check and the insert into RAM are one hold of the map, so that racing puts of a
        // key cannot both take RAM and charge the memory twice.
        let mut ram = self.ram();
        if ram.contains_key(&key) || self.file(&key).contains(&key) {
            return Err(AGAIN);
        }
        if self.memory.try_extension(self.bytes as u64) {
            ram.insert(key, ext.into());
            return Ok(Tier::Ram);
        }
        // The file refuses a key it holds. A racing put that took RAM meanwhile wins.
        drop(ram);
        self.file(&key).put(key, ext)?;
        if self.ram().contains_key(&key) {
            self.file(&key).release(&key);
            return Err(AGAIN);
        }
        Ok(Tier::Nvme)
    }

    pub fn get(&self, key: &ExtKey) -> Result<Arc<[u8]>> {
        if let Some(ext) = self.ram().get(key) {
            return Ok(ext.clone());
        }
        self.file(key).get(key).map(Arc::from)
    }

    pub fn tier(&self, key: &ExtKey) -> Option<Tier> {
        if self.ram().contains_key(key) {
            return Some(Tier::Ram);
        }
        self.file(key).contains(key).then_some(Tier::Nvme)
    }

    /// Brings an extension on NVMe back to RAM if the code memory has room for it. Returns whether
    /// it moved. A record that cannot be read stays where it is, and the error says why.
    pub fn promote(&self, key: &ExtKey) -> Result<bool> {
        if self.ram().contains_key(key) || !self.file(key).contains(key) {
            return Ok(false);
        }
        if !self.memory.try_extension(self.bytes as u64) {
            return Ok(false);
        }
        match self.file(key).get(key) {
            Ok(ext) => {
                self.ram().insert(*key, ext.into());
                self.file(key).release(key);
                Ok(true)
            }
            Err(e) => {
                self.memory.release_extension(self.bytes as u64);
                Err(e)
            }
        }
    }

    /// Moves extensions from RAM to NVMe, smallest key first, while the code memory holds more than
    /// its limit: 1-bit codes that grew take their room back. Returns how many moved. One that
    /// cannot be written stays in RAM, and the error says why.
    pub fn demote(&self) -> Result<usize> {
        let mut moved = 0;
        loop {
            let (codes, ext) = self.memory.used();
            if ext == 0 || codes.saturating_add(ext) <= self.memory.limit() {
                return Ok(moved);
            }
            let Some((key, value)) = self.ram().first_key_value().map(|(k, v)| (*k, v.clone()))
            else {
                return Ok(moved);
            };
            self.file(&key).put(key, &value)?;
            self.ram().remove(&key);
            self.memory.release_extension(self.bytes as u64);
            moved += 1;
        }
    }

    /// Whether the code memory has room for one more extension.
    pub fn room(&self) -> bool {
        let (codes, ext) = self.memory.used();
        codes.saturating_add(ext).saturating_add(self.bytes as u64) <= self.memory.limit()
    }

    /// The entry went: its RAM goes back to the budget, or its record may be reused.
    pub fn release(&self, key: &ExtKey) {
        if self.ram().remove(key).is_some() {
            self.memory.release_extension(self.bytes as u64);
        } else {
            self.file(key).release(key);
        }
    }

    /// Extensions held in RAM and on NVMe.
    pub fn counts(&self) -> (usize, usize) {
        let nvme = self.files.iter().map(|f| f.stats().records).sum();
        (self.ram().len(), nvme)
    }

    pub fn flush(&self) -> Result<()> {
        self.files.iter().try_for_each(RecordFile::flush)
    }
}
