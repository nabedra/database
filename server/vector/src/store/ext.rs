//! 4-bit code extensions: in RAM while the worker's code memory lasts, otherwise on NVMe. 1-bit
//! codes are charged to the same [`Memory`] and always stay in RAM, so they come first and
//! extensions take what is left. An extension stays where it was put.

use std::collections::HashMap;
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
    ram: Mutex<HashMap<ExtKey, Arc<[u8]>>>,
    /// One per core; the key's partition picks it.
    files: Vec<RecordFile<ExtKey>>,
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
            ram: Mutex::new(HashMap::new()),
            files,
        })
    }

    fn ram(&self) -> std::sync::MutexGuard<'_, HashMap<ExtKey, Arc<[u8]>>> {
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
        if self.ram().contains_key(&key) || self.file(&key).contains(&key) {
            return Err(Error::Internal("a stored extension is stored again"));
        }
        if self.memory.try_extension(self.bytes as u64) {
            self.ram().insert(key, ext.into());
            return Ok(Tier::Ram);
        }
        self.file(&key).put(key, ext)?;
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

    /// The entry went: its RAM goes back to the budget, or its record may be reused.
    pub fn release(&self, key: &ExtKey) {
        if self.ram().remove(key).is_some() {
            self.memory.release_extension(self.bytes as u64);
        } else {
            self.file(key).release(key);
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.files.iter().try_for_each(RecordFile::flush)
    }
}
