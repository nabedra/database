//! Raw vectors by row version: in RAM, or on NVMe in one file per core.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use nabedra_rt::fs::os;

use super::disk::{self, Backend, Disk};
use super::file::{FileStats, RecordFile};
use crate::types::{Error, Pos, Result};

/// A row version that has an entry: its slot in the row map and its position.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct VecKey {
    pub slot: u32,
    pub at: Pos,
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Stats {
    pub vectors: usize,
    /// Bytes of vectors held in RAM, dirty pages included.
    pub ram_bytes: u64,
    pub disk_bytes: u64,
    pub page_reads: u64,
    pub page_writes: u64,
}

/// Where the raw vectors of an index live. The row map puts a version's vector when it adds the
/// version, reads it for codes, copies and exact scores, and releases it when the LocalIndex slot
/// rule lets the version go: no entry, no copy in flight, no queued append and no checkpoint
/// names it.
pub trait Vectors: Send + Sync + fmt::Debug {
    /// A key that is stored already is refused. An error stores nothing.
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()>;
    /// Damage is an error, never a vector.
    fn get(&self, key: VecKey) -> Result<Arc<[f32]>> {
        self.get_many(&[key])
            .pop()
            .unwrap_or(Err(Error::Internal("no answer")))
    }
    /// One result per key, in order, in one batch of I/O.
    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>>;
    fn release(&self, key: VecKey);
    /// Whether a read costs I/O.
    fn on_disk(&self) -> bool;
    fn stats(&self) -> Stats;
}

/// Raw vectors in RAM, as an index without a store keeps them in its rows.
#[derive(Debug, Default)]
pub struct RamVectors {
    map: Mutex<HashMap<VecKey, Arc<[f32]>>>,
}

impl RamVectors {
    pub fn new() -> RamVectors {
        RamVectors::default()
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<VecKey, Arc<[f32]>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Vectors for RamVectors {
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()> {
        let mut map = self.map();
        if map.contains_key(&key) {
            return Err(Error::Internal("a stored version is stored again"));
        }
        map.insert(key, vec.into());
        Ok(())
    }

    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>> {
        let map = self.map();
        let get = |k| map.get(k).cloned().ok_or(Error::Internal("no stored version"));
        keys.iter().map(get).collect()
    }

    fn release(&self, key: VecKey) {
        self.map().remove(&key);
    }

    fn on_disk(&self) -> bool {
        false
    }

    fn stats(&self) -> Stats {
        let map = self.map();
        let bytes = map.values().map(|v| v.len() as u64 * 4).sum();
        Stats {
            vectors: map.len(),
            ram_bytes: bytes,
            ..Stats::default()
        }
    }
}

/// Raw vectors on NVMe: one file of `dim · 4`-byte records per core, written by that core. A
/// version's vector lives in the file of `slot % cores`.
#[derive(Debug)]
pub struct NvmeVectors {
    dim: usize,
    files: Vec<RecordFile<VecKey>>,
}

impl NvmeVectors {
    /// Empties or creates `dir/raw-<core>` for each core. `threads` pread threads per file
    /// where io_uring is not used.
    pub fn open(dir: &Path, cores: usize, dim: usize, backend: Backend, threads: usize) -> Result<Self> {
        let io = |e: std::io::Error| Error::Store(format!("{}: {e}", dir.display()));
        os::create_dir_all(dir).map_err(io)?;
        let disks = (0..cores)
            .map(|c| disk::open(&dir.join(format!("raw-{c}")), backend, threads).map_err(io))
            .collect::<Result<Vec<_>>>()?;
        NvmeVectors::on(disks, dim)
    }

    /// Over empty files, one per core.
    pub fn on(disks: Vec<Arc<dyn Disk>>, dim: usize) -> Result<Self> {
        if disks.is_empty() || dim == 0 {
            return Err(Error::Internal("a vector store needs a core and a dimension"));
        }
        let record = dim.checked_mul(4).ok_or(Error::Internal("dimension"))?;
        let files = disks
            .into_iter()
            .enumerate()
            .map(|(c, d)| RecordFile::new(format!("raw-{c}"), d, record))
            .collect::<Result<Vec<_>>>()?;
        Ok(NvmeVectors { dim, files })
    }

    /// `"io_uring"` or `"pread"`.
    pub fn backend(&self) -> &'static str {
        self.files[0].backend()
    }

    fn file(&self, key: VecKey) -> &RecordFile<VecKey> {
        &self.files[key.slot as usize % self.files.len()]
    }

    /// Writes every page held in RAM.
    pub fn flush(&self) -> Result<()> {
        self.files.iter().try_for_each(RecordFile::flush)
    }

    pub fn file_stats(&self) -> Vec<FileStats> {
        self.files.iter().map(RecordFile::stats).collect()
    }

    #[cfg(test)]
    pub(crate) fn files(&self) -> &[RecordFile<VecKey>] {
        &self.files
    }

    fn decode(&self, bytes: &[u8]) -> Result<Arc<[f32]>> {
        let word = |c: &[u8]| f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let v: Arc<[f32]> = bytes.chunks_exact(4).map(word).collect();
        // The index stores only finite vectors, so anything else is damage the checksum missed.
        if v.len() != self.dim || v.iter().any(|x| !x.is_finite()) {
            return Err(Error::Store("a stored vector is not finite".into()));
        }
        Ok(v)
    }
}

impl Vectors for NvmeVectors {
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()> {
        if vec.len() != self.dim {
            return Err(Error::Internal("a vector of other dimensions"));
        }
        let bytes: Vec<u8> = vec.iter().flat_map(|x| x.to_le_bytes()).collect();
        self.file(key).put(key, &bytes)
    }

    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>> {
        let mut out: Vec<Option<Result<Arc<[f32]>>>> = keys.iter().map(|_| None).collect();
        for (c, file) in self.files.iter().enumerate() {
            let mine: Vec<usize> = (0..keys.len())
                .filter(|&i| keys[i].slot as usize % self.files.len() == c)
                .collect();
            if mine.is_empty() {
                continue;
            }
            let ks: Vec<VecKey> = mine.iter().map(|&i| keys[i]).collect();
            for (i, r) in mine.into_iter().zip(file.get_many(&ks)) {
                out[i] = Some(r.and_then(|b| self.decode(&b)));
            }
        }
        out.into_iter()
            .map(|r| r.unwrap_or(Err(Error::Internal("no answer"))))
            .collect()
    }

    fn release(&self, key: VecKey) {
        self.file(key).release(&key);
    }

    fn on_disk(&self) -> bool {
        true
    }

    fn stats(&self) -> Stats {
        let mut s = Stats::default();
        for f in self.files.iter().map(RecordFile::stats) {
            s.vectors += f.records;
            s.ram_bytes += f.dirty_pages as u64 * 4096;
            s.disk_bytes += f.pages_on_disk * 4096;
            s.page_reads += f.page_reads;
            s.page_writes += f.page_writes;
        }
        s
    }
}
