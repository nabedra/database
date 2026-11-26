//! A file of fixed-size records on 4 KiB pages, written with `O_DIRECT`.
//!
//! Records are packed: record `i` holds bytes `[i·r, (i+1)·r)`, so a read takes the one to few
//! pages around it in one request. New records append. A record is reused only after `release`,
//! which the row map calls when the LocalIndex slot rule lets the version go. Pages that changed
//! since the last flush stay in RAM, and so does the last page while records still fill it, so
//! appends write each page about once.
//!
//! Every page on disk has a CRC32C, kept in RAM: the file lives as long as the process, because
//! open empties it and recovery writes every vector again (decision L4). A read checks every page
//! it uses, so a torn, misdirected or damaged page, a short read or an I/O error is an error and
//! never a record. A page that changed while it was read is read again.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crc_fast::CrcAlgorithm;

use super::disk::{Disk, Req};
use super::pages::{PAGE, Pages};
use crate::types::{Error, Result};

/// CRC32C (Castagnoli), as `checkpoint` and the `'x'` records use.
pub fn crc32c(data: &[u8]) -> u32 {
    // A 32-bit CRC is returned in the low bits.
    crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, data) as u32
}

const PAGE64: u64 = PAGE as u64;
/// Reads of a page that keeps changing.
const RETRIES: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct FileStats {
    pub records: usize,
    pub free: usize,
    pub pages_on_disk: u64,
    pub dirty_pages: usize,
    pub page_reads: u64,
    pub page_writes: u64,
}

struct State<K> {
    map: HashMap<K, u64>,
    /// Released records, reused last in, first out.
    free: Vec<u64>,
    /// Records ever placed.
    records: u64,
    /// The checksum of every page on disk.
    crcs: Vec<u32>,
    dirty: BTreeMap<u64, Box<[u8]>>,
    page_reads: u64,
    page_writes: u64,
}

pub struct RecordFile<K> {
    name: String,
    disk: Arc<dyn Disk>,
    rec: u64,
    /// Dirty pages that start a flush.
    flush_pages: usize,
    state: Mutex<State<K>>,
}

impl<K> std::fmt::Debug for RecordFile<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordFile({}, {:?})", self.name, self.disk)
    }
}

fn damaged(name: &str, page: u64, why: &str) -> Error {
    Error::Store(format!("{name}: page {page}: {why}"))
}

/// Copies the bytes that a page and a record share.
fn overlap(page: u64, data: &[u8], start: u64, out: &mut [u8]) {
    let (p0, end) = (page * PAGE64, start + out.len() as u64);
    let (lo, hi) = (start.max(p0), end.min(p0 + PAGE64));
    if lo < hi {
        let (a, b) = ((lo - start) as usize, (hi - start) as usize);
        out[a..b].copy_from_slice(&data[(lo - p0) as usize..(hi - p0) as usize]);
    }
}

impl<K: Copy + Eq + Hash> RecordFile<K> {
    /// `disk` is an empty file. `record` bytes per record, at least one.
    pub fn new(name: impl Into<String>, disk: Arc<dyn Disk>, record: usize) -> Result<Self> {
        if record == 0 {
            return Err(Error::Internal("a record needs a byte"));
        }
        Ok(RecordFile {
            name: name.into(),
            disk,
            rec: record as u64,
            flush_pages: 16,
            state: Mutex::new(State {
                map: HashMap::new(),
                free: Vec::new(),
                records: 0,
                crcs: Vec::new(),
                dirty: BTreeMap::new(),
                page_reads: 0,
                page_writes: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State<K>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn backend(&self) -> &'static str {
        self.disk.kind()
    }

    /// The bytes of record `i`, and its pages.
    fn span(&self, i: u64) -> Result<(u64, u64, u64)> {
        let start = i.checked_mul(self.rec).ok_or(Error::Internal("file too large"))?;
        let end = start + self.rec;
        Ok((start, start / PAGE64, (end - 1) / PAGE64))
    }

    /// Puts a page into the dirty set, read from disk if it is there.
    fn dirty(&self, st: &mut State<K>, page: u64) -> Result<()> {
        if st.dirty.contains_key(&page) {
            return Ok(());
        }
        let mut data = vec![0u8; PAGE].into_boxed_slice();
        if let Some(&crc) = st.crcs.get(page as usize) {
            let mut req = [Req::new(page * PAGE64, 1)];
            self.disk.read(&mut req);
            st.page_reads += 1;
            let [req] = req;
            req.result
                .map_err(|e| damaged(&self.name, page, &e.to_string()))?;
            if crc32c(req.buf.bytes()) != crc {
                return Err(damaged(&self.name, page, "checksum"));
            }
            data.copy_from_slice(req.buf.bytes());
        }
        st.dirty.insert(page, data);
        Ok(())
    }

    /// Writes dirty pages in runs. With `all` false, the last page stays while records still
    /// fill it. A failed write keeps the pages dirty, so nothing that was stored is lost.
    fn flush_locked(&self, st: &mut State<K>, all: bool) -> Result<()> {
        let end = st.records * self.rec;
        let tail = (!all && !end.is_multiple_of(PAGE64)).then_some(end / PAGE64);
        let pages: Vec<u64> = st.dirty.keys().copied().filter(|p| Some(*p) != tail).collect();
        for run in pages.chunk_by(|a, b| a + 1 == *b) {
            let mut buf = Pages::new(run.len());
            for (i, p) in run.iter().enumerate() {
                let page = &st.dirty[p];
                buf.bytes_mut()[i * PAGE..(i + 1) * PAGE].copy_from_slice(page);
            }
            let first = run[0];
            self.disk
                .write(first * PAGE64, buf.bytes())
                .map_err(|e| damaged(&self.name, first, &format!("write: {e}")))?;
            st.page_writes += run.len() as u64;
            for (i, p) in run.iter().enumerate() {
                let crc = crc32c(buf.page(i));
                let p = *p as usize;
                if st.crcs.len() <= p {
                    // Pages never written read as zeroes.
                    let zero = crc32c(&[0u8; PAGE]);
                    st.crcs.resize(p + 1, zero);
                }
                st.crcs[p] = crc;
                st.dirty.remove(&(p as u64));
            }
        }
        Ok(())
    }

    /// Writes every dirty page.
    pub fn flush(&self) -> Result<()> {
        let mut st = self.lock();
        self.flush_locked(&mut st, true)
    }

    /// Stores a record for `key`. A key that is stored already is refused. An error stores
    /// nothing.
    pub fn put(&self, key: K, data: &[u8]) -> Result<()> {
        if data.len() as u64 != self.rec {
            return Err(Error::Internal("a record of the wrong size"));
        }
        let mut st = self.lock();
        if st.map.contains_key(&key) {
            return Err(Error::Internal("a stored version is stored again"));
        }
        let (i, new) = match st.free.pop() {
            Some(i) => (i, false),
            None => (st.records, true),
        };
        let undo = |st: &mut State<K>| {
            if !new {
                st.free.push(i);
            }
        };
        let (start, first, last) = match self.span(i) {
            Ok(s) => s,
            Err(e) => {
                undo(&mut st);
                return Err(e);
            }
        };
        for p in first..=last {
            if let Err(e) = self.dirty(&mut st, p) {
                undo(&mut st);
                return Err(e);
            }
        }
        for p in first..=last {
            let page = st.dirty.get_mut(&p).ok_or(Error::Internal("page"))?;
            let (p0, end) = (p * PAGE64, start + self.rec);
            let (lo, hi) = (start.max(p0), end.min(p0 + PAGE64));
            page[(lo - p0) as usize..(hi - p0) as usize]
                .copy_from_slice(&data[(lo - start) as usize..(hi - start) as usize]);
        }
        if new {
            st.records += 1;
        }
        st.map.insert(key, i);
        if st.dirty.len() >= self.flush_pages
            && let Err(e) = self.flush_locked(&mut st, false)
        {
            st.map.remove(&key);
            if new {
                st.records -= 1;
            } else {
                st.free.push(i);
            }
            return Err(e);
        }
        Ok(())
    }

    /// The record goes, and its place may be reused. Returns whether it was there.
    pub fn release(&self, key: &K) -> bool {
        let mut st = self.lock();
        match st.map.remove(key) {
            Some(i) => {
                st.free.push(i);
                true
            }
            None => false,
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.lock().map.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Result<Vec<u8>> {
        self.get_many(std::slice::from_ref(key))
            .pop()
            .unwrap_or(Err(Error::Internal("no answer")))
    }

    /// Reads records in one batch of I/O. One result per key, in order.
    pub fn get_many(&self, keys: &[K]) -> Vec<Result<Vec<u8>>> {
        let mut out: Vec<Option<Result<Vec<u8>>>> = keys.iter().map(|_| None).collect();
        for _ in 0..RETRIES {
            if out.iter().all(Option::is_some) {
                break;
            }
            self.read_round(keys, &mut out);
        }
        let changing = || Err(Error::Store(format!("{}: a page kept changing", self.name)));
        out.into_iter().map(|r| r.unwrap_or_else(changing)).collect()
    }

    fn read_round(&self, keys: &[K], out: &mut [Option<Result<Vec<u8>>>]) {
        struct Plan {
            key: usize,
            record: u64,
            start: u64,
            buf: Vec<u8>,
            /// The request, its first page and the checksums it expects (`None`: was dirty).
            req: Option<(usize, u64, Vec<Option<u32>>)>,
        }
        let mut plans = Vec::new();
        let mut reqs = Vec::new();
        {
            let st = self.lock();
            for (k, key) in keys.iter().enumerate() {
                if out[k].is_some() {
                    continue;
                }
                let Some(&i) = st.map.get(key) else {
                    out[k] = Some(Err(Error::Internal("no stored version")));
                    continue;
                };
                let Ok((start, first, last)) = self.span(i) else {
                    out[k] = Some(Err(Error::Internal("file too large")));
                    continue;
                };
                let mut buf = vec![0u8; self.rec as usize];
                let mut expect = Vec::new();
                let on_disk = st.crcs.len() as u64;
                for p in first..=last {
                    match st.dirty.get(&p) {
                        Some(page) => {
                            overlap(p, page, start, &mut buf);
                            expect.push(None);
                        }
                        None if p < on_disk => expect.push(Some(st.crcs[p as usize])),
                        None => break,
                    }
                }
                if (expect.len() as u64) < last - first + 1 {
                    out[k] = Some(Err(damaged(&self.name, first, "never written")));
                    continue;
                }
                let req = expect.iter().any(Option::is_some).then(|| {
                    reqs.push(Req::new(first * PAGE64, expect.len()));
                    (reqs.len() - 1, first, expect)
                });
                plans.push(Plan {
                    key: k,
                    record: i,
                    start,
                    buf,
                    req,
                });
            }
        }
        if !reqs.is_empty() {
            self.disk.read(&mut reqs);
        }
        let mut st = self.lock();
        st.page_reads += reqs.iter().map(|r| (r.buf.len() / PAGE) as u64).sum::<u64>();
        for mut plan in plans {
            if st.map.get(&keys[plan.key]) != Some(&plan.record) {
                out[plan.key] = Some(Err(Error::Internal("the version was released")));
                continue;
            }
            let Some((r, first, expect)) = plan.req.take() else {
                out[plan.key] = Some(Ok(plan.buf));
                continue;
            };
            let req = &reqs[r];
            let mut bad = None;
            let mut changed = false;
            for (j, crc) in expect.iter().enumerate() {
                let Some(crc) = crc else {
                    continue;
                };
                let p = first + j as u64;
                let now = st.crcs.get(p as usize).filter(|_| !st.dirty.contains_key(&p));
                if now != Some(crc) {
                    changed = true;
                    break;
                }
                let why = match &req.result {
                    Err(e) => Some(e.to_string()),
                    Ok(()) if crc32c(req.buf.page(j)) != *crc => Some("checksum".to_owned()),
                    Ok(()) => None,
                };
                if let Some(why) = why {
                    bad.get_or_insert((p, why));
                    continue;
                }
                overlap(p, req.buf.page(j), plan.start, &mut plan.buf);
            }
            if changed {
                continue;
            }
            out[plan.key] = Some(match bad {
                Some((p, why)) => Err(damaged(&self.name, p, &why)),
                None => Ok(plan.buf),
            });
        }
    }

    pub fn stats(&self) -> FileStats {
        let st = self.lock();
        FileStats {
            records: st.map.len(),
            free: st.free.len(),
            pages_on_disk: st.crcs.len() as u64,
            dirty_pages: st.dirty.len(),
            page_reads: st.page_reads,
            page_writes: st.page_writes,
        }
    }

    /// The record index of a key, for tests of reuse.
    #[cfg(test)]
    pub fn record_of(&self, key: &K) -> Option<u64> {
        self.lock().map.get(key).copied()
    }

    /// Keys whose records share a page with this one's, for fault tests.
    #[cfg(test)]
    pub fn pages_of(&self, key: &K) -> Option<std::collections::BTreeSet<u64>> {
        let i = self.record_of(key)?;
        let (_, first, last) = self.span(i).ok()?;
        Some((first..=last).collect())
    }
}
