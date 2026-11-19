//! Reads a run: point lookups, iterators and range tombstones.

use std::cmp::Ordering;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use bytes::Bytes;

use crate::cache::{BlockCache, CacheKey};
use crate::error::{Error, FileId, Result};
use crate::fs::FsFile;
use crate::run::block::{BlockIter, Target};
use crate::run::bloom;
use crate::run::format::{BLOCK_TRAILER_LEN, BlockHandle, FOOTER_LEN, Footer, RunMeta};
use crate::source::{InternalIterator, RangeTombstone};
use crate::types::{Kind, MAX_SEQ, SEEK_KIND_BYTE, Seq, parse_internal_key};

/// Read-side counters shared by every run of a store.
#[derive(Debug, Default)]
pub struct RunStats {
    corruption_detected: AtomicU64,
    block_reads: AtomicU64,
    block_read_bytes: AtomicU64,
    bloom_checks: AtomicU64,
    bloom_negatives: AtomicU64,
}

impl RunStats {
    /// Checksum or structure failures found while reading runs.
    pub fn corruption_detected(&self) -> u64 {
        self.corruption_detected.load(AtomicOrdering::Relaxed)
    }

    /// Blocks read from the file (cache misses).
    pub fn block_reads(&self) -> u64 {
        self.block_reads.load(AtomicOrdering::Relaxed)
    }

    pub fn block_read_bytes(&self) -> u64 {
        self.block_read_bytes.load(AtomicOrdering::Relaxed)
    }

    pub fn bloom_checks(&self) -> u64 {
        self.bloom_checks.load(AtomicOrdering::Relaxed)
    }

    /// Lookups the bloom filter answered without reading a data block.
    pub fn bloom_negatives(&self) -> u64 {
        self.bloom_negatives.load(AtomicOrdering::Relaxed)
    }

    fn bump(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, AtomicOrdering::Relaxed);
    }
}

/// What every run of one store reads through: the worker-shared block cache, the
/// store's cache namespace and its counters.
#[derive(Clone, Debug)]
pub struct RunReadContext {
    pub cache: Arc<BlockCache>,
    pub namespace: u64,
    pub stats: Arc<RunStats>,
}

impl RunReadContext {
    /// A context with a fresh namespace in `cache`.
    pub fn new(cache: Arc<BlockCache>) -> RunReadContext {
        let namespace = cache.new_namespace();
        RunReadContext {
            cache,
            namespace,
            stats: Arc::new(RunStats::default()),
        }
    }
}

/// The newest version of a user key at or below a snapshot, within one run. Range
/// tombstones are not applied: the caller combines them across sources (see
/// [`Run::max_covering_tombstone_seq`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// A `Put` (the value) or a `JournalPut` (an encoded locator).
    Found {
        kind: Kind,
        value: Bytes,
        seq: Seq,
    },
    Deleted {
        seq: Seq,
    },
    NotFound,
}

/// An open, immutable run. Cheap to share; iterators keep it (and its descriptor) alive.
pub struct Run {
    id: u64,
    file: Arc<dyn FsFile>,
    ctx: RunReadContext,
    footer: Footer,
    meta: RunMeta,
    /// Sorted by start, then sequence descending.
    tombstones: Vec<RangeTombstone>,
    /// `reach[i]` is the index of the tombstone with the largest end among `0..=i`, so a
    /// backwards scan can stop once nothing earlier reaches the key.
    reach: Vec<u32>,
    /// Data blocks end where the range-delete block starts.
    data_end: u64,
    /// Every block ends at or before the footer.
    footer_at: u64,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

impl Run {
    /// Opens a run: checks the footer, then loads the properties and range tombstones.
    /// Index and filter blocks are read lazily through the block cache.
    pub fn open(file: Arc<dyn FsFile>, id: u64, ctx: RunReadContext) -> Result<Arc<Run>> {
        let corruption = |offset: u64, what: &'static str| {
            RunStats::bump(&ctx.stats.corruption_detected, 1);
            Error::Corruption {
                file: FileId::Run { id },
                offset,
                what,
            }
        };
        let file_size = file.len()?;
        let Some(footer_at) = file_size.checked_sub(FOOTER_LEN as u64) else {
            return Err(corruption(0, "run shorter than its footer"));
        };
        let mut footer = [0u8; FOOTER_LEN];
        read_at(&*file, &mut footer, footer_at)
            .map_err(|e| map_read_error(e, || corruption(footer_at, "run truncated")))?;
        let footer = Footer::decode(&footer).map_err(|what| corruption(footer_at, what))?;
        // The blocks after the data are laid out in a fixed order; anything else is damage.
        let laid_out = [footer.range_del, footer.bloom, footer.props, footer.index]
            .windows(2)
            .all(|pair| pair[0].end() == Some(pair[1].offset))
            && footer.index.end() == Some(footer_at);
        if !laid_out {
            return Err(corruption(footer_at, "run footer handles out of place"));
        }
        let mut run = Run {
            id,
            file,
            ctx,
            footer,
            meta: empty_meta(id),
            tombstones: Vec::new(),
            reach: Vec::new(),
            data_end: footer.range_del.offset,
            footer_at,
        };
        let props = run.read_block_uncached(footer.props)?;
        run.meta = RunMeta::decode(&props, file_size)
            .filter(|meta| meta.id == id)
            .ok_or_else(|| run.corruption(footer.props.offset, "bad run properties"))?;
        run.load_tombstones()?;
        Ok(Arc::new(run))
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn meta(&self) -> &RunMeta {
        &self.meta
    }

    pub fn stats(&self) -> &Arc<RunStats> {
        &self.ctx.stats
    }

    /// Range tombstones stored in this run, sorted by start then newest first.
    pub fn range_tombstones(&self) -> &[RangeTombstone] {
        &self.tombstones
    }

    /// The highest sequence `<= snapshot_seq` of a tombstone in this run covering
    /// `user_key`. Cost: a binary search plus a backwards walk over tombstones that start
    /// at or before the key, cut short once none of the earlier ones reaches it.
    pub fn max_covering_tombstone_seq(&self, user_key: &[u8], snapshot_seq: Seq) -> Option<Seq> {
        let end = self
            .tombstones
            .partition_point(|t| t.start.as_ref() <= user_key);
        let mut best: Option<Seq> = None;
        for i in (0..end).rev() {
            if self.tombstones[self.reach[i] as usize].end.as_ref() <= user_key {
                break;
            }
            let t = &self.tombstones[i];
            if t.end.as_ref() > user_key && t.seq <= snapshot_seq {
                best = Some(best.map_or(t.seq, |b| b.max(t.seq)));
            }
        }
        best
    }

    /// Whether the bloom filter allows `user_key`. Also false outside the run's key range.
    pub fn may_contain(&self, user_key: &[u8], fill_cache: bool) -> Result<bool> {
        if self.meta.num_entries == 0
            || user_key < crate::types::user_key(&self.meta.smallest)
            || user_key > crate::types::user_key(&self.meta.largest)
        {
            return Ok(false);
        }
        let filter = self.read_block(self.footer.bloom, fill_cache)?;
        RunStats::bump(&self.ctx.stats.bloom_checks, 1);
        let maybe = bloom::may_contain(&filter, user_key);
        if !maybe {
            RunStats::bump(&self.ctx.stats.bloom_negatives, 1);
        }
        Ok(maybe)
    }

    /// The newest point entry for `user_key` with sequence `<= snapshot_seq`: bloom
    /// filter, then index, then one data block. The value is a slice of the cached block.
    pub fn get(&self, user_key: &[u8], snapshot_seq: Seq, fill_cache: bool) -> Result<Lookup> {
        if !self.may_contain(user_key, fill_cache)? {
            return Ok(Lookup::NotFound);
        }
        let target = Target {
            user_key,
            trailer: (snapshot_seq.min(MAX_SEQ) << 8) | u64::from(SEEK_KIND_BYTE),
        };
        let index_block = self.read_block(self.footer.index, fill_cache)?;
        let mut index = self.block_iter(index_block, self.footer.index.offset)?;
        index.seek(target);
        if !index.valid() {
            self.check_block(&index, self.footer.index.offset)?;
            return Ok(Lookup::NotFound);
        }
        let handle = self.data_handle(&index)?;
        let block = self.read_block(handle, fill_cache)?;
        let mut data = self.block_iter(block, handle.offset)?;
        data.seek(target);
        if !data.valid() {
            self.check_block(&data, handle.offset)?;
            // The index named this block because its last key is >= target.
            return Err(self.corruption(handle.offset, "index points past the target"));
        }
        let parsed = parse_internal_key(data.key())
            .ok_or_else(|| self.corruption(handle.offset, "bad internal key in a data block"))?;
        if parsed.user_key != user_key {
            return Ok(Lookup::NotFound);
        }
        match parsed.kind {
            Kind::Put | Kind::JournalPut => Ok(Lookup::Found {
                kind: parsed.kind,
                value: data.value(),
                seq: parsed.seq,
            }),
            Kind::Delete => Ok(Lookup::Deleted { seq: parsed.seq }),
            Kind::RangeDelete => {
                Err(self.corruption(handle.offset, "range tombstone in a data block"))
            }
        }
    }

    /// An iterator over every point entry. `fill_cache = false` reads blocks that are not
    /// cached without inserting them (for scans and compaction).
    pub fn iter(self: &Arc<Self>, fill_cache: bool) -> RunIter {
        RunIter {
            run: Arc::clone(self),
            fill_cache,
            index: None,
            data: None,
            data_offset: u64::MAX,
            error: None,
        }
    }

    fn corruption(&self, offset: u64, what: &'static str) -> Error {
        RunStats::bump(&self.ctx.stats.corruption_detected, 1);
        Error::Corruption {
            file: FileId::Run { id: self.id },
            offset,
            what,
        }
    }

    fn block_iter(&self, block: Bytes, offset: u64) -> Result<BlockIter> {
        BlockIter::new(block).map_err(|what| self.corruption(offset, what))
    }

    fn check_block(&self, block: &BlockIter, offset: u64) -> Result<()> {
        match block.corruption() {
            Some(what) => Err(self.corruption(offset, what)),
            None => Ok(()),
        }
    }

    fn data_handle(&self, index: &BlockIter) -> Result<BlockHandle> {
        BlockHandle::decode_varint(index.value_slice())
            .filter(|h| h.end().is_some_and(|end| end <= self.data_end))
            .ok_or_else(|| self.corruption(self.footer.index.offset, "bad data block handle"))
    }

    /// A block through the cache. Cached blocks were checked when they were read.
    fn read_block(&self, handle: BlockHandle, fill_cache: bool) -> Result<Bytes> {
        let key = CacheKey::Run {
            store: self.ctx.namespace,
            run: self.id,
            offset: handle.offset,
        };
        if let Some(block) = self.ctx.cache.get(&key) {
            return Ok(block);
        }
        let block = self.read_block_uncached(handle)?;
        Ok(if fill_cache {
            self.ctx.cache.insert(key, block)
        } else {
            block
        })
    }

    /// One `pread` of the block and its trailer; the CRC is checked before anything uses it.
    fn read_block_uncached(&self, handle: BlockHandle) -> Result<Bytes> {
        let total = usize::try_from(handle.size)
            .ok()
            .and_then(|size| size.checked_add(BLOCK_TRAILER_LEN))
            .filter(|_| handle.end().is_some_and(|end| end <= self.footer_at))
            .ok_or_else(|| self.corruption(handle.offset, "block handle out of range"))?;
        let mut buf = vec![0u8; total];
        read_at(&*self.file, &mut buf, handle.offset)
            .map_err(|e| map_read_error(e, || self.corruption(handle.offset, "run truncated")))?;
        RunStats::bump(&self.ctx.stats.block_reads, 1);
        RunStats::bump(&self.ctx.stats.block_read_bytes, total as u64);
        if !crate::run::format::block_checksum_ok(&buf) {
            return Err(self.corruption(handle.offset, "block checksum mismatch"));
        }
        buf.truncate(total - BLOCK_TRAILER_LEN);
        Ok(Bytes::from(buf))
    }

    fn load_tombstones(&mut self) -> Result<()> {
        let handle = self.footer.range_del;
        let block = self.read_block_uncached(handle)?;
        let mut it = self.block_iter(block, handle.offset)?;
        it.seek_to_first();
        let mut tombstones = Vec::new();
        while it.valid() {
            let parsed = parse_internal_key(it.key())
                .filter(|p| p.kind == Kind::RangeDelete)
                .ok_or_else(|| self.corruption(handle.offset, "bad range tombstone key"))?;
            tombstones.push(RangeTombstone {
                start: Bytes::copy_from_slice(parsed.user_key),
                end: it.value(),
                seq: parsed.seq,
            });
            it.next();
        }
        self.check_block(&it, handle.offset)?;
        if tombstones.len() as u64 != self.meta.num_range_tombstones {
            return Err(self.corruption(handle.offset, "range tombstone count mismatch"));
        }
        let mut reach = Vec::with_capacity(tombstones.len());
        let mut best = 0u32;
        for (i, t) in tombstones.iter().enumerate() {
            if t.end > tombstones[best as usize].end {
                best = i as u32;
            }
            reach.push(best);
        }
        self.tombstones = tombstones;
        self.reach = reach;
        Ok(())
    }
}

fn empty_meta(id: u64) -> RunMeta {
    RunMeta {
        id,
        file_size: 0,
        smallest: Vec::new(),
        largest: Vec::new(),
        smallest_seq: 0,
        largest_seq: 0,
        num_entries: 0,
        num_deletions: 0,
        num_journal_values: 0,
        num_range_tombstones: 0,
        num_data_blocks: 0,
        oldest_journal_segment: None,
    }
}

fn read_at(file: &dyn FsFile, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

/// A short read means the file is shorter than its own handles say: corruption.
fn map_read_error(error: io::Error, corruption: impl FnOnce() -> Error) -> Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        corruption()
    } else {
        error.into()
    }
}

/// Two-level iterator: the index block picks data blocks, each walked by a [`BlockIter`].
/// While it lives it pins the index block, and the current data block while positioned
/// on it; both count as `pinned_bytes` in the block cache.
pub struct RunIter {
    run: Arc<Run>,
    fill_cache: bool,
    index: Option<BlockIter>,
    data: Option<BlockIter>,
    data_offset: u64,
    error: Option<Error>,
}

impl RunIter {
    pub fn run(&self) -> &Arc<Run> {
        &self.run
    }

    fn fail(&mut self, error: Error) {
        self.error = Some(error);
        self.data = None;
    }

    /// Clears an earlier error and makes sure the index block is loaded.
    fn start(&mut self) -> bool {
        self.error = None;
        if self.index.is_none() {
            let handle = self.run.footer.index;
            match self
                .run
                .read_block(handle, self.fill_cache)
                .and_then(|block| self.run.block_iter(block, handle.offset))
            {
                Ok(index) => self.index = Some(index),
                Err(error) => {
                    self.fail(error);
                    return false;
                }
            }
        }
        true
    }

    /// Loads the data block the index is positioned on. False (with `data = None`) when
    /// the index is exhausted or something failed.
    fn load_data(&mut self) -> bool {
        let index = self.index.as_ref().expect("index loaded");
        if !index.valid() {
            if let Some(what) = index.corruption() {
                let error = self.run.corruption(self.run.footer.index.offset, what);
                self.fail(error);
            }
            self.data = None;
            return false;
        }
        let handle = match self.run.data_handle(index) {
            Ok(handle) => handle,
            Err(error) => {
                self.fail(error);
                return false;
            }
        };
        if self.data.is_some() && self.data_offset == handle.offset {
            return true;
        }
        match self
            .run
            .read_block(handle, self.fill_cache)
            .and_then(|block| self.run.block_iter(block, handle.offset))
        {
            Ok(data) => {
                self.data = Some(data);
                self.data_offset = handle.offset;
                true
            }
            Err(error) => {
                self.fail(error);
                false
            }
        }
    }

    /// After the data iterator ran off its block: report damage, or report nothing.
    fn data_exhausted(&mut self) -> bool {
        if let Some(what) = self.data.as_ref().and_then(BlockIter::corruption) {
            let error = self.run.corruption(self.data_offset, what);
            self.fail(error);
            return false;
        }
        true
    }

    fn skip_forward(&mut self) {
        while self.error.is_none() && !self.data.as_ref().is_some_and(BlockIter::valid) {
            if self.data.is_some() && !self.data_exhausted() {
                return;
            }
            self.index.as_mut().expect("index loaded").next();
            if !self.load_data() {
                return;
            }
            self.data.as_mut().expect("loaded").seek_to_first();
        }
    }

    fn skip_backward(&mut self) {
        while self.error.is_none() && !self.data.as_ref().is_some_and(BlockIter::valid) {
            if self.data.is_some() && !self.data_exhausted() {
                return;
            }
            self.index.as_mut().expect("index loaded").prev();
            if !self.load_data() {
                return;
            }
            self.data.as_mut().expect("loaded").seek_to_last();
        }
    }

    fn seek_target(&mut self, target: Target<'_>) {
        if !self.start() {
            return;
        }
        self.index.as_mut().expect("index loaded").seek(target);
        if self.load_data() {
            self.data.as_mut().expect("loaded").seek(target);
            self.skip_forward();
        }
    }
}

impl InternalIterator for RunIter {
    fn valid(&self) -> bool {
        self.error.is_none() && self.data.as_ref().is_some_and(BlockIter::valid)
    }

    fn seek_to_first(&mut self) {
        if !self.start() {
            return;
        }
        self.index.as_mut().expect("index loaded").seek_to_first();
        if self.load_data() {
            self.data.as_mut().expect("loaded").seek_to_first();
            self.skip_forward();
        }
    }

    fn seek_to_last(&mut self) {
        if !self.start() {
            return;
        }
        self.index.as_mut().expect("index loaded").seek_to_last();
        if self.load_data() {
            self.data.as_mut().expect("loaded").seek_to_last();
            self.skip_backward();
        }
    }

    fn seek(&mut self, target: &[u8]) {
        self.seek_target(Target::from_internal(target));
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        let target = Target::from_internal(target);
        self.seek_target(target);
        if self.error.is_some() {
            return;
        }
        if !self.valid() {
            self.seek_to_last();
        } else if target.compare_key(self.key()) == Ordering::Greater {
            self.prev();
        }
    }

    fn next(&mut self) {
        debug_assert!(self.valid());
        self.data.as_mut().expect("valid").next();
        self.skip_forward();
    }

    fn prev(&mut self) {
        debug_assert!(self.valid());
        self.data.as_mut().expect("valid").prev();
        self.skip_backward();
    }

    fn key(&self) -> &[u8] {
        self.data.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.data.as_ref().expect("valid").value()
    }

    fn status(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}
