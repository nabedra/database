//! Reads a run: point lookups, iterators and range tombstones.

use std::cmp::Ordering;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use bytes::Bytes;

use crate::cache::{BlockCache, CacheKey, CacheReservation};
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
///
/// While open it holds the top-level index in memory (checked at open, charged to the
/// block cache as a reservation); index and filter partitions and data blocks go through
/// the block cache.
pub struct Run {
    id: u64,
    file: Arc<dyn FsFile>,
    ctx: RunReadContext,
    footer: Footer,
    meta: RunMeta,
    /// The top-level index block: one entry per partition.
    top: Bytes,
    /// Charges `top` to the block cache while the run is open.
    _pinned: Option<CacheReservation>,
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
    /// Opens a run: checks the footer, then loads the properties, the range tombstones and
    /// the top-level index (every partition handle is checked). Index and filter
    /// partitions are read lazily through the block cache.
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
        let laid_out = footer.range_del.end() == Some(footer.partitions.offset)
            && footer.partitions_end() == Some(footer.props.offset)
            && footer.props.end() == Some(footer.index.offset)
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
            top: Bytes::new(),
            _pinned: None,
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
        run.load_top_index()?;
        Ok(Arc::new(run))
    }

    /// Reads the top-level index and checks every entry's partition handles, so a damaged
    /// top-level index refuses the run at open.
    fn load_top_index(&mut self) -> Result<()> {
        let handle = self.footer.index;
        let top = self.read_block_uncached(handle)?;
        let mut it = self.block_iter(top.clone(), handle.offset)?;
        it.seek_to_first();
        let mut partitions = 0u64;
        while it.valid() {
            self.partition_handles(&it)?;
            partitions += 1;
            it.next();
        }
        self.check_block(&it, handle.offset)?;
        if (partitions == 0) != (self.meta.num_data_blocks == 0) {
            return Err(self.corruption(handle.offset, "top-level index does not match the data"));
        }
        self._pinned = Some(self.ctx.cache.reserve(top.len()));
        self.top = top;
        Ok(())
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
        let target = Target {
            user_key,
            trailer: (MAX_SEQ << 8) | u64::from(SEEK_KIND_BYTE),
        };
        Ok(self.locate(target, fill_cache)?.is_some())
    }

    /// The index partition that holds `target` if anything does, after the key range and
    /// that partition's filter allow the user key. The first partition whose last key is
    /// `>= target` holds every entry of the user key at or below the target's sequence.
    fn locate(&self, target: Target<'_>, fill_cache: bool) -> Result<Option<BlockHandle>> {
        let user_key = target.user_key;
        if self.meta.num_entries == 0
            || user_key < crate::types::user_key(&self.meta.smallest)
            || user_key > crate::types::user_key(&self.meta.largest)
        {
            return Ok(None);
        }
        let mut top = self.block_iter(self.top.clone(), self.footer.index.offset)?;
        top.seek(target);
        if !top.valid() {
            self.check_block(&top, self.footer.index.offset)?;
            return Ok(None);
        }
        let (index, filter) = self.partition_handles(&top)?;
        let filter = self.read_block(filter, fill_cache)?;
        RunStats::bump(&self.ctx.stats.bloom_checks, 1);
        if !bloom::may_contain(&filter, user_key) {
            RunStats::bump(&self.ctx.stats.bloom_negatives, 1);
            return Ok(None);
        }
        Ok(Some(index))
    }

    /// The newest point entry for `user_key` with sequence `<= snapshot_seq`: top-level
    /// index (in memory), the partition's filter, the index partition, then one data
    /// block; each of the last three cached or one `pread`. The value is a slice of the
    /// cached block.
    pub fn get(&self, user_key: &[u8], snapshot_seq: Seq, fill_cache: bool) -> Result<Lookup> {
        let target = Target {
            user_key,
            trailer: (snapshot_seq.min(MAX_SEQ) << 8) | u64::from(SEEK_KIND_BYTE),
        };
        let Some(part) = self.locate(target, fill_cache)? else {
            return Ok(Lookup::NotFound);
        };
        let index_block = self.read_block(part, fill_cache)?;
        let mut index = self.block_iter(index_block, part.offset)?;
        index.seek(target);
        if !index.valid() {
            self.check_block(&index, part.offset)?;
            // The top-level index named this partition because its last key is >= target.
            return Err(self.corruption(part.offset, "partition points past the target"));
        }
        let handle = self.data_handle(&index, part.offset)?;
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

    /// The data block handle at `index`'s position; `at` is the index partition's offset.
    fn data_handle(&self, index: &BlockIter, at: u64) -> Result<BlockHandle> {
        BlockHandle::decode_varint(index.value_slice())
            .filter(|h| h.end().is_some_and(|end| end <= self.data_end))
            .ok_or_else(|| self.corruption(at, "bad data block handle"))
    }

    /// The index and filter partition handles at `top`'s position, both inside the
    /// partition region.
    fn partition_handles(&self, top: &BlockIter) -> Result<(BlockHandle, BlockHandle)> {
        let start = self.footer.partitions.offset;
        let end = self.footer.partitions_end();
        let inside = |h: &BlockHandle| {
            h.offset >= start && h.end().is_some_and(|e| end.is_some_and(|end| e <= end))
        };
        BlockHandle::decode_pair(top.value_slice())
            .filter(|(index, filter)| inside(index) && inside(filter))
            .ok_or_else(|| self.corruption(self.footer.index.offset, "bad partition handle"))
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

/// The index of a run as one sequence of data block handles: the in-memory top-level
/// index picks index partitions, each walked by a [`BlockIter`].
struct IndexIter {
    top: BlockIter,
    part: Option<BlockIter>,
    part_offset: u64,
    error: Option<Error>,
}

impl IndexIter {
    fn new(run: &Run) -> Result<IndexIter> {
        Ok(IndexIter {
            top: run.block_iter(run.top.clone(), run.footer.index.offset)?,
            part: None,
            part_offset: u64::MAX,
            error: None,
        })
    }

    fn valid(&self) -> bool {
        self.error.is_none() && self.part.as_ref().is_some_and(BlockIter::valid)
    }

    fn fail(&mut self, error: Error) {
        self.error = Some(error);
        self.part = None;
    }

    /// Loads the partition the top-level index is positioned on (kept when it is already
    /// loaded). False when the top-level index is exhausted or something failed.
    fn load(&mut self, run: &Run, fill_cache: bool) -> bool {
        if !self.top.valid() {
            if let Some(what) = self.top.corruption() {
                self.fail(run.corruption(run.footer.index.offset, what));
            }
            self.part = None;
            return false;
        }
        let index = match run.partition_handles(&self.top) {
            Ok((index, _)) => index,
            Err(error) => {
                self.fail(error);
                return false;
            }
        };
        if self.part.is_some() && self.part_offset == index.offset {
            return true;
        }
        match run
            .read_block(index, fill_cache)
            .and_then(|block| run.block_iter(block, index.offset))
        {
            Ok(part) => {
                self.part = Some(part);
                self.part_offset = index.offset;
                true
            }
            Err(error) => {
                self.fail(error);
                false
            }
        }
    }

    /// After the partition iterator ran off its block: damage, or the next partition.
    fn skip(&mut self, run: &Run, fill_cache: bool, forward: bool) {
        while self.error.is_none() && !self.part.as_ref().is_some_and(BlockIter::valid) {
            let Some(part) = &self.part else {
                return;
            };
            if let Some(what) = part.corruption() {
                let error = run.corruption(self.part_offset, what);
                self.fail(error);
                return;
            }
            if forward {
                self.top.next();
            } else {
                self.top.prev();
            }
            if !self.load(run, fill_cache) {
                return;
            }
            let part = self.part.as_mut().expect("loaded");
            if forward {
                part.seek_to_first();
            } else {
                part.seek_to_last();
            }
        }
    }

    fn seek_to_first(&mut self, run: &Run, fill_cache: bool) {
        self.error = None;
        self.top.seek_to_first();
        if self.load(run, fill_cache) {
            self.part.as_mut().expect("loaded").seek_to_first();
            self.skip(run, fill_cache, true);
        }
    }

    fn seek_to_last(&mut self, run: &Run, fill_cache: bool) {
        self.error = None;
        self.top.seek_to_last();
        if self.load(run, fill_cache) {
            self.part.as_mut().expect("loaded").seek_to_last();
            self.skip(run, fill_cache, false);
        }
    }

    fn seek(&mut self, run: &Run, fill_cache: bool, target: Target<'_>) {
        self.error = None;
        self.top.seek(target);
        if self.load(run, fill_cache) {
            self.part.as_mut().expect("loaded").seek(target);
            self.skip(run, fill_cache, true);
        }
    }

    fn next(&mut self, run: &Run, fill_cache: bool) {
        self.part.as_mut().expect("valid").next();
        self.skip(run, fill_cache, true);
    }

    fn prev(&mut self, run: &Run, fill_cache: bool) {
        self.part.as_mut().expect("valid").prev();
        self.skip(run, fill_cache, false);
    }
}

/// Three-level iterator: the top-level index picks index partitions, which pick data
/// blocks, each walked by a [`BlockIter`]. While it lives it pins its current index
/// partition, and the current data block while positioned on it; both count as
/// `pinned_bytes` in the block cache.
pub struct RunIter {
    run: Arc<Run>,
    fill_cache: bool,
    index: Option<IndexIter>,
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

    /// Clears an earlier error and makes sure the index iterator exists.
    fn start(&mut self) -> bool {
        self.error = None;
        if self.index.is_none() {
            match IndexIter::new(&self.run) {
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
            if let Some(error) = index.error.clone() {
                self.fail(error);
            }
            self.data = None;
            return false;
        }
        let at = index.part_offset;
        let handle = match self
            .run
            .data_handle(index.part.as_ref().expect("valid"), at)
        {
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
            let run = &self.run;
            self.index
                .as_mut()
                .expect("index loaded")
                .next(run, self.fill_cache);
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
            let run = &self.run;
            self.index
                .as_mut()
                .expect("index loaded")
                .prev(run, self.fill_cache);
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
        let run = &self.run;
        self.index
            .as_mut()
            .expect("index loaded")
            .seek(run, self.fill_cache, target);
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
        let run = &self.run;
        self.index
            .as_mut()
            .expect("index loaded")
            .seek_to_first(run, self.fill_cache);
        if self.load_data() {
            self.data.as_mut().expect("loaded").seek_to_first();
            self.skip_forward();
        }
    }

    fn seek_to_last(&mut self) {
        if !self.start() {
            return;
        }
        let run = &self.run;
        self.index
            .as_mut()
            .expect("index loaded")
            .seek_to_last(run, self.fill_cache);
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
