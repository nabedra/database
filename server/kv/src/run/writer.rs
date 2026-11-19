//! Builds one run file from internal-key-sorted entries plus range tombstones.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::fs::{Fs, FsFile, OpenMode};
use crate::run::block::BlockBuilder;
use crate::run::bloom::BloomBuilder;
use crate::run::format::{BlockHandle, Footer, RunMeta, seal_block};
use crate::source::RangeTombstone;
use crate::types::{
    Kind, Locator, MAX_SEQ, compare_internal_keys, encode_internal_key, parse_internal_key,
    user_key,
};

/// Largest value a run stores, so every block size fits the footer's `u32`.
pub const MAX_VALUE_LEN: usize = 1 << 31;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunWriterOptions {
    /// Target data block size. An entry larger than this forms a block of its own. Index
    /// partitions are cut at the same size (see [`crate::run`]'s format notes).
    pub block_size: usize,
    /// Every n-th key of a data block is stored whole.
    pub restart_interval: usize,
    pub bloom_bits_per_key: usize,
    /// Encoded blocks are buffered up to this size before one `pwrite`.
    pub write_buffer_bytes: usize,
}

impl Default for RunWriterOptions {
    fn default() -> Self {
        RunWriterOptions {
            block_size: 4096,
            restart_interval: 16,
            bloom_bits_per_key: 10,
            write_buffer_bytes: 1 << 20,
        }
    }
}

/// Writes a run. Feed point entries in strictly increasing internal-key order with
/// [`add`](RunWriter::add), range tombstones in any order with
/// [`add_range_tombstone`](RunWriter::add_range_tombstone), then call
/// [`finish`](RunWriter::finish), which leaves the file durable (`sync_all`). Making the
/// run reachable (directory sync, manifest edit) is the caller's job, as is removing the
/// file when the writer fails or is dropped unfinished.
///
/// Memory: the write buffer, one data block, one index partition and one `u64` hash per
/// distinct user key of that partition, the finished partitions (about 1.3 bytes per
/// key for the filters plus one index entry per data block), one top-level entry per
/// partition, and the range tombstones.
pub struct RunWriter {
    file: Arc<dyn FsFile>,
    id: u64,
    options: RunWriterOptions,
    out: Vec<u8>,
    /// File offset of `out[0]`.
    written: u64,
    data: BlockBuilder,
    /// The index partition being built.
    index: BlockBuilder,
    index_value: Vec<u8>,
    /// The filter partition being built.
    bloom: BloomBuilder,
    /// A partition was just cut: the next entry's user key enters the new filter even if
    /// it equals the previous entry's.
    bloom_fresh: bool,
    /// Finished filter and index partitions, with trailers; offsets relative to its start.
    parts: Vec<u8>,
    /// Per partition: its last internal key and its index and filter handles (relative).
    top: Vec<(Vec<u8>, BlockHandle, BlockHandle)>,
    last_key: Vec<u8>,
    tombstones: Vec<RangeTombstone>,
    meta: RunMeta,
    failed: Option<Error>,
}

impl RunWriter {
    /// Creates `path`, which must not exist.
    pub fn create(fs: &dyn Fs, path: &Path, id: u64, options: RunWriterOptions) -> Result<Self> {
        let file = fs.open(path, OpenMode::CreateNew)?;
        Ok(RunWriter {
            file,
            id,
            options,
            out: Vec::with_capacity(options.write_buffer_bytes.min(1 << 20) + options.block_size),
            written: 0,
            data: BlockBuilder::new(options.restart_interval),
            index: BlockBuilder::new(1),
            index_value: Vec::with_capacity(20),
            bloom: BloomBuilder::new(options.bloom_bits_per_key),
            bloom_fresh: false,
            parts: Vec::new(),
            top: Vec::new(),
            last_key: Vec::new(),
            tombstones: Vec::new(),
            meta: RunMeta {
                id,
                file_size: 0,
                smallest: Vec::new(),
                largest: Vec::new(),
                smallest_seq: MAX_SEQ,
                largest_seq: 0,
                num_entries: 0,
                num_deletions: 0,
                num_journal_values: 0,
                num_range_tombstones: 0,
                num_data_blocks: 0,
                oldest_journal_segment: None,
            },
            failed: None,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Point entries added so far.
    pub fn num_entries(&self) -> u64 {
        self.meta.num_entries
    }

    /// Approximate file size so far, for splitting compaction output.
    pub fn estimated_size(&self) -> u64 {
        self.written + (self.out.len() + self.data.size_estimate()) as u64
    }

    /// Adds a point entry: a `Put`, a `JournalPut` (whose value is an encoded
    /// [`Locator`]) or a `Delete`. Keys must strictly increase.
    pub fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        self.check_failed()?;
        let parsed = parse_internal_key(ikey).ok_or(Error::InvalidArgument(
            "run entry is not a valid internal key",
        ))?;
        if parsed.kind == Kind::RangeDelete {
            return Err(Error::InvalidArgument(
                "range tombstones go through add_range_tombstone",
            ));
        }
        if value.len() > MAX_VALUE_LEN {
            return Err(Error::InvalidArgument("value too large for a run"));
        }
        let first = self.meta.num_entries == 0;
        if !first && compare_internal_keys(ikey, &self.last_key) != Ordering::Greater {
            return Err(Error::InvalidArgument("run entries out of order"));
        }
        match parsed.kind {
            Kind::Delete => self.meta.num_deletions += 1,
            Kind::JournalPut => {
                let locator = Locator::decode(value)
                    .ok_or(Error::InvalidArgument("journal put value is not a locator"))?;
                self.meta.num_journal_values += 1;
                self.meta.oldest_journal_segment = Some(
                    self.meta
                        .oldest_journal_segment
                        .map_or(locator.segment, |s| s.min(locator.segment)),
                );
            }
            Kind::Put | Kind::RangeDelete => {}
        }

        // A block that would overflow is closed first, so an oversized entry starts a
        // fresh block and is closed right after it.
        let entry_estimate = ikey.len() + value.len() + 15;
        if !self.data.is_empty()
            && self.data.size_estimate() + entry_estimate > self.options.block_size
        {
            self.flush_data_block()?;
        }
        // After any partition cut above, so the key lands in its own partition's filter.
        if first || self.bloom_fresh || user_key(&self.last_key) != parsed.user_key {
            self.bloom.add_key(parsed.user_key);
            self.bloom_fresh = false;
        }
        self.data.add(ikey, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(ikey);
        if first {
            self.meta.smallest = ikey.to_vec();
        }
        self.meta.smallest_seq = self.meta.smallest_seq.min(parsed.seq);
        self.meta.largest_seq = self.meta.largest_seq.max(parsed.seq);
        self.meta.num_entries += 1;
        if self.data.size_estimate() >= self.options.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Adds a range tombstone. An empty or inverted range deletes nothing and is dropped.
    pub fn add_range_tombstone(&mut self, tombstone: RangeTombstone) -> Result<()> {
        self.check_failed()?;
        if tombstone.seq > MAX_SEQ {
            return Err(Error::InvalidArgument("range tombstone sequence too large"));
        }
        if tombstone.start < tombstone.end {
            self.tombstones.push(tombstone);
        }
        Ok(())
    }

    /// Writes the remaining blocks and the footer, then `sync_all`s the file and advises
    /// the OS to drop its pages (the block cache is the only cache).
    pub fn finish(mut self) -> Result<RunMeta> {
        self.check_failed()?;
        if self.meta.num_entries == 0 && self.tombstones.is_empty() {
            return Err(Error::InvalidArgument("a run needs at least one entry"));
        }
        if !self.data.is_empty() {
            self.flush_data_block()?;
        }
        if !self.index.is_empty() {
            self.cut_partition()?;
        }

        // Range tombstones, sorted by internal key of their start.
        self.tombstones.sort_by(|a, b| {
            a.start
                .cmp(&b.start)
                .then_with(|| b.seq.cmp(&a.seq))
                .then_with(|| a.end.cmp(&b.end))
        });
        let mut range_del = BlockBuilder::new(16);
        let mut key = Vec::new();
        let mut max_end: Option<&Bytes> = None;
        for t in &self.tombstones {
            key.clear();
            encode_internal_key(&mut key, &t.start, t.seq, Kind::RangeDelete);
            range_del.add(&key, &t.end);
            self.meta.smallest_seq = self.meta.smallest_seq.min(t.seq);
            self.meta.largest_seq = self.meta.largest_seq.max(t.seq);
            if max_end.is_none_or(|end| *end < t.end) {
                max_end = Some(&t.end);
            }
        }
        if let Some(first) = self.tombstones.first() {
            let start = crate::types::internal_key(&first.start, first.seq, Kind::RangeDelete);
            if self.meta.num_entries == 0
                || compare_internal_keys(&start, &self.meta.smallest) == Ordering::Less
            {
                self.meta.smallest = start;
            }
            let end =
                crate::types::internal_key(max_end.expect("non-empty"), MAX_SEQ, Kind::RangeDelete);
            self.meta.largest = if self.meta.num_entries == 0
                || compare_internal_keys(&end, &self.last_key) == Ordering::Greater
            {
                end
            } else {
                self.last_key.clone()
            };
        } else {
            self.meta.largest = self.last_key.clone();
        }
        self.meta.num_range_tombstones = self.tombstones.len() as u64;

        let handle = emit_block(&mut self.out, self.written, |out| {
            range_del.finish_into(out)
        });
        let range_del = self.check_emitted(handle)?;
        let partitions = BlockHandle {
            offset: self.written + self.out.len() as u64,
            size: self.parts.len() as u64,
        };
        if partitions.size > u64::from(u32::MAX) {
            return self.fail(Error::InvalidArgument(
                "run partitions larger than u32::MAX",
            ));
        }
        self.out.extend_from_slice(&self.parts);
        self.parts = Vec::new();
        let meta = &self.meta;
        let handle = emit_block(&mut self.out, self.written, |out| meta.encode(out));
        let props = self.check_emitted(handle)?;
        let mut top = BlockBuilder::new(1);
        for (key, index, filter) in &self.top {
            let at = |h: &BlockHandle| BlockHandle {
                offset: partitions.offset + h.offset,
                size: h.size,
            };
            self.index_value.clear();
            BlockHandle::encode_pair(at(index), at(filter), &mut self.index_value);
            top.add(key, &self.index_value);
        }
        let handle = emit_block(&mut self.out, self.written, |out| top.finish_into(out));
        let index = self.check_emitted(handle)?;
        let footer = Footer {
            index,
            partitions,
            range_del,
            props,
        };
        self.out.extend_from_slice(&footer.encode());
        self.write_out()?;
        let size = self.written;
        self.file.sync_all()?;
        self.file.advise_dont_need(0, size);
        self.meta.file_size = size;
        Ok(self.meta)
    }

    fn check_failed(&self) -> Result<()> {
        match &self.failed {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let data = &mut self.data;
        let handle = emit_block(&mut self.out, self.written, |out| data.finish_into(out));
        let handle = self.check_emitted(handle)?;
        self.index_value.clear();
        handle.encode_varint(&mut self.index_value);
        self.index.add(&self.last_key, &self.index_value);
        self.meta.num_data_blocks += 1;
        if self.index.size_estimate() >= self.options.block_size {
            self.cut_partition()?;
        }
        if self.out.len() >= self.options.write_buffer_bytes {
            self.write_out()?;
        }
        Ok(())
    }

    /// Finishes the current filter and index partitions (kept in memory until `finish`
    /// writes them after the data) and records their top-level entry.
    fn cut_partition(&mut self) -> Result<()> {
        let bloom = &self.bloom;
        let filter = emit_block(&mut self.parts, 0, |out| bloom.finish_into(out));
        let filter = self.check_emitted(filter)?;
        self.bloom.clear();
        let index = &mut self.index;
        let handle = emit_block(&mut self.parts, 0, |out| index.finish_into(out));
        let index = self.check_emitted(handle)?;
        self.top.push((self.last_key.clone(), index, filter));
        self.bloom_fresh = true;
        Ok(())
    }

    fn check_emitted(&mut self, handle: Option<BlockHandle>) -> Result<BlockHandle> {
        match handle {
            Some(handle) => Ok(handle),
            None => self.fail(Error::InvalidArgument("run block larger than u32::MAX")),
        }
    }

    fn write_out(&mut self) -> Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.file.write_all_at(&self.out, self.written) {
            return self.fail(error.into());
        }
        self.written += self.out.len() as u64;
        self.out.clear();
        Ok(())
    }

    fn fail<T>(&mut self, error: Error) -> Result<T> {
        self.failed = Some(error.clone());
        Err(error)
    }
}

/// Appends one block plus its CRC trailer to `out`, whose first byte sits at file offset
/// `written`. `None` when the block is too large for a handle.
fn emit_block(
    out: &mut Vec<u8>,
    written: u64,
    fill: impl FnOnce(&mut Vec<u8>),
) -> Option<BlockHandle> {
    let start = out.len();
    fill(out);
    let size = (out.len() - start) as u64;
    if size > u64::from(u32::MAX) {
        return None;
    }
    seal_block(out, start);
    Some(BlockHandle {
        offset: written + start as u64,
        size,
    })
}
