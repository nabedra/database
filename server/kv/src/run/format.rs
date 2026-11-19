//! Run file layout.
//!
//! ```text
//! run    = data_block* ‖ range_del_block ‖ bloom_block ‖ props_block ‖ index_block ‖ footer
//! block  = contents ‖ masked_crc32c(contents):u32le
//! footer = magic:u64le ‖ version:u32le ‖ index ‖ bloom ‖ range_del ‖ props
//!          ‖ masked_crc32c(footer[0..60]):u32le                       (64 bytes)
//! handle = offset:u64le ‖ size:u32le                  (in the footer, 12 bytes)
//! ```
//!
//! Index entries map the last internal key of each data block to a varint handle
//! (`varint64 offset ‖ varint64 size`). The range-delete block maps
//! `internal_key(start, seq, RangeDelete)` to `end`. Sizes exclude the CRC trailer.

use crate::coding::{get_varint64, put_varint64, read_u32_le, read_u64_le};
use crate::crc;
use crate::types::{Seq, parse_internal_key};

pub(crate) const MAGIC: u64 = u64::from_le_bytes(*b"NBKVRUN1");
pub(crate) const FORMAT_VERSION: u32 = 1;
pub const FOOTER_LEN: usize = 64;
pub(crate) const BLOCK_TRAILER_LEN: usize = 4;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

impl BlockHandle {
    pub(crate) fn encode_varint(&self, out: &mut Vec<u8>) {
        put_varint64(out, self.offset);
        put_varint64(out, self.size);
    }

    pub(crate) fn decode_varint(input: &[u8]) -> Option<BlockHandle> {
        let (offset, a) = get_varint64(input)?;
        let (size, b) = get_varint64(&input[a..])?;
        (a + b == input.len()).then_some(BlockHandle { offset, size })
    }

    /// Offset just past the block's CRC trailer.
    pub(crate) fn end(&self) -> Option<u64> {
        self.offset
            .checked_add(self.size)?
            .checked_add(BLOCK_TRAILER_LEN as u64)
    }
}

/// Appends the masked CRC32C trailer of `out[start..]`.
pub(crate) fn seal_block(out: &mut Vec<u8>, start: usize) {
    let checksum = crc::mask(crc::crc32c(&out[start..]));
    out.extend_from_slice(&checksum.to_le_bytes());
}

/// True when `block` (contents followed by the trailer) carries a matching CRC.
pub(crate) fn block_checksum_ok(block: &[u8]) -> bool {
    let Some(split) = block.len().checked_sub(BLOCK_TRAILER_LEN) else {
        return false;
    };
    let stored = read_u32_le(block, split).expect("trailer present");
    crc::unmask(stored) == crc::crc32c(&block[..split])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Footer {
    pub(crate) index: BlockHandle,
    pub(crate) bloom: BlockHandle,
    pub(crate) range_del: BlockHandle,
    pub(crate) props: BlockHandle,
}

impl Footer {
    pub(crate) fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        out[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        for (i, handle) in [self.index, self.bloom, self.range_del, self.props]
            .iter()
            .enumerate()
        {
            let at = 12 + 12 * i;
            out[at..at + 8].copy_from_slice(&handle.offset.to_le_bytes());
            let size = u32::try_from(handle.size).expect("checked by the writer");
            out[at + 8..at + 12].copy_from_slice(&size.to_le_bytes());
        }
        let checksum = crc::mask(crc::crc32c(&out[..60]));
        out[60..64].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8; FOOTER_LEN]) -> Result<Footer, &'static str> {
        let stored = read_u32_le(bytes, 60).expect("fixed size");
        if crc::unmask(stored) != crc::crc32c(&bytes[..60]) {
            return Err("run footer checksum mismatch");
        }
        if read_u64_le(bytes, 0) != Some(MAGIC) {
            return Err("bad run magic");
        }
        if read_u32_le(bytes, 8) != Some(FORMAT_VERSION) {
            return Err("unsupported run format version");
        }
        let handle = |i: usize| {
            let at = 12 + 12 * i;
            BlockHandle {
                offset: read_u64_le(bytes, at).expect("fixed size"),
                size: u64::from(read_u32_le(bytes, at + 8).expect("fixed size")),
            }
        };
        Ok(Footer {
            index: handle(0),
            bloom: handle(1),
            range_del: handle(2),
            props: handle(3),
        })
    }
}

/// What a finished run holds. Stored in the run's properties block and returned by
/// [`crate::run::RunWriter::finish`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunMeta {
    pub id: u64,
    pub file_size: u64,
    /// Smallest internal key, over point entries and range tombstone starts.
    pub smallest: Vec<u8>,
    /// Largest internal key, over point entries and range tombstone ends. A tombstone end
    /// `e` is represented as `internal_key(e, MAX_SEQ, RangeDelete)`, which sorts before
    /// every real entry of `e`; since the end is exclusive the bound is conservative.
    pub largest: Vec<u8>,
    /// Sequence range over every point entry and range tombstone.
    pub smallest_seq: Seq,
    pub largest_seq: Seq,
    /// Point entries (puts, journal puts and point tombstones).
    pub num_entries: u64,
    pub num_deletions: u64,
    pub num_journal_values: u64,
    pub num_range_tombstones: u64,
    pub num_data_blocks: u64,
    /// Lowest journal segment any `JournalPut` locator in this run points into, for
    /// journal GC.
    pub oldest_journal_segment: Option<u64>,
}

impl RunMeta {
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.to_le_bytes());
        for key in [&self.smallest, &self.largest] {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
        for value in [
            self.smallest_seq,
            self.largest_seq,
            self.num_entries,
            self.num_deletions,
            self.num_journal_values,
            self.num_range_tombstones,
            self.num_data_blocks,
            self.oldest_journal_segment.unwrap_or(u64::MAX),
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }

    pub(crate) fn decode(bytes: &[u8], file_size: u64) -> Option<RunMeta> {
        let id = read_u64_le(bytes, 0)?;
        let mut at = 8usize;
        let key = |at: &mut usize| -> Option<Vec<u8>> {
            let len = read_u32_le(bytes, *at)? as usize;
            let key = bytes.get(*at + 4..(*at + 4).checked_add(len)?)?.to_vec();
            *at += 4 + len;
            parse_internal_key(&key)?;
            Some(key)
        };
        let smallest = key(&mut at)?;
        let largest = key(&mut at)?;
        let mut fields = [0u64; 8];
        for field in &mut fields {
            *field = read_u64_le(bytes, at)?;
            at += 8;
        }
        if at != bytes.len() {
            return None;
        }
        let [
            smallest_seq,
            largest_seq,
            num_entries,
            num_deletions,
            num_journal_values,
            num_range_tombstones,
            num_data_blocks,
            oldest_segment,
        ] = fields;
        Some(RunMeta {
            id,
            file_size,
            smallest,
            largest,
            smallest_seq,
            largest_seq,
            num_entries,
            num_deletions,
            num_journal_values,
            num_range_tombstones,
            num_data_blocks,
            oldest_journal_segment: (oldest_segment != u64::MAX).then_some(oldest_segment),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Kind, internal_key};

    #[test]
    fn footer_and_meta_round_trip() {
        let footer = Footer {
            index: BlockHandle {
                offset: 1 << 33,
                size: 77,
            },
            bloom: BlockHandle { offset: 5, size: 6 },
            range_del: BlockHandle { offset: 7, size: 0 },
            props: BlockHandle {
                offset: 9,
                size: u64::from(u32::MAX),
            },
        };
        let bytes = footer.encode();
        assert_eq!(Footer::decode(&bytes), Ok(footer));
        for bit in 0..FOOTER_LEN * 8 {
            let mut bad = bytes;
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(Footer::decode(&bad).is_err(), "bit {bit}");
        }

        let meta = RunMeta {
            id: 42,
            file_size: 1000,
            smallest: internal_key(b"a", 1, Kind::Put),
            largest: internal_key(b"zz", 9, Kind::Delete),
            smallest_seq: 1,
            largest_seq: 9,
            num_entries: 3,
            num_deletions: 1,
            num_journal_values: 0,
            num_range_tombstones: 2,
            num_data_blocks: 1,
            oldest_journal_segment: Some(4),
        };
        let mut encoded = Vec::new();
        meta.encode(&mut encoded);
        assert_eq!(RunMeta::decode(&encoded, 1000), Some(meta));
        assert_eq!(RunMeta::decode(&encoded[1..], 1000), None);
        encoded.push(0);
        assert_eq!(RunMeta::decode(&encoded, 1000), None);

        let handle = BlockHandle {
            offset: 300,
            size: 4096,
        };
        let mut buf = Vec::new();
        handle.encode_varint(&mut buf);
        assert_eq!(BlockHandle::decode_varint(&buf), Some(handle));
        buf.push(0);
        assert_eq!(BlockHandle::decode_varint(&buf), None);
    }
}
