//! On-disk layout of journal segments and records (plan section 2, "Journal format").
//!
//! ```text
//! segment = header block A (4 KiB) ‖ header block B (4 KiB) ‖ record*   (records at 8192)
//! header block: one header copy at its start, zeros elsewhere
//! header copy = magic "NBKVJNL1" | version u32 | 0 u32 | segment u64 | epoch u64
//!             | first_seq u64 | crc u32 (masked CRC32C of the 40 bytes before it)
//!
//! record = header(48) ‖ scope(scope_len) ‖ payload(payload_len)
//! header = header_crc u32 | payload_crc u32 | payload_len u32 | kind u8 | flags u8
//!        | scope_len u8 | 0 u8 | epoch u64 | first_seq u64 | count u32 | 0 u32 | durable_seq u64
//! ```
//!
//! All integers are little-endian and both CRCs are masked ([`crate::crc::mask`]).
//! `header_crc` covers header bytes 4..48 and the scope; `payload_crc` covers the payload.
//!
//! The header is kept twice, each copy in its own 4 KiB block, and a header rewrite (a
//! reused free file, a re-epoch) writes and syncs copy A before it writes copy B. A power
//! loss during the rewrite can therefore destroy at most one block, even on a device
//! whose atomic or program unit is a whole 4 KiB page, and the other copy (old or new)
//! survives. `first_seq` is the sequence the segment's first record takes; it lets
//! recovery pick its start segment without reading records and keeps sequence continuity
//! checkable across segment boundaries.

use crate::coding::{read_u32_le, read_u64_le};
use crate::crc;
use crate::types::Seq;

/// Alignment unit: the header block size, and the boundary every group commit that
/// follows a sync starts on.
pub const BLOCK: u64 = 4096;
/// Offset of the first record: after the two header blocks.
pub const RECORDS_START: u64 = 2 * BLOCK;
pub const SEGMENT_MAGIC: [u8; 8] = *b"NBKVJNL1";
pub const FORMAT_VERSION: u32 = 1;
const SEGMENT_HEADER_LEN: usize = 44;
const SEGMENT_HEADER_COPY_B: usize = BLOCK as usize;

pub const RECORD_HEADER_LEN: usize = 48;

/// `kind` byte of a record. Zero is reserved, so an all-zero header never parses.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordType {
    Batch = 1,
    Relocate = 2,
    /// No data. With `count > 0` it voids that many sequence numbers (see the writer's
    /// `NoSpace` handling and the open-time gap fill); with [`FLAG_ROLL`] it ends the
    /// segment; otherwise it aligns the next write to [`BLOCK`].
    Pad = 3,
}

impl RecordType {
    pub fn from_u8(byte: u8) -> Option<RecordType> {
        match byte {
            1 => Some(RecordType::Batch),
            2 => Some(RecordType::Relocate),
            3 => Some(RecordType::Pad),
            _ => None,
        }
    }
}

/// The submitter asked for [`super::Durability::Sync`].
pub const FLAG_SYNC: u8 = 1;
/// On a `Pad`: the rest of this segment is unused in this epoch; continue at the next one.
pub const FLAG_ROLL: u8 = 2;
/// On a `Pad`: written and synced by a clean close. Nothing follows it in this epoch.
pub const FLAG_CLOSE: u8 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub segment: u64,
    pub epoch: u64,
    pub first_seq: Seq,
}

/// What one copy of a segment header holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderCopy {
    Zero,
    Bad,
    /// The CRC holds but the version is not one this build writes.
    Unsupported(u32),
    Valid(SegmentHeader),
}

impl SegmentHeader {
    /// One 4 KiB header block (copy A and copy B are identical blocks).
    pub fn encode_block(&self) -> Vec<u8> {
        let mut block = vec![0u8; BLOCK as usize];
        block[..SEGMENT_HEADER_LEN].copy_from_slice(&self.encode_copy());
        block
    }

    /// Both header blocks, `[0, RECORDS_START)`, for a file no record was written to.
    pub fn encode_blocks(&self) -> Vec<u8> {
        let mut blocks = self.encode_block();
        blocks.extend_from_slice(&self.encode_block());
        blocks
    }

    fn encode_copy(&self) -> [u8; SEGMENT_HEADER_LEN] {
        let mut out = [0u8; SEGMENT_HEADER_LEN];
        out[0..8].copy_from_slice(&SEGMENT_MAGIC);
        out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[16..24].copy_from_slice(&self.segment.to_le_bytes());
        out[24..32].copy_from_slice(&self.epoch.to_le_bytes());
        out[32..40].copy_from_slice(&self.first_seq.to_le_bytes());
        let crc = crc::mask(crc::crc32c(&out[..40]));
        out[40..44].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes both copies from the header blocks `[0, RECORDS_START)` (which may be short).
    pub fn decode_blocks(block: &[u8]) -> [HeaderCopy; 2] {
        [
            decode_copy(block.get(..SEGMENT_HEADER_LEN)),
            decode_copy(
                block.get(SEGMENT_HEADER_COPY_B..SEGMENT_HEADER_COPY_B + SEGMENT_HEADER_LEN),
            ),
        ]
    }
}

fn decode_copy(bytes: Option<&[u8]>) -> HeaderCopy {
    let Some(bytes) = bytes else {
        return HeaderCopy::Bad;
    };
    if bytes.iter().all(|&b| b == 0) {
        return HeaderCopy::Zero;
    }
    let stored = read_u32_le(bytes, 40).unwrap_or(0);
    if bytes[0..8] != SEGMENT_MAGIC || crc::mask(crc::crc32c(&bytes[..40])) != stored {
        return HeaderCopy::Bad;
    }
    let version = read_u32_le(bytes, 8).unwrap_or(0);
    if version != FORMAT_VERSION {
        return HeaderCopy::Unsupported(version);
    }
    HeaderCopy::Valid(SegmentHeader {
        segment: read_u64_le(bytes, 16).unwrap_or(0),
        epoch: read_u64_le(bytes, 24).unwrap_or(0),
        first_seq: read_u64_le(bytes, 32).unwrap_or(0),
    })
}

/// The fixed fields of a record header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub payload_crc: u32,
    pub payload_len: u32,
    pub kind: u8,
    pub flags: u8,
    pub scope_len: u8,
    pub epoch: u64,
    pub first_seq: Seq,
    pub count: u32,
    pub durable_seq: Seq,
}

impl RecordHeader {
    /// Total bytes of the record on disk.
    pub fn record_len(&self) -> u64 {
        RECORD_HEADER_LEN as u64 + u64::from(self.scope_len) + u64::from(self.payload_len)
    }

    /// Offset of the payload relative to the record start.
    pub fn payload_offset(&self) -> u64 {
        RECORD_HEADER_LEN as u64 + u64::from(self.scope_len)
    }

    /// Sequence after the last one this record takes.
    pub fn end_seq(&self) -> Seq {
        self.first_seq + u64::from(self.count)
    }

    pub fn record_type(&self) -> Option<RecordType> {
        RecordType::from_u8(self.kind)
    }

    pub fn is_pad(&self) -> bool {
        self.kind == RecordType::Pad as u8
    }

    fn decode_unchecked(h: &[u8]) -> RecordHeader {
        RecordHeader {
            payload_crc: read_u32_le(h, 4).unwrap_or(0),
            payload_len: read_u32_le(h, 8).unwrap_or(0),
            kind: h[12],
            flags: h[13],
            scope_len: h[14],
            epoch: read_u64_le(h, 16).unwrap_or(0),
            first_seq: read_u64_le(h, 24).unwrap_or(0),
            count: read_u32_le(h, 32).unwrap_or(0),
            durable_seq: read_u64_le(h, 40).unwrap_or(0),
        }
    }
}

/// Fields the writer chooses for one record.
#[derive(Clone, Copy, Debug)]
pub struct RecordFields {
    pub kind: RecordType,
    pub flags: u8,
    pub epoch: u64,
    pub first_seq: Seq,
    pub count: u32,
    pub durable_seq: Seq,
}

/// The payload of a record being encoded.
pub enum Payload<'a> {
    Bytes(&'a [u8]),
    Zeros(usize),
}

/// Appends one record to `out`. The caller checks that `scope` fits in a `u8` and the
/// payload in a `u32`.
pub fn encode_record(out: &mut Vec<u8>, fields: &RecordFields, scope: &[u8], payload: Payload<'_>) {
    let start = out.len();
    let payload_len = match payload {
        Payload::Bytes(bytes) => bytes.len(),
        Payload::Zeros(n) => n,
    };
    out.reserve(RECORD_HEADER_LEN + scope.len() + payload_len);
    out.resize(start + RECORD_HEADER_LEN, 0);
    out.extend_from_slice(scope);
    let payload_start = out.len();
    match payload {
        Payload::Bytes(bytes) => out.extend_from_slice(bytes),
        Payload::Zeros(n) => out.resize(payload_start + n, 0),
    }
    let payload_crc = crc::mask(crc::crc32c(&out[payload_start..]));
    let h = &mut out[start..start + RECORD_HEADER_LEN];
    h[4..8].copy_from_slice(&payload_crc.to_le_bytes());
    h[8..12].copy_from_slice(&(payload_len as u32).to_le_bytes());
    h[12] = fields.kind as u8;
    h[13] = fields.flags;
    h[14] = scope.len() as u8;
    h[16..24].copy_from_slice(&fields.epoch.to_le_bytes());
    h[24..32].copy_from_slice(&fields.first_seq.to_le_bytes());
    h[32..36].copy_from_slice(&fields.count.to_le_bytes());
    h[40..48].copy_from_slice(&fields.durable_seq.to_le_bytes());
    let header_crc = crc::mask(crc::crc32c(
        &out[start + 4..start + RECORD_HEADER_LEN + scope.len()],
    ));
    out[start..start + 4].copy_from_slice(&header_crc.to_le_bytes());
}

/// Smallest record a pad can be.
pub const MIN_PAD: u64 = RECORD_HEADER_LEN as u64;

/// Length of the pad record that moves `pos` to the next [`BLOCK`] boundary, or `None`
/// when `pos` is already aligned. A gap shorter than a record header is widened by one
/// block. The caller checks the result against the segment end.
pub fn align_pad_len(pos: u64) -> Option<u64> {
    let gap = (BLOCK - pos % BLOCK) % BLOCK;
    match gap {
        0 => None,
        g if g < MIN_PAD => Some(g + BLOCK),
        g => Some(g),
    }
}

pub fn align_up(pos: u64) -> u64 {
    pos.div_ceil(BLOCK) * BLOCK
}

/// Classification of the 48 header bytes plus the scope bytes that follow them.
#[derive(Debug, PartialEq, Eq)]
pub enum HeaderCheck {
    /// All 48 bytes are zero.
    Zero,
    /// Non-zero and the header CRC fails.
    Bad,
    Valid(RecordHeader),
}

/// Checks the record header at the start of `buf`, which holds the record's first
/// bytes: at least [`RECORD_HEADER_LEN`], and the scope too unless it lies beyond the
/// readable segment (then a non-zero header counts as `Bad`). On `Valid`, the scope is
/// `buf[RECORD_HEADER_LEN..][..scope_len]`.
pub fn check_header(buf: &[u8]) -> HeaderCheck {
    let h = &buf[..RECORD_HEADER_LEN];
    if h.iter().all(|&b| b == 0) {
        return HeaderCheck::Zero;
    }
    let header = RecordHeader::decode_unchecked(h);
    let Some(scope) = buf.get(RECORD_HEADER_LEN..RECORD_HEADER_LEN + usize::from(header.scope_len))
    else {
        return HeaderCheck::Bad;
    };
    let stored = read_u32_le(h, 0).unwrap_or(0);
    let crc = crc::extend(crc::crc32c(&h[4..]), scope);
    if crc::mask(crc) != stored {
        return HeaderCheck::Bad;
    }
    HeaderCheck::Valid(header)
}

pub fn payload_crc_ok(header: &RecordHeader, payload: &[u8]) -> bool {
    crc::mask(crc::crc32c(payload)) == header.payload_crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ns_unit_record_round_trips_and_detects_damage() {
        let mut out = Vec::new();
        let fields = RecordFields {
            kind: RecordType::Batch,
            flags: FLAG_SYNC,
            epoch: 3,
            first_seq: 10,
            count: 2,
            durable_seq: 9,
        };
        encode_record(&mut out, &fields, b"g1/", Payload::Bytes(b"payload"));
        assert_eq!(out.len(), RECORD_HEADER_LEN + 3 + 7);
        let check = check_header(&out);
        let HeaderCheck::Valid(h) = check else {
            panic!("expected a valid header, got {check:?}")
        };
        assert_eq!(
            &out[RECORD_HEADER_LEN..][..usize::from(h.scope_len)],
            b"g1/"
        );
        assert_eq!(
            (h.epoch, h.first_seq, h.count, h.durable_seq),
            (3, 10, 2, 9)
        );
        assert_eq!(h.record_len() as usize, out.len());
        assert!(payload_crc_ok(&h, &out[h.payload_offset() as usize..]));

        // A flipped scope byte breaks the header CRC; a flipped payload byte only the payload's.
        let mut bad = out.clone();
        bad[RECORD_HEADER_LEN] ^= 1;
        assert_eq!(check_header(&bad), HeaderCheck::Bad);
        assert_eq!(
            check_header(&out[..RECORD_HEADER_LEN + 1]),
            HeaderCheck::Bad
        );
        let mut bad = out.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(!payload_crc_ok(&h, &bad[h.payload_offset() as usize..]));
        assert_eq!(check_header(&[0u8; RECORD_HEADER_LEN]), HeaderCheck::Zero);
    }

    #[test]
    fn ns_unit_segment_header_copies() {
        let header = SegmentHeader {
            segment: 7,
            epoch: 2,
            first_seq: 100,
        };
        let blocks = header.encode_blocks();
        assert_eq!(blocks.len() as u64, RECORDS_START);
        assert_eq!(
            SegmentHeader::decode_blocks(&blocks),
            [HeaderCopy::Valid(header); 2]
        );
        let mut torn = blocks.clone();
        torn[5] ^= 0xFF;
        assert_eq!(
            SegmentHeader::decode_blocks(&torn),
            [HeaderCopy::Bad, HeaderCopy::Valid(header)]
        );
        // A whole 4 KiB unit destroyed: the copy in the other block survives.
        let mut destroyed = blocks.clone();
        destroyed[..BLOCK as usize].fill(0xA5);
        assert_eq!(
            SegmentHeader::decode_blocks(&destroyed),
            [HeaderCopy::Bad, HeaderCopy::Valid(header)]
        );
        let mut destroyed = blocks.clone();
        destroyed[BLOCK as usize..].fill(0);
        assert_eq!(
            SegmentHeader::decode_blocks(&destroyed),
            [HeaderCopy::Valid(header), HeaderCopy::Zero]
        );
        assert_eq!(
            SegmentHeader::decode_blocks(&blocks[..BLOCK as usize]),
            [HeaderCopy::Valid(header), HeaderCopy::Bad]
        );
        assert_eq!(
            SegmentHeader::decode_blocks(&vec![0u8; RECORDS_START as usize]),
            [HeaderCopy::Zero; 2]
        );
        assert_eq!(align_pad_len(4096), None);
        assert_eq!(align_pad_len(4096 + 10), Some(4086));
        assert_eq!(align_pad_len(8192 - 20), Some(20 + 4096));
    }
}
