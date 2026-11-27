//! On-disk layout of journal segments and records (plan section 2, "Journal format").
//!
//! ```text
//! segment = header block A (4 KiB) ‖ header block B (4 KiB) ‖ record*   (records at 8192)
//! header block: one header copy at its start, zeros elsewhere
//! header copy = magic "NBKVJNL1" | version u32 | key_check u32 | segment u64 | epoch u64
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
//! **Frames.** The segment header's `version` says how the record headers of that
//! segment are protected. Version 1 (the CRC frame) is the layout above. Version 2 (the
//! authenticated frame, LSM-1) has the same 48 bytes, but `header_crc` (bytes 0..4) and
//! the reserved word (bytes 36..40) hold the low and high halves of a 64-bit SipHash-2-4
//! tag, under the store's journal key, of header bytes 4..36, 40..48 and the scope
//! ([`crate::mac`]). The payload CRC is the same in both frames. A version 2 segment
//! exists only in a format 2 store (`FORMAT` "nabedra-kv 2", plus `JOURNAL_KEY`); a
//! format 2 store may still hold version 1 segments written before its upgrade, and
//! never writes a version 2 record into a version 1 segment.
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
use crate::mac::JournalKey;
use crate::types::Seq;

/// Alignment unit: the header block size, and the boundary every group commit that
/// follows a sync starts on.
pub const BLOCK: u64 = 4096;
/// Offset of the first record: after the two header blocks.
pub const RECORDS_START: u64 = 2 * BLOCK;
pub const SEGMENT_MAGIC: [u8; 8] = *b"NBKVJNL1";
/// Segment version of the CRC frame.
pub const FORMAT_VERSION: u32 = 1;
/// Segment version of the authenticated frame.
pub const AUTH_FORMAT_VERSION: u32 = 2;
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
    /// The segment's records use the authenticated frame (version 2).
    pub auth: bool,
    /// Version 2 only (header bytes 12..16, zero in version 1): [`key_check`] of the
    /// key the records are tagged with, so a store whose `JOURNAL_KEY` is not the one
    /// its segments were written under refuses to open instead of reading every
    /// record as damage.
    pub key_check: u32,
}

/// A 32-bit check value of `key` for segment `segment` at `epoch`. It identifies the key;
/// it is not what keeps forged records out (the record tags are).
pub fn key_check(key: &JournalKey, segment: u64, epoch: u64) -> u32 {
    key.tag(&[
        b"NBKVJNL2 key check",
        &segment.to_le_bytes(),
        &epoch.to_le_bytes(),
    ]) as u32
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
        let version = if self.auth {
            AUTH_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        };
        out[0..8].copy_from_slice(&SEGMENT_MAGIC);
        out[8..12].copy_from_slice(&version.to_le_bytes());
        out[12..16].copy_from_slice(&self.key_check.to_le_bytes());
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
    if version != FORMAT_VERSION && version != AUTH_FORMAT_VERSION {
        return HeaderCopy::Unsupported(version);
    }
    HeaderCopy::Valid(SegmentHeader {
        segment: read_u64_le(bytes, 16).unwrap_or(0),
        epoch: read_u64_le(bytes, 24).unwrap_or(0),
        first_seq: read_u64_le(bytes, 32).unwrap_or(0),
        auth: version == AUTH_FORMAT_VERSION,
        key_check: if version == AUTH_FORMAT_VERSION {
            read_u32_le(bytes, 12).unwrap_or(0)
        } else {
            0
        },
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

/// Appends one record to `out`, in the authenticated frame under `key`, else in the CRC
/// frame. The caller checks that `scope` fits in a `u8` and the payload in a `u32`.
///
/// Every record the journal writes has `durable_seq < first_seq` (the last completed
/// sync covers only earlier records); recovery relies on it (LSM-1).
pub fn encode_record(
    out: &mut Vec<u8>,
    fields: &RecordFields,
    scope: &[u8],
    payload: Payload<'_>,
    key: Option<&JournalKey>,
) {
    debug_assert!(fields.durable_seq < fields.first_seq, "{fields:?}");
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
    match key {
        None => {
            let header_crc = crc::mask(crc::crc32c(
                &out[start + 4..start + RECORD_HEADER_LEN + scope.len()],
            ));
            out[start..start + 4].copy_from_slice(&header_crc.to_le_bytes());
        }
        Some(key) => {
            let h = &out[start..start + RECORD_HEADER_LEN];
            let tag = key.tag(&[&h[4..36], &h[40..48], scope]);
            out[start..start + 4].copy_from_slice(&(tag as u32).to_le_bytes());
            out[start + 36..start + 40].copy_from_slice(&((tag >> 32) as u32).to_le_bytes());
        }
    }
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
/// readable segment (then a non-zero header counts as `Bad`). `key` is the journal key
/// for a segment in the authenticated frame, `None` for the CRC frame. On `Valid`, the
/// scope is `buf[RECORD_HEADER_LEN..][..scope_len]`.
pub fn check_header(buf: &[u8], key: Option<&JournalKey>) -> HeaderCheck {
    let h = &buf[..RECORD_HEADER_LEN];
    if h.iter().all(|&b| b == 0) {
        return HeaderCheck::Zero;
    }
    let header = RecordHeader::decode_unchecked(h);
    let Some(scope) = buf.get(RECORD_HEADER_LEN..RECORD_HEADER_LEN + usize::from(header.scope_len))
    else {
        return HeaderCheck::Bad;
    };
    let low = read_u32_le(h, 0).unwrap_or(0);
    let ok = match key {
        None => crc::mask(crc::extend(crc::crc32c(&h[4..]), scope)) == low,
        Some(key) => {
            let high = read_u32_le(h, 36).unwrap_or(0);
            key.tag(&[&h[4..36], &h[40..48], scope]) == (u64::from(high) << 32 | u64::from(low))
        }
    };
    if !ok {
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
        encode_record(&mut out, &fields, b"g1/", Payload::Bytes(b"payload"), None);
        assert_eq!(out.len(), RECORD_HEADER_LEN + 3 + 7);
        let check = check_header(&out, None);
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
        assert_eq!(check_header(&bad, None), HeaderCheck::Bad);
        assert_eq!(
            check_header(&out[..RECORD_HEADER_LEN + 1], None),
            HeaderCheck::Bad
        );
        let mut bad = out.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(!payload_crc_ok(&h, &bad[h.payload_offset() as usize..]));
        assert_eq!(
            check_header(&[0u8; RECORD_HEADER_LEN], None),
            HeaderCheck::Zero
        );
    }

    /// LSM-1: the authenticated frame. A header checks only under its own key, never as a
    /// CRC frame, and a CRC-frame header (what client bytes could forge) never checks as
    /// an authenticated one.
    #[test]
    fn ns_unit_authenticated_frame_needs_the_key() {
        let key = JournalKey::from_bytes(*b"fedcba9876543210");
        let other = JournalKey::from_bytes(*b"fedcba9876543211");
        let fields = RecordFields {
            kind: RecordType::Batch,
            flags: FLAG_SYNC,
            epoch: 3,
            first_seq: 10,
            count: 2,
            durable_seq: 9,
        };
        let mut auth = Vec::new();
        encode_record(
            &mut auth,
            &fields,
            b"g1/",
            Payload::Bytes(b"payload"),
            Some(&key),
        );
        let mut plain = Vec::new();
        encode_record(
            &mut plain,
            &fields,
            b"g1/",
            Payload::Bytes(b"payload"),
            None,
        );
        assert_eq!(auth.len(), plain.len(), "same layout, same length");
        let HeaderCheck::Valid(h) = check_header(&auth, Some(&key)) else {
            panic!("the key's own header must check")
        };
        assert_eq!((h.first_seq, h.count, h.durable_seq), (10, 2, 9));
        assert!(payload_crc_ok(&h, &auth[h.payload_offset() as usize..]));
        assert_eq!(check_header(&auth, Some(&other)), HeaderCheck::Bad);
        assert_eq!(check_header(&auth, None), HeaderCheck::Bad);
        assert_eq!(check_header(&plain, Some(&key)), HeaderCheck::Bad);
        for i in 0..RECORD_HEADER_LEN + 3 {
            let mut bad = auth.clone();
            bad[i] ^= 0x01;
            assert_eq!(check_header(&bad, Some(&key)), HeaderCheck::Bad, "byte {i}");
        }
        let header = SegmentHeader {
            segment: 7,
            epoch: 2,
            first_seq: 100,
            auth: true,
            key_check: key_check(&key, 7, 2),
        };
        assert_eq!(
            SegmentHeader::decode_blocks(&header.encode_blocks()),
            [HeaderCopy::Valid(header); 2]
        );
    }

    #[test]
    fn ns_unit_segment_header_copies() {
        let header = SegmentHeader {
            segment: 7,
            epoch: 2,
            first_seq: 100,
            auth: false,
            key_check: 0,
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
