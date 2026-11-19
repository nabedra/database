//! Shared contract: sequence numbers, entry kinds, internal keys and journal locators.
//!
//! Every module encodes keys through this file. Changing it needs the lead.
//!
//! An *internal key* is `user_key ‖ trailer`, where `trailer` is the big-endian `u64`
//! `(seq << 8) | kind`. Internal keys order by user key ascending (bytewise), then by
//! trailer descending, so the newest version of a key comes first. Because user keys
//! have different lengths, encoded internal keys must be compared with
//! [`compare_internal_keys`], never bytewise.

use std::cmp::Ordering;

/// A commit sequence number. Each operation of a batch takes its own number:
/// operation `i` of a batch whose first number is `s` has sequence `s + i`.
pub type Seq = u64;

/// Sequence numbers use the high 56 bits of a trailer.
pub const MAX_SEQ: Seq = (1 << 56) - 1;

/// Length of the trailer appended to every user key.
pub const TRAILER_LEN: usize = 8;

/// Kind byte used only by [`seek_key`]. It sorts before every real kind of the same
/// sequence and is never stored.
pub const SEEK_KIND_BYTE: u8 = 0xFF;

/// What an entry means.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// A point tombstone.
    Delete = 0,
    /// A value stored inline.
    Put = 1,
    /// The value is an encoded [`Locator`] into the journal. Reads resolve it.
    JournalPut = 2,
    /// A range tombstone: the user key is the inclusive start, the value the exclusive end.
    RangeDelete = 3,
}

impl Kind {
    pub fn from_u8(byte: u8) -> Option<Kind> {
        match byte {
            0 => Some(Kind::Delete),
            1 => Some(Kind::Put),
            2 => Some(Kind::JournalPut),
            3 => Some(Kind::RangeDelete),
            _ => None,
        }
    }
}

pub fn pack_trailer(seq: Seq, kind: Kind) -> u64 {
    debug_assert!(seq <= MAX_SEQ, "sequence {seq} exceeds MAX_SEQ");
    (seq << 8) | kind as u64
}

pub fn unpack_trailer(trailer: u64) -> Option<(Seq, Kind)> {
    Some((trailer >> 8, Kind::from_u8((trailer & 0xFF) as u8)?))
}

/// Appends `user_key ‖ trailer(seq, kind)` to `out`.
pub fn encode_internal_key(out: &mut Vec<u8>, user_key: &[u8], seq: Seq, kind: Kind) {
    out.reserve(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&pack_trailer(seq, kind).to_be_bytes());
}

pub fn internal_key(user_key: &[u8], seq: Seq, kind: Kind) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    encode_internal_key(&mut out, user_key, seq, kind);
    out
}

/// The smallest internal key for `user_key` visible at snapshot `seq`: seeking to it
/// lands on the newest version with sequence `<= seq`.
pub fn seek_key(user_key: &[u8], seq: Seq) -> Vec<u8> {
    debug_assert!(seq <= MAX_SEQ);
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&((seq << 8) | u64::from(SEEK_KIND_BYTE)).to_be_bytes());
    out
}

/// A decoded internal key borrowing its user key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedKey<'a> {
    pub user_key: &'a [u8],
    pub seq: Seq,
    pub kind: Kind,
}

/// Splits an internal key. `None` when it is shorter than a trailer or has an unknown
/// kind (including [`SEEK_KIND_BYTE`]); callers treat that as corruption.
pub fn parse_internal_key(ikey: &[u8]) -> Option<ParsedKey<'_>> {
    let split = ikey.len().checked_sub(TRAILER_LEN)?;
    let (user_key, trailer) = ikey.split_at(split);
    let (seq, kind) = unpack_trailer(u64::from_be_bytes(trailer.try_into().ok()?))?;
    Some(ParsedKey {
        user_key,
        seq,
        kind,
    })
}

/// The user key of an internal key. Panics in debug builds when `ikey` is too short.
pub fn user_key(ikey: &[u8]) -> &[u8] {
    debug_assert!(ikey.len() >= TRAILER_LEN);
    &ikey[..ikey.len().saturating_sub(TRAILER_LEN)]
}

fn trailer(ikey: &[u8]) -> u64 {
    let start = ikey.len().saturating_sub(TRAILER_LEN);
    let mut bytes = [0u8; TRAILER_LEN];
    bytes[TRAILER_LEN - (ikey.len() - start)..].copy_from_slice(&ikey[start..]);
    u64::from_be_bytes(bytes)
}

/// Orders encoded internal keys: user key ascending, then trailer descending.
pub fn compare_internal_keys(a: &[u8], b: &[u8]) -> Ordering {
    user_key(a)
        .cmp(user_key(b))
        .then_with(|| trailer(b).cmp(&trailer(a)))
}

/// Where a journal value lives. Stored (encoded) as the value of a [`Kind::JournalPut`]
/// entry. `crc32c` is the unmasked [`crate::crc::crc32c`] of the value bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Locator {
    /// Journal segment file number.
    pub segment: u64,
    /// Reuse epoch of that segment file when the value was written.
    pub epoch: u64,
    /// Byte offset of the value inside the segment file.
    pub offset: u64,
    pub len: u32,
    pub crc32c: u32,
}

impl Locator {
    pub const LEN: usize = 32;

    pub fn encode(&self) -> [u8; Self::LEN] {
        let mut out = [0u8; Self::LEN];
        out[0..8].copy_from_slice(&self.segment.to_le_bytes());
        out[8..16].copy_from_slice(&self.epoch.to_le_bytes());
        out[16..24].copy_from_slice(&self.offset.to_le_bytes());
        out[24..28].copy_from_slice(&self.len.to_le_bytes());
        out[28..32].copy_from_slice(&self.crc32c.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Locator> {
        if bytes.len() != Self::LEN {
            return None;
        }
        let u64_at = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let u32_at = |i: usize| u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        Some(Locator {
            segment: u64_at(0),
            epoch: u64_at(8),
            offset: u64_at(16),
            len: u32_at(24),
            crc32c: u32_at(28),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_keys_order_by_user_key_then_newest_first() {
        let mut keys = [
            internal_key(b"abc", 5, Kind::Put),
            internal_key(b"ab", 1, Kind::Put),
            internal_key(b"ab", 9, Kind::Delete),
            internal_key(b"b", 0, Kind::Put),
            internal_key(b"", 3, Kind::Put),
        ];
        keys.sort_by(|a, b| compare_internal_keys(a, b));
        let parsed: Vec<_> = keys
            .iter()
            .map(|k| {
                let p = parse_internal_key(k).unwrap();
                (p.user_key.to_vec(), p.seq)
            })
            .collect();
        assert_eq!(
            parsed,
            vec![
                (b"".to_vec(), 3),
                (b"ab".to_vec(), 9),
                (b"ab".to_vec(), 1),
                (b"abc".to_vec(), 5),
                (b"b".to_vec(), 0),
            ]
        );
    }

    #[test]
    fn seek_key_sorts_before_every_entry_at_or_below_its_sequence() {
        let seek = seek_key(b"k", 7);
        assert_eq!(
            compare_internal_keys(&seek, &internal_key(b"k", 7, Kind::RangeDelete)),
            Ordering::Less
        );
        assert_eq!(
            compare_internal_keys(&seek, &internal_key(b"k", 8, Kind::Put)),
            Ordering::Greater
        );
        assert!(parse_internal_key(&seek).is_none());
    }

    #[test]
    fn locator_round_trips() {
        let l = Locator {
            segment: 7,
            epoch: 3,
            offset: 1 << 40,
            len: 99,
            crc32c: 0xABCD,
        };
        assert_eq!(Locator::decode(&l.encode()), Some(l));
        assert_eq!(Locator::decode(&[0; 31]), None);
    }
}
