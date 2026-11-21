//! Key-value blocks with prefix-compressed keys and restart points.
//!
//! ```text
//! block   = entry* ‖ restart:u32le* ‖ num_restarts:u32le
//! entry   = varint32 shared ‖ varint32 non_shared ‖ varint32 value_len
//!           ‖ key[shared..] ‖ value
//! ```
//!
//! Every `restart_interval`-th entry stores its whole key (`shared == 0`) and its offset
//! goes in the restart array, so a seek binary-searches the restarts and scans at most
//! one interval. On disk each block is followed by a masked CRC32C of its contents; the
//! reader checks it before building a [`BlockIter`].

use std::cmp::Ordering;

use bytes::Bytes;

use crate::coding::{put_varint32, read_u32_le};
use crate::types::TRAILER_LEN;

pub(crate) struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    interval: usize,
    counter: usize,
    entries: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub(crate) fn new(restart_interval: usize) -> BlockBuilder {
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            interval: restart_interval.max(1),
            counter: 0,
            entries: 0,
            last_key: Vec::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Encoded size if the block were finished now.
    pub(crate) fn size_estimate(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.counter < self.interval {
            common_prefix(&self.last_key, key)
        } else {
            self.restarts
                .push(u32::try_from(self.buf.len()).expect("block larger than u32::MAX"));
            self.counter = 0;
            0
        };
        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, (key.len() - shared) as u32);
        put_varint32(&mut self.buf, value.len() as u32);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);
        self.counter += 1;
        self.entries += 1;
    }

    /// Appends the finished block to `out` and resets the builder for the next one.
    pub(crate) fn finish_into(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.buf);
        for restart in &self.restarts {
            out.extend_from_slice(&restart.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.buf.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.entries = 0;
        self.last_key.clear();
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// A seek target: an internal key split into its user key and trailer, compared like
/// [`crate::types::compare_internal_keys`] without building the encoded key.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Target<'a> {
    pub(crate) user_key: &'a [u8],
    pub(crate) trailer: u64,
}

impl<'a> Target<'a> {
    pub(crate) fn from_internal(ikey: &'a [u8]) -> Target<'a> {
        let split = ikey.len().saturating_sub(TRAILER_LEN);
        Target {
            user_key: &ikey[..split],
            trailer: trailer_of(ikey),
        }
    }

    /// Where `ikey` sorts relative to this target.
    pub(crate) fn compare_key(&self, ikey: &[u8]) -> Ordering {
        let split = ikey.len().saturating_sub(TRAILER_LEN);
        ikey[..split]
            .cmp(self.user_key)
            .then_with(|| self.trailer.cmp(&trailer_of(ikey)))
    }
}

/// The trailer of an internal key, zero-extended on the left when the key is shorter
/// than a trailer (the same rule as `compare_internal_keys`).
fn trailer_of(ikey: &[u8]) -> u64 {
    let start = ikey.len().saturating_sub(TRAILER_LEN);
    let mut bytes = [0u8; TRAILER_LEN];
    bytes[TRAILER_LEN - (ikey.len() - start)..].copy_from_slice(&ikey[start..]);
    u64::from_be_bytes(bytes)
}

/// Walks one block. Keys that are stored whole are borrowed from the block; a key is
/// copied only when the next one shares a prefix with it. Values are zero-copy slices.
///
/// Structural damage that passed the CRC (only a bug can produce it) makes the iterator
/// invalid with [`BlockIter::corruption`] set; it never panics on bad bytes.
pub(crate) struct BlockIter {
    data: Bytes,
    /// Offset of the restart array, which is also the end of the entries.
    restarts: usize,
    num_restarts: u32,
    /// Offset of the current entry; `== restarts` when not positioned.
    current: usize,
    /// Offset just past the current entry.
    next: usize,
    restart_index: u32,
    key_buf: Vec<u8>,
    /// When set, the current key is `data[a..b]` and `key_buf` is stale.
    key_in_place: Option<(usize, usize)>,
    value: (usize, usize),
    corruption: Option<&'static str>,
}

impl BlockIter {
    pub(crate) fn new(data: Bytes) -> Result<BlockIter, &'static str> {
        let len = data.len();
        if len < 4 {
            return Err("block shorter than its restart count");
        }
        let num_restarts = read_u32_le(&data, len - 4).ok_or("bad restart count")?;
        let restarts = (num_restarts as usize)
            .checked_mul(4)
            .and_then(|bytes| (len - 4).checked_sub(bytes))
            .ok_or("restart array larger than the block")?;
        if num_restarts == 0 {
            return Err("block without restart points");
        }
        Ok(BlockIter {
            data,
            restarts,
            num_restarts,
            current: restarts,
            next: restarts,
            restart_index: 0,
            key_buf: Vec::new(),
            key_in_place: None,
            value: (0, 0),
            corruption: None,
        })
    }

    pub(crate) fn valid(&self) -> bool {
        self.current < self.restarts && self.corruption.is_none()
    }

    pub(crate) fn corruption(&self) -> Option<&'static str> {
        self.corruption
    }

    pub(crate) fn key(&self) -> &[u8] {
        debug_assert!(self.valid());
        match self.key_in_place {
            Some((start, end)) => &self.data[start..end],
            None => &self.key_buf,
        }
    }

    pub(crate) fn value(&self) -> Bytes {
        debug_assert!(self.valid());
        self.data.slice(self.value.0..self.value.1)
    }

    pub(crate) fn value_slice(&self) -> &[u8] {
        &self.data[self.value.0..self.value.1]
    }

    pub(crate) fn seek_to_first(&mut self) {
        self.seek_to_restart(0);
        self.parse_next();
    }

    pub(crate) fn seek_to_last(&mut self) {
        self.seek_to_restart(self.num_restarts - 1);
        while self.parse_next() && self.next < self.restarts {}
    }

    pub(crate) fn next(&mut self) {
        debug_assert!(self.valid());
        self.parse_next();
    }

    pub(crate) fn prev(&mut self) {
        debug_assert!(self.valid());
        let original = self.current;
        loop {
            let Some(point) = self.restart_point(self.restart_index) else {
                return;
            };
            if point < original {
                break;
            }
            if self.restart_index == 0 {
                self.invalidate();
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart(self.restart_index);
        while self.parse_next() && self.next < original {}
    }

    /// Positions at the first entry whose key is `>= target`.
    pub(crate) fn seek(&mut self, target: Target<'_>) {
        // Find the last restart whose key is < target; every entry before it is too.
        let mut left = 0u32;
        let mut right = self.num_restarts - 1;
        while left < right {
            let mid = left + (right - left).div_ceil(2);
            let Some(key) = self.restart_key(mid) else {
                return;
            };
            if target.compare_key(key) == Ordering::Less {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart(left);
        while self.parse_next() {
            if target.compare_key(self.key()) != Ordering::Less {
                return;
            }
        }
    }

    fn invalidate(&mut self) {
        self.current = self.restarts;
        self.next = self.restarts;
        self.key_in_place = None;
        self.key_buf.clear();
    }

    fn corrupt(&mut self, what: &'static str) -> bool {
        self.corruption = Some(what);
        self.invalidate();
        false
    }

    fn restart_point(&mut self, index: u32) -> Option<usize> {
        match read_u32_le(&self.data, self.restarts + 4 * index as usize) {
            // An empty block has one restart point, at the end of its (zero) entries.
            Some(point) if (point as usize) <= self.restarts => Some(point as usize),
            _ => {
                self.corrupt("restart point out of range");
                None
            }
        }
    }

    /// The whole key stored at restart `index`.
    fn restart_key(&mut self, index: u32) -> Option<&[u8]> {
        let at = self.restart_point(index)?;
        match decode_header(&self.data[at..self.restarts]) {
            Some((0, key_len, _, used)) if at + used + key_len <= self.restarts => {
                Some(&self.data[at + used..at + used + key_len])
            }
            _ => {
                self.corrupt("bad entry at a restart point");
                None
            }
        }
    }

    fn seek_to_restart(&mut self, index: u32) {
        self.key_in_place = None;
        self.key_buf.clear();
        self.restart_index = index;
        match self.restart_point(index) {
            Some(point) => {
                self.current = point;
                self.next = point;
            }
            None => self.invalidate(),
        }
    }

    /// Decodes the entry at `self.next`. Returns false at the end of the block or on damage.
    fn parse_next(&mut self) -> bool {
        if self.corruption.is_some() {
            return false;
        }
        self.current = self.next;
        if self.current >= self.restarts {
            self.invalidate();
            return false;
        }
        let at = self.current;
        let Some((shared, non_shared, value_len, used)) =
            decode_header(&self.data[at..self.restarts])
        else {
            return self.corrupt("bad block entry header");
        };
        let key_start = at + used;
        let Some(value_end) = key_start
            .checked_add(non_shared)
            .and_then(|v| v.checked_add(value_len))
            .filter(|&end| end <= self.restarts)
        else {
            return self.corrupt("block entry overruns the block");
        };
        let key_end = key_start + non_shared;
        if shared == 0 {
            self.key_in_place = Some((key_start, key_end));
        } else {
            if let Some((start, end)) = self.key_in_place.take() {
                self.key_buf.clear();
                // One allocation per iterator in the common case, not one per growth.
                self.key_buf.reserve((end - start + non_shared).max(64));
                self.key_buf.extend_from_slice(&self.data[start..end]);
            }
            if shared > self.key_buf.len() {
                return self.corrupt("block entry shares more than the previous key");
            }
            self.key_buf.truncate(shared);
            self.key_buf
                .extend_from_slice(&self.data[key_start..key_end]);
        }
        self.value = (key_end, value_end);
        self.next = value_end;
        while self.restart_index + 1 < self.num_restarts {
            match self.restart_point(self.restart_index + 1) {
                Some(point) if point <= self.current => self.restart_index += 1,
                Some(_) => break,
                None => return false,
            }
        }
        true
    }
}

/// `(shared, non_shared, value_len, header_bytes)`. Most headers are three one-byte
/// varints, which take the fast path.
#[inline]
fn decode_header(input: &[u8]) -> Option<(usize, usize, usize, usize)> {
    if let [a, b, c, ..] = *input
        && (a | b | c) < 0x80
    {
        return Some((usize::from(a), usize::from(b), usize::from(c), 3));
    }
    let mut at = 0;
    let shared = varint32(input, &mut at)?;
    let non_shared = varint32(input, &mut at)?;
    let value_len = varint32(input, &mut at)?;
    Some((shared as usize, non_shared as usize, value_len as usize, at))
}

/// Same encoding as [`crate::coding::get_varint32`], decoded in place.
#[inline]
fn varint32(input: &[u8], at: &mut usize) -> Option<u32> {
    let mut value = 0u64;
    for shift in [0, 7, 14, 21, 28] {
        let byte = *input.get(*at)?;
        *at += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte < 0x80 {
            return u32::try_from(value).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Kind, internal_key, seek_key};

    fn build(entries: &[(Vec<u8>, Vec<u8>)], interval: usize) -> Bytes {
        let mut builder = BlockBuilder::new(interval);
        for (k, v) in entries {
            builder.add(k, v);
        }
        let mut out = Vec::new();
        let estimate = builder.size_estimate();
        builder.finish_into(&mut out);
        assert_eq!(out.len(), estimate);
        assert!(builder.is_empty());
        Bytes::from(out)
    }

    fn entries(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    internal_key(
                        format!("key{:05}", i / 2).as_bytes(),
                        100 - (i % 2) as u64,
                        Kind::Put,
                    ),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect()
    }

    #[test]
    fn block_iterates_both_ways_and_seeks() {
        for interval in [1, 2, 16] {
            for n in [1, 2, 15, 16, 17, 100] {
                let entries = entries(n);
                let mut it = BlockIter::new(build(&entries, interval)).unwrap();
                it.seek_to_first();
                let mut forward = Vec::new();
                while it.valid() {
                    forward.push((it.key().to_vec(), it.value().to_vec()));
                    it.next();
                }
                assert_eq!(forward, entries);
                it.seek_to_last();
                let mut backward = Vec::new();
                while it.valid() {
                    backward.push((it.key().to_vec(), it.value().to_vec()));
                    it.prev();
                }
                backward.reverse();
                assert_eq!(backward, entries);
                assert!(it.corruption().is_none());

                for i in 0..n / 2 + 1 {
                    let user = format!("key{i:05}");
                    // Seeking at sequence 100 lands on the newest version.
                    let target = seek_key(user.as_bytes(), 100);
                    it.seek(Target::from_internal(&target));
                    let expected = entries
                        .iter()
                        .position(|(k, _)| crate::types::compare_internal_keys(k, &target).is_ge());
                    match expected {
                        Some(pos) => {
                            assert!(it.valid());
                            assert_eq!(it.key(), &entries[pos].0[..]);
                        }
                        None => assert!(!it.valid()),
                    }
                }
            }
        }
    }

    #[test]
    fn headers_decode_like_the_shared_varints() {
        for (shared, non_shared, value_len) in [
            (0u32, 0u32, 0u32),
            (1, 127, 128),
            (300, 5, u32::MAX),
            (127, 127, 127),
        ] {
            let mut buf = Vec::new();
            put_varint32(&mut buf, shared);
            put_varint32(&mut buf, non_shared);
            put_varint32(&mut buf, value_len);
            let used = buf.len();
            buf.extend_from_slice(b"tail");
            assert_eq!(
                decode_header(&buf),
                Some((
                    shared as usize,
                    non_shared as usize,
                    value_len as usize,
                    used
                ))
            );
            assert_eq!(decode_header(&buf[..used - 1]), None);
        }
        // More than 32 bits.
        assert_eq!(decode_header(&[0x80, 0x80, 0x80, 0x80, 0x10, 0, 0]), None);
        assert_eq!(decode_header(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]), None);
    }

    #[test]
    fn empty_block_is_valid_and_empty() {
        let mut it = BlockIter::new(build(&[], 16)).unwrap();
        it.seek_to_first();
        assert!(!it.valid());
        it.seek_to_last();
        assert!(!it.valid());
        it.seek(Target::from_internal(&seek_key(b"a", 1)));
        assert!(!it.valid());
        assert!(it.corruption().is_none());
    }

    #[test]
    fn damaged_blocks_report_corruption_without_panicking() {
        let block = build(&entries(40), 4);
        assert!(BlockIter::new(Bytes::from_static(&[1, 0])).is_err());
        assert!(BlockIter::new(Bytes::from_static(&[0, 0, 0, 0])).is_err());
        assert!(BlockIter::new(Bytes::from_static(&[9, 0, 0, 0])).is_err());
        // Every single-byte change either keeps iteration well formed or reports
        // corruption; it never panics.
        for at in 0..block.len() {
            for delta in [1u8, 0x80] {
                let mut bytes = block.to_vec();
                bytes[at] ^= delta;
                let Ok(mut it) = BlockIter::new(Bytes::from(bytes)) else {
                    continue;
                };
                it.seek_to_first();
                let mut steps = 0;
                while it.valid() && steps < 1000 {
                    it.next();
                    steps += 1;
                }
                it.seek_to_last();
                while it.valid() && steps < 2000 {
                    it.prev();
                    steps += 1;
                }
                it.seek(Target::from_internal(&seek_key(b"key00007", 5)));
            }
        }
    }
}
