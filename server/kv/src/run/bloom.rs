//! Whole-user-key bloom filter, cache-local.
//!
//! ```text
//! filter = line[num_lines] ‖ num_probes:u8          (line = 64 bytes = 512 bits)
//! ```
//!
//! One 64-bit hash per key: its low half picks a 64-byte line (`fastrange`), its high
//! half drives the probes inside that line (multiply by the golden ratio, take the top
//! 9 bits), so a lookup touches one CPU cache line however large the filter is. This is
//! the cache-local blocked bloom layout common to LSM engines. At 10 bits per key and 6 probes the false
//! positive rate is about 1%.

use crate::cache::fmix64;

/// A fast, non-keyed 64-bit hash (one MurmurHash3 x64 lane plus its finalizer). User keys
/// can come from clients, but a filter only decides whether to read a block, so a
/// crafted collision costs a block read and nothing else.
pub(crate) fn hash64(data: &[u8]) -> u64 {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let mut h = 0x9e37_79b9_7f4a_7c15 ^ (data.len() as u64).wrapping_mul(C1);
    let (chunks, rest) = data.as_chunks::<8>();
    for chunk in chunks {
        let k = u64::from_le_bytes(*chunk);
        h ^= k.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h = h.rotate_left(27).wrapping_mul(5).wrapping_add(0x52dc_e729);
    }
    if !rest.is_empty() {
        let mut tail = [0u8; 8];
        tail[..rest.len()].copy_from_slice(rest);
        let k = u64::from_le_bytes(tail);
        h ^= k.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    }
    fmix64(h)
}

const LINE_BYTES: usize = 64;

/// The line a hash selects, among `num_lines`.
fn line_of(hash: u64, num_lines: usize) -> usize {
    ((u64::from(hash as u32) * num_lines as u64) >> 32) as usize
}

/// Calls `probe(bit)` for each of the `num_probes` bit positions (0..512) of a hash.
#[inline]
fn for_each_probe(hash: u64, num_probes: u8, mut probe: impl FnMut(usize) -> bool) -> bool {
    let mut h = (hash >> 32) as u32;
    for _ in 0..num_probes {
        if !probe((h >> 23) as usize) {
            return false;
        }
        h = h.wrapping_mul(0x9e37_79b9);
    }
    true
}

/// Collects one hash per distinct user key; the filter is sized at [`finish_into`].
///
/// [`finish_into`]: BloomBuilder::finish_into
pub(crate) struct BloomBuilder {
    bits_per_key: usize,
    hashes: Vec<u64>,
}

impl BloomBuilder {
    pub(crate) fn new(bits_per_key: usize) -> BloomBuilder {
        BloomBuilder {
            bits_per_key: bits_per_key.max(1),
            hashes: Vec::new(),
        }
    }

    pub(crate) fn add_key(&mut self, user_key: &[u8]) {
        self.hashes.push(hash64(user_key));
    }

    /// Forgets every key (the next filter partition starts empty).
    pub(crate) fn clear(&mut self) {
        self.hashes.clear();
    }

    pub(crate) fn finish_into(&self, out: &mut Vec<u8>) {
        let num_probes = ((self.bits_per_key * 69) / 100).clamp(1, 30) as u8;
        let num_lines = (self.hashes.len() * self.bits_per_key)
            .div_ceil(LINE_BYTES * 8)
            .max(1);
        let start = out.len();
        out.resize(start + num_lines * LINE_BYTES, 0);
        let lines = &mut out[start..];
        for &hash in &self.hashes {
            let line = &mut lines[line_of(hash, num_lines) * LINE_BYTES..][..LINE_BYTES];
            for_each_probe(hash, num_probes, |bit| {
                line[bit / 8] |= 1 << (bit % 8);
                true
            });
        }
        out.push(num_probes);
    }
}

/// False only when `user_key` was certainly not added. A filter this build does not
/// understand answers true, which costs a block read and never a wrong result.
pub(crate) fn may_contain(filter: &[u8], user_key: &[u8]) -> bool {
    let Some((&num_probes, lines)) = filter.split_last() else {
        return true;
    };
    if lines.is_empty() || lines.len() % LINE_BYTES != 0 || num_probes == 0 || num_probes > 30 {
        return true;
    }
    let hash = hash64(user_key);
    let line = &lines[line_of(hash, lines.len() / LINE_BYTES) * LINE_BYTES..][..LINE_BYTES];
    for_each_probe(hash, num_probes, |bit| {
        line[bit / 8] & (1 << (bit % 8)) != 0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_has_no_false_negatives_and_about_one_percent_false_positives() {
        let mut builder = BloomBuilder::new(10);
        let n = 20_000u32;
        for i in 0..n {
            builder.add_key(format!("user/{i}").as_bytes());
        }
        let mut filter = Vec::new();
        builder.finish_into(&mut filter);
        assert_eq!(filter.len(), (n as usize * 10).div_ceil(512) * 64 + 1);
        for i in 0..n {
            assert!(may_contain(&filter, format!("user/{i}").as_bytes()));
        }
        let trials = 200_000u32;
        let false_positives = (0..trials)
            .filter(|i| may_contain(&filter, format!("other/{i}").as_bytes()))
            .count();
        let rate = false_positives as f64 / f64::from(trials);
        eprintln!("bloom false positive rate at 10 bits/key: {rate}");
        assert!(rate < 0.0125, "false positive rate {rate}");
    }

    #[test]
    fn empty_and_unknown_filters() {
        let mut filter = Vec::new();
        BloomBuilder::new(10).finish_into(&mut filter);
        assert_eq!(filter.len(), 65);
        assert!(!may_contain(&filter, b"anything"));
        assert!(may_contain(&[], b"x"));
        assert!(may_contain(
            &[0xFF; 65][..64]
                .iter()
                .chain(&[0])
                .copied()
                .collect::<Vec<_>>(),
            b"x"
        ));
        assert!(may_contain(
            &[0u8; 65][..64]
                .iter()
                .chain(&[31])
                .copied()
                .collect::<Vec<_>>(),
            b"x"
        ));
        assert!(may_contain(&[0, 6], b"x"), "not a whole number of lines");
    }

    #[test]
    fn hash_depends_on_every_byte_and_the_length() {
        let base = hash64(b"abcdefghij");
        assert_ne!(base, hash64(b"abcdefghik"));
        assert_ne!(base, hash64(b"bbcdefghij"));
        assert_ne!(hash64(b"a"), hash64(b"a\0"));
        assert_ne!(hash64(b""), hash64(b"\0"));
    }
}
