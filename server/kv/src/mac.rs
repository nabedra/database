//! The journal key and the record tag of authenticated journal frames (format 2, LSM-1).
//!
//! A format 2 store keeps a 128-bit random key in `kv/JOURNAL_KEY`. Every record header of
//! a format 2 journal segment carries a 64-bit SipHash-2-4 tag of its fields and scope
//! under that key, in place of the format 1 header CRC (plan section 2, "Journal format").
//! Client bytes inside a payload cannot carry a valid tag, so recovery never takes them
//! for a record, whether as evidence of a completed sync or as a resync point after a
//! damaged header. Payload checksums stay CRC32C: the tag covers the payload CRC field,
//! so a payload is bound to an authenticated header.
//!
//! SipHash-2-4 is a keyed pseudo-random function built for short inputs (Aumasson and
//! Bernstein, 2012). It is implemented here, like CRC32C in [`crate::crc`], so the engine
//! keeps its dependency set; the tests check it against the reference vectors and against
//! `std`'s `SipHasher`, which is the same function.

use std::fmt;

use crate::coding::read_u32_le;
use crate::crc;
use crate::error::{Error, FileId, Result};

/// Bytes of a [`JournalKey`].
pub const KEY_LEN: usize = 16;
/// `JOURNAL_KEY` file: magic ‖ key ‖ masked CRC32C of the 24 bytes before it.
pub const KEY_FILE_LEN: usize = 8 + KEY_LEN + 4;
const KEY_MAGIC: [u8; 8] = *b"NBKVKEY1";

/// The secret of a format 2 store. It never leaves the store directory: `Debug` hides it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct JournalKey {
    k0: u64,
    k1: u64,
}

impl fmt::Debug for JournalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JournalKey(..)")
    }
}

impl JournalKey {
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> JournalKey {
        let (a, b) = bytes.split_at(8);
        JournalKey {
            k0: u64::from_le_bytes(a.try_into().expect("8 bytes")),
            k1: u64::from_le_bytes(b.try_into().expect("8 bytes")),
        }
    }

    fn to_bytes(self) -> [u8; KEY_LEN] {
        let mut out = [0u8; KEY_LEN];
        out[..8].copy_from_slice(&self.k0.to_le_bytes());
        out[8..].copy_from_slice(&self.k1.to_le_bytes());
        out
    }

    /// A new random key: the operating system's random source in a product build, the
    /// node's seeded stream in a simulation build.
    pub fn generate() -> Result<JournalKey> {
        let mut bytes = [0u8; KEY_LEN];
        fill_random(&mut bytes)?;
        Ok(JournalKey::from_bytes(bytes))
    }

    /// The `JOURNAL_KEY` file contents.
    pub fn encode_file(self) -> [u8; KEY_FILE_LEN] {
        let mut out = [0u8; KEY_FILE_LEN];
        out[..8].copy_from_slice(&KEY_MAGIC);
        out[8..8 + KEY_LEN].copy_from_slice(&self.to_bytes());
        let crc = crc::mask(crc::crc32c(&out[..8 + KEY_LEN]));
        out[8 + KEY_LEN..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes a `JOURNAL_KEY` file. A damaged file is `Corruption`: without the key no
    /// format 2 record can be read (plan section 3, R3).
    pub fn decode_file(bytes: &[u8]) -> Result<JournalKey> {
        let damaged = Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "JOURNAL_KEY is damaged",
        };
        if bytes.len() != KEY_FILE_LEN || bytes[..8] != KEY_MAGIC {
            return Err(damaged);
        }
        let stored = read_u32_le(bytes, 8 + KEY_LEN).unwrap_or(0);
        if crc::mask(crc::crc32c(&bytes[..8 + KEY_LEN])) != stored {
            return Err(damaged);
        }
        let key: [u8; KEY_LEN] = bytes[8..8 + KEY_LEN].try_into().expect("16 bytes");
        Ok(JournalKey::from_bytes(key))
    }

    /// SipHash-2-4 of the concatenation of `parts` under this key.
    #[inline]
    pub fn tag(&self, parts: &[&[u8]]) -> u64 {
        let mut s = Sip::new(self.k0, self.k1);
        for part in parts {
            s.write(part);
        }
        s.finish()
    }
}

#[cfg(feature = "sim")]
fn fill_random(out: &mut [u8; KEY_LEN]) -> Result<()> {
    out[..8].copy_from_slice(&nabedra_rt::ids::random_u64().to_le_bytes());
    out[8..].copy_from_slice(&nabedra_rt::ids::random_u64().to_le_bytes());
    Ok(())
}

#[cfg(all(not(feature = "sim"), unix))]
fn fill_random(out: &mut [u8; KEY_LEN]) -> Result<()> {
    // SAFETY: `out` is valid for writes of its length, which is below getentropy's
    // 256-byte limit.
    let rc = unsafe { libc::getentropy(out.as_mut_ptr().cast(), out.len()) };
    if rc != 0 {
        return Err(Error::from(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(all(not(feature = "sim"), not(unix)))]
fn fill_random(out: &mut [u8; KEY_LEN]) -> Result<()> {
    // Development builds only (the server runs on Unix): std seeds every RandomState
    // from the operating system's random source.
    use std::hash::{BuildHasher, Hasher};
    for (i, chunk) in out.chunks_mut(8).enumerate() {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_usize(i);
        chunk.copy_from_slice(&h.finish().to_le_bytes()[..chunk.len()]);
    }
    Ok(())
}

/// SipHash-2-4 state.
struct Sip {
    v0: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    /// Pending bytes (fewer than 8), little-endian.
    tail: u64,
    ntail: usize,
    len: usize,
}

impl Sip {
    #[inline]
    fn new(k0: u64, k1: u64) -> Sip {
        Sip {
            v0: k0 ^ 0x736f_6d65_7073_6575,
            v1: k1 ^ 0x646f_7261_6e64_6f6d,
            v2: k0 ^ 0x6c79_6765_6e65_7261,
            v3: k1 ^ 0x7465_6462_7974_6573,
            tail: 0,
            ntail: 0,
            len: 0,
        }
    }

    #[inline(always)]
    fn round(&mut self) {
        self.v0 = self.v0.wrapping_add(self.v1);
        self.v1 = self.v1.rotate_left(13);
        self.v1 ^= self.v0;
        self.v0 = self.v0.rotate_left(32);
        self.v2 = self.v2.wrapping_add(self.v3);
        self.v3 = self.v3.rotate_left(16);
        self.v3 ^= self.v2;
        self.v0 = self.v0.wrapping_add(self.v3);
        self.v3 = self.v3.rotate_left(21);
        self.v3 ^= self.v0;
        self.v2 = self.v2.wrapping_add(self.v1);
        self.v1 = self.v1.rotate_left(17);
        self.v1 ^= self.v2;
        self.v2 = self.v2.rotate_left(32);
    }

    #[inline(always)]
    fn compress(&mut self, m: u64) {
        self.v3 ^= m;
        self.round();
        self.round();
        self.v0 ^= m;
    }

    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        self.len += bytes.len();
        while self.ntail != 0 && !bytes.is_empty() {
            self.tail |= u64::from(bytes[0]) << (8 * self.ntail);
            self.ntail += 1;
            bytes = &bytes[1..];
            if self.ntail == 8 {
                let m = self.tail;
                self.compress(m);
                self.tail = 0;
                self.ntail = 0;
            }
        }
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.compress(u64::from_le_bytes(chunk.try_into().expect("8 bytes")));
        }
        for (i, &b) in chunks.remainder().iter().enumerate() {
            self.tail |= u64::from(b) << (8 * i);
        }
        self.ntail = chunks.remainder().len();
    }

    #[inline]
    fn finish(mut self) -> u64 {
        let b = ((self.len as u64 & 0xff) << 56) | self.tail;
        self.compress(b);
        self.v2 ^= 0xff;
        self.round();
        self.round();
        self.round();
        self.round();
        self.v0 ^ self.v1 ^ self.v2 ^ self.v3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_key() -> JournalKey {
        let mut bytes = [0u8; KEY_LEN];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        JournalKey::from_bytes(bytes)
    }

    #[test]
    fn ns_unit_siphash_matches_the_reference_vectors() {
        // Appendix A of the SipHash paper: key 00..0f, message 00..(n-1).
        let key = reference_key();
        let msg: Vec<u8> = (0u8..64).collect();
        let expect = [
            0x726f_db47_dd0e_0e31u64,
            0x74f8_39c5_93dc_67fd,
            0x0d6c_8009_d9a9_4f5a,
            0x8567_6696_d7fb_7e2d,
        ];
        for (n, want) in expect.iter().enumerate() {
            assert_eq!(key.tag(&[&msg[..n]]), *want, "length {n}");
        }
    }

    #[test]
    #[allow(deprecated)]
    fn ns_unit_siphash_matches_std_for_every_split() {
        use std::hash::Hasher;
        let key = JournalKey::from_bytes(*b"0123456789abcdef");
        let JournalKey { k0, k1 } = key;
        let msg: Vec<u8> = (0..200u32).map(|i| (i * 31 + 7) as u8).collect();
        for n in 0..msg.len() {
            let mut std_h = std::hash::SipHasher::new_with_keys(k0, k1);
            std_h.write(&msg[..n]);
            let want = std_h.finish();
            assert_eq!(key.tag(&[&msg[..n]]), want, "length {n}");
            for cut in [0, 1, 3, 7, 8, 9, n / 2] {
                let cut = cut.min(n);
                let (a, b) = msg[..n].split_at(cut);
                assert_eq!(key.tag(&[a, b]), want, "length {n} split at {cut}");
            }
        }
    }

    #[test]
    fn ns_unit_key_file_round_trips_and_detects_damage() {
        let key = reference_key();
        let file = key.encode_file();
        assert_eq!(JournalKey::decode_file(&file).unwrap(), key);
        for i in 0..file.len() {
            let mut bad = file;
            bad[i] ^= 0x10;
            assert!(JournalKey::decode_file(&bad).is_err(), "byte {i}");
        }
        assert!(JournalKey::decode_file(&file[..KEY_FILE_LEN - 1]).is_err());
        assert_eq!(format!("{key:?}"), "JournalKey(..)");
        let a = JournalKey::generate().unwrap();
        let b = JournalKey::generate().unwrap();
        assert_ne!(a, b);
    }
}
